//! `connections-reconnect`: the source-owned Direct reconnect schedule, exponential backoff with
//! deterministic jitter, and ordered Proxy retirement (`docs/architecture/connections.md:1`,
//! `crates/rdm-node/src/connection_reconnect.rs:51`, `crates/rdm-core/src/reconnect.rs:1`).
//!
//! When a source's latest `Direct` connection entry to a destination is `Failed` for the
//! destination's current incarnation, `reconnect_plan` reconstructs the pending [`ReconnectItem`]
//! from that indexed row (`logged_at_ms + reconnect_backoff(ordinal) + deterministic_jitter`).
//! Each failed attempt appends `next_failure` at `attempt_ordinal + 1` in the same recovery round.
//! Once a scheduled `Direct` dial succeeds (`Connected`), `owed_direct_restored` produces the
//! ordered `Proxy Disconnected(reason=direct-restored)` entry before retiring the Proxy route.
//!
//! Build: `cargo build --example connections_reconnect` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/connections_reconnect
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{Filter, MeshNode, NodeStatus, NodeView, Nodes, Peers};
use rdm_core::connections::{
    ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection,
};
use rdm_core::reconnect::{
    deterministic_jitter, due, first_failure_for, next_due, next_failure, owed_direct_restored,
    reconnect_backoff, reconnect_plan, DIRECT_RESTORED,
};
use rdm_core::{ConnectionId, IncarnationId, NodeKind};

fn to_end(n: &NodeView) -> Result<ConnectionEnd> {
    Ok(ConnectionEnd {
        name: n.name.clone(),
        node_id: n.node_id.clone(),
        incarnation: Some(
            n.incarnation_id
                .clone()
                .ok_or_else(|| anyhow!("{} has no incarnation_id", n.name))?,
        ),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-connections-reconnect");
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let all = Nodes::new(peers.carrier_at(birth.name)).get(&Filter::new()).await?;

    let src = all
        .iter()
        .find(|n| n.kind == NodeKind::Compute && n.status == NodeStatus::Ready)
        .ok_or_else(|| anyhow!("no Ready compute node in estate"))?;
    let dst = all
        .iter()
        .find(|n| n.kind == NodeKind::Broker && n.status == NodeStatus::Ready && n.mesh != src.mesh)
        .or_else(|| all.iter().find(|n| n.kind == NodeKind::Broker && n.status == NodeStatus::Ready))
        .ok_or_else(|| anyhow!("no Ready broker node in estate"))?;
    let gw = all
        .iter()
        .find(|n| n.kind == NodeKind::Gateway && n.status == NodeStatus::Ready)
        .ok_or_else(|| anyhow!("no Ready gateway node in estate"))?;

    let (src_end, dst_end, gw_end) = (to_end(src)?, to_end(dst)?, to_end(gw)?);
    let dst_inc = dst_end.incarnation.clone().unwrap();
    let alpn = rshape_consumer::data_alpn().name;

    let mut held = ConnectionsHeld::new();
    held.set_own_source(src.name.clone());
    held.mark_complete();

    let failed_1 = first_failure_for(
        src_end.clone(),
        dst_end.clone(),
        alpn.clone(),
        None,
        "not-sent:quic-connect",
        10_000,
    );
    let proxy_up = NodeConnection {
        id: ConnectionId::mint(),
        source: src_end.clone(),
        destination: dst_end.clone(),
        kind: ConnectionKind::Proxy,
        alpn: alpn.clone(),
        state: ConnectionState::Connected,
        carrier: Some(gw_end),
        recovery: None,
        reason: None,
        logged_at_ms: 10_010,
    };
    held.apply(failed_1.clone()).map_err(|e| anyhow!("{e:?}"))?;
    held.apply(proxy_up).map_err(|e| anyhow!("{e:?}"))?;

    let plan_1 = reconnect_plan(&held, |_| Some(dst_inc.clone()));
    assert_eq!(plan_1.len(), 1);
    let item_1 = &plan_1[0];
    assert_eq!(item_1.due_ms, next_due(&failed_1).unwrap());
    assert!(due(&plan_1, item_1.due_ms - 1).is_empty());
    assert_eq!(due(&plan_1, item_1.due_ms).len(), 1);
    println!(
        "ordinal 1 ({} -> {}): round={} backoff={}ms jitter={}ms due_ms={}",
        src.name,
        dst.name,
        item_1.recovery.recovery_round,
        reconnect_backoff(1),
        deterministic_jitter(&src.name, &dst.name, 1),
        item_1.due_ms
    );

    let failed_2 = next_failure(item_1, "not-sent:quic-connect", item_1.due_ms);
    held.apply(failed_2).map_err(|e| anyhow!("{e:?}"))?;
    let plan_2 = reconnect_plan(&held, |_| Some(dst_inc.clone()));
    let item_2 = &plan_2[0];
    println!(
        "ordinal 2 ({} -> {}): round={} backoff={}ms jitter={}ms due_ms={}",
        src.name,
        dst.name,
        item_2.recovery.recovery_round,
        reconnect_backoff(2),
        deterministic_jitter(&src.name, &dst.name, 2),
        item_2.due_ms
    );

    let superseded = reconnect_plan(&held, |_| Some(IncarnationId::mint()));
    assert!(superseded.is_empty(), "superseded destination incarnation drops the reconnect series");

    let direct_up = NodeConnection {
        id: ConnectionId::mint(),
        source: src_end,
        destination: dst_end,
        kind: ConnectionKind::Direct,
        alpn,
        state: ConnectionState::Connected,
        carrier: None,
        recovery: None,
        reason: None,
        logged_at_ms: item_2.due_ms,
    };
    held.apply(direct_up).map_err(|e| anyhow!("{e:?}"))?;
    let owed = owed_direct_restored(&held, item_2.due_ms + 1);
    assert_eq!(owed.len(), 1);
    assert_eq!(owed[0].reason.as_deref(), Some(DIRECT_RESTORED));
    println!(
        "Direct Connected at {}ms: owed Proxy retirement state={:?} reason={:?}",
        item_2.due_ms, owed[0].state, owed[0].reason
    );
    held.apply(owed[0].clone()).map_err(|e| anyhow!("{e:?}"))?;
    assert!(reconnect_plan(&held, |_| Some(dst_inc.clone())).is_empty());
    Ok(())
}
