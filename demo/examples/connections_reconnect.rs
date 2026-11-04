//! `connections-reconnect`: a lost Direct's Failed row and the ordered Proxy deletion when a Direct
//! comes back (`docs/architecture/connections.md` §9, §10; `crates/rdm-core/src/reconnect.rs`).
//!
//! A lost Direct is dialled on `Send` and nothing else (Q348): its Failed row records the dial whose
//! attempts were spent, and no dial is scheduled from it. A source holding a Proxy returns to Direct
//! when the destination dials it (R-CN1) or the Proxy is invalidated (§8); once its own Direct is
//! `Connected`, `owed_direct_restored` produces the ordered `Proxy Disconnected(reason=direct-restored)`
//! entry before the Proxy route is deleted.
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
use rdm_core::reconnect::{owed_direct_restored, DIRECT_RESTORED};
use rdm_core::ConnectionId;

fn to_end(n: &NodeView) -> Result<ConnectionEnd> {
    Ok(ConnectionEnd {
        mesh: n.mesh().clone(),
        r#type: n.r#type.clone(),
        path: n.path.clone(), name: n.name.clone(),
        node_id: n.node_id.clone(),
        incarnation: Some(
            n.incarnation
                .clone()
                .ok_or_else(|| anyhow!("path {:?}, name {:?} has no incarnation_id", n.path, n.name))?,
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
    let all = Nodes::new(peers.carrier_at(birth.location())).get(&Filter::new()).await?;

    let src = all
        .iter()
        .find(|n| n.r#type == "compute" && n.state == NodeStatus::Ready)
        .ok_or_else(|| anyhow!("no Ready compute node in estate"))?;
    let dst = all
        .iter()
        .find(|n| n.r#type == "broker" && n.state == NodeStatus::Ready && n.mesh() != src.mesh())
        .or_else(|| all.iter().find(|n| n.r#type == "broker" && n.state == NodeStatus::Ready))
        .ok_or_else(|| anyhow!("no Ready broker node in estate"))?;
    let gw = all
        .iter()
        .find(|n| n.r#type == "gateway" && n.state == NodeStatus::Ready)
        .ok_or_else(|| anyhow!("no Ready gateway node in estate"))?;

    let (src_end, dst_end, gw_end) = (to_end(src)?, to_end(dst)?, to_end(gw)?);
    let alpn = rshape_consumer::data_alpn().name;

    let mut held = ConnectionsHeld::new();
    held.set_own_source(src.node_id.clone());
    held.mark_complete();

    let failed_1 = NodeConnection {
        id: ConnectionId::mint(),
        source: src_end.clone(),
        destination: dst_end.clone(),
        kind: ConnectionKind::Direct,
        alpn: alpn.clone(),
        state: ConnectionState::Failed,
        carrier: None,
        reason: Some("not-sent:quic-connect".into()),
        logged_at_ms: 10_000,
    };
    let proxy_up = NodeConnection {
        id: ConnectionId::mint(),
        source: src_end.clone(),
        destination: dst_end.clone(),
        kind: ConnectionKind::Proxy,
        alpn: alpn.clone(),
        state: ConnectionState::Connected,
        carrier: Some(gw_end),
        reason: None,
        logged_at_ms: 10_010,
    };
    held.apply(failed_1.clone()).map_err(|e| anyhow!("{e:?}"))?;
    held.apply(proxy_up).map_err(|e| anyhow!("{e:?}"))?;

    println!(
        "Direct Failed (path {:?}, name {:?} -> path {:?}, name {:?}): reason={:?}; nothing dials from it (Q348)",
        src.path,
        src.name,
        dst.path,
        dst.name,
        failed_1.reason,
    );

    let direct_up = NodeConnection {
        id: ConnectionId::mint(),
        source: src_end,
        destination: dst_end,
        kind: ConnectionKind::Direct,
        alpn,
        state: ConnectionState::Connected,
        carrier: None,
        reason: None,
        logged_at_ms: 20_000,
    };
    held.apply(direct_up).map_err(|e| anyhow!("{e:?}"))?;
    let owed = owed_direct_restored(&held, 20_001);
    assert_eq!(owed.len(), 1);
    assert_eq!(owed[0].reason.as_deref(), Some(DIRECT_RESTORED));
    println!(
        "Direct Connected at 20000ms: owed Proxy delete state={:?} reason={:?}",
        owed[0].state, owed[0].reason
    );
    held.apply(owed[0].clone()).map_err(|e| anyhow!("{e:?}"))?;
    assert!(owed_direct_restored(&held, 20_002).is_empty(), "the delete landed: nothing more is owed");
    Ok(())
}
