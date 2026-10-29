//! `ping`: application and transport liveness probe on op `0x01`
//! (`docs/architecture/node-rpc-envelope.md:591`, `crates/rdm-core/src/ping.rs:78`).
//!
//! [`Ping`] on op `0x01` (`PingRequest::Ping { payload }` -> `PingReply::Pong { payload }`) is the
//! direct-hop liveness and sanity probe served by every RDM node and node-admin. Because `Ping` is
//! a direct transport probe (`Ping::FORWARDABLE == false`), `call::<Ping>` echoes `Pong` from both
//! a node-admin and a role member, while `call_via::<Ping>` is refused before sending with
//! `NotSentReason::NotForwardable { op: 0x01 }`.
//!
//! Build: `cargo build --example ping` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/ping [<path.name>]
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{Filter, MeshNode, NodeStatus, Nodes, PathName, Peers};
use rdm_core::outcome::{NotSentReason, RpcOutcome};
use rdm_core::ping::{Ping, PingReply, PingRequest};
use rdm_core::{NodeKind, NodeProtocol};
use rdm_transport::{CallOptions, NodeTarget};

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-ping");
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let entry = birth.name.clone();
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let carrier = peers.carrier_at(entry.clone());
    let nodes = Nodes::new(carrier.clone());

    let all = nodes.get(&Filter::new()).await?;
    peers.learn(&all);
    let target: PathName = match std::env::args().nth(1) {
        Some(arg) => arg.parse().map_err(|e| anyhow!("{arg}: {e}"))?,
        None => all
            .iter()
            .find(|n| n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
            .map(|n| n.name.clone())
            .ok_or_else(|| anyhow!("no Ready role node found in estate"))?,
    };
    let row = all.iter().find(|n| n.name == target).ok_or_else(|| anyhow!("no row holds {target}"))?;

    let req_admin = PingRequest::Ping { payload: b"ping-admin".to_vec() };
    let (out_admin, _) = carrier
        .client()
        .call::<Ping>(&NodeTarget::CurrentPath(entry.clone()), &req_admin, &CallOptions::default())
        .await;
    let RpcOutcome::Reply(reply_admin) = out_admin else {
        return Err(anyhow!("Ping to {entry} failed: {out_admin:?}"));
    };
    let PingReply::Pong { payload: pong_admin } = reply_admin.into_value() else {
        return Err(anyhow!("unexpected PingReply from {entry}"));
    };
    assert_eq!(pong_admin, b"ping-admin");
    println!(
        "Ping (0x{:02x}) to admin {entry}: Pong({:?})",
        Ping::OP,
        String::from_utf8_lossy(&pong_admin)
    );

    let req_member = PingRequest::Ping { payload: b"ping-member".to_vec() };
    let (out_member, _) = carrier
        .client()
        .call::<Ping>(&NodeTarget::ExactNode(row.node_id.clone()), &req_member, &CallOptions::default())
        .await;
    let RpcOutcome::Reply(reply_member) = out_member else {
        return Err(anyhow!("Ping to {} failed: {out_member:?}", row.name));
    };
    let PingReply::Pong { payload: pong_member } = reply_member.into_value() else {
        return Err(anyhow!("unexpected PingReply from {}", row.name));
    };
    assert_eq!(pong_member, b"ping-member");
    println!(
        "Ping (0x{:02x}) to member {} ({}): Pong({:?})",
        Ping::OP,
        row.name,
        row.node_id,
        String::from_utf8_lossy(&pong_member)
    );

    let (out_via, _) = carrier
        .client()
        .call_via::<Ping>(
            &NodeTarget::CurrentPath(entry),
            &row.node_id,
            &req_member,
            &CallOptions::default(),
        )
        .await;
    let RpcOutcome::NotSent(not_sent) = out_via else {
        return Err(anyhow!("expected NotSent(NotForwardable), got {out_via:?}"));
    };
    assert_eq!(not_sent.reason(), &NotSentReason::NotForwardable { op: Ping::OP });
    println!("call_via::<Ping> refused before sending: {:?}", not_sent.reason());
    Ok(())
}
