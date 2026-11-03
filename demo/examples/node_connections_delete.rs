//! `node.connection.delete`: graceful node-wide closure (`ConnectionScope::All`) or selective cut
//! (`ConnectionScope::Connections`) of a live member's data-plane connections while the process
//! keeps running (`docs/architecture/lifecycles/node-connections-delete.md:1`,
//! `crates/rdm-admin/src/workflows/node_connections_delete.rs:6`,
//! `crates/rdm-core/src/node_command.rs:59`).
//!
//! `node.connection.delete` is invoked either:
//! - directly at the member's mesh-primary (`Nodes::connections_delete` with `ConnectionScope::All`
//!   or `ConnectionScope::Connections`), which sends `NodeCommandRequest::DeleteConnections` on op
//!   `0x22` and answers `StatusReply::ConnectionsDeleted { closed, not_open, held, receipt }`; or
//! - as a Build workflow (`Nodes::delete_connections`), which moves any held primary roles first, streams
//!   `NodeEvent::ConnectionsDeleted`, and sets the member's status to
//!   [`NodeStatus::ConnectionsDeleted`] while the process keeps running with the same birth.
//! An empty `ConnectionScope::Connections(vec![])` is refused by name (`RejectedConnectionScope`)
//! before anything is sent.
//!
//! Build: `cargo build --example node_connections_delete` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_connections_delete [<path> <name>]
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{
    Connections, ExactBirth, Filter, MeshNode, NodeEvent, NodeStatus, NodeView, Nodes, PathName,
    Peers,
};
use rdm_core::status::ConnectionScope;

use rdm_transport::{NodeTarget, RouteChoice};

fn exact(n: &NodeView) -> Result<ExactBirth> {
    let incarnation = n.incarnation_id.clone().ok_or_else(|| anyhow!("path {:?}, name {:?} has no live birth", n.name.path, n.name.name))?;
    Ok(ExactBirth {
        target: NodeTarget::ExactNode(n.node_id.clone()),
        node_id: n.node_id.clone(),
        incarnation,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-connections-delete");
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let entry = birth.name.clone();
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let carrier = peers.carrier_at(entry);
    let nodes = Nodes::new(carrier.clone());
    let connections = Connections::new(carrier);

    let all = nodes.get(&Filter::new()).await?;
    peers.learn(&all);
    let mut args = std::env::args().skip(1);
    let target: PathName = match (args.next(), args.next()) {
        (Some(path), Some(name)) => PathName::new(path.as_str(), name.as_str()).map_err(|e| anyhow!("path {path:?}, name {name:?}: {e}"))?,
        (Some(path), None) => return Err(anyhow!("path {path:?} names no name: usage node_connections_delete [<path> <name>]")),
        _ => all
            .iter()
            .find(|n| n.r#type != "node-admin" && n.status == NodeStatus::Ready)
            .map(|n| n.name.clone())
            .ok_or_else(|| anyhow!("no Ready role node found in estate"))?,
    };
    let row = all.iter().find(|n| n.name == target).ok_or_else(|| anyhow!("no row holds path {:?}, name {:?}", target.path, target.name))?;
    let primary = all
        .iter()
        .find(|n| n.mesh == row.mesh && n.r#type == "node-admin" && n.is_primary)
        .ok_or_else(|| anyhow!("no row names {}'s mesh-primary", row.mesh))?;
    let birth_target = exact(row)?;

    let held_before = connections.get(&birth_target, &Filter::new()).await?;
    println!(
        "node.connection.get on path {:?}, name {:?} ({}): {} held entries",
        target.path,
        target.name,
        row.node_id,
        held_before.connections.len()
    );

    let refused = nodes
        .connections_delete(
            &birth_target,
            &primary.node_id,
            &RouteChoice::Direct,
            &ConnectionScope::Connections(Vec::new()),
        )
        .await?;
    println!("empty ConnectionScope::Connections refused: {refused:?}");

    let cut = nodes
        .connections_delete(
            &birth_target,
            &primary.node_id,
            &RouteChoice::Direct,
            &ConnectionScope::All,
        )
        .await?;
    println!("node.connection.delete {{ All }} via mesh-primary path {:?}, name {:?}: {cut:?}", primary.name.path, primary.name.name);

    let mut stream = nodes.delete_connections(&target).await?;
    let mut saw_deleted_event = false;
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        println!("workflow frame: {frame:?}");
        if frame == rdm_client::Frame::Event(NodeEvent::ConnectionsDeleted) {
            saw_deleted_event = true;
        }
    }
    assert!(saw_deleted_event, "workflow must emit NodeEvent::ConnectionsDeleted");

    let after = nodes
        .get(
            &Filter::new()
                .eq("path", [target.path()])
                .eq("name", [target.name()]),
        )
        .await?
        .into_iter()
        .find(|n| n.name == target)
        .ok_or_else(|| anyhow!("no row for path {:?}, name {:?} after node.connection.delete", target.path, target.name))?;
    assert_eq!(after.status, NodeStatus::ConnectionsDeleted);
    assert_eq!(after.node_id, row.node_id);
    assert_eq!(after.incarnation_id, row.incarnation_id);
    println!(
        "after node.connection.delete: path={:?} name={:?} node_id={} incarnation={:?} status={:?}",
        after.name.path, after.name.name, after.node_id, after.incarnation_id, after.status
    );
    Ok(())
}
