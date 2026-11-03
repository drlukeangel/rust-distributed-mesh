//! `node.name.update`: rename a node (lifecycles/node-name-update.md).
//!
//! A rename is one call to the node's mesh-primary, the one writer of its mesh's rows, for the node's
//! exact birth, naming the new name within the node's path; the path never moves. Its one condition is
//! that the node holds no connection: the caller first deletes its connections (`node.connection.delete
//! { All }`, a node command of its mesh-primary), then renames. A held name answers `NameHeld` naming
//! the node that holds it, and a name outside the grammar is `RejectedName`.
//!
//! The mesh-primary writes the row under the new name and tells the node (`ApplyNodeName`); the node
//! takes it and rejoins its mesh under it, and the call completes once it is back on its mesh
//! publishing the new name.
//!
//! Build: `cargo build --example node_name_update` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_name_update <path> <name> <new name>
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{ExactBirth, Filter, Frame, MeshNode, NodeView, Nodes, PathName, Peers, WorkflowStream};

use rdm_transport::{NodeTarget, RouteChoice};

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.location()))
}

fn exact(n: &NodeView) -> Result<ExactBirth> {
    let incarnation = n.incarnation_id.clone().ok_or_else(|| anyhow!("path {:?}, name {:?} has no live birth", n.path, n.name))?;
    Ok(ExactBirth { target: NodeTarget::ExactNode(n.node_id.clone()), node_id: n.node_id.clone(), incarnation })
}

/// Read a workflow's frames to its terminal: `Complete` is the only success.
async fn finish(what: &str, mut w: WorkflowStream) -> Result<()> {
    while let Some(frame) = w.next().await {
        let frame = frame?;
        println!("{what}: {frame:?}");
        match frame {
            Frame::Complete => return Ok(()),
            Frame::Failed { step, reason } => return Err(anyhow!("{what} failed at {step:?}: {reason}")),
            Frame::FabricPrimaryMoved { fabric_primary } => return Err(anyhow!("{what}: the fabric-primary moved to {fabric_primary:?}; re-submit the Build there")),
            _ => {}
        }
    }
    Err(anyhow!("{what}: the stream ended without a terminal frame"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-name-update");
    let mut args = std::env::args().skip(1);
    let (path, name, new_name) = match (args.next(), args.next(), args.next()) {
        (Some(path), Some(name), Some(new_name)) => (path, name, new_name),
        _ => return Err(anyhow!("usage: node_name_update <path> <name> <new name>")),
    };
    let node = PathName::new(path.as_str(), name.as_str()).map_err(|e| anyhow!("path {path:?}, name {name:?}: {e}"))?;
    // The new name at the same path, unchecked here: a name outside the grammar is the mesh-primary's
    // `RejectedName`, never refused before the call.
    let renamed = PathName { path: node.path.clone(), name: new_name };
    let (peers, entry) = enter().await?;
    let nodes = Nodes::new(peers.carrier_at(entry));

    let all = nodes.get(&Filter::new()).await?;
    peers.learn(&all);
    let row = all.iter().find(|n| n.location() == node).ok_or_else(|| anyhow!("no row holds path {:?}, name {:?}", node.path, node.name))?;
    let primary = all.iter().find(|n| n.mesh == row.mesh && n.r#type == "node-admin" && n.is_primary).ok_or_else(|| anyhow!("no row names {}'s primary", row.mesh))?;

    // The one condition: no connection. A node command of its mesh-primary, never a Build (Q342).
    finish("delete connections", nodes.delete_connections(&node, &rdm_core::status::ConnectionScope::All).await?).await?;

    // Answered once the node is back on its mesh under the new name.
    let reply = nodes.name_update(&exact(row)?, &primary.node_id, &RouteChoice::Direct, &renamed).await?;
    println!("node.name.update path {:?}, name {:?} -> name {:?}: {reply:?}", node.path, node.name, renamed.name);
    Ok(())
}
