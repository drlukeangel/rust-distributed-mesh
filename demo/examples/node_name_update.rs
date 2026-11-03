//! `node.name.update`: rename a live node (lifecycles/node-name-update.md).
//!
//! A rename is one call to the node's mesh-primary, the one writer of its mesh's rows, for the node's
//! exact birth, naming the new name within the node's path (`2-old` at path `mesh1.broker`); the
//! path never moves. Its precondition is that the node admits no
//! connection: the mesh-primary holds the birth's `node.connections.deleted` of an `All`, then its
//! `node.mesh.left`. So the caller first runs those two steps as one Build, then renames. A stopped
//! node is never renamed, a held name answers `NameHeld` naming the node that holds it, and a name
//! outside the grammar is `RejectedName`.
//!
//! The mesh-primary claims the new name, writes the row, releases the old name and tells the left node
//! its name (`ApplyNodeName`); the node, off its mesh, publishes nothing, and its stop or its next join
//! carries the name. Here the renamed node is then stopped under its new name: a stopped birth with its
//! process kept, waiting for a start or a delete.
//!
//! Build: `cargo build --example node_name_update` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_name_update <path> <name> [<new name>]
//! ```
//!
//! The new name defaults to the node's name with `-old` after it.
use anyhow::{anyhow, Context, Result};
use rdm_client::{ExactBirth, Filter, Frame, MeshNode, NodeView, Nodes, PathName, Peers, WorkflowStream};
use rdm_core::build::Step;

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
    let (path, name) = match (args.next(), args.next()) {
        (Some(path), Some(name)) => (path, name),
        _ => return Err(anyhow!("usage: node_name_update <path> <name> [<new name>]")),
    };
    let node = PathName::new(path.as_str(), name.as_str()).map_err(|e| anyhow!("path {path:?}, name {name:?}: {e}"))?;
    // The new name at the same path, unchecked here: a name outside the grammar is the mesh-primary's
    // `RejectedName`, never refused before the call.
    let renamed = match args.next() {
        Some(name) => PathName { path: node.path.clone(), name },
        None => node.renamed().map_err(|e| anyhow!("path {:?}, name {:?}: {e}", node.path, node.name))?,
    };
    let (peers, entry) = enter().await?;
    let nodes = Nodes::new(peers.carrier_at(entry));

    let all = nodes.get(&Filter::new()).await?;
    peers.learn(&all);
    let row = all.iter().find(|n| n.location() == node).ok_or_else(|| anyhow!("no row holds path {:?}, name {:?}", node.path, node.name))?;
    let primary = all.iter().find(|n| n.mesh == row.mesh && n.r#type == "node-admin" && n.is_primary).ok_or_else(|| anyhow!("no row names {}'s primary", row.mesh))?;

    // The precondition: its connections deleted, then its mesh left.
    finish("delete connections, leave mesh", nodes.steps(vec![Step::DeleteConnections { path: node.path.clone(), name: node.name.clone()}, Step::LeaveMesh { path: node.path.clone(), name: node.name.clone()}]).await?).await?;

    let reply = nodes.name_update(&exact(row)?, &primary.node_id, &RouteChoice::Direct, &renamed).await?;
    println!("node.name.update path {:?}, name {:?} -> name {:?}: {reply:?}", node.path, node.name, renamed.name);

    // The renamed node, stopped under its new name.
    finish(&format!("node.stop path {:?}, name {:?}", renamed.path, renamed.name), nodes.stop(&renamed).await?).await
}
