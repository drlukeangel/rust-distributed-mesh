//! `node.name.update`: rename a live node (lifecycles/node-name-update.md).
//!
//! A rename is one call to the node's mesh-primary, the one writer of its mesh's rows, for the node's
//! exact birth, naming the new name only (the part of the `path.name` after its path: `2.old` of
//! `mesh1.broker.2.old`); the path never moves. Its precondition is that the node admits no
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
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_name_update <path.name> [<new name>]
//! ```
//!
//! The new name defaults to the node's name with `.old` after it.
use anyhow::{anyhow, Context, Result};
use rdm_client::{ExactBirth, Filter, Frame, MeshNode, NodeView, Nodes, PathName, Peers, WorkflowStream};
use rdm_core::build::Step;
use rdm_core::NodeKind;
use rdm_transport::{NodeTarget, RouteChoice};

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

fn exact(n: &NodeView) -> Result<ExactBirth> {
    let incarnation = n.incarnation_id.clone().ok_or_else(|| anyhow!("{} has no live birth", n.name))?;
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
    let node = args.next().ok_or_else(|| anyhow!("usage: node_name_update <path.name> [<new name>]"))?;
    let node: PathName = node.parse().map_err(|e| anyhow!("{node}: {e}"))?;
    let name = args.next().unwrap_or_else(|| node.renamed().name());
    let (peers, entry) = enter().await?;
    let nodes = Nodes::new(peers.carrier_at(entry));

    let mesh = nodes.get(&Filter::new().eq("mesh", [node.mesh.clone()])).await?;
    peers.learn(&mesh);
    let row = mesh.iter().find(|n| n.name == node).ok_or_else(|| anyhow!("no row holds {node}"))?;
    let primary = mesh.iter().find(|n| n.kind == NodeKind::NodeAdmin && n.is_primary).ok_or_else(|| anyhow!("no row names {}'s primary", node.mesh))?;

    // The precondition: its connections deleted, then its mesh left.
    finish("delete connections, leave mesh", nodes.steps(vec![Step::DeleteConnections { node: node.clone() }, Step::LeaveMesh { node: node.clone() }]).await?).await?;

    let reply = nodes.name_update(&exact(row)?, &primary.node_id, &RouteChoice::Direct, &name).await?;
    println!("node.name.update {node} -> {name}: {reply:?}");

    // The renamed node, stopped under its new name.
    let renamed = node.with_name(&name).map_err(|e| anyhow!("{name} at {}: {e}", node.path()))?;
    finish(&format!("node.stop {renamed}"), nodes.stop(&renamed).await?).await
}
