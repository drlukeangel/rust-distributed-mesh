//! `node.mesh.leave`: take a node off its mesh on its own, then bring it back
//! (lifecycles/node-mesh-leave.md).
//!
//! A leave is a Build step commanded by the node's mesh-primary, and only after the node's connections
//! are deleted: the data plane first, then the control plane. So the caller submits the two as one
//! Build of steps, `node.connection.delete { All }` then `node.mesh.leave`. The node leaves its mesh's
//! gossip (and the backbone, a node-admin) and its Node RPC pool and then publishes nothing; it keeps
//! its endpoint bound, its node id, incarnation, name, port and data dir, so a fresh Node RPC dial still
//! reaches it for its rename, stop, start or delete. A node that still holds a connection refuses the
//! leave `BusyHoldingConnections`, naming them, and leaves nothing.
//!
//! The way back onto the mesh is a stop and a start: the stop finds the node already left and
//! completes at once, and the start makes the same process rejoin as itself (same node id and
//! incarnation), `node.joined` then `node.started`.
//!
//! Build: `cargo build --example node_mesh_leave` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_mesh_leave <path> <name>
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{Filter, Frame, MeshNode, Nodes, PathName, Peers, WorkflowStream};
use rdm_core::build::Step;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
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

/// The node's row as the entry admin reads it now.
async fn row(nodes: &Nodes, node: &PathName) -> Result<String> {
    let rows = nodes.get(&Filter::new().eq("path", [node.path()]).eq("name", [node.name()])).await?;
    let n = rows.iter().find(|n| &n.name == node).ok_or_else(|| anyhow!("no row holds path {:?}, name {:?}", node.path, node.name))?;
    Ok(format!("{:?}, stopped {}, node id {}, incarnation {:?}", n.status, n.stopped, n.node_id, n.incarnation_id.as_ref().map(|i| i.to_string())))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-mesh-leave");
    let (path, name) = match (std::env::args().nth(1), std::env::args().nth(2)) {
        (Some(path), Some(name)) => (path, name),
        _ => return Err(anyhow!("usage: node_mesh_leave <path> <name>")),
    };
    let node = PathName::new(path.as_str(), name.as_str()).map_err(|e| anyhow!("path {path:?}, name {name:?}: {e}"))?;
    let (peers, entry) = enter().await?;
    let nodes = Nodes::new(peers.carrier_at(entry));
    println!("before: {}", row(&nodes, &node).await?);

    finish("delete connections, leave mesh", nodes.steps(vec![Step::DeleteConnections { node: node.clone() }, Step::LeaveMesh { node: node.clone() }]).await?).await?;
    println!("left: {}", row(&nodes, &node).await?);

    // Back onto the mesh: stopped, then started as the same birth.
    finish(&format!("node.stop path {:?}, name {:?}", node.path, node.name), nodes.stop(&node).await?).await?;
    finish(&format!("node.start path {:?}, name {:?}", node.path, node.name), nodes.start(&node).await?).await?;
    println!("after: {}", row(&nodes, &node).await?);
    Ok(())
}
