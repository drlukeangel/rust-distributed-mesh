//! `node.fabric-primary.update` (op `0x21`): move the fabric-primary before maintenance on the node-admin
//! that is it (lifecycles/node-primary-update.md, the fabric-primary).
//!
//! The fabric-primary is commanded and moves itself: to a ready node-admin of its own mesh first, so the
//! fabric-primary and that mesh's primary move together; when its mesh has no ready successor, to
//! another mesh's mesh-primary. The successor installs itself; the command's stream answers
//! `PrimaryUpdating`, then `PrimaryUpdated { node_id }`, the new fabric-primary. A Build stream open across the move ends `FabricPrimaryMoved` naming the new
//! fabric-primary, and the caller re-submits its Build id there (`examples/build_reattach.rs`).
//!
//! As with `node.primary.update`, the command names the Build it runs under, which keys the operation:
//! here the fabric's current Build.
//!
//! Build: `cargo build --example fabric_primary_update` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/fabric_primary_update
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{BuildId, Fabrics, Filter, MeshNode, Nodes, PathName, Peers};
use rdm_core::handover::NodePrimaryUpdateReply;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.location()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-fabric-primary-update");
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let nodes = Nodes::new(carrier.clone());
    peers.learn(&nodes.get(&Filter::new()).await?);

    let fabrics = Fabrics::new(carrier.clone());
    let fabric = fabrics.get(&Filter::new()).await?.into_iter().next().ok_or_else(|| anyhow!("fabric.get answered no fabric"))?;
    let fabric_primary = fabric.fabric_primary.clone().ok_or_else(|| anyhow!("fabric.get names no fabric-primary"))?;
    let build_id = BuildId::parse(fabric.build_id.as_deref().ok_or_else(|| anyhow!("fabric.get names no current Build"))?).map_err(|e| anyhow!("the current Build id: {e}"))?;

    match nodes.fabric_primary_update(&fabric.name, &fabric_primary, &build_id).await? {
        NodePrimaryUpdateReply::PrimaryUpdated { node_id } => {
            println!("node.fabric-primary.update at {fabric_primary}: updated; the new fabric-primary is {node_id}");
        }
        refused => println!("node.fabric-primary.update at {fabric_primary}: {refused:?}"),
    }
    // The record follows the move: the fabric's own read names the new fabric-primary once it is recorded.
    let after = fabrics.get(&Filter::new()).await?.into_iter().next().and_then(|f| f.fabric_primary);
    println!("fabric.get now names {:?}", after.map(|p| p.to_string()));
    Ok(())
}
