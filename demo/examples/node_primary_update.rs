//! `node.primary.update` (op `0x21`): move a mesh's primary to its successor before maintenance on the
//! node that holds it (lifecycles/node-primary-update.md).
//!
//! Who is primary is the election's result over the mesh's ready node-admins. A planned move does not
//! pick the successor: the mesh-primary is commanded, the election over its own view with itself
//! excluded names a ready successor (none is refused by name, and nothing changes), the mesh-primary
//! finishes the writes it has issued and calls the successor, which installs itself and answers
//! `PrimaryUpdated` with the revisions it installed. When the mesh-primary is also the fabric-primary
//! and the successor is in its mesh, both primaries move together. The successor's row records the
//! change after it happened; a read made at once may still name the old primary.
//!
//! The command names the Build and attempt it runs under: they key the operation, so an identical
//! repeat is answered as the first was. A stop, delete or replace of a primary runs this move itself
//! as its first step; the call here is the move alone, made under the fabric's current Build.
//!
//! Build: `cargo build --example node_primary_update` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_primary_update <mesh>
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{BuildId, Fabrics, Filter, MeshNode, Nodes, PathName, Peers};
use rdm_core::handover::NodePrimaryUpdateReply;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-primary-update");
    let mesh = std::env::args().nth(1).ok_or_else(|| anyhow!("usage: node_primary_update <mesh>"))?;
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let nodes = Nodes::new(carrier.clone());
    peers.learn(&nodes.get(&Filter::new()).await?);

    let fabric = Fabrics::new(carrier.clone()).get(&Filter::new()).await?.into_iter().next().ok_or_else(|| anyhow!("fabric.get answered no fabric"))?;
    let primary = fabric.meshes.iter().find(|m| m.name == mesh).ok_or_else(|| anyhow!("{} holds no mesh {mesh}", fabric.name))?.primary_admin.clone().ok_or_else(|| anyhow!("{mesh} names no mesh-primary"))?;
    // The Build and attempt the move runs under: the fabric's current Build.
    let build_id = BuildId::parse(fabric.build_id.as_deref().ok_or_else(|| anyhow!("fabric.get names no current Build"))?).map_err(|e| anyhow!("the current Build id: {e}"))?;
    let attempt = carrier.build(&build_id).await?.attempt;

    match nodes.primary_update(&fabric.name, &primary, &build_id, attempt).await? {
        NodePrimaryUpdateReply::PrimaryUpdated { revisions } => {
            println!("node.primary.update of {mesh} at {primary}: updated");
            for (node_type, from, to, revision) in revisions {
                println!("  {node_type}: {from:?} -> {to:?} at revision {revision}");
            }
        }
        refused => println!("node.primary.update of {mesh} at {primary}: {refused:?}"),
    }
    Ok(())
}
