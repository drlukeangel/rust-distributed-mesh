//! The fabric-wide reads: `fabric.get` (the fabric, its meshes and their primaries) and
//! `fabric.atlas.get` (one row per node of the current Build's topology, as the fabric-primary holds
//! it).
//!
//! `fabric.get` answers from the admin it is sent to: the fabric's status, provider, fabric-primary
//! and current Build, and each mesh's status and mesh-primary. The atlas is the fabric-primary's own
//! record: for every node the current Build names, the birth its node row names current, the status the
//! operations left it in, whether it is a stopped birth waiting for a start or a delete, and the
//! operation accepted on it whose attempt has not completed (`accepted`). `observed` is the status the
//! fabric-primary's gossip view holds for the same birth: context only, never what an acceptance reads.
//! An application reads the atlas for a fabric-wide picture of its nodes, for example to show which
//! node an operation is still running on.
//!
//! The atlas is answered by the fabric-primary, so the read is sent there: the carrier starts at the
//! fabric-primary `fabric.get` names.
//!
//! Build: `cargo build --example fabric_atlas_read` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/fabric_atlas_read
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{Fabrics, Filter, MeshNode, Nodes, PathName, Peers};

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-fabric-atlas-read");
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);

    let fabric = Fabrics::new(carrier.clone()).get(&Filter::new()).await?.into_iter().next().ok_or_else(|| anyhow!("fabric.get answered no fabric"))?;
    println!("fabric {} ({}): {:?}, provider {:?}, fabric-primary {:?}", fabric.name, fabric.id, fabric.status, fabric.provider, fabric.fabric_primary.as_ref().map(|p| p.to_string()));
    for m in &fabric.meshes {
        println!("  mesh {}: {:?}, mesh-primary {:?}, {} nodes", m.name, m.status, m.primary_admin.as_ref().map(|p| p.to_string()), m.nodes.len());
    }

    // The atlas, at the fabric-primary.
    let fabric_primary = fabric.fabric_primary.clone().ok_or_else(|| anyhow!("fabric.get names no fabric-primary"))?;
    peers.learn(&Nodes::new(carrier).get(&Filter::new()).await?);
    let atlas = Fabrics::new(peers.carrier_at(fabric_primary)).atlas_get().await?;
    for n in &atlas.nodes {
        let pending = n.accepted.as_deref().map(|op| format!(", {op} running")).unwrap_or_default();
        println!("  {} {:?}{}{pending} (gossip view: {:?})", n.name, n.status, if n.stopped { " stopped" } else { "" }, n.observed);
    }
    Ok(())
}
