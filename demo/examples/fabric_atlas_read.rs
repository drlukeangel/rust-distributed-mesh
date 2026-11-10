//! The fabric-wide reads: `fabric.get` (the fabric, its meshes and their primaries) and
//! `fabric.atlas.get` (the topology plus what is in flight, Q439).
//!
//! `fabric.get` answers from the admin it is sent to: the fabric's status, provider, fabric-primary
//! and current Build, and each mesh's status and mesh-primary. The atlas is laid out the same way: the
//! fabric with `fabric.get`'s fields and the current Build and its phase, then each mesh with
//! `mesh.get`'s fields and its nodes. Each node is its topology row read from its record (birth, type,
//! endpoint, the status the operations left it in), beside what is in flight on it: whether it is a
//! stopped birth waiting for a start or a delete, and the operation accepted on it that has not
//! completed (`accepted`). `observed` is the status the answering admin's gossip view holds for the
//! same birth: context only, never what an acceptance reads. An application reads the atlas for a
//! fabric-wide picture of its nodes, for example to show which node an operation is still running on;
//! `mesh.atlas.get` (`Meshes::atlas_get`) answers the same shape for the meshes it names.
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
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.location()))
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
    let atlas = Fabrics::new(peers.carrier_for(fabric_primary)).atlas_get().await?;
    for f in &atlas.fabrics {
        let build = f.build.as_ref().map(|b| format!("Build {} {:?}", b.build_id, b.phase)).unwrap_or_else(|| "no Build".into());
        println!("atlas: fabric {} ({}), {build}", f.name, f.id);
        for m in &f.meshes {
            println!("  mesh {}: {:?}, mesh-primary {:?}", m.name, m.status, m.primary_admin.as_ref().map(|p| p.to_string()));
            for n in &m.nodes {
                let pending = n.accepted.as_ref().map(|op| format!(", {} running", op.action)).unwrap_or_default();
                println!("    {} path {:?}, name {:?} {:?}{}{pending} (gossip view: {:?})", n.node.r#type, n.node.path, n.node.name, n.node.status, if n.stopped { " stopped" } else { "" }, n.observed);
            }
        }
    }
    Ok(())
}
