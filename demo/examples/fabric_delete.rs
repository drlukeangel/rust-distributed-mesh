//! `fabric.delete`: full fabric teardown as a Rust caller (`docs/architecture/lifecycles/fabric-delete.md:1`,
//! `crates/rdm-admin/src/workflows/fabric_delete.rs:6`, `crates/rdm-core/src/build.rs:140`).
//!
//! `fabric.delete` runs as one Build (`Change::DeleteFabric`) driven by the fabric-primary:
//! 1. `mesh.delete` on each mesh leaf-inward, the fabric-primary's own mesh last (keeping the
//!    fabric-primary seat on its own mesh);
//! 2. `node.delete` of each other mesh's final mesh-primary after its `mesh.deleted`;
//! 3. `fabric.deleted` (`BuildFrame::Complete`) reported by the fabric-primary while still alive;
//! 4. a separate `node.delete` (`Nodes::delete`) of the fabric-primary from outside the fabric,
//!    whose `NodeEvent::Deleted` is its last reply before its runtime exits.
//!
//! Build: `cargo build --example fabric_delete` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/fabric_delete
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{
    BuildFrame, Change, Fabrics, Filter, Frame, MeshNode, NodeEvent, Nodes, Peers,
};

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-fabric-delete");
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let entry = birth.name.clone();
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let carrier = peers.carrier_at(entry.clone());

    let fabric = Fabrics::new(carrier.clone())
        .get(&Filter::new())
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("fabrics.get returned no fabric"))?;
    println!(
        "before fabric.delete: fabric={} ({}) status={:?} fabric_primary={:?} meshes={}",
        fabric.name,
        fabric.id,
        fabric.status,
        fabric.fabric_primary,
        fabric.meshes.len()
    );

    let mut stream = carrier.create(Change::DeleteFabric).await?;
    let build_id = stream.build_id().clone();
    println!(
        "fabric.delete Build started: build_id={build_id} attempt={} disposition={:?}",
        stream.attempt(),
        stream.disposition()
    );
    let mut step_count = 0usize;
    let mut completed = false;
    while let Some(frame) = stream.next().await {
        match frame? {
            BuildFrame::Step { attempt, operation, step } => {
                step_count += 1;
                if step == "mesh.deleted" || step == "fabric.deleted" || operation.starts_with("mesh.delete:") {
                    println!("  step #{step_count} (attempt {attempt}): {operation} -> {step}");
                }
            }
            BuildFrame::Blocked { attempt, operation, step, reason } => {
                println!("  blocked (attempt {attempt}): {operation}/{step} ({reason})");
            }
            BuildFrame::Complete { attempt } => {
                println!(
                    "fabric.delete Build {build_id} Complete on attempt {attempt} ({step_count} step frames)"
                );
                completed = true;
                break;
            }
            other => return Err(anyhow!("unexpected BuildFrame in fabric.delete: {other:?}")),
        }
    }
    assert!(completed, "fabric.delete Build must reach BuildFrame::Complete");

    // Step 4 (fabric-delete.md): separate node.delete of the surviving fabric-primary from outside.
    let mut del_stream = Nodes::new(carrier).delete(&entry).await?;
    let mut deleted = false;
    while let Some(frame) = del_stream.next().await {
        match frame {
            Ok(f) => {
                println!("node.delete {entry} frame: {f:?}");
                if f == Frame::Event(NodeEvent::Deleted) {
                    deleted = true;
                }
                if f.is_terminal() {
                    break;
                }
            }
            Err(e) => {
                println!("node.delete {entry} stream closed after final reply: {e}");
                break;
            }
        }
    }
    assert!(deleted, "fabric-primary must emit NodeEvent::Deleted as its final reply");
    println!("fabric-primary {entry} deleted: fabric teardown complete");
    Ok(())
}
