//! Fabric-primary takeover and complete-view recomputation (`docs/architecture/fabric-primary.md:208`,
//! §8–§9, §11; `rdm_admin::fabric_handover`): a successor fabric-primary hydrates the immutable Fabric
//! identity, the current Build's required Mesh set, and the complete Mesh-status view before publishing
//! the Fabric lifecycle state, without minting a new Build.
//!
//! When the fabric-primary changes, the successor reads `expected` from the accepted topology Build
//! (`FabricView::build_id`), combines its own Mesh state with peer Mesh statuses (`mesh.get`), and
//! holds publication if any required Mesh is missing (§8.3). Once `expected == observed`, it publishes
//! the Fabric status under its own identity (`FabricView::fabric_primary`), keeping `FabricView::id`
//! and `FabricView::build_id` unchanged (§9, §11).
//!
//! Build: `cargo build --example fabric_primary_takeover` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/fabric_primary_takeover
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{BuildId, Fabrics, Filter, MeshNode, Meshes, Nodes, PathName, Peers, ScopeStatus};
use rdm_core::handover::NodePrimaryUpdateReply;
use std::collections::BTreeSet;
use std::time::Duration;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode =
        serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-fabric-primary-takeover");
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let rows = Nodes::new(carrier.clone()).get(&Filter::new()).await?;
    peers.learn_known(&rows);

    let before = Fabrics::new(carrier.clone())
        .get(&Filter::new())
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("fabric.get answered no fabric"))?;
    let before_fp = before
        .fabric_primary
        .clone()
        .ok_or_else(|| anyhow!("fabric.get names no fabric-primary"))?;
    let build_id = BuildId::parse(
        before
            .build_id
            .as_deref()
            .ok_or_else(|| anyhow!("fabric.get names no current Build"))?,
    )
    .map_err(|e| anyhow!("current Build id: {e}"))?;
    let attempt = carrier.build(&build_id).await?.attempt;
    let expected: BTreeSet<String> = before.meshes.iter().map(|m| m.name.clone()).collect();
    println!(
        "before takeover: fabric {} ({}) {:?}, primary {}, Build {}, expected meshes {expected:?}",
        before.name,
        before.id,
        before.status,
        before_fp,
        build_id.log()
    );

    let fp_carrier = peers.carrier_at(before_fp.clone());
    let to_id = match Nodes::new(fp_carrier).fabric_primary_update(&before.name, &before_fp, &build_id, attempt).await? {
        NodePrimaryUpdateReply::PrimaryUpdated { revisions } | NodePrimaryUpdateReply::AlreadyApplied { revisions } => {
            revisions.iter().find(|(t, _, _, _)| t == rdm_core::handover::FABRIC).map(|(_, _, to, _)| to.node_id.clone()).ok_or_else(|| anyhow!("no fabric entry in revisions"))?
        }
        other => return Err(anyhow!("node.fabric-primary.update at {before_fp}: {other:?}")),
    };
    let after_fp = rows
        .iter()
        .find(|n| n.node_id == to_id)
        .map(|n| n.name.clone())
        .ok_or_else(|| anyhow!("no row holds successor {to_id}"))?;

    let succ_carrier = peers.carrier_at(after_fp.clone());
    let after = Fabrics::new(succ_carrier.clone())
        .fact("successor fabric-primary hydrated complete view", Duration::from_secs(30), |f| {
            f.fabric_primary.as_ref() == Some(&after_fp) && f.status == ScopeStatus::Ready
        })
        .await?;
    let meshes = Meshes::new(succ_carrier.clone()).get(&Filter::new()).await?;
    let observed: BTreeSet<String> = meshes
        .iter()
        .filter(|m| m.status == ScopeStatus::Ready)
        .map(|m| m.name.clone())
        .collect();
    let missing: BTreeSet<String> = expected.difference(&observed).cloned().collect();
    println!(
        "after takeover at {}: fabric_id={} (same={}), Build={:?} (same={}), status={:?}",
        after_fp,
        after.id,
        after.id == before.id,
        after.build_id,
        after.build_id == before.build_id,
        after.status
    );
    println!("complete-view gate: expected={expected:?}, observed={observed:?}, missing={missing:?}");
    for m in &meshes {
        println!("  mesh {} ({:?}): {:?}, mesh-primary {:?}", m.name, m.id, m.status, m.primary_admin);
    }

    // Move the fabric-primary back to the initial node-admin so the estate returns to its starting primary.
    let _ = Nodes::new(succ_carrier)
        .fabric_primary_update(&before.name, &after_fp, &build_id, attempt)
        .await?;
    let restored = Fabrics::new(peers.carrier_at(before_fp.clone()))
        .fact("fabric-primary returned to initial node-admin", Duration::from_secs(30), |f| {
            f.fabric_primary.as_ref() == Some(&before_fp) && f.status == ScopeStatus::Ready
        })
        .await?;
    println!("returned: fabric-primary is {:?} ({:?})", restored.fabric_primary, restored.status);
    Ok(())
}
