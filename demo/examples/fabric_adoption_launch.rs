//! Fabric adoption launch (`docs/architecture/lifecycles/fabric-recovery.md:170`): adopt a fabric from
//! its shared store root on a fresh data directory when no node-admin is alive and the fabric-primary's
//! data directory is gone too (`rdm_admin::AdminConfig::from_env`, `AdminConfig::adopt`).
//!
//! When no node-admin lives, a person or a script launches one node-admin at the lost fabric-primary's
//! `path.name` with no launcher (`RDM_NODE_ID` and `RDM_LAUNCHER` unset), given `RDM_NODE_NAME`,
//! `RDM_FABRIC_ID`, `RDM_MESH` and `RDM_STORE_DIR`. `AdminConfig::from_env` tells that adoption launch
//! from Day 0 by `RDM_NODE_NAME` alone: it sets `adopt: Some(<path.name>)`, reuses the fabric id
//! `RDM_FABRIC_ID` names without minting one, and mints a fresh `node_id` for the new birth. A recovery
//! flag (`RDM_FABRIC_PRIMARY`, `RDM_MESH_PRIMARY`) or a non-admin `path.name` is refused by name.
//!
//! Build: `cargo build --example fabric_adoption_launch` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/fabric_adoption_launch
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::admin::AdminConfig;
use rdm_client::{Fabrics, Filter, MeshNode, Nodes, PathName, Peers};
use std::collections::BTreeMap;
use std::path::Path;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode =
        serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-fabric-adoption-launch");
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);

    let fabric = Fabrics::new(carrier.clone())
        .get(&Filter::new())
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("fabric.get answered no fabric"))?;
    let rows = Nodes::new(carrier).get(&Filter::new()).await?;
    let fp = rows
        .iter()
        .find(|n| n.is_fabric_primary)
        .ok_or_else(|| anyhow!("no row names the fabric-primary"))?;
    let store_dir = fp
        .data_dir
        .as_deref()
        .and_then(|d| Path::new(d).parent())
        .map(|root| root.join("store"))
        .unwrap_or_else(|| std::env::temp_dir().join("rdm-store"));
    let fresh_data_dir = std::env::temp_dir().join(format!("adopt-{}", fp.name));

    let base: BTreeMap<&str, String> = BTreeMap::from([
        ("RDM_FABRIC", fabric.name.clone()),
        ("RDM_FABRIC_ID", fabric.id.to_string()),
        ("RDM_MESH", fp.mesh.clone()),
        ("RDM_NODE_NAME", fp.name.to_string()),
        ("RDM_STORE_DIR", store_dir.display().to_string()),
        ("RDM_DATA_DIR", fresh_data_dir.display().to_string()),
    ]);
    let cfg = AdminConfig::from_env(|k| base.get(k).cloned()).map_err(|e| anyhow!(e))?;
    println!(
        "live fabric {} ({}): lost fabric-primary {} (old node_id {})",
        fabric.name, fabric.id, fp.name, fp.node_id
    );
    println!(
        "adoption launch config: adopt={:?}, fabric_id={} (named={}), mesh={}, new node_id={}",
        cfg.adopt.as_ref().map(|p| p.to_string()),
        cfg.fabric_id,
        cfg.fabric_id_named,
        cfg.mesh,
        cfg.node_id
    );

    // A recovery flag alongside RDM_NODE_NAME is refused by name.
    let mut with_flag = base.clone();
    with_flag.insert("RDM_FABRIC_PRIMARY", "true".into());
    match AdminConfig::from_env(|k| with_flag.get(k).cloned()) {
        Err(refusal) => println!("with RDM_FABRIC_PRIMARY=true: refused ({refusal})"),
        Ok(_) => return Err(anyhow!("expected RDM_FABRIC_PRIMARY with RDM_NODE_NAME to be refused")),
    }

    // A non-admin path.name in RDM_NODE_NAME is refused by name.
    let mut non_admin = base;
    non_admin.insert("RDM_NODE_NAME", format!("{}.broker.1", fp.mesh));
    match AdminConfig::from_env(|k| non_admin.get(k).cloned()) {
        Err(refusal) => println!("with non-admin RDM_NODE_NAME: refused ({refusal})"),
        Ok(_) => return Err(anyhow!("expected non-admin RDM_NODE_NAME to be refused")),
    }
    Ok(())
}
