//! Lost data-dir node recovery: `docs/architecture/lifecycles/node-recovery.md:145`
//! (`crates/rdm-admin/src/admin/records.rs:16`).
//!
//! Terminates a member's runtime process and removes its inherited `data_dir`,
//! then waits for the mesh-primary to hold across `data_dir_gone_floors()`
//! staleness floors (`data_dir_gone_hold_ms()`), record `data-dir-lost`, and
//! recover the member at the same `path.name` over a newly allocated
//! `data_dir` with a newly minted `endpoint_id`, `node_id`, and
//! `incarnation_id`.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use rdm_admin::admin::{
    DEFAULT_DATA_DIR_GONE_FLOORS, ENV_DATA_DIR_GONE_FLOORS, data_dir_gone_floors,
    data_dir_gone_hold_ms,
};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{DeploymentProvider, TerminationMode, adopt};
use rdm_client::{Filter, MeshNode, NodeStatus, Nodes, PathName, Peers, await_fact};
use rdm_core::{NodeKind, RuntimeFact};

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode =
        serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-data-dir-lost-recovery");
    println!(
        "{}={:?} (default {}), effective floors={}, hold={}ms",
        ENV_DATA_DIR_GONE_FLOORS,
        std::env::var(ENV_DATA_DIR_GONE_FLOORS).ok(),
        DEFAULT_DATA_DIR_GONE_FLOORS,
        data_dir_gone_floors(),
        data_dir_gone_hold_ms()
    );

    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let nodes = Nodes::new(carrier);
    let rows = nodes.get(&Filter::new()).await?;
    peers.learn_known(&rows);

    let before = match std::env::args().nth(1) {
        Some(arg) => {
            let name: PathName = arg.parse().map_err(|e| anyhow!("{arg}: {e}"))?;
            rows.iter()
                .find(|n| n.name == name && n.status == NodeStatus::Ready)
                .ok_or_else(|| anyhow!("no ready row for {name}"))?
                .clone()
        }
        None => rows
            .iter()
            .find(|n| n.kind != NodeKind::NodeAdmin && !n.is_primary && n.status == NodeStatus::Ready)
            .ok_or_else(|| anyhow!("no ready secondary role node in the estate"))?
            .clone(),
    };
    let target_name = before.name.clone();
    let old_dir = before.data_dir.clone().ok_or_else(|| anyhow!("missing data_dir"))?;
    let fact = RuntimeFact::read_record(Path::new(&old_dir))
        .ok_or_else(|| anyhow!("no runtime.json in {old_dir}"))?
        .map_err(|e| anyhow!(e))?;

    let provider = ProcessDeploymentProvider::new();
    let handle = adopt(&provider, &fact).map_err(|e| anyhow!(e))?;
    println!(
        "before: {} node_id={} inc={:?} endpoint={:?} data_dir={}",
        before.name, before.node_id, before.incarnation_id, before.endpoint_id, old_dir
    );

    provider
        .terminate(&handle, TerminationMode::Immediate, rdm_admin::deployment::deadline::Deadline::step())
        .await
        .map_err(|e| anyhow!("{e}"))?;
    std::fs::remove_dir_all(&old_dir)?;
    println!("terminated {} and removed {}; awaiting recovery over fresh data_dir", before.name, old_dir);

    let filter = Filter::new().eq("mesh", [target_name.mesh.clone()]);
    let old_id = before.node_id.clone();
    let recovered = await_fact(
        "node",
        "fabric-primary",
        "recovered birth ready over newly allocated data_dir",
        Duration::from_secs(120),
        || nodes.get(&filter),
        |list| {
            list.iter().any(|n| {
                n.name == target_name && n.node_id != old_id && n.status == NodeStatus::Ready
            })
        },
        |list| {
            list.iter()
                .find(|n| n.name == target_name)
                .map(|n| format!("{} {:?} ({})", n.name, n.status, n.node_id))
                .unwrap_or_else(|| format!("no {target_name}"))
        },
    )
    .await?;
    let after = recovered
        .iter()
        .find(|n| n.name == target_name)
        .ok_or_else(|| anyhow!("missing {target_name}"))?;
    println!(
        "after:  {} node_id={} inc={:?} endpoint={:?} data_dir={:?} (new_dir={})",
        after.name,
        after.node_id,
        after.incarnation_id,
        after.endpoint_id,
        after.data_dir,
        after.data_dir.as_deref() != Some(old_dir.as_str())
    );
    Ok(())
}
