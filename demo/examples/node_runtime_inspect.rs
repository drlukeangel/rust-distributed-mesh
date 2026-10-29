//! Node runtime inspection: `docs/architecture/lifecycles/node-runtime-inspect.md:1`
//! (`crates/rdm-admin/src/workflows/node_runtime_inspect.rs:6`).
//!
//! Runs `inspect_runtime` against a live member's `RuntimeFact` (`Running`), a
//! fact from a foreign control domain (`Unavailable`), and an exited process
//! locator without and with a matching `ExitRecord` (`Exited(None)` and
//! `Exited(Some(code))`).

use std::path::Path;

use anyhow::{Context, Result, anyhow};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::workflows::node_runtime_inspect::inspect_runtime;
use rdm_client::{Filter, MeshNode, NodeStatus, Nodes, PathName, Peers};
use rdm_core::{NodeKind, RuntimeFact};
use rdm_core::runtime::{ExitRecord, RuntimeLocator, TRANSPORT_STOPPED_EXIT_CODE};

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode =
        serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-runtime-inspect");
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let nodes = Nodes::new(carrier);
    let rows = nodes.get(&Filter::new()).await?;
    peers.learn_known(&rows);

    let target = match std::env::args().nth(1) {
        Some(arg) => {
            let name: PathName = arg.parse().map_err(|e| anyhow!("{arg}: {e}"))?;
            rows.iter().find(|n| n.name == name).ok_or_else(|| anyhow!("missing {name}"))?
        }
        None => rows
            .iter()
            .find(|n| n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
            .ok_or_else(|| anyhow!("no ready role node in the estate"))?,
    };
    let dir = target.data_dir.as_deref().ok_or_else(|| anyhow!("missing data_dir"))?;
    let inc = target.incarnation_id.as_ref().ok_or_else(|| anyhow!("missing incarnation_id"))?;
    let fact = RuntimeFact::read_record(Path::new(dir))
        .ok_or_else(|| anyhow!("no runtime.json in {dir}"))?
        .map_err(|e| anyhow!(e))?;

    let provider = ProcessDeploymentProvider::new();
    let live = inspect_runtime(&provider, &fact, Some(dir), inc.as_str()).await;
    println!("live member {} ({}): {:?}", target.name, inc, live);

    let mut foreign = fact.clone();
    foreign.control_domain = "process:foreign-boot:0".to_string();
    let refused = inspect_runtime(&provider, &foreign, Some(dir), inc.as_str()).await;
    println!("foreign control_domain: {:?}", refused);

    let scratch = std::env::temp_dir().join(format!("rdm-inspect-example-{}", std::process::id()));
    std::fs::create_dir_all(&scratch)?;
    let scratch_str = scratch.to_str().ok_or_else(|| anyhow!("non-utf8 temp path"))?;
    let mut exited_fact = fact.clone();
    exited_fact.locator = RuntimeLocator::Process { pid: u32::MAX - 1, start: 1 };

    let unproven_code = inspect_runtime(&provider, &exited_fact, Some(scratch_str), inc.as_str()).await;
    println!("exited without exit record: {:?}", unproven_code);

    ExitRecord {
        deployment_id: exited_fact.deployment_id.clone(),
        incarnation: inc.as_str().to_string(),
        code: TRANSPORT_STOPPED_EXIT_CODE,
        reason: "transport stopped".to_string(),
    }
    .write(&scratch)
    .map_err(|e| anyhow!(e))?;
    let proven_code = inspect_runtime(&provider, &exited_fact, Some(scratch_str), inc.as_str()).await;
    println!("exited with matching exit record: {:?}", proven_code);

    let _ = std::fs::remove_dir_all(&scratch);
    Ok(())
}
