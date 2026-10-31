//! Node recovery (`docs/architecture/lifecycles/node-recovery.md:1`): reconcile an unheard or exited
//! member over its inherited data directory (`rdm_admin::workflows::node_recovery`).
//!
//! A silent member is never restarted or deleted for being silent: its mesh-primary marks it
//! `PendingReconnect` at the staleness floor, runs the reach ladder `node.tickle` half a floor later
//! (`tickle_ladder_at_default_budget()`, direct then `VIA_PEER_TICKLE_FANOUT` live peers at once), and
//! on `unreached` inspects the birth's runtime (`node.runtime.inspect`). On `exited`, the mesh-primary
//! opens a recovery attempt of the current Build on its mesh's rows and runs `node.delete:<path.name>`
//! followed by `node.create:<path.name>` over the inherited `data_dir`, minting a new `node_id` and
//! `incarnation_id` while the current Build stays `Complete`.
//!
//! Build: `cargo build --example node_recovery` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_recovery [<path.name>]
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{adopt, inspect_runtime, DeploymentProvider, TerminationMode};
use rdm_admin::workflows::node_recovery::{tickle_ladder_at_default_budget, VIA_PEER_TICKLE_FANOUT};
use rdm_client::{await_fact, Builds, Filter, MeshNode, NodeStatus, Nodes, PathName, Peers};
use rdm_core::{NodeKind, RuntimeFact};
use std::path::Path;
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
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-recovery");
    println!(
        "node.tickle budget: {:?} across direct + {} peer carrier(s)",
        tickle_ladder_at_default_budget(),
        VIA_PEER_TICKLE_FANOUT
    );

    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let nodes = Nodes::new(carrier.clone());
    let rows = nodes.get(&Filter::new()).await?;
    peers.learn_known(&rows);

    let target = match std::env::args().nth(1) {
        Some(arg) => {
            let name: PathName = arg.parse().map_err(|e| anyhow!("{arg}: {e}"))?;
            rows.iter()
                .find(|n| n.name == name)
                .ok_or_else(|| anyhow!("no row holds {name}"))?
                .clone()
        }
        None => rows
            .iter()
            .find(|n| n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
            .ok_or_else(|| anyhow!("no ready role node in the estate"))?
            .clone(),
    };
    let dir = target
        .data_dir
        .as_deref()
        .ok_or_else(|| anyhow!("{} carries no data_dir", target.name))?;
    let incarnation = target
        .incarnation_id
        .as_ref()
        .ok_or_else(|| anyhow!("{} carries no incarnation", target.name))?;
    let fact = RuntimeFact::read_record(Path::new(dir))
        .ok_or_else(|| anyhow!("no runtime.json in {dir}"))?
        .map_err(|e| anyhow!(e))?;
    let provider = ProcessDeploymentProvider::new();
    let handle = adopt(&provider, &fact).map_err(|e| anyhow!(e))?;
    let before_inspect = inspect_runtime(&provider, &fact, Some(dir), incarnation.as_str()).await;
    println!(
        "before: {} {:?}, node_id {}, incarnation {}, data_dir {dir}, inspect {before_inspect:?}",
        target.name, target.status, target.node_id, incarnation
    );

    provider
        .terminate(&handle, TerminationMode::Immediate, rdm_admin::deployment::deadline::Deadline::step())
        .await
        .map_err(|e| anyhow!("{e}"))?;
    let after_inspect = inspect_runtime(&provider, &fact, Some(dir), incarnation.as_str()).await;
    println!("terminated {}: inspect is now {after_inspect:?}; awaiting node recovery", target.name);

    let (target_name, old_id) = (target.name.clone(), target.node_id.clone());
    let filter = Filter::new().eq("mesh", [target_name.mesh.clone()]);
    let recovered = await_fact(
        "node",
        "fabric-primary",
        "recovered birth ready at path.name",
        Duration::from_secs(90),
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
    let row = recovered
        .iter()
        .find(|n| n.name == target.name)
        .ok_or_else(|| anyhow!("no row holds {}", target.name))?;
    println!(
        "recovered: {} {:?}, new node_id {}, incarnation {:?}, inherited data_dir {:?} (same={})",
        row.name,
        row.status,
        row.node_id,
        row.incarnation_id,
        row.data_dir,
        row.data_dir == target.data_dir
    );

    let current = Builds::new(carrier).get(&Filter::new().eq("current", ["true"])).await?;
    if let Some(b) = current.first() {
        let ops: Vec<String> = b
            .steps
            .iter()
            .filter(|s| s.operation.ends_with(&format!(":{}", target.name)) && s.step == "Complete")
            .map(|s| format!("attempt {} {}", s.attempt, s.operation))
            .collect();
        println!("current Build {} ({:?}): receipts for {}: {ops:?}", b.build_id.log(), b.phase, target.name);
    }
    Ok(())
}
