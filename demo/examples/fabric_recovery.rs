//! Fabric recovery (`docs/architecture/lifecycles/fabric-recovery.md:1`): restore the fabric when the
//! fabric-primary is lost while another node-admin lives (`rdm_admin::workflows::fabric_recovery`).
//!
//! Unheard past the staleness floor, the lost fabric-primary is taken by nobody: the lowest ready
//! node-admin of its mesh (or, when its mesh has none, the lowest ready mesh-primary on the backbone)
//! runs one workflow without taking a primary role itself. Node investigation decides what becomes of
//! the lost birth; on `exited`, node recovery re-creates the node-admin at its `path.name` over its
//! inherited data directory as an attempt of the current Build (`node.delete:<path.name>`, then
//! `node.create:<path.name>`). Once the new birth at that `path.name` is ready, the executor restores it
//! as both mesh-primary and fabric-primary (`node.primary.update` carrying both records at their next
//! revisions).
//!
//! Build: `cargo build --example fabric_recovery` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/fabric_recovery
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{adopt, DeploymentProvider, TerminationMode};
use rdm_client::{
    await_fact, Builds, Fabrics, Filter, MeshNode, NodeStatus, NodeView, Nodes, PathName, Peers,
    ScopeStatus,
};
use rdm_core::{OperationSubject, RuntimeFact};
use std::path::Path;
use std::time::Duration;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode =
        serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

fn adopt_birth(row: &NodeView) -> Result<(ProcessDeploymentProvider, rdm_admin::deployment::provider::DeploymentHandle)> {
    let dir = row.data_dir.as_deref().ok_or_else(|| anyhow!("path {:?}, name {:?} carries no data_dir", row.name.path, row.name.name))?;
    let fact = RuntimeFact::read_record(Path::new(dir)).ok_or_else(|| anyhow!("no runtime.json in {dir}"))?.map_err(|e| anyhow!(e))?;
    let provider = ProcessDeploymentProvider::new();
    let handle = adopt(&provider, &fact).map_err(|e| anyhow!(e))?;
    Ok((provider, handle))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-fabric-recovery");
    let (peers, entry) = enter().await?;
    let rows = Nodes::new(peers.carrier_at(entry)).get(&Filter::new()).await?;
    peers.learn_known(&rows);

    let fp = rows.iter().find(|n| n.is_fabric_primary).ok_or_else(|| anyhow!("no row names the fabric-primary"))?.clone();
    let executor = rows
        .iter()
        .find(|n| n.r#type == "node-admin" && n.mesh == fp.mesh && n.node_id != fp.node_id && n.status == NodeStatus::Ready)
        .or_else(|| rows.iter().find(|n| n.r#type == "node-admin" && n.mesh != fp.mesh && n.is_primary && n.status == NodeStatus::Ready))
        .ok_or_else(|| anyhow!("no surviving ready node-admin to run fabric recovery"))?
        .clone();

    let (provider, handle) = adopt_birth(&fp)?;
    println!("before: fabric-primary path {:?}, name {:?} (node_id {}, incarnation {:?}, pid {:?}); executor path {:?}, name {:?}", fp.name.path, fp.name.name, fp.node_id, fp.incarnation_id, handle.pid, executor.name.path, executor.name.name);
    provider.terminate(&handle, TerminationMode::Immediate, rdm_admin::deployment::deadline::Deadline::step()).await.map_err(|e| anyhow!("{e}"))?;
    println!("terminated path {:?}, name {:?}; awaiting fabric recovery via path {:?}, name {:?}", fp.name.path, fp.name.name, executor.name.path, executor.name.name);

    let obs_nodes = Nodes::new(peers.carrier_at(executor.name.clone()));
    let (fp_name, old_id) = (fp.name.clone(), fp.node_id.clone());
    let filter = Filter::new().eq("mesh", [fp.mesh.clone()]);
    let recovered = await_fact(
        "node",
        &format!("path {:?}, name {:?}", executor.name.path, executor.name.name),
        "re-created fabric-primary ready and restored",
        Duration::from_secs(90),
        || obs_nodes.get(&filter),
        |list| list.iter().any(|n| n.name == fp_name && n.node_id != old_id && n.status == NodeStatus::Ready && n.is_primary && n.is_fabric_primary),
        |list| list.iter().find(|n| n.name == fp_name).map(|n| format!("path {:?}, name {:?} {:?} node_id={} primary={} fabric_primary={}", n.name.path, n.name.name, n.status, n.node_id, n.is_primary, n.is_fabric_primary)).unwrap_or_else(|| format!("no path {:?}, name {:?}", fp_name.path, fp_name.name)),
    )
    .await?;
    peers.learn_known(&recovered);
    let restored = recovered.iter().find(|n| n.name == fp.name && n.node_id != old_id).ok_or_else(|| anyhow!("no row holds path {:?}, name {:?} at a new node_id", fp.name.path, fp.name.name))?;
    println!("restored: path {:?}, name {:?} {:?}, new node_id {}, incarnation {:?}, is_primary {}, is_fabric_primary {}", restored.name.path, restored.name.name, restored.status, restored.node_id, restored.incarnation_id, restored.is_primary, restored.is_fabric_primary);

    let fabric = Fabrics::new(peers.carrier_at(restored.name.clone()))
        .fact("fabric ready under restored primary", Duration::from_secs(30), |f| f.status == ScopeStatus::Ready && f.fabric_primary.as_ref() == Some(&restored.node_id))
        .await?;
    println!("fabric {} ({}): {:?}, fabric-primary {:?}", fabric.name, fabric.id, fabric.status, fabric.fabric_primary);

    let current = Builds::new(peers.carrier_at(restored.name.clone())).get(&Filter::new().eq("current", ["true"])).await?;
    if let Some(b) = current.first() {
        // A receipt on the fabric-primary's path: its location, or the lost or the re-created node.
        let on_fp = |subject: &Option<OperationSubject>| match subject {
            Some(OperationSubject::Location(p)) => *p == fp.name,
            Some(OperationSubject::Node(id)) | Some(OperationSubject::Birth { node_id: id, .. }) => *id == old_id || *id == restored.node_id,
            _ => false,
        };
        let ops: Vec<String> = b.steps.iter().filter(|s| on_fp(&s.operation.subject) && s.step == "Complete").map(|s| format!("attempt {} {:?}", s.attempt, s.operation)).collect();
        println!("current Build {} ({:?}): recovery receipts {ops:?}", b.build_id.log(), b.phase);
    }
    Ok(())
}
