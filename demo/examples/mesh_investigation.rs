//! Peer-mesh investigation (`docs/architecture/lifecycles/mesh-investigation.md:1`): the fabric-primary's
//! reachability ladder over an unheard peer mesh (`rdm_admin::workflows::mesh_investigation`).
//!
//! When a peer mesh goes unheard on the backbone, the fabric-primary tracks elapsed time `t` since its
//! last direct backbone receipt from that mesh (`Rungs::configured()`): `probe 1` at `2/3` of the
//! staleness floor (each node-admin probed direct, then `Forward { ProbeNodeState }` through up to two
//! live members of that mesh at once), the silent `mark` at `1` floor, `probe 2` at `4/3` floor, and the
//! `decide` step at `2` floors. When a live member answers the carried probe with `connection-lost`
//! (it cannot reach its own node-admin), `Step::Decide` recovers the mesh's node-admins as an attempt of
//! the current Build; once the new mesh-primary reports its checklist complete, the fabric returns from
//! `degraded` to `ready`.
//!
//! Build: `cargo build --example mesh_investigation` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/mesh_investigation
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{adopt, DeploymentProvider, TerminationMode};
use rdm_admin::workflows::mesh_investigation::{Ladder, ProbeOutcome, Rungs, Step};
use rdm_client::{
    await_fact, Fabrics, Filter, MeshNode, NodeStatus, NodeView, Nodes, PathName, Peers,
    ScopeStatus,
};
use rdm_core::{NodeId, NodeKind, RuntimeFact};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode =
        serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

async fn terminate_birth(provider: &ProcessDeploymentProvider, row: &NodeView) -> Result<u32> {
    let dir = row.data_dir.as_deref().ok_or_else(|| anyhow!("{} carries no data_dir", row.name))?;
    let fact = RuntimeFact::read_record(Path::new(dir)).ok_or_else(|| anyhow!("no runtime.json in {dir}"))?.map_err(|e| anyhow!(e))?;
    let handle = adopt(provider, &fact).map_err(|e| anyhow!(e))?;
    let pid = handle.pid.ok_or_else(|| anyhow!("{} has no pid", row.name))?;
    provider.terminate(&handle, TerminationMode::Immediate, rdm_admin::deployment::deadline::Deadline::step()).await.map_err(|e| anyhow!("{e}"))?;
    Ok(pid)
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-mesh-investigation");
    let rungs = Rungs::configured();
    println!(
        "configured rungs: track={:?}, probe1={:?}, mark={:?}, probe2={:?}, decide={:?}",
        rungs.track, rungs.probe1, rungs.mark, rungs.probe2, rungs.decide
    );
    let mut ladder = Ladder::new(rungs);
    for (at, outcome) in [
        (rungs.probe1, Some(ProbeOutcome::ConnectionLost)),
        (rungs.mark, None),
        (rungs.probe2, Some(ProbeOutcome::ConnectionLost)),
        (rungs.decide, None),
    ] {
        for step in ladder.step("peer-mesh", at) {
            println!("  ladder at {at:?}: {step:?}");
            if let (Step::Probe { n, unheard_ms, ticket }, Some(out)) = (step, outcome) {
                let _ = ladder.probed("peer-mesh", ticket, n, out, unheard_ms);
            }
        }
    }

    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let rows = Nodes::new(carrier.clone()).get(&Filter::new()).await?;
    peers.learn_known(&rows);
    let fp = rows.iter().find(|n| n.is_fabric_primary).ok_or_else(|| anyhow!("no fabric-primary"))?;
    let peer_mesh = rows
        .iter()
        .find(|n| n.mesh != fp.mesh)
        .map(|n| n.mesh.clone())
        .ok_or_else(|| anyhow!("no peer mesh outside {}", fp.mesh))?;
    let peer_admins: Vec<NodeView> = rows
        .iter()
        .filter(|n| n.mesh == peer_mesh && n.kind == NodeKind::NodeAdmin)
        .cloned()
        .collect();
    let carriers: Vec<String> = rows
        .iter()
        .filter(|n| n.mesh == peer_mesh && n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
        .take(3)
        .map(|n| n.name.to_string())
        .collect();

    let provider = ProcessDeploymentProvider::new();
    let mut old_ids: BTreeMap<PathName, NodeId> = BTreeMap::new();
    for admin in &peer_admins {
        let pid = terminate_birth(&provider, admin).await?;
        old_ids.insert(admin.name.clone(), admin.node_id.clone());
        println!("terminated {} (node_id {}, pid {pid}); live carriers in {peer_mesh}: {carriers:?}", admin.name, admin.node_id);
    }

    let fp_carrier = peers.carrier_at(fp.name.clone());
    let fp_nodes = Nodes::new(fp_carrier.clone());
    let filter = Filter::new().eq("mesh", [peer_mesh.clone()]);
    let recovered = await_fact(
        "node",
        &fp.name.to_string(),
        "peer-mesh node-admins re-created and ready",
        Duration::from_secs(120),
        || fp_nodes.get(&filter),
        |list| {
            old_ids.iter().all(|(name, old_id)| {
                list.iter().any(|n| &n.name == name && &n.node_id != old_id && n.status == NodeStatus::Ready)
            })
        },
        |list| {
            list.iter()
                .filter(|n| n.kind == NodeKind::NodeAdmin)
                .map(|n| format!("{} {:?} ({})", n.name, n.status, n.node_id))
                .collect::<Vec<_>>()
                .join(", ")
        },
    )
    .await?;
    for n in recovered.iter().filter(|n| n.kind == NodeKind::NodeAdmin) {
        println!("recovered {}: {:?}, new node_id {}, incarnation {:?}, is_primary {}", n.name, n.status, n.node_id, n.incarnation_id, n.is_primary);
    }

    let fabric = Fabrics::new(fp_carrier)
        .fact("fabric restored to ready after peer-mesh recovery", Duration::from_secs(60), |f| {
            f.status == ScopeStatus::Ready
        })
        .await?;
    println!("fabric {} ({}): {:?}", fabric.name, fabric.id, fabric.status);
    Ok(())
}
