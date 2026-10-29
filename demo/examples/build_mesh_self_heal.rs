//! `build-mesh-self-heal`: autonomous mesh-primary self-heal claiming a new attempt on the
//! current Build without minting a new Build (`docs/architecture/build.md:289`,
//! `crates/rdm-core/src/build_claim.rs:1`, `crates/rdm-admin/src/workflows/node_recovery.rs:6`).
//!
//! When a member of a peer mesh exits unexpectedly, its mesh-primary proves the exit via
//! `node.tickle` and `node.runtime.inspect`, opens and claims the next attempt in its mesh's drift
//! lane (`Some(mesh)`, R-BI7; `crates/rdm-admin/src/build_state.rs:360-364`), and executes
//! `node.delete:<path.name>` followed by `node.create:<path.name>` under that attempt. No new
//! `BuildId` is minted and the Build stays `Complete` (`docs/architecture/build.md:289`); the
//! fabric-primary's Build log (`FetchBuildFacts`, op `0x1F`) records the mesh lane's attempt and
//! recovery receipts.
//!
//! Build: `cargo build --example build_mesh_self_heal` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/build_mesh_self_heal [<path.name>]
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_admin::build::BuildId;
use rdm_admin::build_facts_read::fetch_build_facts;
use rdm_admin::build_state::{fold, BuildProjection, BuildState};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{adopt, DeploymentProvider, TerminationMode};
use rdm_client::{await_fact, Builds, CallEnd, Filter, MeshNode, NodeStatus, Nodes, PathName, Peers};
use rdm_core::build::BuildPhase;
use rdm_core::{NodeKind, RuntimeFact};
use rdm_transport::NodeTarget;
use std::path::Path;
use std::time::Duration;

async fn read_projection(carrier: &rdm_client::BuildCarrier, id: &BuildId) -> Result<BuildProjection, CallEnd> {
    let target = NodeTarget::CurrentPath(carrier.fabric_primary().clone());
    let fetched = fetch_build_facts(carrier.client(), &target, id)
        .await
        .map_err(|e| CallEnd::Indeterminate {
            code: rdm_core::outcome::Code::Unknown,
            reason: e.to_string(),
        })?;
    fold(&fetched.facts).remove(id).ok_or_else(|| CallEnd::Indeterminate {
        code: rdm_core::outcome::Code::Unknown,
        reason: format!("no projection for {id}"),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-build-mesh-self-heal");
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let entry = birth.name.clone();
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let carrier = peers.carrier_at(entry.clone());
    let nodes = Nodes::new(carrier.clone());
    let builds = Builds::new(carrier.clone());

    let before_build = builds
        .get(&Filter::new().eq("current", ["true"]))
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no current Build"))?;
    assert_eq!(before_build.phase, BuildPhase::Complete);
    let build_id = before_build.build_id.clone();

    let all = nodes.get(&Filter::new()).await?;
    let target = match std::env::args().nth(1) {
        Some(arg) => {
            let name: PathName = arg.parse().map_err(|e| anyhow!("{arg}: {e}"))?;
            all.into_iter().find(|n| n.name == name).ok_or_else(|| anyhow!("no row holds {name}"))?
        }
        None => all
            .into_iter()
            .find(|n| n.mesh != entry.mesh && n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
            .ok_or_else(|| anyhow!("no Ready role node in peer mesh"))?,
    };
    let mesh = target.mesh.clone();
    let before_proj = read_projection(&carrier, &build_id).await?;
    let before_lane_attempt = before_proj.lanes.get(&mesh).map(|l| l.attempt).unwrap_or(0);
    println!(
        "before self-heal: build_id={} phase={:?} fabric_attempt={} mesh_lane_attempt({})={}",
        before_build.build_id, before_build.phase, before_build.attempt, mesh, before_lane_attempt
    );

    let dir = target.data_dir.as_deref().ok_or_else(|| anyhow!("{} has no data_dir", target.name))?;
    let fact = RuntimeFact::read_record(Path::new(dir))
        .ok_or_else(|| anyhow!("no runtime.json in {dir}"))?
        .map_err(|e| anyhow!(e))?;
    let provider = ProcessDeploymentProvider::new();
    let handle = adopt(&provider, &fact).map_err(|e| anyhow!(e))?;
    provider
        .terminate(&handle, TerminationMode::Immediate)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    println!(
        "terminated {} ({}) in peer mesh {}; awaiting mesh-primary self-heal attempt on Build {}",
        target.name, target.node_id, mesh, before_build.build_id
    );

    let old_id = target.node_id.clone();
    let target_name = target.name.clone();
    let filter = Filter::new().eq("path", [target_name.path()]).eq("name", [target_name.name()]);
    let _ = await_fact(
        "node",
        &entry.to_string(),
        "peer mesh member self-healed Ready with new node_id",
        Duration::from_secs(90),
        || nodes.get(&filter),
        |list| list.iter().any(|n| n.name == target_name && n.node_id != old_id && n.status == NodeStatus::Ready),
        |list| format!("{:?}", list.iter().map(|n| (&n.name, n.status, &n.node_id)).collect::<Vec<_>>()),
    )
    .await?;

    let after_proj = await_fact(
        "build log",
        &entry.to_string(),
        "mesh drift lane attempt completed on current Build",
        Duration::from_secs(30),
        || read_projection(&carrier, &build_id),
        |p| p.lanes.get(&mesh).is_some_and(|l| l.attempt > before_lane_attempt && l.state == BuildState::Complete),
        |p| format!("lane={:?}", p.lanes.get(&mesh).map(|l| (l.attempt, l.state, l.steps.len()))),
    )
    .await?;
    let after_build = builds
        .get(&Filter::new().eq("current", ["true"]))
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no current Build"))?;
    assert_eq!(after_build.build_id, before_build.build_id, "self-heal never mints a new BuildId");
    assert_eq!(after_build.phase, BuildPhase::Complete, "the Build stays completed");
    let lane = after_proj.lanes.get(&mesh).expect("mesh drift lane present");
    let ops: Vec<_> = lane
        .steps
        .iter()
        .filter(|r| r.attempt == lane.attempt)
        .map(|r| format!("{}:{}", r.operation, r.step))
        .collect();
    println!(
        "after self-heal: build_id={} (unchanged) phase={:?} lane({})_attempt={} executor={:?} reason={} steps={}",
        after_build.build_id,
        after_build.phase,
        mesh,
        lane.attempt,
        lane.executor,
        lane.reason.as_str(),
        ops.len()
    );
    Ok(())
}
