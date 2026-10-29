//! `topology-view-staleness` and `cadences`: the staleness floor and gossip cadences read and shown
//! on a stale node (`docs/architecture/node-identity-and-state.md:1`,
//! `docs/architecture/gossip.md:488`, `crates/rdm-core/src/cadence.rs:1`).
//!
//! RDM reads the three fabric cadences once from the environment (`RDM_STALENESS_MS`,
//! `RDM_GOSSIP_INTERVAL_MS`, `RDM_BACKBONE_INTERVAL_MS`):
//! - [`staleness_floor`] (default 30 s): how long a member stays heard without a fresh digest;
//!   past it, the node's pruner marks the member [`NodeStatus::PendingReconnect`] (soft and
//!   reversible, never death).
//! - [`gossip_interval`] (default 2 s): how often a member publishes its mesh digest.
//! - [`backbone_gossip_interval`] (default 5 s): how often a mesh-primary publishes on the backbone.
//!
//! Build: `cargo build --example topology_view_staleness` in `demo/`.
//! Run against a live estate (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/topology_view_staleness [<path.name>]
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{await_fact, Filter, MeshNode, NodeStatus, NodeView, Nodes, PathName, Peers};
use rdm_core::cadence::{backbone_gossip_interval, gossip_interval, staleness_floor};
use rdm_core::{NodeKind, RuntimeFact, RuntimeLocator};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{adopt, DeploymentProvider, TerminationMode};
use std::path::Path;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-topology-view-staleness");
    println!(
        "cadences: staleness_floor={:?} gossip_interval={:?} backbone_gossip_interval={:?}",
        staleness_floor(),
        gossip_interval(),
        backbone_gossip_interval()
    );

    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let entry = birth.name.clone();
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let nodes = Nodes::new(peers.carrier_at(entry.clone()));

    let all = nodes.get(&Filter::new()).await?;
    let target: PathName = match std::env::args().nth(1) {
        Some(arg) => arg.parse().map_err(|e| anyhow!("{arg}: {e}"))?,
        None => all
            .iter()
            .find(|n| n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
            .map(|n| n.name.clone())
            .ok_or_else(|| anyhow!("no Ready role node found in estate"))?,
    };
    let before: NodeView = all
        .into_iter()
        .find(|n| n.name == target)
        .ok_or_else(|| anyhow!("no row holds {target}"))?;
    let data_dir = before.data_dir.as_deref().ok_or_else(|| anyhow!("{target} has no data_dir"))?;
    let fact = RuntimeFact::read_record(Path::new(data_dir))
        .ok_or_else(|| anyhow!("no runtime.json in {data_dir}"))?
        .map_err(|e| anyhow!(e))?;
    let RuntimeLocator::Process { pid, .. } = fact.locator else {
        return Err(anyhow!("{target} is not a host process runtime"));
    };
    println!(
        "before: node={} node_id={} incarnation={:?} status={:?} pid={pid}",
        before.name, before.node_id, before.incarnation_id, before.status
    );

    // The member goes silent: its runtime ends through the deployment provider, as an application's own
    // tooling would end it (the demo starts, signals or removes no process itself). Its digests stop, and
    // silence is all the view sees.
    let provider = ProcessDeploymentProvider::new();
    let handle = adopt(&provider, &fact).map_err(|e| anyhow!(e))?;
    provider.terminate(&handle, TerminationMode::Immediate).await.map_err(|e| anyhow!("{e}"))?;
    let target_filter = Filter::new()
        .eq("path", [target.path()])
        .eq("name", [target.name()]);
    let wait_bound = staleness_floor() * 3 + Duration::from_secs(15);
    let stale_res = await_fact(
        "node",
        &entry.to_string(),
        "member marked PendingReconnect past staleness_floor",
        wait_bound,
        || async {
            nodes
                .get(&target_filter)
                .await?
                .into_iter()
                .find(|n| n.name == target)
                .ok_or_else(|| rdm_client::CallEnd::NotFound {
                    read: "nodes.get",
                    detail: format!("no row for {target}"),
                })
        },
        |n| n.status == NodeStatus::PendingReconnect,
        |n| format!("status={:?}", n.status),
    )
    .await;
    let stale = stale_res?;
    println!(
        "stale past staleness_floor: node={} node_id={} incarnation={:?} status={:?}",
        stale.name, stale.node_id, stale.incarnation_id, stale.status
    );

    let resumed = await_fact(
        "node",
        &entry.to_string(),
        "the silent member's path is Ready again as a new birth (node recovery)",
        wait_bound,
        || async {
            nodes
                .get(&target_filter)
                .await?
                .into_iter()
                .find(|n| n.name == target)
                .ok_or_else(|| rdm_client::CallEnd::NotFound {
                    read: "nodes.get",
                    detail: format!("no row for {target}"),
                })
        },
        |n| n.status == NodeStatus::Ready && n.node_id != before.node_id,
        |n| format!("status={:?} node_id={}", n.status, n.node_id),
    )
    .await?;
    println!(
        "recovered after the silence: node={} node_id={} incarnation={:?} status={:?}",
        resumed.name, resumed.node_id, resumed.incarnation_id, resumed.status
    );
    Ok(())
}
