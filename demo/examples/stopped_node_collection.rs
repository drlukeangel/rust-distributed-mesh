//! Stopped-node garbage collection: `docs/architecture/node-identity-and-state.md:559`
//! (`crates/rdm-admin/src/stopped_gc.rs:349`).
//!
//! Stops a secondary role member via `nodes.stop`, shows the node's `Stopped`
//! row (`stopped == true`), and waits for the fabric-primary's stopped-node
//! collector (`RDM_STOPPED_NODE_TTL`) to age the stop receipt and collect the
//! stopped birth through an ordinary `node.delete` Build.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use rdm_admin::stopped_gc::{ENV, SHIPPED_TTL, ttl_from_env};
use rdm_client::{
    Builds, Filter, Frame, MeshNode, NodeStatus, Nodes, PathName, Peers, WorkflowStream,
    await_fact,
};
use rdm_core::NodeKind;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode =
        serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

async fn finish(what: &str, mut w: WorkflowStream) -> Result<()> {
    while let Some(frame) = w.next().await {
        match frame? {
            Frame::Complete => return Ok(()),
            Frame::Failed { step, reason } => {
                return Err(anyhow!("{what} failed at {step:?}: {reason}"));
            }
            Frame::FabricPrimaryMoved { fabric_primary } => {
                return Err(anyhow!("{what}: fabric-primary moved to {fabric_primary:?}"));
            }
            _ => {}
        }
    }
    Err(anyhow!("{what}: stream ended without a terminal frame"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-stopped-node-collection");
    let raw = std::env::var(ENV).ok();
    println!(
        "{}={:?} (shipped {:?}) -> parsed={:?}, zero_refusal={:?}",
        ENV,
        raw,
        SHIPPED_TTL,
        ttl_from_env(raw.as_deref()),
        ttl_from_env(Some("0s")).err()
    );

    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let nodes = Nodes::new(carrier.clone());
    let builds = Builds::new(carrier);
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
            .find(|n| {
                n.kind != NodeKind::NodeAdmin
                    && !n.is_primary
                    && n.status == NodeStatus::Ready
            })
            .ok_or_else(|| anyhow!("no ready secondary role node in the estate"))?
            .clone(),
    };
    println!(
        "before stop: {} status={:?} stopped={} node_id={}",
        target.name, target.status, target.stopped, target.node_id
    );

    finish(&format!("node.stop {}", target.name), nodes.stop(&target.name).await?).await?;
    let stopped_rows = nodes.get(&Filter::new().eq("mesh", [target.mesh.clone()])).await?;
    if let Some(s) = stopped_rows.iter().find(|n| n.name == target.name) {
        println!(
            "after stop:  {} status={:?} stopped={} inc={:?}",
            s.name, s.status, s.stopped, s.incarnation_id
        );
    }

    let target_name = target.name.clone();
    let filter = Filter::new().eq("mesh", [target_name.mesh.clone()]);
    await_fact(
        "node",
        "fabric-primary",
        "stopped birth collected by RDM_STOPPED_NODE_TTL sweep",
        Duration::from_secs(60),
        || nodes.get(&filter),
        |list| list.iter().all(|n| n.name != target_name),
        |list| {
            list.iter()
                .find(|n| n.name == target_name)
                .map(|n| format!("{} {:?} (stopped={})", n.name, n.status, n.stopped))
                .unwrap_or_else(|| format!("{target_name} collected"))
        },
    )
    .await?;

    let all_builds = builds.get(&Filter::new()).await?;
    let gc_build = all_builds.last().ok_or_else(|| anyhow!("no builds"))?;
    println!(
        "collected {}: latest build={} phase={:?}",
        target_name,
        gc_build.build_id.log(),
        gc_build.phase
    );
    Ok(())
}
