//! Node-investigation workflow: `docs/architecture/lifecycles/node-investigation.md:1`
//! (`crates/rdm-admin/src/node_investigation.rs:1`).
//!
//! Runs `investigate` against a live mesh member in the estate (reaching the
//! exact birth on the tickle step without touching the OS runtime) and then on
//! an unreached birth backed by the member's `RuntimeFact` (stepping into
//! `inspect_runtime` to return `Finding::Running`).

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::inspect_runtime;
use rdm_admin::model::{Fabric, Mesh, Node, NodeId, NodeKind, NodeStatus, PathName, ScopeStatus};
use rdm_admin::node_investigation::investigate;
use rdm_admin::offline::TickleClient;
use rdm_admin::topology::Topology;
use rdm_client::{Fabrics, Filter, MeshNode, Nodes, Peers};
use rdm_core::RuntimeFact;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode =
        serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-investigation");
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry.clone());
    let fabrics = Fabrics::new(carrier.clone());
    let nodes = Nodes::new(carrier.clone());

    let fabric_view = fabrics
        .get(&Filter::new())
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no fabric"))?;
    let node_views = nodes.get(&Filter::new()).await?;
    peers.learn_known(&node_views);
    let model_nodes: Vec<Node> = node_views
        .iter()
        .map(|n| Ok(serde_json::from_value(serde_json::to_value(n)?)?))
        .collect::<Result<_>>()?;

    let view = Topology {
        fabric: Fabric {
            id: fabric_view.id,
            name: fabric_view.name,
            status: ScopeStatus::Ready,
            provider: rdm_admin::model::ProviderKind::Process,
        },
        meshes: fabric_view
            .meshes
            .iter()
            .map(|m| Mesh { id: m.id.clone(), name: m.name.clone(), status: ScopeStatus::Ready })
            .collect(),
        nodes: model_nodes.clone(),
    };

    let admin = model_nodes.iter().find(|n| n.name == entry).ok_or_else(|| anyhow!("missing {entry}"))?;
    let target = match std::env::args().nth(1) {
        Some(arg) => {
            let name: PathName = arg.parse().map_err(|e| anyhow!("{arg}: {e}"))?;
            model_nodes.iter().find(|n| n.name == name).ok_or_else(|| anyhow!("missing {name}"))?
        }
        None => model_nodes
            .iter()
            .find(|n| n.mesh == entry.mesh && n.kind != NodeKind::NodeAdmin && n.status == NodeStatus::Ready)
            .ok_or_else(|| anyhow!("no ready role node in {}", entry.mesh))?,
    };
    let dir = target.data_dir.as_deref().ok_or_else(|| anyhow!("missing data_dir"))?;
    let inc = target.incarnation_id.as_ref().ok_or_else(|| anyhow!("missing incarnation_id"))?;
    let fact = RuntimeFact::read_record(Path::new(dir))
        .ok_or_else(|| anyhow!("no runtime.json in {dir}"))?
        .map_err(|e| anyhow!(e))?;

    let calls = TickleClient {
        client: Arc::new(carrier.client().clone()),
        me_id: admin.node_id.clone(),
        me: admin.name.to_string(),
        connections: None,
        commands: None,
    };

    let provider = ProcessDeploymentProvider::new();
    let reached = investigate(target, &view, &calls, async {
        inspect_runtime(&provider, &fact, Some(dir), inc.as_str()).await.into()
    })
    .await;
    println!(
        "step 1 (live member {}): verdict={} inspected={:?}",
        target.name,
        reached.name(),
        reached.inspected()
    );

    let mut unreached_node = target.clone();
    unreached_node.node_id = NodeId::mint();
    unreached_node.mesh = "isolated".to_string();
    let inspected = investigate(&unreached_node, &view, &calls, async {
        inspect_runtime(&provider, &fact, Some(dir), inc.as_str()).await.into()
    })
    .await;
    println!(
        "step 2 (unreached birth backed by {} runtime.json): verdict={} inspected={:?}",
        target.name,
        inspected.name(),
        inspected.inspected()
    );
    Ok(())
}
