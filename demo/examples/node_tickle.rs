//! Node tickle reach ladder: `docs/architecture/lifecycles/node-tickle.md:1`
//! (`crates/rdm-admin/src/workflows/node_tickle.rs:8`).
//!
//! Runs `tickle` against a live mesh member (direct probe), sends the same
//! `ProbeNodeState` through an eligible peer carrier of the member's mesh via
//! `TickleCalls::via_peer`, verifies `birth_answered`, and shows the refusal
//! when a node record holds no `incarnation_id`.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use rdm_admin::model::{Fabric, Mesh, Node, NodeKind, NodeStatus, PathName, ScopeStatus};
use rdm_admin::topology::Topology;
use rdm_admin::workflows::node_tickle::{
    ProbeEnd, Tickle, TickleCalls, TickleClient, VIA_PEER_TICKLE_FANOUT, birth_answered,
    eligible_carriers, tickle, tickle_ladder_at_default_budget,
};
use rdm_client::{Fabrics, Filter, MeshNode, Nodes, Peers};
use rdm_core::status::StatusRequest;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH")
        .context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode =
        serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-tickle");
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
    let inc = target.incarnation_id.as_ref().ok_or_else(|| anyhow!("missing incarnation_id"))?;

    let carriers: Vec<_> = eligible_carriers(&view, target, Some(&admin.node_id))
        .map(|c| c.name.to_string())
        .collect();
    println!(
        "ladder budget={:?}, fanout={}, eligible carriers for {}: [{}]",
        tickle_ladder_at_default_budget(),
        VIA_PEER_TICKLE_FANOUT,
        target.name,
        carriers.join(", ")
    );

    let calls = TickleClient {
        client: Arc::new(carrier.client().clone()),
        me_id: admin.node_id.clone(),
        me: admin.name.to_string(),
        connections: None,
        commands: None,
    };

    match tickle(target, &view, &calls).await.map_err(|e| anyhow!(e))? {
        Tickle::Reached { answer, carrier } => println!(
            "direct tickle {}: reached(answer={}, carrier={:?}, birth_answered={})",
            target.name,
            answer.name(),
            carrier,
            birth_answered(Some(&answer), &target.node_id, inc)
        ),
        Tickle::Unreached(u) => println!("direct tickle {}: unreached({})", target.name, u.direct.name()),
    }

    let peer_carrier = eligible_carriers(&view, target, Some(&admin.node_id))
        .next()
        .ok_or_else(|| anyhow!("no eligible carrier"))?;
    let probe = StatusRequest::ProbeNodeState {
        node_id: target.node_id.clone(),
        incarnation: inc.clone(),
    };
    let via_end = calls.via_peer(peer_carrier, target, &probe).await;
    let via_ok = match &via_end {
        ProbeEnd::Reply(r) => birth_answered(Some(r), &target.node_id, inc),
        _ => false,
    };
    println!(
        "via-peer tickle {} through {}: end={} birth_answered={}",
        target.name,
        peer_carrier.name,
        via_end.name(),
        via_ok
    );

    let mut unborn = target.clone();
    unborn.incarnation_id = None;
    let err = tickle(&unborn, &view, &calls).await.expect_err("unborn node must be refused");
    println!("unborn refusal: {err}");
    Ok(())
}
