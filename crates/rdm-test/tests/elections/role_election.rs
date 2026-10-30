//! i143.e11.s4 process E2E: a role cohort's role is the one election (PRD §11, §13.1).
//!
//! One fabric, `{node_admin: 1, broker: 3, gateway: 1, compute: 1}`. From public surfaces only:
//! - every cohort advertises exactly one primary (`roles_as_expected`), a role cohort like an rpc
//!   cohort;
//! - kill the broker primary (SIGKILL): the next-lowest ready broker is announced by a
//!   `rdm.mesh.election.resolve.via-recompute` span with `kind = broker`; the view's one broker
//!   primary is that successor, which keeps its role over the birth drift recovery brings back at the
//!   killed path whatever NodeId it drew (elections §5) — read the way a product reads it, by
//!   `is_primary` on `node.get` (`Nodes::get`), never computed product-side.

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, NodeStatus};
use rdm_test::elections::{advertised_primaries, primaries_as_expected, NodeType};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "role-cohort".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "the_brokers_elect_the_next_lowest_ready_node_id".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn live(nodes: &[Value]) -> Vec<Value> {
    nodes.iter().filter(|n| !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))).cloned().collect()
}

async fn settle(estate: &Estate, label: &str) -> Vec<Value> {
    estate
        .until_view(&format!("{label}: the view settles on the computed roles"), Duration::from_secs(30), |views| {
            let nodes = live(&Estate::values(views));
            (!nodes.is_empty() && nodes.iter().all(|n| n["status"] == "ready") && primaries_as_expected(&nodes).is_ok()).then_some(nodes)
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_brokers_elect_the_next_lowest_ready_node_id() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Broker, 3), (NodeKind::Gateway, 1), (NodeKind::Compute, 1)])] }).await;
    let brokers: NodeType = ("mesh1".into(), "broker".into());
    let nodes = settle(&estate, "roles").await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    assert_eq!(nodes.iter().filter(|n| n["kind"] == "broker").count(), 3, "{nodes:#?}");
    let primaries = advertised_primaries(&nodes);
    assert_eq!(primaries.keys().map(|c| c.1.as_str()).collect::<Vec<_>>(), ["broker", "compute", "gateway", "node_admin"]);
    let killed = primaries[&brokers][0].clone();
    let killed_id = s(&nodes.iter().find(|n| n["name"] == killed.as_str()).unwrap()["node_id"]);

    // The successor, decided from the pre-kill view: the lowest NodeId among the other ready brokers.
    let (succ, succ_id) = nodes
        .iter()
        .filter(|n| n["kind"] == "broker" && n["node_id"] != killed_id.as_str())
        .map(|n| (s(&n["name"]), s(&n["node_id"])))
        .min_by(|a, b| a.1.cmp(&b.1))
        .unwrap();
    estate.kill_node(&killed).await;
    // The role vacates only on the provider's exited verdict, which node recovery takes at the killed
    // birth's TRUE OFFLINE mark (elections §4.1, §5; `node_recovery_due`). The announcement names the
    // winner it elected and the birth it replaced: the election's span is the only place that fact lives,
    // so the span is awaited.
    wait_for(&format!("{succ} ({succ_id}) announced as the broker successor to {killed_id}"), crate::node_recovery_due(), || async {
        named(&estate.spans(), "rdm.mesh.election.resolve.via-recompute")
            .iter()
            .any(|sp| {
                let a = &sp["attributes"];
                a["election_level"] == "node_type" && a["mesh"] == "mesh1" && a["kind"] == "broker" && a["winner_node_id"] == succ_id.as_str() && a["previous_node_id"] == killed_id.as_str()
            })
            .then_some(())
    })
    .await;

    // A product reads the role from node-admin's view and never computes it. Drift recovery
    // brings a new birth back at the killed path within seconds, a new NodeId, a candidate for a later vacancy that
    // never displaces the successor, whatever NodeId it drew (fabric-node-lifecycle-elections.md §5:
    // "a living primary keeps its role; a lower-NodeId candidate that later becomes Ready never
    // displaces it"): the one advertised broker primary is the announced successor, and the killed
    // birth is no longer ready anywhere in the view.
    let view = estate
        .until_view("the view's one broker primary is the announced successor and the killed birth is gone", rdm_transport::topology::staleness_floor() + Duration::from_secs(30), |view| {
            let advertised: Vec<_> = view.iter().filter(|n| n.mesh == "mesh1" && n.kind == NodeKind::Broker && n.is_primary).collect();
            let killed_gone = !view.iter().any(|n| n.node_id.to_string() == killed_id && n.status == NodeStatus::Ready);
            (advertised.len() == 1 && advertised[0].node_id.to_string() == succ_id && killed_gone).then(|| view.to_vec())
        })
        .await;
    estate.artifact("view-after-kill.json", &json!({"killed": killed, "killed_id": killed_id, "successor": succ, "view": view}));
    estate.stop().await;
    let spans = estate.spans();
    let announced = named(&spans, "rdm.mesh.election.resolve.via-recompute").into_iter().find(|sp| sp["attributes"]["kind"] == "broker" && sp["attributes"]["winner_node_id"] == succ_id.as_str()).cloned().unwrap();
    estate.record_trace_url(announced["trace_id"].as_str().unwrap_or(""));
}
