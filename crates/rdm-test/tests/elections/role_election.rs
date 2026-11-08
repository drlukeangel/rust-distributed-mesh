//! i143.e11.s4 process E2E: a node type's role is the one election (PRD §11, §13.1).
//!
//! One fabric, `{node-admin: 1, broker: 3, gateway: 1, compute: 1}`. From public surfaces only:
//! - every node type of each mesh advertises exactly one primary (`roles_as_expected`), a node type like rpc
//!   nodes;
//! - kill the broker primary (SIGKILL): unheard past the staleness floor, it is superseded by the
//!   next-lowest ready broker, elected by the brokers themselves (Q406): a
//!   `rdm.mesh.election.resolve.via-recompute` span with `type = broker` whose observer is a broker,
//!   and none from a node-admin; the winner records the role on its own record at the next revision; the
//!   view's one broker primary is that successor, which keeps its role over the birth drift recovery
//!   brings back at the killed path whatever NodeId it drew (elections §5) — read the way a product
//!   reads it, by `is_primary` on `node.get` (`Nodes::get`), never computed product-side.

use rdm_client::{FabricDesired, MeshDesired, NodeStatus};
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
    nodes.iter().filter(|n| !matches!(n["state"].as_str(), Some("dead" | "pending-reconnect"))).cloned().collect()
}

async fn settle(estate: &Estate, label: &str) -> Vec<Value> {
    estate
        .until_view(&format!("{label}: the view settles on the computed roles"), Duration::from_secs(30), |views| {
            let nodes = live(&Estate::values(views));
            (!nodes.is_empty() && nodes.iter().all(|n| n["state"] == "ready") && primaries_as_expected(&nodes).is_ok()).then_some(nodes)
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_brokers_elect_the_next_lowest_ready_node_id() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.resize(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1), ("broker", 3), ("gateway", 1), ("compute", 1)])] }).await;
    let brokers: NodeType = ("mesh1".into(), "broker".into());
    let nodes = settle(&estate, "roles").await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    assert_eq!(nodes.iter().filter(|n| n["type"] == "broker").count(), 3, "{nodes:#?}");
    let primaries = advertised_primaries(&nodes);
    assert_eq!(primaries.keys().map(|c| c.1.as_str()).collect::<Vec<_>>(), ["broker", "compute", "gateway", "node-admin"]);
    let killed_id = primaries[&brokers][0].clone();
    let killed = rdm_test::estate::location(nodes.iter().find(|n| n["node_id"] == killed_id).unwrap());

    // The successor, decided from the pre-kill view: the lowest NodeId among the other ready brokers.
    let (succ, succ_id) = nodes
        .iter()
        .filter(|n| n["type"] == "broker" && n["node_id"] != killed_id.as_str())
        .map(|n| (rdm_test::estate::location(&n), s(&n["node_id"])))
        .min_by(|a, b| a.1.cmp(&b.1))
        .unwrap();
    // The brokers elect from the facts they hold (Q406): the primary they supersede is the one they
    // heard, its role on its own Digest. Each other broker holds it before it is killed.
    let killed_node_id = rdm_core::NodeId::parse(&killed_id).unwrap();
    let role = rdm_core::PrimaryRole::NodePrimary("broker".into());
    for broker in nodes.iter().filter(|n| n["type"] == "broker" && n["node_id"] != killed_id.as_str()).map(|n| rdm_test::estate::location(&n)) {
        wait_for(&format!("{broker:?} holds {killed_id} as the broker primary from its Digest"), Duration::from_secs(10), || async {
            let held = estate.held_topology_at(&broker).await?;
            held.members.iter().any(|m| m.node.node_id == killed_node_id && m.primary.iter().any(|r| r.role == role)).then_some(())
        })
        .await;
    }
    estate.kill_node(&killed).await;
    // The brokers elect once the killed primary is unheard past the staleness floor (Q406; elections
    // §4.1), within node recovery's bound (`node_recovery_due`). The winner's own span names the winner,
    // the birth it replaced and that it installed itself: the election's span is the only place that fact
    // lives, so the span is awaited.
    wait_for(&format!("{succ:?} ({succ_id}) elected itself the broker successor to {killed_id}"), crate::node_recovery_due(), || async {
        named(&estate.spans(), "rdm.mesh.election.resolve.via-recompute")
            .iter()
            .any(|sp| {
                let a = &sp["attributes"];
                a["election_level"] == "node_type_primary" && a["mesh"] == "mesh1" && a["type"] == "broker" && a["winner_node_id"] == succ_id.as_str() && a["previous_node_id"] == killed_id.as_str() && a["observer_node_id"] == succ_id.as_str() && a["outcome"] == "installed"
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
        .until_view("the view's one broker primary is the announced successor and the killed birth is gone", rdm_core::cadence::staleness_floor() + Duration::from_secs(30), |view| {
            let advertised: Vec<_> = view.iter().filter(|n| n.mesh() == "mesh1" && n.r#type == "broker" && n.is_primary).collect();
            let killed_gone = !view.iter().any(|n| n.node_id.to_string() == killed_id && n.state == NodeStatus::Ready);
            (advertised.len() == 1 && advertised[0].node_id.to_string() == succ_id && killed_gone).then(|| view.to_vec())
        })
        .await;
    estate.artifact("view-after-kill.json", &json!({"killed": killed, "killed_id": killed_id, "successor": succ, "view": view}));
    // The winner records the role on its own record at the next revision over the killed primary's.
    let announced_revision = || named(&estate.spans(), "rdm.mesh.election.resolve.via-recompute").into_iter().find(|sp| sp["attributes"]["observer_node_id"] == succ_id.as_str() && sp["attributes"]["outcome"] == "installed").and_then(|sp| sp["attributes"]["previous_revision"].to_string().trim_matches('"').parse::<u64>().ok());
    let previous = announced_revision().expect("the winner's span names the revision it superseded");
    let succ_node_id = rdm_core::NodeId::parse(&succ_id).unwrap();
    wait_for(&format!("{succ_id}'s own record holds is_primary {}", previous + 1), Duration::from_secs(10), || async { (estate.record(&succ_node_id).and_then(|r| r.is_primary) == Some(previous + 1)).then_some(()) }).await;
    estate.stop().await;
    let spans = estate.spans();
    // Elected by the brokers: every broker election span is a broker's, none a node-admin's.
    let broker_elections: Vec<_> = named(&spans, "rdm.mesh.election.resolve.via-recompute").into_iter().filter(|sp| sp["attributes"]["type"] == "broker").collect();
    assert!(broker_elections.iter().all(|sp| sp["attributes"]["observer_type"] == "broker"), "no node-admin computes the broker election: {broker_elections:#?}");
    let announced = broker_elections.into_iter().find(|sp| sp["attributes"]["winner_node_id"] == succ_id.as_str() && sp["attributes"]["outcome"] == "installed").cloned().unwrap();
    estate.record_trace_url(announced["trace_id"].as_str().unwrap_or(""));
}
