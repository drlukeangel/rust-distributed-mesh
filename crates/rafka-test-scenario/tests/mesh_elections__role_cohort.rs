//! i143.e11.s4 process E2E: a role cohort's seat is the one election (PRD §11, §13.1).
//!
//! One fabric, `{node_admin: 1, broker: 3, gateway: 1, compute: 1}`. From public surfaces only:
//! - every cohort's advertised primary is the one the canonical election computes from the
//!   view's NodeIds and statuses (`seats_as_expected`), a role cohort like an rpc cohort;
//! - kill the broker primary (SIGKILL): the next-lowest ready broker is announced by a
//!   `rdm.mesh.election.resolve.via-recompute` span with `kind = broker`; the view's one broker
//!   primary is the election's answer on the same sample (drift rebirths the path as a new NodeId
//!   that wins or not by it) — read the way a product reads it, by `is_primary` on node-admin's
//!   view (`rafka-node-admin-client` `nodes()`), never computed product-side.

use rafka_node_admin_client::NodeAdminClient;
use rafka_test_scenario::elections::{advertised_primaries, seats_as_expected, Cohort};
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "role-cohort".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "the_broker_cohort_elects_the_next_lowest_ready_node_id".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn live(nodes: &[Value]) -> Vec<Value> {
    nodes.iter().filter(|n| !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))).cloned().collect()
}

async fn settle(estate: &Estate, label: &str) -> Vec<Value> {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let nodes = live(&estate.nodes().await);
        if !nodes.is_empty() && nodes.iter().all(|n| n["status"] == "ready-for-traffic") && seats_as_expected(&nodes).is_ok() {
            return nodes;
        }
        if Instant::now() > until {
            panic!("{label}: the view did not settle on the computed seats in 30s: {nodes:#?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_broker_cohort_elects_the_next_lowest_ready_node_id() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let desired = json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "broker": 3, "gateway": 1, "compute": 1}]});
    let (status, accepted) = estate.post("/api/build", &desired).await;
    assert_eq!(status, 202, "{accepted}");
    estate.await_build(accepted["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let brokers: Cohort = ("mesh1".into(), "broker".into());
    let nodes = settle(&estate, "roles").await;
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
    let silence_bound = rafka_mesh_transport::membership::staleness_floor() * 2 + rafka_mesh_transport::membership::backbone_gossip_interval() * 2 + Duration::from_secs(10);
    wait_for(&format!("{succ} ({succ_id}) announced as the broker successor to {killed_id}"), silence_bound, || async {
        named(&estate.spans(), "rdm.mesh.election.resolve.via-recompute")
            .iter()
            .any(|sp| {
                let a = &sp["attributes"];
                a["election_level"] == "node_type" && a["mesh"] == "mesh1" && a["kind"] == "broker" && a["winner_node_id"] == succ_id.as_str() && a["previous_node_id"] == killed_id.as_str()
            })
            .then_some(())
    })
    .await;

    // A product reads the seat from node-admin's view and never computes it. Drift recovery
    // rebirths the killed path within seconds as a new NodeId that wins or not by it, so the seat
    // is checked against the election's answer on the same sample, never against the announced
    // successor alone: the one advertised broker primary is the lowest ready NodeId of that view,
    // and the killed birth is no longer ready anywhere in it.
    let client = NodeAdminClient::new(estate.admin.clone());
    let view = wait_for("the view's one broker primary is the election's answer and the killed birth is gone", rafka_mesh_transport::membership::staleness_floor() + Duration::from_secs(30), || async {
        let view = client.nodes().await.ok()?;
        let ready: Vec<_> = view.iter().filter(|n| n.mesh == "mesh1" && n.kind == rafka_mesh_entity::NodeKind::Broker && n.status == rafka_node_admin_client::NodeStatus::ReadyForTraffic).collect();
        let advertised: Vec<_> = ready.iter().filter(|n| n.is_primary).collect();
        let lowest = ready.iter().min_by(|a, b| a.node_id.to_string().cmp(&b.node_id.to_string()))?;
        let killed_gone = !view.iter().any(|n| n.node_id.to_string() == killed_id && n.status == rafka_node_admin_client::NodeStatus::ReadyForTraffic);
        (advertised.len() == 1 && advertised[0].node_id == lowest.node_id && killed_gone).then_some(view)
    })
    .await;
    estate.artifact("view-after-kill.json", &json!({"killed": killed, "killed_id": killed_id, "successor": succ, "view": view}));
    estate.stop().await;
    let spans = estate.spans();
    let announced = named(&spans, "rdm.mesh.election.resolve.via-recompute").into_iter().find(|sp| sp["attributes"]["kind"] == "broker" && sp["attributes"]["winner_node_id"] == succ_id.as_str()).cloned().unwrap();
    estate.record_trace_url(announced["trace_id"].as_str().unwrap_or(""));
}
