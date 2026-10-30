//! The replica on the demo's own node processes (Q296): produce and fetch through the gateway, served
//! by the mesh's broker primary, across that primary's stop and RDM's election of the next.
//!
//! CONTRACT (Q296; node-primary-update.md, the node-type primary): the gateway forwards a client's
//! `Produce`/`Fetch` to its mesh's broker primary; a repeat of one operation id answers the offset it
//! was given; stopping the broker primary, RDM elects the other broker and the gateway follows it,
//! producing there; started again, the stopped broker's log still holds every record it acknowledged.
//! Must NOT happen: a record appended twice for one operation id, a produce answered by a broker that
//! is not the primary, or an acknowledged record lost.

use rdm_client::binding::{sha256_file, Binding, BindingSet, Candidate};
use rdm_client::{FabricDesired, MeshDesired, NodeStatus};
use rdm_core::outcome::RpcOutcome;
use rdm_core::{NodeId, NodeKind};
use rdm_test::estate::{Estate, Owner};
use rdm_transport::{Budget, CallOptions, NodeTarget};
use rshape_consumer::replica::{Records, RecordsReply, RecordsRequest};
use std::path::PathBuf;
use std::time::Duration;

const GATEWAY: &str = "mesh1.gateway.1";
const BROKERS: [&str; 2] = ["mesh1.broker.1", "mesh1.broker.2"];
const SETTLE: Duration = Duration::from_secs(120);
const TOPIC: &str = "orders";

fn candidate() -> String {
    std::env::var("RDM_CANDIDATE_SHA").unwrap_or_else(|_| "demo-local".into())
}

/// The demo's own executables, as cargo built them for this test.
fn binding_set(candidate: &str) -> BindingSet {
    let bind = |launch_id: &str, exe: &str| {
        let executable = PathBuf::from(exe);
        let sha256 = sha256_file(&executable).unwrap_or_else(|e| panic!("the demo's {launch_id} executable {}: {e}", executable.display()));
        Binding { launch_id: launch_id.into(), executable, sha256, image: None }
    };
    let bindings = vec![
        bind("node_admin", env!("CARGO_BIN_EXE_rshape-node-admin")),
        bind("gateway", env!("CARGO_BIN_EXE_rshape-gateway")),
        bind("broker", env!("CARGO_BIN_EXE_rshape-broker")),
    ];
    BindingSet { candidate: Candidate { sha: candidate.into(), build: String::new() }, launch_ids: bindings.iter().map(|b| b.launch_id.clone()).collect(), bindings }
}

fn owner(test: &str) -> Owner {
    Owner { product: "rshape".into(), feature: "demo-replica".into(), subfeature: "broker-primary".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

async fn ready(estate: &Estate, node: &str) {
    estate.until_node(node, &format!("{node} ready"), SETTLE, |n| n.filter(|n| n.status == NodeStatus::Ready).map(|_| ())).await;
}

/// The node id of `node`'s current birth.
async fn node_id(estate: &Estate, node: &str) -> NodeId {
    serde_json::from_value(estate.node_value(node).await["node_id"].clone()).expect("a node id")
}

/// `request` at `node`'s [`Records`] op, answered within 40 s.
async fn records(estate: &Estate, node: &str, request: &RecordsRequest) -> RecordsReply {
    let options = CallOptions { budget: Budget::Overall(Duration::from_secs(40)), ..CallOptions::default() };
    let target = NodeTarget::ExactNode(node_id(estate, node).await);
    let (outcome, _) = estate.carrier().await.client().call::<Records>(&target, request, &options).await;
    match outcome {
        RpcOutcome::Reply(reply) => reply.into_value(),
        other => panic!("{node}'s records op answered {other:?} to {request:?}"),
    }
}

async fn produce(estate: &Estate, operation: &str, record: &str) -> u64 {
    match records(estate, GATEWAY, &RecordsRequest::Produce { operation: operation.into(), topic: TOPIC.into(), record: record.into() }).await {
        RecordsReply::Produced { offset, .. } => offset,
        other => panic!("the gateway's produce of {operation} was not acknowledged: {other:?}"),
    }
}

async fn fetch(estate: &Estate, node: &str) -> Vec<(u64, String)> {
    match records(estate, node, &RecordsRequest::Fetch { topic: TOPIC.into(), from: 0, max: 100 }).await {
        RecordsReply::Fetched { records, .. } => records,
        other => panic!("{node}'s fetch of {TOPIC} was not answered: {other:?}"),
    }
}

/// The broker the rows name the broker primary, awaited.
async fn broker_primary(estate: &Estate, not: Option<&str>) -> String {
    let until = std::time::Instant::now() + SETTLE;
    loop {
        for broker in BROKERS {
            let row = estate.node_value(broker).await;
            if row["is_primary"] == true && row["status"] == "ready" && Some(broker) != not {
                return broker.to_string();
            }
        }
        assert!(std::time::Instant::now() < until, "no broker other than {not:?} is the broker primary within {SETTLE:?}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[test]
fn the_gateway_produces_at_the_broker_primary_and_follows_its_election_with_no_acknowledged_record_lost() {
    let test = "the_gateway_produces_at_the_broker_primary_and_follows_its_election_with_no_acknowledged_record_lost";
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap().block_on(async {
        let candidate = candidate();
        let estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&candidate), &candidate, &["gateway", "broker"]).await.unwrap_or_else(|e| panic!("the demo's binding set is refused: {e}"));
        estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::Gateway, 1), (NodeKind::Broker, 2)])] }).await;
        for node in [GATEWAY, BROKERS[0], BROKERS[1]] {
            ready(&estate, node).await;
        }

        // Produce through the gateway at the broker primary; a repeat of the operation id is the same record.
        let first = broker_primary(&estate, None).await;
        let offset = produce(&estate, "op-1", "order 1").await;
        assert_eq!(produce(&estate, "op-1", "order 1").await, offset, "a repeat of one operation id answers the offset it was given");
        assert_eq!(fetch(&estate, GATEWAY).await, vec![(offset, "order 1".to_string())], "the gateway's fetch reads the broker primary's log, the record once");
        let other = BROKERS.iter().find(|b| **b != first).unwrap();
        assert!(matches!(records(&estate, other, &RecordsRequest::Produce { operation: "op-x".into(), topic: TOPIC.into(), record: "x".into() }).await, RecordsReply::NotPrimary { .. }), "a broker that is not the primary refuses a produce by name");

        // The broker primary stopped: RDM elects the other broker, and the gateway produces there.
        Estate::workflow(&format!("node.stop of {first}"), estate.nodes_obj().await.stop(&first.parse().unwrap()).await).await;
        let second = broker_primary(&estate, Some(&first)).await;
        let offset2 = produce(&estate, "op-2", "order 2").await;
        assert_eq!(fetch(&estate, GATEWAY).await, vec![(offset2, "order 2".to_string())], "after the election the gateway produces at {second}");

        // Started again, the stopped broker still holds what it acknowledged.
        Estate::workflow(&format!("node.start of {first}"), estate.nodes_obj().await.start(&first.parse().unwrap()).await).await;
        ready(&estate, &first).await;
        assert_eq!(fetch(&estate, &first).await, vec![(offset, "order 1".to_string())], "{first}'s log keeps the record it acknowledged before its stop");

        estate.shutdown().await;
    });
}
