//! What every demo cell on the demo's own node processes shares (Q295): the binding set of cargo's
//! built executables, the estate those executables run as, and the calls the cells make to them.
#![allow(dead_code)]

use rdm_client::binding::{sha256_file, Binding, BindingSet, Candidate};
use rdm_client::{FabricDesired, MeshDesired, NodeStatus};
use rdm_core::outcome::RpcOutcome;
use rdm_core::{NodeId, NodeKind};
use rdm_test::estate::{Estate, Owner};
use rdm_transport::{Budget, CallOptions, NodeTarget};
use rshape_consumer::replica::{Records, RecordsReply, RecordsRequest};
use std::path::PathBuf;
use std::time::Duration;

/// How long a node is waited for.
pub const SETTLE: Duration = Duration::from_secs(120);

/// The candidate the binding set names: the RDM commit the consumer build imported when it says,
/// else this checkout's own label.
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
        bind("compute", env!("CARGO_BIN_EXE_rshape-compute")),
    ];
    BindingSet { candidate: Candidate { sha: candidate.into(), build: String::new() }, launch_ids: bindings.iter().map(|b| b.launch_id.clone()).collect(), bindings }
}

/// The estate of `test`: the demo's node-admin, then one Build to mesh1 holding `shape` (each kind's
/// count; the node-admin is counted), every node of it Ready.
pub async fn estate(feature: &str, test: &str, shape: &[(NodeKind, u32)]) -> Estate {
    let candidate = candidate();
    let owner = Owner { product: "rshape".into(), feature: feature.into(), subfeature: "demo".into(), rung: "MN".into(), provider: "process".into(), test: test.into() };
    let estate = Estate::bootstrap_on(owner, "fabric1", "mesh1", &binding_set(&candidate), &candidate, &["gateway", "broker", "compute"])
        .await
        .unwrap_or_else(|e| panic!("the demo's binding set is refused: {e}"));
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", shape.iter().copied())] }).await;
    for (kind, count) in shape {
        for ordinal in 1..=*count {
            ready(&estate, &format!("mesh1.{}.{ordinal}", kind.segment())).await;
        }
    }
    estate
}

/// `node` Ready, awaited.
pub async fn ready(estate: &Estate, node: &str) {
    estate.until_node(node, &format!("{node} ready"), SETTLE, |n| n.filter(|n| n.status == NodeStatus::Ready).map(|_| ())).await;
}

/// The node id of `node`'s current birth.
pub async fn node_id(estate: &Estate, node: &str) -> NodeId {
    serde_json::from_value(estate.node_value(node).await["node_id"].clone()).expect("a node id")
}

/// `request` at `node`'s replica op ([`Records`]), answered within 40 s.
pub async fn records(estate: &Estate, node: &str, request: &RecordsRequest) -> RecordsReply {
    let options = CallOptions { budget: Budget::Overall(Duration::from_secs(40)), ..CallOptions::default() };
    let target = NodeTarget::ExactNode(node_id(estate, node).await);
    let (outcome, _) = estate.carrier().await.client().call::<Records>(&target, request, &options).await;
    match outcome {
        RpcOutcome::Reply(reply) => reply.into_value(),
        other => panic!("{node}'s records op answered {other:?} to {request:?}"),
    }
}

/// `topic`'s records as `node` answers a fetch from offset 0.
pub async fn fetch(estate: &Estate, node: &str, topic: &str) -> Vec<(u64, String)> {
    match records(estate, node, &RecordsRequest::Fetch { topic: topic.into(), from: 0, max: 1000 }).await {
        RecordsReply::Fetched { records, .. } => records,
        other => panic!("{node}'s fetch of {topic} was not answered: {other:?}"),
    }
}

/// Run `body` on a four-worker runtime.
pub fn run<F: std::future::Future<Output = ()>>(body: F) {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap().block_on(body);
}

/// The topics `node` holds, as its [`rshape_consumer::replica::TopicsRead`] answers.
pub async fn topics(estate: &Estate, node: &str) -> Vec<String> {
    use rshape_consumer::replica::{TopicsRead, TopicsReply, TopicsRequest};
    let target = NodeTarget::ExactNode(node_id(estate, node).await);
    let (outcome, _) = estate.carrier().await.client().call::<TopicsRead>(&target, &TopicsRequest::List, &CallOptions::default()).await;
    match outcome {
        RpcOutcome::Reply(reply) => match reply.into_value() {
            TopicsReply::Topics { topics } => topics,
            other => panic!("{node}'s topics read was refused: {other:?}"),
        },
        other => panic!("{node}'s topics read answered {other:?}"),
    }
}

/// `node` holds every topic of `want`, awaited.
pub async fn until_topics(estate: &Estate, node: &str, want: &[&str]) {
    let until = std::time::Instant::now() + SETTLE;
    loop {
        let held = topics(estate, node).await;
        if want.iter().all(|t| held.iter().any(|h| h == t)) {
            return;
        }
        assert!(std::time::Instant::now() < until, "{node} does not hold the topics {want:?} within {SETTLE:?}: it holds {held:?}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Produce `record` to `topic` through `gateway`, acknowledged.
pub async fn produce(estate: &Estate, gateway: &str, operation: &str, topic: &str, record: &str) -> u64 {
    match records(estate, gateway, &RecordsRequest::Produce { operation: operation.into(), topic: topic.into(), record: record.into() }).await {
        RecordsReply::Produced { offset, .. } => offset,
        other => panic!("{gateway}'s produce of {operation} to {topic} was not acknowledged: {other:?}"),
    }
}
