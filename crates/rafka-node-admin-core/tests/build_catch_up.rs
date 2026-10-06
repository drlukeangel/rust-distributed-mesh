//! i143.e1.s5 functional: an admin that joins the fabric Build topic after a
//! Build was accepted still holds that Build in its projection, so it can be
//! the successor. A fact broadcast before a member is connected is never
//! delivered to it by gossip alone; on `NeighborUp` the admins already on the
//! topic send their active Builds' facts to their direct neighbours.

use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, SecretKey};
use rafka_node_admin_core::build::{BuildId, BuildIntent, MeshDesired};
use rafka_node_admin_core::build_state::{
    AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildIntentFact, BuildState, BuildStateAdapter,
};
use rafka_node_admin_core::fabric_builds::FabricBuildStateAdapter;
use rafka_node_admin_core::model::FabricId;
use std::sync::Arc;
use std::time::Duration;

async fn admin(peers: Vec<EndpointAddr>) -> (Endpoint, Router, Arc<FabricBuildStateAdapter>) {
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::generate())
        .alpns(vec![iroh_gossip::ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .unwrap()
        .bind()
        .await
        .unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let builds = Arc::new(FabricBuildStateAdapter::join(&gossip, &endpoint, &fabric1(), peers, Default::default(), rafka_node_admin_core::shutdown::ShutdownControl::memory("test-admin"), "test-admin".into()).await.unwrap());
    (endpoint, router, builds)
}

fn addr(e: &Endpoint) -> EndpointAddr {
    EndpointAddr::new(e.id()).with_ip_addr(e.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap())
}

fn intent(id: &BuildId) -> BuildIntentFact {
    BuildIntentFact {
        build_id: id.clone(),
        intent: BuildIntent::ReconcileMesh { desired: MeshDesired { name: "mesh1".into(), node_admin: 2, rpc_node: 3 } },
        traceparent: None,
        submitted_at_ms: 0,
        desired: None,
        reason: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admin_joining_after_a_build_was_accepted_holds_it_and_only_active_builds() {
    let (a_ep, a_router, a) = admin(vec![]).await;

    // A, alone on the topic, accepts an active Build (claimed) and a finished one.
    let active = BuildId::mint();
    a.publish_intent(&intent(&active)).await.unwrap();
    a.claim_attempt(&BuildAttemptClaim { build_id: active.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let finished = BuildId::mint();
    a.publish_intent(&intent(&finished)).await.unwrap();
    a.claim_attempt(&BuildAttemptClaim { build_id: finished.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    a.append_attempt_receipt(&BuildAttemptReceipt { build_id: finished.clone(), attempt: 1, outcome: AttemptOutcome::Converged })
        .await
        .unwrap();

    // C joins later.
    let (_c_ep, c_router, c) = admin(vec![addr(&a_ep)]).await;
    let mut caught_up = false;
    for _ in 0..200 {
        if c.read_build(&active).await.is_ok_and(|v| v.attempt == 1 && v.executor.as_deref() == Some("mesh1.admin.1")) {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(caught_up, "C never received the Build accepted before it joined");
    let v = c.read_build(&active).await.unwrap();
    assert_eq!((v.state, v.intent), (BuildState::Running, intent(&active).intent));
    assert!(c.read_build(&finished).await.is_err(), "finished Builds are history, not catch-up");

    // From here on C hears A's new facts directly.
    a.append_attempt_receipt(&BuildAttemptReceipt { build_id: active.clone(), attempt: 1, outcome: AttemptOutcome::Converged })
        .await
        .unwrap();
    let mut complete = false;
    for _ in 0..200 {
        if c.read_build(&active).await.is_ok_and(|v| v.state == BuildState::Complete) {
            complete = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(complete, "C missed a fact appended after it joined");
    a_router.shutdown().await.unwrap();
    c_router.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_catch_up_arrives_in_messages_that_fit_and_keeps_the_connection() {
    use rafka_node_admin_core::build_state::{BuildStepReceipt, StepOutcome};
    let (a_ep, a_router, a) = admin(vec![]).await;
    let id = BuildId::mint();
    a.publish_intent(&intent(&id)).await.unwrap();
    a.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    // Far more than one gossip message's worth of receipts, each with output.
    for i in 0..120 {
        a.append_step_receipt(&BuildStepReceipt {
            build_id: id.clone(),
            attempt: 1,
            operation: format!("create-node:mesh1.rpc.{i}"),
            step: "AllocateEndpoints".into(),
            outcome: StepOutcome::Complete,
            output: Some(serde_json::json!([{"slot": "rpc-0", "addr": format!("127.0.0.1:{}", 41000 + i), "freshness": "f".repeat(32)}])),
        })
        .await
        .unwrap();
    }
    let (_c_ep, c_router, c) = admin(vec![addr(&a_ep)]).await;
    let mut all = false;
    for _ in 0..200 {
        if c.read_build(&id).await.is_ok_and(|v| v.steps.len() == 120) {
            all = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(all, "C did not receive the whole catch-up: {:?}", c.read_build(&id).await.map(|v| v.steps.len()));
    // The connection survived: a fact appended afterwards still arrives.
    a.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    let mut complete = false;
    for _ in 0..200 {
        if c.read_build(&id).await.is_ok_and(|v| v.state == BuildState::Complete) {
            complete = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(complete, "the connection did not survive the catch-up");
    a_router.shutdown().await.unwrap();
    c_router.shutdown().await.unwrap();
}

/// iroh-gossip refuses a frame of 4096 bytes or more, and the frame is the
/// payload plus the message envelope (enum tags, the 32-byte message id, the
/// length prefix, the scope). A catch-up message packed to 4096 bytes of
/// payload is refused at write, which closes the connection and drops the
/// peer from every topic it shares. Here two active Builds together pack to
/// between the wire limit and 4096 bytes of payload: they must arrive in
/// messages that fit, and the connection must carry later facts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_catch_up_at_the_wire_limit_arrives_and_keeps_the_connection() {
    use rafka_node_admin_core::build_state::BuildFact;
    let (a_ep, a_router, a) = admin(vec![]).await;
    let (one, two) = (BuildId::mint(), BuildId::mint());
    let padded = |id: &BuildId, pad: usize| BuildIntentFact { traceparent: Some("x".repeat(pad)), ..intent(id) };
    // Two intents whose facts encode to 4053 bytes of JSON: with the wire
    // object and a nonce of 1 to 20 digits, 4073 to 4092 bytes of payload.
    let facts_len = |pad: usize| serde_json::to_vec(&vec![BuildFact::Intent(padded(&one, pad)), BuildFact::Intent(padded(&two, pad))]).unwrap().len();
    let pad = (4053 - facts_len(0)) / 2;
    let pad = if facts_len(pad) < 4053 { pad + 1 } else { pad };
    assert!((4052..=4054).contains(&facts_len(pad)), "{}", facts_len(pad));
    a.publish_intent(&padded(&one, pad)).await.unwrap();
    a.publish_intent(&padded(&two, pad)).await.unwrap();

    let (_c_ep, c_router, c) = admin(vec![addr(&a_ep)]).await;
    let mut both = false;
    for _ in 0..200 {
        if c.read_build(&one).await.is_ok() && c.read_build(&two).await.is_ok() {
            both = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(both, "C never received the two Builds: the catch-up message exceeded the gossip wire limit");

    // The connection survived: C hears a fact appended afterwards.
    a.claim_attempt(&BuildAttemptClaim { build_id: one.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let mut heard = false;
    for _ in 0..200 {
        if c.read_build(&one).await.is_ok_and(|v| v.attempt == 1) {
            heard = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(heard, "C stopped hearing A after the catch-up");
    a_router.shutdown().await.unwrap();
    c_router.shutdown().await.unwrap();
}

/// The test fabric's canonical id.
fn fabric1() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}
