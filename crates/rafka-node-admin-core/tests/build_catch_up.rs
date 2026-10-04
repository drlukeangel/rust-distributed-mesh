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
    let builds = Arc::new(FabricBuildStateAdapter::join(&gossip, &endpoint, "fabric1", peers).await.unwrap());
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
