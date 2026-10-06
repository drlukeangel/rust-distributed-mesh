//! i143.e1.s7 functional: the current desired topology travels the fabric's
//! control topic on its own, independent of Build history, over real
//! iroh-gossip.
//!
//! - An admin that joins after a Build completed and was forgotten receives
//!   the current desired revision (a neighbour's catch-up) and no Build
//!   history.
//! - Two admins that each proposed the next revision from the same base
//!   while apart converge on the same winner once they meet; the other
//!   revision is refused by name on both, and neither is merged.

use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, SecretKey};
use rafka_node_admin_core::build::{BuildId, BuildIntent, FabricDesired, MeshDesired};
use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildIntentFact, BuildStateAdapter};
use rafka_node_admin_core::desired::{DesiredStore, DesiredTopology, Standing};
use rafka_node_admin_core::fabric_builds::FabricBuildStateAdapter;
use rafka_node_admin_core::model::FabricId;
use std::sync::Arc;
use std::time::Duration;

fn fabric1() -> FabricId {
    FabricId::parse("0123456789ab").unwrap()
}

async fn admin(peers: Vec<EndpointAddr>, desired: Arc<DesiredStore>) -> (Endpoint, Router, Arc<FabricBuildStateAdapter>) {
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
    let builds = Arc::new(FabricBuildStateAdapter::join(&gossip, &endpoint, &fabric1(), peers, desired, rafka_node_admin_core::shutdown::ShutdownControl::memory("test-admin"), "test-admin".into()).await.unwrap());
    (endpoint, router, builds)
}

fn addr(e: &Endpoint) -> EndpointAddr {
    EndpointAddr::new(e.id()).with_ip_addr(e.bound_sockets().into_iter().find(|s| s.is_ipv4()).unwrap())
}

fn shape(rpc: u32) -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired { name: "mesh1".into(), node_admin: 2, rpc_node: rpc }] }
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..400 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("never: {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_admin_learns_the_current_desired_shape_without_the_build_that_set_it() {
    let root = DesiredTopology::root(fabric1(), "fabric1", "mesh1");
    let a_store = Arc::new(DesiredStore::holding(root.clone()));
    let (a_ep, a_router, a) = admin(vec![], a_store.clone()).await;

    // Build A sets the desired shape, converges, and is forgotten.
    let build = BuildId::mint();
    let next = root.next(shape(3), &build);
    a_store.offer(next.clone());
    a.publish_desired(&next).await.unwrap();
    a.publish_intent(&BuildIntentFact {
        build_id: build.clone(),
        intent: BuildIntent::ReconcileFabric { desired: shape(3) },
        traceparent: None,
        submitted_at_ms: 0,
        desired: Some(next.mark()),
        reason: None,
    })
    .await
    .unwrap();
    a.claim_attempt(&BuildAttemptClaim { build_id: build.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    a.append_attempt_receipt(&BuildAttemptReceipt { build_id: build.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    a.forget(&build).await.unwrap();
    assert!(a.read_build(&build).await.is_err(), "forgotten history");
    assert_eq!(a_store.current(), Some(next.clone()), "forget leaves the desired topology");

    // C joins after all of it, holding nothing.
    let c_store = Arc::new(DesiredStore::default());
    let (_c_ep, c_router, c) = admin(vec![addr(&a_ep)], c_store.clone()).await;
    eventually("C holds the current desired revision", || c_store.current().as_ref() == Some(&next)).await;
    assert!(c.read_build(&build).await.is_err(), "no Build history travels with it");
    assert!(c.facts().await.unwrap().is_empty(), "a completed Build's receipts are not shipped");
    c_router.shutdown().await.unwrap();
    a_router.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_updates_from_one_base_converge_on_one_winner_once_the_admins_meet() {
    let root = DesiredTopology::root(fabric1(), "fabric1", "mesh1");
    // Apart, each admin proposed revision 2 from revision 1.
    let won = root.next(shape(3), &BuildId("bld-aaaa".into()));
    let lost = root.next(shape(9), &BuildId("bld-zzzz".into()));
    let a_store = Arc::new(DesiredStore::holding(root.clone()));
    a_store.offer(won.clone());
    let b_store = Arc::new(DesiredStore::holding(root.clone()));
    b_store.offer(lost.clone());
    let (a_ep, a_router, _a) = admin(vec![], a_store.clone()).await;
    // They meet: each hands the other its current revision.
    let (_b_ep, b_router, _b) = admin(vec![addr(&a_ep)], b_store.clone()).await;
    eventually("B holds the winning revision", || b_store.current().as_ref() == Some(&won)).await;
    // Neither admin runs a Build on the losing revision, whether or not it
    // saw the fork itself; nothing was merged.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for s in [&a_store, &b_store] {
        assert_eq!(s.current(), Some(won.clone()));
        assert_eq!(s.standing(&lost.mark()), Standing::Lost);
        assert_eq!(s.standing(&won.mark()), Standing::Current);
    }
    assert!(b_store.refused(&lost.mark()), "the admin that wrote it refused it by name");
    b_router.shutdown().await.unwrap();
    a_router.shutdown().await.unwrap();
}
