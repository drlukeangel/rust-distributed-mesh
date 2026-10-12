//! A node's gossip is shut down once, by its router (i143.e12.s34, #3060). `Router::shutdown`
//! runs every registered `ProtocolHandler::shutdown`, gossip's included, before it closes the
//! endpoint (iroh `protocol.rs` `Router::shutdown`, run loop `protocols.shutdown()`). A stop
//! that also calls `Gossip::shutdown` itself sends the gossip actor a second `Shutdown` after
//! the first one stopped it: that second call can only fail (iroh-gossip warns
//! `error while shutting down gossip`) or, when its message lands in the actor's channel after the
//! actor's receiver drained it, wait forever on a reply nobody can send, holding the router's run
//! loop inside `protocols.shutdown()` and the stop with it.
//!
//! Each cell runs on a current-thread runtime, so every task (the router's run loop, the gossip
//! actor) logs to the subscriber this test thread installed.

use crate::common::{admin_side, AdminSide};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId};
use rafka_node_admin_core::app_hydration::{HookOutcome, Hydration};
use rafka_node_rpc_testkit::hydrate_probe::{Mode, TestHydrator};
use rafka_node_rpc_testkit::node;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

/// Every event iroh-gossip logs: its target and message.
#[derive(Clone, Default)]
struct GossipEvents(Arc<Mutex<Vec<(String, String)>>>);

struct Message<'a>(&'a mut String);
impl tracing::field::Visit for Message<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            *self.0 = format!("{value:?}");
        }
    }
}

impl<S: tracing::Subscriber> Layer<S> for GossipEvents {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut message = String::new();
        event.record(&mut Message(&mut message));
        self.0.lock().unwrap().push((event.metadata().target().to_string(), message));
    }
}

impl GossipEvents {
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        let only_gossip = tracing_subscriber::filter::filter_fn(|m| m.target().starts_with("iroh_gossip"));
        tracing::subscriber::set_default(tracing_subscriber::registry().with(self.clone().with_filter(only_gossip)))
    }

    fn count(&self, what: &str) -> usize {
        self.0.lock().unwrap().iter().filter(|(_, m)| m.contains(what)).count()
    }

    fn all(&self) -> Vec<(String, String)> {
        self.0.lock().unwrap().clone()
    }
}

/// The gossip actor's own line when it takes a `Shutdown` (iroh-gossip `net.rs` event loop).
const ACTOR_TOOK_SHUTDOWN: &str = "received shutdown message, quit all topics";
/// iroh-gossip's line when a `Gossip::shutdown` call fails (its `ProtocolHandler::shutdown`).
const SHUTDOWN_FAILED: &str = "error while shutting down gossip";

fn launch_in(dir: &std::path::Path, fabric: &FabricId, admin: &AdminSide) -> Launch {
    Launch {
        fabric: "fabric1".into(),
        fabric_id: fabric.clone(),
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![admin.seed.clone()],
        launcher: Some(admin.launcher.clone()),
        data_dir: dir.to_path_buf(),
        store_dir: dir.to_path_buf(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
        mesh_issuer: None,
    }
}

async fn admitted(what: &str) -> (std::path::PathBuf, AdminSide, Launch) {
    let dir = std::env::temp_dir().join(format!("{what}-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
    let fabric = FabricId::mint();
    let admin = admin_side("127.0.0.1".parse().unwrap(), &fabric).await;
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = launch_in(&dir, &fabric, &admin);
    admin.deployed(&launch, &key);
    (dir, admin, launch)
}

fn assert_shut_down_once(events: &GossipEvents, how: &str) {
    assert_eq!(events.count(SHUTDOWN_FAILED), 0, "{how}: no gossip shutdown call may fail: {:?}", events.all());
    assert_eq!(events.count(ACTOR_TOOK_SHUTDOWN), 1, "{how}: the gossip actor takes exactly one Shutdown: {:?}", events.all());
}

// @feature: node-lifecycle
/// CONTRACT: `RunningNode::stop` shuts the node's gossip down once, through its router: the gossip
/// actor takes one `Shutdown`, and no shutdown call fails against an actor already stopped. What
/// must NOT happen: a second `Gossip::shutdown` after the first stopped the actor (it fails, or
/// strands its reply and holds the router's run loop for ever).
#[tokio::test]
async fn a_stopped_node_shuts_its_gossip_down_once_through_its_router() {
    let events = GossipEvents::default();
    let _sub = events.install();
    let (dir, _admin, launch) = admitted("gossip-once-stop").await;
    let running = node::start(&launch, |b, _| b).await.expect("the admitted node starts");
    tokio::time::timeout(Duration::from_secs(10), running.stop(Duration::ZERO)).await.expect("the stop ends");
    assert_shut_down_once(&events, "stop");
    let _ = std::fs::remove_dir_all(&dir);
}

// @feature: node-lifecycle
/// CONTRACT: `RunningNode::stop_commanded` (a parked process shutting down) shuts the node's gossip
/// down once, through its router.
#[tokio::test]
async fn a_parked_node_shutting_down_shuts_its_gossip_down_once_through_its_router() {
    let events = GossipEvents::default();
    let _sub = events.install();
    let (dir, _admin, launch) = admitted("gossip-once-parked").await;
    let running = node::start(&launch, |b, _| b).await.expect("the admitted node starts");
    tokio::time::timeout(Duration::from_secs(10), running.stop_commanded()).await.expect("the shutdown ends");
    assert_shut_down_once(&events, "stop_commanded");
    let _ = std::fs::remove_dir_all(&dir);
}

// @feature: node-lifecycle
/// CONTRACT: a node whose `hydrate_before_ready` hook fails ends at start by name, and its gossip
/// is shut down once, through its router.
#[tokio::test]
async fn a_node_whose_hook_fails_shuts_its_gossip_down_once_through_its_router() {
    let events = GossipEvents::default();
    let _sub = events.install();
    let (dir, _admin, launch) = admitted("gossip-once-hook-failed").await;
    let hydrator = TestHydrator::new(Mode::Fixed(HookOutcome::Failed { reason: "the cell's hook fails".into() }));
    let started = tokio::time::timeout(Duration::from_secs(10), node::start_hydrating(&launch, Hydration::new(hydrator), |b, _| b)).await.expect("the start ends");
    let why = started.err().expect("a failed hook ends the node").to_string();
    assert!(why.contains("failed hydrate_before_ready") && why.contains("the cell's hook fails"), "{why}");
    assert_shut_down_once(&events, "close");
    let _ = std::fs::remove_dir_all(&dir);
}
