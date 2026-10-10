//! A node ends its stop-node span before it asks its process to end (node-stop.md): a shutdown
//! that follows the request must not be able to take the span's evidence with it.
//!
//! Its own process: the cell installs the global tracing subscriber, and the process-wide stop
//! command it waits on is one per process.

use crate::common::admin_side_serving;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::node;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

const STOP_NODE: &str = "rdm.node_admin.status.update.via-stop-node";

/// A span's close that takes as long as a descheduled or loaded process takes to finish one: the
/// stop span's close blocks the closing thread for `CLOSE_TAKES`, then marks it ended.
struct SlowClose {
    ended: Arc<AtomicBool>,
}

const CLOSE_TAKES: Duration = Duration::from_millis(300);

impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> tracing_subscriber::Layer<S> for SlowClose {
    fn on_close(&self, id: tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if ctx.span(&id).is_some_and(|s| s.name() == STOP_NODE) {
            std::thread::sleep(CLOSE_TAKES);
            self.ended.store(true, Ordering::SeqCst);
        }
    }
}

/// CONTRACT: when the process is asked to end because a stop-node was served, the stop-node span
/// has already ended. What must NOT happen: the request to end made inside the span, so that
/// the shutdown it starts can finish before the span ends and the span is never recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stop_span_has_ended_before_the_process_is_asked_to_end() {
    if crate::own_process::delegated(module_path!(), "the_stop_span_has_ended_before_the_process_is_asked_to_end") {
        return;
    }
    let ended = Arc::new(AtomicBool::new(false));
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(SlowClose { ended: ended.clone() })).expect("this cell owns its process");

    let fabric = FabricId::mint();
    let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
    let dir = std::env::temp_dir().join(format!("stop-span-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id: fabric,
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![admin.seed.clone()],
        launcher: Some(admin.launcher.clone()),
        data_dir: dir.clone(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
        mesh_issuer: None,
    };
    admin.deployed(&launch, &key);
    let _running = node::start(&launch, |b, _| b).await.expect("the node comes up");

    // The node holds the authority as a node-admin in its own book once it has joined; the
    // command is sent until the node takes it.
    let book = &admin.observer.membership.book;
    let mut heard = book.heard_changes();
    while book.get(launch.node_id.as_str()).is_none() {
        heard.changed().await.unwrap();
    }
    let (d, _) = book.get(launch.node_id.as_str()).unwrap();
    admin.observer.resolver.insert(ResolvedNode {
        node_id: d.node.node_id.clone(),
        name: d.node.name.clone(),
        endpoint_id: d.node.endpoint_id.0.parse().unwrap(),
        incarnation: d.node.incarnation.clone(),
        transport_addr: d.node.transport_addr,
    });
    let target = NodeTarget::ExactNode(d.node.node_id.clone());
    let stop = StatusRequest::StopNode { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), build_id: "bld_stop_span".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() };
    loop {
        let (out, _) = admin.observer.client.call::<Status>(&target, &stop, &CallOptions::default()).await;
        match out {
            RpcOutcome::Reply(r) if matches!(r.value(), StatusReply::Applied | StatusReply::AlreadyApplied) => break,
            RpcOutcome::Reply(r) if matches!(r.value(), StatusReply::RejectedNotAuthority { .. }) => tokio::time::sleep(Duration::from_millis(50)).await,
            other => panic!("the node did not admit the stop: {other:?}"),
        }
    }

    rafka_node_admin_core::node_self::stop_command().wait().await;
    assert!(ended.load(Ordering::SeqCst), "the process was asked to end while the stop-node span had not ended");
    let _ = std::fs::remove_dir_all(&dir);
}
