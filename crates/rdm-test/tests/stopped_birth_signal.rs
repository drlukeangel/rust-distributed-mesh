//! A stopped birth's process ends on its termination signal without running its stop again
//! (node-stop.md step 6: "A stopped node leaves the stopped status only through node.start or
//! node.delete"; node-identity-and-state.md: "SIGINT and SIGTERM run the node's own stop steps", the
//! signal stop of a live node).
//!
//! The mesh's primary is a real Day-0 node-admin in this process; it launches an rpc node and commands
//! its stop. Its own process: the cell holds process-wide state (the stopped process's flag).

use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::before_ready::{BeforeReadyHook, HookOutcome};
use rdm_admin::join::Deployed;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::status::{NodeState, StatusReply, StatusRequest};
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, NodeId};
use rdm_test::before_ready_probe::{Mode, TestBeforeReady};
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" => Some(dir.clone()),
        "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// A launch of `name` by `root`, its data dir holding the runtime record its provider would write.
fn launch_by(root: &Running, name: &str, dir: &std::path::Path) -> Launch {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
    Launch {
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.to_path_buf(),
        store_dir: dir.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    }
}

/// What `root` verifies the join of `launch`'s birth against.
fn deployed(launch: &Launch) -> Deployed {
    let key = rdm_test::node::load_or_mint_key(&launch.data_dir).unwrap();
    Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: rdm_core::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).unwrap(),
        data_dir: launch.data_dir.display().to_string(),
        last_accepted_name: None,
        held: false,
    }
}

async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != MemberStatus::ReadyForTraffic {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published ReadyForTraffic"));
}

/// The `StopNode` `root` sends the birth `launch` names, over Node RPC as its mesh's primary sends it.
async fn stop(root: &Running, launch: &Launch) -> StatusReply {
    let req = StatusRequest::StopNode { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), build_id: "q4rbx1n7c2wd".into(), attempt: 1, operation: format!("node.stop:{}", launch.name) };
    let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: WRONG }, ..Default::default() };
    if let Some(reply) = crate::common::root_lifecycle_reply(root, launch, &req).await {
        return reply;
    }
    match root.node_rpc.client.call::<rdm_core::status::Status>(&rdm_transport::NodeTarget::ExactNode(launch.node_id.clone()), &req, &opts).await.0 {
        rdm_core::outcome::RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} to {} did not end in a reply: {other:?}", req.op(), launch.name),
    }
}

// @feature: node-lifecycle
/// CONTRACT: an rpc node stopped by its mesh-primary's `StopNode` keeps its process live, and the
/// termination signal that follows (a retire's `TerminateRuntime`) ends its process without running its stop
/// again: its `node.connections.delete { All }` ran once, at the command, nothing is owed on the
/// signal, and its mesh-primary holds it `Stopped`.
/// Must NOT happen: the signal running node-stop.md's steps a second time on the stopped birth and
/// owing `ConnectionsDeleting` after its `Stopped` (release run b7548453, rshape lifecycle:
/// mesh1.broker.3's `ConnectionsDeleting` refused `rejected-invalid-node-transition` after its
/// commanded stop's `Stopped` was applied).
#[test]
fn a_stopped_births_signal_runs_no_second_stop() {
    if crate::own_process::delegated(module_path!(), "a_stopped_births_signal_runs_no_second_stop") {
        return;
    }
    let cap = crate::common::capture("a_stopped_births_signal_runs_no_second_stop");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("stopped-birth-signal-{}", NodeId::mint()));
        let (root_dir, rpc_dir) = (base.join("admin1"), base.join("rpc1"));
        std::fs::create_dir_all(&root_dir).unwrap();
        // The mesh's primary: a Day-0 node-admin that does not reconcile (it would retire the node this
        // cell launches in its own process).
        let root = start_with(day0_config(&root_dir), Wiring { before_ready: BeforeReadyHook::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling();
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;

        let launch = launch_by(&root, "mesh1.rpc.1", &rpc_dir);
        let _ = root.runner.joins.expect(deployed(&launch));
        let passing = BeforeReadyHook::new(TestBeforeReady::new(Mode::Fixed(HookOutcome::Ok)));
        let rpc = rdm_test::node::start_before_ready(&launch, passing, |b, _| b).await.expect("the rpc node comes up");
        until_ready("mesh1.rpc.1", || {
            let mut d = rpc.digest.clone();
            d.status = *rpc.status.lock().unwrap();
            d
        })
        .await;
        assert!(matches!(stop(&root, &launch).await, StatusReply::Stopped { .. }), "the rpc node stops");

        // The signal a role binary takes: its process ends.
        tokio::time::timeout(WRONG, rpc.stop()).await.expect("the stopped node's process ends on its signal");

        let of = |name: &str| -> Vec<std::collections::BTreeMap<String, String>> {
            spans.0.lock().unwrap().values().filter(|(n, _, f)| n == name && f.get("node").map(String::as_str) == Some("mesh1.rpc.1")).map(|(_, _, f)| f.clone()).collect()
        };
        let alls = of("rdm.node_admin.node.connections.delete.via-stop");
        assert_eq!(alls.len(), 1, "its node.connections.delete {{ All }} ran once, at the command: {alls:#?}");
        let owed = of("rdm.node_rpc.status.update.via-owed-on-signal");
        assert!(owed.is_empty(), "a stopped birth owes nothing on its signal: {owed:#?}");
        let held = root.status_authority().unwrap().declared.lock().unwrap().node(&launch.node_id, &launch.incarnation);
        assert_eq!(held, Some(NodeState::Stopped), "its mesh-primary holds it Stopped");

        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
