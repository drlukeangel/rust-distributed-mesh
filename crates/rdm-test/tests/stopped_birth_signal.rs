//! A stopped birth's process ends on its termination signal without running its stop again
//! (node-stop.md step 6: "A stopped node leaves the stopped status only through node.start or
//! node.delete"; node-identity-and-state.md: "SIGINT and SIGTERM run the node's own stop steps", the
//! signal stop of a live node).
//!
//! The mesh's primary is a real Day-0 node-admin in this process; it launches an rpc node and commands
//! its stop. Its own process: the cell holds process-wide state (the stopped process's flag).

use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::node_hooks::{NodeHooks, HookOutcome};
use rdm_admin::join::Deployed;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::status::{NodeState, StatusReply, StatusRequest};
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, NodeId};
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);

pub(crate) fn day0_config(dir: &std::path::Path) -> AdminConfig {
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
pub(crate) fn launch_by(root: &Running, name: &str, dir: &std::path::Path) -> Launch {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
    Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
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
pub(crate) fn deployed(launch: &Launch) -> Deployed {
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
        listeners: Vec::new(),
    }
}

pub(crate) async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != MemberStatus::Ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published Ready"));
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
/// termination signal that follows (a delete's `TerminateRuntime`) ends its process without running its stop
/// again: its `node.connection.delete { All }` ran once, at the command, nothing is owed on the
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
        // The mesh's primary: a Day-0 node-admin that does not reconcile (it would delete the node this
        // cell launches in its own process).
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;

        let launch = launch_by(&root, "mesh1.rpc.1", &rpc_dir);
        let _ = root.runner.joins.expect(deployed(&launch));
        let passing = NodeHooks::new(TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok)));
        let rpc = rdm_test::node::start_pre_node_ready(&launch, passing, |b, _| b).await.expect("the rpc node comes up");
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
        let alls = of("rdm.node_admin.connection.delete.via-stop");
        assert_eq!(alls.len(), 1, "its node.connection.delete {{ All }} ran once, at the command: {alls:#?}");
        let owed = of("rdm.node_rpc.status.update.via-owed-on-signal");
        assert!(owed.is_empty(), "a stopped birth owes nothing on its signal: {owed:#?}");
        let held = root.status_authority().unwrap().declared.lock().unwrap().node(&launch.node_id, &launch.incarnation);
        assert_eq!(held, Some(NodeState::Stopped), "its mesh-primary holds it Stopped");

        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}

/// The spans this run of the cell wrote to its JSONL artifact: its own process's file
/// (`testkit.<pid>-<pid namespace inode>.spans.jsonl`), never an earlier run's beside it in the dir.
fn exported(dir: &std::path::Path) -> Vec<serde_json::Value> {
    let own = format!("testkit.{}-", std::process::id());
    std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("the cell's artifact dir {} is unreadable: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "jsonl") && p.file_name().is_some_and(|n| n.to_string_lossy().starts_with(&own)))
        .flat_map(|p| std::fs::read_to_string(p).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok()).collect::<Vec<_>>())
        .collect()
}

// @feature: node-lifecycle
/// CONTRACT (Luke 2026-10-27, i143-3209 inbox 0001: a node stop "should take less than a second", its
/// declaration delivered, with no `CANCELLED` `caller-gone` tail): a live rpc node's signal stop runs
/// node-stop.md's steps as its own subject (Q55), owes `ConnectionsDeleting` and then `Stopped` to its
/// mesh-primary (node-stop.md step 4: "an owed message ... sent whether or not the stop call's stream is
/// still open"), over the connection to its mesh-primary its transport cut spares (node-stop.md
/// "Transport cut", Q206), and ends once each is answered or its receiver is proven gone (Q206): each
/// declaration is applied by the mesh-primary and its call answered, the last within a second of the
/// stop's `node.connection.delete { All }`, and the process ends within a second of the signal.
/// Must NOT happen: a declaration's call ending `CANCELLED` `caller-gone` at the process's end, its
/// reply's connection cut under it by the stop's own transport cut (Rafka 141, trace
/// 447b70d402fa6dc7c3f601b684d6d8f4: ConnectionsDeleting applied at 6.4 ms, its reply's write
/// `Gone("connection lost")`, its call `caller-gone` at 3001 ms; `Stopped`'s dial `Failed("closed")`).
#[test]
fn a_live_births_signal_stop_delivers_its_declarations_within_a_second() {
    if crate::own_process::delegated(module_path!(), "a_live_births_signal_stop_delivers_its_declarations_within_a_second") {
        return;
    }
    const BOUND: Duration = Duration::from_secs(1);
    let cap = crate::common::capture("a_live_births_signal_stop_delivers_its_declarations_within_a_second");
    let (admin_id, stopped_in, held) = cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("live-birth-signal-{}", NodeId::mint()));
        let (root_dir, rpc_dir) = (base.join("admin1"), base.join("rpc1"));
        std::fs::create_dir_all(&root_dir).unwrap();
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;

        let launch = launch_by(&root, "mesh1.rpc.1", &rpc_dir);
        let _ = root.runner.joins.expect(deployed(&launch));
        let passing = NodeHooks::new(TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok)));
        let rpc = rdm_test::node::start_pre_node_ready(&launch, passing, |b, _| b).await.expect("the rpc node comes up");
        until_ready("mesh1.rpc.1", || {
            let mut d = rpc.digest.clone();
            d.status = *rpc.status.lock().unwrap();
            d
        })
        .await;

        // The signal a role binary takes, on a live birth: node.stop's steps, then its process ends.
        let began = std::time::Instant::now();
        tokio::time::timeout(WRONG, rpc.stop()).await.expect("the live node's process ends on its signal");
        let stopped_in = began.elapsed();
        let held = root.status_authority().unwrap().declared.lock().unwrap().node(&launch.node_id, &launch.incarnation);
        let admin_id = root.digest.lock().unwrap().node.node_id.clone();
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
        (admin_id, stopped_in, held)
    });
    cap.flush().expect("the cell's spans reach its artifact");
    let spans = exported(&cap.dir);
    let named = |name: &'static str| spans.iter().filter(move |s| s["name"] == name);
    let attr = |s: &serde_json::Value, k: &str| s["attributes"][k].as_str().unwrap_or_default().to_string();
    let (start, end) = (|s: &serde_json::Value| s["start_unix_nano"].as_u64().unwrap_or(0), |s: &serde_json::Value| s["end_unix_nano"].as_u64().unwrap_or(0));
    let all = named("rdm.node_admin.connection.delete.via-stop").find(|s| attr(s, "node") == "mesh1.rpc.1").expect("the signal stop's node.connection.delete { All }");
    let from = start(all);
    // Each declaration the stop owes, applied by the mesh-primary.
    for state in ["ConnectionsDeleting", "Stopped"] {
        let applied = named("rdm.node_admin.status.update.via-declaration").find(|s| attr(s, "sender") == "mesh1.rpc.1" && attr(s, "state") == state && attr(s, "outcome") == "applied");
        assert!(applied.is_some_and(|s| start(s) >= from), "the mesh-primary applied the stop's {state}");
    }
    // Every call the stopping node made to its mesh-primary from the stop on: answered, none cut short
    // by the process's end, the last answered inside the bound.
    let target = format!("ExactNode(NodeId({:?}))", admin_id.to_string());
    let calls: Vec<&serde_json::Value> = named("rdm.node_rpc.request.update.via-call").filter(|s| start(s) >= from && attr(s, "target") == target && attr(s, "op") == "27").collect();
    assert!(calls.len() >= 2, "the stop's two declarations are calls to the mesh-primary: {calls:#?}");
    for c in &calls {
        assert_eq!(attr(c, "code"), "OK", "a declaration's call is answered, never ended by the process's end: {c}");
    }
    let last = calls.iter().map(|c| end(c)).max().unwrap_or(from);
    let took = Duration::from_nanos(last.saturating_sub(from));
    assert!(took < BOUND, "the stop's declarations are answered {took:?} after its node.connection.delete {{ All }} began, past {BOUND:?}");
    assert!(stopped_in < BOUND, "the signal stop ended its process {stopped_in:?} after the signal, past {BOUND:?}");
    assert_eq!(held, Some(NodeState::Stopped), "its mesh-primary holds it Stopped from its declaration");
}
