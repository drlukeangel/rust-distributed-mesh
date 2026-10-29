//! `pre_node_ready` (i143.e12.s20): the one hook an app supplies to be born full. RDM runs
//! it after the join is accepted, the topology installed and the mesh channel joined, and gates
//! Ready on it. The authority here is the rig's admin side, serving a test app's pull behind the
//! gate of its own hook; the node is a real in-process rpc node.

use crate::common::{admin_side_serving, AdminSide, Spans};
use rdm_core::digest::MeshDigest;
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId};
use rdm_admin::node_hooks::{HookOutcome, NodeHooks, PreNodeReadyState, RetryOn};
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rdm_test::pre_node_ready_probe::{Mode, PullServer, Row, TestPreNodeReady};
use rdm_test::node::{self, RunningNode};
use std::sync::Arc;
use std::time::Duration;

/// A bound that only fires when the cell is wrong: every wait below ends on an event.
const WRONG: Duration = Duration::from_secs(60);

struct Birth {
    admin: AdminSide,
    primary: Arc<PullServer>,
    launch: Launch,
    dir: std::path::PathBuf,
}

fn rows() -> Vec<Row> {
    vec![Row { version: 1, value: "a".into() }, Row { version: 2, value: "b".into() }, Row { version: 3, value: "c".into() }]
}

/// An authority (the rig's admin side) in `status`, its pull gate closed, and the launch of one
/// rpc node it deployed.
async fn birth(status: MemberStatus) -> Birth {
    let primary = PullServer::new(rows());
    let fabric = FabricId::mint();
    let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, status, {
        let primary = primary.clone();
        move |b| primary.serve(b)
    })
    .await;
    let dir = std::env::temp_dir().join(format!("pre-node-ready-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
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
        store_dir: dir.clone(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    };
    admin.deployed(&launch, &key);
    Birth { admin, primary, launch, dir }
}

fn start(b: &Birth, pre_node_ready: &Arc<TestPreNodeReady>) -> tokio::task::JoinHandle<anyhow::Result<RunningNode>> {
    let (launch, pre_node_ready) = (b.launch.clone(), NodeHooks::new(pre_node_ready.clone()));
    tokio::spawn(async move { node::start_pre_node_ready(&launch, pre_node_ready, |b, _| b).await })
}

/// The statuses the authority heard of the node, in order, from the moment it is called.
fn observe(b: &Birth) -> Arc<std::sync::Mutex<Vec<MemberStatus>>> {
    let seen: Arc<std::sync::Mutex<Vec<MemberStatus>>> = Arc::default();
    let (digest_view, id, out) = (b.admin.observer.topology_view.digest_view.clone(), b.launch.node_id.to_string(), seen.clone());
    tokio::spawn(async move {
        let mut heard = digest_view.heard_changes();
        loop {
            heard.borrow_and_update();
            if let Some((d, _)) = digest_view.get(&id) {
                let mut seen = out.lock().unwrap();
                if seen.last() != Some(&d.status) {
                    seen.push(d.status);
                }
            }
            if heard.changed().await.is_err() {
                return;
            }
        }
    });
    seen
}

/// Resolves once `pred` holds of `digest_view`, re-checked on every change the digest view hears.
async fn until(digest_view: &rdm_transport::topology::DigestView, what: &str, pred: impl Fn(&rdm_transport::topology::DigestView) -> bool) {
    let waited = async {
        let mut heard = digest_view.heard_changes();
        loop {
            heard.borrow_and_update();
            if pred(digest_view) {
                return;
            }
            heard.changed().await.unwrap();
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("never happened: {what}"));
}

fn attempts(spans: &Spans) -> Vec<(String, String)> {
    let mut found: Vec<(String, String, String)> = spans
        .0
        .lock()
        .unwrap()
        .values()
        .filter(|(name, _, _)| name == "rdm.mesh.node.update.via-pre-node-ready-attempt")
        .map(|(_, _, f)| (f.get("attempt").cloned().unwrap_or_default(), f.get("outcome").cloned().unwrap_or_default(), f.get("retry_on").cloned().unwrap_or_default()))
        .collect();
    found.sort();
    found.into_iter().map(|(_, outcome, retry_on)| (outcome, retry_on)).collect()
}

fn spans_named(spans: &Spans, name: &str) -> Vec<std::collections::BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, _, f)| f.clone()).collect()
}

/// One downward command from the authority to the node, answered by the node whoever sends it
/// (node-rpc.md §14: nothing inside the mesh checks who is calling).
async fn command(b: &Birth, req: impl Fn(&MeshDigest) -> StatusRequest) -> StatusReply {
    let (d, target) = resolved(b).await;
    let (out, _) = b.admin.observer.client.call::<Status>(&target, &req(&d), &CallOptions::default()).await;
    match out {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("the node never took the command: {other:?}"),
    }
}

/// `node.connection.delete { All }` of the node, commanded on op 0x22 as its mesh-primary commands it.
async fn delete_all(b: &Birth) -> rdm_core::node_command::NodeCommandReply {
    let (d, target) = resolved(b).await;
    let all = rdm_core::node_command::NodeCommandRequest::DeleteConnections { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
    crate::common::node_command_completion(&b.admin.observer.client, &target, &all).await
}

/// The node's digest as the authority heard it, its birth resolved for a call.
async fn resolved(b: &Birth) -> (MeshDigest, NodeTarget) {
    let digest_view = &b.admin.observer.topology_view.digest_view;
    until(digest_view, "the authority heard the node's digest", |digest_view| digest_view.get(b.launch.node_id.as_str()).is_some()).await;
    let (d, _) = digest_view.get(b.launch.node_id.as_str()).unwrap();
    b.admin.observer.resolver.insert(ResolvedNode {
        node_id: d.node.node_id.clone(),
        name: d.node.name.clone(),
        endpoint_id: d.node.endpoint_id.0.parse().unwrap(),
        incarnation: d.node.incarnation.clone(),
        transport_addr: d.node.transport_addr,
    });
    let target = NodeTarget::ExactNode(d.node.node_id.clone());
    (d, target)
}

fn probe(d: &MeshDigest) -> StatusRequest {
    StatusRequest::ProbeNodeState { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }
}

/// CONTRACT: a node whose hook is `Blocked` on its authority stays Pending: it publishes Pending
/// and never Ready, its blocker is named on `rdm.mesh.node.reject.via-pre-node-ready-blocked`,
/// and the authority's own pull gate answered the pull typed `NotReady`. When the authority's hook
/// has passed and it publishes Ready, the hook runs again, returns `Ok`, the blocker span
/// closes naming the event that cleared it (`primary-ready`) and only then does the node publish
/// Ready (`via-ready`). What must NOT happen: the node Ready before its hook passed.
#[test]
fn a_node_blocked_on_its_mesh_primary_is_never_ready_until_the_mesh_primary_is() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_node_blocked_on_its_mesh_primary_is_never_ready_until_the_mesh_primary_is") {
        return;
    }
    let cap = crate::common::capture("a_node_blocked_on_its_mesh_primary_is_never_ready_until_the_mesh_primary_is");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let b = birth(MemberStatus::Pending).await;
        let seen = observe(&b);
        let pre_node_ready = TestPreNodeReady::new(Mode::Pull);
        let running = start(&b, &pre_node_ready).await.unwrap().expect("a node whose hook is blocked still comes up, Pending");

        assert_eq!(*running.status.lock().unwrap(), MemberStatus::Pending, "a blocked node is Pending");
        until(&b.admin.observer.topology_view.digest_view, "the authority heard the node Pending", |digest_view| digest_view.get(b.launch.node_id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::Pending)).await;
        assert!(!seen.lock().unwrap().contains(&MemberStatus::Ready), "Ready was published while the hook was blocked: {:?}", seen.lock().unwrap());
        assert_eq!(b.primary.answered(), 0, "the authority answered no rows before its own hook passed");
        assert_eq!(spans_named(&spans, "rdm.node_rpc.request.reject.via-pre-node-ready-not-passed").len(), 1, "the authority's gate refused the pull typed NotReady once");
        assert_eq!(attempts(&spans), vec![("blocked".to_string(), "primary-ready".to_string())]);
        let blocker = spans_named(&spans, "rdm.mesh.node.reject.via-pre-node-ready-blocked");
        assert_eq!(blocker.len(), 1, "the blocker is named once");
        assert!(blocker[0]["blocker"].contains("NotReady"), "the blocker names the authority's answer: {:?}", blocker[0]);
        assert!(spans_named(&spans, "rdm.mesh.node.update.via-ready").is_empty(), "no via-ready before the hook passed");
        let handle = running.pre_node_ready().expect("a registered hook has a handle");
        assert!(matches!(handle.state(), PreNodeReadyState::Blocked { retry_on: RetryOn::PrimaryReady, .. }), "{:?}", handle.state());

        b.primary.gate.open();
        b.admin.publish_status(MemberStatus::Ready).await;
        let settled = tokio::time::timeout(WRONG, handle.settled()).await.expect("the hook runs again when the authority is Ready");
        assert_eq!(settled, PreNodeReadyState::Passed);
        until(&b.admin.observer.topology_view.digest_view, "the authority heard the node Ready", |digest_view| digest_view.get(b.launch.node_id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::Ready)).await;

        assert_eq!(*seen.lock().unwrap(), vec![MemberStatus::Pending, MemberStatus::Ready]);
        assert_eq!(attempts(&spans), vec![("blocked".to_string(), "primary-ready".to_string()), ("ok".to_string(), String::new())]);
        assert_eq!(spans_named(&spans, "rdm.mesh.node.reject.via-pre-node-ready-blocked")[0]["cleared_by"], "primary-ready");
        assert_eq!(spans_named(&spans, "rdm.mesh.node.update.via-ready").len(), 1, "via-ready follows the successful attempt");
        assert_eq!(pre_node_ready.held().0.len(), 3, "the pull's rows are held");
        running.stop().await;
        let _ = std::fs::remove_dir_all(&b.dir);
    })
}

/// CONTRACT: a retry armed AFTER the event it waits on already happened still completes. The
/// authority becomes Ready while the first attempt is still running; when the attempt then returns
/// `Blocked { PrimaryReady }` the node runs the hook again at once, with no further event, and
/// then runs the rest of its start to Ready. What must NOT happen: the node waits for an
/// event that already went by.
#[test]
fn a_retry_armed_after_the_mesh_primary_was_already_ready_still_completes() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_retry_armed_after_the_mesh_primary_was_already_ready_still_completes") {
        return;
    }
    let cap = crate::common::capture("a_retry_armed_after_the_mesh_primary_was_already_ready_still_completes");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let b = birth(MemberStatus::Pending).await;
        let (started, resume) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
        let pre_node_ready = TestPreNodeReady::new(Mode::Pause { started: started.clone(), resume: resume.clone(), outcome: HookOutcome::Blocked { reason: "the authority was not Ready when this attempt asked".into(), retry_on: RetryOn::PrimaryReady } });
        let starting = start(&b, &pre_node_ready);
        tokio::time::timeout(WRONG, started.notified()).await.expect("the first attempt began");
        let node_view = pre_node_ready.digest_view().expect("the attempt was handed the node's topology view");

        // The event: the authority publishes Ready, and the node hears it, while the attempt runs.
        b.primary.gate.open();
        b.admin.publish_status(MemberStatus::Ready).await;
        until(&node_view, "the node heard its authority Ready", |digest_view| digest_view.get(b.admin.launcher.node_id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::Ready)).await;
        pre_node_ready.set_mode(Mode::Local);
        resume.notify_one();

        let running = starting.await.unwrap().expect("the node comes up");
        let settled = tokio::time::timeout(WRONG, running.pre_node_ready().expect("handle").settled()).await.expect("the retry completes");
        assert_eq!(settled, PreNodeReadyState::Passed);
        assert_eq!(attempts(&spans), vec![("blocked".to_string(), "primary-ready".to_string()), ("ok".to_string(), String::new())]);
        // node.started follows the hook's pass by the start's topology, Build-floor and rafka-time steps
        // (node-start.md steps 4-5): the node is Ready once its authority hears it so.
        until(&b.admin.observer.topology_view.digest_view, "the authority heard the node Ready", |digest_view| digest_view.get(b.launch.node_id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::Ready)).await;
        assert_eq!(*running.status.lock().unwrap(), MemberStatus::Ready);
        running.stop().await;
        let _ = std::fs::remove_dir_all(&b.dir);
    })
}

/// CONTRACT: while its hook is blocked the node answers its authority's status probe with Status
/// `Blocked { node_id, incarnation, reason, retry_on }` (node-start.md step 3): the exact birth, the
/// hook's own reason and the event it runs again on. It serves `node.connection.delete { All }`
/// (`ConnectionsDeleted`); the deletion at its admission cancels the hook: the blocker span closes `deleted`, the hook does not run again, and
/// the node never publishes Ready. What must NOT happen: `Current` or `NotReady` for a
/// blocked node's probe (the executor could not name the blocker, or would take its door for it), or
/// `NotReady` for a lifecycle command (s15).
#[test]
fn a_blocked_node_answers_status_and_connections_delete_and_its_hook_ends() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_blocked_node_answers_status_and_connections_delete_and_its_hook_ends") {
        return;
    }
    let cap = crate::common::capture("a_blocked_node_answers_status_and_connections_delete_and_its_hook_ends");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let b = birth(MemberStatus::Pending).await;
        let seen = observe(&b);
        let pre_node_ready = TestPreNodeReady::new(Mode::Pull);
        let running = start(&b, &pre_node_ready).await.unwrap().expect("a blocked node still comes up, Pending");

        let handle = running.pre_node_ready().expect("handle");
        let PreNodeReadyState::Blocked { reason: hook_reason, .. } = handle.state() else { panic!("the hook is blocked: {:?}", handle.state()) };
        let probed = command(&b, probe).await;
        let d = running.digest.clone();
        assert_eq!(probed, StatusReply::Blocked { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), reason: hook_reason.clone(), retry_on: RetryOn::PrimaryReady }, "a blocked node answers Blocked: the exact birth, the hook's own reason and its retry event");
        assert!(matches!(delete_all(&b).await, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { .. }), "node.connection.delete {{ All }} is answered while the hook is blocked");
        let settled = tokio::time::timeout(WRONG, handle.settled()).await.expect("the deletion ends the hook");
        assert_eq!(settled, PreNodeReadyState::Cancelled);
        assert_eq!(spans_named(&spans, "rdm.mesh.node.reject.via-pre-node-ready-blocked")[0]["cleared_by"], "cancelled");
        assert_eq!(pre_node_ready.attempts.load(std::sync::atomic::Ordering::SeqCst), 1, "the hook did not run again");
        assert!(!seen.lock().unwrap().contains(&MemberStatus::Ready), "{:?}", seen.lock().unwrap());
        assert!(spans_named(&spans, "rdm.mesh.node.update.via-ready").is_empty());
        running.stop().await;
        let _ = std::fs::remove_dir_all(&b.dir);
    })
}

/// CONTRACT: a delete during the hook cancels the pull. The authority's gate is open and the pull
/// is held in flight; `node.connection.delete { All }` reaches the node; the attempt span closes `cancelled`, the pull
/// is dropped in flight (started = answered + dropped, nothing left running), its call span never
/// records an outcome, and the node never publishes Ready. What must NOT happen: the
/// pull answering into a node that was deleted, or the node going Ready afterwards.
#[test]
fn a_connections_delete_during_the_pull_cancels_it_and_orphans_no_call() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_connections_delete_during_the_pull_cancels_it_and_orphans_no_call") {
        return;
    }
    let cap = crate::common::capture("a_connections_delete_during_the_pull_cancels_it_and_orphans_no_call");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let b = birth(MemberStatus::Ready).await;
        b.primary.gate.open();
        b.primary.hold();
        let seen = observe(&b);
        let pre_node_ready = TestPreNodeReady::new(Mode::Pull);
        let starting = start(&b, &pre_node_ready);
        tokio::time::timeout(WRONG, b.primary.entered(1)).await.expect("the pull is in flight at the authority");

        assert!(matches!(delete_all(&b).await, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { .. }), "node.connection.delete {{ All }} is answered while the pull is in flight");
        let running = tokio::time::timeout(WRONG, starting).await.expect("the deletion ends the first attempt").unwrap().expect("the deleted node's start returns");
        assert_eq!(running.pre_node_ready().expect("handle").state(), PreNodeReadyState::Cancelled);
        use std::sync::atomic::Ordering::SeqCst;
        assert_eq!((pre_node_ready.pulls_started.load(SeqCst), pre_node_ready.pulls_answered.load(SeqCst), pre_node_ready.pulls_dropped.load(SeqCst)), (1, 0, 1), "the pull was dropped in flight");
        assert_eq!(attempts(&spans), vec![("cancelled".to_string(), String::new())]);
        let calls: Vec<_> = spans_named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|f| f.get("op").is_some_and(|o| o == "116")).collect();
        assert_eq!(calls.len(), 1, "one pull call was made");
        assert!(!calls[0].contains_key("outcome"), "the dropped call recorded no outcome: it ended with the hook, not with a reply: {:?}", calls[0]);
        b.primary.release();
        assert!(pre_node_ready.held().0.is_empty(), "no row was applied after the deletion");
        assert!(!seen.lock().unwrap().contains(&MemberStatus::Ready), "{:?}", seen.lock().unwrap());
        assert!(spans_named(&spans, "rdm.mesh.node.update.via-ready").is_empty());
        running.stop().await;
        let _ = std::fs::remove_dir_all(&b.dir);
    })
}

/// CONTRACT: rows gossiped during the pull cause no duplicate state change or side effect. Row 2
/// arrives by gossip while the pull is in flight and again in the pull's answer; the app's
/// state changed once per version (1, 2 and 3 once each), the duplicate was ignored, and the app
/// emitted three effects. What must NOT happen: a version applied twice.
#[test]
fn rows_gossiped_during_the_pull_change_the_state_once_per_version() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "rows_gossiped_during_the_pull_change_the_state_once_per_version") {
        return;
    }
    let cap = crate::common::capture("rows_gossiped_during_the_pull_change_the_state_once_per_version");
    cap.run(crate::common::Threads::Current, async move {
        let b = birth(MemberStatus::Ready).await;
        b.primary.gate.open();
        b.primary.hold();
        let pre_node_ready = TestPreNodeReady::new(Mode::Pull);
        let starting = start(&b, &pre_node_ready);
        tokio::time::timeout(WRONG, b.primary.entered(1)).await.expect("the pull is in flight at the authority");

        pre_node_ready.gossiped(Row { version: 2, value: "b".into() });
        b.primary.release();
        let running = starting.await.unwrap().expect("the node comes up");
        let (rows, changes, ignored) = pre_node_ready.held();
        assert_eq!(rows.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(changes.into_iter().collect::<Vec<_>>(), vec![(1, 1), (2, 1), (3, 1)], "each version changed the state once");
        assert_eq!(ignored, 1, "row 2 arrived twice and the second was ignored");
        assert_eq!(pre_node_ready.effects.load(std::sync::atomic::Ordering::SeqCst), 3, "one side effect per version");
        assert_eq!(*running.status.lock().unwrap(), MemberStatus::Ready);
        running.stop().await;
        let _ = std::fs::remove_dir_all(&b.dir);
    })
}

/// CONTRACT: a hook that returns `Failed` ends the node by name: `start` returns an error naming
/// the node and the hook's reason, the attempt span records `failed` with that reason, no
/// `via-ready` is emitted and the authority never hears the node Ready. What must NOT happen: the
/// node staying up Pending, or going Ready.
#[test]
fn a_node_whose_hook_fails_ends_by_name_and_is_never_ready() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_node_whose_hook_fails_ends_by_name_and_is_never_ready") {
        return;
    }
    let cap = crate::common::capture("a_node_whose_hook_fails_ends_by_name_and_is_never_ready");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let b = birth(MemberStatus::Ready).await;
        let seen = observe(&b);
        let pre_node_ready = TestPreNodeReady::new(Mode::Fixed(HookOutcome::Failed { reason: "the snapshot of kind `config` is not complete at its boundary".into() }));
        let why = match start(&b, &pre_node_ready).await.unwrap() {
            Ok(running) => {
                running.stop().await;
                panic!("a node whose hook failed must end, not come up");
            }
            Err(e) => e.to_string(),
        };
        assert!(why.contains("mesh1.rpc.1") && why.contains("pre_node_ready") && why.contains("is not complete at its boundary"), "the end names the node and the reason: {why}");
        let failed = spans_named(&spans, "rdm.mesh.node.update.via-pre-node-ready-attempt");
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0]["outcome"], "failed");
        assert!(failed[0]["reason"].contains("is not complete at its boundary"));
        assert!(spans_named(&spans, "rdm.mesh.node.update.via-ready").is_empty());
        assert!(!seen.lock().unwrap().contains(&MemberStatus::Ready), "{:?}", seen.lock().unwrap());
        let _ = std::fs::remove_dir_all(&b.dir);
    })
}

/// CONTRACT (fabric-node-hooks.md "Outcomes and the gate": `Blocked { reason, retry_on }` "the reason
/// is named in the blocker evidence and answered to the executor's ProbeNodeState"): a hook that
/// returns `Blocked` with no reason names no blocker, so it is no Blocked outcome: the node ends by
/// name, saying its hook was blocked with no reason, the attempt span records `failed` with that
/// reason, no `via-pre-node-ready-blocked` span is opened, no `via-ready` is emitted and the authority
/// never hears the node Ready. What must NOT happen: the node staying up Pending and answering its
/// executor's probe `Blocked` with an empty reason, which the executor frames as a Build stream's
/// `Blocked { WaitForNodeReady, reason }` that names nothing.
#[test]
fn a_node_whose_hook_is_blocked_with_no_reason_ends_by_name_and_is_never_ready() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_node_whose_hook_is_blocked_with_no_reason_ends_by_name_and_is_never_ready") {
        return;
    }
    let cap = crate::common::capture("a_node_whose_hook_is_blocked_with_no_reason_ends_by_name_and_is_never_ready");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let b = birth(MemberStatus::Ready).await;
        let seen = observe(&b);
        let pre_node_ready = TestPreNodeReady::new(Mode::Fixed(HookOutcome::Blocked { reason: String::new(), retry_on: RetryOn::PrimaryAddressed }));
        let why = match start(&b, &pre_node_ready).await.unwrap() {
            Ok(running) => {
                let state = running.pre_node_ready().map(|h| h.state());
                running.stop().await;
                panic!("a node whose hook was blocked with no reason must end by name, not come up: its hook state {state:?}");
            }
            Err(e) => e.to_string(),
        };
        assert!(why.contains("mesh1.rpc.1") && why.contains("pre_node_ready") && why.contains("no reason"), "the end names the node, the hook and that it gave no reason: {why}");
        let attempts = spans_named(&spans, "rdm.mesh.node.update.via-pre-node-ready-attempt");
        assert_eq!(attempts.len(), 1, "the hook ran once: {attempts:?}");
        assert_eq!(attempts[0]["outcome"], "failed");
        assert!(attempts[0]["reason"].contains("no reason") && attempts[0]["reason"].contains("primary-addressed"), "the attempt names the refusal and the event the hook asked for: {attempts:?}");
        assert!(spans_named(&spans, "rdm.mesh.node.reject.via-pre-node-ready-blocked").is_empty(), "no blocker was opened for a reason that names nothing");
        assert!(spans_named(&spans, "rdm.mesh.node.update.via-ready").is_empty());
        assert!(!seen.lock().unwrap().contains(&MemberStatus::Ready), "{:?}", seen.lock().unwrap());
        let _ = std::fs::remove_dir_all(&b.dir);
    })
}

/// CONTRACT: the hook is handed the exact birth and the admin that accepted its join; nothing
/// about the context is the app's to guess. What must NOT happen: an authority-less context for a
/// node a launcher admitted.
#[test]
fn the_hook_is_handed_the_accepting_mesh_primary_of_its_exact_birth() {
    let cap = crate::common::capture("the_hook_is_handed_the_accepting_mesh_primary_of_its_exact_birth");
    cap.run(crate::common::Threads::Current, async move {
        let b = birth(MemberStatus::Ready).await;
        let pre_node_ready = TestPreNodeReady::new(Mode::Local);
        let running = start(&b, &pre_node_ready).await.unwrap().expect("the node comes up");
        assert_eq!(*pre_node_ready.contexts.lock().unwrap(), vec![(1, Some("mesh1.admin.1".to_string()), None)]);
        assert_eq!(running.pre_node_ready().expect("handle").state(), PreNodeReadyState::Passed);
        running.stop().await;
        let _ = std::fs::remove_dir_all(&b.dir);
    })
}

/// CONTRACT: the launcher's `WaitForNodeReady` observer (`rig::LiveMesh::ready`) answers ready for a
/// birth only once that birth's own digest says Ready, as the node-admin's own observer
/// does. A node whose hook is blocked is Pending and still answers Ping (it is on the mesh and
/// serves status and its node commands while Pending): the observer names its Pending. What
/// must NOT happen: a Ping a Pending node answers standing in for ready, which lets a create return
/// while the birth is still Pending.
#[test]
fn the_launchers_ready_waits_for_the_births_own_ready_not_a_ping() {
    let cap = crate::common::capture("the_launchers_ready_waits_for_the_births_own_ready_not_a_ping");
    cap.run(crate::common::Threads::Current, async move {
        use rdm_admin::deployment::pipeline::NodeObserver;
        let b = birth(MemberStatus::Pending).await;
        let pre_node_ready = TestPreNodeReady::new(Mode::Pull);
        let running = start(&b, &pre_node_ready).await.unwrap().expect("a blocked node still comes up, Pending");
        let digest_view = &b.admin.observer.topology_view.digest_view;
        until(digest_view, "the authority heard the node's Pending digest", |digest_view| digest_view.get(b.launch.node_id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::Pending)).await;
        let (d, _) = digest_view.get(b.launch.node_id.as_str()).unwrap();
        let mut node = rdm_admin::model::Node::allocated(d.node.name.clone());
        node.node_id = d.node.node_id.clone();
        node.endpoint_id = Some(d.node.endpoint_id.clone());
        node.incarnation_id = Some(d.node.incarnation.clone());
        node.transport_addr = Some(d.node.transport_addr);
        let target = b.admin.observer.echo_target(&node).unwrap();
        assert!(matches!(b.admin.observer.echo(&target).await, RpcOutcome::Reply(_)), "the Pending node answers Ping");
        let ready = b.admin.observer.ready(&node).await;
        let why = ready.expect_err("a Pending birth is not ready");
        assert!(why.contains("mesh1.rpc.1") && why.contains("Pending"), "the refusal names the birth and its Pending: {why}");
        running.stop().await;
        let _ = std::fs::remove_dir_all(&b.dir);
    })
}
