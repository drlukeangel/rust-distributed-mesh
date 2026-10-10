//! `hydrate_before_ready` (i143.e12.s20): the one hook an app supplies to be born full. RDM runs
//! it after the join is accepted, the topology installed and the mesh channel joined, and gates
//! Ready on it. The authority here is the rig's admin side, serving a test app's pull behind the
//! gate of its own hook; the node is a real in-process rpc node.

use crate::common::{admin_side_serving, AdminSide, Spans};
use rafka_mesh_entity::digest::MeshDigest;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId};
use rafka_node_admin_core::app_hydration::{HookOutcome, Hydration, HydrationState, RetryOn};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{NodeState, Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::hydrate_probe::{Mode, PullServer, Row, TestHydrator};
use rafka_node_rpc_testkit::node::{self, RunningNode};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

/// A bound that only fires when the cell is wrong: every wait below ends on an event.
const WRONG: Duration = Duration::from_secs(60);

struct Birth {
    admin: AdminSide,
    authority: Arc<PullServer>,
    launch: Launch,
    dir: std::path::PathBuf,
}

fn rows() -> Vec<Row> {
    vec![Row { version: 1, value: "a".into() }, Row { version: 2, value: "b".into() }, Row { version: 3, value: "c".into() }]
}

/// An authority (the rig's admin side) in `status`, its pull gate closed, and the launch of one
/// rpc node it deployed.
async fn birth(status: MemberStatus) -> Birth {
    let authority = PullServer::new(rows());
    let fabric = FabricId::mint();
    let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, status, {
        let authority = authority.clone();
        move |b| authority.serve(b)
    })
    .await;
    let dir = std::env::temp_dir().join(format!("hydrate-before-ready-{}", NodeId::mint()));
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
    };
    admin.deployed(&launch, &key);
    Birth { admin, authority, launch, dir }
}

fn start(b: &Birth, hydrator: &Arc<TestHydrator>) -> tokio::task::JoinHandle<anyhow::Result<RunningNode>> {
    let (launch, hydration) = (b.launch.clone(), Hydration::new(hydrator.clone()));
    tokio::spawn(async move { node::start_hydrating(&launch, hydration, |b, _| b).await })
}

/// The statuses the authority heard of the node, in order, from the moment it is called.
fn observe(b: &Birth) -> Arc<std::sync::Mutex<Vec<MemberStatus>>> {
    let seen: Arc<std::sync::Mutex<Vec<MemberStatus>>> = Arc::default();
    let (book, id, out) = (b.admin.observer.membership.book.clone(), b.launch.node_id.to_string(), seen.clone());
    tokio::spawn(async move {
        let mut heard = book.heard_changes();
        loop {
            heard.borrow_and_update();
            if let Some((d, _)) = book.get(&id) {
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

/// Resolves once `pred` holds of `book`, re-checked on every change the book hears.
async fn until(book: &rafka_mesh_transport::membership::DigestBook, what: &str, pred: impl Fn(&rafka_mesh_transport::membership::DigestBook) -> bool) {
    let waited = async {
        let mut heard = book.heard_changes();
        loop {
            heard.borrow_and_update();
            if pred(book) {
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
        .filter(|(name, _, _)| name == "rdm.mesh.node.update.via-hydrate-attempt")
        .map(|(_, _, f)| (f.get("attempt").cloned().unwrap_or_default(), f.get("outcome").cloned().unwrap_or_default(), f.get("retry_on").cloned().unwrap_or_default()))
        .collect();
    found.sort();
    found.into_iter().map(|(_, outcome, retry_on)| (outcome, retry_on)).collect()
}

fn spans_named(spans: &Spans, name: &str) -> Vec<std::collections::BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, _, f)| f.clone()).collect()
}

/// One downward command from the authority to the node, once the node holds the authority as a
/// node-admin in its own book (the node refuses a command from a stranger).
async fn command(b: &Birth, req: impl Fn(&MeshDigest) -> StatusRequest) -> StatusReply {
    let book = &b.admin.observer.membership.book;
    until(book, "the authority heard the node's digest", |book| book.get(b.launch.node_id.as_str()).is_some()).await;
    let (d, _) = book.get(b.launch.node_id.as_str()).unwrap();
    b.admin.observer.resolver.insert(ResolvedNode {
        node_id: d.node.node_id.clone(),
        name: d.node.name.clone(),
        endpoint_id: d.node.endpoint_id.0.parse().unwrap(),
        incarnation: d.node.incarnation.clone(),
        transport_addr: d.node.transport_addr,
    });
    let target = NodeTarget::ExactNode(d.node.node_id.clone());
    let deadline = tokio::time::Instant::now() + WRONG;
    loop {
        let (out, _) = b.admin.observer.client.call::<Status>(&target, &req(&d), &CallOptions::default()).await;
        match out {
            RpcOutcome::Reply(r) if !matches!(r.value(), StatusReply::RejectedNotAuthority { .. }) => return r.into_value(),
            _ if tokio::time::Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(50)).await,
            other => panic!("the node never took the command: {other:?}"),
        }
    }
}

fn probe(d: &MeshDigest) -> StatusRequest {
    StatusRequest::ProbeNodeState { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }
}

fn drain(d: &MeshDigest) -> StatusRequest {
    StatusRequest::DrainNode { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), build_id: "bld_hydrate".into(), attempt: 1, operation: "drain-node:mesh1.rpc.1".into() }
}

fn tracing_to(spans: &Spans) -> tracing::subscriber::DefaultGuard {
    tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()))
}

/// CONTRACT: a node whose hook is `Blocked` on its authority stays Pending: it publishes Pending
/// and never ReadyForTraffic, its blocker is named on `rdm.mesh.node.reject.via-hydration-blocked`,
/// and the authority's own pull gate answered the pull typed `NotReady`. When the authority's hook
/// has passed and it publishes ReadyForTraffic, the hook runs again, returns `Ok`, the blocker span
/// closes naming the event that cleared it (`authority-ready`) and only then does the node publish
/// ReadyForTraffic (`via-ready`). What must NOT happen: the node Ready before its hook passed.
#[tokio::test]
async fn a_node_blocked_on_its_authority_is_never_ready_until_the_authority_is() {
    let spans = Spans::default();
    let _sub = tracing_to(&spans);
    let b = birth(MemberStatus::Pending).await;
    let seen = observe(&b);
    let hydrator = TestHydrator::new(Mode::Pull);
    let running = start(&b, &hydrator).await.unwrap().expect("a node whose hook is blocked still comes up, Pending");

    assert_eq!(*running.status.lock().unwrap(), MemberStatus::Pending, "a blocked node is Pending");
    until(&b.admin.observer.membership.book, "the authority heard the node Pending", |book| book.get(b.launch.node_id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::Pending)).await;
    assert!(!seen.lock().unwrap().contains(&MemberStatus::ReadyForTraffic), "ReadyForTraffic was published while the hook was blocked: {:?}", seen.lock().unwrap());
    assert_eq!(b.authority.answered(), 0, "the authority answered no rows before its own hook passed");
    assert_eq!(spans_named(&spans, "rdm.node_rpc.request.reject.via-hydration-not-passed").len(), 1, "the authority's gate refused the pull typed NotReady once");
    assert_eq!(attempts(&spans), vec![("blocked".to_string(), "authority-ready".to_string())]);
    let blocker = spans_named(&spans, "rdm.mesh.node.reject.via-hydration-blocked");
    assert_eq!(blocker.len(), 1, "the blocker is named once");
    assert!(blocker[0]["blocker"].contains("NotReady"), "the blocker names the authority's answer: {:?}", blocker[0]);
    assert!(spans_named(&spans, "rdm.mesh.node.update.via-ready").is_empty(), "no via-ready before the hook passed");
    let handle = running.hydration().expect("a registered hook has a handle");
    assert!(matches!(handle.state(), HydrationState::Blocked { retry_on: RetryOn::AuthorityReady, .. }), "{:?}", handle.state());

    b.authority.gate.open();
    b.admin.publish_status(MemberStatus::ReadyForTraffic).await;
    let settled = tokio::time::timeout(WRONG, handle.settled()).await.expect("the hook runs again when the authority is Ready");
    assert_eq!(settled, HydrationState::Passed);
    until(&b.admin.observer.membership.book, "the authority heard the node Ready", |book| book.get(b.launch.node_id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::ReadyForTraffic)).await;

    assert_eq!(*seen.lock().unwrap(), vec![MemberStatus::Pending, MemberStatus::ReadyForTraffic]);
    assert_eq!(attempts(&spans), vec![("blocked".to_string(), "authority-ready".to_string()), ("ok".to_string(), String::new())]);
    assert_eq!(spans_named(&spans, "rdm.mesh.node.reject.via-hydration-blocked")[0]["cleared_by"], "authority-ready");
    assert_eq!(spans_named(&spans, "rdm.mesh.node.update.via-ready").len(), 1, "via-ready follows the successful attempt");
    assert_eq!(hydrator.held().0.len(), 3, "the pull's rows are held");
    running.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&b.dir);
}

/// CONTRACT: a retry armed AFTER the event it waits on already happened still completes. The
/// authority becomes Ready while the first attempt is still running; when the attempt then returns
/// `Blocked { AuthorityReady }` the node runs the hook again at once, with no further event.
/// What must NOT happen: the node waits for an event that already went by.
#[tokio::test]
async fn a_retry_armed_after_the_authority_was_already_ready_still_completes() {
    let spans = Spans::default();
    let _sub = tracing_to(&spans);
    let b = birth(MemberStatus::Pending).await;
    let (started, resume) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
    let hydrator = TestHydrator::new(Mode::Pause { started: started.clone(), resume: resume.clone(), outcome: HookOutcome::Blocked { reason: "the authority was not Ready when this attempt asked".into(), retry_on: RetryOn::AuthorityReady } });
    let starting = start(&b, &hydrator);
    tokio::time::timeout(WRONG, started.notified()).await.expect("the first attempt began");
    let node_book = hydrator.book().expect("the attempt was handed the node's membership");

    // The event: the authority publishes Ready, and the node hears it, while the attempt runs.
    b.authority.gate.open();
    b.admin.publish_status(MemberStatus::ReadyForTraffic).await;
    until(&node_book, "the node heard its authority Ready", |book| book.get(b.admin.launcher.node_id.as_str()).is_some_and(|(d, _)| d.status == MemberStatus::ReadyForTraffic)).await;
    hydrator.set_mode(Mode::Local);
    resume.notify_one();

    let running = starting.await.unwrap().expect("the node comes up");
    let settled = tokio::time::timeout(WRONG, running.hydration().expect("handle").settled()).await.expect("the retry completes");
    assert_eq!(settled, HydrationState::Passed);
    assert_eq!(attempts(&spans), vec![("blocked".to_string(), "authority-ready".to_string()), ("ok".to_string(), String::new())]);
    assert_eq!(*running.status.lock().unwrap(), MemberStatus::ReadyForTraffic);
    running.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&b.dir);
}

/// CONTRACT: while its hook is blocked the node answers its authority's status probe with its
/// state (Pending) and obeys `drain-node` (`Applied`); the retirement cancels the hook: the
/// blocker span closes `retired`, the hook does not run again, and the node never publishes
/// ReadyForTraffic. What must NOT happen: `NotReady` for a lifecycle command (s15).
#[tokio::test]
async fn a_blocked_node_answers_status_and_drain_node_and_its_hook_ends() {
    let spans = Spans::default();
    let _sub = tracing_to(&spans);
    let b = birth(MemberStatus::Pending).await;
    let seen = observe(&b);
    let hydrator = TestHydrator::new(Mode::Pull);
    let running = start(&b, &hydrator).await.unwrap().expect("a blocked node still comes up, Pending");

    let probed = command(&b, probe).await;
    assert!(matches!(probed, StatusReply::Current { state: NodeState::Pending, .. }), "a Pending node answers its state: {probed:?}");
    assert_eq!(command(&b, drain).await, StatusReply::Applied, "drain-node is admitted while the hook is blocked");
    let handle = running.hydration().expect("handle");
    let settled = tokio::time::timeout(WRONG, handle.settled()).await.expect("the retirement ends the hook");
    assert_eq!(settled, HydrationState::Retired);
    assert_eq!(spans_named(&spans, "rdm.mesh.node.reject.via-hydration-blocked")[0]["cleared_by"], "retired");
    assert_eq!(hydrator.attempts.load(std::sync::atomic::Ordering::SeqCst), 1, "the hook did not run again");
    assert!(!seen.lock().unwrap().contains(&MemberStatus::ReadyForTraffic), "{:?}", seen.lock().unwrap());
    assert!(spans_named(&spans, "rdm.mesh.node.update.via-ready").is_empty());
    running.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&b.dir);
}

/// CONTRACT: a retire during hydration cancels the pull. The authority's gate is open and the pull
/// is held in flight; `drain-node` reaches the node; the attempt span closes `cancelled`, the pull
/// is dropped in flight (started = answered + dropped, nothing left running), its call span never
/// records an outcome, and the node never publishes ReadyForTraffic. What must NOT happen: the
/// pull answering into a node that was retired, or the node going Ready afterwards.
#[tokio::test]
async fn a_retire_during_the_pull_cancels_it_and_orphans_no_call() {
    let spans = Spans::default();
    let _sub = tracing_to(&spans);
    let b = birth(MemberStatus::ReadyForTraffic).await;
    b.authority.gate.open();
    b.authority.hold();
    let seen = observe(&b);
    let hydrator = TestHydrator::new(Mode::Pull);
    let starting = start(&b, &hydrator);
    tokio::time::timeout(WRONG, b.authority.entered(1)).await.expect("the pull is in flight at the authority");

    assert_eq!(command(&b, drain).await, StatusReply::Applied, "drain-node is admitted while the pull is in flight");
    let running = tokio::time::timeout(WRONG, starting).await.expect("the retirement ends the first attempt").unwrap().expect("the retired node's start returns");
    assert_eq!(running.hydration().expect("handle").state(), HydrationState::Retired);
    use std::sync::atomic::Ordering::SeqCst;
    assert_eq!((hydrator.pulls_started.load(SeqCst), hydrator.pulls_answered.load(SeqCst), hydrator.pulls_dropped.load(SeqCst)), (1, 0, 1), "the pull was dropped in flight");
    assert_eq!(attempts(&spans), vec![("cancelled".to_string(), String::new())]);
    let calls: Vec<_> = spans_named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|f| f.get("op").is_some_and(|o| o == "116")).collect();
    assert_eq!(calls.len(), 1, "one pull call was made");
    assert!(!calls[0].contains_key("outcome"), "the dropped call recorded no outcome: it ended with the hook, not with a reply: {:?}", calls[0]);
    b.authority.release();
    assert!(hydrator.held().0.is_empty(), "no row was applied after the retirement");
    assert!(!seen.lock().unwrap().contains(&MemberStatus::ReadyForTraffic), "{:?}", seen.lock().unwrap());
    assert!(spans_named(&spans, "rdm.mesh.node.update.via-ready").is_empty());
    running.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&b.dir);
}

/// CONTRACT: rows gossiped during the pull cause no duplicate state change or side effect. Row 2
/// arrives by gossip while the pull is in flight and again in the pull's answer; the app's
/// state changed once per version (1, 2 and 3 once each), the duplicate was ignored, and the app
/// emitted three effects. What must NOT happen: a version applied twice.
#[tokio::test]
async fn rows_gossiped_during_the_pull_change_the_state_once_per_version() {
    let spans = Spans::default();
    let _sub = tracing_to(&spans);
    let b = birth(MemberStatus::ReadyForTraffic).await;
    b.authority.gate.open();
    b.authority.hold();
    let hydrator = TestHydrator::new(Mode::Pull);
    let starting = start(&b, &hydrator);
    tokio::time::timeout(WRONG, b.authority.entered(1)).await.expect("the pull is in flight at the authority");

    hydrator.gossiped(Row { version: 2, value: "b".into() });
    b.authority.release();
    let running = starting.await.unwrap().expect("the node comes up");
    let (rows, changes, ignored) = hydrator.held();
    assert_eq!(rows.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
    assert_eq!(changes.into_iter().collect::<Vec<_>>(), vec![(1, 1), (2, 1), (3, 1)], "each version changed the state once");
    assert_eq!(ignored, 1, "row 2 arrived twice and the second was ignored");
    assert_eq!(hydrator.effects.load(std::sync::atomic::Ordering::SeqCst), 3, "one side effect per version");
    assert_eq!(*running.status.lock().unwrap(), MemberStatus::ReadyForTraffic);
    running.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&b.dir);
}

/// CONTRACT: a hook that returns `Failed` ends the node by name: `start` returns an error naming
/// the node and the hook's reason, the attempt span records `failed` with that reason, no
/// `via-ready` is emitted and the authority never hears the node Ready. What must NOT happen: the
/// node staying up Pending, or going Ready.
#[tokio::test]
async fn a_node_whose_hook_fails_ends_by_name_and_is_never_ready() {
    let spans = Spans::default();
    let _sub = tracing_to(&spans);
    let b = birth(MemberStatus::ReadyForTraffic).await;
    let seen = observe(&b);
    let hydrator = TestHydrator::new(Mode::Fixed(HookOutcome::Failed { reason: "the snapshot of kind `config` is not complete at its boundary".into() }));
    let why = match start(&b, &hydrator).await.unwrap() {
        Ok(running) => {
            running.stop(Duration::ZERO).await;
            panic!("a node whose hook failed must end, not come up");
        }
        Err(e) => e.to_string(),
    };
    assert!(why.contains("mesh1.rpc.1") && why.contains("hydrate_before_ready") && why.contains("is not complete at its boundary"), "the end names the node and the reason: {why}");
    let failed = spans_named(&spans, "rdm.mesh.node.update.via-hydrate-attempt");
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["outcome"], "failed");
    assert!(failed[0]["reason"].contains("is not complete at its boundary"));
    assert!(spans_named(&spans, "rdm.mesh.node.update.via-ready").is_empty());
    assert!(!seen.lock().unwrap().contains(&MemberStatus::ReadyForTraffic), "{:?}", seen.lock().unwrap());
    let _ = std::fs::remove_dir_all(&b.dir);
}

/// CONTRACT: the hook is handed the exact birth and the admin that accepted its join; nothing
/// about the context is the app's to guess. What must NOT happen: an authority-less context for a
/// node a launcher admitted.
#[tokio::test]
async fn the_hook_is_handed_the_accepting_authority_of_its_exact_birth() {
    let spans = Spans::default();
    let _sub = tracing_to(&spans);
    let b = birth(MemberStatus::ReadyForTraffic).await;
    let hydrator = TestHydrator::new(Mode::Local);
    let running = start(&b, &hydrator).await.unwrap().expect("the node comes up");
    assert_eq!(*hydrator.contexts.lock().unwrap(), vec![(1, Some("mesh1.admin.1".to_string()), None)]);
    assert_eq!(running.hydration().expect("handle").state(), HydrationState::Passed);
    running.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&b.dir);
}
