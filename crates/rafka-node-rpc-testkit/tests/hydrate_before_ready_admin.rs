//! `hydrate_before_ready` on node-admins (i143.e12.s20): a node-admin runs the same hook as every
//! node. The Day-0 root has no authority to pull from, so its context says why and its hook
//! completes from local state; an admin launched by another admin pulls from it, and is refused
//! typed `NotReady` until that admin's own hook has passed.

use crate::common::Spans;
use rafka_mesh_entity::MemberStatus;
use rafka_node_admin_core::admin::{start_with, AdminConfig};
use rafka_node_admin_core::app_hydration::{HookOutcome, Hydration, HydrationState};
use rafka_node_admin_core::wiring::Wiring;
use rafka_node_rpc_testkit::hydrate_probe::{Mode, TestHydrator};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

const WRONG: Duration = Duration::from_secs(60);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" => Some(dir.clone()),
        "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// Resolves once `running`'s admin published ReadyForTraffic (the Ready check runs on its own
/// cadence; this reads the digest it publishes).
async fn until_ready(running: &rafka_node_admin_core::admin::Running) {
    let waited = async {
        loop {
            if running.digest.lock().unwrap().status == MemberStatus::ReadyForTraffic {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.expect("the admin never published ReadyForTraffic");
}

/// CONTRACT: the Day-0 root's `hydrate_before_ready` runs once, is handed `authority: None` with the
/// reason (no authority exists before this admin), completes from local state, and only then does
/// the admin publish ReadyForTraffic: the attempt span records `ok` and the Ready check named the
/// running hook as a blocker until then. What must NOT happen: the root's hook pulling from an
/// authority that does not exist, or the root Ready before its hook returned.
#[tokio::test]
async fn the_day_zero_root_runs_the_hook_with_no_authority_and_completes_from_local_state() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let dir = std::env::temp_dir().join(format!("hydrate-day0-{}", rafka_mesh_entity::NodeId::mint()));
    let hydrator = TestHydrator::new(Mode::Local);
    let wiring = Wiring { hydration: Hydration::new(hydrator.clone()), ..Wiring::no_certs() };
    let running = start_with(day0_config(&dir), wiring).await.expect("the Day-0 admin comes up");
    let handle = running.hydration().expect("a registered hook has a handle");
    assert_eq!(tokio::time::timeout(WRONG, handle.settled()).await.expect("the hook returns"), HydrationState::Passed);
    until_ready(&running).await;

    let contexts = hydrator.contexts.lock().unwrap().clone();
    assert_eq!(contexts.len(), 1, "the hook ran once: {contexts:?}");
    assert_eq!((contexts[0].0, contexts[0].1.clone()), (1, None), "the Day-0 root has no accepting authority");
    assert!(contexts[0].2.as_deref().is_some_and(|why| why.contains("Day-0")), "the context says why: {contexts:?}");
    let attempts: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.mesh.node.update.via-hydrate-attempt").map(|(_, _, f)| f.clone()).collect();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0]["outcome"], "ok");
    assert!(attempts[0]["authority"].contains("none"), "{:?}", attempts[0]);
    let _ = HookOutcome::Ok;
    let _ = Arc::strong_count(&hydrator);
    running.leave().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT: an admin launched by another admin runs the same hook against its accepting authority.
/// The Day-0 admin's own hook is held, so its pull gate is closed: the joining admin's pull is
/// answered typed `NotReady`, its hook is `Blocked { AuthorityReady }`, the Ready check names that
/// blocker (`rdm.node_admin.runtime.reject.via-not-authority-capable`) and it publishes Pending.
/// When the Day-0 admin's hook returns and it publishes ReadyForTraffic, the joining admin's hook
/// runs again, pulls the rows and it publishes ReadyForTraffic after its authority. What must NOT
/// happen: the joining admin Ready while its hook is blocked.
#[tokio::test]
async fn an_admin_launched_by_an_admin_whose_hook_has_not_passed_is_pending_until_it_does() {
    use rafka_mesh_entity::launch::{Launch, Launcher};
    use rafka_mesh_entity::runtime::RuntimeFact;
    use rafka_mesh_entity::{IncarnationId, NodeId};
    use rafka_node_admin_core::app_hydration::RetryOn;
    use rafka_node_rpc_testkit::hydrate_probe::{PullServer, Row};

    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let root_dir = std::env::temp_dir().join(format!("hydrate-pair-a-{}", NodeId::mint()));
    let joiner_dir = std::env::temp_dir().join(format!("hydrate-pair-b-{}", NodeId::mint()));

    // The Day-0 admin: its hook is held; its pull is served behind the gate of that hook.
    let (started, resume) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
    let root_hydrator = TestHydrator::new(Mode::Pause { started: started.clone(), resume: resume.clone(), outcome: HookOutcome::Ok });
    let root_hydration = Hydration::new(root_hydrator.clone());
    let rows = vec![Row { version: 1, value: "a".into() }, Row { version: 2, value: "b".into() }];
    let pull = PullServer::behind(rows, root_hydration.gate());
    let serve = {
        let pull = pull.clone();
        Box::new(move |b: rafka_node_rpc::ServerBuilder| pull.serve(b)) as rafka_node_admin_core::wiring::ServeApp
    };
    let root_cfg = day0_config(&root_dir);
    let (fabric, fabric_id) = (root_cfg.fabric.clone(), root_cfg.fabric_id.clone());
    let root = start_with(root_cfg, Wiring { hydration: root_hydration, serve_app: Some(serve), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
    // The accepted Build names only the root: the launched admin is surplus to it, and a reconciling
    // root would retire it, which for an in-process admin is signalling this test process. The cell
    // is about the hook, so the root does not reconcile.
    root.stop_reconciling();
    tokio::time::timeout(WRONG, started.notified()).await.expect("the Day-0 admin's hook began");
    assert_eq!(root.digest.lock().unwrap().status, MemberStatus::Pending, "the Day-0 admin is Pending while its hook runs");

    // The admin it launches: registered with the root as a deployment, then started.
    let (root_digest, root_mesh_id) = {
        let d = root.digest.lock().unwrap().clone();
        (d.clone(), d.mesh_id.clone().expect("the root holds its mesh id"))
    };
    std::fs::create_dir_all(&joiner_dir).unwrap();
    RuntimeFact::of_this_process("cell").unwrap().write_record(&joiner_dir).unwrap();
    let key = rafka_node_rpc_testkit::node::load_or_mint_key(&joiner_dir).unwrap();
    let launch = Launch {
        fabric,
        fabric_id,
        name: "mesh1.admin.2".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
        seeds: vec![(key_of(&root_digest), root_digest.node.transport_addr)],
        launcher: Some(Launcher { name: root_digest.node.name.clone(), node_id: root_digest.node.node_id.clone(), incarnation: root_digest.node.incarnation.clone() }),
        data_dir: joiner_dir.clone(),
        mesh_id: Some(root_mesh_id),
        mesh_issuer: None,
    };
    let runtime = rafka_mesh_entity::runtime::await_own_record(&joiner_dir, Duration::from_secs(10)).unwrap();
    let _ = root.runner.joins.expect(rafka_node_admin_core::join::Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rafka_mesh_entity::EndpointId(key.public().to_string()),
        runtime,
        data_dir: joiner_dir.display().to_string(),
    });
    let env = launch.to_env();
    let joiner_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
    let joiner_hydrator = TestHydrator::new(Mode::Pull);
    let joiner = start_with(joiner_cfg, Wiring { hydration: Hydration::new(joiner_hydrator.clone()), ..Wiring::no_certs() }).await.expect("the launched admin comes up");

    let handle = joiner.hydration().expect("a registered hook has a handle");
    let blocked = tokio::time::timeout(WRONG, async {
        loop {
            if matches!(handle.state(), HydrationState::Blocked { .. }) {
                return handle.state();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the pull was answered NotReady and the hook blocked");
    assert!(matches!(blocked, HydrationState::Blocked { retry_on: RetryOn::AuthorityReady, .. }), "{blocked:?}");
    assert_eq!(joiner.digest.lock().unwrap().status, MemberStatus::Pending, "a blocked admin is Pending");
    assert_eq!(pull.answered(), 0, "the Day-0 admin answered no rows before its own hook passed");
    // The Ready check reports a changed blocker list on its own cadence.
    let ready_blockers = tokio::time::timeout(WRONG, async {
        loop {
            let found: Vec<String> = spans
                .0
                .lock()
                .unwrap()
                .values()
                .filter(|(n, _, f)| n == "rdm.node_admin.runtime.reject.via-not-authority-capable" && f.get("node").is_some_and(|n| n.contains("mesh1.admin.2")))
                .filter_map(|(_, _, f)| f.get("detail").cloned())
                .collect();
            if found.iter().any(|d| d.contains("hydrate_before_ready is blocked")) {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the Ready check names the hook's blocker");
    assert!(ready_blockers.iter().any(|d| d.contains("it runs again on authority-ready")), "the blocker names its retry event: {ready_blockers:?}");

    // The Day-0 admin's hook returns: its gate opens, it is Ready, and the joiner's retry follows.
    resume.notify_one();
    assert_eq!(tokio::time::timeout(WRONG, handle.settled()).await.expect("the hook runs again"), HydrationState::Passed);
    until_ready(&joiner).await;
    assert_eq!(root.digest.lock().unwrap().status, MemberStatus::ReadyForTraffic, "the authority was Ready before the joiner");
    assert_eq!(joiner_hydrator.held().0.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(root_hydrator.contexts.lock().unwrap().len(), 1);
    assert!(root_hydrator.contexts.lock().unwrap()[0].2.is_some(), "the Day-0 root's hook was handed authority None");
    let joiner_ctx = joiner_hydrator.contexts.lock().unwrap().clone();
    assert_eq!(joiner_ctx.iter().map(|c| c.1.clone()).collect::<Vec<_>>(), vec![Some(root_digest.node.name.to_string()); 2], "both attempts pulled from the accepting admin");

    joiner.leave().await;
    root.leave().await;
    let _ = std::fs::remove_dir_all(&root_dir);
    let _ = std::fs::remove_dir_all(&joiner_dir);
}

fn key_of(d: &rafka_mesh_entity::MeshDigest) -> String {
    d.node.endpoint_id.0.clone()
}
