//! `pre_node_ready` on node-admins (i143.e12.s20): a node-admin runs the same hook as every
//! node. The Day-0 root has no mesh-primary to pull from, so its context says why and its hook
//! completes from local state.

use rdm_core::model::NodeStatus;
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::node_hooks::{HookOutcome, NodeHooks, PreNodeReadyState};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use std::sync::Arc;
use std::time::Duration;

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

/// Resolves once `running`'s admin published Ready (the Ready check runs on its own
/// cadence; this reads the digest it publishes).
async fn until_ready(running: &rdm_admin::admin::Running) {
    let waited = async {
        loop {
            if running.digest.lock().unwrap().status == NodeStatus::Ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.expect("the admin never published Ready");
}

/// CONTRACT: the Day-0 root's `pre_node_ready` runs once, is handed `authority: None` with the
/// reason (no authority exists before this admin), completes from local state, and only then does
/// the admin publish Ready: the attempt span records `ok` and the Ready check named the
/// running hook as a blocker until then. What must NOT happen: the root's hook pulling from an
/// authority that does not exist, or the root Ready before its hook returned.
#[test]
fn the_day_zero_root_runs_the_hook_with_no_mesh_primary_and_completes_from_local_state() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "the_day_zero_root_runs_the_hook_with_no_mesh_primary_and_completes_from_local_state") {
        return;
    }
    let cap = crate::common::capture("the_day_zero_root_runs_the_hook_with_no_mesh_primary_and_completes_from_local_state");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let dir = std::env::temp_dir().join(format!("pre-node-ready-day0-{}", rdm_core::NodeId::mint()));
        let pre_node_ready = TestPreNodeReady::new(Mode::Local);
        let wiring = Wiring { node_hooks: NodeHooks::new(pre_node_ready.clone()), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() };
        let running = start_with(day0_config(&dir), wiring).await.expect("the Day-0 admin comes up");
        let handle = running.pre_node_ready().expect("a registered hook has a handle");
        assert_eq!(tokio::time::timeout(WRONG, handle.settled()).await.expect("the hook returns"), PreNodeReadyState::Passed);
        until_ready(&running).await;

        let contexts = pre_node_ready.contexts.lock().unwrap().clone();
        assert_eq!(contexts.len(), 1, "the hook ran once: {contexts:?}");
        assert_eq!((contexts[0].0, contexts[0].1.clone()), (1, None), "the Day-0 root has no accepting authority");
        assert!(contexts[0].2.as_deref().is_some_and(|why| why.contains("Day-0")), "the context says why: {contexts:?}");
        let attempts: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.mesh.node.update.via-pre-node-ready-attempt").map(|(_, _, f)| f.clone()).collect();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0]["outcome"], "ok");
        assert!(attempts[0]["accepting_primary"].contains("none"), "{:?}", attempts[0]);
        let _ = HookOutcome::Ok;
        let _ = Arc::strong_count(&pre_node_ready);
        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}
