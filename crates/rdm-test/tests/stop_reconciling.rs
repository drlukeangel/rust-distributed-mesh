//! A node-admin told to stop reconciling starts no Build attempt after it (`Running::stop_reconciling`:
//! "Stop executing and reconciling: no Build attempt and no drift recovery starts after this"), the
//! attempts of a Build its drive was running included.
//!
//! The mesh's primary is a real Day-0 node-admin in this process; the node this cell launches by hand
//! runs in its own process too (its runtime record names it), so a Build that took it for surplus would
//! end this process. Its own process.

use crate::stopped_birth_signal::{day0_config, deployed, launch_by, until_ready};
use rdm_admin::admin::start_with;
use rdm_admin::node_hooks::{HookOutcome, NodeHooks};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::NodeId;
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);

// @feature: node-lifecycle
/// CONTRACT (`Running::stop_reconciling`): once the Day-0 fabric is ready-for-traffic and its admin is
/// told to stop reconciling, no attempt of any Build begins (no `via-reconcile`, no deployment step), so a
/// node launched by hand afterwards is never taken for surplus and runs on.
/// Must NOT happen: the Day-0 Build's drive, spawned before the call and not stopped by it, planning a
/// further attempt from a view that holds the hand-launched node and deleting it as surplus (Rafka 141,
/// #3193: 33 SIGTERMs of the test process through `ProcessDeploymentProvider::terminate` in the Day-0
/// Build's drive).
#[test]
fn an_admin_that_stopped_reconciling_starts_no_build_attempt() {
    if crate::own_process::delegated(module_path!(), "an_admin_that_stopped_reconciling_starts_no_build_attempt") {
        return;
    }
    let cap = crate::common::capture("an_admin_that_stopped_reconciling_starts_no_build_attempt");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("stop-reconciling-{}", NodeId::mint()));
        let (root_dir, rpc_dir) = (base.join("admin1"), base.join("rpc1"));
        std::fs::create_dir_all(&root_dir).unwrap();
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        tokio::time::timeout(WRONG, spans.recorded("rdm.node_admin.fabric.update.via-state-entered", |f| f.get("to").map(String::as_str) == Some("ready-for-traffic")))
            .await
            .expect("the Day-0 fabric reaches ready-for-traffic");
        root.stop_reconciling().await;
        // Every span opened from here on is keyed above the last one opened before.
        let stopped_at = spans.0.lock().unwrap().keys().copied().max().unwrap_or(0);

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
        // Two staleness floors with the hand-launched node in the view: a drive still running plans its
        // next attempt from it within one.
        tokio::time::sleep(rdm_core::cadence::staleness_floor() * 2).await;

        let after: Vec<(String, std::collections::BTreeMap<String, String>)> = spans
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, (n, _, _))| **k > stopped_at && (n == "rdm.node_admin.build.update.via-reconcile" || n == "rdm.node_admin.deployment.update.via-step"))
            .map(|(_, (n, _, f))| (n.clone(), f.clone()))
            .collect();
        assert!(after.is_empty(), "a Build attempt began after the admin stopped reconciling: {after:#?}");
        assert_eq!(*rpc.status.lock().unwrap(), rdm_core::MemberStatus::ReadyForTraffic, "the hand-launched node runs on");

        rpc.stop().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
