//! A node-admin told to stop reconciling starts no Build drive pass after it (`Running::stop_reconciling`:
//! "Stop executing and reconciling: no Build's drive pass, section run or drift recovery starts after
//! this"), the passes of a Build its drive was running included.
//!
//! The mesh's primary is a real Day-0 node-admin in this process; the node it creates through its
//! `node.create` runs in this process too ([`crate::in_process_provider`]). Its own process.

use crate::stopped_birth_signal::{created, day0_root, until_ready};
use rdm_core::NodeId;
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);

// @feature: node-lifecycle
/// CONTRACT (`Running::stop_reconciling`): once the Day-0 fabric is ready, a node is created through its
/// admin's `node.create` and the admin is told to stop reconciling, no drive pass of any Build begins (no
/// `via-reconcile`, no deployment step), the create's own Build included, and the node runs on.
/// Must NOT happen: a Build's drive, spawned before the call and not stopped by it, planning a further
/// pass from the view that holds the node (Rafka 141, #3193: 33 SIGTERMs of the test process through
/// `ProcessDeploymentProvider::terminate` in the Day-0 Build's drive).
#[test]
fn an_admin_that_stopped_reconciling_starts_no_build_drive() {
    if crate::own_process::delegated(module_path!(), "an_admin_that_stopped_reconciling_starts_no_build_drive") {
        return;
    }
    let cap = crate::common::capture("an_admin_that_stopped_reconciling_starts_no_build_drive");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("stop-reconciling-{}", NodeId::mint()));
        let (root_dir, rpc_dir) = (base.join("admin1"), base.join("rpc1"));
        std::fs::create_dir_all(&root_dir).unwrap();
        let (root, provider) = day0_root(&root_dir).await;
        tokio::time::timeout(WRONG, spans.recorded("rdm.node_admin.fabric.update.via-state-entered", |f| f.get("to").map(String::as_str) == Some("ready")))
            .await
            .expect("the Day-0 fabric reaches ready");
        let (_launch, rpc) = created(&root, &provider, rdm_core::PathName::new("mesh1.node", "1").unwrap(), &rpc_dir).await;
        until_ready("mesh1.node.1", || {
            let mut d = rpc.digest.clone();
            d.status = *rpc.status.lock().unwrap();
            d
        })
        .await;
        root.stop_reconciling().await;
        // Every span opened from here on is keyed above the last one opened before.
        let stopped_at = spans.0.lock().unwrap().keys().copied().max().unwrap_or(0);
        // Two staleness floors with the created node in the view: a drive still running plans its
        // next pass from it within one.
        tokio::time::sleep(rdm_core::cadence::staleness_floor() * 2).await;

        let after: Vec<(String, std::collections::BTreeMap<String, String>)> = spans
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, (n, _, _))| **k > stopped_at && (n == "rdm.node_admin.build.update.via-reconcile" || n == "rdm.node_admin.deployment.update.via-step"))
            .map(|(_, (n, _, f))| (n.clone(), f.clone()))
            .collect();
        assert!(after.is_empty(), "a Build drive pass began after the admin stopped reconciling: {after:#?}");
        assert_eq!(*rpc.status.lock().unwrap(), rdm_core::model::NodeStatus::Ready, "the created node runs on");

        rpc.stop().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
