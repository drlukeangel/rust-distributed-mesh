//! What the fabric-primary's executor pass reads from the fabric's nodes store.
//!
//! The fabric-primary's drift check (`reconcile_drift`) reads every mesh's durable runtime rows: the
//! births at a path its view holds nothing at, and the exact runtime of an unheard birth whose digest it
//! holds none of. It reads them once, as it takes the fabric-primary role, and holds them while it keeps
//! it: a row written later is a birth the fabric-primary's own join door admitted (build.md §5.3: the
//! launching admin writes a birth's runtime row), whose digest that door installs in its view, or a
//! birth of a mesh with a running mesh-primary, which that mesh-primary recovers (node-recovery.md:
//! "The mesh-primary is the reconciler for its own members") and which reads its own mesh's rows as it
//! takes the role. Only reads of every runtime row are recorded (`NodesStorage::runtimes`).

use crate::offline_tick_store_reads::{day0_config, recorded, until_ready, RuntimeReads};
use rdm_admin::admin::start_on;
use rdm_admin::node_hooks::NodeHooks;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::NodeId;

// @feature: node-lifecycle
/// CONTRACT (build.md §5.3: "the launching admin a birth's runtime row"; node-recovery.md "Detection and
/// proof" step 3; fabric-primary.md:353: runtime loss after a Build completed is node recovery, which the
/// lost node's mesh-primary starts): a Day-0 node-admin (mesh1.admin.1, the fabric-primary) is ready and
/// its executor runs its passes for six gossip intervals, unstopped. Its pass read every runtime row once,
/// as it took the fabric-primary role, and no pass after that read any. Must NOT happen: a read of every
/// runtime row on every executor pass while this admin is the fabric-primary (rust-distributed-mesh#100,
/// boot.rs:3240 before the fix): one tail read of the nodes store every 300 ms under its one read lock,
/// racing its writers.
#[test]
fn the_fabric_primary_reads_the_runtime_rows_once_as_it_takes_the_role_and_no_executor_pass_reads_them_again() {
    let cell = "the_fabric_primary_reads_the_runtime_rows_once_as_it_takes_the_role_and_no_executor_pass_reads_them_again";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("executor-pass-store-reads-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let reads = RuntimeReads::default();

        let root = start_on(day0_config(&dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, recorded(&dir, &reads)).await.expect("the Day-0 admin comes up");
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        // The executor's first passes as the fabric-primary: the take.
        tokio::time::sleep(2 * rdm_core::cadence::gossip_interval()).await;
        let taken = reads.all().len();
        // Six gossip intervals of executor passes, one at least every 300 ms.
        tokio::time::sleep(6 * rdm_core::cadence::gossip_interval()).await;

        let all = reads.all();
        let after: Vec<Vec<String>> = all[taken..].iter().filter(|s| !s.iter().any(|f| f.starts_with("admin/recovery.rs"))).cloned().collect();
        let by_the_take: Vec<Vec<String>> = all.iter().filter(|s| s.iter().any(|f| f.starts_with("admin/reconcile.rs"))).cloned().collect();
        eprintln!("mesh1.admin.1 read the runtime rows {} times outside its offline tick after its take: {after:#?}", after.len());
        eprintln!("mesh1.admin.1's executor read them {} times as it took the fabric-primary role: {by_the_take:#?}", by_the_take.len());
        assert_eq!(after.len(), 0, "no executor pass of the fabric-primary reads the runtime rows after its take: {after:#?}");
        assert_eq!(by_the_take.len(), 1, "the fabric-primary reads every mesh's runtime rows once, as it takes the role: {by_the_take:#?}");

        root.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}
