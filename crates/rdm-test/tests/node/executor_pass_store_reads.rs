//! What the fabric-primary's executor pass reads from the fabric's nodes store.
//!
//! No executor pass of the fabric-primary reads the fabric's durable runtime rows. A runtime row is
//! written by the launching admin (build.md §5.3), whose join door installs the birth's digest in its
//! view; a lost member is recovered by its own mesh's primary as that primary's own workflow
//! (node-recovery.md "Execution"; Q342: a repair is never a Build or part of one). Only reads of every
//! runtime row are recorded (`NodesStorage::runtimes`).

use crate::offline_tick_store_reads::{day0_config, recorded, until_ready, RuntimeReads};
use rdm_admin::admin::start_on;
use rdm_admin::node_hooks::NodeHooks;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::NodeId;

// @feature: node-lifecycle
/// CONTRACT (build.md §5.3: "the launching admin a birth's runtime row"; node-recovery.md "Execution":
/// the lost node's mesh-primary recovers it as its own workflow, never a Build, Q342): a Day-0 node-admin
/// (mesh1.node-admin.1, the fabric-primary) is ready and its executor runs its passes for eight gossip
/// intervals, unstopped. From its ready on, no pass reads any runtime row, as it takes the fabric-primary
/// role or after. Must NOT happen: a read of every runtime row on an executor pass while this admin is
/// the fabric-primary (rust-distributed-mesh#100, boot.rs:3240 before the fix): one tail read of the
/// nodes store every 300 ms under its one read lock, racing its writers.
#[test]
fn no_executor_pass_of_the_fabric_primary_reads_the_runtime_rows() {
    let cell = "no_executor_pass_of_the_fabric_primary_reads_the_runtime_rows";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("executor-pass-store-reads-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let reads = RuntimeReads::default();

        let root = start_on(day0_config(&dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, recorded(&dir, &reads)).await.expect("the Day-0 admin comes up");
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        // Its start read the store once before it was ready (the contradictory-start check of a start with
        // no launcher, Q227): every read after this is an executor pass's.
        let at_ready = reads.all().len();
        // The executor's passes as the fabric-primary, the take's included: eight gossip intervals, one
        // pass at least every 300 ms.
        tokio::time::sleep(8 * rdm_core::cadence::gossip_interval()).await;

        let all = reads.all();
        // Outside the mesh-primary's offline tick (admin/recovery.rs).
        let by_a_pass: Vec<Vec<String>> = all[at_ready..].iter().filter(|s| !s.iter().any(|f| f.starts_with("admin/recovery.rs"))).cloned().collect();
        eprintln!("mesh1.node-admin.1 read the runtime rows {} times outside its offline tick: {by_a_pass:#?}", by_a_pass.len());
        assert_eq!(by_a_pass.len(), 0, "no executor pass of the fabric-primary reads the runtime rows: {by_a_pass:#?}");

        root.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}
