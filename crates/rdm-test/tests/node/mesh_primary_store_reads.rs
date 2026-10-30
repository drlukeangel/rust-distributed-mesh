//! What a node-admin reads from and writes to the fabric's nodes store when it takes a mesh's writing,
//! and what an admin that is no primary writes there.
//!
//! - The mesh-primary is the one writer of its mesh's node records (build.md §5.3, the writer map). It
//!   reads them whole once, when it takes the mesh's writing, into the copy every later write starts
//!   from (storage-whole-records.md "The rule"). Its atlas holds the rows its writer holds
//!   (`WrittenRows::feeds`), and it reads them from the store no second time (Q207, Q227).
//! - A birth's runtime row is the launching admin's to write (build.md §5.3: "the launching admin a
//!   birth's runtime row ... no two writers write one key"). An admin that is no primary writes none,
//!   and reads none back at its start (Q227).

use crate::admin_reads_from_atlas::{day0_config, deployed, launch_by, until_ready};
use rdm_admin::admin::{start_on, AdminConfig};
use rdm_admin::node_hooks::{HookOutcome, NodeHooks};
use rdm_admin::storage::{NameHeld, NodeRow, NodesStorage, RuntimeRow};
use rdm_admin::storage_backend::{StorageBackend, StorageBoundaries, StorageScope, TopologyStorageProviders};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::{IncarnationId, NodeId};
use rdm_node::storage_error::StorageError;
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// One call an admin made on its nodes store: the call and what it named (a mesh, a node id).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Call {
    op: &'static str,
    subject: String,
    /// Where in the admin the call was made: its first frames in rdm-admin past the store's own,
    /// innermost first (`file:line`).
    sites: Vec<String>,
}

/// Every call one admin made on its nodes store, in order.
#[derive(Debug, Clone, Default)]
struct Calls(Arc<Mutex<Vec<Call>>>);

impl Calls {
    fn push(&self, op: &'static str, subject: impl Into<String>) {
        let trace = std::backtrace::Backtrace::force_capture().to_string();
        let sites = trace.lines().map(str::trim).filter(|l| l.starts_with("at ") && l.contains("crates/rdm-admin/src") && !l.contains("storage_backend.rs")).take(3).map(|l| l.rsplit("crates/rdm-admin/src/").next().unwrap_or(l).to_string()).collect();
        self.0.lock().unwrap().push(Call { op, subject: subject.into(), sites });
    }
    fn of(&self, op: &str) -> Vec<String> {
        self.0.lock().unwrap().iter().filter(|c| c.op == op).map(|c| c.subject.clone()).collect()
    }
    /// The calls `op` whose sites pass through any of `files`.
    fn through(&self, op: Option<&str>, files: &[&str]) -> Vec<Call> {
        self.0.lock().unwrap().iter().filter(|c| op.is_none_or(|o| c.op == o) && c.sites.iter().any(|s| files.iter().any(|f| s.starts_with(f)))).cloned().collect()
    }
    /// The calls `op` whose sites pass through none of `files`.
    fn not_through(&self, op: &str, files: &[&str]) -> Vec<Call> {
        self.0.lock().unwrap().iter().filter(|c| c.op == op && !c.sites.iter().any(|s| files.iter().any(|f| s.starts_with(f)))).cloned().collect()
    }
}

/// A nodes store that records every call made on it, then makes it on `inner`.
struct Recorded {
    inner: Arc<dyn NodesStorage>,
    calls: Calls,
}

#[async_trait::async_trait]
impl NodesStorage for Recorded {
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> {
        self.calls.push("put_runtime", row.node_id.to_string());
        self.inner.put_runtime(row).await
    }
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> {
        self.calls.push("runtimes", "");
        self.inner.runtimes().await
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> {
        self.calls.push("runtime", node_id.to_string());
        self.inner.runtime(node_id, incarnation).await
    }
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError> {
        self.calls.push("node", node_id.to_string());
        self.inner.node(node_id).await
    }
    async fn node_at(&self, name: &rdm_core::PathName) -> Result<Option<NodeRow>, StorageError> {
        self.calls.push("node_at", name.to_string());
        self.inner.node_at(name).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
        self.calls.push("nodes", mesh);
        self.inner.nodes(mesh).await
    }
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError> {
        self.calls.push("put_node", row.node_id.to_string());
        self.inner.put_node(row).await
    }
    async fn index_path(&self, name: &rdm_core::PathName, node_id: &NodeId) -> Result<(), StorageError> {
        self.calls.push("index_path", node_id.to_string());
        self.inner.index_path(name, node_id).await
    }
    async fn index_tags(&self, mesh: &str, node_id: &NodeId, leaving: &BTreeSet<String>, arriving: &BTreeSet<String>) -> Result<(), StorageError> {
        self.calls.push("index_tags", node_id.to_string());
        self.inner.index_tags(mesh, node_id, leaving, arriving).await
    }
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError> {
        self.calls.push("tagged", label);
        self.inner.tagged(label).await
    }
    async fn name_claim(&self, name: &rdm_core::PathName) -> Result<Option<NodeId>, StorageError> {
        self.calls.push("name_claim", name.to_string());
        self.inner.name_claim(name).await
    }
    async fn claim_node_name(&self, name: &rdm_core::PathName, node_id: &NodeId) -> Result<Result<(), NameHeld>, StorageError> {
        self.calls.push("claim_node_name", node_id.to_string());
        self.inner.claim_node_name(name, node_id).await
    }
    async fn release_node_name(&self, name: &rdm_core::PathName, node_id: &NodeId) -> Result<(), StorageError> {
        self.calls.push("release_node_name", node_id.to_string());
        self.inner.release_node_name(name, node_id).await
    }
}

/// RDM's file backend whose nodes store (its runtime rows on the store root included) records every
/// call one admin makes on it.
struct Recording {
    inner: Arc<dyn StorageBackend>,
    calls: Calls,
}

#[async_trait::async_trait]
impl StorageBackend for Recording {
    fn qualify(&self, provider: rdm_admin::model::ProviderKind) -> Result<(), String> {
        self.inner.qualify(provider)
    }
    async fn own_row(&self, data_dir: &Path, writer: &IncarnationId) -> Result<Option<rdm_admin::storage::NodeRecord>, StorageError> {
        self.inner.own_row(data_dir, writer).await
    }
    async fn put_own_row(&self, data_dir: &Path, writer: &IncarnationId, record: &rdm_admin::storage::NodeRecord) -> Result<(), StorageError> {
        self.inner.put_own_row(data_dir, writer, record).await
    }
    async fn open(&self, scope: &StorageScope) -> Result<StorageBoundaries, String> {
        let mut b = self.inner.open(scope).await?;
        b.nodes = Arc::new(Recorded { inner: b.nodes.clone(), calls: self.calls.clone() });
        b.root_runtimes = b.root_runtimes.take().map(|r| Arc::new(Recorded { inner: r, calls: self.calls.clone() }) as Arc<dyn NodesStorage>);
        Ok(b)
    }
    async fn delete_fabric(&self, data_dir: &Path, fabric_id: &rdm_admin::model::FabricId) -> Result<(), String> {
        self.inner.delete_fabric(data_dir, fabric_id).await
    }
}

/// The providers an admin of the cell's store dir `store` registers: RDM's file backend as `file`,
/// recording that admin's calls on its nodes store into `calls`.
fn recorded(store: &Path, calls: &Calls) -> TopologyStorageProviders {
    TopologyStorageProviders::new().register(rdm_core::launch::TOPOLOGY_STORAGE_FILE, Arc::new(Recording { inner: rdm_admin::storage_backend::file_backend(store.to_path_buf()), calls: calls.clone() }))
}

// @feature: node-lifecycle
/// CONTRACT (build.md §5.3 the writer map; storage-whole-records.md "The rule"; Q207, Q227): a Day-0
/// node-admin takes mesh1's writing (its mesh-primary, and the fabric-primary) and mesh1.admin.2,
/// launched by it, joins; both ready, and the fabric left to settle. The mesh-primary listed mesh1's
/// records from the store once: its writer's fill as it took the mesh, which its atlas holds too.
/// mesh1.admin.2, no primary, made no write to the nodes store and read no runtime row at its start;
/// its runtime row is its launcher's write, at its join. Must NOT happen: a second listing of the
/// mesh's records by its writer (the writer's copy filled twice, the atlas filled from the store, an
/// attempt's completion filling a mesh this admin writes); a runtime row written by an admin that did
/// not launch the birth; a read of every runtime row at an admin's start.
#[test]
fn a_mesh_primary_lists_its_meshs_records_once_and_an_admin_that_is_no_primary_writes_no_runtime_row() {
    let cell = "a_mesh_primary_lists_its_meshs_records_once_and_an_admin_that_is_no_primary_writes_no_runtime_row";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("primary-store-reads-{}", NodeId::mint()));
        let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
        std::fs::create_dir_all(&root_dir).unwrap();
        let (root_calls, admin_calls) = (Calls::default(), Calls::default());

        // The Day-0 admin does not reconcile: its Build names only itself, and mesh1.admin.2 is launched here.
        let root = start_on(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, recorded(&root_dir, &root_calls)).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;

        let launch = launch_by(&root, "mesh1.admin.2", &admin_dir, &root_dir);
        let _ = root.runner.joins.expect(deployed(&launch));
        let env = launch.to_env();
        let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let admin = start_on(admin_cfg, Wiring { node_hooks: NodeHooks::new(TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok))), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, recorded(&root_dir, &admin_calls)).await.expect("the launched admin comes up");
        until_ready("mesh1.admin.2", || admin.digest.lock().unwrap().clone()).await;
        // Every heard birth's view change has run: a write that follows a birth heard has been made.
        tokio::time::sleep(Duration::from_secs(3)).await;

        eprintln!("mesh1.admin.1 calls on the nodes store: {:#?}", root_calls.0.lock().unwrap());
        eprintln!("mesh1.admin.2 calls on the nodes store: {:#?}", admin_calls.0.lock().unwrap());
        let root_id = root.digest.lock().unwrap().node.node_id.to_string();
        // The take of mesh1's writing: the writer's fill (RoleFold's, `WrittenRows::fill`), the install's
        // arrivals, the atlas. The Day-0 admin's start reads before it are not the take: its contradictory-start
        // check (no launcher), and the primaries recorded on the rows (`take_recorded_primaries`, an election
        // input read only at a start with no launcher, Q227).
        let take = root_calls.through(Some("nodes"), &["node_rows.rs", "status_rpc.rs", "atlas.rs"]);
        assert_eq!(take.iter().filter(|c| c.subject == "mesh1").count(), 1, "the mesh-primary lists its mesh's records once as it takes the mesh's writing: {take:#?}");
        let atlas = root_calls.through(None, &["atlas.rs"]);
        assert!(atlas.is_empty(), "the mesh-primary's atlas reads nothing from the store for the mesh it writes: it holds its writer's rows: {atlas:#?}");
        // Every runtime row is written once, by the admin that launched its birth (the Day-0 admin its own).
        let mut runtime_writes = root_calls.of("put_runtime");
        runtime_writes.sort();
        let mut launched = vec![root_id.clone(), launch.node_id.to_string()];
        launched.sort();
        assert_eq!(runtime_writes, launched, "the launching admin writes each birth's runtime row once: its own (Day 0) and mesh1.admin.2's at its join: {:#?}", root_calls.0.lock().unwrap());
        assert_eq!(admin_calls.of("put_runtime"), Vec::<String>::new(), "mesh1.admin.2, no primary and no launcher, writes no runtime row");
        assert_eq!(admin_calls.of("put_node"), Vec::<String>::new(), "mesh1.admin.2, no primary, writes no node record");
        // The offline tick's read of every runtime row on every admin (`durable_rows`, recovery.rs) is
        // rust-distributed-mesh#99, not this cell's.
        let read_back = admin_calls.not_through("runtimes", &["admin/recovery.rs"]);
        assert!(read_back.is_empty(), "mesh1.admin.2 reads no runtime row back at its start: {read_back:#?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
