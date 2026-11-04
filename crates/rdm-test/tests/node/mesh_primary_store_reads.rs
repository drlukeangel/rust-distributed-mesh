//! What node-admins write to and read from the fabric's nodes store at their starts.
//!
//! - Each node writes every field of its own node record itself, a node-admin's included (Q356,
//!   build.md §5.3): its first line at its start, before Ready. Its mesh-primary writes a node's record
//!   only for a node that is lost, killed or deleted, never a live node's.
//! - A node reads nothing from the nodes store at its start, join or create (Q227); the launch and
//!   the join's answer carry what it writes.

use crate::admin_reads_from_atlas::{day0_config, deployed, launch_by, until_ready};
use rdm_admin::admin::{start_on, AdminConfig};
use rdm_admin::node_hooks::{HookOutcome, NodeHooks};
use rdm_admin::storage::{NodeRecord, NodesStorage};
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
    subject: serde_json::Value,
    /// Where in the admin the call was made: its first frames in rdm-admin past the store's own,
    /// innermost first (`file:line`).
    sites: Vec<String>,
}

/// Every call one admin made on its nodes store, in order.
#[derive(Debug, Clone, Default)]
struct Calls(Arc<Mutex<Vec<Call>>>);

impl Calls {
    fn push(&self, op: &'static str, subject: impl serde::Serialize) {
        let trace = std::backtrace::Backtrace::force_capture().to_string();
        let sites = trace.lines().map(str::trim).filter(|l| l.starts_with("at ") && l.contains("crates/rdm-admin/src") && !l.contains("storage_backend.rs")).take(3).map(|l| l.rsplit("crates/rdm-admin/src/").next().unwrap_or(l).to_string()).collect();
        self.0.lock().unwrap().push(Call { op, subject: serde_json::to_value(subject).expect("structured store subject"), sites });
    }
    fn of(&self, op: &str) -> Vec<String> {
        self.0.lock().unwrap().iter().filter(|c| c.op == op).map(|c| c.subject.as_str().expect("this operation selects a scalar ID").to_owned()).collect()
    }
}

/// A nodes store that records every call made on it, then makes it on `inner`.
struct Recorded {
    inner: Arc<dyn NodesStorage>,
    calls: Calls,
}

#[async_trait::async_trait]
impl NodesStorage for Recorded {
    async fn node(&self, node_id: &NodeId) -> Result<Option<rdm_admin::storage::NodeLine>, StorageError> {
        self.calls.push("node", node_id.to_string());
        self.inner.node(node_id).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRecord>, StorageError> {
        self.calls.push("nodes", mesh);
        self.inner.nodes(mesh).await
    }
    async fn put_node(&self, row: &NodeRecord) -> Result<(), StorageError> {
        self.calls.push("put_node", row.node_id.to_string());
        self.inner.put_node(row).await
    }
    async fn records_at(&self, name: &rdm_core::PathName) -> Result<Vec<rdm_admin::storage::NodeRecord>, StorageError> {
        self.calls.push("node_at", name);
        self.inner.records_at(name).await
    }
}

/// RDM's file backend whose nodes store records every call one admin makes on it.
struct Recording {
    inner: Arc<dyn StorageBackend>,
    calls: Calls,
}

#[async_trait::async_trait]
impl StorageBackend for Recording {
    fn qualify(&self, provider: rdm_admin::model::ProviderKind) -> Result<(), String> {
        self.inner.qualify(provider)
    }
    async fn own_row(&self, data_dir: &Path, writer: &IncarnationId) -> Result<Option<rdm_admin::storage::OwnRow>, StorageError> {
        self.inner.own_row(data_dir, writer).await
    }
    async fn put_own_row(&self, data_dir: &Path, writer: &IncarnationId, record: &rdm_admin::storage::OwnRow) -> Result<(), StorageError> {
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
/// CONTRACT (node-identity-and-state.md §2.5: "A node writes every field of its own record itself
/// (Q356). Its mesh-primary, or the fabric-primary for a mesh with no mesh-primary, writes a node's
/// record only when the node cannot: a node that is lost, killed or deleted"; Q227): a Day-0 node-admin
/// (mesh1's primary, and the fabric-primary) and mesh1.node-admin.2, launched by it, which joins; both
/// ready, and the fabric left to settle. Each admin wrote only its own record: mesh1.node-admin.2 its
/// first line before it was ready, and the mesh-primary no line of mesh1.node-admin.2, a live node.
/// mesh1.node-admin.2 read nothing from the nodes store before it was ready. Must NOT happen: a node's
/// record written by another node while it lives; a read of the store at a node's start.
#[test]
fn each_node_admin_writes_only_its_own_record_and_reads_nothing_at_its_start() {
    let cell = "each_node_admin_writes_only_its_own_record_and_reads_nothing_at_its_start";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("primary-store-reads-{}", NodeId::mint()));
        let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
        std::fs::create_dir_all(&root_dir).unwrap();
        let (root_calls, admin_calls) = (Calls::default(), Calls::default());

        // The Day-0 admin does not reconcile: its Build names only itself, and mesh1.node-admin.2 is launched here.
        let root = start_on(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, recorded(&root_dir, &root_calls)).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;

        let launch = launch_by(&root, rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), &admin_dir, &root_dir);
        let _ = root.runner.joins.expect(deployed(&launch));
        let env = launch.to_env();
        let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let admin = start_on(admin_cfg, Wiring { node_hooks: NodeHooks::new(TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok))), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, recorded(&root_dir, &admin_calls)).await.expect("the launched admin comes up");
        until_ready("mesh1.node-admin.2", || admin.digest.lock().unwrap().clone()).await;
        let at_ready: Vec<Call> = admin_calls.0.lock().unwrap().clone();
        // Every heard birth's view change has run: a write that follows a birth heard has been made.
        tokio::time::sleep(Duration::from_secs(3)).await;

        eprintln!("mesh1.node-admin.1 calls on the nodes store: {:#?}", root_calls.0.lock().unwrap());
        eprintln!("mesh1.node-admin.2 calls on the nodes store: {:#?}", admin_calls.0.lock().unwrap());
        let root_id = root.digest.lock().unwrap().node.node_id.to_string();
        let admin_id = launch.node_id.to_string();
        // Each admin writes its own record and no other's: mesh1.node-admin.2 is live, so its
        // mesh-primary writes none of its lines.
        let root_writes = root_calls.of("put_node");
        assert!(root_writes.contains(&root_id) && root_writes.iter().all(|id| *id == root_id), "the mesh-primary writes its own record and no live node's: {:#?}", root_calls.0.lock().unwrap());
        let admin_writes = admin_calls.of("put_node");
        assert!(admin_writes.contains(&admin_id) && admin_writes.iter().all(|id| *id == admin_id), "mesh1.node-admin.2 writes its own record and no other's: {:#?}", admin_calls.0.lock().unwrap());
        // Its start: its own record's first line before Ready, and no read of the store (Q227).
        let start_writes: Vec<&Call> = at_ready.iter().filter(|c| c.op == "put_node").collect();
        assert!(!start_writes.is_empty(), "mesh1.node-admin.2 writes its record's first line before it is ready: {at_ready:#?}");
        let start_reads: Vec<&Call> = at_ready.iter().filter(|c| c.op != "put_node").collect();
        assert!(start_reads.is_empty(), "mesh1.node-admin.2 reads nothing from the nodes store at its start: {start_reads:#?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
