//! What a node-admin's offline tick reads from the fabric's nodes store.
//!
//! Only the mesh-primary runs node investigation on its mesh's births (fabric-node-lifecycle-elections.md
//! §4.1; node-recovery.md "Detection and the verdict" step 2: "Only the mesh-primary acts"), so only it
//! watches the births a durable runtime row alone names. It reads its mesh's runtime rows once, as it
//! takes the role, and holds them while it keeps it: a row written later names a birth its own executor
//! launched (build.md §5.3: the launching admin writes a birth's runtime row; node-recovery.md: every
//! attempt in a mesh with a running mesh-primary is that mesh-primary's to run), which its view holds.
//! An admin that is not its mesh's primary reads no runtime row on any tick.

use rdm_admin::admin::{start_on, AdminConfig, Running};
use rdm_admin::join::Deployed;
use rdm_admin::node_hooks::{HookOutcome, NodeHooks};
use rdm_admin::storage::{NameHeld, NodeRow, NodesStorage, RuntimeRow};
use rdm_admin::storage_backend::{StorageBackend, StorageBoundaries, StorageScope, TopologyStorageProviders};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, NodeId};
use rdm_node::storage_error::StorageError;
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
pub(crate) const WRONG: Duration = Duration::from_secs(60);

/// The call sites of every read of every runtime row one admin made on its nodes store: per read,
/// its first frames in rdm-admin past the store's own (`file:line`).
#[derive(Debug, Clone, Default)]
pub(crate) struct RuntimeReads(Arc<Mutex<Vec<Vec<String>>>>);

impl RuntimeReads {
    fn push(&self) {
        let trace = std::backtrace::Backtrace::force_capture().to_string();
        let sites = trace.lines().map(str::trim).filter(|l| l.starts_with("at ") && l.contains("crates/rdm-admin/src") && !l.contains("storage_backend.rs")).take(3).map(|l| l.rsplit("crates/rdm-admin/src/").next().unwrap_or(l).to_string()).collect();
        self.0.lock().unwrap().push(sites);
    }
    /// The reads made by the offline tick (`admin/recovery.rs`, `TenureRows`).
    pub(crate) fn by_the_tick(&self) -> Vec<Vec<String>> {
        self.0.lock().unwrap().iter().filter(|s| s.iter().any(|f| f.starts_with("admin/recovery.rs"))).cloned().collect()
    }
    /// Every read recorded so far, in order.
    pub(crate) fn all(&self) -> Vec<Vec<String>> {
        self.0.lock().unwrap().clone()
    }
}

/// A nodes store that records every read of every runtime row made on it, then makes each call on `inner`.
struct Recorded {
    inner: Arc<dyn NodesStorage>,
    reads: RuntimeReads,
}

#[async_trait::async_trait]
impl NodesStorage for Recorded {
    async fn put_runtime(&self, row: &RuntimeRow) -> Result<(), StorageError> {
        self.inner.put_runtime(row).await
    }
    async fn runtimes(&self) -> Result<Vec<RuntimeRow>, StorageError> {
        self.reads.push();
        self.inner.runtimes().await
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<RuntimeRow>, StorageError> {
        self.inner.runtime(node_id, incarnation).await
    }
    async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, StorageError> {
        self.inner.node(node_id).await
    }
    async fn node_at(&self, name: &rdm_core::PathName) -> Result<Option<NodeRow>, StorageError> {
        self.inner.node_at(name).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, StorageError> {
        self.inner.nodes(mesh).await
    }
    async fn put_node(&self, row: &NodeRow) -> Result<(), StorageError> {
        self.inner.put_node(row).await
    }
    async fn index_path(&self, name: &rdm_core::PathName, node_id: &NodeId) -> Result<(), StorageError> {
        self.inner.index_path(name, node_id).await
    }
    async fn index_tags(&self, mesh: &str, node_id: &NodeId, leaving: &BTreeSet<String>, arriving: &BTreeSet<String>) -> Result<(), StorageError> {
        self.inner.index_tags(mesh, node_id, leaving, arriving).await
    }
    async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, StorageError> {
        self.inner.tagged(label).await
    }
    async fn name_claim(&self, name: &rdm_core::PathName) -> Result<Option<NodeId>, StorageError> {
        self.inner.name_claim(name).await
    }
    async fn claim_node_name(&self, name: &rdm_core::PathName, node_id: &NodeId) -> Result<Result<(), NameHeld>, StorageError> {
        self.inner.claim_node_name(name, node_id).await
    }
    async fn release_node_name(&self, name: &rdm_core::PathName, node_id: &NodeId) -> Result<(), StorageError> {
        self.inner.release_node_name(name, node_id).await
    }
}

/// RDM's file backend whose nodes store (its runtime rows on the store root included) records every
/// read of every runtime row one admin makes on it.
struct Recording {
    inner: Arc<dyn StorageBackend>,
    reads: RuntimeReads,
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
        b.nodes = Arc::new(Recorded { inner: b.nodes.clone(), reads: self.reads.clone() });
        b.root_runtimes = b.root_runtimes.take().map(|r| Arc::new(Recorded { inner: r, reads: self.reads.clone() }) as Arc<dyn NodesStorage>);
        Ok(b)
    }
    async fn delete_fabric(&self, data_dir: &Path, fabric_id: &rdm_admin::model::FabricId) -> Result<(), String> {
        self.inner.delete_fabric(data_dir, fabric_id).await
    }
}

/// The providers an admin of the cell's store dir `store` registers: RDM's file backend as `file`,
/// recording that admin's reads of the runtime rows into `reads`.
pub(crate) fn recorded(store: &Path, reads: &RuntimeReads) -> TopologyStorageProviders {
    TopologyStorageProviders::new().register(rdm_core::launch::TOPOLOGY_STORAGE_FILE, Arc::new(Recording { inner: rdm_admin::storage_backend::file_backend(store.to_path_buf()), reads: reads.clone() }))
}

pub(crate) fn day0_config(dir: &Path) -> AdminConfig {
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

/// A launch of `name` by `root`, its data dir holding the runtime record its provider would write.
fn launch_by(root: &Running, name: &str, dir: &Path, store: &Path) -> Launch {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
    Launch {
        topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.to_path_buf(),
        store_dir: store.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    }
}

/// What `root` verifies the first join of `launch`'s birth against.
fn deployed(launch: &Launch) -> Deployed {
    let key = rdm_test::node::load_or_mint_key(&launch.data_dir).unwrap();
    Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: rdm_core::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).unwrap(),
        data_dir: launch.data_dir.display().to_string(),
        last_accepted_name: None,
        held: false,
        listeners: Vec::new(),
    }
}

pub(crate) async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != MemberStatus::Ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published Ready"));
}

// @feature: node-lifecycle
/// CONTRACT (fabric-node-lifecycle-elections.md §4.1: "The mesh-primary then runs node investigation";
/// node-recovery.md "Detection and the verdict" step 2: "Only the mesh-primary acts"; build.md §5.3):
/// a Day-0 node-admin (mesh1.admin.1, its mesh's primary) and mesh1.admin.2, launched by it, are both
/// ready and run their offline ticks for six gossip intervals. The mesh-primary's tick read the runtime
/// rows once, as it took the role; mesh1.admin.2's tick, no primary, read none. Must NOT happen: a read
/// of every runtime row on every tick of every admin (rust-distributed-mesh#99: 12 reads in 3 s on the
/// mesh-primary, 9 on an admin that is no primary), each a tail read of the nodes store under its one
/// read lock racing its writer, and on an admin that is no primary the fold of the whole store its
/// start never needed (Q227).
#[test]
fn the_offline_tick_reads_the_runtime_rows_once_on_the_mesh_primary_and_never_on_an_admin_that_is_no_primary() {
    let cell = "the_offline_tick_reads_the_runtime_rows_once_on_the_mesh_primary_and_never_on_an_admin_that_is_no_primary";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("offline-tick-store-reads-{}", NodeId::mint()));
        let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
        std::fs::create_dir_all(&root_dir).unwrap();
        let (root_reads, admin_reads) = (RuntimeReads::default(), RuntimeReads::default());

        // The Day-0 admin does not reconcile: its Build names only itself, and mesh1.admin.2 is launched here.
        let root = start_on(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, recorded(&root_dir, &root_reads)).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;

        let launch = launch_by(&root, "mesh1.admin.2", &admin_dir, &root_dir);
        let _ = root.runner.joins.expect(deployed(&launch));
        let env = launch.to_env();
        let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let admin = start_on(admin_cfg, Wiring { node_hooks: NodeHooks::new(TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok))), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, recorded(&root_dir, &admin_reads)).await.expect("the launched admin comes up");
        until_ready("mesh1.admin.2", || admin.digest.lock().unwrap().clone()).await;
        // Six ticks of each admin's offline loop at the cell's gossip interval.
        tokio::time::sleep(6 * rdm_core::cadence::gossip_interval()).await;

        let (on_root, on_admin) = (root_reads.by_the_tick(), admin_reads.by_the_tick());
        eprintln!("mesh1.admin.1's tick read the runtime rows {} times: {on_root:#?}", on_root.len());
        eprintln!("mesh1.admin.2's tick read the runtime rows {} times: {on_admin:#?}", on_admin.len());
        assert_eq!(on_admin.len(), 0, "mesh1.admin.2, no primary, reads no runtime row on any tick: {on_admin:#?}");
        assert_eq!(on_root.len(), 1, "the mesh-primary reads its mesh's runtime rows once, as it takes the role: {on_root:#?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
