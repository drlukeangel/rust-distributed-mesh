//! `fabric.delete` replaces `FabricShutdown` (i143.e12.s59; lifecycles/fabric-delete.md,
//! lifecycles/mesh-investigation.md "During `fabric.delete`", R-FS1 revised).
//!
//! 1. `crit:e12.s59.1`: `fabric.delete` of a two-mesh fabric runs `mesh.delete` on each mesh
//!    leaf-inward (the fabric-primary's own mesh last, where the fabric seat stays put), deletes
//!    each other mesh's final mesh-primary (`MeshLeft` + mesh row `Dead`), releases the fabric name
//!    (`FabricStorage::release_name`) and reports `fabric.deleted` (`Complete` of
//!    `fabric.delete:<fabric_id>`) while the fabric-primary remains alive for its separate
//!    `node.delete`. After that separate `node.delete`, every node row is `Deleted`, every mesh row
//!    is `Dead` in both `status.storage` and `mesh.storage`, and no fabric identity row remains held
//!    in `fabric.storage`.
//! 2. `crit:e12.s59.2`: an unanswered mesh during `fabric.delete` (R-FS1 revised):
//!    - Held (unproven runtime) is refused by name (`Failed`), writing no `Deleted` or `Dead` row
//!      and creating no `node-admin`.
//!    - Every node of the mesh proven exited by the provider (fully crashed) commands nothing and
//!      creates no `node-admin`: every birth's node row is written `Deleted` on that proof, the mesh
//!      status row and `mesh.storage` row are written `Dead` by the fabric-primary as acting primary
//!      with no survivor (`R-MD1`), and `MeshDeleted` and `MeshLeft` are recorded.
//!    - Ordinary members live while all `node-admin`s are proven exited (`carrier-edge-lost`)
//!      records `AcceptedRoster` and the exited admin's `TerminateRuntime` receipt, selects the next
//!      free `node-admin` path (`leaving_mesh_admin_path`), and once that one `node-admin` is
//!      created into the mesh under its existing `MeshId` on the same Build, amends the roster
//!      (`RosterAmended`), adopts held state (`roster_rows_read`), runs `mesh.delete` (`members_exited`),
//!      and is removed last by the fabric-primary's `node.delete`.

use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::node_rows::RecordAt as _;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId};
use rdm_admin::accepted::{compile, plan_for_build, AcceptedStore, FabricTopology, TopologyChange};
use rdm_admin::admin::Records;
use rdm_admin::build::{BuildId, BuildOperation};
use rdm_admin::build_state::{AttemptOutcome, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::control_plane::ControlPlane;
use rdm_admin::deployment::pipeline::{DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, RuntimeProof, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::executor::{executor_for, seat_move};
use rdm_admin::fabric_storage::{FabricIdentity, FabricStorage, MemoryFabricStorage};
use rdm_admin::join::Joins;
use rdm_admin::mesh_delete::{accepted_roster, amend_roster, complete_fabric_delete, handoff_verdict, leaving_roster, manifest_reference, members_exited, mesh_left_proof, mesh_left_rows, operation, owner_resume, record_completion, roster_records, roster_rows_read, unanswered_mesh_delete, HandoffVerdict, MeshDeleteKey, OwnerResume, RosterAmended, RosterBirth, UnansweredMeshOutcome, STEP_ACCEPTED_ROSTER, STEP_LEAVING_ROSTER, STEP_MESH_LEFT};
use rdm_admin::model::{DeploymentId, Fabric, Mesh, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rdm_admin::node_rows::{ClaimFor, Claimed, NodeRows};
use rdm_admin::status_rpc::Declared;
use rdm_admin::storage::{MemoryMeshStorage, MemoryNodesStorage, MeshStorage, NodeRow, NodesStorage, RuntimeRow};
use rdm_admin::topology::Topology;
use rdm_core::status::MeshState;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

const DOMAIN: &str = "fabric-delete-cell";

#[derive(Default)]
struct Fleet {
    pids: BTreeMap<u32, String>,
    running_pids: Mutex<BTreeSet<u32>>,
    terminated: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for Fleet {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, _: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        unreachable!("a mesh or fabric delete launches nothing here")
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        let pid = h.pid.unwrap();
        self.running_pids.lock().unwrap().remove(&pid);
        self.terminated.lock().unwrap().push(self.pids[&pid].clone());
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        let pid = h.pid.unwrap();
        if self.running_pids.lock().unwrap().contains(&pid) {
            DeploymentStatus::Running
        } else {
            DeploymentStatus::Exited { code: Some(0) }
        }
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

struct NoRpc;
#[async_trait::async_trait]
impl NodeObserver for NoRpc {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Default)]
struct View(Mutex<BTreeMap<String, Node>>, Mutex<BTreeSet<String>>, Mutex<Option<Arc<NodeRows>>>);
impl TopologySink for View {
    fn publish(&self, n: Node) {
        self.0.lock().unwrap().insert(n.name.to_string(), n);
    }
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, name: &PathName, node_id: &NodeId, incarnation: &IncarnationId) {
        let mut view = self.0.lock().unwrap();
        if view.get(&name.to_string()).is_some_and(|n| n.node_id == *node_id && n.incarnation_id.as_ref() == Some(incarnation)) {
            view.remove(&name.to_string());
        }
        self.1.lock().unwrap().insert(name.to_string());
    }
    fn node_rows(&self) -> Option<Arc<NodeRows>> {
        self.2.lock().unwrap().clone()
    }
}

impl View {
    /// The writer node.delete's `NodeDeleted` step writes a member's row `Deleted` through
    /// (node-delete.md step 3): the executing admin's.
    fn writes_through(&self, rows: Arc<NodeRows>) {
        *self.2.lock().unwrap() = Some(rows);
    }
}

fn node_record(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn handle(name: &str, pid: u32) -> DeploymentHandle {
    DeploymentHandle {
        deployment_id: DeploymentId::parse(&rdm_core::fixture_crockford60(&format!("dep-{name}"))).unwrap(),
        provider: ProviderKind::Process,
        pid: Some(pid),
        start: Some(1),
        container: None,
        domain: Some(DOMAIN.into()),
    }
}

fn live_roster(records: &[Node]) -> Result<Vec<RosterBirth>, String> {
    records.iter().filter(|n| n.status.is_live()).map(RosterBirth::of).collect()
}

async fn rows_written(storage: &MemoryNodesStorage, rows: &NodeRows, records: &[Node]) {
    for n in records {
        // The record a create wrote: the node's row and its birth.
        let mut row = NodeRow { topology_storage_provider: None, writer: None,
                name: n.name.clone(),
                node_id: n.node_id.clone(),
                status: NodeStatus::Ready,
                config: BTreeMap::from([("role".to_string(), n.name.to_string())]),
                tags: Default::default(),
                allocated: None,
                renamed_from: None,
                app_types: None, inherited_by: None,
                executable: None,
                births: Vec::new(),
                predecessor: None,
                mesh_id: None,
                storage_persistence: None,
                placement: None, revision: 0,
            };
        if let Some(incarnation) = &n.incarnation_id {
            row.birth_mut(incarnation, &n.name);
        }
        storage.write_record(&row).await.unwrap();
        assert!(matches!(rows.claim(&n.name, &n.node_id, "create-node", None, None, ClaimFor::Create).await, Ok(Claimed::HeldByNode(_))), "the writer holds {}'s row", n.name);
        let h = handle(&n.name.to_string(), 50_000);
        storage
            .put_runtime(&RuntimeRow {
                node_id: n.node_id.clone(),
                name: n.name.clone(),
                incarnation_id: n.incarnation_id.clone().unwrap(),
                runtime: h.fact().unwrap(),
                data_dir: None,
            })
            .await
            .unwrap();
    }
}

/// crit:e12.s59.1 — `fabric.delete` of a two-mesh fabric runs `mesh.delete` leaf-inward (`mesh2`
/// first, then the fabric-primary's own mesh `mesh1` last with the fabric seat kept on
/// `mesh1.admin.1`), deletes `mesh2`'s final mesh-primary (`mesh2.admin.1`), releases the fabric
/// name (`FabricStorage::release_name`), and reports `fabric.deleted` (`Complete` of
/// `fabric.delete:<fabric_id>`) while `mesh1.admin.1` remains alive for its separate `node.delete`.
/// Once that separate `node.delete` runs, no live node, mesh or fabric row remains.
#[tokio::test]
async fn fabric_delete_leaves_no_node_mesh_or_fabric_row_and_fabric_primary_awaits_its_own_node_delete() {
    let data_root = std::env::temp_dir().join(format!("fabric-delete-{}", NodeId::mint()));
    let fabric_id = FabricId::mint();
    let (id1, id2) = (MeshId::mint(), MeshId::mint());
    let m1_a1 = node_record("mesh1.admin.1", true, true);
    let m1_r1 = node_record("mesh1.rpc.1", true, false);
    let m2_a1 = node_record("mesh2.admin.1", true, false);
    let m2_r1 = node_record("mesh2.rpc.1", true, false);
    let all_nodes = vec![m1_a1.clone(), m1_r1.clone(), m2_a1.clone(), m2_r1.clone()];
    let handles: BTreeMap<String, DeploymentHandle> = all_nodes.iter().enumerate().map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 51_000 + i as u32))).collect();
    let fleet = Fleet { pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(), ..Default::default() };

    let observed = Arc::new(RwLock::new(Topology {
        fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![
            Mesh { id: Some(id1.clone()), name: "mesh1".into(), status: ScopeStatus::Ready },
            Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Ready },
        ],
        nodes: all_nodes.clone(),
    }));
    let initial = FabricTopology::of_observed(&*observed.read().await);

    // Durable stores for fabric, meshes, statuses, and each mesh's node rows.
    let fabric_storage = Arc::new(MemoryFabricStorage::new());
    let identity = FabricIdentity { fabric_id: fabric_id.clone(), name: "fabric1".into() };
    rdm_admin::fabric_storage::claim_identity(&*fabric_storage, &identity).await.unwrap().unwrap();
    let mesh_storage = MemoryMeshStorage::default();
    for (id, name) in [(&id1, "mesh1"), (&id2, "mesh2")] {
        rdm_admin::storage::claim_mesh(&mesh_storage, None, &rdm_admin::storage::MeshRecord { mesh_id: id.clone(), name: name.into(), dead: false, states: Default::default() }).await.unwrap().unwrap();
    }
    let declared = Mutex::new(Declared::default());
    let nodes_storage = Arc::new(MemoryNodesStorage::default());
    let rows_m1 = Arc::new(NodeRows::new(m1_a1.name.clone(), m1_a1.node_id.clone(), observed.clone(), nodes_storage.clone()));
    let rows_m2 = Arc::new(NodeRows::new(m2_a1.name.clone(), m2_a1.node_id.clone(), observed.clone(), nodes_storage.clone()));
    rows_written(&nodes_storage, &rows_m1, &[m1_a1.clone(), m1_r1.clone()]).await;
    rows_written(&nodes_storage, &rows_m2, &[m2_a1.clone(), m2_r1.clone()]).await;

    // Compile TopologyChange::DeleteFabric and accept the Build.
    let deleted_topology = compile(&initial, &TopologyChange::DeleteFabric, &*observed.read().await).expect("TopologyChange::DeleteFabric compiles to an empty/deleted topology");
    assert!(deleted_topology.meshes.is_empty(), "a deleted fabric topology names no meshes: {deleted_topology:?}");

    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = AcceptedStore::seeded(&*builds, fabric_id.clone(), initial.clone(), "mesh1.admin.1").await.unwrap();
    let mut cp = ControlPlane::new(builds.clone(), accepted, m1_a1.name.clone(), observed.read().await.clone(), crate::adopted_time());
    cp.topology = observed.clone();
    // The fabric-primary's door reads the node records its writers wrote (boot.rs: one nodes store),
    // never a birth from the gossip view (Q197).
    let _ = cp.node_rows.set(nodes_storage.clone());
    let opened = cp.submit("POST /api/build", TopologyChange::DeleteFabric).await.expect("DeleteFabric is accepted as a Build");
    let build_id = opened.build_id;
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();

    // Step 1a: initial plan orders leaf-inward: non-FP mesh (mesh2) first, FP's own mesh (mesh1)
    // after, and DeleteFabric at the end.
    let proj = builds.read_build(&build_id).await.unwrap();
    let initial_ops = plan_for_build(&proj, &*observed.read().await).operations;
    assert_eq!(
        initial_ops,
        vec![
            BuildOperation::ShutdownMesh { mesh: "mesh2".into(), mesh_id: id2.clone() },
            BuildOperation::ShutdownMesh { mesh: "mesh1".into(), mesh_id: id1.clone() },
            BuildOperation::DeleteFabric { fabric_id: fabric_id.clone() },
        ],
        "fabric.delete plans non-FP meshes first, FP's own mesh last, then DeleteFabric"
    );

    let joins = Joins::default();
    let view_sink = View::default();
    let template = LaunchTemplate { fabric: "fabric1".into(),
        fabric_id: fabric_id.clone(),
        executable: "/nonexistent/rdm-rpc-node".into(),
        seeds: vec![("fpkey".into(), "127.0.0.1:34000".parse().unwrap())],
        launcher: rdm_core::launch::Launcher { name: m1_a1.name.clone(), node_id: m1_a1.node_id.clone(), incarnation: m1_a1.incarnation_id.clone().unwrap() },
        env: BTreeMap::new(),
        data_root: data_root.clone(),
        store_dir: data_root.clone(),
        issuing: None,
        roots: Default::default(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
        binding: None,
    };
    let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view_sink, lifecycle: &NoLifecycleEvents, builds: &*builds, template: &template, timeouts: Timeouts::default() };

    // Execute ShutdownMesh(mesh2): mesh2.admin.1 deletes mesh2.rpc.1 and hands off MeshDeleted.
    let op_m2 = operation(&id2);
    let key_m2 = MeshDeleteKey { mesh_id: id2.clone(), build_id: build_id.clone(), attempt: 1, operation: op_m2.clone() };
    let me_m2 = RosterBirth::of(&m2_a1).unwrap();
    let rt_m2 = RuntimeProof::of(&handles["mesh2.admin.1"].fact().unwrap());
    let m2_records = vec![m2_a1.clone(), m2_r1.clone()];
    let roster_m2 = accepted_roster(&*builds, &key_m2, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me_m2, &rt_m2, || live_roster(&m2_records)).await.unwrap();
    let leaving_m2 = accepted_roster(&*builds, &key_m2, STEP_LEAVING_ROSTER, "mesh2.admin.1", &me_m2, &rt_m2, || live_roster(&m2_records)).await.unwrap();
    // mesh2.admin.1 executes its members' node.deletes: their rows Deleted are node.delete's own.
    view_sink.writes_through(rows_m2.clone());
    members_exited(&*builds, &key_m2, "mesh2.admin.1", &leaving_m2, &me_m2, rt_m2.clone(), &m2_records, &[], |n: Node| {
        let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
        async move {
            let name = n.name.to_string();
            pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: build_id.clone(), attempt: 1, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
        }
    })
    .await
    .unwrap();
    let facts = builds.facts().await.unwrap();
    let admitted_m2 = rdm_admin::mesh_delete::Admitted { roster: roster_m2, final_primary: me_m2.clone(), final_runtime: rt_m2, amendments: Vec::new() };
    let HandoffVerdict::Accept(manifest_m2) = handoff_verdict(&facts, &key_m2, &admitted_m2, &manifest_reference(build_id.as_str(), 1, &op_m2, false)) else { panic!("mesh2 handoff accepted") };
    record_completion(&*builds, &key_m2, "mesh1.admin.1", &manifest_m2).await.unwrap();
    observed.write().await.nodes.retain(|n| n.name != m2_r1.name);
    observed.write().await.meshes.iter_mut().find(|m| m.name == "mesh2").unwrap().status = ScopeStatus::Dead;

    // Step 2: after mesh2's MeshDeleted, re-planning the SAME Build plans DeleteLiveNode(mesh2.admin.1)
    // before ShutdownMesh(mesh1) and DeleteFabric!
    let proj = builds.read_build(&build_id).await.unwrap();
    let after_m2_deleted = plan_for_build(&proj, &*observed.read().await).operations;
    assert_eq!(
        after_m2_deleted,
        vec![
            BuildOperation::DeleteLiveNode { node: m2_a1.name.clone(), exited: None, birth: rdm_admin::build::PinnedBirth::of(&m2_a1) },
            BuildOperation::ShutdownMesh { mesh: "mesh1".into(), mesh_id: id1.clone() },
            BuildOperation::DeleteFabric { fabric_id: fabric_id.clone() },
        ],
        "after mesh2's MeshDeleted, fabric.delete plans DeleteLiveNode(mesh2.admin.1) on the same Build"
    );
    assert_eq!(executor_for(&after_m2_deleted[0], &*observed.read().await), Some(m1_a1.name.clone()), "fabric-primary executes DeleteLiveNode(mesh2.admin.1)");

    // Execute DeleteLiveNode(mesh2.admin.1) as fabric-primary: mesh2.admin.1 stops, its row is written
    // Deleted by FP as acting primary, and MeshLeft + mesh_left_rows write mesh2 Dead.
    let term_m2_a1 = pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: build_id.clone(), attempt: 1, node: m2_a1.clone(), handle: handles["mesh2.admin.1"].clone(), unreached: false }).await.unwrap();
    observed.write().await.nodes.iter_mut().filter(|n| n.name == m2_a1.name).for_each(|n| {
        n.status = NodeStatus::Leaving;
        n.stopped = true;
    });
    rows_m1.deleted(&m2_a1, "node.delete:mesh2.admin.1").await.unwrap();
    observed.write().await.nodes.retain(|n| n.name != m2_a1.name);
    let left_m2 = mesh_left_proof(&manifest_m2, &term_m2_a1).unwrap();
    mesh_left_rows(Some(&rows_m1), &mesh_storage, &Default::default(), &declared, "mesh1.admin.1", &key_m2, "mesh2", &left_m2.receipts).await.unwrap();
    builds
        .append_step_receipt(&BuildStepReceipt {
            mesh: None,
            build_id: build_id.clone(),
            attempt: 1,
            operation: op_m2.clone(),
            step: STEP_MESH_LEFT.into(),
            outcome: StepOutcome::Complete,
            output: serde_json::to_value(&left_m2).ok(),
            executor: Some("mesh1.admin.1".into()),
        })
        .await
        .unwrap();

    // Step 1b: now only mesh1 (the FP's own mesh) and DeleteFabric remain. On mesh1, seat_move is
    // None (the fabric seat stays on mesh1.admin.1) and executor_for is mesh1.admin.1.
    let proj = builds.read_build(&build_id).await.unwrap();
    let after_m2_left = plan_for_build(&proj, &*observed.read().await).operations;
    assert_eq!(
        after_m2_left,
        vec![
            BuildOperation::ShutdownMesh { mesh: "mesh1".into(), mesh_id: id1.clone() },
            BuildOperation::DeleteFabric { fabric_id: fabric_id.clone() },
        ],
        "once mesh2 is MeshLeft, ShutdownMesh(mesh1) and DeleteFabric remain"
    );
    assert_eq!(seat_move(&after_m2_left[0], &*observed.read().await), None, "the fabric seat stays on mesh1.admin.1 during its own mesh's mesh.delete");
    assert_eq!(executor_for(&after_m2_left[0], &*observed.read().await), Some(m1_a1.name.clone()));

    // Execute ShutdownMesh(mesh1): mesh1.admin.1 deletes mesh1.rpc.1, commits mesh1 Dead in
    // status.storage and mesh.storage, and records MeshDeleted while staying alive itself.
    let op_m1 = operation(&id1);
    let key_m1 = MeshDeleteKey { mesh_id: id1.clone(), build_id: build_id.clone(), attempt: 1, operation: op_m1.clone() };
    let me_m1 = RosterBirth::of(&m1_a1).unwrap();
    let rt_m1 = RuntimeProof::of(&handles["mesh1.admin.1"].fact().unwrap());
    let m1_records = vec![m1_a1.clone(), m1_r1.clone()];
    let roster_m1 = accepted_roster(&*builds, &key_m1, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me_m1, &rt_m1, || live_roster(&m1_records)).await.unwrap();
    let leaving_m1 = accepted_roster(&*builds, &key_m1, STEP_LEAVING_ROSTER, "mesh1.admin.1", &me_m1, &rt_m1, || live_roster(&m1_records)).await.unwrap();
    view_sink.writes_through(rows_m1.clone());
    members_exited(&*builds, &key_m1, "mesh1.admin.1", &leaving_m1, &me_m1, rt_m1.clone(), &m1_records, &[], |n: Node| {
        let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
        async move {
            let name = n.name.to_string();
            pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: build_id.clone(), attempt: 1, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
        }
    })
    .await
    .unwrap();
    let facts = builds.facts().await.unwrap();
    let admitted_m1 = rdm_admin::mesh_delete::Admitted { roster: roster_m1, final_primary: me_m1.clone(), final_runtime: rt_m1, amendments: Vec::new() };
    let HandoffVerdict::Accept(manifest_m1) = handoff_verdict(&facts, &key_m1, &admitted_m1, &manifest_reference(build_id.as_str(), 1, &op_m1, false)) else { panic!("mesh1 handoff accepted") };
    { let mut r = mesh_storage.mesh(&id1.clone()).await.unwrap().unwrap_or_else(|| rdm_admin::storage::MeshRecord::new(id1.clone(), "mesh1")); r.states.insert(MeshState::Dead); mesh_storage.put_mesh(&r).await.unwrap(); }
    rdm_admin::storage::claim_mesh(&mesh_storage, None, &rdm_admin::storage::MeshRecord { mesh_id: id1.clone(), name: "mesh1".into(), dead: true, states: Default::default() }).await.unwrap().unwrap();
    record_completion(&*builds, &key_m1, "mesh1.admin.1", &manifest_m1).await.unwrap();
    observed.write().await.nodes.retain(|n| n.name != m1_r1.name);
    observed.write().await.meshes.iter_mut().find(|m| m.name == "mesh1").unwrap().status = ScopeStatus::Dead;

    // Step 3: after mesh1's MeshDeleted, re-planning plans ONLY DeleteFabric (mesh1.admin.1 is NOT
    // deleted inside fabric.delete!).
    let proj = builds.read_build(&build_id).await.unwrap();
    let before_complete = plan_for_build(&proj, &*observed.read().await).operations;
    assert_eq!(
        before_complete,
        vec![BuildOperation::DeleteFabric { fabric_id: fabric_id.clone() }],
        "the fabric-primary is NOT deleted inside fabric.delete; only DeleteFabric remains"
    );
    complete_fabric_delete(&*builds, &*fabric_storage, &build_id, 1, &fabric_id, "mesh1.admin.1").await.expect("complete_fabric_delete releases the fabric name and records fabric.deleted");
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: build_id.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();

    // At fabric.deleted:
    // - No operations remain on the fabric.delete Build.
    // - The fabric name is released in fabric.storage (`fabric()` is None).
    // - The fabric-primary `mesh1.admin.1` is STILL ALIVE (`Ready` row in nodes.storage,
    //   runtime not terminated).
    let proj = builds.read_build(&build_id).await.unwrap();
    assert!(plan_for_build(&proj, &*observed.read().await).operations.is_empty(), "fabric.delete Build has no operations left at fabric.deleted");
    assert_eq!(fabric_storage.fabric().await.unwrap(), None, "fabric identity row is released at fabric.deleted");
    assert!(!fleet.terminated.lock().unwrap().contains(&"mesh1.admin.1".to_string()), "fabric-primary is still alive at fabric.deleted");
    assert_eq!(nodes_storage.record_at(&m1_a1.name).await.unwrap().unwrap().status, NodeStatus::Ready, "fabric-primary's node row is still live at fabric.deleted");

    // Step 4: a separate call from outside the fabric runs `node.delete` of the fabric-primary.
    assert!(cp.of_deleted_mesh(&m1_a1.name).await, "the fabric-primary's mesh is Dead after fabric.deleted");
    let fp_delete = cp
        .open_attempt_in(tracing::info_span!("cell.fp-delete"), "build.create", AttemptReason::Delete, m1_a1.name.clone(), rdm_admin::control_plane::ActionKind::Delete)
        .await
        .expect("the separate node.delete of the fabric-primary is admitted");
    assert_eq!(fp_delete.attempt, 2);
    let proj2 = builds.read_build(&build_id).await.unwrap();
    let fp_ops = plan_for_build(&proj2, &*observed.read().await).operations;
    assert_eq!(fp_ops, vec![BuildOperation::DeleteLiveNode { node: m1_a1.name.clone(), exited: None, birth: rdm_admin::build::PinnedBirth::of(&m1_a1) }], "attempt 2 plans only the fabric-primary's own node.delete");
    assert_eq!(seat_move(&fp_ops[0], &*observed.read().await), None, "the last node of the last mesh moves no seat on its separate node.delete");
    assert_eq!(executor_for(&fp_ops[0], &*observed.read().await), Some(m1_a1.name.clone()), "the fabric-primary executes its own final node.delete");

    let term_m1_a1 = pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: build_id.clone(), attempt: 2, node: m1_a1.clone(), handle: handles["mesh1.admin.1"].clone(), unreached: false }).await.unwrap();
    observed.write().await.nodes.iter_mut().filter(|n| n.name == m1_a1.name).for_each(|n| {
        n.status = NodeStatus::Leaving;
        n.stopped = true;
    });
    rows_m1.deleted(&m1_a1, "node.delete:mesh1.admin.1").await.unwrap();
    let left_m1 = mesh_left_proof(&manifest_m1, &term_m1_a1).unwrap();
    mesh_left_rows(Some(&rows_m1), &mesh_storage, &Default::default(), &declared, "mesh1.admin.1", &key_m1, "mesh1", &left_m1.receipts).await.unwrap();

    // Every node row is Deleted, both mesh rows are Dead in status.storage and mesh.storage, and
    // the fabric name is released.
    for n in &all_nodes {
        let row = nodes_storage.record_at(&n.name).await.unwrap().expect("node row exists");
        assert_eq!(row.status, NodeStatus::Deleted, "{}'s node row is Deleted", n.name);
    }
    for (id, name) in [(&id1, "mesh1"), (&id2, "mesh2")] {
        assert!(mesh_storage.mesh(&id.clone()).await.unwrap().is_some_and(|r| r.states.contains(&MeshState::Dead)), "{name} status row is Dead");
    }
    let mesh_rows = mesh_storage.meshes().await.unwrap();
    assert!(mesh_rows.iter().all(|m| m.dead), "every mesh row in mesh.storage is Dead: {mesh_rows:?}");
    assert_eq!(fabric_storage.fabric().await.unwrap(), None, "no fabric identity row remains");

    let _ = std::fs::remove_dir_all(&data_root);
}

/// crit:e12.s59.2 — An unanswered mesh during `fabric.delete` (R-FS1 revised; node-recovery.md step 3:
/// any verdict but `reached` re-creates or deletes, nothing waits for a proof):
/// 1. No member answers and none runs: every member is deleted by its node.delete (its node row
///    `Deleted`), the mesh row is written `Dead` as acting primary with no survivor, and `MeshDeleted`
///    and `MeshLeft` are recorded without commanding anything or creating a `node-admin`.
/// 2. Members live (`carrier-edge-lost`: the `node-admin`s unreached while ordinary members live)
///    deletes the node-admins, records `AcceptedRoster` + predecessor, selects `mesh2.admin.2` via
///    `leaving_mesh_admin_path`, and once `mesh2.admin.2` is created into `mesh2` under its own
///    `MeshId` on the same Build, amends the roster (`RosterAmended`), adopts held state, runs
///    `mesh.delete`, and is removed last by the fabric-primary's `node.delete`.
#[tokio::test]
async fn unanswered_mesh_during_fabric_delete_creates_one_admin_when_members_live_or_deletes_every_member_when_none_runs() {
    let data_root = std::env::temp_dir().join(format!("fabric-delete-unanswered-{}", NodeId::mint()));
    let fabric_id = FabricId::mint();
    let (id1, id2) = (MeshId::mint(), MeshId::mint());
    let fp = node_record("mesh1.admin.1", true, true);

    // ---- Arm 1 (Held / unproven) & Arm 2 (Fully crashed) on mesh2 ----
    {
        let m2_a1 = node_record("mesh2.admin.1", false, false);
        let m2_r1 = node_record("mesh2.rpc.1", false, false);
        let m2_nodes = vec![m2_a1.clone(), m2_r1.clone()];
        let handles: BTreeMap<String, DeploymentHandle> = [(&m2_a1, 52_001u32), (&m2_r1, 52_002u32)].into_iter().map(|(n, pid)| (n.name.to_string(), handle(&n.name.to_string(), pid))).collect();
        let fleet = Fleet {
            pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(),
            // Initially mesh2.admin.1's runtime is still running (unproven exited verdict).
            running_pids: Mutex::new(BTreeSet::from([52_001u32])),
            ..Default::default()
        };
        let view = Arc::new(RwLock::new(Topology {
            fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(id1.clone()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![fp.clone(), m2_a1.clone(), m2_r1.clone()],
        }));
        let nodes_storage = Arc::new(MemoryNodesStorage::default());
        let rows_fp = Arc::new(NodeRows::new(fp.name.clone(), fp.node_id.clone(), view.clone(), nodes_storage.clone()));
        rows_written(&nodes_storage, &rows_fp, &m2_nodes).await;
        for (n, h) in [(&m2_a1, &handles["mesh2.admin.1"]), (&m2_r1, &handles["mesh2.rpc.1"])] {
            nodes_storage.put_runtime(&RuntimeRow { node_id: n.node_id.clone(), name: n.name.clone(), incarnation_id: n.incarnation_id.clone().unwrap(), runtime: h.fact().unwrap(), data_dir: None }).await.unwrap();
        }
        let mesh_storage = MemoryMeshStorage::default();
        rdm_admin::storage::claim_mesh(&mesh_storage, None, &rdm_admin::storage::MeshRecord { mesh_id: id2.clone(), name: "mesh2".into(), dead: false, states: Default::default() }).await.unwrap().unwrap();
        // The fabric-primary's records.
        let records = Records::default();
        records.set_status(nodes_storage.clone(), Arc::new(MemoryMeshStorage::default()), Arc::new(MemoryFabricStorage::new()), "mesh1".into(), fp.name.clone(), fp.node_id.clone());
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: FabricTopology::root("fabric1", "mesh1", id1.clone()), submitted_change: Some(TopologyChange::DeleteFabric), submitted_at_ms: 0 })
            .await
            .unwrap();
        let op_m2 = operation(&id2);
        let key_m2 = MeshDeleteKey { mesh_id: id2.clone(), build_id: build_id.clone(), attempt: 1, operation: op_m2.clone() };
        let held_paths: Vec<PathName> = view.read().await.nodes.iter().map(|n| n.name.clone()).collect();
        // Each exited birth's node.delete as a crashed target, the fabric-primary its executor: the
        // ordinary workflow on the exited-verdict row, its NodeDeleted writing the row through rows_fp.
        let joins = Joins::default();
        let sink = View::default();
        sink.writes_through(rows_fp.clone());
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: fabric_id.clone(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("fpkey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: fp.name.clone(), node_id: fp.node_id.clone(), incarnation: fp.incarnation_id.clone().unwrap() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &sink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let delete = |b: RosterBirth| {
            let (pipeline, handles, build_id, m2_nodes) = (&pipeline, &handles, &build_id, &m2_nodes);
            async move {
                let record = m2_nodes.iter().find(|n| n.node_id.to_string() == b.node_id).cloned().ok_or_else(|| format!("{} is not a mesh2 record", b.node))?;
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: build_id.clone(), attempt: 1, node: record, handle: handles[&b.node].clone(), unreached: true }).await.map_err(|e| e.to_string())
            }
        };

        // Every node of mesh2 (mesh2.admin.1 and mesh2.rpc.1) has crashed. Nothing is commanded and no
        // node-admin is created; rows are written Deleted by each node.delete and mesh2 is written Dead
        // as acting primary.
        fleet.running_pids.lock().unwrap().clear();
        let crashed = unanswered_mesh_delete(&builds, &fleet, &records, Some(&rows_fp), &mesh_storage, &*nodes_storage, &key_m2, "mesh1.admin.1", "mesh2", &m2_nodes, &held_paths, &delete)
            .await
            .expect("a mesh none of whose members runs resolves on its members' deletes");
        let UnansweredMeshOutcome::FullyCrashed(manifest) = crashed else { panic!("expected FullyCrashed, got {crashed:?}") };
        assert_eq!(manifest.receipts.len(), 2, "every birth of mesh2 has an exit receipt");
        for n in &m2_nodes {
            let row = nodes_storage.record_at(&n.name).await.unwrap().unwrap();
            assert_eq!(row.status, NodeStatus::Deleted, "{}'s row is written Deleted by its node.delete", n.name);
        }
        assert!(mesh_storage.mesh(&id2.clone()).await.unwrap().is_some_and(|r| r.states.contains(&MeshState::Dead)), "mesh2 status row is written Dead by acting primary");
        assert!(mesh_storage.meshes().await.unwrap().iter().all(|m| m.dead), "mesh2 row in mesh.storage is Dead");
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        assert_eq!(owner_resume(&steps, &op_m2), Ok(OwnerResume::Left), "MeshLeft is recorded so no separate DeleteLiveNode is planned for the crashed primary");
        // Each roster birth's end is node.delete's own (mesh-delete.md step 3, "each member's step is
        // its own `node.delete:<path.name>`"): its TerminateRuntime, NodeDeleted and Complete receipts
        // under its own key, never a receipt copied by hand.
        for n in &m2_nodes {
            let op = format!("node.delete:{}", n.name);
            for step in ["TerminateRuntime", "NodeDeleted", "Complete"] {
                assert!(steps.iter().any(|r| r.operation == op && r.step == step && r.outcome == StepOutcome::Complete), "{}'s {step} receipt under {op}: {:?}", n.name, steps.iter().map(|r| format!("{}/{}", r.operation, r.step)).collect::<Vec<_>>());
            }
        }
        assert!(steps.iter().all(|r| !r.operation.starts_with(&format!("{op_m2}/"))), "no receipt is written under a key outside node.delete's: {:?}", steps.iter().map(|r| r.operation.clone()).collect::<BTreeSet<_>>());
    }

    // ---- Arm 3 (Members live / carrier-edge-lost: create one node-admin into mesh2) ----
    {
        let m2_a1 = node_record("mesh2.admin.1", false, false);
        let m2_r1 = node_record("mesh2.rpc.1", true, false);
        let m2_nodes = vec![m2_a1.clone(), m2_r1.clone()];
        let handles: BTreeMap<String, DeploymentHandle> = [(&m2_a1, 53_001u32), (&m2_r1, 53_002u32)].into_iter().map(|(n, pid)| (n.name.to_string(), handle(&n.name.to_string(), pid))).collect();
        let fleet = Fleet {
            pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(),
            // mesh2.admin.1 exited, while mesh2.rpc.1 is still running!
            running_pids: Mutex::new(BTreeSet::from([53_002u32])),
            ..Default::default()
        };
        let view = Arc::new(RwLock::new(Topology {
            fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(id1.clone()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![fp.clone(), m2_a1.clone(), m2_r1.clone()],
        }));
        let nodes_storage = Arc::new(MemoryNodesStorage::default());
        let rows_fp = Arc::new(NodeRows::new(fp.name.clone(), fp.node_id.clone(), view.clone(), nodes_storage.clone()));
        rows_written(&nodes_storage, &rows_fp, &m2_nodes).await;
        for (n, h) in [(&m2_a1, &handles["mesh2.admin.1"]), (&m2_r1, &handles["mesh2.rpc.1"])] {
            nodes_storage.put_runtime(&RuntimeRow { node_id: n.node_id.clone(), name: n.name.clone(), incarnation_id: n.incarnation_id.clone().unwrap(), runtime: h.fact().unwrap(), data_dir: None }).await.unwrap();
        }
        let mesh_storage = MemoryMeshStorage::default();
        rdm_admin::storage::claim_mesh(&mesh_storage, None, &rdm_admin::storage::MeshRecord { mesh_id: id2.clone(), name: "mesh2".into(), dead: false, states: Default::default() }).await.unwrap().unwrap();
        let declared = Mutex::new(Declared::default());
        let records = Records::default();
        records.set_status(nodes_storage.clone(), Arc::new(MemoryMeshStorage::default()), Arc::new(MemoryFabricStorage::new()), "mesh1".into(), fp.name.clone(), fp.node_id.clone());
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: FabricTopology::root("fabric1", "mesh1", id1.clone()), submitted_change: Some(TopologyChange::DeleteFabric), submitted_at_ms: 0 })
            .await
            .unwrap();
        let op_m2 = operation(&id2);
        let key_m2 = MeshDeleteKey { mesh_id: id2.clone(), build_id: build_id.clone(), attempt: 1, operation: op_m2.clone() };
        let held_paths: Vec<PathName> = view.read().await.nodes.iter().map(|n| n.name.clone()).collect();
        let joins = Joins::default();
        let view_sink = View::default();
        view_sink.writes_through(rows_fp.clone());
        let fp_template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: fabric_id.clone(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("fpkey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: fp.name.clone(), node_id: fp.node_id.clone(), incarnation: fp.incarnation_id.clone().unwrap() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let fp_pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view_sink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &fp_template, timeouts: Timeouts::default() };
        // The unreached admin's node.delete, the fabric-primary its executor.
        let delete = |b: RosterBirth| {
            let (pipeline, handles, build_id, m2_nodes) = (&fp_pipeline, &handles, &build_id, &m2_nodes);
            async move {
                let record = m2_nodes.iter().find(|n| n.node_id.to_string() == b.node_id).cloned().ok_or_else(|| format!("{} is not a mesh2 record", b.node))?;
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: build_id.clone(), attempt: 1, node: record, handle: handles[&b.node].clone(), unreached: true }).await.map_err(|e| e.to_string())
            }
        };
        let outcome = unanswered_mesh_delete(&builds, &fleet, &records, Some(&rows_fp), &mesh_storage, &*nodes_storage, &key_m2, "mesh1.admin.1", "mesh2", &m2_nodes, &held_paths, &delete)
            .await
            .expect("members live with the admin unreached resolves to CreateAdmin");
        let UnansweredMeshOutcome::CreateAdmin { path, predecessor } = outcome else { panic!("expected CreateAdmin, got {outcome:?}") };
        assert_eq!(path, "mesh2.admin.2".parse::<PathName>().unwrap(), "creates one node-admin at the next free ordinal above mesh2.admin.1");
        assert_eq!(predecessor, RosterBirth::of(&m2_a1).unwrap());
        // The exited admin's roster receipt is its own node.delete's TerminateRuntime (mesh-delete.md
        // "Resume after the primary's loss" step 1), and nothing is written under any other key.
        let steps = builds.read_build(&build_id).await.unwrap().steps;
        assert!(steps.iter().any(|r| r.operation == format!("node.delete:{}", m2_a1.name) && r.step == "TerminateRuntime" && r.outcome == StepOutcome::Complete), "mesh2.admin.1's exit is its node.delete's receipt: {:?}", steps.iter().map(|r| format!("{}/{}", r.operation, r.step)).collect::<Vec<_>>());
        assert!(steps.iter().all(|r| !r.operation.starts_with(&format!("{op_m2}/"))), "no receipt is written under a key outside node.delete's: {:?}", steps.iter().map(|r| r.operation.clone()).collect::<BTreeSet<_>>());

        // The one node-admin `mesh2.admin.2` is created into `mesh2` under `id2` on the same Build,
        // becomes mesh-primary, amends the roster, adopts held state (`roster_rows_read`), and runs
        // `mesh.delete` (`members_exited`), then is removed last by the fabric-primary's `node.delete`.
        let m2_a2 = node_record("mesh2.admin.2", true, false);
        let h_a2 = handle("mesh2.admin.2", 53_003);
        let mut fleet = fleet;
        fleet.pids.insert(53_003, "mesh2.admin.2".into());
        let rt_a2 = RuntimeProof::of(&h_a2.fact().unwrap());
        let me_a2 = RosterBirth::of(&m2_a2).unwrap();
        view.write().await.nodes.retain(|n| n.name != m2_a1.name);
        view.write().await.nodes.push(m2_a2.clone());
        let rows_a2 = Arc::new(NodeRows::new(m2_a2.name.clone(), m2_a2.node_id.clone(), view.clone(), nodes_storage.clone()));
        rows_written(&nodes_storage, &rows_a2, std::slice::from_ref(&m2_a2)).await;

        amend_roster(&builds, &key_m2, "mesh1.admin.1", &RosterAmended { predecessor: predecessor.clone(), final_primary: me_a2.clone(), final_runtime: rt_a2.clone() })
            .await
            .expect("RosterAmended admits the created mesh2.admin.2");

        let owner_facts = builds.facts().await.unwrap();
        let (leaving, known) = leaving_roster(&builds, &builds, &key_m2, "mesh2.admin.2", &me_a2, &rt_a2, || async { Ok(owner_facts) })
            .await
            .unwrap();
        let current_nodes = view.read().await.nodes.clone();
        let records_for_leave = roster_records(&current_nodes, &leaving, |_| None);
        roster_rows_read(Some(&rows_a2), "mesh2.admin.2", &op_m2, &leaving, &records_for_leave).await;

        // mesh2.admin.2 executes its members' node.deletes: their rows Deleted are node.delete's own.
        view_sink.writes_through(rows_a2.clone());
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: fabric_id.clone(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("fpkey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher { name: m2_a2.name.clone(), node_id: m2_a2.node_id.clone(), incarnation: m2_a2.incarnation_id.clone().unwrap() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let pipeline = DeploymentPipeline { provider: &fleet, joins: &joins, observer: &NoRpc, sink: &view_sink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let manifest = members_exited(&builds, &key_m2, "mesh2.admin.2", &leaving, &me_a2, rt_a2.clone(), &records_for_leave, &known, |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let name = n.name.to_string();
                pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: build_id.clone(), attempt: 1, node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
            }
        })
        .await
        .expect("created admin mesh2.admin.2 completes mesh.delete of mesh2");

        record_completion(&builds, &key_m2, "mesh1.admin.1", &manifest).await.unwrap();
        let term_a2 = pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { build_id: build_id.clone(), attempt: 1, node: m2_a2.clone(), handle: h_a2, unreached: false }).await.unwrap();
        view.write().await.nodes.iter_mut().filter(|n| n.name == m2_a2.name).for_each(|n| {
            n.status = NodeStatus::Leaving;
            n.stopped = true;
        });
        rows_fp.deleted(&m2_a2, "node.delete:mesh2.admin.2").await.unwrap();
        let left = mesh_left_proof(&manifest, &term_a2).unwrap();
        mesh_left_rows(Some(&rows_fp), &mesh_storage, &Default::default(), &declared, "mesh1.admin.1", &key_m2, "mesh2", &left.receipts).await.unwrap();

        for name in [&m2_a1.name, &m2_r1.name, &m2_a2.name] {
            let row = nodes_storage.record_at(name).await.unwrap().unwrap();
            assert_eq!(row.status, NodeStatus::Deleted, "{name}'s node row is Deleted");
        }
        assert!(mesh_storage.mesh(&id2.clone()).await.unwrap().is_some_and(|r| r.states.contains(&MeshState::Dead)));
    }

    let _ = std::fs::remove_dir_all(&data_root);
}

// @feature: fabric-delete
/// CONTRACT:
/// - `docs/architecture/lifecycles/fabric-delete.md:15` — "Caller | Typed caller"
/// - `docs/architecture/lifecycles/fabric-delete.md:17` — "Driver | The fabric-primary, one Build"
/// - `docs/architecture/lifecycles/fabric-delete.md:27-33` — "1. Run [mesh.delete](mesh-delete.md) on each mesh, the fabric-primary's own mesh last. Each mesh.delete keeps its mesh-primary alive until every other member is deleted and it has reported mesh.deleted. On its own mesh the fabric-primary stays, because no other mesh is left to take it, so that mesh.delete skips its [node.fabric-primary.update](node-primary-update.md#the-fabric-primary-nodefabric-primaryupdate) step. A mesh whose mesh-primary does not answer is reached through [mesh investigation](mesh-investigation.md#during-fabricdelete), whose outcome during fabric.delete is the delete."
/// - `docs/architecture/lifecycles/fabric-delete.md:34-35` — "2. After each other mesh's mesh.deleted, run [node.delete](node-delete.md) of that mesh's mesh-primary. The fabric-primary is the executor and proves its exit."
/// - `docs/architecture/lifecycles/fabric-delete.md:36-37` — "3. The fabric-primary, still alive, reports fabric.deleted. The primary runs the workflow needs are retained until it completes ([Elections](../fabric-node-lifecycle-elections.md))."
/// - `docs/architecture/lifecycles/fabric-delete.md:38-40` — "4. A separate call from outside the fabric runs node.delete of the fabric-primary. Its node.deleted is its last reply, and the caller takes its word, because nobody is left in the fabric to record it. This is the one exception to the receipt."
/// - `docs/architecture/lifecycles/fabric-delete.md:42` — "A local OS signal or a node stop is node lifecycle and starts no fabric.delete."
/// - `docs/architecture/lifecycles/fabric-delete.md:64-68` — "A step that cannot complete ends the workflow `Failed { step, reason }`, naming that step, and no fabric.deleted follows. Completion of steps 1 to 3 rests on each deleted runtime's exact its node.delete receipt: a runtime inside the workflow whose delete has no receipt is never reported deleted. A node the fabric-primary cannot reach, directly or through a node-admin, and cannot prove exited is named in `Failed { step, reason }`."
/// - `docs/architecture/lifecycles/fabric-delete.md:70-73` — "A fabric-primary lost mid-delete is fabric recovery's: its executor continues the delete as fabric-primary and re-creates nothing ([fabric recovery](fabric-recovery.md) step 4, Q155); a mesh left with no node-admin is deleted in process at the executor, as any mesh with no mesh-primary is. Once a runtime's deletion receipt is durable, nothing recovers it for being absent."
#[test]
fn fabric_delete_step_faults_recover_or_fail_as_the_page_specifies() {
    let cap = crate::mesh_delete_resume::capture();
    let data_root = std::env::temp_dir().join(format!("fabric-delete-step-faults-{}", NodeId::mint()));
    let fabric_id = FabricId::mint();
    let (id1, id2, id3) = (MeshId::mint(), MeshId::mint(), MeshId::mint());
    let fp = node_record("mesh1.admin.1", true, true);
    let m1_r1 = node_record("mesh1.rpc.1", false, false);
    let m2_a1 = node_record("mesh2.admin.1", false, false);
    let m2_r1 = node_record("mesh2.rpc.1", true, false);
    let m3_a1 = node_record("mesh3.admin.1", false, false);
    let m3_r1 = node_record("mesh3.rpc.1", false, false);

    let all_nodes = vec![
        fp.clone(),
        m1_r1.clone(),
        m2_a1.clone(),
        m2_r1.clone(),
        m3_a1.clone(),
        m3_r1.clone(),
    ];
    let handles: BTreeMap<String, DeploymentHandle> = all_nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.name.to_string(), handle(&n.name.to_string(), 54_000 + i as u32)))
        .collect();
    let fleet = Fleet {
        pids: handles.iter().map(|(n, h)| (h.pid.unwrap(), n.clone())).collect(),
        // mesh2.rpc.1 (pid 54_003) is still running while mesh2.admin.1 exited;
        // mesh3.admin.1 and mesh3.rpc.1 have both exited.
        running_pids: Mutex::new(BTreeSet::from([handles["mesh2.rpc.1"].pid.unwrap()])),
        ..Default::default()
    };

    let trace_id = cap.run(async {
        let root = tracing::info_span!("cell.fabric-delete-step-faults", fabric_id = %fabric_id);
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();

        let observed = Arc::new(RwLock::new(Topology {
            fabric: Fabric {
                id: fabric_id.clone(),
                name: "fabric1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: vec![
                Mesh { id: Some(id1.clone()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(id3.clone()), name: "mesh3".into(), status: ScopeStatus::Ready },
            ],
            nodes: all_nodes.clone(),
        }));

        let fabric_storage = Arc::new(MemoryFabricStorage::new());
        let identity = FabricIdentity { fabric_id: fabric_id.clone(), name: "fabric1".into() };
        rdm_admin::fabric_storage::claim_identity(&*fabric_storage, &identity).await.unwrap().unwrap();
        let mesh_storage = MemoryMeshStorage::default();
        for (id, name) in [(&id1, "mesh1"), (&id2, "mesh2"), (&id3, "mesh3")] {
            rdm_admin::storage::claim_mesh(
                &mesh_storage,
                None,
                &rdm_admin::storage::MeshRecord {
                    mesh_id: id.clone(),
                    name: name.into(),
                    dead: false,
                    states: Default::default(),
                },
            )
            .await
            .unwrap()
            .unwrap();
        }
        let declared = Mutex::new(Declared::default());
        let nodes_storage = Arc::new(MemoryNodesStorage::default());
        let rows_fp = Arc::new(NodeRows::new(fp.name.clone(), fp.node_id.clone(), observed.clone(), nodes_storage.clone()));
        rows_written(&nodes_storage, &rows_fp, &all_nodes).await;
        for (n, h) in handles.iter().map(|(k, h)| (all_nodes.iter().find(|n| n.name.to_string() == *k).unwrap(), h)) {
            nodes_storage
                .put_runtime(&RuntimeRow {
                    node_id: n.node_id.clone(),
                    name: n.name.clone(),
                    incarnation_id: n.incarnation_id.clone().unwrap(),
                    runtime: h.fact().unwrap(),
                    data_dir: None,
                })
                .await
                .unwrap();
        }
        let records = Records::default();
        records.set_status(
            nodes_storage.clone(),
            Arc::new(MemoryMeshStorage::default()),
            fabric_storage.clone(),
            "mesh1".into(),
            fp.name.clone(),
            fp.node_id.clone(),
        );

        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let initial = FabricTopology::of_observed(&*observed.read().await);
        let accepted = AcceptedStore::seeded(&*builds, fabric_id.clone(), initial, "mesh1.admin.1").await.unwrap();
        let mut cp = ControlPlane::new(builds.clone(), accepted, fp.name.clone(), observed.read().await.clone(), crate::adopted_time());
        cp.topology = observed.clone();
        let _ = cp.node_rows.set(nodes_storage.clone());

        // Step 42: a node stop is node lifecycle and starts no fabric.delete.
        let stop_opened = cp
            .open_attempt_in(
                tracing::info_span!("cell.node-stop"),
                "node.stop",
                AttemptReason::Stop,
                m1_r1.name.clone(),
                rdm_admin::control_plane::ActionKind::Stop,
            )
            .await
            .expect("node.stop opens an attempt on the existing topology Build");
        assert!(
            !builds.read_build(&stop_opened.build_id).await.unwrap().deletes_fabric(),
            "a node stop starts no fabric.delete"
        );

        // Caller & Driver: typed caller submits DeleteFabric; the fabric-primary drives it as one Build.
        let opened = cp.submit("POST /api/build", TopologyChange::DeleteFabric).await.expect("DeleteFabric is accepted as one Build");
        let build_id = opened.build_id;
        builds
            .claim_attempt(&BuildAttemptClaim {
                claimant: Some("mesh1.admin.1".into()),
                mesh: None,
                build_id: build_id.clone(),
                attempt: 1,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();

        // Failure & recovery under fabric.delete: while fabric.delete is in force, an absent node is
        // never recovered or re-created (`rdm.node_admin.build.reject.via-fabric-delete-in-force`).
        let m1_r1_inc = m1_r1.incarnation_id.clone().unwrap();
        let in_force_err = cp
            .open_recovery("test", m1_r1.name.clone(), m1_r1.node_id.clone(), m1_r1_inc.clone())
            .await
            .expect_err("recovery is refused while fabric.delete is in force");
        assert!(matches!(in_force_err, rdm_admin::control_plane::Refusal::BuildInProgress(_)));
        let joins = Joins::default();
        let view_sink = View::default();
        view_sink.writes_through(rows_fp.clone());
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: fabric_id.clone(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![("fpkey".into(), "127.0.0.1:34000".parse().unwrap())],
            launcher: rdm_core::launch::Launcher {
                name: fp.name.clone(),
                node_id: fp.node_id.clone(),
                incarnation: fp.incarnation_id.clone().unwrap(),
            },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let pipeline = DeploymentPipeline {
            provider: &fleet,
            joins: &joins,
            observer: &NoRpc,
            sink: &view_sink,
            lifecycle: &NoLifecycleEvents,
            builds: &*builds,
            template: &template,
            timeouts: Timeouts::default(),
        };

        // Failure (fabric-delete.md:64-68): a node of an unanswered mesh that cannot be proven exited
        // (no durable runtime row) fails the step by name and writes no Deleted or Dead row.
        let op_m3 = operation(&id3);
        let key_m3 = MeshDeleteKey {
            mesh_id: id3.clone(),
            build_id: build_id.clone(),
            attempt: 1,
            operation: op_m3.clone(),
        };
        let m3_nodes = vec![m3_a1.clone(), m3_r1.clone()];
        let held_paths: Vec<PathName> = observed.read().await.nodes.iter().map(|n| n.name.clone()).collect();
        let empty_runtime_store = MemoryNodesStorage::default();
        let unproven_err = unanswered_mesh_delete(
            &*builds,
            &fleet,
            &records,
            Some(&rows_fp),
            &mesh_storage,
            &empty_runtime_store,
            &key_m3,
            "mesh1.admin.1",
            "mesh3",
            &m3_nodes,
            &held_paths,
            |_| async { unreachable!("unproven runtime refuses before delete") },
        )
        .await
        .expect_err("unproven runtime must fail unanswered_mesh_delete by name");
        assert!(
            unproven_err.contains(&op_m3) && unproven_err.contains("mesh3.admin.1"),
            "failure names step and node: {unproven_err}"
        );
        assert_eq!(nodes_storage.record_at(&m3_a1.name).await.unwrap().unwrap().status, NodeStatus::Ready);

        // Fault 1 (`mesh3` fully crashed): every member of `mesh3` is proven exited by the provider.
        // `unanswered_mesh_delete` deletes each member in-process without creating a `node-admin`,
        // writes all member rows `Deleted` and `mesh3` `Dead`, and records `MeshDeleted` + `MeshLeft`.
        let delete_m3 = |b: RosterBirth| {
            let (pipeline, handles, build_id, m3_nodes) = (&pipeline, &handles, &build_id, &m3_nodes);
            async move {
                let record = m3_nodes
                    .iter()
                    .find(|n| n.node_id.to_string() == b.node_id)
                    .cloned()
                    .ok_or_else(|| format!("{} is not a mesh3 record", b.node))?;
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                        build_id: build_id.clone(),
                        attempt: 1,
                        node: record,
                        handle: handles[&b.node].clone(),
                        unreached: true,
                    })
                    .await
                    .map_err(|e| e.to_string())
            }
        };
        let crashed_m3 = unanswered_mesh_delete(
            &*builds,
            &fleet,
            &records,
            Some(&rows_fp),
            &mesh_storage,
            &*nodes_storage,
            &key_m3,
            "mesh1.admin.1",
            "mesh3",
            &m3_nodes,
            &held_paths,
            &delete_m3,
        )
        .await
        .expect("fully crashed leaf mesh3 resolves on its members' deletes");
        assert!(matches!(crashed_m3, UnansweredMeshOutcome::FullyCrashed(_)));
        assert_eq!(owner_resume(&builds.read_build(&build_id).await.unwrap().steps, &op_m3), Ok(OwnerResume::Left));
        observed.write().await.nodes.retain(|n| n.name != m3_a1.name && n.name != m3_r1.name);
        observed.write().await.meshes.iter_mut().find(|m| m.name == "mesh3").unwrap().status = ScopeStatus::Dead;

        // Fault 2 (`mesh2` carrier-edge-lost: `mesh2.admin.1` exited while `mesh2.rpc.1` is still
        // running): `unanswered_mesh_delete` deletes `mesh2.admin.1` and returns `CreateAdmin` for
        // `mesh2.admin.2`. Once `mesh2.admin.2` is created, it amends the roster, deletes `mesh2.rpc.1`,
        // records `MeshDeleted`, and is deleted last by the fabric-primary.
        let op_m2 = operation(&id2);
        let key_m2 = MeshDeleteKey {
            mesh_id: id2.clone(),
            build_id: build_id.clone(),
            attempt: 1,
            operation: op_m2.clone(),
        };
        let m2_nodes = vec![m2_a1.clone(), m2_r1.clone()];
        let delete_m2 = |b: RosterBirth| {
            let (pipeline, handles, build_id, m2_nodes) = (&pipeline, &handles, &build_id, &m2_nodes);
            async move {
                let record = m2_nodes
                    .iter()
                    .find(|n| n.node_id.to_string() == b.node_id)
                    .cloned()
                    .ok_or_else(|| format!("{} is not a mesh2 record", b.node))?;
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                        build_id: build_id.clone(),
                        attempt: 1,
                        node: record,
                        handle: handles[&b.node].clone(),
                        unreached: true,
                    })
                    .await
                    .map_err(|e| e.to_string())
            }
        };
        let outcome_m2 = unanswered_mesh_delete(
            &*builds,
            &fleet,
            &records,
            Some(&rows_fp),
            &mesh_storage,
            &*nodes_storage,
            &key_m2,
            "mesh1.admin.1",
            "mesh2",
            &m2_nodes,
            &held_paths,
            &delete_m2,
        )
        .await
        .expect("mesh2 with live rpc member resolves to CreateAdmin");
        let UnansweredMeshOutcome::CreateAdmin { path, predecessor } = outcome_m2 else {
            panic!("expected CreateAdmin for mesh2, got {outcome_m2:?}");
        };
        assert_eq!(path, "mesh2.admin.2".parse::<PathName>().unwrap());
        let m2_a2 = node_record("mesh2.admin.2", true, false);
        let h_a2 = handle("mesh2.admin.2", 54_099);
        let mut fleet = fleet;
        fleet.pids.insert(54_099, "mesh2.admin.2".into());
        let rt_a2 = RuntimeProof::of(&h_a2.fact().unwrap());
        let me_a2 = RosterBirth::of(&m2_a2).unwrap();
        observed.write().await.nodes.retain(|n| n.name != m2_a1.name);
        observed.write().await.nodes.push(m2_a2.clone());
        let rows_a2 = Arc::new(NodeRows::new(m2_a2.name.clone(), m2_a2.node_id.clone(), observed.clone(), nodes_storage.clone()));
        rows_written(&nodes_storage, &rows_a2, std::slice::from_ref(&m2_a2)).await;

        amend_roster(
            &*builds,
            &key_m2,
            "mesh1.admin.1",
            &RosterAmended {
                predecessor,
                final_primary: me_a2.clone(),
                final_runtime: rt_a2.clone(),
            },
        )
        .await
        .unwrap();
        let owner_facts = builds.facts().await.unwrap();
        let (leaving_m2, known_m2) = leaving_roster(&*builds, &*builds, &key_m2, "mesh2.admin.2", &me_a2, &rt_a2, || async { Ok(owner_facts) })
            .await
            .unwrap();
        let current_nodes = observed.read().await.nodes.clone();
        let records_for_leave = roster_records(&current_nodes, &leaving_m2, |_| None);
        roster_rows_read(Some(&rows_a2), "mesh2.admin.2", &op_m2, &leaving_m2, &records_for_leave).await;

        view_sink.writes_through(rows_a2.clone());
        let pipeline = DeploymentPipeline {
            provider: &fleet,
            joins: &joins,
            observer: &NoRpc,
            sink: &view_sink,
            lifecycle: &NoLifecycleEvents,
            builds: &*builds,
            template: &template,
            timeouts: Timeouts::default(),
        };
        let manifest_m2 = members_exited(&*builds, &key_m2, "mesh2.admin.2", &leaving_m2, &me_a2, rt_a2, &records_for_leave, &known_m2, |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let name = n.name.to_string();
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                        build_id: build_id.clone(),
                        attempt: 1,
                        node: n,
                        handle: handles[&name].clone(),
                        unreached: false,
                    })
                    .await
                    .map_err(|e| e.to_string())
            }
        })
        .await
        .unwrap();
        record_completion(&*builds, &key_m2, "mesh1.admin.1", &manifest_m2).await.unwrap();
        let term_a2 = pipeline
            .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                build_id: build_id.clone(),
                attempt: 1,
                node: m2_a2.clone(),
                handle: h_a2,
                unreached: false,
            })
            .await
            .unwrap();
        observed.write().await.nodes.iter_mut().filter(|n| n.name == m2_a2.name).for_each(|n| {
            n.status = NodeStatus::Leaving;
            n.stopped = true;
        });
        rows_fp.deleted(&m2_a2, "node.delete:mesh2.admin.2").await.unwrap();
        let left_m2 = mesh_left_proof(&manifest_m2, &term_a2).unwrap();
        mesh_left_rows(Some(&rows_fp), &mesh_storage, &Default::default(), &declared, "mesh1.admin.1", &key_m2, "mesh2", &left_m2.receipts)
            .await
            .unwrap();
        observed.write().await.nodes.retain(|n| n.name != m2_r1.name && n.name != m2_a2.name);
        observed.write().await.meshes.iter_mut().find(|m| m.name == "mesh2").unwrap().status = ScopeStatus::Dead;

        // Step 3: On the fabric-primary's own mesh (`mesh1`), `seat_move` is `None` (no
        // `node.fabric-primary.update`), `mesh1.rpc.1` is deleted, `MeshDeleted` is recorded, and
        // `complete_fabric_delete` releases the fabric name and records `Complete` of
        // `fabric.delete:<fabric_id>` while `mesh1.admin.1` stays alive (`Ready`). Replaying
        // `complete_fabric_delete` on resume is idempotent.
        let shutdown_m1 = BuildOperation::ShutdownMesh { mesh: "mesh1".into(), mesh_id: id1.clone() };
        assert_eq!(seat_move(&shutdown_m1, &*observed.read().await), None, "fabric seat stays on fabric-primary during its own mesh's delete");
        let op_m1 = operation(&id1);
        let key_m1 = MeshDeleteKey {
            mesh_id: id1.clone(),
            build_id: build_id.clone(),
            attempt: 1,
            operation: op_m1.clone(),
        };
        let me_m1 = RosterBirth::of(&fp).unwrap();
        let rt_m1 = RuntimeProof::of(&handles["mesh1.admin.1"].fact().unwrap());
        let m1_records = vec![fp.clone(), m1_r1.clone()];
        let roster_m1 = accepted_roster(&*builds, &key_m1, STEP_ACCEPTED_ROSTER, "mesh1.admin.1", &me_m1, &rt_m1, || live_roster(&m1_records))
            .await
            .unwrap();
        let leaving_m1 = accepted_roster(&*builds, &key_m1, STEP_LEAVING_ROSTER, "mesh1.admin.1", &me_m1, &rt_m1, || live_roster(&m1_records))
            .await
            .unwrap();
        view_sink.writes_through(rows_fp.clone());
        members_exited(&*builds, &key_m1, "mesh1.admin.1", &leaving_m1, &me_m1, rt_m1.clone(), &m1_records, &[], |n: Node| {
            let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
            async move {
                let name = n.name.to_string();
                pipeline
                    .delete_node(&rdm_admin::deployment::pipeline::DeleteRequest {
                        build_id: build_id.clone(),
                        attempt: 1,
                        node: n,
                        handle: handles[&name].clone(),
                        unreached: false,
                    })
                    .await
                    .map_err(|e| e.to_string())
            }
        })
        .await
        .unwrap();
        let facts = builds.facts().await.unwrap();
        let admitted_m1 = rdm_admin::mesh_delete::Admitted {
            roster: roster_m1,
            final_primary: me_m1,
            final_runtime: rt_m1,
            amendments: Vec::new(),
        };
        let HandoffVerdict::Accept(manifest_m1) = handoff_verdict(&facts, &key_m1, &admitted_m1, &manifest_reference(build_id.as_str(), 1, &op_m1, false)) else {
            panic!("mesh1 handoff accepted");
        };
        record_completion(&*builds, &key_m1, "mesh1.admin.1", &manifest_m1).await.unwrap();
        assert_eq!(owner_resume(&builds.read_build(&build_id).await.unwrap().steps, &op_m1), Ok(OwnerResume::Deleted(manifest_m1.clone())));

        // Once a runtime's deletion receipt is durable, nothing recovers it for being absent (`fabric-delete.md:73`).
        let already_deleted = cp
            .open_recovery("test", m1_r1.name.clone(), m1_r1.node_id.clone(), m1_r1_inc)
            .await
            .expect("durable deletion receipt answers AlreadyApplied");
        assert!(matches!(already_deleted, rdm_admin::control_plane::Recovery::AlreadyApplied(_)));

        complete_fabric_delete(&*builds, &*fabric_storage, &build_id, 1, &fabric_id, "mesh1.admin.1")
            .await
            .expect("complete_fabric_delete releases the fabric name and records fabric.deleted");
        complete_fabric_delete(&*builds, &*fabric_storage, &build_id, 2, &fabric_id, "mesh1.admin.1")
            .await
            .expect("replayed complete_fabric_delete on resume is idempotent");
        assert_eq!(fabric_storage.fabric().await.unwrap(), None, "fabric name is released");
        assert_eq!(
            nodes_storage.record_at(&fp.name).await.unwrap().unwrap().status,
            NodeStatus::Ready,
            "fabric-primary remains alive after fabric.deleted"
        );
        trace_id
    });

    let spans = cap.spans();
    let _ = std::fs::remove_dir_all(&data_root);
    let named = |name: &str| {
        spans
            .iter()
            .filter(|s| s["name"] == name && s["trace_id"] == trace_id.as_str())
            .cloned()
            .collect::<Vec<_>>()
    };
    assert!(!named("rdm.node_admin.mesh.update.via-mesh-deleted-receipt").is_empty());
    assert!(!named("rdm.node_admin.mesh.update.via-dead-row").is_empty());
    assert!(!named("rdm.node_admin.build.reject.via-fabric-delete-in-force").is_empty());
    assert!(!named("rdm.node_admin.build.reject.via-recovery-already-applied").is_empty());
    let completed = named("rdm.node_admin.fabric.delete.via-build");
    assert!(!completed.is_empty(), "expected rdm.node_admin.fabric.delete.via-build span");
    assert_eq!(completed[0]["attributes"]["outcome"], "fabric-deleted");
}
