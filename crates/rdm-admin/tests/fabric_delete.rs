//! `fabric.delete` replaces `FabricShutdown` (i143.e12.s59; lifecycles/fabric-delete.md step 1,
//! R-FS1 revised).
//!
//! 1. `crit:e12.s59.1`: `fabric.delete` of a two-mesh fabric runs `mesh.delete` on each mesh
//!    leaf-inward (the fabric-primary's own mesh last, where the fabric-primary role stays put), deletes
//!    each other mesh's final mesh-primary (`MeshLeft` + mesh row `Dead`), releases the fabric name
//!    (`FabricStorage::release_name`) and reports `fabric.deleted` (`Complete` of
//!    `fabric.delete:<fabric_id>`) while the fabric-primary remains alive for its separate
//!    `node.delete`. After that separate `node.delete`, every node row is `Deleted`, every mesh row
//!    is `Dead` in both `status.storage` and `mesh.storage`, and no fabric identity row remains held
//!    in `fabric.storage`.

use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::node_rows::RecordAt as _;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId};
use rdm_admin::accepted::{compile, plan_for_build, AcceptedStore, FabricTopology};
use rdm_core::build::BuildChange;
use rdm_admin::admin::Records;
use rdm_admin::build::{BuildId, BuildOperation};
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, StepOutcome};
use rdm_admin::deployment::pipeline::{DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, RuntimeIdentity, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::executor::{executor_for, role_move};
use rdm_admin::fabric_storage::{FabricIdentity, MemoryFabricStorage};
use rdm_admin::join::Joins;
use rdm_admin::mesh_delete::{accepted_roster, complete_fabric_delete, first_mismatch, handoff_verdict, leaving_roster, manifest_of, manifest_reference, members_exited, mesh_left_rows, operation, owner_resume, record_completion, roster_records, roster_rows_read, stored_roster, ExitManifest, HandoffVerdict, MeshDeleteKey, OwnerResume, RosterBirth, STEP_ACCEPTED_ROSTER, STEP_LEAVING_ROSTER, STEP_MESH_LEFT};
use rdm_admin::model::{DeploymentId, Fabric, Mesh, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rdm_admin::node_rows::{RowFor, RowCreated, NodeRows};
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
    pids: BTreeMap<u32, NodeId>,
    running_pids: Mutex<BTreeSet<u32>>,
    terminated: Mutex<Vec<NodeId>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for Fleet {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        DOMAIN.into()
    }
    async fn spawn(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        unreachable!("a mesh or fabric delete launches nothing here")
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        let pid = h.pid.unwrap();
        self.running_pids.lock().unwrap().remove(&pid);
        self.terminated.lock().unwrap().push(self.pids[&pid].clone());
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        let pid = h.pid.unwrap();
        if self.running_pids.lock().unwrap().contains(&pid) {
            DeploymentStatus::Running
        } else {
            DeploymentStatus::Exited { code: Some(0) }
        }
    }
    async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
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
struct View(Mutex<BTreeMap<NodeId, Node>>, Mutex<std::collections::HashSet<(NodeId, IncarnationId)>>, Mutex<Option<Arc<NodeRows>>>);
impl TopologySink for View {
    fn publish(&self, n: Node) {
        self.0.lock().unwrap().insert(n.node_id.clone(), n);
    }
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, name: &PathName, node_id: &NodeId, incarnation: &IncarnationId) {
        let mut view = self.0.lock().unwrap();
        if view.get(node_id).is_some_and(|n| n.node_id == *node_id && n.incarnation_id.as_ref() == Some(incarnation)) {
            view.remove(node_id);
        }
        self.1.lock().unwrap().insert((node_id.clone(), incarnation.clone()));
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

fn node_record(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn handle(pid: u32) -> DeploymentHandle {
    DeploymentHandle {
        deployment_id: DeploymentId::mint(),
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

/// The MeshLeft proof set of `key` over `roster` with `final_primary`, from the step events of the
/// Builds `of`: every birth, the final primary's included, proven exited by its own node.delete's
/// `TerminateRuntime` (mesh-delete.md, mesh-left). An incomplete proof set fails the cell by name.
async fn left_manifest(builds: &dyn BuildStateAdapter, of: &[&BuildId], key: &MeshDeleteKey, roster: &[RosterBirth], final_primary: &RosterBirth) -> ExitManifest {
    let mut steps = Vec::new();
    for b in of {
        steps.extend(builds.read_build(b).await.unwrap().steps);
    }
    let all = manifest_of(key, roster, final_primary, None, &steps, true);
    assert_eq!(first_mismatch(&all, roster, final_primary, true), None, "every birth of the mesh is proven exited: {all:?}");
    all
}

/// The roster of `mesh` as the node records of `storage` hold it at the step, over the step events of
/// `build_id` (D67: the topology of the moment is the records).
async fn records_roster(storage: &MemoryNodesStorage, builds: &dyn BuildStateAdapter, build_id: &BuildId, mesh: &str, mesh_id: &MeshId) -> Vec<RosterBirth> {
    let steps = builds.read_build(build_id).await.unwrap().steps;
    stored_roster(&storage.nodes(mesh).await.unwrap(), mesh, mesh_id, &steps)
}

async fn rows_written(storage: &MemoryNodesStorage, rows: &NodeRows, records: &[Node]) {
    for n in records {
        // The record a create wrote: the node's row and its birth.
        let mut row = NodeRow { r#type: n.r#type.clone(), mesh: n.mesh.clone(), initial_properties: None, topology_storage_provider: None, writer: None,
                path: n.path.clone(), name: n.name.clone(),
                node_id: n.node_id.clone(),
                status: NodeStatus::Ready,
                allocated: None,
                renamed_from: None,
                    executable: None,
                births: Vec::new(),
                predecessor: None,
                mesh_id: None,
                storage_persistence: None,
               
            };
        if let Some(incarnation) = &n.incarnation_id {
            row.birth_mut(incarnation, &n.location());
        }
        storage.write_record(&row).await.unwrap();
        assert!(matches!(rows.create_row(&n.location(), &n.mesh.clone(), &n.r#type, &n.node_id, &rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&n.location())), None, None, RowFor::Create).await, Ok(RowCreated::HeldByNode(_))), "the writer holds {:?}'s row", n.name);
        let h = handle(50_000);
        storage
            .put_runtime(&RuntimeRow { r#type: n.r#type.clone(), mesh: n.mesh.clone(),
                node_id: n.node_id.clone(),
                path: n.path.clone(), name: n.name.clone(),
                incarnation_id: n.incarnation_id.clone().unwrap(),
                runtime: h.fact().unwrap(),
                data_dir: None,
            })
            .await
            .unwrap();
    }
}

/// crit:e12.s59.1 — `fabric.delete` of a two-mesh fabric runs `mesh.delete` leaf-inward (`mesh2`
/// first, then the fabric-primary's own mesh `mesh1` last with the fabric-primary role kept on
/// `mesh1.node-admin.1`), deletes `mesh2`'s final mesh-primary (`mesh2.node-admin.1`), and reports
/// `fabric.deleted` (`Complete` of `fabric.delete:<fabric_id>`) while `mesh1.node-admin.1` remains alive for
/// its separate `node.delete`. Once that separate `node.delete` runs, no live node or mesh row remains.
#[tokio::test]
async fn fabric_delete_leaves_no_node_mesh_or_fabric_row_and_fabric_primary_awaits_its_own_node_delete() {
    let data_root = std::env::temp_dir().join(format!("fabric-delete-{}", NodeId::mint()));
    let fabric_id = FabricId::mint();
    let (id1, id2) = (MeshId::mint(), MeshId::mint());
    let m1_a1 = node_record(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true);
    let m1_r1 = node_record(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", true, false);
    let m2_a1 = node_record(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false);
    let m2_r1 = node_record(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", true, false);
    let all_nodes = vec![m1_a1.clone(), m1_r1.clone(), m2_a1.clone(), m2_r1.clone()];
    let handles: BTreeMap<NodeId, DeploymentHandle> = all_nodes.iter().enumerate().map(|(i, n)| (n.node_id.clone(), handle(51_000 + i as u32))).collect();
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
    rdm_admin::fabric_storage::write_identity(&*fabric_storage, &identity).await.unwrap().unwrap();
    let mesh_storage = MemoryMeshStorage::default();
    for (id, name) in [(&id1, "mesh1"), (&id2, "mesh2")] {
        rdm_admin::storage::write_mesh(&mesh_storage, None, &rdm_admin::storage::MeshRecord { mesh_id: id.clone(), name: name.into(), state: MeshState::Pending }).await.unwrap().unwrap();
    }
    let declared = Mutex::new(Declared::default());
    let nodes_storage = Arc::new(MemoryNodesStorage::default());
    let rows_m1 = Arc::new(NodeRows::new(m1_a1.location(), m1_a1.node_id.clone(), observed.clone(), nodes_storage.clone()));
    let rows_m2 = Arc::new(NodeRows::new(m2_a1.location(), m2_a1.node_id.clone(), observed.clone(), nodes_storage.clone()));
    rows_written(&nodes_storage, &rows_m1, &[m1_a1.clone(), m1_r1.clone()]).await;
    rows_written(&nodes_storage, &rows_m2, &[m2_a1.clone(), m2_r1.clone()]).await;

    // Compile BuildChange::DeleteFabric and accept the Build.
    let deleted_topology = compile(&initial, &BuildChange::DeleteFabric, &*observed.read().await).expect("BuildChange::DeleteFabric compiles to an empty/deleted topology");
    assert!(deleted_topology.meshes.is_empty(), "a deleted fabric topology names no meshes: {deleted_topology:?}");

    let builds = Arc::new(crate::stepping(&rdm_core::NodeId::mint(), "mesh1"));
    let accepted = AcceptedStore::seeded(&*builds, fabric_id.clone(), initial.clone(), &m1_a1.birth().unwrap()).await.unwrap();
    let mut cp = crate::control_plane(builds.clone(), accepted, m1_a1.location(), observed.read().await.clone(), crate::adopted_time());
    cp.topology = observed.clone();
    // The fabric-primary's `BuildService` reads the node records its writers wrote (boot.rs: one nodes store),
    // never a birth from the gossip view (Q197).
    crate::bind_store(&cp, nodes_storage.clone());
    let opened = cp.submit("POST /api/build", BuildChange::DeleteFabric).await.expect("DeleteFabric is accepted as a Build");
    let build_id = opened.build_id;

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
        executable: "/nonexistent/testkit-node".into(),
        seeds: vec![("fpkey".into(), "127.0.0.1:34000".parse().unwrap())],
        launcher: rdm_core::launch::Launcher { mesh: m1_a1.mesh.clone(), path: m1_a1.path.clone(), name: m1_a1.name.clone(), node_id: m1_a1.node_id.clone(), incarnation: m1_a1.incarnation_id.clone().unwrap() },
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

    // Execute ShutdownMesh(mesh2): mesh2.node-admin.1 deletes mesh2.node.1 and hands off MeshDeleted.
    let op_m2 = operation(&id2);
    let key_m2 = MeshDeleteKey { mesh_id: id2.clone(), build_id: build_id.clone(), operation: op_m2.clone() };
    let me_m2 = RosterBirth::of(&m2_a1).unwrap();
    let rt_m2 = RuntimeIdentity::of(&handles[&m2_a1.node_id].fact().unwrap());
    let m2_records = vec![m2_a1.clone(), m2_r1.clone()];
    let roster_m2 = accepted_roster(&*builds, &key_m2, STEP_ACCEPTED_ROSTER, m1_a1.node_id.as_str(), || live_roster(&m2_records)).await.unwrap();
    let leaving_m2 = accepted_roster(&*builds, &key_m2, STEP_LEAVING_ROSTER, m2_a1.node_id.as_str(), || live_roster(&m2_records)).await.unwrap();
    // mesh2.node-admin.1 executes its members' node.deletes: their rows Deleted are node.delete's own.
    view_sink.writes_through(rows_m2.clone());
    members_exited(&*builds, &key_m2, m2_a1.node_id.as_str(), &leaving_m2, &me_m2, rt_m2.clone(), &m2_records, |n: Node| {
        let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
        async move {
            let name = n.node_id.clone();
            pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: Some(build_id.clone()), node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
        }
    })
    .await
    .unwrap();
    let facts = builds.read_build(&build_id).await.unwrap().steps;
    let admitted_m2 = rdm_admin::mesh_delete::Admitted { roster: roster_m2, final_primary: me_m2.clone(), final_runtime: rt_m2 };
    let HandoffVerdict::Accept(manifest_m2) = handoff_verdict(&facts, &key_m2, &admitted_m2, &manifest_reference(build_id.as_str(), &op_m2, false)) else { panic!("mesh2 handoff accepted") };
    record_completion(&*builds, &key_m2, m1_a1.node_id.as_str(), &manifest_m2).await.unwrap();
    observed.write().await.nodes.retain(|n| n.location() != m2_r1.location());
    observed.write().await.meshes.iter_mut().find(|m| m.name == "mesh2").unwrap().status = ScopeStatus::Dead;

    // Step 2: after mesh2's MeshDeleted, re-planning the SAME Build plans DeleteLiveNode(mesh2.node-admin.1)
    // before ShutdownMesh(mesh1) and DeleteFabric!
    let proj = builds.read_build(&build_id).await.unwrap();
    let after_m2_deleted = plan_for_build(&proj, &*observed.read().await).operations;
    assert_eq!(
        after_m2_deleted,
        vec![
            BuildOperation::DeleteLiveNode { r#type: m2_a1.r#type.clone(), mesh: m2_a1.mesh.clone(), path: m2_a1.path.clone(), name: m2_a1.name.clone(), exited: None, birth: rdm_admin::build::PinnedBirth::of(&m2_a1) },
            BuildOperation::ShutdownMesh { mesh: "mesh1".into(), mesh_id: id1.clone() },
            BuildOperation::DeleteFabric { fabric_id: fabric_id.clone() },
        ],
        "after mesh2's MeshDeleted, fabric.delete plans DeleteLiveNode(mesh2.node-admin.1) on the same Build"
    );
    assert_eq!(executor_for(&after_m2_deleted[0], &*observed.read().await), Some(m1_a1.node_id.clone()), "fabric-primary executes DeleteLiveNode(mesh2.node-admin.1)");

    // Execute DeleteLiveNode(mesh2.node-admin.1) as fabric-primary: mesh2.node-admin.1 stops, its row is written
    // Deleted by FP as acting primary, and MeshLeft + mesh_left_rows write mesh2 Dead.
    pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(m2_a1).node_id), build_id: Some(build_id.clone()), node: m2_a1.clone(), handle: handles[&m2_a1.node_id].clone(), unreached: false }).await.unwrap();
    observed.write().await.nodes.iter_mut().filter(|n| n.location() == m2_a1.location()).for_each(|n| {
        n.status = NodeStatus::Leaving;
        n.stopped = true;
    });
    rows_m1.deleted(&m2_a1, &rdm_core::OperationId::node("node.delete", &m2_a1.node_id)).await.unwrap();
    observed.write().await.nodes.retain(|n| n.location() != m2_a1.location());
    let left_m2 = left_manifest(&*builds, &[&build_id], &key_m2, &manifest_m2.roster, &me_m2).await;
    mesh_left_rows(Some(&rows_m1), &mesh_storage, &Default::default(), &declared, m1_a1.node_id.as_str(), &key_m2, "mesh2", &left_m2.receipts).await.unwrap();
    builds
        .write_step(&rdm_admin::build_state::StepWrite { build_id: build_id.clone(), mesh: None, operation: op_m2.clone(), step: STEP_MESH_LEFT.into(), outcome: StepOutcome::Complete })
        .await
        .unwrap();

    // Step 1b: now only mesh1 (the FP's own mesh) and DeleteFabric remain. On mesh1, role_move is
    // None (the fabric-primary role stays on mesh1.node-admin.1) and executor_for is mesh1.node-admin.1.
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
    assert_eq!(role_move(&after_m2_left[0], &*observed.read().await), None, "the fabric-primary role stays on mesh1.node-admin.1 during its own mesh's mesh.delete");
    assert_eq!(executor_for(&after_m2_left[0], &*observed.read().await), Some(m1_a1.node_id.clone()));

    // Execute ShutdownMesh(mesh1): mesh1.node-admin.1 deletes mesh1.node.1, commits mesh1 Dead in
    // status.storage and mesh.storage, and records MeshDeleted while staying alive itself.
    let op_m1 = operation(&id1);
    let key_m1 = MeshDeleteKey { mesh_id: id1.clone(), build_id: build_id.clone(), operation: op_m1.clone() };
    let me_m1 = RosterBirth::of(&m1_a1).unwrap();
    let rt_m1 = RuntimeIdentity::of(&handles[&m1_a1.node_id].fact().unwrap());
    let m1_records = vec![m1_a1.clone(), m1_r1.clone()];
    let roster_m1 = accepted_roster(&*builds, &key_m1, STEP_ACCEPTED_ROSTER, m1_a1.node_id.as_str(), || live_roster(&m1_records)).await.unwrap();
    let leaving_m1 = accepted_roster(&*builds, &key_m1, STEP_LEAVING_ROSTER, m1_a1.node_id.as_str(), || live_roster(&m1_records)).await.unwrap();
    view_sink.writes_through(rows_m1.clone());
    members_exited(&*builds, &key_m1, m1_a1.node_id.as_str(), &leaving_m1, &me_m1, rt_m1.clone(), &m1_records, |n: Node| {
        let (pipeline, handles, build_id) = (&pipeline, &handles, &build_id);
        async move {
            let name = n.node_id.clone();
            pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(n).node_id), build_id: Some(build_id.clone()), node: n, handle: handles[&name].clone(), unreached: false }).await.map_err(|e| e.to_string())
        }
    })
    .await
    .unwrap();
    let facts = builds.read_build(&build_id).await.unwrap().steps;
    let admitted_m1 = rdm_admin::mesh_delete::Admitted { roster: roster_m1, final_primary: me_m1.clone(), final_runtime: rt_m1 };
    let HandoffVerdict::Accept(manifest_m1) = handoff_verdict(&facts, &key_m1, &admitted_m1, &manifest_reference(build_id.as_str(), &op_m1, false)) else { panic!("mesh1 handoff accepted") };
    { let mut r = mesh_storage.mesh(&id1.clone()).await.unwrap().unwrap_or_else(|| rdm_admin::storage::MeshRecord::new(id1.clone(), "mesh1")); r.state = MeshState::Deleted; mesh_storage.put_mesh(&r).await.unwrap(); }
    rdm_admin::storage::write_mesh(&mesh_storage, None, &rdm_admin::storage::MeshRecord { mesh_id: id1.clone(), name: "mesh1".into(), state: MeshState::Deleted }).await.unwrap().unwrap();
    record_completion(&*builds, &key_m1, m1_a1.node_id.as_str(), &manifest_m1).await.unwrap();
    observed.write().await.nodes.retain(|n| n.location() != m1_r1.location());
    observed.write().await.meshes.iter_mut().find(|m| m.name == "mesh1").unwrap().status = ScopeStatus::Dead;

    // Step 3: after mesh1's MeshDeleted, re-planning plans ONLY DeleteFabric (mesh1.node-admin.1 is NOT
    // deleted inside fabric.delete!).
    let proj = builds.read_build(&build_id).await.unwrap();
    let before_complete = plan_for_build(&proj, &*observed.read().await).operations;
    assert_eq!(
        before_complete,
        vec![BuildOperation::DeleteFabric { fabric_id: fabric_id.clone() }],
        "the fabric-primary is NOT deleted inside fabric.delete; only DeleteFabric remains"
    );
    complete_fabric_delete(&*builds, &build_id, &fabric_id, m1_a1.node_id.as_str()).await.expect("complete_fabric_delete records fabric.deleted");
    complete_fabric_delete(&*builds, &build_id, &fabric_id, m1_a1.node_id.as_str()).await.expect("replayed complete_fabric_delete on resume is idempotent");
    // The fabric-primary's drive ends the Build: its `rdm.build.ended` `complete` (build.md §5).
    crate::end_build(&*builds, &build_id, &m1_a1.birth().unwrap(), rdm_admin::events_storage::EndedState::Complete).await;

    // At fabric.deleted:
    // - No operations remain on the fabric.delete Build.
    // - The fabric-primary `mesh1.node-admin.1` is STILL ALIVE (`Ready` row in nodes.storage,
    //   runtime not terminated).
    let proj = builds.read_build(&build_id).await.unwrap();
    assert!(plan_for_build(&proj, &*observed.read().await).operations.is_empty(), "fabric.delete Build has no operations left at fabric.deleted");
    assert!(!fleet.terminated.lock().unwrap().contains(&m1_a1.node_id), "fabric-primary is still alive at fabric.deleted");
    assert_eq!(nodes_storage.record_at(&m1_a1.location()).await.unwrap().unwrap().status, NodeStatus::Ready, "fabric-primary's node row is still live at fabric.deleted");

    // Step 4: a separate call from outside the fabric runs `node.delete` of the fabric-primary.
    // It is a Delete of the fabric-primary's node.id, its own Build (build.md §1), composed from the
    // records: the fabric's last node, of a deleted mesh.
    let fp_delete = cp
        .submit_in(tracing::info_span!("cell.fp-delete"), "build.create", BuildChange::Delete { meshes: Vec::new(), node_ids: vec![m1_a1.node_id.clone()] })
        .await
        .expect("the separate node.delete of the fabric-primary is admitted");
    assert_ne!(fp_delete.build_id, build_id, "the fabric-primary's node.delete is a Build of its own");
    let proj2 = builds.read_build(&fp_delete.build_id).await.unwrap();
    assert!(proj2.topology.meshes.is_empty(), "no mesh is left: {:?}", proj2.topology.meshes.keys().collect::<Vec<_>>());
    let fp_ops = plan_for_build(&proj2, &*observed.read().await).operations;
    assert_eq!(fp_ops, vec![BuildOperation::DeleteLiveNode { r#type: m1_a1.r#type.clone(), mesh: m1_a1.mesh.clone(), path: m1_a1.path.clone(), name: m1_a1.name.clone(), exited: None, birth: rdm_admin::build::PinnedBirth::of(&m1_a1) }], "the Build plans only the fabric-primary's own node.delete");
    assert_eq!(role_move(&fp_ops[0], &*observed.read().await), None, "the last node of the last mesh moves no role on its separate node.delete");
    assert_eq!(executor_for(&fp_ops[0], &*observed.read().await), Some(m1_a1.node_id.clone()), "the fabric-primary executes its own final node.delete");

    pipeline.delete_node(&rdm_admin::deployment::pipeline::DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(m1_a1).node_id), build_id: Some(fp_delete.build_id.clone()), node: m1_a1.clone(), handle: handles[&m1_a1.node_id].clone(), unreached: false }).await.unwrap();
    observed.write().await.nodes.iter_mut().filter(|n| n.location() == m1_a1.location()).for_each(|n| {
        n.status = NodeStatus::Leaving;
        n.stopped = true;
    });
    rows_m1.deleted(&m1_a1, &rdm_core::OperationId::node("node.delete", &m1_a1.node_id)).await.unwrap();
    let left_m1 = left_manifest(&*builds, &[&build_id, &fp_delete.build_id], &key_m1, &manifest_m1.roster, &me_m1).await;
    mesh_left_rows(Some(&rows_m1), &mesh_storage, &Default::default(), &declared, m1_a1.node_id.as_str(), &key_m1, "mesh1", &left_m1.receipts).await.unwrap();

    // Every node row is Deleted, and both mesh rows are deleted in status.storage and mesh.storage.
    for n in &all_nodes {
        let row = nodes_storage.node(&n.node_id).await.unwrap().expect("node row exists");
        assert_eq!(row.status, NodeStatus::Deleted, "{:?}'s node row is Deleted", n.name);
    }
    for (id, name) in [(&id1, "mesh1"), (&id2, "mesh2")] {
        assert!(mesh_storage.mesh(&id.clone()).await.unwrap().is_some_and(|r| r.state == MeshState::Deleted), "{name} status row is deleted");
    }
    let mesh_rows = mesh_storage.meshes().await.unwrap();
    assert!(mesh_rows.iter().all(|m| m.is_deleted()), "every mesh row in mesh.storage is deleted: {mesh_rows:?}");

    let _ = std::fs::remove_dir_all(&data_root);
}
