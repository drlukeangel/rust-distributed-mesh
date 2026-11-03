//! A mesh's new mesh-primary picks up the mesh's section of a resubmitted Build from the Build's events
//! (build.md §5: "A mesh's new mesh-primary runs the steps its mesh owes from those events").
//!
//! The fabric-primary `mesh1.node-admin.1` drives a Build that grows mesh2 by two nodes. It sends
//! mesh2's section to mesh2's primary `mesh2.node-admin.1` (A), which completes one create, writes its
//! `rdm.build.step` events, and is lost in the middle of the other: the section's stream breaks, the
//! section ends `UNAVAILABLE`, and the Build ends `incomplete` (Q344, Q346). mesh2's other admin
//! `mesh2.node-admin.2` (B) becomes its primary. The caller resubmits the Build by its id: the
//! fabric-primary sends mesh2's section to B, which plans it from the Build's record and its step
//! events (Q292), runs only what is not complete, and the Build ends `complete`.

use rdm_admin::build::{BuildId, BuildOperation, MeshDesired};
use rdm_admin::build_drive::{Dispatched, Dispatcher, Drive, DriveEnd, DriveEnv, InnerItem, InnerStream};
use rdm_admin::build_state::{BuildState, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::executor::OperationRunner;
use rdm_admin::model::*;
use rdm_admin::topology::Topology;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, RwLock};

fn node(name: rdm_core::PathName, mesh: &str, r#type: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name, mesh, r#type);
    n.status = NodeStatus::Ready;
    n.is_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// The observed fabric: mesh1 with the fabric-primary, mesh2 with its primary A, its other admin B and
/// one node.
fn observed() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }, Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready }],
        nodes: vec![
            Node { is_fabric_primary: true, ..node(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true) },
            node(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true),
            node(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", false),
            node(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", false),
        ],
    }
}

/// Realises a create by adding the node to the observed topology. Armed with `die_at`, the admin that
/// runs that create is lost there: it never returns.
struct Runner {
    topology: Arc<RwLock<Topology>>,
    ran: Mutex<Vec<rdm_core::OperationId>>,
    die_at: Mutex<Option<PathName>>,
}

#[async_trait::async_trait]
impl OperationRunner for Runner {
    async fn run(&self, _: &BuildId, op: &BuildOperation) -> Result<(), rdm_admin::executor::RunEnd> {
        self.ran.lock().unwrap().push(op.key());
        let BuildOperation::CreateNode { path, name, .. } = op else { return Err(format!("unexpected {op:?}").into()) };
        let at = PathName::new(path, name).unwrap();
        if self.die_at.lock().unwrap().as_ref() == Some(&at) {
            std::future::pending::<()>().await;
        }
        self.topology.write().await.nodes.push(node(at, op.mesh().unwrap(), &op.r#type().unwrap(), false));
        Ok(())
    }
}

/// The fixture's dispatcher, the section stream of `lost` broken once the cell loses that admin: what
/// the fabric-primary reads of a mesh-primary lost mid-section.
struct Losing {
    inner: Arc<crate::loopback::Loopback>,
    lost: rdm_core::NodeId,
    gone: Arc<Notify>,
}

/// A section's stream that breaks when its admin is lost.
struct Breaks {
    inner: Box<dyn InnerStream>,
    gone: Arc<Notify>,
}

#[async_trait::async_trait]
impl InnerStream for Breaks {
    async fn next(&mut self) -> InnerItem {
        tokio::select! {
            _ = self.gone.notified() => InnerItem::Broke("mesh2.node-admin.1 was lost mid-section: its connection closed".into()),
            item = self.inner.next() => item,
        }
    }
    async fn accepted(&mut self) {
        self.inner.accepted().await
    }
}

#[async_trait::async_trait]
impl Dispatcher for Losing {
    async fn dispatch(&self, build_id: &BuildId, mesh: &str, context: rdm_core::context::CallContext) -> Dispatched {
        match self.inner.dispatch(build_id, mesh, context).await {
            Dispatched::Stream(s) if self.inner.sections().last().is_some_and(|(to, _)| to.node_id == self.lost) => Dispatched::Stream(Box::new(Breaks { inner: s, gone: self.gone.clone() })),
            other => other,
        }
    }
    async fn run_own(&self, turn: &rdm_admin::stop_gate::Turn, build_id: &BuildId, mesh: &str, context: rdm_core::context::CallContext, operations: Vec<BuildOperation>) -> Dispatched {
        self.inner.run_own(turn, build_id, mesh, context, operations).await
    }
}

/// The drive's gate: every check at its default.
struct Open;

#[async_trait::async_trait]
impl rdm_admin::build_drive::Gate for Open {}

/// CONTRACT (build.md §5: the mesh-primary writes its mesh's `rdm.build.step` events, and "A mesh's new
/// mesh-primary runs the steps its mesh owes from those events"; Q344: a call that ends short ends its
/// section; Q346: the Build ends `incomplete`, what ran stands, and a resubmit runs it again; build.md
/// §1: "a step whose event ended `complete` is never run again"): mesh2's primary A completes one create
/// and is lost in the other; the Build ends `incomplete` naming mesh2's section. B becomes mesh2's
/// primary, and the caller's resubmit sends mesh2's section to B, which runs what its mesh owes and never
/// the create A completed. The Build ends `complete`, one `rdm.build.ended` per run.
/// Must NOT happen: the completed create run again, the resubmit's section sent to the lost A, or the
/// Build left in progress.
#[test]
fn a_resubmitted_incomplete_build_is_run_by_the_meshs_new_primary_from_the_step_events() {
    use rdm_admin::events_storage::EndedState;
    let cap = crate::mesh_delete_resume::capture();
    let (trace_id, first, again, ran, sections, created, a, b) = cap.run(async {
        let root = tracing::info_span!("cell.resubmit-at-the-new-mesh-primary");
        let trace_id = crate::mesh_delete_resume::trace_id(&root);
        let _in_root = root.enter();
        let t = observed();
        let fabric_id = t.fabric.id.clone();
        let birth = |p: &str, n: &str| t.node(&PathName::new(p, n).unwrap()).unwrap().birth().unwrap();
        let (fp, a, b) = (birth("mesh1.node-admin", "1"), birth("mesh2.node-admin", "1"), birth("mesh2.node-admin", "2"));
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let accepted = rdm_admin::accepted::AcceptedStore::seeded(&*builds, fabric_id.clone(), rdm_admin::accepted::FabricTopology::of_observed(&t), &fp).await.unwrap();
        let topology = Arc::new(RwLock::new(t.clone()));
        let mut cp = crate::control_plane(builds.clone(), accepted, PathName::new("mesh1.node-admin", "1").unwrap(), t, crate::adopted_time());
        cp.topology = topology.clone();
        let change = crate::named_request(&cp, |t: &Topology| crate::grown(t, &[MeshDesired::of("mesh2", [("node-admin", 2), ("node", 3)])])).await;
        let build = cp.submit("test", change).await.unwrap().build_id;

        let runner = Arc::new(Runner { topology: topology.clone(), ran: Mutex::new(Vec::new()), die_at: Mutex::new(Some(PathName::new("mesh2.node", "3").unwrap())) });
        let loopback = crate::loopback::Loopback::new(builds.clone(), builds.clone(), topology.clone(), runner.clone());
        let gone = Arc::new(Notify::new());
        let env = Arc::new(DriveEnv {
            me_mesh: "mesh1".into(),
            me_path: "mesh1.node-admin".into(),
            me_name: "1".into(),
            birth: fp.clone(),
            topology: topology.clone(),
            builds: builds.clone(),
            dispatcher: Arc::new(Losing { inner: loopback.clone(), lost: a.node_id.clone(), gone: gone.clone() }),
            gate: Arc::new(Open),
            notices: crate::written_by(Default::default(), &*builds, &fabric_id, &fp),
            turns: rdm_admin::stop_gate::StopGate::new(),
        });
        let (node2, node3) = (created_id(&builds, &build, "2").await, created_id(&builds, &build, "3").await);
        // The first run: A completes mesh2.node.2 and is lost in mesh2.node.3.
        let drive = Drive::detached(build.clone());
        let running = tokio::spawn({
            let (env, drive) = (env.clone(), drive.clone());
            async move { env.run(&drive).await }
        });
        let op2 = rdm_core::OperationId::node("node.create", &node2);
        for _ in 0..400 {
            if builds.read_build(&build).await.unwrap().operation_completed(&op2) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(builds.read_build(&build).await.unwrap().operation_completed(&op2), "A wrote mesh2.node.2's complete step");
        // A is lost: its process and its run go, and its section's stream breaks.
        loopback.door(&a).runs.stop().await;
        gone.notify_one();
        let end = tokio::time::timeout(Duration::from_secs(10), running).await.expect("the first run ends once A is lost").unwrap();
        let first = (end, builds.read_build(&build).await.unwrap());
        // B is mesh2's primary now; A is silent.
        for n in topology.write().await.nodes.iter_mut() {
            if n.node_id == a.node_id {
                n.status = NodeStatus::PendingReconnect;
                n.is_primary = false;
            }
            if n.node_id == b.node_id {
                n.is_primary = true;
            }
        }
        *runner.die_at.lock().unwrap() = None;
        // The caller's resubmit, by the Build's id.
        let resubmit = Drive::detached(build.clone());
        let end = tokio::time::timeout(Duration::from_secs(10), env.run(&resubmit)).await.expect("the resubmitted run ends");
        let again = (end, builds.read_build(&build).await.unwrap(), crate::build_drive::ended_states(&*builds, &build).await);
        let ran = runner.ran.lock().unwrap().clone();
        (trace_id, first, again, ran, loopback.sections(), (node2, node3), a, b)
    });
    println!("trace_id={trace_id}");
    let (end, p) = first;
    assert_eq!(end, DriveEnd::Terminal, "the lost section ends the first run");
    assert_eq!(p.state, BuildState::Incomplete, "the Build ends incomplete: {p:?}");
    assert!(matches!(p.unfinished.as_slice(), [u] if u.mesh == "mesh2" && u.code == rdm_core::outcome::Code::Unavailable), "the Build names mesh2's section: {:?}", p.unfinished);
    let (end, p, ended) = again;
    assert_eq!((end, p.state), (DriveEnd::Terminal, BuildState::Complete), "the resubmit completes the Build");
    assert!(matches!(ended.as_slice(), [EndedState::Incomplete { .. }, EndedState::Complete]), "one rdm.build.ended per run: {ended:?}");
    let (node2, node3) = created;
    let op2 = rdm_core::OperationId::node("node.create", &node2);
    let op3 = rdm_core::OperationId::node("node.create", &node3);
    assert_eq!(ran.iter().filter(|o| **o == op2).count(), 1, "mesh2.node.2 completed in the first run and never runs again: {ran:?}");
    assert_eq!(ran.iter().filter(|o| **o == op3).count(), 2, "mesh2.node.3 ran at A, then at B: {ran:?}");
    assert_eq!(sections, vec![(a.clone(), "mesh2".to_string()), (b.clone(), "mesh2".to_string())], "the resubmit's section went to mesh2's new primary");
    let at_b: Vec<_> = p.steps_in("mesh2").filter(|s| s.operation == op3.to_string() && s.step == "Complete").map(|s| s.source.clone()).collect();
    assert!(at_b.iter().all(|s| s.contains(b.node_id.as_str())) && !at_b.is_empty(), "mesh2.node.3's complete step is B's: {at_b:?}");
    let spans = cap.spans();
    let at_b: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-reconcile" && s["attributes"]["executor_node_id"] == b.node_id.as_str()).collect();
    assert_eq!(at_b.len(), 1, "B ran mesh2's section once: {at_b:?}");
    // The section's operations, each named by its action and its node's id.
    let ops = at_b[0]["attributes"]["operations"].as_str().unwrap_or_default();
    assert!(ops.contains(node3.as_str()) && !ops.contains(node2.as_str()), "B's section is what mesh2 owed, read from the step events: {ops}");
}

/// The node.id the Build `build` minted for `mesh2.node.<name>` (D58).
async fn created_id(builds: &MemoryBuildStateAdapter, build: &BuildId, name: &str) -> rdm_core::NodeId {
    let p = builds.read_build(build).await.unwrap();
    p.topology.created_id(&PathName::new("mesh2.node", name).unwrap()).cloned().unwrap_or_else(|| panic!("the Build creates mesh2.node.{name}"))
}
