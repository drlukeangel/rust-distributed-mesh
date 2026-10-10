//! The fabric-primary's drive of a Build, one cell per gate it passes and per way it can be stopped
//! (op `0x20`; node-rpc-envelope.md "Build, op `0x20`"): the Build state, the view and the
//! executors are fixtures, the drive is the product's.
//!
//! The fixture view: `mesh1` has the fabric-primary `mesh1.admin.1`, its mesh primary
//! `mesh1.admin.2` (so a node of `mesh1` is executed by an admin that is not the fabric-primary),
//! and `mesh1.rpc.1`. The Build grows `mesh1` by one rpc node.

use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology, TopologyChange};
use rafka_node_admin_core::build::{BuildId, BuildOperation, MeshDesired};
use rafka_node_admin_core::build_claim::{AttemptContexts, ClaimDoor};
use rafka_node_admin_core::build_drive::{Drive, DriveEnd, DriveEnv, Gate, Read};
use rafka_node_admin_core::build_state::{BuildStateAdapter, BuildStateError, MemoryBuildStateAdapter};
use rafka_node_admin_core::executor::OperationRunner;
use rafka_node_admin_core::http::ControlPlane;
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc_contract::build::BuildReply;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.is_primary = primary;
    n.is_fabric_primary = fabric_primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

fn view() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![node("mesh1.admin.1", false, true), node("mesh1.admin.2", true, false), node("mesh1.rpc.1", false, false)],
    }
}

/// A gate the cell opens and closes.
struct Switch {
    authorizes: AtomicBool,
    fenced: AtomicBool,
}

#[async_trait::async_trait]
impl Gate for Switch {
    fn authorizes(&self) -> bool {
        self.authorizes.load(Ordering::SeqCst)
    }
    async fn fence(&self) -> Result<(), BuildStateError> {
        if self.fenced.load(Ordering::SeqCst) {
            Err(BuildStateError::Fenced { node: "mesh1.admin.1".into(), by: "leaving".into() })
        } else {
            Ok(())
        }
    }
}

struct Creates(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl OperationRunner for Creates {
    async fn run(&self, _: &BuildId, _: u32, op: &BuildOperation) -> Result<(), String> {
        self.0.lock().unwrap().push(op.key());
        Ok(())
    }
}

struct Rig {
    builds: Arc<MemoryBuildStateAdapter>,
    env: Arc<DriveEnv>,
    switch: Arc<Switch>,
    dispatch: Arc<crate::loopback::Loopback>,
    proofs: Arc<crate::loopback::Proofs>,
    ran: Arc<Creates>,
    build: BuildId,
}

async fn rig() -> Rig {
    let change = TopologyChange::ReconcileMesh { desired: MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 2)]) };
    rig_for(view(), change, |ran, _| ran).await
}

/// A Build of `change` over `t`, run by `runner(ran, topology)`: `ran` records what ran.
async fn rig_for(t: Topology, change: TopologyChange, runner: impl FnOnce(Arc<Creates>, Arc<RwLock<Topology>>) -> Arc<dyn OperationRunner>) -> Rig {
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let topology = Arc::new(RwLock::new(t.clone()));
    let mut cp = ControlPlane::new(builds.clone(), accepted.clone(), "mesh1.admin.1".parse().unwrap(), t, crate::adopted_time());
    cp.topology = topology.clone();
    let opened = cp.submit("test", change).await.unwrap();
    let ran = Arc::new(Creates(Mutex::new(Vec::new())));
    let dispatch = crate::loopback::Loopback::new(builds.clone(), builds.clone(), topology.clone(), runner(ran.clone(), topology.clone()));
    let proofs = Arc::new(crate::loopback::Proofs::default());
    let switch = Arc::new(Switch { authorizes: AtomicBool::new(true), fenced: AtomicBool::new(false) });
    let me: PathName = "mesh1.admin.1".parse().unwrap();
    let env = Arc::new(DriveEnv {
        me: me.clone(),
        topology: topology.clone(),
        accepted,
        builds: builds.clone(),
        door: Arc::new(ClaimDoor { me, topology, builds: builds.clone(), contexts: Arc::new(AttemptContexts::in_memory()) }),
        dispatcher: dispatch.clone(),
        gate: switch.clone(),
        departure: proofs.clone(),
        verdicts: Arc::new(crate::loopback::NoVerdicts),
    });
    Rig { builds, env, switch, dispatch, proofs, ran, build: opened.build_id }
}

async fn next_within(reader: &mut rafka_node_admin_core::build_drive::DriveReader) -> Option<Read> {
    tokio::time::timeout(Duration::from_millis(50), reader.next()).await.ok().flatten()
}

/// CONTRACT: a fabric-primary that yielded its seat dispatches nothing. The Build log's sticky
/// fence is checked before the claim: the drive ends `SeatLost` naming the fence, no attempt is
/// claimed, no executor is called, and the callers' streams end with the seat loss (the caller
/// re-submits by the Build id at the new seat).
#[tokio::test]
async fn a_fabric_primary_that_yielded_its_seat_claims_and_dispatches_nothing() {
    let r = rig().await;
    r.switch.fenced.store(true, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    match r.env.run(&drive).await {
        DriveEnd::SeatLost(why) => assert!(why.contains("yielded the fabric-primary seat"), "{why}"),
        other => panic!("a fenced seat ends the drive SeatLost: {other:?}"),
    }
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty(), "no executor was called");
    assert!(r.ran.0.lock().unwrap().is_empty());
    assert!(matches!(next_within(&mut reader).await, Some(Read::SeatLost(why)) if why.contains("leaving")));
}

/// CONTRACT: a cut-off view dispatches nothing. It authorizes no claim and no call; the drive
/// pauses without ending, and the same drive goes on to the Build's end once the view authorizes.
#[tokio::test]
async fn a_cut_off_view_dispatches_nothing_and_the_drive_goes_on_once_it_authorizes() {
    let r = rig().await;
    r.switch.authorizes.store(false, Ordering::SeqCst);
    let drive = Drive::detached(r.build.clone());
    assert!(matches!(r.env.run(&drive).await, DriveEnd::Paused(why) if why.contains("cut off")));
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 0, "no attempt was claimed");
    assert!(r.dispatch.dispatched.lock().unwrap().is_empty());
    r.switch.authorizes.store(true, Ordering::SeqCst);
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh1.rpc.2".to_string()]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "the node of mesh1 is run by mesh1's primary");
}

/// CONTRACT: an executor that cannot be reached is replaced only on proof of its departure. The
/// attempt claimed for it stays its own, the drive blocks with a frame naming why (once, however
/// many passes find the same reason), and no other admin is claimed for it and nothing runs.
#[tokio::test]
async fn an_unreachable_executor_without_proof_keeps_its_attempt_and_blocks_once() {
    let r = rig().await;
    r.dispatch.lose("mesh1.admin.2");
    let drive = Drive::detached(r.build.clone());
    let mut reader = drive.reader(1);
    for _ in 0..3 {
        match r.env.run(&drive).await {
            DriveEnd::Stalled(why) => assert!(why.contains("departure is not proven"), "{why}"),
            other => panic!("silence proves nothing: {other:?}"),
        }
    }
    let p = r.builds.read_build(&r.build).await.unwrap();
    assert_eq!((p.attempt, p.executor.as_deref()), (1, Some("mesh1.admin.2")), "attempt 1 is still the unreachable admin's");
    assert!(r.ran.0.lock().unwrap().is_empty(), "nothing ran on silence");
    match next_within(&mut reader).await {
        Some(Read::Frame(BuildReply::Blocked { attempt: 1, step, reason, .. })) => {
            assert_eq!(step, "departure-proof");
            assert!(reason.contains("mesh1.admin.2") && reason.contains("not proven"), "{reason}");
        }
        other => panic!("the drive says why it waits: {other:?}"),
    }
    assert!(next_within(&mut reader).await.is_none(), "the same blocker is framed once, not on every pass");
    // The proof arrives: the next attempt is claimed for the admin the view now names, and the
    // caller's reader carries its frames to the terminal.
    r.proofs.exited.lock().unwrap().insert("mesh1.admin.2".into());
    {
        let mut v = r.env.topology.write().await;
        for n in v.nodes.iter_mut() {
            match n.name.to_string().as_str() {
                "mesh1.admin.2" => {
                    n.status = NodeStatus::PendingReconnect;
                    n.is_primary = false;
                }
                "mesh1.admin.1" => n.is_primary = true,
                _ => {}
            }
        }
    }
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.1".to_string(), 2)], "attempt 2 went to the admin the view names, not to the lost one");
    assert_eq!(
        *r.ran.0.lock().unwrap(),
        vec!["create-node:mesh1.admin.2".to_string(), "create-node:mesh1.rpc.2".to_string()],
        "the lost admin's path is part of what is left, planned from the view as it is now"
    );
}

/// CONTRACT: an executor runs the first operation of the attempt the fabric-primary claimed for it
/// whatever its own view says. The fabric-primary's view named `mesh1.admin.2` the executor; the view
/// of `mesh1.admin.2` holds no mesh primary yet (it has not elected itself since the old one was
/// lost), so by it the operation is the fabric-primary's. The attempt runs on `mesh1.admin.2` and is
/// not passed back: every hand-off follows a step completed, so two views that disagree never
/// claim attempts back and forth.
#[tokio::test]
async fn an_executor_whose_view_lags_runs_the_first_operation_it_was_claimed_for_and_the_attempt_is_not_passed_back() {
    let r = rig().await;
    let mut theirs = view();
    for n in theirs.nodes.iter_mut() {
        n.is_primary = false;
    }
    r.dispatch.sees("mesh1.admin.2", Arc::new(RwLock::new(theirs)));
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(*r.ran.0.lock().unwrap(), vec!["create-node:mesh1.rpc.2".to_string()]);
    assert_eq!(*r.dispatch.dispatched.lock().unwrap(), vec![("mesh1.admin.2".to_string(), 1)], "one attempt, on the admin the fabric-primary's view named");
    assert_eq!(r.builds.read_build(&r.build).await.unwrap().attempt, 1);
}

/// Moves the fabric seat to `mesh2.admin.1` when the operation is prepared, as the mesh-retire
/// handover does for the mesh that holds the seat.
struct SeatMover {
    ran: Arc<Creates>,
    topology: Arc<RwLock<Topology>>,
}

#[async_trait::async_trait]
impl OperationRunner for SeatMover {
    async fn run(&self, build: &BuildId, attempt: u32, op: &BuildOperation) -> Result<(), String> {
        self.ran.run(build, attempt, op).await
    }
    async fn prepare(&self, _: &BuildId, _: u32, op: &BuildOperation) -> Result<(), String> {
        if matches!(op, BuildOperation::ShutdownMesh { .. }) {
            for n in self.topology.write().await.nodes.iter_mut() {
                n.is_fabric_primary = n.name.to_string() == "mesh2.admin.1";
            }
        }
        Ok(())
    }
}

/// CONTRACT: a mesh retire whose fabric-primary sits inside the leaving mesh is run by the admin
/// the handover made the fabric-primary. The claimed executor hands the seat over when the
/// operation is prepared, the executor of the operation is then another admin, and the attempt is
/// passed to it before the operation runs: the leaving mesh's admin never runs its own retire.
#[tokio::test]
async fn a_mesh_retire_whose_seat_moved_at_prepare_is_run_by_the_new_fabric_primary() {
    let mut t = view();
    t.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic });
    t.nodes.push(node("mesh2.admin.1", true, false));
    let r = rig_for(t, TopologyChange::RemoveMesh { mesh: "mesh1".into() }, |ran, topology| Arc::new(SeatMover { ran, topology })).await;
    let drive = Drive::detached(r.build.clone());
    assert_eq!(r.env.run(&drive).await, DriveEnd::Terminal);
    assert_eq!(
        *r.dispatch.dispatched.lock().unwrap(),
        vec![("mesh1.admin.1".to_string(), 1), ("mesh2.admin.1".to_string(), 2)],
        "attempt 1 prepared the handover and passed the attempt on; attempt 2 ran it at the new fabric-primary"
    );
    assert_eq!(r.ran.0.lock().unwrap().len(), 1, "the retire ran once");
}
