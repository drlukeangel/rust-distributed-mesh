//! product=mesh, feature=fabric-lifecycle, subfeature=fabric-delete, rung=MM, provider=process.
//!
//! `fabric.delete` is the one Build that may interrupt the Build in progress (build.md §1): its
//! acceptance record is the durable intent; once written, the interrupted Build opens no further
//! operation and is never resumed. Operations already admitted at an executor finish or fail within
//! their bounds, their launch evidence retained; a round the fabric-primary drives itself ends at the
//! acceptance; when every open attempt of the interrupted Build has ended its run receives `Failed`
//! naming the delete, and the teardown runs mesh by mesh. Before the initial fabric first reaches
//! ready-for-traffic, fabric.delete is admitted and its own mesh.delete and node.delete steps run exempt
//! from the `fabric-not-ready` refusal, while a standalone mesh.delete or node.delete is still refused
//! (states.md "The initial fabric before ready-for-traffic"; lifecycles/fabric-delete.md "Precondition").
//!
//! Every estate runs the testkit's faulted node-admin (`faults::binding_set`), so a cell holds an
//! executor at a cut through its fault door; the testkit application (`RDM_TEST_APP`) holds the
//! fabric in state-sync when a cell needs a fabric that never reaches ready-for-traffic.

use crate::build_log;
use rdm_client::{BuildFrame, BuildId, BuildStream, CallEnd, Change, LaunchKind as NodeKind, MeshCounts, PathName, ScopeStatus};
use rdm_test::estate::{named, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::time::Duration;

/// The bound of every wait of these cells: a teardown, a Build's terminal, a fact on a log.
const SETTLE: Duration = Duration::from_secs(180);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "fabric-lifecycle".into(), subfeature: "fabric-delete".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

fn path(n: &str) -> PathName {
    n.parse().unwrap()
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// Whether `dir`, a launched runtime's data dir under the estate root, is `node`'s (`<path.name>-<suffix>`).
fn of_node(dir: &std::path::Path, node: &str) -> bool {
    dir.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(&format!("{node}-")))
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// An estate of the faulted node-admin whose testkit application is `app` (`never`: it takes the
/// state-sync round and never answers, so the fabric never reaches ready-for-traffic).
async fn estate_with(test: &str, app: Option<&str>) -> Estate {
    let sha = candidate_sha();
    let env: Vec<(&str, &str)> = app.map(|a| ("RDM_TEST_APP", a)).into_iter().collect();
    Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &env).await.expect("the faulted-admin binding set is accepted")
}

/// A fabric at `shape` that reached ready-for-traffic.
async fn ready_fabric(test: &str, shape: &[(&str, u32, u32)]) -> Estate {
    let estate = estate_with(test, None).await;
    estate.reconcile_shape("fabric1", shape).await;
    estate.settles_to(shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    estate
}

/// The fabric is held in state-sync under `build`, as the fabric-primary reports it.
async fn held_in_state_sync(estate: &Estate, build: &str) {
    estate
        .fabrics_obj()
        .await
        .fact(&format!("the fabric is held in state-sync under {build}"), Duration::from_secs(60), |f| f.status == ScopeStatus::StateSync && f.build_id.as_deref() == Some(build))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
}

/// `build.create { DeleteFabric }` at the fabric-primary, admitted: its stream. A refusal names itself.
async fn delete_admitted(estate: &Estate) -> BuildStream {
    estate.carrier().await.create(Change::DeleteFabric).await.unwrap_or_else(|end| panic!("fabric.delete was refused: {end}"))
}

/// Every frame of `stream` to its terminal (`Complete`, `Failed` or `FabricPrimaryMoved`), bounded by
/// `within`: a stream that breaks, ends without a terminal or outlasts the bound fails the cell by name.
async fn terminal_of(what: &str, stream: &mut BuildStream, within: Duration) -> Vec<BuildFrame> {
    let id = stream.build_id().clone();
    tokio::time::timeout(within, async {
        let mut frames = Vec::new();
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap_or_else(|e| panic!("{what}: the stream of Build {id} broke before its terminal: {e}"));
            let end = matches!(frame, BuildFrame::Complete { .. } | BuildFrame::Failed { .. } | BuildFrame::FabricPrimaryMoved { .. });
            frames.push(frame);
            if end {
                return frames;
            }
        }
        panic!("{what}: the stream of Build {id} ended without a terminal frame: {frames:?}")
    })
    .await
    .unwrap_or_else(|_| panic!("{what}: Build {id} reached no terminal frame within {within:?}"))
}

/// The delete's stream followed to `Complete` across the loss of the fabric-primary that accepted it: a
/// stream that breaks is reattached by its build id at the fabric-primary `admin` names (under the delete,
/// fabric recovery's executor, which holds the fabric-primary's roles: fabric-recovery.md step 4, Q155),
/// bounded.
/// The fabric-primary the stream ended at.
async fn delete_followed(estate: &Estate, stream: BuildStream, admin: &str) -> String {
    let id = stream.build_id().clone();
    let at = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
    let seen = at.clone();
    let followed = tokio::time::timeout(SETTLE * 2, rdm_client::follow_with(stream, |id, _from| {
        let seen = seen.clone();
        async move {
            let deadline = tokio::time::Instant::now() + SETTLE;
            loop {
                if let Some(fp) = estate.fabric_view_at(&path(admin)).await.and_then(|v| v.fabric_primary) {
                    // The fabric-primary's node.get first: a birth recovery re-created is learnt from a view that lists it.
                    if estate.node_views_at(&fp).await.is_some() {
                        if let Ok(again) = estate.carrier_at(&fp).await.reattach(&id).await {
                            *seen.lock().unwrap() = Some(fp.to_string());
                            return Ok(again);
                        }
                    }
                }
                assert!(tokio::time::Instant::now() < deadline, "{admin} named no fabric-primary that resubmitted Build {id} within {SETTLE:?}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }))
    .await
    .unwrap_or_else(|_| panic!("fabric.delete {id} reached no terminal frame within {:?}", SETTLE * 2))
    .unwrap_or_else(|e| panic!("fabric.delete {id} could not be followed to its terminal: {e}"));
    match followed.frames.last() {
        Some(BuildFrame::Complete { .. }) => {}
        other => panic!("fabric.delete {id} ended {other:?}: {:?}", followed.frames),
    }
    let fp = at.lock().unwrap().clone();
    fp.unwrap_or_else(|| panic!("fabric.delete {id} completed with no reattachment after its fabric-primary was lost"))
}

/// Fabric recovery under an in-force fabric.delete re-creates nothing (fabric-recovery.md step 4, Q155):
/// no node.create span opens after the delete's acceptance (`accepted_ns`), and the workflow's executor
/// `fp` holds the fabric-primary's roles, its own node named restored.
fn nothing_recreated_under_the_delete(spans: &[Value], accepted_ns: u64, fp: &str) {
    // A mesh-primary's write of the rows of the members it now writes for (`moment = arrival`) creates nothing.
    let created: Vec<&Value> = spans.iter().filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_admin.node.create.")) && sp["attributes"]["moment"] != "arrival" && start(sp) >= accepted_ns).collect();
    assert!(created.is_empty(), "nothing is created after the delete's acceptance: {created:?}");
    let held: Vec<&Value> = named(spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| start(sp) >= accepted_ns && sp["attributes"]["restored"] == fp).collect();
    assert!(!held.is_empty(), "fabric recovery's executor {fp} holds the fabric-primary's roles under the delete");
    for sp in &held {
        assert!(sp["attributes"]["arm"].as_str().is_some_and(|a| a.ends_with("under-delete")), "the workflow ran its arm under the delete: {sp}");
    }
}

/// The receipts of Build `id` as the fabric-primary `admin` holds them.
async fn receipts_at(estate: &Estate, admin: &str, id: &str) -> rdm_client::BuildReceipts {
    estate.carrier_at(&path(admin)).await.get(&BuildId::parse(id).unwrap()).await.unwrap_or_else(|e| panic!("build.get {id} at {admin}: {e}"))
}

fn count(receipts: &rdm_client::BuildReceipts, operation: impl Fn(&str) -> bool, step: &str) -> usize {
    receipts.steps.iter().filter(|r| operation(&r.operation) && r.step == step).count()
}

fn listed(receipts: &rdm_client::BuildReceipts) -> Vec<String> {
    receipts.steps.iter().map(|r| format!("{} {} #{}", r.operation, r.step, r.attempt)).collect()
}

/// The interrupted Build reads `Failed`, its last failure naming the delete.
fn failed_naming(receipts: &rdm_client::BuildReceipts, delete: &str) {
    assert_eq!(receipts.phase, rdm_core::build::BuildPhase::Failed, "the interrupted Build {} is failed: {:?}", receipts.build_id, receipts.last_failure);
    let why = receipts.last_failure.clone().unwrap_or_default();
    assert!(why.contains(delete), "the interrupted Build's failure names the delete {delete}: {why}");
}

/// The teardown's receipts: every mesh's mesh.delete reported `MeshDeleted`, mesh2's primary has its
/// node.delete receipt, and fabric.deleted was reported.
fn torn_down(receipts: &rdm_client::BuildReceipts, meshes: usize) {
    assert_eq!(count(receipts, |o| o.starts_with("mesh.delete:"), rdm_admin::mesh_delete::STEP_MESH_DELETED), meshes, "every mesh is deleted once: {:?}", listed(receipts));
    assert_eq!(count(receipts, |o| o == "node.delete:mesh2.admin.1", "Complete"), 1, "mesh2's primary has its node.delete receipt: {:?}", listed(receipts));
    assert_eq!(count(receipts, |o| o.starts_with("fabric.delete:"), "Complete"), 1, "fabric.deleted is reported: {:?}", listed(receipts));
}

/// CONTRACT (states.md "The initial fabric before ready-for-traffic": "fabric.delete is admitted before
/// the first ready-for-traffic ... its own mesh.delete and node.delete steps, and only those, run exempt
/// from this refusal"; build.md §1: "a round the fabric-primary drives itself ends at the acceptance, its
/// hook's future dropped"; fabric-delete.md "Receipts"): a two-mesh fabric whose application takes
/// `sync_state` and never answers is held in state-sync. A standalone mesh.delete of mesh2 and node.delete
/// of mesh2.rpc.1 are refused `fabric-not-ready`; fabric.delete is admitted, and a second fabric.delete
/// answers the same Build. The teardown deletes both meshes, mesh2's primary by its node.delete, reports
/// fabric.deleted, and leaves no runtime the fabric launched running; the application is never called
/// again after the acceptance. What must NOT happen: the stalled fabric left undeletable, a round of
/// state-sync opened for the delete, or a runtime left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_held_in_state_sync_by_an_application_that_never_answers_is_deleted_with_every_receipt() {
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1)];
    let mut estate = estate_with("a_fabric_held_in_state_sync_by_an_application_that_never_answers_is_deleted_with_every_receipt", Some("never")).await;
    // The first Build is Complete only once the fabric's ready-for-traffic state is set (Q154), which the
    // application never lets happen: it is followed by its stream, never awaited to its end.
    let first_stream = estate.submit_shape("fabric1", &shape).await;
    let first = first_stream.build_id().to_string();
    estate.settled_views(&shape).await;
    held_in_state_sync(&estate, &first).await;

    let carrier = estate.carrier().await;
    let standalone: Vec<(&str, Result<(), CallEnd>)> = vec![
        ("mesh.delete mesh2", carrier.create(Change::RemoveMesh { mesh: "mesh2".into() }).await.map(|_| ())),
        ("node.delete mesh2.rpc.1", estate.nodes_obj().await.delete(&path("mesh2.rpc.1")).await.map(|_| ())),
    ];
    for (workflow, end) in standalone {
        match end {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "fabric-not-ready" && detail.contains(workflow), "{workflow} is refused fabric-not-ready: {reason} {detail}"),
            other => panic!("the standalone {workflow} was not refused fabric-not-ready: {other:?}"),
        }
    }

    let accepted_ns = now_ns();
    let mut delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let again = delete_admitted(&estate).await;
    assert_eq!(again.build_id().to_string(), id, "a second fabric.delete answers the first");
    drop(again);
    let frames = terminal_of("fabric.delete", &mut delete, SETTLE).await;
    assert!(matches!(frames.last(), Some(BuildFrame::Complete { .. })), "fabric.delete {id} completes: {frames:?}");
    let receipts = receipts_at(&estate, "mesh1.admin.1", &id).await;
    torn_down(&receipts, 2);
    assert_eq!(estate.live_runtimes(), vec![], "no runtime the fabric launched is left running");
    estate.stop().await;

    let spans = estate.spans();
    let late: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-sync-state-called").into_iter().filter(|sp| start(sp) >= accepted_ns).collect();
    assert!(late.is_empty(), "the application is called no more after the delete's acceptance: {late:?}");
    let entered: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-state-entered").into_iter().filter(|sp| start(sp) >= accepted_ns && sp["attributes"]["to"] == "state-sync").collect();
    assert!(entered.is_empty(), "no state-sync is entered after the delete's acceptance: {entered:?}");
}

/// CONTRACT (build.md §1: "Operations already admitted at an executor finish or fail within their
/// existing bounds and their launch evidence is retained ... When every open attempt of the interrupted
/// Build has ended, its run receives `Failed { step, reason }` naming the delete"): before the fabric
/// first reaches ready-for-traffic, a Build growing mesh1 by two rpc nodes is held by the fabric-primary's
/// fault door inside the launch of the first, and fabric.delete is accepted. Released, the launch ends
/// within its bound and its create's receipts stand; the second rpc node is never launched; the growth's
/// run receives `Failed` naming the delete, and the teardown deletes the launched runtime with the rest.
/// What must NOT happen: a launch opened after the acceptance, the growth resumed, or its runtime left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_delete_accepted_mid_launch_lets_the_launch_end_and_launches_nothing_after() {
    let mut estate = estate_with("a_fabric_delete_accepted_mid_launch_lets_the_launch_end_and_launches_nothing_after", Some("never")).await;
    let door = Door::at(&estate, "mesh1.admin.1").await;
    door.arm("launch", json!({"kind": "receipt", "step": "DeployRuntime", "operation": "node.create:mesh1.rpc.1"})).await;
    let mut grow = estate.submit_change(Change::ReconcileMesh { mesh: MeshCounts { name: "mesh1".into(), node_admin: 1, rpc_node: 2, broker: 0, gateway: 0, compute: 0 } }).await;
    let grow_id = grow.build_id().to_string();
    let held = door.wait_held("launch").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(grow_id.as_str()), "the growth is held inside mesh1.rpc.1's launch: {held}");

    let mut delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    door.release("launch").await;
    let ended = terminal_of("the interrupted growth", &mut grow, SETTLE).await;
    match ended.last() {
        Some(BuildFrame::Failed { reason, .. }) => assert!(reason.contains(&id), "the growth's run receives Failed naming the delete {id}: {reason}"),
        other => panic!("the interrupted growth ended {other:?}, not Failed naming the delete: {ended:?}"),
    }
    let frames = terminal_of("fabric.delete", &mut delete, SETTLE).await;
    assert!(matches!(frames.last(), Some(BuildFrame::Complete { .. })), "fabric.delete {id} completes: {frames:?}");

    let grown = receipts_at(&estate, "mesh1.admin.1", &grow_id).await;
    failed_naming(&grown, &id);
    assert_eq!(count(&grown, |o| o == "node.create:mesh1.rpc.1", "DeployRuntime"), 1, "the admitted launch's evidence is retained: {:?}", listed(&grown));
    assert_eq!(count(&grown, |o| o == "node.create:mesh1.rpc.1", "Complete"), 1, "the admitted launch ended within its bound: {:?}", listed(&grown));
    assert_eq!(count(&grown, |o| o == "node.create:mesh1.rpc.2", "AllocateIdentity"), 0, "nothing is launched after the acceptance: {:?}", listed(&grown));
    let receipts = receipts_at(&estate, "mesh1.admin.1", &id).await;
    assert_eq!(count(&receipts, |o| o.starts_with("mesh.delete:"), rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh1 is deleted: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| o.starts_with("fabric.delete:"), "Complete"), 1, "fabric.deleted is reported: {:?}", listed(&receipts));
    assert_eq!(estate.live_runtimes(), vec![], "the launched runtime is deleted with the fabric");
    estate.stop().await;
}

/// CONTRACT (build.md §1: "its acceptance record is the durable intent ... a successor fabric-primary
/// reads the delete as the current Build and continues the teardown"; "the interrupted Build opens no
/// further operation and is never resumed"; fabric-recovery.md step 4: "Under an in-force fabric.delete
/// ... the workflow re-creates and restores nothing ... the executor holds the fabric-primary's roles to
/// the end and continues the delete"): on a two-mesh fabric (two node-admins in mesh1), the
/// fabric-primary's growth of mesh1 is held by its fault door inside mesh1.rpc.2's launch, fabric.delete
/// is accepted, and once mesh1.admin.2's log holds the acceptance the fabric-primary is killed. Fabric
/// recovery's executor, mesh1.admin.2, holds the fabric-primary's roles and reads the delete as current:
/// the growth's attempt ends once its executor's exit is read and reads `Failed` naming the delete,
/// mesh1.rpc.2's create is never run again, and its launched runtime is deleted with the fabric, which
/// reports fabric.deleted. What must NOT happen: the growth resumed, the lost fabric-primary re-created
/// (no node.create after the acceptance), or the outstanding runtime left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fabric_primary_lost_after_the_delete_is_accepted_with_a_create_outstanding_is_continued_by_its_successor() {
    let shape = [("mesh1", 2, 1), ("mesh2", 1, 1)];
    let mut estate = ready_fabric("the_fabric_primary_lost_after_the_delete_is_accepted_with_a_create_outstanding_is_continued_by_its_successor", &shape).await;
    let door = Door::at(&estate, "mesh1.admin.1").await;
    door.arm("launch", json!({"kind": "receipt", "step": "DeployRuntime", "operation": "node.create:mesh1.rpc.2"})).await;
    let grow = estate.submit_change(Change::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }).await;
    let grow_id = grow.build_id().to_string();
    let held = door.wait_held("launch").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(grow_id.as_str()), "the growth is held inside mesh1.rpc.2's launch: {held}");
    let launched: Vec<u32> = estate.live_runtimes().into_iter().filter(|(dir, _)| of_node(dir, "mesh1.rpc.2")).map(|(_, pid)| pid).collect();
    assert_eq!(launched.len(), 1, "mesh1.rpc.2's runtime is launched: {:?}", estate.live_runtimes());

    let accepted_ns = now_ns();
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    build_log::build_fact(&estate.carrier_at(&path("mesh1.admin.2")).await, &id, "the delete's acceptance", SETTLE, |_| true).await.unwrap_or_else(|end| panic!("{end}"));
    drop(grow);
    estate.kill_bootstrap();
    let fp = delete_followed(&estate, delete, "mesh1.admin.2").await;

    let grown = receipts_at(&estate, &fp, &grow_id).await;
    failed_naming(&grown, &id);
    assert_eq!(count(&grown, |o| o == "node.create:mesh1.rpc.2", "Complete"), 0, "the outstanding create is never run again: {:?}", listed(&grown));
    let receipts = receipts_at(&estate, &fp, &id).await;
    torn_down(&receipts, 2);
    let left: Vec<u32> = estate.live_runtimes().into_iter().map(|(_, pid)| pid).collect();
    assert!(!left.contains(&launched[0]), "the outstanding runtime of mesh1.rpc.2 is captured and deleted: {left:?}");
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !of_node(dir, &fp)).collect::<Vec<_>>(), vec![], "only the fabric-primary {fp} is left, for its separate node.delete");
    estate.stop().await;
    nothing_recreated_under_the_delete(&estate.spans(), accepted_ns, &fp);
}

/// CONTRACT (lifecycles/fabric-delete.md "Numbered end-to-end sequence"; build.md §1: "a successor
/// fabric-primary reads the delete as the current Build and continues the teardown"; fabric-delete.md
/// "Failure": "A fabric-primary lost mid-delete is fabric recovery's: its executor continues the delete as
/// fabric-primary and re-creates nothing"): on a two-mesh fabric, fabric.delete's executor (the
/// fabric-primary) is held by its fault door at mesh2's primary's node.delete, after mesh2 is deleted, and
/// killed. Fabric recovery's executor in mesh1 holds the fabric-primary's roles and runs the next attempt:
/// mesh2's primary is deleted once, its row written by that executor, mesh1 is deleted, and fabric.deleted
/// is reported. What must NOT happen: a mesh deleted twice, the lost fabric-primary re-created, the
/// teardown left unfinished, or a runtime left beside the fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_delete_executor_killed_after_the_first_mesh_is_finished_by_the_next_attempt() {
    let shape = [("mesh1", 2, 1), ("mesh2", 1, 1)];
    let mut estate = ready_fabric("the_delete_executor_killed_after_the_first_mesh_is_finished_by_the_next_attempt", &shape).await;
    let door = Door::at(&estate, "mesh1.admin.1").await;
    door.arm("first-mesh", json!({"kind": "receipt", "step": "NodeDeleted", "operation": "node.delete:mesh2.admin.1"})).await;
    let accepted_ns = now_ns();
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let held = door.wait_held("first-mesh").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(id.as_str()), "the delete is held at mesh2's primary's node.delete: {held}");
    build_log::build_fact(&estate.carrier_at(&path("mesh1.admin.2")).await, &id, "mesh2's MeshDeleted", SETTLE, |b| {
        b.steps.iter().any(|r| r.operation.starts_with("mesh.delete:") && r.step == rdm_admin::mesh_delete::STEP_MESH_DELETED)
    })
    .await
    .unwrap_or_else(|end| panic!("{end}"));
    estate.kill_bootstrap();
    let fp = delete_followed(&estate, delete, "mesh1.admin.2").await;

    let receipts = receipts_at(&estate, &fp, &id).await;
    torn_down(&receipts, 2);
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !of_node(dir, &fp)).collect::<Vec<_>>(), vec![], "only the fabric-primary {fp} is left, for its separate node.delete");
    estate.stop().await;
    nothing_recreated_under_the_delete(&estate.spans(), accepted_ns, &fp);
}

/// CONTRACT (Q156, its second cell; fabric-recovery.md "Nothing alive": "a person or a script launches a
/// node-admin at the lost fabric-primary's path.name, with no launcher, given the fabric id, its mesh and
/// the store root ... It installs the recorded primaries before electing (Q149) ... and restores it as
/// fabric-primary by step 4 once ReadyForTraffic"; build.md §1: "a successor fabric-primary reads the
/// delete as the current Build and continues the teardown"): a one-mesh fabric's sole node-admin, the
/// fabric-primary, accepts fabric.delete and is held by its fault door at mesh1's mesh.delete, its members
/// still serving, and is killed. A person launches a node-admin at mesh1.admin.1 over a new data dir,
/// handed the fabric id, mesh1, the store root and the custody. It adopts the fabric, becomes the
/// fabric-primary and finishes the delete: mesh1 is deleted and fabric.deleted is reported. What must NOT
/// happen: the launch refused while nothing else can finish the delete, a second fabric, or a member's
/// runtime left running beside the adopted fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Q156 boundary, open: the adoption claim is refused build-in-progress by mesh1's in-flight row, which the delete's fabric-lane attempt the lost birth claimed holds; Q156's claim before any mutation and Q152's release of a departed claimant's row (departure proof first) need a ruling on their order"]
async fn the_sole_admin_lost_mid_delete_is_adopted_by_a_launch_that_finishes_the_delete() {
    let shape = [("mesh1", 1, 2)];
    let mut estate = ready_fabric("the_sole_admin_lost_mid_delete_is_adopted_by_a_launch_that_finishes_the_delete", &shape).await;
    let mesh_id = estate.mesh_view("mesh1").await.id.map(|i| i.to_string()).expect("mesh1's id");
    let door = Door::at(&estate, "mesh1.admin.1").await;
    door.arm("roster", json!({"kind": "receipt", "step": rdm_admin::mesh_delete::STEP_ACCEPTED_ROSTER, "operation": format!("mesh.delete:{mesh_id}")})).await;
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let held = door.wait_held("roster").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(id.as_str()), "the delete is held at mesh1's mesh.delete: {held}");
    let custody = rdm_test::test_certs::custody_of(&estate.bootstrap_data_dir("mesh1"), "mesh1");
    estate.kill_bootstrap();

    let mut env: Vec<(&str, String)> = vec![("RDM_NODE_NAME", "mesh1.admin.1".into()), ("RDM_FABRIC_ID", estate.fabric_id.clone()), ("RDM_MESH", "mesh1".into())];
    env.extend(custody);
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let data_dir = estate.root.join("adopted-mesh1.admin.1");
    let adopted = estate.restart_admin_with(&data_dir, &env);
    assert_eq!(adopted.to_string(), "mesh1.admin.1", "the launch is born at the lost fabric-primary's path.name");
    let fp = delete_followed(&estate, delete, "mesh1.admin.1").await;
    let receipts = receipts_at(&estate, &fp, &id).await;
    assert_eq!(count(&receipts, |o| o.starts_with("mesh.delete:"), rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh1 is deleted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| o.starts_with("fabric.delete:"), "Complete"), 1, "fabric.deleted is reported: {:?}", listed(&receipts));
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !of_node(dir, &fp)).collect::<Vec<_>>(), vec![], "only the adopted fabric-primary {fp} is left, for its separate node.delete");
    estate.stop().await;
}
