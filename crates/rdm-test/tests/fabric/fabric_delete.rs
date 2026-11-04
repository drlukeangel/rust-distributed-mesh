//! product=mesh, feature=fabric-lifecycle, subfeature=fabric-delete, rung=MM, provider=process.
//!
//! `fabric.delete` is the one Build that may interrupt the Build in progress (build.md §1): its
//! acceptance record is the durable intent; once written, the interrupted Build opens no further
//! operation and is never resumed. Operations already admitted at an executor finish or fail within
//! their bounds, their launch evidence retained; a checklist the fabric-primary drives itself ends at the
//! acceptance; when every step of the interrupted Build in flight has ended, that Build ends
//! `incomplete`, each section it did not finish naming the delete, and the teardown runs mesh by mesh. While a Build is in progress fabric.delete is
//! admitted, while a standalone mesh.delete or node.delete is refused build-in-progress
//! (lifecycles/fabric-delete.md "Precondition").
//!
//! Every estate runs the testkit's faulted node-admin (`faults::binding_set`), so a cell holds an
//! executor at a cut through its fault service; a cell that needs a fabric that never reaches
//! ready holds a planned node's `pre_node_ready` (`rdm_test::write_pre_node_ready_knob`).

use crate::build_log;
use rdm_client::{BuildFrame, BuildId, BuildStream, CallEnd, Change, MeshDesired, PathName, ScopeStatus};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_core::{OperationId, OperationSubject};
use rdm_test::faults::{binding_set, candidate_sha, FaultClient};
use serde_json::{json, Value};
use std::time::Duration;

/// The bound of every wait of these cells: a teardown, a Build's terminal, a fact on a log.
const SETTLE: Duration = Duration::from_secs(180);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "fabric-lifecycle".into(), subfeature: "fabric-delete".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

/// The action of step operation `o` (`noun.verb:<id>`, build.md §5).
fn action_of(o: &str) -> &str {
    o.split(':').next().unwrap_or_default()
}

/// Whether step operation `o` is `action` on the mesh `mesh_id`.
fn on_mesh(o: &str, action: &str, mesh_id: &str) -> bool {
    o == OperationId::new(action, OperationSubject::Mesh(rdm_core::MeshId::parse(mesh_id).expect("a mesh id"))).to_string()
}

/// Whether step operation `o` is `action` of the node `node_id`.
fn of_id(o: &str, action: &str, node_id: &str) -> bool {
    o == format!("{action}:{node_id}")
}

/// The step events of `b`, `(op, step, complete)` each, in event.id order (build.md §5).
fn steps_of(b: &rdm_client::Build) -> Vec<(String, String, bool)> {
    b.events
        .iter()
        .filter(|e| e["type"] == "rdm.build.step")
        .map(|e| (e["data"]["op"].as_str().unwrap_or_default().to_string(), e["data"]["step"].as_str().unwrap_or_default().to_string(), e["data"]["state"] == "complete"))
        .collect()
}

/// Every settled node's id, by location.
async fn ids_of(estate: &Estate) -> std::collections::BTreeMap<PathName, String> {
    estate.node_values().await.iter().map(|n| (rdm_test::estate::location(n), n["node_id"].as_str().unwrap_or_default().to_string())).collect()
}

/// The settled node id at `path`/`name`.
fn id_in(ids: &std::collections::BTreeMap<PathName, String>, path: &str, name: &str) -> String {
    ids.get(&Estate::path(path, name)).cloned().unwrap_or_else(|| panic!("no settled node at {path}/{name}: {ids:?}"))
}

/// A cut's `node`: the subject of a member's node.delete, its node id.
fn delete_subject(node_id: &str) -> Value {
    json!({"Node": node_id})
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// Whether `dir`, a launched runtime's data dir under the estate root, is `node`'s (`<path.name>-<suffix>`).
/// Whether `dir` is the data dir of the node `node_id`: a data dir is named by its birth's node id.
fn of_node(dir: &std::path::Path, node_id: &str) -> bool {
    dir.file_name().and_then(|n| n.to_str()) == Some(node_id)
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// An estate of the faulted node-admin.
async fn estate_with(test: &str) -> Estate {
    let sha = candidate_sha();
    Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted")
}

/// A fabric at `shape` that reached ready.
async fn ready_fabric(test: &str, shape: &[(&str, u32, u32)]) -> Estate {
    let estate = estate_with(test).await;
    estate.resize_shape("fabric1", shape).await;
    estate.settles_to(shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    estate
}

/// The fabric is held pending under `build`.
async fn held_pending(estate: &Estate, build: &str) {
    estate
        .fabrics_obj()
        .await
        .fact(&format!("the fabric is held pending under {build}"), Duration::from_secs(60), |f| f.status == ScopeStatus::Pending && f.build_id.as_deref() == Some(build))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
}

/// `build.create { DeleteFabric }` at the fabric-primary, admitted: its stream. A refusal names itself.
async fn delete_admitted(estate: &Estate) -> BuildStream {
    estate.carrier().await.create(Change::DeleteFabric).await.unwrap_or_else(|end| panic!("fabric.delete was refused: {end}"))
}

/// Every frame of `stream` to its terminal (`Complete`, `Incomplete`, `Failed` or `FabricPrimaryMoved`), bounded by
/// `within`: a stream that breaks, ends without a terminal or outlasts the bound fails the cell by name.
async fn terminal_of(what: &str, stream: &mut BuildStream, within: Duration) -> Vec<BuildFrame> {
    let id = stream.build_id().clone();
    tokio::time::timeout(within, async {
        let mut frames = Vec::new();
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap_or_else(|e| panic!("{what}: the stream of Build {id} broke before its terminal: {e}"));
            let end = matches!(frame, BuildFrame::Complete { .. } | BuildFrame::Incomplete { .. } | BuildFrame::Failed { .. } | BuildFrame::FabricPrimaryMoved { .. });
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
async fn delete_followed(estate: &Estate, stream: BuildStream, admin: &PathName) -> (PathName, String) {
    let id = stream.build_id().clone();
    let at = std::sync::Arc::new(std::sync::Mutex::new(None::<(PathName, String)>));
    let seen = at.clone();
    let followed = tokio::time::timeout(SETTLE * 2, rdm_client::follow_with(stream, |id| {
        let seen = seen.clone();
        async move {
            let deadline = tokio::time::Instant::now() + SETTLE;
            loop {
                // The view names the fabric-primary by its node id; `admin`'s own node.get names its location.
                let views = estate.node_views_at(admin).await.unwrap_or_default();
                if let Some(fp) = estate.fabric_view_at(admin).await.and_then(|v| v.fabric_primary) {
                    // The fabric-primary's node.get first: a birth recovery re-created is learnt from a view that lists it.
                    if let Some(at) = views.iter().find(|n| n.node_id == fp).map(|n| n.location()) {
                        if estate.node_views_at(&at).await.is_some() {
                            if let Ok(again) = estate.carrier_at(&at).await.reattach(&id).await {
                                *seen.lock().unwrap() = Some((at, fp.to_string()));
                                return Ok(again);
                            }
                        }
                    }
                } else if let Ok(again) = estate.carrier_at(admin).await.reattach(&id).await {
                    // A delete that ended before its view named a fabric-primary again (its last mesh deleted):
                    // `admin` itself, the fabric-primary that reported fabric.deleted, answers the reattachment:
                    // its node id is the row its node.get lists at its name, else, its meshes deleted and no row
                    // listed, the node the reattachment by its name reached.
                    let reached = estate.peers().await.node_at(admin);
                    let own = views.iter().find(|n| n.location() == *admin).map(|n| n.node_id.clone()).or(reached)
                        .unwrap_or_else(|| panic!("{admin:?} answered the reattachment of Build {id}, and neither its node.get nor the reattachment names its node id"))
                        .to_string();
                    *seen.lock().unwrap() = Some((admin.clone(), own));
                    return Ok(again);
                }
                assert!(tokio::time::Instant::now() < deadline, "{admin:?} named no fabric-primary that resubmitted Build {id} within {SETTLE:?}");
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
fn nothing_recreated_under_the_delete(spans: &[Value], accepted_ns: u64, fp: &(PathName, String)) {
    let fp = fp.1.as_str();
    // A mesh-primary's write of the rows of the members it now writes for (`moment = arrival`) creates nothing.
    let created: Vec<&Value> = spans.iter().filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_admin.node.create.")) && sp["attributes"]["moment"] != "arrival" && start(sp) >= accepted_ns).collect();
    assert!(created.is_empty(), "nothing is created after the delete's acceptance: {created:?}");
    let held: Vec<&Value> = named(spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| start(sp) >= accepted_ns && sp["attributes"]["restored"] == fp).collect();
    assert!(!held.is_empty(), "fabric recovery's executor {fp} holds the fabric-primary's roles under the delete");
    for sp in &held {
        assert!(sp["attributes"]["arm"].as_str().is_some_and(|a| a.ends_with("under-delete")), "the workflow ran its arm under the delete: {sp}");
    }
}

/// The recorded transitions of the fabric-primary's roles under the fabric.delete `id` from `since`
/// (fabric-recovery.md step 4: "by one recorded transition at the next revision"): each fabric-recovery
/// workflow that ended holding them, at the admin its `node_path` and `node_name` name.
fn held_roles<'a>(spans: &'a [Value], id: &str, since: u64) -> Vec<&'a Value> {
    let held = format!("holds the fabric-primary's roles under fabric.delete {id}");
    named(spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| start(sp) >= since && sp["attributes"]["outcome"].as_str().is_some_and(|o| o.starts_with(&held))).collect()
}

/// Build `id` and its events as the fabric-primary `admin` reads them.
async fn receipts_at(estate: &Estate, admin: &PathName, id: &str) -> rdm_client::Build {
    estate.carrier_at(admin).await.build(&BuildId::parse(id).unwrap()).await.unwrap_or_else(|e| panic!("build.get [id: {id}] at {admin:?}: {e}"))
}

fn count(receipts: &rdm_client::Build, operation: impl Fn(&str) -> bool, step: &str) -> usize {
    steps_of(receipts).iter().filter(|(o, s, _)| operation(o) && s == step).count()
}

fn listed(receipts: &rdm_client::Build) -> Vec<String> {
    steps_of(receipts).iter().map(|(o, s, complete)| format!("{o} {s} {}", if *complete { "complete" } else { "failed" })).collect()
}

/// The interrupted Build ended `incomplete` (Q346: it ran, and its sections ended short), its
/// `rdm.build.ended` naming the delete.
fn incomplete_naming(receipts: &rdm_client::Build, delete: &str) {
    assert_eq!(receipts.phase, rdm_core::build::BuildPhase::Incomplete, "the interrupted Build {} is incomplete: {:?}", receipts.build_id, receipts.events);
    let ended = receipts.events.iter().rev().find(|e| e["type"] == "rdm.build.ended").map(|e| e["data"].to_string()).unwrap_or_default();
    assert!(ended.contains(delete), "the interrupted Build's end names the delete {delete}: {ended}");
}

/// The teardown's receipts: every mesh's mesh.delete reported `MeshDeleted`, mesh2's primary has its
/// node.delete receipt, and fabric.deleted was reported.
fn torn_down(receipts: &rdm_client::Build, meshes: usize, mesh2_primary: &str) {
    assert_eq!(count(receipts, |o| action_of(o) == "mesh.delete", rdm_admin::mesh_delete::STEP_MESH_DELETED), meshes, "every mesh is deleted once: {:?}", listed(receipts));
    assert_eq!(count(receipts, |o| of_id(o, "node.delete", mesh2_primary), "Complete"), 1, "mesh2's primary has its node.delete receipt: {:?}", listed(receipts));
    assert_eq!(count(receipts, |o| action_of(o) == "fabric.delete", "Complete"), 1, "fabric.deleted is reported: {:?}", listed(receipts));
}

/// CONTRACT (build.md §1: "The one exception is fabric.delete, which may interrupt the Build in progress";
/// fabric-delete.md "Receipts"; fabric-node-hooks.md "A node never reaches Ready while its hook
/// has not returned `Ok`"; build.md §1: operations admitted before the delete "finish or fail within their
/// existing bounds and their launch evidence is retained", Q179): a Build adding mesh1.node.1, whose
/// `pre_node_ready` never passes, puts the fabric back to pending and keeps it there; mesh1.node.1 is
/// launched and held when the delete is accepted, and its create fails within its bound under it. A standalone mesh.delete of mesh1 and node.delete of
/// mesh1.node-admin.1 are refused build-in-progress naming that Build; fabric.delete is admitted, and a second
/// fabric.delete answers the same Build. The held Build reads `incomplete` naming the delete; the teardown
/// deletes mesh1, the held node with it, reports fabric.deleted, and leaves no runtime the fabric
/// launched running; the fabric is never made ready after the acceptance. What must NOT
/// happen: the stalled fabric left undeletable, the fabric made ready for the delete, or a runtime left
/// running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_held_pending_by_a_node_whose_pre_node_ready_never_passes_is_deleted_with_every_receipt() {
    let mut estate = estate_with("a_fabric_held_pending_by_a_node_whose_pre_node_ready_never_passes_is_deleted_with_every_receipt").await;
    let ids = ids_of(&estate).await;
    // mesh1.node.1's hook is Blocked from its first attempt and never passes: its mesh, and the fabric, stay pending.
    rdm_test::write_pre_node_ready_knob(&estate.root, &rdm_core::PathName::new("mesh1.node", "1").unwrap(), "held by the cell").unwrap();
    let mut held = estate.carrier().await.create(estate.shape_change(&rdm_client::FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1), ("node", 1)])] }).await).await.unwrap_or_else(|end| panic!("the fabric-primary accepts the growth: {end}"));
    let first = held.build_id().to_string();
    // mesh1.node.1 is launched and held: the growth reports it Blocked at its pre_node_ready.
    let until = tokio::time::Instant::now() + SETTLE;
    loop {
        match tokio::time::timeout_at(until, held.next()).await {
            // The frame's operation is the growth's one create, mesh1.node.1's, keyed by the node.id its
            // record names (D58); its reason names the birth by node id.
            Ok(Some(Ok(BuildFrame::Pending { operation, .. }))) if action_of(&operation) == "node.create" => break,
            Ok(Some(Ok(frame @ (BuildFrame::Complete { .. } | BuildFrame::Failed { .. } | BuildFrame::Incomplete { .. } | BuildFrame::FabricPrimaryMoved { .. })))) => panic!("the growth {first} ended {frame:?} before mesh1.node.1 was held"),
            Ok(Some(Ok(_))) => {}
            other => panic!("the growth {first} reported no Blocked naming mesh1.node.1 within {SETTLE:?}: {other:?}"),
        }
    }
    drop(held);
    held_pending(&estate, &first).await;

    let carrier = estate.carrier().await;
    let standalone: Vec<(&str, Result<(), CallEnd>)> = vec![
        ("mesh.delete mesh1", carrier.create(Change::Delete { meshes: vec!["mesh1".into()], node_ids: vec![] }).await.map(|_| ())),
        ("node.delete mesh1.node-admin.1", estate.delete_node(&Estate::path("mesh1.node-admin", "1")).await.map(|_| ())),
    ];
    for (workflow, end) in standalone {
        match end {
            Err(CallEnd::BuildInProgress { current }) => assert_eq!(current.as_str(), first.as_str(), "{workflow} is refused naming the held Build"),
            other => panic!("the standalone {workflow} was not refused build-in-progress: {other:?}"),
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
    incomplete_naming(&receipts_at(&estate, &Estate::path("mesh1.node-admin", "1"), &first).await, &id);
    let receipts = receipts_at(&estate, &Estate::path("mesh1.node-admin", "1"), &id).await;
    assert_eq!(count(&receipts, |o| action_of(o) == "mesh.delete", rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh1 is deleted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| action_of(o) == "fabric.delete", "Complete"), 1, "fabric.deleted is reported: {:?}", listed(&receipts));
    assert_eq!(estate.live_runtimes(), vec![], "no runtime the fabric launched is left running");
    estate.stop().await;

    let spans = estate.spans();
    let ready: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-every-mesh-ready").into_iter().filter(|sp| start(sp) >= accepted_ns).collect();
    assert!(ready.is_empty(), "the fabric is never made ready after the delete's acceptance: {ready:?}");
}

/// CONTRACT (build.md §1: "Operations already admitted at an executor finish or fail within their
/// existing bounds and their launch evidence is retained ... When every step of the interrupted Build in
/// flight has ended, that Build ends `incomplete`, each section it did not finish naming the delete"): on a fabric the Day-0
/// Build brought to ready (the Day-0 Build launches no node, and no other Build is accepted
/// before its ready), a Build growing mesh1 by two nodes is held by the fabric-primary's
/// fault service inside the launch of the first, and fabric.delete is accepted. The second node's create
/// was admitted with the first (Q245: a mesh's member creates run at once). Released, the launch ends
/// within its bound and its create's receipts stand; no create opens after the acceptance; the growth's
/// run receives `Incomplete`, each section it did not finish naming the delete (Q346), and the teardown
/// deletes the launched runtimes with the rest.
/// What must NOT happen: a launch opened after the acceptance, the growth resumed, or its runtime left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_delete_accepted_mid_launch_lets_the_launch_end_and_launches_nothing_after() {
    let mut estate = estate_with("a_fabric_delete_accepted_mid_launch_lets_the_launch_end_and_launches_nothing_after").await;
    let ids = ids_of(&estate).await;
    let faults = FaultClient::at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).await;
    faults.arm("launch", json!({"kind": "step-event", "step": "DeployRuntime", "operation": "node.create", "at": Estate::path("mesh1.node", "1")})).await;
    let grown = [MeshDesired::of("mesh1", [("node-admin", 1), ("node", 2)])];
    let mut grow = estate.submit_named("mesh1 grown to two nodes", |current, executables| current.resized(&grown, false, executables)).await;
    let grow_id = grow.build_id().to_string();
    let held = faults.wait_held("launch").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(grow_id.as_str()), "the growth is held inside mesh1.node.1's launch: {held}");

    let mut delete = delete_admitted(&estate).await;
    let accepted_ns = now_ns();
    let id = delete.build_id().to_string();
    faults.release("launch").await;
    let ended = terminal_of("the interrupted growth", &mut grow, SETTLE).await;
    match ended.last() {
        Some(BuildFrame::Incomplete { sections }) => assert!(!sections.is_empty() && sections.iter().all(|s| s.reason.contains(&id)), "the growth's run receives Incomplete, each unfinished section naming the delete {id}: {sections:?}"),
        other => panic!("the interrupted growth ended {other:?}, not Incomplete naming the delete: {ended:?}"),
    }
    let frames = terminal_of("fabric.delete", &mut delete, SETTLE).await;
    assert!(matches!(frames.last(), Some(BuildFrame::Complete { .. })), "fabric.delete {id} completes: {frames:?}");

    let grown = receipts_at(&estate, &Estate::path("mesh1.node-admin", "1"), &grow_id).await;
    incomplete_naming(&grown, &id);
    let launched_id = build_log::created_id(&estate, &grow_id, &Estate::path("mesh1.node", "1")).await;
    assert_eq!(count(&grown, |o| of_id(o, "node.create", launched_id.as_str()), "DeployRuntime"), 1, "the admitted launch's evidence is retained: {:?}", listed(&grown));
    assert_eq!(count(&grown, |o| of_id(o, "node.create", launched_id.as_str()), "Complete"), 1, "the admitted launch ended within its bound: {:?}", listed(&grown));
    let receipts = receipts_at(&estate, &Estate::path("mesh1.node-admin", "1"), &id).await;
    assert_eq!(count(&receipts, |o| action_of(o) == "mesh.delete", rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh1 is deleted: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| action_of(o) == "fabric.delete", "Complete"), 1, "fabric.deleted is reported: {:?}", listed(&receipts));
    assert_eq!(estate.live_runtimes(), vec![], "the launched runtimes are deleted with the fabric");
    estate.stop().await;
    let spans = estate.spans();
    let creates = named(&spans, "rdm.node_admin.node.create.via-build");
    assert!([Estate::path("mesh1.node", "1"), Estate::path("mesh1.node", "2")].iter().all(|n| creates.iter().any(|sp| rdm_test::estate::at_location(sp, n))), "both creates were opened, before the acceptance: {creates:?}");
    let opened: Vec<&Value> = creates.into_iter().filter(|sp| start(sp) >= accepted_ns).collect();
    assert!(opened.is_empty(), "no create opens after the acceptance: {opened:?}");
}

/// CONTRACT (build.md §1: "its acceptance record is the durable intent ... a successor fabric-primary
/// reads the delete as the current Build and continues the teardown"; "the interrupted Build opens no
/// further operation and is never resumed"; fabric-recovery.md step 4: "Under an in-force fabric.delete
/// ... the workflow re-creates and restores nothing ... the executor holds the fabric-primary's roles to
/// the end and continues the delete"): on a two-mesh fabric (two node-admins in mesh1), the
/// fabric-primary's growth of mesh1 is held by its fault service inside mesh1.node.2's launch, fabric.delete
/// is accepted, and once mesh1.node-admin.2's log holds the acceptance the fabric-primary is killed. Fabric
/// recovery's executor, mesh1.node-admin.2, holds the fabric-primary's roles and reads the delete as current:
/// the growth's run ends once its executor's exit is read and reads `incomplete` naming the delete,
/// mesh1.node.2's create is never run again, and its launched runtime, a birth of mesh1's roster by its
/// claim, is deleted with the fabric under its own node.delete receipt (fabric-delete.md "Receipts"),
/// or, when it ended on its own before the roster was captured, is proven exited by the provider's find
/// and is no birth of the roster; the fabric reports fabric.deleted. What must NOT happen: the growth resumed, the lost fabric-primary
/// re-created (no node.create after the acceptance), or the outstanding runtime left running or left to
/// exit on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fabric_primary_lost_after_the_delete_is_accepted_with_a_create_outstanding_is_continued_by_its_successor() {
    let shape = [("mesh1", 2, 1), ("mesh2", 1, 1)];
    let mut estate = ready_fabric("the_fabric_primary_lost_after_the_delete_is_accepted_with_a_create_outstanding_is_continued_by_its_successor", &shape).await;
    let ids = ids_of(&estate).await;
    let faults = FaultClient::at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).await;
    faults.arm("launch", json!({"kind": "step-event", "step": "DeployRuntime", "operation": "node.create", "at": Estate::path("mesh1.node", "2")})).await;
    let grow = estate.submit_named("one more node in mesh1", |current, executables| current.with_one_more("mesh1", "node", executables)).await;
    let grow_id = grow.build_id().to_string();
    let held = faults.wait_held("launch").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(grow_id.as_str()), "the growth is held inside mesh1.node.2's launch: {held}");
    // mesh1.node.2's runtime is the one live runtime of no settled node.
    let launched: Vec<u32> = estate.live_runtimes().into_iter().filter(|(dir, _)| !ids.values().any(|id| of_node(dir, id))).map(|(_, pid)| pid).collect();
    assert_eq!(launched.len(), 1, "mesh1.node.2's runtime is launched: {:?}", estate.live_runtimes());

    let accepted_ns = now_ns();
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    build_log::build_fact(&estate, &id, "the delete's acceptance", SETTLE, |_| true).await.unwrap_or_else(|end| panic!("{end}"));
    drop(grow);
    estate.kill_bootstrap();
    let fp = delete_followed(&estate, delete, &Estate::path("mesh1.node-admin", "2")).await;

    let grown = receipts_at(&estate, &fp.0, &grow_id).await;
    incomplete_naming(&grown, &id);
    let outstanding = build_log::created_id(&estate, &grow_id, &Estate::path("mesh1.node", "2")).await;
    assert_eq!(count(&grown, |o| of_id(o, "node.create", outstanding.as_str()), "Complete"), 0, "the outstanding create is never run again: {:?}", listed(&grown));
    let receipts = receipts_at(&estate, &fp.0, &id).await;
    torn_down(&receipts, 2, &id_in(&ids, "mesh2.node-admin", "1"));
    // mesh1.node.2's node id is the one its create launched it under: its own boot names it (the claim's
    // span is the lost fabric-primary's, whose evidence the kill may cut).
    let rpc2 = named(&estate.spans(), "rdm.mesh.node.create.via-boot").into_iter().find(|sp| rdm_test::estate::at_location(sp, &Estate::path("mesh1.node", "2"))).and_then(|sp| sp["attributes"]["node_id"].as_str().map(String::from)).unwrap_or_else(|| panic!("mesh1.node.2's boot names its node id"));
    let deleted = count(&receipts, |o| of_id(o, "node.delete", &rpc2), "Complete");
    let left: Vec<u32> = estate.live_runtimes().into_iter().map(|(_, pid)| pid).collect();
    assert!(!left.contains(&launched[0]), "the outstanding runtime of mesh1.node.2 is captured and deleted: {left:?}");
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !of_node(dir, &fp.1)).collect::<Vec<_>>(), vec![], "only the fabric-primary {fp:?} is left, for its separate node.delete");
    estate.stop().await;
    let spans = estate.spans();
    // The launch never joins (its launcher is lost holding DeployRuntime), and the runtime may end on its
    // own before mesh1's roster is captured: then the provider's find proves it exited and no runtime is
    // left for the delete (deployment-provider.md §2, "Find is for the launcher's successor").
    let gone = named(&spans, "rdm.node_admin.mesh.resolve.via-claimed-birth-gone").into_iter().filter(|sp| sp["attributes"]["member"] == rpc2.as_str() && sp["attributes"]["found"] == "exited").count();
    assert_eq!(deleted + gone, 1, "the outstanding birth of mesh1.node.2, claimed and launched before the acceptance, is either a birth of mesh1's roster whose delete is receipted once ({deleted}) or a launch the provider proves exited at the capture ({gone}): {:?}", listed(&receipts));
    nothing_recreated_under_the_delete(&spans, accepted_ns, &fp);
}

/// CONTRACT (lifecycles/fabric-delete.md "Numbered end-to-end sequence"; build.md §1: "a successor
/// fabric-primary reads the delete as the current Build and continues the teardown"; fabric-delete.md
/// "Failure": "A fabric-primary lost mid-delete is fabric recovery's: its executor continues the delete as
/// fabric-primary and re-creates nothing"): on a two-mesh fabric, fabric.delete's executor (the
/// fabric-primary) is held by its fault service at mesh2's primary's node.delete, after mesh2 is deleted, and
/// killed. Fabric recovery's executor in mesh1 holds the fabric-primary's roles and continues the delete:
/// mesh2's primary is deleted once, its row written by that executor, mesh1 is deleted, and fabric.deleted
/// is reported. What must NOT happen: a mesh deleted twice, the lost fabric-primary re-created, the
/// teardown left unfinished, or a runtime left beside the fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_delete_executor_killed_after_the_first_mesh_is_finished_by_fabric_recoverys_executor() {
    let shape = [("mesh1", 2, 1), ("mesh2", 1, 1)];
    let mut estate = ready_fabric("the_delete_executor_killed_after_the_first_mesh_is_finished_by_fabric_recoverys_executor", &shape).await;
    let ids = ids_of(&estate).await;
    let faults = FaultClient::at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).await;
    faults.arm("first-mesh", json!({"kind": "step-event", "step": "NodeDeleted", "operation": "node.delete", "node": delete_subject(&id_in(&ids, "mesh2.node-admin", "1"))})).await;
    let accepted_ns = now_ns();
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let held = faults.wait_held("first-mesh").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(id.as_str()), "the delete is held at mesh2's primary's node.delete: {held}");
    build_log::build_fact(&estate, &id, "mesh2's MeshDeleted", SETTLE, |b| {
        b.steps.iter().any(|r| action_of(&r.operation) == "mesh.delete" && r.step == rdm_admin::mesh_delete::STEP_MESH_DELETED)
    })
    .await
    .unwrap_or_else(|end| panic!("{end}"));
    estate.kill_bootstrap();
    let fp = delete_followed(&estate, delete, &Estate::path("mesh1.node-admin", "2")).await;

    let receipts = receipts_at(&estate, &fp.0, &id).await;
    torn_down(&receipts, 2, &id_in(&ids, "mesh2.node-admin", "1"));
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !of_node(dir, &fp.1)).collect::<Vec<_>>(), vec![], "only the fabric-primary {fp:?} is left, for its separate node.delete");
    estate.stop().await;
    nothing_recreated_under_the_delete(&estate.spans(), accepted_ns, &fp);
}

/// CONTRACT (Q159; fabric-recovery.md "Nothing alive": "A fabric.delete in force when no node-admin was
/// alive is the current Build (build.md §1): the adoption launch is created at a new path.name of the
/// node-admin type in an existing, not-yet-deleted mesh, never at the lost fabric-primary's, which the
/// delete removes; this is the one create a delete admits, and ordinary recovery creates none. It takes by
/// one recorded transition the fabric-primary role, the lost mesh's mesh-primary role and its own mesh's,
/// continues the teardown, reports fabric.deleted while alive, and is then deleted by an outside caller"):
/// a one-mesh fabric's sole node-admin, the fabric-primary, accepts fabric.delete and is held by its fault
/// fault service at mesh1's mesh.delete, its members still serving, and is killed: every admin is lost during the
/// delete. A person launches a node-admin at mesh1.node-admin.2, a new path.name in mesh1, over a new data
/// dir, handed the fabric id, mesh1, the store root and the custody. Its fabric recovery holds the
/// fabric-primary's roles under the delete by one recorded transition, its drive finishes the delete from
/// the delete's events (mesh1 deleted once, fabric.deleted reported), and it is the one runtime left, for
/// the outside caller's node.delete. What must NOT happen: the launch refused while nothing else can finish
/// the delete, a create after the delete's acceptance other than the launch's own claim of its path.name,
/// a second fabric, or a member's runtime left running beside the adopted fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sole_admin_lost_mid_delete_is_adopted_by_a_launch_at_a_new_path_name_that_finishes_the_delete() {
    let shape = [("mesh1", 1, 2)];
    let mut estate = ready_fabric("the_sole_admin_lost_mid_delete_is_adopted_by_a_launch_at_a_new_path_name_that_finishes_the_delete", &shape).await;
    let ids = ids_of(&estate).await;
    let mesh_id = estate.mesh_view("mesh1").await.id.map(|i| i.to_string()).expect("mesh1's id");
    let faults = FaultClient::at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).await;
    faults.arm("roster", json!({"kind": "step-event", "step": rdm_admin::mesh_delete::STEP_ACCEPTED_ROSTER, "operation": "mesh.delete", "node": {"Mesh": mesh_id}})).await;
    let accepted_ns = now_ns();
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let held = faults.wait_held("roster").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(id.as_str()), "the delete is held at mesh1's mesh.delete: {held}");
    let custody = rdm_test::test_certs::custody_of(&estate.bootstrap_data_dir("mesh1"), "mesh1");
    let store = rdm_test::estate::estate_store_dir(&estate.root);
    let fabrics_before: Vec<String> = std::fs::read_dir(&store).unwrap().flatten().map(|e| e.file_name().to_string_lossy().to_string()).collect();
    estate.kill_bootstrap();

    let mut env: Vec<(&str, String)> = vec![("RDM_NODE_PATH", "mesh1.node-admin".into()), ("RDM_NODE_NAME", "2".into()), ("RDM_FABRIC_ID", estate.fabric_id.clone()), ("RDM_MESH", "mesh1".into())];
    env.extend(custody);
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let data_dir = estate.root.join("adopted-mesh1.node-admin.2");
    let adopted = estate.restart_admin_with(&data_dir, &env);
    assert_eq!(adopted, Estate::path("mesh1.node-admin", "2"), "the launch is born at its new path.name");
    estate.enter(&adopted).await;
    let fp = delete_followed(&estate, delete, &Estate::path("mesh1.node-admin", "2")).await;
    assert_eq!(fp.0, Estate::path("mesh1.node-admin", "2"), "the adoption launch is the fabric-primary that finishes the delete");
    let receipts = receipts_at(&estate, &fp.0, &id).await;
    assert_eq!(count(&receipts, |o| action_of(o) == "mesh.delete", rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh1 is deleted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| action_of(o) == "fabric.delete", "Complete"), 1, "fabric.deleted is reported: {:?}", listed(&receipts));
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !dir.starts_with(&data_dir)).collect::<Vec<_>>(), vec![], "only the adopted fabric-primary {fp:?} is left, for its separate node.delete");
    let fabrics_after: Vec<String> = std::fs::read_dir(&store).unwrap().flatten().map(|e| e.file_name().to_string_lossy().to_string()).collect();
    assert_eq!(fabrics_after, fabrics_before, "no second fabric on the store");
    estate.stop().await;

    let spans = estate.spans();
    let taken = held_roles(&spans, &id, accepted_ns);
    assert!(taken.len() == 1 && rdm_test::estate::names_location(taken[0], "node", &Estate::path("mesh1.node-admin", "2")), "the launch's fabric recovery holds the fabric-primary's roles under the delete by one recorded transition: {taken:#?}");
    // The one create a delete admits: the launch's own claim of its new path.name.
    let created: Vec<&Value> = spans
        .iter()
        .filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_admin.node.create.")) && sp["attributes"]["moment"] != "arrival" && start(sp) >= accepted_ns)
        .filter(|sp| !(sp["attributes"]["moment"] == "create" && rdm_test::estate::names_location(sp, "subject", &Estate::path("mesh1.node-admin", "2"))))
        .collect();
    assert!(created.is_empty(), "nothing is created after the delete's acceptance but the launch itself: {created:?}");
}

/// CONTRACT (Q160; fabric-recovery.md step 4: "The executor takes the fabric-primary role and the lost
/// mesh's mesh-primary role by one recorded transition at the next revision, keeping its own mesh's ...
/// its own mesh is deleted last, it reports fabric.deleted alive, and an outside caller deletes it";
/// mesh-delete.md "Resume after the primary's loss" step 2: "Under an in-force fabric.delete no
/// node-admin is created: the mesh is deleted in process at the executor holding the fabric-primary's
/// roles"): on a two-mesh fabric whose mesh1 holds one node-admin, the fabric-primary, fabric.delete's
/// executor is held by its fault service at mesh2's mesh.delete and killed: mesh1 is left with no node-admin.
/// From the backbone, mesh2's primary runs fabric recovery, records the transition and holds the fabric-primary role and mesh1's mesh-primary role; it deletes mesh1 in process
/// (its serving member by node.delete, the lost fabric-primary as an unreached birth), then its own mesh2
/// last, and reports fabric.deleted, the one runtime left for the outside caller's node.delete. What must
/// NOT happen: the backbone writing nothing and the delete left unfinished, a node-admin created for mesh1,
/// mesh2 deleted before mesh1, or a member's runtime left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_backbone_executor_takes_both_roles_and_deletes_the_admin_less_mesh_in_process() {
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1)];
    let mut estate = ready_fabric("the_backbone_executor_takes_both_roles_and_deletes_the_admin_less_mesh_in_process", &shape).await;
    let ids = ids_of(&estate).await;
    let mesh2_id = estate.mesh_view("mesh2").await.id.map(|i| i.to_string()).expect("mesh2's id");
    let faults = FaultClient::at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).await;
    faults.arm("roster", json!({"kind": "step-event", "step": rdm_admin::mesh_delete::STEP_ACCEPTED_ROSTER, "operation": "mesh.delete", "node": {"Mesh": mesh2_id}})).await;
    let accepted_ns = now_ns();
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let held = faults.wait_held("roster").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(id.as_str()), "the delete is held at mesh2's mesh.delete: {held}");
    estate.kill_bootstrap();

    let fp = delete_followed(&estate, delete, &Estate::path("mesh2.node-admin", "1")).await;
    assert_eq!(fp.0, Estate::path("mesh2.node-admin", "1"), "the backbone executor is the fabric-primary that finishes the delete");
    let receipts = receipts_at(&estate, &fp.0, &id).await;
    assert_eq!(count(&receipts, |o| action_of(o) == "mesh.delete", rdm_admin::mesh_delete::STEP_MESH_DELETED), 2, "each mesh is deleted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| action_of(o) == "fabric.delete", "Complete"), 1, "fabric.deleted is reported: {:?}", listed(&receipts));
    let deleted: Vec<String> = steps_of(&receipts).into_iter().filter(|(_, s, _)| s == rdm_admin::mesh_delete::STEP_MESH_DELETED).map(|(o, _, _)| o).collect();
    assert!(deleted.last().is_some_and(|o| on_mesh(o, "mesh.delete", &mesh2_id)), "its own mesh2 is deleted last: {deleted:?}");
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !of_node(dir, &fp.1)).collect::<Vec<_>>(), vec![], "only the fabric-primary {fp:?} is left, for its separate node.delete");
    estate.stop().await;
    let spans = estate.spans();
    let taken = held_roles(&spans, &id, accepted_ns);
    assert!(taken.len() == 1 && rdm_test::estate::names_location(taken[0], "node", &Estate::path("mesh2.node-admin", "1")), "one recorded transition, at the backbone executor: {taken:#?}");
    nothing_recreated_under_the_delete(&spans, accepted_ns, &fp);
}

/// CONTRACT (fabric-delete.md step 1: "A mesh whose mesh-primary does not answer its mesh.delete is
/// reached the way an unheard mesh-primary is reached in a live fabric (node investigation: its exact
/// birth, direct then via peers of its mesh), and during fabric.delete the outcome is a delete, never a
/// node.create of a node-admin"; mesh-delete.md "Resume after the primary's loss" step 2: "Under an
/// in-force fabric.delete no node-admin is created: the mesh is deleted in process at the executor
/// holding the fabric-primary's roles"): on a two-mesh fabric whose mesh2 holds one node-admin, that
/// admin's runtime is killed and fabric.delete is admitted at once, its delete-mesh to mesh2's primary
/// sent while the fabric-primary's view still holds that primary live. The call ends with no reply; node
/// investigation of the primary's exact birth does not reach it; mesh2 is deleted in process at the
/// fabric-primary (its serving member by node.delete, its lost admin as an unreached birth), then the
/// fabric-primary's own mesh1 last, and fabric.deleted is reported. What must NOT happen: the delete
/// ending incomplete on the unanswered call, a node-admin created for mesh2, a mesh deleted twice, or a
/// member's runtime left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_that_does_not_answer_its_mesh_delete_is_investigated_and_its_mesh_deleted_in_process() {
    let shape = [("mesh1", 1, 1), ("mesh2", 1, 1)];
    let mut estate = ready_fabric("a_mesh_primary_that_does_not_answer_its_mesh_delete_is_investigated_and_its_mesh_deleted_in_process", &shape).await;
    let ids = ids_of(&estate).await;
    let (mesh1_id, mesh2_id) = (estate.mesh_view("mesh1").await.id.map(|i| i.to_string()).expect("mesh1's id"), estate.mesh_view("mesh2").await.id.map(|i| i.to_string()).expect("mesh2's id"));
    let (admin2, node2) = (id_in(&ids, "mesh2.node-admin", "1"), id_in(&ids, "mesh2.node", "1"));
    let fp = (Estate::path("mesh1.node-admin", "1"), id_in(&ids, "mesh1.node-admin", "1"));
    let accepted_ns = now_ns();
    estate.kill_pid(runtime_pid(&estate, &admin2));
    let mut delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let frames = terminal_of("fabric.delete", &mut delete, SETTLE).await;
    assert!(matches!(frames.last(), Some(BuildFrame::Complete { .. })), "fabric.delete {id} completes: {frames:?}");
    let receipts = receipts_at(&estate, &fp.0, &id).await;
    assert_eq!(count(&receipts, |o| on_mesh(o, "mesh.delete", &mesh2_id), rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh2 is deleted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| on_mesh(o, "mesh.delete", &mesh1_id), rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh1 is deleted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| of_id(o, "node.delete", &admin2), "Complete"), 1, "mesh2's lost admin has its node.delete receipt: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| of_id(o, "node.delete", &node2), "Complete"), 1, "mesh2's serving member has its node.delete receipt: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| action_of(o) == "fabric.delete", "Complete"), 1, "fabric.deleted is reported: {:?}", listed(&receipts));
    let deleted: Vec<String> = steps_of(&receipts).into_iter().filter(|(_, s, _)| s == rdm_admin::mesh_delete::STEP_MESH_DELETED).map(|(o, _, _)| o).collect();
    assert!(deleted.last().is_some_and(|o| on_mesh(o, "mesh.delete", &mesh1_id)), "the fabric-primary's own mesh1 is deleted last: {deleted:?}");
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !of_node(dir, &fp.1)).collect::<Vec<_>>(), vec![], "only the fabric-primary {fp:?} is left, for its separate node.delete");
    estate.stop().await;
    let spans = estate.spans();
    // The one investigation of the unanswered primary decided the in-process delete.
    let investigated: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.resolve.via-primary-investigation").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh2" && sp["attributes"]["primary_node_id"] == admin2.as_str()).collect();
    assert!(investigated.len() == 1 && investigated[0]["attributes"]["verdict"] != "reached", "the unanswered primary is investigated once and not reached: {investigated:#?}");
    let in_process: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.delete.via-in-process").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh2").collect();
    assert!(in_process.len() == 1 && in_process[0]["attributes"]["outcome"] == "deleted", "mesh2 is deleted in process at the fabric-primary: {in_process:#?}");
    let created: Vec<&Value> = spans.iter().filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_admin.node.create.")) && sp["attributes"]["moment"] != "arrival" && start(sp) >= accepted_ns).collect();
    assert!(created.is_empty(), "nothing is created after the delete's acceptance: {created:?}");
}

/// The bound of the fabric-primary's own node.delete: the stop's (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`,
/// 5 s by default, and the 10 s the mesh-primary allows a stop's reply past it), which the termination of
/// a process runtime and its inspection fit inside.
const OWN_DELETE_BOUND: Duration = Duration::from_secs(15);

/// Whether `pid` is a running process (a zombie has exited).
fn running(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

/// CONTRACT (lifecycles/fabric-delete.md "After completion": "A separate `node.delete` of the
/// fabric-primary, called from outside the fabric; its `node.deleted` is its last reply and the caller
/// takes its word"; step 4; node-delete.md step 1: "For a live process compose the whole node.stop"):
/// a one-mesh fabric's fabric-primary, its sole node-admin, completes fabric.delete and reports
/// fabric.deleted alive. An outside caller's node.delete of it is accepted and executed by that
/// fabric-primary itself: its stop holds the admin's tasks and runs to its cut while the run that
/// commanded it waits on it, the delete answers node.deleted, and the runtime has exited, all inside
/// the stop's bound. What must NOT happen: the stop waiting on the run that waits on it (the StopNode
/// step cancelled at its reply budget, TerminateRuntime never reached), or the runtime left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fabric_primary_left_after_fabric_deleted_is_deleted_by_an_outside_node_delete_inside_the_stop_bound() {
    let shape = [("mesh1", 1, 1)];
    let mut estate = ready_fabric("the_fabric_primary_left_after_fabric_deleted_is_deleted_by_an_outside_node_delete_inside_the_stop_bound", &shape).await;
    let ids = ids_of(&estate).await;
    let mut delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let frames = terminal_of("fabric.delete", &mut delete, SETTLE).await;
    assert!(matches!(frames.last(), Some(BuildFrame::Complete { .. })), "fabric.delete {id} completes: {frames:?}");
    assert_eq!(estate.live_runtimes(), vec![], "only the fabric-primary is left, for its separate node.delete");
    let pid = estate.bootstrap_pid().expect("the fabric-primary mesh1.node-admin.1 is the bootstrap admin");
    assert!(running(pid), "the fabric-primary reported fabric.deleted alive");

    let mut own = estate.delete_node(&Estate::path("mesh1.node-admin", "1")).await.unwrap_or_else(|end| panic!("the outside node.delete of the fabric-primary was refused: {end}"));
    let begun = tokio::time::Instant::now();
    let replies = tokio::time::timeout(OWN_DELETE_BOUND, async {
        let mut replies = Vec::new();
        while let Some(frame) = own.next().await {
            match frame {
                Ok(frame) => {
                    let end = frame.is_terminal();
                    replies.push(format!("{frame:?}"));
                    if end {
                        break;
                    }
                }
                // node.deleted is its last reply: the exited process serves nothing after it.
                Err(end) => {
                    replies.push(format!("ended: {end}"));
                    break;
                }
            }
        }
        replies
    })
    .await
    .unwrap_or_else(|_| panic!("the fabric-primary's own node.delete answered no node.deleted within {OWN_DELETE_BOUND:?}"));
    assert!(replies.iter().any(|r| r == &format!("{:?}", rdm_client::Frame::Event(rdm_client::NodeEvent::Deleted))), "the fabric-primary's own node.delete answers node.deleted: {replies:?}");
    assert!(!replies.iter().any(|r| r.starts_with("Failed")), "no step of the fabric-primary's own node.delete failed: {replies:?}");
    while running(pid) && begun.elapsed() < OWN_DELETE_BOUND {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!running(pid), "the fabric-primary's runtime exited within {OWN_DELETE_BOUND:?} of its node.delete: {replies:?}");
    estate.stop().await;
}

/// The staleness floor of this run (`RDM_STALENESS_MS`, the gate's 3 s).
fn floor() -> Duration {
    Duration::from_millis(std::env::var("RDM_STALENESS_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(30_000))
}

/// The one running runtime of the node `id` (its data dir is named by its node id), read from the
/// host: the node id is the one the cell settled, so no admin has to answer a read for it.
fn runtime_pid(estate: &Estate, id: &str) -> u32 {
    let pids: Vec<u32> = estate.live_runtimes().into_iter().filter(|(dir, _)| of_node(dir, id)).map(|(_, pid)| pid).collect();
    assert_eq!(pids.len(), 1, "{id} has one running runtime: {:?}", estate.live_runtimes());
    pids[0]
}

/// Whether `pid` runs: a process whose state is not a zombie.
fn alive(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|s| !s.rsplit(')').next().unwrap_or("").trim_start().starts_with('Z'))
}

/// The environment of a person's adoption launch at `name`: the fabric id, its mesh, the custody.
fn adoption_env(estate: &Estate, at: &PathName, custody: &[(&'static str, String)]) -> Vec<(&'static str, String)> {
    let mut env: Vec<(&'static str, String)> = vec![("RDM_NODE_PATH", at.path.clone()), ("RDM_NODE_NAME", at.name.clone()), ("RDM_FABRIC_ID", estate.fabric_id.clone()), ("RDM_MESH", "mesh1".into())];
    env.extend(custody.iter().cloned());
    env
}

fn fabrics_on(store: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(store).unwrap().flatten().map(|e| e.file_name().to_string_lossy().to_string()).collect()
}

/// CONTRACT (Q159; fabric-recovery.md "Nothing alive": "If no not-yet-deleted mesh remains, the remaining
/// work is the dead fabric-primary's own node.delete from outside, and nothing is created"; fabric-delete.md
/// step 4: "A separate call from outside the fabric runs node.delete of the fabric-primary ... the caller
/// takes its word"): a one-mesh fabric's sole node-admin, the fabric-primary, deletes mesh1 under
/// fabric.delete; once mesh1's MeshDeleted is durable it is held by its fault service at fabric.delete's
/// Complete, before fabric.deleted, and killed. The delete is in force, no node-admin is
/// alive and no not-yet-deleted mesh remains. Two staleness floors on, nothing has been created and no
/// runtime of the fabric runs; a person's adoption launch at a new path.name in mesh1 is refused by name,
/// naming the outside caller's node.delete, and creates nothing; the store holds the one
/// fabric. The remaining work is the outside caller's node.delete of the dead fabric-primary, whose
/// runtime has exited. What must NOT happen: a node created after the delete's acceptance, an adoption
/// launch admitted, a recorded transition of the fabric-primary's roles, a second fabric, or a runtime
/// left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fabric_primary_lost_with_no_mesh_left_leaves_only_its_own_node_delete_from_outside_and_creates_nothing() {
    let shape = [("mesh1", 1, 1)];
    let mut estate = ready_fabric("the_fabric_primary_lost_with_no_mesh_left_leaves_only_its_own_node_delete_from_outside_and_creates_nothing", &shape).await;
    let ids = ids_of(&estate).await;
    let mesh_id = estate.mesh_view("mesh1").await.id.map(|i| i.to_string()).expect("mesh1's id");
    let faults = FaultClient::at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).await;
    faults.arm("deleted", json!({"kind": "step-event", "step": rdm_admin::mesh_delete::STEP_MESH_DELETED, "operation": "mesh.delete", "node": {"Mesh": mesh_id}})).await;
    let accepted_ns = now_ns();
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let held = faults.wait_held("deleted").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(id.as_str()), "the delete is held at mesh1's MeshDeleted: {held}");
    // mesh1's MeshDeleted made durable; the fabric-primary is held at fabric.delete's Complete, before
    // fabric.deleted is written.
    faults.arm("next", json!({"kind": "step-event", "step": "Complete", "operation": "fabric.delete"})).await;
    faults.release("deleted").await;
    let next = faults.wait_held("next").await;
    assert_eq!(next["hit"]["build_id"].as_str(), Some(id.as_str()), "the delete is held at fabric.delete's Complete: {next}");
    build_log::build_fact(&estate, &id, "mesh1's MeshDeleted", SETTLE, |b| {
        b.steps.iter().any(|r| on_mesh(&r.operation, "mesh.delete", &mesh_id) && r.step == rdm_admin::mesh_delete::STEP_MESH_DELETED)
    })
    .await
    .unwrap_or_else(|end| panic!("{end}"));
    let before = receipts_at(&estate, &Estate::path("mesh1.node-admin", "1"), &id).await;
    assert_eq!(count(&before, |o| action_of(o) == "fabric.delete", "Complete"), 0, "fabric.deleted is not reported yet: {:?}", listed(&before));
    let fp_pid = estate.bootstrap_pid().expect("the fabric-primary's pid");
    let custody = rdm_test::test_certs::custody_of(&estate.bootstrap_data_dir("mesh1"), "mesh1");
    let store = rdm_test::estate::estate_store_dir(&estate.root);
    let fabrics_before = fabrics_on(&store);
    drop(delete);
    estate.kill_bootstrap();

    // Nothing inside the fabric is left to create anything.
    tokio::time::sleep(floor() * 2).await;
    assert_eq!(estate.live_runtimes(), vec![], "no runtime of the fabric runs");
    // A person's adoption launch at a new path.name in the deleted mesh is refused by name.
    let env = adoption_env(&estate, &Estate::path("mesh1.node-admin", "2"), &custody);
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let (status, err) = estate.admin_launch_refused(&estate.root.join("adopted-mesh1.node-admin.2"), &env, SETTLE);
    assert!(!status.success(), "the adoption launch with no mesh left is refused: {status:?} {err}");
    assert!(
        err.contains("refusing to start") && err.contains("no not-yet-deleted mesh remains") && err.contains("node.delete from outside") && err.contains("nothing is created"),
        "the refusal names the outside caller's node.delete: {err}"
    );
    // The outside caller's node.delete of the dead fabric-primary takes its word: its runtime has exited,
    // and nothing else of the fabric is left to delete.
    assert!(!alive(fp_pid), "the dead fabric-primary's runtime has exited");
    assert_eq!(estate.live_runtimes(), vec![], "no runtime of the fabric runs beside it");
    assert_eq!(fabrics_on(&store), fabrics_before, "no second fabric on the store");
    estate.stop().await;

    let spans = estate.spans();
    let taken = held_roles(&spans, &id, accepted_ns);
    assert!(taken.is_empty(), "no transition of the fabric-primary's roles was recorded: {taken:#?}");
    let created: Vec<&Value> = spans.iter().filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_admin.node.create.")) && sp["attributes"]["moment"] != "arrival" && start(sp) >= accepted_ns).collect();
    assert!(created.is_empty(), "nothing is created after the delete's acceptance: {created:?}");
}

/// CONTRACT (Q160; fabric-recovery.md step 4: "The executor takes the fabric-primary role and the lost
/// mesh's mesh-primary role by one recorded transition at the next revision, keeping its own mesh's ...
/// its own mesh is deleted last, it reports fabric.deleted alive"; "If the executor is
/// lost, the next one in ..." the workflow; fabric-delete.md "Failure": "Once a runtime's deletion receipt
/// is durable, nothing recovers it for being absent"): on a two-mesh fabric whose mesh1 holds one
/// node-admin, the fabric-primary, and mesh2 two, fabric.delete's executor is held by its fault service at
/// mesh2's mesh.delete and killed. From the backbone, mesh2.node-admin.1 takes both roles and deletes mesh1 in
/// process; it is held by its own fault service once mesh1's last member is deleted (its NodeDeleted durable,
/// its delete's Complete not), before mesh1's row is written Dead or its MeshDeleted, and killed. mesh2.node-admin.2, the one node-admin left, records its own
/// transition and finishes the teardown: mesh1 deleted once, then its own mesh2 last, fabric.deleted
/// reported, the one runtime left for the outside caller's node.delete. What must NOT happen: the teardown
/// left unfinished, a mesh deleted twice, a node created after the acceptance, or a member's runtime left
/// running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_backbone_executor_lost_midway_through_the_teardown_is_followed_by_the_next_which_finishes_it() {
    let shape = [("mesh1", 1, 1), ("mesh2", 2, 1)];
    let mut estate = ready_fabric("the_backbone_executor_lost_midway_through_the_teardown_is_followed_by_the_next_which_finishes_it", &shape).await;
    let ids = ids_of(&estate).await;
    let mesh1_id = estate.mesh_view("mesh1").await.id.map(|i| i.to_string()).expect("mesh1's id");
    let mesh2_id = estate.mesh_view("mesh2").await.id.map(|i| i.to_string()).expect("mesh2's id");
    let faults = FaultClient::at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).await;
    faults.arm("roster", json!({"kind": "step-event", "step": rdm_admin::mesh_delete::STEP_ACCEPTED_ROSTER, "operation": "mesh.delete", "node": {"Mesh": mesh2_id}})).await;
    let executor = FaultClient::at(&estate, &rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).await;
    executor.arm("midway", json!({"kind": "step-event", "step": "Complete", "node": delete_subject(&id_in(&ids, "mesh1.node", "1"))})).await;
    let accepted_ns = now_ns();
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let held = faults.wait_held("roster").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(id.as_str()), "the delete is held at mesh2's mesh.delete: {held}");
    estate.kill_bootstrap();

    let midway = executor.wait_held("midway").await;
    assert_eq!(midway["hit"]["build_id"].as_str(), Some(id.as_str()), "the backbone executor is held midway, at the Complete of mesh1's last member's delete: {midway}");
    // The transition is on the primary records: the held executor's own view names it the fabric-primary.
    let first = estate.fabric_view_at(&Estate::path("mesh2.node-admin", "1")).await.and_then(|v| v.fabric_primary).map(|p| p.to_string());
    assert_eq!(first, Some(id_in(&ids, "mesh2.node-admin", "1")), "the backbone executor recorded its transition: it holds the fabric-primary role");
    estate.kill_pid(runtime_pid(&estate, &id_in(&ids, "mesh2.node-admin", "1")));

    let fp = delete_followed(&estate, delete, &Estate::path("mesh2.node-admin", "2")).await;
    assert_eq!(fp.0, Estate::path("mesh2.node-admin", "2"), "the next executor is the fabric-primary that finishes the delete");
    let receipts = receipts_at(&estate, &fp.0, &id).await;
    assert_eq!(count(&receipts, |o| of_id(o, "node.delete", &id_in(&ids, "mesh1.node", "1")), "Complete"), 1, "mesh1.node.1's delete is finished once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| of_id(o, "node.delete", &id_in(&ids, "mesh2.node-admin", "1")), "Complete"), 1, "the dead executor mesh2.node-admin.1, a member of mesh2 not yet Deleted, has its delete receipted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| on_mesh(o, "mesh.delete", &mesh1_id), rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh1 is deleted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| on_mesh(o, "mesh.delete", &mesh2_id), rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh2 is deleted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| action_of(o) == "fabric.delete", "Complete"), 1, "fabric.deleted is reported: {:?}", listed(&receipts));
    let deleted: Vec<String> = steps_of(&receipts).into_iter().filter(|(_, s, _)| s == rdm_admin::mesh_delete::STEP_MESH_DELETED).map(|(o, _, _)| o).collect();
    assert!(deleted.last().is_some_and(|o| on_mesh(o, "mesh.delete", &mesh2_id)), "the executor's own mesh2 is deleted last: {deleted:?}");
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !of_node(dir, &fp.1)).collect::<Vec<_>>(), vec![], "only the fabric-primary {fp:?} is left, for its separate node.delete");
    estate.stop().await;
    let spans = estate.spans();
    let taken: Vec<&Value> = held_roles(&spans, &id, accepted_ns).into_iter().filter(|sp| rdm_test::estate::names_location(sp, "node", &Estate::path("mesh2.node-admin", "2"))).collect();
    assert_eq!(taken.len(), 1, "the next executor recorded its own transition: {taken:#?}");
    nothing_recreated_under_the_delete(&spans, accepted_ns, &fp);
}

/// CONTRACT (Q159; fabric-recovery.md "Nothing alive": "the adoption launch is created at a new path and name
/// of the node-admin type in an existing, not-yet-deleted mesh ... It takes by one recorded transition the
/// fabric-primary role, the lost mesh's mesh-primary role and its own mesh's, continues the teardown, reports
/// fabric.deleted while alive"; "a provider's exit evidence releases no mesh row by itself"): a one-mesh
/// fabric's sole node-admin, the fabric-primary, accepts
/// fabric.delete, is held at mesh1's mesh.delete and killed. A person's adoption launch at mesh1.node-admin.2
/// takes over and deletes mesh1's members; it is held by a boot cut once mesh1.node.2's runtime is
/// terminated, before its NodeDeleted is durable, and killed, so again no node-admin is alive. A second launch at mesh1.node-admin.3 records its own transition and
/// finishes it: mesh1.node.2's delete receipted once, mesh1 deleted once, fabric.deleted reported, the one runtime left for the outside caller's
/// node.delete, one fabric on the store. What must NOT happen: the second launch refused while mesh1 is not
/// yet deleted, a mesh deleted twice, a create after the acceptance other than each launch's claim of its
/// own path.name, a second fabric, or a member's runtime left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_adoption_launch_lost_midway_through_the_teardown_is_followed_by_the_next_launch_which_finishes_it() {
    let shape = [("mesh1", 1, 2)];
    let mut estate = ready_fabric("an_adoption_launch_lost_midway_through_the_teardown_is_followed_by_the_next_launch_which_finishes_it", &shape).await;
    let ids = ids_of(&estate).await;
    let mesh_id = estate.mesh_view("mesh1").await.id.map(|i| i.to_string()).expect("mesh1's id");
    let faults = FaultClient::at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).await;
    faults.arm("roster", json!({"kind": "step-event", "step": rdm_admin::mesh_delete::STEP_ACCEPTED_ROSTER, "operation": "mesh.delete", "node": {"Mesh": mesh_id}})).await;
    let accepted_ns = now_ns();
    let delete = delete_admitted(&estate).await;
    let id = delete.build_id().to_string();
    let held = faults.wait_held("roster").await;
    assert_eq!(held["hit"]["build_id"].as_str(), Some(id.as_str()), "the delete is held at mesh1's mesh.delete: {held}");
    let custody = rdm_test::test_certs::custody_of(&estate.bootstrap_data_dir("mesh1"), "mesh1");
    let store = rdm_test::estate::estate_store_dir(&estate.root);
    let fabrics_before = fabrics_on(&store);
    // The first launch is held, from its first line, once its teardown has terminated mesh1.node.2's runtime.
    let faults = estate.root.join("faults");
    std::fs::create_dir_all(&faults).unwrap();
    let midway = json!([{"id": "midway", "kind": "step-event", "step": "NodeDeleted", "node": delete_subject(&id_in(&ids, "mesh1.node", "2"))}]);
    std::fs::write(rdm_test::admin_faults::boot_cuts_file(&estate.root, &rdm_core::PathName::new("mesh1.node-admin", "2").unwrap()), serde_json::to_vec(&midway).unwrap()).unwrap();
    estate.kill_bootstrap();

    let env = adoption_env(&estate, &Estate::path("mesh1.node-admin", "2"), &custody);
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let first = estate.restart_admin_with(&estate.root.join("adopted-mesh1.node-admin.2"), &env);
    assert_eq!(first, Estate::path("mesh1.node-admin", "2"), "the first launch is born at its new path.name");
    estate.enter(&first).await;
    let first_faults = FaultClient::at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "2").unwrap()).await;
    let at = first_faults.wait_held("midway").await;
    assert_eq!(at["hit"]["build_id"].as_str(), Some(id.as_str()), "the first launch is held midway, at mesh1.node.2's NodeDeleted: {at}");
    let first_pid = *estate.restarted_pids().last().expect("the first launch's pid");
    estate.kill_pid(first_pid);

    let env = adoption_env(&estate, &Estate::path("mesh1.node-admin", "3"), &custody);
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let data_dir = estate.root.join("adopted-mesh1.node-admin.3");
    let second = estate.restart_admin_with(&data_dir, &env);
    assert_eq!(second, Estate::path("mesh1.node-admin", "3"), "the second launch is born at its new path.name");
    estate.enter(&second).await;
    let fp = delete_followed(&estate, delete, &Estate::path("mesh1.node-admin", "3")).await;
    assert_eq!(fp.0, Estate::path("mesh1.node-admin", "3"), "the second launch is the fabric-primary that finishes the delete");
    let receipts = receipts_at(&estate, &fp.0, &id).await;
    assert_eq!(count(&receipts, |o| of_id(o, "node.delete", &id_in(&ids, "mesh1.node", "2")), "NodeDeleted"), 1, "mesh1.node.2's delete is receipted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| action_of(o) == "mesh.delete", rdm_admin::mesh_delete::STEP_MESH_DELETED), 1, "mesh1 is deleted once: {:?}", listed(&receipts));
    assert_eq!(count(&receipts, |o| action_of(o) == "fabric.delete", "Complete"), 1, "fabric.deleted is reported: {:?}", listed(&receipts));
    assert_eq!(estate.live_runtimes().into_iter().filter(|(dir, _)| !dir.starts_with(&data_dir)).collect::<Vec<_>>(), vec![], "only the second launch {fp:?} is left, for its separate node.delete");
    assert!(!alive(first_pid), "the lost first launch is not running");
    assert_eq!(fabrics_on(&store), fabrics_before, "no second fabric on the store");
    estate.stop().await;

    let spans = estate.spans();
    let taken: Vec<&Value> = held_roles(&spans, &id, accepted_ns).into_iter().filter(|sp| rdm_test::estate::names_location(sp, "node", &Estate::path("mesh1.node-admin", "3"))).collect();
    assert_eq!(taken.len(), 1, "the second launch recorded its own transition of the fabric-primary's roles: {taken:#?}");
    let created: Vec<&Value> = spans
        .iter()
        .filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_admin.node.create.")) && sp["attributes"]["moment"] != "arrival" && start(sp) >= accepted_ns)
        .filter(|sp| !(sp["attributes"]["moment"] == "create" && [Estate::path("mesh1.node-admin", "2"), Estate::path("mesh1.node-admin", "3")].iter().any(|at| rdm_test::estate::names_location(sp, "subject", at))))
        .collect();
    assert!(created.is_empty(), "nothing is created after the delete's acceptance but each launch itself: {created:?}");
}
