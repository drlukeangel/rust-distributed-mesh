//! product=mesh, feature=fabric-lifecycle, subfeature=own-delete-matrix, rung=MM, provider=process.
//!
//! The node-admins a stop, restart or delete reaches whose executor is, or could be, the node itself:
//! the e2e rows of the matrix whose functional rows are `crates/rdm-admin/tests/own_run_stop.rs`.
//!
//! Why missed: until f982ad9fa no cell had a node-admin execute a stop or delete of itself, its own run
//! commanding its own stop. The control plane names a node-admin the executor of its own operation in
//! one place only, `executor.rs` `node_op_executor`'s `last_mesh_gone` (every mesh Dead and the target
//! its mesh's only node: the fabric-primary after fabric.deleted, fabric-delete.md step 4). Everywhere
//! else a primary hands over first (node.primary.update) and another admin executes (node-stop.md "Primary"), or the
//! call is refused by name before anything moves (`last_admin`, Q7). So every node_stop, node_delete,
//! node_restart, connections-delete and mesh_delete cell had another admin execute, and the one place
//! the branch is reached, fabric-delete.md step 4, was never called: every fabric_delete cell stopped
//! at "only the fabric-primary is left, for its separate node.delete" and ended with `Estate::stop`,
//! SIGTERM, the signal's stop with no commander and no run in progress. The functional cell that
//! reached it (`rdm-admin/tests/fabric_delete.rs`, crit:e12.s59.1) asserted the executor and ran the
//! pipeline over a stub fleet, with no gate, no run turn, no caller stream and no process to end.
//!
//! The rows, each read where the pages put its executor:
//! - the fabric-primary's own node.delete after fabric.deleted, from outside, in a one-mesh fabric
//!   (`fabric_delete::the_fabric_primary_left_after_fabric_deleted_is_deleted_by_an_outside_node_delete_inside_the_stop_bound`,
//!   kept) and in a two-mesh fabric whose own mesh has two admins (here): its executor is itself;
//! - after fabric.deleted, its own stop, restart and connections delete: refused by name (node-stop.md
//!   "Primary": no successor), then its own delete ends it;
//! - an ordinary admin's node.delete (its mesh-primary executes), and the fabric-primary's node.delete
//!   with a successor in its mesh (it hands over first; the successor executes);
//! - a mesh-primary deleting itself: the pages never make it its own executor (mesh-delete.md step 7:
//!   the fabric-primary executes the deleted mesh's final admin, `stopped_last_admin` and
//!   `canonical_ids` cover it); its standalone delete with no successor, and the fabric-primary's with
//!   none in its mesh, are refused by name;
//! - node.stop then node.start of the fabric-primary, then node.restart of the fabric-primary, in a
//!   one-mesh and a two-mesh fabric: it hands over first and its successor executes;
//! - the demo estate's teardown (`scripts/demo-estate.sh down`): `rdm-rpc-probe fabric-delete`, then
//!   `rdm-rpc-probe node-delete` of the fabric-primary from outside.
//!
//! Every delete answers node.deleted as its last reply and the runtime it deletes has exited inside
//! the stop's bound of the call's acceptance; every stop answers stopped with the process alive.

use rdm_client::{CallEnd, Frame, LaunchKind as NodeKind, NodeEvent, NodeStatus, NodeView, PathName};
use rdm_test::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

/// The bound of every wait of these cells that is not a delete's: a shape settling, a Build's terminal.
const SETTLE: Duration = Duration::from_secs(180);

/// The bound of a node.delete from its acceptance to node.deleted and the exit of the runtime it
/// deletes: the stop's (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`, 5 s by default, and the 10 s the
/// executor allows a stop's reply past it), which the termination of a process runtime and its
/// inspection fit inside (the same bound as `fabric_delete::OWN_DELETE_BOUND`).
const DELETE_BOUND: Duration = Duration::from_secs(15);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "fabric-lifecycle".into(), subfeature: "own-delete-matrix".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

fn path(n: &str) -> PathName {
    n.parse().unwrap()
}

fn attr(sp: &Value, k: &str) -> String {
    sp["attributes"][k].as_str().map(str::to_string).unwrap_or_else(|| sp["attributes"][k].to_string())
}

/// Whether `pid` runs: a process whose state is not a zombie.
fn running(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

/// A fabric at `shape`, settled and ready, entered at its bootstrap admin `mesh1.admin.1`,
/// the fabric-primary.
async fn ready(test: &str, shape: &[(&str, u32, u32)]) -> (Estate, Vec<NodeView>) {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", shape).await;
    let views = estate.settles_to(shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let fp: Vec<&NodeView> = views.iter().filter(|n| n.is_fabric_primary).collect();
    assert!(fp.len() == 1 && fp[0].name == path("mesh1.admin.1"), "mesh1.admin.1 is the fabric-primary: {fp:?}");
    (estate, views)
}

/// The pid of `node`'s runtime: the bootstrap admin's, or the one its launching admin recorded.
async fn pid_of(estate: &Estate, node: &str) -> u32 {
    if node == "mesh1.admin.1" {
        return estate.bootstrap_pid().expect("the estate holds its bootstrap admin");
    }
    estate.pid_of(node).await as u32
}

/// node.delete of `node`, accepted, read to its last reply (the terminal, or the stream's end after
/// it), and the exit of `pid`, both inside [`DELETE_BOUND`] of the acceptance. A delete whose target
/// hands over first ends its first stream `FabricPrimaryMoved` (node-stop.md "Primary"); the delete
/// is followed at the fabric-primary it names, to `Complete` with its `NodeDeleted` step. Answers the
/// replies.
async fn deleted_inside_the_bound(estate: &mut Estate, node: &str, pid: u32) -> Vec<String> {
    let mut stream = estate.nodes_obj().await.delete(&path(node)).await.unwrap_or_else(|end| panic!("node.delete of {node} was refused: {end}"));
    let accepted = stream.accepted().clone();
    let begun = tokio::time::Instant::now();
    let (mut replies, moved) = tokio::time::timeout(DELETE_BOUND, async {
        let mut replies = Vec::new();
        let mut moved = None;
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(frame) => {
                    let end = frame.is_terminal();
                    if let Frame::FabricPrimaryMoved { fabric_primary } = &frame {
                        moved = Some(fabric_primary.clone());
                    }
                    replies.push(format!("{frame:?}"));
                    if end {
                        break;
                    }
                }
                // A deleted runtime that served the stream serves nothing after node.deleted.
                Err(end) => {
                    replies.push(format!("ended: {end}"));
                    break;
                }
            }
        }
        (replies, moved)
    })
    .await
    .unwrap_or_else(|_| panic!("node.delete of {node} answered no last reply within {DELETE_BOUND:?}"));
    if let Some(fp) = moved {
        let fp = fp.unwrap_or_else(|| panic!("node.delete of {node} handed over to no named fabric-primary: {replies:?}"));
        estate.enter(&path(&fp)).await;
        let left = DELETE_BOUND.saturating_sub(begun.elapsed());
        let followed = tokio::time::timeout(left, estate.resume(&accepted.build_id, accepted.attempt)).await.unwrap_or_else(|_| panic!("node.delete of {node} followed at {fp} reached no Complete within {DELETE_BOUND:?} of its acceptance: {replies:?}"));
        let deleted = followed.frames.iter().any(|f| matches!(f, rdm_client::BuildFrame::Step { operation, step, .. } if operation == &format!("node.delete:{node}") && step == "NodeDeleted"));
        replies.extend(followed.frames.iter().map(|f| format!("{f:?}")));
        assert!(deleted, "node.delete of {node}, followed at {fp}, receipts its NodeDeleted: {replies:?}");
    } else {
        assert!(replies.iter().any(|r| r == &format!("{:?}", Frame::Event(NodeEvent::Deleted))), "node.delete of {node} answers node.deleted: {replies:?}");
    }
    assert!(!replies.iter().any(|r| r.starts_with("Failed")), "no step of node.delete of {node} failed: {replies:?}");
    while running(pid) && begun.elapsed() < DELETE_BOUND {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!running(pid), "{node}'s runtime {pid} exited within {DELETE_BOUND:?} of its node.delete: {replies:?}");
    replies
}

/// fabric.delete at the fabric-primary, to `Complete`, leaving only the fabric-primary's runtime.
async fn fabric_deleted(estate: &Estate) {
    let mut delete = estate.carrier().await.create(rdm_client::Change::DeleteFabric).await.unwrap_or_else(|end| panic!("fabric.delete was refused: {end}"));
    let id = delete.build_id().clone();
    let frames = tokio::time::timeout(SETTLE, async {
        let mut frames = Vec::new();
        while let Some(frame) = delete.next().await {
            let frame = frame.unwrap_or_else(|e| panic!("the stream of fabric.delete {id} broke before its terminal: {e}"));
            let end = matches!(frame, rdm_client::BuildFrame::Complete { .. } | rdm_client::BuildFrame::Failed { .. } | rdm_client::BuildFrame::FabricPrimaryMoved { .. });
            frames.push(frame);
            if end {
                break;
            }
        }
        frames
    })
    .await
    .unwrap_or_else(|_| panic!("fabric.delete {id} reached no terminal within {SETTLE:?}"));
    assert!(matches!(frames.last(), Some(rdm_client::BuildFrame::Complete { .. })), "fabric.delete {id} completes: {frames:?}");
    assert_eq!(estate.live_runtimes(), vec![], "only the fabric-primary is left, for its separate node.delete");
}

/// A Build call refused by name, `reason`, before anything is admitted: what each answers is recorded
/// beside the cell's artifact.
fn refused_by_name(what: &str, reason: &str, call: Result<rdm_client::WorkflowStream, CallEnd>) -> String {
    let refused = match call {
        Ok(stream) => panic!("{what} was admitted (Build {}), where it is refused by name", stream.accepted().build_id),
        Err(end) => end,
    };
    let CallEnd::Rejected { reason: named, detail } = &refused else { panic!("{what} ends Rejected: {refused:?}") };
    assert_eq!(named, reason, "{what}: {refused:?}");
    detail.clone()
}

/// CONTRACT (fabric-delete.md "After completion" and step 4; node-delete.md step 1; node-stop.md
/// "Transport cut": "never ... the stop call's stream"): a two-mesh fabric, mesh1 {2 node-admins, 1 rpc
/// node} holding the fabric-primary mesh1.admin.1 and mesh2 {1, 1}, completes fabric.delete: mesh2
/// first, then its own mesh1 (mesh1.admin.2 deleted inside it), and reports fabric.deleted alive, the
/// one runtime left. An outside caller's node.delete of it is executed by itself: its stop holds its
/// tasks without its own run's turns (`via-stop-quiesce` commanded by the delete's Build), its cut
/// spares the caller reading that Build (`spared`), it answers node.deleted, and once the caller read
/// it, it ends its own runtime through its provider: the process exits, all inside the stop's bound. What must NOT
/// happen: a stop waiting on the run that waits on it, a cut of the caller's stream before node.deleted,
/// or the runtime ending before node.deleted or left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_mesh_fabrics_primary_left_after_fabric_deleted_is_deleted_by_its_own_run_from_an_outside_call_inside_the_bound() {
    let test = "a_two_mesh_fabrics_primary_left_after_fabric_deleted_is_deleted_by_its_own_run_from_an_outside_call_inside_the_bound";
    let (mut estate, _) = ready(test, &[("mesh1", 2, 1), ("mesh2", 1, 1)]).await;
    fabric_deleted(&estate).await;
    let pid = pid_of(&estate, "mesh1.admin.1").await;
    assert!(running(pid), "the fabric-primary reported fabric.deleted alive");
    let replies = deleted_inside_the_bound(&mut estate, "mesh1.admin.1", pid).await;
    estate.stop().await;
    let spans = estate.spans();
    let of_it = |name: &str| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| attr(sp, "node") == "mesh1.admin.1").cloned().collect() };
    let held: Vec<Value> = of_it("rdm.node_admin.node.update.via-stop-quiesce").into_iter().filter(|sp| !attr(sp, "commanded_by").is_empty()).collect();
    assert_eq!(held.len(), 1, "its own run commanded its stop once, holding without that run's turns: {held:#?}");
    let cut: Vec<Value> = of_it("rdm.node_admin.node.update.via-stop-transport-cut").into_iter().filter(|sp| attr(sp, "operation") == "node.stop:mesh1.admin.1").collect();
    assert!(cut.len() == 1 && attr(&cut[0], "spared") != "0", "its own delete's cut spared the caller reading the Build: {cut:#?}");
    // `via-own-runtime` closes after the provider's terminate of the process it runs in returns, which
    // it never does: the span is not exported, and the runtime's end is read from its exit instead.
    estate.artifact("own-delete.json", &json!({"replies": replies, "quiesce": held[0], "cut": cut[0]}));
    estate.record_trace_url(held[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-stop.md "Primary": "A target that is a primary hands over first ... With no successor
/// the stop is refused by name before anything moves"; fabric-delete.md "After completion": the one call
/// left is the fabric-primary's own node.delete): a one-mesh fabric {1 node-admin, 1 rpc node} completes
/// fabric.delete, the fabric-primary alive and alone. Its node.stop, node.restart and
/// node.connection.delete are each refused by name, `unknown-node` (the delete took it out of the
/// accepted topology): none is run by itself as its own executor, which would leave a stopped
/// fabric-primary that nothing is left to start or delete; its node.delete, step 4's call, is the one
/// admitted. Then its own node.delete from outside answers node.deleted and its runtime exits
/// inside the stop's bound. What must NOT happen: a stop or connections delete of the
/// fabric-primary admitted after fabric.deleted, or its own delete failing after the refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_fabric_deleted_the_fabric_primarys_own_stop_and_connections_delete_are_refused_by_name_and_its_own_delete_ends_it() {
    let test = "after_fabric_deleted_the_fabric_primarys_own_stop_and_connections_delete_are_refused_by_name_and_its_own_delete_ends_it";
    let (mut estate, _) = ready(test, &[("mesh1", 1, 1)]).await;
    fabric_deleted(&estate).await;
    let nodes = estate.nodes_obj().await;
    let fp = path("mesh1.admin.1");
    let mut refusals = Vec::new();
    refusals.push(refused_by_name("node.stop of the fabric-primary after fabric.deleted", "unknown-node", nodes.stop(&fp).await));
    refusals.push(refused_by_name("node.connection.delete of the fabric-primary after fabric.deleted", "unknown-node", nodes.delete_connections(&fp).await));
    let pid = pid_of(&estate, "mesh1.admin.1").await;
    assert!(running(pid), "the refusals moved nothing: the fabric-primary runs");
    let replies = deleted_inside_the_bound(&mut estate, "mesh1.admin.1", pid).await;
    estate.stop().await;
    estate.artifact("refusals.json", &json!({"refusals": refusals, "own_delete": replies}));
}

/// CONTRACT (node-delete.md: "Caller / executor: Typed caller / accepted node-admin executor"; steps
/// 1-4; mesh-delete.md "Executor": the mesh-primary; node-stop.md "Primary": a target that is a primary
/// hands over first and is stopped by its successor): a one-mesh fabric {3 node-admins, 1 rpc node}.
/// node.delete of an admin that is no primary is executed by the mesh-primary: node.deleted, its runtime
/// exited inside the bound. Then node.delete of the fabric-primary mesh1.admin.1, also mesh1's primary,
/// which has a successor: it hands over first and the successor executes the delete, never itself:
/// node.deleted, its runtime exited inside the bound, and the remaining admin names itself the
/// fabric-primary. What must NOT happen: an admin executing its own delete while another admin of its
/// mesh can, or a delete whose runtime outlives the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_admins_delete_and_the_fabric_primarys_delete_with_a_successor_are_executed_by_another_admin_inside_the_bound() {
    let test = "an_ordinary_admins_delete_and_the_fabric_primarys_delete_with_a_successor_are_executed_by_another_admin_inside_the_bound";
    let (mut estate, views) = ready(test, &[("mesh1", 3, 1)]).await;
    let mut admins: Vec<String> = views.iter().filter(|n| n.kind == NodeKind::NodeAdmin && !n.is_primary && !n.is_fabric_primary).map(|n| n.name.to_string()).collect();
    admins.sort();
    assert_eq!(admins.len(), 2, "two admins are no primary: {views:?}");
    let (ordinary, successor) = (admins[1].clone(), admins[0].clone());
    let pid = pid_of(&estate, &ordinary).await;
    let ordinary_replies = deleted_inside_the_bound(&mut estate, &ordinary, pid).await;

    // The fabric-primary hands over to its successor first, which the typed objects enter.
    estate.enter(&path(&successor)).await;
    let pid = pid_of(&estate, "mesh1.admin.1").await;
    let fp_replies = deleted_inside_the_bound(&mut estate, "mesh1.admin.1", pid).await;
    let fabric = estate.fabric_view_at(&path(&successor)).await.expect("the successor answers fabric.get");
    assert_eq!(fabric.fabric_primary, Some(path(&successor)), "the successor is the fabric-primary");
    estate.stop().await;
    let spans = estate.spans();
    for (node, from) in [(ordinary.as_str(), "mesh1.admin.1"), ("mesh1.admin.1", successor.as_str())] {
        let own: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-stop-quiesce").into_iter().filter(|sp| attr(sp, "node") == node && !attr(sp, "commanded_by").is_empty()).collect();
        assert!(own.is_empty(), "{node}'s delete is executed by {from}, never by its own run: {own:#?}");
    }
    estate.artifact("deletes.json", &json!({"ordinary": ordinary_replies, "fabric_primary": fp_replies}));
}

/// CONTRACT (node-stop.md "Primary": with no successor a primary's stop "is refused by name before
/// anything moves"; a node.delete that would leave a mesh with members and no node-admin is refused by
/// name, `would-leave-mesh-without-admin`, at acceptance (control_plane); mesh-delete.md step 7:
/// a mesh-primary is deleted by its own node.delete only after its mesh.delete, the fabric-primary
/// executing): a two-mesh fabric, mesh1 {1 node-admin, 1 rpc node} holding the fabric-primary and mesh2
/// {2, 1}. node.delete of the fabric-primary, its mesh's only admin beside an rpc node, is refused
/// `would-leave-mesh-without-admin` by name. node.delete of mesh2's admin that is no primary is executed by mesh2's primary:
/// node.deleted, its runtime exited inside the bound. node.delete of mesh2's primary, now its mesh's only
/// admin beside an rpc node, is refused `would-leave-mesh-without-admin` by name, naming mesh2. What must NOT happen: a
/// mesh-primary or the fabric-primary deleting itself while its mesh has members, or a refusal that
/// moves a primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_a_two_mesh_fabric_a_primarys_delete_with_no_successor_is_refused_by_name_and_an_ordinary_admins_delete_ends_it_inside_the_bound() {
    let test = "in_a_two_mesh_fabric_a_primarys_delete_with_no_successor_is_refused_by_name_and_an_ordinary_admins_delete_ends_it_inside_the_bound";
    let (mut estate, views) = ready(test, &[("mesh1", 1, 1), ("mesh2", 2, 1)]).await;
    let nodes = estate.nodes_obj().await;
    let fp_refused = refused_by_name("node.delete of the fabric-primary, mesh1's only admin", "would-leave-mesh-without-admin", nodes.delete(&path("mesh1.admin.1")).await);
    let primary = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).expect("mesh2's primary");
    let ordinary = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && !n.is_primary).map(|n| n.name.to_string()).expect("mesh2's other admin");
    let pid = pid_of(&estate, &ordinary).await;
    let replies = deleted_inside_the_bound(&mut estate, &ordinary, pid).await;
    // node.deleted is mesh2's primary's; the fabric-primary's view drops the node once mesh2's
    // publication reaches it.
    estate.until_node(&ordinary, &format!("{ordinary} gone from the fabric-primary's view"), Duration::from_secs(30), |n| n.is_none().then_some(())).await;
    let mp_refused = refused_by_name("node.delete of mesh2's primary, its mesh's only admin", "would-leave-mesh-without-admin", estate.nodes_obj().await.delete(&path(&primary)).await);
    assert!(mp_refused.contains("mesh2"), "the refusal names mesh2: {mp_refused}");
    for now in estate.mesh_node_views("mesh2").await.iter().filter(|n| n.kind == NodeKind::NodeAdmin) {
        assert!(now.name.to_string() == primary && now.is_primary && now.status == NodeStatus::Ready, "mesh2's primary stays mesh2's primary and runs: {now:?}");
    }
    estate.stop().await;
    estate.artifact("deletes.json", &json!({"fabric_primary_refused": fp_refused, "ordinary": replies, "mesh_primary_refused": mp_refused}));
}

/// The frames of a workflow `call` to its end, followed across the fabric-primary's hand-over:
/// a stream ending `FabricPrimaryMoved` is resumed at the admin it names (estate_teardown's pattern).
async fn followed(estate: &mut Estate, what: &str, call: Result<rdm_client::WorkflowStream, CallEnd>) -> Vec<Frame> {
    let mut stream = call.unwrap_or_else(|end| panic!("{what} was refused: {end}"));
    let accepted = stream.accepted().clone();
    let frames = Estate::frames_of(&mut stream).await;
    match frames.last() {
        Some(Frame::Complete) => frames,
        Some(Frame::FabricPrimaryMoved { fabric_primary }) => {
            if let Some(fp) = fabric_primary.as_deref() {
                estate.enter(&path(fp)).await;
            }
            estate.resume(&accepted.build_id, accepted.attempt).await;
            frames
        }
        other => panic!("{what} ended {other:?}: {frames:?}"),
    }
}

/// node.stop then node.start of the fabric-primary mesh1.admin.1 of a fabric at `shape` (mesh1 with two
/// admins), then node.stop then node.start of the fabric-primary it handed over to.
async fn stop_and_start_of_the_fabric_primary_twice(test: &str, shape: &[(&str, u32, u32)]) {
    let (mut estate, views) = ready(test, shape).await;
    let successor = views.iter().find(|n| n.mesh == "mesh1" && n.kind == NodeKind::NodeAdmin && !n.is_primary).map(|n| n.name.clone()).expect("mesh1's second admin");
    let before = views.iter().find(|n| n.name == path("mesh1.admin.1")).cloned().unwrap();
    let pid = pid_of(&estate, "mesh1.admin.1").await;
    estate.enter(&successor).await;

    let begun = tokio::time::Instant::now();
    let stop = estate.nodes_obj().await.stop(&before.name).await;
    let stopped = followed(&mut estate, "node.stop of the fabric-primary", stop).await;
    let stop_took = begun.elapsed();
    assert!(stopped.iter().any(|f| matches!(f, Frame::Event(NodeEvent::Stopped))) || matches!(stopped.last(), Some(Frame::FabricPrimaryMoved { .. })), "the stop answered stopped, or handed over first: {stopped:?}");
    let fabric = estate.fabric_view_at(&successor).await.expect("the successor answers fabric.get");
    let fp = fabric.fabric_primary.clone().expect("a fabric-primary after the stop");
    assert_ne!(fp, before.name, "the stopped admin handed over first");
    estate.enter(&fp).await;
    let stopped_view = estate.mesh_node_views("mesh1").await.into_iter().find(|n| n.name == before.name).expect("node.get lists the stopped admin");
    assert!(stopped_view.stopped && stopped_view.incarnation_id == before.incarnation_id, "the stopped fabric-primary is the same birth, stopped with its process live: {stopped_view:?}");
    assert!(running(pid), "a stop keeps the process: {pid} runs");

    let started = Estate::workflow("node.start of the former fabric-primary", estate.nodes_obj().await.start(&before.name).await).await;
    let back = estate.mesh_node_views("mesh1").await.into_iter().find(|n| n.name == before.name).expect("node.get lists the started admin");
    assert!(!back.stopped && back.incarnation_id == before.incarnation_id && back.status == NodeStatus::Ready, "the same birth runs again: {back:?}");
    assert!(running(pid), "the start rejoined the same process {pid}");

    // node.stop then node.start of the fabric-primary it handed over to: it hands over first, and it is the
    // same node after.
    let primary = estate.mesh_node_views("mesh1").await.into_iter().find(|n| n.name == fp).expect("node.get lists the fabric-primary");
    let other = if fp == before.name { successor.clone() } else { before.name.clone() };
    estate.enter(&other).await;
    let stop = estate.nodes_obj().await.stop(&fp).await;
    let restarted = followed(&mut estate, "node.stop of the fabric-primary", stop).await;
    let fabric = estate.fabric_view_at(&other).await.expect("the other admin answers fabric.get");
    let now = fabric.fabric_primary.clone().unwrap_or_else(|| panic!("a fabric-primary after the stop: {fabric:?}"));
    estate.enter(&now).await;
    Estate::workflow("node.start of the second fabric-primary", estate.nodes_obj().await.start(&fp).await).await;
    let after = estate.mesh_node_views("mesh1").await.into_iter().find(|n| n.name == fp).expect("node.get lists the started admin");
    assert!(after.node_id == primary.node_id && after.status == NodeStatus::Ready, "the stopped and started fabric-primary is the same node, running: {after:?}");
    estate.stop().await;
    let spans = estate.spans();
    let own: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-stop-quiesce").into_iter().filter(|sp| !attr(sp, "commanded_by").is_empty()).collect();
    assert!(own.is_empty(), "no admin's own run commanded its own stop: its successor executed each: {own:#?}");
    estate.artifact("fabric-primary-stop-start-restart.json", &json!({"stop_ms": stop_took.as_millis() as u64, "stopped": format!("{stopped:?}"), "started": format!("{started:?}"), "restarted": format!("{restarted:?}")}));
}

/// CONTRACT (node-stop.md "Primary": "A target that is a primary hands over first by node.primary.update
/// (node.fabric-primary.update for the fabric-primary) and is stopped by its successor"; node-start.md;
/// Q183): a one-mesh fabric {2 node-admins, 1 rpc node}. node.stop of the fabric-primary
/// mesh1.admin.1 hands over to mesh1.admin.2 first, which executes the stop: the same birth stopped,
/// its process alive. node.start makes it rejoin as the same birth. node.stop of the fabric-primary
/// it handed over to hands over first again, and its node.start completes, the same node running after.
/// What must NOT happen: an admin executing its own stop (its own run commanding its stop), or a stop
/// that ends the process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_a_one_mesh_fabric_the_fabric_primarys_stops_and_starts_are_executed_by_its_successor() {
    let test = "in_a_one_mesh_fabric_the_fabric_primarys_stops_and_starts_are_executed_by_its_successor";
    stop_and_start_of_the_fabric_primary_twice(test, &[("mesh1", 2, 1)]).await;
}

/// CONTRACT (as the one-mesh row; node-primary-update.md "The fabric-primary"): the same in a two-mesh
/// fabric, mesh1 {2 node-admins, 1 rpc node} holding the fabric-primary and mesh2 {1, 1}.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_a_two_mesh_fabric_the_fabric_primarys_stops_and_starts_are_executed_by_its_successor() {
    let test = "in_a_two_mesh_fabric_the_fabric_primarys_stops_and_starts_are_executed_by_its_successor";
    stop_and_start_of_the_fabric_primary_twice(test, &[("mesh1", 2, 1), ("mesh2", 1, 1)]).await;
}

/// CONTRACT (fabric-delete.md steps 1-4 as the demo estate's teardown runs them, `scripts/demo-estate.sh
/// down`: `rdm-rpc-probe fabric-delete` at the Day-0 admin, then `rdm-rpc-probe node-delete` of the
/// fabric-primary from outside): a two-mesh fabric of two node-admins and an rpc node each. The probe's
/// fabric.delete completes; its node.delete of the fabric-primary answers node.deleted; the
/// fabric-primary's runtime exits inside the stop's bound of that call's acceptance, and the estate's
/// own stop then finds nothing to kill. What must NOT happen: the teardown leaving a runtime for the
/// estate's signal or its SIGKILL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_demo_teardown_deletes_the_fabric_then_the_fabric_primary_from_outside_and_ends_clean_inside_the_bound() {
    let test = "the_demo_teardown_deletes_the_fabric_then_the_fabric_primary_from_outside_and_ends_clean_inside_the_bound";
    let (mut estate, _) = ready(test, &[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    let pid = pid_of(&estate, "mesh1.admin.1").await;
    let deleted = estate.probe(&["fabric-delete", "--target", "path:mesh1.admin.1"]);
    assert_eq!((deleted["outcome"].as_str(), deleted["terminal"].as_str()), (Some("Reply"), Some("Complete")), "the probe's fabric.delete completes: {deleted}");
    assert_eq!(estate.live_runtimes(), vec![], "only the fabric-primary is left, for its separate node.delete");
    let begun = std::time::Instant::now();
    let own = estate.probe(&["node-delete", "--target", "path:mesh1.admin.1"]);
    assert_eq!((own["outcome"].as_str(), own["deleted"].as_bool()), (Some("Reply"), Some(true)), "the probe's node.delete of the fabric-primary answers node.deleted: {own}");
    assert!(!own["replies"].as_array().into_iter().flatten().any(|r| r.as_str().is_some_and(|r| r.starts_with("Failed"))), "no step failed: {own}");
    while running(pid) && begun.elapsed() < DELETE_BOUND {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!running(pid), "the fabric-primary's runtime exited within {DELETE_BOUND:?} of the probe's node.delete: {own}");
    let killed = estate.stop().await;
    assert_eq!(killed, Vec::<u32>::new(), "the teardown left nothing for the estate's stop to kill");
    estate.artifact("demo-teardown.json", &json!({"fabric_delete": deleted, "node_delete": own}));
}
