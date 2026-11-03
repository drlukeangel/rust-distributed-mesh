//! product=mesh, feature=node-lifecycle, subfeature=node-commands, rung=MN, provider=process.
//!
//! Node commands and repairs are never part of a Build (build.md §1): node.stop and node.start are
//! node commands of the primary above the node (node-stop.md, node-start.md), and node recovery is
//! the primary's own workflow (node-recovery.md). A caller's Build in flight neither holds them nor
//! is written by them.

use rdm_client::{BuildFrame, BuildStream, FabricDesired, Filter, Frame, MeshDesired, NodeEvent};
use rdm_core::PathName;
use rdm_test::estate::{Estate, Owner};
use std::time::Duration;

const FABRIC: &str = "fabric1";
const SHAPE: [(&str, u32, u32); 1] = [("mesh1", 2, 2)];
const SHAPE_GROWN: [(&str, u32, u32); 1] = [("mesh1", 2, 3)];
const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-commands".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn desired(shape: &[(&str, u32, u32)]) -> FabricDesired {
    FabricDesired { fabric: FABRIC.into(), meshes: shape.iter().map(|(m, a, r)| MeshDesired::of(*m, [("node-admin", *a), ("node", *r)])).collect() }
}

async fn read_to_complete(stream: &mut BuildStream) {
    while let Some(frame) = stream.next().await {
        match frame.expect("stream frame") {
            BuildFrame::Complete { .. } => return,
            BuildFrame::Failed { reason, .. } => panic!("the Build could not start: {reason}"),
            BuildFrame::Incomplete { sections } => panic!("the Build ended incomplete: {sections:?}"),
            _ => {}
        }
    }
    panic!("the Build stream ended before Complete");
}

/// The held Build's stream up to its Pending frame at the grown node's readiness.
async fn until_blocked(stream: &mut BuildStream) {
    while let Some(frame) = stream.next().await {
        match frame.expect("stream frame") {
            BuildFrame::Pending { step, .. } if step == "WaitForNodeReady" => return,
            other @ (BuildFrame::Complete { .. } | BuildFrame::Failed { .. }) => panic!("the Build ended before it was held: {other:?}"),
            _ => {}
        }
    }
    panic!("the Build stream ended before it was held");
}

async fn workflow(mut stream: rdm_client::WorkflowStream) -> Vec<Frame> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        let Ok(next) = tokio::time::timeout_at(deadline, stream.next()).await else { panic!("no terminal within {SETTLE:?}: {out:?}") };
        let Some(f) = next else { break };
        let f = f.unwrap_or_else(|e| panic!("the reply stream broke: {e}"));
        let terminal = f.is_terminal();
        out.push(f);
        if terminal {
            break;
        }
    }
    out
}

fn events(frames: &[Frame]) -> Vec<NodeEvent> {
    frames.iter().filter_map(|f| if let Frame::Event(e) = f { Some(*e) } else { None }).collect()
}

/// A fabric of SHAPE, then a grown Build held at its new node's `pre_node_ready` (the knob), its
/// stream returned at the Pending frame.
async fn held_build(test: &str) -> (Estate, rdm_client::BuildCarrier, BuildStream, PathName) {
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let carrier = estate.carrier().await;
    let mut first = carrier.create(estate.shape_change_at(&carrier, &desired(&SHAPE)).await).await.expect("shape accepted");
    read_to_complete(&mut first).await;
    estate.settles_to(&SHAPE, Duration::from_secs(90)).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let grown_node = PathName::new("mesh1.node", "3").unwrap();
    rdm_test::write_pre_node_ready_knob(&estate.root, &grown_node, "hold-for-node-commands").unwrap();
    let mut held = carrier.create(estate.shape_change_at(&carrier, &desired(&SHAPE_GROWN)).await).await.expect("grown shape accepted");
    until_blocked(&mut held).await;
    (estate, carrier, held, grown_node)
}

/// CONTRACT: node.stop and node.start are node commands, never a Build or part of one (build.md §1;
/// node-stop.md, node-start.md): while a caller's Build is held in progress, a stop of a ready node
/// answers its connections delete and `stopped` and ends `Complete`, then a start answers `joined`,
/// `started` and ends `Complete`. Neither is refused build-in-progress, neither writes a Build, and the
/// held Build's record is the one it was: its id, its phase and its events unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_and_a_start_run_while_a_build_is_held_and_write_no_build() {
    let test = "a_stop_and_a_start_run_while_a_build_is_held_and_write_no_build";
    let (mut estate, carrier, mut held, grown_node) = held_build(test).await;
    let builds_before = carrier.builds(&Filter::new()).await.expect("build.get");
    let held_before = carrier.build(held.build_id()).await.expect("build.get [id]");

    let node = PathName::new("mesh1.node", "1").unwrap();
    let nodes = estate.nodes_obj().await;
    let stopped = workflow(nodes.stop(&node).await.expect("the stop is accepted while a Build is held")).await;
    assert_eq!(events(&stopped), vec![NodeEvent::ConnectionsDeleted, NodeEvent::Stopped], "{stopped:?}");
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    let started = workflow(nodes.start(&node).await.expect("the start is accepted while a Build is held")).await;
    assert_eq!(events(&started), vec![NodeEvent::Joined, NodeEvent::Started], "{started:?}");
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");

    let builds_after = carrier.builds(&Filter::new()).await.expect("build.get");
    let ids = |b: &[rdm_client::Build]| b.iter().map(|b| b.build_id.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&builds_after), ids(&builds_before), "a stop and a start write no Build");
    let held_after = carrier.build(held.build_id()).await.expect("build.get [id]");
    assert_eq!((held_after.phase, &held_after.events), (held_before.phase, &held_before.events), "the held Build is not written by a node command: no event of it");

    rdm_test::write_pre_node_ready_knob(&estate.root, &grown_node, "pass").unwrap();
    read_to_complete(&mut held).await;
    estate.settles_to(&SHAPE_GROWN, Duration::from_secs(90)).await;
    estate.stop().await;
}

/// CONTRACT: node recovery is the primary's own workflow, never a Build or part of one
/// (node-recovery.md "Execution"): a member whose runtime is killed while a caller's Build is held in
/// progress is re-created with its path and name and a new node.id, ready, within the recovery's own
/// bound, while that Build is still held. Nothing waits for the Build, no Build is written for the
/// recovery, and the held Build's record is the one it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_lost_while_a_build_is_held_is_recovered_before_the_build_ends() {
    let test = "a_member_lost_while_a_build_is_held_is_recovered_before_the_build_ends";
    let (mut estate, carrier, mut held, grown_node) = held_build(test).await;
    let builds_before = carrier.builds(&Filter::new()).await.expect("build.get");
    let held_before = carrier.build(held.build_id()).await.expect("build.get [id]");

    let node = PathName::new("mesh1.node", "1").unwrap();
    let lost = estate.node_views().await.into_iter().find(|n| n.location() == node).expect("node.get lists the member");
    estate.kill_node(&node).await;
    let due = crate::node_recovery_due();
    let deadline = tokio::time::Instant::now() + due;
    loop {
        let views = estate.node_views().await;
        if views.iter().any(|n| n.location() == node && n.node_id != lost.node_id && n.status == rdm_client::NodeStatus::Ready) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "the lost member was not re-created ready within {due:?} while the Build was held: {views:?}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let builds_after = carrier.builds(&Filter::new()).await.expect("build.get");
    let ids = |b: &[rdm_client::Build]| b.iter().map(|b| b.build_id.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&builds_after), ids(&builds_before), "a recovery writes no Build");
    let held_after = carrier.build(held.build_id()).await.expect("build.get [id]");
    assert_eq!((held_after.phase, &held_after.events), (held_before.phase, &held_before.events), "the held Build is not written by a recovery: no event of it");

    rdm_test::write_pre_node_ready_knob(&estate.root, &grown_node, "pass").unwrap();
    read_to_complete(&mut held).await;
    estate.settles_to(&SHAPE_GROWN, Duration::from_secs(90)).await;
    estate.stop().await;
}
