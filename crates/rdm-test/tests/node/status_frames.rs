//! product=mesh, feature=node-lifecycle, subfeature=status-frames, rung=MM, provider=process.
//!
//! The lifecycle status frames a mesh-primary publishes on its mesh channel (gossip.md, the
//! lifecycle frames): `NodeStopped` for a node.stop, a node command (Q342), and `NodeDeleting` for a
//! node.delete's step. Each carries the gossip lifecycle op: type, Build, operation, the birth, its
//! path and name, its mesh and the Rafka-time of the event, and no attempt (Q344: "the gossip
//! lifecycle op's leftover `attempt` field goes"). The spans an estate exports are the cell's flame.

use rdm_client::{Frame, NodeEvent};
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

const FABRIC: &str = "fabric1";
const SHAPE: [(&str, u32, u32); 1] = [("mesh1", 2, 3)];
const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "status-frames".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

fn attr<'a>(sp: &'a Value, k: &str) -> &'a str {
    sp["attributes"][k].as_str().unwrap_or_default()
}

/// The frames of a node command or Build stream, to its end; a broken stream is a defect of the cell.
async fn frames_of(mut stream: rdm_client::WorkflowStream) -> Vec<Frame> {
    let mut frames = Vec::new();
    while let Some(f) = stream.next().await {
        frames.push(f.expect("the stream does not break"));
    }
    frames
}

/// CONTRACT (Q344: "A Build has no attempts ... and the gossip lifecycle op's leftover `attempt` field
/// goes"; node-stop.md: the mesh-primary publishes `NodeStopped`; node-delete.md: `NodeDeleting` is
/// published at the delete's NodeDeleting step): a node.stop of one node and a node.delete of another
/// each publish their status frame on mesh1's channel, `NodeStopped` naming no Build (a node command
/// is never a Build, Q342) and `NodeDeleting` naming the delete's Build, each with its operation and
/// exact birth, and neither frame's span carries an attempt. Must NOT happen: an `attempt` on either
/// frame, a Build opened by the stop, or the deleting frame naming another Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_stopped_and_a_node_deleting_frame_carry_their_operation_and_birth_and_no_attempt() {
    let test = "a_node_stopped_and_a_node_deleting_frame_carry_their_operation_and_birth_and_no_attempt";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    estate.resize_shape(FABRIC, &SHAPE).await;
    let views = estate.settled_views(&SHAPE).await;
    estate.until_fabric_ready(SETTLE).await;
    let mut plain = views.iter().filter(|n| n.r#type == "node" && !n.is_primary).cloned().collect::<Vec<_>>();
    plain.sort_by_key(|n| n.location());
    let (stopped, deleted) = (plain[0].clone(), plain[1].clone());

    let nodes = estate.nodes_obj().await;
    let stop = nodes.stop(&stopped.location()).await.unwrap_or_else(|e| panic!("node.stop of {:?} was refused: {e}", stopped.location()));
    assert!(stop.accepted().is_none(), "node.stop is a node command and opens no Build (Q342)");
    let frames = frames_of(stop).await;
    assert_eq!(frames.last(), Some(&Frame::Complete), "the stop completes: {frames:?}");
    assert!(frames.contains(&Frame::Event(NodeEvent::Stopped)), "{frames:?}");

    let delete = estate.nodes_obj().await.delete(vec![deleted.node_id.clone()]).await.unwrap_or_else(|e| panic!("node.delete of {:?} was refused: {e}", deleted.location()));
    let build = delete.accepted().cloned().expect("node.delete opens a Build").build_id.to_string();
    let frames = frames_of(delete).await;
    assert_eq!(frames.last(), Some(&Frame::Complete), "the delete completes: {frames:?}");

    estate.stop().await;
    let spans = estate.spans();
    let frame_of = |kind: &str, node_id: &str| -> Value {
        named(&spans, "rdm.mesh.topology.update.via-status-event")
            .into_iter()
            .find(|sp| attr(sp, "frame") == kind && attr(sp, "subject_id") == node_id)
            .cloned()
            .unwrap_or_else(|| panic!("the mesh-primary published {kind} of {node_id}"))
    };
    let stopped_frame = frame_of("node-stopped", stopped.node_id.as_str());
    let deleting_frame = frame_of("node-deleting", deleted.node_id.as_str());
    for (sp, node) in [(&stopped_frame, &stopped), (&deleting_frame, &deleted)] {
        assert!(sp["attributes"].get("attempt").is_none(), "a status frame carries no attempt (Q344): {sp}");
        assert_eq!(attr(sp, "incarnation_id"), node.incarnation.as_ref().map(|i| i.as_str()).unwrap_or_default(), "the frame names the exact birth: {sp}");
        assert_eq!((attr(sp, "subject_path"), attr(sp, "subject_name")), (node.path.as_str(), node.name.as_str()), "{sp}");
    }
    assert_eq!(attr(&stopped_frame, "operation"), "node.stop", "{stopped_frame}");
    assert_eq!(attr(&stopped_frame, "build_id"), rdm_core::LifecycleOp::NO_BUILD, "a node command's frame names no Build: {stopped_frame}");
    assert_eq!(attr(&deleting_frame, "operation"), "node.delete", "{deleting_frame}");
    assert_eq!(attr(&deleting_frame, "build_id"), build, "the deleting frame names the delete's Build: {deleting_frame}");
    for name in ["rdm.node_admin.node.update.via-node-stopped", "rdm.node_admin.node.update.via-node-deleting"] {
        for sp in named(&spans, name) {
            assert!(sp["attributes"].get("attempt").is_none(), "{name} carries no attempt (Q344): {sp}");
        }
    }
    println!("flame (NodeStopped): http://192.168.68.99:16687/trace/{}", stopped_frame["trace_id"].as_str().unwrap_or_default());
    println!("flame (NodeDeleting): http://192.168.68.99:16687/trace/{}", deleting_frame["trace_id"].as_str().unwrap_or_default());
    estate.record_trace_url(deleting_frame["trace_id"].as_str().unwrap_or(""));
}
