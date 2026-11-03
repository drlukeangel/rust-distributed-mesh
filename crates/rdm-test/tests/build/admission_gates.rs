//! End-to-end cell for `build-admission-gates` (`docs/architecture/build.md:321`).

use rdm_client::{BuildFrame, BuildStream, CallEnd, FabricDesired, MeshDesired, Nodes};
use rdm_test::estate::{named, Estate, Owner};
use std::time::Duration;

const FABRIC: &str = "fabric1";
const SHAPE1: [(&str, u32, u32); 1] = [("mesh1", 2, 1)];
const SHAPE_GROWN: [(&str, u32, u32); 1] = [("mesh1", 2, 2)];

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-build".into(),
        subfeature: "build-admission-gates".into(),
        rung: "MM".into(),
        provider: "process".into(),
        test: test.into(),
    }
}

async fn read_to_complete(stream: &mut BuildStream) {
    while let Some(frame) = stream.next().await {
        match frame.expect("stream frame") {
            BuildFrame::Complete { .. } => return,
            BuildFrame::Failed { operation, step, reason, .. } => {
                panic!("Build failed at {operation:?} {step}: {reason}")
            }
            _ => {}
        }
    }
    panic!("Build stream ended without Complete");
}

async fn wait_for_blocked_frame(stream: &mut BuildStream) -> (String, String) {
    while let Some(frame) = stream.next().await {
        match frame.expect("stream frame") {
            BuildFrame::Blocked { step, reason, .. } if step != "fabric-change" => {
                return (step, reason);
            }
            other @ (BuildFrame::Complete { .. } | BuildFrame::Failed { .. }) => {
                panic!("Build ended before Blocked frame: {other:?}")
            }
            _ => {}
        }
    }
    panic!("Build stream ended before Blocked frame");
}

/// CONTRACT (build.md §4.4; node-stop.md, node-start.md "Entry"/"Admission"; Q342, D40): a node command
/// is never a Build, so it passes no Build gate. A caller's node.stop and node.start go to the node's
/// mesh-primary, which admits them by name and writes nothing: a stop sent through a carrier that is
/// not the fabric-primary reaches mesh1's primary all the same and is refused `no-secondary` there (the
/// sole rpc node holds its type's role and no other rpc node can take it), never `not-fabric-primary`;
/// a node at no path is `unknown-node`; a start of a ready birth is `node-not-live`. A Build while
/// another is in progress is refused `build-in-progress`, naming it: that gate is the Build's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operation_admission_gates_refuse_non_fabric_primary_unknown_node_live_start_and_concurrent_build_over_node_rpc()
{
    let test = "operation_admission_gates_refuse_non_fabric_primary_unknown_node_live_start_and_concurrent_build_over_node_rpc";
    let mut estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    let carrier = estate.carrier().await;
    let desired = FabricDesired {
        fabric: FABRIC.into(),
        meshes: SHAPE1
            .iter()
            .map(|(m, a, r)| MeshDesired::of(*m, [("node-admin", *a), ("node", *r)]))
            .collect(),
    };
    let mut stream = carrier
        .create(estate.shape_change_at(&carrier, &desired).await)
        .await
        .expect("shape accepted");
    read_to_complete(&mut stream).await;
    estate.settles_to(&SHAPE1, Duration::from_secs(90)).await;

    // 1. A stop through a carrier that is not the fabric-primary: the node command goes to the node's
    //    mesh-primary, which refuses the sole rpc node's stop `no-secondary`; nothing answers
    //    `not-fabric-primary`.
    let views = estate.node_views().await;
    let non_fp = views.iter().find(|n| n.r#type == "node-admin" && !n.is_fabric_primary).expect("other admin");
    let non_fp_carrier = estate.peers().await.carrier_for(non_fp.node_id.clone());
    assert_eq!(non_fp_carrier.target(), &rdm_transport::NodeTarget::ExactNode(non_fp.node_id.clone()));
    match Nodes::new(non_fp_carrier).stop(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).await.err().expect("stop of sole rpc-node must be refused") {
        CallEnd::Rejected { reason, .. } => assert_eq!(reason, "no-secondary"),
        other => panic!("expected no-secondary from mesh1's primary, got {other:?}"),
    }

    // 2. A node stands at the path -> `unknown-node`, answered by the caller's node.get, nothing sent.
    match Nodes::new(carrier.clone()).stop(&rdm_core::PathName::new("mesh1.node", "99").unwrap()).await.err().expect("unknown node must be refused") {
        CallEnd::Rejected { reason, .. } => assert_eq!(reason, "unknown-node"),
        other => panic!("expected unknown-node for mesh1.node.99, got {other:?}"),
    }

    // 3. The node's state admits the command (a start needs a stopped birth) -> `node-not-live`.
    match Nodes::new(carrier.clone()).start(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).await.err().expect("starting a Ready node must be refused") {
        CallEnd::Rejected { reason, .. } => assert_eq!(reason, "node-not-live"),
        other => panic!("expected node-not-live when starting Ready mesh1.node.1, got {other:?}"),
    }

    // 4. The current Build is settled -> `build-in-progress`, naming the Build: a second Build is refused.
    rdm_test::write_pre_node_ready_knob(&estate.root, &rdm_core::PathName::new("mesh1.node", "2").unwrap(), "hold-for-gate-check").unwrap();
    let grown = FabricDesired {
        fabric: FABRIC.into(),
        meshes: SHAPE_GROWN
            .iter()
            .map(|(m, a, r)| MeshDesired::of(*m, [("node-admin", *a), ("node", *r)]))
            .collect(),
    };
    // The grown shape's Create, read once: the same request submitted again while it runs (its node is
    // in the topology by then, so a read now would add nothing).
    let grow = estate.shape_change_at(&carrier, &grown).await;
    let mut active_stream = carrier.create(grow.clone()).await.expect("grown shape accepted");
    let active_build_id = active_stream.build_id().clone();
    let (blocked_step, _) = wait_for_blocked_frame(&mut active_stream).await;
    assert_eq!(blocked_step, "WaitForNodeReady");
    let again = carrier.create(grow).await.err().expect("a second Build during the active one must be refused");
    match again {
        CallEnd::BuildInProgress { current } => assert_eq!(current, active_build_id, "build-in-progress refusal must name the active Build id"),
        other => panic!("expected BuildInProgress naming {active_build_id}, got {other:?}"),
    }

    rdm_test::write_pre_node_ready_knob(&estate.root, &rdm_core::PathName::new("mesh1.node", "2").unwrap(), "pass").unwrap();
    read_to_complete(&mut active_stream).await;
    estate.settles_to(&SHAPE_GROWN, Duration::from_secs(90)).await;
    estate.stop().await;

    let spans = estate.spans();
    assert!(named(&spans, "rdm.node_admin.build.reject.via-not-fabric-primary").is_empty(), "no node command is refused not-fabric-primary");
    let no_secondary_spans = named(&spans, "rdm.node_admin.build.reject.via-no-secondary");
    assert!(!no_secondary_spans.is_empty(), "expected rdm.node_admin.build.reject.via-no-secondary span at mesh1's primary");
    let not_live_spans = named(&spans, "rdm.node_admin.node.reject.via-not-live");
    assert!(!not_live_spans.is_empty(), "expected rdm.node_admin.node.reject.via-not-live span");
    let in_progress_spans = named(&spans, "rdm.node_admin.build.reject.via-build-in-progress");
    assert!(
        in_progress_spans.iter().any(|s| s["attributes"]["build_id"].as_str() == Some(active_build_id.as_str())),
        "expected rdm.node_admin.build.reject.via-build-in-progress span naming {active_build_id}"
    );
    estate.record_trace_url(no_secondary_spans[0]["trace_id"].as_str().unwrap_or_default());
}
