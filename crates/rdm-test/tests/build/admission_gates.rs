//! End-to-end cell for `build-admission-gates` (`docs/architecture/build.md:321`).

use rdm_client::{BuildFrame, BuildStream, CallEnd, FabricDesired, MeshDesired, Nodes};
use rdm_core::{NodeKind, PathName};
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
                panic!("Build failed at {operation} {step}: {reason}")
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

/// Page: `docs/architecture/build.md:321`
/// "Every operation on the topology, a Build or a standalone node.stop, node.start,
/// node.connection.delete or node.delete, passes the fabric-primary's gates in this order before
/// anything runs. Each gate reads one source. A gate that fails refuses by name and nothing is written.
/// - The receiver is the fabric-primary -> `not-fabric-primary`, naming the fabric-primary
/// - The current Build is settled -> `build-in-progress`, naming the Build
/// - A node stands at the path -> `unknown-node`
/// - The node's state admits the action: a start needs a stopped birth... -> `node-not-live`"
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
            .map(|(m, a, r)| MeshDesired::of(*m, [(NodeKind::NodeAdmin, *a), (NodeKind::RpcNode, *r)]))
            .collect(),
    };
    let mut stream = carrier
        .create(estate.shape_change_at(&carrier, &desired).await)
        .await
        .expect("shape accepted");
    read_to_complete(&mut stream).await;
    estate.settles_to(&SHAPE1, Duration::from_secs(90)).await;

    // 1. Gate: The receiver is the fabric-primary -> `not-fabric-primary`, naming the fabric-primary,
    //    which redirects the carrier to `mesh1.admin.1`, where Gate 7 (`A primary's stop, connections
    //    delete or delete has a successor`) refuses stopping the sole rpc-node with `no-secondary`.
    let non_fp_carrier = estate.carrier_at(&"mesh1.admin.2".parse::<PathName>().unwrap()).await;
    assert_eq!(non_fp_carrier.fabric_primary().to_string(), "mesh1.admin.2");
    let primary_stop_err = Nodes::new(non_fp_carrier)
        .stop(&"mesh1.rpc.1".parse().unwrap())
        .await
        .err()
        .expect("stop of sole rpc-node must be refused");
    match primary_stop_err {
        CallEnd::Rejected { reason, .. } => {
            assert_eq!(reason, "no-secondary");
        }
        other => panic!("expected no-secondary after redirect to mesh1.admin.1, got {other:?}"),
    }

    // 2. Gate: A node stands at the path -> `unknown-node`.
    let unknown_err = Nodes::new(carrier.clone())
        .stop(&"mesh1.rpc.99".parse().unwrap())
        .await
        .err()
        .expect("unknown node must be refused");
    match unknown_err {
        CallEnd::Rejected { reason, .. } => {
            assert_eq!(reason, "unknown-node");
        }
        other => panic!("expected unknown-node for mesh1.rpc.99, got {other:?}"),
    }

    // 3. Gate: The node's state admits the action (a start needs a stopped birth) -> `node-not-live`.
    let start_live_err = Nodes::new(carrier.clone())
        .start(&"mesh1.rpc.1".parse().unwrap())
        .await
        .err()
        .expect("starting a Ready node must be refused");
    match start_live_err {
        CallEnd::Rejected { reason, .. } => {
            assert_eq!(reason, "node-not-live");
        }
        other => panic!("expected node-not-live when starting Ready mesh1.rpc.1, got {other:?}"),
    }

    // 4. Gate: The current Build is settled -> `build-in-progress`, naming the Build.
    rdm_test::write_pre_node_ready_knob(&estate.root, "mesh1.rpc.2", "hold-for-gate-check")
        .unwrap();
    let grown = FabricDesired {
        fabric: FABRIC.into(),
        meshes: SHAPE_GROWN
            .iter()
            .map(|(m, a, r)| MeshDesired::of(*m, [(NodeKind::NodeAdmin, *a), (NodeKind::RpcNode, *r)]))
            .collect(),
    };
    let mut active_stream = carrier
        .create(estate.shape_change_at(&carrier, &grown).await)
        .await
        .expect("grown shape accepted");
    let active_build_id = active_stream.build_id().clone();
    let (blocked_step, _) = wait_for_blocked_frame(&mut active_stream).await;
    assert_eq!(blocked_step, "WaitForNodeReady");

    let in_progress_err = Nodes::new(carrier.clone())
        .stop(&"mesh1.rpc.1".parse().unwrap())
        .await
        .err()
        .expect("stop during active Build must be refused");
    match in_progress_err {
        CallEnd::BuildInProgress { current } => {
            assert_eq!(
                current, active_build_id,
                "build-in-progress refusal must name the active Build id"
            );
        }
        other => panic!("expected BuildInProgress naming {active_build_id}, got {other:?}"),
    }

    rdm_test::write_pre_node_ready_knob(&estate.root, "mesh1.rpc.2", "pass").unwrap();
    read_to_complete(&mut active_stream).await;
    estate.settles_to(&SHAPE_GROWN, Duration::from_secs(90)).await;
    estate.stop().await;

    let spans = estate.spans();
    let not_fp_spans = named(&spans, "rdm.node_admin.build.reject.via-not-fabric-primary");
    assert!(
        not_fp_spans.iter().any(|s| {
            s["attributes"]["node"].as_str() == Some("mesh1.admin.2")
                && s["attributes"]["fabric_primary"].as_str() == Some("mesh1.admin.1")
        }),
        "expected rdm.node_admin.build.reject.via-not-fabric-primary on mesh1.admin.2 naming mesh1.admin.1"
    );
    let no_secondary_spans = named(&spans, "rdm.node_admin.build.reject.via-no-secondary");
    assert!(
        !no_secondary_spans.is_empty(),
        "expected rdm.node_admin.build.reject.via-no-secondary span"
    );
    let unknown_spans = named(&spans, "rdm.node_admin.build.reject.via-unknown-node");
    assert!(
        !unknown_spans.is_empty(),
        "expected rdm.node_admin.build.reject.via-unknown-node span"
    );
    let not_live_spans = named(&spans, "rdm.node_admin.node.reject.via-not-live");
    assert!(
        !not_live_spans.is_empty(),
        "expected rdm.node_admin.node.reject.via-not-live span"
    );
    let in_progress_spans = named(&spans, "rdm.node_admin.build.reject.via-build-in-progress");
    assert!(
        in_progress_spans
            .iter()
            .any(|s| s["attributes"]["build_id"].as_str() == Some(active_build_id.as_str())),
        "expected rdm.node_admin.build.reject.via-build-in-progress span naming {active_build_id}"
    );
    estate.record_trace_url(
        in_progress_spans[0]["trace_id"].as_str().unwrap_or_default(),
    );
}
