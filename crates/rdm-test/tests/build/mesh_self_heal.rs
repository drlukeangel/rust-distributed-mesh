//! E2E test for `build-mesh-self-heal` (`docs/architecture/build.md:289`).

use rdm_client::NodeStatus;
use rdm_core::build::BuildPhase;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "build-mesh-self-heal".into(),
        subfeature: "mesh-self-heal".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "mesh_primary_self_heals_killed_member_on_current_build_while_build_stays_completed"
            .into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// CONTRACT (node-recovery.md "Execution": the mesh-primary of the member's mesh recovers it, its own workflow; build.md §1: a repair is never a Build or part of one, Q342): a killed member of mesh2 is recovered by mesh2's primary as a new birth at its path and name; the current Build stays completed and current, and no Build is minted or reopened for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_primary_self_heals_killed_member_on_current_build_while_build_stays_completed() {
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 2)];
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let accepted = estate.resize_shape("fabric1", &shape).await.build_id;
    estate.settles_to(&shape, Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;

    let primary = estate.node_views().await.into_iter().find(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.is_primary).expect("mesh primary");
    let mesh2_primary = primary.node_id.to_string();
    let executor = rdm_core::framing::Birth { node_id: primary.node_id.clone(), incarnation: primary.incarnation_id.clone().expect("launched primary") };
    let victim = estate
        .node_views()
        .await
        .into_iter()
        .filter(|n| n.mesh == "mesh2" && n.r#type == "node" && !n.is_primary)
        .map(|n| (n.location(), n.node_id, n.incarnation_id.expect("launched victim")))
        .min_by(|a, b| a.0.cmp(&b.0))
        .expect("a node of mesh2");

    estate.kill_node(&victim.0).await;

    let born = estate
        .until_node(
            &victim.0,
            &format!("{:?} is recovered by its mesh-primary", victim.0),
            crate::node_recovery_due(),
            |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != victim.1).cloned(),
        )
        .await;
    assert_ne!(born.node_id, victim.1);

    let carrier = estate.carrier().await;
    let current = carrier.build(&accepted).await.expect("build.get [id]");
    assert_eq!(current.build_id, accepted, "no new Build was minted for drift recovery");
    assert_eq!(current.phase, BuildPhase::Complete, "the Build stays completed");
    assert!(current.current, "the accepted Build remains the current Build");

    // The recovery records its end when its create returns, after the node it created is ready: the
    // estate stops only once it has, so the stop never cuts the recovery short.
    wait_for("mesh2's primary's recovery of its member ends", Duration::from_secs(60), || {
        let (estate, victim) = (&estate, victim.1.to_string());
        async move {
            named(&estate.spans(), "rdm.node_admin.node.update.via-node-recovery")
                .into_iter()
                .any(|sp| s(&sp["attributes"]["node_id"]) == victim && !s(&sp["attributes"]["outcome"]).is_empty())
                .then_some(())
        }
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    // The mesh-primary self-heals its own mesh as its own workflow, never a Build or part of one
    // (node-recovery.md "Execution"; Q342): its recovery submit, then the delete and the create of the
    // exact birth under it, and no Build reconciles the recovery.
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["node_id"]) == victim.1.as_str())
        .collect();
    let recovery = recoveries
        .iter()
        .copied()
        .find(|sp| s(&sp["attributes"]["submitter"]) == mesh2_primary && s(&sp["attributes"]["mesh"]) == "mesh2" && s(&sp["attributes"]["outcome"]) == "complete")
        .unwrap_or_else(|| panic!("mesh2's primary recovered its member in mesh2 as its own workflow: {recoveries:#?}"));
    for name in ["rdm.node_admin.node.delete.via-node-recovery", "rdm.node_admin.node.create.via-node-recovery"] {
        assert!(
            named(&spans, name).into_iter().any(|child| child["parent_span_id"] == recovery["span_id"] && child["attributes"]["node_id"] == victim.1.as_str() && child["attributes"]["incarnation_id"] == victim.2.as_str()),
            "{name} of the exact birth runs under the recovery: {recovery}"
        );
    }
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|sp| s(&sp["attributes"]["executor"]) == executor.to_string() && s(&sp["attributes"]["lane"]) == "mesh2").collect();
    assert!(reconciles.is_empty(), "no Build reconciles the recovery: {reconciles:#?}");
    estate.record_trace_url(&s(&recovery["trace_id"]));
}
