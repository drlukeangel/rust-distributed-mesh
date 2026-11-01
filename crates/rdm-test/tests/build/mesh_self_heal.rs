//! E2E test for `build-mesh-self-heal` (`docs/architecture/build.md:289`).

use rdm_client::NodeStatus;
use rdm_core::build::BuildPhase;
use rdm_test::estate::{named, Estate, Owner};
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

/// CONTRACT: docs/architecture/build.md:289 "the mesh-primary of the member's mesh opens the next attempt of B123 on its mesh's rows, claims and runs it itself (R-BI7); the Build stays completed, and the fabric-primary reads the attempt's verdict from the Build journal"
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_primary_self_heals_killed_member_on_current_build_while_build_stays_completed() {
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 2)];
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let accepted = estate.reconcile_shape("fabric1", &shape).await.build_id;
    estate.settles_to(&shape, Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;

    let primary = estate.node_views().await.into_iter().find(|n| n.mesh == "mesh2" && n.kind == rdm_core::NodeKind::NodeAdmin && n.is_primary).expect("mesh primary");
    let mesh2_primary = primary.node_id.to_string();
    let executor = rdm_core::framing::Birth { node_id: primary.node_id.clone(), incarnation: primary.incarnation_id.clone().expect("launched primary") };
    let victim = estate
        .node_views()
        .await
        .into_iter()
        .filter(|n| n.mesh == "mesh2" && n.kind == rdm_core::NodeKind::RpcNode && !n.is_primary)
        .map(|n| (n.name, n.node_id, n.incarnation_id.expect("launched victim")))
        .min_by(|a, b| a.0.cmp(&b.0))
        .expect("an rpc node of mesh2");

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

    estate.stop().await;
    let spans = estate.spans();
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-node-recovery")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["node_id"]) == victim.1.as_str())
        .collect();
    assert!(
        recoveries.iter().any(|sp| s(&sp["attributes"]["submitter"]) == mesh2_primary
            && s(&sp["attributes"]["mesh"]) == "mesh2"
            && s(&sp["attributes"]["build_id"]) == accepted.to_string()),
        "mesh2's primary initiated its member's recovery in mesh2 on the current Build: {recoveries:#?}"
    );
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .filter(|sp| {
            s(&sp["attributes"]["executor"]) == executor.to_string()
                && s(&sp["attributes"]["lane"]) == "mesh2"
        })
        .collect();
    assert!(
        !reconciles.is_empty()
            && reconciles.iter().any(|sp| {
                ["rdm.node_admin.node.delete.via-build", "rdm.node_admin.node.create.via-build"].iter().all(|name| named(&spans, name).into_iter().any(|child| child["attributes"]["node_id"] == victim.1.as_str() && child["attributes"]["incarnation_id"] == victim.2.as_str() && child["trace_id"] == sp["trace_id"] && child["parent_span_id"] == sp["span_id"]))
            }),
        "mesh2's primary reconciled the mesh2 recovery attempt with node.delete and node.create: {reconciles:#?}"
    );
}
