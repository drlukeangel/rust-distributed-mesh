//! A node.create accepted right after a node.delete of the same class completed, before the
//! fabric-primary's view has dropped the deleted birth: the soak `i143-rshape-soak-authority-container`
//! at `2a641bf6` (its 97th checklist, a hand-off of mesh2's role) saw the create reconciled against a view
//! that still held the deleted admin, plan no operation and complete, and nothing re-created the path
//! in the 150 s it waited (i143 PRD, e10.s2 "The fifth run").
use rdm_client::{LaunchKind as NodeKind, NodeStatus};
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-build".into(),
        subfeature: "create-after-delete".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_create_accepted_inside_a_deletions_window_births_the_path_once".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// A Build id as its spans log it: `bid_xxxx-xxxx-xxxx`.
fn logged(id: &str) -> String {
    format!("bid_{}-{}-{}", &id[0..4], &id[4..8], &id[8..12])
}

/// CONTRACT (build.md "Every execution attempt re-reads the accepted topology and current
/// realized/observed state and computes remaining work"; node-delete.md, node-create.md): the
/// realized state a Build plans from counts a birth the previous Build deleted as gone from the
/// moment its deletion completed (the `NodeDeleted` receipt, the exited verdict), whether or not the
/// planner's gossip view has dropped it yet. A node.delete of an rpc node completes; a node.create of
/// an rpc node in the same mesh is accepted at once, inside the window before the fabric-primary's
/// view drops the deleted birth. The create plans the birth at the lowest free ordinal, the deleted
/// path, and the path is born once as a new node.id, ready. What must NOT happen: the
/// create's attempt reconciled with no operation and the Build complete with nothing born, a second
/// birth at the path, or the deleted node.id back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_accepted_inside_a_deletions_window_births_the_path_once() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    // Two meshes: the delete and the create run on mesh2's lane, executed by mesh2's primary, and the
    // fabric-primary in mesh1 plans the create from a view it learns of mesh2 through gossip.
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2), ("mesh2", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 2), ("mesh2", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // The soak's shape: the deleted node is mesh2's role-holding node-admin (a hand-off), so the role
    // moves to the other admin, which executes the delete, and the create is a node-admin in mesh2.
    let path = estate.mesh_view("mesh2").await.primary_admin.expect("mesh2 has a primary admin").to_string();
    let path = path.as_str();
    let before = estate.node_view(path).await;
    assert_eq!(before.status, NodeStatus::Ready, "{path} is ready before the delete: {before:?}");

    let mut deleting = estate.nodes_obj().await.delete(&path.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of {path}: {e}"));
    let delete = Estate::complete(&mut deleting).await.build_id.to_string();
    // At once, inside the window: the create is submitted on the delete's completion, before any view read.
    let mut creating = estate.create_node("mesh2", NodeKind::NodeAdmin).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of a node-admin in mesh2: {e}"));
    let create = Estate::complete(&mut creating).await.build_id.to_string();

    let born = estate
        .until_node(path, &format!("{path} is born again as a new node.id and ready"), Duration::from_secs(90), |n| {
            n.filter(|n| n.status == NodeStatus::Ready && n.node_id != before.node_id).cloned()
        })
        .await;
    assert_ne!(born.node_id, before.node_id, "{path} is a new birth, never the deleted node.id back");
    estate.stop().await;
    let spans = estate.spans();
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|sp| [logged(&create), create.clone()].contains(&s(&sp["attributes"]["build_id"]))).collect();
    let all_ids: Vec<String> = named(&spans, "rdm.node_admin.build.update.via-reconcile").iter().map(|sp| s(&sp["attributes"]["build_id"])).collect();
    assert!(!reconciles.is_empty(), "the create Build {create} was reconciled (reconciles seen: {all_ids:?})");
    assert!(
        reconciles.iter().any(|sp| !s(&sp["attributes"]["operations"]).is_empty()),
        "the create Build {create} planned the birth at {path} (an attempt with an operation), not `converged` with none while the view still held the deleted birth of delete {delete}: {reconciles:?}"
    );
    let creates = named(&spans, "rdm.node_admin.node.create.via-build").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == path && [logged(&create), create.clone()].contains(&s(&sp["attributes"]["build_id"]))).count();
    assert_eq!(creates, 1, "{path} is born once by the create Build {create}");
}
