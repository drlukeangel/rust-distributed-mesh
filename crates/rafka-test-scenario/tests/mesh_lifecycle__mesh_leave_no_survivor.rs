//! i143.e12.s39 (#3066) process E2E, form (b) of the mesh leave resumed after the leaving mesh's
//! primary is lost mid-leave (R11; mesh-leave.md, "Resume after the primary's loss", step 2: "or,
//! when none survives, the one node-admin the fabric-primary created for it"): mesh2's only admin is
//! lost mid-tier, and the fabric-primary creates the one node-admin the leaving mesh may gain. The
//! run and the shared assertions are `mesh_lifecycle__mesh_leave_resume`'s (form (a)).

use crate::mesh_lifecycle__mesh_leave_resume::{assert_resumed, attr_u, primary_lost_mid_leave, s, start_ns};
use rafka_test_scenario::estate::named;
use serde_json::Value;

/// CONTRACT: mesh2's only admin, killed while it holds rpc.2's drain receipt, leaves no admin of
/// the mesh: the fabric-primary proves and records its exact Exited first, creates the one
/// node-admin the leaving mesh may gain at a path no birth held, amends the roster for it, and
/// the resumed attempt finishes; MeshLeft covers the four births, both primaries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_whose_only_admin_is_lost_mid_tier_finishes_under_the_admin_the_fabric_primary_creates() {
    let run = primary_lost_mid_leave("a_leave_whose_only_admin_is_lost_mid_tier_finishes_under_the_admin_the_fabric_primary_creates", 1).await;
    let created: Vec<&Value> = named(&run.spans, "rdm.node_admin.node.create.via-leaving-mesh").into_iter().filter(|sp| sp["attributes"]["outcome"] == "created").collect();
    assert_eq!(created.len(), 1, "the fabric-primary created one admin for mesh2: {created:?}");
    assert_eq!(s(&created[0]["attributes"]["node"]), "mesh2.admin.2", "{}", created[0]);
    let amended = assert_resumed(&run, "mesh2.admin.2", 4);
    assert!(start_ns(created[0]) < start_ns(&amended), "the admin is created before the amendment admits it");
    let outer = format!("shutdown-mesh:{}", run.mesh2_id);
    let admitted: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-owner-admitted-roster").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str()).collect();
    assert!(admitted.iter().any(|sp| attr_u(sp, "amendments") == 1), "the created admin runs the owner's accepted roster plus the amendment: {admitted:?}");
}
