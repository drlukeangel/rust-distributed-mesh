//! i143.e12.s39 (#3066) process E2E, form (b) of the mesh leave resumed after the leaving mesh's
//! primary is lost mid-leave (R11; mesh-delete.md, "Resume after the primary's loss", step 2: "or,
//! when none survives, the one node-admin the fabric-primary created for it"): mesh2's only admin is
//! lost mid-tier, and the fabric-primary creates the one node-admin the leaving mesh may gain. The
//! run and the shared assertions are `mesh_lifecycle__mesh_delete_resume`'s (form (a)).

use crate::mesh_lifecycle__mesh_delete_resume::{assert_resumed, attr_u, primary_lost_mid_leave, s, start_ns, Rpc1};
use rafka_test_scenario::estate::named;
use serde_json::Value;

/// CONTRACT: mesh2's only admin, killed while it holds rpc.2's and rpc.1's StopNode
/// receipts (both stopped, neither NodeStopped published, no Exited receipt), leaves no admin of the
/// mesh: the fabric-primary proves and records its
/// exact Exited first, creates the one node-admin the leaving mesh may gain at a path no birth
/// held, amends the roster for it, and the resumed attempt finishes; MeshLeft covers the four
/// births, both primaries. The created admin joined after rpc.1 stopped and holds no record of it:
/// the accepted roster decides what runs, not its view (mesh-delete.md, "Resume after the primary's
/// loss", step 3: "runs only the tiers without a terminal receipt"), so rpc.1 and rpc.2 both run
/// under it and the lost primary's exit is the one receipt retained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_whose_only_admin_is_lost_mid_tier_finishes_under_the_admin_the_fabric_primary_creates() {
    let run = primary_lost_mid_leave("a_leave_whose_only_admin_is_lost_mid_tier_finishes_under_the_admin_the_fabric_primary_creates", 1, Rpc1::Stopped).await;
    let created: Vec<&Value> = named(&run.spans, "rdm.node_admin.node.create.via-leaving-mesh").into_iter().filter(|sp| sp["attributes"]["outcome"] == "created").collect();
    assert_eq!(created.len(), 1, "the fabric-primary created one admin for mesh2: {created:?}");
    assert_eq!(s(&created[0]["attributes"]["node"]), "mesh2.admin.2", "{}", created[0]);
    // mesh2's primary committed its own mesh row Leaving when it admitted delete-mesh and declared
    // it up to the fabric-primary (DeclareMeshState, "report committed state upward"), which
    // applied it before the primary was lost and before it created the admin.
    let declared: Vec<&Value> = named(&run.spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|sp| sp["attributes"]["op"] == "declare-mesh-state" && sp["attributes"]["sender"] == "mesh2.admin.1" && sp["attributes"]["outcome"] == "applied" && s(&sp["attributes"]["node"]).starts_with("mesh1."))
        .filter(|sp| start_ns(sp) < start_ns(created[0]))
        .collect();
    assert!(declared.iter().any(|sp| sp["attributes"]["state"] == "Leaving"), "the fabric-primary applied mesh2's declared Leaving before it created the admin: {declared:?}");
    // The created admin is handed the mesh's current state before its Ready: mesh2 is leaving, and
    // a mesh's state only moves forward (states.md), never back to Pending or ready-for-traffic.
    let handed: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.update.via-state-handoff").into_iter().filter(|sp| sp["attributes"]["target"] == "mesh2.admin.2" && sp["attributes"]["outcome"] == "applied").collect();
    assert!(!handed.is_empty() && handed.iter().all(|sp| sp["attributes"]["state"] == "Leaving"), "the created admin is handed mesh2's Leaving: {handed:?}");
    let pending: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|sp| sp["attributes"]["target"] == "mesh2.admin.2").collect();
    assert!(pending.is_empty(), "the created admin is never handed Pending: {pending:?}");
    let amended = assert_resumed(&run, "mesh2.admin.2", 4);
    assert!(start_ns(created[0]) < start_ns(&amended), "the admin is created before the amendment admits it");
    let outer = format!("shutdown-mesh:{}", run.mesh2_id);
    // The created admin admits over the roster captured once plus the amendment that names it, and
    // captures none of its own: it reads the leaving roster recorded in the Build (gossiped Build
    // facts) with the amendment, from its own facts once they hold it (`via-own-build-facts`) or
    // from the owner's (`via-owner-build-facts`), or, holding no roster, the owner's accepted roster
    // plus the amendment (`via-owner-admitted-roster`). Either way its member workflows run over
    // the four births and only the one with no terminal receipt.
    let me = |sp: &&Value| s(&sp["attributes"]["me"]).starts_with("mesh2.admin.2 ") && sp["attributes"]["operation"] == outer.as_str() && attr_u(sp, "amendments") == 1;
    let admitted: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-owner-admitted-roster").into_iter().chain(named(&run.spans, "rdm.node_admin.mesh.resolve.via-owner-build-facts")).chain(named(&run.spans, "rdm.node_admin.mesh.resolve.via-own-build-facts")).filter(me).collect();
    assert!(!admitted.is_empty(), "the created admin admits over the accepted roster plus the amendment naming it");
    let captured: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-accepted-roster").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str() && attr_u(sp, "attempt") >= 2 && sp["attributes"]["source"] == "captured").collect();
    assert!(captured.is_empty(), "the resumed attempt captures no roster: {captured:?}");
    let resumed: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-retained-receipts").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str() && attr_u(sp, "attempt") >= 2).collect();
    // The door held rpc.2 and rpc.1 stopped, neither with an Exited receipt: the lost
    // primary's recorded exit is the one retained, and both ordinary members run under the created
    // admin. The final primary itself is no tier member.
    assert!(
        resumed.iter().any(|sp| attr_u(sp, "roster") == 4 && s(&sp["attributes"]["retained_births"]) == "mesh2.admin.1" && attr_u(sp, "remaining") == 2),
        "the created admin runs only the members with no terminal receipt over the roster plus the amendment, the lost primary's exit retained: {resumed:?}"
    );
    let tier: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.update.via-leave-tier").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str() && sp["attributes"]["tier"] == "ordinary-nodes" && attr_u(sp, "attempt") >= 2 && sp["attributes"]["outcome"] == "complete").collect();
    assert!(tier.iter().any(|sp| attr_u(sp, "members") == 2), "the created admin runs both ordinary members, the stopped rpc.1 it holds no record of included: {tier:?}");
}
