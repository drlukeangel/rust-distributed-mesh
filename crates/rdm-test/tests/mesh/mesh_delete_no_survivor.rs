//! i143.e12.s39 (#3066) process E2E, form (b) of the mesh delete resumed after the deleting mesh's
//! primary is lost mid-delete (mesh-delete.md, "Resume after the primary's loss", step 2: "or, when
//! none survives, the one node-admin the fabric-primary creates for it, the only new node a deleting
//! mesh may gain"): mesh2's only admin is lost mid-tier, and on the resubmit the fabric-primary
//! creates the one node-admin the deleting mesh may gain. The run and the shared assertions are
//! `mesh_delete_resume`'s (form (a)).

use rdm_test::estate::Estate;
use crate::mesh_delete_resume::{assert_resumed, attr_u, primary_lost_mid_leave, s, start_ns, Rpc1};
use rdm_test::estate::named;
use serde_json::Value;

/// CONTRACT: mesh2's only admin, killed while it holds rpc.2's and rpc.1's StopNode step events (both
/// stopped, neither NodeStopped published, no TerminateRuntime event), leaves no admin of the mesh,
/// and the first run ends incomplete. On the resubmit the fabric-primary deletes the lost primary by
/// its node.delete first, then creates the one node-admin the deleting mesh may gain at a path no birth
/// held, and hands it mesh2's section; the Build completes, and mesh-left covers the four births of
/// mesh2's records, both primaries included. The created admin joined after rpc.1 stopped and holds no
/// record of it: the mesh's records decide what runs, not its view (mesh-delete.md, "Resume after the
/// primary's loss", step 3: "runs only the tiers without a terminal event"; D67), so rpc.1 and rpc.2
/// both run under it and the lost primary's delete is the one that stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_whose_only_admin_is_lost_mid_tier_finishes_under_the_admin_the_fabric_primary_creates() {
    let run = primary_lost_mid_leave("a_leave_whose_only_admin_is_lost_mid_tier_finishes_under_the_admin_the_fabric_primary_creates", 1, Rpc1::Stopped).await;
    let created: Vec<&Value> = named(&run.spans, "rdm.node_admin.node.create.via-leaving-mesh").into_iter().filter(|sp| sp["attributes"]["outcome"] == "created").collect();
    assert_eq!(created.len(), 1, "the fabric-primary created one admin for mesh2: {created:?}");
    let created_at = Estate::path("mesh2.node-admin", "2");
    assert!(rdm_test::estate::at_location(created[0], &created_at), "{}", created[0]);
    assert!(start_ns(created[0]) >= run.resubmitted_ns, "the admin is created in the resubmit: {}", created[0]);
    // mesh2's primary committed its own mesh row Deleting when it admitted delete-mesh and declared
    // it up to the fabric-primary (DeclareMeshState, "report committed state upward"), which
    // applied it before the primary was lost and before it created the admin.
    let declared: Vec<&Value> = named(&run.spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|sp| sp["attributes"]["op"] == "declare-mesh-state" && s(&sp["attributes"]["sender"]) == run.lost.1 && sp["attributes"]["outcome"] == "applied" && s(&sp["attributes"]["node_path"]).starts_with("mesh1."))
        .filter(|sp| start_ns(sp) < start_ns(created[0]))
        .collect();
    assert!(declared.iter().any(|sp| sp["attributes"]["state"] == "Deleting"), "the fabric-primary applied mesh2's declared Deleting before it created the admin: {declared:?}");
    // The created admin holds the mesh's current state before its Ready: mesh2 is deleting, and a
    // mesh's state only moves forward (states.md), never back to Pending or ready. It
    // takes it from its maker's topology at entry or from the hand-off, which answers
    // already-applied when the entry pull brought it first.
    let handed: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.update.via-state-handoff").into_iter().filter(|sp| rdm_test::estate::names_location(sp, "target", &created_at) && (sp["attributes"]["outcome"] == "applied" || sp["attributes"]["outcome"] == "already-applied")).collect();
    assert!(!handed.is_empty() && handed.iter().all(|sp| sp["attributes"]["state"] == "Deleting"), "the created admin is handed mesh2's Deleting: {handed:?}");
    let pending: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|sp| rdm_test::estate::names_location(sp, "target", &created_at)).collect();
    assert!(pending.is_empty(), "the created admin is never handed Pending: {pending:?}");
    let completion = assert_resumed(&run, &created_at, 4);
    // Resume step 2: every silent node-admin of the mesh's records, the lost primary among them, is
    // deleted before anything is created for the mesh.
    let lost_terminate: Vec<&Value> = named(&run.spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &run.lost.0) && sp["attributes"]["step"] == "TerminateRuntime" && s(&sp["attributes"]["build_id"]) == run.b && sp["attributes"]["outcome"] == "complete").collect();
    assert!(lost_terminate.iter().all(|sp| sp["end_unix_nano"].as_u64().unwrap_or(0) <= start_ns(created[0])), "the lost primary is deleted before the admin is created: {lost_terminate:?}");
    let outer = "mesh.delete";
    // The created admin runs the delete over the mesh's records at the step (D67): the LeavingRoster
    // event the lost primary wrote stands, and the roster is read from the records, its own birth
    // among them.
    let leaving: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-accepted-roster").into_iter().filter(|sp| s(&sp["attributes"]["mesh_id"]) == run.mesh2_id && sp["attributes"]["step"] == "LeavingRoster" && start_ns(sp) >= run.resubmitted_ns).collect();
    assert!(!leaving.is_empty() && leaving.iter().all(|sp| sp["attributes"]["source"] == "records-resumed" && attr_u(sp, "roster") == 4), "the created admin reads the roster from the mesh's records, the lost primary's LeavingRoster event standing: {leaving:?}");
    let resumed: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-retained-receipts").into_iter().filter(|sp| sp["attributes"]["operation"] == outer && s(&sp["attributes"]["mesh_id"]) == run.mesh2_id && start_ns(sp) >= run.resubmitted_ns).collect();
    // The fault service held rpc.2 and rpc.1 stopped, neither with a TerminateRuntime event: the lost primary's
    // finished delete is the one that stands, and both ordinary members run under the created admin.
    // The new mesh-primary itself is no tier member.
    assert!(
        resumed.iter().any(|sp| attr_u(sp, "roster") == 4 && s(&sp["attributes"]["retained_births"]) == run.lost.1 && attr_u(sp, "remaining") == 2),
        "the created admin runs only the members with no terminal event over the mesh's records, the lost primary's delete standing: {resumed:?}"
    );
    let tier: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.update.via-leave-tier").into_iter().filter(|sp| sp["attributes"]["operation"] == outer && sp["attributes"]["tier"] == "ordinary-nodes" && start_ns(sp) >= run.resubmitted_ns && sp["attributes"]["outcome"] == "complete").collect();
    assert!(tier.iter().any(|sp| attr_u(sp, "members") == 2), "the created admin runs both ordinary members, the stopped rpc.1 it holds no record of included: {tier:?}");
    eprintln!("mesh_delete_no_survivor: created admin trace {}, mesh.deleted trace {}", s(&created[0]["trace_id"]), s(&completion["trace_id"]));
}
