//! The round of a mesh.delete, as the two Status bodies of the mesh delete command pair name it
//! (node-rpc-envelope.md, "Mesh delete command pair": "The workflow identity is (mesh_id, build_id,
//! attempt, operation) ... A stale/conflicting round or incomplete manifest is refused by name with
//! a typed Status refusal naming the mismatch"). The leaving mesh-primary decides a `DeleteMesh` on
//! the round of the leave it holds (`mesh_delete::MeshDeleteRuns::admit`, which its door
//! `admin::mesh_delete::admit_delete_mesh` answers from); the fabric-primary decides a `MeshDeleted` on the
//! rounds it holds open (`mesh_delete::MeshDeleteBook::admitted_for`, which its door `validate_handoff`
//! answers from).
// @feature: node-lifecycle

use crate::mesh_delete_resume::{capture, trace_id};
use rdm_core::{BuildId, MeshId, NodeId};
use rdm_admin::deployment::pipeline::RuntimeProof;
use rdm_admin::mesh_delete::{admit_delete_mesh, operation, run_admitted, Admitted, ExitManifest, MeshDeleteAdmission, MeshDeleteBook, MeshDeleteKey, DeleteMeshAdmission, MeshDeleteRunEnd, MeshDeleteRuns, RosterBirth};
use rdm_admin::status_rpc::Declared;
use rdm_admin::storage::{MemoryMeshStorage, MeshStatusRow, MeshStorage};
use rdm_core::status::{MeshDeleteRound, MeshState, StatusReply};
use serde_json::Value;

const STALE: &str = "rdm.node_admin.mesh.reject.via-stale-round";
const CONFLICTING: &str = "rdm.node_admin.mesh.reject.via-conflicting-round";

/// An owner of a leave, named by a fresh node id.
fn owner_named() -> rdm_admin::mesh_delete::MeshDeleteOwner {
    let id = NodeId::mint();
    rdm_admin::mesh_delete::MeshDeleteOwner { name: id.to_string(), target: rdm_transport::NodeTarget::ExactNode(id) }
}

fn key(mesh_id: &MeshId, build_id: &str, attempt: u32) -> MeshDeleteKey {
    MeshDeleteKey { mesh_id: mesh_id.clone(), build_id: BuildId::parse(build_id).unwrap(), attempt, operation: operation(mesh_id) }
}

fn round(k: &MeshDeleteKey) -> MeshDeleteRound {
    MeshDeleteRound { build_id: k.build_id.to_string(), attempt: k.attempt, operation: k.operation.clone() }
}

fn final_primary() -> RosterBirth {
    RosterBirth { node: "mesh2.admin.1".into(), node_id: NodeId::mint().to_string(), incarnation_id: "i1".into() }
}

fn runtime() -> RuntimeProof {
    RuntimeProof { deployment_id: "d".into(), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: "l".into() }
}

/// The cell's spans, written to `target/cells/mesh_delete_round/<cell>/spans.json`; its trace id and
/// that path are printed.
fn keep(cell: &str, trace_id: &str, spans: &[Value]) {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cells/mesh_delete_round").join(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(spans).unwrap()).unwrap();
    println!("cell={cell} trace_id={trace_id} spans={}", dir.join("spans.json").display());
}

/// The reject spans named `name` in the cell's own trace, with the round each names.
fn refusals(spans: &[Value], trace_id: &str, name: &str) -> Vec<Value> {
    spans.iter().filter(|s| s["name"] == name && s["trace_id"] == trace_id).cloned().collect()
}

/// The attributes a round reject span carries: the call, the round held and the round offered.
fn named_rounds(span: &Value) -> (String, String, String, String, String, String, String) {
    let a = |k: &str| span["attributes"][k].as_str().unwrap_or_default().to_string();
    (a("call"), a("held_build_id"), a("held_attempt"), a("held_operation"), a("build_id"), a("attempt"), a("operation"))
}

/// CONTRACT (node-rpc-envelope.md, "Mesh delete command pair"): the leaving mesh-primary holds the
/// round of the `delete-mesh` it admitted last. A `delete-mesh` of an earlier attempt of that Build is
/// `RejectedStaleRound { held, offered }`; one of another Build is `RejectedConflictingRound { held,
/// offered }`. Neither is admitted: the run keeps its round and its handoff goes to the sender of the
/// round held. After the manifest is recorded the same holds: the round held resends its handoff and
/// an earlier attempt is still stale. Each refusal is its reject span (`via-stale-round`,
/// `via-conflicting-round`) naming the call, the round held and the round offered. What must NOT
/// happen: a stale or conflicting `delete-mesh` admitted (`Running`, `Start`), or the handoff moved to
/// its sender.
#[test]
fn a_delete_mesh_of_an_earlier_attempt_or_another_build_is_refused_by_name_and_the_run_keeps_its_round() {
    let cap = capture();
    let mesh_id = MeshId::mint();
    let (held, stale, other) = (key(&mesh_id, "ay59nvr5jw53", 2), key(&mesh_id, "ay59nvr5jw53", 1), key(&mesh_id, "k4eyvfmr6wmf", 1));
    let (owner, late) = (owner_named(), owner_named());
    let last = final_primary();
    let manifest = ExitManifest { mesh_id: mesh_id.to_string(), roster: vec![last.clone()], final_primary: last, final_runtime: Some(runtime()), receipts: vec![] };
    let runs = MeshDeleteRuns::default();
    let ((running, end, exited), trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.mesh.reject.via-delete-mesh-round");
        let trace_id = trace_id(&root);
        let out = tracing::Instrument::instrument(
            async {
                let running = vec![runs.admit(&held, &owner), runs.admit(&stale, &late), runs.admit(&other, &late), runs.admit(&held, &owner)];
                let m = manifest.clone();
                let end = run_admitted(&runs, &mesh_id, move |_, _| {
                    let m = m.clone();
                    async move { Ok::<ExitManifest, String>(m) }
                })
                .await;
                let exited = vec![runs.admit(&stale, &late), runs.admit(&held, &owner)];
                (running, end, exited)
            },
            root,
        )
        .await;
        (out, trace_id)
    });
    assert_eq!(
        running,
        vec![
            MeshDeleteAdmission::Start,
            MeshDeleteAdmission::Refused(StatusReply::RejectedStaleRound { held: round(&held), offered: round(&stale) }),
            MeshDeleteAdmission::Refused(StatusReply::RejectedConflictingRound { held: round(&held), offered: round(&other) }),
            MeshDeleteAdmission::Running,
        ],
        "while the members run, an earlier attempt is stale and another Build conflicts; the round held is the same leave"
    );
    assert_eq!(end, MeshDeleteRunEnd::Exited { key: held.clone(), owner: owner.clone(), manifest: manifest.clone() }, "the run ends under the round held, its handoff to that round's sender");
    assert_eq!(exited, vec![MeshDeleteAdmission::Refused(StatusReply::RejectedStaleRound { held: round(&held), offered: round(&stale) }), MeshDeleteAdmission::Resend { manifest, recorded: held.attempt }], "after the manifest, an earlier attempt is still stale and the round held resends");
    let spans = cap.spans();
    keep("delete_mesh_round", &trace_id, &spans);
    let stale_spans = refusals(&spans, &trace_id, STALE);
    let conflicting_spans = refusals(&spans, &trace_id, CONFLICTING);
    let named = |k: &MeshDeleteKey| (k.build_id.to_string(), k.attempt.to_string(), k.operation.clone());
    let expect = |offered: &MeshDeleteKey| {
        let (h, o) = (named(&held), named(offered));
        ("delete-mesh".to_string(), h.0, h.1, h.2, o.0, o.1, o.2)
    };
    assert_eq!(stale_spans.iter().map(named_rounds).collect::<Vec<_>>(), vec![expect(&stale), expect(&stale)], "{stale_spans:?}");
    assert_eq!(conflicting_spans.iter().map(named_rounds).collect::<Vec<_>>(), vec![expect(&other)], "{conflicting_spans:?}");
}

/// CONTRACT (node-rpc-envelope.md, "Mesh delete command pair": the fabric-primary "answers MeshDeleted
/// Applied or AlreadyApplied after handoff validation"; "A stale/conflicting round ... is refused by
/// name"): the fabric-primary holds attempt 2 of the leave open. A `mesh-deleted` naming attempt 1 is
/// `RejectedStaleRound { held, offered }`; one naming attempt 3, which the owner never opened, or
/// another Build, is `RejectedConflictingRound { held, offered }`; the round held is admitted. A
/// refused round ends nothing: the open leave still waits for its handoff. A `mesh-deleted` for a mesh
/// with no open leave names no open command (`RejectedUnmatchedCompletion { field: "command" }`).
/// Each round refusal is its reject span naming the call, the round held and the round offered. What
/// must NOT happen: a round other than the one held admitted, or the open leave ended by it.
#[test]
fn a_mesh_deleted_report_naming_a_round_the_fabric_primary_does_not_hold_is_refused_by_name() {
    let cap = capture();
    let mesh_id = MeshId::mint();
    let (held, stale, unopened, other) = (key(&mesh_id, "ay59nvr5jw53", 2), key(&mesh_id, "ay59nvr5jw53", 1), key(&mesh_id, "ay59nvr5jw53", 3), key(&mesh_id, "k4eyvfmr6wmf", 2));
    let elsewhere = key(&MeshId::mint(), "xe5k179s7rw5", 1);
    let last = final_primary();
    let admitted = Admitted { roster: vec![last.clone()], final_primary: last.clone(), final_runtime: runtime(), amendments: Vec::new() };
    let book = MeshDeleteBook::default();
    let handoff = book.open(held.clone(), admitted);
    let ((refused, accepted, unknown), trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.mesh.reject.via-mesh-deleted-round");
        let trace_id = trace_id(&root);
        let out = tracing::Instrument::instrument(
            async {
                let refused: Vec<Result<(), StatusReply>> = [&stale, &unopened, &other].into_iter().map(|k| book.admitted_for(k).map(|_| ())).collect();
                let accepted = book.admitted_for(&held).map(|a| a.final_primary);
                let unknown = book.admitted_for(&elsewhere).map(|_| ());
                (refused, accepted, unknown)
            },
            root,
        )
        .await;
        (out, trace_id)
    });
    assert_eq!(
        refused,
        vec![
            Err(StatusReply::RejectedStaleRound { held: round(&held), offered: round(&stale) }),
            Err(StatusReply::RejectedConflictingRound { held: round(&held), offered: round(&unopened) }),
            Err(StatusReply::RejectedConflictingRound { held: round(&held), offered: round(&other) }),
        ],
        "each round the owner does not hold open is refused by name, the round held and the one offered"
    );
    assert_eq!(accepted, Ok(last), "the round held is admitted");
    assert!(matches!(&unknown, Err(StatusReply::RejectedUnmatchedCompletion { field, reported, .. }) if field == "command" && reported.contains(elsewhere.build_id.as_str())), "a mesh with no open leave names no open command: {unknown:?}");
    assert!(handoff.borrow().is_none(), "a refused round ends nothing: the open leave still waits for its handoff");
    let spans = cap.spans();
    keep("mesh_deleted_round", &trace_id, &spans);
    let named = |k: &MeshDeleteKey| (k.build_id.to_string(), k.attempt.to_string(), k.operation.clone());
    let expect = |offered: &MeshDeleteKey| {
        let (h, o) = (named(&held), named(offered));
        ("mesh-deleted".to_string(), h.0, h.1, h.2, o.0, o.1, o.2)
    };
    assert_eq!(refusals(&spans, &trace_id, STALE).iter().map(named_rounds).collect::<Vec<_>>(), vec![expect(&stale)]);
    assert_eq!(refusals(&spans, &trace_id, CONFLICTING).iter().map(named_rounds).collect::<Vec<_>>(), vec![expect(&unopened), expect(&other)]);
}

/// CONTRACT (states.md: the mesh's `leaving` is entered by the `delete-mesh` down op, "one commit
/// stores the row, applies it, gossips it and answers the op"; mesh-delete.md step 5: at mesh.deleted
/// the mesh-primary writes the mesh row deleted; node-rpc-envelope.md, "Mesh delete command pair":
/// `DeleteMesh` answers Applied or AlreadyApplied on admission, and a stale round is refused by name):
/// the first `delete-mesh` of the mesh commits its row Leaving and is admitted. Once the members exited
/// and the row is Dead (mesh.deleted, the handoff's own write), a `delete-mesh` of the round held
/// replays the recorded MeshDeleted report, an earlier attempt is `RejectedStaleRound`, and the Build's
/// next attempt is replayed the same recorded report, naming the attempt its manifest was recorded under,
/// with no member workflow run again (Luke 2026-10-13: "Re-admitting DeleteMesh must replay the recorded
/// MeshDeleted report without repeating completed deletions"); none of them commits the row again,
/// which stays Dead. What must
/// NOT happen: a round after mesh.deleted answered `RejectedInvalidMeshTransition { current: Dead }`
/// (a lost handoff could then never be sent again, and the leave never ends), or a second Leaving
/// commit.
#[test]
fn a_delete_mesh_after_mesh_deleted_is_decided_on_its_round_and_commits_the_leaving_row_once() {
    let cap = capture();
    let mesh_id = MeshId::mint();
    let (held, stale, next) = (key(&mesh_id, "ay59nvr5jw53", 2), key(&mesh_id, "ay59nvr5jw53", 1), key(&mesh_id, "ay59nvr5jw53", 3));
    let (owner, late) = (owner_named(), owner_named());
    let last = final_primary();
    let manifest = ExitManifest { mesh_id: mesh_id.to_string(), roster: vec![last.clone()], final_primary: last, final_runtime: Some(runtime()), receipts: vec![] };
    let (runs, storage, declared) = (MeshDeleteRuns::default(), MemoryMeshStorage::default(), std::sync::Mutex::new(Declared::default()));
    let ((first, end, after, row), trace_id) = cap.run(async {
        let root = tracing::info_span!("rdm.test.mesh.update.via-delete-mesh-after-deleted");
        let trace_id = trace_id(&root);
        let out = tracing::Instrument::instrument(
            async {
                let first = admit_delete_mesh(&runs, &storage, &declared, "mesh2", &held, &owner).await;
                let m = manifest.clone();
                let end = run_admitted(&runs, &mesh_id, move |_, _| {
                    let m = m.clone();
                    async move { Ok::<ExitManifest, String>(m) }
                })
                .await;
                // mesh.deleted: the mesh-primary's handoff commits its mesh row Dead before it calls
                // mesh-deleted; the call is lost, so the owner sends delete-mesh again.
                storage.put_mesh_status("mesh2", &MeshStatusRow { mesh_id: mesh_id.clone(), state: MeshState::Dead }).await.unwrap();
                declared.lock().unwrap().meshes.insert(mesh_id.clone(), MeshState::Dead);
                let mut after = Vec::new();
                for (k, sender) in [(&held, &owner), (&stale, &late), (&next, &owner)] {
                    after.push(admit_delete_mesh(&runs, &storage, &declared, "mesh2", k, sender).await);
                }
                let row = storage.mesh_statuses().await.unwrap().get(&mesh_id).copied();
                (first, end, after, row)
            },
            root,
        )
        .await;
        (out, trace_id)
    });
    let spans = cap.spans();
    keep("delete_mesh_after_deleted", &trace_id, &spans);
    assert_eq!(first, DeleteMeshAdmission { leaving_row: Some(StatusReply::Applied), admission: MeshDeleteAdmission::Start }, "the first delete-mesh commits the row Leaving and starts the leave");
    assert_eq!(end, MeshDeleteRunEnd::Exited { key: held.clone(), owner: owner.clone(), manifest: manifest.clone() });
    assert_eq!(
        after,
        vec![
            DeleteMeshAdmission { leaving_row: None, admission: MeshDeleteAdmission::Resend { manifest: manifest.clone(), recorded: held.attempt } },
            DeleteMeshAdmission { leaving_row: None, admission: MeshDeleteAdmission::Refused(StatusReply::RejectedStaleRound { held: round(&held), offered: round(&stale) }) },
            DeleteMeshAdmission { leaving_row: None, admission: MeshDeleteAdmission::Resend { manifest, recorded: held.attempt } },
        ],
        "after mesh.deleted each delete-mesh is decided on its round, and none commits the row again"
    );
    assert_eq!(row, Some(MeshState::Dead), "the mesh row stays Dead");
    assert_eq!(refusals(&spans, &trace_id, STALE).len(), 1, "the stale round is refused by its own span");
}
