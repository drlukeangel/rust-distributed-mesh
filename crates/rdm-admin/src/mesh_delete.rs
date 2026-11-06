//! The mesh-delete workflow's shared parts (mesh-delete.md; node-rpc-envelope.md, "Mesh delete
//! command pair").
//!
//! The fabric-primary owns the outer workflow `mesh.delete:<mesh_id>`. The deleting mesh's primary
//! deletes every other member with the whole node.delete (ordinary nodes first, the other
//! node-admins after, each tier a set of parallel member workflows), records one exit manifest, hands it to the
//! fabric-primary (`MeshDeleted`, mesh.deleted) and keeps running. The fabric-primary validates the
//! manifest against the roster it captured at admission and records mesh.deleted's completion
//! (`MeshDeleted`): the mesh.delete ends there, its primary alive. The primary's end is a separate
//! node.delete with the fabric-primary as executor; once that node.delete holds the final birth's
//! exact exited verdict, every birth of the mesh is proven exited and the fabric-primary records
//! `MeshLeft` and publishes mesh-left (R-MD1 items 2-3).
//!
//! Each manifest is one immutable step receipt of the accepted Build under the outer operation
//! (`OtherMembersExited` at the mesh-primary; `MeshDeleted` and `MeshLeft` at the fabric-primary);
//! the structured `receipt_manifest` of the messages is its reference. The fabric-primary reads the
//! mesh-primary's from the Build's committed records in storage (Q292), never from a reference alone.

use crate::build::BuildId;
use crate::build_state::{BuildFact, BuildStateAdapter, BuildStateError, BuildStep, StepOutcome, StepWrite};
use crate::deployment::pipeline::{RuntimeIdentity, TerminalReceipt};
use crate::model::{Node};
use futures_util::future::join_all;
use rdm_core::{MeshId, NodeId};
use rdm_core::status::{MeshDeleteIdentity, StatusReply, StatusRequest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::watch;

/// The step at the leaving mesh-primary that records every other member's exit.
pub const STEP_OTHER_MEMBERS_EXITED: &str = "OtherMembersExited";
/// The step at the fabric-primary that records mesh.deleted (mesh-delete.md step 5): the validated
/// handoff's manifest, every birth of the mesh but its final primary, which is still running.
pub const STEP_MESH_DELETED: &str = "MeshDeleted";
/// The step at the fabric-primary that records the complete departure: every birth of the mesh, the
/// final primary included, proven exited (R-MD1 item 2), once the final primary's own node.delete
/// holds its exact exited verdict.
pub const STEP_MESH_LEFT: &str = "MeshLeft";
/// The step at the fabric-primary that records the accepted roster it captured at admission
/// (mesh-delete.md step 1).
pub const STEP_ACCEPTED_ROSTER: &str = "AcceptedRoster";
/// The step at the leaving mesh-primary that records the roster its member workflows run over.
pub const STEP_LEAVING_ROSTER: &str = "LeavingRoster";

/// The outer operation key of `mesh_id`'s shutdown.
pub fn operation(mesh_id: &MeshId) -> rdm_core::OperationId {
    rdm_core::OperationId::new("mesh.delete", rdm_core::OperationSubject::Mesh(mesh_id.clone()))
}

/// The reference a message carries for a manifest receipt.
pub fn manifest_reference(build_id: &str, operation: &rdm_core::OperationId, all_members: bool) -> rdm_core::status::ManifestReference {
    rdm_core::status::ManifestReference {
        build_id: build_id.to_owned(), operation: operation.clone(),
        scope: if all_members { rdm_core::status::ManifestScope::AllMembersExited } else { rdm_core::status::ManifestScope::OtherMembersExited },
    }
}

/// One accepted birth of the leaving mesh.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RosterBirth {
    /// The explicitly declared owning mesh.
    pub mesh: String,
    /// The explicitly declared type.
    pub r#type: String,
    /// The node's `(path, name)`.
    pub node: crate::model::PathName,
    /// The logical node.
    pub node_id: String,
    /// The exact birth.
    pub incarnation_id: String,
}

impl RosterBirth {
    /// The birth `n` holds.
    pub fn of(n: &Node) -> Result<Self, String> {
        let incarnation_id = n.incarnation_id.as_ref().map(|i| i.to_string()).ok_or_else(|| format!("{} has no known birth", n.node_id))?;
        Ok(Self { mesh: n.mesh.clone(), r#type: n.r#type.clone(), node: n.location(), node_id: n.node_id.to_string(), incarnation_id })
    }

    /// The exact birth the roster names, refused by name when its incarnation is not a Crockford id
    /// (a roster of an older format).
    pub fn birth(&self) -> Result<crate::model::IncarnationId, rdm_core::IdError> {
        crate::model::IncarnationId::parse(&self.incarnation_id)
    }

    /// The record of this exact birth the accepted roster alone gives (R11: the roster decides
    /// what runs): its `(path, name)`, node id and incarnation, `Leaving` as a birth of a leaving
    /// mesh. The member workflow reads the rest where it reads it for any birth (its runtime from
    /// the birth's durable runtime row). It carries no config: no row is written from it.
    pub fn record(&self) -> Result<Node, String> {
        let name = self.node.clone();
        let mut n = Node::allocated(name, self.mesh.clone(), &self.r#type);
        n.node_id = NodeId::parse(&self.node_id).map_err(|e| format!("the roster birth {}'s node id {} does not parse: {e}", self.node_id, self.node_id))?;
        n.incarnation_id = Some(self.birth().map_err(|e| format!("the roster birth {}: {e}", self.node_id))?);
        n.status = crate::model::NodeStatus::Leaving;
        Ok(n)
    }
}

/// The exit manifest: the accepted roster, and the provider's terminal receipt of every birth that
/// exited. `OtherMembersExited` and `MeshDeleted` cover every birth but the final primary (whose
/// exact runtime is named, still running); `MeshLeft` covers every birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExitManifest {
    /// The mesh's minted id.
    pub mesh_id: String,
    /// Every accepted birth of the mesh, the final primary included.
    pub roster: Vec<RosterBirth>,
    /// The final mesh-primary.
    pub final_primary: RosterBirth,
    /// The final primary's exact runtime by fingerprint, when `OtherMembersExited` or `MeshDeleted`
    /// names it still running; `None` in `MeshLeft`, whose receipts include the final primary's exit.
    pub final_runtime: Option<RuntimeIdentity>,
    /// One terminal receipt per birth that exited.
    pub receipts: Vec<TerminalReceipt>,
}

/// The first field on which `manifest` does not cover `roster` and `final_primary` exactly, as
/// `(field, expected, reported)`: no missing birth, no extra birth, no substitution, and every
/// receipt binds its roster birth. `with_final` says whether the final primary's own exit is
/// among the receipts (`MeshLeft`) or not (the handoff).
pub fn first_mismatch(manifest: &ExitManifest, roster: &[RosterBirth], final_primary: &RosterBirth, with_final: bool) -> Option<(String, String, String)> {
    let join = |v: Vec<String>| if v.is_empty() { "none".to_string() } else { v.join(", ") };
    let mut want: Vec<&RosterBirth> = roster.iter().collect();
    want.sort();
    let mut have: Vec<&RosterBirth> = manifest.roster.iter().collect();
    have.sort();
    if want != have {
        return Some(("roster".into(), join(want.iter().map(|b| format!("{} {}", b.node_id, b.incarnation_id)).collect()), join(have.iter().map(|b| format!("{} {}", b.node_id, b.incarnation_id)).collect())));
    }
    if &manifest.final_primary != final_primary {
        return Some(("final_primary".into(), format!("{} {}", final_primary.node_id, final_primary.incarnation_id), format!("{} {}", manifest.final_primary.node_id, manifest.final_primary.incarnation_id)));
    }
    let mut needed: BTreeMap<(&str, &str), ()> = roster.iter().filter(|b| with_final || *b != final_primary).map(|b| ((b.node_id.as_str(), b.incarnation_id.as_str()), ())).collect();
    for r in &manifest.receipts {
        if needed.remove(&(r.node_id.as_str(), r.incarnation_id.as_str())).is_none() {
            return Some(("receipt".into(), "one receipt per roster birth".into(), format!("an unexpected or repeated receipt for {} {}", r.node_id, r.incarnation_id)));
        }
    }
    if let Some(((node_id, inc), _)) = needed.into_iter().next() {
        return Some(("receipt".into(), format!("a terminal receipt for {node_id} {inc}"), "none".into()));
    }
    None
}

/// The roster of the leave `key` (mesh-delete.md step 1), under `step` (`AcceptedRoster` at the
/// fabric-primary, `LeavingRoster` at the leaving mesh-primary): the mesh's births as its node records
/// hold them at the step (`capture`, [`stored_roster`]; D67: the topology of the moment is the records,
/// and a birth that exited keeps its `Deleted` record, so a resumed run reads the same births). The
/// step's `rdm.build.step` event is written the first time, with no output.
pub async fn accepted_roster(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, step: &str, executor: &str, capture: impl FnOnce() -> Result<Vec<RosterBirth>, String>) -> Result<Vec<RosterBirth>, String> {
    let build_id = key.build_id.clone();
    let operation = &key.operation;
    let view = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the {step} event of {operation}: {e}"))?;
    let roster = capture()?;
    let written = view.completed(&operation.to_string(), step);
    if !written {
        builds
            .write_step(&StepWrite { build_id: build_id.clone(), mesh: None, operation: operation.clone(), step: step.to_string(), outcome: StepOutcome::Complete })
            .await
            .map_err(|e| format!("{executor}: the {step} event of {operation} could not be written: {e}"))?;
    }
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-accepted-roster", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %operation, step, source = if written { "records-resumed" } else { "records" }, roster = roster.len())
        .in_scope(|| tracing::info!("the leave runs over the mesh's births as its node records hold them"));
    Ok(roster)
}

/// The proof that a roster birth's runtime exited: its own node.delete's (`node.delete:<node_id>`)
/// `TerminateRuntime` step ended `complete`, which the provider's exact Exited alone completes
/// (mesh-delete.md step 3). The receipt names the birth; its runtime is the one the birth's durable
/// runtime row names (D67), not carried here.
fn exit_of(b: &RosterBirth) -> TerminalReceipt {
    TerminalReceipt { path: b.node.path.clone(), name: b.node.name.clone(), node_id: b.node_id.clone(), incarnation_id: b.incarnation_id.clone(), runtime: None, exit_code: None, verdict: None, data_dir: None }
}

/// The births of `roster` whose own node.delete's `TerminateRuntime` step ended `complete` in `steps`:
/// the provider's proof that each runtime exited, not that the delete of its birth is finished
/// ([`members_exited`] runs a delete that holds only this proof on to its end).
pub fn exit_receipts(steps: &[BuildStep], roster: &[RosterBirth]) -> BTreeMap<(String, String), TerminalReceipt> {
    let terminate = crate::deployment::pipeline::DeleteStep::TerminateRuntime.name();
    roster
        .iter()
        .filter(|b| NodeId::parse(&b.node_id).is_ok_and(|id| completed(steps, &rdm_core::OperationId::node("node.delete", &id).to_string(), terminate)))
        .map(|b| ((b.node_id.clone(), b.incarnation_id.clone()), exit_of(b)))
        .collect()
}

/// Whether `operation`'s `step` ended `complete` in `steps`.
fn completed(steps: &[BuildStep], operation: &str, step: &str) -> bool {
    steps.iter().any(|s| s.operation == operation && s.step == step && s.outcome == StepOutcome::Complete)
}

/// Whether `b`'s node.delete is finished in `steps`: its `NodeDeleted` and its `Complete` steps ended
/// `complete`. A delete cut anywhere after its exited verdict is owed its remaining steps.
pub(crate) fn finished_delete(steps: &[BuildStep], b: &RosterBirth) -> bool {
    let Ok(node_id) = NodeId::parse(&b.node_id) else { return false };
    let operation = rdm_core::OperationId::node("node.delete", &node_id).to_string();
    completed(steps, &operation, crate::deployment::pipeline::DeleteStep::NodeDeleted.name()) && completed(steps, &operation, crate::deployment::pipeline::DeleteStep::Complete.name())
}

/// Where the fabric-primary's outer workflow `operation` resumes, read from the Build's events before
/// it calls anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerResume {
    /// The `MeshLeft` step ended `complete`: the departure is the record.
    Left,
    /// mesh.deleted's `MeshDeleted` step ended `complete`: the mesh.delete ended, its final primary
    /// alive. Nothing is called: its end is a separate node.delete, and mesh-left follows that
    /// node.delete's exited verdict.
    Deleted,
    /// Nothing settles the mesh.delete: the owner leads it (admission, `DeleteMesh`, the handoff, the
    /// completion).
    Lead,
}

/// [`OwnerResume`] for `operation` (`mesh.delete:<mesh_id>`) over `steps`.
pub fn owner_resume(steps: &[BuildStep], operation: &str) -> OwnerResume {
    if completed(steps, operation, STEP_MESH_LEFT) {
        OwnerResume::Left
    } else if completed(steps, operation, STEP_MESH_DELETED) {
        OwnerResume::Deleted
    } else {
        OwnerResume::Lead
    }
}

/// Record mesh.deleted's completion (mesh-delete.md step 5) for the mesh.delete `key`: the `MeshDeleted`
/// step's event, by `executor`, the fabric-primary that owns the mesh.delete, once `manifest` (every
/// birth of the mesh but its final primary proven exited) is validated. One
/// `rdm.node_admin.mesh.update.via-mesh-deleted-receipt` span.
pub async fn record_completion(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, executor: &str, manifest: &ExitManifest) -> Result<(), String> {
    builds
        .write_step(&StepWrite { build_id: key.build_id.clone(), mesh: None, operation: key.operation.clone(), step: STEP_MESH_DELETED.into(), outcome: StepOutcome::Complete })
        .await
        .map_err(|e| format!("{}: the {STEP_MESH_DELETED} event could not be written: {e}", key.operation))?;
    tracing::info_span!("rdm.node_admin.mesh.update.via-mesh-deleted-receipt", executor = %executor, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation, receipts = manifest.receipts.len(), roster = manifest.roster.len(), final_primary_node_id = %manifest.final_primary.node_id)
        .in_scope(|| tracing::info!("mesh.deleted: every birth but the final primary is proven exited; the mesh.delete ends with its primary alive, whose end is its own node.delete"));
    Ok(())
}

/// The exit manifest of the leave `key` over `roster` with `final_primary`, as `steps` prove it: one
/// receipt per roster birth whose own node.delete's `TerminateRuntime` ended `complete`, the final
/// primary's included only `with_final`.
pub fn manifest_of(key: &MeshDeleteKey, roster: &[RosterBirth], final_primary: &RosterBirth, final_runtime: Option<RuntimeIdentity>, steps: &[BuildStep], with_final: bool) -> ExitManifest {
    let exits = exit_receipts(steps, roster);
    let receipts = roster.iter().filter(|b| with_final || *b != final_primary).filter_map(|b| exits.get(&(b.node_id.clone(), b.incarnation_id.clone())).cloned()).collect();
    ExitManifest { mesh_id: key.mesh_id.to_string(), roster: roster.to_vec(), final_primary: final_primary.clone(), final_runtime, receipts }
}

/// node.runtime.inspect of the lost birth `lost` (mesh-delete.md, "Resume after the primary's loss",
/// step 1: the lost final primary; "when none survives": another node-admin of the leaving mesh; a
/// member of an unanswered mesh during fabric.delete), read from the runtime the birth wrote on its own
/// record (`row`, a point read of the record of that exact birth): the one inspect
/// ([`crate::deployment::provider::inspect_runtime`]). Its answer is the verdict the birth's node.delete
/// acts on (node-recovery.md step 3: `TerminateRuntime` first when it runs). A birth with no record
/// names no runtime to act on.
pub async fn predecessor_verdict(provider: &dyn crate::deployment::provider::DeploymentProvider, row: Option<&crate::storage::NodeRecord>, lost: &RosterBirth) -> Result<crate::deployment::provider::Inspected, String> {
    let named = format!("{} {}", lost.node_id, lost.incarnation_id);
    let row = row.ok_or_else(|| format!("no node record of {named} was ever written: its runtime is unknown"))?;
    Ok(crate::deployment::provider::inspect_runtime(provider, &row.runtime, row.storage.data_dir.as_deref(), &lost.incarnation_id).await)
}

/// The first available generated name for a node-admin created in a leaving mesh.
/// The accepted roster and current view reserve separate path/name pairs. Existing fields are never parsed for ownership or ordinals.
pub fn leaving_mesh_admin_path(mesh: &str, roster: &[RosterBirth], held: &[crate::model::PathName]) -> Result<crate::model::PathName, String> {
    let occupied: std::collections::BTreeSet<_> = roster.iter().map(|b| b.node.clone()).chain(held.iter().cloned()).collect();
    for ordinal in 1..=u32::MAX {
        let candidate = crate::model::PathName::numbered(mesh, "node-admin", ordinal);
        if !occupied.contains(&candidate) {
            return Ok(candidate);
        }
    }
    Err(format!("{mesh}: generated admin names are exhausted"))
}

/// The roster the leaving primary `me` runs `DeleteMesh` over (mesh-delete.md step 1): its mesh's births
/// as its node records hold them now (`capture`, D67), its `LeavingRoster` event written the first time.
/// `me` must be a birth of it and the mesh's primary the fabric-primary sent `DeleteMesh` to; any other
/// birth refuses the run by name.
pub async fn leaving_roster(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, executor: &str, me: &RosterBirth, capture: impl FnOnce() -> Result<Vec<RosterBirth>, String>) -> Result<Vec<RosterBirth>, String> {
    let roster = accepted_roster(builds, key, STEP_LEAVING_ROSTER, executor, capture).await?;
    if !roster.contains(me) {
        return Err(format!("{}: {executor} ({} {}) is not a birth of the mesh's records, so no member is stopped", key.operation, me.node_id, me.incarnation_id));
    }
    Ok(roster)
}

/// The roster of `mesh` captured from its node records on the store, every birth that was a member when
/// this Build's delete began: each record's birth, by the mesh of its path; a `Deleted` record only when
/// this Build's own node.delete of that node deleted it (its `NodeDeleting` step ended `complete`), never
/// a birth an earlier Build deleted: a mesh deleted and created again under the name holds only `Deleted`
/// records of the deleted mesh's nodes. One topology Build runs at a time (build.md §6.9), so no create
/// is in flight across the delete. In path order.
pub fn stored_roster(rows: &[crate::storage::NodeRecord], mesh: &str, steps: &[BuildStep]) -> Vec<RosterBirth> {
    let deleting = crate::deployment::pipeline::DeleteStep::NodeDeleting.name();
    let began = |r: &crate::storage::NodeRecord| completed(steps, &rdm_core::OperationId::node("node.delete", &r.node_id).to_string(), deleting);
    let mut roster: Vec<RosterBirth> = rows
        .iter()
        .filter(|r| r.mesh() == mesh)
        .filter(|r| r.state != crate::model::NodeStatus::Deleted || began(r))
        .map(|r| RosterBirth { mesh: r.mesh(), r#type: r.r#type.clone(), node: r.location(), node_id: r.node_id.to_string(), incarnation_id: r.incarnation.as_str().to_string() })
        .collect();
    roster.sort();
    roster.dedup();
    roster
}

/// The records the leave's member workflows act on (R11: the leave acts on the immutable accepted
/// roster's exact births; mesh-delete.md "Failure/certainty behavior": "No timeout or gossip absence
/// proves exit"): every roster birth as `view` holds that exact birth (node id and incarnation), else
/// as `held` gives it, the record this admin's topology view holds of the birth. A birth the view does
/// not list and that has no Exited receipt is worked from the held record. A birth neither holds is
/// absent here, and runs from the roster birth itself
/// ([`members_exited`], [`RosterBirth::record`]).
pub fn roster_records(view: &[Node], roster: &[RosterBirth], held: impl Fn(&RosterBirth) -> Option<Node>) -> Vec<Node> {
    let exact = |b: &RosterBirth, n: &Node| n.node_id.to_string() == b.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.as_str() == b.incarnation_id);
    roster
        .iter()
        .filter_map(|b| {
            view.iter().find(|n| exact(b, n)).cloned().or_else(|| {
                let n = held(b).filter(|n| exact(b, n))?;
                tracing::info_span!("rdm.node_admin.mesh.resolve.via-held-record", member = %b.node_id, node_id = %b.node_id, incarnation_id = %b.incarnation_id, status = ?n.status)
                    .in_scope(|| tracing::info!("a roster birth the view no longer holds is worked from the record this admin's topology view holds of it"));
                Some(n)
            })
        })
        .collect()
}

/// The leaving mesh-primary's member workflows over the accepted `roster` (mesh-delete.md steps 3-4):
/// every ordinary node in parallel, then, only once every one has its exact terminal receipt, every
/// other node-admin in parallel; then the one exit manifest, recorded as the `OtherMembersExited`
/// step receipt of the accepted Build. A member whose node.delete the Build already holds every
/// step's receipt of (a successful child of an earlier, incomplete run, or the lost final primary's
/// delete the fabric-primary ran as a crashed target) keeps its receipt and is not run again: its
/// row was written `Deleted` and its `NodeDeleted` published by node.delete's own steps, once, and
/// nothing of it is written or published again here; a member whose delete holds its exact Exited
/// receipt and not every later step (the executor was cut after the provider's proof) runs on: its
/// receipted steps are reused and the steps without a receipt are executed in order, `NodeDeleted`
/// included. A failed or unrunnable member leaves its tier incomplete. `me` is the final primary
/// (still running, its exact runtime `final_runtime`); `records` are the Node records of the roster's
/// births ([`roster_records`]: the view's record, else the topology view's record of that exact birth),
/// and a roster birth they hold no record of runs from the roster birth itself
/// ([`RosterBirth::record`]): the accepted roster decides what runs (mesh-delete.md, "Resume after
/// the primary's loss" step 3); `shutdown` is one member's whole
/// node.delete (mesh-delete.md step 3), answering the provider's terminal receipt of its exact
/// runtime, its row `Deleted` and its `NodeDeleted` the delete's own.
#[allow(clippy::too_many_arguments)]
pub async fn members_exited<F, Fut>(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, executor: &str, roster: &[RosterBirth], me: &RosterBirth, final_runtime: RuntimeIdentity, records: &[Node], shutdown: F) -> Result<ExitManifest, String>
where
    F: Fn(Node) -> Fut,
    Fut: std::future::Future<Output = Result<TerminalReceipt, String>>,
{
    use tracing::Instrument;
    if !roster.contains(me) {
        return Err(format!("{executor} ({} {}) is not a birth of the roster accepted for {}: no member is stopped", me.node_id, me.incarnation_id, format!("{:?}", key.operation)));
    }
    let build_id = key.build_id.clone();
    let steps = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the events of {}: {e}", key.operation))?.steps;
    let exits = exit_receipts(&steps, roster);
    let mut receipts: Vec<TerminalReceipt> = Vec::new();
    let mut remaining: Vec<&RosterBirth> = Vec::new();
    // Births kept with their receipts: a member whose own delete finished.
    let mut retained: Vec<&RosterBirth> = Vec::new();
    // Births whose own node.delete holds the provider's exact Exited and not every later step.
    let mut owed: Vec<&RosterBirth> = Vec::new();
    for b in roster.iter().filter(|b| *b != me) {
        match exits.get(&(b.node_id.clone(), b.incarnation_id.clone())) {
            // The exited verdict is a step of the delete, not the delete: a birth is done once its delete
            // has every step's receipt. One that holds the proof alone is run on (its receipted steps
            // are reused, the rest executed in order, NodeDeleted included).
            Some(t) => {
                if finished_delete(&steps, b) {
                    receipts.push(t.clone());
                    retained.push(b);
                } else {
                    owed.push(b);
                    remaining.push(b);
                }
            }
            None => remaining.push(b),
        }
    }
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-retained-receipts", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, roster = roster.len(), retained = receipts.len(), remaining = remaining.len(), retained_births = %receipts.iter().map(|t| t.node_id.as_str()).collect::<Vec<_>>().join(","), delete_owed = %owed.iter().map(|b| b.node_id.clone()).collect::<Vec<_>>().join(","))
        .in_scope(|| tracing::info!("members whose delete has every step's receipt keep it; the rest run, a delete that holds only its exited verdict from the step it has no receipt for"));
    let is_admin = |b: &RosterBirth| rdm_core::is_node_admin(&b.r#type);
    for (tier, admins) in [("ordinary-nodes", false), ("node-admins", true)] {
        let mut failed = Vec::new();
        let mut members: Vec<Node> = Vec::new();
        for b in remaining.iter().filter(|b| is_admin(b) == admins) {
            match records.iter().find(|n| n.node_id.to_string() == b.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.as_str() == b.incarnation_id)) {
                Some(n) => members.push(n.clone()),
                // The accepted roster decides what runs, not what this admin holds: a birth it never
                // held a record of (it joined after that birth stopped) runs from the roster birth.
                None => match b.record() {
                    Ok(n) => {
                        tracing::info_span!("rdm.node_admin.mesh.resolve.via-roster-birth", member = %b.node_id, node_id = %b.node_id, incarnation_id = %b.incarnation_id, tier, operation = %key.operation.action, operation_subject = ?key.operation.subject)
                            .in_scope(|| tracing::info!("a roster birth this admin holds no record of runs from the accepted roster's exact birth"));
                        members.push(n);
                    }
                    Err(e) => failed.push(format!("{} {}: {e}", b.node_id, b.incarnation_id)),
                },
            }
        }
        let kept = retained.iter().filter(|b| is_admin(b) == admins).count();
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-leave-tier", tier, members = members.len(), retained = kept, mesh_id = %key.mesh_id, operation = %key.operation.action, operation_subject = ?key.operation.subject, outcome = tracing::field::Empty);
        let outcomes = join_all(members.iter().cloned().map(&shutdown)).instrument(span.clone()).await;
        for (n, r) in members.iter().zip(outcomes) {
            match r {
                Ok(t) => receipts.push(t),
                Err(e) => failed.push(format!("{}: {e}", n.node_id)),
            }
        }
        span.record("outcome", if failed.is_empty() { "complete" } else { "incomplete" });
        if !failed.is_empty() {
            return Err(format!("tier {tier} is incomplete, the next tier does not start; members with an Exited receipt keep it for the next run: {}", failed.join("; ")));
        }
    }
    let manifest = ExitManifest { mesh_id: key.mesh_id.to_string(), roster: roster.to_vec(), final_primary: me.clone(), final_runtime: Some(final_runtime), receipts };
    builds
        .write_step(&crate::build_state::StepWrite { build_id: key.build_id.clone(), mesh: None, operation: key.operation.clone(), step: STEP_OTHER_MEMBERS_EXITED.into(), outcome: StepOutcome::Complete })
        .await
        .map_err(|e| format!("the exit manifest could not be recorded: {e}"))?;
    tracing::info_span!("rdm.node_admin.mesh.update.via-exit-manifest-recorded", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, receipts = manifest.receipts.len(), roster = manifest.roster.len())
        .in_scope(|| tracing::info!("every other member's exact exit is proven: the exit manifest is recorded"));
    Ok(manifest)
}

/// mesh-left at the fabric-primary, after exact coverage and before the `MeshLeft` receipt: the
/// final birth's node.delete wrote its row `Deleted` and proved its exit, so every birth of the mesh
/// is proven exited. `rows` is the fabric-primary's writer, writing as acting primary of a mesh that
/// has none left; `proof` the complete proof set; the mesh's state deleted is ensured on `meshes` and
/// held in `declared`, and the Mesh row deleted on `meshes`: writing the deleted record frees the
/// name (R-MD1 item 2, R-MN1 item 2).
#[allow(clippy::too_many_arguments)]
pub async fn mesh_left_rows(rows: Option<&crate::node_rows::NodeRows>, meshes: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, declared: &std::sync::Mutex<crate::status_rpc::Declared>, executor: &str, key: &MeshDeleteKey, mesh: &str, exited: &[TerminalReceipt]) -> Result<(), String> {
    drop_mesh_left_contacts(rows, executor, key, mesh, exited).await?;
    ensure_mesh_deleted(meshes, caches, declared, executor, key, mesh).await
}

/// The Mesh record of `mesh_id` at `mesh` written deleted on mesh.storage (R-MN1 item 2: a mesh's name is
/// released by its removal, the record deleted; R-MD1: the mesh-primary writes it at mesh.deleted,
/// mesh-left's fabric-primary as acting primary when no primary survived): the record its writer holds,
/// deleted, written whole through the claim of the name, which it
/// then releases. Returns what the record is now: `deleted-written`; `already-deleted` (this mesh's record was
/// deleted already: a repeat writes nothing); or `name-held` (another mesh's live claim holds the name:
/// nothing is written).
pub(crate) async fn mesh_row_deleted(meshes: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, mesh: &str, mesh_id: &MeshId) -> Result<&'static str, String> {
    let _one = caches.meshes.lock(mesh_id).await;
    let held = match caches.meshes.get(mesh_id) {
        Some(r) => Some(r),
        None => meshes.mesh(mesh_id).await.map_err(|e| format!("mesh.storage could not read the record of {mesh} ({mesh_id}): {e}"))?,
    };
    let mut row = held.clone().unwrap_or_else(|| crate::storage::MeshRecord::new(mesh_id.clone(), mesh));
    row.state = rdm_core::status::MeshState::Deleted;
    match crate::storage::write_mesh(meshes, held.as_ref(), &row).await {
        Ok(Ok(crate::storage::MeshWritten::Written { .. })) => {
            caches.mesh_moved(held.as_ref(), &row).await;
            caches.meshes.hold(mesh_id.clone(), row);
            Ok("deleted-written")
        }
        Ok(Ok(crate::storage::MeshWritten::Held)) => Ok("already-deleted"),
        Ok(Err(crate::storage::MeshRefused::NameHeld { .. })) => Ok("name-held"),
        Ok(Err(refused)) => Err(format!("mesh.storage refused the deleted record of {mesh} ({mesh_id}): {refused}")),
        Err(e) => Err(format!("mesh.storage could not write the deleted record of {mesh} ({mesh_id}): {e}")),
    }
}

/// mesh-left takes the primaries of every birth the complete proof set `proof` names off its node's
/// record (each member's record is deleted by its node.delete, mesh-delete.md "Rows") (R-MN1; a deleted
/// node's record is written by the admin acting for its mesh, Q356, here the fabric-primary as acting
/// primary of a mesh with none left): a deleted birth never answers, and a maker's stored map never
/// hands it to a mesh created again under the name. Each record is written whole from its latest line;
/// a record of another birth is left as it is. One `rdm.node_admin.node.remove.via-mesh-left-contact`
/// span names them, and a refused write ends mesh-left by name.
async fn drop_mesh_left_contacts(rows: Option<&crate::node_rows::NodeRows>, executor: &str, key: &MeshDeleteKey, mesh: &str, exited: &[TerminalReceipt]) -> Result<(), String> {
    let rows = rows.ok_or_else(|| format!("{}: {executor} holds no nodes store to take {mesh}'s births' primaries off", format!("{:?}", key.operation)))?;
    let span = tracing::info_span!("rdm.node_admin.node.remove.via-mesh-left-contact", node = %executor, mesh = %mesh, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, dropped = tracing::field::Empty, error = tracing::field::Empty);
    let mut dropped = Vec::new();
    for t in exited {
        let node_id = NodeId::parse(&t.node_id).map_err(|e| format!("{}: the Exited receipt of {} {} names no node id ({:?}): {e}", format!("{:?}", key.operation), t.node_id, t.incarnation_id, t.node_id))?;
        let storage = rows.storage();
        let written = async {
            let Some(mut r) = storage.node(&node_id).await.map_err(|e| e.to_string())?.map(|l| l.record) else { return Ok(()) };
            if r.incarnation.as_str() != t.incarnation_id || (r.is_primary.is_none() && r.is_fabric_primary.is_none()) {
                return Ok(());
            }
            crate::storage::clear_primaries(&mut r);
            storage.put_node(&r).await.map_err(|e| e.to_string())
        }
        .await;
        if let Err(e) = written {
            let why = format!("{}: the primaries of {} ({node_id}) could not be taken off its record: {e}", format!("{:?}", key.operation), t.node_id);
            span.record("error", why.as_str());
            span.in_scope(|| tracing::info!("mesh-left: a deleted birth's primaries are not taken off its record"));
            return Err(why);
        }
        dropped.push(format!("{} {node_id}", t.node_id));
    }
    span.record("dropped", dropped.join(", ").as_str());
    span.in_scope(|| tracing::info!("mesh-left: the removed mesh's deleted births' primaries are taken off their records"));
    Ok(())
}

/// mesh-left's ensure-deleted (R-MD1 = A): the deleted mesh's state deleted on the mesh (`mesh.storage`),
/// read by its key. The row held is the mesh-primary's commit at mesh.deleted
/// (or this fabric-primary's earlier write): AlreadyApplied, nothing written, no second transition.
/// No row held means no primary survived to write it: this fabric-primary puts it as acting primary
/// of a mesh with none left and records it as applied. One
/// `rdm.node_admin.mesh.update.via-deleted-row` span names the outcome and who wrote the row.
async fn ensure_mesh_deleted(meshes: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, declared: &std::sync::Mutex<crate::status_rpc::Declared>, executor: &str, key: &MeshDeleteKey, mesh: &str) -> Result<(), String> {
    let deleted = rdm_core::status::MeshState::Deleted;
    let span = tracing::info_span!("rdm.node_admin.mesh.update.via-deleted-row", node = %executor, mesh = %mesh, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, outcome = tracing::field::Empty, writer = tracing::field::Empty, mesh_row = tracing::field::Empty, error = tracing::field::Empty);
    let refused = |e: String| {
        span.record("outcome", "not-committed");
        span.record("error", e.as_str());
        span.in_scope(|| tracing::info!("mesh-left: the mesh row deleted is not committed"));
        format!("{}: the mesh row deleted of {mesh} ({}) is not committed: {e}", format!("{:?}", key.operation), key.mesh_id)
    };
    // This fabric-primary takes the writing of a mesh left with no primary: the mesh's record is read
    // now, never taken from a copy held from before (storage-whole-records.md, "The rule").
    let stored = meshes.mesh(&key.mesh_id).await.map_err(|e| refused(format!("{mesh}'s record could not be read: {e}")))?;
    if let Some(r) = &stored {
        let _one = caches.meshes.lock(&key.mesh_id).await;
        caches.meshes.hold(key.mesh_id.clone(), r.clone());
    }
    let held = stored.as_ref().is_some_and(|r| r.state == deleted);
    let applied_here = declared.lock().unwrap().meshes.get(&key.mesh_id) == Some(&deleted);
    let (outcome, writer) = if held { ("already-applied", if applied_here { "acting-primary" } else { "mesh-primary" }) } else { ("applied", "acting-primary") };
    let mesh_row = mesh_row_deleted(meshes, caches, mesh, &key.mesh_id).await.map_err(refused)?;
    if !held {
        declared.lock().unwrap().hold_mesh(key.mesh_id.clone(), deleted);
    }
    span.record("outcome", outcome);
    span.record("writer", writer);
    span.record("mesh_row", mesh_row);
    span.in_scope(|| tracing::info!("mesh-left: the mesh row deleted stands among the mesh's status rows and on mesh.storage before the MeshLeft receipt; the mesh's name is released"));
    Ok(())
}

/// What the fabric-primary decides of a `MeshDeleted` handoff from the Build facts it read from the
/// sender (node-rpc-envelope.md, "Mesh leave command pair").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandoffVerdict {
    /// The manifest covers the accepted roster exactly and names the admitted final runtime.
    Accept(ExitManifest),
    /// The manifest does not match what admission captured: `(field, expected, reported)`.
    Unmatched {
        /// The first field that does not match.
        field: String,
        /// What admission captured for it.
        expected: String,
        /// What the handoff reported.
        reported: String,
    },
    /// The leaving primary recorded no exit manifest for the leave: its member workflows are
    /// incomplete. `without_exit` names every roster birth (the final primary aside) that holds
    /// no exact Exited receipt.
    Incomplete {
        /// Why the handoff is refused, naming the births.
        reason: String,
        /// `<path and name> <incarnation>` of every birth without an Exited receipt.
        without_exit: Vec<String>,
    },
}

impl HandoffVerdict {
    /// The reply that refuses this handoff; `None` when it is accepted.
    pub fn refusal(&self) -> Option<StatusReply> {
        match self {
            HandoffVerdict::Accept(_) => None,
            HandoffVerdict::Unmatched { field, expected, reported } => Some(rdm_core::reply::status::rejected_unmatched_completion(field.clone(), expected.clone(), reported.clone())),
            HandoffVerdict::Incomplete { reason, without_exit } => Some(rdm_core::reply::status::bounded_incomplete_manifest(without_exit, reason)),
        }
    }
}

/// The verdict on the handoff of the leave `key`, admitted with `admitted`, over `steps` (the Build's
/// events, read now); `receipt_manifest` is the reference the call carried. The manifest is the
/// mesh's records' roster with every other birth's exit proven by its own node.delete's events (D67).
pub fn handoff_verdict(steps: &[BuildStep], key: &MeshDeleteKey, admitted: &Admitted, receipt_manifest: &rdm_core::status::ManifestReference) -> HandoffVerdict {
    let unmatched = |field: &str, expected: String, reported: String| HandoffVerdict::Unmatched { field: field.into(), expected, reported };
    let operation = &key.operation;
    if !names_workflow(receipt_manifest, key) {
        return unmatched("receipt_manifest", format!("{:?}", manifest_reference(key.build_id.as_str(), operation, false)), format!("{receipt_manifest:?}"));
    }
    if !completed(steps, &operation.to_string(), STEP_OTHER_MEMBERS_EXITED) {
        // No manifest is recorded: the member workflows are incomplete. The refusal names every
        // roster birth whose exit no event proves.
        let held = exit_receipts(steps, &admitted.roster);
        let without_exit: Vec<String> = admitted.roster.iter().filter(|b| **b != admitted.final_primary && !held.contains_key(&(b.node_id.clone(), b.incarnation_id.clone()))).map(|b| format!("{} {}", b.node_id, b.incarnation_id)).collect();
        let named = if without_exit.is_empty() { "none (every other birth exited, but no exit manifest is recorded)".to_string() } else { without_exit.join(", ") };
        let reason = format!("{operation} (build {}): the member workflows are incomplete: no {STEP_OTHER_MEMBERS_EXITED} step ended complete among the {} events read; roster births whose exit no event proves: {named}", key.build_id, steps.len());
        tracing::info_span!("rdm.node_admin.mesh.reject.via-incomplete-handoff", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %operation, roster = admitted.roster.len(), exited = held.len(), without_exit = %named)
            .in_scope(|| tracing::info!("the leaving primary's member workflows are incomplete: the handoff is refused by name as an incomplete manifest and the open leave ends"));
        return HandoffVerdict::Incomplete { reason, without_exit };
    }
    let manifest = manifest_of(key, &admitted.roster, &admitted.final_primary, Some(admitted.final_runtime.clone()), steps, false);
    if let Some((field, expected, reported)) = first_mismatch(&manifest, &admitted.roster, &admitted.final_primary, false) {
        return unmatched(&field, expected, reported);
    }
    HandoffVerdict::Accept(manifest)
}

/// Whether every reference field names this workflow's manifest.
fn names_workflow(receipt_manifest: &rdm_core::status::ManifestReference, key: &MeshDeleteKey) -> bool {
    receipt_manifest.build_id == key.build_id.as_str()
        && receipt_manifest.operation == key.operation
        && receipt_manifest.scope == rdm_core::status::ManifestScope::OtherMembersExited
}

/// The verdict on the handoff of the leave `key` from the Build's committed records as `reader` read
/// them from storage (`read`, Q292), the receipts the sender `sender` wrote among them. Storage that holds no
/// record of the Build (`UnknownBuild`) holds no exit manifest of the sender: its member
/// workflows are incomplete, every roster birth but the final primary without an Exited receipt,
/// and the open leave ends now (mesh-delete.md: "Missing receipts ... leave the workflow
/// incomplete"). `Err` is a read that decides nothing (the storage could not be read): the refusal is
/// `RejectedSourceFailed` naming the Build storage, the read and its own failure
/// ([`refuse_source_failed`]), and the open leave stays open.
pub fn handoff_verdict_from_read(read: Result<Vec<BuildStep>, BuildStateError>, key: &MeshDeleteKey, admitted: &Admitted, receipt_manifest: &rdm_core::status::ManifestReference, reader: &str, sender: &str) -> Result<HandoffVerdict, StatusReply> {
    match read {
        Ok(steps) => Ok(handoff_verdict(&steps, key, admitted, receipt_manifest)),
        Err(BuildStateError::UnknownBuild(build)) => Ok(match handoff_verdict(&[], key, admitted, receipt_manifest) {
            HandoffVerdict::Incomplete { reason, without_exit } => HandoffVerdict::Incomplete { reason: format!("{reader}: the Build storage holds no record of {build}, none of the final primary {sender}'s: {reason}"), without_exit },
            other => other,
        }),
        // A read that failed is not the storage holding nothing: the leave stays open and the call is
        // refused naming the read.
        Err(e) => Err(refuse_source_failed("mesh-deleted", key, &format!("the Build storage, read by {reader} for the final primary {sender}'s receipts"), &format!("read-committed {} for the manifest {receipt_manifest:?}", key.build_id), &e.to_string())),
    }
}

/// The refusal of `call` (`delete-mesh` at the leaving mesh-primary, `mesh-deleted` at the
/// fabric-primary) for the leave `key` because what it had to read failed: `source` failed `op` for
/// `cause`. `RejectedSourceFailed`, under its reject span naming the call, the workflow and the three
/// fields; it is never this admin's own `NotReady` (node-rpc-envelope.md, "Node RPC readiness gate").
/// Nothing is admitted or accepted.
pub fn refuse_source_failed(call: &'static str, key: &MeshDeleteKey, source: &str, op: &str, cause: &str) -> StatusReply {
    tracing::info_span!("rdm.node_admin.mesh.reject.via-source-failed", call, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation.action, operation_subject = ?key.operation.subject, source, op, cause)
        .in_scope(|| tracing::info!("what the call needed to read failed: it is refused by name and nothing is admitted"));
    rdm_core::reply::status::bounded_source_failed(source, op, cause)
}

/// The admin a leave hands back to, the owner of the workflow, and where a call to it goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshDeleteOwner {
    /// Where the handoff and the read of the owner's Build facts go: the endpoint and path of the
    /// `delete-mesh` call, or the fabric-primary's birth.
    pub target: rdm_transport::NodeTarget,
    /// What spans name the owner: its `(path, name)` where a record names it, else its endpoint.
    pub name: String,
}

impl std::fmt::Display for MeshDeleteOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

/// The admin a `delete-mesh`'s handoff goes back to, the owner of the workflow: the admin whose call
/// sent the command, reached back over that call's endpoint and path (`call`; no record of it is
/// read, as a stopped node joins back to the call that commanded its start, node-start.md step 2),
/// else the fabric-primary its view names (`fabric_primary`; DeleteMesh is the fabric-primary's
/// command, node-rpc-envelope.md "Mesh delete command pair"). Nothing checks who is calling
/// (node-rpc.md §14). With neither (a command that came on no Node RPC call, and a view naming no
/// fabric-primary), the leave has no owner to hand back to and is refused by name.
pub fn leave_owner(call: Option<MeshDeleteOwner>, fabric_primary: Option<MeshDeleteOwner>, me: &str, key: &MeshDeleteKey) -> Result<MeshDeleteOwner, StatusReply> {
    match call.or(fabric_primary) {
        Some(owner) => Ok(owner),
        None => Err(refuse_source_failed("delete-mesh", key, &format!("the view of {me}"), &format!("resolve the owner of delete-mesh for {}", key.mesh_id), "the command came on no call this admin can reach back over, and its view names no fabric-primary to hand the leave back to")),
    }
}

/// The final primary's runtime, as a `MeshDeleted` names it, adopted by this admin's provider: the
/// provider proof path the owner stops it through (mesh-delete.md step 5). A runtime the provider
/// refuses is refused by name.
pub fn adopt_final_runtime(provider: &dyn crate::deployment::provider::DeploymentProvider, final_runtime: &rdm_core::RuntimeFact, me: &str, key: &MeshDeleteKey) -> Result<(), StatusReply> {
    match crate::deployment::provider::adopt(provider, final_runtime) {
        Ok(_) => Ok(()),
        Err(refusal) => Err(refuse_source_failed("mesh-deleted", key, &format!("the {:?} provider of {me}", provider.kind()), &format!("adopt the final primary's runtime {}", final_runtime.deployment_id), &refusal.to_string())),
    }
}

/// The answer to a `MeshDeleted` for `key` that holds no open leave, from the fabric-primary's own
/// Build facts `steps`: once mesh.deleted's completion receipt is recorded, a repeated handoff of the
/// same final birth (`final_node_id`, `final_incarnation`) is `AlreadyApplied` and records nothing; a
/// handoff naming another birth is refused by name, both births named. `None` when no completion is
/// recorded: the handoff matches no open mesh.delete.
pub fn replayed_handoff(steps: &[BuildStep], key: &MeshDeleteKey, final_primary: Option<&RosterBirth>, final_node_id: &NodeId, final_incarnation: &str) -> Option<StatusReply> {
    if owner_resume(steps, &key.operation.to_string()) == OwnerResume::Lead {
        return None;
    }
    // The final primary is the mesh's one birth its records do not hold deleted (D67): the handoff that
    // names it repeats; one that names another is refused, both named.
    let reply = match final_primary {
        Some(last) if last.node_id != final_node_id.as_str() || last.incarnation_id != final_incarnation => rdm_core::reply::status::rejected_unmatched_completion("final_primary".into(), format!("{} {}", last.node_id, last.incarnation_id), format!("{final_node_id} {final_incarnation}")),
        _ => rdm_core::reply::already_applied(),
    };
    let last = final_primary.map(|b| format!("{} {}", b.node_id, b.incarnation_id)).unwrap_or_default();
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-handoff-replayed", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %key.operation, final_primary = %last, outcome = reply.name())
        .in_scope(|| tracing::info!("a handoff after mesh.deleted's completion receipt: answered from the receipt, nothing is recorded again"));
    Some(reply)
}

/// The key of one open mesh leave at the fabric-primary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MeshDeleteKey {
    /// The mesh that leaves.
    pub mesh_id: MeshId,
    /// The Build the operation belongs to.
    pub build_id: BuildId,
    /// `mesh.delete:<mesh_id>`.
    pub operation: rdm_core::OperationId,
}

impl MeshDeleteKey {
    /// The workflow this key names, as a workflow refusal carries it (its mesh id is the call's own).
    pub fn identity(&self) -> MeshDeleteIdentity {
        MeshDeleteIdentity { build_id: self.build_id.to_string(), operation: self.operation.clone() }
    }
}

/// How the workflow a `delete-mesh` or `mesh-deleted` offers for a mesh stands against the workflow held for
/// it (node-rpc-envelope.md, "Mesh delete command pair": the workflow identity is `(mesh_id,
/// build_id, operation)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowOrder {
    /// The workflow held: the same Build and operation.
    Same,
    /// Another Build or operation: conflicting.
    Other,
}

/// Where `offered` stands against `held`, both workflows of one mesh.
pub fn workflow_order(held: &MeshDeleteKey, offered: &MeshDeleteKey) -> WorkflowOrder {
    if held.build_id != offered.build_id || held.operation != offered.operation {
        return WorkflowOrder::Other;
    }
    WorkflowOrder::Same
}

/// The refusal of the workflow `offered` by `call` (`delete-mesh` at the leaving mesh-primary,
/// `mesh-deleted` at the fabric-primary) against the workflow `held` for its mesh:
/// `RejectedConflictingWorkflow`, another Build or operation. Its reject span names the call and both
/// workflows. Nothing is admitted.
pub fn refuse_workflow(call: &'static str, held: &MeshDeleteKey, offered: &MeshDeleteKey) -> StatusReply {
    let (h, o) = (held.identity(), offered.identity());
    tracing::info_span!("rdm.node_admin.mesh.reject.via-conflicting-workflow", call, mesh_id = %offered.mesh_id, held_build_id = %h.build_id, held_operation = %h.operation.action, held_operation_subject = ?h.operation.subject, held_operation_step = ?h.operation.step, held_operation_occurrence = ?h.operation.occurrence, build_id = %o.build_id, operation = %o.operation.action, operation_subject = ?o.operation.subject, operation_step = ?o.operation.step, operation_occurrence = ?o.operation.occurrence)
        .in_scope(|| tracing::info!("the call names a workflow of the mesh that is not the one held: it is refused by name and nothing is admitted"));
    rdm_core::reply::status::rejected_conflicting_workflow(h, o)
}

/// What the fabric-primary captured at admission.
#[derive(Debug, Clone)]
pub struct Admitted {
    /// Every accepted birth of the mesh.
    pub roster: Vec<RosterBirth>,
    /// The final mesh-primary.
    pub final_primary: RosterBirth,
    /// The final primary's exact runtime, by fingerprint, as its provider domain holds it.
    pub final_runtime: RuntimeIdentity,
}

/// How an open leave ends at the fabric-primary: the validated manifest, or the named reason the
/// leaving primary's member workflows are incomplete.
pub type MeshDeleteOutcome = Result<ExitManifest, String>;

struct OpenLeave {
    admitted: Admitted,
    handoff: watch::Sender<Option<MeshDeleteOutcome>>,
}

/// The mesh leaves this admin, as the fabric-primary, has admitted and awaits a `MeshDeleted` for.
#[derive(Default)]
pub struct MeshDeleteView {
    open: Mutex<HashMap<MeshDeleteKey, OpenLeave>>,
}

impl MeshDeleteView {
    /// Open `key` before `DeleteMesh` is sent, holding what admission captured; returns what
    /// resolves when a validated handoff arrives. Opening again keeps the first capture.
    pub fn open(&self, key: MeshDeleteKey, admitted: Admitted) -> watch::Receiver<Option<MeshDeleteOutcome>> {
        self.open.lock().unwrap().entry(key).or_insert_with(|| OpenLeave { admitted, handoff: watch::channel(None).0 }).handoff.subscribe()
    }

    /// What admission captured for `key`, when it is open.
    pub fn admitted(&self, key: &MeshDeleteKey) -> Option<Admitted> {
        self.open.lock().unwrap().get(key).map(|o| o.admitted.clone())
    }

    /// What admission captured for the workflow a `MeshDeleted` names, or the reply that refuses it. Only
    /// a workflow this owner holds open is admitted; any other workflow of a mesh it holds a leave open for
    /// is refused by name against that workflow ([`refuse_workflow`]: another Build or operation
    /// conflicts); a mesh it holds no leave open for names no open command.
    pub fn admitted_for(&self, key: &MeshDeleteKey) -> Result<Admitted, StatusReply> {
        let open = self.open.lock().unwrap();
        if let Some(o) = open.get(key) {
            return Ok(o.admitted.clone());
        }
        // The workflow held for the mesh: its open leave of the call's Build and operation, else any
        // open leave of the mesh.
        let held = open.keys().filter(|k| k.mesh_id == key.mesh_id).max_by_key(|k| (k.build_id == key.build_id && k.operation == key.operation)).cloned();
        drop(open);
        match held {
            Some(held) => Err(refuse_workflow("mesh-deleted", &held, key)),
            None => Err(rdm_core::reply::status::rejected_unmatched_completion("command".into(), format!("an open leave of mesh {}", key.mesh_id), key.identity().to_string())),
        }
    }

    /// A validated handoff for `key`: whether it was the first outcome of the leave.
    pub fn resolve(&self, key: &MeshDeleteKey, manifest: ExitManifest) -> bool {
        self.end(key, Ok(manifest))
    }

    /// The leaving primary reported its member workflows incomplete for `key`: the leave ends
    /// with `reason` at once (the owner's run ends inside the call, never on a wait); whether it
    /// was the first outcome of the leave.
    pub fn incomplete(&self, key: &MeshDeleteKey, reason: String) -> bool {
        self.end(key, Err(reason))
    }

    fn end(&self, key: &MeshDeleteKey, outcome: MeshDeleteOutcome) -> bool {
        match self.open.lock().unwrap().get(key) {
            Some(o) => o.handoff.send_if_modified(|held| {
                if held.is_some() {
                    return false;
                }
                *held = Some(outcome);
                true
            }),
            None => false,
        }
    }

    /// Forget the leave: its receipts are the record.
    pub fn close(&self, key: &MeshDeleteKey) {
        self.open.lock().unwrap().remove(key);
    }
}

/// The bound on the owner's wait for a `MeshDeleted` handoff that no event ends: the final primary's
/// `MeshDeleted` call lost while it lives (the call answered NotSent or Indeterminate;
/// `call_mesh_deleted` records the outcome and nothing calls again). Every other end is an event: the
/// handoff, the incomplete report inside the call, or the final primary unheard in the owner's view
/// ([`await_mesh_deleted`]). A run that ends incomplete ends its section
/// ([`crate::executor::RunEnd::Incomplete`]) and the Build `incomplete` (Q346); its resubmit resumes
/// from the step events.
pub const MESH_DELETED_WAIT: std::time::Duration = std::time::Duration::from_secs(120);

/// The owner's wait for the handoff of the open leave `key` (mesh-delete.md step 5), sent by the
/// final primary `primary` (its exact birth, named): the validated manifest, or the run's
/// incomplete end. A handoff held is read first. Each time the owner's view changes (`view`),
/// `final_primary_lost` answers whether the final primary is unheard there, naming its verdict; that
/// ends the run incomplete at once, so the resubmit resumes the leave in the order "Resume after the
/// primary's loss" gives (the lost primary's verdict and its node.delete first, then the leave over
/// the mesh's records).
pub async fn await_mesh_deleted<F, Fut>(key: &MeshDeleteKey, primary: &str, mut handoff: watch::Receiver<Option<MeshDeleteOutcome>>, mut view: watch::Receiver<u64>, mut final_primary_lost: F, within: std::time::Duration) -> Result<ExitManifest, crate::executor::RunEnd>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<String>>,
{
    let outer = &key.operation;
    let bound = tokio::time::sleep(within);
    tokio::pin!(bound);
    view.borrow_and_update();
    loop {
        let held = handoff.borrow_and_update().clone();
        if let Some(outcome) = held {
            return outcome.map_err(|reason| crate::executor::RunEnd::Incomplete(format!("{outer:?}: {primary} reported its member workflows incomplete: {reason}")));
        }
        if let Some(verdict) = final_primary_lost().await {
            tracing::info_span!("rdm.node_admin.mesh.update.via-final-primary-lost", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), operation = %outer.action, operation_subject = ?outer.subject, final_primary = %primary, verdict = %verdict)
                .in_scope(|| tracing::info!("the final primary is lost before it sent mesh-deleted: this run ends and the resubmit resumes the leave from its step events"));
            return Err(crate::executor::RunEnd::Incomplete(format!(
                "{outer:?}: the final primary {primary} is lost ({verdict}) and sent no mesh-deleted; the workflow is incomplete, its step events stand, and the resubmit resumes the leave after its loss"
            )));
        }
        tokio::select! {
            changed = handoff.changed() => {
                if changed.is_err() {
                    return Err(crate::executor::RunEnd::Incomplete(format!("{outer:?}: the open leave was closed while its handoff was awaited")));
                }
            }
            changed = view.changed() => {
                if changed.is_err() {
                    return Err(crate::executor::RunEnd::Incomplete(format!("{outer:?}: the owner's view ended while the handoff of {primary} was awaited")));
                }
            }
            _ = &mut bound => {
                return Err(crate::executor::RunEnd::Incomplete(format!("{outer:?}: {primary}, live, sent no valid mesh-deleted within {within:?}; the workflow is incomplete and its receipts stand")));
            }
        }
    }
}

/// One mesh leave at the leaving mesh's primary, on its natural key `(mesh_id, Leaving)`.
#[derive(Debug, Clone)]
enum MeshDeleteRun {
    /// The member workflows are running; `key` is the `delete-mesh` admitted last and `owner` its
    /// sender, whom the handoff goes to.
    Running { key: MeshDeleteKey, owner: MeshDeleteOwner },
    /// Every other member exited and the manifest is recorded under the workflow: the report to
    /// `owner`, under `key` (the `delete-mesh` admitted last), is what is left.
    Exited { key: MeshDeleteKey, owner: MeshDeleteOwner, manifest: ExitManifest },
}

/// What a `delete-mesh` admission does for the leave of its mesh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshDeleteAdmission {
    /// No run is held: the member workflows run for this sender under its key.
    Start,
    /// The member workflows are running: their handoff goes to this sender, under its key.
    Running,
    /// The manifest is recorded under this workflow: the recorded MeshDeleted report is what repeats,
    /// to this sender under its key, and no member workflow runs again (a rerun resumes from the
    /// Build's events).
    Repeat {
        /// The manifest recorded.
        manifest: ExitManifest,
    },
    /// The `delete-mesh` names another Build or operation (`RejectedConflictingWorkflow`): nothing is
    /// admitted and the leave held, its workflow and its sender, is unchanged.
    Refused(StatusReply),
}

/// What a run's manifest found the leave holding when it was recorded.
enum RunExit {
    /// The leave is still held under the key the run recorded under: the handoff goes to `owner`.
    Exited(MeshDeleteOwner),
    /// A later `delete-mesh` was admitted while the run ran: its manifest is recorded under its own key.
    Moved,
    /// The leave is no longer held.
    Forgotten,
}

/// How a run of the member workflows ended ([`run_admitted`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshDeleteRunEnd {
    /// Every other member exited and the manifest is recorded under `key`: the handoff goes to
    /// `owner` under it.
    Exited {
        /// The `delete-mesh` admitted last, whose key the manifest is recorded under.
        key: MeshDeleteKey,
        /// Its sender.
        owner: MeshDeleteOwner,
        /// The manifest.
        manifest: ExitManifest,
    },
    /// The member workflows are incomplete: `owner`, the sender of the `delete-mesh` admitted last
    /// (`key`), is told so.
    Incomplete {
        /// The `delete-mesh` admitted last.
        key: MeshDeleteKey,
        /// Its sender.
        owner: MeshDeleteOwner,
        /// Why.
        reason: String,
    },
    /// The leave is no longer held: nothing is handed off.
    Forgotten,
}

/// The mesh leaves this admin, as a leaving mesh's primary, is running or has finished, one per
/// mesh: the natural key `(mesh_id, Leaving)` (node-rpc-envelope.md, "Status").
/// The owner a handoff goes to is the admin that sent `delete-mesh` last for the mesh (a successor
/// fabric-primary sending the same workflow again takes the handoff over), under the key it sent.
#[derive(Default)]
pub struct MeshDeleteRuns(Mutex<HashMap<MeshId, MeshDeleteRun>>);

impl MeshDeleteRuns {
    /// `delete-mesh` for `key` from `sender`. The workflow held for the mesh decides it: the same workflow
    /// is the same leave; another Build or operation is refused by name against the workflow held
    /// ([`refuse_workflow`]) and changes nothing.
    pub fn admit(&self, key: &MeshDeleteKey, sender: &MeshDeleteOwner) -> MeshDeleteAdmission {
        let mut runs = self.0.lock().unwrap();
        let held = runs.get(&key.mesh_id).cloned();
        if let Some(MeshDeleteRun::Running { key: h, .. } | MeshDeleteRun::Exited { key: h, .. }) = &held {
            if workflow_order(h, key) == WorkflowOrder::Other {
                drop(runs);
                return MeshDeleteAdmission::Refused(refuse_workflow("delete-mesh", h, key));
            }
        }
        match held {
            Some(MeshDeleteRun::Running { .. }) => {
                runs.insert(key.mesh_id.clone(), MeshDeleteRun::Running { key: key.clone(), owner: sender.clone() });
                MeshDeleteAdmission::Running
            }
            // Exited under this workflow (another workflow was refused above): the recorded report is
            // replayed to this sender under its key.
            Some(MeshDeleteRun::Exited { manifest, .. }) => {
                runs.insert(key.mesh_id.clone(), MeshDeleteRun::Exited { key: key.clone(), owner: sender.clone(), manifest: manifest.clone() });
                MeshDeleteAdmission::Repeat { manifest }
            }
            _ => {
                runs.insert(key.mesh_id.clone(), MeshDeleteRun::Running { key: key.clone(), owner: sender.clone() });
                MeshDeleteAdmission::Start
            }
        }
    }

    /// Whether a leave of `mesh_id` is held, running or exited: only a workflow admitted after its
    /// mesh row was committed Leaving begins one ([`admit_delete_mesh`]).
    pub fn holds(&self, mesh_id: &MeshId) -> bool {
        self.0.lock().unwrap().contains_key(mesh_id)
    }

    /// The `delete-mesh` admitted last for `mesh_id` while its member workflows run, and its sender.
    fn running(&self, mesh_id: &MeshId) -> Option<(MeshDeleteKey, MeshDeleteOwner)> {
        match self.0.lock().unwrap().get(mesh_id) {
            Some(MeshDeleteRun::Running { key, owner }) => Some((key.clone(), owner.clone())),
            _ => None,
        }
    }

    /// A run recorded its manifest under `ran`.
    fn exited(&self, ran: &MeshDeleteKey, manifest: &ExitManifest) -> RunExit {
        let mut runs = self.0.lock().unwrap();
        match runs.get(&ran.mesh_id).cloned() {
            Some(MeshDeleteRun::Running { key, owner }) if &key == ran => {
                runs.insert(key.mesh_id.clone(), MeshDeleteRun::Exited { key, owner: owner.clone(), manifest: manifest.clone() });
                RunExit::Exited(owner)
            }
            Some(MeshDeleteRun::Running { .. }) => RunExit::Moved,
            _ => RunExit::Forgotten,
        }
    }

    /// The run of `mesh_id` ended incomplete: it is forgotten (its receipts stand) and the
    /// `delete-mesh` admitted last, with its sender, is returned.
    fn incomplete(&self, mesh_id: &MeshId) -> Option<(MeshDeleteKey, MeshDeleteOwner)> {
        match self.0.lock().unwrap().remove(mesh_id) {
            Some(MeshDeleteRun::Running { key, owner } | MeshDeleteRun::Exited { key, owner, .. }) => Some((key, owner)),
            None => None,
        }
    }
}

/// What a `delete-mesh` did at the leaving mesh-primary ([`admit_delete_mesh`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteMeshAdmission {
    /// The commit of the mesh row `Leaving` this workflow made, as the commit answered, or `None` when
    /// it made none.
    pub leaving_row: Option<StatusReply>,
    /// What the workflow does for the leave: `Refused` with the commit's refusal when the row's commit
    /// was refused, so nothing is admitted.
    pub admission: MeshDeleteAdmission,
}

/// `delete-mesh` for `key` from `sender` at the leaving mesh-primary (states.md: the mesh's
/// `leaving` is entered by this down op, and "one commit stores the row, applies it, gossips it and
/// answers the op"; node-identity-and-state.md 2.7: the mesh's primary writes every status of its own
/// mesh row). The first workflow of the mesh's leave puts and applies the row before it is admitted: a
/// refused put admits nothing and is the reply, so the owner's repeated `delete-mesh` is decided
/// afresh. A workflow that finds a leave of the mesh held commits nothing: the workflow that began the
/// leave committed the row, and the leave may since have written it Dead (mesh.deleted, before its
/// handoff); it is decided on the workflow held alone ([`MeshDeleteRuns::admit`]), so the workflow held resends
/// a lost handoff and a conflicting workflow is refused by name.
pub async fn admit_delete_mesh(runs: &MeshDeleteRuns, storage: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, declared: &Mutex<crate::status_rpc::Declared>, mesh: &str, key: &MeshDeleteKey, sender: &MeshDeleteOwner) -> DeleteMeshAdmission {
    if runs.holds(&key.mesh_id) {
        return DeleteMeshAdmission { leaving_row: None, admission: runs.admit(key, sender) };
    }
    let committed = crate::status_rpc::commit_mesh_state(storage, caches, declared, mesh, &key.mesh_id, rdm_core::status::MeshState::Deleting, 0).await;
    let admission = match &committed {
        StatusReply::Applied | StatusReply::AlreadyApplied => runs.admit(key, sender),
        refused => MeshDeleteAdmission::Refused(refused.clone()),
    };
    DeleteMeshAdmission { leaving_row: Some(committed), admission }
}

/// The one run of the member workflows of the leave `mesh_id` admitted ([`MeshDeleteAdmission::Start`]):
/// `run(key, owner)` runs them under the `delete-mesh` admitted last and records its manifest under
/// that key. A `delete-mesh` of the same workflow admitted while they run joins this run
/// ([`MeshDeleteAdmission::Running`]): the handoff goes to its sender, and no member is stopped twice
/// (node-rpc-envelope.md, "Mesh delete command pair": the manifest is scoped to `(mesh_id, build_id,
/// operation)`).
pub async fn run_admitted<F, Fut>(runs: &MeshDeleteRuns, mesh_id: &MeshId, run: F) -> MeshDeleteRunEnd
where
    F: Fn(MeshDeleteKey, MeshDeleteOwner) -> Fut,
    Fut: std::future::Future<Output = Result<ExitManifest, String>>,
{
    let mut ran: Option<MeshDeleteKey> = None;
    loop {
        let Some((key, owner)) = runs.running(mesh_id) else { return MeshDeleteRunEnd::Forgotten };
        if let Some(earlier) = &ran {
            tracing::info_span!("rdm.node_admin.mesh.resolve.via-later-delete-mesh", mesh_id = %mesh_id, build_id = %key.build_id.log(), ran_build_id = %earlier.build_id, operation = %key.operation.action, operation_subject = ?key.operation.subject, owner = %owner)
                .in_scope(|| tracing::info!("a later delete-mesh joined the run: its manifest is recorded under its own key from the receipts every member's exit already holds"));
        }
        match run(key.clone(), owner).await {
            Ok(manifest) => match runs.exited(&key, &manifest) {
                RunExit::Exited(owner) => return MeshDeleteRunEnd::Exited { key, owner, manifest },
                RunExit::Moved => ran = Some(key),
                RunExit::Forgotten => return MeshDeleteRunEnd::Forgotten,
            },
            Err(reason) => {
                return match runs.incomplete(mesh_id) {
                    Some((key, owner)) => MeshDeleteRunEnd::Incomplete { key, owner, reason },
                    None => MeshDeleteRunEnd::Forgotten,
                };
            }
        }
    }
}

/// What serves `DeleteMesh` (at the deleting mesh's primary) and `MeshDeleted` (at the
/// fabric-primary); the `StatusService` hands each to it. The executor's lifecycle hooks hand it every
/// birth they publish deleted, so the fabric-primary's mesh.delete observes the final birth's exit
/// proof (mesh-left), while node.delete stays the ordinary workflow it is.
#[async_trait::async_trait]
pub trait MeshDeleter: Send + Sync {
    /// `DeleteMesh` from `sender`, on a call from the endpoint `peer` (as the transport proved it):
    /// admit and run the member workflows; the handoff goes back over that call.
    async fn delete_mesh(&self, peer: Option<iroh::PublicKey>, sender: Option<&Node>, req: &StatusRequest) -> StatusReply;
    /// `MeshDeleted` from `sender`: validate the handoff.
    async fn mesh_deleted(&self, sender: Option<&Node>, req: &StatusRequest) -> StatusReply;
    /// This admin, as an executor, published `op`'s birth deleted (NodeDeleted, from the provider's
    /// exact exited verdict).
    async fn birth_deleted(&self, _op: &rdm_core::LifecycleOp) {}
}

/// The `MeshDeleteService` a running admin fills once its runner exists.
pub type MeshDeleterSlot = Arc<OnceLock<Arc<dyn MeshDeleter>>>;

/// Record the `fabric.delete:<fabric_id>` `Complete` receipt on `builds` (lifecycles/fabric-delete.md
/// step 3: the fabric-primary, still alive, reports `fabric.deleted`).
pub async fn complete_fabric_delete(
    builds: &dyn BuildStateAdapter,
    build_id: &BuildId,
    fabric_id: &rdm_core::FabricId,
    executor: &str,
) -> Result<(), String> {
    let operation = rdm_core::OperationId::new("fabric.delete", rdm_core::OperationSubject::Fabric(fabric_id.clone()));
    builds
        .write_step(&crate::build_state::StepWrite { build_id: build_id.clone(), mesh: None, operation: operation.clone(), step: "Complete".into(), outcome: StepOutcome::Complete })
        .await
        .map_err(|e| format!("{operation:?}: the Complete receipt could not be recorded: {e}"))?;
    tracing::info_span!("rdm.node_admin.fabric.delete.via-build", node = %executor, fabric_id = %fabric_id, build_id = %build_id.log(), operation = %operation.action, operation_subject = ?operation.subject, outcome = "fabric-deleted")
        .in_scope(|| tracing::info!("fabric.deleted: the fabric name is released and the fabric-primary awaits its separate node.delete"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: a manifest reference preserves every operation field of this workflow's other-member manifest.
    #[test]
    fn manifest_reference_keeps_structured_identity() {
        use rdm_core::status::{ManifestReference, ManifestScope};
        let mesh_id = MeshId::mint();
        let key = MeshDeleteKey { operation: operation(&mesh_id), mesh_id, build_id: BuildId::mint(), };
        let reference = manifest_reference(key.build_id.as_str(), &key.operation, false);
        assert_eq!((reference.build_id.as_str(), &reference.operation, reference.scope.clone()), (key.build_id.as_str(), &key.operation, ManifestScope::OtherMembersExited));
        assert_eq!(serde_json::from_value::<ManifestReference>(serde_json::to_value(&reference).unwrap()).unwrap(), reference);
        assert!(serde_json::from_value::<ManifestReference>(serde_json::json!("build/2/mesh.delete:mesh/other-members-exited")).is_err());
        assert_eq!(manifest_reference(key.build_id.as_str(), &key.operation, true).scope, ManifestScope::AllMembersExited);
        assert!(reference.encoded_len().unwrap() <= rdm_core::status::MAX_RECEIPT_MANIFEST_BYTES);
        let mut oversized = reference;
        oversized.operation.action = "x".repeat(rdm_core::status::MAX_RECEIPT_MANIFEST_BYTES);
        assert!(oversized.encoded_len().unwrap() > rdm_core::status::MAX_RECEIPT_MANIFEST_BYTES);
    }

    /// An owner of a leave, named by a fresh node id.
    fn owner() -> MeshDeleteOwner {
        let id = NodeId::mint();
        MeshDeleteOwner { name: id.to_string(), target: rdm_transport::NodeTarget::ExactNode(id) }
    }

    fn birth(node: crate::model::PathName, mesh: &str, r#type: &str, inc: &str) -> RosterBirth {
        RosterBirth { mesh: mesh.to_owned(), r#type: r#type.to_string(), node, node_id: NodeId::mint().to_string(), incarnation_id: rdm_core::fixture_crockford60(inc) }
    }
    fn receipt(b: &RosterBirth) -> TerminalReceipt {
        TerminalReceipt {
            path: b.node.path.clone(), name: b.node.name.clone(),
            node_id: b.node_id.clone(),
            incarnation_id: b.incarnation_id.clone(),
            runtime: Some(RuntimeIdentity { deployment_id: "d".into(), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: "l".into() }),
            exit_code: Some(0),
            verdict: None,
            data_dir: None,
        }
    }

    /// CONTRACT: the handoff of a running leave goes to the admin that sent `delete-mesh` last: a
    /// successor fabric-primary sending the same workflow again while the member workflows run
    /// takes the handoff over, and a repeated `delete-mesh` after the manifest is recorded resends
    /// it to that sender.
    #[test]
    fn a_delete_mesh_from_a_successor_owner_takes_over_the_running_leaves_handoff() {
        let runs = MeshDeleteRuns::default();
        let key = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), operation: rdm_core::OperationId::unscoped("mesh.delete:m") };
        let (first, successor) = (owner(), owner());
        assert_eq!(runs.admit(&key, &first), MeshDeleteAdmission::Start);
        assert_eq!(runs.admit(&key, &successor), MeshDeleteAdmission::Running);
        let last = birth(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", "i3");
        let manifest = ExitManifest { mesh_id: "m".into(), roster: vec![last.clone()], final_primary: last, final_runtime: None, receipts: vec![] };
        assert!(matches!(runs.exited(&key, &manifest), RunExit::Exited(o) if o == successor), "the handoff goes to the owner that sent delete-mesh last");
        let third = owner();
        assert_eq!(runs.admit(&key, &third), MeshDeleteAdmission::Repeat { manifest });
        assert_eq!(runs.incomplete(&key.mesh_id), Some((key.clone(), third)), "a resend is owned by its sender");
    }

    /// CONTRACT: an open leave the leaving primary reports incomplete ends at once with the named
    /// reason (the owner's wait resolves now, never on a deadline), and a later handoff for the same
    /// leave does not overwrite that outcome.
    #[test]
    fn an_incomplete_report_ends_the_open_leave_at_once_with_its_reason() {
        let mesh_delete_view = MeshDeleteView::default();
        let key = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), operation: rdm_core::OperationId::unscoped("mesh.delete:m") };
        let last = birth(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", "i3");
        let runtime = receipt(&last).runtime.unwrap();
        let rx = mesh_delete_view.open(key.clone(), Admitted { roster: vec![last.clone()], final_primary: last.clone(), final_runtime: runtime });
        assert!(rx.borrow().is_none());
        assert!(mesh_delete_view.incomplete(&key, "roster births without an exact Exited receipt: mesh2.node.1 i1".into()));
        assert_eq!(rx.borrow().clone(), Some(Err("roster births without an exact Exited receipt: mesh2.node.1 i1".into())), "the wait resolves now with the reason");
        let manifest = ExitManifest { mesh_id: "m".into(), roster: vec![last.clone()], final_primary: last, final_runtime: None, receipts: vec![] };
        assert!(!mesh_delete_view.resolve(&key, manifest), "the first outcome stands");
        assert!(matches!(rx.borrow().clone(), Some(Err(_))));
    }

    /// CONTRACT: a manifest covers the accepted roster exactly: a missing receipt and an extra or
    /// substituted birth are each named by the first field that is wrong. A receipt carries no runtime
    /// (D67: a step event has no output); the final primary's exact runtime is checked against the
    /// admitted one where the hand-off is validated.
    #[test]
    fn a_manifest_covers_the_roster_exactly_and_names_what_it_does_not() {
        let (a, b, last) = (birth(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", "i1"), birth(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", "i2"), birth(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", "i3"));
        let roster = vec![a.clone(), b.clone(), last.clone()];
        let ok = ExitManifest { mesh_id: "m".into(), roster: roster.clone(), final_primary: last.clone(), final_runtime: Some(receipt(&last).runtime.unwrap()), receipts: vec![receipt(&a), receipt(&b)] };
        assert_eq!(first_mismatch(&ok, &roster, &last, false), None);
        let mut missing = ok.clone();
        missing.receipts.pop();
        assert_eq!(first_mismatch(&missing, &roster, &last, false).map(|m| m.0), Some("receipt".into()));
        let mut substituted = ok.clone();
        substituted.receipts[0].incarnation_id = "other".into();
        assert_eq!(first_mismatch(&substituted, &roster, &last, false).map(|m| m.0), Some("receipt".into()));
        let mut extra_birth = ok.clone();
        extra_birth.roster.push(birth(rdm_core::PathName::new("mesh2.node", "9").unwrap(), "mesh2", "node", "i9"));
        assert_eq!(first_mismatch(&extra_birth, &roster, &last, false).map(|m| m.0), Some("roster".into()));
        // The complete departure includes the final primary's own exit.
        let mut complete = ok.clone();
        complete.final_runtime = None;
        assert_eq!(first_mismatch(&complete, &roster, &last, true).map(|m| m.0), Some("receipt".into()), "the final primary has no receipt yet");
        complete.receipts.push(receipt(&last));
        assert_eq!(first_mismatch(&complete, &roster, &last, true), None);
    }
    /// CONTRACT (mesh-delete.md "Failure"; node-rpc-envelope.md "Mesh delete command pair":
    /// "A lost handoff or missing exact receipt leaves the workflow incomplete, never successful"): the
    /// mesh-primary's MeshDeleted report is lost. The owner's bounded wait ends the section incomplete,
    /// its step events standing. The Build's rerun re-admits the same mesh.delete, and the mesh-primary
    /// replays the recorded report with no second run of the member workflows. The replay validates
    /// against the rerun's open workflow from the step events and resolves it exactly once.
    #[tokio::test]
    async fn a_lost_mesh_deleted_report_is_replayed_to_the_rerun_and_resolves_it_exactly_once() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let runs = MeshDeleteRuns::default();
        let first = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), operation: rdm_core::OperationId::unscoped("mesh.delete:m") };
        let next = MeshDeleteKey { ..first.clone() };
        let owner = owner();
        let last = birth(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", "i3");
        let runtime = RuntimeIdentity { deployment_id: "d".into(), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: "l".into() };
        let manifest = ExitManifest { mesh_id: first.mesh_id.to_string(), roster: vec![last.clone()], final_primary: last.clone(), final_runtime: Some(runtime.clone()), receipts: vec![] };
        let admitted = Admitted { roster: vec![last.clone()], final_primary: last.clone(), final_runtime: runtime };

        // The member workflows run once and record the manifest; the report the
        // mesh-primary then calls up is lost: the cell delivers nothing to the owner.
        assert_eq!(runs.admit(&first, &owner), MeshDeleteAdmission::Start);
        let ran = AtomicUsize::new(0);
        let end = run_admitted(&runs, &first.mesh_id, |_, _| {
            ran.fetch_add(1, SeqCst);
            let m = manifest.clone();
            async move { Ok(m) }
        })
        .await;
        assert!(matches!(end, MeshDeleteRunEnd::Exited { .. }), "the run records its manifest");

        // The owner's wait for run 1's report is bounded: it ends the run incomplete.
        let mesh_delete_view = MeshDeleteView::default();
        let (_view, view_rx) = watch::channel(0u64);
        let wait = mesh_delete_view.open(first.clone(), admitted.clone());
        let lost = await_mesh_deleted(&first, &last.node_id.clone(), wait, view_rx.clone(), || std::future::ready(None), std::time::Duration::from_millis(50)).await;
        assert!(matches!(&lost, Err(crate::executor::RunEnd::Incomplete(r)) if r.contains("sent no valid mesh-deleted")), "a lost report ends run 1 incomplete: {lost:?}");

        // The rerun re-admits the same mesh.delete: the recorded report is replayed, and the member
        // workflows do not run again.
        assert_eq!(runs.admit(&next, &owner), MeshDeleteAdmission::Repeat { manifest: manifest.clone() }, "the rerun's DeleteMesh replays the recorded report");
        assert_eq!(ran.load(SeqCst), 1, "the member workflows ran once");

        // The replay validates against the rerun's open workflow from the step event run 1 wrote ...
        let facts = vec![BuildStep {
            build_id: first.build_id.clone(),
            mesh: "mesh2".into(),
            operation: first.operation.to_string(),
            step: STEP_OTHER_MEMBERS_EXITED.into(),
            outcome: StepOutcome::Complete,
            subject: None,
            source: String::new(),
        }];
        let reference = manifest_reference(first.build_id.as_str(), &first.operation, false);
        assert!(matches!(handoff_verdict(&facts, &next, &admitted, &reference), HandoffVerdict::Accept(m) if m == manifest), "the rerun's report is accepted from the step event run 1 wrote");

        // ... and resolves it exactly once.
        let wait = mesh_delete_view.open(next.clone(), admitted.clone());
        assert!(mesh_delete_view.resolve(&next, manifest.clone()), "the first replay resolves the rerun");
        assert!(!mesh_delete_view.resolve(&next, manifest.clone()), "a second replay is AlreadyApplied");
        let got = await_mesh_deleted(&next, &last.node_id.clone(), wait, view_rx, || std::future::ready(None), std::time::Duration::from_millis(50)).await;
        assert_eq!(got.ok(), Some(manifest), "the rerun continues with the replayed manifest");
    }

    /// CONTRACT (storage-whole-records.md, "Who writes what changes"): a fabric-primary that becomes a
    /// mesh's writer after its last node-admin is gone writes `Dead` on the mesh's record as storage
    /// holds it then, never on the copy it held from before: the Ready the mesh-primary
    /// recorded after that copy was taken stands beside `Dead`. Must NOT happen: the stale copy written
    /// whole over the mesh-primary's write.
    #[tokio::test]
    async fn a_fabric_primary_taking_a_meshs_writing_writes_dead_on_the_record_loaded_then() {
        use crate::storage::{MeshRecord, MeshStorage, MemoryMeshStorage};
        use rdm_core::status::MeshState;
        let meshes = MemoryMeshStorage::default();
        let caches = crate::record_cache::Caches::default();
        let key = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), operation: rdm_core::OperationId::unscoped("mesh.delete:m") };
        let mut boot = MeshRecord::new(key.mesh_id.clone(), "mesh2");
        boot.state = MeshState::Pending;
        crate::storage::write_mesh(&meshes, None, &boot).await.unwrap().unwrap();
        // The copy this fabric-primary took before mesh2's primary wrote Ready.
        caches.meshes.hold(key.mesh_id.clone(), boot.clone());
        let mut ready = boot.clone();
        ready.state = MeshState::Ready;
        meshes.put_mesh(&ready).await.unwrap();

        let declared = std::sync::Mutex::new(crate::status_rpc::Declared::default());
        ensure_mesh_deleted(&meshes, &caches, &declared, "mesh1.node-admin.1", &key, "mesh2").await.unwrap();
        let stored = meshes.mesh(&key.mesh_id).await.unwrap().expect("mesh2's record stands");
        assert_eq!(stored.state, MeshState::Deleted, "deleted is written over the mesh-primary's ready: {stored:?}");
        assert_eq!(caches.meshes.get(&key.mesh_id), Some(stored), "the copy held is the record written");
    }
}
