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
//! the `receipt_manifest` string of the messages is its reference. The fabric-primary reads the
//! mesh-primary's from the Build's committed records in storage (Q292), never from a reference alone.

use crate::build::BuildId;
use crate::build_state::{BuildFact, BuildStateAdapter, BuildStateError, BuildStepReceipt, LocalBuildLog, StepOutcome};
use crate::deployment::pipeline::{RuntimeProof, TerminalReceipt};
use crate::model::{Node, NodeKind};
use futures_util::future::join_all;
use rdm_core::{LifecycleOp, MeshId, NodeId};
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
/// The step at the fabric-primary that admits a new final primary after the admitted one (or the
/// last amended one) was lost mid-leave (mesh-delete.md, "Resume after the primary's loss").
pub const STEP_ROSTER_AMENDED: &str = "RosterAmended";

/// The outer operation key of `mesh_id`'s shutdown.
pub fn operation(mesh_id: &MeshId) -> String {
    format!("mesh.delete:{mesh_id}")
}

/// The reference a message carries for a manifest receipt.
pub fn manifest_reference(build_id: &str, attempt: u32, operation: &str, all_members: bool) -> String {
    format!("{build_id}/{attempt}/{operation}/{}", if all_members { "all-members-exited" } else { "other-members-exited" })
}

/// One accepted birth of the leaving mesh.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RosterBirth {
    /// The node's `path.name`.
    pub node: String,
    /// The logical node.
    pub node_id: String,
    /// The exact birth.
    pub incarnation_id: String,
}

impl RosterBirth {
    /// The birth `n` holds.
    pub fn of(n: &Node) -> Result<Self, String> {
        let incarnation_id = n.incarnation_id.as_ref().map(|i| i.to_string()).ok_or_else(|| format!("{} has no known birth", n.name))?;
        Ok(Self { node: n.name.to_string(), node_id: n.node_id.to_string(), incarnation_id })
    }

    /// The exact birth the roster names, refused by name when its incarnation is not a Crockford id
    /// (a roster of an older format).
    pub fn birth(&self) -> Result<crate::model::IncarnationId, rdm_core::IdError> {
        crate::model::IncarnationId::parse(&self.incarnation_id)
    }

    /// The record of this exact birth the accepted roster alone gives (R11: the roster decides
    /// what runs): its `path.name`, node id and incarnation, `Leaving` as a birth of a leaving
    /// mesh. The member workflow reads the rest where it reads it for any birth (its runtime from
    /// the birth's durable runtime row). It carries no config: no row is written from it.
    pub fn record(&self) -> Result<Node, String> {
        let name: crate::model::PathName = self.node.parse().map_err(|e| format!("the roster birth's path.name {} does not parse: {e:?}", self.node))?;
        let mut n = Node::allocated(name);
        n.node_id = NodeId::parse(&self.node_id).map_err(|e| format!("the roster birth {}'s node id {} does not parse: {e}", self.node, self.node_id))?;
        n.incarnation_id = Some(self.birth().map_err(|e| format!("the roster birth {}: {e}", self.node))?);
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
    pub final_runtime: Option<RuntimeProof>,
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
        return Some(("roster".into(), join(want.iter().map(|b| format!("{} {}", b.node, b.incarnation_id)).collect()), join(have.iter().map(|b| format!("{} {}", b.node, b.incarnation_id)).collect())));
    }
    if &manifest.final_primary != final_primary {
        return Some(("final_primary".into(), format!("{} {}", final_primary.node, final_primary.incarnation_id), format!("{} {}", manifest.final_primary.node, manifest.final_primary.incarnation_id)));
    }
    let mut needed: BTreeMap<(&str, &str), ()> = roster.iter().filter(|b| with_final || *b != final_primary).map(|b| ((b.node_id.as_str(), b.incarnation_id.as_str()), ())).collect();
    for r in &manifest.receipts {
        if needed.remove(&(r.node_id.as_str(), r.incarnation_id.as_str())).is_none() {
            return Some(("receipt".into(), "one receipt per roster birth".into(), format!("an unexpected or repeated receipt for {} {}", r.node, r.incarnation_id)));
        }
        if r.runtime.is_none() {
            return Some(("receipt.runtime".into(), format!("the exact runtime of {} {}", r.node, r.incarnation_id), "no runtime fingerprint".into()));
        }
    }
    if let Some(((node_id, inc), _)) = needed.into_iter().next() {
        return Some(("receipt".into(), format!("a terminal receipt for {node_id} {inc}"), "none".into()));
    }
    if !with_final && manifest.final_runtime.is_none() {
        return Some(("final_runtime".into(), "the final primary's exact runtime".into(), "none".into()));
    }
    None
}

/// The accepted roster of the leave `key` (mesh-delete.md step 1): captured once, at the first
/// admission of the operation in its Build, and kept as its own step receipt under `step`
/// (`AcceptedRoster` at the fabric-primary, `LeavingRoster` at the leaving mesh-primary). The row
/// is an [`ExitManifest`] with no receipts yet: the roster, the final primary and its exact runtime
/// as this admin holds them at admission (the Build-fact codec carries it as the exit-manifest
/// output, `crate::wire`). A resumed attempt reads that receipt and never recaptures: a birth that
/// exited is no longer live, and the proof set must still cover it. With no receipt recorded,
/// `capture()` (the roster as this admin's view holds it now) is recorded with one blind append and
/// returned.
pub async fn accepted_roster(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, step: &str, executor: &str, final_primary: &RosterBirth, final_runtime: &RuntimeProof, capture: impl FnOnce() -> Result<Vec<RosterBirth>, String>) -> Result<Vec<RosterBirth>, String> {
    let build_id = key.build_id.clone();
    let view = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the {step} receipt of {}: {e}", key.operation))?;
    let recorded = view.steps.iter().filter(|r| r.operation == key.operation && r.step == step && r.outcome == StepOutcome::Complete).min_by_key(|r| r.attempt);
    let (roster, source, accepted_in) = match recorded {
        Some(r) => {
            let row: ExitManifest = r
                .output
                .clone()
                .and_then(|v| serde_json::from_value(v).ok())
                .ok_or_else(|| format!("{executor}: the {step} receipt of {} (Build {build_id}, attempt {}) holds no readable roster: the accepted roster cannot be named", key.operation, r.attempt))?;
            for b in row.roster.iter().chain(std::iter::once(&row.final_primary)) {
                b.birth().map_err(|e| format!("{executor}: the {step} receipt of {} (Build {build_id}, attempt {}) names the roster birth {} at incarnation {:?}, which is not a Crockford id: {e}", key.operation, r.attempt, b.node, b.incarnation_id))?;
            }
            (row.roster, "receipt", r.attempt)
        }
        None => {
            let roster = capture()?;
            let row = ExitManifest { mesh_id: key.mesh_id.to_string(), roster: roster.clone(), final_primary: final_primary.clone(), final_runtime: Some(final_runtime.clone()), receipts: Vec::new() };
            builds
                .append_step_receipt(&BuildStepReceipt {
                    mesh: None,
                    build_id: build_id.clone(),
                    attempt: key.attempt,
                    operation: key.operation.clone(),
                    step: step.to_string(),
                    outcome: StepOutcome::Complete,
                    output: serde_json::to_value(&row).ok(),
                    executor: Some(executor.to_string()),
                })
                .await
                .map_err(|e| format!("{executor}: the {step} receipt of {} could not be recorded: {e}", key.operation))?;
            (roster, "captured", key.attempt)
        }
    };
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-accepted-roster", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, step, source, accepted_in, roster = roster.len())
        .in_scope(|| tracing::info!("the leave runs over the roster accepted at its admission"));
    Ok(roster)
}

/// The exact Exited receipts the Build already holds for births of `roster`: each member's
/// node.delete `TerminateRuntime` step receipt under node.delete's own operation (`node.delete:<path>`,
/// mesh-delete.md step 3; a lost final primary's the same, its node.delete run by the fabric-primary
/// as a crashed target, "Resume after the primary's loss" step 1), from any attempt, whose terminal
/// receipt names that exact birth. The provider's proof that a runtime exited, not that the delete of
/// its birth is finished ([`members_exited`] runs a delete that holds only this proof on to its end).
pub fn exit_receipts(steps: &[BuildStepReceipt], _operation: &str, roster: &[RosterBirth]) -> BTreeMap<(String, String), TerminalReceipt> {
    let terminate = crate::deployment::pipeline::DeleteStep::TerminateRuntime.name();
    let mut held = BTreeMap::new();
    for b in roster {
        let Some(deleted) = b.node.parse::<crate::model::PathName>().ok().map(|node| crate::build::BuildOperation::DeleteLiveNode { node, exited: None, birth: None }.key()) else { continue };
        let proof = steps
            .iter()
            .filter(|r| r.operation == deleted && r.step == terminate && r.outcome == StepOutcome::Complete)
            .filter_map(|r| r.output.clone().and_then(|v| serde_json::from_value::<TerminalReceipt>(v).ok()))
            .find(|t| t.node_id == b.node_id && t.incarnation_id == b.incarnation_id);
        if let Some(t) = proof {
            held.insert((b.node_id.clone(), b.incarnation_id.clone()), t);
        }
    }
    held
}

/// The `NodeDeleted` op of `b`'s node.delete when that delete has every step's receipt among `steps`
/// (this Build's own facts, folded with the owner's read at admission): a `NodeDeleted` receipt under
/// `node.delete:<path>` naming the exact birth, and the `Complete` receipt of that operation in the
/// same Build and lane at that attempt or later (an attempt number names one attempt per lane). `None`
/// for a delete cut anywhere after its exited verdict (before `NodeDeleted`'s receipt, before its
/// publication, before `Complete`): the delete is owed its remaining steps.
pub(crate) fn finished_delete(steps: &[BuildStepReceipt], executor: &str, b: &RosterBirth) -> Result<Option<LifecycleOp>, String> {
    let named = format!("{} {}", b.node, b.incarnation_id);
    let path: crate::model::PathName = b.node.parse().map_err(|e| format!("{executor}: the roster birth {named} names no path.name: {e:?}"))?;
    let node_id = NodeId::parse(&b.node_id).map_err(|e| format!("{executor}: the roster birth {named}'s node id {} does not parse: {e}", b.node_id))?;
    let birth = b.birth().map_err(|e| format!("{executor}: the roster birth {named}: {e}"))?;
    let operation = crate::build::BuildOperation::DeleteLiveNode { node: path, exited: None, birth: None }.key();
    let deleted = crate::deployment::pipeline::DeleteStep::NodeDeleted.name();
    let complete = crate::deployment::pipeline::DeleteStep::Complete.name();
    Ok(steps
        .iter()
        .filter(|r| r.operation == operation && r.step == deleted && r.outcome == StepOutcome::Complete)
        .filter_map(|r| r.output.clone().and_then(|v| serde_json::from_value::<LifecycleOp>(v).ok()).map(|op| (r, op)))
        .filter(|(_, op)| op.node_id == node_id && op.incarnation == birth)
        .find(|(r, _)| steps.iter().any(|c| c.build_id == r.build_id && c.mesh == r.mesh && c.operation == operation && c.step == complete && c.outcome == StepOutcome::Complete && c.attempt >= r.attempt))
        .map(|(_, op)| op))
}

/// Where the fabric-primary's outer workflow `operation` resumes, read from its own Build facts
/// (`steps`) before it calls anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerResume {
    /// The `MeshLeft` receipt is recorded: the departure is the record.
    Left,
    /// mesh.deleted's completion receipt is recorded (`MeshDeleted`, its validated manifest): the
    /// mesh.delete ended, its final primary alive. Nothing is called: its end is a separate
    /// node.delete, and mesh-left follows that node.delete's exited verdict.
    Deleted(ExitManifest),
    /// Nothing recorded settles the mesh.delete: the owner leads it (admission, `DeleteMesh`, the
    /// handoff, the completion).
    Lead,
}

/// [`OwnerResume`] for `operation` over `steps`.
pub fn owner_resume(steps: &[BuildStepReceipt], operation: &str) -> Result<OwnerResume, String> {
    if steps.iter().any(|r| r.operation == operation && r.step == STEP_MESH_LEFT && r.outcome == StepOutcome::Complete) {
        return Ok(OwnerResume::Left);
    }
    match completion_of(steps, operation) {
        Some(manifest) => Ok(OwnerResume::Deleted(manifest?)),
        None => Ok(OwnerResume::Lead),
    }
}

/// mesh.deleted's completion receipt (`MeshDeleted`) of `operation` in `steps`, decoded: `None` when
/// none is recorded, `Err` naming the receipt when its manifest does not decode.
pub fn completion_of(steps: &[BuildStepReceipt], operation: &str) -> Option<Result<ExitManifest, String>> {
    let r = steps.iter().filter(|r| r.operation == operation && r.step == STEP_MESH_DELETED && r.outcome == StepOutcome::Complete).min_by_key(|r| r.attempt)?;
    Some(r.output.clone().and_then(|v| serde_json::from_value(v).ok()).ok_or_else(|| format!("{operation}: the {STEP_MESH_DELETED} receipt (attempt {}) holds no readable manifest", r.attempt)))
}

/// Record mesh.deleted's completion (mesh-delete.md step 5) for the mesh.delete `key`: the validated
/// manifest `manifest` (every birth of the mesh but its final primary) as the `MeshDeleted` step
/// receipt, by `executor`, the fabric-primary that owns the mesh.delete. One
/// `rdm.node_admin.mesh.update.via-mesh-deleted-receipt` span.
pub async fn record_completion(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, executor: &str, manifest: &ExitManifest) -> Result<(), String> {
    builds
        .append_step_receipt(&BuildStepReceipt {
            mesh: None,
            build_id: key.build_id.clone(),
            attempt: key.attempt,
            operation: key.operation.clone(),
            step: STEP_MESH_DELETED.into(),
            outcome: StepOutcome::Complete,
            output: serde_json::to_value(manifest).ok(),
            executor: Some(executor.to_string()),
        })
        .await
        .map_err(|e| format!("{}: the {STEP_MESH_DELETED} receipt could not be recorded: {e}", key.operation))?;
    tracing::info_span!("rdm.node_admin.mesh.update.via-mesh-deleted-receipt", node = %executor, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, receipts = manifest.receipts.len(), roster = manifest.roster.len(), final_primary = %manifest.final_primary.node)
        .in_scope(|| tracing::info!("mesh.deleted: every birth but the final primary is proven exited; the mesh.delete ends with its primary alive, whose end is its own node.delete"));
    Ok(())
}

/// mesh-left's complete proof set (R-MD1 item 2: "MeshLeft still requires complete exact-birth
/// terminal coverage"): mesh.deleted's validated manifest `completion` (every birth but the final
/// primary) and `terminal`, the provider's exact Exited of the final primary that its node.delete
/// proved. `terminal` must name the completion's final primary and the exact runtime it named still
/// running; the set must then cover the roster exactly, the final primary included. Anything else
/// refuses by name, the field and both values.
pub fn mesh_left_proof(completion: &ExitManifest, terminal: &TerminalReceipt) -> Result<ExitManifest, String> {
    let last = &completion.final_primary;
    let named = format!("{} {}", last.node, last.incarnation_id);
    if terminal.node_id != last.node_id || terminal.incarnation_id != last.incarnation_id {
        return Err(format!("final_primary: expected {named}, the Exited receipt names {} {}", terminal.node, terminal.incarnation_id));
    }
    if completion.final_runtime.is_none() || terminal.runtime != completion.final_runtime {
        return Err(format!("final_runtime: expected {:?}, the Exited receipt of {named} names {:?}", completion.final_runtime, terminal.runtime));
    }
    let mut all = completion.clone();
    all.final_runtime = None;
    all.receipts.push(terminal.clone());
    if let Some((field, expected, reported)) = first_mismatch(&all, &completion.roster, last, true) {
        return Err(format!("{field}: expected {expected}, reported {reported}"));
    }
    Ok(all)
}

/// The roster row recorded under `step` (`AcceptedRoster`, `LeavingRoster`) for `operation`, from
/// the first attempt that recorded it: `None` when none is recorded, `Err` when it does not decode.
pub fn accepted_row(steps: &[BuildStepReceipt], operation: &str, step: &str) -> Option<Result<ExitManifest, String>> {
    let r = steps.iter().filter(|r| r.operation == operation && r.step == step && r.outcome == StepOutcome::Complete).min_by_key(|r| r.attempt)?;
    Some(r.output.clone().and_then(|v| serde_json::from_value(v).ok()).ok_or_else(|| format!("the {step} receipt of {operation} (attempt {}) holds no readable roster: the accepted roster cannot be named", r.attempt)))
}

/// A roster amendment (mesh-delete.md, "Resume after the primary's loss", step 2): `final_primary`
/// replaces `predecessor`, whose exact Exited receipt the Build already holds. `final_runtime` is the
/// new final primary's exact runtime as the fabric-primary's provider domain holds it at the
/// amendment, what a later resume checks the final runtime against, as `AcceptedRoster` carries the
/// admitted one's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RosterAmended {
    /// The final primary that was lost.
    pub predecessor: RosterBirth,
    /// The new final primary.
    pub final_primary: RosterBirth,
    /// The new final primary's exact runtime, by fingerprint.
    pub final_runtime: RuntimeProof,
}

/// What recording an amendment did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Amended {
    /// The amendment is recorded now.
    Applied,
    /// The identical amendment is already recorded.
    AlreadyApplied,
}

/// Every `RosterAmended` receipt recorded for `operation` in `steps`, decoded, each once.
fn amendment_rows(steps: &[BuildStepReceipt], operation: &str) -> Result<Vec<RosterAmended>, String> {
    let mut rows: Vec<RosterAmended> = Vec::new();
    for r in steps.iter().filter(|r| r.operation == operation && r.step == STEP_ROSTER_AMENDED && r.outcome == StepOutcome::Complete) {
        let a: RosterAmended = r.output.clone().and_then(|v| serde_json::from_value(v).ok()).ok_or_else(|| format!("the {STEP_ROSTER_AMENDED} receipt of {operation} (attempt {}) does not decode: the amended roster cannot be named", r.attempt))?;
        if !rows.contains(&a) {
            rows.push(a);
        }
    }
    Ok(rows)
}

/// The amendment chain of `operation` from the admitted final primary `first`: each amendment
/// replaces the final primary the one before it named. Two amendments for one predecessor are
/// refused by name; they are never recorded (`amend_roster`).
fn amendment_chain(first: &RosterBirth, steps: &[BuildStepReceipt], operation: &str) -> Result<Vec<RosterAmended>, String> {
    let rows = amendment_rows(steps, operation)?;
    let mut chain: Vec<RosterAmended> = Vec::new();
    let mut current = first.clone();
    while let Some(next) = rows.iter().find(|a| a.predecessor == current) {
        if let Some(other) = rows.iter().find(|a| a.predecessor == current && *a != next) {
            return Err(format!("{operation}: two {STEP_ROSTER_AMENDED} receipts replace {} {}: {} {} and {} {}", current.node, current.incarnation_id, next.final_primary.node, next.final_primary.incarnation_id, other.final_primary.node, other.final_primary.incarnation_id));
        }
        if chain.len() >= rows.len() {
            return Err(format!("{operation}: the {STEP_ROSTER_AMENDED} receipts form a loop at {} {}", current.node, current.incarnation_id));
        }
        chain.push(next.clone());
        current = next.final_primary.clone();
    }
    Ok(chain)
}

/// What the fabric-primary admitted for `operation`, read from `steps`: the `AcceptedRoster` row
/// extended by every `RosterAmended` receipt (mesh-delete.md, "Resume after the primary's loss";
/// node-rpc-envelope.md, mesh-deleted manifest): the roster is the accepted one plus every amended
/// birth, the final primary and its runtime are the last amendment's, else the admitted ones.
/// `None` when no roster is recorded.
pub fn admitted_of(steps: &[BuildStepReceipt], operation: &str) -> Option<Result<Admitted, String>> {
    let row = accepted_row(steps, operation, STEP_ACCEPTED_ROSTER)?;
    Some(row.and_then(|row| {
        let final_runtime = row.final_runtime.clone().ok_or_else(|| format!("the {STEP_ACCEPTED_ROSTER} receipt of {operation} names no final runtime"))?;
        let chain = amendment_chain(&row.final_primary, steps, operation)?;
        let mut roster = row.roster;
        for a in &chain {
            if !roster.contains(&a.final_primary) {
                roster.push(a.final_primary.clone());
            }
        }
        let (final_primary, final_runtime) = chain.last().map(|a| (a.final_primary.clone(), a.final_runtime.clone())).unwrap_or((row.final_primary, final_runtime));
        Ok(Admitted { roster, final_primary, final_runtime, amendments: chain })
    }))
}

/// The span of a resume refused because the runtime of the lost final primary `predecessor` is not
/// known here.
pub fn span_unproven(key: &MeshDeleteKey, predecessor: &RosterBirth, reason: &str) {
    tracing::info_span!("rdm.node_admin.mesh.reject.via-unproven-primary", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, predecessor = %format!("{} {}", predecessor.node, predecessor.incarnation_id), reason = %reason)
        .in_scope(|| tracing::info!("the lost final primary's runtime is not known here: the resume is refused and nothing is amended"));
}

/// Record `amendment` under `key`'s operation (step 2 of the resume), only after the predecessor's
/// exact Exited receipt is recorded for that birth and runtime. The identical amendment replays
/// `AlreadyApplied`; a second amendment for the same predecessor is refused by name; an amendment
/// of a birth that is not the current final primary, or that names an exited birth, is refused.
pub async fn amend_roster(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, executor: &str, amendment: &RosterAmended) -> Result<Amended, String> {
    let build_id = key.build_id.clone();
    let operation = key.operation.as_str();
    let named = |b: &RosterBirth| format!("{} {}", b.node, b.incarnation_id);
    let (pred, next) = (named(&amendment.predecessor), named(&amendment.final_primary));
    let span = |outcome: &str| {
        tracing::info_span!("rdm.node_admin.mesh.update.via-roster-amended", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %operation, predecessor = %pred, final_primary = %next, outcome)
            .in_scope(|| tracing::info!("the roster amendment admits the new final primary"));
    };
    let steps = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the receipts of {operation}: {e}"))?.steps;
    if amendment_rows(&steps, operation)?.contains(amendment) {
        span("already-applied");
        return Ok(Amended::AlreadyApplied);
    }
    if let Some(held) = amendment_rows(&steps, operation)?.into_iter().find(|a| a.predecessor == amendment.predecessor) {
        tracing::info_span!("rdm.node_admin.mesh.reject.via-second-amendment", mesh_id = %key.mesh_id, operation = %operation, predecessor = %pred, held = %named(&held.final_primary), asked = %next)
            .in_scope(|| tracing::info!("a second amendment for the same predecessor is refused"));
        return Err(format!("{operation}: a second amendment for the same predecessor {pred} is refused: {STEP_ROSTER_AMENDED} already names {} as its replacement, not {next}", named(&held.final_primary)));
    }
    let admitted = admitted_of(&steps, operation).ok_or_else(|| format!("{operation}: no {STEP_ACCEPTED_ROSTER} receipt is recorded: there is no final primary to replace"))??;
    if amendment.predecessor != admitted.final_primary {
        return Err(format!("{operation}: the amendment replaces {pred}, but the final primary is {}", named(&admitted.final_primary)));
    }
    let exits = exit_receipts(&steps, operation, &[amendment.predecessor.clone(), amendment.final_primary.clone()]);
    match exits.get(&(amendment.predecessor.node_id.clone(), amendment.predecessor.incarnation_id.clone())) {
        Some(t) if t.runtime.as_ref() == Some(&admitted.final_runtime) => {}
        held => {
            let reason = format!("{operation}: no exact Exited receipt of the final primary {pred} is recorded ({}): {STEP_ROSTER_AMENDED} is written only after it, and a silent primary is not proven exited", if held.is_some() { "the one held names another runtime" } else { "none held" });
            span_unproven(key, &amendment.predecessor, &reason);
            return Err(reason);
        }
    }
    if amendment.final_primary == amendment.predecessor || exits.contains_key(&(amendment.final_primary.node_id.clone(), amendment.final_primary.incarnation_id.clone())) || admitted.amendments.iter().any(|a| a.predecessor == amendment.final_primary) {
        return Err(format!("{operation}: the amendment names {next} as final primary, a birth that has exited"));
    }
    builds
        .append_step_receipt(&BuildStepReceipt {
            mesh: None,
            build_id,
            attempt: key.attempt,
            operation: operation.to_string(),
            step: STEP_ROSTER_AMENDED.into(),
            outcome: StepOutcome::Complete,
            output: serde_json::to_value(amendment).ok(),
            executor: Some(executor.to_string()),
        })
        .await
        .map_err(|e| format!("{operation}: the {STEP_ROSTER_AMENDED} receipt could not be recorded: {e}"))?;
    span("applied");
    Ok(Amended::Applied)
}

/// The roster a leaving primary runs over: `roster` (its `LeavingRoster`, captured once) extended by
/// the new final primary of every amendment recorded in `steps`.
pub fn amended_roster(roster: &[RosterBirth], steps: &[BuildStepReceipt], operation: &str) -> Result<Vec<RosterBirth>, String> {
    let mut out = roster.to_vec();
    for a in amendment_rows(steps, operation)? {
        if !out.contains(&a.final_primary) {
            out.push(a.final_primary);
        }
    }
    Ok(out)
}

/// node.runtime.inspect of the lost birth `lost` (mesh-delete.md, "Resume after the primary's loss",
/// step 1: the lost final primary; "when none survives": another node-admin of the leaving mesh; a
/// member of an unanswered mesh during fabric.delete), whose runtime was admitted as `runtime`, read
/// from the durable runtime row this admin stored for that birth (`row`, the point read of the blind
/// put every admin makes the moment it first holds a birth): the same runtime by fingerprint, then the
/// one inspect ([`crate::deployment::provider::inspect_runtime`]). Its answer is the verdict the birth's
/// node.delete acts on (node-recovery.md step 3: `TerminateRuntime` first when it runs). A birth with
/// no row, or another runtime, names no runtime to act on.
pub async fn predecessor_verdict(provider: &dyn crate::deployment::provider::DeploymentProvider, row: Option<&crate::storage::RuntimeRow>, lost: &RosterBirth, runtime: &RuntimeProof) -> Result<crate::deployment::provider::Inspected, String> {
    let named = format!("{} {}", lost.node, lost.incarnation_id);
    let row = row.ok_or_else(|| format!("no durable runtime row of {named} was ever written here: its runtime is unknown"))?;
    if &RuntimeProof::of(&row.runtime) != runtime {
        return Err(format!("the durable runtime row of {named} names the runtime {:?}, not the one admitted for it {runtime:?}", RuntimeProof::of(&row.runtime)));
    }
    Ok(crate::deployment::provider::inspect_runtime(provider, &row.runtime, row.data_dir.as_deref(), &lost.incarnation_id).await)
}

/// The fabric-primary's resume after the leaving mesh's final primary `admitted.final_primary` was
/// lost and `held` (runtime `held_runtime`) is the mesh's node-admin primary now (mesh-delete.md,
/// "Resume after the primary's loss", steps 1-2): the lost birth's verdict, read from its durable
/// runtime row `row` through the inspect ([`predecessor_verdict`]) unless the Build already holds
/// its receipt, then its node.delete as an unreached birth (`delete`: the fabric-primary its
/// executor, `TerminateRuntime` first when it runs), whose `TerminateRuntime` receipt under
/// `node.delete:<path>` is its roster receipt; then `RosterAmended` admits `held`. A lost primary
/// whose runtime is unknown here refuses the resume by name and nothing is amended.
#[allow(clippy::too_many_arguments)]
pub async fn resume_after_loss<F, Fut>(builds: &dyn BuildStateAdapter, provider: &dyn crate::deployment::provider::DeploymentProvider, row: Option<&crate::storage::RuntimeRow>, key: &MeshDeleteKey, executor: &str, admitted: &Admitted, held: &RosterBirth, held_runtime: &RuntimeProof, delete: F) -> Result<Amended, crate::executor::RunEnd>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), crate::executor::RunEnd>>,
{
    let operation = key.operation.as_str();
    let lost = &admitted.final_primary;
    let steps = builds.read_build(&key.build_id.clone()).await.map_err(|e| format!("{executor}: the Build {} could not be read for the receipts of {operation}: {e}", key.build_id))?.steps;
    if !exit_receipts(&steps, operation, std::slice::from_ref(lost)).contains_key(&(lost.node_id.clone(), lost.incarnation_id.clone())) {
        // A lost primary whose runtime is unknown here refuses the resume by name; the leave is
        // incomplete, not failed.
        predecessor_verdict(provider, row, lost, &admitted.final_runtime).await.map_err(|reason| {
            span_unproven(key, lost, &reason);
            crate::executor::RunEnd::Incomplete(format!("{operation} is refused: the final primary {} {} is silent and its runtime is not known here, so {} {} is not admitted in its place: {reason}", lost.node, lost.incarnation_id, held.node, held.incarnation_id))
        })?;
        // node.delete of the exact birth records the receipt the amendment rests on.
        delete().await?;
    }
    Ok(amend_roster(builds, key, executor, &RosterAmended { predecessor: lost.clone(), final_primary: held.clone(), final_runtime: held_runtime.clone() }).await?)
}

/// The owner's step receipts a leave reads from its Build `facts` (mesh-delete.md "Build step key":
/// `mesh.delete:<mesh_id>`, and each member's step its own `node.delete:<path.name>`): the receipts
/// of the leave's own `operation`, and those of the node.delete of every birth of the leaving mesh,
/// the mesh of `me`, its mesh-primary.
fn leave_receipts(facts: Vec<BuildFact>, operation: &str, me: &RosterBirth) -> Result<Vec<BuildStepReceipt>, String> {
    let mesh = me.node.parse::<crate::model::PathName>().map_err(|e| format!("{operation}: the mesh-primary {} names no path.name: {e}", me.node))?.mesh;
    let of_member = |op: &str| op.strip_prefix("node.delete:").and_then(|p| p.parse::<crate::model::PathName>().ok()).is_some_and(|p| p.mesh == mesh);
    Ok(facts.into_iter().filter_map(|f| if let BuildFact::Step(r) = f { Some(r) } else { None }).filter(|r| r.operation == operation || of_member(&r.operation)).collect())
}

/// The roster a leaving primary runs `DeleteMesh` over, and the owner's step receipts it read for it
/// (mesh-delete.md, "Resume after the primary's loss", step 3): its `LeavingRoster` `roster` extended
/// by every roster amendment. This Build's own facts (`own`) are gossip-fed and may not yet hold the
/// amendment that names `me` final primary, nor the lost primary's exit receipt recorded before it;
/// gossip is never authority, so when the amendment chain `own` gives does not end at `me`, the
/// owner's Build facts are read (`fetch_owner`, the same read `validate_handoff` makes of the
/// sender's) and the amendments and exit receipts are taken from both. The owner is read too when
/// the chain `own` gives ends at `me` but `own` does not hold the finished node.delete (its exact
/// Exited receipt and every later step's) of every amendment's predecessor, which the fabric-primary
/// recorded before the amendment (mesh-delete.md,
/// "Resume after the primary's loss" step 3: "Completed member receipts stand"): gossip carries the
/// two operations' facts apart. A chain that still does not end at `me` refuses the run by name.
/// What is read from the owner is used for this run and written nowhere.
pub async fn admission_roster<F, Fut>(roster: &[RosterBirth], own: &[BuildStepReceipt], operation: &str, me: &RosterBirth, fetch_owner: F) -> Result<(Vec<RosterBirth>, Vec<BuildStepReceipt>), String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<BuildFact>, String>>,
{
    let named = |b: &RosterBirth| format!("{} {}", b.node, b.incarnation_id);
    let first = accepted_row(own, operation, STEP_LEAVING_ROSTER).ok_or_else(|| format!("{operation}: no {STEP_LEAVING_ROSTER} receipt is recorded: the leave has no roster to run over"))??.final_primary;
    let last = |steps: &[BuildStepReceipt]| amendment_chain(&first, steps, operation).map(|c| c.last().map(|a| a.final_primary.clone()).unwrap_or_else(|| first.clone()));
    let chain = amendment_chain(&first, own, operation)?;
    // Kept as [`members_exited`] keeps a member: its exact Exited receipt and every later step's.
    let proofs = exit_receipts(own, operation, &chain.iter().map(|a| a.predecessor.clone()).collect::<Vec<_>>());
    let predecessors_deleted = chain.iter().try_fold(true, |all, a| finished_delete(own, &named(me), &a.predecessor).map(|d| all && d.is_some() && proofs.contains_key(&(a.predecessor.node_id.clone(), a.predecessor.incarnation_id.clone()))))?;
    if &last(own)? == me && predecessors_deleted {
        let amendments = chain.len();
        if amendments > 0 {
            tracing::info_span!("rdm.node_admin.mesh.resolve.via-own-build-facts", operation = %operation, me = %named(me), first_primary = %named(&first), amendments)
                .in_scope(|| tracing::info!("this admin's own Build facts hold the amendment that names it final primary: it runs the roster captured once plus every amendment, reading the owner for nothing"));
        }
        return Ok((amended_roster(roster, own, operation)?, Vec::new()));
    }
    let known = leave_receipts(fetch_owner().await?, operation, me)?;
    let all: Vec<BuildStepReceipt> = own.iter().cloned().chain(known.iter().cloned()).collect();
    let final_primary = last(&all)?;
    let amendments = amendment_chain(&first, &all, operation)?.len();
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-owner-build-facts", operation = %operation, me = %named(me), final_primary = %named(&final_primary), amendments, owner_receipts = known.len())
        .in_scope(|| tracing::info!("this admin's own Build facts do not name it final primary, or do not hold every predecessor's finished delete: the owner's Build facts are read for the amendments and the receipts"));
    if &final_primary != me {
        return Err(format!("{operation}: {} is not the final primary: the owner's Build facts name {} after {amendments} amendment(s), so no member is stopped", named(me), named(&final_primary)));
    }
    Ok((amended_roster(roster, &all, operation)?, known))
}

/// The path of the one node-admin the fabric-primary creates for a leaving mesh no admin of which
/// survives (mesh-delete.md, "Resume after the primary's loss", step 2: "the one node-admin the
/// fabric-primary created for it, the only new birth a leaving mesh may gain"): a new birth, so a
/// path no birth ever held, the next admin ordinal of `mesh` above every one the accepted roster
/// (`roster`) and the view (`held`, every status, `.old` names included) hold. A path a lost birth
/// still holds is never reused: the create door would target against that birth and replace it.
pub fn leaving_mesh_admin_path(mesh: &str, roster: &[RosterBirth], held: &[crate::model::PathName]) -> Result<crate::model::PathName, String> {
    let named = roster.iter().filter_map(|b| b.node.parse::<crate::model::PathName>().ok());
    let top = named.chain(held.iter().cloned()).filter(|p| p.mesh == mesh && p.kind == NodeKind::NodeAdmin).map(|p| p.ordinal).max().unwrap_or(0);
    let next = top.checked_add(1).ok_or_else(|| format!("{mesh}: no admin ordinal is free above {top}"))?;
    Ok(crate::model::PathName::new(mesh, NodeKind::NodeAdmin, next))
}

/// The roster a leaving primary `me` runs `DeleteMesh` over, and the owner's step receipts it read
/// for it (mesh-delete.md step 1 and "Resume after the primary's loss"). A `LeavingRoster` this admin
/// holds is the roster captured once ([`admission_roster`] extends it by the amendments). With none
/// held, this admin does not know from its own facts whether it is admitted at all: the owner's
/// Build facts are read (`fetch_owner`, the sender of `DeleteMesh`; gossip is never authority) and
/// what the owner admitted decides. Admitted as the final primary at admission: the first run of the
/// leave, and the owner's accepted roster is recorded as the `LeavingRoster`: the roster is captured
/// once, by the owner (mesh-delete.md step 1), never again from this admin's view. Admitted by a `RosterAmended` (the admin
/// the election in its role, or the one node-admin the fabric-primary created when none survived): the
/// roster is the owner's accepted roster plus every amendment, used for this run and written
/// nowhere, so the accepted roster is never recaptured. Any other final primary refuses the run by
/// name, both births named.
///
/// When this admin's own log (`local`, the log `builds` reads) holds no fact of the Build at all
/// (`DeleteMesh` outran the gossip that carries the Build's facts), the owner's Build facts are read
/// once and absorbed into `local` before anything else is decided (node-build-hydration.md: "Facts
/// are absorbed into the existing Build log"), as an executor absorbs the intent its claim names;
/// no Build gossip is authored for them, and that one read is what the rest of the admission uses.
/// A Build log that could not be read refuses the run by name.
#[allow(clippy::too_many_arguments)]
pub async fn leaving_roster<F, Fut>(builds: &dyn BuildStateAdapter, local: &dyn LocalBuildLog, key: &MeshDeleteKey, executor: &str, me: &RosterBirth, my_runtime: &RuntimeProof, fetch_owner: F) -> Result<(Vec<RosterBirth>, Vec<BuildStepReceipt>), String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<BuildFact>, String>>,
{
    let operation = key.operation.as_str();
    let named = |b: &RosterBirth| format!("{} {}", b.node, b.incarnation_id);
    let build_id = key.build_id.clone();
    let unread = |e: BuildStateError| format!("{executor}: the Build {build_id} could not be read for the receipts of {operation}: {e}");
    // The owner's Build facts: read once when this admin's own log holds none, else not yet read.
    enum Owner<F> {
        Read(Vec<BuildFact>),
        Unread(F),
    }
    let (own, owner) = match builds.read_build(&build_id).await {
        Ok(p) => (p.steps, Owner::Unread(fetch_owner)),
        Err(BuildStateError::UnknownBuild(_)) => {
            let facts = fetch_owner().await?;
            local.absorb_facts(&facts).await;
            let own = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the owner's {} Build facts of {build_id} were absorbed for {operation}, but its own log still cannot read the Build: {e}", facts.len()))?.steps;
            tracing::info_span!("rdm.node_admin.build.update.via-owner-facts-absorbed", build_id = %build_id.log(), operation = %operation, executor = %executor, facts = facts.len(), steps = own.len())
                .in_scope(|| tracing::info!("this admin's own log held no fact of the Build: the owner's Build facts are absorbed into it before the leave is admitted"));
            (own, Owner::Read(facts))
        }
        Err(e) => return Err(unread(e)),
    };
    let fetch_owner = || async move {
        match owner {
            Owner::Read(facts) => Ok(facts),
            Owner::Unread(fetch) => fetch().await,
        }
    };
    if accepted_row(&own, operation, STEP_LEAVING_ROSTER).is_some() {
        let roster = accepted_roster(builds, key, STEP_LEAVING_ROSTER, executor, me, my_runtime, || Err(format!("{executor}: the {STEP_LEAVING_ROSTER} receipt of {operation} it holds is not read back"))).await?;
        return admission_roster(&roster, &own, operation, me, fetch_owner).await;
    }
    let known = leave_receipts(fetch_owner().await?, operation, me)?;
    let all: Vec<BuildStepReceipt> = own.iter().cloned().chain(known.iter().cloned()).collect();
    let admitted = admitted_of(&all, operation).ok_or_else(|| format!("{operation}: neither {executor}'s Build facts nor the owner's hold an {STEP_ACCEPTED_ROSTER} receipt: the leave was admitted for no birth, so no member is stopped"))??;
    if &admitted.final_primary != me {
        return Err(format!("{operation}: {executor} ({}) is not the final primary: the owner admitted {} after {} amendment(s), so no member is stopped", named(me), named(&admitted.final_primary), admitted.amendments.len()));
    }
    if admitted.amendments.is_empty() {
        let roster = accepted_roster(builds, key, STEP_LEAVING_ROSTER, executor, me, my_runtime, || Ok(admitted.roster.clone())).await?;
        return Ok((roster, known));
    }
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-owner-admitted-roster", operation = %operation, me = %named(me), amendments = admitted.amendments.len(), roster = admitted.roster.len(), owner_receipts = known.len())
        .in_scope(|| tracing::info!("this admin holds no roster and an amendment admits it: it runs the owner's accepted roster plus every amendment and captures none"));
    Ok((admitted.roster, known))
}

/// The roster of `mesh` (`mesh_id`) captured from its node rows on the store, every birth that was a
/// member when this Build's delete began: each row's newest birth, or, on the row a create's claim wrote
/// before any fact of its birth (lifecycles/node-create.md step 1: the name is claimed before the
/// deployment is recorded and launched), the birth that claim allocated, so a create admitted before
/// the delete and still in flight is a birth the delete deletes (build.md §1); a row of another mesh of
/// that name left out; a `Deleted` row only when this Build's own node.delete of that exact birth deleted it (its
/// `NodeDeleting` receipt names it), never a birth an earlier Build deleted. In path order.
pub fn stored_roster(rows: &[crate::storage::NodeRow], mesh: &str, mesh_id: &MeshId, steps: &[BuildStepReceipt]) -> Vec<RosterBirth> {
    let deleting = crate::deployment::pipeline::DeleteStep::NodeDeleting.name();
    let began = |r: &crate::storage::NodeRow, inc: &crate::model::IncarnationId| {
        let op = format!("node.delete:{}", r.name);
        steps.iter().filter(|s| s.operation == op && s.step == deleting && s.outcome == StepOutcome::Complete).filter_map(|s| s.output.clone().and_then(|v| serde_json::from_value::<LifecycleOp>(v).ok())).any(|o| o.node_id == r.node_id && o.incarnation == *inc)
    };
    let mut roster: Vec<RosterBirth> = rows
        .iter()
        .filter(|r| r.mesh_id.as_ref().is_none_or(|id| id == mesh_id) && r.name.mesh == mesh)
        .filter_map(|r| r.newest_birth().map(|b| b.incarnation_id.clone()).or_else(|| r.allocated.as_ref().map(|a| a.incarnation.clone())).map(|inc| (r, inc)))
        .filter(|(r, inc)| r.status != crate::model::NodeStatus::Deleted || began(r, inc))
        .map(|(r, inc)| RosterBirth { node: r.name.to_string(), node_id: r.node_id.to_string(), incarnation_id: inc.as_str().to_string() })
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
                tracing::info_span!("rdm.node_admin.mesh.resolve.via-held-record", member = %b.node, node_id = %b.node_id, incarnation_id = %b.incarnation_id, status = ?n.status)
                    .in_scope(|| tracing::info!("a roster birth the view no longer holds is worked from the record this admin's topology view holds of it"));
                Some(n)
            })
        })
        .collect()
}

/// The read the leave's member workflows take before they run (R13: "Rebuild after a role change
/// reads the mesh's own rows"): the row of every roster birth `records` hold no record of, held by
/// `rows`, the writer of the mesh's rows, so the birth's `Deleted` row, node.delete's `NodeDeleted`
/// step's write (node-delete.md step 3), is written from the row its node holds; the admin created
/// for a leaving mesh with no admin left joined after such a birth stopped, and its views never held it. One
/// `rdm.node_admin.mesh.resolve.via-roster-row` span per birth read names what the read found; a
/// row not read leaves the birth's `Deleted` write to refuse by name.
pub async fn roster_rows_read(rows: Option<&crate::node_rows::NodeRows>, executor: &str, operation: &str, roster: &[RosterBirth], records: &[Node]) {
    let Some(rows) = rows else { return };
    let exact = |b: &RosterBirth, n: &Node| n.node_id.as_str() == b.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.as_str() == b.incarnation_id);
    for b in roster.iter().filter(|b| !records.iter().any(|n| exact(b, n))) {
        let read = match (b.node.parse::<crate::model::PathName>(), NodeId::parse(&b.node_id)) {
            (Ok(name), Ok(node_id)) => rows.hold_row_of(&name, &node_id).await,
            (name, node_id) => Err(format!("{executor}: the roster birth {} ({}) names no path.name or node id: {:?} {:?}", b.node, b.node_id, name.err(), node_id.err())),
        };
        let (outcome, detail) = match &read {
            Ok(true) => ("held", String::new()),
            Ok(false) => ("no-row", format!("the store holds no row of {} at {}", b.node_id, b.node)),
            Err(e) => ("unreadable", e.clone()),
        };
        tracing::info_span!("rdm.node_admin.mesh.resolve.via-roster-row", node = %executor, member = %b.node, node_id = %b.node_id, incarnation_id = %b.incarnation_id, operation, outcome, detail = %detail)
            .in_scope(|| tracing::info!("the row of a roster birth this admin holds no record of is read from the mesh's own rows"));
    }
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
/// the primary's loss" step 3); `known` are step receipts read from the owner's Build facts at
/// admission ([`admission_roster`]), folded with this Build's own; `shutdown` is one member's whole
/// node.delete (mesh-delete.md step 3), answering the provider's terminal receipt of its exact
/// runtime, its row `Deleted` and its `NodeDeleted` the delete's own.
#[allow(clippy::too_many_arguments)]
pub async fn members_exited<F, Fut>(builds: &dyn BuildStateAdapter, key: &MeshDeleteKey, executor: &str, roster: &[RosterBirth], me: &RosterBirth, final_runtime: RuntimeProof, records: &[Node], known: &[BuildStepReceipt], shutdown: F) -> Result<ExitManifest, String>
where
    F: Fn(Node) -> Fut,
    Fut: std::future::Future<Output = Result<TerminalReceipt, String>>,
{
    use tracing::Instrument;
    if !roster.contains(me) {
        return Err(format!("{executor} ({} {}) is not a birth of the roster accepted for {}: no member is stopped", me.node, me.incarnation_id, key.operation));
    }
    let build_id = key.build_id.clone();
    let mut steps = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the receipts of {}: {e}", key.operation))?.steps;
    steps.extend(known.iter().cloned());
    let proofs = exit_receipts(&steps, &key.operation, roster);
    let mut receipts: Vec<TerminalReceipt> = Vec::new();
    let mut remaining: Vec<&RosterBirth> = Vec::new();
    // Births kept with their receipts: a member whose own delete finished.
    let mut retained: Vec<&RosterBirth> = Vec::new();
    // Births whose own node.delete holds the provider's exact Exited and not every later step.
    let mut owed: Vec<&RosterBirth> = Vec::new();
    for b in roster.iter().filter(|b| *b != me) {
        match proofs.get(&(b.node_id.clone(), b.incarnation_id.clone())) {
            // The exited verdict is a step of the delete, not the delete: a birth is done once its delete
            // has every step's receipt. One that holds the proof alone is run on (its receipted steps
            // are reused, the rest executed in order, NodeDeleted included).
            Some(t) => match finished_delete(&steps, executor, b)? {
                Some(_) => {
                    receipts.push(t.clone());
                    retained.push(b);
                }
                None => {
                    owed.push(b);
                    remaining.push(b);
                }
            },
            None => remaining.push(b),
        }
    }
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-retained-receipts", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, roster = roster.len(), retained = receipts.len(), remaining = remaining.len(), retained_births = %receipts.iter().map(|t| t.node.as_str()).collect::<Vec<_>>().join(","), delete_owed = %owed.iter().map(|b| b.node.as_str()).collect::<Vec<_>>().join(","))
        .in_scope(|| tracing::info!("members whose delete has every step's receipt keep it; the rest run, a delete that holds only its exited verdict from the step it has no receipt for"));
    let is_admin = |b: &RosterBirth| b.node.parse::<crate::model::PathName>().is_ok_and(|p| p.kind == NodeKind::NodeAdmin);
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
                        tracing::info_span!("rdm.node_admin.mesh.resolve.via-roster-birth", member = %b.node, node_id = %b.node_id, incarnation_id = %b.incarnation_id, tier, operation = %key.operation, attempt = key.attempt)
                            .in_scope(|| tracing::info!("a roster birth this admin holds no record of runs from the accepted roster's exact birth"));
                        members.push(n);
                    }
                    Err(e) => failed.push(format!("{} {}: {e}", b.node, b.incarnation_id)),
                },
            }
        }
        let kept = retained.iter().filter(|b| is_admin(b) == admins).count();
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-leave-tier", tier, members = members.len(), retained = kept, mesh_id = %key.mesh_id, operation = %key.operation, attempt = key.attempt, outcome = tracing::field::Empty);
        let outcomes = join_all(members.iter().cloned().map(&shutdown)).instrument(span.clone()).await;
        for (n, r) in members.iter().zip(outcomes) {
            match r {
                Ok(t) => receipts.push(t),
                Err(e) => failed.push(format!("{}: {e}", n.name)),
            }
        }
        span.record("outcome", if failed.is_empty() { "complete" } else { "incomplete" });
        if !failed.is_empty() {
            return Err(format!("tier {tier} is incomplete, the next tier does not start; members with an Exited receipt keep it for the next run: {}", failed.join("; ")));
        }
    }
    let manifest = ExitManifest { mesh_id: key.mesh_id.to_string(), roster: roster.to_vec(), final_primary: me.clone(), final_runtime: Some(final_runtime), receipts };
    builds
        .append_step_receipt(&BuildStepReceipt {
            mesh: None,
            build_id: key.build_id.clone(),
            attempt: key.attempt,
            operation: key.operation.clone(),
            step: STEP_OTHER_MEMBERS_EXITED.into(),
            outcome: StepOutcome::Complete,
            output: serde_json::to_value(&manifest).ok(),
            executor: Some(executor.to_string()),
        })
        .await
        .map_err(|e| format!("the exit manifest could not be recorded: {e}"))?;
    tracing::info_span!("rdm.node_admin.mesh.update.via-exit-manifest-recorded", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, receipts = manifest.receipts.len(), roster = manifest.roster.len())
        .in_scope(|| tracing::info!("every other member's exact exit is proven: the exit manifest is recorded"));
    Ok(manifest)
}

/// mesh-left at the fabric-primary, after exact coverage and before the `MeshLeft` receipt: the
/// final birth's node.delete wrote its row `Deleted` and proved its exit, so every birth of the mesh
/// is proven exited. `rows` is the fabric-primary's writer, writing as acting primary of a mesh that
/// has none left; `proof` the complete proof set; the mesh's status Dead is ensured on `meshes` and
/// held in `declared`, and the Mesh row Dead on `meshes`, which releases the mesh's name (R-MD1
/// item 2, R-MN1 item 2).
#[allow(clippy::too_many_arguments)]
pub async fn mesh_left_rows(rows: Option<&crate::node_rows::NodeRows>, meshes: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, declared: &std::sync::Mutex<crate::status_rpc::Declared>, executor: &str, key: &MeshDeleteKey, mesh: &str, proof: &[TerminalReceipt]) -> Result<(), String> {
    drop_mesh_left_contacts(rows, executor, key, mesh, proof).await?;
    ensure_mesh_dead(meshes, caches, declared, executor, key, mesh).await
}

/// The Mesh record of `mesh_id` at `mesh` written Dead on mesh.storage (R-MN1 item 2: a mesh's name is
/// released by its removal, the record Dead; R-MD1: the mesh-primary writes it at mesh.deleted,
/// mesh-left's fabric-primary as acting primary when no primary survived): the record its writer holds,
/// Dead with the state Dead among its states, written whole through the claim of the name, which it
/// then releases. Returns what the record is now: `dead-written`; `already-dead` (this mesh's record was
/// Dead already: a repeat writes nothing); or `name-held` (another mesh's live claim holds the name:
/// nothing is written).
pub(crate) async fn mesh_row_dead(meshes: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, mesh: &str, mesh_id: &MeshId) -> Result<&'static str, String> {
    let _one = caches.meshes.lock(mesh_id).await;
    let held = match caches.meshes.get(mesh_id) {
        Some(r) => Some(r),
        None => meshes.mesh(mesh_id).await.map_err(|e| format!("mesh.storage could not read the record of {mesh} ({mesh_id}): {e}"))?,
    };
    let mut row = held.clone().unwrap_or_else(|| crate::storage::MeshRecord::new(mesh_id.clone(), mesh));
    row.dead = true;
    row.states.insert(rdm_core::status::MeshState::Dead);
    match crate::storage::claim_mesh(meshes, held.as_ref(), &row).await {
        Ok(Ok(crate::storage::MeshClaim::Written { .. })) => {
            caches.meshes.hold(mesh_id.clone(), row);
            Ok("dead-written")
        }
        Ok(Ok(crate::storage::MeshClaim::Held)) => Ok("already-dead"),
        Ok(Err(crate::storage::MeshClaimRefused::NameHeld { .. })) => Ok("name-held"),
        Ok(Err(refused)) => Err(format!("mesh.storage refused the Dead record of {mesh} ({mesh_id}): {refused}")),
        Err(e) => Err(format!("mesh.storage could not write the Dead record of {mesh} ({mesh_id}): {e}")),
    }
}

/// mesh-left takes the address and the primaries of every birth the complete proof set `proof` names
/// off its node's record (R-MN1; R14 item 2: a mesh's records are its primary's, written here by the
/// fabric-primary as acting primary of a mesh with none left): a deleted birth never answers, and a
/// maker's stored map never hands it to a mesh created again under the name. Each record is written
/// whole from the copy the writer holds ([`crate::node_rows::WrittenRows::update`]); a birth whose record
/// holds no address for it is left as it is. One `rdm.node_admin.node.remove.via-mesh-left-contact` span
/// names them, and a refused write ends mesh-left by name.
async fn drop_mesh_left_contacts(rows: Option<&crate::node_rows::NodeRows>, executor: &str, key: &MeshDeleteKey, mesh: &str, proof: &[TerminalReceipt]) -> Result<(), String> {
    let rows = rows.ok_or_else(|| format!("{}: {executor} holds no nodes store to take {mesh}'s births' addresses off", key.operation))?;
    let span = tracing::info_span!("rdm.node_admin.node.remove.via-mesh-left-contact", node = %executor, mesh = %mesh, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, dropped = tracing::field::Empty, error = tracing::field::Empty);
    let mut dropped = Vec::new();
    for t in proof {
        let node_id = NodeId::parse(&t.node_id).map_err(|e| format!("{}: the Exited receipt of {} {} names no node id ({:?}): {e}", key.operation, t.node, t.incarnation_id, t.node_id))?;
        let written = rows
            .written()
            .update(&**rows.storage(), &node_id, None, |r| {
                if let Some(b) = r.births.iter_mut().find(|b| b.incarnation_id.as_str() == t.incarnation_id) {
                    crate::storage::clear_address_and_primaries(b);
                }
            })
            .await;
        if let Err(e) = written {
            let why = format!("{}: the address of {} ({node_id}) could not be taken off its record: {e}", key.operation, t.node);
            span.record("error", why.as_str());
            span.in_scope(|| tracing::info!("mesh-left: a deleted birth's address is not taken off its record"));
            return Err(why);
        }
        dropped.push(format!("{} {node_id}", t.node));
    }
    span.record("dropped", dropped.join(", ").as_str());
    span.in_scope(|| tracing::info!("mesh-left: the removed mesh's deleted births' addresses are taken off their records"));
    Ok(())
}

/// mesh-left's ensure-Dead (R-MD1 = A): the deleted mesh's status Dead on the mesh (`mesh.storage`),
/// read by its key. The row held is the mesh-primary's commit at mesh.deleted
/// (or this fabric-primary's earlier write): AlreadyApplied, nothing written, no second transition.
/// No row held means no primary survived to write it: this fabric-primary puts it as acting primary
/// of a mesh with none left and records it as applied. One
/// `rdm.node_admin.mesh.update.via-dead-row` span names the outcome and who wrote the row.
async fn ensure_mesh_dead(meshes: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, declared: &std::sync::Mutex<crate::status_rpc::Declared>, executor: &str, key: &MeshDeleteKey, mesh: &str) -> Result<(), String> {
    let dead = rdm_core::status::MeshState::Dead;
    let span = tracing::info_span!("rdm.node_admin.mesh.update.via-dead-row", node = %executor, mesh = %mesh, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, outcome = tracing::field::Empty, writer = tracing::field::Empty, mesh_row = tracing::field::Empty, error = tracing::field::Empty);
    let refused = |e: String| {
        span.record("outcome", "not-committed");
        span.record("error", e.as_str());
        span.in_scope(|| tracing::info!("mesh-left: the mesh row Dead is not committed"));
        format!("{}: the mesh row Dead of {mesh} ({}) is not committed: {e}", key.operation, key.mesh_id)
    };
    // This fabric-primary takes the writing of a mesh left with no primary: the mesh's record is read
    // now, never taken from a copy held from before (storage-whole-records.md, "The rule").
    let stored = meshes.mesh(&key.mesh_id).await.map_err(|e| refused(format!("{mesh}'s record could not be read: {e}")))?;
    if let Some(r) = &stored {
        let _one = caches.meshes.lock(&key.mesh_id).await;
        caches.meshes.hold(key.mesh_id.clone(), r.clone());
    }
    let held = stored.as_ref().is_some_and(|r| r.states.contains(&dead));
    let applied_here = declared.lock().unwrap().meshes.get(&key.mesh_id) == Some(&dead);
    let (outcome, writer) = if held { ("already-applied", if applied_here { "acting-primary" } else { "mesh-primary" }) } else { ("applied", "acting-primary") };
    let mesh_row = mesh_row_dead(meshes, caches, mesh, &key.mesh_id).await.map_err(refused)?;
    if !held {
        declared.lock().unwrap().hold_mesh(key.mesh_id.clone(), dead);
    }
    span.record("outcome", outcome);
    span.record("writer", writer);
    span.record("mesh_row", mesh_row);
    span.in_scope(|| tracing::info!("mesh-left: the mesh row Dead stands among the mesh's status rows and on mesh.storage before the MeshLeft receipt; the mesh's name is released"));
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
        /// `<path.name> <incarnation>` of every birth without an Exited receipt.
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

/// The verdict on the handoff of the leave `key`, admitted with `admitted`, over `facts` (the
/// sender's Build facts, read now); `receipt_manifest` is the reference the call carried.
pub fn handoff_verdict(facts: &[BuildFact], key: &MeshDeleteKey, admitted: &Admitted, receipt_manifest: &str) -> HandoffVerdict {
    let unmatched = |field: &str, expected: String, reported: String| HandoffVerdict::Unmatched { field: field.into(), expected, reported };
    let operation = key.operation.as_str();
    // The reference names where the manifest is recorded (node-rpc-envelope.md "Mesh delete command
    // pair": scoped to the Build, attempt and operation): this workflow's attempt, or an earlier attempt of
    // it when the report is a replay of what that attempt recorded.
    let attempt = match recorded_attempt(receipt_manifest, key) {
        Some(a) => a,
        None => return unmatched("receipt_manifest", format!("{} at attempt {} or earlier", manifest_reference(key.build_id.as_str(), key.attempt, operation, false), key.attempt), receipt_manifest.to_string()),
    };
    let receipt = facts.iter().find_map(|f| match f {
        BuildFact::Step(r) if r.operation == operation && r.attempt == attempt && r.step == STEP_OTHER_MEMBERS_EXITED && r.outcome == StepOutcome::Complete => Some(r.clone()),
        _ => None,
    });
    let Some(receipt) = receipt else {
        // No manifest is recorded: the member workflows are incomplete. The refusal names every
        // accepted birth the sender's facts hold no exact Exited receipt for.
        let steps: Vec<BuildStepReceipt> = facts.iter().filter_map(|f| if let BuildFact::Step(r) = f { Some(r.clone()) } else { None }).collect();
        let held = exit_receipts(&steps, operation, &admitted.roster);
        let without_exit: Vec<String> = admitted.roster.iter().filter(|b| **b != admitted.final_primary && !held.contains_key(&(b.node_id.clone(), b.incarnation_id.clone()))).map(|b| format!("{} {}", b.node, b.incarnation_id)).collect();
        let named = if without_exit.is_empty() { "none (every other birth exited, but no exit manifest is recorded)".to_string() } else { without_exit.join(", ") };
        let reason = format!("{operation} (build {}, attempt {attempt}): the member workflows are incomplete: {receipt_manifest} names no Complete {STEP_OTHER_MEMBERS_EXITED} receipt among the {} facts read; roster births without an exact Exited receipt: {named}", key.build_id, facts.len());
        tracing::info_span!("rdm.node_admin.mesh.reject.via-incomplete-handoff", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt, operation = %operation, roster = admitted.roster.len(), exited = held.len(), without_exit = %named)
            .in_scope(|| tracing::info!("the leaving primary's member workflows are incomplete: the handoff is refused by name as an incomplete manifest and the open leave ends"));
        return HandoffVerdict::Incomplete { reason, without_exit };
    };
    let manifest: ExitManifest = match receipt.output.clone().and_then(|v| serde_json::from_value(v).ok()) {
        Some(m) => m,
        None => return unmatched("receipt_manifest", "a readable exit manifest".into(), format!("{receipt_manifest}: its output does not decode")),
    };
    // A stale hand-off after an amendment: it names a replaced primary, or omits an amended birth.
    let named = |b: &RosterBirth| format!("{} {}", b.node, b.incarnation_id);
    if let Some(a) = admitted.amendments.iter().find(|a| a.predecessor == manifest.final_primary) {
        return unmatched("final_primary", format!("{} ({STEP_ROSTER_AMENDED} replaced {} with it)", named(&admitted.final_primary), named(&a.predecessor)), format!("{}, the predecessor {STEP_ROSTER_AMENDED} replaced", named(&manifest.final_primary)));
    }
    if let Some(a) = admitted.amendments.iter().find(|a| !manifest.roster.contains(&a.final_primary)) {
        return unmatched("roster_amendment", format!("{} in the roster ({STEP_ROSTER_AMENDED} for {})", named(&a.final_primary), named(&a.predecessor)), "a roster that omits it".into());
    }
    if let Some((field, expected, reported)) = first_mismatch(&manifest, &admitted.roster, &admitted.final_primary, false) {
        return unmatched(&field, expected, reported);
    }
    if manifest.final_runtime.as_ref() != Some(&admitted.final_runtime) {
        return unmatched("final_runtime", format!("{:?}", admitted.final_runtime), format!("{:?}", manifest.final_runtime));
    }
    HandoffVerdict::Accept(manifest)
}

/// The attempt `receipt_manifest` names (`<build_id>/<attempt>/<operation>/other-members-exited`,
/// [`manifest_reference`]) when it names `key`'s Build and operation at `key`'s attempt or an earlier one.
fn recorded_attempt(receipt_manifest: &str, key: &MeshDeleteKey) -> Option<u32> {
    let (head, scope) = receipt_manifest.rsplit_once('/')?;
    let (head, operation) = head.split_once('/').and_then(|(build, rest)| rest.split_once('/').map(|(attempt, op)| ((build, attempt), op)))?;
    let attempt: u32 = head.1.parse().ok()?;
    (head.0 == key.build_id.as_str() && operation == key.operation && scope == "other-members-exited" && attempt <= key.attempt).then_some(attempt)
}

/// The verdict on the handoff of the leave `key` from the Build's committed records as `reader` read
/// them from storage (`read`, Q292), the receipts the sender `sender` wrote among them. Storage that holds no
/// record of the Build (`UnknownBuild`) holds no exit manifest of the sender: its member
/// workflows are incomplete, every roster birth but the final primary without an Exited receipt,
/// and the open leave ends now (mesh-delete.md: "Missing receipts ... leave the workflow
/// incomplete"). `Err` is a read that decides nothing (the storage could not be read): the refusal is
/// `RejectedSourceFailed` naming the Build storage, the read and its own failure
/// ([`refuse_source_failed`]), and the open leave stays open.
pub fn handoff_verdict_from_read(read: Result<Vec<BuildFact>, BuildStateError>, key: &MeshDeleteKey, admitted: &Admitted, receipt_manifest: &str, reader: &str, sender: &str) -> Result<HandoffVerdict, StatusReply> {
    match read {
        Ok(facts) => Ok(handoff_verdict(&facts, key, admitted, receipt_manifest)),
        Err(BuildStateError::UnknownBuild(build)) => Ok(match handoff_verdict(&[], key, admitted, receipt_manifest) {
            HandoffVerdict::Incomplete { reason, without_exit } => HandoffVerdict::Incomplete { reason: format!("{reader}: the Build storage holds no record of {build}, none of the final primary {sender}'s: {reason}"), without_exit },
            other => other,
        }),
        // A read that failed is not the storage holding nothing: the leave stays open and the call is
        // refused naming the read.
        Err(e) => Err(refuse_source_failed("mesh-deleted", key, &format!("the Build storage, read by {reader} for the final primary {sender}'s receipts"), &format!("read-committed {} for the manifest {receipt_manifest}", key.build_id), &e.to_string())),
    }
}

/// The refusal of `call` (`delete-mesh` at the leaving mesh-primary, `mesh-deleted` at the
/// fabric-primary) for the leave `key` because what it had to read failed: `source` failed `op` for
/// `cause`. `RejectedSourceFailed`, under its reject span naming the call, the workflow and the three
/// fields; it is never this admin's own `NotReady` (node-rpc-envelope.md, "Node RPC readiness gate").
/// Nothing is admitted or accepted.
pub fn refuse_source_failed(call: &'static str, key: &MeshDeleteKey, source: &str, op: &str, cause: &str) -> StatusReply {
    tracing::info_span!("rdm.node_admin.mesh.reject.via-source-failed", call, mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, source, op, cause)
        .in_scope(|| tracing::info!("what the call needed to read failed: it is refused by name and nothing is admitted"));
    rdm_core::reply::status::bounded_source_failed(source, op, cause)
}

/// The admin a leave hands back to, the owner of the workflow, and where a call to it goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshDeleteOwner {
    /// Where the handoff and the read of the owner's Build facts go: the endpoint and path of the
    /// `delete-mesh` call, or the fabric-primary's birth.
    pub target: rdm_transport::NodeTarget,
    /// What spans name the owner: its `path.name` where a record names it, else its endpoint.
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
pub fn replayed_handoff(steps: &[BuildStepReceipt], key: &MeshDeleteKey, final_node_id: &NodeId, final_incarnation: &str) -> Option<StatusReply> {
    let completion = match completion_of(steps, &key.operation)? {
        Ok(m) => m,
        Err(reason) => return Some(rdm_core::reply::status::not_ready(reason)),
    };
    let last = &completion.final_primary;
    let reply = if last.node_id == final_node_id.as_str() && last.incarnation_id == final_incarnation {
        rdm_core::reply::already_applied()
    } else {
        rdm_core::reply::status::rejected_unmatched_completion("final_primary".into(), format!("{} {}", last.node_id, last.incarnation_id), format!("{final_node_id} {final_incarnation}"))
    };
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-handoff-replayed", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %key.operation, final_primary = %format!("{} {}", last.node, last.incarnation_id), outcome = reply.name())
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
    /// The Build attempt that holds it.
    pub attempt: u32,
    /// `mesh.delete:<mesh_id>`.
    pub operation: String,
}

impl MeshDeleteKey {
    /// The workflow this key names, as a workflow refusal carries it (its mesh id is the call's own).
    pub fn identity(&self) -> MeshDeleteIdentity {
        MeshDeleteIdentity { build_id: self.build_id.to_string(), attempt: self.attempt, operation: self.operation.clone() }
    }
}

/// How the workflow a `delete-mesh` or `mesh-deleted` offers for a mesh stands against the workflow held for
/// it (node-rpc-envelope.md, "Mesh delete command pair": the workflow identity is `(mesh_id,
/// build_id, attempt, operation)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowOrder {
    /// The workflow held.
    Same,
    /// A later attempt of the workflow held: the same Build and operation.
    Later,
    /// An earlier attempt of the workflow held: stale.
    Earlier,
    /// Another Build or operation: conflicting.
    Other,
}

/// Where `offered` stands against `held`, both workflows of one mesh.
pub fn workflow_order(held: &MeshDeleteKey, offered: &MeshDeleteKey) -> WorkflowOrder {
    if held.build_id != offered.build_id || held.operation != offered.operation {
        return WorkflowOrder::Other;
    }
    match offered.attempt.cmp(&held.attempt) {
        std::cmp::Ordering::Equal => WorkflowOrder::Same,
        std::cmp::Ordering::Greater => WorkflowOrder::Later,
        std::cmp::Ordering::Less => WorkflowOrder::Earlier,
    }
}

/// The refusal of the workflow `offered` by `call` (`delete-mesh` at the leaving mesh-primary,
/// `mesh-deleted` at the fabric-primary) against the workflow `held` for its mesh: `RejectedStaleWorkflow`
/// for an earlier attempt of the workflow held, `RejectedConflictingWorkflow` for any other workflow it does
/// not admit. Its reject span names the call and both workflows. Nothing is admitted.
pub fn refuse_workflow(call: &'static str, held: &MeshDeleteKey, offered: &MeshDeleteKey) -> StatusReply {
    let (h, o) = (held.identity(), offered.identity());
    if workflow_order(held, offered) == WorkflowOrder::Earlier {
        tracing::info_span!("rdm.node_admin.mesh.reject.via-stale-workflow", call, mesh_id = %offered.mesh_id, held_build_id = %h.build_id, held_attempt = h.attempt, held_operation = %h.operation, build_id = %o.build_id, attempt = o.attempt, operation = %o.operation)
            .in_scope(|| tracing::info!("the call names an earlier attempt of the workflow held for the mesh: it is refused by name and nothing is admitted"));
        rdm_core::reply::status::rejected_stale_workflow(h, o)
    } else {
        tracing::info_span!("rdm.node_admin.mesh.reject.via-conflicting-workflow", call, mesh_id = %offered.mesh_id, held_build_id = %h.build_id, held_attempt = h.attempt, held_operation = %h.operation, build_id = %o.build_id, attempt = o.attempt, operation = %o.operation)
            .in_scope(|| tracing::info!("the call names a workflow of the mesh that is not the one held nor an attempt it admits: it is refused by name and nothing is admitted"));
        rdm_core::reply::status::rejected_conflicting_workflow(h, o)
    }
}

/// What the fabric-primary captured at admission.
#[derive(Debug, Clone)]
pub struct Admitted {
    /// Every accepted birth of the mesh.
    pub roster: Vec<RosterBirth>,
    /// The final mesh-primary.
    pub final_primary: RosterBirth,
    /// The final primary's exact runtime, by fingerprint, as its provider domain holds it.
    pub final_runtime: RuntimeProof,
    /// Every roster amendment, in chain order from the admitted final primary.
    pub amendments: Vec<RosterAmended>,
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
    /// is refused by name against that workflow ([`refuse_workflow`]: an earlier attempt is stale, a later
    /// attempt it never opened or another Build conflicts); a mesh it holds no leave open for names
    /// no open command.
    pub fn admitted_for(&self, key: &MeshDeleteKey) -> Result<Admitted, StatusReply> {
        let open = self.open.lock().unwrap();
        if let Some(o) = open.get(key) {
            return Ok(o.admitted.clone());
        }
        // The workflow held for the mesh: its open leave of the call's Build and operation, else its
        // latest open attempt.
        let held = open.keys().filter(|k| k.mesh_id == key.mesh_id).max_by_key(|k| (k.build_id == key.build_id && k.operation == key.operation, k.attempt)).cloned();
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
    /// with `reason` at once (the owner's attempt ends inside the call, never on a wait); whether it
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
/// ([`await_mesh_deleted`]). An attempt that ends incomplete is handed back to the
/// fabric-primary ([`crate::executor::RunEnd::Incomplete`]): the Build stays in progress and the
/// fabric-primary's drive claims the next attempt once what it decides on moves, and that attempt
/// resumes from the receipts.
pub const MESH_DELETED_WAIT: std::time::Duration = std::time::Duration::from_secs(120);

/// The owner's wait for the handoff of the open leave `key` (mesh-delete.md step 5), sent by the
/// final primary `primary` (its exact birth, named): the validated manifest, or the attempt's
/// incomplete end. A handoff held is read first. Each time the owner's view changes (`view`),
/// `final_primary_lost` answers whether the final primary is unheard there, naming its verdict; that
/// ends the attempt incomplete at once, so the next attempt resumes the leave in the order "Resume
/// after the primary's loss" gives (the lost primary's verdict and its node.delete first, then
/// `RosterAmended`, then a fresh claim).
pub async fn await_mesh_deleted<F, Fut>(key: &MeshDeleteKey, primary: &str, mut handoff: watch::Receiver<Option<MeshDeleteOutcome>>, mut view: watch::Receiver<u64>, mut final_primary_lost: F, within: std::time::Duration) -> Result<ExitManifest, crate::executor::RunEnd>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<String>>,
{
    let outer = key.operation.as_str();
    let bound = tokio::time::sleep(within);
    tokio::pin!(bound);
    view.borrow_and_update();
    loop {
        let held = handoff.borrow_and_update().clone();
        if let Some(outcome) = held {
            return outcome.map_err(|reason| crate::executor::RunEnd::Incomplete(format!("{outer}: {primary} reported its member workflows incomplete: {reason}")));
        }
        if let Some(verdict) = final_primary_lost().await {
            tracing::info_span!("rdm.node_admin.mesh.update.via-final-primary-lost", mesh_id = %key.mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, operation = %outer, final_primary = %primary, verdict = %verdict)
                .in_scope(|| tracing::info!("the final primary is lost before it sent mesh-deleted: this attempt ends and the next resumes the leave from its receipts"));
            return Err(crate::executor::RunEnd::Incomplete(format!(
                "{outer}: the final primary {primary} is lost ({verdict}) and sent no mesh-deleted; the workflow is incomplete, its receipts stand, and the next attempt resumes the leave after its loss"
            )));
        }
        tokio::select! {
            changed = handoff.changed() => {
                if changed.is_err() {
                    return Err(crate::executor::RunEnd::Incomplete(format!("{outer}: the open leave was closed while its handoff was awaited")));
                }
            }
            changed = view.changed() => {
                if changed.is_err() {
                    return Err(crate::executor::RunEnd::Incomplete(format!("{outer}: the owner's view ended while the handoff of {primary} was awaited")));
                }
            }
            _ = &mut bound => {
                return Err(crate::executor::RunEnd::Incomplete(format!("{outer}: {primary}, live, sent no valid mesh-deleted within {within:?}; the workflow is incomplete and its receipts stand")));
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
    /// Every other member exited and the manifest is recorded under attempt `recorded` of the workflow:
    /// the report to `owner`, under `key` (the `delete-mesh` admitted last), is what is left.
    Exited { key: MeshDeleteKey, owner: MeshDeleteOwner, manifest: ExitManifest, recorded: u32 },
}

/// What a `delete-mesh` admission does for the leave of its mesh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshDeleteAdmission {
    /// No run is held: the member workflows run for this sender under its key.
    Start,
    /// The member workflows are running: their handoff goes to this sender, under its key.
    Running,
    /// The manifest is recorded under this workflow, at this attempt or an earlier one: the recorded
    /// MeshDeleted report is what repeats, to this sender under its key, and no member workflow runs
    /// again (mesh-delete.md "Failure and receipts": the next attempt resumes from the receipts).
    Resend {
        /// The manifest recorded.
        manifest: ExitManifest,
        /// The attempt it was recorded under: the replayed report's `receipt_manifest` names it.
        recorded: u32,
    },
    /// The `delete-mesh` names an earlier attempt of the workflow held (`RejectedStaleWorkflow`) or another
    /// Build or operation (`RejectedConflictingWorkflow`): nothing is admitted and the leave held, its
    /// workflow and its sender, is unchanged.
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
/// mesh: the natural key `(mesh_id, Leaving)` (node-rpc-envelope.md, "Status"), never the attempt.
/// The owner a handoff goes to is the admin that sent `delete-mesh` last for the mesh (a successor
/// fabric-primary re-dispatching the attempt, or the owner's next attempt, takes the handoff over),
/// under the key it sent.
#[derive(Default)]
pub struct MeshDeleteRuns(Mutex<HashMap<MeshId, MeshDeleteRun>>);

impl MeshDeleteRuns {
    /// `delete-mesh` for `key` from `sender`. The workflow held for the mesh decides it: the same workflow
    /// or a later attempt of it is the same leave; an earlier attempt or another Build or operation
    /// is refused by name against the workflow held ([`refuse_workflow`]) and changes nothing.
    pub fn admit(&self, key: &MeshDeleteKey, sender: &MeshDeleteOwner) -> MeshDeleteAdmission {
        let mut runs = self.0.lock().unwrap();
        let held = runs.get(&key.mesh_id).cloned();
        if let Some(MeshDeleteRun::Running { key: h, .. } | MeshDeleteRun::Exited { key: h, .. }) = &held {
            if matches!(workflow_order(h, key), WorkflowOrder::Earlier | WorkflowOrder::Other) {
                drop(runs);
                return MeshDeleteAdmission::Refused(refuse_workflow("delete-mesh", h, key));
            }
        }
        match held {
            Some(MeshDeleteRun::Running { .. }) => {
                runs.insert(key.mesh_id.clone(), MeshDeleteRun::Running { key: key.clone(), owner: sender.clone() });
                MeshDeleteAdmission::Running
            }
            // Exited under this workflow or an earlier attempt of it (an earlier or another workflow was
            // refused above): the recorded report is replayed to this sender under its key.
            Some(MeshDeleteRun::Exited { manifest, recorded, .. }) => {
                runs.insert(key.mesh_id.clone(), MeshDeleteRun::Exited { key: key.clone(), owner: sender.clone(), manifest: manifest.clone(), recorded });
                MeshDeleteAdmission::Resend { manifest, recorded }
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
                let recorded = key.attempt;
                runs.insert(key.mesh_id.clone(), MeshDeleteRun::Exited { key, owner: owner.clone(), manifest: manifest.clone(), recorded });
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
/// a lost handoff, a later attempt resumes from the receipts and a stale or conflicting workflow is
/// refused by name.
pub async fn admit_delete_mesh(runs: &MeshDeleteRuns, storage: &dyn crate::storage::MeshStorage, caches: &crate::record_cache::Caches, declared: &Mutex<crate::status_rpc::Declared>, mesh: &str, key: &MeshDeleteKey, sender: &MeshDeleteOwner) -> DeleteMeshAdmission {
    if runs.holds(&key.mesh_id) {
        return DeleteMeshAdmission { leaving_row: None, admission: runs.admit(key, sender) };
    }
    let committed = crate::status_rpc::commit_mesh_state(storage, caches, declared, mesh, &key.mesh_id, rdm_core::status::MeshState::Leaving, 0).await;
    let admission = match &committed {
        StatusReply::Applied | StatusReply::AlreadyApplied => runs.admit(key, sender),
        refused => MeshDeleteAdmission::Refused(refused.clone()),
    };
    DeleteMeshAdmission { leaving_row: Some(committed), admission }
}

/// The one run of the member workflows of the leave `mesh_id` admitted ([`MeshDeleteAdmission::Start`]):
/// `run(key, owner)` runs them under the `delete-mesh` admitted last and records its manifest under
/// that key. A `delete-mesh` of a later attempt admitted while they run joins this run
/// ([`MeshDeleteAdmission::Running`]): when the run ends, its manifest is recorded again under the later
/// key by `run` over the receipts every member's exit already holds, so no member is stopped twice
/// and the handoff names the attempt whose open leave the owner holds (node-rpc-envelope.md, "Mesh
/// delete command pair": the manifest is scoped to `(mesh_id, build_id, attempt, operation)`; a
/// later attempt resumes from the recorded receipts, "Caller execution and certainty").
pub async fn run_admitted<F, Fut>(runs: &MeshDeleteRuns, mesh_id: &MeshId, run: F) -> MeshDeleteRunEnd
where
    F: Fn(MeshDeleteKey, MeshDeleteOwner) -> Fut,
    Fut: std::future::Future<Output = Result<ExitManifest, String>>,
{
    let mut ran: Option<MeshDeleteKey> = None;
    loop {
        let Some((key, owner)) = runs.running(mesh_id) else { return MeshDeleteRunEnd::Forgotten };
        if let Some(earlier) = &ran {
            tracing::info_span!("rdm.node_admin.mesh.resolve.via-later-delete-mesh", mesh_id = %mesh_id, build_id = %key.build_id.log(), attempt = key.attempt, ran_build_id = %earlier.build_id, ran_attempt = earlier.attempt, operation = %key.operation, owner = %owner)
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
/// fabric-primary); the status door hands each to it. The executor's lifecycle hooks hand it every
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

/// The door a running admin fills once its runner exists.
pub type MeshDeleterSlot = Arc<OnceLock<Arc<dyn MeshDeleter>>>;

/// What an unanswered mesh (`mesh`, with no live `node-admin` primary in `view`) resolves to during
/// `fabric.delete` (lifecycles/fabric-delete.md, "An unanswered mesh during `fabric.delete`";
/// lifecycles/mesh-investigation.md, "During `fabric.delete`"; R-FS1 revised).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnansweredMeshOutcome {
    /// Every node of the mesh was proven exited by the provider: rows are `Deleted`, mesh row is
    /// `Dead`, and `MeshLeft` is recorded.
    FullyCrashed(ExitManifest),
    /// Ordinary members are live while all `node-admin`s are proven exited: create one `node-admin`
    /// at `path` to amend the roster from `predecessor` and run `mesh.delete`.
    CreateAdmin {
        /// The path for the one `node-admin` created into the leaving mesh.
        path: crate::model::PathName,
        /// The exited `node-admin` birth that `AcceptedRoster` names as predecessor for `RosterAmended`.
        predecessor: RosterBirth,
    },
}

/// Resolve an unanswered mesh `mesh` during `fabric.delete` (R-FS1 revised; mesh-investigation.md,
/// "During `fabric.delete`"): every node-admin of the mesh, and every ordinary member not live, is
/// inspected from its durable runtime row ([`predecessor_verdict`]) and deleted by node.delete as an
/// unreached birth (`delete`: this fabric-primary its executor, under the leave's Build and attempt,
/// `TerminateRuntime` first when it runs), whose `TerminateRuntime` receipt under `node.delete:<path>`
/// is the birth's roster receipt and whose `NodeDeleted` step writes its row `Deleted`; nothing of a
/// birth is written or receipted outside its node.delete. Every birth gone: the mesh row Dead and
/// `MeshDeleted` and `MeshLeft` recorded ([`UnansweredMeshOutcome::FullyCrashed`]). Ordinary members
/// live: the accepted roster is recorded and the one node-admin the leaving mesh may gain is named
/// ([`UnansweredMeshOutcome::CreateAdmin`]). A birth whose runtime is unknown here refuses the resolve
/// by name.
#[allow(clippy::too_many_arguments)]
pub async fn unanswered_mesh_delete<F, Fut>(
    builds: &dyn BuildStateAdapter,
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    records: &crate::admin::Records,
    rows: Option<&crate::node_rows::NodeRows>,
    meshes: &dyn crate::storage::MeshStorage,
    runtime_store: &dyn crate::storage::NodesStorage,
    key: &MeshDeleteKey,
    executor: &str,
    mesh: &str,
    mesh_nodes: &[Node],
    held_paths: &[crate::model::PathName],
    delete: F,
) -> Result<UnansweredMeshOutcome, String>
where
    F: Fn(RosterBirth) -> Fut,
    Fut: std::future::Future<Output = Result<TerminalReceipt, String>>,
{
    unanswered_mesh_delete_with(builds, provider, records, rows, meshes, runtime_store, key, executor, mesh, mesh_nodes, held_paths, delete, &[]).await
}

/// [`unanswered_mesh_delete`], where the live ordinary members `in_process` names were deleted already by
/// their ordinary node.delete, this fabric-primary their executor, each with its terminal receipt: under an
/// in-force fabric.delete no node-admin is created for the mesh, which is deleted in process at the
/// executor (mesh-delete.md "Resume after the primary's loss" step 2, Q160; mesh-investigation.md "During
/// fabric.delete").
#[allow(clippy::too_many_arguments)]
pub async fn unanswered_mesh_delete_with<F, Fut>(
    builds: &dyn BuildStateAdapter,
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    records: &crate::admin::Records,
    rows: Option<&crate::node_rows::NodeRows>,
    meshes: &dyn crate::storage::MeshStorage,
    runtime_store: &dyn crate::storage::NodesStorage,
    key: &MeshDeleteKey,
    executor: &str,
    mesh: &str,
    mesh_nodes: &[Node],
    held_paths: &[crate::model::PathName],
    delete: F,
    in_process: &[(RosterBirth, TerminalReceipt)],
) -> Result<UnansweredMeshOutcome, String>
where
    F: Fn(RosterBirth) -> Fut,
    Fut: std::future::Future<Output = Result<TerminalReceipt, String>>,
{
    let outer = key.operation.as_str();
    let admins: Vec<&Node> = mesh_nodes.iter().filter(|n| n.mesh == mesh && n.kind == NodeKind::NodeAdmin).collect();
    let ordinary: Vec<&Node> = mesh_nodes.iter().filter(|n| n.mesh == mesh && n.kind != NodeKind::NodeAdmin).collect();
    if admins.is_empty() {
        return Err(format!("{outer}: {mesh} holds no node-admin birth to delete"));
    }
    let mut admin_exits: Vec<(RosterBirth, RuntimeProof, TerminalReceipt)> = Vec::new();
    for admin in &admins {
        let birth = RosterBirth::of(admin)?;
        let inc = birth.birth().map_err(|e| format!("{outer}: {e}"))?;
        let row = runtime_store.runtime(&admin.node_id, &inc).await.map_err(|e| format!("{outer}: the runtime row of {} {} could not be read: {e}", birth.node, birth.incarnation_id))?;
        let runtime = row.as_ref().map(|r| RuntimeProof::of(&r.runtime)).ok_or_else(|| {
            let reason = format!("no durable runtime row of {} {} was ever written here", birth.node, birth.incarnation_id);
            span_unproven(key, &birth, &reason);
            format!("{outer}: {mesh} is unanswered and the runtime of its node-admin {} {} is not known here ({reason})", birth.node, birth.incarnation_id)
        })?;
        predecessor_verdict(provider, row.as_ref(), &birth, &runtime).await.map_err(|reason| {
            span_unproven(key, &birth, &reason);
            format!("{outer}: {mesh} is unanswered and the runtime of its node-admin {} {} is not known here ({reason})", birth.node, birth.incarnation_id)
        })?;
        let terminal = delete(birth.clone()).await.map_err(|e| format!("{outer}: the node.delete of the unreached node-admin {} {} is incomplete: {e}", birth.node, birth.incarnation_id))?;
        admin_exits.push((birth, runtime, terminal));
    }
    let mut ordinary_exits: Vec<TerminalReceipt> = Vec::new();
    let mut live_ordinary: Vec<RosterBirth> = Vec::new();
    for ord in &ordinary {
        let birth = RosterBirth::of(ord)?;
        if let Some((_, terminal)) = in_process.iter().find(|(b, _)| *b == birth) {
            ordinary_exits.push(terminal.clone());
            continue;
        }
        let inc = birth.birth().map_err(|e| format!("{outer}: {e}"))?;
        let row = runtime_store.runtime(&ord.node_id, &inc).await.map_err(|e| format!("{outer}: the runtime row of {} {} could not be read: {e}", birth.node, birth.incarnation_id))?;
        match row.as_ref() {
            Some(r) => {
                let runtime = RuntimeProof::of(&r.runtime);
                match predecessor_verdict(provider, row.as_ref(), &birth, &runtime).await {
                    // A member that runs and is live in this view answers for the mesh: kept.
                    Ok(crate::deployment::provider::Inspected::Running) if ord.status.is_live() => live_ordinary.push(birth),
                    Ok(_) => {
                        let terminal = delete(birth.clone()).await.map_err(|e| format!("{outer}: the node.delete of the unreached member {} {} is incomplete: {e}", birth.node, birth.incarnation_id))?;
                        ordinary_exits.push(terminal);
                    }
                    Err(reason) => {
                        span_unproven(key, &birth, &reason);
                        return Err(format!("{outer}: {mesh} is unanswered and the runtime of its member {} {} is not known here ({reason})", birth.node, birth.incarnation_id));
                    }
                }
            }
            None => {
                if ord.status.is_live() {
                    live_ordinary.push(birth);
                } else {
                    return Err(format!("{outer}: {mesh} is unanswered and its member {} {} has no durable runtime row to act on", birth.node, birth.incarnation_id));
                }
            }
        }
    }
    let roster: Vec<RosterBirth> = mesh_nodes.iter().filter(|n| n.mesh == mesh).map(RosterBirth::of).collect::<Result<Vec<_>, _>>()?;
    let (first_admin, first_runtime, _) = &admin_exits[0];
    if live_ordinary.is_empty() {
        let mut all_receipts: Vec<TerminalReceipt> = ordinary_exits;
        all_receipts.extend(admin_exits.iter().map(|(_, _, t)| t.clone()));
        let left_manifest = deleted_in_process(builds, records, rows, meshes, key, executor, mesh, roster, first_admin, Some(first_runtime.clone()), all_receipts).await?;
        return Ok(UnansweredMeshOutcome::FullyCrashed(left_manifest));
    }
    // The exited node-admins' roster receipts are their node.delete's; the accepted roster names the
    // first as the final primary the created admin is amended in for.
    accepted_roster(builds, key, STEP_ACCEPTED_ROSTER, executor, first_admin, first_runtime, || Ok(roster.clone())).await?;
    let path = leaving_mesh_admin_path(mesh, &roster, held_paths)?;
    Ok(UnansweredMeshOutcome::CreateAdmin { path, predecessor: first_admin.clone() })
}

/// The record of a mesh whose every birth was deleted in process by the fabric-primary's executor (an
/// unanswered mesh during fabric.delete, mesh-investigation.md "During fabric.delete"; a mesh left with no
/// node-admin under an in-force fabric.delete, mesh-delete.md "Resume after the primary's loss" step 2,
/// Q160): every birth's row went Deleted inside its own node.delete, whose receipts are `all_receipts`;
/// the mesh's rows left (mesh-left), mesh.deleted's completion receipt (`MeshDeleted`, its manifest every
/// birth but `final_primary`) and `MeshLeft` (its manifest every birth) recorded. Answers the `MeshLeft`
/// manifest.
#[allow(clippy::too_many_arguments)]
pub async fn deleted_in_process(
    builds: &dyn BuildStateAdapter,
    records: &crate::admin::Records,
    rows: Option<&crate::node_rows::NodeRows>,
    meshes: &dyn crate::storage::MeshStorage,
    key: &MeshDeleteKey,
    executor: &str,
    mesh: &str,
    roster: Vec<RosterBirth>,
    final_primary: &RosterBirth,
    final_runtime: Option<RuntimeProof>,
    all_receipts: Vec<TerminalReceipt>,
) -> Result<ExitManifest, String> {
    let outer = key.operation.as_str();
    let other_receipts: Vec<TerminalReceipt> = all_receipts.iter().filter(|r| r.node_id != final_primary.node_id || r.incarnation_id != final_primary.incarnation_id).cloned().collect();
    mesh_left_rows(rows, meshes, &records.caches, &records.declared, executor, key, mesh, &all_receipts).await?;
    let deleted_manifest = ExitManifest { mesh_id: key.mesh_id.to_string(), roster: roster.clone(), final_primary: final_primary.clone(), final_runtime, receipts: other_receipts };
    record_completion(builds, key, executor, &deleted_manifest).await?;
    let left_manifest = ExitManifest { mesh_id: key.mesh_id.to_string(), roster, final_primary: final_primary.clone(), final_runtime: None, receipts: all_receipts };
    builds
        .append_step_receipt(&BuildStepReceipt {
            mesh: None,
            build_id: key.build_id.clone(),
            attempt: key.attempt,
            operation: key.operation.clone(),
            step: STEP_MESH_LEFT.into(),
            outcome: StepOutcome::Complete,
            output: serde_json::to_value(&left_manifest).ok(),
            executor: Some(executor.to_string()),
        })
        .await
        .map_err(|e| format!("{outer}: the {STEP_MESH_LEFT} receipt could not be recorded: {e}"))?;
    Ok(left_manifest)
}

/// Release the fabric's name on `storage` (`FabricStorage::release_name`) and record the
/// `fabric.delete:<fabric_id>` `Complete` receipt on `builds` (lifecycles/fabric-delete.md step 3:
/// "The fabric-primary, still alive, releases the fabric name and reports `fabric.deleted`").
pub async fn complete_fabric_delete(
    builds: &dyn BuildStateAdapter,
    storage: &dyn crate::fabric_storage::FabricStorage,
    build_id: &BuildId,
    attempt: u32,
    fabric_id: &rdm_core::FabricId,
    executor: &str,
) -> Result<(), String> {
    let operation = format!("fabric.delete:{fabric_id}");
    if let Some(identity) = storage.fabric().await.map_err(|e| format!("{operation}: fabric.storage could not read the fabric identity: {e}"))?.map(|r| r.identity()) {
        if identity.fabric_id == *fabric_id {
            crate::fabric_storage::release_name(&*storage, &identity).await.map_err(|e| format!("{operation}: fabric.storage could not release the fabric name {}: {e}", identity.name))?;
        }
    }
    builds
        .append_step_receipt(&BuildStepReceipt {
            mesh: None,
            build_id: build_id.clone(),
            attempt,
            operation: operation.clone(),
            step: "Complete".into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: Some(executor.to_string()),
        })
        .await
        .map_err(|e| format!("{operation}: the Complete receipt could not be recorded: {e}"))?;
    tracing::info_span!("rdm.node_admin.fabric.delete.via-build", node = %executor, fabric_id = %fabric_id, build_id = %build_id.log(), attempt, operation = %operation, outcome = "fabric-deleted")
        .in_scope(|| tracing::info!("fabric.deleted: the fabric name is released and the fabric-primary awaits its separate node.delete"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An owner of a leave, named by a fresh node id.
    fn owner() -> MeshDeleteOwner {
        let id = NodeId::mint();
        MeshDeleteOwner { name: id.to_string(), target: rdm_transport::NodeTarget::ExactNode(id) }
    }

    fn birth(node: &str, inc: &str) -> RosterBirth {
        RosterBirth { node: node.into(), node_id: format!("id-{node}"), incarnation_id: inc.into() }
    }
    fn receipt(b: &RosterBirth) -> TerminalReceipt {
        TerminalReceipt {
            node: b.node.clone(),
            node_id: b.node_id.clone(),
            incarnation_id: b.incarnation_id.clone(),
            runtime: Some(RuntimeProof { deployment_id: "d".into(), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: "l".into() }),
            exit_code: Some(0),
            verdict: None,
            data_dir: None,
        }
    }

    /// CONTRACT (lifecycles/mesh-delete.md step 1: the fabric-primary "captures the exact roster once";
    /// lifecycles/node-create.md step 1: the row writer claims the name before the deployment is recorded
    /// and launched; build.md §1: an operation admitted before fabric.delete's acceptance finishes or
    /// fails within its bound and its launch evidence is retained): a roster captured from a mesh's node
    /// rows while a create holds its claim (the claim row, its birth allocated and no birth fact written
    /// yet) holds that create's birth, by the incarnation its claim allocated, beside every member's
    /// newest birth. What must NOT happen: the claimed birth left out, so its launched runtime is no
    /// birth of the delete and is never deleted.
    #[test]
    fn a_roster_captured_from_the_node_rows_holds_the_birth_a_create_in_flight_claimed() {
        use crate::storage::{Allocation, BirthFacts, NodeRow};
        let mesh_id = MeshId::mint();
        let joined = |name: &str| {
            let name: crate::model::PathName = name.parse().unwrap();
            let mut row = NodeRow::new(name.clone(), NodeId::mint(), crate::model::NodeStatus::Ready);
            row.mesh_id = Some(mesh_id.clone());
            row.births.push(BirthFacts::new(crate::model::IncarnationId::mint(), name));
            row
        };
        let (admin, rpc) = (joined("mesh1.admin.1"), joined("mesh1.rpc.1"));
        let claimed_inc = crate::model::IncarnationId::mint();
        let mut claim = NodeRow::new("mesh1.rpc.2".parse().unwrap(), NodeId::mint(), crate::model::NodeStatus::Pending);
        claim.allocated = Some(Allocation { build_id: BuildId::mint(), operation: "node.create:mesh1.rpc.2".into(), incarnation: claimed_inc.clone(), deployment_id: crate::model::DeploymentId::mint() });
        let roster = stored_roster(&[admin.clone(), rpc.clone(), claim.clone()], "mesh1", &mesh_id, &[]);
        let of = |r: &NodeRow, inc: &crate::model::IncarnationId| RosterBirth { node: r.name.to_string(), node_id: r.node_id.to_string(), incarnation_id: inc.as_str().to_string() };
        let mut want = vec![of(&admin, &admin.births[0].incarnation_id), of(&rpc, &rpc.births[0].incarnation_id), of(&claim, &claimed_inc)];
        want.sort();
        assert_eq!(roster, want, "the create in flight's claimed birth is a birth of the roster");
    }

    /// CONTRACT: the handoff of a running leave goes to the admin that sent `delete-mesh` last: a
    /// successor fabric-primary re-dispatching the same attempt while the member workflows run
    /// takes the handoff over, and a repeated `delete-mesh` after the manifest is recorded resends
    /// it to that sender.
    #[test]
    fn a_delete_mesh_from_a_successor_owner_takes_over_the_running_leaves_handoff() {
        let runs = MeshDeleteRuns::default();
        let key = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), attempt: 1, operation: "mesh.delete:m".into() };
        let (first, successor) = (owner(), owner());
        assert_eq!(runs.admit(&key, &first), MeshDeleteAdmission::Start);
        assert_eq!(runs.admit(&key, &successor), MeshDeleteAdmission::Running);
        let last = birth("mesh2.admin.1", "i3");
        let manifest = ExitManifest { mesh_id: "m".into(), roster: vec![last.clone()], final_primary: last, final_runtime: None, receipts: vec![] };
        assert!(matches!(runs.exited(&key, &manifest), RunExit::Exited(o) if o == successor), "the handoff goes to the owner that sent delete-mesh last");
        let third = owner();
        assert_eq!(runs.admit(&key, &third), MeshDeleteAdmission::Resend { manifest, recorded: 1 });
        assert_eq!(runs.incomplete(&key.mesh_id), Some((key.clone(), third)), "a resend is owned by its sender");
        // A later attempt after the manifest is recorded runs again under its own key, from the receipts.
        let later = MeshDeleteKey { attempt: 2, ..key.clone() };
        let fourth = owner();
        assert_eq!(runs.admit(&later, &fourth), MeshDeleteAdmission::Start);
    }

    /// CONTRACT: `delete-mesh` admission at the leaving primary is keyed on the natural key
    /// `(mesh_id, Leaving)` (node-rpc-envelope.md, "Status": "Idempotency is the natural key ...
    /// `(mesh_id, state)`"), never on the attempt: a `delete-mesh` of a later attempt of the Build
    /// that reaches the primary while the member workflows of an earlier attempt run is the same
    /// leave, answered `AlreadyApplied` (`Running`), and starts no second set of member workflows.
    /// What must NOT happen: `Start` for it, a second run beside the first stopping the same members.
    #[test]
    fn a_delete_mesh_of_a_later_attempt_while_the_members_run_joins_the_run_under_way() {
        let runs = MeshDeleteRuns::default();
        let first = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), attempt: 1, operation: "mesh.delete:m".into() };
        let later = MeshDeleteKey { attempt: 2, ..first.clone() };
        let (owner1, owner2) = (owner(), owner());
        assert_eq!(runs.admit(&first, &owner1), MeshDeleteAdmission::Start, "the first delete-mesh starts the member workflows");
        assert_eq!(runs.admit(&later, &owner2), MeshDeleteAdmission::Running, "a later attempt's delete-mesh while the member workflows run is the same leave (mesh_id, Leaving)");
    }

    /// CONTRACT: the leave of a mesh has one run of its member workflows (the natural key
    /// `(mesh_id, Leaving)`). A `delete-mesh` of a later attempt admitted while the run is under way
    /// joins it; when the run ends, its manifest is recorded again under the later attempt's key by
    /// the run over the receipts every member's exit already holds (one more call of the run, after
    /// the first ended, never beside it), and the run ends naming the later attempt and its sender,
    /// whom the handoff goes to (node-rpc-envelope.md, "Mesh delete command pair": the manifest is
    /// scoped to `(mesh_id, build_id, attempt, operation)`). A repeat of that `delete-mesh` resends
    /// the handoff. What must NOT happen: two runs at once, or a handoff naming the earlier attempt,
    /// whose open leave the owner no longer holds.
    #[tokio::test]
    async fn a_later_delete_mesh_joins_the_one_run_and_the_run_ends_naming_the_later_attempt() {
        use tracing_subscriber::layer::SubscriberExt;
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (capture, layer) = rdm_transport::span_capture("i143-s48-leave-admission", exporter.clone());
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

        let runs = Arc::new(MeshDeleteRuns::default());
        let first = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), attempt: 1, operation: "mesh.delete:m".into() };
        let later = MeshDeleteKey { attempt: 2, ..first.clone() };
        let (owner1, owner2) = (owner(), owner());
        let last = birth("mesh2.admin.1", "i3");
        let manifest = ExitManifest { mesh_id: "m".into(), roster: vec![last.clone()], final_primary: last, final_runtime: None, receipts: vec![] };
        assert_eq!(runs.admit(&first, &owner1), MeshDeleteAdmission::Start);

        // The run: the first call holds until the cell opens the gate.
        let calls: Arc<Mutex<Vec<(u32, MeshDeleteOwner)>>> = Arc::default();
        let (live, most) = (Arc::new(std::sync::atomic::AtomicUsize::new(0)), Arc::new(std::sync::atomic::AtomicUsize::new(0)));
        let (entered, gate) = (Arc::new(tokio::sync::Notify::new()), Arc::new(watch::Sender::new(false)));
        let run = {
            let (calls, live, most, entered, gate, manifest) = (calls.clone(), live.clone(), most.clone(), entered.clone(), gate.clone(), manifest.clone());
            move |key: MeshDeleteKey, owner: MeshDeleteOwner| {
                let (calls, live, most, entered, gate, manifest) = (calls.clone(), live.clone(), most.clone(), entered.clone(), gate.clone(), manifest.clone());
                async move {
                    let now = live.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    most.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                    calls.lock().unwrap().push((key.attempt, owner));
                    entered.notify_one();
                    let _ = gate.subscribe().wait_for(|open| *open).await;
                    live.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    Ok::<ExitManifest, String>(manifest)
                }
            }
        };
        let driver = {
            let (runs, mesh_id) = (runs.clone(), first.mesh_id.clone());
            tokio::spawn(async move { run_admitted(&runs, &mesh_id, run).await })
        };
        entered.notified().await;
        assert_eq!(runs.admit(&later, &owner2), MeshDeleteAdmission::Running, "attempt 2's delete-mesh joins the run under way");
        gate.send_replace(true);
        let end = driver.await.unwrap();
        assert_eq!(end, MeshDeleteRunEnd::Exited { key: later.clone(), owner: owner2.clone(), manifest: manifest.clone() }, "the run ends naming attempt 2 and its sender");
        assert_eq!(*calls.lock().unwrap(), vec![(1, owner1), (2, owner2.clone())], "the run is called once more under attempt 2's key once attempt 1's call ended");
        assert_eq!(most.load(std::sync::atomic::Ordering::SeqCst), 1, "never two runs at once");
        assert_eq!(runs.admit(&later, &owner2), MeshDeleteAdmission::Resend { manifest, recorded: 2 }, "a repeat of attempt 2's delete-mesh resends its handoff");

        capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let joined: Vec<_> = exporter.get_finished_spans().unwrap().into_iter().filter(|s| s.name == "rdm.node_admin.mesh.resolve.via-later-delete-mesh").collect();
        assert_eq!(joined.len(), 1, "one later delete-mesh joined the run");
        let attrs: std::collections::BTreeMap<String, String> = joined[0].attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect();
        assert_eq!((attrs.get("attempt").map(String::as_str), attrs.get("ran_attempt").map(String::as_str)), (Some("2"), Some("1")), "{attrs:?}");
        eprintln!("span rdm.node_admin.mesh.resolve.via-later-delete-mesh trace_id={} {attrs:?}", joined[0].span_context.trace_id());
    }

    /// CONTRACT: an open leave the leaving primary reports incomplete ends at once with the named
    /// reason (the owner's wait resolves now, never on a deadline), and a later handoff for the same
    /// leave does not overwrite that outcome.
    #[test]
    fn an_incomplete_report_ends_the_open_leave_at_once_with_its_reason() {
        let mesh_delete_view = MeshDeleteView::default();
        let key = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), attempt: 1, operation: "mesh.delete:m".into() };
        let last = birth("mesh2.admin.1", "i3");
        let runtime = receipt(&last).runtime.unwrap();
        let rx = mesh_delete_view.open(key.clone(), Admitted { roster: vec![last.clone()], final_primary: last.clone(), final_runtime: runtime, amendments: Vec::new() });
        assert!(rx.borrow().is_none());
        assert!(mesh_delete_view.incomplete(&key, "roster births without an exact Exited receipt: mesh2.rpc.1 i1".into()));
        assert_eq!(rx.borrow().clone(), Some(Err("roster births without an exact Exited receipt: mesh2.rpc.1 i1".into())), "the wait resolves now with the reason");
        let manifest = ExitManifest { mesh_id: "m".into(), roster: vec![last.clone()], final_primary: last, final_runtime: None, receipts: vec![] };
        assert!(!mesh_delete_view.resolve(&key, manifest), "the first outcome stands");
        assert!(matches!(rx.borrow().clone(), Some(Err(_))));
    }

    /// CONTRACT: a manifest covers the accepted roster exactly: a missing receipt, an extra or
    /// substituted birth, a receipt with no exact runtime and a missing final runtime are each
    /// named by the first field that is wrong.
    #[test]
    fn a_manifest_covers_the_roster_exactly_and_names_what_it_does_not() {
        let (a, b, last) = (birth("mesh2.rpc.1", "i1"), birth("mesh2.admin.2", "i2"), birth("mesh2.admin.1", "i3"));
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
        extra_birth.roster.push(birth("mesh2.rpc.9", "i9"));
        assert_eq!(first_mismatch(&extra_birth, &roster, &last, false).map(|m| m.0), Some("roster".into()));
        let mut no_runtime = ok.clone();
        no_runtime.receipts[0].runtime = None;
        assert_eq!(first_mismatch(&no_runtime, &roster, &last, false).map(|m| m.0), Some("receipt.runtime".into()));
        let mut no_final = ok.clone();
        no_final.final_runtime = None;
        assert_eq!(first_mismatch(&no_final, &roster, &last, false).map(|m| m.0), Some("final_runtime".into()));
        // The complete departure includes the final primary's own exit.
        let mut complete = ok.clone();
        complete.final_runtime = None;
        assert_eq!(first_mismatch(&complete, &roster, &last, true).map(|m| m.0), Some("receipt".into()), "the final primary has no receipt yet");
        complete.receipts.push(receipt(&last));
        assert_eq!(first_mismatch(&complete, &roster, &last, true), None);
    }
    /// CONTRACT (mesh-delete.md "Failure and receipts"; node-rpc-envelope.md "Mesh delete command pair":
    /// "A lost handoff or missing exact receipt leaves the workflow incomplete, never successful"): the
    /// mesh-primary's MeshDeleted report for attempt 1 is lost. The owner's bounded wait ends attempt 1
    /// incomplete, its receipts standing. The owner's next attempt re-admits the same mesh.delete, and the
    /// mesh-primary replays the recorded report, naming the receipt recorded under attempt 1, with no
    /// second run of the member workflows. The replay validates against attempt 2's open workflow from that
    /// receipt and resolves it exactly once.
    #[tokio::test]
    async fn a_lost_mesh_deleted_report_is_replayed_to_the_next_attempt_and_resolves_it_exactly_once() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let runs = MeshDeleteRuns::default();
        let first = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), attempt: 1, operation: "mesh.delete:m".into() };
        let next = MeshDeleteKey { attempt: 2, ..first.clone() };
        let owner = owner();
        let last = birth("mesh2.admin.1", "i3");
        let proof = RuntimeProof { deployment_id: "d".into(), provider: "process".into(), control_domain_fingerprint: "f".into(), locator_kind: "process-pid-start".into(), locator_fingerprint: "l".into() };
        let manifest = ExitManifest { mesh_id: first.mesh_id.to_string(), roster: vec![last.clone()], final_primary: last.clone(), final_runtime: Some(proof.clone()), receipts: vec![] };
        let admitted = Admitted { roster: vec![last.clone()], final_primary: last.clone(), final_runtime: proof, amendments: vec![] };

        // The member workflows run once and record the manifest under attempt 1; the report the
        // mesh-primary then calls up is lost: the cell delivers nothing to the owner.
        assert_eq!(runs.admit(&first, &owner), MeshDeleteAdmission::Start);
        let ran = AtomicUsize::new(0);
        let end = run_admitted(&runs, &first.mesh_id, |_, _| {
            ran.fetch_add(1, SeqCst);
            let m = manifest.clone();
            async move { Ok(m) }
        })
        .await;
        assert!(matches!(end, MeshDeleteRunEnd::Exited { .. }), "the run records its manifest under attempt 1");

        // The owner's wait for attempt 1's report is bounded: it ends the attempt incomplete.
        let mesh_delete_view = MeshDeleteView::default();
        let (_view, view_rx) = watch::channel(0u64);
        let wait = mesh_delete_view.open(first.clone(), admitted.clone());
        let lost = await_mesh_deleted(&first, &last.node, wait, view_rx.clone(), || std::future::ready(None), std::time::Duration::from_millis(50)).await;
        assert!(matches!(&lost, Err(crate::executor::RunEnd::Incomplete(r)) if r.contains("sent no valid mesh-deleted")), "a lost report ends attempt 1 incomplete: {lost:?}");

        // The owner's next attempt re-admits the same mesh.delete: the recorded report is replayed,
        // naming where its manifest was recorded, and the member workflows do not run again.
        assert_eq!(runs.admit(&next, &owner), MeshDeleteAdmission::Resend { manifest: manifest.clone(), recorded: 1 }, "attempt 2's DeleteMesh replays the recorded report");
        assert_eq!(ran.load(SeqCst), 1, "the member workflows ran once");

        // The replay validates against attempt 2's open workflow from the receipt recorded under attempt 1 ...
        let facts = vec![BuildFact::Step(BuildStepReceipt {
            build_id: first.build_id.clone(),
            attempt: 1,
            operation: first.operation.clone(),
            step: STEP_OTHER_MEMBERS_EXITED.into(),
            outcome: StepOutcome::Complete,
            output: Some(serde_json::to_value(&manifest).unwrap()),
            executor: None,
            mesh: None,
        })];
        let reference = manifest_reference(first.build_id.as_str(), 1, &first.operation, false);
        assert!(matches!(handoff_verdict(&facts, &next, &admitted, &reference), HandoffVerdict::Accept(m) if m == manifest), "attempt 2's report is accepted from the manifest recorded under attempt 1");

        // ... and resolves it exactly once.
        let wait = mesh_delete_view.open(next.clone(), admitted.clone());
        assert!(mesh_delete_view.resolve(&next, manifest.clone()), "the first replay resolves attempt 2");
        assert!(!mesh_delete_view.resolve(&next, manifest.clone()), "a second replay is AlreadyApplied");
        let got = await_mesh_deleted(&next, &last.node, wait, view_rx, || std::future::ready(None), std::time::Duration::from_millis(50)).await;
        assert_eq!(got.ok(), Some(manifest), "attempt 2 continues with the replayed manifest");
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
        let key = MeshDeleteKey { mesh_id: MeshId::mint(), build_id: BuildId::mint(), attempt: 1, operation: "mesh.delete:m".into() };
        let mut boot = MeshRecord::new(key.mesh_id.clone(), "mesh2");
        boot.states.insert(MeshState::Pending);
        crate::storage::claim_mesh(&meshes, None, &boot).await.unwrap().unwrap();
        // The copy this fabric-primary took before mesh2's primary wrote Ready.
        caches.meshes.hold(key.mesh_id.clone(), boot.clone());
        let mut ready = boot.clone();
        ready.states.insert(MeshState::Ready);
        meshes.put_mesh(&ready).await.unwrap();

        let declared = std::sync::Mutex::new(crate::status_rpc::Declared::default());
        ensure_mesh_dead(&meshes, &caches, &declared, "mesh1.admin.1", &key, "mesh2").await.unwrap();
        let stored = meshes.mesh(&key.mesh_id).await.unwrap().expect("mesh2's record stands");
        assert!(stored.dead, "Dead is written: {stored:?}");
        assert!(stored.states.contains(&MeshState::Ready), "the mesh-primary's Ready stands beside Dead: {stored:?}");
        assert!(stored.states.contains(&MeshState::Dead), "{stored:?}");
        assert_eq!(caches.meshes.get(&key.mesh_id), Some(stored), "the copy held is the record written");
    }
}
