//! The mesh-leave workflow's shared parts (mesh-leave.md; node-rpc-envelope.md, "Mesh leave
//! command pair").
//!
//! The fabric-primary owns the outer workflow `shutdown-mesh:<mesh_id>`. The leaving mesh's primary
//! drains and stops every other member (ordinary nodes first, the other node-admins after, each
//! tier a set of parallel member workflows), records one exit manifest, hands it to the
//! fabric-primary (`MeshLeave`) and keeps running. The fabric-primary validates the manifest
//! against the roster it captured at admission, drains and stops the final primary itself, and only
//! with every exact exit proven records `MeshLeft`.
//!
//! The manifest is one immutable step receipt of the accepted Build (`OtherMembersExited` at the
//! mesh-primary, `MeshLeft` at the fabric-primary) under the outer operation; the `receipt_manifest`
//! string of the messages is its reference. The fabric-primary reads it with a Build-facts read
//! (`0x1F`) from the sender, never from a reference alone.

use crate::build::BuildId;
use crate::build_state::{BuildFact, BuildStateAdapter, BuildStepReceipt, StepOutcome};
use crate::deployment::pipeline::{RuntimeProof, TerminalReceipt};
use crate::model::{Node, NodeKind};
use futures_util::future::join_all;
use rafka_mesh_entity::{MeshId, NodeId};
use rafka_node_rpc_contract::status::{StatusReply, StatusRequest};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::watch;

/// The step at the leaving mesh-primary that records every other member's exit.
pub const STEP_OTHER_MEMBERS_EXITED: &str = "OtherMembersExited";
/// The step at the fabric-primary that records the complete departure.
pub const STEP_MESH_LEFT: &str = "MeshLeft";
/// The step at the fabric-primary that records the accepted roster it captured at admission
/// (mesh-leave.md step 1).
pub const STEP_ACCEPTED_ROSTER: &str = "AcceptedRoster";
/// The step at the leaving mesh-primary that records the roster its member workflows run over.
pub const STEP_LEAVING_ROSTER: &str = "LeavingRoster";
/// The step at the fabric-primary that admits a new final primary after the admitted one (or the
/// last amended one) was lost mid-leave (mesh-leave.md, "Resume after the primary's loss").
pub const STEP_ROSTER_AMENDED: &str = "RosterAmended";

/// The outer operation key of `mesh_id`'s shutdown.
pub fn operation(mesh_id: &MeshId) -> String {
    format!("shutdown-mesh:{mesh_id}")
}

/// The operation key a member's drain and stop run under, correlated to the outer one.
pub fn member_operation(outer: &str, path: &str) -> String {
    format!("{outer}/{path}")
}

/// The reference a message carries for a manifest receipt.
pub fn manifest_reference(build_id: &str, attempt: u32, operation: &str, all_members: bool) -> String {
    format!("{build_id}/{attempt}/{operation}/{}", if all_members { "all-members-exited" } else { "other-members-exited" })
}

/// One accepted birth of the leaving mesh.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
        let incarnation_id = n.incarnation_id.as_ref().map(|i| i.0.clone()).ok_or_else(|| format!("{} has no known birth", n.name))?;
        Ok(Self { node: n.name.to_string(), node_id: n.node_id.to_string(), incarnation_id })
    }
}

/// The exit manifest: the accepted roster, and the provider's terminal receipt of every birth that
/// exited. `OtherMembersExited` covers every birth but the final primary (whose exact runtime is
/// named, still running); `MeshLeft` covers every birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitManifest {
    /// The mesh's minted id.
    pub mesh_id: String,
    /// Every accepted birth of the mesh, the final primary included.
    pub roster: Vec<RosterBirth>,
    /// The final mesh-primary.
    pub final_primary: RosterBirth,
    /// The final primary's exact runtime by fingerprint, when `OtherMembersExited` names it still
    /// running; `None` in `MeshLeft`, whose receipts include the final primary's exit.
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

/// The accepted roster of the leave `key` (mesh-leave.md step 1): captured once, at the first
/// admission of the operation in its Build, and kept as its own step receipt under `step`
/// (`AcceptedRoster` at the fabric-primary, `LeavingRoster` at the leaving mesh-primary). The row
/// is an [`ExitManifest`] with no receipts yet: the roster, the final primary and its exact runtime
/// as this admin holds them at admission (the Build-fact codec carries it as the exit-manifest
/// output, `crate::wire`). A resumed attempt reads that receipt and never recaptures: a birth that
/// exited is no longer live, and the proof set must still cover it. With no receipt recorded,
/// `capture()` (the roster as this admin's view holds it now) is recorded with one blind append and
/// returned.
pub async fn accepted_roster(builds: &dyn BuildStateAdapter, key: &LeaveKey, step: &str, executor: &str, final_primary: &RosterBirth, final_runtime: &RuntimeProof, capture: impl FnOnce() -> Result<Vec<RosterBirth>, String>) -> Result<Vec<RosterBirth>, String> {
    let build_id = BuildId(key.build_id.clone());
    let view = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the {step} receipt of {}: {e}", key.operation))?;
    let recorded = view.steps.iter().filter(|r| r.operation == key.operation && r.step == step && r.outcome == StepOutcome::Complete).min_by_key(|r| r.attempt);
    let (roster, source, accepted_in) = match recorded {
        Some(r) => {
            let row: ExitManifest = r
                .output
                .clone()
                .and_then(|v| serde_json::from_value(v).ok())
                .ok_or_else(|| format!("{executor}: the {step} receipt of {} (Build {build_id}, attempt {}) holds no readable roster: the accepted roster cannot be named", key.operation, r.attempt))?;
            (row.roster, "receipt", r.attempt)
        }
        None => {
            let roster = capture()?;
            let row = ExitManifest { mesh_id: key.mesh_id.to_string(), roster: roster.clone(), final_primary: final_primary.clone(), final_runtime: Some(final_runtime.clone()), receipts: Vec::new() };
            builds
                .append_step_receipt(&BuildStepReceipt {
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
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-accepted-roster", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %key.operation, step, source, accepted_in, roster = roster.len())
        .in_scope(|| tracing::info!("the leave runs over the roster accepted at its admission"));
    Ok(roster)
}

/// The exact Exited receipts the Build already holds for births of `roster`: each member workflow's
/// `TerminateRuntime` step receipt under `<operation>/<path>`, from any attempt, whose terminal
/// receipt names that exact birth.
pub fn exit_receipts(steps: &[BuildStepReceipt], operation: &str, roster: &[RosterBirth]) -> BTreeMap<(String, String), TerminalReceipt> {
    let terminate = crate::deployment::pipeline::RetireStep::TerminateRuntime.name();
    let mut held = BTreeMap::new();
    for b in roster {
        let member = member_operation(operation, &b.node);
        let receipt = steps
            .iter()
            .filter(|r| r.operation == member && r.step == terminate && r.outcome == StepOutcome::Complete)
            .filter_map(|r| r.output.clone().and_then(|v| serde_json::from_value::<TerminalReceipt>(v).ok()))
            .find(|t| t.node_id == b.node_id && t.incarnation_id == b.incarnation_id);
        if let Some(t) = receipt {
            held.insert((b.node_id.clone(), b.incarnation_id.clone()), t);
        }
    }
    held
}

/// Where the fabric-primary's outer workflow `operation` resumes, read from its own Build facts
/// (`steps`) before it calls anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerResume {
    /// The `MeshLeft` receipt is recorded: the departure is the record.
    Left,
    /// The final primary's exact exit is recorded (the owner stopped it, then `MeshLeft` was not
    /// recorded): the complete proof set, from the receipts, that `MeshLeft` records. Nothing is
    /// called: the final primary is gone and sends nothing.
    FinalExited(ExitManifest),
    /// Nothing recorded settles the leave: the owner leads it (admission, `LeaveMesh`, the
    /// handoff, the final primary's stop).
    Lead,
}

impl OwnerResume {
    /// The complete proof set this resume records as `MeshLeft` without calling the mesh, when it
    /// holds one.
    pub fn mesh_left_manifest(&self) -> Option<&ExitManifest> {
        match self {
            OwnerResume::FinalExited(all) => Some(all),
            OwnerResume::Left | OwnerResume::Lead => None,
        }
    }
}

/// [`OwnerResume`] for `operation` over `steps`.
pub fn owner_resume(steps: &[BuildStepReceipt], operation: &str) -> Result<OwnerResume, String> {
    if steps.iter().any(|r| r.operation == operation && r.step == STEP_MESH_LEFT && r.outcome == StepOutcome::Complete) {
        return Ok(OwnerResume::Left);
    }
    let Some(admitted) = admitted_of(steps, operation).transpose()? else {
        return Ok(OwnerResume::Lead);
    };
    let last = &admitted.final_primary;
    let Some(terminal) = exit_receipts(steps, operation, std::slice::from_ref(last)).remove(&(last.node_id.clone(), last.incarnation_id.clone())) else {
        return Ok(OwnerResume::Lead);
    };
    // The final primary's exit with no exit manifest naming it as final primary is a primary lost
    // mid-leave, never a stop by this owner: the resume amends the roster (mesh-leave.md, "Resume
    // after the primary's loss").
    let Some(manifest_row) = steps
        .iter()
        .filter(|r| r.operation == operation && r.step == STEP_OTHER_MEMBERS_EXITED && r.outcome == StepOutcome::Complete)
        .filter(|r| r.output.clone().and_then(|v| serde_json::from_value::<ExitManifest>(v).ok()).is_some_and(|m| &m.final_primary == last))
        .max_by_key(|r| r.attempt)
    else {
        return Ok(OwnerResume::Lead);
    };
    // The final primary's exact exit is recorded: the rest of the proof is in this log or nowhere.
    let named = format!("{} {}", last.node, last.incarnation_id);
    if terminal.runtime.as_ref() != Some(&admitted.final_runtime) {
        return Err(format!("{operation}: the final primary {named}'s Exited receipt names the runtime {:?}, not the one admitted for it {:?}", terminal.runtime, admitted.final_runtime));
    }
    let manifest: ExitManifest = manifest_row.output.clone().and_then(|v| serde_json::from_value(v).ok()).ok_or_else(|| format!("{operation}: the {STEP_OTHER_MEMBERS_EXITED} manifest held in this Build's facts does not decode: MeshLeft cannot be recorded"))?;
    if manifest.final_runtime.as_ref() != Some(&admitted.final_runtime) {
        return Err(format!("{operation}: the {STEP_OTHER_MEMBERS_EXITED} manifest names the final runtime {:?}, not the one admitted for it {:?}", manifest.final_runtime, admitted.final_runtime));
    }
    let mut all = manifest;
    all.final_runtime = None;
    all.receipts.push(terminal);
    if let Some((field, expected, reported)) = first_mismatch(&all, &admitted.roster, last, true) {
        return Err(format!("{operation}: the recorded proof set is not complete: {field}: expected {expected}, reported {reported}"));
    }
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-recorded-final-exit", operation = %operation, final_primary = %named, receipts = all.receipts.len(), roster = admitted.roster.len(), amendments = admitted.amendments.len())
        .in_scope(|| tracing::info!("the final primary's exact exit and every other member's are recorded: MeshLeft is recorded from the receipts, nothing is called"));
    Ok(OwnerResume::FinalExited(all))
}

/// The roster row recorded under `step` (`AcceptedRoster`, `LeavingRoster`) for `operation`, from
/// the first attempt that recorded it: `None` when none is recorded, `Err` when it does not decode.
pub fn accepted_row(steps: &[BuildStepReceipt], operation: &str, step: &str) -> Option<Result<ExitManifest, String>> {
    let r = steps.iter().filter(|r| r.operation == operation && r.step == step && r.outcome == StepOutcome::Complete).min_by_key(|r| r.attempt)?;
    Some(r.output.clone().and_then(|v| serde_json::from_value(v).ok()).ok_or_else(|| format!("the {step} receipt of {operation} (attempt {}) holds no readable roster: the accepted roster cannot be named", r.attempt)))
}

/// A roster amendment (mesh-leave.md, "Resume after the primary's loss", step 2): `final_primary`
/// replaces `predecessor`, whose exact Exited receipt the Build already holds. `final_runtime` is the
/// new final primary's exact runtime as the fabric-primary's provider domain holds it at the
/// amendment, what a later resume checks the final runtime against, as `AcceptedRoster` carries the
/// admitted one's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
/// extended by every `RosterAmended` receipt (mesh-leave.md, "Resume after the primary's loss";
/// node-rpc-envelope.md, mesh-leave manifest): the roster is the accepted one plus every amended
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

/// The span of a resume refused because the lost final primary `predecessor` is silent but not
/// proven exited (R-D1: unheard is not dead).
pub fn span_unproven(key: &LeaveKey, predecessor: &RosterBirth, reason: &str) {
    tracing::info_span!("rdm.node_admin.mesh.reject.via-unproven-primary", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %key.operation, predecessor = %format!("{} {}", predecessor.node, predecessor.incarnation_id), reason = %reason)
        .in_scope(|| tracing::info!("the lost final primary is not proven exited: the resume is refused and nothing is amended"));
}

/// Record the lost final primary's exact provider Exited `terminal` as its roster receipt (step 1 of
/// the resume): the `TerminateRuntime` step receipt of its member operation, which every reader of
/// exit receipts (`exit_receipts`) takes. `terminal` must name the current final primary's exact
/// birth and the runtime admitted (or amended) for it; anything else proves nothing and is refused.
pub async fn record_predecessor_exit(builds: &dyn BuildStateAdapter, key: &LeaveKey, executor: &str, terminal: &TerminalReceipt) -> Result<(), String> {
    let build_id = BuildId(key.build_id.clone());
    let operation = key.operation.as_str();
    let steps = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the receipts of {operation}: {e}"))?.steps;
    let admitted = admitted_of(&steps, operation).ok_or_else(|| format!("{operation}: no {STEP_ACCEPTED_ROSTER} receipt is recorded: there is no final primary to replace"))??;
    let lost = &admitted.final_primary;
    if terminal.node_id != lost.node_id || terminal.incarnation_id != lost.incarnation_id {
        return Err(format!("{operation}: the Exited receipt names {} {}, not the final primary {} {}", terminal.node, terminal.incarnation_id, lost.node, lost.incarnation_id));
    }
    if terminal.runtime.as_ref() != Some(&admitted.final_runtime) {
        return Err(format!("{operation}: the Exited receipt of {} {} names the runtime {:?}, not the one admitted for it {:?}: it proves nothing", lost.node, lost.incarnation_id, terminal.runtime, admitted.final_runtime));
    }
    if exit_receipts(&steps, operation, std::slice::from_ref(lost)).contains_key(&(lost.node_id.clone(), lost.incarnation_id.clone())) {
        return Ok(());
    }
    builds
        .append_step_receipt(&BuildStepReceipt {
            build_id,
            attempt: key.attempt,
            operation: member_operation(operation, &lost.node),
            step: crate::deployment::pipeline::RetireStep::TerminateRuntime.name().to_string(),
            outcome: StepOutcome::Complete,
            output: serde_json::to_value(terminal).ok(),
            executor: Some(executor.to_string()),
        })
        .await
        .map_err(|e| format!("{operation}: the Exited receipt of {} {} could not be recorded: {e}", lost.node, lost.incarnation_id))?;
    tracing::info_span!("rdm.node_admin.mesh.update.via-predecessor-exit-recorded", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %operation, predecessor = %format!("{} {}", lost.node, lost.incarnation_id), exit_code = terminal.exit_code.map(|c| c.to_string()).unwrap_or_default())
        .in_scope(|| tracing::info!("the lost final primary's exact exit is recorded as its roster receipt"));
    Ok(())
}

/// Record `amendment` under `key`'s operation (step 2 of the resume), only after the predecessor's
/// exact Exited receipt is recorded for that birth and runtime. The identical amendment replays
/// `AlreadyApplied`; a second amendment for the same predecessor is refused by name; an amendment
/// of a birth that is not the current final primary, or that names an exited birth, is refused.
pub async fn amend_roster(builds: &dyn BuildStateAdapter, key: &LeaveKey, executor: &str, amendment: &RosterAmended) -> Result<Amended, String> {
    let build_id = BuildId(key.build_id.clone());
    let operation = key.operation.as_str();
    let named = |b: &RosterBirth| format!("{} {}", b.node, b.incarnation_id);
    let (pred, next) = (named(&amendment.predecessor), named(&amendment.final_primary));
    let span = |outcome: &str| {
        tracing::info_span!("rdm.node_admin.mesh.update.via-roster-amended", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %operation, predecessor = %pred, final_primary = %next, outcome)
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

/// The provider's exact Exited of the lost final primary `lost`, whose runtime was admitted as
/// `runtime` (mesh-leave.md, "Resume after the primary's loss", step 1), proven from the durable
/// runtime row this admin stored for that birth (`rows`, the blind put every admin makes the moment
/// it first holds a birth): the same runtime by fingerprint, adopted in this admin's provider domain,
/// and that runtime's own terminal status. A birth no row names, another runtime, or a runtime not
/// proven exited proves nothing.
pub async fn prove_predecessor_exit(provider: &dyn crate::deployment::provider::DeploymentProvider, rows: &[crate::storage::RuntimeRow], lost: &RosterBirth, runtime: &RuntimeProof) -> Result<TerminalReceipt, String> {
    let named = format!("{} {}", lost.node, lost.incarnation_id);
    let row = rows
        .iter()
        .find(|r| r.node_id.to_string() == lost.node_id && r.incarnation_id.0 == lost.incarnation_id)
        .ok_or_else(|| format!("no durable runtime row of {named} is held ({} rows): its runtime is unknown", rows.len()))?;
    if &RuntimeProof::of(&row.runtime) != runtime {
        return Err(format!("the durable runtime row of {named} names the runtime {:?}, not the one admitted for it {runtime:?}", RuntimeProof::of(&row.runtime)));
    }
    let handle = crate::deployment::provider::adopt(provider, &row.runtime).map_err(|r| format!("the runtime of {named} cannot be adopted in this provider domain: {r}"))?;
    let inspected = provider.inspect(&handle).await;
    match crate::deployment::provider::exit_proof(inspected, &row.runtime, row.data_dir.as_deref().map(std::path::Path::new), &lost.incarnation_id).await {
        Ok(crate::deployment::provider::DeploymentStatus::Exited { code }) => Ok(TerminalReceipt { node: lost.node.clone(), node_id: lost.node_id.clone(), incarnation_id: lost.incarnation_id.clone(), runtime: Some(runtime.clone()), exit_code: code }),
        Ok(other) => Err(format!("the runtime of {named} is not proven exited: {other:?}")),
        Err(e) => Err(format!("the exit record of {named} cannot be read: {e}")),
    }
}

/// The fabric-primary's resume after the leaving mesh's final primary `admitted.final_primary` was
/// lost and `held` (runtime `held_runtime`) is the mesh's node-admin primary now (mesh-leave.md,
/// "Resume after the primary's loss", steps 1-2): the lost birth's exact Exited, proven from the
/// durable runtime rows `rows` unless the Build already holds it, is recorded as its roster receipt,
/// then `RosterAmended` admits `held`. A lost primary not proven exited refuses the resume by name
/// and nothing is amended (R-D1).
#[allow(clippy::too_many_arguments)]
pub async fn resume_after_loss(builds: &dyn BuildStateAdapter, provider: &dyn crate::deployment::provider::DeploymentProvider, rows: &[crate::storage::RuntimeRow], key: &LeaveKey, executor: &str, admitted: &Admitted, held: &RosterBirth, held_runtime: &RuntimeProof) -> Result<Amended, String> {
    let operation = key.operation.as_str();
    let lost = &admitted.final_primary;
    let steps = builds.read_build(&BuildId(key.build_id.clone())).await.map_err(|e| format!("{executor}: the Build {} could not be read for the receipts of {operation}: {e}", key.build_id))?.steps;
    if !exit_receipts(&steps, operation, std::slice::from_ref(lost)).contains_key(&(lost.node_id.clone(), lost.incarnation_id.clone())) {
        let terminal = prove_predecessor_exit(provider, rows, lost, &admitted.final_runtime).await.map_err(|reason| {
            span_unproven(key, lost, &reason);
            format!("{operation} is refused: the final primary {} {} is silent but not proven exited, so {} {} is not admitted in its place: {reason}", lost.node, lost.incarnation_id, held.node, held.incarnation_id)
        })?;
        record_predecessor_exit(builds, key, executor, &terminal).await?;
    }
    amend_roster(builds, key, executor, &RosterAmended { predecessor: lost.clone(), final_primary: held.clone(), final_runtime: held_runtime.clone() }).await
}

/// The roster a leaving primary runs `LeaveMesh` over, and the owner's step receipts it read for it
/// (mesh-leave.md, "Resume after the primary's loss", step 3): its `LeavingRoster` `roster` extended
/// by every roster amendment. This Build's own facts (`own`) are gossip-fed and may not yet hold the
/// amendment that names `me` final primary, nor the lost primary's exit receipt recorded before it;
/// gossip is never authority, so when the amendment chain `own` gives does not end at `me`, the
/// owner's Build facts are read (`fetch_owner`, the same read `validate_handoff` makes of the
/// sender's) and the amendments and exit receipts are taken from both. A chain that still does not
/// end at `me` refuses the run by name. What is read from the owner is used for this run and
/// written nowhere.
pub async fn admission_roster<F, Fut>(roster: &[RosterBirth], own: &[BuildStepReceipt], operation: &str, me: &RosterBirth, fetch_owner: F) -> Result<(Vec<RosterBirth>, Vec<BuildStepReceipt>), String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<BuildFact>, String>>,
{
    let named = |b: &RosterBirth| format!("{} {}", b.node, b.incarnation_id);
    let first = accepted_row(own, operation, STEP_LEAVING_ROSTER).ok_or_else(|| format!("{operation}: no {STEP_LEAVING_ROSTER} receipt is recorded: the leave has no roster to run over"))??.final_primary;
    let last = |steps: &[BuildStepReceipt]| amendment_chain(&first, steps, operation).map(|c| c.last().map(|a| a.final_primary.clone()).unwrap_or_else(|| first.clone()));
    if &last(own)? == me {
        return Ok((amended_roster(roster, own, operation)?, Vec::new()));
    }
    let known: Vec<BuildStepReceipt> = fetch_owner().await?.into_iter().filter_map(|f| if let BuildFact::Step(r) = f { Some(r) } else { None }).filter(|r| r.operation == operation || r.operation.starts_with(&format!("{operation}/"))).collect();
    let all: Vec<BuildStepReceipt> = own.iter().cloned().chain(known.iter().cloned()).collect();
    let final_primary = last(&all)?;
    let amendments = amendment_chain(&first, &all, operation)?.len();
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-owner-build-facts", operation = %operation, me = %named(me), final_primary = %named(&final_primary), amendments, owner_receipts = known.len())
        .in_scope(|| tracing::info!("this admin's own Build facts do not name it final primary: the owner's Build facts are read for the amendments"));
    if &final_primary != me {
        return Err(format!("{operation}: {} is not the final primary: the owner's Build facts name {} after {amendments} amendment(s), so no member is stopped", named(me), named(&final_primary)));
    }
    Ok((amended_roster(roster, &all, operation)?, known))
}

/// The path of the one node-admin the fabric-primary creates for a leaving mesh no admin of which
/// survives (mesh-leave.md, "Resume after the primary's loss", step 2: "the one node-admin the
/// fabric-primary created for it, the only new birth a leaving mesh may gain"): a new birth, so a
/// path no birth ever held, the next admin ordinal of `mesh` above every one the accepted roster
/// (`roster`) and the view (`held`, every status, `.old` names included) hold. A path a lost birth
/// still holds is never reused: the create door would fence against that birth and replace it.
pub fn leaving_mesh_admin_path(mesh: &str, roster: &[RosterBirth], held: &[crate::model::PathName]) -> Result<crate::model::PathName, String> {
    let named = roster.iter().filter_map(|b| b.node.parse::<crate::model::PathName>().ok());
    let top = named.chain(held.iter().cloned()).filter(|p| p.mesh == mesh && p.kind == NodeKind::NodeAdmin).map(|p| p.ordinal).max().unwrap_or(0);
    let next = top.checked_add(1).ok_or_else(|| format!("{mesh}: no admin ordinal is free above {top}"))?;
    Ok(crate::model::PathName::new(mesh, NodeKind::NodeAdmin, next))
}

/// The roster a leaving primary `me` runs `LeaveMesh` over, and the owner's step receipts it read
/// for it (mesh-leave.md step 1 and "Resume after the primary's loss"). A `LeavingRoster` this admin
/// holds is the roster captured once ([`admission_roster`] extends it by the amendments). With none
/// held, this admin does not know from its own facts whether it is admitted at all: the owner's
/// Build facts are read (`fetch_owner`, the sender of `LeaveMesh`; gossip is never authority) and
/// what the owner admitted decides. Admitted as the final primary at admission: the first run of the
/// leave, and `capture()` is recorded as the `LeavingRoster`. Admitted by a `RosterAmended` (the admin
/// the election seated, or the one node-admin the fabric-primary created when none survived): the
/// roster is the owner's accepted roster plus every amendment, used for this run and written
/// nowhere, so the accepted roster is never recaptured. Any other final primary refuses the run by
/// name, both births named.
#[allow(clippy::too_many_arguments)]
pub async fn leaving_roster<C, F, Fut>(builds: &dyn BuildStateAdapter, key: &LeaveKey, executor: &str, me: &RosterBirth, my_runtime: &RuntimeProof, capture: C, fetch_owner: F) -> Result<(Vec<RosterBirth>, Vec<BuildStepReceipt>), String>
where
    C: FnOnce() -> Result<Vec<RosterBirth>, String>,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<BuildFact>, String>>,
{
    let operation = key.operation.as_str();
    let named = |b: &RosterBirth| format!("{} {}", b.node, b.incarnation_id);
    let build_id = BuildId(key.build_id.clone());
    let own = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the receipts of {operation}: {e}"))?.steps;
    if accepted_row(&own, operation, STEP_LEAVING_ROSTER).is_some() {
        let roster = accepted_roster(builds, key, STEP_LEAVING_ROSTER, executor, me, my_runtime, capture).await?;
        return admission_roster(&roster, &own, operation, me, fetch_owner).await;
    }
    let known: Vec<BuildStepReceipt> = fetch_owner().await?.into_iter().filter_map(|f| if let BuildFact::Step(r) = f { Some(r) } else { None }).filter(|r| r.operation == operation || r.operation.starts_with(&format!("{operation}/"))).collect();
    let all: Vec<BuildStepReceipt> = own.iter().cloned().chain(known.iter().cloned()).collect();
    let admitted = admitted_of(&all, operation).ok_or_else(|| format!("{operation}: neither {executor}'s Build facts nor the owner's hold an {STEP_ACCEPTED_ROSTER} receipt: the leave was admitted for no birth, so no member is stopped"))??;
    if &admitted.final_primary != me {
        return Err(format!("{operation}: {executor} ({}) is not the final primary: the owner admitted {} after {} amendment(s), so no member is stopped", named(me), named(&admitted.final_primary), admitted.amendments.len()));
    }
    if admitted.amendments.is_empty() {
        let roster = accepted_roster(builds, key, STEP_LEAVING_ROSTER, executor, me, my_runtime, capture).await?;
        return Ok((roster, known));
    }
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-owner-admitted-roster", operation = %operation, me = %named(me), amendments = admitted.amendments.len(), roster = admitted.roster.len(), owner_receipts = known.len())
        .in_scope(|| tracing::info!("this admin holds no roster and an amendment admits it: it runs the owner's accepted roster plus every amendment and captures none"));
    Ok((admitted.roster, known))
}

/// The leaving mesh-primary's member workflows over the accepted `roster` (mesh-leave.md steps 3-4):
/// every ordinary node in parallel, then, only once every one has its exact terminal receipt, every
/// other node-admin in parallel; then the one exit manifest, recorded as the `OtherMembersExited`
/// step receipt of the accepted Build. A member whose exact Exited receipt the Build already holds
/// (a successful child of an earlier, incomplete run) keeps it and is not run again; a failed or
/// unrunnable member leaves its tier incomplete. `me` is the final primary (still running, its exact
/// runtime `final_runtime`); `records` are the view's Node records of the mesh, of any status;
/// `known` are step receipts read from the owner's Build facts at admission
/// ([`admission_roster`]), folded with this Build's own; `shutdown` is one member's drain/stop
/// workflow and the provider's terminal receipt of its exact runtime; `delete_row` is the
/// mesh-primary's write of a member's row `Deleted` on that member's exact Exited receipt (step 3:
/// "On that receipt the mesh-primary, the writer of its mesh's rows, writes the member's row
/// Deleted"), a keyed write that is repeated for a retained receipt, never inferred done from it.
#[allow(clippy::too_many_arguments)]
pub async fn members_exited<F, Fut, G, GFut>(builds: &dyn BuildStateAdapter, key: &LeaveKey, executor: &str, roster: &[RosterBirth], me: &RosterBirth, final_runtime: RuntimeProof, records: &[Node], known: &[BuildStepReceipt], shutdown: F, delete_row: G) -> Result<ExitManifest, String>
where
    F: Fn(Node) -> Fut,
    Fut: std::future::Future<Output = Result<TerminalReceipt, String>>,
    G: Fn(RosterBirth) -> GFut,
    GFut: std::future::Future<Output = Result<(), String>>,
{
    use tracing::Instrument;
    if !roster.contains(me) {
        return Err(format!("{executor} ({} {}) is not a birth of the roster accepted for {}: no member is stopped", me.node, me.incarnation_id, key.operation));
    }
    let build_id = BuildId(key.build_id.clone());
    let mut steps = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the receipts of {}: {e}", key.operation))?.steps;
    steps.extend(known.iter().cloned());
    let held = exit_receipts(&steps, &key.operation, roster);
    let mut receipts: Vec<TerminalReceipt> = Vec::new();
    let mut remaining: Vec<&RosterBirth> = Vec::new();
    let mut retained: Vec<&RosterBirth> = Vec::new();
    for b in roster.iter().filter(|b| *b != me) {
        match held.get(&(b.node_id.clone(), b.incarnation_id.clone())) {
            Some(t) => {
                receipts.push(t.clone());
                retained.push(b);
            }
            None => remaining.push(b),
        }
    }
    tracing::info_span!("rdm.node_admin.mesh.resolve.via-retained-receipts", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %key.operation, roster = roster.len(), retained = receipts.len(), remaining = remaining.len(), retained_births = %receipts.iter().map(|t| t.node.as_str()).collect::<Vec<_>>().join(","))
        .in_scope(|| tracing::info!("members with an exact Exited receipt keep it; only the rest run"));
    let is_admin = |b: &RosterBirth| b.node.parse::<crate::model::PathName>().is_ok_and(|p| p.kind == NodeKind::NodeAdmin);
    for (tier, admins) in [("ordinary-nodes", false), ("node-admins", true)] {
        let mut failed = Vec::new();
        let mut members: Vec<Node> = Vec::new();
        for b in remaining.iter().filter(|b| is_admin(b) == admins) {
            match records.iter().find(|n| n.node_id.to_string() == b.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.0 == b.incarnation_id)) {
                Some(n) => members.push(n.clone()),
                None => failed.push(format!("{} {}: this view holds no record of the accepted birth, so its workflow cannot run and it has no Exited receipt", b.node, b.incarnation_id)),
            }
        }
        // A retained member of this tier exited in an earlier run: its row is written Deleted again
        // (keyed and repeatable), never taken as written because its receipt is held.
        let kept: Vec<RosterBirth> = retained.iter().filter(|b| is_admin(b) == admins).map(|b| (*b).clone()).collect();
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-leave-tier", tier, members = members.len(), retained = kept.len(), mesh_id = %key.mesh_id, operation = %key.operation, attempt = key.attempt, outcome = tracing::field::Empty);
        let exit_then_row = |n: Node| {
            let (shutdown, delete_row) = (&shutdown, &delete_row);
            async move {
                let t = shutdown(n).await?;
                delete_row(RosterBirth { node: t.node.clone(), node_id: t.node_id.clone(), incarnation_id: t.incarnation_id.clone() }).await?;
                Ok::<TerminalReceipt, String>(t)
            }
        };
        let (outcomes, kept_rows) = futures_util::future::join(join_all(members.iter().cloned().map(exit_then_row)), join_all(kept.iter().cloned().map(&delete_row))).instrument(span.clone()).await;
        for (n, r) in members.iter().zip(outcomes) {
            match r {
                Ok(t) => receipts.push(t),
                Err(e) => failed.push(format!("{}: {e}", n.name)),
            }
        }
        for (b, r) in kept.iter().zip(kept_rows) {
            if let Err(e) = r {
                failed.push(format!("{} {} (Exited receipt retained): {e}", b.node, b.incarnation_id));
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
            build_id: BuildId(key.build_id.clone()),
            attempt: key.attempt,
            operation: key.operation.clone(),
            step: STEP_OTHER_MEMBERS_EXITED.into(),
            outcome: StepOutcome::Complete,
            output: serde_json::to_value(&manifest).ok(),
            executor: Some(executor.to_string()),
        })
        .await
        .map_err(|e| format!("the exit manifest could not be recorded: {e}"))?;
    tracing::info_span!("rdm.node_admin.mesh.update.via-exit-manifest-recorded", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %key.operation, receipts = manifest.receipts.len(), roster = manifest.roster.len())
        .in_scope(|| tracing::info!("every other member's exact exit is proven: the exit manifest is recorded"));
    Ok(manifest)
}

/// A roster birth's row `Deleted` on its exact Exited receipt (mesh-leave.md step 3 at the
/// mesh-primary; step 7 for the final primary at the fabric-primary): written by `rows`, the
/// writer of the mesh's rows, from the record `records` hold of the exact birth, else from the
/// record the writer holds of the node's row (a departed birth the view no longer names). A
/// refusal names the birth and the writer's reason; `executor` is the admin writing.
pub async fn birth_row_deleted(rows: Option<&crate::node_rows::NodeRows>, executor: &str, operation: &str, records: &[Node], b: &RosterBirth) -> Result<(), String> {
    let rows = rows.ok_or_else(|| format!("{executor} holds no writer of node rows, so {} {}'s row is not written Deleted", b.node, b.incarnation_id))?;
    let exact = records.iter().find(|n| n.node_id.as_str() == b.node_id && n.incarnation_id.as_ref().is_some_and(|i| i.0 == b.incarnation_id));
    let written = match exact {
        Some(record) => rows.deleted(record, operation).await,
        None => {
            let name: crate::model::PathName = b.node.parse().map_err(|e| format!("{executor}: the roster birth's path.name {} does not parse: {e}", b.node))?;
            let node_id = NodeId::parse(&b.node_id).map_err(|e| format!("{executor}: the roster birth {}'s node id {} does not parse: {e}", b.node, b.node_id))?;
            rows.deleted_birth(&name, &node_id, operation).await
        }
    };
    written.map_err(|e| format!("{} {}'s row is not written Deleted: {e}", b.node, b.incarnation_id))
}

/// mesh-left at the fabric-primary (mesh-leave.md step 7), after exact coverage and before the
/// `MeshLeft` receipt: "At mesh-left the fabric-primary writes the final primary's row Deleted, as
/// acting primary of a mesh that has none left, and the mesh row Dead". `rows` is the
/// fabric-primary's writer, `records` its view's records of the mesh, `final_primary` the exact
/// final birth; the Dead row is committed through `status` and held in `declared`.
#[allow(clippy::too_many_arguments)]
pub async fn mesh_left_rows(rows: Option<&crate::node_rows::NodeRows>, status: &dyn crate::status_storage::StatusStorage, declared: &std::sync::Mutex<crate::status_rpc::Declared>, executor: &str, key: &LeaveKey, mesh: &str, records: &[Node], final_primary: &RosterBirth) -> Result<(), String> {
    birth_row_deleted(rows, executor, &key.operation, records, final_primary).await?;
    let committed = crate::status_rpc::commit_mesh_state(status, declared, &key.mesh_id, rafka_node_rpc_contract::status::MeshState::Dead).await;
    tracing::info_span!("rdm.node_admin.mesh.update.via-dead-row", node = %executor, mesh = %mesh, mesh_id = %key.mesh_id, build_id = %key.build_id, attempt = key.attempt, operation = %key.operation, outcome = committed.name())
        .in_scope(|| tracing::info!("mesh-left: the mesh row Dead is stored and applied before the MeshLeft receipt; the mesh's name is released"));
    match committed {
        StatusReply::Applied | StatusReply::AlreadyApplied => Ok(()),
        refused => Err(format!("{}: the mesh row Dead of {mesh} ({}) is not committed: {} ({refused:?})", key.operation, key.mesh_id, refused.name())),
    }
}

/// What the fabric-primary decides of a `MeshLeave` handoff from the Build facts it read from the
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
            HandoffVerdict::Unmatched { field, expected, reported } => Some(StatusReply::RejectedUnmatchedCompletion { field: field.clone(), expected: expected.clone(), reported: reported.clone() }),
            HandoffVerdict::Incomplete { reason, .. } => Some(StatusReply::NotReady { reason: reason.clone() }),
        }
    }
}

/// The verdict on the handoff of the leave `key`, admitted with `admitted`, over `facts` (the
/// sender's Build facts, read now); `receipt_manifest` is the reference the call carried.
pub fn handoff_verdict(facts: &[BuildFact], key: &LeaveKey, admitted: &Admitted, receipt_manifest: &str) -> HandoffVerdict {
    let unmatched = |field: &str, expected: String, reported: String| HandoffVerdict::Unmatched { field: field.into(), expected, reported };
    let (operation, attempt) = (key.operation.as_str(), key.attempt);
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
        tracing::info_span!("rdm.node_admin.mesh.reject.via-incomplete-handoff", mesh_id = %key.mesh_id, build_id = %key.build_id, attempt, operation = %operation, roster = admitted.roster.len(), exited = held.len(), without_exit = %named)
            .in_scope(|| tracing::info!("the leaving primary's member workflows are incomplete: the handoff is refused not-ready and the open leave ends"));
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

/// The key of one open mesh leave at the fabric-primary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeaveKey {
    /// The mesh that leaves.
    pub mesh_id: MeshId,
    /// The Build the operation belongs to.
    pub build_id: String,
    /// The Build attempt that holds it.
    pub attempt: u32,
    /// `shutdown-mesh:<mesh_id>`.
    pub operation: String,
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
pub type LeaveOutcome = Result<ExitManifest, String>;

struct OpenLeave {
    admitted: Admitted,
    handoff: watch::Sender<Option<LeaveOutcome>>,
}

/// The mesh leaves this admin, as the fabric-primary, has admitted and awaits a `MeshLeave` for.
#[derive(Default)]
pub struct LeaveBook {
    open: Mutex<HashMap<LeaveKey, OpenLeave>>,
}

impl LeaveBook {
    /// Open `key` before `LeaveMesh` is sent, holding what admission captured; returns what
    /// resolves when a validated handoff arrives. Opening again keeps the first capture.
    pub fn open(&self, key: LeaveKey, admitted: Admitted) -> watch::Receiver<Option<LeaveOutcome>> {
        self.open.lock().unwrap().entry(key).or_insert_with(|| OpenLeave { admitted, handoff: watch::channel(None).0 }).handoff.subscribe()
    }

    /// What admission captured for `key`, when it is open.
    pub fn admitted(&self, key: &LeaveKey) -> Option<Admitted> {
        self.open.lock().unwrap().get(key).map(|o| o.admitted.clone())
    }

    /// The open leaves of `mesh_id`: `(build_id, attempt, operation)` of each.
    pub fn open_for(&self, mesh_id: &MeshId) -> Vec<(String, u32, String)> {
        self.open.lock().unwrap().keys().filter(|k| &k.mesh_id == mesh_id).map(|k| (k.build_id.clone(), k.attempt, k.operation.clone())).collect()
    }

    /// A validated handoff for `key`: whether it was the first outcome of the leave.
    pub fn resolve(&self, key: &LeaveKey, manifest: ExitManifest) -> bool {
        self.end(key, Ok(manifest))
    }

    /// The leaving primary reported its member workflows incomplete for `key`: the leave ends
    /// with `reason` at once (the owner's attempt ends inside the call, never on a wait); whether it
    /// was the first outcome of the leave.
    pub fn incomplete(&self, key: &LeaveKey, reason: String) -> bool {
        self.end(key, Err(reason))
    }

    fn end(&self, key: &LeaveKey, outcome: LeaveOutcome) -> bool {
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
    pub fn close(&self, key: &LeaveKey) {
        self.open.lock().unwrap().remove(key);
    }
}

/// One mesh leave at the leaving mesh's primary.
#[derive(Debug, Clone)]
enum LeaveRun {
    /// The member workflows are running for the owner that sent `leave-mesh` last.
    Running(NodeId),
    /// Every other member exited and the manifest is recorded: the handoff to the owner is what
    /// is left.
    Exited(NodeId, ExitManifest),
}

/// What a `leave-mesh` admission does for the run of its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaveAdmission {
    /// No run is held: the member workflows start for this sender.
    Start,
    /// The member workflows are running: their handoff goes to this sender.
    Running,
    /// The manifest is recorded: the handoff is what repeats, to this sender.
    Resend(ExitManifest),
}

/// The mesh leaves this admin, as a leaving mesh's primary, is running or has finished. The owner
/// a handoff goes to is the admin that sent `leave-mesh` last for the key (a successor
/// fabric-primary re-dispatching the same attempt takes the handoff over).
#[derive(Default)]
pub struct LeaveRuns(Mutex<HashMap<LeaveKey, LeaveRun>>);

impl LeaveRuns {
    /// `leave-mesh` for `key` from `sender`.
    pub fn admit(&self, key: &LeaveKey, sender: &NodeId) -> LeaveAdmission {
        let mut runs = self.0.lock().unwrap();
        match runs.get(key).cloned() {
            Some(LeaveRun::Running(_)) => {
                runs.insert(key.clone(), LeaveRun::Running(sender.clone()));
                LeaveAdmission::Running
            }
            Some(LeaveRun::Exited(_, manifest)) => {
                runs.insert(key.clone(), LeaveRun::Exited(sender.clone(), manifest.clone()));
                LeaveAdmission::Resend(manifest)
            }
            None => {
                runs.insert(key.clone(), LeaveRun::Running(sender.clone()));
                LeaveAdmission::Start
            }
        }
    }

    /// The run of `key` recorded its manifest: the owner its handoff goes to now.
    pub fn exited(&self, key: &LeaveKey, manifest: &ExitManifest) -> Option<NodeId> {
        let mut runs = self.0.lock().unwrap();
        let owner = match runs.get(key) {
            Some(LeaveRun::Running(o) | LeaveRun::Exited(o, _)) => o.clone(),
            None => return None,
        };
        runs.insert(key.clone(), LeaveRun::Exited(owner.clone(), manifest.clone()));
        Some(owner)
    }

    /// The run of `key` ended incomplete: it is forgotten (its receipts stand) and the owner it
    /// ran for is returned.
    pub fn incomplete(&self, key: &LeaveKey) -> Option<NodeId> {
        match self.0.lock().unwrap().remove(key) {
            Some(LeaveRun::Running(o) | LeaveRun::Exited(o, _)) => Some(o),
            None => None,
        }
    }
}

/// What serves `LeaveMesh` (at the leaving mesh's primary) and `MeshLeave` (at the
/// fabric-primary); the status door hands each to it.
#[async_trait::async_trait]
pub trait MeshLeaver: Send + Sync {
    /// `LeaveMesh` from `sender`: admit and run the member workflows.
    async fn leave_mesh(&self, sender: Option<&Node>, req: &StatusRequest) -> StatusReply;
    /// `MeshLeave` from `sender`: validate the handoff.
    async fn mesh_leave(&self, sender: Option<&Node>, req: &StatusRequest) -> StatusReply;
}

/// The door a running admin fills once its runner exists.
pub type LeaverSlot = Arc<OnceLock<Arc<dyn MeshLeaver>>>;

#[cfg(test)]
mod tests {
    use super::*;

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
        }
    }

    /// CONTRACT: the handoff of a running leave goes to the admin that sent `leave-mesh` last: a
    /// successor fabric-primary re-dispatching the same attempt while the member workflows run
    /// takes the handoff over, and a repeated `leave-mesh` after the manifest is recorded resends
    /// it to that sender.
    #[test]
    fn a_leave_mesh_from_a_successor_owner_takes_over_the_running_leaves_handoff() {
        let runs = LeaveRuns::default();
        let key = LeaveKey { mesh_id: MeshId::mint(), build_id: "b".into(), attempt: 1, operation: "shutdown-mesh:m".into() };
        let (first, successor) = (NodeId::mint(), NodeId::mint());
        assert_eq!(runs.admit(&key, &first), LeaveAdmission::Start);
        assert_eq!(runs.admit(&key, &successor), LeaveAdmission::Running);
        let last = birth("mesh2.admin.1", "i3");
        let manifest = ExitManifest { mesh_id: "m".into(), roster: vec![last.clone()], final_primary: last, final_runtime: None, receipts: vec![] };
        assert_eq!(runs.exited(&key, &manifest), Some(successor.clone()), "the handoff goes to the owner that sent leave-mesh last");
        let third = NodeId::mint();
        assert_eq!(runs.admit(&key, &third), LeaveAdmission::Resend(manifest));
        assert_eq!(runs.incomplete(&key), Some(third), "a resend is owned by its sender");
    }

    /// CONTRACT: an open leave the leaving primary reports incomplete ends at once with the named
    /// reason (the owner's wait resolves now, never on a deadline), and a later handoff for the same
    /// leave does not overwrite that outcome.
    #[test]
    fn an_incomplete_report_ends_the_open_leave_at_once_with_its_reason() {
        let book = LeaveBook::default();
        let key = LeaveKey { mesh_id: MeshId::mint(), build_id: "b".into(), attempt: 1, operation: "shutdown-mesh:m".into() };
        let last = birth("mesh2.admin.1", "i3");
        let runtime = receipt(&last).runtime.unwrap();
        let rx = book.open(key.clone(), Admitted { roster: vec![last.clone()], final_primary: last.clone(), final_runtime: runtime, amendments: Vec::new() });
        assert!(rx.borrow().is_none());
        assert!(book.incomplete(&key, "roster births without an exact Exited receipt: mesh2.rpc.1 i1".into()));
        assert_eq!(rx.borrow().clone(), Some(Err("roster births without an exact Exited receipt: mesh2.rpc.1 i1".into())), "the wait resolves now with the reason");
        let manifest = ExitManifest { mesh_id: "m".into(), roster: vec![last.clone()], final_primary: last, final_runtime: None, receipts: vec![] };
        assert!(!book.resolve(&key, manifest), "the first outcome stands");
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
}
