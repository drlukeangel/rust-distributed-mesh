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
use crate::build_state::{BuildStateAdapter, BuildStepReceipt, StepOutcome};
use crate::deployment::pipeline::{RuntimeProof, TerminalReceipt};
use crate::model::{Node, NodeKind};
use futures_util::future::join_all;
use rafka_mesh_entity::MeshId;
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

/// The leaving mesh-primary's member workflows over the accepted `roster` (mesh-leave.md steps 3-4):
/// every ordinary node in parallel, then, only once every one has its exact terminal receipt, every
/// other node-admin in parallel; then the one exit manifest, recorded as the `OtherMembersExited`
/// step receipt of the accepted Build. A member whose exact Exited receipt the Build already holds
/// (a successful child of an earlier, incomplete run) keeps it and is not run again; a failed or
/// unrunnable member leaves its tier incomplete. `me` is the final primary (still running, its exact
/// runtime `final_runtime`); `records` are the view's Node records of the mesh, of any status;
/// `shutdown` is one member's drain/stop workflow and the provider's terminal receipt of its exact
/// runtime.
#[allow(clippy::too_many_arguments)]
pub async fn members_exited<F, Fut>(builds: &dyn BuildStateAdapter, key: &LeaveKey, executor: &str, roster: &[RosterBirth], me: &RosterBirth, final_runtime: RuntimeProof, records: &[Node], shutdown: F) -> Result<ExitManifest, String>
where
    F: Fn(Node) -> Fut,
    Fut: std::future::Future<Output = Result<TerminalReceipt, String>>,
{
    use tracing::Instrument;
    if !roster.contains(me) {
        return Err(format!("{executor} ({} {}) is not a birth of the roster accepted for {}: no member is stopped", me.node, me.incarnation_id, key.operation));
    }
    let build_id = BuildId(key.build_id.clone());
    let steps = builds.read_build(&build_id).await.map_err(|e| format!("{executor}: the Build {build_id} could not be read for the receipts of {}: {e}", key.operation))?.steps;
    let held = exit_receipts(&steps, &key.operation, roster);
    let mut receipts: Vec<TerminalReceipt> = Vec::new();
    let mut remaining: Vec<&RosterBirth> = Vec::new();
    for b in roster.iter().filter(|b| *b != me) {
        match held.get(&(b.node_id.clone(), b.incarnation_id.clone())) {
            Some(t) => receipts.push(t.clone()),
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
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-leave-tier", tier, members = members.len(), mesh_id = %key.mesh_id, operation = %key.operation, attempt = key.attempt, outcome = tracing::field::Empty);
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
}

struct OpenLeave {
    admitted: Admitted,
    handoff: watch::Sender<Option<ExitManifest>>,
}

/// The mesh leaves this admin, as the fabric-primary, has admitted and awaits a `MeshLeave` for.
#[derive(Default)]
pub struct LeaveBook {
    open: Mutex<HashMap<LeaveKey, OpenLeave>>,
}

impl LeaveBook {
    /// Open `key` before `LeaveMesh` is sent, holding what admission captured; returns what
    /// resolves when a validated handoff arrives. Opening again keeps the first capture.
    pub fn open(&self, key: LeaveKey, admitted: Admitted) -> watch::Receiver<Option<ExitManifest>> {
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

    /// A validated handoff for `key`: whether it was the first.
    pub fn resolve(&self, key: &LeaveKey, manifest: ExitManifest) -> bool {
        match self.open.lock().unwrap().get(key) {
            Some(o) => o.handoff.send_replace(Some(manifest)).is_none(),
            None => false,
        }
    }

    /// Forget the leave: its receipts are the record.
    pub fn close(&self, key: &LeaveKey) {
        self.open.lock().unwrap().remove(key);
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
