//! Semantic wedge detectors (i143.e8.s4, PRD §12 "wedge cuts").
//!
//! A wedge is judged by the consequences it must leave, never by the injector's success and never
//! by a duration. The detector takes the typed [`Evidence`] a scenario collected around one cut and
//! answers [`Verdict`] or a [`Refusal`] naming every missing consequence:
//!
//! ```text
//! primitive   the injector said it armed the fault           (never sufficient alone)
//! fault       the fault is ACTIVE: held, naming the exact call, still held at the last read
//! progress    stalled progress: two or more reads taken while the fault was active carry one
//!             marker, and the work they measure is not complete
//! routing     the typed routing effect the cut's position implies (routable or not)
//! control     the public control effect: seats equal what the public candidates compute,
//!             authority did not move, and a running silent runtime was neither replaced,
//!             re-attempted nor re-born
//! release     the injection was released and the hold ended
//! recovery    the work the cut held completed, and its progress marker moved past the stall
//! reconciled  every final-state check holds (receipts once, one runtime, the pointer, the ledger)
//! dispatches  no call reached a birth after that birth was superseded
//! ```
//!
//! The detector holds no clock. How long a scenario observed is the scenario's affair; what the
//! observation showed is the detector's.
//!
//! Each judgment is one span: `rdm.scenario.wedge.resolve.via-detector` when every consequence
//! holds, `rdm.scenario.wedge.reject.via-<invariant>` for each one that does not.

use crate::ledger::Ledger;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fmt;

/// The injector reported success and nothing else was observed.
pub const PRIMITIVE_SUCCESS_ALONE: &str = "wedge_detector_rejects_primitive_success_alone";
/// No acknowledgement that the fault is active (never held, no call named, or no longer held).
pub const NO_ACTIVE_FAULT_ACK: &str = "wedge_detector_requires_active_fault_ack";
/// The fault was reported active and the work it should hold progressed (or finished) anyway.
pub const NO_PROGRESS_CONSEQUENCE: &str = "wedge_detector_requires_stalled_progress";
/// The typed routing effect is missing or is not the one the cut's position implies.
pub const NO_ROUTING_CONSEQUENCE: &str = "wedge_detector_requires_routing_consequence";
/// The public control effect is missing, or the seats diverge from what the public candidates compute.
pub const NO_CONTROL_CONSEQUENCE: &str = "wedge_detector_requires_control_consequence";
/// A runtime that was alive and silent was replaced, re-attempted or re-born: silence is not death.
pub const SILENT_RUNTIME_JUDGED_DEAD: &str = "wedge_detector_never_judges_a_running_silent_runtime_dead";
/// The injection was never released, or the hold did not end.
pub const NO_RELEASE: &str = "wedge_detector_requires_release";
/// The held work did not complete after the release.
pub const NO_RECOVERY: &str = "wedge_detector_requires_recovery";
/// The final state was not reconciled, or a reconciliation check failed.
pub const NO_RECONCILIATION: &str = "wedge_detector_requires_stateful_reconciliation";
/// A call was dispatched to a birth after that birth had been superseded.
pub const SUPERSEDED_BIRTH_DISPATCHED: &str = "wedge_detector_forbids_superseded_birth_dispatch";

/// Every invariant the detector names.
pub const INVARIANTS: [&str; 10] = [
    PRIMITIVE_SUCCESS_ALONE,
    NO_ACTIVE_FAULT_ACK,
    NO_PROGRESS_CONSEQUENCE,
    NO_ROUTING_CONSEQUENCE,
    NO_CONTROL_CONSEQUENCE,
    SILENT_RUNTIME_JUDGED_DEAD,
    NO_RELEASE,
    NO_RECOVERY,
    NO_RECONCILIATION,
    SUPERSEDED_BIRTH_DISPATCHED,
];

/// The wedge families of PRD §12, plus the two the acceptance text names (a runtime alive without
/// progress, a fabric-primary stall).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum Family {
    RequestPreSend,
    RequestPostApply,
    BuildStep,
    DeploymentStep,
    AcceptedBuildPersistence,
    FabricPointerCatchUp,
    BuildInProgressRefusal,
    RuntimeFactProduction,
    RuntimeFactPublication,
    RuntimeFactAdoption,
    ProviderDomain,
    RuntimeMetadataHydration,
    LifecycleHook,
    PendingBeforeBuild,
    ElectionAuthorityMovement,
    StaleSlotRace,
    PartitionHeal,
    ExactRuntimeExit,
    FabricShutdown,
    RecoveryHandoff,
    SilentRuntime,
    FabricPrimaryStall,
}

impl Family {
    pub const ALL: [Family; 22] = [
        Family::RequestPreSend,
        Family::RequestPostApply,
        Family::BuildStep,
        Family::DeploymentStep,
        Family::AcceptedBuildPersistence,
        Family::FabricPointerCatchUp,
        Family::BuildInProgressRefusal,
        Family::RuntimeFactProduction,
        Family::RuntimeFactPublication,
        Family::RuntimeFactAdoption,
        Family::ProviderDomain,
        Family::RuntimeMetadataHydration,
        Family::LifecycleHook,
        Family::PendingBeforeBuild,
        Family::ElectionAuthorityMovement,
        Family::StaleSlotRace,
        Family::PartitionHeal,
        Family::ExactRuntimeExit,
        Family::FabricShutdown,
        Family::RecoveryHandoff,
        Family::SilentRuntime,
        Family::FabricPrimaryStall,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Family::RequestPreSend => "request-pre-send",
            Family::RequestPostApply => "request-post-apply",
            Family::BuildStep => "build-step",
            Family::DeploymentStep => "deployment-step",
            Family::AcceptedBuildPersistence => "accepted-build-persistence",
            Family::FabricPointerCatchUp => "fabric-pointer-catch-up",
            Family::BuildInProgressRefusal => "build-in-progress-refusal",
            Family::RuntimeFactProduction => "runtime-fact-production",
            Family::RuntimeFactPublication => "runtime-fact-publication",
            Family::RuntimeFactAdoption => "runtime-fact-adoption",
            Family::ProviderDomain => "provider-domain",
            Family::RuntimeMetadataHydration => "runtime-metadata-hydration",
            Family::LifecycleHook => "lifecycle-hook",
            Family::PendingBeforeBuild => "pending-before-build",
            Family::ElectionAuthorityMovement => "election-authority-movement",
            Family::StaleSlotRace => "stale-slot-race",
            Family::PartitionHeal => "partition-heal",
            Family::ExactRuntimeExit => "exact-runtime-exit",
            Family::FabricShutdown => "fabric-shutdown",
            Family::RecoveryHandoff => "recovery-handoff",
            Family::SilentRuntime => "silent-runtime",
            Family::FabricPrimaryStall => "fabric-primary-stall",
        }
    }
}

/// What the injector itself said. On its own it proves nothing.
#[derive(Debug, Clone, Serialize)]
pub struct Primitive {
    pub armed: bool,
    pub ack: Value,
}

/// The acknowledgement that the fault is active.
#[derive(Debug, Clone, Serialize)]
pub struct FaultAck {
    /// The injector reports the fault holding something now.
    pub held: bool,
    /// The exact call, process or step it holds, as the injector named it.
    pub names: Option<Value>,
    /// The fault was still held at the last progress read.
    pub held_at_last_read: bool,
}

/// Stalled progress: the markers of reads taken while the fault was active.
#[derive(Debug, Clone, Serialize)]
pub struct Progress {
    pub reads: Vec<String>,
    /// The work these reads measure finished while the fault was active.
    pub complete_while_held: bool,
}

/// The typed routing effect.
#[derive(Debug, Clone, Serialize)]
pub struct Routing {
    /// What the cut's position implies.
    pub expected_routable: bool,
    /// What the public view showed during the hold.
    pub observed_routable: bool,
    pub observed: String,
}

/// The public control effect around the hold.
#[derive(Debug, Clone, Serialize)]
pub struct Control {
    /// The seats the public view advertises equal the ones the public candidates compute
    /// (`elections::seats_as_expected`), before, during and after.
    pub seats_as_expected: [bool; 3],
    /// The seats' detail when one diverges.
    pub seats_detail: String,
    pub fabric_primary: [String; 3],
    /// The cut is a membership change (a join, a mesh birth): the fabric primary may legitimately
    /// move, and only the public candidates' computation judges it. For a stall of a live admin it
    /// may not move at all.
    pub authority_may_move: bool,
    pub incarnation: [String; 3],
    /// The Build attempt (or the runtime's birth count) the work held, before and during.
    pub attempts: [u64; 2],
    /// The exact runtime was alive, as the provider holds it, during the hold.
    pub exact_runtime_alive: bool,
    /// The hold ended with the work replaced: a new attempt, a new incarnation or a retire.
    pub replaced_during: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Release {
    pub acked: bool,
    pub hold_ended: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Recovery {
    /// The held work completed.
    pub work_complete: bool,
    /// The marker after the release, to be compared with the held reads.
    pub marker_after: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Check {
    pub name: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Reconciliation {
    pub checks: Vec<Check>,
}

impl Reconciliation {
    pub fn check(&mut self, name: impl Into<String>, ok: bool, detail: impl Into<String>) {
        self.checks.push(Check { name: name.into(), ok, detail: detail.into() });
    }

    /// The RPC operation ledger's own state algebra against the final state, as one check: its
    /// refusal, whole, is the detail.
    pub fn ledger(&mut self, ledger: &Ledger, final_state: &BTreeMap<String, Vec<u8>>) {
        match ledger.reconcile_state(final_state) {
            Ok(r) => self.check("rpc-ledger", true, format!("{} operations reconcile with the final state", r.issued)),
            Err(refusal) => self.check("rpc-ledger", false, refusal.to_string()),
        }
    }
}

/// One call that reached a birth.
#[derive(Debug, Clone, Serialize)]
pub struct Dispatch {
    pub birth: String,
    pub current_birth: String,
    /// The birth had been superseded when the call reached it.
    pub after_supersession: bool,
}

/// Everything a scenario collected around one cut.
#[derive(Debug, Clone, Serialize)]
pub struct Evidence {
    pub family: Family,
    pub cut: String,
    pub primitive: Option<Primitive>,
    pub fault: Option<FaultAck>,
    pub progress: Option<Progress>,
    pub routing: Option<Routing>,
    pub control: Option<Control>,
    pub release: Option<Release>,
    pub recovery: Option<Recovery>,
    pub reconciliation: Option<Reconciliation>,
    pub dispatches: Vec<Dispatch>,
}

impl Evidence {
    pub fn new(family: Family, cut: impl Into<String>) -> Self {
        Self {
            family,
            cut: cut.into(),
            primitive: None,
            fault: None,
            progress: None,
            routing: None,
            control: None,
            release: None,
            recovery: None,
            reconciliation: None,
            dispatches: Vec::new(),
        }
    }
}

/// One refused consequence: the invariant it fails and what was seen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Rejection {
    pub invariant: &'static str,
    pub detail: String,
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.invariant, self.detail)
    }
}

/// Every consequence the evidence lacks, never just the first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Refusal {
    pub family: &'static str,
    pub cut: String,
    pub rejections: Vec<Rejection>,
}

impl Refusal {
    pub fn invariants(&self) -> Vec<&'static str> {
        self.rejections.iter().map(|r| r.invariant).collect()
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "wedge `{}` ({}) refused:", self.cut, self.family)?;
        for r in &self.rejections {
            write!(f, "\n  {r}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Refusal {}

/// A wedge that proved: the consequences that were checked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Verdict {
    pub family: &'static str,
    pub cut: String,
    pub proved: Vec<&'static str>,
}

/// The one span a rejection emits. The names are literals: a span name is static.
fn reject_span(invariant: &'static str, family: &str, cut: &str, detail: &str) {
    macro_rules! s {
        ($name:literal) => {
            tracing::info_span!($name, invariant, family, cut, detail).in_scope(|| {})
        };
    }
    match invariant {
        PRIMITIVE_SUCCESS_ALONE => s!("rdm.scenario.wedge.reject.via-primitive-success-alone"),
        NO_ACTIVE_FAULT_ACK => s!("rdm.scenario.wedge.reject.via-missing-fault-ack"),
        NO_PROGRESS_CONSEQUENCE => s!("rdm.scenario.wedge.reject.via-missing-progress-consequence"),
        NO_ROUTING_CONSEQUENCE => s!("rdm.scenario.wedge.reject.via-missing-routing-consequence"),
        NO_CONTROL_CONSEQUENCE => s!("rdm.scenario.wedge.reject.via-missing-control-consequence"),
        SILENT_RUNTIME_JUDGED_DEAD => s!("rdm.scenario.wedge.reject.via-silent-runtime-judged-dead"),
        NO_RELEASE => s!("rdm.scenario.wedge.reject.via-missing-release"),
        NO_RECOVERY => s!("rdm.scenario.wedge.reject.via-missing-recovery"),
        NO_RECONCILIATION => s!("rdm.scenario.wedge.reject.via-missing-reconciliation"),
        SUPERSEDED_BIRTH_DISPATCHED => s!("rdm.scenario.wedge.reject.via-superseded-birth-dispatch"),
        other => unreachable!("an invariant the detector does not name: {other}"),
    }
}

fn rej(invariant: &'static str, detail: impl Into<String>) -> Rejection {
    Rejection { invariant, detail: detail.into() }
}

/// Judge one cut. Pure over the evidence; emits one span per rejection, or the resolve span.
pub fn judge(e: &Evidence) -> Result<Verdict, Refusal> {
    let mut out: Vec<Rejection> = Vec::new();

    // The injector's success, with nothing observed, is the one thing refused as itself.
    let observed_anything = e.fault.is_some()
        || e.progress.is_some()
        || e.routing.is_some()
        || e.control.is_some()
        || e.release.is_some()
        || e.recovery.is_some()
        || e.reconciliation.is_some()
        || !e.dispatches.is_empty();
    if !observed_anything {
        match &e.primitive {
            Some(p) if p.armed => out.push(rej(PRIMITIVE_SUCCESS_ALONE, format!("the injector acknowledged arming ({}) and nothing about the fault, its consequences, its release or the final state was observed", p.ack))),
            _ => out.push(rej(NO_ACTIVE_FAULT_ACK, "nothing was armed and nothing was observed")),
        }
        return finish(e, out, vec![]);
    }

    let mut proved: Vec<&'static str> = Vec::new();

    match &e.fault {
        None => out.push(rej(NO_ACTIVE_FAULT_ACK, "no acknowledgement that the fault is active was recorded")),
        Some(f) if !f.held => out.push(rej(NO_ACTIVE_FAULT_ACK, "the injector never reported the fault holding anything")),
        Some(f) if f.names.is_none() => out.push(rej(NO_ACTIVE_FAULT_ACK, "the injector reported held without naming the call, step or process it holds")),
        Some(f) if !f.held_at_last_read => out.push(rej(NO_ACTIVE_FAULT_ACK, "the fault was no longer held at the last progress read: the reads do not measure a stall")),
        Some(_) => proved.push("fault-active"),
    }

    match &e.progress {
        None => out.push(rej(NO_PROGRESS_CONSEQUENCE, "no progress was read while the fault was active")),
        Some(p) if p.reads.len() < 2 => out.push(rej(NO_PROGRESS_CONSEQUENCE, format!("{} progress read(s) while held: a stall needs at least two", p.reads.len()))),
        Some(p) if p.complete_while_held => out.push(rej(NO_PROGRESS_CONSEQUENCE, "the work the fault should hold completed while the fault was reported active")),
        Some(p) => match p.reads.windows(2).find(|w| w[0] != w[1]) {
            Some(w) => out.push(rej(NO_PROGRESS_CONSEQUENCE, format!("progress moved while the fault was reported active: `{}` then `{}`", w[0], w[1]))),
            None => proved.push("progress-stalled"),
        },
    }

    match &e.routing {
        None => out.push(rej(NO_ROUTING_CONSEQUENCE, "no routing effect was observed")),
        Some(r) if r.expected_routable != r.observed_routable => out.push(rej(
            NO_ROUTING_CONSEQUENCE,
            format!("the cut's position implies routable={}; the public view showed routable={} ({})", r.expected_routable, r.observed_routable, r.observed),
        )),
        Some(_) => proved.push("routing"),
    }

    match &e.control {
        None => out.push(rej(NO_CONTROL_CONSEQUENCE, "no control or election effect was observed")),
        Some(c) => {
            let before = out.len();
            let phases = ["before", "during", "after"];
            for (i, ok) in c.seats_as_expected.iter().enumerate() {
                if !ok {
                    out.push(rej(NO_CONTROL_CONSEQUENCE, format!("{} the hold the advertised seats diverge from the ones the public candidates compute: {}", phases[i], c.seats_detail)));
                }
            }
            if !c.authority_may_move && (c.fabric_primary[0] != c.fabric_primary[1] || c.fabric_primary[1] != c.fabric_primary[2]) {
                out.push(rej(NO_CONTROL_CONSEQUENCE, format!("the fabric primary moved across a stall of a live admin: {:?}", c.fabric_primary)));
            }
            if out.len() == before {
                proved.push("control");
            }
            let reborn = c.incarnation[0] != c.incarnation[1] || c.incarnation[1] != c.incarnation[2];
            if c.exact_runtime_alive && (c.replaced_during || reborn || c.attempts[0] != c.attempts[1]) {
                out.push(rej(
                    SILENT_RUNTIME_JUDGED_DEAD,
                    format!(
                        "the exact runtime was alive and silent, and the control plane replaced it: replaced={}, incarnations {:?}, attempts {:?}",
                        c.replaced_during, c.incarnation, c.attempts
                    ),
                ));
            } else {
                proved.push("silent-runtime-not-dead");
            }
        }
    }

    match &e.release {
        None => out.push(rej(NO_RELEASE, "the fault was never released")),
        Some(r) if !r.acked => out.push(rej(NO_RELEASE, "the injector never acknowledged the release")),
        Some(r) if !r.hold_ended => out.push(rej(NO_RELEASE, "the release was acknowledged and the hold did not end")),
        Some(_) => proved.push("released"),
    }

    match (&e.recovery, &e.progress) {
        (None, _) => out.push(rej(NO_RECOVERY, "the held work was not observed after the release")),
        (Some(r), _) if !r.work_complete => out.push(rej(NO_RECOVERY, format!("the held work did not complete after the release (marker `{}`)", r.marker_after))),
        (Some(r), Some(p)) if p.reads.first().is_some_and(|held| *held == r.marker_after) => {
            out.push(rej(NO_RECOVERY, format!("the progress marker after the release equals the held one (`{}`): the release changed nothing", r.marker_after)))
        }
        (Some(_), _) => proved.push("recovered"),
    }

    match &e.reconciliation {
        None => out.push(rej(NO_RECONCILIATION, "the final state was not reconciled")),
        Some(r) if r.checks.is_empty() => out.push(rej(NO_RECONCILIATION, "the reconciliation holds no check")),
        Some(r) => {
            let failed: Vec<String> = r.checks.iter().filter(|c| !c.ok).map(|c| format!("{} ({})", c.name, c.detail)).collect();
            if failed.is_empty() {
                proved.push("reconciled");
            } else {
                out.push(rej(NO_RECONCILIATION, format!("failed: {}", failed.join("; "))));
            }
        }
    }

    let stale: Vec<String> = e.dispatches.iter().filter(|d| d.after_supersession && d.birth != d.current_birth).map(|d| format!("{} (current {})", d.birth, d.current_birth)).collect();
    if stale.is_empty() {
        proved.push("no-superseded-dispatch");
    } else {
        out.push(rej(SUPERSEDED_BIRTH_DISPATCHED, format!("dispatched to a superseded birth after its supersession: {}", stale.join(", "))));
    }

    finish(e, out, proved)
}

fn finish(e: &Evidence, out: Vec<Rejection>, proved: Vec<&'static str>) -> Result<Verdict, Refusal> {
    let family = e.family.name();
    if out.is_empty() {
        tracing::info_span!("rdm.scenario.wedge.resolve.via-detector", family, cut = %e.cut, proved = %proved.join(",")).in_scope(|| {});
        return Ok(Verdict { family, cut: e.cut.clone(), proved });
    }
    for r in &out {
        reject_span(r.invariant, family, &e.cut, &r.detail);
    }
    Err(Refusal { family, cut: e.cut.clone(), rejections: out })
}

/// A progress marker for the Build a cut holds: state, attempt and the steps its operation holds.
pub fn build_marker(state: &str, attempt: u64, done_steps: &[String]) -> String {
    json!({"state": state, "attempt": attempt, "done": done_steps}).to_string()
}
