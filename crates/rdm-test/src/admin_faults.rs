//! Testkit-only node-admin stalls: decorators over the traits a node-admin is built from, armed and
//! released through a door, so a scenario holds a real admin at one named cut of its documented
//! Build, deployment, lifecycle and hydration sequences and releases it (i143.e8.s2).
//!
//! The product node-admin carries no fault code. Its `Wiring` (`rdm_admin::wiring`)
//! takes the decorators here: a `FaultedBuilds` over the Build state, a `FaultedProvider` over
//! the deployment provider and a `FaultedEvents`
//! over the lifecycle events. Each decorator
//! asks the shared [`AdminFaults`] at its one cut; an armed, matching cut holds the caller there
//! until the door releases it. Nothing else changes: every other call passes straight through.
//!
//! A cut is one-shot: it holds the first matching call, records what held ([`AdminFaults::state`]),
//! and after its release it is spent. The door is the testkit Node RPC op `0x75`
//! ([`crate::fault_door`]): arm, release and state; the state is the active-injection
//! acknowledgement (`held: true` with the exact call it holds) and the observed consequence
//! counters. A cut that must be in force before the admin's Node RPC door is ready is armed from
//! `<root>/faults/<name>.boot.json` ([`arm_boot_cuts`]).

use crate::fault_door::{CutRow, FaultState, Hit};
use async_trait::async_trait;
use rdm_core::LifecycleOp;
use rdm_admin::build::BuildId;
use rdm_admin::certs::CertChoice;
use rdm_admin::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome,
    StepOutcome,
};
use rdm_admin::deployment::pipeline::LifecycleEvents;
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::ProviderKind;
use rdm_admin::wiring::{FabricHooks, Wiring};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Instrument;

/// Where one armed cut holds a call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum CutSpec {
    /// A deployment-pipeline or Build step: the step's work is done and its receipt is not yet
    /// durable. `operation` is a prefix of the receipt's operation key (`create-node`,
    /// `retire-node`); `node` is the operation's path.
    Receipt {
        /// The step name.
        step: String,
        /// A prefix of the receipt's operation key.
        #[serde(default)] operation: Option<String>,
        /// The operation's path.
        #[serde(default)] node: Option<String>,
        /// The receipt is a Failed one (the step's work ended Failed and its receipt is not yet
        /// durable); a Complete one when false.
        #[serde(default)] failed: bool,
    },
    /// A lifecycle event a deletion publishes (`deleting`, `restarting`, `deleted`): the step's
    /// receipt is durable and the event is not yet published. `stopped`: the stop stream's `stopped`
    /// reached the executor and is not yet recorded in the birth's declared-state row (i146 Q36).
    Event {
        /// The event name: `deleting`, `restarting`, `deleted` or `stopped`.
        event: String,
        /// The node the event concerns.
        #[serde(default)] node: Option<String>,
    },
    /// An accepted Build is about to become durable: its one write (`publish_accepted`) has been called
    /// and has not been made. The Build is its own entity, current from the moment it is durable
    /// (R-BI3), so until the release nothing holds it and nothing executes it.
    AcceptedBuild,
    /// An attempt's claim is durable on this admin's log and broadcast (`claim_attempt` decided
    /// `Won`) and the fabric-primary has not yet dispatched the attempt to its executor. `attempt`
    /// narrows the cut to one attempt number.
    ClaimedAttempt {
        /// The attempt, when the cut is for one.
        #[serde(default)] attempt: Option<u32>,
    },
    /// The deployment provider reports a control domain no held birth's runtime fact is in: the
    /// admin cannot adopt what it hears, so its Ready gate stays blocked until the release.
    ProviderDomain,
    /// The admin's leave withholds the listed announcements (1-based) on `channel` (`mesh`,
    /// `backbone` or `any`): the leave skips that one publish call, so the frame is never offered
    /// to the channel. It models a lost announcement at the send seam, not in a peer queue.
    LeaveAnnouncement {
        /// The announcements to withhold, 1-based.
        announcements: Vec<u32>,
        /// The channel: `mesh`, `backbone` or `any`.
        channel: String,
    },
    /// A joining admin has joined and is about to make the topology read (op `0x1E`) of node-start.md
    /// step 4, and has not made it: its start waits there, the node stays Pending and takes no traffic,
    /// until the release. The joiner arms it from `<root>/faults/<name>.boot.json` before its first line.
    TopologyRead,
    /// The admin withholds `announcement`, one of its final mesh topologies or leadership announcements
    /// (`rdm_transport::topology::Withhold`: `stopping-aggregate`, `primary-updated`,
    /// `aggregate-in-flight`), from `channel` (`mesh`, `backbone` or `any`) while the cut is armed:
    /// each such send is skipped, so the frame is never offered to the channel. It models the
    /// announcement lost on its way out (R-NS2: best-effort dissemination), not in a peer queue.
    Announcement {
        /// The announcement withheld.
        announcement: String,
        /// The channel: `mesh`, `backbone` or `any`.
        channel: String,
    },
    /// The deployment provider refuses every `terminate` of the runtime at `pid` while the cut is
    /// armed: the call is answered `DeployError::Terminate`, named as the testkit's, and nothing is
    /// signalled. It models a provider that refuses a terminate (node-recovery.md step 4, "a
    /// terminate the provider refuses"); nothing holds.
    ProviderTerminate {
        /// The exact runtime's pid; every runtime when absent.
        #[serde(default)] pid: Option<u32>,
    },
    /// A node's declaration (`DeclareNodeState`) reached this admin's status door and is not yet
    /// decided: held unanswered until the release, so its node still owes it (i146 Q39). `node`
    /// narrows the cut to one declaring node, `state` to one state (`Ready`, `Leaving`, ...).
    Declaration {
        /// The declaring node's `path.name`.
        #[serde(default)] node: Option<String>,
        /// The state declared, as `NodeState` names it.
        #[serde(default)] state: Option<String>,
    },
    /// The deployment provider's inspection of the runtimes at `pids` answers `Unknown` while the cut is
    /// armed: no exited verdict of them can be read, while every other runtime, a launch of this admin's
    /// included, is inspected as ever. It models a lost mesh whose runtimes no surviving admin can
    /// inspect (elections §5, i144 Q51), and nothing holds.
    ProviderUnproven {
        /// The runtimes' pids.
        pids: Vec<u32>,
    },
    /// The reconcile loop's pass on this admin (the drift check and the pick-up of active Builds,
    /// `rdm_admin::wiring::DriftSeam`) is about to run: held before the pass's turn and outside every
    /// lock, until the release, so a birth a failed Build left surplus is not removed meanwhile (i146
    /// Q39). `node` narrows the cut to one admin.
    DriftPass {
        /// The admin's `path.name`.
        #[serde(default)] node: Option<String>,
    },
    /// A storage change's move (storage.md §2 "Changing it") this admin, as `node`'s mesh-primary, is
    /// about to carry to `node` in its `mesh.unblock`: the first matching command is held until the
    /// release and carried then, the move not done now; every later one passes. It models one command
    /// delayed on its way to the node.
    StorageStep {
        /// The commanded node's `path.name`.
        #[serde(default)] node: Option<String>,
    },
    /// A storage change's copy and switch, about to run on this admin as the fabric-primary: held until
    /// the release, every mesh-primary having replied `blocked`.
    StorageSwitch,
}

/// What a decorator offers the cuts at the moment it is called.
#[derive(Debug, Clone)]
pub enum Probe {
    /// A Build step receipt about to become durable.
    Receipt {
        /// The step name.
        step: String,
        /// The receipt's operation key.
        operation: String,
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
        /// Whether the receipt is a Failed one.
        failed: bool,
    },
    /// A lifecycle event about to be published.
    Event {
        /// The event name.
        event: &'static str,
        /// The lifecycle operation the event names.
        op: LifecycleOp,
    },
    /// An attempt's claim that is durable and broadcast, about to be returned to the drive.
    Claimed {
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
        /// The executor the claim names.
        executor: String,
    },
    /// An accepted Build, durable, about to be returned to the acceptance.
    Accepted {
        /// The Build.
        build_id: String,
    },
    /// The provider reports a control domain no held birth is in.
    ProviderDomain,
    /// A leave announcement about to be published.
    LeaveAnnouncement {
        /// The announcing node.
        node: String,
        /// The announcement's 1-based number.
        announcement: u32,
        /// The channel it is published on.
        channel: &'static str,
    },
    /// A final mesh topology or leadership announcement about to be sent on a channel.
    Announcement {
        /// The announcing node.
        node: String,
        /// The announcement.
        announcement: &'static str,
        /// The channel it is sent on.
        channel: &'static str,
    },
    /// A joining admin's topology read, about to be made.
    TopologyRead {
        /// The joining admin.
        node: String,
        /// The primary the read goes to: the admin that answered the join.
        primary: String,
    },
    /// A provider `terminate` about to be made.
    ProviderTerminate {
        /// The runtime's pid, when the handle names one.
        pid: Option<u32>,
        /// The deployment the runtime belongs to.
        deployment_id: String,
    },
    /// A node's declaration about to be decided.
    Declaration {
        /// The declaring node.
        node: String,
        /// The state declared.
        state: String,
    },
    /// A provider `inspect` about to be made.
    ProviderInspect {
        /// The runtime's pid, when the handle names one.
        pid: Option<u32>,
    },
    /// A reconcile pass about to run.
    DriftPass {
        /// The admin whose pass it is.
        node: String,
    },
    /// A storage change's move about to be carried to its node.
    StorageStep {
        /// The commanded node.
        node: String,
        /// The change.
        change_id: String,
    },
    /// A storage change's switch about to run.
    StorageSwitch {
        /// The change.
        change_id: String,
    },
}

impl CutSpec {
    fn matches(&self, p: &Probe) -> bool {
        let node_is = |want: &Option<String>, got: &str| want.as_deref().is_none_or(|w| got == w || got.ends_with(&format!(":{w}")));
        match (self, p) {
            (CutSpec::Receipt { step, operation, node, failed }, Probe::Receipt { step: s, operation: o, failed: f, .. }) => {
                failed == f && step == s && operation.as_deref().is_none_or(|prefix| o.starts_with(prefix)) && node_is(node, o)
            }
            (CutSpec::Event { event, node }, Probe::Event { event: e, op }) => event == e && node_is(node, &op.name.to_string()),
            (CutSpec::AcceptedBuild, Probe::Accepted { .. }) => true,
            (CutSpec::ClaimedAttempt { attempt }, Probe::Claimed { attempt: a, .. }) => attempt.is_none_or(|w| w == *a),
            (CutSpec::ProviderDomain, Probe::ProviderDomain) => true,
            (CutSpec::LeaveAnnouncement { announcements, channel }, Probe::LeaveAnnouncement { announcement, channel: c, .. }) => announcements.contains(announcement) && (channel == "any" || channel == c),
            (CutSpec::TopologyRead, Probe::TopologyRead { .. }) => true,
            (CutSpec::Announcement { announcement, channel }, Probe::Announcement { announcement: a, channel: c, .. }) => announcement == a && (channel == "any" || channel == c),
            (CutSpec::ProviderTerminate { pid }, Probe::ProviderTerminate { pid: p, .. }) => pid.is_none_or(|w| *p == Some(w)),
            (CutSpec::Declaration { node, state }, Probe::Declaration { node: n, state: s }) => node_is(node, n) && state.as_deref().is_none_or(|w| w == s),
            (CutSpec::ProviderUnproven { pids }, Probe::ProviderInspect { pid }) => pid.is_some_and(|p| pids.contains(&p)),
            (CutSpec::DriftPass { node }, Probe::DriftPass { node: n }) => node_is(node, n),
            (CutSpec::StorageStep { node }, Probe::StorageStep { node: n, .. }) => node_is(node, n),
            (CutSpec::StorageSwitch, Probe::StorageSwitch { .. }) => true,
            _ => false,
        }
    }
}

impl Probe {
    /// What a cut that holds this call records of it.
    fn hit(&self) -> Hit {
        match self {
            Probe::Receipt { step, operation, build_id, attempt, .. } => Hit::Receipt { step: step.clone(), operation: operation.clone(), build_id: build_id.clone(), attempt: *attempt },
            Probe::Event { event, op } => Hit::Event { event: event.to_string(), operation: op.operation.clone(), build_id: op.build_id.clone(), attempt: op.attempt, node: op.name.to_string(), incarnation_id: op.incarnation.as_str().to_string() },
            Probe::Accepted { build_id } => Hit::Accepted { build_id: build_id.clone() },
            Probe::Claimed { build_id, attempt, executor } => Hit::Claimed { build_id: build_id.clone(), attempt: *attempt, executor: executor.clone() },
            Probe::ProviderDomain => Hit::ProviderDomain,
            Probe::LeaveAnnouncement { node, announcement, channel } => Hit::LeaveAnnouncement { node: node.clone(), announcement: *announcement, channel: channel.to_string() },
            Probe::TopologyRead { node, primary } => Hit::TopologyRead { node: node.clone(), reads_from: primary.clone() },
            Probe::Announcement { node, announcement, channel } => Hit::Announcement { node: node.clone(), announcement: announcement.to_string(), channel: channel.to_string() },
            Probe::ProviderTerminate { pid, deployment_id } => Hit::ProviderTerminate { pid: *pid, deployment_id: deployment_id.clone() },
            Probe::Declaration { node, state } => Hit::Declaration { node: node.clone(), state: state.clone() },
            Probe::ProviderInspect { pid } => Hit::ProviderInspect { pid: *pid },
            Probe::DriftPass { node } => Hit::DriftPass { node: node.clone() },
            Probe::StorageStep { node, change_id } => Hit::StorageStep { node: node.clone(), change_id: change_id.clone() },
            Probe::StorageSwitch { change_id } => Hit::StorageSwitch { change_id: change_id.clone() },
        }
    }
}

struct Armed {
    spec: CutSpec,
    /// The first call that held here.
    hit: Option<Hit>,
    held: bool,
    released: bool,
    /// Flips to `true` on release; every held call waits on it.
    release: tokio::sync::watch::Sender<bool>,
    /// Calls held here now.
    held_calls: u64,
    held_since: Option<Instant>,
    held_ms: Option<u64>,
    /// Matching calls that arrived while another was already held here (they hold too).
    seen_while_held: u64,
    /// For a condition (nothing holds): its hold span, open from the first consultation until the release.
    condition_span: Option<tracing::Span>,
}

/// The armed cuts of one node-admin process.
pub struct AdminFaults {
    /// The admin's name.
    pub name: String,
    cuts: Mutex<BTreeMap<String, Armed>>,
    /// Every Build step receipt the Build decorator saw, by call (a consequence counter).
    receipts_seen: AtomicU64,
}

impl AdminFaults {
    /// The cuts of the admin named `name`; none armed.
    pub fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self { name: name.into(), cuts: Mutex::new(BTreeMap::new()), receipts_seen: AtomicU64::new(0) })
    }

    /// Arm `spec` under `id`. Refused by name when `id` is already armed and not spent.
    pub fn arm(&self, id: &str, spec: CutSpec) -> Result<(), String> {
        let mut cuts = self.cuts.lock().unwrap();
        if cuts.get(id).is_some_and(|c| !c.released) {
            return Err(format!("{}: cut `{id}` is already armed and not released", self.name));
        }
        let span = tracing::info_span!("rdm.testkit.fault.update.via-arm", node = %self.name, cut = id, spec = %serde_json::to_string(&spec).unwrap_or_default());
        span.in_scope(|| tracing::info!("cut armed"));
        cuts.insert(id.to_string(), Armed { spec, hit: None, held: false, released: false, release: tokio::sync::watch::channel(false).0, held_calls: 0, held_since: None, held_ms: None, seen_while_held: 0, condition_span: None });
        Ok(())
    }

    /// Release cut `id`: the call it holds goes on. Returns whether it held a call and for how long.
    /// Refused by name when nothing is armed under `id`.
    pub fn release(&self, id: &str) -> Result<(bool, Option<u64>), String> {
        let mut cuts = self.cuts.lock().unwrap();
        let c = cuts.get_mut(id).ok_or_else(|| format!("{}: no cut `{id}` is armed", self.name))?;
        let was_held = c.held;
        let held_so_far = c.held_since.map(|t| t.elapsed().as_millis() as u64);
        c.release.send_replace(true);
        // A cut that held nothing is spent by its release too: it can no longer hold.
        c.released = true;
        // A condition (not a held call) has no held caller to clear it.
        if matches!(c.spec, CutSpec::ProviderDomain | CutSpec::LeaveAnnouncement { .. } | CutSpec::Announcement { .. } | CutSpec::ProviderTerminate { .. } | CutSpec::ProviderUnproven { .. }) {
            c.held = false;
            c.held_ms = c.held_since.take().map(|t| t.elapsed().as_millis() as u64);
            // Dropping the span ends it: the hold span lasts from the first consultation to here.
            c.condition_span = None;
        }
        let held_ms = c.held_ms.or(held_so_far);
        tracing::info_span!("rdm.testkit.fault.update.via-release", node = %self.name, cut = id, was_held)
            .in_scope(|| tracing::info!("cut released"));
        Ok((was_held, held_ms))
    }

    /// Every cut and what it held, and the consequence counters.
    pub fn state(&self) -> FaultState {
        let cuts = self.cuts.lock().unwrap();
        let rows = cuts
            .iter()
            .map(|(id, c)| {
                let row = CutRow {
                    spec: c.spec.clone(),
                    armed: !c.released,
                    held: c.held,
                    released: c.released,
                    hit: c.hit.clone(),
                    held_for_ms: c.held_since.map(|t| t.elapsed().as_millis() as u64).or(c.held_ms),
                    seen_while_held: c.seen_while_held,
                };
                (id.clone(), row)
            })
            .collect();
        FaultState { node: self.name.clone(), cuts: rows, receipts_seen: self.receipts_seen.load(Ordering::SeqCst) }
    }

    /// Hold the caller if an armed cut matches `probe`; returns when the cut is released. Every
    /// matching call holds until the release (a second writer of the same thing cannot slip past a
    /// cut the first one is held at).
    pub async fn hold(&self, probe: Probe) {
        if let Some((id, release)) = self.hold_at_cut(&probe) {
            self.wait_release(&id, &probe, release).await;
        }
    }

    /// Holds the call at the first armed cut that matches it: the cut's id and its release, `None`
    /// when no cut matches.
    fn hold_at_cut(&self, probe: &Probe) -> Option<(String, tokio::sync::watch::Receiver<bool>)> {
        let mut cuts = self.cuts.lock().unwrap();
        // A storage step cut holds its first command only (`CutSpec::StorageStep`).
        cuts.iter_mut().find(|(_, c)| !c.released && c.spec.matches(probe) && !(matches!(c.spec, CutSpec::StorageStep { .. }) && c.hit.is_some())).map(|(id, c)| {
            if c.held_calls > 0 {
                c.seen_while_held += 1;
            }
            c.hit.get_or_insert_with(|| probe.hit());
            c.held_calls += 1;
            c.held = true;
            c.held_since.get_or_insert_with(Instant::now);
            (id.clone(), c.release.subscribe())
        })
    }

    /// Waits for the release of the cut `id` that holds the call, then counts the call released.
    async fn wait_release(&self, id: &str, probe: &Probe, mut release: tokio::sync::watch::Receiver<bool>) {
        let span = tracing::info_span!("rdm.testkit.fault.update.via-hold", node = %self.name, cut = %id, detail = %probe.hit().detail());
        span.in_scope(|| tracing::info!("cut holds"));
        let _ = release.wait_for(|released| *released).instrument(span).await;
        let mut cuts = self.cuts.lock().unwrap();
        if let Some(c) = cuts.get_mut(id) {
            c.held_calls -= 1;
            if c.held_calls == 0 {
                c.held = false;
                c.held_ms = c.held_since.take().map(|t| t.elapsed().as_millis() as u64);
            }
        }
    }

    /// For a cut that is a condition rather than a held call: is it in force now? Records the first
    /// time it was consulted while in force, and counts every consultation.
    pub fn in_force(&self, probe: Probe) -> bool {
        let mut cuts = self.cuts.lock().unwrap();
        let mut any = false;
        for (id, c) in cuts.iter_mut().filter(|(_, c)| !c.released && c.spec.matches(&probe)) {
            any = true;
            c.held = true;
            c.held_since.get_or_insert_with(Instant::now);
            c.hit.get_or_insert_with(|| probe.hit());
            c.seen_while_held += 1;
            // A root of its own, following the call that consulted it first: the hold lasts until the
            // release, and a child span would hold that call's whole ancestry open with it (a serve
            // span that never ends, so never exported).
            c.condition_span.get_or_insert_with(|| {
                let hold = tracing::info_span!(parent: None, "rdm.testkit.fault.update.via-hold", node = %self.name, cut = %id, detail = %probe.hit().detail());
                hold.follows_from(tracing::Span::current());
                hold
            });
        }
        any
    }
}

// ---- the decorators ------------------------------------------------------------------------------

/// The Build state, with the step-receipt and accepted-Build cuts.
pub(crate) struct FaultedBuilds {
    inner: Arc<dyn BuildStateAdapter>,
    faults: Arc<AdminFaults>,
}

#[async_trait]
impl BuildStateAdapter for FaultedBuilds {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        self.faults.hold(Probe::Accepted { build_id: accepted.build_id.to_string() }).await;
        self.inner.publish_accepted(accepted).await
    }
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(opened).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn accepted_newest_first(&self) -> Result<Vec<BuildId>, BuildStateError> {
        self.inner.accepted_newest_first().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        let outcome = self.inner.claim_attempt(claim).await?;
        if outcome == ClaimOutcome::Won {
            self.faults.hold(Probe::Claimed { build_id: claim.build_id.to_string(), attempt: claim.attempt, executor: claim.executor.clone() }).await;
        }
        Ok(outcome)
    }
    async fn claim_in_flight(&self, claim: &BuildAttemptClaim, claims: &dyn rdm_admin::storage::MeshStorage, mesh: &str, row: &rdm_admin::mesh_claims::InFlight) -> Result<rdm_admin::build_state::InFlightClaim, BuildStateError> {
        let decided = self.inner.claim_in_flight(claim, claims, mesh, row).await?;
        if decided == rdm_admin::build_state::InFlightClaim::Decided(ClaimOutcome::Won) {
            self.faults.hold(Probe::Claimed { build_id: claim.build_id.to_string(), attempt: claim.attempt, executor: claim.executor.clone() }).await;
        }
        Ok(decided)
    }
    async fn in_flight_live(&self, held: &rdm_admin::mesh_claims::InFlight) -> Result<bool, BuildStateError> {
        self.inner.in_flight_live(held).await
    }
    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(claim).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.faults.receipts_seen.fetch_add(1, Ordering::SeqCst);
        let failed = receipt.outcome != StepOutcome::Complete;
        self.faults
            .hold(Probe::Receipt { step: receipt.step.clone(), operation: receipt.operation.clone(), build_id: receipt.build_id.to_string(), attempt: receipt.attempt, failed })
            .await;
        self.inner.append_step_receipt(receipt).await
    }
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn note_blocked(&self, build_id: &rdm_admin::build::BuildId, lane: Option<&str>, attempt: u32, operation: &str, step: &str, reason: &str) {
        self.inner.note_blocked(build_id, lane, attempt, operation, step, reason).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn committed_facts(&self, build: Option<&rdm_admin::build::BuildId>) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        let facts = self.facts().await?;
        Ok(match build { Some(b) => facts.into_iter().filter(|f| f.build_id() == b).collect(), None => facts })
    }
    async fn recovery_of(&self, path: &rdm_admin::model::PathName, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(rdm_admin::build::BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(rdm_admin::build::BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &rdm_admin::build::BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }
    fn held_name(&self) -> Option<rdm_admin::model::PathName> {
        self.inner.held_name()
    }
}

/// The deployment provider; its control domain reads foreign while the provider-domain cut is armed.
pub(crate) struct FaultedProvider {
    inner: Arc<dyn DeploymentProvider>,
    faults: Arc<AdminFaults>,
}

#[async_trait]
impl DeploymentProvider for FaultedProvider {
    fn kind(&self) -> ProviderKind {
        self.inner.kind()
    }
    fn control_domain(&self) -> String {
        let real = self.inner.control_domain();
        if self.faults.in_force(Probe::ProviderDomain) {
            format!("{real}#testkit-foreign-domain")
        } else {
            real
        }
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        self.inner.spawn(spec).await
    }
    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
        if self.faults.in_force(Probe::ProviderTerminate { pid: handle.pid, deployment_id: handle.deployment_id.to_string() }) {
            return Err(DeployError::Terminate {
                deployment: handle.deployment_id.to_string(),
                reason: format!("testkit: {} refuses the terminate of pid {:?} while its provider-terminate cut is armed; nothing was signalled", self.faults.name, handle.pid),
            });
        }
        self.inner.terminate(handle, mode).await
    }
    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus {
        if self.faults.in_force(Probe::ProviderInspect { pid: handle.pid }) {
            return DeploymentStatus::Unknown;
        }
        self.inner.inspect(handle).await
    }
    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
        self.inner.signal_stop(handle).await
    }
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        self.inner.find(spec).await
    }
    async fn bind_ip(&self, node: &rdm_admin::model::PathName, birth: &rdm_admin::model::DeploymentId) -> Result<std::net::IpAddr, DeployError> {
        self.inner.bind_ip(node, birth).await
    }
    fn launched(&self) -> Vec<DeploymentHandle> {
        self.inner.launched()
    }
    fn exits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.exits()
    }
    async fn failure_detail(&self, handle: &DeploymentHandle, data_dir: &std::path::Path) -> String {
        self.inner.failure_detail(handle, data_dir).await
    }
}

/// The lifecycle events a deletion publishes, each with its cut before the publish.
pub(crate) struct FaultedEvents {
    inner: Arc<dyn LifecycleEvents>,
    faults: Arc<AdminFaults>,
}

#[async_trait]
impl LifecycleEvents for FaultedEvents {
    async fn deleting(&self, op: &LifecycleOp) {
        self.faults.hold(Probe::Event { event: "deleting", op: op.clone() }).await;
        self.inner.deleting(op).await
    }
    async fn deleted(&self, op: &LifecycleOp) {
        self.faults.hold(Probe::Event { event: "deleted", op: op.clone() }).await;
        self.inner.deleted(op).await
    }
    // Every other event the admin publishes passes through unchanged: a decorator that leaves one
    // to the trait's empty default withholds that event's gossip from every faulted estate.
    async fn stopped(&self, op: &LifecycleOp) {
        self.inner.stopped(op).await
    }
    async fn joined(&self, op: &LifecycleOp) {
        self.inner.joined(op).await
    }
    async fn leaving(&self, op: &LifecycleOp) {
        self.inner.leaving(op).await
    }
    fn now_rafka_ms(&self) -> u64 {
        self.inner.now_rafka_ms()
    }
}

/// The leave's seam: an armed [`CutSpec::LeaveAnnouncement`] that matches withholds that publish.
struct FaultedLeave(Arc<AdminFaults>);

impl rdm_admin::wiring::LeaveSeam for FaultedLeave {
    fn withholds(&self, node: &str, announcement: u32, channel: &'static str) -> bool {
        self.0.in_force(Probe::LeaveAnnouncement { node: node.to_string(), announcement, channel })
    }
}

/// The backbone's announcement seam: an armed [`CutSpec::Announcement`] that matches withholds that send.
struct FaultedAnnouncement(Arc<AdminFaults>);

impl rdm_admin::wiring::AnnouncementSeam for FaultedAnnouncement {
    fn withholds(&self, node: &str, announcement: &'static str, channel: &'static str) -> bool {
        self.0.in_force(Probe::Announcement { node: node.to_string(), announcement, channel })
    }
}

/// The topology read's seam: an armed [`CutSpec::TopologyRead`] holds the joining admin's start before it
/// makes the read, until the release.
struct FaultedRead(Arc<AdminFaults>);

#[async_trait]
impl rdm_admin::wiring::ReadSeam for FaultedRead {
    async fn before_topology_read(&self, node: &str, primary: &str) {
        self.0.hold(Probe::TopologyRead { node: node.to_string(), primary: primary.to_string() }).await;
    }
}

/// The status door's seam: an armed [`CutSpec::Declaration`] holds a node's declaration unanswered
/// before it is decided, until the release.
struct FaultedDeclaration(Arc<AdminFaults>);

#[async_trait]
impl rdm_admin::wiring::DeclarationSeam for FaultedDeclaration {
    async fn before_declaration(&self, node: &str, state: rdm_core::status::NodeState) {
        self.0.hold(Probe::Declaration { node: node.to_string(), state: format!("{state:?}") }).await;
    }
}

/// The reconcile loop's drift seam: an armed [`CutSpec::DriftPass`] holds the admin's pass before it
/// runs, until the release, and the loop names the cut that holds it.
struct FaultedDrift(Arc<AdminFaults>);

impl rdm_admin::wiring::DriftSeam for FaultedDrift {
    fn before_drift_pass(&self, node: &str) -> Option<rdm_admin::wiring::DriftHold> {
        let probe = Probe::DriftPass { node: node.to_string() };
        let (id, release) = self.0.hold_at_cut(&probe)?;
        let faults = self.0.clone();
        Some(rdm_admin::wiring::DriftHold { by: format!("the testkit cut {id}"), released: Box::pin(async move { faults.wait_release(&id, &probe, release).await }) })
    }
}

struct FaultedCommandFrame(Arc<AdminFaults>);

#[async_trait]
impl rdm_admin::wiring::CommandFrameSeam for FaultedCommandFrame {
    async fn before_stopped(&self, op: &LifecycleOp) {
        self.0.hold(Probe::Event { event: "stopped", op: op.clone() }).await;
    }

    fn hold_storage_step(&self, node: &rdm_core::PathName, change_id: &rdm_core::ChangeId) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>> {
        let probe = Probe::StorageStep { node: node.to_string(), change_id: change_id.to_string() };
        let (id, release) = self.0.hold_at_cut(&probe)?;
        let faults = self.0.clone();
        Some(Box::pin(async move { faults.wait_release(&id, &probe, release).await }))
    }

    async fn before_storage_switch(&self, change_id: &rdm_core::ChangeId) {
        self.0.hold(Probe::StorageSwitch { change_id: change_id.to_string() }).await;
    }
}

/// The `Wiring` that puts every decorator of `faults` into a node-admin.
pub fn wiring(faults: Arc<AdminFaults>) -> Wiring {
    let (f_builds, f_provider, f_events) = (faults.clone(), faults.clone(), faults.clone());
    Wiring { types: Vec::new(), alpns: Vec::new(), declared: Vec::new(), listeners: Vec::new(),
        leave_seam: Some(Arc::new(FaultedLeave(faults.clone()))),
        announcement_seam: Some(Arc::new(FaultedAnnouncement(faults.clone()))),
        read_seam: Some(Arc::new(FaultedRead(faults.clone()))),
        declaration_seam: Some(Arc::new(FaultedDeclaration(faults.clone()))),
        command_frame_seam: Some(Arc::new(FaultedCommandFrame(faults.clone()))),
        drift_seam: Some(Arc::new(FaultedDrift(faults.clone()))),
        builds: Some(Box::new(move |inner| Arc::new(FaultedBuilds { inner, faults: f_builds }))),
        fabric_storage: None,
        provider: Some(Box::new(move |inner| Arc::new(FaultedProvider { inner, faults: f_provider }))),
        lifecycle_events: Some(Box::new(move |inner| Arc::new(FaultedEvents { inner, faults: f_events }))),
        node_hooks: Default::default(),
        certs: CertChoice::NoCerts,
        serve_app: None,
        fabric_hooks: FabricHooks::no_app_work(),
    }
}

// ---- the boot cuts --------------------------------------------------------------------------------

/// One cut a boot file lists: `{"id": .., "kind": .., ..}`.
#[derive(Deserialize)]
struct BootArm {
    id: String,
    #[serde(flatten)]
    spec: CutSpec,
}

/// Where a node's boot arms live: `<root>/faults/<path.name>.boot.json`, with `root` the directory
/// holding every node's data dir.
pub(crate) fn faults_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("faults")
}

/// Arm what `<root>/faults/<name>.boot.json` lists (`[{"id": .., "kind": .., ..}]`) and remove the
/// file: a birth at this path arms its boot cuts once, so a later birth of the same path is clean.
/// A cut that must be in force before the admin's Node RPC door is ready (which answers NotReady
/// until then) is armed here.
pub fn arm_boot_cuts(faults: &AdminFaults, root: &std::path::Path) -> Result<usize, String> {
    let file = faults_dir(root).join(format!("{}.boot.json", faults.name));
    let bytes = match std::fs::read(&file) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(format!("{}: {e}", file.display())),
    };
    let arms: Vec<BootArm> = serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", file.display()))?;
    for a in &arms {
        faults.arm(&a.id, a.spec.clone())?;
    }
    std::fs::remove_file(&file).map_err(|e| format!("{}: {e}", file.display()))?;
    Ok(arms.len())
}
