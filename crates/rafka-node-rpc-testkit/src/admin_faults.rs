//! Testkit-only node-admin stalls: decorators over the traits a node-admin is built from, armed and
//! released through a door, so a scenario holds a real admin at one named cut of its documented
//! Build, deployment, lifecycle and hydration sequences and releases it (i143.e8.s2).
//!
//! The product node-admin carries no fault code. Its `Wiring` (`rafka_node_admin_core::wiring`)
//! takes the decorators here: a [`FaultedBuilds`] over the Build state, a [`FaultedProvider`] over
//! the deployment provider, a [`FaultedEvents`]
//! over the lifecycle events and one [`FaultedHook`] per reachable lifecycle phase. Each decorator
//! asks the shared [`AdminFaults`] at its one cut; an armed, matching cut parks the caller there
//! until the door releases it. Nothing else changes: every other call passes straight through.
//!
//! A cut is one-shot: it parks the first matching call, records what parked ([`AdminFaults::state`]),
//! and after its release it is spent. The door is the testkit Node RPC op `0x75`
//! ([`crate::fault_door`]): arm, release and state; the state is the active-injection
//! acknowledgement (`held: true` with the exact call it holds) and the observed consequence
//! counters. A cut that must be in force before the admin's Node RPC door is ready is armed from
//! `<root>/faults/<name>.boot.json` ([`arm_boot_cuts`]).

use crate::fault_door::{CutRow, FaultState, Hit};
use async_trait::async_trait;
use rafka_mesh_entity::LifecycleOp;
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::certs::CertChoice;
use rafka_node_admin_core::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt, ClaimOutcome,
    StepOutcome,
};
use rafka_node_admin_core::deployment::pipeline::LifecycleEvents;
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::fabric_storage::FabricIdentity;
use rafka_node_admin_core::lifecycle::{HookContext, HookPhase, LifecycleHook, LifecycleHookSpec, LifecycleScope, LifecycleState, ShapePredicate, TransitionKey};
use rafka_node_admin_core::model::ProviderKind;
use rafka_node_admin_core::wiring::{FabricHooks, Wiring};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Instrument;

/// Where one armed cut parks a call.
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
    },
    /// A lifecycle event a retirement publishes (`deleting`, `restarting`, `deleted`): the step's
    /// receipt is durable and the event is not yet published.
    Event {
        /// The event name: `deleting`, `restarting` or `deleted`.
        event: String,
        /// The node the event concerns.
        #[serde(default)] node: Option<String>,
    },
    /// A lifecycle hook phase of the `Node: Pending -> ReadyForTraffic` transition.
    Hook {
        /// The hook phase.
        phase: String,
        /// The node the hook concerns.
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
    /// The admin withholds the catch-up it sends every neighbour that comes up on the Build topic
    /// (the shutdown in force, the Fabric record and the active Builds' facts): the NeighborUp task
    /// skips that neighbour's catch-up, so none of its frames is offered to the topic. It models the
    /// one-shot catch-up lost at the send seam.
    CatchUp,
    /// The testkit application took the fabric's `sync_state` round and has not answered it: the
    /// fabric stays in state-sync until the release (`test_app` mode `hold`).
    StateSync,
    /// A joining admin has joined and is about to make the topology read (op `0x1E`) of node-start.md
    /// step 4, and has not made it: its start waits there, the node stays Pending and takes no traffic,
    /// until the release. The joiner arms it from `<root>/faults/<name>.boot.json` before its first line.
    TopologyRead,
    /// The admin withholds `announcement`, one of its final aggregates or leadership announcements
    /// (`rafka_mesh_transport::membership::Withhold`: `stopping-aggregate`, `primary-updated`,
    /// `aggregate-in-flight`), from `channel` (`mesh`, `backbone` or `any`) while the cut is armed:
    /// each such send is skipped, so the frame is never offered to the channel. It models the
    /// announcement lost on its way out (R-NS2: best-effort dissemination), not in a peer queue.
    Announcement {
        /// The announcement withheld.
        announcement: String,
        /// The channel: `mesh`, `backbone` or `any`.
        channel: String,
    },
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
    },
    /// A lifecycle event about to be published.
    Event {
        /// The event name.
        event: &'static str,
        /// The lifecycle operation the event names.
        op: LifecycleOp,
    },
    /// A lifecycle hook about to run.
    Hook {
        /// The hook phase.
        phase: &'static str,
        /// The hook's target.
        target: String,
        /// The transition the hook belongs to.
        transition_id: String,
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
    /// A catch-up about to be sent to a neighbour.
    CatchUp {
        /// The sending node.
        node: String,
        /// The neighbour.
        neighbour: String,
    },
    /// The application's answer to a `sync_state` round, about to be returned.
    StateSync {
        /// The round's Build.
        build_id: String,
        /// The round's attempt.
        attempt: u32,
    },
    /// A final aggregate or leadership announcement about to be sent on a channel.
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
        /// The authority the read goes to: the admin that answered the join.
        authority: String,
    },
}

impl CutSpec {
    fn matches(&self, p: &Probe) -> bool {
        let node_is = |want: &Option<String>, got: &str| want.as_deref().is_none_or(|w| got == w || got.ends_with(&format!(":{w}")));
        match (self, p) {
            (CutSpec::Receipt { step, operation, node }, Probe::Receipt { step: s, operation: o, .. }) => {
                step == s && operation.as_deref().is_none_or(|prefix| o.starts_with(prefix)) && node_is(node, o)
            }
            (CutSpec::Event { event, node }, Probe::Event { event: e, op }) => event == e && node_is(node, &op.name.to_string()),
            (CutSpec::Hook { phase, node }, Probe::Hook { phase: ph, target, .. }) => phase == ph && node_is(node, target),
            (CutSpec::AcceptedBuild, Probe::Accepted { .. }) => true,
            (CutSpec::ClaimedAttempt { attempt }, Probe::Claimed { attempt: a, .. }) => attempt.is_none_or(|w| w == *a),
            (CutSpec::ProviderDomain, Probe::ProviderDomain) => true,
            (CutSpec::LeaveAnnouncement { announcements, channel }, Probe::LeaveAnnouncement { announcement, channel: c, .. }) => announcements.contains(announcement) && (channel == "any" || channel == c),
            (CutSpec::CatchUp, Probe::CatchUp { .. }) => true,
            (CutSpec::StateSync, Probe::StateSync { .. }) => true,
            (CutSpec::TopologyRead, Probe::TopologyRead { .. }) => true,
            (CutSpec::Announcement { announcement, channel }, Probe::Announcement { announcement: a, channel: c, .. }) => announcement == a && (channel == "any" || channel == c),
            _ => false,
        }
    }
}

impl Probe {
    /// What a cut that holds this call records of it.
    fn hit(&self) -> Hit {
        match self {
            Probe::Receipt { step, operation, build_id, attempt } => Hit::Receipt { step: step.clone(), operation: operation.clone(), build_id: build_id.clone(), attempt: *attempt },
            Probe::Event { event, op } => Hit::Event { event: event.to_string(), operation: op.operation.clone(), build_id: op.build_id.clone(), attempt: op.attempt, node: op.name.to_string(), incarnation_id: op.incarnation.as_str().to_string() },
            Probe::Hook { phase, target, transition_id } => Hit::Hook { phase: phase.to_string(), target: target.clone(), transition_id: transition_id.clone() },
            Probe::Accepted { build_id } => Hit::Accepted { build_id: build_id.clone() },
            Probe::Claimed { build_id, attempt, executor } => Hit::Claimed { build_id: build_id.clone(), attempt: *attempt, executor: executor.clone() },
            Probe::ProviderDomain => Hit::ProviderDomain,
            Probe::LeaveAnnouncement { node, announcement, channel } => Hit::LeaveAnnouncement { node: node.clone(), announcement: *announcement, channel: channel.to_string() },
            Probe::CatchUp { node, neighbour } => Hit::CatchUp { node: node.clone(), neighbour: neighbour.clone() },
            Probe::StateSync { build_id, attempt } => Hit::StateSync { build_id: build_id.clone(), attempt: *attempt },
            Probe::TopologyRead { node, authority } => Hit::TopologyRead { node: node.clone(), reads_from: authority.clone() },
            Probe::Announcement { node, announcement, channel } => Hit::Announcement { node: node.clone(), announcement: announcement.to_string(), channel: channel.to_string() },
        }
    }
}

struct Armed {
    spec: CutSpec,
    /// The first call that parked here.
    hit: Option<Hit>,
    held: bool,
    released: bool,
    /// Flips to `true` on release; every parked call waits on it.
    release: tokio::sync::watch::Sender<bool>,
    /// Calls parked here now.
    parked: u64,
    held_since: Option<Instant>,
    held_ms: Option<u64>,
    /// Matching calls that arrived while another was already parked here (they park too).
    seen_while_held: u64,
    /// For a condition (nothing parks): its hold span, open from the first consultation until the release.
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
        cuts.insert(id.to_string(), Armed { spec, hit: None, held: false, released: false, release: tokio::sync::watch::channel(false).0, parked: 0, held_since: None, held_ms: None, seen_while_held: 0, condition_span: None });
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
        // A cut that parked nothing is spent by its release too: it can no longer park.
        c.released = true;
        // A condition (not a parked call) has no parked caller to clear it.
        if matches!(c.spec, CutSpec::ProviderDomain | CutSpec::LeaveAnnouncement { .. } | CutSpec::CatchUp | CutSpec::Announcement { .. }) {
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

    /// Every cut and what it parked, and the consequence counters.
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

    /// Park the caller if an armed cut matches `probe`; returns when the cut is released. Every
    /// matching call parks until the release (a second writer of the same thing cannot slip past a
    /// cut the first one is parked at).
    pub async fn hold(&self, probe: Probe) {
        let parked = {
            let mut cuts = self.cuts.lock().unwrap();
            cuts.iter_mut().find(|(_, c)| !c.released && c.spec.matches(&probe)).map(|(id, c)| {
                if c.parked > 0 {
                    c.seen_while_held += 1;
                }
                c.hit.get_or_insert_with(|| probe.hit());
                c.parked += 1;
                c.held = true;
                c.held_since.get_or_insert_with(Instant::now);
                (id.clone(), c.release.subscribe())
            })
        };
        let Some((id, mut release)) = parked else { return };
        let span = tracing::info_span!("rdm.testkit.fault.update.via-hold", node = %self.name, cut = %id, detail = %probe.hit().detail());
        span.in_scope(|| tracing::info!("cut holds"));
        let _ = release.wait_for(|released| *released).instrument(span).await;
        let mut cuts = self.cuts.lock().unwrap();
        if let Some(c) = cuts.get_mut(&id) {
            c.parked -= 1;
            if c.parked == 0 {
                c.held = false;
                c.held_ms = c.held_since.take().map(|t| t.elapsed().as_millis() as u64);
            }
        }
    }

    /// For a cut that is a condition rather than a parked call: is it in force now? Records the first
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
    async fn publish_fabric(&self, record: &FabricIdentity) -> Result<(), BuildStateError> {
        self.inner.publish_fabric(record).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
    }
    async fn current_build(&self) -> Result<rafka_node_admin_core::build_state::CurrentBuild, rafka_node_admin_core::build_state::BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rafka_node_admin_core::build_state::BuildStateError> {
        self.inner.newest_accepted_at_ms().await
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
    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(claim).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.faults.receipts_seen.fetch_add(1, Ordering::SeqCst);
        if receipt.outcome == StepOutcome::Complete {
            self.faults
                .hold(Probe::Receipt { step: receipt.step.clone(), operation: receipt.operation.clone(), build_id: receipt.build_id.to_string(), attempt: receipt.attempt })
                .await;
        }
        self.inner.append_step_receipt(receipt).await
    }
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn recovery_of(&self, path: &rafka_node_admin_core::model::PathName, node_id: &rafka_node_admin_core::model::NodeId, incarnation: &rafka_node_admin_core::model::IncarnationId) -> Result<Option<(rafka_node_admin_core::build::BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &rafka_node_admin_core::model::NodeId, incarnation: &rafka_node_admin_core::model::IncarnationId) -> Result<Option<(rafka_node_admin_core::build::BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &rafka_node_admin_core::build::BuildId, mesh: &str) -> Result<Vec<rafka_node_admin_core::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
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
        self.inner.terminate(handle, mode).await
    }
    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus {
        self.inner.inspect(handle).await
    }
    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
        self.inner.signal_stop(handle).await
    }
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
        self.inner.find(spec).await
    }
    async fn bind_ip(&self, node: &rafka_node_admin_core::model::PathName) -> Result<std::net::IpAddr, DeployError> {
        self.inner.bind_ip(node).await
    }
    fn launched(&self) -> Vec<DeploymentHandle> {
        self.inner.launched()
    }
    async fn failure_detail(&self, handle: &DeploymentHandle, data_dir: &std::path::Path) -> String {
        self.inner.failure_detail(handle, data_dir).await
    }
}

/// The lifecycle events a retirement publishes, each with its cut before the publish.
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
    async fn restarting(&self, op: &LifecycleOp) {
        self.faults.hold(Probe::Event { event: "restarting", op: op.clone() }).await;
        self.inner.restarting(op).await
    }
    // Every other event the admin publishes passes through unchanged: a decorator that leaves one
    // to the trait's empty default withholds that event's gossip from every faulted estate.
    async fn stopped(&self, op: &LifecycleOp) {
        self.inner.stopped(op).await
    }
    async fn joined(&self, op: &LifecycleOp) {
        self.inner.joined(op).await
    }
    async fn connections_deleting(&self, op: &LifecycleOp) {
        self.inner.connections_deleting(op).await
    }
    async fn leaving(&self, op: &LifecycleOp) {
        self.inner.leaving(op).await
    }
    async fn mesh_leaving(&self, event: &rafka_node_admin_core::deployment::pipeline::MeshLeaveEvent) {
        self.inner.mesh_leaving(event).await
    }
    async fn mesh_leave(&self, event: &rafka_node_admin_core::deployment::pipeline::MeshLeaveEvent) {
        self.inner.mesh_leave(event).await
    }
    async fn mesh_left(&self, event: &rafka_node_admin_core::deployment::pipeline::MeshLeaveEvent) {
        self.inner.mesh_left(event).await
    }
    fn now_rafka_ms(&self) -> u64 {
        self.inner.now_rafka_ms()
    }
}

/// A registered lifecycle hook that does nothing but cross its phase's cut.
pub(crate) struct FaultedHook {
    phase: HookPhase,
    faults: Arc<AdminFaults>,
}

#[async_trait]
impl LifecycleHook for FaultedHook {
    async fn run(&self, ctx: &HookContext) -> Result<(), String> {
        self.faults.hold(Probe::Hook { phase: self.phase.as_str(), target: ctx.target.clone(), transition_id: ctx.transition_id.clone() }).await;
        Ok(())
    }
}

/// The Build topic's catch-up seam: an armed [`CutSpec::CatchUp`] withholds that neighbour's catch-up.
struct FaultedCatchUp(Arc<AdminFaults>);

impl rafka_node_admin_core::wiring::CatchUpSeam for FaultedCatchUp {
    fn withholds(&self, node: &str, neighbour: &str) -> bool {
        self.0.in_force(Probe::CatchUp { node: node.to_string(), neighbour: neighbour.to_string() })
    }
}

/// The leave's seam: an armed [`CutSpec::LeaveAnnouncement`] that matches withholds that publish.
struct FaultedLeave(Arc<AdminFaults>);

impl rafka_node_admin_core::wiring::LeaveSeam for FaultedLeave {
    fn withholds(&self, node: &str, announcement: u32, channel: &'static str) -> bool {
        self.0.in_force(Probe::LeaveAnnouncement { node: node.to_string(), announcement, channel })
    }
}

/// The backbone's announcement seam: an armed [`CutSpec::Announcement`] that matches withholds that send.
struct FaultedAnnouncement(Arc<AdminFaults>);

impl rafka_node_admin_core::wiring::AnnouncementSeam for FaultedAnnouncement {
    fn withholds(&self, node: &str, announcement: &'static str, channel: &'static str) -> bool {
        self.0.in_force(Probe::Announcement { node: node.to_string(), announcement, channel })
    }
}

/// The topology read's seam: an armed [`CutSpec::TopologyRead`] parks the joining admin's start before it
/// makes the read, until the release.
struct FaultedRead(Arc<AdminFaults>);

#[async_trait]
impl rafka_node_admin_core::wiring::ReadSeam for FaultedRead {
    async fn before_topology_read(&self, node: &str, authority: &str) {
        self.0.hold(Probe::TopologyRead { node: node.to_string(), authority: authority.to_string() }).await;
    }
}

/// The phases of `Node: Pending -> ReadyForTraffic` the admin runs (`bring_into_traffic`); the
/// drain phases belong to a transition into `Draining`, which no admin path runs.
pub(crate) const NODE_READY_PHASES: [HookPhase; 3] = [HookPhase::BeforeEligibility, HookPhase::AfterEligibilityBeforeCommit, HookPhase::AfterTransition];

/// The `Wiring` that puts every decorator and hook of `faults` into a node-admin.
pub fn wiring(faults: Arc<AdminFaults>) -> Wiring {
    let (f_builds, f_provider, f_events) = (faults.clone(), faults.clone(), faults.clone());
    let key = TransitionKey { scope: LifecycleScope::Node, from: LifecycleState::Pending, to: LifecycleState::ReadyForTraffic };
    Wiring {
        leave_seam: Some(Arc::new(FaultedLeave(faults.clone()))),
        announcement_seam: Some(Arc::new(FaultedAnnouncement(faults.clone()))),
        catch_up_seam: Some(Arc::new(FaultedCatchUp(faults.clone()))),
        read_seam: Some(Arc::new(FaultedRead(faults.clone()))),
        builds: Some(Box::new(move |inner| Arc::new(FaultedBuilds { inner, faults: f_builds }))),
        fabric_storage: None,
        provider: Some(Box::new(move |inner| Arc::new(FaultedProvider { inner, faults: f_provider }))),
        lifecycle_events: Some(Box::new(move |inner| Arc::new(FaultedEvents { inner, faults: f_events }))),
        hydration: Default::default(),
        certs: CertChoice::NoCerts,
        serve_app: None,
        fabric_hooks: FabricHooks::no_app_work(),
        round_actions: None,
        hooks: NODE_READY_PHASES
            .iter()
            .map(|&phase| {
                (
                    LifecycleHookSpec { hook_id: format!("testkit-{}", phase.as_str()), transition: key, phase, applies_when: ShapePredicate::Always, blocking: true, max_attempts: 1 },
                    Arc::new(FaultedHook { phase, faults: faults.clone() }) as Arc<dyn LifecycleHook>,
                )
            })
            .collect(),
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
