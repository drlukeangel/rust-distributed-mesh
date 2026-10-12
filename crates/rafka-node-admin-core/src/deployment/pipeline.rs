//! `DeploymentPipeline`: create or retire one runtime (PRD §8;
//! mesh-control-plane.md §6).
//!
//! ```text
//! create: AllocateIdentity -> PrepareStorage -> PrepareNetwork
//!   -> DeployRuntime -> RegisterExactRuntimeHandle -> ResolveProviderControlDomain
//!   -> MakeRuntimeFactAvailableToBirth -> WaitForBind
//!   -> PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata -> WaitForMeshJoin
//!   -> WaitForNodeReady -> Complete
//! retire: DrainNode (drain-node to the exact birth; its receipt carries the CommandAdmission)
//!   -> AwaitNodeDrained (bounded; a deadline is an arm, never a failure)
//!   -> StopNode (stop-node, no implicit drain) -> AwaitNodeLeft (its `Stopped` reply, bounded, likewise)
//!   -> TerminateRuntime (the provider's stop and exact inspection: only `Exited` is terminal proof)
//!   -> RemoveTopologyMembership -> Complete
//! delete of a crashed target: NodeDeleting -> TerminateRuntime (the exact exit proof, nothing sent)
//!   -> NodeDeleted (from the mesh-primary's recorded exit proof) -> RemoveTopologyMembership -> Complete
//! update: UpdateNode (node recovery's copy of the caller-owned meta) -> Complete
//! ```
//!
//! Node recovery (node-recovery.md "Steps") is three operations of these, never a sequence of its
//! own: the delete of the crashed target, the create over its data dir, the update.
//!
//! A removal releases identity, never storage: the data dir and its endpoint key stay
//! (node-delete.md "Storage"), and no step here deletes a node's storage.
//!
//! Node-admin is the brain: identity, endpoints, storage and readiness are
//! decided here; the provider only realises or retires the runtime. Every
//! step appends a receipt to the Build's state and runs under one child span
//! of the pipeline's parent span. Optional steps (DNS, load balancer,
//! firewall) have no realisation on these providers and are not run.
//!
//! The runtime steps after `DeployRuntime` call no provider: each commits,
//! as its own receipt, one prerequisite of a managed birth (PRD §5.3) over
//! what `DeployRuntime` obtained: the handle names its runtime exactly; the
//! runtime lives in this provider's control domain; the normalized
//! `RuntimeFact` is in the birth's data dir, where the birth waits for it
//! before it publishes anything; the birth's projection (topology, fact,
//! data dir) is published. `WaitForNodeReady` does not begin until the
//! Build's state holds a `Complete` receipt for each of them
//! ([`READY_PREREQUISITES`]), and `WaitForMeshJoin` takes only the birth's
//! own digest carrying exactly that fact and data dir.
//!
//! Re-runs reconcile forward. A step whose receipt in an earlier attempt of
//! the same Build is `Complete` is reused from the decision that receipt
//! carries, never decided again, until the first step that has to run; from
//! there every step runs. `DeployRuntime` first looks for the runtime its
//! deployment id already started, so a crash between spawning and the
//! receipt never starts a second one. Every other step is idempotent.
//!
//! Each receipt records the executing admin (its endpoint, the seed every launch it makes
//! carries). A birth launched by another executor that never joined is stopped and decided afresh
//! under this executor's endpoint (`rdm.node_admin.deployment.reject.via-lost-executor`); a birth
//! that joined, and an identity no runtime was made under, stand.

use super::endpoint::KindSpec;
use crate::join::{Deployed, Joins};
use super::provider::{DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch, TerminationMode};
use crate::build::BuildId;
use crate::build_state::{BuildStateAdapter, BuildStepReceipt, StepOutcome};
use crate::model::{DeploymentId, EndpointId, IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::meta::StorageMeta;
use rafka_mesh_entity::{LifecycleOp, RuntimeFact};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// One step of creating a node, in the order of [`CreateStep::ORDER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateStep {
    /// Mint the birth's node id, incarnation and deployment id. Its receipt names the node id the
    /// name claim at the start of `PrepareStorage` is made for.
    AllocateIdentity,
    /// Claim the birth's name at the writer of its mesh's rows (`crate::node_rows::NodeRows::claim`),
    /// then ensure the node's transport key in its data directory and hand a node-admin its held
    /// runtime rows.
    PrepareStorage,
    /// Record that the provider needs nothing per node: the host network namespace is shared and a container provider's network exists per fabric.
    PrepareNetwork,
    /// Start the runtime through the provider.
    DeployRuntime,
    /// The provider's handle names its runtime exactly (a pid with its
    /// start token, an immutable container id).
    RegisterExactRuntimeHandle,
    /// The runtime lives in this provider's control domain.
    ResolveProviderControlDomain,
    /// The normalized `RuntimeFact` is in the birth's data dir.
    MakeRuntimeFactAvailableToBirth,
    /// Wait for the birth to report the addresses it bound.
    WaitForBind,
    /// The birth's projection: its node record and data dir, and the fact it
    /// publishes with its own membership digest.
    PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata,
    /// A mesh's first admin only: the fabric primary applies `MeshStatus::Pending` at this exact
    /// birth, through its Status door, as soon as the birth is bound and published and before it
    /// is heard or asked to be Ready (e4.s11): a peer mesh's admin is heard on the backbone only
    /// once it is its mesh's primary, which needs Ready, which needs this.
    ApplyMeshPending,
    /// Wait for the birth's own digest to reach the admin's membership.
    WaitForMeshJoin,
    /// Wait for the birth to declare itself ready.
    WaitForNodeReady,
    /// The node is created.
    Complete,
}

/// What `WaitForNodeReady` requires a `Complete` receipt for before it begins.
pub const READY_PREREQUISITES: [CreateStep; 4] = [
    CreateStep::RegisterExactRuntimeHandle,
    CreateStep::ResolveProviderControlDomain,
    CreateStep::MakeRuntimeFactAvailableToBirth,
    CreateStep::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata,
];

impl CreateStep {
    /// Every create step in order.
    pub const ORDER: [CreateStep; 13] = [
        Self::AllocateIdentity,
        Self::PrepareStorage,
        Self::PrepareNetwork,
        Self::DeployRuntime,
        Self::RegisterExactRuntimeHandle,
        Self::ResolveProviderControlDomain,
        Self::MakeRuntimeFactAvailableToBirth,
        Self::WaitForBind,
        Self::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata,
        Self::ApplyMeshPending,
        Self::WaitForMeshJoin,
        Self::WaitForNodeReady,
        Self::Complete,
    ];

    /// The step's name as it appears in receipts and spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::AllocateIdentity => "AllocateIdentity",
            Self::PrepareStorage => "PrepareStorage",
            Self::PrepareNetwork => "PrepareNetwork",
            Self::DeployRuntime => "DeployRuntime",
            Self::RegisterExactRuntimeHandle => "RegisterExactRuntimeHandle",
            Self::ResolveProviderControlDomain => "ResolveProviderControlDomain",
            Self::MakeRuntimeFactAvailableToBirth => "MakeRuntimeFactAvailableToBirth",
            Self::WaitForBind => "WaitForBind",
            Self::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata => "PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata",
            Self::WaitForMeshJoin => "WaitForMeshJoin",
            Self::ApplyMeshPending => "ApplyMeshPending",
            Self::WaitForNodeReady => "WaitForNodeReady",
            Self::Complete => "Complete",
        }
    }
}

/// The step a start adds to the wait for ready it shares with a create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartStep {
    /// `start-node` to the parked birth: the reply is `Started` once it has rejoined as itself.
    StartNode,
}

impl StartStep {
    /// The step's name as it appears in receipts and spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::StartNode => "StartNode",
        }
    }
}

/// The steps of node recovery's node.update (node-recovery.md sequence step 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStep {
    /// The caller-owned meta of the exited birth copied onto the new node (the frame name of
    /// node.update on 0x1B, node-update.md "Wire").
    UpdateNode,
    /// The update is done.
    Complete,
}

impl UpdateStep {
    /// The step's name as it appears in receipts and spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::UpdateNode => "UpdateNode",
            Self::Complete => "Complete",
        }
    }
}

/// One step of retiring a node, in the order of [`RetireStep::ORDER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireStep {
    /// The pre-notice (a removal only): the executor holds the operation; the node is
    /// found and not routable.
    NodeDeleting,
    /// `drain-node` to the exact birth: refuse new work, finish eligible work, keep running.
    DrainNode,
    /// Wait, bounded, for the birth's `node-drained` completion call.
    AwaitNodeDrained,
    /// `stop-node` to the exact birth: enter `Leaving` and shut down, with no implicit drain.
    StopNode,
    /// Wait, bounded, for node-left: the `Stopped` reply of the birth's `stop-node` call (R-ON1;
    /// the birth makes no upward node-left call).
    AwaitNodeLeft,
    /// Stop the runtime through the provider.
    TerminateRuntime,
    /// The departure (a removal only): the provider proved the runtime terminal; the
    /// node has left.
    NodeDeleted,
    /// The pre-event of a restart: the executor holds `restart-node:<path>`; the birth is held
    /// through its Leaving until its later birth is heard.
    NodeRestarting,
    /// A node-admin target only, before its drain or stop: when it holds a seat, TakeSeats to it
    /// and the committed seats installed (node-drain.md step 0; node-rpc-envelope.md "TakeSeats").
    /// Not in [`Self::ORDER`]: no other target runs it.
    SeatTransfer,
    /// Remove the node from the local topology.
    RemoveTopologyMembership,
    /// The node is retired.
    Complete,
}

impl RetireStep {
    /// Every retire step in order.
    pub const ORDER: [RetireStep; 9] = [
        Self::NodeDeleting,
        Self::DrainNode,
        Self::AwaitNodeDrained,
        Self::StopNode,
        Self::AwaitNodeLeft,
        Self::TerminateRuntime,
        Self::NodeDeleted,
        Self::RemoveTopologyMembership,
        Self::Complete,
    ];

    /// The step's name as it appears in receipts and spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::NodeDeleting => "NodeDeleting",
            Self::NodeDeleted => "NodeDeleted",
            Self::NodeRestarting => "NodeRestarting",
            Self::SeatTransfer => "SeatTransfer",
            Self::DrainNode => "DrainNode",
            Self::AwaitNodeDrained => "AwaitNodeDrained",
            Self::StopNode => "StopNode",
            Self::AwaitNodeLeft => "AwaitNodeLeft",
            Self::TerminateRuntime => "TerminateRuntime",
            Self::RemoveTopologyMembership => "RemoveTopologyMembership",
            Self::Complete => "Complete",
        }
    }
}

/// The steps a replacement adds to the create and retire steps it composes
/// ([`DeploymentPipeline::replace`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceStep {
    /// The predecessor gives up its `path.name` (`<path>.old`) before the successor is created, so
    /// two nodes never hold one `path.name`.
    RenamePredecessor,
    /// A replace of an exited predecessor: its storage goes into the successor's empty directory,
    /// before the successor's process exists.
    HandoffStorage,
}

impl ReplaceStep {
    /// The step's name as it appears in receipts and spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::RenamePredecessor => "RenamePredecessor",
            Self::HandoffStorage => "HandoffStorage",
        }
    }
}

/// Day 0: the externally started admin adopts its own runtime (PRD §5.3).
/// One step of adopting the externally started admin's own runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdoptStep {
    /// Take the current process as the admin's runtime.
    AdoptCurrentRuntime,
    /// Register the exact handle of the runtime.
    RegisterExactRuntimeHandle,
    /// Resolve the provider's control domain.
    ResolveProviderControlDomain,
    /// Publish the runtime fact and its metadata.
    PublishRuntimeFactAndCurrentRuntimeMetadata,
}

impl AdoptStep {
    /// Every adopt step in order.
    pub const ORDER: [AdoptStep; 4] =
        [Self::AdoptCurrentRuntime, Self::RegisterExactRuntimeHandle, Self::ResolveProviderControlDomain, Self::PublishRuntimeFactAndCurrentRuntimeMetadata];

    /// The step's name as it appears in receipts and spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::AdoptCurrentRuntime => "AdoptCurrentRuntime",
            Self::RegisterExactRuntimeHandle => "RegisterExactRuntimeHandle",
            Self::ResolveProviderControlDomain => "ResolveProviderControlDomain",
            Self::PublishRuntimeFactAndCurrentRuntimeMetadata => "PublishRuntimeFactAndCurrentRuntimeMetadata",
        }
    }
}

/// Where the Day-0 admin keeps its adoption receipts, in its data dir.
pub(crate) const ADOPTION_RECEIPTS: &str = "runtime-adoption.json";

/// One Day-0 adoption step's receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AdoptionReceipt {
    pub step: String,
    pub outcome: StepOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
}

/// The Day-0 adoption of this process, one receipt and span per step.
pub struct CurrentRuntimeAdoption {
    node: PathName,
    data_dir: PathBuf,
    span: tracing::Span,
    receipts: Vec<AdoptionReceipt>,
    /// Set by `AdoptCurrentRuntime`; `begin` returns only once it is.
    fact: Option<RuntimeFact>,
}

impl CurrentRuntimeAdoption {
    /// `AdoptCurrentRuntime`, `RegisterExactRuntimeHandle` and
    /// `ResolveProviderControlDomain` for this process, under a new
    /// deployment id. The receipts so far are in `data_dir`.
    pub async fn begin(node: &PathName, data_dir: &std::path::Path, deployment_id: &DeploymentId) -> Result<Self, PipelineError> {
        Self::begin_in(node, data_dir, deployment_id, None).await
    }

    /// [`Self::begin`], for a process that runs in a container of the container runtime whose
    /// control domain is `container_domain`: its runtime is that container, by immutable id.
    /// Its receipts are written on the blocking pool (`record_store::blocking_io`).
    pub async fn begin_in(node: &PathName, data_dir: &std::path::Path, deployment_id: &DeploymentId, container_domain: Option<String>) -> Result<Self, PipelineError> {
        let (node, data_dir, deployment_id) = (node.clone(), data_dir.to_path_buf(), deployment_id.clone());
        crate::record_store::blocking_io(move || Self::begin_on_disk(&node, &data_dir, &deployment_id, container_domain)).await
    }

    fn begin_on_disk(node: &PathName, data_dir: &std::path::Path, deployment_id: &DeploymentId, container_domain: Option<String>) -> Result<Self, PipelineError> {
        let kind = if container_domain.is_some() { crate::model::ProviderKind::Container } else { crate::model::ProviderKind::Process };
        let span = tracing::info_span!(
            "rdm.node_admin.deployment.update.via-pipeline",
            pipeline = "adopt-current",
            build_id = "",
            provider = ?kind,
            node = %node,
            attempt = 1u32,
        );
        let mut a = Self { node: node.clone(), data_dir: data_dir.to_path_buf(), span, receipts: Vec::new(), fact: None };
        a.fact = a.step(AdoptStep::AdoptCurrentRuntime, |n| {
            match &container_domain {
                Some(d) => RuntimeFact::of_this_container(&deployment_id.0, d),
                None => RuntimeFact::of_this_process(&deployment_id.0),
            }
            .map(|f| (Some(f.clone()), RuntimeEvidence::of(&f)))
            .map_err(|e| format!("{n}: {e}"))
        })?;
        let f = a.fact().clone();
        a.step(AdoptStep::RegisterExactRuntimeHandle, |n| {
            f.validate().map_err(|e| format!("{n}: {e}"))?;
            f.verify_is_this_runtime().map_err(|e| format!("{n}: {e}"))?;
            Ok((None, RuntimeEvidence::of(&f)))
        })?;
        a.step(AdoptStep::ResolveProviderControlDomain, |n| {
            let domain = match &container_domain {
                Some(d) => d.clone(),
                None => rafka_mesh_entity::runtime::process_control_domain()?,
            };
            if f.control_domain != domain {
                return Err(format!("{n}: its runtime is in control domain {}, not this provider's domain {}", f.domain_fingerprint(), rafka_mesh_entity::runtime::fingerprint(&domain)));
            }
            Ok((None, RuntimeEvidence::of(&f)))
        })?;
        Ok(a)
    }

    /// The runtime fact the adoption established.
    pub fn fact(&self) -> &RuntimeFact {
        self.fact.as_ref().expect("AdoptCurrentRuntime set the fact")
    }

    /// `PublishRuntimeFactAndCurrentRuntimeMetadata`: `publish` puts the fact
    /// and this data dir into the projection membership publishes.
    /// Its receipt is written on the blocking pool (`record_store::blocking_io`).
    pub async fn publish(self, publish: impl FnOnce(&RuntimeFact, String) + Send + 'static) -> Result<RuntimeFact, PipelineError> {
        crate::record_store::blocking_io(move || self.publish_on_disk(publish)).await
    }

    fn publish_on_disk(mut self, publish: impl FnOnce(&RuntimeFact, String)) -> Result<RuntimeFact, PipelineError> {
        let (f, dir) = (self.fact().clone(), self.data_dir.display().to_string());
        self.step(AdoptStep::PublishRuntimeFactAndCurrentRuntimeMetadata, |_| {
            publish(&f, dir.clone());
            Ok((None, RuntimeEvidence { data_dir: Some(dir.clone()), ..RuntimeEvidence::of(&f) }))
        })?;
        Ok(f)
    }

    fn step(&mut self, step: AdoptStep, work: impl FnOnce(&PathName) -> Result<(Option<RuntimeFact>, RuntimeEvidence), String>) -> Result<Option<RuntimeFact>, PipelineError> {
        let span = tracing::info_span!(
            parent: &self.span,
            "rdm.node_admin.deployment.update.via-step",
            step = step.name(),
            build_id = "",
            provider = ?crate::model::ProviderKind::Process,
            node = %self.node,
            attempt = 1u32,
            outcome = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
        );
        let started = Instant::now();
        let mut r = span.in_scope(|| work(&self.node));
        self.receipts.push(match &r {
            Ok((_, ev)) => AdoptionReceipt { step: step.name().into(), outcome: StepOutcome::Complete, output: serde_json::to_value(ev).ok() },
            Err(reason) => AdoptionReceipt { step: step.name().into(), outcome: StepOutcome::Failed { reason: reason.clone() }, output: None },
        });
        // Written after every step; the Ready gate reads them back. A receipt that cannot be
        // written fails its step: a lost one would read later as a step never completed.
        let file = self.data_dir.join(ADOPTION_RECEIPTS);
        if let Err(e) = serde_json::to_vec_pretty(&self.receipts).map_err(|e| e.to_string()).and_then(|b| std::fs::write(&file, b).map_err(|e| e.to_string())) {
            let unwritten = format!("{}: the receipt of {} cannot be written: {}: {e}", self.node, step.name(), file.display());
            r = Err(match r {
                Ok(_) => unwritten,
                Err(failed) => format!("{failed}; {unwritten}"),
            });
        }
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        span.record("outcome", if r.is_ok() { "complete" } else { "failed" });
        r.map(|(f, _)| f).map_err(|reason| PipelineError { step: step.name(), reason })
    }
}

/// The Day-0 adoption steps `data_dir` holds no `Complete` receipt for.
/// `Err` names the receipts file when it cannot be read or parsed.
/// Read on the blocking pool (`record_store::blocking_io`).
pub(crate) async fn adoption_missing(data_dir: &std::path::Path) -> Result<Vec<&'static str>, String> {
    let file = data_dir.join(ADOPTION_RECEIPTS);
    let receipts: Vec<AdoptionReceipt> = crate::record_store::blocking_io(move || match std::fs::read(&file) {
        Ok(b) => serde_json::from_slice(&b).map_err(|e| format!("{}: {e}", file.display())),
        // No receipt written: no step of the adoption is complete.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("{}: {e}", file.display())),
    })
    .await?;
    Ok(AdoptStep::ORDER
        .iter()
        .map(|s| s.name())
        .filter(|s| !receipts.iter().any(|r| r.step == *s && r.outcome == StepOutcome::Complete))
        .collect())
}

/// What the pipeline asks of the live mesh (gossip membership and Node RPC).
#[async_trait::async_trait]
pub trait NodeObserver: Send + Sync {
    /// What this exact birth (`node_id`, `incarnation`) publishes with its
    /// own membership digest; `None` until it has joined fabric membership.
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication>;
    /// Is the node serving?
    async fn ready(&self, node: &Node) -> Result<(), String>;
    /// The named blocker the node reports for its Ready (its `hydrate_before_ready` hook's
    /// `Blocked` outcome, with the event it retries on), or `None`. Its only source is the subject's
    /// Status `Blocked` answer to `ProbeNodeState`. A node that is only still Pending, with no hook
    /// blocker, has none, and neither has a node whose door answers `NotReady`: `ready` fails for it,
    /// the step waits, and nothing is framed `Blocked`.
    async fn ready_blocker(&self, _node: &Node) -> Option<String> {
        None
    }
    /// Send `cmd` (`drain-node` or `stop-node`) to the exact birth over Node RPC, opening the
    /// command before the send so its completion finds it. Answers what the call established,
    /// never a guess: the birth admitted it, or by name why nothing was established. The reply is
    /// admission, not completion.
    async fn send_command(&self, node: &Node, _cmd: crate::node_commands::NodeCommand, _ctx: &CommandContext) -> CommandAdmission {
        CommandAdmission::NotSent { reason: format!("this observer holds no Node RPC client to command {}", node.name) }
    }
    /// The SeatTransfer step of a drain, stop, restart or replace of the node-admin `node`: TakeSeats
    /// to it for every seat this admin's records say it holds, the committed seats installed from
    /// the reply. A target that holds no seat transfers nothing.
    async fn transfer_seats(&self, _node: &Node, _ctx: &CommandContext) -> Result<crate::fabric_handover::SeatTransferOutcome, String> {
        Ok(crate::fabric_handover::SeatTransferOutcome::NotAHolder)
    }
    /// Wait up to `within` for the commanded birth's completion at this admin: its `node-drained`
    /// call, or node-left, which is the `Stopped` reply of its `stop-node` call.
    async fn await_completion(&self, node: &Node, _cmd: crate::node_commands::NodeCommand, _ctx: &CommandContext, _within: Duration) -> Completion {
        Completion::NotAwaited { admission: format!("this observer holds no command book for {}", node.name) }
    }
}

/// The runtime part of a birth's own membership digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publication {
    /// The runtime fact the birth publishes.
    pub runtime: Option<RuntimeFact>,
    /// The birth's data directory.
    pub data_dir: Option<String>,
}

/// What a runtime step commits to its receipt: the runtime by fingerprint
/// (no pid, container id or domain in evidence).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RuntimeEvidence {
    deployment_id: String,
    provider: String,
    provider_control_domain_fingerprint: String,
    runtime_locator_kind: String,
    runtime_locator_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data_dir: Option<String>,
}

impl RuntimeEvidence {
    fn of(f: &RuntimeFact) -> Self {
        Self {
            deployment_id: f.deployment_id.clone(),
            provider: f.provider.as_str().into(),
            provider_control_domain_fingerprint: f.domain_fingerprint(),
            runtime_locator_kind: f.locator.kind().into(),
            runtime_locator_fingerprint: f.locator_fingerprint(),
            data_dir: None,
        }
    }
}

/// The last attempt before `attempt` whose run of `operation` ended: it failed a step, or it
/// reached its `Complete` step. Only runs after it were cut short (their executor died) and hand
/// their receipts on: a failed run decided nothing worth keeping, and a finished run's work is
/// done, so a later attempt of the same operation (a second restart of one node) runs afresh.
fn last_ended_attempt(receipts: &[&BuildStepReceipt], operation: &str, attempt: u32) -> u32 {
    receipts
        .iter()
        .filter(|r| r.operation == operation && r.attempt < attempt)
        .filter(|r| matches!(r.outcome, StepOutcome::Failed { .. }) || (r.step == CreateStep::Complete.name() && r.outcome == StepOutcome::Complete))
        .map(|r| r.attempt)
        .max()
        .unwrap_or(0)
}

/// The prerequisites of [`READY_PREREQUISITES`] that `receipts` holds no
/// `Complete` receipt for: of `operation`, from the runs after its last
/// ended attempt ([`last_ended_attempt`]), up to and including `attempt`.
pub(crate) fn ready_prerequisites_missing(receipts: &[BuildStepReceipt], operation: &str, attempt: u32) -> Vec<&'static str> {
    let ours: Vec<&BuildStepReceipt> = receipts.iter().filter(|r| r.operation == operation && r.attempt <= attempt).collect();
    let ended = last_ended_attempt(&ours, operation, attempt);
    READY_PREREQUISITES
        .iter()
        .map(|s| s.name())
        .filter(|s| !ours.iter().any(|r| r.attempt > ended && r.step == *s && r.outcome == StepOutcome::Complete))
        .collect()
}

/// The typed output of a `ReleaseStorage` receipt ([`crate::wire::RELEASE_STORAGE_STEP`]). No step
/// of this pipeline runs it: a delete retains the data dir and endpoint key (node-delete.md
/// "Storage"). It stays the receipt's shape so the Build topic's positional layout
/// ([`crate::wire`]) and a receipt an earlier build wrote still decode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "arm", rename_all = "kebab-case")]
pub enum StorageDisposition {
    /// Released through the exact locator.
    Released {
        /// The locator the storage was released through.
        locator: String,
    },
    /// `Persistent { on_retire: Preserve }`: left where it is.
    Preserved,
    /// The Build holds no meta for this path: left where it is, never destroyed on missing evidence.
    PreservedNoMeta,
}

/// What one directed command (`drain-node`, `stop-node`) established at the exact birth: the
/// DrainNode / StopNode step's receipt. Admission is not completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "arm", rename_all = "kebab-case")]
pub enum CommandAdmission {
    /// The birth answered `Applied`.
    Admitted,
    /// The birth answered `AlreadyApplied`: the same operation was admitted before.
    AlreadyAdmitted,
    /// The birth answered `stop-node` with `Stopped`: it drained, cut its mesh connections and is
    /// parked, and this is what its drain established.
    Stopped {
        /// The drain's receipt.
        receipt: rafka_node_rpc_contract::status::DrainReceipt,
    },
    /// The birth answered `start-node` with `Started`: it rejoined as itself.
    Started,
    /// The birth answered `start-node` with `StartFailed`: it is parked still, and nothing after
    /// `step` ran.
    StartFailed {
        /// The step that failed.
        step: String,
        /// Why.
        reason: String,
    },
    /// The call never reached the birth (no route, dial refused, connection lost before the send).
    NotSent {
        /// Why the call was not sent.
        reason: String,
    },
    /// The call may have reached the birth; no reply came back.
    Indeterminate {
        /// Why the outcome is unknown.
        reason: String,
    },
    /// The birth, or the op fence, refused the call by name (a stale incarnation, an unserved op).
    Refused {
        /// The refusal's name.
        reply: String,
    },
}

impl CommandAdmission {
    /// Whether the birth holds the command.
    pub fn admitted(&self) -> bool {
        matches!(self, Self::Admitted | Self::AlreadyAdmitted | Self::Stopped { .. } | Self::Started)
    }
    /// The arm's name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::AlreadyAdmitted => "already-admitted",
            Self::Stopped { .. } => "stopped",
            Self::Started => "started",
            Self::StartFailed { .. } => "start-failed",
            Self::NotSent { .. } => "not-sent",
            Self::Indeterminate { .. } => "indeterminate",
            Self::Refused { .. } => "refused",
        }
    }
}

/// The arm one directed command call is: only `Applied` and `AlreadyApplied` admit it; every other
/// reply is `Refused` by its name, an unsent call `NotSent`, an unanswered one `Indeterminate`.
pub fn command_admission(out: &rafka_node_rpc_contract::outcome::RpcOutcome<rafka_node_rpc_contract::status::StatusReply>) -> CommandAdmission {
    use rafka_node_rpc_contract::outcome::RpcOutcome;
    use rafka_node_rpc_contract::status::StatusReply;
    match out {
        RpcOutcome::Reply(r) => match r.value() {
            StatusReply::Applied => CommandAdmission::Admitted,
            StatusReply::AlreadyApplied => CommandAdmission::AlreadyAdmitted,
            StatusReply::Stopped { receipt } => CommandAdmission::Stopped { receipt: *receipt },
            StatusReply::Started => CommandAdmission::Started,
            StatusReply::StartFailed { step, reason } => CommandAdmission::StartFailed { step: step.clone(), reason: reason.clone() },
            other => CommandAdmission::Refused { reply: format!("{}: {other:?}", other.name()) },
        },
        RpcOutcome::NotSent(n) => CommandAdmission::NotSent { reason: format!("{:?}", n.reason()) },
        RpcOutcome::Indeterminate(i) => CommandAdmission::Indeterminate { reason: format!("{:?}", i.reason()) },
        RpcOutcome::Unserved(u) => CommandAdmission::Refused { reply: format!("unserved op {:#04x}", u.op()) },
        RpcOutcome::RejectedStale(r) => CommandAdmission::Refused { reply: format!("stale target {}", r.target_node_id()) },
    }
}

/// Whether the exact birth's completion (its `node-drained` call, or node-left: the `Stopped` reply
/// of its `stop-node` call) reached this admin: the
/// AwaitNodeDrained / AwaitNodeLeft step's receipt. A missing completion is indeterminate, never
/// success; the provider's terminal proof is what ends a stop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "arm", rename_all = "kebab-case")]
pub enum Completion {
    /// The matching completion call was accepted.
    Received,
    /// The lifecycle drain deadline passed with no completion call.
    Deadline,
    /// This deployment's own runtime exited before any completion call came.
    RuntimeExited,
    /// The command was not admitted, so no completion was awaited.
    NotAwaited {
        /// Why: the admission arm.
        admission: String,
    },
}

/// What the TerminateRuntime step commits: the provider's exact inspection said this birth's
/// runtime exited. It binds the node, the birth and the immutable runtime (by fingerprint, never a
/// pid or container id), so a recycled pid or a reusable container name cannot satisfy it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalReceipt {
    /// The node's `path.name`.
    pub node: String,
    /// The logical node.
    pub node_id: String,
    /// The exact birth.
    pub incarnation_id: String,
    /// The runtime that exited, by fingerprint; `None` when the handle names no exact runtime.
    pub runtime: Option<RuntimeProof>,
    /// The exit code, when the provider's record proves one.
    pub exit_code: Option<i32>,
}

/// An exact runtime by fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeProof {
    /// The deployment that ran the birth.
    pub deployment_id: String,
    /// The provider that ran it.
    pub provider: String,
    /// The control domain, by fingerprint.
    pub control_domain_fingerprint: String,
    /// The locator's kind.
    pub locator_kind: String,
    /// The locator, by fingerprint.
    pub locator_fingerprint: String,
}

impl RuntimeProof {
    /// The proof of `fact`.
    pub fn of(fact: &RuntimeFact) -> Self {
        Self {
            deployment_id: fact.deployment_id.clone(),
            provider: fact.provider.as_str().into(),
            control_domain_fingerprint: fact.domain_fingerprint(),
            locator_kind: fact.locator.kind().into(),
            locator_fingerprint: fact.locator_fingerprint(),
        }
    }
}

/// The identity a directed command carries: the Build, the attempt and the command's operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandContext {
    /// The Build the operation belongs to.
    pub build_id: String,
    /// The Build attempt that holds it.
    pub attempt: u32,
    /// `drain-node:<path>` or `stop-node:<path>`.
    pub operation: String,
}

/// The identity a mesh-leave gossip hook carries: the mesh, the Build, the attempt and the outer
/// operation `shutdown-mesh:<mesh_id>`, with the receipt manifest reference and the final primary
/// where the event has them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshLeaveEvent {
    /// The mesh that leaves.
    pub mesh_id: rafka_mesh_entity::MeshId,
    /// The Build the operation belongs to.
    pub build_id: String,
    /// The Build attempt that holds it.
    pub attempt: u32,
    /// `shutdown-mesh:<mesh_id>`.
    pub operation: String,
    /// The reference to the terminal receipts, when the event carries one.
    pub receipt_manifest: Option<String>,
    /// The final mesh-primary, when the event names it.
    pub final_primary: Option<(NodeId, IncarnationId)>,
}

/// Publishes the lifecycle events of a removal or a restart.
///
/// Every method is required: an implementation states what it does with each event, so a wrapper
/// cannot drop one silently by leaving it to a default.
#[async_trait::async_trait]
pub trait LifecycleEvents: Send + Sync {
    /// The removal is under way.
    async fn deleting(&self, op: &LifecycleOp);
    /// The removal completed.
    async fn deleted(&self, op: &LifecycleOp);
    /// A restart's pre-event (`NodeRestarting`): the birth is held through its Leaving.
    async fn restarting(&self, op: &LifecycleOp);
    /// The exact birth answered `stop-node` with `stopped`: it is parked. The mesh primary publishes
    /// the departure as `NodeStopped` in its in-flight overlay; the node gossips nothing.
    async fn stopped(&self, op: &LifecycleOp);
    /// The stopped birth answered `start-node` with `Started`: this executor took its `node.joined`
    /// (`op` is the start's operation on that exact birth). Its stop ends here and nowhere else
    /// (node-start.md step 2), the birth is held `Pending`, and this mesh's projection says so.
    async fn joined(&self, op: &LifecycleOp);
    /// The accepted `drain-node` command's hook: `node-draining`, on the executor's own mesh
    /// channel and the backbone.
    async fn draining(&self, op: &LifecycleOp);
    /// The accepted `stop-node` command's hook: `node-leaving`, on the executor's own mesh channel
    /// and the backbone.
    async fn leaving(&self, op: &LifecycleOp);
    /// The accepted `leave-mesh` command's hook: `mesh-leaving`, authored by the fabric-primary on
    /// the backbone and its own mesh channel.
    async fn mesh_leaving(&self, event: &MeshLeaveEvent);
    /// The `mesh-leave` handoff call's hook: authored by the still-running mesh-primary on the
    /// backbone and its own mesh channel.
    async fn mesh_leave(&self, event: &MeshLeaveEvent);
    /// The completed workflow's hook: `mesh-left`, authored by the fabric-primary after every exact
    /// exit is proven.
    async fn mesh_left(&self, event: &MeshLeaveEvent);
    /// The Rafka-time the events are stamped with (`event_at_rafka_ms`): the clock the process
    /// composed for its membership, never a clock of the pipeline's own.
    fn now_rafka_ms(&self) -> u64;
}

/// No events: a pipeline under test with no membership, and so no rafka-time.
pub struct NoLifecycleEvents;

#[async_trait::async_trait]
impl LifecycleEvents for NoLifecycleEvents {
    async fn deleting(&self, _op: &LifecycleOp) {}
    async fn deleted(&self, _op: &LifecycleOp) {}
    async fn restarting(&self, _op: &LifecycleOp) {}
    async fn stopped(&self, _op: &LifecycleOp) {}
    async fn joined(&self, _op: &LifecycleOp) {}
    async fn draining(&self, _op: &LifecycleOp) {}
    async fn leaving(&self, _op: &LifecycleOp) {}
    async fn mesh_leaving(&self, _event: &MeshLeaveEvent) {}
    async fn mesh_leave(&self, _event: &MeshLeaveEvent) {}
    async fn mesh_left(&self, _event: &MeshLeaveEvent) {}
    /// A pipeline with no membership holds no rafka-time: its events carry zero, and no OS clock
    /// stands in for one.
    fn now_rafka_ms(&self) -> u64 {
        0
    }
}

/// The destination of a created node's record: the admin's topology.
pub trait TopologySink: Send + Sync {
    /// Publish `node`.
    fn publish(&self, node: Node);
    /// Remove the node named `name`.
    fn remove(&self, name: &PathName);
    /// Remove exactly the birth `incarnation` of `node_id` at `name`: a record of another birth held
    /// at the path (a recovery's new node) stays.
    fn remove_birth(&self, name: &PathName, node_id: &NodeId, incarnation: &IncarnationId);
    /// The birth `incarnation` of `node_id` gives up `<name>` to its successor: its launch record no
    /// longer stands at the path. The view names it by its row, which the mesh's row writer moves to
    /// `<name>.old`. Whatever else is recorded at the path stays.
    fn rename_predecessor(&self, _name: &PathName, _node_id: &NodeId, _incarnation: &IncarnationId) {}
    /// The writer of node rows this admin's executor writes through (`crate::node_rows`): a
    /// create's name claim, a replace's move to `.old`, a removal's `Deleted` row. `None` where the
    /// sink keeps no nodes store.
    fn node_rows(&self) -> Option<std::sync::Arc<crate::node_rows::NodeRows>> {
        None
    }
}

/// The fabric-wide facts every launch carries.
#[derive(Debug, Clone)]
pub struct LaunchTemplate {
    /// The Fabric's name (its label) and its identity.
    pub fabric: String,
    /// The fabric's minted id.
    pub fabric_id: rafka_mesh_entity::FabricId,
    /// The executable every launch runs.
    pub executable: PathBuf,
    /// Members to join gossip through: `(public key, address)`.
    pub seeds: Vec<(String, SocketAddr)>,
    /// The admin these launches are made by: the target of each birth's `JoinNode`.
    pub launcher: rafka_mesh_entity::launch::Launcher,
    /// Passed through to every node (`RDM_EVIDENCE_DIR`, `RUST_LOG`, ...).
    pub env: BTreeMap<String, String>,
    /// The root of every node's data directory.
    pub data_root: PathBuf,
    /// The fabric's shared store root every launch carries (R14): the estate's one directory,
    /// handed on unchanged.
    pub store_dir: PathBuf,
    /// The issuing material a new mesh's first node-admin is launched with: set on the template of
    /// that launch alone, from the signer's `issue_mesh_issuer`. `None` for every other launch.
    pub mesh_issuer: Option<Vec<u8>>,
}

impl LaunchTemplate {
    /// The executing admin, as its receipts record it: the endpoint (`key@addr`) every launch
    /// from this template carries as its seed. Another birth of the same admin path has another
    /// endpoint, so a decision recorded under one executor is told from another's.
    pub fn executor(&self) -> String {
        self.seeds.iter().map(|(k, a)| format!("{k}@{a}")).collect::<Vec<_>>().join(",")
    }
}

/// A request to create one node.
#[derive(Debug, Clone)]
pub struct CreateRequest {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
    /// The node to create.
    pub node: PathName,
    /// The endpoint plan of the node's kind.
    pub spec: &'static KindSpec,
    /// The data dir the create's spec names (node-recovery.md sequence step 4: node recovery's
    /// create, over the exited birth's data dir): taken over whole, its endpoint key loaded and never
    /// minted. `None`: a fresh data dir.
    pub data_dir: Option<PathBuf>,
    /// The runtime rows this admin holds, handed to a node-admin's own nodes.storage before it
    /// starts: its durable map of the births a predecessor heard.
    pub held_runtimes: Vec<crate::storage::RuntimeRow>,
}

/// Work the executor does at the birth between its mesh join and its Ready: the fabric primary's
/// hand-off of its mesh's state at a mesh's first admin (Pending for a new mesh; the mesh's current
/// state for the admin created for an existing one). Runs as its own receipted step
/// (`ApplyMeshPending`), which records the state applied; a failure fails the create, so nothing
/// downstream starts under an assumed state.
pub(crate) type BeforeReady = std::sync::Arc<dyn Fn(Node) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<rafka_node_rpc_contract::status::MeshState, String>> + Send>> + Send + Sync>;

/// Work a create awaits inside its `DeployRuntime` step right before it launches the new runtime,
/// and only then: a step an earlier attempt receipted (its runtime still runs and is reused) and a
/// runtime the provider finds this deployment already started run none of it. A create over an
/// exited birth's data dir fences that birth here (node-recovery.md "Delete fences the old instance
/// before the new join"): the new runtime carries the same endpoint key, so the fence is done before
/// it exists and never once it does.
pub(crate) type BeforeLaunch<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;

/// A request to retire one node.
#[derive(Debug, Clone)]
pub struct RetireRequest {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
    /// The node's current record.
    pub node: Node,
    /// The runtime handle of the birth.
    pub handle: DeploymentHandle,
    /// A crashed target (node-delete.md "Crashed target"; node-recovery.md sequence step 3): the
    /// mesh-primary's recorded exit proof of the exact birth. Its stop steps are already done:
    /// nothing is sent to it, and node.deleted is published from this proof. `None`: a live target.
    pub exit: Option<crate::status_storage::NodeExitRow>,
}

/// A request to start (or restart) a parked birth: the same process rejoins as itself.
#[derive(Debug, Clone)]
pub struct StartRequest {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
    /// The node's current record.
    pub node: Node,
    /// The runtime handle of the birth.
    pub handle: DeploymentHandle,
    /// What this admin deployed for the birth, held again for the rejoin's `JoinNode`.
    pub deployed: Deployed,
}

/// What follows a stop's `stopped`: the process stays parked (a standalone stop, a restart), or the
/// provider ends it and proves the exit (a delete, a replace, a mesh leave). `publish_stopped` is
/// whether the mesh primary publishes `NodeStopped` from the `stopped` reply: every whole node.stop
/// does (node-stop.md step 5), a delete's, a replace's and a mesh leave member's included; a restart
/// holds the birth with `NodeRestarting` instead (node-restart.md).
#[derive(Debug, Clone, Copy)]
pub(crate) enum StopEnd {
    Park { publish_stopped: bool },
    Terminate { publish_stopped: bool },
}

/// What a stop's wait step must have seen to complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopProof {
    /// Whatever the birth answered: the shutdown leg of a retire or a mesh leave member, which the
    /// provider's terminal proof judges.
    Lenient,
    /// stop-node admitted and `stopped` received, or the step fails by name: a whole node.stop, a
    /// standalone one or the stop a restart composes, and a replace's predecessor, whose only proof
    /// is that reply (node-stop.md "Completion": the matching `node.stopped`; node-replace.md step 1).
    Stopped,
}

/// A request to replace one birth with a new node at its path.
#[derive(Debug, Clone)]
pub struct ReplaceRequest {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
    /// The old birth's current record.
    pub predecessor: Node,
    /// The old birth's exact runtime handle.
    pub handle: DeploymentHandle,
    /// The endpoint plan of the successor's kind.
    pub spec: &'static KindSpec,
    /// The runtime rows this admin holds, handed to a node-admin successor's nodes.storage before it
    /// starts.
    pub held_runtimes: Vec<crate::storage::RuntimeRow>,
}

/// A request for node recovery's node.update (node-recovery.md sequence step 6): the caller-owned
/// meta of the exited birth copied onto the new node created at its path.name.
#[derive(Debug, Clone)]
pub struct UpdateRequest {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
    /// The node updated: the one the create made at the exited birth's path.name.
    pub node: Node,
    /// The exited birth's record, whose caller-owned meta is copied.
    pub from: Node,
}

/// The proven-terminal predecessor of a replace, whose storage the successor's create takes between
/// its prepared identity and the start of its runtime.
struct ExitedPredecessor<'a> {
    build_id: &'a BuildId,
    predecessor: &'a Node,
}

/// A step that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineError {
    /// The step.
    pub step: &'static str,
    /// Why it failed.
    pub reason: String,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} failed: {}", self.step, self.reason)
    }
}

/// The bounds of the pipeline's waits.
#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    /// How long `WaitForBind` waits.
    pub bind: Duration,
    /// How long `WaitForMeshJoin` waits.
    pub join: Duration,
    /// How long `WaitForNodeReady` waits.
    pub ready: Duration,
    /// How long `AwaitNodeDrained` and `AwaitNodeLeft` wait for a completion call.
    pub drain: Duration,
    /// Stop-ladder grace before a forced kill; longer than the node's own
    /// drain deadline (node-rpc §35).
    pub stop_grace: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            bind: Duration::from_secs(20),
            join: Duration::from_secs(30),
            ready: Duration::from_secs(30),
            drain: Duration::from_secs(10),
            stop_grace: Duration::from_secs(8),
        }
    }
}

/// Where a birth reported it bound: the addresses its own digest names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bound {
    /// The transport address the birth bound.
    pub transport: SocketAddr,
    /// The non-Iroh listeners the birth bound, by name.
    pub listeners: Vec<(String, SocketAddr)>,
}

impl Bound {
    fn of(d: &rafka_mesh_entity::MeshDigest) -> Result<Self, String> {
        let transport = d.node.transport_addr;
        if transport.ip().is_unspecified() || transport.port() == 0 {
            return Err(format!("it reported the transport address {transport}, which is not an address a peer can dial"));
        }
        let mut listeners = Vec::new();
        if let Some(base) = &d.admin_api_base {
            let addr = base
                .strip_prefix("http://")
                .and_then(|a| a.parse::<SocketAddr>().ok())
                .ok_or_else(|| format!("it reported the control API base {base:?}, which is not http://<ip>:<port>"))?;
            listeners.push(("control".to_string(), addr));
        }
        Ok(Self { transport, listeners })
    }
}

/// The pipeline that creates and retires runtimes.
pub struct DeploymentPipeline<'a> {
    /// The provider that realises runtimes.
    pub provider: &'a dyn DeploymentProvider,
    /// The births this admin deployed and awaits a `JoinNode` from.
    pub joins: &'a Joins,
    /// The observer of births' membership.
    pub observer: &'a dyn NodeObserver,
    /// Where created nodes are published.
    pub sink: &'a dyn TopologySink,
    /// Where lifecycle events are published.
    pub lifecycle: &'a dyn LifecycleEvents,
    /// The Build state receipts are appended to.
    pub builds: &'a dyn BuildStateAdapter,
    /// The facts every launch carries.
    pub template: &'a LaunchTemplate,
    /// The bounds of the waits.
    pub timeouts: Timeouts,
}

/// A created node and the runtime realising it.
#[derive(Debug, Clone)]
pub struct Created {
    /// The created node's record.
    pub node: Node,
    /// The runtime realising the node.
    pub handle: DeploymentHandle,
}

/// What `AllocateIdentity` decides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Identity {
    node_id: NodeId,
    incarnation: IncarnationId,
    /// Always `None`: a create mints a new node and supersedes no birth (node-restart.md "No
    /// supersedes lineage"). The field keeps its position in the receipt's wire form
    /// (`wire::WireOutput::Identity`, positional in every Build message).
    supersedes: Option<IncarnationId>,
    deployment_id: DeploymentId,
}

/// Read the transport key an existing data dir holds, minting none: a recovery's node takes the
/// endpoint key of the data dir it inherits (node-recovery.md, "Retained"), so a dir with no key
/// is refused by name, never given a new one.
pub(crate) fn load_transport_key(data_dir: &std::path::Path) -> Result<EndpointId, String> {
    let path = data_dir.join("node-key");
    let h = match std::fs::read_to_string(&path) {
        Ok(h) => h,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !data_dir.is_dir() => {
            return Err(format!("the inherited data dir {} does not exist: no node-key to load the endpoint key from", data_dir.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(format!("node-key is missing in the inherited data dir {}: the endpoint key cannot be retained", data_dir.display())),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let bytes: [u8; 32] = hex::decode(h.trim())
        .map_err(|e| format!("{}: {e}", path.display()))?
        .try_into()
        .map_err(|_| format!("{}: not 32 bytes", path.display()))?;
    Ok(EndpointId(iroh::SecretKey::from_bytes(&bytes).public().to_string()))
}

/// One run of one operation (`create-node:<path>`, `retire-node:<path>`).
pub(crate) struct Run<'r> {
    build_id: &'r BuildId,
    attempt: u32,
    node: &'r PathName,
    operation: String,
    /// Outputs of this operation's `Complete` receipts from earlier attempts.
    done: HashMap<String, Option<serde_json::Value>>,
    /// The executor that recorded each of `done` (`None`: a line that did not record it).
    owner: HashMap<String, Option<String>>,
    /// Still reusing: no step of this run has had to execute yet.
    reusing: bool,
}

impl DeploymentPipeline<'_> {
    /// The decisions earlier attempts made for this operation and may hand
    /// on: the `Complete` receipts of runs that were cut short (the executor
    /// died mid-run), after the last attempt whose run ended
    /// ([`last_ended_attempt`]): failed, or finished.
    async fn begin<'r>(&self, build_id: &'r BuildId, attempt: u32, node: &'r PathName, operation: String) -> Run<'r> {
        let (done, owner) = match self.builds.read_build(build_id).await {
            Ok(view) => {
                let earlier: Vec<BuildStepReceipt> = view.steps.into_iter().filter(|r| r.operation == operation && r.attempt < attempt).collect();
                let ended = last_ended_attempt(&earlier.iter().collect::<Vec<_>>(), &operation, attempt);
                let handed_on: Vec<BuildStepReceipt> = earlier.into_iter().filter(|r| r.attempt > ended && r.outcome == StepOutcome::Complete).collect();
                let owner = handed_on.iter().map(|r| (r.step.clone(), r.executor.clone())).collect();
                (handed_on.into_iter().map(|r| (r.step, r.output)).collect(), owner)
            }
            Err(_) => (HashMap::new(), HashMap::new()),
        };
        Run { build_id, attempt, node, operation, done, owner, reusing: true }
    }

    fn pipeline_span(&self, kind: &'static str, build_id: &BuildId, node: &PathName, attempt: u32) -> tracing::Span {
        tracing::info_span!(
            "rdm.node_admin.deployment.update.via-pipeline",
            pipeline = kind,
            build_id = %build_id,
            provider = ?self.provider.kind(),
            node = %node,
            attempt,
        )
    }

    /// Run one step under its span and append its receipt (with its output),
    /// or reuse an earlier attempt's decision. A failure stops the run.
    async fn step<T, Fut>(&self, run: &mut Run<'_>, step: &'static str, work: Fut) -> Result<T, PipelineError>
    where
        T: Serialize + DeserializeOwned,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "rdm.node_admin.deployment.update.via-step",
            step,
            build_id = %run.build_id,
            provider = ?self.provider.kind(),
            node = %run.node,
            attempt = run.attempt,
            outcome = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
        );
        let started = Instant::now();
        if run.reusing {
            let prior = run.done.get(step).map(|o| serde_json::from_value::<T>(o.clone().unwrap_or(serde_json::Value::Null)));
            if let Some(Ok(v)) = prior {
                span.record("elapsed_ms", started.elapsed().as_millis() as u64);
                span.record("outcome", "reused");
                return Ok(v);
            }
            run.reusing = false;
        }
        let r = work.instrument(span.clone()).await;
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        let (outcome, output) = match &r {
            Ok(v) => (StepOutcome::Complete, serde_json::to_value(v).ok().filter(|v| !v.is_null())),
            Err(reason) => (StepOutcome::Failed { reason: reason.clone() }, None),
        };
        // The receipt is the step's durable commit: a step whose receipt the Build state does not
        // write to the executor's own log (a fact with no wire shape, a fenced seat, a failed local
        // write) is not complete, and the run stops at it by name. A failed broadcast after the
        // local write is not a refusal: the Build state names it and the step is committed.
        let appended = self
            .builds
            .append_step_receipt(&BuildStepReceipt {
                build_id: run.build_id.clone(),
                attempt: run.attempt,
                operation: run.operation.clone(),
                step: step.into(),
                outcome,
                output,
                executor: Some(self.template.executor()),
            })
            .await;
        if let Err(e) = appended {
            let unwritten = format!("{}: the {step} receipt of {} (Build {}, attempt {}) was not written: {e}", self.template.executor(), run.operation, run.build_id, run.attempt);
            span.record("outcome", "receipt-refused");
            tracing::info_span!(
                "rdm.node_admin.deployment.reject.via-receipt-refused",
                step,
                build_id = %run.build_id,
                attempt = run.attempt,
                operation = %run.operation,
                node = %run.node,
                work = if r.is_ok() { "complete" } else { "failed" },
                detail = %unwritten,
            )
            .in_scope(|| tracing::info!("the step's receipt was refused: the step is not committed"));
            let reason = match r {
                Ok(_) => unwritten,
                // The step failed; its Failed receipt not written is said with it.
                Err(failed) => format!("{failed}; and {unwritten}"),
            };
            return Err(PipelineError { step, reason });
        }
        span.record("outcome", if r.is_ok() { "complete" } else { "failed" });
        r.map_err(|reason| PipelineError { step, reason })
    }

    /// Create `req.node` through every step.
    pub async fn create(&self, req: &CreateRequest) -> Result<Created, PipelineError> {
        self.create_with(req, None, None).await
    }

    /// `create`, with `before_ready` run as the `ApplyMeshPending` step once the birth has joined
    /// its mesh and before it is asked to be Ready, and `before_launch` awaited right before the new
    /// runtime is launched ([`BeforeLaunch`]).
    pub(crate) async fn create_with(&self, req: &CreateRequest, before_ready: Option<BeforeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("create", &req.build_id, &req.node, req.attempt);
        self.create_steps(req, before_ready, before_launch).instrument(span).await
    }

    async fn create_steps(&self, req: &CreateRequest, before_ready: Option<BeforeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        let mut run = self.begin(&req.build_id, req.attempt, &req.node, format!("create-node:{}", req.node)).await;
        let mut created = self.create_to_ready(&mut run, req, before_ready, None, before_launch).await?;
        created.node.status = NodeStatus::ReadyForTraffic;
        self.step(&mut run, CreateStep::Complete.name(), async {
            self.sink.publish(created.node.clone());
            Ok(())
        })
        .await?;
        Ok(created)
    }

    /// Every create step through `WaitForNodeReady`, as steps of `run`. A replacement runs them inside
    /// its own operation; the replace of an exited predecessor ([`ExitedPredecessor`]) also hands its storage
    /// over once the successor's identity, storage and network are decided, and only then does the
    /// successor start.
    /// `req.data_dir`: the data dir the spec names (the create over an exited birth's), taken over
    /// whole (node-recovery.md sequence step 4). `before_launch`: awaited right before the new runtime
    /// is launched, and only when it is ([`BeforeLaunch`]).
    async fn create_to_ready(&self, run: &mut Run<'_>, req: &CreateRequest, before_ready: Option<BeforeReady>, exited_predecessor: Option<&ExitedPredecessor<'_>>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        // The name this create claimed: a step after the claim that fails withdraws it, so the next
        // attempt's fresh birth is not refused by this one's claim (lifecycles/node-create.md step 1).
        let mut claimed: Option<NodeId> = None;
        let operation = run.operation.clone();
        let r = self.create_to_ready_claimed(run, req, before_ready, exited_predecessor, before_launch, &mut claimed).await;
        if let (Err(e), Some(node_id), Some(rows)) = (&r, &claimed, self.sink.node_rows()) {
            rows.withdraw(&req.node, node_id, &operation, &format!("the create failed at {}: {}", e.step, e.reason)).await;
        }
        r
    }

    /// An earlier attempt's birth that is decided afresh lets go of the name its create claimed.
    async fn withdraw_earlier(&self, run: &Run<'_>, req: &CreateRequest, why: &str) {
        let (Some(rows), Some(Some(earlier))) = (self.sink.node_rows(), run.done.get(CreateStep::AllocateIdentity.name())) else { return };
        if let Ok(earlier) = serde_json::from_value::<Identity>(earlier.clone()) {
            rows.withdraw(&req.node, &earlier.node_id, &run.operation, why).await;
        }
    }

    /// A runtime launched by an executor that is not this one and never joined this one (`recorded`:
    /// the executor that launched it, as far as this run knows): stopped through its exact handle,
    /// so the birth can be decided afresh under this executor's endpoint. One `via-lost-executor`
    /// span; a stop that fails ends the run by name.
    async fn stop_lost_launch(&self, req: &CreateRequest, stranded: &DeploymentHandle, recorded: &str) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "rdm.node_admin.deployment.reject.via-lost-executor",
            node = %req.node,
            build_id = %req.build_id,
            attempt = req.attempt,
            step = CreateStep::DeployRuntime.name(),
            deployment_id = %stranded.deployment_id,
            recorded_executor = recorded,
            executor = %self.template.executor(),
        );
        self.provider
            .terminate(stranded, TerminationMode::Graceful { grace: self.timeouts.stop_grace })
            .instrument(span)
            .await
            .map_err(|e| PipelineError { step: CreateStep::DeployRuntime.name(), reason: format!("{}: the earlier attempt's runtime was launched by another executor and never joined; stopping it failed: {e}", req.node) })
    }

    /// The identity an earlier run of this operation claimed `req.node` with, for a run that holds
    /// none of that run's receipts: its executor was lost, and with it the only journal its receipts
    /// had reached (build.md §5.2). The claim row names the identity and the operation of the Build
    /// that allocated it ([`crate::storage::NodeRow::allocated`]). Only a birth that was never launched
    /// is continued; lifecycles/node-create.md requires exact provider proof of a runtime's state even
    /// for a deployment that never joined:
    /// - the provider proves that exact deployment was never launched ([`Found::NeverLaunched`]): this
    ///   run continues the birth under the same identity (its claim is that birth's,
    ///   `Claimed::HeldByNode`);
    /// - the provider proves it was launched and has exited ([`Found::Exited`]), as the receipt arm's
    ///   "the earlier attempt's runtime is gone": its claim is withdrawn (that birth's key only) and
    ///   the birth is decided afresh (`None`);
    /// - one runs and the birth has not joined membership: the lost executor launched it, and the
    ///   lost-executor rule ([`Self::stop_lost_launch`]) stops it through its exact handle, its claim
    ///   is withdrawn and the birth decided afresh (`None`);
    /// - one runs and the birth has joined membership: refused by name, nothing stopped, withdrawn or
    ///   minted, since this run holds none of that create's receipts past its claim.
    /// No such claim: `None`, and the identity is minted. One `via-claimed-allocation` span.
    async fn claimed_allocation(&self, run: &Run<'_>, req: &CreateRequest, rows: &crate::node_rows::NodeRows, bind_addr: SocketAddr) -> Result<Option<Identity>, PipelineError> {
        use tracing::Instrument;
        let step = CreateStep::AllocateIdentity.name();
        let Some((node_id, a)) = rows.allocation_at(&req.node, &req.build_id, &run.operation).await.map_err(|reason| PipelineError { step, reason })? else { return Ok(None) };
        let identity = Identity { node_id, incarnation: a.incarnation, supersedes: None, deployment_id: a.deployment_id };
        let data_dir = req.data_dir.clone().unwrap_or_else(|| self.template.data_root.join(format!("{}-{}", req.node, identity.node_id)));
        let spec = ResolvedNodeLaunch { node: req.node.clone(), deployment_id: identity.deployment_id.clone(), executable: self.template.executable.clone(), args: vec![], env: Default::default(), data_dir, transport: bind_addr, listeners: vec![] };
        let span = tracing::info_span!(
            "rdm.node_admin.node.resolve.via-claimed-allocation",
            node = %req.node,
            build_id = %req.build_id,
            attempt = req.attempt,
            operation = %run.operation,
            node_id = %identity.node_id,
            incarnation_id = %identity.incarnation.0,
            deployment_id = %identity.deployment_id,
            executor = %self.template.executor(),
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
        );
        match self.provider.find(&spec).instrument(span.clone()).await {
            Ok(Found::Exited) => {
                span.record("outcome", "launched-exited");
                span.in_scope(|| tracing::info!("the claim names a birth this operation allocated whose deployment the provider proves was launched and has exited: the birth is decided afresh"));
                rows.withdraw(&req.node, &identity.node_id, &run.operation, "the birth its claim names was launched by a lost executor and has exited: a fresh birth claims the name").await;
                Ok(None)
            }
            Ok(Found::NeverLaunched) => {
                span.record("outcome", "never-launched");
                span.in_scope(|| tracing::info!("the claim names a birth this operation allocated whose deployment the provider proves was never launched: this run continues it"));
                Ok(Some(identity))
            }
            Ok(Found::Running(_)) if self.observer.joined(&identity.node_id, &identity.incarnation).await.is_some() => {
                let reason = format!(
                    "{}: the claim of {} (Build {}) names the birth {} at deployment {}, whose runtime runs and has joined fabric membership; this run holds none of that create's receipts past its claim, so the birth is neither stopped, withdrawn nor decided afresh",
                    req.node, run.operation, req.build_id, identity.node_id, identity.deployment_id
                );
                span.record("outcome", "launched-and-joined").record("detail", reason.as_str());
                Err(PipelineError { step, reason })
            }
            Ok(Found::Running(stranded)) => {
                span.record("outcome", "launched-by-lost-executor");
                span.in_scope(|| tracing::info!("the claim names a birth this operation allocated whose runtime the lost executor launched: it is stopped and the birth decided afresh"));
                self.stop_lost_launch(req, &stranded, "the executor whose claim names the birth").instrument(span.clone()).await?;
                rows.withdraw(&req.node, &identity.node_id, &run.operation, "the birth its claim names was launched by a lost executor and never joined this one: a fresh birth claims the name").await;
                Ok(None)
            }
            Err(e) => {
                let reason = format!(
                    "{}: the claim of {} (Build {}) names the birth {} at deployment {}, and the provider could not prove whether a runtime of that deployment exists, so the birth is neither continued nor decided afresh: {e}",
                    req.node, run.operation, req.build_id, identity.node_id, identity.deployment_id
                );
                span.record("outcome", "unproven").record("detail", reason.as_str());
                Err(PipelineError { step, reason })
            }
        }
    }

    async fn create_to_ready_claimed(&self, run: &mut Run<'_>, req: &CreateRequest, before_ready: Option<BeforeReady>, exited_predecessor: Option<&ExitedPredecessor<'_>>, before_launch: Option<BeforeLaunch<'_>>, claimed: &mut Option<NodeId>) -> Result<Created, PipelineError> {
        let (run_build, run_operation, run_attempt) = (&req.build_id, run.operation.clone(), req.attempt);
        // A receipt names a runtime; its birth is reused only while that
        // runtime runs. A launch that has since died (its executor's mesh was
        // lost with it) is a lost birth: everything is decided afresh, a new
        // identity and endpoints the current allocator hands out.
        let executor = self.template.executor();
        let recorded_by_another = |run: &Run<'_>, step: CreateStep| run.owner.get(step.name()).is_some_and(|o| o.as_deref() != Some(executor.as_str()));
        if let Some(Some(h)) = run.done.get(CreateStep::DeployRuntime.name()) {
            let earlier = serde_json::from_value::<DeploymentHandle>(h.clone()).ok();
            let alive = match &earlier {
                Some(h) => self.provider.inspect(h).await == DeploymentStatus::Running,
                None => false,
            };
            if !alive {
                tracing::info!(node = %req.node, "the earlier attempt's runtime is gone: a fresh birth");
                self.withdraw_earlier(run, req, "the earlier attempt's runtime is gone: a fresh birth claims the name").await;
                run.done.clear();
            } else if recorded_by_another(&run, CreateStep::DeployRuntime) && !run.done.contains_key(CreateStep::WaitForMeshJoin.name()) {
                // Launched by an executor that is not this one and never joined: its only seed is
                // that executor's endpoint, which this birth can no longer rely on (the lost admin's
                // address may answer as another process). The birth is stopped and decided afresh
                // under this executor's endpoint; a birth that joined has used its seed and stands.
                let stranded = earlier.expect("alive implies a decoded handle");
                let recorded = run.owner.get(CreateStep::DeployRuntime.name()).and_then(|o| o.clone()).unwrap_or_else(|| "not recorded".into());
                self.stop_lost_launch(req, &stranded, &recorded).await?;
                self.withdraw_earlier(run, req, "the earlier attempt's runtime was launched by another executor and never joined: a fresh birth claims the name").await;
                run.done.clear();
            }
        }
        let rows = self.sink.node_rows();
        // The node binds port 0 and the operating system assigns the port; the address it
        // reports at its join is the one this admin publishes for the birth.
        let bind_ip = self
            .provider
            .bind_ip(&req.node)
            .await
            .map_err(|e| PipelineError { step: CreateStep::DeployRuntime.name(), reason: format!("{}: no address to bind: {e}", req.node) })?;
        let bind_addr = SocketAddr::new(bind_ip, 0);
        // A run holding no AllocateIdentity receipt of this operation continues the birth an earlier
        // run claimed the name for, when the claim row names one ([`Self::claimed_allocation`]); any
        // other run mints.
        let claimed_before = match &rows {
            Some(rows) if !run.done.contains_key(CreateStep::AllocateIdentity.name()) => self.claimed_allocation(run, req, rows, bind_addr).await?,
            _ => None,
        };
        let id: Identity = self
            .step(run, CreateStep::AllocateIdentity.name(), async {
                Ok(claimed_before.unwrap_or_else(|| Identity { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), supersedes: None, deployment_id: DeploymentId::mint() }))
            })
            .await?;
        let bind_listeners: Vec<(String, SocketAddr)> = req.spec.listeners.iter().map(|n| (n.to_string(), bind_addr)).collect();
        let inherits = req.data_dir.is_some();
        let data_dir = match req.data_dir.clone() {
            Some(d) => d,
            None => self.template.data_root.join(format!("{}-{}", req.node, id.node_id)),
        };
        let claiming = &mut *claimed;
        let endpoint_id: EndpointId = self.step(run, CreateStep::PrepareStorage.name(), async {
            // The name claim, by the writer of the mesh's rows (lifecycles/node-create.md step 1):
            // before anything is prepared or launched for the birth (refused, nothing is), and after
            // the AllocateIdentity receipt names its node id, so an executor lost at any point leaves
            // only a claim that receipt names: a rerun reuses the identity and its claim holds the row
            // (`Claimed::HeldByNode`), or decides afresh and withdraws it (`withdraw_earlier`).
            if let Some(rows) = &rows {
                // The identity is written into the claim row with it, so it is durable wherever the
                // claim is (`NodeRow::allocated`): a later run of this operation that never heard
                // this one's receipts continues the same birth rather than being refused by it.
                let allocated = crate::storage::Allocation { build_id: req.build_id.clone(), operation: run_operation.clone(), incarnation: id.incarnation.clone(), deployment_id: id.deployment_id.clone() };
                rows.claim(&req.node, &id.node_id, &run_operation, Some(&allocated))
                    .await
                    .map_err(|e| format!("{}: the name claim refused the birth {}; nothing is launched: {e}", req.node, id.node_id))?;
                *claiming = Some(id.node_id.clone());
            }
            // An inherited data dir keeps its key: it is loaded, never minted; both on the blocking pool.
            let key = if inherits {
                let dir = data_dir.clone();
                crate::record_store::blocking_io(move || load_transport_key(&dir)).await?
            } else {
                EndpointId(crate::admin::load_or_mint_key(&data_dir).await?.public().to_string())
            };
            if !req.held_runtimes.is_empty() {
                for row in &req.held_runtimes {
                    crate::storage::FileNodesStorage::put_runtime_in(&data_dir, &self.template.launcher.incarnation, row).await.map_err(|e| e.to_string())?;
                }
            }
            Ok(key)
        })
        .await?;
        // A PrepareStorage an earlier run of this operation receipted made the claim: a failure from
        // here withdraws it as well.
        *claimed = Some(id.node_id.clone());
        // The process provider shares the host network namespace and the
        // container provider's network exists per fabric: nothing per node.
        self.step(run, CreateStep::PrepareNetwork.name(), async { Ok(()) }).await?;
        // A replace of an exited predecessor: the successor now has its identity, key and directory.
        // The predecessor, ended with the provider's exact terminal proof before the create began,
        // hands its storage over into that directory before the successor's process exists.
        if let Some(exited_predecessor) = exited_predecessor {
            self.step(run, ReplaceStep::HandoffStorage.name(), async { self.hand_over_storage(exited_predecessor, &data_dir).await }).await?;
        }
        let launch = Launch {
            fabric: self.template.fabric.clone(),
            fabric_id: self.template.fabric_id.clone(),
            name: req.node.clone(),
            node_id: id.node_id.clone(),
            incarnation: id.incarnation.clone(),
            supersedes: None,
            bind_addr,
            listeners: bind_listeners.clone(),
            launcher: Some(self.template.launcher.clone()),
            seeds: self.template.seeds.clone(),
            data_dir: data_dir.clone(),
            store_dir: self.template.store_dir.clone(),
            mesh_id: self.template.env.get(rafka_mesh_entity::launch::ENV_MESH_ID).and_then(|v| rafka_mesh_entity::MeshId::parse(v).ok()),
            mesh_issuer: self.template.mesh_issuer.clone(),
        };
        // The launch environment; its TRACEPARENT is taken inside the
        // DeployRuntime step, so the runtime's boot span is that step's child.
        let spec_for = |traceparent: Option<String>| {
            let mut env = self.template.env.clone();
            env.extend(launch.to_env());
            if let Some(tp) = traceparent {
                env.insert("TRACEPARENT".into(), tp);
            }
            ResolvedNodeLaunch {
                node: req.node.clone(),
                deployment_id: id.deployment_id.clone(),
                executable: self.template.executable.clone(),
                args: vec![],
                env,
                data_dir: data_dir.clone(),
                transport: bind_addr,
                listeners: bind_listeners.clone(),
            }
        };
        let handle: DeploymentHandle = self
            .step(run, CreateStep::DeployRuntime.name(), async {
                let spec = spec_for(rafka_mesh_telemetry::current_traceparent());
                if let Some(h) = self.provider.find(&spec).await.map_err(|e| e.to_string())?.running() {
                    tracing::info!(deployment_id = %spec.deployment_id, "adopting the runtime this deployment already started");
                    return Ok(h);
                }
                if let Some(before) = before_launch {
                    before.await;
                }
                self.provider.spawn(&spec).await.map_err(|e| e.to_string())
            })
            .await?;
        // What this admin deployed, held before the node can report: its `JoinNode` is verified
        // against it, and the address it reports is the one published for the birth.
        // The deployment is held only while this create runs, however it ends.
        struct Forget<'j>(&'j Joins, NodeId, String, u32);
        impl Drop for Forget<'_> {
            fn drop(&mut self) {
                self.0.end(&self.1, &self.2, self.3);
            }
        }
        let _forget = Forget(self.joins, id.node_id.clone(), req.build_id.to_string(), req.attempt);
        let reported = match handle.fact() {
            Some(runtime) => Some(self.joins.expect(Deployed {
                name: req.node.clone(),
                node_id: id.node_id.clone(),
                incarnation: id.incarnation.clone(),
                supersedes: None,
                endpoint_id: endpoint_id.clone(),
                runtime,
                data_dir: data_dir.display().to_string(),
                last_accepted_name: None,
                held: false,
            })),
            None => None,
        };
        // What DeployRuntime obtained, committed one prerequisite at a time.
        let fact = handle.fact();
        let exact = || {
            fact.clone().ok_or_else(|| {
                format!(
                    "{} (deployment {}): the provider's handle names no exact runtime (a pid needs its start token, a container its immutable id)",
                    req.node, id.deployment_id
                )
            })
        };
        self.step(run, CreateStep::RegisterExactRuntimeHandle.name(), async {
            let f = exact()?;
            if f.deployment_id != id.deployment_id.0 {
                return Err(format!("{}: the handle realises deployment {}, not {}", req.node, f.deployment_id, id.deployment_id));
            }
            f.validate().map_err(|e| format!("{}: {e}", req.node))?;
            Ok(RuntimeEvidence::of(&f))
        })
        .await?;
        self.step(run, CreateStep::ResolveProviderControlDomain.name(), async {
            let f = exact()?;
            let domain = self.provider.control_domain();
            if f.control_domain != domain {
                return Err(format!(
                    "{}: its runtime is in control domain {} (fingerprint), not this provider's {}",
                    req.node,
                    f.domain_fingerprint(),
                    rafka_mesh_entity::runtime::fingerprint(&domain)
                ));
            }
            Ok(RuntimeEvidence::of(&f))
        })
        .await?;
        self.step(run, CreateStep::MakeRuntimeFactAvailableToBirth.name(), async {
            let f = exact()?;
            let (written, dir) = (f.clone(), data_dir.clone());
            // Written and read back on the blocking pool (`record_store::blocking_io`).
            let back = crate::record_store::blocking_io(move || written.write_record(&dir).map(|()| RuntimeFact::read_record(&dir))).await.map_err(|e| format!("{}: {e}", req.node))?;
            match back {
                Some(Ok(back)) if back == f => Ok(RuntimeEvidence::of(&f)),
                Some(Ok(_)) => Err(format!("{}: {} reads back another runtime", req.node, data_dir.display())),
                Some(Err(e)) => Err(format!("{}: {e}", req.node)),
                None => Err(format!("{}: {} holds no runtime record after it was written", req.node, data_dir.display())),
            }
        })
        .await?;
        let fact = exact().map_err(|reason| PipelineError { step: CreateStep::MakeRuntimeFactAvailableToBirth.name(), reason })?;
        let bound: Bound = self
            .step(run, CreateStep::WaitForBind.name(), async {
                let mut reported = reported.ok_or_else(|| format!("{}: the handle names no exact runtime to verify a join against", req.node))?;
                let until = Instant::now() + self.timeouts.bind;
                loop {
                    if let Some(d) = reported.borrow().clone() {
                        return Bound::of(&d).map_err(|e| format!("{}: {e}", req.node));
                    }
                    if let DeploymentStatus::Exited { code } = self.provider.inspect(&handle).await {
                        let detail = self.provider.failure_detail(&handle, &data_dir).await;
                        return Err(format!("runtime exited (code {code:?}) before reporting where it bound: {detail}"));
                    }
                    let left = until.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(format!("{} did not report where it bound (JoinNode) within {:?}", req.node, self.timeouts.bind));
                    }
                    let _ = tokio::time::timeout(left.min(Duration::from_millis(100)), reported.changed()).await;
                }
            })
            .await?;
        let mut node = Node::allocated(req.node.clone());
        node.node_id = id.node_id.clone();
        node.endpoint_id = Some(endpoint_id);
        node.incarnation_id = Some(id.incarnation.clone());
        node.deployment_id = Some(id.deployment_id.clone());
        node.provider = Some(self.provider.kind());
        node.data_dir = Some(data_dir.display().to_string());
        node.transport_addr = Some(bound.transport);
        node.listeners = bound.listeners.clone();
        node.status = NodeStatus::Pending;
        // One coherent birth projection: the node record (topology and data
        // dir) here; the fact itself the birth publishes with its own digest,
        // which WaitForMeshJoin holds to exactly this fact and data dir.
        self.step(run, CreateStep::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata.name(), async {
            self.sink.publish(node.clone());
            Ok(RuntimeEvidence { data_dir: node.data_dir.clone(), ..RuntimeEvidence::of(&fact) })
        })
        .await?;
        // Every create records this step: a mesh's first admin receives the fabric primary's
        // Pending here; any other birth (the admin created for an existing mesh included) records
        // that none was owed, so the Build facts say so.
        let target = node.clone();
        let _: serde_json::Value = self
            .step(run, CreateStep::ApplyMeshPending.name(), async {
                match &before_ready {
                    Some(hand_off) => hand_off(target).await.map(|state| serde_json::json!({"applied": format!("{state:?}")})),
                    None => Ok(serde_json::json!({"applied": null, "reason": "no Pending owed: not a new mesh's first admin"})),
                }
            })
            .await?;
        let published = Publication { runtime: Some(fact.clone()), data_dir: node.data_dir.clone() };
        self.step(run, CreateStep::WaitForMeshJoin.name(), async {
            let until = Instant::now() + self.timeouts.join;
            let (joined, last) = loop {
                let last = self.observer.joined(&id.node_id, &id.incarnation).await;
                if last.as_ref() == Some(&published) {
                    break (true, last);
                }
                // A runtime that exited will never join: its own last words, now, not a timeout.
                if let DeploymentStatus::Exited { code } = self.provider.inspect(&handle).await {
                    let detail = self.provider.failure_detail(&handle, &data_dir).await;
                    return Err(format!("runtime exited (code {code:?}) before joining the mesh: {detail}"));
                }
                if Instant::now() >= until {
                    break (false, last);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            match last {
                _ if joined => Ok(()),
                None => Err(format!("no membership digest from {} incarnation {} within {:?}", req.node, id.incarnation, self.timeouts.join)),
                Some(p) => Err(format!(
                    "{} incarnation {} publishes runtime {} with data dir {:?}, not the runtime {} made available with data dir {:?}",
                    req.node,
                    id.incarnation,
                    p.runtime.as_ref().map_or_else(|| "none".to_string(), |f| f.locator_fingerprint()),
                    p.data_dir,
                    fact.locator_fingerprint(),
                    published.data_dir
                )),
            }
        })
        .await?;
        self.step(run, CreateStep::WaitForNodeReady.name(), async {
            // Ready only over committed prerequisites: each has a Complete
            // receipt in the Build's state, not merely a step that ran.
            let receipts = self.builds.read_build(run_build).await.map_err(|e| format!("reading {run_build}'s receipts before Ready: {e}"))?.steps;
            let missing = ready_prerequisites_missing(&receipts, &run_operation, run_attempt);
            if !missing.is_empty() {
                return Err(format!("{} cannot be Ready: no Complete receipt for {}", req.node, missing.join(", ")));
            }
            let until = Instant::now() + self.timeouts.ready;
            loop {
                match self.observer.ready(&node).await {
                    Ok(()) => return Ok(()),
                    Err(e) if Instant::now() >= until => return Err(e),
                    Err(not_ready) => {
                        // A runtime that exited will never be Ready: its own last words, now, not a timeout.
                        if let DeploymentStatus::Exited { code } = self.provider.inspect(&handle).await {
                            let detail = self.provider.failure_detail(&handle, &data_dir).await;
                            return Err(format!("runtime exited (code {code:?}) before it was Ready ({not_ready}): {detail}"));
                        }
                        if let Some(blocker) = self.observer.ready_blocker(&node).await {
                            self.builds.note_blocked(run_build, run_attempt, &run_operation, CreateStep::WaitForNodeReady.name(), &blocker).await;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await
                    }
                }
            }
        })
        .await?;
        Ok(Created { node, handle })
    }

    /// A standalone drain (node-drain.md): `drain-node:<path>` is the drain-node command, the wait for
    /// the exact birth's `node-drained` and `Complete`, whose receipt names the birth. The process keeps
    /// running; nothing stops it and nothing is deleted.
    pub async fn drain(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("drain", &req.build_id, &req.node.name, req.attempt);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("drain-node:{name}")).await;
            let mut node = req.node.clone();
            self.seat_transfer_step(&mut run, &node).await?;
            self.drain_steps_with(&mut run, &mut node, &req.handle, true).await?;
            self.complete(&mut run).await
        }
        .instrument(span)
        .await
    }

    /// A standalone stop (node-stop.md): `stop-node:<path>` is the stop-node command, whose reply is the
    /// exact birth's `stopped`: it drained, cut its mesh connections and is parked, its process alive.
    /// The mesh primary publishes `NodeStopped` from that reply, and `Complete` names the birth and the
    /// rafka-time it stopped at. No
    /// runtime exit is asked and no departure is published.
    pub async fn stop(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("stop", &req.build_id, &req.node.name, req.attempt);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("stop-node:{name}")).await;
            let mut node = req.node.clone();
            self.seat_transfer_step(&mut run, &node).await?;
            self.stop_steps_with(&mut run, &mut node, &req.handle, StopProof::Stopped, StopEnd::Park { publish_stopped: true }).await?;
            // The `Complete` receipt opens the birth's stopped period: it commits the stop's
            // lifecycle op, the exact birth and the rafka-time it stopped at, which the stopped-node
            // garbage collector ages it from (fabric-node-lifecycle.md §6.9 Age).
            let ctx = self.command_context(&run, crate::node_commands::NodeCommand::Stop, &node);
            let op = self.command_op(&ctx, &node).map_err(|reason| PipelineError { step: RetireStep::Complete.name(), reason })?;
            let _: LifecycleOp = self.step(&mut run, RetireStep::Complete.name(), async { Ok(op) }).await?;
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// A standalone start (node-start.md): `start-node:<path>` is the start-node command to the parked
    /// birth, whose reply is `Started` once it rejoined, then the wait for it to report ready.
    pub async fn start(&self, req: &StartRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("start", &req.build_id, &req.node.name, req.attempt);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("start-node:{name}")).await;
            let mut node = req.node.clone();
            self.start_steps(&mut run, &mut node, &req.handle, &req.deployed).await?;
            self.complete(&mut run).await?;
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// A restart (node-restart.md): ONE operation, `restart-node:<path>`, on the live process, which
    /// never exits. Its `NodeRestarting` pre-event holds the birth through the stop that follows; the
    /// birth's seats move (a node-admin), it drains and stops and parks, its process alive; the same
    /// process starts again, the same node id, incarnation, endpoint key and port. The stop and the
    /// start are the stop and start steps every stop and start runs, here as steps of this one
    /// operation, and its one `Complete`, after the start, ends the restart.
    pub async fn restart(&self, req: &StartRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("restart", &req.build_id, &req.node.name, req.attempt);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("restart-node:{name}")).await;
            let mut node = req.node.clone();
            // The pre-event: the receipt first, then the publish, so the Build facts carry the hold
            // from the moment it is announced and a successor derives it from the same facts.
            let op = LifecycleOp {
                build_id: req.build_id.to_string(),
                attempt: req.attempt,
                operation: run.operation.clone(),
                node_id: node.node_id.clone(),
                incarnation: node.incarnation_id.clone().ok_or_else(|| PipelineError { step: RetireStep::NodeRestarting.name(), reason: format!("{name} has no known birth") })?,
                name: name.clone(),
                event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
            };
            let op = self.step(&mut run, RetireStep::NodeRestarting.name(), async { Ok(op.clone()) }).await?;
            self.lifecycle.restarting(&op).await;
            // The stop of the live process: it parks and stays, and its `stopped` is the stop's
            // completion. `NodeRestarting` holds it, so no `NodeStopped` is published for it.
            self.seat_transfer_step(&mut run, &node).await?;
            self.drain_steps(&mut run, &mut node, &req.handle).await?;
            self.stop_steps_with(&mut run, &mut node, &req.handle, StopProof::Stopped, StopEnd::Park { publish_stopped: false }).await?;
            // The start of the same process.
            self.start_steps(&mut run, &mut node, &req.handle, &req.deployed).await?;
            self.step(&mut run, CreateStep::Complete.name(), async {
                self.sink.publish(node.clone());
                Ok(())
            })
            .await?;
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// The start leg: the admin holds what it deployed for the birth again (its `JoinNode` is
    /// verified against it), `start-node` goes to the parked birth and its reply is `Started`, then
    /// the wait for the birth to report ready.
    async fn start_steps(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle, deployed: &Deployed) -> Result<(), PipelineError> {
        use crate::node_commands::NodeCommand;
        let ctx = self.command_context(run, NodeCommand::Start, node);
        // Held while this start runs, however it ends; a deployment that never heard its birth is remembered abandoned.
        struct Forget<'j>(&'j Joins, NodeId, String, u32);
        impl Drop for Forget<'_> {
            fn drop(&mut self) {
                self.0.end(&self.1, &self.2, self.3);
            }
        }
        let _forget = Forget(self.joins, node.node_id.clone(), run.build_id.to_string(), run.attempt);
        // The birth's join to this executor, admitted at its join door: its node.joined (node-start.md
        // step 2), which precedes everything the start does after it, node.started included.
        let mut reported = self.joins.expect(deployed.clone());
        let _ = handle;
        // The start on this exact birth, as the stop it ends is keyed: `node.joined` is taken under it.
        let start_op = node.incarnation_id.clone().map(|incarnation| LifecycleOp {
            build_id: run.build_id.to_string(),
            attempt: run.attempt,
            operation: run.operation.clone(),
            node_id: node.node_id.clone(),
            incarnation,
            name: node.name.clone(),
            event_at_rafka_ms: 0,
        });
        let admission: CommandAdmission = self
            .step(run, StartStep::StartNode.name(), async {
                node.status = NodeStatus::Pending;
                self.sink.publish(node.clone());
                // node.joined is taken when the birth's join is admitted, before the birth can report
                // node.started: a birth that runs its whole start before it answers (a node-admin's
                // hook runs inside its start) answers `Started` after its node.started.
                let take_joined = || async {
                    if let Some(op) = &start_op {
                        let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op.clone() };
                        self.lifecycle.joined(&op).await;
                    }
                };
                let send = self.observer.send_command(node, NodeCommand::Start, &ctx);
                tokio::pin!(send);
                let mut joined = false;
                let admission = loop {
                    tokio::select! {
                        biased;
                        admitted = async { reported.wait_for(Option::is_some).await.is_ok() }, if !joined => {
                            joined = true;
                            if admitted {
                                take_joined().await;
                            }
                        }
                        admission = &mut send => break admission,
                    }
                };
                match &admission {
                    CommandAdmission::Started => {
                        // `start-node` answered `Started`: the birth rejoined. A start with no join to
                        // this executor admitted (its join door saw none) takes node.joined here.
                        if !joined || reported.borrow().is_none() {
                            take_joined().await;
                        }
                        Ok(admission)
                    }
                    CommandAdmission::StartFailed { step, reason } => Err(format!("{}: node.start failed at {step}: {reason}", node.name)),
                    other => Err(format!("{}: start-node was not answered Started ({other:?}); the birth is not known to have rejoined", node.name)),
                }
            })
            .await?;
        let _ = admission;
        let until = Instant::now() + self.timeouts.ready;
        let _: () = self
            .step(run, CreateStep::WaitForNodeReady.name(), async {
                loop {
                    match self.observer.ready(node).await {
                        Ok(()) => return Ok(()),
                        Err(e) if Instant::now() >= until => return Err(e),
                        Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                    }
                }
            })
            .await?;
        node.status = NodeStatus::ReadyForTraffic;
        Ok(())
    }

    /// The `Complete` receipt of a standalone drain or start (a stop's commits its lifecycle op). It commits no output: the attempt it
    /// belongs to carried the action fenced to the exact birth (its `Opened` fact,
    /// `BuildProjection::attempt_actions`), so a later attempt's plan reads it as that birth's and
    /// plans nothing more for it.
    async fn complete(&self, run: &mut Run<'_>) -> Result<(), PipelineError> {
        self.step(run, RetireStep::Complete.name(), async { Ok(()) }).await
    }

    /// `node.delete` of the parked birth `req.node` a replacement left at its `.old` path
    /// (node-decommission.md), as ONE operation `delete-node:<path>.old`:
    ///
    /// ```text
    /// NodeDeleting -> TerminateRuntime (the provider's exact Exited) -> NodeDeleted -> Complete (the birth)
    /// ```
    ///
    /// Its stop steps are done ("For a crashed process those stop steps are already done", and a
    /// parked one already sent its `stopped`): it is sent no command. Only the provider's matching
    /// Exited is the proof, and the executor publishes NodeDeleted from it. Storage is kept: "It
    /// releases node.id and name, not storage". The lifecycle events name the birth as its own
    /// digest does (`<path>`), by its exact `(node.id, incarnation)`.
    pub async fn delete_parked(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("delete", &req.build_id, &req.node.name, req.attempt);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("delete-node:{name}")).await;
            let birth = req.node.incarnation_id.clone().ok_or_else(|| PipelineError { step: RetireStep::NodeDeleting.name(), reason: format!("{name} has no known birth") })?;
            let op = LifecycleOp {
                build_id: req.build_id.to_string(),
                attempt: req.attempt,
                operation: run.operation.clone(),
                node_id: req.node.node_id.clone(),
                incarnation: birth,
                name: PathName { old: false, ..name.clone() },
                event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
            };
            let op = self.step(&mut run, RetireStep::NodeDeleting.name(), async { Ok(op.clone()) }).await?;
            self.lifecycle.deleting(&op).await;
            self.terminate_step(&mut run, &req.node, &req.handle).await?;
            let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op };
            let op = self.step(&mut run, RetireStep::NodeDeleted.name(), self.deleted_row(&req.node, op.clone())).await?;
            self.lifecycle.deleted(&op).await;
            self.complete(&mut run).await
        }
        .instrument(span)
        .await
    }

    /// Retire `req.node` through every step (node.delete): the provider's exact terminal receipt of
    /// the birth's runtime, the proof its NodeDeleted was published from.
    pub async fn retire(&self, req: &RetireRequest) -> Result<TerminalReceipt, PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("retire", &req.build_id, &req.node.name, req.attempt);
        self.retire_steps(req).instrument(span).await
    }

    async fn retire_steps(&self, req: &RetireRequest) -> Result<TerminalReceipt, PipelineError> {
        let name = &req.node.name;
        let mut run = self.begin(&req.build_id, req.attempt, name, format!("retire-node:{name}")).await;
        let mut node = req.node.clone();
        let handle = &req.handle;
        // A crashed target is deleted only on the exit proof of exactly its birth: a proof of
        // another birth is refused by name before anything is written.
        if let Some(exit) = &req.exit {
            if exit.node_id != node.node_id || node.incarnation_id.as_ref() != Some(&exit.incarnation_id) {
                return Err(PipelineError {
                    step: RetireStep::NodeDeleting.name(),
                    reason: format!(
                        "{name}: the exit proof names birth {} at {}, the delete is of {} at {}",
                        exit.node_id,
                        exit.incarnation_id.0,
                        node.node_id,
                        node.incarnation_id.as_ref().map(|i| i.0.as_str()).unwrap_or("no known birth")
                    ),
                });
            }
        }
        // The pre-notice: this executor's journal holds its Claim for the attempt (it runs nothing
        // before that) and now records this operation; every node hears the node is being removed
        // and stops routing to it, while it stays found.
        let op = LifecycleOp {
            build_id: req.build_id.to_string(),
            attempt: req.attempt,
            operation: run.operation.clone(),
            node_id: node.node_id.clone(),
            incarnation: node.incarnation_id.clone().ok_or_else(|| PipelineError { step: RetireStep::NodeDeleting.name(), reason: format!("{name} has no known birth") })?,
            name: name.clone(),
            event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
        };
        // The receipt first, then the publish: the Build facts carry the overlay from the
        // moment it is announced, so a successor derives it from the same facts.
        let op = self.step(&mut run, RetireStep::NodeDeleting.name(), async { Ok(op.clone()) }).await?;
        self.lifecycle.deleting(&op).await;
        let terminal = if node.parked || req.exit.is_some() {
            // A delete of a stopped birth: its whole stop already ran (its seats moved, its drain
            // receipt, its connections cut, `stopped`, NodeStopped), and nothing is composed or
            // fabricated again. The delete proceeds to the exact runtime's exit proof
            // (node-decommission.md 1-2, node-stop.md 6). A crashed target likewise: its stop steps
            // are already done, and no node reply or receipt is fabricated for the exited process
            // (node-delete.md step 1); the provider's exact inspection is all that is asked.
            self.terminate_step(&mut run, &node, handle).await?
        } else {
            // The two halves of the shutdown leg are separate operations (node-drain.md, node-stop.md):
            // drain-node and its node-drained, then stop-node, its node-left reply and the provider's
            // terminal proof. A caller that puts other work between them (a replace) calls the halves.
            self.seat_transfer_step(&mut run, &node).await?;
            self.drain_steps(&mut run, &mut node, handle).await?;
            self.stop_steps_with(&mut run, &mut node, handle, StopProof::Lenient, StopEnd::Terminate { publish_stopped: true }).await?.expect("a stop that terminates yields the provider's receipt")
        };
        // The departure: the provider's inspection above is the proof. Nothing earlier (the
        // node's Leaving, its drained reply, the claim) is. A crashed target's node.deleted is
        // published from its mesh-primary's recorded exit proof (node-recovery.md sequence step 3).
        let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op };
        let exit = req.exit.as_ref();
        let op = self
            .step(&mut run, RetireStep::NodeDeleted.name(), async {
                if let Some(exit) = exit {
                    tracing::info_span!(
                        "rdm.node_admin.node.delete.via-exit-proof",
                        build_id = %req.build_id,
                        attempt = req.attempt,
                        node = %name,
                        node_id = %exit.node_id,
                        incarnation_id = %exit.incarnation_id.0,
                        exit_code = exit.exit_code.map(|c| c.to_string()).unwrap_or_else(|| "none".into()),
                        proof = %exit.proof,
                        observed_at_ms = exit.observed_at_ms,
                    )
                    .in_scope(|| tracing::info!("the exited instance is deleted on the mesh primary's exit proof"));
                }
                // The deleted birth's row goes Deleted at its path.name, its config kept, inside the
                // step and before node.deleted (node-decommission.md step 3): a node created at that
                // path.name, a recovery's create included, claims the name over it and seeds its
                // config from it (node-recovery.md "Retained").
                self.deleted_row(&node, op.clone()).await
            })
            .await?;
        self.lifecycle.deleted(&op).await;
        // node-delete.md step 3: the data dir, node-identity.json and endpoint key are retained; no
        // step of the delete touches the birth's storage. Exactly the deleted birth leaves the
        // executor's records, and stays out of its view even when the records never held it.
        self.step(&mut run, RetireStep::RemoveTopologyMembership.name(), async {
            self.sink.remove_birth(name, &op.node_id, &op.incarnation);
            Ok(())
        })
        .await?;
        self.step(&mut run, RetireStep::Complete.name(), async { Ok(()) }).await?;
        Ok(terminal)
    }

    /// The command identity for `cmd` on `node` in this run: the Build, the attempt and the
    /// operation (`drain-node:<path>` / `stop-node:<path>`).
    fn command_context(&self, run: &Run<'_>, cmd: crate::node_commands::NodeCommand, node: &Node) -> CommandContext {
        // A drain's operation key has one source: the `node.drain` object derives it, and the receipt, the wire request and the completion all carry that key.
        let operation = match cmd {
            crate::node_commands::NodeCommand::Drain => rafka_node_admin_client::DrainContext::new(rafka_node_admin_client::BuildId(run.build_id.to_string()), run.attempt, node.name.clone()).operation(),
            crate::node_commands::NodeCommand::Stop => rafka_node_admin_client::StopContext::new(rafka_node_admin_client::BuildId(run.build_id.to_string()), run.attempt, node.name.clone()).operation(),
            crate::node_commands::NodeCommand::Start => rafka_node_admin_client::StartContext::new(rafka_node_admin_client::BuildId(run.build_id.to_string()), run.attempt, node.name.clone()).operation(),
        };
        CommandContext { build_id: run.build_id.to_string(), attempt: run.attempt, operation }
    }

    /// The lifecycle op a command's gossip hook carries.
    fn command_op(&self, ctx: &CommandContext, node: &Node) -> Result<LifecycleOp, String> {
        Ok(LifecycleOp {
            build_id: ctx.build_id.clone(),
            attempt: ctx.attempt,
            operation: ctx.operation.clone(),
            node_id: node.node_id.clone(),
            incarnation: node.incarnation_id.clone().ok_or_else(|| format!("{} has no known birth", node.name))?,
            name: node.name.clone(),
            event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
        })
    }

    /// `NodeDeleted`'s work (node-decommission.md step 3, before node.deleted): the writer of the
    /// mesh's rows writes `record`'s row `Deleted` at its `path.name`, the record and its config
    /// kept; a refusal fails the step by name.
    async fn deleted_row(&self, record: &Node, op: LifecycleOp) -> Result<LifecycleOp, String> {
        if let Some(rows) = self.sink.node_rows() {
            rows.deleted(record, &op.operation).await.map_err(|e| format!("{}: the Deleted row of {} is not written: {e}", record.name, record.node_id))?;
        }
        Ok(op)
    }

    /// Resolves when this deployment's own runtime has exited.
    async fn until_exited(&self, handle: &DeploymentHandle) {
        while !matches!(self.provider.inspect(handle).await, DeploymentStatus::Exited { .. }) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// SeatTransfer (node-drain.md step 0, node-stop.md "Seat holder"): before a node-admin target
    /// drains, the seats it holds move to this executor by TakeSeats, once per run and under the
    /// run's own operation, so an identical retry of the run sends the identical call.
    /// A refusal fails the step by name and nothing is drained, a fabric seat included: it is never
    /// taken by an executor of another mesh (`HandoverDoor::take_seats` refuses it). `OtherMesh` is a
    /// holder of its own mesh's seat alone with no other admin in that mesh: the seat stays with it
    /// until its birth is gone and the ordinary election seats the next.
    async fn seat_transfer_step(&self, run: &mut Run<'_>, node: &Node) -> Result<(), PipelineError> {
        if node.kind != crate::model::NodeKind::NodeAdmin {
            return Ok(());
        }
        let ctx = CommandContext { build_id: run.build_id.to_string(), attempt: run.attempt, operation: run.operation.clone() };
        let _: crate::fabric_handover::SeatTransferOutcome = self.step(run, RetireStep::SeatTransfer.name(), self.observer.transfer_seats(node, &ctx)).await?;
        Ok(())
    }

    /// The drain half of a shutdown (node-drain.md): `drain-node` to the exact birth, then the
    /// wait for its `node-drained` completion call. The birth stays running. The completion wait
    /// ends at the lifecycle drain deadline (an arm, recorded) or at this runtime's exit.
    pub(crate) async fn drain_steps(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle) -> Result<(), PipelineError> {
        self.drain_steps_with(run, node, handle, false).await
    }

    /// [`Self::drain_steps`]; `strict` (a standalone drain) fails the wait step by name unless the
    /// matching `node-drained` call was accepted: a command not admitted, an admission
    /// acknowledgement, a deadline and an exit are each not drain completion (node-drain.md).
    async fn drain_steps_with(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle, strict: bool) -> Result<(), PipelineError> {
        use crate::node_commands::NodeCommand;
        let ctx = self.command_context(run, NodeCommand::Drain, node);
        node.status = NodeStatus::Draining;
        let admission: CommandAdmission = self
            .step(run, RetireStep::DrainNode.name(), async {
                self.sink.publish(node.clone());
                // The accepted command's hook, then the command.
                let op = self.command_op(&ctx, node)?;
                self.lifecycle.draining(&op).await;
                Ok(self.observer.send_command(node, NodeCommand::Drain, &ctx).await)
            })
            .await?;
        let _: Completion = self
            .step(run, RetireStep::AwaitNodeDrained.name(), async {
                if !admission.admitted() {
                    // Admission is not drain completion, and a command the birth never held has
                    // none to follow (node-drain.md): a standalone drain is not complete.
                    if strict {
                        return Err(format!("{}: drain-node was not admitted ({admission:?}); no node-drained can follow, the drain is not complete", node.name));
                    }
                    return Ok(Completion::NotAwaited { admission: format!("{admission:?}") });
                }
                let c = tokio::select! {
                    c = self.observer.await_completion(node, NodeCommand::Drain, &ctx, self.timeouts.drain) => c,
                    () = self.until_exited(handle) => Completion::RuntimeExited,
                };
                match (strict, &c) {
                    (true, c) if *c != Completion::Received => Err(format!("{}: drain-node was admitted but no matching node-drained call was accepted ({c:?}); the drain is not complete", node.name)),
                    _ => Ok(c),
                }
            })
            .await?;
        Ok(())
    }

    /// The stop half of a shutdown (node-stop.md): `stop-node` to the exact birth (no implicit
    /// drain), the wait for node-left (the `Stopped` reply of that call), the mesh primary's
    /// `NodeStopped` from that matching reply, then, by `end`, the parked process or the provider's
    /// stop and inspection of the exact runtime, whose `Exited` alone is terminal proof. `proof` says
    /// what the wait step must have seen to complete (a standalone
    /// stop and a replace's predecessor stop fail it by name unless stop-node was admitted and the
    /// birth's `stopped` reply received).
    /// `end` says what follows: the process parks, or the provider ends it.
    async fn stop_steps_with(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle, proof: StopProof, end: StopEnd) -> Result<Option<TerminalReceipt>, PipelineError> {
        use crate::node_commands::NodeCommand;
        let ctx = self.command_context(run, NodeCommand::Stop, node);
        node.status = NodeStatus::Leaving;
        let publish_stopped = matches!(end, StopEnd::Park { publish_stopped: true } | StopEnd::Terminate { publish_stopped: true });
        // `stop-node` is held until the birth is parked, and its reply is `stopped`: the birth drained,
        // cut its mesh connections and entered Leaving. The command is `Stopped` or it is not complete.
        let admission: CommandAdmission = self
            .step(run, RetireStep::StopNode.name(), async {
                self.sink.publish(node.clone());
                let op = self.command_op(&ctx, node)?;
                self.lifecycle.leaving(&op).await;
                Ok(self.observer.send_command(node, NodeCommand::Stop, &ctx).await)
            })
            .await?;
        let _: Completion = self
            .step(run, RetireStep::AwaitNodeLeft.name(), async {
                if !admission.admitted() {
                    // A command the birth never held has no left reply to follow (node-stop.md): a
                    // standalone stop and a replace's predecessor stop are not complete.
                    if proof == StopProof::Stopped {
                        return Err(format!("{}: stop-node was not admitted ({admission:?}); no left reply can follow, the stop is not complete", node.name));
                    }
                    return Ok(Completion::NotAwaited { admission: format!("{admission:?}") });
                }
                let c = tokio::select! {
                    c = self.observer.await_completion(node, NodeCommand::Stop, &ctx, self.timeouts.drain) => c,
                    () = self.until_exited(handle) => Completion::RuntimeExited,
                };
                // The mesh primary publishes the departure from the matching `stopped`; the node gossips nothing.
                if publish_stopped && c == Completion::Received {
                    let op = self.command_op(&ctx, node)?;
                    self.lifecycle.stopped(&op).await;
                }
                match (proof != StopProof::Lenient, &c) {
                    (true, c) if *c != Completion::Received => Err(format!("{}: stop-node was admitted but its left reply was not received ({c:?}); the stop is not complete", node.name)),
                    _ => Ok(c),
                }
            })
            .await?;
        match end {
            StopEnd::Park { .. } => Ok(None),
            StopEnd::Terminate { .. } => Ok(Some(self.terminate_step(run, node, handle).await?)),
        }
    }

    /// `TerminateRuntime`: the provider's stop and inspection of the exact runtime; only its `Exited`
    /// is terminal proof, and the receipt binds the exact birth and runtime. A replacement of a birth
    /// the provider already proves terminal asks only this.
    pub(crate) async fn terminate_step(&self, run: &mut Run<'_>, node: &Node, handle: &DeploymentHandle) -> Result<TerminalReceipt, PipelineError> {
        let name = node.name.clone();
        self.step(run, RetireStep::TerminateRuntime.name(), async {
            self.provider
                .terminate(handle, TerminationMode::Graceful { grace: self.timeouts.stop_grace })
                .await
                .map_err(|e| e.to_string())?;
            // Only an exact inspection that says the runtime exited is terminal proof.
            let exit_code = match self.provider.inspect(handle).await {
                DeploymentStatus::Running => return Err(format!("{name} still runs after the stop ladder")),
                DeploymentStatus::Unknown => return Err(format!("{name}: the provider cannot inspect its runtime after the stop ladder; no terminal proof")),
                DeploymentStatus::Exited { code } => code,
            };
            // Exited is not yet released: the kernel frees a dead process's sockets after the
            // process is gone, so its advertised addresses can read as held for a few tens of
            // milliseconds more. A successor at the same addresses (a restart) must not race that:
            // the operating system is asked, as `WaitForBind` asks it, until nothing holds them.
            let mut held: Vec<(String, SocketAddr, super::endpoint::SlotTransport)> = Vec::new();
            if let Some(t) = node.transport_addr {
                held.push(("transport".into(), t, super::endpoint::SlotTransport::Udp));
            }
            held.extend(node.listeners.iter().map(|(n, a)| (n.clone(), *a, super::endpoint::SlotTransport::Tcp)));
            let until = Instant::now() + Duration::from_secs(3);
            // Still held by the exited runtime: not released per `/proc` (a port another live
            // process was handed meanwhile, as estates sharing one block do, is released).
            // `/proc` is read on the blocking pool; a table it cannot read fails the step by name,
            // never reads as released.
            let exited_pid = handle.pid;
            let started = Instant::now();
            loop {
                held = crate::record_store::blocking_io(move || {
                    let mut still = Vec::with_capacity(held.len());
                    for h in held {
                        if !super::endpoint::released_by(h.1, h.2, exited_pid).map_err(|e| format!("cannot prove {} {} released: {e}", h.0, h.1))? {
                            still.push(h);
                        }
                    }
                    Ok::<_, String>(still)
                })
                .await
                .map_err(|e| format!("{name} exited, but {e}"))?;
                if held.is_empty() {
                    break;
                }
                if Instant::now() >= until {
                    let elapsed = started.elapsed();
                    let names = crate::record_store::blocking_io(move || {
                        held.iter()
                            .map(|h| {
                                let who = super::endpoint::port_holder(h.1, h.2).unwrap_or_else(|e| format!("holder unknown: {e}"));
                                format!("{} {} ({who})", h.0, h.1)
                            })
                            .collect::<Vec<String>>()
                    })
                    .await;
                    return Err(format!("{name} exited, but the operating system still holds {} after {elapsed:?}", names.join(", ")));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tracing::info!(node = %name, released_after_ms = started.elapsed().as_millis() as u64, "the runtime exited and the operating system released its addresses");
            Ok(TerminalReceipt {
                node: name.to_string(),
                node_id: node.node_id.to_string(),
                incarnation_id: node.incarnation_id.as_ref().map(|i| i.0.clone()).ok_or_else(|| format!("{name} has no known birth: the exit proves no birth"))?,
                runtime: handle.fact().as_ref().map(RuntimeProof::of),
                exit_code,
            })
        })
        .await
    }

    /// The directory of `node`'s birth as it is now: the data dir its record names, else the
    /// root's `<path>-<node_id>`.
    pub(crate) fn birth_dir(&self, node: &Node) -> PathBuf {
        node.data_dir.as_deref().map(PathBuf::from).unwrap_or_else(|| self.template.data_root.join(format!("{}-{}", node.name, node.node_id)))
    }

    /// Replace `req.predecessor` with a new node at its path, as ONE operation `replace-node:<path>`.
    ///
    /// A live predecessor follows Rafka's replace recipe (node-replace.md 1-3):
    ///
    /// ```text
    /// DrainNode -> AwaitNodeDrained -> StopNode -> AwaitNodeLeft (NodeStopped)   (node[1].stop: parked)
    ///   -> RenamePredecessor                                                    (node[1].update: <path>.old)
    ///   -> AllocateIdentity -> PrepareStorage -> PrepareNetwork -> DeployRuntime -> ...
    ///   -> WaitForBind -> ... -> WaitForMeshJoin -> WaitForNodeReady -> Complete (node.create)
    /// ```
    ///
    /// The predecessor stays parked at `<path>.old`, its process, incarnation and data kept: the data
    /// transfer is Rafka's own step after the create ("Rafka transfers its data as its own recipe
    /// step. RDM interprets no application data."), and the predecessor's delete, or its rollback
    /// (rename back, start), is a later step of the recipe. A replace publishes no NodeDeleting or
    /// NodeDeleted ("delete retains the existing NodeDeleting/NodeDeleted gossip names. There is no new
    /// replace gossip frame.").
    ///
    /// A predecessor the provider already proves terminal is sent nothing (node-recovery.md: no RPC
    /// can be required from the proven-terminal old birth), and the replace deletes the exited
    /// instance before it creates: NodeDeleting -> TerminateRuntime -> RenamePredecessor -> NodeDeleted (its
    /// Deleted row at `<path>.old`) -> AllocateIdentity -> PrepareStorage -> PrepareNetwork ->
    /// HandoffStorage -> DeployRuntime -> ... -> WaitForNodeReady -> Complete. It was never stopped, so
    /// it is renamed only on the provider's exact terminal proof (node-update.md, "Rename
    /// precondition"), and the old identity's delete releases no storage (node-delete.md "Storage"):
    /// its directory keeps what HandoffStorage did not move, its identity files and endpoint key.
    ///
    /// `before_ready` is the successor's `ApplyMeshPending` work (a mesh's first admin); `after_ready`
    /// commits the successor's lifecycle transition once it is ready.
    pub async fn replace<F, Fut>(&self, req: &ReplaceRequest, before_ready: Option<BeforeReady>, after_ready: F) -> Result<Created, PipelineError>
    where
        F: FnOnce(Node) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        use tracing::Instrument;
        let span = self.pipeline_span("replace", &req.build_id, &req.predecessor.name, req.attempt);
        self.replace_steps(req, before_ready, after_ready).instrument(span).await
    }

    async fn replace_steps<F, Fut>(&self, req: &ReplaceRequest, before_ready: Option<BeforeReady>, after_ready: F) -> Result<Created, PipelineError>
    where
        F: FnOnce(Node) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let name = &req.predecessor.name;
        let mut run = self.begin(&req.build_id, req.attempt, name, format!("replace-node:{name}")).await;
        let mut old = req.predecessor.clone();
        let birth = old.incarnation_id.clone().ok_or_else(|| PipelineError { step: RetireStep::NodeDeleting.name(), reason: format!("{name} has no known birth") })?;
        // The exact old birth, named by every receipt that identifies it (an exited predecessor's
        // NodeDeleting and NodeDeleted, and the rename of either).
        let op = LifecycleOp {
            build_id: req.build_id.to_string(),
            attempt: req.attempt,
            operation: run.operation.clone(),
            node_id: old.node_id.clone(),
            incarnation: birth.clone(),
            name: name.clone(),
            event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
        };
        // A birth the provider already proves terminal is sent nothing (node-recovery.md: no RPC can
        // be required from the proven-terminal old birth). An earlier attempt that announced its
        // delete (its NodeDeleting receipt) continues as the replace of an exited predecessor.
        let exited = run.done.contains_key(RetireStep::NodeDeleting.name()) || matches!(self.provider.inspect(&req.handle).await, DeploymentStatus::Exited { .. });
        if exited {
            // The receipt first, then the publish: the Build facts carry the overlay from the moment
            // it is announced, so a successor executor derives it from the same facts.
            let op = self.step(&mut run, RetireStep::NodeDeleting.name(), async { Ok(op.clone()) }).await?;
            self.lifecycle.deleting(&op).await;
            // The provider's exact terminal proof, before the predecessor gives up its name: it was
            // never stopped, so it is renamed only on that proof (node-update.md, "Rename precondition").
            self.terminate_step(&mut run, &old, &req.handle).await?;
        } else {
            // The recipe's first step (node-replace.md 1): the complete stop leaves the predecessor
            // parked, and the stop's departure (`NodeStopped`) drops it from every book, before it
            // gives up its name and before anything of the successor exists.
            self.seat_transfer_step(&mut run, &old).await?;
            self.drain_steps(&mut run, &mut old, &req.handle).await?;
            // The complete stop (node-stop.md): stop-node admitted and `stopped` received, or the
            // step fails by name; a predecessor that is not stopped keeps its path.name, and a birth
            // that does not answer goes to node recovery (node-recovery.md).
            self.stop_steps_with(&mut run, &mut old, &req.handle, StopProof::Stopped, StopEnd::Park { publish_stopped: true }).await?;
        }
        // node-replace.md 2: the predecessor gives up its name, renamed `<path>.old`, under the rename
        // precondition (node-update.md): parked by its `stopped` reply, or ended on the provider's
        // exact terminal proof above. The receipt names the exact old birth.
        let proof = if exited { "exited" } else { "stopped" };
        let to = format!("{name}.old");
        self.step(&mut run, ReplaceStep::RenamePredecessor.name(), async {
            tracing::info_span!(
                "rdm.node_admin.node.update.via-replace-rename",
                build_id = %req.build_id,
                attempt = req.attempt,
                node = %name,
                node_id = %old.node_id,
                incarnation_id = %birth.0,
                from = %name,
                to = %to,
                proof,
            )
            .in_scope(|| tracing::info!("the predecessor gives up its path.name: the successor is created at it"));
            // node1.update(.old): the writer of the mesh's rows moves the predecessor's row first.
            if let Some(rows) = self.sink.node_rows() {
                rows.moved_to_old(&old, &op.operation).await.map_err(|e| format!("{name}: the predecessor {} keeps its path.name: {e}", old.node_id))?;
            }
            self.sink.rename_predecessor(name, &old.node_id, &birth);
            Ok(op.clone())
        })
        .await?;
        if exited {
            // The exited predecessor's delete, then the create: after
            // the provider's exact terminal proof and the rename, the old identity's departure and its
            // Deleted row at `<path>.old`, before anything of the successor exists. The row is written
            // while this executor is the writer of the mesh's rows: a mesh's only admin, replaced by
            // the fabric-primary as the acting primary of a mesh with no primary, hands the mesh's
            // seat to its successor at the successor's readiness, and from then on that successor
            // writes the mesh's rows (R13). The delete keeps the predecessor's storage (node-delete.md
            // "Storage"); the successor's create takes over only what HandoffStorage moves.
            let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op.clone() };
            let parked = Node { name: name.renamed(), ..old.clone() };
            let op = self.step(&mut run, RetireStep::NodeDeleted.name(), self.deleted_row(&parked, op)).await?;
            self.lifecycle.deleted(&op).await;
        }
        // node-replace.md 3: node.create at the freed name, a new node.id, incarnation and directory.
        let creq = CreateRequest { build_id: req.build_id.clone(), attempt: req.attempt, node: name.clone(), spec: req.spec, data_dir: None, held_runtimes: req.held_runtimes.clone() };
        let exited_predecessor = ExitedPredecessor { build_id: &req.build_id, predecessor: &old };
        let mut created = self.create_to_ready(&mut run, &creq, before_ready, exited.then_some(&exited_predecessor), None).await?;
        created.node.status = NodeStatus::ReadyForTraffic;
        after_ready(created.node.clone()).await.map_err(|reason| PipelineError { step: CreateStep::WaitForNodeReady.name(), reason })?;
        self.step(&mut run, CreateStep::Complete.name(), async {
            self.sink.publish(created.node.clone());
            Ok(())
        })
        .await?;
        Ok(created)
    }

    /// node.update of `req.node` in node recovery (node-recovery.md sequence step 6), as ONE
    /// operation `update-node:<path>`: `UpdateNode`, the caller-owned meta of the exited birth
    /// `req.from` copied onto the new node (only the caller-owned group moves:
    /// `NodeMeta::with_caller_owned_of`), then `Complete`. The new node is the one its create made
    /// at the exited birth's path.name, never the exited birth.
    pub async fn update(&self, req: &UpdateRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("update", &req.build_id, &req.node.name, req.attempt);
        self.update_steps(req).instrument(span).await
    }

    async fn update_steps(&self, req: &UpdateRequest) -> Result<(), PipelineError> {
        let name = &req.node.name;
        let mut run = self.begin(&req.build_id, req.attempt, name, format!("update-node:{name}")).await;
        let (onto, from) = (&req.node, &req.from);
        self.step(&mut run, UpdateStep::UpdateNode.name(), async {
            if onto.node_id == from.node_id || onto.incarnation_id.is_none() {
                return Err(format!("{name}: node.update copies the exited birth {}'s caller-owned meta onto the node its create made there, and the node held at the path is {} at {}", from.node_id, onto.node_id, onto.incarnation_id.as_ref().map(|i| i.0.as_str()).unwrap_or("no known birth")));
            }
            // The caller-owned group as this admin's records hold it of the exited birth: the node
            // record carries no caller-owned field, so the group is its default and nothing is written.
            let copied = crate::node_update::meta_of(onto).with_caller_owned_of(&crate::node_update::meta_of(from));
            tracing::info!(node = %name, from = %from.node_id, onto = %onto.node_id, otel_debug = copied.otel_debug, written = false, "node.update onto the new node: the caller-owned group of the exited birth as held, no row written");
            Ok(())
        })
        .await?;
        self.step(&mut run, UpdateStep::Complete.name(), async { Ok(()) }).await
    }

    /// The exited predecessor's storage goes into the successor's directory: after the successor's
    /// identity exists and before its process does.
    /// The top-level directories of every directory that holds this operation's storage (the
    /// predecessor's, and a successor an earlier attempt of this operation left behind, named by its
    /// `AllocateIdentity` receipt in this Build) move into the successor's directory. No other
    /// directory at the path is read: a birth deleted earlier at the path keeps its data dir
    /// (node-delete.md "Storage") and is not this operation's. Files (the identity, the transport key,
    /// the runtime record) never move. What the Build's `StorageMeta` for the path says decides: only
    /// persistent storage is handed over, and a node-admin's records are seeded from its maker at join,
    /// never inherited.
    async fn hand_over_storage(&self, exited_predecessor: &ExitedPredecessor<'_>, successor_dir: &std::path::Path) -> Result<(), String> {
        let predecessor = exited_predecessor.predecessor;
        let name = &predecessor.name;
        let build = self.builds.read_build(exited_predecessor.build_id).await.ok();
        let storage = build.as_ref().and_then(|b| b.topology.meshes.get(&name.mesh).and_then(|m| m.meta(name).map(|meta| meta.storage)));
        let withheld = match storage {
            _ if name.kind == NodeKind::NodeAdmin => Some("a node-admin's records are seeded from its maker when it joins, never inherited from its predecessor"),
            Some(StorageMeta::Persistent { .. }) => None,
            Some(StorageMeta::Ephemeral) => Some("ephemeral storage: nothing survives a replacement"),
            None => Some("the accepted Build holds no storage meta for this path: storage is never moved on missing evidence"),
        };
        if let Some(reason) = withheld {
            tracing::info!(node = %name, to = %successor_dir.display(), reason, "no storage was handed over");
            return Ok(());
        }
        let current = self.birth_dir(predecessor);
        let mut candidates = vec![current.clone()];
        let operation = format!("replace-node:{name}");
        for r in build.iter().flat_map(|b| b.steps.iter()).filter(|r| r.operation == operation && r.step == CreateStep::AllocateIdentity.name() && r.outcome == StepOutcome::Complete) {
            let id: Identity = r
                .output
                .clone()
                .ok_or_else(|| format!("{name}: the AllocateIdentity receipt of attempt {} of {operation} (Build {}) carries no identity", r.attempt, exited_predecessor.build_id))
                .and_then(|o| serde_json::from_value(o).map_err(|e| format!("{name}: the AllocateIdentity receipt of attempt {} of {operation} (Build {}) is not an identity: {e}", r.attempt, exited_predecessor.build_id)))?;
            let dir = self.template.data_root.join(format!("{name}-{}", id.node_id));
            if dir != successor_dir && !candidates.contains(&dir) {
                candidates.push(dir);
            }
        }
        let sources = storage_sources(candidates).await.map_err(|e| format!("{name}: {e}"))?;
        // A directory is moved only once the runtime that used it is gone: the predecessor's is
        // proven terminal by `TerminateRuntime`, an earlier attempt's successor is checked here.
        for source in &sources {
            if let Some(fact) = &source.runtime {
                if let Ok(h) = super::provider::adopt(self.provider, fact) {
                    if self.provider.inspect(&h).await == DeploymentStatus::Running {
                        return Err(format!("{name}: {} holds storage of this path and its runtime (deployment {}) still runs; it is not moved", source.dir.display(), fact.deployment_id));
                    }
                }
            }
        }
        let moved = move_storage(sources, successor_dir).await.map_err(|e| format!("{name}: {e}"))?;
        tracing::info!(node = %name, to = %successor_dir.display(), handed_over = ?moved, "the predecessor's storage was handed to the successor's empty directory");
        Ok(())
    }
}

/// A directory that holds a path's storage: the subdirectories it holds, and the runtime record
/// of the runtime that used it (`None`: none is written).
#[derive(Debug)]
struct StorageSource {
    dir: PathBuf,
    holders: Vec<PathBuf>,
    runtime: Option<RuntimeFact>,
}

/// Of the `candidates` (an operation's own directories: its predecessor's and each earlier
/// attempt's successor's), those that hold storage, each with the subdirectories it holds and its
/// runtime record; a candidate that is gone or holds none is skipped. `Err` names the directory or
/// record that could not be read.
async fn storage_sources(candidates: Vec<PathBuf>) -> Result<Vec<StorageSource>, String> {
    crate::record_store::blocking_io(move || {
        // The subdirectories `dir` holds; `None` when it is gone (never made, or removed).
        let subdirs = |dir: &std::path::Path| -> Result<Option<Vec<PathBuf>>, String> {
            let named = |e: std::io::Error| format!("{}: {e}", dir.display());
            let entries = match std::fs::read_dir(dir) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(named(e)),
            };
            let mut out = Vec::new();
            for entry in entries {
                let entry = entry.map_err(named)?;
                if entry.file_type().map_err(named)?.is_dir() {
                    out.push(entry.path());
                }
            }
            Ok(Some(out))
        };
        let mut out = Vec::new();
        for dir in candidates {
            let Some(holders) = subdirs(&dir)? else { continue };
            if holders.is_empty() {
                continue;
            }
            let runtime = RuntimeFact::read_record(&dir).transpose()?;
            out.push(StorageSource { dir, holders, runtime });
        }
        Ok(out)
    })
    .await
}

/// Move every holder of `sources` into `successor`; the names moved. `Err` names the move that
/// failed, or the holder whose name the successor already holds.
async fn move_storage(sources: Vec<StorageSource>, successor: &std::path::Path) -> Result<Vec<String>, String> {
    let successor = successor.to_path_buf();
    crate::record_store::blocking_io(move || {
        let mut moved = Vec::new();
        for dir in sources.into_iter().flat_map(|s| s.holders) {
            let file = dir.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
            let to = successor.join(&file);
            if to.try_exists().map_err(|e| format!("{}: {e}", to.display()))? {
                return Err(format!("{} would move to {}, which already exists in the successor's directory", dir.display(), to.display()));
            }
            std::fs::rename(&dir, &to).map_err(|e| format!("moving {} to {}: {e}", dir.display(), to.display()))?;
            moved.push(file);
        }
        Ok(moved)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_store::{op_waits_on_the_blocking_pool, runtime_runs_while_a_fifo_is_read, tempdir};

    /// CONTRACT: the Day-0 adoption writes its receipts on the blocking pool, never on a runtime
    /// worker.
    #[test]
    fn day_zero_adoption_writes_its_receipts_on_the_blocking_pool() {
        let dir = tempdir("adopt-pool");
        let (waited, a) = op_waits_on_the_blocking_pool(Box::pin(async move { dir }), |dir| {
            Box::pin(async move { CurrentRuntimeAdoption::begin(&"mesh1.admin.1".parse().unwrap(), &dir, &DeploymentId::mint()).await.map(|_| ()) })
        });
        assert!(waited, "the adoption finished on the runtime's thread while the blocking pool was held");
        a.unwrap();
    }

    /// CONTRACT: a receipt the adoption cannot write fails its step naming the file: a lost
    /// receipt never surfaces later as a Ready blocker for a step that completed.
    #[tokio::test]
    async fn an_adoption_receipt_that_cannot_be_written_fails_its_step_naming_the_file() {
        let dir = tempdir("adopt-unwritable");
        std::fs::create_dir_all(dir.join(ADOPTION_RECEIPTS)).unwrap();
        let err = CurrentRuntimeAdoption::begin(&"mesh1.admin.1".parse().unwrap(), &dir, &DeploymentId::mint()).await.map(|_| ()).expect_err("an unwritten receipt is a failed step");
        assert!(err.reason.contains(ADOPTION_RECEIPTS), "{err}");
    }

    /// CONTRACT: the Ready gate reads the adoption receipts on the blocking pool.
    #[test]
    fn the_adoption_receipts_are_read_on_the_blocking_pool() {
        let dir = tempdir("adopt-fifo");
        let fifo = dir.join(ADOPTION_RECEIPTS);
        let (heard, missing) = runtime_runs_while_a_fifo_is_read(fifo, b"[]".to_vec(), Box::pin(async move { adoption_missing(&dir).await }));
        assert!(heard, "the runtime stopped while the receipts were read");
        assert_eq!(missing.unwrap().len(), 4);
    }

    /// CONTRACT: receipts that cannot be read are an error naming the file, never "every step
    /// missing".
    #[tokio::test]
    async fn unreadable_adoption_receipts_are_an_error_naming_the_file() {
        let dir = tempdir("adopt-unreadable");
        std::fs::create_dir_all(dir.join(ADOPTION_RECEIPTS)).unwrap();
        let err = adoption_missing(&dir).await.expect_err("unreadable receipts are not missing ones");
        assert!(err.contains(ADOPTION_RECEIPTS), "{err}");
        std::fs::remove_dir(dir.join(ADOPTION_RECEIPTS)).unwrap();
        std::fs::write(dir.join(ADOPTION_RECEIPTS), b"not json").unwrap();
        let err = adoption_missing(&dir).await.expect_err("unparsable receipts are not missing ones");
        assert!(err.contains(ADOPTION_RECEIPTS), "{err}");
    }

    /// CONTRACT: the storage hand-over looks at the disk on the blocking pool.
    #[test]
    fn the_storage_look_waits_on_the_blocking_pool() {
        let root = tempdir("handover-pool");
        let current = root.join("mesh1.rpc.1-n1");
        std::fs::create_dir_all(current.join("data")).unwrap();
        let (waited, found) = op_waits_on_the_blocking_pool(Box::pin(async move { (root, current) }), |(root, current)| {
            Box::pin(async move { storage_sources(vec![current, root.join("mesh1.rpc.1-n2")]).await })
        });
        assert!(waited, "the look finished on the runtime's thread while the blocking pool was held");
        assert_eq!(found.unwrap().len(), 1);
    }

    /// CONTRACT: a storage directory whose runtime record cannot be read is never moved: the
    /// record is what proves its runtime gone, and the refusal names the file.
    #[tokio::test]
    async fn storage_whose_runtime_record_cannot_be_read_is_refused_naming_the_file() {
        let root = tempdir("handover-record");
        let current = root.join("mesh1.rpc.1-n1");
        std::fs::create_dir_all(current.join("data")).unwrap();
        std::fs::create_dir_all(current.join(rafka_mesh_entity::runtime::RUNTIME_FILE)).unwrap();
        let err = storage_sources(vec![current.clone()]).await.expect_err("an unreadable record proves nothing");
        assert!(err.contains(rafka_mesh_entity::runtime::RUNTIME_FILE), "{err}");
    }

    /// CONTRACT: a storage directory that cannot be read is an error naming it, never "holds
    /// nothing"; only a candidate directory never made is skipped.
    #[tokio::test]
    async fn a_storage_directory_that_cannot_be_read_is_an_error_naming_it() {
        let root = tempdir("handover-dir");
        let never_made = root.join("mesh1.rpc.1-n0");
        assert!(storage_sources(vec![never_made]).await.unwrap().is_empty());
        let current = root.join("mesh1.rpc.1-n1");
        std::fs::write(&current, b"a file where the birth's directory should be").unwrap();
        let err = storage_sources(vec![current.clone()]).await.expect_err("an unreadable directory holds something unknown");
        assert!(err.contains(&current.display().to_string()), "{err}");
    }

    fn receipt(attempt: u32, step: &str, outcome: StepOutcome) -> BuildStepReceipt {
        BuildStepReceipt { build_id: BuildId("bld-1".into()), attempt, operation: "create-node:mesh1.rpc.1".into(), step: step.into(), outcome, output: None, executor: None }
    }

    #[test]
    fn ready_requires_a_complete_receipt_for_each_runtime_prerequisite_of_this_run() {
        let op = "create-node:mesh1.rpc.1";
        let all: Vec<BuildStepReceipt> = READY_PREREQUISITES.iter().map(|s| receipt(1, s.name(), StepOutcome::Complete)).collect();
        assert!(ready_prerequisites_missing(&all, op, 1).is_empty());
        // One lost receipt is named.
        let lost: Vec<_> = all.iter().filter(|r| r.step != "MakeRuntimeFactAvailableToBirth").cloned().collect();
        assert_eq!(ready_prerequisites_missing(&lost, op, 1), vec!["MakeRuntimeFactAvailableToBirth"]);
        // Another operation's receipts are not this run's.
        let other: Vec<_> = all.iter().cloned().map(|mut r| {
            r.operation = "create-node:mesh1.rpc.2".into();
            r
        }).collect();
        assert_eq!(ready_prerequisites_missing(&other, op, 1).len(), 4);
        // A failed attempt voids what came before it: attempt 3 after a failed
        // attempt 2 cannot stand on attempt 1's receipts.
        let mut failed = all.clone();
        failed.push(receipt(2, "WaitForBind", StepOutcome::Failed { reason: "exited".into() }));
        assert_eq!(ready_prerequisites_missing(&failed, op, 3).len(), 4);
        // An attempt cut short (no failure) hands its receipts on.
        assert!(ready_prerequisites_missing(&all, op, 2).is_empty());
        // A finished run hands nothing on: attempt 5 of the same operation (a second restart
        // of one node) stands on its own receipts only.
        let mut finished = all.clone();
        finished.push(receipt(1, CreateStep::Complete.name(), StepOutcome::Complete));
        assert_eq!(ready_prerequisites_missing(&finished, op, 5).len(), 4);
        assert!(ready_prerequisites_missing(&finished, op, 1).is_empty(), "the finished run itself is ready");
        // A Failed receipt is no Complete one.
        let mut half = lost.clone();
        half.push(receipt(1, "MakeRuntimeFactAvailableToBirth", StepOutcome::Failed { reason: "disk".into() }));
        assert_eq!(ready_prerequisites_missing(&half, op, 1), vec!["MakeRuntimeFactAvailableToBirth"]);
    }

    #[tokio::test]
    async fn day_zero_adopts_this_process_with_one_receipt_per_step() {
        let dir = std::env::temp_dir().join(format!("rafka-adopt-current-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let node: PathName = "mesh1.admin.1".parse().unwrap();
        let a = CurrentRuntimeAdoption::begin(&node, &dir, &DeploymentId::mint()).await.unwrap();
        assert_eq!(adoption_missing(&dir).await.unwrap(), vec!["PublishRuntimeFactAndCurrentRuntimeMetadata"], "Ready waits for publication");
        let published = std::sync::Arc::new(std::sync::Mutex::new(None));
        let into = published.clone();
        let fact = a.publish(move |f, d| *into.lock().unwrap() = Some((f.clone(), d))).await.unwrap();
        assert_eq!(*published.lock().unwrap(), Some((fact.clone(), dir.display().to_string())));
        assert!(adoption_missing(&dir).await.unwrap().is_empty());
        fact.verify_is_this_runtime().unwrap();
        let receipts: Vec<AdoptionReceipt> = serde_json::from_slice(&std::fs::read(dir.join(ADOPTION_RECEIPTS)).unwrap()).unwrap();
        assert_eq!(receipts.iter().map(|r| r.step.as_str()).collect::<Vec<_>>(), AdoptStep::ORDER.iter().map(|s| s.name()).collect::<Vec<_>>());
        // Evidence by fingerprint: no pid in a receipt.
        let text = std::fs::read_to_string(dir.join(ADOPTION_RECEIPTS)).unwrap();
        assert!(!text.contains("\"pid\""), "{text}");
        // No receipts: nothing is adopted.
        std::fs::remove_file(dir.join(ADOPTION_RECEIPTS)).unwrap();
        assert_eq!(adoption_missing(&dir).await.unwrap().len(), 4);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
