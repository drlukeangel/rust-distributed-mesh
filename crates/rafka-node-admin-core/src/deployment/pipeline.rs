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
//!   -> StopNode (stop-node, no implicit drain) -> AwaitNodeLeft (bounded, likewise)
//!   -> TerminateRuntime (the provider's stop and exact inspection: only `Exited` is terminal proof)
//!   -> ReleaseStorage (a removal: by the Build's StorageMeta)
//!   -> RemoveTopologyMembership -> Complete
//! ```
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
use super::provider::{DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use crate::build::BuildId;
use crate::build_state::{BuildStateAdapter, BuildStepReceipt, StepOutcome};
use crate::model::{DeploymentId, EndpointId, IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::meta::{PersistentRetireDisposition, StorageMeta};
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
    /// Mint the birth's incarnation and deployment id, and the node id unless the birth restarts a node.
    AllocateIdentity,
    /// Ensure the node's transport key in its data directory and hand a node-admin its held runtime rows.
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
    /// Wait, bounded, for the birth's `node-left` completion call.
    AwaitNodeLeft,
    /// Stop the runtime through the provider.
    TerminateRuntime,
    /// The departure (a removal only): the provider proved the runtime terminal; the
    /// node has left.
    NodeDeleted,
    /// The pre-event of a restart: the executor holds `restart-node:<path>`; the birth is held
    /// through its Leaving until its later birth is heard.
    NodeRestarting,
    /// Only on a removal.
    ReleaseStorage,
    /// Remove the node from the local topology.
    RemoveTopologyMembership,
    /// The node is retired.
    Complete,
}

impl RetireStep {
    /// Every retire step in order.
    pub const ORDER: [RetireStep; 10] = [
        Self::NodeDeleting,
        Self::DrainNode,
        Self::AwaitNodeDrained,
        Self::StopNode,
        Self::AwaitNodeLeft,
        Self::TerminateRuntime,
        Self::NodeDeleted,
        Self::ReleaseStorage,
        Self::RemoveTopologyMembership,
        Self::Complete,
    ];

    /// The step's name as it appears in receipts and spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::NodeDeleting => "NodeDeleting",
            Self::NodeDeleted => "NodeDeleted",
            Self::NodeRestarting => "NodeRestarting",
            Self::DrainNode => "DrainNode",
            Self::AwaitNodeDrained => "AwaitNodeDrained",
            Self::StopNode => "StopNode",
            Self::AwaitNodeLeft => "AwaitNodeLeft",
            Self::TerminateRuntime => "TerminateRuntime",
            Self::ReleaseStorage => "ReleaseStorage",
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
    /// The predecessor's storage goes into the successor's empty directory, after the predecessor
    /// is proven terminal and before the successor's process exists.
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
    pub fn begin(node: &PathName, data_dir: &std::path::Path, deployment_id: &DeploymentId) -> Result<Self, PipelineError> {
        Self::begin_in(node, data_dir, deployment_id, None)
    }

    /// [`Self::begin`], for a process that runs in a container of the container runtime whose
    /// control domain is `container_domain`: its runtime is that container, by immutable id.
    pub fn begin_in(node: &PathName, data_dir: &std::path::Path, deployment_id: &DeploymentId, container_domain: Option<String>) -> Result<Self, PipelineError> {
        let kind = if container_domain.is_some() { crate::model::ProviderKind::Container } else { crate::model::ProviderKind::Process };
        let span = tracing::info_span!(
            "rdm.node_admin.deployment.update.via-pipeline",
            pipeline = "adopt-current",
            build_id = "",
            provider = ?kind,
            node = %node,
            attempt = 1u32,
            restart = false,
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
    pub fn publish(mut self, publish: impl FnOnce(&RuntimeFact, String)) -> Result<RuntimeFact, PipelineError> {
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
        let r = span.in_scope(|| work(&self.node));
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        span.record("outcome", if r.is_ok() { "complete" } else { "failed" });
        self.receipts.push(match &r {
            Ok((_, ev)) => AdoptionReceipt { step: step.name().into(), outcome: StepOutcome::Complete, output: serde_json::to_value(ev).ok() },
            Err(reason) => AdoptionReceipt { step: step.name().into(), outcome: StepOutcome::Failed { reason: reason.clone() }, output: None },
        });
        // Written after every step; the Ready gate reads them back.
        if let Err(e) = serde_json::to_vec_pretty(&self.receipts).map_err(|e| e.to_string()).and_then(|b| std::fs::write(self.data_dir.join(ADOPTION_RECEIPTS), b).map_err(|e| e.to_string())) {
            tracing::warn!(node = %self.node, error = %e, "the Day-0 adoption receipts cannot be written");
        }
        r.map(|(f, _)| f).map_err(|reason| PipelineError { step: step.name(), reason })
    }
}

/// The Day-0 adoption steps `data_dir` holds no `Complete` receipt for.
pub(crate) fn adoption_missing(data_dir: &std::path::Path) -> Vec<&'static str> {
    let receipts: Vec<AdoptionReceipt> = std::fs::read(data_dir.join(ADOPTION_RECEIPTS)).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    AdoptStep::ORDER
        .iter()
        .map(|s| s.name())
        .filter(|s| !receipts.iter().any(|r| r.step == *s && r.outcome == StepOutcome::Complete))
        .collect()
}

/// What the pipeline asks of the live mesh (gossip membership and Node RPC).
#[async_trait::async_trait]
pub trait NodeObserver: Send + Sync {
    /// What this exact birth (`node_id`, `incarnation`) publishes with its
    /// own membership digest; `None` until it has joined fabric membership.
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication>;
    /// Is the node serving?
    async fn ready(&self, node: &Node) -> Result<(), String>;
    /// Send `cmd` (`drain-node` or `stop-node`) to the exact birth over Node RPC, opening the
    /// command before the send so its completion finds it. Answers what the call established,
    /// never a guess: the birth admitted it, or by name why nothing was established. The reply is
    /// admission, not completion.
    async fn send_command(&self, node: &Node, _cmd: crate::node_commands::NodeCommand, _ctx: &CommandContext) -> CommandAdmission {
        CommandAdmission::NotSent { reason: format!("this observer holds no Node RPC client to command {}", node.name) }
    }
    /// Wait up to `within` for the commanded birth's completion call (`node-drained` /
    /// `node-left`) at this admin.
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

/// The lifecycle events a Mesh executor publishes around a retirement: the pre-notice once its
/// own journal holds the step that records it, the departure once the provider proved the
/// runtime terminal. Membership carries them; this trait is how the pipeline reaches it.
/// Why a birth is being retired: the storage disposition and the lifecycle events follow it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetireKind {
    /// The logical node leaves the topology.
    Removal,
    /// The birth stops; the logical node is reborn at the same path.
    Restart,
}

/// What a removal did with the logical node's storage, by the accepted Build's
/// `StorageMeta` for its path: the ReleaseStorage step's receipt.
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
        matches!(self, Self::Admitted | Self::AlreadyAdmitted)
    }
    /// The arm's name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::AlreadyAdmitted => "already-admitted",
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
            other => CommandAdmission::Refused { reply: format!("{}: {other:?}", other.name()) },
        },
        RpcOutcome::NotSent(n) => CommandAdmission::NotSent { reason: format!("{:?}", n.reason()) },
        RpcOutcome::Indeterminate(i) => CommandAdmission::Indeterminate { reason: format!("{:?}", i.reason()) },
        RpcOutcome::Unserved(u) => CommandAdmission::Refused { reply: format!("unserved op {:#04x}", u.op()) },
        RpcOutcome::RejectedStale(r) => CommandAdmission::Refused { reply: format!("stale target {}", r.target_node_id()) },
    }
}

/// Whether the exact birth's completion call (`node-drained`, `node-left`) reached this admin: the
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
#[async_trait::async_trait]
pub trait LifecycleEvents: Send + Sync {
    /// The removal is under way.
    async fn deleting(&self, op: &LifecycleOp);
    /// The removal completed.
    async fn deleted(&self, op: &LifecycleOp);
    /// A restart's pre-event (`NodeRestarting`): the birth is held through its Leaving.
    async fn restarting(&self, op: &LifecycleOp);
    /// The accepted `drain-node` command's hook: `node-draining`, on the executor's own mesh
    /// channel and the backbone.
    async fn draining(&self, _op: &LifecycleOp) {}
    /// The accepted `stop-node` command's hook: `node-leaving`, on the executor's own mesh channel
    /// and the backbone.
    async fn leaving(&self, _op: &LifecycleOp) {}
    /// The accepted `leave-mesh` command's hook: `mesh-leaving`, authored by the fabric-primary on
    /// the backbone and its own mesh channel.
    async fn mesh_leaving(&self, _event: &MeshLeaveEvent) {}
    /// The `mesh-leave` handoff call's hook: authored by the still-running mesh-primary on the
    /// backbone and its own mesh channel.
    async fn mesh_leave(&self, _event: &MeshLeaveEvent) {}
    /// The completed workflow's hook: `mesh-left`, authored by the fabric-primary after every exact
    /// exit is proven.
    async fn mesh_left(&self, _event: &MeshLeaveEvent) {}
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
    /// The birth `incarnation` of `node_id` is renamed `<name>.old` in the executor's view: its
    /// successor takes the path. Only that birth is renamed; whatever else is recorded at the path stays.
    fn rename_predecessor(&self, _name: &PathName, _node_id: &NodeId, _incarnation: &IncarnationId) {}
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
    /// `Some(current record)` for a restart: same node id, data dir and
    /// transport key, a new incarnation.
    pub restart_of: Option<Node>,
    /// The runtime rows this admin holds, handed to a node-admin's own nodes.storage before it
    /// starts: its durable map of the births a predecessor heard.
    pub held_runtimes: Vec<crate::storage::RuntimeRow>,
}

/// Work the executor does at the birth between its mesh join and its Ready: the fabric primary's
/// Pending hand-off at a mesh's first admin. Runs as its own receipted step (`ApplyMeshPending`);
/// a failure fails the create, so nothing downstream starts under an assumed state.
pub(crate) type BeforeReady = std::sync::Arc<dyn Fn(Node) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> + Send + Sync>;

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
    /// A removal of the logical node (the pre-notice and the departure are published, and the
    /// storage goes by the Build's StorageMeta), or the retire half of a restart (the birth stops,
    /// the logical node and its storage stay).
    pub kind: RetireKind,
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

/// The predecessor a successor's create stops and takes the storage of, between its prepared
/// identity and the start of its runtime.
struct Replacing<'a> {
    build_id: &'a BuildId,
    predecessor: &'a Node,
    handle: &'a DeploymentHandle,
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
    supersedes: Option<IncarnationId>,
    deployment_id: DeploymentId,
}

/// Write (if absent) and read back the node's transport key in its data dir;
/// a restart reuses it, a fresh data dir gets a new one.
fn ensure_transport_key(data_dir: &std::path::Path) -> Result<EndpointId, String> {
    std::fs::create_dir_all(data_dir).map_err(|e| format!("{}: {e}", data_dir.display()))?;
    let path = data_dir.join("node-key");
    let key = match std::fs::read_to_string(&path) {
        Ok(h) => {
            let bytes: [u8; 32] = hex::decode(h.trim())
                .map_err(|e| format!("{}: {e}", path.display()))?
                .try_into()
                .map_err(|_| format!("{}: not 32 bytes", path.display()))?;
            iroh::SecretKey::from_bytes(&bytes)
        }
        Err(_) => {
            let k = iroh::SecretKey::generate();
            std::fs::write(&path, hex::encode(k.to_bytes())).map_err(|e| format!("{}: {e}", path.display()))?;
            k
        }
    };
    Ok(EndpointId(key.public().to_string()))
}

async fn poll<F, Fut>(within: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let until = Instant::now() + within;
    loop {
        if f().await {
            return true;
        }
        if Instant::now() >= until {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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

    fn pipeline_span(&self, kind: &'static str, build_id: &BuildId, node: &PathName, attempt: u32, restart: bool) -> tracing::Span {
        tracing::info_span!(
            "rdm.node_admin.deployment.update.via-pipeline",
            pipeline = kind,
            build_id = %build_id,
            provider = ?self.provider.kind(),
            node = %node,
            attempt,
            restart,
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
        span.record("outcome", if r.is_ok() { "complete" } else { "failed" });
        let (outcome, output) = match &r {
            Ok(v) => (StepOutcome::Complete, serde_json::to_value(v).ok().filter(|v| !v.is_null())),
            Err(reason) => (StepOutcome::Failed { reason: reason.clone() }, None),
        };
        let _ = self
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
        r.map_err(|reason| PipelineError { step, reason })
    }

    /// Create (or restart) `req.node` through every step.
    pub async fn create(&self, req: &CreateRequest) -> Result<Created, PipelineError> {
        self.create_with(req, None).await
    }

    /// `create`, with `before_ready` run as the `ApplyMeshPending` step once the birth has joined
    /// its mesh and before it is asked to be Ready.
    pub(crate) async fn create_with(&self, req: &CreateRequest, before_ready: Option<BeforeReady>) -> Result<Created, PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("create", &req.build_id, &req.node, req.attempt, req.restart_of.is_some());
        self.create_steps(req, before_ready).instrument(span).await
    }

    async fn create_steps(&self, req: &CreateRequest, before_ready: Option<BeforeReady>) -> Result<Created, PipelineError> {
        let op = if req.restart_of.is_some() { "restart-node" } else { "create-node" };
        let mut run = self.begin(&req.build_id, req.attempt, &req.node, format!("{op}:{}", req.node)).await;
        let mut created = self.create_to_ready(&mut run, req, before_ready, None).await?;
        created.node.status = NodeStatus::ReadyForTraffic;
        self.step(&mut run, CreateStep::Complete.name(), async {
            self.sink.publish(created.node.clone());
            Ok(())
        })
        .await?;
        Ok(created)
    }

    /// Every create step through `WaitForNodeReady`, as steps of `run`. A replacement runs them
    /// inside its own operation ([`Replacing`]): once the successor's identity, storage and network
    /// are decided, the predecessor is stopped and its storage handed over, and only then does the
    /// successor start.
    async fn create_to_ready(&self, run: &mut Run<'_>, req: &CreateRequest, before_ready: Option<BeforeReady>, replacing: Option<&Replacing<'_>>) -> Result<Created, PipelineError> {
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
                run.done.clear();
            } else if recorded_by_another(&run, CreateStep::DeployRuntime) && !run.done.contains_key(CreateStep::WaitForMeshJoin.name()) {
                // Launched by an executor that is not this one and never joined: its only seed is
                // that executor's endpoint, which this birth can no longer rely on (the lost admin's
                // address may answer as another process). The birth is stopped and decided afresh
                // under this executor's endpoint; a birth that joined has used its seed and stands.
                let stranded = earlier.expect("alive implies a decoded handle");
                let span = tracing::info_span!(
                    "rdm.node_admin.deployment.reject.via-lost-executor",
                    node = %req.node,
                    build_id = %req.build_id,
                    attempt = req.attempt,
                    step = CreateStep::DeployRuntime.name(),
                    deployment_id = %stranded.deployment_id,
                    recorded_executor = run.owner.get(CreateStep::DeployRuntime.name()).and_then(|o| o.as_deref()).unwrap_or("not recorded"),
                    executor = %executor,
                );
                use tracing::Instrument;
                self.provider
                    .terminate(&stranded, TerminationMode::Graceful { grace: self.timeouts.stop_grace })
                    .instrument(span)
                    .await
                    .map_err(|e| PipelineError { step: CreateStep::DeployRuntime.name(), reason: format!("{}: the earlier attempt's runtime was launched by another executor and never joined; stopping it failed: {e}", req.node) })?;
                run.done.clear();
            }
        }
        let prior = req.restart_of.clone();
        let id: Identity = self
            .step(run, CreateStep::AllocateIdentity.name(), async {
                Ok(Identity {
                    node_id: prior.as_ref().map(|p| p.node_id.clone()).unwrap_or_else(NodeId::mint),
                    incarnation: IncarnationId::mint(),
                    supersedes: prior.as_ref().and_then(|p| p.incarnation_id.clone()),
                    deployment_id: DeploymentId::mint(),
                })
            })
            .await?;
        // The node binds port 0 and the operating system assigns the port; the address it
        // reports at its join is the one this admin publishes for the birth.
        let bind_addr = SocketAddr::new(self.provider.bind_ip(&req.node), 0);
        let bind_listeners: Vec<(String, SocketAddr)> = req.spec.listeners.iter().map(|n| (n.to_string(), bind_addr)).collect();
        let data_dir = match prior.as_ref().and_then(|p| p.data_dir.clone()) {
            Some(d) => PathBuf::from(d),
            None => self.template.data_root.join(format!("{}-{}", req.node, id.node_id)),
        };
        let endpoint_id: EndpointId = self.step(run, CreateStep::PrepareStorage.name(), async {
            let key = ensure_transport_key(&data_dir)?;
            if !req.held_runtimes.is_empty() {
                let store = crate::storage::FileNodesStorage::open(&data_dir).map_err(|e| e.to_string())?;
                for row in &req.held_runtimes {
                    crate::storage::NodesStorage::put_runtime(&store, row).await.map_err(|e| e.to_string())?;
                }
            }
            Ok(key)
        })
        .await?;
        // The process provider shares the host network namespace and the
        // container provider's network exists per fabric: nothing per node.
        self.step(run, CreateStep::PrepareNetwork.name(), async { Ok(()) }).await?;
        // A replacement: the successor now has its identity, key and directory. The predecessor is
        // stopped, with the provider's exact terminal proof, and its storage handed over into that
        // directory before the successor's process exists.
        if let Some(replacing) = replacing {
            self.stop_and_hand_over(run, replacing, &data_dir).await?;
        }
        let launch = Launch {
            fabric: self.template.fabric.clone(),
            fabric_id: self.template.fabric_id.clone(),
            name: req.node.clone(),
            node_id: id.node_id.clone(),
            incarnation: id.incarnation.clone(),
            supersedes: id.supersedes.clone(),
            bind_addr,
            listeners: bind_listeners.clone(),
            launcher: Some(self.template.launcher.clone()),
            seeds: self.template.seeds.clone(),
            data_dir: data_dir.clone(),
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
                if let Some(h) = self.provider.find(&spec).await {
                    tracing::info!(deployment_id = %spec.deployment_id, "adopting the runtime this deployment already started");
                    return Ok(h);
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
                supersedes: id.supersedes.clone(),
                endpoint_id: endpoint_id.clone(),
                runtime,
                data_dir: data_dir.display().to_string(),
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
            f.write_record(&data_dir).map_err(|e| format!("{}: {e}", req.node))?;
            match RuntimeFact::read_record(&data_dir) {
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
        if let Some(p) = &prior {
            node.is_primary = p.is_primary;
        }
        // One coherent birth projection: the node record (topology and data
        // dir) here; the fact itself the birth publishes with its own digest,
        // which WaitForMeshJoin holds to exactly this fact and data dir.
        self.step(run, CreateStep::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata.name(), async {
            self.sink.publish(node.clone());
            Ok(RuntimeEvidence { data_dir: node.data_dir.clone(), ..RuntimeEvidence::of(&fact) })
        })
        .await?;
        // Every create records this step: a mesh's first admin receives the fabric primary's
        // Pending here; any other birth records that none was owed, so the Build facts say so.
        let target = node.clone();
        let _: serde_json::Value = self
            .step(run, CreateStep::ApplyMeshPending.name(), async {
                match &before_ready {
                    Some(hand_off) => hand_off(target).await.map(|()| serde_json::json!({"applied": "Pending"})),
                    None => Ok(serde_json::json!({"applied": null, "reason": "not a mesh's first admin"})),
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
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
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
        let span = self.pipeline_span("drain", &req.build_id, &req.node.name, req.attempt, false);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("drain-node:{name}")).await;
            let mut node = req.node.clone();
            self.drain_steps_with(&mut run, &mut node, &req.handle, true).await?;
            self.complete_for_birth(&mut run, &node).await
        }
        .instrument(span)
        .await
    }

    /// A standalone stop (node-stop.md): `stop-node:<path>` is the stop-node command, the wait for the
    /// exact birth's `node-left`, the provider's stop with its Exited proof, and `Complete`, whose receipt
    /// names the birth. No drain is sent and no departure is published.
    pub async fn stop(&self, req: &RetireRequest) -> Result<TerminalReceipt, PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("stop", &req.build_id, &req.node.name, req.attempt, false);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("stop-node:{name}")).await;
            let mut node = req.node.clone();
            let terminal = self.stop_steps_with(&mut run, &mut node, &req.handle, true).await?;
            self.complete_for_birth(&mut run, &node).await?;
            Ok(terminal)
        }
        .instrument(span)
        .await
    }

    /// The `Complete` receipt of a standalone operation: it names the exact birth the operation
    /// completed for, so a later attempt's plan reads it and plans nothing more for that birth.
    async fn complete_for_birth(&self, run: &mut Run<'_>, node: &Node) -> Result<(), PipelineError> {
        let birth = serde_json::json!({
            "node_id": node.node_id,
            "incarnation": node.incarnation_id,
        });
        let _: serde_json::Value = self.step(run, RetireStep::Complete.name(), async { Ok(birth) }).await?;
        Ok(())
    }

    /// Retire `req.node` through every step.
    pub async fn retire(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("retire", &req.build_id, &req.node.name, req.attempt, false);
        self.retire_steps(req).instrument(span).await
    }

    async fn retire_steps(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        let name = &req.node.name;
        let mut run = self.begin(&req.build_id, req.attempt, name, format!("retire-node:{name}")).await;
        let mut node = req.node.clone();
        let handle = &req.handle;
        // The pre-notice, for a departure only: this executor's journal holds its Claim for the
        // attempt (it runs nothing before that) and now records this operation; every node hears
        // the node is being removed and stops routing to it, while it stays found. A restart
        // (`RetireKind::Restart`) stops the birth and keeps the logical node: it never emits a
        // departure, so the same NodeId's next incarnation is taken.
        let op = if req.kind == RetireKind::Removal {
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
            Some(op)
        } else {
            // A restart: the pre-event of `restart-node:<path>` on the same receipt-then-publish
            // order, so every node holds the birth through the Leaving that follows.
            let op = LifecycleOp {
                build_id: req.build_id.to_string(),
                attempt: req.attempt,
                operation: format!("restart-node:{name}"),
                node_id: node.node_id.clone(),
                incarnation: node.incarnation_id.clone().ok_or_else(|| PipelineError { step: RetireStep::NodeRestarting.name(), reason: format!("{name} has no known birth") })?,
                name: name.clone(),
                event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
            };
            let op = self.step(&mut run, RetireStep::NodeRestarting.name(), async { Ok(op.clone()) }).await?;
            self.lifecycle.restarting(&op).await;
            None
        };
        // The two halves of the shutdown leg are separate operations (node-drain.md, node-stop.md):
        // drain-node and its node-drained, then stop-node, its node-left and the provider's
        // terminal proof. A caller that puts other work between them (a replace) calls the halves.
        self.drain_steps(&mut run, &mut node, handle).await?;
        let _terminal = self.stop_steps(&mut run, &mut node, handle).await?;
        // The departure: the provider's inspection above is the proof. Nothing earlier (the
        // node's Leaving, its drained reply, the claim) is.
        if let Some(op) = op {
            let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op };
            let op = self.step(&mut run, RetireStep::NodeDeleted.name(), async { Ok(op.clone()) }).await?;
            self.lifecycle.deleted(&op).await;
        }
        if req.kind == RetireKind::Removal {
            self.release_storage_step(&mut run, &req.build_id, &node).await?;
        }
        self.step(&mut run, RetireStep::RemoveTopologyMembership.name(), async {
            self.sink.remove(name);
            Ok(())
        })
        .await?;
        self.step(&mut run, RetireStep::Complete.name(), async { Ok(()) }).await?;
        Ok(())

    }

    /// The command identity for `cmd` on `node` in this run: the Build, the attempt and the
    /// operation (`drain-node:<path>` / `stop-node:<path>`).
    fn command_context(&self, run: &Run<'_>, cmd: crate::node_commands::NodeCommand, node: &Node) -> CommandContext {
        // A drain's operation key has one source: the `node.drain` object derives it, and the receipt, the wire request and the completion all carry that key.
        let operation = match cmd {
            crate::node_commands::NodeCommand::Drain => rafka_node_admin_client::DrainContext::new(rafka_node_admin_client::BuildId(run.build_id.to_string()), run.attempt, node.name.clone()).operation(),
            crate::node_commands::NodeCommand::Stop => format!("{}:{}", cmd.operation_prefix(), node.name),
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

    /// Resolves when this deployment's own runtime has exited.
    async fn until_exited(&self, handle: &DeploymentHandle) {
        while !matches!(self.provider.inspect(handle).await, DeploymentStatus::Exited { .. }) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// One member's shutdown inside a mesh leave: its drain half and its stop half under its own
    /// run, whose operation is the outer `shutdown-mesh:<mesh_id>` correlated with the member's path.
    /// The children keep their `drain-node:<path>` and `stop-node:<path>` identities. The result is
    /// the provider's terminal receipt of the exact runtime.
    pub(crate) async fn shutdown_member(&self, build_id: &BuildId, attempt: u32, outer_operation: &str, node: &mut Node, handle: &DeploymentHandle) -> Result<TerminalReceipt, PipelineError> {
        use tracing::Instrument;
        let path = node.name.clone();
        let span = self.pipeline_span("shutdown-member", build_id, &path, attempt, false);
        async {
            let mut run = self.begin(build_id, attempt, &path, crate::mesh_leave::member_operation(outer_operation, &path.to_string())).await;
            self.drain_steps(&mut run, node, handle).await?;
            self.stop_steps(&mut run, node, handle).await
        }
        .instrument(span)
        .await
    }

    /// The drain half of a shutdown (node-drain.md): `drain-node` to the exact birth, then the
    /// wait for its `node-drained` completion call. The birth stays running. The completion wait
    /// ends at the lifecycle drain deadline (an arm, recorded) or at this runtime's exit.
    pub(crate) async fn drain_steps(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle) -> Result<(), PipelineError> {
        self.drain_steps_with(run, node, handle, false).await
    }

    /// [`Self::drain_steps`]; `strict` (a standalone drain) fails the wait step by name unless the
    /// matching `node-drained` call was accepted: an admission acknowledgement, a deadline and an exit
    /// are each indeterminate, never drain completion (node-drain.md).
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
    /// drain), the wait for its `node-left` completion call, then the provider's stop and
    /// inspection of the exact runtime: only its `Exited` is terminal proof.
    pub(crate) async fn stop_steps(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle) -> Result<TerminalReceipt, PipelineError> {
        self.stop_steps_with(run, node, handle, false).await
    }

    /// [`Self::stop_steps`]; `strict` (a standalone stop) fails the wait step by name unless the matching
    /// `node-left` call was accepted (node-stop.md: the completion is NodeLeft plus the runtime's Exited).
    async fn stop_steps_with(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle, strict: bool) -> Result<TerminalReceipt, PipelineError> {
        use crate::node_commands::NodeCommand;
        let ctx = self.command_context(run, NodeCommand::Stop, node);
        node.status = NodeStatus::Leaving;
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
                    return Ok(Completion::NotAwaited { admission: format!("{admission:?}") });
                }
                let c = tokio::select! {
                    c = self.observer.await_completion(node, NodeCommand::Stop, &ctx, self.timeouts.drain) => c,
                    () = self.until_exited(handle) => Completion::RuntimeExited,
                };
                match (strict, &c) {
                    (true, c) if *c != Completion::Received => Err(format!("{}: stop-node was admitted but no matching node-left call was accepted ({c:?}); the stop is not complete", node.name)),
                    _ => Ok(c),
                }
            })
            .await?;
        self.terminate_step(run, node, handle).await
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
            let exited_pid = handle.pid;
            let still = |h: &(String, SocketAddr, super::endpoint::SlotTransport)| !super::endpoint::released_by(h.1, h.2, exited_pid);
            let started = Instant::now();
            loop {
                held.retain(|h| still(h));
                if held.is_empty() {
                    break;
                }
                if Instant::now() >= until {
                    let names: Vec<String> = held.iter().map(|h| format!("{} {} ({})", h.0, h.1, super::endpoint::port_holder(h.1, h.2))).collect();
                    return Err(format!("{name} exited, but the operating system still holds {} after {:?}", names.join(", "), started.elapsed()));
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

    /// The storage disposition of a removal (the lock's StorageMeta): what the accepted Build's
    /// meta for this path says, read here and interpreted here, never by the provider. The receipt
    /// names the disposition.
    pub(crate) async fn release_storage_step(&self, run: &mut Run<'_>, build_id: &BuildId, node: &Node) -> Result<(), PipelineError> {
        let name = &node.name;
        let _disposition: StorageDisposition = self
            .step(run, RetireStep::ReleaseStorage.name(), async {
                let storage = self
                    .builds
                    .read_build(build_id)
                    .await
                    .ok()
                    .and_then(|b| b.topology.meshes.get(&name.mesh).and_then(|m| m.meta(name).map(|meta| meta.storage)));
                let release = match storage {
                    Some(StorageMeta::Ephemeral) | Some(StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Release }) => true,
                    Some(StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve }) => false,
                    // No meta for this path in the Build: storage is never destroyed on missing evidence.
                    None => return Ok(StorageDisposition::PreservedNoMeta),
                };
                if !release {
                    return Ok(StorageDisposition::Preserved);
                }
                match node.data_dir.as_deref() {
                    Some(dir) => match std::fs::remove_dir_all(dir) {
                        Ok(()) => Ok(StorageDisposition::Released { locator: dir.to_string() }),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(StorageDisposition::Released { locator: dir.to_string() }),
                        Err(e) => Err(format!("{dir}: {e}")),
                    },
                    None => Ok(StorageDisposition::Released { locator: String::new() }),
                }
            })
            .await?;

        Ok(())
    }

    /// The directory of `node`'s birth as it is now: the data dir its record names, else the
    /// root's `<path>-<node_id>`.
    fn birth_dir(&self, node: &Node) -> PathBuf {
        node.data_dir.as_deref().map(PathBuf::from).unwrap_or_else(|| self.template.data_root.join(format!("{}-{}", node.name, node.node_id)))
    }

    /// Replace `req.predecessor` with a new node at its path, in the order node-replace.md and
    /// node-recovery.md give, as ONE operation `replace-node:<path>`:
    ///
    /// ```text
    /// NodeDeleting
    ///   -> [MarkDraining -> WaitForDrain -> PublishLeaving -> CloseRpcAdmission]  (a birth not yet proven terminal)
    ///   -> RenamePredecessor -> AllocateIdentity -> PrepareStorage -> PrepareNetwork
    ///   -> TerminateRuntime -> HandoffStorage
    ///   -> DeployRuntime -> ... -> WaitForBind -> ... -> WaitForMeshJoin -> WaitForNodeReady
    ///   -> NodeDeleted -> ReleaseStorage -> Complete
    /// ```
    ///
    /// The old identity's `NodeDeleted` is published after the successor is admitted and ready, and
    /// never before: a replace is not a delete followed by a spawn. `before_ready` is the successor's
    /// `ApplyMeshPending` work (a mesh's first admin); `after_ready` commits the successor's lifecycle
    /// transition once it is ready.
    pub async fn replace<F, Fut>(&self, req: &ReplaceRequest, before_ready: Option<BeforeReady>, after_ready: F) -> Result<Created, PipelineError>
    where
        F: FnOnce(Node) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        use tracing::Instrument;
        let span = self.pipeline_span("replace", &req.build_id, &req.predecessor.name, req.attempt, false);
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
        // The receipt first, then the publish: the Build facts carry the overlay from the moment it is
        // announced, so a successor executor derives it from the same facts.
        let op = LifecycleOp {
            build_id: req.build_id.to_string(),
            attempt: req.attempt,
            operation: run.operation.clone(),
            node_id: old.node_id.clone(),
            incarnation: birth.clone(),
            name: name.clone(),
            event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
        };
        let op = self.step(&mut run, RetireStep::NodeDeleting.name(), async { Ok(op.clone()) }).await?;
        self.lifecycle.deleting(&op).await;
        // A birth the provider already proves terminal is sent nothing (node-recovery.md: no RPC can
        // be required from the proven-terminal old birth); any other birth drains first.
        if !matches!(self.provider.inspect(&req.handle).await, DeploymentStatus::Exited { .. }) {
            self.drain_steps(&mut run, &mut old, &req.handle).await?;
        }
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
            )
            .in_scope(|| tracing::info!("the predecessor gives up its path.name: the successor is created at it"));
            self.sink.rename_predecessor(name, &old.node_id, &birth);
            Ok(())
        })
        .await?;
        let creq = CreateRequest { build_id: req.build_id.clone(), attempt: req.attempt, node: name.clone(), spec: req.spec, restart_of: None, held_runtimes: req.held_runtimes.clone() };
        let replacing = Replacing { build_id: &req.build_id, predecessor: &old, handle: &req.handle };
        let mut created = self.create_to_ready(&mut run, &creq, before_ready, Some(&replacing)).await?;
        created.node.status = NodeStatus::ReadyForTraffic;
        after_ready(created.node.clone()).await.map_err(|reason| PipelineError { step: CreateStep::WaitForNodeReady.name(), reason })?;
        // The departure of the old identity: the provider's inspection is the proof, and the
        // successor is admitted and ready.
        let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op };
        let op = self.step(&mut run, RetireStep::NodeDeleted.name(), async { Ok(op.clone()) }).await?;
        self.lifecycle.deleted(&op).await;
        self.release_storage_step(&mut run, &req.build_id, &old).await?;
        self.step(&mut run, CreateStep::Complete.name(), async {
            self.sink.publish(created.node.clone());
            Ok(())
        })
        .await?;
        Ok(created)
    }

    /// The predecessor stops, with the provider's exact terminal proof, and its storage goes into the
    /// successor's directory: after the successor's identity exists and before its process does.
    async fn stop_and_hand_over(&self, run: &mut Run<'_>, replacing: &Replacing<'_>, successor_dir: &std::path::Path) -> Result<(), PipelineError> {
        // A birth the provider already proves terminal is asked only for that proof; any other birth is
        // stopped through the stop leg (stop-node, node-left, the provider's stop and inspection).
        if matches!(self.provider.inspect(replacing.handle).await, DeploymentStatus::Exited { .. }) {
            self.terminate_step(run, replacing.predecessor, replacing.handle).await?;
        } else {
            let mut old = replacing.predecessor.clone();
            self.stop_steps(run, &mut old, replacing.handle).await?;
        }
        self.step(run, ReplaceStep::HandoffStorage.name(), async { self.hand_over_storage(replacing, successor_dir).await }).await
    }

    /// The top-level directories of every directory that holds this path's storage (the predecessor's,
    /// and a successor an earlier attempt of this operation left behind) move into the successor's
    /// directory. Files (the identity, the transport key, the runtime record) never move. What the
    /// Build's `StorageMeta` for the path says decides: only persistent storage is handed over, and a
    /// node-admin's records are seeded from its maker at join, never inherited.
    async fn hand_over_storage(&self, replacing: &Replacing<'_>, successor_dir: &std::path::Path) -> Result<(), String> {
        let predecessor = replacing.predecessor;
        let name = &predecessor.name;
        let storage = self
            .builds
            .read_build(replacing.build_id)
            .await
            .ok()
            .and_then(|b| b.topology.meshes.get(&name.mesh).and_then(|m| m.meta(name).map(|meta| meta.storage)));
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
        let mut sources = vec![current.clone()];
        for entry in std::fs::read_dir(&self.template.data_root).map_err(|e| format!("{}: {e}", self.template.data_root.display()))?.flatten() {
            let (dir, file) = (entry.path(), entry.file_name().to_string_lossy().to_string());
            if file.starts_with(&format!("{name}-")) && dir != successor_dir && dir != current && entry.file_type().is_ok_and(|t| t.is_dir()) {
                sources.push(dir);
            }
        }
        let mut moved: Vec<String> = Vec::new();
        for source in sources {
            let Ok(entries) = std::fs::read_dir(&source) else { continue };
            let holders: Vec<_> = entries.flatten().filter(|e| e.file_type().is_ok_and(|t| t.is_dir())).collect();
            if holders.is_empty() {
                continue;
            }
            // A directory is moved only once the runtime that used it is gone: the predecessor's is
            // proven terminal by `TerminateRuntime`, an earlier attempt's successor is checked here.
            if let Some(Ok(fact)) = RuntimeFact::read_record(&source) {
                if let Ok(h) = super::provider::adopt(self.provider, &fact) {
                    if self.provider.inspect(&h).await == DeploymentStatus::Running {
                        return Err(format!("{name}: {} holds storage of this path and its runtime (deployment {}) still runs; it is not moved", source.display(), fact.deployment_id));
                    }
                }
            }
            for dir in holders {
                let to = successor_dir.join(dir.file_name());
                if to.exists() {
                    return Err(format!("{name}: {} would move to {}, which already exists in the successor's directory", dir.path().display(), to.display()));
                }
                std::fs::rename(dir.path(), &to).map_err(|e| format!("{name}: moving {} to {}: {e}", dir.path().display(), to.display()))?;
                moved.push(dir.file_name().to_string_lossy().to_string());
            }
        }
        tracing::info!(node = %name, to = %successor_dir.display(), handed_over = ?moved, "the predecessor's storage was handed to the successor's empty directory");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn day_zero_adopts_this_process_with_one_receipt_per_step() {
        let dir = std::env::temp_dir().join(format!("rafka-adopt-current-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let node: PathName = "mesh1.admin.1".parse().unwrap();
        let a = CurrentRuntimeAdoption::begin(&node, &dir, &DeploymentId::mint()).unwrap();
        assert_eq!(adoption_missing(&dir), vec!["PublishRuntimeFactAndCurrentRuntimeMetadata"], "Ready waits for publication");
        let mut published = None;
        let fact = a.publish(|f, d| published = Some((f.clone(), d))).unwrap();
        assert_eq!(published, Some((fact.clone(), dir.display().to_string())));
        assert!(adoption_missing(&dir).is_empty());
        fact.verify_is_this_runtime().unwrap();
        let receipts: Vec<AdoptionReceipt> = serde_json::from_slice(&std::fs::read(dir.join(ADOPTION_RECEIPTS)).unwrap()).unwrap();
        assert_eq!(receipts.iter().map(|r| r.step.as_str()).collect::<Vec<_>>(), AdoptStep::ORDER.iter().map(|s| s.name()).collect::<Vec<_>>());
        // Evidence by fingerprint: no pid in a receipt.
        let text = std::fs::read_to_string(dir.join(ADOPTION_RECEIPTS)).unwrap();
        assert!(!text.contains("\"pid\""), "{text}");
        // No receipts: nothing is adopted.
        std::fs::remove_file(dir.join(ADOPTION_RECEIPTS)).unwrap();
        assert_eq!(adoption_missing(&dir).len(), 4);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
