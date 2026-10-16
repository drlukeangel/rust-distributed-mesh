//! `DeploymentPipeline`: create or retire one runtime (PRD §8;
//! mesh-control-plane.md §6).
//!
//! ```text
//! create: AllocateIdentity -> PrepareStorage -> PrepareNetwork
//!   -> DeployRuntime -> RegisterExactRuntimeHandle -> ResolveProviderControlDomain
//!   -> MakeRuntimeFactAvailableToBirth -> WaitForBind
//!   -> PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata -> WaitForMeshJoin
//!   -> WaitForNodeReady -> Complete
//! retire: StopNode (stop-node to the exact birth: its node.connections.delete { All }, then its
//!   transport cut; the `Stopped` reply carries the All's ConnectionsReceipt)
//!   -> AwaitNodeLeft (its `Stopped` reply, bounded; a deadline is an arm, never a failure)
//!   -> TerminateRuntime (the provider's stop and exact inspection: only `Exited` is terminal proof)
//!   -> RemoveTopologyMembership -> Complete
//! delete of a crashed target: NodeDeleting -> TerminateRuntime (the exact exit proof, nothing sent)
//!   -> NodeDeleted (from the mesh-primary's recorded exit proof) -> RemoveTopologyMembership -> Complete
//! update: UpdateNode (a recipe's node.name.update of a name through node.name.update's own core) -> Complete
//! ```
//!
//! Node recovery (node-recovery.md "Steps") is two operations of these, never a sequence of its
//! own: the delete of the crashed target, then the create over its data dir. A recipe
//! (lifecycles/node-replace.md) is likewise its steps' own operations, in order.
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
use crate::build_state::{BuildStateAdapter, BuildStateError, BuildStepReceipt, StepOutcome};
use crate::model::{DeploymentId, EndpointId, IncarnationId, Node, NodeId, NodeStatus, PathName};
use rdm_core::launch::Launch;
use rdm_core::{LifecycleOp, RuntimeFact};
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

/// The steps of a recipe's `node.name.update` as a Build operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStep {
    /// A recipe's name put.
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

/// The steps of creating a mesh (`create-mesh:<mesh>`, node-identity-and-state.md §2.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateMeshStep {
    /// The mesh's name is claimed by insert-and-fail at its key. Receipted only when the claim is
    /// refused: a won claim, or the mesh's own row found standing, ends in `Complete`.
    ClaimMeshName,
    /// The name is claimed and the mesh's id is held for its launches: the operation is done.
    Complete,
}

impl CreateMeshStep {
    /// The step's name as it appears in receipts and spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::ClaimMeshName => "ClaimMeshName",
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
    /// `node.connections.delete { All }` to the exact birth through its mesh-primary: graceful
    /// closure of every open connection; the process keeps running.
    DeleteConnections,
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
    /// The pre-event of a restart: the executor holds `node.restart:<path>`; the birth is held
    /// through its Leaving until its later birth is heard.
    NodeRestarting,
    /// Remove the node from the local topology.
    RemoveTopologyMembership,
    /// The node is retired.
    Complete,
}

impl RetireStep {
    /// Every retire step in order.
    pub const ORDER: [RetireStep; 7] = [
        Self::NodeDeleting,
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
            Self::DeleteConnections => "DeleteConnections",
            Self::StopNode => "StopNode",
            Self::AwaitNodeLeft => "AwaitNodeLeft",
            Self::TerminateRuntime => "TerminateRuntime",
            Self::RemoveTopologyMembership => "RemoveTopologyMembership",
            Self::Complete => "Complete",
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
                Some(d) => RuntimeFact::of_this_container(&deployment_id.as_str(), d),
                None => RuntimeFact::of_this_process(&deployment_id.as_str()),
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
                None => rdm_core::runtime::process_control_domain()?,
            };
            if f.control_domain != domain {
                return Err(format!("{n}: its runtime is in control domain {}, not this provider's domain {}", f.domain_fingerprint(), rdm_core::runtime::fingerprint(&domain)));
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
        let _ = f.write_record(&self.data_dir);
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
            Err(reason) => AdoptionReceipt { step: step.name().into(), outcome: StepOutcome::failed(reason.clone()), output: None },
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
///
/// An arm with no field is written `Arm {}`: an internally tagged enum ignores a field on a unit arm
/// even under `deny_unknown_fields`, and an empty field list refuses it. The JSON is the same.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "arm", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StorageDisposition {
    /// Released through the exact locator.
    Released {
        /// The locator the storage was released through.
        locator: String,
    },
    /// `Persistent { on_retire: Preserve }`: left where it is.
    Preserved {},
    /// The Build holds no path config for this path: left where it is, never destroyed on missing evidence.
    PreservedNoMeta {},
}

/// What one directed command (`node.connections.delete`, `stop-node`, `start-node`) established at
/// the exact birth: its step's receipt. Admission is not completion.
///
/// An arm with no field is written `Arm {}`: an internally tagged enum ignores a field on a unit arm
/// even under `deny_unknown_fields`, and an empty field list refuses it. The JSON is the same.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "arm", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CommandAdmission {
    /// The birth answered `Applied`.
    Admitted {},
    /// The birth answered `AlreadyApplied`: the same operation was admitted before.
    AlreadyAdmitted {},
    /// The birth answered `stop-node` with `Stopped`: its `node.connections.delete { All }`
    /// completed, it cut its transport and is stopped, and this is what that `All` established.
    Stopped {
        /// The `All`'s receipt.
        receipt: rdm_core::status::ConnectionsReceipt,
    },
    /// The birth answered `node.connections.delete { All }` with `node.connections.deleted`.
    ConnectionsDeleted {
        /// What the closure established.
        receipt: rdm_core::status::ConnectionsReceipt,
    },
    /// The birth answered `start-node` with `Started`: it rejoined as itself.
    Started {},
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
    /// The birth, or the op, refused the call by name (a stale incarnation, an unserved op).
    Refused {
        /// The refusal's name.
        reply: String,
    },
}

impl CommandAdmission {
    /// Whether the birth holds the command.
    pub fn admitted(&self) -> bool {
        matches!(self, Self::Admitted {} | Self::AlreadyAdmitted {} | Self::Stopped { .. } | Self::ConnectionsDeleted { .. } | Self::Started {})
    }
    /// The arm's name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Admitted {} => "admitted",
            Self::AlreadyAdmitted {} => "already-admitted",
            Self::Stopped { .. } => "stopped",
            Self::ConnectionsDeleted { .. } => "connections-deleted",
            Self::Started {} => "started",
            Self::StartFailed { .. } => "start-failed",
            Self::NotSent { .. } => "not-sent",
            Self::Indeterminate { .. } => "indeterminate",
            Self::Refused { .. } => "refused",
        }
    }
}

/// The arm one directed command call is: only `Applied` and `AlreadyApplied` admit it; every other
/// reply is `Refused` by its name, an unsent call `NotSent`, an unanswered one `Indeterminate`.
pub fn command_admission(out: &rdm_core::outcome::RpcOutcome<rdm_core::status::StatusReply>) -> CommandAdmission {
    use rdm_core::outcome::RpcOutcome;
    use rdm_core::status::StatusReply;
    match out {
        RpcOutcome::Reply(r) => match r.value() {
            StatusReply::Applied => CommandAdmission::Admitted {},
            StatusReply::AlreadyApplied => CommandAdmission::AlreadyAdmitted {},
            StatusReply::Stopped { receipt } => CommandAdmission::Stopped { receipt: receipt.clone() },
            StatusReply::ConnectionsDeleted { receipt: Some(receipt), .. } => CommandAdmission::ConnectionsDeleted { receipt: receipt.clone() },
            StatusReply::Started => CommandAdmission::Started {},
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
///
/// An arm with no field is written `Arm {}`: an internally tagged enum ignores a field on a unit arm
/// even under `deny_unknown_fields`, and an empty field list refuses it. The JSON is the same.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "arm", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Completion {
    /// The matching completion call was accepted.
    Received {},
    /// The lifecycle drain deadline passed with no completion call.
    Deadline {},
    /// This deployment's own runtime exited before any completion call came.
    RuntimeExited {},
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
    /// `node.connections.delete:<path>` or `node.stop:<path>`.
    pub operation: String,
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
    /// The exact birth's `stop-node` completed (its `stopped` reply is held, whichever workflow
    /// stopped it: a stop, a restart, a replace, a delete): from its stop's transport cut it says
    /// nothing more. A former seat holder's committed update is said by its mesh's primary, its next
    /// writer, from here (R-NS2).
    async fn stop_completed(&self, op: &LifecycleOp);
    /// The stopped birth answered `start-node` with `Started`: this executor took its `node.joined`
    /// (`op` is the start's operation on that exact birth). Its stop ends here and nowhere else
    /// (node-start.md step 2), the birth is held `Pending`, and this mesh's projection says so.
    async fn joined(&self, op: &LifecycleOp);
    /// The accepted `drain-node` command's hook: `node-draining`, on the executor's own mesh
    /// channel and the backbone.
    async fn connections_deleting(&self, op: &LifecycleOp);
    /// The birth admitted the `stop-node` command (its call came back admitted): the mesh-primary
    /// sets it stopping (states.md "Who sets each state"), and its projection carries that. No frame
    /// is published: `NodeLeaving` is a reserved position (gossip.md frame catalog).
    async fn leaving(&self, op: &LifecycleOp);
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
    async fn stop_completed(&self, _op: &LifecycleOp) {}
    async fn joined(&self, _op: &LifecycleOp) {}
    async fn connections_deleting(&self, _op: &LifecycleOp) {}
    async fn leaving(&self, _op: &LifecycleOp) {}
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
    /// The writer of node rows this admin's executor writes through (`crate::node_rows`): a
    /// create's name claim and a removal's `Deleted` row. `None` where the
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
    pub fabric_id: rdm_core::FabricId,
    /// The executable every launch runs.
    pub executable: PathBuf,
    /// Members to join gossip through: `(public key, address)`.
    pub seeds: Vec<(String, SocketAddr)>,
    /// The admin these launches are made by: the target of each birth's `JoinNode`.
    pub launcher: rdm_core::launch::Launcher,
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
    /// The bound of a `node.connections.delete { All }` every launch carries: the launching admin's
    /// own (`AdminConfig::connections_delete_deadline`).
    pub connections_delete_deadline: Duration,
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
pub(crate) type BeforeReady = std::sync::Arc<dyn Fn(Node) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<rdm_core::status::MeshState, String>> + Send>> + Send + Sync>;

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
/// provider ends it and proves the exit (a delete, a mesh leave). `publish_stopped` is
/// whether the mesh primary publishes `NodeStopped` from the `stopped` reply: every whole node.stop
/// does (node-stop.md step 5), a delete's and a mesh leave member's included; a restart
/// holds the birth with `NodeRestarting` instead (node-restart.md).
#[derive(Debug, Clone, Copy)]
pub(crate) enum StopEnd {
    Park { publish_stopped: bool },
    Terminate { publish_stopped: bool },
}

/// When a `TerminateRuntime` step fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Terminate {
    /// On any runtime that is not exited: a delete's terminate.
    Always,
    /// Only on a runtime the provider reads `Running` at the step's instant: a hung runtime's hold (R-FM1).
    OnlyRunning,
}

/// What a stop's wait step must have seen to complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopProof {
    /// Whatever the birth answered: the shutdown leg of a retire or a mesh leave member, which the
    /// provider's terminal proof judges.
    Lenient,
    /// stop-node admitted and `stopped` received, or the step fails by name: a whole node.stop, a
    /// standalone one (a recipe's stop included) or the stop a restart composes, whose only proof is
    /// that reply (node-stop.md "Completion": the matching `node.stopped`).
    Stopped,
}

/// A step that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineError {
    /// The step.
    pub step: &'static str,
    /// Why it failed.
    pub reason: String,
}

/// A step's failure as its receipt records it: why, and for a `TerminateRuntime` where it stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepFailure {
    /// Why the step failed.
    pub reason: String,
    /// Where a failed `TerminateRuntime` stopped; none for every other step.
    pub stage: Option<crate::build_state::TerminateStage>,
}

impl From<String> for StepFailure {
    fn from(reason: String) -> Self {
        Self { reason, stage: None }
    }
}

impl StepFailure {
    fn at(stage: crate::build_state::TerminateStage, reason: String) -> Self {
        Self { reason, stage: Some(stage) }
    }
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
    /// How long `AwaitNodeLeft` waits for the stop's reply.
    pub completion: Duration,
    /// Stop-ladder grace before a forced kill; longer than the node's own connections-delete bound
    /// (its launch's `connections_delete_deadline`, node-rpc §35).
    pub stop_grace: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            bind: Duration::from_secs(20),
            join: Duration::from_secs(30),
            ready: Duration::from_secs(30),
            completion: Duration::from_secs(10),
            stop_grace: Duration::from_secs(8),
        }
    }
}

/// Where a birth reported it bound: the addresses its own digest names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bound {
    /// The transport address the birth bound.
    pub transport: SocketAddr,
    /// The non-Iroh listeners the birth bound, by name.
    pub listeners: Vec<(String, SocketAddr)>,
}

impl Bound {
    fn of(d: &rdm_core::MeshDigest) -> Result<Self, String> {
        let transport = d.node.transport_addr;
        if transport.ip().is_unspecified() || transport.port() == 0 {
            return Err(format!("it reported the transport address {transport}, which is not an address a peer can dial"));
        }
        // A node-admin opens no HTTP listener (rdm-crate-and-test-layout.md §2): its `admin_api_base` is config.
        Ok(Self { transport, listeners: Vec::new() })
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
#[serde(deny_unknown_fields)]
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
/// How an inherited data dir failed when `PrepareStorage` inspected it (R-FM2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InheritedDirError {
    /// `<data_dir>` does not exist (`rm -rf` / unmounted).
    Gone(String),
    /// `<data_dir>` exists on disk, but `node-key` is missing, unreadable, or corrupt.
    Broken(String),
}

impl InheritedDirError {
    /// Human-readable error message for the failure.
    pub fn into_message(self) -> String {
        match self {
            Self::Gone(m) | Self::Broken(m) => m,
        }
    }
}

/// Inspect `<data_dir>/node-key` of an inherited data dir (R-FM2): classifies whether `<data_dir>`
/// is absent (`Gone`) or exists with a missing, unreadable, or corrupt `node-key` (`Broken`).
pub fn inspect_transport_key(data_dir: &std::path::Path) -> Result<EndpointId, InheritedDirError> {
    let path = data_dir.join("node-key");
    let h = match std::fs::read_to_string(&path) {
        Ok(h) => h,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !data_dir.is_dir() => {
            return Err(InheritedDirError::Gone(format!("the inherited data dir {} does not exist: no node-key to load the endpoint key from", data_dir.display())))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(InheritedDirError::Broken(format!("node-key is missing in the inherited data dir {}: the endpoint key cannot be retained", data_dir.display())))
        }
        Err(e) => return Err(InheritedDirError::Broken(format!("{}: {e}", path.display()))),
    };
    let bytes: [u8; 32] = hex::decode(h.trim())
        .map_err(|e| InheritedDirError::Broken(format!("{}: {e}", path.display())))?
        .try_into()
        .map_err(|_| InheritedDirError::Broken(format!("{}: not 32 bytes", path.display())))?;
    Ok(EndpointId(iroh::SecretKey::from_bytes(&bytes).public().to_string()))
}

/// endpoint key of the data dir it inherits (node-recovery.md, "Retained"), so a dir with no key
/// is refused by name, never given a new one.
pub(crate) fn load_transport_key(data_dir: &std::path::Path) -> Result<EndpointId, String> {
    inspect_transport_key(data_dir).map_err(InheritedDirError::into_message)
}

/// One run of one operation (`node.create:<path>`, `node.delete:<path>`).
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
    ///
    /// A Build this node cannot read fails the run by name, before any step: an unreadable Build is
    /// never a Build with no earlier receipts, whose run would decide afresh what an earlier attempt
    /// already decided. A Build this node holds no fact of is its own error.
    async fn begin<'r>(&self, build_id: &'r BuildId, attempt: u32, node: &'r PathName, operation: String) -> Result<Run<'r>, PipelineError> {
        let view = match self.builds.read_build(build_id).await {
            Ok(view) => view,
            Err(e) => {
                let executor = self.template.executor();
                let reason = match &e {
                    BuildStateError::UnknownBuild(_) => format!("{executor}: this node holds no fact of Build {build_id}, so {operation} (attempt {attempt}, node {node}) has no log to run from: {e}"),
                    _ => format!("{executor}: Build {build_id} cannot be read for {operation} (attempt {attempt}, node {node}): {e}"),
                };
                tracing::info_span!("rdm.node_admin.deployment.reject.via-build-unreadable", build_id = %build_id.log(), attempt, operation = %operation, node = %node, detail = %reason)
                    .in_scope(|| tracing::info!("the Build cannot be read: the run stops before its first step"));
                return Err(PipelineError { step: "ReadBuild", reason });
            }
        };
        let earlier: Vec<BuildStepReceipt> = view.steps.into_iter().filter(|r| r.operation == operation && r.attempt < attempt).collect();
        let ended = last_ended_attempt(&earlier.iter().collect::<Vec<_>>(), &operation, attempt);
        let handed_on: Vec<BuildStepReceipt> = earlier.into_iter().filter(|r| r.attempt > ended && r.outcome == StepOutcome::Complete).collect();
        let owner = handed_on.iter().map(|r| (r.step.clone(), r.executor.clone())).collect();
        let done = handed_on.into_iter().map(|r| (r.step, r.output)).collect();
        Ok(Run { build_id, attempt, node, operation, done, owner, reusing: true })
    }

    fn pipeline_span(&self, kind: &'static str, build_id: &BuildId, node: &PathName, attempt: u32) -> tracing::Span {
        tracing::info_span!(
            "rdm.node_admin.deployment.update.via-pipeline",
            pipeline = kind,
            build_id = %build_id.log(),
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
        self.step_with(run, step, async move { work.await.map_err(StepFailure::from) }).await
    }

    /// [`Self::step`] for a step whose failure names where it stopped: its Failed receipt carries
    /// the typed stage (`TerminateRuntime`, node-recovery.md step 4's end).
    async fn step_with<T, Fut>(&self, run: &mut Run<'_>, step: &'static str, work: Fut) -> Result<T, PipelineError>
    where
        T: Serialize + DeserializeOwned,
        Fut: std::future::Future<Output = Result<T, StepFailure>>,
    {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "rdm.node_admin.deployment.update.via-step",
            step,
            build_id = %run.build_id.log(),
            provider = ?self.provider.kind(),
            node = %run.node,
            attempt = run.attempt,
            outcome = tracing::field::Empty,
            stage = tracing::field::Empty,
            elapsed_ms = tracing::field::Empty,
            io_calls = tracing::field::Empty,
            io_queued_ms = tracing::field::Empty,
            io_ms = tracing::field::Empty,
            awaiting = tracing::field::Empty,
            awaited = tracing::field::Empty,
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
        // Where the time of a step that took long went: its blocking calls' wait for a thread and their
        // run on one, beside the elapsed time. A step cancelled while awaiting a blocking call or a
        // peer call still records its elapsed time, any blocking I/O it began, and outcome = "cancelled".
        struct StepGuard {
            span: tracing::Span,
            started: Instant,
            io: crate::record_store::IoTime,
            done: bool,
        }
        impl Drop for StepGuard {
            fn drop(&mut self) {
                if !self.done {
                    self.span.record("elapsed_ms", self.started.elapsed().as_millis() as u64);
                    if self.io.calls() > 0 {
                        self.span
                            .record("io_calls", self.io.calls())
                            .record("io_queued_ms", self.io.queued_ms())
                            .record("io_ms", self.io.ran_ms());
                    }
                    self.span.record("outcome", "cancelled");
                }
            }
        }
        let io = crate::record_store::IoTime::default();
        let mut guard = StepGuard { span: span.clone(), started, io: io.clone(), done: false };
        let r = crate::record_store::io_time_with(io.clone(), work.instrument(span.clone())).await;
        guard.done = true;
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        if io.calls() > 0 {
            span.record("io_calls", io.calls()).record("io_queued_ms", io.queued_ms()).record("io_ms", io.ran_ms());
        }
        let (outcome, output) = match &r {
            Ok(v) => (StepOutcome::Complete, serde_json::to_value(v).ok().filter(|v| !v.is_null())),
            Err(f) => {
                if let Some(stage) = &f.stage {
                    span.record("stage", stage.name());
                }
                (StepOutcome::Failed { reason: f.reason.clone(), stage: f.stage.clone() }, None)
            }
        };
        // The receipt is the step's durable commit: a step whose receipt the Build state does not
        // write to the executor's own log (a fact with no wire shape, a fenced seat, a failed local
        // write) is not complete, and the run stops at it by name. A failed broadcast after the
        // local write is not a refusal: the Build state names it and the step is committed.
        let appended = self
            .builds
            .append_step_receipt(&BuildStepReceipt {
                mesh: None,
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
                build_id = %run.build_id.log(),
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
                Err(failed) => format!("{}; and {unwritten}", failed.reason),
            };
            return Err(PipelineError { step, reason });
        }
        span.record("outcome", if r.is_ok() { "complete" } else { "failed" });
        r.map_err(|f| PipelineError { step, reason: f.reason })
    }

    /// Create `req.node` through every step.
    pub async fn create(&self, req: &CreateRequest) -> Result<Created, PipelineError> {
        self.create_with(req, None, None).await
    }

    /// `create`, with `before_ready` run as the `ApplyMeshPending` step once the birth has joined
    /// its mesh and before it is asked to be Ready, and `before_launch` awaited right before the new
    /// runtime is launched ([`BeforeLaunch`]).
    pub(crate) async fn create_with(&self, req: &CreateRequest, before_ready: Option<BeforeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        let claim_for = if req.data_dir.is_some() { crate::node_rows::ClaimFor::Recovery } else { crate::node_rows::ClaimFor::Create };
        self.create_with_claim(req, claim_for, before_ready, before_launch).await
    }

    /// `create_with` for a recovery Build (`ClaimFor::Recovery` even when `req.data_dir` is `None`
    /// after `RDM_DATA_DIR_GONE_FLOORS`, R-FM2).
    pub(crate) async fn create_recovery_with(&self, req: &CreateRequest, before_ready: Option<BeforeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        self.create_with_claim(req, crate::node_rows::ClaimFor::Recovery, before_ready, before_launch).await
    }

    async fn create_with_claim(&self, req: &CreateRequest, claim_for: crate::node_rows::ClaimFor, before_ready: Option<BeforeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("create", &req.build_id, &req.node, req.attempt);
        self.create_steps(req, claim_for, before_ready, before_launch).instrument(span).await
    }

    async fn create_steps(&self, req: &CreateRequest, claim_for: crate::node_rows::ClaimFor, before_ready: Option<BeforeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        let mut run = self.begin(&req.build_id, req.attempt, &req.node, format!("node.create:{}", req.node)).await?;
        let mut created = self.create_to_ready(&mut run, req, claim_for, before_ready, before_launch).await?;
        created.node.status = NodeStatus::ReadyForTraffic;
        self.step(&mut run, CreateStep::Complete.name(), async {
            self.sink.publish(created.node.clone());
            Ok(())
        })
        .await?;
        Ok(created)
    }

    /// Every create step through `WaitForNodeReady`, as steps of `run`.
    /// `req.data_dir`: the data dir the spec names (the create over an exited birth's), taken over
    /// whole (node-recovery.md sequence step 4). `before_launch`: awaited right before the new runtime
    /// is launched, and only when it is ([`BeforeLaunch`]).
    async fn create_to_ready(&self, run: &mut Run<'_>, req: &CreateRequest, claim_for: crate::node_rows::ClaimFor, before_ready: Option<BeforeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        // The name this create claimed: a step after the claim that fails withdraws it, so the next
        // attempt's fresh birth is not refused by this one's claim (lifecycles/node-create.md step 1).
        let mut claimed: Option<NodeId> = None;
        let operation = run.operation.clone();
        let r = self.create_to_ready_claimed(run, req, claim_for, before_ready, before_launch, &mut claimed).await;
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
            build_id = %req.build_id.log(),
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
            build_id = %req.build_id.log(),
            attempt = req.attempt,
            operation = %run.operation,
            node_id = %identity.node_id,
            incarnation_id = %identity.incarnation.as_str(),
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

    async fn create_to_ready_claimed(&self, run: &mut Run<'_>, req: &CreateRequest, claim_for: crate::node_rows::ClaimFor, before_ready: Option<BeforeReady>, before_launch: Option<BeforeLaunch<'_>>, claimed: &mut Option<NodeId>) -> Result<Created, PipelineError> {
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
                // A runtime the operating system holds suspended is not gone (deployment-provider.md
                // §2.5): its birth is kept, never launched over.
                Some(h) => match self.provider.inspect(h).await {
                    DeploymentStatus::Running | DeploymentStatus::OsStopped => true,
                    DeploymentStatus::Exited { .. } | DeploymentStatus::Unknown => false,
                },
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
            let step_span = tracing::Span::current();
            let mut done_awaits: Vec<&'static str> = Vec::with_capacity(3);
            let inherited_key = if inherits {
                step_span.record("awaiting", "transport-key");
                let dir = data_dir.clone();
                let k = crate::record_store::blocking_io(move || load_transport_key(&dir)).await?;
                done_awaits.push("transport-key");
                step_span.record("awaited", done_awaits.join(",").as_str());
                Some(k)
            } else {
                None
            };
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
                // A create over an exited birth's data dir (or fresh dir on lost-data-dir recovery)
                // is node recovery's (node-recovery.md step 4), whose claim inherits the Deleted
                // row's tags with its config.
                let of = claim_for;
                step_span.record("awaiting", "claim");
                rows.claim(&req.node, &id.node_id, &run_operation, Some(&allocated), of)
                    .await
                    .map_err(|e| format!("{}: the name claim refused the birth {}; nothing is launched: {e}", req.node, id.node_id))?;
                done_awaits.push("claim");
                step_span.record("awaited", done_awaits.join(",").as_str());
                *claiming = Some(id.node_id.clone());
            }
            // An inherited data dir keeps its key: it is loaded, never minted; both on the blocking pool.
            let key = match inherited_key {
                Some(k) => k,
                None => {
                    step_span.record("awaiting", "transport-key");
                    let k = EndpointId(crate::admin::load_or_mint_key(&data_dir).await?.public().to_string());
                    done_awaits.push("transport-key");
                    step_span.record("awaited", done_awaits.join(",").as_str());
                    k
                }
            };
            if !req.held_runtimes.is_empty() {
                step_span.record("awaiting", "held-runtimes");
                for row in &req.held_runtimes {
                    crate::storage::FileNodesStorage::put_runtime_in(&data_dir, &self.template.launcher.incarnation, row).await.map_err(|e| e.to_string())?;
                }
                done_awaits.push("held-runtimes");
                step_span.record("awaited", done_awaits.join(",").as_str());
            }
            step_span.record("awaiting", "done");
            Ok(key)
        })
        .await?;
        // A PrepareStorage an earlier run of this operation receipted made the claim: a failure from
        // here withdraws it as well.
        *claimed = Some(id.node_id.clone());
        // The process provider shares the host network namespace and the
        // container provider's network exists per fabric: nothing per node.
        self.step(run, CreateStep::PrepareNetwork.name(), async { Ok(()) }).await?;
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
            mesh_id: self.template.env.get(rdm_core::launch::ENV_MESH_ID).and_then(|v| rdm_core::MeshId::parse(v).ok()),
            mesh_issuer: self.template.mesh_issuer.clone(),
            connections_delete_deadline: self.template.connections_delete_deadline,
        };
        // The launch environment; its TRACEPARENT is taken inside the
        // DeployRuntime step, so the runtime's boot span is that step's child.
        let spec_for = |traceparent: Option<String>| {
            let mut env = self.template.env.clone();
            env.extend(launch.to_env());
            env.insert("RDM_INHERITED_DATA_DIR".into(), if inherits { "1".into() } else { "0".into() });
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
                let spec = spec_for(rdm_transport::current_traceparent());
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
            if f.deployment_id != id.deployment_id.as_str() {
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
                    rdm_core::runtime::fingerprint(&domain)
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
                            self.builds.note_blocked(run_build, None, run_attempt, &run_operation, CreateStep::WaitForNodeReady.name(), &blocker).await;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await
                    }
                }
            }
        })
        .await?;
        Ok(Created { node, handle })
    }

    /// A standalone `node.connections.delete { All }` (node-connections-delete.md):
    /// `node.connections.delete:<path>`. A node-admin target that held a seat had it moved by
    /// node.primary.update before this runs (the fabric-primary's drive, `crate::executor::seat_move`);
    /// the exact birth's mesh-primary sets connections-deleting at admission and holds the birth's
    /// `node.connections.deleted` with its receipt. The process keeps running; nothing
    /// stops it and nothing is deleted.
    pub async fn delete_connections(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("node.connections.delete", &req.build_id, &req.node.name, req.attempt);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.connections.delete:{name}")).await?;
            let node = req.node.clone();
            let ctx = CommandContext { build_id: run.build_id.to_string(), attempt: run.attempt, operation: run.operation.clone() };
            let _: CommandAdmission = self
                .step(&mut run, RetireStep::DeleteConnections.name(), async {
                    let op = self.command_op(&ctx, &node)?;
                    self.lifecycle.connections_deleting(&op).await;
                    match self.observer.send_command(&node, crate::node_commands::NodeCommand::DeleteConnections, &ctx).await {
                        a @ CommandAdmission::ConnectionsDeleted { .. } => Ok(a),
                        other => Err(format!("{}: node.connections.delete {{ All }} ended {other:?}; no node.connections.deleted is held", node.name)),
                    }
                })
                .await?;
            self.complete(&mut run).await
        }
        .instrument(span)
        .await
    }

    /// A standalone stop (node-stop.md): `node.stop:<path>` is the stop-node command, whose reply is the
    /// exact birth's `stopped`: it drained, cut its mesh connections and is parked, its process alive.
    /// The mesh primary publishes `NodeStopped` from that reply, and `Complete` names the birth and the
    /// rafka-time it stopped at. No
    /// runtime exit is asked and no departure is published.
    pub async fn stop(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("stop", &req.build_id, &req.node.name, req.attempt);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.stop:{name}")).await?;
            let mut node = req.node.clone();
            self.stop_steps_with(&mut run, &mut node, &req.handle, StopProof::Stopped, StopEnd::Park { publish_stopped: true }).await?;
            // The `Complete` receipt opens the birth's stopped period: it commits the stop's
            // lifecycle op, the exact birth and the rafka-time it stopped at, which the stopped-node
            // garbage collector ages it from (node-identity-and-state.md §6.9 Age).
            let ctx = self.command_context(&run, crate::node_commands::NodeCommand::Stop, &node);
            let op = self.command_op(&ctx, &node).map_err(|reason| PipelineError { step: RetireStep::Complete.name(), reason })?;
            let _: LifecycleOp = self.step(&mut run, RetireStep::Complete.name(), async { Ok(op) }).await?;
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// A standalone start (node-start.md): `node.start:<path>` is the start-node command to the parked
    /// birth, whose reply is `Started` once it rejoined, then the wait for it to report ready.
    pub async fn start(&self, req: &StartRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("start", &req.build_id, &req.node.name, req.attempt);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.start:{name}")).await?;
            let mut node = req.node.clone();
            self.start_steps(&mut run, &mut node, &req.handle, &req.deployed).await?;
            self.complete(&mut run).await?;
            Ok(())
        }
        .instrument(span)
        .await
    }

    /// A restart (node-restart.md): ONE operation, `node.restart:<path>`, on the live process, which
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
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.restart:{name}")).await?;
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
            if node.parked {
                // The birth is parked: this restart's own earlier attempt stopped it and ended before its
                // start completed (only this restart's attempts carry a restart of a parked birth: the
                // door refuses a fresh one, `node-not-live`). Its stop is done in the world; the run
                // resumes at the start (R-PV1: "`node.start` when start is the pending step").
                tracing::info_span!("rdm.node_admin.node.update.via-restart-resumed", build_id = %req.build_id.log(), attempt = req.attempt, node = %name, node_id = %node.node_id, incarnation_id = %op.incarnation.as_str(), resumes_at = StartStep::StartNode.name())
                    .in_scope(|| tracing::info!("the restart's birth is parked by its own stop: the run resumes at its start"));
            } else {
                // The stop of the live process: it parks and stays, and its `stopped` is the stop's
                // completion. `NodeRestarting` holds it, so no `NodeStopped` is published for it.
                self.stop_steps_with(&mut run, &mut node, &req.handle, StopProof::Stopped, StopEnd::Park { publish_stopped: false }).await?;
            }
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
        self.start_steps_as(run, node, handle, deployed, [StartStep::StartNode.name(), CreateStep::WaitForNodeReady.name()]).await
    }

    /// [`Self::start_steps`] whose two steps are receipted as `steps` (the send, the readiness): a
    /// replace's successor starts in the operation that created it, and its start's receipts are its own.
    async fn start_steps_as(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle, deployed: &Deployed, steps: [&'static str; 2]) -> Result<(), PipelineError> {
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
            .step(run, steps[0], async {
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
                    CommandAdmission::Started {} => {
                        // `start-node` answered `Started`: the birth rejoined. A start with no join to
                        // this executor admitted (its join door saw none) takes node.joined here.
                        if !joined || reported.borrow().is_none() {
                            take_joined().await;
                        }
                        Ok(admission)
                    }
                    // The reissue of the start that already brought this birth back (a later attempt
                    // of the same Build, after its executor was lost): its node.joined and node.started
                    // were taken when it ran; taking node.joined again would set the birth back to
                    // Pending with no node.started to follow. Readiness is read next, as for any start.
                    CommandAdmission::AlreadyAdmitted {} => Ok(admission),
                    CommandAdmission::StartFailed { step, reason } => Err(format!("{}: node.start failed at {step}: {reason}", node.name)),
                    other => Err(format!("{}: start-node was not answered Started ({other:?}); the birth is not known to have rejoined", node.name)),
                }
            })
            .await?;
        let _ = admission;
        let until = Instant::now() + self.timeouts.ready;
        let _: () = self
            .step(run, steps[1], async {
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

    /// `node.delete` of the stopped birth `req.node` (lifecycles/node-delete.md), as ONE operation under
    /// node.delete's one key, `node.delete:<path>`:
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
    pub async fn delete_stopped(&self, req: &RetireRequest) -> Result<(), PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("delete", &req.build_id, &req.node.name, req.attempt);
        async {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.delete:{name}")).await?;
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

    /// Delete a hung birth whose `held-running` hold expired (R-FM1): the first step is
    /// `TerminateRuntime` on the exact runtime (graceful then forced), which produces the `Exited`
    /// proof and records the durable exit-proof row; no RPC command (`drain-node` / `stop-node`) is
    /// sent to the hung process. A `TerminateRuntime` that fails ends the Build Failed at
    /// `TerminateRuntime` by name, its receipt naming where it stopped (node-recovery.md step 4's
    /// end): at its own inspection (the hold is void), at the provider's error, or with exit unproven
    /// (no hold submits the birth's terminate again; a person decides).
    pub(crate) async fn delete_hung(&self, req: &RetireRequest, records: &crate::admin::Records) -> Result<TerminalReceipt, PipelineError> {
        use tracing::Instrument;
        let span = self.pipeline_span("retire", &req.build_id, &req.node.name, req.attempt);
        self.delete_hung_steps(req, records).instrument(span).await
    }

    async fn delete_hung_steps(&self, req: &RetireRequest, records: &crate::admin::Records) -> Result<TerminalReceipt, PipelineError> {
        let name = &req.node.name;
        let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.delete:{name}")).await?;
        let mut node = req.node.clone();
        let handle = &req.handle;
        let incarnation = node.incarnation_id.clone().ok_or_else(|| PipelineError { step: RetireStep::TerminateRuntime.name(), reason: format!("{name} has no known birth") })?;
        let terminal = self.terminate_step_when(&mut run, &node, handle, Terminate::OnlyRunning).await?;
        let exit = records
            .put_exit(&node, terminal.exit_code, "terminate-runtime")
            .await
            .map_err(|reason| PipelineError { step: RetireStep::TerminateRuntime.name(), reason })?;
        records.clear_held_running(&node).await;
        node.exit_proven = true;
        let op = LifecycleOp {
            build_id: req.build_id.to_string(),
            attempt: req.attempt,
            operation: run.operation.clone(),
            node_id: node.node_id.clone(),
            incarnation,
            name: name.clone(),
            event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
        };
        let op = self.step(&mut run, RetireStep::NodeDeleting.name(), async { Ok(op.clone()) }).await?;
        self.lifecycle.deleting(&op).await;
        let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op };
        let op = self
            .step(&mut run, RetireStep::NodeDeleted.name(), async {
                tracing::info_span!(
                    "rdm.node_admin.node.delete.via-exit-proof",
                    build_id = %req.build_id.log(),
                    attempt = req.attempt,
                    node = %name,
                    node_id = %exit.node_id,
                    incarnation_id = %exit.incarnation_id.as_str(),
                    exit_code = exit.exit_code.map(|c| c.to_string()).unwrap_or_else(|| "none".into()),
                    proof = %exit.proof,
                    observed_at_ms = exit.observed_at_ms,
                )
                .in_scope(|| tracing::info!("the exited instance is deleted on the mesh primary's exit proof"));
                self.deleted_row(&node, op.clone()).await
            })
            .await?;
        self.lifecycle.deleted(&op).await;
        self.step(&mut run, RetireStep::RemoveTopologyMembership.name(), async {
            self.sink.remove_birth(name, &op.node_id, &op.incarnation);
            Ok(())
        })
        .await?;
        self.step(&mut run, RetireStep::Complete.name(), async { Ok(()) }).await?;
        Ok(terminal)
    }

    async fn retire_steps(&self, req: &RetireRequest) -> Result<TerminalReceipt, PipelineError> {
        let name = &req.node.name;
        let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.delete:{name}")).await?;
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
                        exit.incarnation_id.as_str(),
                        node.node_id,
                        node.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("no known birth")
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
            // (lifecycles/node-delete.md 1-2, node-stop.md 6). A crashed target likewise: its stop steps
            // are already done, and no node reply or receipt is fabricated for the exited process
            // (node-delete.md step 1); the provider's exact inspection is all that is asked.
            self.terminate_step(&mut run, &node, handle).await?
        } else if self.stop_skipped_os_stopped(&run, &node, handle).await {
            // An OS-suspended target (node-delete.md): it answers no call, so no stop is sent; the
            // delete begins at TerminateRuntime on the exact handle, whose Exited is the proof.
            self.terminate_step(&mut run, &node, handle).await?
        } else {
            // The two halves of the shutdown leg are separate operations (lifecycles/node-connections-delete.md, node-stop.md):
            // drain-node and its node-drained, then stop-node, its node-left reply and the provider's
            // terminal proof.
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
                        build_id = %req.build_id.log(),
                        attempt = req.attempt,
                        node = %name,
                        node_id = %exit.node_id,
                        incarnation_id = %exit.incarnation_id.as_str(),
                        exit_code = exit.exit_code.map(|c| c.to_string()).unwrap_or_else(|| "none".into()),
                        proof = %exit.proof,
                        observed_at_ms = exit.observed_at_ms,
                    )
                    .in_scope(|| tracing::info!("the exited instance is deleted on the mesh primary's exit proof"));
                }
                // The deleted birth's row goes Deleted at its path.name, its config kept, inside the
                // step and before node.deleted (lifecycles/node-delete.md step 3): a node created at that
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
    /// operation (`node.connections.delete:<path>` / `node.stop:<path>`).
    fn command_context(&self, run: &Run<'_>, cmd: crate::node_commands::NodeCommand, node: &Node) -> CommandContext {
        let operation = match cmd {
            crate::node_commands::NodeCommand::DeleteConnections => format!("node.connections.delete:{}", node.name),
            crate::node_commands::NodeCommand::Stop => rdm_client::StopContext::new(run.build_id.clone(), run.attempt, node.name.clone()).operation(),
            crate::node_commands::NodeCommand::Start => rdm_client::StartContext::new(run.build_id.clone(), run.attempt, node.name.clone()).operation(),
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

    /// `NodeDeleted`'s work (lifecycles/node-delete.md step 3, before node.deleted): the writer of the
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

    /// The stop half of a shutdown (node-stop.md): `stop-node` to the exact birth (no implicit
    /// drain), the wait for node-left (the `Stopped` reply of that call), the mesh primary's
    /// `NodeStopped` from that matching reply, then, by `end`, the parked process or the provider's
    /// stop and inspection of the exact runtime, whose `Exited` alone is terminal proof. `proof` says
    /// what the wait step must have seen to complete (a standalone
    /// stop fails it by name unless stop-node was admitted and the
    /// birth's `stopped` reply received).
    /// `end` says what follows: the process parks, or the provider ends it.
    async fn stop_steps_with(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle, proof: StopProof, end: StopEnd) -> Result<Option<TerminalReceipt>, PipelineError> {
        self.stop_steps_as(run, node, handle, proof, end, [RetireStep::StopNode.name(), RetireStep::AwaitNodeLeft.name()]).await
    }

    /// [`Self::stop_steps_with`] whose two steps are receipted as `steps` (the send, the left reply):
    /// a replace stops two nodes in one operation, and each stop's receipts are its own.
    async fn stop_steps_as(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle, proof: StopProof, end: StopEnd, steps: [&'static str; 2]) -> Result<Option<TerminalReceipt>, PipelineError> {
        use crate::node_commands::NodeCommand;
        let ctx = self.command_context(run, NodeCommand::Stop, node);
        let publish_stopped = matches!(end, StopEnd::Park { publish_stopped: true } | StopEnd::Terminate { publish_stopped: true });
        // `stop-node` is held until the birth is parked, and its reply is `stopped`: the birth drained,
        // cut its mesh connections and entered Leaving. The command is `Stopped` or it is not complete.
        // The mesh-primary sets the birth stopping on the stop call's admission and never before it
        // (states.md "Who sets each state"): a command the birth never held sets nothing.
        let admission: CommandAdmission = self
            .step(run, steps[0], async {
                let op = self.command_op(&ctx, node)?;
                let admission = self.observer.send_command(node, NodeCommand::Stop, &ctx).await;
                if admission.admitted() {
                    self.sink.publish(Node { status: NodeStatus::Leaving, ..node.clone() });
                    self.lifecycle.leaving(&op).await;
                }
                Ok(admission)
            })
            .await?;
        if admission.admitted() {
            node.status = NodeStatus::Leaving;
        }
        let _: Completion = self
            .step(run, steps[1], async {
                if !admission.admitted() {
                    // A command the birth never held has no left reply to follow (node-stop.md): a
                    // standalone stop is not complete.
                    if proof == StopProof::Stopped {
                        return Err(format!("{}: stop-node was not admitted ({admission:?}); no left reply can follow, the stop is not complete", node.name));
                    }
                    return Ok(Completion::NotAwaited { admission: format!("{admission:?}") });
                }
                // The stop's reply is its completion (node-stop.md: stop-node answers `Stopped` on its own
                // call): a `StopNode` receipt that recorded `Stopped` is the left reply, whichever executor
                // made the call. Only an admission without it waits for the reply.
                let c = match &admission {
                    CommandAdmission::Stopped { .. } => {
                        crate::node_commands::completion_span(node, NodeCommand::Stop, &ctx, true);
                        Completion::Received {}
                    }
                    _ => tokio::select! {
                        c = self.observer.await_completion(node, NodeCommand::Stop, &ctx, self.timeouts.completion) => c,
                        () = self.until_exited(handle) => Completion::RuntimeExited {},
                    },
                };
                // The stop's completion: a former seat holder's update is said first (R-NS2), then the
                // mesh primary publishes the departure from the matching `stopped`; the node gossips nothing.
                if matches!(c, Completion::Received {}) {
                    let op = self.command_op(&ctx, node)?;
                    self.lifecycle.stop_completed(&op).await;
                    if publish_stopped {
                        self.lifecycle.stopped(&op).await;
                    }
                }
                match (proof != StopProof::Lenient, &c) {
                    (true, c) if *c != Completion::Received {} => Err(format!("{}: stop-node was admitted but its left reply was not received ({c:?}); the stop is not complete", node.name)),
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
    /// is terminal proof, and the receipt binds the exact birth and runtime. A delete of a birth the
    /// provider already proves terminal asks only this.
    pub(crate) async fn terminate_step(&self, run: &mut Run<'_>, node: &Node, handle: &DeploymentHandle) -> Result<TerminalReceipt, PipelineError> {
        self.terminate_step_when(run, node, handle, Terminate::Always).await
    }

    /// [`Self::terminate_step`], firing as `when` says: a hung runtime's hold (R-FM1) terminates only
    /// a runtime the provider reads `Running` at the step's instant (deployment-provider.md §2.5: an
    /// `OsStopped` there voids it, and nothing uncertain is terminated); an `Exited` runtime is proven
    /// with no signal.
    async fn terminate_step_when(&self, run: &mut Run<'_>, node: &Node, handle: &DeploymentHandle, when: Terminate) -> Result<TerminalReceipt, PipelineError> {
        let name = node.name.clone();
        self.step_with(run, RetireStep::TerminateRuntime.name(), async {
            use crate::build_state::{InspectionVerdict, TerminateStage};
            if when == Terminate::OnlyRunning {
                // node.runtime.inspect at the terminate's instant: its span is the evidence (node-runtime-inspect.md).
                let status = self.inspected_at(node, handle, "terminate", "the hold's terminate inspects the exact runtime at its instant and fires only on Running").await;
                match status {
                    DeploymentStatus::Running | DeploymentStatus::Exited { .. } => {}
                    DeploymentStatus::OsStopped => {
                        return Err(StepFailure::at(
                            TerminateStage::Inspection { verdict: InspectionVerdict::OsStopped },
                            format!("{name}: the operating system holds its exact runtime suspended at the terminate's instant (os-stopped): the hold is void and nothing is terminated"),
                        ))
                    }
                    DeploymentStatus::Unknown => {
                        return Err(StepFailure::at(
                            TerminateStage::Inspection { verdict: InspectionVerdict::Unknown },
                            format!("{name}: the provider cannot tell its exact runtime's state at the terminate's instant: the hold is void and nothing is terminated"),
                        ))
                    }
                }
            }
            if let Err(e) = self.provider.terminate(handle, TerminationMode::Graceful { grace: self.timeouts.stop_grace }).await {
                let error = e.to_string();
                return Err(StepFailure::at(TerminateStage::Provider { error: error.clone() }, error));
            }
            // Only an exact inspection that says the runtime exited is terminal proof.
            let exit_code = match self.provider.inspect(handle).await {
                DeploymentStatus::Running => return Err(StepFailure::at(TerminateStage::Unproven, format!("{name} still runs after the stop ladder"))),
                DeploymentStatus::OsStopped => return Err(StepFailure::at(TerminateStage::Unproven, format!("{name}: the operating system holds its runtime suspended after the stop ladder (os-stopped); no terminal proof"))),
                DeploymentStatus::Unknown => return Err(StepFailure::at(TerminateStage::Unproven, format!("{name}: the provider cannot inspect its runtime after the stop ladder; no terminal proof"))),
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
                    // Exit is proven: this failure has no terminate stage, so it stops no resubmission and
                    // the next tick's Exited carries the birth (node-recovery.md step 4).
                    return Err(format!("{name} exited, but the operating system still holds {} after {elapsed:?}", names.join(", ")).into());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tracing::info!(node = %name, released_after_ms = started.elapsed().as_millis() as u64, "the runtime exited and the operating system released its addresses");
            Ok(TerminalReceipt {
                node: name.to_string(),
                node_id: node.node_id.to_string(),
                incarnation_id: node.incarnation_id.as_ref().map(|i| i.to_string()).ok_or_else(|| format!("{name} has no known birth: the exit proves no birth"))?,
                runtime: handle.fact().as_ref().map(RuntimeProof::of),
                exit_code,
            })
        })
        .await
    }

    /// node.runtime.inspect of `node`'s exact runtime by this pipeline's executor for `caller`, at the
    /// caller's instant (node-runtime-inspect.md): the provider's answer, spanned with its verdict.
    async fn inspected_at(&self, node: &Node, handle: &DeploymentHandle, caller: &'static str, why: &'static str) -> DeploymentStatus {
        let status = self.provider.inspect(handle).await;
        let verdict = match &status {
            DeploymentStatus::Running => "running",
            DeploymentStatus::OsStopped => "os-stopped",
            DeploymentStatus::Exited { .. } => "exited",
            DeploymentStatus::Unknown => "unavailable",
        };
        let incarnation = node.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
        tracing::info_span!("rdm.node_admin.node.resolve.via-provider-inspection", node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation, inspector = %self.template.executor(), caller, verdict).in_scope(|| tracing::info!("{why}"));
        status
    }

    /// Whether the delete of the live birth `node` skips its stop (deployment-provider.md §3: "A
    /// `node.delete` of a target whose provider state is `OsStopped` skips the stop by name and begins
    /// at `TerminateRuntime`: the stop cannot execute, and the delete was explicitly commanded"): the
    /// provider reads its exact runtime `OsStopped` at the delete's instant. The skip is named
    /// (`rdm.node_admin.node.delete.via-os-stopped`). Any other answer leaves the ordinary stop to run:
    /// `Running` is the live target, and an `Exited` or `Unknown` runtime is asked nothing more than
    /// the stop's call already asks it.
    async fn stop_skipped_os_stopped(&self, run: &Run<'_>, node: &Node, handle: &DeploymentHandle) -> bool {
        let status = self.inspected_at(node, handle, "delete", "the delete inspects the exact runtime at its instant: an os-stopped one answers no stop").await;
        if status != DeploymentStatus::OsStopped {
            return false;
        }
        let incarnation = node.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
        tracing::info_span!(
            "rdm.node_admin.node.delete.via-os-stopped",
            build_id = %run.build_id.log(),
            attempt = run.attempt,
            node = %node.name,
            node_id = %node.node_id,
            incarnation_id = %incarnation,
            skipped = "StopNode AwaitNodeLeft",
            begins_at = "TerminateRuntime",
        )
        .in_scope(|| tracing::info!("the operating system holds the exact runtime suspended: its stop is skipped by name (a suspended process answers no call) and the delete begins at TerminateRuntime"));
        true
    }

    /// The directory of `node`'s birth as it is now: the data dir its record names, else the
    /// root's `<path>-<node_id>`.
    pub(crate) fn birth_dir(&self, node: &Node) -> PathBuf {
        node.data_dir.as_deref().map(PathBuf::from).unwrap_or_else(|| self.template.data_root.join(format!("{}-{}", node.name, node.node_id)))
    }

    /// node.name.update of the name of `node` as a recipe's step (node-name-update.md;
    /// lifecycles/node-replace.md 3-4), as ONE operation `node.update:<path>`: `UpdateNode`, the
    /// update `put` (node.name.update's own core at the node's mesh-primary, answered `NameUpdated`
    /// or refused by name), then `Complete`.
    pub async fn update_name<F>(&self, build_id: &BuildId, attempt: u32, node: &PathName, put: F) -> Result<(), PipelineError>
    where
        F: std::future::Future<Output = Result<(), String>>,
    {
        use tracing::Instrument;
        let span = self.pipeline_span("update", build_id, node, attempt);
        async {
            let mut run = self.begin(build_id, attempt, node, format!("node.update:{node}")).await?;
            self.step(&mut run, UpdateStep::UpdateNode.name(), put).await?;
            self.step(&mut run, UpdateStep::Complete.name(), async { Ok(()) }).await
        }
        .instrument(span)
        .await
    }
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

    fn receipt(attempt: u32, step: &str, outcome: StepOutcome) -> BuildStepReceipt {
        BuildStepReceipt { mesh: None, build_id: BuildId::parse("3qcfk1kq47m9").unwrap(), attempt, operation: "node.create:mesh1.rpc.1".into(), step: step.into(), outcome, output: None, executor: None }
    }

    #[test]
    fn ready_requires_a_complete_receipt_for_each_runtime_prerequisite_of_this_run() {
        let op = "node.create:mesh1.rpc.1";
        let all: Vec<BuildStepReceipt> = READY_PREREQUISITES.iter().map(|s| receipt(1, s.name(), StepOutcome::Complete)).collect();
        assert!(ready_prerequisites_missing(&all, op, 1).is_empty());
        // One lost receipt is named.
        let lost: Vec<_> = all.iter().filter(|r| r.step != "MakeRuntimeFactAvailableToBirth").cloned().collect();
        assert_eq!(ready_prerequisites_missing(&lost, op, 1), vec!["MakeRuntimeFactAvailableToBirth"]);
        // Another operation's receipts are not this run's.
        let other: Vec<_> = all.iter().cloned().map(|mut r| {
            r.operation = "node.create:mesh1.rpc.2".into();
            r
        }).collect();
        assert_eq!(ready_prerequisites_missing(&other, op, 1).len(), 4);
        // A failed attempt voids what came before it: attempt 3 after a failed
        // attempt 2 cannot stand on attempt 1's receipts.
        let mut failed = all.clone();
        failed.push(receipt(2, "WaitForBind", StepOutcome::failed("exited")));
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
        half.push(receipt(1, "MakeRuntimeFactAvailableToBirth", StepOutcome::failed("disk")));
        assert_eq!(ready_prerequisites_missing(&half, op, 1), vec!["MakeRuntimeFactAvailableToBirth"]);
    }

    /// A provider that answers every inspection `status` and counts the terminates asked of it,
    /// refusing each with `refuse` when it is set.
    struct Answers {
        status: DeploymentStatus,
        terminated: std::sync::atomic::AtomicUsize,
        refuse: Option<String>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for Answers {
        fn kind(&self) -> crate::model::ProviderKind {
            crate::model::ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            "process:cell".into()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, super::super::provider::DeployError> {
            Err(super::super::provider::DeployError::Spawn { node: spec.node.to_string(), reason: "this provider only answers inspections".into() })
        }
        async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode) -> Result<(), super::super::provider::DeployError> {
            self.terminated.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match &self.refuse {
                Some(reason) => Err(super::super::provider::DeployError::Terminate { deployment: h.deployment_id.to_string(), reason: reason.clone() }),
                None => Ok(()),
            }
        }
        async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
            self.status.clone()
        }
        async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), super::super::provider::DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch) -> Result<Found, super::super::provider::DeployError> {
            Ok(Found::NeverLaunched)
        }
    }

    struct NoSink;
    impl TopologySink for NoSink {
        fn publish(&self, _: Node) {}
        fn remove(&self, _: &PathName) {}
        fn remove_birth(&self, _: &PathName, _: &NodeId, _: &IncarnationId) {}
    }

    struct NoObserver;
    #[async_trait::async_trait]
    impl NodeObserver for NoObserver {
        async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
            None
        }
        async fn ready(&self, _: &Node) -> Result<(), String> {
            Err("this cell observes nothing".into())
        }
    }

    /// CONTRACT (deployment-provider.md §2.5: "the terminate step re-inspects the exact runtime at its
    /// instant and fires only on `Running`"; node-recovery.md step 4): a hung birth's hold expired and
    /// its recovery's `TerminateRuntime` runs, but the provider now reads the exact runtime `OsStopped`
    /// (or cannot tell, `Unknown`). The step fails by name, its receipt `Failed` naming the state, and
    /// no terminate is asked of the provider. Must NOT happen: a signal to a runtime the operating
    /// system holds suspended, or to one whose state cannot be told.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_holds_terminate_reinspects_and_never_fires_on_an_os_stopped_or_unknown_runtime() {
        for (status, named) in [(DeploymentStatus::OsStopped, "os-stopped"), (DeploymentStatus::Unknown, "cannot tell")] {
            let provider = Answers { status: status.clone(), terminated: Default::default(), refuse: None };
            let builds = crate::build_state::MemoryBuildStateAdapter::new();
            let build_id = BuildId::mint();
            builds.publish_accepted(&crate::build_state::BuildAccepted { build_id: build_id.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
            let template = cell_template("hold-terminate");
            let joins = Joins::default();
            let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &NoObserver, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
            let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
            node.incarnation_id = Some(IncarnationId::mint());
            let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: crate::model::ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("process:cell".into()) };
            let req = RetireRequest { build_id: build_id.clone(), attempt: 1, node, handle, exit: None };
            let capture = crate::span_capture::Capture::new("i144-e2-hold-terminate");
            let err = {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                pipeline.delete_hung(&req, &crate::admin::Records::default()).await.expect_err("the hold's terminate does not fire")
            };
            let inspected = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
            assert_eq!(inspected.iter().map(|c| (c.attributes.get("caller").cloned().unwrap_or_default(), c.attributes.get("verdict").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("terminate".to_string(), if status == DeploymentStatus::OsStopped { "os-stopped".to_string() } else { "unavailable".to_string() })], "the re-inspection is spanned");
            assert_eq!(err.step, RetireStep::TerminateRuntime.name(), "{err}");
            assert!(err.reason.contains(named) && err.reason.contains("nothing is terminated"), "{status:?}: {err}");
            assert_eq!(provider.terminated.load(std::sync::atomic::Ordering::SeqCst), 0, "{status:?}: no terminate was asked of the provider");
            let receipts = builds.read_build(&build_id).await.unwrap().steps;
            assert!(receipts.iter().any(|r| r.step == RetireStep::TerminateRuntime.name() && matches!(&r.outcome, StepOutcome::Failed { reason, .. } if reason.contains(named))), "{status:?}: the step's receipt is Failed by name: {receipts:?}");
            // i144 R21: the receipt names where it stopped: at its own inspection, which voids the hold.
            let verdict = if status == DeploymentStatus::OsStopped { crate::build_state::InspectionVerdict::OsStopped } else { crate::build_state::InspectionVerdict::Unknown };
            let stages: Vec<_> = receipts.iter().filter(|r| r.step == RetireStep::TerminateRuntime.name()).map(|r| r.outcome.clone()).collect();
            assert!(matches!(stages.as_slice(), [StepOutcome::Failed { stage: Some(crate::build_state::TerminateStage::Inspection { verdict: v }), .. }] if *v == verdict), "{status:?}: the receipt's stage is inspection {verdict:?}: {stages:?}");
        }
    }

    /// CONTRACT (i144 R21; node-recovery.md step 4's end: "A terminate the provider refuses or cannot
    /// prove ends the attempt `Failed { step: TerminateRuntime, reason }` by name, and no hold submits
    /// that birth's terminate again"): a hung birth's hold expired and its `TerminateRuntime` reads the
    /// exact runtime `Running`, so it asks the provider. When the provider returns an error the receipt
    /// is Failed with the typed stage `provider`, carrying that error; when it answers but the runtime
    /// still runs after the stop ladder, the stage is `unproven`. Both stop a hold's resubmission; a
    /// failure at the step's own inspection does not.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_hold_terminate_the_provider_refused_or_could_not_prove_names_its_stage() {
        use crate::build_state::{InspectionVerdict, TerminateStage};
        for (refuse, want) in [(Some("the daemon refused".to_string()), TerminateStage::Provider { error: "terminating deployment".into() }), (None, TerminateStage::Unproven)] {
            let provider = Answers { status: DeploymentStatus::Running, terminated: Default::default(), refuse: refuse.clone() };
            let builds = crate::build_state::MemoryBuildStateAdapter::new();
            let build_id = BuildId::mint();
            builds.publish_accepted(&crate::build_state::BuildAccepted { build_id: build_id.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
            let data_root = tempdir("hold-terminate-stage");
            let template = LaunchTemplate {
                fabric: "fabric1".into(),
                fabric_id: rdm_core::FabricId::mint(),
                executable: "/nonexistent/rafka-rpc-node".into(),
                seeds: vec![],
                launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
                env: BTreeMap::new(),
                data_root: data_root.clone(),
                store_dir: data_root.clone(),
                mesh_issuer: None,
                connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            };
            let joins = Joins::default();
            let timeouts = Timeouts { stop_grace: Duration::from_millis(1), ..Timeouts::default() };
            let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &NoObserver, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts };
            let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
            node.incarnation_id = Some(IncarnationId::mint());
            let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: crate::model::ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("process:cell".into()) };
            let req = RetireRequest { build_id: build_id.clone(), attempt: 1, node, handle, exit: None };
            let capture = crate::span_capture::Capture::new("i144-r21-hold-terminate-stage");
            let err = {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                pipeline.delete_hung(&req, &crate::admin::Records::default()).await.expect_err("the hold's terminate is not proven")
            };
            assert_eq!(err.step, RetireStep::TerminateRuntime.name(), "{err}");
            assert_eq!(provider.terminated.load(std::sync::atomic::Ordering::SeqCst), 1, "{refuse:?}: the provider was asked once");
            let receipts = builds.read_build(&build_id).await.unwrap().steps;
            let stage = receipts.iter().find_map(|r| match &r.outcome {
                StepOutcome::Failed { stage, .. } if r.step == RetireStep::TerminateRuntime.name() => stage.clone(),
                _ => None,
            });
            match (&want, &stage) {
                (TerminateStage::Provider { error: prefix }, Some(TerminateStage::Provider { error })) => assert!(error.contains(prefix.as_str()) && error.contains("the daemon refused"), "the provider's error is carried: {error}"),
                (TerminateStage::Unproven, Some(TerminateStage::Unproven)) => {}
                _ => panic!("{refuse:?}: the receipt's stage is {want:?}: {stage:?} in {receipts:?}"),
            }
            let stage = stage.expect("a stage");
            assert!(stage.stops_resubmission(), "{stage:?} stops a hold's resubmission");
            let stepped = capture.named("rdm.node_admin.deployment.update.via-step").into_iter().find(|c| c.attributes.get("step").map(String::as_str) == Some("TerminateRuntime")).expect("the step's span");
            assert_eq!(stepped.attributes.get("stage").map(String::as_str), Some(stage.name()), "the step's span names the stage: {stepped:?}");
        }
        assert!(!TerminateStage::Inspection { verdict: InspectionVerdict::OsStopped }.stops_resubmission() && !TerminateStage::Inspection { verdict: InspectionVerdict::Unknown }.stops_resubmission(), "a failure at the step's own inspection voids the hold, never stops it");
    }

    /// The launch template of a cell whose provider never spawns: mesh1.admin.1's, its data root a
    /// fresh directory named `cell`.
    fn cell_template(cell: &str) -> LaunchTemplate {
        let data_root = tempdir(cell);
        LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: rdm_core::FabricId::mint(),
            executable: "/nonexistent/rafka-rpc-node".into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root,
            mesh_issuer: None,
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        }
    }

    /// A Build accepted in `builds` for a cell: mesh1 rooted, nothing else.
    async fn accepted_build(builds: &crate::build_state::MemoryBuildStateAdapter) -> BuildId {
        let build_id = BuildId::mint();
        builds.publish_accepted(&crate::build_state::BuildAccepted { build_id: build_id.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        build_id
    }

    /// A provider whose exact runtime the operating system holds suspended until it is terminated:
    /// `OsStopped` until then, `Exited` (SIGKILL's code) after.
    struct Suspended {
        terminated: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for Suspended {
        fn kind(&self) -> crate::model::ProviderKind {
            crate::model::ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            "process:cell".into()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, super::super::provider::DeployError> {
            Err(super::super::provider::DeployError::Spawn { node: spec.node.to_string(), reason: "this provider only answers inspections".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), super::super::provider::DeployError> {
            self.terminated.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
            if self.terminated.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                DeploymentStatus::OsStopped
            } else {
                DeploymentStatus::Exited { code: Some(137) }
            }
        }
        async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), super::super::provider::DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch) -> Result<Found, super::super::provider::DeployError> {
            Ok(Found::NeverLaunched)
        }
    }

    /// An observer of one birth that answers `stop-node` with `stop`, answers `start-node` with
    /// `Started`, reports it ready, and records every command sent to it, in order.
    struct Birth {
        stop: CommandAdmission,
        sent: std::sync::Mutex<Vec<crate::node_commands::NodeCommand>>,
    }

    #[async_trait::async_trait]
    impl NodeObserver for Birth {
        async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
            None
        }
        async fn ready(&self, _: &Node) -> Result<(), String> {
            Ok(())
        }
        async fn send_command(&self, _: &Node, cmd: crate::node_commands::NodeCommand, _: &CommandContext) -> CommandAdmission {
            self.sent.lock().unwrap().push(cmd);
            match cmd {
                crate::node_commands::NodeCommand::Stop => self.stop.clone(),
                crate::node_commands::NodeCommand::Start => CommandAdmission::Started {},
                crate::node_commands::NodeCommand::DeleteConnections => CommandAdmission::NotSent { reason: "this cell sends no node.connections.delete".into() },
            }
        }
    }

    fn process_handle() -> DeploymentHandle {
        DeploymentHandle { deployment_id: DeploymentId::mint(), provider: crate::model::ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("process:cell".into()) }
    }

    /// CONTRACT (deployment-provider.md §2.5: "An explicit `node.delete` of an OS-stopped birth stays
    /// available: it skips the Node RPC stop by name (a suspended process answers no call), runs
    /// `TerminateRuntime` on the exact handle, takes the exit proof and continues"; node-delete.md
    /// "OS-suspended target"): the delete of a live (not parked) birth whose provider reads its exact
    /// runtime `OsStopped` at the delete's instant sends it no command; the inspection is spanned
    /// (`caller = delete`, `os-stopped`), the skip is named (`rdm.node_admin.node.delete.via-os-stopped`),
    /// and the run's receipts are NodeDeleting, TerminateRuntime, NodeDeleted,
    /// RemoveTopologyMembership, Complete: one terminate of the exact runtime, whose `Exited` is the
    /// proof. Must NOT happen: a `stop-node` or `node.connections.delete` sent to the suspended birth, a
    /// StopNode or AwaitNodeLeft receipt.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn an_explicit_delete_of_an_os_stopped_birth_sends_no_stop_and_terminates_its_exact_runtime() {
        let provider = Suspended { terminated: Default::default() };
        let observer = Birth { stop: CommandAdmission::NotSent { reason: "a suspended birth answers no call".into() }, sent: Default::default() };
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let build_id = accepted_build(&builds).await;
        let template = cell_template("os-stopped-delete");
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &observer, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
        node.status = NodeStatus::PendingReconnect;
        node.incarnation_id = Some(IncarnationId::mint());
        let req = RetireRequest { build_id: build_id.clone(), attempt: 1, node, handle: process_handle(), exit: None };
        let capture = crate::span_capture::Capture::new("i144-e3-os-stopped-delete");
        let terminal = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            pipeline.retire(&req).await.expect("the delete of an os-stopped birth completes")
        };
        assert_eq!(terminal.exit_code, Some(137), "the terminate's Exited is the proof: {terminal:?}");
        assert!(observer.sent.lock().unwrap().is_empty(), "no command is sent to the suspended birth: {:?}", observer.sent.lock().unwrap());
        assert_eq!(provider.terminated.load(std::sync::atomic::Ordering::SeqCst), 1, "one terminate of the exact runtime");
        let inspected = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
        assert!(inspected.iter().any(|c| c.attributes.get("caller").map(String::as_str) == Some("delete") && c.attributes.get("verdict").map(String::as_str) == Some("os-stopped")), "the delete's inspection is spanned: {inspected:?}");
        assert_eq!(capture.named("rdm.node_admin.node.delete.via-os-stopped").len(), 1, "the skipped stop is named once");
        let steps: Vec<String> = builds.read_build(&build_id).await.unwrap().steps.into_iter().map(|r| r.step).collect();
        assert_eq!(steps, ["NodeDeleting", "TerminateRuntime", "NodeDeleted", "RemoveTopologyMembership", "Complete"].map(String::from).to_vec(), "the delete begins at TerminateRuntime");
    }

    /// CONTRACT (R-PV1: "The pending lifecycle action is resumed per its receipts: `node.start` when
    /// start is the pending step (the node did stop)"; node-restart.md steps 2-4): a restart whose
    /// earlier attempt parked the birth (its stop completed; its start failed) is resumed by its next
    /// attempt at the start: the birth is parked, so `start-node` is the one command sent, and the run's
    /// receipts are NodeRestarting, StartNode, WaitForNodeReady, Complete. Must NOT happen: a
    /// `stop-node` sent again to the parked birth, a StopNode or AwaitNodeLeft receipt in the resumed run.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_restart_of_a_birth_its_own_restart_parked_resumes_at_start() {
        let provider = Answers { status: DeploymentStatus::Running, terminated: Default::default(), refuse: None };
        let observer = Birth { stop: CommandAdmission::Indeterminate { reason: "a resumed restart sends its parked birth no stop".into() }, sent: Default::default() };
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let build_id = accepted_build(&builds).await;
        let template = cell_template("restart-resumed");
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &observer, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
        node.status = NodeStatus::Leaving;
        node.parked = true;
        node.incarnation_id = Some(IncarnationId::mint());
        node.endpoint_id = Some(EndpointId("key-mesh1.rpc.1".into()));
        let handle = process_handle();
        let deployed = Deployed {
            name: node.name.clone(),
            node_id: node.node_id.clone(),
            incarnation: node.incarnation_id.clone().unwrap(),
            supersedes: None,
            endpoint_id: EndpointId("key-mesh1.rpc.1".into()),
            runtime: handle.fact().expect("a process handle names its exact runtime"),
            data_dir: template.data_root.join("mesh1.rpc.1").display().to_string(),
            last_accepted_name: Some(node.name.clone()),
            held: true,
        };
        let req = StartRequest { build_id: build_id.clone(), attempt: 2, node, handle, deployed };
        pipeline.restart(&req).await.expect("the resumed restart completes");
        assert_eq!(*observer.sent.lock().unwrap(), vec![crate::node_commands::NodeCommand::Start], "start-node is the one command the parked birth is sent");
        let steps: Vec<String> = builds.read_build(&build_id).await.unwrap().steps.into_iter().filter(|r| r.attempt == 2).map(|r| r.step).collect();
        assert_eq!(steps, ["NodeRestarting", "StartNode", "WaitForNodeReady", "Complete"].map(String::from).to_vec(), "the resumed restart begins at its start");
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
