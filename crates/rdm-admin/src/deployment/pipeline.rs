//! `DeploymentPipeline`: create or delete one runtime (PRD §8;
//! mesh-control-plane.md §6).
//!
//! ```text
//! create: AllocateIdentity -> PrepareStorage -> PrepareNetwork
//!   -> DeployRuntime -> RegisterExactRuntimeHandle -> ResolveProviderControlDomain
//!   -> MakeRuntimeFactAvailableToBirth -> WaitForBind
//!   -> PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata -> WaitForMeshJoin
//!   -> WaitForNodeReady -> Complete
//! delete: StopNode (stop-node to the exact birth: its node.connection.delete { All }, then its
//!   transport cut; the `Stopped` reply carries the All's ConnectionsReceipt)
//!   -> AwaitNodeLeft (its `Stopped` reply, bounded; a deadline is an arm, never a failure)
//!   -> TerminateRuntime (the provider's stop and exact inspection: only `Exited` is terminal proof)
//!   -> RemoveTopologyMembership -> Complete
//! delete of a crashed target: NodeDeleting -> TerminateRuntime (the exact exited verdict, nothing sent)
//!   -> NodeDeleted (from the mesh-primary's recorded exited verdict) -> RemoveTopologyMembership -> Complete
//! update: UpdateNode (a Build's node.name.update of a name through node.name.update's own core) -> Complete
//! ```
//!
//! Node recovery (node-recovery.md "Steps") is two operations of these, never a sequence of its
//! own: the delete of the crashed target, then the create over its data dir. A Build's steps
//! (lifecycles/node-replace.md) are likewise their own operations, in order.
//!
//! A removal releases identity, never storage: the data dir and its endpoint key stay
//! (node-delete.md "Storage"), and no step here deletes a node's storage.
//!
//! Node-admin is the brain: identity, endpoints, storage and readiness are
//! decided here; the provider only realises or deletes the runtime. Every
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
//! ([`READY_PREREQUISITES`]), and `WaitForMeshJoin` completes from the
//! validated report of exactly that birth, carrying that fact and data dir
//! (node-create.md step 5), never from gossip.
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
    /// publishes with its own topology digest.
    PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata,
    /// A mesh's first admin only: the fabric primary applies `MeshStatus::Pending` at this exact
    /// birth, through its Status door, as soon as the birth is bound and published and before it
    /// is heard or asked to be Ready (e4.s11): a peer mesh's admin is heard on the backbone only
    /// once it is its mesh's primary, which needs Ready, which needs this.
    ApplyMeshPending,
    /// Wait for the birth's own digest to reach the admin's topology view.
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
    /// `start-node` to the stopped birth: the reply is `Started` once it has rejoined as itself.
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

/// The steps of a Build's `node.name.update` as a Build operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStep {
    /// A Build's name put.
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

/// The steps of a Build's `node.mesh.leave` as a Build operation (`node.mesh.leave:<path>`,
/// node-mesh-leave.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaveStep {
    /// The node's mesh-primary carried `LeaveMesh` to the exact birth and holds its `node.mesh.left`.
    LeaveMesh,
    /// The leave is done.
    Complete,
}

impl LeaveStep {
    /// The step's name as it appears in receipts and spans.
    pub fn name(self) -> &'static str {
        match self {
            Self::LeaveMesh => "LeaveMesh",
            Self::Complete => "Complete",
        }
    }
}

/// The steps of creating a mesh (`create-mesh:<mesh>`, node-identity-and-state.md §2.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateMeshStep {
    /// The mesh's name is claimed at its key ([`crate::storage::claim_mesh`]). Receipted only when the claim is
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

/// One step of deleting a node, in the order of [`DeleteStep::ORDER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteStep {
    /// The pre-notice (a removal only): the executor holds the operation; the node is
    /// found and not routable.
    NodeDeleting,
    /// `node.connection.delete { All }` to the exact birth through its mesh-primary: graceful
    /// closure of every open connection; the process keeps running.
    DeleteConnections,
    /// `stop-node` to the exact birth: enter `Leaving` and shut down, with no implicit connections delete.
    StopNode,
    /// Wait, bounded, for node-left: the `Stopped` reply of the birth's `stop-node` call (R-ON1;
    /// the birth makes no upward node-left call).
    AwaitNodeLeft,
    /// Stop the runtime through the provider.
    TerminateRuntime,
    /// The departure (a removal only): the provider proved the runtime terminal; the
    /// node has left.
    NodeDeleted,
    /// Remove the node from the local topology.
    RemoveTopologyMembership,
    /// The node is deleted.
    Complete,
}

impl DeleteStep {
    /// Every delete step in order.
    pub const ORDER: [DeleteStep; 7] = [
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
    /// Its receipts are written on the blocking pool (`blocking::blocking_io`).
    pub async fn begin_in(node: &PathName, data_dir: &std::path::Path, deployment_id: &DeploymentId, container_domain: Option<String>) -> Result<Self, PipelineError> {
        let (node, data_dir, deployment_id) = (node.clone(), data_dir.to_path_buf(), deployment_id.clone());
        crate::blocking::blocking_io(move || Self::begin_on_disk(&node, &data_dir, &deployment_id, container_domain)).await
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
    /// and this data dir into the projection the topology view publishes.
    /// Its receipt is written on the blocking pool (`blocking::blocking_io`).
    pub async fn publish(self, publish: impl FnOnce(&RuntimeFact, String) + Send + 'static) -> Result<RuntimeFact, PipelineError> {
        crate::blocking::blocking_io(move || self.publish_on_disk(publish)).await
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
            code = tracing::field::Empty,
            detail = tracing::field::Empty,
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
        span.record("code", if r.is_ok() { "OK" } else { "FAILED_PRECONDITION" }).record("detail", if r.is_ok() { "complete" } else { "failed" });
        r.map(|(f, _)| f).map_err(|reason| PipelineError { step: step.name(), reason , code: rdm_core::outcome::Code::FailedPrecondition })
    }
}

/// The Day-0 adoption steps `data_dir` holds no `Complete` receipt for.
/// `Err` names the receipts file when it cannot be read or parsed.
/// Read on the blocking pool (`blocking::blocking_io`).
pub(crate) async fn adoption_missing(data_dir: &std::path::Path) -> Result<Vec<&'static str>, String> {
    let file = data_dir.join(ADOPTION_RECEIPTS);
    let receipts: Vec<AdoptionReceipt> = crate::blocking::blocking_io(move || match std::fs::read(&file) {
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

/// What the pipeline asks of the live mesh (gossip topology and Node RPC).
#[async_trait::async_trait]
pub trait NodeObserver: Send + Sync {
    /// What this exact birth (`node_id`, `incarnation`) publishes with its own topology digest;
    /// `None` until it has joined fabric membership. Read by the lost-executor rule alone
    /// (`claimed_allocation`: a birth a lost executor launched that has joined stands); the create's
    /// own waits, `WaitForBind` and `WaitForMeshJoin`, complete from the birth's validated report
    /// (node-create.md step 5), never from here.
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication>;
    /// Is the node serving?
    async fn ready(&self, node: &Node) -> Result<(), String>;
    /// The named blocker the node reports for its Ready (its `pre_node_ready` hook's
    /// `Blocked` outcome, with the event it retries on), or `None`. Its only source is the subject's
    /// Status `Blocked` answer to `ProbeNodeState`. A node that is only still Pending, with no hook
    /// blocker, has none, and neither has a node whose door answers `NotReady`: `ready` fails for it,
    /// the step waits, and nothing is framed `Blocked`.
    async fn ready_blocker(&self, _node: &Node) -> Option<String> {
        None
    }
    /// Send `cmd` (`node.connection.delete` or `stop-node`) to the exact birth over Node RPC, opening the
    /// command before the send so its completion finds it. Answers what the call established,
    /// never a guess: the birth admitted it, or by name why nothing was established. The reply is
    /// admission, not completion.
    async fn send_command(&self, node: &Node, _cmd: crate::node_commands::NodeCommand, _ctx: &CommandContext) -> CommandAdmission {
        CommandAdmission::NotSent { reason: format!("this observer holds no Node RPC client to command {}", node.name), code: rdm_core::outcome::Code::Unavailable }
    }
    /// Wait up to `within` for the commanded birth's completion at this admin: its
    /// `node.connections.deleted`, or node-left, which is the `Stopped` reply of its `stop-node` call.
    async fn await_completion(&self, node: &Node, _cmd: crate::node_commands::NodeCommand, _ctx: &CommandContext, _within: Duration) -> Completion {
        Completion::NotAwaited { admission: format!("this observer awaits no completion of a command to {}", node.name) }
    }
}

/// The runtime part of a birth's digest: what its validated report carries, and what was made
/// available to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publication {
    /// The runtime fact the birth reports.
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
/// done, so a later attempt of the same operation (a second stop of one node) runs afresh.
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
/// "Storage"). It stays the receipt's shape so a Build message's positional layout
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
    /// `Persistent { disposition: Preserve }`: left where it is.
    Preserved {},
    /// The Build holds no create spec for this path: left where it is, never destroyed on missing evidence.
    PreservedNoMeta {},
}

/// What one directed command (`node.connection.delete`, `stop-node`, `start-node`) established at
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
    /// The birth answered `stop-node` with `Stopped`: its `node.connection.delete { All }`
    /// completed, it cut its transport and is stopped, and this is what that `All` established.
    Stopped {
        /// The `All`'s receipt.
        receipt: rdm_core::status::ConnectionsReceipt,
    },
    /// The birth answered `node.connection.delete { All }` with `node.connections.deleted`.
    ConnectionsDeleted {
        /// What the closure established.
        receipt: rdm_core::status::ConnectionsReceipt,
    },
    /// The birth answered `start-node` with `Started`: it rejoined as itself.
    Started {},
    /// The birth answered `start-node` with `StartFailed`: it is stopped still, and nothing after
    /// `step` ran.
    StartFailed {
        /// The step that failed.
        step: String,
        /// Why.
        reason: String,
        /// Standard code of the original typed command outcome.
        #[serde(default = "rdm_core::outcome::legacy_failed_code")]
        code: rdm_core::outcome::Code,
    },
    /// The call never reached the birth (no route, dial refused, connection lost before the send).
    NotSent {
        /// Why the call was not sent.
        reason: String,
        /// Standard code of the original typed command outcome.
        #[serde(default = "rdm_core::outcome::legacy_failed_code")]
        code: rdm_core::outcome::Code,
    },
    /// The call may have reached the birth; no reply came back.
    Indeterminate {
        /// Why the outcome is unknown.
        reason: String,
        /// Standard code of the original typed command outcome.
        #[serde(default = "rdm_core::outcome::legacy_failed_code")]
        code: rdm_core::outcome::Code,
    },
    /// The birth, or the op, refused the call by name (a stale incarnation, an unserved op).
    Refused {
        /// The refusal's name.
        reply: String,
        /// Standard code of the original typed command outcome.
        #[serde(default = "rdm_core::outcome::legacy_failed_code")]
        code: rdm_core::outcome::Code,
    },
}

impl CommandAdmission {
    /// The original command's standard code, retained beside its explanation.
    pub fn code(&self) -> rdm_core::outcome::Code {
        use rdm_core::outcome::Code;
        match self {
            Self::StartFailed { code, .. } | Self::NotSent { code, .. } | Self::Indeterminate { code, .. } | Self::Refused { code, .. } => *code,
            Self::AlreadyAdmitted {} => Code::AlreadyExists,
            Self::Stopped { receipt } | Self::ConnectionsDeleted { receipt } if matches!(receipt, rdm_core::status::ConnectionsReceipt::Deadline { .. }) => Code::DeadlineExceeded,
            _ => Code::Ok,
        }
    }
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
    let code = out.code();
    match out {
        RpcOutcome::Reply(r) => match r.value() {
            StatusReply::Applied => CommandAdmission::Admitted {},
            StatusReply::AlreadyApplied => CommandAdmission::AlreadyAdmitted {},
            StatusReply::Stopped { receipt } => CommandAdmission::Stopped { receipt: receipt.clone() },
            StatusReply::ConnectionsDeleted { receipt: Some(receipt), .. } => CommandAdmission::ConnectionsDeleted { receipt: receipt.clone() },
            StatusReply::Started => CommandAdmission::Started {},
            StatusReply::StartFailed { step, reason } => CommandAdmission::StartFailed { step: step.clone(), reason: reason.clone(), code },
            other => CommandAdmission::Refused { reply: format!("{}: {other:?}", other.name()), code },
        },
        RpcOutcome::NotSent(n) => CommandAdmission::NotSent { reason: format!("{:?}", n.reason()), code },
        RpcOutcome::Indeterminate(i) => CommandAdmission::Indeterminate { reason: format!("{:?}", i.reason()), code },
        RpcOutcome::Unserved(u) => CommandAdmission::Refused { reply: format!("unserved op {:#04x}", u.op()), code },
        RpcOutcome::RejectedStale(r) => CommandAdmission::Refused { reply: format!("stale target {}", r.target_node_id()), code },
    }
}

/// Whether the exact birth's completion (its `node.connections.deleted`, or node-left: the `Stopped`
/// reply of its `stop-node` call) reached this admin: the AwaitNodeLeft step's receipt for a stop. A missing completion is indeterminate, never
/// success; the provider's terminal proof is what ends a stop.
///
/// An arm with no field is written `Arm {}`: an internally tagged enum ignores a field on a unit arm
/// even under `deny_unknown_fields`, and an empty field list refuses it. The JSON is the same.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "arm", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Completion {
    /// The matching completion call was accepted.
    Received {},
    /// The lifecycle connections delete deadline passed with no completion call.
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
    /// An unreached birth's delete (node-recovery.md step 3): what the provider read of the runtime at
    /// the step's own instant, `running` (then terminated), `exited`, `os-stopped` or `unavailable`.
    /// The re-created node takes the inherited data dir only on `exited` (`R-FM2`). `None` for every
    /// other delete. Positional in a Build message, after `exit_code`.
    #[serde(default)]
    pub verdict: Option<String>,
    /// The data dir the birth ran over, as the delete resolved its record: a replacement's create of
    /// the member's successor takes it over whole (build-deployment.md §3: "`Persistent` inherits the
    /// predecessor's data dir and endpoint key"). `None` when the record names none. Positional on the Build
    /// topic's wire, after `verdict`.
    #[serde(default)]
    pub data_dir: Option<String>,
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
    /// `node.connection.delete:<path>` or `node.stop:<path>`.
    pub operation: String,
}

/// Publishes the lifecycle events of a removal or a stop.
///
/// Every method is required: an implementation states what it does with each event, so a wrapper
/// cannot drop one silently by leaving it to a default.
#[async_trait::async_trait]
pub trait LifecycleEvents: Send + Sync {
    /// The removal is under way.
    async fn deleting(&self, op: &LifecycleOp);
    /// The removal completed.
    async fn deleted(&self, op: &LifecycleOp);
    /// The exact birth answered `stop-node` with `stopped`: it is stopped. The mesh primary publishes
    /// the departure as `NodeStopped` in its in-flight overlay; the node gossips nothing.
    async fn stopped(&self, op: &LifecycleOp);
    /// The stopped birth answered `start-node` with `Started`: this executor took its `node.joined`
    /// (`op` is the start's operation on that exact birth). Its stop ends here and nowhere else
    /// (node-start.md step 2), the birth is held `Pending`, and this mesh's projection says so.
    async fn joined(&self, op: &LifecycleOp);
    /// The birth admitted the `stop-node` command (its call came back admitted): the mesh-primary
    /// sets it stopping (states.md "Who sets each state"), and its projection carries that. No frame
    /// is published: `NodeLeaving` is a reserved position (gossip.md frame catalog).
    async fn leaving(&self, op: &LifecycleOp);
    /// The Rafka-time the events are stamped with (`event_at_rafka_ms`): the clock the process
    /// composed for its topology view, never a clock of the pipeline's own.
    fn now_rafka_ms(&self) -> u64;
}

/// No events: a pipeline under test with no topology view, and so no rafka-time.
pub struct NoLifecycleEvents;

#[async_trait::async_trait]
impl LifecycleEvents for NoLifecycleEvents {
    async fn deleting(&self, _op: &LifecycleOp) {}
    async fn deleted(&self, _op: &LifecycleOp) {}
    async fn stopped(&self, _op: &LifecycleOp) {}
    async fn joined(&self, _op: &LifecycleOp) {}
    async fn leaving(&self, _op: &LifecycleOp) {}
    /// A pipeline with no topology view holds no rafka-time: its events carry zero, and no OS clock
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
    /// The binding `executable` is the file of, as the create launching from this template runs it:
    /// its name claim records it on the node's row ([`crate::storage::NodeRow::executable`]).
    /// `None` for a template no create launches from.
    pub binding: Option<rdm_core::binding::Binding>,
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
    /// The issuing material of its mesh a node-admin is launched with (fabric-certs.md: every
    /// launched admin), set on the template of a node-admin's launch from the launching admin's
    /// [`CertIssuer::issuing_material`](crate::certs::CertIssuer::issuing_material). `None` for every
    /// other launch and for a launcher configured with no certs.
    pub issuing: Option<rdm_core::launch::IssuingMaterial>,
    /// The fabric's root set the launching admin holds (fabric-certs.md §chain), the holder its own
    /// admissions read: every launch carries the set as it stands at the launch (`RDM_ROOTS`), one
    /// root, or two during a root rotation. Empty for a launcher configured with no certs.
    pub roots: rdm_transport::connections::RootSet,
    /// The bound of a `node.connection.delete { All }` every launch carries: the launching admin's
    /// own (`AdminConfig::connections_delete_deadline`).
    pub connections_delete_deadline: Duration,
    /// The app's declared secrets and the values the launching admin holds (Q150): a launch sets the
    /// values its kind is named for, withholds every declared name from inheritance, and a node-admin's
    /// launch carries every value in custody (`RDM_DECLARED_SECRETS`).
    pub declared: super::declared::DeclaredSecrets,
}

impl LaunchTemplate {
    /// `text` with every secret this template launches with replaced by name
    /// ([`super::declared::scrub`]): each declared value the launching admin holds, in every spelling,
    /// the custody document, and the root, the issuing key and the fabric CA's key.
    pub fn scrub(&self, text: &str) -> String {
        let mut secrets = self.declared.spellings();
        if let Some(m) = &self.issuing {
            secrets.extend([(rdm_core::launch::ENV_CA_SECRET, &m.root), (rdm_core::launch::ENV_ISSUING_KEY, &m.key), (rdm_core::launch::ENV_FABRIC_CA_KEY, &m.ca_key)].map(|(k, v)| (k.to_string(), v.0.clone())));
        }
        super::declared::scrub(text, &secrets)
    }

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
    /// The path's entry in the accepted Build the fabric-primary put on the create
    /// ([`crate::build::EntrySpec`]): a plain create's row takes its tags and config at the claim.
    pub entry: Option<crate::build::EntrySpec>,
}

/// Work the executor does at the birth between its mesh join and its Ready: the fabric primary's
/// hand-off of its mesh's state at a mesh's first admin (Pending for a new mesh; the mesh's current
/// state for the admin created for an existing one). Runs as its own receipted step
/// (`ApplyMeshPending`), which records the state applied; a failure fails the create, so nothing
/// downstream starts under an assumed state.
pub(crate) type PreNodeReady = std::sync::Arc<dyn Fn(Node) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<rdm_core::status::MeshState, String>> + Send>> + Send + Sync>;

/// Work a create awaits inside its `DeployRuntime` step right before it launches the new runtime,
/// and only then: a step an earlier attempt receipted (its runtime still runs and is reused) and a
/// runtime the provider finds this deployment already started run none of it. A create over an
/// exited birth's data dir targets that birth here (node-recovery.md "Delete targets the old instance
/// before the new join"): the new runtime carries the same endpoint key, so the target is done before
/// it exists and never once it does.
pub(crate) type BeforeLaunch<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;

/// A request to delete one node.
#[derive(Debug, Clone)]
pub struct DeleteRequest {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
    /// The node's current record.
    pub node: Node,
    /// The runtime handle of the birth.
    pub handle: DeploymentHandle,
    /// An unreached birth (node-recovery.md sequence step 3): node investigation's verdict was not
    /// `reached`. Nothing is sent to it: `TerminateRuntime` first when its runtime runs at the step's
    /// instant, then node.deleted, waiting for no proof of termination. `false`: a live or stopped
    /// target.
    pub unreached: bool,
}

/// A request to start a stopped birth: the same process rejoins as itself.
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

/// What follows a stop's `stopped`: the process stays stopped (a standalone stop), or the
/// provider ends it and proves the exit (a delete, a mesh leave). `publish_stopped` is
/// whether the mesh primary publishes `NodeStopped` from the `stopped` reply: every whole node.stop
/// does (node-stop.md step 5), a delete's and a mesh leave member's included.
#[derive(Debug, Clone, Copy)]
pub(crate) enum StopEnd {
    Stop { publish_stopped: bool },
    Terminate { publish_stopped: bool },
}

/// When a `TerminateRuntime` step fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Terminate {
    /// On any runtime that is not exited: a delete's terminate, whose `Exited` is the proof
    /// node.deleted is published from.
    Always,
    /// Only on a runtime the provider reads `Running` at the step's instant: an unreached birth's
    /// delete (node-recovery.md step 3: `TerminateRuntime` first when the verdict was `running`;
    /// node-runtime-inspect.md: "at the step's own instant, before it asks the provider to
    /// terminate"). Any other read terminates nothing and waits for nothing.
    Unreached,
    /// As [`Self::Unreached`], for an explicit node.delete of a birth no call reaches: an `os-stopped`
    /// runtime is terminated `Immediate` (deployment-provider.md §2.5: "An explicit `node.delete` of an
    /// OS-stopped birth ... runs `TerminateRuntime` on the exact handle as `terminate(Immediate)`";
    /// node-runtime-inspect.md "What follows"), where node recovery leaves it as it is.
    Explicit,
    /// Never: fabric recovery's backbone arm terminates nothing in a mesh it is outside of
    /// (fabric-recovery.md step 3, Q148; node-delete.md). The step still reads the exact runtime at
    /// its instant, and the receipt names that read, so the create's data dir follows it (`R-FM2`).
    Backbone,
}

/// What a stop's wait step must have seen to complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopProof {
    /// Whatever the birth answered: the shutdown leg of a delete or a mesh leave member, which the
    /// provider's terminal proof judges.
    Lenient,
    /// stop-node admitted and `stopped` received, or the step fails by name: a whole node.stop, a
    /// standalone one (a Build's stop included), whose only proof is
    /// that reply (node-stop.md "Completion": the matching `node.stopped`).
    Stopped,
}

/// A step that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineError {
    /// The standard code captured at failure.
    pub code: rdm_core::outcome::Code,
    /// The step.
    pub step: &'static str,
    /// Why it failed.
    pub reason: String,
}

/// A step's failure as its receipt records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepFailure {
    pub code: rdm_core::outcome::Code,
    /// Why the step failed.
    pub reason: String,
}

impl From<String> for StepFailure {
    fn from(reason: String) -> Self {
        Self { reason, code: rdm_core::outcome::Code::FailedPrecondition }
    }
}

impl StepFailure {
    fn coded(code: rdm_core::outcome::Code, reason: String) -> Self { Self { reason, code } }
}

/// A provider's failure as its step's: a call past its deadline stays `DeadlineExceeded` (Q304),
/// never a refusal or an absence.
impl From<super::provider::DeployError> for StepFailure {
    fn from(e: super::provider::DeployError) -> Self {
        let code = match e {
            super::provider::DeployError::Deadline { .. } => rdm_core::outcome::Code::DeadlineExceeded,
            _ => rdm_core::outcome::Code::FailedPrecondition,
        };
        Self { reason: e.to_string(), code }
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

/// Where a birth reported it bound: the transport address its own digest names and the listeners its
/// join report carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bound {
    /// The transport address the birth bound.
    pub transport: SocketAddr,
    /// The non-Iroh listeners the birth bound, by name.
    pub listeners: Vec<(String, SocketAddr)>,
}

impl Bound {
    fn of(r: &crate::join::JoinReport) -> Result<Self, String> {
        let transport = r.digest.node.transport_addr;
        if transport.ip().is_unspecified() || transport.port() == 0 {
            return Err(format!("it reported the transport address {transport}, which is not an address a peer can dial"));
        }
        // The listeners the join verified against the assignment (Q151: exactly the assigned names, each
        // on its assigned address with a port that is not 0; `Deployed::verify`). A node-admin opens no
        // listener (rdm-crate-and-test-layout.md §2): its `admin_api_base` is config.
        Ok(Self { transport, listeners: r.listeners.clone() })
    }
}

/// The pipeline that creates and deletes runtimes.
pub struct DeploymentPipeline<'a> {
    /// The provider that realises runtimes.
    pub provider: &'a dyn DeploymentProvider,
    /// The births this admin deployed and awaits a `JoinNode` from.
    pub joins: &'a Joins,
    /// The observer of births' topology view.
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
    /// Always `None`: a create mints a new node and supersedes no birth. The field keeps its position in the receipt's wire form
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
                return Err(PipelineError { step: "ReadBuild", reason , code: rdm_core::outcome::Code::FailedPrecondition });
            }
        };
        // The same attempt's receipts another executor recorded for this operation hand on too: the lost final
        // primary's delete the fabric-primary ran to its exact Exited, which the new mesh-primary, its mesh's
        // writer, runs on in the same attempt (mesh-delete.md "Resume after the primary's loss" steps 1 and 3:
        // "Completed member receipts stand").
        let me = self.template.executor();
        let earlier: Vec<BuildStepReceipt> = view.steps.into_iter().filter(|r| r.operation == operation && (r.attempt < attempt || (r.attempt == attempt && r.executor.as_deref().is_some_and(|e| e != me)))).collect();
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
    fn step<'s, T, Fut>(&'s self, run: &'s mut Run<'_>, step: &'static str, work: Fut) -> futures_util::future::BoxFuture<'s, Result<T, PipelineError>>
    where
        T: Serialize + DeserializeOwned + Send + 's,
        Fut: std::future::Future<Output = Result<T, String>> + Send + 's,
    {
        // Boxed where it is made: a step's state machine holds its work, and the operation that runs
        // it holds only the box on its worker's stack.
        Box::pin(self.step_body(run, step, async move { work.await.map_err(StepFailure::from) }))
    }

    /// [`Self::step`] for a step whose failure names where it stopped: its Failed receipt carries
    /// the typed stage (`TerminateRuntime`, node-recovery.md step 4's end).
    fn step_with<'s, T, Fut>(&'s self, run: &'s mut Run<'_>, step: &'static str, work: Fut) -> futures_util::future::BoxFuture<'s, Result<T, PipelineError>>
    where
        T: Serialize + DeserializeOwned + Send + 's,
        Fut: std::future::Future<Output = Result<T, StepFailure>> + Send + 's,
    {
        // Boxed where it is made, as `step`.
        Box::pin(self.step_body(run, step, work))
    }

    async fn step_body<T, Fut>(&self, run: &mut Run<'_>, step: &'static str, work: Fut) -> Result<T, PipelineError>
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
            code = tracing::field::Empty,
            detail = tracing::field::Empty,
            reason = tracing::field::Empty,
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
                span.record("code", "ALREADY_EXISTS").record("detail", "already-applied");
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
            io: crate::blocking::IoTime,
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
                    self.span.record("code", "CANCELLED").record("detail", "caller-gone");
                }
            }
        }
        let io = crate::blocking::IoTime::default();
        let mut guard = StepGuard { span: span.clone(), started, io: io.clone(), done: false };
        // RDM's diagnostics (a spawn error, a runtime's last words) are scrubbed of every declared value
        // and RDM's four secrets before they reach the receipt or the span (Q150 §0.1).
        let r = crate::blocking::io_time_with(io.clone(), work.instrument(span.clone())).await.map_err(|mut f| {
            f.reason = self.template.scrub(&f.reason);
            f
        });
        guard.done = true;
        span.record("elapsed_ms", started.elapsed().as_millis() as u64);
        if io.calls() > 0 {
            span.record("io_calls", io.calls()).record("io_queued_ms", io.queued_ms()).record("io_ms", io.ran_ms());
        }
        let (outcome, output) = match &r {
            Ok(v) => {
                span.record("code", "OK").record("detail", "complete");
                (StepOutcome::Complete, serde_json::to_value(v).ok().filter(|v| !v.is_null()))
            },
            Err(f) => {
                span.record("code", f.code.name()).record("detail", "failed").record("reason", f.reason.as_str());
                (StepOutcome::Failed { reason: f.reason.clone(), code: f.code }, None)
            }
        };
        // The receipt is the step's durable commit: a step whose receipt the Build state does not
        // write to the executor's own log (a fact with no wire shape, a targeted role, a failed local
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
            span.record("code", "FAILED_PRECONDITION").record("detail", "receipt-refused");
            return Err(PipelineError { step, reason, code: rdm_core::outcome::Code::FailedPrecondition });
        }
        span.record("outcome", if r.is_ok() { "complete" } else { "failed" });
        r.map_err(|f| PipelineError { step, reason: f.reason, code: f.code })
    }

    /// Create `req.node` through every step.
    pub async fn create(&self, req: &CreateRequest) -> Result<Created, PipelineError> {
        self.create_with(req, None, None).await
    }

    /// `create`, with `pre_node_ready` run as the `ApplyMeshPending` step once the birth has joined
    /// its mesh and before it is asked to be Ready, and `before_launch` awaited right before the new
    /// runtime is launched ([`BeforeLaunch`]).
    pub(crate) async fn create_with(&self, req: &CreateRequest, pre_node_ready: Option<PreNodeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        let claim_for = if req.data_dir.is_some() { crate::node_rows::ClaimFor::Recovery } else { crate::node_rows::ClaimFor::Create };
        self.create_with_claim(req, claim_for, pre_node_ready, before_launch).await
    }

    /// `create_with` for a Build's create of a replace's successor at the name its predecessor
    /// released (`ClaimFor::Successor`, node-replace.md step 3): a fresh data dir, its row inheriting
    /// the predecessor's config and tags.
    pub(crate) async fn create_successor_with(&self, req: &CreateRequest, pre_node_ready: Option<PreNodeReady>) -> Result<Created, PipelineError> {
        self.create_with_claim(req, crate::node_rows::ClaimFor::Successor, pre_node_ready, None).await
    }

    /// `create_with` for a recovery Build (`ClaimFor::Recovery` even when `req.data_dir` is `None`
    /// after `RDM_DATA_DIR_GONE_FLOORS`, R-FM2).
    pub(crate) async fn create_recovery_with(&self, req: &CreateRequest, pre_node_ready: Option<PreNodeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        self.create_with_claim(req, crate::node_rows::ClaimFor::Recovery, pre_node_ready, before_launch).await
    }

    fn create_with_claim<'a>(&'a self, req: &'a CreateRequest, claim_for: crate::node_rows::ClaimFor, pre_node_ready: Option<PreNodeReady>, before_launch: Option<BeforeLaunch<'a>>) -> futures_util::future::BoxFuture<'a, Result<Created, PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("create", &req.build_id, &req.node, req.attempt);
        // Boxed where it is made: the create's steps are one large state machine, and every caller (a
        // step, a recovery, a drive) holds only the box on its worker's stack.
        Box::pin(self.create_steps(req, claim_for, pre_node_ready, before_launch).instrument(span))
    }

    async fn create_steps(&self, req: &CreateRequest, claim_for: crate::node_rows::ClaimFor, pre_node_ready: Option<PreNodeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        let mut run = self.begin(&req.build_id, req.attempt, &req.node, format!("node.create:{}", req.node)).await?;
        let mut created = self.create_to_ready(&mut run, req, claim_for, pre_node_ready, before_launch).await?;
        created.node.status = NodeStatus::Ready;
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
    fn create_to_ready<'a>(&'a self, run: &'a mut Run<'_>, req: &'a CreateRequest, claim_for: crate::node_rows::ClaimFor, pre_node_ready: Option<PreNodeReady>, before_launch: Option<BeforeLaunch<'a>>) -> futures_util::future::BoxFuture<'a, Result<Created, PipelineError>> {
        // Boxed where it is made, as `step`.
        Box::pin(self.create_to_ready_body(run, req, claim_for, pre_node_ready, before_launch))
    }

    async fn create_to_ready_body(&self, run: &mut Run<'_>, req: &CreateRequest, claim_for: crate::node_rows::ClaimFor, pre_node_ready: Option<PreNodeReady>, before_launch: Option<BeforeLaunch<'_>>) -> Result<Created, PipelineError> {
        // The name this create claimed: a step after the claim that fails withdraws it, so the next
        // attempt's fresh birth is not refused by this one's claim (lifecycles/node-create.md step 1).
        let mut claimed: Option<NodeId> = None;
        let operation = run.operation.clone();
        let r = self.create_to_ready_claimed(run, req, claim_for, pre_node_ready, before_launch, &mut claimed).await;
        if let (Err(e), Some(node_id), Some(rows)) = (&r, &claimed, self.sink.node_rows()) {
            // Under a fabric.delete that interrupts this Build, the claim is the launch evidence the
            // delete retains (build.md §1, Q179): the teardown's roster, read from the node rows, holds
            // the birth, and its node.delete ends the runtime this create launched.
            if let Some(delete) = self.interrupting_delete(&req.build_id).await {
                tracing::info_span!("rdm.node_admin.node.update.via-claim-kept", node = %req.node, subject_id = %node_id, operation = %operation, delete_build_id = %delete.log(), failed_step = e.step, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the create failed under a fabric.delete: its claim stays for the teardown, which deletes the birth it launched"));
                return r;
            }
            rows.withdraw(&req.node, node_id, &operation, &format!("the create failed at {}: {}", e.step, e.reason)).await;
        }
        r
    }

    /// The fabric.delete accepted after the Build `build_id` that interrupts it (build.md §1), when one does.
    async fn interrupting_delete(&self, build_id: &crate::build::BuildId) -> Option<crate::build::BuildId> {
        let build = self.builds.read_build(build_id).await.ok()?;
        crate::build_state::interrupted_by(self.builds, &build).await.ok().flatten().map(|d| d.build_id)
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
            .terminate(stranded, TerminationMode::Graceful { grace: self.timeouts.stop_grace }, super::deadline::Deadline::stop(self.timeouts.stop_grace))
            .instrument(span)
            .await
            .map_err(|e| PipelineError { step: CreateStep::DeployRuntime.name(), reason: format!("{}: the earlier attempt's runtime was launched by another executor and never joined; stopping it failed: {e}", req.node) , code: rdm_core::outcome::Code::FailedPrecondition })
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
    async fn claimed_allocation(&self, run: &Run<'_>, req: &CreateRequest, rows: &crate::node_rows::NodeRows) -> Result<Option<Identity>, PipelineError> {
        use tracing::Instrument;
        let step = CreateStep::AllocateIdentity.name();
        let (claimed, read) = rows.allocation_at(&req.node, &req.build_id, &run.operation).await.map_err(|reason| PipelineError { step, reason , code: rdm_core::outcome::Code::FailedPrecondition })?;
        let Some((node_id, a)) = claimed else { return Ok(None) };
        let identity = Identity { node_id, incarnation: a.incarnation, supersedes: None, deployment_id: a.deployment_id };
        let data_dir = req.data_dir.clone().unwrap_or_else(|| self.template.data_root.join(format!("{}-{}", req.node, identity.node_id)));
        // `find` reads the launch's name, data dir and deployment; the birth's address is bound only once
        // its identity is decided.
        let spec = ResolvedNodeLaunch { secrets: Default::default(), withhold: Vec::new(), node: req.node.clone(), deployment_id: identity.deployment_id.clone(), executable: self.template.executable.clone(), args: vec![], env: Default::default(), data_dir, transport: SocketAddr::from(([0, 0, 0, 0], 0)), listeners: vec![] };
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
            read_status = tracing::field::Empty,
            read_revision = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
        );
        // What the claim's read returned at the name: the row's status and revision.
        if let Some(r) = &read {
            span.record("read_status", format!("{:?}", r.status).as_str()).record("read_revision", r.revision);
        }
        match self.provider.find(&spec, super::deadline::Deadline::step()).instrument(span.clone()).await {
            Ok(Found::Exited(_)) => {
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
                Err(PipelineError { step, reason , code: rdm_core::outcome::Code::FailedPrecondition })
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
                Err(PipelineError { step, reason , code: rdm_core::outcome::Code::FailedPrecondition })
            }
        }
    }

    fn create_to_ready_claimed<'a>(&'a self, run: &'a mut Run<'_>, req: &'a CreateRequest, claim_for: crate::node_rows::ClaimFor, pre_node_ready: Option<PreNodeReady>, before_launch: Option<BeforeLaunch<'a>>, claimed: &'a mut Option<NodeId>) -> futures_util::future::BoxFuture<'a, Result<Created, PipelineError>> {
        // Boxed where it is made, as `step`.
        Box::pin(self.create_to_ready_claimed_body(run, req, claim_for, pre_node_ready, before_launch, claimed))
    }

    async fn create_to_ready_claimed_body(&self, run: &mut Run<'_>, req: &CreateRequest, claim_for: crate::node_rows::ClaimFor, pre_node_ready: Option<PreNodeReady>, before_launch: Option<BeforeLaunch<'_>>, claimed: &mut Option<NodeId>) -> Result<Created, PipelineError> {
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
                Some(h) => match self.provider.inspect(h, super::deadline::Deadline::step()).await {
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
        // A run holding no AllocateIdentity receipt of this operation continues the birth an earlier
        // run claimed the name for, when the claim row names one ([`Self::claimed_allocation`]); any
        // other run mints.
        let claimed_before = match &rows {
            Some(rows) if !run.done.contains_key(CreateStep::AllocateIdentity.name()) => self.claimed_allocation(run, req, rows).await?,
            _ => None,
        };
        let id: Identity = self
            .step(run, CreateStep::AllocateIdentity.name(), async {
                Ok(claimed_before.unwrap_or_else(|| Identity { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), supersedes: None, deployment_id: DeploymentId::mint() }))
            })
            .await?;
        // The birth's address, bound for its deployment: the birth a run continues binds the address it
        // holds, and a new birth at a name a predecessor still holds is given its own. The node binds
        // port 0 and the operating system assigns the port; the address it reports at its join is the
        // one this admin publishes for the birth.
        let bind_ip = self
            .provider
            .bind_ip(&req.node, &id.deployment_id, super::deadline::Deadline::step())
            .await
            .map_err(|e| PipelineError { step: CreateStep::DeployRuntime.name(), reason: format!("{}: no address to bind: {e}", req.node) , code: rdm_core::outcome::Code::FailedPrecondition })?;
        let bind_addr = SocketAddr::new(bind_ip, 0);
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
                let k = crate::blocking::blocking_io(move || load_transport_key(&dir)).await?;
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
                // A plain create's row carries the tag labels and the config its create spec names, from the
                // path's entry the fabric-primary put on the create (Q166; proposal
                // executable-bindings-at-create.md rule 1; a Build message carries no entry's tags, config or
                // executable, and this admin reads no Build record, Q227), so its first join hands it that
                // config; a recovery's and a successor's carry their seed's.
                // Whatever the claim, its storage and placement are the create spec's (build-deployment.md §3:
                // a successor's storage follows its create spec's), written onto the row (build.md §1): a
                // Build message carries them, read from the Build this admin holds.
                step_span.record("awaiting", "entry-spec");
                let held = self.builds.read_build(&req.build_id).await.map_err(|e| format!("{}: the accepted Build {} could not be read for the node's create spec; nothing is launched: {e}", req.node, req.build_id))?;
                done_awaits.push("entry-spec");
                let placed = held.topology.mesh(&req.node.mesh).and_then(|m| m.spec(&req.node)).map(|s| (s.storage_persistence, s.placement));
                let (tags, config) = match (of, &req.entry) {
                    (crate::node_rows::ClaimFor::Create, Some(e)) => (e.tags.clone(), e.config.clone()),
                    _ => Default::default(),
                };
                step_span.record("awaiting", "claim");
                rows.claim_spec(&req.node, &id.node_id, &run_operation, Some(&allocated), self.template.binding.as_ref(), &tags, &config, placed, of)
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
                crate::deployment::provider::write_held_runtimes(&data_dir, &req.held_runtimes).await?;
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
            // The provider the launching admin writes (storage.md §2 "every launch"): the one its own
            // template names, as `RDM_MESH_ID` is taken from it.
            topology_storage_provider: self.template.env.get(rdm_core::launch::ENV_TOPOLOGY_STORAGE_PROVIDER).cloned().unwrap_or_else(|| rdm_core::launch::TOPOLOGY_STORAGE_FILE.into()),
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
            issuing: self.template.issuing.clone(),
            roots: self.template.roots.current(),
            connections_delete_deadline: self.template.connections_delete_deadline,
            // A node-admin is written every declared value it may launch with, beside the root (Q150).
            declared: if req.node.kind == rdm_core::NodeKind::NodeAdmin { self.template.declared.custody() } else { Vec::new() },
        };
        // The launch environment; its TRACEPARENT is taken inside the
        // DeployRuntime step, so the runtime's boot span is that step's child.
        // In order (Q150): RDM's own launch variables, then the declared secrets of this node's kind
        // (`secrets`), then RDM's fixed pass-through; a later tier never replaces an earlier one's name
        // (a declared name RDM sets or passes through refused the admin's start). The host environment
        // reaches a process runtime only for other names, and never a declared one (`withhold`).
        let spec_for = |traceparent: Option<String>| {
            let mut env = launch.to_env();
            env.insert("RDM_INHERITED_DATA_DIR".into(), if inherits { "1".into() } else { "0".into() });
            if let Some(tp) = traceparent {
                env.insert("TRACEPARENT".into(), tp);
            }
            for (k, v) in &self.template.env {
                env.entry(k.clone()).or_insert_with(|| v.clone());
            }
            ResolvedNodeLaunch { secrets: self.template.declared.set_on(req.node.kind), withhold: self.template.declared.names(),
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
            .step_with(run, CreateStep::DeployRuntime.name(), async {
                let spec = spec_for(rdm_transport::current_traceparent());
                // One deadline for the step's provider calls (Q304): the find that resolves this exact
                // launch and the spawn spend the same budget, and a call past it fails the step by
                // name, its outcome unknown.
                let deadline = super::deadline::Deadline::step();
                if let Some(h) = self.provider.find(&spec, deadline).await.map_err(StepFailure::from)?.running() {
                    tracing::info!(deployment_id = %spec.deployment_id, "adopting the runtime this deployment already started");
                    return Ok(h);
                }
                if let Some(before) = before_launch {
                    before.await;
                }
                self.provider.spawn(&spec, deadline).await.map_err(StepFailure::from)
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
                listeners: bind_listeners.clone(),
            })),
            None => None,
        };
        // The same report completes WaitForMeshJoin (node-create.md step 5).
        let admitted = reported.clone();
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
            // Written and read back on the blocking pool (`blocking::blocking_io`).
            let back = crate::blocking::blocking_io(move || written.write_record(&dir).map(|()| RuntimeFact::read_record(&dir))).await.map_err(|e| format!("{}: {e}", req.node))?;
            match back {
                Some(Ok(back)) if back == f => Ok(RuntimeEvidence::of(&f)),
                Some(Ok(_)) => Err(format!("{}: {} reads back another runtime", req.node, data_dir.display())),
                Some(Err(e)) => Err(format!("{}: {e}", req.node)),
                None => Err(format!("{}: {} holds no runtime record after it was written", req.node, data_dir.display())),
            }
        })
        .await?;
        let fact = exact().map_err(|reason| PipelineError { step: CreateStep::MakeRuntimeFactAvailableToBirth.name(), reason , code: rdm_core::outcome::Code::FailedPrecondition })?;
        let bound: Bound = self
            .step_with(run, CreateStep::WaitForBind.name(), async {
                let mut reported = reported.ok_or_else(|| format!("{}: the handle names no exact runtime to verify a join against", req.node))?;
                let until = Instant::now() + self.timeouts.bind;
                let wait = super::deadline::Deadline::after(self.timeouts.bind);
                loop {
                    if let Some(d) = reported.borrow().clone() {
                        return Bound::of(&d).map_err(|e| StepFailure::from(format!("{}: {e}", req.node)));
                    }
                    if let DeploymentStatus::Exited { code } = self.provider.inspect(&handle, super::deadline::Deadline::step().within(Some(wait))).await {
                        let detail = self.provider.failure_detail(&handle, &data_dir, super::deadline::Deadline::step()).await;
                        return Err(StepFailure::coded(rdm_core::outcome::Code::Unavailable, format!("runtime exited (code {code:?}) before reporting where it bound: {detail}")));
                    }
                    let left = until.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(StepFailure::coded(rdm_core::outcome::Code::DeadlineExceeded, format!("{} did not report where it bound (JoinNode) within {:?}", req.node, self.timeouts.bind)));
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
        // dir) here; the fact itself the birth reports in its JoinNode digest,
        // which the accepting authority validated against exactly this fact and
        // data dir before it was admitted.
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
                match &pre_node_ready {
                    Some(hand_off) => hand_off(target).await.map(|state| serde_json::json!({"applied": format!("{state:?}")})),
                    None => Ok(serde_json::json!({"applied": null, "reason": "no Pending owed: not a new mesh's first admin"})),
                }
            })
            .await?;
        // The validated report of this exact birth, the one the accepting authority admitted
        // (node-create.md step 5): never a digest gossip carried. On a run that reused WaitForBind's
        // receipt the report is the one this admin kept when the earlier attempt ended (`Joins::end`)
        // or, this process having restarted since, the one kept durable at admission.
        let published = Publication { runtime: Some(fact.clone()), data_dir: node.data_dir.clone() };
        self.step_with(run, CreateStep::WaitForMeshJoin.name(), async {
            let mut admitted = admitted.ok_or_else(|| format!("{}: the handle names no exact runtime to verify a join against", req.node))?;
            let until = Instant::now() + self.timeouts.join;
            loop {
                let held = admitted.borrow().clone().map(|r| r.digest).filter(|d| d.node.node_id == id.node_id && d.node.incarnation == id.incarnation).map(|d| Publication { runtime: d.node.runtime.clone(), data_dir: d.data_dir.clone() });
                // Held by no process any more (this admin restarted after the join was admitted): the
                // admitted birth's runtime row, kept durable at admission (`Joins::keep_admitted`).
                let reported = match held {
                    Some(p) => Some(p),
                    None => self.joins.durable_report(&id.node_id, &id.incarnation).await.map_err(|e| StepFailure::from(format!("{}: {e}", req.node)))?.map(|row| Publication { runtime: Some(row.runtime), data_dir: row.data_dir }),
                };
                if let Some(reported) = reported {
                    if reported == published {
                        return Ok(());
                    }
                    return Err(StepFailure::coded(rdm_core::outcome::Code::FailedPrecondition, format!(
                        "{} incarnation {} reported runtime {} with data dir {:?}, not the runtime {} made available with data dir {:?}",
                        req.node,
                        id.incarnation,
                        reported.runtime.as_ref().map_or_else(|| "none".to_string(), |f| f.locator_fingerprint()),
                        reported.data_dir,
                        fact.locator_fingerprint(),
                        published.data_dir
                    )));
                }
                // A runtime that exited will never report: its own last words, now, not a timeout.
                if let DeploymentStatus::Exited { code } = self.provider.inspect(&handle, super::deadline::Deadline::step().within(Some(super::deadline::Deadline::after(until.saturating_duration_since(Instant::now()))))).await {
                    let detail = self.provider.failure_detail(&handle, &data_dir, super::deadline::Deadline::step()).await;
                    return Err(StepFailure::coded(rdm_core::outcome::Code::Unavailable, format!("runtime exited (code {code:?}) before its join was admitted: {detail}")));
                }
                let left = until.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(StepFailure::coded(rdm_core::outcome::Code::DeadlineExceeded, format!("no validated report of {} incarnation {} is held within {:?}", req.node, id.incarnation, self.timeouts.join)));
                }
                let _ = tokio::time::timeout(left.min(Duration::from_millis(100)), admitted.changed()).await;
            }
        })
        .await?;
        self.step_with(run, CreateStep::WaitForNodeReady.name(), async {
            // Ready only over committed prerequisites: each has a Complete
            // receipt in the Build's state, not merely a step that ran.
            let receipts = self.builds.read_build(run_build).await.map_err(|e| format!("reading {run_build}'s receipts before Ready: {e}"))?.steps;
            let missing = ready_prerequisites_missing(&receipts, &run_operation, run_attempt);
            if !missing.is_empty() {
                return Err(format!("{} cannot be Ready: no Complete receipt for {}", req.node, missing.join(", ")).into());
            }
            let until = Instant::now() + self.timeouts.ready;
            loop {
                match self.observer.ready(&node).await {
                    Ok(()) => return Ok(()),
                    Err(e) if Instant::now() >= until => return Err(StepFailure::coded(rdm_core::outcome::Code::DeadlineExceeded, e)),
                    Err(not_ready) => {
                        // A runtime that exited will never be Ready: its own last words, now, not a timeout.
                        if let DeploymentStatus::Exited { code } = self.provider.inspect(&handle, super::deadline::Deadline::step().within(Some(super::deadline::Deadline::after(until.saturating_duration_since(Instant::now()))))).await {
                            let detail = self.provider.failure_detail(&handle, &data_dir, super::deadline::Deadline::step()).await;
                            return Err(StepFailure::coded(rdm_core::outcome::Code::Unavailable, format!("runtime exited (code {code:?}) before it was Ready ({not_ready}): {detail}")));
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

    /// A standalone `node.connection.delete { All }` (node-connections-delete.md):
    /// `node.connection.delete:<path>`. A node-admin target that held a role had it moved by
    /// node.primary.update before this runs (the fabric-primary's drive, `crate::executor::role_move`);
    /// the exact birth's mesh-primary sets connections-deleting at admission and holds the birth's
    /// `node.connection.deleted` with its receipt. The process keeps running; nothing
    /// stops it and nothing is deleted.
    pub fn delete_connections<'a>(&'a self, req: &'a DeleteRequest) -> futures_util::future::BoxFuture<'a, Result<(), PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("node.connection.delete", &req.build_id, &req.node.name, req.attempt);
        Box::pin(async move {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.connection.delete:{name}")).await?;
            let node = req.node.clone();
            let ctx = CommandContext { build_id: run.build_id.to_string(), attempt: run.attempt, operation: run.operation.clone() };
            let _: CommandAdmission = self
                .step_with(&mut run, DeleteStep::DeleteConnections.name(), async {
                    // Nothing is recorded before the command leaves (i144 R22): the node reports
                    // connections-deleting on its command's stream and its mesh-primary records it then.
                    self.command_op(&ctx, &node)?;
                    match self.observer.send_command(&node, crate::node_commands::NodeCommand::DeleteConnections, &ctx).await {
                        a @ CommandAdmission::ConnectionsDeleted { .. } => Ok(a),
                        other => Err(StepFailure::coded(other.code(), format!("{}: node.connection.delete {{ All }} ended {other:?}; no node.connection.deleted is held", node.name))),
                    }
                })
                .await?;
            self.complete(&mut run).await
        }
        .instrument(span))
    }

    /// A standalone stop (node-stop.md): `node.stop:<path>` is the stop-node command, whose reply is the
    /// exact birth's `stopped`: it deleted its connections, cut its mesh connections and is stopped, its process alive.
    /// The mesh primary publishes `NodeStopped` from that reply, and `Complete` names the birth and the
    /// rafka-time it stopped at. No
    /// runtime exit is asked and no departure is published.
    pub fn stop<'a>(&'a self, req: &'a DeleteRequest) -> futures_util::future::BoxFuture<'a, Result<(), PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("stop", &req.build_id, &req.node.name, req.attempt);
        Box::pin(async move {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.stop:{name}")).await?;
            let mut node = req.node.clone();
            self.stop_steps_with(&mut run, &mut node, &req.handle, StopProof::Stopped, StopEnd::Stop { publish_stopped: true }).await?;
            // The `Complete` receipt opens the birth's stopped period: it commits the stop's
            // lifecycle op, the exact birth and the rafka-time it stopped at, which the stopped-node
            // garbage collector ages it from (node-identity-and-state.md §6.9 Age).
            let ctx = self.command_context(&run, crate::node_commands::NodeCommand::Stop, &node);
            let op = self.command_op(&ctx, &node).map_err(|reason| PipelineError { step: DeleteStep::Complete.name(), reason , code: rdm_core::outcome::Code::FailedPrecondition })?;
            let _: LifecycleOp = self.step(&mut run, DeleteStep::Complete.name(), async { Ok(op) }).await?;
            Ok(())
        }
        .instrument(span))
    }

    /// A standalone start (node-start.md): `node.start:<path>` is the start-node command to the stopped
    /// birth, whose reply is `Started` once it rejoined, then the wait for it to report ready.
    pub fn start<'a>(&'a self, req: &'a StartRequest) -> futures_util::future::BoxFuture<'a, Result<(), PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("start", &req.build_id, &req.node.name, req.attempt);
        Box::pin(async move {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.start:{name}")).await?;
            let mut node = req.node.clone();
            self.start_steps(&mut run, &mut node, &req.handle, &req.deployed).await?;
            self.complete(&mut run).await?;
            Ok(())
        }
        .instrument(span))
    }

    /// The start leg: the admin holds what it deployed for the birth again (its `JoinNode` is
    /// verified against it), `start-node` goes to the stopped birth and its reply is `Started`, then
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
        // Register the exact birth expected at the join door. This writes no lifecycle state.
        let _reported = self.joins.expect(deployed.clone());
        let admission: CommandAdmission = self
            .step_with(run, steps[0], async {
                let admission = self.observer.send_command(node, NodeCommand::Start, &ctx).await;
                match &admission {
                    CommandAdmission::Started {} => Ok(admission),
                    // The reissue of the start that already brought this birth back (a later attempt
                    // of the same Build, after its executor was lost): its node.joined and node.started
                    // were taken when it ran; taking node.joined again would set the birth back to
                    // Pending with no node.started to follow. Readiness is read next, as for any start.
                    CommandAdmission::AlreadyAdmitted {} => Ok(admission),
                    CommandAdmission::StartFailed { step, reason, code } => Err(StepFailure::coded(*code, format!("{}: node.start failed at {step}: {reason}", node.name))),
                    other => Err(StepFailure::coded(other.code(), format!("{}: start-node was not answered Started ({other:?}); the birth is not known to have rejoined", node.name))),
                }
            })
            .await?;
        let _ = admission;
        let until = Instant::now() + self.timeouts.ready;
        let _: () = self
            .step_with(run, steps[1], async {
                loop {
                    match self.observer.ready(node).await {
                        Ok(()) => return Ok(()),
                        Err(e) if Instant::now() >= until => return Err(StepFailure::coded(rdm_core::outcome::Code::DeadlineExceeded, e)),
                        Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                    }
                }
            })
            .await?;
        node.status = NodeStatus::Ready;
        Ok(())
    }

    /// The `Complete` receipt of a standalone connections delete or start (a stop's commits its lifecycle op). It commits no output: the attempt it
    /// belongs to carried the action targeted to the exact birth (its `Opened` fact,
    /// `BuildProjection::attempt_actions`), so a later attempt's plan reads it as that birth's and
    /// plans nothing more for it.
    async fn complete(&self, run: &mut Run<'_>) -> Result<(), PipelineError> {
        self.step(run, DeleteStep::Complete.name(), async { Ok(()) }).await
    }

    /// `node.delete` of the stopped birth `req.node` (lifecycles/node-delete.md), as ONE operation under
    /// node.delete's one key, `node.delete:<path>`:
    ///
    /// ```text
    /// NodeDeleting -> TerminateRuntime (the provider's exact Exited) -> NodeDeleted -> Complete (the birth)
    /// ```
    ///
    /// Its stop steps are done ("For a crashed process those stop steps are already done", and a
    /// stopped one already sent its `stopped`): it is sent no command. Only the provider's matching
    /// Exited is the proof, and the executor publishes NodeDeleted from it. Storage is kept: "It
    /// releases node.id and name, not storage". The lifecycle events name the birth as its own
    /// digest does (`<path>`), by its exact `(node.id, incarnation)`.
    pub fn delete_stopped<'a>(&'a self, req: &'a DeleteRequest) -> futures_util::future::BoxFuture<'a, Result<(), PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("delete", &req.build_id, &req.node.name, req.attempt);
        Box::pin(async move {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.delete:{name}")).await?;
            let birth = req.node.incarnation_id.clone().ok_or_else(|| PipelineError { step: DeleteStep::NodeDeleting.name(), reason: format!("{name} has no known birth") , code: rdm_core::outcome::Code::FailedPrecondition })?;
            let op = LifecycleOp {
                build_id: req.build_id.to_string(),
                attempt: req.attempt,
                operation: run.operation.clone(),
                node_id: req.node.node_id.clone(),
                incarnation: birth,
                name: PathName { old: false, ..name.clone() },
                event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
                traceparent: None,
                tracestate: None,
            };
            let op = self.step(&mut run, DeleteStep::NodeDeleting.name(), Box::pin(self.deleting_row(&req.node, op.clone(), false))).await?;
            self.lifecycle.deleting(&op).await;
            self.terminate_step(&mut run, &req.node, &req.handle).await?;
            let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op };
            let op = self.step(&mut run, DeleteStep::NodeDeleted.name(), self.deleted_row(&req.node, op.clone(), false)).await?;
            self.lifecycle.deleted(&op).await;
            self.complete(&mut run).await
        }
        .instrument(span))
    }

    /// Terminate the runtime of a deleted birth heard again (node-investigation.md: "The node that refused
    /// it reports it to the fabric-primary, which terminates its runtime"; Q110, Q128): the existing
    /// `TerminateRuntime` step on its exact runtime, under `node.terminate:<path>@<incarnation>`. Its
    /// departure and Deleted row stand: nothing else is written.
    pub fn terminate_returned<'a>(&'a self, req: &'a DeleteRequest) -> futures_util::future::BoxFuture<'a, Result<TerminalReceipt, PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("terminate", &req.build_id, &req.node.name, req.attempt);
        Box::pin(async move {
            let name = &req.node.name;
            let birth = req.node.incarnation_id.clone().ok_or_else(|| PipelineError { step: DeleteStep::TerminateRuntime.name(), reason: format!("{name} has no known birth"), code: rdm_core::outcome::Code::FailedPrecondition })?;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.terminate:{name}@{}", birth.as_str())).await?;
            let receipt = self.terminate_step_when(&mut run, &req.node, &req.handle, Terminate::Unreached, TerminationMode::Graceful { grace: self.timeouts.stop_grace }).await?;
            self.complete(&mut run).await?;
            Ok(receipt)
        }
        .instrument(span))
    }

    /// Delete `req.node` through every step (node.delete): the provider's exact terminal receipt of
    /// the birth's runtime, the proof its NodeDeleted was published from.
    pub fn delete_node<'a>(&'a self, req: &'a DeleteRequest) -> futures_util::future::BoxFuture<'a, Result<TerminalReceipt, PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("delete", &req.build_id, &req.node.name, req.attempt);
        Box::pin(self.delete_steps(req, Terminate::Unreached).instrument(span))
    }

    /// An explicit node.delete of an unreached birth (a mesh.delete's lost or silent birth, a member of a
    /// mesh deleted in process under fabric.delete; fabric-delete.md "Receipts"): the unreached delete,
    /// except that an `os-stopped` runtime is terminated `Immediate` (deployment-provider.md §2.5;
    /// node-runtime-inspect.md "What follows"), never left as node recovery leaves it.
    pub fn delete_explicit<'a>(&'a self, req: &'a DeleteRequest) -> futures_util::future::BoxFuture<'a, Result<TerminalReceipt, PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("delete", &req.build_id, &req.node.name, req.attempt);
        Box::pin(self.delete_steps(req, Terminate::Explicit).instrument(span))
    }

    /// Fabric recovery's backbone arm's node.delete of the exact lost birth (fabric-recovery.md step 3,
    /// Q148): the unreached delete, its runtime read and never terminated.
    pub fn delete_from_backbone<'a>(&'a self, req: &'a DeleteRequest) -> futures_util::future::BoxFuture<'a, Result<TerminalReceipt, PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("delete", &req.build_id, &req.node.name, req.attempt);
        Box::pin(self.delete_steps(req, Terminate::Backbone).instrument(span))
    }

    async fn delete_steps(&self, req: &DeleteRequest, unreached: Terminate) -> Result<TerminalReceipt, PipelineError> {
        let name = &req.node.name;
        let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.delete:{name}")).await?;
        let mut node = req.node.clone();
        let handle = &req.handle;
        // The pre-notice: this executor's journal holds its Claim for the attempt (it runs nothing
        // before that) and now records this operation; every node hears the node is being removed
        // and stops routing to it, while it stays found.
        let op = LifecycleOp {
            build_id: req.build_id.to_string(),
            attempt: req.attempt,
            operation: run.operation.clone(),
            node_id: node.node_id.clone(),
            incarnation: node.incarnation_id.clone().ok_or_else(|| PipelineError { step: DeleteStep::NodeDeleting.name(), reason: format!("{name} has no known birth") , code: rdm_core::outcome::Code::FailedPrecondition })?,
            name: name.clone(),
            event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
            traceparent: None,
            tracestate: None,
        };
        // The receipt first, then the publish: the Build facts carry the overlay from the
        // moment it is announced, so a successor derives it from the same facts.
        // An explicit delete of an unreached birth ends at its exact Exited when this executor does not write
        // its mesh's rows (below): its NodeDeleting writes no row it does not write either.
        let for_writer = matches!(unreached, Terminate::Explicit) && req.unreached;
        let op = self.step(&mut run, DeleteStep::NodeDeleting.name(), Box::pin(self.deleting_row(&node, op.clone(), for_writer))).await?;
        self.lifecycle.deleting(&op).await;
        let terminal = if req.unreached || run.done.contains_key(DeleteStep::TerminateRuntime.name()) {
            // An unreached birth (node-recovery.md sequence step 3): nothing is sent to it and no node
            // reply or receipt is fabricated; `TerminateRuntime` first when its runtime runs at the
            // step's instant, and nothing waits for a proof of termination. A delete handed its exact
            // Exited runs on from it: the exited verdict is a step already taken, and nothing is stopped.
            self.terminate_step_when(&mut run, &node, handle, unreached, TerminationMode::Graceful { grace: self.timeouts.stop_grace }).await?
        } else if node.stopped {
            // A delete of a stopped birth: its whole stop already ran (its roles moved, its connections delete
            // receipt, its connections cut, `stopped`, NodeStopped), and nothing is composed or
            // fabricated again. The delete proceeds to the exact runtime's exited verdict
            // (lifecycles/node-delete.md 1-2, node-stop.md 6).
            self.terminate_step(&mut run, &node, handle).await?
        } else if self.stop_skipped_os_stopped(&run, &node, handle).await {
            // An OS-suspended target (node-delete.md): it answers no call, so no stop is sent; the
            // delete begins at TerminateRuntime on the exact handle, whose Exited is the proof. A
            // suspended runtime runs no handler, so the terminate is Immediate: no graceful wait, and
            // nothing of it is resumed (i144 R23).
            self.terminate_step_when(&mut run, &node, handle, Terminate::Always, TerminationMode::Immediate).await?
        } else {
            // The two halves of the shutdown leg are separate operations (lifecycles/node-connections-delete.md, node-stop.md):
            // node.connection.delete and its node.connections.deleted, then stop-node, its node-left reply and the provider's
            // terminal proof.
            self.stop_steps_with(&mut run, &mut node, handle, StopProof::Lenient, StopEnd::Terminate { publish_stopped: true }).await?.expect("a stop that terminates yields the provider's receipt")
        };
        // An explicit delete of an unreached birth whose mesh's rows this executor does not write, read at the
        // write's instant: the lost final primary of a leaving mesh whose election named a surviving admin
        // (mesh-delete.md "Resume after the primary's loss" steps 1 and 3; node-identity-and-state.md §4.3,
        // R-OW1). Its steps end at the provider's exact Exited, the roster receipt the amendment rests on;
        // NodeDeleted and the steps after it are the new mesh-primary's, which runs the delete on from them.
        let writes = match (unreached, self.sink.node_rows()) {
            (Terminate::Explicit, Some(rows)) if req.unreached => rows.writes_unreached(&node).await,
            _ => true,
        };
        if !writes {
            tracing::info_span!("rdm.node_admin.node.delete.via-exit-for-writer", node = %name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %req.build_id.log(), attempt = req.attempt, operation = %run.operation)
                .in_scope(|| tracing::info!("the birth's exact Exited is recorded; its Deleted row and the steps after it are its mesh's writer's"));
            return Ok(terminal);
        }
        // The departure: the step above. Nothing earlier (the node's Leaving, its connections-deleted reply, the
        // claim) is.
        let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op };
        let op = self
            .step(&mut run, DeleteStep::NodeDeleted.name(), async {
                // The deleted birth's row goes Deleted at its path.name, its config kept, inside the
                // step and before node.deleted (lifecycles/node-delete.md step 3): a node created at that
                // path.name, a recovery's create included, claims the name over it and seeds its
                // config from it (node-recovery.md "Retained").
                self.deleted_row(&node, op.clone(), req.unreached).await
            })
            .await?;
        self.lifecycle.deleted(&op).await;
        // node-delete.md step 3: the data dir, node-identity.json and endpoint key are retained; no
        // step of the delete touches the birth's storage. Exactly the deleted birth leaves the
        // executor's records, and stays out of its view even when the records never held it.
        self.step(&mut run, DeleteStep::RemoveTopologyMembership.name(), async {
            self.sink.remove_birth(name, &op.node_id, &op.incarnation);
            Ok(())
        })
        .await?;
        self.step(&mut run, DeleteStep::Complete.name(), async { Ok(()) }).await?;
        Ok(terminal)
    }

    /// node.delete of the executor's own live birth `req.node` (fabric-delete.md step 4 and "After
    /// completion": the fabric-primary's own node.delete, called from outside the fabric), under
    /// node.delete's one key:
    ///
    /// ```text
    /// NodeDeleting -> StopNode -> AwaitNodeLeft -> NodeDeleted -> RemoveTopologyMembership -> Complete
    /// ```
    ///
    /// The whole node.stop, the node stopped with its process alive; then its row Deleted and node.id and
    /// name released, and node.deleted on the caller's stream as its last reply. No `TerminateRuntime`
    /// runs here and no `Exited` of the runtime is receipted: the runtime is the executor's own, which cannot read its own `Exited`, and
    /// nobody is left in the fabric to record it ("Receipts": the one exception). The executor ends its
    /// runtime through the provider after the reply (`AdminRunner::delete_own`). Every other node.delete
    /// is [`Self::delete_node`]: the provider's `Exited` before node.deleted.
    pub fn delete_own<'a>(&'a self, req: &'a DeleteRequest) -> futures_util::future::BoxFuture<'a, Result<(), PipelineError>> {
        use tracing::Instrument;
        let span = self.pipeline_span("delete", &req.build_id, &req.node.name, req.attempt);
        Box::pin(async move {
            let name = &req.node.name;
            let mut run = self.begin(&req.build_id, req.attempt, name, format!("node.delete:{name}")).await?;
            let mut node = req.node.clone();
            let op = LifecycleOp {
                build_id: req.build_id.to_string(),
                attempt: req.attempt,
                operation: run.operation.clone(),
                node_id: node.node_id.clone(),
                incarnation: node.incarnation_id.clone().ok_or_else(|| PipelineError { step: DeleteStep::NodeDeleting.name(), reason: format!("{name} has no known birth"), code: rdm_core::outcome::Code::FailedPrecondition })?,
                name: name.clone(),
                event_at_rafka_ms: self.lifecycle.now_rafka_ms(),
                traceparent: None,
                tracestate: None,
            };
            let op = self.step(&mut run, DeleteStep::NodeDeleting.name(), Box::pin(self.deleting_row(&node, op.clone(), false))).await?;
            self.lifecycle.deleting(&op).await;
            self.stop_steps_with(&mut run, &mut node, &req.handle, StopProof::Lenient, StopEnd::Stop { publish_stopped: true }).await?;
            let op = LifecycleOp { event_at_rafka_ms: self.lifecycle.now_rafka_ms(), ..op };
            let op = self.step(&mut run, DeleteStep::NodeDeleted.name(), self.deleted_row(&node, op.clone(), false)).await?;
            self.lifecycle.deleted(&op).await;
            self.step(&mut run, DeleteStep::RemoveTopologyMembership.name(), async {
                self.sink.remove_birth(name, &op.node_id, &op.incarnation);
                Ok(())
            })
            .await?;
            self.step(&mut run, DeleteStep::Complete.name(), async { Ok(()) }).await
        }
        .instrument(span))
    }

    /// The command identity for `cmd` on `node` in this run: the Build, the attempt and the
    /// operation (`node.connection.delete:<path>` / `node.stop:<path>`).
    fn command_context(&self, run: &Run<'_>, cmd: crate::node_commands::NodeCommand, node: &Node) -> CommandContext {
        let operation = match cmd {
            crate::node_commands::NodeCommand::DeleteConnections => format!("node.connection.delete:{}", node.name),
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
            traceparent: None,
            tracestate: None,
        })
    }

    /// The `NodeDeleting` step's row (states.md "Who sets each state": deleting from node.delete's adopted
    /// claim, by "the node's mesh-primary (the fabric-primary for a mesh with no primary)"): `Deleting`
    /// written on the node's record before the step's receipt, by the writer of its mesh's rows alone
    /// (node-identity-and-state.md §4.3). `for_writer`: an explicit delete of an unreached birth, which an
    /// executor that does not write its mesh's rows runs to the exact Exited for its mesh's writer
    /// ([`Self::delete_steps`]): there the step's receipt stands without the row, whose `Deleting` is that
    /// writer's (mesh-delete.md "Resume after the primary's loss" step 3: completed receipts stand). Any
    /// other refusal fails the step by name.
    async fn deleting_row(&self, record: &Node, op: LifecycleOp, for_writer: bool) -> Result<LifecycleOp, String> {
        if let Some(rows) = self.sink.node_rows() {
            match rows.status(record, &op.incarnation, crate::model::NodeStatus::Deleting).await {
                Ok(()) => {}
                Err(crate::node_rows::RowRefused::NotMeshPrimary(_)) if for_writer => {}
                Err(e) => return Err(format!("{}: the Deleting row of {} is not written: {e}", record.name, record.node_id)),
            }
        }
        Ok(op)
    }

    /// `NodeDeleted`'s work (lifecycles/node-delete.md step 3, before node.deleted): the writer of the
    /// mesh's rows writes `record`'s row `Deleted` at its `path.name`, the record and its config
    /// kept; a refusal fails the step by name. `unreached`: the delete of an unreached birth
    /// ([`crate::node_rows::NodeRows::deleted_unreached`]).
    async fn deleted_row(&self, record: &Node, op: LifecycleOp, unreached: bool) -> Result<LifecycleOp, String> {
        if let Some(rows) = self.sink.node_rows() {
            let written = if unreached { rows.deleted_unreached(record, &op.operation).await } else { rows.deleted(record, &op.operation).await };
            written.map_err(|e| format!("{}: the Deleted row of {} is not written: {e}", record.name, record.node_id))?;
        }
        Ok(op)
    }

    /// Resolves when this deployment's own runtime has exited.
    async fn until_exited(&self, handle: &DeploymentHandle) {
        while !matches!(self.provider.inspect(handle, super::deadline::Deadline::step()).await, DeploymentStatus::Exited { .. }) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The stop half of a shutdown (node-stop.md): `stop-node` to the exact birth (no implicit
    /// connections delete), the wait for node-left (the `Stopped` reply of that call), the mesh primary's
    /// `NodeStopped` from that matching reply, then, by `end`, the stopped process or the provider's
    /// stop and inspection of the exact runtime, whose `Exited` alone is terminal proof. `proof` says
    /// what the wait step must have seen to complete (a standalone
    /// stop fails it by name unless stop-node was admitted and the
    /// birth's `stopped` reply received).
    /// `end` says what follows: the process stops, or the provider ends it.
    async fn stop_steps_with(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle, proof: StopProof, end: StopEnd) -> Result<Option<TerminalReceipt>, PipelineError> {
        self.stop_steps_as(run, node, handle, proof, end, [DeleteStep::StopNode.name(), DeleteStep::AwaitNodeLeft.name()]).await
    }

    /// [`Self::stop_steps_with`] whose two steps are receipted as `steps` (the send, the left reply):
    /// a replace stops two nodes in one operation, and each stop's receipts are its own.
    async fn stop_steps_as(&self, run: &mut Run<'_>, node: &mut Node, handle: &DeploymentHandle, proof: StopProof, end: StopEnd, steps: [&'static str; 2]) -> Result<Option<TerminalReceipt>, PipelineError> {
        use crate::node_commands::NodeCommand;
        let ctx = self.command_context(run, NodeCommand::Stop, node);
        let publish_stopped = matches!(end, StopEnd::Stop { publish_stopped: true } | StopEnd::Terminate { publish_stopped: true });
        // `stop-node` is held until the birth is stopped, and its reply is `stopped`: the birth deleted its connections,
        // cut its mesh connections and entered Leaving. The command is `Stopped` or it is not complete.
        // The mesh-primary sets the birth stopping on the stop call's admission and never before it
        // (states.md "Who sets each state"): a command the birth never held sets nothing.
        let admission: CommandAdmission = self
            .step(run, steps[0], async {
                Ok(self.observer.send_command(node, NodeCommand::Stop, &ctx).await)
            })
            .await?;
        if admission.admitted() {
            node.status = NodeStatus::Leaving;
        }
        let _: Completion = self
            .step_with(run, steps[1], async {
                if !admission.admitted() {
                    // A command the birth never held has no left reply to follow (node-stop.md): a
                    // standalone stop is not complete.
                    if proof == StopProof::Stopped {
                        return Err(StepFailure::coded(admission.code(), format!("{}: stop-node was not admitted ({admission:?}); no left reply can follow, the stop is not complete", node.name)));
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
                // The stop's completion: the mesh primary publishes the departure from the matching
                // `stopped`; the node gossips nothing.
                if matches!(c, Completion::Received {}) {
                    let op = self.command_op(&ctx, node)?;
                    if publish_stopped {
                        self.lifecycle.stopped(&op).await;
                    }
                }
                match (proof != StopProof::Lenient, &c) {
                    (true, c) if *c != Completion::Received {} => Err(StepFailure::coded(if matches!(c, Completion::Deadline {}) { rdm_core::outcome::Code::DeadlineExceeded } else { rdm_core::outcome::Code::Unavailable }, format!("{}: stop-node was admitted but its left reply was not received ({c:?}); the stop is not complete", node.name))),
                    _ => Ok(c),
                }
            })
            .await?;
        match end {
            StopEnd::Stop { .. } => Ok(None),
            StopEnd::Terminate { .. } => Ok(Some(self.terminate_step(run, node, handle).await?)),
        }
    }

    /// `TerminateRuntime`: the provider's stop and inspection of the exact runtime; only its `Exited`
    /// is terminal proof, and the receipt binds the exact birth and runtime. A delete of a birth the
    /// provider already proves terminal asks only this.
    pub(crate) async fn terminate_step(&self, run: &mut Run<'_>, node: &Node, handle: &DeploymentHandle) -> Result<TerminalReceipt, PipelineError> {
        self.terminate_step_when(run, node, handle, Terminate::Always, TerminationMode::Graceful { grace: self.timeouts.stop_grace }).await
    }

    /// [`Self::terminate_step`], firing as `when` says: an unreached birth's delete terminates only a
    /// runtime the provider reads `Running` at the step's instant, and on any other read terminates
    /// nothing and waits for nothing (node-recovery.md step 3); the receipt names what it read.
    async fn terminate_step_when(&self, run: &mut Run<'_>, node: &Node, handle: &DeploymentHandle, when: Terminate, mut mode: TerminationMode) -> Result<TerminalReceipt, PipelineError> {
        let name = node.name.clone();
        self.step_with(run, DeleteStep::TerminateRuntime.name(), async {
            // One deadline for the step's inspection, terminate and re-inspection (Q304): a graceful
            // stop's grace is inside it, and it never resets within the step.
            let deadline = match mode {
                TerminationMode::Graceful { grace } => super::deadline::Deadline::stop(grace),
                TerminationMode::Immediate => super::deadline::Deadline::step(),
            };
            let receipt = |exit_code: Option<i32>, verdict: Option<&str>| -> Result<TerminalReceipt, StepFailure> {
                Ok(TerminalReceipt {
                    node: name.to_string(),
                    node_id: node.node_id.to_string(),
                    incarnation_id: node.incarnation_id.as_ref().map(|i| i.to_string()).ok_or_else(|| format!("{name} has no known birth: the receipt names no birth"))?,
                    runtime: handle.fact().as_ref().map(RuntimeProof::of),
                    exit_code,
                    verdict: verdict.map(str::to_string),
                    data_dir: node.data_dir.clone(),
                })
            };
            let verdict = if when != Terminate::Always {
                // node.runtime.inspect at the terminate's instant: its span is the evidence (node-runtime-inspect.md).
                let why = match when {
                    Terminate::Backbone => "fabric recovery's backbone arm reads the exact runtime at its instant and terminates nothing",
                    Terminate::Explicit => "an explicit delete's TerminateRuntime inspects the exact runtime at its instant and terminates it, an os-stopped one Immediate",
                    _ => "node recovery's TerminateRuntime inspects the exact runtime at its instant and terminates it on every read but os-stopped",
                };
                match self.inspected_at(node, handle, "terminate", why, deadline).await {
                    DeploymentStatus::Running if when == Terminate::Backbone => return receipt(None, Some("running")),
                    DeploymentStatus::Exited { code } if when == Terminate::Backbone => return receipt(code, Some("exited")),
                    DeploymentStatus::Running => Some("running"),
                    // Never skipped for an exited runtime: the provider's terminate removes what it
                    // left (node-recovery.md step 3, Q145); the receipt still names the read.
                    DeploymentStatus::Exited { .. } => Some("exited"),
                    // An explicit delete of an OS-suspended runtime terminates it at once: a suspended
                    // runtime runs no handler, so nothing of it is resumed and nothing waits for grace.
                    DeploymentStatus::OsStopped if when == Terminate::Explicit => {
                        mode = TerminationMode::Immediate;
                        Some("os-stopped")
                    }
                    DeploymentStatus::OsStopped => return receipt(None, Some("os-stopped")),
                    DeploymentStatus::Unknown => return receipt(None, Some("unavailable")),
                }
            } else {
                None
            };
            // A terminate past its deadline is never done: its outcome is unknown, by name.
            if let Err(e) = self.provider.terminate(handle, mode, deadline).await {
                return Err(StepFailure::from(e));
            }
            // Only an exact inspection that says the runtime exited is terminal proof.
            let exit_code = match self.provider.inspect(handle, deadline).await {
                DeploymentStatus::Running => return Err(StepFailure::from(format!("{name} still runs after the stop ladder"))),
                DeploymentStatus::OsStopped => return Err(StepFailure::from(format!("{name}: the operating system holds its runtime suspended after the stop ladder (os-stopped)"))),
                DeploymentStatus::Unknown => return Err(StepFailure::from(format!("{name}: the provider cannot inspect its runtime after the stop ladder"))),
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
                held = crate::blocking::blocking_io(move || {
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
                    let names = crate::blocking::blocking_io(move || {
                        held.iter()
                            .map(|h| {
                                let who = super::endpoint::port_holder(h.1, h.2).unwrap_or_else(|e| format!("holder unknown: {e}"));
                                format!("{} {} ({who})", h.0, h.1)
                            })
                            .collect::<Vec<String>>()
                    })
                    .await;
                    return Err(StepFailure::coded(rdm_core::outcome::Code::DeadlineExceeded, format!("{name} exited, but the operating system still holds {} after {elapsed:?}", names.join(", "))));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            tracing::info!(node = %name, released_after_ms = started.elapsed().as_millis() as u64, "the runtime exited and the operating system released its addresses");
            receipt(exit_code, verdict)
        })
        .await
    }

    /// node.runtime.inspect of `node`'s exact runtime by this pipeline's executor for `caller`, at the
    /// caller's instant (node-runtime-inspect.md): the provider's answer, spanned with its verdict.
    async fn inspected_at(&self, node: &Node, handle: &DeploymentHandle, caller: &'static str, why: &'static str, deadline: super::deadline::Deadline) -> DeploymentStatus {
        let status = self.provider.inspect(handle, deadline).await;
        let verdict = match &status {
            DeploymentStatus::Running => "running",
            DeploymentStatus::OsStopped => "os-stopped",
            DeploymentStatus::Exited { .. } => "exited",
            DeploymentStatus::Unknown => "unavailable",
        };
        // The fields node-runtime-inspect.md's span names: the exit code of an exited runtime, and why the
        // provider could not answer.
        let exit_code = match &status {
            DeploymentStatus::Exited { code } => code.map(|c| c.to_string()).unwrap_or_default(),
            _ => String::new(),
        };
        let reason = if status == DeploymentStatus::Unknown { "the provider cannot read the exact runtime's state" } else { "" };
        let incarnation = node.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
        tracing::info_span!("rdm.node_admin.node.resolve.via-provider-inspection", node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation, inspector = %self.template.executor(), handle = ?handle, caller, verdict, exit_code, reason).in_scope(|| tracing::info!("{why}"));
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
        let status = self.inspected_at(node, handle, "delete", "the delete inspects the exact runtime at its instant: an os-stopped one answers no stop", super::deadline::Deadline::step()).await;
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

    /// A Build's node.mesh.leave of `node` (`node.mesh.leave:<path>`, node-mesh-leave.md): `leave`
    /// is the mesh-primary's carry of `LeaveMesh` to the exact birth, then `Complete`.
    pub fn leave_mesh<'a, F>(&'a self, build_id: &'a BuildId, attempt: u32, node: &'a PathName, leave: F) -> futures_util::future::BoxFuture<'a, Result<(), PipelineError>>
    where
        F: std::future::Future<Output = Result<(), String>> + Send + 'a,
    {
        use tracing::Instrument;
        let span = self.pipeline_span("leave", build_id, node, attempt);
        Box::pin(async move {
            let mut run = self.begin(build_id, attempt, node, format!("node.mesh.leave:{node}")).await?;
            self.step(&mut run, LeaveStep::LeaveMesh.name(), leave).await?;
            self.step(&mut run, LeaveStep::Complete.name(), async { Ok(()) }).await
        }
        .instrument(span))
    }

    /// node.name.update of the name of `node` as a Build's step (node-name-update.md;
    /// lifecycles/node-replace.md step 2), as ONE operation `node.update:<path>`: `UpdateNode`, the
    /// update `put` (node.name.update's own core at the node's mesh-primary, answered `NameUpdated`
    /// or refused by name), then `Complete`.
    pub fn update_name<'a, F>(&'a self, build_id: &'a BuildId, attempt: u32, node: &'a PathName, put: F) -> futures_util::future::BoxFuture<'a, Result<(), PipelineError>>
    where
        F: std::future::Future<Output = Result<(), String>> + Send + 'a,
    {
        use tracing::Instrument;
        let span = self.pipeline_span("update", build_id, node, attempt);
        Box::pin(async move {
            let mut run = self.begin(build_id, attempt, node, format!("node.update:{node}")).await?;
            self.step(&mut run, UpdateStep::UpdateNode.name(), put).await?;
            self.step(&mut run, UpdateStep::Complete.name(), async { Ok(()) }).await
        }
        .instrument(span))
    }

    /// A property of `node` a Build applies in place (build.md §1, "The Build applies the diff") as the
    /// Build operation `operation` (`node.config.update:<path>`, `node.tag.create:<path>`,
    /// `node.tag.delete:<path>`): its one step `step`, the put at the node's mesh-primary, then
    /// `Complete`. No runtime is touched.
    pub fn apply_in_place<'a, F>(&'a self, build_id: &'a BuildId, attempt: u32, node: &'a PathName, operation: String, step: &'static str, put: F) -> futures_util::future::BoxFuture<'a, Result<(), PipelineError>>
    where
        F: std::future::Future<Output = Result<(), String>> + Send + 'a,
    {
        use tracing::Instrument;
        let span = self.pipeline_span("update", build_id, node, attempt);
        Box::pin(async move {
            let mut run = self.begin(build_id, attempt, node, operation).await?;
            self.step(&mut run, step, put).await?;
            self.step(&mut run, UpdateStep::Complete.name(), async { Ok(()) }).await
        }
        .instrument(span))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{op_waits_on_the_blocking_pool, runtime_runs_while_a_fifo_is_read, tempdir};

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
        // A finished run hands nothing on: attempt 5 of the same operation (a second stop
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
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<DeploymentHandle, super::super::provider::DeployError> {
            Err(super::super::provider::DeployError::Spawn { node: spec.node.to_string(), reason: "this provider only answers inspections".into() })
        }
        async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode, _deadline: crate::deployment::deadline::Deadline) -> Result<(), super::super::provider::DeployError> {
            self.terminated.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match &self.refuse {
                Some(reason) => Err(super::super::provider::DeployError::Terminate { deployment: h.deployment_id.to_string(), reason: reason.clone() }),
                None => Ok(()),
            }
        }
        async fn inspect(&self, _: &DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> DeploymentStatus {
            self.status.clone()
        }
        async fn signal_stop(&self, _: &DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> Result<(), super::super::provider::DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<Found, super::super::provider::DeployError> {
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

    /// The launch template of a cell whose provider never spawns: mesh1.admin.1's, its data root a
    /// fresh directory named `cell`.
    fn cell_template(cell: &str) -> LaunchTemplate {
        let data_root = tempdir(cell);
        LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: rdm_core::FabricId::mint(),
            executable: "/nonexistent/rdm-rpc-node".into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: data_root.clone(),
            store_dir: data_root,
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
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
        modes: std::sync::Mutex<Vec<TerminationMode>>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for Suspended {
        fn kind(&self) -> crate::model::ProviderKind {
            crate::model::ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            "process:cell".into()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<DeploymentHandle, super::super::provider::DeployError> {
            Err(super::super::provider::DeployError::Spawn { node: spec.node.to_string(), reason: "this provider only answers inspections".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, mode: TerminationMode, _deadline: crate::deployment::deadline::Deadline) -> Result<(), super::super::provider::DeployError> {
            self.terminated.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.modes.lock().unwrap().push(mode);
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> DeploymentStatus {
            if self.terminated.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                DeploymentStatus::OsStopped
            } else {
                DeploymentStatus::Exited { code: Some(137) }
            }
        }
        async fn signal_stop(&self, _: &DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> Result<(), super::super::provider::DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<Found, super::super::provider::DeployError> {
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
                crate::node_commands::NodeCommand::DeleteConnections => CommandAdmission::NotSent { reason: "this cell sends no node.connection.delete".into() , code: rdm_core::outcome::Code::FailedPrecondition },
            }
        }
    }

    fn process_handle() -> DeploymentHandle {
        DeploymentHandle { deployment_id: DeploymentId::mint(), provider: crate::model::ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("process:cell".into()) }
    }

    /// CONTRACT (deployment-provider.md §2.5: "An explicit `node.delete` of an OS-stopped birth stays
    /// available: it skips the Node RPC stop by name (a suspended process answers no call), runs
    /// `TerminateRuntime` on the exact handle, takes the exited verdict and continues"; node-delete.md
    /// "OS-suspended target"): the delete of a live (not stopped) birth whose provider reads its exact
    /// runtime `OsStopped` at the delete's instant sends it no command; the inspection is spanned
    /// (`caller = delete`, `os-stopped`), the skip is named (`rdm.node_admin.node.delete.via-os-stopped`),
    /// and the run's receipts are NodeDeleting, TerminateRuntime, NodeDeleted,
    /// RemoveTopologyMembership, Complete: one terminate of the exact runtime, whose `Exited` is the
    /// proof. Must NOT happen: a `stop-node` or `node.connection.delete` sent to the suspended birth, a
    /// StopNode or AwaitNodeLeft receipt.
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:14
    /// "`node.delete`, step 2: before sending `node.stop`, the executor inspects the birth's runtime.
    ///  On `Exited { exit_code }` or `OsStopped`, it skips `node.stop` (a dead or frozen process
    ///  cannot run a graceful stop) and proceeds to `TerminateRuntime`, which still runs so the
    ///  provider cleans up any container, child entry, or handle."
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:17
    /// "Explicit delete (`nodes.delete`, `meshes.delete`, `fabric.delete`): if the target is
    ///  reachable, `node.delete` skips inspection and runs `node.stop` then `TerminateRuntime`. If
    ///  the target is unreachable, `node.delete` runs `node.investigation`: `node.runtime.inspect`
    ///  first (2 s bound), then the tickles if not `Exited`, then `TerminateRuntime`."
    // @feature: node-lifecycle
    #[tokio::test]
    async fn an_explicit_delete_of_an_os_stopped_birth_sends_no_stop_and_terminates_its_exact_runtime() {
        let provider = Suspended { terminated: Default::default(), modes: Default::default() };
        let observer = Birth { stop: CommandAdmission::NotSent { reason: "a suspended birth answers no call".into() , code: rdm_core::outcome::Code::FailedPrecondition }, sent: Default::default() };
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let build_id = accepted_build(&builds).await;
        let template = cell_template("os-stopped-delete");
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &observer, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
        node.status = NodeStatus::PendingReconnect;
        node.incarnation_id = Some(IncarnationId::mint());
        let req = DeleteRequest { build_id: build_id.clone(), attempt: 1, node, handle: process_handle(), unreached: false };
        let capture = crate::span_capture::Capture::new("i144-e3-os-stopped-delete");
        let terminal = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            pipeline.delete_node(&req).await.expect("the delete of an os-stopped birth completes")
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

    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:14
    /// "On `Exited { exit_code }` or `OsStopped`, it skips `node.stop` ... and proceeds to
    ///  `TerminateRuntime`"
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:17
    /// "Explicit delete (`nodes.delete`, `meshes.delete`, `fabric.delete`) ... proceeds to
    ///  `TerminateRuntime`"
    /// CONTRACT (deployment-provider.md §2.5: "An explicit `node.delete` of an OS-stopped birth stays
    /// available: it skips the Node RPC stop by name ..., runs `TerminateRuntime` on the exact handle as
    /// `terminate(Immediate)` (no graceful wait; nothing of the runtime is resumed) and continues";
    /// node-runtime-inspect.md "What follows": "On an explicit delete, an `os-stopped` result skips the
    /// unexecutable Node RPC stop by name and proceeds to `TerminateRuntime` on the exact handle";
    /// fabric-delete.md "Receipts": a fabric.delete's member deletes are node.deletes): the explicit delete of
    /// a birth no call reaches (a member of a mesh deleted in process under fabric.delete, a mesh.delete's
    /// lost or silent birth) whose provider reads its exact runtime `OsStopped` sends it no command and
    /// terminates that runtime once, `Immediate`; the receipt names the `os-stopped` read and carries the
    /// terminate's `Exited` code, and NodeDeleted follows that proof. Must NOT happen: the os-stopped birth
    /// receipted and written deleted with nothing terminated (node recovery's rule, which leaves an
    /// os-stopped runtime as it is), a graceful terminate of a suspended runtime, or a command sent to it.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn an_explicit_delete_of_an_unreached_os_stopped_birth_terminates_its_exact_runtime_immediately() {
        let provider = Suspended { terminated: Default::default(), modes: Default::default() };
        let observer = Birth { stop: CommandAdmission::NotSent { reason: "a suspended birth answers no call".into(), code: rdm_core::outcome::Code::FailedPrecondition }, sent: Default::default() };
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let build_id = accepted_build(&builds).await;
        let template = cell_template("explicit-os-stopped-delete");
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &observer, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let mut node = Node::allocated("mesh1.admin.1".parse().unwrap());
        node.status = NodeStatus::PendingReconnect;
        node.incarnation_id = Some(IncarnationId::mint());
        let req = DeleteRequest { build_id: build_id.clone(), attempt: 1, node, handle: process_handle(), unreached: true };
        let terminal = pipeline.delete_explicit(&req).await.expect("the explicit delete of an os-stopped birth completes");
        assert_eq!(*provider.modes.lock().unwrap(), vec![TerminationMode::Immediate], "the suspended runtime is terminated once, Immediate");
        assert_eq!((terminal.exit_code, terminal.verdict.as_deref()), (Some(137), Some("os-stopped")), "the receipt names the os-stopped read and the terminate's Exited: {terminal:?}");
        assert!(observer.sent.lock().unwrap().is_empty(), "no command is sent to the suspended birth: {:?}", observer.sent.lock().unwrap());
        let steps: Vec<String> = builds.read_build(&build_id).await.unwrap().steps.into_iter().map(|r| r.step).collect();
        assert_eq!(steps, ["NodeDeleting", "TerminateRuntime", "NodeDeleted", "RemoveTopologyMembership", "Complete"].map(String::from).to_vec());
    }

    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:14
    /// "`node.delete`, step 2: before sending `node.stop`, the executor inspects the birth's runtime.
    ///  On `Exited { exit_code }` or `OsStopped`, it skips `node.stop` (a dead or frozen process
    ///  cannot run a graceful stop) and proceeds to `TerminateRuntime`, which still runs so the
    ///  provider cleans up any container, child entry, or handle."
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:18
    /// "Recovery delete (`node.recovery` step 4): `node.investigation` has already run in step 1
    ///  (`node.runtime.inspect` first, then the tickles when not `Exited`). If `node.runtime.inspect`
    ///  proved `Exited { exit_code }` or `OsStopped`, `node.delete` skips `node.stop` and proceeds
    ///  directly to `TerminateRuntime`."
    /// CONTRACT (node-recovery.md step 3; deployment-provider.md §2.7 "Signal stop and terminate"): a
    /// recovery's delete of a birth whose runtime already exited sends it nothing and still asks its
    /// provider to `terminate` it once, which removes what the runtime left (a container); the receipt
    /// is the inspection's `exited` with its code. Must NOT happen: the step ending on the inspection's
    /// `exited` with no `terminate`, leaving the exited container on the host.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_recovery_of_a_birth_whose_runtime_exited_asks_its_provider_to_terminate_it_once() {
        let provider = Answers { status: DeploymentStatus::Exited { code: Some(137) }, terminated: Default::default(), refuse: None };
        let observer = Birth { stop: CommandAdmission::NotSent { reason: "an unheard birth is sent no stop".into(), code: rdm_core::outcome::Code::FailedPrecondition }, sent: Default::default() };
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let build_id = accepted_build(&builds).await;
        let template = cell_template("exited-recovery-delete");
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &observer, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
        node.status = NodeStatus::PendingReconnect;
        node.incarnation_id = Some(IncarnationId::mint());
        let req = DeleteRequest { build_id, attempt: 1, node, handle: process_handle(), unreached: true };
        let terminal = pipeline.delete_node(&req).await.expect("the recovery's delete of an exited birth completes");
        assert_eq!((terminal.exit_code, terminal.verdict.as_deref()), (Some(137), Some("exited")), "the receipt is the inspection's exited: {terminal:?}");
        assert!(observer.sent.lock().unwrap().is_empty(), "no command is sent to the unheard birth: {:?}", observer.sent.lock().unwrap());
        assert_eq!(provider.terminated.load(std::sync::atomic::Ordering::SeqCst), 1, "the provider is asked once to terminate the exited runtime, which removes what it left");
    }

    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:26
    /// "Target | The exact birth's runtime handle (`RuntimeFact`: `deployment_id`, `provider`, `locator`)"
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:32
    /// "Spans | `rdm.node_admin.node.resolve.via-provider-inspection` (`node`, `node_id`,
    ///  `incarnation_id`, `inspector`, `handle`, `verdict`, `exit_code`, `reason`)"
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:104
    /// "`rdm.node_admin.node.resolve.via-provider-inspection` (`node`, `node_id`, `incarnation_id`,
    ///  `inspector`, `handle`, `verdict`, `exit_code`, `reason`)"
    /// CONTRACT (node-runtime-inspect.md Observability contract: the inspect span carries "node,
    /// node_id, incarnation_id, inspector, handle, verdict, exit_code, exited_verdict, reason"; Target:
    /// "The exact runtime handle recorded at launch"): the inspection's span names the exact handle
    /// the provider was asked about, the one the caller's launch record holds. Must NOT happen: an
    /// inspection span with no `handle`, so the evidence cannot say which runtime was inspected.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_inspection_span_names_the_exact_handle_the_provider_was_asked_about() {
        let provider = Suspended { terminated: Default::default(), modes: Default::default() };
        let observer = Birth { stop: CommandAdmission::NotSent { reason: "a suspended birth answers no call".into(), code: rdm_core::outcome::Code::FailedPrecondition }, sent: Default::default() };
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let build_id = accepted_build(&builds).await;
        let template = cell_template("inspect-handle");
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &observer, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
        node.status = NodeStatus::PendingReconnect;
        node.incarnation_id = Some(IncarnationId::mint());
        let handle = process_handle();
        let req = DeleteRequest { build_id, attempt: 1, node, handle: handle.clone(), unreached: false };
        let capture = crate::span_capture::Capture::new("i144-e3-inspect-handle");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            pipeline.delete_node(&req).await.expect("the delete of an os-stopped birth completes");
        }
        let inspected = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
        assert!(!inspected.is_empty(), "the delete's inspection is spanned");
        for c in &inspected {
            assert_eq!(c.attributes.get("handle").map(String::as_str), Some(format!("{handle:?}").as_str()), "the inspection names the exact handle it asked the provider about: {c:?}");
        }
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

    /// A provider that records the environment of each launch it is asked for and launches nothing.
    #[derive(Default)]
    struct RecordsLaunches {
        envs: std::sync::Mutex<Vec<BTreeMap<String, String>>>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for RecordsLaunches {
        fn kind(&self) -> crate::model::ProviderKind {
            crate::model::ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            "process:cell".into()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<DeploymentHandle, super::super::provider::DeployError> {
            self.envs.lock().unwrap().push(spec.env.clone());
            Err(super::super::provider::DeployError::Spawn { node: spec.node.to_string(), reason: "this cell launches nothing".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode, _deadline: crate::deployment::deadline::Deadline) -> Result<(), super::super::provider::DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> DeploymentStatus {
            DeploymentStatus::Unknown
        }
        async fn signal_stop(&self, _: &DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> Result<(), super::super::provider::DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<Found, super::super::provider::DeployError> {
            Ok(Found::NeverLaunched)
        }
    }

    /// CONTRACT (fabric-ca-renew.md "Launch": "A launched node is written the current set at launch
    /// (`RDM_ROOTS`), the way `RDM_MESH_ID` is, so a node born mid-overlap trusts both"): every launch
    /// carries the launching admin's root set as the holder stands at that launch: one root, then,
    /// once the holder is replaced by two, both. A launcher whose set is empty (configured with no
    /// certs) writes none.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_launch_carries_the_launching_admins_root_set_as_it_stands_at_the_launch() {
        let (a, b) = (rdm_core::certs::mint_key().verifying_key(), rdm_core::certs::mint_key().verifying_key());
        let provider = RecordsLaunches::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let build_id = accepted_build(&builds).await;
        builds.claim_attempt(&crate::build_state::BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
        let template = LaunchTemplate { roots: rdm_transport::connections::RootSet::new(vec![a]), ..cell_template("launch-roots") };
        let joins = Joins::default();
        let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &NoObserver, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        let create = |node: &str| CreateRequest { build_id: build_id.clone(), attempt: 1, node: node.parse().unwrap(), spec: &crate::deployment::endpoint::RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None };
        let _ = pipeline.create(&create("mesh1.rpc.1")).await.map(|_| ()).expect_err("the provider launches nothing");
        template.roots.replace(vec![a, b]);
        let _ = pipeline.create(&create("mesh1.rpc.2")).await.map(|_| ()).expect_err("the provider launches nothing");
        template.roots.replace(vec![]);
        let _ = pipeline.create(&create("mesh1.rpc.3")).await.map(|_| ()).expect_err("the provider launches nothing");
        let written: Vec<Option<String>> = provider.envs.lock().unwrap().iter().map(|e| e.get(rdm_core::launch::ENV_ROOTS).cloned()).collect();
        let hex = |k: &rdm_core::certs::VerifyingKey| rdm_core::launch::encode_hex(k.as_bytes());
        assert_eq!(written, vec![Some(hex(&a)), Some(format!("{},{}", hex(&a), hex(&b))), None], "each launch carries the set as it stood at that launch");
        let _ = std::fs::remove_dir_all(&template.data_root);
    }

    /// A provider whose exact runtime is this test process and whose birth reports, as the node's own
    /// `JoinNode` does, to the accepting primary that holds its deployment: the digest the primary
    /// validates against the deployment and admits. No digest of the birth reaches any view: gossip is
    /// silent.
    struct ReportsToThePrimary {
        joins: std::sync::Arc<Joins>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for ReportsToThePrimary {
        fn kind(&self) -> crate::model::ProviderKind {
            crate::model::ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            rdm_core::runtime::process_control_domain().expect("this process has a control domain")
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<DeploymentHandle, super::super::provider::DeployError> {
            let refused = |reason: String| super::super::provider::DeployError::Spawn { node: spec.node.to_string(), reason };
            let fact = RuntimeFact::of_this_process(&spec.deployment_id.to_string()).map_err(refused)?;
            let (pid, start) = match fact.locator {
                rdm_core::RuntimeLocator::Process { pid, start } => (pid, start),
                other => return Err(refused(format!("this process is not a container: {other:?}"))),
            };
            let handle = DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: crate::model::ProviderKind::Process, pid: Some(pid), start: Some(start), container: None, domain: Some(fact.control_domain.clone()) };
            let key = crate::admin::load_or_mint_key(&spec.data_dir).await.map_err(refused)?;
            let env = |k: &str| spec.env.get(k).cloned().ok_or_else(|| refused(format!("the launch carries no {k}")));
            let digest = rdm_core::MeshDigest {
                held_hash: Default::default(),
                fabric_id: rdm_core::FabricId::mint(),
                node: rdm_core::MeshNode {
                    node_id: NodeId::parse(&env(rdm_core::launch::ENV_NODE_ID)?).map_err(|e| refused(e.to_string()))?,
                    name: spec.node.clone(),
                    endpoint_id: EndpointId(key.public().to_string()),
                    transport_addr: "127.0.0.1:34567".parse().unwrap(),
                    incarnation: IncarnationId::parse(&env(rdm_core::launch::ENV_INCARNATION)?).map_err(|e| refused(e.to_string()))?,
                    supersedes: None,
                    runtime: handle.fact(),
                },
                status: rdm_core::MemberStatus::Pending,
                node_state: Default::default(),
                admin_api_base: None,
                digest_seq: 0,
                emitted_at_rafka_ms: 0,
                data_dir: Some(spec.data_dir.display().to_string()),
                mesh_id: None,
                in_flight: None,
                load: None,
                gossip: None,
                is_primary: None,
                is_fabric_primary: None,
            };
            let joins = self.joins.clone();
            tokio::spawn(async move {
                loop {
                    if matches!(joins.standing(&digest, &[]), crate::join::Standing::Deployed { .. }) {
                        joins.report(&crate::join::JoinReport { digest: digest.clone(), listeners: Vec::new() });
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            Ok(handle)
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode, _deadline: crate::deployment::deadline::Deadline) -> Result<(), super::super::provider::DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> DeploymentStatus {
            DeploymentStatus::Running
        }
        async fn signal_stop(&self, _: &DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> Result<(), super::super::provider::DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<Found, super::super::provider::DeployError> {
            Ok(Found::NeverLaunched)
        }
    }

    /// CONTRACT (node-create.md step 5: on acceptance the authority "completes WaitForBind /
    /// WaitForMeshJoin from the validated report, never from gossip"; "Join completion and failure
    /// evidence": "Only an accepted full report completes the deployment's bind/join waits ... a
    /// digest ... is insufficient"): a birth whose JoinNode the accepting authority admitted
    /// completes WaitForMeshJoin while no digest of it reached any view, and the create goes on to
    /// WaitForNodeReady, where nothing answers Ready. Must NOT happen: WaitForMeshJoin waiting past
    /// its deadline for a topology digest of a birth whose report was accepted.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn wait_for_mesh_join_completes_from_the_accepted_report_while_gossip_is_silent() {
        let joins = std::sync::Arc::new(Joins::default());
        let provider = ReportsToThePrimary { joins: joins.clone() };
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let build_id = accepted_build(&builds).await;
        builds.claim_attempt(&crate::build_state::BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
        let template = cell_template("join-from-report");
        let timeouts = Timeouts { join: Duration::from_millis(1500), ready: Duration::from_millis(300), ..Timeouts::default() };
        let pipeline = DeploymentPipeline { provider: &provider, joins: &joins, observer: &NoObserver, sink: &NoSink, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts };
        let req = CreateRequest { build_id: build_id.clone(), attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &crate::deployment::endpoint::RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None };
        let err = pipeline.create(&req).await.map(|_| ()).expect_err("nothing answers Ready: the create ends at WaitForNodeReady");
        let steps: Vec<(String, StepOutcome)> = builds.read_build(&build_id).await.unwrap().steps.into_iter().map(|r| (r.step, r.outcome)).collect();
        assert_eq!(err.step, CreateStep::WaitForNodeReady.name(), "WaitForMeshJoin completes from the accepted report with no digest in any view: {err}; receipts {steps:?}");
        assert!(steps.iter().any(|(s, o)| s == CreateStep::WaitForMeshJoin.name() && *o == StepOutcome::Complete), "WaitForMeshJoin's receipt is Complete: {steps:?}");
        let _ = std::fs::remove_dir_all(&template.data_root);
    }

    // @feature: data-dir-lost-recovery
    /// CONTRACT: docs/architecture/lifecycles/node-recovery.md:147
    /// "Gone versus broken is decided from the filesystem error on the inherited `data_dir`, never
    ///  guessed. An absent directory path (`NotFound` on the directory itself) is gone; an existing
    ///  directory on disk whose `node-key` is missing, unreadable, or corrupt (`NotFound` on
    ///  `node-key` inside an existing directory, `EIO`, `EACCES`, or decode failure) is broken"
    #[test]
    fn inspect_transport_key_distinguishes_absent_data_dir_gone_from_existing_dir_broken_and_loads_valid_key(
    ) {
        assert_eq!(crate::admin::DEFAULT_DATA_DIR_GONE_FLOORS, 2);
        assert!(crate::admin::data_dir_gone_floors() >= 1);
        assert!(crate::admin::data_dir_gone_hold_ms() >= 1);

        let dir = std::env::temp_dir().join(format!(
            "rdm-data-dir-lost-unit-{}",
            rdm_core::IncarnationId::mint()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        // 1. Absent directory -> InheritedDirError::Gone
        match inspect_transport_key(&dir) {
            Err(InheritedDirError::Gone(msg)) => {
                assert!(msg.contains("does not exist"), "unexpected message: {msg}");
            }
            other => panic!("expected Gone for absent directory, got {other:?}"),
        }

        // 2. Existing directory with missing `node-key` -> InheritedDirError::Broken
        std::fs::create_dir_all(&dir).unwrap();
        match inspect_transport_key(&dir) {
            Err(InheritedDirError::Broken(msg)) => {
                assert!(
                    msg.contains("node-key is missing"),
                    "unexpected message: {msg}"
                );
            }
            other => panic!("expected Broken for missing node-key, got {other:?}"),
        }

        // 3. Existing directory with corrupt `node-key` -> InheritedDirError::Broken
        let key_path = dir.join("node-key");
        std::fs::write(&key_path, "not-valid-hex").unwrap();
        assert!(matches!(
            inspect_transport_key(&dir),
            Err(InheritedDirError::Broken(_))
        ));

        std::fs::write(&key_path, "deadbeef").unwrap();
        assert!(matches!(
            inspect_transport_key(&dir),
            Err(InheritedDirError::Broken(_))
        ));

        // 4. Repaired 32-byte hex `node-key` -> Ok(EndpointId)
        let secret = iroh::SecretKey::from_bytes(&[0x42u8; 32]);
        let expected_ep = EndpointId(secret.public().to_string());
        std::fs::write(&key_path, hex::encode(secret.to_bytes())).unwrap();
        assert_eq!(inspect_transport_key(&dir), Ok(expected_ep));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
