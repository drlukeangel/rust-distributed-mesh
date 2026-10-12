//! The wire shape of the Build topic (`fabric_builds`): every type here is positional postcard
//! (`rafka_mesh_transport::wire`), carrying every field and no internally tagged enum.
//!
//! The domain types (`BuildFact`, `FabricIdentity`, the accepted topology) are JSON elsewhere: the
//! REST views, the local journal and the tests read `"fact"`, `"kind"`, `"action"`,
//! `"persistence"` keys and absent optional fields. They keep that shape; these mirrors are what
//! travels. A conversion is one `From` per direction, so a field added to a domain type without
//! its mirror does not compile.
//!
//! A step receipt's `output` is the JSON value the step committed (`serde_json::Value`, the
//! REST and journal shape). On the wire it is the typed result of the step that produced it
//! ([`WireOutput`]), chosen by the receipt's own step name. A step name this build does not
//! know to produce an output, or an output that is not the type its step produces, is refused by
//! name ([`WireRefusal`]) — never sent as something else, never dropped.

use crate::accepted::{AttemptAction, FabricTopology, MeshTopology, TopologyChange};
use crate::build::{BuildId, FabricDesired, MeshDesired};
use crate::build_state::{AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStepReceipt, AttemptOpened, StepOutcome};
use crate::deployment::pipeline::Bound;
use crate::deployment::pipeline::{CommandAdmission, Completion, CreateStep, Identity, RetireStep, StorageDisposition, TerminalReceipt};
use crate::mesh_leave::{ExitManifest, RosterAmended};
use crate::deployment::provider::DeploymentHandle;
use crate::fabric_builds::BuildMessage;
use crate::fabric_storage::{FabricIdentity, FabricShutdown};
use crate::model::{EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName};
use rafka_mesh_entity::meta::{NodeMeta, PersistentRetireDisposition, PlacementMeta, StorageMeta};
use rafka_mesh_entity::LifecycleOp;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Why a Build fact cannot be put on the wire. Names the Build, attempt and step it concerns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WireRefusal {
    pub build_id: BuildId,
    pub attempt: u32,
    pub step: String,
    pub reason: String,
}

impl std::fmt::Display for WireRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the output of step {} (Build {}, attempt {}) has no wire shape: {}", self.step, self.build_id, self.attempt, self.reason)
    }
}

// ---- the Build fact ------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
pub(crate) enum WireFact {
    Accepted(WireAccepted),
    Opened(WireOpened),
    Claim(BuildAttemptClaim),
    Step(WireStep),
    Attempt(BuildAttemptReceipt),
    Forget { build_id: BuildId },
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WireAccepted {
    build_id: BuildId,
    topology: WireTopology,
    submitted_change: Option<WireChange>,
    submitted_at_ms: u64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WireOpened {
    build_id: BuildId,
    attempt: u32,
    reason: AttemptReason,
    action: Option<WireAction>,
    opened_by: String,
    opened_at_ms: u64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WireStep {
    build_id: BuildId,
    attempt: u32,
    operation: String,
    step: String,
    outcome: StepOutcome,
    output: Option<WireOutput>,
    executor: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct WireTopology {
    fabric: String,
    meshes: BTreeMap<String, WireMesh>,
}

#[derive(Serialize, Deserialize)]
struct WireMesh {
    name: String,
    mesh_id: MeshId,
    nodes: BTreeSet<PathName>,
    node_meta_by_path: BTreeMap<PathName, WireNodeMeta>,
}

#[derive(Serialize, Deserialize)]
struct WireNodeMeta {
    storage: WireStorage,
    placement: PlacementMeta,
}

#[derive(Serialize, Deserialize)]
enum WireStorage {
    Ephemeral,
    Persistent { on_retire: PersistentRetireDisposition },
}

#[derive(Serialize, Deserialize)]
struct WireMeshDesired {
    name: String,
    node_admin: u32,
    rpc_node: u32,
    broker: u32,
    gateway: u32,
    compute: u32,
}

#[derive(Serialize, Deserialize)]
struct WireFabricDesired {
    fabric: String,
    meshes: Vec<WireMeshDesired>,
}

#[derive(Serialize, Deserialize)]
enum WireChange {
    ReconcileFabric { desired: WireFabricDesired },
    ReconcileMesh { desired: WireMeshDesired },
    AddNode { mesh: String, node_kind: NodeKind },
    RemoveNode { node: PathName },
    CreateMesh { desired: WireMeshDesired },
    RemoveMesh { mesh: String },
}

#[derive(Serialize, Deserialize)]
enum WireAction {
    Restart { path: PathName, from_incarnation: IncarnationId },
    Replace { path: PathName, from_incarnation: IncarnationId },
    Drain { path: PathName, from_incarnation: IncarnationId },
    Stop { path: PathName, from_incarnation: IncarnationId },
    Start { path: PathName, from_incarnation: IncarnationId },
    Recover { path: PathName, node_id: NodeId, from_incarnation: IncarnationId },
    Delete { path: PathName, from_incarnation: IncarnationId },
}

impl From<&MeshDesired> for WireMeshDesired {
    fn from(m: &MeshDesired) -> Self {
        Self { name: m.name.clone(), node_admin: m.node_admin, rpc_node: m.rpc_node, broker: m.broker, gateway: m.gateway, compute: m.compute }
    }
}
impl From<WireMeshDesired> for MeshDesired {
    fn from(m: WireMeshDesired) -> Self {
        Self { name: m.name, node_admin: m.node_admin, rpc_node: m.rpc_node, broker: m.broker, gateway: m.gateway, compute: m.compute }
    }
}
impl From<&FabricDesired> for WireFabricDesired {
    fn from(f: &FabricDesired) -> Self {
        Self { fabric: f.fabric.clone(), meshes: f.meshes.iter().map(Into::into).collect() }
    }
}
impl From<WireFabricDesired> for FabricDesired {
    fn from(f: WireFabricDesired) -> Self {
        Self { fabric: f.fabric, meshes: f.meshes.into_iter().map(Into::into).collect() }
    }
}
impl From<&TopologyChange> for WireChange {
    fn from(c: &TopologyChange) -> Self {
        match c {
            TopologyChange::ReconcileFabric { desired } => Self::ReconcileFabric { desired: desired.into() },
            TopologyChange::ReconcileMesh { desired } => Self::ReconcileMesh { desired: desired.into() },
            TopologyChange::AddNode { mesh, node_kind } => Self::AddNode { mesh: mesh.clone(), node_kind: *node_kind },
            TopologyChange::RemoveNode { node } => Self::RemoveNode { node: node.clone() },
            TopologyChange::CreateMesh { desired } => Self::CreateMesh { desired: desired.into() },
            TopologyChange::RemoveMesh { mesh } => Self::RemoveMesh { mesh: mesh.clone() },
        }
    }
}
impl From<WireChange> for TopologyChange {
    fn from(c: WireChange) -> Self {
        match c {
            WireChange::ReconcileFabric { desired } => Self::ReconcileFabric { desired: desired.into() },
            WireChange::ReconcileMesh { desired } => Self::ReconcileMesh { desired: desired.into() },
            WireChange::AddNode { mesh, node_kind } => Self::AddNode { mesh, node_kind },
            WireChange::RemoveNode { node } => Self::RemoveNode { node },
            WireChange::CreateMesh { desired } => Self::CreateMesh { desired: desired.into() },
            WireChange::RemoveMesh { mesh } => Self::RemoveMesh { mesh },
        }
    }
}
impl From<&AttemptAction> for WireAction {
    fn from(a: &AttemptAction) -> Self {
        match a {
            AttemptAction::Restart { path, from_incarnation } => Self::Restart { path: path.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Replace { path, from_incarnation } => Self::Replace { path: path.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Drain { path, from_incarnation } => Self::Drain { path: path.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Stop { path, from_incarnation } => Self::Stop { path: path.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Start { path, from_incarnation } => Self::Start { path: path.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Recover { path, node_id, from_incarnation } => Self::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Delete { path, from_incarnation } => Self::Delete { path: path.clone(), from_incarnation: from_incarnation.clone() },
        }
    }
}
impl From<WireAction> for AttemptAction {
    fn from(a: WireAction) -> Self {
        match a {
            WireAction::Restart { path, from_incarnation } => Self::Restart { path, from_incarnation },
            WireAction::Replace { path, from_incarnation } => Self::Replace { path, from_incarnation },
            WireAction::Drain { path, from_incarnation } => Self::Drain { path, from_incarnation },
            WireAction::Stop { path, from_incarnation } => Self::Stop { path, from_incarnation },
            WireAction::Start { path, from_incarnation } => Self::Start { path, from_incarnation },
            WireAction::Recover { path, node_id, from_incarnation } => Self::Recover { path, node_id, from_incarnation },
            WireAction::Delete { path, from_incarnation } => Self::Delete { path, from_incarnation },
        }
    }
}
impl From<&NodeMeta> for WireNodeMeta {
    fn from(m: &NodeMeta) -> Self {
        let storage = match m.storage {
            StorageMeta::Ephemeral => WireStorage::Ephemeral,
            StorageMeta::Persistent { on_retire } => WireStorage::Persistent { on_retire },
        };
        Self { storage, placement: m.placement }
    }
}
impl From<WireNodeMeta> for NodeMeta {
    fn from(m: WireNodeMeta) -> Self {
        let storage = match m.storage {
            WireStorage::Ephemeral => StorageMeta::Ephemeral,
            WireStorage::Persistent { on_retire } => StorageMeta::Persistent { on_retire },
        };
        Self { storage, placement: m.placement }
    }
}
impl From<&FabricTopology> for WireTopology {
    fn from(t: &FabricTopology) -> Self {
        let meshes = t
            .meshes
            .iter()
            .map(|(k, m)| {
                let mesh = WireMesh {
                    name: m.name.clone(),
                    mesh_id: m.mesh_id.clone(),
                    nodes: m.nodes.clone(),
                    node_meta_by_path: m.node_meta_by_path.iter().map(|(p, meta)| (p.clone(), meta.into())).collect(),
                };
                (k.clone(), mesh)
            })
            .collect();
        Self { fabric: t.fabric.clone(), meshes }
    }
}
impl From<WireTopology> for FabricTopology {
    fn from(t: WireTopology) -> Self {
        let meshes = t
            .meshes
            .into_iter()
            .map(|(k, m)| {
                let mesh = MeshTopology { name: m.name, mesh_id: m.mesh_id, nodes: m.nodes, node_meta_by_path: m.node_meta_by_path.into_iter().map(|(p, meta)| (p, meta.into())).collect() };
                (k, mesh)
            })
            .collect();
        Self { fabric: t.fabric, meshes }
    }
}

// ---- a step's output -----------------------------------------------------------------------

/// What a step commits to its receipt, typed by the step that produced it.
#[derive(Serialize, Deserialize)]
pub(crate) enum WireOutput {
    Identity(Identity),
    Bound(Bound),
    EndpointId(EndpointId),
    DeploymentHandle(DeploymentHandle),
    RuntimeEvidence(WireRuntimeEvidence),
    MeshPending(WireMeshPending),
    LifecycleOp(LifecycleOp),
    CommandAdmission(WireCommandAdmission),
    Completion(WireCompletion),
    Storage(WireStorageDisposition),
    Terminal(TerminalReceipt),
    ExitManifest(ExitManifest),
    SeatTransfer(WireSeatTransfer),
    RosterAmended(RosterAmended),
}

/// The SeatTransfer step's receipt (`fabric_handover::SeatTransferOutcome`), positional.
#[derive(Serialize, Deserialize)]
pub(crate) enum WireSeatTransfer {
    NotAHolder,
    Committed { seats: Vec<rafka_mesh_entity::seat::transfer::CommittedSeat> },
    CommittedRow { seats: Vec<rafka_mesh_entity::seat::transfer::CommittedSeat> },
    NoTransfer,
    OtherMesh { holder_mesh: String },
}

impl From<&crate::fabric_handover::SeatTransferOutcome> for WireSeatTransfer {
    fn from(o: &crate::fabric_handover::SeatTransferOutcome) -> Self {
        use crate::fabric_handover::SeatTransferOutcome as O;
        match o {
            O::NotAHolder => Self::NotAHolder,
            O::Committed { seats } => Self::Committed { seats: seats.clone() },
            O::CommittedRow { seats } => Self::CommittedRow { seats: seats.clone() },
            O::NoTransfer => Self::NoTransfer,
            O::OtherMesh { holder_mesh } => Self::OtherMesh { holder_mesh: holder_mesh.clone() },
        }
    }
}

impl From<WireSeatTransfer> for crate::fabric_handover::SeatTransferOutcome {
    fn from(o: WireSeatTransfer) -> Self {
        match o {
            WireSeatTransfer::NotAHolder => Self::NotAHolder,
            WireSeatTransfer::Committed { seats } => Self::Committed { seats },
            WireSeatTransfer::CommittedRow { seats } => Self::CommittedRow { seats },
            WireSeatTransfer::NoTransfer => Self::NoTransfer,
            WireSeatTransfer::OtherMesh { holder_mesh } => Self::OtherMesh { holder_mesh },
        }
    }
}

/// A runtime step's receipt: the runtime by fingerprint.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireRuntimeEvidence {
    deployment_id: String,
    provider: String,
    provider_control_domain_fingerprint: String,
    runtime_locator_kind: String,
    runtime_locator_fingerprint: String,
    data_dir: Option<String>,
}

/// `ApplyMeshPending`'s receipt: `{"applied": "Pending"}`, or `{"applied": null, "reason": …}` for a
/// birth that is not its mesh's first admin.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireMeshPending {
    applied: Option<String>,
    reason: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum WireCommandAdmission {
    Admitted,
    AlreadyAdmitted,
    NotSent { reason: String },
    Indeterminate { reason: String },
    Refused { reply: String },
    Stopped { receipt: rafka_node_rpc_contract::status::DrainReceipt },
    Started,
    StartFailed { step: String, reason: String },
}

#[derive(Serialize, Deserialize)]
pub(crate) enum WireCompletion {
    Received,
    Deadline,
    RuntimeExited,
    NotAwaited { admission: String },
}

#[derive(Serialize, Deserialize)]
pub(crate) enum WireStorageDisposition {
    Released { locator: String },
    Preserved,
    PreservedNoMeta,
}

impl From<&CommandAdmission> for WireCommandAdmission {
    fn from(d: &CommandAdmission) -> Self {
        match d {
            CommandAdmission::Admitted => Self::Admitted,
            CommandAdmission::AlreadyAdmitted => Self::AlreadyAdmitted,
            CommandAdmission::NotSent { reason } => Self::NotSent { reason: reason.clone() },
            CommandAdmission::Indeterminate { reason } => Self::Indeterminate { reason: reason.clone() },
            CommandAdmission::Refused { reply } => Self::Refused { reply: reply.clone() },
            CommandAdmission::Stopped { receipt } => Self::Stopped { receipt: *receipt },
            CommandAdmission::Started => Self::Started,
            CommandAdmission::StartFailed { step, reason } => Self::StartFailed { step: step.clone(), reason: reason.clone() },
        }
    }
}
impl From<WireCommandAdmission> for CommandAdmission {
    fn from(d: WireCommandAdmission) -> Self {
        match d {
            WireCommandAdmission::Admitted => Self::Admitted,
            WireCommandAdmission::AlreadyAdmitted => Self::AlreadyAdmitted,
            WireCommandAdmission::NotSent { reason } => Self::NotSent { reason },
            WireCommandAdmission::Indeterminate { reason } => Self::Indeterminate { reason },
            WireCommandAdmission::Refused { reply } => Self::Refused { reply },
            WireCommandAdmission::Stopped { receipt } => Self::Stopped { receipt },
            WireCommandAdmission::Started => Self::Started,
            WireCommandAdmission::StartFailed { step, reason } => Self::StartFailed { step, reason },
        }
    }
}
impl From<&Completion> for WireCompletion {
    fn from(a: &Completion) -> Self {
        match a {
            Completion::Received => Self::Received,
            Completion::Deadline => Self::Deadline,
            Completion::RuntimeExited => Self::RuntimeExited,
            Completion::NotAwaited { admission } => Self::NotAwaited { admission: admission.clone() },
        }
    }
}
impl From<WireCompletion> for Completion {
    fn from(a: WireCompletion) -> Self {
        match a {
            WireCompletion::Received => Self::Received,
            WireCompletion::Deadline => Self::Deadline,
            WireCompletion::RuntimeExited => Self::RuntimeExited,
            WireCompletion::NotAwaited { admission } => Self::NotAwaited { admission },
        }
    }
}
impl From<&StorageDisposition> for WireStorageDisposition {
    fn from(s: &StorageDisposition) -> Self {
        match s {
            StorageDisposition::Released { locator } => Self::Released { locator: locator.clone() },
            StorageDisposition::Preserved => Self::Preserved,
            StorageDisposition::PreservedNoMeta => Self::PreservedNoMeta,
        }
    }
}
impl From<WireStorageDisposition> for StorageDisposition {
    fn from(s: WireStorageDisposition) -> Self {
        match s {
            WireStorageDisposition::Released { locator } => Self::Released { locator },
            WireStorageDisposition::Preserved => Self::Preserved,
            WireStorageDisposition::PreservedNoMeta => Self::PreservedNoMeta,
        }
    }
}

/// The step an earlier build ran on a removal to release storage. No step of this build runs it (a
/// delete retains storage, node-delete.md "Storage"); its receipt keeps its typed output
/// (`WireOutput::Storage`), so the positional layout of `WireOutput` and a Build fact an earlier
/// build wrote still decode.
pub const RELEASE_STORAGE_STEP: &str = "ReleaseStorage";

/// The type of output each step produces; a step not listed commits none.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Produces {
    Identity,
    Bound,
    EndpointId,
    DeploymentHandle,
    RuntimeEvidence,
    MeshPending,
    LifecycleOp,
    CommandAdmission,
    Completion,
    Storage,
    Terminal,
    ExitManifest,
    SeatTransfer,
    RosterAmended,
}

fn produces(operation: &str, step: &str) -> Option<Produces> {
    let is = |c: CreateStep| c.name() == step;
    // A standalone stop's `Complete` commits the stop's lifecycle op: the exact birth and its
    // rafka-time, the age of its stopped period (fabric-node-lifecycle.md §6.9 Age). Every other
    // operation's `Complete` commits none.
    if ret_complete(step) && operation.starts_with("stop-node:") {
        return Some(Produces::LifecycleOp);
    }
    let ret = |r: RetireStep| r.name() == step;
    Some(if is(CreateStep::AllocateIdentity) {
        Produces::Identity
    } else if is(CreateStep::WaitForBind) {
        Produces::Bound
    } else if is(CreateStep::PrepareStorage) {
        Produces::EndpointId
    } else if is(CreateStep::DeployRuntime) {
        Produces::DeploymentHandle
    } else if is(CreateStep::RegisterExactRuntimeHandle)
        || is(CreateStep::ResolveProviderControlDomain)
        || is(CreateStep::MakeRuntimeFactAvailableToBirth)
        || is(CreateStep::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata)
    {
        Produces::RuntimeEvidence
    } else if is(CreateStep::ApplyMeshPending) {
        Produces::MeshPending
    } else if ret(RetireStep::NodeDeleting) || ret(RetireStep::NodeRestarting) || ret(RetireStep::NodeDeleted) || step == crate::deployment::pipeline::ReplaceStep::RenamePredecessor.name() {
        Produces::LifecycleOp
    } else if ret(RetireStep::DrainNode) || ret(RetireStep::StopNode) || step == crate::deployment::pipeline::StartStep::StartNode.name() {
        Produces::CommandAdmission
    } else if ret(RetireStep::AwaitNodeDrained) || ret(RetireStep::AwaitNodeLeft) {
        Produces::Completion
    } else if step == RELEASE_STORAGE_STEP {
        Produces::Storage
    } else if ret(RetireStep::TerminateRuntime) {
        Produces::Terminal
    } else if ret(RetireStep::SeatTransfer) {
        Produces::SeatTransfer
    } else if [crate::mesh_leave::STEP_OTHER_MEMBERS_EXITED, crate::mesh_leave::STEP_MESH_DELETED, crate::mesh_leave::STEP_MESH_LEFT, crate::mesh_leave::STEP_ACCEPTED_ROSTER, crate::mesh_leave::STEP_LEAVING_ROSTER].contains(&step) {
        // The accepted roster rides the exit-manifest shape: an exit manifest with no receipts yet.
        Produces::ExitManifest
    } else if step == crate::mesh_leave::STEP_ROSTER_AMENDED {
        Produces::RosterAmended
    } else {
        return None;
    })
}

fn typed<T: serde::de::DeserializeOwned>(v: &serde_json::Value) -> Result<T, String> {
    serde_json::from_value(v.clone()).map_err(|e| format!("not a {}: {e}", std::any::type_name::<T>().rsplit("::").next().unwrap_or("value")))
}

fn json<T: Serialize>(v: &T) -> Result<serde_json::Value, String> {
    serde_json::to_value(v).map_err(|e| e.to_string())
}

fn ret_complete(step: &str) -> bool {
    RetireStep::Complete.name() == step
}

fn output_to_wire(operation: &str, step: &str, v: &serde_json::Value) -> Result<WireOutput, String> {
    let Some(kind) = produces(operation, step) else {
        return Err("this step commits no output".into());
    };
    Ok(match kind {
        Produces::Identity => WireOutput::Identity(typed(v)?),
        Produces::Bound => WireOutput::Bound(typed(v)?),
        Produces::EndpointId => WireOutput::EndpointId(typed(v)?),
        Produces::DeploymentHandle => WireOutput::DeploymentHandle(typed(v)?),
        Produces::RuntimeEvidence => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Row {
                deployment_id: String,
                provider: String,
                provider_control_domain_fingerprint: String,
                runtime_locator_kind: String,
                runtime_locator_fingerprint: String,
                #[serde(default)]
                data_dir: Option<String>,
            }
            let r: Row = typed(v)?;
            WireOutput::RuntimeEvidence(WireRuntimeEvidence {
                deployment_id: r.deployment_id,
                provider: r.provider,
                provider_control_domain_fingerprint: r.provider_control_domain_fingerprint,
                runtime_locator_kind: r.runtime_locator_kind,
                runtime_locator_fingerprint: r.runtime_locator_fingerprint,
                data_dir: r.data_dir,
            })
        }
        Produces::MeshPending => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Row {
                applied: Option<String>,
                #[serde(default)]
                reason: Option<String>,
            }
            let r: Row = typed(v)?;
            WireOutput::MeshPending(WireMeshPending { applied: r.applied, reason: r.reason })
        }
        Produces::LifecycleOp => WireOutput::LifecycleOp(typed(v)?),
        Produces::CommandAdmission => WireOutput::CommandAdmission((&typed::<CommandAdmission>(v)?).into()),
        Produces::Completion => WireOutput::Completion((&typed::<Completion>(v)?).into()),
        Produces::Storage => WireOutput::Storage((&typed::<StorageDisposition>(v)?).into()),
        Produces::Terminal => WireOutput::Terminal(typed(v)?),
        Produces::ExitManifest => WireOutput::ExitManifest(typed(v)?),
        Produces::SeatTransfer => WireOutput::SeatTransfer((&typed::<crate::fabric_handover::SeatTransferOutcome>(v)?).into()),
        Produces::RosterAmended => WireOutput::RosterAmended(typed(v)?),
    })
}

fn output_to_json(o: WireOutput) -> Result<serde_json::Value, String> {
    match o {
        WireOutput::Identity(i) => json(&i),
        WireOutput::Bound(b) => json(&b),
        WireOutput::EndpointId(e) => json(&e),
        WireOutput::DeploymentHandle(h) => json(&h),
        WireOutput::RuntimeEvidence(e) => {
            let mut v = serde_json::json!({
                "deployment_id": e.deployment_id,
                "provider": e.provider,
                "provider_control_domain_fingerprint": e.provider_control_domain_fingerprint,
                "runtime_locator_kind": e.runtime_locator_kind,
                "runtime_locator_fingerprint": e.runtime_locator_fingerprint,
            });
            if let Some(d) = e.data_dir {
                v["data_dir"] = d.into();
            }
            Ok(v)
        }
        WireOutput::MeshPending(p) => {
            let mut v = serde_json::json!({ "applied": p.applied });
            if let Some(r) = p.reason {
                v["reason"] = r.into();
            }
            Ok(v)
        }
        WireOutput::LifecycleOp(op) => json(&op),
        WireOutput::CommandAdmission(d) => json(&CommandAdmission::from(d)),
        WireOutput::Completion(a) => json(&Completion::from(a)),
        WireOutput::Storage(s) => json(&StorageDisposition::from(s)),
        WireOutput::Terminal(t) => json(&t),
        WireOutput::ExitManifest(m) => json(&m),
        WireOutput::SeatTransfer(o) => json(&crate::fabric_handover::SeatTransferOutcome::from(o)),
        WireOutput::RosterAmended(a) => json(&a),
    }
}

// ---- conversions ---------------------------------------------------------------------------

/// `fact` as its wire shape, or the refusal naming the step whose output has none.
pub(crate) fn fact_to_wire(fact: &BuildFact) -> Result<WireFact, WireRefusal> {
    Ok(match fact {
        BuildFact::Accepted(a) => WireFact::Accepted(WireAccepted {
            build_id: a.build_id.clone(),
            topology: (&a.topology).into(),
            submitted_change: a.submitted_change.as_ref().map(Into::into),
            submitted_at_ms: a.submitted_at_ms,
        }),
        BuildFact::Opened(o) => WireFact::Opened(WireOpened {
            build_id: o.build_id.clone(),
            attempt: o.attempt,
            reason: o.reason,
            action: o.action.as_ref().map(Into::into),
            opened_by: o.opened_by.clone(),
            opened_at_ms: o.opened_at_ms,
        }),
        BuildFact::Claim(c) => WireFact::Claim(c.clone()),
        BuildFact::Step(s) => {
            let refuse = |reason: String| WireRefusal { build_id: s.build_id.clone(), attempt: s.attempt, step: s.step.clone(), reason };
            let output = match &s.output {
                None => None,
                Some(v) => Some(output_to_wire(&s.operation, &s.step, v).map_err(refuse)?),
            };
            WireFact::Step(WireStep {
                build_id: s.build_id.clone(),
                attempt: s.attempt,
                operation: s.operation.clone(),
                step: s.step.clone(),
                outcome: s.outcome.clone(),
                output,
                executor: s.executor.clone(),
            })
        }
        BuildFact::Attempt(a) => WireFact::Attempt(a.clone()),
        BuildFact::Forget { build_id } => WireFact::Forget { build_id: build_id.clone() },
    })
}

/// The Build fact a wire fact carries.
pub(crate) fn fact_from_wire(w: WireFact) -> Result<BuildFact, WireRefusal> {
    Ok(match w {
        WireFact::Accepted(a) => BuildFact::Accepted(BuildAccepted {
            build_id: a.build_id,
            topology: a.topology.into(),
            submitted_change: a.submitted_change.map(Into::into),
            submitted_at_ms: a.submitted_at_ms,
        }),
        WireFact::Opened(o) => BuildFact::Opened(AttemptOpened {
            build_id: o.build_id,
            attempt: o.attempt,
            reason: o.reason,
            action: o.action.map(Into::into),
            opened_by: o.opened_by,
            opened_at_ms: o.opened_at_ms,
        }),
        WireFact::Claim(c) => BuildFact::Claim(c),
        WireFact::Step(s) => {
            let (build_id, attempt, step) = (s.build_id.clone(), s.attempt, s.step.clone());
            let output = match s.output {
                None => None,
                Some(o) => Some(output_to_json(o).map_err(|reason| WireRefusal { build_id: build_id.clone(), attempt, step: step.clone(), reason })?),
            };
            BuildFact::Step(BuildStepReceipt { build_id: s.build_id, attempt: s.attempt, operation: s.operation, step: s.step, outcome: s.outcome, output, executor: s.executor })
        }
        WireFact::Attempt(a) => BuildFact::Attempt(a),
        WireFact::Forget { build_id } => BuildFact::Forget { build_id },
    })
}

/// The Fabric record of the Build topic's frame: the identity, and the Build its sender read as
/// current when it sent it. The frame is the one the topic always carried; no receiver takes
/// `build_id` as a pointer (R-BI3: a receiver reads its current Build from its own Build entities).
#[derive(Serialize, Deserialize)]
struct WireFabric {
    fabric_id: FabricId,
    name: String,
    build_id: Option<BuildId>,
}

/// The Fabric identity a join answer carries.
#[derive(Serialize, Deserialize)]
struct WireFabricIdentity {
    fabric_id: FabricId,
    name: String,
}

/// The message of the Build topic as it travels: one postcard frame.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireMessage {
    nonce: u64,
    facts: Vec<WireFact>,
    fabric: Option<WireFabric>,
    shutdown: Option<FabricShutdown>,
}

impl WireMessage {
    /// `m` in its wire shape, or the refusal naming the step whose output has none.
    pub(crate) fn of(m: &BuildMessage) -> Result<Self, WireRefusal> {
        Ok(Self {
            nonce: m.nonce,
            facts: m.facts.iter().map(fact_to_wire).collect::<Result<_, _>>()?,
            fabric: m.fabric.as_ref().map(|r| WireFabric { fabric_id: r.identity.fabric_id.clone(), name: r.identity.name.clone(), build_id: r.current.clone() }),
            shutdown: m.shutdown.clone(),
        })
    }

    pub(crate) fn into_message(self) -> Result<BuildMessage, WireRefusal> {
        Ok(BuildMessage {
            nonce: self.nonce,
            facts: self.facts.into_iter().map(fact_from_wire).collect::<Result<_, _>>()?,
            fabric: self.fabric.map(|r| crate::fabric_builds::FabricMessage { identity: FabricIdentity { fabric_id: r.fabric_id, name: r.name }, current: r.build_id }),
            shutdown: self.shutdown,
        })
    }
}

// ---- the join answer -----------------------------------------------------------------------

/// What a join's `Joined` reply carries (`JoinNode`, op `0x1D`): the admin's control snapshot and
/// the statuses it holds, as one postcard frame. The topology is not in it: the joiner reads it
/// with `GetTopology` (op `0x1E`, [`crate::topology_read`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinAnswer {
    /// The answering admin's path name.
    pub served_by: String,
    /// The control facts the answer carries.
    pub control: JoinControl,
    /// The statuses the admin holds (original publisher and instant kept).
    pub statuses: Vec<rafka_mesh_transport::membership::Frame>,
}

/// The fabric control state an admin hands a joiner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinControl {
    /// The deployment provider the fabric's policy names.
    pub provider: crate::model::ProviderKind,
    /// The Fabric identity the admin holds.
    pub fabric: Option<FabricIdentity>,
    /// The shutdown the admin holds.
    pub shutdown: Option<FabricShutdown>,
    /// The answering admin's current Build and its attempt: the joiner's copy of that Build's facts
    /// must reach the attempt before Ready.
    pub build: Option<BuildFloor>,
    /// The answering admin's rafka-time now, in milliseconds: the joiner adopts it as it stands.
    /// An admin that holds no rafka-time answers `NotReady` naming why and builds no answer.
    pub rafka_time_ms: u64,
    /// The member cert the accepting authority's signer issued for exactly this birth, as opaque
    /// bytes RDM never parses (`fabric-certs.md`). Empty when the authority is configured with no
    /// certs.
    pub member_cert: Vec<u8>,
    /// The row's name for this birth, the name it was admitted under: the node installs it before
    /// it publishes anything or declares readiness (R-JN1, node-start.md step 2). Filled by the join
    /// door for the accepted birth; a node refuses an answer without it.
    pub name: Option<PathName>,
    /// The config the birth's row holds, handed beside its name; `None` when no row was ever
    /// written at the name. Appended last: it is the control frame's final field.
    pub config: Option<std::collections::BTreeMap<String, String>>,
}

/// The answering admin's current Build, and the attempt a joiner's copy of it must reach before Ready.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildFloor {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
}

#[derive(Serialize, Deserialize)]
struct WireJoinAnswer {
    served_by: String,
    control: WireControl,
    statuses: Vec<rafka_mesh_transport::membership::Frame>,
}

#[derive(Serialize, Deserialize)]
struct WireControl {
    provider: crate::model::ProviderKind,
    fabric: Option<WireFabricIdentity>,
    shutdown: Option<FabricShutdown>,
    build: Option<WireBuildFloor>,
    rafka_time_ms: u64,
    member_cert: Vec<u8>,
    name: Option<PathName>,
    config: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Serialize, Deserialize)]
struct WireBuildFloor {
    build_id: BuildId,
    attempt: u32,
}

/// `answer` as one postcard frame.
pub fn answer_to_wire(answer: &JoinAnswer) -> Result<Vec<u8>, String> {
    let c = &answer.control;
    let wire = WireJoinAnswer {
        served_by: answer.served_by.clone(),
        control: WireControl {
            provider: c.provider,
            fabric: c.fabric.as_ref().map(|r| WireFabricIdentity { fabric_id: r.fabric_id.clone(), name: r.name.clone() }),
            shutdown: c.shutdown.clone(),
            build: c.build.as_ref().map(|b| WireBuildFloor { build_id: b.build_id.clone(), attempt: b.attempt }),
            rafka_time_ms: c.rafka_time_ms,
            member_cert: c.member_cert.clone(),
            name: c.name.clone(),
            config: c.config.clone(),
        },
        statuses: answer.statuses.clone(),
    };
    rafka_mesh_transport::wire::encode(&wire).map_err(|e| e.to_string())
}

/// The join answer one postcard frame carries.
pub fn answer_from_wire(bytes: &[u8]) -> Result<JoinAnswer, String> {
    let w: WireJoinAnswer = rafka_mesh_transport::wire::decode(bytes).map_err(|e| e.to_string())?;
    Ok(JoinAnswer {
        served_by: w.served_by,
        control: JoinControl {
            provider: w.control.provider,
            fabric: w.control.fabric.map(|r| FabricIdentity { fabric_id: r.fabric_id, name: r.name }),
            shutdown: w.control.shutdown,
            build: w.control.build.map(|b| BuildFloor { build_id: b.build_id, attempt: b.attempt }),
            rafka_time_ms: w.control.rafka_time_ms,
            member_cert: w.control.member_cert,
            name: w.control.name,
            config: w.control.config,
        },
        statuses: w.statuses,
    })
}
