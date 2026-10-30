//! The typed shape of a Build message (`fabric_builds::BuildMessage`, the check a Build fact passes
//! before it commits; no call carries one, Q292): every type here is positional postcard
//! (`rdm_transport::wire`), carrying every field and no internally tagged enum.
//!
//! The domain types (`BuildFact`, `FabricIdentity`, the accepted topology) are JSON elsewhere: the
//! REST views, the local journal and the tests read `"fact"`, `"kind"`, `"action"`,
//! `"persistence"` keys and absent optional fields. They keep that shape; these mirrors are what
//! travels. A conversion is one `From` per direction, so a field added to a domain type without
//! its mirror does not compile.
//!
//! A step receipt's `output` is the JSON value the step committed (`serde_json::Value`, the
//! REST and journal shape). On the wire it is the typed result of the step that produced it
//! (`WireOutput`), chosen by the receipt's own step name. A step name this build does not
//! know to produce an output, or an output that is not the type its step produces, is refused by
//! name (`WireRefusal`) — never sent as something else, never dropped.

use crate::accepted::{AttemptAction, FabricTopology, MeshTopology, TopologyChange};
use crate::build::{BuildId, FabricDesired, MeshDesired};
use crate::build_state::{AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStepReceipt, AttemptOpened, StepOutcome};
use crate::deployment::pipeline::Bound;
use crate::deployment::pipeline::{CommandAdmission, Completion, CreateStep, Identity, DeleteStep, StorageDisposition, TerminalReceipt};
use crate::mesh_delete::{ExitManifest, RosterAmended};
use crate::deployment::provider::DeploymentHandle;
use crate::fabric_builds::BuildMessage;
use crate::model::{EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName};
use rdm_core::meta::{NodeSpec, PersistentDisposition, PlacementMeta, StorageMeta};
use rdm_core::LifecycleOp;
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
    Claim(WireClaim),
    Step(WireStep),
    Attempt(WireAttempt),
    Forget { build_id: BuildId },
    /// One mesh's section of an accepted Build (Q284), appended last: what travels in place of the
    /// whole acceptance, which only the fabric-primary records.
    Section(WireSection),
}

/// A section of an accepted Build (Q284): the Build's id, fabric, `topology_storage_provider` and
/// acceptance time, whether it deletes the fabric (the one use of its change outside the
/// fabric-primary), every mesh it names by name and id, and the meshes the section holds (one, or none
/// for a Build with no mesh or none touched), with their pinned births.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireSection {
    build_id: BuildId,
    fabric: String,
    topology_storage_provider: Option<String>,
    submitted_at_ms: u64,
    deletes_fabric: bool,
    named: BTreeMap<String, MeshId>,
    meshes: Vec<WireMesh>,
    pinned: Vec<(PathName, crate::build::PinnedBirth)>,
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
    mesh: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WireClaim {
    build_id: BuildId,
    attempt: u32,
    executor: String,
    claimant: Option<String>,
    mesh: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WireAttempt {
    build_id: BuildId,
    attempt: u32,
    outcome: crate::build_state::AttemptOutcome,
    mesh: Option<String>,
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
    mesh: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct WireTopology {
    fabric: String,
    meshes: BTreeMap<String, WireMesh>,
    /// The birth each node the Build deletes held at acceptance (build.md §4.4), appended last.
    pinned: Vec<(PathName, crate::build::PinnedBirth)>,
}

#[derive(Serialize, Deserialize)]
struct WireMesh {
    name: String,
    mesh_id: MeshId,
    nodes: BTreeSet<PathName>,
    spec_by_path: BTreeMap<PathName, WireSpec>,
}

/// A create spec as a Build message carries it: storage and placement, positional. The executable, the
/// tags and the config never ride gossip (proposal executable-bindings-at-create.md rule 7; Q166): the
/// store root's record of the accepted Build carries them.
#[derive(Serialize, Deserialize)]
struct WireSpec {
    storage: WireStorage,
    placement: PlacementMeta,
}

#[derive(Serialize, Deserialize)]
enum WireStorage {
    Ephemeral,
    Persistent { disposition: PersistentDisposition },
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
    DeleteFabric,
    /// Reserved position: the removed per-kind executable-set change (proposal
    /// executable-bindings-at-create.md §1). Read as the stored history it is
    /// ([`TopologyChange::Reserved`]); kept so `Reconcile` keeps its position.
    Reserved7,
    /// The complete topology (proposal executable-bindings-at-create.md rule 1), appended last. Each
    /// node's create spec travels as the topology's do, by its path.name with its storage and placement,
    /// no executable, no tags and no config (rule 7: nothing about executables rides gossip); the stored
    /// accepted Build keeps them.
    Reconcile { based_on: BuildId, nodes: Vec<(PathName, WireSpec)> },
}

#[derive(Serialize, Deserialize)]
enum WireAction {
    /// Reserved position: the removed node.restart's action (Q183). Never sent; a fact carrying it is
    /// refused by name ([`fact_from_wire`]). Kept so every later action keeps its position.
    Reserved0 { path: PathName, from_incarnation: IncarnationId },
    DeleteConnections { path: PathName, node_id: NodeId, from_incarnation: IncarnationId },
    Stop { path: PathName, node_id: NodeId, from_incarnation: IncarnationId },
    Start { path: PathName, node_id: NodeId, from_incarnation: IncarnationId },
    Recover { path: PathName, node_id: NodeId, from_incarnation: IncarnationId },
    Delete { path: PathName, node_id: NodeId, from_incarnation: IncarnationId },
    Steps { steps: Vec<rdm_core::build::Step>, pinned: Vec<(PathName, crate::build::PinnedBirth)> },
    Terminate { path: PathName, node_id: NodeId, from_incarnation: IncarnationId },
    /// Fabric recovery's backbone re-create, a node.create alone (Q142); appended, so every earlier
    /// variant keeps its position.
    RecoverFromBackbone { path: PathName, node_id: NodeId, from_incarnation: IncarnationId },
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
            TopologyChange::DeleteFabric => Self::DeleteFabric,
            TopologyChange::Reserved {} => Self::Reserved7,
            TopologyChange::Reconcile { based_on, nodes, .. } => Self::Reconcile { based_on: based_on.clone(), nodes: nodes.iter().map(|s| (s.path.clone(), s.into())).collect() },
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
            WireChange::DeleteFabric => Self::DeleteFabric,
            WireChange::Reserved7 => Self::Reserved {},
            WireChange::Reconcile { based_on, nodes } => Self::Reconcile { based_on, nodes: nodes.into_iter().map(|(p, w)| w.at(p)).collect(), topology_storage_provider: None },
        }
    }
}
impl From<&AttemptAction> for WireAction {
    fn from(a: &AttemptAction) -> Self {
        match a {
            AttemptAction::DeleteConnections { path, node_id, from_incarnation } => Self::DeleteConnections { path: path.clone(), node_id: node_id.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Stop { path, node_id, from_incarnation } => Self::Stop { path: path.clone(), node_id: node_id.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Start { path, node_id, from_incarnation } => Self::Start { path: path.clone(), node_id: node_id.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Recover { path, node_id, from_incarnation, backbone: false } => Self::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Recover { path, node_id, from_incarnation, backbone: true } => Self::RecoverFromBackbone { path: path.clone(), node_id: node_id.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Delete { path, node_id, from_incarnation } => Self::Delete { path: path.clone(), node_id: node_id.clone(), from_incarnation: from_incarnation.clone() },
            AttemptAction::Steps { steps, pinned } => Self::Steps { steps: steps.clone(), pinned: pinned.clone() },
            AttemptAction::Terminate { path, node_id, from_incarnation } => Self::Terminate { path: path.clone(), node_id: node_id.clone(), from_incarnation: from_incarnation.clone() },
        }
    }
}
impl TryFrom<WireAction> for AttemptAction {
    type Error = String;
    /// The action a wire action carries; the reserved position of the removed node.restart is refused
    /// by name.
    fn try_from(a: WireAction) -> Result<Self, String> {
        Ok(match a {
            WireAction::Reserved0 { path, .. } => return Err(format!("the attempt's action is node.restart of {path}, a reserved position: node.restart is removed (Q183)")),
            WireAction::DeleteConnections { path, node_id, from_incarnation } => Self::DeleteConnections { path, node_id, from_incarnation },
            WireAction::Stop { path, node_id, from_incarnation } => Self::Stop { path, node_id, from_incarnation },
            WireAction::Start { path, node_id, from_incarnation } => Self::Start { path, node_id, from_incarnation },
            WireAction::Recover { path, node_id, from_incarnation } => Self::Recover { path, node_id, from_incarnation, backbone: false },
            WireAction::RecoverFromBackbone { path, node_id, from_incarnation } => Self::Recover { path, node_id, from_incarnation, backbone: true },
            WireAction::Delete { path, node_id, from_incarnation } => Self::Delete { path, node_id, from_incarnation },
            WireAction::Steps { steps, pinned } => Self::Steps { steps, pinned },
            WireAction::Terminate { path, node_id, from_incarnation } => Self::Terminate { path, node_id, from_incarnation },
        })
    }
}
/// An action through its wire form and back, as a Build message carries it.
#[cfg(test)]
pub(crate) fn action_round_trip(a: &AttemptAction) -> AttemptAction {
    let wire = WireAction::from(a);
    let bytes = rdm_transport::wire::encode(&wire).expect("an action encodes");
    rdm_transport::wire::decode::<WireAction>(&bytes).expect("an action decodes").try_into().expect("an action this build reads")
}

impl From<&NodeSpec> for WireSpec {
    fn from(m: &NodeSpec) -> Self {
        let storage = match m.storage_persistence {
            StorageMeta::Ephemeral => WireStorage::Ephemeral,
            StorageMeta::Persistent { disposition } => WireStorage::Persistent { disposition },
        };
        Self { storage, placement: m.placement }
    }
}
impl WireSpec {
    /// The create spec at `path` heard in a Build message: it names no executable, no tags and no config
    /// (proposal executable-bindings-at-create.md rule 7; Q166): the store root's record of the accepted
    /// Build is the one that does.
    fn at(self, path: PathName) -> NodeSpec {
        let storage_persistence = match self.storage {
            WireStorage::Ephemeral => StorageMeta::Ephemeral,
            WireStorage::Persistent { disposition } => StorageMeta::Persistent { disposition },
        };
        NodeSpec { path, storage_persistence, placement: self.placement, executable: None, tags: Default::default(), config: Default::default() }
    }
}
impl From<&MeshTopology> for WireMesh {
    fn from(m: &MeshTopology) -> Self {
        Self { name: m.name.clone(), mesh_id: m.mesh_id.clone(), nodes: m.nodes.clone(), spec_by_path: m.spec_by_path.iter().map(|(p, meta)| (p.clone(), meta.into())).collect() }
    }
}
impl From<WireMesh> for MeshTopology {
    fn from(m: WireMesh) -> Self {
        Self { name: m.name, mesh_id: m.mesh_id, nodes: m.nodes, spec_by_path: m.spec_by_path.into_iter().map(|(p, w)| (p.clone(), w.at(p))).collect() }
    }
}
impl From<&FabricTopology> for WireTopology {
    fn from(t: &FabricTopology) -> Self {
        let meshes = t
            .meshes
            .iter()
            .map(|(k, m)| (k.clone(), m.into()))
            .collect();
        Self { fabric: t.fabric.clone(), meshes, pinned: t.pinned.clone() }
    }
}
impl From<WireTopology> for FabricTopology {
    fn from(t: WireTopology) -> Self {
        let meshes = t
            .meshes
            .into_iter()
            .map(|(k, m)| (k, m.into()))
            .collect();
        // The Build topic carries no topology_storage_provider: the journal's record of the accepted
        // Build is the one that does.
        Self { fabric: t.fabric, meshes, topology_storage_provider: None, storage_change: None, pinned: t.pinned, section_of: None }
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
    RosterAmended(RosterAmended),
    /// The recorded transition under an in-force fabric.delete (Q160).
    PrimariesTaken(crate::build_state::PrimariesTaken),
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
    NotSent { reason: String, code: rdm_core::outcome::Code },
    Indeterminate { reason: String, code: rdm_core::outcome::Code },
    Refused { reply: String, code: rdm_core::outcome::Code },
    Stopped { receipt: rdm_core::status::ConnectionsReceipt },
    Started,
    ConnectionsDeleted { receipt: rdm_core::status::ConnectionsReceipt },
    StartFailed { step: String, reason: String, code: rdm_core::outcome::Code },
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
            CommandAdmission::Admitted {} => Self::Admitted,
            CommandAdmission::AlreadyAdmitted {} => Self::AlreadyAdmitted,
            CommandAdmission::NotSent { reason, code } => Self::NotSent { reason: reason.clone(), code: *code },
            CommandAdmission::Indeterminate { reason, code } => Self::Indeterminate { reason: reason.clone(), code: *code },
            CommandAdmission::Refused { reply, code } => Self::Refused { reply: reply.clone(), code: *code },
            CommandAdmission::Stopped { receipt } => Self::Stopped { receipt: receipt.clone() },
            CommandAdmission::ConnectionsDeleted { receipt } => Self::ConnectionsDeleted { receipt: receipt.clone() },
            CommandAdmission::Started {} => Self::Started,
            CommandAdmission::StartFailed { step, reason, code } => Self::StartFailed { step: step.clone(), reason: reason.clone(), code: *code },
        }
    }
}
impl From<WireCommandAdmission> for CommandAdmission {
    fn from(d: WireCommandAdmission) -> Self {
        match d {
            WireCommandAdmission::Admitted => Self::Admitted {},
            WireCommandAdmission::AlreadyAdmitted => Self::AlreadyAdmitted {},
            WireCommandAdmission::NotSent { reason, code } => Self::NotSent { reason, code },
            WireCommandAdmission::Indeterminate { reason, code } => Self::Indeterminate { reason, code },
            WireCommandAdmission::Refused { reply, code } => Self::Refused { reply, code },
            WireCommandAdmission::Stopped { receipt } => Self::Stopped { receipt },
            WireCommandAdmission::ConnectionsDeleted { receipt } => Self::ConnectionsDeleted { receipt },
            WireCommandAdmission::Started => Self::Started {},
            WireCommandAdmission::StartFailed { step, reason, code } => Self::StartFailed { step, reason, code },
        }
    }
}
impl From<&Completion> for WireCompletion {
    fn from(a: &Completion) -> Self {
        match a {
            Completion::Received {} => Self::Received,
            Completion::Deadline {} => Self::Deadline,
            Completion::RuntimeExited {} => Self::RuntimeExited,
            Completion::NotAwaited { admission } => Self::NotAwaited { admission: admission.clone() },
        }
    }
}
impl From<WireCompletion> for Completion {
    fn from(a: WireCompletion) -> Self {
        match a {
            WireCompletion::Received => Self::Received {},
            WireCompletion::Deadline => Self::Deadline {},
            WireCompletion::RuntimeExited => Self::RuntimeExited {},
            WireCompletion::NotAwaited { admission } => Self::NotAwaited { admission },
        }
    }
}
impl From<&StorageDisposition> for WireStorageDisposition {
    fn from(s: &StorageDisposition) -> Self {
        match s {
            StorageDisposition::Released { locator } => Self::Released { locator: locator.clone() },
            StorageDisposition::Preserved {} => Self::Preserved,
            StorageDisposition::PreservedNoMeta {} => Self::PreservedNoMeta,
        }
    }
}
impl From<WireStorageDisposition> for StorageDisposition {
    fn from(s: WireStorageDisposition) -> Self {
        match s {
            WireStorageDisposition::Released { locator } => Self::Released { locator },
            WireStorageDisposition::Preserved => Self::Preserved {},
            WireStorageDisposition::PreservedNoMeta => Self::PreservedNoMeta {},
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
    RosterAmended,
    PrimariesTaken,
}

fn produces(operation: &str, step: &str) -> Option<Produces> {
    let is = |c: CreateStep| c.name() == step;
    // A standalone stop's `Complete` commits the stop's lifecycle op: the exact birth and its
    // rafka-time, the age of its stopped period (node-identity-and-state.md §6.9 Age). Every other
    // operation's `Complete` commits none.
    if ret_complete(step) && operation.starts_with("node.stop:") {
        return Some(Produces::LifecycleOp);
    }
    let ret = |r: DeleteStep| r.name() == step;
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
    } else if ret(DeleteStep::NodeDeleting) || ret(DeleteStep::NodeDeleted) {
        Produces::LifecycleOp
    } else if ret(DeleteStep::DeleteConnections) || ret(DeleteStep::StopNode) || step == crate::deployment::pipeline::StartStep::StartNode.name() {
        Produces::CommandAdmission
    } else if ret(DeleteStep::AwaitNodeLeft) {
        Produces::Completion
    } else if step == RELEASE_STORAGE_STEP {
        Produces::Storage
    } else if ret(DeleteStep::TerminateRuntime) {
        Produces::Terminal
    } else if [crate::mesh_delete::STEP_OTHER_MEMBERS_EXITED, crate::mesh_delete::STEP_MESH_DELETED, crate::mesh_delete::STEP_MESH_LEFT, crate::mesh_delete::STEP_ACCEPTED_ROSTER, crate::mesh_delete::STEP_LEAVING_ROSTER].contains(&step) {
        // The accepted roster rides the exit-manifest shape: an exit manifest with no receipts yet.
        Produces::ExitManifest
    } else if step == crate::mesh_delete::STEP_ROSTER_AMENDED {
        Produces::RosterAmended
    } else if step == crate::build_state::STEP_PRIMARIES_TAKEN {
        Produces::PrimariesTaken
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
    DeleteStep::Complete.name() == step
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
        Produces::RosterAmended => WireOutput::RosterAmended(typed(v)?),
        Produces::PrimariesTaken => WireOutput::PrimariesTaken(typed(v)?),
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
        WireOutput::RosterAmended(a) => json(&a),
        WireOutput::PrimariesTaken(t) => json(&t),
    }
}

// ---- conversions ---------------------------------------------------------------------------

/// `fact` as its wire shape, or the refusal naming the step whose output has none.
pub(crate) fn fact_to_wire(fact: &BuildFact) -> Result<WireFact, WireRefusal> {
    Ok(match fact {
        BuildFact::Accepted(a) if a.topology.section_of.is_some() => WireFact::Section(WireSection {
            build_id: a.build_id.clone(),
            fabric: a.topology.fabric.clone(),
            topology_storage_provider: a.topology.topology_storage_provider.clone(),
            submitted_at_ms: a.submitted_at_ms,
            deletes_fabric: matches!(a.submitted_change, Some(TopologyChange::DeleteFabric)),
            named: a.topology.section_of.clone().unwrap_or_default(),
            meshes: a.topology.meshes.values().map(WireMesh::from).collect(),
            pinned: a.topology.pinned.clone(),
        }),
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
            mesh: o.mesh.clone(),
        }),
        BuildFact::Claim(c) => WireFact::Claim(WireClaim { build_id: c.build_id.clone(), attempt: c.attempt, executor: c.executor.clone(), claimant: c.claimant.clone(), mesh: c.mesh.clone() }),
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
                mesh: s.mesh.clone(),
            })
        }
        BuildFact::Attempt(a) => WireFact::Attempt(WireAttempt { build_id: a.build_id.clone(), attempt: a.attempt, outcome: a.outcome.clone(), mesh: a.mesh.clone() }),
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
            action: {
                let refuse = |reason: String| WireRefusal { build_id: o.build_id.clone(), attempt: o.attempt, step: "Opened".into(), reason };
                if o.reason == AttemptReason::Reserved2 {
                    return Err(refuse("the attempt's reason is node.restart's, a reserved position: node.restart is removed (Q183)".into()));
                }
                o.action.map(AttemptAction::try_from).transpose().map_err(refuse)?
            },
            build_id: o.build_id,
            attempt: o.attempt,
            reason: o.reason,
            opened_by: o.opened_by,
            opened_at_ms: o.opened_at_ms,
            mesh: o.mesh,
            context: None,
        }),
        WireFact::Claim(c) => BuildFact::Claim(BuildAttemptClaim { build_id: c.build_id, attempt: c.attempt, executor: c.executor, claimant: c.claimant, mesh: c.mesh }),
        WireFact::Step(s) => {
            let (build_id, attempt, step) = (s.build_id.clone(), s.attempt, s.step.clone());
            let output = match s.output {
                None => None,
                Some(o) => Some(output_to_json(o).map_err(|reason| WireRefusal { build_id: build_id.clone(), attempt, step: step.clone(), reason })?),
            };
            BuildFact::Step(BuildStepReceipt { build_id: s.build_id, attempt: s.attempt, operation: s.operation, step: s.step, outcome: s.outcome, output, executor: s.executor, mesh: s.mesh })
        }
        WireFact::Attempt(a) => BuildFact::Attempt(BuildAttemptReceipt { build_id: a.build_id, attempt: a.attempt, outcome: a.outcome, mesh: a.mesh }),
        WireFact::Forget { build_id } => BuildFact::Forget { build_id },
        WireFact::Section(c) => BuildFact::Accepted(BuildAccepted {
            build_id: c.build_id,
            topology: FabricTopology {
                fabric: c.fabric,
                meshes: c.meshes.into_iter().map(|m| (m.name.clone(), m.into())).collect(),
                topology_storage_provider: c.topology_storage_provider,
                storage_change: None,
                pinned: c.pinned,
                section_of: Some(c.named),
            },
            submitted_change: c.deletes_fabric.then_some(TopologyChange::DeleteFabric),
            submitted_at_ms: c.submitted_at_ms,
        }),
    })
}

/// The Fabric identity at the reserved position of [`WireMessage`]: kept so the frame keeps its bytes.
#[derive(Serialize, Deserialize)]
struct WireFabricIdentity {
    fabric_id: FabricId,
    name: String,
}

/// A Build message in its typed shape: one postcard frame.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireMessage {
    nonce: u64,
    facts: Vec<WireFact>,
    /// Reserved position: the removed Fabric record of the Build channel (Q185). Sent as none; a frame
    /// carrying one is refused by name. Kept so the frame keeps its bytes.
    fabric: Option<WireFabricIdentity>,
}

impl WireMessage {
    /// `m` in its wire shape, or the refusal naming the step whose output has none.
    pub(crate) fn of(m: &BuildMessage) -> Result<Self, WireRefusal> {
        Ok(Self { nonce: m.nonce, facts: m.facts.iter().map(fact_to_wire).collect::<Result<_, _>>()?, fabric: None })
    }

    /// The message this frame carries, or why it is not one: a frame carrying the removed Fabric
    /// record (the reserved position) is refused by name, as a fact this build does not read is.
    pub(crate) fn into_message(self) -> Result<BuildMessage, String> {
        if let Some(r) = self.fabric {
            return Err(format!("a Build message carrying the Fabric record of Fabric {} ({}): the Build channel's record is removed (Q185) and its position is reserved", r.fabric_id, r.name));
        }
        Ok(BuildMessage { nonce: self.nonce, facts: self.facts.into_iter().map(fact_from_wire).collect::<Result<_, _>>().map_err(|e| e.to_string())? })
    }
}

// ---- the join answer -----------------------------------------------------------------------

pub use rdm_node::wire::{answer_from_wire, answer_to_wire, BuildFloor, JoinAnswer, JoinControl};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::StepOutcome;
    use crate::deployment::pipeline::{CreateStep, DeleteStep};
    use crate::model::{DeploymentId, ProviderKind};
    use serde_json::{json, Value};

    const EXTRA: &str = "zz_extra";

    /// The Build fact of a completed step whose receipt carries `output`.
    fn step_fact(operation: &str, step: &str, output: Value) -> BuildFact {
        BuildFact::Step(BuildStepReceipt {
            mesh: None,
            build_id: BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(),
            attempt: 3,
            operation: operation.into(),
            step: step.into(),
            outcome: StepOutcome::Complete,
            output: Some(output),
            executor: None,
        })
    }

    /// `valid` with one field no type of its kind declares, put at the JSON pointer `at` ("" is the
    /// output itself).
    fn with_extra(mut valid: Value, at: &str) -> Value {
        let shown = valid.to_string();
        let target = valid.pointer_mut(at).unwrap_or_else(|| panic!("the valid output has nothing at {at:?}: {shown}"));
        target.as_object_mut().unwrap_or_else(|| panic!("the valid output at {at:?} is not an object: {shown}")).insert(EXTRA.into(), json!(1));
        valid
    }

    /// CONTRACT (CLAUDE.md "DURABLE": "a reader that does not RECOGNISE a value must REFUSE with a
    /// named reason"; this module's doc: an output that is not the type its step produces "is refused
    /// by name ... never dropped"): the step's valid output goes to the wire; the same output with one
    /// field the type does not declare (at `at`) is refused, and the refusal names the step, the Build
    /// and attempt, and the field. Must NOT happen: the field dropped and the fact sent as a different
    /// output than the one the step committed.
    fn unknown_field_is_refused_by_name(operation: &str, step: &str, valid: Value, at: &str) {
        if let Err(r) = fact_to_wire(&step_fact(operation, step, valid.clone())) {
            panic!("the valid output of step {step} was refused: {r}");
        }
        let Err(refusal) = fact_to_wire(&step_fact(operation, step, with_extra(valid, at))) else {
            panic!("the output of step {step} carried the unknown field {EXTRA} at {at:?} and went to the wire: the field was dropped");
        };
        assert_eq!(refusal.step, step, "the step whose output was refused");
        assert_eq!(refusal.attempt, 3);
        let text = refusal.to_string();
        assert!(text.contains(&format!("step {step} ")) && text.contains("attempt 3"), "the refusal names the step and attempt: {text}");
        assert!(text.contains(&format!("unknown field `{EXTRA}`")), "the refusal names the unknown field: {text}");
    }

    fn runtime_proof() -> Value {
        json!({ "deployment_id": "d1", "provider": "process", "control_domain_fingerprint": "cd", "locator_kind": "pid", "locator_fingerprint": "lf" })
    }

    fn roster_birth(name: &str) -> Value {
        json!({ "node": name, "node_id": crate::model::NodeId::mint().to_string(), "incarnation_id": crate::model::IncarnationId::mint().to_string() })
    }

    fn terminal() -> Value {
        json!({ "node": "mesh1.rpc.1", "node_id": crate::model::NodeId::mint().to_string(), "incarnation_id": crate::model::IncarnationId::mint().to_string(), "runtime": runtime_proof(), "exit_code": 0 })
    }

    #[test]
    fn an_identity_output_with_an_unknown_field_is_refused_by_name() {
        let valid = json!({ "node_id": crate::model::NodeId::mint(), "incarnation": crate::model::IncarnationId::mint(), "supersedes": null, "deployment_id": DeploymentId::mint() });
        unknown_field_is_refused_by_name("node.create:mesh1.rpc.1", CreateStep::AllocateIdentity.name(), valid, "");
    }

    #[test]
    fn a_bound_output_with_an_unknown_field_is_refused_by_name() {
        let valid = json!({ "transport": "127.0.0.1:4000", "listeners": [["http", "127.0.0.1:4001"]] });
        unknown_field_is_refused_by_name("node.create:mesh1.rpc.1", CreateStep::WaitForBind.name(), valid, "");
    }

    /// An endpoint id is the endpoint's key as one string: it has no field to add, so what the reader
    /// refuses is an object where the string is.
    #[test]
    fn an_endpoint_id_output_that_is_an_object_is_refused_by_name() {
        let step = CreateStep::PrepareStorage.name();
        let operation = "node.create:mesh1.rpc.1";
        if let Err(r) = fact_to_wire(&step_fact(operation, step, json!("endpoint-key"))) {
            panic!("the valid output of step {step} was refused: {r}");
        }
        let Err(refusal) = fact_to_wire(&step_fact(operation, step, json!({ EXTRA: 1 }))) else { panic!("an object went to the wire as the endpoint id") };
        let text = refusal.to_string();
        assert!(text.contains(&format!("step {step} ")) && text.contains("not a EndpointId"), "the refusal names the step and the type: {text}");
    }

    #[test]
    fn a_deployment_handle_output_with_an_unknown_field_is_refused_by_name() {
        let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: Some(7), start: Some(8), container: None, domain: Some("cd".into()) };
        unknown_field_is_refused_by_name("node.create:mesh1.rpc.1", CreateStep::DeployRuntime.name(), serde_json::to_value(&handle).unwrap(), "");
    }

    #[test]
    fn a_runtime_evidence_output_with_an_unknown_field_is_refused_by_name() {
        let valid = json!({ "deployment_id": "d1", "provider": "process", "provider_control_domain_fingerprint": "cd", "runtime_locator_kind": "pid", "runtime_locator_fingerprint": "lf" });
        unknown_field_is_refused_by_name("node.create:mesh1.rpc.1", CreateStep::RegisterExactRuntimeHandle.name(), valid, "");
    }

    #[test]
    fn a_mesh_pending_output_with_an_unknown_field_is_refused_by_name() {
        unknown_field_is_refused_by_name("node.create:mesh1.rpc.1", CreateStep::ApplyMeshPending.name(), json!({ "applied": "mesh1" }), "");
    }

    #[test]
    fn a_lifecycle_op_output_with_an_unknown_field_is_refused_by_name() {
        let op = rdm_core::LifecycleOp {
            build_id: "b1".into(),
            attempt: 3,
            operation: "node.delete:mesh1.rpc.1".into(),
            node_id: crate::model::NodeId::mint(),
            incarnation: crate::model::IncarnationId::mint(),
            name: "mesh1.rpc.1".parse().unwrap(),
            event_at_rafka_ms: 4_242,
            traceparent: None,
            tracestate: None,
        };
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", DeleteStep::NodeDeleting.name(), serde_json::to_value(&op).unwrap(), "");
    }

    #[test]
    fn a_command_admission_output_with_an_unknown_field_is_refused_by_name() {
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", DeleteStep::DeleteConnections.name(), json!({ "arm": "admitted" }), "");
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", DeleteStep::StopNode.name(), json!({ "arm": "not-sent", "reason": "no route" }), "");
    }

    #[test]
    fn a_connections_receipt_in_an_admission_with_an_unknown_field_is_refused_by_name() {
        let valid = json!({ "arm": "connections-deleted", "receipt": { "Established": { "in_flight": 0, "closed": [] } } });
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", DeleteStep::DeleteConnections.name(), valid, "/receipt/Established");
    }

    #[test]
    fn a_completion_output_with_an_unknown_field_is_refused_by_name() {
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", DeleteStep::AwaitNodeLeft.name(), json!({ "arm": "received" }), "");
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", DeleteStep::AwaitNodeLeft.name(), json!({ "arm": "not-awaited", "admission": "refused" }), "");
    }

    #[test]
    fn a_storage_disposition_output_with_an_unknown_field_is_refused_by_name() {
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", RELEASE_STORAGE_STEP, json!({ "arm": "preserved" }), "");
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", RELEASE_STORAGE_STEP, json!({ "arm": "released", "locator": "dir" }), "");
    }

    #[test]
    fn a_terminal_output_with_an_unknown_field_is_refused_by_name() {
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", DeleteStep::TerminateRuntime.name(), terminal(), "");
    }

    #[test]
    fn a_runtime_proof_inside_a_terminal_output_with_an_unknown_field_is_refused_by_name() {
        unknown_field_is_refused_by_name("node.delete:mesh1.rpc.1", DeleteStep::TerminateRuntime.name(), terminal(), "/runtime");
    }

    fn exit_manifest() -> Value {
        json!({ "mesh_id": "meshd0000001", "roster": [roster_birth("mesh1.admin.1")], "final_primary": roster_birth("mesh1.admin.1"), "final_runtime": runtime_proof(), "receipts": [terminal()] })
    }

    #[test]
    fn an_exit_manifest_output_with_an_unknown_field_is_refused_by_name() {
        let step = crate::mesh_delete::STEP_OTHER_MEMBERS_EXITED;
        unknown_field_is_refused_by_name("mesh.delete:mesh1", step, exit_manifest(), "");
        unknown_field_is_refused_by_name("mesh.delete:mesh1", step, exit_manifest(), "/roster/0");
        unknown_field_is_refused_by_name("mesh.delete:mesh1", step, exit_manifest(), "/final_primary");
        unknown_field_is_refused_by_name("mesh.delete:mesh1", step, exit_manifest(), "/receipts/0");
    }

    #[test]
    fn a_roster_amended_output_with_an_unknown_field_is_refused_by_name() {
        let valid = json!({ "predecessor": roster_birth("mesh1.admin.1"), "final_primary": roster_birth("mesh1.admin.2"), "final_runtime": runtime_proof() });
        unknown_field_is_refused_by_name("mesh.delete:mesh1", crate::mesh_delete::STEP_ROSTER_AMENDED, valid.clone(), "");
        unknown_field_is_refused_by_name("mesh.delete:mesh1", crate::mesh_delete::STEP_ROSTER_AMENDED, valid, "/final_runtime");
    }
}
