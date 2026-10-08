//! The wire shape of the Build topic (`fabric_builds`): every type here is positional postcard
//! (`rafka_mesh_transport::wire`), carrying every field and no internally tagged enum.
//!
//! The domain types (`BuildFact`, `FabricRecord`, the accepted topology) are JSON elsewhere: the
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
use crate::deployment::pipeline::{AdmissionClosure, CreateStep, DrainOutcome, Identity, RetireStep, StorageDisposition};
use crate::deployment::provider::DeploymentHandle;
use crate::fabric_builds::BuildMessage;
use crate::fabric_storage::{FabricRecord, FabricShutdown};
use crate::model::{EndpointId, FabricId, IncarnationId, NodeKind, PathName};
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
        }
    }
}
impl From<WireAction> for AttemptAction {
    fn from(a: WireAction) -> Self {
        match a {
            WireAction::Restart { path, from_incarnation } => Self::Restart { path, from_incarnation },
            WireAction::Replace { path, from_incarnation } => Self::Replace { path, from_incarnation },
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
                let mesh = MeshTopology { name: m.name, nodes: m.nodes, node_meta_by_path: m.node_meta_by_path.into_iter().map(|(p, meta)| (p, meta.into())).collect() };
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
    Drain(WireDrain),
    Admission(WireAdmission),
    Storage(WireStorageDisposition),
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
pub(crate) enum WireDrain {
    Established { in_flight: u64 },
    NotSent { reason: String },
    Indeterminate { reason: String },
    Refused { reply: String },
    Deadline { last_in_flight: Option<u64> },
}

#[derive(Serialize, Deserialize)]
pub(crate) enum WireAdmission {
    Heard,
    ThisRuntimeExited,
    Deadline { last_refusal: String },
    DrainNotEstablished,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum WireStorageDisposition {
    Released { locator: String },
    Preserved,
    PreservedNoMeta,
}

impl From<&DrainOutcome> for WireDrain {
    fn from(d: &DrainOutcome) -> Self {
        match d {
            DrainOutcome::Established { in_flight } => Self::Established { in_flight: *in_flight },
            DrainOutcome::NotSent { reason } => Self::NotSent { reason: reason.clone() },
            DrainOutcome::Indeterminate { reason } => Self::Indeterminate { reason: reason.clone() },
            DrainOutcome::Refused { reply } => Self::Refused { reply: reply.clone() },
            DrainOutcome::Deadline { last_in_flight } => Self::Deadline { last_in_flight: *last_in_flight },
        }
    }
}
impl From<WireDrain> for DrainOutcome {
    fn from(d: WireDrain) -> Self {
        match d {
            WireDrain::Established { in_flight } => Self::Established { in_flight },
            WireDrain::NotSent { reason } => Self::NotSent { reason },
            WireDrain::Indeterminate { reason } => Self::Indeterminate { reason },
            WireDrain::Refused { reply } => Self::Refused { reply },
            WireDrain::Deadline { last_in_flight } => Self::Deadline { last_in_flight },
        }
    }
}
impl From<&AdmissionClosure> for WireAdmission {
    fn from(a: &AdmissionClosure) -> Self {
        match a {
            AdmissionClosure::Heard => Self::Heard,
            AdmissionClosure::ThisRuntimeExited => Self::ThisRuntimeExited,
            AdmissionClosure::Deadline { last_refusal } => Self::Deadline { last_refusal: last_refusal.clone() },
            AdmissionClosure::DrainNotEstablished => Self::DrainNotEstablished,
        }
    }
}
impl From<WireAdmission> for AdmissionClosure {
    fn from(a: WireAdmission) -> Self {
        match a {
            WireAdmission::Heard => Self::Heard,
            WireAdmission::ThisRuntimeExited => Self::ThisRuntimeExited,
            WireAdmission::Deadline { last_refusal } => Self::Deadline { last_refusal },
            WireAdmission::DrainNotEstablished => Self::DrainNotEstablished,
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
    Drain,
    Admission,
    Storage,
}

fn produces(step: &str) -> Option<Produces> {
    let is = |c: CreateStep| c.name() == step;
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
    } else if ret(RetireStep::NodeDeleting) || ret(RetireStep::NodeRestarting) || ret(RetireStep::NodeDeleted) {
        Produces::LifecycleOp
    } else if ret(RetireStep::MarkDraining) || ret(RetireStep::WaitForDrain) {
        Produces::Drain
    } else if ret(RetireStep::CloseRpcAdmission) {
        Produces::Admission
    } else if ret(RetireStep::ReleaseStorage) {
        Produces::Storage
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

fn output_to_wire(step: &str, v: &serde_json::Value) -> Result<WireOutput, String> {
    let Some(kind) = produces(step) else {
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
        Produces::Drain => WireOutput::Drain((&typed::<DrainOutcome>(v)?).into()),
        Produces::Admission => WireOutput::Admission((&typed::<AdmissionClosure>(v)?).into()),
        Produces::Storage => WireOutput::Storage((&typed::<StorageDisposition>(v)?).into()),
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
        WireOutput::Drain(d) => json(&DrainOutcome::from(d)),
        WireOutput::Admission(a) => json(&AdmissionClosure::from(a)),
        WireOutput::Storage(s) => json(&StorageDisposition::from(s)),
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
                Some(v) => Some(output_to_wire(&s.step, v).map_err(refuse)?),
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

#[derive(Serialize, Deserialize)]
struct WireFabric {
    fabric_id: FabricId,
    name: String,
    build_id: Option<BuildId>,
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
            fabric: m.fabric.as_ref().map(|r| WireFabric { fabric_id: r.fabric_id.clone(), name: r.name.clone(), build_id: r.build_id.clone() }),
            shutdown: m.shutdown.clone(),
        })
    }

    pub(crate) fn into_message(self) -> Result<BuildMessage, WireRefusal> {
        Ok(BuildMessage {
            nonce: self.nonce,
            facts: self.facts.into_iter().map(fact_from_wire).collect::<Result<_, _>>()?,
            fabric: self.fabric.map(|r| FabricRecord { fabric_id: r.fabric_id, name: r.name, build_id: r.build_id }),
            shutdown: self.shutdown,
        })
    }
}

// ---- the join answer -----------------------------------------------------------------------

/// What a join's `Joined` reply carries: the admin's entry answer as one postcard frame
/// (`JoinNode`, op `0x1D`). Its topology projection is the typed [`Topology`], its control state
/// is [`WireControl`], its members and source snapshots carry `WireDigest`s, and its status
/// frames are the gossip `Frame`s they already are. The in-memory [`EntryAnswer`] keeps the JSON
/// values its consumers read; they are converted here and nowhere else.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireEntryAnswer {
    served_by: String,
    topology: crate::topology::Topology,
    members: Vec<rafka_mesh_entity::wire::WireDigest>,
    control: WireControl,
    statuses: Vec<rafka_mesh_transport::membership::Frame>,
    sources: Vec<WireSource>,
}

/// The fabric control state an admin hands a joiner.
#[derive(Serialize, Deserialize, Default)]
pub(crate) struct WireControl {
    fabric: Option<WireFabric>,
    shutdown: Option<FabricShutdown>,
    build: Option<WireBuildFloor>,
}

#[derive(Serialize, Deserialize)]
struct WireBuildFloor {
    build_id: BuildId,
    attempt: u32,
}

#[derive(Serialize, Deserialize)]
struct WireSource {
    mesh: String,
    publisher: rafka_mesh_transport::snapshot::PublisherId,
    topology_version: u64,
    digests: Vec<rafka_mesh_entity::wire::WireDigest>,
    in_flight: Vec<LifecycleOp>,
    departed: Vec<LifecycleOp>,
}

/// The JSON shape the answer's control value has in memory (`admin.rs` writes and reads it).
#[derive(Deserialize)]
struct ControlJson {
    #[serde(default)]
    fabric: Option<FabricRecord>,
    #[serde(default)]
    shutdown: Option<FabricShutdown>,
    #[serde(default)]
    build: Option<WireBuildFloorJson>,
}

#[derive(Deserialize)]
struct WireBuildFloorJson {
    build_id: BuildId,
    attempt: u32,
}

/// `answer` as one postcard frame, or the reason a part of it has no wire shape.
pub fn answer_to_wire(answer: &rafka_mesh_transport::entry::EntryAnswer) -> Result<Vec<u8>, String> {
    let topology: crate::topology::Topology = serde_json::from_value(answer.topology.clone()).map_err(|e| format!("the answer's topology is not a Topology: {e}"))?;
    let control = if answer.control.is_null() {
        WireControl::default()
    } else {
        let c: ControlJson = serde_json::from_value(answer.control.clone()).map_err(|e| format!("the answer's control state does not decode: {e}"))?;
        WireControl {
            fabric: c.fabric.map(|r| WireFabric { fabric_id: r.fabric_id, name: r.name, build_id: r.build_id }),
            shutdown: c.shutdown,
            build: c.build.map(|b| WireBuildFloor { build_id: b.build_id, attempt: b.attempt }),
        }
    };
    let wire = WireEntryAnswer {
        served_by: answer.served_by.clone(),
        topology,
        members: answer.members.iter().map(Into::into).collect(),
        control,
        statuses: answer.statuses.clone(),
        sources: answer
            .sources
            .iter()
            .map(|s| WireSource {
                mesh: s.mesh.clone(),
                publisher: s.publisher.clone(),
                topology_version: s.topology_version,
                digests: s.digests.iter().map(Into::into).collect(),
                in_flight: s.in_flight.clone(),
                departed: s.departed.clone(),
            })
            .collect(),
    };
    rafka_mesh_transport::wire::encode(&wire).map_err(|e| e.to_string())
}

/// The entry answer one postcard frame carries.
pub fn answer_from_wire(bytes: &[u8]) -> Result<rafka_mesh_transport::entry::EntryAnswer, String> {
    let w: WireEntryAnswer = rafka_mesh_transport::wire::decode(bytes).map_err(|e| e.to_string())?;
    let control = serde_json::json!({
        "fabric": w.control.fabric.map(|r| FabricRecord { fabric_id: r.fabric_id, name: r.name, build_id: r.build_id }),
        "shutdown": w.control.shutdown,
        "build": w.control.build.map(|b| serde_json::json!({ "build_id": b.build_id, "attempt": b.attempt })),
    });
    Ok(rafka_mesh_transport::entry::EntryAnswer {
        served_by: w.served_by,
        topology: serde_json::to_value(&w.topology).map_err(|e| e.to_string())?,
        members: w.members.into_iter().map(Into::into).collect(),
        control,
        statuses: w.statuses,
        sources: w
            .sources
            .into_iter()
            .map(|s| rafka_mesh_transport::snapshot::SourceSnapshot {
                mesh: s.mesh,
                publisher: s.publisher,
                topology_version: s.topology_version,
                digests: s.digests.into_iter().map(Into::into).collect(),
                in_flight: s.in_flight,
                departed: s.departed,
            })
            .collect(),
    })
}
