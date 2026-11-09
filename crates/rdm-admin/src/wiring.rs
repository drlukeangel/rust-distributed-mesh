//! The decoration points of a node-admin executable.
//!
//! `start` builds a node-admin from concrete parts and holds them behind traits: the Fabric's
//! control storage (`FabricStorage`), the Build state (`BuildStateAdapter`), the deployment
//! provider and the lifecycle events a deletion publishes. A [`Wiring`] lets an executable that IS
//! a node-admin (a consumer's own, or a testkit's) wrap each of them once before the admin runs. The
//! product passes [`Wiring::default`]: nothing is wrapped. Application work enters a lifecycle only
//! through the [Fabric and node hooks](../../../guide/architecture/fabric-node-hooks.html)
//! (`pre_node_ready`, `on_node_ready`, `on_call_failed`, `on_mesh_ready`, `on_fabric_ready`, `on_build_started`, `on_build_ended`,
//! `on_node_investigated`, `pre_node_recreate`; build.md §9): the
//! admin's transition pipeline registers no application hook. The core carries no fault code; a
//! decorator that delays, refuses or records lives in the executable that passes it.

use crate::build_state::BuildStateAdapter;
use crate::deployment::pipeline::LifecycleEvents;
use crate::deployment::provider::DeploymentProvider;
use crate::fabric_storage::FabricStorage;
use rdm_core::context::CallContext;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// One decorator: takes the part the admin built and returns the part it will use.
pub(crate) type Wrap<T> = Box<dyn FnOnce(Arc<T>) -> Arc<T> + Send>;

/// Whether an executable that IS a node-admin withholds one of its own leave announcements. The
/// leave asks it before each publish on each channel (`"mesh"` or `"backbone"`); a `true` answer
/// skips that one publish call and nothing else: the frame, the channel and the delivery path are
/// untouched. The product passes none, so every announcement is published.
pub trait LeaveSeam: Send + Sync {
    /// Whether the leave withholds announcement `announcement` of `node` on `channel`.
    fn withholds(&self, node: &str, announcement: u32, channel: &'static str) -> bool;
}

/// Whether an executable that IS a node-admin withholds one of its final mesh topologies or leadership
/// announcements from a channel (R-NS2: "Final mesh topologies and leadership announcements are best-effort
/// dissemination. A stop does not wait for their delivery"): its backbone asks it before each send of
/// an announcement `rdm_gossip::backbone::Withhold` names, on each channel (`"backbone"` or
/// `"mesh"`); a `true` answer skips that one send and nothing else: the frame, the channel and the
/// delivery path are untouched. The product passes none, so every announcement is sent.
pub trait AnnouncementSeam: Send + Sync {
    /// Whether `node` withholds `announcement` from `channel`.
    fn withholds(&self, node: &str, announcement: &'static str, channel: &'static str) -> bool;
}

/// Where a joining node-admin's start waits before it makes its topology read (op `0x1E`, node-start.md
/// step 4): the start asks it once, after the join and the hook and before the read, and goes on when it
/// returns. The read, its frames and the accepting primary it goes to are untouched. The product passes none, so
/// the read is made at once.
#[async_trait::async_trait]
pub trait ReadSeam: Send + Sync {
    /// Returns when the start of `node` may make its topology read of `primary`.
    async fn before_topology_read(&self, node: &str, primary: &str);
}

/// Asked by the `StatusService` before it decides a node's declaration (`DeclareNodeState`): a testkit
/// cut holds one there unanswered, so it stays owed across a primary change (i146 Q39). The product passes
/// none, so a declaration is decided at once.
#[async_trait::async_trait]
pub trait DeclarationSeam: Send + Sync {
    /// Returns when the exact node's declaration may be decided. The optional separate
    /// location supports cuts armed before that node was allocated; identity is always explicit.
    async fn before_declaration(&self, node_id: &rdm_core::NodeId, location: Option<&rdm_core::PathName>, state: rdm_core::status::NodeState);
}

/// Asked by the reconcile loop before each of its passes (the fabric-primary's pick-up of active Builds): a testkit cut holds the drift pass there, so a birth a
/// failed Build left surplus is not removed until the release (i146 Q39). The loop awaits a hold outside
/// every lock and before the pass's turn of the admin's stop gate, inside the pass's
/// `rdm.node_admin.build.update.via-drift-held` span, which names what held it and lasts as long as the
/// hold. The product passes none, so every pass runs at once.
pub trait DriftSeam: Send + Sync {
    /// `Some` when something holds the reconcile pass of `node` (its globally unique NodeId), `None` when nothing does.
    fn before_drift_pass(&self, node: &str) -> Option<DriftHold>;
}

/// Asked by a create's target once its node investigation found the birth at the path unreached, before
/// the create acts on that verdict (`AdminRunner::create`): a testkit cut holds the create there, so what
/// the admin hears before the create acts is the cell's to order. The product passes none, so the create
/// acts at once.
#[async_trait::async_trait]
pub trait TargetSeam: Send + Sync {
    /// Returns when the create at `path` may act on its target's verdict on the unreached birth `node_id`.
    async fn before_target_act(&self, path: &rdm_core::PathName, node_id: &rdm_core::NodeId);
}

/// A held reconcile pass: what holds it, and the wait that ends at its release.
pub struct DriftHold {
    /// What holds the pass, named on its span.
    pub by: String,
    /// Ends when the pass may run.
    pub released: Pin<Box<dyn Future<Output = ()> + Send>>,
}

/// Adds an app's own Node RPC ops to the server a node-admin builds, before it is built
/// (`docs/architecture/node-rpc.md`): the app clones the gate of its
/// [`NodeHooks`](crate::node_hooks::NodeHooks) into each (`ServerBuilder::serve_gated`) so none
/// answers before its hook returned `Ok`.
pub type ServeApp = Box<dyn FnOnce(rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder + Send>;

/// What `on_fabric_ready` is told: the fabric, the accepted Build whose change made it ready, and its
/// observability context (`docs/architecture/fabric-node-hooks.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricReadyCtx {
    /// The fabric.
    pub fabric_id: rdm_core::FabricId,
    /// The accepted Build whose change made the fabric ready.
    pub build_id: String,
    /// The Build's observability context.
    pub context: CallContext,
}

/// What `on_build_started` is told, from the write of the Build's `rdm.build.started` event: the fabric,
/// the Build and its observability context (`docs/architecture/fabric-node-hooks.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildStartedCtx {
    /// The fabric.
    pub fabric_id: rdm_core::FabricId,
    /// The Build.
    pub build_id: String,
    /// The Build's observability context.
    pub context: CallContext,
}

/// The terminal state a Build's ending recorded, as its caller reads it on the Build stream
/// (`docs/architecture/fabric-node-hooks.md`, `docs/architecture/build.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildEndedState {
    /// The Build is `Complete`: every section finished.
    ///
    /// `as_str()` is `complete`.
    Complete,
    /// The Build is `Incomplete`: it ran and not every section finished, each named (Q346).
    ///
    /// `as_str()` is `incomplete`.
    Incomplete {
        /// Each section that ended short of its end.
        sections: Vec<rdm_core::build::UnfinishedSection>,
    },
    /// The Build is `Failed`: it could not start, why and its code (Q346).
    ///
    /// `as_str()` is `failed`.
    Failed {
        /// Why it could not start.
        reason: String,
        /// The standard code captured where it could not start.
        code: rdm_core::outcome::Code,
    },
}

impl BuildEndedState {
    /// The state's name, as the notice's span carries it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Incomplete { .. } => "incomplete",
            Self::Failed { .. } => "failed",
        }
    }
}

/// What `on_build_ended` is told, from the write of the Build's `rdm.build.ended` event: the fabric,
/// the Build, its terminal state and its observability context (`docs/architecture/fabric-node-hooks.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildEndedCtx {
    /// The fabric.
    pub fabric_id: rdm_core::FabricId,
    /// The Build.
    pub build_id: String,
    /// `Complete`, `Incomplete { sections }` or `Failed { reason, code }`.
    pub state: BuildEndedState,
    /// The Build's observability context.
    pub context: CallContext,
}

/// A node investigation's result for its client, as `on_node_investigated` carries it
/// (node-investigation.md, Q420–Q423). node.runtime.inspect's answers are steps inside the
/// investigation, never a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeInvestigatedResult {
    /// `ready`: the birth answered a tickle; it was ready the whole time. A held birth off its mesh
    /// is still commanded back through node.mesh.join for its view (Q87).
    Ready,
    /// `rejoined`: the investigation brought it back, by a command back through node.mesh.join or a
    /// start, answered under its `Send`.
    Rejoined,
    /// `re-created`: written `dead`; node recovery deletes it and creates it again at its path and
    /// name with a new node.id.
    ReCreated,
}

impl NodeInvestigatedResult {
    /// The result's name, as node-investigation.md, the `rdm.node.investigated` event and the
    /// notice's span carry it: `ready`, `rejoined` or `re-created`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Rejoined => "rejoined",
            Self::ReCreated => "re-created",
        }
    }
}

/// What `on_node_investigated` is told (Q312): the investigated birth and its node type, the result
/// its mesh-primary recorded, node.runtime.inspect's answer that led to it, and the investigation's
/// observability context (`docs/architecture/fabric-node-hooks.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInvestigatedCtx {
    /// The fabric.
    pub fabric_id: rdm_core::FabricId,
    /// The birth's mesh.
    pub mesh_id: rdm_core::MeshId,
    /// The birth's node id.
    pub node_id: rdm_core::NodeId,
    /// The birth's incarnation.
    pub incarnation: rdm_core::IncarnationId,
    /// The birth's path.
    pub path: String,
    /// The birth's name.
    pub name: String,
    /// The birth's node type.
    pub r#type: String,
    /// The investigation's result: `ready`, `rejoined` or `re-created`.
    pub result: NodeInvestigatedResult,
    /// node.runtime.inspect's answer that led to the result; `None` for `ready`, which the birth's
    /// answer to a tickle led to.
    pub inspected: Option<crate::deployment::provider::Inspected>,
    /// The investigation's observability context.
    pub context: CallContext,
}

impl NodeInvestigatedCtx {
    /// node.runtime.inspect's answer that led to the result, by name ([`crate::deployment::provider::Inspected::name`]);
    /// empty for `ready`, which carries none.
    pub fn inspected_name(&self) -> &'static str {
        self.inspected.as_ref().map_or("", crate::deployment::provider::Inspected::name)
    }
}

/// What `on_mesh_ready` is told: the fabric and the mesh that is ready
/// (`docs/architecture/fabric-node-hooks.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshReadyCtx {
    /// The fabric.
    pub fabric_id: rdm_core::FabricId,
    /// The mesh's id.
    pub mesh_id: rdm_core::MeshId,
    /// The mesh's name.
    pub mesh: String,
}

/// The owner is about to record a lifecycle command frame; executable decorations may hold it.
#[async_trait::async_trait]
pub trait CommandFrameSeam: Send + Sync {
    /// Before the stopped report is recovered by its mesh-primary.
    async fn before_stopped(&self, operation: &rdm_core::LifecycleOp);
    /// The mesh-primary is about to carry a move of the storage change `change_id` to the node `node`:
    /// a decoration that holds the command answers the release it waits for, and the mesh-primary
    /// carries the command once it resolves, the move not done now.
    fn hold_storage_step(&self, _node: &crate::model::PathName, _change_id: &rdm_core::ChangeId) -> Option<std::pin::Pin<Box<dyn Future<Output = ()> + Send>>> {
        None
    }
    /// The fabric-primary is about to run the storage change `change_id`'s copy and switch.
    async fn before_storage_switch(&self, _change_id: &rdm_core::ChangeId) {}
}

/// `on_fabric_ready`: the notice the fabric-primary calls once the fabric is ready
/// (`docs/architecture/fabric-node-hooks.md`). Fire and forget: it gates nothing.
pub type FabricReadyNotice = Arc<dyn Fn(FabricReadyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// `on_mesh_ready`: the notice a mesh-primary calls once its mesh's ready is on its Mesh
/// record (`docs/architecture/fabric-node-hooks.md`). Fire and forget: it gates nothing.
pub type MeshReadyNotice = Arc<dyn Fn(MeshReadyCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// `on_build_started`: the notice the fabric-primary calls each time it writes a Build's
/// `rdm.build.started` event, at acceptance or when a caller's resubmission runs a failed or
/// incomplete Build again (`docs/architecture/fabric-node-hooks.md`). Fire and forget: it gates nothing.
pub type BuildStartedNotice = Arc<dyn Fn(BuildStartedCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// `on_build_ended`: the notice the fabric-primary calls once a Build's `Complete` or `Failed` is
/// recorded (`docs/architecture/fabric-node-hooks.md`). Fire and forget: it gates nothing.
pub type BuildEndedNotice = Arc<dyn Fn(BuildEndedCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// `on_node_investigated`: the notice a mesh-primary calls once it writes a node investigation's
/// result on a node of its mesh as its `rdm.node.investigated` event
/// (`docs/architecture/fabric-node-hooks.md`). Fire and forget: it gates nothing.
pub type NodeInvestigatedNotice = Arc<dyn Fn(NodeInvestigatedCtx) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// `pre_node_recreate`: the hook the mesh-primary running a node recovery asks once it writes a
/// verdict whose `next` is `recreate`, before node.delete of the target birth and node.create at its
/// path and name (`docs/architecture/fabric-node-hooks.md` "pre_node_recreate", Q416). Told the
/// `NodeInvestigatedCtx` of `on_node_investigated`, the application releases what the lost birth held
/// there and answers `Ok(())`, and the recovery runs on, or `Err(reason)`, its `Failed { reason }`: the
/// recovery ends failed, still owed, and the next recovery tick asks again. One call under the hook's
/// `Send`, whose time limit covers it: no answer within it, or a panic, is `Failed`. A call still
/// running at the time limit is cancelled (its future dropped) and named so on the hook's span: the
/// application's work must tolerate being called again after a cancellation. A view that holds no id
/// for the birth's mesh asks no application: the recovery ends failed, named on the hook's span as its
/// own outcome, and the next recovery tick asks again.
pub type PreNodeRecreate = Arc<dyn Fn(NodeInvestigatedCtx) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

/// The application's notices and hooks a node-admin calls
/// ([fabric and node hooks](../../../guide/architecture/fabric-node-hooks.html),
/// `docs/architecture/fabric-node-hooks.md`).
#[derive(Default, Clone)]
pub struct FabricHooks {
    /// `on_mesh_ready`: optional, because it gates nothing.
    pub on_mesh_ready: Option<MeshReadyNotice>,
    /// `on_fabric_ready`: optional, because it gates nothing.
    pub on_fabric_ready: Option<FabricReadyNotice>,
    /// `on_build_started`: optional, because it gates nothing.
    pub on_build_started: Option<BuildStartedNotice>,
    /// `on_build_ended`: optional, because it gates nothing.
    pub on_build_ended: Option<BuildEndedNotice>,
    /// `on_node_investigated`: optional, because it gates nothing.
    pub on_node_investigated: Option<NodeInvestigatedNotice>,
    /// `pre_node_recreate`: optional; with none, the recovery's re-create is answered `Ok`.
    pub pre_node_recreate: Option<PreNodeRecreate>,
}

impl FabricHooks {
    /// The hooks of an embedding with no application work: no notice and no hook.
    pub fn no_app_work() -> Self {
        Self { on_mesh_ready: None, on_fabric_ready: None, on_build_started: None, on_build_ended: None, on_node_investigated: None, pre_node_recreate: None }
    }
}

/// The seams and application declarations a node-admin executable passes to the admin
/// (`docs/architecture/node-admin-lifecycle.md` §2, `docs/architecture/fabric-node-hooks.md`).
#[derive(Default)]
pub struct Wiring {
    /// The embedded app's own protocols on this admin's endpoint, registered like every node's
    /// (`rdm_transport::connections::Alpn`): RDM owns their accept loop, every inbound connection on
    /// one passes `node.connection.create` before the app takes it from
    /// [`crate::admin::Running::connections`], and a registration naming RDM's own Node RPC or gossip
    /// ALPN refuses the admin's start by name (fabric-certs.md §On a connection).
    pub alpns: Vec<rdm_transport::connections::Alpn>,
    /// The application's notices a node-admin calls.
    pub fabric_hooks: FabricHooks,
    /// Asked by the leave before each announcement publish.
    pub leave_seam: Option<Arc<dyn LeaveSeam>>,
    /// Asked by the backbone before each final mesh topology or leadership announcement it sends.
    pub announcement_seam: Option<Arc<dyn AnnouncementSeam>>,
    /// Asked by a joining admin's start before it makes its topology read.
    pub read_seam: Option<Arc<dyn ReadSeam>>,
    /// Asked by the `StatusService` before it decides a node's declaration.
    pub declaration_seam: Option<Arc<dyn DeclarationSeam>>,
    /// Asked before a command's stopped frame is recorded by its owner.
    pub command_frame_seam: Option<Arc<dyn CommandFrameSeam>>,
    /// Asked by the reconcile loop before each drift pass.
    pub drift_seam: Option<Arc<dyn DriftSeam>>,
    /// Asked by a create before it acts on its target's verdict on an unreached birth.
    pub target_seam: Option<Arc<dyn TargetSeam>>,
    /// Wraps `fabric.storage` before the Fabric identity record and the shutdown control take it.
    pub fabric_storage: Option<Wrap<dyn FabricStorage>>,
    /// Wraps the Build state the control routes, the executor, the deployment pipelines and the
    /// drift check all use.
    pub builds: Option<Wrap<dyn BuildStateAdapter>>,
    /// Wraps the deployment provider the pipelines, the drift check and the Ready gate use.
    pub provider: Option<Wrap<dyn DeploymentProvider>>,
    /// Wraps the lifecycle events a deletion publishes.
    pub lifecycle_events: Option<Wrap<dyn LifecycleEvents>>,
    /// The app's `pre_node_ready` hook, run after the join is accepted, the topology installed
    /// and the mesh channel joined. The product passes none: the hook passes at once and the admin's
    /// Ready waits on nothing of the app's.
    pub node_hooks: crate::node_hooks::NodeHooks,
    /// The app's own ops, added to the server before it is built.
    pub serve_app: Option<ServeApp>,
    /// The app's declared secrets (Q150), registered at boot: each per-process
    /// variable by name, with the node types it is set on. The bootstrap admin reads each value once
    /// from its own environment; every node-admin RDM launches is written them beside the root; a value
    /// is set in the environment of the named kinds' processes only
    /// ([`crate::deployment::declared`]). A name RDM sets or passes through refuses the start by name.
    pub declared: Vec<rdm_core::launch::DeclaredSecret>,
    /// The app's listeners (Q151), registered at boot: each type's listeners by
    /// name. A create assigns each the birth's address with port 0 (`RDM_LISTENERS`); the node binds
    /// them and reports the ports at its join, which refuses a report that does not carry exactly them;
    /// they are published on the node's row. A duplicate name in a kind, a name RDM owns and a
    /// node-admin's listeners refuse the start by name ([`crate::deployment::endpoint::ListenerNames`]).
    pub listeners: Vec<rdm_core::launch::DeclaredListeners>,
    /// The cert material: the fabric's root ([`CertChoice::Ca`](crate::certs::CertChoice::Ca)) or no certs by name
    /// ([`CertChoice::NoCerts`](crate::certs::CertChoice::NoCerts)). There is no default: a
    /// node-admin started with [`CertChoice::Unchosen`](crate::certs::CertChoice::Unchosen) is refused at start by name.
    pub certs: crate::certs::CertChoice,
}

impl Wiring {
    /// Nothing wrapped and no app work, with no certs by explicit choice: an in-process node-admin that
    /// issues nothing.
    pub fn no_certs() -> Self {
        Self { certs: crate::certs::CertChoice::NoCerts, ..Self::default() }
    }
}

/// `wrap` applied to `part`, or `part` unchanged.
pub(crate) fn apply<T: ?Sized>(wrap: Option<Wrap<T>>, part: Arc<T>) -> Arc<T> {
    match wrap {
        Some(w) => w(part),
        None => part,
    }
}
