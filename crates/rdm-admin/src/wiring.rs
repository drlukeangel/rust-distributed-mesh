//! The decoration points of a node-admin executable.
//!
//! `start` builds a node-admin from concrete parts and holds them behind traits: the Fabric's
//! control storage (`FabricStorage`), the Build state (`BuildStateAdapter`), the deployment
//! provider, the lifecycle events a retirement publishes, and the lifecycle hook registry. A
//! [`Wiring`] lets an executable that IS a node-admin (a consumer's own, or a testkit's) wrap each
//! of them once, and register hooks before the registry is built, before the admin runs. The product
//! passes [`Wiring::default`]: nothing is wrapped and no hook is registered. The core carries no
//! fault code; a decorator that delays, refuses or records lives in the executable that passes it.

use crate::build_state::BuildStateAdapter;
use crate::deployment::pipeline::LifecycleEvents;
use crate::deployment::provider::DeploymentProvider;
use crate::fabric_storage::FabricStorage;
use crate::lifecycle::{LifecycleHook, LifecycleHookSpec};
use rdm_core::context::CallContext;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// One decorator: takes the part the admin built and returns the part it will use.
pub(crate) type Wrap<T> = Box<dyn FnOnce(Arc<T>) -> Arc<T> + Send>;

/// Whether an executable that IS a node-admin withholds one of its own leave announcements. The
/// leave asks it before each publish on each channel (`"mesh"` or `"backbone"`); a `true` answer
/// skips that one publish call and nothing else: the frame, the topic and the delivery path are
/// untouched. The product passes none, so every announcement is published.
pub trait LeaveSeam: Send + Sync {
    /// Whether the leave withholds announcement `announcement` of `node` on `channel`.
    fn withholds(&self, node: &str, announcement: u32, channel: &'static str) -> bool;
}

/// Whether an executable that IS a node-admin withholds one of its final aggregates or leadership
/// announcements from a channel (R-NS2: "Final aggregates and leadership announcements are best-effort
/// dissemination. A stop does not wait for their delivery"): its backbone asks it before each send of
/// an announcement `rdm_transport::membership::Withhold` names, on each channel (`"backbone"` or
/// `"mesh"`); a `true` answer skips that one send and nothing else: the frame, the topic and the
/// delivery path are untouched. The product passes none, so every announcement is sent.
pub trait AnnouncementSeam: Send + Sync {
    /// Whether `node` withholds `announcement` from `channel`.
    fn withholds(&self, node: &str, announcement: &'static str, channel: &'static str) -> bool;
}

/// Whether an executable that IS a node-admin withholds the catch-up it sends a neighbour that came
/// up on the Build topic (the shutdown in force, the Fabric record and the active Builds' facts).
/// The NeighborUp task asks it once per neighbour, before it sends anything; a `true` answer skips
/// that whole catch-up and nothing else: the frames, the topic and the delivery path are untouched.
/// The product passes none, so every neighbour is caught up.
pub trait CatchUpSeam: Send + Sync {
    /// Whether the catch-up of `node` to `neighbour` is withheld.
    fn withholds(&self, node: &str, neighbour: &str) -> bool;
}

/// Where a joining node-admin's start waits before it makes its topology read (op `0x1E`, node-start.md
/// step 4): the start asks it once, after the join and the hook and before the read, and goes on when it
/// returns. The read, its frames and the authority it goes to are untouched. The product passes none, so
/// the read is made at once.
#[async_trait::async_trait]
pub trait ReadSeam: Send + Sync {
    /// Returns when the start of `node` may make its topology read of `authority`.
    async fn before_topology_read(&self, node: &str, authority: &str);
}

/// Asked by the status door before it decides a node's declaration (`DeclareNodeState`): a testkit
/// cut holds one there unanswered, so it stays owed across a seat move (i146 Q39). The product passes
/// none, so a declaration is decided at once.
#[async_trait::async_trait]
pub trait DeclarationSeam: Send + Sync {
    /// Returns when the declaration of `state` by `node` (its `path.name`) may be decided.
    async fn before_declaration(&self, node: &str, state: rdm_core::status::NodeState);
}

/// Asked by the reconcile loop before each of its passes (the drift check, `reconcile_drift`, and the
/// fabric-primary's pick-up of active Builds): a testkit cut holds the drift pass there, so a birth a
/// failed Build left surplus is not removed until the release (i146 Q39). The loop awaits a hold outside
/// every lock and before the pass's turn of the admin's stop gate, inside the pass's
/// `rdm.node_admin.build.update.via-drift-held` span, which names what held it and lasts as long as the
/// hold. The product passes none, so every pass runs at once.
pub trait DriftSeam: Send + Sync {
    /// `Some` when something holds the reconcile pass of `node` (its `path.name`), `None` when nothing does.
    fn before_drift_pass(&self, node: &str) -> Option<DriftHold>;
}

/// A held reconcile pass: what holds it, and the wait that ends at its release.
pub struct DriftHold {
    /// What holds the pass, named on its span.
    pub by: String,
    /// Ends when the pass may run.
    pub released: Pin<Box<dyn Future<Output = ()> + Send>>,
}

/// Adds an app's own Node RPC ops to the server a node-admin builds, before it is built: the app clones
/// the gate of its [`BeforeReadyHook`](crate::before_ready::BeforeReadyHook) into each
/// (`ServerBuilder::serve_gated`) so none answers before its hook returned `Ok`.
pub type ServeApp = Box<dyn FnOnce(rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder + Send>;

/// The state fields of the application hand-off: `fabric_id`, `build_id`, `attempt`,
/// `operation = state-sync:<fabric_id>` and the observability context (fabric-state-sync.md).
/// `SyncState` carries them to the application and `StateSynced` carries them back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSyncHandoff {
    /// The fabric.
    pub fabric_id: rdm_core::FabricId,
    /// The accepted Build the checklist belongs to.
    pub build_id: String,
    /// The Build attempt that holds the checklist.
    pub attempt: u32,
    /// `state-sync:<fabric_id>`.
    pub operation: String,
    /// The attempt's observability context.
    pub context: CallContext,
}

/// The application's report that its `sync_state` work failed, by name (Q158; build.md: hooks
/// "report failure through the lifecycle receipt path"): the Build the fabric changes for fails on
/// its `fabric-change` step, `reason` carried on its `Failed`. Waiting is not failure: an
/// application still working answers nothing, and the Build stays Blocked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSyncFailed {
    /// Why the work failed, named by the application.
    pub reason: String,
}

/// The application's answer to `sync_state`: `Ok` is its `StateSynced` for the call's fields, `Err`
/// its failure by name.
pub type StateSyncAnswer = Result<StateSyncHandoff, StateSyncFailed>;

impl From<StateSyncHandoff> for StateSyncAnswer {
    fn from(synced: StateSyncHandoff) -> Self {
        Ok(synced)
    }
}

/// The `sync_state` choice. A configuration with none is refused at start by name: an application
/// that forgets to register must not get a fabric that skips its day 0 and seed.
#[derive(Clone)]
pub enum SyncState {
    /// The embedding owes no application work in state-sync. RDM's own executables and test
    /// estates choose this on purpose; state-sync completes at once, naming `no-app-work`.
    NoAppWork,
    /// The application's async function: called by the fabric-primary with the `SyncState`
    /// fields; the answer is its `StateSynced`, or its failure by name ([`StateSyncFailed`]).
    Hook(Arc<dyn Fn(StateSyncHandoff) -> Pin<Box<dyn Future<Output = StateSyncAnswer> + Send>> + Send + Sync>),
}

impl SyncState {
    /// The application's `sync_state` function. It answers its `StateSynced` (a [`StateSyncHandoff`])
    /// or a [`StateSyncAnswer`], whose `Err` reports the work failed by name.
    pub fn hook<F, Fut, A>(f: F) -> Self
    where
        F: Fn(StateSyncHandoff) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = A> + Send + 'static,
        A: Into<StateSyncAnswer> + 'static,
    {
        Self::Hook(Arc::new(move |fields| {
            let answer = f(fields);
            Box::pin(async move { answer.await.into() })
        }))
    }
}

/// The owner is about to record a lifecycle command frame; executable decorations may hold it.
#[async_trait::async_trait]
pub trait CommandFrameSeam: Send + Sync {
    /// Before the stopped report is recovered by its mesh-primary.
    async fn before_stopped(&self, operation: &rdm_core::LifecycleOp);
}

/// The one notice the application gets after the open-traffic checklist completed and the fabric is
/// ready-for-traffic. It gates nothing.
pub type TrafficOpenedNotice = Arc<dyn Fn(StateSyncHandoff) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// The application hooks the fabric-primary calls ([fabric and node hooks](../../../docs/architecture/fabric-node-hooks.md)).
#[derive(Default, Clone)]
pub struct FabricHooks {
    /// `sync_state`. `None` is refused at start.
    pub sync_state: Option<SyncState>,
    /// `traffic_opened`: optional, because it gates nothing. Its state fields name the
    /// `open-traffic:<fabric_id>` operation.
    pub traffic_opened: Option<TrafficOpenedNotice>,
}

impl FabricHooks {
    /// The hooks of an embedding with no application work: state-sync completes at once.
    pub fn no_app_work() -> Self {
        Self { sync_state: Some(SyncState::NoAppWork), traffic_opened: None }
    }
}

/// The seams a node-admin executable passes to the admin.
#[derive(Default)]
pub struct Wiring {
    /// The embedded app's own kinds, declared once for this admin birth.
    pub types: Vec<String>,
    /// The embedded app's own protocols on this admin's endpoint, registered like every node's
    /// (`rdm_transport::connections::Alpn`): RDM owns their accept loop, every inbound connection on
    /// one passes `node.connections.admit` before the app takes it from
    /// [`crate::admin::Running::connections`], and a registration naming RDM's own Node RPC or gossip
    /// ALPN refuses the admin's start by name (fabric-certs.md §On a connection).
    pub alpns: Vec<rdm_transport::connections::Alpn>,
    /// The application hooks the fabric-primary calls. `sync_state` must be chosen.
    pub fabric_hooks: FabricHooks,
    /// The embedding's own action a node completes before it checks in to a commit-state or
    /// open-traffic checklist, run after RDM publishes the node's scratchpad to the fabric's store root
    /// at commit-state ([`crate::fabric_status::StorePublication`]). `None`: RDM's publication alone.
    pub state_actions: Option<Arc<dyn crate::fabric_status::StateActions>>,
    /// Asked by the Build topic's NeighborUp catch-up before it sends.
    pub catch_up_seam: Option<Arc<dyn CatchUpSeam>>,
    /// Asked by the leave before each announcement publish.
    pub leave_seam: Option<Arc<dyn LeaveSeam>>,
    /// Asked by the backbone before each final aggregate or leadership announcement it sends.
    pub announcement_seam: Option<Arc<dyn AnnouncementSeam>>,
    /// Asked by a joining admin's start before it makes its topology read.
    pub read_seam: Option<Arc<dyn ReadSeam>>,
    /// Asked by the status door before it decides a node's declaration.
    pub declaration_seam: Option<Arc<dyn DeclarationSeam>>,
    /// Asked before a command's stopped frame is recorded by its owner.
    pub command_frame_seam: Option<Arc<dyn CommandFrameSeam>>,
    /// Asked by the reconcile loop before each drift pass.
    pub drift_seam: Option<Arc<dyn DriftSeam>>,
    /// Wraps `fabric.storage` before the Fabric identity holder and the shutdown control take it.
    pub fabric_storage: Option<Wrap<dyn FabricStorage>>,
    /// Wraps the Build state the control routes, the executor, the deployment pipelines and the
    /// drift check all use.
    pub builds: Option<Wrap<dyn BuildStateAdapter>>,
    /// Wraps the deployment provider the pipelines, the drift check and the Ready gate use.
    pub provider: Option<Wrap<dyn DeploymentProvider>>,
    /// Wraps the lifecycle events a retirement publishes.
    pub lifecycle_events: Option<Wrap<dyn LifecycleEvents>>,
    /// The app's `before_ready` hook, run after the join is accepted, the topology installed
    /// and the mesh channel joined. The product passes none: the hook passes at once and the admin's
    /// Ready waits on nothing of the app's.
    pub before_ready: crate::before_ready::BeforeReadyHook,
    /// The app's own ops, added to the server before it is built.
    pub serve_app: Option<ServeApp>,
    /// Hooks registered, in order, before the lifecycle registry is built.
    pub hooks: Vec<(LifecycleHookSpec, Arc<dyn LifecycleHook>)>,
    /// The app's declared secrets (Q150), registered at boot like the hooks: each per-process
    /// variable by name, with the node kinds it is set on. The bootstrap admin reads each value once
    /// from its own environment; every node-admin RDM launches is written them beside the root; a value
    /// is set in the environment of the named kinds' processes only
    /// ([`crate::deployment::declared`]). A name RDM sets or passes through refuses the start by name.
    pub declared: Vec<rdm_core::launch::DeclaredSecret>,
    /// The app's listeners (Q151), registered at boot like the hooks: each product kind's listeners by
    /// name. A create assigns each the birth's address with port 0 (`RDM_LISTENERS`); the node binds
    /// them and reports the ports at its join, which refuses a report that does not carry exactly them;
    /// they are published on the node's row. A duplicate name in a kind, a name RDM owns and a
    /// node-admin's listeners refuse the start by name ([`crate::deployment::endpoint::ListenerNames`]).
    pub listeners: Vec<rdm_core::launch::DeclaredListeners>,
    /// The cert material: the fabric's root ([`CertChoice::Ca`]) or no certs by name
    /// ([`CertChoice::NoCerts`]). There is no default: a
    /// node-admin started with [`CertChoice::Unchosen`] is refused at start by name.
    pub certs: crate::certs::CertChoice,
}

impl Wiring {
    /// Nothing wrapped and no hook, with no certs by explicit choice: an in-process node-admin that
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
