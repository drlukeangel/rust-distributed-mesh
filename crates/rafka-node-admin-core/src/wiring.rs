//! The decoration points of a node-admin executable.
//!
//! `start` builds a node-admin from concrete parts and holds them behind traits: the Fabric's
//! control storage (`FabricStorage`), the Build state (`BuildStateAdapter`), the deployment
//! provider, the lifecycle events a retirement publishes, and the lifecycle hook registry. A
//! [`Wiring`] lets an executable that IS a node-admin (a consumer's own, or a testkit's) wrap each
//! of them once, and register hooks before the registry seals, before the admin runs. The product
//! passes [`Wiring::default`]: nothing is wrapped and no hook is registered. The core carries no
//! fault code; a decorator that delays, refuses or records lives in the executable that passes it.

use crate::build_state::BuildStateAdapter;
use crate::deployment::pipeline::LifecycleEvents;
use crate::deployment::provider::DeploymentProvider;
use crate::fabric_storage::FabricStorage;
use crate::lifecycle::{LifecycleHook, LifecycleHookSpec};
use rafka_node_rpc_contract::context::CallContext;
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

/// Whether an executable that IS a node-admin withholds the catch-up it sends a neighbour that came
/// up on the Build topic (the shutdown in force, the Fabric record and the active Builds' facts).
/// The NeighborUp task asks it once per neighbour, before it sends anything; a `true` answer skips
/// that whole catch-up and nothing else: the frames, the topic and the delivery path are untouched.
/// The product passes none, so every neighbour is caught up.
pub trait CatchUpSeam: Send + Sync {
    /// Whether the catch-up of `node` to `neighbour` is withheld.
    fn withholds(&self, node: &str, neighbour: &str) -> bool;
}

/// Adds an app's own Node RPC ops to the server a node-admin seals, before it seals: the app clones
/// the gate of its [`Hydration`](crate::app_hydration::Hydration) into each
/// (`ServerBuilder::serve_gated`) so none answers before its hook returned `Ok`.
pub type ServeApp = Box<dyn FnOnce(rafka_node_rpc::ServerBuilder) -> rafka_node_rpc::ServerBuilder + Send>;

/// The round fields of the application hand-off: `fabric_id`, `build_id`, `attempt`,
/// `operation = state-sync:<fabric_id>` and the observability context (fabric-state-sync.md).
/// `SyncState` carries them to the application and `StateSynced` carries them back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSyncRound {
    /// The fabric.
    pub fabric_id: rafka_mesh_entity::FabricId,
    /// The accepted Build the round belongs to.
    pub build_id: String,
    /// The Build attempt that holds the round.
    pub attempt: u32,
    /// `state-sync:<fabric_id>`.
    pub operation: String,
    /// The attempt's observability context.
    pub context: CallContext,
}

/// The `sync_state` choice. A configuration with none is refused at start by name: an application
/// that forgets to register must not get a fabric that skips its day 0 and seed.
#[derive(Clone)]
pub enum SyncState {
    /// The embedding owes no application work in state-sync. RDM's own executables and test
    /// estates choose this on purpose; state-sync completes at once, naming `no-app-work`.
    NoAppWork,
    /// The application's async function: called by the fabric-primary with the `SyncState` round
    /// fields; the answer is its `StateSynced`.
    Hook(Arc<dyn Fn(StateSyncRound) -> Pin<Box<dyn Future<Output = StateSyncRound> + Send>> + Send + Sync>),
}

impl SyncState {
    /// The application's `sync_state` function.
    pub fn hook<F, Fut>(f: F) -> Self
    where
        F: Fn(StateSyncRound) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = StateSyncRound> + Send + 'static,
    {
        Self::Hook(Arc::new(move |round| Box::pin(f(round))))
    }
}

/// The one notice the application gets after the open-traffic round completed and the fabric is
/// ready-for-traffic. It gates nothing.
pub type TrafficOpenedNotice = Arc<dyn Fn(StateSyncRound) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// The application hooks the fabric-primary calls ([fabric and node hooks](../../../docs/architecture/fabric-node-hooks.md)).
#[derive(Default, Clone)]
pub struct FabricHooks {
    /// `sync_state`. `None` is refused at start.
    pub sync_state: Option<SyncState>,
    /// `traffic_opened`: optional, because it gates nothing. Its round fields name the
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
    /// The application hooks the fabric-primary calls. `sync_state` must be chosen.
    pub fabric_hooks: FabricHooks,
    /// The action a node completes before it checks in to a commit-state or open-traffic round.
    /// `None` binds [`crate::fabric_rounds::NoScratchpad`], which publishes nothing and says so.
    pub round_actions: Option<Arc<dyn crate::fabric_rounds::RoundActions>>,
    /// Asked by the Build topic's NeighborUp catch-up before it sends.
    pub catch_up_seam: Option<Arc<dyn CatchUpSeam>>,
    /// Asked by the leave before each announcement publish.
    pub leave_seam: Option<Arc<dyn LeaveSeam>>,
    /// Wraps `fabric.storage` before the accepted-Build pointer and the shutdown control take it.
    pub fabric_storage: Option<Wrap<dyn FabricStorage>>,
    /// Wraps the Build state the control routes, the executor, the deployment pipelines and the
    /// drift check all use.
    pub builds: Option<Wrap<dyn BuildStateAdapter>>,
    /// Wraps the deployment provider the pipelines, the drift check and the Ready gate use.
    pub provider: Option<Wrap<dyn DeploymentProvider>>,
    /// Wraps the lifecycle events a retirement publishes.
    pub lifecycle_events: Option<Wrap<dyn LifecycleEvents>>,
    /// The app's `hydrate_before_ready` hook, run after the join is accepted, the topology installed
    /// and the mesh channel joined. The product passes none: nothing is hydrated and the admin's
    /// Ready waits on nothing of the app's.
    pub hydration: crate::app_hydration::Hydration,
    /// The app's own ops, added to the server before it seals.
    pub serve_app: Option<ServeApp>,
    /// Hooks registered, in order, before the lifecycle registry seals.
    pub hooks: Vec<(LifecycleHookSpec, Arc<dyn LifecycleHook>)>,
    /// The app's cert signer, or the explicit choice of no certs. There is no default: a node-admin
    /// started with [`CertChoice::Unchosen`] is refused at start by name.
    pub certs: crate::certs::CertChoice,
}

impl Wiring {
    /// Nothing wrapped and no hook, with no certs by explicit choice: what RDM's own executables
    /// pass.
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
