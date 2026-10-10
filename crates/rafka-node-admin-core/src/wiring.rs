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

/// The seams a node-admin executable passes to the admin.
#[derive(Default)]
pub struct Wiring {
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
