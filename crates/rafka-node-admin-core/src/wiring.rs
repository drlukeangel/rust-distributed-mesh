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
pub type Wrap<T> = Box<dyn FnOnce(Arc<T>) -> Arc<T> + Send>;

#[derive(Default)]
pub struct Wiring {
    /// Wraps `fabric.storage` before the accepted-Build pointer and the shutdown control take it.
    pub fabric_storage: Option<Wrap<dyn FabricStorage>>,
    /// Wraps the Build state the control routes, the executor, the deployment pipelines and the
    /// drift check all use.
    pub builds: Option<Wrap<dyn BuildStateAdapter>>,
    /// Wraps the deployment provider the pipelines, the drift check and the Ready gate use.
    pub provider: Option<Wrap<dyn DeploymentProvider>>,
    /// Wraps the lifecycle events a retirement publishes.
    pub lifecycle_events: Option<Wrap<dyn LifecycleEvents>>,
    /// The Rafka-time source the admin stamps every gossip frame with. `None` is the OS clock, the
    /// binding an RDM executable supplies; a product that has adopted a Rafka-time source supplies
    /// that source here.
    pub clock: Option<rafka_mesh_transport::clock::SharedClock>,
    /// Hooks registered, in order, before the lifecycle registry seals.
    pub hooks: Vec<(LifecycleHookSpec, Arc<dyn LifecycleHook>)>,
}

/// `wrap` applied to `part`, or `part` unchanged.
pub(crate) fn apply<T: ?Sized>(wrap: Option<Wrap<T>>, part: Arc<T>) -> Arc<T> {
    match wrap {
        Some(w) => w(part),
        None => part,
    }
}
