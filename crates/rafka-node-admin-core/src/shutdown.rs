//! Fabric shutdown (fabric-mesh-lifecycle.md §11.1, FML-21..27).
//!
//! Only the fabric-primary accepts `/api/shutdown`. It persists [`FabricShutdown`] through
//! `fabric.storage` and disseminates it on the fabric control channel. Every admin that learns it
//! persists its own copy, stops reconciling (no Build attempt, no drift recovery, no rebirth) and
//! gossips `Draining`. The record holds no drain progress: every admin derives the freeze barrier
//! (every current live admin birth in its view is `Draining`) from its own view.
//!
//! Once the barrier holds, each mesh-primary drains its own Mesh: ordinary members, then its
//! non-primary admins, each stopped through its RuntimeFact whoever launched it. The fabric-primary
//! drains its own Mesh the same way, waits until every Mesh holds no live runtime but its
//! mesh-primary (gone from the view, not merely `Leaving`), stops the other mesh-primaries, and
//! stops itself last. A runtime it cannot stop or that stays live past the bound is named
//! (`rafka.node_admin.fabric.update.via-shutdown-incomplete`, and the progress `/api/fabric`
//! shows); it is never inferred stopped.

use crate::fabric_storage::{FabricShutdown, FabricStorage, FabricStorageError};
use crate::model::{Node, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use serde::Serialize;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::watch;

/// Where a shutdown stands, as this admin sees it: diagnostics, never authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ShutdownPhase {
    /// Reconciliation is frozen; the barrier does not hold yet.
    Frozen,
    /// Every live admin is `Draining`: Meshes are draining.
    Draining,
    /// The fabric-primary is stopping the mesh-primaries and then itself.
    Spine,
}

/// One runtime a stopper could not stop, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Incomplete {
    pub node: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ShutdownProgress {
    pub initiated_by: String,
    pub phase: ShutdownPhase,
    pub incomplete: Vec<Incomplete>,
}

/// Broadcasts a shutdown on the fabric control channel.
pub type Publish = Arc<dyn Fn(FabricShutdown) + Send + Sync>;

/// This admin's fabric shutdown state: the held record (persisted through `fabric.storage`), who is
/// waiting on it, and the progress it reports.
pub struct ShutdownControl {
    storage: Arc<dyn FabricStorage>,
    tx: watch::Sender<Option<FabricShutdown>>,
    node: String,
    progress: Mutex<Option<ShutdownProgress>>,
    publish: OnceLock<Publish>,
}

impl ShutdownControl {
    /// Open over `storage`: a shutdown already held there (this admin restarted or joined during
    /// one) is in force from the start.
    pub fn open(storage: Arc<dyn FabricStorage>, node: impl Into<String>) -> Result<Self, FabricStorageError> {
        let held = storage.shutdown()?;
        let progress = held.as_ref().map(|s| ShutdownProgress { initiated_by: s.initiated_by.clone(), phase: ShutdownPhase::Frozen, incomplete: Vec::new() });
        Ok(Self { storage, tx: watch::channel(held).0, node: node.into(), progress: Mutex::new(progress), publish: OnceLock::new() })
    }

    /// Over memory storage, holding no shutdown: for runs that need no restart survival.
    pub fn memory(node: impl Into<String>) -> Arc<Self> {
        Arc::new(Self::open(Arc::new(crate::fabric_storage::MemoryFabricStorage::new()), node).expect("memory storage reads"))
    }

    /// Where this admin broadcasts a shutdown it initiates or hands to a neighbour.
    pub fn set_publish(&self, publish: Publish) {
        let _ = self.publish.set(publish);
    }

    pub fn held(&self) -> Option<FabricShutdown> {
        self.tx.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<FabricShutdown>> {
        self.tx.subscribe()
    }

    pub fn progress(&self) -> Option<ShutdownProgress> {
        self.progress.lock().unwrap().clone()
    }

    fn set_phase(&self, phase: ShutdownPhase) {
        if let Some(p) = self.progress.lock().unwrap().as_mut() {
            p.phase = phase;
        }
    }

    fn incomplete(&self, node: &str, reason: String) {
        tracing::info_span!("rafka.node_admin.fabric.update.via-shutdown-incomplete", observer = %self.node, node, reason = %reason)
            .in_scope(|| tracing::info!("a runtime was not stopped"));
        if let Some(p) = self.progress.lock().unwrap().as_mut() {
            p.incomplete.push(Incomplete { node: node.to_string(), reason });
        }
    }

    /// Hold `shutdown` heard `via` from `from`: persisted through `fabric.storage` before anything
    /// waits on it. Returns whether this admin learned it now (it held none before).
    pub fn learn(&self, shutdown: FabricShutdown, via: &str, from: &str) -> Result<bool, FabricStorageError> {
        if self.held().is_some() {
            return Ok(false);
        }
        let held = self.storage.put_shutdown(&shutdown)?;
        *self.progress.lock().unwrap() =
            Some(ShutdownProgress { initiated_by: held.initiated_by.clone(), phase: ShutdownPhase::Frozen, incomplete: Vec::new() });
        tracing::info_span!(
            "rafka.node_admin.fabric.update.via-shutdown-learned",
            node = %self.node,
            initiated_by = %held.initiated_by,
            via,
            from,
        )
        .in_scope(|| tracing::info!("fabric shutdown held; reconciliation freezes"));
        self.tx.send_replace(Some(held));
        Ok(true)
    }

    /// The fabric-primary begins a shutdown: hold it, then broadcast it.
    pub fn initiate(&self, shutdown: FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        self.learn(shutdown, "initiated", "self")?;
        let held = self.held().expect("held after learn");
        if let Some(p) = self.publish.get() {
            p(held.clone());
        }
        Ok(held)
    }
}

/// Every current live admin birth in `view` is `Draining`: the Fabric-wide freeze barrier.
pub fn freeze_barrier(view: &Topology) -> bool {
    let admins: Vec<&Node> = view.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.status.is_live()).collect();
    !admins.is_empty() && admins.iter().all(|n| n.status == NodeStatus::Draining)
}

/// A runtime still present in `view`: live, or `Leaving` (announced but still running).
fn present(n: &Node) -> bool {
    n.status.is_live() || n.status == NodeStatus::Leaving
}

/// What one admin stops, in order, once the barrier holds: as `mesh`'s primary, its ordinary
/// members, then its non-primary admins.
pub fn own_mesh_drain(view: &Topology, me: &PathName) -> (Vec<Node>, Vec<Node>) {
    let mesh = me.mesh.clone();
    let members = view.nodes.iter().filter(|n| n.mesh == mesh && n.kind != NodeKind::NodeAdmin && present(n)).cloned().collect();
    let admins = view.nodes.iter().filter(|n| n.mesh == mesh && n.kind == NodeKind::NodeAdmin && n.name != *me && present(n)).cloned().collect();
    (members, admins)
}

/// Whether every Mesh in `view` holds no runtime but its mesh-primary: what the fabric-primary
/// waits on before stopping the spine. Returns the runtimes still present otherwise.
pub fn drained_to_primaries(view: &Topology) -> Result<Vec<Node>, Vec<Node>> {
    let primaries: Vec<Node> = view.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.is_primary && present(n)).cloned().collect();
    let left: Vec<Node> = view.nodes.iter().filter(|n| present(n) && !primaries.iter().any(|p| p.name == n.name)).cloned().collect();
    if left.is_empty() {
        Ok(primaries)
    } else {
        Err(left)
    }
}

/// How long a stopper waits for the runtimes it drains to disappear.
pub fn drain_bound() -> Duration {
    std::env::var("RAFKA_SHUTDOWN_DRAIN_BOUND_MS").ok().and_then(|v| v.parse().ok()).map(Duration::from_millis).unwrap_or(Duration::from_secs(60))
}

/// The admin-side seams the drain uses.
#[async_trait::async_trait]
pub trait Stopper: Send + Sync {
    /// The current view.
    async fn view(&self) -> Topology;
    /// Stop `node`'s exact runtime through its RuntimeFact; `Err` names why it cannot.
    async fn stop(&self, node: &Node) -> Result<(), String>;
}

/// Run one admin's part of the shutdown once it holds one, until it has nothing left to do.
/// Returns `true` when this admin is the fabric-primary and has stopped the spine (it stops itself
/// next); a mesh-primary and a non-primary admin wait to be stopped by theirs.
pub async fn drain(me: PathName, control: Arc<ShutdownControl>, stopper: Arc<dyn Stopper>) -> bool {
    let tick = Duration::from_millis(200);
    // The barrier, from this admin's own view.
    loop {
        let view = stopper.view().await;
        if freeze_barrier(&view) {
            break;
        }
        tokio::time::sleep(tick).await;
    }
    control.set_phase(ShutdownPhase::Draining);
    let view = stopper.view().await;
    let i_am_mesh_primary = view.nodes.iter().any(|n| n.name == me && n.is_primary);
    if !i_am_mesh_primary {
        return false;
    }
    tracing::info_span!("rafka.node_admin.fabric.update.via-shutdown-drain", node = %me, mesh = %me.mesh)
        .in_scope(|| tracing::info!("freeze barrier holds: draining this mesh"));
    let (members, admins) = own_mesh_drain(&view, &me);
    for group in [members, admins] {
        stop_and_wait(&control, &*stopper, group).await;
    }
    let is_fabric_primary = stopper.view().await.nodes.iter().any(|n| n.name == me && n.is_fabric_primary);
    if !is_fabric_primary {
        return false;
    }
    // The spine: every Mesh drained to its primary, then the other primaries.
    let deadline = tokio::time::Instant::now() + drain_bound();
    let primaries = loop {
        match drained_to_primaries(&stopper.view().await) {
            Ok(p) => break p,
            Err(left) if tokio::time::Instant::now() >= deadline => {
                for n in &left {
                    control.incomplete(&n.name.to_string(), format!("still present ({:?}) when the drain bound passed", n.status));
                }
                break stopper.view().await.nodes.iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.is_primary && present(n)).cloned().collect();
            }
            Err(_) => tokio::time::sleep(tick).await,
        }
    };
    control.set_phase(ShutdownPhase::Spine);
    let others: Vec<Node> = primaries.into_iter().filter(|n| n.name != me).collect();
    stop_and_wait(&control, &*stopper, others).await;
    true
}

async fn stop_and_wait(control: &ShutdownControl, stopper: &dyn Stopper, nodes: Vec<Node>) {
    if nodes.is_empty() {
        return;
    }
    let mut waiting = Vec::new();
    for n in nodes {
        match stopper.stop(&n).await {
            Ok(()) => waiting.push(n),
            Err(reason) => control.incomplete(&n.name.to_string(), reason),
        }
    }
    let deadline = tokio::time::Instant::now() + drain_bound();
    loop {
        let view = stopper.view().await;
        waiting.retain(|w| view.nodes.iter().any(|n| n.name == w.name && n.incarnation_id == w.incarnation_id && present(n)));
        if waiting.is_empty() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            for w in &waiting {
                control.incomplete(&w.name.to_string(), "stopped but still present when the drain bound passed".into());
            }
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
