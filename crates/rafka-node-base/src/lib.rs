//! The product node base (i143.e11): what a product's role process is on the imported Mesh
//! substrate, composed exactly as a downstream product composes it (i141 PRD §3, §16):
//!
//! - one Iroh endpoint per process carrying the mesh, gossip and Node RPC ALPNs;
//! - one sealed effective catalog: RDM's core protocols, the product's own families and its
//!   transitional adapters, each named once (`node-rpc-rdm-ownership.md` §12);
//! - one `NodeRpcServer` on that endpoint, one `LiveNodeResolver` fed by membership, one
//!   `NodeRpcClient`;
//! - the node's lifecycle as every Mesh node has it: born from node-admin's launch, published
//!   with its RuntimeFact, declaring its state to its authority, drained and stopped on signal.
//!
//! The base carries no application knowledge. A role (`broker`, `gateway`, `compute`,
//! `registry`) is a `NodeKind` the product names, the families it serves, and nothing else.
#![deny(missing_docs)]


pub use rafka_mesh_entity::launch::Launch;
pub use rafka_mesh_entity::NodeKind;
pub use rafka_node_rpc::{NodeRpcClient, ServerBuilder};
pub use rafka_node_rpc_testkit::node::{drain_deadline_from_env, leave_linger_from_env, RunningNode};

pub mod families;
pub mod leadership;

use anyhow::{anyhow, Result};
use rafka_node_rpc_contract::catalog::CatalogEntry;
use std::sync::Arc;
use std::time::Duration;

/// The product family every role's own tags are ledgered under.
pub(crate) const PRODUCT: &str = "rdm-roles";

/// One live legacy op a product serves through a transitional adapter until its named cut. The
/// proof product keeps one (the data-frame op), so the adapter seam is exercised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyAdapter {
    /// The legacy op's tag byte.
    pub op: u8,
    /// The op's catalog name.
    pub name: &'static str,
    /// The product the core ledger reserves the op for: a transitional entry seals under the
    /// ledger's owner, never under the composing product's name.
    pub owner: &'static str,
    /// The migration unit that retires the adapter.
    pub migration_unit: &'static str,
}

/// The proof product's transitional adapters: the ledgered Rafka reservations it stands in for.
pub const LEGACY_ADAPTERS: &[LegacyAdapter] = &[LegacyAdapter { op: 0x12, name: "data-frame", owner: "rafka", migration_unit: "i142 U6" }];

/// The role this process is: its kind decides its path.name segment and which product families
/// it serves (`families::for_kind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Role {
    /// The node kind this process runs as.
    pub kind: NodeKind,
}

impl Role {
    /// The broker role: kind `Broker`.
    pub fn broker() -> Self {
        Self { kind: NodeKind::Broker }
    }
    /// The gateway role: kind `Gateway`.
    pub fn gateway() -> Self {
        Self { kind: NodeKind::Gateway }
    }
    /// The compute role: kind `Compute`.
    pub fn compute() -> Self {
        Self { kind: NodeKind::Compute }
    }
    /// A registry is a plain product node on the base: it serves core protocols only.
    pub fn registry() -> Self {
        Self { kind: NodeKind::RpcNode }
    }

    /// The role's name, the kind's name.
    pub fn name(self) -> &'static str {
        self.kind.name()
    }
}

/// A process's launch, as node-admin handed it (`Launch::from_env`), refused by name when the
/// path's kind is not this role's.
pub fn launch_for(role: Role) -> Result<Launch> {
    let launch = Launch::from_env(|k| std::env::var(k).ok()).map_err(|e| anyhow!("refusing to start: {e}"))?;
    if launch.name.kind != role.kind {
        return Err(anyhow!("refusing to start: launched as {} but this executable is a {}", launch.name, role.name()));
    }
    Ok(launch)
}

/// Compose this process's catalog beyond the core: the role's own families and the product's
/// transitional adapters, into the one builder that seals once (i141 PRD §16).
pub fn compose(role: Role, served_by: &str, b: ServerBuilder, client: Arc<rafka_node_rpc::NodeRpcClient>) -> ServerBuilder {
    let b = families::for_kind(role.kind, served_by, b, client);
    LEGACY_ADAPTERS.iter().fold(b, |b, a| b.adapter(CatalogEntry::transitional(a.op, a.name, a.owner, a.migration_unit, 64 * 1024)))
}

/// The testkit's oracles the proof estate's probe speaks, served by every role exactly as the
/// generic rpc node serves them: the proof store (the data-dir KV oracle), the resolve probe and
/// the declare probe. Proof families, nothing of a product's domain.
pub struct Oracles {
    /// The data-dir key-value store the proof store protocol serves.
    pub store: Arc<rafka_node_rpc_testkit::proof_store::FileProofStore>,
    /// The declare oracle calls out through the process's one client, set once the node runs.
    pub declare_client: Arc<std::sync::OnceLock<rafka_node_rpc_testkit::node_rpc::ProcessNodeRpc>>,
}

impl Oracles {
    /// Open the oracles under `launch`'s data directory; refused by name when the proof store
    /// cannot be opened.
    pub fn open(launch: &Launch) -> Result<Self> {
        let store = rafka_node_rpc_testkit::proof_store::FileProofStore::open(&launch.data_dir).map_err(|e| anyhow!("the proof store refused to open: {e}"))?;
        Ok(Self { store: Arc::new(store), declare_client: Arc::new(std::sync::OnceLock::new()) })
    }

    /// Add the proof store, resolve probe and declare probe protocols to `b`.
    pub fn serve(&self, b: ServerBuilder, launch: &Launch, resolver: Arc<rafka_node_rpc::LiveNodeResolver>) -> ServerBuilder {
        use rafka_node_rpc_testkit::{declare_probe, proof_store, resolve_probe};
        declare_probe::serve(resolve_probe::serve(proof_store::serve(b, self.store.clone(), launch), resolver, launch), self.declare_client.clone(), launch)
    }
}

/// Run a role process to completion: boot as `launch` says, serve, wait for the stop signal (or
/// the mesh transport stopping for good), drain, leave. Every role binary is this call.
pub async fn run(role: Role) -> Result<()> {
    run_with(role, rafka_node_rpc_testkit::app_hydration::Hydration::none(), |b| b).await
}

/// [`run`] for a product that hydrates before Ready: `hydration` carries its
/// `hydrate_before_ready` hook, and `serve_app` adds the product's own ops (each behind the gate of
/// `hydration`, `ServerBuilder::serve_gated`) to the catalog before it seals. A hook that fails
/// after the node came up Pending ends the process by name.
pub async fn run_with(role: Role, hydration: rafka_node_rpc_testkit::app_hydration::Hydration, serve_app: impl FnOnce(ServerBuilder) -> ServerBuilder) -> Result<()> {
    let _telemetry = rafka_mesh_telemetry::init_evidence_telemetry(&format!("rafka-{}", role.name()));
    let launch = launch_for(role)?;
    let boot = tracing::info_span!(
        "rdm.mesh.node.create.via-deployment",
        node = %launch.name,
        node_id = %launch.node_id,
        incarnation_id = %launch.incarnation,
        kind = role.name(),
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rafka_mesh_telemetry::set_parent(&boot, &tp);
    }
    let oracles = Oracles::open(&launch)?;
    let running = {
        use tracing::Instrument;
        rafka_node_rpc_testkit::node::start_hydrating(&launch, hydration, |b, seams| serve_app(oracles.serve(compose(role, &launch.node_id.to_string(), b, seams.client), &launch, seams.resolver)))
            .instrument(tracing::Span::none())
            .await
            .map_err(|e| {
                boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
                e
            })?
    };
    let _ = oracles.declare_client.set(running.node_rpc.clone());
    let served = running.server.catalog().entries().count();
    boot.in_scope(|| tracing::info!(served, kind = role.name(), "role process serving on the imported substrate"));
    drop(boot);
    println!("RDM_NODE_READY {}", launch.node_id);
    let binary = format!("rafka-{}", role.name());
    let ended = tokio::select! {
        () = rafka_node_rpc_testkit::node::wait_for_signal(&binary) => None,
        why = running.hydration_failed() => Some(why),
    };
    if let Some(why) = ended {
        tracing::info_span!("rdm.mesh.node.delete.via-hydration-failed", node = %launch.name, reason = %why).in_scope(|| tracing::error!("hydrate_before_ready failed; this node ends"));
        running.stop(Duration::ZERO).await;
        return Err(anyhow!("{} failed hydrate_before_ready: {why}", launch.name));
    }
    // A `stop-node` has no drain leg; only a signal drains the node itself.
    if !rafka_node_rpc_testkit::node::stop_commanded() {
        let deadline = drain_deadline_from_env();
        let drain = tracing::info_span!("rdm.mesh.node.update.via-drain", node = %launch.name, incarnation_id = %launch.incarnation, deadline_ms = deadline.as_millis() as u64, in_flight_at_deadline = tracing::field::Empty);
        let left = {
            use tracing::Instrument;
            running.drain(deadline).instrument(drain.clone()).await
        };
        drain.record("in_flight_at_deadline", left);
        drop(drain);
    }
    tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation).in_scope(|| tracing::info!("stopping"));
    if rafka_node_rpc_testkit::node::stop_commanded() {
        running.stop_commanded().await;
    } else {
        running.stop(leave_linger_from_env()).await;
    }
    Ok(())
}
