//! The node runtime (`rdm-node`): what an application's role process runs to join its mesh,
//! run its `pre_node_ready` hook and serve its Node RPC ops (`fabric-node-hooks.md`, `node-rpc.md`).
//!
//! # Where it sits
//!
//! Above `rdm-core` and `rdm-transport`; `rdm-admin` builds node-admin on it. An application's own
//! process kinds (its broker, gateway or compute executable) are a `main` that calls [`run_with`]:
//! node-admin launches the process with its launch in the environment ([`read_launch`]), and RDM
//! joins it to its mesh, runs the application's `pre_node_ready` hook ([`node_hooks`]) before Ready
//! and serves the application's own ops beside the core ones.
//!
//! # Example
//!
//! ```no_run
//! use rdm_node::node_hooks::{HookOutcome, NodeHooks, NodeReadyCtx, PreNodeReady};
//! use rdm_node::{read_launch, Role};
//! use std::sync::Arc;
//!
//! /// The application's work before its node reports Ready.
//! struct Warm;
//!
//! #[async_trait::async_trait]
//! impl PreNodeReady for Warm {
//!     async fn pre_node_ready(&self, _ctx: &NodeReadyCtx) -> HookOutcome {
//!         HookOutcome::Ok
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let launch = read_launch()?;
//!     let hooks = NodeHooks::new(Arc::new(Warm));
//!     // `serve_app` adds the application's own ops to the node's server before it is built.
//!     rdm_node::run_with(Role::of(&launch), &[], hooks, |server| server).await
//! }
//! ```
//!
//! # Guide
//!
//! [Building an application on RDM](../../guide/eng/consumer-guide.html),
//! [Your own ops on Node RPC](../../guide/eng/consumer-guide-ops.html) and
//! [Fabric and node hooks](../../guide/architecture/fabric-node-hooks.html), in the docs site
//! `scripts/docs-site.sh` builds.
#![deny(missing_docs)]

/// A node's launch configuration read from its environment (`deployment-provider.md` §2.6).
pub use rdm_core::launch::Launch;
/// Core certificate, DTO, model, canonical-name and status contracts.
pub use rdm_core::{certs, dto, model, names, status, NodeKind};
/// The Node RPC client and server builder (`node-rpc.md`).
pub use rdm_transport::{NodeRpcClient, ServerBuilder};
/// The running node handle, its seams and its process signal helpers.
pub use crate::node::{
    listen_for_stop,
    stop_requested, wait_for_signal, Node, NodeSeams, RunningNode,
};

pub mod node_hooks;
pub mod connection_reconnect;
pub mod connections_writer;
pub mod held_name;
pub mod join;
pub mod leadership;
pub mod member_handover;
pub mod node;
pub mod node_name;
pub mod node_rpc;
pub mod node_self;
pub mod nodes_storage;
pub mod operation;
pub mod rafka_time;
pub mod blocking;
pub mod durable_file;
#[cfg(feature = "file-backend")]
pub mod record_store;
pub mod owed;
pub mod storage;
pub mod storage_backend;
pub mod storage_move;
#[cfg(feature = "file-backend")]
pub mod storage_copy;
pub mod storage_error;
pub mod storage_handle;
#[cfg(feature = "file-backend")]
pub mod store_log;
#[cfg(feature = "file-backend")]
pub mod store_root;
pub mod taskdump;
pub mod topology_read;
pub mod wire;
pub mod workflows;

use anyhow::{anyhow, Result};

/// The role this process is: its kind decides its path.name segment and which product families
/// it serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Role {
    /// The node kind this process runs as.
    pub kind: NodeKind,
}

impl Role {
    /// The role `launch` names: the kind of its path.name, the node_type node-admin launched it as.
    pub fn of(launch: &Launch) -> Self {
        Self { kind: launch.name.kind }
    }

    /// The role's name, the kind's name.
    pub fn name(self) -> &'static str {
        self.kind.name()
    }
}

/// A process's launch, as node-admin handed it (`Launch::from_env`); a launch that is absent or
/// malformed is refused by name.
pub fn read_launch() -> Result<Launch> {
    Launch::from_env(|k| std::env::var(k).ok()).map_err(|e| anyhow!("refusing to start: {e}"))
}

/// [`read_launch`], refused by name when the path's kind is not this role's.
pub fn launch_for(role: Role) -> Result<Launch> {
    let launch = read_launch()?;
    if launch.name.kind != role.kind {
        return Err(anyhow!(
            "refusing to start: launched as {} but this executable is a {}",
            launch.name,
            role.name()
        ));
    }
    Ok(launch)
}

/// The file name of the executable this process runs, as its telemetry service and its boot span
/// name it; refused by name when the process cannot read its own executable path.
pub fn executable_name() -> Result<String> {
    let exe = std::env::current_exe().map_err(|e| {
        anyhow!("refusing to start: this process cannot read its own executable path: {e}")
    })?;
    exe.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or_else(|| anyhow!("refusing to start: the executable path {} names no file", exe.display()))
}

#[cfg(feature = "file-backend")]
/// The one node executable's run: the role is the node_type its launch names ([`Role::of`]), and
/// the process runs it to completion as [`run`] does.
pub async fn run_launched() -> Result<()> {
    run(Role::of(&read_launch()?), &[]).await
}

#[cfg(feature = "file-backend")]
/// Run a role process to completion: boot as `launch` says, serve, wait for the stop signal (or
/// the mesh transport stopping for good), then `node.connections.delete` and leave.
pub async fn run(role: Role, types: &[&str]) -> Result<()> {
    run_with(role, types, crate::node_hooks::NodeHooks::none(), |b| b).await
}

#[cfg(feature = "file-backend")]
/// [`run`] for a product with a hook before Ready (`fabric-node-hooks.md` "pre_node_ready"): `node_hooks` carries its
/// `pre_node_ready` hook, and `serve_app` adds the product's own ops to the catalog before
/// it is built (`node-rpc.md` §9). A hook that fails after the node came up Pending ends the process by name.
///
/// # Examples
///
/// ```no_run
/// use rdm_node::node_hooks::{HookOutcome, NodeHooks, NodeReadyCtx, PreNodeReady};
/// use rdm_node::{read_launch, Role};
/// use std::sync::Arc;
///
/// struct Ready;
///
/// #[async_trait::async_trait]
/// impl PreNodeReady for Ready {
///     async fn pre_node_ready(&self, _ctx: &NodeReadyCtx) -> HookOutcome {
///         HookOutcome::Ok
///     }
/// }
///
/// async fn run_role() -> anyhow::Result<()> {
///     let launch = read_launch()?;
///     rdm_node::run_with(Role::of(&launch), &[], NodeHooks::new(Arc::new(Ready)), |b| b).await
/// }
/// ```
pub async fn run_with(
    role: Role,
    types: &[&str],
    node_hooks: crate::node_hooks::NodeHooks,
    serve_app: impl FnOnce(ServerBuilder) -> ServerBuilder,
) -> Result<()> {
    run_with_alpns(role, types, &[], node_hooks, serve_app).await
}

#[cfg(feature = "file-backend")]
/// [`run_with`], also registering the application's data-plane `alpns` on the process's one endpoint.
pub async fn run_with_alpns(
    role: Role,
    types: &[&str],
    alpns: &[rdm_transport::connections::Alpn],
    node_hooks: crate::node_hooks::NodeHooks,
    serve_app: impl FnOnce(ServerBuilder) -> ServerBuilder,
) -> Result<()> {
    run_on(role, crate::storage_backend::NodeStorageProviders::file(), types, alpns, node_hooks, serve_app).await
}

/// [`run_with_alpns`] on the topology storage `providers` its executable registers (storage.md §2): the
/// node's connections rows are the ones the provider its launch names opens.
pub async fn run_on(
    role: Role,
    providers: crate::storage_backend::NodeStorageProviders,
    types: &[&str],
    alpns: &[rdm_transport::connections::Alpn],
    node_hooks: crate::node_hooks::NodeHooks,
    serve_app: impl FnOnce(ServerBuilder) -> ServerBuilder,
) -> Result<()> {
    crate::node::listen_for_stop();
    let executable = executable_name()?;
    let _telemetry = rdm_transport::init_evidence_telemetry(&executable);
    let launch = launch_for(role)?;
    let boot = tracing::info_span!(
        "rdm.mesh.node.create.via-deployment",
        node = %launch.name,
        node_id = %launch.node_id,
        incarnation_id = %launch.incarnation,
        kind = role.name(),
        executable = %executable,
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rdm_transport::set_parent(&boot, &tp);
    }
    let running = {
        use tracing::Instrument;
        let started = crate::node::start_on(&launch, providers, rdm_transport::clock::RafkaTime::unadopted(), node_hooks, |s| s, None, |b, _seams| serve_app(b), types, alpns)
            .instrument(tracing::Span::none());
        let started = tokio::select! {
            r = started => r,
            () = crate::node::stop_requested() => {
                boot.in_scope(|| tracing::info!("stopped by signal before the node booted"));
                drop(boot);
                tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation, booted = false).in_scope(|| tracing::info!("stopping"));
                return Ok(());
            }
        };
        started.map_err(|e| {
            boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
            e
        })?
    };
    let served = running.server.catalog().entries().count();
    boot.in_scope(|| tracing::info!(served, kind = role.name(), "role process serving on the imported substrate"));
    drop(boot);
    println!("RDM_NODE_READY {}", launch.node_id);
    let ended = tokio::select! {
        () = crate::node::wait_for_signal(&executable) => None,
        why = running.pre_node_ready_failed() => Some(why),
    };
    if let Some(why) = ended {
        tracing::info_span!("rdm.mesh.node.delete.via-pre-node-ready-failed", node = %launch.name, reason = %why).in_scope(|| tracing::error!("pre_node_ready failed; this node ends"));
        running.stop().await;
        return Err(anyhow!("{} failed pre_node_ready: {why}", launch.name));
    }
    tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation, booted = true).in_scope(|| tracing::info!("stopping"));
    running.stop().await;
    Ok(())
}

/// Declared app channels and events.
pub use rdm_transport::gossip;
/// Data-plane connections over registered application ALPNs (`i148`).
pub use rdm_transport::connections;

