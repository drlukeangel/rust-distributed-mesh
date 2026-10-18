//! The product node runtime (`rdm-node`): what a product's role process is on the imported Mesh
//! substrate, composed without depending on `rafka-node-admin-core` or `rafka-node-rpc-testkit`.
#![deny(missing_docs)]

pub use rdm_core::launch::Launch;
pub use rdm_core::{certs, dto, model, names, status, NodeKind};
pub use rdm_transport::{NodeRpcClient, ServerBuilder};
pub use crate::node::{
    listen_for_stop, stop_commanded,
    stop_requested, wait_for_signal, NodeSeams, RunningNode,
};

pub mod app_hydration;
pub mod connections_writer;
pub mod held_name;
pub mod join;
pub mod leadership;
pub mod member_handover;
pub mod node;
pub mod node_name;
pub mod node_rpc;
pub mod node_self;
pub mod rafka_time;
pub mod record_store;
pub mod rounds;
pub mod storage;
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

/// The one node executable's run: the role is the node_type its launch names ([`Role::of`]), and
/// the process runs it to completion as [`run`] does.
pub async fn run_launched() -> Result<()> {
    run(Role::of(&read_launch()?), &[]).await
}

/// Run a role process to completion: boot as `launch` says, serve, wait for the stop signal (or
/// the mesh transport stopping for good), then `node.connections.delete` and leave.
pub async fn run(role: Role, types: &[&str]) -> Result<()> {
    run_with(role, types, crate::app_hydration::Hydration::none(), |b| b).await
}

/// [`run`] for a product that hydrates before Ready: `hydration` carries its
/// `hydrate_before_ready` hook, and `serve_app` adds the product's own ops to the catalog before
/// it seals. A hook that fails after the node came up Pending ends the process by name.
pub async fn run_with(
    role: Role,
    types: &[&str],
    hydration: crate::app_hydration::Hydration,
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
        let started = crate::node::start_with_rafka_time_hydrating_and(&launch, rdm_transport::clock::RafkaTime::unadopted(), hydration, |s| s, None, |b, _seams| serve_app(b), types)
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
        why = running.hydration_failed() => Some(why),
    };
    if let Some(why) = ended {
        tracing::info_span!("rdm.mesh.node.delete.via-hydration-failed", node = %launch.name, reason = %why).in_scope(|| tracing::error!("hydrate_before_ready failed; this node ends"));
        running.stop().await;
        return Err(anyhow!("{} failed hydrate_before_ready: {why}", launch.name));
    }
    tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation, booted = true).in_scope(|| tracing::info!("stopping"));
    if crate::node::stop_commanded() {
        running.stop_commanded().await;
    } else {
        running.stop().await;
    }
    Ok(())
}

/// Declared app channels and events.
pub use rdm_transport::gossip;
