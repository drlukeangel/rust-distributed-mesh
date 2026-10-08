//! The R-shape consumer's composition root: a role process composed from RDM's public seams and
//! nothing else. No Rafka business crate, no application semantics: a role is a label (a
//! `NodeKind`), the families it serves are the node base's generic proof families, and the
//! testkit's opaque protocols 0x70 (proof store), 0x71 (resolve probe), 0x72 (declare probe)
//! and, for the roles that originate or accept work, 0x73 (originate) are served as the testkit
//! defines them. No opcode is allocated here.

use anyhow::{anyhow, Result};
use rafka_node_base::{compose, drain_deadline_from_env, launch_for, leave_linger_from_env, Oracles, Role};
use rafka_node_rpc_testkit::{node, originate};
use tracing::Instrument;

/// Whether `role` serves the originate door (0x73): the roles work enters through. A compute node
/// originates opaque calls; a gateway accepts them and routes by the supplied RDM mechanisms. A
/// broker answers deterministic proof acknowledgements only.
pub fn serves_originate(role: Role) -> bool {
    matches!(role.name(), "compute" | "gateway")
}

/// Run `role` to completion on the imported substrate: boot as the launch node-admin handed says,
/// serve, wait for the stop signal, drain, leave. `binary` names the process in telemetry and
/// in its exit line.
pub async fn run(role: Role, binary: &str) -> Result<()> {
    let _telemetry = rafka_mesh_telemetry::init_evidence_telemetry(binary);
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
    let originates = serves_originate(role);
    let running = node::start_with_seams(&launch, |b, seams| {
        let client = seams.client.clone();
        let b = oracles.serve(compose(role, &launch.node_id.to_string(), b, client), &launch, seams.resolver.clone());
        if originates {
            originate::serve(b, seams, &launch)
        } else {
            b
        }
    })
    .instrument(tracing::Span::none())
    .await
    .map_err(|e| {
        boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
        e
    })?;
    let _ = oracles.declare_client.set(running.node_rpc.clone());
    let served = running.server.catalog().entries().count();
    boot.in_scope(|| tracing::info!(served, kind = role.name(), originates, "role process serving on the imported substrate"));
    drop(boot);
    println!("RAFKA_NODE_READY {}", launch.node_id);
    node::wait_for_signal(binary).await;
    let deadline = drain_deadline_from_env();
    let drain = tracing::info_span!("rdm.mesh.node.update.via-drain", node = %launch.name, incarnation_id = %launch.incarnation, deadline_ms = deadline.as_millis() as u64, in_flight_at_deadline = tracing::field::Empty);
    let left = running.drain(deadline).instrument(drain.clone()).await;
    drain.record("in_flight_at_deadline", left);
    drop(drain);
    tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation).in_scope(|| tracing::info!("stopping"));
    running.stop(leave_linger_from_env()).await;
    Ok(())
}

/// The process exit for a role binary: its error, named, and status 3 (the node base's code for a
/// node that failed to come up).
pub fn exit_on_error(binary: &str, result: Result<()>) {
    if let Err(e) = result {
        eprintln!("{binary}: {:#}", anyhow!(e));
        std::process::exit(3);
    }
}
