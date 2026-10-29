//! The node-admin process entry (`docs/architecture/node-admin-lifecycle.md` §2): what every
//! executable that IS a node-admin runs. The shipped `rdm-node-admin` and a consumer's own
//! node-admin executable are this call under their own service name.

use crate::admin::{start_on, AdminConfig};
use crate::wiring::Wiring;
use tracing::Instrument;

#[cfg(feature = "file-backend")]
/// Run a node-admin to completion (`docs/architecture/node-admin-lifecycle.md` §2): configured from
/// the environment, serving its control API, leaving on a fabric shutdown or a signal. `service` is
/// the telemetry service name and the prefix of its console lines.
pub async fn run(service: &str) {
    run_with(service, |cfg| Wiring { fabric_hooks: crate::wiring::FabricHooks::no_app_work(), certs: material_or_exit(service, cfg), ..Wiring::default() }).await
}

/// The cert material RDM's own node-admin executable holds ([`crate::certs::CertChoice::from_config`],
/// `docs/architecture/fabric-certs.md`); one that holds none, or holds material that does not decode,
/// refuses to start by name.
pub fn material_or_exit(service: &str, cfg: &AdminConfig) -> crate::certs::CertChoice {
    crate::certs::CertChoice::from_config(cfg).unwrap_or_else(|why| {
        tracing::info_span!("rdm.node_admin.cert.reject.via-no-material", node = %cfg.launch.as_ref().map(|l| l.name.to_string()).unwrap_or_else(|| cfg.mesh.clone()), reason = %why)
            .in_scope(|| tracing::error!("this node-admin holds no usable cert material: it refuses to start"));
        eprintln!("{service}: refusing to start: {why}");
        rdm_transport::flush_before_exit();
        std::process::exit(2);
    })
}

#[cfg(feature = "file-backend")]
/// [`run`], for an executable that decorates the parts its node-admin is built from
/// (`docs/architecture/node-admin-lifecycle.md` §2): `wiring` is called once with the configuration
/// the environment produced, inside the runtime, before the admin starts.
pub async fn run_with(service: &str, wiring: impl FnOnce(&AdminConfig) -> Wiring) {
    run_on(service, |cfg| crate::storage_backend::TopologyStorageProviders::file(cfg.store_dir.clone()), wiring).await
}

/// [`run_with`] on the topology storage providers `providers` registers from the configuration
/// (`docs/architecture/storage.md` §2): every durable read and write of the node-admin goes through
/// the boundaries the one its `topology_storage_provider` names opens.
pub async fn run_on(service: &str, providers: impl FnOnce(&AdminConfig) -> crate::storage_backend::TopologyStorageProviders, wiring: impl FnOnce(&AdminConfig) -> Wiring) {
    // Heard from the first line: a stop that arrives while the admin boots ends the boot by name.
    listen_for_stop();
    // The bootstrap's first step (Q208): the configuration, which holds this admin's node id (its
    // launch's, else minted here), read before the telemetry, so every span the process exports carries
    // it. Read on the blocking pool, never a worker of the runtime (Q30).
    let cfg = match crate::blocking::blocking_io(|| AdminConfig::from_env(|k| std::env::var(k).ok())).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{service}: refusing to start: {e}");
            std::process::exit(2);
        }
    };
    let _telemetry = rdm_transport::init_evidence_telemetry_for_instance(service, &cfg.node_id.to_string());
    let wiring = wiring(&cfg);
    let storage = providers(&cfg);
    let boot = tracing::info_span!(
        "rdm.mesh.node.create.via-deployment",
        node = tracing::field::Empty,
        node_id = %cfg.node_id,
        incarnation_id = tracing::field::Empty,
        kind = "node_admin",
        fabric = %cfg.fabric,
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rdm_transport::set_parent(&boot, &tp);
    }
    if let Some(l) = &cfg.launch {
        boot.record("node", l.name.to_string().as_str());
        boot.record("incarnation_id", l.incarnation.as_str());
    }
    // The runtime's long-lived tasks (endpoint, gossip, control API) must not
    // hold the boot span open: it closes, and is exported, once booted.
    let running = {
        use tracing::Instrument;
        let started = tokio::select! {
            r = start_on(cfg, wiring, storage).instrument(tracing::Span::none()) => r,
            () = stop_requested() => {
                // Stopped before it booted: the boot ends by name and the process exits through its
                // telemetry flush (the guard drops as this returns).
                boot.in_scope(|| tracing::info!("stopped by signal before the node-admin booted"));
                drop(boot);
                tracing::info_span!("rdm.mesh.node.delete.via-signal", booted = false).in_scope(|| tracing::info!("stopping"));
                return;
            }
        };
        match started {
            Ok(r) => r,
            Err(e) => {
                boot.in_scope(|| tracing::error!(error = %e, "node-admin failed to come up"));
                eprintln!("{service}: {e}");
                // The refused boot's spans (the reason it exits for) reach the evidence file and the
                // collector: the boot span closes, then the exporters flush before the exit.
                drop(boot);
                rdm_transport::flush_before_exit();
                std::process::exit(3);
            }
        }
    };
    drop(boot);
    running.announce();

    // Leave on a signal. A signal is a local stop: this admin leaves and nothing Fabric-wide follows.
    tokio::select! {
        () = stop_requested() => {}
        // A hook that failed ends the node by name, as an unaccepted join does.
        why = running.pre_node_ready_failed() => {
            let node = running.digest.lock().unwrap().node.name.to_string();
            tracing::info_span!("rdm.mesh.node.delete.via-pre-node-ready-failed", node = %node, reason = %why)
                .in_scope(|| tracing::error!("pre_node_ready failed; this node-admin ends"));
            eprintln!("{service}: {node} failed pre_node_ready: {why}");
            rdm_transport::flush_before_exit();
            std::process::exit(3);
        }
        // A mesh transport that stopped for good leaves a runtime that can neither be heard nor
        // answer: it ends, and its exit is the death proof the fabric recovers from.
        reason = rdm_transport::topology::until_transport_stopped() => {
            tracing::info_span!("rdm.mesh.node.delete.via-transport-stopped", reason = %reason)
                .in_scope(|| tracing::error!("the mesh transport stopped; this runtime exits"));
            eprintln!("{service}: the mesh transport stopped: {reason}");
            rdm_transport::flush_before_exit();
            rdm_core::runtime::exit_transport_stopped(&reason);
        }
    }
    // A birth whose stop was admitted (its process alive, or a start of it under way) runs no
    // second stop on its signal (node-stop.md step 6): it closes at once.
    if running.node_self().stop_admitted() {
        let span = tracing::info_span!("rdm.mesh.node.delete.via-signal", stopped = true);
        running.leave_after_stop().instrument(span).await;
        return;
    }
    // A live admin's signal is node.stop's own steps, this admin its own subject (i146 Q55), then the
    // process end. Instrumented, never entered across the await: the span's busy and idle time are real.
    let span = tracing::info_span!("rdm.mesh.node.delete.via-signal");
    running.leave().instrument(span).await;
}

/// Signal listener and stop notification (`docs/architecture/lifecycles/node-stop.md`).
pub use rdm_node::node::{listen_for_stop, stop_requested};
