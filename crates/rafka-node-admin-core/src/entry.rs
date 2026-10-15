//! The node-admin process entry: what every executable that IS a node-admin runs. The shipped
//! `rafka-node-admin` and a consumer's own node-admin executable are this call under their own
//! service name.

use crate::admin::{start_with, AdminConfig};
use crate::wiring::Wiring;
use tracing::Instrument;

/// Run a node-admin to completion: configured from the environment, serving its control API,
/// leaving on a fabric shutdown or a signal. `service` is the telemetry service name and the
/// prefix of its console lines.
pub async fn run(service: &str) {
    run_with(service, |_| Wiring { fabric_hooks: crate::wiring::FabricHooks::no_app_work(), ..Wiring::no_certs() }).await
}

/// [`run`], for an executable that decorates the parts its node-admin is built from: `wiring` is
/// called once with the configuration the environment produced, inside the runtime, before the
/// admin starts.
pub async fn run_with(service: &str, wiring: impl FnOnce(&AdminConfig) -> Wiring) {
    // Heard from the first line: a stop that arrives while the admin boots ends the boot by name.
    listen_for_stop();
    let _telemetry = rdm_transport::init_evidence_telemetry(service);
    let cfg = match AdminConfig::from_env(|k| std::env::var(k).ok()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{service}: refusing to start: {e}");
            std::process::exit(2);
        }
    };
    let wiring = wiring(&cfg);
    let boot = tracing::info_span!(
        "rdm.mesh.node.create.via-deployment",
        node = tracing::field::Empty,
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
            r = start_with(cfg, wiring).instrument(tracing::Span::none()) => r,
            () = stop_requested() => {
                // Stopped before it booted: the boot ends by name and the process exits through its
                // telemetry drain (the guard drops as this returns).
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
                // collector: the boot span closes, then the exporters drain before the exit.
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
        why = running.hydration_failed() => {
            let node = running.digest.lock().unwrap().node.name.to_string();
            tracing::info_span!("rdm.mesh.node.delete.via-hydration-failed", node = %node, reason = %why)
                .in_scope(|| tracing::error!("hydrate_before_ready failed; this node-admin ends"));
            eprintln!("{service}: {node} failed hydrate_before_ready: {why}");
            rdm_transport::flush_before_exit();
            std::process::exit(3);
        }
        // A mesh transport that stopped for good leaves a runtime that can neither be heard nor
        // answer: it ends, and its exit is the death proof the fabric recovers from.
        reason = rdm_transport::membership::until_transport_stopped() => {
            tracing::info_span!("rdm.mesh.node.delete.via-transport-stopped", reason = %reason)
                .in_scope(|| tracing::error!("the mesh transport stopped; this runtime exits"));
            eprintln!("{service}: the mesh transport stopped: {reason}");
            rdm_transport::flush_before_exit();
            rdm_core::runtime::exit_transport_stopped(&reason);
        }
    }
    // A parked admin (stopped, its process alive) that is told to end has nothing to drain and no
    // departure to announce: it closes at once.
    if crate::node_self::parking().is_parked() {
        let span = tracing::info_span!("rdm.mesh.node.delete.via-signal", parked = true);
        running.leave_after_stop().instrument(span).await;
        return;
    }
    // Instrumented, never entered across the await: the span's busy and idle time are real.
    let span = tracing::info_span!("rdm.mesh.node.delete.via-signal");
    running.leave().instrument(span).await;
}

pub use rdm_node::node::{listen_for_stop, stop_requested};
