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
    run_with(service, |_| Wiring::default()).await
}

/// [`run`], for an executable that decorates the parts its node-admin is built from: `wiring` is
/// called once with the configuration the environment produced, inside the runtime, before the
/// admin starts.
pub async fn run_with(service: &str, wiring: impl FnOnce(&AdminConfig) -> Wiring) {
    let _telemetry = rafka_mesh_telemetry::init_evidence_telemetry(service);
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
        rafka_mesh_telemetry::set_parent(&boot, &tp);
    }
    if let Some(l) = &cfg.launch {
        boot.record("node", l.name.to_string().as_str());
        boot.record("incarnation_id", l.incarnation.0.as_str());
    }
    // The runtime's long-lived tasks (endpoint, gossip, control API) must not
    // hold the boot span open: it closes, and is exported, once booted.
    let running = {
        use tracing::Instrument;
        match start_with(cfg, wiring).instrument(tracing::Span::none()).await {
            Ok(r) => r,
            Err(e) => {
                boot.in_scope(|| tracing::error!(error = %e, "node-admin failed to come up"));
                eprintln!("{service}: {e}");
                std::process::exit(3);
            }
        }
    };
    drop(boot);
    println!("RDM_NODE_ADMIN_API_BASE={}", running.api_base);

    // Leave when this admin's part of a fabric shutdown is done (the fabric-primary, after it has
    // stopped the mesh-primary spine; fabric-mesh-lifecycle.md §11.1), or on a signal. A signal is a
    // local stop: this admin leaves and nothing Fabric-wide follows.
    let shutdown = running.control.shutdown.clone();
    let by_route = shutdown.notified();
    tokio::pin!(by_route);
    let fabric_shutdown = tokio::select! {
        _ = &mut by_route => true,
        _ = signal() => false,
        // A `stop-node` this admin admitted and answered with its `node-left`: it leaves with no
        // drain leg.
        () = crate::node_self::stop_command().wait() => {
            let span = tracing::info_span!("rdm.mesh.node.delete.via-signal", fabric_shutdown = false, commanded = true);
            running.leave_after_stop().instrument(span).await;
            return;
        }
        // A mesh transport that stopped for good leaves a runtime that can neither be heard nor
        // answer: it ends, and its exit is the death proof the fabric recovers from.
        reason = rafka_mesh_transport::membership::until_transport_stopped() => {
            tracing::info_span!("rdm.mesh.node.delete.via-transport-stopped", reason = %reason)
                .in_scope(|| tracing::error!("the mesh transport stopped; this runtime exits"));
            eprintln!("{service}: the mesh transport stopped: {reason}");
            rafka_mesh_telemetry::flush_before_exit();
            rafka_mesh_entity::runtime::exit_transport_stopped(&reason);
        }
    };
    // A `stop-node` was admitted and the provider's stop signal arrived before its `node-left` was
    // sent: the stop is still the commanded one, completed first, with no drain leg.
    if crate::node_self::stop_command().commanded() {
        crate::node_self::stop_command().wait().await;
        let span = tracing::info_span!("rdm.mesh.node.delete.via-signal", fabric_shutdown = false, commanded = true);
        running.leave_after_stop().instrument(span).await;
        return;
    }
    // Instrumented, never entered across the await: the span's busy and idle time are real.
    let span = tracing::info_span!("rdm.mesh.node.delete.via-signal", fabric_shutdown);
    running.leave().instrument(span).await;
}

async fn signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
