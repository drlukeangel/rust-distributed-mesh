//! `rshape-node-admin`: the R-shape consumer's node-admin entry point, composed from RDM's public
//! node-admin seam (`AdminConfig::from_env`, `start`). All configuration is the environment.

use rafka_node_admin_core::admin::{start, AdminConfig};

use tracing::Instrument;

#[tokio::main]
async fn main() {
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rshape-node-admin");
    let cfg = match AdminConfig::from_env(|k| std::env::var(k).ok()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("rshape-node-admin: refusing to start: {e}");
            drop(telemetry);
            std::process::exit(2);
        }
    };
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
        match start(cfg).instrument(tracing::Span::none()).await {
            Ok(r) => r,
            Err(e) => {
                boot.in_scope(|| tracing::error!(error = %e, "node-admin failed to come up"));
                eprintln!("rshape-node-admin: {e}");
                drop(telemetry);
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
        // A mesh transport that stopped for good leaves a runtime that can neither be heard nor
        // answer: it ends, and its exit is the death proof the fabric recovers from.
        reason = rafka_mesh_transport::membership::until_transport_stopped() => {
            tracing::info_span!("rdm.mesh.node.delete.via-transport-stopped", reason = %reason)
                .in_scope(|| tracing::error!("the mesh transport stopped; this runtime exits"));
            eprintln!("rshape-node-admin: the mesh transport stopped: {reason}");
            drop(telemetry);
            std::process::exit(4);
        }
    };
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
