//! `rafka-node-admin`: a fabric member serving the control API. All
//! configuration is the environment (`docs/i143/design.md` §3).

use rafka_node_admin_core::admin::{start, AdminConfig};

#[tokio::main]
async fn main() {
    let _telemetry = rafka_telemetry::init_evidence_telemetry("rafka-node-admin");
    let cfg = match AdminConfig::from_env(|k| std::env::var(k).ok()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("rafka-node-admin: refusing to start: {e}");
            std::process::exit(2);
        }
    };
    let boot = tracing::info_span!(
        "rafka.mesh.node.create.via-deployment",
        node = tracing::field::Empty,
        incarnation_id = tracing::field::Empty,
        kind = "node_admin",
        fabric = %cfg.fabric,
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rafka_telemetry::set_parent(&boot, &tp);
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
                eprintln!("rafka-node-admin: {e}");
                std::process::exit(3);
            }
        }
    };
    drop(boot);
    println!("RAFKA_NODE_ADMIN_API_BASE={}", running.api_base);

    // Stop on the shutdown route (the whole fabric this admin started), a
    // Build's request that this admin leave (its own retirement), or a signal.
    let (shutdown, leave) = (running.control.shutdown.clone(), running.control.leave.clone());
    let by_route = shutdown.notified();
    let by_build = leave.notified();
    tokio::pin!(by_route, by_build);
    let fabric_shutdown = tokio::select! {
        _ = &mut by_route => true,
        _ = &mut by_build => false,
        _ = signal() => false,
    };
    let span = tracing::info_span!("rafka.mesh.node.delete.via-signal", fabric_shutdown);
    let _g = span.enter();
    if fabric_shutdown {
        running.runner.stop_all().await;
    }
    running.leave().await;
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
