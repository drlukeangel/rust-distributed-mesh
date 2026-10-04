//! `rafka-rpc-node`: the generic RPC proof node a node-admin deploys.
//! All configuration is the launch environment (`docs/i143/design.md` §3).

use rafka_node_rpc_testkit::launch::Launch;
use rafka_node_rpc_testkit::node;

#[tokio::main]
async fn main() {
    let _telemetry = rafka_telemetry::init_evidence_telemetry("rafka-rpc-node");
    let launch = match Launch::from_env(|k| std::env::var(k).ok()) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("rafka-rpc-node: refusing to start: {e}");
            std::process::exit(2);
        }
    };
    let boot = tracing::info_span!(
        "rafka.mesh.node.create.via-deployment",
        node = %launch.name,
        node_id = %launch.node_id,
        incarnation_id = %launch.incarnation,
        kind = "rpc_node",
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rafka_telemetry::set_parent(&boot, &tp);
    }
    // The node's long-lived tasks (endpoints, gossip) must not hold the boot
    // span open: it closes, and is exported, once booted.
    let running = {
        use tracing::Instrument;
        match node::start(&launch, |b| b).instrument(tracing::Span::none()).await {
            Ok(r) => r,
            Err(e) => {
                boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
                eprintln!("rafka-rpc-node: {e:#}");
                std::process::exit(3);
            }
        }
    };
    drop(boot);
    println!("RAFKA_NODE_READY {}", launch.node_id);
    wait_for_signal().await;
    let deadline = node::drain_deadline_from_env();
    let drain = tracing::info_span!(
        "rafka.mesh.node.update.via-drain",
        node = %launch.name,
        incarnation_id = %launch.incarnation,
        deadline_ms = deadline.as_millis() as u64,
        in_flight_at_deadline = tracing::field::Empty,
    );
    let left = {
        use tracing::Instrument;
        running.drain(deadline).instrument(drain.clone()).await
    };
    drain.record("in_flight_at_deadline", left);
    drop(drain);
    tracing::info_span!("rafka.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation)
        .in_scope(|| tracing::info!("stopping"));
    running.stop(node::leave_linger_from_env()).await;
}

async fn wait_for_signal() {
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
