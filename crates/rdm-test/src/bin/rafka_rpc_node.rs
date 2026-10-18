//! `rafka-rpc-node`: the generic RPC proof node a node-admin deploys.
//! All configuration is the launch environment.

use rdm_test::launch::Launch;
use rdm_test::{declare_probe, node, proof_store, resolve_probe};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // A stop that arrives while the node boots is heard from here on.
    node::listen_for_stop();
    let telemetry = rdm_transport::init_evidence_telemetry("rafka-rpc-node");
    let launch = match Launch::from_env(|k| std::env::var(k).ok()) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("rafka-rpc-node: refusing to start: {e}");
            drop(telemetry);
            std::process::exit(2);
        }
    };
    #[cfg(feature = "testkit-skew")]
    let skew = launch.data_dir.parent().and_then(|root| rdm_test::skew_os_clock_from_root(root, &launch.name.to_string()));
    #[cfg(not(feature = "testkit-skew"))]
    let skew: Option<i64> = None;
    let boot = tracing::info_span!(
        "rdm.mesh.node.create.via-deployment",
        node = %launch.name,
        node_id = %launch.node_id,
        incarnation_id = %launch.incarnation,
        kind = "rpc_node",
        os_clock_skew_ms = skew.unwrap_or(0),
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rdm_transport::set_parent(&boot, &tp);
    }
    // The declare oracle calls out through the node's one client, which exists once the node runs.
    let declare_client: Arc<std::sync::OnceLock<rdm_test::node_rpc::ProcessNodeRpc>> = Arc::new(std::sync::OnceLock::new());
    let store = match proof_store::FileProofStore::open(&launch.data_dir) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            boot.in_scope(|| tracing::error!(error = %e, "the proof store refused to open"));
            eprintln!("rafka-rpc-node: proof store: {e}");
            drop(telemetry);
            std::process::exit(3);
        }
    };
    // The node's long-lived tasks (endpoints, gossip) must not hold the boot
    // span open: it closes, and is exported, once booted.
    let running = {
        use tracing::Instrument;
        // A scenario's `before_ready` knob for this node (passing immediately when no knob file exists).
        let before_ready = match launch.data_dir.parent().map(|root| rdm_test::before_ready_probe::KnobBeforeReady::always(root, &launch.name.to_string())) {
            Some(hook) => rdm_test::before_ready::BeforeReadyHook::new(hook),
            None => rdm_test::before_ready::BeforeReadyHook::none(),
        };
        let started = node::start_before_ready(&launch, before_ready, |b, seams| {
            let b = declare_probe::serve(resolve_probe::serve(proof_store::serve(b, store, &launch, &seams.name), seams.resolver.clone(), &launch, &seams.name), declare_client.clone(), &launch);
            rdm_test::originate::serve(b, seams, &launch)
        })
        .instrument(tracing::Span::none());
        let started = tokio::select! {
            r = started => r,
            () = node::stop_requested() => {
                // Stopped before it booted: the boot ends by name and the process exits through
                // its telemetry drain (the guard drops as main returns).
                boot.in_scope(|| tracing::info!("stopped by signal before the node booted"));
                drop(boot);
                tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation, booted = false)
                    .in_scope(|| tracing::info!("stopping"));
                return;
            }
        };
        match started {
            Ok(r) => r,
            Err(e) => {
                boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
                eprintln!("rafka-rpc-node: {e:#}");
                drop(telemetry);
                std::process::exit(3);
            }
        }
    };
    let _ = declare_client.set(running.node_rpc.clone());
    // The member cert the admin that accepted this join issued, as the testkit's own span records it.
    tracing::info_span!(
        "rdm.testkit.cert.resolve.via-running-node",
        node = %launch.name,
        incarnation_id = %launch.incarnation,
        member_cert_len = running.member_cert.len() as u64,
        member_cert = %String::from_utf8_lossy(&running.member_cert),
    )
    .in_scope(|| tracing::info!("the node's member cert"));
    drop(boot);
    println!("RDM_NODE_READY {}", launch.node_id);
    node::wait_for_signal("rafka-rpc-node").await;
    tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation, booted = true)
        .in_scope(|| tracing::info!("stopping"));
    if node::stop_commanded() {
        running.stop_commanded().await;
    } else {
        running.stop().await;
    }
}
