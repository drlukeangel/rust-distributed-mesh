//! `testkit-node`: the testkit node a node-admin deploys for the `node` type.
//! All configuration is the launch environment.

use rdm_test::launch::Launch;
use rdm_test::{declare_probe, node, testkit_store, resolve_probe};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // A stop that arrives while the node boots is heard from here on.
    node::listen_for_stop();
    let telemetry = rdm_transport::init_evidence_telemetry("testkit-node");
    let launch = match Launch::from_env(|k| std::env::var(k).ok()) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("testkit-node: refusing to start: {e}");
            drop(telemetry);
            std::process::exit(2);
        }
    };
    #[cfg(feature = "testkit-skew")]
    let skew = launch.data_dir.parent().and_then(|root| rdm_test::skew_os_clock_from_root(root, &launch.location()));
    #[cfg(not(feature = "testkit-skew"))]
    let skew: Option<i64> = None;
    let boot = tracing::info_span!(
        "rdm.mesh.node.create.via-deployment",
        node = %launch.node_id, node_path = %launch.path, node_name = %launch.name,
        node_id = %launch.node_id,
        incarnation_id = %launch.incarnation,
        r#type = "node",
        os_clock_skew_ms = skew.unwrap_or(0),
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rdm_transport::set_parent(&boot, &tp);
    }
    // The declare oracle calls out through the node's one client, which exists once the node runs.
    let declare_client: Arc<std::sync::OnceLock<rdm_test::node_rpc::ProcessNodeRpc>> = Arc::new(std::sync::OnceLock::new());
    let store = match testkit_store::FileTestkitStore::open(&launch.data_dir) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            boot.in_scope(|| tracing::error!(error = %e, "the testkit store refused to open"));
            eprintln!("testkit-node: testkit store: {e}");
            drop(telemetry);
            std::process::exit(3);
        }
    };
    // The node's long-lived tasks (endpoints, gossip) must not hold the boot
    // span open: it closes, and is exported, once booted.
    let running = {
        use tracing::Instrument;
        // A scenario's `pre_node_ready` knob for this node (passing immediately when no knob file exists).
        let pre_node_ready = match launch.data_dir.parent().map(|root| rdm_test::pre_node_ready_probe::KnobPreNodeReady::always(root, &launch.location())) {
            Some(hook) => rdm_test::node_hooks::NodeHooks::new(hook),
            None => rdm_test::node_hooks::NodeHooks::none(),
        }
        .with_on_call_failed(Arc::new(rdm_test::test_app::CallFailed));
        let register = |b, seams: rdm_test::originate::Seams| {
            let b = declare_probe::serve(resolve_probe::serve(testkit_store::serve(b, store, &launch, &seams.name), seams.resolver.clone(), &launch, &seams.name), declare_client.clone(), &launch);
            rdm_test::originate::serve(b, seams, &launch)
        };
        // The testkit's app protocol, registered when the estate's root holds its knob: every admitted
        // connection's streams are echoed, and the originate service's `Stream` dials on it.
        let started = async {
            if let Some(alpn) = node::app_protocol_for(&launch) {
                let (running, acceptor) = node::start_pre_node_ready_with_alpn(&launch, pre_node_ready, alpn, register).await?;
                node::echo_streams(acceptor, launch.node_id.to_string());
                Ok(running)
            } else {
                node::start_pre_node_ready(&launch, pre_node_ready, register).await
            }
        }
        .instrument(tracing::Span::none());
        let started = tokio::select! {
            r = started => r,
            () = node::stop_requested() => {
                // Stopped before it booted: the boot ends by name and the process exits through
                // its telemetry flush (the guard drops as main returns).
                boot.in_scope(|| tracing::info!("stopped by signal before the node booted"));
                drop(boot);
                tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.node_id, node_path = %launch.path, node_name = %launch.name, incarnation_id = %launch.incarnation, booted = false)
                    .in_scope(|| tracing::info!("stopping"));
                return;
            }
        };
        match started {
            Ok(r) => r,
            Err(e) => {
                boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
                eprintln!("testkit-node: {e:#}");
                drop(telemetry);
                std::process::exit(3);
            }
        }
    };
    let _ = declare_client.set(running.node_rpc.clone());
    // The node-row knob: this node reads another node's row from the nodes storage when the estate asks.
    node::watch_node_row_knob(&launch, running.node_row_read());
    // The member cert the admin that accepted this join issued, as the testkit's own span records it (hex).
    tracing::info_span!(
        "rdm.testkit.cert.resolve.via-running-node",
        node = %launch.node_id, node_path = %launch.path, node_name = %launch.name,
        incarnation_id = %launch.incarnation,
        member_cert_len = running.member_cert.len() as u64,
        member_cert = %hex::encode(&running.member_cert),
    )
    .in_scope(|| tracing::info!("the node's member cert"));
    drop(boot);
    println!("RDM_NODE_READY {}", launch.node_id);
    node::wait_for_signal("testkit-node").await;
    tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.node_id, node_path = %launch.path, node_name = %launch.name, incarnation_id = %launch.incarnation, booted = true)
        .in_scope(|| tracing::info!("stopping"));
    running.stop().await;
}
