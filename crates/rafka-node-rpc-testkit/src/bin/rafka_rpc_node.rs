//! `rafka-rpc-node`: the generic RPC proof node a node-admin deploys.
//! All configuration is the launch environment (`docs/i143/design.md` §3).

use rafka_node_rpc_testkit::launch::Launch;
use rafka_node_rpc_testkit::{declare_probe, node, proof_store, resolve_probe};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-rpc-node");
    let launch = match Launch::from_env(|k| std::env::var(k).ok()) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("rafka-rpc-node: refusing to start: {e}");
            drop(telemetry);
            std::process::exit(2);
        }
    };
    let boot = tracing::info_span!(
        "rdm.mesh.node.create.via-deployment",
        node = %launch.name,
        node_id = %launch.node_id,
        incarnation_id = %launch.incarnation,
        kind = "rpc_node",
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rafka_mesh_telemetry::set_parent(&boot, &tp);
    }
    // The declare oracle calls out through the node's one client, which exists once the node runs.
    let declare_client: Arc<std::sync::OnceLock<rafka_node_rpc_testkit::node_rpc::ProcessNodeRpc>> = Arc::new(std::sync::OnceLock::new());
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
        match node::start_with_seams(&launch, |b, seams| {
            let b = declare_probe::serve(resolve_probe::serve(proof_store::serve(b, store, &launch), seams.resolver.clone(), &launch), declare_client.clone(), &launch);
            rafka_node_rpc_testkit::originate::serve(b, seams, &launch)
        })
            .instrument(tracing::Span::none())
            .await
        {
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
    drop(boot);
    println!("RDM_NODE_READY {}", launch.node_id);
    node::wait_for_signal("rafka-rpc-node").await;
    let deadline = node::drain_deadline_from_env();
    let drain = tracing::info_span!(
        "rdm.mesh.node.update.via-drain",
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
    tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation)
        .in_scope(|| tracing::info!("stopping"));
    running.stop(node::leave_linger_from_env()).await;
}
