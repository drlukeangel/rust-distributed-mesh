use anyhow::Result;
use std::net::SocketAddrV4;

/// The rafka cross-mesh REPEATER binary: a standalone, trust-agnostic relay that
/// bridges two (or more) mesh gossip swarms. It subscribes to each mesh's topic and
/// re-broadcasts ORIGIN digests verbatim onto the others. It NEVER validates certs —
/// the receiving node does that against the shared-root CA. See
/// `rafka_node_base::run_repeater` for the full doc + loop-prevention rules.
///
/// Env:
///   RAFKA_REPEATER_MESHES  comma-separated mesh ids to bridge (e.g. "mesh1,mesh2")
///   RAFKA_NODE_BIND_ADDR   iroh endpoint bind (default 127.0.0.1:14720)
#[tokio::main]
async fn main() -> Result<()> {
    let meshes: Vec<String> = std::env::var("RAFKA_REPEATER_MESHES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let bind: SocketAddrV4 = std::env::var("RAFKA_NODE_BIND_ADDR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| "127.0.0.1:14720".parse().expect("default bind"));

    // Telemetry: spans (rafka.repeater.relay) land in Jaeger under service "repeater".
    std::env::set_var("OTEL_SERVICE_NAME", "repeater");
    let _guard = rafka_telemetry::init_telemetry("repeater");

    rafka_node_base::run_repeater(meshes, bind).await
}
