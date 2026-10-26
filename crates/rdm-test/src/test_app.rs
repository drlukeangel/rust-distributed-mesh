//! The testkit's application: it takes the fabric's notices the way an embedding does.
//!
//! `on_mesh_ready` and `on_fabric_ready` (Q175) each only record a span. A scenario that holds a fabric
//! short of ready-for-traffic holds a node's `pre_node_ready` (`crate::write_pre_node_ready_knob`), as an
//! application holds its own node.
//!
//! It declares one secret (Q150), [`DECLARED_SECRET`], set on the broker and the gateway only: a
//! scenario sets its value in the bootstrap admin's environment and reads which processes hold it.
//!
//! It declares three listeners (Q151), [`LISTENERS`], on the gateway: a gateway binds them from its
//! launch, reports them at its join and its row publishes them.
//!
//! It registers a second topology storage provider (storage.md §2), [`TOPOLOGY_STORAGE_TARGET`], in
//! every executable of the testkit: RDM's file backend at a place of its own beside the fabric's store
//! dir ([`topology_storage_dir`]), a separate durable namespace standing in for an application's
//! backend.

use rdm_admin::wiring::{FabricHooks, FabricReadyCtx, MeshReadyCtx};
use std::sync::Arc;
use tracing::Instrument;

/// The testkit application's declared secret: set in the process environment of [`DECLARED_KINDS`]
/// only.
pub const DECLARED_SECRET: &str = "TESTKIT_DECLARED_SECRET";

/// The kinds [`DECLARED_SECRET`] is set on.
pub const DECLARED_KINDS: [rdm_core::NodeKind; 2] = [rdm_core::NodeKind::Broker, rdm_core::NodeKind::Gateway];

/// The kind the testkit application declares listeners on (Q151).
pub const LISTENER_KIND: rdm_core::NodeKind = rdm_core::NodeKind::Gateway;

/// The listeners the testkit application declares on [`LISTENER_KIND`], as Rafka's gateway names its
/// own.
pub const LISTENERS: [&str; 3] = ["kafka", "rest", "proxy"];

/// The name the testkit registers its second topology storage provider under (storage.md §2: a name
/// is bound to its place at registration).
pub const TOPOLOGY_STORAGE_TARGET: &str = "test-target";

/// Where [`TOPOLOGY_STORAGE_TARGET`] keeps its stores for a fabric whose store dir
/// (`RDM_STORE_DIR`) is `store_dir`: a directory beside it, never inside it, so the file provider's
/// store dir holds only its own roots.
pub fn topology_storage_dir(store_dir: &std::path::Path) -> std::path::PathBuf {
    let name = store_dir.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    store_dir.with_file_name(format!("{name}-{TOPOLOGY_STORAGE_TARGET}"))
}

/// The testkit application's listener declarations, as its wiring registers them.
pub fn listeners() -> Vec<rdm_core::launch::DeclaredListeners> {
    vec![rdm_core::launch::DeclaredListeners { kind: LISTENER_KIND, names: LISTENERS.iter().map(|n| n.to_string()).collect() }]
}

/// The testkit application's declarations, as its wiring registers them.
pub fn declared() -> Vec<rdm_core::launch::DeclaredSecret> {
    vec![rdm_core::launch::DeclaredSecret { name: DECLARED_SECRET.into(), kinds: DECLARED_KINDS.to_vec() }]
}

/// The hooks of this application: each notice records a span.
pub fn hooks() -> FabricHooks {
    let on_mesh_ready: rdm_admin::wiring::MeshReadyNotice = Arc::new(|ready: MeshReadyCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-mesh-ready", fabric_id = %ready.fabric_id, mesh = %ready.mesh, mesh_id = %ready.mesh_id, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application was told the mesh is ready-for-traffic") }.instrument(span))
    });
    let on_fabric_ready: rdm_admin::wiring::FabricReadyNotice = Arc::new(|ready: FabricReadyCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-fabric-ready", fabric_id = %ready.fabric_id, build_id = %ready.build_id, attempt = ready.attempt, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application opens client traffic") }.instrument(span))
    });
    FabricHooks { on_mesh_ready: Some(on_mesh_ready), on_fabric_ready: Some(on_fabric_ready) }
}
