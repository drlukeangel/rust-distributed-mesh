//! `on_mesh_ready`: an application's notice that one of its meshes is ready (fabric-node-hooks.md).
//!
//! A node-admin executable registers the notice in its [`Wiring`]'s [`FabricHooks`]. The mesh-primary
//! calls it once its mesh's ready is on its Mesh record, and the call is fire and forget: the notice
//! gates nothing, so the mesh is ready whatever the notice does and however long it takes. Here the
//! application's work is to count the meshes it was told about and write one log line per notice;
//! a product starts its per-mesh work where the line is (a topic layout, a cache warm-up).
//!
//! Build: `cargo build --example on_mesh_ready` in `demo/`. Run: as a node-admin executable, in place of
//! `rshape-node-admin` (the Day-0 admin of `scripts/demo-estate.sh up`, or a Build naming it for the
//! node-admin paths). Every node-admin of the fabric should register the same notices: whichever admin
//! is a mesh's primary calls them.
use rdm_admin::wiring::{FabricHooks, MeshReadyCtx, MeshReadyNotice, Wiring};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const SERVICE: &str = "example-on-mesh-ready";

/// The notice: one line naming the mesh, and the count of meshes this admin was told are ready.
fn on_mesh_ready() -> MeshReadyNotice {
    let told = Arc::new(AtomicU64::new(0));
    Arc::new(move |ready: MeshReadyCtx| {
        let told = told.clone();
        Box::pin(async move {
            let n = told.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::info!(target: "example", fabric_id = %ready.fabric_id, mesh = %ready.mesh, mesh_id = %ready.mesh_id, meshes_ready = n, "on_mesh_ready: the mesh is ready");
        })
    })
}

#[tokio::main]
async fn main() {
    rdm_admin::entry::run_with(SERVICE, |cfg| Wiring {
        fabric_hooks: FabricHooks { on_mesh_ready: Some(on_mesh_ready()), ..FabricHooks::no_app_work() },
        certs: rdm_admin::entry::material_or_exit(SERVICE, cfg),
        ..Wiring::default()
    })
    .await
}
