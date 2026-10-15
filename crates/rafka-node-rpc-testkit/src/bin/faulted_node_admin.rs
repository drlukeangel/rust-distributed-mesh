//! The testkit's node-admin executable (i143.e8.s2): the public node-admin runtime under its own
//! service name, with the testkit's stall decorators wired in and their door served on its Node RPC
//! server (op `0x75`, `rafka_node_rpc_testkit::fault_door`). A scenario binds
//! it as the `node_admin` launch id of an estate and holds the admin at a documented cut
//! (`rafka_node_rpc_testkit::admin_faults`). A product binary never carries any of this.

use rdm_core::{NodeKind, PathName};
use rafka_node_rpc_testkit::admin_faults::{self, AdminFaults};
use rafka_node_rpc_testkit::test_app;

#[tokio::main]
async fn main() {
    rdm_admin::entry::run_with("faulted-node-admin", |cfg| {
        // The path.name this admin is: the launch's, or the Day-0 admin's first.
        let name = cfg.launch.as_ref().map(|l| l.name.to_string()).unwrap_or_else(|| PathName::new(&cfg.mesh, NodeKind::NodeAdmin, 1).to_string());
        // The directory that holds every node's data dir of this fabric: where its boot cuts are listed.
        let root = cfg.data_dir.parent().map(std::path::Path::to_path_buf).unwrap_or_else(|| cfg.data_dir.clone());
        let faults = AdminFaults::new(name.clone());
        let fail = |what: &str, e: String| -> ! {
            eprintln!("faulted-node-admin {name}: {what}: {e}");
            std::process::exit(2)
        };
        let armed = admin_faults::arm_boot_cuts(&faults, &root).unwrap_or_else(|e| fail("boot cuts", e));
        tracing::info_span!("rdm.testkit.fault.create.via-boot", node = %name, door_op = rafka_node_rpc_testkit::fault_door::OP, boot_cuts = armed).in_scope(|| tracing::info!("boot cuts armed; the fault door is served on the Node RPC server"));
        // This executable is the testkit's own: it signs with the deterministic test signer, so a scenario
        // reads what the bytes encode. A launch that carries issuing material says so in a span.
        if let Some(l) = &cfg.launch {
            // The export filter admits `rafka*` targets only: this executable's own target is not one.
            tracing::info_span!(
                target: "rafka_node_rpc_testkit",
                "rdm.testkit.cert.resolve.via-launch",
                node = %name,
                mesh_issuer_len = l.mesh_issuer.as_ref().map_or(0, Vec::len) as u64,
                mesh_issuer = %String::from_utf8_lossy(l.mesh_issuer.as_deref().unwrap_or_default()),
            )
            .in_scope(|| tracing::info!(target: "rafka_node_rpc_testkit", "the launch's issuing material"));
        }
        // The testkit's application answers the fabric's hooks as `RDM_TEST_APP` says.
        let mode = test_app::Mode::from_env(|k| std::env::var(k).ok()).unwrap_or_else(|e| fail("test app", e));
        let mut wiring = admin_faults::wiring(faults.clone());
        let door = faults.clone();
        wiring.serve_app = Some(Box::new(move |b| rafka_node_rpc_testkit::fault_door::serve(b, door)));
        wiring.fabric_hooks = test_app::hooks(mode, faults).unwrap_or_else(|e| fail("test app", e));
        wiring.certs = rdm_admin::certs::CertChoice::Signer(std::sync::Arc::new(rafka_node_rpc_testkit::test_certs::TestCertSigner));
        wiring
    })
    .await
}
