//! The testkit's node-admin executable (i143.e8.s2): the public node-admin runtime under its own
//! service name, with the testkit's stall decorators wired in and their door served on its Node RPC
//! server (op `0x75`, `rdm_test::fault_door`). A scenario binds
//! it as the `node_admin` launch id of an estate and holds the admin at a documented cut
//! (`rdm_test::admin_faults`). A product binary never carries any of this.

use rdm_core::{NodeKind, PathName};
use rdm_test::admin_faults::{self, AdminFaults};
use rdm_test::test_app;

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
        tracing::info_span!("rdm.testkit.fault.create.via-boot", node = %name, door_op = rdm_test::fault_door::OP, boot_cuts = armed).in_scope(|| tracing::info!("boot cuts armed; the fault door is served on the Node RPC server"));
        // This executable holds the cert material every RDM node-admin does (fabric-certs.md): the root its
        // environment or data dir gives it, or its launch's issuing material. A launch's issuing cert (never
        // its key), and whether the root rode beside it (never the root), are recorded in a span, so a
        // scenario reads what the admin was launched with.
        if let Some(l) = &cfg.launch {
            // The export filter admits `rafka*` targets only: this executable's own target is not one.
            tracing::info_span!(
                target: "rafka_node_rpc_testkit",
                "rdm.testkit.cert.resolve.via-launch",
                node = %name,
                issuing = l.issuing.is_some(),
                root = l.issuing.is_some(),
                ca_key = l.issuing.as_ref().is_some_and(|m| m.ca_key.is_some()),
                ca_cert = %l.issuing.as_ref().and_then(|m| rdm_core::certs::decode_issuing_cert(&m.cert).ok()).map(|c| rdm_core::certs::encode_ca_cert(&c.ca)).unwrap_or_default(),
                issuing_cert = %l.issuing.as_ref().map(|m| m.cert.as_str()).unwrap_or(""),
            )
            .in_scope(|| tracing::info!(target: "rafka_node_rpc_testkit", "the launch's issuing material"));
        }
        // The testkit's application answers the fabric's hooks as `RDM_TEST_APP` says.
        let mode = test_app::Mode::from_env(|k| std::env::var(k).ok()).unwrap_or_else(|e| fail("test app", e));
        let mut wiring = admin_faults::wiring(faults.clone());
        let door = faults.clone();
        wiring.serve_app = Some(Box::new(move |b| rdm_test::fault_door::serve(b, door)));
        wiring.fabric_hooks = test_app::hooks(mode, faults).unwrap_or_else(|e| fail("test app", e));
        wiring.certs = rdm_admin::entry::material_or_exit("faulted-node-admin", cfg);
        wiring
    })
    .await
}
