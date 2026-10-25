//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

/// Only this story's spans reached the exporter: every one is RDM's own (`rdm.*`). A transport
/// internal (iroh, noq) or any other foreign span in a cell's trace is a defect of the capture.
pub fn assert_own_story<'a>(names: impl IntoIterator<Item = &'a str>) {
    let foreign: std::collections::BTreeSet<&str> = names.into_iter().filter(|n| !n.starts_with("rdm.")).collect();
    assert!(foreign.is_empty(), "spans outside the cell's story were exported: {foreign:?}");
}

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;

/// The binary's process environment, set once, here, before `main` and before any thread exists
/// (one binary is one process): a cell never sets or removes a variable. The gate's cadence
/// (`tools/test-runner/src/lib.rs` CADENCE), each value only where the environment the binary was
/// started with names none: the gate's own export, or the environment a delegating parent hands the
/// child it starts for one cell (`own_process::delegated_with_env`, a cell whose process needs other
/// values: a stretched checklist, its own evidence directory, a runtime socket), stays as given.
#[ctor::ctor]
fn process_env() {
    for (k, v) in [("RDM_STALENESS_MS", "3000"), ("RDM_GOSSIP_INTERVAL_MS", "500"), ("RDM_BACKBONE_INTERVAL_MS", "500")] {
        if std::env::var_os(k).is_none() {
            std::env::set_var(k, v);
        }
    }
}

/// The rafka-time a test admin holds, adopted from a reference nowhere near any OS clock.
pub fn adopted_time() -> rdm_transport::clock::RafkaTime {
    let t = rdm_transport::clock::RafkaTime::unadopted();
    t.adopt(1_000_000);
    t
}

/// The executable a test request names for a path of `kind`: a file of its own per kind, hashed once,
/// so the door's acceptance (it hashes every executable a request creates a node on) passes.
pub fn test_executable(kind: rdm_core::NodeKind) -> rdm_core::binding::Binding {
    use std::os::unix::fs::PermissionsExt;
    static DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    let dir = DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("admin-test-executables-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    });
    let p = dir.join(kind.name());
    if !p.exists() {
        let tmp = dir.join(format!("{}.{}", kind.name(), rand::random::<u32>()));
        std::fs::write(&tmp, format!("#!/bin/sh\n# {}\nexit 0\n", kind.name())).unwrap();
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).unwrap();
        let _ = std::fs::rename(&tmp, &p);
    }
    rdm_core::binding::Binding { launch_id: kind.name().into(), sha256: rdm_core::binding::sha256_file(&p).unwrap(), executable: p, image: None }
}

/// `change`, a count form, as the complete-topology request a caller builds from what it reads
/// (proposal executable-bindings-at-create.md rule 1: the count is the door's vocabulary, expanded into
/// named paths before the typed call): compiled against `cp`'s current Build, every path it creates
/// named on [`test_executable`] of its kind, every held path listed as its entry holds it; a change
/// that creates no path is returned as it is. `cp` reads its node rows from an empty store when it has
/// none.
pub async fn named_request(cp: &rdm_admin::control_plane::ControlPlane, change: rdm_admin::accepted::TopologyChange) -> rdm_admin::accepted::TopologyChange {
    use rdm_admin::build_state::BuildStateAdapter as _;
    let current = cp.builds.current().await.expect("the Build state reads").expect("a current Build");
    let observed = cp.topology.read().await.clone();
    let next = rdm_admin::accepted::compile(&current.topology, &change, &observed).unwrap_or_else(|e| panic!("{change:?} compiles: {e}"));
    // A change that creates no path is submitted as it is: only a create names an executable.
    if next.meshes.values().flat_map(|m| m.nodes.iter()).all(|p| current.topology.contains(p)) {
        return change;
    }
    let _ = cp.node_rows.set(std::sync::Arc::new(rdm_admin::storage::MemoryNodesStorage::default()));
    let nodes = next
        .meshes
        .values()
        .flat_map(|m| m.path_config_by_path.iter())
        .map(|(p, c)| {
            let mut c = c.clone();
            if !current.topology.contains(p) && c.executable.is_none() {
                c.executable = Some(test_executable(p.kind));
            }
            (p.clone(), c)
        })
        .collect();
    rdm_admin::accepted::TopologyChange::Reconcile { based_on: current.build_id, nodes }
}

#[cfg(feature = "file-backend")]
mod admin_app_alpns;
mod app_protocol_dials;
mod admin_serves_forward;
mod ids_crockford;
#[cfg(feature = "file-backend")]
mod backbone_anti_entropy;
mod boot_stop;
mod loopback;
mod mesh_delete_resume;
mod mesh_delete_workflow;
mod mesh_delete_source;
mod mesh_delete_view;
mod build_catch_up;
mod build_admission;
mod build_claim;
mod build_drive;
mod build_run;
mod build_facts_read;
mod build_takeover;
mod command_admission_status;
#[cfg(feature = "file-backend")]
mod connections_writer;
#[cfg(feature = "file-backend")]
mod container_proof;
mod container_unsupported;
mod current_build;
mod drift_convergence;
#[cfg(feature = "file-backend")]
mod fabric_handover;
mod fabric_not_ready;
mod status_rpc;
mod executor_redecide;
#[cfg(feature = "file-backend")]
mod connections_reconnect;
mod gossip_frames_wire;
mod abandoned_join;
mod join_admission;
mod join_wire;
mod node_objects;
#[cfg(feature = "file-backend")]
mod node_rows;
mod node_get_reads;
mod node_rpc_objects;
#[cfg(feature = "file-backend")]
mod stop_gate_quiesce;
#[cfg(feature = "file-backend")]
mod stop_gate_view_lock;
mod own_run_stop;
mod process_launch_environment;
mod provider_state;
mod provider_policy;
mod readiness_gates;
mod reconcile_variants;
mod receipt_refusal;
mod recovery_admission;
#[cfg(feature = "file-backend")]
mod restart_hold_from_facts;
mod restart_one_operation;
mod pipeline_moves_no_seat;
mod pipeline_step_io;
mod status_declarations;
#[cfg(feature = "file-backend")]
mod status_seat_fold;
mod stopped_delete;
mod stopped_gc;
#[cfg(feature = "file-backend")]
mod store_root_boot;
mod topology_read;
mod fabric_delete;

#[allow(dead_code)]
#[path = "../../../tools/test-support/gossip_wire.rs"]
mod gossip_wire;
