//! A node-admin process stopped while it boots (#3047): the stop is heard and the process exits
//! through its telemetry drain, so its evidence file holds the boot and the stop.

use rafka_mesh_entity::launch::{Launch, Launcher};
use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use std::time::{Duration, Instant};

/// CONTRACT: a launched `rafka-node-admin` that is held in its boot (no runtime record from its
/// provider yet: `admin.rs` waits for it up to 10 s) and is sent SIGTERM ends its boot by name and
/// exits 0 through its telemetry drain: its evidence file holds `rdm.mesh.node.create.via-deployment`
/// and `rdm.mesh.node.delete.via-signal` with `booted=false`. What must NOT happen: the default
/// SIGTERM disposition ending the process before its batch export, leaving the file empty, or the
/// abandoned boot holding the exit past a Graceful stop's 5 s grace.
#[test]
fn a_node_admin_stopped_while_it_boots_exits_through_its_telemetry_drain() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit/a_node_admin_stopped_while_it_boots_exits_through_its_telemetry_drain");
    std::fs::create_dir_all(&dir).unwrap();
    let data_dir = std::env::temp_dir().join(format!("rdm-boot-stop-{}", NodeId::mint()));
    std::fs::create_dir_all(&data_dir).unwrap();
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id: FabricId::mint(),
        name: "mesh1.admin.2".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
        seeds: Vec::new(),
        launcher: Some(Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() }),
        data_dir: data_dir.clone(),
        store_dir: data_dir.clone(),
        mesh_id: None,
        mesh_issuer: None,
    };
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-node-admin"))
        .envs(launch.to_env())
        .env("RDM_EVIDENCE_DIR", &dir)
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("OTEL_SERVICE_NAME")
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("rafka-node-admin starts");
    let pid = child.id();
    let prefix = format!("rafka-node-admin.{pid}-");
    let file = || {
        std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok().map(|e| e.path())).find(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(&prefix)))
    };
    // Its telemetry is up once its evidence file exists; its boot then waits for the runtime record.
    let until = Instant::now() + Duration::from_secs(10);
    let path = loop {
        if let Some(p) = file() {
            break p;
        }
        assert!(Instant::now() < until, "rafka-node-admin {pid} opened no evidence file in {}", dir.display());
        std::thread::sleep(Duration::from_millis(5));
    };
    unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    let signalled = Instant::now();
    let status = child.wait().unwrap();
    let stopped_in = signalled.elapsed();
    let _ = std::fs::remove_dir_all(&data_dir);
    let spans: Vec<serde_json::Value> = std::fs::read_to_string(&path).unwrap().lines().filter_map(|l| serde_json::from_str(l).ok()).collect();
    let names: Vec<&str> = spans.iter().filter_map(|s| s["name"].as_str()).collect();
    assert!(names.contains(&"rdm.mesh.node.create.via-deployment"), "{} holds no boot span ({status}, {} spans: {names:?})", path.display(), spans.len());
    let stop = spans.iter().find(|s| s["name"] == "rdm.mesh.node.delete.via-signal").unwrap_or_else(|| panic!("{} holds no stop span ({status}, {names:?})", path.display()));
    assert_eq!(stop["attributes"]["booted"], "false", "{stop}");
    assert!(status.success(), "the stopped admin exits 0: {status}");
    // Within a Graceful stop's 5 s grace: the boot's record wait is dropped with the boot, never
    // waited out (10 s) by the runtime's shutdown.
    assert!(stopped_in < Duration::from_secs(5), "the admin exited {} ms after its stop: something of its abandoned boot held the exit", stopped_in.as_millis());
}
