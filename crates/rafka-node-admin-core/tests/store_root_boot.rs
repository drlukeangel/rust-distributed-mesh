//! A node-admin's boot and the fabric's store root (R14 items 1 and 8): a node-admin process given
//! no `store_dir`, or one it cannot use, is refused by name at boot, naming the path and the cause;
//! nothing falls back to its data dir.

use std::time::{Duration, Instant};

/// A bootstrap `rafka-node-admin` run on `env` until it exits: its exit status, its stderr and the
/// spans of its evidence file.
fn boot(name: &str, env: &[(&str, String)]) -> (std::process::ExitStatus, String, Vec<serde_json::Value>, std::path::PathBuf) {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let data_dir = dir.join("mesh1.admin.1");
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-node-admin"));
    cmd.env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("RDM_DATA_DIR", &data_dir)
        .env("RDM_EVIDENCE_DIR", &dir)
        .env("MESH_SPAWN_TYPE", "process")
        .env("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let child = cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().expect("rafka-node-admin starts");
    let pid = child.id();
    let started = Instant::now();
    let out = child.wait_with_output().unwrap();
    assert!(started.elapsed() < Duration::from_secs(10), "a refused boot took {} ms", started.elapsed().as_millis());
    let prefix = format!("rafka-node-admin.{pid}-");
    let spans = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(&prefix)))
        .flat_map(|p| std::fs::read_to_string(p).unwrap_or_default().lines().filter_map(|l| serde_json::from_str(l).ok()).collect::<Vec<serde_json::Value>>())
        .collect();
    (out.status, String::from_utf8_lossy(&out.stderr).into_owned(), spans, data_dir)
}

/// CONTRACT (R14 item 8; acceptance "a missing store_dir is refused by name at boot"): a node-admin
/// started with no `RDM_STORE_DIR` is refused by name before it boots (exit 2, the variable named);
/// one whose `RDM_STORE_DIR` does not exist is refused at boot naming the path and the cause (exit
/// 3), its boot's storage step naming the store_dir it refused; neither creates the store_dir nor writes a store in its
/// data dir.
#[test]
fn a_node_admin_with_no_usable_store_dir_is_refused_by_name_at_boot() {
    let (status, stderr, _, data_dir) = boot("a_node_admin_with_no_store_dir", &[]);
    assert_eq!(status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("refusing to start: RDM_STORE_DIR is required"), "{stderr}");
    assert!(!data_dir.join("fabric").exists() && !data_dir.join("meshes").exists(), "a refused boot wrote a store in its data dir");

    let missing = std::env::temp_dir().join(format!("rdm-no-store-{}", std::process::id()));
    let (status, stderr, spans, data_dir) = boot("a_node_admin_with_a_missing_store_dir", &[("RDM_STORE_DIR", missing.display().to_string())]);
    assert_eq!(status.code(), Some(3), "{stderr}");
    let named = format!("refusing to start: {}: the store_dir (RDM_STORE_DIR) cannot be used: No such file or directory", missing.display());
    assert!(stderr.contains(&named), "{stderr}");
    assert!(!missing.exists(), "the boot created the store_dir it was refused for");
    assert!(!data_dir.join("fabric").exists() && !data_dir.join("meshes").exists() && !data_dir.join("connections").exists(), "a refused boot fell back to its data dir");
    assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.node.create.via-deployment"), "the refused process left no deployment span: {spans:?}");
    let boot_span = spans.iter().find(|s| s["name"] == "rdm.mesh.node.create.via-boot").unwrap_or_else(|| panic!("the refused boot left no boot span: {spans:?}"));
    println!("trace_id={}", boot_span["trace_id"].as_str().unwrap_or(""));
    let refused = spans.iter().find(|s| s["name"] == "rdm.mesh.node.resolve.via-storage-opened").unwrap_or_else(|| panic!("no storage step span: {spans:?}"));
    assert_eq!(refused["trace_id"], boot_span["trace_id"], "the storage step is the boot's own story");
    assert_eq!(refused["attributes"]["store_dir"].as_str(), Some(missing.display().to_string().as_str()), "{refused}");
}
