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

/// The spans the evidence files of process `pid` under `dir` hold.
fn spans_of(dir: &std::path::Path, pid: u32) -> Vec<serde_json::Value> {
    let prefix = format!("rafka-node-admin.{pid}-");
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(&prefix)))
        .flat_map(|p| std::fs::read_to_string(p).unwrap_or_default().lines().filter_map(|l| serde_json::from_str(l).ok()).collect::<Vec<serde_json::Value>>())
        .collect()
}

/// The fabric directories under `store`: every directory but the fabrics' name index beside them (R-FN1).
fn fabric_dirs(store: &std::path::Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(store)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir() && e.file_name() != rafka_node_admin_core::store_root::FABRIC_NAMES)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

/// CONTRACT (R-FI1=A; node-admin-lifecycle.md:65 "Restart | its own data dir (same NodeId)"): a
/// bootstrap node-admin restarted on its own data dir, with the fabric's store_dir and no
/// `RDM_FABRIC_ID`, reads the fabric id from its own row before the store root opens, and restarts
/// as the same node in the same Fabric: it opens `<store_dir>/<its fabric id>/`, mints no second
/// fabric directory, and its boot's storage step names that fabric id. What must NOT happen: the
/// restart minting a new fabric id, opening an empty root and being refused because "fabric.storage
/// holds no Fabric record".
#[test]
fn a_restarted_bootstrap_admin_opens_its_fabric_from_its_own_row() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit/a_restarted_bootstrap_admin_opens_its_fabric_from_its_own_row");
    let _ = std::fs::remove_dir_all(&dir);
    let store = dir.join("store");
    std::fs::create_dir_all(&store).unwrap();
    let data_dir = dir.join("mesh1.admin.1");
    let run = || {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-node-admin"));
        cmd.env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("RDM_DATA_DIR", &data_dir)
            .env("RDM_STORE_DIR", &store)
            .env("RDM_EVIDENCE_DIR", &dir)
            .env("MESH_SPAWN_TYPE", "process")
            .env("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0");
        cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().expect("rafka-node-admin starts")
    };
    let own = data_dir.join("nodes").join("self.json");
    let own_incarnation = || std::fs::read_to_string(&own).ok().and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()).and_then(|v| v["record"]["incarnation_id"].as_str().map(str::to_string));
    let stop = |mut child: std::process::Child| {
        unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
        let status = child.wait().unwrap();
        let mut err = String::new();
        std::io::Read::read_to_string(child.stderr.as_mut().unwrap(), &mut err).unwrap();
        (status, err)
    };

    // Day 0: the admin mints its fabric and writes its own row and its mesh's row on the root.
    let first = run();
    // A panic below must not leave the Day-0 process running: it is stopped by its exact pid.
    struct Reap(u32);
    impl Drop for Reap {
        fn drop(&mut self) {
            unsafe { libc::kill(self.0 as i32, libc::SIGKILL) };
        }
    }
    let reap_first = Reap(first.id());
    let mut first = first;
    let until = Instant::now() + Duration::from_secs(20);
    let fabric = loop {
        let dirs = fabric_dirs(&store);
        if let (Some(_), [f]) = (own_incarnation(), dirs.as_slice()) {
            if store.join(f).join("meshes").join("mesh1.json").exists() {
                break f.clone();
            }
        }
        if let Some(s) = first.try_wait().unwrap() {
            std::mem::forget(reap_first);
            panic!("the Day-0 admin exited {s:?}");
        }
        assert!(Instant::now() < until, "the Day-0 admin wrote no own row and mesh row in 20 s: {:?}", fabric_dirs(&store));
        std::thread::sleep(Duration::from_millis(20));
    };
    let day0 = own_incarnation().unwrap();
    // R-FN1: the fabric's claim of its name is at the path level, beside the per-fabric roots.
    let index: serde_json::Value = serde_json::from_slice(&std::fs::read(store.join(rafka_node_admin_core::store_root::FABRIC_NAMES).join("fabric1.json")).expect("the fabric's name claim at <store_dir>/fabrics/fabric1")).unwrap();
    assert_eq!(index["record"]["fabric_id"].as_str(), Some(fabric.as_str()), "the name index names this fabric: {index}");
    let (status, err) = stop(first);
    std::mem::forget(reap_first);
    assert!(status.success(), "the Day-0 admin's stop: {status:?} {err}");

    // The restart: same data dir and store_dir, no fabric id handed in.
    let mut second = run();
    let pid = second.id();
    let reap_second = Reap(pid);
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        if own_incarnation().is_some_and(|i| i != day0) {
            break;
        }
        if second.try_wait().unwrap().is_some() {
            std::mem::forget(reap_second);
            let (status, err) = stop(second);
            panic!("the restart exited {status:?}: {err}");
        }
        assert!(Instant::now() < until, "the restart wrote no own row under a new incarnation in 20 s");
        std::thread::sleep(Duration::from_millis(20));
    }
    let (status, err) = stop(second);
    std::mem::forget(reap_second);
    assert!(status.success(), "the restart's stop: {status:?} {err}");
    assert_eq!(fabric_dirs(&store), vec![fabric.clone()], "the restart opened a fabric directory other than its own");
    let spans = spans_of(&dir, pid);
    let opened = spans.iter().find(|s| s["name"] == "rdm.mesh.node.resolve.via-storage-opened").unwrap_or_else(|| panic!("the restart left no storage step: {spans:?}"));
    println!("trace_id={}", opened["trace_id"].as_str().unwrap_or(""));
    assert_eq!(opened["attributes"]["fabric_id"].as_str(), Some(fabric.as_str()), "{opened}");
    assert_eq!(opened["attributes"]["restart"].as_str(), Some("true"), "{opened}");
}
