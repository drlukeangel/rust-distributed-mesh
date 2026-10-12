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

/// CONTRACT (states.md "The initial fabric before ready-for-traffic": "Once the fabric is ready-for-traffic
/// these are admitted as usual"; "A resize or roll puts the fabric back to pending"): the admin that
/// opens a store root already holding the Fabric row, published at the fabric's first state-commit,
/// knows the fabric has first reached ready-for-traffic from the root itself, not from the status it
/// hears. A bootstrap admin on a fresh root has not (one `rdm.node_admin.fabric.update.via-first-ready`
/// says `ready-for-traffic`, when it climbs); the same admin restarted on its data dir and root says
/// `fabric-row-on-store-root` at its boot, before any status is heard. What must NOT happen: a restarted
/// admin, or one born into a resize after the first ready-for-traffic, refusing a caller's workflow
/// `fabric-not-ready` because it heard pending.
#[test]
fn an_admin_that_opens_a_root_holding_the_fabric_row_knows_the_fabric_first_reached_ready_for_traffic() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit/an_admin_that_opens_a_root_holding_the_fabric_row_knows_the_fabric_first_reached_ready_for_traffic");
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
    struct Reap(u32);
    impl Drop for Reap {
        fn drop(&mut self) {
            unsafe { libc::kill(self.0 as i32, libc::SIGKILL) };
        }
    }
    let stop = |mut child: std::process::Child| {
        unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
        let status = child.wait().unwrap();
        let mut err = String::new();
        std::io::Read::read_to_string(child.stderr.as_mut().unwrap(), &mut err).unwrap();
        (status, err)
    };
    let first_ready = |pid: u32| -> Vec<String> {
        spans_of(&dir, pid).iter().filter(|s| s["name"] == "rdm.node_admin.fabric.update.via-first-ready").map(|s| s["attributes"]["reason"].as_str().unwrap_or_default().to_string()).collect()
    };

    // Day 0: the fabric climbs to ready-for-traffic on a fresh root and says why it knows.
    let first = run();
    let day0_pid = first.id();
    let reap_first = Reap(day0_pid);
    let until = Instant::now() + Duration::from_secs(30);
    let row = loop {
        let published = fabric_dirs(&store).first().map(|f| store.join(f).join("fabric").join("identity.json")).filter(|p| p.exists());
        if let (Some(row), true) = (published, first_ready(day0_pid) == ["ready-for-traffic"]) {
            break row;
        }
        assert!(Instant::now() < until, "the Day-0 admin published no Fabric row and reached ready-for-traffic in 30 s: {:?} {:?}", fabric_dirs(&store), first_ready(day0_pid));
        std::thread::sleep(Duration::from_millis(50));
    };
    let (status, err) = stop(first);
    std::mem::forget(reap_first);
    assert!(status.success(), "the Day-0 admin's stop: {status:?} {err}");
    assert!(row.exists(), "the Fabric row stands on the root");

    // The restart: the root holds the Fabric row, so the admin knows at boot.
    let second = run();
    let pid = second.id();
    let reap_second = Reap(pid);
    let until = Instant::now() + Duration::from_secs(20);
    while first_ready(pid).is_empty() {
        assert!(Instant::now() < until, "the restart said nothing of the fabric having reached ready-for-traffic in 20 s");
        std::thread::sleep(Duration::from_millis(20));
    }
    let (status, err) = stop(second);
    std::mem::forget(reap_second);
    assert!(status.success(), "the restart's stop: {status:?} {err}");
    let spans = spans_of(&dir, pid);
    let said: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.fabric.update.via-first-ready").collect();
    assert_eq!(said.first().map(|s| s["attributes"]["reason"].as_str()), Some(Some("fabric-row-on-store-root")), "the restart knows from the root, at boot: {said:?}");
    println!("trace_id={}", said[0]["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-MN1: "a mesh created again under the same name is a new mesh with a new id"; R-ML2: "a
/// dead mesh's path.name used again is mesh.create of a new mesh with a new MeshId", "a dead mesh is
/// never restarted"; states.md: a mesh id is never reissued): a bootstrap node-admin restarted on its
/// own data dir reads its mesh's row by its mesh's id, the one its own row holds, never by the name
/// alone. Its mesh removed and a mesh created again under the name (mesh1's row on the store root now
/// another MeshId's), the restart is refused by name, naming both mesh ids; its own mesh's row Dead,
/// it is refused by name. Must NOT happen: the restart taking the recreated mesh's id as its own (the
/// row at the name read as its mesh) and joining it.
#[test]
fn a_restarted_admin_reads_its_mesh_row_by_its_mesh_id_and_never_joins_a_mesh_created_again_under_its_name() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit/a_restarted_admin_reads_its_mesh_row_by_its_mesh_id");
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
    struct Reap(u32);
    impl Drop for Reap {
        fn drop(&mut self) {
            unsafe { libc::kill(self.0 as i32, libc::SIGKILL) };
        }
    }
    let own = data_dir.join("nodes").join("self.json");
    let own_incarnation = || std::fs::read_to_string(&own).ok().and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok()).and_then(|v| v["record"]["incarnation_id"].as_str().map(str::to_string));
    let stderr_of = |child: &mut std::process::Child| {
        let mut err = String::new();
        std::io::Read::read_to_string(child.stderr.as_mut().unwrap(), &mut err).unwrap();
        err
    };

    // Day 0: the admin mints its fabric and mesh1, and writes its own row and mesh1's row.
    let mut first = run();
    let reap_first = Reap(first.id());
    let until = Instant::now() + Duration::from_secs(20);
    let (fabric, mesh_row) = loop {
        if let (Some(_), [f]) = (own_incarnation(), fabric_dirs(&store).as_slice()) {
            let row = store.join(f).join("meshes").join("mesh1.json");
            if row.exists() {
                break (f.clone(), row);
            }
        }
        assert!(first.try_wait().unwrap().is_none(), "the Day-0 admin exited");
        assert!(Instant::now() < until, "the Day-0 admin wrote no own row and mesh row in 20 s");
        std::thread::sleep(Duration::from_millis(20));
    };
    let day0 = own_incarnation().unwrap();
    let removed: serde_json::Value = serde_json::from_slice(&std::fs::read(&mesh_row).unwrap()).unwrap();
    let removed_id = removed["record"]["mesh_id"].as_str().unwrap().to_string();
    unsafe { libc::kill(first.id() as i32, libc::SIGTERM) };
    assert!(first.wait().unwrap().success(), "the Day-0 admin's stop");
    std::mem::forget(reap_first);

    // A restart against `row` on the store root: it must be refused by name before it writes its own
    // row again (the boot fails at its storage step).
    let restart = |row: serde_json::Value, what: &str| -> (String, u32) {
        std::fs::write(&mesh_row, serde_json::to_vec(&row).unwrap()).unwrap();
        let mut child = run();
        let pid = child.id();
        let reap = Reap(pid);
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                std::mem::forget(reap);
                let err = stderr_of(&mut child);
                assert!(!status.success(), "{what}: the restart exited {status:?} without a refusal: {err}");
                return (err, pid);
            }
            if own_incarnation().is_some_and(|i| i != day0) {
                unsafe { libc::kill(pid as i32, libc::SIGTERM) };
                let _ = child.wait();
                std::mem::forget(reap);
                panic!("{what}: the restart took the row at mesh1 as its mesh and booted (its own row rewritten under a new incarnation): {}", stderr_of(&mut child));
            }
            assert!(Instant::now() < until, "{what}: the restart neither refused nor booted in 10 s");
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let recreated_id = rafka_node_admin_core::model::MeshId::mint().to_string();
    let (err, pid) = restart(serde_json::json!({"format": removed["format"], "record": {"mesh_id": recreated_id, "name": "mesh1", "dead": false}}), "mesh1 created again");
    assert!(err.contains("refusing to start") && err.contains(&removed_id) && err.contains(&recreated_id), "the refusal names this admin's mesh and the mesh created again under its name: {err}");
    let spans = spans_of(&dir, pid);
    let opened = spans.iter().find(|s| s["name"] == "rdm.mesh.node.resolve.via-storage-opened").unwrap_or_else(|| panic!("the refused restart left no storage step: {spans:?}"));
    println!("trace_id={}", opened["trace_id"].as_str().unwrap_or(""));
    assert_eq!(opened["attributes"]["fabric_id"].as_str(), Some(fabric.as_str()), "{opened}");
    assert_eq!(opened["attributes"]["mesh_id"].as_str(), Some(removed_id.as_str()), "the restart's storage step names the mesh id its own row holds: {opened}");
    assert!(opened["attributes"]["error"].as_str().is_some_and(|e| e.contains(&recreated_id)), "the storage step names the refusal and the mesh created again: {opened}");
    let (err, _) = restart(serde_json::json!({"format": removed["format"], "record": {"mesh_id": removed_id, "name": "mesh1", "dead": true}}), "mesh1 Dead");
    assert!(err.contains("refusing to start") && err.contains(&removed_id) && err.contains("Dead"), "the refusal names this admin's mesh as Dead: {err}");
}
