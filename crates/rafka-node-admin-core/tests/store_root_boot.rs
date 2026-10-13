//! A node-admin's boot and the fabric's store root (R14 items 1 and 8): a node-admin process given
//! no `store_dir`, or one it cannot use, is refused by name at boot, naming the path and the cause;
//! nothing falls back to its data dir. An admin started on a root prepared through the storage adapters,
//! with no previous process, boots from its rows (R-SB1).

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

/// A bootstrap `rafka-node-admin` on its own data dir and the fabric's store_dir under `dir`: the Day-0 root.
struct Day0 {
    dir: std::path::PathBuf,
    store: std::path::PathBuf,
    data_dir: std::path::PathBuf,
}

/// A running `rafka-node-admin`, killed by its exact pid when a failed assertion drops it.
struct Running {
    child: std::process::Child,
    pid: u32,
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Day0 {
    fn at(name: &str) -> Self {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        let store = dir.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let data_dir = dir.join("mesh1.admin.1");
        Self { dir, store, data_dir }
    }

    fn run(&self) -> Running {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-node-admin"));
        cmd.env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("RDM_DATA_DIR", &self.data_dir)
            .env("RDM_STORE_DIR", &self.store)
            .env("RDM_EVIDENCE_DIR", &self.dir)
            .env("MESH_SPAWN_TYPE", "process")
            .env("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0");
        let child = cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().expect("rafka-node-admin starts");
        let pid = child.id();
        Running { child, pid }
    }

    /// Stop `running` as a signal does, and the spans it left.
    fn stop(&self, mut running: Running) -> Vec<serde_json::Value> {
        unsafe { libc::kill(running.pid as i32, libc::SIGTERM) };
        let status = running.child.wait().unwrap();
        let mut err = String::new();
        std::io::Read::read_to_string(running.child.stderr.as_mut().unwrap(), &mut err).unwrap();
        assert!(status.success(), "the stop of {}: {status:?} {err}", running.pid);
        spans_of(&self.dir, running.pid)
    }

    /// The fabric's directory on the store root.
    fn fabric(&self) -> std::path::PathBuf {
        let dirs = fabric_dirs(&self.store);
        assert_eq!(dirs.len(), 1, "one fabric on the root: {dirs:?}");
        self.store.join(&dirs[0])
    }

    /// The ready-for-traffic row of the fabric, in the status rows of mesh1: the file the fabric event is put as.
    fn ready_rows(&self) -> Vec<std::path::PathBuf> {
        let dir = self.fabric().join("status").join("mesh1").join("fabric");
        std::fs::read_dir(dir).map(|d| d.filter_map(|e| e.ok().map(|e| e.path())).collect()).unwrap_or_default()
    }

    /// Day 0 to the end of the fabric's first climb: the Fabric row published at state-commit, the admin saying it
    /// reached ready-for-traffic, and, when `row` is set, the fabric event's row put on the root. Returns the
    /// running admin.
    fn day0_until_ready(&self, row: bool) -> Running {
        let running = self.run();
        let until = Instant::now() + Duration::from_secs(30);
        loop {
            let published = fabric_dirs(&self.store).first().is_some_and(|f| self.store.join(f).join("fabric").join("identity.json").exists());
            let said: Vec<String> = first_ready_reasons(&spans_of(&self.dir, running.pid));
            if published && said == ["ready-for-traffic"] && (!row || !self.ready_rows().is_empty()) {
                return running;
            }
            assert!(
                Instant::now() < until,
                "the Day-0 admin did not finish its first climb in 30 s: the Fabric row published {published}, it said {said:?}, the ready-for-traffic rows on the root {:?}",
                self.ready_rows()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// Why each `rdm.node_admin.fabric.update.via-first-ready` span in `spans` says the admin knows the fabric
/// first reached ready-for-traffic.
fn first_ready_reasons(spans: &[serde_json::Value]) -> Vec<String> {
    spans.iter().filter(|s| s["name"] == "rdm.node_admin.fabric.update.via-first-ready").map(|s| s["attributes"]["reason"].as_str().unwrap_or_default().to_string()).collect()
}

/// CONTRACT (fabric-ready-for-traffic.md: once the round's check-ins are in, "the fabric is
/// ready-for-traffic"; wire.md `ApplyFabricEvent`: "the fabric-primary applies a Fabric event at a mesh
/// primary"; status_storage.rs: a Fabric event is a keyed row of its mesh's status rows): the
/// fabric-primary applies the ready-for-traffic fabric event at every mesh primary, its own mesh's
/// included. On a one-mesh fabric the Day-0 admin is that primary: the row `status/mesh1/fabric/<fabric
/// id>-<hex of "ready-for-traffic">.json` stands on the root once the fabric first reaches
/// ready-for-traffic. What must NOT happen: the fact that the fabric first reached ready-for-traffic
/// written on every mesh but the fabric-primary's, so that no row says it on a fabric of one mesh.
#[test]
fn the_fabric_primary_puts_the_ready_for_traffic_row_on_its_own_mesh() {
    let day0 = Day0::at("the_fabric_primary_puts_the_ready_for_traffic_row_on_its_own_mesh");
    let running = day0.day0_until_ready(true);
    let spans = day0.stop(running);
    let rows = day0.ready_rows();
    assert_eq!(rows.len(), 1, "one ready-for-traffic row on the root: {rows:?}");
    let fabric = fabric_dirs(&day0.store).remove(0);
    let hex: String = "ready-for-traffic".bytes().map(|b| format!("{b:02x}")).collect();
    assert_eq!(rows[0].file_name().and_then(|n| n.to_str()), Some(format!("{fabric}-{hex}.json").as_str()), "the row is keyed by the fabric and the event");
    let put = spans.iter().find(|s| s["name"] == "rdm.node_admin.status.update.via-declare" && format!("{}", s["attributes"]["key"]).contains("FabricEventAt")).unwrap_or_else(|| panic!("the fabric-primary declared no fabric event: {:?}", spans.iter().map(|s| &s["name"]).collect::<Vec<_>>()));
    assert_eq!(put["attributes"]["via"].as_str(), Some("self"), "the Day-0 admin is its own mesh's primary: {put}");
    println!("trace_id={}", put["trace_id"].as_str().unwrap_or(""));
}

/// The config and tags of the inherited birth's row at mesh1.admin.1.
const INHERITED_CONFIG: [(&str, &str); 1] = [("zone", "a")];
const INHERITED_TAGS: [&str; 1] = ["edge"];

/// What a prepared root holds beside the fabric's own record and the accepted topology: a fabric in
/// state-commit (its Fabric row published, no ready-for-traffic row), or one that first reached
/// ready-for-traffic (the ready-for-traffic fabric event's row among its mesh's status rows).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum History {
    StateCommit,
    ReadyForTraffic,
}

/// A fabric's durable store root and the data dir of its one node-admin, prepared through the storage
/// adapters with no previous process (R-SB1): the Fabric row and the fabric's name claim, mesh1's row,
/// the admin's own row (fabric1, mesh1, `mesh1.admin.1`, its node key), the accepted Build of the root
/// topology in the admin's own Build journal and, for [`History::ReadyForTraffic`], the
/// ready-for-traffic row. The rows the fabric's state-commit publishes are held in the root's
/// scratchpad and published by it, as the fabric-primary's commit-state action does. The store names no
/// node but the admin itself: nothing is alive.
struct Prepared {
    dir: std::path::PathBuf,
    store: std::path::PathBuf,
    data_dir: std::path::PathBuf,
    fabric_id: rafka_node_admin_core::model::FabricId,
    mesh_id: rafka_node_admin_core::model::MeshId,
    node_id: rafka_node_admin_core::model::NodeId,
    /// The incarnation the own row says this birth last ran.
    incarnation: rafka_node_admin_core::model::IncarnationId,
    build_id: rafka_node_admin_core::build::BuildId,
}

impl Prepared {
    fn at(name: &str, history: History) -> Self {
        use rafka_node_admin_core::fabric_storage::FabricStorage as _;
        use rafka_node_admin_core::model::{EndpointId, FabricId, IncarnationId, MeshId, NodeId};
        use rafka_node_admin_core::status_storage::StatusStorage as _;
        use rafka_node_admin_core::storage::{MeshStorage as _, NodesStorage as _};
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        let store = dir.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let data_dir = dir.join("mesh1.admin.1");
        std::fs::create_dir_all(&data_dir).unwrap();
        let (fabric_id, mesh_id, node_id, incarnation, build_id) = (FabricId::mint(), MeshId::mint(), NodeId::mint(), IncarnationId::mint(), rafka_node_admin_core::build::BuildId::mint());
        let key = iroh::SecretKey::generate();
        std::fs::write(data_dir.join("node-key"), hex::encode(key.to_bytes())).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let root = rafka_node_admin_core::store_root::StoreRoot::open(&store, &fabric_id).unwrap();
            assert_eq!(root.writes_to(), "scratchpad", "an empty root holds its rows until the fabric's state-commit");
            let fabric = rafka_node_admin_core::fabric_storage::FileFabricStorage::open(&root, &incarnation).await.unwrap();
            let identity = rafka_node_admin_core::fabric_storage::FabricIdentity { fabric_id: fabric_id.clone(), name: "fabric1".into() };
            fabric.claim_identity(&identity).await.unwrap().expect("the fabric's name is free");
            let meshes = rafka_node_admin_core::storage::FileMeshStorage::open(&root, &incarnation).await.unwrap();
            meshes.claim_mesh(&rafka_node_admin_core::storage::MeshRecord { mesh_id: mesh_id.clone(), name: "mesh1".into(), dead: false }).await.unwrap().expect("mesh1's name is free");
            let nodes = rafka_node_admin_core::storage::FileNodesStorage::open(&data_dir, &root, "mesh1", &incarnation).await.unwrap();
            let own = rafka_node_admin_core::storage::NodeRecord {
                node_id: node_id.clone(),
                fabric_id: fabric_id.clone(),
                name: "mesh1.admin.1".parse().unwrap(),
                mesh_id: mesh_id.clone(),
                incarnation_id: incarnation.clone(),
                endpoint_id: EndpointId(key.public().to_string()),
                transport_addr: "127.0.0.1:1".parse().unwrap(),
                listeners: Vec::new(),
            };
            nodes.put_own(&own).await.unwrap();
            // The birth's row at its path.name and its contact among mesh1's members, as its mesh's writer
            // left them: the name is held by this node.id.
            nodes.put_contact(&own).await.unwrap();
            let row = rafka_node_admin_core::storage::NodeRow {
                name: own.name.clone(),
                node_id: node_id.clone(),
                status: rafka_node_admin_core::model::NodeStatus::ReadyForTraffic,
                config: INHERITED_CONFIG.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
                tags: INHERITED_TAGS.iter().map(|t| t.to_string()).collect(),
                allocated: None,
                renamed_from: None,
            };
            nodes.put_node_row(&row).await.unwrap().expect("mesh1.admin.1 is free");
            if history == History::ReadyForTraffic {
                let status = rafka_node_admin_core::status_storage::FileStatusStorage::open(&root, "mesh1", &incarnation).await.unwrap();
                status.put_fabric_event(&rafka_node_admin_core::status_storage::FabricEventRow { fabric_id: fabric_id.clone(), event: "ready-for-traffic".into() }).await.unwrap();
            }
            root.scratchpad().expect("held until the state-commit").publish().await.unwrap();
            let journal = rafka_node_admin_core::build_state::FileJournal::open(&data_dir).await.unwrap();
            let accepted = rafka_node_admin_core::build_state::BuildAccepted {
                build_id: build_id.clone(),
                topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", mesh_id.clone()),
                submitted_change: None,
                submitted_at_ms: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64,
            };
            rafka_node_admin_core::build_state::accept(&journal, &accepted, None, "mesh1.admin.1", "prepared-root").await.unwrap();
        });
        let prepared = Self { dir, store, data_dir, fabric_id, mesh_id, node_id, incarnation, build_id };
        assert_eq!(fabric_dirs(&prepared.store), vec![prepared.fabric_id.to_string()], "one fabric on the root");
        assert!(prepared.fabric_row().exists(), "the Fabric row stands on the root");
        assert_eq!(prepared.ready_rows().len(), usize::from(history == History::ReadyForTraffic), "{history:?}: the ready-for-traffic rows on the root");
        prepared
    }

    fn fabric_row(&self) -> std::path::PathBuf {
        self.store.join(self.fabric_id.to_string()).join("fabric").join("identity.json")
    }

    fn ready_rows(&self) -> Vec<std::path::PathBuf> {
        let dir = self.store.join(self.fabric_id.to_string()).join("status").join("mesh1").join("fabric");
        std::fs::read_dir(dir).map(|d| d.filter_map(|e| e.ok().map(|e| e.path())).collect()).unwrap_or_default()
    }

    /// The own row the admin wrote: its incarnation, fabric id and mesh id.
    fn own_row(&self) -> Option<(String, String, String)> {
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(self.data_dir.join("nodes").join("self.json")).ok()?).ok()?;
        let r = &v["record"];
        Some((r["incarnation_id"].as_str()?.to_string(), r["fabric_id"].as_str()?.to_string(), r["mesh_id"].as_str()?.to_string()))
    }

    /// The node.id the own row names.
    fn own_node_id(&self) -> Option<String> {
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(self.data_dir.join("nodes").join("self.json")).ok()?).ok()?;
        Some(v["record"]["node_id"].as_str()?.to_string())
    }

    /// Read through the storage adapters once the admin has ended: the row at mesh1.admin.1 and the
    /// node.ids mesh1's contacts name.
    fn rows_after(&self) -> (Option<rafka_node_admin_core::storage::NodeRow>, Vec<String>) {
        use rafka_node_admin_core::storage::NodesStorage as _;
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let root = rafka_node_admin_core::store_root::StoreRoot::open(&self.store, &self.fabric_id).unwrap();
            let reader = rafka_node_admin_core::model::IncarnationId::mint();
            let nodes = rafka_node_admin_core::storage::FileNodesStorage::open(&self.data_dir, &root, "mesh1", &reader).await.unwrap();
            let row = nodes.node_row_at(&"mesh1.admin.1".parse().unwrap()).await.unwrap();
            let contacts = nodes.contacts().await.unwrap().into_iter().map(|c| c.node_id.to_string()).collect();
            (row, contacts)
        })
    }

    /// Start a bootstrap `rafka-node-admin` on the prepared data dir and root, no fabric id handed in.
    fn start(&self) -> Running {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-node-admin"));
        cmd.env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("RDM_DATA_DIR", &self.data_dir)
            .env("RDM_STORE_DIR", &self.store)
            .env("RDM_EVIDENCE_DIR", &self.dir)
            .env("MESH_SPAWN_TYPE", "process")
            .env("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0");
        let child = cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().expect("rafka-node-admin starts");
        let pid = child.id();
        Running { child, pid }
    }

    /// The spans `running` has left once one named `name` is among them, within `within`.
    fn until(&self, running: &mut Running, name: &str, within: Duration) -> Vec<serde_json::Value> {
        let until = Instant::now() + within;
        loop {
            let spans = spans_of(&self.dir, running.pid);
            if spans.iter().any(|s| s["name"] == name) {
                return spans;
            }
            if let Some(status) = running.child.try_wait().unwrap() {
                let mut err = String::new();
                std::io::Read::read_to_string(running.child.stderr.as_mut().unwrap(), &mut err).unwrap();
                panic!("the admin started on the prepared root exited {status:?} before {name}: {err}");
            }
            let story: Vec<(&serde_json::Value, &serde_json::Value)> = spans
                .iter()
                .filter(|s| [INHERITED_ROOT, "rdm.mesh.entry.update.via-rafka-time-adopted", "rdm.mesh.entry.resolve.via-recovery-pull"].contains(&s["name"].as_str().unwrap_or_default()))
                .map(|s| (&s["name"], &s["attributes"]))
                .collect();
            assert!(Instant::now() < until, "no {name} in {} ms; its boot from its own row, its rafka-time adoption and its recovery pulls so far: {story:?}", within.as_millis());
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The end of the cell: the admin is sent the signal a teardown sends, and the spans it left.
    fn end(&self, mut running: Running) -> Vec<serde_json::Value> {
        unsafe { libc::kill(running.pid as i32, libc::SIGTERM) };
        let status = running.child.wait().unwrap();
        let mut err = String::new();
        std::io::Read::read_to_string(running.child.stderr.as_mut().unwrap(), &mut err).unwrap();
        assert!(status.success(), "the end of {}: {status:?} {err}", running.pid);
        spans_of(&self.dir, running.pid)
    }
}

/// The defined zero-contact recovery path (R-SB1; fabric-recovery.md "Nothing alive": "No node is alive to
/// read topology from or to ping"): the store names no node but this admin, so nothing is alive by the
/// store's own evidence. The admin adopts its own clock at once, `rdm.mesh.entry.update.via-rafka-time-adopted`
/// with `source = own-clock` and `reason = nothing-alive`, and pulls from no one: no
/// `rdm.mesh.entry.resolve.via-recovery-pull`. Returns the adoption span.
fn assert_zero_contact_recovery(spans: &[serde_json::Value]) -> serde_json::Value {
    let recovery = spans.iter().find(|s| s["name"] == INHERITED_ROOT).unwrap_or_else(|| panic!("the admin did not boot as a new node over its inherited root: {spans:?}"));
    assert_eq!(recovery["attributes"]["contacts"].as_str(), Some("0"), "the store names no other node (the inherited birth is no node to reach): {recovery}");
    let adopted: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.entry.update.via-rafka-time-adopted").collect();
    assert_eq!(adopted.len(), 1, "rafka-time adopted once: {adopted:?}");
    assert_eq!((adopted[0]["attributes"]["source"].as_str(), adopted[0]["attributes"]["reason"].as_str()), (Some("own-clock"), Some("nothing-alive")), "{}", adopted[0]);
    let pulls: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.entry.resolve.via-recovery-pull").collect();
    assert!(pulls.is_empty(), "nothing is alive to pull from: {pulls:?}");
    adopted[0].clone()
}

/// The boot of an admin started on an inherited root: a new node over the inherited data dir.
const INHERITED_ROOT: &str = "rdm.node_admin.node.create.via-inherited-root";
/// The accepted Build reloaded from the inherited data dir's own journal.
const BUILD_RELOADED: &str = "rdm.node_admin.fabric.update.via-inherited-root";

/// How long an admin started on a prepared root may take to boot: nothing it does waits on a node.
const BOOTS_WITHIN: Duration = Duration::from_secs(10);

/// CONTRACT (R-SB1, Luke 2026-10-13: "An admin started on an inherited root is fabric recovery: a new
/// node.id and incarnation over the inherited directory (fabric-recovery.md:10), seated by the ordinary
/// rules from its rows", "a root holding the fabric's own record and the accepted topology (the admin
/// boots from its own row, recovery mode, FabricId and MeshIds preserved)"; fabric-recovery.md: "The
/// relaunched node-admin is a new node over its inherited data dir: a new node.id and incarnation, the
/// same path.name and endpoint key, and a new port"; "Nothing alive": "The launched node-admin reuses the
/// fabric id and each MeshId it reads from the store, and mints none", "node.delete of the exited
/// instance, then node.create at the same path.name, with a new node.id over the inherited data dir", "A
/// node's name is released by node recovery's node.delete and taken by its node.create, which carries a
/// new node.id"; R-FI1: a node-admin's own row names the Fabric whose root it opens): an admin started on
/// a prepared root, with no fabric id handed in, reads the fabric id from its own row before the store
/// root opens (the storage step names that fabric id and mesh1's id, `inherited_root = true`) and boots
/// as a new node over it (`node.create.via-inherited-root`): a node.id and an incarnation it mints, the
/// inherited birth named beside them, no supersedes lineage, no contact. It deletes the inherited birth
/// (its row at mesh1.admin.1 `Deleted`, its contact dropped) and then claims mesh1.admin.1 under its own
/// node.id, the row's config and tags seeded from the Deleted row. It takes the zero-contact recovery path
/// at once, reloads the accepted Build from the inherited journal (`fabric.update.via-inherited-root`
/// naming that Build), writes its own row under its new node.id and incarnation with the same FabricId
/// and MeshId, and reaches Ready as that node. It mints no second fabric directory, and mesh1's row still
/// names its MeshId.
/// Must NOT happen: the inherited node.id or incarnation kept (a restart), a new fabric id or MeshId
/// minted, a Day-0 Build accepted, the inherited birth taken for a node to reach, a wait on a node that
/// is not there, or the name refused as held by the inherited birth.
#[test]
fn an_admin_started_on_a_prepared_root_boots_as_a_new_node_in_recovery_with_its_fabric_and_mesh_ids() {
    let root = Prepared::at("an_admin_started_on_a_prepared_root_boots_as_a_new_node", History::StateCommit);
    let mut running = root.start();
    let spans = root.until(&mut running, BUILD_RELOADED, BOOTS_WITHIN);
    let adopted = assert_zero_contact_recovery(&spans);
    let opened = spans.iter().find(|s| s["name"] == "rdm.mesh.node.resolve.via-storage-opened").unwrap_or_else(|| panic!("no storage step: {spans:?}"));
    let attr = |s: &serde_json::Value, k: &str| s["attributes"][k].as_str().unwrap_or_default().to_string();
    assert_eq!((attr(opened, "fabric_id"), attr(opened, "mesh_id"), attr(opened, "inherited_root")), (root.fabric_id.to_string(), root.mesh_id.to_string(), "true".to_string()), "{opened}");
    let recovery = spans.iter().find(|s| s["name"] == INHERITED_ROOT).unwrap();
    let node_id = attr(recovery, "node_id");
    let incarnation = attr(recovery, "incarnation_id");
    assert!(rafka_node_admin_core::model::NodeId::parse(&node_id).is_ok() && node_id != root.node_id.to_string(), "a new node.id: {recovery}");
    assert!(!incarnation.is_empty() && incarnation != root.incarnation.to_string(), "a new incarnation: {recovery}");
    assert_eq!(
        (attr(recovery, "node"), attr(recovery, "inherited_node_id"), attr(recovery, "inherited_incarnation_id"), attr(recovery, "fabric_id"), attr(recovery, "supersedes")),
        ("mesh1.admin.1".to_string(), root.node_id.to_string(), root.incarnation.to_string(), root.fabric_id.to_string(), String::new()),
        "the same path.name over the inherited birth, no supersedes lineage: {recovery}"
    );
    let deleted: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.delete.via-node-row" && attr(s, "subject") == "mesh1.admin.1").collect();
    assert_eq!(deleted.len(), 1, "the inherited birth's node.delete, once: {deleted:?}");
    assert_eq!((attr(deleted[0], "node_id"), attr(deleted[0], "outcome")), (root.node_id.to_string(), "row-deleted".to_string()), "{}", deleted[0]);
    let claimed: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.create.via-node-row" && attr(s, "subject") == "mesh1.admin.1").collect();
    assert_eq!(claimed.len(), 1, "this node's name claim, once: {claimed:?}");
    assert_eq!(
        (attr(claimed[0], "node_id"), attr(claimed[0], "moment"), attr(claimed[0], "seeded_from"), attr(claimed[0], "outcome")),
        (node_id.clone(), "claim".to_string(), root.node_id.to_string(), "row-written".to_string()),
        "{}",
        claimed[0]
    );
    assert!(deleted[0]["start_unix_nano"].as_u64() < claimed[0]["start_unix_nano"].as_u64(), "node.delete of the inherited birth, then the claim");
    for s in [deleted[0], claimed[0]] {
        assert_eq!(s["trace_id"], opened["trace_id"], "a step of the boot's own story: {s}");
    }
    let reloaded = spans.iter().find(|s| s["name"] == BUILD_RELOADED).unwrap();
    assert_eq!(attr(reloaded, "build_id"), root.build_id.to_string(), "the accepted Build reloaded from the inherited journal: {reloaded}");
    assert_eq!(reloaded["trace_id"], opened["trace_id"], "the Build reload is a step of the boot's own story");
    let day0: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.create.via-accepted").collect();
    assert!(day0.is_empty(), "a recovery accepts no Build of its own: {day0:?}");
    assert_eq!(adopted["trace_id"], opened["trace_id"], "the zero-contact path is the boot's own story");
    let until = Instant::now() + BOOTS_WITHIN;
    while root.own_row().is_none_or(|(i, _, _)| i == root.incarnation.to_string()) {
        assert!(Instant::now() < until, "the admin wrote no own row under a new incarnation");
        std::thread::sleep(Duration::from_millis(20));
    }
    let (own_incarnation, fabric_id, mesh_id) = root.own_row().unwrap();
    assert_eq!((root.own_node_id().unwrap_or_default(), own_incarnation), (node_id.clone(), incarnation.clone()), "the own row names the new node.id and incarnation");
    assert_eq!((fabric_id, mesh_id), (root.fabric_id.to_string(), root.mesh_id.to_string()), "the own row keeps the FabricId and MeshId");
    let spans = root.until(&mut running, "rdm.mesh.node.update.via-ready", BOOTS_WITHIN);
    let ready = spans.iter().find(|s| s["name"] == "rdm.mesh.node.update.via-ready").unwrap();
    assert_eq!(
        (attr(ready, "node_id"), attr(ready, "fabric_id"), attr(ready, "mesh_id"), attr(ready, "build_id")),
        (node_id.clone(), root.fabric_id.to_string(), root.mesh_id.to_string(), root.build_id.to_string()),
        "Ready as the new node in the prepared Fabric and mesh, holding the accepted Build: {ready}"
    );
    assert_eq!(ready["trace_id"], opened["trace_id"], "Ready ends the boot's own story");
    root.end(running);
    assert_eq!(fabric_dirs(&root.store), vec![root.fabric_id.to_string()], "no second fabric directory");
    let mesh_row: serde_json::Value = serde_json::from_slice(&std::fs::read(root.store.join(root.fabric_id.to_string()).join("meshes").join("mesh1.json")).unwrap()).unwrap();
    assert_eq!(mesh_row["record"]["mesh_id"].as_str(), Some(root.mesh_id.to_string().as_str()), "mesh1's row keeps its MeshId: {mesh_row}");
    let (row, contacts) = root.rows_after();
    let row = row.expect("a row at mesh1.admin.1");
    assert_eq!(row.node_id.to_string(), node_id, "mesh1.admin.1 is held by the new node.id: {row:?}");
    let config: Vec<(String, String)> = row.config.into_iter().collect();
    let tags: Vec<String> = row.tags.into_iter().collect();
    assert_eq!((config, tags), (INHERITED_CONFIG.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), INHERITED_TAGS.iter().map(|t| t.to_string()).collect()), "config and tags seeded from the Deleted row");
    assert!(!contacts.contains(&root.node_id.to_string()), "the inherited birth's contact is dropped: {contacts:?}");
    println!("trace_id={}", opened["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-SB1: "a root holding ready-for-traffic history (the first-ready latch is set at boot)";
/// states.md "The initial fabric before ready-for-traffic": "Once the fabric is ready-for-traffic these
/// are admitted as usual"): the durable fact that the fabric first reached ready-for-traffic is the
/// ready-for-traffic fabric event's row among its mesh's status rows. An admin started on a prepared root
/// holding that row knows it at boot, before any status is heard, and says why once
/// (`rdm.node_admin.fabric.update.via-first-ready`, `ready-for-traffic-row-on-store-root`), on the
/// zero-contact recovery path. What must NOT happen: the admin refusing a caller's workflow
/// `fabric-not-ready` because it heard pending, or learning the fact from anything but the row.
#[test]
fn an_admin_started_on_a_prepared_root_holding_the_ready_for_traffic_row_knows_at_boot_the_fabric_first_reached_it() {
    let root = Prepared::at("an_admin_started_on_a_prepared_root_holding_the_ready_for_traffic_row", History::ReadyForTraffic);
    let mut running = root.start();
    let spans = root.until(&mut running, BUILD_RELOADED, BOOTS_WITHIN);
    assert_zero_contact_recovery(&spans);
    let reasons = first_ready_reasons(&spans);
    assert_eq!(reasons, ["ready-for-traffic-row-on-store-root"], "the admin knows from the row, at boot, once");
    let said = spans.iter().find(|s| s["name"] == "rdm.node_admin.fabric.update.via-first-ready").unwrap();
    let reloaded = spans.iter().find(|s| s["name"] == BUILD_RELOADED).unwrap();
    assert!(said["start_unix_nano"].as_u64() <= reloaded["start_unix_nano"].as_u64(), "known before the boot reloads its Build: {said} {reloaded}");
    root.end(running);
    println!("trace_id={}", said["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-SB1: "a root whose fabric is in state-commit with no prior ready-for-traffic (the latch is
/// not set)"; states.md "The initial fabric before ready-for-traffic": "Until the initial fabric first
/// reaches ready-for-traffic it is pending, in state-sync or in state-commit, and nothing that moves a
/// seat or writes caller state runs"; store_root.rs: the Fabric row is published at the fabric's first
/// STATE-COMMIT): a prepared root holding the Fabric row and no ready-for-traffic row is a fabric that
/// has not first reached ready-for-traffic. An admin started on it, on the zero-contact recovery path,
/// does not know at boot that the fabric reached it: by the time it reloads its Build it has said no
/// first-ready from the store, and every first-ready it ever says is its own climb's
/// (`ready-for-traffic`). What must NOT happen: the Fabric row taken as the signal, so that a seat move or
/// a caller's workflow is admitted while the fabric is in state-commit.
#[test]
fn an_admin_started_on_a_prepared_root_whose_fabric_is_in_state_commit_does_not_know_it_reached_ready_for_traffic() {
    let root = Prepared::at("an_admin_started_on_a_prepared_root_whose_fabric_is_in_state_commit", History::StateCommit);
    let mut running = root.start();
    let spans = root.until(&mut running, BUILD_RELOADED, BOOTS_WITHIN);
    assert_zero_contact_recovery(&spans);
    assert!(first_ready_reasons(&spans).is_empty(), "the admin knew at boot: {:?}", first_ready_reasons(&spans));
    let spans = root.end(running);
    let reasons = first_ready_reasons(&spans);
    assert!(reasons.iter().all(|r| r == "ready-for-traffic"), "the admin knew from something other than the fabric reaching ready-for-traffic: {reasons:?}");
    let reloaded = spans.iter().find(|s| s["name"] == BUILD_RELOADED).unwrap();
    println!("trace_id={}", reloaded["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (R-MN1: "a mesh created again under the same name is a new mesh with a new id"; R-ML2: "a
/// dead mesh's path.name used again is mesh.create of a new mesh with a new MeshId", "a dead mesh is
/// never restarted"; states.md: a mesh id is never reissued; R-SB1: the cell starts an admin on a root
/// prepared through the storage adapters): an admin started on a prepared root reads its mesh's row by
/// its mesh's id, the one its own row holds, never by the name alone. mesh1's row on the root another
/// MeshId's (the mesh removed and created again under the name), the admin is refused by name, naming
/// both mesh ids, at its storage step; its own mesh's row Dead, it is refused by name. Must NOT happen:
/// the admin taking the recreated mesh's id as its own (the row at the name read as its mesh) and
/// joining it.
#[test]
fn an_admin_started_on_a_prepared_root_reads_its_mesh_row_by_its_mesh_id_and_never_joins_a_mesh_created_again_under_its_name() {
    let root = Prepared::at("an_admin_started_on_a_prepared_root_reads_its_mesh_row_by_its_mesh_id", History::StateCommit);
    let mesh_row = root.store.join(root.fabric_id.to_string()).join("meshes").join("mesh1.json");
    let held: serde_json::Value = serde_json::from_slice(&std::fs::read(&mesh_row).unwrap()).unwrap();
    let own_id = root.mesh_id.to_string();
    // A start against `row` at mesh1 on the root: refused by name at its storage step, before it writes
    // its own row again.
    let refused = |row: serde_json::Value, what: &str| -> (String, u32) {
        std::fs::write(&mesh_row, serde_json::to_vec(&row).unwrap()).unwrap();
        let mut running = root.start();
        let until = Instant::now() + BOOTS_WITHIN;
        loop {
            if let Some(status) = running.child.try_wait().unwrap() {
                let mut err = String::new();
                std::io::Read::read_to_string(running.child.stderr.as_mut().unwrap(), &mut err).unwrap();
                assert!(!status.success(), "{what}: the admin exited {status:?} without a refusal: {err}");
                return (err, running.pid);
            }
            assert!(root.own_row().is_some_and(|(i, _, _)| i == root.incarnation.to_string()), "{what}: the admin took the row at mesh1 as its mesh and booted (its own row rewritten)");
            assert!(Instant::now() < until, "{what}: the admin neither refused nor booted in {} ms", BOOTS_WITHIN.as_millis());
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    let recreated_id = rafka_node_admin_core::model::MeshId::mint().to_string();
    let (err, pid) = refused(serde_json::json!({"format": held["format"], "record": {"mesh_id": recreated_id, "name": "mesh1", "dead": false}}), "mesh1 created again");
    assert!(err.contains("refusing to start") && err.contains(&own_id) && err.contains(&recreated_id), "the refusal names this admin's mesh and the mesh created again under its name: {err}");
    let spans = spans_of(&root.dir, pid);
    let opened = spans.iter().find(|s| s["name"] == "rdm.mesh.node.resolve.via-storage-opened").unwrap_or_else(|| panic!("the refused start left no storage step: {spans:?}"));
    println!("trace_id={}", opened["trace_id"].as_str().unwrap_or(""));
    assert_eq!(opened["attributes"]["fabric_id"].as_str(), Some(root.fabric_id.to_string().as_str()), "{opened}");
    assert_eq!(opened["attributes"]["mesh_id"].as_str(), Some(own_id.as_str()), "the storage step names the mesh id the own row holds: {opened}");
    assert!(opened["attributes"]["error"].as_str().is_some_and(|e| e.contains(&recreated_id)), "the storage step names the refusal and the mesh created again: {opened}");
    let (err, _) = refused(serde_json::json!({"format": held["format"], "record": {"mesh_id": own_id, "name": "mesh1", "dead": true}}), "mesh1 Dead");
    assert!(err.contains("refusing to start") && err.contains(&own_id) && err.contains("Dead"), "the refusal names this admin's mesh as Dead: {err}");
}
