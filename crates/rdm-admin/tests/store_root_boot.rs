//! A node-admin's boot and the fabric's store root (R14 items 1 and 8): a node-admin process given
//! no `store_dir`, or one it cannot use, is refused by name at boot, naming the path and the cause;
//! nothing falls back to its data dir. An admin started on a root prepared through the storage adapters,
//! with no previous process, boots from its rows (R-SB1).

use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::node_rows::RecordAt as _;
use std::time::{Duration, Instant};

/// A bootstrap `rdm-node-admin` run on `env` until it exits: its exit status, its stderr and the
/// spans of its evidence file.
fn boot(name: &str, env: &[(&str, String)]) -> (std::process::ExitStatus, String, Vec<serde_json::Value>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join("i143-testkit").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let data_dir = dir.join("mesh1.admin.1");
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rdm-node-admin"));
    cmd.env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("RDM_DATA_DIR", &data_dir)
        .env("RDM_EVIDENCE_DIR", &dir)
        .env("MESH_SPAWN_TYPE", "process")
        .env("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let child = cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().expect("rdm-node-admin starts");
    let pid = child.id();
    let started = Instant::now();
    let out = child.wait_with_output().unwrap();
    assert!(started.elapsed() < Duration::from_secs(10), "a refused boot took {} ms", started.elapsed().as_millis());
    let prefix = format!("rdm-node-admin.{pid}-");
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
    let prefix = format!("rdm-node-admin.{pid}-");
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
        .filter(|e| e.path().is_dir() && e.file_name() != rdm_admin::store_root::FABRIC_NAMES)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

/// A bootstrap `rdm-node-admin` on its own data dir and the fabric's store_dir under `dir`: the Day-0 root.
struct Day0 {
    dir: std::path::PathBuf,
    store: std::path::PathBuf,
    data_dir: std::path::PathBuf,
}

/// A running `rdm-node-admin`, killed by its exact pid when a failed assertion drops it.
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
        let dir = std::env::temp_dir().join("i143-testkit").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        let store = dir.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let data_dir = dir.join("mesh1.admin.1");
        Self { dir, store, data_dir }
    }

    fn run(&self) -> Running {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rdm-node-admin"));
        cmd.env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("RDM_DATA_DIR", &self.data_dir)
            .env("RDM_STORE_DIR", &self.store)
            .env("RDM_EVIDENCE_DIR", &self.dir)
            .env("MESH_SPAWN_TYPE", "process")
            .env("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0");
        let child = cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().expect("rdm-node-admin starts");
        let pid = child.id();
        Running { child, pid }
    }

    /// Stop `running` as a signal does, and the spans it left. The admin leaves within its bound
    /// (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`, CLAUDE.md: a signal stop's in-flight handlers finish within
    /// it "before the node leaves"), then exits through its telemetry's exit flush (2 s); one that has not
    /// exited by both and a margin fails by name and is killed by its pid, never waited on for good.
    fn stop(&self, mut running: Running) -> Vec<serde_json::Value> {
        // The admin's environment is cleared (`run`): it holds the default deadline.
        let deadline = Duration::from_millis(5_000);
        let within = deadline + Duration::from_secs(2) + Duration::from_secs(8);
        unsafe { libc::kill(running.pid as i32, libc::SIGTERM) };
        let until = Instant::now() + within;
        let status = loop {
            if let Some(status) = running.child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < until, "the Day-0 admin {} did not exit within {within:?} of its SIGTERM (its connections-delete deadline {deadline:?}, its exit flush 2 s, a margin of 8 s)", running.pid);
            std::thread::sleep(Duration::from_millis(50));
        };
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

    /// Day 0 to the end of the fabric's first change: the Fabric row written at Day 0 and the
    /// fabric-primary entered ready. Returns the running admin.
    fn day0_until_ready(&self) -> Running {
        let running = self.run();
        let until = Instant::now() + Duration::from_secs(30);
        loop {
            let published = fabric_dirs(&self.store).first().is_some_and(|f| rdm_admin::store_log::read_record(&self.store.join(f).join(rdm_admin::store_root::FABRIC_FILE), "fabric-record", None).is_some());
            let ready = spans_of(&self.dir, running.pid).iter().any(|s| s["name"] == "rdm.node_admin.fabric.update.via-state-entered" && s["attributes"]["to"] == "ready");
            if published && ready {
                return running;
            }
            assert!(Instant::now() < until, "the Day-0 admin did not finish its first change in 30 s: the Fabric row published {published}, ready entered {ready}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
/// The durable writes of a Day-0 birth, by the span that names each: the Fabric row, mesh1's row, the
/// accepted root Build, and a node row put at an arrival.
const DAY0_WRITES: [&str; 3] = ["rdm.node_admin.fabric.create.via-fabric-row", "rdm.node_admin.mesh.create.via-mesh-row", "rdm.node_admin.build.create.via-accepted"];

/// CONTRACT (R-OW1, ruling text item 2: "Day 0: the bootstrap admin, as mesh1's primary, claims
/// (mesh1.admin, 1) [...] and writes its own row through the same validation and storage
/// path as any create, the first write of its birth, before Ready. No bypass; a conflicting holder is
/// NameHeld, never overwritten"; item 1: "The mesh-primary writes every row in its mesh, including its
/// own"): the Day-0 admin's first durable write is the claim of its own path.name, one
/// `rdm.node_admin.node.create.via-node-row` span with `subject = mesh1.admin.1`, its own node.id,
/// `moment = claim`, `outcome = row-written`, a step of its boot's own trace, started before the Fabric
/// row, mesh1's row, the accepted root Build and Ready. Its own birth's arrival in its first view finds the
/// row its node holds and writes nothing (`row-held-by-node`): one writer, one write. The row stands on the
/// store root at mesh1.admin.1 under the boot's node.id, the one the bootstrap's first step minted and
/// the process's deployment span carries from its start (Q208). What must NOT happen: a Day-0 fabric whose
/// primary holds no row of its own, so that `mesh1.admin.1` has no claimed name.
#[test]
fn the_day0_admin_claims_its_own_path_name_as_the_first_write_of_its_birth_before_ready() {
    let day0 = Day0::at("the_day0_admin_claims_its_own_path_name_as_the_first_write_of_its_birth");
    let running = day0.day0_until_ready();
    let spans = day0.stop(running);
    let attr = |s: &serde_json::Value, k: &str| s["attributes"][k].as_str().unwrap_or_default().to_string();
    let start = |s: &serde_json::Value| s["start_unix_nano"].as_u64().unwrap_or(u64::MAX);
    let boot = spans.iter().find(|s| s["name"] == "rdm.mesh.node.create.via-boot").unwrap_or_else(|| panic!("no boot span: {spans:?}"));
    let node_id = attr(boot, "node_id");
    assert_eq!(attr(boot, "node"), "mesh1.admin.1", "{boot}");
    // Q208: the bootstrap's first step minted the node id before the process's telemetry, so its first
    // boot span, the deployment's, opened before the boot reads anything, already carries it.
    let deployment = spans.iter().find(|s| s["name"] == "rdm.mesh.node.create.via-deployment").unwrap_or_else(|| panic!("no deployment span: {spans:?}"));
    assert!(rdm_admin::model::NodeId::parse(&node_id).is_ok(), "the boot runs as a minted node id: {boot}");
    assert_eq!(attr(deployment, "node_id"), node_id, "the process's first boot span carries the node id its boot runs as: {deployment}");
    let rows: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.create.via-node-row" && attr(s, "subject") == "mesh1.admin.1").collect();
    let claims: Vec<&serde_json::Value> = rows.iter().copied().filter(|s| attr(s, "moment") == "claim").collect();
    assert_eq!(claims.len(), 1, "the Day-0 admin claims its own path.name once: every node row span of mesh1.admin.1 {rows:?}");
    let claim = claims[0];
    assert_eq!(
        (attr(claim, "node"), attr(claim, "node_id"), attr(claim, "outcome")),
        ("mesh1.admin.1".to_string(), node_id.clone(), "row-written".to_string()),
        "the claim of its own name under its own node.id, written: {claim}"
    );
    assert_eq!(claim["trace_id"], boot["trace_id"], "the claim is a step of the boot's own story: {claim}");
    for name in DAY0_WRITES.iter().chain(["rdm.mesh.node.update.via-ready"].iter()) {
        let later = spans.iter().find(|s| s["name"] == *name).unwrap_or_else(|| panic!("Day 0 left no {name}: {:?}", spans.iter().map(|s| &s["name"]).collect::<Vec<_>>()));
        assert!(start(claim) < start(later), "the claim of mesh1.admin.1 starts before {name}: claim {claim} then {later}");
    }
    for arrival in rows.iter().filter(|s| attr(s, "moment") == "arrival") {
        assert!(start(claim) < start(arrival), "the claim precedes its birth's arrival: {arrival}");
        assert_eq!(attr(arrival, "outcome"), "row-held-by-node", "its own birth's arrival finds the claimed row and writes nothing: {arrival}");
    }
    let fabric_id = rdm_admin::model::FabricId::parse(&fabric_dirs(&day0.store).remove(0)).unwrap();
    let row = {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let root = rdm_admin::store_root::StoreRoot::open(&day0.store, &fabric_id).unwrap();
            let reader = rdm_admin::model::IncarnationId::mint();
            let nodes = rdm_admin::storage::FileNodesStorage::open(&day0.data_dir, &root, "mesh1", &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &reader)).await.unwrap();
            nodes.record_at(&"mesh1.admin.1".parse().unwrap()).await.unwrap()
        })
    };
    let row = row.expect("the store root holds the row at mesh1.admin.1");
    assert_eq!(row.node_id.to_string(), node_id, "mesh1.admin.1 is held by the Day-0 admin's node.id: {row:?}");
    println!("trace_id={}", boot["trace_id"].as_str().unwrap_or(""));
}

/// The config and tags of the inherited birth's row at mesh1.admin.1.
const INHERITED_CONFIG: [(&str, &str); 1] = [("zone", "a")];
const INHERITED_TAGS: [&str; 1] = ["edge"];

/// A fabric's durable store root and the data dir of its one node-admin, prepared through the storage
/// adapters with no previous process (R-SB1): the Fabric row and the fabric's name claim, mesh1's row,
/// the admin's own row (fabric1, mesh1, `mesh1.admin.1`, its node key), the accepted Build of the root
/// topology in the Build journal on the shared store root (R-BI6), each written straight to the root. Made
/// by [`Prepared::at`], the store names no node but the admin itself; made by [`Prepared::recovering`], it
/// also records the nodes the lost fabric-primary launched, none of them running.
struct Prepared {
    dir: std::path::PathBuf,
    store: std::path::PathBuf,
    data_dir: std::path::PathBuf,
    fabric_id: rdm_admin::model::FabricId,
    mesh_id: rdm_admin::model::MeshId,
    node_id: rdm_admin::model::NodeId,
    /// The incarnation the own row says this birth last ran.
    incarnation: rdm_admin::model::IncarnationId,
    build_id: rdm_admin::build::BuildId,
    /// The nodes the lost fabric-primary launched, as the store records them: path.name and node.id.
    launched: Vec<(String, rdm_admin::model::NodeId)>,
}

/// The nodes [`Prepared::recovering`] records the lost fabric-primary as having launched.
const LAUNCHED: [&str; 2] = ["mesh1.admin.2", "mesh1.rpc.1"];

/// A loopback address nothing listens on: a port the operating system assigned and that was let go.
fn unanswered_addr() -> std::net::SocketAddr {
    std::net::UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

impl Prepared {
    /// A store root that names no node but the admin's own lost birth.
    fn at(name: &str) -> Self {
        Self::prepare(name, &[])
    }

    /// A store root of a fabric whose fabric-primary launched nodes before it was lost: beside its own
    /// birth, the rows of [`LAUNCHED`], each Ready with an endpoint key and an address on its
    /// newest birth, none of them running.
    fn recovering(name: &str) -> Self {
        Self::prepare(name, &LAUNCHED)
    }

    fn prepare(name: &str, launched_names: &[&str]) -> Self {
        use rdm_admin::fabric_storage::FabricStorage as _;
        use rdm_admin::model::{EndpointId, FabricId, IncarnationId, MeshId, NodeId};
        use rdm_admin::storage::NodesStorage as _;
        let dir = std::env::temp_dir().join("i143-testkit").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        let store = dir.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let data_dir = dir.join("mesh1.admin.1");
        std::fs::create_dir_all(&data_dir).unwrap();
        let (fabric_id, mesh_id, node_id, incarnation, build_id) = (FabricId::mint(), MeshId::mint(), NodeId::mint(), IncarnationId::mint(), rdm_admin::build::BuildId::mint());
        let key = iroh::SecretKey::generate();
        std::fs::write(data_dir.join("node-key"), hex::encode(key.to_bytes())).unwrap();
        let launched = tokio::runtime::Runtime::new().unwrap().block_on(async {
            let root = rdm_admin::store_root::StoreRoot::open(&store, &fabric_id).unwrap();
            let fabric = rdm_admin::fabric_storage::FileFabricStorage::open(&root, &rdm_admin::store_log::LogWriter::new(&rdm_core::NodeId::mint(), &incarnation)).await.unwrap();
            let identity = rdm_admin::fabric_storage::FabricIdentity { fabric_id: fabric_id.clone(), name: "fabric1".into() };
            rdm_admin::fabric_storage::claim_identity(&fabric, &identity).await.unwrap().expect("the fabric's name is free");
            let meshes = rdm_admin::storage::FileMeshStorage::open(&root, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &incarnation)).await.unwrap();
            rdm_admin::storage::claim_mesh(&meshes, None, &rdm_admin::storage::MeshRecord { mesh_id: mesh_id.clone(), name: "mesh1".into(), dead: false, states: Default::default() }).await.unwrap().expect("mesh1's name is free");
            let nodes = rdm_admin::storage::FileNodesStorage::open(&data_dir, &root, "mesh1", &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &incarnation)).await.unwrap();
            let own = rdm_admin::storage::NodeRecord {
                node_id: node_id.clone(),
                fabric_id: fabric_id.clone(),
                name: "mesh1.admin.1".parse().unwrap(),
                mesh_id: mesh_id.clone(),
                incarnation_id: incarnation.clone(),
                endpoint_id: EndpointId(key.public().to_string()),
                transport_addr: "127.0.0.1:1".parse().unwrap(),
                listeners: Vec::new(),
                is_primary: None,
                is_fabric_primary: None,
                topology_storage_provider: None,
            };
            rdm_admin::storage::FileNodesStorage::put_own_in(&data_dir, &incarnation, &own).await.unwrap();
            let mut runtime = rdm_core::RuntimeFact::of_this_process(&rdm_admin::model::DeploymentId::mint().to_string()).unwrap();
            runtime.locator = rdm_core::runtime::RuntimeLocator::Process { pid: 1, start: u64::MAX };
            nodes
                .put_runtime(&rdm_admin::storage::RuntimeRow {
                    node_id: node_id.clone(),
                    name: own.name.clone(),
                    incarnation_id: incarnation.clone(),
                    runtime,
                    data_dir: Some(data_dir.display().to_string()),
                })
                .await
                .unwrap();
            // The birth's row at its path.name, its address on its newest birth, as its mesh's writer left
            // them: the name is held by this node.id.
            let mut birth = rdm_admin::storage::BirthFacts::new(incarnation.clone(), own.name.clone());
            (birth.endpoint_id, birth.transport_addr) = (Some(own.endpoint_id.clone()), Some(own.transport_addr));
            let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, writer: None,
                name: own.name.clone(),
                node_id: node_id.clone(),
                status: rdm_admin::model::NodeStatus::Ready,
                config: INHERITED_CONFIG.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
                tags: INHERITED_TAGS.iter().map(|t| t.to_string()).collect(),
                allocated: None,
                renamed_from: None,
                app_types: None, inherited_by: None,
                executable: None,
                births: vec![birth],
                predecessor: None,
                mesh_id: Some(mesh_id.clone()),
                storage_persistence: None,
                placement: None, revision: 0,
            };
            nodes.write_record(&row).await.unwrap();
            let mut launched = Vec::new();
            for name in launched_names {
                let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
                let mut birth = rdm_admin::storage::BirthFacts::new(incarnation, name.parse().unwrap());
                (birth.endpoint_id, birth.transport_addr) = (Some(EndpointId(iroh::SecretKey::generate().public().to_string())), Some(unanswered_addr()));
                let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, writer: None,
                    name: name.parse().unwrap(),
                    node_id: node_id.clone(),
                    status: rdm_admin::model::NodeStatus::Ready,
                    config: Default::default(),
                    tags: Default::default(),
                    allocated: None,
                    renamed_from: None,
                    app_types: None, inherited_by: None,
                    executable: None,
                    births: vec![birth],
                    predecessor: None,
                    mesh_id: Some(mesh_id.clone()),
                    storage_persistence: None,
                    placement: None, revision: 0,
                };
                nodes.write_record(&row).await.unwrap();
                launched.push((name.to_string(), node_id));
            }
            let journal = rdm_admin::build_state::FileJournal::open_on_root(&root, &data_dir, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &incarnation)).await.unwrap();
            let accepted = rdm_admin::build_state::BuildAccepted {
                build_id: build_id.clone(),
                topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", mesh_id.clone()),
                submitted_change: None,
                submitted_at_ms: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64,
            };
            rdm_admin::build_state::accept(&journal, &accepted, None, "mesh1.admin.1", "prepared-root").await.unwrap();
            launched
        });
        let prepared = Self { dir, store, data_dir, fabric_id, mesh_id, node_id, incarnation, build_id, launched };
        assert_eq!(fabric_dirs(&prepared.store), vec![prepared.fabric_id.to_string()], "one fabric on the root");
        assert!(rdm_admin::store_log::read_record(&prepared.fabric_row(), "fabric-record", None).is_some(), "the Fabric row stands on the root");
        assert!(rdm_admin::store_log::read_record(&prepared.store.join(prepared.fabric_id.to_string()).join(rdm_admin::store_root::BUILDS_FILE), "build-record", Some(&format!("{}/accepted", prepared.build_id))).is_some(), "the accepted Build stands on the shared store root as its own record");
        assert!(!prepared.data_dir.join("fabric").exists(), "no per-admin Build journal in data_dir");
        prepared
    }

    fn fabric_row(&self) -> std::path::PathBuf {
        self.store.join(self.fabric_id.to_string()).join(rdm_admin::store_root::FABRIC_FILE)
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
    /// node.ids whose live records' newest births carry an address, every mesh's.
    fn rows_after(&self) -> (Option<rdm_admin::storage::NodeRow>, Vec<String>) {
        use rdm_admin::storage::NodesStorage as _;
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let root = rdm_admin::store_root::StoreRoot::open(&self.store, &self.fabric_id).unwrap();
            let reader = rdm_admin::model::IncarnationId::mint();
            let nodes = rdm_admin::storage::FileNodesStorage::open(&self.data_dir, &root, "mesh1", &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &reader)).await.unwrap();
            let row = nodes.record_at(&"mesh1.admin.1".parse().unwrap()).await.unwrap();
            let meshes = rdm_admin::storage::FileMeshStorage::open(&root, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &reader)).await.unwrap();
            let contacts = rdm_admin::storage::live_records(&meshes, &nodes, "mesh1").await.unwrap().into_iter().filter(|r| r.newest_birth().is_some_and(|b| b.transport_addr.is_some())).map(|r| r.node_id.to_string()).collect();
            (row, contacts)
        })
    }

    /// Start a bootstrap `rdm-node-admin` on the prepared data dir and root, no fabric id handed in.
    fn start(&self) -> Running {
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rdm-node-admin"));
        cmd.env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("RDM_DATA_DIR", &self.data_dir)
            .env("RDM_STORE_DIR", &self.store)
            .env("RDM_EVIDENCE_DIR", &self.dir)
            .env("MESH_SPAWN_TYPE", "process")
            .env("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0");
        // The collector the run exports to (the gate's), as an estate passes it: the admin's boot reads in Jaeger.
        if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            cmd.env("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint);
        }
        let child = cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().expect("rdm-node-admin starts");
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
            // Every span the admin closed, in order, with its duration: what its boot was doing when the wait ran out.
            let closed: Vec<String> = {
                let mut v: Vec<(u64, String)> = spans.iter().map(|s| {
                    let st = s["start_unix_nano"].as_u64().unwrap_or(0);
                    let ms = s["end_unix_nano"].as_u64().unwrap_or(st).saturating_sub(st) / 1_000_000;
                    (st, format!("{} {}ms {}", s["name"].as_str().unwrap_or_default(), ms, s["attributes"].get("outcome").and_then(|o| o.as_str()).unwrap_or_default()))
                }).collect();
                v.sort();
                v.into_iter().map(|(_, l)| l).collect()
            };
            assert!(Instant::now() < until, "no {name} in {} ms; its boot from its own row, its rafka-time adoption and its recovery pulls so far: {story:?}; every span it closed: {closed:#?}", within.as_millis());
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
        // Shown with any later failure of the cell: an exit flush that ended at its bound names its loss here.
        let lines: Vec<&str> = err.lines().collect();
        let named: Vec<&str> = lines.iter().copied().filter(|l| l.contains("telemetry")).collect();
        eprintln!("the admin's telemetry lines at its end ({}): {named:#?}; its last stderr lines: {:#?}", running.pid, &lines[lines.len().saturating_sub(20)..]);
        spans_of(&self.dir, running.pid)
    }
}

/// The recovery of a fabric whose recorded nodes do not run (fabric-recovery.md "Nothing alive": "No
/// node is alive to read topology from or to ping"; "When the recovery root contains no other node from
/// which to obtain rafka-time, the launched node-admin adopts its own clock"): the admin's contacts are
/// exactly the nodes the store records it launched, its own lost birth never among them; it pulls from
/// them (`rdm.mesh.entry.resolve.via-recovery-pull`), no call of the boot targets its own lost birth, and
/// with none answering it adopts its own clock once (`source = own-clock`). Returns the adoption span.
fn assert_recovery_reaches_the_recorded_nodes(spans: &[serde_json::Value], root: &Prepared) -> serde_json::Value {
    let recovery = spans.iter().find(|s| s["name"] == INHERITED_ROOT).unwrap_or_else(|| panic!("the admin did not boot as a new node over its inherited root: {spans:?}"));
    assert_eq!(recovery["attributes"]["contacts"].as_str(), Some(root.launched.len().to_string().as_str()), "the contacts are the nodes the store records, the inherited birth not among them: {recovery}");
    let pulls: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.entry.resolve.via-recovery-pull").collect();
    assert!(!pulls.is_empty(), "the admin pulls from the recorded nodes: no recovery pull in {spans:?}");
    let targets: Vec<String> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.request.update.via-call").filter_map(|s| s["attributes"]["target"].as_str().map(str::to_string)).collect();
    for (name, id) in &root.launched {
        assert!(targets.iter().any(|t| t.contains(&id.to_string())), "the admin called {name} ({id}), a node the store records: {targets:?}");
    }
    assert!(!targets.iter().any(|t| t.contains(&root.node_id.to_string())), "no call targets the admin's own lost birth {}: {targets:?}", root.node_id);
    let adopted: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.entry.update.via-rafka-time-adopted").collect();
    assert_eq!(adopted.len(), 1, "rafka-time adopted once: {adopted:?}");
    assert_eq!(adopted[0]["attributes"]["source"].as_str(), Some("own-clock"), "no recorded node answers: {}", adopted[0]);
    adopted[0].clone()
}

/// The boot of an admin started on an inherited root: a new node over the inherited data dir.
const INHERITED_ROOT: &str = "rdm.node_admin.node.create.via-inherited-root";
/// The accepted Build reloaded from the inherited data dir's own journal.
const BUILD_RELOADED: &str = "rdm.node_admin.fabric.update.via-inherited-root";

/// How long an admin started on a prepared root may take to boot: nothing it does waits on a node.
const BOOTS_WITHIN: Duration = Duration::from_secs(10);
/// How long an admin started on a root recording nodes that do not run may take to boot: before its first
/// mutation it asks the recorded node-admins whether one answers (Q156, Q199), one probe each at once
/// within the default call budget (10 s), then pulls rafka-time from the recorded nodes at once within one
/// read's budget (10 s), each unanswered; and a margin.
const RECOVERS_WITHIN: Duration = Duration::from_secs(30);

/// CONTRACT (R-SB1, Luke 2026-10-13: "An admin started on an inherited root is fabric recovery: a new
/// node.id and incarnation over the inherited directory (fabric-recovery.md:10), seated by the ordinary
/// rules from its rows", "a root holding the fabric's own record and the accepted topology (the admin
/// boots from its own row, recovery mode, FabricId and MeshIds preserved)"; fabric-recovery.md: "The
/// relaunched node-admin is a new node over its inherited data dir: a new node.id and incarnation, the
/// same path.name and endpoint key, and a new port"; "Nothing alive": "The launched node-admin reuses the
/// fabric id and each MeshId it reads from the store, and mints none", "node.delete of the exited
/// instance, then node.create at the same path.name, with a new node.id over the inherited data dir", "A
/// node's name is released by node recovery's node.delete and taken by its node.create, which carries a
/// new node.id"; R-FI1: a node-admin's own row names the Fabric whose root it opens; Luke 2026-10-26: a
/// recovery is of a fabric-primary that had launched nodes before it died): an admin started on a
/// prepared root recording the nodes its lost birth launched (mesh1.admin.2 and mesh1.rpc.1, not
/// running), with no fabric id handed in, reads the fabric id from its own row before the store
/// root opens (the storage step names that fabric id and mesh1's id, `inherited_root = true`) and boots
/// as a new node over it (`node.create.via-inherited-root`): a node.id and an incarnation it mints, the
/// inherited birth named beside them, no supersedes lineage, its contacts the recorded nodes. It deletes the inherited birth
/// (its row at mesh1.admin.1 `Deleted`, so its birth's address names no node) and then claims mesh1.admin.1 under its own
/// node.id, the row's config and tags seeded from the Deleted row. It pulls from the recorded nodes, none
/// answering, and adopts its own clock, reloads the accepted Build from the inherited journal (`fabric.update.via-inherited-root`
/// naming that Build), writes its own row under its new node.id and incarnation with the same FabricId
/// and MeshId, and reaches Ready as that node. It mints no second fabric directory, and mesh1's row still
/// names its MeshId.
/// Must NOT happen: the inherited node.id or incarnation kept (a restart), a new fabric id or MeshId
/// minted, a Day-0 Build accepted, the inherited birth taken for a node to reach or called, or the name
/// refused as held by the inherited birth.
#[test]
fn an_admin_started_on_a_prepared_root_boots_as_a_new_node_in_recovery_with_its_fabric_and_mesh_ids() {
    let root = Prepared::recovering("an_admin_started_on_a_prepared_root_boots_as_a_new_node");
    let mut running = root.start();
    // The inherited-root step closes once its check before its first mutation and its claim are done,
    // after the Build reload; the rafka-time adoption follows its pulls.
    let _ = root.until(&mut running, BUILD_RELOADED, RECOVERS_WITHIN);
    let _ = root.until(&mut running, INHERITED_ROOT, RECOVERS_WITHIN);
    let _ = root.until(&mut running, "rdm.mesh.entry.update.via-rafka-time-adopted", RECOVERS_WITHIN);
    // The deployment span closes, and reaches the evidence file, only once the boot returns
    // (entry.rs), which can be after the rafka-time adoption's span: read the spans once it is there.
    let spans = root.until(&mut running, "rdm.mesh.node.create.via-deployment", RECOVERS_WITHIN);
    let opened = spans.iter().find(|s| s["name"] == "rdm.mesh.node.resolve.via-storage-opened").unwrap_or_else(|| panic!("no storage step: {spans:?}"));
    let attr = |s: &serde_json::Value, k: &str| s["attributes"][k].as_str().unwrap_or_default().to_string();
    assert_eq!((attr(opened, "fabric_id"), attr(opened, "mesh_id"), attr(opened, "inherited_root")), (root.fabric_id.to_string(), root.mesh_id.to_string(), "true".to_string()), "{opened}");
    let recovery = spans.iter().find(|s| s["name"] == INHERITED_ROOT).unwrap();
    let node_id = attr(recovery, "node_id");
    let incarnation = attr(recovery, "incarnation_id");
    assert!(rdm_admin::model::NodeId::parse(&node_id).is_ok() && node_id != root.node_id.to_string(), "a new node.id: {recovery}");
    // Q208: the first step minted that new node.id, never the inherited row's, before the telemetry.
    let deployment = spans.iter().find(|s| s["name"] == "rdm.mesh.node.create.via-deployment").unwrap_or_else(|| panic!("no deployment span: {spans:?}"));
    assert_eq!(attr(deployment, "node_id"), node_id, "the process's first boot span carries the new node.id: {deployment}");
    assert!(!incarnation.is_empty() && incarnation != root.incarnation.to_string(), "a new incarnation: {recovery}");
    assert_eq!(
        (attr(recovery, "node"), attr(recovery, "inherited_node_id"), attr(recovery, "inherited_incarnation_id"), attr(recovery, "fabric_id"), attr(recovery, "supersedes")),
        ("mesh1.admin.1".to_string(), root.node_id.to_string(), root.incarnation.to_string(), root.fabric_id.to_string(), String::new()),
        "the same path.name over the inherited birth, no supersedes lineage: {recovery}"
    );
    let deleted: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.delete.via-node-row" && attr(s, "subject") == "mesh1.admin.1").collect();
    assert_eq!(deleted.len(), 1, "the inherited birth's node.delete, once: {deleted:?}");
    assert_eq!((attr(deleted[0], "node_id"), attr(deleted[0], "outcome")), (root.node_id.to_string(), "row-deleted".to_string()), "{}", deleted[0]);
    let claimed: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.create.via-node-row" && attr(s, "subject") == "mesh1.admin.1" && attr(s, "moment") == "claim").collect();
    assert_eq!(claimed.len(), 1, "this node's name claim, once: {claimed:?}");
    assert_eq!(
        (attr(claimed[0], "node_id"), attr(claimed[0], "moment"), attr(claimed[0], "seeded_from"), attr(claimed[0], "outcome")),
        (node_id.clone(), "claim".to_string(), root.node_id.to_string(), "row-seeded".to_string()),
        "{}",
        claimed[0]
    );
    assert!(deleted[0]["start_unix_nano"].as_u64() < claimed[0]["start_unix_nano"].as_u64(), "node.delete of the inherited birth, then the claim");
    for s in [deleted[0], claimed[0]] {
        assert_eq!(s["trace_id"], opened["trace_id"], "a step of the boot's own story: {s}");
    }
    let reloaded = spans.iter().find(|s| s["name"] == BUILD_RELOADED).unwrap();
    assert_eq!(attr(reloaded, "build_id"), root.build_id.log().to_string(), "the accepted Build reloaded from the inherited journal: {reloaded}");
    assert_eq!(reloaded["trace_id"], opened["trace_id"], "the Build reload is a step of the boot's own story");
    let day0: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.create.via-accepted").collect();
    assert!(day0.is_empty(), "a recovery accepts no Build of its own: {day0:?}");
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
    // The evidence file is complete once the admin has exited: the recovery's pulls and calls are read
    // from it whole.
    let ended = root.end(running);
    let adopted = assert_recovery_reaches_the_recorded_nodes(&ended, &root);
    assert_eq!(adopted["trace_id"], opened["trace_id"], "the rafka-time adoption is the boot's own story");
    assert_eq!(fabric_dirs(&root.store), vec![root.fabric_id.to_string()], "no second fabric directory");
    let mesh_row = rdm_admin::store_log::read_record(&root.store.join(root.fabric_id.to_string()).join(rdm_admin::store_root::MESHES_FILE), "mesh-record", Some(&root.mesh_id.to_string())).expect("mesh1's row");
    assert_eq!(mesh_row["record"]["mesh_id"].as_str(), Some(root.mesh_id.to_string().as_str()), "mesh1's row keeps its MeshId: {mesh_row}");
    let (row, contacts) = root.rows_after();
    let row = row.expect("a row at mesh1.admin.1");
    assert_eq!(row.node_id.to_string(), node_id, "mesh1.admin.1 is held by the new node.id: {row:?}");
    let config: Vec<(String, String)> = row.config.into_iter().collect();
    let tags: Vec<String> = row.tags.into_iter().collect();
    assert_eq!((config, tags), (INHERITED_CONFIG.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(), INHERITED_TAGS.iter().map(|t| t.to_string()).collect()), "config and tags seeded from the Deleted row");
    assert!(!contacts.contains(&root.node_id.to_string()), "the inherited birth is no node to reach: its record is Deleted: {contacts:?}");
    println!("trace_id={}", opened["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (Q200, Luke 2026-10-26; fabric-recovery.md "Nothing alive": "A store that names no node but
/// the launching admin's own lost birth holds no fabric to recover: the launch deletes that storage and
/// starts as Day 0, a new fabric-primary."): an admin started on its own data dir, with no fabric id
/// handed in, whose store root names no node but its own lost birth, deletes that fabric's storage (its
/// directory on the store root and its own row in its data dir) and starts as Day 0: it accepts the root
/// Build of a new fabric (`build.create.via-accepted`), claims mesh1.admin.1 under a node.id it mints,
/// writes its own row naming a new fabric id and MeshId, and reaches Ready; the store root then holds that
/// one new fabric. Must NOT happen: a recovery boot over the inherited root (`node.create.via-inherited-root`),
/// the deleted fabric's directory left on the store root, or its fabric id, MeshId or node.id reused.
#[test]
fn a_launch_whose_store_names_no_node_but_its_own_lost_birth_deletes_that_storage_and_starts_as_day0() {
    let root = Prepared::at("a_launch_whose_store_names_no_node_but_its_own_lost_birth");
    let mut running = root.start();
    let spans = root.until(&mut running, "rdm.mesh.node.update.via-ready", BOOTS_WITHIN);
    let names: Vec<&serde_json::Value> = spans.iter().map(|s| &s["name"]).collect();
    assert!(!spans.iter().any(|s| s["name"] == INHERITED_ROOT), "no recovery boot over a store that names no node but the lost birth: {names:?}");
    assert!(spans.iter().any(|s| s["name"] == "rdm.node_admin.build.create.via-accepted"), "Day 0 accepts the root Build of a new fabric: {names:?}");
    let (_, fabric_id, mesh_id) = root.own_row().expect("the Day-0 admin's own row");
    let node_id = root.own_node_id().unwrap_or_default();
    assert!(fabric_id != root.fabric_id.to_string() && mesh_id != root.mesh_id.to_string() && node_id != root.node_id.to_string(), "a new fabric id, MeshId and node.id: own row {fabric_id} {mesh_id} {node_id}");
    root.end(running);
    assert_eq!(fabric_dirs(&root.store), vec![fabric_id.clone()], "the store root holds the new fabric alone: the lost birth's fabric {} is deleted", root.fabric_id);
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
    let root = Prepared::recovering("an_admin_started_on_a_prepared_root_reads_its_mesh_row_by_its_mesh_id");
    let fabric_root = root.store.join(root.fabric_id.to_string());
    let meshes = fabric_root.join(rdm_admin::store_root::MESHES_FILE);
    let name_claim = fabric_root.join("locks").join("name").join("mesh1");
    let held = rdm_admin::store_log::read_record(&meshes, "mesh-record", Some(&root.mesh_id.to_string())).expect("mesh1's record");
    let claim: serde_json::Value = serde_json::from_slice(&std::fs::read(&name_claim).unwrap()).unwrap();
    let own_id = root.mesh_id.to_string();
    // A start against the rows `write` puts on the root: refused by name at its storage step, before it
    // writes its own row again.
    let refused = |write: &dyn Fn(), what: &str| -> (String, u32) {
        write();
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
    let recreated_id = rdm_admin::model::MeshId::mint().to_string();
    // mesh1 created again: its record at its own id, and the name's claim naming it.
    append_record(&meshes, held["format"].as_str().unwrap(), &recreated_id, serde_json::json!({"mesh_id": recreated_id, "name": "mesh1", "dead": false}));
    let (err, pid) = refused(&|| std::fs::write(&name_claim, serde_json::to_vec(&serde_json::json!({"format": claim["format"], "record": {"name": "mesh1", "mesh_id": recreated_id}, "by": claim["by"]})).unwrap()).unwrap(), "mesh1 created again");
    assert!(err.contains("refusing to start") && err.contains(&own_id) && err.contains(&recreated_id), "the refusal names this admin's mesh and the mesh created again under its name: {err}");
    let spans = spans_of(&root.dir, pid);
    let opened = spans.iter().find(|s| s["name"] == "rdm.mesh.node.resolve.via-storage-opened").unwrap_or_else(|| panic!("the refused start left no storage step: {spans:?}"));
    println!("trace_id={}", opened["trace_id"].as_str().unwrap_or(""));
    assert_eq!(opened["attributes"]["fabric_id"].as_str(), Some(root.fabric_id.to_string().as_str()), "{opened}");
    assert_eq!(opened["attributes"]["mesh_id"].as_str(), Some(own_id.as_str()), "the storage step names the mesh id the own row holds: {opened}");
    assert!(opened["attributes"]["error"].as_str().is_some_and(|e| e.contains(&recreated_id)), "the storage step names the refusal and the mesh created again: {opened}");
    std::fs::write(&name_claim, serde_json::to_vec(&claim).unwrap()).unwrap();
    let (err, _) = refused(&|| append_record(&meshes, held["format"].as_str().unwrap(), &own_id, serde_json::json!({"mesh_id": own_id, "name": "mesh1", "dead": true, "states": ["Dead"]})), "mesh1 Dead");
    assert!(err.contains("refusing to start") && err.contains(&own_id) && err.contains("Dead"), "the refusal names this admin's mesh as Dead: {err}");
}

/// Append `record` as one line of the store file `file`, as another writer would.
fn append_record(file: &std::path::Path, format: &str, key: &str, record: serde_json::Value) {
    let op = rdm_admin::store_log::Op { node_id: rdm_admin::model::NodeId::mint().to_string(), incarnation_id: rdm_admin::model::IncarnationId::mint().to_string(), seq: 1 };
    let line = rdm_admin::store_log::encode_line(&op, format, key, &serde_json::to_vec(&record).unwrap());
    std::io::Write::write_all(&mut std::fs::OpenOptions::new().append(true).open(file).unwrap(), &line).unwrap();
}

// ---------------------------------------------------------------------------------------------------
// The adoption launch (Q156; fabric-recovery.md "Nothing alive"): a node-admin launched at the lost
// fabric-primary's path.name with no launcher, given the fabric id, its mesh and the store root.
// ---------------------------------------------------------------------------------------------------

/// The bound of an adoption launch's refusal, or of its claim and its first writes.
const ADOPTS_WITHIN: Duration = Duration::from_secs(40);

/// The span an adoption launch's claim of the fabric-recovery attempt leaves.
const ADOPTION_CLAIM: &str = "rdm.node_admin.fabric.update.via-adoption-claim";

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

/// The custody a person holds and hands an adoption launch in its environment (fabric-certs.md §Custody):
/// the fabric's root, the fabric CA under it, and mesh1's issuing material minted under that CA.
fn custody_env(root: rdm_core::certs::SigningKey, ca: rdm_core::certs::CaKey) -> Vec<(&'static str, String)> {
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(now_ms());
    let m = rdm_admin::certs::CertIssuer::with_root(root, ca, None, time).issuing_material("mesh1").expect("mesh1's issuing material").expect("a root holder hands issuing material");
    vec![("RDM_CA_SECRET", m.root.0), ("RDM_FABRIC_CA_KEY", m.ca_key.0), ("RDM_ISSUING_KEY", m.key.0), ("RDM_ISSUING_CERT", m.cert)]
}

/// Custody under a root minted now: what a person holds for a prepared root, which holds no cert material.
fn minted_custody() -> Vec<(&'static str, String)> {
    let root = rdm_core::certs::mint_key();
    let ca = rdm_core::certs::CaKey::mint(&root, rdm_core::certs::CA_CERT_TTL_SECS, now_ms());
    custody_env(root, ca)
}

/// The custody the data dir `dir` of a running fabric's Day-0 admin keeps (`ca-secret`, `fabric-ca`).
fn custody_of(dir: &std::path::Path) -> Vec<(&'static str, String)> {
    let root = rdm_core::certs::decode_key(std::fs::read_to_string(dir.join("ca-secret")).unwrap().trim()).unwrap();
    let kept = std::fs::read_to_string(dir.join("fabric-ca")).unwrap();
    let mut lines = kept.lines();
    let (key, cert) = (lines.next().unwrap(), lines.next().unwrap());
    let ca = rdm_core::certs::CaKey::from_parts(rdm_core::certs::decode_key(key).unwrap(), rdm_core::certs::decode_ca_cert(cert).unwrap());
    custody_env(root, ca)
}

/// An adoption launch: `rdm-node-admin` at mesh1.admin.1 with no launcher, given `fabric_id`, mesh1 and
/// `store`, on a fresh data dir `dir/<data_dir>`, with `extra` beside (a later value replaces an earlier).
fn adopter(dir: &std::path::Path, data_dir: &str, store: &std::path::Path, fabric_id: &str, extra: &[(&str, String)]) -> Running {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rdm-node-admin"));
    cmd.env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("RDM_DATA_DIR", dir.join(data_dir))
        .env("RDM_STORE_DIR", store)
        .env("RDM_EVIDENCE_DIR", dir)
        .env("MESH_SPAWN_TYPE", "process")
        .env("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0")
        .env("RDM_NODE_NAME", "mesh1.admin.1")
        .env("RDM_FABRIC_ID", fabric_id)
        .env("RDM_MESH", "mesh1");
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let child = cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().expect("rdm-node-admin starts");
    let pid = child.id();
    Running { child, pid }
}

/// `running`'s exit status and stderr once it exits within `within`; `None` while it still runs then.
fn exited_within(running: &mut Running, within: Duration) -> Option<(std::process::ExitStatus, String)> {
    let until = Instant::now() + within;
    loop {
        if let Some(status) = running.child.try_wait().unwrap() {
            let mut err = String::new();
            std::io::Read::read_to_string(running.child.stderr.as_mut().unwrap(), &mut err).unwrap();
            return Some((status, err));
        }
        if Instant::now() >= until {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The adoption claim span `pid` left under `dir`, once one is there within `within`.
fn claim_of(dir: &std::path::Path, pid: u32, within: Duration) -> Option<serde_json::Value> {
    let until = Instant::now() + within;
    loop {
        if let Some(s) = spans_of(dir, pid).into_iter().find(|s| s["name"] == ADOPTION_CLAIM) {
            return Some(s);
        }
        if Instant::now() >= until {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn attr_of(s: &serde_json::Value, k: &str) -> String {
    s["attributes"][k].as_str().unwrap_or_default().to_string()
}

impl Prepared {
    /// [`Prepared::at`] as a fabric whose one admin, the fabric-primary, is lost: its birth's row records
    /// it its mesh's primary and the fabric-primary, and the Fabric record names its provider, `process`.
    fn adoptable(name: &str) -> Self {
        let root = Self::at(name);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let store = rdm_admin::store_root::StoreRoot::open(&root.store, &root.fabric_id).unwrap();
            let writer = rdm_admin::model::IncarnationId::mint();
            let nodes = rdm_admin::storage::FileNodesStorage::open(&root.dir.join("fixture"), &store, "mesh1", &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &writer)).await.unwrap();
            let mut row = nodes.record_at(&"mesh1.admin.1".parse().unwrap()).await.unwrap().expect("the lost birth's row");
            let birth = row.births.last_mut().unwrap();
            (birth.is_primary, birth.is_fabric_primary) = (Some(1), Some(1));
            nodes.write_record(&row).await.unwrap();
            use rdm_admin::fabric_storage::FabricStorage as _;
            let fabric = rdm_admin::fabric_storage::FileFabricStorage::open(&store, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &writer)).await.unwrap();
            let mut record = fabric.fabric().await.unwrap().expect("the Fabric record");
            record.provider = Some(rdm_admin::model::ProviderKind::Process);
            fabric.put_fabric(&record).await.unwrap();
        });
        root
    }

    /// The row at mesh1.admin.1 and the row of `node_id`, read through the storage adapters.
    fn row_and_node(&self, node_id: &str) -> (Option<rdm_admin::storage::NodeRow>, Option<rdm_admin::storage::NodeRow>) {
        use rdm_admin::storage::NodesStorage as _;
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let root = rdm_admin::store_root::StoreRoot::open(&self.store, &self.fabric_id).unwrap();
            let nodes = rdm_admin::storage::FileNodesStorage::open(&self.dir.join("reader"), &root, "mesh1", &rdm_admin::store_log::LogWriter::mint()).await.unwrap();
            let at = nodes.record_at(&"mesh1.admin.1".parse().unwrap()).await.unwrap();
            let of = match rdm_admin::model::NodeId::parse(node_id) {
                Ok(id) => nodes.node(&id).await.unwrap(),
                Err(_) => None,
            };
            (at, of)
        })
    }
}

/// CONTRACT (Q156; fabric-recovery.md "Nothing alive": "given the fabric id, its mesh and the store root,
/// which must hold that fabric or the start is refused by name"): an adoption launch given a fabric id
/// whose store root holds no Fabric record is refused by name, naming the id and the store root, at once.
/// Must NOT happen: the launch taken for a Day 0 that creates the fabric under the given id (a Fabric
/// record, a name claim or a Build written), or a refusal that does not name the id and the root.
#[test]
fn an_adoption_launch_whose_store_root_holds_no_such_fabric_is_refused_by_name_naming_the_id_and_the_root() {
    let d = Day0::at("an_adoption_launch_whose_store_root_holds_no_such_fabric");
    let fabric_id = rdm_admin::model::FabricId::mint().to_string();
    let mut a = adopter(&d.dir, "adopter", &d.store, &fabric_id, &minted_custody());
    let (status, err) = exited_within(&mut a, ADOPTS_WITHIN).expect("the adoption launch neither refused nor ended: it ran, as a Day 0 of the given id would");
    assert!(!status.success(), "{status:?} {err}");
    assert!(err.contains("refusing to start") && err.contains(&fabric_id) && err.contains(&d.store.display().to_string()) && err.contains("holds no Fabric record"), "the refusal names the fabric id and the store root: {err}");
    assert!(rdm_admin::store_log::read_record(&d.store.join(&fabric_id).join(rdm_admin::store_root::FABRIC_FILE), "fabric-record", None).is_none(), "a Fabric record was written for the given id");
    let claims: Vec<std::path::PathBuf> = std::fs::read_dir(d.store.join(rdm_admin::store_root::FABRIC_NAMES)).map(|e| e.flatten().map(|e| e.path()).collect()).unwrap_or_default();
    assert!(claims.is_empty(), "the fabric's name was claimed: {claims:?}");
}

/// CONTRACT (Q156; fabric-recovery.md "Nothing alive": "It is given the fabric root and CA, from its
/// environment or an inherited data dir ... and refuses to start without them"): an adoption launch on a
/// fresh data dir with no root in its environment is refused by name, naming `RDM_CA_SECRET`, before
/// anything of the fabric is written. Must NOT happen: a root minted for it (a Day 0's), a claim, or the
/// lost birth's row touched.
#[test]
fn an_adoption_launch_without_the_fabric_root_is_refused_by_name() {
    let root = Prepared::adoptable("an_adoption_launch_without_the_fabric_root");
    let mut a = adopter(&root.dir, "adopter", &root.store, &root.fabric_id.to_string(), &[]);
    let (status, err) = exited_within(&mut a, ADOPTS_WITHIN).expect("the adoption launch with no root did not end");
    assert!(!status.success(), "{status:?} {err}");
    assert!(err.contains("RDM_CA_SECRET") && err.contains("no cert material"), "the refusal names the missing root: {err}");
    assert!(!root.dir.join("adopter").join("ca-secret").exists(), "a root was minted for the adoption launch");
    assert!(spans_of(&root.dir, a.pid).iter().all(|s| s["name"] != ADOPTION_CLAIM), "the refused launch claimed");
    let (at, _) = root.row_and_node("");
    assert_eq!(at.map(|r| (r.node_id.to_string(), r.status)), Some((root.node_id.to_string(), rdm_admin::model::NodeStatus::Ready)), "the lost birth's row is untouched");
}

/// CONTRACT (Q156: "Day 0 has no RDM_NODE_NAME, mints the fabric id unless given, and a given id whose
/// store root already holds a Fabric record is refused by name"): a Day 0 (no RDM_NODE_NAME) given the id
/// of a fabric its store root already holds is refused by name, naming the id, the store root and what
/// adopts a fabric (RDM_NODE_NAME), on a fresh data dir. Must NOT happen: the start taken as a Day 0 of
/// that fabric, or a refusal that names the data dir rather than the fabric it was given.
#[test]
fn a_day0_given_a_fabric_id_whose_store_root_already_holds_that_fabric_is_refused_by_name() {
    let root = Prepared::adoptable("a_day0_given_a_fabric_id_whose_store_root_already_holds_it");
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_rdm-node-admin"));
    cmd.env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("RDM_DATA_DIR", root.dir.join("day0"))
        .env("RDM_STORE_DIR", &root.store)
        .env("RDM_EVIDENCE_DIR", &root.dir)
        .env("MESH_SPAWN_TYPE", "process")
        .env("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0")
        .env("RDM_FABRIC_ID", root.fabric_id.to_string());
    let child = cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped()).spawn().expect("rdm-node-admin starts");
    let pid = child.id();
    let mut day0 = Running { child, pid };
    let (status, err) = exited_within(&mut day0, ADOPTS_WITHIN).expect("the Day 0 did not end");
    assert!(!status.success(), "{status:?} {err}");
    assert!(
        err.contains("refusing to start") && err.contains("Day 0") && err.contains(&root.fabric_id.to_string()) && err.contains(&root.store.display().to_string()) && err.contains("RDM_NODE_NAME"),
        "the refusal names Day 0, the given fabric id, the store root that holds it and the launch that adopts it: {err}"
    );
}

/// CONTRACT (Q156: "the provider is the Fabric record's, a differing MESH_SPAWN_TYPE refused by name"):
/// an adoption launch whose MESH_SPAWN_TYPE (container) differs from the provider the Fabric record names
/// (process) is refused by name, naming both, before it claims anything. Must NOT happen: the launch
/// running the fabric's nodes under another provider, or a claim.
#[test]
fn an_adoption_launch_whose_spawn_type_differs_from_the_fabric_records_provider_is_refused_by_name() {
    let root = Prepared::adoptable("an_adoption_launch_whose_spawn_type_differs");
    let mut env = minted_custody();
    env.push(("MESH_SPAWN_TYPE", "container".into()));
    let mut a = adopter(&root.dir, "adopter", &root.store, &root.fabric_id.to_string(), &env);
    let (status, err) = exited_within(&mut a, ADOPTS_WITHIN).expect("the adoption launch under another provider did not end");
    assert!(!status.success(), "{status:?} {err}");
    assert!(err.contains("MESH_SPAWN_TYPE") && err.contains("container") && err.contains("process") && err.contains("Fabric record"), "the refusal names both providers: {err}");
    assert!(spans_of(&root.dir, a.pid).iter().all(|s| s["name"] != ADOPTION_CLAIM), "the refused launch claimed");
}

/// CONTRACT (Q156; fabric-recovery.md "Nothing alive": "Before any recovery mutation other than the claim
/// itself, it claims the fabric-recovery attempt of the current Build for that mesh; a second launch's
/// claim loses and it exits. Concurrent adoption launches resolve to the same recovery attempt for the
/// exact lost birth"; build.md §5.2, Q142): on a prepared root whose one admin, the fabric-primary, is
/// lost and whose journal holds no attempt, two adoption launches are started at once, at the same
/// path.name. One claim wins; the other is refused by name, naming the claimant that holds the attempt,
/// and exits having deleted and claimed nothing. One birth stands at mesh1.admin.1, the winner's.
/// Must NOT happen: both claims won (two births at one path), the loser's node.delete or name claim, or
/// the loser's node.id holding any row.
#[test]
fn two_adoption_launches_claim_one_attempt_one_wins_and_the_other_is_refused_by_name() {
    let root = Prepared::adoptable("two_adoption_launches_claim_one_attempt");
    let custody = minted_custody();
    let a = adopter(&root.dir, "adopter-a", &root.store, &root.fabric_id.to_string(), &custody);
    let b = adopter(&root.dir, "adopter-b", &root.store, &root.fabric_id.to_string(), &custody);
    let (ca, cb) = (claim_of(&root.dir, a.pid, ADOPTS_WITHIN), claim_of(&root.dir, b.pid, ADOPTS_WITHIN));
    let (ca, cb) = (ca.expect("launch a made no adoption claim"), cb.expect("launch b made no adoption claim"));
    let outcomes = [attr_of(&ca, "outcome"), attr_of(&cb, "outcome")];
    assert_eq!(outcomes.iter().filter(|o| *o == "won").count(), 1, "one claim wins: {ca} {cb}");
    assert_eq!((attr_of(&ca, "build_id"), attr_of(&ca, "attempt"), attr_of(&ca, "lane")), (attr_of(&cb, "build_id"), attr_of(&cb, "attempt"), attr_of(&cb, "lane")), "both launches resolved to the one attempt: {ca} {cb}");
    let (mut winner, mut loser, won, lost) = if outcomes[0] == "won" { (a, b, ca, cb) } else { (b, a, cb, ca) };
    let (status, err) = exited_within(&mut loser, ADOPTS_WITHIN).expect("the losing launch did not exit");
    assert!(!status.success(), "{status:?} {err}");
    assert!(err.contains("refusing to start") && err.contains("lost") && err.contains(&attr_of(&won, "node_id")), "the loser is refused by name, naming the claimant holding the attempt: {err}");
    let loser_spans = spans_of(&root.dir, loser.pid);
    let written: Vec<&serde_json::Value> = loser_spans.iter().filter(|s| ["rdm.node_admin.node.delete.via-node-row", "rdm.node_admin.node.create.via-node-row"].contains(&s["name"].as_str().unwrap_or_default())).collect();
    assert!(written.is_empty(), "the loser deleted or claimed: {written:?}");
    let won_id = attr_of(&won, "node_id");
    let until = Instant::now() + ADOPTS_WITHIN;
    while root.row_and_node("").0.is_none_or(|r| r.node_id.to_string() != won_id) {
        assert!(Instant::now() < until, "the winner {won_id} took no row at mesh1.admin.1: {:?}", root.row_and_node("").0);
        assert!(winner.child.try_wait().unwrap().is_none(), "the winning launch exited");
        std::thread::sleep(Duration::from_millis(50));
    }
    let (at, of_loser) = root.row_and_node(&attr_of(&lost, "node_id"));
    assert!(at.as_ref().is_some_and(|r| r.node_id.to_string() == won_id && r.status != rdm_admin::model::NodeStatus::Deleted && r.births.len() <= 1), "one birth at mesh1.admin.1, the winner's: {at:?}");
    assert!(of_loser.is_none(), "the loser's node.id holds a row: {of_loser:?}");
    unsafe { libc::kill(winner.pid as i32, libc::SIGTERM) };
    let _ = exited_within(&mut winner, ADOPTS_WITHIN);
    println!("trace_id={}", won["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (Q156; fabric-recovery.md "Nothing alive": "runs node investigation on the lost birth: reached,
/// or any node-admin answering before its first mutation, it is refused NameHeld and exits, closing the
/// attempt through the failure path"): a fabric's one admin is alive but held by the operating system, so
/// it answers nothing until the adoption launch at its path.name has claimed the fabric-recovery attempt;
/// it is then let run. Its answer reaches the launch before its first mutation: the launch is refused
/// NameHeld, naming the admin that answered, the attempt it claimed reads Failed on the Build journal and
/// the mesh's in-flight row is released. Must NOT happen: the live fabric-primary's row deleted or its
/// name taken, the attempt left open, or the launch running on as a second admin at the path.
#[test]
fn an_admin_answering_before_the_adopters_first_mutation_refuses_the_adoption_and_nothing_is_deleted() {
    let d = Day0::at("an_admin_answering_before_the_adopters_first_mutation");
    let mut live = d.day0_until_ready();
    let fabric_id = fabric_dirs(&d.store).first().cloned().expect("the Day-0 fabric");
    let live_id: String = {
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(d.data_dir.join("nodes").join("self.json")).unwrap()).unwrap();
        v["record"]["node_id"].as_str().unwrap().to_string()
    };
    let custody = custody_of(&d.data_dir);
    // The Day-0 Build is Complete from its own ready: its attempt reads Converged on the journal
    // and then releases mesh1's in-flight row. The admin is held only once both stand, so the launch's
    // claim meets no attempt of Day 0.
    let until = Instant::now() + ADOPTS_WITHIN;
    while !tokio::runtime::Runtime::new().unwrap().block_on(async {
        use rdm_admin::build_state::BuildStateAdapter as _;
        use rdm_admin::storage::MeshStorage as _;
        let fid = rdm_admin::model::FabricId::parse(&fabric_id).unwrap();
        let root = rdm_admin::store_root::StoreRoot::open(&d.store, &fid).unwrap();
        let reader = rdm_admin::model::IncarnationId::mint();
        let writer = rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &reader);
        let journal = rdm_admin::build_state::FileJournal::open_on_root(&root, &d.dir.join("reader-day0"), &writer).await.unwrap();
        let converged = journal.facts().await.unwrap().iter().any(|f| matches!(f, rdm_admin::build_state::BuildFact::Attempt(r) if matches!(r.outcome, rdm_admin::build_state::AttemptOutcome::Converged)));
        let released = rdm_admin::storage::FileMeshStorage::open(&root, &writer).await.unwrap().in_flight("mesh1").await.unwrap().is_none();
        converged && released
    }) {
        assert!(Instant::now() < until, "the Day-0 Build did not read Converged with mesh1's in-flight row released");
        std::thread::sleep(Duration::from_millis(50));
    }
    unsafe { libc::kill(live.pid as i32, libc::SIGSTOP) };
    let mut a = adopter(&d.dir, "adopter", &d.store, &fabric_id, &custody);
    let claim = claim_of(&d.dir, a.pid, ADOPTS_WITHIN);
    unsafe { libc::kill(live.pid as i32, libc::SIGCONT) };
    let claim = claim.expect("the adoption launch made no claim while the live admin was held");
    assert_eq!(attr_of(&claim, "outcome"), "won", "{claim}");
    let (status, err) = exited_within(&mut a, ADOPTS_WITHIN).expect("the adoption launch ran on beside the live admin");
    assert!(!status.success(), "{status:?} {err}");
    assert!(err.contains("refusing to start") && err.contains("NameHeld") && err.contains("mesh1.admin.1"), "refused NameHeld, naming the admin that answered: {err}");
    let spans = spans_of(&d.dir, a.pid);
    let written: Vec<&serde_json::Value> = spans.iter().filter(|s| ["rdm.node_admin.node.delete.via-node-row", "rdm.node_admin.node.create.via-node-row"].contains(&s["name"].as_str().unwrap_or_default())).collect();
    assert!(written.is_empty(), "the refused launch deleted or claimed a row: {written:?}");
    let (row, in_flight, facts) = tokio::runtime::Runtime::new().unwrap().block_on(async {
        use rdm_admin::build_state::BuildStateAdapter as _;
        use rdm_admin::storage::MeshStorage as _;
        let fid = rdm_admin::model::FabricId::parse(&fabric_id).unwrap();
        let root = rdm_admin::store_root::StoreRoot::open(&d.store, &fid).unwrap();
        let reader = rdm_admin::model::IncarnationId::mint();
        let nodes = rdm_admin::storage::FileNodesStorage::open(&d.dir.join("reader"), &root, "mesh1", &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &reader)).await.unwrap();
        let meshes = rdm_admin::storage::FileMeshStorage::open(&root, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &reader)).await.unwrap();
        let journal = rdm_admin::build_state::FileJournal::open_on_root(&root, &d.dir.join("reader"), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &reader)).await.unwrap();
        (nodes.record_at(&"mesh1.admin.1".parse().unwrap()).await.unwrap(), meshes.in_flight("mesh1").await.unwrap(), journal.facts().await.unwrap())
    });
    let row = row.expect("the live fabric-primary's row");
    assert!(row.node_id.to_string() == live_id && row.status != rdm_admin::model::NodeStatus::Deleted && row.newest_birth().is_some_and(|b| b.is_fabric_primary.is_some()), "the live fabric-primary's row is intact: {row:?}");
    assert!(in_flight.as_ref().is_none_or(|r| r.attempt.to_string() != attr_of(&claim, "attempt") || r.build_id.log().to_string() != attr_of(&claim, "build_id")), "the claimed attempt's in-flight row is released: {in_flight:?}");
    let closed = facts.iter().any(|f| matches!(f, rdm_admin::build_state::BuildFact::Attempt(r) if r.build_id.log().to_string() == attr_of(&claim, "build_id") && r.attempt.to_string() == attr_of(&claim, "attempt") && r.mesh.as_deref() == Some("mesh1") && matches!(r.outcome, rdm_admin::build_state::AttemptOutcome::Failed { .. })));
    assert!(closed, "the claimed attempt reads Failed on the journal: {facts:?}");
    // The refused attempt owes no exit: two staleness floors on, the live fabric-primary opened no
    // recovery of its own birth and the journal holds no attempt in mesh1's lane past the refused one.
    let floor = Duration::from_millis(std::env::var("RDM_STALENESS_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(30_000));
    std::thread::sleep(floor * 2);
    let recoveries: Vec<serde_json::Value> = spans_of(&d.dir, live.pid).into_iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-recovery" && attr_of(s, "node_id") == live_id).collect();
    assert!(recoveries.is_empty(), "the live fabric-primary opened a recovery of its own birth: {recoveries:?}");
    let later = tokio::runtime::Runtime::new().unwrap().block_on(async {
        use rdm_admin::build_state::BuildStateAdapter as _;
        let fid = rdm_admin::model::FabricId::parse(&fabric_id).unwrap();
        let root = rdm_admin::store_root::StoreRoot::open(&d.store, &fid).unwrap();
        let reader = rdm_admin::model::IncarnationId::mint();
        rdm_admin::build_state::FileJournal::open_on_root(&root, &d.dir.join("reader-after"), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &reader)).await.unwrap().facts().await.unwrap()
    });
    let opened_after: Vec<&rdm_admin::build_state::BuildFact> = later.iter().filter(|f| matches!(f, rdm_admin::build_state::BuildFact::Opened(o) if o.mesh.as_deref() == Some("mesh1") && o.attempt.to_string() != attr_of(&claim, "attempt"))).collect();
    assert!(opened_after.is_empty(), "an attempt in mesh1's lane was opened after the refused one: {opened_after:?}");
    assert!(live.child.try_wait().unwrap().is_none(), "the live admin is still running");
    d.stop(live);
    println!("trace_id={}", claim["trace_id"].as_str().unwrap_or(""));
}

impl Prepared {
    /// fabric.delete accepted on this root after its root Build, so the delete is in force; with `dead`,
    /// mesh1's row is Dead too, its mesh.delete done: no not-yet-deleted mesh remains.
    fn delete_in_force(&self, dead: bool) {
        use rdm_admin::storage::MeshStorage as _;
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let root = rdm_admin::store_root::StoreRoot::open(&self.store, &self.fabric_id).unwrap();
            let writer = rdm_admin::model::IncarnationId::mint();
            let journal = rdm_admin::build_state::FileJournal::open_on_root(&root, &self.dir.join("fixture"), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &writer)).await.unwrap();
            let delete = rdm_admin::build_state::BuildAccepted {
                build_id: rdm_admin::build::BuildId::mint(),
                topology: rdm_admin::accepted::FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: Default::default() },
                submitted_change: Some(rdm_admin::accepted::TopologyChange::DeleteFabric),
                submitted_at_ms: now_ms(),
            };
            rdm_admin::build_state::accept(&journal, &delete, None, "mesh1.admin.1", "fabric.delete").await.unwrap();
            if dead {
                let meshes = rdm_admin::storage::FileMeshStorage::open(&root, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &writer)).await.unwrap();
                let mut m = meshes.mesh_named("mesh1").await.unwrap().expect("mesh1's row");
                m.dead = true;
                meshes.put_mesh(&m).await.unwrap();
            }
        });
    }
}

/// CONTRACT (Q159; fabric-recovery.md "Nothing alive": "the adoption launch is created at a new path.name of
/// the node-admin type in an existing, not-yet-deleted mesh, never at the lost fabric-primary's, which the
/// delete removes"): on a prepared root whose one admin, the fabric-primary, is lost while a fabric.delete
/// is in force, an adoption launch at the lost fabric-primary's path.name is refused by name, naming the
/// delete and the path.name the delete removes, before it claims anything. Must NOT happen: a claim, or
/// the lost birth's row touched.
#[test]
fn under_a_fabric_delete_an_adoption_launch_at_the_lost_fabric_primarys_path_name_is_refused_by_name() {
    let root = Prepared::adoptable("under_a_fabric_delete_an_adoption_launch_at_the_lost_path_name");
    root.delete_in_force(false);
    let mut a = adopter(&root.dir, "adopter", &root.store, &root.fabric_id.to_string(), &minted_custody());
    let (status, err) = exited_within(&mut a, ADOPTS_WITHIN).expect("the adoption launch at the lost path.name under a delete did not end");
    assert!(!status.success(), "{status:?} {err}");
    assert!(err.contains("refusing to start") && err.contains("fabric.delete") && err.contains("mesh1.admin.1") && err.contains("new path.name"), "the refusal names the delete and the path.name it removes: {err}");
    assert!(spans_of(&root.dir, a.pid).iter().all(|s| s["name"] != ADOPTION_CLAIM), "the refused launch claimed");
    let (at, _) = root.row_and_node("");
    assert_eq!(at.map(|r| (r.node_id.to_string(), r.status)), Some((root.node_id.to_string(), rdm_admin::model::NodeStatus::Ready)), "the lost birth's row is untouched");
}

/// CONTRACT (Q159; fabric-recovery.md "Nothing alive": "If no not-yet-deleted mesh remains, the remaining
/// work is the dead fabric-primary's own node.delete from outside, and nothing is created"): on a prepared
/// root whose fabric.delete is in force and whose one mesh is deleted, an adoption launch at a new
/// path.name is refused by name, naming the outside caller's node.delete, before it claims or writes
/// anything. Must NOT happen: a claim, a row written for the launch, or the lost birth's row touched.
#[test]
fn under_a_fabric_delete_with_no_mesh_left_an_adoption_launch_creates_nothing() {
    let root = Prepared::adoptable("under_a_fabric_delete_with_no_mesh_left_an_adoption_launch");
    root.delete_in_force(true);
    let mut a = adopter(&root.dir, "adopter", &root.store, &root.fabric_id.to_string(), &[minted_custody(), vec![("RDM_NODE_NAME", "mesh1.admin.2".into())]].concat());
    let (status, err) = exited_within(&mut a, ADOPTS_WITHIN).expect("the adoption launch with no mesh left did not end");
    assert!(!status.success(), "{status:?} {err}");
    assert!(err.contains("refusing to start") && err.contains("no not-yet-deleted mesh remains") && err.contains("node.delete from outside") && err.contains("nothing is created"), "the refusal names the outside caller's node.delete: {err}");
    assert!(spans_of(&root.dir, a.pid).iter().all(|s| s["name"] != ADOPTION_CLAIM && s["name"] != "rdm.node_admin.node.create.via-node-row"), "the refused launch claimed or created");
    let (at, _) = root.row_and_node("");
    assert_eq!(at.map(|r| (r.node_id.to_string(), r.status)), Some((root.node_id.to_string(), rdm_admin::model::NodeStatus::Ready)), "the lost birth's row is untouched");
}

/// The member cert `pid` installed for the birth at `node`, as the evidence spans it
/// (`rdm.node.cert.update.via-join`, the bytes' length), within `within`.
fn member_cert_installed(dir: &std::path::Path, pid: u32, node: &str, within: Duration) -> Option<u64> {
    let until = Instant::now() + within;
    loop {
        let len = spans_of(dir, pid).into_iter().filter(|s| s["name"] == "rdm.node.cert.update.via-join" && s["attributes"]["node"] == node).filter_map(|s| s["attributes"]["cert_len"].as_u64().or_else(|| s["attributes"]["cert_len"].as_str().and_then(|v| v.parse().ok()))).max();
        if len.is_some_and(|l| l > 0) || Instant::now() >= until {
            return len;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

// @feature: node-lifecycle
/// CONTRACT (fabric-certs.md §When a cert is issued, every birth "held by the birth it was issued to";
/// fabric-certs.md:82 "a new node over an inherited data dir never reuses its predecessor's cert";
/// fabric-recovery.md: the node-admin over its inherited data dir is a new node): an admin
/// re-created at its path over its inherited data dir (a new node.id and incarnation, no JoinNode) holds
/// a member cert of its own once it boots, so an app protocol it dials admits it. Must NOT happen: the
/// re-created admin installing no cert (its app-protocol dials refused `no-credential`).
#[test]
fn an_admin_re_created_at_its_path_over_its_inherited_data_dir_holds_a_member_cert() {
    let root = Prepared::recovering("an_admin_re_created_at_its_path_holds_a_member_cert");
    let mut running = root.start();
    let _ = root.until(&mut running, BUILD_RELOADED, RECOVERS_WITHIN);
    let installed = member_cert_installed(&root.dir, running.pid, "mesh1.admin.1", Duration::from_secs(20));
    let _ = root.end(running);
    assert!(installed.is_some_and(|l| l > 0), "the re-created admin installed no member cert of its own: {installed:?}");
}

// @feature: node-lifecycle
/// CONTRACT (fabric-certs.md §When a cert is issued; fabric-recovery.md "Nothing alive": the adoption
/// launch "is given the fabric root and CA" and is a new birth at the lost fabric-primary's path.name):
/// the adoption launch, which made no JoinNode, holds a member cert of its own once it has taken the
/// path, so an app protocol it dials admits it. Must NOT happen: the adopter installing no cert.
#[test]
fn an_adoption_launch_holds_a_member_cert_once_it_holds_the_path() {
    let root = Prepared::adoptable("an_adoption_launch_holds_a_member_cert");
    let mut a = adopter(&root.dir, "adopter", &root.store, &root.fabric_id.to_string(), &minted_custody());
    let claim = claim_of(&root.dir, a.pid, ADOPTS_WITHIN).expect("the adoption launch made no claim");
    assert_eq!(attr_of(&claim, "outcome"), "won", "{claim}");
    let installed = member_cert_installed(&root.dir, a.pid, "mesh1.admin.1", ADOPTS_WITHIN);
    unsafe { libc::kill(a.pid as i32, libc::SIGTERM) };
    let _ = exited_within(&mut a, ADOPTS_WITHIN);
    assert!(installed.is_some_and(|l| l > 0), "the adoption launch installed no member cert of its own: {installed:?}");
}
