//! A node-admin answers a topology read and a join from what it holds, never from the store (Q227:
//! "no node reads anything from the store on create, start or rejoin; every node pulls what it needs
//! from its mesh-primary or the fabric-primary"; Q234: the hydration sync carries the node's own whole
//! row from its mesh-primary's atlas, never from the store).
//!
//! The defect this cell pins (lane 1's gate at 11bb3b90d, trace ec9ddcfd3696a66fb9006c639361fe0f):
//! mesh2.admin.2's start read rafka-time from its mesh-primary with `GetTopology`, and the primary's
//! topology door read the fabric's nodes store on every read, unconditionally; the store's tail held
//! an object another writer was still writing, so the read was refused `not-ready` ("the stored map
//! could not be read: ... an unended object") and the start failed at `node.rafka-time`.

use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::join::Deployed;
use rdm_admin::node_hooks::{HookOutcome, NodeHooks};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, NodeId};
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use std::io::Write as _;
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
pub(crate) const WRONG: Duration = Duration::from_secs(60);

pub(crate) fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" => Some(dir.clone()),
        "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// A launch of `name` by `root`, its data dir holding the runtime record its provider would write.
pub(crate) fn launch_by(root: &Running, name: &str, dir: &std::path::Path, store: &std::path::Path) -> Launch {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
    Launch {
        topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.to_path_buf(),
        store_dir: store.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    }
}

/// What `root` verifies the first join of `launch`'s birth against.
pub(crate) fn deployed(launch: &Launch) -> Deployed {
    let key = rdm_test::node::load_or_mint_key(&launch.data_dir).unwrap();
    Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: rdm_core::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).unwrap(),
        data_dir: launch.data_dir.display().to_string(),
        last_accepted_name: None,
        held: false,
        listeners: Vec::new(),
    }
}

pub(crate) async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != MemberStatus::Ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published Ready"));
}

/// The fabric's nodes store under `store`: `<store>/<fabric id>/nodes.jsonl`.
fn nodes_file(store: &std::path::Path, root: &Running) -> std::path::PathBuf {
    let fabric = root.digest.lock().unwrap().fabric_id.clone();
    let file = store.join(fabric.to_string()).join("nodes.jsonl");
    assert!(file.exists(), "the fabric's nodes store is at {}", file.display());
    file
}

// @feature: node-lifecycle
/// CONTRACT (Q227, Q234; node-topology-get.md): a Day-0 node-admin (mesh1.admin.1, the fabric-primary
/// and mesh1's mesh-primary) and mesh1.admin.2, launched by it, both ready. The fabric's nodes store
/// then ends in an object another writer has not finished writing (an unended final line, as the
/// gate's store held it). mesh1.admin.2 reads the topology of its mesh-primary, every mesh and then
/// mesh1 alone (the rafka-time read's shape): each is served whole, with the primaries and the
/// rafka-time, from what the primary holds, and with the reader's own whole row from the primary's
/// atlas, the row its start's hydration sync already gave it. Must NOT happen: a topology read refused `not-ready`
/// because the store could not be read, or any read of the store by the serving door.
#[test]
fn a_mesh_primary_serves_a_topology_read_with_its_store_unreadable() {
    let cell = "a_mesh_primary_serves_a_topology_read_with_its_store_unreadable";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("admin-reads-{}", NodeId::mint()));
        let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
        std::fs::create_dir_all(&root_dir).unwrap();

        // The primary: a Day-0 node-admin that does not reconcile (its Build names only itself).
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        let root_digest = root.digest.lock().unwrap().clone();

        let launch = launch_by(&root, "mesh1.admin.2", &admin_dir, &root_dir);
        let _ = root.runner.joins.expect(deployed(&launch));
        let env = launch.to_env();
        let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let admin = start_with(admin_cfg, Wiring { node_hooks: NodeHooks::new(TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok))), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the launched admin comes up");
        until_ready("mesh1.admin.2", || admin.digest.lock().unwrap().clone()).await;
        // Its start's hydration sync carried its own whole row from its mesh-primary's atlas (Q234).
        let own = admin.topology_view.own_row().expect("mesh1.admin.2 holds its own row from its hydration sync");
        assert_eq!((own.node_id.clone(), own.name.to_string()), (launch.node_id.clone(), "mesh1.admin.2".to_string()), "the row held is the node's own: {own:?}");

        // The store's tail: an object another writer is still writing, never ended.
        let file = nodes_file(&root_dir, &root);
        std::fs::OpenOptions::new().append(true).open(&file).unwrap().write_all(br#"{"v":1,"kind":"record","key":"unended","record":{"name":"mesh1.rpc.9""#).unwrap();

        let target = rdm_transport::NodeTarget::ExactNode(root_digest.node.node_id.clone());
        for mesh in [None, Some("mesh1")] {
            let read = tokio::time::timeout(WRONG, rdm_admin::topology_read::read_topology(&admin.node_rpc.client, &target, "mesh1.admin.2", mesh, None))
                .await
                .expect("the read ends")
                .unwrap_or_else(|e| panic!("the mesh-primary refused the read of {mesh:?} with its store unreadable: {e}"));
            assert!(read.installed.iter().any(|m| m.mesh == "mesh1"), "mesh1 is served whole ({mesh:?}): {:?}", read.installed.iter().map(|m| &m.mesh).collect::<Vec<_>>());
            assert!(read.rafka_time.is_some() && !read.primaries.is_empty(), "the rafka-time and the primaries are served ({mesh:?})");
            let own = read.own_row.as_ref().unwrap_or_else(|| panic!("its mesh-primary answers the reader its own row ({mesh:?})"));
            assert_eq!(own.0.node_id, launch.node_id, "the own row is the reader's ({mesh:?}): {own:?}");
            assert!(own.1 > 0, "the own row carries the row's revision ({mesh:?}): {own:?}");
        }
        let served: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.mesh.topology.serve.via-read" && f.get("node").map(String::as_str) == Some("mesh1.admin.1")).map(|(_, _, f)| f.clone()).collect();
        assert!(served.iter().all(|f| f.get("outcome").map(String::as_str) == Some("served")), "every read the primary served was served: {served:?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}

/// The folds of the fabric's four topology stores (`fabric`, `meshes`, `nodes`, `builds` .jsonl) the
/// capture holds, in the order they began: each fold's file name, its caller's span and its outcome.
fn topology_folds(spans: &crate::common::Spans) -> Vec<(String, String, String)> {
    const FOLD: &str = "rdm.node.store.resolve.via-fold";
    let held: std::collections::BTreeMap<u64, _> = spans.0.lock().unwrap().clone().into_iter().collect();
    held.into_values()
        .filter_map(|(n, _, f)| {
            let file = f.get("file")?.rsplit('/').next()?.to_string();
            (n == FOLD && TOPOLOGY_STORES.contains(&file.as_str())).then(|| (file, f.get("caller").cloned().unwrap_or_default(), f.get("outcome").cloned().unwrap_or_default()))
        })
        .collect()
}

/// The folds since the `from`th of [`topology_folds`] that a launched node-admin that is not the
/// fabric-primary never makes (Q227): any fold of `fabric`, `meshes` or `nodes` .jsonl (the Fabric
/// identity, the recorded primaries and its own row come in its pulls), and any fold of
/// `builds.jsonl`: an admin that is not the fabric-primary reads a Build's committed records whole
/// when it needs them (Q292, `committed_facts`, no fold), so neither its start (the derivation's
/// install) nor its hierarchy's turn folds them.
fn start_folds(spans: &crate::common::Spans, from: usize) -> Vec<(String, String, String, String)> {
    const FOLD: &str = "rdm.node.store.resolve.via-fold";
    let held: std::collections::BTreeMap<u64, _> = spans.0.lock().unwrap().clone().into_iter().collect();
    held.into_values()
        .filter_map(|(n, _, f)| {
            let file = f.get("file")?.rsplit('/').next()?.to_string();
            (n == FOLD && TOPOLOGY_STORES.contains(&file.as_str())).then(|| (file, f.get("reader").cloned().unwrap_or_default(), f.get("caller").cloned().unwrap_or_default(), f.get("outcome").cloned().unwrap_or_default()))
        })
        .skip(from)
        .collect()
}

/// The node-admin's four topology stores on the fabric's store root (rdm#89).
const TOPOLOGY_STORES: [&str; 4] = ["fabric.jsonl", "meshes.jsonl", "nodes.jsonl", "builds.jsonl"];

/// Change one byte in the middle of the first line of each of `files` under `store`: the line no longer
/// matches its checksum, so any fold from byte 0 refuses the file by name, while a reader that has read
/// past it (the Day-0 admin, its writer) meets nothing of it. Answers each file's length, the byte's
/// offset and the byte written.
fn unreadable_from_byte_0(store: &std::path::Path, files: &[&str]) -> Vec<(std::path::PathBuf, usize, usize, u8)> {
    files
        .iter()
        .map(|file| {
            let path = store.join(file);
            let bytes = std::fs::read(&path).unwrap();
            let start = bytes.iter().position(|b| *b != b'\n').unwrap_or_else(|| panic!("{} holds a line", path.display()));
            let end = start + bytes[start..].iter().position(|b| *b == b'\n').unwrap_or_else(|| panic!("{}'s first line is ended", path.display()));
            let at = start + (end - start) / 2;
            let changed = if bytes[at] == b'0' { b'1' } else { b'0' };
            std::os::unix::fs::FileExt::write_at(&std::fs::OpenOptions::new().write(true).open(&path).unwrap(), &[changed], at as u64).unwrap();
            (path, bytes.len(), at, changed)
        })
        .collect()
}

/// A Day-0 node-admin that does not reconcile, ready, and the launch of mesh1.admin.2 by it, its first
/// join expected: the base dir, the root, the launch and the fabric's store root dir.
async fn root_and_launch(tag: &str) -> (std::path::PathBuf, Running, Launch, std::path::PathBuf) {
    let base = std::env::temp_dir().join(format!("{tag}-{}", NodeId::mint()));
    let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
    std::fs::create_dir_all(&root_dir).unwrap();
    let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
    root.stop_reconciling().await;
    until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
    let launch = launch_by(&root, "mesh1.admin.2", &admin_dir, &root_dir);
    let _ = root.runner.joins.expect(deployed(&launch));
    let store = nodes_file(&root_dir, &root).parent().unwrap().to_path_buf();
    (base, root, launch, store)
}

async fn start_launched(launch: &Launch) -> Result<Running, String> {
    let env = launch.to_env();
    let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
    start_with(admin_cfg, Wiring { node_hooks: NodeHooks::new(TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok))), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await
}

// @feature: node-lifecycle
/// CONTRACT (Q227: "no node reads anything from the store on create, start or rejoin"; rdm#89: a
/// node-admin's topology stores open without folding): a Day-0 node-admin (mesh1.admin.1) is ready, then
/// the first line of the fabric's `fabric.jsonl` no longer matches its checksum, so no fold from byte 0
/// reads it (mesh1.admin.1 has read past it), and mesh1.admin.2, launched by it, starts. Its start opens
/// all four topology stores and folds no `fabric.jsonl` (its Fabric identity is its entry pull's, Q227):
/// it comes up Ready, holding its own row from its mesh-primary's hydration sync, and makes no other
/// start read of a topology store either ([`start_folds`]). A reader's
/// first read of `fabric.jsonl` then folds it and is
/// refused by name, naming the file, and the file is left as written. Must NOT happen: the start refused, or held, by a store it never needed
/// to start (before rdm#89, the open folded each whole file and refused the line,
/// `TopologyStorageHandle::open`).
#[test]
fn a_launched_admin_starts_with_its_fabric_store_unreadable() {
    let cell = "a_launched_admin_starts_with_its_fabric_store_unreadable";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let (base, root, launch, store) = root_and_launch("admin-unread-stores").await;
        let written = unreadable_from_byte_0(&store, &["fabric.jsonl"]);
        let folds_before = topology_folds(&spans).len();

        let admin = start_launched(&launch).await.unwrap_or_else(|e| panic!("mesh1.admin.2 starts with its fabric store unreadable: {e}"));
        until_ready("mesh1.admin.2", || admin.digest.lock().unwrap().clone()).await;
        let own = admin.topology_view.own_row().expect("mesh1.admin.2 holds its own row from its hydration sync");
        assert_eq!(own.node_id, launch.node_id, "the row held is the node's own: {own:?}");
        let since = start_folds(&spans, folds_before);
        assert!(since.is_empty(), "the start folds none of the stores it takes from its pulls: {since:?}");

        // A reader's first read of fabric.jsonl folds it, and the line refuses it by name.
        let root_dir = store.parent().unwrap();
        let fabric_id = rdm_core::FabricId::parse(store.file_name().unwrap().to_str().unwrap()).unwrap();
        let reader = rdm_admin::fabric_storage::FileFabricStorage::open(&rdm_admin::store_root::StoreRoot::open(root_dir, &fabric_id).unwrap(), &rdm_node::store_log::LogWriter::mint()).await.expect("the fabric store opens, reading nothing");
        let refused = rdm_admin::fabric_storage::FabricStorage::fabric(&reader).await.expect_err("the first read of fabric.jsonl folds it and meets the line");
        assert!(refused.to_string().contains("fabric.jsonl") && refused.to_string().contains("byte offset"), "refused by name, naming the file and the offset: {refused}");
        let fabric_folds: Vec<_> = topology_folds(&spans).split_off(folds_before).into_iter().filter(|(f, _, _)| f == "fabric.jsonl").collect();
        assert_eq!(fabric_folds.iter().map(|(_, _, o)| o.as_str()).collect::<Vec<_>>(), vec!["refused"], "the one fold of fabric.jsonl is the read's: {fabric_folds:?}");
        for (path, len, at, changed) in &written {
            let now = std::fs::read(path).unwrap();
            assert!(now.len() >= *len && now[*at] == *changed, "{} is left as written: nothing of it is removed or rewritten", path.display());
        }

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}

// @feature: node-lifecycle
/// CONTRACT (Q227: "no node reads anything from the store on create, start or rejoin; every node pulls
/// what it needs from its mesh-primary or the fabric-primary", as Q292 amends it: a starting admin reads
/// the named Build's committed records from the Build storage; Q234: the hydration sync,
/// node.topology.get from the node's mesh-primary, carries what it needs): a Day-0 node-admin (mesh1.admin.1, the fabric-primary and
/// mesh1's mesh-primary) is ready, then the first line of each of the fabric's four topology stores
/// but its Build records (`fabric`, `meshes`, `nodes` .jsonl) no longer matches its checksum, so no fold
/// from byte 0 reads any of them (mesh1.admin.1, their writer, has read past it); the Build records are
/// the one store a starting admin reads (Q292). mesh1.admin.2, launched by it,
/// starts: it comes up Ready, holding its own row from its hydration sync, the fabric-primary's and
/// mesh1's mesh-primary's records naming mesh1.admin.1's birth (the recorded primaries its
/// mesh-primary serves), and the current Build the join named, its committed records read from the
/// Build storage and absorbed (Q292), so its floor is met by that read alone. Its start makes none of
/// the folds [`start_folds`] names. Must NOT happen: the start refused by the recorded primaries' read of
/// the meshes and nodes stores (boot.rs `take_recorded_primaries`, Q149 arm A) or by the current
/// Build's read of the builds store ("the builds store cannot be read for the current Build"), the
/// floor checked against a fold of the store, the derivation's install folding the builds store, or,
/// once it is Ready, its hierarchy's turns folding `builds.jsonl`.
#[test]
fn a_launched_admin_is_ready_with_its_fabric_meshes_and_nodes_stores_unreadable() {
    let cell = "a_launched_admin_is_ready_with_its_fabric_meshes_and_nodes_stores_unreadable";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let (base, root, launch, store) = root_and_launch("admin-unread-all").await;
        let root_birth = root.digest.lock().unwrap().node.clone();
        unreadable_from_byte_0(&store, &TOPOLOGY_STORES[..3]);
        let folds_before = topology_folds(&spans).len();

        let admin = start_launched(&launch).await.unwrap_or_else(|e| panic!("mesh1.admin.2 starts with its fabric, meshes and nodes stores unreadable: {e}"));
        until_ready("mesh1.admin.2", || admin.digest.lock().unwrap().clone()).await;
        let since = start_folds(&spans, folds_before);

        let own = admin.topology_view.own_row().expect("mesh1.admin.2 holds its own row from its hydration sync");
        assert_eq!(own.node_id, launch.node_id, "the row held is the node's own: {own:?}");
        let fabric = admin.topology_view.primaries().fabric().expect("mesh1.admin.2 holds the fabric-primary's record from its hydration sync");
        assert_eq!((fabric.node_id.clone(), fabric.incarnation.clone()), (root_birth.node_id.clone(), root_birth.incarnation.clone()), "the record names the Day-0 admin's birth: {fabric:?}");
        let mesh1 = admin.topology_view.primaries().meshes().get("mesh1").cloned().expect("mesh1.admin.2 holds mesh1's mesh-primary record from its hydration sync");
        assert_eq!(mesh1.node_id, root_birth.node_id, "mesh1's record names the Day-0 admin: {mesh1:?}");
        let of_admin = |name: &str| -> Vec<std::collections::BTreeMap<String, String>> { spans.0.lock().unwrap().values().filter(|(n, _, f)| n == name && f.get("node").map(String::as_str) == Some("mesh1.admin.2")).map(|(_, _, f)| f.clone()).collect() };
        let read = of_admin("rdm.node_admin.build.update.via-read-committed");
        assert!(read.iter().any(|f| f.get("outcome").map(String::as_str) == Some("absorbed")), "the floor's Build is read from the Build storage and absorbed: {read:?}");
        // The admin's digest goes Ready before its Ready span is emitted (admin/boot.rs: set_own_status, then
        // the held Build read, then `via-ready`): the span is waited for, never read at the digest's instant.
        let ready = rdm_test::estate::wait_for("mesh1.admin.2's Ready span", Duration::from_secs(10), || {
            let ready = of_admin("rdm.mesh.node.update.via-ready");
            async move { (!ready.is_empty()).then_some(ready) }
        })
        .await;
        assert!(ready.iter().any(|f| f.get("build_id").is_some_and(|b| !b.is_empty() && !b.starts_with("unreadable"))), "the Ready names the current Build its log holds, read from the Build storage: {ready:?}");
        assert!(since.is_empty(), "mesh1.admin.2's start reads none of the stores it takes from its pulls: {since:?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
