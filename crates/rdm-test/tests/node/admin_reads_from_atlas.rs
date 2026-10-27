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
const WRONG: Duration = Duration::from_secs(60);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
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
fn launch_by(root: &Running, name: &str, dir: &std::path::Path, store: &std::path::Path) -> Launch {
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
fn deployed(launch: &Launch) -> Deployed {
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

async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != MemberStatus::ReadyForTraffic {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published ReadyForTraffic"));
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
            assert!(read.rafka_time.is_some() && !read.seats.is_empty(), "the rafka-time and the primaries are served ({mesh:?})");
            let own = read.own_row.as_ref().unwrap_or_else(|| panic!("its mesh-primary answers the reader its own row ({mesh:?})"));
            assert_eq!(own.node_id, launch.node_id, "the own row is the reader's ({mesh:?}): {own:?}");
        }
        let served: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.mesh.topology.serve.via-read" && f.get("node").map(String::as_str) == Some("mesh1.admin.1")).map(|(_, _, f)| f.clone()).collect();
        assert!(served.iter().all(|f| f.get("outcome").map(String::as_str) == Some("served")), "every read the primary served was served: {served:?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
