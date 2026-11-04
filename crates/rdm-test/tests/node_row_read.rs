//! A node's point read of another node's row from the nodes storage (query.md §1, Q237):
//! `RunningNode::node_row(id | structured location)`, the store opened on the node's first read and never at its
//! start, join or create (Q227).

use crate::common::{admin_side, TEST_MESH_ID};
use rdm_admin::storage::{NodeRecord, NodesStorage as _};
use rdm_core::launch::Launch;
use rdm_core::model::NodeStatus;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId, PathName};
use rdm_node::storage_error::StorageError;
use rdm_test::node;
use std::collections::BTreeMap;
use std::path::Path;

const READ: &str = "rdm.node.row.resolve.via-store";
const FOLD: &str = "rdm.node.store.resolve.via-fold";

fn launch(dir: &Path, fabric: &FabricId, name: PathName, admin: &rdm_test::rig::AdminSide) -> Launch {
    Launch {
        initial_properties: Default::default(),
        topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: fabric.clone(),
        mesh: "mesh1".into(),
        r#type: "gateway".into(),
        path: name.path.clone(), name: name.name.clone(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![admin.seed.clone()],
        launcher: Some(admin.launcher.clone()),
        data_dir: dir.to_path_buf(),
        store_dir: dir.to_path_buf(),
        mesh_id: Some(MeshId::parse(TEST_MESH_ID).unwrap()),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    }
}

/// The folds of the fabric's `nodes.jsonl` the cell's capture holds, in the order they began, each with
/// the span it ran under.
fn nodes_folds(spans: &crate::common::Spans) -> Vec<Option<String>> {
    let held: BTreeMap<u64, _> = spans.0.lock().unwrap().clone().into_iter().collect();
    held.into_values().filter(|(n, _, f)| n == FOLD && f.get("file").is_some_and(|p| p.ends_with("nodes.jsonl"))).map(|(_, parent, _)| parent).collect()
}

// @feature: node-lifecycle
/// CONTRACT (query.md §1, Q237: "A node reads another node's row from the nodes storage itself: a point
/// read by `id` or by `name`, the store opened on the node's first read and never at its start, join or
/// create"; Q227): broker1's record stands in the fabric's nodes storage as broker1 wrote it (Q356).
/// gw1 starts and joins, and its start folds no `nodes.jsonl`. broker1 then writes its record again,
/// and every primary gw1 could call is gone: gw1 reads broker1's record by separate `path` and `name`
/// and by id, and both answer the record last written, whole; a name no record was written at answers
/// none. The first read opens the store (one fold of `nodes.jsonl`, under the read's span) and the
/// second folds nothing again; no read makes a Node RPC call. Must NOT happen: the store opened at the
/// node's start, a read sent to a primary, or a row older than the one written.
#[test]
fn a_node_reads_another_nodes_config_from_the_nodes_storage_with_no_primary_reachable() {
    let cap = crate::common::capture("a_node_reads_another_nodes_config_from_the_nodes_storage_with_no_primary_reachable");
    let spans = cap.spans.clone();
    let seen = spans.clone();
    let gateway_id = cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("node-row-read-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let fabric = FabricId::mint();
        let admin = admin_side("127.0.0.1".parse().unwrap(), &fabric).await;

        // broker1's record, as broker1 writes it to the fabric's nodes storage (Q356).
        let root = rdm_node::store_root::StoreRoot::open(&dir, &fabric).unwrap();
        let writer = rdm_admin::storage::FileNodesStorage::open(&root, &rdm_node::store_log::LogWriter::mint()).await.expect("broker1's nodes storage opens");
        let broker: PathName = rdm_core::PathName::new("mesh1.broker", "1").unwrap();
        let mut row: NodeRecord = rdm_node::nodes_storage::fixture_record(&broker, "broker", NodeId::mint(), IncarnationId::mint(), NodeStatus::Ready);
        // A (path, name) resolves to the live record standing there (node-identity-and-state.md §2).
        writer.put_node(&row).await.expect("broker1's record is written");
        let written = nodes_folds(&seen).len();

        let launch = launch(&dir, &fabric, PathName::new("mesh1.gateway", "1").unwrap(), &admin);
        let key = node::load_or_mint_key(&dir).unwrap();
        admin.deployed(&launch, &key);
        let gw1 = node::start(&launch, |b, _| b).await.expect("gw1 starts and joins");
        assert_eq!(nodes_folds(&seen).len(), written, "gw1's start folds no nodes.jsonl (Q227)");

        // broker1 writes its record again; then no primary is reachable.
        row.state = NodeStatus::Leaving;
        writer.put_node(&row).await.expect("broker1's record is written again");
        drop(admin);

        let by_name = gw1.node_row(broker.clone()).await.expect("gw1 reads broker1's row by name").expect("broker1's row stands");
        assert_eq!(by_name, row, "the whole row, as last written");
        assert_eq!(by_name.state, NodeStatus::Leaving, "the row as last written");
        let by_id = gw1.node_row(row.node_id.clone()).await.expect("gw1 reads broker1's row by id").expect("broker1's row stands");
        assert_eq!(by_id, row, "by id, the same row");
        let none = gw1.node_row(rdm_core::PathName::new("mesh1.broker", "9").unwrap()).await.expect("a read of a name no row was written at");
        assert_eq!(none, None);

        let folds = nodes_folds(&seen);
        assert_eq!(folds.len(), written + 1, "the first read opens the store, once: {folds:?}");
        assert_eq!(folds.last().cloned().flatten().as_deref(), Some(READ), "the fold ran under gw1's read: {folds:?}");

        gw1.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
        launch.node_id
    });
    let held: BTreeMap<u64, _> = spans.0.lock().unwrap().clone().into_iter().collect();
    let reads: Vec<_> = held.values().filter(|(n, _, _)| n == READ).map(|(_, _, f)| f.clone()).collect();
    let outcomes: Vec<(String, String)> = reads.iter().map(|f| (f.get("by").cloned().unwrap_or_default(), f.get("outcome").cloned().unwrap_or_default())).collect();
    assert_eq!(outcomes, vec![("location".into(), "found".into()), ("id".into(), "found".into()), ("location".into(), "none".into())], "{reads:?}");
    assert!(reads.iter().all(|f| f.get("node").map(String::as_str) == Some(gateway_id.as_str())), "{reads:?}");
    let called: Vec<_> = held.values().filter(|(n, parent, _)| n.starts_with("rdm.node_rpc.") && parent.as_deref() == Some(READ)).collect();
    assert!(called.is_empty(), "no read makes a Node RPC call: {called:?}");
}

// @feature: node-lifecycle
/// CONTRACT (Q227: no node reads the store on create, start or rejoin; Q237: the store opened on the
/// node's first read): the fabric's `nodes.jsonl` holds a line no fold of this build can read. gw1's
/// start succeeds over it and folds nothing; its first read of a row opens the store and is refused by
/// name, naming the file, and nothing else of the node is touched. Must NOT happen: the node's start
/// refused, or held, by the nodes storage it never needed to start.
#[test]
fn a_nodes_start_opens_no_nodes_store_and_its_first_read_does() {
    let cap = crate::common::capture("a_nodes_start_opens_no_nodes_store_and_its_first_read_does");
    let spans = cap.spans.clone();
    let seen = spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("node-row-read-unreadable-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let fabric = FabricId::mint();
        let admin = admin_side("127.0.0.1".parse().unwrap(), &fabric).await;
        let root = rdm_node::store_root::StoreRoot::open(&dir, &fabric).unwrap();
        let file = root.dir().join(rdm_node::store_root::NODES_FILE);
        // A line in a `dataschema` this build does not read: whole, and refused by any fold of the file.
        let line = String::from_utf8(rdm_node::store_log::encode_line(&rdm_node::store_log::Attributes::of(&rdm_node::store_log::LogWriter::mint(), "node-record", "node-record/9", "x"), b"{}")).unwrap();
        std::fs::write(&file, &line).unwrap();

        let launch = launch(&dir, &fabric, PathName::new("mesh1.gateway", "1").unwrap(), &admin);
        let key = node::load_or_mint_key(&dir).unwrap();
        admin.deployed(&launch, &key);
        let gw1 = node::start(&launch, |b, _| b).await.expect("gw1 starts over a nodes store it cannot fold");
        assert!(nodes_folds(&seen).is_empty(), "gw1's start folds no nodes.jsonl (Q227)");
        // The start appended gw1's own record after the line, reading none of it (Q227, Q356).
        let started = std::fs::read_to_string(&file).unwrap();
        assert!(started.starts_with(&line), "the start leaves the line it cannot read as it is");

        // A read by id folds the records; a (path, name) resolves through its name claim first, and
        // a name no node claims reads no record (node-identity-and-state.md §2).
        let refused = gw1.node_row(NodeId::mint()).await.expect_err("the first read folds the store and meets the line");
        assert!(matches!(&refused, StorageError::Unrecognised { file: f, .. } if f.ends_with("nodes.jsonl")), "refused by name, naming the file: {refused:?}");
        assert_eq!(nodes_folds(&seen), vec![Some(READ.to_string())], "the one fold is the read's");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), started, "the read leaves the file as it is");

        gw1.stop().await;
        drop(admin);
        let _ = std::fs::remove_dir_all(&dir);
    });
    let reads: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == READ).map(|(_, _, f)| f.clone()).collect();
    assert_eq!(reads.len(), 1, "{reads:?}");
    assert_eq!(reads[0].get("outcome").map(String::as_str), Some("refused"), "{reads:?}");
    assert!(reads[0].get("cause").is_some_and(|c| c.contains("nodes.jsonl")), "{reads:?}");
}

fn estate_owner(test: &str) -> rdm_test::estate::Owner {
    rdm_test::estate::Owner {
        product: "mesh".into(),
        feature: "query".into(),
        subfeature: "node-row".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

// @feature: node-lifecycle
/// CONTRACT (query.md §1, Q237), on real node processes: mesh1.node.2's mesh-primary, mesh1.node-admin.1, has
/// written its row to the nodes storage. mesh1.node-admin.1, the fabric's only node-admin and so every
/// primary, is then killed. mesh1.node.1 reads mesh1.node.2's row from the nodes storage by its node id
/// (the testkit's node-row knob, `RunningNode::node_row`) and answers it. Spans: mesh1.node.1's
/// `rdm.node.row.resolve.via-store` is `found`; the store's fold of `nodes.jsonl` in that trace is the
/// read's own, under it; no Node RPC call descends from it. Must NOT happen: the read needing a primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nodes_row_is_read_from_the_nodes_storage_by_another_node_process_with_no_primary_alive() {
    let cell = "a_nodes_row_is_read_from_the_nodes_storage_by_another_node_process_with_no_primary_alive";
    let mut estate = rdm_test::estate::Estate::bootstrap(estate_owner(cell), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 1, 2)]).await;
    let views = estate.settled_views(&[("mesh1", 1, 2)]).await;
    estate.until_fabric_ready(std::time::Duration::from_secs(120)).await;
    let id_at = |name: &str| views.iter().find(|v| v.location() == PathName::new("mesh1.node", name).unwrap()).unwrap_or_else(|| panic!("mesh1.node.{name} is settled: {views:?}")).node_id.to_string();
    let (reader, read_id) = (id_at("1"), id_at("2"));

    // Every primary is gone: the fabric's one node-admin is killed.
    estate.kill_bootstrap();
    let dir = rdm_test::node::node_row_knob_dir(&estate.root);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{reader}.staged")), &read_id).unwrap();
    std::fs::rename(dir.join(format!("{reader}.staged")), dir.join(&reader)).unwrap();
    let answer: serde_json::Value = rdm_test::estate::wait_for("mesh1.node.1 answers its read of mesh1.node.2's row", std::time::Duration::from_secs(30), || {
        let file = dir.join(format!("{reader}.answer"));
        async move { std::fs::read_to_string(&file).ok().and_then(|t| serde_json::from_str(&t).ok()) }
    })
    .await;
    assert_eq!((&answer["outcome"], &answer["node_id"]), (&serde_json::json!("found"), &serde_json::json!(read_id)), "mesh1.node.2's row, as its mesh-primary wrote it: {answer}");

    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &serde_json::Value, k: &str| sp["attributes"][k].as_str().map(str::to_string).unwrap_or_else(|| sp["attributes"][k].to_string());
    let read = rdm_test::estate::named(&spans, READ).into_iter().find(|sp| attr(sp, "node") == reader).unwrap_or_else(|| panic!("mesh1.node.1's {READ}"));
    assert_eq!((attr(read, "by"), attr(read, "subject_id"), attr(read, "outcome")), ("id".to_string(), format!("{:?}", Some(NodeId::parse(&read_id).unwrap())), "found".to_string()), "{read}");
    let folds: Vec<_> = rdm_test::estate::named(&spans, FOLD).into_iter().filter(|sp| sp["trace_id"] == read["trace_id"] && attr(sp, "file").ends_with("nodes.jsonl")).collect();
    assert_eq!(folds.len(), 1, "the read's trace holds one fold of nodes.jsonl: {folds:?}");
    assert!(rdm_test::estate::descends_from(&spans, folds[0], read), "the store is opened by the read: {}", folds[0]);
    let called: Vec<_> = spans.iter().filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_rpc.")) && rdm_test::estate::descends_from(&spans, sp, read)).collect();
    assert!(called.is_empty(), "the read makes no Node RPC call: {called:?}");
    eprintln!("read: trace {} {} node={} outcome={}", read["trace_id"].as_str().unwrap_or_default(), READ, attr(read, "node"), attr(read, "outcome"));
    estate.record_trace_url(read["trace_id"].as_str().unwrap_or_default());
}
