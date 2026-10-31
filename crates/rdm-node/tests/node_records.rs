//! i150 storage contract: node properties and writer-birth lifecycle records.

use rdm_core::{FabricId, IncarnationId, MeshId, NodeId, NodeKind};
use rdm_node::node_records::{
    FileNodeRecords, MemoryNodeRecords, NodeLifecycle, NodeProperties, NodeRecordsStorage,
    RecordBirth,
};
use rdm_node::store_root::StoreRoot;
use std::collections::{BTreeMap, BTreeSet};

fn birth() -> RecordBirth {
    RecordBirth {
        node_id: NodeId::mint(),
        incarnation_id: IncarnationId::mint(),
    }
}
fn properties() -> NodeProperties {
    NodeProperties {
        birth: birth(),
        config: [("port".into(), "45001".into()), ("empty".into(), "".into())].into(),
        tags: ["west".into(), "ready".into()].into(),
        endpoint_id: None,
        transport_addr: Some("127.0.0.1:45002".parse().unwrap()),
        listeners: vec![("data".into(), "127.0.0.1:45001".parse().unwrap())],
        app_types: None,
        held: ["example.record".into()].into(),
    }
}
fn lifecycle(node_id: NodeId, writer: RecordBirth, mesh_id: MeshId) -> NodeLifecycle {
    NodeLifecycle {
        node_id,
        writer,
        path: "mesh1.rpc".into(),
        name: "north".into(),
        node_type: NodeKind::RpcNode,
        fabric_id: FabricId::mint(),
        mesh_id,
        incarnation_id: IncarnationId::mint(),
        status: rdm_node::model::NodeStatus::Ready,
        declared: BTreeSet::new(),
        operations: vec![],
        silence: None,
        data_dir_lost: None,
        data_dir_unreadable: None,
        cert: None,
        signing_cert: None,
        is_primary: false,
        is_fabric_primary: false,
        executable: None,
        storage_persistence: None,
        placement: None,
        topology_storage_provider: Some("file".into()),
        allocated: None,
        predecessor: None,
        renamed_from: None,
        inherited_by: None,
    }
}
fn root(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("node-records-{tag}-{}", NodeId::mint()));
    std::fs::create_dir_all(&root).unwrap();
    root
}

async fn late_writer_contract(store: &dyn NodeRecordsStorage) {
    let node = NodeId::mint();
    let mesh = MeshId::mint();
    let old = lifecycle(node.clone(), birth(), mesh.clone());
    let mut current = old.clone();
    current.writer = RecordBirth {
        node_id: old.writer.node_id.clone(),
        incarnation_id: IncarnationId::mint(),
    };
    store.put_lifecycle(&old).await.unwrap();
    store.put_lifecycle(&current).await.unwrap();
    let mut late = old.clone();
    late.status = rdm_node::model::NodeStatus::Deleted;
    store.put_lifecycle(&late).await.unwrap();
    assert_eq!(
        store.lifecycle(&node, &current.writer).await.unwrap(),
        Some(current)
    );
    assert_eq!(
        store.lifecycle(&node, &old.writer).await.unwrap(),
        Some(late)
    );
    assert_eq!(
        store.lifecycle(&node, &birth()).await.unwrap(),
        None,
        "a missing current writer never falls back to another writer"
    );
    assert_eq!(store.lifecycles(&mesh).await.unwrap().len(), 2);
    assert!(store.lifecycles(&MeshId::mint()).await.unwrap().is_empty());
}

// @feature: node-records
/// CONTRACT (i150 R4): a predecessor's late write cannot overwrite the successor writer's record.
#[tokio::test]
async fn a_late_primary_write_changes_only_its_own_record() {
    late_writer_contract(&MemoryNodeRecords::default()).await;
    let dir = root("late-writer");
    let store = FileNodeRecords::open(&StoreRoot::at(&dir)).await.unwrap();
    late_writer_contract(&store).await;
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

// @feature: node-records
/// CONTRACT (i150 R2/R3): node IDs address independent records; writes at an old ID cannot erase
/// the successor's properties, and reading a missing ID returns absence without substituting it.
#[tokio::test]
async fn an_old_nodes_update_preserves_its_successors_properties() {
    let dir = root("successor");
    let store = FileNodeRecords::open(&StoreRoot::at(&dir)).await.unwrap();
    let mut old = properties();
    let successor = properties();
    store.put_properties(&old).await.unwrap();
    store.put_properties(&successor).await.unwrap();
    old.config = BTreeMap::new();
    store.put_properties(&old).await.unwrap();
    assert_eq!(
        store.properties(&successor.birth.node_id).await.unwrap(),
        Some(successor)
    );
    assert_eq!(store.properties(&NodeId::mint()).await.unwrap(), None);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

// @feature: node-records
/// CONTRACT (i150 e0/e2 storage canary): an acknowledged property write survives the writer
/// process's clean exit and SIGKILL, and a fresh process can read every complete property value.
/// This is a storage canary, not proof that a running node has switched to this adapter.
#[tokio::test]
async fn acknowledged_properties_survive_writer_process_exit() {
    const CHILD: &str = "RDM_NODE_RECORDS_CANARY_CHILD";
    if let Some(dir) = std::env::var_os(CHILD) {
        let dir = std::path::PathBuf::from(dir);
        let records: Vec<NodeProperties> =
            serde_json::from_slice(&std::fs::read(dir.join("input.json")).unwrap()).unwrap();
        let store = FileNodeRecords::open(&StoreRoot::at(&dir)).await.unwrap();
        for record in &records {
            store.put_properties(record).await.unwrap();
        }
        if std::env::var_os("RDM_NODE_RECORDS_CANARY_KILL").is_some() {
            // Kill after the final durable acknowledgement; no Rust destructors or telemetry flush.
            unsafe {
                libc::raise(libc::SIGKILL);
            }
            unreachable!("SIGKILL ends the writer");
        }
        return;
    }
    for kill in [false, true] {
        let dir = root(if kill { "killed" } else { "clean" });
        let records: Vec<_> = (0..100)
            .map(|i| {
                let mut r = properties();
                r.config.insert("item".into(), i.to_string());
                r
            })
            .collect();
        std::fs::write(
            dir.join("input.json"),
            serde_json::to_vec(&records).unwrap(),
        )
        .unwrap();
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args([
            "node_records::acknowledged_properties_survive_writer_process_exit",
            "--exact",
            "--test-threads=1",
        ])
        .env(CHILD, &dir);
        if kill {
            cmd.env("RDM_NODE_RECORDS_CANARY_KILL", "1");
        } else {
            cmd.env_remove("RDM_NODE_RECORDS_CANARY_KILL");
        }
        let output = tokio::task::spawn_blocking(move || cmd.output().unwrap())
            .await
            .unwrap();
        if kill {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                output.status.signal(),
                Some(libc::SIGKILL),
                "writer reached forced-exit boundary: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let reopened = FileNodeRecords::open(&StoreRoot::at(&dir)).await.unwrap();
        for record in records {
            assert_eq!(
                reopened.properties(&record.birth.node_id).await.unwrap(),
                Some(record),
                "complete record after writer exit, kill={kill}"
            );
        }
        drop(reopened);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

// @feature: node-records
/// CONTRACT (i150 R11): unreadable evidence is an error, never a missing node or fallback writer.
#[tokio::test]
async fn an_unrecognised_record_is_not_answered_as_absent() {
    let dir = root("malformed");
    let node = properties();
    let store = FileNodeRecords::open(&StoreRoot::at(&dir)).await.unwrap();
    store.put_properties(&node).await.unwrap();
    drop(store);
    let file = dir.join(rdm_node::node_records::NODE_RECORDS_FILE);
    let mut bytes = std::fs::read(&file).unwrap();
    // Damage a complete committed line without manufacturing another valid record.
    let at = bytes.iter().position(|b| *b == b'{').unwrap();
    bytes[at] = b'!';
    std::fs::write(&file, bytes).unwrap();
    let reopened = FileNodeRecords::open(&StoreRoot::at(&dir)).await.unwrap();
    assert!(
        matches!(
            reopened.properties(&node.birth.node_id).await,
            Err(rdm_node::storage_error::StorageError::Unrecognised { .. })
        ),
        "malformed persisted evidence must fail the read by name"
    );
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}
