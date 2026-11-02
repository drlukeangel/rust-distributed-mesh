//! i150 storage contract: the property records the nodes write, beside their records in the nodes store.

use rdm_core::{FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName};
use rdm_node::node_records::{
    FileNodeRecords, MemoryNodeRecords, NodeProperties, NodeRecordsStorage,
    RecordBirth,
};
use rdm_node::nodes_storage::{FileNodesStorage, MemoryNodesStorage, NodeRow, NodesStorage};
use rdm_node::store_root::StoreRoot;
use std::collections::BTreeMap;

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
fn record(node_id: NodeId, mesh: &str) -> NodeRow {
    let name = PathName::new(&format!("{mesh}.rpc"), "north").unwrap();
    let mut row = NodeRow::new(name.clone(), mesh, NodeKind::RpcNode, node_id, rdm_node::model::NodeStatus::Ready);
    row.birth_mut(&IncarnationId::mint(), &name);
    row.topology_storage_provider = Some("file".into());
    row
}

fn root(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("node-records-{tag}-{}", NodeId::mint()));
    std::fs::create_dir_all(&root).unwrap();
    root
}

async fn latest_line_contract(store: &dyn NodesStorage) {
    let node = NodeId::mint();
    let first = record(node.clone(), "mesh1");
    let mut later = first.clone();
    later.status = rdm_node::model::NodeStatus::Stopped;
    // Two writes append the node's record: its id is the key, the latest line is current.
    store.put_node(&first).await.unwrap();
    store.put_node(&later).await.unwrap();
    let unrelated = record(NodeId::mint(), "mesh2");
    store.put_node(&unrelated).await.unwrap();
    assert_eq!(store.node(&node).await.unwrap(), Some(later), "the latest line is the node's record");
    assert_eq!(store.node(&NodeId::mint()).await.unwrap(), None, "a node with no record has none yet");
    assert_eq!(store.nodes("mesh1").await.unwrap().len(), 1, "one record per node");
    assert!(store.nodes("mesh3").await.unwrap().is_empty());
}

// @feature: node-records
/// CONTRACT (i150 R4, Q323, Q331): one node record keyed by node id in the nodes store, append-only,
/// the latest line current; no writer is in its key. Must NOT happen: a read selecting a record by its
/// writer, or two records of one node.
#[tokio::test]
async fn a_nodes_record_is_its_latest_line_at_its_node_id() {
    latest_line_contract(&MemoryNodesStorage::default()).await;
    let dir = root("latest-line");
    let fabric = FabricId::mint();
    let store_root = StoreRoot::open(&dir, &fabric).unwrap();
    let store = FileNodesStorage::open(&dir, &store_root, "mesh1", &rdm_node::store_log::LogWriter::mint()).await.unwrap();
    latest_line_contract(&store).await;
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

// @feature: node-records
/// CONTRACT (i150 R3/R6): cancelling the caller retains ordering through the actual file put,
/// including publication to the local view. A later update preserves all untouched fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_property_caller_cannot_release_an_unresolved_write() {
    use rdm_node::node_records::NodePropertyWriter;
    use std::sync::{Arc, Mutex};
    let dir = root("cancelled-property");
    let store = Arc::new(FileNodeRecords::open(&StoreRoot::at(&dir)).await.unwrap());
    let writer = NodePropertyWriter::new(store.clone());
    let initial = properties();
    let shown = Arc::new(Mutex::new(Vec::new()));
    let door = rdm_node::store_log::write_door::arm(
        &dir.join(rdm_node::node_records::NODE_RECORDS_FILE),
        initial.birth.node_id.as_str(),
    );
    let (first, seed, published) = (writer.clone(), initial.clone(), shown.clone());
    let caller = tokio::spawn(async move {
        first
            .update(
                seed,
                |r| {
                    r.config.insert("first".into(), "durable".into());
                },
                move |r| published.lock().unwrap().push(r.clone()),
            )
            .await
    });
    door.entered().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let (next, seed, published) = (writer.clone(), initial.clone(), shown.clone());
    let later = tokio::spawn(async move {
        next.update(
            seed,
            |r| {
                r.tags.insert("later".into());
            },
            move |r| published.lock().unwrap().push(r.clone()),
        )
        .await
    });
    assert!(
        shown.lock().unwrap().is_empty(),
        "unresolved writes must not be installed"
    );
    door.release();
    let complete = later.await.unwrap().unwrap().record;
    assert_eq!(complete.config["first"], "durable");
    assert!(complete.tags.contains("later"));
    {
        let values = shown.lock().unwrap();
        assert_eq!(
            values.len(),
            2,
            "both admitted writes publish despite caller cancellation"
        );
        assert!(!values[0].tags.contains("later"));
        assert_eq!(values[1], complete);
    }
    assert_eq!(
        store.properties(&initial.birth.node_id).await.unwrap(),
        Some(complete)
    );
    drop(writer);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

/// A provider that commits a put but loses its acknowledgement once. All later point reads
/// expose the committed record; the runtime decides what that means, never the provider.
struct LostAcknowledgement {
    records: MemoryNodeRecords,
    lose: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl NodeRecordsStorage for LostAcknowledgement {
    fn provider(&self) -> String {
        self.records.provider()
    }

    async fn put_properties(
        &self,
        record: &NodeProperties,
    ) -> Result<(), rdm_node::storage_error::StorageError> {
        self.records.put_properties(record).await?;
        if self.lose.swap(false, std::sync::atomic::Ordering::SeqCst) {
            Err(rdm_node::storage_error::StorageError::Io {
                file: "properties".into(),
                reason: "acknowledgement lost".into(),
            })
        } else {
            Ok(())
        }
    }
    async fn properties(
        &self,
        node: &NodeId,
    ) -> Result<Option<NodeProperties>, rdm_node::storage_error::StorageError> {
        self.records.properties(node).await
    }
}

// @feature: node-records
/// CONTRACT (i150 R8): an uncertain put invalidates the pre-write snapshot. The next command
/// resolves the durable record before changing any property.
#[tokio::test]
async fn a_lost_storage_acknowledgement_is_resolved_before_the_next_change() {
    use rdm_node::node_records::NodePropertyWriter;
    use std::sync::Arc;
    let store = Arc::new(LostAcknowledgement {
        records: Default::default(),
        lose: true.into(),
    });
    let writer = NodePropertyWriter::new(store.clone());
    let initial = properties();
    assert!(writer
        .update(
            initial.clone(),
            |r| {
                r.config = [("source".into(), "build-a".into())].into();
            },
            |_| panic!("an uncertain effect must not be published as acknowledged")
        )
        .await
        .is_err());
    let own = writer
        .update(
            initial,
            |r| {
                assert_eq!(
                    r.config["source"], "build-a",
                    "the committed record is resolved before mutation"
                );
                r.config = [("source".into(), "node".into())].into();
            },
            |_| {},
        )
        .await
        .unwrap();
    assert_eq!(own.record.config["source"], "node");
}

// @feature: node-records
/// CONTRACT (i150 R8): an entire entry is one put even when the storage acknowledgement is
/// lost: the next command reads both its fields. A later Build's entry replaces both whole
/// fields, including empty values.
#[tokio::test]
async fn a_complete_entry_is_one_put_and_a_later_entry_replaces_both_fields_whole() {
    use rdm_core::properties::EntryProperties;
    use rdm_node::node_records::NodePropertyWriter;
    use std::sync::Arc;
    let store = Arc::new(LostAcknowledgement { records: Default::default(), lose: true.into() });
    let writer = NodePropertyWriter::new(store.clone());
    let initial = properties();
    let entry = EntryProperties {
        config: [("source".into(), "build".into())].into(),
        tags: ["build".into()].into(),
    };
    assert!(writer.entry(initial.clone(), entry, |_| panic!("unacknowledged write")).await.is_err());
    writer.update(initial.clone(), |record| {
        assert_eq!(record.config["source"], "build");
        assert_eq!(record.tags, ["build".into()].into());
        record.config = [("source".into(), "node".into())].into();
        record.tags = ["node".into()].into();
    }, |_| {}).await.unwrap();
    let restarted = NodePropertyWriter::new(store.clone());
    let empty = EntryProperties { config: Default::default(), tags: Default::default() };
    let next = restarted.entry(initial, empty, |_| {}).await.unwrap();
    assert!(next.record.config.is_empty());
    assert!(next.record.tags.is_empty());
}

// @feature: node-records
/// CONTRACT (i150 R13, Q331): migration copies the complete property record and the node's record in
/// the nodes store. A fresh provider instance returns every value unchanged.
#[tokio::test]
async fn a_provider_copy_preserves_both_record_kinds() {
    use rdm_node::node_records::NodePropertyWriter;
    use rdm_node::storage_copy::{copy, CopyRun, FileProviderLines};
    use std::sync::Arc;
    let (source_dir, destination_dir) = (root("copy-source"), root("copy-destination"));
    let fabric = FabricId::mint();
    let root = StoreRoot::open(&source_dir, &fabric).unwrap();
    let records = Arc::new(FileNodeRecords::open(&root).await.unwrap());
    let writer = NodePropertyWriter::new(records.clone());
    let initial = properties();
    let written = writer
        .update(
            initial.clone(),
            |r| {
                r.config.insert("migrated".into(), "complete".into());
            },
            |_| {},
        )
        .await
        .unwrap()
        .record;
    let nodes = FileNodesStorage::open(&source_dir, &root, "mesh1", &rdm_node::store_log::LogWriter::mint()).await.unwrap();
    let previous = record(initial.birth.node_id.clone(), "mesh1");
    let mut current = previous.clone();
    current.status = rdm_node::model::NodeStatus::Stopped;
    nodes.put_node(&previous).await.unwrap();
    nodes.put_node(&current).await.unwrap();
    nodes.finish_issued_writes().await;
    let from = FileProviderLines::open("file", &source_dir, &fabric)
        .await
        .unwrap();
    let to = FileProviderLines::open("target", &destination_dir, &fabric)
        .await
        .unwrap();
    let run = CopyRun {
        build_id: rdm_core::BuildId::mint(),
        change_id: rdm_core::ChangeId::mint(),
        source: "file".into(),
        destination: "target".into(),
    };
    copy(&run, &from, &to).await.unwrap();
    drop(to);
    let destination_root = StoreRoot::open(&destination_dir, &fabric).unwrap();
    let reopened = FileNodeRecords::open(&destination_root)
        .await
        .unwrap();
    let reopened_nodes = FileNodesStorage::open(&destination_dir, &destination_root, "mesh1", &rdm_node::store_log::LogWriter::mint()).await.unwrap();
    assert_eq!(
        reopened.properties(&initial.birth.node_id).await.unwrap(),
        Some(written)
    );
    assert_eq!(reopened_nodes.node(&initial.birth.node_id).await.unwrap(), Some(current), "the copy carries the node's latest line");
    assert_eq!(reopened_nodes.nodes("mesh1").await.unwrap().len(), 1);
    let _ = previous;
    drop(reopened_nodes);
    drop(nodes);
    drop(reopened);
    drop(from);
    drop(writer);
    drop(records);
    std::fs::remove_dir_all(source_dir).unwrap();
    std::fs::remove_dir_all(destination_dir).unwrap();
}

// @feature: node-records
/// CONTRACT (i150 R12): a same-ID start refreshes bound metadata and declarations while the
/// acknowledged properties survive. Actual new addresses replace old ones.
#[tokio::test]
async fn a_same_id_start_refreshes_bound_facts_and_preserves_applied_properties() {
    use rdm_node::node_records::NodePropertyWriter;
    use std::sync::Arc;
    let dir = root("same-id-bind");
    let store = Arc::new(FileNodeRecords::open(&StoreRoot::at(&dir)).await.unwrap());
    let writer = NodePropertyWriter::new(store.clone());
    let initial = properties();
    let before = writer
        .update(
            initial.clone(),
            |r| {
                r.config.insert("own".into(), "retained".into());
                r.tags.insert("retained".into());
            },
            |_| {},
        )
        .await
        .unwrap()
        .record;
    drop(writer);
    let mut bound = initial.clone();
    bound.transport_addr = Some("127.0.0.1:45012".parse().unwrap());
    bound.listeners = vec![("data".into(), "127.0.0.1:45011".parse().unwrap())];
    bound.held = ["new.declaration".into()].into();
    let writer = NodePropertyWriter::new(store.clone());
    let after = writer.bound(bound.clone(), |_| {}).await.unwrap().record;
    assert_eq!(after.birth, before.birth);
    assert_eq!(after.config, before.config);
    assert_eq!(after.tags, before.tags);
    assert_eq!(after.endpoint_id, bound.endpoint_id);
    assert_eq!(after.transport_addr, bound.transport_addr);
    assert_eq!(after.listeners, bound.listeners);
    assert_eq!(after.app_types, bound.app_types);
    assert_eq!(after.held, bound.held);
    assert_eq!(
        store.properties(&initial.birth.node_id).await.unwrap(),
        Some(after)
    );
    drop(writer);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

// @feature: node-records
/// CONTRACT (i150 R6/R13): a cancelled caller's admitted property put still holds provider
/// migration until that actual append ends. The next property command uses the new provider.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_migration_waits_for_a_cancelled_callers_property_write() {
    use rdm_node::node_records::NodePropertyWriter;
    use rdm_node::storage_backend::{FileNodeBackend, NodeStorageHandle, NodeStorageProviders};
    use std::sync::Arc;
    let (source, destination) = (root("admitted-source"), root("admitted-destination"));
    let initial = properties();
    let launch = rdm_core::launch::Launch {
        initial_properties: Default::default(),
        topology_storage_provider: "file".into(),
        fabric: "fabric1".into(),
        fabric_id: FabricId::mint(),
        mesh: "mesh1".into(),
        kind: rdm_core::NodeKind::RpcNode,
        name: rdm_core::PathName::new("mesh1.rpc", "2").unwrap(),
        node_id: initial.birth.node_id.clone(),
        incarnation: initial.birth.incarnation_id.clone(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![],
        launcher: None,
        data_dir: source.join("data"),
        store_dir: source.clone(),
        mesh_id: Some(MeshId::mint()),
        issuing: None,
        roots: vec![],
        connections_delete_deadline: std::time::Duration::from_secs(5),
        declared: vec![],
    };
    let providers = NodeStorageProviders::new()
        .register("file", Arc::new(FileNodeBackend::at(&source)))
        .register("target", Arc::new(FileNodeBackend::at(&destination)));
    let handle = NodeStorageHandle::open(&providers, &launch).await.unwrap();
    let writer = NodePropertyWriter::new(handle.node_records());
    let door = rdm_node::store_log::write_door::arm(
        &source
            .join(launch.fabric_id.as_str())
            .join(rdm_node::node_records::NODE_RECORDS_FILE),
        initial.birth.node_id.as_str(),
    );
    let (w, seed) = (writer.clone(), initial.clone());
    let caller = tokio::spawn(async move {
        w.update(
            seed,
            |r| {
                r.tags.insert("before-move".into());
            },
            |_| {},
        )
        .await
    });
    door.entered().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let moving = handle.clone();
    let moved = tokio::spawn(async move { moving.replace("target").await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while handle.provider() != "target" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the handle swaps before waiting on its old admissions");
    assert!(
        !moved.is_finished(),
        "the cancelled caller's actual put still owns old-provider admission"
    );
    door.release();
    moved.await.unwrap().unwrap();
    let old = FileNodeRecords::open(&StoreRoot::open(&source, &launch.fabric_id).unwrap())
        .await
        .unwrap();
    let committed = old
        .properties(&initial.birth.node_id)
        .await
        .unwrap()
        .expect("the admitted put finished on its original provider");
    assert!(committed.tags.contains("before-move"));
    let later = writer
        .update(
            initial.clone(),
            |r| {
                r.tags.insert("after-move".into());
            },
            |_| {},
        )
        .await
        .unwrap()
        .record;
    assert!(later.tags.contains("before-move"));
    assert!(later.tags.contains("after-move"));
    assert_eq!(
        handle
            .node_records()
            .properties(&initial.birth.node_id)
            .await
            .unwrap(),
        Some(later)
    );
    assert_eq!(
        old.properties(&initial.birth.node_id).await.unwrap(),
        Some(committed)
    );
    drop(old);
    drop(writer);
    drop(handle);
    std::fs::remove_dir_all(source).unwrap();
    std::fs::remove_dir_all(destination).unwrap();
}

// @feature: node-records
/// Tag commands validate against the latest serialized set and preserve every other field.
#[tokio::test]
async fn tag_deltas_preserve_properties_and_refusals_change_nothing() {
    use rdm_node::node_records::{NodePropertyWriter, TagChange};
    use rdm_core::status::{StatusReply, TagScope, MAX_NODE_TAGS};
    let records = std::sync::Arc::new(MemoryNodeRecords::default());
    let writer = NodePropertyWriter::new(records.clone());
    let mut initial = properties();
    initial.tags = (0..MAX_NODE_TAGS).map(|n| format!("tag-{n}")).collect();
    writer.bound(initial.clone(), |_| {}).await.unwrap();
    let refused = writer.tags(initial.clone(), TagChange::Create(["extra".into()].into()), |_| panic!("a refused set is never published")).await.unwrap();
    assert!(matches!(refused, StatusReply::RejectedTags { .. }), "{refused:?}");
    assert_eq!(records.properties(&initial.birth.node_id).await.unwrap(), Some(initial.clone()));
    let removed = writer.tags(initial.clone(), TagChange::Delete(TagScope::Labels(["tag-0".into(), "absent".into()].into())), |_| {}).await.unwrap();
    assert!(matches!(removed, StatusReply::TagsDeleted { not_carried, .. } if not_carried == ["absent".into()].into()));
    let added = writer.tags(initial.clone(), TagChange::Create(["extra".into(), "tag-1".into()].into()), |_| {}).await.unwrap();
    assert!(matches!(added, StatusReply::TagsCreated { already_carried, .. } if already_carried == ["tag-1".into()].into()));
    let mut expected = initial.clone();
    expected.tags.remove("tag-0");
    expected.tags.insert("extra".into());
    assert_eq!(records.properties(&initial.birth.node_id).await.unwrap(), Some(expected.clone()));
    let invalid = writer.tags(initial.clone(), TagChange::Create(["bad label".into()].into()), |_| panic!("invalid label was published")).await.unwrap();
    assert!(matches!(invalid, StatusReply::RejectedTags { .. }));
    assert_eq!(records.properties(&initial.birth.node_id).await.unwrap(), Some(expected));
    writer.tags(initial.clone(), TagChange::Delete(TagScope::All), |_| {}).await.unwrap();
    initial.tags.clear();
    assert_eq!(records.properties(&initial.birth.node_id).await.unwrap(), Some(initial));
}
