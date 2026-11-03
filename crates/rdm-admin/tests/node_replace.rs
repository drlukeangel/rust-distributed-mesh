//! Functional cell for `node.replace` (`docs/architecture/lifecycles/node-replace.md`).

use rdm_admin::model::{
    Fabric, IncarnationId, Mesh, Node, NodeId, NodeStatus, PathName, ProviderKind, ScopeStatus,
};
use rdm_admin::node_rows::{record_at, RowFor, RowCreated, NodeRows, RowRefused, WriteRecord};
use rdm_admin::storage::{Allocation, BirthRef, MemoryNodesStorage, NameHeld, NodeRow, NodesStorage};
use rdm_admin::topology::Topology;
use rdm_core::binding::Binding;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing_subscriber::layer::SubscriberExt as _;

struct Captured {
    attributes: BTreeMap<String, String>,
}

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

impl Capture {
    fn new(cell: &str) -> Self {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (capture, layer) = rdm_transport::span_capture(cell, exporter.clone());
        Self {
            exporter,
            capture,
            dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
        }
    }

    fn named(&self, name: &str) -> Vec<Captured> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| Captured {
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }
}

// @feature: node-replace
/// Page: docs/architecture/lifecycles/node-replace.md:19
/// "Properties | The successor's own property record starts from its Build entry's config and tags; its
/// lifecycle claim retains the exact immutable launch snapshot ... Tags remain in the subject's own
/// property record; a current-pair lookup selects the successor's ID, not the predecessor's properties
/// ... It never writes the entry over a later node or caller update ... Nothing is copied from the
/// predecessor"
/// Page: docs/architecture/lifecycles/node-replace.md:36
/// "3. node.create of node[2] at `<name>`, free only because the row above moved the key: a re-birth
/// with a new node.id, incarnation and the specified bin... A claim that finds `<name>` held is refused
/// `NameHeld { by: node[1] }` and the Build stops at this step by name."
///
/// Each successor's claim names its predecessor by explicit replacement provenance
/// (`Allocation.predecessor`, the exact birth its admission fixed), never by the row last written at a
/// location. Its claim writes the entry's launch snapshot (`initial_properties`); the successor writes
/// its own property record at launch, so the claim is asserted on its snapshot, never on properties.
#[test]
fn node_replace_refuses_held_name_until_predecessor_renamed_or_deleted_and_takes_its_entrys_config_and_tags() {
    let capture = Capture::new("node_replace_inherits_config_and_tags");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let admin_name: PathName = rdm_core::PathName::new("mesh1.admin", "1").unwrap();
        let mut admin = Node::allocated(admin_name.clone(), "mesh1", rdm_core::NodeKind::NodeAdmin);
        admin.status = NodeStatus::Ready;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        admin.incarnation_id = Some(IncarnationId::mint());
        let admin_id = admin.node_id.clone();

        let name1: PathName = rdm_core::PathName::new("mesh1.rpc", "1").unwrap();
        let old1 = name1.renamed().unwrap();
        let mut pred1 = Node::allocated(name1.clone(), "mesh1", rdm_core::NodeKind::RpcNode);
        pred1.status = NodeStatus::Ready;
        pred1.incarnation_id = Some(IncarnationId::mint());
        let pred1_id = pred1.node_id.clone();

        let name2: PathName = rdm_core::PathName::new("mesh1.rpc", "2").unwrap();
        let mut pred2 = Node::allocated(name2.clone(), "mesh1", rdm_core::NodeKind::RpcNode);
        pred2.status = NodeStatus::Ready;
        pred2.incarnation_id = Some(IncarnationId::mint());
        let pred2_id = pred2.node_id.clone();

        let view = Arc::new(RwLock::new(Topology {
            fabric: Fabric {
                id: rdm_core::FabricId::mint(),
                name: "fabric1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: vec![Mesh {
                id: Some(rdm_core::MeshId::mint()),
                name: "mesh1".into(),
                status: ScopeStatus::Ready,
            }],
            nodes: vec![admin, pred1.clone(), pred2.clone()],
        }));

        // Each predecessor's lifecycle record carries its exact birth: what the successor's admitted
        // provenance names.
        let store = Arc::new(MemoryNodesStorage::default());
        let mut pred1_row = NodeRow::new(name1.clone(), "mesh1", rdm_core::NodeKind::RpcNode, pred1_id.clone(), NodeStatus::Ready);
        pred1_row.birth_mut(pred1.incarnation_id.as_ref().unwrap(), &name1);
        store.write_record(&pred1_row).await.unwrap();
        let mut pred2_row = NodeRow::new(name2.clone(), "mesh1", rdm_core::NodeKind::RpcNode, pred2_id.clone(), NodeStatus::Ready);
        pred2_row.birth_mut(pred2.incarnation_id.as_ref().unwrap(), &name2);
        store.write_record(&pred2_row).await.unwrap();

        let rows = NodeRows::new(admin_name, admin_id, view, store.clone());
        rows.written().hold(&pred1_row);
        rows.written().hold(&pred2_row);

        let provenance = |pred: &Node, at: &PathName| Allocation {
            predecessor: Some(BirthRef { node_id: pred.node_id.clone(), incarnation_id: pred.incarnation_id.clone().unwrap() }),
            build_id: rdm_core::BuildId::mint(),
            operation: rdm_core::OperationId::location("node.create", at),
            incarnation: IncarnationId::mint(),
            deployment_id: rdm_admin::model::DeploymentId::mint(),
        };
        let succ1_id = NodeId::mint();
        let succ1_allocation = provenance(&pred1, &name1);
        let v2_binding = Binding {
            launch_id: "rpc_node".into(),
            executable: "/bin/rdm-rpc-node-v2".into(),
            sha256: "2".repeat(64),
            image: None,
        };
        let entry_tags = BTreeSet::from(["tier-a".to_string(), "canary".to_string()]);
        let entry_config = BTreeMap::from([("zone".to_string(), "b".to_string())]);
        let claim1 = || rows.create_row_with_spec(&name1, "mesh1", rdm_core::NodeKind::RpcNode, &succ1_id, &succ1_allocation.operation, Some(&succ1_allocation), Some(&v2_binding), &entry_tags, &entry_config, None, RowFor::Successor);

        // 1. Before predecessor node[1] releases `mesh1.rpc.1`, successor node[2]'s claim is refused NameHeld.
        assert_eq!(
            claim1().await,
            Err(RowRefused::NameHeld(NameHeld {
                name: name1.clone(),
                by: pred1_id.clone(),
            }))
        );

        // 2. Rename predecessor node[1] to `mesh1.rpc.1-old` (step 2 of node-replace.md).
        let renamed_row = rows.rename_row_with_intent(&pred1, &old1, &rdm_admin::accepted::RenameIntent::Replacement { original: name1.clone() }).await.expect("rename_row");
        assert_eq!(renamed_row.name, old1);
        assert_eq!(renamed_row.renamed_from, Some(name1.clone()));
        assert_eq!(renamed_row.replacement_from, Some(name1.clone()), "the replacement intent retains the predecessor's original pair");
        assert!(
            record_at(&*store, &name1).await.unwrap().is_none(),
            "mesh1.rpc.1 is free after rename_row"
        );

        // 3. Successor node[2] claims `mesh1.rpc.1` with RowFor::Successor, naming node[1]'s exact
        //    birth: its launch snapshot is its Build entry's config and tags (Q305), never node[1]'s,
        //    and node[1]'s retained row is marked inherited by it.
        let RowCreated::Written { row: succ1_row, seeded_from } = claim1().await.expect("successor claim after rename") else {
            panic!("expected RowCreated::Written");
        };
        assert_eq!(seeded_from, Some(pred1_id.clone()), "the admitted provenance names the predecessor");
        let snapshot1 = succ1_row.initial_properties.clone().expect("the claim retains the launch snapshot");
        assert_eq!((&snapshot1.config, &snapshot1.tags), (&entry_config, &entry_tags), "the successor's snapshot is its entry's");
        assert_eq!(succ1_row.allocated.as_ref().and_then(|a| a.predecessor.clone()), succ1_allocation.predecessor, "the claim keeps its explicit provenance");
        assert_eq!(succ1_row.executable, Some(v2_binding.clone()));
        let retained_old = record_at(&*store, &old1)
            .await
            .unwrap()
            .expect("retained .old row stands");
        assert_eq!((retained_old.node_id.clone(), retained_old.inherited_by.clone()), (pred1_id.clone(), Some(succ1_id.clone())));
        assert_eq!(record_at(&*store, &name1).await.unwrap().map(|r| r.node_id), Some(succ1_id.clone()), "a current-pair lookup selects the successor's ID");

        // 4. A repeated cutover finds the successor's own claim and writes nothing: its snapshot is the
        //    first claim's, never re-taken, and a later node or caller update in its own property record
        //    is never written over.
        let RowCreated::HeldByNode(held_row) = claim1().await.expect("repeated successor claim") else {
            panic!("expected HeldByNode on repeated claim");
        };
        assert_eq!(held_row.initial_properties, Some(snapshot1.clone()), "a repeated cutover keeps the first claim's snapshot");

        // 5. Delete-instead-of-rename path (`mesh1.rpc.2`): predecessor is deleted, and the successor's
        //    claim names its exact birth: its snapshot is still its own entry's.
        rows.deleted(&pred2, &rdm_core::OperationId::node("node.delete", &pred2.node_id))
            .await
            .expect("delete pred2");
        let succ2_id = NodeId::mint();
        let succ2_allocation = provenance(&pred2, &name2);
        let entry2_tags = BTreeSet::from(["tier-c".to_string()]);
        let entry2_config = BTreeMap::new();
        let claim2 = || rows.create_row_with_spec(&name2, "mesh1", rdm_core::NodeKind::RpcNode, &succ2_id, &succ2_allocation.operation, Some(&succ2_allocation), Some(&v2_binding), &entry2_tags, &entry2_config, None, RowFor::Successor);
        let RowCreated::Written { row: succ2_row, seeded_from: seeded2 } = claim2().await.expect("successor claim over deleted predecessor") else {
            panic!("expected RowCreated::Written for succ2");
        };
        assert_eq!(seeded2, Some(pred2_id));
        // Q305 over a Deleted predecessor too: an entry naming no config is an empty map, never a
        // predecessor's, and its tags are the entry's.
        let snapshot2 = succ2_row.initial_properties.clone().expect("the claim retains the launch snapshot");
        assert_eq!((&snapshot2.config, &snapshot2.tags), (&entry2_config, &entry2_tags), "an empty entry config is empty: no predecessor's is a fallback");

        // An executor lost after the claim and before its receipt (Q305): the re-run of the claim
        // (PrepareStorage again, the same birth) finds its own lifecycle record and writes nothing over
        // it. Must NOT happen: the re-run reseeding the snapshot.
        let rerun = claim2().await.expect("the re-run claim");
        assert!(matches!(&rerun, RowCreated::HeldByNode(row) if row.initial_properties.as_ref() == Some(&snapshot2)), "the re-run finds its own birth's claim: {rerun:?}");
    });

    drop(rt);

    let inherited_spans = capture.named("rdm.node_admin.node.update.via-inherited-row");
    let outcomes: Vec<&str> = inherited_spans
        .iter()
        .filter_map(|s| s.attributes.get("outcome").map(String::as_str))
        .collect();
    assert_eq!(
        outcomes,
        vec!["inherited", "already-inherited", "none-retained", "none-retained"],
        "the re-run claim (Q305) reads the .old row again and finds none retained"
    );
}
