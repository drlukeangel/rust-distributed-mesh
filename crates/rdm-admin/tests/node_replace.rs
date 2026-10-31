//! Functional cell for `node.replace` (`docs/architecture/lifecycles/node-replace.md`).

use rdm_admin::model::{
    Fabric, IncarnationId, Mesh, Node, NodeId, NodeStatus, PathName, ProviderKind, ScopeStatus,
};
use rdm_admin::node_rows::{record_at, ClaimFor, Claimed, NodeRows, RowRefused, WriteRecord};
use rdm_admin::storage::{MemoryNodesStorage, NameHeld, NodeRow, NodesStorage};
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
/// "Inherited: Config and tags are inherited from the retained `<name>.old` row, or from the Deleted
/// predecessor row at `<name>` when the predecessor was deleted. The successor's tag-index entries use
/// its own id. Retained predecessor labels are removed from the active index, not erased from its row.
/// A repeated cutover keeps the successor's already-inherited values and never overwrites a later update."
/// Page: docs/architecture/lifecycles/node-replace.md:36
/// "3. node.create of node[2] at `<name>`, free only because the row above moved the key: a re-birth
/// with a new node.id, incarnation and the specified bin... A claim that finds `<name>` held is refused
/// `NameHeld { by: node[1] }` and the Build stops at this step by name."
#[test]
fn node_replace_refuses_held_name_until_predecessor_renamed_or_deleted_and_takes_its_entrys_config_and_tags() {
    let capture = Capture::new("node_replace_inherits_config_and_tags");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let admin_name: PathName = "mesh1.admin.1".parse().unwrap();
        let mut admin = Node::allocated(admin_name.clone());
        admin.status = NodeStatus::Ready;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        admin.incarnation_id = Some(IncarnationId::mint());
        let admin_id = admin.node_id.clone();

        let name1: PathName = "mesh1.rpc.1".parse().unwrap();
        let old1 = name1.renamed();
        let mut pred1 = Node::allocated(name1.clone());
        pred1.status = NodeStatus::Ready;
        pred1.incarnation_id = Some(IncarnationId::mint());
        let pred1_id = pred1.node_id.clone();

        let name2: PathName = "mesh1.rpc.2".parse().unwrap();
        let mut pred2 = Node::allocated(name2.clone());
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

        let store = Arc::new(MemoryNodesStorage::default());
        let mut pred1_row = NodeRow::new(name1.clone(), pred1_id.clone(), NodeStatus::Ready);
        pred1_row
            .config
            .insert("topic".into(), "orders-v1".into());
        pred1_row.tags = BTreeSet::from(["tier-a".to_string(), "canary".to_string()]);
        store.write_record(&pred1_row).await.unwrap();

        let mut pred2_row = NodeRow::new(name2.clone(), pred2_id.clone(), NodeStatus::Ready);
        pred2_row
            .config
            .insert("topic".into(), "payments-v1".into());
        pred2_row.tags = BTreeSet::from(["tier-b".to_string()]);
        store.write_record(&pred2_row).await.unwrap();

        let rows = NodeRows::new(admin_name, admin_id, view, store.clone());
        rows.written().hold(&pred1_row);
        rows.written().hold(&pred2_row);

        assert_eq!(
            store.tagged("tier-a").await.unwrap(),
            BTreeSet::from([pred1_id.clone()])
        );

        let succ1_id = NodeId::mint();
        let v2_binding = Binding {
            launch_id: "rpc_node".into(),
            executable: "/bin/rdm-rpc-node-v2".into(),
            sha256: "2".repeat(64),
            image: None,
        };

        // 1. Before predecessor node[1] releases `mesh1.rpc.1`, successor node[2]'s claim is refused NameHeld.
        let refused = rows
            .claim(
                &name1,
                &succ1_id,
                "node.create:mesh1.rpc.1",
                None,
                Some(&v2_binding),
                ClaimFor::Successor,
            )
            .await;
        assert_eq!(
            refused,
            Err(RowRefused::NameHeld(NameHeld {
                name: name1.clone(),
                by: pred1_id.clone(),
            }))
        );

        // 2. Rename predecessor node[1] to `mesh1.rpc.1.old` (step 2 of node-replace.md).
        let renamed_row = rows.rename_row(&pred1, &old1).await.expect("rename_row");
        assert_eq!(renamed_row.name, old1);
        assert_eq!(renamed_row.renamed_from, Some(name1.clone()));
        assert!(
            record_at(&*store, &name1).await.unwrap().is_none(),
            "mesh1.rpc.1 is free after rename_row"
        );

        // 3. Successor node[2] claims `mesh1.rpc.1` with ClaimFor::Successor: its row takes its Build
        //    entry's config and tags (Q305), never `mesh1.rpc.1.old`'s; it indexes the entry's tags under
        //    succ1_id and removes pred1_id from the active tag index while keeping tags on the retained
        //    `.old` row with `inherited_by = Some(succ1_id)`.
        let entry_tags = BTreeSet::from(["tier-a".to_string(), "canary".to_string()]);
        let entry_config = BTreeMap::from([("zone".to_string(), "b".to_string())]);
        let claimed1 = rows
            .claim_spec(
                &name1,
                &succ1_id,
                "node.create:mesh1.rpc.1",
                None,
                Some(&v2_binding),
                &entry_tags,
                &entry_config,
                None,
                ClaimFor::Successor,
            )
            .await
            .expect("successor claim after rename");
        let Claimed::Written {
            row: succ1_row,
            seeded_from,
        } = claimed1
        else {
            panic!("expected Claimed::Written");
        };
        assert_eq!(seeded_from, Some(pred1_id.clone()));
        // Q305: the successor's config and tags are its Build entry's, never the predecessor's.
        assert_ne!(pred1_row.config, entry_config, "the cell's entry differs from the predecessor's config");
        assert_eq!(succ1_row.config, entry_config, "the successor's config is its entry's, not the predecessor's");
        assert_eq!(succ1_row.tags, entry_tags, "the successor's tags are its entry's");
        assert_eq!(succ1_row.executable, Some(v2_binding.clone()));

        assert_eq!(
            store.tagged("tier-a").await.unwrap(),
            BTreeSet::from([succ1_id.clone()]),
            "active tag index must list successor id only, removing retained predecessor id"
        );
        assert_eq!(
            store.tagged("canary").await.unwrap(),
            BTreeSet::from([succ1_id.clone()])
        );

        let retained_old = record_at(&*store, &old1)
            .await
            .unwrap()
            .expect("retained .old row stands");
        assert_eq!(retained_old.inherited_by, Some(succ1_id.clone()));
        assert_eq!(
            retained_old.tags, pred1_row.tags,
            "retained predecessor labels are removed from the active index, not erased from its row"
        );

        // 4. Repeated cutover keeps successor's already-inherited values and never overwrites a later update.
        rows.written()
            .update(&*store, &succ1_id, None, |r| {
                r.config.insert("topic".into(), "orders-v2-updated".into());
            })
            .await
            .unwrap();
        let repeated = rows
            .claim(
                &name1,
                &succ1_id,
                "node.create:mesh1.rpc.1",
                None,
                Some(&v2_binding),
                ClaimFor::Successor,
            )
            .await
            .expect("repeated successor claim");
        let Claimed::HeldByNode(held_row) = repeated else {
            panic!("expected HeldByNode on repeated claim");
        };
        assert_eq!(
            held_row.config.get("topic").map(String::as_str),
            Some("orders-v2-updated"),
            "repeated cutover must never overwrite a later update on the successor"
        );

        // 5. Delete-instead-of-rename path (`mesh1.rpc.2`): predecessor is deleted, successor inherits
        //    config and tags from the Deleted predecessor row at `mesh1.rpc.2`.
        rows.deleted(&pred2, "node.delete:mesh1.rpc.2")
            .await
            .expect("delete pred2");
        assert!(
            store.tagged("tier-b").await.unwrap().is_empty(),
            "deleted predecessor labels leave active tag index"
        );

        let succ2_id = NodeId::mint();
        let entry2_tags = BTreeSet::from(["tier-c".to_string()]);
        let entry2_config = BTreeMap::new();
        let claimed2 = rows
            .claim_spec(
                &name2,
                &succ2_id,
                "node.create:mesh1.rpc.2",
                None,
                Some(&v2_binding),
                &entry2_tags,
                &entry2_config,
                None,
                ClaimFor::Successor,
            )
            .await
            .expect("successor claim over deleted predecessor");
        let Claimed::Written {
            row: succ2_row,
            seeded_from: seeded2,
        } = claimed2
        else {
            panic!("expected Claimed::Written for succ2");
        };
        assert_eq!(seeded2, Some(pred2_id));
        // Q305 over a Deleted predecessor too: an entry naming no config is an empty map, never the
        // predecessor's, and its tags are the entry's.
        assert_ne!(pred2_row.config, entry2_config, "the predecessor held config the entry does not name");
        assert_eq!(succ2_row.config, entry2_config, "an empty entry config is empty: the predecessor's is not a fallback");
        assert_eq!(succ2_row.tags, entry2_tags);
        assert!(store.tagged("tier-b").await.unwrap().is_empty(), "the predecessor's label is not the successor's");
        assert_eq!(store.tagged("tier-c").await.unwrap(), BTreeSet::from([succ2_id.clone()]));

        // An executor lost after the row write and before its receipt (Q305): the gateway's own
        // update writes the ports it bound, and the re-run of the claim (PrepareStorage again, the same
        // birth) finds its own row and writes nothing over it. Must NOT happen: the re-run reseeding the
        // row with the entry's config and erasing the gateway's ports.
        let ports = BTreeMap::from([("rafka.kafka-endpoint".to_string(), "10.231.7.5:60371".to_string())]);
        rows.written().update(&*store, &succ2_id, None, |r| r.config = ports.clone()).await.expect("the successor's own update").expect("its row");
        let rerun = rows
            .claim_spec(&name2, &succ2_id, "node.create:mesh1.rpc.2", None, Some(&v2_binding), &entry2_tags, &entry2_config, None, ClaimFor::Successor)
            .await
            .expect("the re-run claim");
        assert!(matches!(rerun, Claimed::HeldByNode(_)), "the re-run finds its own birth's row: {rerun:?}");
        assert_eq!(rdm_admin::node_rows::record_at(&*store, &name2).await.unwrap().expect("the successor's row").config, ports, "the gateway's own update stands over the re-run claim");
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
