//! product=mesh, feature=node-records, subfeature=own-properties, rung=MN, provider=process.

use rdm_client::{NodeStatus, NodeView};
use rdm_core::{NodeId, NodeKind, PathName};
use rdm_node::node_records::{NodeLifecycle, NodeRecordsStorage as _};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

// @feature: node-records
/// CONTRACT (i150 R3/R7/R8): Builds apply entry properties to an existing node; a subsequent
/// own update stands through reattaching the same Build; a later Build overrides it. Publishing
/// the own map is one node-owned durable write, with no upward config delivery or copied atlas.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_applies_properties_and_a_nodes_later_update_stands() {
    let test = "a_build_applies_properties_and_a_nodes_later_update_stands";
    let owner = Owner {
        product: "mesh".into(),
        feature: "node-records".into(),
        subfeature: "build-properties".into(),
        rung: "MN".into(),
        provider: "process".into(),
        test: test.into(),
    };
    let mut estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;
    let shape = [("mesh1", 1, 2)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settled_views(&shape).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let subject: rdm_core::PathName = rdm_core::PathName::new("mesh1.rpc", "2").unwrap();
    let change = |value: &'static str| {
        let subject = subject.clone();
        move |current: &rdm_test::executables::Current, _: &rdm_test::executables::Executables| {
            current
                .topology
                .values()
                .flat_map(|mesh| mesh.values())
                .cloned()
                .map(|mut spec| {
                    if spec.path == subject {
                        spec.config = [("entry".into(), value.into())].into();
                        spec.tags = [value.into()].into();
                    }
                    spec
                })
                .collect()
        }
    };
    let stream = estate
        .submit_named(
            "Build A applies an existing node's properties",
            change("build-a"),
        )
        .await;
    let applied = estate.follow(stream).await;
    let before = estate.node_view(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await;
    assert_eq!(before.config, [("entry".into(), "build-a".into())].into());
    assert_eq!(before.tags, ["build-a".into()].into());
    let answer = estate.probe(&[
        "own-config",
        "--target",
        &format!("exact:{}", before.node_id),
        "--value",
        "listener.data=127.0.0.1:45005",
    ]);
    assert_eq!(answer["own_config"], "config-updated", "{answer}");
    let own: BTreeMap<String, String> = [("listener.data".into(), "127.0.0.1:45005".into())].into();
    let again = estate
        .carrier()
        .await
        .reattach(&applied.build_id)
        .await
        .expect("the same Build reattaches");
    estate.follow(again.stream).await;
    let after = estate.node_view(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await;
    assert_eq!(
        after.node_id, before.node_id,
        "in-place property operations preserve identity"
    );
    assert_eq!(
        after.config, own,
        "reattaching the same Build preserves the later own map"
    );
    assert_eq!(after.tags, before.tags);
    let stream = estate
        .submit_named(
            "Build B overrides the existing node's own properties",
            change("build-b"),
        )
        .await;
    estate.follow(stream).await;
    let later = estate.node_view(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await;
    assert_eq!(later.node_id, before.node_id);
    assert_eq!(later.config, [("entry".into(), "build-b".into())].into());
    assert_eq!(later.tags, ["build-b".into()].into());
    estate.artifact(
        "build-property-readbacks",
        &json!({"build_id": applied.build_id, "before": before, "own": after, "later": later}),
    );
    estate.stop().await;
    let spans = estate.spans();
    let own_write = rdm_test::estate::named(&spans, "rdm.node.record.update.via-properties")
        .into_iter()
        .find(|s| s["attributes"]["node_id"] == before.node_id.as_str())
        .expect("the production node-owned write occurred");
    estate.record_trace_url(own_write["trace_id"].as_str().unwrap());
    assert!(
        rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-config")
            .into_iter()
            .all(|s| s["attributes"]["subject"] != "mesh1.rpc.2"
                || s["attributes"]["apply"] != "own"),
        "an own update must not deliver the map to a mesh-primary"
    );
    assert!(
        rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-config-push").is_empty(),
        "node properties must not be copied upward to the fabric-primary"
    );
}

// @feature: node-records
/// CONTRACT (i150 R3, restart canary): a real node acknowledges its own property write only
/// after that node has persisted it. Killing that process preserves the acknowledged map for
/// recovery under a new ID; the retained log names the original node as the writer. Admin
/// forwarding of the map cannot stand in for that node-owned durable write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn own_properties_survive_node_restart() {
    let test = "own_properties_survive_node_restart";
    let owner = Owner {
        product: "mesh".into(),
        feature: "node-records".into(),
        subfeature: "own-properties".into(),
        rung: "MN".into(),
        provider: "process".into(),
        test: test.into(),
    };
    let mut estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;
    let shape = [("mesh1", 1, 2)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settled_views(&shape).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let before = estate
        .node_views()
        .await
        .into_iter()
        .find(|n| n.name == rdm_core::PathName::new("mesh1.rpc", "2").unwrap())
        .expect("the non-primary subject is ready");
    let expected: BTreeMap<String, String> =
        [("listener.data".into(), "127.0.0.1:45005".into())].into();
    let answer = estate.probe(&[
        "own-config",
        "--target",
        &format!("exact:{}", before.node_id),
        "--value",
        "listener.data=127.0.0.1:45005",
    ]);
    assert_eq!(
        answer["own_config"], "config-updated",
        "the own update was acknowledged: {answer}"
    );
    estate.artifact(
        "acknowledged-properties",
        &json!({"node_id": before.node_id, "config": expected}),
    );
    // Evidence export is batched. Observe the completed write span before SIGKILL so this
    // real-process canary retains its flame; storage durability is independently checked by
    // the forced-exit storage canary, which performs no telemetry flush or wait.
    let write_trace = wait_for(
        "the acknowledged own-write span is exported",
        Duration::from_secs(10),
        || async {
            estate
                .spans_named(&["rdm.node.record.update.via-properties"])
                .into_iter()
                .find(|s| s["attributes"]["node_id"] == before.node_id.as_str())
                .and_then(|s| s["trace_id"].as_str().map(str::to_string))
        },
    )
    .await;
    estate.kill_node(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await;
    let recovered = wait_for(
        "a recovered node exposes the acknowledged config",
        crate::node_recovery_due(),
        || async {
            estate.node_views().await.into_iter().find(|n| {
                n.name == rdm_core::PathName::new("mesh1.rpc", "2").unwrap()
                    && n.node_id != before.node_id
                    && n.status == rdm_client::NodeStatus::Ready
            })
        },
    )
    .await;
    let records = estate
        .fabric_store_root()
        .join(rdm_node::node_records::NODE_RECORDS_FILE);
    estate.stop().await;
    assert_eq!(
        recovered.config, expected,
        "the complete acknowledged map survives forced process loss"
    );
    let log = rdm_node::store_log::read_file(&records).unwrap_or_else(|e| panic!("the node must persist its own properties before acknowledging them; retained record log: {e}"));
    let own = log
        .iter()
        .rev()
        .find(|line| line.kind() == "node-properties" && line.key == before.node_id.as_str())
        .expect("the original node's acknowledged record survives its process exit");
    let record: rdm_node::node_records::NodeProperties = own.decode(&records).unwrap();
    assert_eq!(
        record.config, expected,
        "the log contains the entire acknowledged map"
    );
    assert_eq!(
        own.op.as_ref().unwrap().node_id,
        before.node_id.to_string(),
        "the node itself wrote the acknowledged record"
    );
    let spans = estate.spans();
    let writes = rdm_test::estate::named(&spans, "rdm.node.record.update.via-properties");
    let written = writes
        .into_iter()
        .find(|s| s["attributes"]["node_id"] == before.node_id.as_str())
        .expect("the production own-write span proves this path executed");
    assert_eq!(
        written["trace_id"].as_str(),
        Some(write_trace.as_str()),
        "the retained flame is the observed own write"
    );
    estate.record_trace_url(&write_trace);
}

// ---- The i150 plan's named acceptance cells (docs/plans/i150-node-writes-lifecyle-updates.md) ----


fn owner_of(test: &str, subfeature: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-records".into(), subfeature: subfeature.into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

/// `estate` reconciled to `shape` (each `(mesh, node_admin, rpc_node)`), its views settled and its
/// fabric ready: a cell acts on its nodes only once the fabric has first reached ready (Q74).
async fn estate_at(estate: Estate, shape: &[(&str, u32, u32)]) -> Estate {
    estate.reconcile_shape("fabric1", shape).await;
    estate.settled_views(shape).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate
}

fn pair(path: &str, name: &str) -> PathName {
    PathName::new(path, name).unwrap()
}

/// node.get by the separate pair: every node the fabric-primary lists at `at`.
async fn at_pair(estate: &Estate, at: &PathName) -> Vec<NodeView> {
    estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("path", [at.path.clone()]).eq("name", [at.name.clone()])).await.unwrap_or_else(|e| panic!("node.get [path: {}, name: {}]: {e:?}", at.path, at.name))
}

/// node.get by the node id.
async fn by_id(estate: &Estate, id: &NodeId) -> Vec<NodeView> {
    estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("id", [id.to_string()])).await.unwrap_or_else(|e| panic!("node.get [id: {id}]: {e:?}"))
}

/// The exact birth of `v`, as a caller names it.
fn birth_of(v: &NodeView) -> rdm_client::ExactBirth {
    rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(v.node_id.clone()), node_id: v.node_id.clone(), incarnation: v.incarnation_id.clone().expect("a launched birth names its incarnation") }
}

async fn fabric_primary(estate: &Estate) -> NodeId {
    estate.node_views().await.into_iter().find(|n| n.is_fabric_primary).expect("the view names a fabric-primary").node_id
}

/// node.config.update of `v`'s exact birth, sent to the fabric-primary, which sends it down through the
/// node's mesh-primary (node-config-update.md "A caller's update" step 1).
async fn config_update(estate: &Estate, v: &NodeView, config: BTreeMap<String, String>) -> rdm_core::status::StatusReply {
    let fp = fabric_primary(estate).await;
    let carrier = estate.carrier().await;
    rdm_client::NodeRpc::new(carrier.client()).config_update(&fp, &rdm_transport::RouteChoice::Direct, &birth_of(v), config, &rdm_transport::CallOptions::default()).await.unwrap_or_else(|e| panic!("node.config.update of {:?} at the fabric-primary: {e:?}", v.name))
}

/// node.tag.create of `labels` on `v`'s exact birth, sent to the fabric-primary (commands go down, Q203).
async fn tags_create(estate: &Estate, v: &NodeView, labels: &BTreeSet<String>) -> rdm_core::status::StatusReply {
    let fp = fabric_primary(estate).await;
    let carrier = estate.carrier().await;
    rdm_client::NodeRpc::new(carrier.client()).tags_create(&fp, &rdm_transport::RouteChoice::Direct, &birth_of(v), labels, &rdm_transport::CallOptions::default()).await.unwrap_or_else(|e| panic!("node.tag.create of {:?} at the fabric-primary: {e:?}", v.name))
}

/// `steps` as one Build of node primitives, followed to its Complete.
async fn steps_to_complete(estate: &Estate, steps: Vec<rdm_core::build::Step>) -> rdm_client::Accepted {
    let mut stream = estate.nodes_obj().await.steps(steps).await.unwrap_or_else(|e| panic!("the steps were not accepted: {e}"));
    Estate::complete(&mut stream).await
}

/// The own update of `v`'s config through the testkit probe: the node's own local durable write.
fn own_update(estate: &Estate, v: &NodeView, value: &str) {
    let answer = estate.probe(&["own-config", "--target", &format!("exact:{}", v.node_id), "--value", value]);
    assert_eq!(answer["own_config"], "config-updated", "the own update was acknowledged: {answer}");
}

fn records_file(estate: &Estate) -> std::path::PathBuf {
    estate.fabric_store_root().join(rdm_node::node_records::NODE_RECORDS_FILE)
}

/// Every lifecycle record of `node` in the fabric's node records, oldest first, whoever wrote it.
fn lifecycles(estate: &Estate, node: &NodeId) -> Vec<NodeLifecycle> {
    let file = records_file(estate);
    let prefix = format!("{node}/");
    rdm_node::store_log::read_file(&file)
        .unwrap_or_else(|e| panic!("the node records {} read whole: {e}", file.display()))
        .iter()
        .filter(|l| l.kind() == "node-lifecycle" && l.key.starts_with(&prefix))
        .map(|l| l.decode(&file).unwrap_or_else(|e| panic!("a lifecycle record of {node} decodes: {e}")))
        .collect()
}

/// The latest lifecycle record of `node` written under the writer `writer`'s node id.
fn written_by(estate: &Estate, node: &NodeId, writer: &NodeId) -> Option<NodeLifecycle> {
    lifecycles(estate, node).into_iter().rev().find(|r| r.writer.node_id == *writer)
}

fn status_of(r: &NodeLifecycle) -> String {
    serde_json::to_value(r.status).unwrap().as_str().unwrap_or_default().to_string()
}

/// The fabric's node records as a late-completing provider put commits them: the put of `record` at
/// its own key, issued earlier, lands now.
async fn late_lifecycle_put(estate: &Estate, record: &NodeLifecycle) {
    let store = rdm_node::node_records::FileNodeRecords::open(&rdm_node::store_root::StoreRoot::at(&estate.fabric_store_root())).await.expect("the fabric's node records open");
    store.put_lifecycle(record).await.expect("the late lifecycle put is durable");
}

async fn late_properties_put(estate: &Estate, record: &rdm_node::node_records::NodeProperties) {
    let store = rdm_node::node_records::FileNodeRecords::open(&rdm_node::store_root::StoreRoot::at(&estate.fabric_store_root())).await.expect("the fabric's node records open");
    store.put_properties(record).await.expect("the late property put is durable");
}

/// Each named field of `a` and `b` (both serialized) that differs, as `field: a -> b`; a field absent
/// from either is named too, so a renamed field cannot pass as equal.
fn differing(a: &impl serde::Serialize, b: &impl serde::Serialize, fields: &[&str]) -> Vec<String> {
    let (a, b) = (serde_json::to_value(a).unwrap(), serde_json::to_value(b).unwrap());
    fields
        .iter()
        .filter_map(|k| match (a.get(*k), b.get(*k)) {
            (Some(x), Some(y)) if x == y => None,
            (x, y) => Some(format!("{k}: {x:?} -> {y:?}")),
        })
        .collect()
}

/// Every field of `a` and `b` (both serialized objects) but `skip` that differs.
fn differing_except(a: &impl serde::Serialize, b: &impl serde::Serialize, skip: &[&str]) -> Vec<String> {
    let (av, bv) = (serde_json::to_value(a).unwrap(), serde_json::to_value(b).unwrap());
    let keys: BTreeSet<String> = av.as_object().unwrap().keys().chain(bv.as_object().unwrap().keys()).filter(|k| !skip.contains(&k.as_str())).cloned().collect();
    differing(a, b, &keys.iter().map(String::as_str).collect::<Vec<_>>())
}

fn attr(sp: &Value, k: &str) -> String {
    sp["attributes"][k].as_str().map(str::to_string).unwrap_or_else(|| sp["attributes"][k].to_string())
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or_default()
}

fn end_ns(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or_default()
}

/// Wait until `who` is listed ready and holding its mesh's primary role.
async fn primary_is(estate: &Estate, who: &NodeView) {
    estate
        .until_view(&format!("{:?} ({}) holds {}'s primary role", who.name, who.node_id, who.mesh), Duration::from_secs(120), |views| {
            views.iter().any(|n| n.node_id == who.node_id && n.is_primary && n.status == NodeStatus::Ready).then_some(())
        })
        .await
}

/// The shared resolver's record of a read of `subject` (i150 § OTLP span-assertion bar, planned
/// `rdm.node_admin.node.resolve.via-lifecycle-record`: subject ID, selected writer ID/incarnation,
/// outcome, reason) that selected `writer`.
fn resolved<'a>(spans: &'a [Value], subject: &NodeId, writer: &NodeId) -> Option<&'a Value> {
    named(spans, "rdm.node_admin.node.resolve.via-lifecycle-record").into_iter().find(|sp| attr(sp, "subject_id") == subject.as_str() && attr(sp, "writer_node_id") == writer.as_str())
}

/// The lifecycle fields a writer reconstructs for a subject it takes over (i150 R10, R12): identity,
/// placement, executable and storage, never the status a late put carries.
const RECONSTRUCTED: [&str; 14] = ["node_id", "path", "name", "kind", "node_type", "fabric_id", "mesh_id", "incarnation_id", "executable", "storage_persistence", "placement", "topology_storage_provider", "initial_properties", "allocated"];

// @feature: node-records
/// CONTRACT (i150 R1, e1; node-identity-and-state.md "A path is a path": "Replacement renames the
/// predecessor to `<name>-old` at the same path"; node-replace.md steps 1-3): a move keeps the node's
/// identity. `mesh1.rpc` / `2` is replaced keeping its predecessor: the predecessor is then listed at
/// `mesh1.rpc` / `2-old` under the same node id, incarnation and process, stopped, and the successor
/// is a new node id at `mesh1.rpc` / `2`. node.get by each separate pair names exactly one node, and
/// by the predecessor's id names it at the moved pair. Its lifecycle record keeps its node id and
/// incarnation and names the moved pair with the pair it came from; its own property record stays at
/// its node id; the rename span names the two fields it moved to. Must NOT happen: a move minting an
/// identity, or one pair naming two births.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_path_move_preserves_the_node_identity() {
    let test = "a_path_move_preserves_the_node_identity";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "path-move"), "fabric1", "mesh1").await, &[("mesh1", 1, 2)]).await;
    let subject = pair("mesh1.rpc", "2");
    let moved = subject.renamed().expect("the reserved name fits");
    let before = estate.node_view(&subject).await;
    let pid = estate.pid_of(&subject).await;
    steps_to_complete(&estate, rdm_test::steps::replace_keeping_predecessor(&subject, &before.mesh, before.kind).unwrap()).await;
    let at_moved = at_pair(&estate, &moved).await;
    assert_eq!(at_moved.len(), 1, "node.get [path, name] names one node at the moved pair: {at_moved:#?}");
    let predecessor = at_moved[0].clone();
    assert_eq!((&predecessor.node_id, &predecessor.incarnation_id, predecessor.kind, &predecessor.mesh), (&before.node_id, &before.incarnation_id, before.kind, &before.mesh), "the move keeps the node's identity");
    assert!(predecessor.stopped, "the moved predecessor is stopped: {predecessor:?}");
    assert_eq!(estate.pid_of(&moved).await, pid, "the moved node is the same process");
    let at_name = at_pair(&estate, &subject).await;
    assert_eq!(at_name.len(), 1, "node.get [path, name] names one node at the original pair: {at_name:#?}");
    let successor = at_name[0].clone();
    assert_ne!(successor.node_id, before.node_id, "the successor at the original pair is a new node id");
    assert_eq!(successor.status, NodeStatus::Ready, "{successor:?}");
    let named_by_id: Vec<PathName> = by_id(&estate, &before.node_id).await.into_iter().map(|n| n.name).collect();
    assert_eq!(named_by_id, vec![moved.clone()], "node.get by the moved node's id names it at the moved pair");
    let record = lifecycles(&estate, &before.node_id).pop().expect("the moved node's lifecycle record");
    assert_eq!(
        (&record.node_id, Some(&record.incarnation_id), record.path.as_str(), record.name.as_str()),
        (&before.node_id, before.incarnation_id.as_ref(), moved.path.as_str(), moved.name.as_str()),
        "the moved node's lifecycle record keeps its identity and names the moved pair: {record:?}"
    );
    assert_eq!(record.replacement_from.as_ref(), Some(&subject), "the retained predecessor's record names the pair it moved from: {record:?}");
    let own = estate.node_properties(&before.node_id).expect("the moved node's own property record stays at its node id");
    assert_eq!(Some(&own.birth.incarnation_id), before.incarnation_id.as_ref(), "{own:?}");
    estate.artifact("path-move.json", &json!({"before": before, "predecessor": predecessor, "successor": successor, "record": record}));
    estate.stop().await;
    let spans = estate.spans();
    let renames: Vec<&Value> = named(&spans, "rdm.node_admin.name.update.via-writer").into_iter().filter(|sp| attr(sp, "node_id") == before.node_id.as_str()).collect();
    let moved_to = renames
        .iter()
        .find(|sp| attr(sp, "to_path") == moved.path && attr(sp, "to_name") == moved.name && attr(sp, "outcome") == "name-updated")
        .unwrap_or_else(|| panic!("the rename of {} to the moved pair is recorded with its two fields: {renames:#?}", before.node_id));
    estate.record_trace_url(moved_to["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R2, R5, R11; e2, e5; query.md §1: "A current-pair lookup resolves the path/name
/// claim to a node ID ... neither another writer's value nor a raw last-written-path index supplies a
/// fallback"): `mesh1.rpc` / `2` is replaced and its predecessor deleted. Then the deleted birth's
/// late writes land: an earlier put of its lifecycle record, Ready at the pair, commits under the
/// writer that issued it, and a put of its own properties commits with another map. node.get by the
/// pair names the successor alone with its own config, the deleted birth is listed live nowhere, and a
/// later Build naming the pair applies its entry to the successor, never to the deleted birth's
/// record. The shared resolver records the read that selected the successor's current writer. Must NOT
/// happen: the late write making the pair name the deleted birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_birth_cannot_hide_its_successor() {
    let test = "a_deleted_birth_cannot_hide_its_successor";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "deleted-birth"), "fabric1", "mesh1").await, &[("mesh1", 1, 2)]).await;
    let subject = pair("mesh1.rpc", "2");
    let before = estate.node_view(&subject).await;
    steps_to_complete(&estate, rdm_test::steps::replace(&subject, &before.mesh, before.kind).unwrap()).await;
    let successor = estate.until_node(&subject, "the successor is ready at the pair", Duration::from_secs(120), |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != before.node_id).cloned()).await;
    let writer = estate.node_views().await.into_iter().find(|n| n.mesh == "mesh1" && n.kind == NodeKind::NodeAdmin && n.is_primary).expect("mesh1's primary").node_id;
    // The deleted birth's late writes.
    let earlier = lifecycles(&estate, &before.node_id)
        .into_iter()
        .rev()
        .find(|r| status_of(r) == "ready" && r.path == subject.path && r.name == subject.name)
        .expect("an earlier Ready lifecycle record of the predecessor at the pair");
    late_lifecycle_put(&estate, &earlier).await;
    let late: BTreeMap<String, String> = [("late".into(), "predecessor".into())].into();
    let mut late_properties = estate.node_properties(&before.node_id).expect("the predecessor's own property record");
    late_properties.config = late.clone();
    late_properties_put(&estate, &late_properties).await;
    let at = at_pair(&estate, &subject).await;
    assert_eq!(
        at.iter().map(|n| (n.node_id.clone(), n.status)).collect::<Vec<_>>(),
        vec![(successor.node_id.clone(), NodeStatus::Ready)],
        "node.get by the pair names the successor alone after the deleted birth's late writes: {at:#?}"
    );
    assert_ne!(at[0].config, late, "the successor's config is its own, never the deleted birth's late map");
    for n in by_id(&estate, &before.node_id).await {
        assert!(!matches!(n.status, NodeStatus::Pending | NodeStatus::Ready | NodeStatus::ConnectionsDeleting) && n.name != subject, "the deleted birth is listed live nowhere and never at the pair: {n:?}");
    }
    // Acceptance reads the current record: a Build naming the pair applies to the successor alone.
    let entry: BTreeMap<String, String> = [("entry".into(), "after-late-writes".into())].into();
    let stream = estate
        .submit_named("a Build names the pair after the deleted birth's late writes", |current, _| {
            current.topology.values().flat_map(|mesh| mesh.values()).cloned().map(|mut spec| {
                if spec.path == subject {
                    spec.config = entry.clone();
                }
                spec
            }).collect()
        })
        .await;
    estate.follow(stream).await;
    let after = estate.node_view(&subject).await;
    assert_eq!((&after.node_id, &after.config), (&successor.node_id, &entry), "the Build's entry reaches the successor");
    assert_eq!(estate.node_properties(&before.node_id).expect("the deleted birth's record").config, late, "no write reaches the deleted birth's record");
    estate.artifact("deleted-birth.json", &json!({"before": before, "successor": successor, "late_lifecycle": earlier, "after": after}));
    estate.stop().await;
    let spans = estate.spans();
    let read = resolved(&spans, &successor.node_id, &writer).unwrap_or_else(|| {
        panic!(
            "the shared resolver records the read of the pair that selected the successor {} under its current writer {writer} (i150 OTLP table, rdm.node_admin.node.resolve.via-lifecycle-record; {} such spans)",
            successor.node_id,
            named(&spans, "rdm.node_admin.node.resolve.via-lifecycle-record").len()
        )
    });
    assert!(named(&spans, "rdm.node_admin.node.resolve.via-lifecycle-record").into_iter().all(|sp| attr(sp, "subject_id") != before.node_id.as_str() || attr(sp, "outcome") != "selected"), "no read selects the deleted birth");
    estate.record_trace_url(read["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R12, R13; e2; node-config-update.md "Deferred application and startup": "Existing
/// durable properties and application recognition survive startup"): every field of a node survives
/// its restart, compared whole, not sampled. `mesh1.rpc` / `2` updates its own config and is tagged.
/// 1. Stopped then started (the same birth): node.get lists every field as before but the measured
///    load and gossip counts and the member cert its start's join issues afresh (fabric-certs.md
///    "Every JoinNode issues a fresh member cert"), its property record is the same record, and its lifecycle record differs
///    only by the stop and start it recorded and that fresh cert. 2. Killed: the node recovered at the pair (a new node id
///    over the inherited data dir) lists the same pair, kind, mesh, config, tags, executable, storage,
///    placement, data dir and endpoint key; its own property record holds the config and tags; its
///    lifecycle record carries the same identity fields and names its predecessor. Must NOT happen: a
///    field reset by a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_node_fields_survive_node_restart() {
    let test = "all_node_fields_survive_node_restart";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "all-fields"), "fabric1", "mesh1").await, &[("mesh1", 1, 2)]).await;
    let subject = pair("mesh1.rpc", "2");
    let first = estate.node_view(&subject).await;
    own_update(&estate, &first, "listener.data=127.0.0.1:45005");
    let labels: BTreeSet<String> = ["rabbit".to_string()].into();
    let tagged = tags_create(&estate, &first, &labels).await;
    assert_eq!(tagged.name(), "tags-created", "{tagged:?}");
    let own: BTreeMap<String, String> = [("listener.data".into(), "127.0.0.1:45005".into())].into();
    let before = estate.until_node(&subject, "the node lists its own config and its tag", Duration::from_secs(30), |n| n.filter(|n| n.config == own && n.tags == labels).cloned()).await;
    let writer = estate.node_views().await.into_iter().find(|n| n.mesh == "mesh1" && n.kind == NodeKind::NodeAdmin && n.is_primary).expect("mesh1's primary").node_id;
    let properties = estate.node_properties(&before.node_id).expect("the node's own property record");
    let lifecycle = written_by(&estate, &before.node_id, &writer).expect("the node's lifecycle record under its mesh-primary");

    // 1. The same birth: stopped, then started.
    estate.stop_then_start(&subject).await;
    let started = estate
        .until_node(&subject, "the same birth is ready again", Duration::from_secs(60), |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id == before.node_id && n.incarnation_id == before.incarnation_id).cloned())
        .await;
    // fabric-certs.md "Every JoinNode issues a fresh member cert": the start's join (node-start.md step
    // 2) issues the same birth a new cert under the same issuer; every other listed field stands.
    let changed = differing_except(&before, &started, &["load", "gossip", "cert"]);
    assert!(changed.is_empty(), "every listed field survives the stop and start: {changed:#?}");
    let (was, now) = (before.cert.expect("the birth's member cert"), started.cert.expect("the started birth's member cert"));
    assert!(now.issued_at >= was.issued_at && now.issuer_expires_at == was.issuer_expires_at, "the start's join issues a fresh member cert under the same issuer: {was:?} -> {now:?}");
    assert_eq!(estate.node_properties(&before.node_id).expect("the property record"), properties, "the whole property record survives the stop and start");
    let restarted = written_by(&estate, &before.node_id, &writer).expect("the lifecycle record after the start");
    let changed = differing_except(&lifecycle, &restarted, &["operations", "declared", "cert"]);
    assert!(changed.is_empty(), "the lifecycle record differs only by the stop and start it records and the fresh member cert: {changed:#?}");
    let (was, now) = (lifecycle.cert.as_ref().expect("the record's member cert dates"), restarted.cert.as_ref().expect("the started record's member cert dates"));
    assert!(now.issued_at >= was.issued_at && now.issuer_expires_at == was.issuer_expires_at, "the record holds the fresh member cert of the start's join, under the same issuer: {was:?} -> {now:?}");

    // 2. Forced exit: recovered at the pair as a new node over the inherited data dir.
    estate.kill_node(&subject).await;
    let recovered = estate.until_node(&subject, "a recovered node is ready at the pair", crate::node_recovery_due(), |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != before.node_id).cloned()).await;
    let changed = differing(&before, &recovered, &["name", "kind", "mesh", "config", "tags", "executable", "storage_persistence", "topology_storage_provider", "placement", "data_dir", "endpoint_id", "properties"]);
    assert!(changed.is_empty(), "the recovered node lists every retained field: {changed:#?}");
    assert!(recovered.transport_addr.is_some(), "the recovered node lists the address it bound: {recovered:?}");
    let recovered_properties = wait_for("the recovered node's own property record", Duration::from_secs(30), || async { estate.node_properties(&recovered.node_id) }).await;
    assert_eq!((&recovered_properties.config, &recovered_properties.tags), (&properties.config, &properties.tags), "the recovered node's own record holds the config and tags");
    let recovered_lifecycle = lifecycles(&estate, &recovered.node_id).pop().expect("the recovered node's lifecycle record");
    let changed = differing(&lifecycle, &recovered_lifecycle, &["path", "name", "kind", "node_type", "fabric_id", "mesh_id", "executable", "storage_persistence", "placement", "topology_storage_provider"]);
    assert!(changed.is_empty(), "the recovered node's lifecycle record keeps every retained field: {changed:#?}");
    assert_eq!(recovered_lifecycle.predecessor.as_ref().map(|p| &p.node_id), Some(&before.node_id), "the recovered record names its predecessor: {recovered_lifecycle:?}");
    estate.artifact("all-fields.json", &json!({"before": before, "started": started, "recovered": recovered, "lifecycle": lifecycle, "recovered_lifecycle": recovered_lifecycle}));
    estate.stop().await;
    let spans = estate.spans();
    let write = named(&spans, "rdm.node.record.update.via-properties")
        .into_iter()
        .find(|sp| attr(sp, "node_id") == before.node_id.as_str() && attr(sp, "outcome") == "durable")
        .unwrap_or_else(|| panic!("the node's own update is one durable node-owned write ({})", before.node_id));
    estate.record_trace_url(write["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R6, e3; node-primary-update.md "Before installing or publishing a role that writes
/// lifecycle records, the candidate settles earlier issued writes of its writer birth"; approved
/// 4dbf01eba): mesh2's primary A hands its role to B (A stopped then started), then B hands it back
/// (B stopped then started): A regains the role as the same birth, so its lifecycle key is the one it
/// wrote before. A's regained record of itself carries the stop and start B recorded in its tenure.
/// Spans: A's regain preparation settled its issued writes, and the serialized lifecycle writer records
/// every put of writer A with its phase and completion; every put A issued before the regain ended
/// before the regain's first put began. Must NOT happen: a regain put overtaking an earlier put of the
/// same writer birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_regained_primary_waits_for_its_earlier_put() {
    let test = "a_regained_primary_waits_for_its_earlier_put";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "regain"), "fabric1", "mesh1").await, &[("mesh1", 1, 0), ("mesh2", 2, 1)]).await;
    let views = estate.node_views().await;
    let a = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().expect("mesh2's primary");
    let b = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.node_id != a.node_id).cloned().expect("mesh2's other admin");
    estate.stop_then_start(&a.name).await;
    primary_is(&estate, &b).await;
    estate.stop_then_start(&b.name).await;
    primary_is(&estate, &a).await;
    let regained = estate.node_view(&a.name).await;
    assert_eq!((&regained.node_id, &regained.incarnation_id), (&a.node_id, &a.incarnation_id), "A regains as the same birth");
    let b_of_a = written_by(&estate, &a.node_id, &b.node_id).expect("B recorded A's stop and start in its tenure");
    let a_of_a = wait_for("A's regained record of itself carries B's tenure", Duration::from_secs(30), || async {
        written_by(&estate, &a.node_id, &a.node_id).filter(|r| b_of_a.operations.iter().all(|op| r.operations.contains(op)) && r.is_primary)
    })
    .await;
    estate.artifact("regain.json", &json!({"a": a, "b": b, "b_of_a": b_of_a, "a_of_a": a_of_a}));
    estate.stop().await;
    let spans = estate.spans();
    let prepared: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-takeover-prepared").into_iter().filter(|sp| attr(sp, "node_id") == a.node_id.as_str() && attr(sp, "outcome") == "prepared").collect();
    let regain = *prepared.iter().max_by_key(|sp| start_ns(sp)).expect("A's regain preparation is recorded");
    assert_eq!(attr(regain, "settled"), "true", "A's regain settled its issued writes first: {regain}");
    let puts: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-lifecycle-record").into_iter().filter(|sp| attr(sp, "writer_node_id") == a.node_id.as_str() && !sp["attributes"]["phase"].is_null()).collect();
    assert!(!puts.is_empty(), "the serialized lifecycle writer records every put of writer {} with its subject, writer birth, phase and completion outcome (i150 OTLP table, rdm.node_admin.node.update.via-lifecycle-record)", a.node_id);
    let first_regain_put = puts.iter().filter(|sp| start_ns(sp) >= start_ns(regain)).map(|sp| start_ns(sp)).min().expect("the regain's puts are recorded");
    for p in puts.iter().filter(|sp| start_ns(sp) < start_ns(regain)) {
        assert!(end_ns(p) <= first_regain_put, "a put A issued before its regain ended before the regain's first put began: {p}");
    }
    estate.record_trace_url(regain["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R10, e3; node-primary-update.md steps 4-5, the frozen takeover snapshot; proposal
/// node-record.md R-NR8): mesh2's primary A is killed; B takes the role and is killed in its tenure; a
/// third writer C takes it. Then an earlier put of A's record of the member lands late at A's key. C's
/// record of every surviving mesh2 node reconstructs each identity, placement, executable and storage
/// field A recorded, its status is the member's live one, and node.get lists the member ready as the
/// same birth. The resolver records the member's read selecting writer C. Must NOT happen: a value
/// invented for a field A's record did not hold, or the late put's status read as current.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takeover_survives_mesh_primary_restart() {
    let test = "takeover_survives_mesh_primary_restart";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "takeover"), "fabric1", "mesh1").await, &[("mesh1", 1, 0), ("mesh2", 3, 1)]).await;
    let views = estate.node_views().await;
    let a = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().expect("mesh2's primary");
    let member = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::RpcNode).cloned().expect("mesh2's rpc node");
    let recorded: BTreeMap<NodeId, NodeLifecycle> = views.iter().filter(|n| n.mesh == "mesh2").map(|n| (n.node_id.clone(), written_by(&estate, &n.node_id, &a.node_id).unwrap_or_else(|| panic!("A's record of {:?}", n.name)))).collect();
    estate.kill_node(&a.name).await;
    let other_primary = |not: Vec<NodeId>| move |views: &[NodeView]| views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary && n.status == NodeStatus::Ready && !not.contains(&n.node_id)).cloned();
    let b = estate.until_view("another admin of mesh2 takes its primary role", Duration::from_secs(120), other_primary(vec![a.node_id.clone()])).await;
    estate.kill_node(&b.name).await;
    let c = estate.until_view("a third writer takes mesh2's primary role", Duration::from_secs(180), other_primary(vec![a.node_id.clone(), b.node_id.clone()])).await;
    let earliest = lifecycles(&estate, &member.node_id).into_iter().find(|r| r.writer.node_id == a.node_id).expect("A's earliest record of the member");
    late_lifecycle_put(&estate, &earliest).await;
    let c_of_member = wait_for("C's lifecycle record of the member", Duration::from_secs(60), || async { written_by(&estate, &member.node_id, &c.node_id) }).await;
    let changed = differing(&recorded[&member.node_id], &c_of_member, &RECONSTRUCTED);
    assert!(changed.is_empty(), "C reconstructs every field A recorded of the member: {changed:#?}");
    assert_eq!(status_of(&c_of_member), "ready", "C's record holds the member's live status, not the late put's {}: {c_of_member:?}", status_of(&earliest));
    for (id, earlier) in &recorded {
        if [&a.node_id, &b.node_id].contains(&id) {
            continue;
        }
        let current = written_by(&estate, id, &c.node_id).unwrap_or_else(|| panic!("C's record of {id}"));
        let changed = differing(earlier, &current, &RECONSTRUCTED);
        assert!(changed.is_empty(), "C reconstructs every field A recorded of {id}: {changed:#?}");
    }
    let listed = estate.node_view(&member.name).await;
    assert_eq!((&listed.node_id, &listed.incarnation_id, listed.status), (&member.node_id, &member.incarnation_id, NodeStatus::Ready), "node.get lists the member ready as the same birth");
    estate.artifact("takeover.json", &json!({"a": a, "b": b, "c": c, "late_put": earliest, "c_of_member": c_of_member}));
    estate.stop().await;
    let spans = estate.spans();
    assert!(
        named(&spans, "rdm.node_admin.primary.update.via-takeover-prepared").into_iter().any(|sp| attr(sp, "node_id") == c.node_id.as_str() && attr(sp, "outcome") == "prepared"),
        "C's takeover preparation completed"
    );
    let read = resolved(&spans, &member.node_id, &c.node_id).unwrap_or_else(|| panic!("the shared resolver records the member's read selecting writer {} (i150 OTLP table, rdm.node_admin.node.resolve.via-lifecycle-record)", c.node_id));
    estate.record_trace_url(read["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R8, e4; node-config-update.md "Build application": "`ApplyNodeProperties` persists
/// config, tags and `PropertyApplication(build_id, semantic entry operation)` in one durable put";
/// "Resolve the exact durable application before repeating an uncertain Build effect"): Build A applies
/// `mesh1.rpc` / `2`'s entry, its property record holding A's application; the node then updates its
/// own config; it is killed and recovered at the pair. Replaying Build A leaves the node's identity,
/// its later own config and A's tags as they stand. The node records A's repeat as already applied.
/// Must NOT happen: the replay writing A's map over the later update.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_applied_build_update_survives_node_restart() {
    let test = "an_applied_build_update_survives_node_restart";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "applied-build"), "fabric1", "mesh1").await, &[("mesh1", 1, 2)]).await;
    let subject = pair("mesh1.rpc", "2");
    let stream = estate
        .submit_named("Build A applies the node's entry properties", |current, _| {
            current.topology.values().flat_map(|mesh| mesh.values()).cloned().map(|mut spec| {
                if spec.path == subject {
                    spec.config = [("entry".into(), "build-a".into())].into();
                    spec.tags = ["build-a".into()].into();
                }
                spec
            }).collect()
        })
        .await;
    let applied = estate.follow(stream).await;
    let first = estate.node_view(&subject).await;
    assert_eq!((first.config.clone(), first.tags.clone()), ([("entry".into(), "build-a".into())].into(), ["build-a".into()].into()), "Build A applied");
    let application = rdm_core::properties::PropertyApplication::entry(&applied.build_id, &subject);
    wait_for("the property record holds Build A's application", Duration::from_secs(30), || async { estate.node_properties(&first.node_id).filter(|p| p.applied.contains(&application)) }).await;
    own_update(&estate, &first, "listener.data=127.0.0.1:45005");
    let own: BTreeMap<String, String> = [("listener.data".into(), "127.0.0.1:45005".into())].into();
    estate.kill_node(&subject).await;
    let recovered = estate.until_node(&subject, "a recovered node is ready at the pair", crate::node_recovery_due(), |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != first.node_id).cloned()).await;
    assert_eq!(recovered.config, own, "the recovered node carries the later own update");
    let again = estate.carrier().await.reattach(&applied.build_id).await.expect("Build A reattaches");
    estate.follow(again.stream).await;
    let after = estate.node_view(&subject).await;
    assert_eq!((&after.node_id, &after.config, &after.tags), (&recovered.node_id, &own, &first.tags), "replaying the applied Build keeps the node's identity, its later config and A's tags");
    assert_eq!(estate.node_properties(&after.node_id).expect("the node's own record").config, own, "the durable record keeps the later config");
    estate.artifact("applied-build.json", &json!({"build_id": applied.build_id, "first": first, "recovered": recovered, "after": after}));
    estate.stop().await;
    let spans = estate.spans();
    let repeat = named(&spans, "rdm.node.record.update.via-already-applied")
        .into_iter()
        .find(|sp| attr(sp, "build_id") == applied.build_id.as_str())
        .unwrap_or_else(|| panic!("the node records Build {}'s repeat as already applied, naming its exact target and the original operation identity (i150 OTLP table, rdm.node.record.update.via-already-applied)", applied.build_id));
    estate.record_trace_url(repeat["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R9, e4; node-config-update.md "Deferred application and startup": "A stopped process
/// remains alive with its Node RPC endpoint bound and applies an update as a running node does ...
/// Existing durable properties and application recognition survive startup; an older environment must
/// not overwrite them"): `mesh1.rpc` / `2` is stopped and update A is acknowledged; its start is held
/// at its Ready declaration (the faulted admin's cut) and update B is acknowledged meanwhile; the cut
/// is released. The node is ready as the same birth with B, and its property record holds B. Must NOT
/// happen: the start's Ready erasing the newer acknowledged update.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_preserves_a_newer_owed_update() {
    let test = "a_start_preserves_a_newer_owed_update";
    let sha = rdm_test::faults::candidate_sha();
    let estate = Estate::bootstrap_on(owner_of(test, "owed-update"), "fabric1", "mesh1", &rdm_test::faults::binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let mut estate = estate_at(estate, &[("mesh1", 1, 2)]).await;
    let subject = pair("mesh1.rpc", "2");
    let before = estate.node_view(&subject).await;
    Estate::complete(&mut estate.nodes_obj().await.stop(&subject).await.expect("the stop is accepted")).await;
    estate.until_stopped_at_the_fabric_primary(&subject).await;
    let a: BTreeMap<String, String> = [("owed".into(), "a".into())].into();
    let reply = config_update(&estate, &before, a).await;
    assert_eq!(reply.name(), "config-updated", "the stopped process applies update A: {reply:?}");
    let door = rdm_test::faults::Door::at(&estate, &pair("mesh1.admin", "1")).await;
    door.arm("ready-held", json!({"kind": "declaration", "node_id": before.node_id, "state": "Ready"})).await;
    let starting = {
        let nodes = estate.nodes_obj().await;
        let subject = subject.clone();
        tokio::spawn(async move { Estate::complete(&mut nodes.start(&subject).await.expect("the start is accepted")).await })
    };
    door.wait_held("ready-held").await;
    let b: BTreeMap<String, String> = [("owed".into(), "b".into())].into();
    let reply = config_update(&estate, &before, b.clone()).await;
    assert_eq!(reply.name(), "config-updated", "update B is acknowledged during the start: {reply:?}");
    door.release("ready-held").await;
    starting.await.expect("the start completes");
    let after = estate.until_node(&subject, "the node is ready again", Duration::from_secs(60), |n| n.filter(|n| n.status == NodeStatus::Ready).cloned()).await;
    assert_eq!((&after.node_id, &after.incarnation_id), (&before.node_id, &before.incarnation_id), "the same birth starts");
    assert_eq!(after.config, b, "the start's Ready keeps the newer acknowledged update");
    assert_eq!(estate.node_properties(&before.node_id).expect("the node's own record").config, b, "the durable record holds update B");
    estate.artifact("owed-update.json", &json!({"before": before, "after": after}));
    estate.stop().await;
    let spans = estate.spans();
    let applied = named(&spans, "rdm.node_admin.node.update.via-apply-node-config").into_iter().filter(|sp| attr(sp, "node") == before.node_id.as_str()).max_by_key(|sp| start_ns(sp)).expect("the node applied B");
    estate.record_trace_url(applied["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R11, e5; query.md §1; fabric-recovery.md "Nothing alive"): mesh2's `mesh2.rpc` /
/// `2` is replaced and its predecessor deleted. Every node-admin is then lost and one is started on
/// the fabric-primary's retained data dir and store. Its first node.get answer by the pair names the
/// successor, read from the retained records; the current Build and its attempt are unchanged (no
/// completed work runs again); the shape settles. The resolver records the read of the successor.
/// Must NOT happen: the fresh fabric-primary naming the deleted birth, or nothing, at the pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_node_reads_survive_fabric_primary_restart() {
    let test = "current_node_reads_survive_fabric_primary_restart";
    let shape = [("mesh1", 1, 0), ("mesh2", 1, 2)];
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "fabric-primary-restart"), "fabric1", "mesh1").await, &shape).await;
    let subject = pair("mesh2.rpc", "2");
    let before = estate.node_view(&subject).await;
    steps_to_complete(&estate, rdm_test::steps::replace(&subject, &before.mesh, before.kind).unwrap()).await;
    let successor = estate.until_node(&subject, "the successor is ready at the pair", Duration::from_secs(120), |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != before.node_id).cloned()).await;
    let build = estate.accepted_build_id().await;
    let attempt = estate.attempt_of_build(&build).await;
    let admins: Vec<String> = estate.node_views().await.into_iter().filter(|n| n.kind == NodeKind::NodeAdmin).map(|n| n.node_id.to_string()).collect();
    for (dir, pid) in estate.live_runtimes() {
        if admins.contains(&dir.file_name().unwrap().to_string_lossy().to_string()) {
            estate.kill_pid(pid);
        }
    }
    estate.kill_bootstrap();
    let fp_dir = estate.bootstrap_data_dir("mesh1");
    let restarted = estate.restart_admin_with(&fp_dir, &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    estate.enter(&restarted).await;
    let first = wait_for("the restarted fabric-primary answers node.get by the pair", Duration::from_secs(60), || async {
        estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("path", [subject.path.clone()]).eq("name", [subject.name.clone()])).await.ok()
    })
    .await;
    assert_eq!(first.iter().map(|n| n.node_id.clone()).collect::<Vec<_>>(), vec![successor.node_id.clone()], "the fresh fabric-primary's first answer names the successor at the pair: {first:#?}");
    assert_eq!(estate.accepted_build_id().await, build, "no new Build");
    assert_eq!(estate.attempt_of_build(&build).await, attempt, "no completed attempt runs again");
    estate.settles_to(&shape, Duration::from_secs(240)).await;
    let fp = fabric_primary(&estate).await;
    estate.artifact("fabric-primary-restart.json", &json!({"before": before, "successor": successor, "first": first, "build": build, "attempt": attempt}));
    estate.stop().await;
    let spans = estate.spans();
    let read = named(&spans, "rdm.node_admin.node.resolve.via-lifecycle-record")
        .into_iter()
        .find(|sp| attr(sp, "subject_id") == successor.node_id.as_str() && attr(sp, "node_id") == fp.as_str())
        .unwrap_or_else(|| panic!("the restarted fabric-primary {fp}'s resolver records its read of the successor {} (i150 OTLP table, rdm.node_admin.node.resolve.via-lifecycle-record)", successor.node_id));
    estate.record_trace_url(read["trace_id"].as_str().unwrap());
}
