//! product=mesh, feature=node-records, subfeature=own-properties, rung=MN, provider=process.

use rdm_client::{NodeStatus, NodeView};
use rdm_core::{NodeId, PathName};
use rdm_node::nodes_storage::{NodeRow, NodeRowsStorage as _};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

// @feature: node-records
/// CONTRACT (i150 R3/R7; build.md §1, Q342: a Build states only the nodes it adds or removes): a node's
/// own node.config.update stands through a later Build, which never states a node it does not touch.
/// Node A updates its own config; a Create of node B follows; A's config and tags are what A wrote,
/// and its node.id is unchanged. Publishing the own map is one node-owned durable write, with no upward
/// config delivery or copied atlas. Must NOT happen: the Build putting back A's earlier values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nodes_config_update_stands_through_a_later_build() {
    let test = "a_nodes_config_update_stands_through_a_later_build";
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
    estate.resize_shape("fabric1", &shape).await;
    estate.settled_views(&shape).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let subject: rdm_core::PathName = rdm_core::PathName::new("mesh1.node", "2").unwrap();
    let before = estate.node_view(&subject).await;
    let answer = estate.probe(&[
        "own-config",
        "--target",
        &format!("exact:{}", before.node_id),
        "--value",
        "listener.data=127.0.0.1:45005",
    ]);
    assert_eq!(answer["own_config"], "config-updated", "{answer}");
    let own: BTreeMap<String, String> = [("listener.data".into(), "127.0.0.1:45005".into())].into();
    // Node B: a Create of one more node, which states node A nowhere.
    let mut created = estate.create_node("mesh1", "node").await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of node B: {e}"));
    let applied = Estate::complete(&mut created).await.expect("the call opened a Build");
    estate.settled_views(&[("mesh1", 1, 3)]).await;
    let after = estate.node_view(&subject).await;
    assert_eq!(after.node_id, before.node_id, "a Build that does not touch node A keeps its identity");
    assert_eq!(after.config, own, "node A's own config stands through the Build of node B");
    assert_eq!(after.tags, before.tags, "node A's tags stand through the Build of node B");
    estate.artifact(
        "build-property-readbacks",
        &json!({"build_id": applied.build_id, "before": before, "own": after}),
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
            .all(|s| s["attributes"]["subject"] != "mesh1.node.2"
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
    estate.resize_shape("fabric1", &shape).await;
    estate.settled_views(&shape).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let before = estate
        .node_views()
        .await
        .into_iter()
        .find(|n| n.location() == rdm_core::PathName::new("mesh1.node", "2").unwrap())
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
    estate.kill_node(&rdm_core::PathName::new("mesh1.node", "2").unwrap()).await;
    let recovered = wait_for(
        "a recovered node exposes the acknowledged config",
        crate::node_recovery_due(),
        || async {
            estate.node_views().await.into_iter().find(|n| {
                n.location() == rdm_core::PathName::new("mesh1.node", "2").unwrap()
                    && n.node_id != before.node_id
                    && n.status == rdm_client::NodeStatus::Ready
            })
        },
    )
    .await;
    let records = estate
        .fabric_store_root()
        .join(rdm_admin::store_root::NODES_FILE);
    estate.stop().await;
    assert_eq!(
        recovered.config, expected,
        "the complete acknowledged map survives forced process loss"
    );
    let log = rdm_node::store_log::read_file(&records).unwrap_or_else(|e| panic!("the node must persist its own properties before acknowledging them; retained record log: {e}"));
    let own = log
        .iter()
        .rev()
        .find(|line| line.kind() == "node-properties" && line.subject() == before.node_id.as_str())
        .expect("the original node's acknowledged record survives its process exit");
    let record: rdm_node::node_records::NodeProperties = own.decode(&records).unwrap();
    assert_eq!(
        record.config, expected,
        "the log contains the entire acknowledged map"
    );
    assert_eq!(
        own.attributes.source_node_id(),
        Some(before.node_id.as_str()),
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
    estate.resize_shape("fabric1", shape).await;
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

/// The own update of `v`'s config through the testkit probe: the node's own local durable write.
fn own_update(estate: &Estate, v: &NodeView, value: &str) {
    let answer = estate.probe(&["own-config", "--target", &format!("exact:{}", v.node_id), "--value", value]);
    assert_eq!(answer["own_config"], "config-updated", "the own update was acknowledged: {answer}");
}

/// `node`'s record: its latest line at its node id in the nodes store (query.md §1, Q323), read as the
/// store answers it.
async fn latest(estate: &Estate, node: &NodeId) -> Option<NodeRow> {
    let rows = rdm_node::nodes_storage::FileNodeRows::open(&rdm_node::store_root::StoreRoot::at(&estate.fabric_store_root())).await.expect("the fabric's nodes store opens");
    rows.node(node).await.unwrap_or_else(|e| panic!("the record of {node} is read: {e}"))
}

fn status_of(r: &NodeRow) -> String {
    serde_json::to_value(r.status).unwrap().as_str().unwrap_or_default().to_string()
}

/// `r`'s fields as these cells compare them: the record's own fields, and its newest birth's
/// incarnation and lifecycle facts its mesh-primary writes (never the birth's addresses).
fn fields(r: &NodeRow) -> Value {
    let b = r.newest_birth();
    json!({
        "node_id": r.node_id, "name": r.name, "type": r.r#type, "mesh": r.mesh, "mesh_id": r.mesh_id, "status": r.status,
        "incarnation_id": b.map(|b| &b.incarnation_id), "declared": b.map(|b| &b.declared), "operations": b.map(|b| &b.operations),
        "silence": b.and_then(|b| b.silence),
        "cert": b.and_then(|b| b.cert.as_ref()), "signing_cert": b.and_then(|b| b.signing_cert.as_ref()),
        "executable": r.executable, "storage_persistence": r.storage_persistence, "topology_storage_provider": r.topology_storage_provider,
        "allocated": r.allocated, "initial_properties": r.initial_properties, "predecessor": r.predecessor, "renamed_from": r.renamed_from,
    })
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

/// The shared resolver's reads of `subject` that read its record's latest line (i150 § OTLP
/// span-assertion bar, `rdm.node_admin.node.resolve.via-lifecycle-record`: the reading node, the
/// subject, its mesh and the outcome; Q323: no writer is selected), begun at or after `since_ns`.
fn reads_of<'a>(spans: &'a [Value], subject: &NodeId, since_ns: u64) -> Vec<&'a Value> {
    named(spans, "rdm.node_admin.node.resolve.via-lifecycle-record").into_iter().filter(|sp| attr(sp, "subject_id") == subject.as_str() && attr(sp, "outcome") == "read" && start_ns(sp) >= since_ns).collect()
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// The fields a node's record keeps across a change of its mesh-primary (i150 R12; Q323: the successor
/// writes the same record at the node id): identity, executable and storage ([`fields`]).
const KEPT: [&str; 11] =["node_id", "name", "type", "mesh", "mesh_id", "incarnation_id", "executable", "storage_persistence", "topology_storage_provider", "initial_properties", "allocated"];

// @feature: node-records
/// CONTRACT (i150 R1, e1; Q347: "node.name.update needs no connections, and the node rejoins its
/// mesh under the new name. The command completes once the node is back on its mesh, publishing the
/// new name"; node-name-update.md steps 1-6): `mesh1.node` / `2`, ready on its mesh, has its
/// connections deleted (`node.connection.delete { All }`, a node command) and is renamed `moved` by
/// the caller's `node.name.update`, answered `NameUpdated`. Once answered, with no wait, node.get by the
/// moved pair names one node under the same node id, incarnation and process, not stopped, and none at
/// the old pair; node.get by its id names it at the moved pair; its lifecycle record names the moved
/// pair. The node is back on its mesh: it is listed ready again at the moved pair. The mesh-primary's
/// rename span says `connections=deleted`, `rejoin=mesh-joined`; its `node.mesh.join` command span
/// names the moved pair and answers `mesh-joined`; the mesh-primary admits the node's join report under
/// the moved pair. Must NOT happen: the rename refused because the node is on its mesh, an identity
/// minted, the answer before the node is back on its mesh, or a join refused `JoinMismatch("name")`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_with_no_connection_renamed_rejoins_its_mesh_under_the_new_name_keeping_its_identity() {
    let test = "a_node_with_no_connection_renamed_rejoins_its_mesh_under_the_new_name_keeping_its_identity";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "name-update"), "fabric1", "mesh1").await, &[("mesh1", 1, 2)]).await;
    let subject = pair("mesh1.node", "2");
    let moved = pair("mesh1.node", "moved");
    let before = estate.node_view(&subject).await;
    let pid = estate.pid_of(&subject).await;
    let nodes = estate.nodes_obj().await;
    Estate::workflow(&format!("node.connection.delete {{ All }} of {subject:?}"), nodes.delete_connections(&subject, &rdm_core::status::ConnectionScope::All).await).await;
    let mesh_primary = estate.node_views().await.into_iter().find(|n| n.mesh == before.mesh && n.is_primary && rdm_core::is_node_admin(&n.r#type)).expect("the view names mesh1's primary").node_id;
    let renamed_at = now_ns();
    let reply = nodes.name_update(&birth_of(&before), &mesh_primary, &rdm_transport::RouteChoice::Direct, &moved).await.unwrap_or_else(|e| panic!("node.name.update of {subject:?}: {e:?}"));
    assert!(matches!(reply, rdm_core::status::StatusReply::NameUpdated { .. }), "a node with no connection is renamed on its mesh: {reply:?}");
    // No wait: the reads the moment the caller is answered.
    let at_moved = at_pair(&estate, &moved).await;
    assert_eq!(at_moved.len(), 1, "node.get [path, name] names one node at the moved pair: {at_moved:#?}");
    let renamed = at_moved[0].clone();
    assert_eq!((&renamed.node_id, &renamed.incarnation_id, renamed.r#type.clone(), &renamed.mesh), (&before.node_id, &before.incarnation_id, before.r#type.clone(), &before.mesh), "the rename keeps the node's identity");
    assert!(!renamed.stopped, "the renamed node is live: {renamed:?}");
    assert!(at_pair(&estate, &subject).await.is_empty(), "nothing is listed at the old pair");
    let named_by_id: Vec<PathName> = by_id(&estate, &before.node_id).await.into_iter().map(|n| n.location()).collect();
    assert_eq!(named_by_id, vec![moved.clone()], "node.get by the node's id names it at the moved pair");
    let record = latest(&estate, &before.node_id).await.expect("the renamed node's record");
    assert_eq!(
        (&record.node_id, record.newest_birth().map(|b| &b.incarnation_id), &record.location()),
        (&before.node_id, before.incarnation_id.as_ref(), &moved),
        "the renamed node's record keeps its identity and names the moved pair: {record:?}"
    );
    // Back on its mesh: ready again at the moved pair, the same process.
    let back = estate.until_node(&moved, "the renamed node is ready on its mesh at the moved pair", Duration::from_secs(60), |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id == before.node_id).cloned()).await;
    assert_eq!(estate.pid_of(&moved).await, pid, "the renamed node is the same process");
    estate.artifact("name-update-rejoin.json", &json!({"before": before, "renamed": renamed, "back": back, "record": record}));
    estate.stop().await;
    let spans = estate.spans();
    let rename = named(&spans, "rdm.node_admin.name.update.via-writer")
        .into_iter()
        .find(|sp| attr(sp, "node_id") == before.node_id.as_str() && attr(sp, "to_name") == moved.name && attr(sp, "outcome") == "name-updated")
        .unwrap_or_else(|| panic!("the mesh-primary's rename of {} to {moved:?} is recorded", before.node_id));
    assert_eq!((attr(rename, "connections"), attr(rename, "apply"), attr(rename, "answered"), attr(rename, "rejoin")), ("deleted".to_string(), "direct".to_string(), "applied".to_string(), "mesh-joined".to_string()), "{rename:#}");
    let rejoin = named(&spans, "rdm.node_admin.node.update.via-mesh-join-commanded")
        .into_iter()
        .find(|sp| attr(sp, "node_id") == before.node_id.as_str() && attr(sp, "cause") == "node.name.update" && start_ns(sp) >= renamed_at)
        .unwrap_or_else(|| panic!("the mesh-primary commanded {}'s node.mesh.join after the rename", before.node_id));
    assert_eq!((attr(rejoin, "node_path"), attr(rejoin, "node_name"), attr(rejoin, "outcome")), (moved.path.clone(), moved.name.clone(), "mesh-joined".to_string()), "{rejoin:#}");
    assert_eq!(rejoin["trace_id"], rename["trace_id"], "the rejoin is the rename's own step, in its trace");
    let admitted = named(&spans, "rdm.node_admin.node.update.via-join").into_iter().find(|sp| attr(sp, "node_id") == before.node_id.as_str() && start_ns(sp) >= renamed_at && attr(sp, "outcome") == "installed");
    assert!(admitted.is_some_and(|sp| rdm_test::estate::names_location(sp, "admitted", &moved)), "the mesh-primary admits the node's join report under the moved pair");
    assert!(named(&spans, "rdm.node_admin.node.reject.via-join-mismatch").into_iter().all(|sp| attr(sp, "node_id") != before.node_id.as_str()), "no join of the renamed node is refused");
    estate.record_trace_url(rename["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R11, e5.s2; query.md §1: "Historical records, including Deleted nodes, remain
/// queryable by their own IDs and matching properties; a history query does not redirect to the current
/// pair's claimant"; Q323: the record's latest line is current): `mesh1.node` / `2` is deleted and a new
/// node created at the pair (Q345: a new node is a Build that adds it). node.get by the deleted node's
/// id answers that node's latest line, Deleted, under the same id and incarnation, and the new node is
/// not in that answer; the shared resolver records the
/// read of the deleted node's record. Must NOT happen: an empty answer for the deleted birth, or the read
/// redirected to the pair's claimant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_birth_is_read_by_its_id() {
    let test = "a_deleted_birth_is_read_by_its_id";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "deleted-history"), "fabric1", "mesh1").await, &[("mesh1", 1, 2)]).await;
    let subject = pair("mesh1.node", "2");
    let before = estate.node_view(&subject).await;
    Estate::workflow(&format!("the Delete of {subject:?}"), estate.delete_node(&subject).await).await;
    Estate::workflow(&format!("the Create of a node at {subject:?}"), estate.create_node(&before.mesh, &before.r#type).await).await;
    let successor = estate.until_node(&subject, "the new node is ready at the pair", Duration::from_secs(120), |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != before.node_id).cloned()).await;
    let record = latest(&estate, &before.node_id).await.expect("the deleted node's record");
    assert_eq!(status_of(&record), "deleted", "the deleted node's latest line is Deleted: {record:?}");
    let read_at = now_ns();
    let listed = by_id(&estate, &before.node_id).await;
    assert_eq!(
        listed.iter().map(|n| (n.node_id.clone(), n.incarnation_id.clone(), n.status)).collect::<Vec<_>>(),
        vec![(before.node_id.clone(), before.incarnation_id.clone(), NodeStatus::Deleted)],
        "node.get by the deleted node's id answers its latest line, Deleted: {listed:#?}"
    );
    assert!(listed.iter().all(|n| n.node_id != successor.node_id), "the read by the deleted node's id is not redirected to the pair's claimant");
    estate.artifact("deleted-history.json", &json!({"before": before, "successor": successor, "record": record, "listed": listed}));
    estate.stop().await;
    let spans = estate.spans();
    let read = *reads_of(&spans, &before.node_id, read_at).first().expect("the shared resolver records the read of the deleted node's record");
    estate.record_trace_url(read["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R12, R13; e2; node-config-update.md "Deferred application and startup": "Existing
/// durable properties and application recognition survive startup"): every field of a node survives
/// its restart, compared whole, not sampled. A node of mesh1 holding no role updates its own
/// config and is tagged.
/// 1. Stopped then started (the same birth): node.get lists every field as before but the measured
///    load and gossip counts and the member cert its start's join issues afresh (fabric-certs.md
///    "Every JoinNode issues a fresh member cert"), its property record is the same record, and its lifecycle record differs
///    only by the stop and start it recorded and that fresh cert. 2. Killed: the node recovered at the pair (a new node id
///    and endpoint, Q358, on a new data dir, its storage ephemeral, Q359) lists the same pair, type, mesh,
///    config, tags, executable and storage; its own property record holds the config and tags; its
///    lifecycle record carries the same identity fields and names its predecessor. Must NOT happen: a
///    field reset by a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_node_fields_survive_node_restart() {
    let test = "all_node_fields_survive_node_restart";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "all-fields"), "fabric1", "mesh1").await, &[("mesh1", 1, 2)]).await;
    // The subject holds no role: a primary's stop hands its role over first (node-stop.md "Primary"),
    // so its `is_primary` is not a field a stop and start keeps.
    let first = estate.node_views().await.into_iter().find(|n| n.mesh == "mesh1" && n.r#type == "node" && !n.is_primary).expect("a node of mesh1 that holds no role");
    let subject = first.location();
    own_update(&estate, &first, "listener.data=127.0.0.1:45005");
    let labels: BTreeSet<String> = ["rabbit".to_string()].into();
    let tagged = tags_create(&estate, &first, &labels).await;
    assert_eq!(tagged.name(), "tags-created", "{tagged:?}");
    let own: BTreeMap<String, String> = [("listener.data".into(), "127.0.0.1:45005".into())].into();
    let before = estate.until_node(&subject, "the node lists its own config and its tag", Duration::from_secs(30), |n| n.filter(|n| n.config == own && n.tags == labels).cloned()).await;
    let properties = estate.node_properties(&before.node_id).expect("the node's own property record");
    let lifecycle = fields(&latest(&estate, &before.node_id).await.expect("the node's record"));

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
    let restarted = fields(&latest(&estate, &before.node_id).await.expect("the record after the start"));
    let changed = differing_except(&lifecycle, &restarted, &["operations", "declared", "cert"]);
    assert!(changed.is_empty(), "the record differs only by the stop and start it records and the fresh member cert: {changed:#?}");
    let (was, now) = (&lifecycle["cert"], &restarted["cert"]);
    assert!(!was.is_null() && !now.is_null(), "the record holds the member cert dates before and after the start: {was:?} -> {now:?}");
    assert!(now["issued_at"].as_u64() >= was["issued_at"].as_u64() && now["issuer_expires_at"] == was["issuer_expires_at"], "the record holds the fresh member cert of the start's join, under the same issuer: {was:?} -> {now:?}");

    // Evidence export is batched and a SIGKILL flushes nothing: the own update's write span (the OTLP
    // table's control) is observed before the kill, as own_properties_survive_node_restart does.
    let write_trace = wait_for("the own update's durable write span is exported", Duration::from_secs(10), || async {
        estate
            .spans_named(&["rdm.node.record.update.via-properties"])
            .into_iter()
            .find(|sp| attr(sp, "node_id") == before.node_id.as_str() && attr(sp, "outcome") == "durable")
            .and_then(|sp| sp["trace_id"].as_str().map(str::to_string))
    })
    .await;

    // 2. Forced exit: recovered at the pair as a new node, with a new endpoint (Q358), on a new data dir
    //    (a `node`'s storage is ephemeral, Q359).
    estate.kill_node(&subject).await;
    let recovered = estate.until_node(&subject, "a recovered node is ready at the pair", crate::node_recovery_due(), |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != before.node_id).cloned()).await;
    let changed = differing(&before, &recovered, &["name", "type", "mesh", "config", "tags", "executable", "storage_persistence", "topology_storage_provider", "properties"]);
    assert!(changed.is_empty(), "the recovered node lists every retained field: {changed:#?}");
    assert!(recovered.endpoint_id.is_some() && recovered.endpoint_id != before.endpoint_id, "a new endpoint_id (Q358): {recovered:?}");
    assert!(recovered.data_dir.is_some() && recovered.data_dir != before.data_dir, "a new data dir: the node's storage is ephemeral (Q359): {recovered:?}");
    assert!(recovered.transport_addr.is_some(), "the recovered node lists the address it bound: {recovered:?}");
    let recovered_properties = wait_for("the recovered node's own property record", Duration::from_secs(30), || async { estate.node_properties(&recovered.node_id) }).await;
    assert_eq!((&recovered_properties.config, &recovered_properties.tags), (&properties.config, &properties.tags), "the recovered node's own record holds the config and tags");
    let recovered_record = latest(&estate, &recovered.node_id).await.expect("the recovered node's record");
    let recovered_lifecycle = fields(&recovered_record);
    let changed = differing(&lifecycle, &recovered_lifecycle, &["name", "type", "mesh", "mesh_id", "executable", "storage_persistence", "topology_storage_provider"]);
    assert!(changed.is_empty(), "the recovered node's record keeps every retained field: {changed:#?}");
    assert_eq!(recovered_record.predecessor.as_ref().map(|p| &p.node_id), Some(&before.node_id), "the recovered record names its predecessor: {recovered_record:?}");
    estate.artifact("all-fields.json", &json!({"before": before, "started": started, "recovered": recovered, "lifecycle": lifecycle, "recovered_lifecycle": recovered_lifecycle}));
    estate.stop().await;
    estate.record_trace_url(&write_trace);
}

// @feature: node-records
/// CONTRACT (i150 R4, R5, R12, e3; proposal node-record.md R-NR2 and R-NR8 as Q323 rewrote them: "There
/// is no takeover preparation. The successor installs the election's result the call carries and is
/// primary from that moment ... as mesh-primary it writes status and role flags into its nodes' records
/// from then on"): mesh2's primary A is killed; B takes the role and is killed in its tenure; a third
/// admin C takes it. Every surviving mesh2 node's record, its latest line at its node id, keeps each
/// identity, executable and storage field it held under A; the member's latest line is
/// Ready, and node.get lists the member ready as the same birth. The resolver records a read of the
/// member's record after C took the role. A superseded primary's late write is not put: Q323 builds
/// nothing against it ("it gets its own ruling the day it shows"). Must NOT happen: a field of a
/// surviving node's record lost or invented across the change of its mesh-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takeover_survives_mesh_primary_restart() {
    let test = "takeover_survives_mesh_primary_restart";
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "takeover"), "fabric1", "mesh1").await, &[("mesh1", 1, 0), ("mesh2", 3, 1)]).await;
    let views = estate.node_views().await;
    let a = views.iter().find(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.is_primary).cloned().expect("mesh2's primary");
    let member = views.iter().find(|n| n.mesh == "mesh2" && n.r#type == "node").cloned().expect("mesh2's node");
    let mut recorded: BTreeMap<NodeId, Value> = BTreeMap::new();
    for n in views.iter().filter(|n| n.mesh == "mesh2") {
        recorded.insert(n.node_id.clone(), fields(&latest(&estate, &n.node_id).await.unwrap_or_else(|| panic!("{:?}'s record under A", n.name))));
    }
    estate.kill_node(&a.location()).await;
    let other_primary = |not: Vec<NodeId>| move |views: &[NodeView]| views.iter().find(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.is_primary && n.status == NodeStatus::Ready && !not.contains(&n.node_id)).cloned();
    let b = estate.until_view("another admin of mesh2 takes its primary role", Duration::from_secs(120), other_primary(vec![a.node_id.clone()])).await;
    estate.kill_node(&b.location()).await;
    let c = estate.until_view("a third admin takes mesh2's primary role", Duration::from_secs(180), other_primary(vec![a.node_id.clone(), b.node_id.clone()])).await;
    let c_holds_at = now_ns();
    let member_now = fields(&wait_for("the member's latest line is Ready", Duration::from_secs(60), || async { latest(&estate, &member.node_id).await.filter(|r| status_of(r) == "ready") }).await);
    let changed = differing(&recorded[&member.node_id], &member_now, &KEPT);
    assert!(changed.is_empty(), "the member's record keeps every field it held under A: {changed:#?}");
    for (id, earlier) in &recorded {
        if [&a.node_id, &b.node_id].contains(&id) {
            continue;
        }
        let current = fields(&latest(&estate, id).await.unwrap_or_else(|| panic!("{id}'s record under C")));
        let changed = differing(earlier, &current, &KEPT);
        assert!(changed.is_empty(), "{id}'s record keeps every field it held under A: {changed:#?}");
    }
    let listed = estate.node_view(&member.location()).await;
    assert_eq!((&listed.node_id, &listed.incarnation_id, listed.status), (&member.node_id, &member.incarnation_id, NodeStatus::Ready), "node.get lists the member ready as the same birth");
    estate.artifact("takeover.json", &json!({"a": a, "b": b, "c": c, "recorded": recorded, "member_now": member_now}));
    estate.stop().await;
    let spans = estate.spans();
    let read = *reads_of(&spans, &member.node_id, c_holds_at).first().unwrap_or_else(|| panic!("the shared resolver records a read of the member {}'s record after C {} took mesh2's role (i150 OTLP table, rdm.node_admin.node.resolve.via-lifecycle-record)", member.node_id, c.node_id));
    estate.record_trace_url(read["trace_id"].as_str().unwrap());
}

// @feature: node-records
/// CONTRACT (i150 R9, e4; node-config-update.md "Deferred application and startup": "A stopped process
/// remains alive with its Node RPC endpoint bound and applies an update as a running node does ...
/// Existing durable properties and application recognition survive startup; an older environment must
/// not overwrite them"): `mesh1.node` / `2` is stopped and update A is acknowledged; its start is held
/// at its Ready declaration (the faulted admin's cut) and update B is acknowledged meanwhile; the cut
/// is released. The node is ready as the same birth with B, and its property record holds B. Must NOT
/// happen: the start's Ready erasing the newer acknowledged update.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_preserves_a_newer_owed_update() {
    let test = "a_start_preserves_a_newer_owed_update";
    let sha = rdm_test::faults::candidate_sha();
    let estate = Estate::bootstrap_on(owner_of(test, "owed-update"), "fabric1", "mesh1", &rdm_test::faults::binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    let mut estate = estate_at(estate, &[("mesh1", 1, 2)]).await;
    let subject = pair("mesh1.node", "2");
    let before = estate.node_view(&subject).await;
    Estate::complete(&mut estate.nodes_obj().await.stop(&subject).await.expect("the stop is accepted")).await;
    estate.until_stopped_at_the_fabric_primary(&subject).await;
    let a: BTreeMap<String, String> = [("owed".into(), "a".into())].into();
    let reply = config_update(&estate, &before, a).await;
    assert_eq!(reply.name(), "config-updated", "the stopped process applies update A: {reply:?}");
    let faults = rdm_test::faults::FaultClient::at(&estate, &pair("mesh1.node-admin", "1")).await;
    faults.arm("ready-held", json!({"kind": "declaration", "node_id": before.node_id, "state": "Ready"})).await;
    let starting = {
        let nodes = estate.nodes_obj().await;
        let subject = subject.clone();
        tokio::spawn(async move { Estate::complete(&mut nodes.start(&subject).await.expect("the start is accepted")).await })
    };
    faults.wait_held("ready-held").await;
    let b: BTreeMap<String, String> = [("owed".into(), "b".into())].into();
    let reply = config_update(&estate, &before, b.clone()).await;
    assert_eq!(reply.name(), "config-updated", "update B is acknowledged during the start: {reply:?}");
    faults.release("ready-held").await;
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
/// CONTRACT (i150 R11, e5; query.md §1; fabric-recovery.md "Nothing alive": "Every node the store names
/// is re-created through node recovery, never waited for"; Q335 item 4): mesh2's `mesh2.node` / `2` is
/// deleted and a new node created at the pair (Q345). Every node-admin is then lost and one is started on the
/// fabric-primary's retained data dir and store. Its first node.get answer by the pair names the
/// successor, read from the retained records; the current Build is unchanged and not started again (a
/// lost node-admin's node recovery is never a Build, Q342); the shape settles. The resolver records the
/// read of the successor. Must NOT happen: the fresh fabric-primary naming the deleted birth, or nothing,
/// at the pair; the Build started again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_node_reads_survive_fabric_primary_restart() {
    let test = "current_node_reads_survive_fabric_primary_restart";
    let shape = [("mesh1", 1, 0), ("mesh2", 1, 2)];
    let mut estate = estate_at(Estate::bootstrap(owner_of(test, "fabric-primary-restart"), "fabric1", "mesh1").await, &shape).await;
    let subject = pair("mesh2.node", "2");
    let before = estate.node_view(&subject).await;
    Estate::workflow(&format!("the Delete of {subject:?}"), estate.delete_node(&subject).await).await;
    Estate::workflow(&format!("the Create of a node at {subject:?}"), estate.create_node(&before.mesh, &before.r#type).await).await;
    let successor = estate.until_node(&subject, "the new node is ready at the pair", Duration::from_secs(120), |n| n.filter(|n| n.status == NodeStatus::Ready && n.node_id != before.node_id).cloned()).await;
    let build = estate.accepted_build_id().await;
    let starts = estate.starts_of_build(&build).await;
    let admins: Vec<String> = estate.node_views().await.into_iter().filter(|n| n.r#type == "node-admin").map(|n| n.node_id.to_string()).collect();
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
    let starts_at_read = estate.starts_of_build(&build).await;
    estate.settles_to(&shape, Duration::from_secs(240)).await;
    let fp = fabric_primary(&estate).await;
    estate.artifact("fabric-primary-restart.json", &json!({"before": before, "successor": successor, "first": first, "build": build, "starts": starts}));
    estate.stop().await;
    let spans = estate.spans();
    let read = named(&spans, "rdm.node_admin.node.resolve.via-lifecycle-record")
        .into_iter()
        .find(|sp| attr(sp, "subject_id") == successor.node_id.as_str() && attr(sp, "node_id") == fp.as_str() && attr(sp, "outcome") == "read")
        .unwrap_or_else(|| panic!("the restarted fabric-primary {fp}'s resolver records its read of the successor {}'s latest line (i150 OTLP table, rdm.node_admin.node.resolve.via-lifecycle-record, outcome read)", successor.node_id));
    // The Build is not started again after the start: a lost node-admin's node recovery is the
    // primary's own workflow, never a Build (Q342).
    assert_eq!(starts_at_read, starts, "the current Build is not started again after the start");
    estate.record_trace_url(read["trace_id"].as_str().unwrap());
}
