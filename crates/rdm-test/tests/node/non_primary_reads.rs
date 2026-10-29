//! Where a read sent to a node that is not a primary is answered (query.md §1; Q224, Q243): only a
//! primary keeps an atlas, so a node that is not a primary, a node-admin or an rpc node, forwards a get
//! to its own mesh-primary and nowhere else; the mesh-primary answers it from its atlas or carries it up
//! to the fabric-primary.

use rdm_client::{ExactBirth, Filter, NodeRpc, NodeView, Nodes};
use rdm_core::status::StatusReply;
use rdm_core::NodeKind;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "non-primary-reads".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(v) => v.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn names(nodes: &[NodeView]) -> Vec<String> {
    nodes.iter().map(|n| n.name.to_string()).collect()
}

fn labels(l: &[&str]) -> BTreeSet<String> {
    l.iter().map(|s| s.to_string()).collect()
}

/// The exact birth of `n`, the node a tags or config command names.
fn birth_of(n: &NodeView) -> ExactBirth {
    ExactBirth { target: rdm_transport::NodeTarget::ExactNode(n.node_id.clone()), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap_or_else(|| panic!("{} names its incarnation", n.name)) }
}

/// The two meshes the cells read: mesh1 holds the fabric-primary alone; mesh2 holds its mesh-primary, a
/// node-admin that is not a primary and one rpc node. The fabric-primary carries the tag `red`; the rpc
/// node the tag `blue` and the config `zone=eu-1`, each written on its row by its mesh-primary and never
/// gossiped (Q203). Returns the estate, entered at the fabric-primary, with the fabric-primary,
/// mesh2's mesh-primary, mesh2's other node-admin and mesh2's rpc node, once the fabric-primary answers
/// every row.
async fn two_meshes_tagged(test: &str) -> (Estate, NodeView, NodeView, NodeView, NodeView, BTreeMap<String, String>) {
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 2, 1)]).await;
    estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 2, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter_fabric_primary().await;
    let views = estate.node_views().await;
    let fp = views.iter().find(|n| n.is_fabric_primary).cloned().unwrap_or_else(|| panic!("a fabric-primary: {views:#?}"));
    let mp = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().unwrap_or_else(|| panic!("mesh2's mesh-primary: {views:#?}"));
    let other = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && !n.is_primary).cloned().unwrap_or_else(|| panic!("mesh2's node-admin that is not a primary: {views:#?}"));
    let rpc = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::RpcNode).cloned().unwrap_or_else(|| panic!("mesh2's rpc node: {views:#?}"));
    assert_eq!(fp.mesh, "mesh1", "the fabric-primary is mesh1's: {fp:?}");

    // Commands go down: each tag and the config is sent to the fabric-primary, which sends it to the
    // node's mesh-primary.
    let nodes = estate.nodes_obj().await;
    for (n, tag) in [(&fp, "red"), (&rpc, "blue")] {
        let reply = nodes.tags_create(&birth_of(n), &fp.node_id, &rdm_transport::RouteChoice::Direct, &labels(&[tag])).await.unwrap_or_else(|e| panic!("node.tag.create of {} at the fabric-primary: {e:?}", n.name));
        assert_eq!(reply.name(), "tags-created", "{}: {reply:?}", n.name);
    }
    let config: BTreeMap<String, String> = [("zone".to_string(), "eu-1".to_string())].into();
    let carrier = estate.carrier().await;
    let reply = NodeRpc::new(carrier.client()).config_update(&fp.node_id, &rdm_transport::RouteChoice::Direct, &birth_of(&rpc), config.clone(), &rdm_transport::CallOptions::default()).await.unwrap_or_else(|e| panic!("node.config.update of {} at the fabric-primary: {e:?}", rpc.name));
    assert!(matches!(reply, StatusReply::ConfigUpdated { .. }), "node.config.updated of {}: {reply:?}", rpc.name);
    wait_for("the fabric-primary answers both tags and the config", Duration::from_secs(30), || async {
        let red = nodes.get(&Filter::new().eq("tags", ["red"])).await.ok()?;
        let blue = nodes.get(&Filter::new().eq("tags", ["blue"])).await.ok()?;
        (names(&red) == [fp.name.to_string()] && names(&blue) == [rpc.name.to_string()] && blue[0].config == config).then_some(())
    })
    .await;
    (estate, fp, mp, other, rpc, config)
}

// @feature: node-lifecycle
/// CONTRACT (query.md §1: "a node that is not a primary forwards a get to its own mesh-primary and
/// nowhere else"; Q243): mesh2's node-admin that is not a primary keeps no atlas. `node.get [mesh2,
/// blue]` sent to it answers the rpc node's record with its tag and its config, row-only properties no
/// gossip view holds: forwarded to mesh2's mesh-primary (`rdm.node_admin.node.serve.via-forward` at the
/// node-admin, naming mesh2's mesh-primary, the fabric-primary not named), which answers from its atlas.
/// `node.get [red]`, naming no mesh, answers the fabric-primary by its tag: forwarded to mesh2's
/// mesh-primary, which carries it up to the fabric-primary. Must NOT happen: the node-admin answering
/// from its own view (rows without their tags, `[]`) or a not-ready refusal of an atlas it lacks; the
/// node-admin sending a read to the fabric-primary itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_admin_that_is_not_a_primary_forwards_nodes_get_to_its_mesh_primary() {
    let test = "a_node_admin_that_is_not_a_primary_forwards_nodes_get_to_its_mesh_primary";
    let (mut estate, fp, mp, other, rpc, config) = two_meshes_tagged(test).await;

    let at_other = Nodes::new(estate.carrier_at(&other.name).await);
    let own_mesh = at_other.get(&Filter::new().eq("mesh", ["mesh2"]).eq("tags", ["blue"])).await.unwrap_or_else(|e| panic!("node.get [mesh2, blue] at {}: {e:?}", other.name));
    assert_eq!(names(&own_mesh), [rpc.name.to_string()], "the record mesh2's mesh-primary's atlas holds, its tag present");
    assert_eq!(own_mesh[0].config, config, "the record's config present: {:?}", own_mesh[0]);
    let no_mesh = at_other.get(&Filter::new().eq("tags", ["red"])).await.unwrap_or_else(|e| panic!("node.get [red] at {}: {e:?}", other.name));
    assert_eq!(names(&no_mesh), [fp.name.to_string()], "a read naming no mesh reaches the fabric-primary's atlas through mesh2's mesh-primary");

    estate.stop().await;
    let spans = estate.spans();
    let (other_name, mp_name) = (other.name.to_string(), mp.name.to_string());
    let forwarded: Vec<&Value> = named(&spans, "rdm.node_admin.node.serve.via-forward").into_iter().filter(|sp| attr(sp, "node") == other_name).collect();
    let own = forwarded.iter().find(|sp| attr(sp, "pairs").contains("blue")).unwrap_or_else(|| panic!("{other_name} forwards [mesh2, blue]: {forwarded:#?}"));
    assert_eq!(attr(own, "mesh_primary"), mp_name, "forwarded to its own mesh-primary: {own}");
    assert_eq!(attr(own, "fabric_primary"), "", "never to the fabric-primary: {own}");
    assert_eq!(attr(own, "outcome"), "nodes", "{own}");
    let none = forwarded.iter().find(|sp| attr(sp, "pairs").contains("red")).unwrap_or_else(|| panic!("{other_name} forwards [red]: {forwarded:#?}"));
    assert_eq!(attr(none, "mesh_primary"), mp_name, "a read naming no mesh is forwarded to its own mesh-primary too: {none}");
    assert_eq!(attr(none, "fabric_primary"), "", "never to the fabric-primary: {none}");
    let carried: Vec<&Value> = named(&spans, "rdm.node_admin.node.serve.via-forward").into_iter().filter(|sp| attr(sp, "node") == mp_name && attr(sp, "pairs").contains("red")).collect();
    let up = carried.last().unwrap_or_else(|| panic!("{mp_name} carries [red] up"));
    assert_eq!(attr(up, "fabric_primary"), fp.name.to_string(), "mesh2's mesh-primary carries it up to the fabric-primary: {up}");
    let answered = named(&spans, "rdm.node_admin.node.serve.via-get").into_iter().filter(|sp| attr(sp, "node") == mp_name && attr(sp, "pairs").contains("blue")).count();
    assert!(answered > 0, "{mp_name} answers [mesh2, blue] from its atlas");
    let trace = own["trace_id"].as_str().unwrap_or_default().to_string();
    println!("forwarded read: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

// @feature: node-lifecycle
/// CONTRACT (query.md §1: "a node that is not a primary forwards a get to its own mesh-primary and
/// nowhere else"; Q243): mesh2's rpc node keeps no atlas. `node.get [mesh2, blue]` sent to it answers
/// its own record with its tag and its config: forwarded to mesh2's mesh-primary
/// (`rdm.node.node.serve.via-forward` at the rpc node naming mesh2's mesh-primary), which answers from
/// its atlas. `node.get [red]`, naming no mesh, answers the fabric-primary by its tag through mesh2's
/// mesh-primary. Its own `node.connection.get` stays its own. Must NOT happen: the rpc node refusing the get
/// (`RequestNotHandled`) or sending it to the fabric-primary itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_rpc_node_forwards_nodes_get_to_its_mesh_primary() {
    let test = "an_rpc_node_forwards_nodes_get_to_its_mesh_primary";
    let (mut estate, fp, mp, _other, rpc, config) = two_meshes_tagged(test).await;

    let at_rpc = Nodes::new(estate.carrier_at(&rpc.name).await);
    let own_mesh = at_rpc.get(&Filter::new().eq("mesh", ["mesh2"]).eq("tags", ["blue"])).await.unwrap_or_else(|e| panic!("node.get [mesh2, blue] at {}: {e:?}", rpc.name));
    assert_eq!(names(&own_mesh), [rpc.name.to_string()], "the record mesh2's mesh-primary's atlas holds, its tag present");
    assert_eq!(own_mesh[0].config, config, "the record's config present: {:?}", own_mesh[0]);
    let no_mesh = at_rpc.get(&Filter::new().eq("tags", ["red"])).await.unwrap_or_else(|e| panic!("node.get [red] at {}: {e:?}", rpc.name));
    assert_eq!(names(&no_mesh), [fp.name.to_string()], "a read naming no mesh reaches the fabric-primary's atlas through mesh2's mesh-primary");
    let connections = rdm_client::Connections::new(estate.carrier_at(&rpc.name).await).get(&birth_of(&rpc), &Filter::new()).await;
    assert!(connections.is_ok(), "node.connection.get of the rpc node is its own: {connections:?}");

    estate.stop().await;
    let spans = estate.spans();
    let (rpc_name, mp_name) = (rpc.name.to_string(), mp.name.to_string());
    let forwarded: Vec<&Value> = named(&spans, "rdm.node.node.serve.via-forward").into_iter().filter(|sp| attr(sp, "node") == rpc_name).collect();
    let own = forwarded.iter().find(|sp| attr(sp, "pairs").contains("blue")).unwrap_or_else(|| panic!("{rpc_name} forwards [mesh2, blue]: {forwarded:#?}"));
    assert_eq!(attr(own, "mesh_primary"), mp_name, "forwarded to its own mesh-primary: {own}");
    assert_eq!(attr(own, "outcome"), "nodes", "{own}");
    let none = forwarded.iter().find(|sp| attr(sp, "pairs").contains("red")).unwrap_or_else(|| panic!("{rpc_name} forwards [red]: {forwarded:#?}"));
    assert_eq!(attr(none, "mesh_primary"), mp_name, "a read naming no mesh is forwarded to its own mesh-primary too: {none}");
    let carried: Vec<&Value> = named(&spans, "rdm.node_admin.node.serve.via-forward").into_iter().filter(|sp| attr(sp, "node") == mp_name && attr(sp, "pairs").contains("red")).collect();
    let up = carried.last().unwrap_or_else(|| panic!("{mp_name} carries [red] up"));
    assert_eq!(attr(up, "fabric_primary"), fp.name.to_string(), "mesh2's mesh-primary carries it up to the fabric-primary: {up}");
    let trace = own["trace_id"].as_str().unwrap_or_default().to_string();
    println!("forwarded read: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}
