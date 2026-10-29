//! Where a read sent to a mesh-primary is answered (query.md §1; Q224: only a primary keeps an atlas, a
//! mesh-primary's holding its own mesh and nothing else, the fabric-primary's every mesh).
//!
//! A mesh-primary answers a read of its own mesh from its atlas, the fabric-primary not asked, and
//! carries any other read up to the fabric-primary, whose atlas holds every mesh: a read of another
//! mesh, a read naming no mesh, and `fabric.get`.

use rdm_client::{ExactBirth, Filter, NodeView, Nodes};
use rdm_core::NodeKind;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::process_faults::{ExactRuntime, Fault};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "mesh-primary-reads".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
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

/// The exact birth of `n`, the node a tags command names.
fn birth_of(n: &NodeView) -> ExactBirth {
    ExactBirth { target: rdm_transport::NodeTarget::ExactNode(n.node_id.clone()), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap_or_else(|| panic!("{} names its incarnation", n.name)) }
}

// @feature: node-lifecycle
/// CONTRACT (query.md §1: "a mesh-primary answers a read of its own mesh from its atlas and carries any
/// other read up to the fabric-primary"; Q224): two meshes, the fabric-primary mesh1.admin.1 and
/// mesh2's mesh-primary with one rpc node, each of mesh1.admin.1 and mesh2's rpc node carrying a tag on
/// its row. `node.get` sent to mesh2's mesh-primary naming mesh1, and one naming no mesh, answer
/// mesh1.admin.1 by its tag: carried up to the fabric-primary, whose atlas holds mesh1's rows
/// (`rdm.node_admin.node.serve.via-forward` at mesh2's mesh-primary naming the fabric-primary). With
/// the fabric-primary's runtime held (SIGSTOP), `node.get` of mesh2 sent to mesh2's mesh-primary still
/// answers mesh2's rpc node by its tag, from its own atlas, with no store read
/// (`rdm.node_admin.node.serve.via-get` at mesh2's mesh-primary, `io_calls` 0). Must NOT happen:
/// another mesh's read answered by mesh2's mesh-primary from its own view (rows without their tags,
/// `[]`); its own mesh's read sent to the fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_answers_its_own_meshs_read_with_the_fabric_primary_held_and_carries_another_meshs_read_up() {
    let test = "a_mesh_primary_answers_its_own_meshs_read_with_the_fabric_primary_held_and_carries_another_meshs_read_up";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 1, 1)]).await;
    estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 1, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter_fabric_primary().await;
    let views = estate.node_views().await;
    let fp = views.iter().find(|n| n.is_fabric_primary).cloned().unwrap_or_else(|| panic!("a fabric-primary: {views:#?}"));
    let mp = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).cloned().unwrap_or_else(|| panic!("mesh2's mesh-primary: {views:#?}"));
    let rpc = views.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::RpcNode).cloned().unwrap_or_else(|| panic!("mesh2's rpc node: {views:#?}"));
    assert_eq!(fp.mesh, "mesh1", "the fabric-primary is mesh1's: {fp:?}");

    // Commands go down: each tag is sent to the fabric-primary, which sends it to the node's mesh-primary.
    let nodes = estate.nodes_obj().await;
    for (n, tag) in [(&fp, "red"), (&rpc, "blue")] {
        let reply = nodes.tags_create(&birth_of(n), &fp.node_id, &rdm_transport::RouteChoice::Direct, &labels(&[tag])).await.unwrap_or_else(|e| panic!("node.tags.create of {} at the fabric-primary: {e:?}", n.name));
        assert_eq!(reply.name(), "tags-created", "{}: {reply:?}", n.name);
    }
    wait_for("the fabric-primary answers both tags", Duration::from_secs(30), || async {
        let red = nodes.get(&Filter::new().eq("tags", ["red"])).await.ok()?;
        let blue = nodes.get(&Filter::new().eq("tags", ["blue"])).await.ok()?;
        (names(&red) == [fp.name.to_string()] && names(&blue) == [rpc.name.to_string()]).then_some(())
    })
    .await;

    // Reads sent to mesh2's mesh-primary.
    let at_mp = Nodes::new(estate.carrier_at(&mp.name).await);
    let other_mesh = at_mp.get(&Filter::new().eq("mesh", ["mesh1"]).eq("tags", ["red"])).await.unwrap_or_else(|e| panic!("node.get [mesh1, red] at {}: {e:?}", mp.name));
    assert_eq!(names(&other_mesh), [fp.name.to_string()], "another mesh's read is carried up to the fabric-primary, whose atlas holds mesh1's rows");
    let no_mesh = at_mp.get(&Filter::new().eq("tags", ["red"])).await.unwrap_or_else(|e| panic!("node.get [red] at {}: {e:?}", mp.name));
    assert_eq!(names(&no_mesh), [fp.name.to_string()], "a read naming no mesh is carried up to the fabric-primary");

    // The fabric-primary's runtime held: mesh2's mesh-primary answers its own mesh from its atlas.
    let fp_dir = estate.data_dir(&fp.name.to_string()).await;
    let fp_rt = ExactRuntime::published(Path::new(&fp_dir)).unwrap_or_else(|r| panic!("{}: no exact runtime published in {fp_dir}: {r:?}", fp.name));
    let held = fp_rt.apply(Fault::Stop).expect("the fabric-primary's runtime is held");
    assert_eq!(held.state_after, Some('T'), "the OS holds the fabric-primary: {held:?}");
    let own = at_mp.get(&Filter::new().eq("mesh", ["mesh2"]).eq("tags", ["blue"])).await;
    fp_rt.apply(Fault::Continue).expect("the fabric-primary's runtime is released");
    let own = own.unwrap_or_else(|e| panic!("node.get [mesh2, blue] at {} with the fabric-primary held: {e:?}", mp.name));
    assert_eq!(names(&own), [rpc.name.to_string()], "mesh2's mesh-primary answers its own mesh from its atlas, the fabric-primary not asked");

    estate.stop().await;
    let spans = estate.spans();
    let mp_name = mp.name.to_string();
    let carried: Vec<&Value> = named(&spans, "rdm.node_admin.node.serve.via-forward").into_iter().filter(|sp| attr(sp, "node") == mp_name).collect();
    let up = carried.iter().find(|sp| attr(sp, "pairs").contains("mesh1") && attr(sp, "pairs").contains("red")).unwrap_or_else(|| panic!("{mp_name} carries [mesh1, red] up: {carried:#?}"));
    assert_eq!(attr(up, "fabric_primary"), fp.name.to_string(), "the read is carried to the fabric-primary: {up}");
    assert_eq!(attr(up, "outcome"), "nodes", "{up}");
    assert!(!carried.iter().any(|sp| attr(sp, "pairs").contains("mesh2")), "{mp_name} carries none of its own mesh's reads up: {carried:#?}");
    let answered: Vec<&Value> = named(&spans, "rdm.node_admin.node.serve.via-get").into_iter().filter(|sp| attr(sp, "node") == mp_name && attr(sp, "pairs").contains("blue")).collect();
    let own_read = answered.last().unwrap_or_else(|| panic!("{mp_name} answers [mesh2, blue] itself"));
    assert_eq!(attr(own_read, "nodes"), "1", "{own_read}");
    assert_eq!(attr(own_read, "io_calls"), "0", "no store read: {own_read}");
    let trace = up["trace_id"].as_str().unwrap_or_default().to_string();
    println!("carried read: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}
