//! The Build request is a batch that names only what it adds or removes (build.md §1, Q342; Q280): a
//! `Create { meshes, nodes }`, each node whole by its Nodes schema, and a `Delete { meshes, node_ids }`,
//! each node by its node.id, every node with its own result. A Build never states a node it does not
//! touch, so it never writes back a property a node changed since the caller read it (Q236, amended by
//! Q342).
use rdm_client::{CallEnd, NodeStatus};
use rdm_core::build::{Meshes, Nodes};
use rdm_core::PathName;
use rdm_test::estate::{named, Estate, Owner};
use std::collections::BTreeMap;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-build".into(),
        subfeature: "batch-change".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// The Nodes schema of a node a Create adds at `path`.`name` in `mesh`, of `type`, on the estate's
/// executable for it: its storage the type's.
fn node(estate: &Estate, path: &str, name: &str, mesh: &str, r#type: &str) -> Nodes {
    Nodes {
        path: path.into(),
        name: name.into(),
        mesh: mesh.into(),
        r#type: r#type.into(),
        storage_persistence: None,
        executable: Some(estate.executables.of(r#type)),
        tags: Default::default(),
        config: Default::default(),
    }
}

fn at(path: &str, name: &str) -> PathName {
    PathName::new(path, name).unwrap()
}

// @feature: fabric-build
/// CONTRACT (build.md §1 "The request is a batch": `Create { meshes, nodes }`, "a node whose mesh is not
/// yet live creates that mesh with its first node-admin"; mesh-create.md): from Day 0's one admin, one
/// Create names two meshes not yet live, mesh2 and mesh3, and each one's node-admin and rpc node. The
/// fabric-primary accepts it as one Build, and once it completes every node it named is ready at the
/// path and name the request gave it, in its mesh. Must NOT happen: a mesh created with none of its
/// nodes, a node at a path the request did not name, more than one Build accepted for the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_of_two_meshes_and_their_nodes_is_one_build_that_births_each() {
    let cell = "a_create_of_two_meshes_and_their_nodes_is_one_build_that_births_each";
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let nodes = vec![
        node(&estate, "mesh2.node-admin", "1", "mesh2", "node-admin"),
        node(&estate, "mesh2.node", "1", "mesh2", "node"),
        node(&estate, "mesh3.node-admin", "1", "mesh3", "node-admin"),
        node(&estate, "mesh3.node", "1", "mesh3", "node"),
    ];
    let meshes = vec![Meshes { name: "mesh2".into() }, Meshes { name: "mesh3".into() }];
    let created = Estate::workflow("the Create of mesh2 and mesh3", estate.meshes_obj().await.create(meshes, nodes).await).await.expect("the call opened a Build");
    estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 1, 1), ("mesh3", 1, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    for (path, mesh) in [(at("mesh2.node-admin", "1"), "mesh2"), (at("mesh2.node", "1"), "mesh2"), (at("mesh3.node-admin", "1"), "mesh3"), (at("mesh3.node", "1"), "mesh3")] {
        let v = estate.node_view(&path).await;
        assert_eq!((v.status, v.mesh.as_str()), (NodeStatus::Ready, mesh), "{path:?} is ready in {mesh}: {v:?}");
    }
    estate.stop().await;
    let spans = estate.spans();
    let accepted: Vec<&serde_json::Value> = named(&spans, "rdm.node_admin.build.create.via-node-rpc").into_iter().filter(|sp| sp["attributes"]["build_id"].as_str().is_some_and(|b| !b.is_empty())).collect();
    assert_eq!(accepted.len(), 1, "the one Create is one Build: {accepted:?}");
    assert_eq!(accepted[0]["attributes"]["build_id"].as_str(), Some(created.build_id.to_string().as_str()), "the Build accepted is the Create's: {}", accepted[0]);
    estate.record_trace_url(accepted[0]["trace_id"].as_str().unwrap());
}

// @feature: fabric-build
/// CONTRACT (build.md §1: `Delete { meshes, node_ids }` names "each node by its node.id, never by its
/// path and name", refused by name "a node.id already deleted or replaced, in its own result"): mesh1
/// holds mesh1.node.1 and mesh1.node.2; a Delete naming mesh1.node.1's node.id deletes that birth and no
/// other. A second Delete of the same node.id is refused `unknown-node` naming it. A Create at the same
/// path and name births a new node.id there, and a Delete naming the replaced node.id is refused the
/// same way, the new birth untouched. Must NOT happen: a Delete resolving a path and name to whichever
/// birth holds it now, mesh1.node.2 deleted, a refusal that does not name the node.id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_by_node_id_deletes_that_birth_and_refuses_it_by_name_once_gone() {
    let cell = "a_delete_by_node_id_deletes_that_birth_and_refuses_it_by_name_once_gone";
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let nodes = estate.nodes_obj().await;
    let two = vec![node(&estate, "mesh1.node", "1", "mesh1", "node"), node(&estate, "mesh1.node", "2", "mesh1", "node")];
    Estate::workflow("the Create of mesh1.node.1 and mesh1.node.2", nodes.create(two).await).await.expect("the call opened a Build");
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    let (one, two) = (estate.node_view(&at("mesh1.node", "1")).await, estate.node_view(&at("mesh1.node", "2")).await);

    let deleted = Estate::workflow("the Delete of mesh1.node.1 by its node.id", nodes.delete(vec![one.node_id.clone()]).await).await.expect("the call opened a Build");
    estate.settled_views(&[("mesh1", 1, 1)]).await;
    assert_eq!(estate.node_view(&at("mesh1.node", "2")).await.node_id, two.node_id, "mesh1.node.2 is untouched by the Delete of mesh1.node.1");

    let refused_by_name = |end: Result<rdm_client::WorkflowStream, CallEnd>, what: &str| match end {
        Err(CallEnd::Rejected { reason, detail }) => {
            assert_eq!(reason, "unknown-node", "{what}: refused by name: {detail}");
            assert!(detail.contains(one.node_id.as_str()), "{what}: the refusal names the node.id {}: {detail}", one.node_id);
        }
        Err(other) => panic!("{what}: refused unknown-node naming {}, not {other}", one.node_id),
        Ok(_) => panic!("{what}: accepted; a node.id already deleted is refused by name"),
    };
    refused_by_name(nodes.delete(vec![one.node_id.clone()]).await, "a second Delete of the deleted node.id");

    Estate::workflow("the Create at mesh1.node.1 again", nodes.create(vec![node(&estate, "mesh1.node", "1", "mesh1", "node")]).await).await.expect("the call opened a Build");
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    let again = estate.node_view(&at("mesh1.node", "1")).await;
    assert_ne!(again.node_id, one.node_id, "the path and name are born again as a new node.id");
    refused_by_name(nodes.delete(vec![one.node_id.clone()]).await, "a Delete of the replaced node.id");
    let still = estate.node_view(&at("mesh1.node", "1")).await;
    assert_eq!((still.node_id.clone(), still.status), (again.node_id.clone(), NodeStatus::Ready), "the new birth at mesh1.node.1 is untouched by the refused Delete: {still:?}");
    estate.stop().await;
    let spans = estate.spans();
    let accepted = named(&spans, "rdm.node_admin.build.create.via-node-rpc").into_iter().find(|sp| sp["attributes"]["build_id"].as_str() == Some(deleted.build_id.to_string().as_str())).expect("the Delete's acceptance span");
    estate.record_trace_url(accepted["trace_id"].as_str().unwrap());
}

// @feature: fabric-build
/// CONTRACT (build.md §1: "The caller states only what it adds or removes, never a node it does not
/// touch: an existing node's `config` and `tags` change through `node.config.update` and `node.tag.*` at
/// any time, the last write winning, and no Build writes them back"; Q236 amended by Q342): a caller
/// reads mesh1.node.1 (`node.get`), then a `node.config.update` sets `zone=b` on it, then the caller's
/// Create adds mesh1.node.2. Once the Create completes, mesh1.node.1's config is still `zone=b`. Must NOT
/// happen: the Build writing back the config the caller read, `zone=a`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_config_update_made_after_the_callers_read_survives_a_create() {
    let cell = "a_config_update_made_after_the_callers_read_survives_a_create";
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let nodes = estate.nodes_obj().await;
    let zone = |z: &str| -> BTreeMap<String, String> { [("zone".to_string(), z.to_string())].into() };
    let first = Nodes { config: zone("a"), ..node(&estate, "mesh1.node", "1", "mesh1", "node") };
    Estate::workflow("the Create of mesh1.node.1 with zone=a", nodes.create(vec![first]).await).await.expect("the call opened a Build");
    estate.settled_views(&[("mesh1", 1, 1)]).await;
    let read = estate.node_view(&at("mesh1.node", "1")).await;
    assert_eq!(read.config, zone("a"), "the caller reads mesh1.node.1 with the config its Create named: {read:?}");

    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(read.node_id.clone()), node_id: read.node_id.clone(), incarnation: read.incarnation_id.clone().expect("the read names the incarnation") };
    let carrier = estate.carrier().await;
    let mesh_primary = estate.mesh_view("mesh1").await.primary_admin.expect("mesh1's primary");
    let updated = rdm_client::NodeRpc::new(carrier.client()).config_update(&mesh_primary, &rdm_transport::RouteChoice::Direct, &birth, zone("b"), &rdm_transport::CallOptions::default()).await;
    assert!(matches!(updated, Ok(rdm_core::status::StatusReply::ConfigUpdated { .. })), "node.config.update of mesh1.node.1 to zone=b: {updated:?}");

    let created = Estate::workflow("the caller's Create of mesh1.node.2", nodes.create(vec![node(&estate, "mesh1.node", "2", "mesh1", "node")]).await).await.expect("the call opened a Build");
    estate.settled_views(&[("mesh1", 1, 2)]).await;
    let after = estate.node_view(&at("mesh1.node", "1")).await;
    assert_eq!(after.config, zone("b"), "the update made after the caller's read survives its Create {}: {after:?}", created.build_id);
    estate.stop().await;
    let spans = estate.spans();
    let rewrites: Vec<&serde_json::Value> = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| sp["attributes"]["step"] == "UpdateConfig" && sp["attributes"]["build_id"].as_str() == Some(created.build_id.to_string().as_str()))
        .collect();
    assert!(rewrites.is_empty(), "the Create of mesh1.node.2 writes no node's config: {rewrites:?}");
    let accepted = named(&spans, "rdm.node_admin.build.create.via-node-rpc").into_iter().find(|sp| sp["attributes"]["build_id"].as_str() == Some(created.build_id.to_string().as_str())).expect("the Create's acceptance span");
    estate.record_trace_url(accepted["trace_id"].as_str().unwrap());
}

// @feature: fabric-build
/// CONTRACT (D58; build.md §1: the fabric-primary "composes the whole topology from its records" at
/// acceptance and writes the Build record once): the fabric-primary mints each created node's node.id
/// when it composes the topology, and the Build record names it. A Create of mesh1.node.1 and
/// mesh1.node.2 is accepted; its record's topology (`builds.jsonl`) names each created node by path and
/// name with its node.id, and once the Build completes node.get lists each node running with exactly
/// the node.id its record names. Must NOT happen: a record that names no node.id for a node it creates,
/// a node running with a node.id its Build never named (one minted after acceptance).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_creates_build_record_names_each_created_nodes_node_id_and_the_node_runs_with_it() {
    let cell = "a_creates_build_record_names_each_created_nodes_node_id_and_the_node_runs_with_it";
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let nodes = estate.nodes_obj().await;
    let two = vec![node(&estate, "mesh1.node", "1", "mesh1", "node"), node(&estate, "mesh1.node", "2", "mesh1", "node")];
    let created = Estate::workflow("the Create of mesh1.node.1 and mesh1.node.2", nodes.create(two).await).await.expect("the call opened a Build");
    let facts = crate::build_log::log_read(&estate, &created.build_id.to_string()).await.expect("the Create's Build record is on the store root");
    let accepted = facts.iter().find_map(|f| match f { rdm_admin::build_state::BuildFact::Accepted(a) => Some(a.clone()), _ => None }).expect("the Create's accepted record");
    let topology = serde_json::to_value(&accepted.topology).expect("the record's topology");
    let named_ids = topology["created"].as_array().cloned().unwrap_or_default();
    for path in [at("mesh1.node", "1"), at("mesh1.node", "2")] {
        let running = estate.node_view(&path).await;
        let in_record = named_ids.iter().find(|e| e[0]["path"] == path.path.as_str() && e[0]["name"] == path.name.as_str()).unwrap_or_else(|| panic!("the Build record names a node.id for {path:?}: {topology:#}"));
        assert_eq!(in_record[1].as_str(), Some(running.node_id.as_str()), "{path:?} runs with the node.id its Build record names: {topology:#}");
    }
    estate.stop().await;
    let spans = estate.spans();
    let accepted_span = named(&spans, "rdm.node_admin.build.create.via-node-rpc").into_iter().find(|sp| sp["attributes"]["build_id"].as_str() == Some(created.build_id.to_string().as_str())).expect("the Create's acceptance span");
    estate.record_trace_url(accepted_span["trace_id"].as_str().unwrap());
}
