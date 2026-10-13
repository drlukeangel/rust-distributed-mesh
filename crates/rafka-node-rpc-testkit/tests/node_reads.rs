//! The reads over Node RPC (node-rpc-envelope.md "Typed objects mapped to wire and execution
//! owners"): `node.get` answers one node by id or the nodes of a mesh, `mesh.get` one mesh or
//! every mesh of a fabric, `fabric.get` the fabric, each as the answering node-admin's view holds
//! it, and `node.connections.get` a node's held connections (fabric-mesh-node-lifecycles.md:
//! "Reading or cutting a node's held connections"). The typed objects read them from the node
//! itself over Node RPC; they hold no control API client.

use rafka_mesh_entity::connections::NodeConnection;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, NodeKind, PathName};
use rafka_node_admin_client::{BuildCarrier, CallEnd, ConnectionsView, ExactBirth, Fabrics, MeshSelector, Meshes, NodeRpc, NodeSelector, NodeStatus, Nodes, TagFilter};
use rafka_node_rpc_contract::status::TagScope;
use rafka_node_rpc_contract::status::StatusReply;
use rafka_node_admin_core::admin::{start_with, AdminConfig};
use rafka_node_admin_core::wiring::{FabricHooks, Wiring};
use rafka_node_rpc::{NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rafka_node_rpc_testkit::node;
use rafka_node_rpc_testkit::originate::Seams;
use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

const WRONG: Duration = Duration::from_secs(60);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.clone()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// A caller outside the mesh: its own endpoint, and a resolver that holds exactly `nodes`.
async fn caller(nodes: &[ResolvedNode]) -> (Arc<NodeRpcClient>, iroh::Endpoint) {
    let endpoint = rafka_node_rpc::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller's endpoint binds");
    let resolver = Arc::new(StaticResolver::new());
    for n in nodes {
        resolver.insert(n.clone());
    }
    (Arc::new(NodeRpcClient::new(endpoint.clone(), resolver).with_caller_system("rdm")), endpoint)
}

/// The ids of the connections a node holds: its latest Direct per destination that is `connected`,
/// and its active Proxies (the open entries `node.connections.delete` names).
fn own_ids(held: &rafka_mesh_entity::connections::ConnectionsHeld) -> BTreeSet<String> {
    held.own_latest_directs()
        .into_iter()
        .filter(|e| e.state == rafka_mesh_entity::connections::ConnectionState::Connected)
        .chain(held.own_active_proxies())
        .map(|e: &NodeConnection| e.id.to_string())
        .collect()
}

fn ids(view: &ConnectionsView) -> BTreeSet<String> {
    view.connections.iter().map(|c| c.id.clone()).collect()
}

/// CONTRACT: a Day-0 node-admin answers the four reads over Node RPC from what it holds, and the
/// caller reaches nothing but the admin's Node RPC endpoint. `node.get` of `mesh1` and of the
/// admin's id lists the admin as its view holds it (ReadyForTraffic, both seats, its birth, its
/// load and gossip stats when its view carries them); an id it does not hold lists nothing.
/// `mesh.get` of `mesh1`, by name and by id, is the one mesh with the admin as its primary and only
/// node; `mesh.get` of the fabric lists every mesh the view holds; a mesh it does not hold is
/// refused naming the mesh (`unknown-mesh`). `fabric.get` is `fabric1` with the admin as its
/// fabric-primary. `node.connections.get` of the admin's exact birth names the admin and exactly the
/// connections it holds. Each read is served by the admin: its serve span names what was read and
/// how much. What must NOT happen: a read that needs a control API, a read answered from anything
/// but the admin's own view, or an unknown mesh answered as an empty list.
#[test]
fn a_node_admin_answers_node_mesh_and_fabric_reads_and_its_connections_over_node_rpc() {
    if crate::own_process::delegated(module_path!(), "a_node_admin_answers_node_mesh_and_fabric_reads_and_its_connections_over_node_rpc") {
        return;
    }
    let cap = crate::common::capture("a_node_admin_answers_node_mesh_and_fabric_reads_and_its_connections_over_node_rpc");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("node-reads-{}", NodeId::mint()));
        let running = start_with(day0_config(&dir), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        let me = running.digest.lock().unwrap().clone();
        // The reads answer from the view the admin holds: it holds itself ready, in both seats.
        tokio::time::timeout(WRONG, async {
            loop {
                let ready = running.control.topology.read().await.births().any(|n| n.node_id == me.node.node_id && n.status == rafka_node_admin_core::model::NodeStatus::ReadyForTraffic && n.is_primary && n.is_fabric_primary);
                if ready {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the admin's view never held it ready in both seats");
        let admin = ResolvedNode { node_id: me.node.node_id.clone(), name: me.node.name.clone(), endpoint_id: me.node.endpoint_id.0.parse().unwrap(), incarnation: me.node.incarnation.clone(), transport_addr: me.node.transport_addr };
        let (client, _endpoint) = caller(&[admin]).await;
        let carrier = BuildCarrier::new(client.clone(), me.node.name.clone());
        let nodes = Nodes::new(carrier.clone());
        let meshes = Meshes::new(carrier.clone());
        let fabrics = Fabrics::new(carrier.clone());

        // node.get: what the admin's view holds of itself, by mesh and by id.
        let by_mesh = nodes.get(&NodeSelector::Mesh("mesh1".into())).await.unwrap_or_else(|e| panic!("node.get of mesh1: {e}"));
        let view = running.control.topology.read().await.clone();
        let loads = (running.control.loads.get().expect("the admin reads its members' loads"))();
        let held = view.births().find(|n| n.node_id == me.node.node_id).cloned().expect("the admin's view holds the admin");
        assert_eq!(by_mesh.len(), 1, "mesh1 is the admin alone: {by_mesh:#?}");
        let got = &by_mesh[0];
        assert_eq!((got.name.clone(), got.kind, got.mesh.as_str(), got.node_id.clone()), (held.name.clone(), NodeKind::NodeAdmin, "mesh1", me.node.node_id.clone()));
        assert_eq!((got.incarnation_id.clone(), got.endpoint_id.clone(), got.transport_addr), (Some(me.node.incarnation.clone()), Some(me.node.endpoint_id.0.clone()), Some(me.node.transport_addr)));
        assert_eq!((got.status, got.is_primary, got.is_fabric_primary), (NodeStatus::ReadyForTraffic, true, true), "{got:#?}");
        let (load, gossip) = loads.get(me.node.node_id.as_str()).cloned().unwrap_or((None, None));
        assert_eq!((got.load.is_some(), got.gossip.is_some()), (load.is_some(), gossip.is_some()), "the load and gossip stats are what the admin's view carries: {got:#?}");
        let by_id = nodes.get(&NodeSelector::Id(me.node.node_id.clone())).await.unwrap_or_else(|e| panic!("node.get of the admin's id: {e}"));
        assert_eq!(by_id.iter().map(|n| n.node_id.clone()).collect::<Vec<_>>(), vec![me.node.node_id.clone()]);
        let stranger = nodes.get(&NodeSelector::Id(NodeId::mint())).await.unwrap_or_else(|e| panic!("node.get of an id the admin does not hold: {e}"));
        assert!(stranger.is_empty(), "an id the view does not hold lists nothing: {stranger:#?}");

        // mesh.get: one mesh by name and by id, every mesh, and a mesh the admin does not hold.
        let mesh_id = me.mesh_id.clone().expect("the Day-0 admin holds its mesh id");
        for selector in [MeshSelector::Mesh("mesh1".into()), MeshSelector::Mesh(mesh_id.to_string())] {
            let one = meshes.get(&selector).await.unwrap_or_else(|e| panic!("mesh.get of {selector:?}: {e}"));
            assert_eq!(one.len(), 1, "{selector:?}: {one:#?}");
            assert_eq!((one[0].name.as_str(), one[0].primary_admin.clone(), one[0].nodes.clone()), ("mesh1", Some(me.node.name.clone()), vec![me.node.name.clone()]), "{selector:?}");
        }
        let every = meshes.get(&MeshSelector::Fabric).await.unwrap_or_else(|e| panic!("mesh.get of the fabric: {e}"));
        let held_meshes: Vec<String> = view.fabric_view().meshes.iter().map(|m| m.name.clone()).collect();
        assert_eq!(every.iter().map(|m| m.name.clone()).collect::<Vec<_>>(), held_meshes, "every mesh the view holds");
        match meshes.get(&MeshSelector::Mesh("mesh9".into())).await {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "unknown-mesh" && detail.contains("mesh9"), "refused naming the mesh: {reason}: {detail}"),
            other => panic!("a mesh the admin does not hold is refused by name, not {other:?}"),
        }

        // fabric.get.
        let fabric = fabrics.get().await.unwrap_or_else(|e| panic!("fabric.get: {e}"));
        assert_eq!((fabric.name.as_str(), fabric.fabric_primary.clone()), ("fabric1", Some(me.node.name.clone())));
        assert_eq!(fabric.meshes.iter().map(|m| m.name.clone()).collect::<Vec<_>>(), held_meshes);

        // fabric.get awaited: a fact the admin's view holds is the read that held it; a fact it never
        // reports ends the call Indeterminate, naming the fact, the admin and its last report.
        let awaited = fabrics.fact("the admin is the fabric-primary", WRONG, |f| f.fabric_primary.as_ref() == Some(&me.node.name)).await.unwrap_or_else(|e| panic!("fabric.get awaited: {e}"));
        assert_eq!(awaited.fabric_primary, Some(me.node.name.clone()));
        match fabrics.fact("a fabric-primary other than the admin", Duration::from_secs(1), |f| f.fabric_primary.as_ref().is_some_and(|p| *p != me.node.name)).await {
            Err(CallEnd::Indeterminate { reason }) => assert!(
                reason.contains("a fabric-primary other than the admin") && reason.contains(&me.node.name.to_string()) && reason.contains(&format!("fabric-primary {}", me.node.name)),
                "the unreported fact is named with the admin and its last report: {reason}"
            ),
            other => panic!("a fact the admin never reports ends Indeterminate, not {other:?}"),
        }

        // node.connections.get of the admin: exactly what it holds.
        let birth = ExactBirth { target: NodeTarget::ExactNode(me.node.node_id.clone()), node_id: me.node.node_id.clone(), incarnation: me.node.incarnation.clone() };
        let connections = nodes.connections_get(&birth).await.unwrap_or_else(|e| panic!("node.connections.get of the admin: {e}"));
        let (complete, own) = {
            let held = running.control.connections.get().expect("the admin holds its connections").lock().unwrap();
            (held.is_complete(), own_ids(&held))
        };
        assert_eq!((connections.node.as_str(), connections.complete), ("mesh1.admin.1", complete));
        assert_eq!(ids(&connections), own, "the connections the admin holds: {connections:#?}");

        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    // The trace: every read served by the admin, naming what was read and how much.
    let served = |name: &str, wanted: &dyn Fn(&std::collections::BTreeMap<String, String>) -> bool| spans.0.lock().unwrap().values().any(|(n, _, f)| n == name && wanted(f));
    let at = |f: &std::collections::BTreeMap<String, String>, k: &str, v: &str| f.get(k).map(String::as_str) == Some(v);
    assert!(served("rdm.node_admin.node.serve.via-get", &|f| at(f, "select", "mesh") && at(f, "nodes", "1")), "node.get of mesh1 served");
    assert!(served("rdm.node_admin.node.serve.via-get", &|f| at(f, "select", "id") && at(f, "nodes", "0")), "node.get of an unheld id served, listing nothing");
    assert!(served("rdm.node_admin.mesh.serve.via-get", &|f| at(f, "select", "mesh") && at(f, "meshes", "1")), "mesh.get of mesh1 served");
    assert!(served("rdm.node_admin.mesh.reject.via-unknown-mesh", &|f| at(f, "mesh", "mesh9")), "mesh.get of mesh9 refused");
    assert!(served("rdm.node_admin.fabric.serve.via-get", &|f| f.contains_key("meshes")), "fabric.get served");
    assert!(served("rdm.node_admin.node.serve.via-connections-get", &|f| at(f, "node", "mesh1.admin.1")), "node.connections.get served by the admin");
}

fn labels(l: &[&str]) -> BTreeSet<String> {
    l.iter().map(|s| s.to_string()).collect()
}

/// CONTRACT (R-TG1, Luke 2026-10-12; node-rpc-envelope.md node.tags: "`node.tags.create { label |
/// [labels] }` adds one label or a list; `node.tags.delete { All | [labels] }` removes every label or
/// the named ones ... Each label is its own keyed row (label, node id) in the node store's tags index.
/// Create is a blind put of that key, so creating a label the node already carries is ignored and
/// writes no duplicate; delete tombstones the named keys"; node.get: "the tag filter names a label,
/// matches every node that carries it, and is read through the node store's tags index"; proposal
/// node-properties-own-commands.md §1, §3): a caller's `node.tags.create {rabbit, red}` of the Day-0
/// admin ends `TagsCreated` carrying nothing already; `node.get` of its id answers those tags; `Any
/// {rabbit}` and `All {rabbit, red}` answer the admin, `All {rabbit, blue}` and `Any {blue}` nothing.
/// `node.tags.create {red}` again ends `TagsCreated` naming red as already carried, and nothing
/// changes. `node.tags.delete {rabbit, green}` ends `TagsDeleted` naming green as not carried, and
/// rabbit no longer selects the admin. A create whose label is not a label (`Rabbit`) reaching the
/// mesh-primary is refused `RejectedTags` naming it, and nothing changes. `node.tags.delete { All }`
/// leaves the admin no label and selected by none. The mesh-primary's `via-writer` spans name the
/// labels that arrived and left; each tag read's `via-get` span names `select=tags`, its filter, its
/// labels and one index read per label. What must NOT happen: a label lost, a removed label still
/// selecting the node, a duplicate row for a label already carried, or a tag read that reads anything
/// but the tags index.
#[test]
fn node_tags_create_and_delete_set_a_nodes_tags_and_node_get_answers_it_by_tag_through_the_tags_index() {
    if crate::own_process::delegated(module_path!(), "node_tags_create_and_delete_set_a_nodes_tags_and_node_get_answers_it_by_tag_through_the_tags_index") {
        return;
    }
    let cap = crate::common::capture("node_tags_create_and_delete_set_a_nodes_tags_and_node_get_answers_it_by_tag_through_the_tags_index");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("node-tags-{}", NodeId::mint()));
        let running = start_with(day0_config(&dir), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        let me = running.digest.lock().unwrap().clone();
        // A tags command is refused until the initial fabric has first reached ready-for-traffic (states.md "The
        // initial fabric before ready-for-traffic"): the admin is ready in both seats, and the fabric is ready.
        tokio::time::timeout(WRONG, async {
            loop {
                let t = running.control.topology.read().await;
                let ready = t.fabric.status == rafka_node_admin_core::model::ScopeStatus::ReadyForTraffic
                    && t.births().any(|n| n.node_id == me.node.node_id && n.status == rafka_node_admin_core::model::NodeStatus::ReadyForTraffic && n.is_primary && n.is_fabric_primary);
                drop(t);
                if ready {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the admin's view never held it ready in both seats under a ready-for-traffic fabric");
        let admin = ResolvedNode { node_id: me.node.node_id.clone(), name: me.node.name.clone(), endpoint_id: me.node.endpoint_id.0.parse().unwrap(), incarnation: me.node.incarnation.clone(), transport_addr: me.node.transport_addr };
        let (client, _endpoint) = caller(&[admin]).await;
        let nodes = Nodes::new(BuildCarrier::new(client.clone(), me.node.name.clone()));
        let by_id = || async { nodes.get(&NodeSelector::Id(me.node.node_id.clone())).await.unwrap_or_else(|e| panic!("node.get of the admin's id: {e}")) };
        let by_tags = |f: TagFilter| {
            let nodes = nodes.clone();
            async move { nodes.get(&NodeSelector::Tags(f.clone())).await.unwrap_or_else(|e| panic!("node.get of {f:?}: {e}")).into_iter().map(|n| n.node_id).collect::<Vec<_>>() }
        };
        let got = by_id().await.first().cloned().expect("node.get lists the admin by its id");
        assert!(got.tags.is_empty(), "a node born with no tags carries none: {got:#?}");
        let route = rafka_node_rpc::RouteChoice::Direct;
        let mine = vec![me.node.node_id.clone()];
        let birth = ExactBirth { target: NodeTarget::ExactNode(me.node.node_id.clone()), node_id: me.node.node_id.clone(), incarnation: me.node.incarnation.clone() };
        let created = |already: &[&str]| StatusReply::TagsCreated { node_id: me.node.node_id.clone(), incarnation: me.node.incarnation.clone(), already_carried: labels(already) };
        let deleted = |not: &[&str]| StatusReply::TagsDeleted { node_id: me.node.node_id.clone(), incarnation: me.node.incarnation.clone(), not_carried: labels(not) };

        // node.tags.create {rabbit, red}.
        let reply = nodes.tags_create(&birth, &me.node.node_id, &route, &labels(&["rabbit", "red"])).await.expect("node.tags.create answered");
        assert_eq!(reply, created(&[]));
        assert_eq!(by_id().await[0].tags, labels(&["rabbit", "red"]), "node.get of the admin's id answers the labels created");
        assert_eq!(by_tags(TagFilter::Any(labels(&["rabbit"]))).await, mine, "Any {{rabbit}} answers the admin");
        assert_eq!(by_tags(TagFilter::All(labels(&["rabbit", "red"]))).await, mine, "All {{rabbit, red}} answers the admin");
        assert!(by_tags(TagFilter::All(labels(&["rabbit", "blue"]))).await.is_empty(), "All {{rabbit, blue}} answers nothing: the admin carries no blue");
        assert!(by_tags(TagFilter::Any(labels(&["blue"]))).await.is_empty(), "Any {{blue}} answers nothing");

        // A label the node already carries is ignored and named.
        let reply = nodes.tags_create(&birth, &me.node.node_id, &route, &labels(&["red"])).await.expect("node.tags.create answered");
        assert_eq!(reply, created(&["red"]), "red is already carried");
        assert_eq!(by_id().await[0].tags, labels(&["rabbit", "red"]), "nothing changes");

        // node.tags.delete {rabbit, green}: green is not carried.
        let reply = nodes.tags_delete(&birth, &me.node.node_id, &route, &TagScope::Labels(labels(&["rabbit", "green"]))).await.expect("node.tags.delete answered");
        assert_eq!(reply, deleted(&["green"]));
        assert!(by_tags(TagFilter::Any(labels(&["rabbit"]))).await.is_empty(), "Any {{rabbit}} answers nothing once rabbit is deleted");
        assert_eq!(by_tags(TagFilter::Any(labels(&["red", "rabbit"]))).await, mine, "Any {{red, rabbit}} answers the admin once");
        assert_eq!(by_id().await[0].tags, labels(&["red"]), "node.get of the admin's id answers the label left");

        // A create whose label is not a label, past the typed object's own check, is refused by the
        // mesh-primary naming it, and nothing changes.
        let refused = NodeRpc::new(&client).tags_create(&me.node.node_id, &route, &birth, &labels(&["Rabbit"]), &rafka_node_rpc::CallOptions::default()).await.expect("a typed reply");
        assert!(matches!(&refused, StatusReply::RejectedTags { reason } if reason.contains("\"Rabbit\"")), "refused naming the label: {refused:?}");
        assert_eq!(by_id().await[0].tags, labels(&["red"]), "a refused create changes nothing");

        // node.tags.delete { All }.
        let reply = nodes.tags_delete(&birth, &me.node.node_id, &route, &TagScope::All).await.expect("node.tags.delete answered");
        assert_eq!(reply, deleted(&[]));
        assert!(by_id().await[0].tags.is_empty(), "the admin carries no label");
        assert!(by_tags(TagFilter::Any(labels(&["red"]))).await.is_empty(), "Any {{red}} answers nothing");

        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    let named = |name: &str| spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, parent, f)| (parent.clone(), f.clone())).collect::<Vec<_>>();
    let at = |f: &std::collections::BTreeMap<String, String>, k: &str| f.get(k).cloned().unwrap_or_default();
    let creates: Vec<(String, String, String)> = named("rdm.node_admin.node.tags.create.via-writer").iter().map(|(_, f)| (at(f, "outcome"), at(f, "arrived"), at(f, "already_carried"))).collect();
    assert!(creates.contains(&("tags-created".into(), "rabbit,red".into(), "".into())), "the first create's labels arrived: {creates:?}");
    assert!(creates.contains(&("tags-created".into(), "".into(), "red".into())), "the second create wrote nothing new: {creates:?}");
    assert!(creates.iter().any(|(o, _, _)| o == "rejected-tags"), "the non-label create was refused: {creates:?}");
    let deletes: Vec<(String, String, String)> = named("rdm.node_admin.node.tags.delete.via-writer").iter().map(|(_, f)| (at(f, "outcome"), at(f, "left"), at(f, "not_carried"))).collect();
    assert!(deletes.contains(&("tags-deleted".into(), "rabbit".into(), "green".into())), "the named delete removed rabbit: {deletes:?}");
    assert!(deletes.contains(&("tags-deleted".into(), "red".into(), "".into())), "the All delete removed red: {deletes:?}");
    let reads = named("rdm.node_admin.node.serve.via-get");
    let tag_reads: Vec<(String, String, String, String)> = reads.iter().filter(|(_, f)| at(f, "select") == "tags").map(|(_, f)| (at(f, "filter"), at(f, "labels"), at(f, "index_reads"), at(f, "nodes"))).collect();
    assert!(tag_reads.contains(&("all".into(), "rabbit,red".into(), "2".into(), "1".into())), "All {{rabbit, red}}: two index reads, one node: {tag_reads:?}");
    assert!(tag_reads.contains(&("any".into(), "blue".into(), "1".into(), "0".into())), "Any {{blue}}: one index read, no node: {tag_reads:?}");
}

/// CONTRACT: an rpc node answers `node.connections.get` of its exact birth itself, with exactly the
/// connections it holds (its latest Direct per destination that is connected and its active
/// Proxies), named by the minted ids `node.connections.delete` names, its route to its admin among
/// them; the answer names the node by the name it holds. What must NOT happen: a control API in the
/// path, or a connection the node does not hold.
#[test]
fn an_rpc_node_answers_its_held_connections_by_the_ids_its_delete_names() {
    let cap = crate::common::capture("an_rpc_node_answers_its_held_connections_by_the_ids_its_delete_names");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = crate::common::admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("node-reads-rpc-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let name: PathName = "mesh1.rpc.1".parse().unwrap();
        let dir = root.join(name.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rafka_mesh_entity::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = Launch {
            fabric: "fabric1".into(),
            fabric_id: fabric.clone(),
            name: name.clone(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            seeds: vec![admin.seed.clone()],
            launcher: Some(admin.launcher.clone()),
            data_dir: dir,
            store_dir: store.clone(),
            mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
            mesh_issuer: None,
            connections_delete_deadline: rafka_mesh_entity::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        };
        admin.deployed(&launch, &key);
        let seams: Arc<OnceLock<Seams>> = Arc::default();
        let slot = seams.clone();
        let _running = node::start_with_seams(&launch, move |b, s| {
            let _ = slot.set(s);
            b
        })
        .await
        .unwrap_or_else(|e| panic!("{name} comes up: {e}"));
        let seams = seams.get().cloned().expect("the node handed its seams");
        let book = &admin.observer.membership.book;
        let mut heard = book.heard_changes();
        while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic) {
            heard.changed().await.unwrap();
        }
        // The node originates a data-plane connection to its admin: a Direct entry of its log.
        match seams.client.call::<rafka_node_rpc_contract::ping::Ping>(&NodeTarget::ExactNode(admin.launcher.node_id.clone()), &rafka_node_rpc_contract::ping::PingRequest::Ping { payload: b"the node reaches its admin".to_vec() }, &rafka_node_rpc::CallOptions::default()).await.0 {
            rafka_node_rpc_contract::outcome::RpcOutcome::Reply(_) => {}
            other => panic!("the node's data-plane ping did not end in a reply: {other:?}"),
        }
        seams.connections.drain().await;

        let (d, _) = book.get(launch.node_id.as_str()).unwrap();
        let rpc = ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr };
        let (client, _endpoint) = caller(&[rpc]).await;
        let nodes = Nodes::new(BuildCarrier::new(client.clone(), "mesh1.admin.1".parse().unwrap()));
        let birth = ExactBirth { target: NodeTarget::ExactNode(launch.node_id.clone()), node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone() };
        let connections = nodes.connections_get(&birth).await.unwrap_or_else(|e| panic!("node.connections.get of {name}: {e}"));
        let (complete, own) = {
            let held = seams.connections.held();
            let held = held.lock().unwrap();
            (held.is_complete(), own_ids(&held))
        };
        assert_eq!((connections.node.as_str(), connections.complete), ("mesh1.rpc.1", complete));
        assert_eq!(ids(&connections), own, "exactly the connections the node holds: {connections:#?}");
        let to_admin = connections.connections.iter().find(|c| c.destination == "mesh1.admin.1").unwrap_or_else(|| panic!("the node's route to its admin is listed: {connections:#?}"));
        assert_eq!((to_admin.source.as_str(), to_admin.kind.as_str(), to_admin.state.as_str()), ("mesh1.rpc.1", "direct", "connected"));
        let _ = std::fs::remove_dir_all(&root);
    });
    assert!(spans.0.lock().unwrap().values().any(|(n, _, f)| n == "rdm.node_admin.node.serve.via-connections-get" && f.get("node").map(String::as_str) == Some("mesh1.rpc.1")), "node.connections.get served by the rpc node");
}
