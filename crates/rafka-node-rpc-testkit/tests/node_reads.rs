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
use rafka_node_admin_client::{BuildCarrier, BuildId, Builds, CallEnd, ConnectionsView, ExactBirth, Fabrics, MeshSelector, Meshes, NodeSelector, NodeStatus, Nodes};
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

        // build.get awaited: the Build the fabric's pointer names is the read that held it; a Build the
        // fabric-primary holds no fact of is never reported, and the call ends Indeterminate, naming the
        // fact, the Build, the admin and how its last read ended.
        let builds = Builds::new(carrier.clone());
        let day0 = BuildId(running.control.accepted.build_id().await.expect("the Day-0 fabric's pointer names its Build").0);
        let held = running.control.builds.read_build(&rafka_node_admin_core::build::BuildId(day0.0.clone())).await.unwrap_or_else(|e| panic!("the admin's Day-0 Build: {e}"));
        let awaited = builds.fact(&day0, "the Build is held", WRONG, |b| b.build_id == day0).await.unwrap_or_else(|e| panic!("build.get awaited: {e}"));
        assert_eq!((awaited.attempt, awaited.steps.len()), (held.attempt, held.steps.len()), "the Build as the admin holds it: {awaited:#?}");
        let unheld = BuildId(NodeId::mint().to_string());
        match builds.fact(&unheld, "the Build is held", Duration::from_secs(1), |b| b.build_id == unheld).await {
            Err(CallEnd::Indeterminate { reason }) => assert!(
                reason.contains("the Build is held") && reason.contains(&unheld.0) && reason.contains(&me.node.name.to_string()) && reason.contains("unknown-build"),
                "the unreported fact is named with the Build, the admin and how its last read ended: {reason}"
            ),
            other => panic!("a Build the admin holds no fact of ends Indeterminate, not {other:?}"),
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
        let admin = crate::common::admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let root = std::env::temp_dir().join(format!("node-reads-rpc-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let name: PathName = "mesh1.rpc.1".parse().unwrap();
        let dir = root.join(name.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
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
