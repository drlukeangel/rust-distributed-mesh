//! A running node's own config: the one call an application makes (`RunningNode::config_update`).

use crate::common::{admin_side, TEST_MESH_ID};
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::status::{StatusReply, StatusRequest};
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId};
use rdm_test::node;
use std::collections::BTreeMap;
use std::time::Duration;

// @feature: node-lifecycle
/// CONTRACT (i150 R3): own updates persist locally, and a caller's downward apply uses the
/// same node writer. Both the application's current map and its durable record agree; no
/// own update goes to a primary or waits for its response.
#[test]
fn a_running_node_persists_own_and_caller_config_through_one_writer() {
    let cap = crate::common::capture("a_running_node_persists_own_and_caller_config_through_one_writer");
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("node-own-config-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let fabric = FabricId::mint();
        let admin = admin_side("127.0.0.1".parse().unwrap(), &fabric).await;
        let launch = Launch { initial_properties: Default::default(), topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: fabric.clone(),
            mesh: "mesh1".into(),
            kind: rdm_core::NodeKind::RpcNode,
            name: rdm_core::PathName::new("mesh1.rpc", "1").unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            seeds: vec![admin.seed.clone()],
            launcher: Some(admin.launcher.clone()),
            data_dir: dir.clone(),
            store_dir: dir.clone(),
            mesh_id: Some(MeshId::parse(TEST_MESH_ID).unwrap()),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        let key = node::load_or_mint_key(&dir).unwrap();
        admin.deployed(&launch, &key);
        let running = node::start(&launch, |b, _| b).await.expect("the admitted node starts");
        let primary = running.topology_view.primaries().mesh("mesh1").expect("the node's view names mesh1's primary");
        assert_eq!(primary.node_id, admin.launcher.node_id, "the admin side is mesh1's primary in the node's view");

        let config: BTreeMap<String, String> = [("listener.data".to_string(), "127.0.0.1:43003".to_string())].into();
        let reply = running.config_update(config.clone()).await.expect("a local durable write answers");
        assert!(matches!(reply, StatusReply::ConfigUpdated { ref config, .. } if config.get("listener.data").map(String::as_str) == Some("127.0.0.1:43003")));
        assert!(admin.config_updates.lock().unwrap().is_empty(), "own properties never travel to the mesh-primary");
        assert_eq!(running.topology_view.config(), Some(config.clone()));
        let addr = running.topology_view.endpoint().bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("the node's address");
        admin.observer.resolver.insert(rdm_transport::ResolvedNode { mesh: launch.mesh.clone(), kind: launch.kind, node_id: launch.node_id.clone(), name: launch.name.clone(), endpoint_id: key.public(), incarnation: launch.incarnation.clone(), transport_addr: addr });
        let target = rdm_transport::NodeTarget::ExactNode(launch.node_id.clone());
        let caller: BTreeMap<String, String> = [("source".into(), "caller".into())].into();
        let apply = StatusRequest::ApplyNodeConfig { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), config: caller.clone() };
        let (out, _) = admin.observer.client.call::<rdm_core::status::Status>(&target, &apply, &rdm_transport::CallOptions::default()).await;
        assert!(matches!(out, rdm_core::outcome::RpcOutcome::Reply(ref r) if *r.value() == StatusReply::Applied), "{out:?}");
        assert_eq!(running.topology_view.config(), Some(caller));
        let later: BTreeMap<String, String> = [("source".into(), "own".into())].into();
        running.config_update(later.clone()).await.expect("the later own write answers");
        assert_eq!(running.topology_view.config(), Some(later.clone()));
        // The same wire endpoint applies config and tags atomically for one Build entry.
        let entry = rdm_core::properties::EntryProperties {
            config: [("source".into(), "build".into())].into(), tags: ["build".into()].into() };
        let apply = StatusRequest::ApplyNodeProperties { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), entry };
        let (ack, _) = admin.observer.client.call::<rdm_core::status::Status>(&target, &apply, &rdm_transport::CallOptions::default()).await;
        assert!(matches!(ack, rdm_core::outcome::RpcOutcome::Reply(ref r) if *r.value() == StatusReply::Applied), "{ack:?}");
        drop(ack); // The caller retains no completion knowledge.
        running.config_update(later.clone()).await.unwrap();
        let tags = StatusRequest::CreateTags { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), labels: ["node".into()].into() };
        let (ack, _) = admin.observer.client.call::<rdm_core::status::Status>(&target, &tags, &rdm_transport::CallOptions::default()).await;
        assert!(matches!(ack, rdm_core::outcome::RpcOutcome::Reply(ref r) if matches!(r.value(), StatusReply::TagsCreated { .. })), "{ack:?}");
        assert_eq!(running.topology_view.config(), Some(later.clone()));
        use rdm_node::node_records::NodeRecordsStorage;
        let root = rdm_node::store_root::StoreRoot::open(&launch.store_dir, &launch.fabric_id).unwrap();
        let records = rdm_node::node_records::FileNodeRecords::open(&root).await.unwrap();
        let durable = records.properties(&launch.node_id).await.unwrap().unwrap();
        assert_eq!(durable.config, later);
        assert_eq!(durable.tags, ["build".into(), "node".into()].into());
        assert!(admin.config_updates.lock().unwrap().is_empty());
        running.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    let calls = cap.spans.0.lock().unwrap();
    assert!(calls.values().all(|(name, _, _)| name != "rdm.node_admin.config.update.via-call"));
    assert!(calls.values().any(|(name, _, _)| name == "rdm.node.record.update.via-properties"));
}

fn estate_owner(test: &str) -> rdm_test::estate::Owner {
    rdm_test::estate::Owner {
        product: "mesh".into(),
        feature: "node-config".into(),
        subfeature: "own-config".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// The config `node.get` at `admin` answers for `node`, from that admin's atlas.
async fn config_at(estate: &rdm_test::estate::Estate, admin: &rdm_core::PathName, node: &rdm_core::NodeId) -> Option<BTreeMap<String, String>> {
    let views = estate.node_views_at(admin).await?;
    views.into_iter().find(|v| v.node_id == *node).map(|v| v.config)
}

// @feature: node-lifecycle
/// CONTRACT (i150 R3/R11): both primaries read a node's own acknowledged map directly from
/// the same shared record. There is no upward config delivery or copied atlas map.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nodes_own_config_is_read_from_its_record_at_both_primaries() {
    let cell = "a_nodes_own_config_is_read_from_its_record_at_both_primaries";
    let mut estate = rdm_test::estate::Estate::bootstrap(estate_owner(cell), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 1, 1)]).await;
    let views = estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 1, 1)]).await;
    let node_id = views.iter().find(|n| n.mesh == "mesh2" && n.kind == rdm_core::NodeKind::RpcNode).expect("mesh2 rpc node").node_id.clone();
    let target = format!("exact:{node_id}");
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let out = estate.probe(&["own-config", "--target", &target, "--value", "listener.data=127.0.0.1:45005"]);
    assert_eq!(out["own_config"], "config-updated", "mesh2's primary answers the node's own update: {out}");
    let config: BTreeMap<String, String> = [("listener.data".to_string(), "127.0.0.1:45005".to_string())].into();
    assert_eq!(config_at(&estate, &rdm_core::PathName::new("mesh2.admin", "1").unwrap(), &node_id).await, Some(config.clone()), "mesh2's primary answers the map at once, from its atlas");
    assert_eq!(config_at(&estate, &rdm_core::PathName::new("mesh1.admin", "1").unwrap(), &node_id).await, Some(config), "the fabric-primary reads the same durable record without a push");
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &serde_json::Value, k: &str| sp["attributes"][k].as_str().map(str::to_string).unwrap_or_else(|| sp["attributes"][k].to_string());
    let written = rdm_test::estate::named(&spans, "rdm.node.record.update.via-properties").into_iter().find(|span| attr(span, "node_id") == node_id.as_str()).expect("the node wrote its own record");
    assert_eq!(attr(written, "outcome"), "durable");
    assert!(rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-config-push").is_empty(), "no primary sends copied config upward");
    assert!(rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-apply-node-config").iter().all(|span| attr(span, "node") != node_id.as_str()), "an own update requires no config command back to the node");
    assert!(rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-config").iter().all(|span| attr(span, "apply") != "own"));
    estate.record_trace_url(written["trace_id"].as_str().unwrap());
}
