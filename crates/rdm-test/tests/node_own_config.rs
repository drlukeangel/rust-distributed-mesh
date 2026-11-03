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
            r#type: "node".into(),
            path: "mesh1.node".into(), name: "1".into(),
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
        admin.observer.resolver.insert(rdm_transport::ResolvedNode { mesh: launch.mesh.clone(), r#type: launch.r#type, node_id: launch.node_id.clone(), path: launch.path.clone(), name: launch.name.clone(), endpoint_id: key.public(), incarnation: launch.incarnation.clone(), transport_addr: addr });
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
        use rdm_node::nodes_storage::NodesStorage;
        // The node's own part of its record, read back from the fabric's nodes.jsonl (Q331, Q333).
        let root = rdm_node::store_root::StoreRoot::open(&launch.store_dir, &launch.fabric_id).unwrap();
        let records = rdm_node::nodes_storage::FileNodesStorage::open(&launch.data_dir, &root, &launch.mesh, &rdm_node::store_log::LogWriter::mint()).await.unwrap();
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
    let node_id = views.iter().find(|n| n.mesh == "mesh2" && n.r#type == "node").expect("mesh2 node").node_id.clone();
    let target = format!("exact:{node_id}");
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let out = estate.probe(&["own-config", "--target", &target, "--value", "listener.data=127.0.0.1:45005"]);
    assert_eq!(out["own_config"], "config-updated", "mesh2's primary answers the node's own update: {out}");
    let config: BTreeMap<String, String> = [("listener.data".to_string(), "127.0.0.1:45005".to_string())].into();
    assert_eq!(config_at(&estate, &rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), &node_id).await, Some(config.clone()), "mesh2's primary answers the map at once, from its atlas");
    assert_eq!(config_at(&estate, &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), &node_id).await, Some(config), "the fabric-primary reads the same durable record without a push");
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

// @feature: node-lifecycle
/// CONTRACT (Q227: the fabric-primary of a single mesh with nobody to pull from reads the node
/// records; fabric-recovery.md "Nothing alive"; node-config-update.md "Build application": "True
/// recovery inherits the predecessor's acknowledged properties."): mesh1's fabric-primary, its only
/// node-admin (one node beside it, so its store names a node and Q200's Day 0 is not this case),
/// changes its own config, is lost, and a person starts a node-admin again on its data dir (fabric
/// recovery, a new birth at its path and name). The new birth's config is the changed map, read from
/// the lost birth's own part of its record in nodes.jsonl by the recovery's claim, and the new birth
/// writes that map as its own part. Must NOT happen: the new birth starting from an empty launch
/// snapshot, the change made before the loss gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_fabric_primary_keeps_its_own_config_across_its_restart() {
    let cell = "a_lone_fabric_primary_keeps_its_own_config_across_its_restart";
    let shape = [("mesh1", 1, 1)];
    let mut estate = rdm_test::estate::Estate::bootstrap(estate_owner(cell), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settled_views(&shape).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let fp = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
    let before = estate.node_view(&fp).await;
    let config: BTreeMap<String, String> = [("listener.data".to_string(), "127.0.0.1:45007".to_string())].into();
    // node.config.update of its own exact birth, sent to it as the fabric-primary, which sends it down
    // to the node's mesh-primary, itself (node-config-update.md "A caller's update").
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(before.node_id.clone()), node_id: before.node_id.clone(), incarnation: before.incarnation_id.clone().expect("the fabric-primary names its incarnation") };
    let carrier = estate.carrier().await;
    let reply = rdm_client::NodeRpc::new(carrier.client()).config_update(&before.node_id, &rdm_transport::RouteChoice::Direct, &birth, config.clone(), &rdm_transport::CallOptions::default()).await;
    assert!(matches!(reply, Ok(StatusReply::ConfigUpdated { ref config, .. }) if config.get("listener.data").map(String::as_str) == Some("127.0.0.1:45007")), "the lone fabric-primary updates its own config: {reply:?}");
    // The kill flushes nothing: the acknowledged write's span is exported before it.
    let written = rdm_test::estate::wait_for("the fabric-primary's own write span is exported", Duration::from_secs(10), || async {
        estate.spans_named(&["rdm.node.record.update.via-properties"]).into_iter()
            .find(|s| s["attributes"]["node_id"] == before.node_id.as_str() && s["attributes"]["outcome"] == "durable")
            .and_then(|s| s["trace_id"].as_str().map(str::to_string))
    })
    .await;
    let fp_dir = estate.bootstrap_data_dir("mesh1");
    estate.kill_bootstrap();
    let restarted = estate.restart_admin_with(&fp_dir, &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    assert_eq!(restarted, fp, "the start on the data dir is at its own row's (path, name)");
    estate.enter(&restarted).await;
    let after = rdm_test::estate::wait_for("the restarted fabric-primary is ready at its (path, name)", Duration::from_secs(120), || async {
        estate.node_views().await.into_iter().find(|n| n.location() == fp && n.node_id != before.node_id && n.status == rdm_client::NodeStatus::Ready)
    })
    .await;
    estate.artifact("lone-fabric-primary-restart.json", &serde_json::json!({"before": before, "config": config, "after": after}));
    estate.stop().await;
    assert_eq!(after.config, config, "the restarted lone fabric-primary's config is the one it changed before its loss: {after:?}");
    let spans = estate.spans();
    let own = rdm_test::estate::named(&spans, "rdm.node.record.update.via-properties").into_iter()
        .find(|s| s["attributes"]["node_id"] == after.node_id.as_str())
        .unwrap_or_else(|| panic!("the restarted fabric-primary {} wrote its own part", after.node_id));
    estate.record_trace_url(&written);
    estate.record_trace_url(own["trace_id"].as_str().unwrap());
}
