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
/// CONTRACT (node-config-update.md, a node's own update, steps 1-2; Q235 P1, P2, P4): a running node puts
/// its own config with `RunningNode::config_update`: it holds the map at once and sends it, owed until
/// answered, to the mesh-primary its topology view names (here its launcher, the admin side), sent by the
/// node itself, naming its own exact birth and the whole map. The answer, `ConfigUpdated` carrying the
/// row as written at revision 1, is handed back and installed: the node holds the map at revision 1, and
/// no `ApplyNodeConfig` is needed. An `ApplyNodeConfig` delivered after it at an older or the same
/// revision is answered `Applied` and installs nothing; one at a newer revision installs its map. Must
/// NOT happen: the call sent to an admin the view does not name the mesh-primary, the map held only after
/// a round trip, or an older map installed over a newer one.
#[test]
fn a_running_nodes_own_config_update_goes_to_its_mesh_primary_is_held_at_once_and_installs_the_row_by_revision() {
    let cap = crate::common::capture("a_running_nodes_own_config_update_goes_to_its_mesh_primary_is_held_at_once_and_installs_the_row_by_revision");
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("node-own-config-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let fabric = FabricId::mint();
        let admin = admin_side("127.0.0.1".parse().unwrap(), &fabric).await;
        let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: fabric.clone(),
            name: "mesh1.rpc.1".parse().unwrap(),
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
        let update = running.config_update(config.clone());
        let reply = tokio::time::timeout(Duration::from_secs(20), update).await.expect("the owed call is answered").expect("a typed reply");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), config: config.clone(), revision: 1 }, "the mesh-primary's answer is handed back, the row as written");
        let sent = admin.config_updates.lock().unwrap().clone();
        assert_eq!(
            sent,
            vec![(key.public(), StatusRequest::UpdateNodeConfig { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), config: config.clone() })],
            "one node.config.update, sent by the node itself to its mesh-primary, naming its own birth and the whole map"
        );
        assert_eq!((running.topology_view.config(), running.topology_view.config_revision()), (Some(config.clone()), 1), "the node holds the row's map at its revision");

        // An apply delivered after the answer, at an older or the same revision, installs nothing; a newer one does.
        let addr = running.topology_view.endpoint().bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("the node's address");
        admin.observer.resolver.insert(rdm_transport::ResolvedNode { node_id: launch.node_id.clone(), name: launch.name.clone(), endpoint_id: key.public(), incarnation: launch.incarnation.clone(), transport_addr: addr });
        let target = rdm_transport::NodeTarget::ExactNode(launch.node_id.clone());
        let apply = |config: BTreeMap<String, String>, revision: u64| StatusRequest::ApplyNodeConfig { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), config, revision };
        let stale: BTreeMap<String, String> = [("listener.data".to_string(), "127.0.0.1:1".to_string())].into();
        for revision in [0, 1] {
            let (out, _) = admin.observer.client.call::<rdm_core::status::Status>(&target, &apply(stale.clone(), revision), &rdm_transport::CallOptions::default()).await;
            assert!(matches!(out, rdm_core::outcome::RpcOutcome::Reply(ref r) if *r.value() == StatusReply::Applied), "{out:?}");
            assert_eq!(running.topology_view.config(), Some(config.clone()), "an apply at revision {revision} leaves the newer map");
        }
        let newer: BTreeMap<String, String> = [("listener.data".to_string(), "127.0.0.1:43004".to_string())].into();
        let (out, _) = admin.observer.client.call::<rdm_core::status::Status>(&target, &apply(newer.clone(), 4), &rdm_transport::CallOptions::default()).await;
        assert!(matches!(out, rdm_core::outcome::RpcOutcome::Reply(ref r) if *r.value() == StatusReply::Applied), "{out:?}");
        assert_eq!((running.topology_view.config(), running.topology_view.config_revision()), (Some(newer), 4), "a newer revision installs its map");

        running.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    let calls: Vec<_> = cap.spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.config.update.via-call").map(|(_, _, f)| f.clone()).collect();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].get("outcome").map(String::as_str), Some("config-updated"), "{calls:?}");
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
async fn config_at(estate: &rdm_test::estate::Estate, admin: &str, node: &str) -> Option<BTreeMap<String, String>> {
    let views = estate.node_views_at(&admin.parse().unwrap()).await?;
    views.into_iter().find(|v| v.name.to_string() == node).map(|v| v.config)
}

// @feature: node-lifecycle
/// CONTRACT (node-config-update.md, a node's own update, steps 1-3; Q235 P1-P3), on real node processes:
/// mesh2.rpc.1 puts its own config (`RunningNode::config_update`, through the testkit originate door).
/// Its mesh-primary, mesh2.admin.1, which is not the fabric-primary (mesh1.admin.1), writes the row and
/// its atlas and answers `config-updated` with the row's revision: `node.get` at mesh2.admin.1 answers
/// the map at once. It owes the row up; the fabric-primary's atlas takes it and `node.get` there
/// answers it. Spans: mesh2.admin.1's `via-config` (apply=own, push=owed) ends before mesh1.admin.1's
/// `via-config-pushed` (atlas installed) begins; mesh2.admin.1's `via-config-push` is `applied`;
/// mesh2.rpc.1 is sent no `ApplyNodeConfig` (no `via-apply-node-config` span of it). Must NOT happen:
/// the update going to the fabric-primary first (`via-fabric-primary`), or the fabric-primary's atlas
/// holding the map before the mesh-primary's row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nodes_own_config_update_is_on_its_mesh_primarys_row_before_the_fabric_primarys_atlas() {
    let cell = "a_nodes_own_config_update_is_on_its_mesh_primarys_row_before_the_fabric_primarys_atlas";
    let mut estate = rdm_test::estate::Estate::bootstrap(estate_owner(cell), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 1, 1)]).await;
    estate.settled_views(&[("mesh1", 1, 0), ("mesh2", 1, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let out = estate.probe(&["own-config", "--target", "path:mesh2.rpc.1", "--value", "listener.data=127.0.0.1:45005"]);
    assert_eq!(out["own_config"], "config-updated", "mesh2's primary answers the node's own update: {out}");
    let revision = out["revision"].as_u64().unwrap_or(0);
    assert!(revision > 0, "the answer carries the row's revision: {out}");
    let config: BTreeMap<String, String> = [("listener.data".to_string(), "127.0.0.1:45005".to_string())].into();
    assert_eq!(config_at(&estate, "mesh2.admin.1", "mesh2.rpc.1").await, Some(config.clone()), "mesh2's primary answers the map at once, from its atlas");
    rdm_test::estate::wait_for("the fabric-primary answers the pushed map", Duration::from_secs(30), || async {
        (config_at(&estate, "mesh1.admin.1", "mesh2.rpc.1").await.as_ref() == Some(&config)).then_some(())
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &serde_json::Value, k: &str| sp["attributes"][k].as_str().map(str::to_string).unwrap_or_else(|| sp["attributes"][k].to_string());
    let written = rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-config").into_iter().find(|sp| attr(sp, "node") == "mesh2.admin.1" && attr(sp, "subject") == "mesh2.rpc.1" && attr(sp, "apply") == "own").unwrap_or_else(|| panic!("mesh2.admin.1's via-config of mesh2.rpc.1, apply=own"));
    assert_eq!((attr(written, "push"), attr(written, "outcome")), ("owed".to_string(), "config-updated".to_string()), "{written}");
    let pushed = rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-config-pushed").into_iter().find(|sp| attr(sp, "node") == "mesh1.admin.1" && attr(sp, "subject") == "mesh2.rpc.1" && attr(sp, "atlas") == "installed").unwrap_or_else(|| panic!("mesh1.admin.1's via-config-pushed of mesh2.rpc.1, installed"));
    let push = rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-config-push").into_iter().find(|sp| attr(sp, "node") == "mesh2.admin.1" && attr(sp, "subject") == "mesh2.rpc.1").unwrap_or_else(|| panic!("mesh2.admin.1's via-config-push"));
    assert_eq!((attr(push, "fabric_primary"), attr(push, "outcome")), ("mesh1.admin.1".to_string(), "applied".to_string()), "{push}");
    // The causal chain: the push is owed from the row write, and the fabric-primary's install is the push's call.
    assert!(rdm_test::estate::descends_from(&spans, push, written), "the push is owed by the row write: {push}");
    assert!(rdm_test::estate::descends_from(&spans, pushed, push), "the fabric-primary's atlas takes the row from the push: {pushed}");
    assert!(rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-apply-node-config").iter().all(|sp| attr(sp, "node") != "mesh2.rpc.1"), "the node is sent no ApplyNodeConfig");
    assert!(rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-fabric-primary").is_empty(), "nothing goes to the fabric-primary first");
    for (what, sp) in [("written", written), ("push", push), ("pushed", pushed)] {
        eprintln!("{what}: trace {} {} node={} outcome={}", sp["trace_id"].as_str().unwrap_or_default(), sp["name"].as_str().unwrap_or_default(), attr(sp, "node"), attr(sp, "outcome"));
    }
    estate.record_trace_url(written["trace_id"].as_str().unwrap_or_default());
}
