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
/// CONTRACT (node-config-update.md step 1; Q197: nothing is decided on gossip; D21): a running node puts
/// its own config with `RunningNode::config_update`, naming no fabric-primary: the call goes to the
/// node-admin it joined through (its launcher, the birth its join answered from, never one a digest
/// names), owed until that admin answers, sent by the node itself (the endpoint the transport proved is
/// the node's), naming the node's own exact birth and the whole map; the admin's answer is handed back
/// as it is. Must NOT happen: the call sent to any other admin, or naming another birth.
#[test]
fn a_running_nodes_own_config_update_goes_to_the_node_admin_it_joined_through_and_its_answer_is_handed_back() {
    let cap = crate::common::capture("a_running_nodes_own_config_update_goes_to_the_node_admin_it_joined_through_and_its_answer_is_handed_back");
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
        let through = running.joined_admin().expect("the node holds the admin it joined through");
        assert_eq!((through.name.to_string(), through.target.clone()), (admin.launcher.name.to_string(), rdm_transport::NodeTarget::ExactNode(admin.launcher.node_id.clone())), "the node joined through its launcher");

        let config: BTreeMap<String, String> = [("listener.data".to_string(), "127.0.0.1:43003".to_string())].into();
        let reply = tokio::time::timeout(Duration::from_secs(20), running.config_update(config.clone())).await.expect("the owed call is answered").expect("a typed reply");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone() }, "the admin's answer is handed back");
        let sent = admin.config_updates.lock().unwrap().clone();
        assert_eq!(
            sent,
            vec![(key.public(), StatusRequest::UpdateNodeConfig { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), config })],
            "one node.config.update, sent by the node itself to the admin it joined through, naming its own birth and the whole map"
        );

        running.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    let calls: Vec<_> = cap.spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.node.config.update.via-call").map(|(_, _, f)| f.clone()).collect();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].get("outcome").map(String::as_str), Some("config-updated"), "{calls:?}");
}
