//! A node answers anyone who reaches it: nothing inside the mesh checks who is calling
//! (node-rpc.md §14; node-rpc-envelope.md "Caller execution and certainty": "A stopped endpoint
//! answers anyone who reaches it: the mesh adds no generic caller check or authorship validation").
//!
//! CONTRACT: a stopped node is commanded by an admin it never heard of (the admin created after its
//! predecessor's loss, whose digest a stopped node cannot hear). Its `stop-node` naming the exact birth
//! is answered `Stopped` with the receipt that stopped it, and its probe `CurrentStopped`: the target is
//! the op and the exact birth the request names, never the caller. A command naming another birth
//! of the node is `RejectedStaleIncarnation`; one naming another node id with this birth's
//! incarnation is answered by the birth it reached (R-UC1a, R-RX5: no receiver refuses a call for
//! the node id it names). What must NOT happen: a refusal that names the caller (`SenderNotSubject`,
//! or a `ReceiverNotPrimary` decided by who sent it or by the node id the call names). The node's
//! decision span names the caller it holds no digest of by its endpoint.
//!
//! Its own process: the cell holds process-wide state (the stop flag).

use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{ConnectionsReceipt, Status, StatusReply, StatusRequest};
use rdm_test::node;
use std::sync::Arc;
use std::time::Duration;

async fn call(client: &NodeRpcClient, target: &NodeTarget, req: &StatusRequest) -> StatusReply {
    let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
    if let Some(reply) = crate::common::lifecycle_reply(client, target, req, &opts, None).await { return reply; }
    match client.call::<Status>(target, req, &opts).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} did not end in a reply: {other:?}", req.op()),
    }
}

#[test]
fn a_stopped_node_answers_a_stop_and_a_probe_from_an_admin_it_never_heard_of() {
    let cap = crate::common::capture("a_stopped_node_answers_a_stop_and_a_probe_from_an_admin_it_never_heard_of");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_stopped_node_answers_a_stop_and_a_probe_from_an_admin_it_never_heard_of") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = crate::common::admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let dir = std::env::temp_dir().join(format!("no-caller-check-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = Launch {
            fabric: "fabric1".into(),
            fabric_id: fabric,
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
            mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        admin.deployed(&launch, &key);
        let _running = node::start(&launch, |b, _| b).await.expect("the node comes up");

        let digest_view = &admin.observer.topology_view.digest_view;
        let mut heard = digest_view.heard_changes();
        while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic) {
            heard.changed().await.unwrap();
        }
        let (born, _) = digest_view.get(launch.node_id.as_str()).unwrap();
        let resolved = ResolvedNode {
            node_id: born.node.node_id.clone(),
            name: born.node.name.clone(),
            endpoint_id: born.node.endpoint_id.0.parse().unwrap(),
            incarnation: born.node.incarnation.clone(),
            transport_addr: born.node.transport_addr,
        };
        admin.observer.resolver.insert(resolved.clone());
        let target = NodeTarget::ExactNode(born.node.node_id.clone());
        let (node_id, incarnation) = (born.node.node_id.clone(), born.node.incarnation.clone());
        let stop = |attempt: u32, incarnation: &IncarnationId| StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "k8wmccyepcgk".into(), attempt, operation: "node.stop:mesh1.rpc.1".into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };

        // The admin the node knows stops it: the node stops, and hears no digest from now on.
        let stopped = call(&admin.observer.client, &target, &stop(1, &incarnation)).await;
        assert!(matches!(stopped, StatusReply::Stopped { receipt: ConnectionsReceipt::Established { .. } }), "{stopped:?}");

        // An admin the stopped node never heard of: a fresh endpoint key, in no digest the node holds.
        let stranger_key = iroh::SecretKey::generate();
        let stranger_endpoint = stranger_key.public().to_string();
        assert!(digest_view.all().iter().all(|d| d.node.endpoint_id.0 != stranger_key.public().to_string()), "the stranger is in no digest");
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(resolved);
        let stranger = NodeRpcClient::new(rdm_transport::endpoint::bind(stranger_key, "127.0.0.1:0".parse().unwrap()).await.unwrap(), resolver).with_caller_system("rdm");

        // Its stop naming the exact birth is answered as the known admin's was: stopping a stopped birth is done.
        assert_eq!(call(&stranger, &target, &stop(2, &incarnation)).await, stopped, "a stopped endpoint answers anyone who reaches it");
        // Its probe is answered for the subject: stopped.
        assert!(matches!(call(&stranger, &target, &probe).await, StatusReply::CurrentStopped { .. }), "the probe is decided for the subject, not the caller");

        // The exact birth the request names is checked: another birth of the node is refused naming the one held.
        let stale = IncarnationId::mint();
        assert_eq!(call(&stranger, &target, &stop(3, &stale)).await, StatusReply::RejectedStaleIncarnation { held: incarnation.clone() });
        // The node id the request names is never a reason to refuse: the birth the call reached answers it.
        let another = StatusRequest::StopNode { node_id: NodeId::mint(), incarnation: incarnation.clone(), build_id: "k8wmccyepcgk".into(), attempt: 4, operation: "node.stop:mesh1.rpc.9".into() };
        let reply = call(&stranger, &NodeTarget::ExactNode(node_id.clone()), &another).await;
        assert_eq!(reply, stopped, "a stop naming another node id is answered by the birth it reached, as the stop of a stopped birth: {reply:?}");

        // The node's own decision span names the stop it answered the stranger, and names the
        // stranger by the one fact the node holds of it: its endpoint.
        tokio::time::timeout(Duration::from_secs(10), spans.recorded("rdm.node_admin.status.update.via-command", |f| f.get("op").is_some_and(|o| o == "stop-node") && f.get("outcome").is_some_and(|o| o == "node-stopped") && f.get("sender").is_some_and(|s| s.contains(&stranger_endpoint))))
            .await
            .expect("the node decided the stranger's node.stop: Stopped, naming the sender it holds no digest of by its endpoint");
        let _ = std::fs::remove_dir_all(&dir);
    })
}
