//! The join of a stopped node after its rename (R-JN1; lifecycles/node-start.md step 2,
//! lifecycles/node-create.md step 4).
//!
//! CONTRACT: a node born `mesh1.rpc.1` is stopped, and while it is stopped its row is renamed
//! `mesh1.rpc.1.old` with a config. Its start presents the name it holds, the one its authority
//! last accepted for this exact birth; the authority admits it under the row's name, installs the
//! canonical digest and hands the row's name and config in the join answer, and the node publishes
//! `mesh1.rpc.1.old` with that config from its first digest after the join on, and its app answers
//! every call under that name (the proof store's provenance names `mesh1.rpc.1.old`: a node's name
//! is the one it holds, R12). What must NOT happen: the start refused `JoinMismatch("name")`, a
//! digest of the started node naming `mesh1.rpc.1`, one missing the row's config, or an app reply
//! naming the birth's frozen `mesh1.rpc.1`.
//!
//! Its own process: the cell holds process-wide state (the parking flag).

use crate::common::admin_side_serving;
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rafka_node_admin_core::model::NodeStatus;
use rafka_node_admin_core::storage::NodeRow;
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::node;
use rafka_node_rpc_testkit::proof_store::{self, ProofRequest, ProofStore};
use std::time::Duration;

async fn call(admin: &crate::common::AdminSide, target: &NodeTarget, req: &StatusRequest) -> StatusReply {
    let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
    match admin.observer.client.call::<Status>(target, req, &opts).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} did not end in a reply: {other:?}", req.op()),
    }
}

#[test]
fn a_node_renamed_while_stopped_starts_under_its_rows_name_and_config() {
    let cap = crate::common::capture("a_node_renamed_while_stopped_starts_under_its_rows_name_and_config");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_node_renamed_while_stopped_starts_under_its_rows_name_and_config") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let dir = std::env::temp_dir().join(format!("rejoin-name-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let born: PathName = "mesh1.rpc.1".parse().unwrap();
        let old: PathName = "mesh1.rpc.1.old".parse().unwrap();
        let launch = Launch {
            fabric: "fabric1".into(),
            fabric_id: fabric,
            name: born.clone(),
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
            mesh_issuer: None,
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        };
        admin.deployed(&launch, &key);
        let store = std::sync::Arc::new(proof_store::FileProofStore::open(&dir).unwrap());
        let _running = node::start_with_seams(&launch, |b, seams| proof_store::serve(b, store, &launch, &seams.name)).await.expect("the node comes up");

        let book = &admin.observer.membership.book;
        let mut digests = book.digest_changes();
        while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic) {
            digests.changed().await.unwrap();
        }
        let (d, _) = book.get(launch.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "evnhx9g6n82z".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "evnhx9g6n82z".into(), attempt: 2, operation: "node.start:mesh1.rpc.1.old".into() };
        assert!(matches!(call(&admin, &target, &stop).await, StatusReply::Stopped { .. }));
        let seq_at_stop = book.get(launch.node_id.as_str()).unwrap().0.digest_seq;

        // While it is stopped the mesh-primary renames its row: the node is sent nothing. Its
        // authority holds the birth under the row's name, with the name it last accepted.
        let config: std::collections::BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
        admin.put_row(NodeRow { name: old.clone(), node_id: node_id.clone(), status: NodeStatus::Leaving, config: config.clone(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None });
        admin.deployed_as(&launch, &key, old.clone(), Some(born.clone()));

        let started = call(&admin, &target, &start).await;
        assert_eq!(started, StatusReply::Started, "the start of a node renamed while stopped is admitted under its row's name");
        digests.borrow_and_update();
        while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.digest_seq <= seq_at_stop || d.status != MemberStatus::ReadyForTraffic) {
            let (d, _) = book.get(launch.node_id.as_str()).unwrap();
            if d.digest_seq > seq_at_stop {
                assert_eq!((&d.node.name, &d.config), (&old, &config), "every digest after the join names the row's name and carries its config");
            }
            digests.changed().await.unwrap();
        }
        let (d, _) = book.get(launch.node_id.as_str()).unwrap();
        assert_eq!((&d.node.name, &d.config, &d.node.incarnation), (&old, &config, &incarnation), "the same birth publishes the row's name and config");
        // Its app answers under the name it holds: the proof store's provenance is the row's name.
        let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let got = match admin.observer.client.call::<ProofStore>(&target, &ProofRequest::Get { key: b"41".to_vec() }, &opts).await.0 {
            RpcOutcome::Reply(r) => r.into_value(),
            other => panic!("the proof store get did not end in a reply: {other:?}"),
        };
        let at = got.provenance().unwrap_or_else(|| panic!("the store's own reply carries its provenance: {got:?}"));
        assert_eq!((at.node.as_str(), at.node_id.as_str(), at.incarnation_id.as_str()), (old.to_string().as_str(), node_id.as_str(), incarnation.as_str()), "the app answers under the row's name it holds: {got:?}");
        // Every span of the started node after its join names the row's name it holds, never its boot name (R12, crit:e12.s47.2).
        let join_seq = {
            let held_spans = spans.0.lock().unwrap();
            *held_spans
                .iter()
                .find(|(_, (n, _, f))| n == "rdm.mesh.node.update.via-join-admitted" && f.get("installed").map(String::as_str) == Some(old.to_string().as_str()))
                .expect("the start's join installed the row's name")
                .0
        };
        let join_closed = spans.3.lock().unwrap()[&join_seq];
        let held_spans = spans.0.lock().unwrap();
        let admin_name = admin.launcher.name.to_string();
        let after_join: Vec<(u64, String, String)> = held_spans
            .iter()
            .filter_map(|(&seq, (n, _, f))| {
                let node = f.get("node")?;
                (seq > join_closed && node != &admin_name).then(|| (seq, n.clone(), node.clone()))
            })
            .collect();
        assert!(!after_join.is_empty(), "the started node emitted spans after its join");
        for (seq, span, node) in &after_join {
            assert_eq!(node, &old.to_string(), "span #{seq} ({span}) after the join must name the row's name {old}, never the boot name {born}: {after_join:?}");
        }
        let start_nodes: Vec<String> = held_spans
            .values()
            .filter(|(n, _, _)| n == "rdm.node_admin.status.update.via-start-node")
            .map(|(_, _, f)| f["node"].clone())
            .collect();
        assert_eq!(start_nodes, vec![old.to_string()], "rdm.node_admin.status.update.via-start-node names the row's name the node holds after the join");
        drop(held_spans);
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT: a node born `mesh1.rpc.1.new` takes `mesh1.rpc.1` live (its `node.connections.delete
/// { All }` completed, then `ApplyNodeName` answered `Applied`; lifecycles/node-name-update.md "Rename precondition") and is then stopped: its authority's held stopped digest names `mesh1.rpc.1`, and so
/// does its row. Its start presents the name it holds, `mesh1.rpc.1`, and is admitted; its stop and
/// start spans name `mesh1.rpc.1`. What must NOT happen: the join presenting the birth's frozen
/// `.new`, refused `JoinMismatch("name")`, or a lifecycle span after the rename naming `.new`.
#[test]
fn a_node_renamed_live_then_stopped_presents_the_name_it_holds_at_its_start() {
    let cap = crate::common::capture("a_node_renamed_live_then_stopped_presents_the_name_it_holds_at_its_start");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), "a_node_renamed_live_then_stopped_presents_the_name_it_holds_at_its_start") {
            return;
        }
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let dir = std::env::temp_dir().join(format!("rejoin-held-name-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let born: PathName = "mesh1.rpc.1.new".parse().unwrap();
        let taken: PathName = "mesh1.rpc.1".parse().unwrap();
        let launch = Launch {
            fabric: "fabric1".into(),
            fabric_id: fabric,
            name: born.clone(),
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
            mesh_issuer: None,
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        };
        admin.deployed(&launch, &key);
        let _running = node::start(&launch, |b, _| b).await.expect("the node comes up");

        let book = &admin.observer.membership.book;
        let mut digests = book.digest_changes();
        while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic) {
            digests.changed().await.unwrap();
        }
        let (d, _) = book.get(launch.node_id.as_str()).unwrap();
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let (node_id, incarnation) = (launch.node_id.clone(), launch.incarnation.clone());
        let delete = StatusRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
        assert!(matches!(call(&admin, &target, &delete).await, StatusReply::ConnectionsDeleted { held, .. } if held.is_empty()), "every connection is deleted");
        let apply = StatusRequest::ApplyNodeName { node_id: node_id.clone(), incarnation: incarnation.clone(), name: taken.to_string() };
        assert_eq!(call(&admin, &target, &apply).await, StatusReply::Applied);
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "ggv7fthkf13d".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "ggv7fthkf13d".into(), attempt: 2, operation: "node.start:mesh1.rpc.1".into() };
        assert!(matches!(call(&admin, &target, &stop).await, StatusReply::Stopped { .. }));
        let seq_at_stop = book.get(launch.node_id.as_str()).unwrap().0.digest_seq;

        // The authority holds the birth under its row's name, and its held stopped digest names it too.
        admin.put_row(NodeRow { name: taken.clone(), node_id: node_id.clone(), status: NodeStatus::Leaving, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None });
        admin.deployed_as(&launch, &key, taken.clone(), Some(taken.clone()));

        assert_eq!(call(&admin, &target, &start).await, StatusReply::Started, "the start presents the name the node holds");
        digests.borrow_and_update();
        while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.digest_seq <= seq_at_stop || d.status != MemberStatus::ReadyForTraffic) {
            digests.changed().await.unwrap();
        }
        assert_eq!(book.get(launch.node_id.as_str()).unwrap().0.node.name, taken, "the started node publishes the name it holds");
        // Its lifecycle spans after the rename name the name it holds, never its birth's (R12).
        for span in ["rdm.node_admin.status.update.via-stop-node", "rdm.node_admin.status.update.via-start-node"] {
            let named: Vec<String> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == span).map(|(_, _, f)| f["node"].clone()).collect();
            assert_eq!(named, vec![taken.to_string()], "{span} names the name the node holds");
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}
