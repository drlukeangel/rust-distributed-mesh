//! A live node takes the name its mesh-primary's row now holds (`ApplyNodeName`; node-rpc-envelope.md
//! "Meta and config", R12/R13, R-CR1).
//!
//! CONTRACT: a node born `mesh1.rpc.1.new` answers `ApplyNodeName { name: mesh1.rpc.1 }` with
//! `Busy` naming the route it holds to its admin (its join), and holds nothing: its digest still
//! says `.new`. Drained (one call in flight at the node holds the drain open until the cell
//! releases it, so the drain's completion comes after `DrainNode` is answered) and its connections
//! deleted (`DeleteConnections { All }`), it answers `Applied` and holds `mesh1.rpc.1`; the same
//! name again is `AlreadyApplied`, and a stale incarnation is refused naming the one held. What
//! must NOT happen: a name taken while the node holds a connection, or a digest naming the new name
//! before the apply.

use crate::common::admin_side_serving;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rafka_node_admin_core::node_commands::CommandKey;
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{ConnectionScope, Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::hydrate_probe::{HydratePull, PullRequest, PullServer};
use rafka_node_rpc_testkit::node;
use std::time::Duration;

#[test]
fn a_live_node_takes_its_rows_name_only_once_its_connections_are_deleted() {
    let cap = crate::common::capture("a_live_node_takes_its_rows_name_only_once_its_connections_are_deleted");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let dir = std::env::temp_dir().join(format!("apply-node-name-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
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
        };
        admin.deployed(&launch, &key);
        // The node serves one op the cell can hold: a call into it stays in flight until released.
        let pull = PullServer::new(vec![]);
        pull.gate.open();
        let running = node::start(&launch, {
            let pull = pull.clone();
            move |b, _| pull.serve(b)
        })
        .await
        .expect("the node comes up");

        let book = &admin.observer.membership.book;
        // Every digest the admin's book takes ticks this count: the cell awaits a published digest, never the cadence.
        let mut digests = book.digest_changes();
        let mut heard = book.heard_changes();
        while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic) {
            heard.changed().await.unwrap();
        }
        let (d, _) = book.get(launch.node_id.as_str()).unwrap();
        assert_eq!(d.node.name, born);
        admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
        let client = &admin.observer.client;
        let call = |req: StatusRequest| {
            let (target, opts) = (target.clone(), opts.clone());
            async move {
                match client.call::<Status>(&target, &req, &opts).await.0 {
                    RpcOutcome::Reply(r) => r.into_value(),
                    other => panic!("{} did not end in a reply: {other:?}", req.op()),
                }
            }
        };
        let apply = StatusRequest::ApplyNodeName { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), name: taken.to_string() };

        // Holding its join route: the name is refused Busy, naming that route, and nothing is held.
        match call(apply.clone()).await {
            StatusReply::Busy { reason } => assert!(reason.contains("mesh1.admin.1 (direct, entry "), "Busy names the route the node holds: {reason}"),
            other => panic!("a node holding a connection answered {other:?}"),
        }
        let seq = book.get(launch.node_id.as_str()).unwrap().0.digest_seq;
        digests.borrow_and_update();
        while book.get(launch.node_id.as_str()).unwrap().0.digest_seq == seq {
            digests.changed().await.unwrap();
        }
        assert_eq!(book.get(launch.node_id.as_str()).unwrap().0.node.name, born, "a Busy node keeps publishing its birth's name");

        // Drained, and every connection deleted: the name moves. One call in flight at the node holds
        // the drain open, so its completion (`node-drained`, called at this admin side) comes after
        // `DrainNode` is answered.
        let operation = format!("drain-node:{born}");
        let mut drained = admin.observer.commands.open(CommandKey { build_id: "bld_apply".into(), attempt: 1, operation: operation.clone(), node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone() });
        pull.hold();
        let held_call = async { client.call::<HydratePull>(&target, &PullRequest::Pull, &opts).await.0 };
        let steps = async {
            pull.entered(1).await;
            let drain = StatusRequest::DrainNode { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), build_id: "bld_apply".into(), attempt: 1, operation: operation.clone() };
            assert_eq!(call(drain).await, StatusReply::Applied);
            let delete = StatusRequest::DeleteConnections { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), scope: ConnectionScope::All };
            assert!(matches!(call(delete).await, StatusReply::ConnectionsDeleted { held, .. } if held.is_empty()), "every connection is deleted");
            pull.release();
        };
        let (held_out, ()) = tokio::join!(held_call, steps);
        assert!(matches!(held_out, RpcOutcome::Reply(_)), "the held call answers once released: {held_out:?}");
        tokio::time::timeout(Duration::from_secs(20), drained.wait_for(|done| *done))
            .await
            .expect("the drained birth called node-drained at its commander")
            .expect("the command book is held by the admin side");
        assert_eq!(call(apply.clone()).await, StatusReply::Applied);
        assert_eq!(running.membership.held_name(&born), taken, "the node holds the applied name");

        // The same name again is done; a stale incarnation is refused naming the one held.
        assert_eq!(call(apply.clone()).await, StatusReply::AlreadyApplied);
        let stale = StatusRequest::ApplyNodeName { node_id: launch.node_id.clone(), incarnation: IncarnationId::mint(), name: taken.to_string() };
        assert_eq!(call(stale).await, StatusReply::RejectedStaleIncarnation { held: launch.incarnation.clone() });
        let _ = std::fs::remove_dir_all(&dir);
    })
}
