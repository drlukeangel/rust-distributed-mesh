//! A live node takes the name its mesh-primary's row now holds (`ApplyNodeName`; node-rpc-envelope.md
//! "Meta and config", R12/R13, lifecycles/node-replace.md step 4).
//!
//! CONTRACT: a node born `mesh1.rpc.1.new` answers `ApplyNodeName { name: mesh1.rpc.1 }` with
//! `Busy` naming its in-flight count (the apply call itself not counted) while a call is in flight,
//! and holds nothing: its digest still says `.new`. With nothing in flight it answers `Applied`, and
//! every digest it publishes from then on carries `mesh1.rpc.1`; the same name again is
//! `AlreadyApplied`, and a stale incarnation is refused naming the one held. What must NOT happen: a
//! name taken while traffic is in flight, a digest naming the new name before the apply, or a later
//! cadence digest reverting to the birth's name.

use crate::common::admin_side_serving;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, PathName};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::hydrate_probe::{HydratePull, PullRequest, PullServer};
use rafka_node_rpc_testkit::node;
use std::time::Duration;

#[test]
fn a_live_node_takes_its_rows_name_only_with_nothing_in_flight_and_publishes_it_from_then_on() {
    let cap = crate::common::capture("a_live_node_takes_its_rows_name_only_with_nothing_in_flight_and_publishes_it_from_then_on");
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
            mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
            mesh_issuer: None,
        };
        admin.deployed(&launch, &key);
        // The node serves one held op: a call into it stays in flight until the cell releases it.
        let pull = PullServer::new(vec![]);
        pull.gate.open();
        pull.hold();
        let _running = node::start(&launch, {
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

        // One call in flight: the name is refused Busy, naming that one call, and nothing is held.
        let held = async { client.call::<HydratePull>(&target, &PullRequest::Pull, &opts).await.0 };
        let busy = async {
            pull.entered(1).await;
            match call(apply.clone()).await {
                StatusReply::Busy { reason } => assert!(reason.contains("1 call in flight"), "Busy names the one call in flight, not the apply itself: {reason}"),
                other => panic!("a node with a call in flight answered {other:?}"),
            }
            let seq = book.get(launch.node_id.as_str()).unwrap().0.digest_seq;
            digests.borrow_and_update();
            while book.get(launch.node_id.as_str()).unwrap().0.digest_seq == seq {
                digests.changed().await.unwrap();
            }
            assert_eq!(book.get(launch.node_id.as_str()).unwrap().0.node.name, born, "a Busy node keeps publishing its birth's name");
            // Nothing in flight from here: the held call answers once released.
            pull.release();
        };
        let (out, ()) = tokio::join!(held, busy);
        assert!(matches!(out, RpcOutcome::Reply(_)), "the held call answers once released: {out:?}");

        assert_eq!(call(apply.clone()).await, StatusReply::Applied);
        // The node published the name before it answered; the admin hears that digest.
        digests.borrow_and_update();
        while book.get(launch.node_id.as_str()).unwrap().0.node.name != taken {
            digests.changed().await.unwrap();
        }
        let seq = book.get(launch.node_id.as_str()).unwrap().0.digest_seq;
        while book.get(launch.node_id.as_str()).unwrap().0.digest_seq <= seq + 1 {
            digests.changed().await.unwrap();
        }
        assert_eq!(book.get(launch.node_id.as_str()).unwrap().0.node.name, taken, "later cadence digests keep the applied name");

        // The same name again is done; a stale incarnation is refused naming the one held.
        assert_eq!(call(apply.clone()).await, StatusReply::AlreadyApplied);
        let stale = StatusRequest::ApplyNodeName { node_id: launch.node_id.clone(), incarnation: IncarnationId::mint(), name: taken.to_string() };
        assert_eq!(call(stale).await, StatusReply::RejectedStaleIncarnation { held: launch.incarnation.clone() });
        let _ = std::fs::remove_dir_all(&dir);
    })
}
