//! A start after a stop joins back the admin that commanded it (node-start.md step 2: "Join the
//! accepting authority"; the stopped node is reached by a fresh Node RPC dial to its still-bound
//! endpoint, states.md "stopped nodes"). The commander reached the stopped node, so its key is the
//! call's own, proved by the transport (node-rpc.md §14), and the path the call came in on is a path
//! back. Which birth holds that key is the member record the node installed from its mesh-primary's
//! projection (gossip.md §3.1, the authoritative member record; R-HB1): never the heartbeat book,
//! which holds a member only once its own digest reached this node.
//!
//! CONTRACT: an rpc node launched by its mesh's primary is stopped and started by another node-admin
//! of its mesh that has never said anything on the mesh channel, so the node's heartbeat book holds
//! no digest of it, while the primary's member projection, which the node installed, carries it. The
//! stop is answered `Stopped` and the start `Started`: the start joined the commander through the
//! call's key, and the node is the same birth, ReadyForTraffic again. Must NOT happen: a start
//! refused because the commander is not in the heartbeat book, or a start that joins anyone but the
//! admin that commanded it.
//!
//! Its own process: the cell holds process-wide state (the parking flag).

use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{IncarnationId, MemberStatus, MeshId, NodeId};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{DrainReceipt, NodeState, Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::node;
use std::time::Duration;

/// What a step that is immediate on a correct node is given before the cell fails by name.
const NEVER: Duration = Duration::from_secs(20);

async fn call(admin: &crate::common::AdminSide, target: &NodeTarget, req: &StatusRequest) -> StatusReply {
    let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
    match admin.observer.client.call::<Status>(target, req, &opts).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} did not end in a reply: {other:?}", req.op()),
    }
}

#[test]
fn a_stopped_node_whose_heartbeat_book_lacks_its_commander_starts_and_joins_it_through_the_call() {
    let cell = "a_stopped_node_whose_heartbeat_book_lacks_its_commander_starts_and_joins_it_through_the_call";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let fabric = rafka_mesh_entity::FabricId::mint();
        let primary = crate::common::admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let commander = crate::common::admin_side_silent_as("127.0.0.1".parse().unwrap(), &primary, "mesh1.admin.2").await;
        let dir = std::env::temp_dir().join(format!("rejoin-commander-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
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
            seeds: vec![primary.seed.clone()],
            launcher: Some(primary.launcher.clone()),
            data_dir: dir.clone(),
            store_dir: dir.clone(),
            mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
            mesh_issuer: None,
        };
        primary.deployed(&launch, &key);
        let running = node::start(&launch, |b, _| b).await.expect("the node comes up");
        let book = &primary.observer.membership.book;
        let mut heard = book.heard_changes();
        tokio::time::timeout(NEVER, async {
            while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic) {
                heard.changed().await.unwrap();
            }
        })
        .await
        .expect("the node never reached ReadyForTraffic at its primary");

        // From here on the primary carries the commander in the member projection it publishes; the
        // commander itself says nothing on the mesh channel.
        let _carried = primary.carry_in_projection(&commander);
        let commander_key = commander.seed.0.clone();
        let commander_id = commander.launcher.node_id.clone();
        tokio::time::timeout(NEVER, async {
            while running.membership.projected_member_at(&commander_key).is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the node never installed a projection of its primary carrying the commander");
        assert!(running.membership.book.get(commander_id.as_str()).is_none(), "the node's heartbeat book holds the commander: the cell proves nothing");

        // The commander reaches the node by its birth, as the primary's book holds it.
        let (born, _) = book.get(launch.node_id.as_str()).unwrap();
        commander.observer.resolver.insert(ResolvedNode {
            node_id: born.node.node_id.clone(),
            name: born.node.name.clone(),
            endpoint_id: born.node.endpoint_id.0.parse().unwrap(),
            incarnation: born.node.incarnation.clone(),
            transport_addr: born.node.transport_addr,
        });
        let target = NodeTarget::ExactNode(born.node.node_id.clone());
        let (node_id, incarnation) = (born.node.node_id.clone(), born.node.incarnation.clone());
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_rejoin".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_rejoin".into(), attempt: 2, operation: "start-node:mesh1.rpc.1".into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };

        let left = call(&commander, &target, &stop).await;
        assert!(matches!(left, StatusReply::Stopped { receipt: DrainReceipt::Established { .. } }), "the commander's stop is answered Stopped: {left:?}");
        assert!(running.membership.book.get(commander_id.as_str()).is_none(), "the node's heartbeat book came to hold the commander before its start: the cell proves nothing");

        // The commander deployed this birth and awaits its join, as the admin that commands its start.
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        let started = call(&commander, &target, &start).await;
        assert_eq!(started, StatusReply::Started, "the node starts after its stop, joining the admin that commanded it through the call");
        tokio::time::timeout(NEVER, async {
            while !matches!(call(&commander, &target, &probe).await, StatusReply::Current { state: NodeState::ReadyForTraffic, .. }) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the started node never answered ReadyForTraffic");

        // The start's join went to the commander, named by the key of its own call.
        let held = spans.0.lock().unwrap();
        let joins: Vec<&std::collections::BTreeMap<String, String>> = held
            .values()
            .filter(|(name, _, fields)| name == "rdm.mesh.entry.update.via-pull-attempt" && fields.get("node").map(String::as_str) == Some("mesh1.rpc.1") && fields.get("outcome").map(String::as_str) == Some("answered"))
            .map(|(_, _, fields)| fields)
            .collect();
        let short = commander_key.parse::<iroh::PublicKey>().unwrap().fmt_short().to_string();
        assert!(joins.iter().any(|f| f.get("anchor") == Some(&short)), "no answered join of the start named the commander's key {short}: {joins:?}");
        drop(held);
        let _ = std::fs::remove_dir_all(&dir);
    })
}
