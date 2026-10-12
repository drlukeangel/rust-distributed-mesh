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
//! The same lookup names the commander of every command the node serves and of the completion it
//! owes: the drain's `node-drained` call goes back to the admin whose call commanded the drain,
//! reached through that call, and the stop and start spans name that admin.
//!
//! Each cell runs in its own process: the cells hold process-wide state (the parking flag).

use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{IncarnationId, MemberStatus, MeshId, NodeId};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{DrainReceipt, NodeState, Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::node;
use std::collections::BTreeMap;
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

/// The cell's mesh: `primary` launched the rpc node `running` (`mesh1.rpc.1`, ReadyForTraffic at its
/// primary); `commander` (`mesh1.admin.2`) never says anything on the mesh channel, so the node's
/// heartbeat book holds no digest of it, while the primary carries it in the member projection the
/// node installed (`_carried`). The commander reaches the node by its exact birth (`target`).
struct Rig {
    primary: crate::common::AdminSide,
    commander: crate::common::AdminSide,
    _carried: Option<crate::common::Carried>,
    running: node::RunningNode,
    launch: Launch,
    key: iroh::SecretKey,
    dir: std::path::PathBuf,
    target: NodeTarget,
    node_id: NodeId,
    incarnation: IncarnationId,
    commander_key: String,
    commander_id: NodeId,
}

async fn rig(tag: &str) -> Rig {
    rig_carried(tag, true).await
}

/// [`rig`], with the commander carried in the primary's member projection only when `carry` holds:
/// otherwise no record the node holds names the commander, as for an admin born after its stop.
async fn rig_carried(tag: &str, carry: bool) -> Rig {
    let fabric = rafka_mesh_entity::FabricId::mint();
    let primary = crate::common::admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
    let commander = crate::common::admin_side_silent_as("127.0.0.1".parse().unwrap(), &primary, "mesh1.admin.2").await;
    let dir = std::env::temp_dir().join(format!("{tag}-{}", NodeId::mint()));
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

    // From here on the primary carries the commander in the member projection it publishes (when
    // `carry`); the commander itself says nothing on the mesh channel.
    let commander_key = commander.seed.0.clone();
    let commander_id = commander.launcher.node_id.clone();
    let carried = carry.then(|| primary.carry_in_projection(&commander));
    if carry {
        tokio::time::timeout(NEVER, async {
            while running.membership.projected_member_at(&commander_key).is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the node never installed a projection of its primary carrying the commander");
    } else {
        assert!(running.membership.projected_member_at(&commander_key).is_none(), "a projection the node holds names the commander: the cell proves nothing");
    }
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
    Rig { primary, commander, _carried: carried, running, launch, key, dir, target, node_id, incarnation, commander_key, commander_id }
}

/// The fields of every span named `name` the node `node` emitted.
fn spans_of(spans: &crate::common::Spans, name: &str, node: &str) -> Vec<BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, fields)| n == name && fields.get("node").map(String::as_str) == Some(node)).map(|(_, _, fields)| fields.clone()).collect()
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
        let Rig { primary: _primary, commander, _carried, running, launch, key, dir, target, node_id, incarnation, commander_key, commander_id } = rig("rejoin-commander").await;
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
        let joins = spans_of(&spans, "rdm.mesh.entry.update.via-pull-attempt", "mesh1.rpc.1");
        let answered: Vec<&BTreeMap<String, String>> = joins.iter().filter(|f| f.get("outcome").map(String::as_str) == Some("answered")).collect();
        let short = commander_key.parse::<iroh::PublicKey>().unwrap().fmt_short().to_string();
        assert!(answered.iter().any(|f| f.get("anchor") == Some(&short)), "no answered join of the start named the commander's key {short}: {answered:?}");
        // The stop and the start name the admin whose call commanded them.
        for name in ["rdm.node_admin.status.update.via-stop-node", "rdm.node_admin.status.update.via-start-node"] {
            let commanded = spans_of(&spans, name, "mesh1.rpc.1");
            assert!(!commanded.is_empty() && commanded.iter().all(|f| f.get("commander") == Some(&commander_id.to_string())), "{name} names the commander {commander_id}: {commanded:?}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (R-UC1, Luke 2026-10-12, rulings page: "A stopped node told to start joins back over the
/// endpoint and path of the call that commanded it. It holds no record of the commander, and never
/// needs one. The `commander-unrecorded` refusal is an identity check inside a workflow, so it goes.
/// Who answered is learned from the join answer. No new field on `StartNode`."): the rpc node is
/// stopped and started by an admin of its mesh that no record the node holds names (neither its
/// heartbeat book nor any projection it installed: an admin born after its stop). The stop is
/// answered `Stopped` and the start `Started`: the start joined the admin through the call's endpoint
/// and path, and the node is the same birth, ReadyForTraffic again. Must NOT happen: the start
/// refused `commander-unrecorded`.
#[test]
fn a_stopped_node_commanded_by_an_admin_no_record_it_holds_names_starts_and_joins_it_through_the_call() {
    let cell = "a_stopped_node_commanded_by_an_admin_no_record_it_holds_names_starts_and_joins_it_through_the_call";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let Rig { primary: _primary, commander, _carried, running, launch, key, dir, target, node_id, incarnation, commander_id, .. } = rig_carried("rejoin-unrecorded", false).await;
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_unrecorded".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_unrecorded".into(), attempt: 2, operation: "start-node:mesh1.rpc.1".into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        let left = call(&commander, &target, &stop).await;
        assert!(matches!(left, StatusReply::Stopped { .. }), "the commander's stop is answered Stopped: {left:?}");
        assert!(running.membership.book.get(commander_id.as_str()).is_none(), "the node's heartbeat book holds the commander: the cell proves nothing");
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        let started = call(&commander, &target, &start).await;
        assert_eq!(started, StatusReply::Started, "the node starts after its stop, joining the admin that commanded it through the call, with no record of it");
        tokio::time::timeout(NEVER, async {
            while !matches!(call(&commander, &target, &probe).await, StatusReply::Current { state: NodeState::ReadyForTraffic, .. }) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the started node never answered ReadyForTraffic");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (node-start.md step 2: "A stopped node told to start joins back over the endpoint and
/// path of the call that commanded it"; node-rpc-envelope.md "Status": a start's natural key is
/// `(node_id, incarnation, Pending)`, so a later start of the birth joins the one under way): the rpc
/// node is stopped, then commanded to start by an admin of its mesh (`mesh1.admin.3`) that is lost
/// while the start joins it: its join door takes no join, then its endpoint closes, as an executor
/// killed mid-restart. A second admin of the mesh (`mesh1.admin.2`, the successor) commands the start
/// while the first start is still under way. The successor's start is answered `Started`: the start
/// joined the successor over the successor's own call, and the node is the same birth,
/// ReadyForTraffic again. Must NOT happen: the successor's start answering the lost commander's
/// outcome (`StartFailed` at `node.join`, every join attempt aimed at the lost admin), or the
/// successor's call cut before its answer.
#[test]
fn a_start_commanded_again_while_it_joins_a_lost_commander_joins_the_later_commander_and_starts() {
    let cell = "a_start_commanded_again_while_it_joins_a_lost_commander_joins_the_later_commander_and_starts";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let Rig { primary, commander: successor, _carried, running, launch, key, dir, target, node_id, incarnation, commander_key: successor_key, .. } = rig("rejoin-successor").await;
        // The lost commander: an admin of the mesh, carried in the primary's projection, whose join
        // door takes no join.
        let lost = crate::common::admin_side_silent_deaf_as("127.0.0.1".parse().unwrap(), &primary, "mesh1.admin.3").await;
        let _lost_carried = primary.carry_in_projection(&lost);
        let lost_key = lost.seed.0.clone();
        tokio::time::timeout(NEVER, async {
            while running.membership.projected_member_at(&lost_key).is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the node never installed a projection of its primary carrying the lost commander");
        let (born, _) = primary.observer.membership.book.get(node_id.as_str()).unwrap();
        lost.observer.resolver.insert(ResolvedNode {
            node_id: born.node.node_id.clone(),
            name: born.node.name.clone(),
            endpoint_id: born.node.endpoint_id.0.parse().unwrap(),
            incarnation: born.node.incarnation.clone(),
            transport_addr: born.node.transport_addr,
        });
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_successor".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() };
        let start = |attempt: u32| StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_successor".into(), attempt, operation: "start-node:mesh1.rpc.1".into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        let left = call(&lost, &target, &stop).await;
        assert!(matches!(left, StatusReply::Stopped { .. }), "the stop is answered Stopped: {left:?}");

        // The lost commander's start: its join door answers no join, and once the start's first join
        // attempt reached it, its endpoint closes.
        let lost_short = lost_key.parse::<iroh::PublicKey>().unwrap().fmt_short().to_string();
        let first_req = start(2);
        let first = async { lost.observer.client.call::<Status>(&target, &first_req, &CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(60) }, ..CallOptions::default() }).await.0 };
        let rest = async {
            let at_lost = |f: &BTreeMap<String, String>| f.get("node").map(String::as_str) == Some("mesh1.rpc.1") && f.get("anchor") == Some(&lost_short);
            if tokio::time::timeout(NEVER, spans.recorded("rdm.mesh.entry.update.via-pull-attempt", at_lost)).await.is_err() {
                panic!("the start never made a join attempt at the commander that sent it");
            }
            lost.observer.client.endpoint().close().await;

            // The successor commands the start while the first is under way.
            successor.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
            let started = call(&successor, &target, &start(3)).await;
            assert_eq!(started, StatusReply::Started, "the successor's start is answered Started: the start joined the successor over the successor's own call");
            tokio::time::timeout(NEVER, async {
                while !matches!(call(&successor, &target, &probe).await, StatusReply::Current { state: NodeState::ReadyForTraffic, .. }) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("the started node never answered ReadyForTraffic");
            let successor_short = successor_key.parse::<iroh::PublicKey>().unwrap().fmt_short().to_string();
            let joins = spans_of(&spans, "rdm.mesh.entry.update.via-pull-attempt", "mesh1.rpc.1");
            assert!(
                joins.iter().any(|f| f.get("outcome").map(String::as_str) == Some("answered") && f.get("anchor") == Some(&successor_short)),
                "an answered join of the start names the successor's key {successor_short}: {joins:?}"
            );
        };
        let (_lost_answer, ()) = tokio::join!(first, rest);
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (node-drain.md: the drain's completion is the node's `node-drained` call at the admin
/// that commanded it; node-rpc.md §14: the call's endpoint key names that admin; gossip.md §3.1, R-HB1:
/// which birth holds the key is the member projection the node installed, never the heartbeat book):
/// the rpc node is drained by an admin of its mesh that its heartbeat book holds no digest of and its
/// installed projection carries. The drain is answered `Applied`, and its `node-drained` call reaches
/// that admin back through the drain's call and resolves the drain command it holds open (`Applied`).
/// The node's command span names the admin as sender, and its drain and completion spans name it as
/// commander. Must NOT happen: no completion call because the heartbeat book does not hold the
/// commander (`commander-unresolved`), or a span with an empty commander.
#[test]
fn a_node_drained_by_a_commander_its_heartbeat_book_lacks_calls_its_completion_back_through_the_call() {
    let cell = "a_node_drained_by_a_commander_its_heartbeat_book_lacks_calls_its_completion_back_through_the_call";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let Rig { primary: _primary, commander, _carried, running, dir, target, node_id, incarnation, commander_id, .. } = rig("drain-commander").await;
        let operation = "drain-node:mesh1.rpc.1";
        let mut completed = commander.observer.commands.open(rafka_node_admin_core::node_commands::CommandKey { build_id: "bld_drain".into(), attempt: 1, operation: operation.into(), node_id: node_id.clone(), incarnation: incarnation.clone() });
        let drain = StatusRequest::DrainNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_drain".into(), attempt: 1, operation: operation.into() };
        assert_eq!(call(&commander, &target, &drain).await, StatusReply::Applied, "the commander's drain is admitted");
        tokio::time::timeout(NEVER, completed.wait_for(|done| *done)).await.expect("the node's node-drained call never resolved the drain the commander holds open").expect("the command book is held");
        assert!(running.membership.book.get(commander_id.as_str()).is_none(), "the node's heartbeat book came to hold the commander: the cell proves nothing");

        let commander_id = commander_id.to_string();
        let sent = spans_of(&spans, "rdm.node_admin.status.update.via-command", "mesh1.rpc.1");
        assert!(sent.iter().any(|f| f.get("op").map(String::as_str) == Some("drain-node") && f.get("sender").map(String::as_str) == Some("mesh1.admin.2")), "the drain's command span names the commander as sender: {sent:?}");
        let drained = spans_of(&spans, "rdm.node_admin.status.update.via-drain-node", "mesh1.rpc.1");
        assert!(!drained.is_empty() && drained.iter().all(|f| f.get("commander") == Some(&commander_id)), "the drain names its commander: {drained:?}");
        // The node opens its completion span once the commander's reply is back, after the commander
        // resolved its command: it is awaited, never read before it can exist.
        let applied = |f: &BTreeMap<String, String>| f.get("node").map(String::as_str) == Some("mesh1.rpc.1") && f.get("commander") == Some(&commander_id) && f.get("answer").map(String::as_str) == Some("applied");
        if tokio::time::timeout(NEVER, spans.recorded("rdm.node_admin.status.update.via-completion-call", applied)).await.is_err() {
            panic!("the completion call went to the commander and was applied: {:?}", spans_of(&spans, "rdm.node_admin.status.update.via-completion-call", "mesh1.rpc.1"));
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (node-start.md step 2, "Join the accepting authority": the start joins the admin that
/// commanded it through its call; error canon: a refusal names what failed): the node's resolver
/// holds the commander departed (a departed node never returns, so the resolver refuses to hold it at
/// the call's path). The start is answered `StartFailed` at `node.join`, naming the commander, its key
/// and the resolver's refusal. Must NOT happen: the refusal dropped, and the start failing later as a
/// join that reached no one.
#[test]
fn a_start_whose_commander_the_resolver_refuses_is_failed_naming_the_refusal() {
    let cell = "a_start_whose_commander_the_resolver_refuses_is_failed_naming_the_refusal";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let Rig { primary: _primary, commander, _carried, running, launch, key, dir, target, node_id, incarnation, commander_id, .. } = rig("refused-commander").await;
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_refused".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_refused".into(), attempt: 2, operation: "start-node:mesh1.rpc.1".into() };
        let left = call(&commander, &target, &stop).await;
        assert!(matches!(left, StatusReply::Stopped { .. }), "the commander's stop is answered Stopped: {left:?}");
        let held = running.membership.projected_member_at(&commander.seed.0).expect("the projection carries the commander");
        running.node_rpc.resolver.depart(&commander_id, &held.node.incarnation, &held.node.name);
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        match call(&commander, &target, &start).await {
            StatusReply::StartFailed { step, reason } => {
                assert_eq!(step, "node.join", "{reason}");
                assert!(reason.contains("commander-refused") && reason.contains("mesh1.admin.2") && reason.contains("Departed"), "the refusal names the commander and the resolver's refusal: {reason}");
            }
            other => panic!("a start whose commander the resolver refuses fails at its join naming the refusal: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}
