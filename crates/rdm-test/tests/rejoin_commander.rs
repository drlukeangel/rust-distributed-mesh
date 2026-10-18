//! A start after a stop joins back the admin that commanded it (node-start.md step 2: "A stopped
//! node told to start joins back over the endpoint and path of the call that commanded it; it holds
//! no record of the commander and never refuses for lacking one, and learns who answered from the
//! join answer"; the stopped node is reached by a fresh Node RPC dial to its still-bound endpoint,
//! states.md "stopped nodes"). The commander reached the stopped node, so its key is the call's own,
//! proved by the transport (node-rpc.md §14), and the path the call came in on is a path back: the
//! join is a call to that endpoint at that path, whose target names no node (R-UC1a: no receiver
//! refuses a call for the node id it names). No projection, heartbeat book or resolver record of
//! the commander is read.
//!
//! The stop and start spans name the commander by the node id the node's
//! member projection gives its endpoint, when one does.
//!
//! Each cell runs in its own process: the cells hold process-wide state (the parking flag).

use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, MemberStatus, MeshId, NodeId};
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{ConnectionsReceipt, NodeState, Status, StatusReply, StatusRequest};
use rdm_test::node;
use std::collections::BTreeMap;
use std::time::Duration;

/// What a step that is immediate on a correct node is given before the cell fails by name.
const NEVER: Duration = Duration::from_secs(20);

async fn call(admin: &crate::common::AdminSide, target: &NodeTarget, req: &StatusRequest) -> StatusReply {
    let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
    if let Some(reply) = crate::common::lifecycle_reply(&admin.observer.client, target, req, &opts, Some(&admin.observer.membership.book)).await { return reply; }
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
    rig_hydrating(tag, carry, None).await
}

/// [`rig_carried`], the node running `hydrator` as its `hydrate_before_ready` hook when one is given.
async fn rig_hydrating(tag: &str, carry: bool, hydrator: Option<std::sync::Arc<rdm_test::hydrate_probe::TestHydrator>>) -> Rig {
    let fabric = rdm_core::FabricId::mint();
    let primary = crate::common::admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
    let commander = crate::common::admin_side_silent_as("127.0.0.1".parse().unwrap(), &primary, "mesh1.admin.2").await;
    let dir = std::env::temp_dir().join(format!("{tag}-{}", NodeId::mint()));
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
        seeds: vec![primary.seed.clone()],
        launcher: Some(primary.launcher.clone()),
        data_dir: dir.clone(),
        store_dir: dir.clone(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
        mesh_issuer: None,
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    primary.deployed(&launch, &key);
    let running = match hydrator {
        Some(h) => node::start_hydrating(&launch, rdm_admin::app_hydration::Hydration::new(h), |b, _| b).await,
        None => node::start(&launch, |b, _| b).await,
    }
    .expect("the node comes up");
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
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "ptwmpv3sma8m".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "ptwmpv3sma8m".into(), attempt: 2, operation: "node.start:mesh1.rpc.1".into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };

        let left = call(&commander, &target, &stop).await;
        assert!(matches!(left, StatusReply::Stopped { receipt: ConnectionsReceipt::Established { .. } }), "the commander's stop is answered Stopped: {left:?}");
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
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "daycheyancwf".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "daycheyancwf".into(), attempt: 2, operation: "node.start:mesh1.rpc.1".into() };
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
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "7f0xhg20h9hv".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = |attempt: u32| StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "7f0xhg20h9hv".into(), attempt, operation: "node.start:mesh1.rpc.1".into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        let left = call(&lost, &target, &stop).await;
        assert!(matches!(left, StatusReply::Stopped { .. }), "the stop is answered Stopped: {left:?}");

        // The lost commander's start: its join door answers no join, and once the start's first join
        // attempt reached it, its endpoint closes.
        let lost_short = lost_key.parse::<iroh::PublicKey>().unwrap().fmt_short().to_string();
        let first_req = start(2);
        let first_req = match first_req {
            StatusRequest::StartNode { node_id, incarnation, build_id, attempt, operation } => rdm_core::node_command::NodeCommandRequest::StartNode { node_id, incarnation, build_id, attempt, operation },
            _ => unreachable!("the first command is a start"),
        };
        let first = async { crate::common::node_command(&lost.observer.client, &target, &first_req, &CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(60) }, ..CallOptions::default() }).await };
        let rest = async {
            let at_lost = |f: &BTreeMap<String, String>| f.get("node").map(String::as_str) == Some("mesh1.rpc.1") && f.get("anchor") == Some(&lost_short) && f.contains_key("outcome");
            if tokio::time::timeout(NEVER, spans.recorded("rdm.mesh.entry.update.via-pull-attempt", at_lost)).await.is_err() {
                panic!("the start never made a join attempt at the commander that sent it");
            }
            let close_lost = {
                let ep = lost.observer.client.endpoint().clone();
                tokio::spawn(async move { ep.close().await })
            };

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
            let _ = close_lost.await;
        };
        let (_lost_answer, ()) = tokio::join!(first, rest);
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (node-start.md step 2: the stopped node "holds no record of the commander and never
/// refuses for lacking one"; R-UC1a): the node's resolver holds the commander departed (a record
/// that says it never returns), yet the commander's call reached the node. The start is answered
/// `Started`: the join went back over the call's endpoint and path, and no record of the commander,
/// the resolver's included, was read. Must NOT happen: the start refused at `node.join` naming the
/// resolver's record (`commander-refused`, `Departed`).
#[test]
fn a_start_whose_commander_the_resolver_holds_departed_joins_it_over_its_call() {
    let cell = "a_start_whose_commander_the_resolver_holds_departed_joins_it_over_its_call";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let Rig { primary: _primary, commander, _carried, running, launch, key, dir, target, node_id, incarnation, commander_id, .. } = rig("departed-commander").await;
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "848haseghsta".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "848haseghsta".into(), attempt: 2, operation: "node.start:mesh1.rpc.1".into() };
        let left = call(&commander, &target, &stop).await;
        assert!(matches!(left, StatusReply::Stopped { .. }), "the commander's stop is answered Stopped: {left:?}");
        let held = running.membership.projected_member_at(&commander.seed.0).expect("the projection carries the commander");
        running.node_rpc.resolver.depart(&commander_id, &held.node.incarnation, &held.node.name);
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        let started = call(&commander, &target, &start).await;
        assert_eq!(started, StatusReply::Started, "the start joins the commander over its call, whatever the resolver holds of it");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (fabric-node-hooks.md "Outcomes and the gate": a Blocked hook runs again only when its
/// `retry_on` occurs; node-start.md step 2: the stopped node holds no record of the commander and
/// learns who answered from the join answer): an rpc node is stopped and started by an admin no
/// record it holds names, and its hook answers `Blocked { AuthorityAddressed }` after that start. The
/// commander then addresses the node with a down op (a probe): the hook runs again at once, passes,
/// and the node is ReadyForTraffic. The authority is known by the endpoint of the call it answered
/// on, so its down op is counted whatever record names it. Must NOT happen: the hook waiting on, and
/// the node held Pending, because the down op came from an endpoint no record names.
#[test]
fn a_hook_blocked_on_its_authoritys_word_runs_again_when_a_commander_no_record_names_addresses_it() {
    let cell = "a_hook_blocked_on_its_authoritys_word_runs_again_when_a_commander_no_record_names_addresses_it";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        use rdm_admin::app_hydration::{HookOutcome, HydrationState, RetryOn};
        use rdm_test::hydrate_probe::{Mode, TestHydrator};
        let hydrator = TestHydrator::new(Mode::Local);
        let Rig { primary: _primary, commander, _carried, running, launch, key, dir, target, node_id, incarnation, .. } = rig_hydrating("addressed-unrecorded", false, Some(hydrator.clone())).await;
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "f4te7echyh6f".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "f4te7echyh6f".into(), attempt: 2, operation: "node.start:mesh1.rpc.1".into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        assert!(matches!(call(&commander, &target, &stop).await, StatusReply::Stopped { .. }), "the commander's stop is answered Stopped");
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        // After this start the hook waits on its authority's word.
        hydrator.set_mode(Mode::Fixed(HookOutcome::Blocked { reason: "waits for its authority to address it".into(), retry_on: RetryOn::AuthorityAddressed }));
        assert_eq!(call(&commander, &target, &start).await, StatusReply::Started, "the start joins the commander over its call; its hook runs on, blocked");
        let handle = running.hydration().expect("a registered hook has a handle");
        tokio::time::timeout(NEVER, async {
            while !matches!(handle.state(), HydrationState::Blocked { retry_on: RetryOn::AuthorityAddressed, .. }) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the hook never blocked on its authority's word: {:?}", handle.state()));
        // The commander's down op: the hook runs again, and this time passes.
        hydrator.set_mode(Mode::Local);
        let _ = call(&commander, &target, &probe).await;
        let settled = tokio::time::timeout(NEVER, handle.settled()).await.unwrap_or_else(|_| panic!("the hook never ran again on the commander's down op: {:?}", handle.state()));
        assert_eq!(settled, HydrationState::Passed);
        tokio::time::timeout(NEVER, async {
            while !matches!(call(&commander, &target, &probe).await, StatusReply::Current { state: NodeState::ReadyForTraffic, .. }) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the started node never answered ReadyForTraffic");
        let blocked = spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.mesh.node.reject.via-hydration-blocked" && f.get("cleared_by").map(String::as_str) == Some("authority-addressed")).count();
        assert_eq!(blocked, 1, "the blocker span closes naming the event that cleared it, authority-addressed");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (node-start.md steps 2 and 4: the start joins back over the call that commanded it, then
/// reads topology from its mesh-primary, the admin commanding it; node-topology-get.md): a stopped
/// node's start is commanded by an admin that answers its join and is lost while the start's hook
/// runs (its endpoint closes before the topology read). A later admin commands the start while it is
/// under way. The start's topology read and rafka-time refresh go to the later commander, reached back
/// over its own call, and the start is answered `Started`: the node is ReadyForTraffic, its rafka-time
/// refresh served by the later commander. Must NOT happen: the read held on the lost commander's
/// endpoint until its deadline and the start failed at `node.topology`.
#[test]
fn a_start_whose_commander_is_lost_after_its_join_reads_its_topology_from_the_later_commander() {
    let cell = "a_start_whose_commander_is_lost_after_its_join_reads_its_topology_from_the_later_commander";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        use rdm_admin::app_hydration::HookOutcome;
        use rdm_test::hydrate_probe::{Mode, TestHydrator};
        let hydrator = TestHydrator::new(Mode::Local);
        let Rig { primary, commander: successor, _carried, running: _running, launch, key, dir, target, node_id, incarnation, commander_key: successor_key, .. } = rig_hydrating("topology-successor", true, Some(hydrator.clone())).await;
        // The lost commander: an admin of the mesh that answers the start's join, carried in the
        // primary's projection.
        let lost = crate::common::admin_side_silent_as("127.0.0.1".parse().unwrap(), &primary, "mesh1.admin.3").await;
        let _lost_carried = primary.carry_in_projection(&lost);
        let (born, _) = primary.observer.membership.book.get(node_id.as_str()).unwrap();
        lost.observer.resolver.insert(ResolvedNode {
            node_id: born.node.node_id.clone(),
            name: born.node.name.clone(),
            endpoint_id: born.node.endpoint_id.0.parse().unwrap(),
            incarnation: born.node.incarnation.clone(),
            transport_addr: born.node.transport_addr,
        });
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bq0z8f2kq3e1".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = |attempt: u32| StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bq0z8f2kq3e1".into(), attempt, operation: "node.start:mesh1.rpc.1".into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        assert!(matches!(call(&lost, &target, &stop).await, StatusReply::Stopped { .. }), "the lost commander's stop is answered Stopped");

        // The lost commander's start joins it, then holds in its hook until the commander is gone.
        let (hook_started, hook_resume) = (std::sync::Arc::new(tokio::sync::Notify::new()), std::sync::Arc::new(tokio::sync::Notify::new()));
        hydrator.set_mode(Mode::Pause { started: hook_started.clone(), resume: hook_resume.clone(), outcome: HookOutcome::Ok });
        lost.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        let first = async { call(&lost, &target, &start(2)).await };
        let rest = async {
            tokio::time::timeout(NEVER, hook_started.notified()).await.expect("the lost commander's start never reached its hook: its join was not answered");
            lost.observer.client.endpoint().close().await;
            // The later commander commands the start while it is under way; its hook then passes.
            successor.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
            let later = async { call(&successor, &target, &start(3)).await };
            let release = async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                hook_resume.notify_one();
            };
            let (started, ()) = tokio::join!(later, release);
            assert_eq!(started, StatusReply::Started, "the later commander's start is answered Started: its reads went to the later commander");
            tokio::time::timeout(NEVER, async {
                while !matches!(call(&successor, &target, &probe).await, StatusReply::Current { state: NodeState::ReadyForTraffic, .. }) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("the started node never answered ReadyForTraffic");
            let successor_short = successor_key.parse::<iroh::PublicKey>().unwrap().fmt_short().to_string();
            let refreshed = spans_of(&spans, "rdm.mesh.node.update.via-rafka-time-refreshed", "mesh1.rpc.1");
            assert!(
                refreshed.iter().any(|f| f.get("outcome").map(String::as_str) == Some("adopted") && f.get("served_by").is_some_and(|s| s.contains(&successor_short))),
                "the start's rafka-time refresh is served by the later commander {successor_short}: {refreshed:?}"
            );
        };
        let (_lost_answer, ()) = tokio::join!(first, rest);
        let _ = std::fs::remove_dir_all(&dir);
    })
}
