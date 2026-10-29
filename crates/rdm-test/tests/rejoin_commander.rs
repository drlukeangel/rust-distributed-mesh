//! A start after a stop joins back the admin that commanded it (node-start.md step 2: "A stopped
//! node told to start joins back over the endpoint and path of the call that commanded it; it holds
//! no record of the commander and never refuses for lacking one, and learns who answered from the
//! join answer"; the stopped node is reached by a fresh Node RPC dial to its still-bound endpoint,
//! states.md "stopped nodes"). The commander reached the stopped node, so its key is the call's own,
//! proved by the transport (node-rpc.md §14), and the path the call came in on is a path back: the
//! join is a call to that endpoint at that path, whose target names no node (R-UC1a: no receiver
//! refuses a call for the node id it names). No projection, heartbeat view or resolver record of
//! the commander is read.
//!
//! The stop and start spans name the commander by the node id the node's
//! member projection gives its endpoint, when one does.
//!
//! Each cell runs in its own process: the cells hold process-wide state (the stop flag).

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
pub(crate) const NEVER: Duration = Duration::from_secs(20);

pub(crate) async fn call(admin: &crate::common::AdminSide, target: &NodeTarget, req: &StatusRequest) -> StatusReply {
    let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
    if let Some(reply) = crate::common::lifecycle_reply(&admin.observer.client, target, req, &opts, Some(&admin.observer.topology_view.digest_view)).await { return reply; }
    match admin.observer.client.call::<Status>(target, req, &opts).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} did not end in a reply: {other:?}", req.op()),
    }
}

/// The cell's mesh: `primary` launched the rpc node `running` (`mesh1.rpc.1`, Ready at its
/// primary); `commander` (`mesh1.admin.2`) never says anything on the mesh channel, so the node's
/// heartbeat view holds no digest of it, while the primary carries it in the member projection the
/// node installed (`_carried`). The commander reaches the node by its exact birth (`target`).
pub(crate) struct Rig {
    pub(crate) primary: crate::common::AdminSide,
    pub(crate) commander: crate::common::AdminSide,
    pub(crate) _carried: Option<crate::common::Carried>,
    pub(crate) running: node::RunningNode,
    pub(crate) launch: Launch,
    pub(crate) key: iroh::SecretKey,
    pub(crate) dir: std::path::PathBuf,
    pub(crate) target: NodeTarget,
    pub(crate) node_id: NodeId,
    pub(crate) incarnation: IncarnationId,
    pub(crate) commander_key: String,
    pub(crate) commander_id: NodeId,
}

async fn rig(tag: &str) -> Rig {
    rig_carried(tag, true).await
}

/// [`rig`], with the commander carried in the primary's member projection only when `carry` holds:
/// otherwise no record the node holds names the commander, as for an admin born after its stop.
async fn rig_carried(tag: &str, carry: bool) -> Rig {
    rig_pre_node_ready(tag, carry, None).await
}

/// [`rig_carried`], the node running `pre_node_ready` as its `pre_node_ready` hook when one is given.
pub(crate) async fn rig_pre_node_ready(tag: &str, carry: bool, pre_node_ready: Option<std::sync::Arc<rdm_test::pre_node_ready_probe::TestPreNodeReady>>) -> Rig {
    let fabric = rdm_core::FabricId::mint();
    let primary = crate::common::admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, |b| b).await;
    let commander = crate::common::admin_side_silent_as("127.0.0.1".parse().unwrap(), &primary, "mesh1.admin.2").await;
    let dir = std::env::temp_dir().join(format!("{tag}-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
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
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    };
    primary.deployed(&launch, &key);
    let running = match pre_node_ready {
        Some(h) => node::start_pre_node_ready(&launch, rdm_admin::node_hooks::NodeHooks::new(h), |b, _| b).await,
        None => node::start(&launch, |b, _| b).await,
    }
    .expect("the node comes up");
    let digest_view = &primary.observer.topology_view.digest_view;
    let mut heard = digest_view.heard_changes();
    tokio::time::timeout(NEVER, async {
        while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready) {
            heard.changed().await.unwrap();
        }
    })
    .await
    .expect("the node never reached Ready at its primary");

    // Only the carry's delta is in flight once the carry starts: the node first holds its primary's
    // projection naming it Ready, so no later delta reaches it ahead of an earlier one. A delta
    // ahead of its predecessor is a gap, and the top-up's topology read installs the primary's whole
    // projection into the node's view, the commander included (gossip.md §3.3).
    let own_key = digest_view.get(launch.node_id.as_str()).map(|(d, _)| d.node.endpoint_id.0.clone()).expect("the primary holds the node");
    tokio::time::timeout(NEVER, async {
        while running.topology_view.projected_member_at(&own_key).is_none_or(|d| d.status != MemberStatus::Ready) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the node never installed its primary's projection naming it Ready");

    // From here on the primary carries the commander in the member projection it publishes (when
    // `carry`); the commander itself says nothing on the mesh channel.
    let commander_key = commander.seed.0.clone();
    let commander_id = commander.launcher.node_id.clone();
    let carried = carry.then(|| primary.carry_in_projection(&commander));
    if carry {
        tokio::time::timeout(NEVER, async {
            while running.topology_view.projected_member_at(&commander_key).is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the node never installed a projection of its primary carrying the commander");
    } else {
        assert!(running.topology_view.projected_member_at(&commander_key).is_none(), "a projection the node holds names the commander: the cell proves nothing");
    }
    assert!(running.topology_view.digest_view.get(commander_id.as_str()).is_none(), "the node's heartbeat view holds the commander: the cell proves nothing");

    // The commander reaches the node by its birth, as the primary's view holds it.
    let (born, _) = digest_view.get(launch.node_id.as_str()).unwrap();
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
pub(crate) fn spans_of(spans: &crate::common::Spans, name: &str, node: &str) -> Vec<BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, fields)| n == name && fields.get("node").map(String::as_str) == Some(node)).map(|(_, _, fields)| fields.clone()).collect()
}

#[test]
fn a_stopped_node_whose_heartbeat_view_lacks_its_commander_starts_and_joins_it_through_the_call() {
    let cell = "a_stopped_node_whose_heartbeat_view_lacks_its_commander_starts_and_joins_it_through_the_call";
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
        assert!(running.topology_view.digest_view.get(commander_id.as_str()).is_none(), "the node's heartbeat view came to hold the commander before its start: the cell proves nothing");

        // The commander deployed this birth and awaits its join, as the admin that commands its start.
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        let started = call(&commander, &target, &start).await;
        assert_eq!(started, StatusReply::Started, "the node starts after its stop, joining the admin that commanded it through the call");
        tokio::time::timeout(NEVER, async {
            while !matches!(call(&commander, &target, &probe).await, StatusReply::Current { state: NodeState::Ready, .. }) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the started node never answered Ready");

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
/// heartbeat view nor any projection it installed: an admin born after its stop). The stop is
/// answered `Stopped` and the start `Started`: the start joined the admin through the call's endpoint
/// and path, and the node is the same birth, Ready again. Must NOT happen: the start
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
        assert!(running.topology_view.digest_view.get(commander_id.as_str()).is_none(), "the node's heartbeat view holds the commander: the cell proves nothing");
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        let started = call(&commander, &target, &start).await;
        assert_eq!(started, StatusReply::Started, "the node starts after its stop, joining the admin that commanded it through the call, with no record of it");
        tokio::time::timeout(NEVER, async {
            while !matches!(call(&commander, &target, &probe).await, StatusReply::Current { state: NodeState::Ready, .. }) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the started node never answered Ready");
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
/// Ready again. Must NOT happen: the successor's start answering the lost commander's
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
            while running.topology_view.projected_member_at(&lost_key).is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the node never installed a projection of its primary carrying the lost commander");
        let (born, _) = primary.observer.topology_view.digest_view.get(node_id.as_str()).unwrap();
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
                while !matches!(call(&successor, &target, &probe).await, StatusReply::Current { state: NodeState::Ready, .. }) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("the started node never answered Ready");
            let successor_short = successor_key.parse::<iroh::PublicKey>().unwrap().fmt_short().to_string();
            let joins = spans_of(&spans, "rdm.mesh.entry.update.via-pull-attempt", "mesh1.rpc.1");
            assert!(
                joins.iter().any(|f| f.get("outcome").map(String::as_str) == Some("answered") && f.get("anchor") == Some(&successor_short)),
                "an answered join of the start names the successor's key {successor_short}: {joins:?}"
            );
            let _ = close_lost.await;
        };
        // The cell ends with the successor's start: the lost commander's call, on its closed
        // endpoint, is dropped then.
        let (mut first, mut rest) = (std::pin::pin!(first), std::pin::pin!(rest));
        let mut first_ended = false;
        loop {
            tokio::select! {
                () = &mut rest => break,
                _ = &mut first, if !first_ended => first_ended = true,
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (node-start.md step 2: the stopped node "holds no record of the commander and never
/// refuses for lacking one"; R-UC1a): the node's resolver holds the commander deleted (a record
/// that says it never returns), yet the commander's call reached the node. The start is answered
/// `Started`: the join went back over the call's endpoint and path, and no record of the commander,
/// the resolver's included, was read. Must NOT happen: the start refused at `node.join` naming the
/// resolver's record (`commander-refused`, `Deleted`).
#[test]
fn a_start_whose_commander_the_resolver_holds_deleted_joins_it_over_its_call() {
    let cell = "a_start_whose_commander_the_resolver_holds_deleted_joins_it_over_its_call";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let Rig { primary: _primary, commander, _carried, running, launch, key, dir, target, node_id, incarnation, commander_id, .. } = rig("deleted-commander").await;
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "848haseghsta".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "848haseghsta".into(), attempt: 2, operation: "node.start:mesh1.rpc.1".into() };
        let left = call(&commander, &target, &stop).await;
        assert!(matches!(left, StatusReply::Stopped { .. }), "the commander's stop is answered Stopped: {left:?}");
        let held = running.topology_view.projected_member_at(&commander.seed.0).expect("the projection carries the commander");
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
/// record it holds names, and its hook answers `Blocked { PrimaryAddressed }` after that start. The
/// commander then addresses the node with a down op (a probe): the hook runs again at once, passes,
/// and the node is Ready. The primary is known by the endpoint of the call it answered
/// on, so its down op is counted whatever record names it. Must NOT happen: the hook waiting on, and
/// the node held Pending, because the down op came from an endpoint no record names.
#[test]
fn a_hook_blocked_on_its_primarys_word_runs_again_when_a_commander_no_record_names_addresses_it() {
    let cell = "a_hook_blocked_on_its_primarys_word_runs_again_when_a_commander_no_record_names_addresses_it";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        use rdm_admin::node_hooks::{HookOutcome, PreNodeReadyState, RetryOn};
        use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
        let pre_node_ready = TestPreNodeReady::new(Mode::Local);
        let Rig { primary: _primary, commander, _carried, running, launch, key, dir, target, node_id, incarnation, .. } = rig_pre_node_ready("addressed-unrecorded", false, Some(pre_node_ready.clone())).await;
        let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "f4te7echyh6f".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "f4te7echyh6f".into(), attempt: 2, operation: "node.start:mesh1.rpc.1".into() };
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        assert!(matches!(call(&commander, &target, &stop).await, StatusReply::Stopped { .. }), "the commander's stop is answered Stopped");
        commander.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        // After this start the hook waits on its authority's word.
        pre_node_ready.set_mode(Mode::Fixed(HookOutcome::Blocked { reason: "waits for its authority to address it".into(), retry_on: RetryOn::PrimaryAddressed }));
        assert_eq!(call(&commander, &target, &start).await, StatusReply::Started, "the start joins the commander over its call; its hook runs on, blocked");
        let handle = running.pre_node_ready().expect("a registered hook has a handle");
        tokio::time::timeout(NEVER, async {
            while !matches!(handle.state(), PreNodeReadyState::Blocked { retry_on: RetryOn::PrimaryAddressed, .. }) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the hook never blocked on its authority's word: {:?}", handle.state()));
        // The commander's down op: the hook runs again, and this time passes.
        pre_node_ready.set_mode(Mode::Local);
        let _ = call(&commander, &target, &probe).await;
        let settled = tokio::time::timeout(NEVER, handle.settled()).await.unwrap_or_else(|_| panic!("the hook never ran again on the commander's down op: {:?}", handle.state()));
        assert_eq!(settled, PreNodeReadyState::Passed);
        tokio::time::timeout(NEVER, async {
            while !matches!(call(&commander, &target, &probe).await, StatusReply::Current { state: NodeState::Ready, .. }) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the started node never answered Ready");
        let blocked = spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.mesh.node.reject.via-pre-node-ready-blocked" && f.get("cleared_by").map(String::as_str) == Some("primary-addressed")).count();
        assert_eq!(blocked, 1, "the blocker span closes naming the event that cleared it, primary-addressed");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// CONTRACT (node-start.md steps 2 and 4: the start joins back over the call that commanded it, then
/// reads topology from its mesh-primary, the admin commanding it; node-topology-get.md): a stopped
/// node's start is commanded by an admin that answers its join and is lost while the start's hook
/// runs (its endpoint closes before the topology read). A later admin commands the start while it is
/// under way. The start's topology read and rafka-time refresh go to the later commander, reached back
/// over its own call, and the start is answered `Started`: the node is Ready, its rafka-time
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
        use rdm_admin::node_hooks::HookOutcome;
        use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
        let pre_node_ready = TestPreNodeReady::new(Mode::Local);
        let Rig { primary, commander: successor, _carried, running: _running, launch, key, dir, target, node_id, incarnation, commander_key: successor_key, .. } = rig_pre_node_ready("topology-successor", true, Some(pre_node_ready.clone())).await;
        // The lost commander: an admin of the mesh that answers the start's join, carried in the
        // primary's projection.
        let lost = crate::common::admin_side_silent_as("127.0.0.1".parse().unwrap(), &primary, "mesh1.admin.3").await;
        let _lost_carried = primary.carry_in_projection(&lost);
        let (born, _) = primary.observer.topology_view.digest_view.get(node_id.as_str()).unwrap();
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
        pre_node_ready.set_mode(Mode::Pause { started: hook_started.clone(), resume: hook_resume.clone(), outcome: HookOutcome::Ok });
        lost.deployed_as(&launch, &key, launch.name.clone(), Some(launch.name.clone()));
        // The lost commander's own call breaks with its endpoint: what it ends in is not this cell's.
        let first_req = match start(2) {
            StatusRequest::StartNode { node_id, incarnation, build_id, attempt, operation } => rdm_core::node_command::NodeCommandRequest::StartNode { node_id, incarnation, build_id, attempt, operation },
            _ => unreachable!("the first command is a start"),
        };
        let first = async { crate::common::node_command(&lost.observer.client, &target, &first_req, &CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(60) }, ..CallOptions::default() }).await };
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
                while !matches!(call(&successor, &target, &probe).await, StatusReply::Current { state: NodeState::Ready, .. }) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("the started node never answered Ready");
            let successor_short = successor_key.parse::<iroh::PublicKey>().unwrap().fmt_short().to_string();
            let refreshed = spans_of(&spans, "rdm.mesh.node.update.via-rafka-time-refreshed", "mesh1.rpc.1");
            assert!(
                refreshed.iter().any(|f| f.get("outcome").map(String::as_str) == Some("adopted") && f.get("served_by").is_some_and(|s| s.contains(&successor_short))),
                "the start's rafka-time refresh is served by the later commander {successor_short}: {refreshed:?}"
            );
        };
        // The cell ends with the later commander's start: the lost commander's call, on its closed
        // endpoint, is dropped then.
        let (mut first, mut rest) = (std::pin::pin!(first), std::pin::pin!(rest));
        let mut first_ended = false;
        loop {
            tokio::select! {
                () = &mut rest => break,
                _ = &mut first, if !first_ended => first_ended = true,
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    })
}
