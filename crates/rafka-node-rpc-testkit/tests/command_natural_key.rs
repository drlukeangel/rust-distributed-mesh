//! A birth admits a command once per natural key, never per attempt or commander
//! (node-rpc-envelope.md, "Status": "Idempotency is the natural key `(node_id, incarnation,
//! state)`, `(mesh_id, state)` or `(fabric_id, event)`. Backward transitions are refused by name";
//! node-drain.md, "Failure and receipts": "Completed steps are not rerun. Settle an idempotent
//! uncertain step by its natural key and `AlreadyApplied`"). A drain's key is `(node_id,
//! incarnation, Draining)`; a fabric round's is the round of its fabric and accepted Build.
//!
//! One admin side commands the birth under several attempts of one Build: each attempt is its own
//! open command in the admin's command book (or its own round check-in), matched by its attempt, so
//! a completion carrying another attempt resolves nothing. The natural key holds no commander, so a
//! second physical node-admin adds nothing the attempts do not already prove.
//!
//! A cell that stops the birth runs in its own process (the process-wide parking flag).

use crate::common::admin_side_serving;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, Seat, SeatHolder};
use rafka_node_admin_core::node_commands::CommandKey;
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{DrainReceipt, NodeState, Status, StatusReply, StatusRequest};
use rafka_node_admin_core::app_hydration::{HookOutcome, Hydration};
use rafka_node_rpc_testkit::hydrate_probe::{HydratePull, Mode, PullRequest, PullServer, TestHydrator};
use rafka_node_rpc_testkit::node;
use std::time::Duration;

const BUILD: &str = "bld_natural_key";
const NODE: &str = "mesh1.rpc.1";

/// A node of the admin side's mesh serving one held op (a call into it stays in flight until the
/// cell releases it), ready, resolved by the admin side.
struct Birth {
    admin: crate::common::AdminSide,
    launch: Launch,
    key: iroh::SecretKey,
    pull: std::sync::Arc<PullServer>,
    target: NodeTarget,
    dir: std::path::PathBuf,
    running: node::RunningNode,
}

impl Birth {
    async fn up(cell: &str) -> Self {
        Self::up_hydrating(cell, None).await
    }

    /// [`Self::up`], its `hydrate_before_ready` hook `hydrator` when one is given.
    async fn up_hydrating(cell: &str, hydrator: Option<std::sync::Arc<TestHydrator>>) -> Self {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let dir = std::env::temp_dir().join(format!("{cell}-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = Launch {
            fabric: "fabric1".into(),
            fabric_id: fabric,
            name: NODE.parse().unwrap(),
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
        let pull = PullServer::new(vec![]);
        pull.gate.open();
        let running = match hydrator {
            None => node::start(&launch, {
                let pull = pull.clone();
                move |b, _| pull.serve(b)
            })
            .await,
            Some(h) => node::start_hydrating(&launch, Hydration::new(h), {
                let pull = pull.clone();
                move |b, _| pull.serve(b)
            })
            .await,
        }
        .expect("the node comes up");
        let birth = Self { target: NodeTarget::ExactNode(launch.node_id.clone()), admin, launch, key, pull, dir, running };
        birth.status_heard(MemberStatus::ReadyForTraffic).await;
        let (d, _) = birth.admin.observer.membership.book.get(birth.launch.node_id.as_str()).unwrap();
        birth.admin.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        birth
    }

    /// Resolves once the admin side's book holds a digest of this birth saying `status`.
    async fn status_heard(&self, status: MemberStatus) {
        let book = &self.admin.observer.membership.book;
        let mut heard = book.heard_changes();
        let mut digests = book.digest_changes();
        tokio::time::timeout(Duration::from_secs(30), async {
            while book.get(self.launch.node_id.as_str()).is_none_or(|(d, _)| d.status != status) {
                tokio::select! {
                    r = heard.changed() => r.unwrap(),
                    r = digests.changed() => r.unwrap(),
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the admin side never heard {NODE} say {status:?}"));
    }

    fn opts() -> CallOptions {
        CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() }
    }

    async fn call(&self, req: &StatusRequest) -> StatusReply {
        match self.admin.observer.client.call::<Status>(&self.target, req, &Self::opts()).await.0 {
            RpcOutcome::Reply(r) => r.into_value(),
            other => panic!("{} did not end in a reply: {other:?}", req.op()),
        }
    }

    /// One call into the held op, in flight until [`PullServer::release`]; resolves once it entered.
    async fn hold_one_call(self: &std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.pull.hold();
        let me = self.clone();
        let held = tokio::spawn(async move {
            let out = me.admin.observer.client.call::<HydratePull>(&me.target, &PullRequest::Pull, &Self::opts()).await.0;
            assert!(matches!(out, RpcOutcome::Reply(_)), "the held call answers once released: {out:?}");
        });
        self.pull.entered(1).await;
        held
    }

    fn command_key(&self, attempt: u32, operation: &str) -> CommandKey {
        CommandKey { build_id: BUILD.into(), attempt, operation: operation.into(), node_id: self.launch.node_id.clone(), incarnation: self.launch.incarnation.clone() }
    }

    fn drain(&self, attempt: u32) -> StatusRequest {
        StatusRequest::DrainNode { node_id: self.launch.node_id.clone(), incarnation: self.launch.incarnation.clone(), build_id: BUILD.into(), attempt, operation: format!("drain-node:{NODE}") }
    }

    fn stop(&self, attempt: u32) -> StatusRequest {
        StatusRequest::StopNode { node_id: self.launch.node_id.clone(), incarnation: self.launch.incarnation.clone(), build_id: BUILD.into(), attempt, operation: format!("stop-node:{NODE}") }
    }

    fn start(&self, attempt: u32) -> StatusRequest {
        StatusRequest::StartNode { node_id: self.launch.node_id.clone(), incarnation: self.launch.incarnation.clone(), build_id: BUILD.into(), attempt, operation: format!("start-node:{NODE}") }
    }

    /// The admin side's open drain command of `attempt`: resolves when its node-drained is accepted.
    fn open_drain(&self, attempt: u32) -> tokio::sync::watch::Receiver<bool> {
        self.admin.observer.commands.open(self.command_key(attempt, &format!("drain-node:{NODE}")))
    }
}

async fn node_drained(rx: &mut tokio::sync::watch::Receiver<bool>, attempt: u32) {
    tokio::time::timeout(Duration::from_secs(20), rx.wait_for(|done| *done))
        .await
        .unwrap_or_else(|_| panic!("attempt {attempt}'s commander never accepted a node-drained naming attempt {attempt}"))
        .expect("the command book is held by the admin side");
}

fn spans_named(spans: &crate::common::Spans, name: &str) -> Vec<std::collections::BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, _, f)| f.clone()).collect()
}

/// CONTRACT: a birth draining under attempt 1 answers attempt 2's `DrainNode` `AlreadyApplied` and
/// runs no second drain; when its in-flight work reaches zero, the commander of each attempt that
/// asked accepts a `node-drained` naming its own attempt. Once drained, attempt 3's `DrainNode` is
/// `AlreadyApplied` too and attempt 3's commander gets its `node-drained` at once (the held
/// completion reaches the commander that asks). A `StopNode` is a different state and is admitted
/// (`Stopped`, the drain established); a `DrainNode` after it is the backward transition and is
/// refused naming `Leaving`. The started birth is a new cycle: its next `DrainNode` is admitted and
/// drains. What must NOT happen: a second drain task for one birth's one drain, `Applied` for a
/// repeat of the same state, or a later attempt's commander left without its completion.
#[test]
fn a_drain_from_a_later_attempt_joins_the_drain_under_way_and_each_commander_gets_node_drained() {
    let cell = "a_drain_from_a_later_attempt_joins_the_drain_under_way_and_each_commander_gets_node_drained";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let birth = std::sync::Arc::new(Birth::up("drain-natural-key").await);
        let held = birth.hold_one_call().await;
        let (mut first, mut second, mut third) = (birth.open_drain(1), birth.open_drain(2), birth.open_drain(3));

        // Attempt 1 admits the drain; the held call keeps it under way.
        assert_eq!(birth.call(&birth.drain(1)).await, StatusReply::Applied, "attempt 1 admits the drain");
        // Attempt 2 of the same Build asks for the same state of the same birth.
        assert_eq!(birth.call(&birth.drain(2)).await, StatusReply::AlreadyApplied, "attempt 2's DrainNode for the birth attempt 1 is draining is the same natural key (node_id, incarnation, Draining)");
        assert!(!*first.borrow() && !*second.borrow(), "nothing is drained while a call is in flight");

        // The in-flight work reaches zero: each attempt that asked gets node-drained naming itself.
        birth.pull.release();
        held.await.unwrap();
        node_drained(&mut first, 1).await;
        node_drained(&mut second, 2).await;

        // Drained: a later attempt is the same state again, and its commander gets the held completion.
        assert_eq!(birth.call(&birth.drain(3)).await, StatusReply::AlreadyApplied, "a drained birth answers a later attempt's DrainNode AlreadyApplied");
        node_drained(&mut third, 3).await;
        assert_eq!(spans_named(&spans, "rdm.node_admin.status.update.via-drain-node").len(), 1, "one drain task for the birth's one drain, whichever attempts asked");
        let accepted: Vec<_> = spans_named(&spans, "rdm.node_admin.status.update.via-completion-accepted").into_iter().filter(|f| f.get("op").map(String::as_str) == Some("node-drained")).collect();
        for attempt in ["1", "2", "3"] {
            assert!(accepted.iter().any(|f| f.get("attempt").map(String::as_str) == Some(attempt) && f.get("outcome").map(String::as_str) == Some("applied")), "the commander accepted attempt {attempt}'s node-drained as its first: {accepted:?}");
        }

        // StopNode is a different state: admitted, the drain established with nothing in flight.
        let stopped = birth.call(&birth.stop(4)).await;
        assert!(matches!(stopped, StatusReply::Stopped { receipt: DrainReceipt::Established { .. } }), "a StopNode after a completed drain is admitted: {stopped:?}");
        // A drain of a stopped birth goes backward: refused naming the state it is in.
        assert_eq!(birth.call(&birth.drain(5)).await, StatusReply::RejectedInvalidNodeTransition { current: NodeState::Leaving }, "a DrainNode after the stop is refused by name");

        // Started, the birth is a new cycle: its next drain is admitted and drains.
        birth.admin.deployed(&birth.launch, &birth.key);
        assert_eq!(birth.call(&birth.start(6)).await, StatusReply::Started);
        birth.status_heard(MemberStatus::ReadyForTraffic).await;
        let mut after_start = birth.open_drain(7);
        assert_eq!(birth.call(&birth.drain(7)).await, StatusReply::Applied, "the started birth's drain is a new drain");
        node_drained(&mut after_start, 7).await;
        assert_eq!(spans_named(&spans, "rdm.node_admin.status.update.via-drain-node").len(), 2, "one drain task per drain: the first cycle's and the started birth's");
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}

/// CONTRACT: a `DrainNode` that reaches a birth while its stop is draining is the same state the
/// stop's drain is in: it answers `AlreadyApplied`, runs no drain task of its own, and its
/// commander accepts a `node-drained` naming its attempt once the stop's drain is established,
/// before the stop answers `Stopped`. What must NOT happen: a second drain loop beside the stop's,
/// `Applied` for it, or its commander left without a completion.
#[test]
fn a_drain_during_a_stop_joins_the_stops_drain_and_runs_no_drain_of_its_own() {
    let cell = "a_drain_during_a_stop_joins_the_stops_drain_and_runs_no_drain_of_its_own";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let birth = std::sync::Arc::new(Birth::up("drain-during-stop").await);
        let held = birth.hold_one_call().await;
        let mut drained = birth.open_drain(2);

        // The stop is admitted and drains: its held call keeps it Draining.
        let stop = {
            let birth = birth.clone();
            tokio::spawn(async move { birth.call(&birth.stop(1)).await })
        };
        birth.status_heard(MemberStatus::Draining).await;
        assert_eq!(birth.call(&birth.drain(2)).await, StatusReply::AlreadyApplied, "a DrainNode for a birth whose stop is draining is the same natural key (node_id, incarnation, Draining)");
        assert!(!*drained.borrow(), "nothing is drained while a call is in flight");

        birth.pull.release();
        held.await.unwrap();
        node_drained(&mut drained, 2).await;
        let stopped = stop.await.unwrap();
        assert!(matches!(stopped, StatusReply::Stopped { receipt: DrainReceipt::Established { .. } }), "the stop completes on its own drain: {stopped:?}");
        assert!(spans_named(&spans, "rdm.node_admin.status.update.via-drain-node").is_empty(), "the stop's drain is the only drain: no drain task of its own for the DrainNode");
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}

fn commit_state(birth: &Birth, attempt: u32) -> StatusRequest {
    let fabric_id = birth.launch.fabric_id.clone();
    StatusRequest::CommitState { operation: format!("commit-state:{fabric_id}"), fabric_id, node_id: birth.launch.node_id.clone(), incarnation: birth.launch.incarnation.clone(), build_id: BUILD.into(), attempt }
}

/// CONTRACT: a birth that took its mesh primary's `commit-state` under attempt 1 and stored its
/// action answers attempt 2's `commit-state` of the same fabric and Build `AlreadyApplied`: the
/// round is the same natural key `(fabric_id, event)`, so the action is not run again, and the
/// check-in is made again to the commander that asks, naming attempt 2. What must NOT happen: the
/// action run a second time, `Applied` for the same round, or attempt 2's commander left without a
/// check-in naming its own attempt.
#[test]
fn a_round_command_from_a_later_attempt_is_already_applied_and_checked_in_under_that_attempt() {
    let cell = "a_round_command_from_a_later_attempt_is_already_applied_and_checked_in_under_that_attempt";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let birth = Birth::up("round-natural-key").await;
        // The admin side holds its mesh's primary seat, and says so once on the mesh channel.
        let launcher = birth.admin.launcher.clone();
        let holder = SeatHolder { mesh: "mesh1".into(), node_id: launcher.node_id.clone(), incarnation: launcher.incarnation.clone(), epoch: 1 };
        let seated = rafka_mesh_transport::membership::Frame::Seated { seat: Seat::MeshPrimary, holder };
        let seats = birth.running.membership.seats();
        let mut changed = seats.subscribe();
        birth.admin.observer.membership.publish_lifecycle(&seated).await.expect("the seat record goes out on the mesh channel");
        tokio::time::timeout(Duration::from_secs(20), async {
            while seats.mesh("mesh1").is_none_or(|h| !h.is_birth(&launcher.node_id, &launcher.incarnation)) {
                changed.changed().await.unwrap();
            }
        })
        .await
        .expect("the birth holds the admin side as its mesh primary");

        let check_ins = birth.admin.observer.check_ins.subscribe();
        let checked_in = |check_ins: &tokio::sync::watch::Receiver<Vec<StatusRequest>>, attempt: u32| {
            let mut check_ins = check_ins.clone();
            async move {
                tokio::time::timeout(Duration::from_secs(20), check_ins.wait_for(|held| held.iter().any(|r| matches!(r, StatusRequest::StateCommitted { attempt: a, build_id, .. } if *a == attempt && build_id == BUILD))))
                    .await
                    .unwrap_or_else(|_| panic!("the commander never got a state-committed check-in naming attempt {attempt}"))
                    .expect("the admin side holds its check-ins");
            }
        };
        assert_eq!(birth.call(&commit_state(&birth, 1)).await, StatusReply::Applied, "attempt 1 admits the round");
        checked_in(&check_ins, 1).await;
        assert_eq!(birth.call(&commit_state(&birth, 2)).await, StatusReply::AlreadyApplied, "attempt 2's commit-state of the same fabric and Build is the same natural key (fabric_id, event)");
        checked_in(&check_ins, 2).await;

        let rounds = spans_named(&spans, "rdm.node_admin.fabric.update.via-member-round");
        let acted: Vec<_> = rounds.iter().filter(|f| f.get("acted").map(String::as_str) == Some("true")).collect();
        assert_eq!(acted.len(), 1, "the round's action runs once, whichever attempts command it: {rounds:?}");
        assert!(rounds.iter().any(|f| f.get("attempt").map(String::as_str) == Some("2") && f.get("acted").map(String::as_str) == Some("false")), "attempt 2's check-in is made again without the action: {rounds:?}");
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}

/// CONTRACT: a `StartNode` that reaches a parked birth while another attempt's start of it is under
/// way (its hook still running) is the same natural key: it runs no rejoin of its own and answers
/// that start's outcome, `Started`, once it ends; the birth is ready afterwards. What must NOT
/// happen: a second rejoin of one birth, or the second start's own failure path (Leaving, the
/// server drained, the mesh connections cut) tearing down the start under way.
#[test]
fn a_start_from_a_later_attempt_joins_the_start_under_way_and_answers_its_outcome() {
    let cell = "a_start_from_a_later_attempt_joins_the_start_under_way_and_answers_its_outcome";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let hydrator = TestHydrator::new(Mode::Fixed(HookOutcome::Ok));
        let birth = std::sync::Arc::new(Birth::up_hydrating("start-natural-key", Some(hydrator.clone())).await);
        let stopped = birth.call(&birth.stop(1)).await;
        assert!(matches!(stopped, StatusReply::Stopped { .. }), "{stopped:?}");

        // Attempt 2's start runs until its hook, which holds until the cell lets it go.
        let (started, resume) = (std::sync::Arc::new(tokio::sync::Notify::new()), std::sync::Arc::new(tokio::sync::Notify::new()));
        hydrator.set_mode(Mode::Pause { started: started.clone(), resume: resume.clone(), outcome: HookOutcome::Ok });
        birth.admin.deployed(&birth.launch, &birth.key);
        let first = {
            let birth = birth.clone();
            tokio::spawn(async move { birth.call(&birth.start(2)).await })
        };
        started.notified().await;

        // Attempt 3 asks for the same start while it is under way.
        let second = {
            let birth = birth.clone();
            tokio::spawn(async move { birth.call(&birth.start(3)).await })
        };
        tokio::time::timeout(Duration::from_secs(20), spans.recorded("rdm.node_admin.status.update.via-start-node", "joined", "true"))
            .await
            .expect("attempt 3's StartNode joins the start under way instead of running one of its own");
        hydrator.set_mode(Mode::Fixed(HookOutcome::Ok));
        resume.notify_one();

        assert_eq!(first.await.unwrap(), StatusReply::Started, "attempt 2's start completes");
        assert_eq!(second.await.unwrap(), StatusReply::Started, "attempt 3 answers the outcome of the start it joined");
        birth.status_heard(MemberStatus::ReadyForTraffic).await;
        let rejoins = spans_named(&spans, "rdm.mesh.node.update.via-start");
        assert_eq!(rejoins.len(), 1, "one rejoin of the birth: {rejoins:?}");
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}
