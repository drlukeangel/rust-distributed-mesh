//! A birth admits a command once per natural key, never per attempt or commander
//! (node-rpc-envelope.md, "Status": "Idempotency is the natural key `(node_id, incarnation,
//! state)`, `(mesh_id, state)` or `(fabric_id, event)`. Backward transitions are refused by name";
//! "Completed steps are not rerun. Settle an idempotent uncertain step by its natural key and
//! `AlreadyApplied`"). A start's key is `(node_id, incarnation, Pending)`.
//!
//! One admin side commands the birth under several attempts of one Build: each attempt is its own
//! open command in the admin's command book, matched by its attempt, so a completion carrying
//! another attempt resolves nothing. A natural key holds no commander, so a second physical
//! node-admin adds nothing the attempts do not already prove. A fabric round is scoped to the
//! accepted Build and attempt (node-rpc-envelope.md, "Fabric status rounds") and its command comes
//! from whichever node-admin holds the mesh's primary seat: its cell moves the seat between two
//! admin sides.
//!
//! A cell that stops the birth runs in its own process (the process-wide parking flag).

use crate::common::{admin_side_serving, admin_side_serving_as};
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId};
use rafka_node_admin_core::node_commands::CommandKey;
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{ConnectionsReceipt, NodeState, Status, StatusReply, StatusRequest};
use rafka_node_admin_core::app_hydration::{HookOutcome, Hydration};
use rafka_node_rpc_testkit::hydrate_probe::{HydratePull, Mode, PullRequest, PullServer, TestHydrator};
use rafka_node_rpc_testkit::node;
use std::time::Duration;

const BUILD: &str = "d9efmj2jpt75";
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
        Self::launched(cell, None, rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE).await
    }

    /// [`Self::up`], its `hydrate_before_ready` hook `hydrator` when one is given.
    async fn up_hydrating(cell: &str, hydrator: Option<std::sync::Arc<TestHydrator>>) -> Self {
        Self::launched(cell, hydrator, rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE).await
    }

    /// [`Self::up`], launched with `deadline` as its `node.connections.delete { All }` bound.
    async fn up_bounded(cell: &str, deadline: Duration) -> Self {
        Self::launched(cell, None, deadline).await
    }

    async fn launched(cell: &str, hydrator: Option<std::sync::Arc<TestHydrator>>, deadline: Duration) -> Self {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
        let dir = std::env::temp_dir().join(format!("{cell}-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
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
            connections_delete_deadline: deadline,
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

    /// Wait, bounded, until the birth's own state is `status`: what it is, for its heartbeat carries no
    /// status (gossip.md §3.1 `Digest`).
    async fn own_status(&self, status: MemberStatus) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while *self.running.status.lock().unwrap() != status {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{NODE} never became {status:?}"));
    }

    /// Resolves once the admin side's book holds this birth at `status`: the status it set (its
    /// ready-for-traffic, from the birth's node.started).
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
        CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() }
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

    fn stop(&self, attempt: u32) -> StatusRequest {
        StatusRequest::StopNode { node_id: self.launch.node_id.clone(), incarnation: self.launch.incarnation.clone(), build_id: BUILD.into(), attempt, operation: format!("node.stop:{NODE}") }
    }

    fn start(&self, attempt: u32) -> StatusRequest {
        StatusRequest::StartNode { node_id: self.launch.node_id.clone(), incarnation: self.launch.incarnation.clone(), build_id: BUILD.into(), attempt, operation: format!("node.start:{NODE}") }
    }
}

fn spans_named(spans: &crate::common::Spans, name: &str) -> Vec<std::collections::BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, _, f)| f.clone()).collect()
}

/// CONTRACT: a `StartNode` that reaches a parked birth while another attempt's start of it is under
/// way (its hook still running) is the same natural key: it runs no rejoin of its own and answers
/// that start's outcome, `Started`, once it ends; the birth is ready afterwards. What must NOT
/// happen: a second rejoin of one birth, or the second start's own failure path (Leaving, the
/// connections deleted, the transport cut) tearing down the start under way.
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
        tokio::time::timeout(Duration::from_secs(20), spans.recorded("rdm.node_admin.status.update.via-start-node", |f| f.get("joined").map(String::as_str) == Some("true")))
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

/// A round action that holds until the cell opens its gate, counting every run.
struct HeldActions {
    entered: tokio::sync::Notify,
    gate: tokio::sync::watch::Sender<bool>,
    runs: std::sync::atomic::AtomicUsize,
}

impl HeldActions {
    fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { entered: tokio::sync::Notify::new(), gate: tokio::sync::watch::Sender::new(false), runs: std::sync::atomic::AtomicUsize::new(0) })
    }
    async fn run(&self) -> Result<(), String> {
        self.runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered.notify_one();
        let mut open = self.gate.subscribe();
        let _ = open.wait_for(|o| *o).await;
        Ok(())
    }
    fn runs(&self) -> usize {
        self.runs.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl rafka_node_admin_core::fabric_rounds::RoundActions for HeldActions {
    async fn commit_state(&self, _node: &rdm_core::PathName) -> Result<(), String> {
        self.run().await
    }
    async fn open_traffic(&self, _node: &rdm_core::PathName) -> Result<(), String> {
        self.run().await
    }
}

impl Birth {
    /// `admin` announces itself holder of `mesh1`'s primary seat at `revision`; resolves once this
    /// birth holds that record.
    async fn seat(&self, admin: &crate::common::AdminSide, revision: u64) {
        let holder = rdm_core::seat::SeatHolder { mesh: "mesh1".into(), node_id: admin.launcher.node_id.clone(), incarnation: admin.launcher.incarnation.clone(), revision };
        admin.observer.membership.publish_lifecycle(&rdm_transport::membership::Frame::Seated { seat: rdm_core::seat::Seat::MeshPrimary, holder: holder.clone() }).await.expect("the seat record publishes");
        let seats = self.running.membership.seats();
        let mut changed = seats.subscribe();
        tokio::time::timeout(Duration::from_secs(20), async {
            while seats.mesh("mesh1").as_ref() != Some(&holder) {
                changed.changed().await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{NODE} never held {} as mesh1's primary at revision {revision}", admin.launcher.name));
    }

    /// The round command `kind` of `attempt` from `admin`, the birth resolved by it.
    async fn round_from(&self, admin: &crate::common::AdminSide, kind: rafka_node_admin_core::fabric_state::RoundKind, attempt: u32) -> StatusReply {
        let key = rafka_node_admin_core::fabric_state::RoundKey { kind, fabric_id: self.launch.fabric_id.clone(), build_id: BUILD.into(), attempt };
        let req = kind.down(&key, self.launch.node_id.clone(), self.launch.incarnation.clone());
        match admin.observer.client.call::<Status>(&self.target, &req, &Self::opts()).await.0 {
            RpcOutcome::Reply(r) => r.into_value(),
            other => panic!("{} from {} did not end in a reply: {other:?}", req.op(), admin.launcher.name),
        }
    }
}

/// Resolves once `admin` has taken a check-in named `completion` of `attempt` from the birth.
async fn checked_in(admin: &crate::common::AdminSide, completion: &str, attempt: u32) {
    let mut held = admin.observer.check_ins.subscribe();
    tokio::time::timeout(Duration::from_secs(20), held.wait_for(|reqs| reqs.iter().any(|r| r.op() == completion && rafka_node_admin_core::fabric_state::RoundOp::of(r).is_some_and(|o| o.attempt == attempt))))
        .await
        .unwrap_or_else(|_| panic!("{} never took a {completion} check-in of attempt {attempt}", admin.launcher.name))
        .expect("the check-ins are held by the admin side");
}

/// CONTRACT: a fabric round is scoped to the accepted Build and attempt, never to the node-admin
/// that commands it (node-rpc-envelope.md, "Fabric status rounds": "The round is scoped to the
/// accepted Build, attempt and planned exact births"; "Idempotency is the natural key"). When the
/// mesh's primary seat moves and the new holder sends the same round, the birth answers it
/// `AlreadyApplied` and its action is not run again: while the action runs the new holder is owed
/// the check-in once it is stored, and after it is stored the check-in is made to the new holder at
/// once (serve_round's contract: that is how a new primary's checklist rebuilds itself). What must
/// NOT happen: `Applied` to the new holder and a second run of the round's action.
#[test]
fn a_round_sent_again_by_the_new_seat_holder_is_the_same_round_and_its_action_runs_once() {
    let cell = "a_round_sent_again_by_the_new_seat_holder_is_the_same_round_and_its_action_runs_once";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        use rafka_node_admin_core::fabric_state::RoundKind;
        let birth = Birth::up("round-new-commander").await;
        let actions = HeldActions::new();
        birth.running.own.set_round_actions(actions.clone());

        // A second node-admin of mesh1 on the same gossip mesh, resolving the birth too.
        let admin2 = admin_side_serving_as("127.0.0.1".parse().unwrap(), &birth.launch.fabric_id, "mesh1.admin.2", MemberStatus::ReadyForTraffic, |b| b).await;
        let admin1_addr = iroh::EndpointAddr::new(birth.admin.seed.0.parse::<iroh::PublicKey>().unwrap()).with_ip_addr(birth.admin.seed.1);
        admin2.observer.membership.join_peers(vec![admin1_addr]).await.expect("admin2 joins admin1's mesh channel");
        let (d, _) = birth.admin.observer.membership.book.get(birth.launch.node_id.as_str()).unwrap();
        admin2.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let book = &birth.running.membership.book;
        let (mut heard, mut digests) = (book.heard_changes(), book.digest_changes());
        tokio::time::timeout(Duration::from_secs(20), async {
            while book.get(admin2.launcher.node_id.as_str()).is_none() {
                tokio::select! {
                    r = heard.changed() => r.unwrap(),
                    r = digests.changed() => r.unwrap(),
                }
            }
        })
        .await
        .expect("the birth hears admin2's digest");

        // commit-state: admin1 commands it and the action runs, held.
        birth.seat(&birth.admin, 1).await;
        assert_eq!(birth.round_from(&birth.admin, RoundKind::StateCommit, 1).await, StatusReply::Applied, "the round's first command is admitted");
        actions.entered.notified().await;
        // The seat moves; the new holder sends the same round while the action runs.
        birth.seat(&admin2, 2).await;
        assert_eq!(birth.round_from(&admin2, RoundKind::StateCommit, 1).await, StatusReply::AlreadyApplied, "the same round from the new seat holder is the round already taken");
        actions.gate.send_replace(true);
        checked_in(&birth.admin, "state-committed", 1).await;
        checked_in(&admin2, "state-committed", 1).await;
        assert_eq!(actions.runs(), 1, "the commit-state action runs once for the round, whoever commands it");

        // open-traffic: the action is stored before the seat moves; the new holder is checked in at once.
        birth.seat(&birth.admin, 3).await;
        assert_eq!(birth.round_from(&birth.admin, RoundKind::OpenTraffic, 1).await, StatusReply::Applied);
        checked_in(&birth.admin, "traffic-opened", 1).await;
        birth.seat(&admin2, 4).await;
        assert_eq!(birth.round_from(&admin2, RoundKind::OpenTraffic, 1).await, StatusReply::AlreadyApplied, "a stored round from the new seat holder is the round already taken");
        checked_in(&admin2, "traffic-opened", 1).await;
        assert_eq!(actions.runs(), 2, "one action per round: commit-state once and open-traffic once");
        for round in ["commit-state", "open-traffic"] {
            let acted = |a: &str| spans_named(&spans, "rdm.node_admin.fabric.update.via-member-round").into_iter().filter(|f| f.get("round").map(String::as_str) == Some(round) && f.get("acted").map(String::as_str) == Some(a)).count();
            assert_eq!((acted("true"), acted("false")), (1, 1), "{round}: one acted round for the first holder and one check-in without acting for the new holder");
        }
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}

/// CONTRACT: a member checks in with the primary that commanded it (fabric-state-commit.md, round
/// sequence: "M->>N: commit-state (exact planned birth)" then "N->>M: state-committed"; states.md,
/// "The round checklist": "A new primary, through election or recovery, re-sends the current round.
/// Members already done check in again, and the checklist rebuilds itself"). The seat moved to
/// admin2, which commands the birth at once; the birth has not yet heard the seat move, so its seat
/// book still names admin1. The command is admitted and the birth's check-in goes to admin2, the
/// commander the call came from. What must NOT happen: the check-in addressed to the holder the
/// birth's seat book names, which no longer holds the round, refuses it `RejectedNotAuthority` and
/// never asks again: admin2's command was answered `Applied`, so it never re-sends and its
/// checklist never completes (gate red, tree b24a4303, mesh_elections__sticky_seat trace
/// 6bd35dbd: mesh1.rpc.1/2/3 checked in at the old holder 625kzaz0h98j, rejected-not-authority).
#[test]
fn a_round_command_from_a_new_primary_the_birth_has_not_heard_take_the_seat_is_checked_in_to_that_commander() {
    let cell = "a_round_command_from_a_new_primary_the_birth_has_not_heard_take_the_seat_is_checked_in_to_that_commander";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        use rafka_node_admin_core::fabric_state::RoundKind;
        let birth = Birth::up("round-unheard-commander").await;
        let actions = HeldActions::new();
        actions.gate.send_replace(true);
        birth.running.own.set_round_actions(actions.clone());

        // A second node-admin of mesh1 on the same gossip mesh, resolving the birth, and carried in
        // admin1's member projection, so the birth names a call from it by its endpoint.
        let admin2 = admin_side_serving_as("127.0.0.1".parse().unwrap(), &birth.launch.fabric_id, "mesh1.admin.2", MemberStatus::ReadyForTraffic, |b| b).await;
        let admin1_addr = iroh::EndpointAddr::new(birth.admin.seed.0.parse::<iroh::PublicKey>().unwrap()).with_ip_addr(birth.admin.seed.1);
        admin2.observer.membership.join_peers(vec![admin1_addr]).await.expect("admin2 joins admin1's mesh channel");
        let (d, _) = birth.admin.observer.membership.book.get(birth.launch.node_id.as_str()).unwrap();
        admin2.observer.resolver.insert(ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr });
        let _carried = birth.admin.carry_in_projection(&admin2);
        // The new primary has been a member all along: the birth hears its digest (its resolver dials it).
        let book = &birth.running.membership.book;
        let (mut heard, mut digests) = (book.heard_changes(), book.digest_changes());
        tokio::time::timeout(Duration::from_secs(20), async {
            while book.get(admin2.launcher.node_id.as_str()).is_none() {
                tokio::select! {
                    r = heard.changed() => r.unwrap(),
                    r = digests.changed() => r.unwrap(),
                }
            }
        })
        .await
        .expect("the birth hears admin2's digest");
        let admin2_endpoint = admin2.own_digest().node.endpoint_id.0.clone();
        tokio::time::timeout(Duration::from_secs(20), async {
            while birth.running.membership.projected_member_at(&admin2_endpoint).is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the birth's member projection names admin2 at its endpoint");

        // The birth's seat book names admin1; admin2, which took the seat, commands the round.
        birth.seat(&birth.admin, 1).await;
        assert_eq!(birth.round_from(&admin2, RoundKind::StateCommit, 1).await, StatusReply::Applied, "the round's first command is admitted, whoever sends it");
        checked_in(&admin2, "state-committed", 1).await;
        let at_admin1 = birth.admin.observer.check_ins.borrow().iter().filter(|r| r.op() == "state-committed").count();
        assert_eq!(at_admin1, 0, "the check-in went to the commander, never to the holder the birth's seat book names");
        // The completion call's span closes once its reply is back at the birth, after admin2 took it.
        let admin2_id = admin2.launcher.node_id.to_string();
        let named_admin2 = || spans_named(&spans, "rdm.node_admin.status.update.via-completion-call").into_iter().any(|c| c.get("commander") == Some(&admin2_id) && c.get("answer").map(String::as_str) == Some("applied"));
        tokio::time::timeout(Duration::from_secs(10), async {
            while !named_admin2() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the completion call names admin2 as its commander and was applied: {:?}", spans_named(&spans, "rdm.node_admin.status.update.via-completion-call")));
        assert_eq!(actions.runs(), 1, "the action ran once");
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}

/// CONTRACT: `node.connections.delete { All }` lets the eligible in-flight work finish under the
/// birth's bound (node-connections-delete.md step 5), the one its launch carries
/// (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`, read once where the process reads its launch): a call
/// that never finishes ends the closure at that bound with `Deadline` naming the count it last
/// observed, never zero, and the process keeps running. The birth is launched with a 500 ms bound
/// and the process environment names none. What must NOT happen: `Established` while the call is
/// held, a closure that never ends while the call is held, or a closure that runs to any bound but
/// the launch's (the 5000 ms default an unset environment means).
#[test]
fn an_all_with_work_that_never_finishes_ends_at_its_bound_with_a_deadline_receipt() {
    let cell = "an_all_with_work_that_never_finishes_ends_at_its_bound_with_a_deadline_receipt";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        assert!(std::env::var_os("RDM_CONNECTIONS_DELETE_DEADLINE_MS").is_none(), "this binary's process environment names no connections-delete bound");
        let bound = Duration::from_millis(500);
        let birth = std::sync::Arc::new(Birth::up_bounded("connections-delete-bound", bound).await);
        let held = birth.hold_one_call().await;
        let all = StatusRequest::DeleteConnections { node_id: birth.launch.node_id.clone(), incarnation: birth.launch.incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
        let began = std::time::Instant::now();
        let reply = tokio::time::timeout(Duration::from_secs(10), birth.call(&all)).await.expect("the closure ends at its bound while the held call is in flight");
        let took = began.elapsed();
        assert!(
            took >= bound && took < Duration::from_secs(2),
            "the closure ends at the launch's 500 ms bound, never at the 5000 ms an unset environment means: it took {took:?}"
        );
        assert!(
            matches!(&reply, StatusReply::ConnectionsDeleted { receipt: Some(ConnectionsReceipt::Deadline { last_in_flight: 1, .. }), .. }),
            "the Deadline receipt names the one call it last observed in flight, never zero: {reply:?}"
        );
        let decided = spans_named(&spans, "rdm.node_admin.node.connections.delete.via-delete-connections");
        assert!(
            decided.iter().any(|f| f.get("deadline_ms").map(String::as_str) == Some("500") && f.get("receipt").map(String::as_str) == Some("deadline")),
            "the birth's node.connections.delete span names the bound it ran under and the Deadline receipt: {decided:?}"
        );
        birth.pull.release();
        held.await.unwrap();
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}

/// CONTRACT: a `StopNode` that reaches a parked birth while its start is under way is refused
/// `NotReady` naming the start under way, as a `StartNode` that reaches a birth whose stop is under
/// way is refused naming the stop: the start ends the birth's stopped cycle, so the receipt of the
/// stop that parked it answers for a birth that is leaving Leaving. Once the start ends, the stop
/// is decided on the cycle it ended in: the started birth is stopped afresh, its connections deleted again.
/// What must NOT happen: `Stopped` with the parked cycle's receipt while the start goes on to make
/// the birth live (its commander holding a stopped birth that is ready for traffic, and the
/// mesh-primary's NodeStopped for it published after the start's node.joined).
#[test]
fn a_stop_during_a_start_is_refused_naming_the_start_and_never_answers_the_parked_cycles_receipt() {
    let cell = "a_stop_during_a_start_is_refused_naming_the_start_and_never_answers_the_parked_cycles_receipt";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let hydrator = TestHydrator::new(Mode::Fixed(HookOutcome::Ok));
        let birth = std::sync::Arc::new(Birth::up_hydrating("stop-during-start", Some(hydrator.clone())).await);
        let stopped = birth.call(&birth.stop(1)).await;
        assert!(matches!(stopped, StatusReply::Stopped { .. }), "{stopped:?}");

        // The start runs until its hook, which holds until the cell lets it go.
        let (started, resume) = (std::sync::Arc::new(tokio::sync::Notify::new()), std::sync::Arc::new(tokio::sync::Notify::new()));
        hydrator.set_mode(Mode::Pause { started: started.clone(), resume: resume.clone(), outcome: HookOutcome::Ok });
        birth.admin.deployed(&birth.launch, &birth.key);
        let start = {
            let birth = birth.clone();
            tokio::spawn(async move { birth.call(&birth.start(2)).await })
        };
        started.notified().await;

        // A stop of the birth while its start is under way.
        let during = birth.call(&birth.stop(3)).await;
        assert!(
            matches!(&during, StatusReply::NotReady { reason } if reason.contains("start is under way")),
            "a StopNode while the start is under way is refused naming the start, never answered with the parked cycle's receipt: {during:?}"
        );

        hydrator.set_mode(Mode::Fixed(HookOutcome::Ok));
        resume.notify_one();
        assert_eq!(start.await.unwrap(), StatusReply::Started);
        birth.status_heard(MemberStatus::ReadyForTraffic).await;

        // The started birth's stop is a stop of its own cycle.
        let after = birth.call(&birth.stop(4)).await;
        assert!(matches!(after, StatusReply::Stopped { receipt: ConnectionsReceipt::Established { .. } }), "the started birth is stopped afresh: {after:?}");
        assert!(birth.running.own.is_parked(), "the stop that answered Stopped left the birth parked");
        assert_eq!(spans_named(&spans, "rdm.node_admin.status.update.via-stop-node").len(), 2, "one stop per cycle: the first cycle's and the started birth's");
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}
