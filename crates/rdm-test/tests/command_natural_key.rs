//! A birth admits a command once per natural key, never per attempt or commander
//! (node-rpc-envelope.md, "Status": "Idempotency is the natural key `(node_id, incarnation,
//! state)`, `(mesh_id, state)` or `(fabric_id, event)`. Backward transitions are refused by name";
//! "Completed steps are not rerun. Settle an idempotent uncertain step by its natural key and
//! `AlreadyApplied`"). A start's key is `(node_id, incarnation, Pending)`.
//!
//! One admin side commands the birth under several attempts of one Build: each attempt is its own
//! open command at the admin, matched by its attempt, so a completion carrying
//! another attempt resolves nothing. A natural key holds no commander, so a second physical
//! node-admin adds nothing the attempts do not already prove. A fabric checklist is scoped to the
//! accepted Build and attempt (node-rpc-envelope.md, "Fabric status command pairs") and its command comes
//! from whichever node-admin holds the mesh's primary seat: its cell moves the seat between two
//! admin sides.
//!
//! A cell that stops the birth runs in its own process (the process-wide stop flag).

use crate::common::admin_side_serving;
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId};
use rdm_admin::node_commands::CommandKey;
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{ConnectionsReceipt, Status, StatusReply, StatusRequest};
use rdm_admin::node_hooks::{HookOutcome, NodeHooks};
use rdm_test::pre_node_ready_probe::{PreNodeReadyPull, Mode, PullRequest, PullServer, TestPreNodeReady};
use rdm_test::node;
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

    /// [`Self::up`], its `pre_node_ready` hook when one is given.
    async fn up_pre_node_ready(cell: &str, pre_node_ready: Option<std::sync::Arc<TestPreNodeReady>>) -> Self {
        Self::launched(cell, pre_node_ready, rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE).await
    }

    /// [`Self::up`], launched with `deadline` as its `node.connection.delete { All }` bound.
    async fn up_bounded(cell: &str, deadline: Duration) -> Self {
        Self::launched(cell, None, deadline).await
    }

    async fn launched(cell: &str, pre_node_ready: Option<std::sync::Arc<TestPreNodeReady>>, deadline: Duration) -> Self {
        let fabric = FabricId::mint();
        let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, |b| b).await;
        let dir = std::env::temp_dir().join(format!("{cell}-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
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
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: deadline,
            declared: Default::default(),
        };
        admin.deployed(&launch, &key);
        let pull = PullServer::new(vec![]);
        pull.gate.open();
        let running = match pre_node_ready {
            None => node::start(&launch, {
                let pull = pull.clone();
                move |b, _| pull.serve(b)
            })
            .await,
            Some(h) => node::start_pre_node_ready(&launch, NodeHooks::new(h), {
                let pull = pull.clone();
                move |b, _| pull.serve(b)
            })
            .await,
        }
        .expect("the node comes up");
        let birth = Self { target: NodeTarget::ExactNode(launch.node_id.clone()), admin, launch, key, pull, dir, running };
        birth.status_heard(MemberStatus::Ready).await;
        let (d, _) = birth.admin.observer.topology_view.digest_view.get(birth.launch.node_id.as_str()).unwrap();
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

    /// Resolves once the admin side's view holds this birth at `status`: the status it set (its
    /// ready, from the birth's node.started).
    async fn status_heard(&self, status: MemberStatus) {
        let digest_view = &self.admin.observer.topology_view.digest_view;
        let mut heard = digest_view.heard_changes();
        let mut digests = digest_view.digest_changes();
        tokio::time::timeout(Duration::from_secs(30), async {
            while digest_view.get(self.launch.node_id.as_str()).is_none_or(|(d, _)| d.status != status) {
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
        if let Some(reply) = crate::common::lifecycle_reply(&self.admin.observer.client, &self.target, req, &Self::opts(), Some(&self.admin.observer.topology_view.digest_view)).await { return reply; }
        match self.admin.observer.client.call::<Status>(&self.target, req, &Self::opts()).await.0 {
            RpcOutcome::Reply(r) => r.into_value(),
            other => panic!("{} did not end in a reply: {other:?}", req.op()),
        }
    }

    /// A node command (op 0x22, i144 R22) to the birth, read through its ordered stream: every frame,
    /// or the failure that ended it.
    async fn command(&self, req: &rdm_core::node_command::NodeCommandRequest) -> Result<Vec<rdm_core::node_command::NodeCommandReply>, String> {
        crate::common::node_command(&self.admin.observer.client, &self.target, req, &Self::opts()).await
    }

    /// One call into the held op, in flight until [`PullServer::release`]; resolves once it entered.
    async fn hold_one_call(self: &std::sync::Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.pull.hold();
        let me = self.clone();
        let held = tokio::spawn(async move {
            let out = me.admin.observer.client.call::<PreNodeReadyPull>(&me.target, &PullRequest::Pull, &Self::opts()).await.0;
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

/// CONTRACT: a `StartNode` that reaches a stopped birth while another attempt's start of it is under
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
        let pre_node_ready = TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok));
        let birth = std::sync::Arc::new(Birth::up_pre_node_ready("start-natural-key", Some(pre_node_ready.clone())).await);
        let stopped = birth.call(&birth.stop(1)).await;
        assert!(matches!(stopped, StatusReply::Stopped { .. }), "{stopped:?}");

        // Attempt 2's start runs until its hook, which holds until the cell lets it go.
        let (started, resume) = (std::sync::Arc::new(tokio::sync::Notify::new()), std::sync::Arc::new(tokio::sync::Notify::new()));
        pre_node_ready.set_mode(Mode::Pause { started: started.clone(), resume: resume.clone(), outcome: HookOutcome::Ok });
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
        pre_node_ready.set_mode(Mode::Fixed(HookOutcome::Ok));
        resume.notify_one();

        assert_eq!(first.await.unwrap(), StatusReply::Started, "attempt 2's start completes");
        assert_eq!(second.await.unwrap(), StatusReply::Started, "attempt 3 answers the outcome of the start it joined");
        birth.status_heard(MemberStatus::Ready).await;
        let rejoins = spans_named(&spans, "rdm.mesh.node.update.via-start");
        assert_eq!(rejoins.len(), 1, "one rejoin of the birth: {rejoins:?}");
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}

/// CONTRACT: `node.connection.delete { All }` lets the eligible in-flight work finish under the
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
        let all = rdm_core::node_command::NodeCommandRequest::DeleteConnections { node_id: birth.launch.node_id.clone(), incarnation: birth.launch.incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
        let began = std::time::Instant::now();
        let reply = tokio::time::timeout(Duration::from_secs(10), birth.command(&all)).await.expect("the closure ends at its bound while the held call is in flight").expect("the command completes").pop().expect("its completion");
        let took = began.elapsed();
        assert!(
            took >= bound && took < Duration::from_secs(2),
            "the closure ends at the launch's 500 ms bound, never at the 5000 ms an unset environment means: it took {took:?}"
        );
        assert!(
            matches!(&reply, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { receipt: Some(ConnectionsReceipt::Deadline { last_in_flight: 1, .. }), .. }),
            "the Deadline receipt names the one call it last observed in flight, never zero: {reply:?}"
        );
        let decided = spans_named(&spans, "rdm.node_admin.connection.delete.via-delete-connections");
        assert!(
            decided.iter().any(|f| f.get("deadline_ms").map(String::as_str) == Some("500") && f.get("receipt").map(String::as_str) == Some("deadline")),
            "the birth's node.connection.delete span names the bound it ran under and the Deadline receipt: {decided:?}"
        );
        birth.pull.release();
        held.await.unwrap();
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}

/// CONTRACT: a `StopNode` that reaches a stopped birth while its start is under way is refused
/// `NotReady` naming the start under way, as a `StartNode` that reaches a birth whose stop is under
/// way is refused naming the stop: the start ends the birth's stopped cycle, so the receipt of the
/// stop that stopped it answers for a birth that is leaving Leaving. Once the start ends, the stop
/// is decided on the cycle it ended in: the started birth is stopped afresh, its connections deleted again.
/// What must NOT happen: `Stopped` with the stopped cycle's receipt while the start goes on to make
/// the birth live (its commander holding a stopped birth that is ready, and the
/// mesh-primary's NodeStopped for it published after the start's node.joined).
#[test]
fn a_stop_during_a_start_is_refused_naming_the_start_and_never_answers_the_stopped_cycles_receipt() {
    let cell = "a_stop_during_a_start_is_refused_naming_the_start_and_never_answers_the_stopped_cycles_receipt";
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        if crate::own_process::delegated(module_path!(), cell) {
            return;
        }
        let pre_node_ready = TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok));
        let birth = std::sync::Arc::new(Birth::up_pre_node_ready("stop-during-start", Some(pre_node_ready.clone())).await);
        let stopped = birth.call(&birth.stop(1)).await;
        assert!(matches!(stopped, StatusReply::Stopped { .. }), "{stopped:?}");

        // The start runs until its hook, which holds until the cell lets it go.
        let (started, resume) = (std::sync::Arc::new(tokio::sync::Notify::new()), std::sync::Arc::new(tokio::sync::Notify::new()));
        pre_node_ready.set_mode(Mode::Pause { started: started.clone(), resume: resume.clone(), outcome: HookOutcome::Ok });
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
            "a StopNode while the start is under way is refused naming the start, never answered with the stopped cycle's receipt: {during:?}"
        );

        pre_node_ready.set_mode(Mode::Fixed(HookOutcome::Ok));
        resume.notify_one();
        assert_eq!(start.await.unwrap(), StatusReply::Started);
        birth.status_heard(MemberStatus::Ready).await;

        // The started birth's stop is a stop of its own cycle.
        let after = birth.call(&birth.stop(4)).await;
        assert!(matches!(after, StatusReply::Stopped { receipt: ConnectionsReceipt::Established { .. } }), "the started birth is stopped afresh: {after:?}");
        assert!(birth.running.own.is_stopped(), "the stop that answered Stopped left the birth stopped");
        assert_eq!(spans_named(&spans, "rdm.node_admin.status.update.via-stop-node").len(), 2, "one stop per cycle: the first cycle's and the started birth's");
        let _ = std::fs::remove_dir_all(&birth.dir);
    })
}

/// CONTRACT (i144 R22; Luke Q7: "Replies (NodeCommand replies, classified into the existing stream
/// order)"): the node answers `node.connection.delete` on op 0x22 with its ordered stream. `All` with
/// no work in flight: `CommandAccepted`, `ConnectionsDeleting`, `ConnectionsDeleted` `Established`.
/// `All` with a call held in flight past the birth's 500 ms bound: `CommandAccepted`,
/// `ConnectionsDeleting`, `ConnectionsDeleted` `Deadline` naming the one call. `Connections([id])`:
/// `CommandAccepted`, `ConnectionsDeleted` naming the id not open, no progress frame. A stale
/// incarnation: one refusal frame. Must NOT happen: a progress frame for `Connections`, a completion
/// before the progress frame, or a refusal after the acceptance.
#[test]
fn the_node_answers_a_connections_delete_on_its_command_stream_in_order_for_both_scopes() {
    use rdm_core::node_command::{NodeCommandReply as N, NodeCommandRequest};
    use rdm_core::status::ConnectionScope;
    let cell = "the_node_answers_a_connections_delete_on_its_command_stream_in_order_for_both_scopes";
    let cap = crate::common::capture(cell);
    cap.run(crate::common::Threads::Workers(4), async move {
        let delete = |b: &Birth, incarnation: IncarnationId, scope: ConnectionScope| NodeCommandRequest::DeleteConnections { node_id: b.launch.node_id.clone(), incarnation, scope };
        let names = |frames: &[N]| frames.iter().map(|f| f.name()).collect::<Vec<_>>();

        let fresh = Birth::up("node-command-order").await;
        let never = rdm_core::ConnectionId::mint();
        let some = fresh.command(&delete(&fresh, fresh.launch.incarnation.clone(), ConnectionScope::Connections(vec![never.clone()]))).await.expect("Connections ends");
        assert_eq!(names(&some), ["command-accepted", "connections-deleted"], "Connections: the acceptance, then the completion, no progress frame: {some:?}");
        assert!(matches!(&some[1], N::ConnectionsDeleted { not_open, receipt: None, .. } if *not_open == vec![never.clone()]), "{some:?}");
        let stale = fresh.command(&delete(&fresh, IncarnationId::mint(), ConnectionScope::All)).await.expect("a refusal is one frame");
        assert!(matches!(stale.as_slice(), [N::RejectedStaleIncarnation { .. }]), "{stale:?}");
        let all = fresh.command(&delete(&fresh, fresh.launch.incarnation.clone(), ConnectionScope::All)).await.expect("All ends");
        assert_eq!(names(&all), ["command-accepted", "connections-deleting", "connections-deleted"], "All: acceptance, the node's report, completion: {all:?}");
        assert!(matches!(&all[2], N::ConnectionsDeleted { receipt: Some(rdm_core::status::ConnectionsReceipt::Established { .. }), .. }), "{all:?}");
        let _ = std::fs::remove_dir_all(&fresh.dir);

        let bounded = std::sync::Arc::new(Birth::up_bounded("node-command-deadline", Duration::from_millis(500)).await);
        let held = bounded.hold_one_call().await;
        let deadline = bounded.command(&delete(&bounded, bounded.launch.incarnation.clone(), ConnectionScope::All)).await.expect("All ends at its bound");
        assert_eq!(names(&deadline), ["command-accepted", "connections-deleting", "connections-deleted"], "{deadline:?}");
        assert!(matches!(&deadline[2], N::ConnectionsDeleted { receipt: Some(rdm_core::status::ConnectionsReceipt::Deadline { last_in_flight: 1, .. }), .. }), "Deadline names the call it last saw in flight: {deadline:?}");
        bounded.pull.release();
        held.await.unwrap();
        let _ = std::fs::remove_dir_all(&bounded.dir);
    })
}

