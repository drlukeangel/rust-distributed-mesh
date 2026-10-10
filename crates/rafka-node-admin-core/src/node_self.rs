//! A node birth obeying the commands of its owning mesh-admin: `drain-node`, `stop-node` and
//! `start-node` (node-drain.md, node-stop.md, node-start.md). Every node kind runs this one logic
//! over its own surface: an rpc node, a role node and a node-admin differ only in how they refuse
//! new work, how they publish and how they leave and rejoin the mesh (their [`Session`]).
//!
//! - `DrainNode`: answer `Applied` (or `AlreadyApplied` for the same operation), refuse new work,
//!   and when the eligible in-flight work reaches zero publish `node-drained` on the node's own
//!   mesh channel and call `NodeDrained` at the commanding admin. The process keeps running.
//! - `StopNode`: drain (`Draining`; the receipt is `Established` or, at the bound, `Deadline`),
//!   hard-cut the mesh connections (gossip neighbours, pooled and accepted peer connections; never
//!   the endpoint and never the commander's connection the call rides), enter `Leaving`, and answer
//!   [`StatusReply::Left`] on that same call. The node gossips nothing for the stop. The process
//!   stays alive and parked, its endpoint bound.
//! - `StartNode`: a parked birth rejoins as itself (same node id, incarnation, endpoint key and
//!   port) through its [`Session`], and answers `Started`, or `StartFailed` naming the step.
//!
//! Nothing here waits on an answer for correctness: a lost reply is the commander's indeterminate,
//! never this node's retry.

use crate::model::{IncarnationId, NodeId, PathName};
use rafka_mesh_entity::{LifecycleOp, MemberStatus, MeshDigest};
use rafka_mesh_transport::membership::{Frame, Membership};
use rafka_node_rpc::{NodeRpcClient, NodeRpcServer, NodeTarget};
use rafka_node_rpc_contract::status::{DrainReceipt, NodeState, Status, StatusReply, StatusRequest};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Whether this process is parked: it was stopped and not yet started. A parked process exits at
/// once on a termination signal (nothing is left to drain); only a delete ends it.
pub struct Parking {
    parked: std::sync::atomic::AtomicBool,
}

/// The process's parking.
pub fn parking() -> &'static Parking {
    static PARKING: std::sync::OnceLock<Parking> = std::sync::OnceLock::new();
    PARKING.get_or_init(|| Parking { parked: std::sync::atomic::AtomicBool::new(false) })
}

impl Parking {
    /// A `stop-node` completed: the process is parked.
    pub fn park(&self) {
        self.parked.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    /// A `start-node` rejoined: the process is live again.
    pub fn unpark(&self) {
        self.parked.store(false, std::sync::atomic::Ordering::SeqCst);
    }
    /// Whether the process is parked.
    pub fn is_parked(&self) -> bool {
        self.parked.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// The drain bound of a stop: `RDM_DRAIN_DEADLINE_MS`, 5000 ms when unset. Strictly shorter than
/// the stop ladder's grace.
pub fn drain_deadline_from_env() -> Duration {
    Duration::from_millis(std::env::var("RDM_DRAIN_DEADLINE_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(5000))
}

/// What a hard cut closed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CutReport {
    /// Gossip neighbours the node held on its mesh channel before the cut.
    pub neighbours: usize,
    /// Pooled peer connections this node had dialled, closed.
    pub pooled: usize,
    /// Connections peers had made to this node, closed (the commander's spared).
    pub accepted: usize,
}

/// Why a start stopped, and at which step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartFailed {
    /// `node.join`, `node.topology` or `node.hydrate`.
    pub step: &'static str,
    /// Why.
    pub reason: String,
}

/// How a process leaves its mesh when parked and joins it again when started: the part of
/// stop and start that differs by node kind.
#[async_trait::async_trait]
pub trait Session: Send + Sync {
    /// Hard-cut the mesh connections: leave the gossip topics, close the pooled peer connections and
    /// the connections peers made, sparing `spare` (the commander, whose call is in flight). The
    /// endpoint stays bound.
    async fn cut(&self, spare: Option<iroh::PublicKey>) -> CutReport;
    /// Rejoin as the same birth: join the authority, rejoin the mesh channel, take the topology and
    /// run the hydrate hook with `AfterStop` in its context under `retire`, the new cycle's token.
    /// `authority` is the admin that commanded the start: it answers the join and is the hook's
    /// accepting authority.
    async fn rejoin(&self, retire: rafka_node_rpc::CancelToken, authority: &NodeId) -> Result<(), StartFailed>;
}

/// What a node does before it stops being eligible for the seat it holds: a node-admin that holds the
/// fabric-primary seat fences its Build log and hands its committed facts to its neighbours
/// (`FabricBuildStateAdapter::yield_seat`). The argument names the command that is about to move it.
pub type YieldSeat = Arc<dyn Fn(&'static str) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// Declares a state this birth entered to its authority.
pub type Declare = Arc<dyn Fn(NodeState) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// How far this birth took a round it was commanded.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RoundProgress {
    /// The action is running.
    Acting,
    /// The action is stored; the check-in is not yet answered.
    Acted,
    /// The commander took the check-in.
    CheckedIn,
}

/// Where a birth stands between a stop and a start.
enum Cycle {
    /// Running: a stop has not been admitted.
    Live,
    /// A stop is under way; later stops of the birth wait for the same receipt.
    Stopping(tokio::sync::watch::Receiver<Option<DrainReceipt>>),
    /// Stopped and parked, with the receipt its drain established.
    Parked(DrainReceipt),
}

/// The surface a node kind gives the commands.
pub struct NodeSelf {
    /// This birth's logical node.
    pub node_id: NodeId,
    /// This birth's incarnation.
    pub incarnation: IncarnationId,
    /// This birth's `path.name`.
    pub name: PathName,
    /// This birth's Node RPC server: it refuses new calls while draining and counts the work in flight.
    pub server: NodeRpcServer,
    /// The process's one Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// This birth's membership: the mesh channel its completion frames go out on.
    pub membership: Membership,
    /// Set this birth's status and answer the digest to publish.
    pub set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync>,
    /// This birth's status now.
    pub status: Arc<dyn Fn() -> MemberStatus + Send + Sync>,
    /// Declare the state this birth entered to its authority (a node-admin of its mesh); `None` for
    /// a surface whose declarations are made elsewhere (a node-admin).
    pub declare: Option<Declare>,
    /// Run before this birth publishes `Draining` or `Leaving`: the planned hand-over's fence. The
    /// election hands a seat on at that status, so the old holder fences before it says it (R-A2:
    /// "the old holder fences claims and transfers committed Build facts BEFORE the new holder
    /// acts"). `None` for a node that holds no seat.
    pub yield_seat: Option<YieldSeat>,
    /// Cancelled when `drain-node` or `stop-node` is admitted: the birth is retired, so its
    /// `hydrate_before_ready` hook and any pull it has in flight end.
    pub retire: Option<rafka_node_rpc::Retirement>,
    /// The backbone, for a node-admin: its round hooks go out on its mesh channel and the backbone.
    /// `None` for an ordinary node: its hooks go out on its mesh channel and its mesh-admin carries
    /// them onto the backbone.
    pub backbone: Option<rafka_mesh_transport::membership::Backbone>,
    seen: Mutex<HashSet<String>>,
    /// Where this birth stands between a stop and a start.
    cycle: Mutex<Cycle>,
    /// How this process leaves and rejoins its mesh; filled once the process holds the parts.
    session: Mutex<Option<Arc<dyn Session>>>,
    /// What this birth completes before it checks in to a commit-state or open-traffic round.
    round_actions: Mutex<Arc<dyn crate::fabric_rounds::RoundActions>>,
    /// The rounds this birth took from a commander, and whether its check-in was made.
    rounds_seen: Mutex<std::collections::HashMap<(crate::fabric_state::RoundKey, NodeId), RoundProgress>>,
}

impl NodeSelf {
    /// A node surface over its parts.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        incarnation: IncarnationId,
        name: PathName,
        server: NodeRpcServer,
        client: Arc<NodeRpcClient>,
        membership: Membership,
        set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync>,
        status: Arc<dyn Fn() -> MemberStatus + Send + Sync>,
        declare: Option<Declare>,
    ) -> Self {
        Self { node_id, incarnation, name, server, client, membership, set_status, status, declare, yield_seat: None, retire: None, backbone: None, seen: Mutex::new(HashSet::new()), cycle: Mutex::new(Cycle::Live), session: Mutex::new(None), round_actions: Mutex::new(Arc::new(crate::fabric_rounds::NoScratchpad)), rounds_seen: Mutex::new(std::collections::HashMap::new()) }
    }

    /// This surface, fencing its seat before it publishes `Draining` or `Leaving`.
    pub fn with_yield_seat(mut self, yield_seat: YieldSeat) -> Self {
        self.yield_seat = Some(yield_seat);
        self
    }

    /// This surface, publishing its round hooks on `backbone` as well as its mesh channel.
    pub fn with_backbone(mut self, backbone: rafka_mesh_transport::membership::Backbone) -> Self {
        self.backbone = Some(backbone);
        self
    }

    /// This surface, cancelling `retire` when a drain or a stop is admitted.
    pub fn with_retire(mut self, retire: rafka_node_rpc::Retirement) -> Self {
        self.retire = Some(retire);
        self
    }

    /// This surface, leaving and rejoining its mesh through `session` when it is stopped and started.
    pub fn set_session(&self, session: Arc<dyn Session>) {
        *self.session.lock().unwrap() = Some(session);
    }

    /// Whether this birth is parked: stopped, its process alive, waiting for a start or a delete.
    pub fn is_parked(&self) -> bool {
        matches!(*self.cycle.lock().unwrap(), Cycle::Parked(_))
    }

    /// This surface, completing `actions` before it checks in to a round.
    pub fn with_round_actions(self, actions: Arc<dyn crate::fabric_rounds::RoundActions>) -> Self {
        *self.round_actions.lock().unwrap() = actions;
        self
    }

    /// Serve `commit-state` or `open-traffic` from `commander`, the primary of this birth's mesh
    /// (the caller resolved it). The command is admitted at once; the birth then completes its
    /// action and calls the matching completion at `commander`. A command this birth already took
    /// is answered `AlreadyApplied`, and when its check-in was made, the check-in is made again:
    /// that is how a new primary's checklist rebuilds itself.
    fn serve_round(self: &Arc<Self>, commander: &NodeId, req: &StatusRequest) -> StatusReply {
        let op = crate::fabric_state::RoundOp::of(req).expect("a round op");
        if *op.node_id != self.node_id {
            return StatusReply::RejectedNotAuthority { why: rafka_node_rpc_contract::status::NotAuthority::ReceiverNotPrimary { needed: "the subject node itself".into() } };
        }
        if *op.incarnation != self.incarnation {
            return StatusReply::RejectedStaleIncarnation { held: self.incarnation.clone() };
        }
        let want = op.kind.operation(op.fabric_id);
        if op.operation != want {
            return StatusReply::NotReady { reason: format!("{}: {} names operation {}, the round's is {want}", self.name, req.op(), op.operation) };
        }
        let key = op.key();
        let done = {
            let mut seen = self.rounds_seen.lock().unwrap();
            match seen.get(&(key.clone(), commander.clone())).copied() {
                Some(progress) => Some(progress),
                None => {
                    seen.insert((key.clone(), commander.clone()), RoundProgress::Acting);
                    None
                }
            }
        };
        let (me, commander) = (self.clone(), commander.clone());
        let parent = tracing::Span::current();
        match done {
            None => {
                tokio::spawn(async move { me.round(commander, key, true, parent).await });
                StatusReply::Applied
            }
            Some(RoundProgress::Acted | RoundProgress::CheckedIn) => {
                tokio::spawn(async move { me.round(commander, key, false, parent).await });
                StatusReply::AlreadyApplied
            }
            Some(RoundProgress::Acting) => StatusReply::AlreadyApplied,
        }
    }

    /// The round of this birth: its action (once), then the completion call at its commander.
    async fn round(self: Arc<Self>, commander: NodeId, key: crate::fabric_state::RoundKey, act: bool, parent: tracing::Span) {
        use tracing::Instrument;
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-member-round", node = %self.name, round = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, commander = %commander, acted = act, "otel.kind" = "internal");
        async {
            if act {
                let actions = self.round_actions.lock().unwrap().clone();
                if let Err(e) = crate::fabric_rounds::run_action(&*actions, &self.name, key.kind).await {
                    tracing::info_span!("rdm.node_admin.fabric.update.via-round-action-failed", node = %self.name, round = key.kind.command(), error = %e, "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("this birth's action failed: it owes no check-in"));
                    self.rounds_seen.lock().unwrap().remove(&(key.clone(), commander.clone()));
                    return;
                }
            }
            self.rounds_seen.lock().unwrap().insert((key.clone(), commander.clone()), RoundProgress::Acted);
            let req = key.kind.up(&key, self.node_id.clone(), self.incarnation.clone());
            // The check-in hook runs in the path of the completion call, before it goes out.
            let frame = crate::fabric_state::hook_frame(&key, true, self.node_id.clone(), self.incarnation.clone(), &self.name.to_string(), self.membership.clock().now_rafka_ms());
            let channels = if self.backbone.is_some() { "mesh,backbone" } else { "mesh" };
            tracing::info_span!("rdm.node_admin.fabric.update.via-round-hook", node = %self.name, round = key.kind.command(), hook = key.kind.completion(), subject_id = %self.node_id, incarnation_id = %self.incarnation.0, build_id = %key.build_id, attempt = key.attempt, channels, bound = true, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the check-in hook is published"));
            match &self.backbone {
                Some(backbone) => backbone.publish_command_hook(&frame).await,
                None => {
                    let _ = self.membership.publish_lifecycle(&frame).await;
                }
            }
            let (out, _) = self.client.call::<Status>(&NodeTarget::ExactNode(commander.clone()), &req, &rafka_node_rpc::CallOptions::default()).await;
            let answer = match &out {
                rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => r.value().name().to_string(),
                other => other.name().to_string(),
            };
            if matches!(&out, rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) if matches!(r.value(), StatusReply::Applied | StatusReply::AlreadyApplied)) {
                self.rounds_seen.lock().unwrap().insert((key.clone(), commander.clone()), RoundProgress::CheckedIn);
            }
            tracing::info_span!("rdm.node_admin.status.update.via-completion-call", node = %self.name, completion = key.kind.completion(), build_id = %key.build_id, attempt = key.attempt, commander = %commander, answer = %answer, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the completion call was made to the commanding primary"));
        }
        .instrument(span)
        .await
    }

    /// Serve `DrainNode`, `StopNode` or `StartNode` from `commander` (a node-admin the caller resolved,
    /// whose connection is `spare`), or a round command from its mesh primary. `None` for any other
    /// request.
    pub async fn serve(self: &Arc<Self>, commander: &NodeId, spare: Option<iroh::PublicKey>, req: &StatusRequest) -> Option<StatusReply> {
        if matches!(req, StatusRequest::CommitState { .. } | StatusRequest::OpenTraffic { .. }) {
            return Some(self.serve_round(commander, req));
        }
        let (node_id, incarnation, build_id, attempt, operation) = match req {
            StatusRequest::DrainNode { node_id, incarnation, build_id, attempt, operation }
            | StatusRequest::StopNode { node_id, incarnation, build_id, attempt, operation }
            | StatusRequest::StartNode { node_id, incarnation, build_id, attempt, operation } => (node_id, incarnation, build_id, attempt, operation),
            _ => return None,
        };
        if *node_id != self.node_id {
            return Some(StatusReply::RejectedNotAuthority { why: rafka_node_rpc_contract::status::NotAuthority::ReceiverNotPrimary { needed: "the subject node itself".into() } });
        }
        if *incarnation != self.incarnation {
            return Some(StatusReply::RejectedStaleIncarnation { held: self.incarnation.clone() });
        }
        let op = LifecycleOp {
            build_id: build_id.clone(),
            attempt: *attempt,
            operation: operation.clone(),
            node_id: self.node_id.clone(),
            incarnation: self.incarnation.clone(),
            name: self.name.clone(),
            event_at_rafka_ms: self.membership.clock().now_rafka_ms(),
        };
        // The command's serve span carries the caller's propagated trace; the work this command
        // starts continues it, so the spans it opens join the outer trace.
        let parent = tracing::Span::current();
        match req {
            StatusRequest::StopNode { .. } => Some(self.stop(commander.clone(), spare, op, parent).await),
            StatusRequest::StartNode { .. } => Some(self.start(commander.clone(), spare, op, parent).await),
            _ => {
                let key = format!("{build_id}/{attempt}/{operation}");
                let current = (self.status)();
                if !self.seen.lock().unwrap().insert(key.clone()) {
                    return Some(StatusReply::AlreadyApplied);
                }
                // Forward only: a node already leaving is not drained.
                if current == MemberStatus::Leaving {
                    self.seen.lock().unwrap().remove(&key);
                    return Some(StatusReply::RejectedInvalidNodeTransition { current: NodeState::Leaving });
                }
                let (me, commander) = (self.clone(), commander.clone());
                tokio::spawn(async move { me.drain(commander, op, parent).await });
                Some(StatusReply::Applied)
            }
        }
    }

    async fn drain(self: Arc<Self>, commander: NodeId, op: LifecycleOp, parent: tracing::Span) {
        use tracing::Instrument;
        let span = tracing::info_span!(
            parent: &parent,
            "rdm.node_admin.status.update.via-drain-node",
            node = %self.name, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
            commander = %commander, in_flight_at_zero = tracing::field::Empty, "otel.kind" = "internal"
        );
        async {
            if let Some(retire) = &self.retire {
                retire.cancel();
            }
            if let Some(yield_seat) = &self.yield_seat {
                yield_seat("drain-node").await;
            }
            self.server.drain();
            let draining = (self.set_status)(MemberStatus::Draining);
            let _ = self.membership.publish(&draining).await;
            // This birth's own report of the state it entered goes to its authority beside the
            // completion: neither waits for the other.
            let declared = async {
                if let Some(declare) = &self.declare {
                    declare(NodeState::Draining).await;
                }
            };
            let completion = async {
                // The work still in flight besides the calls answering the command itself.
                let stats = self.server.stats();
                loop {
                    let in_flight = rafka_node_rpc::ServerStats::get(&stats.in_flight);
                    if in_flight == 0 {
                        break;
                    }
                    let mut d = draining.clone();
                    d.in_flight = Some(in_flight);
                    let _ = self.membership.publish(&d).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                let mut d = draining.clone();
                d.in_flight = Some(0);
                let _ = self.membership.publish(&d).await;
                tracing::Span::current().record("in_flight_at_zero", true);
                self.complete(&commander, &op).await;
            };
            tokio::join!(declared, completion);
        }
        .instrument(span)
        .await
    }

    /// The stop: drain, declare `Leaving`, hard-cut the mesh connections, park. The receipt is the
    /// drain's. A second stop of the same birth, while this one runs or after it, answers the same
    /// receipt: stopping a stopped birth is done.
    async fn stop(self: &Arc<Self>, commander: NodeId, spare: Option<iroh::PublicKey>, op: LifecycleOp, parent: tracing::Span) -> StatusReply {
        use tracing::Instrument;
        enum Next {
            Done(DrainReceipt),
            Wait(tokio::sync::watch::Receiver<Option<DrainReceipt>>),
            Run(tokio::sync::watch::Sender<Option<DrainReceipt>>),
        }
        let next = {
            let mut cycle = self.cycle.lock().unwrap();
            match &*cycle {
                Cycle::Parked(r) => Next::Done(*r),
                Cycle::Stopping(rx) => Next::Wait(rx.clone()),
                Cycle::Live => {
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    *cycle = Cycle::Stopping(rx);
                    Next::Run(tx)
                }
            }
        };
        let receipt = match next {
            Next::Done(r) => r,
            Next::Wait(mut rx) => match rx.wait_for(|r| r.is_some()).await {
                Ok(r) => r.expect("waited for a receipt"),
                Err(_) => return StatusReply::NotReady { reason: format!("{}: the stop that was under way ended without a receipt", self.name) },
            },
            Next::Run(tx) => {
                let span = tracing::info_span!(
                    parent: &parent,
                    "rdm.node_admin.status.update.via-stop-node",
                    node = %self.name, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
                    commander = %commander, state = "Leaving", receipt = tracing::field::Empty, "otel.kind" = "internal"
                );
                let receipt = self.park(&commander, spare, &op).instrument(span.clone()).await;
                span.record("receipt", receipt.name());
                *self.cycle.lock().unwrap() = Cycle::Parked(receipt);
                let _ = tx.send(Some(receipt));
                receipt
            }
        };
        StatusReply::Left { receipt }
    }

    async fn park(&self, commander: &NodeId, spare: Option<iroh::PublicKey>, op: &LifecycleOp) -> DrainReceipt {
        use tracing::Instrument;
        parking().park();
        if let Some(retire) = &self.retire {
            retire.cancel();
        }
        if let Some(yield_seat) = &self.yield_seat {
            yield_seat("stop-node").await;
        }
        // 1. The whole drain: Draining is live and not routable; the receipt never turns a deadline into zero.
        let receipt = self
            .drain_for_stop()
            .instrument(tracing::info_span!("rdm.node_admin.node.drain.via-stop", node = %self.name, operation = %op.operation, receipt = tracing::field::Empty))
            .await;
        // 2. Leaving is declared to the authority while this node can still reach it, then no more is
        // said: the node gossips nothing for its stop.
        if let Some(declare) = &self.declare {
            declare(NodeState::Leaving).await;
        }
        // 3. The hard cut, sparing the commander's connection the stop call rides.
        let session = self.session.lock().unwrap().clone();
        let report = match session {
            Some(session) => session.cut(spare).instrument(tracing::info_span!("rdm.node_admin.node.connections.delete.via-stop", node = %self.name, operation = %op.operation, commander = %commander)).await,
            None => CutReport::default(),
        };
        // Leaving is this node's own status from the moment its tasks are cut; nothing publishes it.
        let _ = (self.set_status)(MemberStatus::Leaving);
        tracing::info_span!(
            "rdm.node_admin.node.connections.delete.via-stop-cut",
            node = %self.name, operation = %op.operation, neighbours = report.neighbours, pooled = report.pooled, accepted = report.accepted
        )
        .in_scope(|| tracing::info!("the mesh connections were cut; the endpoint stays bound and the commander's connection stays open"));
        receipt
    }

    /// Refuse new work and wait for the eligible in-flight work, bounded by the drain deadline. The
    /// stop call itself is in flight and is not eligible work.
    async fn drain_for_stop(&self) -> DrainReceipt {
        self.server.drain();
        let draining = (self.set_status)(MemberStatus::Draining);
        let stats = self.server.stats();
        let eligible = || rafka_node_rpc::ServerStats::get(&stats.in_flight).saturating_sub(1);
        let began = eligible();
        let until = tokio::time::Instant::now() + drain_deadline_from_env();
        let receipt = loop {
            let n = eligible();
            let mut d = draining.clone();
            d.in_flight = Some(n);
            let _ = self.membership.publish(&d).await;
            if n == 0 {
                break DrainReceipt::Established { in_flight: began };
            }
            if tokio::time::Instant::now() >= until {
                break DrainReceipt::Deadline { last_in_flight: n };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        tracing::Span::current().record("receipt", receipt.name());
        receipt
    }

    /// The start: a parked birth rejoins as itself through its session. A birth that is not parked
    /// refuses it, naming the state it is in.
    async fn start(self: &Arc<Self>, commander: NodeId, spare: Option<iroh::PublicKey>, op: LifecycleOp, parent: tracing::Span) -> StatusReply {
        use tracing::Instrument;
        let span = tracing::info_span!(
            parent: &parent,
            "rdm.node_admin.status.update.via-start-node",
            node = %self.name, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
            commander = %commander, outcome = tracing::field::Empty, failed_step = tracing::field::Empty, "otel.kind" = "internal"
        );
        let reply = async {
            let receipt = match &*self.cycle.lock().unwrap() {
                Cycle::Parked(r) => *r,
                Cycle::Stopping(_) => return StatusReply::NotReady { reason: format!("{}: a stop is still under way; start is the transition out of Leaving and only for a parked process", self.name) },
                Cycle::Live => {
                    let current = match (self.status)() {
                        MemberStatus::Pending => NodeState::Pending,
                        MemberStatus::ReadyForTraffic => NodeState::ReadyForTraffic,
                        MemberStatus::Draining => NodeState::Draining,
                        MemberStatus::Leaving => NodeState::Leaving,
                    };
                    return StatusReply::RejectedInvalidNodeTransition { current };
                }
            };
            let Some(session) = self.session.lock().unwrap().clone() else {
                return StatusReply::NotReady { reason: format!("{}: this process holds no session to rejoin its mesh with", self.name) };
            };
            let retire = match &self.retire {
                Some(r) => r.renew(),
                None => rafka_node_rpc::CancelToken::new(),
            };
            let _ = (self.set_status)(MemberStatus::Pending);
            self.server.resume();
            match session.rejoin(retire, &commander).await {
                Ok(()) => {
                    *self.cycle.lock().unwrap() = Cycle::Live;
                    parking().unpark();
                    StatusReply::Started
                }
                Err(failed) => {
                    // The start stopped at its step: the node is parked again, as it was.
                    let _ = (self.set_status)(MemberStatus::Leaving);
                    self.server.drain();
                    let _ = session.cut(spare).await;
                    tracing::Span::current().record("failed_step", failed.step);
                    let _ = receipt;
                    StatusReply::StartFailed { step: failed.step.to_string(), reason: failed.reason }
                }
            }
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", reply.name());
        reply
    }

    /// The drain's completion: the node's own gossip hook on its mesh channel, then the call at the
    /// commanding admin, while this endpoint is open.
    async fn complete(&self, commander: &NodeId, op: &LifecycleOp) {
        let frame = Frame::NodeDrained { op: op.clone(), forwarded_by: None };
        let req = StatusRequest::NodeDrained { node_id: op.node_id.clone(), incarnation: op.incarnation.clone(), build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone() };
        let kind = "node-drained";
        let _ = self.membership.publish_lifecycle(&frame).await;
        let (out, _) = self.client.call::<Status>(&NodeTarget::ExactNode(commander.clone()), &req, &rafka_node_rpc::CallOptions::default()).await;
        let answer = match &out {
            rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => r.value().name().to_string(),
            other => other.name().to_string(),
        };
        tracing::info_span!(
            "rdm.node_admin.status.update.via-completion-call",
            node = %self.name, completion = kind, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
            commander = %commander, answer = %answer, "otel.kind" = "internal"
        )
        .in_scope(|| tracing::info!("the completion call was made to the commanding admin"));
    }
}
