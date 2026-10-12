//! A node birth obeying the commands of its owning mesh-admin: `drain-node`, `stop-node` and
//! `start-node` (node-drain.md, node-stop.md, node-start.md). Every node kind runs this one logic
//! over its own surface: an rpc node, a role node and a node-admin differ only in how they refuse
//! new work, how they publish and how they leave and rejoin the mesh (their [`Session`]).
//!
//! - `DrainNode`: answer `Applied`, refuse new work, and observe the eligible in-flight work under
//!   the drain bound: when it reaches zero (`Established`) publish `node-drained` on the node's own
//!   mesh channel and call `NodeDrained` at the commanding admin; at the bound (`Deadline`, naming
//!   the count last observed) the drain ends not drained and owes no `node-drained`. The process
//!   keeps running and the birth stays Draining either way. Admission is keyed on the natural key
//!   `(node_id, incarnation, Draining)`, never the attempt (node-rpc-envelope.md, "Status"): a
//!   `DrainNode` of any attempt or commander for a birth already draining, or drained, in this cycle
//!   answers `AlreadyApplied` and runs no drain of its own; its commander is owed `node-drained`
//!   naming its own operation, when the drain reaches zero or at once when it already has.
//! - `StopNode`: drain (`Draining`; the receipt is `Established` or, at the bound, `Deadline`),
//!   hold the node's own tasks (no turn begins; a turn already past its wait point runs to its end)
//!   beside the owed `node-drained` completions, declare `Leaving` to the authority, publish a
//!   mesh-primary node-admin's last aggregate with itself `Leaving` (gossip.md, "Mesh delete"),
//!   hard-cut the mesh connections (gossip neighbours, pooled and accepted peer connections; never
//!   the endpoint and never the commander's connection the call rides), enter `Leaving`, and answer
//!   [`StatusReply::Stopped`] on that same call. The node authors no departure gossip for the stop.
//!   The process stays alive and parked, its endpoint bound.
//! - `StartNode`: a parked birth rejoins as itself (same node id, incarnation, endpoint key and
//!   port) through its [`Session`], and answers `Started`, or `StartFailed` naming the step.
//!
//! Nothing here waits on an answer for correctness: a lost reply is the commander's indeterminate,
//! never this node's retry.

use crate::model::{IncarnationId, NodeId, PathName};
use rafka_mesh_entity::{LifecycleOp, MemberStatus, MeshDigest};
use rafka_mesh_transport::membership::{Frame, Membership};
use rafka_node_rpc::{NodeRpcClient, NodeRpcServer, NodeTarget};
use rafka_node_rpc_contract::status::{ConnectionScope, DrainReceipt, NodeState, Status, StatusReply, StatusRequest};
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

/// The drain bound of a drain and of a stop's drain: `RDM_DRAIN_DEADLINE_MS`, 5000 ms when unset.
/// Strictly shorter than the stop ladder's grace.
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
    /// Turns of the node's gated tasks in progress when the cut began (a node-admin's park gate,
    /// quiesced by the stop before it; an ordinary node holds none).
    pub turns_in_progress: usize,
}

/// What holding a node's own tasks for its stop waited for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TasksParked {
    /// Turns of its gated tasks already past their wait point when they were held; the stop waited
    /// for each to end.
    pub turns_in_progress: usize,
    /// Those turns by task (`task` or `task:n`, comma-separated); empty when there were none.
    pub waited_for: String,
}

/// What a `node.connections.delete` did at this birth (R-CR3): the connections it cut, and every
/// connection id it accounts for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Deleted {
    /// What the hard cut closed.
    pub report: CutReport,
    /// The open entries cut, their drops written under the name the birth holds.
    pub closed: Vec<rafka_mesh_entity::ConnectionId>,
    /// The ids named that are no open entry of the birth: unknown, or already closed.
    pub not_open: Vec<rafka_mesh_entity::ConnectionId>,
    /// The open entries the birth still holds after the cut, a drop the storage refused included.
    pub held: Vec<rafka_mesh_entity::ConnectionId>,
}

/// The reason every drop `node.connections.delete` writes carries.
pub const CONNECTIONS_DELETE: &str = "node.connections.delete";

/// Why a start stopped, and at which step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartFailed {
    /// `node.join`, `node.hydrate`, `node.topology` or `node.rafka-time`.
    pub step: &'static str,
    /// Why.
    pub reason: String,
}

impl std::fmt::Display for StartFailed {
    /// The reason: it names the node, the step's authority and what failed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for StartFailed {}

/// A first start whose refusal is a string (a node-admin's) carries the step's reason.
impl From<StartFailed> for String {
    fn from(failed: StartFailed) -> Self {
        failed.reason
    }
}

/// How a process leaves its mesh when parked and joins it again when started: the part of
/// stop and start that differs by node kind.
#[async_trait::async_trait]
pub trait Session: Send + Sync {
    /// Hold this process's own tasks for its stop, once its drain is done: no turn begins, and this
    /// resolves once every turn already past its wait point has ended, so nothing a turn does
    /// follows the stop's declaration or its cut. A surface whose tasks need no holding has nothing
    /// to wait for.
    async fn park_tasks(&self) -> TasksParked {
        TasksParked::default()
    }
    /// Hard-cut the mesh connections: leave the gossip topics, close the pooled peer connections and
    /// the connections peers made, sparing `spare` (the commander, whose call is in flight). The
    /// endpoint stays bound.
    async fn cut(&self, spare: Option<iroh::PublicKey>) -> CutReport;
    /// Rejoin as the same birth: join the authority, rejoin the mesh channel, take the topology and
    /// run the hydrate hook with `AfterStop` in its context under `retire`, the new cycle's token.
    /// `commander` is the endpoint key of the call that commanded the start: the admin that holds it
    /// answers the join and is the hook's accepting authority
    /// ([`crate::workflows::node_start::join_back`]). `rows` is the start's hold on this birth's connections rows: the
    /// session drops it once it holds the name its join answer handed, and no earlier, so the join's
    /// connection is recorded under that name (node-start.md step 2).
    async fn rejoin(&self, retire: rafka_node_rpc::CancelToken, commander: iroh::PublicKey, rows: Option<crate::connections_writer::RowsHeld>) -> Result<(), StartFailed>;
}

/// Declares a state this birth entered to its authority.
pub type Declare = Arc<dyn Fn(NodeState) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// How far this birth took a round, keyed by the round alone: its kind in its fabric for the
/// accepted Build and attempt (node-rpc-envelope.md, "Fabric status rounds"), never the node-admin
/// that commanded it.
enum RoundProgress {
    /// The action is running for `first`, the commander whose command started it: every other
    /// commander that sent the round meanwhile (a new holder of the mesh's primary seat), with the
    /// serve span of its command, is owed the check-in once the action is stored.
    Acting {
        /// The commander whose command started the action.
        first: NodeId,
        /// The commanders owed the check-in when it is stored.
        joined: Vec<(NodeId, tracing::Span)>,
    },
    /// The action is stored: a commander that sends the round is checked in at once.
    Acted,
}

/// Where a birth stands between a stop and a start.
enum Cycle {
    /// Running: a stop has not been admitted.
    Live,
    /// A stop is under way; later stops of the birth wait for the same receipt.
    Stopping(tokio::sync::watch::Receiver<Option<DrainReceipt>>),
    /// Stopped and parked, with the receipt its drain established.
    Parked(DrainReceipt),
    /// Parked and a start is under way (the natural key `(node_id, incarnation, Pending)`): later
    /// starts of the birth answer the same outcome, and a stop is refused naming the start. A failed
    /// start parks the birth again with the receipt it was parked with.
    Starting {
        /// The outcome of the start under way, once it ends.
        outcome: tokio::sync::watch::Receiver<Option<StatusReply>>,
    },
}

/// This birth's drain in its current cycle (live since its birth or its last start). Its natural
/// key is `(node_id, incarnation, Draining)` (node-rpc-envelope.md, "Status"): one drain per cycle,
/// whichever attempt or commander asks for it.
enum Drain {
    /// Under way (a `DrainNode`'s drain, or a stop's): every commander that asked, with its own
    /// operation, is owed `node-drained` when the eligible in-flight work reaches zero.
    Running(Vec<Owed>),
    /// The drain ended with its receipt: `Established` when the eligible in-flight work reached
    /// zero (a commander that asks now is owed `node-drained` at once), `Deadline` at the drain
    /// bound (not drained: a commander that asks now is owed none).
    Ended(DrainReceipt),
}

/// A commander owed `node-drained` by this cycle's drain.
struct Owed {
    /// The admin whose `DrainNode` asked, as the call resolved it; `None` when this birth held no
    /// birth for the calling endpoint then (it is read from the book when the completion is due).
    commander: Option<NodeId>,
    /// The endpoint the `DrainNode` came from, as the transport proved it.
    endpoint: Option<iroh::PublicKey>,
    /// Its operation: the completion names its own Build, attempt and operation.
    op: LifecycleOp,
    /// The serve span of its `DrainNode`: the completion call continues that command's trace,
    /// whichever drain (the one it started, one it joined, or a stop's) reached zero.
    trace: tracing::Span,
}

impl Owed {
    /// The completion call this commander is owed, in its own command's trace.
    async fn complete(self, me: &NodeSelf) {
        use tracing::Instrument;
        me.complete(self.commander.as_ref(), self.endpoint, &self.op).instrument(self.trace).await
    }
}

/// How a `DrainNode` was admitted.
enum DrainAdmission {
    /// The first of this cycle: its drain runs.
    Start,
    /// A drain is under way: this commander is owed its completion when it ends.
    Joined,
    /// The birth is drained: this commander is owed its completion now.
    Drained,
    /// The drain ended at its bound, not drained: this commander is owed no completion.
    EndedAtBound,
}

/// The surface a node kind gives the commands.
pub struct NodeSelf {
    /// This birth's logical node.
    pub node_id: NodeId,
    /// This birth's incarnation.
    pub incarnation: IncarnationId,
    /// This birth's `path.name` at its birth; the name it holds is [`Self::held_name`].
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
    /// Cancelled when `drain-node` or `stop-node` is admitted: the birth is retired, so its
    /// `hydrate_before_ready` hook and any pull it has in flight end.
    pub retire: Option<rafka_node_rpc::Retirement>,
    /// The backbone, for a node-admin: its round hooks go out on its mesh channel and the backbone.
    /// `None` for an ordinary node: its hooks go out on its mesh channel and its mesh-admin carries
    /// them onto the backbone.
    pub backbone: Option<rafka_mesh_transport::membership::Backbone>,
    /// This birth's drain in its current cycle; `None` until a drain or a stop is admitted.
    drain: Mutex<Option<Drain>>,
    /// Where this birth stands between a stop and a start.
    cycle: Mutex<Cycle>,
    /// How this process leaves and rejoins its mesh; filled once the process holds the parts.
    session: Mutex<Option<Arc<dyn Session>>>,
    /// The process's connections writer: what this birth holds (R-CR1) and where the drops of a
    /// `node.connections.delete` are written; filled once the process holds it.
    connections: Mutex<Option<Arc<crate::connections_writer::ConnectionsWriter>>>,
    /// What this birth completes before it checks in to a commit-state or open-traffic round.
    round_actions: Mutex<Arc<dyn crate::fabric_rounds::RoundActions>>,
    /// The rounds this birth took, by round, and how far it took each.
    rounds_seen: Mutex<std::collections::HashMap<crate::fabric_state::RoundKey, RoundProgress>>,
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
        Self { node_id, incarnation, name, server, client, membership, set_status, status, declare, retire: None, backbone: None, drain: Mutex::new(None), cycle: Mutex::new(Cycle::Live), session: Mutex::new(None), connections: Mutex::new(None), round_actions: Mutex::new(Arc::new(crate::fabric_rounds::NoScratchpad)), rounds_seen: Mutex::new(std::collections::HashMap::new()) }
    }

    /// The name this birth holds and publishes: its row's name once it took one (live through
    /// `ApplyNodeName`, or from its join answer), else its birth's (R12). Every span, refusal and
    /// frame of this surface names it.
    pub fn held_name(&self) -> PathName {
        self.membership.held_name(&self.name)
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

    /// This surface, holding the process's connections writer: what this birth holds and where a
    /// `node.connections.delete` writes its drops.
    pub fn set_connections(&self, connections: Arc<crate::connections_writer::ConnectionsWriter>) {
        *self.connections.lock().unwrap() = Some(connections);
    }

    /// The connections this birth holds now (R-CR1), from its connections writer; refused by name
    /// when the process has not handed it one, so nothing is decided on a set it cannot read.
    pub async fn held_routes(&self) -> Result<Vec<rafka_mesh_entity::connections::NodeConnection>, String> {
        let writer = self.connections.lock().unwrap().clone();
        match writer {
            Some(w) => Ok(w.held_routes().await),
            None => Err(format!("{}: this birth holds no connections writer, so the connections it holds cannot be read", self.held_name())),
        }
    }

    /// `node.connections.get` at this birth (fabric-mesh-node-lifecycles.md: "Reading or cutting a
    /// node's held connections"): the connections it holds now, under the name it holds, once every
    /// observation handed to its writer has landed; the ids are those `node.connections.delete` names.
    pub async fn connections_get(&self) -> StatusReply {
        let writer = self.connections.lock().unwrap().clone();
        match writer {
            Some(w) => crate::views::connections_answer(&w.held_connections().await),
            None => StatusReply::NotReady { reason: format!("{}: this birth holds no connections writer, so the connections it holds cannot be read", self.held_name()) },
        }
    }

    /// `node.connections.delete` at this birth (R-CR3; node-stop.md step 3 is its `All`). `All` is
    /// the stop's cut through the [`Session`] (gossip neighbours, pooled and accepted peer
    /// connections, sparing `spare`, the commander whose call is in flight); `Connections` cuts the
    /// pooled and accepted connections of the routes whose open entry it names, and only those, never
    /// the commander's own connection. Each route cut gets its drop, written under the name the birth
    /// holds; an id that is no open entry is named `not_open`. The endpoint stays bound.
    pub async fn delete_connections(&self, spare: Option<iroh::PublicKey>, scope: &ConnectionScope) -> Deleted {
        let writer = self.connections.lock().unwrap().clone();
        let (report, routes, not_open) = match scope {
            ConnectionScope::All => {
                let session = self.session.lock().unwrap().clone();
                let report = match session {
                    Some(session) => session.cut(spare).await,
                    None => CutReport::default(),
                };
                // What the cut leaves open in the log is dropped too, the commander's spared stream
                // included: nothing routes on this birth's connections once the call is answered.
                let routes = match &writer {
                    Some(w) => w.held_routes().await,
                    None => Vec::new(),
                };
                (report, routes, Vec::new())
            }
            ConnectionScope::Connections(ids) => {
                let open = match &writer {
                    Some(w) => w.held_routes().await,
                    None => Vec::new(),
                };
                let mut report = CutReport::default();
                let (mut routes, mut not_open) = (Vec::new(), Vec::new());
                for id in ids {
                    match open.iter().find(|r| &r.id == id) {
                        Some(route) if !routes.iter().any(|r: &rafka_mesh_entity::connections::NodeConnection| r.id == route.id) => {
                            // A Proxy rides its carrier's Direct, which is its own entry: only the
                            // route is retired. A Direct's connections both ways are cut.
                            if route.kind == rafka_mesh_entity::connections::ConnectionKind::Direct {
                                if let Some(peer) = self.client.endpoint_of(&route.destination.node_id) {
                                    report.pooled += self.client.close_pooled_to(&peer, CONNECTIONS_DELETE);
                                    report.accepted += self.server.close_connections_from(peer, spare, CONNECTIONS_DELETE);
                                }
                            }
                            routes.push(route.clone());
                        }
                        Some(_) => {}
                        None => not_open.push(id.clone()),
                    }
                }
                (report, routes, not_open)
            }
        };
        let (closed, refused) = match &writer {
            Some(w) => w.write_drops(&routes, CONNECTIONS_DELETE).await,
            None => (Vec::new(), Vec::new()),
        };
        for (id, why) in &refused {
            tracing::info_span!("rdm.node_admin.node.connections.reject.via-refused-drop", node = %self.held_name(), entry = %id, refusal = %why)
                .in_scope(|| tracing::info!("the connection was cut and its drop was refused by storage: the log still holds it open"));
        }
        let held = match &writer {
            Some(w) => w.held_routes().await.into_iter().map(|r| r.id).collect(),
            None => Vec::new(),
        };
        Deleted { report, closed, not_open, held }
    }

    /// Serve `DeleteConnections` from `commander` (its connection `spare`): a scope that names
    /// nothing to cut is refused by name and cuts nothing; any other is the cut, answered
    /// `node.connections.deleted`. One span names the scope and every count.
    async fn serve_delete_connections(&self, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, scope: &ConnectionScope) -> StatusReply {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "rdm.node_admin.node.connections.delete.via-delete-connections",
            node = %self.held_name(), commander = %named(commander), scope = scope.name(), named = match scope { ConnectionScope::All => 0, ConnectionScope::Connections(ids) => ids.len() },
            closed = tracing::field::Empty, not_open = tracing::field::Empty, held = tracing::field::Empty, neighbours = tracing::field::Empty, pooled = tracing::field::Empty, accepted = tracing::field::Empty,
            outcome = tracing::field::Empty, reason = tracing::field::Empty, "otel.kind" = "internal"
        );
        let reply = async {
            if self.connections.lock().unwrap().is_none() {
                return StatusReply::NotReady { reason: format!("{}: this birth holds no connections writer yet; nothing is cut", self.held_name()) };
            }
            if let Some(reason) = scope.refusal() {
                tracing::Span::current().record("reason", reason.as_str());
                return StatusReply::RejectedConnectionScope { reason: format!("{}: {reason}", self.held_name()) };
            }
            let d = self.delete_connections(spare, scope).await;
            let current = tracing::Span::current();
            current.record("closed", d.closed.len()).record("not_open", d.not_open.len()).record("held", d.held.len());
            current.record("neighbours", d.report.neighbours).record("pooled", d.report.pooled).record("accepted", d.report.accepted);
            StatusReply::ConnectionsDeleted { closed: d.closed, not_open: d.not_open, held: d.held }
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("node.connections.delete decided at this birth"));
        reply
    }

    /// Whether this birth is parked: stopped, its process alive, waiting for a start or a delete.
    /// A birth whose start is under way is parked until the start ends.
    pub fn is_parked(&self) -> bool {
        matches!(*self.cycle.lock().unwrap(), Cycle::Parked(_) | Cycle::Starting { .. })
    }

    /// This surface, completing `actions` before it checks in to a round.
    pub fn with_round_actions(self, actions: Arc<dyn crate::fabric_rounds::RoundActions>) -> Self {
        self.set_round_actions(actions);
        self
    }

    /// Complete `actions` before checking in to a round, from the next round this birth takes.
    pub fn set_round_actions(&self, actions: Arc<dyn crate::fabric_rounds::RoundActions>) {
        *self.round_actions.lock().unwrap() = actions;
    }

    /// Serve `commit-state` or `open-traffic` from `commander`, the primary of this birth's mesh
    /// (the caller resolved it). The command is admitted at once; the birth then completes its
    /// action and calls the matching completion at `commander`. A round this birth already took,
    /// whichever commander sends it, is answered `AlreadyApplied` and its action is not run again:
    /// the check-in is made to the commander that sends it, at once when the action is stored and
    /// otherwise once it is. That is how a new primary's checklist rebuilds itself.
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
            return StatusReply::NotReady { reason: format!("{}: {} names operation {}, the round's is {want}", self.held_name(), req.op(), op.operation) };
        }
        let key = op.key();
        let parent = tracing::Span::current();
        enum Admit {
            Act,
            Joined,
            CheckIn,
        }
        let admit = {
            let mut seen = self.rounds_seen.lock().unwrap();
            match seen.get_mut(&key) {
                None => {
                    seen.insert(key.clone(), RoundProgress::Acting { first: commander.clone(), joined: Vec::new() });
                    Admit::Act
                }
                Some(RoundProgress::Acting { first, joined }) => {
                    if first != commander && !joined.iter().any(|(c, _)| c == commander) {
                        joined.push((commander.clone(), parent.clone()));
                    }
                    Admit::Joined
                }
                Some(RoundProgress::Acted) => Admit::CheckIn,
            }
        };
        let (me, commander) = (self.clone(), commander.clone());
        match admit {
            Admit::Act => {
                tokio::spawn(async move { me.round(commander, key, parent).await });
                StatusReply::Applied
            }
            Admit::CheckIn => {
                tokio::spawn(async move { me.check_in_again(commander, key, parent).await });
                StatusReply::AlreadyApplied
            }
            Admit::Joined => StatusReply::AlreadyApplied,
        }
    }

    /// The round of this birth: its action (once per round), then the completion call at its
    /// commander and at every commander that sent the round while the action ran.
    async fn round(self: Arc<Self>, commander: NodeId, key: crate::fabric_state::RoundKey, parent: tracing::Span) {
        use tracing::Instrument;
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-member-round", node = %self.held_name(), round = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, commander = %commander, acted = true, "otel.kind" = "internal");
        async {
            let actions = self.round_actions.lock().unwrap().clone();
            if let Err(e) = crate::fabric_rounds::run_action(&*actions, &self.held_name(), key.kind).await {
                tracing::info_span!("rdm.node_admin.fabric.update.via-round-action-failed", node = %self.held_name(), round = key.kind.command(), error = %e, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("this birth's action failed: it owes no check-in"));
                // Nobody is owed a check-in; the next command of the round runs the action afresh.
                self.rounds_seen.lock().unwrap().remove(&key);
                return;
            }
            let joined = match self.rounds_seen.lock().unwrap().insert(key.clone(), RoundProgress::Acted) {
                Some(RoundProgress::Acting { joined, .. }) => joined,
                _ => Vec::new(),
            };
            // Each commander owed is checked in on its own: one that cannot be reached holds up no other.
            for (joiner, parent) in joined {
                let (me, key) = (self.clone(), key.clone());
                tokio::spawn(async move { me.check_in_again(joiner, key, parent).await });
            }
            self.check_in(&commander, &key).await;
        }
        .instrument(span)
        .await
    }

    /// The check-in of a round whose action this birth already stored, made to `commander`: the
    /// action is not run again.
    async fn check_in_again(self: Arc<Self>, commander: NodeId, key: crate::fabric_state::RoundKey, parent: tracing::Span) {
        use tracing::Instrument;
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-member-round", node = %self.held_name(), round = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, commander = %commander, acted = false, "otel.kind" = "internal");
        self.check_in(&commander, &key).instrument(span).await
    }

    /// The check-in hook, then the completion call at `commander`, naming `key`'s round.
    async fn check_in(&self, commander: &NodeId, key: &crate::fabric_state::RoundKey) {
        let req = key.kind.up(key, self.node_id.clone(), self.incarnation.clone());
        // The check-in hook runs in the path of the completion call, before it goes out.
        let frame = crate::fabric_state::hook_frame(key, true, self.node_id.clone(), self.incarnation.clone(), &self.held_name().to_string(), self.membership.clock().now_rafka_ms());
        let channels = if self.backbone.is_some() { "mesh,backbone" } else { "mesh" };
        tracing::info_span!("rdm.node_admin.fabric.update.via-round-hook", node = %self.held_name(), round = key.kind.command(), hook = key.kind.completion(), subject_id = %self.node_id, incarnation_id = %self.incarnation.0, build_id = %key.build_id, attempt = key.attempt, channels, bound = true, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the check-in hook is published"));
        // What became of the hook is named on the call's span: handed to gossip, or why not.
        let hook = match &self.backbone {
            Some(backbone) => backbone.publish_command_hook(&frame).await,
            None => self.membership.publish_lifecycle(&frame).await,
        };
        let hook = match hook {
            Ok(()) => "published".to_string(),
            Err(e) => format!("not-published: {e}"),
        };
        let (out, _) = self.client.call::<Status>(&NodeTarget::ExactNode(commander.clone()), &req, &rafka_node_rpc::CallOptions::default()).await;
        let answer = match &out {
            rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => r.value().name().to_string(),
            other => other.name().to_string(),
        };
        tracing::info_span!("rdm.node_admin.status.update.via-completion-call", node = %self.held_name(), completion = key.kind.completion(), build_id = %key.build_id, attempt = key.attempt, commander = %commander, hook = %hook, answer = %answer, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the completion call was made to the commanding primary"));
    }

    /// Serve `DrainNode`, `StopNode` or `StartNode` on the exact birth it names, whoever calls
    /// (node-rpc.md §14: nothing inside the mesh checks who is calling): `commander` is the caller as
    /// the receiver resolved it (`None` when it holds no birth for the calling endpoint), `spare` the
    /// call's own connection. A round command is served for `commander`, the primary of this birth's
    /// mesh, which its check-in goes to. `None` for any other request.
    pub async fn serve(self: &Arc<Self>, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, req: &StatusRequest) -> Option<StatusReply> {
        if matches!(req, StatusRequest::CommitState { .. } | StatusRequest::OpenTraffic { .. }) {
            return Some(match commander {
                Some(commander) => self.serve_round(commander, req),
                None => StatusReply::NotReady { reason: format!("{}: no primary of its mesh is known to check {} in to", self.held_name(), req.op()) },
            });
        }
        if let StatusRequest::DeleteConnections { node_id, incarnation, .. } = req {
            if *node_id != self.node_id {
                return Some(StatusReply::RejectedNotAuthority { why: rafka_node_rpc_contract::status::NotAuthority::ReceiverNotPrimary { needed: "the subject node itself".into() } });
            }
            if *incarnation != self.incarnation {
                return Some(StatusReply::RejectedStaleIncarnation { held: self.incarnation.clone() });
            }
            let StatusRequest::DeleteConnections { scope, .. } = req else { unreachable!("matched above") };
            return Some(self.serve_delete_connections(commander, spare, scope).await);
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
            name: self.held_name(),
            event_at_rafka_ms: self.membership.clock().now_rafka_ms(),
        };
        // The command's serve span carries the caller's propagated trace; the work this command
        // starts continues it, so the spans it opens join the outer trace.
        let parent = tracing::Span::current();
        let commander = commander.cloned();
        match req {
            StatusRequest::StopNode { .. } => Some(self.stop(commander, spare, op, parent).await),
            StatusRequest::StartNode { .. } => Some(self.start(commander, spare, op, parent).await),
            _ => {
                use tracing::Instrument;
                // Forward only: a node already leaving is not drained. A parked birth whose start
                // is under way is still in Leaving until the start ends (states.md, "Stopped
                // nodes"), whatever status the rejoin has set: the drain of the cycle its stop
                // ended never answers for it.
                if self.is_parked() || (self.status)() == MemberStatus::Leaving {
                    return Some(StatusReply::RejectedInvalidNodeTransition { current: NodeState::Leaving });
                }
                let me = self.clone();
                match self.admit_drain(commander.as_ref(), spare, &op, &parent) {
                    DrainAdmission::Start => {
                        // The eligible work in flight when the drain is admitted: every dispatched
                        // handler but this command's own, which is in flight while it is served.
                        let began = rafka_node_rpc::ServerStats::get(&self.server.stats().in_flight).saturating_sub(1);
                        tokio::spawn(async move { me.drain(commander, op, parent, began).await });
                        Some(StatusReply::Applied)
                    }
                    DrainAdmission::Joined | DrainAdmission::EndedAtBound => Some(StatusReply::AlreadyApplied),
                    DrainAdmission::Drained => {
                        tokio::spawn(async move { me.complete(commander.as_ref(), spare, &op).await }.instrument(parent));
                        Some(StatusReply::AlreadyApplied)
                    }
                }
            }
        }
    }

    /// Admit `commander`'s `DrainNode` for `op` on the natural key `(node_id, incarnation,
    /// Draining)` of this cycle (the caller checked the birth and that it is not leaving): the first
    /// starts the drain; any later one, of any attempt or commander, is owed the same drain's
    /// completion under its own operation. An identical repeat while the drain runs is owed it once.
    fn admit_drain(&self, commander: Option<&NodeId>, endpoint: Option<iroh::PublicKey>, op: &LifecycleOp, serve: &tracing::Span) -> DrainAdmission {
        let mut drain = self.drain.lock().unwrap();
        let owed_now = || Owed { commander: commander.cloned(), endpoint, op: op.clone(), trace: serve.clone() };
        match &mut *drain {
            None => {
                *drain = Some(Drain::Running(vec![owed_now()]));
                DrainAdmission::Start
            }
            Some(Drain::Running(owed)) => {
                if !owed.iter().any(|o| o.commander.as_ref() == commander && o.endpoint == endpoint && o.op.build_id == op.build_id && o.op.attempt == op.attempt && o.op.operation == op.operation) {
                    owed.push(owed_now());
                }
                DrainAdmission::Joined
            }
            Some(Drain::Ended(DrainReceipt::Established { .. })) => DrainAdmission::Drained,
            Some(Drain::Ended(DrainReceipt::Deadline { .. })) => DrainAdmission::EndedAtBound,
        }
    }

    /// The drain of this cycle ended with `receipt`: the commanders it owes `node-drained` are
    /// returned, each once, when the receipt is `Established` (empty when another drain of the
    /// birth took them); a `Deadline` is not drained and owes none.
    fn ended(&self, receipt: DrainReceipt) -> Vec<Owed> {
        match self.drain.lock().unwrap().replace(Drain::Ended(receipt)) {
            Some(Drain::Running(owed)) if matches!(receipt, DrainReceipt::Established { .. }) => owed,
            _ => Vec::new(),
        }
    }

    /// The drain `commander`'s `DrainNode` admitted, `began` the eligible work in flight then: refuse
    /// new work and observe the in-flight work under the drain bound (node-drain.md step 3).
    async fn drain(self: Arc<Self>, commander: Option<NodeId>, op: LifecycleOp, parent: tracing::Span, began: u64) {
        use tracing::Instrument;
        let span = tracing::info_span!(
            parent: &parent,
            "rdm.node_admin.status.update.via-drain-node",
            node = %self.held_name(), operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
            commander = %named(commander.as_ref()), receipt = tracing::field::Empty, in_flight = tracing::field::Empty, in_flight_at_zero = tracing::field::Empty, "otel.kind" = "internal"
        );
        async {
            if let Some(retire) = &self.retire {
                retire.cancel();
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
                let stats = self.server.stats();
                let until = tokio::time::Instant::now() + drain_deadline_from_env();
                let receipt = loop {
                    let in_flight = rafka_node_rpc::ServerStats::get(&stats.in_flight);
                    let mut d = draining.clone();
                    d.in_flight = Some(in_flight);
                    let _ = self.membership.publish(&d).await;
                    if in_flight == 0 {
                        break DrainReceipt::Established { in_flight: began };
                    }
                    if tokio::time::Instant::now() >= until {
                        break DrainReceipt::Deadline { last_in_flight: in_flight };
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                };
                // The receipt's count first, its name last: a reader that sees the receipt's name
                // on the span sees the whole receipt (the count is never read before it is written).
                let current = tracing::Span::current();
                match receipt {
                    DrainReceipt::Established { in_flight } => {
                        current.record("in_flight", in_flight);
                        current.record("in_flight_at_zero", true);
                    }
                    DrainReceipt::Deadline { last_in_flight } => {
                        current.record("in_flight", last_in_flight);
                    }
                }
                current.record("receipt", receipt.name());
                if let DrainReceipt::Deadline { last_in_flight } = receipt {
                    tracing::info!(last_in_flight, "the drain bound passed with work in flight: the birth stays Draining, not drained, and owes no node-drained");
                }
                // Each commander owed is called on its own: one that cannot be reached holds up no other.
                futures_util::future::join_all(self.ended(receipt).into_iter().map(|owed| owed.complete(&self))).await;
            };
            tokio::join!(declared, completion);
        }
        .instrument(span)
        .await
    }

    /// The stop: drain, declare `Leaving`, hard-cut the mesh connections, park. The receipt is the
    /// drain's. A second stop of the same birth, while this one runs or after it, answers the same
    /// receipt: stopping a stopped birth is done. A stop while a start of the birth is under way is
    /// refused naming the start.
    async fn stop(self: &Arc<Self>, commander: Option<NodeId>, spare: Option<iroh::PublicKey>, op: LifecycleOp, parent: tracing::Span) -> StatusReply {
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
                // The start ends the stopped cycle: the receipt that parked the birth does not
                // answer for a birth on its way out of Leaving, as a start does not run beside a stop.
                Cycle::Starting { .. } => return StatusReply::NotReady { reason: format!("{}: a start is under way; the stop is decided on the cycle it ends in", self.held_name()) },
                Cycle::Stopping(rx) => Next::Wait(rx.clone()),
                Cycle::Live => {
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    *cycle = Cycle::Stopping(rx);
                    // The stop drains the birth: a DrainNode that reaches it meanwhile is this
                    // cycle's drain and joins it, never a drain of its own.
                    let mut drain = self.drain.lock().unwrap();
                    if drain.is_none() {
                        *drain = Some(Drain::Running(Vec::new()));
                    }
                    Next::Run(tx)
                }
            }
        };
        let receipt = match next {
            Next::Done(r) => r,
            Next::Wait(mut rx) => match rx.wait_for(|r| r.is_some()).await {
                Ok(r) => r.expect("waited for a receipt"),
                Err(_) => return StatusReply::NotReady { reason: format!("{}: the stop that was under way ended without a receipt", self.held_name()) },
            },
            Next::Run(tx) => {
                let span = tracing::info_span!(
                    parent: &parent,
                    "rdm.node_admin.status.update.via-stop-node",
                    node = %self.held_name(), operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
                    commander = %named(commander.as_ref()), state = "Leaving", receipt = tracing::field::Empty, "otel.kind" = "internal"
                );
                let receipt = self.park(commander.as_ref(), spare, &op).instrument(span.clone()).await;
                span.record("receipt", receipt.name());
                *self.cycle.lock().unwrap() = Cycle::Parked(receipt);
                let _ = tx.send(Some(receipt));
                receipt
            }
        };
        StatusReply::Stopped { receipt }
    }

    async fn park(&self, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, op: &LifecycleOp) -> DrainReceipt {
        use tracing::Instrument;
        parking().park();
        if let Some(retire) = &self.retire {
            retire.cancel();
        }
        // 1. The whole drain: Draining is live and not routable; the receipt never turns a deadline into zero.
        let receipt = self
            .drain_for_stop()
            .instrument(tracing::info_span!("rdm.node_admin.node.drain.via-stop", node = %self.held_name(), operation = %op.operation, receipt = tracing::field::Empty))
            .await;
        // An established drain is this cycle's drain reaching zero: every commander whose
        // DrainNode joined it gets node-drained now, while the connections are whole. A deadline is
        // not drained, and owes none.
        let owed = self.ended(receipt);
        let completions = futures_util::future::join_all(owed.into_iter().map(|owed| owed.complete(self)));
        let session = self.session.lock().unwrap().clone();
        let held_then_declared = async {
            // 2. The node's own tasks are held: no turn begins, and every turn already past its wait
            // point runs to its end before anything more is said or cut.
            if let Some(session) = &session {
                let span = tracing::info_span!(
                    "rdm.node_admin.node.update.via-stop-quiesce",
                    node = %self.held_name(), operation = %op.operation, turns_in_progress = tracing::field::Empty, waited_for = tracing::field::Empty
                );
                let parked = session.park_tasks().instrument(span.clone()).await;
                span.record("turns_in_progress", parked.turns_in_progress);
                span.record("waited_for", parked.waited_for.as_str());
            }
            // 3. Leaving is declared to the authority while this node can still reach it, then no
            // more is said: the node gossips nothing for its stop.
            if let Some(declare) = &self.declare {
                declare(NodeState::Leaving).await;
            }
        };
        // The owed completions are called beside the hold and the declaration, so the stop waits
        // for neither longer than for the slower.
        tokio::join!(completions, held_then_declared);
        // 4. R-NS1 item 2 (gossip.md mesh-leave section): a node-admin that still publishes its
        // mesh's aggregate (the last admin of a deleted mesh, still its primary) publishes it once
        // more before the cut, itself Leaving: "Its last aggregate before a stop carries itself
        // Leaving." It follows the hold, so no gated turn publishes after it; after the cut it
        // publishes nothing (node-stop.md step 4, R-ON1).
        if let Some(backbone) = self.backbone.as_ref().filter(|b| b.is_mesh_primary()) {
            let me = (self.set_status)(MemberStatus::Leaving);
            let mut members: Vec<MeshDigest> = backbone.own_members(&self.membership);
            match members.iter_mut().find(|d| d.node.node_id == me.node.node_id) {
                Some(slot) => *slot = me.clone(),
                None => members.push(me.clone()),
            }
            let count = members.len();
            let published = backbone.publish(&self.membership, members).await;
            tracing::info_span!("rdm.node_admin.node.update.via-leaving-aggregate", node = %self.held_name(), mesh = %self.held_name().mesh, operation = %op.operation, status = "Leaving", members = count, published)
                .in_scope(|| tracing::info!("the last aggregate before the stop carries this node Leaving"));
        }
        // 5. node.connections.delete { All }: the hard cut, sparing the commander's connection the
        // stop call rides, and the drop of every connection it held.
        let report = self
            .delete_connections(spare, &ConnectionScope::All)
            .instrument(tracing::info_span!("rdm.node_admin.node.connections.delete.via-stop", node = %self.held_name(), operation = %op.operation, commander = %named(commander)))
            .await
            .report;
        // Leaving is this node's own status from the moment its tasks are cut; nothing publishes it.
        let _ = (self.set_status)(MemberStatus::Leaving);
        tracing::info_span!(
            "rdm.node_admin.node.connections.delete.via-stop-cut",
            node = %self.held_name(), operation = %op.operation, neighbours = report.neighbours, pooled = report.pooled, accepted = report.accepted, turns_in_progress = report.turns_in_progress
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
    /// refuses it, naming the state it is in. A start that reaches the birth while another start of
    /// it is under way is the same natural key `(node_id, incarnation, Pending)`: it runs no rejoin
    /// of its own and answers that start's outcome.
    async fn start(self: &Arc<Self>, commander: Option<NodeId>, spare: Option<iroh::PublicKey>, op: LifecycleOp, parent: tracing::Span) -> StatusReply {
        use tracing::Instrument;
        let span = tracing::info_span!(
            parent: &parent,
            "rdm.node_admin.status.update.via-start-node",
            node = %self.held_name(), operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
            commander = %named(commander.as_ref()), joined = tracing::field::Empty, outcome = tracing::field::Empty, failed_step = tracing::field::Empty, "otel.kind" = "internal"
        );
        let reply = async {
            enum Next {
                Run(tokio::sync::watch::Sender<Option<StatusReply>>, DrainReceipt, Arc<dyn Session>),
                Join(tokio::sync::watch::Receiver<Option<StatusReply>>),
            }
            let next = {
                let mut cycle = self.cycle.lock().unwrap();
                match &*cycle {
                    Cycle::Parked(r) => {
                        let receipt = *r;
                        let Some(session) = self.session.lock().unwrap().clone() else {
                            return StatusReply::NotReady { reason: format!("{}: this process holds no session to rejoin its mesh with", self.held_name()) };
                        };
                        let (tx, rx) = tokio::sync::watch::channel(None);
                        *cycle = Cycle::Starting { outcome: rx };
                        Next::Run(tx, receipt, session)
                    }
                    Cycle::Starting { outcome, .. } => Next::Join(outcome.clone()),
                    Cycle::Stopping(_) => return StatusReply::NotReady { reason: format!("{}: a stop is still under way; start is the transition out of Leaving and only for a parked process", self.held_name()) },
                    Cycle::Live => {
                        let current = match (self.status)() {
                            MemberStatus::Pending => NodeState::Pending,
                            MemberStatus::ReadyForTraffic => NodeState::ReadyForTraffic,
                            MemberStatus::Draining => NodeState::Draining,
                            MemberStatus::Leaving => NodeState::Leaving,
                        };
                        return StatusReply::RejectedInvalidNodeTransition { current };
                    }
                }
            };
            let (outcome, receipt, session) = match next {
                Next::Join(mut rx) => {
                    tracing::Span::current().record("joined", true);
                    return match rx.wait_for(|r| r.is_some()).await {
                        Ok(r) => r.clone().expect("waited for an outcome"),
                        Err(_) => StatusReply::NotReady { reason: format!("{}: the start that was under way ended without an outcome", self.held_name()) },
                    };
                }
                Next::Run(tx, receipt, session) => (tx, receipt, session),
            };
            let retire = match &self.retire {
                Some(r) => r.renew(),
                None => rafka_node_rpc::CancelToken::new(),
            };
            // No row is written from here until the session holds the name its join answer hands:
            // every connection the start makes or accepts is recorded under that name.
            let writer = self.connections.lock().unwrap().clone();
            let rows = match writer {
                Some(w) => Some(w.hold_rows().await),
                None => None,
            };
            let _ = (self.set_status)(MemberStatus::Pending);
            self.server.resume();
            // The admin that commanded the start answers its join, reached back through its own call:
            // the call's endpoint key names it (node-start.md step 2).
            let joined = match spare {
                Some(key) => session.rejoin(retire, key, rows).await,
                None => Err(StartFailed { step: "node.join", reason: format!("{}: commander-uncalled: the start came on no Node RPC call, so no endpoint names the admin to join back", self.held_name()) }),
            };
            let reply = match joined {
                Ok(()) => {
                    // A new cycle: its next drain is a new drain.
                    let mut cycle = self.cycle.lock().unwrap();
                    *cycle = Cycle::Live;
                    *self.drain.lock().unwrap() = None;
                    drop(cycle);
                    parking().unpark();
                    StatusReply::Started
                }
                Err(failed) => {
                    // The start stopped at its step: the node is parked again, as it was: its tasks
                    // held (a turn its rejoin let begin runs to its end), then its connections cut.
                    let _ = (self.set_status)(MemberStatus::Leaving);
                    self.server.drain();
                    let _ = session.park_tasks().await;
                    let _ = session.cut(spare).await;
                    *self.cycle.lock().unwrap() = Cycle::Parked(receipt);
                    tracing::Span::current().record("failed_step", failed.step);
                    StatusReply::StartFailed { step: failed.step.to_string(), reason: failed.reason }
                }
            };
            outcome.send_replace(Some(reply.clone()));
            reply
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", reply.name());
        reply
    }

    /// The drain's completion: the node's own gossip hook on its mesh channel, then the call at the
    /// commanding admin, while this endpoint is open. The commander is the caller as its command
    /// resolved it, else the node this birth's book holds at the calling endpoint now; when it holds
    /// none, the hook is all that goes out and the span says why no call was made.
    async fn complete(&self, commander: Option<&NodeId>, endpoint: Option<iroh::PublicKey>, op: &LifecycleOp) {
        let frame = Frame::NodeDrained { op: op.clone(), forwarded_by: None };
        let req = StatusRequest::NodeDrained { node_id: op.node_id.clone(), incarnation: op.incarnation.clone(), build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone() };
        let kind = "node-drained";
        // What became of the hook is named on the call's span: handed to gossip, or why not.
        let hook = match self.membership.publish_lifecycle(&frame).await {
            Ok(()) => "published".to_string(),
            Err(e) => format!("not-published: {e}"),
        };
        let target = commander.cloned().or_else(|| self.node_at_endpoint(endpoint));
        let answer = match &target {
            Some(commander) => match self.client.call::<Status>(&NodeTarget::ExactNode(commander.clone()), &req, &rafka_node_rpc::CallOptions::default()).await.0 {
                rafka_node_rpc_contract::outcome::RpcOutcome::Reply(r) => r.value().name().to_string(),
                other => other.name().to_string(),
            },
            None => format!("commander-unresolved: the endpoint {} that commanded the drain is in no digest this birth holds", endpoint.map(|k| k.to_string()).unwrap_or_else(|| "(none)".into())),
        };
        tracing::info_span!(
            "rdm.node_admin.status.update.via-completion-call",
            node = %self.held_name(), completion = kind, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
            commander = %named(target.as_ref()), hook = %hook, answer = %answer, "otel.kind" = "internal"
        )
        .in_scope(|| tracing::info!("the completion call was made to the commanding admin"));
    }

    /// The node this birth's digest book holds at the endpoint `key` now.
    fn node_at_endpoint(&self, key: Option<iroh::PublicKey>) -> Option<NodeId> {
        let key = key?.to_string();
        self.membership.book.all().into_iter().find(|d| d.node.endpoint_id.0 == key).map(|d| d.node.node_id)
    }
}

/// A commander as a span field: its node id, or empty when the call resolved none.
fn named(commander: Option<&NodeId>) -> String {
    commander.map(ToString::to_string).unwrap_or_default()
}

/// The record this runtime's provider wrote for it (`rafka_mesh_entity::runtime::read_own_record`),
/// waited for on the caller's runtime for at most `within`: a boot that ends (a stop signal) drops
/// the wait at once, where a blocking wait would hold the runtime's shutdown until `within` passed.
pub async fn wait_for_own_record(dir: &std::path::Path, within: std::time::Duration) -> Result<rafka_mesh_entity::RuntimeFact, String> {
    let until = tokio::time::Instant::now() + within;
    loop {
        let last = match rafka_mesh_entity::runtime::read_own_record(dir) {
            Ok(f) => return Ok(f),
            Err(e) => e,
        };
        if tokio::time::Instant::now() >= until {
            return Err(format!("the provider wrote no runtime record for this runtime within {within:?}: {last}"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
