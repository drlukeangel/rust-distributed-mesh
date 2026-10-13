//! A node birth obeying the commands of its owning mesh-admin: `node.connections.delete`,
//! `node.stop` and `node.start` (node-connections-delete.md, node-stop.md, node-start.md). Every node
//! kind runs this one logic over its own surface: an rpc node, a role node and a node-admin differ
//! only in how they refuse new work, how they publish and how they leave and rejoin the mesh (their
//! [`Session`]).
//!
//! - `DeleteConnections`: `All` blocks admission of new application work, lets the eligible work in
//!   flight finish under `RDM_CONNECTIONS_DELETE_DEADLINE_MS`, closes every open Direct or Proxy entry
//!   of the connections log and answers `ConnectionsDeleted` with its `ConnectionsReceipt`
//!   (`Established`, or `Deadline` at the bound). `Connections` closes exactly the named entries.
//!   Gossip neighbours and the Node RPC pool are not connections: neither scope touches them.
//! - `StopNode`: the whole `node.connections.delete { All }`, then hold the node's own tasks, declare
//!   `Leaving` to the authority, publish a mesh-primary node-admin's last aggregate with itself
//!   `Leaving` (gossip.md, "Mesh delete"), then the stop's own transport cut (gossip neighbours and the
//!   Node RPC pool; never the endpoint and never the commander's connection the call rides), and answer
//!   [`StatusReply::Stopped`] with the `All`'s receipt on that same call. The process stays alive,
//!   its endpoint bound.
//! - `StartNode`: a stopped birth rejoins as itself (same node id, incarnation, endpoint key and
//!   port) through its [`Session`], and answers `Started`, or `StartFailed` naming the step.
//! - `DrainNode` and `NodeDrained` are reserved Status positions (R-CR4): refused by name.
//!
//! Nothing here waits on an answer for correctness: a lost reply is the commander's indeterminate,
//! never this node's retry.

use crate::model::{IncarnationId, NodeId, PathName};
use rafka_mesh_entity::{LifecycleOp, MemberStatus, MeshDigest};
use rafka_mesh_transport::membership::Membership;
use rafka_node_rpc::{NodeRpcClient, NodeRpcServer, NodeTarget};
use rafka_node_rpc_contract::status::{ConnectionScope, ConnectionsReceipt, NodeState, Status, StatusReply, StatusRequest};
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

/// The bound of a `node.connections.delete { All }`, standalone or inside a stop:
/// `RDM_CONNECTIONS_DELETE_DEADLINE_MS`, 5000 ms when unset. Strictly shorter than the stop ladder's grace.
pub fn connections_delete_deadline_from_env() -> Duration {
    Duration::from_millis(std::env::var("RDM_CONNECTIONS_DELETE_DEADLINE_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(5000))
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
    /// The open entries closed, their drops written under the name the birth holds.
    pub closed: Vec<rafka_mesh_entity::ConnectionId>,
    /// The ids named that are no open entry of the birth: unknown, or already closed.
    pub not_open: Vec<rafka_mesh_entity::ConnectionId>,
    /// The open entries the birth still holds after the cut, a drop the storage refused included.
    pub held: Vec<rafka_mesh_entity::ConnectionId>,
    /// For `All`, what the closure established; `None` for `Connections`.
    pub receipt: Option<ConnectionsReceipt>,
}

/// What waiting for the eligible work in flight established.
enum InFlight {
    /// It reached zero: `began` was the count when the wait began.
    Zero { began: u64 },
    /// The bound passed: `last` was the last count observed.
    Deadline { last: u64 },
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
    /// the connections peers made, sparing the connections from `spare` (the commanders whose calls
    /// are in flight and answered on their connections). The endpoint stays bound.
    async fn cut(&self, spare: &[iroh::PublicKey]) -> CutReport;
    /// Rejoin as the same birth: join the authority, rejoin the mesh channel, take the topology and
    /// run the hydrate hook with `AfterStop` in its context under `retire`, the new cycle's token.
    /// `commander` holds the endpoint key of the call that commanded the start last: each join
    /// attempt goes back over that call ([`crate::workflows::node_start::JoinTo::Commander`]), and
    /// the admin that answers is the hook's accepting authority. `rows` is the start's hold on this
    /// birth's connections rows: the session drops it once it holds the name its join answer handed,
    /// and no earlier, so the join's connection is recorded under that name (node-start.md step 2).
    async fn rejoin(&self, retire: rafka_node_rpc::CancelToken, commander: tokio::sync::watch::Receiver<iroh::PublicKey>, rows: Option<crate::connections_writer::RowsHeld>) -> Result<(), StartFailed>;
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
    /// Running: a stop has not been admitted. A birth a start brought back holds that start's command,
    /// `(build_id, operation)`: its reissue by a later attempt of the same Build answers `AlreadyApplied`
    /// (node-rpc-envelope.md: "Reissuing a naturally idempotent uncertain step can settle it with
    /// AlreadyApplied"); its node.joined and node.started were taken when it ran. A birth that never
    /// stopped holds `None`.
    Live(Option<(String, String)>),
    /// A stop is under way; later stops of the birth wait for the same receipt.
    Stopping(tokio::sync::watch::Receiver<Option<ConnectionsReceipt>>),
    /// Stopped, its process alive, with the receipt its node.connections.delete { All } established.
    Stopped(ConnectionsReceipt),
    /// Stopped and a start is under way (the natural key `(node_id, incarnation, Pending)`): later
    /// starts of the birth answer the same outcome, and a stop is refused naming the start. A failed
    /// start leaves the birth stopped again with the receipt it was stopped with.
    Starting {
        /// The outcome of the start under way, once it ends.
        outcome: tokio::sync::watch::Receiver<Option<StatusReply>>,
        /// The endpoint key of the call that commanded the start last: the start's join goes back
        /// over it (node-start.md step 2). A later start's call hands its own key here. `None` for a
        /// start that came on no Node RPC call.
        commander: Option<tokio::sync::watch::Sender<iroh::PublicKey>>,
        /// The endpoint keys of every call commanding the start, each awaiting its answer: a failed
        /// start that parks the birth again spares their connections, so each call is answered.
        calls: Vec<iroh::PublicKey>,
    },
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
    /// The process's live resolver: an admin that commanded this birth is applied to it at the path
    /// its call came in on, so the completion call goes back there.
    pub resolver: Arc<rafka_node_rpc::LiveNodeResolver>,
    /// This birth's membership: the mesh channel its completion frames go out on.
    pub membership: Membership,
    /// Set this birth's status and answer the digest to publish.
    pub set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync>,
    /// This birth's status now.
    pub status: Arc<dyn Fn() -> MemberStatus + Send + Sync>,
    /// Declare the state this birth entered to its authority (a node-admin of its mesh); `None` for
    /// a surface whose declarations are made elsewhere (a node-admin).
    pub declare: Option<Declare>,
    /// Cancelled when `node.connections.delete { All }` or `stop-node` is admitted: the birth is
    /// retired, so its `hydrate_before_ready` hook and any pull it has in flight end.
    pub retire: Option<rafka_node_rpc::Retirement>,
    /// The backbone, for a node-admin: its round hooks go out on its mesh channel and the backbone.
    /// `None` for an ordinary node: its hooks go out on its mesh channel and its mesh-admin carries
    /// them onto the backbone.
    pub backbone: Option<rafka_mesh_transport::membership::Backbone>,
    /// The bound of this birth's `node.connections.delete { All }`, standalone or inside a stop
    /// (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`, read once where the process reads its launch or its
    /// admin config): the eligible work in flight is let finish until it passes.
    pub connections_delete_deadline: Duration,
    /// Where this birth stands between a stop and a start.
    cycle: Mutex<Cycle>,
    /// How this process leaves and rejoins its mesh; filled once the process holds the parts.
    session: Mutex<Option<Arc<dyn Session>>>,
    /// The client this birth originates its data-plane connections on, the ones its connections log
    /// holds as Direct entries (connections.md "Two planes, two words"); `None` for a process whose
    /// data plane is not a Node RPC client of its own. `node.connections.delete` closes its pooled
    /// connection to the destination of each Direct entry it closes, and nothing of the Node RPC pool.
    data_plane: Mutex<Option<Arc<NodeRpcClient>>>,
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
        resolver: Arc<rafka_node_rpc::LiveNodeResolver>,
        membership: Membership,
        set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync>,
        status: Arc<dyn Fn() -> MemberStatus + Send + Sync>,
        declare: Option<Declare>,
        connections_delete_deadline: Duration,
    ) -> Self {
        Self { node_id, incarnation, name, server, client, resolver, membership, set_status, status, declare, connections_delete_deadline, retire: None, backbone: None, cycle: Mutex::new(Cycle::Live(None)), session: Mutex::new(None), data_plane: Mutex::new(None), connections: Mutex::new(None), round_actions: Mutex::new(Arc::new(crate::fabric_rounds::NoScratchpad)), rounds_seen: Mutex::new(std::collections::HashMap::new()) }
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

    /// This surface, cancelling `retire` when `node.connections.delete { All }` or a stop is admitted.
    pub fn with_retire(mut self, retire: rafka_node_rpc::Retirement) -> Self {
        self.retire = Some(retire);
        self
    }

    /// This surface, leaving and rejoining its mesh through `session` when it is stopped and started.
    pub fn set_session(&self, session: Arc<dyn Session>) {
        *self.session.lock().unwrap() = Some(session);
    }

    /// This surface, closing the data-plane connections of the Direct entries a
    /// `node.connections.delete` closes on `client`.
    pub fn set_data_plane(&self, client: Arc<NodeRpcClient>) {
        *self.data_plane.lock().unwrap() = Some(client);
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

    /// `node.connections.delete` at this birth (R-CR5; node-connections-delete.md): a connection is
    /// an open Direct or Proxy entry of this birth's connections log, and nothing else. `All` closes
    /// every open entry; `Connections` closes exactly the entries it names. Each closed entry gets its
    /// drop, written under the name the birth holds; an id that is no open entry is named
    /// `not_open`. Gossip neighbours and the Node RPC pool are not connections: neither scope touches
    /// them, so a birth whose connections are deleted is still heard on gossip and still answers calls.
    pub async fn delete_connections(&self, scope: &ConnectionScope) -> Deleted {
        let writer = self.connections.lock().unwrap().clone();
        let open = match &writer {
            Some(w) => w.held_routes().await,
            None => Vec::new(),
        };
        // `All` is graceful node-wide closure (node-connections-delete.md step 3): admission of new
        // application work is blocked, and the eligible work in flight (every dispatched handler but
        // this call's own) is let finish, bounded by `RDM_CONNECTIONS_DELETE_DEADLINE_MS`.
        // The birth is retired at the admission of `All`: its `hydrate_before_ready` hook and any pull
        // it has in flight end; it returns to service only through a stop and a start.
        let in_flight = match scope {
            ConnectionScope::All => {
                if let Some(retire) = &self.retire {
                    retire.cancel();
                }
                Some(self.await_in_flight().await)
            }
            ConnectionScope::Connections(_) => None,
        };
        let (routes, not_open) = match scope {
            ConnectionScope::All => (open, Vec::new()),
            ConnectionScope::Connections(ids) => {
                let (mut routes, mut not_open): (Vec<rafka_mesh_entity::connections::NodeConnection>, Vec<rafka_mesh_entity::ConnectionId>) = (Vec::new(), Vec::new());
                for id in ids {
                    match open.iter().find(|r| &r.id == id) {
                        Some(route) if !routes.iter().any(|r| r.id == route.id) => routes.push(route.clone()),
                        Some(_) => {}
                        None => not_open.push(id.clone()),
                    }
                }
                (routes, not_open)
            }
        };
        // Each Direct entry closed is its data-plane connection closed, on that connection's own
        // span naming the entry (R-CR6: the pool's hard cut is the stop's transport cut alone); a
        // Proxy rides its carrier's Direct and only its route retires.
        let data_plane = self.data_plane.lock().unwrap().clone();
        if let Some(data_plane) = &data_plane {
            for route in routes.iter().filter(|r| r.kind == rafka_mesh_entity::connections::ConnectionKind::Direct) {
                if let Some(peer) = data_plane.endpoint_of(&route.destination.node_id) {
                    for c in data_plane.close_connections_to(&peer, CONNECTIONS_DELETE) {
                        tracing::info_span!(
                            "rdm.node_admin.connection.delete.via-connections-delete",
                            node = %self.held_name(),
                            entry = %route.id,
                            destination = %route.destination.name,
                            peer = %c.key.peer.fmt_short(),
                            incarnation_id = %c.key.birth(),
                            connection = c.connection,
                        )
                        .in_scope(|| tracing::info!("the data-plane connection of a closed Direct entry was closed"));
                    }
                }
            }
        }
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
        let receipt = in_flight.map(|w| match w {
            InFlight::Zero { began } => ConnectionsReceipt::Established { in_flight: began, closed: closed.clone() },
            InFlight::Deadline { last } => ConnectionsReceipt::Deadline { last_in_flight: last, closed: closed.clone(), held: held.clone() },
        });
        Deleted { closed, not_open, held, receipt }
    }

    /// Block admission of new application work and wait for the eligible work in flight to reach
    /// zero, bounded by `RDM_CONNECTIONS_DELETE_DEADLINE_MS`. The call that asked is in flight while
    /// it waits and is not eligible work. The node refuses new non-status calls with typed `Draining`
    /// from here on (Node RPC §35); control streams stay usable.
    async fn await_in_flight(&self) -> InFlight {
        self.server.drain();
        let stats = self.server.stats();
        let eligible = || rafka_node_rpc::ServerStats::get(&stats.in_flight).saturating_sub(1);
        let began = eligible();
        let until = tokio::time::Instant::now() + connections_delete_deadline_from_env();
        loop {
            let n = eligible();
            if n == 0 {
                return InFlight::Zero { began };
            }
            if tokio::time::Instant::now() >= until {
                return InFlight::Deadline { last: n };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Serve `DeleteConnections` from `commander` (its connection `spare`): a scope that names
    /// nothing to cut is refused by name and cuts nothing; any other is the cut, answered
    /// `node.connections.deleted`. One span names the scope and every count.
    async fn serve_delete_connections(&self, commander: Option<&NodeId>, scope: &ConnectionScope) -> StatusReply {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "rdm.node_admin.node.connections.delete.via-delete-connections",
            node = %self.held_name(), commander = %named(commander), scope = scope.name(), named = match scope { ConnectionScope::All => 0, ConnectionScope::Connections(ids) => ids.len() },
            closed = tracing::field::Empty, not_open = tracing::field::Empty, held = tracing::field::Empty, receipt = tracing::field::Empty,
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
            let d = self.delete_connections(scope).await;
            let current = tracing::Span::current();
            current.record("closed", d.closed.len()).record("not_open", d.not_open.len()).record("held", d.held.len());
            if let Some(r) = &d.receipt {
                current.record("receipt", r.name());
            }
            StatusReply::ConnectionsDeleted { closed: d.closed, not_open: d.not_open, held: d.held, receipt: d.receipt }
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
        matches!(*self.cycle.lock().unwrap(), Cycle::Stopped(_) | Cycle::Starting { .. })
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
        // Answered by the birth the call reached (R-RX5: no receiver refuses a downward call for the
        // node id it names); the exact birth it names is checked, so a call that reached another
        // birth is refused naming the one held.
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
        tracing::info_span!("rdm.node_admin.fabric.update.via-round-hook", node = %self.held_name(), round = key.kind.command(), hook = key.kind.completion(), subject_id = %self.node_id, incarnation_id = %self.incarnation.as_str(), build_id = %key.build_id, attempt = key.attempt, channels, bound = true, "otel.kind" = "internal")
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

    /// Serve `DeleteConnections`, `StopNode` or `StartNode` on the exact birth it names; `DrainNode`
    /// and `NodeDrained` are reserved positions, refused by name, whoever calls
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
        if let StatusRequest::DeleteConnections { incarnation, .. } = req {
            if *incarnation != self.incarnation {
                return Some(StatusReply::RejectedStaleIncarnation { held: self.incarnation.clone() });
            }
            let StatusRequest::DeleteConnections { scope, .. } = req else { unreachable!("matched above") };
            return Some(self.serve_delete_connections(commander, scope).await);
        }
        if matches!(req, StatusRequest::DrainNode { .. } | StatusRequest::NodeDrained { .. }) {
            return Some(crate::node_commands::refuse_reserved(&self.held_name().to_string(), &named(commander), req));
        }
        let (incarnation, build_id, attempt, operation) = match req {
            StatusRequest::StopNode { incarnation, build_id, attempt, operation, .. }
            | StatusRequest::StartNode { incarnation, build_id, attempt, operation, .. } => (incarnation, build_id, attempt, operation),
            _ => return None,
        };
        // The birth the call reached obeys it (R-RX5: no receiver refuses a downward call for the
        // node id it names); the exact birth it names is checked, so a call that reached another
        // birth is refused naming the one held.
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
            _ => Some(self.start(commander, spare, op, parent).await),
        }
    }

    /// The stop (node-stop.md): the whole `node.connections.delete { All }`, declare `Leaving`, the
    /// stop's own transport cut. The receipt is the `All`'s. A second stop of the same birth, while this one runs or after it, answers the same
    /// receipt: stopping a stopped birth is done. A stop while a start of the birth is under way is
    /// refused naming the start.
    async fn stop(self: &Arc<Self>, commander: Option<NodeId>, spare: Option<iroh::PublicKey>, op: LifecycleOp, parent: tracing::Span) -> StatusReply {
        use tracing::Instrument;
        enum Next {
            Done(ConnectionsReceipt),
            Wait(tokio::sync::watch::Receiver<Option<ConnectionsReceipt>>),
            Run(tokio::sync::watch::Sender<Option<ConnectionsReceipt>>),
        }
        let next = {
            let mut cycle = self.cycle.lock().unwrap();
            match &*cycle {
                Cycle::Stopped(r) => Next::Done(r.clone()),
                // The start ends the stopped cycle: the receipt that parked the birth does not
                // answer for a birth on its way out of Leaving, as a start does not run beside a stop.
                Cycle::Starting { .. } => return StatusReply::NotReady { reason: format!("{}: a start is under way; the stop is decided on the cycle it ends in", self.held_name()) },
                Cycle::Stopping(rx) => Next::Wait(rx.clone()),
                Cycle::Live(_) => {
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    *cycle = Cycle::Stopping(rx);
                    Next::Run(tx)
                }
            }
        };
        let receipt = match next {
            Next::Done(r) => r,
            Next::Wait(mut rx) => match rx.wait_for(|r| r.is_some()).await {
                Ok(r) => r.clone().expect("waited for a receipt"),
                Err(_) => return StatusReply::NotReady { reason: format!("{}: the stop that was under way ended without a receipt", self.held_name()) },
            },
            Next::Run(tx) => {
                let span = tracing::info_span!(
                    parent: &parent,
                    "rdm.node_admin.status.update.via-stop-node",
                    node = %self.held_name(), operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
                    commander = %named(commander.as_ref()), state = "Leaving", receipt = tracing::field::Empty, "otel.kind" = "internal"
                );
                let receipt = self.stop_steps(commander.as_ref(), spare, &op).instrument(span.clone()).await;
                span.record("receipt", receipt.name());
                *self.cycle.lock().unwrap() = Cycle::Stopped(receipt.clone());
                let _ = tx.send(Some(receipt.clone()));
                receipt
            }
        };
        StatusReply::Stopped { receipt }
    }

    /// The stop's own steps (node-stop.md steps 2-3): the whole `node.connections.delete { All }`,
    /// then the transport cut; the receipt is the All's.
    async fn stop_steps(&self, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, op: &LifecycleOp) -> ConnectionsReceipt {
        use tracing::Instrument;
        parking().park();
        if let Some(retire) = &self.retire {
            retire.cancel();
        }
        // 1. node.connections.delete { All } (node-stop.md step 2), whole: admission of new
        // application work is blocked, the eligible work in flight finishes under the bound, and every
        // open Direct or Proxy entry is closed, its drop written under the name the birth holds. The
        // receipt never turns a deadline into zero.
        let deleted = self
            .delete_connections(&ConnectionScope::All)
            .instrument(tracing::info_span!("rdm.node_admin.node.connections.delete.via-stop", node = %self.held_name(), operation = %op.operation, commander = %named(commander)))
            .await;
        let receipt = deleted.receipt.expect("All carries its receipt");
        let session = self.session.lock().unwrap().clone();
        // 2. The node's own tasks are held: no turn begins, and every turn already past its wait
        // point runs to its end before anything more is said or cut.
        if let Some(session) = &session {
            let span = tracing::info_span!(
                "rdm.node_admin.node.update.via-stop-quiesce",
                node = %self.held_name(), operation = %op.operation, turns_in_progress = tracing::field::Empty, waited_for = tracing::field::Empty
            );
            let held = session.park_tasks().instrument(span.clone()).await;
            span.record("turns_in_progress", held.turns_in_progress);
            span.record("waited_for", held.waited_for.as_str());
        }
        // 3. Leaving is declared to the authority while this node can still reach it, then no more
        // is said: the node gossips nothing for its stop.
        if let Some(declare) = &self.declare {
            declare(NodeState::Leaving).await;
        }
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
        // 5. The stop's own step after its connections are deleted (node-stop.md step 3): cut its
        // transport, its gossip neighbours and its Node RPC pool, sparing the commander's connection the
        // stop call rides. Never its endpoint. The cut runs inside its own span: the span's start is
        // when the cut began, and what it cut is recorded on it once it ends.
        let cut = tracing::info_span!(
            "rdm.node_admin.node.update.via-stop-transport-cut",
            node = %self.held_name(), operation = %op.operation,
            neighbours = tracing::field::Empty, pooled = tracing::field::Empty, accepted = tracing::field::Empty, turns_in_progress = tracing::field::Empty
        );
        let report = match &session {
            Some(session) => session.cut(spare.as_slice()).instrument(cut.clone()).await,
            None => CutReport::default(),
        };
        cut.record("neighbours", report.neighbours);
        cut.record("pooled", report.pooled);
        cut.record("accepted", report.accepted);
        cut.record("turns_in_progress", report.turns_in_progress);
        // stopping is this node's own status from the moment its tasks are cut; nothing publishes it.
        let _ = (self.set_status)(MemberStatus::Leaving);
        cut.in_scope(|| tracing::info!("the stop cut its transport: gossip neighbours and the Node RPC pool; the endpoint stays bound and the commander's connection stays open"));
        receipt
    }

    /// The start: a parked birth rejoins as itself through its session. A birth that is not parked
    /// refuses it, naming the state it is in. A start that reaches the birth while another start of
    /// it is under way is the same natural key `(node_id, incarnation, Pending)`: it runs no rejoin
    /// of its own and answers that start's outcome, and its call is the one the start's join goes
    /// back over from then on (node-start.md step 2: the start joins back over the call that
    /// commanded it, and this call commanded it last).
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
                Run(tokio::sync::watch::Sender<Option<StatusReply>>, ConnectionsReceipt, Arc<dyn Session>, Option<tokio::sync::watch::Receiver<iroh::PublicKey>>),
                Join(tokio::sync::watch::Receiver<Option<StatusReply>>),
            }
            let next = {
                let mut cycle = self.cycle.lock().unwrap();
                match &mut *cycle {
                    Cycle::Stopped(r) => {
                        let receipt = r.clone();
                        let Some(session) = self.session.lock().unwrap().clone() else {
                            return StatusReply::NotReady { reason: format!("{}: this process holds no session to rejoin its mesh with", self.held_name()) };
                        };
                        let (tx, rx) = tokio::sync::watch::channel(None);
                        let (commander, commanded) = match spare {
                            Some(key) => {
                                let (tx, rx) = tokio::sync::watch::channel(key);
                                (Some(tx), Some(rx))
                            }
                            None => (None, None),
                        };
                        *cycle = Cycle::Starting { outcome: rx, commander, calls: spare.into_iter().collect() };
                        Next::Run(tx, receipt, session, commanded)
                    }
                    Cycle::Starting { outcome, commander, calls } => {
                        // This call commanded the start last: its join goes back over this call from
                        // its next attempt on.
                        if let (Some(key), Some(commander)) = (spare, commander.as_ref()) {
                            commander.send_replace(key);
                            calls.push(key);
                        }
                        Next::Join(outcome.clone())
                    }
                    Cycle::Stopping(_) => return StatusReply::NotReady { reason: format!("{}: a stop is still under way; start is the transition out of Leaving and only for a parked process", self.held_name()) },
                    Cycle::Live(Some((build_id, operation))) if *build_id == op.build_id && *operation == op.operation => return StatusReply::AlreadyApplied,
                    Cycle::Live(_) => {
                        return StatusReply::RejectedInvalidNodeTransition { current: crate::status_rpc::declared_of_member((self.status)()) };
                    }
                }
            };
            let (outcome, receipt, session, commanded) = match next {
                Next::Join(mut rx) => {
                    tracing::Span::current().record("joined", true);
                    return match rx.wait_for(|r| r.is_some()).await {
                        Ok(r) => r.clone().expect("waited for an outcome"),
                        Err(_) => StatusReply::NotReady { reason: format!("{}: the start that was under way ended without an outcome", self.held_name()) },
                    };
                }
                Next::Run(tx, receipt, session, commanded) => (tx, receipt, session, commanded),
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
            // The admin that commanded the start answers its join, reached back over its own call:
            // the call's endpoint key and path (node-start.md step 2).
            let joined = match commanded {
                Some(commander) => session.rejoin(retire, commander, rows).await,
                None => Err(StartFailed { step: "node.join", reason: format!("{}: commander-uncalled: the start came on no Node RPC call, so no endpoint names the admin to join back", self.held_name()) }),
            };
            let reply = match joined {
                Ok(()) => {
                    // A new cycle: its next drain is a new drain.
                    let mut cycle = self.cycle.lock().unwrap();
                    *cycle = Cycle::Live(Some((op.build_id.clone(), op.operation.clone())));
                    drop(cycle);
                    parking().unpark();
                    StatusReply::Started
                }
                Err(failed) => {
                    // The start stopped at its step: the node is parked again, as it was: its tasks
                    // held (a turn its rejoin let begin runs to its end), then its connections cut,
                    // sparing every call commanding the start so each is answered.
                    let _ = (self.set_status)(MemberStatus::Leaving);
                    self.server.drain();
                    let _ = session.park_tasks().await;
                    let calls = match &*self.cycle.lock().unwrap() {
                        Cycle::Starting { calls, .. } => calls.clone(),
                        _ => spare.into_iter().collect(),
                    };
                    let _ = session.cut(&calls).await;
                    *self.cycle.lock().unwrap() = Cycle::Stopped(receipt);
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
