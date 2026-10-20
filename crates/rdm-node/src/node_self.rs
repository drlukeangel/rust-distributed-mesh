//! A node birth obeying the commands of its owning mesh-admin: `node.connections.delete`,
//! `node.stop` and `node.start` (node-connections-delete.md, node-stop.md, node-start.md). Every node
//! kind runs this one logic over its own surface: an rpc node, a role node and a node-admin differ
//! only in how they refuse new work, how they publish and how they leave and rejoin the mesh (their
//! [`Session`]).
//!
//! - `DeleteConnections`: `All` blocks admission of new application work, lets the eligible work in
//!   flight finish under the birth's bound ([`NodeSelf::connections_delete_deadline`], from its launch or
//!   its admin config), closes every open Direct or Proxy entry
//!   of the connections log and answers `ConnectionsDeleted` with its `ConnectionsReceipt`
//!   (`Established`, or `Deadline` at the bound). `Connections` closes exactly the named entries.
//!   Gossip neighbours and the Node RPC pool are not connections: neither scope touches them.
//! - `StopNode`: the whole `node.connections.delete { All }`, then hold the node's own tasks, declare
//!   `Leaving` to the authority, publish a mesh-primary node-admin's last aggregate with itself
//!   `Leaving` (gossip.md, "Mesh delete"), then the stop's own transport cut (gossip neighbours and the
//!   Node RPC pool; never the endpoint and never the commander's connection the call rides), and answer
//!   `StopConnectionsDeleted` (Data) with the `All`'s receipt and `NodeStopped` (Terminal) on that stream. The process stays alive,
//!   its endpoint bound.
//! - `StartNode`: a stopped birth rejoins as itself (same node id, incarnation, endpoint key and
//!   port) through its [`Session`], and answers `Started`, or `StartFailed` naming the step.
//! - `Reserved6` and `Reserved7` are reserved Status positions (R-CR4): refused by name.
//!
//! Nothing here waits on an answer for correctness: a lost reply is the commander's indeterminate,
//! never this node's retry.

use crate::connections_writer::HeldConnections;
use crate::model::{IncarnationId, NodeId, PathName};
use rdm_core::connections::{ConnectionKind, ConnectionState, NodeConnection};
use rdm_core::views::{ConnectionView, ConnectionsView};
use rdm_core::{LifecycleOp, MemberStatus, MeshDigest};
use rdm_transport::membership::Membership;
use rdm_transport::{NodeRpcClient, NodeRpcServer, NodeTarget};
use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
use rdm_core::status::{ConnectionScope, ConnectionsReceipt, NodeState, Status, StatusReply, StatusRequest, WhichMesh};
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
    pub closed: Vec<rdm_core::ConnectionId>,
    /// The ids named that are no open entry of the birth: unknown, or already closed.
    pub not_open: Vec<rdm_core::ConnectionId>,
    /// The open entries the birth still holds after the cut, a drop the storage refused included.
    pub held: Vec<rdm_core::ConnectionId>,
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
    /// `node.join`, `node.before-ready`, `node.topology` or `node.rafka-time`.
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
    /// run the `before_ready` hook with `AfterStop` in its context under `retire`, the new cycle's token.
    /// `commander` holds the endpoint key of the call that commanded the start last: each join
    /// attempt goes back over that call ([`crate::workflows::node_start::JoinTo::Commander`]), and
    /// the admin that answers is the hook's accepting authority. `rows` is the start's hold on this
    /// birth's connections rows: the session drops it once it holds the name its join answer handed,
    /// and no earlier, so the join's connection is recorded under that name (node-start.md step 2).
    /// `operation` is the start's own: the ReadyForTraffic the rejoin declares names it (i146 Q36).
    async fn rejoin(&self, retire: rdm_transport::CancelToken, commander: tokio::sync::watch::Receiver<iroh::PublicKey>, rows: Option<crate::connections_writer::RowsHeld>, operation: rdm_core::status::DeclaredOperation, progress: Option<CommandProgress>) -> Result<(), StartFailed>;
    /// Leave the channels this birth is on (its mesh; the backbone too, a node-admin), its hook task
    /// ended and its hook gate closed, its connections kept: what a live birth does before its start
    /// sequence runs again on a commanded `node.mesh.join` (node-mesh-join.md step 10, Q87). A stop's
    /// cut does this and then cuts the connections.
    async fn leave_channels(&self);
}

/// Declares a state this birth entered to its authority, with the operation that produced it when
/// one did (i146 Q36).
pub type Declare = Arc<dyn Fn(NodeState, Option<rdm_core::status::DeclaredOperation>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// The operation a stop by this process's own signal names on its leave step's spans.
pub const SIGNAL_STOP: &str = "signal";

/// Who a stop answers to (i146 Q55): the mesh-primary's `StopNode` on its command stream, or this
/// process's own signal, the node its own subject. One workflow serves both ([`NodeSelf::stop_steps`]).
enum StopBy<'a> {
    /// The command: who commanded it, the connection its call rides, its operation and its stream.
    Command { commander: Option<&'a NodeId>, spare: Option<iroh::PublicKey>, op: &'a LifecycleOp, progress: Option<CommandProgress> },
    /// This process's own signal.
    Signal,
}

/// What a stop by this process's own signal leaves behind (i146 Q55): the receipt of its
/// `node.connections.delete { All }`, and each declaration it owes its mesh-primary, one owed call
/// apiece that the process's exit drops (node-rpc.md §34).
pub struct SignalStopped {
    /// The `All`'s receipt.
    pub receipt: ConnectionsReceipt,
    /// The owed declarations: connections-deleting, then stopped.
    pub declarations: Vec<tokio::task::JoinHandle<()>>,
}

/// The operation `op` names, as a declaration carries it (i146 Q36).
pub(crate) fn declared(op: &LifecycleOp) -> rdm_core::status::DeclaredOperation {
    rdm_core::status::DeclaredOperation { build_id: op.build_id.clone(), attempt: op.attempt, operation: op.operation.clone() }
}

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
    pub resolver: Arc<rdm_transport::LiveNodeResolver>,
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
    /// retired, so its `before_ready` hook and any pull it has in flight end.
    pub retire: Option<rdm_transport::Retirement>,
    /// The backbone, for a node-admin: its round hooks go out on its mesh channel and the backbone.
    /// `None` for an ordinary node: its hooks go out on its mesh channel and its mesh-admin carries
    /// them onto the backbone.
    pub backbone: Option<rdm_transport::membership::Backbone>,
    /// The bound of this birth's `node.connections.delete { All }`, standalone or inside a stop
    /// (`RDM_CONNECTIONS_DELETE_DEADLINE_MS`, read once where the process reads its launch or its
    /// admin config): the eligible work in flight is let finish until it passes. Strictly shorter
    /// than the stop ladder's grace.
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
    /// The process's data-plane `ConnectionsRegistry`: `node.connections.delete` closes active
    /// `Connection`s by their row id and blocks new connections under `All`.
    data_connections: Mutex<Option<rdm_transport::connections::ConnectionsRegistry>>,
    /// What this birth completes before it checks in to a commit-state or open-traffic round.
    round_actions: Mutex<Arc<dyn crate::rounds::RoundActions>>,
    /// The rounds this birth took, by round, and how far it took each.
    rounds_seen: Mutex<std::collections::HashMap<crate::rounds::RoundKey, RoundProgress>>,
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
        resolver: Arc<rdm_transport::LiveNodeResolver>,
        membership: Membership,
        set_status: Arc<dyn Fn(MemberStatus) -> MeshDigest + Send + Sync>,
        status: Arc<dyn Fn() -> MemberStatus + Send + Sync>,
        declare: Option<Declare>,
        connections_delete_deadline: Duration,
    ) -> Self {
        membership.set_author(&rdm_core::PublisherId { node: name.to_string(), node_id: node_id.clone(), incarnation: incarnation.clone() });
        Self { node_id, incarnation, name, server, client, resolver, membership, set_status, status, declare, connections_delete_deadline, retire: None, backbone: None, cycle: Mutex::new(Cycle::Live(None)), session: Mutex::new(None), data_plane: Mutex::new(None), connections: Mutex::new(None), data_connections: Mutex::new(None), round_actions: Mutex::new(Arc::new(crate::rounds::NoScratchpad)), rounds_seen: Mutex::new(std::collections::HashMap::new()) }
    }

    /// The name this birth holds and publishes: its row's name once it took one (live through
    /// `ApplyNodeName`, or from its join answer), else its birth's (R12). Every span, refusal and
    /// frame of this surface names it.
    pub fn held_name(&self) -> PathName {
        self.membership.held_name(&self.name)
    }

    /// This surface, publishing its round hooks on `backbone` as well as its mesh channel.
    pub fn with_backbone(mut self, backbone: rdm_transport::membership::Backbone) -> Self {
        self.backbone = Some(backbone);
        self
    }

    /// This surface, cancelling `retire` when `node.connections.delete { All }` or a stop is admitted.
    pub fn with_retire(mut self, retire: rdm_transport::Retirement) -> Self {
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

    /// This surface, holding the process's data-plane `ConnectionsRegistry`.
    pub fn set_data_connections(&self, registry: rdm_transport::connections::ConnectionsRegistry) {
        *self.data_connections.lock().unwrap() = Some(registry);
    }

    /// `node.cert.renew` on this birth (node-cert-renew.md step 4, Reply): the cert must decode and
    /// name this exact birth; it is installed, so every connection this birth opens from now on
    /// presents it, and answered `node.cert.renewed`. Nothing is closed, set or published. A cert
    /// that does not decode, or names another birth, is refused by name and the held one kept.
    pub fn renew_cert(&self, cert: &[u8]) -> StatusReply {
        let me = self.held_name();
        // The refusal is the reply's: the authority's `via-renewal` span names it.
        let refuse = rdm_core::reply::status::rejected_cert;
        let signed = match rdm_core::certs::cert_from_bytes(cert) {
            Ok(s) => s,
            Err(e) => return refuse(format!("{me} ({} {}): the cert does not decode: {e}", self.node_id, self.incarnation.as_str())),
        };
        if signed.cert.node_id != self.node_id || signed.cert.incarnation != self.incarnation {
            return refuse(format!(
                "the cert names {} ({} {}), not this birth {me} ({} {})",
                signed.cert.name,
                signed.cert.node_id,
                signed.cert.incarnation.as_str(),
                self.node_id,
                self.incarnation.as_str()
            ));
        }
        let Some(registry) = self.data_connections.lock().unwrap().clone() else {
            return rdm_core::reply::status::not_ready(format!("{me} is still starting: it holds no connections registry to present a cert from"));
        };
        registry.set_member_cert(cert.to_vec());
        tracing::info_span!("rdm.node.cert.update.via-installed", node = %me, node_id = %self.node_id, incarnation_id = %self.incarnation.as_str(), expires_at = signed.cert.expiry_ms)
            .in_scope(|| tracing::info!("the renewed member cert is installed: every connection opened from now presents it"));
        StatusReply::CertRenewed { node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), expires_at: signed.cert.expiry_ms }
    }

    /// The fabric's root set this birth admits on (`node.connections.admit`): the one its admissions
    /// read.
    pub fn roots(&self) -> Vec<rdm_core::certs::VerifyingKey> {
        self.data_connections.lock().unwrap().as_ref().map(|r| r.roots().current()).unwrap_or_default()
    }

    /// The connections this birth holds now (R-CR1), from its connections writer; refused by name
    /// when the process has not handed it one, so nothing is decided on a set it cannot read.
    pub async fn held_routes(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, String> {
        let writer = self.connections.lock().unwrap().clone();
        let data_connections = self.data_connections.lock().unwrap().clone();
        match writer {
            Some(w) => {
                let mut routes = w.held_routes().await;
                if let Some(reg) = data_connections {
                    routes.extend(reg.held_open_tunnels());
                }
                Ok(routes)
            }
            None => Err(format!("{}: this birth holds no connections writer, so the connections it holds cannot be read", self.held_name())),
        }
    }

    /// `node.connections.get` at this birth (fabric-mesh-node-lifecycles.md: "Reading or cutting a
    /// node's held connections"): the connections it holds now, under the name it holds, once every
    /// observation handed to its writer has landed; the ids are those `node.connections.delete` names.
    pub async fn connections_get(&self) -> StatusReply {
        let writer = self.connections.lock().unwrap().clone();
        let data_connections = self.data_connections.lock().unwrap().clone();
        match writer {
            Some(w) => {
                let mut held = w.held_connections().await;
                if let Some(reg) = data_connections {
                    held.routes.extend(reg.held_open_tunnels());
                }
                connections_answer(&held)
            }
            None => rdm_core::reply::status::not_ready(format!("{}: this birth holds no connections writer, so the connections it holds cannot be read", self.held_name())),
        }
    }

    /// `node.connections.delete` at this birth (R-CR5; node-connections-delete.md): a connection is
    /// an open Direct or Proxy entry of this birth's connections log, and nothing else. `All` closes
    /// every open entry; `Connections` closes exactly the entries it names. Each closed entry gets its
    /// drop, written under the name the birth holds; an id that is no open entry is named
    /// `not_open`. Gossip neighbours and the Node RPC pool are not connections: neither scope touches
    /// them, so a birth whose connections are deleted is still heard on gossip and still answers calls.
    pub async fn delete_connections(&self, scope: &ConnectionScope) -> Deleted {
        self.delete_connections_reporting(scope, || async {}).await
    }

    /// [`Self::delete_connections`], calling `admitted` once an `All`'s block on new application work
    /// is in force and before it waits for the work in flight: where the node command reports
    /// connections-deleting on its stream (i144 R22). `Connections` never calls it: it sets no
    /// node-wide state.
    pub async fn delete_connections_reporting<F, Fut>(&self, scope: &ConnectionScope, admitted: F) -> Deleted
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        self.delete_connections_asked(scope, 1, admitted).await
    }

    /// [`Self::delete_connections_reporting`], `asking` the calls in flight that are the asker's own
    /// and not eligible work: the command's one call, none for this process's own signal.
    async fn delete_connections_asked<F, Fut>(&self, scope: &ConnectionScope, asking: u64, admitted: F) -> Deleted
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let writer = self.connections.lock().unwrap().clone();
        // `All` is graceful node-wide closure (node-connections-delete.md step 3): admission of new
        // application work is blocked, and the eligible work in flight (every dispatched handler but
        // this call's own) is let finish, bounded by the birth's `connections_delete_deadline`.
        // The birth is retired at the admission of `All`: its `before_ready` hook and any pull
        // it has in flight end; it returns to service only through a stop and a start.
        let in_flight = match scope {
            ConnectionScope::All => {
                if let Some(retire) = &self.retire {
                    retire.cancel();
                }
                // The block on new application work: from here the node refuses new non-status calls
                // (Node RPC §35); the node is connections-deleting, and says so.
                self.server.begin_connections_delete();
                admitted().await;
                let waited = self.await_in_flight(asking).await;
                // Every connection open once admitted work has finished is closed, the ones that
                // work opened while `All` waited included (step 3.2: "they belong to the same
                // closure"). From here the birth opens no new connection until it starts (proposal
                // node-properties-own-commands.md §2.1, §2.4): blocked before the open set is read,
                // so none opens between the read and the close.
                self.block_new_connections("node.connections.delete All closed this birth's connections");
                Some(waited)
            }
            ConnectionScope::Connections(_) => None,
        };
        let mut open = match &writer {
            Some(w) => w.held_routes().await,
            None => Vec::new(),
        };
        if let Some(reg) = self.data_connections.lock().unwrap().clone() {
            open.extend(reg.held_open_tunnels());
        }
        let (routes, not_open) = match scope {
            ConnectionScope::All => (open, Vec::new()),
            ConnectionScope::Connections(ids) => {
                let (mut routes, mut not_open): (Vec<rdm_core::connections::NodeConnection>, Vec<rdm_core::ConnectionId>) = (Vec::new(), Vec::new());
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
        let data_connections = self.data_connections.lock().unwrap().clone();
        if let Some(reg) = &data_connections {
            for route in &routes {
                reg.close_by_id(&route.id, rdm_transport::connections::Closed::ConnectionsDeleted);
            }
        }
        // Each Direct entry closed is its data-plane connection closed, on that connection's own
        // span naming the entry (R-CR6: the pool's hard cut is the stop's transport cut alone); a
        // Proxy rides its carrier's Direct and only its route retires.
        let data_plane = self.data_plane.lock().unwrap().clone();
        if let Some(data_plane) = &data_plane {
            for route in routes.iter().filter(|r| r.kind == rdm_core::connections::ConnectionKind::Direct) {
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

    /// The admission block (node-name-update.md Precondition: "No new connection is admitted from the
    /// row write through `ApplyNodeName`"; node-replace.md: a node is renamed only once "nothing is
    /// connected to it and nothing routes to it"): the client this birth originates its logged
    /// connections on opens none from here until it starts or joins its mesh again. The Node RPC pool
    /// and gossip are not connections and are untouched.
    fn block_new_connections(&self, cause: &str) {
        if let Some(data_plane) = self.data_plane.lock().unwrap().clone() {
            data_plane.block_new_connections(format!("{} ({}): {cause}; it opens no new connection until it starts or joins its mesh again", self.held_name(), self.node_id));
        }
        if let Some(reg) = self.data_connections.lock().unwrap().clone() {
            reg.block_new_connections();
        }
    }

    /// Lift the admission block: a birth that starts, or that joins its mesh again, opens
    /// connections again.
    fn admit_new_connections(&self) {
        if let Some(data_plane) = self.data_plane.lock().unwrap().clone() {
            data_plane.admit_new_connections();
        }
        if let Some(reg) = self.data_connections.lock().unwrap().clone() {
            reg.unblock_new_connections();
        }
    }

    /// Block admission of new application work and wait for the eligible work in flight to reach
    /// zero, bounded by the birth's `connections_delete_deadline`. The `asking` calls that asked are in
    /// flight while it waits and are not eligible work. The node refuses new non-status calls with typed
    /// `ConnectionsDeleting` from here on (Node RPC §35); control streams stay usable.
    async fn await_in_flight(&self, asking: u64) -> InFlight {
        self.server.begin_connections_delete();
        let stats = self.server.stats();
        let eligible = || rdm_transport::ServerStats::get(&stats.in_flight).saturating_sub(asking);
        let began = eligible();
        let until = tokio::time::Instant::now() + self.connections_delete_deadline;
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

    /// Serve a node command from `commander` on its ordered reply stream (op 0x22, i144 R22; Luke Q6,
    /// Q7): every refusal is decided before `CommandAccepted`; after it the stream ends only with the
    /// command's completion. `node.connections.delete { All }` reports `ConnectionsDeleting` once its
    /// block on new application work is in force, then closes and answers `ConnectionsDeleted`;
    /// `Connections([…])` answers its completion right after the acceptance. A caller gone after the
    /// acceptance stops nothing: the node completes what it accepted.
    pub async fn serve_node_command(self: &Arc<Self>, commander: Option<&NodeId>, req: &NodeCommandRequest, frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        self.serve_node_command_on(commander, None, req, frames).await
    }

    /// Serve the command on its caller's connection, which a stop spares and a start joins back over.
    pub async fn serve_node_command_on(self: &Arc<Self>, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, req: &NodeCommandRequest, mut frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        use tracing::Instrument;
        if !matches!(req, NodeCommandRequest::DeleteConnections { .. }) {
            return self.serve_lifecycle_command(commander, spare, req, frames).await;
        }
        let NodeCommandRequest::DeleteConnections { incarnation, scope, .. } = req else { unreachable!("checked") };
        if *incarnation != self.incarnation {
            return rdm_core::reply::node_command::rejected_stale_incarnation(self.incarnation.clone());
        }
        if self.connections.lock().unwrap().is_none() {
            return rdm_core::reply::node_command::not_ready(format!("{}: this birth holds no connections writer yet; nothing is cut", self.held_name()));
        }
        if let Some(reason) = scope.refusal() {
            return rdm_core::reply::node_command::rejected_connection_scope(format!("{}: {reason}", self.held_name()));
        }
        let accepted = NodeCommandReply::CommandAccepted { node_id: self.node_id.clone(), incarnation: self.incarnation.clone() };
        if let Err(e) = frames.accepted(accepted).await {
            // Nothing was accepted: the caller is gone before the acceptance left, so nothing runs.
            return rdm_core::reply::node_command::not_ready(format!("{}: the command's caller is gone before its acceptance ({e}); nothing is cut", self.held_name()));
        }
        let sink = std::sync::Arc::new(tokio::sync::Mutex::new(frames));
        let (node, node_id, incarnation) = (self.held_name(), self.node_id.clone(), self.incarnation.clone());
        let commander_name = named(commander);
        let report = {
            let sink = sink.clone();
            move || async move {
                // The span opens before the report leaves, so it starts before whatever the report causes
                // (an in-process recorder may take it before the send returns).
                let span = tracing::info_span!("rdm.node_admin.node.connections.delete.via-connections-deleting", node = %node, commander = %commander_name, outcome = tracing::field::Empty, "otel.kind" = "internal");
                let sent = sink.lock().await.progress(rdm_core::reply::node_command::connections_deleting(node_id, incarnation)).instrument(span.clone()).await;
                let outcome = match &sent {
                    Ok(()) => "sent".to_string(),
                    Err(e) => format!("caller gone: {e}"),
                };
                span.record("outcome", outcome.as_str());
                span.in_scope(|| tracing::info!("node.connections.delete All is admitted: the block on new application work is in force; the node reports connections-deleting on the command's stream"));
            }
        };
        let span = tracing::info_span!(
            "rdm.node_admin.node.connections.delete.via-delete-connections",
            node = %self.held_name(), commander = %named(commander), scope = scope.name(), named = match scope { ConnectionScope::All => 0, ConnectionScope::Connections(ids) => ids.len() },
            deadline_ms = tracing::field::Empty, closed = tracing::field::Empty, not_open = tracing::field::Empty, held = tracing::field::Empty, receipt = tracing::field::Empty,
            outcome = tracing::field::Empty, reason = tracing::field::Empty, "otel.kind" = "internal"
        );
        let d = async {
            if matches!(scope, ConnectionScope::All) {
                tracing::Span::current().record("deadline_ms", self.connections_delete_deadline.as_millis() as u64);
            }
            self.delete_connections_reporting(scope, report).await
        }
        .instrument(span.clone())
        .await;
        span.record("closed", d.closed.len()).record("not_open", d.not_open.len()).record("held", d.held.len());
        if let Some(r) = &d.receipt {
            span.record("receipt", r.name());
        }
        span.record("outcome", "connections-deleted");
        span.in_scope(|| tracing::info!("node.connections.delete decided at this birth"));
        NodeCommandReply::ConnectionsDeleted { closed: d.closed, not_open: d.not_open, held: d.held, receipt: d.receipt }
    }

    /// Whether this birth is parked: stopped, its process alive, waiting for a start or a delete.
    /// A birth whose start is under way is parked until the start ends.
    pub fn is_parked(&self) -> bool {
        matches!(*self.cycle.lock().unwrap(), Cycle::Stopped(_) | Cycle::Starting { .. })
    }

    /// Whether a stop of this birth has been admitted: under way, done, or followed by a start that
    /// has not ended. Only a birth with no stop admitted runs node.stop's steps on its own signal.
    pub fn stop_admitted(&self) -> bool {
        !matches!(*self.cycle.lock().unwrap(), Cycle::Live(_))
    }

    /// This surface, completing `actions` before it checks in to a round.
    pub fn with_round_actions(self, actions: Arc<dyn crate::rounds::RoundActions>) -> Self {
        self.set_round_actions(actions);
        self
    }

    /// Complete `actions` before checking in to a round, from the next round this birth takes.
    pub fn set_round_actions(&self, actions: Arc<dyn crate::rounds::RoundActions>) {
        *self.round_actions.lock().unwrap() = actions;
    }

    /// Serve `commit-state` or `open-traffic` from `commander`, the primary of this birth's mesh
    /// (the caller resolved it). The command is admitted at once; the birth then completes its
    /// action and calls the matching completion at `commander`. A round this birth already took,
    /// whichever commander sends it, is answered `AlreadyApplied` and its action is not run again:
    /// the check-in is made to the commander that sends it, at once when the action is stored and
    /// otherwise once it is. That is how a new primary's checklist rebuilds itself.
    fn serve_round(self: &Arc<Self>, commander: &NodeId, req: &StatusRequest) -> StatusReply {
        let op = crate::rounds::RoundOp::of(req).expect("a round op");
        // Answered by the birth the call reached (R-RX5: no receiver refuses a downward call for the
        // node id it names); the exact birth it names is checked, so a call that reached another
        // birth is refused naming the one held.
        if *op.incarnation != self.incarnation {
            return rdm_core::reply::stale(self.incarnation.clone());
        }
        let want = op.kind.operation(op.fabric_id);
        if op.operation != want {
            return rdm_core::reply::status::not_ready(format!("{}: {} names operation {}, the round's is {want}", self.held_name(), req.op(), op.operation));
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
                rdm_core::reply::applied()
            }
            Admit::CheckIn => {
                tokio::spawn(async move { me.check_in_again(commander, key, parent).await });
                rdm_core::reply::already_applied()
            }
            Admit::Joined => rdm_core::reply::already_applied(),
        }
    }

    /// The round of this birth: its action (once per round), then the completion call at its
    /// commander and at every commander that sent the round while the action ran.
    async fn round(self: Arc<Self>, commander: NodeId, key: crate::rounds::RoundKey, parent: tracing::Span) {
        use tracing::Instrument;
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-member-round", node = %self.held_name(), round = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, commander = %commander, acted = true, "otel.kind" = "internal");
        async {
            let actions = self.round_actions.lock().unwrap().clone();
            if let Err(e) = crate::rounds::run_action(&*actions, &self.held_name(), key.kind).await {
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
    async fn check_in_again(self: Arc<Self>, commander: NodeId, key: crate::rounds::RoundKey, parent: tracing::Span) {
        use tracing::Instrument;
        let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-member-round", node = %self.held_name(), round = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, commander = %commander, acted = false, "otel.kind" = "internal");
        self.check_in(&commander, &key).instrument(span).await
    }

    /// The check-in hook, then the completion owed at `commander`, naming `key`'s round (node-rpc.md
    /// §43, i146 Q44): one Owed call, ended by its answer, by the runtime's proof that its receiver is
    /// gone, or by the primary seat of this birth's mesh moving to another node, which is its one
    /// retargeting (Q39): the call to the old holder is dropped and the completion is owed to the new
    /// one. A completion whose receiver was proven gone is owed to the next node the seat names. The
    /// hook runs once, however often the completion is sent.
    async fn check_in(&self, commander: &NodeId, key: &crate::rounds::RoundKey) {
        let req = key.kind.up(key, self.node_id.clone(), self.incarnation.clone());
        // The check-in hook runs in the path of the completion call, before it goes out.
        let frame = crate::rounds::hook_frame(key, true, self.node_id.clone(), self.incarnation.clone(), &self.held_name().to_string(), self.membership.clock().now_rafka_ms());
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
        let owed = rdm_transport::CallOptions { budget: rdm_transport::Budget::Owed, ..rdm_transport::CallOptions::default() };
        let holder = || self.membership.seats().mesh(&self.name.mesh).map(|h| (h.node_id, h.incarnation));
        let mut seats = self.membership.seats().subscribe();
        let mut to = commander.clone();
        loop {
            seats.borrow_and_update();
            let from = holder();
            let called = to.clone();
            // The seat moving to a node other than the one called: the call is dropped and retargeted.
            let moved = async {
                loop {
                    if seats.changed().await.is_err() {
                        std::future::pending::<()>().await;
                    }
                    if let Some((next, _)) = holder().filter(|h| Some(h) != from.as_ref() && h.0 != called) {
                        return next;
                    }
                }
            };
            let target = NodeTarget::ExactNode(to.clone());
            let out = tokio::select! {
                (out, _) = self.client.call::<Status>(&target, &req, &owed) => out,
                next = moved => {
                    self.retargeted(key, &to, &next, "seat-moved");
                    to = next;
                    continue;
                }
            };
            let answer = match &out {
                rdm_core::outcome::RpcOutcome::Reply(r) => r.value().name().to_string(),
                other => format!("{}: {}", other.name(), other.code().name()),
            };
            tracing::info_span!("rdm.node_admin.status.update.via-completion-call", node = %self.held_name(), completion = key.kind.completion(), build_id = %key.build_id, attempt = key.attempt, commander = %to, hook = %hook, answer = %answer, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the completion call was made to the commanding primary"));
            if out.reply().is_some() {
                return;
            }
            // No answer, and nothing but the receiver's end or its owner ends an Owed call: the
            // completion is owed to the next node the seat names.
            let next = loop {
                if let Some((next, _)) = holder().filter(|h| h.0 != to) {
                    break next;
                }
                if seats.changed().await.is_err() {
                    return;
                }
            };
            self.retargeted(key, &to, &next, &answer);
            to = next;
        }
    }

    /// `key`'s completion, owed at `from`, is owed at `to` from now on, because of `why`.
    fn retargeted(&self, key: &crate::rounds::RoundKey, from: &NodeId, to: &NodeId, why: &str) {
        tracing::info_span!("rdm.node_admin.status.update.via-completion-retargeted", node = %self.held_name(), completion = key.kind.completion(), build_id = %key.build_id, attempt = key.attempt, from = %from, to = %to, why, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the completion is owed to the holder of this birth's mesh's primary seat"));
    }

    /// Serve `StopNode` or `StartNode` on the exact birth it names (`node.connections.delete` reaches a
    /// birth on the node command op 0x22, [`Self::serve_node_command`]); `Reserved6`
    /// and `Reserved7` are reserved positions, refused by name, whoever calls
    /// (node-rpc.md §14: nothing inside the mesh checks who is calling): `commander` is the caller as
    /// the receiver resolved it (`None` when it holds no birth for the calling endpoint), `spare` the
    /// call's own connection. A round command is served for `commander`, the primary of this birth's
    /// mesh, which its check-in goes to. `None` for any other request.
    pub async fn serve(self: &Arc<Self>, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, req: &StatusRequest) -> Option<StatusReply> {
        if matches!(req, StatusRequest::CommitState { .. } | StatusRequest::OpenTraffic { .. }) {
            return Some(match commander {
                Some(commander) => self.serve_round(commander, req),
                None => rdm_core::reply::status::not_ready(format!("{}: no primary of its mesh is known to check {} in to", self.held_name(), req.op())),
            });
        }
        if matches!(req, StatusRequest::Reserved6 { .. } | StatusRequest::Reserved7 { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. }) {
            return Some(refuse_reserved(&self.held_name().to_string(), &named(commander), req));
        }
        if let StatusRequest::LeaveMesh { incarnation, operation, .. } = req {
            if *incarnation != self.incarnation {
                return Some(rdm_core::reply::stale(self.incarnation.clone()));
            }
            return Some(self.serve_leave_mesh(spare, operation).await);
        }
        if let StatusRequest::JoinMesh { incarnation, build_id, attempt, operation, .. } = req {
            if *incarnation != self.incarnation {
                return Some(rdm_core::reply::stale(self.incarnation.clone()));
            }
            let declared = rdm_core::status::DeclaredOperation { build_id: build_id.clone(), attempt: *attempt, operation: operation.clone() };
            return Some(self.serve_join_mesh(spare, declared).await);
        }
        None
    }

    /// Q47: admission and progress of the exact node's stop/start on 0x22.
    async fn serve_lifecycle_command(self: &Arc<Self>, commander: Option<&NodeId>, spare: Option<iroh::PublicKey>, req: &NodeCommandRequest, mut frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        let (incarnation, build_id, attempt, operation) = match req {
            NodeCommandRequest::StopNode { incarnation, build_id, attempt, operation, .. }
            | NodeCommandRequest::StartNode { incarnation, build_id, attempt, operation, .. } => (incarnation, build_id, attempt, operation),
            _ => unreachable!("lifecycle command"),
        };
        if *incarnation != self.incarnation {
            return rdm_core::reply::node_command::rejected_stale_incarnation(self.incarnation.clone());
        }
        let stopping = matches!(req, NodeCommandRequest::StopNode { .. });
        let refusal = {
            let cycle = self.cycle.lock().unwrap();
            match &*cycle {
                Cycle::Starting { .. } if stopping => Some(rdm_core::reply::node_command::not_ready(format!("{}: a start is under way; the stop is decided on the cycle it ends in", self.held_name()))),
                Cycle::Stopping(_) if !stopping => Some(rdm_core::reply::node_command::not_ready(format!("{}: a stop is still under way; start is the transition out of Leaving and only for a parked process", self.held_name()))),
                Cycle::Live(Some((build, op))) if !stopping && build == build_id && op == operation => None,
                Cycle::Live(_) if !stopping => Some(rdm_core::reply::node_command::rejected_invalid_node_transition(crate::status::declared_of_member((self.status)()))),
                _ => None,
            }
        };
        if let Some(refusal) = refusal { return refusal; }
        if frames.accepted(NodeCommandReply::CommandAccepted { node_id: self.node_id.clone(), incarnation: self.incarnation.clone() }).await.is_err() {
            return rdm_core::reply::node_command::not_ready(format!("{}: caller gone before CommandAccepted; nothing executed", self.held_name()));
        }
        let progress = Arc::new(tokio::sync::Mutex::new(frames));
        let op = LifecycleOp { node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), name: self.held_name(), build_id: build_id.clone(), attempt: *attempt, operation: operation.clone(), event_at_rafka_ms: self.membership.clock().now_rafka_ms() };
        let result = if stopping {
            self.stop(commander.cloned(), spare, op, tracing::Span::current(), Some(progress)).await
        } else {
            self.start(commander.cloned(), spare, op, tracing::Span::current(), Some(progress)).await
        };
        match result {
            StatusReply::Stopped { .. } => NodeCommandReply::NodeStopped,
            StatusReply::Started | StatusReply::AlreadyApplied => NodeCommandReply::NodeStarted,
            StatusReply::StartFailed { step, reason } => rdm_core::reply::node_command::failed(step, reason, rdm_core::outcome::Code::FailedPrecondition),
            other => rdm_core::reply::node_command::failed(req.op().into(), format!("{}: {other:?}", other.name()), <rdm_core::status::Status as rdm_core::protocol::NodeProtocol>::classify_reply(&other).code()),
        }
    }

    /// Standalone `node.mesh.leave` (`LeaveMesh` on 0x1B; node-mesh-leave.md): refuses
    /// `BusyHoldingConnections` when the connections log still holds an open entry, otherwise
    /// leaves the mesh channel (and backbone, for a node-admin) and cuts the Node RPC pool.
    async fn serve_leave_mesh(&self, spare: Option<iroh::PublicKey>, operation: &str) -> StatusReply {
        let writer = self.connections.lock().unwrap().clone();
        if let Some(w) = &writer {
            let open = w.held_routes().await;
            if !open.is_empty() {
                let connections: Vec<rdm_core::ConnectionId> = open.into_iter().map(|r| r.id).collect();
                tracing::info_span!(
                    "rdm.node.mesh.reject.via-busy-holding-connections",
                    node = %self.held_name(),
                    node_id = %self.node_id,
                    mesh = %self.membership.mesh_name(),
                    mesh_id = %self.membership.mesh_id(),
                    operation = %operation,
                    connections = ?connections,
                    "otel.kind" = "internal"
                )
                .in_scope(|| tracing::info!("node.mesh.leave refused: this birth still holds open connections"));
                return rdm_core::reply::status::busy_holding_connections(connections);
            }
        }
        let (_, left) = self.leave_mesh(spare.as_slice(), operation).await;
        StatusReply::MeshLeft { node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), left }
    }

    /// The `node.mesh.leave` primitive (node-mesh-leave.md): shared by standalone `LeaveMesh` and
    /// `node.stop` step 3. Emits `rdm.node.mesh.update.via-leaving`, leaves the mesh channel (and
    /// the backbone for a node-admin, each emitting `rdm.node.mesh.update.via-left`), and cuts the
    /// Node RPC pool while sparing `spare`.
    async fn leave_mesh(&self, spare: &[iroh::PublicKey], operation: &str) -> (CutReport, Vec<WhichMesh>) {
        use tracing::Instrument;
        let leaving_span = tracing::info_span!(
            "rdm.node.mesh.update.via-leaving",
            node = %self.held_name(),
            node_id = %self.node_id,
            mesh = %self.membership.mesh_name(),
            mesh_id = %self.membership.mesh_id(),
            operation = %operation,
            "otel.kind" = "internal"
        );
        async {
            // The primitive's progress, `leaving`, from its command until the mesh is left: nothing
            // publishes it, in that state or after it (node-mesh-leave.md).
            let _ = (self.set_status)(MemberStatus::Leaving);
            let session = self.session.lock().unwrap().clone();
            let report = match &session {
                Some(session) => session.cut(spare).await,
                None => {
                    self.membership.park().await;
                    tracing::info_span!(
                        "rdm.node.mesh.update.via-left",
                        node = %self.held_name(),
                        node_id = %self.node_id,
                        mesh = %self.membership.mesh_name(),
                        mesh_id = %self.membership.mesh_id(),
                        which = WhichMesh::Own.name(),
                    )
                    .in_scope(|| tracing::info!("left the mesh channel"));
                    if let Some(backbone) = &self.backbone {
                        backbone.park().await;
                        tracing::info_span!(
                            "rdm.node.mesh.update.via-left",
                            node = %self.held_name(),
                            node_id = %self.node_id,
                            mesh = %self.membership.mesh_name(),
                            mesh_id = %self.membership.mesh_id(),
                            which = WhichMesh::Backbone.name(),
                        )
                        .in_scope(|| tracing::info!("left the backbone channel"));
                    }
                    CutReport::default()
                }
            };
            let mut left = vec![WhichMesh::Own];
            if self.backbone.is_some() {
                left.push(WhichMesh::Backbone);
            }
            (report, left)
        }
        .instrument(leaving_span)
        .await
    }

    /// `node.mesh.join` commanded on this live birth (`JoinMesh` on 0x1B; node-mesh-join.md step 10,
    /// node-tickle.md "What follows"; Q87): the whole start sequence runs again on it, from the join
    /// back over the commanding call to its hook, its topology, its Build floor and rafka-time
    /// refreshed last (node-start.md steps 2 to 5), the way a start after a stop does. The birth leaves
    /// its channels first and says Pending; the command is answered `MeshJoined` once the join is
    /// accepted and the channel entered (node.mesh.joined), while the rest of the start runs on, and a
    /// start that ends before that answers how it ended. A parked birth is started by `node.start`,
    /// never by this.
    async fn serve_join_mesh(self: &Arc<Self>, spare: Option<iroh::PublicKey>, operation: rdm_core::status::DeclaredOperation) -> StatusReply {
        let Some(key) = spare else {
            return rdm_core::reply::status::not_ready(format!("{}: commander-uncalled: JoinMesh came on no Node RPC call, so no endpoint names the admin to join back", self.held_name()));
        };
        let Some(session) = self.session.lock().unwrap().clone() else {
            return rdm_core::reply::status::not_ready(format!("{}: this process holds no session to join its mesh with", self.held_name()));
        };
        let (outcome_tx, outcome_rx) = tokio::sync::watch::channel(None);
        let (commander_tx, commander_rx) = tokio::sync::watch::channel(key);
        {
            let mut cycle = self.cycle.lock().unwrap();
            match &*cycle {
                Cycle::Live(_) => {}
                Cycle::Starting { .. } => return rdm_core::reply::status::not_ready(format!("{}: a start is under way; node.mesh.join is decided on the cycle it ends in", self.held_name())),
                Cycle::Stopping(_) | Cycle::Stopped(_) => return rdm_core::reply::status::not_ready(format!("{}: a parked birth is started by node.start, not by node.mesh.join", self.held_name())),
            }
            *cycle = Cycle::Starting { outcome: outcome_rx.clone(), commander: Some(commander_tx), calls: vec![key] };
        }
        // The birth leaves the channels it is on and says Pending: its hook gates its app ops again,
        // and it is published ReadyForTraffic only once the hook has passed.
        session.leave_channels().await;
        let _ = (self.set_status)(MemberStatus::Pending);
        self.server.resume();
        self.admit_new_connections();
        let retire = match &self.retire {
            Some(r) => r.renew(),
            None => rdm_transport::CancelToken::new(),
        };
        // The command's completion is the mesh held again (node-mesh-join.md step 9): the topic parked
        // above is re-subscribed by the start's channel step. No progress stream: the start owes its
        // ReadyForTraffic declaration to its authority as a start after a stop does.
        let mut parked = self.membership.parked_changes();
        parked.borrow_and_update();
        let me = self.clone();
        tokio::spawn(async move {
            let result = session.rejoin(retire, commander_rx, None, operation, None).await;
            let reply = match result {
                Ok(()) => StatusReply::Started,
                Err(failed) => {
                    // The sequence stopped at a step: the birth stays Pending and leaves its channels
                    // again, so its authority holds it unheard and commands it back once more
                    // (node-recovery.md step 2); nothing here retries by hand.
                    tracing::info_span!("rdm.mesh.node.reject.via-mesh-join-start-failed", node = %me.held_name(), step = failed.step, reason = %failed.reason)
                        .in_scope(|| tracing::info!("the commanded start stopped at its step: the birth leaves its channels for its authority's next command"));
                    session.leave_channels().await;
                    rdm_core::reply::status::start_failed(failed.step.to_string(), failed.reason)
                }
            };
            *me.cycle.lock().unwrap() = Cycle::Live(None);
            let _ = outcome_tx.send(Some(reply));
        });
        let joined = if self.backbone.is_some() { vec![WhichMesh::Own, WhichMesh::Backbone] } else { vec![WhichMesh::Own] };
        let mesh_joined = || StatusReply::MeshJoined { node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), joined: joined.clone() };
        let mut outcome = outcome_rx;
        let mut parked_closed = false;
        loop {
            tokio::select! {
                changed = parked.changed(), if !parked_closed => match changed {
                    Ok(()) if !*parked.borrow() => return mesh_joined(),
                    Ok(()) => {}
                    Err(_) => parked_closed = true,
                },
                changed = outcome.changed() => {
                    if changed.is_err() {
                        return rdm_core::reply::status::not_ready(format!("{}: the start that was under way ended without an outcome", self.held_name()));
                    }
                    if let Some(reply) = outcome.borrow().clone() {
                        return match reply {
                            StatusReply::Started => mesh_joined(),
                            other => other,
                        };
                    }
                }
            }
        }
    }

    /// The stop (node-stop.md): the whole `node.connections.delete { All }`, declare `Leaving`, the
    /// stop's own transport cut. The receipt is the `All`'s. A second stop of the same birth, while this one runs or after it, answers the same
    /// receipt: stopping a stopped birth is done. A stop while a start of the birth is under way is
    /// refused naming the start.
    async fn stop(self: &Arc<Self>, commander: Option<NodeId>, spare: Option<iroh::PublicKey>, op: LifecycleOp, parent: tracing::Span, progress: Option<CommandProgress>) -> StatusReply {
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
                Cycle::Starting { .. } => return rdm_core::reply::status::not_ready(format!("{}: a start is under way; the stop is decided on the cycle it ends in", self.held_name())),
                Cycle::Stopping(rx) => Next::Wait(rx.clone()),
                Cycle::Live(_) => {
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    *cycle = Cycle::Stopping(rx);
                    Next::Run(tx)
                }
            }
        };
        let receipt = match next {
            Next::Done(r) => {
                if let Some(progress) = &progress { let _ = progress.lock().await.progress(NodeCommandReply::StopConnectionsDeleted { receipt: r.clone() }).await; }
                r
            },
            Next::Wait(mut rx) => {
                let receipt = match rx.wait_for(|r| r.is_some()).await {
                    Ok(r) => r.clone().expect("waited for a receipt"),
                    Err(_) => return rdm_core::reply::status::not_ready(format!("{}: the stop that was under way ended without a receipt", self.held_name())),
                };
                if let Some(progress) = &progress { let _ = progress.lock().await.progress(NodeCommandReply::StopConnectionsDeleted { receipt: receipt.clone() }).await; }
                receipt
            },
            Next::Run(tx) => {
                let span = tracing::info_span!(
                    parent: &parent,
                    "rdm.node_admin.status.update.via-stop-node",
                    node = %self.held_name(), operation = %op.operation, build_id = %op.build_id, attempt = op.attempt,
                    commander = %named(commander.as_ref()), state = "Stopped", receipt = tracing::field::Empty, "otel.kind" = "internal"
                );
                let (receipt, _) = self.stop_steps(StopBy::Command { commander: commander.as_ref(), spare, op: &op, progress: progress.clone() }).instrument(span.clone()).await;
                span.record("receipt", receipt.name());
                *self.cycle.lock().unwrap() = Cycle::Stopped(receipt.clone());
                let _ = tx.send(Some(receipt.clone()));
                receipt
            }
        };
        StatusReply::Stopped { receipt }
    }

    /// node.stop's steps (node-stop.md steps 2-4), one workflow over the primitives for both of its
    /// subjects (i146 Q55): the mesh-primary's `StopNode` on its command stream, or this process's own
    /// signal. The whole `node.connections.delete { All }`, the node's own tasks held, `node.mesh.leave`,
    /// then `Stopped` to the mesh-primary. A command's stop answers on its stream and declares `Stopped`
    /// naming its operation; a signal's stop owes connections-deleting at the block and `Stopped` after
    /// the leave, waits on no answer, and hands each owed declaration back for the process's exit to
    /// drop (node-rpc.md §34). The receipt is the All's.
    async fn stop_steps(&self, by: StopBy<'_>) -> (ConnectionsReceipt, Vec<tokio::task::JoinHandle<()>>) {
        use tracing::Instrument;
        let (commander, spare, op, progress) = match by {
            StopBy::Command { commander, spare, op, progress } => (commander, spare, Some(op), progress),
            StopBy::Signal => (None, None, None, None),
        };
        let operation = op.map_or(SIGNAL_STOP, |o| o.operation.as_str());
        // A commanded stop parks its live process; a signal's process ends with its stop.
        if op.is_some() {
            parking().park();
        }
        if let Some(retire) = &self.retire {
            retire.cancel();
        }
        // 1. node.connections.delete { All } (node-stop.md step 2), whole: admission of new
        // application work is blocked, the eligible work in flight finishes under the bound, and every
        // open Direct or Proxy entry is closed, its drop written under the name the birth holds. The
        // receipt never turns a deadline into zero. A command's own call is in flight and is not
        // eligible work; a signal has none, and owes its connections-deleting once the block is in force.
        let deleting = Mutex::new(None);
        let deleted = self
            .delete_connections_asked(&ConnectionScope::All, u64::from(op.is_some()), || {
                if op.is_none() {
                    *deleting.lock().unwrap() = self.owe(NodeState::ConnectionsDeleting, None);
                }
                async {}
            })
            .instrument(tracing::info_span!("rdm.node_admin.node.connections.delete.via-stop", node = %self.held_name(), operation = %operation, commander = %named(commander)))
            .await;
        let mut owed: Vec<tokio::task::JoinHandle<()>> = deleting.into_inner().unwrap().into_iter().collect();
        let receipt = deleted.receipt.expect("All carries its receipt");
        if let Some(progress) = &progress {
            let _ = progress.lock().await.progress(NodeCommandReply::StopConnectionsDeleted { receipt: receipt.clone() }).await;
        }
        let session = self.session.lock().unwrap().clone();
        // 2. The node's own tasks are held: no turn begins, and every turn already past its wait
        // point runs to its end before anything more is said or cut.
        if let Some(session) = &session {
            let span = tracing::info_span!(
                "rdm.node_admin.node.update.via-stop-quiesce",
                node = %self.held_name(), operation = %operation, turns_in_progress = tracing::field::Empty, waited_for = tracing::field::Empty
            );
            let held = session.park_tasks().instrument(span.clone()).await;
            span.record("turns_in_progress", held.turns_in_progress);
            span.record("waited_for", held.waited_for.as_str());
        }
        // 4. R-NS1 item 2 (gossip.md mesh-deleted section): a node-admin that still publishes its
        // mesh's aggregate (the last admin of a deleted mesh, still its primary) publishes it once
        // more before the cut, itself Leaving: "Its last aggregate before a stop carries itself
        // Leaving." It follows the hold, so no gated turn publishes after it; after the cut it
        // publishes nothing (node-stop.md step 4, R-ON1).
        if let (Some(backbone), Some(op)) = (self.backbone.as_ref().filter(|b| b.is_mesh_primary()), op) {
            let mesh_deleted = self.membership.book.statuses.mesh(&self.held_name().mesh).is_some_and(|h| h.status == "deleted" || h.status == "dead");
            if (!op.is_restart() && !op.is_stop()) || mesh_deleted {
                let del_op = if op.is_stop() {
                    LifecycleOp { operation: format!("node.delete:{}", self.held_name()), ..op.clone() }
                } else {
                    op.clone()
                };
                self.membership.book.deleting(del_op);
            }
            let me = (self.set_status)(MemberStatus::Leaving);
            let mut members: Vec<MeshDigest> = backbone.own_members(&self.membership);
            match members.iter_mut().find(|d| d.node.node_id == me.node.node_id) {
                Some(slot) => *slot = me.clone(),
                None => members.push(me.clone()),
            }
            let count = members.len();
            // Best-effort dissemination (R-NS2): the stop does not wait for its delivery, and the cut
            // below follows its send at once.
            let published = if backbone.withholds("stopping-aggregate", "backbone") { "withheld" } else if backbone.publish(&self.membership, members).await { "true" } else { "false" };
            if published == "true" && mesh_deleted {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            tracing::info_span!("rdm.node_admin.node.update.via-leaving-aggregate", node = %self.held_name(), mesh = %self.held_name().mesh, operation = %op.operation, status = "Leaving", members = count, published)
                .in_scope(|| tracing::info!("the last aggregate before the stop carries this node Leaving"));
        }
        // 5. The stop's own step after its connections are deleted (node-stop.md step 3;
        // node-mesh-leave.md): leave the mesh and cut its transport, its gossip neighbours and its
        // Node RPC pool, sparing the commander's connection the stop call rides. Never its endpoint.
        let cut = tracing::info_span!(
            "rdm.node_admin.node.update.via-stop-transport-cut",
            node = %self.held_name(), operation = %operation,
            neighbours = tracing::field::Empty, pooled = tracing::field::Empty, accepted = tracing::field::Empty, turns_in_progress = tracing::field::Empty
        );
        let (report, _) = self.leave_mesh(spare.as_slice(), operation).instrument(cut.clone()).await;
        cut.record("neighbours", report.neighbours);
        cut.record("pooled", report.pooled);
        cut.record("accepted", report.accepted);
        cut.record("turns_in_progress", report.turns_in_progress);
        // The cut's span ends with the cut: what follows is the declaration, not the cut.
        cut.in_scope(|| tracing::info!("the stop cut its transport: gossip neighbours and the Node RPC pool; the endpoint stays bound and the commander's connection stays open"));
        drop(cut);
        // 6. This birth declares `Stopped` to its mesh-primary (i146 Q29, Q36, Q55; node-stop.md step 4): an
        // owed message whose delivery is the runtime's (node-rpc.md §43), never waited on, so the stop's
        // receipt and reply never wait for the mesh-primary's answer. A command's names its stop: the owed
        // twin of the reply's `stopped`; whichever of the two reaches the mesh-primary first is applied and
        // the other answers AlreadyApplied (node-stop.md step 5, Q49). A signal's names no operation.
        owed.extend(self.owe(NodeState::Stopped, op.map(declared)));
        (receipt, owed)
    }

    /// Owe `state` to the mesh-primary without waiting for its answer (node-rpc.md §43: an owed message's
    /// delivery is the runtime's): one owed call, handed back for the process's exit to drop (node-rpc.md
    /// §34). A command's stop names its operation; a signal's names none (i146 Q55).
    fn owe(&self, state: NodeState, operation: Option<rdm_core::status::DeclaredOperation>) -> Option<tokio::task::JoinHandle<()>> {
        let declare = self.declare.clone()?;
        match &operation {
            Some(op) => tracing::info_span!("rdm.node_rpc.status.update.via-owed-on-stop", node = %self.held_name(), state = ?state, operation = %op.operation, build_id = %op.build_id, attempt = op.attempt)
                .in_scope(|| tracing::info!("the state is owed to the mesh-primary, naming the stop; the stop does not wait for its answer")),
            None => tracing::info_span!("rdm.node_rpc.status.update.via-owed-on-signal", node = %self.held_name(), state = ?state)
                .in_scope(|| tracing::info!("the state is owed to the mesh-primary; the stop does not wait for its answer")),
        }
        Some(tokio::spawn(declare(state, operation)))
    }

    /// The stop this process's own signal runs: node.stop's steps with the node its own subject
    /// ([`Self::stop_steps`], i146 Q55).
    pub async fn stop_on_signal(&self) -> SignalStopped {
        let (receipt, declarations) = self.stop_steps(StopBy::Signal).await;
        SignalStopped { receipt, declarations }
    }

    /// The start: a parked birth rejoins as itself through its session. A birth that is not parked
    /// refuses it, naming the state it is in. A start that reaches the birth while another start of
    /// it is under way is the same natural key `(node_id, incarnation, Pending)`: it runs no rejoin
    /// of its own and answers that start's outcome, and its call is the one the start's join goes
    /// back over from then on (node-start.md step 2: the start joins back over the call that
    /// commanded it, and this call commanded it last).
    async fn start(self: &Arc<Self>, commander: Option<NodeId>, spare: Option<iroh::PublicKey>, op: LifecycleOp, parent: tracing::Span, progress: Option<CommandProgress>) -> StatusReply {
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
                            return rdm_core::reply::status::not_ready(format!("{}: this process holds no session to rejoin its mesh with", self.held_name()));
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
                    Cycle::Stopping(_) => return rdm_core::reply::status::not_ready(format!("{}: a stop is still under way; start is the transition out of Leaving and only for a parked process", self.held_name())),
                    Cycle::Live(Some((build_id, operation))) if *build_id == op.build_id && *operation == op.operation => return rdm_core::reply::already_applied(),
                    Cycle::Live(_) => {
                        return rdm_core::reply::status::rejected_invalid_node_transition(crate::status::declared_of_member((self.status)()));
                    }
                }
            };
            let (outcome, receipt, session, commanded) = match next {
                Next::Join(mut rx) => {
                    tracing::Span::current().record("joined", true);
                    let reply = match rx.wait_for(|r| r.is_some()).await {
                        Ok(r) => r.clone().expect("waited for an outcome"),
                        Err(_) => rdm_core::reply::status::not_ready(format!("{}: the start that was under way ended without an outcome", self.held_name())),
                    };
                    if matches!(reply, StatusReply::Started) {
                        if let Some(progress) = &progress { let _ = progress.lock().await.progress(NodeCommandReply::NodeJoined).await; }
                    }
                    return reply;
                }
                Next::Run(tx, receipt, session, commanded) => (tx, receipt, session, commanded),
            };
            let retire = match &self.retire {
                Some(r) => r.renew(),
                None => rdm_transport::CancelToken::new(),
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
            self.admit_new_connections();
            // The admin that commanded the start answers its join, reached back over its own call:
            // the call's endpoint key and path (node-start.md step 2).
            let joined = match commanded {
                Some(commander) => session.rejoin(retire, commander, rows, declared(&op), progress).await,
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
                    // The start stopped at its step: the node is parked again, as it was, through the
                    // stop's own primitives: its connections deleted (the calls commanding the start
                    // are in flight and not eligible work), its tasks held (a turn its rejoin let begin
                    // runs to its end), its mesh left, sparing every call commanding the start so each
                    // is answered.
                    let calls = match &*self.cycle.lock().unwrap() {
                        Cycle::Starting { calls, .. } => calls.clone(),
                        _ => spare.into_iter().collect(),
                    };
                    let _ = self.delete_connections_asked(&ConnectionScope::All, calls.len() as u64, || async {}).await;
                    let _ = session.park_tasks().await;
                    let _ = self.leave_mesh(&calls, &op.operation).await;
                    *self.cycle.lock().unwrap() = Cycle::Stopped(receipt);
                    tracing::Span::current().record("failed_step", failed.step);
                    rdm_core::reply::status::start_failed(failed.step.to_string(), failed.reason)
                }
            };
            outcome.send_replace(Some(reply.clone()));
            reply
        }
        .instrument(span.clone())
        .await;
        span.record("node", self.held_name().to_string().as_str());
        span.record("outcome", reply.name());
        reply
    }
}

/// A commander as a span field: its node id, or empty when the call resolved none.
fn named(commander: Option<&NodeId>) -> String {
    commander.map(ToString::to_string).unwrap_or_default()
}

/// The record this runtime's provider wrote for it (`rdm_core::runtime::read_own_record`),
/// waited for on the caller's runtime for at most `within`: a boot that ends (a stop signal) drops
/// the wait at once, where a blocking wait would hold the runtime's shutdown until `within` passed.
pub async fn wait_for_own_record(dir: &std::path::Path, within: std::time::Duration) -> Result<rdm_core::RuntimeFact, String> {
    let until = tokio::time::Instant::now() + within;
    loop {
        let last = match rdm_core::runtime::read_own_record(dir) {
            Ok(f) => return Ok(f),
            Err(e) => e,
        };
        if tokio::time::Instant::now() >= until {
            return Err(format!("the provider wrote no runtime record for this runtime within {within:?}: {last}"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// What a receiver of a `NodeLeft` call holds instead of a command it could complete: nothing, by
/// design (node-rpc-envelope.md, "Node stop command pair").
pub const NODE_LEFT_IS_NEVER_A_CALL: &str = "no command: node-left is never a call; a stopped birth reports left on the reply of its stop-node call";

/// What a reserved Status position is refused with: none is ever a call (R-CR4).
pub const RETIRED_PAIR_IS_RESERVED: &str = "no command: Status positions 6, 7, 8 (StopNode), and 16 (StartNode) are reserved, never executed; lifecycle commands use NodeCommand 0x22";

/// A reserved Status position at `receiver` (node-rpc-envelope.md "Node stop command
/// pair": "a receiver refuses them by name"): nothing starts, and the reply names the position.
pub fn refuse_reserved(receiver: &str, caller: &str, req: &StatusRequest) -> StatusReply {
    let (StatusRequest::Reserved6 { node_id, incarnation, build_id, attempt, operation } | StatusRequest::Reserved7 { node_id, incarnation, build_id, attempt, operation } | StatusRequest::StopNode { node_id, incarnation, build_id, attempt, operation } | StatusRequest::StartNode { node_id, incarnation, build_id, attempt, operation }) = req else {
        unreachable!("refuse_reserved is called for the reserved pair only")
    };
    let reported = format!("{} {operation} (build {build_id}, attempt {attempt})", req.op());
    tracing::info_span!(
        "rdm.node_admin.status.reject.via-reserved-position",
        node = %receiver, op = req.op(), operation = %operation, build_id = %build_id, attempt = *attempt, subject = %node_id, incarnation_id = %incarnation.as_str(),
        caller = %caller, field = "command", expected = RETIRED_PAIR_IS_RESERVED, reported = %reported, "otel.kind" = "internal"
    )
    .in_scope(|| tracing::info!("a reserved Status position was refused by name: it is never a call and starts nothing"));
    rdm_core::reply::status::rejected_unmatched_completion("command".into(), RETIRED_PAIR_IS_RESERVED.into(), reported)
}

/// The answer of a Status door that has no handler for `req` (R24, Luke Q11): `RequestNotHandled`
/// naming the request, decided by dispatch alone, never by node kind, seat or sender, on the
/// receiver's reject span `rdm.node.status.reject.via-request-not-handled`. `receiver` is the door's
/// held name. Nothing is done.
pub fn request_not_handled(receiver: &str, req: &StatusRequest) -> StatusReply {
    tracing::info_span!("rdm.node.status.reject.via-request-not-handled", node = %receiver, request = req.op(), "otel.kind" = "internal")
        .in_scope(|| tracing::info!("this Status door has no handler for the request: not handled here, nothing done"));
    rdm_core::reply::not_handled(req.op().to_string())
}

/// The refusal of a `NodeLeft` call at any receiver, a node-admin or an rpc node (node-rpc-envelope.md,
/// "Node stop command pair": "NodeLeft keeps its position and is never sent; a receiver refuses it by
/// name"). A stop's completion is its `Stopped` reply, so no command is open that a node-left call
/// completes: it is `RejectedUnmatchedCompletion { field: "command" }`, `expected` what the receiver
/// holds instead ([`NODE_LEFT_IS_NEVER_A_CALL`]) and `reported` what the call named. Nothing is
/// completed, whatever command is open for the birth. `name` is the subject's path.name as the
/// receiver holds it (empty when it holds none) and `caller` who called, both for the reject span
/// only; nothing checks who is calling (node-rpc.md §14). Never `NotReady`, the readiness gate's
/// answer (R-NR1).
pub fn refuse_node_left(receiver: &str, name: &str, caller: &str, req: &StatusRequest) -> StatusReply {
    let StatusRequest::NodeLeft { node_id, incarnation, build_id, attempt, operation } = req else { unreachable!("refuse_node_left is called for node-left only") };
    let reported = format!("{} {operation} (build {build_id}, attempt {attempt})", req.op());
    tracing::info_span!(
        "rdm.node_admin.status.reject.via-node-left",
        node = %receiver,
        op = req.op(),
        operation = %operation,
        build_id = %build_id,
        attempt = *attempt,
        subject = %node_id,
        incarnation_id = %incarnation.as_str(),
        name = %name,
        caller = %caller,
        field = "command",
        expected = NODE_LEFT_IS_NEVER_A_CALL,
        reported = %reported,
        "otel.kind" = "internal"
    )
    .in_scope(|| tracing::info!("a node-left call was refused by name: it is never a call, and it completes no command"));
    rdm_core::reply::status::rejected_unmatched_completion("command".into(), NODE_LEFT_IS_NEVER_A_CALL.into(), reported)
}


/// One fact as a connection list carries it.
pub fn connection_view(f: &NodeConnection) -> ConnectionView {
    ConnectionView {
        id: f.id.to_string(),
        source: f.source.name.to_string(),
        destination: f.destination.name.to_string(),
        kind: match f.kind {
            ConnectionKind::Direct => "direct",
            ConnectionKind::Proxy => "proxy",
        }
        .into(),
        state: match f.state {
            ConnectionState::Connected => "connected",
            ConnectionState::Disconnected => "disconnected",
            ConnectionState::Failed => "failed",
            ConnectionState::Refused => "refused",
        }
        .into(),
        carrier: f.carrier.as_ref().map(|c| c.name.to_string()),
        reason: f.reason.clone(),
        logged_at_ms: f.logged_at_ms,
    }
}

/// `node.connections.get` answered from `held`, one read of the node's connections writer: the
/// connections the node holds (its `connected` latest Directs and its active Proxies, each named by
/// the minted id `node.connections.delete` names it by), under the name it holds. Its span names the
/// node, how many it answered and whether its set is complete.
pub fn connections_answer(held: &HeldConnections) -> StatusReply {
    let view = ConnectionsView { node: held.name.to_string(), complete: held.complete, connections: held.routes.iter().map(connection_view).collect() };
    let (node, count, complete) = (&held.name, view.connections.len(), view.complete);
    let reply = StatusReply::within_reply_bound(StatusReply::Connections { connections: view }, "node.connections.get");
    match &reply {
        StatusReply::ReadOverBound { bytes, max, .. } => tracing::info_span!("rdm.node_admin.node.reject.via-over-bound", node = %node, read = "node.connections.get", connections = count, bytes, max, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the held connections encode past the Status reply bound; nothing of them is sent")),
        _ => tracing::info_span!("rdm.node_admin.node.serve.via-connections-get", node = %node, connections = count, complete, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the connections the node holds answered")),
    }
    reply
}


/// Where a node command's frames before its completion go (i144 R22; Luke Q9): the command's own
/// ordered reply stream (op 0x22, [`StreamFrames`]), or an in-process channel when a mesh-primary is the
/// subject of the request it carries ([`ChannelFrames`]). The frames are the same either way.
#[async_trait::async_trait]
pub trait CommandFrames: Send {
    /// The command's acceptance: its first frame.
    async fn accepted(&mut self, frame: NodeCommandReply) -> Result<(), String>;
    /// A progress frame after the acceptance.
    async fn progress(&mut self, frame: NodeCommandReply) -> Result<(), String>;
}

/// The admitted command's progress, shared with its lifecycle steps.
pub type CommandProgress = Arc<tokio::sync::Mutex<Box<dyn CommandFrames>>>;

/// A node command's frames on its ordered reply stream (op 0x22).
pub struct StreamFrames {
    not_started: Option<rdm_transport::stream::ReplySink<NodeCommand, rdm_transport::stream::NotStarted>>,
    streaming: Option<rdm_transport::stream::ReplySink<NodeCommand, rdm_transport::stream::Streaming>>,
}

impl StreamFrames {
    /// The frames of the stream `sink` writes.
    pub fn new(sink: rdm_transport::stream::ReplySink<NodeCommand, rdm_transport::stream::NotStarted>) -> Self {
        Self { not_started: Some(sink), streaming: None }
    }
}

#[async_trait::async_trait]
impl CommandFrames for StreamFrames {
    async fn accepted(&mut self, frame: NodeCommandReply) -> Result<(), String> {
        let sink = self.not_started.take().ok_or("the command was already accepted")?;
        self.streaming = Some(sink.started(frame).await.map_err(|e| format!("{e:?}"))?);
        Ok(())
    }
    async fn progress(&mut self, frame: NodeCommandReply) -> Result<(), String> {
        self.streaming.as_mut().ok_or("no acceptance was sent")?.data(frame).await.map_err(|e| format!("{e:?}"))
    }
}

/// A node command's frames in-process: a mesh-primary that is the subject of the request it carries
/// runs its own birth's command path and reads the same frames from here.
pub struct ChannelFrames(pub tokio::sync::mpsc::UnboundedSender<NodeCommandReply>);

#[async_trait::async_trait]
impl CommandFrames for ChannelFrames {
    async fn accepted(&mut self, frame: NodeCommandReply) -> Result<(), String> {
        self.0.send(frame).map_err(|_| "the mesh-primary's carrier is gone".to_string())
    }
    async fn progress(&mut self, frame: NodeCommandReply) -> Result<(), String> {
        self.0.send(frame).map_err(|_| "the mesh-primary's carrier is gone".to_string())
    }
}
