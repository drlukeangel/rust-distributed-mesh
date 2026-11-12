//! `node.start` (lifecycles/node-start.md, "End-to-end sequence"): the one start workflow. node.create
//! composes it with [`StartKind::FirstStart`] (lifecycles/node-create.md, step 3); a start of a stopped
//! node runs it with [`StartKind::AfterStop`] (node_self.rs, `StartNode`). Every node type runs this
//! sequence; a kind differs only by what it plugs into each step ([`StartNode`], [`AfterHook`]), never
//! by a copy of the sequence.
//!
//! The steps, in node-start.md's order, each a child span of the start's root (the birth's boot span
//! at a first start, `rdm.mesh.node.update.via-start` after a stop), each begun once the one before it
//! ended:
//!
//! 1. `rdm.mesh.node.update.via-join-admitted` (step 2): `JoinNode` to the authority (the admin that
//!    launched the node, or the one whose call commanded its start, reached back over that call's
//!    endpoint and path with no record of it: [`JoinTo`]), presenting the digest the node holds,
//!    Pending; the answer's rafka-time is adopted, so the hook runs with current rafka-time and the
//!    member cert, and the name it hands (its row's) is the step's `installed`: the node holds it
//!    from the channel step on, and every later step is named by it, so a node renamed while stopped
//!    is never named by the name it held before its join (R-JN1, R12). A start with no authority (the Day-0 root, a fabric-primary re-born from its durable
//!    map, a recovery start) makes no join.
//! 2. `rdm.mesh.node.update.via-topology-joined` (step 2, node.joined): the node enters its mesh
//!    channel holding the name the join handed, before it publishes anything (R-JN1), and takes the
//!    status frames the join answered. The kind then brings up what it serves while its hook runs.
//! 3. `rdm.mesh.node.update.via-pre-node-ready-attempt` (step 3): the `pre_node_ready` hook, with the
//!    start kind, the accepting authority (or why there is none), the joined channel, the member cert
//!    and rafka-time. No topology has been read: topology follows the hook.
//! 4. Once the hook passed under its `Send` (step 4), in the start itself, or in the task a spawned
//!    hook runs in:
//!    - `rdm.mesh.node.update.via-build-floor`: the type's Build floor, on every start (a node-admin
//!      reads the Build's committed records it is missing from storage, Q292; a node owes none).
//!      It reads no topology;
//!    - `rdm.mesh.node.update.via-topology-taken`: then one `GetTopology` from the node's own current
//!      mesh-primary (after a stop, the one whose call commanded the start), the final startup read, with no conditional source version (`since: None`):
//!      installed per mesh only from a complete aggregate, and the rafka-time of the same reply adopted
//!      after the install. No second topology request is made to refresh rafka-time.
//! 5. `rdm.mesh.node.update.via-ready` (step 5): the type's node.started report.

use crate::node_hooks::{AcceptingPrimary, AcceptingPrimarySource, Birth, End, HookDriver, NodeReadyCtx, NodeHooks, PreNodeReadyHandle, NoAcceptingPrimary, StartKind, PRE_NODE_READY_STEP};
use crate::model::NodeId;
use crate::node_self::StartFailed;
use crate::topology_read::TopologyRead;
use crate::wire::JoinAnswer;
use rdm_core::MeshDigest;
use rdm_gossip::channel::Channels;
use rdm_gossip::topology::TopologyView;
use rdm_transport::{CancelToken, NodeRpcClient, NodeTarget};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tracing::Instrument as _;

/// The most `JoinNode` calls a start makes, each on the default `Send`, before it ends unreached: a
/// call is made again only on a domain answer (`crate::join::call_join_to_with_listeners`).
pub const JOIN_TURNS: u32 = 5;

/// A boxed future a step's plug returns.
pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// What a start knows before it begins.
pub struct Starting {
    /// The span every step of this start is a child of.
    pub root: tracing::Span,
    /// The name the steps and their refusals carry.
    pub name: String,
    /// The birth's path: rafka-time is adopted for its mesh.
    pub birth_path: String,
    /// The birth's name, beside its path: the name the node was created at.
    pub birth_name: String,
    /// The birth's node id.
    pub node_id: NodeId,
    /// The birth's mesh id, as its launch names it: the join's spans name it.
    pub mesh_id: Option<rdm_core::MeshId>,
    /// The process's one Node RPC client: the join and the topology read go
    /// through it.
    pub client: Arc<NodeRpcClient>,
    /// The process's rafka-time, adopted from the join answer and again from the topology read, last.
    pub rafka_time: rdm_transport::clock::RafkaTime,
}

impl Starting {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.birth_path.clone(), name: self.birth_name.clone() } }
}

/// Whom a start joins.
pub enum Joining {
    /// An accepting primary, named in the join and topology steps' `launcher` as `label`.
    AcceptingPrimary {
        /// What the step spans name the accepting primary.
        label: String,
    },
    /// No accepting primary, and why: the start makes no join, reads no topology and refreshes no rafka-time.
    None(NoAcceptingPrimary),
}

/// The authority a start joined: the admin that answered its join, and where every later call of
/// the start goes.
#[derive(Clone, Debug)]
pub struct JoinThrough {
    /// Where the join went: the launcher's birth at a first start; after a stop, the endpoint and
    /// path of the call whose join was answered, which the topology read goes to as well.
    pub target: NodeTarget,
    /// The admin's endpoint key.
    pub endpoint: iroh::PublicKey,
    /// The answering admin's explicit node ID.
    pub node_id: NodeId,
    /// The short endpoint id the join's attempts name.
    pub anchor: String,
}

/// Whom a start's join goes to (node-start.md step 2), read before each attempt.
pub enum JoinTo {
    /// A first start: the admin that launched the birth, by its birth record (`target`).
    Launcher {
        /// Its birth, as this node's resolver holds it.
        target: NodeTarget,
        /// Its endpoint key.
        endpoint: iroh::PublicKey,
        /// Its path.
        path: String,
        /// The name, beside its path.
        name: String,
    },
    /// A start after a stop: the admin whose call commands the start, reached back over that call's
    /// endpoint and path. The node holds no record of it and never refuses for lacking one. A later
    /// start of the same birth, while this one is under way, is the same start (its natural key is
    /// `(node_id, incarnation, Pending)`, node-rpc-envelope.md "Status") and hands its own call's
    /// endpoint here: each join attempt goes to the call that commanded the start last.
    Commander(tokio::sync::watch::Receiver<iroh::PublicKey>),
}

/// The admin whose call reached this node from endpoint `key`, reached back over that call: the
/// endpoint key the transport proved for the call (node-rpc.md §14) and the path the call came in
/// on, which this node's endpoint holds while the call's connection is open. No record of the admin
/// is read: the node needs none (node-start.md step 2). Refused by name, with the key, when the call
/// left no active path this node's endpoint holds (`commander-unreachable`).
pub async fn reached_back(node: &str, client: &NodeRpcClient, key: &iroh::PublicKey) -> Result<NodeTarget, String> {
    let path = match client.endpoint().remote_info(*key).await {
        Some(info) => info
            .addrs()
            .filter(|a| matches!(a.usage(), iroh::endpoint::TransportAddrUsage::Active))
            .find_map(|a| match a.addr() {
                iroh::TransportAddr::Ip(addr) => Some(*addr),
                _ => None,
            }),
        None => None,
    };
    match path {
        Some(addr) => Ok(NodeTarget::Endpoint { endpoint_id: *key, addr }),
        None => Err(format!("{node}: commander-unreachable: the call from endpoint {key} that commanded this node left no active path this node's endpoint holds")),
    }
}

/// `read` made to the admin a start after a stop reads from (node-start.md steps 2 and 4): the
/// mesh-primary whose call commanded the start last (`commander`), reached back over that call, from
/// the one whose join was answered (`through`). A later call's commander replaces the one a read is
/// under way to: that read is dropped and made again to the later one, so a commander lost after
/// the join holds no read past the call that replaced it. The answer and who gave it; a failure
/// names whom the read went to.
async fn at_commander<T, E: std::fmt::Display, F, Fut>(node: &str, client: &NodeRpcClient, through: &JoinThrough, mut commander: Option<&mut tokio::sync::watch::Receiver<iroh::PublicKey>>, read: F) -> Result<(T, String), (String, String)>
where
    F: Fn(NodeTarget) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let (mut target, mut key, mut who) = (through.target.clone(), through.endpoint, through.node_id.to_string());
    loop {
        if let Some(c) = commander.as_deref_mut() {
            let now = *c.borrow_and_update();
            if now != key {
                who = format!("the admin whose call commanded it last (endpoint {})", now.fmt_short());
                target = reached_back(node, client, &now).await.map_err(|e| (e, who.clone()))?;
                key = now;
            }
        }
        let replaced = async {
            match commander.as_deref_mut() {
                Some(c) => {
                    if c.changed().await.is_err() {
                        std::future::pending::<()>().await;
                    }
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            r = read(target.clone()) => return r.map(|v| (v, who.clone())).map_err(|e| (e.to_string(), who)),
            () = replaced => {
                tracing::info!(node, from = %key.fmt_short(), "a later call commanded the start: the read under way is made again to that admin");
            }
        }
    }
}

/// Whom a first start's one topology read goes to (node-topology-get.md, "exact member → own
/// mesh-primary"): the exact birth of its mesh's current primary as its topology view names it (the
/// birth a top-up reads from, `TopologyView::mesh_primary`), with whom the step names as its server;
/// `None` while the view names none, before the read itself hands the node its primary records
/// (`topology_read::get_topology`, `learn_primaries`), and while it names this node itself (a
/// mesh-primary reads at the primary above it, node-start.md "Admission"), when the read goes to the
/// admin that answered the join.
fn own_mesh_primary(topology_view: &TopologyView, node_id: &NodeId) -> Option<(NodeTarget, String)> {
    let d = topology_view.mesh_primary().ok().filter(|d| &d.node.node_id != node_id)?;
    Some((NodeTarget::ExactNode(d.node.node_id.clone()), d.node.node_id.to_string()))
}

/// `read` made to the admin a first start reads from (node-topology-get.md, "exact member → own
/// mesh-primary"; node-start.md step 4): its mesh's current primary as its topology view names it
/// ([`own_mesh_primary`]), and the admin that answered its join (`through`) only while the view names
/// none. A primary the view names in place of the one a read is under way to replaces it: that read is
/// dropped and made again to the one named now, as a later commander replaces the one a start after a
/// stop reads from ([`at_commander`]), so a primary lost while the read is under way holds no read
/// past the record that named another. Its primary records and the births it holds are what move whom
/// the view names. The answer and who gave it; a failure names whom the read went to.
async fn at_own_mesh_primary<T, E: std::fmt::Display, F, Fut>(node: &str, node_id: &NodeId, topology_view: &TopologyView, through: &JoinThrough, read: F) -> Result<(T, String), (String, String)>
where
    F: Fn(NodeTarget) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let named = || own_mesh_primary(topology_view, node_id).unwrap_or_else(|| (through.target.clone(), through.node_id.to_string()));
    let (mut records, mut births) = (topology_view.primaries().subscribe(), topology_view.digest_view.birth_changes());
    loop {
        records.borrow_and_update();
        births.borrow_and_update();
        let (target, who) = named();
        let reading = who.clone();
        let replaced = async {
            loop {
                tokio::select! {
                    changed = records.changed() => if changed.is_err() { std::future::pending::<()>().await },
                    changed = births.changed() => if changed.is_err() { std::future::pending::<()>().await },
                }
                if named().1 != reading {
                    return;
                }
            }
        };
        tokio::select! {
            r = read(target) => return r.map(|v| (v, who.clone())).map_err(|e| (e.to_string(), who)),
            () = replaced => {
                tracing::info!(node, from = %who, to = %named().1, "the view names another mesh-primary than the one the read is under way to: that read is made again to it");
            }
        }
    }
}

/// Starts the task a node-admin's first-start hook runs in, behind the admin's own task gate.
pub type Spawn = Box<dyn FnOnce(BoxFut<()>) -> tokio::task::JoinHandle<()> + Send>;

/// How the hook runs.
pub enum HookRuns {
    /// The hook's first attempt runs inside the start, which answers its end and, when it passed,
    /// runs the steps after the hook too. When it answered `Pending`, a start after a stop runs the
    /// rest of its attempts inside the start too, so its caller is answered the hook's end; a first
    /// start returns with the node Pending and runs them, then the steps after the hook, in a task its
    /// cancel token ends.
    Inline,
    /// The start returns while the hook, then the steps after it, run in the task `spawn` starts.
    Spawned {
        /// Starts the hook's task.
        spawn: Spawn,
    },
}

/// What the hook runs with, and what the steps after it plug in.
pub struct HookParts {
    /// The exact birth.
    pub birth: Birth,
    /// The hook the embedding app registered.
    pub node_hooks: NodeHooks,
    /// Cancelled when the birth is deleted: the hook and any pull it has in flight end.
    pub cancel: CancelToken,
    /// Where the node's Ready check and its probe read the hook's state.
    pub state: Arc<Mutex<Option<PreNodeReadyHandle>>>,
    /// How the hook runs.
    pub runs: HookRuns,
    /// The type's part of the steps after the hook.
    pub after: Box<dyn AfterHook>,
}

/// What a node type plugs into the steps after the hook. It is owned: the steps run inside the start
/// when the hook runs inline, and otherwise in the task the spawned hook runs in.
#[async_trait::async_trait]
pub trait AfterHook: Send + 'static {
    /// The topology step's read is written to the topology view: the kind takes what it reaches its mesh through (its
    /// gossip peers). Runs inside the topology step's span.
    async fn topology_written(&mut self, channels: &Channels, read: &TopologyRead);

    /// The topology step is about to make its read of `primary`: the kind may wait here before the read is
    /// made. Runs inside the topology step's span. A kind with nothing to wait on makes the read at once.
    async fn before_topology_read(&mut self, primary: &NodeId) {
        let _ = primary;
    }

    /// The Build floor: resolves once the node holds the Build facts the authority's floor requires,
    /// recording on `step` (the floor step's span, which this runs inside) `owed` and `outcome`. A kind
    /// with no Build log owes none.
    async fn floor(&mut self, step: &tracing::Span) {
        step.record("owed", "none");
        step.record("outcome", "this node holds no Build log: it owes no Build floor");
    }

    /// node.started: the type's report once every step before it ended. A report its own record does not
    /// take (its Ready not recorded, Q356) is the start's failed step, answered as any other is.
    async fn report(&mut self) -> Result<(), StartFailed>;

    /// A step after the hook failed, or a start after a stop ended Failed at the hook: `inside` a start
    /// after a stop (which answers the failure; the kind ends what the start brought up), or at a first
    /// start, inside it or in the task its hook ran in (the kind names the failure where its readiness
    /// evidence is read; the node stays Pending, its process running, Q445). A first start that ended Failed at the hook calls
    /// none of it: the node stays Pending, its process keeps running, and its probe answers
    /// `StartFailed` (fabric-node-hooks.md "Where the failure lands").
    async fn start_failed(self: Box<Self>, failed: &StartFailed, inside: bool) {
        let _ = (failed, inside);
    }
}

/// What a node type plugs into `node.start`.
#[async_trait::async_trait]
pub trait StartNode: super::node_mesh_join::MeshJoin {
    /// What this start knows before it begins.
    fn starting(&self) -> Starting;

    /// Whom this start joins.
    fn joining(&self) -> Joining;

    /// Whom the join goes to; called inside the join step. A start after a stop answers
    /// [`JoinTo::Commander`], whatever the node type.
    async fn resolve_join_to(&mut self) -> Result<JoinTo, StartFailed>;
    /// The operation the start's join runs for, when the start was commanded with one (a start after
    /// a stop: its `node.start`; a commanded `node.mesh.join`: that command's); `None` names the type's.
    fn operation(&self) -> Option<rdm_core::OperationId> {
        None
    }

    /// node.joined: the node is on its channel. The kind takes what the join answered of its control
    /// state (a node-admin records the Build floor here, at every start), brings up what it serves
    /// while its hook runs (it is Pending) and hands the hook's parts. `through` is the authority the
    /// join reached and `joined` its answer (both `None` with no authority).
    async fn joined(&mut self, channels: &Channels, through: Option<&JoinThrough>, joined: Option<&JoinAnswer>) -> Result<HookParts, Self::Error>;

    /// The hook of a [`HookRuns::Spawned`] start, and the steps after it, run in `task`.
    async fn hook_started(&mut self, task: tokio::task::JoinHandle<()>) -> Result<(), Self::Error> {
        drop(task);
        Ok(())
    }
}

/// `node.start`: the join, the mesh channel, the hook, the topology, the Build floor, the rafka-time
/// refresh and the report, in that order, for `node` as it plugs into each step.
pub async fn node_start<N: StartNode>(node: &mut N, kind: StartKind) -> Result<(), N::Error> {
    let Starting { root, mut name, birth_path, birth_name, node_id, mesh_id: birth_mesh_id, client, rafka_time } = node.starting();
    let path = rdm_core::PathName { path: birth_path, name: birth_name };
    // 1. and 2. node.mesh.join (node-mesh-join.md), the one primitive: the report, the answer's
    //    rafka-time, cert and name taken before the node says anything, then the mesh channel holding
    //    that name and the status frames the join answered: node.joined. With no authority, the
    //    channel alone.
    // After a stop, the start's reads go to the admin that commanded it last (node-start.md step 2).
    let mut commander: Option<tokio::sync::watch::Receiver<iroh::PublicKey>> = None;
    let (through, joined, channels) = match node.joining() {
        Joining::AcceptingPrimary { label } => {
            let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-join-admitted", node_path = %path.path, node_name = %name, node_id = %node_id, launcher = %label, served_by = tracing::field::Empty, endpoint = tracing::field::Empty, installed = tracing::field::Empty);
            let to = node.resolve_join_to().await?;
            let digest = node.join_digest();
            let (mesh, mesh_id) = (digest.node.mesh, birth_mesh_id.as_ref().map(ToString::to_string).unwrap_or_default());
            let operation = node.operation().unwrap_or_else(|| match kind {
                StartKind::FirstStart => rdm_core::OperationId::node("node.create", &node_id),
                StartKind::AfterStop => rdm_core::OperationId::node("node.start", &node_id),
            });
            let report_parent = matches!(kind, StartKind::FirstStart).then(|| step.clone());
            let call = super::node_mesh_join::JoinCall { name: name.clone(), node_id: &node_id, mesh: &mesh, mesh_id: &mesh_id, client: &client, rafka_time: &rafka_time, to: &to, primary: &label, operation: &operation, report_parent, reported: step, channel_parent: Some(root.clone()) };
            let joined = super::node_mesh_join::node_mesh_join(node, call).await?;
            name = joined.name;
            commander = match &to {
                JoinTo::Commander(c) => Some(c.clone()),
                JoinTo::Launcher { .. } => None,
            };
            (Ok((joined.through, label)), Some(joined.answer), joined.channels)
        }
        Joining::None(why) => {
            let channel = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-topology-joined", node_path = %path.path, node_name = %name, node_id = %node_id, mesh_id = tracing::field::Empty, seeds = tracing::field::Empty);
            (Err(why), None, node.enter_channel(channel, None).await?)
        }
    };
    let member_cert = joined.as_ref().map(|a| a.control.member_cert.clone());
    let parts = node.joined(&channels, through.as_ref().ok().map(|(t, _)| t), joined.as_ref()).await?;
    // 3. The hook, with no topology read.
    let accepting_primary = match &through {
        Ok((t, _)) => AcceptingPrimarySource::Accepted(AcceptingPrimary { node_id: t.node_id.clone(), endpoint: t.endpoint, target: t.target.clone() }),
        Err(why) => AcceptingPrimarySource::None(*why),
    };
    let HookParts { birth, node_hooks, cancel, state, runs, after } = parts;
    // The steps after the hook are this cycle's too: a commanded node.mesh.join that begins the birth's next
    // cycle (its token renewed) ends them, as it ends the hook (rdm#408).
    let cycle = cancel.clone();
    let inherited = matches!(kind, StartKind::AfterStop)
        || matches!(accepting_primary, AcceptingPrimarySource::None(NoAcceptingPrimary::InheritedRoot | NoAcceptingPrimary::Recovery))
        || std::env::var("RDM_INHERITED_DATA_DIR").ok().as_deref() == Some("1");
    let ctx = NodeReadyCtx { start_kind: kind, birth, accepting_primary, client: client.clone(), topology_view: channels.topology_view().clone(), channels: channels.clone(), cancel, rafka_time: rafka_time.clone(), member_cert, attempt: 0, inherited };
    let driver = node_hooks.driver(ctx, root.clone());
    *state.lock().unwrap() = driver.as_ref().map(|d| d.handle());
    // 4. and 5., once the hook passed.
    let rest = AfterTheHook { root, name: name.clone(), birth_path: path.path.clone(), birth_name: path.name.clone(), node_id, client, rafka_time, channels, through: through.ok(), commander, after, cycle };
    match runs {
        HookRuns::Spawned { spawn } => {
            let task = spawn(Box::pin(async move {
                let Some(driver) = driver else {
                    return rest.run_on(None).await;
                };
                match driver.run().await {
                    End::Passed => rest.run_on(Some(driver)).await,
                    End::Failed(why) => rest.ended_failed_at_the_hook(&why),
                    End::Cancelled => {}
                }
            }));
            node.hook_started(task).await
        }
        HookRuns::Inline => {
            let Some(driver) = driver else {
                return rest.run_after_the_hook(kind, None).await.map_err(Into::into);
            };
            let end = match driver.first().await {
                Ok(end) => end,
                // A first start's hook answered Pending: the start returns with the node Pending, and
                // the rest of the hook's run on its Send, then the steps after it, run in a task of
                // their own, ended by the node's cancel token. Its end lands where the probe reads it.
                Err(hook_rest) if kind == StartKind::FirstStart => {
                    tokio::spawn(async move {
                        match hook_rest.await {
                            End::Passed => rest.run_on(Some(driver)).await,
                            End::Failed(why) => rest.ended_failed_at_the_hook(&why),
                            End::Cancelled => {}
                        }
                    });
                    return Ok(());
                }
                // A start after a stop runs its hook to its end inside the start: its node.start caller
                // (or its node.mesh.join commander) is answered that end.
                Err(hook_rest) => hook_rest.await,
            };
            match end {
                End::Passed => rest.run_after_the_hook(kind, Some(driver)).await.map_err(Into::into),
                End::Failed(why) => {
                    rest.ended_failed_at_the_hook(&why);
                    match kind {
                        // A first start's failure lands where the probe reads it: the node stays
                        // Pending, its process keeps running, and its probe answers StartFailed
                        // (fabric-node-hooks.md "Where the failure lands").
                        StartKind::FirstStart => Ok(()),
                        // A start after a stop ends Failed to its node.start caller.
                        StartKind::AfterStop => {
                            let failed = StartFailed { step: PRE_NODE_READY_STEP, reason: why };
                            rest.after.start_failed(&failed, true).await;
                            Err(failed.into())
                        }
                    }
                }
                End::Cancelled => match kind {
                    // A first start cancelled while its hook ran ends without a report and without a refusal.
                    StartKind::FirstStart => Ok(()),
                    StartKind::AfterStop => Err(StartFailed { step: PRE_NODE_READY_STEP, reason: format!("{name} was cancelled while pre_node_ready ran") }.into()),
                },
            }
        }
    }
}

/// The steps after the hook passed (node-start.md steps 4 and 5), with what they run with.
struct AfterTheHook {
    root: tracing::Span,
    name: String,
    birth_path: String,
    birth_name: String,
    node_id: NodeId,
    client: Arc<NodeRpcClient>,
    rafka_time: rdm_transport::clock::RafkaTime,
    channels: Channels,
    /// The authority the join reached, and what the step spans name it; `None` with no authority.
    through: Option<(JoinThrough, String)>,
    /// After a stop, the endpoint of the call that commanded the start last: the topology read goes
    /// to that admin, as the join did (node-start.md step 2; the reads are
    /// the mesh-primary's, node-topology-get.md).
    commander: Option<tokio::sync::watch::Receiver<iroh::PublicKey>>,
    after: Box<dyn AfterHook>,
    /// This cycle's token: cancelled when a commanded node.mesh.join begins the birth's next cycle, which
    /// runs the whole sequence again (node-mesh-join.md step 10); these steps then end, unserved and unreported.
    cycle: CancelToken,
}

impl AfterTheHook {
    /// The `(path, name)` lookup index key of path and name.
    fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.birth_path.clone(), name: self.birth_name.clone() } }
}

impl AfterTheHook {
    /// The start ended Failed at the hook: named on its own span under the start's root. Nothing after
    /// the hook runs, nothing calls the hook again, and the node stays Pending.
    fn ended_failed_at_the_hook(&self, why: &str) {
        tracing::info_span!(parent: &self.root, "rdm.mesh.node.reject.via-start-step-failed", node_path = %self.birth_path, node_name = %self.name, node_id = %self.node_id, step = PRE_NODE_READY_STEP, reason = %why)
            .in_scope(|| tracing::warn!("the start ended Failed at pre_node_ready: the node stays Pending and nothing calls the hook again"));
    }

    /// The steps after a hook that passed inside the start. A start after a stop answers a failed step
    /// to its caller ([`Self::run_inline`]). A first start never exits on one: the failed step is named
    /// and the node stays Pending, its process running, as when the steps run in the start's own task
    /// ([`Self::run_on`]), for the command the create step its mesh owes sends it (mesh-recovery.md step
    /// 4: the successor "runs each step its mesh owes that no `rdm.build.step` event ended"; build.md
    /// §4.1, Q445).
    async fn run_after_the_hook(self, kind: StartKind, driver: Option<Arc<HookDriver>>) -> Result<(), StartFailed> {
        match kind {
            StartKind::FirstStart => {
                self.run_on(driver).await;
                Ok(())
            }
            StartKind::AfterStop => self.run_inline(driver).await,
        }
    }

    /// The steps inside a start after a stop: a failed step ends what the start brought up, and the
    /// start answers it.
    async fn run_inline(self, driver: Option<Arc<HookDriver>>) -> Result<(), StartFailed> {
        match self.steps(driver).await {
            Ok(()) => Ok(()),
            Err((failed, after)) => {
                after.start_failed(&failed, true).await;
                Err(failed)
            }
        }
    }

    /// The steps of a first start, in the task a spawned hook ran in or inside the start: no caller
    /// waits on them, so a failed step is named on its own span under the start's root, and the node
    /// stays Pending.
    async fn run_on(self, driver: Option<Arc<HookDriver>>) {
        let (root, name, path, node_id) = (self.root.clone(), self.name.clone(), self.location(), self.node_id.clone());
        if let Err((failed, after)) = self.steps(driver).await {
            tracing::info_span!(parent: &root, "rdm.mesh.node.reject.via-start-step-failed", node_path = %path.path, node_name = %name, node_id = %node_id, step = failed.step, reason = %failed.reason)
                .in_scope(|| tracing::warn!("a start step after pre_node_ready failed: the node is not started and stays Pending"));
            after.start_failed(&failed, false).await;
        }
    }

    async fn steps(self, driver: Option<Arc<HookDriver>>) -> Result<(), (StartFailed, Box<dyn AfterHook>)> {
        let Self { root, name, birth_path, birth_name, node_id, client, rafka_time, channels, through, mut commander, mut after, cycle } = self;
        let topology_view = channels.topology_view().clone();
        let path = rdm_core::PathName { path: birth_path, name: birth_name };
        // A commanded node.mesh.join began the birth's next cycle while these steps ran: they end where they
        // stand, and the commanded sequence reads the topology and reports (node-mesh-join.md step 10, rdm#408).
        let ended = |at: &'static str| {
            tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-after-hook-cancelled", node_path = %path.path, node_name = %name, node_id = %node_id, at)
                .in_scope(|| tracing::info!("a commanded node.mesh.join began the next cycle: this start's steps after the hook end, unserved and unreported"));
        };
        // 4. The Build facts the authority's floor requires, on every start: before any topology read.
        let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-build-floor", node_path = %path.path, node_name = %name, node_id = %node_id, owed = tracing::field::Empty, blockers = tracing::field::Empty, outcome = tracing::field::Empty);
        tokio::select! {
            biased;
            _ = cycle.cancelled() => {
                ended("build-floor");
                return Ok(());
            }
            _ = after.floor(&step).instrument(step.clone()) => {}
        }
        drop(step);
        // Then the topology, once, the final startup read, with no conditional source version, and the
        // rafka-time of the same reply adopted after its install (node-start.md step 4): from the node's
        // own current mesh-primary (node-topology-get.md, "exact member → own mesh-primary"). After a
        // stop that is the mesh-primary whose call commanded the start, reached back over its call
        // (node-start.md "Admission" and step 2); at a first start the mesh's current primary its
        // topology view names, never the admin that answered the join once another holds the role, and
        // that admin only while the view names none. With no authority, the rafka-time this start
        // adopted is kept.
        if let Some((t, label)) = &through {
            let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-topology-taken", node_path = %path.path, node_name = %name, node_id = %node_id, launcher = %label, served_by = tracing::field::Empty, rafka_time = tracing::field::Empty);
            after.before_topology_read(&t.node_id).instrument(step.clone()).await;
            let read = |target: NodeTarget| {
                let (client, channels, mesh) = (&client, &channels, topology_view.mesh_name());
                async move { crate::topology_read::get_topology(client, &target, mesh, channels, None, None).await }
            };
            let reading = async {
                match commander.as_mut() {
                    None => at_own_mesh_primary(&name, &node_id, &topology_view, t, read).instrument(step.clone()).await,
                    commander => at_commander(&name, &client, t, commander, read).instrument(step.clone()).await,
                }
            };
            let taken = tokio::select! {
                biased;
                _ = cycle.cancelled() => {
                    ended("topology-read");
                    return Ok(());
                }
                taken = reading => taken,
            };
            // A read answered after the next cycle began is that cycle's to take, never this start's.
            if cycle.is_cancelled() {
                ended("topology-read");
                return Ok(());
            }
            let (read, who) = match taken {
                Ok(read) => read,
                Err((e, who)) => return Err((StartFailed { step: "node.topology", reason: format!("{name} could not read the topology of {who}: {e}") }, after)),
            };
            step.record("served_by", tracing::field::display(&who));
            after.topology_written(&channels, &read).instrument(step.clone()).await;
            match step.in_scope(|| read.adopt_rafka_time(&rafka_time, &topology_view, node_id.as_str(), topology_view.mesh_name(), &node_id, &who)) {
                Ok(_) => {
                    step.record("rafka_time", "adopted");
                }
                // A primary takes rafka-time only from the fabric-primary (node-identity-and-state.md
                // §2.5): any other answer is refused by name and the primary keeps the rafka-time it
                // holds (`crate::rafka_time`). A re-entering fabric-primary holds the fabric's time.
                Err(crate::topology_read::TimeNotAdopted::Refused(refusal)) => {
                    step.record("rafka_time", format!("kept: {refusal}").as_str());
                }
                Err(e) => return Err((StartFailed { step: "node.rafka-time", reason: format!("{name} could not adopt rafka-time from {who}: {e}") }, after)),
            }
            drop(step);
        }
        // 5. node.started.
        if cycle.is_cancelled() {
            ended("node.started");
            return Ok(());
        }
        if let Err(failed) = after.report().await {
            return Err((failed, after));
        }
        if let Some(driver) = driver {
            driver.spawn_on_node_ready();
        }
        Ok(())
    }
}

/// The channel step of a first start's plug: given the step's span and the join answer, it enters
/// the channel and answers the topology view and the plug's node.joined step.
pub type EnterSeam<E> = Box<dyn FnOnce(tracing::Span, Option<JoinAnswer>) -> BoxFut<Result<(Channels, JoinedSeam<E>), E>> + Send>;

/// The node.joined step of a first start's plug: what the node brings up while its hook runs,
/// answering the hook's parts and, for a spawned hook, what the start does once the hook's task is
/// running.
pub type JoinedSeam<E> = Box<dyn FnOnce(Option<JoinThrough>) -> BoxFut<Result<(HookParts, Option<HookStarted<E>>), E>> + Send>;

/// What a first start's plug does once its spawned hook's task is running.
pub type HookStarted<E> = Box<dyn FnOnce(tokio::task::JoinHandle<()>) -> BoxFut<Result<(), E>> + Send>;

/// A first start's plug whose steps are continuations: each step runs in the scope the previous one
/// left, so a node type that brings its runtime up while it starts (a node-admin hydrating its stores
/// and entering the election) holds what it built from one step to the next.
pub struct FirstStart<E> {
    /// The listeners the birth bound from its launch, each with its selected port, presented by the
    /// first join (Q151); none for a kind that binds none.
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// What the start knows before it begins.
    pub starting: Option<Starting>,
    /// Whom it joins.
    pub joining: Option<Joining>,
    /// Resolves whom the join goes to.
    pub resolve: Option<Box<dyn FnOnce() -> Result<JoinTo, StartFailed> + Send>>,
    /// The digest the join presents.
    pub digest: Option<MeshDigest>,
    /// The runtime and data dir the join reports beside the digest.
    pub runtime: rdm_core::NodeRuntime,
    /// The channel step.
    pub enter: Option<EnterSeam<E>>,
    /// The node.joined step, once the channel step answered it.
    pub joined: Option<JoinedSeam<E>>,
    /// What follows a spawned hook's start, once the node.joined step answered it.
    pub hook_started: Option<HookStarted<E>>,
}

#[async_trait::async_trait]
impl<E: From<StartFailed> + Send + 'static> super::node_mesh_join::MeshJoin for FirstStart<E> {
    type Error = E;

    fn join_listeners(&self) -> Vec<(String, std::net::SocketAddr)> { self.listeners.clone() }

    fn join_digest(&self) -> MeshDigest {
        self.digest.clone().expect("a first start that joins presents a digest")
    }

    fn join_runtime(&self) -> rdm_core::NodeRuntime {
        self.runtime.clone()
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&JoinAnswer>) -> Result<Channels, E> {
        let (channels, next) = (self.enter.take().expect("the channel is entered once"))(step, joined.cloned()).await?;
        self.joined = Some(next);
        Ok(channels)
    }
}

#[async_trait::async_trait]
impl<E: From<StartFailed> + Send + 'static> StartNode for FirstStart<E> {

    fn starting(&self) -> Starting {
        let s = self.starting.as_ref().expect("a first start is begun once");
        Starting { root: s.root.clone(), name: s.name.clone(), birth_path: s.birth_path.clone(), birth_name: s.birth_name.clone(), node_id: s.node_id.clone(), mesh_id: s.mesh_id.clone(), client: s.client.clone(), rafka_time: s.rafka_time.clone() }
    }

    fn joining(&self) -> Joining {
        match self.joining.as_ref().expect("a first start is begun once") {
            Joining::AcceptingPrimary { label } => Joining::AcceptingPrimary { label: label.clone() },
            Joining::None(why) => Joining::None(*why),
        }
    }

    async fn resolve_join_to(&mut self) -> Result<JoinTo, StartFailed> {
        (self.resolve.take().expect("the authority is resolved once"))()
    }

    async fn joined(&mut self, _: &Channels, through: Option<&JoinThrough>, _: Option<&JoinAnswer>) -> Result<HookParts, E> {
        let (parts, started) = (self.joined.take().expect("node.joined is taken once"))(through.cloned()).await?;
        self.hook_started = started;
        Ok(parts)
    }

    async fn hook_started(&mut self, task: tokio::task::JoinHandle<()>) -> Result<(), E> {
        match self.hook_started.take() {
            Some(started) => started(task).await,
            None => {
                drop(task);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_hooks::{HookOutcome, OnNodeReady, PreNodeReady};
    use rdm_core::{FabricId, IncarnationId, model::NodeStatus, MeshId};

    struct RecordedAfter {
        steps: Arc<Mutex<Vec<&'static str>>>,
        /// The report fails: a step after the hook that ends the start.
        fail_report: bool,
        /// The floor step waits on this, when one is given, after it is recorded.
        floor_held: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait::async_trait]
    impl AfterHook for RecordedAfter {
        async fn topology_written(&mut self, _: &Channels, _: &TopologyRead) {
            self.steps.lock().unwrap().push("topology_written");
        }

        async fn floor(&mut self, step: &tracing::Span) {
            self.steps.lock().unwrap().push("floor");
            if let Some(held) = &self.floor_held {
                held.notified().await;
            }
            step.record("owed", "none");
            step.record("outcome", "unit-cell-floor-checked");
        }

        async fn report(&mut self) -> Result<(), StartFailed> {
            self.steps.lock().unwrap().push("report");
            match self.fail_report {
                true => Err(StartFailed { step: "node.started", reason: "the cell's report fails".into() }),
                false => Ok(()),
            }
        }

        async fn start_failed(self: Box<Self>, _: &StartFailed, inside: bool) {
            self.steps.lock().unwrap().push(if inside { "start_failed(inside)" } else { "start_failed(task)" });
        }
    }

    struct TestHook {
        steps: Arc<Mutex<Vec<&'static str>>>,
        observed_ctx: Arc<Mutex<Vec<(StartKind, bool)>>>,
        ready_notified: Arc<tokio::sync::Notify>,
        /// `Pending` until set, when one is given; `Ok` at once otherwise.
        pass: Option<Arc<std::sync::atomic::AtomicBool>>,
    }

    #[async_trait::async_trait]
    impl PreNodeReady for TestHook {
        async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
            self.steps.lock().unwrap().push("pre_node_ready");
            self.observed_ctx
                .lock()
                .unwrap()
                .push((ctx.start_kind, ctx.inherited));
            match &self.pass {
                Some(pass) if !pass.load(std::sync::atomic::Ordering::SeqCst) => HookOutcome::Pending { reason: "held by the cell".into() },
                _ => HookOutcome::Ok,
            }
        }
    }

    #[async_trait::async_trait]
    impl OnNodeReady for TestHook {
        async fn on_node_ready(&self, _ctx: &NodeReadyCtx) {
            self.steps.lock().unwrap().push("on_node_ready");
            self.ready_notified.notify_one();
        }
    }

    struct TestStartPlug {
        starting: Starting,
        no_accepting_primary: NoAcceptingPrimary,
        steps: Arc<Mutex<Vec<&'static str>>>,
        observed_ctx: Arc<Mutex<Vec<(StartKind, bool)>>>,
        ready_notified: Arc<tokio::sync::Notify>,
        /// The hook's `Pending` until set ([`TestHook::pass`]).
        pass: Option<Arc<std::sync::atomic::AtomicBool>>,
        /// The birth's cycles: the hook runs on the current cycle's token. A fresh token when `None`.
        cycles: Option<rdm_transport::BirthCancel>,
        /// Where the start hands the hook's handle.
        state: Arc<Mutex<Option<crate::node_hooks::PreNodeReadyHandle>>>,
        /// The report fails ([`RecordedAfter::fail_report`]).
        fail_report: bool,
        /// The floor step waits on this ([`RecordedAfter::floor_held`]).
        floor_held: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait::async_trait]
    impl super::super::node_mesh_join::MeshJoin for TestStartPlug {
        type Error = StartFailed;

        fn join_runtime(&self) -> rdm_core::NodeRuntime {
            rdm_core::NodeRuntime::default()
        }

        fn join_digest(&self) -> MeshDigest {
            MeshDigest {
                fabric_id: FabricId::mint(),
                node: rdm_core::MeshNode { mesh: "mesh1".into(), r#type: "node".into(),
                    node_id: self.starting.node_id.clone(),
                    path: self.starting.birth_path.clone(), name: self.starting.birth_name.clone(),
                    endpoint_id: rdm_core::EndpointId(
                        self.starting.client.endpoint().id().to_string(),
                    ),
                    transport_addr: "127.0.0.1:0".parse().unwrap(),
                    incarnation: IncarnationId::mint(),
                    supersedes: None,
                },
                status: NodeStatus::Ready,
                digest_seq: 0,
                emitted_at_rafka_ms: 0,
                primary: Vec::new(),
            }
        }

        async fn enter_channel(
            &mut self,
            _step: tracing::Span,
            _joined: Option<&JoinAnswer>,
        ) -> Result<rdm_gossip::channel::Channels, StartFailed> {
            self.steps.lock().unwrap().push("enter_channel");
            let ep = self.starting.client.endpoint().clone();
            let g = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
            let (fabric_id, mesh_id) = (FabricId::mint(), MeshId::mint());
            let topology_view = TopologyView::new(&fabric_id, "mesh1", &mesh_id, &self.starting.name, Arc::new(self.starting.rafka_time.clone()), Default::default());
            let failed = |e: anyhow::Error| StartFailed { step: "node.channel", reason: e.to_string() };
            let channels = rdm_gossip::channel::Channels::new(&g, &ep, &topology_view).map_err(failed)?;
            channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), Vec::new()).await.map_err(failed)?;
            Ok(channels)
        }
    }

    #[async_trait::async_trait]
    impl StartNode for TestStartPlug {
        fn starting(&self) -> Starting {
            Starting {
                root: self.starting.root.clone(),
                name: self.starting.name.clone(),
                birth_path: self.starting.birth_path.clone(), birth_name: self.starting.birth_name.clone(),
                node_id: self.starting.node_id.clone(),
                mesh_id: self.starting.mesh_id.clone(),
                client: self.starting.client.clone(),
                rafka_time: self.starting.rafka_time.clone(),
            }
        }

        fn joining(&self) -> Joining {
            Joining::None(self.no_accepting_primary)
        }

        async fn resolve_join_to(&mut self) -> Result<JoinTo, StartFailed> {
            unreachable!("Joining::None does not resolve join target")
        }

        async fn joined(
            &mut self,
            _: &rdm_gossip::channel::Channels,
            _: Option<&JoinThrough>,
            _: Option<&JoinAnswer>,
        ) -> Result<HookParts, StartFailed> {
            self.steps.lock().unwrap().push("joined");
            let hook = Arc::new(TestHook {
                steps: self.steps.clone(),
                observed_ctx: self.observed_ctx.clone(),
                ready_notified: self.ready_notified.clone(),
                pass: self.pass.clone(),
            });
            Ok(HookParts {
                birth: Birth {
                    path: self.starting.birth_path.clone(), name: self.starting.birth_name.clone(),
                    node_id: self.starting.node_id.clone(),
                    incarnation: IncarnationId::mint(),
                    fabric_id: FabricId::mint(),
                    mesh_id: MeshId::mint(),
                    r#type: "node".into(),
                },
                node_hooks: NodeHooks::new(hook.clone()).with_on_node_ready(hook),
                cancel: self.cycles.as_ref().map(|c| c.token()).unwrap_or_else(CancelToken::new),
                state: self.state.clone(),
                runs: HookRuns::Inline,
                after: Box::new(RecordedAfter {
                    steps: self.steps.clone(),
                    fail_report: self.fail_report,
                    floor_held: self.floor_held.clone(),
                }),
            })
        }
    }

    /// CONTRACT (docs/architecture/lifecycles/node-start.md, "End-to-end sequence"):
    /// "1. A stopped process remains the same exact runtime and keeps its endpoint/port; it does not start
    /// a new process or load a new executable.
    /// 2. Run node.mesh.join... 3. Run pre_node_ready with the start kind and inherited-data-dir
    /// indication...
    /// 4. Satisfy the required Build floor after the hook. Read committed facts from Build storage
    /// only when the node is below the primary's floor: the current Build when no pointer is known,
    /// otherwise the named Build. Absorb the facts through the existing Build log and Ready check;
    /// a successful storage read alone does not satisfy the floor. This check must not depend on
    /// fetching topology early.
    /// Then obtain topology once through node.topology.get, as the final
    /// startup read. Request it without a conditional source version and write only complete
    /// aggregates to the topology view. The same reply supplies the recorded primaries, startup
    /// context and current Rafka-time; adopt the time after writing topology to the topology view and
    /// before reporting started.
    /// Preserve retained own properties and application recognition. Do not fetch topology before
    /// the Build-floor check and do not issue a second topology request just to refresh time.
    /// 5. Emit `NodeStarted` (Terminal): its hook passed, with nothing pending. Neither a partial aggregate
    /// nor a `Pending` hook permits it. That frame is the node's report: the mesh-primary sets Ready on that
    /// exact birth through the node status apply, not a caller op. The node also declares Ready to its
    /// mesh-primary as an owed message, as it declares Stopped (Q36, Q87); frame and declaration apply once
    /// (Q49). A typed refusal of that apply completes no readiness.
    /// 6. Start ends with node.started. The node then calls on_node_ready once"
    /// "Failures and stream certainty follow the caller execution contract."
    /// "Trace propagation, step spans and correlation follow the runtime spans."
    #[test]
    fn node_start_executes_join_hook_floor_report_and_on_node_ready_in_order() {
        let dir = std::env::temp_dir().join(format!("rdm-node-start-unit-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-node-start-unit")
            .expect("evidence capture starts");

        capture.run(2, async {
            let ep = rdm_transport::endpoint::bind(
                iroh::SecretKey::generate(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .unwrap();
            let resolver = Arc::new(rdm_transport::StaticResolver::new());
            let client = Arc::new(NodeRpcClient::new(ep, resolver));
            let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
            rafka_time.adopt(1_000_000);

            // 1. reached_back refuses with commander-unreachable when no active path is held.
            let missing_key = iroh::SecretKey::generate().public();
            let err = reached_back("mesh1.node.1", &client, &missing_key)
                .await
                .expect_err("unconnected endpoint has no active path");
            assert!(
                err.contains("commander-unreachable"),
                "expected commander-unreachable, got {err}"
            );

            // 2. FirstStart on a fresh Day0Root runs enter_channel -> joined -> pre_node_ready
            //    (inherited=false) -> floor -> report -> on_node_ready.
            let steps = Arc::new(Mutex::new(Vec::new()));
            let observed = Arc::new(Mutex::new(Vec::new()));
            let notify1 = Arc::new(tokio::sync::Notify::new());
            let root1 = tracing::info_span!("rdm.mesh.node.create.via-boot", node_path = "mesh1.node", node_name = "1");
            let mut plug1 = TestStartPlug {
                starting: Starting {
                    root: root1,
                    name: "1".into(),
                    birth_path: "mesh1.node".into(), birth_name: "1".into(),
                    node_id: NodeId::mint(),
                    mesh_id: None,
                    client: client.clone(),
                    rafka_time: rafka_time.clone(),
                },
                no_accepting_primary: NoAcceptingPrimary::Day0Root,
                steps: steps.clone(),
                observed_ctx: observed.clone(),
                ready_notified: notify1.clone(),
                pass: None,
                cycles: None,
                state: Arc::default(),
                fail_report: false,
                floor_held: None,
            };
            node_start(&mut plug1, StartKind::FirstStart)
                .await
                .expect("FirstStart succeeds");
            notify1.notified().await;

            // 3. AfterStop runs with inherited=true.
            let notify2 = Arc::new(tokio::sync::Notify::new());
            let root2 = tracing::info_span!("rdm.mesh.node.update.via-start", node_path = "mesh1.node", node_name = "1");
            let mut plug2 = TestStartPlug {
                starting: Starting {
                    root: root2,
                    name: "1".into(),
                    birth_path: "mesh1.node".into(), birth_name: "1".into(),
                    node_id: NodeId::mint(),
                    mesh_id: None,
                    client,
                    rafka_time,
                },
                no_accepting_primary: NoAcceptingPrimary::Day0Root,
                steps: steps.clone(),
                observed_ctx: observed.clone(),
                ready_notified: notify2.clone(),
                pass: None,
                cycles: None,
                state: Arc::default(),
                fail_report: false,
                floor_held: None,
            };
            node_start(&mut plug2, StartKind::AfterStop)
                .await
                .expect("AfterStop succeeds");
            notify2.notified().await;

            assert_eq!(
                *steps.lock().unwrap(),
                vec![
                    "enter_channel",
                    "joined",
                    "pre_node_ready",
                    "floor",
                    "report",
                    "on_node_ready",
                    "enter_channel",
                    "joined",
                    "pre_node_ready",
                    "floor",
                    "report",
                    "on_node_ready",
                ]
            );
            assert_eq!(
                *observed.lock().unwrap(),
                vec![
                    (StartKind::FirstStart, false),
                    (StartKind::AfterStop, true),
                ]
            );
        });

        let spans = capture.spans().expect("captured spans");
        let _ = std::fs::remove_dir_all(&dir);
        for required in [
            "rdm.mesh.node.update.via-topology-joined",
            "rdm.mesh.node.update.via-pre-node-ready-attempt",
            "rdm.mesh.node.update.via-build-floor",
            "rdm.mesh.node.update.via-on-node-ready",
        ] {
            assert!(
                spans.iter().any(|s| s["name"] == required),
                "expected span {required} in node_start execution"
            );
        }
    }

    /// CONTRACT (node-mesh-join.md step 10: a commanded node.mesh.join "runs the whole sequence again" on the
    /// birth, its next cycle): a first start whose hook passed, still in its steps after the hook, ends where
    /// it stands when a commanded node.mesh.join begins the birth's next cycle (its token renewed): the
    /// earlier start reports nothing and serves no read, and `rdm.mesh.node.update.via-after-hook-cancelled`
    /// names where it ended. Must NOT happen (rdm#408): the earlier start's read and report running beside
    /// the commanded sequence, two topology reads served one birth.
    #[test]
    fn a_first_starts_steps_after_its_hook_end_when_a_commanded_join_begins_the_next_cycle() {
        let dir = std::env::temp_dir().join(format!("rdm-node-start-unit-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-node-start-unit").expect("evidence capture starts");
        capture.run(2, async {
            let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
            let client = Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
            let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
            rafka_time.adopt(1_000_000);
            let steps = Arc::new(Mutex::new(Vec::new()));
            let cycles = rdm_transport::BirthCancel::new();
            let held = Arc::new(tokio::sync::Notify::new());
            let mut plug = TestStartPlug {
                starting: Starting {
                    root: tracing::info_span!("rdm.mesh.node.create.via-boot", node_path = "mesh1.node", node_name = "1"),
                    name: "1".into(),
                    birth_path: "mesh1.node".into(), birth_name: "1".into(),
                    node_id: NodeId::mint(),
                    mesh_id: None,
                    client: client.clone(),
                    rafka_time: rafka_time.clone(),
                },
                no_accepting_primary: NoAcceptingPrimary::Day0Root,
                steps: steps.clone(),
                observed_ctx: Arc::default(),
                ready_notified: Arc::new(tokio::sync::Notify::new()),
                pass: None,
                cycles: Some(cycles.clone()),
                state: Arc::default(),
                fail_report: false,
                floor_held: Some(held.clone()),
            };
            let start = tokio::spawn(async move { node_start(&mut plug, StartKind::FirstStart).await });
            // The hook passed and the steps after it are under way (held in the floor step).
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !steps.lock().unwrap().contains(&"floor") {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the first start reaches its steps after the hook");
            // A commanded node.mesh.join begins the birth's next cycle.
            drop(cycles.renew());
            let ended = tokio::time::timeout(std::time::Duration::from_secs(10), start).await.expect("the earlier start ends once its cycle is renewed").expect("the start's task");
            assert!(ended.is_ok(), "the earlier start ends, not failed: {ended:?}");
            assert!(!steps.lock().unwrap().contains(&"report"), "the earlier start reports nothing: {:?}", steps.lock().unwrap());
        });
        let spans = capture.spans().expect("captured spans");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            spans.iter().any(|s| s["name"] == "rdm.mesh.node.update.via-after-hook-cancelled" && s["attributes"]["at"] == "build-floor"),
            "the earlier start's end is named where it stood"
        );
    }

    /// A node-admin digest of mesh1, Ready.
    fn admin(fabric_id: &FabricId, name: &str, node_id: &NodeId) -> MeshDigest {
        MeshDigest {
            fabric_id: fabric_id.clone(),
            node: rdm_core::MeshNode {
                mesh: "mesh1".into(),
                r#type: "node-admin".into(),
                node_id: node_id.clone(),
                path: "mesh1.node-admin".into(),
                name: name.into(),
                endpoint_id: rdm_core::EndpointId(format!("key-{node_id}")),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation: IncarnationId::mint(),
                supersedes: None,
            },
            status: NodeStatus::Ready,
            digest_seq: 1,
            emitted_at_rafka_ms: 1,
            primary: Vec::new(),
        }
    }

    fn record(d: &MeshDigest, revision: u64) -> rdm_core::Primary {
        rdm_core::Primary { mesh: "mesh1".into(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision }
    }

    /// CONTRACT (node-topology-get.md, "exact member → own mesh-primary"; node-start.md step 4, "obtain
    /// topology once through node.topology.get"; the steps' own words: "at a first start the mesh's
    /// current primary its topology view names, never the admin that answered the join once another
    /// holds the role, and that admin only while the view names none"): a first start's one topology
    /// read goes to its mesh's current primary as its view names it. A read under way to that primary
    /// when the view comes to name another (a later record, its birth held) is dropped and made again to
    /// the one named now, which answers it, on the view's own change: no timer, no retry of a failed
    /// read. A view that names the node itself sends the read to the admin that answered its join (a
    /// mesh-primary reads at the primary above it, node-start.md "Admission"). Must NOT happen: the read
    /// held at a primary the view no longer names (rdm#400: a launcher lost with the read under way), or
    /// a read aimed at the node itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_first_starts_read_under_way_follows_the_primary_its_view_names_now() {
        let fabric_id = FabricId::mint();
        let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
        rafka_time.adopt(1_000_000);
        let view = TopologyView::new(&fabric_id, "mesh1", &MeshId::mint(), "mesh1.node.3", Arc::new(rafka_time), Default::default());
        let (old_id, new_id, me) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        let (old, new) = (admin(&fabric_id, "1", &old_id), admin(&fabric_id, "2", &new_id));
        for d in [&old, &new] {
            assert!(view.digest_view.record_at(d.clone(), std::time::Instant::now()), "the view holds {}", d.node.node_id);
        }
        view.primaries().take(rdm_core::PrimaryRole::MeshPrimary, &record(&old, 1));
        let through = JoinThrough { target: NodeTarget::ExactNode(old_id.clone()), endpoint: iroh::SecretKey::generate().public(), node_id: old_id.clone(), anchor: "launcher".into() };
        // The launcher answers nothing (it is lost with the read under way); any other admin answers.
        let (reads, under_way) = (Arc::new(Mutex::new(Vec::<String>::new())), Arc::new(tokio::sync::Notify::new()));
        let read = |target: NodeTarget| {
            let (reads, under_way, old_id) = (reads.clone(), under_way.clone(), old_id.clone());
            async move {
                let NodeTarget::ExactNode(id) = target else { panic!("a first start reads at an exact birth: {target:?}") };
                reads.lock().unwrap().push(id.to_string());
                under_way.notify_one();
                if id == old_id {
                    std::future::pending::<()>().await;
                }
                Ok::<_, String>(format!("answered by {id}"))
            }
        };
        let elected = async {
            under_way.notified().await;
            view.primaries().take(rdm_core::PrimaryRole::MeshPrimary, &record(&new, 2));
        };
        let (taken, ()) = tokio::time::timeout(std::time::Duration::from_secs(10), async { tokio::join!(at_own_mesh_primary("mesh1.node.3", &me, &view, &through, read), elected) })
            .await
            .expect("the read under way at the launcher ends once the view names another primary");
        assert_eq!(taken, Ok((format!("answered by {new_id}"), new_id.to_string())), "the read is answered by the primary the view names now");
        assert_eq!(*reads.lock().unwrap(), vec![old_id.to_string(), new_id.to_string()], "one read to the primary named first, made again once to the one named now");

        // A view that names the node itself: the read goes to the admin that answered the join.
        let mine = admin(&fabric_id, "3", &me);
        assert!(view.digest_view.record_at(mine.clone(), std::time::Instant::now()));
        view.primaries().take(rdm_core::PrimaryRole::MeshPrimary, &record(&mine, 3));
        let through = JoinThrough { target: NodeTarget::ExactNode(new_id.clone()), endpoint: iroh::SecretKey::generate().public(), node_id: new_id.clone(), anchor: "above".into() };
        let at = |target: NodeTarget| async move { Ok::<_, String>(target) };
        let taken = tokio::time::timeout(std::time::Duration::from_secs(10), at_own_mesh_primary("mesh1.node-admin.3", &me, &view, &through, at)).await.expect("answered");
        assert_eq!(taken, Ok((NodeTarget::ExactNode(new_id.clone()), new_id.to_string())), "a node its view names primary reads at the admin above it, never at itself");
    }

    /// CONTRACT (node-mesh-join.md step 10: "a birth commanded back leaves its channels, runs the whole
    /// sequence again on the commanding call and publishes ready only once its hook has passed (Q87)";
    /// fabric-node-hooks.md "Cancellation and lifecycle commands": "Ending the in-flight start cancels the
    /// token ... The hook and any pull it has in flight end", and "Observability": "A cancelled hook
    /// names the cancellation"): a first start whose hook answered `Pending` returns with the node
    /// Pending and runs the rest of its hook in a task. When the birth begins its next cycle (a
    /// commanded node.mesh.join renews its `BirthCancel`, node_self `serve_join_mesh`), that cycle
    /// replaces this one: the first start's hook ends `Cancelled`, named on its own span, and nothing
    /// after it runs. Must NOT happen (rdm#400): the first start's hook passing at its next attempt and
    /// its Build floor, topology read or report running beside the commanded sequence.
    #[test]
    fn a_first_start_held_at_its_hook_ends_cancelled_when_the_birth_begins_its_next_cycle() {
        let dir = std::env::temp_dir().join(format!("rdm-node-start-cycle-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-node-start-cycle").expect("evidence capture starts");
        let steps = Arc::new(Mutex::new(Vec::new()));
        capture.run(2, async {
            let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
            let client = Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
            let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
            rafka_time.adopt(1_000_000);
            let (cycles, pass, state) = (rdm_transport::BirthCancel::new(), Arc::new(std::sync::atomic::AtomicBool::new(false)), Arc::new(Mutex::new(None)));
            let mut plug = TestStartPlug {
                starting: Starting {
                    root: tracing::info_span!("rdm.mesh.node.create.via-boot", node_path = "mesh1.node", node_name = "4"),
                    name: "4".into(),
                    birth_path: "mesh1.node".into(),
                    birth_name: "4".into(),
                    node_id: NodeId::mint(),
                    mesh_id: None,
                    client,
                    rafka_time,
                },
                no_accepting_primary: NoAcceptingPrimary::Day0Root,
                steps: steps.clone(),
                observed_ctx: Arc::default(),
                ready_notified: Arc::new(tokio::sync::Notify::new()),
                pass: Some(pass.clone()),
                cycles: Some(cycles.clone()),
                state: state.clone(),
                fail_report: false,
                floor_held: None,
            };
            node_start(&mut plug, StartKind::FirstStart).await.expect("a first start whose hook answered Pending returns with the node Pending");
            let handle = state.lock().unwrap().clone().expect("the start handed the hook's handle");
            assert!(matches!(handle.state(), crate::node_hooks::PreNodeReadyState::Pending { .. }), "the hook answered Pending: {:?}", handle.state());
            // The birth begins its next cycle, and the hook would pass at its next attempt.
            let _next = cycles.renew();
            pass.store(true, std::sync::atomic::Ordering::SeqCst);
            let end = tokio::time::timeout(std::time::Duration::from_secs(10), handle.settled()).await.expect("the first start's hook settles");
            assert_eq!(end, crate::node_hooks::PreNodeReadyState::Cancelled, "the next cycle ends the first start's hook: steps {:?}", steps.lock().unwrap());
        });
        let spans = capture.spans().expect("captured spans");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(*steps.lock().unwrap(), vec!["enter_channel", "joined", "pre_node_ready"], "nothing after the hook runs: no Build floor, no topology read, no report");
        assert!(
            spans.iter().any(|s| s["name"] == "rdm.mesh.node.update.via-pre-node-ready-cancelled" && s["attributes"]["node_name"] == "4"),
            "the cancelled hook names the cancellation on its span"
        );
    }

    /// CONTRACT (mesh-recovery.md step 4: "The successor reads the current Build's events and runs each
    /// step its mesh owes that no `rdm.build.step` event ended"; build.md §4.1: an incomplete Build's
    /// resubmission "dispatches the section to the mesh's primary of the moment, which reads the Build's
    /// events and runs the steps its mesh owes"; Q445): a first start never exits on a failed step after
    /// its hook. Whether its hook passed at its first attempt (the steps after it run inside the start) or
    /// at a later one (they run in the start's own task), the failed step is named on its span, the kind
    /// is told inside its own task (the node stays Pending, its process running), and the start returns,
    /// so the create step its mesh owes can command it again. Must NOT happen (rdm#400): a first start
    /// whose hook passed at once answering the failure, so that its node's process exits.
    #[test]
    fn a_first_start_whose_step_after_its_hook_fails_returns_with_the_node_pending() {
        let dir = std::env::temp_dir().join(format!("rdm-node-start-failed-step-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-node-start-failed-step").expect("evidence capture starts");
        let steps = Arc::new(Mutex::new(Vec::new()));
        let started = capture.run(2, async {
            let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
            let client = Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
            let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
            rafka_time.adopt(1_000_000);
            let mut plug = TestStartPlug {
                starting: Starting {
                    root: tracing::info_span!("rdm.mesh.node.create.via-boot", node_path = "mesh1.node", node_name = "3"),
                    name: "3".into(),
                    birth_path: "mesh1.node".into(),
                    birth_name: "3".into(),
                    node_id: NodeId::mint(),
                    mesh_id: None,
                    client,
                    rafka_time,
                },
                no_accepting_primary: NoAcceptingPrimary::Day0Root,
                steps: steps.clone(),
                observed_ctx: Arc::default(),
                ready_notified: Arc::new(tokio::sync::Notify::new()),
                pass: None,
                cycles: None,
                state: Arc::default(),
                fail_report: true,
                floor_held: None,
            };
            node_start(&mut plug, StartKind::FirstStart).await.map_err(|e| e.to_string())
        });
        let spans = capture.spans().expect("captured spans");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(started, Ok(()), "the first start returns with the node Pending, never answering the failed step: steps {:?}", steps.lock().unwrap());
        assert_eq!(*steps.lock().unwrap(), vec!["enter_channel", "joined", "pre_node_ready", "floor", "report", "start_failed(task)"], "the hook passed at its first attempt; the failed step is told as in the start's own task");
        assert!(
            spans.iter().any(|s| s["name"] == "rdm.mesh.node.reject.via-start-step-failed" && s["attributes"]["step"] == "node.started" && s["attributes"]["node_name"] == "3"),
            "the failed step is named on its span"
        );
    }
}
