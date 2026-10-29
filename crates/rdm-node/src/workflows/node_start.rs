//! `node.start` (lifecycles/node-start.md, "End-to-end sequence"): the one start workflow. node.create
//! composes it with [`StartKind::FirstStart`] (lifecycles/node-create.md, step 3); a start of a stopped
//! node runs it with [`StartKind::AfterStop`] (node_self.rs, `StartNode`). Every node kind runs this
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
//!    and rafka-time. No topology has been read: the hook reads none as a prerequisite.
//! 4. Once the hook passed (step 4), in the start itself, or in the task a blocked or spawned hook runs
//!    on in:
//!    - `rdm.mesh.node.update.via-topology-taken`: `GetTopology` from the authority that answered the
//!      join, installed per mesh only from a complete snapshot;
//!    - `rdm.mesh.node.update.via-build-floor`: the kind's Build floor, on every start (a node-admin
//!      reads the Build facts it is missing with `FetchBuildFacts`; an rpc node owes none);
//!    - `rdm.mesh.node.update.via-rafka-time-refreshed`: rafka-time read again from the authority, last,
//!      so it is current when the node re-enters.
//! 5. `rdm.mesh.node.update.via-ready` (step 5): the kind's node.started report.
//!
//! The rafka-time refresh reads the authority's `GetTopology` naming a mesh the topology step installed
//! at the version it installed (an `Unchanged` answer carrying `RafkaTime`; an authority that answered
//! no snapshot is read whole again), and installs nothing: op `0x1E` is the read that carries
//! rafka-time to a started node (`rafka_time.rs`, the pull when the fabric-primary moved reads it the same way).

use crate::node_hooks::{AcceptingAuthority, Authority, Birth, End, Events, HookDriver, NodeReadyCtx, NodeHooks, PreNodeReadyHandle, NoAuthority, StartKind, Step};
use crate::model::{NodeId, PathName};
use crate::node_self::StartFailed;
use crate::topology_read::TopologyRead;
use crate::wire::JoinAnswer;
use rdm_core::MeshDigest;
use rdm_transport::topology::TopologyView;
use rdm_transport::{CancelToken, NodeRpcClient, NodeTarget};
use rdm_core::topology::SourceVersion;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tracing::Instrument as _;

/// The attempts a start's `JoinNode` makes before it ends unreached.
pub const JOIN_ATTEMPTS: u32 = 5;

/// A boxed future a step's plug returns.
pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// What a start knows before it begins.
pub struct Starting {
    /// The span every step of this start is a child of.
    pub root: tracing::Span,
    /// The name the steps and their refusals carry.
    pub name: String,
    /// The birth's `path.name`: rafka-time is adopted for its mesh.
    pub path: PathName,
    /// The birth's node id.
    pub node_id: NodeId,
    /// The process's one Node RPC client: the join, the topology read and the rafka-time refresh go
    /// through it.
    pub client: Arc<NodeRpcClient>,
    /// The process's rafka-time, adopted from the join answer and refreshed last.
    pub rafka_time: rdm_transport::clock::RafkaTime,
}

/// Whom a start joins.
pub enum Joining {
    /// An authority, named in the join and topology steps' `launcher` as `label`.
    Authority {
        /// What the step spans name the authority.
        label: String,
    },
    /// No authority, and why: the start makes no join, reads no topology and refreshes no rafka-time.
    None(NoAuthority),
}

/// The authority a start joined: the admin that answered its join, and where every later call of
/// the start goes.
#[derive(Clone, Debug)]
pub struct JoinThrough {
    /// Where the join went and the topology read and the rafka-time refresh go: the launcher's birth
    /// at a first start; after a stop, the endpoint and path of the call whose join was answered.
    pub target: NodeTarget,
    /// The admin's endpoint key.
    pub endpoint: iroh::PublicKey,
    /// Who answered: the launcher's `path.name` at a first start; after a stop, the name the join
    /// answer's `served_by` hands (node-start.md step 2: the node "learns who answered from the join
    /// answer").
    pub name: PathName,
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
        /// Its `path.name`.
        name: PathName,
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

/// `read` made to the admin a start's reads go to (node-start.md steps 2 and 4): the authority the
/// join reached (`through`), or, after a stop, the admin whose call commanded the start last
/// (`commander`), reached back over that call. A later call's commander replaces the one a read is
/// under way to: that read is dropped and made again to the later one, so a commander lost after
/// the join holds no read past the call that replaced it. The answer and who gave it; a failure
/// names whom the read went to.
async fn at_commander<T, E: std::fmt::Display, F, Fut>(node: &str, client: &NodeRpcClient, through: &JoinThrough, mut commander: Option<&mut tokio::sync::watch::Receiver<iroh::PublicKey>>, read: F) -> Result<(T, String), (String, String)>
where
    F: Fn(NodeTarget) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let (mut target, mut key, mut who) = (through.target.clone(), through.endpoint, through.name.to_string());
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

/// Starts the task a node-admin's first-start hook runs in, behind the admin's own task gate.
pub type Spawn = Box<dyn FnOnce(BoxFut<()>) -> tokio::task::JoinHandle<()> + Send>;

/// How the hook runs.
pub enum HookRuns {
    /// The first attempt runs inside the start, which answers its outcome and, when it passed, runs
    /// the steps after the hook too; a blocked hook runs on, then those steps, in a task held in
    /// `blocked`.
    Inline {
        /// Where the task of a blocked hook is held, so a stop ends it.
        blocked: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    },
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
    /// The retry events of this birth.
    pub events: Events,
    /// Cancelled when the birth is deleted: the hook and any pull it has in flight end.
    pub cancel: CancelToken,
    /// Where the node's Ready check and its probe read the hook's state.
    pub state: Arc<Mutex<Option<PreNodeReadyHandle>>>,
    /// How the hook runs.
    pub runs: HookRuns,
    /// The kind's part of the steps after the hook.
    pub after: Box<dyn AfterHook>,
}

/// What a node kind plugs into the steps after the hook. It is owned: the steps run inside the start
/// when the hook passes at its first attempt, and otherwise in the task the hook runs on in.
#[async_trait::async_trait]
pub trait AfterHook: Send + 'static {
    /// The topology step's read is installed: the kind takes what it reaches its mesh through (its
    /// gossip peers). Runs inside the topology step's span.
    async fn topology_installed(&mut self, topology_view: &TopologyView, read: &TopologyRead);

    /// The topology step is about to make its read of `authority`: the kind may wait here before the read is
    /// made. Runs inside the topology step's span. A kind with nothing to wait on makes the read at once.
    async fn before_topology_read(&mut self, authority: &PathName) {
        let _ = authority;
    }

    /// The Build floor: resolves once the node holds the Build facts the authority's floor requires,
    /// recording on `step` (the floor step's span, which this runs inside) `owed` and `outcome`. A kind
    /// with no Build log owes none.
    async fn floor(&mut self, step: &tracing::Span) {
        step.record("owed", "none");
        step.record("outcome", "this node holds no Build log: it owes no Build floor");
    }

    /// node.started: the kind's report once every step before it ended.
    async fn report(self: Box<Self>);

    /// A step failed: `inside` the start (which answers the failure; the kind ends what the start
    /// brought up), or in the task a blocked or spawned hook ran in, after the start returned (the
    /// kind names the failure where its readiness evidence is read; the node stays Pending).
    async fn start_failed(self: Box<Self>, failed: &StartFailed, inside: bool) {
        let _ = (failed, inside);
    }
}

/// What a node kind plugs into `node.start`.
#[async_trait::async_trait]
pub trait StartNode: super::node_mesh_join::MeshJoin {
    /// What this start knows before it begins.
    fn starting(&self) -> Starting;

    /// Whom this start joins.
    fn joining(&self) -> Joining;

    /// Whom the join goes to; called inside the join step. A start after a stop answers
    /// [`JoinTo::Commander`], whatever the node kind.
    async fn resolve_join_to(&mut self) -> Result<JoinTo, StartFailed>;
    /// The operation the start's join runs for, when the start was commanded with one (a start after
    /// a stop: its `node.start`; a commanded `node.mesh.join`: that command's); `None` names the kind's.
    fn operation(&self) -> Option<String> {
        None
    }

    /// node.joined: the node is on its channel. The kind takes what the join answered of its control
    /// state (a node-admin records the Build floor here, at every start), brings up what it serves
    /// while its hook runs (it is Pending) and hands the hook's parts. `through` is the authority the
    /// join reached and `joined` its answer (both `None` with no authority).
    async fn joined(&mut self, topology_view: &TopologyView, through: Option<&JoinThrough>, joined: Option<&JoinAnswer>) -> Result<HookParts, Self::Error>;

    /// The hook of a [`HookRuns::Spawned`] start, and the steps after it, run in `task`.
    async fn hook_started(&mut self, task: tokio::task::JoinHandle<()>) -> Result<(), Self::Error> {
        drop(task);
        Ok(())
    }
}

/// `node.start`: the join, the mesh channel, the hook, the topology, the Build floor, the rafka-time
/// refresh and the report, in that order, for `node` as it plugs into each step.
pub async fn node_start<N: StartNode>(node: &mut N, kind: StartKind) -> Result<(), N::Error> {
    let Starting { root, mut name, path, node_id, client, rafka_time } = node.starting();
    // 1. and 2. node.mesh.join (node-mesh-join.md), the one primitive: the report, the answer's
    //    rafka-time, cert and name taken before the node says anything, then the mesh channel holding
    //    that name and the status frames the join answered: node.joined. With no authority, the
    //    channel alone.
    // After a stop, the start's reads go to the admin that commanded it last (node-start.md step 2).
    let mut commander: Option<tokio::sync::watch::Receiver<iroh::PublicKey>> = None;
    let (through, joined, topology_view) = match node.joining() {
        Joining::Authority { label } => {
            let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-join-admitted", node = %name, launcher = %label, served_by = tracing::field::Empty, endpoint = tracing::field::Empty, installed = tracing::field::Empty);
            let to = node.resolve_join_to().await?;
            let mesh_id = node.join_digest().mesh_id.as_ref().map(ToString::to_string).unwrap_or_default();
            let operation = node.operation().unwrap_or_else(|| match kind {
                StartKind::FirstStart => format!("node.create:{name}"),
                StartKind::AfterStop => format!("node.start:{name}"),
            });
            let report_parent = matches!(kind, StartKind::FirstStart).then(|| step.clone());
            let call = super::node_mesh_join::JoinCall { name: name.clone(), node_id: &node_id, mesh: &path.mesh, mesh_id: &mesh_id, client: &client, rafka_time: &rafka_time, to: &to, authority: &label, operation: &operation, report_parent, reported: step, channel_parent: Some(root.clone()) };
            let joined = super::node_mesh_join::node_mesh_join(node, call).await?;
            name = joined.name;
            commander = match &to {
                JoinTo::Commander(c) => Some(c.clone()),
                JoinTo::Launcher { .. } => None,
            };
            (Ok((joined.through, label)), Some(joined.answer), joined.topology_view)
        }
        Joining::None(why) => {
            let channel = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-topology-joined", node = %name, mesh_id = tracing::field::Empty, seeds = tracing::field::Empty);
            (Err(why), None, node.enter_channel(channel, None).await?)
        }
    };
    let member_cert = joined.as_ref().map(|a| a.control.member_cert.clone());
    let parts = node.joined(&topology_view, through.as_ref().ok().map(|(t, _)| t), joined.as_ref()).await?;
    // 3. The hook, with no topology read.
    let authority = match &through {
        Ok((t, _)) => Authority::Accepted(AcceptingAuthority { name: t.name.clone(), endpoint: t.endpoint, target: t.target.clone() }),
        Err(why) => Authority::None(*why),
    };
    let HookParts { birth, node_hooks, events, cancel, state, runs, after } = parts;
    let inherited = matches!(kind, StartKind::AfterStop)
        || matches!(authority, Authority::None(NoAuthority::InheritedRoot | NoAuthority::Recovery))
        || std::env::var("RDM_INHERITED_DATA_DIR").ok().as_deref() == Some("1");
    let ctx = NodeReadyCtx { start_kind: kind, birth, authority, client: client.clone(), topology_view: topology_view.clone(), cancel, rafka_time: rafka_time.clone(), member_cert, attempt: 0, inherited };
    let driver = node_hooks.driver(ctx, events, root.clone());
    *state.lock().unwrap() = driver.as_ref().map(|d| d.handle());
    // 4. and 5., once the hook passed.
    let rest = AfterTheHook { root, name: name.clone(), path, node_id, client, rafka_time, topology_view, through: through.ok(), commander, after };
    match runs {
        HookRuns::Spawned { spawn } => {
            let task = spawn(Box::pin(async move {
                let passed = match &driver {
                    None => true,
                    Some(driver) => {
                        let first = driver.attempt().await;
                        driver.run(first).await == End::Passed
                    }
                };
                if passed {
                    rest.run_on(driver).await;
                }
            }));
            node.hook_started(task).await
        }
        HookRuns::Inline { blocked } => {
            let Some(driver) = driver else {
                return rest.run_inline(None).await.map_err(Into::into);
            };
            match driver.attempt().await {
                Step::Passed => rest.run_inline(Some(driver)).await.map_err(Into::into),
                Step::Failed(why) => {
                    let failed = StartFailed { step: "node.pre-node-ready", reason: format!("{name} failed pre_node_ready: {why}") };
                    rest.after.start_failed(&failed, true).await;
                    Err(failed.into())
                }
                Step::Cancelled => match kind {
                    // A first start cancelled while its hook ran ends without a report and without a refusal.
                    StartKind::FirstStart => Ok(()),
                    StartKind::AfterStop => Err(StartFailed { step: "node.pre-node-ready", reason: format!("{name} was cancelled while pre_node_ready ran") }.into()),
                },
                blocked_step @ Step::Blocked(_) => {
                    *blocked.lock().unwrap() = Some(tokio::spawn(async move {
                        if driver.run(blocked_step).await == End::Passed {
                            rest.run_on(Some(driver)).await;
                        }
                    }));
                    Ok(())
                }
            }
        }
    }
}

/// The steps after the hook passed (node-start.md steps 4 and 5), with what they run with.
struct AfterTheHook {
    root: tracing::Span,
    name: String,
    path: PathName,
    node_id: NodeId,
    client: Arc<NodeRpcClient>,
    rafka_time: rdm_transport::clock::RafkaTime,
    topology_view: TopologyView,
    /// The authority the join reached, and what the step spans name it; `None` with no authority.
    through: Option<(JoinThrough, String)>,
    /// After a stop, the endpoint of the call that commanded the start last: the topology read and
    /// the rafka-time refresh go to that admin, as the join did (node-start.md step 2; the reads are
    /// the mesh-primary's, node-topology-get.md).
    commander: Option<tokio::sync::watch::Receiver<iroh::PublicKey>>,
    after: Box<dyn AfterHook>,
}

impl AfterTheHook {
    /// The steps inside the start: a failed step ends what the start brought up, and the start
    /// answers it.
    async fn run_inline(self, driver: Option<Arc<HookDriver>>) -> Result<(), StartFailed> {
        match self.steps(driver).await {
            Ok(()) => Ok(()),
            Err((failed, after)) => {
                after.start_failed(&failed, true).await;
                Err(failed)
            }
        }
    }

    /// The steps in the task a blocked or spawned hook ran in: no caller waits on them, so a failed
    /// step is named on its own span under the start's root, and the node stays Pending.
    async fn run_on(self, driver: Option<Arc<HookDriver>>) {
        let (root, name) = (self.root.clone(), self.name.clone());
        if let Err((failed, after)) = self.steps(driver).await {
            tracing::info_span!(parent: &root, "rdm.mesh.node.reject.via-start-step-failed", node = %name, step = failed.step, reason = %failed.reason)
                .in_scope(|| tracing::warn!("a start step after pre_node_ready failed: the node is not started and stays Pending"));
            after.start_failed(&failed, false).await;
        }
    }

    async fn steps(self, driver: Option<Arc<HookDriver>>) -> Result<(), (StartFailed, Box<dyn AfterHook>)> {
        let Self { root, name, path, node_id, client, rafka_time, topology_view, through, mut commander, mut after } = self;
        // 4. The topology, from the authority that answered the join.
        // A mesh the topology step installed, at the version it installed: what the rafka-time refresh
        // names, so the authority answers it `Unchanged`.
        let mut held: Option<(String, SourceVersion)> = None;
        if let Some((t, label)) = &through {
            let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-topology-taken", node = %name, launcher = %label);
            after.before_topology_read(&t.name).instrument(step.clone()).await;
            let read = match at_commander(&name, &client, t, commander.as_mut(), |target| {
                let (client, topology_view, mesh) = (&client, &topology_view, &t.name.mesh);
                async move { crate::topology_read::get_topology(client, &target, mesh, topology_view, None, None).await }
            }).instrument(step.clone()).await {
                Ok((read, _)) => read,
                Err((e, who)) => return Err((StartFailed { step: "node.topology", reason: format!("{name} could not read the topology of {who}: {e}") }, after)),
            };
            after.topology_installed(&topology_view, &read).instrument(step.clone()).await;
            held = read.installed.first().map(|m| (m.mesh.clone(), SourceVersion { publisher: m.publisher.clone(), topology_version: m.topology_version }));
            drop(step);
        }
        // The Build facts the authority's floor requires, on every start.
        let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-build-floor", node = %name, owed = tracing::field::Empty, blockers = tracing::field::Empty, outcome = tracing::field::Empty);
        after.floor(&step).instrument(step.clone()).await;
        drop(step);
        // rafka-time, last, from the same authority.
        let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-rafka-time-refreshed", node = %name, served_by = tracing::field::Empty, unchanged = tracing::field::Empty, outcome = tracing::field::Empty);
        match &through {
            Some((t, _)) => {
                // The read names the mesh the topology step installed and its version; an authority that
                // answered no snapshot is read whole again, as the topology step read it.
                let (mesh, since) = match held {
                    Some((mesh, since)) => (Some(mesh), Some(since)),
                    None => (None, None),
                };
                let (read, who) = match at_commander(&name, &client, t, commander.as_mut(), |target| {
                    let (client, node, mesh, since) = (&client, &name, mesh.as_deref(), since.clone());
                    async move { crate::topology_read::read_topology(client, &target, node, mesh, since).await }
                }).instrument(step.clone()).await {
                    Ok(read) => read,
                    Err((e, who)) => return Err((StartFailed { step: "node.rafka-time", reason: format!("{name} could not read rafka-time from {who}: {e}") }, after)),
                };
                step.record("served_by", tracing::field::display(&who));
                step.record("unchanged", !read.unchanged.is_empty());
                match step.in_scope(|| read.adopt_rafka_time(&rafka_time, &topology_view, &name, &path.mesh, &node_id, &who)) {
                    Ok(_) => {
                        step.record("outcome", "adopted");
                    }
                    // A primary takes rafka-time only from the fabric-primary (node-identity-and-state.md
                    // §2.5): any other answer is refused by name and the primary keeps the rafka-time it
                    // holds (`crate::rafka_time`). A re-entering fabric-primary holds the fabric's time.
                    Err(crate::topology_read::TimeNotAdopted::Refused(refusal)) => {
                        step.record("outcome", format!("kept: {refusal}").as_str());
                    }
                    Err(e) => return Err((StartFailed { step: "node.rafka-time", reason: format!("{name} could not adopt rafka-time from {who}: {e}") }, after)),
                }
            }
            None => {
                step.record("outcome", "no authority: the rafka-time this start adopted is kept");
            }
        }
        drop(step);
        // 5. node.started.
        after.report().await;
        if let Some(driver) = driver {
            driver.spawn_on_node_ready();
        }
        Ok(())
    }
}

/// The channel step of a first start's plug: given the step's span and the join answer, it enters
/// the channel and answers the topology view and the plug's node.joined step.
pub type EnterSeam<E> = Box<dyn FnOnce(tracing::Span, Option<JoinAnswer>) -> BoxFut<Result<(TopologyView, JoinedSeam<E>), E>> + Send>;

/// The node.joined step of a first start's plug: what the node brings up while its hook runs,
/// answering the hook's parts and, for a spawned hook, what the start does once the hook's task is
/// running.
pub type JoinedSeam<E> = Box<dyn FnOnce(Option<JoinThrough>) -> BoxFut<Result<(HookParts, Option<HookStarted<E>>), E>> + Send>;

/// What a first start's plug does once its spawned hook's task is running.
pub type HookStarted<E> = Box<dyn FnOnce(tokio::task::JoinHandle<()>) -> BoxFut<Result<(), E>> + Send>;

/// A first start's plug whose steps are continuations: each step runs in the scope the previous one
/// left, so a node kind that brings its runtime up while it starts (a node-admin hydrating its stores
/// and entering the election) holds what it built from one step to the next.
pub struct FirstStart<E> {
    /// The immutable app types presented by the first join.
    pub types: Vec<String>,
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

    fn join_types(&self) -> Vec<String> { self.types.clone() }

    fn join_listeners(&self) -> Vec<(String, std::net::SocketAddr)> { self.listeners.clone() }

    fn join_digest(&self) -> MeshDigest {
        self.digest.clone().expect("a first start that joins presents a digest")
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&JoinAnswer>) -> Result<TopologyView, E> {
        let (topology_view, next) = (self.enter.take().expect("the channel is entered once"))(step, joined.cloned()).await?;
        self.joined = Some(next);
        Ok(topology_view)
    }
}

#[async_trait::async_trait]
impl<E: From<StartFailed> + Send + 'static> StartNode for FirstStart<E> {

    fn starting(&self) -> Starting {
        let s = self.starting.as_ref().expect("a first start is begun once");
        Starting { root: s.root.clone(), name: s.name.clone(), path: s.path.clone(), node_id: s.node_id.clone(), client: s.client.clone(), rafka_time: s.rafka_time.clone() }
    }

    fn joining(&self) -> Joining {
        match self.joining.as_ref().expect("a first start is begun once") {
            Joining::Authority { label } => Joining::Authority { label: label.clone() },
            Joining::None(why) => Joining::None(*why),
        }
    }

    async fn resolve_join_to(&mut self) -> Result<JoinTo, StartFailed> {
        (self.resolve.take().expect("the authority is resolved once"))()
    }

    async fn joined(&mut self, _: &TopologyView, through: Option<&JoinThrough>, _: Option<&JoinAnswer>) -> Result<HookParts, E> {
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
    use crate::node_hooks::{Addressed, HookOutcome, OnNodeReady, PreNodeReady};
    use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeKind};

    struct RecordedAfter {
        steps: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl AfterHook for RecordedAfter {
        async fn topology_installed(&mut self, _: &TopologyView, _: &TopologyRead) {
            self.steps.lock().unwrap().push("topology_installed");
        }

        async fn floor(&mut self, step: &tracing::Span) {
            self.steps.lock().unwrap().push("floor");
            step.record("owed", "none");
            step.record("outcome", "unit-cell-floor-checked");
        }

        async fn report(self: Box<Self>) {
            self.steps.lock().unwrap().push("report");
        }
    }

    struct TestHook {
        steps: Arc<Mutex<Vec<&'static str>>>,
        observed_ctx: Arc<Mutex<Vec<(StartKind, bool)>>>,
        ready_notified: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl PreNodeReady for TestHook {
        async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
            self.steps.lock().unwrap().push("pre_node_ready");
            self.observed_ctx
                .lock()
                .unwrap()
                .push((ctx.start_kind, ctx.inherited));
            HookOutcome::Ok
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
        no_authority: NoAuthority,
        steps: Arc<Mutex<Vec<&'static str>>>,
        observed_ctx: Arc<Mutex<Vec<(StartKind, bool)>>>,
        ready_notified: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl super::super::node_mesh_join::MeshJoin for TestStartPlug {
        type Error = StartFailed;

        fn join_digest(&self) -> MeshDigest {
            MeshDigest {
                held_hash: Default::default(),
                fabric_id: FabricId::mint(),
                node: rdm_core::MeshNode {
                    node_id: self.starting.node_id.clone(),
                    name: self.starting.path.clone(),
                    endpoint_id: rdm_core::EndpointId(
                        self.starting.client.endpoint().id().to_string(),
                    ),
                    transport_addr: "127.0.0.1:0".parse().unwrap(),
                    incarnation: IncarnationId::mint(),
                    supersedes: None,
                    runtime: None,
                },
                status: MemberStatus::Ready,
                node_state: Default::default(),
                admin_api_base: None,
                digest_seq: 0,
                emitted_at_rafka_ms: 0,
                data_dir: None,
                mesh_id: None,
                in_flight: None,
                load: None,
                gossip: None,
                is_primary: None,
                is_fabric_primary: None,
            }
        }

        async fn enter_channel(
            &mut self,
            _step: tracing::Span,
            _joined: Option<&JoinAnswer>,
        ) -> Result<TopologyView, StartFailed> {
            self.steps.lock().unwrap().push("enter_channel");
            let ep = self.starting.client.endpoint().clone();
            let g = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
            TopologyView::join(
                &g,
                &ep,
                &FabricId::mint(),
                &self.starting.path.mesh,
                &MeshId::mint(),
                &self.starting.name,
                Arc::new(self.starting.rafka_time.clone()),
                Vec::new(),
                &[],
            )
            .await
            .map_err(|e| StartFailed {
                step: "node.channel",
                reason: e.to_string(),
            })
        }
    }

    #[async_trait::async_trait]
    impl StartNode for TestStartPlug {
        fn starting(&self) -> Starting {
            Starting {
                root: self.starting.root.clone(),
                name: self.starting.name.clone(),
                path: self.starting.path.clone(),
                node_id: self.starting.node_id.clone(),
                client: self.starting.client.clone(),
                rafka_time: self.starting.rafka_time.clone(),
            }
        }

        fn joining(&self) -> Joining {
            Joining::None(self.no_authority)
        }

        async fn resolve_join_to(&mut self) -> Result<JoinTo, StartFailed> {
            unreachable!("Joining::None does not resolve join target")
        }

        async fn joined(
            &mut self,
            topology_view: &TopologyView,
            _: Option<&JoinThrough>,
            _: Option<&JoinAnswer>,
        ) -> Result<HookParts, StartFailed> {
            self.steps.lock().unwrap().push("joined");
            let hook = Arc::new(TestHook {
                steps: self.steps.clone(),
                observed_ctx: self.observed_ctx.clone(),
                ready_notified: self.ready_notified.clone(),
            });
            Ok(HookParts {
                birth: Birth {
                    name: self.starting.path.clone(),
                    node_id: self.starting.node_id.clone(),
                    incarnation: IncarnationId::mint(),
                    fabric_id: FabricId::mint(),
                    mesh_id: MeshId::mint(),
                    kind: NodeKind::RpcNode,
                },
                node_hooks: NodeHooks::new(hook.clone()).with_on_node_ready(hook),
                events: Events::new(topology_view.digest_view.clone(), Addressed::new()),
                cancel: CancelToken::new(),
                state: Arc::default(),
                runs: HookRuns::Inline {
                    blocked: Arc::default(),
                },
                after: Box::new(RecordedAfter {
                    steps: self.steps.clone(),
                }),
            })
        }
    }

    /// CONTRACT (docs/architecture/lifecycles/node-start.md:20, 22-23, 45-66, 121-122, 126-127):
    /// "1. A stopped process remains the same exact runtime and keeps its endpoint/port; it does not start
    /// a new process or load a new executable.
    /// 2. Run node.mesh.join... 3. Run pre_node_ready with the start kind and inherited-data-dir
    /// indication...
    /// 4. Obtain complete topology through node.topology.get. Install only complete source snapshots;
    /// partial snapshots never complete start. The read is the node's hydration sync (Q234): with the
    /// topology it carries the node's own whole row, the recorded primaries the mesh-primary holds (a lost
    /// birth's marked gone) and each mesh's id, and a node-admin started by a launch reads none of them
    /// from the store (Q227). Fetch Build facts only when the node is below the primary's floor, after the
    /// hook and before node.started... Refresh rafka-time **last**, so it is current when the node re-enters.
    /// 5. Emit `NodeStarted` (Terminal): its hook passed, with nothing pending. Neither a partial snapshot
    /// nor a Blocked hook permits it. That frame is the node's report: the mesh-primary sets Ready on that
    /// exact birth through the node status apply, not a caller op. The node also declares Ready to its
    /// mesh-primary as an owed message, as it declares Stopped (Q36, Q87); frame and declaration apply once
    /// (Q49). A typed refusal of that apply completes no readiness.
    /// 6. Start ends with node.started. The node then calls on_node_ready once"
    /// "Failures and stream certainty follow the caller execution contract."
    /// "Trace propagation, step spans and correlation follow the runtime spans."
    #[test]
    fn node_start_executes_join_hook_floor_rafka_time_report_and_on_node_ready_in_order() {
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
            let err = reached_back("mesh1.rpc.1", &client, &missing_key)
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
            let root1 = tracing::info_span!("rdm.mesh.node.create.via-boot", node = "mesh1.rpc.1");
            let mut plug1 = TestStartPlug {
                starting: Starting {
                    root: root1,
                    name: "mesh1.rpc.1".into(),
                    path: "mesh1.rpc.1".parse().unwrap(),
                    node_id: NodeId::mint(),
                    client: client.clone(),
                    rafka_time: rafka_time.clone(),
                },
                no_authority: NoAuthority::Day0Root,
                steps: steps.clone(),
                observed_ctx: observed.clone(),
                ready_notified: notify1.clone(),
            };
            node_start(&mut plug1, StartKind::FirstStart)
                .await
                .expect("FirstStart succeeds");
            notify1.notified().await;

            // 3. AfterStop runs with inherited=true.
            let notify2 = Arc::new(tokio::sync::Notify::new());
            let root2 = tracing::info_span!("rdm.mesh.node.update.via-start", node = "mesh1.rpc.1");
            let mut plug2 = TestStartPlug {
                starting: Starting {
                    root: root2,
                    name: "mesh1.rpc.1".into(),
                    path: "mesh1.rpc.1".parse().unwrap(),
                    node_id: NodeId::mint(),
                    client,
                    rafka_time,
                },
                no_authority: NoAuthority::Day0Root,
                steps: steps.clone(),
                observed_ctx: observed.clone(),
                ready_notified: notify2.clone(),
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
            "rdm.mesh.node.update.via-rafka-time-refreshed",
            "rdm.mesh.node.update.via-on-node-ready",
        ] {
            assert!(
                spans.iter().any(|s| s["name"] == required),
                "expected span {required} in node_start execution"
            );
        }
    }
}
