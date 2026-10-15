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
//!    is never named by the name it held before its join (R-JN1, R12). A start with no authority (the Day-0 root, a fabric-primary reborn from its durable
//!    map, a recovery start) makes no join.
//! 2. `rdm.mesh.node.update.via-membership-joined` (step 2, node.joined): the node enters its mesh
//!    channel holding the name the join handed, before it publishes anything (R-JN1), and takes the
//!    status frames the join answered. The kind then brings up what it serves while its hook runs.
//! 3. `rdm.mesh.node.update.via-hydrate-attempt` (step 3): the `hydrate_before_ready` hook, with the
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
//! rafka-time to a started node (`rafka_time.rs`, the seat-move pull reads it the same way).

use crate::app_hydration::{AcceptingAuthority, Authority, Birth, End, Events, HookDriver, HydrateCtx, Hydration, HydrationHandle, NoAuthority, StartKind, Step};
use crate::model::{NodeId, PathName};
use crate::node_self::StartFailed;
use crate::topology_read::TopologyRead;
use crate::wire::JoinAnswer;
use rdm_core::{MemberStatus, MeshDigest};
use rafka_mesh_transport::membership::Membership;
use rafka_node_rpc::{CancelToken, NodeRpcClient, NodeTarget};
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
    pub rafka_time: rafka_mesh_transport::clock::RafkaTime,
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

/// `JoinNode` with `digest`, each of `attempts` made to whom `to` names then: the launcher, or the
/// admin whose call commanded the start last, reached back over that call. The answer and who gave
/// it: `served_by` names the admin (the launcher's record names it at a first start).
async fn join(node: &str, client: &NodeRpcClient, to: &JoinTo, digest: &MeshDigest) -> Result<(JoinAnswer, JoinThrough), StartFailed> {
    let next = || {
        let to = match to {
            JoinTo::Launcher { target, endpoint, .. } => Ok((target.clone(), endpoint.fmt_short().to_string())),
            JoinTo::Commander(commander) => Err(*commander.borrow()),
        };
        async move {
            match to {
                Ok(t) => Ok(t),
                Err(key) => reached_back(node, client, &key).await.map(|t| (t, key.fmt_short().to_string())),
            }
        }
    };
    let (answer, target) = match crate::join::call_join_to(client, next, digest, JOIN_ATTEMPTS).await {
        Ok(joined) => joined,
        Err(crate::join::JoinFailure::Refused(why)) => return Err(StartFailed { step: "node.join", reason: format!("{node} was refused its join by {}: {why}", whom(to)) }),
        Err(crate::join::JoinFailure::Unreached(why)) => return Err(StartFailed { step: "node.join", reason: format!("{node} could not join through {}: unreached: {why}", whom(to)) }),
    };
    let (endpoint, name) = match (to, &target) {
        (JoinTo::Launcher { endpoint, name, .. }, _) => (*endpoint, name.clone()),
        (JoinTo::Commander(_), NodeTarget::Endpoint { endpoint_id, .. }) => match answer.served_by.parse::<PathName>() {
            Ok(n) => (*endpoint_id, n),
            Err(e) => return Err(StartFailed { step: "node.join", reason: format!("{node}: the join answer from endpoint {endpoint_id} names who answered as {:?}, which is no path.name: {e}", answer.served_by) }),
        },
        (JoinTo::Commander(_), other) => unreachable!("a commander is reached back over its call's endpoint, never {other:?}"),
    };
    Ok((answer, JoinThrough { target, endpoint, name, anchor: endpoint.fmt_short().to_string() }))
}

/// Whom a join goes to, in words.
fn whom(to: &JoinTo) -> String {
    match to {
        JoinTo::Launcher { name, .. } => name.to_string(),
        JoinTo::Commander(commander) => format!("the admin whose call commanded the start (endpoint {})", *commander.borrow()),
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
    pub hydration: Hydration,
    /// The retry events of this birth.
    pub events: Events,
    /// Cancelled when the birth is retired: the hook and any pull it has in flight end.
    pub cancel: CancelToken,
    /// Where the node's Ready check and its probe read the hook's state.
    pub state: Arc<Mutex<Option<HydrationHandle>>>,
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
    async fn topology_installed(&mut self, membership: &Membership, read: &TopologyRead);

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
pub trait StartNode: Send {
    /// This kind's refusal of a start; a step's typed failure converts into it.
    type Error: From<StartFailed> + Send;

    /// What this start knows before it begins.
    fn starting(&self) -> Starting;

    /// Whom this start joins.
    fn joining(&self) -> Joining;

    /// Whom the join goes to; called inside the join step. A start after a stop answers
    /// [`JoinTo::Commander`], whatever the node kind.
    async fn resolve_authority(&mut self) -> Result<JoinTo, StartFailed>;

    /// The digest the join presents; the start presents it Pending.
    fn join_digest(&self) -> MeshDigest;

    /// Enter the mesh channel holding the name the join handed (`None` with no authority), before
    /// anything is published, and take what the join answered of the node's control state. `step` is
    /// the channel step's span: the kind drops it when its channel part ends and records `mesh_id` and
    /// `seeds` on it.
    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&JoinAnswer>) -> Result<Membership, Self::Error>;

    /// node.joined: the node is on its channel. The kind takes what the join answered of its control
    /// state (a node-admin records the Build floor here, at every start), brings up what it serves
    /// while its hook runs (it is Pending) and hands the hook's parts. `through` is the authority the
    /// join reached and `joined` its answer (both `None` with no authority).
    async fn joined(&mut self, membership: &Membership, through: Option<&JoinThrough>, joined: Option<&JoinAnswer>) -> Result<HookParts, Self::Error>;

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
    // 1. The join: the answer's rafka-time and cert are taken before the node says anything.
    let (through, joined) = match node.joining() {
        Joining::Authority { label } => {
            let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-join-admitted", node = %name, launcher = %label, served_by = tracing::field::Empty, endpoint = tracing::field::Empty, installed = tracing::field::Empty);
            let to = node.resolve_authority().await?;
            let mut digest = node.join_digest();
            digest.status = MemberStatus::Pending;
            let mesh_id_str = digest.mesh_id.as_ref().map(ToString::to_string).unwrap_or_default();
            let op_name = match kind {
                StartKind::FirstStart => format!("node.create:{name}"),
                StartKind::AfterStop => format!("node.start:{name}"),
            };
            let joining_span = match kind {
                StartKind::FirstStart => tracing::info_span!(
                    parent: &step,
                    "rdm.node.mesh.update.via-joining",
                    node = %name,
                    node_id = %node_id,
                    mesh = %path.mesh,
                    mesh_id = %mesh_id_str,
                    authority = %label,
                    operation = %op_name,
                ),
                StartKind::AfterStop => tracing::info_span!(
                    "rdm.node.mesh.update.via-joining",
                    node = %name,
                    node_id = %node_id,
                    mesh = %path.mesh,
                    mesh_id = %mesh_id_str,
                    authority = %label,
                    operation = %op_name,
                ),
            };
            let (answer, through) = join(&name, &client, &to, &digest).instrument(joining_span).await?;
            // Who answered, as the join answer names it (node-start.md step 2), and the endpoint it
            // answered on.
            step.record("served_by", through.name.to_string().as_str());
            step.record("endpoint", through.anchor.as_str());
            // The name the answer hands (a join that hands none is refused, `crate::join::call_join`):
            // the node holds it from the answer on, and every step from here is named by it.
            if let Some(handed) = &answer.control.name {
                name = handed.to_string();
            }
            crate::rafka_time::adopt_join_answer(&step, &rafka_time, &name, &answer);
            step.record("installed", name.as_str());
            drop(step);
            (Ok((through, label)), Some(answer))
        }
        Joining::None(why) => (Err(why), None),
    };
    let member_cert = joined.as_ref().map(|a| a.control.member_cert.clone());
    // 2. The mesh channel, holding the name the join handed, and the status frames the join answered:
    //    node.joined.
    let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-membership-joined", node = %name, mesh_id = tracing::field::Empty, seeds = tracing::field::Empty);
    let membership = node.enter_channel(step, joined.as_ref()).await?;
    if let Some(answer) = &joined {
        membership.learn_statuses(&answer.statuses);
    }
    let parts = node.joined(&membership, through.as_ref().ok().map(|(t, _)| t), joined.as_ref()).await?;
    // 3. The hook, with no topology read.
    let authority = match &through {
        Ok((t, _)) => Authority::Accepted(AcceptingAuthority { name: t.name.clone(), endpoint: t.endpoint, target: t.target.clone() }),
        Err(why) => Authority::None(*why),
    };
    let HookParts { birth, hydration, events, cancel, state, runs, after } = parts;
    let inherited = matches!(kind, StartKind::AfterStop)
        || matches!(authority, Authority::None(NoAuthority::InheritedRoot | NoAuthority::Recovery))
        || std::env::var("RDM_INHERITED_DATA_DIR").ok().as_deref() == Some("1");
    let ctx = HydrateCtx { start_kind: kind, birth, authority, client: client.clone(), membership: membership.clone(), cancel, rafka_time: rafka_time.clone(), member_cert, attempt: 0, inherited };
    let driver = hydration.driver(ctx, events, root.clone());
    *state.lock().unwrap() = driver.as_ref().map(|d| d.handle());
    // 4. and 5., once the hook passed.
    let rest = AfterTheHook { root, name: name.clone(), path, node_id, client, rafka_time, membership, through: through.ok(), after };
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
                    let failed = StartFailed { step: "node.hydrate", reason: format!("{name} failed hydrate_before_ready: {why}") };
                    rest.after.start_failed(&failed, true).await;
                    Err(failed.into())
                }
                Step::Retired => match kind {
                    // A first start retired while its hook ran ends without a report and without a refusal.
                    StartKind::FirstStart => Ok(()),
                    StartKind::AfterStop => Err(StartFailed { step: "node.hydrate", reason: format!("{name} was retired while hydrate_before_ready ran") }.into()),
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
    rafka_time: rafka_mesh_transport::clock::RafkaTime,
    membership: Membership,
    /// The authority the join reached, and what the step spans name it; `None` with no authority.
    through: Option<(JoinThrough, String)>,
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
                .in_scope(|| tracing::warn!("a start step after hydrate_before_ready failed: the node is not started and stays Pending"));
            after.start_failed(&failed, false).await;
        }
    }

    async fn steps(self, driver: Option<Arc<HookDriver>>) -> Result<(), (StartFailed, Box<dyn AfterHook>)> {
        let Self { root, name, path, node_id, client, rafka_time, membership, through, mut after } = self;
        // 4. The topology, from the authority that answered the join.
        // A mesh the topology step installed, at the version it installed: what the rafka-time refresh
        // names, so the authority answers it `Unchanged`.
        let mut held: Option<(String, SourceVersion)> = None;
        if let Some((t, label)) = &through {
            let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-topology-taken", node = %name, launcher = %label);
            after.before_topology_read(&t.name).instrument(step.clone()).await;
            let read = match crate::topology_read::get_topology(&client, &t.target, &t.name.mesh, &membership, None, None).instrument(step.clone()).await {
                Ok(read) => read,
                Err(e) => return Err((StartFailed { step: "node.topology", reason: format!("{name} could not read the topology of {}: {e}", t.name) }, after)),
            };
            after.topology_installed(&membership, &read).instrument(step.clone()).await;
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
                step.record("served_by", tracing::field::display(&t.name));
                // The read names the mesh the topology step installed and its version; an authority that
                // answered no snapshot is read whole again, as the topology step read it.
                let (mesh, since) = match held {
                    Some((mesh, since)) => (Some(mesh), Some(since)),
                    None => (None, None),
                };
                let read = match crate::topology_read::read_topology(&client, &t.target, &name, mesh.as_deref(), since).instrument(step.clone()).await {
                    Ok(read) => read,
                    Err(e) => return Err((StartFailed { step: "node.rafka-time", reason: format!("{name} could not read rafka-time from {}: {e}", t.name) }, after)),
                };
                step.record("unchanged", !read.unchanged.is_empty());
                if let Err(e) = step.in_scope(|| read.adopt_rafka_time(&rafka_time, &membership, &name, &path.mesh, &node_id, &t.name.to_string())) {
                    return Err((StartFailed { step: "node.rafka-time", reason: format!("{name} could not adopt rafka-time from {}: {e}", t.name) }, after));
                }
                step.record("outcome", "adopted");
            }
            None => {
                step.record("outcome", "no authority: the rafka-time this start adopted is kept");
            }
        }
        drop(step);
        // 5. node.started.
        after.report().await;
        if let Some(driver) = driver {
            driver.spawn_after_ready();
        }
        Ok(())
    }
}

/// The channel step of a first start's plug: given the step's span and the join answer, it enters
/// the channel and answers the membership and the plug's node.joined step.
pub type EnterSeam<E> = Box<dyn FnOnce(tracing::Span, Option<JoinAnswer>) -> BoxFut<Result<(Membership, JoinedSeam<E>), E>> + Send>;

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
impl<E: From<StartFailed> + Send + 'static> StartNode for FirstStart<E> {
    type Error = E;

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

    async fn resolve_authority(&mut self) -> Result<JoinTo, StartFailed> {
        (self.resolve.take().expect("the authority is resolved once"))()
    }

    fn join_digest(&self) -> MeshDigest {
        self.digest.clone().expect("a first start that joins presents a digest")
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&JoinAnswer>) -> Result<Membership, E> {
        let (membership, next) = (self.enter.take().expect("the channel is entered once"))(step, joined.cloned()).await?;
        self.joined = Some(next);
        Ok(membership)
    }

    async fn joined(&mut self, _: &Membership, through: Option<&JoinThrough>, _: Option<&JoinAnswer>) -> Result<HookParts, E> {
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
