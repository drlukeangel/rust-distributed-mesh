//! `node.start` (lifecycles/node-start.md, "End-to-end sequence"): the one start workflow. node.create
//! composes it with [`StartKind::FirstStart`] (lifecycles/node-create.md, step 3); a start of a stopped
//! node runs it with [`StartKind::AfterStop`] (node_self.rs, `StartNode`). Every node kind runs this
//! sequence; a kind differs only by what it plugs into each step ([`StartNode`]), never by a copy of
//! the sequence.
//!
//! The steps, in order, each a child span of the start's root (the birth's boot span at a first start,
//! `rdm.mesh.node.update.via-start` after a stop):
//!
//! 1. `rdm.mesh.node.update.via-join-admitted`: `JoinNode` to the authority (the admin that launched
//!    the node, or the one that commanded its start), presenting the digest the node holds, Pending;
//!    the answer's rafka-time is adopted, and the name it hands (its row's) is the step's `installed`:
//!    the node holds it from the channel step on, and every later step is named by it, so a node
//!    renamed while stopped is never named by the name it held before its join (R-JN1, R12). A start
//!    with no authority (the Day-0 root, a fabric-primary reborn from its durable map, a recovery
//!    start) makes no join.
//! 2. `rdm.mesh.node.update.via-membership-joined`: the node enters its mesh channel holding the name
//!    the join handed, before it publishes anything (R-JN1).
//! 3. `rdm.mesh.node.update.via-topology-taken`: `GetTopology` from the same authority, installed per
//!    mesh only from a complete snapshot; the read's rafka-time is adopted.
//! 4. `rdm.mesh.node.update.via-hydrate-attempt`: the `hydrate_before_ready` hook, with the start kind,
//!    the accepting authority (or why there is none), the joined channel and the member cert.
//! 5. `rdm.mesh.node.update.via-ready`: the node's report once the hook passed.
//!
//! The topology is read before the hook, and rafka-time is adopted from the join answer and again
//! from the topology read.

use crate::app_hydration::{AcceptingAuthority, Authority, Birth, End, Events, HydrateCtx, Hydration, HydrationHandle, NoAuthority, StartKind, Step};
use crate::model::{IncarnationId, NodeId, PathName};
use crate::node_self::StartFailed;
use crate::topology_read::TopologyRead;
use crate::wire::JoinAnswer;
use rafka_mesh_entity::{MemberStatus, MeshDigest};
use rafka_mesh_transport::membership::Membership;
use rafka_node_rpc::{CancelToken, NodeRpcClient, NodeTarget};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

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
    /// The process's one Node RPC client: the join and the topology read go through it.
    pub client: Arc<NodeRpcClient>,
    /// The process's rafka-time, adopted from the join answer and from the topology read.
    pub rafka_time: rafka_mesh_transport::clock::RafkaTime,
}

/// Whom a start joins.
pub enum Joining {
    /// An authority, named in the join and topology steps' `launcher` as `label`.
    Authority {
        /// What the step spans name the authority.
        label: String,
    },
    /// No authority, and why: the start makes no join and reads no topology.
    None(NoAuthority),
}

/// The authority a start joins, resolved so the call reaches it.
pub struct JoinThrough {
    /// Its node id: the join and the topology read go to exactly this node.
    pub node_id: NodeId,
    /// Its `path.name`.
    pub name: PathName,
    /// Its incarnation: the hook's accepting authority.
    pub incarnation: IncarnationId,
    /// The short endpoint id the join's attempts name.
    pub anchor: String,
}

/// The report of `node.started`, made once the hook passed.
pub type Report = Box<dyn FnOnce() -> BoxFut<()> + Send>;

/// Starts the task a node-admin's first-start hook runs in, behind the admin's own task gate.
pub type Spawn = Box<dyn FnOnce(BoxFut<()>) -> tokio::task::JoinHandle<()> + Send>;

/// How the hook runs.
pub enum HookRuns {
    /// The first attempt runs inside the start, which answers its outcome; a blocked hook runs on in
    /// a task held in `blocked`, and `report` is made when it passes.
    Inline {
        /// The report once the hook passed.
        report: Report,
        /// Where the task of a blocked hook is held, so a stop ends it.
        blocked: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    },
    /// The start returns while the hook runs in the task `spawn` starts: the node's own Ready check
    /// makes the report, reading the hook's state ([`StartNode::hook_started`]).
    Spawned {
        /// Starts the hook's task.
        spawn: Spawn,
    },
}

/// What the hook runs with.
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

    /// The authority, resolved so the join reaches it; called inside the join step.
    fn resolve_authority(&mut self) -> Result<JoinThrough, StartFailed>;

    /// The digest the join presents; the start presents it Pending.
    fn join_digest(&self) -> MeshDigest;

    /// Enter the mesh channel holding the name the join handed (`None` with no authority), before
    /// anything is published. `step` is the channel step's span: the kind drops it when its channel
    /// part ends and records `mesh_id` and `seeds` on it.
    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&JoinAnswer>) -> Result<Membership, Self::Error>;

    /// Inside the topology step, before the read: the status frames the join answered are taken.
    async fn before_topology(&mut self, membership: &Membership, joined: &JoinAnswer) {
        membership.learn_statuses(&joined.statuses);
    }

    /// The topology read is installed (`read` holds the topology step's span, which the kind drops
    /// when its part of the step ends; `None` for a start with no authority). The kind does its own
    /// work before its hook and hands the hook's parts.
    async fn topology_installed(&mut self, membership: &Membership, read: Option<(tracing::Span, TopologyRead)>, joined: Option<JoinAnswer>) -> Result<HookParts, Self::Error>;

    /// The hook failed inside an inline start: the kind ends what the start brought up.
    async fn hook_failed(&mut self) {}

    /// The hook of a [`HookRuns::Spawned`] start runs in `task` (`None` with no hook registered):
    /// the node's Ready check makes the report from here.
    async fn hook_started(&mut self, task: Option<tokio::task::JoinHandle<()>>) -> Result<(), Self::Error> {
        drop(task);
        Ok(())
    }
}

/// `node.start`: the join, the mesh channel, the topology, the hook and the report, in that order,
/// for `node` as it plugs into each step.
pub async fn node_start<N: StartNode>(node: &mut N, kind: StartKind) -> Result<(), N::Error> {
    let Starting { root, mut name, path, node_id, client, rafka_time } = node.starting();
    // 1. The join: the answer's rafka-time and cert are taken before the node says anything.
    let (through, joined) = match node.joining() {
        Joining::Authority { label } => {
            let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-join-admitted", node = %name, launcher = %label, installed = tracing::field::Empty);
            let through = node.resolve_authority()?;
            let mut digest = node.join_digest();
            digest.status = MemberStatus::Pending;
            let answer = match crate::join::call_join(&client, &NodeTarget::ExactNode(through.node_id.clone()), &through.anchor, &digest, JOIN_ATTEMPTS).await {
                Ok(a) => a,
                Err(crate::join::JoinFailure::Refused(why)) => return Err(StartFailed { step: "node.join", reason: format!("{name} was refused its join by {}: {why}", through.name) }.into()),
                Err(crate::join::JoinFailure::Unreached(why)) => return Err(StartFailed { step: "node.join", reason: format!("{name} could not join through {}: unreached: {why}", through.name) }.into()),
            };
            crate::rafka_time::adopt_join_answer(&step, &rafka_time, &name, &answer);
            // The name the answer hands (a join that hands none is refused, `crate::join::call_join`):
            // the node holds it from the channel step on, and every later step is named by it.
            if let Some(handed) = &answer.control.name {
                name = handed.to_string();
            }
            step.record("installed", name.as_str());
            drop(step);
            (Ok((through, label)), Some(answer))
        }
        Joining::None(why) => (Err(why), None),
    };
    let member_cert = joined.as_ref().map(|a| a.control.member_cert.clone());
    // 2. The mesh channel, holding the name the join handed.
    let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-membership-joined", node = %name, mesh_id = tracing::field::Empty, seeds = tracing::field::Empty);
    let membership = node.enter_channel(step, joined.as_ref()).await?;
    // 3. The topology, from the authority that answered the join, and its rafka-time.
    let read = match (&through, &joined) {
        (Ok((through, label)), Some(answer)) => {
            let step = tracing::info_span!(parent: &root, "rdm.mesh.node.update.via-topology-taken", node = %name, launcher = %label);
            node.before_topology(&membership, answer).await;
            let read = crate::topology_read::get_topology(&client, &NodeTarget::ExactNode(through.node_id.clone()), &through.name.mesh, &membership, None, None)
                .await
                .map_err(|e| StartFailed { step: "node.topology", reason: format!("{name} could not read the topology of {}: {e}", through.name) })?;
            read.adopt_rafka_time(&rafka_time, &membership, &name, &path.mesh, &node_id, &through.name.to_string())
                .map_err(|e| StartFailed { step: "node.topology", reason: format!("{name} could not adopt rafka-time from {}: {e}", through.name) })?;
            Some((step, read))
        }
        _ => None,
    };
    let parts = node.topology_installed(&membership, read, joined).await?;
    // 4. The hook, then 5. the report.
    let authority = match &through {
        Ok((t, _)) => Authority::Accepted(AcceptingAuthority { name: t.name.clone(), node_id: t.node_id.clone(), incarnation: t.incarnation.clone() }),
        Err(why) => Authority::None(*why),
    };
    let HookParts { birth, hydration, events, cancel, state, runs } = parts;
    let ctx = HydrateCtx { start_kind: kind, birth, authority, client, membership, cancel, rafka_time, member_cert, attempt: 0 };
    let driver = hydration.driver(ctx, events, root);
    *state.lock().unwrap() = driver.as_ref().map(|d| d.handle());
    match runs {
        HookRuns::Spawned { spawn } => {
            let task = driver.map(|driver| {
                spawn(Box::pin(async move {
                    let first = driver.attempt().await;
                    if driver.run(first).await == End::Passed {
                        driver.spawn_after_ready();
                    }
                }))
            });
            node.hook_started(task).await
        }
        HookRuns::Inline { report, blocked } => {
            let Some(driver) = driver else {
                report().await;
                return Ok(());
            };
            match driver.attempt().await {
                Step::Passed => {
                    report().await;
                    driver.spawn_after_ready();
                    Ok(())
                }
                Step::Failed(why) => {
                    node.hook_failed().await;
                    Err(StartFailed { step: "node.hydrate", reason: format!("{name} failed hydrate_before_ready: {why}") }.into())
                }
                Step::Retired => match kind {
                    // A first start retired while its hook ran ends without a report and without a refusal.
                    StartKind::FirstStart => Ok(()),
                    StartKind::AfterStop => Err(StartFailed { step: "node.hydrate", reason: format!("{name} was retired while hydrate_before_ready ran") }.into()),
                },
                blocked_step @ Step::Blocked(_) => {
                    *blocked.lock().unwrap() = Some(tokio::spawn(async move {
                        if driver.run(blocked_step).await == End::Passed {
                            report().await;
                            driver.spawn_after_ready();
                        }
                    }));
                    Ok(())
                }
            }
        }
    }
}

/// The channel step of a first start's plug: given the step's span and the join answer, it enters
/// the channel and answers the membership and the plug's next step.
pub type EnterSeam<E> = Box<dyn FnOnce(tracing::Span, Option<JoinAnswer>) -> BoxFut<Result<(Membership, InstalledSeam<E>), E>> + Send>;

/// The installed-topology step of a first start's plug: what the node does once the topology is
/// installed, answering the hook's parts and what follows the hook.
pub type InstalledSeam<E> = Box<dyn FnOnce(Option<(tracing::Span, TopologyRead)>, Option<JoinAnswer>) -> BoxFut<Result<(HookParts, AfterHook<E>), E>> + Send>;

/// What a first start's plug does after its hook began.
pub enum AfterHook<E> {
    /// An inline hook: what the start ends if the hook fails.
    OnFailure(Box<dyn FnOnce() -> BoxFut<()> + Send>),
    /// A spawned hook: the node's Ready check, with the hook's task.
    Started(Box<dyn FnOnce(Option<tokio::task::JoinHandle<()>>) -> BoxFut<Result<(), E>> + Send>),
}

/// A first start's plug whose steps are continuations: each step runs in the scope the previous one
/// left, so a node kind that brings its runtime up while it starts (a node-admin hydrating its stores
/// and entering the election) holds what it built from one step to the next.
pub struct FirstStart<E> {
    /// What the start knows before it begins.
    pub starting: Option<Starting>,
    /// Whom it joins.
    pub joining: Option<Joining>,
    /// Resolves the authority for the join.
    pub resolve: Option<Box<dyn FnOnce() -> Result<JoinThrough, StartFailed> + Send>>,
    /// The digest the join presents.
    pub digest: Option<MeshDigest>,
    /// Whether the status frames the join answered are taken before the topology read (`false`: the
    /// kind takes them in its installed-topology step).
    pub statuses_before_topology: bool,
    /// The channel step.
    pub enter: Option<EnterSeam<E>>,
    /// The installed-topology step, once the channel step answered it.
    pub installed: Option<InstalledSeam<E>>,
    /// What follows the hook, once the installed-topology step answered it.
    pub after_hook: Option<AfterHook<E>>,
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

    fn resolve_authority(&mut self) -> Result<JoinThrough, StartFailed> {
        (self.resolve.take().expect("the authority is resolved once"))()
    }

    fn join_digest(&self) -> MeshDigest {
        self.digest.clone().expect("a first start that joins presents a digest")
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&JoinAnswer>) -> Result<Membership, E> {
        let (membership, installed) = (self.enter.take().expect("the channel is entered once"))(step, joined.cloned()).await?;
        self.installed = Some(installed);
        Ok(membership)
    }

    async fn before_topology(&mut self, membership: &Membership, joined: &JoinAnswer) {
        if self.statuses_before_topology {
            membership.learn_statuses(&joined.statuses);
        }
    }

    async fn topology_installed(&mut self, _: &Membership, read: Option<(tracing::Span, TopologyRead)>, joined: Option<JoinAnswer>) -> Result<HookParts, E> {
        let (parts, after) = (self.installed.take().expect("the topology is installed once"))(read, joined).await?;
        self.after_hook = Some(after);
        Ok(parts)
    }

    async fn hook_failed(&mut self) {
        if let Some(AfterHook::OnFailure(end)) = self.after_hook.take() {
            end().await;
        }
    }

    async fn hook_started(&mut self, task: Option<tokio::task::JoinHandle<()>>) -> Result<(), E> {
        match self.after_hook.take() {
            Some(AfterHook::Started(started)) => started(task).await,
            _ => Ok(()),
        }
    }
}
