//! Node investigation (rulings INDEX Q107 item 1, Q393, Q420–Q423; node-investigation.md): the one
//! function every caller uses on one exact birth, ending, for its client, in one result: `ready` (the
//! birth answered a tickle), `rejoined` (the investigation brought it back: commanded back through
//! node.mesh.join, or started) or `re-created` (written `dead`, and node recovery creates it again at
//! its path and name with a new node.id) ([`crate::wiring::NodeInvestigatedResult`]).
//!
//! Its steps, in order: (a) the leads, one read of the store's by-destination connections index
//! (connections.md §3.1), naming the nodes in any mesh that hold a connection to the exact birth: the
//! peers the kick goes through first, never evidence of reachability, since a row nobody marked
//! dropped survives a cut or a crashed source; (b) the provider about the exact runtime
//! (node.runtime.inspect, node-runtime-inspect.md): on `exited` or `os-stopped` no tickle is sent;
//! (c) the direct tickle and (d) the same kick through peers, the leads first
//! ([`crate::offline::tickle`], node-tickle.md). Only an answer from the exact birth counts;
//! unreached, the inspect's answer leads on, an `unavailable` one asked again under the
//! investigation's retries ([`ask_again`]) and the birth written `dead` at their end. What those steps
//! found is a [`Finding`], a step inside the investigation and never its result.

use crate::model::{Node, NodeId, NodeStatus};
use crate::offline::{Tickle, TickleCalls};
use crate::topology::Topology;
use rdm_core::connections::{ConnectionKind, ConnectionState, NodeConnection};

/// What the steps of the investigation of one exact birth found (node-investigation.md steps 2 to 4):
/// the birth's answer to the tickle, or node.runtime.inspect's answer, or a birth not proven by the end
/// of the retries. A step inside the investigation, never its result (Q420): [`follows`] says what it
/// leads to, and the result is written where the investigation ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Finding {
    /// `reached`: the exact birth answered the tickle (node-tickle.md "Result"), direct or through a
    /// peer, and said what it is.
    Reached {
        /// The birth's own answer (`Current`, `CurrentStopped` or `Blocked`).
        answer: rdm_core::status::StatusReply,
        /// The peer that carried the kick, by path and name; `None`: it answered the direct probe.
        carrier: Option<NodeId>,
    },
    /// The provider inspected its exact runtime and it still runs.
    Running,
    /// The provider inspected its exact runtime and the operating system holds it suspended
    /// (deployment-provider.md §2.5).
    OsStopped,
    /// The provider inspected its exact runtime and it exited, `exited { code }`
    /// (node-runtime-inspect.md "Result").
    Exited {
        /// The exit code, when one is provable.
        code: Option<i32>,
    },
    /// The birth is not proven alive or exited by the end of the investigation's retries (Q393): its
    /// inspect answered `unavailable` each time it was asked, or no exact birth was there to ask about;
    /// named by what was missing.
    Dead {
        /// What was missing.
        reason: String,
    },
}

impl Finding {
    /// The step's answer by name: `reached` (the tickle's, node-tickle.md), `running`, `os-stopped` or
    /// `exited` (the inspect's), or `dead` for a birth not proven by the end of the retries.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Reached { .. } => "reached",
            Self::Running => "running",
            Self::OsStopped => "os-stopped",
            Self::Exited { .. } => "exited",
            Self::Dead { .. } => "dead",
        }
    }

    /// node.runtime.inspect's answer the finding came from, `dead` as its last `unavailable`; `None` for
    /// `reached`, which the birth's answer to the tickle decided.
    pub fn inspected(&self) -> Option<crate::deployment::provider::Inspected> {
        use crate::deployment::provider::Inspected;
        match self {
            Self::Reached { .. } => None,
            Self::Running => Some(Inspected::Running),
            Self::OsStopped => Some(Inspected::OsStopped),
            Self::Exited { code } => Some(Inspected::Exited(*code)),
            Self::Dead { reason } => Some(Inspected::Unavailable(reason.clone())),
        }
    }

    /// What the finding names beyond its name: the carrier, the exit code or what was missing.
    pub fn detail(&self) -> String {
        match self {
            Self::Reached { carrier: Some(carrier), .. } => carrier.to_string(),
            Self::Exited { code } => code.map(|c| c.to_string()).unwrap_or_else(|| "no code provable".into()),
            Self::Dead { reason } => reason.clone(),
            _ => String::new(),
        }
    }
}

/// Why an investigation runs (Q393; node-recovery.md steps 1 and 3): the reason is the
/// investigation's, named on its `rdm.node.investigated` event and its spans, never a status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// A self-heal: the birth went unheard past the staleness floor and its mesh-primary investigates
    /// it, the birth `PendingReconnect` while it runs.
    SelfHeal,
    /// An ordered investigation, which sets no `PendingReconnect`: `trigger` names what ordered it, a
    /// command never sent for a reachability reason (`connect-refused`, `connect-timed-out`,
    /// `connect-failed`, `endpoint-unresolved`), the mesh-primary's own call ended with its attempts
    /// spent (`attempts-spent`) or another caller's `ReportCallFailed` (`report-call-failed`).
    Ordered {
        /// What ordered it.
        trigger: &'static str,
    },
}

impl Reason {
    /// `self-heal` or `ordered`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SelfHeal => "self-heal",
            Self::Ordered { .. } => "ordered",
        }
    }

    /// What started it: `silence` for a self-heal, else what ordered it.
    pub fn trigger(&self) -> &'static str {
        match self {
            Self::SelfHeal => "silence",
            Self::Ordered { trigger } => trigger,
        }
    }
}

/// Whether an ordered investigation's `finding` matches `status`, the birth's status in the topology,
/// so it does nothing and ends with no result (node-recovery.md step 3, Q393, Q425: "an ordered
/// investigation's `os-stopped` matches `stopped` and does nothing"): the inspect's answer `running`
/// (and `reached` with any answer but `CurrentStopped`, the birth's own word that it runs) and a live
/// status or a lifecycle state the node reported, in which its runtime runs; `os-stopped` (and
/// `reached` answering `CurrentStopped`, the birth's own word that it is stopped) and `stopped`;
/// `exited` and `dead`. Anything else differs and follows the self-heal.
pub fn matches_topology(finding: &Finding, status: NodeStatus) -> bool {
    use NodeStatus as S;
    match finding {
        Finding::Reached { answer: rdm_core::status::StatusReply::CurrentStopped { .. }, .. } => status == S::Stopped,
        Finding::Reached { .. } | Finding::Running => matches!(status, S::Starting | S::Pending | S::Ready | S::ConnectionsDeleting | S::ConnectionsDeleted | S::Stopping | S::Stopped),
        Finding::OsStopped => status == S::Stopped,
        Finding::Exited { .. } => status == S::Dead,
        Finding::Dead { .. } => false,
    }
}

/// What follows a [`Finding`] (node-recovery.md step 3, Q393), and so the result it leads to
/// (node-investigation.md step 5, Q420–Q423).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Follows {
    /// An ordered investigation whose finding matches the birth's status in the topology: nothing.
    Nothing,
    /// A reached birth ends `ready`, and one that answered `Current` is commanded back through
    /// node.mesh.join for its view (Q87), which writes nothing; an unreached `running` one is commanded
    /// back, sent until answered: answered, `rejoined`.
    MeshJoin,
    /// The birth is commanded to start (node.start), sent until answered: answered, `rejoined`. An
    /// `os-stopped` inspect (Q393: "stopped is asked to start"). A reached birth answering
    /// `CurrentStopped` is never started: a node stops only by a command, and a commanded stop is never
    /// undone by an investigation (Q425).
    Start,
    /// The birth is written `dead`, and its recovery runs at once: `re-created`.
    Dead,
}

impl Follows {
    /// `nothing`, `node.mesh.join`, `node.start` or `dead`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Nothing => "nothing",
            Self::MeshJoin => "node.mesh.join",
            Self::Start => "node.start",
            Self::Dead => "dead",
        }
    }
}

/// What follows `finding` for an investigation run for `reason` of a birth whose status in the
/// topology is `status` (node-recovery.md step 3, Q393): an ordered investigation whose finding
/// matches the status ([`matches_topology`]) does nothing; any other finding follows the self-heal:
/// `reached` ends `ready` ([`Follows::MeshJoin`], node-investigation.md: "to a tickle, the
/// investigation ends `ready`"), a birth answering `CurrentStopped` included, which is never started
/// (Q425); `running` is commanded back through node.mesh.join, `os-stopped` is commanded to start, and
/// `exited` or `dead` (not proven by the end of the retries) is written `dead`.
pub fn follows(finding: &Finding, reason: Reason, status: NodeStatus) -> Follows {
    if matches!(reason, Reason::Ordered { .. }) && matches_topology(finding, status) {
        return Follows::Nothing;
    }
    match finding {
        Finding::Reached { .. } | Finding::Running => Follows::MeshJoin,
        Finding::OsStopped => Follows::Start,
        Finding::Exited { .. } | Finding::Dead { .. } => Follows::Dead,
    }
}

/// [`follows`] for a birth that gave `answer` to its tickle.
pub fn follows_reached(answer: &rdm_core::status::StatusReply, reason: Reason, status: NodeStatus) -> Follows {
    follows(&Finding::Reached { answer: answer.clone(), carrier: None }, reason, status)
}

/// node.runtime.inspect's answer ([`crate::deployment::provider::inspect_runtime`]) as the
/// investigation's finding: its `unavailable` is `dead` once no retry is left ([`ask_again`]).
impl From<crate::deployment::provider::Inspected> for Finding {
    fn from(inspected: crate::deployment::provider::Inspected) -> Self {
        use crate::deployment::provider::Inspected;
        match inspected {
            Inspected::Exited(code) => Finding::Exited { code },
            Inspected::Running => Finding::Running,
            Inspected::OsStopped => Finding::OsStopped,
            Inspected::Unavailable(reason) => Finding::Dead { reason },
        }
    }
}

/// Investigate `node`'s exact birth from `view` over `calls`, the caller's own Node RPC client, call
/// deadline, carriers and leads read (`docs/architecture/lifecycles/node-investigation.md`; Q273,
/// Q393): the index read ([`crate::offline::leads_of`]); then `inspect`, the caller's
/// node.runtime.inspect of the birth's exact runtime, whose `exited` or `os-stopped` is the finding
/// with no tickle sent; otherwise the direct tickle and the same kick through peers, the leads first
/// ([`crate::offline::tickle_with`]). An answer from the exact birth is `reached`; nobody reaching it,
/// the finding is the inspect's: `running` stays `running`, and an `unavailable` answer is asked again
/// under the investigation's retries ([`ask_again`]), `dead` when still not proven at their end. A
/// node the view holds no birth of runs no ladder and is `dead` by name, the provider not asked.
pub async fn investigate<C, I, V>(node: &Node, view: &Topology, calls: &C, inspect: I) -> Finding
where
    C: TickleCalls + ?Sized,
    I: Fn() -> V,
    V: std::future::Future<Output = Finding>,
{
    if node.incarnation_id.is_none() {
        return Finding::Dead { reason: format!("{} holds no recorded birth: there is no exact birth to tickle", node.node_id) };
    }
    let leads = crate::offline::leads_of(node, view, calls).await;
    let inspected = inspect().await;
    if matches!(inspected, Finding::Exited { .. } | Finding::OsStopped) {
        return inspected;
    }
    match crate::offline::tickle_with(node, view, calls, leads).await {
        Err(reason) => Finding::Dead { reason },
        Ok(Tickle::Reached { answer, carrier }) => Finding::Reached { answer, carrier },
        Ok(Tickle::Unreached(unreached)) => {
            unreached.next("verdict");
            drop(unreached);
            match inspected {
                Finding::Dead { .. } => ask_again(&inspect).await,
                proven => proven,
            }
        }
    }
}

/// The investigation's retries of an inspect that answered `unavailable` (node-investigation.md step
/// 4, Q393): `inspect` asked again under one `Send` (its attempts, each within its time limit and each
/// resend after its delay on the one schedule, node-rpc.md §43). The first answer that proves the
/// runtime running, os-stopped or exited is the finding; a birth still not proven when the attempts are
/// spent is `dead`, named by the last answer.
pub async fn ask_again<I, V>(inspect: &I) -> Finding
where
    I: Fn() -> V,
    V: std::future::Future<Output = Finding>,
{
    let asked = rdm_transport::Send::default()
        .run(
            |_: &String| false,
            || {
                let answer = inspect();
                async move {
                    match answer.await {
                        Finding::Dead { reason } => Err(reason),
                        proven => Ok(proven),
                    }
                }
            },
        )
        .await;
    match asked {
        Ok(proven) => proven,
        Err(end) => Finding::Dead { reason: format!("not proven by the end of the investigation's retries: {end}") },
    }
}

/// The runtime recorded at launch of one exact birth, as node.runtime.inspect asks about it
/// (node-runtime-inspect.md step 1: "the birth's launch record").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BirthRuntime {
    /// The exact runtime: provider, control domain and locator.
    pub fact: rdm_core::RuntimeFact,
    /// The data dir the birth reported, where a process runtime records its exit code.
    pub data_dir: Option<String>,
    /// The birth the runtime is of.
    pub incarnation: crate::model::IncarnationId,
}

/// The runtime fact of a birth, one way for every inspect: the one this admin holds of its exact birth
/// `incarnation` (its join report or a topology read: `held`), else the one its node record holds (the
/// node wrote it at its start). `None` when neither holds one. Gossip never carries it (Q387).
pub fn runtime_of(incarnation: &crate::model::IncarnationId, held: Option<&rdm_core::NodeRuntime>, row: Option<&crate::storage::NodeRecord>) -> Option<BirthRuntime> {
    match (held, row) {
        (Some(h), _) if h.runtime.is_some() => Some(BirthRuntime { fact: h.runtime.clone().expect("checked"), data_dir: h.data_dir.clone(), incarnation: incarnation.clone() }),
        (_, Some(r)) => Some(BirthRuntime { fact: r.runtime.clone(), data_dir: r.storage.data_dir.clone(), incarnation: r.incarnation.clone() }),
        _ => None,
    }
}

/// The runtime recorded at launch of `n`'s exact birth (node-recovery.md step 3: "the exact runtime
/// recorded at launch"), the one resolution every inspect copy makes: the runtime `digest_view` holds of
/// that exact birth (its join report, a topology read or this admin's own launch) when it holds a runtime
/// fact, else the runtime on the node's record in `nodes`, a point read ([`runtime_of`]). A birth whose
/// join report this admin never took (another mesh's, or one lost before it was reported here) has its
/// record, which holds the runtime the node wrote at its start. `Err` names what was missing.
pub async fn birth_runtime(digest_view: &rdm_gossip::digest_view::DigestView, nodes: Option<&dyn crate::storage::NodesStorage>, n: &Node) -> Result<BirthRuntime, String> {
    let Some(incarnation) = n.incarnation_id.clone() else { return Err(format!("{} holds no recorded birth: there is no exact runtime to inspect", n.node_id)) };
    // The runtime this admin holds of the exact birth (its join report or a topology read), else the
    // node record's, read at this moment (a runtime read: Q203, Q227 bar only create, start and join
    // reads): what the fabric-primary inspects another mesh's birth with.
    let held = digest_view.runtime_of(&n.node_id, &incarnation);
    let row = match (held.as_ref().is_some_and(|r| r.runtime.is_some()), nodes) {
        (false, Some(nodes)) => crate::node_rows::birth_record(nodes, &n.node_id, &incarnation).await.map_err(|e| format!("the node record of {} at {} could not be read: {e}", n.node_id, incarnation.as_str()))?,
        _ => None,
    };
    runtime_of(&incarnation, held.as_ref(), row.as_ref()).ok_or_else(|| "this admin holds no runtime fact of this exact birth and no node record of it, so no runtime fact".to_string())
}

/// node.runtime.inspect of `n`'s exact birth: [`birth_runtime`], then the one inspect
/// ([`crate::deployment::provider::inspect_runtime`]); beside its answer, the exact runtime handle
/// the provider was asked about (node-runtime-inspect.md "Target"; the inspect span's `handle`),
/// `None` when no fact was held or the provider refused to adopt it, so nothing was asked.
pub async fn inspect_birth_with_handle(provider: &dyn crate::deployment::provider::DeploymentProvider, digest_view: &rdm_gossip::digest_view::DigestView, nodes: Option<&dyn crate::storage::NodesStorage>, n: &Node) -> (crate::deployment::provider::Inspected, Option<crate::deployment::provider::DeploymentHandle>) {
    match birth_runtime(digest_view, nodes, n).await {
        Err(why) => (crate::deployment::provider::Inspected::Unavailable(why), None),
        Ok(b) => {
            let handle = crate::deployment::provider::adopt(provider, &b.fact).ok();
            (crate::deployment::provider::inspect_runtime(provider, &b.fact, b.data_dir.as_deref(), b.incarnation.as_str()).await, handle)
        }
    }
}

/// The fabric and the mesh `node` is in, as `view` holds them: what `on_node_investigated` names
/// besides the birth. The mesh's id is `None` while the view holds none for it.
pub(crate) fn fabric_and_mesh_of(view: &Topology, node: &Node) -> (rdm_core::FabricId, Option<rdm_core::MeshId>) {
    (view.fabric.id.clone(), view.meshes.iter().find(|m| m.name == node.mesh).and_then(|m| m.id.clone()))
}

/// What the primary that investigated a node writes its result with (node-investigation.md, build.md
/// §5): the events store, this admin's exact birth (each event's `source`), its rafka-time, and the
/// application's `on_node_investigated`, sent from the event's write (fabric-node-hooks.md, Q312).
/// One per admin, shared by every place an investigation of it ends: the recovery tick, the self-heal's
/// command back or start, the recovery and the primary watch.
#[derive(Clone)]
pub struct NodeInvestigatedWriter {
    /// What the result is written with: `events.storage`, this admin's exact birth (the event's
    /// `source`) and its rafka-time (the event's `time`).
    pub events: crate::events_storage::EventsWriter,
    /// The application's `on_node_investigated`; `None` when it registers none.
    pub on_node_investigated: Option<crate::wiring::NodeInvestigatedNotice>,
    /// The births whose investigation began here ([`Self::begin`]) and has not ended: each ends once
    /// ([`Self::end`]), so its result is written at most once.
    pub open: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<(NodeId, crate::model::IncarnationId)>>>,
}

impl NodeInvestigatedWriter {
    /// A writer on an events store held in memory, by the birth `node_id` of `fabric_id`, at
    /// rafka-time `1`, with no hook: what a rig with no topology storage writes its results with.
    pub fn in_memory(fabric_id: &rdm_core::FabricId, node_id: &NodeId) -> Self {
        Self { events: crate::events_storage::EventsWriter::in_memory(fabric_id, node_id), on_node_investigated: None, open: Default::default() }
    }

    /// An investigation of the birth `node_id` at `incarnation` began here: the recovery tick started its
    /// ladder, or a primary above it started its look. Its one result is written once ([`Self::end`]); an
    /// investigation already open for that birth stays the one.
    pub(crate) fn begin(&self, node_id: &NodeId, incarnation: &crate::model::IncarnationId) {
        self.open.lock().unwrap().insert((node_id.clone(), incarnation.clone()));
    }

    /// The investigation of `node`'s birth at `incarnation`, run for `reason`, ends here with `result`
    /// and `inspected`, node.runtime.inspect's answer that led to it (`None` for `ready`), the one place
    /// a result is written (node-investigation.md: "The result is the one decision"; Q420-Q423). Open,
    /// it is written as its one event ([`write_node_investigated`]) and is open no more; `result`
    /// `None` ends it with no result: an ordered investigation whose answer matches the birth's status
    /// in the topology (Q393). Not open (its result already written, or no investigation of it began
    /// here), nothing is written, named on the span current here. Answers whether it was open.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn end(&self, ids: (rdm_core::FabricId, Option<rdm_core::MeshId>), node: &Node, incarnation: &crate::model::IncarnationId, result: Option<crate::wiring::NodeInvestigatedResult>, inspected: Option<&crate::deployment::provider::Inspected>, reason: Reason) -> bool {
        let open = self.open.lock().unwrap().remove(&(node.node_id.clone(), incarnation.clone()));
        if !open {
            tracing::info!(node_id = %node.node_id, incarnation_id = %incarnation.as_str(), result = result.map(|r| r.as_str()).unwrap_or("none"), "no investigation of the birth is open here: its result is written already, or none began; nothing is written");
            return false;
        }
        if let Some(result) = result {
            write_node_investigated(self, ids, node, incarnation, result, inspected, reason).await;
        }
        true
    }
}

/// The result `result` of the investigation of `node`'s birth at `incarnation`, with `inspected`,
/// node.runtime.inspect's answer that led to it (`None` for `ready`, which the birth's answer to a
/// tickle led to), for the investigation's `reason` (Q393, Q420–Q423): written by this primary, the one
/// that investigated, as one `rdm.node.investigated` event (node-investigation.md, build.md §5), before
/// anything acts on it; from that write the application's `on_node_investigated` is sent, once, on a
/// task of its own, naming the birth's node type beside it (fabric-node-hooks.md, Q312). Both under
/// `rdm.node_admin.node.update.via-on-node-investigated`, a child of the span current here, which names
/// the event; the event and the notice carry the current span's trace context. `ids` are the fabric's
/// and the birth's mesh's ([`fabric_and_mesh_of`]). The notice gates nothing and orders nothing; an
/// event not written sends none, named on the span.
#[allow(clippy::too_many_arguments)]
async fn write_node_investigated(writer: &NodeInvestigatedWriter, ids: (rdm_core::FabricId, Option<rdm_core::MeshId>), node: &Node, incarnation: &crate::model::IncarnationId, result: crate::wiring::NodeInvestigatedResult, inspected: Option<&crate::deployment::provider::Inspected>, reason: Reason) {
    use tracing::Instrument as _;
    let context = crate::build::current_context();
    let inspected_name = inspected.map(crate::deployment::provider::Inspected::name);
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-on-node-investigated",
        node_path = %node.path, node_name = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        mesh = %node.mesh,
        node_type = %node.r#type,
        result = result.as_str(),
        inspected = inspected_name.unwrap_or_default(),
        reason = reason.as_str(),
        trigger = reason.trigger(),
        event_id = tracing::field::Empty,
        outcome = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let Some(time_ms) = writer.events.time.try_now_ms() else {
        span.record("outcome", "no-rafka-time");
        span.in_scope(|| tracing::info!("this admin holds no rafka-time: the result is not written and the notice is not sent"));
        return;
    };
    let event = crate::events_storage::Event::node_investigated(&writer.events.source, time_ms, &node.node_id, incarnation, &node.path, &node.name, result.as_str(), inspected_name, reason);
    span.record("event_id", event.id.as_str());
    if let Err(e) = writer.events.events.append(&event).instrument(span.clone()).await {
        span.record("outcome", "event-not-written");
        span.in_scope(|| tracing::info!(cause = %e, "the result's rdm.node.investigated event was not written: the notice is not sent"));
        return;
    }
    let (hook, ctx) = match (writer.on_node_investigated.clone(), investigated_ctx(ids, node, incarnation, result, inspected, context)) {
        (Some(hook), Some(ctx)) => (hook, ctx),
        (None, _) => {
            span.record("outcome", "no-hook");
            span.in_scope(|| tracing::info!("the result is written; no on_node_investigated hook is registered"));
            return;
        }
        (Some(_), None) => {
            span.record("outcome", "no-mesh-id");
            span.in_scope(|| tracing::info!("the result is written; this admin's view holds no id for the birth's mesh: the notice is not sent"));
            return;
        }
    };
    span.record("outcome", "sent");
    tokio::spawn(
        async move {
            hook(ctx).await;
            tracing::info!("the application was told the node investigation's result");
        }
        .instrument(span),
    );
}

/// What `on_node_investigated` and `pre_node_recreate` are told of the investigation of `node`'s birth
/// at `incarnation` (fabric-node-hooks.md "on_node_investigated", "pre_node_recreate"; Q312, Q416): the
/// birth and its node type, the fabric's and its mesh's ids `ids` ([`fabric_and_mesh_of`]), the
/// investigation's `result`, `inspected`, node.runtime.inspect's answer that led to it (`None` for
/// `ready`), and the observability `context`. `None` while the view holds no id for the birth's mesh.
pub(crate) fn investigated_ctx(ids: (rdm_core::FabricId, Option<rdm_core::MeshId>), node: &Node, incarnation: &crate::model::IncarnationId, result: crate::wiring::NodeInvestigatedResult, inspected: Option<&crate::deployment::provider::Inspected>, context: rdm_core::context::CallContext) -> Option<crate::wiring::NodeInvestigatedCtx> {
    let (fabric_id, mesh_id) = ids;
    Some(crate::wiring::NodeInvestigatedCtx { fabric_id, mesh_id: mesh_id?, node_id: node.node_id.clone(), incarnation: incarnation.clone(), path: node.path.clone(), name: node.name.clone(), r#type: node.r#type.clone(), result, inspected: inspected.cloned(), context })
}

/// The application's `pre_node_recreate` (`hook`, `None` when it registers none), asked by the primary
/// running the recovery of `node`'s birth at `incarnation` on `inspected`, node.runtime.inspect's
/// answer that led to it, the last step before node.delete of the birth and node.create at its path
/// and name (fabric-node-hooks.md "pre_node_recreate", node-recovery.md sequence step 1, Q416). It is
/// told the [`investigated_ctx`]
/// `on_node_investigated` is told, its `context` taken inside
/// `rdm.node_admin.node.update.via-pre-node-recreate`, a child of the span current here, so the
/// application's work is that span's child. One call, bounded by [`rdm_transport::Send::default`]'s
/// time limit and never sent again: the next recovery tick is the call sent again (Q344).
/// `Ok(())`: the recovery runs on. `Err(reason)`, the recovery's failure, named: the application's
/// `Failed { reason }`; a call still running at the time limit, cancelled (its future dropped); a panic; or a
/// view that holds no id for the birth's mesh, the hook's own outcome, the application not asked. No
/// hook registered is `Ok`.
pub(crate) async fn ask_pre_node_recreate(hook: Option<&crate::wiring::PreNodeRecreate>, ids: (rdm_core::FabricId, Option<rdm_core::MeshId>), node: &Node, incarnation: &crate::model::IncarnationId, inspected: &crate::deployment::provider::Inspected) -> Result<(), String> {
    use futures_util::FutureExt as _;
    use tracing::Instrument as _;
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-pre-node-recreate",
        node_path = %node.path, node_name = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        inspected = inspected.name(),
        outcome = tracing::field::Empty,
        reason = tracing::field::Empty,
        elapsed_ms = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let started = tokio::time::Instant::now();
    let Some(hook) = hook else {
        span.record("outcome", "none-registered");
        span.record("elapsed_ms", 0u64);
        span.in_scope(|| tracing::info!("no pre_node_recreate hook is registered: the recovery runs on"));
        return Ok(());
    };
    // The application's context is taken inside the hook's span: its work parents under it.
    let context = span.in_scope(crate::build::current_context);
    let limit = rdm_transport::Send::default().time_limit.0;
    // Short of `Ok`: the span's `reason`, the recovery's failure, and what the span says of it.
    let ended: Result<(), (String, String, &'static str)> = match investigated_ctx(ids, node, incarnation, crate::wiring::NodeInvestigatedResult::ReCreated, Some(inspected), context) {
        // The hook's own outcome, never the application's answer: it is not asked. The recovery stays
        // owed, and the next tick asks again once the view holds the mesh's id.
        None => {
            let reason = "this admin's view holds no id for the birth's mesh: the application is not asked".to_string();
            Err((reason.clone(), format!("pre_node_recreate: {reason}"), "the application is not asked: the recovery ends failed, still owed, and the next recovery tick asks again"))
        }
        Some(ctx) => {
            let call = std::panic::AssertUnwindSafe(async move { hook(ctx).await }).catch_unwind();
            match tokio::time::timeout(limit, call).instrument(span.clone()).await {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(reason))) => Err((reason.clone(), format!("pre_node_recreate answered Failed: {reason}"), "pre_node_recreate answered Failed: the recovery ends failed, still owed, and nothing is deleted or created")),
                Ok(Err(panic)) => {
                    let what = panic.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| panic.downcast_ref::<String>().cloned()).unwrap_or_default();
                    let reason = format!("pre_node_recreate panicked: {what}");
                    Err((reason.clone(), reason, "pre_node_recreate panicked: the recovery ends failed, still owed, and nothing is deleted or created"))
                }
                Err(_) => {
                    let reason = format!("time limit: the application's call was cancelled after {} ms", limit.as_millis());
                    Err((reason.clone(), format!("pre_node_recreate: {reason}"), "pre_node_recreate did not answer within its time limit: the call is cancelled, the recovery ends failed, still owed, and nothing is deleted or created"))
                }
            }
        }
    };
    span.record("elapsed_ms", started.elapsed().as_millis() as u64);
    match ended {
        Ok(()) => {
            span.record("outcome", "ok");
            span.in_scope(|| tracing::info!("pre_node_recreate answered Ok: the recovery runs on"));
            Ok(())
        }
        Err((reason, failed, said)) => {
            span.record("outcome", "failed");
            span.record("reason", reason.as_str());
            span.in_scope(|| tracing::info!("{said}"));
            Err(failed)
        }
    }
}

/// node.runtime.inspect's span (node-runtime-inspect.md "Observability contract"), the one every
/// caller's inspection emits: `inspector` the caller's node ID, `handle` the exact runtime handle the
/// provider was asked about (empty when nothing was asked), `verdict` the provider's answer and `why`
/// the event naming what the caller asked it for.
pub fn span_inspection(inspector: &str, node: &Node, outcome: &crate::deployment::provider::Inspected, handle: Option<&crate::deployment::provider::DeploymentHandle>, why: &str) {
    use crate::deployment::provider::Inspected;
    let (exit_code, reason) = match outcome {
        Inspected::Exited(code) => (code.map(|c| c.to_string()).unwrap_or_default(), String::new()),
        Inspected::Running | Inspected::OsStopped => (String::new(), String::new()),
        Inspected::Unavailable(why) => (String::new(), why.clone()),
    };
    let incarnation = node.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
    let handle = handle.map(|h| format!("{h:?}")).unwrap_or_default();
    tracing::info_span!("rdm.node_admin.node.resolve.via-provider-inspection", node_path = %node.path, node_name = %node.name, node_id = %node.node_id, incarnation_id = %incarnation, inspector = %inspector, handle = %handle, verdict = outcome.name(), exit_code, reason = %reason)
        .in_scope(|| tracing::info!("{why}"));
}

/// [`inspect_birth_with_handle`]'s answer as the investigation's verdict: the provider's inspection of
/// the exact runtime of `n`'s birth, `dead` by name when no runtime fact is held.
pub async fn inspect_birth(provider: &dyn crate::deployment::provider::DeploymentProvider, digest_view: &rdm_gossip::digest_view::DigestView, nodes: Option<&dyn crate::storage::NodesStorage>, n: &Node) -> Finding {
    inspect_birth_with_handle(provider, digest_view, nodes, n).await.0.into()
}

/// Step a's leads of `node`'s exact birth, from `rows`, the store's by-destination index read for its
/// node id (connections.md §3.1): the sources of the rows that hold a connection to that birth, each
/// resolved to a member of `view` in any mesh that is ready, never `me` (the caller is
/// never its own carrier) and never the node itself; one per node, in path order.
///
/// Taken: the rows connections.md §3.2's carrier discovery reads, `Direct` and `Connected`
/// (`rtable(destination).filter(state=active).filter(kind=direct)`), whose destination incarnation is
/// the probed one. Not taken: a `Disconnected`, `Failed` or `Refused` row (its source said it holds
/// no connection); a row naming another birth of the node, or none; a `Proxy` row, whose source
/// reaches the node only through its carrier while a peer's Forward is one direct inner call (the
/// carrier's own `Direct` row names it); and a row an earlier birth of its source wrote.
pub fn leads_of(rows: &[NodeConnection], view: &Topology, node: &Node, me: Option<&NodeId>) -> Vec<Node> {
    let Some(incarnation) = node.incarnation_id.as_ref() else { return Vec::new() };
    let mut leads: Vec<Node> = Vec::new();
    for row in rows {
        if row.kind != ConnectionKind::Direct || row.state != ConnectionState::Connected {
            continue;
        }
        if row.destination.node_id != node.node_id || row.destination.incarnation.as_ref() != Some(incarnation) {
            continue;
        }
        let Some(source) = view.births().find(|m| m.node_id == row.source.node_id) else { continue };
        let same_birth = row.source.incarnation.as_ref().is_none_or(|i| source.incarnation_id.as_ref() == Some(i));
        if !same_birth || source.node_id == node.node_id || Some(&source.node_id) == me || source.status != NodeStatus::Ready {
            continue;
        }
        if !leads.iter().any(|l| l.node_id == source.node_id) {
            leads.push(source.clone());
        }
    }
    leads.sort_by_key(|l| l.location());
    leads
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, IncarnationId, ProviderKind, ScopeStatus};
    use crate::offline::ProbeEnd;
    use rdm_core::connections::ConnectionEnd;
    use rdm_core::status::{NodeState, StatusReply, StatusRequest};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    /// CONTRACT (fabric-node-hooks.md "pre_node_recreate": "There is no `Pending`: the application's work
    /// is one call under the hook's `Send`, whose time limit covers it"; "Observability": "A cancelled
    /// hook names the cancellation"; Q416): the mesh-primary asking the application's
    /// `pre_node_recreate` before the re-create of the lost birth mesh1.node.1 reads as the recovery's
    /// failure, each named on the hook's span: the application's `Failed`; a hook still running at the
    /// time limit of `rdm_transport::Send::default()`, its call cancelled there (its future dropped) and
    /// never sent again, the next recovery tick asking again; a hook that panics; and a view that holds
    /// no id for the birth's mesh, the hook's own outcome, the application not asked. It reads the hook's
    /// `Ok` as `Ok`, the application told a context taken inside the hook's span. Must NOT happen: a
    /// recovery run on, or held for good, by a hook that never answers or panics; a missing mesh id
    /// named as the application's answer.
    // @feature: node-lifecycle
    #[tokio::test(start_paused = true)]
    async fn pre_node_recreate_unanswered_within_its_time_limit_or_panicking_is_failed_by_name() {
        /// Set when the silent hook's call is dropped: its cancellation.
        struct Cancelled(std::sync::Arc<AtomicBool>);
        impl Drop for Cancelled {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let (node, _) = world();
        let incarnation = node.incarnation_id.clone().unwrap();
        let ids = || (FabricId::mint(), Some(rdm_core::MeshId::mint()));
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let told: std::sync::Arc<Mutex<Vec<crate::wiring::NodeInvestigatedCtx>>> = std::sync::Arc::default();
        let hook = |answer: fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>| -> crate::wiring::PreNodeRecreate {
            let (calls, told) = (calls.clone(), told.clone());
            std::sync::Arc::new(move |ctx| {
                calls.fetch_add(1, Ordering::SeqCst);
                told.lock().unwrap().push(ctx);
                answer()
            })
        };
        let cancelled = std::sync::Arc::new(AtomicBool::new(false));
        let silent: crate::wiring::PreNodeRecreate = {
            let (calls, cancelled) = (calls.clone(), cancelled.clone());
            std::sync::Arc::new(move |_ctx| {
                calls.fetch_add(1, Ordering::SeqCst);
                let guard = Cancelled(cancelled.clone());
                Box::pin(async move {
                    let _guard = guard;
                    std::future::pending::<Result<(), String>>().await
                })
            })
        };
        let panics = hook(|| panic!("the application's release panicked"));
        let ok = hook(|| Box::pin(async { Ok(()) }));
        let capture = crate::span_capture::Capture::new("q416-pre-node-recreate-time-limit");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let exited = &crate::deployment::provider::Inspected::Exited(None);
        let limit = rdm_transport::Send::default().time_limit.0;
        let cancellation = format!("time limit: the application's call was cancelled after {} ms", limit.as_millis());
        let started = tokio::time::Instant::now();
        assert_eq!(ask_pre_node_recreate(Some(&silent), ids(), &node, &incarnation, exited).await, Err(format!("pre_node_recreate: {cancellation}")), "a call still running at the time limit is Failed, its cancellation named");
        assert_eq!(started.elapsed(), limit, "one call, bounded by the time limit of Send::default(), never sent again");
        assert!(cancelled.load(Ordering::SeqCst), "the call is cancelled at the time limit: its future is dropped");
        let panicked = ask_pre_node_recreate(Some(&panics), ids(), &node, &incarnation, exited).await;
        assert_eq!(panicked, Err("pre_node_recreate panicked: the application's release panicked".to_string()), "a panic is Failed, named");
        let not_asked = "this admin's view holds no id for the birth's mesh: the application is not asked".to_string();
        let unnamed = ask_pre_node_recreate(Some(&ok), (FabricId::mint(), None), &node, &incarnation, exited).await;
        assert_eq!(unnamed, Err(format!("pre_node_recreate: {not_asked}")), "no mesh id is the hook's own failure, never the application's answer");
        assert_eq!(ask_pre_node_recreate(Some(&ok), ids(), &node, &incarnation, exited).await, Ok(()));
        assert_eq!(calls.load(Ordering::SeqCst), 3, "each hook is called once, and none is called without the mesh's id");
        let asks = capture.named("rdm.node_admin.node.update.via-pre-node-recreate");
        let seen: Vec<(String, String)> = asks.iter().map(|s| (s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("reason").cloned().unwrap_or_default())).collect();
        assert_eq!(
            seen[..3],
            [("failed".to_string(), cancellation.clone()), ("failed".to_string(), "pre_node_recreate panicked: the application's release panicked".to_string()), ("failed".to_string(), not_asked.clone())],
            "{asks:?}"
        );
        assert_eq!(asks[0].attributes.get("elapsed_ms").map(String::as_str), Some("10000"), "{:?}", asks[0].attributes);
        assert_eq!(seen[3].0, "ok", "{asks:?}");
        let told = told.lock().unwrap().clone();
        let parent = told.last().and_then(|t| t.context.traceparent.clone()).unwrap_or_default();
        assert_eq!(parent.split('-').nth(2), Some(asks[3].span_id.as_str()), "the application's context is the hook's span: {parent} vs {:?}", asks[3]);
        eprintln!("span {} trace_id={} {:?}", asks[0].name, asks[0].trace_id, asks[0].attributes);
        eprintln!("span {} trace_id={} {:?}", asks[2].name, asks[2].trace_id, asks[2].attributes);
    }

    fn ready(name: rdm_core::PathName, mesh: &str, r#type: &str) -> Node {
        let mut n = Node::allocated(name, mesh, r#type);
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::Ready;
        n
    }

    /// The silent `mesh1.node.1`, its mesh's ready members, this admin, and a ready node of mesh2.
    fn world() -> (Node, Topology) {
        let mut node = ready(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node");
        node.status = NodeStatus::PendingReconnect;
        let nodes = vec![ready(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), node.clone(), ready(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), ready(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"), ready(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node"), ready(rdm_core::PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node")];
        (node, Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![], nodes })
    }

    fn of(view: &Topology, name: rdm_core::PathName) -> Node {
        view.births().find(|n| n.location() == name).cloned().unwrap()
    }

    fn end(n: &Node) -> ConnectionEnd {
        ConnectionEnd { mesh: n.mesh.clone(), r#type: n.r#type.clone(), path: n.path.clone(), name: n.name.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone() }
    }

    fn row(source: &Node, destination: &Node, kind: ConnectionKind, state: ConnectionState) -> NodeConnection {
        NodeConnection { id: rdm_core::ConnectionId::mint(), source: end(source), destination: end(destination), kind, alpn: rdm_core::connections::ALPN.to_vec(), state, carrier: None, reason: None, logged_at_ms: 1 }
    }

    fn current(n: &Node) -> StatusReply {
        StatusReply::Current { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), state: NodeState::Ready }
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:8
    /// "1. Read the by-destination connections index ([connections.md §3.1](../connections.md#31-declared-indexes)).
    ///    It names the peers to try; it never proves the node is reachable."
    /// (Q107 item 1 step a; connections.md §3.2, carrier discovery
    /// `rtable(destination).filter(state=active).filter(kind=direct)`): the leads are the sources, in
    /// any mesh, of the `Direct` `Connected` rows to the exact birth, resolved to ready members of the
    /// view, in path order, one per node. Must NOT be a lead: a row marked dropped or failed, a row to
    /// another birth of the node, a `Proxy` row, a row an earlier birth of its source wrote, a source
    /// the view does not hold ready, the caller, or the node itself.
    #[test]
    fn the_leads_are_the_ready_sources_in_any_mesh_of_the_active_direct_rows_to_the_exact_birth() {
        let (node, view) = world();
        let me = of(&view, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap());
        let (r2, r3, m1, m2) = (of(&view, rdm_core::PathName::new("mesh1.node", "2").unwrap()), of(&view, rdm_core::PathName::new("mesh1.node", "3").unwrap()), of(&view, rdm_core::PathName::new("mesh2.node", "1").unwrap()), of(&view, rdm_core::PathName::new("mesh2.node", "2").unwrap()));
        let other_birth = Node { incarnation_id: Some(IncarnationId::mint()), ..node.clone() };
        let earlier_source = Node { incarnation_id: Some(IncarnationId::mint()), ..r3.clone() };
        let unheld = ready(rdm_core::PathName::new("mesh3.node", "1").unwrap(), "mesh3", "node");
        let rows = vec![
            row(&m1, &node, ConnectionKind::Direct, ConnectionState::Connected),
            row(&r2, &node, ConnectionKind::Direct, ConnectionState::Connected),
            NodeConnection { alpn: b"app/1".to_vec(), ..row(&r2, &node, ConnectionKind::Direct, ConnectionState::Connected) },
            row(&m2, &node, ConnectionKind::Direct, ConnectionState::Disconnected),
            row(&m2, &node, ConnectionKind::Proxy, ConnectionState::Connected),
            row(&r3, &other_birth, ConnectionKind::Direct, ConnectionState::Connected),
            row(&earlier_source, &node, ConnectionKind::Direct, ConnectionState::Connected),
            row(&unheld, &node, ConnectionKind::Direct, ConnectionState::Connected),
            row(&me, &node, ConnectionKind::Direct, ConnectionState::Connected),
            row(&node, &node, ConnectionKind::Direct, ConnectionState::Connected),
        ];
        let names = |v: Vec<Node>| v.into_iter().map(|n| n.location()).collect::<Vec<_>>();
        assert_eq!(names(leads_of(&rows, &view, &node, Some(&me.node_id))), [rdm_core::PathName::new("mesh1.node", "2").unwrap(), rdm_core::PathName::new("mesh2.node", "1").unwrap()], "the active Direct sources in any mesh, once each, in path order");
        let mut view = view;
        view.nodes.iter_mut().find(|n| n.node_id == m1.node_id).unwrap().status = NodeStatus::PendingReconnect;
        assert_eq!(names(leads_of(&rows, &view, &node, Some(&me.node_id))), [rdm_core::PathName::new("mesh1.node", "2").unwrap()], "a source the view holds off ready carries nothing");
        assert!(leads_of(&rows, &view, &Node { incarnation_id: None, ..node.clone() }, None).is_empty(), "no birth, no leads");
    }

    fn fact(pid: u32) -> rdm_core::RuntimeFact {
        rdm_core::RuntimeFact { deployment_id: rdm_core::fixture_crockford60(&format!("dep-{pid}")), provider: rdm_core::RuntimeProvider::Process, control_domain: "host-7".into(), locator: rdm_core::RuntimeLocator::Process { pid, start: 1 } }
    }

    fn digest(n: &Node, incarnation: IncarnationId) -> rdm_core::MeshDigest {
        rdm_core::MeshDigest {
            fabric_id: FabricId::mint(),
            node: rdm_core::MeshNode { mesh: n.mesh.clone(), r#type: n.r#type.clone(), node_id: n.node_id.clone(), path: n.path.clone(), name: n.name.clone(), endpoint_id: crate::model::EndpointId(String::new()), transport_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)), incarnation, supersedes: None, },
            status: rdm_core::model::NodeStatus::Ready,
            digest_seq: 1,
            emitted_at_rafka_ms: 0,
            primary: Vec::new(),
        }
    }

    /// The node record of `n`'s birth `inc`, with the runtime `fact` and the data dir `dir`.
    fn record(n: &Node, inc: &IncarnationId, fact: rdm_core::RuntimeFact, dir: &str) -> rdm_node::nodes_storage::NodeRecord {
        let mut r = rdm_node::nodes_storage::NodeRecord { runtime: fact, ..rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(n.path.clone(), n.name.clone()).unwrap(), &n.r#type.clone().to_string(), n.node_id.clone(), inc.clone(), rdm_core::model::NodeStatus::Ready) };
        r.storage.data_dir = Some(dir.into());
        r
    }

    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:25
    /// "1. The caller holds the birth's launch record: the handle the provider returned when it launched
    ///     this exact incarnation (`deployment_id`, `provider`, `control_domain`, `locator`)."
    /// CONTRACT (node-runtime-inspect.md step 1: "The caller holds the birth's launch record: the
    /// handle the provider returned when it launched this exact incarnation"; node-recovery.md step 3:
    /// "the exact runtime recorded at launch"; Q387: a runtime is held from the birth's join report or a
    /// topology read, never from gossip): every inspect resolves a birth's runtime one way, the runtime
    /// this admin holds of that exact birth, else the birth's own durable runtime row; a runtime held of
    /// another birth of the node, or a data dir held with no runtime fact, falls to the row, and so does a
    /// digest of the exact birth (it carries no runtime); with neither, nothing is resolved and the reason
    /// names both.
    #[tokio::test]
    async fn a_births_runtime_is_the_one_held_of_it_else_its_durable_row_one_way_for_every_inspect() {
        use crate::storage::NodesStorage as _;
        let (node, _) = world();
        let inc = node.incarnation_id.clone().unwrap();
        let nodes = crate::storage::MemoryNodesStorage::default();
        let empty = rdm_gossip::digest_view::DigestView::default();
        let none = birth_runtime(&empty, Some(&nodes), &node).await.unwrap_err();
        assert!(none.contains("no runtime fact of this exact birth") && none.contains("no node record"), "{none}");
        nodes.put_node(&record(&node, &inc, fact(2), "/data/from-the-row")).await.unwrap();
        let from_row = BirthRuntime { fact: fact(2), data_dir: Some("/data/from-the-row".into()), incarnation: inc.clone() };
        assert_eq!(birth_runtime(&empty, Some(&nodes), &node).await, Ok(from_row.clone()), "nothing held: the node record");
        let held_of_another = rdm_gossip::digest_view::DigestView::default();
        assert!(held_of_another.hold_runtime(&node.node_id, &IncarnationId::mint(), rdm_core::NodeRuntime { runtime: Some(fact(3)), data_dir: None }));
        let no_fact = rdm_gossip::digest_view::DigestView::default();
        assert!(no_fact.hold_runtime(&node.node_id, &inc, rdm_core::NodeRuntime { runtime: None, data_dir: Some("/data/no-fact".into()) }));
        let heard = rdm_gossip::digest_view::DigestView::default();
        assert!(heard.record(digest(&node, inc.clone())));
        for (digest_view, why) in [(held_of_another, "a runtime held of another birth"), (no_fact, "a data dir held with no runtime fact"), (heard, "a digest of the exact birth, which carries no runtime")] {
            assert_eq!(birth_runtime(&digest_view, Some(&nodes), &node).await, Ok(from_row.clone()), "{why}: the node record");
        }
        let digest_view = rdm_gossip::digest_view::DigestView::default();
        assert!(digest_view.hold_runtime(&node.node_id, &inc, rdm_core::NodeRuntime { runtime: Some(fact(1)), data_dir: Some("/data/from-the-join-report".into()) }));
        assert_eq!(birth_runtime(&digest_view, Some(&nodes), &node).await, Ok(BirthRuntime { fact: fact(1), data_dir: Some("/data/from-the-join-report".into()), incarnation: inc }), "the runtime held of the exact birth, as its join report carried it");
    }

    /// CONTRACT (node-runtime-inspect.md step 1; node-recovery.md step 2: "For an unheard mesh-primary the
    /// fabric-primary runs it"; Q387: gossip never carries a runtime): the fabric-primary inspecting a
    /// birth of another mesh holds it from that mesh's aggregate, which carries no runtime, and holds no
    /// join report of it (its own mesh-primary took that): it resolves the birth's runtime from the
    /// birth's node record, read at that moment, and names both when there is none. Must NOT happen: a
    /// cross-mesh inspect refused for a runtime gossip no longer carries.
    #[tokio::test]
    async fn the_fabric_primary_inspects_another_meshs_birth_by_the_runtime_on_its_node_record() {
        use crate::storage::NodesStorage as _;
        let (_, view) = world();
        let other = of(&view, rdm_core::PathName::new("mesh2.node", "1").unwrap());
        let inc = other.incarnation_id.clone().unwrap();
        let digest_view = rdm_gossip::digest_view::DigestView::default();
        // mesh1's view holds mesh2.node.1 as mesh2's aggregate carried it: identity and status only.
        assert!(digest_view.record(digest(&other, inc.clone())));
        assert_eq!(digest_view.runtime_of(&other.node_id, &inc), None, "no runtime of another mesh's birth is held");
        let nodes = crate::storage::MemoryNodesStorage::default();
        let none = birth_runtime(&digest_view, Some(&nodes), &other).await.unwrap_err();
        assert!(none.contains("no runtime fact of this exact birth") && none.contains("no node record"), "{none}");
        nodes.put_node(&record(&other, &inc, fact(5), "/data/mesh2.node.1")).await.unwrap();
        assert_eq!(birth_runtime(&digest_view, Some(&nodes), &other).await, Ok(BirthRuntime { fact: fact(5), data_dir: Some("/data/mesh2.node.1".into()), incarnation: inc }), "the runtime on the birth's node record");
    }

    /// A caller whose direct probe and carried probes answer as the cell says.
    struct Rig {
        direct: Option<StatusReply>,
        carried: Option<(NodeId, StatusReply)>,
        asked: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl TickleCalls for Rig {
        fn caller(&self) -> String {
            "100000000000".into()
        }
        fn deadline(&self) -> std::time::Duration {
            std::time::Duration::from_secs(1)
        }
        async fn direct(&self, _node: &Node, _probe: &StatusRequest) -> ProbeEnd {
            self.asked.lock().unwrap().push("direct".into());
            match &self.direct {
                Some(r) => ProbeEnd::Reply(r.clone()),
                None => ProbeEnd::Other { end: "NotSent", detail: "not sent: Deadline".into() },
            }
        }
        async fn via_peer(&self, carrier: &Node, _node: &Node, _probe: &StatusRequest) -> ProbeEnd {
            self.asked.lock().unwrap().push(carrier.node_id.to_string());
            match &self.carried {
                Some((c, r)) if carrier.node_id == *c => ProbeEnd::Reply(r.clone()),
                _ => ProbeEnd::Other { end: "NotSent", detail: "not sent: Deadline".into() },
            }
        }
        fn carriers(&self, view: &Topology, node: &Node) -> Vec<Node> {
            let me = of(view, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).node_id;
            crate::offline::eligible_carriers(view, node, Some(&me)).cloned().collect()
        }
        async fn leads(&self, _view: &Topology, _node: &Node) -> Result<Vec<Node>, String> {
            Ok(Vec::new())
        }
    }

    async fn provider_says(f: Finding, asked: &AtomicBool) -> Finding {
        asked.store(true, Ordering::SeqCst);
        f
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:5
    /// "node.runtime.inspect's answers (`running`, `os-stopped`, `exited`, `unavailable`) are steps inside
    ///  it, never a result."
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:11
    /// "2. [node.runtime.inspect](node-runtime-inspect.md). On `exited` or `os-stopped` no tickle is sent: an
    ///     `exited` birth is written `dead`; an `os-stopped` one is commanded to start (step 5).
    ///  3. [node.tickle](node-tickle.md), direct.
    ///  4. The same tickle via those peers. Unreached, a `running` birth is commanded back through
    ///     [node.mesh.join](node-mesh-join.md) (step 5); an inspect that answers `unavailable` is asked again
    ///     under the investigation's retries, and a birth still not proven at their end is written `dead`.
    ///  Only an answer from the exact birth counts: to a tickle, the investigation ends `ready`; to the
    ///  command back or the start, `rejoined`."
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:58
    /// "Target: One node, by `(node.id, incarnation)`
    ///  Steps: The by-destination connections index read, node.runtime.inspect, node.tickle direct, the same tickle via those peers"
    /// What the steps find, the [`Finding`] the result follows from: the exact birth's answer is
    /// `reached`, and only the provider proves an exit.
    #[tokio::test(start_paused = true)]
    async fn only_the_exact_births_answer_is_reached_and_only_the_provider_proves_an_exit() {
        let (node, view) = world();
        let rig = |direct: Option<StatusReply>, carried: Option<(NodeId, StatusReply)>| Rig { direct, carried, asked: Mutex::default() };
        // The inspect first (Q273): on exited or os-stopped no tickle is sent.
        for said in [Finding::Exited { code: Some(137) }, Finding::OsStopped] {
            let asked = AtomicBool::new(false);
            let r = rig(Some(current(&node)), Some((of(&view, rdm_core::PathName::new("mesh1.node", "3").unwrap()).node_id, current(&node))));
            assert_eq!(investigate(&node, &view, &r, || provider_says(said.clone(), &asked)).await, said, "the provider proved the runtime gone: its answer is the finding");
            assert!(asked.load(Ordering::SeqCst), "the provider is asked first");
            assert!(r.asked.lock().unwrap().is_empty(), "no tickle is sent once the inspect proved it gone: {:?}", r.asked.lock().unwrap());
        }
        // Running or unavailable: the ladder runs, and only the exact birth's answer is reached; nobody
        // reaching it, the finding is the inspect's (an `unavailable` one asked again under the
        // investigation's retries, `dead` at their end).
        for said in [Finding::Running, Finding::Dead { reason: "Unknown".into() }] {
            let asked = AtomicBool::new(false);
            let found = investigate(&node, &view, &rig(Some(current(&node)), None), || provider_says(said.clone(), &asked)).await;
            assert_eq!(found, Finding::Reached { answer: current(&node), carrier: None });
            let found = investigate(&node, &view, &rig(None, Some((of(&view, rdm_core::PathName::new("mesh1.node", "3").unwrap()).node_id, current(&node)))), || provider_says(said.clone(), &asked)).await;
            assert_eq!(found, Finding::Reached { answer: current(&node), carrier: Some(of(&view, rdm_core::PathName::new("mesh1.node", "3").unwrap()).node_id) });
            assert_eq!((found.name(), found.detail()), ("reached", of(&view, rdm_core::PathName::new("mesh1.node", "3").unwrap()).node_id.to_string()));
            let another_birth = Node { incarnation_id: Some(IncarnationId::mint()), ..node.clone() };
            let r = rig(Some(current(&another_birth)), Some((of(&view, rdm_core::PathName::new("mesh1.node", "2").unwrap()).node_id, current(&another_birth))));
            let found = investigate(&node, &view, &r, || provider_says(said.clone(), &asked)).await;
            match &said {
                Finding::Dead { .. } => assert!(matches!(&found, Finding::Dead { reason } if reason.contains("not proven by the end of the investigation's retries") && reason.contains("Unknown")), "nobody reached the exact birth and the inspect never proved it: dead at the end of the retries, {found:?}"),
                _ => assert_eq!(found, said, "nobody reached the exact birth: the inspect's answer"),
            }
            assert_eq!(r.asked.lock().unwrap().first().map(String::as_str), Some("direct"), "the direct probe first");
        }
        let asked = AtomicBool::new(false);
        let found = investigate(&Node { incarnation_id: None, ..node.clone() }, &view, &rig(None, None), || provider_says(Finding::Running, &asked)).await;
        assert!(matches!(&found, Finding::Dead { reason } if reason.contains("no recorded birth")), "{found:?}");
        assert!(!asked.load(Ordering::SeqCst), "no ladder ran: the provider is not asked");
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:13
    /// "4. The same tickle via those peers. Unreached, a `running` birth is commanded back through
    ///     [node.mesh.join](node-mesh-join.md) (step 5); an inspect that answers `unavailable` is asked again
    ///     under the investigation's retries, and a birth still not proven at their end is written `dead`."
    /// (Q393: "ITS DEAD IF IT CAN'T PROVE IT OR IT GETS TO RUN RETRIES TILL THE END AND THEN ITS DEAD"):
    /// unreached, an `unavailable` inspect is asked again under one `Send` of the default attempts; the
    /// first answer that proves the runtime is the finding, and one never proven is `dead` once the
    /// attempts are spent, named by its last answer. Must NOT happen: `unavailable` taken as the finding
    /// at its first answer, more asks than the step's first plus the Send's attempts, or a retry once
    /// an answer proved the runtime.
    // @feature: node-investigation
    #[tokio::test(start_paused = true)]
    async fn an_unavailable_inspect_is_asked_again_under_the_retries_and_dead_only_at_their_end() {
        let (node, view) = world();
        let unreached = || Rig { direct: None, carried: None, asked: Mutex::default() };
        let attempts = usize::from(rdm_transport::Send::default().attempts);
        // Never proven: the step's ask and each attempt of the Send, then dead.
        let asks = std::sync::atomic::AtomicUsize::new(0);
        let found = investigate(&node, &view, &unreached(), || {
            asks.fetch_add(1, Ordering::SeqCst);
            async { Finding::Dead { reason: "the provider cannot tell its runtime's state: Unknown".into() } }
        })
        .await;
        assert!(matches!(&found, Finding::Dead { reason } if reason.contains("its Send spent") && reason.contains("Unknown")), "{found:?}");
        assert_eq!(found.name(), "dead");
        assert_eq!(asks.load(Ordering::SeqCst), 1 + attempts, "the step's ask, then each attempt of the investigation's retries");
        // Proven on a retry: that answer is the verdict, and nothing is asked after it.
        for proven in [Finding::Exited { code: Some(9) }, Finding::Running, Finding::OsStopped] {
            let asks = std::sync::atomic::AtomicUsize::new(0);
            let found = investigate(&node, &view, &unreached(), || {
                let n = asks.fetch_add(1, Ordering::SeqCst);
                let answer = if n < 2 { Finding::Dead { reason: "Unknown".into() } } else { proven.clone() };
                async move { answer }
            })
            .await;
            assert_eq!(found, proven, "the first answer that proves the runtime is the verdict");
            assert_eq!(asks.load(Ordering::SeqCst), 3, "no ask after the proving one");
        }
    }

    /// CONTRACT (node-recovery.md step 3, Q393; lead note 0001 C2: "`running` matches a live status
    /// (pending, ready) AND a lifecycle state the node reported (connections-deleting,
    /// connections-deleted, stopping, stopped) ... `os-stopped` matches `stopped`. `exited` matches
    /// `dead`. Anything else differs and follows the self-heal."): an ordered investigation's finding
    /// matches the birth's status in the topology exactly in those pairs; `reached`, the birth's own
    /// answer that it runs, matches as `running` does; `dead` (not proven) matches nothing. Must NOT
    /// happen: `running` matching `dead`, `os-stopped` matching `ready`, or `exited` matching a live
    /// status.
    // @feature: node-investigation
    #[test]
    fn an_ordered_verdict_matches_the_topology_only_in_the_ruled_pairs() {
        use NodeStatus as S;
        let all = [S::Starting, S::Pending, S::Ready, S::ConnectionsDeleting, S::ConnectionsDeleted, S::Leaving, S::Stopping, S::Stopped, S::PendingReconnect, S::Dead, S::Deleting, S::Deleted, S::Withdrawn];
        let (node, _) = world();
        let reached = Finding::Reached { answer: current(&node), carrier: None };
        let stopped = Finding::Reached { answer: rdm_core::status::StatusReply::CurrentStopped { node_id: node.node_id.clone(), incarnation: node.incarnation_id.clone().unwrap() }, carrier: None };
        let runs = [S::Starting, S::Pending, S::Ready, S::ConnectionsDeleting, S::ConnectionsDeleted, S::Stopping, S::Stopped];
        for status in all {
            assert_eq!(matches_topology(&Finding::Running, status), runs.contains(&status), "running and {status:?}");
            assert_eq!(matches_topology(&reached, status), runs.contains(&status), "reached and {status:?}");
            // Review finding 2, R3: the birth's own word that it is stopped matches `stopped` only, as
            // os-stopped does (Q425).
            assert_eq!(matches_topology(&stopped, status), status == S::Stopped, "reached answering CurrentStopped and {status:?}");
            assert_eq!(follows(&stopped, Reason::SelfHeal, status), Follows::MeshJoin, "a self-heal's CurrentStopped ends `ready` and is never started (Q425): {status:?}");
            assert_eq!(matches_topology(&Finding::OsStopped, status), status == S::Stopped, "os-stopped and {status:?}");
            assert_eq!(matches_topology(&Finding::Exited { code: None }, status), status == S::Dead, "exited and {status:?}");
            assert!(!matches_topology(&Finding::Dead { reason: String::new() }, status), "dead (not proven) and {status:?}");
        }
        assert_eq!((Reason::SelfHeal.as_str(), Reason::SelfHeal.trigger()), ("self-heal", "silence"));
        assert_eq!((Reason::Ordered { trigger: "report-call-failed" }.as_str(), Reason::Ordered { trigger: "report-call-failed" }.trigger()), ("ordered", "report-call-failed"));
    }

    /// CONTRACT (node-investigation.md: "Only an answer from the exact birth counts: to a tickle, the
    /// investigation ends `ready`"; Q332:
    /// a spent call is the one clock; Q338: the primary watch loses Q182's "an answered tickle is not
    /// reached"): the exact birth's answer, direct or through a carrier, reaches a member and a
    /// node-admin primary alike. Must NOT happen: an answer from the exact birth held unreached.
    // @feature: node-investigation
    #[tokio::test]
    async fn the_exact_births_answer_reaches_a_member_and_a_primary_alike() {
        let (node, view) = world();
        let carrier = of(&view, rdm_core::PathName::new("mesh1.node", "3").unwrap()).node_id;
        let primary = Node { r#type: "node-admin".into(), is_primary: true, ..node.clone() };
        for (direct, carried) in [(Some(current(&node)), None), (None, Some((carrier.clone(), current(&node))))] {
            for (looked_at, what) in [(&node, "a member"), (&primary, "a node-admin primary")] {
                let rig = Rig { direct: direct.clone(), carried: carried.clone(), asked: Mutex::default() };
                let asked = AtomicBool::new(false);
                let found = investigate(looked_at, &view, &rig, || provider_says(Finding::Running, &asked)).await;
                assert!(matches!(found, Finding::Reached { .. }), "{what} is reached by its answer: {found:?}");
            }
        }
    }

    // @feature: node-investigation
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:9
    /// "1. Read the by-destination connections index ([connections.md §3.1](../connections.md#31-declared-indexes)).
    ///     It names the peers to try; it never proves the node is reachable.
    ///  2. [node.runtime.inspect](node-runtime-inspect.md). On `exited` or `os-stopped` no tickle is sent: an
    ///     `exited` birth is written `dead`; an `os-stopped` one is commanded to start (step 5).
    ///  3. [node.tickle](node-tickle.md), direct.
    ///  4. The same tickle via those peers. Unreached, a `running` birth is commanded back through
    ///     [node.mesh.join](node-mesh-join.md) (step 5); an inspect that answers `unavailable` is asked again
    ///     under the investigation's retries, and a birth still not proven at their end is written `dead`."
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:62
    /// "Spans: [node.tickle](node-tickle.md#operation-contract)'s and [node.runtime.inspect](node-runtime-inspect.md#operation-contract)'s own"
    #[tokio::test]
    async fn investigate_orders_leads_before_mesh_carriers_and_emits_provider_inspection_span_on_unreached()
    {
        struct LeadsRig {
            rows: Vec<NodeConnection>,
            asked: Mutex<Vec<String>>,
        }

        #[async_trait::async_trait]
        impl TickleCalls for LeadsRig {
            fn caller(&self) -> String {
                "100000000000".into()
            }
            fn deadline(&self) -> std::time::Duration {
                std::time::Duration::from_secs(1)
            }
            async fn direct(&self, _node: &Node, _probe: &StatusRequest) -> ProbeEnd {
                self.asked.lock().unwrap().push("direct".into());
                ProbeEnd::Other {
                    end: "NotSent",
                    detail: "not sent: Deadline".into(),
                }
            }
            async fn via_peer(
                &self,
                carrier: &Node,
                _node: &Node,
                _probe: &StatusRequest,
            ) -> ProbeEnd {
                self.asked.lock().unwrap().push(carrier.node_id.to_string());
                ProbeEnd::Other {
                    end: "NotSent",
                    detail: "not sent: Deadline".into(),
                }
            }
            fn carriers(&self, view: &Topology, node: &Node) -> Vec<Node> {
                let me = of(view, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).node_id;
                crate::offline::eligible_carriers(view, node, Some(&me))
                    .cloned()
                    .collect()
            }
            async fn leads(&self, view: &Topology, node: &Node) -> Result<Vec<Node>, String> {
                let me = of(view, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).node_id;
                Ok(leads_of(&self.rows, view, node, Some(&me)))
            }
        }

        let (node, view) = world();
        let m1 = of(&view, rdm_core::PathName::new("mesh2.node", "1").unwrap());
        let r3 = of(&view, rdm_core::PathName::new("mesh1.node", "3").unwrap());
        let rows = vec![
            row(&m1, &node, ConnectionKind::Direct, ConnectionState::Connected),
            row(&r3, &node, ConnectionKind::Direct, ConnectionState::Connected),
        ];
        let rig = LeadsRig {
            rows,
            asked: Mutex::default(),
        };

        let capture = crate::span_capture::Capture::new("node-investigation-unit-order");
        let found = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            let node_for_inspect = node.clone();
            investigate(&node, &view, &rig, || async {
                let outcome = crate::deployment::provider::Inspected::Running;
                span_inspection(
                    "mesh1.node-admin.1",
                    &node_for_inspect,
                    &outcome,
                    None,
                    "node-investigation",
                );
                outcome.into()
            })
            .await
        };

        assert_eq!(found, Finding::Running, "nobody reached it: the inspect's answer");
        assert_eq!(
            *rig.asked.lock().unwrap(),
            vec![
                "direct".to_string(),
                r3.node_id.to_string(),
                m1.node_id.to_string()
            ],
            "direct probe first, then the two leads in path order before non-lead mesh1.node.2"
        );

        let direct_spans = capture.named("rdm.node.tickle.resolve.via-direct");
        assert_eq!(direct_spans.len(), 1);
        assert_eq!(
            direct_spans[0].attributes.get("leads").map(String::as_str),
            Some(format!("{},{}", r3.node_id, m1.node_id).as_str()),
            "the leads, in path order, by node id"
        );

        let peer_spans = capture.named("rdm.node.tickle.resolve.via-peer");
        assert_eq!(peer_spans.len(), 2);
        assert!(peer_spans
            .iter()
            .all(|s| s.attributes.get("lead").map(String::as_str) == Some("true")));

        let unreached_spans = capture.named("rdm.node.tickle.returns-unknown.via-unreached");
        assert_eq!(unreached_spans.len(), 1);
        assert_eq!(
            unreached_spans[0]
                .attributes
                .get("next")
                .map(String::as_str),
            Some("verdict")
        );

        let inspect_spans = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
        assert_eq!(inspect_spans.len(), 1);
        assert_eq!(
            inspect_spans[0]
                .attributes
                .get("verdict")
                .map(String::as_str),
            Some("running")
        );
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:25
    /// "The result is the one decision (Q111). The primary that investigated writes it as one
    ///  `rdm.node.investigated` event, `subject` the node.id, with the incarnation, path, name, result and
    ///  the inspect's answer that led to it"
    /// (review finding 10: every result write goes through one function that writes at most once per
    /// investigation of a birth): an investigation begun here ends once. Its first end writes one event;
    /// a second end of the same investigation, whatever result it names, writes nothing; an end with no
    /// investigation begun here writes nothing; an end with no result (an ordered investigation that
    /// matches the topology, Q393) closes it, and nothing is written after it; a new investigation of the
    /// same birth, begun again, writes its own one result. Must NOT happen: two events for one
    /// investigation, or an event with no investigation begun.
    // @feature: node-investigation
    #[tokio::test]
    async fn one_investigation_of_a_birth_writes_one_result() {
        use crate::deployment::provider::Inspected;
        use crate::wiring::NodeInvestigatedResult as R;
        let (node, view) = world();
        let incarnation = node.incarnation_id.clone().unwrap();
        let writer = NodeInvestigatedWriter::in_memory(&view.fabric.id, &NodeId::mint());
        let ids = || fabric_and_mesh_of(&view, &node);
        let results = || async { writer.events.events.of_node(&node.node_id).await.unwrap().iter().map(|e| e.data.get("result").and_then(|v| v.as_str()).unwrap_or_default().to_string()).collect::<Vec<_>>() };
        assert!(!writer.end(ids(), &node, &incarnation, Some(R::Ready), None, Reason::SelfHeal).await, "no investigation of the birth began here: nothing to end");
        assert_eq!(results().await, Vec::<String>::new(), "no investigation began: no result is written");
        writer.begin(&node.node_id, &incarnation);
        writer.begin(&node.node_id, &incarnation);
        assert!(writer.end(ids(), &node, &incarnation, Some(R::Rejoined), Some(&Inspected::Running), Reason::SelfHeal).await, "the open investigation ends");
        assert!(!writer.end(ids(), &node, &incarnation, Some(R::ReCreated), Some(&Inspected::Running), Reason::SelfHeal).await, "ended already");
        assert!(!writer.end(ids(), &node, &incarnation, Some(R::Ready), None, Reason::SelfHeal).await, "ended already");
        assert_eq!(results().await, vec!["rejoined".to_string()], "one investigation, one result, the first end's");
        writer.begin(&node.node_id, &incarnation);
        assert!(writer.end(ids(), &node, &incarnation, None, None, Reason::Ordered { trigger: "report-call-failed" }).await, "an ordered investigation that matched ends with no result");
        assert!(!writer.end(ids(), &node, &incarnation, Some(R::Ready), None, Reason::Ordered { trigger: "report-call-failed" }).await, "and nothing is written after its end");
        assert_eq!(results().await, vec!["rejoined".to_string()], "the matched ordered investigation wrote nothing");
        writer.begin(&node.node_id, &incarnation);
        assert!(writer.end(ids(), &node, &incarnation, Some(R::Ready), None, Reason::SelfHeal).await);
        assert_eq!(results().await, vec!["rejoined".to_string(), "ready".to_string()], "a new investigation of the birth writes its own one result");
    }
}
