//! Node investigation (rulings INDEX Q107 item 1, Q393; node-investigation.md): the one function every
//! caller uses on one exact birth, ending in one verdict, reached / running / os-stopped / exited, or
//! `dead` when the birth is not proven by the end of the investigation's retries.
//!
//! Its steps, in order: (a) the leads, one read of the store's by-destination connections index
//! (connections.md §3.1), naming the nodes in any mesh that hold a connection to the exact birth: the
//! peers the kick goes through first, never evidence of reachability, since a row nobody marked
//! dropped survives a cut or a crashed source; (b) the provider about the exact runtime
//! (node.runtime.inspect, node-runtime-inspect.md), whose `exited` or `os-stopped` is the verdict;
//! (c) the direct tickle and (d) the same kick through peers, the leads first
//! ([`crate::offline::tickle`], node-tickle.md). Only an answer from the exact birth is `reached`;
//! unreached, the verdict is the inspect's, an `unavailable` one asked again under the investigation's
//! retries ([`ask_again`]) and `dead` at their end.

use crate::model::{Node, NodeId, NodeStatus};
use crate::offline::{Tickle, TickleCalls};
use crate::topology::Topology;
use rdm_core::connections::{ConnectionKind, ConnectionState, NodeConnection};

/// What the investigation of one exact birth found: its one verdict.
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
    /// The verdict as node-investigation.md names it: `reached`, `running`, `os-stopped` or `exited`,
    /// or `dead` for a birth not proven by the end of the retries.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Reached { .. } => "reached",
            Self::Running => "running",
            Self::OsStopped => "os-stopped",
            Self::Exited { .. } => "exited",
            Self::Dead { .. } => "dead",
        }
    }

    /// The provider's answer the verdict came from (node.runtime.inspect's), `dead` as its last
    /// `unavailable`; `None` for `reached`, which asked the provider nothing.
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
/// so it does nothing (node-recovery.md step 3, Q393): `running` (and `reached`, the birth's own answer
/// that it runs) and a live status or a lifecycle state the node reported, in which its runtime runs;
/// `os-stopped` and `stopped`; `exited` and `dead`. Anything else differs and follows the self-heal.
pub fn matches_topology(finding: &Finding, status: NodeStatus) -> bool {
    use NodeStatus as S;
    match finding {
        Finding::Reached { .. } | Finding::Running => matches!(status, S::Starting | S::Pending | S::Ready | S::ConnectionsDeleting | S::ConnectionsDeleted | S::Stopping | S::Stopped),
        Finding::OsStopped => status == S::Stopped,
        Finding::Exited { .. } => status == S::Dead,
        Finding::Dead { .. } => false,
    }
}

/// What follows an investigation's verdict (node-recovery.md step 3, Q393).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Follows {
    /// An ordered investigation whose verdict matches the birth's status in the topology: nothing.
    Nothing,
    /// The birth is commanded back through node.mesh.join, sent until answered.
    MeshJoin,
    /// The birth is commanded to start (node.start), sent until answered.
    Start,
    /// The birth is written `dead`, and its recovery runs at once.
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
/// topology is `status` (node-recovery.md step 3, Q393): an ordered investigation whose verdict
/// matches the status ([`matches_topology`]) does nothing; any other verdict follows the self-heal:
/// `reached` and `running` are commanded back through node.mesh.join, `os-stopped` is commanded to
/// start, and `exited` or `dead` (not proven by the end of the retries) is written `dead`.
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

/// [`follows`] for a `reached` birth, its answer whatever it said: the birth runs.
pub fn follows_reached(reason: Reason, status: NodeStatus) -> Follows {
    follows(&Finding::Running, reason, status)
}

/// node.runtime.inspect's answer ([`crate::deployment::provider::inspect_runtime`]) as the
/// investigation's verdict: its `unavailable` is `dead` once no retry is left ([`ask_again`]).
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
/// node.runtime.inspect of the birth's exact runtime, whose `exited` or `os-stopped` is the verdict
/// with no tickle sent; otherwise the direct tickle and the same kick through peers, the leads first
/// ([`crate::offline::tickle_with`]). An answer from the exact birth is `reached`; nobody reaching it,
/// the verdict is the inspect's: `running` stays `running`, and an `unavailable` answer is asked again
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
/// runtime running, os-stopped or exited is the verdict; a birth still not proven when the attempts are
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
/// recorded at launch"), the one resolution every inspect copy makes: a digest `digest_view` holds of that
/// exact birth (heard, else carried by its mesh-primary's projection, else the last one of a stopped
/// birth) when it carries a runtime fact, else the runtime on the node's record in `nodes`, a point read
/// ([`runtime_of`]). A birth lost before this admin heard it has no digest here; its record still holds
/// the runtime the node wrote at its start. `Err` names what was missing.
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

/// What the primary that investigated a node writes its verdict with (node-investigation.md, build.md
/// §5): the events store, this admin's exact birth (each event's `source`), its rafka-time, and the
/// application's `on_node_investigated`, sent from the event's write (fabric-node-hooks.md, Q312).
#[derive(Clone)]
pub struct NodeInvestigatedWriter {
    /// What the verdict is written with: `events.storage`, this admin's exact birth (the event's
    /// `source`) and its rafka-time (the event's `time`).
    pub events: crate::events_storage::EventsWriter,
    /// The application's `on_node_investigated`; `None` when it registers none.
    pub on_node_investigated: Option<crate::wiring::NodeInvestigatedNotice>,
}

impl NodeInvestigatedWriter {
    /// A writer on an events store held in memory, by the birth `node_id` of `fabric_id`, at
    /// rafka-time `1`, with no hook: what a rig with no topology storage writes its verdicts with.
    pub fn in_memory(fabric_id: &rdm_core::FabricId, node_id: &NodeId) -> Self {
        Self { events: crate::events_storage::EventsWriter::in_memory(fabric_id, node_id), on_node_investigated: None }
    }
}

/// The verdict `verdict` of the investigation of `node`'s birth at `incarnation`, and what RDM does
/// with it, `next`, for the investigation's `reason` (Q393): written by this primary, the one that investigated, as one `rdm.node.investigated`
/// event (node-investigation.md, build.md §5), before anything acts on it; from that write the
/// application's `on_node_investigated` is sent, once, on a task of its own (fabric-node-hooks.md,
/// Q312). Both under `rdm.node_admin.node.update.via-on-node-investigated`, a child of the span current
/// here, which names the event; the event and the notice carry the current span's trace context.
/// `ids` are the fabric's and the birth's mesh's ([`fabric_and_mesh_of`]). The notice gates nothing
/// and orders nothing; an event not written sends none, named on the span.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_node_investigated(writer: &NodeInvestigatedWriter, ids: (rdm_core::FabricId, Option<rdm_core::MeshId>), node: &Node, incarnation: &crate::model::IncarnationId, verdict: crate::wiring::NodeInvestigatedVerdict, next: crate::wiring::NodeInvestigatedNext, reason: Reason) {
    use tracing::Instrument as _;
    let context = crate::build::current_context();
    let (fabric_id, mesh_id) = ids;
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-on-node-investigated",
        node_path = %node.path, node_name = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        mesh = %node.mesh,
        verdict = verdict.as_str(),
        next = next.as_str(),
        reason = reason.as_str(),
        trigger = reason.trigger(),
        event_id = tracing::field::Empty,
        outcome = tracing::field::Empty,
        "otel.kind" = "internal",
    );
    let Some(time_ms) = writer.events.time.try_now_ms() else {
        span.record("outcome", "no-rafka-time");
        span.in_scope(|| tracing::info!("this admin holds no rafka-time: the verdict is not written and the notice is not sent"));
        return;
    };
    let event = crate::events_storage::Event::node_investigated(&writer.events.source, time_ms, &node.node_id, incarnation, &node.path, &node.name, verdict.as_str(), next.as_str(), reason);
    span.record("event_id", event.id.as_str());
    if let Err(e) = writer.events.events.append(&event).instrument(span.clone()).await {
        span.record("outcome", "event-not-written");
        span.in_scope(|| tracing::info!(cause = %e, "the verdict's rdm.node.investigated event was not written: the notice is not sent"));
        return;
    }
    let (hook, mesh_id) = match (writer.on_node_investigated.clone(), mesh_id) {
        (Some(hook), Some(mesh_id)) => (hook, mesh_id),
        (None, _) => {
            span.record("outcome", "no-hook");
            span.in_scope(|| tracing::info!("the verdict is written; no on_node_investigated hook is registered"));
            return;
        }
        (Some(_), None) => {
            span.record("outcome", "no-mesh-id");
            span.in_scope(|| tracing::info!("the verdict is written; this admin's view holds no id for the birth's mesh: the notice is not sent"));
            return;
        }
    };
    span.record("outcome", "sent");
    let ctx = crate::wiring::NodeInvestigatedCtx {
        fabric_id,
        mesh_id,
        node_id: node.node_id.clone(),
        incarnation: incarnation.clone(),
        path: node.path.clone(),
        name: node.name.clone(),
        verdict,
        next,
        context,
    };
    tokio::spawn(
        async move {
            hook(ctx).await;
            tracing::info!("the application was told the node investigation's verdict");
        }
        .instrument(span),
    );
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
    tracing::info_span!("rdm.node_admin.node.resolve.via-provider-inspection", node_path = %node.path, node_name = %node.name, node_id = %node.node_id, incarnation_id = %incarnation, inspector = %inspector, handle = %handle, verdict = crate::admin::verdict_name(outcome), exit_code, reason = %reason)
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

    fn digest(n: &Node, incarnation: IncarnationId, runtime: Option<rdm_core::RuntimeFact>) -> rdm_core::MeshDigest {
        rdm_core::MeshDigest {
            fabric_id: FabricId::mint(),
            node: rdm_core::MeshNode { mesh: n.mesh.clone(), r#type: n.r#type.clone(), node_id: n.node_id.clone(), path: n.path.clone(), name: n.name.clone(), endpoint_id: crate::model::EndpointId(String::new()), transport_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)), incarnation, supersedes: None, },
            status: rdm_core::model::NodeStatus::Ready,
            digest_seq: 1,
            emitted_at_rafka_ms: 0,
            primary: Vec::new(),
        }
    }

    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:25
    /// "1. The caller holds the birth's launch record: the handle the provider returned when it launched
    ///     this exact incarnation (`deployment_id`, `provider`, `control_domain`, `locator`)."
    /// CONTRACT (node-runtime-inspect.md step 1: "The caller holds the birth's launch record: the
    /// handle the provider returned when it launched this exact incarnation"; node-recovery.md step 3:
    /// "the exact runtime recorded at launch"): every inspect resolves a birth's runtime one way, a
    /// digest of that exact birth that carries a runtime fact, else the birth's own durable runtime
    /// row; a digest of another birth of the node, or one with no fact, falls to the row; with neither,
    /// nothing is resolved and the reason names both.
    #[tokio::test]
    async fn a_births_runtime_is_its_digests_else_its_durable_row_one_way_for_every_inspect() {
        use crate::storage::NodesStorage as _;
        let (node, _) = world();
        let inc = node.incarnation_id.clone().unwrap();
        let nodes = crate::storage::MemoryNodesStorage::default();
        let empty = rdm_gossip::digest_view::DigestView::default();
        let none = birth_runtime(&empty, Some(&nodes), &node).await.unwrap_err();
        assert!(none.contains("no digest of this exact birth") && none.contains("no node record"), "{none}");
        nodes.put_node(&{ let mut r = rdm_node::nodes_storage::NodeRecord { runtime: fact(2), ..rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(node.path.clone(), node.name.clone()).unwrap(), &node.r#type.clone().to_string(), node.node_id.clone(), inc.clone(), rdm_core::model::NodeStatus::Ready) }; r.storage.data_dir = Some("/data/from-the-row".into()); r }).await.unwrap();
        let from_row = BirthRuntime { fact: fact(2), data_dir: Some("/data/from-the-row".into()), incarnation: inc.clone() };
        assert_eq!(birth_runtime(&empty, Some(&nodes), &node).await, Ok(from_row.clone()), "no digest: the node record");
        for (held_digest, why) in [(digest(&node, IncarnationId::mint(), Some(fact(3))), "a digest of another birth"), (digest(&node, inc.clone(), None), "a digest with no runtime fact")] {
            let digest_view = rdm_gossip::digest_view::DigestView::default();
            assert!(digest_view.record(held_digest));
            assert_eq!(birth_runtime(&digest_view, Some(&nodes), &node).await, Ok(from_row.clone()), "{why}: the node record");
        }
        let digest_view = rdm_gossip::digest_view::DigestView::default();
        assert!(digest_view.record(digest(&node, inc.clone(), Some(fact(1)))));
        assert_eq!(birth_runtime(&digest_view, Some(&nodes), &node).await, Ok(BirthRuntime { fact: fact(1), data_dir: Some("/data/from-the-digest".into()), incarnation: inc }), "the exact birth's digest carries the runtime recorded at launch");
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
    /// "A primitive. Node investigation is the one function every caller uses on one exact birth, ending in
    ///  one verdict: `reached`, `running`, `os-stopped` or `exited`; a birth not proven by the end of its
    ///  retries is `dead` (Q393)."
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:10
    /// "2. [node.runtime.inspect](node-runtime-inspect.md). `exited` or `os-stopped` is the verdict; no tickle is sent.
    ///  3. [node.tickle](node-tickle.md), direct.
    ///  4. The same tickle via those peers. Unreached, the verdict is the inspect's answer: `running`; an
    ///     inspect that answers `unavailable` is asked again under the investigation's retries, and a birth
    ///     still not proven at their end is `dead`.
    ///  Only an answer from the exact birth counts as `reached`."
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:20
    /// "The verdict is the one decision (Q111). Reached: the node is commanded back through
    ///  [node.mesh.join](node-mesh-join.md). Any other verdict: it is re-created at the same path and name with a
    ///  new node.id (Q110)."
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:55
    /// "Target: One node, by `(node.id, incarnation)`
    ///  Steps: The by-destination connections index read, node.runtime.inspect, node.tickle direct, the same tickle via those peers
    ///  Result: `reached`, `running`, `os-stopped` or `exited`, or `dead` when not proven by the end of the retries"
    #[tokio::test(start_paused = true)]
    async fn only_the_exact_births_answer_is_reached_and_only_the_provider_proves_an_exit() {
        let (node, view) = world();
        let rig = |direct: Option<StatusReply>, carried: Option<(NodeId, StatusReply)>| Rig { direct, carried, asked: Mutex::default() };
        // The inspect first (Q273): exited or os-stopped is the verdict, and no tickle is sent.
        for said in [Finding::Exited { code: Some(137) }, Finding::OsStopped] {
            let asked = AtomicBool::new(false);
            let r = rig(Some(current(&node)), Some((of(&view, rdm_core::PathName::new("mesh1.node", "3").unwrap()).node_id, current(&node))));
            assert_eq!(investigate(&node, &view, &r, || provider_says(said.clone(), &asked)).await, said, "the provider proved the runtime gone: its verdict");
            assert!(asked.load(Ordering::SeqCst), "the provider is asked first");
            assert!(r.asked.lock().unwrap().is_empty(), "no tickle is sent once the inspect proved it gone: {:?}", r.asked.lock().unwrap());
        }
        // Running or unavailable: the ladder runs, and only the exact birth's answer is reached; nobody
        // reaching it, the verdict is the inspect's (an `unavailable` one asked again under the
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

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:12
    /// "4. The same tickle via those peers. Unreached, the verdict is the inspect's answer: `running`; an
    ///     inspect that answers `unavailable` is asked again under the investigation's retries, and a birth
    ///     still not proven at their end is `dead`."
    /// (Q393: "ITS DEAD IF IT CAN'T PROVE IT OR IT GETS TO RUN RETRIES TILL THE END AND THEN ITS DEAD"):
    /// unreached, an `unavailable` inspect is asked again under one `Send` of the default attempts; the
    /// first answer that proves the runtime is the verdict, and one never proven is `dead` once the
    /// attempts are spent, named by its last answer. Must NOT happen: `unavailable` taken as the verdict
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
    /// `dead`. Anything else differs and follows the self-heal."): an ordered investigation's verdict
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
        let runs = [S::Starting, S::Pending, S::Ready, S::ConnectionsDeleting, S::ConnectionsDeleted, S::Stopping, S::Stopped];
        for status in all {
            assert_eq!(matches_topology(&Finding::Running, status), runs.contains(&status), "running and {status:?}");
            assert_eq!(matches_topology(&reached, status), runs.contains(&status), "reached and {status:?}");
            assert_eq!(matches_topology(&Finding::OsStopped, status), status == S::Stopped, "os-stopped and {status:?}");
            assert_eq!(matches_topology(&Finding::Exited { code: None }, status), status == S::Dead, "exited and {status:?}");
            assert!(!matches_topology(&Finding::Dead { reason: String::new() }, status), "dead (not proven) and {status:?}");
        }
        assert_eq!((Reason::SelfHeal.as_str(), Reason::SelfHeal.trigger()), ("self-heal", "silence"));
        assert_eq!((Reason::Ordered { trigger: "report-call-failed" }.as_str(), Reason::Ordered { trigger: "report-call-failed" }.trigger()), ("ordered", "report-call-failed"));
    }

    /// CONTRACT (node-investigation.md: "Only an answer from the exact birth counts as `reached`"; Q332:
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
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:5
    /// "A primitive. Node investigation is the one function every caller uses on one exact birth, ending in
    ///  one verdict: `reached`, `running`, `os-stopped` or `exited`; a birth not proven by the end of its
    ///  retries is `dead` (Q393). Its steps, in order:
    ///  1. Read the by-destination connections index ([connections.md §3.1](../connections.md#31-declared-indexes)).
    ///     It names the peers to try; it never proves the node is reachable.
    ///  2. [node.runtime.inspect](node-runtime-inspect.md). `exited` or `os-stopped` is the verdict; no tickle is sent.
    ///  3. [node.tickle](node-tickle.md), direct.
    ///  4. The same tickle via those peers. Unreached, the verdict is the inspect's answer: `running`; an
    ///     inspect that answers `unavailable` is asked again under the investigation's retries, and a birth
    ///     still not proven at their end is `dead`."
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:59
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
}
