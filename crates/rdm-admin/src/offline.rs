//! The offline tickle (`docs/architecture/node-identity-and-state.md` §7.3,
//! `docs/architecture/lifecycles/node-tickle.md`).
//!
//! A silent node is never restarted or deleted for being silent. Every node's own pruner marks it
//! `PendingReconnect` at the staleness floor. Every node-admin keeps the silence bookkeeping
//! (`first_seen`), so a node-admin that takes the primary role mid-window tickles on the window already
//! running; only the node-admin holding its mesh's `mesh-primary` role tickles. Half a floor after the
//! mark (floor + half a floor in all) it runs the one reach ladder, `node.tickle` ([`tickle`],
//! node-tickle.md: direct, then through up to two live peers at once), which this module holds for
//! every caller of it. Each ladder runs beside the loop, one per exact birth, and the first tick
//! after it ends folds what it found (node-recovery.md: one loop, on every gossip interval). An
//! answer is `reachable-silent`: recorded, no action. The inspect runs before the ladder (Q273): its
//! `exited` or `os-stopped` holds the birth offline with no probe sent; a ladder nobody answered is the
//! birth held offline (TRUE OFFLINE) at that fold, and its recovery follows at once (node-recovery.md
//! steps 2-3; node-tickle.md "What follows": "on `unreached` the investigation's verdict is the
//! inspect's answer, read before the tickle"): nothing holds and nothing is probed again (Q111, Q181). A node that leaves the
//! silent set has returned. The `Dead` status is observer-inferred, never sent, never itself a reason
//! to act: the investigation's verdict is.
//! The admin writes the node's status on its row in nodes.storage and shows it in the view.

use std::collections::{HashMap, HashSet};

/// One silent node of this tick's silent set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SilentNode {
    pub node_id: String,
    pub path: String,
    /// The exact birth's incarnation, as the watched record holds it (`None` for a record that holds
    /// none yet).
    pub incarnation: Option<crate::model::IncarnationId>,
    /// The mark, on this admin's local clock (ms): the instant this admin's digest view holds the
    /// birth went unheard past the staleness floor ([`silent_set`]). `None` for a birth the view holds
    /// no receipt of (a birth only a durable runtime row names), whose silence is first seen at the
    /// tick that lists it.
    pub marked_at_ms: Option<u64>,
}

/// The silent set of one tick: every watched birth, each with its mark read from this admin's digest
/// view at `now` (`now_ms` on the local clock): the birth's last receipt plus the staleness floor
/// `floor`, the instant its silence passed the floor (node-recovery.md "Detection and proof" step 1:
/// "Each node's pruner marks a member `PendingReconnect` once it is unheard past the staleness
/// floor"). A birth the digest view holds only from its mesh-primary's projection is unheard since the digest view
/// first held it, and is tickled only once the view holds it off ready (its caller's
/// filter: the topology, not what this admin has heard, decides who is tickled; Q87). A birth the view holds nothing of, a receipt of another incarnation, or one heard
/// within the floor carries no mark.
pub(crate) fn silent_set<'a>(watched: impl IntoIterator<Item = &'a crate::model::Node>, digest_view: &rdm_transport::topology::DigestView, now: std::time::Instant, now_ms: u64, floor: std::time::Duration) -> Vec<SilentNode> {
    watched
        .into_iter()
        .map(|n| {
            let marked_at_ms = digest_view
                .get_at(n.node_id.as_str(), now)
                .or_else(|| digest_view.projected_at(n.node_id.as_str(), now))
                .filter(|(d, unheard)| Some(&d.node.incarnation) == n.incarnation_id.as_ref() && *unheard >= floor)
                .map(|(_, unheard)| now_ms.saturating_sub((unheard - floor).as_millis() as u64));
            SilentNode { node_id: n.node_id.to_string(), path: n.name.to_string(), incarnation: n.incarnation_id.clone(), marked_at_ms }
        })
        .collect()
}

/// A command its birth never received for a reachability reason (node-tickle.md step 1): the
/// mesh-primary marks the birth `PendingReconnect` at once, so its tickle ladder runs from this mark
/// instead of after the staleness floor. An observer mark: it ends when the birth is heard after it,
/// answers its tickle, or leaves the view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreachableMark {
    /// The exact birth the command named.
    pub incarnation: crate::model::IncarnationId,
    /// When the command ended unsent.
    pub at: std::time::Instant,
    /// The same instant on the local clock, ms since the Unix epoch.
    pub at_ms: u64,
    /// `connect-refused`, `connect-timed-out`, `connect-failed` or `endpoint-unresolved`.
    pub reason: &'static str,
}

/// Whether a reachability `NotSent` marks a birth whose mesh-primary holds `recorded` for it
/// (node-recovery.md step 1): an observer mark beside a live status (`Pending`, `Ready`, or
/// none held), never over a lifecycle state the node reported and the mesh-primary recorded
/// (connections-deleting, stopping, stopped).
pub(crate) fn marks_over(recorded: Option<rdm_core::MemberStatus>) -> bool {
    match recorded {
        Some(rdm_core::MemberStatus::ConnectionsDeleting | rdm_core::MemberStatus::Leaving) => false,
        Some(rdm_core::MemberStatus::Pending | rdm_core::MemberStatus::Ready) | None => true,
    }
}

/// The marks a mesh-primary holds, by node id: written by its executor's observer, read by its tick.
pub type UnreachableMarks = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<crate::model::NodeId, UnreachableMark>>>;

/// The marks of `mesh`'s births that stand at `now`: a mark ends when the view no longer holds that
/// exact birth of `mesh`, or when `digest_view` heard the birth after the mark. The ended ones are dropped
/// from `marks`.
pub(crate) fn held_marks(marks: &UnreachableMarks, view: &crate::topology::Topology, mesh: &str, digest_view: &rdm_transport::topology::DigestView, now: std::time::Instant) -> std::collections::HashMap<crate::model::NodeId, UnreachableMark> {
    let mut marks = marks.lock().unwrap();
    marks.retain(|id, m| {
        let held = view.births().any(|n| &n.node_id == id && n.incarnation_id.as_ref() == Some(&m.incarnation) && n.mesh == mesh);
        let heard_after = digest_view.get_at(id.as_str(), now).is_some_and(|(d, unheard)| d.node.incarnation == m.incarnation && now.saturating_duration_since(m.at) > unheard);
        held && !heard_after
    });
    marks.clone()
}

/// Every marked birth the view holds, other than this admin itself, is watched `PendingReconnect`.
pub(crate) fn watch_marked(watched: &mut std::collections::BTreeMap<String, crate::model::Node>, marks: &std::collections::HashMap<crate::model::NodeId, UnreachableMark>, view: &crate::topology::Topology, me: &crate::model::NodeId) {
    for (id, m) in marks {
        if let Some(n) = view.births().find(|n| &n.node_id == id && n.incarnation_id.as_ref() == Some(&m.incarnation) && &n.node_id != me) {
            watched.entry(id.to_string()).or_insert_with(|| crate::model::Node { status: crate::model::NodeStatus::PendingReconnect, ..n.clone() });
        }
    }
}

/// A silent birth's mark is the earlier of its view's floor mark and its unreachable mark.
pub(crate) fn marked_from(mut s: SilentNode, marks: &std::collections::HashMap<crate::model::NodeId, UnreachableMark>) -> SilentNode {
    if let Some((_, m)) = marks.iter().find(|(id, m)| id.as_str() == s.node_id && s.incarnation.as_ref() == Some(&m.incarnation)) {
        s.marked_at_ms = Some(s.marked_at_ms.map_or(m.at_ms, |at| at.min(m.at_ms)));
    }
    s
}

/// The births of `mesh` the view holds that have left a command of `commands` unanswered past the
/// staleness `floor` (node-investigation.md, Q182), each marked as a silent birth is: from the instant
/// the command passed the floor, on the local clock. A birth that keeps gossiping is marked all the
/// same: it does not answer commands, so it is silent whatever it gossips.
pub(crate) fn unanswered_marks(commands: &crate::node_commands::CommandView, view: &crate::topology::Topology, mesh: &str, floor: std::time::Duration, now: std::time::Instant, now_ms: u64) -> std::collections::HashMap<crate::model::NodeId, UnreachableMark> {
    view.births()
        .filter(|n| n.mesh == mesh)
        .filter_map(|n| {
            let incarnation = n.incarnation_id.clone()?;
            let passed = commands.unanswered_since(&n.node_id, &incarnation, floor, now)? + floor;
            let at_ms = now_ms.saturating_sub(now.saturating_duration_since(passed).as_millis() as u64);
            Some((n.node_id.clone(), UnreachableMark { incarnation, at: passed, at_ms, reason: "command-unanswered" }))
        })
        .collect()
}

/// How many live peers the via-peer step asks (`docs/architecture/lifecycles/node-tickle.md` step 3).
pub const VIA_PEER_TICKLE_FANOUT: usize = 2;

/// The longest one tickle takes when each probe waits `per_call`: the direct probe, then one via-peer
/// rung with every carrier probed at once (`docs/architecture/lifecycles/node-tickle.md` steps 2-3).
/// The fan-out adds no wait.
pub fn tickle_ladder(per_call: std::time::Duration) -> std::time::Duration {
    per_call * 2
}

/// [`tickle_ladder`] at the default call budget (`docs/architecture/lifecycles/node-tickle.md`), the
/// one every call of the tickle spends.
pub fn tickle_ladder_at_default_budget() -> std::time::Duration {
    match rdm_transport::CallOptions::default().budget {
        rdm_transport::Budget::Overall(d) => tickle_ladder(d),
        other => panic!("the default call budget is not one overall deadline: {other:?}"),
    }
}

/// The mesh-primary's recovery tick (node-recovery.md "Detection and proof": "The mesh-primary runs
/// one loop for its own members, on every gossip interval, never on a timer of its own"): the loop
/// runs a pass, then waits here for the next one. Passes start on the grid of the gossip interval
/// laid from the cadence's start, whatever a pass spends: the next pass is one interval after the
/// previous one STARTED, never one interval after it ended.
///
/// A pass that runs past a grid point ([`tokio::time::MissedTickBehavior::Skip`]) is followed by the
/// one pass it owes at once, then the passes return to the same grid; the ticks it ran past are not
/// run back to back (`Burst`: passes that would read the same view an instant apart), and the grid
/// does not move to the late pass (`Delay`: one interval after the late pass, which is the interval
/// plus the pass's own work again).
pub(crate) struct Cadence {
    ticks: tokio::time::Interval,
}

impl Cadence {
    /// The cadence at `period`, the gossip interval; the first pass is the caller's own, and the
    /// first wait ends one interval after this call. A zero period has no interval to run on and is
    /// refused by name.
    pub(crate) fn every(period: std::time::Duration) -> Result<Self, String> {
        if period.is_zero() {
            return Err("the gossip interval (RDM_GOSSIP_INTERVAL_MS) is 0 ms: the mesh-primary's recovery tick runs on every gossip interval, and a zero interval has none".to_string());
        }
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Ok(Self { ticks })
    }

    /// Wait for the next pass.
    pub(crate) async fn next(&mut self) {
        self.ticks.tick().await;
    }
}

// ---- node.tickle (node-tickle.md): the one reach ladder ----

/// The birth's own answer on the probe (`docs/architecture/lifecycles/node-tickle.md` "Probe"):
/// `Current`, `CurrentStopped` while it is stopped, or `Blocked` while its hook is Blocked, naming
/// the exact birth probed. A carrier's `InnerNotSent` (no reply reaches the ladder), a `NotReady`
/// and any refusal are not answers.
pub fn birth_answered(reply: Option<&rdm_core::status::StatusReply>, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> bool {
    use rdm_core::status::StatusReply;
    match reply {
        Some(StatusReply::Current { node_id: n, incarnation: i, .. } | StatusReply::CurrentStopped { node_id: n, incarnation: i } | StatusReply::Blocked { node_id: n, incarnation: i, .. }) => n == node_id && i == incarnation,
        _ => false,
    }
}

/// How one probe of the ladder ended (`docs/architecture/lifecycles/node-tickle.md`): what the
/// caller reads in the Node RPC outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeEnd {
    /// A status reply came back (the node's own, or the carrier's `Forwarded { inner }`);
    /// [`birth_answered`] says whether it is the probed birth's.
    Reply(rdm_core::status::StatusReply),
    /// The carrier could not carry the probe: its own edge to the node is not Active
    /// (`InnerNotSent`, node-tickle.md step 3), with the carrier's account of that edge.
    CarrierEdgeLost(String),
    /// Any other end: the Node RPC outcome's name (`NotSent`, `Indeterminate`, `Unserved`,
    /// `RejectedStale`) and what it says.
    Other {
        /// The outcome's variant name.
        end: &'static str,
        /// What the outcome says.
        detail: String,
    },
}

impl ProbeEnd {
    /// What `out`, one probe's Node RPC outcome, says.
    pub fn of(out: rdm_core::outcome::RpcOutcome<rdm_core::status::StatusReply>) -> Self {
        use rdm_core::outcome::{NotSentReason, RpcOutcome};
        let (end, detail_name) = (out.name(), out.detail_name());
        match out {
            RpcOutcome::Reply(r) => Self::Reply(r.into_value()),
            RpcOutcome::NotSent(ns) => match ns.reason() {
                NotSentReason::CarrierEdgeLost(edge) => Self::CarrierEdgeLost(edge.clone()),
                other => Self::Other { end, detail: format!("not sent: {other:?}") },
            },
            _ => Self::Other { end, detail: detail_name.to_string() },
        }
    }

    /// The end as a span names it: the reply's name, or the failure.
    pub fn name(&self) -> String {
        match self {
            Self::Reply(r) => r.name().to_string(),
            Self::CarrierEdgeLost(edge) => format!("carrier-edge-lost: {edge}"),
            Self::Other { end, detail } => format!("{end}: {detail}"),
        }
    }

    /// Whether the probe reached a process that answered anything at all (a reply, or a carrier's own
    /// account of its edge): a `NotSent` or `RejectedStale` reached none.
    pub fn reached_a_process(&self) -> bool {
        match self {
            Self::Reply(_) | Self::CarrierEdgeLost(_) => true,
            Self::Other { end, .. } => !matches!(*end, "NotSent" | "RejectedStale"),
        }
    }
}

/// One via-peer probe of the ladder, by its carrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerProbe {
    /// The carrier's path.name.
    pub carrier: String,
    /// How the carried probe ended.
    pub end: ProbeEnd,
}

/// The ladder ended with no answer from the birth (node-tickle.md step 4): `unreached`, never death.
/// It holds the ladder's result span until the caller says what it does next (`next`), the span's
/// last field; the span ends with it.
#[derive(Debug)]
pub struct Unreached {
    /// How the direct probe ended.
    pub direct: ProbeEnd,
    /// Every via-peer probe, in the order the carriers were asked.
    pub peers: Vec<PeerProbe>,
    span: tracing::Span,
}

impl Unreached {
    /// An `unreached` a cell states, with no span of its own.
    #[cfg(test)]
    pub(crate) fn of(direct: ProbeEnd, peers: Vec<PeerProbe>) -> Self {
        Self { direct, peers, span: tracing::Span::none() }
    }

    /// The carriers asked, in order, joined by `,`.
    pub fn carriers(&self) -> String {
        self.peers.iter().map(|p| p.carrier.as_str()).collect::<Vec<_>>().join(",")
    }

    /// Record what the caller does next on the ladder's result span (node-tickle.md "Observability
    /// contract": `rdm.node.tickle.returns-unknown.via-unreached`, "what the caller does next").
    pub fn next(&self, what: &str) {
        self.span.record("next", what);
    }
}

/// What the ladder found (`docs/architecture/lifecycles/node-tickle.md` "Result").
#[derive(Debug)]
pub enum Tickle {
    /// The birth is alive and said what it is: its answer, and the carrier that carried it (`None`:
    /// it answered the direct probe).
    Reached {
        /// The birth's own answer.
        answer: rdm_core::status::StatusReply,
        /// The carrier's path.name, when the answer came through a peer.
        carrier: Option<String>,
    },
    /// Nobody reached the birth within the ladder.
    Unreached(Unreached),
}

/// The eligible live peers of `node`'s mesh (`docs/architecture/lifecycles/node-tickle.md` step 3),
/// in path order: every node of its mesh the view holds ready, other than the node and `me` (the
/// caller is never its own carrier).
pub fn eligible_carriers<'a>(view: &'a crate::topology::Topology, node: &'a crate::model::Node, me: Option<&'a crate::model::NodeId>) -> impl Iterator<Item = &'a crate::model::Node> + 'a {
    view.members().filter(move |c| c.mesh == node.mesh && c.node_id != node.node_id && Some(&c.node_id) != me && c.status == crate::model::NodeStatus::Ready)
}

/// What the caller probes with (`docs/architecture/lifecycles/node-tickle.md`): its Node RPC client
/// under its own call deadline, the carriers it holds eligible, and its name. One ladder ([`tickle`])
/// runs over it for every caller.
#[async_trait::async_trait]
pub trait TickleCalls: Send + Sync {
    /// The caller's path.name, named on every span of the ladder.
    fn caller(&self) -> String;
    /// The deadline of one probe.
    fn deadline(&self) -> std::time::Duration;
    /// `probe`, the `ProbeNodeState` naming `node`'s exact birth, direct to `node`: one bounded call.
    async fn direct(&self, node: &crate::model::Node, probe: &rdm_core::status::StatusRequest) -> ProbeEnd;
    /// The same probe carried by `carrier` as `Forward`, whose inner op is the probe.
    async fn via_peer(&self, carrier: &crate::model::Node, node: &crate::model::Node, probe: &rdm_core::status::StatusRequest) -> ProbeEnd;
    /// The eligible live peers of `node`'s mesh, in the order asked, the caller never among them;
    /// the ladder takes the first [`VIA_PEER_TICKLE_FANOUT`] after the leads ([`eligible_carriers`]
    /// is the page's eligibility).
    fn carriers(&self, view: &crate::topology::Topology, node: &crate::model::Node) -> Vec<crate::model::Node>;
    /// The leads of `node`'s exact birth (rulings INDEX Q107 item 1, step a): the nodes in any mesh
    /// that the store's by-destination connections index names as holding a connection to it
    /// ([`crate::node_investigation::leads_of`]), the peers the kick goes through first; never
    /// evidence of reachability. `Err` names why the index could not be read.
    async fn leads(&self, view: &crate::topology::Topology, node: &crate::model::Node) -> Result<Vec<crate::model::Node>, String>;
    /// Whether `node`'s exact birth has left a command of the caller's unanswered past the staleness
    /// floor (node-investigation.md, Q182): an answer to the probe from such a birth is not `reached`.
    fn command_unanswered(&self, _node: &crate::model::Node) -> bool {
        false
    }
}

/// The caller's Node RPC client at the default call budget (`docs/architecture/lifecycles/node-tickle.md`):
/// the `mesh-primary`'s recovery tick and the primary watch probe with it. Its own node id is never
/// a carrier.
pub struct TickleClient {
    /// The caller's Node RPC client.
    pub client: std::sync::Arc<rdm_transport::NodeRpcClient>,
    /// The caller's node id.
    pub me_id: crate::model::NodeId,
    /// The caller's path.name.
    pub me: String,
    /// The caller's connections store, whose by-destination index names the leads (connections.md
    /// §3.1); `None`: no leads, the kick goes through the carriers alone.
    pub connections: Option<std::sync::Arc<dyn crate::storage::ConnectionsStorage>>,
    /// The commands the caller sent: the birth that left one unanswered past the floor is not reached
    /// by an answer (Q182); `None`: the caller sends none.
    pub commands: Option<std::sync::Arc<crate::node_commands::CommandView>>,
}

#[async_trait::async_trait]
impl TickleCalls for TickleClient {
    fn caller(&self) -> String {
        self.me.clone()
    }

    fn deadline(&self) -> std::time::Duration {
        match rdm_transport::CallOptions::default().budget {
            rdm_transport::Budget::Overall(d) => d,
            other => panic!("the default call budget is not one overall deadline: {other:?}"),
        }
    }

    async fn direct(&self, node: &crate::model::Node, probe: &rdm_core::status::StatusRequest) -> ProbeEnd {
        // One connect, direct (node-recovery.md step 2): asked once, never resent; the ladder, not
        // this call, goes on to the peers.
        let once = rdm_transport::CallOptions { once: true, ..rdm_transport::CallOptions::default() };
        let (out, _) = self.client.call::<rdm_core::status::Status>(&rdm_transport::NodeTarget::ExactNode(node.node_id.clone()), probe, &once).await;
        ProbeEnd::of(out)
    }

    async fn via_peer(&self, carrier: &crate::model::Node, node: &crate::model::Node, probe: &rdm_core::status::StatusRequest) -> ProbeEnd {
        let route = rdm_transport::RouteChoice::ViaPeer { carrier: carrier.node_id.clone(), path: carrier.name.clone() };
        let (out, _, _) = self.client.call_routed::<rdm_core::status::Status>(&node.node_id, &route, probe, &rdm_transport::CallOptions::default()).await;
        ProbeEnd::of(out)
    }

    fn carriers(&self, view: &crate::topology::Topology, node: &crate::model::Node) -> Vec<crate::model::Node> {
        eligible_carriers(view, node, Some(&self.me_id)).cloned().collect()
    }

    fn command_unanswered(&self, node: &crate::model::Node) -> bool {
        let (Some(commands), Some(incarnation)) = (&self.commands, &node.incarnation_id) else { return false };
        commands.unanswered_since(&node.node_id, incarnation, rdm_transport::topology::staleness_floor(), std::time::Instant::now()).is_some()
    }

    async fn leads(&self, view: &crate::topology::Topology, node: &crate::model::Node) -> Result<Vec<crate::model::Node>, String> {
        let Some(store) = &self.connections else { return Ok(Vec::new()) };
        let rows = store.connections_to(&node.node_id).await.map_err(|e| format!("the by-destination connections index of {} ({}) could not be read: {e}", node.name, node.node_id))?;
        Ok(crate::node_investigation::leads_of(&rows, view, node, Some(&self.me_id)))
    }
}

/// `node.tickle` (`docs/architecture/lifecycles/node-tickle.md`): ask `node`'s exact birth, by node
/// id and incarnation, whether anyone can reach it. The leads first (rulings INDEX Q107 item 1, step
/// a: one read of the by-destination connections index, `calls.leads`); direct (step 2); on no answer
/// the same probe through up to two peers at once, the leads first, then the caller's eligible live
/// peers of the node's mesh (step 3; Q107 item 1 step c), each a `Forward` whose inner op is the
/// probe. The first answer from the birth, under the probed incarnation, ends the ladder and cancels
/// the probes still running; one failed peer probe does not cancel the other (step 4). It answers
/// `reached` with what the node said, or `unreached`: never dead, a failure, a deadline included,
/// proves nothing. A node the view holds no incarnation for names no exact birth to probe: refused
/// by name, no ladder runs. Run by the `mesh-primary` in node recovery, the `fabric-primary` in mesh
/// investigation, the primary watch and the successor ladder of node.primary.update, over `calls`,
/// the caller's own client, deadline and eligible carriers.
pub async fn tickle<C: TickleCalls + ?Sized>(node: &crate::model::Node, view: &crate::topology::Topology, calls: &C) -> Result<Tickle, String> {
    if node.incarnation_id.is_none() {
        return Err(no_birth(node));
    }
    let leads = leads_of(node, view, calls).await;
    tickle_with(node, view, calls, leads).await
}

fn no_birth(node: &crate::model::Node) -> String {
    format!("{} ({}) holds no recorded birth: there is no exact birth to tickle", node.name, node.node_id)
}

/// The leads (Q107 item 1, step a; node-investigation.md's index read): who holds a connection to the
/// birth, the peers the kick goes through first; never reachability. An index that cannot be read
/// leaves the caller's carriers alone, named on the spans.
pub async fn leads_of<C: TickleCalls + ?Sized>(node: &crate::model::Node, view: &crate::topology::Topology, calls: &C) -> (Vec<crate::model::Node>, String) {
    match calls.leads(view, node).await {
        Ok(leads) => {
            let named = leads.iter().map(|l| l.name.to_string()).collect::<Vec<_>>().join(",");
            (leads, named)
        }
        Err(e) => (Vec::new(), format!("unreadable: {e}")),
    }
}

/// [`tickle`] over leads already read ([`leads_of`]): the direct probe, then the same probe through
/// the leads and the caller's carriers.
pub async fn tickle_with<C: TickleCalls + ?Sized>(node: &crate::model::Node, view: &crate::topology::Topology, calls: &C, (leads, leads_named): (Vec<crate::model::Node>, String)) -> Result<Tickle, String> {
    use futures_util::StreamExt as _;
    use tracing::Instrument as _;
    let Some(incarnation) = node.incarnation_id.clone() else {
        return Err(no_birth(node));
    };
    let caller = calls.caller();
    let deadline_ms = calls.deadline().as_millis() as i64;
    // Every probe names the same exact birth (node-tickle.md "Target").
    let probe = rdm_core::status::StatusRequest::ProbeNodeState { node_id: node.node_id.clone(), incarnation: incarnation.clone() };
    // The probe's span carries its answer or its failure (node-tickle.md "Observability contract").
    let said = |span: &tracing::Span, end: &ProbeEnd| {
        match end {
            ProbeEnd::Reply(r) => span.record("answer", r.name()),
            other => span.record("failure", other.name().as_str()),
        };
    };
    let span = tracing::info_span!("rdm.node.tickle.resolve.via-direct", caller = %caller, node = %node.name, node_id = %node.node_id, incarnation = %incarnation.as_str(), deadline_ms, leads = %leads_named, answer = tracing::field::Empty, failure = tracing::field::Empty, "otel.kind" = "internal");
    let direct = calls.direct(node, &probe).instrument(span.clone()).await;
    said(&span, &direct);
    if let ProbeEnd::Reply(answer) = &direct {
        if birth_answered(Some(answer), &node.node_id, &incarnation) {
            if calls.command_unanswered(node) {
                span.in_scope(|| tracing::info!("the birth answered the direct probe but left a command unanswered past the staleness floor: not reached (Q182)"));
            } else {
                span.in_scope(|| tracing::info!("the birth answered the direct probe: reached"));
                return Ok(Tickle::Reached { answer: answer.clone(), carrier: None });
            }
        }
    }
    drop(span);
    // The leads first, then the caller's eligible carriers not already among them, within the fan-out.
    let mut carriers: Vec<crate::model::Node> = leads.clone();
    for c in calls.carriers(view, node) {
        if !carriers.iter().any(|l| l.node_id == c.node_id) {
            carriers.push(c);
        }
    }
    carriers.truncate(VIA_PEER_TICKLE_FANOUT);
    let (node_id, probe, caller, said, leads) = (&node.node_id, &probe, &caller, &said, &leads);
    let mut running: futures_util::stream::FuturesUnordered<_> = carriers
        .iter()
        .enumerate()
        .map(|(i, carrier)| async move {
            let lead = leads.iter().any(|l| l.node_id == carrier.node_id);
            let span = tracing::info_span!("rdm.node.tickle.resolve.via-peer", caller = %caller, node = %node.name, node_id = %node.node_id, carrier = %carrier.name, lead, answer = tracing::field::Empty, failure = tracing::field::Empty, "otel.kind" = "internal");
            let end = calls.via_peer(carrier, node, probe).instrument(span.clone()).await;
            said(&span, &end);
            (i, carrier.name.to_string(), end, span)
        })
        .collect();
    let mut peers: Vec<(usize, PeerProbe)> = Vec::new();
    while let Some((i, carrier, end, span)) = running.next().await {
        if let ProbeEnd::Reply(answer) = &end {
            if birth_answered(Some(answer), node_id, &incarnation) {
                if calls.command_unanswered(node) {
                    span.in_scope(|| tracing::info!(carrier = %carrier, "the birth answered through the carrier but left a command unanswered past the staleness floor: not reached (Q182)"));
                } else {
                    span.in_scope(|| tracing::info!(carrier = %carrier, "the birth answered through the carrier: reached, the other probes cancelled"));
                    // Dropping `running` cancels the probes still running (step 4).
                    return Ok(Tickle::Reached { answer: answer.clone(), carrier: Some(carrier) });
                }
            }
        }
        peers.push((i, PeerProbe { carrier, end }));
    }
    peers.sort_by_key(|(i, _)| *i);
    let peers: Vec<PeerProbe> = peers.into_iter().map(|(_, p)| p).collect();
    let asked = peers.iter().map(|p| p.carrier.as_str()).collect::<Vec<_>>().join(",");
    // The ladder's result when none of its probes carried the answer; the caller records what it
    // does next on it (`Unreached::next`), and it ends with the result.
    let span = tracing::info_span!("rdm.node.tickle.returns-unknown.via-unreached", caller = %caller, node = %node.name, node_id = %node.node_id, incarnation = %incarnation.as_str(), direct = %direct.name(), carriers = %asked, leads = %leads_named, next = tracing::field::Empty, "otel.kind" = "internal");
    span.in_scope(|| tracing::info!("nobody reached the birth within the ladder: unreached, never death"));
    Ok(Tickle::Unreached(Unreached { direct, peers, span }))
}

/// What one [`OfflineTickle::tick`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct OfflineTickReport {
    pub tickled: Vec<String>,
    pub reachable_silent: Vec<String>,
    /// Of `reachable_silent`, the births whose answer was `Current`: held off their mesh, they are
    /// commanded back through node.mesh.join by their authority (node-tickle.md "What follows", Q87).
    pub answered_current: Vec<String>,
    pub offline: Vec<String>,
    /// Silent nodes heard again: the view holds them live.
    pub returned: Vec<String>,
    /// Silent nodes the view no longer holds (deleted, or recovered under a new node.id at their
    /// path.name): not heard again, gone.
    pub gone: Vec<String>,
}

/// How one ladder of the ladder ended (node-recovery.md step 2): the tickle's result, or no ladder run.
#[derive(Debug)]
enum LadderEnd {
    /// The exact birth answered the tickle (`reached`): its answer's name, whether it was `Current`,
    /// and the carrier that carried it (`None`: direct).
    Reached { answer: &'static str, current: bool, carrier: Option<String> },
    /// Nobody reached the birth: how the direct probe ended, the carriers asked, and the instant on
    /// the tick clock the ladder ended (`verdict_at_ms`: the ladder's probes ended).
    Unreached { direct: String, asked: String, verdict_at_ms: u64 },
    /// No ladder ran: the birth could not be probed, by name (no incarnation, no client).
    NotRun(String),
}

/// What one birth's investigation in the fold returned (Q273): the provider's proof that its runtime is
/// gone, the inspect's verdict named, with no tickle sent; or the ladder's result.
#[derive(Debug)]
pub enum Ladder {
    /// The inspect proved the runtime exited or os-stopped: its verdict's name.
    Gone(String),
    /// The ladder ran: its result.
    Tickled(Result<Tickle, String>),
}

/// One ladder of the ladder, running beside the loop: started by one tick, folded by a later one.
#[derive(Debug)]
struct LadderRun {
    /// The clock of the tick that started it, named on the ladder's TRUE OFFLINE span beside the
    /// ladder's verdict (`LadderEnd::Unreached::verdict_at_ms`).
    started_at_ms: u64,
    path: String,
    /// The mark (or first sighting) the ladder ran on, named on its spans.
    seen_at: u64,
    /// `None`: the ladder was cancelled at a stop.
    task: tokio::task::JoinHandle<Option<LadderEnd>>,
}

/// The exact birth a ladder runs for: node id and incarnation.
type Birth = (String, Option<crate::model::IncarnationId>);

#[derive(Default)]
pub(crate) struct OfflineTickle {
    /// Every node held as silent: node id -> the instant (ms) it was first seen silent. Kept
    /// whether or not this node-admin holds the role.
    first_seen: HashMap<String, u64>,
    /// Silent nodes whose tickle has settled (answered, or true offline): not tickled again until
    /// they leave and re-enter the silent set.
    settled: HashSet<String>,
    /// The nodes held true offline now, by node id.
    offline: HashSet<String>,
    /// The ladder in flight for each exact birth, at most one: a birth whose ladder runs is not
    /// tickled again until a tick folds the ladder.
    running: HashMap<Birth, LadderRun>,
    /// This node-admin's path.name, named on every span it emits: a role handed over mid-window is
    /// read from which admin ran each ladder.
    observer: String,
    /// The admin's stop gate: a stop deletes the ladders in flight.
    gate: crate::stop_gate::StopGate,
}

impl Drop for OfflineTickle {
    /// The ladders are the tickle's: their probes end with it.
    fn drop(&mut self) {
        for ladder in self.running.values() {
            ladder.task.abort();
        }
    }
}

impl OfflineTickle {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::default()
    }

    /// The tickle of node-admin `observer` (its path.name), whose ladders a stop of `gate` deletes.
    pub fn observed_by(observer: String, gate: crate::stop_gate::StopGate) -> Self {
        let mut tickle = Self::default();
        tickle.observer = observer;
        tickle.gate = gate;
        tickle
    }

    /// Is this node held true offline?
    #[cfg(test)]
    pub(crate) fn is_offline(&self, id: &str) -> bool {
        self.offline.contains(id)
    }

    /// How many ladders are in flight, and how many of them have ended (not yet folded).
    #[cfg(test)]
    pub(crate) fn ladders(&self) -> (usize, usize) {
        (self.running.len(), self.running.values().filter(|r| r.task.is_finished()).count())
    }

    /// One tick. `holds_role` is this node-admin's primary role, sampled once for the tick;
    /// `silent` is this tick's silent set (every `PendingReconnect` node it watches); `in_view` says
    /// whether the tick still watches a node id at all, in its view or through the durable rows that
    /// name a birth not yet in it (a node that left the silent set is heard again only if it does); `now_ms` is the tick's start instant, and an instant inside the tick
    /// (a verdict that came back after awaited probes) is `now_ms` plus the time the tick has run;
    /// `probe_delay_ms`
    /// is how long a node stays silent before its one tickle; `tickle` runs the one ladder ([`tickle`],
    /// node-tickle.md) over the birth. `observe` reads iroh's local view of a node once for the reachable-silent and
    /// the TRUE OFFLINE span (`rdm_transport::iroh_obs`); it decides nothing.
    ///
    /// A tick waits on no probe (node-recovery.md: one loop, on every gossip interval). Each ladder it
    /// starts runs beside the loop, at once for every member due, so each member's connect is made
    /// half a floor after its own mark (step 2), whatever another member's connect spends; the first
    /// tick after a ladder ends folds what it found, and nothing is decided before that. A ladder ends
    /// with its birth's silence and with the role: its probes are dropped and nothing of it is folded.
    #[allow(clippy::too_many_arguments)]
    pub async fn tick<P, T, TF, O, OF>(
        &mut self,
        now_ms: u64,
        holds_role: bool,
        silent: Vec<SilentNode>,
        in_view: P,
        probe_delay_ms: u64,
        tickle: T,
        observe: O,
    ) -> OfflineTickReport
    where
        P: Fn(&str) -> bool,
        T: Fn(String) -> TF,
        TF: std::future::Future<Output = Ladder> + Send + 'static,
        O: Fn(String) -> OF,
        OF: std::future::Future<Output = rdm_transport::iroh_obs::IrohObservation>,
    {
        let began = tokio::time::Instant::now();
        let mut report = OfflineTickReport::default();
        let silent_ids: HashSet<String> = silent.iter().map(|n| n.node_id.clone()).collect();
        // A birth this tick does not list silent (heard again, gone, or another birth now at its node
        // id), and every birth while this admin does not hold the role: its ladder's probes are
        // dropped (step 2: an answer "cancels the probes still running"; a new primary runs its own).
        let births: HashSet<Birth> = silent.iter().map(|n| (n.node_id.clone(), n.incarnation.clone())).collect();
        self.running.retain(|birth, ladder| {
            let keep = holds_role && births.contains(birth);
            if !keep {
                ladder.task.abort();
            }
            keep
        });
        let left: Vec<String> = self.first_seen.keys().filter(|id| !silent_ids.contains(*id)).cloned().collect();
        self.first_seen.retain(|id, _| silent_ids.contains(id));
        self.offline.retain(|id| silent_ids.contains(id));
        for id in left {
            // Heard again while the tick still watches the node (its view, or the durable rows that
            // name a birth not yet in its view); a node id it no longer watches was deleted (or
            // recovered under a new node.id at its path.name): gone.
            if in_view(&id) {
                tracing::info_span!("rdm.node_admin.node.update.via-offline-returned", node_id = %id, observer = %self.observer, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!(node_id = %id, "a silent node is heard again: its tickle and its status end"));
                report.returned.push(id);
            } else {
                tracing::info_span!("rdm.node_admin.node.remove.via-offline-gone", node_id = %id, observer = %self.observer, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!(node_id = %id, "a silent node left the view: its tickle and its status end, it was not heard again"));
                report.gone.push(id);
            }
        }
        if holds_role {
            self.settled.retain(|id| silent_ids.contains(id));
        } else {
            self.settled.clear();
        }
        // The window runs from the mark the admin's view holds for the birth (node-recovery.md step 2:
        // "Half a floor after the mark"); a birth with no mark is first seen at this tick.
        for n in &silent {
            self.first_seen.entry(n.node_id.clone()).or_insert(n.marked_at_ms.unwrap_or(now_ms));
        }
        if !holds_role {
            return report;
        }
        // Every ladder that ended since the last tick, folded in the order the silent set lists its birth.
        for n in &silent {
            let birth = (n.node_id.clone(), n.incarnation.clone());
            if !self.running.get(&birth).is_some_and(|r| r.task.is_finished()) {
                continue;
            }
            let Some(LadderRun { started_at_ms, path, seen_at, task }) = self.running.remove(&birth) else { continue };
            // A ladder that panicked takes the loop with it, as one run inside the pass did. A ladder
            // cancelled at a stop found nothing: nothing is folded, and the birth is due again.
            let end = match task.await {
                Ok(Some(end)) => end,
                Ok(None) => continue,
                Err(e) => std::panic::resume_unwind(e.into_panic()),
            };
            let id = n.node_id.clone();
            match end {
                LadderEnd::Reached { answer, current, carrier } => {
                    // An answer is reachable-silent: recorded (node-recovery.md step 2). The tickle's own
                    // span carried the answer; this is the mesh-primary's record of it.
                    self.offline.remove(&id);
                    self.settled.insert(id.clone());
                    let seen = observe(id.clone()).await;
                    let carrier = carrier.unwrap_or_default();
                    tracing::info_span!(
                        "rdm.node_admin.node.update.via-reachable-silent",
                        node_id = %id,
                        path = %path,
                        answer,
                        carrier = %carrier,
                        observer = %self.observer,
                        first_seen_ms = seen_at as i64,
                        iroh_known_addrs = %seen.known_addrs,
                        iroh_active_addrs = %seen.active_addrs,
                        "otel.kind" = "internal"
                    )
                    .in_scope(|| tracing::info!(node_id = %id, answer, carrier = %carrier, "the tickle reached the birth: reachable-silent, recorded, no action"));
                    if current {
                        report.answered_current.push(id.clone());
                    }
                    report.reachable_silent.push(id);
                }
                LadderEnd::NotRun(reason) => {
                    tracing::info_span!("rdm.node_admin.node.reject.via-no-birth", node_id = %id, path = %path, error = %reason, observer = %self.observer, "otel.kind" = "internal")
                        .in_scope(|| tracing::warn!(node_id = %id, error = %reason, "no ladder ran over the birth: no status change"));
                }
                LadderEnd::Unreached { direct: e, asked, verdict_at_ms } => {
                    // Nobody reached the birth: the ladder is the investigation's reach, and the birth is
                    // held offline at this fold, so its inspection and recovery follow on this tick
                    // (node-recovery.md steps 2-3: "Any other verdict ...: the recovery below, at once.
                    // Nothing holds, nothing is inspected again"; Q111, Q181). It is not probed again.
                    self.settled.insert(id.clone());
                    if self.offline.insert(id.clone()) {
                        let seen = observe(id.clone()).await;
                        tracing::info_span!(
                            "rdm.node_admin.node.update.via-true-offline",
                            node_id = %id,
                            path = %path,
                            error = %e,
                            asked = %asked,
                            iroh_known_addrs = %seen.known_addrs,
                            iroh_active_addrs = %seen.active_addrs,
                            no_path_at_ms = verdict_at_ms as i64,
                            ladder_began_ms = started_at_ms as i64,
                            observer = %self.observer,
                            first_seen_ms = seen_at as i64,
                            "otel.kind" = "internal",
                        )
                        .in_scope(|| tracing::info!(node_id = %id, error = %e, "nobody reached the birth on its one ladder: TRUE OFFLINE, its inspection and recovery follow at once"));
                        report.offline.push(id);
                    }
                }
            }
        }
        for SilentNode { node_id: id, path, incarnation, .. } in silent {
            // The mark (or, with none, the instant this admin first saw the node silent): a role taken
            // mid-window tickles on it.
            let seen_at = self.first_seen.get(&id).copied().unwrap_or(now_ms);
            if now_ms.saturating_sub(seen_at) < probe_delay_ms || self.settled.contains(&id) {
                continue;
            }
            let birth = (id.clone(), incarnation);
            if self.running.contains_key(&birth) {
                continue;
            }
            report.tickled.push(id.clone());
            // What this ladder's `unreached` is followed by (Q273): the verdict, the inspect having run
            // before the tickles.
            let next = "verdict";
            // The ladder: the one ladder over the birth (`tickle`). It takes no lock and waits on
            // nothing of this admin's; a stop cancels it where it stands
            // (`StopGate::until_stopped`), and it never resumes.
            let ladder = tickle(id.clone());
            // The verdict's instant on the tick clock: this tick's `now_ms` plus the time since it began.
            let (tick_ms, tick_began) = (now_ms, began);
            let (cancelled_id, cancelled_path, cancelled_by) = (id.clone(), path.clone(), self.observer.clone());
            let run = self.gate.until_stopped(async move {
                match ladder.await {
                    // The inspect proved the runtime gone: no tickle was sent, and the birth is held offline
                    // as an unreached one is, its recovery following at once.
                    Ladder::Gone(verdict) => LadderEnd::Unreached { direct: format!("not probed: node.runtime.inspect answered {verdict}"), asked: String::new(), verdict_at_ms: tick_ms + tick_began.elapsed().as_millis() as u64 },
                    Ladder::Tickled(Ok(Tickle::Reached { answer, carrier })) => LadderEnd::Reached { answer: answer.name(), current: matches!(answer, rdm_core::status::StatusReply::Current { .. }), carrier },
                    Ladder::Tickled(Ok(Tickle::Unreached(unreached))) => {
                        unreached.next(next);
                        let verdict_at_ms = tick_ms + tick_began.elapsed().as_millis() as u64;
                        LadderEnd::Unreached { direct: unreached.direct.name(), asked: unreached.carriers(), verdict_at_ms }
                    }
                    Ladder::Tickled(Err(reason)) => LadderEnd::NotRun(reason),
                }
            });
            let task = tokio::spawn(async move {
                let end = run.await;
                if end.is_none() {
                    tracing::info!(node_id = %cancelled_id, path = %cancelled_path, observer = %cancelled_by, "the tickle ladder was cancelled at the stop: it proves nothing, and the birth is due again");
                }
                end
            });
            self.running.insert(birth, LadderRun { started_at_ms: now_ms, path, seen_at, task });
        }
        report
    }
}

/// The recovery of a TRUE OFFLINE birth (`follow`: the provider's proof, the submit, and the Build
/// followed to its end, `RecoverySubmitter::recover_if_proven`), run beside the mesh-primary's loop.
/// A stop cancels it where it stands, the view lock it holds or waits on included, and does not
/// wait for it (`StopGate::until_stopped`); a submit it already made is answered AlreadyApplied or
/// BuildInProgress when it comes again. The birth leaves `in_flight` either way, so the first tick
/// that finds it due again, after a start or at the role's next primary, submits it again.
pub(crate) fn spawn_recovery_follower<F>(gate: &crate::stop_gate::StopGate, in_flight: std::sync::Arc<std::sync::Mutex<HashSet<(crate::model::NodeId, crate::model::IncarnationId)>>>, birth: (crate::model::NodeId, crate::model::IncarnationId), follow: F) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let follow = gate.until_stopped(follow);
    tokio::spawn(async move {
        if follow.await.is_none() {
            tracing::info!(node_id = %birth.0, incarnation = %birth.1 .as_str(), "the recovery follower was cancelled at the stop: the next tick that finds the birth due submits it again");
        }
        in_flight.lock().unwrap().remove(&birth);
    })
}

/// Write the status the mesh primary holds for a silent birth (`PendingReconnect`, `Dead`, or
/// `None` once it is heard again) on that birth's facts in its node record: the record the writer
/// holds, the mark set or cleared, written whole (storage-whole-records.md). A held record with no
/// incarnation yet has no birth to keep the mark on: that is named, never a silent skip.
///
/// Each refusal is its own span, named where it happened: `via-no-birth` for a record with no
/// incarnation, `via-nodes-storage` (the birth, the status, the store's own cause) for a write the
/// store refused. Nothing is written again from here: the caller's memo still holds the last status
/// that stands, and node-recovery.md's next step (the provider's `Exited`) does not wait on this
/// write ("Dead is observer-inferred and is never itself a reason to act").
pub(crate) async fn put_silence_status(store: &dyn crate::storage::NodesStorage, written: &crate::node_rows::WrittenRows, node: &crate::model::Node, status: Option<crate::model::NodeStatus>) -> Result<bool, String> {
    let word = silence_word(status);
    let Some(incarnation_id) = node.incarnation_id.clone() else {
        let why = format!("{} ({}) holds no incarnation: no birth to keep its silence mark on", node.name, node.node_id);
        tracing::info_span!("rdm.node_admin.node.reject.via-no-birth", node = %node.name, node_id = %node.node_id, status = word, store = "nodes.storage", error = %why)
            .in_scope(|| tracing::info!("the node's silence status names no birth: nothing is written"));
        return Err(why);
    };
    let base = crate::storage::NodeRow::new(node.name.clone(), node.node_id.clone(), node.status);
    let marked = written.update(store, &node.node_id, Some(base), |r| r.birth_mut(&incarnation_id, &node.name).silence = status).await;
    if let Err(e) = marked {
        tracing::info_span!("rdm.node_admin.node.reject.via-nodes-storage", node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation_id.as_str(), status = word, store = "nodes.storage", error = %e)
            .in_scope(|| tracing::info!("nodes.storage refused the birth's silence mark"));
        return Err(format!("the record of {} ({}) with its silence mark at {} ({word}): {e}", node.name, node.node_id, incarnation_id.as_str()));
    }
    Ok(true)
}

/// The silence status as a span names it: the view's own word, or `cleared` for a birth heard again.
fn silence_word(status: Option<crate::model::NodeStatus>) -> &'static str {
    use crate::model::NodeStatus::*;
    match status {
        None => "cleared",
        Some(Pending) => "pending",
        Some(Ready) => "ready",
        Some(ConnectionsDeleting) => "connections-deleting",
        Some(Leaving) => "leaving",
        Some(PendingReconnect) => "pending-reconnect",
        Some(Reserved5) => "reserved-5",
        Some(Dead) => "dead",
        Some(Deleted) => "deleted",
        Some(Withdrawn) => "withdrawn",
        Some(Starting) => "starting",
        Some(ConnectionsDeleted) => "connections-deleted",
        Some(Stopping) => "stopping",
        Some(Stopped) => "stopped",
        Some(Deleting) => "deleting",
        Some(Left) => "left",
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// CONTRACT (node-tickle.md step 1; i144 R9): a command its birth never received for a
    /// reachability reason marks the birth `PendingReconnect` beside a live status, and never over a
    /// lifecycle state the node reported and its mesh-primary recorded (stopping, connections-deleting).
    // @feature: node-lifecycle
    #[test]
    fn a_pending_reconnect_mark_never_overwrites_an_admitted_stopping() {
        use rdm_core::MemberStatus as M;
        for live in [None, Some(M::Pending), Some(M::Ready)] {
            assert!(marks_over(live), "{live:?}: marked");
        }
        for lifecycle in [M::Leaving, M::ConnectionsDeleting] {
            assert!(!marks_over(Some(lifecycle)), "{lifecycle:?}: kept, never overwritten");
        }
    }

    const FLOOR: u64 = 30_000;
    const PROBE_DELAY: u64 = FLOOR / 2;

    fn silent(id: &str) -> Vec<SilentNode> {
        vec![SilentNode { node_id: id.into(), path: format!("mesh1.rpc.{id}"), incarnation: None, marked_at_ms: None }]
    }

    /// How a cell says the via-peer rung of a ladder ended, when the direct probe did not answer.
    #[derive(Debug, Clone, PartialEq)]
    enum ViaPeerVerdict {
        /// A peer carried the birth's answer.
        Answered { via: String },
        /// No carrier reached the birth: `unreached`, naming the carriers asked.
        NoPath { asked: Vec<String> },
        /// No ladder ran, by name.
        CandidatesUnreadable(String),
    }

    fn current_answer() -> rdm_core::status::StatusReply {
        rdm_core::status::StatusReply::Current { node_id: crate::model::NodeId::mint(), incarnation: crate::model::IncarnationId::mint(), state: rdm_core::status::NodeState::Ready }
    }

    /// An `unreached` over `asked` carriers: the direct probe and every carried one ended `NotSent`.
    fn unreached(asked: &[&str]) -> Tickle {
        Tickle::Unreached(Unreached::of(ProbeEnd::Other { end: "NotSent", detail: "no answer".into() }, asked.iter().map(|c| PeerProbe { carrier: c.to_string(), end: ProbeEnd::Other { end: "NotSent", detail: "deadline".into() } }).collect()))
    }

    /// The one ladder's result as a cell says it: the direct probe answered (`direct`), or the
    /// via-peer rung's end.
    fn ladder_end(direct: bool, via: &ViaPeerVerdict) -> Result<Tickle, String> {
        if direct {
            return Ok(Tickle::Reached { answer: current_answer(), carrier: None });
        }
        match via {
            ViaPeerVerdict::Answered { via } => Ok(Tickle::Reached { answer: current_answer(), carrier: Some(via.clone()) }),
            ViaPeerVerdict::NoPath { asked } => Ok(unreached(&asked.iter().map(String::as_str).collect::<Vec<_>>())),
            ViaPeerVerdict::CandidatesUnreadable(reason) => Err(reason.clone()),
        }
    }

    async fn tick(t: &mut OfflineTickle, now: u64, role: bool, set: Vec<SilentNode>, direct: bool, via: ViaPeerVerdict) -> OfflineTickReport {
        tick_observed(t, now, role, set, direct, via, &std::sync::atomic::AtomicUsize::new(0)).await
    }

    /// `tick`, counting every iroh observation read in `reads`. One ladder as the loop runs it, at
    /// one clock reading `now`: the tick that starts what is due, the ladders it started run to their
    /// end beside it (every probe here answers at once), and the next tick, which folds them. The
    /// two ticks' reports, joined in order.
    async fn tick_observed(t: &mut OfflineTickle, now: u64, role: bool, set: Vec<SilentNode>, direct: bool, via: ViaPeerVerdict, reads: &std::sync::atomic::AtomicUsize) -> OfflineTickReport {
        let once = async |t: &mut OfflineTickle| {
            t.tick(
                now,
                role,
                set.clone(),
                |_| true,
                PROBE_DELAY,
                |_| {
                    let end = crate::offline::Ladder::Tickled(ladder_end(direct, &via));
                    async move { end }
                },
                |_| {
                    reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    async { rdm_transport::iroh_obs::IrohObservation::unavailable() }
                },
            )
            .await
        };
        let mut report = once(t).await;
        while t.ladders().1 < t.ladders().0 {
            tokio::task::yield_now().await;
        }
        let folded = once(t).await;
        report.tickled.extend(folded.tickled);
        report.reachable_silent.extend(folded.reachable_silent);
        report.answered_current.extend(folded.answered_current);
        report.offline.extend(folded.offline);
        report.returned.extend(folded.returned);
        report.gone.extend(folded.gone);
        report
    }

    /// CONTRACT (A1, diagnostics only): iroh is read exactly once per answered or failed ladder event,
    /// never on a tick that records neither, and the reports are the tickle's own: the answered node
    /// is reachable-silent and the one ladder nobody answered is TRUE OFFLINE (node-recovery.md step 3,
    /// Q181).
    #[tokio::test(start_paused = true)]
    async fn iroh_is_read_once_per_answered_or_failed_event_and_decides_nothing() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let reads = AtomicUsize::new(0);
        let mut t = OfflineTickle::new();
        assert!(tick_observed(&mut t, 0, true, silent("a"), true, no_path(), &reads).await.tickled.is_empty());
        assert_eq!(reads.load(Ordering::SeqCst), 0, "no event, no read");
        let r = tick_observed(&mut t, PROBE_DELAY, true, silent("a"), true, no_path(), &reads).await;
        assert_eq!(r.reachable_silent, vec!["a".to_string()]);
        assert_eq!(reads.load(Ordering::SeqCst), 1, "one read for the answered event");

        let reads = AtomicUsize::new(0);
        let mut t = OfflineTickle::new();
        tick_observed(&mut t, 0, true, silent("b"), false, no_path(), &reads).await;
        let r1 = tick_observed(&mut t, PROBE_DELAY, true, silent("b"), false, no_path(), &reads).await;
        assert_eq!(r1.offline, vec!["b".to_string()], "the ladder nobody answered is TRUE OFFLINE, as without the read");
        assert_eq!(reads.load(Ordering::SeqCst), 1, "one read for the failed event");
        let r2 = tick_observed(&mut t, PROBE_DELAY + FLOOR, true, silent("b"), false, no_path(), &reads).await;
        assert!(r2.tickled.is_empty() && r2.offline.is_empty(), "settled: no second ladder, no second event: {r2:?}");
        assert_eq!(reads.load(Ordering::SeqCst), 1, "no read on a tick that records nothing");
    }

    mod cadence {
        //! The recovery tick's cadence (node-recovery.md "Detection and proof": "The mesh-primary
        //! runs one loop for its own members, on every gossip interval"), under paused time: each
        //! pass spends a known time, and the clock moves only while the loop waits.
        use super::super::Cadence;
        use std::time::Duration;
        use tokio::time::{sleep, Instant};

        /// The gate's gossip interval (`RDM_GOSSIP_INTERVAL_MS=500`).
        const INTERVAL: Duration = Duration::from_millis(500);

        /// Run one pass per spent time in `work`, each followed by the cadence's wait, and return
        /// the instant each pass started, measured from the loop's start.
        async fn pass_starts(work: &[Duration]) -> Vec<Duration> {
            let start = Instant::now();
            let mut cadence = Cadence::every(INTERVAL).expect("a non-zero interval");
            let mut starts = Vec::new();
            for spent in work {
                starts.push(start.elapsed());
                sleep(*spent).await;
                cadence.next().await;
            }
            starts
        }

        fn ms(v: &[u64]) -> Vec<Duration> {
            v.iter().map(|m| Duration::from_millis(*m)).collect()
        }

        /// CONTRACT (node-recovery.md "Detection and proof", on every gossip interval): a pass that
        /// spends 100 ms leaves the next pass on the interval's grid, starting 500 ms after the
        /// previous pass STARTED: 0, 500, 1000 ms. Must NOT happen: the next pass starting one
        /// interval after the previous pass ENDED (0, 600, 1200 ms), its own work added to each tick.
        #[tokio::test(start_paused = true)]
        async fn the_recovery_tick_starts_every_pass_on_the_gossip_interval_grid() {
            let w = Duration::from_millis(100);
            assert_eq!(pass_starts(&[w, w, w]).await, ms(&[0, 500, 1000]), "each pass starts one interval after the previous one started");
        }

        /// CONTRACT: a zero gossip interval is refused by name, never a loop that spins or a
        /// panic inside the timer.
        #[tokio::test(start_paused = true)]
        async fn a_zero_gossip_interval_is_refused_by_name() {
            let refused = Cadence::every(Duration::ZERO).err().expect("a zero interval is refused");
            assert!(refused.contains("RDM_GOSSIP_INTERVAL_MS") && refused.contains("0 ms"), "{refused}");
        }

        /// CONTRACT (a late tick does not burst, and the grid holds): a pass that spends 1200 ms, past
        /// two grid points (an inline tickle ladder spends whole call budgets), is followed by the one
        /// pass it owes at once (1200 ms), then the passes return to the loop's own grid (1500,
        /// 2000 ms). Must NOT happen: the two missed ticks run back to back (a burst: 1200, 1300,
        /// 1500), the grid moved to the late pass (1200, 1700, 2200), or the interval added after
        /// the pass's work (1700, 2300, 2900).
        #[tokio::test(start_paused = true)]
        async fn a_pass_past_the_interval_owes_one_pass_then_the_tick_returns_to_its_grid() {
            let (long, w) = (Duration::from_millis(1200), Duration::from_millis(100));
            assert_eq!(pass_starts(&[long, w, w, w]).await, ms(&[0, 1200, 1500, 2000]), "one owed pass at once, then the loop's own grid");
        }
    }

    mod ladder {
        //! The tickle ladder beside the loop (node-recovery.md "Detection and proof": "The
        //! mesh-primary runs one loop for its own members, on every gossip interval"; step 2: "Half
        //! a floor after the mark it makes one QUIC connect"), under paused time: a direct connect to
        //! an exited runtime spends a known time, and the clock moves only while every task waits.
        use super::*;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use tokio::time::{sleep, Instant};

        /// What a direct connect to an exited runtime spends at the gate's cadence: the dial ends at
        /// the QUIC idle timeout, the 3 s staleness floor (production's ends at the 10 s call budget).
        const DIAL: Duration = Duration::from_secs(3);

        /// The gate's gossip interval, the loop's tick.
        const INTERVAL: Duration = Duration::from_millis(500);

        fn marked(id: &str) -> SilentNode {
            SilentNode { node_id: id.into(), path: format!("mesh1.rpc.{id}"), incarnation: Some(crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60(&format!("inc-of-{id}"))).unwrap()), marked_at_ms: Some(0) }
        }

        /// CONTRACT (node-recovery.md step 2, per member, and the one loop on every gossip interval):
        /// two members marked at the same instant each get their direct connect half a floor after the
        /// mark, at once, and the pass that starts them waits on neither connect. Their outcomes
        /// are folded by a later tick: both ladders ended with no path, so both births are held offline
        /// at that fold. Must NOT happen: the second member's connect waiting
        /// for the first one's to end (3 s late at the gate's cadence, a whole 10 s call budget at
        /// production's), or a pass that lasts as long as the ladders it starts.
        #[tokio::test(start_paused = true)]
        async fn members_marked_together_are_each_tickled_half_a_floor_after_the_mark_at_once() {
            let start = Instant::now();
            let connects: Arc<Mutex<Vec<(String, Duration)>>> = Arc::default();
            let mut t = OfflineTickle::new();
            let tick = async |t: &mut OfflineTickle, now: u64| {
                t.tick(
                    now,
                    true,
                    vec![marked("a"), marked("b")],
                    |_| true,
                    PROBE_DELAY,
                    |id| {
                        let connects = connects.clone();
                        async move {
                            connects.lock().unwrap().push((id, start.elapsed()));
                            sleep(DIAL).await;
                            crate::offline::Ladder::Tickled(Ok(unreached(&["mesh1.rpc.2"])))
                        }
                    },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await
            };
            let started = tick(&mut t, PROBE_DELAY).await;
            let pass = start.elapsed();
            sleep(DIAL).await;
            assert_eq!(*connects.lock().unwrap(), vec![("a".to_string(), Duration::ZERO), ("b".to_string(), Duration::ZERO)], "both direct connects at half a floor after the mark, at once");
            assert_eq!(pass, Duration::ZERO, "the pass waits on no connect");
            assert_eq!(started.tickled, vec!["a".to_string(), "b".to_string()]);
            // The next tick on the gossip interval's grid after both ladders ended folds them.
            sleep(INTERVAL).await;
            let folded = tick(&mut t, PROBE_DELAY + (DIAL + INTERVAL).as_millis() as u64).await;
            assert!(folded.tickled.is_empty(), "a birth whose ladder was folded with no path is not probed again");
            assert_eq!(folded.offline, vec!["a".to_string(), "b".to_string()], "both ladders ended with no path: both are TRUE OFFLINE at the fold");
        }

        /// CONTRACT (node-stop deletes the work the loop runs beside it; the lead's decision: "a stop
        /// cancels it as part of retiring the birth, and does not wait for it"): a stop while a
        /// tickle ladder and a recovery follower are in flight ends both at the stop, with no time
        /// passing. The follower, queued on the view lock a handler holds, leaves that queue, and its
        /// birth leaves `in_flight`, so the first tick after a start submits again; neither goes on
        /// when the gate opens again, and the next tick folds nothing of the cancelled ladder and starts
        /// it again. Must NOT happen: a ladder's probe or a follower running on past the stop, one held
        /// at the gate while it waits on the view lock, or one resumed by the start.
        #[tokio::test(start_paused = true)]
        async fn a_stops_gate_cancels_the_tickle_ladder_and_the_recovery_follower_in_flight() {
            use crate::model::{IncarnationId, NodeId};
            use std::collections::HashSet;
            use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
            let gate = crate::stop_gate::StopGate::new();
            let ended = Arc::new(AtomicUsize::new(0));
            let mut t = OfflineTickle::observed_by("mesh1.admin.1".into(), gate.clone());
            let tick = async |t: &mut OfflineTickle, now: u64| {
                t.tick(
                    now,
                    true,
                    vec![marked("a")],
                    |_| true,
                    PROBE_DELAY,
                    |_| {
                        let ended = ended.clone();
                        async move {
                            sleep(DIAL).await;
                            ended.fetch_add(1, Ordering::SeqCst);
                            crate::offline::Ladder::Tickled(Ok(unreached(&["mesh1.rpc.2"])))
                        }
                    },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await
            };
            assert_eq!(tick(&mut t, PROBE_DELAY).await.tickled, vec!["a".to_string()]);
            // The follower, queued on the view lock, which a handler holds for write.
            let view = Arc::new(tokio::sync::RwLock::new(()));
            let held = view.clone().write_owned().await;
            let read = Arc::new(AtomicBool::new(false));
            let in_flight: Arc<Mutex<HashSet<(NodeId, IncarnationId)>>> = Arc::default();
            let birth = (NodeId::mint(), IncarnationId::parse(&rdm_core::fixture_crockford60("inc-of-b")).unwrap());
            in_flight.lock().unwrap().insert(birth.clone());
            let follower = {
                let (view, read) = (view.clone(), read.clone());
                spawn_recovery_follower(&gate, in_flight.clone(), birth, async move {
                    let _view = view.read().await;
                    read.store(true, Ordering::SeqCst);
                })
            };
            tokio::task::yield_now().await;
            assert_eq!(t.ladders(), (1, 0), "the ladder runs, at its direct connect");
            assert!(!follower.is_finished(), "the follower runs, queued on the view lock");
            let at = Instant::now();
            gate.quiesce().await;
            tokio::task::yield_now().await;
            assert_eq!(t.ladders(), (1, 1), "the ladder ended at the stop");
            assert!(follower.is_finished(), "the follower ended at the stop");
            assert!(in_flight.lock().unwrap().is_empty(), "its birth left in_flight: a tick after the start submits again");
            assert_eq!(at.elapsed(), Duration::ZERO, "the stop waited on neither");
            gate.resume();
            drop(held);
            sleep(DIAL + INTERVAL).await;
            assert_eq!(ended.load(Ordering::SeqCst), 0, "the ladder's probe never ran on to its end");
            assert!(!read.load(Ordering::SeqCst), "the follower never took the view lock: cancelled, not held");
            let again = tick(&mut t, PROBE_DELAY + (DIAL + INTERVAL).as_millis() as u64).await;
            assert_eq!(again.tickled, vec!["a".to_string()], "nothing is folded of the cancelled ladder: a's ladder starts again");
            assert!(again.offline.is_empty() && !t.is_offline("a"), "a cancelled ladder holds nothing offline");
        }

        /// CONTRACT (at most one ladder in flight per exact birth; step 2: an answer "cancels the
        /// probes still running"): the ticks that come while a birth's ladder runs never start another
        /// for it; a birth heard again has its ladder's probes dropped, never folded; another birth at
        /// the same node id is its own birth, its ladder starts and the old birth's is dropped. Must NOT
        /// happen: a second ladder for a birth whose first is in flight (a retry by tick), or a dropped
        /// ladder's probe running on to its end.
        #[tokio::test(start_paused = true)]
        async fn one_ladder_per_exact_birth_and_a_birth_that_leaves_the_silent_set_drops_its_probes() {
            use std::sync::atomic::{AtomicUsize, Ordering};
            let ended = Arc::new(AtomicUsize::new(0));
            let tick = async |t: &mut OfflineTickle, now: u64, set: Vec<SilentNode>| {
                t.tick(
                    now,
                    true,
                    set,
                    |_| true,
                    PROBE_DELAY,
                    |_| {
                        let ended = ended.clone();
                        async move {
                            sleep(DIAL).await;
                            ended.fetch_add(1, Ordering::SeqCst);
                            crate::offline::Ladder::Tickled(Ok(unreached(&["mesh1.rpc.2"])))
                        }
                    },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await
            };
            let mut t = OfflineTickle::new();
            assert_eq!(tick(&mut t, PROBE_DELAY, vec![marked("a")]).await.tickled, vec!["a".to_string()]);
            for k in 1..=5u64 {
                sleep(INTERVAL).await;
                assert!(tick(&mut t, PROBE_DELAY + k * 500, vec![marked("a")]).await.tickled.is_empty(), "tick {k}: the ladder of a's birth is in flight");
            }
            assert_eq!(t.ladders(), (1, 0), "one ladder, still running");
            let r = tick(&mut t, PROBE_DELAY + 2_500, vec![]).await;
            assert_eq!((r.returned, t.ladders()), (vec!["a".to_string()], (0, 0)), "heard again: its ladder is dropped");
            sleep(DIAL).await;
            assert_eq!(ended.load(Ordering::SeqCst), 0, "the dropped ladder's probe never ran to its end");

            let mut t = OfflineTickle::new();
            assert_eq!(tick(&mut t, PROBE_DELAY, vec![marked("b")]).await.tickled, vec!["b".to_string()]);
            let re_born = SilentNode { incarnation: Some(crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("inc-of-b-again")).unwrap()), ..marked("b") };
            sleep(INTERVAL).await;
            assert_eq!(tick(&mut t, PROBE_DELAY + 500, vec![re_born]).await.tickled, vec!["b".to_string()], "another birth at b's node id: its own ladder");
            assert_eq!(t.ladders(), (1, 0), "the old birth's ladder was dropped");
            sleep(DIAL + INTERVAL).await;
            assert_eq!(ended.load(Ordering::SeqCst), 1, "only the new birth's probe ran to its end");
        }
    }

    mod tickle {
        //! The one reach ladder, `node.tickle` (node-tickle.md; ruling R-SW1), under paused time: a
        //! probe's wait is its deadline, and the clock moves only when every probe waits.
        use super::super::*;
        use super::{FLOOR, PROBE_DELAY};
        use crate::model::{IncarnationId, Node, NodeId};
        use rdm_core::status::{NodeState, RetryOn, StatusReply, StatusRequest};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use tokio::time::{sleep, Instant};

        /// The default call budget each probe spends against a process that never answers.
        const BUDGET: Duration = Duration::from_secs(10);

        fn birth() -> (NodeId, IncarnationId) {
            (NodeId::mint(), IncarnationId::parse(&rdm_core::fixture_crockford60("inc-of-the-silent-node")).unwrap())
        }

        fn current(b: &(NodeId, IncarnationId)) -> StatusReply {
            StatusReply::Current { node_id: b.0.clone(), incarnation: b.1.clone(), state: NodeState::Ready }
        }

        fn stopped(b: &(NodeId, IncarnationId)) -> StatusReply {
            StatusReply::CurrentStopped { node_id: b.0.clone(), incarnation: b.1.clone() }
        }

        /// The silent member `mesh1.rpc.1` at birth `b`, and its mesh: two ready members, this
        /// admin (never a carrier) and the member itself.
        fn member_and_view(b: &(NodeId, IncarnationId)) -> (Node, crate::topology::Topology) {
            use crate::model::{Fabric, FabricId, NodeStatus, ProviderKind, ScopeStatus};
            let ready = |name: &str| {
                let mut n = Node::allocated(name.parse().unwrap());
                n.node_id = NodeId::mint();
                n.incarnation_id = Some(IncarnationId::mint());
                n.status = NodeStatus::Ready;
                n
            };
            let mut node = ready("mesh1.rpc.1");
            node.node_id = b.0.clone();
            node.incarnation_id = Some(b.1.clone());
            node.status = NodeStatus::PendingReconnect;
            let view = crate::topology::Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![], nodes: vec![ready("mesh1.admin.1"), node.clone(), ready("mesh1.rpc.2"), ready("mesh1.rpc.3"), ready("mesh2.rpc.1")] };
            (node, view)
        }

        /// What one probe does: spends `after`, counts its end in `ended`, and ends with `reply`
        /// (`None`: no answer, a deadline).
        #[derive(Clone)]
        struct Probe {
            after: Duration,
            reply: Option<StatusReply>,
        }

        fn probe(after: Duration, reply: Option<StatusReply>) -> Probe {
            Probe { after, reply }
        }

        /// A caller whose probes answer as the cell says: the direct one, and one per carrier by
        /// path.name (a carrier not named fails at once).
        struct Rig {
            direct: Probe,
            peers: Vec<(&'static str, Probe)>,
            ended: Arc<AtomicUsize>,
            /// The carriers the ladder asked, in order.
            asked: Mutex<Vec<String>>,
            /// The leads the index names, by path.name, in the order it gives them.
            leads: Vec<&'static str>,
        }

        impl Rig {
            fn new(direct: Probe, peers: Vec<(&'static str, Probe)>) -> Self {
                Self { direct, peers, ended: Arc::default(), asked: Mutex::default(), leads: Vec::new() }
            }

            fn leading(self, leads: &[&'static str]) -> Self {
                Self { leads: leads.to_vec(), ..self }
            }

            async fn run(&self, p: &Probe) -> ProbeEnd {
                sleep(p.after).await;
                self.ended.fetch_add(1, Ordering::SeqCst);
                match &p.reply {
                    Some(r) => ProbeEnd::Reply(r.clone()),
                    None => ProbeEnd::Other { end: "NotSent", detail: "not sent: Deadline".into() },
                }
            }
        }

        #[async_trait::async_trait]
        impl TickleCalls for Rig {
            fn caller(&self) -> String {
                "mesh1.admin.1".into()
            }

            fn deadline(&self) -> Duration {
                BUDGET
            }

            async fn direct(&self, _node: &Node, probe: &StatusRequest) -> ProbeEnd {
                assert!(matches!(probe, StatusRequest::ProbeNodeState { .. }), "the probe is ProbeNodeState: {probe:?}");
                self.run(&self.direct).await
            }

            async fn via_peer(&self, carrier: &Node, _node: &Node, _probe: &StatusRequest) -> ProbeEnd {
                self.asked.lock().unwrap().push(carrier.name.to_string());
                match self.peers.iter().find(|(c, _)| *c == carrier.name.to_string()) {
                    Some((_, p)) => self.run(p).await,
                    None => ProbeEnd::Other { end: "NotSent", detail: "not sent: NoActiveRoute".into() },
                }
            }

            fn carriers(&self, view: &crate::topology::Topology, node: &Node) -> Vec<Node> {
                let me = view.members().find(|n| n.name.to_string() == self.caller()).map(|n| n.node_id.clone());
                eligible_carriers(view, node, me.as_ref()).cloned().collect()
            }

            async fn leads(&self, view: &crate::topology::Topology, _node: &Node) -> Result<Vec<Node>, String> {
                Ok(self.leads.iter().filter_map(|l| view.members().find(|n| n.name.to_string() == *l).cloned()).collect())
            }
        }

        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:28
        /// "Ladder: Direct first. On failure, the same probe carried by Forward through up to two eligible
        ///  live peers of the node's mesh, both at once; one failed peer probe does not cancel the other"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:41
        /// "3. On no answer, the same probe through up to two eligible live peers of the node's mesh, both
        ///  sent at once, each a `Forward` whose inner op is the probe; a peer that cannot carry it is
        ///  `InnerNotSent` and counts as no answer."
        /// CONTRACT (rulings INDEX Q107 item 1 step c: "The same kick via peers, the leads first";
        /// node-tickle.md step 3: "up to two ... both sent at once"): after the direct probe failed the
        /// kick goes through the leads first, a lead of another mesh included, then the caller's eligible
        /// peers of the node's mesh not already among them, two in all; a lead that is also an eligible
        /// peer is asked once. The spans name the leads read (`leads`, on the direct and the unreached
        /// span) and mark each carrier that was a lead (`lead`).
        #[tokio::test(start_paused = true)]
        async fn the_kick_goes_through_the_leads_first_then_the_callers_peers_within_the_fan_out() {
            let b = birth();
            let (node, view) = member_and_view(&b);
            let capture = crate::span_capture::Capture::new("node-tickle-leads");
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            let rig = Rig::new(probe(Duration::ZERO, None), vec![]).leading(&["mesh2.rpc.1"]);
            let Tickle::Unreached(u) = tickle(&node, &view, &rig).await.unwrap() else { panic!("no answer: unreached") };
            assert_eq!(u.carriers(), "mesh2.rpc.1,mesh1.rpc.2", "the lead of another mesh first, then the first eligible peer of the node's mesh");
            drop(u);
            let leads_of = |name: &str| capture.named(name).iter().map(|s| s.attributes.get("leads").cloned().unwrap_or_default()).collect::<Vec<_>>();
            assert_eq!(leads_of("rdm.node.tickle.resolve.via-direct"), ["mesh2.rpc.1"]);
            assert_eq!(leads_of("rdm.node.tickle.returns-unknown.via-unreached"), ["mesh2.rpc.1"]);
            let mut peers: Vec<(String, String)> = capture.named("rdm.node.tickle.resolve.via-peer").iter().map(|s| (s.attributes.get("carrier").cloned().unwrap_or_default(), s.attributes.get("lead").cloned().unwrap_or_default())).collect();
            peers.sort();
            assert_eq!(peers, [("mesh1.rpc.2".to_string(), "false".to_string()), ("mesh2.rpc.1".to_string(), "true".to_string())], "each carrier says whether it was a lead");
            let rig = Rig::new(probe(Duration::ZERO, None), vec![]).leading(&["mesh1.rpc.3"]);
            let Tickle::Unreached(u) = tickle(&node, &view, &rig).await.unwrap() else { panic!("no answer: unreached") };
            assert_eq!(u.carriers(), "mesh1.rpc.3,mesh1.rpc.2", "a lead that is an eligible peer is asked once, first");
            let rig = Rig::new(probe(Duration::ZERO, None), vec![]).leading(&["mesh2.rpc.1", "mesh1.rpc.3"]);
            let Tickle::Unreached(u) = tickle(&node, &view, &rig).await.unwrap() else { panic!("no answer: unreached") };
            assert_eq!(u.carriers(), "mesh2.rpc.1,mesh1.rpc.3", "two leads take the whole fan-out");
        }

        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:29
        /// "Ends: Any answer from the node, under the probed incarnation, ends the ladder and cancels the
        ///  probes still running"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:40
        /// "2. Direct: `ProbeNodeState { node_id, incarnation }` to the node's held address, with the probe's deadline."
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:44
        /// "4. The first answer from the node, under the probed incarnation, ends the ladder: `reached { answer }`,
        ///  the other probes cancelled. No answer within the ladder: `unreached`."
        /// CONTRACT (node-tickle.md step 2, "Ends"): the birth's answer to the direct probe ends the
        /// ladder: `reached`, no carrier, and no peer is asked.
        #[tokio::test(start_paused = true)]
        async fn the_direct_answer_ends_the_ladder_before_any_peer_is_asked() {
            let b = birth();
            let (node, view) = member_and_view(&b);
            let rig = Rig::new(probe(Duration::from_millis(5), Some(current(&b))), vec![("mesh1.rpc.2", probe(BUDGET, Some(current(&b))))]);
            match tickle(&node, &view, &rig).await.unwrap() {
                Tickle::Reached { answer, carrier: None } => assert_eq!(answer, current(&b)),
                other => panic!("the direct answer is reached, with no carrier: {other:?}"),
            }
            assert!(rig.asked.lock().unwrap().is_empty(), "no peer is asked after a direct answer");
        }

        /// CONTRACT (R-SW1 acceptance 1; node-tickle.md step 3): after the direct probe failed, both
        /// peer probes run at once, so two peers each spending a whole budget cost one budget, not
        /// two; with no answer the ladder is `unreached`, naming both carriers in the order asked,
        /// never the caller itself.
        #[tokio::test(start_paused = true)]
        async fn the_tickle_spends_one_budget_on_both_peer_probes() {
            let b = birth();
            let (node, view) = member_and_view(&b);
            let rig = Rig::new(probe(Duration::ZERO, None), vec![("mesh1.rpc.2", probe(BUDGET, None)), ("mesh1.rpc.3", probe(BUDGET, None))]);
            let start = Instant::now();
            let Tickle::Unreached(u) = tickle(&node, &view, &rig).await.unwrap() else { panic!("no answer: unreached") };
            assert_eq!(start.elapsed(), tickle_ladder(BUDGET) - BUDGET, "the two peer probes overlap: one budget, not two");
            assert_eq!(u.carriers(), "mesh1.rpc.2,mesh1.rpc.3", "both carriers of the birth's mesh, this admin never one: {u:?}");
            assert_eq!(rig.ended.load(Ordering::SeqCst), 3, "no answer: the direct and both carried probes ran to their end");
        }

        /// CONTRACT (R-SW1 completion rule; node-tickle.md "Ladder"): one failed peer probe does not
        /// cancel the other. The first peer fails at 1 s; the second answers for the exact birth at
        /// 3 s and ends the ladder there, naming that carrier.
        #[tokio::test(start_paused = true)]
        async fn the_tickle_takes_the_second_peers_answer_after_the_first_fails() {
            let b = birth();
            let (node, view) = member_and_view(&b);
            let rig = Rig::new(probe(Duration::ZERO, None), vec![("mesh1.rpc.2", probe(Duration::from_secs(1), None)), ("mesh1.rpc.3", probe(Duration::from_secs(3), Some(current(&b))))]);
            let start = Instant::now();
            match tickle(&node, &view, &rig).await.unwrap() {
                Tickle::Reached { carrier: Some(c), .. } => assert_eq!(c, "mesh1.rpc.3"),
                other => panic!("the second peer's answer is reached through it: {other:?}"),
            }
            assert_eq!(start.elapsed(), Duration::from_secs(3), "the second peer's answer, not the first's failure, ends the ladder");
        }

        /// CONTRACT (node-tickle.md step 4): the first answer from the exact birth ends the ladder and
        /// cancels the probe still running, which never completes.
        #[tokio::test(start_paused = true)]
        async fn the_tickle_drops_the_peer_probe_still_running_at_an_answer() {
            let b = birth();
            let (node, view) = member_and_view(&b);
            let rig = Rig::new(probe(Duration::ZERO, None), vec![("mesh1.rpc.2", probe(BUDGET, None)), ("mesh1.rpc.3", probe(Duration::from_secs(1), Some(current(&b))))]);
            let start = Instant::now();
            assert!(matches!(tickle(&node, &view, &rig).await.unwrap(), Tickle::Reached { carrier: Some(c), .. } if c == "mesh1.rpc.3"));
            assert_eq!(start.elapsed(), Duration::from_secs(1));
            sleep(BUDGET * 2).await;
            assert_eq!(rig.ended.load(Ordering::SeqCst), 2, "the probe still running was cancelled with the ladder");
        }

        /// CONTRACT (R-SW1 acceptance 3; node-tickle.md "a stopped node answers CurrentStopped"): a
        /// stopped primary's `CurrentStopped` through a peer is the birth's answer and ends the ladder;
        /// a carrier's own refusal before it is not an answer.
        #[tokio::test(start_paused = true)]
        async fn a_stopped_primary_answering_current_through_a_peer_ends_the_ladder() {
            let b = birth();
            let (node, view) = member_and_view(&b);
            let rig = Rig::new(probe(Duration::ZERO, None), vec![("mesh1.rpc.2", probe(Duration::from_millis(5), Some(StatusReply::NotReady { reason: "carrier not ready".into() }))), ("mesh1.rpc.3", probe(Duration::from_millis(20), Some(stopped(&b))))]);
            match tickle(&node, &view, &rig).await.unwrap() {
                Tickle::Reached { answer, carrier: Some(c) } => assert_eq!((answer, c.as_str()), (stopped(&b), "mesh1.rpc.3")),
                other => panic!("CurrentStopped through a peer is reached: {other:?}"),
            }
        }

        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:26
        /// "Target: One node, by `(node.id, incarnation)`; every probe names the same pair, and only that pair's answer counts"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:27
        /// "Probe: `ProbeNodeState { node_id, incarnation }` on 0x1B, a bounded call with a deadline; the node
        ///  answers `Current { state }`, `CurrentStopped` or `Blocked { reason, retry_on }`"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:71
        /// "`{ \"Current\": { \"node_id\": \"k8pag1qx95gw\", \"incarnation\": \"0000000000000008\", \"state\": \"Ready\" } }`"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:74
        /// "A stopped node answers `CurrentStopped { node_id, incarnation }`; a node held at its hook answers
        ///  `Blocked { node_id, incarnation, reason, retry_on }`. All three are `reached`."
        /// CONTRACT (R-SW1 item 3; node-tickle.md "Probe"): an answer is `Current`, `CurrentStopped` or
        /// `Blocked` from the exact birth probed. A NotReady (a carrier's or anyone's), another
        /// incarnation's reply, a refusal and no reply at all are not answers.
        #[test]
        fn only_the_exact_births_state_is_an_answer() {
            let b = birth();
            let blocked = StatusReply::Blocked { node_id: b.0.clone(), incarnation: b.1.clone(), reason: "rows not yet published".into(), retry_on: RetryOn::TargetReachable { node_id: NodeId::mint() } };
            for r in [current(&b), stopped(&b), blocked] {
                assert!(birth_answered(Some(&r), &b.0, &b.1), "{} is the birth's answer", r.name());
            }
            let other = (b.0.clone(), IncarnationId::parse(&rdm_core::fixture_crockford60("an-older-birth")).unwrap());
            for r in [StatusReply::NotReady { reason: "carrier not ready".into() }, current(&other), stopped(&other), StatusReply::RejectedStaleIncarnation { held: other.1.clone() }] {
                assert!(!birth_answered(Some(&r), &b.0, &b.1), "{} is no answer from the exact birth", r.name());
            }
            assert!(!birth_answered(None, &b.0, &b.1), "no reply (a deadline, carrier-edge-lost) is no answer");
        }

        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:30
        /// "Result: `reached { answer }`: the node is alive and said what it is; `unreached`: nobody reached
        ///  it within the ladder. Unreached is never death"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:31
        /// "What follows: The caller's: on `unreached` the investigation's verdict is the inspect's answer,
        ///  read before the tickle; on `reached` for a node that is off its channel it commands node.mesh.join;
        ///  mesh investigation decides at 60 s"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:32
        /// "Spans: `rdm.node.tickle.resolve.via-direct` for the direct probe, `via-peer` per carrier, each with
        ///  the answer or the failure; `rdm.node.tickle.returns-unknown.via-unreached` when the ladder ends with
        ///  no answer"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:138
        /// "The primitive has no failure of its own: `unreached` is a result, not an error, and it is never
        ///  recorded as the node's death."
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:146
        /// "Direct probe | caller | `rdm.node.tickle.resolve.via-direct` | node, node_id, incarnation, answer or failure, deadline"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:147
        /// "Peer probe | caller, per carrier | `rdm.node.tickle.resolve.via-peer` | carrier, answer or failure"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:148
        /// "Ladder result | caller | the direct or peer span that carried the answer; `rdm.node.tickle.returns-unknown.via-unreached` when none did | what the caller does next"
        /// CONTRACT (node-tickle.md "Observability contract"): one ladder emits
        /// `rdm.node.tickle.resolve.via-direct` for the direct probe (node, node_id, incarnation, the
        /// failure, the deadline), `rdm.node.tickle.resolve.via-peer` per carrier (the carrier, its
        /// answer or failure) and, when none answered, `rdm.node.tickle.returns-unknown.via-unreached`
        /// carrying what the caller does next; a ladder a peer's answer ends has that peer's span
        /// carry the answer and no unreached span. Must NOT happen: a span of another name for a
        /// probe, or an unreached span for a ladder that was answered.
        #[tokio::test(start_paused = true)]
        async fn the_ladder_spans_the_direct_probe_each_carrier_and_the_unreached_end() {
            let b = birth();
            let (node, view) = member_and_view(&b);
            let capture = crate::span_capture::Capture::new("node-tickle-spans");
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            // Nobody reaches the birth: the caller says the inspection follows (node-tickle.md "What follows").
            let rig = Rig::new(probe(Duration::ZERO, None), vec![("mesh1.rpc.2", probe(Duration::from_secs(1), None)), ("mesh1.rpc.3", probe(Duration::from_secs(1), Some(StatusReply::NotReady { reason: "carrier not ready".into() })))]);
            let Tickle::Unreached(u) = tickle(&node, &view, &rig).await.unwrap() else { panic!("unreached") };
            u.next("verdict");
            drop(u);
            let direct = capture.named("rdm.node.tickle.resolve.via-direct");
            assert_eq!(direct.len(), 1, "one direct probe span: {direct:?}");
            let a = &direct[0].attributes;
            assert_eq!((a.get("node").map(String::as_str), a.get("node_id").map(String::as_str), a.get("incarnation").map(String::as_str), a.get("caller").map(String::as_str)), (Some("mesh1.rpc.1"), Some(b.0.as_str()), Some(b.1.as_str()), Some("mesh1.admin.1")), "{a:?}");
            assert_eq!(a.get("failure").map(String::as_str), Some("NotSent: not sent: Deadline"), "{a:?}");
            assert_eq!(a.get("deadline_ms").map(String::as_str), Some("10000"), "{a:?}");
            let mut peers = capture.named("rdm.node.tickle.resolve.via-peer");
            peers.sort_by(|x, y| x.attributes.get("carrier").cmp(&y.attributes.get("carrier")));
            assert_eq!(peers.iter().map(|p| p.attributes.get("carrier").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["mesh1.rpc.2".to_string(), "mesh1.rpc.3".to_string()], "one span per carrier: {peers:?}");
            assert_eq!(peers[0].attributes.get("failure").map(String::as_str), Some("NotSent: not sent: Deadline"), "{:?}", peers[0].attributes);
            assert_eq!(peers[1].attributes.get("answer").map(String::as_str), Some("not-ready"), "a reply that is not the birth's is named as it came: {:?}", peers[1].attributes);
            let unreached = capture.named("rdm.node.tickle.returns-unknown.via-unreached");
            assert_eq!(unreached.len(), 1, "one unreached span: {unreached:?}");
            let a = &unreached[0].attributes;
            assert_eq!((a.get("node_id").map(String::as_str), a.get("carriers").map(String::as_str), a.get("next").map(String::as_str)), (Some(b.0.as_str()), Some("mesh1.rpc.2,mesh1.rpc.3"), Some("verdict")), "{a:?}");
            for sp in direct.iter().chain(&peers).chain(&unreached) {
                eprintln!("span {} trace_id={} {:?}", sp.name, sp.trace_id, sp.attributes);
            }
            // A peer's answer: its span carries the answer, and no unreached span follows.
            let rig = Rig::new(probe(Duration::ZERO, None), vec![("mesh1.rpc.2", probe(Duration::from_secs(1), None)), ("mesh1.rpc.3", probe(Duration::from_millis(10), Some(current(&b))))]);
            assert!(matches!(tickle(&node, &view, &rig).await.unwrap(), Tickle::Reached { carrier: Some(_), .. }));
            assert_eq!(capture.named("rdm.node.tickle.resolve.via-direct").len(), 2);
            let answered: Vec<_> = capture.named("rdm.node.tickle.resolve.via-peer").into_iter().filter(|p| p.attributes.get("answer").map(String::as_str) == Some("current")).collect();
            assert_eq!(answered.len(), 1, "the carrier's span carries the birth's answer: {answered:?}");
            assert_eq!(answered[0].attributes.get("carrier").map(String::as_str), Some("mesh1.rpc.3"));
            assert_eq!(capture.named("rdm.node.tickle.returns-unknown.via-unreached").len(), 1, "an answered ladder has no unreached span");
        }

        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:11
        /// "- the mesh-primary in [node recovery](node-recovery.md), on a member marked `PendingReconnect`;"
        /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:36
        /// "1. The caller holds the node's id and incarnation and a reason to ask: silence past the staleness floor"
        /// CONTRACT (R-SW1 acceptance: the provider check stays mandatory and comes last;
        /// node-recovery.md step 2: "node.runtime.inspect (`exited` or `os-stopped` is the verdict, and no
        /// tickle is sent), node.tickle direct, the same tickle via up to two eligible live peers of the
        /// birth's mesh sent at once"; step 3: "Nothing holds"; Q181):
        /// TRUE OFFLINE, the only tickle outcome that sends the mesh-primary to the provider
        /// (`recovery_due`), is reached at the fold of the one ladder, only after its direct probe and
        /// both its peer probes ended unanswered, the ladder costing the direct probe's budget plus one
        /// peer budget. Must NOT happen: the provider owed before every probe of the ladder ended, or a
        /// second ladder before it.
        #[tokio::test(start_paused = true)]
        async fn the_provider_is_owed_only_after_both_peer_probes_of_the_ladder_ended() {
            let b = birth();
            let (node, view) = member_and_view(&b);
            let rig = Arc::new(Rig::new(probe(BUDGET, None), vec![("mesh1.rpc.2", probe(BUDGET, None)), ("mesh1.rpc.3", probe(BUDGET, None))]));
            let (node, view) = (Arc::new(node), Arc::new(view));
            let mut t = OfflineTickle::new();
            let id = b.0.to_string();
            let set = || vec![SilentNode { node_id: b.0.to_string(), path: "mesh1.rpc.1".into(), incarnation: Some(b.1.clone()), marked_at_ms: None }];
            // The instant each ladder's ladder ended.
            let verdict_at: Arc<Mutex<Vec<Instant>>> = Arc::default();
            let ladder = async |t: &mut OfflineTickle, now: u64| {
                t.tick(
                    now,
                    true,
                    set(),
                    |_| true,
                    PROBE_DELAY,
                    |_| {
                        let (rig, node, view, verdict_at) = (rig.clone(), node.clone(), view.clone(), verdict_at.clone());
                        async move {
                            let end = tickle(&node, &view, &*rig).await;
                            verdict_at.lock().unwrap().push(Instant::now());
                            crate::offline::Ladder::Tickled(end)
                        }
                    },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await
            };
            let ms = |d: Duration| d.as_millis() as u64;
            ladder(&mut t, 0).await;
            // Ladder 1, started by the tick at half a floor: the tick waits on none of it.
            let start = Instant::now();
            assert_eq!(ladder(&mut t, PROBE_DELAY).await.tickled, vec![id.clone()]);
            assert_eq!(start.elapsed(), Duration::ZERO, "the tick that starts ladder 1 waits on none of it");
            sleep(tickle_ladder(BUDGET) + Duration::from_millis(1)).await;
            assert_eq!(verdict_at.lock().unwrap()[0] - start, tickle_ladder(BUDGET), "ladder 1: the direct budget, then one budget for both peers");
            let r1 = ladder(&mut t, PROBE_DELAY + ms(tickle_ladder(BUDGET)) + 1).await;
            assert_eq!(r1.offline, vec![id.clone()], "the ladder nobody answered, folded, is TRUE OFFLINE: the provider is owed");
            assert_eq!(rig.ended.load(Ordering::SeqCst), 3, "TRUE OFFLINE only once the direct and both peer probes of the ladder ended");
            assert!(ladder(&mut t, PROBE_DELAY + ms(tickle_ladder(BUDGET)) + FLOOR).await.tickled.is_empty(), "no second ladder: nothing holds");
            assert_eq!(verdict_at.lock().unwrap().len(), 1, "one ladder ran");
        }
    }

    fn no_path() -> ViaPeerVerdict {
        ViaPeerVerdict::NoPath { asked: vec!["mesh1.rpc.2".into()] }
    }

    #[tokio::test(start_paused = true)]
    async fn the_one_tickle_waits_half_a_floor_after_the_mark() {
        let mut t = OfflineTickle::new();
        assert!(tick(&mut t, 0, true, silent("a"), true, no_path()).await.tickled.is_empty(), "marked silent now: no tickle yet");
        assert!(tick(&mut t, PROBE_DELAY - 1, true, silent("a"), true, no_path()).await.tickled.is_empty());
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), true, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "one connect at floor + half a floor");
        assert_eq!(r.reachable_silent, vec!["a".to_string()], "an answer is reachable-silent, no action");
        assert!(tick(&mut t, PROBE_DELAY * 4, true, silent("a"), true, no_path()).await.tickled.is_empty(), "settled: not tickled again");
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 2: "Half a floor after the mark it makes
    /// one QUIC connect"; step 1: "a node-admin that takes the role mid-window continues the window
    /// already running"): the window runs from the mark, the instant the birth went unheard past the
    /// staleness floor, not from the tick that first lists it. A birth marked at 0 and first listed
    /// by a tick 700 ms later is tickled by the tick at half a floor after the mark, and the ladder's
    /// span names the mark as `first_seen_ms`. Must NOT happen: the connect half a floor after the
    /// sighting (a window that grows by the time between the mark and the tick that saw it).
    #[tokio::test(start_paused = true)]
    async fn the_one_tickle_is_made_half_a_floor_after_the_mark_not_after_the_tick_that_first_saw_it() {
        let marked = |id: &str| vec![SilentNode { node_id: id.into(), path: format!("mesh1.rpc.{id}"), incarnation: None, marked_at_ms: Some(0) }];
        let mut t = OfflineTickle::new();
        assert!(tick(&mut t, 700, true, marked("a"), false, no_path()).await.tickled.is_empty(), "listed 700 ms after its mark: no tickle yet");
        let r = tick(&mut t, PROBE_DELAY, true, marked("a"), false, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "one connect half a floor after the mark, not half a floor after the tick that first listed it");
        assert!(t.is_offline("a"), "the ladder nobody answered holds the birth offline");
        // A node-admin that takes the role later still runs on the mark its own view holds.
        let mut successor = OfflineTickle::new();
        assert!(tick(&mut successor, 700, false, marked("a"), false, no_path()).await.tickled.is_empty(), "not the role: bookkeeping only");
        let r = tick(&mut successor, PROBE_DELAY, true, marked("a"), false, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "the successor tickles on the window from the mark");
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 1: a member is marked once it is unheard
    /// past the staleness floor): the silent set carries, for each watched birth, the mark its digest
    /// digest view holds, the last receipt plus the floor, on the caller's local clock; a birth the digest view holds
    /// no receipt of, a receipt of another incarnation, or one heard within the floor carries none.
    #[test]
    fn the_silent_set_marks_each_birth_at_its_last_receipt_plus_the_floor() {
        use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode};
        let floor = std::time::Duration::from_secs(3);
        let digest = |name: &str| MeshDigest { held_hash: Default::default(),
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode { node_id: crate::model::NodeId::mint(), name: name.parse().unwrap(), endpoint_id: EndpointId("key".into()), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, runtime: None },
            status: MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            emitted_at_rafka_ms: 1,
            digest_seq: 1,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            data_dir: None, is_primary: None, is_fabric_primary: None
        };
        let node_of = |d: &MeshDigest| {
            let mut n = crate::model::Node::allocated(d.node.name.clone());
            n.node_id = d.node.node_id.clone();
            n.incarnation_id = Some(d.node.incarnation.clone());
            n
        };
        let digest_view = rdm_transport::topology::DigestView::default();
        let t0 = std::time::Instant::now();
        let (silent, fresh, re_born, unheld) = (digest("mesh1.rpc.1"), digest("mesh1.rpc.2"), digest("mesh1.rpc.3"), digest("mesh1.rpc.4"));
        digest_view.record_at(silent.clone(), t0);
        digest_view.record_at(fresh.clone(), t0 + floor);
        digest_view.record_at(re_born.clone(), t0);
        let mut other_birth = node_of(&re_born);
        other_birth.incarnation_id = Some(IncarnationId::mint());
        let watched = [node_of(&silent), node_of(&fresh), other_birth, node_of(&unheld)];
        let now = t0 + floor + std::time::Duration::from_millis(700);
        let set = silent_set(watched.iter(), &digest_view, now, 10_000, floor);
        let marks: Vec<(String, Option<u64>)> = set.iter().map(|s| (s.path.clone(), s.marked_at_ms)).collect();
        assert_eq!(
            marks,
            vec![("mesh1.rpc.1".into(), Some(9_300)), ("mesh1.rpc.2".into(), None), ("mesh1.rpc.3".into(), None), ("mesh1.rpc.4".into(), None)],
            "marked 700 ms before now, at its receipt plus the floor; heard within the floor, another incarnation and no receipt carry no mark"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn only_the_primary_tickles_and_a_new_primary_keeps_the_window() {
        let mut t = OfflineTickle::new();
        assert!(tick(&mut t, 0, false, silent("a"), false, no_path()).await.tickled.is_empty(), "not the role: bookkeeping only");
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), true, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "the window started before this admin took the role");
    }

    /// CONTRACT (node-recovery.md step 2: "Half a floor after the mark it runs node investigation on the
    /// exact birth: ... node.tickle direct, the same tickle via up to two eligible live peers ..., then
    /// node.runtime.inspect"; step 3: "Any other verdict (`running`, `os-stopped`, `exited`, `unproven`):
    /// the recovery below, at once. Nothing holds, nothing is inspected again"; node-tickle.md "What
    /// follows": "on `unreached` the investigation's verdict is the inspect's answer, read before the
    /// tickle"; Q111, Q181): the mesh-primary's one ladder,
    /// half a floor after the mark, that nobody answers is held offline at the tick that folds it, so the
    /// inspection and the recovery follow on that same tick, whatever kind of member it is (a node-admin
    /// of the mesh-primary's own mesh included). Must NOT happen: a hold-down opened, a second ladder a
    /// floor later, or the birth left un-inspected for a floor after its ladder found nobody.
    // @feature: node-lifecycle
    #[tokio::test(start_paused = true)]
    async fn a_ladder_nobody_answers_is_the_verdict_at_its_fold_and_opens_no_hold_down() {
        let capture = crate::span_capture::Capture::new("q181-one-ladder-no-hold-down");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let mut t = OfflineTickle::observed_by("mesh1.admin.1".into(), crate::stop_gate::StopGate::new());
        let admin = vec![SilentNode { node_id: "a".into(), path: "mesh1.admin.2".into(), incarnation: None, marked_at_ms: Some(0) }];
        assert!(tick(&mut t, 0, true, admin.clone(), false, no_path()).await.tickled.is_empty(), "nothing before half a floor after the mark");
        let r = tick(&mut t, PROBE_DELAY, true, admin.clone(), false, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "the one ladder runs half a floor after the mark");
        assert_eq!(r.offline, vec!["a".to_string()], "the ladder nobody answered, folded, is the birth offline: its inspection and recovery follow at once: {r:?}");
        let later = tick(&mut t, PROBE_DELAY + FLOOR, true, admin, false, no_path()).await;
        assert!(later.tickled.is_empty(), "no second ladder a floor later: nothing holds, nothing is probed again: {later:?}");
        assert!(capture.named("rdm.node_admin.node.update.via-offline-hold-down-opened").is_empty(), "no hold-down is opened");
        let offline = capture.named("rdm.node_admin.node.update.via-true-offline");
        assert_eq!(offline.len(), 1, "one offline mark, at the fold of the one ladder");
        assert_eq!(offline[0].attributes.get("path").map(String::as_str), Some("mesh1.admin.2"), "{:?}", offline[0].attributes);
    }

    /// CONTRACT (node-recovery.md step 2: the one ladder, half a floor after the mark; step 3: the
    /// recovery at once; Q181): the TRUE OFFLINE mark names the instant no path was known, when the
    /// ladder's probes ended, carried by the ladder to the tick that folds it, and the instant the ladder
    /// began, never the instant of the tick that folds it. The ladder starts at PROBE_DELAY and the tick
    /// waits on none of it; its direct connect and its via-peer step each spend a 10 s budget, so no path
    /// is known at PROBE_DELAY + 20 s, and the next tick folds it. Must NOT happen: a hold-down, a second
    /// ladder, or a mark stamped with the folding tick's clock.
    // @feature: node-lifecycle
    #[tokio::test(start_paused = true)]
    async fn the_offline_mark_names_the_instant_the_ladder_found_nobody_not_the_tick_that_folded_it() {
        const BUDGET: u64 = 10_000;
        let verdict_at = PROBE_DELAY + 2 * BUDGET;
        let capture = crate::span_capture::Capture::new("q181-offline-from-verdict");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let mut t = OfflineTickle::observed_by("mesh1.admin.1".into(), crate::stop_gate::StopGate::new());
        let ladder = async |t: &mut OfflineTickle, now: u64| {
            t.tick(
                now,
                true,
                silent("a"),
                |_| true,
                PROBE_DELAY,
                |_| async {
                    // The direct probe spends its budget, then both carried probes spend one at once.
                    tokio::time::sleep(std::time::Duration::from_millis(2 * BUDGET)).await;
                    crate::offline::Ladder::Tickled(Ok(unreached(&["mesh1.rpc.2"])))
                },
                |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
            )
            .await
        };
        let ended = std::time::Duration::from_millis(2 * BUDGET + 1);
        ladder(&mut t, 0).await;
        let started = tokio::time::Instant::now();
        let r1 = ladder(&mut t, PROBE_DELAY).await;
        assert_eq!(r1.tickled, vec!["a".to_string()], "the ladder runs half a floor after the mark");
        assert_eq!(started.elapsed(), std::time::Duration::ZERO, "the tick that starts the ladder waits on none of it");
        assert!(r1.offline.is_empty() && !t.is_offline("a"), "nothing is decided before a tick folds the ladder");
        tokio::time::sleep(ended).await;
        let folded = ladder(&mut t, verdict_at + 5_000).await;
        assert_eq!(folded.offline, vec!["a".to_string()], "the tick after the ladder ended folds its no path: TRUE OFFLINE");
        assert!(ladder(&mut t, verdict_at + FLOOR).await.tickled.is_empty(), "no second ladder a floor later");
        let failed = capture.named("rdm.node_admin.node.update.via-true-offline");
        assert_eq!(failed.len(), 1, "one TRUE OFFLINE span");
        let at = |k: &str| failed[0].attributes.get(k).cloned().unwrap_or_default();
        assert_eq!((at("no_path_at_ms"), at("ladder_began_ms")), (verdict_at.to_string(), PROBE_DELAY.to_string()), "the mark names the ladder's verdict and its start, not the folding tick: {:?}", failed[0].attributes);
        assert!(capture.named("rdm.node_admin.node.update.via-offline-hold-down-opened").is_empty(), "no hold-down");
        eprintln!("span rdm.node_admin.node.update.via-true-offline trace_id={} {:?}", failed[0].trace_id, failed[0].attributes);
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_via_a_peer_is_reachable_silent() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), false, ViaPeerVerdict::Answered { via: "mesh1.rpc.2".into() }).await;
        assert_eq!(r.reachable_silent, vec!["a".to_string()]);
        assert!(!t.is_offline("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn unreadable_candidates_change_no_status() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), false, ViaPeerVerdict::CandidatesUnreadable("no topology".into())).await;
        assert!(r.offline.is_empty() && r.reachable_silent.is_empty() && !t.is_offline("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_heard_again_ends_its_tickle_and_status() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        tick(&mut t, PROBE_DELAY, true, silent("a"), false, no_path()).await;
        tick(&mut t, PROBE_DELAY + FLOOR, true, silent("a"), false, no_path()).await;
        assert!(t.is_offline("a"));
        let r = tick(&mut t, PROBE_DELAY + FLOOR + 1, true, vec![], false, no_path()).await;
        assert_eq!(r.returned, vec!["a".to_string()]);
        assert!(r.gone.is_empty());
        assert!(!t.is_offline("a"));
    }

    /// CONTRACT (node-recovery.md "Detection and proof": the tickle and the TRUE OFFLINE mark are
    /// the mesh-primary's own observations of a member): a TRUE OFFLINE node whose node id leaves
    /// the view (node.deleted, or a recovery that created a new node.id at its path.name) ends its
    /// tickle and its status as GONE, named apart from a node heard again.
    /// Must NOT happen: a deleted birth reported and spanned `via-offline-returned` ("heard again").
    #[tokio::test(start_paused = true)]
    async fn a_silent_node_that_left_the_view_is_gone_never_heard_again() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        tick(&mut t, PROBE_DELAY, true, silent("a"), false, no_path()).await;
        tick(&mut t, PROBE_DELAY + FLOOR, true, silent("a"), false, no_path()).await;
        assert!(t.is_offline("a"));
        let capture = crate::span_capture::Capture::new("i143-3051-offline-gone");
        let r = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            t.tick(
                PROBE_DELAY + FLOOR + 1,
                true,
                vec![],
                |id: &str| id != "a",
                PROBE_DELAY,
                |_| async { crate::offline::Ladder::Tickled(Ok(unreached(&["mesh1.rpc.2"]))) },
                |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
            )
            .await
        };
        assert!(r.returned.is_empty(), "a node the view no longer holds was not heard again: {r:?}");
        assert_eq!(r.gone, vec!["a".to_string()], "it is gone");
        assert!(!t.is_offline("a"));
        assert!(capture.named("rdm.node_admin.node.update.via-offline-returned").is_empty(), "no heard-again span for a gone node");
        let gone = capture.named("rdm.node_admin.node.remove.via-offline-gone");
        assert_eq!(gone.len(), 1, "one gone span");
        assert_eq!(gone[0].attributes.get("node_id").map(String::as_str), Some("a"));
        eprintln!("span rdm.node_admin.node.remove.via-offline-gone trace_id={} {:?}", gone[0].trace_id, gone[0].attributes);
    }

    /// A nodes store counting every read of any fact on a birth.
    #[derive(Default)]
    struct ReadCounting {
        inner: crate::storage::MemoryNodesStorage,
        reads: std::sync::atomic::AtomicUsize,
    }

    impl ReadCounting {
        fn read(&self) {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for ReadCounting {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.read();
            self.inner.node(node_id).await
        }
        async fn node_at(&self, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.read();
            self.inner.node_at(name).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.read();
            self.inner.nodes(mesh).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node(row).await
        }
        async fn index_path(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.index_path(name, node_id).await
        }
        async fn index_tags(&self, mesh: &str, node_id: &crate::model::NodeId, leaving: &std::collections::BTreeSet<String>, arriving: &std::collections::BTreeSet<String>) -> Result<(), crate::storage_error::StorageError> {
            self.inner.index_tags(mesh, node_id, leaving, arriving).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.inner.tagged(label).await
        }
        async fn name_claim(&self, name: &crate::model::PathName) -> Result<Option<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.inner.name_claim(name).await
        }
        async fn claim_node_name(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<Result<(), crate::storage::NameHeld>, crate::storage_error::StorageError> {
            self.inner.claim_node_name(name, node_id).await
        }
        async fn release_node_name(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.release_node_name(name, node_id).await
        }
    }

    /// CONTRACT (storage-whole-records.md, "The rule" and its first cell): the mesh-primary's silence
    /// mark is applied to the record it holds and the record written whole, with no read of storage
    /// before the write: the declared state already on the record stands beside the mark. Must NOT
    /// happen: a read before the write, or a write that carries the mark alone.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_silence_mark_is_written_on_the_whole_record_held_with_no_read_first() {
        use crate::model::{IncarnationId, Node, NodeStatus, PathName};
        use crate::storage::NodesStorage;
        let mut held = Node::allocated("mesh1.rpc.1".parse::<PathName>().unwrap());
        held.incarnation_id = Some(IncarnationId::mint());
        let store = ReadCounting::default();
        let written = crate::node_rows::WrittenRows::default();
        let inc = held.incarnation_id.clone().unwrap();
        // The record as its writer holds it: the birth's declared state is on it.
        let mut row = crate::storage::NodeRow::new(held.name.clone(), held.node_id.clone(), NodeStatus::Ready);
        row.birth_mut(&inc, &held.name).declared.insert(rdm_core::status::NodeState::Ready);
        written.hold(&row);

        assert_eq!(put_silence_status(&store, &written, &held, Some(NodeStatus::Dead)).await, Ok(true), "the silence mark is written");
        assert_eq!(store.reads.load(std::sync::atomic::Ordering::SeqCst), 0, "the write reads nothing from storage first");
        let stored = store.inner.node(&held.node_id).await.unwrap().expect("the record is written whole");
        let birth = stored.birth(&inc).expect("the birth's facts");
        assert_eq!(birth.silence, Some(NodeStatus::Dead), "the mark is on the record");
        assert!(birth.declared.contains(&rdm_core::status::NodeState::Ready), "the declared state the record held stands beside it");
        held.incarnation_id = None;
        assert!(put_silence_status(&store, &written, &held, None).await.is_err(), "a held record with no birth is named, never skipped");
    }

    /// A nodes store whose puts of silence facts are refused, as a full or read-only disk would.
    #[derive(Default)]
    pub(crate) struct SilenceRefusing {
        pub inner: crate::storage::MemoryNodesStorage,
    }

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for SilenceRefusing {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.node(node_id).await
        }
        async fn node_at(&self, name: &crate::model::PathName) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.node_at(name).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.nodes(mesh).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: format!("nodes/{}/records/{}", row.name.mesh, row.node_id), reason: "No space left on device (os error 28)".into() })
        }
        async fn index_path(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.index_path(name, node_id).await
        }
        async fn index_tags(&self, mesh: &str, node_id: &crate::model::NodeId, leaving: &std::collections::BTreeSet<String>, arriving: &std::collections::BTreeSet<String>) -> Result<(), crate::storage_error::StorageError> {
            self.inner.index_tags(mesh, node_id, leaving, arriving).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.inner.tagged(label).await
        }
        async fn name_claim(&self, name: &crate::model::PathName) -> Result<Option<crate::model::NodeId>, crate::storage_error::StorageError> {
            self.inner.name_claim(name).await
        }
        async fn claim_node_name(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<Result<(), crate::storage::NameHeld>, crate::storage_error::StorageError> {
            self.inner.claim_node_name(name, node_id).await
        }
        async fn release_node_name(&self, name: &crate::model::PathName, node_id: &crate::model::NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.release_node_name(name, node_id).await
        }
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 2: TRUE OFFLINE is "written as status
    /// Dead on the node's row"; CLAUDE.md: an error state names what failed and where, and distinct
    /// facts get distinct spans): when nodes.storage refuses the mesh primary's put of a birth's
    /// silence fact, the refusal is one `rdm.node_admin.node.reject.via-nodes-storage` span naming the
    /// node, its exact birth, the status being written, the store and the store's own cause, and the
    /// write answers that refusal; no row stands. A held record with no incarnation is another fact:
    /// one `via-no-birth` span, never the storage span. Must NOT happen: a refused write with no span
    /// (an info line only), or the two refusals under one span.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_refused_silence_status_write_is_spanned_with_the_birth_the_status_and_the_stores_cause() {
        use crate::model::{IncarnationId, Node, NodeStatus, PathName};
        use crate::storage::NodesStorage;
        let mut held = Node::allocated("mesh1.rpc.1".parse::<PathName>().unwrap());
        held.incarnation_id = Some(IncarnationId::mint());
        let store = SilenceRefusing::default();
        let capture = crate::span_capture::Capture::new("i143-3051-silence-refused");
        let (refused, unborn) = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            let written = crate::node_rows::WrittenRows::default();
            let refused = put_silence_status(&store, &written, &held, Some(NodeStatus::Dead)).await;
            let mut unborn = held.clone();
            unborn.incarnation_id = None;
            (refused, put_silence_status(&store, &written, &unborn, Some(NodeStatus::PendingReconnect)).await)
        };
        let why = refused.expect_err("a refused put answers its refusal");
        assert!(why.contains("No space left on device"), "the answer carries the store's cause: {why}");
        assert!(store.inner.node(&held.node_id).await.unwrap().is_none(), "no record stands");
        let spans = capture.named("rdm.node_admin.node.reject.via-nodes-storage");
        assert_eq!(spans.len(), 1, "one span for the one refused put: {spans:?}");
        let a = &spans[0].attributes;
        let attr = |k: &str| a.get(k).map(String::as_str).unwrap_or_default().to_string();
        assert_eq!(attr("node"), "mesh1.rpc.1", "{a:?}");
        assert_eq!(attr("node_id"), held.node_id.to_string(), "{a:?}");
        assert_eq!(attr("incarnation_id"), held.incarnation_id.clone().unwrap().as_str(), "the exact birth: {a:?}");
        assert_eq!(attr("status"), "dead", "the status it was writing: {a:?}");
        assert_eq!(attr("store"), "nodes.storage", "{a:?}");
        assert!(attr("error").contains("nodes/mesh1/records/") && attr("error").contains("No space left on device (os error 28)"), "the store's own cause, with its file: {a:?}");
        eprintln!("span rdm.node_admin.node.reject.via-nodes-storage trace_id={} {a:?}", spans[0].trace_id);
        assert!(unborn.is_err(), "a held record with no birth is named, never skipped");
        let no_birth = capture.named("rdm.node_admin.node.reject.via-no-birth");
        assert_eq!(no_birth.len(), 1, "the unborn record is its own refusal: {no_birth:?}");
        let b = &no_birth[0].attributes;
        assert_eq!((b.get("node").map(String::as_str), b.get("status").map(String::as_str)), (Some("mesh1.rpc.1"), Some("pending-reconnect")), "{b:?}");
        eprintln!("span rdm.node_admin.node.reject.via-no-birth trace_id={} {b:?}", no_birth[0].trace_id);
    }
}
