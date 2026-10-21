//! Node investigation (rulings INDEX Q107 item 1): the one function every caller uses on one exact
//! birth, ending in one verdict, reached / running / os-stopped / exited with proof / unproven.
//!
//! Its steps, in order: (a) the leads, one read of the store's by-destination connections index
//! (connections.md §3.1), naming the nodes in any mesh that hold a connection to the exact birth: the
//! peers the kick goes through first, never evidence of reachability, since a row nobody marked
//! dropped survives a cut or a crashed source; (b) the direct tickle and (c) the same kick through
//! peers, the leads first ([`crate::offline::tickle`], node-tickle.md); (d) on `unreached` alone, the
//! provider about the exact runtime (node.runtime.inspect, node-runtime-inspect.md). Only an answer
//! from the exact birth is `reached`; only the provider proves an exit.

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
        /// The birth's own answer (`Current`, `CurrentParked` or `Blocked`).
        answer: rdm_core::status::StatusReply,
        /// The peer that carried the kick, by path.name; `None`: it answered the direct probe.
        carrier: Option<String>,
    },
    /// The provider inspected its exact runtime and it still runs.
    Running,
    /// The provider inspected its exact runtime and the operating system holds it suspended:
    /// neither hung nor gone, so no hold opens and nothing is terminated (deployment-provider.md §2.5).
    OsStopped,
    /// The provider inspected its exact runtime and it exited: a verified loss, `exit-proven { code }`
    /// (node-runtime-inspect.md "Result").
    Exited {
        /// The exit code, when one is provable.
        code: Option<i32>,
        /// Where the proof came from, the `proof` of the exit-proof row the caller writes
        /// (node-runtime-inspect.md step 5): `provider` (the provider's own terminal status),
        /// `exit-record` (the runtime's exit record in its data dir) or `none` (exited with no code).
        proof: &'static str,
    },
    /// Nothing proves the birth gone or alive: named by what was missing.
    Unproven {
        /// What was missing.
        reason: String,
    },
}

impl Finding {
    /// The finding's name as it appears in spans: a `reached` is `answered` direct and
    /// `answered-via-peer` through a peer.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Reached { carrier: None, .. } => "answered",
            Self::Reached { carrier: Some(_), .. } => "answered-via-peer",
            Self::Running => "running",
            Self::OsStopped => "os-stopped",
            Self::Exited { .. } => "exit-proven",
            Self::Unproven { .. } => "unproven",
        }
    }

    /// What the finding names beyond its name: the carrier, the exit code or what was missing.
    pub fn detail(&self) -> String {
        match self {
            Self::Reached { carrier: Some(carrier), .. } => carrier.clone(),
            Self::Exited { code, proof } => format!("{} ({proof})", code.map(|c| c.to_string()).unwrap_or_else(|| "no code provable".into())),
            Self::Unproven { reason } => reason.clone(),
            _ => String::new(),
        }
    }
}

/// node.runtime.inspect's answer ([`crate::deployment::provider::inspect_runtime`]) as the
/// investigation's verdict: its `exit-proven` an `Exited` whose `proof` is the inspect's, its
/// `unavailable` unproven.
impl From<crate::deployment::provider::Inspected> for Finding {
    fn from(inspected: crate::deployment::provider::Inspected) -> Self {
        use crate::deployment::provider::Inspected;
        match inspected {
            Inspected::Exited(code, proof) => Finding::Exited { code, proof },
            Inspected::Running => Finding::Running,
            Inspected::OsStopped => Finding::OsStopped,
            Inspected::Unavailable(reason) => Finding::Unproven { reason },
        }
    }
}

/// Investigate `node`'s exact birth from `view` over `calls`, the caller's own Node RPC client, call
/// deadline, carriers and leads read (Q107 item 1): the leads, the direct tickle and the same kick
/// through peers, the leads first ([`crate::offline::tickle`]); then, only when nobody reached the
/// birth, `inspect`, the caller's node.runtime.inspect of the birth's exact runtime, awaited at that
/// point and never before. An answer from the exact birth is `reached` and the provider is not asked;
/// a node the view holds no birth of runs no ladder and is unproven by name, the provider not asked
/// either. Every other verdict is the provider's.
pub async fn investigate<C, V>(node: &Node, view: &Topology, calls: &C, inspect: V) -> Finding
where
    C: TickleCalls + ?Sized,
    V: std::future::Future<Output = Finding>,
{
    match crate::offline::tickle(node, view, calls).await {
        Err(reason) => Finding::Unproven { reason },
        Ok(Tickle::Reached { answer, carrier }) => Finding::Reached { answer, carrier },
        Ok(Tickle::Unreached(unreached)) => {
            unreached.next("inspect");
            drop(unreached);
            inspect.await
        }
    }
}

/// Step a's leads of `node`'s exact birth, from `rows`, the store's by-destination index read for its
/// node id (connections.md §3.1): the sources of the rows that hold a connection to that birth, each
/// resolved to a member of `view` in any mesh that is ready for traffic, never `me` (the caller is
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
        let Some(source) = view.members().find(|m| m.node_id == row.source.node_id) else { continue };
        let same_birth = row.source.incarnation.as_ref().is_none_or(|i| source.incarnation_id.as_ref() == Some(i));
        if !same_birth || source.node_id == node.node_id || Some(&source.node_id) == me || source.status != NodeStatus::ReadyForTraffic {
            continue;
        }
        if !leads.iter().any(|l| l.node_id == source.node_id) {
            leads.push(source.clone());
        }
    }
    leads.sort_by_key(|l| l.name.to_string());
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

    fn ready(name: &str) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::ReadyForTraffic;
        n
    }

    /// The silent `mesh1.rpc.1`, its mesh's ready members, this admin, and a ready node of mesh2.
    fn world() -> (Node, Topology) {
        let mut node = ready("mesh1.rpc.1");
        node.status = NodeStatus::PendingReconnect;
        let nodes = vec![ready("mesh1.admin.1"), node.clone(), ready("mesh1.rpc.2"), ready("mesh1.rpc.3"), ready("mesh2.rpc.1"), ready("mesh2.rpc.2")];
        (node, Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process }, meshes: vec![], nodes })
    }

    fn of(view: &Topology, name: &str) -> Node {
        view.members().find(|n| n.name.to_string() == name).cloned().unwrap()
    }

    fn end(n: &Node) -> ConnectionEnd {
        ConnectionEnd { name: n.name.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone() }
    }

    fn row(source: &Node, destination: &Node, kind: ConnectionKind, state: ConnectionState) -> NodeConnection {
        NodeConnection { id: rdm_core::ConnectionId::mint(), source: end(source), destination: end(destination), kind, alpn: rdm_core::connections::ALPN.to_vec(), state, carrier: None, recovery: None, reason: None, logged_at_ms: 1 }
    }

    fn current(n: &Node) -> StatusReply {
        StatusReply::Current { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic }
    }

    /// CONTRACT (Q107 item 1 step a; connections.md §3.2, carrier discovery
    /// `rtable(destination).filter(state=active).filter(kind=direct)`): the leads are the sources, in
    /// any mesh, of the `Direct` `Connected` rows to the exact birth, resolved to ready members of the
    /// view, in path order, one per node. Must NOT be a lead: a row marked dropped or failed, a row to
    /// another birth of the node, a `Proxy` row, a row an earlier birth of its source wrote, a source
    /// the view does not hold ready, the caller, or the node itself.
    #[test]
    fn the_leads_are_the_ready_sources_in_any_mesh_of_the_active_direct_rows_to_the_exact_birth() {
        let (node, view) = world();
        let me = of(&view, "mesh1.admin.1");
        let (r2, r3, m1, m2) = (of(&view, "mesh1.rpc.2"), of(&view, "mesh1.rpc.3"), of(&view, "mesh2.rpc.1"), of(&view, "mesh2.rpc.2"));
        let other_birth = Node { incarnation_id: Some(IncarnationId::mint()), ..node.clone() };
        let earlier_source = Node { incarnation_id: Some(IncarnationId::mint()), ..r3.clone() };
        let unheld = ready("mesh3.rpc.1");
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
        let names = |v: Vec<Node>| v.into_iter().map(|n| n.name.to_string()).collect::<Vec<_>>();
        assert_eq!(names(leads_of(&rows, &view, &node, Some(&me.node_id))), ["mesh1.rpc.2", "mesh2.rpc.1"], "the active Direct sources in any mesh, once each, in path order");
        let mut view = view;
        view.nodes.iter_mut().find(|n| n.node_id == m1.node_id).unwrap().status = NodeStatus::PendingReconnect;
        assert_eq!(names(leads_of(&rows, &view, &node, Some(&me.node_id))), ["mesh1.rpc.2"], "a source the view holds off ready-for-traffic carries nothing");
        assert!(leads_of(&rows, &view, &Node { incarnation_id: None, ..node.clone() }, None).is_empty(), "no birth, no leads");
    }

    /// A caller whose direct probe and carried probes answer as the cell says.
    struct Rig {
        direct: Option<StatusReply>,
        carried: Option<(&'static str, StatusReply)>,
        asked: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl TickleCalls for Rig {
        fn caller(&self) -> String {
            "mesh1.admin.1".into()
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
            self.asked.lock().unwrap().push(carrier.name.to_string());
            match &self.carried {
                Some((c, r)) if carrier.name.to_string() == *c => ProbeEnd::Reply(r.clone()),
                _ => ProbeEnd::Other { end: "NotSent", detail: "not sent: Deadline".into() },
            }
        }
        fn carriers(&self, view: &Topology, node: &Node) -> Vec<Node> {
            let me = of(view, "mesh1.admin.1").node_id;
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

    /// CONTRACT (Q107 item 1: "Only an answer from the exact birth is 'reached'; only the provider
    /// proves an exit"; node-tickle.md "Result": "Unreached is never death"; node-runtime-inspect.md
    /// "Result"): an answer from the exact birth, direct or carried, is `reached` with that answer and
    /// its carrier, and the provider is never asked; with nobody reaching it, the verdict is the
    /// provider's, `running`, `os-stopped`, `exited` or `unproven`, and only its `exited` is a loss; a
    /// node with no birth runs no ladder and is unproven by name, the provider not asked. An answer
    /// naming another birth reaches nothing.
    #[tokio::test]
    async fn only_the_exact_births_answer_is_reached_and_only_the_provider_proves_an_exit() {
        let (node, view) = world();
        let rig = |direct: Option<StatusReply>, carried: Option<(&'static str, StatusReply)>| Rig { direct, carried, asked: Mutex::default() };
        let asked = AtomicBool::new(false);
        let found = investigate(&node, &view, &rig(Some(current(&node)), None), provider_says(Finding::Exited { code: Some(137), proof: "provider" }, &asked)).await;
        assert_eq!(found, Finding::Reached { answer: current(&node), carrier: None });
        let found = investigate(&node, &view, &rig(None, Some(("mesh1.rpc.3", current(&node)))), provider_says(Finding::Exited { code: Some(137), proof: "provider" }, &asked)).await;
        assert_eq!(found, Finding::Reached { answer: current(&node), carrier: Some("mesh1.rpc.3".into()) });
        assert_eq!((found.name(), found.detail()), ("answered-via-peer", "mesh1.rpc.3".to_string()));
        assert!(!asked.load(Ordering::SeqCst), "an answer settles it: the provider is not asked");
        for said in [Finding::Running, Finding::OsStopped, Finding::Exited { code: Some(137), proof: "provider" }, Finding::Unproven { reason: "Unknown".into() }] {
            let asked = AtomicBool::new(false);
            let another_birth = Node { incarnation_id: Some(IncarnationId::mint()), ..node.clone() };
            let r = rig(Some(current(&another_birth)), Some(("mesh1.rpc.2", current(&another_birth))));
            assert_eq!(investigate(&node, &view, &r, provider_says(said.clone(), &asked)).await, said, "nobody reached the exact birth: the provider's verdict");
            assert!(asked.load(Ordering::SeqCst), "the provider is asked once nobody reached it");
            assert_eq!(r.asked.lock().unwrap().first().map(String::as_str), Some("direct"), "the direct probe first");
        }
        let asked = AtomicBool::new(false);
        let found = investigate(&Node { incarnation_id: None, ..node.clone() }, &view, &rig(None, None), provider_says(Finding::Running, &asked)).await;
        assert!(matches!(&found, Finding::Unproven { reason } if reason.contains("no recorded birth")), "{found:?}");
        assert!(!asked.load(Ordering::SeqCst), "no ladder ran: the provider is not asked");
    }
}
