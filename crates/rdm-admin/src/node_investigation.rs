//! Node investigation (rulings INDEX Q107 item 1; node-investigation.md): the one function every
//! caller uses on one exact birth, ending in one verdict, reached / running / os-stopped / exited /
//! unproven.
//!
//! Its steps, in order: (a) the leads, one read of the store's by-destination connections index
//! (connections.md §3.1), naming the nodes in any mesh that hold a connection to the exact birth: the
//! peers the kick goes through first, never evidence of reachability, since a row nobody marked
//! dropped survives a cut or a crashed source; (b) the direct tickle and (c) the same kick through
//! peers, the leads first ([`crate::offline::tickle`], node-tickle.md); (d) on `unreached` alone, the
//! provider about the exact runtime (node.runtime.inspect, node-runtime-inspect.md). Only an answer
//! from the exact birth is `reached`; every other verdict is the provider's.

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
        /// The peer that carried the kick, by path.name; `None`: it answered the direct probe.
        carrier: Option<String>,
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
    /// Nothing tells the birth gone or alive: named by what was missing.
    Unproven {
        /// What was missing.
        reason: String,
    },
}

impl Finding {
    /// The verdict as node-investigation.md names it: `reached`, `running`, `os-stopped`, `exited` or
    /// `unproven`.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Reached { .. } => "reached",
            Self::Running => "running",
            Self::OsStopped => "os-stopped",
            Self::Exited { .. } => "exited",
            Self::Unproven { .. } => "unproven",
        }
    }

    /// The provider's answer the verdict came from (node.runtime.inspect's), `unproven` as its
    /// `unavailable`; `None` for `reached`, which asked the provider nothing.
    pub fn inspected(&self) -> Option<crate::deployment::provider::Inspected> {
        use crate::deployment::provider::Inspected;
        match self {
            Self::Reached { .. } => None,
            Self::Running => Some(Inspected::Running),
            Self::OsStopped => Some(Inspected::OsStopped),
            Self::Exited { code } => Some(Inspected::Exited(*code)),
            Self::Unproven { reason } => Some(Inspected::Unavailable(reason.clone())),
        }
    }

    /// What the finding names beyond its name: the carrier, the exit code or what was missing.
    pub fn detail(&self) -> String {
        match self {
            Self::Reached { carrier: Some(carrier), .. } => carrier.clone(),
            Self::Exited { code } => code.map(|c| c.to_string()).unwrap_or_else(|| "no code provable".into()),
            Self::Unproven { reason } => reason.clone(),
            _ => String::new(),
        }
    }
}

/// node.runtime.inspect's answer ([`crate::deployment::provider::inspect_runtime`]) as the
/// investigation's verdict: its `unavailable` unproven.
impl From<crate::deployment::provider::Inspected> for Finding {
    fn from(inspected: crate::deployment::provider::Inspected) -> Self {
        use crate::deployment::provider::Inspected;
        match inspected {
            Inspected::Exited(code) => Finding::Exited { code },
            Inspected::Running => Finding::Running,
            Inspected::OsStopped => Finding::OsStopped,
            Inspected::Unavailable(reason) => Finding::Unproven { reason },
        }
    }
}

/// Investigate `node`'s exact birth from `view` over `calls`, the caller's own Node RPC client, call
/// deadline, carriers and leads read (`docs/architecture/lifecycles/node-investigation.md`): the
/// leads, the direct tickle and the same kick through peers, the leads first
/// ([`crate::offline::tickle`]); then, only when nobody reached the
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

/// The runtime fact of a birth, one way for every inspect: the one a digest of it carries, else the
/// one its durable runtime row records (written at its admission, or handed into this admin's
/// nodes.storage at its own create). `None` when neither holds one.
pub fn runtime_of(digest: Option<&rdm_core::digest::MeshDigest>, row: Option<&crate::storage::RuntimeRow>) -> Option<BirthRuntime> {
    match (digest, row) {
        (Some(d), _) if d.node.runtime.is_some() => Some(BirthRuntime { fact: d.node.runtime.clone().expect("checked"), data_dir: d.data_dir.clone(), incarnation: d.node.incarnation.clone() }),
        (_, Some(r)) => Some(BirthRuntime { fact: r.runtime.clone(), data_dir: r.data_dir.clone(), incarnation: r.incarnation_id.clone() }),
        _ => None,
    }
}

/// The runtime recorded at launch of `n`'s exact birth (node-recovery.md step 3: "the exact runtime
/// recorded at launch"), the one resolution every inspect copy makes: a digest `digest_view` holds of that
/// exact birth (heard, else carried by its mesh-primary's projection, else the last one of a stopped
/// birth) when it carries a runtime fact, else the birth's own durable runtime row in `nodes`, a point
/// read by its key ([`runtime_of`]). A birth lost before this admin heard it has no digest here; its
/// row is still the runtime recorded at launch. `Err` names what was missing.
pub async fn birth_runtime(digest_view: &rdm_transport::topology::DigestView, nodes: Option<&dyn crate::storage::NodesStorage>, n: &Node) -> Result<BirthRuntime, String> {
    let Some(incarnation) = n.incarnation_id.clone() else { return Err(format!("{} ({}) holds no recorded birth: there is no exact runtime to inspect", n.name, n.node_id)) };
    let id = n.node_id.as_str();
    let exact = |d: &rdm_core::digest::MeshDigest| d.node.incarnation == incarnation;
    let digest = digest_view
        .get(id)
        .map(|(d, _)| d)
        .filter(exact)
        .or_else(|| digest_view.projected_at(id, std::time::Instant::now()).map(|(d, _)| d).filter(exact))
        .or_else(|| digest_view.stopped_digests().into_iter().find(|d| d.node.node_id == n.node_id && exact(d)));
    let row = match (digest.as_ref().is_some_and(|d| d.node.runtime.is_some()), nodes) {
        (false, Some(nodes)) => nodes.runtime(&n.node_id, &incarnation).await.map_err(|e| format!("the durable runtime row of {} at {} could not be read: {e}", n.node_id, incarnation.as_str()))?,
        _ => None,
    };
    runtime_of(digest.as_ref(), row.as_ref()).ok_or_else(|| {
        let held = if digest.is_some() { "a digest of this exact birth that carries no runtime fact" } else { "no digest of this exact birth" };
        format!("this admin holds {held} and no runtime row of it, so no runtime fact")
    })
}

/// node.runtime.inspect of `n`'s exact birth: [`birth_runtime`], then the one inspect
/// ([`crate::deployment::provider::inspect_runtime`]); beside its answer, the exact runtime handle
/// the provider was asked about (node-runtime-inspect.md "Target"; the inspect span's `handle`),
/// `None` when no fact was held or the provider refused to adopt it, so nothing was asked.
pub async fn inspect_birth_with_handle(provider: &dyn crate::deployment::provider::DeploymentProvider, digest_view: &rdm_transport::topology::DigestView, nodes: Option<&dyn crate::storage::NodesStorage>, n: &Node) -> (crate::deployment::provider::Inspected, Option<crate::deployment::provider::DeploymentHandle>) {
    match birth_runtime(digest_view, nodes, n).await {
        Err(why) => (crate::deployment::provider::Inspected::Unavailable(why), None),
        Ok(b) => {
            let handle = crate::deployment::provider::adopt(provider, &b.fact).ok();
            (crate::deployment::provider::inspect_runtime(provider, &b.fact, b.data_dir.as_deref(), b.incarnation.as_str()).await, handle)
        }
    }
}

/// node.runtime.inspect's span (node-runtime-inspect.md "Observability contract"), the one every
/// caller's inspection emits: `inspector` the caller's path.name, `handle` the exact runtime handle the
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
    tracing::info_span!("rdm.node_admin.node.resolve.via-provider-inspection", node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation, inspector = %inspector, handle = %handle, verdict = crate::admin::verdict_name(outcome), exit_code, reason = %reason)
        .in_scope(|| tracing::info!("{why}"));
}

/// [`inspect_birth_with_handle`]'s answer as the investigation's verdict: the provider's inspection of
/// the exact runtime of `n`'s birth, unproven by name when no runtime fact is held.
pub async fn inspect_birth(provider: &dyn crate::deployment::provider::DeploymentProvider, digest_view: &rdm_transport::topology::DigestView, nodes: Option<&dyn crate::storage::NodesStorage>, n: &Node) -> Finding {
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
        let Some(source) = view.members().find(|m| m.node_id == row.source.node_id) else { continue };
        let same_birth = row.source.incarnation.as_ref().is_none_or(|i| source.incarnation_id.as_ref() == Some(i));
        if !same_birth || source.node_id == node.node_id || Some(&source.node_id) == me || source.status != NodeStatus::Ready {
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
        n.status = NodeStatus::Ready;
        n
    }

    /// The silent `mesh1.rpc.1`, its mesh's ready members, this admin, and a ready node of mesh2.
    fn world() -> (Node, Topology) {
        let mut node = ready("mesh1.rpc.1");
        node.status = NodeStatus::PendingReconnect;
        let nodes = vec![ready("mesh1.admin.1"), node.clone(), ready("mesh1.rpc.2"), ready("mesh1.rpc.3"), ready("mesh2.rpc.1"), ready("mesh2.rpc.2")];
        (node, Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![], nodes })
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
        StatusReply::Current { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), state: NodeState::Ready }
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
        assert_eq!(names(leads_of(&rows, &view, &node, Some(&me.node_id))), ["mesh1.rpc.2"], "a source the view holds off ready carries nothing");
        assert!(leads_of(&rows, &view, &Node { incarnation_id: None, ..node.clone() }, None).is_empty(), "no birth, no leads");
    }

    fn fact(pid: u32) -> rdm_core::RuntimeFact {
        rdm_core::RuntimeFact { deployment_id: rdm_core::fixture_crockford60(&format!("dep-{pid}")), provider: rdm_core::RuntimeProvider::Process, control_domain: "host-7".into(), locator: rdm_core::RuntimeLocator::Process { pid, start: 1 } }
    }

    fn digest(n: &Node, incarnation: IncarnationId, runtime: Option<rdm_core::RuntimeFact>) -> rdm_core::MeshDigest {
        rdm_core::MeshDigest {
            held_hash: Default::default(),
            fabric_id: FabricId::mint(),
            node: rdm_core::MeshNode { node_id: n.node_id.clone(), name: n.name.clone(), endpoint_id: crate::model::EndpointId(String::new()), transport_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)), incarnation, supersedes: None, runtime },
            status: rdm_core::MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 0,
            data_dir: Some("/data/from-the-digest".into()),
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        }
    }

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
        let empty = rdm_transport::topology::DigestView::default();
        let none = birth_runtime(&empty, Some(&nodes), &node).await.unwrap_err();
        assert!(none.contains("no digest of this exact birth") && none.contains("no runtime row"), "{none}");
        nodes.put_runtime(&crate::storage::RuntimeRow { node_id: node.node_id.clone(), name: node.name.clone(), incarnation_id: inc.clone(), runtime: fact(2), data_dir: Some("/data/from-the-row".into()) }).await.unwrap();
        let from_row = BirthRuntime { fact: fact(2), data_dir: Some("/data/from-the-row".into()), incarnation: inc.clone() };
        assert_eq!(birth_runtime(&empty, Some(&nodes), &node).await, Ok(from_row.clone()), "no digest: the durable row");
        for (held_digest, why) in [(digest(&node, IncarnationId::mint(), Some(fact(3))), "a digest of another birth"), (digest(&node, inc.clone(), None), "a digest with no runtime fact")] {
            let digest_view = rdm_transport::topology::DigestView::default();
            assert!(digest_view.record(held_digest));
            assert_eq!(birth_runtime(&digest_view, Some(&nodes), &node).await, Ok(from_row.clone()), "{why}: the durable row");
        }
        let digest_view = rdm_transport::topology::DigestView::default();
        assert!(digest_view.record(digest(&node, inc.clone(), Some(fact(1)))));
        assert_eq!(birth_runtime(&digest_view, Some(&nodes), &node).await, Ok(BirthRuntime { fact: fact(1), data_dir: Some("/data/from-the-digest".into()), incarnation: inc }), "the exact birth's digest carries the runtime recorded at launch");
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

    /// CONTRACT (Q107 item 1; node-investigation.md: "Only an answer from the exact birth counts as
    /// `reached`"; node-runtime-inspect.md "Result"): an answer from the exact birth, direct or carried,
    /// is `reached` with that answer and its carrier, and the provider is never asked; with nobody
    /// reaching it, the verdict is the provider's, `running`, `os-stopped`, `exited` or `unproven`; a
    /// node with no birth runs no ladder and is unproven by name, the provider not asked. An answer
    /// naming another birth reaches nothing.
    #[tokio::test]
    async fn only_the_exact_births_answer_is_reached_and_only_the_provider_proves_an_exit() {
        let (node, view) = world();
        let rig = |direct: Option<StatusReply>, carried: Option<(&'static str, StatusReply)>| Rig { direct, carried, asked: Mutex::default() };
        let asked = AtomicBool::new(false);
        let found = investigate(&node, &view, &rig(Some(current(&node)), None), provider_says(Finding::Exited { code: Some(137) }, &asked)).await;
        assert_eq!(found, Finding::Reached { answer: current(&node), carrier: None });
        let found = investigate(&node, &view, &rig(None, Some(("mesh1.rpc.3", current(&node)))), provider_says(Finding::Exited { code: Some(137) }, &asked)).await;
        assert_eq!(found, Finding::Reached { answer: current(&node), carrier: Some("mesh1.rpc.3".into()) });
        assert_eq!((found.name(), found.detail()), ("reached", "mesh1.rpc.3".to_string()));
        assert!(!asked.load(Ordering::SeqCst), "an answer settles it: the provider is not asked");
        for said in [Finding::Running, Finding::OsStopped, Finding::Exited { code: Some(137) }, Finding::Unproven { reason: "Unknown".into() }] {
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

    /// A caller whose probes answer as `Rig`'s do and whose commands to the birth are unanswered past the
    /// floor, or not.
    struct Silent {
        rig: Rig,
        unanswered: bool,
    }

    #[async_trait::async_trait]
    impl TickleCalls for Silent {
        fn caller(&self) -> String {
            self.rig.caller()
        }
        fn deadline(&self) -> std::time::Duration {
            self.rig.deadline()
        }
        async fn direct(&self, node: &Node, probe: &StatusRequest) -> ProbeEnd {
            self.rig.direct(node, probe).await
        }
        async fn via_peer(&self, carrier: &Node, node: &Node, probe: &StatusRequest) -> ProbeEnd {
            self.rig.via_peer(carrier, node, probe).await
        }
        fn carriers(&self, view: &Topology, node: &Node) -> Vec<Node> {
            self.rig.carriers(view, node)
        }
        async fn leads(&self, view: &Topology, node: &Node) -> Result<Vec<Node>, String> {
            self.rig.leads(view, node).await
        }
        fn command_unanswered(&self, _node: &Node) -> bool {
            self.unanswered
        }
    }

    /// CONTRACT (node-investigation.md, Q182): "a tickle answered by a birth that has left a command
    /// unanswered past the floor is not `reached`": the exact birth's answer, direct or carried, is not
    /// `reached` while it has such a command, and the verdict continues to the provider's inspection;
    /// the same answers from a birth with no such command are `reached`. Must NOT happen: the answer of
    /// a birth that does not answer commands ending the investigation as `reached`, and the provider
    /// never being asked.
    #[tokio::test]
    async fn a_birth_with_a_command_unanswered_past_the_floor_is_not_reached_by_its_tickle_answer() {
        let (node, view) = world();
        let silent = |unanswered: bool, direct: Option<StatusReply>, carried: Option<(&'static str, StatusReply)>| Silent { rig: Rig { direct, carried, asked: Mutex::default() }, unanswered };
        for (direct, carried) in [(Some(current(&node)), None), (None, Some(("mesh1.rpc.3", current(&node))))] {
            let asked = AtomicBool::new(false);
            let found = investigate(&node, &view, &silent(true, direct.clone(), carried.clone()), provider_says(Finding::Running, &asked)).await;
            assert_eq!(found, Finding::Running, "the answer of a birth that answers no command is not reached: the provider's verdict");
            assert!(asked.load(Ordering::SeqCst), "the provider is asked");
            let asked = AtomicBool::new(false);
            let found = investigate(&node, &view, &silent(false, direct, carried), provider_says(Finding::Running, &asked)).await;
            assert!(matches!(found, Finding::Reached { .. }), "a birth with no command unanswered is reached by its answer: {found:?}");
            assert!(!asked.load(Ordering::SeqCst), "reached: the provider is not asked");
        }
    }

    // @feature: node-investigation
    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:5
    /// "Node investigation is the one function every caller uses on one exact birth, ending in
    ///  one verdict: `reached`, `running`, `os-stopped`, `exited` or `unproven`. Its steps, in order:
    ///  1. Read the by-destination connections index (connections.md §3.1). It names the peers to try;
    ///     it never proves the node is reachable.
    ///  2. node.tickle, direct.
    ///  3. The same tickle via those peers.
    ///  4. node.runtime.inspect."
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
                "mesh1.admin.1".into()
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
                self.asked.lock().unwrap().push(carrier.name.to_string());
                ProbeEnd::Other {
                    end: "NotSent",
                    detail: "not sent: Deadline".into(),
                }
            }
            fn carriers(&self, view: &Topology, node: &Node) -> Vec<Node> {
                let me = of(view, "mesh1.admin.1").node_id;
                crate::offline::eligible_carriers(view, node, Some(&me))
                    .cloned()
                    .collect()
            }
            async fn leads(&self, view: &Topology, node: &Node) -> Result<Vec<Node>, String> {
                let me = of(view, "mesh1.admin.1").node_id;
                Ok(leads_of(&self.rows, view, node, Some(&me)))
            }
        }

        let (node, view) = world();
        let m1 = of(&view, "mesh2.rpc.1");
        let r3 = of(&view, "mesh1.rpc.3");
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
            investigate(&node, &view, &rig, async move {
                let outcome = crate::deployment::provider::Inspected::Exited(Some(0));
                span_inspection(
                    "mesh1.admin.1",
                    &node_for_inspect,
                    &outcome,
                    None,
                    "node-investigation",
                );
                outcome.into()
            })
            .await
        };

        assert_eq!(found, Finding::Exited { code: Some(0) });
        assert_eq!((found.name(), found.detail()), ("exited", "0".to_string()));
        assert_eq!(
            *rig.asked.lock().unwrap(),
            vec![
                "direct".to_string(),
                "mesh1.rpc.3".to_string(),
                "mesh2.rpc.1".to_string()
            ],
            "direct probe first, then the two leads in path order before non-lead mesh1.rpc.2"
        );

        let direct_spans = capture.named("rdm.node.tickle.resolve.via-direct");
        assert_eq!(direct_spans.len(), 1);
        assert_eq!(
            direct_spans[0].attributes.get("leads").map(String::as_str),
            Some("mesh1.rpc.3,mesh2.rpc.1")
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
            Some("inspect")
        );

        let inspect_spans = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
        assert_eq!(inspect_spans.len(), 1);
        assert_eq!(
            inspect_spans[0]
                .attributes
                .get("verdict")
                .map(String::as_str),
            Some("exited")
        );
        assert_eq!(
            inspect_spans[0]
                .attributes
                .get("exit_code")
                .map(String::as_str),
            Some("0")
        );
    }
}
