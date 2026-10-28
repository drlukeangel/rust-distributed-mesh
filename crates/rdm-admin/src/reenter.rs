//! A node-admin entering an EXISTING mesh (node-admin-lifecycle.md §4): one code path for a
//! recovering mesh's first admin (after its maker's JoinNode and topology) and for a fabric
//! primary re-born into its own mesh (from its durable map, with no maker).
//!
//! 1. Read topology: [`get_topology`] is the one place "get the topology from <source>" lives.
//!    It reads the maker's JoinNode answer, a durable map, or a LOCAL node of the own mesh (a
//!    gossip join through that node as the seed; its digests fill the view). When the catalogued
//!    topology read (op 0x1E) lands, this function is the one swap.
//! 2. Connect to a local node of the own mesh: the first node of the map that answers a Ping.
//!    Members can exit while a mesh has no admin, so the maker's map alone is not the current read.
//! 3. The own-mesh sweep: ONE bounded core Ping to every node of the own mesh, once, on entry.
//!    A reply proves a live exact birth. No reply is "not yet reached", never death: the node
//!    stays in the map as unheard and goes to node recovery (mesh-recovery.md step 6): the new
//!    mesh-primary's tickle, then the provider's exact exit proof, then recovery only on proof.

use crate::model::{EndpointId, IncarnationId, NodeId, PathName};
use crate::storage::NodeRow;
use rdm_transport::topology::TopologyView;
use rdm_transport::{Budget, CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A node's fixed deadline in the sweep.
pub(crate) const SWEEP_DEADLINE: Duration = Duration::from_secs(2);
/// The most nodes pinged at once.
pub(crate) const SWEEP_CONCURRENCY: usize = 8;

/// One birth of the topology map: what a Ping needs to reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapNode {
    pub node_id: NodeId,
    pub name: PathName,
    pub endpoint_id: EndpointId,
    pub transport_addr: SocketAddr,
    pub incarnation: IncarnationId,
    /// The lifecycle transition the source held the birth in (being born, drained or leaving), or
    /// `None` for a settled birth. Every birth of the own mesh is pinged on entry; the status is
    /// what the view learns of a birth in transition that answers, never `Ready`.
    pub transition: Option<crate::model::NodeStatus>,
    /// What the source knew: a birth the source holds ready is ready, anything else settled is
    /// not yet reached.
    pub ready: bool,
    /// The birth's data dir and control API as the source held them.
    pub data_dir: Option<String>,
    pub admin_api_base: Option<String>,
}

impl MapNode {
    /// The birth `record` names to reach: its newest birth, at the address its writer recorded on it
    /// (`crate::admin::rows_writer::MeshRowsWriter`). A record whose newest birth carries no address
    /// names none. A stored record says nothing of who lives: the birth is settled and not ready.
    pub(crate) fn of_record(record: &NodeRow) -> Option<Self> {
        let b = record.newest_birth()?;
        Some(Self {
            node_id: record.node_id.clone(),
            name: record.name.clone(),
            endpoint_id: b.endpoint_id.clone()?,
            transport_addr: b.transport_addr?,
            incarnation: b.incarnation_id.clone(),
            transition: None,
            ready: false,
            data_dir: None,
            admin_api_base: None,
        })
    }

    pub(crate) fn resolved(&self) -> Option<ResolvedNode> {
        Some(ResolvedNode {
            node_id: self.node_id.clone(),
            name: self.name.clone(),
            endpoint_id: self.endpoint_id.0.parse().ok()?,
            transport_addr: self.transport_addr,
            incarnation: self.incarnation.clone(),
        })
    }

    /// The birth as the view holds it until the topology view speaks for it: `status` is what the map
    /// knows (`Ready` for a settled birth that answered a Ping, the transition its source
    /// held for one in transition that answered, `PendingReconnect` for a settled one not yet
    /// reached; never `Dead`).
    pub(crate) fn as_node(&self, status: crate::model::NodeStatus, provider: crate::model::ProviderKind) -> crate::model::Node {
        let mut node = crate::model::Node::allocated(self.name.clone());
        node.node_id = self.node_id.clone();
        node.endpoint_id = Some(self.endpoint_id.clone());
        node.incarnation_id = Some(self.incarnation.clone());
        node.transport_addr = Some(self.transport_addr);
        node.provider = Some(provider);
        node.status = status;
        node.data_dir = self.data_dir.clone();
        node.admin_api_base = self.admin_api_base.clone();
        node
    }

    pub(crate) fn gossip_addr(&self) -> Option<iroh::EndpointAddr> {
        let key = self.endpoint_id.0.parse::<iroh::PublicKey>().ok()?;
        Some(iroh::EndpointAddr::new(key).with_ip_addr(self.transport_addr))
    }
}

/// The births of one topology read, as the map a Ping reaches.
pub(crate) fn map_of_read(read: &crate::topology_read::TopologyRead, fabric: &rdm_core::FabricId) -> Vec<MapNode> {
    read.installed
        .iter()
        .flat_map(|m| m.members.iter())
        .filter(|d| &d.fabric_id == fabric)
        .map(|d| {
            // A digest says what the node last reported, not that it is alive: whoever answered
            // heard it at an age the digest does not carry. Every birth is therefore not yet
            // reached (the sweep's ping, or its own digest, says more), never ready.
            let transition = (d.status != rdm_core::MemberStatus::Ready).then(|| crate::admin::node_status(d.status));
            MapNode {
                transition,
                ready: false,
                data_dir: d.data_dir.clone(),
                admin_api_base: d.admin_api_base.clone(),
                node_id: d.node.node_id.clone(),
                name: d.node.name.clone(),
                endpoint_id: d.node.endpoint_id.clone(),
                transport_addr: d.node.transport_addr,
                incarnation: d.node.incarnation.clone(),
            }
        })
        .collect()
}

/// The births of a stored map the maker answered with, as the map a Ping reaches. A stored map has
/// no version and says nothing of who lives: every birth is settled-for-the-sweep and not ready.
/// It is for reaching a local node of the own mesh and is never installed as topology.
pub(crate) fn map_of_stored(read: &crate::topology_read::TopologyRead) -> Vec<MapNode> {
    read.stored
        .iter()
        .flat_map(|m| m.nodes.iter())
        .filter_map(|n| {
            Some(MapNode {
                transition: None,
                ready: false,
                data_dir: None,
                admin_api_base: None,
                node_id: n.node_id.clone(),
                name: n.name.parse().ok()?,
                endpoint_id: n.endpoint_id.clone(),
                transport_addr: n.transport_addr,
                incarnation: n.incarnation.clone(),
            })
        })
        .collect()
}

/// Where a topology is read from.
pub(crate) enum TopologySource<'a> {
    /// This admin's durable map (the births the node records hold, `crate::admin::rows_writer::
    /// recorded_births`), with no maker.
    DurableMap(&'a [MapNode]),
    /// A node of the own mesh: its topology is read with `GetTopology` (op 0x1E), which installs
    /// nothing (the map is for reaching nodes, not for holding them), and this admin joins the mesh channel with it as the seed.
    LocalNode { node: &'a MapNode, client: &'a NodeRpcClient, topology_view: &'a TopologyView, me: &'a PathName, me_id: &'a NodeId, time: &'a rdm_transport::clock::RafkaTime, adopt_time: bool },
}

/// The topology `source` holds, every mesh, as births a Ping can reach. `Err` names what failed.
/// The maker's topology is read by the birth itself (`topology_read::get_topology` on the maker),
/// whose installed read is mapped by [`map_of_read`].
pub async fn get_topology(source: TopologySource<'_>) -> Result<Vec<MapNode>, String> {
    match source {
        TopologySource::DurableMap(rows) => Ok(rows.to_vec()),
        TopologySource::LocalNode { node, client, topology_view, me, me_id, time, adopt_time } => {
            let addr = node.gossip_addr().ok_or_else(|| format!("{}: its endpoint id {} is not an iroh key", node.name, node.endpoint_id.0))?;
            let read = crate::topology_read::read_topology(client, &NodeTarget::ExactNode(node.node_id.clone()), topology_view.node(), None, None)
                .await
                .map_err(|e| format!("reading the topology of {}: {e}", node.name))?;
            if adopt_time {
                // An entering admin adopts the rafka-time its entry read is served with, by the same rule as a join.
                read.adopt_rafka_time(time, topology_view, &me.to_string(), &me.mesh, me_id, &node.name.to_string()).map_err(|e| format!("adopting rafka-time from {}: {e}", node.name))?;
            }
            // The seats this node's mesh knows are held: an entering admin computes none before it has them.
            topology_view.learn_primaries(&read.seats, "entry");
            topology_view.join_peers_as("admin.reenter", vec![addr]).await.map_err(|e| format!("joining the mesh channel through {}: {e}", node.name))?;
            let fabric = read.installed.iter().flat_map(|m| m.members.iter()).find(|d| d.node.node_id == node.node_id).map(|d| d.fabric_id.clone());
            let Some(fabric) = fabric else {
                return Err(format!("{} answered a topology read that does not name itself", node.name));
            };
            Ok(map_of_read(&read, &fabric))
        }
    }
}

/// What the entering admin has.
#[derive(Clone)]
pub(crate) struct EntryCtx {
    pub me: PathName,
    pub me_id: NodeId,
    pub client: Arc<NodeRpcClient>,
    pub resolver: Arc<LiveNodeResolver>,
    pub topology_view: TopologyView,
    pub time: rdm_transport::clock::RafkaTime,
}

/// What the sweep found.
#[derive(Debug, Clone, Default)]
pub(crate) struct SweepReport {
    /// The own-mesh node the topology was read from, when one answered.
    pub local: Option<PathName>,
    pub reached: Vec<MapNode>,
    /// Nodes that did not answer, with the call's outcome name: not yet reached, never dead.
    pub not_reached: Vec<(MapNode, String)>,
    pub elapsed_ms: u64,
}

async fn ping(ctx: &EntryCtx, n: &MapNode) -> Result<(), String> {
    if let Some(r) = n.resolved() {
        ctx.resolver.apply(r, None);
    }
    let req = rdm_core::ping::PingRequest::Ping { payload: b"entry-sweep".to_vec() };
    let opts = CallOptions { budget: Budget::Overall(SWEEP_DEADLINE), ..CallOptions::default() };
    let (out, _) = ctx.client.call::<rdm_core::ping::Ping>(&NodeTarget::ExactNode(n.node_id.clone()), &req, &opts).await;
    if out.reply().is_some() {
        Ok(())
    } else {
        Err(out.name().to_string())
    }
}

/// The births of the own-mesh map the entry pings, in path order: the candidates for the local
/// node and the sweep's targets (node-admin-lifecycle.md §4.2).
pub(crate) fn sweep_targets(map: &[MapNode]) -> Vec<MapNode> {
    map.to_vec()
}

/// The candidates for the local node a current topology is read from (node-admin-lifecycle.md
/// §4.1 step 5), in the order they are tried: settled births first, then births in transition, each
/// in path order. A draining node refuses `GetTopology` (node-rpc-envelope.md: "A draining node
/// refuses new non-status calls"), so a settled node that answers is the one to read from.
pub(crate) fn local_candidates(map: &[MapNode]) -> Vec<MapNode> {
    let mut v = map.to_vec();
    v.sort_by_key(|n| (n.transition.is_some(), n.name.to_string()));
    v
}

/// Enter the existing mesh of `ctx.me` from `held`, the topology its source gave: connect to a
/// local node, read the current topology from it, then sweep the own mesh once.
pub(crate) async fn enter_existing_mesh(ctx: &EntryCtx, source: &'static str, held: Vec<MapNode>) -> SweepReport {
    let started = Instant::now();
    let mesh = ctx.me.mesh.clone();
    let own = |nodes: &[MapNode]| -> Vec<MapNode> {
        let mut v: Vec<MapNode> = nodes.iter().filter(|n| n.name.mesh == mesh && n.node_id != ctx.me_id).cloned().collect();
        v.sort_by_key(|n| n.name.to_string());
        v.dedup_by_key(|n| n.name.clone());
        v
    };
    let mut map = own(&held);
    // A local node of the own mesh: the first of the map, in path order, that answers.
    let mut local = None;
    for n in local_candidates(&map) {
        if ping(ctx, &n).await.is_ok() {
            local = Some(n.clone());
            break;
        }
    }
    if let Some(l) = &local {
        match get_topology(TopologySource::LocalNode { node: l, client: &ctx.client, topology_view: &ctx.topology_view, me: &ctx.me, me_id: &ctx.me_id, time: &ctx.time, adopt_time: source != "durable-map" }).await {
            Ok(current) => {
                // The local node's read is current: it replaces the map's entry for a name, and
                // adds the names the map lacked.
                let current = own(&current);
                map.retain(|m| !current.iter().any(|c| c.name == m.name));
                map.extend(current);
                map.sort_by_key(|n| n.name.to_string());
            }
            Err(why) => {
                tracing::info_span!("rdm.node_admin.mesh.reject.via-local-topology-read", node = %ctx.me, mesh = %mesh, local = %l.name, detail = %why)
                    .in_scope(|| tracing::info!("the local node answered but its topology was not read; the sweep runs on the map held"));
            }
        }
    }
    // The sweep: one bounded Ping to every own-mesh node, SWEEP_CONCURRENCY at a time.
    let mut reached = Vec::new();
    let mut not_reached = Vec::new();
    let swept: Vec<MapNode> = sweep_targets(&map);
    for chunk in swept.chunks(SWEEP_CONCURRENCY) {
        let answers = futures_util::future::join_all(chunk.iter().map(|n| async move { (n, ping(ctx, n).await) })).await;
        for (n, a) in answers {
            match a {
                Ok(()) => reached.push(n.clone()),
                Err(outcome) => not_reached.push((n.clone(), outcome)),
            }
        }
    }
    let report = SweepReport { local: local.map(|l| l.name), reached, not_reached, elapsed_ms: started.elapsed().as_millis() as u64 };
    tracing::info_span!(
        "rdm.node_admin.mesh.update.via-entry-sweep",
        node = %ctx.me,
        mesh = %mesh,
        source,
        local = report.local.as_ref().map(|l| l.to_string()).unwrap_or_default(),
        pinged = swept.len(),
        swept = %swept.iter().map(|n| n.name.to_string()).collect::<Vec<_>>().join(","),
        reached = report.reached.len(),
        not_reached = report.not_reached.len(),
        elapsed_ms = report.elapsed_ms,
    )
    .in_scope(|| {
        for (n, outcome) in &report.not_reached {
            tracing::info_span!(
                "rdm.node_admin.node.reject.via-entry-sweep-no-reply",
                node = %n.name,
                node_id = %n.node_id,
                incarnation_id = %n.incarnation.as_str(),
                outcome = %outcome,
                sweeper = %ctx.me,
            )
            .in_scope(|| tracing::info!("no reply to the entry sweep's Ping: not yet reached, not dead"));
        }
        tracing::info!("the own-mesh sweep on entering the mesh");
    });
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::MemberStatus;

    /// An own-mesh birth of the map as a topology read gives it: the status its source held.
    fn birth(name: &str, status: MemberStatus) -> MapNode {
        MapNode {
            node_id: NodeId::mint(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId("k".into()),
            transport_addr: "127.0.0.1:1".parse().unwrap(),
            incarnation: IncarnationId::mint(),
            transition: (status != MemberStatus::Ready).then(|| crate::admin::node_status(status)),
            ready: false,
            data_dir: None,
            admin_api_base: None,
        }
    }

    /// CONTRACT (node-admin-lifecycle.md §4.2: "The new primary sends one bounded Ping, once, to
    /// every node IN ITS OWN MESH"): the entry pings every birth of the own-mesh map, whatever the
    /// status its source held. A member in a lifecycle transition (Pending, ConnectionsDeleting, Leaving) is a
    /// node of the mesh: in a leaving mesh every member is, and an entry that pings only the
    /// Ready ones reads no local topology and sweeps none of them.
    #[test]
    fn the_entry_pings_every_own_mesh_birth_whatever_its_status() {
        let map = vec![
            birth("mesh2.rpc.1", MemberStatus::Leaving),
            birth("mesh2.rpc.2", MemberStatus::Ready),
            birth("mesh2.rpc.3", MemberStatus::ConnectionsDeleting),
            birth("mesh2.rpc.4", MemberStatus::Pending),
        ];
        let pinged: Vec<String> = sweep_targets(&map).iter().map(|n| n.name.to_string()).collect();
        assert_eq!(pinged, vec!["mesh2.rpc.1", "mesh2.rpc.2", "mesh2.rpc.3", "mesh2.rpc.4"]);
    }

    /// CONTRACT (node-admin-lifecycle.md §4.1 step 5: "Read GetTopology from a LOCAL node of the
    /// recovering admin's own mesh"; node-rpc-envelope.md: "A draining node refuses new non-status
    /// calls"): the local node is tried settled births first, then births in transition, each in
    /// path order, so a ConnectionsDeleting node ahead of a Ready one in path order is never the one
    /// the current topology is read from while the ready one answers.
    #[test]
    fn the_local_node_is_tried_settled_births_first() {
        let map = vec![
            birth("mesh2.rpc.1", MemberStatus::ConnectionsDeleting),
            birth("mesh2.rpc.2", MemberStatus::Leaving),
            birth("mesh2.rpc.3", MemberStatus::Ready),
            birth("mesh2.rpc.4", MemberStatus::Ready),
        ];
        let tried: Vec<String> = local_candidates(&map).iter().map(|n| n.name.to_string()).collect();
        assert_eq!(tried, vec!["mesh2.rpc.3", "mesh2.rpc.4", "mesh2.rpc.1", "mesh2.rpc.2"]);
    }
}
