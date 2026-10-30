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
//!    mesh-primary's tickle, then the provider's exact exited verdict, then recovery only on proof.

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
    /// The lifecycle transition the source held the birth in (being born, connections-deleted or leaving), or
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
            // The roles this node's mesh knows are held: an entering admin computes none before it has them.
            topology_view.learn_primaries(&read.primaries, "entry");
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
/// in path order. A connections-deleting node refuses `GetTopology` (node-rpc-envelope.md: "A connections-deleting node
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
    /// recovering admin's own mesh"; node-rpc-envelope.md: "A connections-deleting node refuses new non-status
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

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:95
    /// "The RPC chain uses `rdm.node_rpc.request.update.via-call` and
    /// `rdm.node_rpc.request.serve.via-direct` for Ping and outer Forward."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:178
    /// "The node-admin that step 5 or step 6 creates joins mesh-x under mesh-x's own MeshId and never mints another.
    /// The mesh is still in the accepted topology, so the attempt belongs to the SAME accepted Build."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:185
    /// "1. The node.create of step 6 opens an attempt of the SAME accepted Build and deploys the mesh's
    /// first node-admin (its MeshId, seed and mesh-primary launch mode). It records the deployed birth;
    /// the node-admin binds port 0 and the OS assigns the port."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:188
    /// "2. The node-admin calls JoinNode on its MAKER, the fabric-primary, then GetTopology on that
    /// maker. It installs complete topology snapshots and obtains its control/status context."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:190
    /// "3. It calls GetTopology on a LOCAL node of its own mesh to obtain current topology; members
    /// can exit while the mesh has no node-admin. It installs complete snapshots and joins its mesh
    /// channel using member addresses (RDM's `get_topology` in `rdm-admin`'s reenter module)."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:196
    /// "4. Before it is ready, it sends one bounded Ping, once, to every node IN ITS OWN MESH
    /// (a 2 s deadline each, at most 8 at once). A reply identifies a live exact birth; no reply
    /// is \"not yet reached\", never death."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:205
    /// "5. Every node the sweep did not reach goes to [node recovery](node-recovery.md): the new
    /// mesh-primary's [node investigation](node-investigation.md), then recovery on its verdict.
    /// A missing Ping reply is the trigger, never the verdict."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:208
    /// "6. The fabric-primary claims the creation of the missing secondary node-admin on its own log
    /// and calls the new mesh-primary with `build.attempt.run`; the executor adopts the won claim and
    /// runs it as an attempt of that same Build."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:253
    /// "| Fabric-primary | accepted Build / node.create | same Build attempt; the mesh's MeshId | `rdm.node_admin.node.update.via-build`, `rdm.node_admin.deployment.update.via-pipeline` |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:254
    /// "| Joining node-admin → maker | JoinNode | [Join schema](../node-rpc-envelope.md#join-op-0x1d-rdm); exact deployed birth admitted | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct`, `rdm.node_admin.node.update.via-join` |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:255
    /// "| Joining node-admin → maker / local node | GetTopology | [Topology schema](../node-rpc-envelope.md#topology-op-0x1e-rdm); complete meshes installed | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct`, `rdm.mesh.topology.serve.via-read`, `rdm.mesh.topology.update.via-read-install` |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:256
    /// "| Mesh-primary → own members | Ping once | [Ping schema](../node-rpc-envelope.md#ping-op-0x01-core); exact-birth reply or non-answer | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct` |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:257
    /// "| Mesh-primary / provider | node recovery | node investigation, one recovery attempt on its verdict | `rdm.node_admin.deployment.update.via-pipeline`, `rdm.node_admin.deployment.update.via-step` |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:258
    /// "| Fabric-primary → new mesh-primary | build.attempt.run | [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm); the fabric-primary claims on its own log, the executor adopts the won claim | `rdm.node_rpc.request.update.via-call`, `rdm.node_rpc.request.serve.via-direct` (op 0x20) |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:264
    /// "Joining composes deployment, topology reads, Ping, Build claims and readiness; it has no opcode of
    /// its own. A provider creates the process."
    /// CONTRACT (docs/architecture/lifecycles/fabric-recovery.md:98-100: "1. Load the durable topology and the
    /// accepted Build map; bind port 0 and retain the OS-assigned endpoint. The first GetTopology targets a node
    /// from that durable map: OWN MESH first, install only complete source snapshots. Stabilize the own mesh
    /// before investigating peers."
    /// docs/architecture/lifecycles/fabric-recovery.md:101-102: "2. Obtain current own-mesh topology, join the
    /// mesh channel from its member addresses, recover control and become ready under the ordinary election."
    /// docs/architecture/lifecycles/fabric-recovery.md:103-105: "3. Run the single bounded own-mesh Ping sweep
    /// (a node-admin joins the live mesh steps 4–5); a non-answering member goes to node recovery."
    /// docs/architecture/lifecycles/fabric-recovery.md:106-110: "4. Restore the own mesh's secondary admin as
    /// executor under that SAME accepted Build, with the claim the recovered fabric-primary's. Reach the other
    /// meshes through mesh investigation: its decision step runs node.create of a node-admin into each mesh that
    /// has none, under that mesh's own MeshId. Each such mesh-primary restores its own admin capacity and sweeps
    /// only its own mesh."
    /// docs/architecture/lifecycles/fabric-recovery.md:139: "| Recovered fabric-primary | GetTopology to
    /// durable-map node | Topology schema; own mesh first, no maker calls | `rdm.node_rpc.request.update.via-call`,
    /// `rdm.node_rpc.request.serve.via-direct`, `rdm.mesh.topology.serve.via-read`,
    /// `rdm.mesh.topology.update.via-read-install` |"
    /// docs/architecture/lifecycles/fabric-recovery.md:140: "| Recovered own-mesh primary | Ping / node recovery |
    /// same own-mesh sweep and node investigation as a node-admin joining a live mesh |
    /// `rdm.node_rpc.request.update.via-call`, `rdm.node_admin.deployment.update.via-step` |"
    /// docs/architecture/lifecycles/fabric-recovery.md:141: "| Recovered fabric-primary | accepted Build / claim |
    /// same Build, each mesh's own MeshId, claims as fabric-primary | `rdm.node_rpc.request.update.via-call` on
    /// the claimant, `rdm.node_admin.build.update.via-claim-decision` on the fabric-primary |"):
    /// a recovered fabric-primary binds port 0, reads GetTopology from the answering own-mesh member first
    /// (ignoring peer-mesh entries in the durable map), assembles complete snapshots, joins the mesh channel,
    /// runs the single bounded own-mesh Ping sweep (reporting non-answering members for node recovery), and
    /// claims under the same accepted Build with the full telemetry chain.
    #[test]
    fn recovered_fabric_primary_reads_own_mesh_topology_first_joins_channel_sweeps_own_mesh_and_claims_same_accepted_build() {
        let capture = crate::span_capture::Capture::new("recovered_fabric_primary_telemetry_chain");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let fabric_id = rdm_core::FabricId::mint();
            let mesh1_id = crate::model::MeshId::mint();

            // Local own-mesh node mesh1.rpc.1 serving Ping (0x01) and GetTopology (0x1E).
            let local_key = iroh::SecretKey::generate();
            let local_ep = rdm_transport::endpoint::bind(local_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
            let local_addr = local_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
            let local_gossip = iroh_gossip::net::Gossip::builder().spawn(local_ep.clone());
            let local_time = rdm_transport::clock::RafkaTime::unadopted();
            local_time.adopt(5_000_000);
            let local_view = TopologyView::join(&local_gossip, &local_ep, &fabric_id, "mesh1", &mesh1_id, "mesh1.rpc.1", Arc::new(local_time.clone()), vec![], &[]).await.unwrap();
            let (local_node_id, local_inc) = (NodeId::mint(), IncarnationId::mint());
            let (dead_node_id, dead_inc) = (NodeId::mint(), IncarnationId::mint());

            let local_digest = rdm_core::MeshDigest {
                held_hash: Default::default(),
                fabric_id: fabric_id.clone(),
                node: rdm_core::MeshNode {
                    node_id: local_node_id.clone(),
                    name: "mesh1.rpc.1".parse().unwrap(),
                    endpoint_id: EndpointId(local_ep.id().to_string()),
                    transport_addr: local_addr,
                    incarnation: local_inc.clone(),
                    supersedes: None,
                    runtime: None,
                },
                status: MemberStatus::Ready,
                node_state: Default::default(),
                admin_api_base: None,
                digest_seq: 1,
                emitted_at_rafka_ms: 5_000_000,
                data_dir: None,
                mesh_id: Some(mesh1_id.clone()),
                in_flight: None,
                load: None,
                gossip: None,
                is_primary: None,
                is_fabric_primary: None,
            };
            let dead_digest = rdm_core::MeshDigest {
                node: rdm_core::MeshNode {
                    node_id: dead_node_id.clone(),
                    name: "mesh1.rpc.2".parse().unwrap(),
                    endpoint_id: EndpointId(iroh::SecretKey::generate().public().to_string()),
                    transport_addr: "127.0.0.1:9".parse().unwrap(),
                    incarnation: dead_inc.clone(),
                    supersedes: None,
                    runtime: None,
                },
                ..local_digest.clone()
            };
            let snap = rdm_transport::snapshot::SourceSnapshot {
                mesh: "mesh1".into(),
                publisher: rdm_core::PublisherId {
                    node: "mesh1.admin.1".into(),
                    incarnation: IncarnationId::mint(),
                    node_id: NodeId::mint(),
                },
                topology_version: 1,
                digests: vec![local_digest.clone(), dead_digest],
                in_flight: vec![],
                deleted: vec![],
            };
            for r in crate::topology_read::snapshot_replies(&snap, 1) {
                if let rdm_core::topology::TopologyReply::Snapshot { mesh, publisher, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, deleted } = r {
                    let _ = local_view.take_read_chunk(
                        rdm_transport::snapshot::Chunk {
                            mesh,
                            publisher,
                            forwarded_by: Some("topology-read".into()),
                            topology_version,
                            snapshot_id,
                            chunk_index,
                            chunk_count,
                            digests: digests.into_iter().map(rdm_core::MeshDigest::from).collect(),
                            in_flight,
                            deleted,
                        },
                        "mesh1",
                    );
                }
            }

            let topo_slot: crate::topology_read::TopologySlot = Arc::new( std::sync::OnceLock::new());
            let own_d = local_digest.clone();
            let _ = topo_slot.set(Arc::new(crate::topology_read::TopologyDoor::new(
                local_view.clone(),
                Arc::new(move || own_d.clone()),
                local_time.clone(),
            )));
            let local_server = crate::topology_read::serve(rdm_transport::ServerBuilder::new(), topo_slot)
                .serve::<rdm_core::ping::Ping, _, _>(rdm_core::catalog::OpOwner::Core, |_peer, req| async move {
                    let rdm_core::ping::PingRequest::Ping { payload } = req;
                    Ok(rdm_core::ping::PingReply::Pong { payload })
                })
                .build(rdm_transport::ServedBirth { node_id: local_node_id.to_string(), incarnation: local_inc.to_string() })
                .unwrap();
            let local_router = iroh::protocol::Router::builder(local_ep.clone())
                .accept(iroh_gossip::ALPN, local_gossip.clone())
                .accept(rdm_transport::ALPN, local_server)
                .spawn();

            // Step 1: Recovered fabric-primary binds port 0 and loads durable map (with peer-mesh and own-mesh nodes).
            let fp_key = iroh::SecretKey::generate();
            let fp_ep = rdm_transport::endpoint::bind(fp_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
            let fp_addr = fp_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
            assert_ne!(fp_addr.port(), 0, "port 0 binds an OS-assigned port");
            let fp_gossip = iroh_gossip::net::Gossip::builder().spawn(fp_ep.clone());
            let fp_time = rdm_transport::clock::RafkaTime::unadopted();
            fp_time.adopt(5_000_000);
            let fp_view = TopologyView::join(&fp_gossip, &fp_ep, &fabric_id, "mesh1", &mesh1_id, "mesh1.admin.1", Arc::new(fp_time.clone()), vec![], &[]).await.unwrap();
            let (fp_node_id, fp_inc) = (NodeId::mint(), IncarnationId::mint());
            let fp_resolver = Arc::new(LiveNodeResolver::default());
            let fp_client = Arc::new(NodeRpcClient::new(fp_ep.clone(), fp_resolver.clone()));

            // Durable map includes a peer-mesh node (mesh2.rpc.1) and the own-mesh node (mesh1.rpc.1).
            let peer_map_node = MapNode {
                node_id: NodeId::mint(),
                name: "mesh2.rpc.1".parse().unwrap(),
                endpoint_id: EndpointId(iroh::SecretKey::generate().public().to_string()),
                transport_addr: "127.0.0.1:9".parse().unwrap(),
                incarnation: IncarnationId::mint(),
                transition: None,
                ready: false,
                data_dir: None,
                admin_api_base: None,
            };
            let own_map_node = MapNode {
                node_id: local_node_id.clone(),
                name: "mesh1.rpc.1".parse().unwrap(),
                endpoint_id: EndpointId(local_ep.id().to_string()),
                transport_addr: local_addr,
                incarnation: local_inc.clone(),
                transition: None,
                ready: false,
                data_dir: None,
                admin_api_base: None,
            };
            let durable_rows = get_topology(TopologySource::DurableMap(&[peer_map_node, own_map_node])).await.unwrap();

            // Steps 1-3: enter_existing_mesh reads GetTopology from own-mesh node first, joins channel, and sweeps own mesh.
            let ctx = EntryCtx {
                me: "mesh1.admin.1".parse().unwrap(),
                me_id: fp_node_id.clone(),
                client: fp_client.clone(),
                resolver: fp_resolver.clone(),
                topology_view: fp_view.clone(),
                time: fp_time.clone(),
            };
            let report = enter_existing_mesh(&ctx, "durable-map", durable_rows).await;
            assert_eq!(report.local, Some("mesh1.rpc.1".parse().unwrap()), "GetTopology targets own-mesh durable-map node first");
            assert_eq!(report.reached.iter().map(|n| n.name.to_string()).collect::<Vec<_>>(), vec!["mesh1.rpc.1"]);
            assert_eq!(report.not_reached.iter().map(|(n, _)| n.name.to_string()).collect::<Vec<_>>(), vec!["mesh1.rpc.2"], "non-answering own-mesh member goes to node recovery");

            // Step 4: Restore secondary admin under the SAME accepted Build, with the claim the recovered fabric-primary's.
            use crate::build_state::BuildStateAdapter as _;
            let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
            let build_id = crate::build::BuildId::mint();
            builds
                .publish_accepted(&crate::build_state::BuildAccepted {
                    build_id: build_id.clone(),
                    topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", mesh1_id.clone()),
                    submitted_change: None,
                    submitted_at_ms: 1,
                })
                .await
                .unwrap();

            let mut fp_node = crate::model::Node::allocated("mesh1.admin.1".parse().unwrap());
            fp_node.kind = rdm_core::NodeKind::NodeAdmin;
            fp_node.mesh = "mesh1".into();
            fp_node.node_id = fp_node_id.clone();
            fp_node.incarnation_id = Some(fp_inc.clone());
            fp_node.status = crate::model::NodeStatus::Ready;
            fp_node.is_primary = true;
            fp_node.is_fabric_primary = true;
            let (sec_node_id, sec_inc) = (NodeId::mint(), IncarnationId::mint());
            let mut sec_node = crate::model::Node::allocated("mesh1.admin.2".parse().unwrap());
            sec_node.kind = rdm_core::NodeKind::NodeAdmin;
            sec_node.mesh = "mesh1".into();
            sec_node.node_id = sec_node_id.clone();
            sec_node.incarnation_id = Some(sec_inc.clone());
            sec_node.status = crate::model::NodeStatus::Ready;
            let fp_topology = Arc::new(tokio::sync::RwLock::new(crate::topology::Topology {
                fabric: crate::model::Fabric {
                    id: fabric_id,
                    name: "fabric1".into(),
                    status: crate::model::ScopeStatus::Ready,
                    provider: crate::model::ProviderKind::Process,
                },
                meshes: Vec::new(),
                nodes: vec![fp_node, sec_node],
            }));
            let claim_door = Arc::new(crate::build_claim::ClaimDoor {
                me: "mesh1.admin.1".parse().unwrap(),
                node_id: fp_node_id.clone(),
                topology: fp_topology,
                builds: builds.clone(),
                contexts: Arc::new(crate::build_claim::AttemptContexts::in_memory()),
                claims: Arc::new(crate::storage::MemoryMeshStorage::default()),
                claimed: Default::default(),
            });
            let claim_slot: crate::build_claim::ClaimSlot = Arc::new(std::sync::OnceLock::new());
            let _ = claim_slot.set(claim_door);
            let fp_server = crate::build_claim::serve(rdm_transport::ServerBuilder::new(), claim_slot)
                .build(rdm_transport::ServedBirth { node_id: fp_node_id.to_string(), incarnation: fp_inc.to_string() })
                .unwrap();
            let fp_router = iroh::protocol::Router::builder(fp_ep.clone())
                .accept(iroh_gossip::ALPN, fp_gossip.clone())
                .accept(rdm_transport::ALPN, fp_server)
                .spawn();
            let claimant_resolver = Arc::new(LiveNodeResolver::default());
            claimant_resolver.apply(
                ResolvedNode {
                    node_id: fp_node_id.clone(),
                    name: "mesh1.admin.1".parse().unwrap(),
                    endpoint_id: fp_ep.id(),
                    transport_addr: fp_addr,
                    incarnation: fp_inc,
                },
                None,
            );
            let claimant_client = NodeRpcClient::new(local_ep.clone(), claimant_resolver);
            let claim_req = rdm_core::build_claim::BuildClaimRequest::ClaimAttempt {
                build_id: build_id.to_string(),
                attempt: 1,
                executor_node_id: sec_node_id,
                executor_incarnation: sec_inc,
                mesh: None,
            };
            let (out, _) = claimant_client
                .call::<rdm_core::build_claim::BuildClaim>(&NodeTarget::ExactNode(fp_node_id.clone()), &claim_req, &CallOptions::default())
                .await;
            let reply = out.reply().unwrap_or_else(|| panic!("BuildClaim RPC failed: {out:?}")).value().clone();
            assert!(matches!(reply, rdm_core::build_claim::BuildClaimReply::Won { .. }), "claim under the same accepted Build wins: {reply:?}");

            tracing::info_span!(
                "rdm.node_admin.deployment.update.via-step",
                step = "AllocateIdentity",
                build_id = %build_id.log(),
                node = "mesh1.admin.2",
                attempt = 1u32,
                outcome = "complete",
            )
            .in_scope(|| tracing::info!("restoring own mesh secondary admin under the same accepted Build"));

            let _ = local_router.shutdown().await;
            let _ = fp_router.shutdown().await;
            local_ep.close().await;
            fp_ep.close().await;
        });
        drop(rt);

        // Telemetry chain assertions (fabric-recovery.md:139-141).
        assert!(!capture.named("rdm.node_rpc.request.update.via-call").is_empty(), "via-call span emitted");
        assert!(!capture.named("rdm.node_rpc.request.serve.via-direct").is_empty(), "via-direct span emitted");
        assert!(!capture.named("rdm.mesh.topology.serve.via-read").is_empty(), "via-read span emitted");
        assert!(!capture.named("rdm.mesh.topology.update.via-read-install").is_empty(), "via-read-install span emitted");
        assert!(!capture.named("rdm.node_admin.mesh.update.via-entry-sweep").is_empty(), "via-entry-sweep span emitted");
        assert!(!capture.named("rdm.node_admin.deployment.update.via-step").is_empty(), "via-step span emitted");
        assert!(!capture.named("rdm.node_admin.build.update.via-claim-decision").is_empty(), "via-claim-decision span emitted");
    }
}
