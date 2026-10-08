//! A node-admin entering an EXISTING mesh (node-admin-lifecycle.md §4): one code path for a
//! recovering mesh's first admin (after its maker's JoinNode and topology) and for a fabric
//! primary reborn into its own mesh (from its durable map, with no maker).
//!
//! 1. Read topology: [`get_topology`] is the one place "get the topology from <source>" lives.
//!    It reads the maker's JoinNode answer, a durable map, or a LOCAL node of the own mesh (a
//!    gossip join through that node as the seed; its digests fill the book). When the catalogued
//!    topology read (op 0x1E) lands, this function is the one swap.
//! 2. Connect to a local node of the own mesh: the first node of the map that answers a Ping.
//!    Members can exit while a mesh has no admin, so the maker's map alone is not the current read.
//! 3. The own-mesh sweep: ONE bounded core Ping to every node of the own mesh, once, on entry.
//!    A reply proves a live exact birth. No reply is "not yet reached", never death: the node
//!    stays in the map and goes to the standard decommission ([`decommission_unreached`]).

use crate::model::{EndpointId, IncarnationId, NodeId, PathName};
use crate::storage::NodeRecord;
use rafka_mesh_transport::entry::EntryAnswer;
use rafka_mesh_transport::membership::Membership;
use rafka_node_rpc::{Budget, CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A node's fixed deadline in the sweep.
pub const SWEEP_DEADLINE: Duration = Duration::from_secs(2);
/// The most nodes pinged at once.
pub const SWEEP_CONCURRENCY: usize = 8;

/// One birth of the topology map: what a Ping needs to reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapNode {
    pub node_id: NodeId,
    pub name: PathName,
    pub endpoint_id: EndpointId,
    pub transport_addr: SocketAddr,
    pub incarnation: IncarnationId,
    /// The birth is not in a lifecycle transition (being born, restarted, drained or leaving):
    /// only a settled birth is expected to answer a Ping.
    pub settled: bool,
    /// What the source knew: a birth the source holds ready is ready, anything else settled is
    /// not yet reached.
    pub ready: bool,
}

impl MapNode {
    fn resolved(&self) -> Option<ResolvedNode> {
        Some(ResolvedNode {
            node_id: self.node_id.clone(),
            name: self.name.clone(),
            endpoint_id: self.endpoint_id.0.parse().ok()?,
            transport_addr: self.transport_addr,
            incarnation: self.incarnation.clone(),
        })
    }

    /// The birth as the view holds it until membership speaks for it: `status` is what the map
    /// knows (`ReadyForTraffic` for a birth that answered a Ping, `PendingReconnect` for one not
    /// yet reached; never `Dead`).
    pub fn as_node(&self, status: crate::model::NodeStatus, provider: crate::model::ProviderKind) -> crate::model::Node {
        let mut node = crate::model::Node::allocated(self.name.clone());
        node.node_id = self.node_id.clone();
        node.endpoint_id = Some(self.endpoint_id.clone());
        node.incarnation_id = Some(self.incarnation.clone());
        node.transport_addr = Some(self.transport_addr);
        node.provider = Some(provider);
        node.status = status;
        node
    }

    fn gossip_addr(&self) -> Option<iroh::EndpointAddr> {
        let key = self.endpoint_id.0.parse::<iroh::PublicKey>().ok()?;
        Some(iroh::EndpointAddr::new(key).with_ip_addr(self.transport_addr))
    }
}

/// Where a topology is read from.
pub enum TopologySource<'a> {
    /// The maker's answer to this admin's JoinNode.
    Maker(&'a EntryAnswer),
    /// This admin's durable map (nodes.storage), with no maker.
    DurableMap(&'a [NodeRecord]),
    /// A node of the own mesh: this admin joins the mesh channel with it as the seed and takes the
    /// digests the channel delivers.
    LocalNode { node: &'a MapNode, membership: &'a Membership },
}

/// The topology `source` holds, every mesh, as births a Ping can reach. `Err` names what failed.
pub async fn get_topology(source: TopologySource<'_>) -> Result<Vec<MapNode>, String> {
    match source {
        TopologySource::Maker(answer) => {
            let t: crate::topology::Topology = serde_json::from_value(answer.topology.clone()).map_err(|e| format!("the maker {} answered no topology: {e}", answer.served_by))?;
            Ok(t.nodes
                .into_iter()
                .filter_map(|n| {
                    let settled = matches!(n.status, crate::model::NodeStatus::ReadyForTraffic | crate::model::NodeStatus::PendingReconnect | crate::model::NodeStatus::Dead);
                    let ready = n.status == crate::model::NodeStatus::ReadyForTraffic;
                    Some(MapNode { node_id: n.node_id, name: n.name, endpoint_id: n.endpoint_id?, transport_addr: n.transport_addr?, incarnation: n.incarnation_id?, settled, ready })
                })
                .collect())
        }
        TopologySource::DurableMap(rows) => Ok(rows
            .iter()
            .map(|r| MapNode { node_id: r.node_id.clone(), name: r.name.clone(), endpoint_id: r.endpoint_id.clone(), transport_addr: r.transport_addr, incarnation: r.incarnation_id.clone(), settled: true, ready: false })
            .collect()),
        TopologySource::LocalNode { node, membership } => {
            let addr = node.gossip_addr().ok_or_else(|| format!("{}: its endpoint id {} is not an iroh key", node.name, node.endpoint_id.0))?;
            membership.join_peers(vec![addr]).await.map_err(|e| format!("joining the mesh channel through {}: {e}", node.name))?;
            let floor = membership.book.staleness_floor();
            let until = Instant::now() + floor;
            loop {
                let heard = membership.book.current(floor);
                if heard.iter().any(|d| d.node.node_id == node.node_id) {
                    return Ok(heard
                        .into_iter()
                        .map(|d| MapNode {
                            settled: d.status == rafka_mesh_entity::MemberStatus::ReadyForTraffic,
                            ready: d.status == rafka_mesh_entity::MemberStatus::ReadyForTraffic,
                            node_id: d.node.node_id,
                            name: d.node.name,
                            endpoint_id: d.node.endpoint_id,
                            transport_addr: d.node.transport_addr,
                            incarnation: d.node.incarnation,
                        })
                        .collect());
                }
                if Instant::now() >= until {
                    return Err(format!("{} joined the channel but no digest of it arrived within {} ms", node.name, floor.as_millis()));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// What the entering admin has.
pub struct EntryCtx {
    pub me: PathName,
    pub me_id: NodeId,
    pub client: Arc<NodeRpcClient>,
    pub resolver: Arc<LiveNodeResolver>,
    pub membership: Membership,
}

/// What the sweep found.
#[derive(Debug, Clone, Default)]
pub struct SweepReport {
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
    let req = rafka_node_rpc_contract::ping::PingRequest::Ping { payload: b"entry-sweep".to_vec() };
    let opts = CallOptions { budget: Budget::Overall(SWEEP_DEADLINE), ..CallOptions::default() };
    let (out, _) = ctx.client.call::<rafka_node_rpc_contract::ping::Ping>(&NodeTarget::ExactNode(n.node_id.clone()), &req, &opts).await;
    if out.reply().is_some() {
        Ok(())
    } else {
        Err(out.name().to_string())
    }
}

/// Enter the existing mesh of `ctx.me` from `held`, the topology its source gave: connect to a
/// local node, read the current topology from it, then sweep the own mesh once.
pub async fn enter_existing_mesh(ctx: &EntryCtx, source: &'static str, held: Vec<MapNode>) -> SweepReport {
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
    for n in map.iter().filter(|n| n.settled) {
        if ping(ctx, n).await.is_ok() {
            local = Some(n.clone());
            break;
        }
    }
    if let Some(l) = &local {
        match get_topology(TopologySource::LocalNode { node: l, membership: &ctx.membership }).await {
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
    let swept: Vec<MapNode> = map.iter().filter(|n| n.settled).cloned().collect();
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
                incarnation_id = %n.incarnation.0,
                outcome = %outcome,
                sweeper = %ctx.me,
            )
            .in_scope(|| tracing::info!("no reply to the entry sweep's Ping: not yet reached, not dead"));
        }
        tracing::info!("the own-mesh sweep on entering the mesh");
    });
    report
}

/// What the standard decommission of an unreached node needs.
pub struct Decommission {
    pub me: PathName,
    pub topology: Arc<tokio::sync::RwLock<crate::topology::Topology>>,
    pub accepted: Arc<crate::accepted::AcceptedStore>,
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
    pub control: Arc<crate::http::ControlPlane>,
}

/// How long one node's decommission waits for the Build to be free and its attempt to complete.
pub const DECOMMISSION_WAIT: Duration = Duration::from_secs(180);

/// A node that did not answer the sweep enters the STANDARD decommission, one attempt per node in
/// path order: a requested replacement of the birth, an attempt of the accepted Build opened at
/// the fabric primary (itself, or over its control API), whose retire pipeline drains when
/// sendable, terminates and inspects the exact runtime, and publishes `NodeDeleted` only on a
/// proven exit; the node is then created again as the accepted Build requires. A node heard again
/// meanwhile is not decommissioned.
pub async fn decommission_unreached(d: &Decommission, mut nodes: Vec<MapNode>) {
    nodes.sort_by_key(|n| n.name.to_string());
    for n in nodes {
        let started = Instant::now();
        let outcome = loop {
            let view = d.topology.read().await.clone();
            if view.node(&n.name).is_some_and(|x| x.status.is_live() || x.incarnation_id.as_ref() != Some(&n.incarnation)) {
                break "heard-or-replaced".to_string();
            }
            if started.elapsed() > DECOMMISSION_WAIT {
                break format!("no attempt completed within {} s", DECOMMISSION_WAIT.as_secs());
            }
            let Some(fp) = view.fabric_primary().cloned() else {
                tokio::time::sleep(Duration::from_millis(300)).await;
                continue;
            };
            let opened: Result<u32, String> = if fp.name == d.me {
                d.control.open_attempt("sweep decommission", crate::build_state::AttemptReason::Replace, n.name.clone(), true).await.map(|o| o.attempt).map_err(|r| format!("{r:?}"))
            } else {
                match fp.admin_api_base.as_deref() {
                    Some(base) => rafka_node_admin_client::NodeAdminClient::new(base).replace(&n.name).await.map(|a| a.attempt).map_err(|e| e.to_string()),
                    None => Err(format!("the fabric primary {} advertises no control API", fp.name)),
                }
            };
            match opened {
                Ok(attempt) => {
                    // The attempt is the Build's: wait until it has ended, complete or failed.
                    let ended = loop {
                        let b = d.accepted.current(&*d.builds).await;
                        if let Some(b) = b.filter(|b| b.attempt >= attempt && matches!(b.state, crate::build_state::BuildState::Complete | crate::build_state::BuildState::Failed)) {
                            break Some(b);
                        }
                        if started.elapsed() > DECOMMISSION_WAIT {
                            break None;
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    };
                    break match ended {
                        Some(b) if b.state == crate::build_state::BuildState::Failed => format!("attempt {attempt} failed: {}", b.last_failure.unwrap_or_default()),
                        Some(_) => format!("attempt {attempt}"),
                        None => format!("attempt {attempt} did not end within {} s", DECOMMISSION_WAIT.as_secs()),
                    };
                }
                // One Build in flight, or the seat is moving: wait and ask again.
                Err(e) if e.contains("BuildInProgress") || e.contains("409") || e.contains("NotAuthority") || e.contains("Unavailable") || e.contains("build-in-progress") => {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                Err(e) => break format!("refused: {e}"),
            }
        };
        tracing::info_span!(
            "rdm.node_admin.node.update.via-sweep-decommission",
            node = %n.name,
            node_id = %n.node_id,
            incarnation_id = %n.incarnation.0,
            sweeper = %d.me,
            outcome = %outcome,
            elapsed_ms = started.elapsed().as_millis() as u64,
        )
        .in_scope(|| tracing::info!("a node that did not answer the entry sweep went through the standard decommission"));
    }
}
