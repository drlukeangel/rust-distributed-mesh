//! The rpc node runtime: bind the assigned endpoints, serve Node RPC on every
//! slot, join fabric membership, publish the digest.

use rafka_mesh_entity::launch::Launch;
use anyhow::{anyhow, Context, Result};
use iroh::protocol::Router;
use iroh::{EndpointAddr, SecretKey};
use rafka_mesh_entity::{EndpointSet, MemberStatus, MeshDigest, MeshNode};
use rafka_mesh_transport::membership::Membership;
use rafka_node_rpc::{NodeRpcServer, ServerBuilder};
use rafka_node_rpc_contract::catalog::TagOwner;
use rafka_node_rpc_contract::echo::{Echo, EchoReply, EchoRequest};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The node's transport key, kept in its data dir: a restart keeps it, a
/// replacement (fresh data dir) gets a new one.
pub fn load_or_mint_key(data_dir: &Path) -> Result<SecretKey> {
    std::fs::create_dir_all(data_dir).with_context(|| format!("data dir {}", data_dir.display()))?;
    let path = data_dir.join("node-key");
    if let Ok(hexkey) = std::fs::read_to_string(&path) {
        let bytes: [u8; 32] = hex::decode(hexkey.trim())
            .map_err(|e| anyhow!("{}: {e}", path.display()))?
            .try_into()
            .map_err(|_| anyhow!("{}: not 32 bytes", path.display()))?;
        return Ok(SecretKey::from_bytes(&bytes));
    }
    let key = SecretKey::generate();
    std::fs::write(&path, hex::encode(key.to_bytes()))?;
    Ok(key)
}

/// Core Echo is served by every rpc node.
pub fn core_protocols(b: ServerBuilder) -> ServerBuilder {
    b.serve::<Echo, _, _>(TagOwner::Core, |_peer, req: EchoRequest| async move {
        let EchoRequest::Echo { payload, .. } = req;
        Ok(EchoReply::Echoed { payload })
    })
}

pub struct RunningNode {
    pub routers: Vec<Router>,
    pub membership: Membership,
    pub server: NodeRpcServer,
    pub status: Arc<Mutex<MemberStatus>>,
    pub digest: MeshDigest,
    gossip: iroh_gossip::net::Gossip,
    publisher: tokio::task::JoinHandle<()>,
}

/// The digest key carrying a node's in-flight handler count.
pub const IN_FLIGHT: &str = "in_flight";

/// `RAFKA_DRAIN_DEADLINE_MS` (default 5000): how long a stopping node waits
/// for in-flight handlers. Strictly shorter than node-admin's stop grace.
/// Drain deadline plus the leave linger
/// ([`rafka_mesh_transport::membership::leave_linger_from_env`]) stay inside
/// node-admin's stop grace.
pub use rafka_mesh_transport::membership::leave_linger_from_env;

pub fn drain_deadline_from_env() -> Duration {
    Duration::from_millis(std::env::var("RAFKA_DRAIN_DEADLINE_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(5000))
}

impl RunningNode {
    /// Two-phase shutdown, phase one (node-rpc §35): new calls get a typed
    /// `Draining`, the digest says `Draining` with the in-flight count, and
    /// existing handlers may finish until `deadline`. Returns how many were
    /// still running at the deadline.
    pub async fn drain(&self, deadline: Duration) -> u64 {
        self.server.drain();
        *self.status.lock().unwrap() = MemberStatus::Draining;
        let stats = self.server.stats();
        let until = tokio::time::Instant::now() + deadline;
        loop {
            let in_flight = rafka_node_rpc::ServerStats::get(&stats.in_flight);
            let mut d = self.digest.clone();
            d.status = MemberStatus::Draining;
            d.emitted_unix_ms = now_ms();
            d.extra.insert(IN_FLIGHT.into(), in_flight.to_string());
            let _ = self.membership.publish(&d).await;
            if in_flight == 0 || tokio::time::Instant::now() >= until {
                return in_flight;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Publish `Leaving` and stop serving.
    /// Phase two: say `Leaving` on the fabric, keep saying it every
    /// `LEAVE_EVERY` for `linger`, then leave gossip and close every endpoint.
    ///
    /// iroh-gossip queues a broadcast and acknowledges nothing, and closing
    /// the endpoint drops whatever is still unsent; the linger keeps the
    /// node on the fabric long enough for its own `Leaving` to go out.
    pub async fn stop(self, linger: Duration) {
        *self.status.lock().unwrap() = MemberStatus::Leaving;
        let (digest, membership) = (self.digest.clone(), self.membership.clone());
        rafka_mesh_transport::membership::announce_leaving(linger, rafka_mesh_transport::membership::LEAVE_EVERY, || {
            let mut d = digest.clone();
            d.status = MemberStatus::Leaving;
            d.emitted_unix_ms = now_ms();
            let m = membership.clone();
            async move {
                let _ = m.publish(&d).await;
            }
        })
        .await;
        self.publisher.abort();
        let _ = self.gossip.shutdown().await;
        for r in self.routers {
            let _ = r.shutdown().await;
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Bring the node up exactly as `launch` says.
pub async fn start(launch: &Launch, register: impl FnOnce(ServerBuilder) -> ServerBuilder) -> Result<RunningNode> {
    let key = load_or_mint_key(&launch.data_dir)?;
    let server = register(core_protocols(ServerBuilder::new()))
        .seal(launch.endpoints.first().map(|e| e.slot.clone()).unwrap_or_default())
        .map_err(|e| anyhow!("protocol catalog refused to seal: {e:?}"))?;
    let mut routers = Vec::new();
    let mut gossip = None;
    for (i, slot) in launch.endpoints.iter().enumerate() {
        let ep = rafka_node_rpc::endpoint::bind(key.clone(), slot.addr)
            .await
            .with_context(|| format!("slot {} cannot bind its assigned {}", slot.slot, slot.addr))?;
        let mut b = Router::builder(ep.clone()).accept(rafka_node_rpc::ALPN, server.for_slot(slot.slot.clone()));
        if i == 0 {
            let g = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
            b = b.accept(iroh_gossip::ALPN, g.clone());
            gossip = Some((g, ep));
        }
        routers.push(b.spawn());
    }
    let (g, ep0) = gossip.ok_or_else(|| anyhow!("a node needs at least one endpoint slot"))?;
    let seeds: Vec<EndpointAddr> = launch
        .seeds
        .iter()
        .filter_map(|(k, a)| k.parse::<iroh::PublicKey>().ok().map(|pk| EndpointAddr::new(pk).with_ip_addr(*a)))
        .collect();
    let membership = Membership::join(&g, &ep0, &launch.fabric, seeds).await?;
    let digest = MeshDigest {
        fabric: launch.fabric.clone(),
        node: MeshNode {
            node_id: launch.node_id.clone(),
            name: launch.name.clone(),
            fabric_id: rafka_mesh_entity::FabricId(key.public().to_string()),
            incarnation: launch.incarnation.clone(),
            supersedes: launch.supersedes.clone(),
            endpoints: EndpointSet(launch.endpoints.clone()),
        },
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        emitted_unix_ms: now_ms(),
        // Ready for traffic from birth: the election claim is this instant.
        extra: [(rafka_mesh_entity::READY_SINCE.to_string(), now_ms().to_string())].into_iter().collect(),
    };
    let status = Arc::new(Mutex::new(MemberStatus::ReadyForTraffic));
    let (d, st, stats) = (digest.clone(), status.clone(), server.stats());
    let publisher = membership.publish_every(Duration::from_millis(500), move || {
        let mut d = d.clone();
        d.status = *st.lock().unwrap();
        d.emitted_unix_ms = now_ms();
        d.extra.insert(IN_FLIGHT.into(), rafka_node_rpc::ServerStats::get(&stats.in_flight).to_string());
        d
    });
    Ok(RunningNode { routers, membership, server, status, digest, gossip: g, publisher })
}
