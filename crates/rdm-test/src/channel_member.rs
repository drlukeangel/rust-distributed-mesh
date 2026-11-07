//! A member of a mesh's gossip topology, standing alone: one endpoint bound on loopback, the gossip
//! protocol on it, a router accepting gossip, and the topology view joined under a publisher identity
//! of its own. It is what an app-channel cell needs and nothing else: no Node RPC, no admin, no
//! launch. A cell opens an application channel with `member.app_channel(name, &[other.endpoint()])`
//! (`Channels::join` with `ChannelId::app`, then the channel's one receiver), naming its first
//! neighbours by `endpoint_id`. The member owns its endpoint, its gossip and its router; dropping it
//! drops them.
//!
//! An embedding names no iroh crate: a member is reached through another's [`MemberAddr`].

use iroh::protocol::Router;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId, PublisherId};
use rdm_gossip::topology::TopologyView;

/// Where a [`Member`] is reached: the seed another member joins through.
#[derive(Debug, Clone)]
pub struct MemberAddr(iroh::EndpointAddr);

/// A joined member of one mesh's topology view (see the module docs).
pub struct Member {
    /// Separate descriptive path; the publisher uses its minted node ID.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// Explicit declared node type.
    pub r#type: String,
    /// Its topology view: the cache its channels fill.
    pub topology_view: TopologyView,
    /// Its channels: the mesh channel joined, and the application channels it joins.
    pub channels: rdm_gossip::channel::Channels,
    /// The publisher identity its app events are sourced from.
    pub author: PublisherId,
    endpoint: iroh::Endpoint,
    gossip: iroh_gossip::net::Gossip,
    _router: Router,
}

impl Member {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl Member {
    /// A member named `name` with explicit `mesh_name` and `kind`, of the mesh `mesh`
    /// of `fabric`, joined through `seeds`, under a publisher identity minted for it. A member that
    /// cannot bind or join is a defect of the cell: it panics naming why.
    pub async fn join(fabric: &FabricId, mesh: &MeshId, name: rdm_core::PathName, mesh_name: &str, r#type: &str, seeds: &[MemberAddr]) -> Member {
        let transport = iroh::endpoint::QuicTransportConfig::builder().build();
        let endpoint = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().expect("a loopback socket address"), vec![iroh_gossip::ALPN.to_vec()], transport)
            .await
            .unwrap_or_else(|e| panic!("{name:?}: the member's endpoint does not bind: {e}"));
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let node_id = NodeId::mint();
        let author = PublisherId { node_id, incarnation: IncarnationId::mint() };
        let topology_view = rdm_gossip::topology::TopologyView::new(fabric, mesh_name, mesh, author.node_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let channels = rdm_gossip::channel::Channels::new(&gossip, &endpoint, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), seeds.iter().map(|s| s.0.clone()).collect()).await
            .unwrap_or_else(|e| panic!("{name:?}: the member does not join mesh {mesh_name} ({mesh}): {e}"));
        channels.set_author(&author, None);
        Member { path: name.path.clone(), name: name.name.clone(), r#type: r#type.to_string(), topology_view, channels, author, endpoint, gossip, _router: router }
    }

    /// Where this member is reached: its endpoint and the loopback address it bound.
    pub fn addr(&self) -> MemberAddr {
        MemberAddr(iroh::EndpointAddr::new(self.endpoint.id()).with_ip_addr(self.bound_addr()))
    }

    /// The loopback address its endpoint bound.
    pub fn bound_addr(&self) -> std::net::SocketAddr {
        let port = self.endpoint.bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("the member's endpoint binds an IPv4 socket").port();
        ([127, 0, 0, 1], port).into()
    }

    /// Its endpoint's id: how RDM's own transport cells name it to the gossip protocol.
    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.endpoint.id()
    }

    /// This member as another member's topology view holds it (`TopologyView::learn`): where its
    /// `endpoint_id` is reached.
    pub fn digest(&self) -> rdm_core::MeshDigest {
        rdm_core::MeshDigest {
            fabric_id: self.topology_view.fabric_id().clone(),
            node: rdm_core::MeshNode {
                mesh: self.topology_view.mesh_name().to_owned(),
                r#type: self.r#type.clone(),
                node_id: self.author.node_id.clone(),
                path: self.path.clone(),
                name: self.name.clone(),
                endpoint_id: rdm_core::EndpointId(self.endpoint_id().to_string()),
                transport_addr: self.bound_addr(),
                incarnation: self.author.incarnation.clone(),
                supersedes: None,
                runtime: None,
            },
            status: rdm_core::model::NodeStatus::Pending,
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 1,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        }
    }

    /// Its `endpoint_id`: how a channel join names it as a peer.
    pub fn endpoint(&self) -> rdm_core::EndpointId {
        rdm_core::EndpointId(self.endpoint_id().to_string())
    }

    /// Join the application channel `kind` through `peers` and take its publisher and its one receiver:
    /// a second receiver of one channel is refused `ReceiverHeld`.
    pub async fn app_channel(&self, kind: &str, peers: &[rdm_core::EndpointId]) -> Result<(rdm_gossip::app::Publisher, rdm_gossip::app::Subscriber), rdm_gossip::app::ChannelError> {
        let addrs = peers.iter().map(|p| rdm_gossip::channel::peer(p).expect("a peer's endpoint id parses")).collect();
        self.channels.join(rdm_gossip::channel::ChannelId::app(self.topology_view.fabric_id(), kind), addrs).await.map_err(|e| rdm_gossip::app::ChannelError::Subscribe { r#type: kind.into(), detail: e.to_string() })?;
        self.channels.app(kind).await
    }

    /// Its gossip protocol: RDM's own transport cells subscribe to a channel's iroh-gossip `TopicId` directly on it.
    pub fn gossip(&self) -> &iroh_gossip::net::Gossip {
        &self.gossip
    }
}
