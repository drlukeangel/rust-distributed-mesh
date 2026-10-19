//! A member of a mesh's gossip membership, standing alone: one endpoint bound on loopback, the gossip
//! protocol on it, a router accepting gossip, and the membership joined under a publisher identity
//! of its own. It is what an app-channel cell needs and nothing else: no Node RPC, no admin, no
//! launch. A cell joins its declared app types and opens a channel with
//! `member.membership.join_channel(ty)`. The member owns its endpoint, its gossip and its router;
//! dropping it drops them.
//!
//! An embedding names no iroh crate: a member is reached through another's [`MemberAddr`].

use iroh::protocol::Router;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId, PublisherId};
use rdm_transport::membership::Membership;

/// Where a [`Member`] is reached: the seed another member joins through.
#[derive(Debug, Clone)]
pub struct MemberAddr(iroh::EndpointAddr);

/// A joined member of one mesh's membership (see the module docs).
pub struct Member {
    /// Its membership: the mesh channel joined and its declared app types held.
    pub membership: Membership,
    /// The publisher identity its app events are sourced from.
    pub author: PublisherId,
    endpoint: iroh::Endpoint,
    gossip: iroh_gossip::net::Gossip,
    _router: Router,
}

impl Member {
    /// A member named `name` (a `path.name`; its first segment is the mesh's name) of the mesh `mesh`
    /// of `fabric`, joined through `seeds` and declaring `types`, under a publisher identity minted
    /// for it. A member that cannot bind or join is a defect of the cell: it panics naming why.
    pub async fn join(fabric: &FabricId, mesh: &MeshId, name: &str, seeds: &[MemberAddr], types: &[&str]) -> Member {
        let _ = (fabric, mesh, seeds, types, IncarnationId::mint, NodeId::mint);
        panic!("{name}: rdm_test::channel_member::Member::join is not built yet")
    }

    /// Where this member is reached: its endpoint and the loopback address it bound.
    pub fn addr(&self) -> MemberAddr {
        MemberAddr(iroh::EndpointAddr::new(self.endpoint.id()))
    }

    /// The loopback address its endpoint bound.
    pub fn bound_addr(&self) -> std::net::SocketAddr {
        unimplemented!("not built yet")
    }

    /// Its endpoint's id: how RDM's own transport cells name it to the gossip protocol.
    pub fn endpoint_id(&self) -> iroh::EndpointId {
        self.endpoint.id()
    }

    /// Its gossip protocol: RDM's own transport cells subscribe to a channel's raw topic on it.
    pub fn gossip(&self) -> &iroh_gossip::net::Gossip {
        &self.gossip
    }
}
