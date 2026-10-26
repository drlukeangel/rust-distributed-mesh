//! Where an owed message went and when it is due again (`rdm-node`).

use crate::model::{EndpointId, IncarnationId, Node, NodeId};

/// Where an owed message (a declaration) went: the exact birth, and the address
/// facts the view held for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    /// The exact birth's node id.
    pub node_id: NodeId,
    /// Its incarnation, as the view held it.
    pub incarnation: Option<IncarnationId>,
    /// The endpoint it is resolved by, as the view held it.
    pub endpoint: Option<EndpointId>,
    /// Its transport address, as the view held it.
    pub addr: Option<std::net::SocketAddr>,
}

impl Destination {
    /// The destination `n` is now.
    pub fn of(n: &Node) -> Self {
        Self { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone(), endpoint: n.endpoint_id.clone(), addr: n.transport_addr }
    }
}

/// The last send of one owed message that ended without the birth admitting it. The message rides
/// one Owed call (node-rpc.md §43): the delivery layer resends it until the birth answers, so a lost
/// route is the layer's to wait out and nothing here tracks one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sent {
    /// Where it went.
    pub to: Destination,
    /// A typed reply came back (the birth refused the op by name): terminal until an eligible event.
    /// Otherwise the call ended because the birth's exit or removal is proven.
    pub typed: bool,
}

/// Is an owed message due again (R-S2)? Sent when it becomes owed, and again only when the birth
/// addressed this node or the destination changed to a deliverable birth. A failed delivery is never
/// a reason: the Owed call is the layer's to deliver (node-rpc.md §43). A target that is not
/// deliverable is sent nothing.
pub fn eligible(sent: Option<&Sent>, now: &Destination, deliverable: bool, addressed: bool) -> bool {
    if addressed {
        return true;
    }
    if !deliverable {
        return false;
    }
    match sent {
        None => true,
        Some(s) => &s.to != now,
    }
}
