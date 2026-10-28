//! Deterministic data-plane connection faults (`Q62`, `connections.md` §13 E), driven beside
//! the public `Node::connections` API parallel to [`crate::gossip_faults`], and the Node RPC
//! client's direct dial block beside them.

pub use rdm_transport::connections::PreOpenedFault;
use rdm_core::PathName;
use rdm_node::node::RunningNode;
use rdm_transport::{NodeResolver, NodeTarget, ResolvedNode};

/// Block Direct dials from `source` toward `(destination, alpn)`, answering
/// `ConnectError::NotSent` and recording `Direct Failed` as an unreachable direct dial does.
pub fn block_direct(source: &RunningNode, destination: &PathName, alpn: &[u8]) {
    source
        .data_connections_for_testkit()
        .block_direct_dial(destination, alpn);
}

/// Release a Direct dial block on `source` toward `(destination, alpn)`.
pub fn unblock_direct(source: &RunningNode, destination: &PathName, alpn: &[u8]) {
    source
        .data_connections_for_testkit()
        .unblock_direct_dial(destination, alpn);
}

/// Arm a one-shot pre-`Opened` fault on `destination`'s registered `alpn` handler.
pub fn fault_pre_opened_once(destination: &RunningNode, alpn: &[u8], fault: PreOpenedFault) {
    destination
        .data_connections_for_testkit()
        .fault_pre_opened_once(alpn, fault);
}

/// The birth `source`'s own view names at `destination`, whose process a Node RPC block keys.
fn held_birth(source: &RunningNode, destination: &PathName) -> ResolvedNode {
    source
        .node_rpc
        .resolver
        .resolve(&NodeTarget::CurrentPath(destination.clone()))
        .unwrap_or_else(|f| panic!("{destination} does not resolve in the source's view: {f:?}"))
}

/// Block `source`'s direct Node RPC dials to `destination`, the process its view names there: a
/// direct call ends `NotSent` `Connection` as an unreachable dial does, and a connection its process
/// client holds pooled to it is closed. A call carried through another node of `source`'s mesh
/// (`call_routed` `ViaPeer`) still reaches it; every other destination is untouched. Returns the
/// birth blocked.
pub fn block_direct_node_rpc(source: &RunningNode, destination: &PathName) -> ResolvedNode {
    let birth = held_birth(source, destination);
    source.node_rpc.client.block_direct_dial(birth.endpoint_id);
    birth
}

/// Release [`block_direct_node_rpc`] on `source` toward `destination`: the next direct call dials.
pub fn unblock_direct_node_rpc(source: &RunningNode, destination: &PathName) {
    let birth = held_birth(source, destination);
    source.node_rpc.client.unblock_direct_dial(&birth.endpoint_id);
}
