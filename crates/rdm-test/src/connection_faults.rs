//! Deterministic data-plane connection faults (`Q62`, `connections.md` §13 E), driven beside
//! the public `Node::connections` API parallel to [`crate::gossip_faults`].

pub use rdm_transport::connections::PreOpenedFault;
use rdm_core::PathName;
use rdm_node::node::RunningNode;

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
