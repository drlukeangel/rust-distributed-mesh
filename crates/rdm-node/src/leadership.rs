//! Leadership, as a product sees it (i143.e11.s4): a node-type primary role is the one election,
//! node-admin's (`rafka-node-admin-core` `election.rs`, lowest Ready NodeId per
//! `(mesh, kind)` node type). A product reads the primary from node-admin's public view and never
//! computes it: nothing here orders NodeIds.

use rdm_core::NodeKind;
use rdm_client::NodeView;

/// The node-type primary of `(mesh, kind)` as the view advertises it: the one row with
/// `is_primary`, or `None` while the view advertises none (an election in flight).
pub fn primary_of<'a>(view: &'a [NodeView], mesh: &str, kind: NodeKind) -> Option<&'a NodeView> {
    view.iter().find(|n| n.mesh == mesh && n.kind == kind && n.is_primary)
}

/// The fabric primary as the view advertises it.
pub fn fabric_primary(view: &[NodeView]) -> Option<&NodeView> {
    view.iter().find(|n| n.is_fabric_primary)
}
