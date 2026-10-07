//! Leadership, as a product sees it (i143.e11.s4): a cohort's seat is the one election,
//! node-admin's (`rafka-node-admin-core` `election.rs`, lowest ReadyForTraffic NodeId per
//! `(mesh, kind)` cohort). A product reads the seat from node-admin's public view and never
//! computes it: nothing here orders NodeIds.

use rafka_mesh_entity::NodeKind;
use rafka_node_admin_client::NodeView;

/// The primary of the `(mesh, kind)` cohort as the view advertises it: the one row with
/// `is_primary`, or `None` while the view advertises none (an election in flight).
pub fn primary_of<'a>(view: &'a [NodeView], mesh: &str, kind: NodeKind) -> Option<&'a NodeView> {
    view.iter().find(|n| n.mesh == mesh && n.kind == kind && n.is_primary)
}

/// The fabric primary as the view advertises it.
pub fn fabric_primary(view: &[NodeView]) -> Option<&NodeView> {
    view.iter().find(|n| n.is_fabric_primary)
}
