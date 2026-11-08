//! Leadership, as a product sees it (i143.e11.s4): a node-type primary role is the one election, held
//! by the nodes of that type in its mesh among themselves ([`crate::election`], Q406; lowest Ready
//! NodeId per `(mesh, type)`), its first commanded by the mesh-primary's create (Q407). A product reads
//! the primary from RDM's view and never computes it: nothing here orders NodeIds.


use rdm_client::NodeView;

/// The node-type primary of `(mesh, kind)` as the view advertises it: the one row with
/// `is_primary`, or `None` while the view advertises none (an election in flight).
pub fn primary_of<'a>(view: &'a [NodeView], mesh: &str, r#type: &str) -> Option<&'a NodeView> {
    view.iter().find(|n| n.mesh() == mesh && n.r#type == r#type && n.is_primary)
}

/// The fabric primary as the view advertises it.
pub fn fabric_primary(view: &[NodeView]) -> Option<&NodeView> {
    view.iter().find(|n| n.is_fabric_primary)
}
