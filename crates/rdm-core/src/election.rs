//! The leadership election's one rule (fabric-node-lifecycle-elections.md §3, §10): among the eligible
//! candidates, the lowest complete NodeId. RDM's node-admins compute their own elections (each mesh's
//! mesh-primary, the fabric-primary) from the facts they hold; the nodes of every other type compute
//! their type's election among themselves by the same rule (Q406; rdm-node's `election`); the
//! mesh-primary's pre-check names a commanded successor by it, the primary excluded
//! (lifecycles/node-primary-update.md step 2).

use crate::ids::NodeId;

/// One candidate as the election sees it: its identity, and whether it is eligible (current
/// `Ready`, elections §2).
#[derive(Debug, Clone, Copy)]
pub struct Candidate<'a> {
    /// The candidate's node id.
    pub node_id: &'a NodeId,
    /// Whether the candidate is eligible.
    pub ready: bool,
}

/// The index of the winner among `candidates`: the eligible candidate with the lowest NodeId; `None`
/// when none is eligible.
pub fn elect(candidates: &[Candidate<'_>]) -> Option<usize> {
    (0..candidates.len()).filter(|&i| candidates[i].ready).min_by_key(|&i| candidates[i].node_id)
}

/// The refusal of node.primary.update for a node type with no other node of its type in the mesh
/// (lifecycles/node-primary-update.md "No-successor cases"): `cannot transfer: no secondary of type
/// <type> in mesh <mesh>`.
pub fn no_secondary(r#type: &str, mesh: &str) -> String {
    format!("cannot transfer: no secondary of type {} in mesh {mesh}", r#type)
}
