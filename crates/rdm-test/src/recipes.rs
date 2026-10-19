//! Rafka's replace recipe as a test caller submits it (lifecycles/node-replace.md): RDM has no
//! replace opcode, and the caller's recipe is node primitives run in order. A node is renamed only
//! while it is live, its connections deleted and its mesh left; it is stopped after.

use rdm_core::build::RecipeStep;
use rdm_core::PathName;

/// The predecessor's half of the replace of `node` (node-replace.md steps 1-2): its connections
/// deleted, its mesh left, renamed `<name>.old` live, then stopped as `<name>.old`.
pub fn predecessor_steps(node: &PathName) -> Vec<RecipeStep> {
    vec![
        RecipeStep::DeleteConnections { node: node.clone() },
        RecipeStep::LeaveMesh { node: node.clone() },
        RecipeStep::Update { node: node.clone(), name: node.renamed() },
        RecipeStep::Stop { node: node.renamed() },
    ]
}

/// The replace of `node` (node-replace.md steps 1-3) and the predecessor's delete at `<name>.old`:
/// [`predecessor_steps`], node.create of the successor at `<name>`, the name the rename released, then
/// node[1].delete at `<name>.old`.
pub fn replace(node: &PathName) -> Vec<RecipeStep> {
    let mut steps = predecessor_steps(node);
    steps.extend([RecipeStep::Create { node: node.clone() }, RecipeStep::Delete { node: node.renamed() }]);
    steps
}

/// The rollback of a replace of `node` whose predecessor is stopped at `<name>.old` (node-replace.md
/// "Rollback"): the successor at `<name>` has its connections deleted, leaves its mesh and is deleted;
/// the predecessor starts as `<name>.old`, has its connections deleted, leaves its mesh, is renamed
/// `<name>` live, then is stopped and started under `<name>`.
pub fn rollback(node: &PathName) -> Vec<RecipeStep> {
    let old = node.renamed();
    vec![
        RecipeStep::DeleteConnections { node: node.clone() },
        RecipeStep::LeaveMesh { node: node.clone() },
        RecipeStep::Delete { node: node.clone() },
        RecipeStep::Start { node: old.clone() },
        RecipeStep::DeleteConnections { node: old.clone() },
        RecipeStep::LeaveMesh { node: old.clone() },
        RecipeStep::Update { node: old, name: node.clone() },
        RecipeStep::Stop { node: node.clone() },
        RecipeStep::Start { node: node.clone() },
    ]
}
