//! Rafka's replace steps as a test caller submits them (lifecycles/node-replace.md): RDM has no
//! replace opcode, and the caller's steps are node primitives run in order. A node is renamed only
//! while it is live, its connections deleted and its mesh left; it is stopped after.

use rdm_core::build::Step;
use rdm_core::PathName;

/// The predecessor's half of the replace of `node` (node-replace.md steps 1-2): its connections
/// deleted, its mesh left, renamed `<name>.old` live, then stopped as `<name>.old`.
pub fn predecessor_steps(node: &PathName) -> Vec<Step> {
    vec![
        Step::DeleteConnections { node: node.clone() },
        Step::LeaveMesh { node: node.clone() },
        Step::Update { node: node.clone(), name: node.renamed() },
        Step::Stop { node: node.renamed() },
    ]
}

/// The replace of `node` (node-replace.md steps 1-3) and the predecessor's delete at `<name>.old`:
/// [`predecessor_steps`], node.create of the successor at `<name>`, the name the rename released, then
/// `node[1].delete` at `<name>.old`.
pub fn replace(node: &PathName) -> Vec<Step> {
    let mut steps = predecessor_steps(node);
    steps.extend([Step::Create { node: node.clone(), executable: None }, Step::Delete { node: node.renamed() }]);
    steps
}

/// The rollback of a replace of `node` whose predecessor is stopped at `<name>.old` (node-replace.md
/// "Rollback"): the successor at `<name>` has its connections deleted, leaves its mesh and is deleted;
/// the predecessor starts as `<name>.old`, has its connections deleted, leaves its mesh, is renamed
/// `<name>` live, then is stopped and started under `<name>`.
pub fn rollback(node: &PathName) -> Vec<Step> {
    let old = node.renamed();
    vec![
        Step::DeleteConnections { node: node.clone() },
        Step::LeaveMesh { node: node.clone() },
        Step::Delete { node: node.clone() },
        Step::Start { node: old.clone() },
        Step::DeleteConnections { node: old.clone() },
        Step::LeaveMesh { node: old.clone() },
        Step::Update { node: old, name: node.clone() },
        Step::Stop { node: node.clone() },
        Step::Start { node: node.clone() },
    ]
}
