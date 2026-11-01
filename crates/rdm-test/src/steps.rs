//! Rafka's replace steps as a test caller submits them (lifecycles/node-replace.md): RDM has no
//! replace opcode, and the caller's steps are node primitives run in order. A node is renamed only
//! while it is live, its connections deleted and its mesh left; it is stopped after.

use rdm_core::build::Step;
use rdm_core::PathName;

/// The predecessor's half of the replace of `node` (node-replace.md steps 1-2): its connections
/// deleted, its mesh left, renamed `<name>-old` live, then stopped as `<name>-old`.
pub fn predecessor_steps(node: &PathName) -> Result<Vec<Step>, rdm_core::path::PathNameError> {
    Ok(vec![
        Step::DeleteConnections { node: node.clone() },
        Step::LeaveMesh { node: node.clone() },
        Step::Update { node: node.clone(), name: node.renamed()? },
        Step::Stop { node: node.renamed()? },
    ])
}

/// The replace of `node` with its predecessor kept stopped at `<name>-old` for a rollback
/// (node-replace.md steps 1-3): [`predecessor_steps`], then node.create of the successor at `<name>`,
/// the name the rename released. The Build that creates the successor is what makes the renamed node
/// its predecessor (a name ending `-old` does not), so its later delete or start is a predecessor's.
pub fn replace_keeping_predecessor(node: &PathName, mesh: &str, kind: rdm_core::NodeKind) -> Result<Vec<Step>, rdm_core::path::PathNameError> {
    let mut steps = predecessor_steps(node)?;
    steps.push(Step::Create { mesh: mesh.into(), kind, node: node.clone(), executable: None });
    Ok(steps)
}

/// The replace of `node` (node-replace.md steps 1-3) and the predecessor's delete at `<name>-old`:
/// [`replace_keeping_predecessor`], then `node[1].delete` at `<name>-old`.
pub fn replace(node: &PathName, mesh: &str, kind: rdm_core::NodeKind) -> Result<Vec<Step>, rdm_core::path::PathNameError> {
    let mut steps = replace_keeping_predecessor(node, mesh, kind)?;
    steps.push(Step::Delete { node: node.renamed()? });
    Ok(steps)
}

/// The rollback of a replace of `node` whose predecessor is stopped at `<name>-old` (node-replace.md
/// "Rollback"): the successor at `<name>` has its connections deleted, leaves its mesh and is deleted;
/// the predecessor starts as `<name>-old`, has its connections deleted, leaves its mesh, is renamed
/// `<name>` live, then is stopped and started under `<name>`.
pub fn rollback(node: &PathName) -> Result<Vec<Step>, rdm_core::path::PathNameError> {
    let old = node.renamed()?;
    Ok(vec![
        Step::DeleteConnections { node: node.clone() },
        Step::LeaveMesh { node: node.clone() },
        Step::Delete { node: node.clone() },
        Step::Start { node: old.clone() },
        Step::DeleteConnections { node: old.clone() },
        Step::LeaveMesh { node: old.clone() },
        Step::Update { node: old, name: node.clone() },
        Step::Stop { node: node.clone() },
        Step::Start { node: node.clone() },
    ])
}
