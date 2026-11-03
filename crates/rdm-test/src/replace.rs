//! Rafka's replace as a test caller runs it (lifecycles/node-replace.md): node commands free the name,
//! then one Build adds the successor. RDM has no replace opcode and the sequence is not a Build of its
//! own. A node is renamed only while it is live, its connections deleted and its mesh left; it is
//! stopped after.

use crate::estate::Estate;
use rdm_client::{CallEnd, Frame, WorkflowStream};
use rdm_core::{NodeId, PathName};

/// Read `stream` to its terminal frame: `Ok` on `Complete`, `Err` naming `what` and how it ended.
async fn complete(what: &str, stream: Result<WorkflowStream, CallEnd>) -> Result<Vec<Frame>, String> {
    let mut stream = stream.map_err(|e| format!("{what} was refused: {e}"))?;
    let mut frames = Vec::new();
    while let Some(f) = stream.next().await {
        let f = f.map_err(|e| format!("{what}'s stream broke: {e}"))?;
        let terminal = f.is_terminal();
        frames.push(f);
        if terminal {
            break;
        }
    }
    match frames.last() {
        Some(Frame::Complete) => Ok(frames),
        other => Err(format!("{what} ended {other:?}: {frames:?}")),
    }
}

/// The predecessor's half of the replace of `node` (node-replace.md steps 1-2), each a node command of
/// its mesh-primary: its connections deleted `{ All }`, its mesh left, renamed `<name>-old` live, then
/// stopped as `<name>-old`. Answers the predecessor's node.id.
pub async fn free_the_name(estate: &Estate, node: &PathName) -> Result<NodeId, String> {
    let nodes = estate.nodes_obj().await;
    let before = estate.node_view(node).await;
    let old = node.renamed().map_err(|e| format!("{node:?} has no <name>-old: {e:?}"))?;
    complete(&format!("node.connection.delete {{ All }} of {node:?}"), nodes.delete_connections(node, &rdm_core::status::ConnectionScope::All).await).await?;
    complete(&format!("node.mesh.leave of {node:?}"), nodes.leave_mesh(node).await).await?;
    let primary = primary_of(estate, &before.mesh).await?;
    let incarnation = before.incarnation_id.clone().ok_or_else(|| format!("{node:?} has no known birth"))?;
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(before.node_id.clone()), node_id: before.node_id.clone(), incarnation };
    match nodes.name_update(&birth, &primary, &rdm_transport::RouteChoice::Direct, &old).await {
        Ok(rdm_core::status::StatusReply::NameUpdated { .. }) => {}
        other => return Err(format!("node.name.update of {node:?} to {old:?} ended {other:?}")),
    }
    complete(&format!("node.stop of {old:?}"), nodes.stop(&old).await).await?;
    Ok(before.node_id)
}

/// The mesh-primary of `mesh`, as node.get names it.
async fn primary_of(estate: &Estate, mesh: &str) -> Result<NodeId, String> {
    let found = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", [mesh]).eq("type", ["node-admin"]).eq("is_primary", ["true"])).await.map_err(|e| format!("node.get of {mesh}'s primary: {e}"))?;
    found.into_iter().next().map(|n| n.node_id).ok_or_else(|| format!("node.get names no primary of {mesh}"))
}

/// The successor's Build (node-replace.md step 3): `Create { nodes: [node] }` at the name the
/// predecessor released, its type's executable, read to its `Complete`.
pub async fn add_successor(estate: &Estate, node: &PathName, mesh: &str, r#type: &str) -> Result<Vec<Frame>, String> {
    let successor = rdm_core::build::Nodes {
        path: node.path.clone(),
        name: node.name.clone(),
        mesh: mesh.into(),
        r#type: r#type.into(),
        storage_persistence: None,
        executable: Some(estate.executables.of(r#type)),
        tags: Default::default(),
        config: Default::default(),
    };
    complete(&format!("the Create of the successor at {node:?}"), estate.nodes_obj().await.create(vec![successor]).await).await
}

/// The replace of `node` with its predecessor kept stopped at `<name>-old` for a rollback
/// (node-replace.md steps 1-3): [`free_the_name`], then [`add_successor`]. Answers the predecessor's
/// node.id.
pub async fn replace_keeping_predecessor(estate: &Estate, node: &PathName, mesh: &str, r#type: &str) -> Result<NodeId, String> {
    let predecessor = free_the_name(estate, node).await?;
    add_successor(estate, node, mesh, r#type).await?;
    Ok(predecessor)
}

/// The replace of `node` and the predecessor's delete (node-replace.md steps 1-4):
/// [`replace_keeping_predecessor`], then `Delete { node_ids: [the predecessor] }`. Answers the
/// predecessor's node.id.
pub async fn replace(estate: &Estate, node: &PathName, mesh: &str, r#type: &str) -> Result<NodeId, String> {
    let predecessor = replace_keeping_predecessor(estate, node, mesh, r#type).await?;
    complete(&format!("the Delete of the predecessor {predecessor}"), estate.nodes_obj().await.delete(vec![predecessor.clone()]).await).await?;
    Ok(predecessor)
}
