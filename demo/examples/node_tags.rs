//! `node.tags.create` and `node.tags.delete`: label a node, find nodes by label, remove the label
//! (node-rpc-envelope.md, node properties).
//!
//! A tag is a label on a node's row. The node's mesh-primary is the one writer of its mesh's rows, so
//! both calls go to it for the node's exact birth (`node.id` and incarnation): it writes the row and the
//! label's key of the tags index. A read finds nodes by label with a `tags` pair in its filter; one
//! pair's values are any of, repeated pairs are all of.
//!
//! Build: `cargo build --example node_tags` in `demo/`. Run against a live estate
//! (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_tags <path.name> <label>
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{ExactBirth, Filter, MeshNode, NodeView, Nodes, PathName, Peers};
use rdm_core::status::TagScope;
use rdm_core::NodeKind;
use rdm_transport::{NodeTarget, RouteChoice};
use std::collections::BTreeSet;

/// Enter at the node-admin whose birth the estate printed (`RDM_NODE_BIRTH=` on its stdout;
/// `scripts/demo-estate.sh up` keeps it in `<root>/birth.json`).
async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

/// A call to a node is for its exact birth: the node id and the incarnation its row holds.
fn exact(n: &NodeView) -> Result<ExactBirth> {
    let incarnation = n.incarnation_id.clone().ok_or_else(|| anyhow!("{} has no live birth", n.name))?;
    Ok(ExactBirth { target: NodeTarget::ExactNode(n.node_id.clone()), node_id: n.node_id.clone(), incarnation })
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-tags");
    let mut args = std::env::args().skip(1);
    let (Some(node), Some(label)) = (args.next(), args.next()) else { return Err(anyhow!("usage: node_tags <path.name> <label>")) };
    let node: PathName = node.parse().map_err(|e| anyhow!("{node}: {e}"))?;
    let (peers, entry) = enter().await?;
    let nodes = Nodes::new(peers.carrier_at(entry));

    // The node's row and its mesh-primary's, from one read of its mesh.
    let mesh = nodes.get(&Filter::new().eq("mesh", [node.mesh.clone()])).await?;
    peers.learn(&mesh);
    let row = mesh.iter().find(|n| n.name == node).ok_or_else(|| anyhow!("no row holds {node}"))?;
    let primary = mesh.iter().find(|n| n.kind == NodeKind::NodeAdmin && n.is_primary).ok_or_else(|| anyhow!("no row names {}'s primary", node.mesh))?;

    let labels = BTreeSet::from([label.clone()]);
    let created = nodes.tags_create(&exact(row)?, &primary.node_id, &RouteChoice::Direct, &labels).await?;
    println!("node.tags.create {label} on {node}: {created:?}");

    let tagged = nodes.get(&Filter::new().eq("tags", [label.clone()])).await?;
    println!("node.get [tags: {label}]: {:?}", tagged.iter().map(|n| n.name.to_string()).collect::<Vec<_>>());

    let deleted = nodes.tags_delete(&exact(row)?, &primary.node_id, &RouteChoice::Direct, &TagScope::Labels(labels)).await?;
    println!("node.tags.delete {label} on {node}: {deleted:?}");
    Ok(())
}
