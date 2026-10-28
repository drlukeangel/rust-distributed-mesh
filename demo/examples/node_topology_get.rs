//! `node.topology.get` (op `0x1E`): read the topology a node holds, every mesh or one, and read it again
//! cheaply after a reconnect (node-rpc-envelope.md, the topology read).
//!
//! The read streams `Started`, then for each mesh the node holds one snapshot in chunks (the mesh, the
//! publisher, its `topology_version`, and the nodes with the status the mesh-primary holds for each),
//! then `End`. A caller that already holds a mesh's version names it in `since`; a node holding the same
//! version answers `Unchanged` for that mesh instead of the snapshot. That is the read an application
//! makes after it reconnects: one call says whether what it holds is still current.
//!
//! The target is a node by its current `path.name`; any node answers from its own view.
//!
//! Build: `cargo build --example node_topology_get` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/node_topology_get <path.name>
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{Filter, MeshNode, NodeRpc, Nodes, PathName, Peers};
use rdm_core::topology::{SourceVersion, TopologyReply};
use rdm_core::MeshDigest;
use rdm_transport::{CallOptions, NodeTarget};

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-node-topology-get");
    let node = std::env::args().nth(1).ok_or_else(|| anyhow!("usage: node_topology_get <path.name>"))?;
    let node: PathName = node.parse().map_err(|e| anyhow!("{node}: {e}"))?;
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    // The node's birth, so the call by its path.name resolves.
    peers.learn(&Nodes::new(carrier.clone()).get(&Filter::new()).await?);
    let rpc = NodeRpc::new(carrier.client());
    let target = NodeTarget::CurrentPath(node.clone());

    // Every mesh the node holds.
    let mut held: Vec<(String, SourceVersion)> = Vec::new();
    for frame in rpc.topology_get(&target, None, None, &CallOptions::default()).await? {
        match frame {
            TopologyReply::Snapshot { mesh, publisher, topology_version, chunk_index, chunk_count, digests, .. } => {
                let nodes: Vec<String> = digests.into_iter().map(MeshDigest::from).map(|d| format!("{} {:?}", d.node.name, d.status)).collect();
                println!("{mesh} v{topology_version} from {publisher} (chunk {}/{chunk_count}): {nodes:?}", chunk_index + 1);
                if chunk_index == 0 {
                    held.push((mesh, SourceVersion { publisher, topology_version }));
                }
            }
            TopologyReply::End { meshes } => println!("end: {meshes} mesh(es)"),
            TopologyReply::Started => {}
            other => println!("{other:?}"),
        }
    }

    // After a reconnect: each mesh again, naming the version held. `Unchanged` means it is current.
    for (mesh, since) in held {
        for frame in rpc.topology_get(&target, Some(mesh.clone()), Some(since), &CallOptions::default()).await? {
            match frame {
                TopologyReply::Unchanged { mesh, topology_version, .. } => println!("{mesh}: unchanged at v{topology_version}"),
                TopologyReply::Snapshot { mesh, topology_version, chunk_index: 0, .. } => println!("{mesh}: moved on to v{topology_version}"),
                _ => {}
            }
        }
    }
    Ok(())
}
