//! Forward (`0x1A`): a control-plane call carried by another node when the receiver is not directly
//! reachable from the caller (node-rpc-envelope.md, Forward).
//!
//! Every typed call that names a route takes a `RouteChoice`: `Direct` dials the receiver's own
//! endpoint; `ViaPeer { carrier, path }` sends the call to the carrier, which makes the inner call to the
//! receiver and carries its answer back. The route is executed exactly as chosen: nothing switches
//! routes for the caller. So the caller decides: here it calls `Direct` first and, when that ends
//! `NotSent` (the request provably reached no handler, so sending it again cannot run it twice), sends
//! the same call through a carrier. A carried call that never committed at the receiver ends `NotSent`
//! too, naming the carrier's account (`Carried`, `ConnectionLost`, `ConnectionNoBudget`).
//!
//! The call is `node.config.update`: the node's whole config, sent to the fabric-primary for the node's
//! exact birth. The carrier is a node the caller reaches and that reaches the receiver; here, by
//! default, a node-admin of another mesh. `--via <path.name>` skips the direct attempt and names the
//! carrier.
//!
//! Build: `cargo build --example forward` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/forward <path.name> <key>=<value> [--via <path.name>]
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{CallEnd, ExactBirth, Fabrics, Filter, MeshNode, NodeRpc, NodeView, Nodes, PathName, Peers};
use rdm_core::NodeKind;
use rdm_transport::{CallOptions, NodeTarget, RouteChoice};

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

fn exact(n: &NodeView) -> Result<ExactBirth> {
    let incarnation = n.incarnation_id.clone().ok_or_else(|| anyhow!("{} has no live birth", n.name))?;
    Ok(ExactBirth { target: NodeTarget::ExactNode(n.node_id.clone()), node_id: n.node_id.clone(), incarnation })
}

/// The route through `carrier`: its node id and its `path.name`. The carrier is dialled by its node id,
/// so a carrier replaced since the read is refused rather than another process substituted.
fn via(carrier: &NodeView) -> RouteChoice {
    RouteChoice::ViaPeer { carrier: carrier.node_id.clone(), path: carrier.name.clone() }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-forward");
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(node), Some(pair)) = (args.first(), args.get(1)) else { return Err(anyhow!("usage: forward <path.name> <key>=<value> [--via <path.name>]")) };
    let node: PathName = node.parse().map_err(|e| anyhow!("{node}: {e}"))?;
    let (key, value) = pair.split_once('=').ok_or_else(|| anyhow!("{pair}: not key=value"))?;
    let forced: Option<PathName> = match (args.get(2).map(String::as_str), args.get(3)) {
        (Some("--via"), Some(c)) => Some(c.parse().map_err(|e| anyhow!("{c}: {e}"))?),
        (None, _) => None,
        _ => return Err(anyhow!("usage: forward <path.name> <key>=<value> [--via <path.name>]")),
    };

    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);
    let fabric = Fabrics::new(carrier.clone()).get(&Filter::new()).await?.into_iter().next().ok_or_else(|| anyhow!("fabric.get answered no fabric"))?;
    let rows = Nodes::new(carrier.clone()).get(&Filter::new()).await?;
    peers.learn(&rows);
    let row = rows.iter().find(|n| n.name == node).ok_or_else(|| anyhow!("no row holds {node}"))?;
    let receiver = rows.iter().find(|n| n.is_fabric_primary).ok_or_else(|| anyhow!("no row names the fabric-primary"))?;
    // A carrier: the one named, else a ready node-admin of another mesh than the fabric-primary's.
    let ready_admin_elsewhere = |n: &&NodeView| n.kind == NodeKind::NodeAdmin && n.mesh != receiver.mesh && n.incarnation_id.is_some() && !n.stopped;
    let carrier_row = match &forced {
        Some(c) => rows.iter().find(|n| &n.name == c).ok_or_else(|| anyhow!("no row holds {c}"))?,
        None => rows.iter().find(ready_admin_elsewhere).ok_or_else(|| anyhow!("{} holds no node-admin outside {} to carry the call", fabric.name, receiver.mesh))?,
    };

    let mut config = row.config.clone();
    config.insert(key.to_string(), value.to_string());
    let rpc = NodeRpc::new(carrier.client());
    let opts = CallOptions::default();
    let send = |route: RouteChoice| {
        let (birth, config) = (exact(row), config.clone());
        let (rpc, receiver, opts) = (&rpc, &receiver.node_id, &opts);
        async move { rpc.config_update(receiver, &route, &birth?, config, opts).await.map_err(anyhow::Error::from) }
    };

    let reply = if forced.is_some() {
        send(via(carrier_row)).await?
    } else {
        match send(RouteChoice::Direct).await {
            Ok(reply) => reply,
            // Provably not sent: the same call through a carrier cannot run it twice.
            Err(e) if matches!(e.downcast_ref::<CallEnd>(), Some(CallEnd::NotSent { .. })) => {
                println!("direct to {}: {e}; carried by {}", receiver.name, carrier_row.name);
                send(via(carrier_row)).await?
            }
            Err(e) => return Err(e),
        }
    };
    println!("node.config.update {key}={value} on {node} at {}: {reply:?}", receiver.name);
    Ok(())
}
