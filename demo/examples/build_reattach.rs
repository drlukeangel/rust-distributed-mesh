//! Reattach to a Build after the caller lost its stream (node-rpc-envelope.md "Frames, durable events
//! and reattachment"; build.md: a live run is reattached, never re-run).
//!
//! A Build call answers `Started { build_id, attempt }` and then streams its step frames to the
//! terminal. The frames are the executor's receipts, recorded before they are sent, so a caller that
//! loses the stream (its connection closed, its process restarted, the fabric-primary moved) loses
//! nothing: holding the Build id, it reads the receipts with `build.get [id]` and opens the stream
//! again by re-submitting that id, which never starts a second Build. `BuildCarrier::reattach` is that
//! pair; `BuildCarrier::follow` follows a stream to its terminal and reattaches on its own whenever the
//! stream breaks or ends `FabricPrimaryMoved`, keeping each completed step once.
//!
//! Here the Build is a Delete of one node, named by its node.id (build.md §1): the node is stopped
//! and leaves the fabric. The caller keeps only its Build id from the first stream and drops the
//! stream, as a restarted caller would; then it reattaches by id and follows the Build to its end.
//!
//! Build: `cargo build --example build_reattach` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/build_reattach <path> <name>
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{BuildFrame, MeshNode, Nodes, PathName, Peers};

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.location()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-build-reattach");
    let (Some(path), Some(name)) = (std::env::args().nth(1), std::env::args().nth(2)) else { return Err(anyhow!("usage: build_reattach <path> <name>")) };
    let node = PathName::new(&path, &name).map_err(|e| anyhow!("path {path:?}, name {name:?}: {e}"))?;
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);

    // Submit, keep the Build id, lose the stream.
    let nodes = Nodes::new(carrier.clone());
    let node_id = nodes.get(&rdm_client::Filter::new().eq("path", [node.path.as_str()]).eq("name", [node.name.as_str()])).await?.into_iter().next().map(|n| n.node_id).ok_or_else(|| anyhow!("node.get names no node at {node:?}"))?;
    let accepted = nodes.delete(vec![node_id]).await?.accepted().cloned().ok_or_else(|| anyhow!("the Delete opened no Build"))?;
    println!("accepted: Build {} attempt {}; the stream is dropped", accepted.build_id.log(), accepted.attempt);

    // Reattach by id: the receipts so far, and the stream again.
    let again = carrier.reattach(&accepted.build_id).await?;
    let (ours, earlier): (Vec<_>, Vec<_>) = again.receipts.iter().partition(|r| r.attempt >= accepted.attempt);
    println!("reattached: {} receipt(s) of earlier attempts, {} of this one", earlier.len(), ours.len());
    for r in ours {
        println!("receipt: attempt {} {:?} {} {:?}", r.attempt, r.operation, r.step, r.result);
    }
    // Follow it to its end; a break on the way is reattached the same way.
    let followed = carrier.follow(again.stream).await?;
    for frame in &followed.frames {
        println!("frame: {frame:?}");
    }
    match followed.frames.last() {
        Some(BuildFrame::Complete { attempt }) => println!("Build {} complete at attempt {attempt}, reattached {} more time(s) on the way", accepted.build_id.log(), followed.reattached),
        other => return Err(anyhow!("Build {} ended {other:?}", accepted.build_id.log())),
    }
    Ok(())
}
