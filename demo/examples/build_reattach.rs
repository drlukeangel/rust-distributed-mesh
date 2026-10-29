//! Reattach to a Build after the caller lost its stream (node-rpc-envelope.md "Frames, durable receipts
//! and reattachment"; build.md: a live run is reattached, never claimed again).
//!
//! A Build call answers `Started { build_id, attempt }` and then streams its step frames to the
//! terminal. The frames are the executor's receipts, recorded before they are sent, so a caller that
//! loses the stream (its connection closed, its process restarted, the fabric-primary moved) loses
//! nothing: holding the Build id, it reads the receipts with `build.get [id]` and opens the stream
//! again by re-submitting that id, which never starts a second Build. `BuildCarrier::reattach` is that
//! pair; `BuildCarrier::follow` follows a stream to its terminal and reattaches on its own whenever the
//! stream breaks or ends `FabricPrimaryMoved`, keeping each completed step once.
//!
//! Here the call is one step, `node.connections.delete { All }` of a node (its connections are closed
//! and it keeps running on its mesh), run as the next attempt of the fabric's current Build. The caller keeps only its Build id from the first stream and drops
//! the stream, as a restarted caller would; then it reattaches by id and follows the Build to its end.
//!
//! Build: `cargo build --example build_reattach` in `demo/`. Run against a live estate:
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/build_reattach <path.name>
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{BuildFrame, MeshNode, Nodes, PathName, Peers};
use rdm_core::build::Step;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-build-reattach");
    let node = std::env::args().nth(1).ok_or_else(|| anyhow!("usage: build_reattach <path.name>"))?;
    let node: PathName = node.parse().map_err(|e| anyhow!("{node}: {e}"))?;
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);

    // Submit, keep the Build id, lose the stream.
    let accepted = Nodes::new(carrier.clone()).steps(vec![Step::DeleteConnections { node: node.clone() }]).await?.accepted().clone();
    println!("accepted: Build {} attempt {}; the stream is dropped", accepted.build_id.log(), accepted.attempt);

    // Reattach by id: the receipts so far, and the stream again. The receipts are every attempt's: a
    // call on a node runs as an attempt of the fabric's current Build, so its earlier attempts' are there too.
    let again = carrier.reattach(&accepted.build_id).await?;
    let (ours, earlier): (Vec<_>, Vec<_>) = again.receipts.iter().partition(|r| r.attempt >= accepted.attempt);
    println!("reattached: {} receipt(s) of earlier attempts, {} of this one", earlier.len(), ours.len());
    for r in ours {
        println!("receipt: attempt {} {} {} {:?}", r.attempt, r.operation, r.step, r.result);
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
