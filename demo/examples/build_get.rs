//! `build.get [id]`: one Build read whole, as the fabric-primary answers it (node-rpc-envelope.md
//! "Build, op `0x20`"): its header, its record's change and acceptance time, and each `rdm.build.step`
//! event of its sections (build.md §5): the mesh, the operation, the step and how it ended. A Create's
//! node.create steps name the node.id the fabric-primary minted for the node at acceptance.
//!
//! Build: `cargo build --example build_get` in `demo/`. Run against an estate
//! (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/build_get <build_id>
//! ```
use anyhow::{anyhow, Context, Result};
use rdm_client::{BuildId, MeshNode, Peers};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-build-get");
    let id = std::env::args().nth(1).ok_or_else(|| anyhow!("usage: build_get <build_id>"))?;
    let id = BuildId::parse(&id).map_err(|e| anyhow!("{id}: {e}"))?;
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    let peers = Peers::at(&birth).await.map_err(|e| anyhow!(e))?;
    let build = peers.carrier_at(birth.location()).build(&id).await?;
    let steps: Vec<_> = build
        .events
        .iter()
        .filter(|e| e["type"] == "rdm.build.step")
        .map(|e| json!({ "mesh": e["data"]["mesh"], "operation": e["data"]["op"], "step": e["data"]["step"], "state": e["data"]["state"] }))
        .collect();
    // How it ended: the state of its last `rdm.build.ended` (complete, incomplete or failed, Q346).
    let ended = build.events.iter().rev().find(|e| e["type"] == "rdm.build.ended").map(|e| e["data"]["state"].clone());
    let out = json!({
        "build_id": build.build_id.to_string(),
        "phase": format!("{:?}", build.phase),
        "current": build.current,
        "change": build.change,
        "time": build.time,
        "nodes": build.nodes.len(),
        "steps": steps,
        "ended": ended,
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}
