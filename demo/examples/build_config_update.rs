//! Two Builds over a node's stored config (build.md §1; Q236, Q306).
//!
//! A Build's config is the node's whole map, a PUT: whatever key the entry does not name is gone. So a
//! caller that changes some keys composes its Build from the rows as they stand: every current node's
//! full row (`node.get [["!status", ["dead", "deleted"]]]`, a negated pair, Q306), its config and tags,
//! with only the keys it means to change changed.
//!
//! Here the first Build gives one node `zone=a` and `tier=gold`; the second, composed from the rows the
//! first left, changes `zone` to `b` and keeps `tier`. Each Build's entry is the node's whole config,
//! and the row is read back after each: `{tier: gold, zone: a}`, then `{tier: gold, zone: b}`. A refusal
//! is printed by name.
//!
//! Build: `cargo build --example build_config_update` in `demo/`. Run against an estate
//! (`scripts/demo-estate.sh up`):
//!
//! ```text
//! RDM_NODE_ADMIN_BIRTH="$(cat <root>/birth.json)" target/debug/examples/build_config_update <path.name>
//! ```

use anyhow::{anyhow, Context, Result};
use rdm_client::{BuildCarrier, BuildFrame, BuildId, CallEnd, Change, Fabrics, Filter, MeshNode, Nodes, PathName, Peers};
use std::collections::BTreeMap;

async fn enter() -> Result<(Peers, PathName)> {
    let json = std::env::var("RDM_NODE_ADMIN_BIRTH").context("set RDM_NODE_ADMIN_BIRTH to the birth a node-admin printed")?;
    let birth: MeshNode = serde_json::from_str(&json).context("RDM_NODE_ADMIN_BIRTH is not a birth")?;
    Ok((Peers::at(&birth).await.map_err(|e| anyhow!(e))?, birth.name))
}

/// Every current node: a negated pair (Q306) leaves out the dead and the deleted.
fn current() -> Filter {
    Filter::new().not("status", ["dead", "deleted"])
}

/// One Build: the fabric's current topology composed from every current node's row, with `node`'s
/// config set by `change`, submitted and followed to its end.
async fn build(carrier: &BuildCarrier, node: &PathName, change: impl FnOnce(&mut BTreeMap<String, String>)) -> Result<()> {
    let fabric = Fabrics::new(carrier.clone()).get(&Filter::new()).await?.into_iter().next().ok_or_else(|| anyhow!("fabric.get answered no fabric"))?;
    let based_on = BuildId::parse(fabric.build_id.as_deref().ok_or_else(|| anyhow!("fabric.get names no current Build"))?).map_err(|e| anyhow!("the current Build id: {e}"))?;
    let rows = Nodes::new(carrier.clone()).get(&current()).await?;
    let mut nodes = carrier.build(&based_on).await?.nodes;
    for spec in &mut nodes {
        if let Some(row) = rows.iter().find(|r| r.name == spec.path) {
            spec.tags = row.tags.clone();
            spec.config = row.config.clone();
        }
    }
    let spec = nodes.iter_mut().find(|s| s.path == *node).ok_or_else(|| anyhow!("the current Build names no {node}"))?;
    change(&mut spec.config);
    let config = spec.config.clone();
    let stream = match carrier.create(Change::Reconcile { based_on, nodes, topology_storage_provider: None }).await {
        Ok(s) => s,
        Err(CallEnd::Rejected { reason, detail }) => return Err(anyhow!("refused {reason}: {detail}")),
        Err(e) => return Err(e.into()),
    };
    println!("Build {}: {node} config {config:?}", stream.build_id().log());
    match carrier.follow(stream).await?.frames.last() {
        Some(BuildFrame::Complete { .. }) => Ok(()),
        other => Err(anyhow!("the Build ended {other:?}")),
    }
}

/// `node`'s config as its row holds it.
async fn stored(carrier: &BuildCarrier, node: &PathName) -> Result<BTreeMap<String, String>> {
    let rows = Nodes::new(carrier.clone()).get(&current().eq("name", [node.to_string()])).await?;
    Ok(rows.into_iter().next().ok_or_else(|| anyhow!("node.get answered no current {node}"))?.config)
}

#[tokio::main]
async fn main() -> Result<()> {
    let _telemetry = rdm_transport::init_evidence_telemetry("example-build-config-update");
    let node = std::env::args().nth(1).ok_or_else(|| anyhow!("usage: build_config_update <path.name>"))?;
    let node: PathName = node.parse().map_err(|e| anyhow!("{node}: {e}"))?;
    let (peers, entry) = enter().await?;
    let carrier = peers.carrier_at(entry);

    build(&carrier, &node, |c| {
        c.insert("zone".into(), "a".into());
        c.insert("tier".into(), "gold".into());
    })
    .await?;
    println!("after the first Build: {node} stores {:?}", stored(&carrier, &node).await?);

    // The second Build changes one key; the rows it is composed from carry the rest, so `tier` stays.
    build(&carrier, &node, |c| {
        c.insert("zone".into(), "b".into());
    })
    .await?;
    let after = stored(&carrier, &node).await?;
    println!("after the second Build: {node} stores {after:?}");
    if after.get("zone").map(String::as_str) != Some("b") || after.get("tier").map(String::as_str) != Some("gold") {
        return Err(anyhow!("{node} stores {after:?}, not zone=b with tier=gold"));
    }
    Ok(())
}
