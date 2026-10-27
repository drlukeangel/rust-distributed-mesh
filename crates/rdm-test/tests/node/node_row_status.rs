//! product=mesh, feature=node-lifecycle, subfeature=node-row-status, rung=MN, provider=process.
//!
//! A node's status is the readiness on its durable row (node-identity-and-state.md §4: "its
//! **status** — the readiness on its durable row: starting, pending, ready-for-traffic, ..."), set by
//! the node's mesh-primary at each transition states.md "Who sets each state" names, and durable before
//! the reply (states.md "Setting a state"). The row is read where it is stored: the newest line for the
//! node's id in the nodes store on the fabric's store root.

use rdm_client::{Frame, NodeView};
use rdm_core::PathName;
use rdm_test::estate::{Estate, Owner};
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-row-status".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

/// Every frame of `stream`, in order, up to and including the terminal one, bounded by [`SETTLE`].
async fn frames(mut stream: rdm_client::WorkflowStream) -> Vec<Frame> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        let Ok(next) = tokio::time::timeout_at(deadline, stream.next()).await else {
            panic!("the reply stream sent no terminal within {SETTLE:?}: {out:?}");
        };
        let Some(f) = next else { break };
        let f = f.unwrap_or_else(|e| panic!("the reply stream broke: {e}"));
        let terminal = f.is_terminal();
        out.push(f);
        if terminal {
            break;
        }
    }
    out
}

/// The `status` of the newest record of `node_id` in the nodes store on `root`.
fn row_status(root: &Path, node_id: &str) -> String {
    let file = root.join(rdm_admin::store_root::NODES_FILE);
    let text = std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("the nodes store {} could not be read: {e}", file.display()));
    let row: Value = text.lines().rev().filter_map(|l| serde_json::from_str::<Value>(l).ok()).find(|l| l["key"] == node_id).unwrap_or_else(|| panic!("the nodes store holds no record of {node_id}"));
    row["record"]["status"].as_str().unwrap_or_else(|| panic!("the record of {node_id} carries no status: {row}")).to_string()
}

/// CONTRACT: every node of a ready-for-traffic fabric has `ready-for-traffic` on its durable row, as
/// stored. A stopped node's row carries its stopped status once `node.stop` completes, and a start puts
/// `ready-for-traffic` back on the row once `node.start` completes: the mesh-primary writes the status
/// on the row at each transition, before the transition's reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_row_carries_its_status_through_ready_stop_and_start() {
    let mut estate = Estate::bootstrap(owner("a_node_row_carries_its_status_through_ready_stop_and_start"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    let views: Vec<NodeView> = estate.settled_views(&[("mesh1", 1, 2)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let root = estate.fabric_store_root();

    // Ready-for-traffic: every node's row says so.
    let ready: Vec<(String, String)> = views.iter().map(|v| (v.name.to_string(), row_status(&root, v.node_id.as_str()))).collect();
    assert!(ready.iter().all(|(_, s)| s == "ready-for-traffic"), "every node of a ready-for-traffic fabric is ready-for-traffic on its row: {ready:?}");

    // The rpc node that is not its type's primary: a primary is stopped only after node.primary.update.
    let subject = views.iter().find(|v| v.name.kind == rdm_core::NodeKind::RpcNode && !v.is_primary).unwrap_or_else(|| panic!("mesh1 holds an rpc node that is not its type's primary: {views:?}"));
    let (path, node_id): (PathName, String) = (subject.name.clone(), subject.node_id.to_string());
    let nodes = estate.nodes_obj().await;

    // Stopped: states.md names the status `stopped`; the code's NodeStatus has no `Stopped` and calls a
    // stopped node `leaving` (rdm-core model.rs `NodeStatus::Leaving`, as node.get answers it).
    let stopped = frames(nodes.stop(&path).await.expect("the stop was accepted")).await;
    assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
    assert_eq!(row_status(&root, &node_id), "leaving", "{path}'s row carries its stopped status once node.stop completed");

    // Started again: the row says ready-for-traffic.
    let started = frames(nodes.start(&path).await.expect("the start was accepted")).await;
    assert_eq!(started.last(), Some(&Frame::Complete), "{started:?}");
    assert_eq!(row_status(&root, &node_id), "ready-for-traffic", "{path}'s row is ready-for-traffic once node.start completed");

    estate.stop().await;
}
