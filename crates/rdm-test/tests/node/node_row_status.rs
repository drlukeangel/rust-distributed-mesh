//! product=mesh, feature=node-lifecycle, subfeature=node-row-status, rung=MN, provider=process.
//!
//! A node's status is the readiness on its durable row (node-identity-and-state.md §4: "its
//! **status** — the readiness on its durable row: starting, pending, ready, ..."), set by
//! the node's mesh-primary at each transition states.md "Who sets each state" names, and durable before
//! the reply (states.md "Setting a state"). The row is read where it is stored: the newest line for the
//! node's id in the nodes store on the fabric's store root.

use rdm_client::{Frame, NodeView};
use rdm_core::PathName;
use rdm_test::estate::{named, Estate, Owner};
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

/// CONTRACT: every node of a ready fabric has `ready` on its durable row, as
/// stored. Through `node.connections.delete { All }`, `node.stop`, `node.start` and `node.delete` of one
/// rpc node, its row says, once each call completes, `connections-deleted`, `stopped`,
/// `ready` and `deleted`; and its mesh-primary wrote on its row each status "Who sets each
/// state" names, from the node's frame or its own receipt (one
/// `rdm.node_admin.node.update.via-row-status` span each, `written`; states.md and the paragraph after
/// its table, Q233): pending and ready at its create; connections-deleting and
/// connections-deleted; then node.stop's frames, its acknowledgement setting stopping over the
/// connections-deleted row: stopping, connections-deleting, connections-deleted, leaving, left, stopped;
/// then starting from the start's acknowledgement, pending and ready; then deleting from
/// node.delete's claim and the frames of the stop it composes, stopping through stopped (delete is the
/// mesh-primary's workflow). The node's Stopped declaration may reach the mesh-primary before a stop
/// stream's last frames, whichever first is applied (node-stop.md step 5): each frame after it leaves
/// the row stopped (`node_stop::stop_row_writes`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_row_carries_its_status_through_ready_stop_and_start() {
    let mut estate = Estate::bootstrap(owner("a_node_row_carries_its_status_through_ready_stop_and_start"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    let views: Vec<NodeView> = estate.settled_views(&[("mesh1", 1, 2)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let root = estate.fabric_store_root();

    // Ready: every node's row says so.
    let ready: Vec<(String, String)> = views.iter().map(|v| (v.name.to_string(), row_status(&root, v.node_id.as_str()))).collect();
    assert!(ready.iter().all(|(_, s)| s == "ready"), "every node of a ready fabric is ready on its row: {ready:?}");

    // The rpc node that is not its type's primary: a primary is stopped only after node.primary.update.
    let subject = views.iter().find(|v| v.name.kind == rdm_core::NodeKind::RpcNode && !v.is_primary).unwrap_or_else(|| panic!("mesh1 holds an rpc node that is not its type's primary: {views:?}"));
    let (path, node_id): (PathName, String) = (subject.name.clone(), subject.node_id.to_string());
    let nodes = estate.nodes_obj().await;

    for (call, then) in [("node.connections.delete", "connections-deleted"), ("node.stop", "stopped"), ("node.start", "ready"), ("node.delete", "deleted")] {
        let stream = match call {
            "node.connections.delete" => nodes.delete_connections(&path).await,
            "node.stop" => nodes.stop(&path).await,
            "node.start" => nodes.start(&path).await,
            _ => nodes.delete(&path).await,
        };
        let frames = frames(stream.unwrap_or_else(|e| panic!("{call} of {path} was refused: {e}"))).await;
        assert_eq!(frames.last(), Some(&Frame::Complete), "{call}: {frames:?}");
        assert_eq!(row_status(&root, &node_id), then, "{path}'s row says {then} once {call} completed");
    }

    estate.stop().await;
    let spans = estate.spans();
    // Every status write on the node's row, written or unchanged, in the order its mesh-primary made them.
    let mut rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-row-status").into_iter().filter(|sp| sp["attributes"]["node_id"] == node_id.as_str()).collect();
    rows.sort_by_key(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0));
    let at = |sp: &Value| (sp["attributes"]["status"].as_str().unwrap_or_default().to_string(), sp["attributes"]["outcome"].as_str().unwrap_or_default().to_string());
    let all: Vec<(String, String)> = rows.iter().map(|sp| at(sp)).collect();
    let written = |statuses: &[&str]| statuses.iter().map(|s| (s.to_string(), "written".to_string())).collect::<Vec<_>>();
    let (created, between) = (written(&["Pending", "Ready", "ConnectionsDeleting", "ConnectionsDeleted"]), written(&["Starting", "Pending", "Ready", "Deleting"]));
    let stops = crate::node_stop::stop_row_writes();
    assert!(
        stops.iter().any(|stop| stops.iter().any(|deleted| created.iter().chain(stop).chain(&between).chain(deleted).eq(all.iter()))),
        "{path}'s mesh-primary wrote each status from the node's frame or its own receipt, in states.md's order, each stop's frames moving the row until the node's stopped is taken: {all:?}"
    );
}
