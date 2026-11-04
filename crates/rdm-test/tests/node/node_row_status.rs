//! product=mesh, feature=node-lifecycle, subfeature=node-row-status, rung=MN, provider=process.
//!
//! A node's `state` is on its node record (Q366), written by the node itself at each transition
//! states.md "Who sets each state" gives it (Q356), durable before it reports it; its mesh-primary writes
//! the record only where the node cannot: a deleted node's deleting and deleted. The record is read
//! where it is stored: its lines at the node's id in the nodes store on the fabric's store root, each
//! line's CloudEvents `source` naming its writer.

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

/// The `state` of the newest record of `node_id` in the nodes store on `root`.
fn row_status(root: &Path, node_id: &str) -> String {
    let file = root.join(rdm_admin::store_root::NODES_FILE);
    let text = std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("the nodes store {} could not be read: {e}", file.display()));
    let row: Value = text.lines().rev().filter_map(|l| serde_json::from_str::<Value>(l).ok()).find(|l| l["subject"] == node_id).unwrap_or_else(|| panic!("the nodes store holds no record of {node_id}"));
    row["data"]["state"].as_str().unwrap_or_else(|| panic!("the record of {node_id} carries no state: {row}")).to_string()
}

/// `states`, each as the record serializes it, with each run of one state taken once: a write that
/// leaves the state where it stood (a config, a role, a cert) is a line of the same state.
fn runs(states: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for s in states {
        if out.last() != Some(&s) {
            out.push(s);
        }
    }
    out
}

/// Whether `states` is `pattern` in order, each `(state, optional)` taken once or, when optional, skipped.
fn fits(states: &[String], pattern: &[(&str, bool)]) -> bool {
    match (states.split_first(), pattern.split_first()) {
        (None, None) => true,
        (_, Some(((_, true), rest))) if fits(states, rest) => true,
        (Some((s, more)), Some(((p, _), rest))) => s == p && fits(more, rest),
        _ => false,
    }
}

/// CONTRACT (states.md "Who sets each state"; Q356): every node of a ready fabric has `ready` on its
/// record, as stored. Through `node.connection.delete { All }`, `node.stop`, `node.start` and
/// `node.delete` of one node, its record says, once each call completes, `connections-deleted`,
/// `stopped`, `ready` and `deleted`. Every line of the record up to the delete is the node's own (its
/// `source` the node), carrying each state the node takes, in order: pending and ready at its create;
/// connections-deleting and connections-deleted; then node.stop's, stopping, connections-deleting and
/// connections-deleted (each written only where it moves the record), leaving, left, stopped; then the
/// start's starting (refused once its join's pending is written, node-start.md step 2, Q248), pending and
/// ready; then the stop node.delete composes. The delete's deleted is the mesh-primary's line, the last.
/// Must NOT happen: a line of the live node written by another node, or a state out of order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_record_carries_its_state_through_ready_stop_and_start() {
    let mut estate = Estate::bootstrap(owner("a_node_record_carries_its_state_through_ready_stop_and_start"), "fabric1", "mesh1").await;
    estate.resize_shape("fabric1", &[("mesh1", 1, 2)]).await;
    let views: Vec<NodeView> = estate.settled_views(&[("mesh1", 1, 2)]).await;
    estate.until_fabric_ready(SETTLE).await;
    let root = estate.fabric_store_root();

    // Ready: every node's record says so.
    let ready: Vec<(String, String)> = views.iter().map(|v| (v.node_id.to_string(), row_status(&root, v.node_id.as_str()))).collect();
    assert!(ready.iter().all(|(_, s)| s == "ready"), "every node of a ready fabric is ready on its record: {ready:?}");

    // The node that is not its type's primary: a primary is stopped only after node.primary.update.
    let subject = views.iter().find(|v| v.r#type == "node" && !v.is_primary).unwrap_or_else(|| panic!("mesh1 holds a node that is not its type's primary: {views:?}"));
    let (path, node_id): (PathName, String) = (subject.location(), subject.node_id.to_string());
    let nodes = estate.nodes_obj().await;

    for (call, then) in [("node.connection.delete", "connections-deleted"), ("node.stop", "stopped"), ("node.start", "ready"), ("node.delete", "deleted")] {
        let stream = match call {
            "node.connection.delete" => nodes.delete_connections(&path, &rdm_core::status::ConnectionScope::All).await,
            "node.stop" => nodes.stop(&path).await,
            "node.start" => nodes.start(&path).await,
            _ => nodes.delete(vec![subject.node_id.clone()]).await,
        };
        let frames = frames(stream.unwrap_or_else(|e| panic!("{call} of {path:?} was refused: {e}"))).await;
        assert_eq!(frames.last(), Some(&Frame::Complete), "{call}: {frames:?}");
        assert_eq!(row_status(&root, &node_id), then, "{path:?}'s record says {then} once {call} completed");
    }

    estate.stop().await;
    let lines = estate.record_lines(&subject.node_id);
    estate.artifact("record-lines.json", &serde_json::json!(lines));
    let state = |r: &rdm_node::nodes_storage::NodeRecord| serde_json::to_value(r.state).unwrap().as_str().unwrap_or_default().to_string();
    let (last_writer, last) = lines.last().unwrap_or_else(|| panic!("{path:?} has a record"));
    assert_ne!(last_writer, &node_id, "the deleted node's deleted is its mesh-primary's line: {lines:?}");
    assert_eq!(state(last), "deleted", "the delete's deleted is the record's last line: {lines:?}");
    // Up to the delete, every line is the node's own.
    let deleting = lines.iter().position(|(w, r)| w != &node_id || state(r) == "deleting").unwrap_or(lines.len());
    let own: Vec<String> = runs(lines[..deleting].iter().map(|(_, r)| state(r)));
    assert!(lines[..deleting].iter().all(|(w, _)| w == &node_id), "every line of the live node is its own: {lines:?}");
    // `?` marks a state written only where it moves the record; the delete's own stop is cut short where
    // the mesh-primary's deleting is taken.
    let order = "starting? pending ready connections-deleting connections-deleted stopping connections-deleting? connections-deleted? leaving left stopped starting? pending ready stopping? connections-deleting? connections-deleted? leaving? left? stopped?";
    let pattern: Vec<(&str, bool)> = order.split(' ').map(|t| t.strip_suffix('?').map_or((t, false), |t| (t, true))).collect();
    assert!(fits(&own, &pattern), "{path:?} wrote each state it took on its own record, in states.md's order: {own:?}");
}
