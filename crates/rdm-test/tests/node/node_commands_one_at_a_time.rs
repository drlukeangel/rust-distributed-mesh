//! product=mesh, feature=node-lifecycle, subfeature=node-commands, rung=MN, provider=process.
//!
//! One call per command at a node (Q413; node-recovery.md step 1: "Another step on the same node runs
//! before or after it at the mesh-primary, never beside it: a caller's command naming a node its
//! primary already runs a command or step on is refused `Busy` at once, and the caller's `Send` sends
//! it again"). The primary above a node runs one command, repair or Build step on it at a time; a
//! second caller's command naming it is never a live call waiting beside the first.

use rdm_client::Frame;
use rdm_test::estate::{at_location, named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);
/// How long the cell waits for the second command's first answer while the first command holds the
/// node: the primary answers `Busy` at once, so this bound is never what ends a green run. It stays
/// under the start's own `WaitForNodeReady` bound (30 s), so a run with no `Busy` still lets the start
/// pass and reads what the primary did.
const ANSWERED: Duration = Duration::from_secs(10);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "node-lifecycle".into(), subfeature: "node-commands".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn attr(sp: &Value, k: &str) -> String {
    match &sp["attributes"][k] {
        Value::String(v) => v.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn end_ns(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}

/// Every frame of `stream`, in order, up to and including the terminal one, bounded at [`SETTLE`].
async fn frames(mut stream: rdm_client::WorkflowStream) -> (Vec<Frame>, Option<rdm_core::node_command::NodeCommandReply>) {
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
    (out, stream.completion().cloned())
}

/// CONTRACT (Q413; node-recovery.md step 1; node-connections-delete.md "Wire": "`Busy { reason }`
/// while the primary runs another command or step on the node, which the caller's `Send` sends
/// again"): mesh1's primary runs a `node.start` of `mesh1.node/1`, held at the node's
/// `pre_node_ready` (its knob). A caller's `node.connection.delete { Connections([an id never
/// opened]) }` of the same node is refused `Busy` at once: the caller's stream receives `busy` while
/// the start still holds the node. Once the start is let pass and ends `Complete`, the caller's `Send`
/// sends the delete again and the primary runs it after the start, completing it naming the id not
/// open. At the primary the start's `via-node-command`, then the delete's `busy` inside it, then the
/// delete's `connections-deleted` after it ended. Must NOT happen: a second live `via-node-command`
/// handler for the node beside the first (the delete waiting at the primary inside its own call), or
/// the delete refused after the start ended.
#[test]
fn a_second_command_naming_a_node_its_primary_runs_a_start_on_is_refused_busy_and_runs_after_it() {
    let test = "a_second_command_naming_a_node_its_primary_runs_a_start_on_is_refused_busy_and_runs_after_it";
    let cap = crate::common::capture(test);
    let client_spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
        estate.resize_shape("fabric1", &[("mesh1", 1, 2)]).await;
        estate.settled_views(&[("mesh1", 1, 2)]).await;
        estate.until_fabric_ready(SETTLE).await;
        // The node that holds no primary role: its stop moves none, so the start is the only command it runs.
        let mesh = estate.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", ["mesh1"])).await.expect("node.get [mesh]");
        let primary = mesh.iter().find(|n| n.r#type == "node-admin" && n.is_primary).map(|n| n.location()).expect("mesh1's primary");
        let subject = mesh.iter().find(|n| n.r#type == "node" && !n.is_primary).cloned().expect("a node of mesh1 holding no primary role");
        let (node, node_id) = (subject.location(), subject.node_id.to_string());

        // The first command: a start of the stopped node, held at its pre_node_ready until the cell lets it pass.
        let nodes = estate.nodes_obj().await;
        let (stopped, _) = frames(nodes.stop(&node).await.expect("the stop is accepted")).await;
        assert_eq!(stopped.last(), Some(&Frame::Complete), "{stopped:?}");
        estate.until_stopped_at_the_fabric_primary(&node).await;
        rdm_test::write_pre_node_ready_knob(&estate.root, &node, "held-for-a-second-command").unwrap();
        let starting = nodes.start(&node).await.expect("the start is accepted");

        // The second command, while the start holds the node.
        let never = rdm_core::ConnectionId::mint();
        let second = {
            let (nodes, node, scope) = (estate.nodes_obj().await, node.clone(), rdm_core::status::ConnectionScope::Connections(vec![never.clone()]));
            // A caller that must go after the start: its Send spent `busy` while the start still holds
            // the node (its pre_node_ready can take longer than the Send's delays), it sends again (Q413).
            tokio::spawn(async move {
                let until = tokio::time::Instant::now() + SETTLE;
                loop {
                    match nodes.delete_connections(&node, &scope).await {
                        Ok(stream) => break frames(stream).await,
                        Err(rdm_client::CallEnd::Rejected { reason, .. }) if reason == "busy" && tokio::time::Instant::now() < until => {}
                        Err(e) => panic!("node.connection.delete of {node:?} was refused: {e}"),
                    }
                }
            })
        };
        let busy = tokio::time::timeout(ANSWERED, client_spans.recorded("rdm.node_rpc.stream.update.via-received-frame", |f| f.get("detail").is_some_and(|d| d == "busy"))).await.is_ok();

        // The first command ends; the second runs after it.
        rdm_test::write_pre_node_ready_knob(&estate.root, &node, "pass").unwrap();
        let (started, _) = frames(starting).await;
        assert_eq!(started.last(), Some(&Frame::Complete), "the start completes once let pass: {started:?}");
        let (deleted, completion) = tokio::time::timeout(SETTLE, second).await.expect("the second command ends").expect("the second command's task");
        assert_eq!(deleted.last(), Some(&Frame::Complete), "{deleted:?}");
        assert!(
            matches!(&completion, Some(rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { closed, not_open, receipt: None, .. }) if closed.is_empty() && not_open == &vec![never.clone()]),
            "the delete ran after the start, naming the id the node never opened: {completion:?}"
        );
        estate.stop().await;

        // At the primary: one live handler for the node at a time.
        let spans = estate.spans();
        let commands: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-command").into_iter().filter(|sp| attr(sp, "subject_id") == node_id && at_location(sp, &primary)).collect();
        let one = |command: &str, outcome: &str| -> Vec<&Value> { commands.iter().copied().filter(|sp| attr(sp, "command") == command && attr(sp, "outcome") == outcome).collect() };
        let start = *one("start-node", "node-started").first().unwrap_or_else(|| panic!("the primary ran the start: {commands:#?}"));
        let ran = *one("delete-connections", "connections-deleted").first().unwrap_or_else(|| panic!("the primary ran the delete: {commands:#?}"));
        let beside: Vec<&&Value> = commands.iter().filter(|sp| attr(sp, "outcome") != "busy" && sp["span_id"] != start["span_id"] && start_ns(sp) < end_ns(start) && start_ns(start) < end_ns(sp)).collect();
        assert!(beside.is_empty(), "no second live handler for the node beside the start at its primary: start {start:#?}, beside it {beside:#?}");
        assert!(busy, "the caller's stream received `busy` while the primary ran the start: no Busy within {ANSWERED:?}; the primary's handlers for the node: {commands:#?}");
        let refused = one("delete-connections", "busy");
        assert!(!refused.is_empty() && refused.iter().all(|sp| start_ns(sp) >= start_ns(start) && end_ns(sp) <= end_ns(start)), "the primary refused the delete Busy while the start held the node: {refused:#?} inside {start:#?}");
        assert!(start_ns(ran) >= end_ns(start), "the delete ran after the start ended, never beside it: {ran:#?} after {start:#?}");
        let trace = ran["trace_id"].as_str().unwrap_or("").to_string();
        println!("the second command refused Busy, then run after the start: http://192.168.68.99:16687/trace/{trace}");
        estate.record_trace_url(&trace);
    });
}
