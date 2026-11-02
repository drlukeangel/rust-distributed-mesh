//! The replica (Q296): rafka's produce and fetch, as the demo's own node types on RDM's Node RPC.
//!
//! - The **broker** owns a records log in its data dir and serves [`Records`] (op `0x52`): `Produce`
//!   appends a record to a topic at the next offset, idempotent by the caller's operation id (RDM's
//!   recovery helpers, `rdm_node::operation`, as the consumer example); `Fetch` reads a topic from an
//!   offset. Only its mesh's broker primary, the node-type primary RDM elects, takes a `Produce`; any
//!   other broker answers `NotPrimary` naming the primary it holds. A `Fetch` reads the answering
//!   broker's own log.
//! - The **gateway** serves the same op to its callers and forwards each request to its mesh's broker
//!   primary, found from RDM's topology (`TopologyView::primaries`). A `NotPrimary` or a broker not
//!   ready yet is asked again of the primary the gateway then holds, for up to [`FORWARD_FOR`]: when
//!   RDM elects the next broker primary, the gateway follows it. RDM resends each call itself
//!   ([`Resend`]); a call whose attempts are spent is told to the gateway's `on_call_failed`
//!   ([`PassOver`]), which passes that broker birth over, and the gateway asks the next replica: the
//!   next ready broker of its mesh. A `Produce` whose answer was lost is asked again under the same
//!   operation id, never as a new record.
//!
//! What the application does is a log line (target `rshape`) under RDM's own call and serve spans, as
//! in [`crate::consumer_example`].

use rdm_core::catalog::{LedgerEntry, OpOwner, OpState};
use rdm_core::outcome::{Code, MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use rdm_core::filter::Filter;
use rdm_core::{IncarnationId, NodeId, NodeKind, PathName};
use rdm_node::node_hooks::{CallFailedCtx, OnCallFailed};
use rdm_node::operation::{Decision, OperationStore, Operations, Recorded};
use rdm_transport::topology::TopologyView;
use rdm_transport::{Resend, CallOptions, HandlerFault, NodeRpcClient, NodeTarget, ServerBuilder};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// The product that owns the op.
pub const OWNER: &str = crate::consumer_example::OWNER;
/// How long a gateway keeps asking for its mesh's broker primary before it answers `NoBroker`.
pub const FORWARD_FOR: Duration = Duration::from_secs(30);
/// How long a gateway waits between two asks of one forward.
const ASK_EVERY: Duration = Duration::from_millis(250);

/// A request of [`Records`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordsRequest {
    /// Append `record` to `topic`. A second `Produce` of one `operation` with the same topic and record
    /// answers the offset it was given; with another, `Conflict`.
    Produce { operation: String, topic: String, record: String },
    /// `topic`'s records from offset `from`, at most `max` of them. Read-only.
    Fetch { topic: String, from: u64, max: u32 },
}

/// A reply of [`Records`]: its results, its named refusals, then the six reserved variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordsReply {
    /// The record is durable in the broker primary's log at `offset`.
    Produced { operation: String, topic: String, offset: u64 },
    /// `topic`'s records read, each with its offset; the next read starts at `next`.
    Fetched { topic: String, records: Vec<(u64, String)>, next: u64 },
    /// This broker is not its mesh's broker primary: `primary` is the node id of the one it holds
    /// (empty when it holds none).
    NotPrimary { primary: String },
    /// The operation id is recorded with another topic or record.
    Conflict { operation: String },
    /// The gateway found no broker primary that answered within [`FORWARD_FOR`].
    NoBroker { reason: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

/// The op the broker and the gateway serve.
pub struct Records;

impl NodeProtocol for Records {
    const OP: u8 = 0x52;
    const NAME: &'static str = "replica-records";
    const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 160 * 1024;
    type Request = RecordsRequest;
    type Reply = RecordsReply;

    rdm_core::protocol_variants! {
        request RecordsRequest { Produce, Fetch }
        reply RecordsReply {
            Produced = "produced" => ReplyKind::Success,
            Fetched = "fetched" => ReplyKind::Success,
            NotPrimary = "not-primary" => ReplyKind::new(Code::FailedPrecondition),
            Conflict = "conflict" => ReplyKind::new(Code::AlreadyExists),
            NoBroker = "no-broker" => ReplyKind::new(Code::Unavailable),
            @reserved,
        }
    }

    fn read_only(req: &RecordsRequest) -> bool {
        matches!(req, RecordsRequest::Fetch { .. })
    }
}

/// The ledger row reserving the op for [`OWNER`].
pub fn ledger_rows() -> Vec<LedgerEntry> {
    vec![LedgerEntry { op: Records::OP, family: Records::NAME.to_string(), owner: OpOwner::Product(OWNER.to_string()), state: OpState::Live }]
}

// ---------------------------------------------------------------------------------------------------
// The broker's records log.
// ---------------------------------------------------------------------------------------------------

/// A `Produce`'s input: what an operation id is compared on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProduceInput {
    pub topic: String,
    pub record: String,
}

/// One line of the log: a record accepted at its offset, or its operation completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Line {
    Accepted { operation: String, topic: String, record: String, offset: u64 },
    Completed { operation: String, offset: u64 },
}

/// The log's file, under the broker's data dir.
const LOG_FILE: &str = "records.jsonl";

struct Held {
    file: std::fs::File,
    topics: BTreeMap<String, Vec<String>>,
    operations: BTreeMap<String, Recorded<ProduceInput, u64>>,
    offsets: BTreeMap<String, u64>,
}

/// The broker's durable records log: a JSON-lines file folded in memory, the [`OperationStore`] RDM's
/// recovery helpers decide over. A last line cut short by a crash was never synced, so never
/// acknowledged: it is cut from the file.
#[derive(Clone)]
pub struct RecordsLog {
    held: Arc<Mutex<Held>>,
}

impl RecordsLog {
    /// Open the log under `dir` (created when absent) and fold it.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(LOG_FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(e),
        };
        let mut held = Held { file: std::fs::OpenOptions::new().create(true).append(true).open(&path)?, topics: BTreeMap::new(), operations: BTreeMap::new(), offsets: BTreeMap::new() };
        let mut whole = 0usize;
        for (n, line) in text.split_inclusive('\n').enumerate() {
            if !line.ends_with('\n') {
                break;
            }
            let parsed: Line = serde_json::from_str(line.trim_end()).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{} line {}: {e}", path.display(), n + 1)))?;
            fold(&mut held, &parsed);
            whole += line.len();
        }
        if whole < text.len() {
            held.file.set_len(whole as u64)?;
            held.file.sync_all()?;
        }
        Ok(Self { held: Arc::new(Mutex::new(held)) })
    }

    /// `topic`'s records from `from`, at most `max`, and where the next read starts.
    pub fn read(&self, topic: &str, from: u64, max: u32) -> (Vec<(u64, String)>, u64) {
        let held = self.held.lock().unwrap();
        let Some(records) = held.topics.get(topic) else { return (Vec::new(), from) };
        let end = records.len() as u64;
        let to = end.min(from.saturating_add(u64::from(max)));
        let read: Vec<(u64, String)> = (from.min(end)..to).map(|i| (i, records[i as usize].clone())).collect();
        (read, to.max(from.min(end)))
    }

    /// The offset `operation`'s record was accepted at.
    pub fn offset_of(&self, operation: &str) -> Option<u64> {
        self.held.lock().unwrap().offsets.get(operation).copied()
    }

    fn append(&self, line: Line) -> std::io::Result<()> {
        let mut held = self.held.lock().unwrap();
        let mut text = serde_json::to_string(&line).map_err(std::io::Error::other)?;
        text.push('\n');
        held.file.write_all(text.as_bytes())?;
        held.file.sync_data()?;
        fold(&mut held, &line);
        Ok(())
    }
}

fn fold(held: &mut Held, line: &Line) {
    match line {
        Line::Accepted { operation, topic, record, offset } => {
            held.topics.entry(topic.clone()).or_default().push(record.clone());
            held.offsets.insert(operation.clone(), *offset);
            held.operations.entry(operation.clone()).or_insert(Recorded { input: ProduceInput { topic: topic.clone(), record: record.clone() }, output: None });
        }
        Line::Completed { operation, offset } => {
            if let Some(op) = held.operations.get_mut(operation) {
                op.output = Some(*offset);
            }
        }
    }
}

#[async_trait::async_trait]
impl OperationStore for RecordsLog {
    type Input = ProduceInput;
    type Output = u64;
    type Error = std::io::Error;

    async fn get(&self, operation: &str) -> std::io::Result<Option<Recorded<ProduceInput, u64>>> {
        Ok(self.held.lock().unwrap().operations.get(operation).cloned())
    }

    async fn put_accepted(&self, operation: &str, input: &ProduceInput) -> std::io::Result<()> {
        let log = self.clone();
        let (operation, input) = (operation.to_string(), input.clone());
        tokio::task::spawn_blocking(move || {
            let offset = log.held.lock().unwrap().topics.get(&input.topic).map_or(0, |r| r.len() as u64);
            log.append(Line::Accepted { operation, topic: input.topic, record: input.record, offset })
        })
        .await
        .map_err(|e| std::io::Error::other(format!("the log's write task ended: {e}")))?
    }

    async fn put_completed(&self, operation: &str, output: &u64) -> std::io::Result<()> {
        let log = self.clone();
        let (operation, offset) = (operation.to_string(), *output);
        tokio::task::spawn_blocking(move || log.append(Line::Completed { operation, offset })).await.map_err(|e| std::io::Error::other(format!("the log's write task ended: {e}")))?
    }

    async fn pending(&self) -> std::io::Result<Vec<(String, ProduceInput)>> {
        let held = self.held.lock().unwrap();
        Ok(held.operations.iter().filter(|(_, op)| op.output.is_none()).map(|(id, op)| (id.clone(), op.input.clone())).collect())
    }
}

/// The broker's service: RDM's recovery helpers over its log.
pub struct Broker {
    operations: Operations<RecordsLog>,
}

impl Broker {
    /// Open the log under `dir` and complete every operation it holds accepted and not completed: an
    /// accepted record has its offset, so its completion is only recorded.
    pub async fn open(dir: &Path) -> std::io::Result<Arc<Self>> {
        let operations = Operations::new(RecordsLog::open(dir)?);
        for (operation, _) in operations.pending().await? {
            if let Some(offset) = operations.store().offset_of(&operation) {
                operations.complete(&operation, &offset).await?;
            }
        }
        Ok(Arc::new(Self { operations }))
    }
}

// ---------------------------------------------------------------------------------------------------
// Registration: the broker answers, the gateway forwards.
// ---------------------------------------------------------------------------------------------------

/// What a serving node knows once it is up: its topology view (set by the role's run once the node
/// started) and its own birth. A request before then answers `NotReady`.
#[derive(Clone)]
pub struct Node {
    /// The node's topology view.
    pub view: Arc<OnceLock<TopologyView>>,
    /// The node's own node id.
    pub me: NodeId,
    /// The node's own `(path, name)`.
    pub name: PathName,
    /// The node's own mesh, as its launch names it (never read from the `(path, name)`).
    pub mesh: String,
    /// The broker births the gateway's `on_call_failed` passed over (a gateway's; empty on a broker).
    pub passed: PassedOver,
}

impl Node {
    /// The node id of its mesh's broker primary, as its topology holds it now.
    fn broker_primary(&self) -> Option<NodeId> {
        self.view.get()?.primaries().node_type_primary(&self.mesh, NodeKind::Broker).map(|p| p.node_id)
    }

    /// The broker a forward asks now ([`choose`]): the broker primary its topology holds, or, once
    /// `on_call_failed` passed that birth over, the next ready broker of its mesh.
    fn broker_to_ask(&self) -> Option<NodeId> {
        let view = self.view.get()?;
        let ready: Vec<(NodeId, IncarnationId)> = view
            .nodes_get(&Filter::new().eq("mesh", [self.mesh.clone()]).eq("kind", [NodeKind::Broker.segment()]).eq("status", ["ready"]))
            .unwrap_or_default()
            .into_iter()
            .map(|d| (d.node.node_id, d.node.incarnation))
            .collect();
        let primary = self.broker_primary();
        let mut chosen = None;
        self.passed.0.send_modify(|passed| chosen = choose(primary, &ready, passed));
        chosen
    }
}

/// The broker births a gateway passed over, each with its incarnation: written by its
/// `on_call_failed` ([`PassOver`]), read when a forward chooses the broker to ask ([`choose`]).
#[derive(Clone)]
pub struct PassedOver(Arc<tokio::sync::watch::Sender<BTreeMap<NodeId, IncarnationId>>>);

impl Default for PassedOver {
    fn default() -> Self {
        Self(Arc::new(tokio::sync::watch::channel(BTreeMap::new()).0))
    }
}

impl PassedOver {
    /// Wait until `broker` is passed over, at most `limit`: a spent call's notice runs on a task of
    /// its own, after the call has ended to the forward.
    async fn until_passed(&self, broker: &NodeId, limit: Duration) {
        let mut passed = self.0.subscribe();
        let _ = tokio::time::timeout(limit, passed.wait_for(|p| p.contains_key(broker))).await;
    }

    /// A broker answered that it holds `primary` as its broker primary: that birth is asked again.
    fn ask_again(&self, primary: &str) {
        self.0.send_if_modified(|passed| {
            let before = passed.len();
            passed.retain(|id, _| id.to_string() != primary);
            passed.len() != before
        });
    }
}

/// The broker a forward asks, given the broker primary its topology holds, its mesh's ready brokers
/// (node id and incarnation, in the topology's order) and the births passed over: the primary, unless
/// it is passed over; then the first ready broker that is not, the next replica. No primary held:
/// none, the forward waits for RDM's election. A birth the topology no longer names ready is passed
/// over no more ([`keep_ready`]).
pub fn choose(primary: Option<NodeId>, ready: &[(NodeId, IncarnationId)], passed: &mut BTreeMap<NodeId, IncarnationId>) -> Option<NodeId> {
    keep_ready(passed, ready);
    let primary = primary?;
    if !passed.contains_key(&primary) {
        return Some(primary);
    }
    ready.iter().map(|(id, _)| id).find(|id| !passed.contains_key(*id)).cloned()
}

/// Keep in `passed` only the births `ready` still names (node id and incarnation): a birth stopped,
/// re-created as a new node id, or started as a new incarnation is passed over no more.
pub fn keep_ready(passed: &mut BTreeMap<NodeId, IncarnationId>, ready: &[(NodeId, IncarnationId)]) {
    passed.retain(|id, incarnation| ready.iter().any(|(r, i)| r == id && i == incarnation));
}

/// What one gateway's answer to a forwarded `Produce` is to the node that asked it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Asked {
    /// The record is produced at `offset`.
    Produced { offset: u64 },
    /// The call ended unanswered, `NotSent` or `Indeterminate`: RDM resent it until its attempts were
    /// spent (node-rpc.md §43). That gateway birth is never asked it again.
    Spent(String),
    /// Any other answer (not ready, busy, connections-deleting, no broker): the gateway keeps its turn.
    Answered(String),
}

impl Asked {
    /// What `outcome`, a gateway's answer to a `Produce`, is to its asker.
    pub fn of(outcome: RpcOutcome<RecordsReply>) -> Self {
        match outcome {
            RpcOutcome::Reply(reply) => match reply.into_value() {
                RecordsReply::Produced { offset, .. } => Self::Produced { offset },
                other => Self::Answered(format!("{other:?}")),
            },
            spent @ (RpcOutcome::NotSent(_) | RpcOutcome::Indeterminate(_)) => Self::Spent(format!("{} {}", spent.name(), spent.detail_name())),
            other => Self::Answered(format!("{} {}", other.name(), other.detail_name())),
        }
    }
}

/// One round of a `Produce` through the gateways: each gateway `ready` names (node id and
/// incarnation, in the topology's order) that is not in `spent` is asked once, in order, by `ask`,
/// until one produces it. A gateway whose call is spent goes into `spent` and is not asked again
/// while the topology names that birth ready ([`keep_ready`]); one that answers keeps its turn for
/// the next round. `Err` names each gateway's answer when none produced it.
pub async fn ask_each_gateway<F, Fut>(ready: &[(NodeId, IncarnationId)], spent: &mut BTreeMap<NodeId, IncarnationId>, mut ask: F) -> Result<u64, Vec<String>>
where
    F: FnMut(NodeId) -> Fut,
    Fut: std::future::Future<Output = Asked>,
{
    keep_ready(spent, ready);
    let mut answers = Vec::new();
    let unspent: Vec<_> = ready.iter().filter(|(id, _)| !spent.contains_key(id)).cloned().collect();
    for (gateway, incarnation) in unspent {
        match ask(gateway.clone()).await {
            Asked::Produced { offset } => return Ok(offset),
            Asked::Spent(why) => {
                spent.insert(gateway.clone(), incarnation);
                answers.push(format!("{gateway}: {why}, its attempts spent"));
            }
            Asked::Answered(why) => answers.push(format!("{gateway}: {why}")),
        }
    }
    if answers.is_empty() {
        answers.push(if ready.is_empty() { "no gateway of the mesh is ready".into() } else { "every ready gateway's call is spent".into() });
    }
    Err(answers)
}

/// The gateway's `on_call_failed` (fabric-node-hooks.md "on_call_failed"): a call of this node whose
/// attempts are spent passes its receiver's birth over, so the gateway's forwards ask the next
/// replica. A receiver that is not a ready broker of its mesh is dropped at the next choice.
pub struct PassOver(pub PassedOver);

#[async_trait::async_trait]
impl OnCallFailed for PassOver {
    async fn on_call_failed(&self, ctx: &CallFailedCtx) {
        let Some(receiver) = &ctx.receiver else { return };
        tracing::info!(target: "rshape", receiver_path = %receiver.name.path, receiver_name = %receiver.name.name, receiver_node_id = %receiver.node_id, outcome = ctx.reason.name(), "on_call_failed: a call's attempts are spent; its receiver is passed over and the next replica is asked");
        self.0 .0.send_modify(|passed| {
            passed.insert(receiver.node_id.clone(), receiver.incarnation.clone());
        });
    }
}

/// The broker's `serve_app`: [`Records`] over its log.
pub fn register_broker(b: ServerBuilder, broker: Arc<Broker>, node: Node) -> ServerBuilder {
    b.ledger(ledger_rows()).serve::<Records, _, _>(OpOwner::Product(OWNER.to_string()), move |peer, request| {
        let (broker, node) = (broker.clone(), node.clone());
        async move {
            if node.view.get().is_none() {
                return Ok(Records::not_ready(format!("path {:?}, name {:?} is starting", node.name.path, node.name.name)));
            }
            let faulted = |e: std::io::Error| HandlerFault::invariant_broken(format!("the records log: {e}"));
            match request {
                RecordsRequest::Fetch { topic, from, max } => {
                    let (records, next) = broker.operations.store().read(&topic, from, max);
                    Ok(RecordsReply::Fetched { topic, records, next })
                }
                RecordsRequest::Produce { operation, topic, record } => {
                    match node.broker_primary() {
                        Some(primary) if primary == node.me => {}
                        held => return Ok(RecordsReply::NotPrimary { primary: held.map(|p| p.to_string()).unwrap_or_default() }),
                    }
                    let input = ProduceInput { topic: topic.clone(), record };
                    // Recorded and synced before it is accepted: the runtime acks the request only
                    // once the record is durable. A repeat answers the offset it was given.
                    let decision = broker.operations.decide(&peer.acceptance, &operation, &input).await.map_err(faulted)?;
                    let offset = match decision {
                        Decision::Conflict => return Ok(RecordsReply::Conflict { operation }),
                        Decision::Completed(offset) => offset,
                        Decision::Recorded | Decision::InFlight => {
                            let offset = broker.operations.store().offset_of(&operation).ok_or_else(|| HandlerFault::invariant_broken(format!("{operation} is recorded with no offset")))?;
                            broker.operations.complete(&operation, &offset).await.map_err(faulted)?;
                            offset
                        }
                    };
                    tracing::info!(target: "rshape", path = %node.name.path, name = %node.name.name, %operation, %topic, offset, "a record produced at the broker primary");
                    Ok(RecordsReply::Produced { operation, topic, offset })
                }
            }
        }
    })
}

/// The gateway's `serve_app`: [`Records`], each request forwarded to its mesh's broker primary.
pub fn register_gateway(b: ServerBuilder, client: Arc<NodeRpcClient>, node: Node) -> ServerBuilder {
    b.ledger(ledger_rows()).serve::<Records, _, _>(OpOwner::Product(OWNER.to_string()), move |_peer, request| {
        let (client, node) = (client.clone(), node.clone());
        async move {
            if node.view.get().is_none() {
                return Ok(Records::not_ready(format!("path {:?}, name {:?} is starting", node.name.path, node.name.name)));
            }
            Ok(forward(&client, &node, &request).await)
        }
    })
}

/// `request` at the broker `node` asks now ([`Node::broker_to_ask`]), asked again on a domain answer
/// (`NotPrimary`, not ready) until a broker answers it or [`FORWARD_FOR`] passed. A call whose attempts
/// are spent is never asked again of that broker by the forward: the gateway's `on_call_failed` passes
/// the birth over, and the next turn asks the next replica.
async fn forward(client: &NodeRpcClient, node: &Node, request: &RecordsRequest) -> RecordsReply {
    let until = tokio::time::Instant::now() + FORWARD_FOR;
    let options = CallOptions { resend: Resend::with_time_limit(Duration::from_secs(5)), ..CallOptions::default() };
    let mut last = String::from("no broker primary is held");
    loop {
        let mut spent = None;
        if let Some(broker) = node.broker_to_ask() {
            let (outcome, _) = client.call::<Records>(&NodeTarget::ExactNode(broker.clone()), request, &options).await;
            last = match outcome {
                RpcOutcome::Reply(reply) => match reply.into_value() {
                    RecordsReply::NotPrimary { primary: held } => {
                        node.passed.ask_again(&held);
                        format!("{broker} is not the broker primary (it holds {held:?})")
                    }
                    RecordsReply::NotReady { reason } | RecordsReply::Busy { reason } | RecordsReply::ConnectionsDeleting { reason } => format!("{broker}: {reason}"),
                    answered => {
                        tracing::info!(target: "rshape", path = %node.name.path, name = %node.name.name, %broker, "a request forwarded to a broker answered");
                        return answered;
                    }
                },
                // Never answered: when its attempts are spent (RDM resent it), `on_call_failed` passes
                // the broker over, and the next turn asks the next replica.
                outcome @ (RpcOutcome::NotSent(_) | RpcOutcome::Indeterminate(_)) => {
                    spent = Some(broker.clone());
                    format!("{broker}: {} {}", outcome.name(), outcome.detail_name())
                }
                other => format!("{broker}: {} {}", other.name(), other.detail_name()),
            };
        }
        if tokio::time::Instant::now() >= until {
            return RecordsReply::NoBroker { reason: format!("path {:?}, name {:?}: {last}", node.name.path, node.name.name) };
        }
        match spent {
            Some(broker) => node.passed.until_passed(&broker, ASK_EVERY).await,
            None => tokio::time::sleep(ASK_EVERY).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> (NodeId, IncarnationId) {
        (NodeId::mint(), IncarnationId::mint())
    }

    #[test]
    fn a_forward_asks_the_broker_primary_until_on_call_failed_passes_it_over_then_the_next_replica() {
        let (primary, other) = (broker(), broker());
        let ready = vec![primary.clone(), other.clone()];
        let mut passed = BTreeMap::new();
        assert_eq!(choose(Some(primary.0.clone()), &ready, &mut passed), Some(primary.0.clone()), "nothing passed over: the broker primary");
        passed.insert(primary.0.clone(), primary.1.clone());
        assert_eq!(choose(Some(primary.0.clone()), &ready, &mut passed), Some(other.0.clone()), "the primary passed over: the next ready broker");
        passed.insert(other.0.clone(), other.1.clone());
        assert_eq!(choose(Some(primary.0.clone()), &ready, &mut passed), None, "every ready broker passed over: none is asked");
        assert_eq!(choose(None, &ready, &mut BTreeMap::new()), None, "no broker primary held: the forward waits for RDM's election");
    }

    #[test]
    fn a_birth_the_topology_no_longer_names_ready_is_passed_over_no_more() {
        let (primary, other) = (broker(), broker());
        let mut passed = BTreeMap::from([primary.clone(), other.clone()]);
        // The primary started as a new incarnation; the other broker left the ready set.
        let started = (primary.0.clone(), IncarnationId::mint());
        assert_eq!(choose(Some(primary.0.clone()), &[started], &mut passed), Some(primary.0.clone()), "its new birth is asked");
        assert!(passed.is_empty(), "both marks dropped: {passed:?}");
    }

    #[tokio::test]
    async fn on_call_failed_passes_its_receiver_over_and_a_broker_naming_it_primary_sends_the_gateway_back() {
        let passed = PassedOver::default();
        let (receiver, _) = broker();
        let birth = |node_id: NodeId| rdm_node::node_hooks::CallFailedBirth { node_id, incarnation: IncarnationId::mint(), name: PathName::numbered("mesh1", NodeKind::Broker, 1) };
        let reason = rdm_transport::CallFailedReason::NotSent(rdm_core::outcome::NotSentReason::Deadline);
        PassOver(passed.clone()).on_call_failed(&CallFailedCtx { sender: birth(NodeId::mint()), receiver: Some(birth(receiver.clone())), reason }).await;
        passed.until_passed(&receiver, Duration::from_secs(1)).await;
        assert!(passed.0.borrow().contains_key(&receiver), "the spent call's receiver is passed over");
        passed.ask_again(&receiver.to_string());
        assert!(passed.0.borrow().is_empty(), "a broker that holds it primary sends the gateway back to it");
    }

    #[tokio::test]
    async fn a_gateway_whose_call_is_spent_is_not_asked_again_and_the_next_gateway_is_asked() {
        let (first, second, third) = (broker(), broker(), broker());
        let ready = vec![first.clone(), second.clone(), third.clone()];
        let mut spent = BTreeMap::new();
        let mut asked = Vec::new();
        // Round one: the first gateway's call is spent, the second answers not ready, the third's is spent.
        let answers = ask_each_gateway(&ready, &mut spent, |g| {
            asked.push(g.clone());
            let answer = if g == second.0 { Asked::Answered("NotReady".into()) } else { Asked::Spent("NotSent connect-refused".into()) };
            async move { answer }
        })
        .await;
        assert!(answers.is_err(), "no gateway produced the record: {answers:?}");
        assert_eq!(asked, vec![first.0.clone(), second.0.clone(), third.0.clone()], "each ready gateway asked once, in order");
        assert!(spent.len() == 2 && spent.contains_key(&first.0) && spent.contains_key(&third.0), "the two spent gateways are passed over: {spent:?}");
        // Round two (the topology changed): only the gateway that answered keeps its turn, and produces.
        asked.clear();
        let produced = ask_each_gateway(&ready, &mut spent, |g| {
            asked.push(g.clone());
            async move { Asked::Produced { offset: 7 } }
        })
        .await;
        assert_eq!(produced, Ok(7));
        assert_eq!(asked, vec![second.0.clone()], "a gateway whose call is spent is never asked again");
        // Every ready gateway spent: none is asked until the topology names a new birth.
        spent.insert(second.0.clone(), second.1.clone());
        asked.clear();
        let none = ask_each_gateway(&ready, &mut spent, |g| {
            asked.push(g.clone());
            async move { Asked::Produced { offset: 0 } }
        })
        .await;
        assert_eq!(none, Err(vec!["every ready gateway's call is spent".to_string()]));
        assert!(asked.is_empty(), "no spent gateway is asked: {asked:?}");
        // The first gateway started as a new incarnation: that birth is asked.
        let restarted = (first.0.clone(), IncarnationId::mint());
        let produced = ask_each_gateway(&[restarted, second.clone(), third.clone()], &mut spent, |g| {
            asked.push(g.clone());
            async move { Asked::Produced { offset: 8 } }
        })
        .await;
        assert_eq!((produced, asked), (Ok(8), vec![first.0.clone()]), "the new birth at the first gateway is asked");
    }
}
