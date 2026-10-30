//! The replica (Q296): rafka's produce and fetch, as the demo's own node types on RDM's Node RPC.
//!
//! - The **broker** owns a records log in its data dir and serves [`Records`] (op `0x52`): `Produce`
//!   appends a record to a topic at the next offset, idempotent by the caller's operation id (RDM's
//!   recovery helpers, `rdm_node::operation`, as the consumer example); `Fetch` reads a topic from an
//!   offset. Only its mesh's broker primary, the node-type primary RDM elects, takes a `Produce`; any
//!   other broker answers `NotPrimary` naming the primary it holds. A `Fetch` reads the answering
//!   broker's own log.
//! - The **gateway** serves the same op to its callers and forwards each request to its mesh's broker
//!   primary, found from RDM's topology (`TopologyView::primaries`). A `NotPrimary`, an unreached
//!   primary or a call that was never sent is asked again of the primary the gateway then holds, for
//!   up to [`FORWARD_FOR`]: when RDM elects the next broker primary, the gateway follows it. A
//!   `Produce` whose answer was lost is asked again under the same operation id, never as a new record.
//!
//! What the application does is a log line (target `rshape`) under RDM's own call and serve spans, as
//! in [`crate::consumer_example`].

use rdm_core::catalog::{LedgerEntry, OpOwner, OpState};
use rdm_core::outcome::{Code, MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use rdm_core::{NodeId, NodeKind, PathName};
use rdm_node::operation::{Decision, OperationStore, Operations, Recorded};
use rdm_transport::topology::TopologyView;
use rdm_transport::{Budget, CallOptions, HandlerFault, NodeRpcClient, NodeTarget, ServerBuilder};
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
    /// The node's own path.name.
    pub name: PathName,
}

impl Node {
    /// The node id of its mesh's broker primary, as its topology holds it now.
    fn broker_primary(&self) -> Option<NodeId> {
        self.view.get()?.primaries().node_type_primary(&self.name.mesh, NodeKind::Broker).map(|p| p.node_id)
    }
}

/// The broker's `serve_app`: [`Records`] over its log.
pub fn register_broker(b: ServerBuilder, broker: Arc<Broker>, node: Node) -> ServerBuilder {
    b.ledger(ledger_rows()).serve::<Records, _, _>(OpOwner::Product(OWNER.to_string()), move |peer, request| {
        let (broker, node) = (broker.clone(), node.clone());
        async move {
            if node.view.get().is_none() {
                return Ok(Records::not_ready(format!("{} is starting", node.name)));
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
                    tracing::info!(target: "rshape", node = %node.name, %operation, %topic, offset, "a record produced at the broker primary");
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
                return Ok(Records::not_ready(format!("{} is starting", node.name)));
            }
            Ok(forward(&client, &node, &request).await)
        }
    })
}

/// `request` at the broker primary `node` holds, asked again of the primary it then holds until a
/// broker answers it or [`FORWARD_FOR`] passed.
async fn forward(client: &NodeRpcClient, node: &Node, request: &RecordsRequest) -> RecordsReply {
    let until = tokio::time::Instant::now() + FORWARD_FOR;
    let options = CallOptions { budget: Budget::Overall(Duration::from_secs(5)), ..CallOptions::default() };
    let mut last = String::from("no broker primary is held");
    loop {
        if let Some(primary) = node.broker_primary() {
            let (outcome, _) = client.call::<Records>(&NodeTarget::ExactNode(primary.clone()), request, &options).await;
            last = match outcome {
                RpcOutcome::Reply(reply) => match reply.into_value() {
                    RecordsReply::NotPrimary { primary: held } => format!("{primary} is not the broker primary (it holds {held:?})"),
                    RecordsReply::NotReady { reason } | RecordsReply::Busy { reason } | RecordsReply::ConnectionsDeleting { reason } => format!("{primary}: {reason}"),
                    answered => {
                        tracing::info!(target: "rshape", node = %node.name, broker = %primary, "a request forwarded to the broker primary answered");
                        return answered;
                    }
                },
                // Never dispatched, or its answer lost: asked again (a Produce under the same
                // operation id, so a record is never appended twice).
                other => format!("{primary}: {} {}", other.name(), other.detail_name()),
            };
        }
        if tokio::time::Instant::now() >= until {
            return RecordsReply::NoBroker { reason: format!("{}: {last}", node.name) };
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
