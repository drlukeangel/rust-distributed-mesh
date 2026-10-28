//! The compiled consumer example of `docs/eng/consumer-guide-ops.md`: one product's own ops on Node
//! RPC, end to end, on RDM's public API alone. A product copies this file and changes its first
//! section, the protocols (the request and reply enums, the record's `Entry`, and the constants
//! beside them: op codes, family names, owner, the kinds that serve and call); everything below that
//! section is the integration a product keeps.
//!
//! What it holds, in the guide's order:
//!
//! - **Two protocols** (§1, §2): [`ExampleWork`], a unary op that starts an operation named by the
//!   caller's own operation id (mutating) or reads its state by id (read-only); and
//!   [`ExampleRecords`], a server-streaming op that reads the serving node's operations record from
//!   an index, the stream's terminal naming where the next read starts.
//! - **An application service held in an `Arc`** ([`ExampleService`]): its durable record ([`ExampleRecord`]) is a
//!   JSON-lines file in the node's data dir; every handler clones the `Arc` (§4).
//! - **Registration** (§3): [`ledger_rows`] and [`register`], the `serve_app` handed to
//!   `rdm_node::run_with` by [`run`]. The unary op is served behind the `pre_node_ready` gate
//!   (`ServerBuilder::serve_gated`); the streaming handler reads the same gate itself.
//! - **A mutating handler that records, then accepts** (§3, §5), on RDM's recovery helpers
//!   (`rdm_node::operation`): the record is the application's [`OperationStore`] ([`ExampleRecord`]),
//!   and `Submit` is decided by `Operations::decide`, which records a new operation and syncs it,
//!   then calls `peer.acceptance.accepted()`; a repeat answers the recorded state or result, and its
//!   id with another input is refused `Conflict`. `Get` is `Operations::read`.
//! - **A restart that recovers the pending work** (§5 step 4): [`Resume`], the node's
//!   `pre_node_ready`, starts the work of every operation `Operations::pending` names (accepted and
//!   not completed) before the node is Ready; each work's end is `Operations::complete`.
//! - **Target selection from the topology and calls through the process's client, every outcome
//!   handled** (§4, §5 step 5, §6): [`Caller`], the node's `on_node_ready` on the calling kind. It
//!   picks a serving node with `TopologyView::nodes_get`, finds an operation's node again with
//!   `DigestView::get_by_path_name`, handles `Reply`, `NotSent`, `Indeterminate`, `Unserved` and
//!   `RejectedStale`, asks `Get` by operation id after an `Indeterminate` (never a second `Submit`
//!   merely because a reply was lost), and copies each serving node's record through
//!   [`ExampleRecords`], acknowledging only frames it recorded ([`read_records`]).
//!
//! Telemetry: the guide's checklist (§7 item 4) names a product's spans
//! `<owner>.<domain>.<entity>.<action>.via-<reason>`. This file is in RDM's tree, where every span is
//! `rdm.*` (the `RdmSpansAreRdmPrefixed` ratchet); so what the application does is a log line
//! (target `rshape`) under RDM's own call, serve and `pre_node_ready` spans, and under the recovery
//! helpers' spans (`rdm.node_rpc.operation.*`, each naming the `operation`). A product's copy names
//! its own spans there.
//!
//! On the demo estate (`scripts/demo-estate.sh example`) the `rshape-example` executable runs this
//! as one compute and one broker; the other nodes run the R-shape roles, which do not serve these
//! ops: a `Submit` to one ends `Unserved`, a records copy from one fails `Unserved` on its first
//! item ([`ReadFailure::Unserved`]), and the caller passes that node over.

use rdm_core::catalog::{LedgerEntry, OpOwner, OpState};
use rdm_core::filter::Filter;
use rdm_core::framing::DeliveryId;
use rdm_core::outcome::{Code, MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use rdm_core::streaming::{FrameKind, StreamingProtocol};
use rdm_core::{NodeId, NodeKind, PathName};
use rdm_node::node_hooks::{HookOutcome, NodeHooks, NodeReadyCtx, OnNodeReady, PreNodeReady};
use rdm_node::operation::{Decision, OperationStore, Operations, Recorded, State};
use rdm_node::Role;
use rdm_transport::stream::{ReplyStream, SinkError, StreamFailure, StreamItem};
use rdm_transport::{Budget, CallOptions, HandlerFault, NodeRpcClient, NodeTarget, PreNodeReadyGate, ServerBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::Instrument;

// ---------------------------------------------------------------------------------------------------
// 1. The protocols: what a product changes.
// ---------------------------------------------------------------------------------------------------

/// The product that owns the two ops, as its ledger rows and its serving name it.
pub const OWNER: &str = "rshape-consumer";
/// The kind whose nodes serve the ops and hold the record.
pub const SERVING_KIND: NodeKind = NodeKind::Broker;
/// The kind whose nodes call them ([`Caller`]).
pub const CALLING_KIND: NodeKind = NodeKind::Compute;

/// A request of [`ExampleWork`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkRequest {
    /// Start the operation `operation` on `input`. A second `Submit` of one id with the same input
    /// answers what the record holds for it; with another input it is refused `Conflict`.
    Submit { operation: String, input: String },
    /// The state of the operation `operation`, by its id: what a caller asks after an
    /// `Indeterminate`. Read-only.
    Get { operation: String },
}

/// A reply of [`ExampleWork`]: its results, its named refusals, then the six reserved variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkReply {
    /// The operation is recorded and its work is under way (or was already).
    Accepted { operation: String },
    /// The operation completed, with its output.
    Done { operation: String, output: String },
    /// `Get`: the operation is recorded and its work not yet completed.
    Pending { operation: String },
    /// `Submit`: the id is recorded with another input.
    Conflict { operation: String },
    /// `Get`: no operation of that id is recorded.
    Unknown { operation: String },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

/// The unary op: start an operation, or read its state.
pub struct ExampleWork;

impl NodeProtocol for ExampleWork {
    const OP: u8 = 0x50;
    const NAME: &'static str = "example-work";
    const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 160 * 1024;
    type Request = WorkRequest;
    type Reply = WorkReply;

    rdm_core::protocol_variants! {
        request WorkRequest { Submit, Get }
        reply WorkReply {
            Accepted = "accepted" => ReplyKind::Success,
            Done = "done" => ReplyKind::Success,
            Pending = "pending" => ReplyKind::Success,
            Conflict = "conflict" => ReplyKind::new(Code::AlreadyExists),
            Unknown = "unknown" => ReplyKind::new(Code::NotFound),
            @reserved,
        }
    }

    fn read_only(req: &WorkRequest) -> bool {
        matches!(req, WorkRequest::Get { .. })
    }
}

/// One line of the operations record: an operation accepted, or its completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Entry {
    /// The operation was accepted on `input`.
    Accepted { operation: String, input: String },
    /// The operation completed with `output`.
    Completed { operation: String, output: String },
}

/// A request of [`ExampleRecords`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordsRequest {
    /// The record's entries from index `from`, at most `max` of them.
    Read { from: u64, max: u32 },
}

/// A frame of [`ExampleRecords`]: `Started`, one `Entry` per record line, the terminal `Through`; or
/// one refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordsFrame {
    /// The read began; the record then held `end` entries.
    Started { end: u64 },
    /// The record's entry at `index`.
    Entry { index: u64, entry: Entry },
    /// The terminal: the next read starts at `next`.
    Through { next: u64 },
    /// `from` is past the record's end, `end`.
    OutOfRange { end: u64 },
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    ConnectionsDeleting { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

/// The server-streaming op: read the operations record from an index.
pub struct ExampleRecords;

impl NodeProtocol for ExampleRecords {
    const OP: u8 = 0x51;
    const NAME: &'static str = "example-records";
    const MAX_REQUEST_FRAME_BYTES: usize = 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 160 * 1024;
    type Request = RecordsRequest;
    type Reply = RecordsFrame;

    rdm_core::protocol_variants! {
        request RecordsRequest { Read }
        reply RecordsFrame {
            Started = "started" => ReplyKind::Success,
            Entry = "entry" => ReplyKind::Success,
            Through = "through" => ReplyKind::Success,
            OutOfRange = "out-of-range" => ReplyKind::new(Code::OutOfRange),
            @reserved,
        }
    }

    fn read_only(_req: &RecordsRequest) -> bool {
        true
    }
}

impl StreamingProtocol for ExampleRecords {
    fn frame_kind(frame: &RecordsFrame) -> FrameKind {
        match frame {
            RecordsFrame::Started { .. } => FrameKind::Started,
            RecordsFrame::Entry { .. } => FrameKind::Data,
            RecordsFrame::Through { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Self::classify_reply(other)),
        }
    }
}

/// The application's work on an accepted operation: a pure function of its input, so running it
/// again after a restart yields the same output (§5 step 3).
fn work(input: &str) -> String {
    input.to_uppercase()
}

// ---------------------------------------------------------------------------------------------------
// 2. The application service and its durable record.
// ---------------------------------------------------------------------------------------------------

/// The record's file, under the directory [`ExampleService::open`] is given.
const RECORD_FILE: &str = "operations.jsonl";

struct Held {
    file: std::fs::File,
    entries: Vec<Entry>,
    operations: BTreeMap<String, Recorded<String, String>>,
}

/// The application's durable record of its operations: a JSON-lines file of [`Entry`], folded in
/// memory. It is the [`OperationStore`] RDM's recovery helpers decide over: it persists what it is
/// handed and decides nothing.
#[derive(Clone)]
pub struct ExampleRecord {
    held: Arc<Mutex<Held>>,
}

impl ExampleRecord {
    /// Open the record under `dir` (created when absent) and fold it. A last line cut short by a
    /// crash was never synced, so never accepted: it is cut from the file. Any other line that does
    /// not decode refuses the open, naming its line.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(RECORD_FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(e),
        };
        let (mut entries, mut whole) = (Vec::new(), 0usize);
        for (n, line) in text.split_inclusive('\n').enumerate() {
            if !line.ends_with('\n') {
                break;
            }
            let entry: Entry = serde_json::from_str(line.trim_end()).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{} line {}: {e}", path.display(), n + 1))
            })?;
            entries.push(entry);
            whole += line.len();
        }
        let file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
        if whole < text.len() {
            file.set_len(whole as u64)?;
            file.sync_all()?;
        }
        let mut operations = BTreeMap::new();
        for entry in &entries {
            fold(&mut operations, entry);
        }
        Ok(Self { held: Arc::new(Mutex::new(Held { file, entries, operations })) })
    }

    /// The record's length and its entries from `from`, at most `max`; `Err(end)` when `from` is past
    /// the end.
    pub fn entries(&self, from: u64, max: u32) -> Result<(u64, Vec<(u64, Entry)>), u64> {
        let held = self.held.lock().unwrap();
        let end = held.entries.len() as u64;
        if from > end {
            return Err(end);
        }
        let to = end.min(from.saturating_add(u64::from(max)));
        Ok((end, (from..to).map(|i| (i, held.entries[i as usize].clone())).collect()))
    }

    /// Append `entry` and sync it, off the async threads; it is held in memory once durable.
    async fn append(&self, entry: Entry) -> std::io::Result<()> {
        let held = self.held.clone();
        tokio::task::spawn_blocking(move || {
            let mut held = held.lock().unwrap();
            let mut line = serde_json::to_string(&entry).map_err(std::io::Error::other)?;
            line.push('\n');
            held.file.write_all(line.as_bytes())?;
            held.file.sync_data()?;
            fold(&mut held.operations, &entry);
            held.entries.push(entry);
            Ok(())
        })
        .await
        .map_err(|e| std::io::Error::other(format!("the record's write task ended: {e}")))?
    }
}

#[async_trait::async_trait]
impl OperationStore for ExampleRecord {
    type Input = String;
    type Output = String;
    type Error = std::io::Error;

    async fn get(&self, operation: &str) -> std::io::Result<Option<Recorded<String, String>>> {
        Ok(self.held.lock().unwrap().operations.get(operation).cloned())
    }

    async fn put_accepted(&self, operation: &str, input: &String) -> std::io::Result<()> {
        self.append(Entry::Accepted { operation: operation.into(), input: input.clone() }).await
    }

    async fn put_completed(&self, operation: &str, output: &String) -> std::io::Result<()> {
        self.append(Entry::Completed { operation: operation.into(), output: output.clone() }).await
    }

    async fn pending(&self) -> std::io::Result<Vec<(String, String)>> {
        let held = self.held.lock().unwrap();
        Ok(held.operations.iter().filter(|(_, op)| op.output.is_none()).map(|(id, op)| (id.clone(), op.input.clone())).collect())
    }
}

fn fold(operations: &mut BTreeMap<String, Recorded<String, String>>, entry: &Entry) {
    match entry {
        Entry::Accepted { operation, input } => {
            operations.entry(operation.clone()).or_insert(Recorded { input: input.clone(), output: None });
        }
        Entry::Completed { operation, output } => {
            if let Some(op) = operations.get_mut(operation) {
                op.output = Some(output.clone());
            }
        }
    }
}

/// The application service: RDM's recovery helpers over its record, and the work it runs.
pub struct ExampleService {
    work_time: Duration,
    operations: Operations<ExampleRecord>,
    /// Operations whose work runs in this process now.
    running: Mutex<BTreeSet<String>>,
}

impl ExampleService {
    /// Open the record under `dir` ([`ExampleRecord::open`]). `work_time` is how long one operation's
    /// work takes.
    pub fn open(dir: &Path, work_time: Duration) -> std::io::Result<Arc<Self>> {
        Ok(Arc::new(Self { work_time, operations: Operations::new(ExampleRecord::open(dir)?), running: Mutex::default() }))
    }

    /// The recovery helpers' decisions over the record.
    pub fn operations(&self) -> &Operations<ExampleRecord> {
        &self.operations
    }

    /// Start the work of every operation the record holds accepted and not completed, and not already
    /// running here: what a start does before the node is Ready. Returns how many it started.
    pub async fn resume(self: &Arc<Self>) -> std::io::Result<usize> {
        let pending = self.operations.pending().await?;
        Ok(pending.into_iter().filter(|(id, input)| self.start(id.clone(), input.clone())).count())
    }

    /// Start `operation`'s work on `input` unless it runs already; `true` when this started it. The
    /// work ends by recording the completion; a completion that cannot be recorded leaves the
    /// operation pending, for the next start to finish.
    pub fn start(self: &Arc<Self>, operation: String, input: String) -> bool {
        if !self.running.lock().unwrap().insert(operation.clone()) {
            return false;
        }
        let svc = self.clone();
        tokio::spawn(
            async move {
                tokio::time::sleep(svc.work_time).await;
                let recorded = svc.operations.complete(&operation, &work(&input)).await;
                svc.running.lock().unwrap().remove(&operation);
                match recorded {
                    Ok(_) => tracing::info!(target: "rshape", %operation, "an operation's work completed and its completion is recorded"),
                    Err(e) => tracing::error!(target: "rshape", %operation, error = %e, "an operation's completion could not be recorded: it stays pending until the next start"),
                }
            }
            // The completion is in the trace of what started the work: the request that recorded it,
            // or the start that resumed it.
            .in_current_span(),
        );
        true
    }
}

// ---------------------------------------------------------------------------------------------------
// 3. Registration: the ledger rows and the two handlers.
// ---------------------------------------------------------------------------------------------------

/// The ledger rows reserving the two ops for [`OWNER`].
pub fn ledger_rows() -> Vec<LedgerEntry> {
    [(ExampleWork::OP, ExampleWork::NAME), (ExampleRecords::OP, ExampleRecords::NAME)]
        .into_iter()
        .map(|(op, family)| LedgerEntry { op, family: family.to_string(), owner: OpOwner::Product(OWNER.to_string()), state: OpState::Live })
        .collect()
}

/// The `serve_app` of a node serving the two ops over `service`. `gate` is the node's
/// `pre_node_ready` gate (`NodeHooks::gate`): neither op answers before [`Resume`] passed.
pub fn register(b: ServerBuilder, service: Arc<ExampleService>, gate: PreNodeReadyGate) -> ServerBuilder {
    let work = service.clone();
    let records = service;
    let records_gate = gate.clone();
    b.ledger(ledger_rows())
        .serve_gated::<ExampleWork, _, _>(OpOwner::Product(OWNER.to_string()), gate, move |peer, request| {
            let svc = work.clone();
            async move {
                let faulted = |e: std::io::Error| HandlerFault::invariant_broken(format!("the operations record: {e}"));
                match request {
                    WorkRequest::Submit { operation, input } => {
                        // Recorded and synced before it is accepted: the runtime acks the request
                        // only once its work is recoverable. A repeat answers what the record holds.
                        let decision = svc.operations().decide(&peer.acceptance, &operation, &input).await.map_err(faulted)?;
                        Ok(match decision {
                            Decision::Recorded => {
                                svc.start(operation.clone(), input);
                                tracing::info!(target: "rshape", %operation, "an operation recorded, then accepted; its work started");
                                WorkReply::Accepted { operation }
                            }
                            Decision::InFlight => WorkReply::Accepted { operation },
                            Decision::Completed(output) => WorkReply::Done { operation, output },
                            Decision::Conflict => WorkReply::Conflict { operation },
                        })
                    }
                    WorkRequest::Get { operation } => Ok(match svc.operations().read(&operation).await.map_err(faulted)? {
                        State::Unknown => WorkReply::Unknown { operation },
                        State::InFlight => WorkReply::Pending { operation },
                        State::Completed(output) => WorkReply::Done { operation, output },
                    }),
                }
            }
        })
        .serve_stream::<ExampleRecords, _, _>(OpOwner::Product(OWNER.to_string()), move |_peer, request, sink| {
            let (svc, gate) = (records.clone(), records_gate.clone());
            async move {
                if let Some(reason) = gate.refusal() {
                    return Ok(ExampleRecords::not_ready(reason));
                }
                let RecordsRequest::Read { from, max } = request;
                let (end, entries) = match svc.operations().store().entries(from, max) {
                    Ok(read) => read,
                    Err(end) => return Ok(RecordsFrame::OutOfRange { end }),
                };
                let mut sink = match sink.started(RecordsFrame::Started { end }).await {
                    Ok(sink) => sink,
                    Err(SinkError::CallerGone(_)) => return Ok(RecordsFrame::Through { next: from }),
                    Err(e) => return Err(HandlerFault::invariant_broken(format!("the Started frame: {e:?}"))),
                };
                let mut next = from;
                for (index, entry) in entries {
                    match sink.data(RecordsFrame::Entry { index, entry }).await {
                        Ok(()) => next = index + 1,
                        // The caller ended the invocation: nothing it was not handed is owed.
                        Err(SinkError::CallerGone(_)) => break,
                        Err(e) => return Err(HandlerFault::invariant_broken(format!("entry {index}: {e:?}"))),
                    }
                }
                Ok(RecordsFrame::Through { next })
            }
        })
}

/// Why a read of [`ExampleRecords`] ended without its terminal.
#[derive(Debug)]
pub enum ReadFailure {
    /// The node does not serve the op (`421`), answered as the stream's first item: nothing was
    /// dispatched, as an open that ends `Unserved`.
    Unserved,
    /// A refusal, a frame that could not be recorded, or the stream's failure.
    Ended(String),
}

/// Read an [`ExampleRecords`] stream to its end. Each frame is handed to `record` with its position
/// and acknowledged only once `record` returned `Ok`: what is acknowledged is recorded first (§6).
/// Returns the terminal's `next`, or why the stream ended without one; a `record` error ends the
/// read unacknowledged, and dropping the stream ends the invocation.
pub async fn read_records(
    mut stream: ReplyStream<ExampleRecords>,
    mut record: impl FnMut(Option<(&DeliveryId, u32)>, FrameKind, &RecordsFrame) -> std::io::Result<()>,
) -> Result<u64, ReadFailure> {
    let mut next = None;
    while let Some(item) = stream.next().await {
        match item {
            StreamItem::Frame(kind, frame) => {
                record(stream.position(), kind, &frame).map_err(|e| ReadFailure::Ended(format!("the frame could not be recorded: {e}")))?;
                stream.accepted().await;
                match frame {
                    RecordsFrame::Through { next: n } => next = Some(n),
                    RecordsFrame::Started { .. } | RecordsFrame::Entry { .. } => {}
                    refusal => return Err(ReadFailure::Ended(format!("refused {}", ExampleRecords::reply_detail(&refusal)))),
                }
            }
            StreamItem::Failed(StreamFailure::Unserved(_)) => return Err(ReadFailure::Unserved),
            // The recorded entries stand; nothing past them is assumed.
            StreamItem::Failed(StreamFailure::Indeterminate(reason)) => return Err(ReadFailure::Ended(format!("indeterminate: {}", reason.detail_name()))),
            StreamItem::Failed(failure) => return Err(ReadFailure::Ended(format!("{failure:?}"))),
        }
    }
    next.ok_or_else(|| ReadFailure::Ended("the stream ended without its terminal".into()))
}

// ---------------------------------------------------------------------------------------------------
// 4. The node: its hooks and its run.
// ---------------------------------------------------------------------------------------------------

/// The directory under the node's data dir that holds the record and the copies [`Caller`] keeps.
pub const DATA_SUBDIR: &str = "consumer-example";
/// How long one operation's work takes on the estate: long enough that a broker stopped by a crash
/// holds pending work.
pub const WORK_TIME: Duration = Duration::from_secs(20);
/// How often [`Caller`] submits a new operation and copies a record.
pub const CALL_EVERY: Duration = Duration::from_secs(10);

/// The node's `pre_node_ready`: resume the pending work its record holds. A first start over a new
/// data dir finds none; a start over an inherited data dir (node recovery after the process ended)
/// finds what the ended process accepted and did not complete.
pub struct Resume(pub Arc<ExampleService>);

#[async_trait::async_trait]
impl PreNodeReady for Resume {
    async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
        match self.0.resume().await {
            Ok(resumed) => {
                tracing::info!(target: "rshape", node = %ctx.birth.name, start_kind = ?ctx.start_kind, inherited = ctx.inherited, resumed, "the operations record is read; its pending operations' work is resumed");
                HookOutcome::Ok
            }
            Err(e) => HookOutcome::Failed { reason: format!("the operations record could not be read: {e}") },
        }
    }
}

/// Where one operation stands for its caller.
enum Owed {
    /// Not delivered to any node yet (or provably never dispatched): sent with the next `Submit`.
    ToSend,
    /// Accepted by the node at this path: asked `Get` until it is done.
    Accepted(PathName),
    /// Sent to the node at this path, its answer unknown: asked `Get`, never submitted again until
    /// that node says it does not hold it.
    Uncertain(PathName),
    /// Done (or refused `Conflict`): dropped at the end of the tick.
    Done,
}

/// The calling node's `on_node_ready`: every [`CALL_EVERY`] it submits a new operation to a serving
/// node chosen from the topology, follows every operation it owes to its result, and copies the
/// chosen node's record. It ends when the birth is stopped.
pub struct Caller {
    /// Where its copies of the serving nodes' records are kept.
    pub dir: PathBuf,
}

#[async_trait::async_trait]
impl OnNodeReady for Caller {
    async fn on_node_ready(&self, ctx: &NodeReadyCtx) {
        if ctx.birth.kind != CALLING_KIND {
            return;
        }
        // The calls are the application's own task, ended by the birth's stop, and outside the
        // hook's span: each call is the root of its own trace, not a child of the node's boot.
        let (caller, ctx) = (Caller { dir: self.dir.clone() }, ctx.clone());
        tokio::spawn(
            async move {
                tokio::select! {
                    () = ctx.cancel.cancelled() => {}
                    () = caller.calls(&ctx) => {}
                }
            }
            .instrument(tracing::Span::none()),
        );
    }
}

impl Caller {
    async fn calls(&self, ctx: &NodeReadyCtx) {
        let client = ctx.client.clone();
        let mut owed: BTreeMap<String, (String, Owed)> = BTreeMap::new();
        let mut unserved: BTreeSet<NodeId> = BTreeSet::new();
        let mut copied: BTreeMap<PathName, u64> = BTreeMap::new();
        let mut tick = tokio::time::interval(CALL_EVERY);
        // A tick missed while a call waited out its budget is not made up in a burst.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        for n in 1u64.. {
            tick.tick().await;
            owed.insert(format!("{}-{}-{n:06}", ctx.birth.name, ctx.birth.incarnation.as_str()), (format!("operation {n} of {}", ctx.birth.name), Owed::ToSend));
            // Target selection: the serving kind's ready nodes in this node's mesh, as the topology
            // this node holds names them; a node that answered Unserved is passed over.
            let serving: Vec<_> = match ctx.topology_view.nodes_get(&Filter::new().eq("mesh", [ctx.birth.name.mesh.clone()]).eq("kind", [SERVING_KIND.segment()]).eq("status", ["ready"])) {
                Ok(nodes) => nodes.into_iter().filter(|d| !unserved.contains(&d.node.node_id)).collect(),
                Err(refused) => {
                    tracing::error!(target: "rshape", %refused, "the topology query was refused");
                    return;
                }
            };
            let chosen = (!serving.is_empty()).then(|| serving[(n as usize) % serving.len()].node.clone());
            tracing::info!(target: "rshape", candidates = serving.len(), chosen = ?chosen.as_ref().map(|c| c.name.to_string()), owed = owed.len(), "serving nodes selected from the topology");
            for (operation, (input, state)) in owed.iter_mut() {
                match state {
                    Owed::ToSend => {
                        let Some(target) = &chosen else { continue };
                        let request = WorkRequest::Submit { operation: operation.clone(), input: input.clone() };
                        let (outcome, _) = client.call::<ExampleWork>(&NodeTarget::ExactNode(target.node_id.clone()), &request, &CallOptions::default()).await;
                        log_outcome("submit", operation, &target.name, &outcome);
                        *state = match outcome {
                            RpcOutcome::Reply(reply) => match reply.into_value() {
                                WorkReply::Accepted { .. } | WorkReply::Pending { .. } => Owed::Accepted(target.name.clone()),
                                WorkReply::Done { .. } | WorkReply::Conflict { .. } => Owed::Done,
                                // not_ready, busy, connections_deleting, malformed: the node did not
                                // take it; it is sent again with the next Submit.
                                _ => Owed::ToSend,
                            },
                            // Provably never dispatched: safe to send again.
                            RpcOutcome::NotSent(_) | RpcOutcome::RejectedStale(_) => Owed::ToSend,
                            // The node does not serve the op: chosen no more.
                            RpcOutcome::Unserved(_) => {
                                unserved.insert(target.node_id.clone());
                                Owed::ToSend
                            }
                            // Sent, its answer unknown: asked by id, never submitted blindly.
                            RpcOutcome::Indeterminate(_) => Owed::Uncertain(target.name.clone()),
                        };
                    }
                    Owed::Done => {}
                    Owed::Accepted(path) | Owed::Uncertain(path) => {
                        // A point lookup: the node now at the path that holds the operation (a
                        // recovered node there is a new node id with the same record).
                        let Some((digest, _)) = ctx.topology_view.digest_view.get_by_path_name(path) else { continue };
                        let request = WorkRequest::Get { operation: operation.clone() };
                        let (outcome, _) = client.call::<ExampleWork>(&NodeTarget::ExactNode(digest.node.node_id.clone()), &request, &CallOptions::default()).await;
                        log_outcome("get", operation, path, &outcome);
                        if let RpcOutcome::Reply(reply) = outcome {
                            match reply.into_value() {
                                WorkReply::Done { .. } => *state = Owed::Done,
                                // The node holds no such operation: it never recorded it, so it is
                                // submitted again under the same id.
                                WorkReply::Unknown { .. } => *state = Owed::ToSend,
                                _ => {}
                            }
                        }
                        // Any other outcome of a read-only Get: asked again at the next tick.
                    }
                }
            }
            owed.retain(|_, (_, state)| !matches!(state, Owed::Done));
            if let Some(target) = chosen.as_ref().filter(|t| !unserved.contains(&t.node_id)) {
                self.copy(&client, target.node_id.clone(), &target.name, &mut copied, &mut unserved).await;
            }
        }
    }

    /// Copy the record of the node at `path` from where this copy ends, through [`ExampleRecords`]:
    /// each entry is appended to the copy and synced before it is acknowledged.
    async fn copy(&self, client: &NodeRpcClient, node: NodeId, path: &PathName, copied: &mut BTreeMap<PathName, u64>, unserved: &mut BTreeSet<NodeId>) {
        let file = self.dir.join(format!("copy-of-{path}.jsonl"));
        let from = *copied.entry(path.clone()).or_insert_with(|| std::fs::read_to_string(&file).map(|t| t.lines().count() as u64).unwrap_or(0));
        let options = CallOptions { budget: Budget::Overall(Duration::from_secs(10)), ..Default::default() };
        match client.call_stream::<ExampleRecords>(&NodeTarget::ExactNode(node.clone()), &RecordsRequest::Read { from, max: 256 }, &options).await {
            Ok((stream, _)) => {
                let mut out = match std::fs::OpenOptions::new().create(true).append(true).open(&file) {
                    Ok(f) => f,
                    Err(e) => return tracing::error!(target: "rshape", file = %file.display(), error = %e, "the copy could not be opened"),
                };
                let mut next = from;
                let ended = read_records(stream, |_, _, frame| {
                    if let RecordsFrame::Entry { index, entry } = frame {
                        if *index == next {
                            out.write_all(format!("{}\n", serde_json::to_string(entry).map_err(std::io::Error::other)?).as_bytes())?;
                            out.sync_data()?;
                            next += 1;
                        }
                    }
                    Ok(())
                })
                .await;
                copied.insert(path.clone(), next);
                // The node does not serve the op: chosen no more, as an open that ends Unserved.
                if let Err(ReadFailure::Unserved) = ended {
                    unserved.insert(node);
                    return tracing::info!(target: "rshape", node = %path, outcome = "Unserved", detail = "unserved-op", "the record copy's stream opened and its first item is Unserved: the node does not serve the op");
                }
                tracing::info!(target: "rshape", node = %path, from, next, ended = ?ended, "a serving node's record copied");
            }
            Err((outcome, _)) => {
                if let RpcOutcome::Unserved(_) = outcome {
                    unserved.insert(node);
                }
                tracing::info!(target: "rshape", node = %path, outcome = outcome.name(), detail = outcome.detail_name(), "the record copy did not open");
            }
        }
    }
}

fn log_outcome<R>(call: &str, operation: &str, node: &PathName, outcome: &RpcOutcome<R>) {
    tracing::info!(target: "rshape", call, %operation, %node, outcome = outcome.name(), detail = outcome.detail_name(), code = ?outcome.code(), "an example call answered");
}

/// The `rshape-example` node: its role is the kind its launch names; it serves the two ops over the
/// record in its data dir, resumes the record's pending work before Ready, and, on the calling kind,
/// runs [`Caller`] after Ready. Everything enters through `rdm_node::run_with`.
pub async fn run() -> anyhow::Result<()> {
    let launch = rdm_node::read_launch()?;
    let dir = launch.data_dir.join(DATA_SUBDIR);
    let service = ExampleService::open(&dir, WORK_TIME).map_err(|e| anyhow::anyhow!("refusing to start: the operations record: {e}"))?;
    let hooks = NodeHooks::new(Arc::new(Resume(service.clone()))).with_on_node_ready(Arc::new(Caller { dir }));
    let gate = hooks.gate();
    rdm_node::run_with(Role::of(&launch), &[], hooks, move |b| register(b, service, gate)).await
}
