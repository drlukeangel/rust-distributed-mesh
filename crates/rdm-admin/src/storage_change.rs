//! A storage change (storage.md §2 "Changing it"; Q198): the Build that moves RDM's five stores from
//! the fabric's provider to the one it names, four steps, each an operation of the Build claimed and
//! dispatched by the fabric-primary as the Build's ordinary order (`build.attempt.run`), each ending in
//! its receipt:
//!
//! 1. **Block**, `mesh.block:<mesh>`, run by each mesh's mesh-primary: it waits until its in-flight
//!    commands complete, replies `blocked` and gossips a current mesh topology of its mesh; from there it
//!    issues no new command until it is unblocked.
//! 2. **Copy** and 3. **Switch**, `storage.switch:<change_id>`, run by the fabric-primary once every
//!    mesh-primary replied `blocked`: the copy ([`rdm_node::storage_copy::copy`]), then the `commit`
//!    record for `(build_id, change_id)` inserted on the new provider with the store's insert-and-fail
//!    primitive (the claim primitive, [`ClaimKind::Commit`](rdm_node::storage_copy::ClaimKind::Commit)), then the Fabric row's line.
//! 4. **Unblock**, `mesh.unblock:<mesh>`, run by each mesh-primary: it moves its nodes to the provider
//!    the `commit` record decided (each node's storage handle replaced, node command op 0x22
//!    `StorageStep`), replies `unblocked` and issues commands again. A node that will not move (it refuses
//!    its move by name, a `rejected-stale-change` included, or leaves it unanswered past the staleness
//!    floor: Q182) is re-created at its `path.name` on the provider the change decided, node.delete then
//!    node.create as node recovery's, steps of this attempt of the change's own Build (i149 R19): the
//!    attempt holds the mesh's in-flight row, so no other attempt could. The mesh replies `unblocked`
//!    once every node moved or was re-created.
//!
//! The Build's verdict, `storage.change:<change_id>`, is the fabric-primary's: a change that failed
//! before its switch fails the Build only once no `commit` is read, its mesh-primaries sent
//! `mesh.unblock` on the old provider. Whoever resumes the change learns the decision only from that
//! record; a provider that does not answer is read again until it does. No timer ends a block.

use crate::build::BuildId;
use crate::model::{NodeId, PathName};
use crate::topology::Topology;
use rdm_core::build::{StorageChange, StorageCommit};
use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
use rdm_core::ChangeId;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;

/// A mesh-primary's `mesh.block` span.
pub const BLOCK_SPAN: &str = "rdm.node_admin.mesh.update.via-block";
/// A mesh-primary's `mesh.unblock` span.
pub const UNBLOCK_SPAN: &str = "rdm.node_admin.mesh.update.via-unblock";

/// The fabric-primary's switch: the `commit` record inserted, or read by whoever resumes the change.
pub const SWITCH_SPAN: &str = "rdm.node_admin.build.update.via-storage-switch";

/// The operation key of a storage change's verdict.
pub fn operation(change_id: &ChangeId) -> String {
    format!("storage.change:{change_id}")
}

/// The operation key of `mesh`'s block (`mesh.block`).
pub fn block_operation(_change_id: &ChangeId, mesh: &str) -> String {
    format!("mesh.block:{mesh}")
}

/// The operation key of the copy and the switch.
pub fn switch_operation(change_id: &ChangeId) -> String {
    format!("storage.switch:{change_id}")
}

/// The operation key of `mesh`'s unblock (`mesh.unblock`).
pub fn unblock_operation(_change_id: &ChangeId, mesh: &str) -> String {
    format!("mesh.unblock:{mesh}")
}

/// How a storage step carried to one birth ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Carried {
    /// The birth did it; the provider its writes land on.
    Stepped(String),
    /// The birth holds a newer change: it refused the command `rejected-stale-change`.
    Stale(ChangeId),
    /// The birth refused the step by name.
    Refused(String),
    /// Carried, and the call ended without the birth's answer.
    Unanswered(String),
    /// Not done here: not carried, or the birth not the one commanded. Sent again.
    NotDone(String),
}

/// One storage step on the exact birth `node_id`, on the node command op 0x22: its terminal frame, or
/// a refusal before its acceptance; `Err` names how the call ended with neither.
pub async fn command(client: &rdm_transport::NodeRpcClient, node_id: &NodeId, command: &NodeCommandRequest) -> Result<NodeCommandReply, String> {
    let target = rdm_transport::NodeTarget::ExactNode(node_id.clone());
    let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..Default::default() };
    let mut stream = match client.call_stream::<NodeCommand>(&target, command, &opts).await {
        Ok((stream, _)) => stream,
        Err((rdm_core::outcome::RpcOutcome::Reply(r), _)) => return Ok(r.into_value()),
        Err((other, _)) => return Err(format!("the call ended {}: {other:?}", other.name())),
    };
    while let Some(item) = stream.next().await {
        match item {
            rdm_transport::stream::StreamItem::Frame(_, NodeCommandReply::CommandAccepted { .. }) => stream.accepted().await,
            rdm_transport::stream::StreamItem::Frame(_, reply) => {
                stream.accepted().await;
                return Ok(reply);
            }
            rdm_transport::stream::StreamItem::Failed(e) => return Err(format!("the stream broke after the acceptance: {e:?}")),
        }
    }
    Err("the stream ended without its terminal frame".into())
}

/// The step of the switch operation whose `Failed` receipt names why a change failed before its switch.
pub const COPY_STEP: &str = "Copy";

/// A step's receipt: `step` of `operation`, ending `outcome`. A storage step commits no output: the Build
/// fact's wire carries none for it (`wire.rs` `produces`).
async fn receipt_of(builds: &dyn crate::build_state::BuildStateAdapter, build_id: &BuildId, attempt: u32, operation: String, step: &str, outcome: crate::build_state::StepOutcome) -> Result<(), crate::executor::RunEnd> {
    let r = crate::build_state::BuildStepReceipt { mesh: None, build_id: build_id.clone(), attempt, operation: operation.clone(), step: step.into(), outcome, output: None, executor: None };
    builds.append_step_receipt(&r).await.map_err(|e| crate::executor::RunEnd::Incomplete(format!("{operation}: its {step} could not be recorded: {e}")))
}

/// A step's `Complete` receipt.
async fn receipt(builds: &dyn crate::build_state::BuildStateAdapter, build_id: &BuildId, attempt: u32, operation: String) -> Result<(), crate::executor::RunEnd> {
    receipt_of(builds, build_id, attempt, operation, "Complete", crate::build_state::StepOutcome::Complete).await
}

/// What a read of the `commit` record found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// `commit` found: switched.
    Switched,
    /// No `commit` on a provider that answers: not switched.
    NotSwitched,
    /// The provider did not answer: neither; the block holds.
    NoAnswer(String),
}

impl Decision {
    fn commit_found(&self) -> Option<bool> {
        match self {
            Self::Switched => Some(true),
            Self::NotSwitched => Some(false),
            Self::NoAnswer(_) => None,
        }
    }
}

/// Read the `commit` record of `(build_id, change_id)` on the provider registered as `to`.
pub async fn read_commit(providers: &crate::storage_backend::TopologyStorageProviders, fabric_id: &crate::model::FabricId, to: &str, build_id: &BuildId, change_id: &ChangeId) -> Decision {
    #[cfg(feature = "file-backend")]
    {
        // A read that errs, answered or not, is no read that the record is absent: the block holds.
        let lines = match providers.lines(to, fabric_id).await {
            Ok(l) => l,
            Err(e) => return Decision::NoAnswer(e.to_string()),
        };
        let name = StorageCommit { build_id: build_id.clone(), change_id: change_id.clone() }.name();
        match lines.claim(rdm_node::storage_copy::ClaimKind::Commit, &name).await {
            Ok(Some(_)) => Decision::Switched,
            Ok(None) => Decision::NotSwitched,
            Err(e) => Decision::NoAnswer(e.to_string()),
        }
    }
    #[cfg(not(feature = "file-backend"))]
    {
        let _ = (providers, fabric_id, build_id, change_id);
        Decision::NoAnswer(format!("this build answers no lines of the provider `{to}`"))
    }
}

/// Insert the `commit` record of `(build_id, change_id)` on the provider registered as `to`: `true`
/// when this insert wrote it, `false` when it stood already.
async fn insert_commit(providers: &crate::storage_backend::TopologyStorageProviders, fabric_id: &crate::model::FabricId, to: &str, build_id: &BuildId, change_id: &ChangeId) -> Result<bool, String> {
    #[cfg(feature = "file-backend")]
    {
        use rdm_node::storage_copy::{Claim, ClaimKind, ClaimTaken};
        let lines = providers.lines(to, fabric_id).await.map_err(|e| e.to_string())?;
        let commit = StorageCommit { build_id: build_id.clone(), change_id: change_id.clone() };
        let claim = Claim { kind: ClaimKind::Commit, name: commit.name(), content: serde_json::to_vec(&commit).expect("a commit record serializes") };
        match lines.insert_claim(&claim).await.map_err(|e| e.to_string())? {
            ClaimTaken::Won => Ok(true),
            ClaimTaken::Held(_) => Ok(false),
        }
    }
    #[cfg(not(feature = "file-backend"))]
    {
        let _ = (providers, fabric_id, build_id, change_id);
        Err(format!("this build answers no lines of the provider `{to}`"))
    }
}

/// How a copy ended.
enum Copy {
    Copied,
    /// Answered by name: the new provider refused as not a copy of the old one, or a provider answered
    /// with a named error ([`rdm_node::storage_error::StorageError::answered`]). The change fails
    /// before its switch.
    Failed(String),
    /// A provider did not answer: the copy is resumed.
    NoAnswer(String),
}

/// The copy (step 3), at its one call site: [`rdm_node::storage_copy::copy`]. An error opening either
/// provider's lines is told apart as the copy's own are: answered, the copy fails naming the provider;
/// unanswered, it is resumed.
async fn copy_stores(providers: &crate::storage_backend::TopologyStorageProviders, fabric_id: &crate::model::FabricId, build_id: &BuildId, change: &StorageChange) -> Copy {
    #[cfg(feature = "file-backend")]
    {
        use rdm_node::storage_copy::{copy, CopyError, CopyRun};
        let unopened = |provider: &str, e: rdm_node::storage_error::StorageError| {
            let why = format!("the topology storage provider `{provider}` does not open its lines: {e}");
            if e.answered() {
                Copy::Failed(why)
            } else {
                Copy::NoAnswer(why)
            }
        };
        let (source, destination) = match (providers.lines(&change.from, fabric_id).await, providers.lines(&change.to, fabric_id).await) {
            (Ok(s), Ok(d)) => (s, d),
            (Err(e), _) => return unopened(&change.from, e),
            (_, Err(e)) => return unopened(&change.to, e),
        };
        let run = CopyRun { build_id: build_id.clone(), change_id: change.change_id.clone(), source: change.from.clone(), destination: change.to.clone() };
        match copy(&run, &*source, &*destination).await {
            Ok(_) => Copy::Copied,
            Err(CopyError::Refused(why) | CopyError::Failed(why)) => Copy::Failed(why),
            Err(CopyError::Storage(e)) => Copy::NoAnswer(e.to_string()),
        }
    }
    #[cfg(not(feature = "file-backend"))]
    {
        let _ = (providers, fabric_id, build_id);
        Copy::Failed(format!("this build copies no provider's lines: `{}` to `{}`", change.from, change.to))
    }
}

/// The switch span of one read or insert of the `commit` record.
fn switch_span(change: &StorageChange) -> tracing::Span {
    tracing::info_span!("rdm.node_admin.build.update.via-storage-switch", build_id = %change.build_id.log(), change_id = %change.change_id, provider = %change.to, commit_found = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty)
}

/// Record a read of the `commit` record on `span`.
fn record_read(span: &tracing::Span, decision: &Decision) {
    if let Some(found) = decision.commit_found() {
        span.record("commit_found", found);
    }
    match decision {
        Decision::Switched => span.record("outcome", "found"),
        Decision::NotSwitched => span.record("outcome", "not-switched"),
        Decision::NoAnswer(e) => span.record("outcome", "no-answer").record("reason", e.as_str()),
    };
}

/// A change whose switch the fabric-primary runs now: a fabric.delete accepted meanwhile ends it.
struct Running {
    change_id: ChangeId,
    interrupted: Arc<tokio::sync::Notify>,
}

/// A node-admin's storage changes: the operations of a change it runs.
pub struct StorageChanges {
    /// The admin's storage handle.
    pub handle: Arc<crate::storage_backend::TopologyStorageHandle>,
    /// The providers the admin's executable registers.
    pub providers: crate::storage_backend::TopologyStorageProviders,
    /// The fabric's minted id.
    pub fabric_id: crate::model::FabricId,
    /// The admin's node id.
    pub node_id: NodeId,
    /// Publishes this admin's current mesh topology of its mesh, once it holds its backbone.
    pub aggregate: OnceLock<Arc<dyn Fn() -> futures_util::future::BoxFuture<'static, ()> + Send + Sync>>,
    running: std::sync::Mutex<Option<Running>>,
}

/// How a mesh-primary's move of one node ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathEnd {
    /// The birth at the path moved, or the view holds none there.
    Moved,
    /// The birth would not move: the path was re-created, and the new birth moved.
    Recreated,
}

impl StorageChanges {
    /// The storage changes of the admin `node_id` writing through `handle`.
    pub fn new(handle: Arc<crate::storage_backend::TopologyStorageHandle>, providers: crate::storage_backend::TopologyStorageProviders, fabric_id: crate::model::FabricId, node_id: NodeId) -> Self {
        Self { handle, providers, fabric_id, node_id, aggregate: OnceLock::new(), running: std::sync::Mutex::new(None) }
    }

    /// Read the `commit` record until its provider answers: the provider the change decided.
    pub(crate) async fn decide(&self, change: &StorageChange) -> String {
        let mut backoff = Duration::from_millis(500);
        loop {
            let decision = read_commit(&self.providers, &self.fabric_id, &change.to, &change.build_id, &change.change_id).await;
            let span = switch_span(change);
            record_read(&span, &decision);
            span.in_scope(|| tracing::info!("the change's decision is read from its commit record"));
            match decision {
                Decision::Switched => return change.to.clone(),
                Decision::NotSwitched => return change.from.clone(),
                Decision::NoAnswer(_) => {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    fn door(status_door: Option<Arc<crate::status_rpc::StatusDoor>>) -> Result<Arc<crate::status_rpc::StatusDoor>, crate::executor::RunEnd> {
        status_door.ok_or_else(|| crate::executor::RunEnd::Incomplete("this admin holds no status door yet".into()))
    }

    /// Step 1, `mesh.block` of `mesh`, run by its mesh-primary: it waits until its in-flight commands
    /// complete, replies `blocked` (its receipt) and gossips a current mesh topology of its mesh; it issues no
    /// new command until it is unblocked.
    #[allow(clippy::too_many_arguments)]
    pub async fn block_mesh(&self, builds: &dyn crate::build_state::BuildStateAdapter, build_id: &BuildId, attempt: u32, mesh: &str, change: &StorageChange, status_door: Option<Arc<crate::status_rpc::StatusDoor>>) -> Result<(), crate::executor::RunEnd> {
        let door = Self::door(status_door)?;
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-block", build_id = %build_id.log(), change_id = %change.change_id, mesh = %mesh, node_id = %self.node_id, in_flight = tracing::field::Empty, outcome = tracing::field::Empty);
        let in_flight = door.commands.block().await;
        if let Some(aggregate) = self.aggregate.get() {
            aggregate().await;
        }
        span.record("in_flight", in_flight as u64).record("outcome", "blocked");
        span.in_scope(|| tracing::info!("mesh.block: every command in flight completed; this mesh-primary replies blocked, gossips its mesh's topology and issues no new command until it is unblocked"));
        receipt(builds, build_id, attempt, block_operation(&change.change_id, mesh)).await
    }

    /// Carry the move onto `provider` to the birth at `path` until the birth the view holds there moves
    /// (storage.md §2 step 4). A birth that refuses its move by name (a `rejected-stale-change` of this
    /// change included), or leaves it unanswered past the staleness floor (Q182), will not move: it is
    /// re-created at `path` by `recreate` on the provider this mesh-primary writes (i149 R19), and the move
    /// is carried to the new birth. No timer ends it.
    #[allow(clippy::too_many_arguments)]
    async fn move_until<F, Fut>(&self, door: &crate::status_rpc::StatusDoor, topology: &Arc<RwLock<Topology>>, build_id: &BuildId, change_id: &ChangeId, mesh: &str, path: &PathName, provider: &str, recreate: &F) -> PathEnd
    where
        F: Fn(PathName, crate::build::ExitedBirth) -> Fut,
        Fut: std::future::Future<Output = Result<(), crate::executor::RunEnd>>,
    {
        let floor = rdm_transport::topology::staleness_floor();
        let mut backoff = Duration::from_millis(250);
        // The births re-created at the path: the view may still hold one a moment after its re-create.
        let mut recreated: Vec<NodeId> = Vec::new();
        // The birth whose move went unanswered, since the send of its first unanswered move.
        let mut unanswered: Option<(NodeId, tokio::time::Instant)> = None;
        // A re-create that did not complete, and why: owed until the path holds another birth.
        let mut owed: Option<(crate::build::ExitedBirth, String)> = None;
        loop {
            let birth = topology.read().await.node(path).cloned();
            let lost = match (owed.take(), birth) {
                (Some((from, why)), b) if b.as_ref().is_none_or(|b| b.node_id == from.node_id) => Some((from, format!("its re-create did not complete: {why}"))),
                // No birth held there: nothing to move, or the re-created birth not in the view yet.
                (_, None) if recreated.is_empty() => return PathEnd::Moved,
                (_, None) => None,
                (_, Some(birth)) if recreated.contains(&birth.node_id) => None,
                (_, Some(birth)) => match birth.incarnation_id.clone() {
                    None => None,
                    Some(incarnation) => {
                        let sent = tokio::time::Instant::now();
                        let from = crate::build::ExitedBirth { node_id: birth.node_id.clone(), incarnation: incarnation.clone() };
                        match door.carry_storage_step(&birth.node_id, &incarnation, build_id.as_str(), change_id, provider).await {
                            Carried::Stepped(_) => return if recreated.is_empty() { PathEnd::Moved } else { PathEnd::Recreated },
                            // A refusal of an older change (storage.md §2): a refusal, never a move.
                            Carried::Stale(held) => Some((from, format!("rejected-stale-change: it acted on storage change {held}"))),
                            Carried::Refused(why) => Some((from, format!("refused: {why}"))),
                            Carried::Unanswered(why) => {
                                let since = match unanswered {
                                    Some((ref id, since)) if *id == birth.node_id => since,
                                    _ => sent,
                                };
                                unanswered = Some((birth.node_id.clone(), since));
                                (tokio::time::Instant::now().saturating_duration_since(since) >= floor).then(|| (from, format!("unanswered past the staleness floor: {why}")))
                            }
                            Carried::NotDone(_) => None,
                        }
                    }
                },
            };
            if let Some((from, reason)) = lost {
                let span = tracing::info_span!("rdm.node_admin.node.update.via-storage-move-lost", build_id = %build_id.log(), change_id = %change_id, mesh = %mesh, node = %path, node_id = %from.node_id, incarnation_id = %from.incarnation.as_str(), provider = %provider, reason = %reason, outcome = tracing::field::Empty, error = tracing::field::Empty);
                span.in_scope(|| tracing::info!("the node will not move: it is re-created at its path.name on the provider this mesh-primary writes, steps of this attempt"));
                use tracing::Instrument as _;
                match recreate(path.clone(), from.clone()).instrument(span.clone()).await {
                    Ok(()) => {
                        span.record("outcome", "recreated");
                        recreated.push(from.node_id);
                        continue;
                    }
                    Err(e) => {
                        let error = format!("{e:?}");
                        span.record("outcome", "not-recreated").record("error", error.as_str());
                        owed = Some((from, error));
                    }
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(2));
        }
    }

    /// Step 4, `mesh.unblock` of `mesh`, run by its mesh-primary: the provider the `commit` record decided
    /// (the old one when the change failed before its switch), this mesh-primary and each other node of
    /// the mesh moved onto it, each row recording it; a node that will not move is re-created on it by
    /// `recreate`, steps of this attempt (i149 R19); once every node moved or was re-created it replies
    /// `unblocked` (its receipt) and issues commands again.
    #[allow(clippy::too_many_arguments)]
    pub async fn unblock_mesh<F, Fut>(&self, builds: &dyn crate::build_state::BuildStateAdapter, build_id: &BuildId, attempt: u32, mesh: &str, change: &StorageChange, status_door: Option<Arc<crate::status_rpc::StatusDoor>>, topology: Arc<RwLock<Topology>>, recreate: &F) -> Result<(), crate::executor::RunEnd>
    where
        F: Fn(PathName, crate::build::ExitedBirth) -> Fut,
        Fut: std::future::Future<Output = Result<(), crate::executor::RunEnd>>,
    {
        let door = Self::door(status_door)?;
        let provider = self.decide(change).await;
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-unblock", build_id = %build_id.log(), change_id = %change.change_id, mesh = %mesh, node_id = %self.node_id, provider = %provider, moved = tracing::field::Empty, recreated = tracing::field::Empty, outcome = tracing::field::Empty);
        self.move_own(&door, build_id, &change.change_id, &provider).await?;
        // Moved: this admin writes rows again, on the decided provider (its re-creates' rows among them).
        door.commands.release_rows();
        let paths: Vec<PathName> = topology.read().await.members().filter(|n| n.mesh == mesh && n.node_id != self.node_id && n.status.is_live()).map(|n| n.name.clone()).collect();
        let ends = futures_util::future::join_all(paths.iter().map(|p| self.move_until(&door, &topology, build_id, &change.change_id, mesh, p, &provider, recreate))).await;
        let recreated = ends.iter().filter(|e| **e == PathEnd::Recreated).count();
        door.commands.unblock();
        span.record("moved", (paths.len() - recreated) as u64).record("recreated", recreated as u64).record("outcome", "unblocked");
        span.in_scope(|| tracing::info!("mesh.unblock: every node of this mesh moved or was re-created; this mesh-primary replies unblocked and issues commands again"));
        receipt(builds, build_id, attempt, unblock_operation(&change.change_id, mesh)).await
    }

    /// This admin moved onto `provider`, its own row recording it.
    async fn move_own(&self, door: &crate::status_rpc::StatusDoor, build_id: &BuildId, change_id: &ChangeId, provider: &str) -> Result<(), crate::executor::RunEnd> {
        let own = door.own.get().and_then(|own| own.storage_moves()).ok_or_else(|| crate::executor::RunEnd::Incomplete("this admin holds no storage handle yet".into()))?;
        match own.serve(build_id, change_id, provider).await {
            NodeCommandReply::StorageStepped { .. } => {}
            other => return Err(crate::executor::RunEnd::Incomplete(format!("this admin's move onto {provider}: {}", other.name()))),
        }
        door.record_own_provider(provider).await.map_err(|e| crate::executor::RunEnd::Incomplete(format!("this admin's row: {e}")))
    }

    /// Steps 2 and 3, `storage.switch:<change_id>`, run by the fabric-primary once every mesh-primary
    /// replied `blocked`: the copy, the `commit` record inserted, this admin onto the new provider and
    /// the Fabric row's line. A copy refused by name, or answered by a provider with a named error,
    /// fails the change before its switch once no `commit` is read, its receipt naming why; a copy no
    /// provider answered is resumed on a backoff, no timer ending it. A change resumed after its switch
    /// learns it from the record alone.
    /// A fabric.delete accepted meanwhile ends the operation.
    #[allow(clippy::too_many_arguments)]
    pub async fn switch(&self, builds: &dyn crate::build_state::BuildStateAdapter, build_id: &BuildId, attempt: u32, change: &StorageChange, status_door: Option<Arc<crate::status_rpc::StatusDoor>>, fabric_row: impl std::future::Future<Output = Result<(), String>>) -> Result<(), crate::executor::RunEnd> {
        let door = Self::door(status_door)?;
        let interrupted = Arc::new(tokio::sync::Notify::new());
        *self.running.lock().unwrap() = Some(Running { change_id: change.change_id.clone(), interrupted: interrupted.clone() });
        let notified = interrupted.notified();
        let ran = tokio::select! {
            r = self.switch_steps(builds, build_id, attempt, change, &door, fabric_row) => r,
            _ = notified => Err(crate::executor::RunEnd::failed(format!("storage change {} of Build {build_id} is interrupted by fabric.delete, which resolves it", change.change_id))),
        };
        let mut running = self.running.lock().unwrap();
        if running.as_ref().is_some_and(|r| r.change_id == change.change_id) {
            *running = None;
        }
        ran
    }

    async fn switch_steps(&self, builds: &dyn crate::build_state::BuildStateAdapter, build_id: &BuildId, attempt: u32, change: &StorageChange, door: &Arc<crate::status_rpc::StatusDoor>, fabric_row: impl std::future::Future<Output = Result<(), String>>) -> Result<(), crate::executor::RunEnd> {
        if let Some(seam) = &door.command_frame_seam {
            seam.before_storage_switch(&change.change_id).await;
        }
        let key = switch_operation(&change.change_id);
        if read_commit(&self.providers, &self.fabric_id, &change.to, &change.build_id, &change.change_id).await == Decision::Switched {
            let span = switch_span(change);
            record_read(&span, &Decision::Switched);
            span.in_scope(|| tracing::info!("the change is resumed and its commit record is found: it is switched"));
        } else {
            // 2. Copy.
            let mut backoff = Duration::from_millis(500);
            loop {
                match copy_stores(&self.providers, &self.fabric_id, build_id, change).await {
                    Copy::Copied => break,
                    Copy::Failed(why) => return self.fail_before_switch(builds, build_id, attempt, change, why).await,
                    Copy::NoAnswer(_) => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
            }
            // 3. Switch: the insert is the decision; a lost answer is learnt by reading the record.
            let mut backoff = Duration::from_millis(500);
            loop {
                let span = switch_span(change);
                match insert_commit(&self.providers, &self.fabric_id, &change.to, &change.build_id, &change.change_id).await {
                    Ok(true) => {
                        span.record("commit_found", false).record("outcome", "inserted");
                        span.in_scope(|| tracing::info!("the commit record is inserted on the new provider: the change is switched"));
                        break;
                    }
                    Ok(false) => {
                        span.record("commit_found", true).record("outcome", "found");
                        span.in_scope(|| tracing::info!("the commit record stood already: the change is switched"));
                        break;
                    }
                    Err(e) => {
                        span.record("outcome", "no-answer").record("reason", e.as_str());
                        drop(span);
                        let decision = read_commit(&self.providers, &self.fabric_id, &change.to, &change.build_id, &change.change_id).await;
                        if !matches!(decision, Decision::NoAnswer(_)) {
                            record_read(&switch_span(change), &decision);
                        }
                        if decision == Decision::Switched {
                            break;
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
            }
        }
        // This admin writes the new provider before the Fabric row's line and its receipt.
        self.move_own(door, build_id, &change.change_id, &change.to).await?;
        if let Err(e) = fabric_row.await {
            return Err(crate::executor::RunEnd::Incomplete(format!("{key}: the Fabric row's line naming {} could not be written: {e}", change.to)));
        }
        receipt(builds, build_id, attempt, key).await
    }

    /// A step failed before the switch: the change fails only once no `commit` is read for it (its
    /// mesh-primaries then sent `mesh.unblock` on the old provider, and the Build's verdict names why).
    /// A `commit` found means the change switched after all: it resumes.
    async fn fail_before_switch(&self, builds: &dyn crate::build_state::BuildStateAdapter, build_id: &BuildId, attempt: u32, change: &StorageChange, why: String) -> Result<(), crate::executor::RunEnd> {
        if self.decide(change).await == change.to {
            return Err(crate::executor::RunEnd::Incomplete(format!("storage change {}: {why}; its commit record is found, so it is switched and resumes", change.change_id)));
        }
        let key = switch_operation(&change.change_id);
        receipt_of(builds, build_id, attempt, key.clone(), COPY_STEP, crate::build_state::StepOutcome::Failed { reason: why, code: rdm_core::outcome::Code::FailedPrecondition }).await?;
        receipt(builds, build_id, attempt, key).await
    }

    /// The Build's verdict (`storage.change:<change_id>`), the fabric-primary's once every mesh-primary
    /// replied `unblocked`: switched, the change is done; not switched, the Build fails naming why.
    pub async fn finish(&self, builds: &dyn crate::build_state::BuildStateAdapter, build_id: &BuildId, attempt: u32, change: &StorageChange) -> Result<(), crate::executor::RunEnd> {
        if self.decide(change).await == change.to {
            return receipt(builds, build_id, attempt, operation(&change.change_id)).await;
        }
        let key = switch_operation(&change.change_id);
        let why = builds
            .read_build(build_id)
            .await
            .ok()
            .and_then(|b| b.steps.iter().rev().find_map(|r| match &r.outcome {
                crate::build_state::StepOutcome::Failed { reason, .. } if r.operation == key && r.step == COPY_STEP => Some(reason.clone()),
                _ => None,
            }))
            .unwrap_or_else(|| "its switch did not happen".into());
        Err(crate::executor::RunEnd::failed(format!("storage change {} to {} failed before its switch ({why}); no commit record exists, and every mesh-primary was sent mesh.unblock on {}", change.change_id, change.to, change.from)))
    }

    /// `fabric.delete` admitted while a storage change is in progress (storage.md §2: "it interrupts the
    /// change as it interrupts any Build"): the switch this admin runs is ended; the delete's own Build
    /// sends each mesh-primary `mesh.unblock` onto the provider the `commit` record decided before its
    /// teardown.
    pub fn interrupt_for_delete(&self) {
        if let Some(r) = self.running.lock().unwrap().take() {
            r.interrupted.notify_waiters();
            r.interrupted.notify_one();
        }
    }
}
