//! A node's side of a storage change (storage.md §2 "Changing it"): the block, recorded in its data
//! dir, and the unblock, each a command of the change carried fabric-primary → mesh-primary → node.
//!
//! **Block** (steps 1 and 2): the node stops admitting writes and claim operations to its stores, every
//! write it would make held as owed, never refused; it opens the new provider beside the old one,
//! records the block and both providers in its data dir, which a restart reads before it writes
//! anything, then brings every write it issued before the block to a durable outcome on the old
//! provider and acknowledges. A provider its executable does not register fails the step by name.
//!
//! **Unblock** (step 5): the node moves to the provider its change decided, its owed writes landing
//! there in its own order, and closes the other.
//!
//! The change carries its `change_id`, and a node refuses by name (`rejected-stale-change`) a command
//! of a change older than the one it holds: a change it acted on before its latest one.
//!
//! What a node kind does to block, finish and unblock is its own ([`StorageSteps`]): an rpc node holds
//! its connections writer's queue and its storage handle, a node-admin its handle and every writer on
//! it. The record and the order of the steps are this module's, one for every kind.

use crate::storage_error::StorageError;
use async_trait::async_trait;
use rdm_core::build::StorageStep;
use rdm_core::node_command::NodeCommandReply;
use rdm_core::{BuildId, ChangeId, NodeId};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The file in a node's data dir that records the storage changes it acted on: the block it holds, if
/// any, and both providers.
pub const BLOCK_FILE: &str = "topology-storage-block.json";

/// A node's block span (storage.md §2 steps 1 and 2): it closes on the node's acknowledgement.
pub const BLOCK_SPAN: &str = "rdm.node.build.update.via-storage-block";
/// A node's unblock span (storage.md §2 step 5).
pub const UNBLOCK_SPAN: &str = "rdm.node.build.update.via-storage-unblock";

/// The latest storage change a node acted on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldChange {
    /// The change's Build.
    pub build_id: BuildId,
    /// The change.
    pub change_id: ChangeId,
    /// The old provider.
    pub from: String,
    /// The new provider.
    pub to: String,
    /// Whether the node holds the block: its writes are owed.
    pub blocked: bool,
    /// The provider the node writes: `from` while blocked, the provider it unblocked onto after.
    pub provider: String,
    /// Whether this node runs the change (the fabric-primary): a restart of it that reads no `commit`
    /// lifts its block on `from`, and the change it runs resumes from its block (storage.md §2 step 4).
    #[serde(default)]
    pub executor: bool,
}

/// What a node's data dir records of the storage changes it acted on.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRecord {
    /// The latest change: the one the node holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest: Option<HeldChange>,
    /// Every change the node acted on, the latest included: a command of any other of them is a
    /// command of an older change.
    #[serde(default)]
    pub seen: Vec<ChangeId>,
}

impl BlockRecord {
    /// The record in `data_dir`, the empty one when there is none. A record that does not parse is
    /// refused by name: a node never starts as if it held no block.
    pub fn read(data_dir: &Path) -> Result<Self, StorageError> {
        let path = data_dir.join(BLOCK_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| StorageError::Unrecognised { file: path.display().to_string(), reason: format!("the storage change record does not parse: {e}") }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        }
    }

    /// Write this record to `data_dir`, durably, before the step it records is acknowledged.
    pub async fn write(&self, data_dir: &Path) -> Result<(), StorageError> {
        let (dir, bytes) = (data_dir.to_path_buf(), serde_json::to_vec(self).expect("a storage change record serializes"));
        crate::blocking::blocking_io(move || {
            let (path, tmp) = (dir.join(BLOCK_FILE), dir.join(format!(".{BLOCK_FILE}.tmp")));
            crate::durable_file::write_file(&dir, &tmp, &path, &bytes)
        })
        .await
    }

    /// The block this record holds, if any.
    pub fn held_block(&self) -> Option<&HeldChange> {
        self.latest.as_ref().filter(|l| l.blocked)
    }
}

/// What a node kind does for each step of a storage change.
#[async_trait]
pub trait StorageSteps: Send + Sync {
    /// The provider the node writes now.
    fn provider(&self) -> String;
    /// Open `to` beside the provider the node writes, for the change: refused by name when the
    /// node's executable does not register it or it does not open; nothing is blocked then.
    async fn open_beside(&self, build_id: &BuildId, change_id: &ChangeId, to: &str) -> Result<(), String>;
    /// Stop admitting writes and claim operations: each the node would make waits as owed. Answers how
    /// many writes it issued before the block that are not answered yet.
    async fn block(&self) -> usize;
    /// Resolves once every write and claim operation issued before the block has a durable outcome on
    /// the provider the node writes.
    async fn finish_issued(&self);
    /// How many writes wait on the block now.
    fn owed(&self) -> usize;
    /// Move to `provider` (none when it writes it already), land the owed writes there in order and
    /// close the other provider. A failure keeps the writes owed.
    async fn unblock(&self, build_id: &BuildId, change_id: &ChangeId, provider: &str) -> Result<(), String>;
}

/// A node's storage change commands: its data dir's record and the steps of its kind, one step at a
/// time.
pub struct StorageBlock {
    data_dir: PathBuf,
    node_id: NodeId,
    steps: Arc<dyn StorageSteps>,
    record: tokio::sync::Mutex<BlockRecord>,
}

/// How a command of a storage change ended at the node.
fn stale(held: &ChangeId) -> NodeCommandReply {
    NodeCommandReply::RejectedStaleChange { held: held.clone() }
}

fn failed(step: &StorageStep, reason: String) -> NodeCommandReply {
    NodeCommandReply::Failed { step: format!("storage-{}", step.name()), reason, code: rdm_core::outcome::Code::FailedPrecondition }
}

impl StorageBlock {
    /// The storage change commands of the node `node_id` whose data dir is `data_dir`, over `record`
    /// (read at its start, [`BlockRecord::read`]).
    pub fn new(data_dir: PathBuf, node_id: NodeId, steps: Arc<dyn StorageSteps>, record: BlockRecord) -> Self {
        Self { data_dir, node_id, steps, record: tokio::sync::Mutex::new(record) }
    }

    /// The record as it stands.
    pub async fn record(&self) -> BlockRecord {
        self.record.lock().await.clone()
    }

    /// A start that read a block from its data dir holds it before it writes anything: the node is
    /// blocked from here until its change's unblock, its block span naming the change it read.
    pub async fn hold_at_start(&self) {
        let held = self.record.lock().await.held_block().cloned();
        let Some(held) = held else { return };
        let span = tracing::info_span!(BLOCK_SPAN, build_id = %held.build_id.log(), change_id = %held.change_id, node_id = %self.node_id, issued_writes = 0u64, owed_writes = 0u64, from = %held.from, to = %held.to, read_from = "data-dir", outcome = tracing::field::Empty);
        let _ = self.steps.block().await;
        span.record("outcome", "blocked");
        span.in_scope(|| tracing::info!("this birth read the block of its storage change from its data dir before writing anything: it holds it until its change's unblock"));
    }

    /// The refusal a command of `change_id` meets before the node accepts it: a change older than the
    /// one the node holds.
    pub async fn refusal(&self, change_id: &ChangeId) -> Option<NodeCommandReply> {
        Self::older(&*self.record.lock().await, change_id).map(|held| stale(&held))
    }

    /// Serve one step of the change `change_id` of Build `build_id`.
    pub async fn serve(&self, build_id: &BuildId, change_id: &ChangeId, step: &StorageStep) -> NodeCommandReply {
        let mut record = self.record.lock().await;
        match step {
            StorageStep::Block { from, to } => self.block(&mut record, build_id, change_id, from, to, false).await,
            StorageStep::Unblock { provider } => self.unblock(&mut record, build_id, change_id, provider).await,
        }
    }

    /// The fabric-primary's own block of the change it runs: as [`Self::serve`]'s block, recorded as
    /// the change's executor.
    pub async fn block_executor(&self, build_id: &BuildId, change_id: &ChangeId, from: &str, to: &str) -> NodeCommandReply {
        let mut record = self.record.lock().await;
        self.block(&mut record, build_id, change_id, from, to, true).await
    }

    /// Whether `change_id` is older than the change the node holds: one it acted on before its latest.
    fn older(record: &BlockRecord, change_id: &ChangeId) -> Option<ChangeId> {
        let latest = record.latest.as_ref()?;
        (latest.change_id != *change_id && record.seen.contains(change_id)).then(|| latest.change_id.clone())
    }

    async fn block(&self, record: &mut BlockRecord, build_id: &BuildId, change_id: &ChangeId, from: &str, to: &str, executor: bool) -> NodeCommandReply {
        let span = tracing::info_span!(BLOCK_SPAN, build_id = %build_id.log(), change_id = %change_id, node_id = %self.node_id, issued_writes = tracing::field::Empty, owed_writes = tracing::field::Empty, from = %from, to = %to, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        let step = StorageStep::Block { from: from.to_string(), to: to.to_string() };
        if let Some(held) = Self::older(record, change_id) {
            span.record("outcome", "rejected-stale-change").record("reason", format!("this node holds change {held}").as_str());
            span.in_scope(|| tracing::info!("a block of an older storage change is refused by name"));
            return stale(&held);
        }
        match &record.latest {
            // A change this node switched under answers its block as done: it writes the new provider.
            Some(l) if l.change_id == *change_id && !l.blocked && l.provider != l.from => {
                span.record("issued_writes", 0u64).record("owed_writes", 0u64).record("outcome", "switched");
                return NodeCommandReply::StorageStepped { provider: l.provider.clone() };
            }
            Some(l) if l.change_id != *change_id && l.blocked => {
                let reason = format!("this node is blocked under storage change {} of Build {}: a block of another change waits for its unblock", l.change_id, l.build_id);
                span.record("outcome", "refused").record("reason", reason.as_str());
                return failed(&step, reason);
            }
            _ => {}
        }
        if let Err(e) = self.steps.open_beside(build_id, change_id, to).await {
            span.record("outcome", "provider-refused").record("reason", e.as_str());
            span.in_scope(|| tracing::info!("the new provider is refused by name: nothing is blocked"));
            return failed(&step, e);
        }
        let issued = self.steps.block().await;
        let mut next = record.clone();
        next.latest = Some(HeldChange { build_id: build_id.clone(), change_id: change_id.clone(), from: from.to_string(), to: to.to_string(), blocked: true, provider: from.to_string(), executor: executor || record.latest.as_ref().is_some_and(|l| l.change_id == *change_id && l.executor) });
        if !next.seen.contains(change_id) {
            next.seen.push(change_id.clone());
        }
        if let Err(e) = next.write(&self.data_dir).await {
            // Blocked and not recorded: the block holds, and the step is not acknowledged.
            let reason = format!("the block could not be recorded in the data dir {}: {e}", self.data_dir.display());
            span.record("issued_writes", issued as u64).record("outcome", "unrecorded").record("reason", reason.as_str());
            return failed(&step, reason);
        }
        *record = next;
        self.steps.finish_issued().await;
        span.record("issued_writes", issued as u64).record("owed_writes", self.steps.owed() as u64).record("outcome", "blocked");
        span.in_scope(|| tracing::info!("blocked: no write or claim is admitted, every write is owed, the block and both providers are recorded in the data dir, and every write issued before the block is finished"));
        NodeCommandReply::StorageStepped { provider: from.to_string() }
    }

    async fn unblock(&self, record: &mut BlockRecord, build_id: &BuildId, change_id: &ChangeId, provider: &str) -> NodeCommandReply {
        let span = tracing::info_span!(UNBLOCK_SPAN, build_id = %build_id.log(), change_id = %change_id, node_id = %self.node_id, provider = %provider, owed_writes = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        let step = StorageStep::Unblock { provider: provider.to_string() };
        let Some(latest) = record.latest.clone() else {
            // A birth that holds no change writes the provider it was launched on: one launched on the
            // change's provider has nothing to unblock.
            if self.steps.provider() == provider {
                span.record("owed_writes", 0u64).record("outcome", "not-blocked");
                return NodeCommandReply::StorageStepped { provider: provider.to_string() };
            }
            let reason = format!("this node holds no storage change and writes {}, not {provider}", self.steps.provider());
            span.record("outcome", "refused").record("reason", reason.as_str());
            return failed(&step, reason);
        };
        if latest.change_id != *change_id {
            span.record("outcome", "rejected-stale-change").record("reason", format!("this node holds change {}", latest.change_id).as_str());
            span.in_scope(|| tracing::info!("an unblock of a storage change the node does not hold is refused by name"));
            return stale(&latest.change_id);
        }
        if !latest.blocked && latest.provider == provider {
            span.record("owed_writes", 0u64).record("outcome", "not-blocked");
            return NodeCommandReply::StorageStepped { provider: provider.to_string() };
        }
        let owed = self.steps.owed();
        if let Err(e) = self.steps.unblock(build_id, change_id, provider).await {
            span.record("owed_writes", owed as u64).record("outcome", "failed").record("reason", e.as_str());
            span.in_scope(|| tracing::info!("the unblock failed: the node's writes stay owed and the unblock is retried"));
            return failed(&step, e);
        }
        let mut next = record.clone();
        next.latest = Some(HeldChange { blocked: false, provider: provider.to_string(), ..latest });
        if let Err(e) = next.write(&self.data_dir).await {
            // Unblocked and not recorded: a restart holds the block it reads and the change's next
            // unblock command moves it again.
            span.record("reason", format!("unrecorded in the data dir: {e}").as_str());
        }
        *record = next;
        span.record("owed_writes", owed as u64).record("outcome", "unblocked");
        span.in_scope(|| tracing::info!("unblocked: the node writes the provider its change decided, its owed writes land there in order"));
        NodeCommandReply::StorageStepped { provider: provider.to_string() }
    }
}
