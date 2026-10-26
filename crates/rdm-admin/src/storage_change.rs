//! A storage change (storage.md §2 "Changing it"): the Build that moves RDM's five stores from the
//! fabric's provider to the one it names, run by the fabric-primary as one operation of the Build
//! (`storage.change:<change_id>`), its commands carried fabric-primary → mesh-primary → node:
//!
//! 1. **Block** and 2. **Finish outstanding writes**: every node blocks, records the block and both
//!    providers in its data dir, finishes the writes it issued before the block on the old provider and
//!    acknowledges; the copy waits for every node's acknowledgement or the exited verdict on it.
//! 3. **Copy**: [`rdm_node::storage_copy::copy`], resumed after a lost answer, refused by name when the
//!    new provider holds lines the old one does not.
//! 4. **Switch**: the `commit` record for `(build_id, change_id)` inserted on the new provider with the
//!    store's insert-and-fail primitive (the claim primitive, [`ClaimKind::Commit`]), then the Fabric
//!    row's line. Whoever resumes the change learns the decision only by reading that record.
//! 5. **Unblock**: every node moves to the new provider; the Build completes once every node has
//!    unblocked or been re-created.
//!
//! A step failing before the switch fails the Build only once the fabric-primary has read that no
//! `commit` exists; every node then unblocks on the old provider. No timer ends a block: a provider
//! that does not answer is read again until it does.

use crate::build::BuildId;
use crate::model::{NodeId, PathName};
use crate::storage_block::{BlockRecord, StorageBlock, StorageSteps};
use crate::topology::Topology;
use rdm_core::build::{StorageChange, StorageCommit, StorageStep};
use rdm_core::status::{StatusReply, StatusRequest};
use rdm_core::ChangeId;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;

/// The fabric-primary's switch: the `commit` record inserted, or read by whoever resumes the change.
pub const SWITCH_SPAN: &str = "rdm.node_admin.build.update.via-storage-switch";

/// The operation key of a storage change.
pub fn operation(change_id: &ChangeId) -> String {
    format!("storage.change:{change_id}")
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
        let lines = match providers.lines(to, fabric_id).await {
            Ok(l) => l,
            Err(e) => return Decision::NoAnswer(e),
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
        let lines = providers.lines(to, fabric_id).await?;
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
    Refused(String),
    NoAnswer(String),
}

/// The copy (step 3), at its one call site: [`rdm_node::storage_copy::copy`].
async fn copy_stores(providers: &crate::storage_backend::TopologyStorageProviders, fabric_id: &crate::model::FabricId, build_id: &BuildId, change: &StorageChange) -> Copy {
    #[cfg(feature = "file-backend")]
    {
        use rdm_node::storage_copy::{copy, CopyError, CopyRun};
        let (source, destination) = match (providers.lines(&change.from, fabric_id).await, providers.lines(&change.to, fabric_id).await) {
            (Ok(s), Ok(d)) => (s, d),
            (Err(e), _) | (_, Err(e)) => return Copy::NoAnswer(e),
        };
        let run = CopyRun { build_id: build_id.clone(), change_id: change.change_id.clone(), source: change.from.clone(), destination: change.to.clone() };
        match copy(&run, &*source, &*destination).await {
            Ok(_) => Copy::Copied,
            Err(CopyError::Refused(why)) => Copy::Refused(why),
            Err(CopyError::Storage(e)) => Copy::NoAnswer(e.to_string()),
        }
    }
    #[cfg(not(feature = "file-backend"))]
    {
        let _ = (providers, fabric_id, build_id);
        Copy::Refused(format!("this build copies no provider's lines: `{}` to `{}`", change.from, change.to))
    }
}

/// The switch span of one read or insert of the `commit` record.
fn switch_span(build_id: &BuildId, change: &StorageChange) -> tracing::Span {
    tracing::info_span!("rdm.node_admin.build.update.via-storage-switch", build_id = %build_id.log(), change_id = %change.change_id, provider = %change.to, commit_found = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty)
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

/// A node-admin's steps of a storage change (storage.md §2): its one storage handle, through which
/// every store it reads and writes is reached, and its connections writer's queue, where a connection
/// event stays owed while the admin is blocked (Q169).
pub struct AdminStorageSteps {
    /// The admin's storage handle.
    pub handle: Arc<crate::storage_backend::TopologyStorageHandle>,
    /// The admin's connections writer.
    pub writer: Arc<crate::connections_writer::ConnectionsWriter>,
    rows: tokio::sync::Mutex<Option<crate::connections_writer::RowsHeld>>,
}

impl AdminStorageSteps {
    /// The steps over `handle` and `writer`.
    pub fn new(handle: Arc<crate::storage_backend::TopologyStorageHandle>, writer: Arc<crate::connections_writer::ConnectionsWriter>) -> Self {
        Self { handle, writer, rows: tokio::sync::Mutex::new(None) }
    }
}

#[async_trait::async_trait]
impl StorageSteps for AdminStorageSteps {
    fn provider(&self) -> String {
        self.handle.provider()
    }

    async fn open_beside(&self, build_id: &BuildId, change_id: &ChangeId, to: &str) -> Result<(), String> {
        self.handle.open_beside(to, (build_id, change_id)).await
    }

    async fn block(&self) -> usize {
        let issued = self.handle.block();
        let mut rows = self.rows.lock().await;
        if rows.is_none() {
            *rows = Some(self.writer.hold_rows().await);
        }
        issued
    }

    async fn finish_issued(&self) {
        self.handle.finish_issued().await
    }

    fn owed(&self) -> usize {
        self.handle.owed() + self.writer.in_flight()
    }

    async fn unblock(&self, build_id: &BuildId, change_id: &ChangeId, provider: &str) -> Result<(), String> {
        self.handle.replace_for(provider, Some((build_id, change_id))).await?;
        self.handle.close_beside();
        self.handle.unblock();
        self.rows.lock().await.take();
        Ok(())
    }
}

/// What a node-admin's start makes of a block its data dir records (storage.md §2: "records the block
/// and both providers in its data dir, which a restart reads before it writes anything"): the provider
/// it opens and whether it holds the block. The `commit` record decides: found, the admin writes the
/// new provider; none on a provider that answers, the change is not switched: a launched birth writes
/// the old provider (its launcher, a mesh-primary, launches nothing while it is blocked, so its change
/// ended unswitched), as does the change's executor (the fabric-primary that ran it), whose change
/// resumes from its block; any other start holds its block until its change's unblock. A provider
/// that does not answer is neither, and the block holds. The record is written back with what was
/// decided.
pub async fn resolve_at_start(providers: &crate::storage_backend::TopologyStorageProviders, fabric_id: &crate::model::FabricId, data_dir: &std::path::Path, record: &mut BlockRecord, launched_on: &str, launched: bool) -> Result<(String, bool), String> {
    let Some(held) = record.held_block().cloned() else {
        // The provider this node moved to last, else the one its launch names.
        return Ok((record.latest.as_ref().map(|l| l.provider.clone()).unwrap_or_else(|| launched_on.to_string()), false));
    };
    let change = StorageChange { change_id: held.change_id.clone(), from: held.from.clone(), to: held.to.clone() };
    let span = switch_span(&held.build_id, &change);
    let decision = read_commit(providers, fabric_id, &held.to, &held.build_id, &held.change_id).await;
    record_read(&span, &decision);
    span.in_scope(|| tracing::info!("this admin's start read its block from its data dir and the change's commit record before writing anything"));
    let (provider, hold) = match decision {
        Decision::Switched => (held.to.clone(), false),
        // A birth a launcher launched was launched by a mesh-primary that had unblocked: none found, the
        // change ended unswitched.
        Decision::NotSwitched if held.executor || launched => (held.from.clone(), false),
        Decision::NotSwitched | Decision::NoAnswer(_) => return Ok((held.from.clone(), true)),
    };
    let unblock = tracing::info_span!("rdm.node.build.update.via-storage-unblock", build_id = %held.build_id.log(), change_id = %held.change_id, node_id = tracing::field::Empty, provider = %provider, owed_writes = 0u64, read_from = "data-dir", outcome = if provider == held.to { "unblocked" } else { "not-switched" });
    record.latest = Some(crate::storage_block::HeldChange { blocked: false, provider: provider.clone(), ..held });
    record.write(data_dir).await.map_err(|e| format!("refusing to start: the storage change record in {}: {e}", data_dir.display()))?;
    unblock.in_scope(|| tracing::info!("the start decided its block from the commit record"));
    Ok((provider, hold))
}

/// A change the fabric-primary runs now.
struct Running {
    build_id: BuildId,
    change: StorageChange,
    interrupted: Arc<tokio::sync::Notify>,
}

/// A node-admin's storage changes: its own block, and, while it is the fabric-primary, the change it
/// runs.
pub struct StorageChanges {
    /// The admin's storage handle.
    pub handle: Arc<crate::storage_backend::TopologyStorageHandle>,
    /// The providers the admin's executable registers.
    pub providers: crate::storage_backend::TopologyStorageProviders,
    /// The fabric's minted id.
    pub fabric_id: crate::model::FabricId,
    /// The admin's node id.
    pub node_id: NodeId,
    /// The admin's storage change commands, once it holds them.
    pub own: OnceLock<Arc<StorageBlock>>,
    running: std::sync::Mutex<Option<Running>>,
}

/// How one node's step ended.
enum StepEnd {
    Done,
    Refused(String),
}

impl StorageChanges {
    /// The storage changes of the admin `node_id` writing through `handle`.
    pub fn new(handle: Arc<crate::storage_backend::TopologyStorageHandle>, providers: crate::storage_backend::TopologyStorageProviders, fabric_id: crate::model::FabricId, node_id: NodeId) -> Self {
        Self { handle, providers, fabric_id, node_id, own: OnceLock::new(), running: std::sync::Mutex::new(None) }
    }

    /// The change this admin runs now, if any: `(build_id, change)`.
    pub fn running(&self) -> Option<(BuildId, StorageChange)> {
        self.running.lock().unwrap().as_ref().map(|r| (r.build_id.clone(), r.change.clone()))
    }

    /// `fabric.delete` admitted during a change (storage.md §2: "it interrupts the change as it
    /// interrupts any Build"; i149 R12: it resolves the change before any ordinary write): the change
    /// this admin runs is ended, its `commit` record read, and this admin unblocks onto the provider it
    /// decided, so the delete's acceptance and its teardown are written; every other node is sent its
    /// unblock onto that provider, owed until it answers. A provider that does not answer is read again:
    /// the block holds until it does. Nothing when no change is blocked here.
    pub async fn resolve_for_delete(self: &Arc<Self>, client: Option<Arc<rdm_transport::NodeRpcClient>>, authority: Option<Arc<crate::status_rpc::StatusAuthority>>, topology: Arc<RwLock<Topology>>) {
        let held = match self.own.get() {
            Some(own) => own.record().await.held_block().cloned(),
            None => None,
        };
        let running = self.running.lock().unwrap().take();
        if let Some(r) = &running {
            r.interrupted.notify_waiters();
            r.interrupted.notify_one();
        }
        let Some(held) = held else { return };
        let change = StorageChange { change_id: held.change_id.clone(), from: held.from.clone(), to: held.to.clone() };
        let provider = self.decide(&held.build_id, &change).await;
        let me = self.node_id.clone();
        // This admin first: the delete's own writes follow its unblock.
        if let Some(own) = self.own.get() {
            let _ = own.serve(&held.build_id, &held.change_id, &StorageStep::Unblock { provider: provider.clone() }).await;
        }
        let this = self.clone();
        let step = StorageStep::Unblock { provider };
        tokio::spawn(async move {
            let paths: Vec<PathName> = topology.read().await.members().filter(|n| n.node_id != me && n.status.is_live()).map(|n| n.name.clone()).collect();
            let all = paths.iter().map(|p| this.step_until(client.clone(), authority.clone(), topology.clone(), &held.build_id, &held.change_id, p, &step, None));
            futures_util::future::join_all(all).await;
        });
    }

    /// While this admin holds the fabric-primary seat, a block it holds of a change it does not run is
    /// the block of a change it resumes (storage.md §2 step 4: "Whoever resumes the change, a successor
    /// or the fabric-primary after a lost answer, learns the decision only by reading that record"): it
    /// reads the `commit` record and moves onto the provider it decided, so it can claim and run the
    /// change's next attempt; a change not switched resumes from its block, which blocks this admin
    /// again before its copy. A provider that does not answer is read again: the block holds.
    pub fn resume_when_seated(self: &Arc<Self>, topology: Arc<RwLock<Topology>>) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                let Some(own) = this.own.get().cloned() else { continue };
                if this.running().is_some() || !topology.read().await.fabric_primary().is_some_and(|fp| fp.node_id == this.node_id) {
                    continue;
                }
                let Some(held) = own.record().await.held_block().cloned() else { continue };
                let change = StorageChange { change_id: held.change_id.clone(), from: held.from.clone(), to: held.to.clone() };
                let provider = this.decide(&held.build_id, &change).await;
                if this.running().is_none() {
                    let _ = own.serve(&held.build_id, &held.change_id, &StorageStep::Unblock { provider }).await;
                }
            }
        });
    }

    /// Read the `commit` record until its provider answers: the provider the change decided.
    async fn decide(&self, build_id: &BuildId, change: &StorageChange) -> String {
        let mut backoff = Duration::from_millis(500);
        loop {
            let span = switch_span(build_id, change);
            let decision = read_commit(&self.providers, &self.fabric_id, &change.to, build_id, &change.change_id).await;
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

    /// Run the change `change` of Build `build_id` (attempt `attempt`), this admin the fabric-primary
    /// `me`: the five steps of storage.md §2. `fabric_row` appends the Fabric row's line once the
    /// change is switched. `Err` names why the Build fails (a step that failed before the switch, once
    /// no `commit` is read) or that a fabric.delete interrupted it.
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        self: &Arc<Self>,
        builds: &dyn crate::build_state::BuildStateAdapter,
        build_id: &BuildId,
        attempt: u32,
        change: &StorageChange,
        client: Option<Arc<rdm_transport::NodeRpcClient>>,
        authority: Option<Arc<crate::status_rpc::StatusAuthority>>,
        topology: Arc<RwLock<Topology>>,
        fabric_row: impl std::future::Future<Output = Result<(), String>>,
    ) -> Result<(), crate::executor::RunEnd> {
        let interrupted = Arc::new(tokio::sync::Notify::new());
        *self.running.lock().unwrap() = Some(Running { build_id: build_id.clone(), change: change.clone(), interrupted: interrupted.clone() });
        let notified = interrupted.notified();
        let ran = tokio::select! {
            r = self.run_steps(builds, build_id, attempt, change, client, authority, topology, fabric_row) => r,
            _ = notified => Err(crate::executor::RunEnd::failed(format!("storage change {} of Build {build_id} is interrupted by fabric.delete, which resolves it", change.change_id))),
        };
        let mut running = self.running.lock().unwrap();
        if running.as_ref().is_some_and(|r| r.change.change_id == change.change_id) {
            *running = None;
        }
        ran
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_steps(
        self: &Arc<Self>,
        builds: &dyn crate::build_state::BuildStateAdapter,
        build_id: &BuildId,
        attempt: u32,
        change: &StorageChange,
        client: Option<Arc<rdm_transport::NodeRpcClient>>,
        authority: Option<Arc<crate::status_rpc::StatusAuthority>>,
        topology: Arc<RwLock<Topology>>,
        fabric_row: impl std::future::Future<Output = Result<(), String>>,
    ) -> Result<(), crate::executor::RunEnd> {
        let Some(own) = self.own.get().cloned() else {
            return Err(crate::executor::RunEnd::Incomplete(format!("storage change {}: this admin holds no storage handle yet", change.change_id)));
        };
        // A change resumed after its switch (a lost answer, a successor) learns it from the record alone.
        let resumed = {
            let decision = read_commit(&self.providers, &self.fabric_id, &change.to, build_id, &change.change_id).await;
            if decision == Decision::Switched {
                let span = switch_span(build_id, change);
                record_read(&span, &decision);
                span.in_scope(|| tracing::info!("the change is resumed and its commit record is found: it is switched"));
            }
            decision == Decision::Switched
        };
        if !resumed {
            // 1-2. Block, and finish the writes issued before it. This admin first.
            let me_block = own.block_executor(build_id, &change.change_id, &change.from, &change.to).await;
            if let rdm_core::node_command::NodeCommandReply::Failed { reason, .. } | rdm_core::node_command::NodeCommandReply::NotReady { reason } = me_block {
                return self.fail_before_switch(&own, build_id, change, client, authority, topology, format!("this fabric-primary's block failed: {reason}")).await;
            }
            let step = StorageStep::Block { from: change.from.clone(), to: change.to.clone() };
            let paths = self.paths(&topology).await;
            let all = paths.iter().map(|p| self.step_until(client.clone(), authority.clone(), topology.clone(), build_id, &change.change_id, p, &step, Some(())));
            let ends = futures_util::future::join_all(all).await;
            if let Some((path, why)) = paths.iter().zip(ends).find_map(|(p, e)| match e {
                StepEnd::Refused(why) => Some((p.clone(), why)),
                StepEnd::Done => None,
            }) {
                return self.fail_before_switch(&own, build_id, change, client, authority, topology, format!("{path} failed its block: {why}")).await;
            }
            // 3. Copy.
            let mut backoff = Duration::from_millis(500);
            loop {
                match copy_stores(&self.providers, &self.fabric_id, build_id, change).await {
                    Copy::Copied => break,
                    Copy::Refused(why) => return self.fail_before_switch(&own, build_id, change, client, authority, topology, why).await,
                    Copy::NoAnswer(_) => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
            }
            // 4. Switch: the insert is the decision; a lost answer is learnt by reading the record.
            let mut backoff = Duration::from_millis(500);
            loop {
                let span = switch_span(build_id, change);
                match insert_commit(&self.providers, &self.fabric_id, &change.to, build_id, &change.change_id).await {
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
                        let decision = read_commit(&self.providers, &self.fabric_id, &change.to, build_id, &change.change_id).await;
                        let read = switch_span(build_id, change);
                        record_read(&read, &decision);
                        if decision == Decision::Switched {
                            break;
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
            }
        }
        // 5. Unblock: this admin first, then the Fabric row's line, then every other node.
        let me_step = StorageStep::Unblock { provider: change.to.clone() };
        loop {
            match self.own_step(&own, authority.as_ref(), build_id, &change.change_id, &me_step).await {
                StepEnd::Done => break,
                StepEnd::Refused(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
        if let Err(e) = fabric_row.await {
            // The row's line is owed: the next attempt reads the commit record and writes it.
            return Err(crate::executor::RunEnd::Incomplete(format!("storage change {}: the Fabric row's line naming {} could not be written: {e}", change.change_id, change.to)));
        }
        let paths = self.paths(&topology).await;
        let all = paths.iter().map(|p| self.step_until(client.clone(), authority.clone(), topology.clone(), build_id, &change.change_id, p, &me_step, None));
        futures_util::future::join_all(all).await;
        let receipt = crate::build_state::BuildStepReceipt { mesh: None, build_id: build_id.clone(), attempt, operation: operation(&change.change_id), step: "Complete".into(), outcome: crate::build_state::StepOutcome::Complete, output: None, executor: None };
        builds.append_step_receipt(&receipt).await.map_err(|e| crate::executor::RunEnd::Incomplete(format!("storage change {}: its Complete could not be recorded: {e}", change.change_id)))?;
        Ok(())
    }

    /// A step failed before the switch: the Build fails only once no `commit` is read for the change,
    /// and every node unblocks on the old provider (storage.md §2). A `commit` found means the change
    /// switched after all: it resumes from its unblock.
    #[allow(clippy::too_many_arguments)]
    async fn fail_before_switch(&self, own: &Arc<StorageBlock>, build_id: &BuildId, change: &StorageChange, client: Option<Arc<rdm_transport::NodeRpcClient>>, authority: Option<Arc<crate::status_rpc::StatusAuthority>>, topology: Arc<RwLock<Topology>>, why: String) -> Result<(), crate::executor::RunEnd> {
        let provider = self.decide(build_id, change).await;
        let step = StorageStep::Unblock { provider: provider.clone() };
        loop {
            match self.own_step(own, authority.as_ref(), build_id, &change.change_id, &step).await {
                StepEnd::Done => break,
                StepEnd::Refused(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
        let paths = self.paths(&topology).await;
        let all = paths.iter().map(|p| self.step_until(client.clone(), authority.clone(), topology.clone(), build_id, &change.change_id, p, &step, None));
        futures_util::future::join_all(all).await;
        if provider == change.to {
            return Err(crate::executor::RunEnd::Incomplete(format!("storage change {}: {why}; its commit record is found, so it is switched and resumes", change.change_id)));
        }
        Err(crate::executor::RunEnd::failed(format!("storage change {} to {} failed before its switch ({why}); no commit record exists, and every node unblocked on {}", change.change_id, change.to, change.from)))
    }

    /// This admin's own step: in-process, its row recording the provider at its unblock.
    async fn own_step(&self, own: &Arc<StorageBlock>, authority: Option<&Arc<crate::status_rpc::StatusAuthority>>, build_id: &BuildId, change_id: &ChangeId, step: &StorageStep) -> StepEnd {
        let reply = match own.serve(build_id, change_id, step).await {
            rdm_core::node_command::NodeCommandReply::StorageStepped { provider } => provider,
            other => return StepEnd::Refused(format!("{other:?}")),
        };
        if let (StorageStep::Unblock { .. }, Some(authority)) = (step, authority) {
            let id = self.node_id.clone();
            if let Err(e) = authority.caches.nodes.update(&*authority.nodes_storage, &id, None, move |r| r.topology_storage_provider = Some(reply)).await {
                return StepEnd::Refused(e);
            }
        }
        StepEnd::Done
    }

    /// Every path whose birth is to take the change's steps: the members of the view other than this
    /// admin, live.
    async fn paths(&self, topology: &Arc<RwLock<Topology>>) -> Vec<PathName> {
        topology.read().await.members().filter(|n| n.node_id != self.node_id && n.status.is_live()).map(|n| n.name.clone()).collect()
    }

    /// Send `step` to the birth at `path` through its mesh-primary until it is done: answered by the
    /// birth the view holds at `path` when each send leaves (a re-created one included: an exited
    /// birth's successor), or no birth held there. A birth that refuses a block by name (`block` set)
    /// ends it refused; silence is retried, never read as either.
    #[allow(clippy::too_many_arguments)]
    async fn step_until(&self, client: Option<Arc<rdm_transport::NodeRpcClient>>, authority: Option<Arc<crate::status_rpc::StatusAuthority>>, topology: Arc<RwLock<Topology>>, build_id: &BuildId, change_id: &ChangeId, path: &PathName, step: &StorageStep, block: Option<()>) -> StepEnd {
        let mut backoff = Duration::from_millis(250);
        loop {
            let view = topology.read().await.clone();
            let Some(birth) = view.node(path).cloned() else { return StepEnd::Done };
            if birth.node_id == self.node_id {
                return StepEnd::Done;
            }
            let Some(incarnation) = birth.incarnation_id.clone() else {
                tokio::time::sleep(backoff).await;
                continue;
            };
            let req = StatusRequest::StorageStep { node_id: birth.node_id.clone(), incarnation, build_id: build_id.to_string(), change_id: change_id.clone(), step: step.clone() };
            let mp = view.node_type_primary(&path.mesh, rdm_core::NodeKind::NodeAdmin).cloned();
            let reply = match (mp, &authority, &client) {
                (Some(mp), Some(authority), _) if mp.node_id == self.node_id => {
                    let StatusRequest::StorageStep { node_id, incarnation, build_id, change_id, step } = &req else { unreachable!() };
                    Some(authority.carry_storage_step(node_id, incarnation, build_id, change_id, step).await)
                }
                (Some(mp), _, Some(client)) => {
                    let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(45) }, ..Default::default() };
                    match client.call::<rdm_core::status::Status>(&rdm_transport::NodeTarget::ExactNode(mp.node_id.clone()), &req, &opts).await.0 {
                        rdm_core::outcome::RpcOutcome::Reply(r) => Some(r.into_value()),
                        _ => None,
                    }
                }
                _ => None,
            };
            match reply {
                Some(StatusReply::StorageStepped { .. }) => return StepEnd::Done,
                Some(StatusReply::StorageStepFailed { reason, .. }) if block.is_some() => return StepEnd::Refused(reason),
                Some(StatusReply::RejectedStaleChange { held }) => return StepEnd::Refused(format!("rejected-stale-change: {path} holds storage change {held}")),
                _ => {}
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    }
}
