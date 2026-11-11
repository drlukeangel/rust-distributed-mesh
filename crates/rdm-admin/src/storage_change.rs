//! A storage change (storage.md §2 "Changing it"; Q342): the fabric-primary's own workflow, never a
//! Build, that moves RDM's stores from the fabric's provider to the one `fabric.storage.update` names.
//! One change runs at a time and never beside a Build: each refuses the other `build-in-progress`.
//! Its record is its three events, `rdm.storage.started` on the old provider's events store before its
//! first command, `rdm.storage.committed` on the new one's at the switch, and `rdm.storage.ended` on the
//! one it decided at its end; whoever resumes it learns where it stands only from them.
//!
//! 1. **Block.** The fabric-primary sends `mesh.block` (node command op 0x22 `BlockMesh`) to every
//!    mesh-primary, its own mesh first. Each waits until its in-flight commands complete, its node
//!    recovery of any node of its mesh has finished and every node of its mesh froze its writes, replies
//!    `blocked` and gossips a current mesh topology of its mesh; from there it issues no new command
//!    until it is unblocked.
//! 2. **Copy** and 3. **Switch**, the fabric-primary's own, once every mesh-primary replied `blocked`:
//!    the copy ([`rdm_node::storage_copy::copy`]), then the change's `rdm.storage.committed` event
//!    appended to the new provider's events store, then the Fabric row's line.
//! 4. **Unblock.** The fabric-primary sends `mesh.unblock` (`UnblockMesh`) to every mesh-primary. Each
//!    moves itself and its nodes to the provider the `rdm.storage.committed` event decided (each node's
//!    storage handle replaced, node command op 0x22 `StorageStep`), replies `unblocked` and issues
//!    commands again. A node that will not move (it refuses its move by name, a `rejected-stale-change`
//!    included, or leaves its call unanswered with its `Send` spent: Q344) is re-created on the decided
//!    provider by its mesh-primary's node recovery, never part of the change.
//!
//! A change that fails before its switch fails only once no `rdm.storage.committed` event is read, its
//! mesh-primaries sent `mesh.unblock` on the old provider. A fabric.delete ends the change: its
//! mesh-primaries are sent `mesh.unblock` onto the provider the change decided and it ends `failed`.
//! A provider that does not answer is read again until it does. No timer ends a block.

use crate::model::{Node, NodeId, PathName};
use crate::status_rpc::StatusService;
use crate::topology::Topology;
use futures_util::future::BoxFuture;
use rdm_core::build::StorageChange;
use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
use rdm_core::status::StatusReply;
use rdm_core::ChangeId;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;

/// A mesh-primary's `mesh.block` span.
pub const BLOCK_SPAN: &str = "rdm.node_admin.mesh.update.via-block";
/// A mesh-primary's `mesh.unblock` span.
pub const UNBLOCK_SPAN: &str = "rdm.node_admin.mesh.update.via-unblock";
/// The fabric-primary's switch: the `rdm.storage.committed` event appended, or read by whoever resumes
/// the change.
pub const SWITCH_SPAN: &str = "rdm.node_admin.storage.update.via-switch";
/// The change's start: its `rdm.storage.started` event written, or found by a fabric-primary that resumes it.
pub const STARTED_SPAN: &str = "rdm.node_admin.storage.update.via-started";
/// The change's end: its `rdm.storage.ended` event written.
pub const ENDED_SPAN: &str = "rdm.node_admin.storage.update.via-ended";
/// The fabric-primary's command of one mesh's block or unblock, sent to its mesh-primary.
pub const MESH_COMMAND_SPAN: &str = "rdm.node_admin.storage.update.via-mesh-command";
/// The reason a fabric.delete ends a change with (storage.md §2: "it interrupts the change").
pub const INTERRUPTED: &str = "interrupted by fabric.delete";

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

/// One command of a storage change on the exact birth `node_id`, on the node command op 0x22: its
/// terminal frame, or a refusal before its acceptance; `Err` names how the call ended with neither.
/// The command is tracked while it is open: a storage command is no Build's, so its key names none.
pub async fn command(client: &rdm_transport::NodeRpcClient, commands: &crate::node_commands::CommandView, node_id: &NodeId, command: &NodeCommandRequest) -> Result<NodeCommandReply, String> {
    let change_id = match command {
        NodeCommandRequest::StorageBlock { change_id, .. } | NodeCommandRequest::StorageStep { change_id, .. } | NodeCommandRequest::BlockMesh { change_id, .. } | NodeCommandRequest::UnblockMesh { change_id, .. } => change_id,
        _ => return Err("the storage command function accepts only storage commands".into()),
    };
    let _tracked = commands.track(crate::node_commands::CommandKey {
        operation: rdm_core::OperationId::new(command.op(), rdm_core::OperationSubject::Named(change_id.to_string())),
        node_id: node_id.clone(), incarnation: command.subject().1.clone(),
    });
    command_reply(client, node_id, command).await
}

async fn command_reply(client: &rdm_transport::NodeRpcClient, node_id: &NodeId, command: &NodeCommandRequest) -> Result<NodeCommandReply, String> {
    let target = rdm_transport::NodeTarget::ExactNode(node_id.clone());
    let opts = rdm_transport::CallOptions::default();
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

/// Wait until this admin installs its next view (`views`, its tick); an admin that holds no tick yet,
/// or whose tick has closed, waits `delay`, the resend schedule's.
async fn view_installed(views: &mut Option<tokio::sync::watch::Receiver<u64>>, delay: Duration) {
    let installed = match views.as_mut() {
        Some(rx) => rx.changed().await.is_ok(),
        None => false,
    };
    if !installed {
        tokio::time::sleep(delay).await;
    }
}

/// What a read of the change's `rdm.storage.committed` event found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The event found: switched.
    Switched,
    /// No such event on a provider that answers: not switched.
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

/// The events store of the provider registered as `provider`, opened on `scope` (storage.md §2: the
/// change's events are in the events stores of its two providers).
async fn events_on(providers: &crate::storage_backend::TopologyStorageProviders, scope: &crate::storage_backend::StorageScope, provider: &str) -> Result<Arc<dyn crate::events_storage::EventsStorage>, String> {
    Ok(providers.named(provider)?.open(scope).await?.events)
}

/// Read the change `change_id`'s `rdm.storage.committed` event in the events store of the provider
/// registered as `to`, opened on `scope`.
pub async fn read_commit(providers: &crate::storage_backend::TopologyStorageProviders, scope: &crate::storage_backend::StorageScope, to: &str, change_id: &ChangeId) -> Decision {
    // A read that errs, answered or not, is no read that the event is absent: the block holds.
    let events = match events_on(providers, scope, to).await {
        Ok(e) => e,
        Err(e) => return Decision::NoAnswer(e),
    };
    match events.of_storage_changes().await {
        Ok(read) if read.iter().any(|e| e.commits(change_id)) => Decision::Switched,
        Ok(_) => Decision::NotSwitched,
        Err(e) => Decision::NoAnswer(e.to_string()),
    }
}

/// Append the change `change_id`'s `rdm.storage.committed` event, written by the admin `scope` names at
/// rafka-time `time_ms`, to the events store of the provider registered as `to`: `true` when this
/// append wrote it, `false` when it stood already.
async fn insert_commit(providers: &crate::storage_backend::TopologyStorageProviders, scope: &crate::storage_backend::StorageScope, to: &str, time_ms: u64, change_id: &ChangeId) -> Result<bool, String> {
    let events = events_on(providers, scope, to).await?;
    if events.of_storage_changes().await.map_err(|e| e.to_string())?.iter().any(|e| e.commits(change_id)) {
        return Ok(false);
    }
    events.append(&crate::events_storage::Event::storage_committed(&source_of(scope), time_ms, change_id, to)).await.map_err(|e| e.to_string())?;
    Ok(true)
}

/// The writer of an event: the admin `scope` names.
fn source_of(scope: &crate::storage_backend::StorageScope) -> crate::events_storage::EventSource {
    crate::events_storage::EventSource { fabric_id: scope.fabric_id.clone(), node_id: scope.node_id.clone(), incarnation: scope.writer.clone() }
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

/// The copy (step 2), at its one call site: [`rdm_node::storage_copy::copy`]. An error opening either
/// provider's lines is told apart as the copy's own are: answered, the copy fails naming the provider;
/// unanswered, it is resumed.
async fn copy_stores(providers: &crate::storage_backend::TopologyStorageProviders, fabric_id: &crate::model::FabricId, change: &StorageChange) -> Copy {
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
        let run = CopyRun { change_id: change.change_id.clone(), source: change.from.clone(), destination: change.to.clone() };
        match copy(&run, &*source, &*destination).await {
            Ok(_) => Copy::Copied,
            Err(CopyError::Refused(why) | CopyError::Failed(why)) => Copy::Failed(why),
            Err(CopyError::Storage(e)) => Copy::NoAnswer(e.to_string()),
        }
    }
    #[cfg(not(feature = "file-backend"))]
    {
        let _ = (providers, fabric_id);
        Copy::Failed(format!("this build copies no provider's lines: `{}` to `{}`", change.from, change.to))
    }
}

/// The switch span of one read or append of the `rdm.storage.committed` event.
fn switch_span(change: &StorageChange) -> tracing::Span {
    tracing::info_span!("rdm.node_admin.storage.update.via-switch", change_id = %change.change_id, provider = %change.to, commit_found = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty)
}

/// Record a read of the `rdm.storage.committed` event on `span`.
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

/// How the change's steps ended.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ended {
    /// Switched and every mesh unblocked on the new provider.
    Complete,
    /// Failed before its switch, why; every mesh unblocked on the old provider.
    Failed(String),
}

/// The change this fabric-primary runs now.
struct Running {
    change: StorageChange,
    /// A fabric.delete accepted meanwhile ends the change.
    interrupted: Arc<tokio::sync::Notify>,
    /// Set once the change ended, its `rdm.storage.ended` event written.
    ended: tokio::sync::watch::Receiver<bool>,
}

/// What a fabric-primary's change needs of the admin it runs on, set once at its boot.
pub struct Wiring {
    /// The admin's `StatusService`: its view, its commands, its own birth and its Node RPC client.
    pub status_service: crate::status_rpc::StatusServiceSlot,
    /// The Build state: a Build in progress refuses a change (storage.md §2).
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
    /// The Fabric row's line once the change is switched: the new provider current, the old appended to
    /// the providers the fabric left, written if missing.
    pub fabric_row: Arc<dyn Fn(StorageChange) -> BoxFuture<'static, Result<(), String>> + Send + Sync>,
}

/// A node-admin's storage changes: the one it runs as fabric-primary, and the `mesh.block` and
/// `mesh.unblock` it serves as a mesh-primary.
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
    pub aggregate: OnceLock<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>>,
    /// What the change needs of this admin, set at its boot.
    pub wiring: OnceLock<Wiring>,
    /// This mesh-primary's node recovery, set once it holds it: it re-creates a node of its mesh that
    /// will not move, and a recovery of it not finished keeps `mesh.block` from `blocked`.
    pub(crate) recovery: OnceLock<crate::admin::RecoverySubmitter>,
    running: std::sync::Mutex<Option<Running>>,
}

/// How a mesh-primary's move of one node ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathEnd {
    /// The birth at the path moved, or the view holds none there.
    Moved,
    /// The birth would not move: its node recovery re-created it, and the new birth moved.
    Recreated,
}

/// Which of its two commands the fabric-primary sends a mesh-primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MeshCommand {
    Block,
    Unblock,
}

impl MeshCommand {
    fn name(self) -> &'static str {
        match self {
            Self::Block => "mesh.block",
            Self::Unblock => "mesh.unblock",
        }
    }
}

impl StorageChanges {
    /// The storage changes of the admin `node_id` writing through `handle`.
    pub fn new(handle: Arc<crate::storage_backend::TopologyStorageHandle>, providers: crate::storage_backend::TopologyStorageProviders, fabric_id: crate::model::FabricId, node_id: NodeId) -> Self {
        Self { handle, providers, fabric_id, node_id, aggregate: OnceLock::new(), wiring: OnceLock::new(), recovery: OnceLock::new(), running: std::sync::Mutex::new(None) }
    }

    /// The change this fabric-primary runs now, when one is in progress.
    pub fn in_progress(&self) -> Option<StorageChange> {
        self.running.lock().unwrap().as_ref().map(|r| r.change.clone())
    }

    fn status_service(&self) -> Result<Arc<StatusService>, String> {
        self.wiring.get().and_then(|w| w.status_service.get().cloned()).ok_or_else(|| format!("{}: this admin holds no StatusService yet", self.node_id))
    }

    /// Read the `rdm.storage.committed` event until its provider answers: the provider the change decided.
    pub(crate) async fn decide(&self, change: &StorageChange) -> String {
        let mut backoff = Duration::from_millis(500);
        loop {
            let decision = read_commit(&self.providers, self.handle.scope(), &change.to, &change.change_id).await;
            let span = switch_span(change);
            record_read(&span, &decision);
            span.in_scope(|| tracing::info!("the change's decision is read from its rdm.storage.committed event"));
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

    /// `fabric.storage.update { topology_storage_provider }` at the fabric-primary (storage.md §2
    /// "Changing it"): refused by name for a provider its wiring does not register or cannot open, and
    /// `build-in-progress` while a Build or another change is in progress; a provider the fabric writes
    /// already is `AlreadyApplied`. Otherwise the change's `rdm.storage.started` event is written on the
    /// old provider, the change runs as this fabric-primary's own workflow, and the reply names it.
    pub async fn update(self: &Arc<Self>, provider: &str) -> StatusReply {
        let span = tracing::info_span!("rdm.node_admin.storage.update.via-caller", node_id = %self.node_id, provider = %provider, change_id = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        let reply = self.admit(provider, &span).await;
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("fabric.storage.update decided"));
        reply
    }

    async fn admit(self: &Arc<Self>, provider: &str, span: &tracing::Span) -> StatusReply {
        let (status_service, wiring) = match (self.status_service(), self.wiring.get()) {
            (Ok(status_service), Some(wiring)) => (status_service, wiring),
            (Err(e), _) => return rdm_core::reply::status::not_ready(e),
            (_, None) => return rdm_core::reply::status::not_ready(format!("{}: this admin holds no storage change wiring yet", self.node_id)),
        };
        if !status_service.topology.read().await.is_fabric_primary(&self.node_id) {
            return rdm_core::reply::status::rejected_not_primary(rdm_core::status::NotPrimary::ReceiverNotPrimary { needed: "fabric-primary".into() });
        }
        if let Some(running) = self.in_progress() {
            return rdm_core::reply::status::storage_build_in_progress(None, Some(running.change_id));
        }
        if !self.providers.names().iter().any(|n| n == provider) {
            let reject = crate::build::BuildReject::TopologyStorageProviderNotRegistered { provider: provider.to_string(), registered: self.providers.names() };
            span.record("reason", reject.reason());
            return rdm_core::reply::status::topology_storage_provider_refused(reject.reason(), reject.to_string());
        }
        let from = match self.fabric_provider(&status_service).await {
            Ok(from) => from,
            Err(e) => return rdm_core::reply::status::not_ready(e),
        };
        if from == provider {
            return rdm_core::reply::status::already_applied();
        }
        if let Err(detail) = self.handle.opens(provider).await {
            let reject = crate::build::BuildReject::TopologyStorageProviderNotOpened { provider: provider.to_string(), detail };
            span.record("reason", reject.reason());
            return rdm_core::reply::status::topology_storage_provider_refused(reject.reason(), reject.to_string());
        }
        let change = StorageChange { change_id: ChangeId::mint(), from, to: provider.to_string() };
        // One change at a time, and never beside a Build: this one is held before the Build state is
        // read, so a Build accepted after the read is refused beside it.
        let (interrupted, ended) = {
            let mut running = self.running.lock().unwrap();
            if let Some(r) = running.as_ref() {
                return rdm_core::reply::status::storage_build_in_progress(None, Some(r.change.change_id.clone()));
            }
            let (tx, rx) = tokio::sync::watch::channel(false);
            let interrupted = Arc::new(tokio::sync::Notify::new());
            *running = Some(Running { change: change.clone(), interrupted: interrupted.clone(), ended: rx });
            (interrupted, tx)
        };
        let release = |me: &Self| *me.running.lock().unwrap() = None;
        match wiring.builds.current().await {
            Ok(Some(b)) if matches!(b.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running) => {
                release(self);
                return rdm_core::reply::status::storage_build_in_progress(Some(b.build_id.to_string()), None);
            }
            Ok(_) => {}
            Err(e) => {
                release(self);
                return rdm_core::reply::status::not_ready(format!("{}: the Build in progress could not be read: {e}", self.node_id));
            }
        }
        if let Err(e) = self.write_started(&status_service, &change).await {
            release(self);
            return rdm_core::reply::status::not_ready(e);
        }
        span.record("change_id", change.change_id.as_str());
        let me = self.clone();
        let change_id = change.change_id.clone();
        // The change's steps continue the caller's trace: one causal trace from the request to its end.
        let run = tracing::info_span!(parent: span, "rdm.node_admin.storage.update.via-change", change_id = %change.change_id, from = %change.from, to = %change.to, node_id = %self.node_id);
        tokio::spawn(tracing::Instrument::instrument(async move { me.run(change, interrupted, ended).await }, run));
        rdm_core::reply::status::storage_updated(change_id)
    }

    /// The provider the Fabric row names current.
    async fn fabric_provider(&self, status_service: &StatusService) -> Result<String, String> {
        let row = status_service.fabric_storage.fabric().await.map_err(|e| format!("{}: the Fabric row could not be read: {e}", self.node_id))?;
        Ok(row.and_then(|r| r.topology_storage_provider).unwrap_or_else(|| self.handle.provider()))
    }

    /// The change's `rdm.storage.started` event, on the old provider's events store, before its first
    /// command.
    async fn write_started(&self, status_service: &StatusService, change: &StorageChange) -> Result<(), String> {
        let time_ms = status_service.certs.as_ref().and_then(|c| c.now_ms()).ok_or_else(|| format!("{}: this admin holds no rafka-time to write the rdm.storage.started event on", self.node_id))?;
        let events = events_on(&self.providers, self.handle.scope(), &change.from).await?;
        events.append(&crate::events_storage::Event::storage_started(&source_of(self.handle.scope()), time_ms, &change.change_id, &change.to)).await.map_err(|e| e.to_string())?;
        tracing::info_span!("rdm.node_admin.storage.update.via-started", change_id = %change.change_id, from = %change.from, to = %change.to, outcome = "written")
            .in_scope(|| tracing::info!("the change's rdm.storage.started event is written on the old provider: the change is started"));
        Ok(())
    }

    /// A fabric-primary that takes the role (or starts holding it) resumes a change its predecessor
    /// started (storage.md §2: "Whoever resumes the change learns the decision only from that record"):
    /// the storage changes' events on the provider it writes, the newest change with no
    /// `rdm.storage.ended` event resumed from its first step; each step is idempotent. Its caller holds
    /// the fabric-primary record naming this admin ([`keep_resumed`]).
    pub async fn resume(self: &Arc<Self>) {
        let Ok(status_service) = self.status_service() else { return };
        if self.in_progress().is_some() {
            return;
        }
        let read = match self.handle.events().of_storage_changes().await {
            Ok(read) => read,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.storage.update.via-started", node_id = %self.node_id, outcome = "no-answer", reason = %e)
                    .in_scope(|| tracing::info!("the storage changes' events could not be read: a change in progress is resumed at the next read"));
                return;
            }
        };
        let Some(started) = read.iter().rev().find(|e| e.r#type == crate::events_storage::STORAGE_STARTED) else { return };
        let (Some(change_id), Some(to)) = (started.change_id(), started.topology_storage_provider().map(String::from)) else { return };
        if read.iter().any(|e| e.r#type == crate::events_storage::STORAGE_ENDED && e.change_id().as_ref() == Some(&change_id)) {
            return;
        }
        // The old provider: the Fabric row's current one before the switch, the last it left after it.
        let row = match status_service.fabric_storage.fabric().await {
            Ok(row) => row,
            Err(_) => return,
        };
        let current = row.as_ref().and_then(|r| r.topology_storage_provider.clone()).unwrap_or_else(|| self.handle.provider());
        let from = if current == to { row.and_then(|r| r.previous_topology_storage_providers.last().cloned()).unwrap_or(current) } else { current };
        let change = StorageChange { change_id, from, to };
        let (interrupted, ended) = {
            let mut running = self.running.lock().unwrap();
            if running.is_some() {
                return;
            }
            let (tx, rx) = tokio::sync::watch::channel(false);
            let interrupted = Arc::new(tokio::sync::Notify::new());
            *running = Some(Running { change: change.clone(), interrupted: interrupted.clone(), ended: rx });
            (interrupted, tx)
        };
        let resumed = tracing::info_span!("rdm.node_admin.storage.update.via-started", change_id = %change.change_id, from = %change.from, to = %change.to, node_id = %self.node_id, outcome = "resumed");
        resumed.in_scope(|| tracing::info!("a change this fabric-primary's predecessor started has no rdm.storage.ended event: it is resumed from its events"));
        let me = self.clone();
        let run = tracing::info_span!(parent: &resumed, "rdm.node_admin.storage.update.via-change", change_id = %change.change_id, from = %change.from, to = %change.to, node_id = %self.node_id);
        tokio::spawn(tracing::Instrument::instrument(async move { me.run(change, interrupted, ended).await }, run));
    }

    /// The change, to its `rdm.storage.ended` event: its steps, or, once a fabric.delete interrupts it,
    /// every mesh-primary sent `mesh.unblock` onto the provider it decided and the change ended failed.
    async fn run(self: Arc<Self>, change: StorageChange, interrupted: Arc<tokio::sync::Notify>, ended: tokio::sync::watch::Sender<bool>) {
        let notified = interrupted.notified();
        let end = tokio::select! {
            end = self.steps(&change) => end,
            // Each mesh-primary's unblock reads the decision itself: the provider it moves onto is the
            // one the change's rdm.storage.committed event decided.
            _ = notified => {
                self.command_every_mesh(&change, MeshCommand::Unblock).await;
                Ended::Failed(INTERRUPTED.into())
            }
        };
        let provider = self.decide(&change).await;
        self.write_ended(&change, &provider, &end).await;
        *self.running.lock().unwrap() = None;
        let _ = ended.send(true);
    }

    /// The change's `rdm.storage.ended` event on the provider it decided, written once that provider
    /// answers.
    async fn write_ended(&self, change: &StorageChange, provider: &str, end: &Ended) {
        let state = match end {
            Ended::Complete => crate::events_storage::StepState::Complete,
            Ended::Failed(reason) => crate::events_storage::StepState::Failed { reason: reason.clone(), code: if reason == INTERRUPTED { "ABORTED".into() } else { "FAILED_PRECONDITION".into() } },
        };
        let mut backoff = Duration::from_millis(500);
        loop {
            let time_ms = self.status_service().ok().and_then(|d| d.certs.as_ref().and_then(|c| c.now_ms()));
            let written = match time_ms {
                None => Err(format!("{}: this admin holds no rafka-time to write the rdm.storage.ended event on", self.node_id)),
                Some(time_ms) => match events_on(&self.providers, self.handle.scope(), provider).await {
                    Ok(events) => events.append(&crate::events_storage::Event::storage_ended(&source_of(self.handle.scope()), time_ms, &change.change_id, provider, &state)).await.map_err(|e| e.to_string()),
                    Err(e) => Err(e),
                },
            };
            let span = tracing::info_span!("rdm.node_admin.storage.update.via-ended", change_id = %change.change_id, provider = %provider, state = tracing::field::Empty, reason = tracing::field::Empty, outcome = tracing::field::Empty);
            match &end {
                Ended::Complete => span.record("state", "complete"),
                Ended::Failed(reason) => span.record("state", "failed").record("reason", reason.as_str()),
            };
            match written {
                Ok(()) => {
                    span.record("outcome", "written");
                    span.in_scope(|| tracing::info!("the change's rdm.storage.ended event is written on the provider it decided: the change is over"));
                    return;
                }
                Err(e) => {
                    span.record("outcome", "no-answer").record("reason", e.as_str());
                    drop(span);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    /// Steps 1 to 4: every mesh blocked, the copy, the switch and the Fabric row's line, every mesh
    /// unblocked. A copy refused by name, or answered by a provider with a named error, fails the change
    /// before its switch once no `rdm.storage.committed` event is read: every mesh is unblocked on the
    /// old provider. A copy no provider answered is resumed on a backoff, no timer ending it.
    async fn steps(&self, change: &StorageChange) -> Ended {
        self.command_every_mesh(change, MeshCommand::Block).await;
        if let Ok(status_service) = self.status_service() {
            if let Some(seam) = &status_service.command_frame_seam {
                seam.before_storage_switch(&change.change_id).await;
            }
        }
        if read_commit(&self.providers, self.handle.scope(), &change.to, &change.change_id).await == Decision::Switched {
            let span = switch_span(change);
            record_read(&span, &Decision::Switched);
            span.in_scope(|| tracing::info!("the change is resumed and its rdm.storage.committed event is found: it is switched"));
        } else {
            // 2. Copy.
            let mut backoff = Duration::from_millis(500);
            loop {
                match copy_stores(&self.providers, &self.fabric_id, change).await {
                    Copy::Copied => break,
                    Copy::Failed(why) => {
                        if self.decide(change).await == change.to {
                            break;
                        }
                        self.command_every_mesh(change, MeshCommand::Unblock).await;
                        return Ended::Failed(why);
                    }
                    Copy::NoAnswer(_) => {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
            }
            // 3. Switch: the event's append is the decision; a lost answer is learnt by reading it.
            self.switch(change).await;
        }
        // This admin writes the new provider before the Fabric row's line, so the line lands there.
        let mut backoff = Duration::from_millis(500);
        loop {
            let moved = match self.status_service() {
                Ok(status_service) => self.move_own(&status_service, &change.change_id, &change.to).await,
                Err(e) => Err(e),
            };
            match moved {
                Ok(()) => break,
                Err(e) => {
                    tracing::info_span!("rdm.node_admin.storage.update.via-switch", change_id = %change.change_id, provider = %change.to, outcome = "not-moved", reason = %e)
                        .in_scope(|| tracing::info!("this fabric-primary has not moved onto the new provider yet: moved again"));
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
        // The Fabric row's line, written if missing.
        let mut backoff = Duration::from_millis(500);
        while let Some(wiring) = self.wiring.get() {
            match (wiring.fabric_row)(change.clone()).await {
                Ok(()) => break,
                Err(e) => {
                    tracing::info_span!("rdm.node_admin.storage.update.via-switch", change_id = %change.change_id, provider = %change.to, outcome = "fabric-row-unwritten", reason = %e)
                        .in_scope(|| tracing::info!("the Fabric row's line naming the new provider is not written yet: written again"));
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
        // 4. Unblock.
        self.command_every_mesh(change, MeshCommand::Unblock).await;
        Ended::Complete
    }

    /// Append the change's `rdm.storage.committed` event on the new provider until it stands.
    async fn switch(&self, change: &StorageChange) {
        let mut backoff = Duration::from_millis(500);
        loop {
            let span = switch_span(change);
            let Some(time_ms) = self.status_service().ok().and_then(|d| d.certs.as_ref().and_then(|c| c.now_ms())) else {
                span.record("outcome", "no-answer").record("reason", "this admin holds no rafka-time to write the rdm.storage.committed event on");
                drop(span);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
                continue;
            };
            match insert_commit(&self.providers, self.handle.scope(), &change.to, time_ms, &change.change_id).await {
                Ok(true) => {
                    span.record("commit_found", false).record("outcome", "inserted");
                    span.in_scope(|| tracing::info!("the rdm.storage.committed event is appended on the new provider: the change is switched"));
                    return;
                }
                Ok(false) => {
                    span.record("commit_found", true).record("outcome", "found");
                    span.in_scope(|| tracing::info!("the rdm.storage.committed event stood already: the change is switched"));
                    return;
                }
                Err(e) => {
                    span.record("outcome", "no-answer").record("reason", e.as_str());
                    drop(span);
                    if read_commit(&self.providers, self.handle.scope(), &change.to, &change.change_id).await == Decision::Switched {
                        record_read(&switch_span(change), &Decision::Switched);
                        return;
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    /// `command` to every mesh's mesh-primary, the fabric-primary's own mesh first, each sent again
    /// until it answers its terminal: commands go down, fabric-primary to mesh-primary (a lost
    /// mesh-primary is investigated and recovered as any node is, and its successor is sent it).
    async fn command_every_mesh(&self, change: &StorageChange, command: MeshCommand) {
        let Ok(status_service) = self.status_service() else { return };
        let view = status_service.topology.read().await.clone();
        let own_mesh = view.own(&self.node_id).map(|n| n.mesh.clone());
        let mut meshes: Vec<String> = view.meshes.iter().filter(|m| m.status != crate::model::ScopeStatus::Dead).map(|m| m.name.clone()).collect();
        meshes.sort_by_key(|m| (Some(m) != own_mesh.as_ref(), m.clone()));
        for mesh in meshes {
            self.command_mesh(&status_service, change, &mesh, command).await;
        }
    }

    /// `command` to `mesh`'s mesh-primary, the birth the view names now, one call on its own `Send`
    /// (Q344) per turn until it answers its terminal (`blocked` or `unblocked`). A call that ended
    /// unanswered is never made again to that birth: it is a lost node, investigated and re-created by
    /// its own workflow (storage.md "A mesh-primary that does not reply `blocked` or `unblocked` is a
    /// lost node"), and the command waits on this admin's installed view until it names another birth,
    /// as it does while the view names no mesh-primary. Another turn on the same birth is taken on a
    /// domain answer only, a reply other than the terminal, a new delivery after the client's resend
    /// schedule's next delay (node-rpc.md §43). This fabric-primary's own mesh is served in-process.
    async fn command_mesh(&self, status_service: &Arc<StatusService>, change: &StorageChange, mesh: &str, command: MeshCommand) {
        let configs = status_service.configs.get();
        let pacing = configs.map(|c| c.client().delivery().pacing()).unwrap_or_else(rdm_transport::Pacing::from_cadences);
        // A clone remembers the version its source last saw: only a view installed from now on wakes it.
        let mut views = configs.and_then(|c| c.views());
        if let Some(rx) = views.as_mut() {
            rx.borrow_and_update();
        }
        let mut turn = 0u32;
        let mut unanswered: Option<(NodeId, Option<rdm_core::IncarnationId>)> = None;
        loop {
            turn = turn.saturating_add(1);
            let mut called = false;
            let primary = status_service.topology.read().await.node_type_primary(mesh, "node-admin").cloned();
            if let (Some(p), Some(lost)) = (&primary, &unanswered) {
                if (&p.node_id, &p.incarnation_id) == (&lost.0, &lost.1) {
                    view_installed(&mut views, pacing.after(turn)).await;
                    continue;
                }
            }
            let span = tracing::info_span!("rdm.node_admin.storage.update.via-mesh-command", change_id = %change.change_id, mesh = %mesh, command = command.name(), mesh_primary = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty);
            let reply = match &primary {
                None => Err(format!("the view names no mesh-primary of {mesh} yet")),
                Some(p) if p.node_id == self.node_id => Ok(match command {
                    MeshCommand::Block => self.serve_block(change).await,
                    MeshCommand::Unblock => self.serve_unblock(change).await,
                }),
                Some(p) => match (p.incarnation_id.clone(), configs) {
                    (Some(incarnation), Some(configs)) => {
                        called = true;
                        let request = match command {
                            MeshCommand::Block => NodeCommandRequest::BlockMesh { node_id: p.node_id.clone(), incarnation, change_id: change.change_id.clone(), from: change.from.clone(), to: change.to.clone() },
                            MeshCommand::Unblock => NodeCommandRequest::UnblockMesh { node_id: p.node_id.clone(), incarnation, change_id: change.change_id.clone(), from: change.from.clone(), to: change.to.clone() },
                        };
                        self::command(configs.client(), &status_service.commands, &p.node_id, &request).await
                    }
                    (None, _) => Err(format!("{mesh}'s mesh-primary {} holds no incarnation in the view", p.node_id)),
                    (_, None) => Err("this admin has no Node RPC client yet".into()),
                },
            };
            if let Some(p) = &primary {
                span.record("mesh_primary", p.node_id.as_str());
            }
            match reply {
                Ok(NodeCommandReply::MeshBlocked) if command == MeshCommand::Block => {
                    span.record("outcome", "blocked");
                    return;
                }
                Ok(NodeCommandReply::MeshUnblocked { .. }) if command == MeshCommand::Unblock => {
                    span.record("outcome", "unblocked");
                    return;
                }
                Ok(other) => {
                    span.record("outcome", other.name());
                    drop(span);
                    tokio::time::sleep(pacing.after(turn)).await;
                }
                Err(e) => {
                    span.record("outcome", "no-answer").record("reason", e.as_str());
                    drop(span);
                    if let (true, Some(p)) = (called, &primary) {
                        unanswered = Some((p.node_id.clone(), p.incarnation_id.clone()));
                    }
                    // What the view names changes only with a view installed.
                    view_installed(&mut views, pacing.after(turn)).await;
                }
            }
        }
    }

    /// `mesh.block` at this mesh-primary (step 1): every birth of its mesh, a stopped one and a retained
    /// predecessor included, freezes its writes; then its in-flight commands complete, it gossips a
    /// current mesh topology of its mesh, replies `blocked` and issues no new command until it is
    /// unblocked. A freeze not answered yet is no block, nor is a node recovery of a node of its mesh
    /// this mesh-primary has not finished: the fabric-primary sends it again.
    pub async fn serve_block(&self, change: &StorageChange) -> NodeCommandReply {
        let status_service = match self.status_service() {
            Ok(status_service) => status_service,
            Err(e) => return rdm_core::reply::node_command::not_ready(e),
        };
        let view = status_service.topology.read().await.clone();
        let Some(mesh) = view.own(&self.node_id).filter(|n| view.is_mesh_primary(&n.mesh, &self.node_id)).map(|n| n.mesh.clone()) else {
            return rdm_core::reply::node_command::not_ready(format!("{}: this admin is no mesh-primary in its view; mesh.block is a mesh-primary's", self.node_id));
        };
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-block", change_id = %change.change_id, mesh = %mesh, node_id = %self.node_id, in_flight = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        // An unanswered freeze is resolved through node investigation/recovery before `blocked` (storage.md
        // §2 step 1): while this mesh-primary's recovery of a birth of its mesh has not finished, the
        // birth's successor has not acknowledged its freeze, whether or not the view holds it yet.
        if let Some(recovery) = self.recovery.get().and_then(|r| r.unfinished_recovery(&mesh)) {
            span.record("outcome", "not-blocked").record("reason", recovery.as_str());
            span.in_scope(|| tracing::info!("mesh.block: this mesh-primary's node recovery of a node of its mesh has not finished; nothing is copied"));
            return rdm_core::reply::node_command::not_ready(format!("mesh.block:{mesh}: {recovery}; the provider is not copied"));
        }
        // Nodes own their property writes: the primary's command block alone cannot make their old
        // provider quiescent. Stopped births are included.
        let subjects: Vec<Node> = view.births().filter(|node| node.mesh == mesh && node.status != crate::model::NodeStatus::Deleted).cloned().collect();
        for subject in &subjects {
            if let Err(reason) = status_service.carry_storage_block(subject, &change.change_id).await {
                span.record("outcome", "not-blocked").record("reason", reason.as_str());
                span.in_scope(|| tracing::info!("mesh.block: a node of this mesh has not frozen its writes yet; nothing is copied"));
                return rdm_core::reply::node_command::not_ready(format!("mesh.block:{mesh}: {reason}; the provider is not copied"));
            }
        }
        let in_flight = status_service.commands.block().await;
        if let Some(aggregate) = self.aggregate.get() {
            aggregate().await;
        }
        span.record("in_flight", in_flight as u64).record("outcome", "blocked");
        span.in_scope(|| tracing::info!("mesh.block: every command in flight completed; this mesh-primary replies blocked, gossips its mesh's topology and issues no new command until it is unblocked"));
        NodeCommandReply::MeshBlocked
    }

    /// `mesh.unblock` at this mesh-primary (step 4): the provider the `rdm.storage.committed` event
    /// decided (the old one when the change failed before its switch), this mesh-primary and each other
    /// node of its mesh moved onto it, each row recording it; a node that will not move is re-created
    /// on it by this mesh-primary's node recovery; once every node moved or was re-created it replies
    /// `unblocked` and issues commands again.
    pub async fn serve_unblock(&self, change: &StorageChange) -> NodeCommandReply {
        let status_service = match self.status_service() {
            Ok(status_service) => status_service,
            Err(e) => return rdm_core::reply::node_command::not_ready(e),
        };
        let mesh = {
            let view = status_service.topology.read().await;
            match view.own(&self.node_id).filter(|n| view.is_mesh_primary(&n.mesh, &self.node_id)) {
                Some(n) => n.mesh.clone(),
                None => return rdm_core::reply::node_command::not_ready(format!("{}: this admin is no mesh-primary in its view; mesh.unblock is a mesh-primary's", self.node_id)),
            }
        };
        // The decision, read once (storage.md step 4): a read error fails the unblock by name, never as
        // absence, and the fabric-primary sends it again.
        let provider = match read_commit(&self.providers, self.handle.scope(), &change.to, &change.change_id).await {
            Decision::Switched => change.to.clone(),
            Decision::NotSwitched => change.from.clone(),
            Decision::NoAnswer(e) => return rdm_core::reply::node_command::not_ready(format!("storage change {}: its rdm.storage.committed event on {} could not be read: {e}", change.change_id, change.to)),
        };
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-unblock", change_id = %change.change_id, mesh = %mesh, node_id = %self.node_id, provider = %provider, moved = tracing::field::Empty, recreated = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty);
        if let Err(e) = self.move_own(&status_service, &change.change_id, &provider).await {
            span.record("outcome", "not-moved").record("reason", e.as_str());
            return rdm_core::reply::node_command::not_ready(e);
        }
        // Moved: this admin writes rows again, on the decided provider.
        status_service.commands.release_rows();
        let topology = status_service.topology.clone();
        let paths: Vec<PathName> = topology.read().await.births().filter(|n| n.mesh == mesh && n.node_id != self.node_id && n.status != crate::model::NodeStatus::Deleted).map(|n| n.location()).collect();
        let ends = futures_util::future::join_all(paths.iter().map(|p| self.move_until(&status_service, &topology, &change.change_id, &mesh, p, &provider))).await;
        let recreated = ends.iter().filter(|e| **e == PathEnd::Recreated).count();
        status_service.commands.unblock();
        span.record("moved", (paths.len() - recreated) as u64).record("recreated", recreated as u64).record("outcome", "unblocked");
        span.in_scope(|| tracing::info!("mesh.unblock: every node of this mesh moved or was re-created; this mesh-primary replies unblocked and issues commands again"));
        NodeCommandReply::MeshUnblocked { provider }
    }

    /// Carry the move onto `provider` to the birth at `path` until the birth the view holds there moves
    /// (storage.md §2 step 4). A birth that refuses its move by name (a `rejected-stale-change` of this
    /// change included), or leaves its call unanswered with its `Send` spent (Q344), will not move: it is
    /// re-created by this mesh-primary's node recovery, and the move is carried to the new birth. No
    /// timer ends it.
    async fn move_until(&self, status_service: &StatusService, topology: &Arc<RwLock<Topology>>, change_id: &ChangeId, mesh: &str, path: &PathName, provider: &str) -> PathEnd {
        let pacing = rdm_transport::Pacing::from_cadences();
        let mut turn = 0u32;
        // The births re-created at the path: the view may still hold one a moment after its re-create.
        let mut recreated: Vec<NodeId> = Vec::new();
        loop {
            turn = turn.saturating_add(1);
            let birth = topology.read().await.node(path).cloned();
            let lost = match birth {
                // No birth held there: nothing to move, or the re-created birth not in the view yet.
                None if recreated.is_empty() => return PathEnd::Moved,
                None => None,
                Some(birth) if recreated.contains(&birth.node_id) => None,
                Some(birth) => match birth.incarnation_id.clone() {
                    None => None,
                    Some(incarnation) => {
                        match status_service.carry_storage_step(&birth.node_id, &incarnation, change_id, provider).await {
                            Carried::Stepped(_) => return if recreated.is_empty() { PathEnd::Moved } else { PathEnd::Recreated },
                            // A refusal of an older change (storage.md §2): a refusal, never a move.
                            Carried::Stale(held) => Some((birth, format!("rejected-stale-change: it acted on storage change {held}"))),
                            Carried::Refused(why) => Some((birth, format!("refused: {why}"))),
                            // Its call on `Send` ended with its attempts spent (Q344): the end is the
                            // birth's, never sent again.
                            Carried::Unanswered(why) => Some((birth, format!("unanswered: {why}"))),
                            Carried::NotDone(_) => None,
                        }
                    }
                },
            };
            if let Some((birth, reason)) = lost {
                let span = tracing::info_span!("rdm.node_admin.node.update.via-storage-move-lost", change_id = %change_id, mesh = %mesh, node_path = %path.path, node_name = %path.name, node_id = %birth.node_id, incarnation_id = %birth.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or(""), provider = %provider, reason = %reason, outcome = tracing::field::Empty);
                span.in_scope(|| tracing::info!("the node will not move: this mesh-primary's node recovery re-creates it on the provider this mesh-primary writes"));
                match self.recovery.get().cloned() {
                    Some(recovery) => {
                        use tracing::Instrument as _;
                        recovery.recreate(&birth).instrument(span.clone()).await;
                        span.record("outcome", "recovery-submitted");
                        recreated.push(birth.node_id);
                        continue;
                    }
                    None => {
                        span.record("outcome", "no-recovery-yet");
                    }
                }
            }
            tokio::time::sleep(pacing.after(turn)).await;
        }
    }

    /// This admin moved onto `provider` (the provider it writes is the Fabric record's, Q361).
    async fn move_own(&self, status_service: &StatusService, change_id: &ChangeId, provider: &str) -> Result<(), String> {
        let own = status_service.own.get().and_then(|own| own.storage_moves()).ok_or_else(|| "this admin holds no storage handle yet".to_string())?;
        if let Some(refused) = own.refusal(change_id, provider).await {
            return Err(format!("this admin's move onto {provider}: {}", refused.name()));
        }
        match own.serve(change_id, provider).await {
            NodeCommandReply::StorageStepped { .. } => Ok(()),
            other => Err(format!("this admin's move onto {provider}: {}", other.name())),
        }
    }

    /// `fabric.delete` admitted while a storage change is in progress (storage.md §2: "it interrupts the
    /// change as it interrupts any Build"): the change is ended, every mesh-primary sent `mesh.unblock`
    /// onto the provider its `rdm.storage.committed` event decided and its `rdm.storage.ended` event
    /// written failed, before this returns and the delete is written.
    pub async fn interrupt_for_delete(&self) {
        let ended = {
            let running = self.running.lock().unwrap();
            let Some(r) = running.as_ref() else { return };
            r.interrupted.notify_waiters();
            r.interrupted.notify_one();
            r.ended.clone()
        };
        let mut ended = ended;
        let _ = ended.wait_for(|e| *e).await;
    }
}

/// Keep a fabric-primary's storage change resumed: each time the primary records change, and once at
/// the start, an admin the fabric-primary record names resumes a change its predecessor started and
/// did not end ([`StorageChanges::resume`]). It reads the record it is woken by: the view rebuilt from
/// that record later would miss the change that woke it.
pub async fn keep_resumed(changes: Arc<StorageChanges>, primaries: rdm_gossip::primary::PrimaryView) {
    let mut changed = primaries.subscribe();
    loop {
        if primaries.fabric().is_some_and(|p| p.node_id == changes.node_id) {
            changes.resume().await;
        }
        if changed.changed().await.is_err() {
            return;
        }
    }
}
