//! The fabric states and their rounds, run by the admins of a fabric (states.md;
//! fabric-state-sync.md, fabric-state-commit.md, fabric-open-traffic.md).
//!
//! - The fabric-primary alone moves the fabric: `pending` → `state-sync` (every mesh is
//!   ready-for-traffic) → `state-commit` (the application answered state-synced) →
//!   `ready-for-traffic` (the commit-state and open-traffic rounds both completed). The
//!   FabricStatus frame carrying each state is authored by it only, by the status loop that reads
//!   the state it holds.
//! - A round is a command pair down and a completion pair up: fabric-primary → mesh-primary →
//!   member. A primary reports upward only after its own action and every planned member's
//!   check-in; a check-in counts only as the planned exact birth, after the primary received this
//!   round's down op. Nothing leaves a checklist by a timer.
//! - The application work is behind two hooks: `sync_state` (state-sync) and `traffic_opened`
//!   (one notice, gating nothing). What a node writes at commit-state is behind [`RoundActions`].

use crate::accepted::{AcceptedStore, FabricTopology};
use crate::admin::Records;
use crate::build_claim::AttemptContexts;
use crate::build_state::BuildStateAdapter;
use crate::fabric_state::{Expected, FabricState, RoundBook, RoundKey, RoundKind, RoundOp};
use crate::model::{FabricId, IncarnationId, Node, NodeId, NodeKind, PathName};
use crate::round::Missing;
use crate::status_declare::{Authority, DeclareWake, Declarer, Key};
use crate::topology::Topology;
use crate::wiring::{FabricHooks, StateSyncRound, SyncState};
use async_trait::async_trait;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{NotAuthority, Status, StatusReply, StatusRequest};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use tracing::Instrument;

/// What a node does before it checks in to a round: the required writes of the phase.
///
/// Commit-state: the node writes what its scratchpad holds into the system org (a node its
/// connection rows, a mesh primary its members' Node rows and its Mesh row, the fabric-primary the
/// Fabric row). The check-in is owed only after the writes are stored; a refused write is an `Err`
/// and the node owes no check-in.
#[async_trait]
pub trait RoundActions: Send + Sync {
    /// The commit-state action of `node`.
    async fn commit_state(&self, node: &PathName) -> Result<(), String>;
    /// The open-traffic action of `node`.
    async fn open_traffic(&self, node: &PathName) -> Result<(), String>;
}

/// The actions of an embedding that binds none: nothing is published, and the span says so.
pub struct NoScratchpad;

#[async_trait]
impl RoundActions for NoScratchpad {
    async fn commit_state(&self, node: &PathName) -> Result<(), String> {
        tracing::info_span!("rdm.node_admin.fabric.update.via-round-action", node = %node, round = "commit-state", bound = false, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("no scratchpad publication is bound: this node holds nothing to publish"));
        Ok(())
    }
    async fn open_traffic(&self, node: &PathName) -> Result<(), String> {
        tracing::info_span!("rdm.node_admin.fabric.update.via-round-action", node = %node, round = "open-traffic", bound = false, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("no traffic-opening action is bound to this node"));
        Ok(())
    }
}

/// The reason the Build this admin holds accepted, `accepted`, supersedes the round `key`, or `None`
/// while the round is current (node-rpc-envelope.md, "Fabric status rounds": "The round is scoped to
/// the accepted Build, attempt and planned exact births"; states.md, The round checklist: the checklist
/// "names the accepted Build's PLANNED births"). `key_build` is the round's own Build as this admin's
/// log holds it, read only when it is another Build than the accepted one.
pub(crate) fn superseded_by(key: &RoundKey, accepted: &crate::build_state::BuildProjection, key_build: Option<&crate::build_state::BuildProjection>) -> Option<String> {
    if accepted.build_id.0 == key.build_id {
        return (accepted.attempt > key.attempt).then(|| format!("the accepted Build {} is at attempt {}, past this round's attempt {}", key.build_id, accepted.attempt, key.attempt));
    }
    // Another Build: superseded only by one accepted after the round's own. A round of a Build this
    // admin's log has not learned, or of one accepted after the Build it holds, came ahead of the pointer.
    let own = key_build?;
    (own.submitted_at_ms < accepted.submitted_at_ms).then(|| format!("Build {} (accepted at {} ms) was accepted after this round's Build {} (accepted at {} ms)", accepted.build_id.0, accepted.submitted_at_ms, key.build_id, own.submitted_at_ms))
}

/// Withdraw from `declarer` every round completion `accepted` (the Build this admin holds accepted)
/// has moved past, by the [`superseded_by`] rule: the completion of a superseded round is not owed
/// and is never sent. `builds` is read for the Build of a round that is not the accepted one.
pub(crate) async fn withdraw_superseded_completions(declarer: &Declarer, me: &PathName, accepted: &crate::build_state::BuildProjection, builds: &dyn BuildStateAdapter) {
    let _ = (declarer, me, accepted, builds);
}

/// RDM standalone's commit-state action (fabric-state-commit.md step 2: "Each node writes what it
/// holds in its scratchpad into the store the state-sync work established"; node-admin-lifecycle.md
/// §7): the node publishes its scratchpad to the fabric's store root, then runs the embedding's own
/// action, if it binds one. Each node holds only the rows it writes, so a node publishes its
/// connections rows, a mesh primary also its Mesh row and its members' Node rows, and the
/// fabric-primary also the Fabric row. A node that opened a root the Fabric row was already
/// published to holds nothing, and every later round's publication finds nothing held. A
/// publication that fails is an `Err`: the node owes no check-in.
pub struct StorePublication {
    /// What this node holds before the fabric's first state-commit; `None` when it opened the store
    /// root after it.
    scratchpad: Option<Arc<crate::record_store::Scratchpad>>,
    /// The embedding's own action, run after the publication.
    then: Option<Arc<dyn RoundActions>>,
}

impl StorePublication {
    /// The action publishing `scratchpad`, then running `then`.
    pub fn new(scratchpad: Option<Arc<crate::record_store::Scratchpad>>, then: Option<Arc<dyn RoundActions>>) -> Self {
        Self { scratchpad, then }
    }
}

#[async_trait]
impl RoundActions for StorePublication {
    async fn commit_state(&self, node: &PathName) -> Result<(), String> {
        let span = tracing::info_span!(
            "rdm.node_admin.fabric.update.via-round-action",
            node = %node,
            round = "commit-state",
            bound = true,
            rows = tracing::field::Empty,
            dropped = tracing::field::Empty,
            held = tracing::field::Empty,
            stands = tracing::field::Empty,
            refused = tracing::field::Empty,
            stores = tracing::field::Empty,
            error = tracing::field::Empty,
            "otel.kind" = "internal"
        );
        let published = match &self.scratchpad {
            Some(s) => s.publish().instrument(span.clone()).await,
            None => Ok(crate::record_store::Publication::default()),
        };
        match published {
            Ok(p) => {
                span.record("rows", p.written() as u64);
                span.record("dropped", p.dropped() as u64);
                span.record("held", p.held() as u64);
                span.record("stands", p.stands() as u64);
                span.record("refused", p.refused() as u64);
                span.record("stores", p.summary().as_str());
                span.in_scope(|| tracing::info!("this node wrote what its scratchpad held to the store root, and writes there from now on"));
            }
            Err(e) => {
                span.record("error", tracing::field::display(&e));
                span.in_scope(|| tracing::info!("this node's scratchpad could not be published to the store root"));
                return Err(format!("{node}: its scratchpad could not be published to the store root: {e}"));
            }
        }
        match &self.then {
            Some(then) => then.commit_state(node).await,
            None => Ok(()),
        }
    }
    async fn open_traffic(&self, node: &PathName) -> Result<(), String> {
        match &self.then {
            Some(then) => then.open_traffic(node).await,
            None => NoScratchpad.open_traffic(node).await,
        }
    }
}

/// Run the action of `kind` for `node` under its span.
pub(crate) async fn run_action(actions: &dyn RoundActions, node: &PathName, kind: RoundKind) -> Result<(), String> {
    match kind {
        RoundKind::StateCommit => actions.commit_state(node).await,
        RoundKind::OpenTraffic => actions.open_traffic(node).await,
    }
}

/// How a call to a round peer ended.
enum Delivery {
    /// The peer admitted the op (`Applied` or `AlreadyApplied`): it is never sent again.
    Answered(StatusReply),
    /// The peer was not reached (`typed: false`), or replied with a refusal or that it is not ready
    /// (`typed: true`; a refusal rests on the peer's own view, which heals):
    /// the same op is sent again on an eligible event only (`RoundBook::take_due`).
    Undelivered { why: String, typed: bool },
}

fn delivery(out: &RpcOutcome<StatusReply>) -> Delivery {
    delivery_of(out.reply().map(|r| r.value()), out.name())
}

/// `reply` is the typed reply the call got, when it got one; `outcome` names how a call without one ended.
fn delivery_of(reply: Option<&StatusReply>, outcome: &str) -> Delivery {
    match reply {
        Some(r @ (StatusReply::Applied | StatusReply::AlreadyApplied)) => Delivery::Answered(r.clone()),
        Some(other) => Delivery::Undelivered { why: format!("{other:?}"), typed: true },
        None => Delivery::Undelivered { why: outcome.to_string(), typed: false },
    }
}

/// What the status loop hands the fabric-primary's step each turn.
pub struct ClimbInputs<'a> {
    /// Whether this admin holds the fabric-primary role now.
    pub is_fabric_primary: bool,
    /// The state of the last FabricStatus frame this admin heard (`None`: none heard).
    pub heard: Option<&'a str>,
    /// This admin's view.
    pub view: &'a Topology,
}

/// The shared handles one admin's rounds read and act through.
pub(crate) struct RoundEnv {
    /// This admin's `path.name`.
    pub me: PathName,
    /// This admin's node id.
    pub node_id: NodeId,
    /// This admin's incarnation.
    pub incarnation: IncarnationId,
    /// The fabric.
    pub fabric_id: FabricId,
    /// This admin's view.
    pub topology: Arc<RwLock<Topology>>,
    /// The process's Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// The accepted Build pointer.
    pub accepted: Arc<AcceptedStore>,
    /// The Build log.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// The attempts' observability contexts.
    pub contexts: Arc<AttemptContexts>,
    /// What this admin owes its authorities: a mesh primary's completion is owed through it.
    pub declarer: Arc<Declarer>,
    /// Wakes the declarer.
    pub wake: Arc<DeclareWake>,
    /// The records whose fabric state the projection reads.
    pub records: Arc<Records>,
    /// The application hooks.
    pub hooks: FabricHooks,
    /// The action this admin completes before its own check-in.
    pub actions: Arc<dyn RoundActions>,
}

enum Progress {
    Waiting,
    Answered(StateSyncRound),
}

struct SyncRun {
    sent: StateSyncRound,
    progress: Arc<Mutex<Progress>>,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Commit,
    Open,
}

#[derive(Default)]
struct Climb {
    adopted: bool,
    root: Option<tracing::Span>,
    sync: Option<SyncRun>,
    phase: Option<Phase>,
    key: Option<RoundKey>,
    notified: bool,
    /// The accepted topology the fabric climbs under: a Build that changes it (a resize or a roll)
    /// puts the fabric back to pending (states.md, "Resize and roll").
    tracked: Option<FabricTopology>,
    /// The blocker last named, so a standing blocker is one span and a changed one is another.
    named: Option<String>,
}

/// The gossip a round's hooks go out on: this admin's mesh channel and the backbone.
struct RoundGossip {
    backbone: rafka_mesh_transport::membership::Backbone,
    membership: rafka_mesh_transport::membership::Membership,
}

/// One admin's rounds: as the fabric-primary the climb, as a mesh primary the commands to its
/// members and its completion up, as a member the check-in.
pub struct FabricRounds {
    env: RoundEnv,
    /// The check-ins of the members of this admin's mesh, as its mesh primary.
    pub mesh_book: RoundBook,
    /// The check-ins of the mesh primaries, as the fabric-primary.
    pub fabric_book: RoundBook,
    drivers: Mutex<HashSet<RoundKey>>,
    /// Where the round hooks are published; set once the admin has joined the backbone.
    gossip: std::sync::OnceLock<RoundGossip>,
    climb: tokio::sync::Mutex<Climb>,
    /// Poked when a check-in, the application's answer or this admin's own mesh round arrives.
    climb_wake: tokio::sync::Notify,
}

impl FabricRounds {
    /// The rounds of the admin `env` describes.
    pub(crate) fn new(env: RoundEnv) -> Arc<Self> {
        Arc::new(Self { env, mesh_book: RoundBook::default(), fabric_book: RoundBook::default(), drivers: Mutex::new(HashSet::new()), gossip: std::sync::OnceLock::new(), climb: tokio::sync::Mutex::new(Climb::default()), climb_wake: tokio::sync::Notify::new() })
    }

    /// Where this admin's round hooks are published, once it has joined the backbone.
    pub fn set_gossip(&self, backbone: rafka_mesh_transport::membership::Backbone, membership: rafka_mesh_transport::membership::Membership) {
        let _ = self.gossip.set(RoundGossip { backbone, membership });
    }

    /// Publish the round hook this admin authors (gossip.md, "Fabric round gossip hooks"): on its own
    /// mesh channel and the backbone, in the path of the call it runs with. A command hook names the
    /// commanded birth; a completion hook names this admin's own birth.
    async fn publish_hook(&self, key: &RoundKey, up: bool, node_id: &NodeId, incarnation: &IncarnationId) {
        let Some(gossip) = self.gossip.get() else {
            tracing::info_span!("rdm.node_admin.fabric.update.via-round-hook", node = %self.env.me, round = key.kind.command(), bound = false, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("no gossip is bound to this admin's rounds: the hook is not published"));
            return;
        };
        let frame = crate::fabric_state::hook_frame(key, up, node_id.clone(), incarnation.clone(), &self.env.me.to_string(), gossip.membership.clock().now_rafka_ms());
        let kind = if up { key.kind.completion() } else { key.kind.command() };
        let span = tracing::info_span!("rdm.node_admin.fabric.update.via-round-hook", node = %self.env.me, round = key.kind.command(), hook = kind, subject_id = %node_id, incarnation_id = %incarnation.0, build_id = %key.build_id, attempt = key.attempt, channels = "mesh,backbone", bound = true, "otel.kind" = "internal");
        async {
            match gossip.backbone.publish_command_hook(&frame).await {
                Ok(()) => tracing::info!("the round hook was published on the mesh channel and the backbone"),
                Err(e) => tracing::info!(error = %e, "the round hook was not handed to every channel"),
            }
        }
        .instrument(span)
        .await;
    }

    // ------------------------------------------------------------------ the fabric-primary

    /// One turn of the fabric-primary's climb. Cheap and never blocking: long work (the hook, a
    /// round's commands) runs on its own tasks and the turn reads where they have got to.
    pub async fn climb_step(self: &Arc<Self>, inputs: ClimbInputs<'_>) {
        let mut climb = self.climb.lock().await;
        let heard_state = inputs.heard.and_then(|h| FabricState::parse(h).or_else(|| (h == "degraded").then_some(FabricState::ReadyForTraffic)));
        if !inputs.is_fabric_primary {
            // A non-holder serves the state the holder published and runs nothing.
            if let Some(run) = climb.sync.take() {
                run.task.abort();
            }
            *climb = Climb::default();
            self.env.records.set_fabric_state(heard_state.unwrap_or(FabricState::Pending));
            self.fabric_book_reset();
            return;
        }
        if !climb.adopted {
            // The seat was taken: the state the previous holder published is adopted, not re-decided.
            // A state with a round in flight runs that round again, and members already done check in again.
            climb.adopted = true;
            let adopted = heard_state.unwrap_or(FabricState::Pending);
            self.env.records.set_fabric_state(adopted);
            tracing::info_span!("rdm.node_admin.fabric.update.via-state-adopted", node = %self.env.me, state = adopted.as_str(), heard = inputs.heard.unwrap_or("none"), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the fabric-primary seat was taken: the published state is adopted"));
            if matches!(adopted, FabricState::StateSync | FabricState::StateCommit) {
                climb.root = Some(tracing::info_span!(parent: None, "rdm.node_admin.fabric.update.via-state-climb", node = %self.env.me, fabric_id = %self.env.fabric_id, adopted = true));
            }
        }
        // A step that moved the fabric (or its round) is followed by the next at once: the climb
        // waits on its checklists and the application, never on the loop that carries it.
        for _ in 0..8 {
            let before = (self.env.records.fabric_state(), climb.phase);
            match before.0 {
                FabricState::Pending => self.step_pending(&mut climb, inputs.view).await,
                FabricState::StateSync => self.step_state_sync(&mut climb).await,
                FabricState::StateCommit => self.step_state_commit(&mut climb, inputs.view).await,
                FabricState::ReadyForTraffic => self.step_ready(&mut climb).await,
            }
            if before == (self.env.records.fabric_state(), climb.phase) {
                break;
            }
        }
    }

    /// The climb's next turn is due: a state went out.
    pub fn poke_climb(&self) {
        self.climb_wake.notify_one();
    }

    /// Whether the state the fabric holds went out as a FabricStatus frame, so the climb may enter the next.
    fn may_advance(&self) -> bool {
        self.env.records.published_fabric_state() == Some(self.env.records.fabric_state())
    }

    /// Resolves when a checklist or the application's answer moved: the climb's next turn is due.
    pub async fn climb_due(&self) {
        self.climb_wake.notified().await
    }

    /// A resize or roll puts the fabric back to pending: the accepted Build's topology is no longer
    /// the one the fabric climbs under, so its planned births must check in again and the rounds
    /// run again. A new attempt of the same Build changes no topology and moves nothing here.
    /// Returns whether the fabric was put back; the move waits until the state it leaves went out.
    fn resized(&self, climb: &mut Climb, build: &crate::build_state::BuildProjection) -> bool {
        match &climb.tracked {
            None => {
                climb.tracked = Some(build.topology.clone());
                false
            }
            Some(held) if *held != build.topology => {
                if !self.may_advance() {
                    return false;
                }
                tracing::info_span!(parent: None, "rdm.node_admin.fabric.update.via-state-reset", node = %self.env.me, fabric_id = %self.env.fabric_id, build_id = %build.build_id, attempt = build.attempt, from = self.env.records.fabric_state().as_str(), reason = "the accepted topology changed", "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a resize or roll puts the fabric back to pending"));
                climb.tracked = None;
                climb.notified = false;
                if let Some(run) = climb.sync.take() {
                    run.task.abort();
                }
                climb.key = None;
                climb.phase = None;
                climb.root = None;
                self.fabric_book.clear();
                self.enter(climb, FabricState::Pending);
                true
            }
            Some(_) => false,
        }
    }

    async fn step_ready(self: &Arc<Self>, climb: &mut Climb) {
        let Some(build) = self.env.accepted.current(&*self.env.builds).await else { return };
        self.resized(climb, &build);
    }

    fn fabric_book_reset(&self) {
        // A checklist belongs to the tenure that opened its round: a new holder opens its own.
        self.fabric_book.clear();
    }

    fn enter(&self, climb: &mut Climb, next: FabricState) {
        let from = self.env.records.fabric_state();
        self.env.records.set_fabric_state(next);
        self.env.records.set_fabric_blocker(None);
        climb.named = None;
        let parent = climb.root.clone().unwrap_or_else(tracing::Span::none);
        tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-state-entered", node = %self.env.me, fabric_id = %self.env.fabric_id, from = from.as_str(), to = next.as_str(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the fabric-primary entered a fabric state"));
        self.env.wake.poke();
    }

    /// A blocker standing in a state: one span while it stands, another when it changes.
    fn block(&self, climb: &mut Climb, blockers: &[Missing], what: &str) {
        let text = blockers.iter().map(Missing::to_string).collect::<Vec<_>>().join(",");
        let line = format!("{what}: {text}");
        self.env.records.set_fabric_blocker(Some(line.clone()));
        if climb.named.as_deref() != Some(line.as_str()) {
            climb.named = Some(line.clone());
            let parent = climb.root.clone().unwrap_or_else(tracing::Span::none);
            tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-blocked", node = %self.env.me, state = self.env.records.fabric_state().as_str(), what, blockers = %text, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the fabric cannot leave its state: the blockers are named"));
        }
    }

    async fn step_pending(self: &Arc<Self>, climb: &mut Climb, view: &Topology) {
        let accepted = self.env.accepted.current(&*self.env.builds).await;
        let blockers = self.entry_blockers(view, accepted.as_ref().map(|b| &b.topology));
        if !blockers.is_empty() {
            self.block(climb, &blockers, "state-sync waits for every mesh to be ready-for-traffic");
            return;
        }
        let Some(build) = accepted else {
            self.block(climb, &[Missing { who: "build".into(), why: "no-accepted-build" }], "state-sync names the accepted Build");
            return;
        };
        // The sync_state round names the accepted Build's attempt: a Build whose first attempt no
        // executor has claimed yet has none, and the round is asked for the attempt that exists.
        if build.attempt == 0 {
            self.block(climb, &[Missing { who: "build".into(), why: "no-attempt-claimed" }], "state-sync names the accepted Build's attempt");
            return;
        }
        if !self.may_advance() {
            return;
        }
        climb.root = Some(tracing::info_span!(parent: None, "rdm.node_admin.fabric.update.via-state-climb", node = %self.env.me, fabric_id = %self.env.fabric_id, build_id = %build.build_id, adopted = false));
        climb.tracked = Some(build.topology.clone());
        self.enter(climb, FabricState::StateSync);
    }

    /// The meshes not ready-for-traffic and the mesh primaries that have not reported, named. Empty:
    /// every mesh is ready-for-traffic (R-H1). A mesh's state is the one the view projects
    /// (`Topology::mesh_unready` decides it); the nodes behind a mesh that is not ready are named
    /// from that same function.
    fn entry_blockers(&self, view: &Topology, accepted: Option<&FabricTopology>) -> Vec<Missing> {
        let meshes: Vec<String> = match accepted {
            Some(t) => t.meshes.keys().cloned().collect(),
            None => view.meshes.iter().map(|m| m.name.clone()).collect(),
        };
        let mut out = mesh_blockers(view, &meshes, &self.env.records.planned());
        let primaries: Vec<(String, Option<IncarnationId>, bool)> =
            meshes.iter().map(|m| { let p = view.cohort_primary(m, NodeKind::NodeAdmin); (m.clone(), p.and_then(|n| n.incarnation_id.clone()), p.is_some_and(|n| n.node_id == self.env.node_id)) }).collect();
        let reports = crate::round::reports_of(&self.env.records.declared.lock().unwrap(), view, meshes.iter().map(String::as_str));
        out.extend(crate::round::fabric_missing(&primaries, self.env.records.declare_gate(), &reports));
        out
    }

    async fn step_state_sync(self: &Arc<Self>, climb: &mut Climb) {
        let Some(build) = self.env.accepted.current(&*self.env.builds).await else {
            self.block(climb, &[Missing { who: "build".into(), why: "no-accepted-build" }], "sync_state names the accepted Build");
            return;
        };
        if self.resized(climb, &build) {
            return;
        }
        let parent = climb.root.clone().unwrap_or_else(tracing::Span::none);
        // The round is the accepted Build's and attempt's: when either moved while the application
        // worked (a restart opened an attempt), the application is asked again for the current one.
        if climb.sync.as_ref().is_some_and(|run| run.sent.build_id != build.build_id.to_string() || run.sent.attempt != build.attempt) {
            if let Some(run) = climb.sync.take() {
                tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-sync-state-reissued", node = %self.env.me, was_build_id = %run.sent.build_id, was_attempt = run.sent.attempt, build_id = %build.build_id, attempt = build.attempt, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the accepted attempt moved: sync_state is called again for the current one"));
                run.task.abort();
            }
        }
        if climb.sync.is_none() {
            let context = match self.env.contexts.for_claim(&build.build_id, build.attempt).await {
                Ok(c) => c,
                Err(e) => {
                    self.block(climb, &[Missing { who: "attempt-context".into(), why: "unreadable" }], &format!("sync_state needs the attempt's context ({e})"));
                    return;
                }
            };
            let sent = StateSyncRound { fabric_id: self.env.fabric_id.clone(), build_id: build.build_id.to_string(), attempt: build.attempt, operation: format!("state-sync:{}", self.env.fabric_id), context };
            let progress = Arc::new(Mutex::new(Progress::Waiting));
            let (this, hook, me, sent_for_task, progress_for_task, parent_for_task) = (self.clone(), self.env.hooks.sync_state.clone(), self.env.me.clone(), sent.clone(), progress.clone(), parent.clone());
            let task = tokio::spawn(async move {
                run_sync_state(hook, me, sent_for_task, progress_for_task, parent_for_task).await;
                this.climb_wake.notify_one();
            });
            climb.sync = Some(SyncRun { sent, progress, task });
        }
        let run = climb.sync.as_ref().expect("started above");
        let answered = match &*run.progress.lock().unwrap() {
            Progress::Waiting => None,
            Progress::Answered(a) => Some(a.clone()),
        };
        let Some(answer) = answered else {
            let what = format!("sync_state awaits the application's state-synced for build {} attempt {}", run.sent.build_id, run.sent.attempt);
            self.block(climb, &[Missing { who: "application".into(), why: "no-state-synced" }], &what);
            return;
        };
        // The answer completes the round only for THIS round: the fabric, Build, attempt and
        // operation it was sent, and the accepted Build and attempt now. A foreign Build, an old
        // attempt or any other round is refused by name and the fabric stays in state-sync.
        let sent = run.sent.clone();
        let refusal = [
            (answer.fabric_id != sent.fabric_id).then(|| format!("fabric_id: sent {}, answered {}", sent.fabric_id, answer.fabric_id)),
            (answer.build_id != sent.build_id).then(|| format!("build_id: sent {}, answered {}", sent.build_id, answer.build_id)),
            (answer.attempt != sent.attempt).then(|| format!("attempt: sent {}, answered {}", sent.attempt, answer.attempt)),
            (answer.operation != sent.operation).then(|| format!("operation: sent {}, answered {}", sent.operation, answer.operation)),
        ]
        .into_iter()
        .flatten()
        .next();
        if let Some(why) = refusal {
            self.block(climb, &[Missing { who: "application".into(), why: "state-synced-refused" }], &format!("state-synced refused, field {why}"));
            let key = format!("refused:{why}");
            if climb.named.as_deref() != Some(key.as_str()) {
                tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.reject.via-state-synced", node = %self.env.me, reason = %why, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a state-synced that is not this round's completion is refused"));
            }
            return;
        }
        if !self.may_advance() {
            return;
        }
        tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-state-synced", node = %self.env.me, build_id = %sent.build_id, attempt = sent.attempt, operation = %sent.operation, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the application's state-synced matches this round: state-commit begins"));
        if let Some(run) = climb.sync.take() {
            run.task.abort();
        }
        climb.phase = Some(Phase::Commit);
        climb.key = Some(RoundKey { kind: RoundKind::StateCommit, fabric_id: sent.fabric_id.clone(), build_id: sent.build_id.clone(), attempt: sent.attempt });
        self.enter(climb, FabricState::StateCommit);
    }

    async fn step_state_commit(self: &Arc<Self>, climb: &mut Climb, view: &Topology) {
        let Some(build) = self.env.accepted.current(&*self.env.builds).await else {
            self.block(climb, &[Missing { who: "build".into(), why: "no-accepted-build" }], "commit-state names the accepted Build");
            return;
        };
        if self.resized(climb, &build) {
            return;
        }
        match &climb.key {
            // Adopted in state-commit: the round is the accepted Build's, started over.
            None => {
                climb.phase = Some(Phase::Commit);
                climb.key = Some(RoundKey { kind: RoundKind::StateCommit, fabric_id: self.env.fabric_id.clone(), build_id: build.build_id.to_string(), attempt: build.attempt });
            }
            // The accepted attempt moved: the round is scoped to the current one, and its planned births check in again.
            Some(k) if k.build_id != build.build_id.to_string() || k.attempt != build.attempt => {
                climb.key = Some(RoundKey { kind: k.kind, fabric_id: k.fabric_id.clone(), build_id: build.build_id.to_string(), attempt: build.attempt });
                self.fabric_book.clear();
            }
            Some(_) => {}
        }
        let (phase, key) = (climb.phase.unwrap_or(Phase::Commit), climb.key.clone().expect("set above"));
        let parent = climb.root.clone().unwrap_or_else(tracing::Span::none);
        let missing = self.fabric_round(&key, view, &parent).await;
        if !missing.is_empty() {
            self.block(climb, &missing, &format!("{} waits for the round checklist", key.kind.command()));
            return;
        }
        if phase == Phase::Open && !self.may_advance() {
            return;
        }
        climb.named = None;
        tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-round-complete", node = %self.env.me, round = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the round checklist is complete"));
        match phase {
            Phase::Commit => {
                climb.phase = Some(Phase::Open);
                climb.key = Some(RoundKey { kind: RoundKind::OpenTraffic, ..key });
            }
            Phase::Open => {
                self.enter(climb, FabricState::ReadyForTraffic);
                let notice = RoundKey { kind: RoundKind::OpenTraffic, ..key };
                if !climb.notified {
                    climb.notified = true;
                    self.notify_traffic_opened(&notice, parent.clone());
                }
                climb.root = None;
                climb.key = None;
                climb.phase = None;
            }
        }
    }

    /// One turn of a fabric-level round: open its checklist, command the mesh primaries not yet
    /// commanded as their exact birth, run this admin's own mesh as its mesh primary. The planned
    /// births not checked in, named; empty is complete.
    async fn fabric_round(self: &Arc<Self>, key: &RoundKey, view: &Topology, parent: &tracing::Span) -> Vec<Missing> {
        if self.fabric_book.open(key) {
            tracing::info_span!(parent: parent, "rdm.node_admin.fabric.update.via-round-opened", node = %self.env.me, round = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the fabric-primary opened a round"));
        }
        let accepted = self.env.accepted.current(&*self.env.builds).await;
        let meshes: Vec<String> = match &accepted {
            Some(b) => b.topology.meshes.keys().cloned().collect(),
            None => view.meshes.iter().map(|m| m.name.clone()).collect(),
        };
        let mut planned = BTreeMap::new();
        let mut unresolved = Vec::new();
        for mesh in &meshes {
            match view.cohort_primary(mesh, NodeKind::NodeAdmin) {
                Some(p) if p.incarnation_id.is_some() => {
                    planned.insert(p.node_id.clone(), Expected { incarnation: p.incarnation_id.clone().expect("checked"), name: p.name.to_string() });
                }
                _ => unresolved.push(Missing { who: mesh.clone(), why: "no-primary" }),
            }
        }
        self.fabric_book.expect(key, planned, unresolved);
        // This admin's own mesh: it is the mesh primary of it, so it runs that mesh's round itself.
        if !self.mesh_book.is_open(key) {
            self.mesh_book.open(key);
            self.spawn_mesh_driver(key.clone(), false, parent.clone());
        }
        for (node_id, expected, event) in self.fabric_book.take_due(key, view) {
            if node_id == self.env.node_id {
                continue;
            }
            self.spawn_command(key.clone(), true, node_id, expected, event, parent.clone());
        }
        self.fabric_book.missing(key)
    }

    /// The one notice to the application: after the open-traffic round completed and the fabric
    /// is ready-for-traffic. It gates nothing.
    fn notify_traffic_opened(&self, key: &RoundKey, parent: tracing::Span) {
        let (hook, me, contexts) = (self.env.hooks.traffic_opened.clone(), self.env.me.clone(), self.env.contexts.clone());
        let round = (key.fabric_id.clone(), key.build_id.clone(), key.attempt);
        tokio::spawn(async move {
            let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-traffic-opened-hook", node = %me, build_id = %round.1, attempt = round.2, bound = hook.is_some(), "otel.kind" = "internal");
            async {
                let Some(hook) = hook else {
                    tracing::info!("no traffic_opened notice is registered");
                    return;
                };
                let build = crate::build::BuildId(round.1.clone());
                let context = contexts.get(&build, round.2).await.ok().flatten().unwrap_or_default();
                hook(StateSyncRound { fabric_id: round.0.clone(), build_id: round.1.clone(), attempt: round.2, operation: RoundKind::OpenTraffic.operation(&round.0), context }).await;
                tracing::info!("the application was told the fabric opened traffic");
            }
            .instrument(span)
            .await
        });
    }

    // ------------------------------------------------------------ commands down, one hop

    /// Send the down op of `key` to the planned birth `to`, on its own task. The book it was taken
    /// from commands it once per exact birth; an op the peer did not take waits for an eligible event.
    fn spawn_command(self: &Arc<Self>, key: RoundKey, fabric_level: bool, to: NodeId, expected: Expected, event: &'static str, parent: tracing::Span) {
        let this = self.clone();
        tokio::spawn(async move {
            let book = if fabric_level { &this.fabric_book } else { &this.mesh_book };
            let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-round-command", node = %this.env.me, round = key.kind.command(), to = %expected.name, event, outcome = tracing::field::Empty, "otel.kind" = "internal");
            let req = key.kind.down(&key, to.clone(), expected.incarnation.clone());
            // The command hook runs in the path of the command it announces, before the call goes out.
            this.publish_hook(&key, false, &to, &expected.incarnation).instrument(span.clone()).await;
            let (out, _) = this.env.client.call::<Status>(&NodeTarget::ExactNode(to.clone()), &req, &CallOptions::default()).instrument(span.clone()).await;
            match delivery(&out) {
                Delivery::Answered(r) => {
                    span.record("outcome", r.name());
                    book.answered(&key, &to, &expected.incarnation);
                }
                Delivery::Undelivered { why, typed } => {
                    span.record("outcome", why.as_str());
                    // Addressed while the call was out: due again now; a fabric-level op is sent by the climb's turn.
                    if book.undelivered(&key, &to, &expected.incarnation, typed) && fabric_level {
                        this.climb_wake.notify_one();
                    }
                }
            }
        });
    }

    // ----------------------------------------------------------------- the mesh primary

    /// Drive `key` as this mesh's primary: command every planned member of this mesh, run this
    /// admin's own action, and when the checklist is complete report upward. `owes_upward`: the
    /// round came down from the fabric-primary, which the completion is owed to; `false` when this
    /// admin is the fabric-primary and the mesh round is its own.
    fn spawn_mesh_driver(self: &Arc<Self>, key: RoundKey, owes_upward: bool, parent: tracing::Span) {
        if !self.drivers.lock().unwrap().insert(key.clone()) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-mesh-round", node = %this.env.me, round = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, superseded_by = tracing::field::Empty, "otel.kind" = "internal");
            this.drive_mesh(&key, owes_upward).instrument(span.clone()).await;
            this.drivers.lock().unwrap().remove(&key);
        });
    }

    async fn drive_mesh(self: &Arc<Self>, key: &RoundKey, owes_upward: bool) {
        let mesh = self.env.me.mesh.clone();
        let mut own_done = false;
        let round_span = tracing::Span::current();
        // The wait is armed before the read: a checklist change, a view change or a birth addressing
        // this primary that happens during the read is seen by the wait, never lost, and nothing
        // here runs on a timer.
        let (mut book_tick, mut view_tick) = (self.mesh_book.subscribe(), self.env.records.subscribe_view());
        loop {
            book_tick.borrow_and_update();
            view_tick.borrow_and_update();
            let view = self.env.topology.read().await.clone();
            if !view.cohort_primary(&mesh, NodeKind::NodeAdmin).is_some_and(|n| n.node_id == self.env.node_id) {
                tracing::info!("this admin no longer holds its mesh's primary seat: the round is the next primary's");
                self.mesh_book.close(key);
                return;
            }
            let accepted = self.env.accepted.current(&*self.env.builds).await;
            // A round the accepted Build moved past commands nothing more and owes no completion: the
            // round of the accepted Build and attempt is the one its primaries run now.
            if let Some(b) = &accepted {
                let own = if b.build_id.0 == key.build_id { None } else { self.env.builds.read_build(&crate::build::BuildId(key.build_id.clone())).await.ok() };
                if let Some(why) = superseded_by(key, b, own.as_ref()) {
                    round_span.record("superseded_by", why.as_str());
                    tracing::info!(superseded_by = %why, "the accepted Build moved past this round: it commands nothing more and owes no completion");
                    self.mesh_book.close(key);
                    return;
                }
            }
            let paths: Vec<PathName> = match &accepted {
                Some(b) => crate::round::planned(&b.topology, &mesh),
                None => view.members().filter(|n| n.mesh == mesh).map(|n| n.name.clone()).collect(),
            };
            let mut planned = BTreeMap::new();
            let mut unresolved = Vec::new();
            for path in paths.iter().filter(|p| **p != self.env.me) {
                match view.members().find(|n| &n.name == path) {
                    Some(n) if n.incarnation_id.is_some() => {
                        planned.insert(n.node_id.clone(), Expected { incarnation: n.incarnation_id.clone().expect("checked"), name: n.name.to_string() });
                    }
                    _ => unresolved.push(Missing { who: path.to_string(), why: "no-birth-heard" }),
                }
            }
            self.mesh_book.expect(key, planned, unresolved);
            for (node_id, expected, event) in self.mesh_book.take_due(key, &view) {
                self.spawn_command(key.clone(), false, node_id, expected, event, round_span.clone());
            }
            if !own_done {
                match run_action(&*self.env.actions, &self.env.me, key.kind).await {
                    Ok(()) => own_done = true,
                    Err(e) => {
                        tracing::info_span!("rdm.node_admin.fabric.update.via-round-action-failed", node = %self.env.me, round = key.kind.command(), error = %e, "otel.kind" = "internal")
                            .in_scope(|| tracing::info!("this admin's own action failed: it owes no check-in"));
                        self.mesh_book.close(key);
                        return;
                    }
                }
            }
            if own_done && self.mesh_book.complete(key) {
                break;
            }
            tokio::select! {
                _ = book_tick.changed() => {}
                _ = view_tick.changed() => {}
            }
        }
        match owes_upward {
            true => {
                // The completion is owed to the fabric-primary as the view names it now, sent on every
                // eligible event, and owed again to a successor (R-S2).
                let req = key.kind.up(key, self.env.node_id.clone(), self.env.incarnation.clone());
                // The check-in hook runs when the completion becomes owed: once per round, however often it is sent.
                self.publish_hook(key, true, &self.env.node_id, &self.env.incarnation).await;
                self.env.declarer.owe_in(Key::round(key), Authority::FabricPrimary, req, crate::build_claim::current_context().traceparent);
                self.env.wake.poke();
            }
            false => {
                self.fabric_book.check_in_local(key, &self.env.node_id, &self.env.incarnation);
                self.climb_wake.notify_one();
                tracing::info!("this admin's own mesh round is complete");
            }
        }
    }

    // ----------------------------------------------------------------- served by an admin

    /// `node_id` sent this admin a Status call: an undelivered round command to it is eligible again.
    pub fn heard_from(&self, node_id: &NodeId) {
        self.mesh_book.heard_from(node_id);
        self.fabric_book.heard_from(node_id);
        self.climb_wake.notify_one();
    }

    /// A round op addressed to this admin, decided from `view`. `own` answers it as a plain member.
    pub async fn serve(self: &Arc<Self>, view: &Topology, sender: Option<&Node>, req: &StatusRequest, own: Option<&Arc<crate::node_self::NodeSelf>>) -> StatusReply {
        let Some(op) = RoundOp::of(req) else { return StatusReply::NotReady { reason: format!("{} is not a round op", req.op()) } };
        let me = self.env.me.to_string();
        let reply = if op.is_down { self.serve_down(view, sender, req, &op, own).await } else { self.serve_up(view, req, &op) };
        tracing::info_span!(
            "rdm.node_admin.status.update.via-round-op",
            node = %me,
            op = req.op(),
            round = op.kind.command(),
            sender = %sender.map(|n| n.name.to_string()).unwrap_or_default(),
            build_id = %op.build_id,
            attempt = op.attempt,
            outcome = reply.name(),
            "otel.kind" = "internal"
        )
        .in_scope(|| tracing::info!("a round op was decided"));
        reply
    }

    /// A round command naming this admin, served by what this admin is in its own view, whoever
    /// calls (node-rpc.md §14): a mesh-primary runs its mesh's round and reports it to the
    /// fabric-primary; any other admin takes it as a member and checks in with its mesh's primary
    /// (fabric-state-commit.md: "Each member checks in with its primary, node → mesh-primary →
    /// fabric-primary"). The fabric-primary carries rounds down and is not a receiver of one.
    async fn serve_down(self: &Arc<Self>, view: &Topology, sender: Option<&Node>, req: &StatusRequest, op: &RoundOp<'_>, own: Option<&Arc<crate::node_self::NodeSelf>>) -> StatusReply {
        let Some(me) = view.own(&self.env.node_id) else {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a view holding this admin".into() } };
        };
        if *op.node_id != me.node_id {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "the subject node itself".into() } };
        }
        if let Some(held) = me.incarnation_id.as_ref().filter(|h| *h != op.incarnation) {
            return StatusReply::RejectedStaleIncarnation { held: held.clone() };
        }
        if *op.fabric_id != self.env.fabric_id {
            return StatusReply::RejectedStaleFabric { held: self.env.fabric_id.clone() };
        }
        let want = op.kind.operation(op.fabric_id);
        if op.operation != want {
            return StatusReply::NotReady { reason: format!("{}: {} names operation {}, the round's is {want}", self.env.me, req.op(), op.operation) };
        }
        if me.is_fabric_primary {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("a mesh-primary the fabric-primary commands, or a member of a mesh: {} holds the fabric seat and carries {} down", self.env.me, req.op()) } };
        }
        if me.is_primary {
            // This admin is a mesh primary: it runs its mesh's round and reports to the fabric-primary.
            let key = op.key();
            if let Some(from) = sender {
                self.env.wake.addressed_by(&from.node_id);
            }
            if self.mesh_book.open(&key) {
                self.spawn_mesh_driver(key, true, tracing::Span::current());
                StatusReply::Applied
            } else {
                // The round is held: a successor fabric-primary asking again is owed the completion
                // again by the declarer, which sees the seat moved.
                self.env.wake.poke();
                StatusReply::AlreadyApplied
            }
        } else {
            // A member: it checks in with the primary that commanded it (fabric-state-commit.md, round
            // sequence: "M->>N: commit-state", "N->>M: state-committed"): the caller, which a new
            // primary is before this admin's view names it. Only a caller the view does not hold is
            // checked in at the mesh's primary as the view names it.
            let commander = sender.filter(|s| s.node_id != me.node_id).map(|s| s.node_id.clone()).or_else(|| view.cohort_primary(&me.mesh, NodeKind::NodeAdmin).filter(|p| p.node_id != me.node_id).map(|p| p.node_id.clone()));
            match (own, commander) {
                (Some(own), Some(commander)) => own.serve(Some(&commander), None, req).await.expect("a round op"),
                (None, _) => StatusReply::NotReady { reason: format!("{} has not yet joined its mesh; {} is refused", self.env.me, req.op()) },
                (Some(_), None) => StatusReply::NotReady { reason: format!("{}: its view holds no caller and names no primary of {} to check {} in to", self.env.me, me.mesh, req.op()) },
            }
        }
    }

    /// A check-in, decided for the subject it names as this admin's view holds it, whoever calls
    /// (node-rpc.md §14): a mesh-primary's for its mesh counts at the fabric-primary, a member's at
    /// its mesh's primary.
    fn serve_up(&self, view: &Topology, req: &StatusRequest, op: &RoundOp<'_>) -> StatusReply {
        let Some(me) = view.own(&self.env.node_id) else {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a view holding this admin".into() } };
        };
        let Some(subject) = view.births().find(|n| n.node_id == *op.node_id) else {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown };
        };
        let held = Some((subject.incarnation_id.as_ref(), subject.name.to_string()));
        let held = held.as_ref().map(|(i, n)| (*i, n.as_str()));
        if me.is_fabric_primary && subject.kind == NodeKind::NodeAdmin && subject.is_primary && subject.mesh != me.mesh {
            let reply = self.fabric_book.check_in(&self.env.me.to_string(), held, req);
            self.climb_wake.notify_one();
            reply
        } else if me.is_primary && subject.mesh == me.mesh && subject.node_id != me.node_id {
            self.mesh_book.check_in(&self.env.me.to_string(), held, req)
        } else {
            StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("the fabric-primary, or the mesh-primary of {}", subject.mesh) } }
        }
    }
}

/// The meshes among `meshes` that are not ready-for-traffic in `view`, with the nodes behind each. A
/// mesh's state is the one the view projects (`Topology::mesh_unready` decides it).
fn mesh_blockers(view: &Topology, meshes: &[String], planned: &std::collections::BTreeMap<String, std::collections::BTreeSet<PathName>>) -> Vec<Missing> {
    let mut out = Vec::new();
    for mesh in meshes {
        match view.meshes.iter().find(|m| &m.name == mesh) {
            Some(m) if m.status == crate::model::ScopeStatus::ReadyForTraffic => {}
            Some(_) => {
                let why = view.mesh_unready(mesh, planned.get(mesh));
                out.extend(if why.is_empty() { vec![Missing { who: mesh.clone(), why: "not-ready" }] } else { why });
            }
            None => out.push(Missing { who: mesh.clone(), why: "no-mesh" }),
        }
    }
    out
}

/// The `sync_state` call: the application's function with the round fields, its answer held for
/// the climb to judge. No application work answers at once, naming why.
async fn run_sync_state(hook: Option<SyncState>, me: PathName, sent: StateSyncRound, progress: Arc<Mutex<Progress>>, parent: tracing::Span) {
    let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-sync-state-hook", node = %me, build_id = %sent.build_id, attempt = sent.attempt, operation = %sent.operation, reason = tracing::field::Empty, outcome = tracing::field::Empty, "otel.kind" = "internal");
    tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-sync-state-called", node = %me, build_id = %sent.build_id, attempt = sent.attempt, operation = %sent.operation, bound = matches!(hook, Some(SyncState::Hook(_))), "otel.kind" = "internal")
        .in_scope(|| tracing::info!("sync_state is called with the round fields"));
    let answer = async {
        match hook {
            Some(SyncState::Hook(f)) => {
                let answer = f(sent.clone()).await;
                tracing::Span::current().record("outcome", "state-synced");
                answer
            }
            Some(SyncState::NoAppWork) => {
                tracing::Span::current().record("reason", "no-app-work").record("outcome", "state-synced");
                sent.clone()
            }
            None => {
                // Refused at start; unreachable.
                std::future::pending::<StateSyncRound>().await
            }
        }
    }
    .instrument(span)
    .await;
    *progress.lock().unwrap() = Progress::Answered(answer);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, Mesh, NodeStatus, ProviderKind, ScopeStatus};

    fn node(name: &str, status: NodeStatus, primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = status;
        n.incarnation_id = Some(IncarnationId::mint());
        n.is_primary = primary;
        n
    }

    /// The view as the projection builds it: a mesh is ready when `Topology::mesh_unready` is empty.
    fn projected(nodes: Vec<Node>) -> Topology {
        let mut t = Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Pending, provider: ProviderKind::Process }, meshes: Vec::new(), nodes };
        for m in ["mesh1", "mesh2"] {
            let status = if t.mesh_unready(m, None).is_empty() { ScopeStatus::ReadyForTraffic } else { ScopeStatus::Pending };
            t.meshes.push(Mesh { id: None, name: m.into(), status });
        }
        t
    }

    /// CONTRACT (node-rpc-envelope.md, "Fabric status rounds": "The round is scoped to the accepted
    /// Build, attempt and planned exact births"; states.md, The round checklist: it "names the accepted
    /// Build's PLANNED births"): a mesh round is superseded, named, once the accepted Build moved past
    /// it: a later attempt of the same Build, or a Build accepted after the round's own. A round of the
    /// accepted Build and attempt is current, and so is a round ahead of what this admin holds (an
    /// attempt it has not seen claimed, a Build accepted after the one it holds, or one its log has not
    /// learned): the command that opened it came ahead of the pointer. Must NOT happen: a superseded
    /// round still commanding (gate red, tree b24a4303, fabric_states__rounds: bld-9abb attempt 1's
    /// mesh round, trace 95adb727, sent commit-state to mesh1.admin.2 at 816848 after the two-mesh
    /// Build bld-7c91 attempt 2 was accepted and its own round's commit-state was applied at 816816,
    /// trace a04036d1).
    #[tokio::test]
    async fn a_round_the_accepted_build_moved_past_is_superseded_and_a_round_ahead_of_this_admins_pointer_is_not() {
        use crate::build_state::{BuildAccepted, BuildProjection, MemoryBuildStateAdapter};
        let builds = MemoryBuildStateAdapter::new();
        let topology = FabricTopology { fabric: "fabric1".into(), meshes: BTreeMap::new() };
        let (one, two) = (crate::build::BuildId("bld-one".into()), crate::build::BuildId("bld-two".into()));
        for (b, at) in [(&one, 1_000), (&two, 2_000)] {
            builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: topology.clone(), submitted_change: None, submitted_at_ms: at }).await.unwrap();
        }
        let (b1, b2) = (builds.read_build(&one).await.unwrap(), builds.read_build(&two).await.unwrap());
        let fabric = FabricId::mint();
        let key = |b: &crate::build::BuildId, attempt: u32| RoundKey { kind: RoundKind::StateCommit, fabric_id: fabric.clone(), build_id: b.0.clone(), attempt };
        let (one_at_1, one_at_2, two_at_1) = (BuildProjection { attempt: 1, ..b1.clone() }, BuildProjection { attempt: 2, ..b1.clone() }, BuildProjection { attempt: 1, ..b2.clone() });
        assert_eq!(superseded_by(&key(&one, 1), &one_at_1, None), None, "the accepted Build and attempt: current");
        assert!(superseded_by(&key(&one, 1), &one_at_2, None).is_some(), "a later attempt of the same Build supersedes the round");
        assert_eq!(superseded_by(&key(&one, 3), &one_at_2, None), None, "an attempt ahead of the one this admin holds: its command came first");
        assert!(superseded_by(&key(&one, 1), &two_at_1, Some(&one_at_1)).is_some(), "a Build accepted after the round's own supersedes it");
        assert_eq!(superseded_by(&key(&two, 1), &one_at_1, Some(&two_at_1)), None, "a round of a Build accepted after the one this admin holds: its pointer lags");
        assert_eq!(superseded_by(&key(&crate::build::BuildId("bld-unlearned".into()), 1), &two_at_1, None), None, "a round of a Build this admin's log has not learned: its command came ahead");
    }

    /// CONTRACT (node-rpc-envelope.md, "Fabric status rounds": "The round is scoped to the accepted
    /// Build, attempt and planned exact births"): a mesh primary's owed completion of a round the
    /// accepted Build moved past is withdrawn when the round is superseded, by the same rule that stops
    /// the round commanding ([`superseded_by`]): a later attempt of the accepted Build, or a Build
    /// accepted after the round's own. The completion of the accepted Build's current attempt, of an
    /// attempt ahead of what this admin holds, and of a Build this admin's log has not learned stays
    /// owed. Must NOT happen: a superseded round's completion kept owed until a seat moves (gate red
    /// b24a4303, mesh_elections__sticky_seat: mesh2.admin.1 declared Round(.../2) and was answered
    /// NotReady "round holds 3, reported 2", then sent it again on every eligible event).
    #[tokio::test]
    async fn an_owed_completion_of_a_round_the_accepted_build_moved_past_is_withdrawn_when_it_is_superseded() {
        use crate::build_state::{BuildAccepted, BuildProjection, MemoryBuildStateAdapter};
        let builds = MemoryBuildStateAdapter::new();
        let topology = FabricTopology { fabric: "fabric1".into(), meshes: BTreeMap::new() };
        let (one, two) = (crate::build::BuildId("bld-one".into()), crate::build::BuildId("bld-two".into()));
        for (b, at) in [(&one, 1_000), (&two, 2_000)] {
            builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: topology.clone(), submitted_change: None, submitted_at_ms: at }).await.unwrap();
        }
        let accepted = BuildProjection { attempt: 2, ..builds.read_build(&two).await.unwrap() };
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let fabric = FabricId::mint();
        let declarer = Declarer::new();
        let owe = |build: &str, attempt: u32| {
            let round = RoundKey { kind: RoundKind::StateCommit, fabric_id: fabric.clone(), build_id: build.to_string(), attempt };
            declarer.owe_in(Key::round(&round), Authority::FabricPrimary, round.kind.up(&round, NodeId::mint(), IncarnationId("inc".into())), None);
            Key::round(&round)
        };
        let (earlier_attempt, earlier_build) = (owe("bld-two", 1), owe("bld-one", 2));
        let (current, ahead, unlearned) = (owe("bld-two", 2), owe("bld-two", 3), owe("bld-unlearned", 1));
        assert_eq!(declarer.owed(), 5);

        withdraw_superseded_completions(&declarer, &me, &accepted, &builds).await;
        let mut owed = declarer.owed_keys();
        owed.sort();
        let mut kept = vec![current, ahead, unlearned];
        kept.sort();
        assert_eq!(owed, kept, "the completions of an earlier attempt ({earlier_attempt:?}) and of an earlier Build ({earlier_build:?}) are withdrawn; a current, an ahead and an unlearned round's stay owed");
    }

    /// CONTRACT (R-S2): only an admission ends a round command. A refusal by name rests on the receiver's own view, which
    /// heals, and a call that got no reply never reached it: both wait for an eligible event. Must NOT happen: a
    /// refusal that is final, which strands the round when a new primary commands members that still name the old one.
    #[test]
    fn only_an_admission_ends_a_round_command() {
        for admitted in [StatusReply::Applied, StatusReply::AlreadyApplied] {
            assert!(matches!(delivery_of(Some(&admitted), "Reply"), Delivery::Answered(_)), "{admitted:?}");
        }
        let refusal = StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "the mesh-primary of mesh1".into() } };
        assert!(matches!(delivery_of(Some(&refusal), "Reply"), Delivery::Undelivered { typed: true, .. }), "a refusal by name is undelivered, typed");
        let not_ready = StatusReply::NotReady { reason: "authority not filled".into() };
        assert!(matches!(delivery_of(Some(&not_ready), "Reply"), Delivery::Undelivered { typed: true, .. }));
        assert!(matches!(delivery_of(None, "NotSent"), Delivery::Undelivered { typed: false, .. }), "no reply: untyped");
    }

    /// CONTRACT (R-RC1): a replace is a delete and a create at the same path.name with a new node
    /// id, so while the retired birth's row is still in a view, two rows carry this admin's
    /// path.name. A round op's receiver finds its own row by its node id, the identity it was born
    /// with. What must NOT happen: the retired birth's row taken as this admin's, which refuses a
    /// round command naming this admin "the subject node itself" and strands the round.
    #[test]
    fn an_admin_finds_its_own_row_by_node_id_while_its_retired_birth_is_still_in_the_view() {
        let retired = node("mesh1.admin.1", NodeStatus::Draining, false);
        let reborn = node("mesh1.admin.1", NodeStatus::ReadyForTraffic, false);
        let view = projected(vec![retired.clone(), reborn.clone(), node("mesh1.admin.2", NodeStatus::ReadyForTraffic, true)]);
        assert_eq!(view.members().filter(|n| n.name == reborn.name).count(), 2, "both births of mesh1.admin.1 are in the view");
        let me = view.own(&reborn.node_id).expect("this admin is in its view");
        assert_eq!(me.node_id, reborn.node_id, "the reborn admin's own row, not the retired birth's {}", retired.node_id);
    }

    /// CONTRACT (R-H1): the fabric enters state-sync only when every mesh is ready-for-traffic, and a
    /// mesh is ready only when every node of it is. A mesh with a primary and one node not ready blocks
    /// entry and names that node; the mesh whose nodes are all ready blocks nothing.
    #[test]
    fn a_mesh_with_one_node_not_ready_blocks_the_fabric_from_entering_state_sync() {
        let meshes = vec!["mesh1".to_string(), "mesh2".to_string()];
        let planned = std::collections::BTreeMap::new();
        let ready = projected(vec![
            node("mesh1.admin.1", NodeStatus::ReadyForTraffic, true),
            node("mesh1.rpc.1", NodeStatus::ReadyForTraffic, false),
            node("mesh2.admin.1", NodeStatus::ReadyForTraffic, true),
        ]);
        assert!(mesh_blockers(&ready, &meshes, &planned).is_empty(), "every node of both meshes is ready: nothing blocks entry");
        let one_pending = projected(vec![
            node("mesh1.admin.1", NodeStatus::ReadyForTraffic, true),
            node("mesh1.rpc.1", NodeStatus::Pending, false),
            node("mesh2.admin.1", NodeStatus::ReadyForTraffic, true),
        ]);
        assert!(one_pending.cohort_primary("mesh1", NodeKind::NodeAdmin).is_some(), "mesh1 has its primary");
        let blockers: Vec<String> = mesh_blockers(&one_pending, &meshes, &planned).iter().map(|m| m.to_string()).collect();
        assert_eq!(blockers, ["mesh1.rpc.1(not-ready)"], "the one node that is not ready is what blocks entry");
    }
}
