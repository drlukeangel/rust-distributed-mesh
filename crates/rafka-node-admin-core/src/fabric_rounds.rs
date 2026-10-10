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
use crate::model::{FabricId, IncarnationId, Node, NodeId, NodeKind, NodeStatus, PathName};
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

/// Run the action of `kind` for `node` under its span.
pub(crate) async fn run_action(actions: &dyn RoundActions, node: &PathName, kind: RoundKind) -> Result<(), String> {
    match kind {
        RoundKind::StateCommit => actions.commit_state(node).await,
        RoundKind::OpenTraffic => actions.open_traffic(node).await,
    }
}

/// How a call to a round peer ended.
enum Delivery {
    /// The peer answered with a typed reply the round can act on (it admitted or refused by name).
    Answered(StatusReply),
    /// The peer was not reached, or said it is not ready: the same op is sent again.
    Undelivered(String),
}

/// A reply that names why the op is refused: sending it again changes nothing.
fn typed_refusal(r: &StatusReply) -> bool {
    matches!(
        r,
        StatusReply::RejectedStaleIncarnation { .. }
            | StatusReply::RejectedStaleMesh { .. }
            | StatusReply::RejectedStaleFabric { .. }
            | StatusReply::RejectedNotAuthority { .. }
            | StatusReply::RejectedInvalidNodeTransition { .. }
            | StatusReply::RejectedInvalidMeshTransition { .. }
            | StatusReply::RejectedUnmatchedCompletion { .. }
    )
}

fn delivery(out: &RpcOutcome<StatusReply>) -> Delivery {
    match out.reply().map(|r| r.value()) {
        Some(r @ (StatusReply::Applied | StatusReply::AlreadyApplied)) => Delivery::Answered(r.clone()),
        Some(r) if typed_refusal(r) => Delivery::Answered(r.clone()),
        Some(other) => Delivery::Undelivered(format!("{other:?}")),
        None => Delivery::Undelivered(out.name().to_string()),
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

/// One admin's rounds: as the fabric-primary the climb, as a mesh primary the commands to its
/// members and its completion up, as a member the check-in.
pub struct FabricRounds {
    env: RoundEnv,
    /// The check-ins of the members of this admin's mesh, as its mesh primary.
    pub mesh_book: RoundBook,
    /// The check-ins of the mesh primaries, as the fabric-primary.
    pub fabric_book: RoundBook,
    drivers: Mutex<HashSet<RoundKey>>,
    climb: tokio::sync::Mutex<Climb>,
    /// Poked when a check-in, the application's answer or this admin's own mesh round arrives.
    climb_wake: tokio::sync::Notify,
}

impl FabricRounds {
    /// The rounds of the admin `env` describes.
    pub(crate) fn new(env: RoundEnv) -> Arc<Self> {
        Arc::new(Self { env, mesh_book: RoundBook::default(), fabric_book: RoundBook::default(), drivers: Mutex::new(HashSet::new()), climb: tokio::sync::Mutex::new(Climb::default()), climb_wake: tokio::sync::Notify::new() })
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
        if !self.may_advance() {
            return;
        }
        climb.root = Some(tracing::info_span!(parent: None, "rdm.node_admin.fabric.update.via-state-climb", node = %self.env.me, fabric_id = %self.env.fabric_id, build_id = %build.build_id, adopted = false));
        climb.tracked = Some(build.topology.clone());
        self.enter(climb, FabricState::StateSync);
    }

    /// The planned births not ready-for-traffic and the mesh primaries that have not reported,
    /// named. Empty: every mesh is ready-for-traffic (R-H1).
    fn entry_blockers(&self, view: &Topology, accepted: Option<&FabricTopology>) -> Vec<Missing> {
        let meshes: Vec<String> = match accepted {
            Some(t) => t.meshes.keys().cloned().collect(),
            None => view.meshes.iter().map(|m| m.name.clone()).collect(),
        };
        let mut out = Vec::new();
        for mesh in &meshes {
            let paths: Vec<PathName> = match accepted {
                Some(t) => crate::round::planned(t, mesh),
                None => view.members().filter(|n| &n.mesh == mesh).map(|n| n.name.clone()).collect(),
            };
            for path in paths {
                match view.members().find(|n| n.name == path) {
                    None => out.push(Missing { who: path.to_string(), why: "no-birth-heard" }),
                    Some(n) if n.status != NodeStatus::ReadyForTraffic => out.push(Missing { who: path.to_string(), why: "not-ready" }),
                    Some(_) => {}
                }
            }
        }
        let primaries: Vec<(String, Option<IncarnationId>, bool)> =
            meshes.iter().map(|m| { let p = view.cohort_primary(m, NodeKind::NodeAdmin); (m.clone(), p.and_then(|n| n.incarnation_id.clone()), p.is_some_and(|n| n.name == self.env.me)) }).collect();
        let reports = self.env.records.declared.lock().unwrap().reports.clone();
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
            self.spawn_mesh_driver(key.clone(), None, parent.clone());
        }
        for (node_id, expected) in self.fabric_book.take_uncommanded(key) {
            if node_id == self.env.node_id {
                continue;
            }
            self.spawn_command(key.clone(), true, node_id, expected, parent.clone());
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
                let context = contexts.get(&build, round.2).ok().flatten().unwrap_or_default();
                hook(StateSyncRound { fabric_id: round.0.clone(), build_id: round.1.clone(), attempt: round.2, operation: RoundKind::OpenTraffic.operation(&round.0), context }).await;
                tracing::info!("the application was told the fabric opened traffic");
            }
            .instrument(span)
            .await
        });
    }

    // ------------------------------------------------------------ commands down, one hop

    /// Send the down op of `key` to the planned birth `to`, on its own task. The book it was taken
    /// from commands it once per exact birth; an op the peer did not take is sent again.
    fn spawn_command(self: &Arc<Self>, key: RoundKey, fabric_level: bool, to: NodeId, expected: Expected, parent: tracing::Span) {
        let this = self.clone();
        tokio::spawn(async move {
            let book = if fabric_level { &this.fabric_book } else { &this.mesh_book };
            let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-round-command", node = %this.env.me, round = key.kind.command(), to = %expected.name, outcome = tracing::field::Empty, "otel.kind" = "internal");
            let req = key.kind.down(&key, to.clone(), expected.incarnation.clone());
            let (out, _) = this.env.client.call::<Status>(&NodeTarget::ExactNode(to.clone()), &req, &CallOptions::default()).instrument(span.clone()).await;
            match delivery(&out) {
                Delivery::Answered(r) => {
                    span.record("outcome", r.name());
                }
                Delivery::Undelivered(why) => {
                    span.record("outcome", why.as_str());
                    book.uncommand(&key, &to, &expected.incarnation);
                }
            }
        });
    }

    // ----------------------------------------------------------------- the mesh primary

    /// Drive `key` as this mesh's primary: command every planned member of this mesh, run this
    /// admin's own action, and when the checklist is complete report upward. `commander` is the
    /// fabric-primary that sent the down op; `None` when this admin is the fabric-primary and the
    /// mesh round is its own.
    fn spawn_mesh_driver(self: &Arc<Self>, key: RoundKey, commander: Option<NodeId>, parent: tracing::Span) {
        if !self.drivers.lock().unwrap().insert(key.clone()) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-mesh-round", node = %this.env.me, round = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, "otel.kind" = "internal");
            this.drive_mesh(&key, commander).instrument(span.clone()).await;
            this.drivers.lock().unwrap().remove(&key);
        });
    }

    async fn drive_mesh(self: &Arc<Self>, key: &RoundKey, commander: Option<NodeId>) {
        let mesh = self.env.me.mesh.clone();
        let mut own_done = false;
        let round_span = tracing::Span::current();
        loop {
            let view = self.env.topology.read().await.clone();
            if !view.cohort_primary(&mesh, NodeKind::NodeAdmin).is_some_and(|n| n.name == self.env.me) {
                tracing::info!("this admin no longer holds its mesh's primary seat: the round is the next primary's");
                self.mesh_book.close(key);
                return;
            }
            let accepted = self.env.accepted.current(&*self.env.builds).await;
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
            for (node_id, expected) in self.mesh_book.take_uncommanded(key) {
                self.spawn_command(key.clone(), false, node_id, expected, round_span.clone());
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
                _ = self.mesh_book.changed() => {}
                _ = tokio::time::sleep(rafka_mesh_entity::cadence::gossip_interval()) => {}
            }
        }
        match commander {
            Some(_) => {
                // The completion is owed to the fabric-primary as the view names it now, sent on every
                // eligible event, and owed again to a successor (R-S2).
                let req = key.kind.up(key, self.env.node_id.clone(), self.env.incarnation.clone());
                self.env.declarer.owe_in(Key::Round(format!("{}/{}/{}/{}", key.kind.completion(), key.fabric_id, key.build_id, key.attempt)), Authority::FabricPrimary, req, crate::build_claim::current_context().traceparent);
                self.env.wake.poke();
            }
            None => {
                self.fabric_book.check_in_local(key, &self.env.node_id, &self.env.incarnation);
                self.climb_wake.notify_one();
                tracing::info!("this admin's own mesh round is complete");
            }
        }
    }

    // ----------------------------------------------------------------- served by an admin

    /// A round op addressed to this admin, decided from `view`. `own` answers it as a plain member.
    pub async fn serve(self: &Arc<Self>, view: &Topology, sender: Option<&Node>, req: &StatusRequest, own: Option<&Arc<crate::node_self::NodeSelf>>) -> StatusReply {
        let Some(op) = RoundOp::of(req) else { return StatusReply::NotReady { reason: format!("{} is not a round op", req.op()) } };
        let me = self.env.me.to_string();
        let reply = if op.is_down { self.serve_down(view, sender, req, &op, own).await } else { self.serve_up(view, sender, req) };
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

    async fn serve_down(self: &Arc<Self>, view: &Topology, sender: Option<&Node>, req: &StatusRequest, op: &RoundOp<'_>, own: Option<&Arc<crate::node_self::NodeSelf>>) -> StatusReply {
        let Some(me) = view.members().find(|n| n.name == self.env.me) else {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a view holding this admin".into() } };
        };
        let Some(sender) = sender else { return StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: "unknown peer".into() } } };
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
        if sender.is_fabric_primary && sender.name != me.name && me.is_primary {
            // This admin is a mesh primary and the fabric-primary commands its mesh.
            let key = op.key();
            self.env.wake.addressed_by(&sender.node_id);
            if self.mesh_book.open(&key) {
                self.spawn_mesh_driver(key, Some(sender.node_id.clone()), tracing::Span::current());
                StatusReply::Applied
            } else {
                // The round is held: a successor fabric-primary asking again is owed the completion
                // again by the declarer, which sees the seat moved.
                self.env.wake.poke();
                StatusReply::AlreadyApplied
            }
        } else if sender.kind == NodeKind::NodeAdmin && sender.is_primary && sender.mesh == me.mesh && sender.name != me.name {
            match own {
                Some(own) => own.serve(&sender.node_id, req).await.expect("a round op"),
                None => StatusReply::NotReady { reason: format!("{} has not yet joined its mesh; {} from {} is refused", self.env.me, req.op(), sender.name) },
            }
        } else {
            StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }
        }
    }

    fn serve_up(&self, view: &Topology, sender: Option<&Node>, req: &StatusRequest) -> StatusReply {
        let Some(me) = view.members().find(|n| n.name == self.env.me) else {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a view holding this admin".into() } };
        };
        let who = sender.map(|n| (&n.node_id, n.incarnation_id.as_ref(), n.name.to_string()));
        let who_ref = who.as_ref().map(|(a, b, c)| (*a, *b, c.as_str()));
        match sender {
            Some(s) if me.is_fabric_primary && s.kind == NodeKind::NodeAdmin && s.is_primary && s.mesh != me.mesh => {
                let reply = self.fabric_book.check_in(&self.env.me.to_string(), who_ref, req);
                self.climb_wake.notify_one();
                reply
            }
            Some(s) if me.is_primary && s.mesh == me.mesh && s.name != me.name => self.mesh_book.check_in(&self.env.me.to_string(), who_ref, req),
            _ => StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "the fabric-primary, or the mesh-primary of the sender's mesh".into() } },
        }
    }
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
