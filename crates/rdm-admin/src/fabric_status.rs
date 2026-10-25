//! The fabric states and their checklists, run by the admins of a fabric (states.md;
//! fabric-state-sync.md, fabric-state-commit.md, lifecycles/fabric-ready-for-traffic.md).
//!
//! - The fabric-primary alone moves the fabric: `pending` → `state-sync` (every mesh is
//!   ready-for-traffic) → `state-commit` (the application answered state-synced) →
//!   `ready-for-traffic` (the commit-state and open-traffic checklists both completed). The
//!   FabricStatus frame carrying each state is authored by it only, by the status loop that reads
//!   the state it holds.
//! - A checklist is a command pair down and a completion pair up: fabric-primary → mesh-primary →
//!   member. A primary reports upward only after its own action and every planned member's
//!   check-in; a check-in counts only as the planned exact birth, after the primary received this
//!   checklist's down op. Nothing leaves a checklist by a timer.
//! - The application work is behind two hooks: `sync_state` (state-sync) and `traffic_opened`
//!   (one notice, gating nothing). What a node writes at commit-state is behind [`StateActions`].
//! - A new primary, through election or recovery, re-sends the current state's command before it republishes
//!   the status it adopted (states.md, "The check-in checklist"; R-S3): members already done check in
//!   again, and the checklist rebuilds itself from the check-ins heard after the checklist began. Its
//!   down op is the read `ProbeNodeState` (Q87) ([`FabricStates::adopted_step`]).

use crate::accepted::FabricTopology;
use crate::admin::Records;
use crate::build_claim::AttemptContexts;
use crate::build_state::BuildStateAdapter;
use crate::fabric_state::{Expected, FabricState, Checklists, StateKey, StateCommand, StateOp};
use crate::model::{FabricId, IncarnationId, Node, NodeId, NodeKind, PathName};
use crate::checklist::{fabric_missing, missing_members, reports_of, Missing};
use crate::status_declare::{Authority, DeclareWake, Declarer, Key};
use crate::topology::Topology;
use crate::wiring::{FabricHooks, StateSyncHandoff, SyncState};
use rdm_transport::topology::{Announced, DigestBook};
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{NotPrimary, Status, StatusReply, StatusRequest};
use rdm_core::MemberStatus;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::Instrument;

pub use rdm_node::fabric_states::{run_action, NoScratchpad, StateActions};
#[cfg(feature = "file-backend")]
pub use rdm_node::fabric_states::StorePublication;

/// The reason the Build this admin holds accepted, `accepted`, supersedes the checklist `key`, or `None`
/// while the checklist is current (node-rpc-envelope.md, "Fabric status command pairs": "The state's command is scoped to
/// the accepted Build, attempt and planned exact births"; states.md, The check-in checklist: the checklist
/// "names the accepted Build's PLANNED births"). `key_build` is the checklist's own Build as this admin's
/// log holds it, read only when it is another Build than the accepted one.
pub(crate) fn superseded_by(key: &StateKey, accepted: &crate::build_state::BuildProjection, key_build: Option<&crate::build_state::BuildProjection>) -> Option<String> {
    if accepted.build_id.as_str() == key.build_id {
        return (accepted.attempt > key.attempt).then(|| format!("the accepted Build {} is at attempt {}, past this checklist's attempt {}", key.build_id, accepted.attempt, key.attempt));
    }
    // Another Build: superseded only by one accepted after the checklist's own. A checklist of a Build this
    // admin's log has not learned, or of one accepted after the Build it holds, came ahead of this admin's log.
    let own = key_build?;
    (own.submitted_at_ms < accepted.submitted_at_ms).then(|| format!("Build {} (accepted at {} ms) was accepted after this checklist's Build {} (accepted at {} ms)", accepted.build_id.as_str(), accepted.submitted_at_ms, key.build_id, own.submitted_at_ms))
}

/// Withdraw from `declarer` every state completion `accepted` (the Build this admin holds accepted)
/// has moved past, by the [`superseded_by`] rule: the completion of a superseded checklist is not owed
/// and is never sent. `builds` is read for the Build of a checklist that is not the accepted one.
pub(crate) async fn withdraw_superseded_completions(declarer: &Declarer, me: &PathName, accepted: &crate::build_state::BuildProjection, builds: &dyn BuildStateAdapter) {
    for (key, state_key) in declarer.state_completions() {
        if let Some(why) = state_superseded_by(&state_key, accepted, builds, &me.to_string()).await {
            declarer.withdraw_completion(me, &key, &why);
        }
    }
}

/// [`superseded_by`] for `checklist`, reading the checklist's own Build from `builds` when it is another than
/// the accepted one. A Build this admin's log holds no fact of is a checklist that came ahead of the
/// pointer (the same answer [`superseded_by`] gives it). A Build that cannot be read leaves the
/// question open, and says so by name: it is neither superseded nor known to be current.
async fn state_superseded_by(state_key: &StateKey, accepted: &crate::build_state::BuildProjection, builds: &dyn BuildStateAdapter, node: &str) -> Option<String> {
    if accepted.build_id.as_str() == state_key.build_id {
        return superseded_by(state_key, accepted, None);
    }
    let read = match crate::build::BuildId::parse(&state_key.build_id) {
        Err(e) => Err(format!("the checklist names Build {:?}, which is not a Crockford id: {e}", state_key.build_id)),
        Ok(id) => match builds.read_build(&id).await {
            Ok(own) => Ok(Some(own)),
            Err(crate::build_state::BuildStateError::UnknownBuild(_)) => Ok(None),
            Err(e) => Err(e.to_string()),
        },
    };
    let own = match read {
        Ok(own) => own,
        Err(error) => {
            tracing::info_span!("rdm.node_admin.fabric.reject.via-state-build-unreadable", node, command = state_key.kind.command(), build_id = %state_key.build_id, attempt = state_key.attempt, accepted_build_id = %accepted.build_id, error = %error)
                .in_scope(|| tracing::info!("the checklist's Build cannot be read: whether the accepted Build moved past the checklist is not known"));
            None
        }
    };
    superseded_by(state_key, accepted, own.as_ref())
}

/// How a call to a state peer ended.
enum Delivery {
    /// The peer admitted the op (`Applied` or `AlreadyApplied`): it is never sent again.
    Answered(StatusReply),
    /// The peer replied with a refusal or that it is not ready (`typed: true`; a refusal rests on the
    /// peer's own view, which heals), or a command's Owed call ended without a reply (`typed: false`:
    /// the peer's exit or removal is proven): the same op is sent again on an eligible event only
    /// (`Checklists::take_due`).
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
pub struct ChangeInputs<'a> {
    /// Whether this admin holds the fabric-primary role now.
    pub is_fabric_primary: bool,
    /// The state of the last FabricStatus frame this admin heard (`None`: none heard).
    pub heard: Option<&'a str>,
    /// This admin's view.
    pub view: &'a Topology,
}

/// The shared handles one admin's checklists read and act through.
pub(crate) struct StatesEnv {
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
    pub actions: Arc<dyn StateActions>,
    /// The state keys this node's action ran for, in any role (`rdm_node::fabric_states::ActedRecord`),
    /// shared with the node's member path: once per state key (states.md "The check-in checklist").
    pub acted: Arc<rdm_node::fabric_states::ActedRecord>,
}

/// Which checklists of an adopted status are complete now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Complete {
    /// Whether the mesh's checklist is complete.
    pub mesh: bool,
    /// Whether the fabric's checklist is complete.
    pub fabric: bool,
}

/// What the status loop hands [`FabricStates::adopted_step`] each turn.
pub struct AdoptedInputs<'a> {
    /// This admin's view, as installed this turn.
    pub view: &'a Topology,
    /// The accepted Build's planned births of this admin's mesh; `None`: the Build was unreadable.
    pub planned: Option<Vec<PathName>>,
    /// The digest book: the check-ins of the mesh checklist are the digests heard since it began.
    pub book: &'a DigestBook,
}

/// What the fabric-primary's view held of a mesh primary when it sent that primary the down op:
/// the facts the primary's declaration is decided against (its birth, the endpoint it is resolved
/// by, and its mesh's id). The down op goes again when any of them changes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Addressed {
    node_id: NodeId,
    incarnation: Option<IncarnationId>,
    endpoint: Option<crate::model::EndpointId>,
    mesh_id: Option<crate::model::MeshId>,
}

impl Addressed {
    fn of(p: &Node, mesh: &crate::model::Mesh) -> Self {
        Self { node_id: p.node_id.clone(), incarnation: p.incarnation_id.clone(), endpoint: p.endpoint_id.clone(), mesh_id: mesh.id.clone() }
    }
}

/// One adopted status's checklist: when it began (a check-in counts only after that) and the hold last named.
struct AdoptedChecklist {
    began: Instant,
    held: Option<String>,
}

/// The checklists a new primary runs before it republishes the statuses it adopted (states.md, "The
/// check-in checklist"; R-S3): its mesh's while it adopted its mesh's status, the fabric's while it
/// adopted the fabric's. A checklist exists exactly while its status is adopted and unpublished.
#[derive(Default)]
struct Adopted {
    mesh: Option<AdoptedChecklist>,
    fabric: Option<AdoptedChecklist>,
    /// The mesh primaries this admin, as the fabric-primary, has sent the down op to, by mesh: the
    /// view's primary changing from this is what sends it again (R-S2).
    addressed: BTreeMap<String, Addressed>,
}

enum Progress {
    Waiting,
    Answered(StateSyncHandoff),
    /// The application reported its work failed, naming why (Q158).
    Failed(String),
}

struct SyncRun {
    sent: StateSyncHandoff,
    progress: Arc<Mutex<Progress>>,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Commit,
    Open,
}

#[derive(Default)]
struct Change {
    adopted: bool,
    root: Option<tracing::Span>,
    sync: Option<SyncRun>,
    phase: Option<Phase>,
    key: Option<StateKey>,
    notified: bool,
    /// The accepted topology the fabric changes under: a Build that changes it (a resize or an executable
    /// replacement) puts the fabric back to pending (states.md, "Resize and executable replacement").
    tracked: Option<FabricTopology>,
    /// The Build whose topology `tracked` is. One Build's topology never changes: an admin that held the
    /// Build as heard on gossip (no executables) and reads it from the store root once fabric-primary
    /// holds two copies of one topology, and that is no resize.
    tracked_build: Option<crate::build::BuildId>,
    /// The blocker last named, so a standing blocker is one span and a changed one is another.
    named: Option<String>,
}

/// The gossip a checklist's hooks go out on: this admin's mesh channel and the backbone.
struct HookGossip {
    backbone: rdm_transport::topology::Backbone,
    membership: rdm_transport::topology::Membership,
}

/// One admin's checklists: as the fabric-primary the change, as a mesh primary the commands to its
/// members and its completion up, as a member the check-in.
pub struct FabricStates {
    env: StatesEnv,
    /// The check-ins of the members of this admin's mesh, as its mesh primary.
    pub mesh_book: Checklists,
    /// The check-ins of the mesh primaries, as the fabric-primary.
    pub fabric_book: Checklists,
    /// The mesh checklists a driver runs, each with the opening of its checklist the driver drives.
    drivers: Mutex<HashMap<StateKey, u64>>,
    /// The checklists whose own action this admin stored, as its mesh's primary: a checklist run again
    /// (a re-opened checklist after a cut-off, a re-sent command) checks in from this record and never
    /// runs the action twice.
    acted: Mutex<HashSet<StateKey>>,
    /// Where the state hooks are published; set once the admin has joined the backbone.
    gossip: std::sync::OnceLock<HookGossip>,
    change: tokio::sync::Mutex<Change>,
    /// Poked when a check-in, the application's answer or this admin's own mesh checklist arrives.
    change_wake: tokio::sync::Notify,
    /// The checklists of the statuses this admin adopted as a new primary.
    adopted: Mutex<Adopted>,
}

impl FabricStates {
    /// The checklists of the admin `env` describes.
    pub(crate) fn new(env: StatesEnv) -> Arc<Self> {
        Arc::new(Self { env, mesh_book: Checklists::default(), fabric_book: Checklists::default(), drivers: Mutex::new(HashMap::new()), acted: Mutex::new(HashSet::new()), gossip: std::sync::OnceLock::new(), change: tokio::sync::Mutex::new(Change::default()), change_wake: tokio::sync::Notify::new(), adopted: Mutex::new(Adopted::default()) })
    }

    /// The paths the current Build plans in each mesh (`node.get` names a birth its view no longer
    /// holds at one of them); `Err` names why the Build could not be read.
    pub(crate) async fn planned_paths(&self) -> crate::views::Planned {
        match self.env.builds.current().await {
            Ok(Some(b)) => Ok(b.topology.meshes.iter().map(|(m, t)| (m.clone(), t.nodes.iter().cloned().collect())).collect()),
            Ok(None) => Ok(BTreeMap::new()),
            Err(e) => Err(format!("the current Build could not be read: {e}")),
        }
    }

    fn held_name_in(&self, view: &Topology) -> PathName {
        self.env
            .builds
            .held_name()
            .or_else(|| view.own(&self.env.node_id).map(|n| n.name.clone()))
            .unwrap_or_else(|| self.env.me.clone())
    }

    async fn held_name(&self) -> PathName {
        if let Some(held) = self.env.builds.held_name() {
            return held;
        }
        self.env
            .topology
            .read()
            .await
            .own(&self.env.node_id)
            .map(|n| n.name.clone())
            .unwrap_or_else(|| self.env.me.clone())
    }

    /// Where this admin's state hooks are published, once it has joined the backbone.
    pub fn set_gossip(&self, backbone: rdm_transport::topology::Backbone, membership: rdm_transport::topology::Membership) {
        let _ = self.gossip.set(HookGossip { backbone, membership });
    }

    /// Publish the state hook this admin authors (gossip.md, "Fabric state gossip hooks"): on its own
    /// mesh channel and the backbone, in the path of the call it runs with. A command hook names the
    /// commanded birth; a completion hook names this admin's own birth.
    async fn publish_hook(&self, key: &StateKey, up: bool, node_id: &NodeId, incarnation: &IncarnationId) {
        let me = self.held_name().await;
        let Some(gossip) = self.gossip.get() else {
            tracing::info_span!("rdm.node_admin.fabric.update.via-state-hook", node = %me, command = key.kind.command(), bound = false, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("no gossip is bound to this admin's checklists: the hook is not published"));
            return;
        };
        let _ = rdm_node::fabric_states::publish_hook(&me, key, up, node_id, incarnation, &gossip.membership, Some(&gossip.backbone)).await;
    }

    // ------------------------------------------------------------------ the fabric-primary

    /// One turn of the fabric-primary's change. Cheap and never blocking: long work (the hook, a
    /// checklist's commands) runs on its own tasks and the turn reads where they have got to.
    pub async fn change_step(self: &Arc<Self>, inputs: ChangeInputs<'_>) {
        let mut change = self.change.lock().await;
        let heard_state = inputs.heard.and_then(|h| FabricState::parse(h).or_else(|| (h == "degraded").then_some(FabricState::ReadyForTraffic)));
        if !inputs.is_fabric_primary {
            // A non-holder serves the state the holder published and runs nothing.
            if let Some(run) = change.sync.take() {
                run.task.abort();
            }
            *change = Change::default();
            self.env.records.set_fabric_state(heard_state.unwrap_or(FabricState::Pending));
            self.fabric_book_reset();
            return;
        }
        let me = self.held_name_in(inputs.view);
        if !change.adopted {
            // The seat was taken: the state the previous holder published is adopted, not re-decided.
            // A state with a checklist in flight runs that checklist again, and members already done check in again.
            change.adopted = true;
            let adopted = heard_state.unwrap_or(FabricState::Pending);
            self.env.records.set_fabric_state(adopted);
            tracing::info_span!("rdm.node_admin.fabric.update.via-state-adopted", node = %me, state = adopted.as_str(), heard = inputs.heard.unwrap_or("none"), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the fabric-primary seat was taken: the published state is adopted"));
            if matches!(adopted, FabricState::StateSync | FabricState::StateCommit) {
                change.root = Some(tracing::info_span!(parent: None, "rdm.node_admin.fabric.update.via-change", node = %me, fabric_id = %self.env.fabric_id, adopted = true));
            }
        }
        // A step that moved the fabric (or its checklist) is followed by the next at once: the change
        // waits on its checklists and the application, never on the loop that carries it.
        for _ in 0..8 {
            let before = (self.env.records.fabric_state(), change.phase);
            match before.0 {
                FabricState::Pending => self.step_pending(&mut change, inputs.view).await,
                FabricState::StateSync => self.step_state_sync(&mut change).await,
                FabricState::StateCommit => self.step_state_commit(&mut change, inputs.view).await,
                FabricState::ReadyForTraffic => self.step_ready(&mut change).await,
            }
            if before == (self.env.records.fabric_state(), change.phase) {
                break;
            }
        }
    }

    /// The change's next turn is due: a state went out.
    pub fn poke_change(&self) {
        self.change_wake.notify_one();
    }

    /// Whether the state the fabric holds went out as a FabricStatus frame, so the change may enter the next.
    fn may_advance(&self) -> bool {
        self.env.records.published_fabric_state() == Some(self.env.records.fabric_state())
    }

    /// Resolves when a checklist or the application's answer moved: the change's next turn is due.
    pub async fn change_due(&self) {
        self.change_wake.notified().await
    }

    /// A resize or an executable replacement puts the fabric back to pending: the accepted Build's topology is no longer
    /// the one the fabric changes under, so its planned births must check in again and the checklists
    /// run again. A new attempt of the same Build changes no topology and moves nothing here.
    /// A Build whose request names, for a path, the executable that path's row already records, or changes
    /// only a held path's storage, placement or tags, is neither: its paths, and each path's executable
    /// resolved against the rows ([`crate::accepted::same_on_the_rows`]), are the ones the fabric changes
    /// under, which holds it.
    /// Returns whether the fabric was put back; the move waits until the state it leaves went out.
    async fn resized(&self, change: &mut Change, build: &crate::build_state::BuildProjection, me: &PathName) -> bool {
        match &change.tracked {
            None => {
                change.tracked = Some(build.topology.clone());
                change.tracked_build = Some(build.build_id.clone());
                false
            }
            Some(held) if *held != build.topology && change.tracked_build.as_ref() == Some(&build.build_id) => {
                // The same Build read again from another copy (the store root's beside gossip's): its
                // topology did not change, so the fabric stays where it is and the copy read now is held.
                change.tracked = Some(build.topology.clone());
                false
            }
            Some(held) if *held != build.topology => {
                if self.same_on_the_rows(held, &build.topology).await {
                    tracing::info_span!("rdm.node_admin.fabric.update.via-executables-recorded", node = %me, fabric_id = %self.env.fabric_id, build_id = %build.build_id.log(), attempt = build.attempt, state = self.env.records.fabric_state().as_str(), "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("the Build holds the same paths, each on the executable its row records: no resize and no executable replacement, and the fabric stays where it is"));
                    change.tracked = Some(build.topology.clone());
                    change.tracked_build = Some(build.build_id.clone());
                    return false;
                }
                if !self.may_advance() {
                    return false;
                }
                tracing::info_span!(parent: None, "rdm.node_admin.fabric.update.via-state-reset", node = %me, fabric_id = %self.env.fabric_id, build_id = %build.build_id.log(), attempt = build.attempt, from = self.env.records.fabric_state().as_str(), reason = "the accepted topology changed", "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a resize or an executable replacement puts the fabric back to pending"));
                change.tracked = None;
                change.tracked_build = None;
                change.notified = false;
                if let Some(run) = change.sync.take() {
                    run.task.abort();
                }
                change.key = None;
                change.phase = None;
                change.root = None;
                self.fabric_book.clear();
                self.enter(change, FabricState::Pending, me);
                true
            }
            Some(_) => false,
        }
    }

    /// Whether this step acts on nothing of `build`: the accepted topology is not the one the fabric
    /// changes under. Either [`Self::resized`] put the fabric back to pending, or the move waits until
    /// the state it leaves went out, and until then no checklist of the new Build runs inside the old
    /// one's change (fabric-state-sync.md: the hook follows "all planned meshes ready").
    async fn topology_moved(&self, change: &mut Change, build: &crate::build_state::BuildProjection, me: &PathName) -> bool {
        self.resized(change, build, me).await || (change.tracked.as_ref().is_some_and(|held| *held != build.topology) && change.tracked_build.as_ref() != Some(&build.build_id))
    }

    /// A Build this fabric-primary has just accepted (states.md, Resize and executable replacement: "A resize or an executable replacement puts
    /// the fabric back to pending"): when the current Build's topology is not the one the fabric changes
    /// under, the fabric is back to pending now, inside the acceptance and before its door answers, and
    /// the status loop is woken to send the state at once. No read after the answer pairs the accepted
    /// Build with the state the previous topology's change reached. A change that holds no topology yet
    /// takes the current Build at its own next step. Whether the fabric went back to pending.
    pub async fn accepted(self: &Arc<Self>) -> bool {
        let mut change = self.change.lock().await;
        if change.tracked.is_none() {
            return false;
        }
        let me = self.held_name().await;
        let reset = match self.env.builds.current().await {
            Ok(Some(build)) => self.resized(&mut change, &build, &me).await,
            Ok(None) => false,
            Err(e) => {
                self.block(&mut change, &[unreadable(&e)], "the acceptance reads the current Build", &me);
                false
            }
        };
        if reset {
            self.change_wake.notify_one();
        }
        reset
    }

    async fn step_ready(self: &Arc<Self>, change: &mut Change) {
        let me = self.held_name().await;
        match self.env.builds.current().await {
            Ok(Some(build)) => {
                self.resized(change, &build, &me).await;
            }
            Ok(None) => {}
            Err(e) => self.block(change, &[unreadable(&e)], "ready-for-traffic reads the current Build", &me),
        }
    }

    /// Whether `next` holds `held`'s paths with each path's executable resolved against what the node rows
    /// record ([`crate::accepted::same_on_the_rows`]). Rows this admin cannot read hold nothing: the two differ.
    async fn same_on_the_rows(&self, held: &FabricTopology, next: &FabricTopology) -> bool {
        let Some(rows) = self.env.records.contacts.get() else {
            return false;
        };
        match crate::accepted::recorded_executables(&**rows, next).await {
            Ok(recorded) => crate::accepted::same_on_the_rows(held, next, &recorded),
            Err(e) => {
                tracing::info!(error = %e, "the node rows could not be read for what each path runs: the accepted topology is taken as changed");
                false
            }
        }
    }

    fn fabric_book_reset(&self) {
        // A checklist belongs to the tenure that opened it: a new holder opens its own. The
        // tenure's own mesh checklists go with it (i146 Q45): a checklist it opens again re-runs this admin's
        // own mesh checklist, whose members check in again from their record.
        let ended = self.fabric_book.keys();
        if ended.is_empty() {
            return;
        }
        for key in &ended {
            self.mesh_book.close(key);
        }
        self.fabric_book.clear();
        tracing::info_span!("rdm.node_admin.fabric.remove.via-tenure-ended", node = %self.env.me, checklists = ended.len(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("this admin holds the fabric-primary role no more: the checklists it opened and its own mesh's checklists are closed"));
    }

    fn enter(&self, change: &mut Change, next: FabricState, me: &PathName) {
        let from = self.env.records.fabric_state();
        self.env.records.set_fabric_state(next);
        self.env.records.set_fabric_blocker(None);
        change.named = None;
        let parent = change.root.clone().unwrap_or_else(tracing::Span::none);
        tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-state-entered", node = %me, fabric_id = %self.env.fabric_id, from = from.as_str(), to = next.as_str(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the fabric-primary entered a fabric state"));
        self.env.wake.poke();
    }

    /// A blocker standing in a state: one span while it stands, another when it changes.
    fn block(&self, change: &mut Change, blockers: &[Missing], what: &str, me: &PathName) {
        let text = blockers.iter().map(Missing::to_string).collect::<Vec<_>>().join(",");
        let line = format!("{what}: {text}");
        self.env.records.set_fabric_blocker(Some(line.clone()));
        if change.named.as_deref() != Some(line.as_str()) {
            change.named = Some(line.clone());
            let parent = change.root.clone().unwrap_or_else(tracing::Span::none);
            tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-blocked", node = %me, state = self.env.records.fabric_state().as_str(), what, blockers = %text, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the fabric cannot leave its state: the blockers are named"));
        }
    }

    /// Whether the current Build `build` is a fabric.delete: the fabric changes no more, a checklist this
    /// fabric-primary drives ends at the delete's acceptance with its hook's future dropped, and no checklist
    /// is opened for the delete (build.md §1). The blocker names the delete.
    fn deleting(&self, change: &mut Change, build: &crate::build_state::BuildProjection, me: &PathName) -> bool {
        if !build.deletes_fabric() {
            return false;
        }
        if let Some(run) = change.sync.take() {
            run.task.abort();
        }
        self.block(change, &[Missing { who: "build".into(), why: "fabric-delete" }], &format!("fabric.delete Build {} is the current Build: no checklist is opened", build.build_id), me);
        true
    }

    async fn step_pending(self: &Arc<Self>, change: &mut Change, view: &Topology) {
        let me = self.held_name_in(view);
        let accepted = match self.env.builds.current().await {
            Ok(b) => b,
            Err(e) => return self.block(change, &[unreadable(&e)], "state-sync names the current Build", &me),
        };
        if accepted.as_ref().is_some_and(|b| self.deleting(change, b, &me)) {
            return;
        }
        let mut blockers = self.entry_blockers(view, accepted.as_ref().map(|b| &b.topology));
        // During an executable replacement a member still on its old executable does not satisfy its
        // planned replacement: the fabric changes once every planned member runs its entry's executable
        // (states.md "Resize and executable replacement"), as the node rows record what each runs.
        if let (Some(build), Some(rows)) = (accepted.as_ref().filter(|b| b.submitted_change.as_ref().is_some_and(crate::accepted::TopologyChange::plans_replacements)), self.env.records.contacts.get()) {
            match crate::accepted::recorded_executables(&**rows, &build.topology).await {
                Ok(recorded) => {
                    // A member whose row records another executable than its entry names is owed its
                    // replacement (proposal executable-bindings-at-create.md rule 5).
                    blockers.extend(crate::accepted::paths_owed_replacement(&build.topology, &recorded).into_iter().map(|p| Missing { who: p.to_string(), why: "replacement-owed" }))
                }
                Err(e) => blockers.push(Missing { who: e, why: "rows-unreadable" }),
            }
        }
        if !blockers.is_empty() {
            self.block(change, &blockers, "state-sync waits for every mesh to be ready-for-traffic", &me);
            return;
        }
        let Some(build) = accepted else {
            self.block(change, &[Missing { who: "build".into(), why: "no-accepted-build" }], "state-sync names the accepted Build", &me);
            return;
        };
        // The sync_state call names the accepted Build's attempt: a Build whose first attempt no
        // executor has claimed yet has none, and the call names the attempt that exists.
        if build.attempt == 0 {
            self.block(change, &[Missing { who: "build".into(), why: "no-attempt-claimed" }], "state-sync names the accepted Build's attempt", &me);
            return;
        }
        if !self.may_advance() {
            return;
        }
        change.root = Some(tracing::info_span!(parent: None, "rdm.node_admin.fabric.update.via-change", node = %me, fabric_id = %self.env.fabric_id, build_id = %build.build_id.log(), adopted = false));
        change.tracked = Some(build.topology.clone());
        change.tracked_build = Some(build.build_id.clone());
        self.enter(change, FabricState::StateSync, &me);
    }

    /// The meshes not ready-for-traffic and the mesh primaries that have not reported, named. Empty:
    /// every mesh is ready-for-traffic (R-H1). A mesh's state is the one the view projects
    /// (`Topology::mesh_unready` decides it); the nodes behind a mesh that is not ready are named
    /// from that same function.
    fn entry_blockers(&self, view: &Topology, accepted: Option<&FabricTopology>) -> Vec<Missing> {
        let (meshes, planned): (Vec<String>, std::collections::BTreeMap<String, std::collections::BTreeSet<PathName>>) = match accepted {
            Some(t) => (
                t.meshes.keys().cloned().collect(),
                t.meshes.iter().map(|(m, mt)| (m.clone(), mt.nodes.clone())).collect(),
            ),
            None => (view.meshes.iter().map(|m| m.name.clone()).collect(), self.env.records.planned()),
        };
        let mut out = mesh_blockers(view, &meshes, &planned);
        let primaries: Vec<(String, Option<IncarnationId>, bool)> =
            meshes.iter().map(|m| { let p = view.node_type_primary(m, NodeKind::NodeAdmin); (m.clone(), p.and_then(|n| n.incarnation_id.clone()), p.is_some_and(|n| n.node_id == self.env.node_id)) }).collect();
        let reports = reports_of(&self.env.records.declared.lock().unwrap(), view, meshes.iter().map(String::as_str));
        out.extend(fabric_missing(&primaries, self.env.records.declare_gate(), &reports));
        out
    }

    async fn step_state_sync(self: &Arc<Self>, change: &mut Change) {
        let me = self.held_name().await;
        let build = match self.env.builds.current().await {
            Ok(Some(build)) => build,
            Ok(None) => return self.block(change, &[Missing { who: "build".into(), why: "no-accepted-build" }], "sync_state names the accepted Build", &me),
            Err(e) => return self.block(change, &[unreadable(&e)], "sync_state names the current Build", &me),
        };
        if self.topology_moved(change, &build, &me).await || self.deleting(change, &build, &me) {
            return;
        }
        let parent = change.root.clone().unwrap_or_else(tracing::Span::none);
        // The state-sync is the accepted Build's and attempt's: when either moved while the application
        // worked (a restart opened an attempt), the application is asked again for the current one.
        if change.sync.as_ref().is_some_and(|run| run.sent.build_id != build.build_id.to_string() || run.sent.attempt != build.attempt) {
            if let Some(run) = change.sync.take() {
                tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-sync-state-reissued", node = %me, was_build_id = %run.sent.build_id, was_attempt = run.sent.attempt, build_id = %build.build_id.log(), attempt = build.attempt, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the accepted attempt moved: sync_state is called again for the current one"));
                run.task.abort();
            }
        }
        if change.sync.is_none() {
            let context = match self.env.contexts.for_claim(&build.build_id, build.attempt).await {
                Ok(c) => c,
                Err(e) => {
                    self.block(change, &[Missing { who: "attempt-context".into(), why: "unreadable" }], &format!("sync_state needs the attempt's context ({e})"), &me);
                    return;
                }
            };
            let sent = StateSyncHandoff { fabric_id: self.env.fabric_id.clone(), build_id: build.build_id.to_string(), attempt: build.attempt, operation: format!("state-sync:{}", self.env.fabric_id), context };
            let progress = Arc::new(Mutex::new(Progress::Waiting));
            let (this, hook, me_for_task, sent_for_task, progress_for_task, parent_for_task) = (self.clone(), self.env.hooks.sync_state.clone(), me.clone(), sent.clone(), progress.clone(), parent.clone());
            let task = tokio::spawn(async move {
                run_sync_state(hook, me_for_task, sent_for_task, progress_for_task, parent_for_task).await;
                this.change_wake.notify_one();
            });
            change.sync = Some(SyncRun { sent, progress, task });
        }
        let run = change.sync.as_ref().expect("started above");
        let answered = match &*run.progress.lock().unwrap() {
            Progress::Waiting => None,
            Progress::Answered(a) => Some(Ok(a.clone())),
            Progress::Failed(reason) => Some(Err(reason.clone())),
        };
        // A failure the application reports is the call's answer (Q158; states.md "Resize and executable replacement": "a
        // state's check-in or a hook that reports failure fails the Build on that step"): the drive fails
        // the Build it was called for on its `fabric-change` step, and the fabric stays in state-sync,
        // the failure named.
        let answer = match answered {
            None => None,
            Some(Ok(answer)) => Some(answer),
            Some(Err(reason)) => {
                let sent = &run.sent;
                self.env.records.set_state_sync_failed(crate::admin::StateSyncFailure { build_id: sent.build_id.clone(), attempt: sent.attempt, operation: sent.operation.clone(), reason: reason.clone() });
                let what = format!("sync_state reported the application's work failed for build {} attempt {}: {reason}", sent.build_id, sent.attempt);
                self.block(change, &[Missing { who: "application".into(), why: "state-sync-failed" }], &what, &me);
                return;
            }
        };
        let Some(answer) = answer else {
            let what = format!("sync_state awaits the application's state-synced for build {} attempt {}", run.sent.build_id, run.sent.attempt);
            self.block(change, &[Missing { who: "application".into(), why: "no-state-synced" }], &what, &me);
            return;
        };
        // The answer completes state-sync only for THIS state-sync: the fabric, Build, attempt and
        // operation it was sent, and the accepted Build and attempt now. A foreign Build, an old
        // attempt or any other state-sync is refused by name and the fabric stays in state-sync.
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
            self.block(change, &[Missing { who: "application".into(), why: "state-synced-refused" }], &format!("state-synced refused, field {why}"), &me);
            let key = format!("refused:{why}");
            if change.named.as_deref() != Some(key.as_str()) {
                tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.reject.via-state-synced", node = %me, reason = %why, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a state-synced that is not this state-sync's completion is refused"));
            }
            return;
        }
        if !self.may_advance() {
            return;
        }
        tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-state-synced", node = %me, build_id = %sent.build_id, attempt = sent.attempt, operation = %sent.operation, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the application's state-synced matches this state-sync: state-commit begins"));
        if let Some(run) = change.sync.take() {
            run.task.abort();
        }
        change.phase = Some(Phase::Commit);
        change.key = Some(StateKey { kind: StateCommand::StateCommit, fabric_id: sent.fabric_id.clone(), build_id: sent.build_id.clone(), attempt: sent.attempt });
        self.enter(change, FabricState::StateCommit, &me);
    }

    async fn step_state_commit(self: &Arc<Self>, change: &mut Change, view: &Topology) {
        let me = self.held_name_in(view);
        let build = match self.env.builds.current().await {
            Ok(Some(build)) => build,
            Ok(None) => return self.block(change, &[Missing { who: "build".into(), why: "no-accepted-build" }], "commit-state names the accepted Build", &me),
            Err(e) => return self.block(change, &[unreadable(&e)], "commit-state names the current Build", &me),
        };
        if self.topology_moved(change, &build, &me).await || self.deleting(change, &build, &me) {
            return;
        }
        match &change.key {
            // Adopted in state-commit: the checklist is the accepted Build's, started over.
            None => {
                change.phase = Some(Phase::Commit);
                change.key = Some(StateKey { kind: StateCommand::StateCommit, fabric_id: self.env.fabric_id.clone(), build_id: build.build_id.to_string(), attempt: build.attempt });
            }
            // The accepted attempt moved: the checklist is scoped to the current one, and its planned births check in again.
            Some(k) if k.build_id != build.build_id.to_string() || k.attempt != build.attempt => {
                change.key = Some(StateKey { kind: k.kind, fabric_id: k.fabric_id.clone(), build_id: build.build_id.to_string(), attempt: build.attempt });
                self.fabric_book.clear();
            }
            Some(_) => {}
        }
        let (phase, key) = (change.phase.unwrap_or(Phase::Commit), change.key.clone().expect("set above"));
        let parent = change.root.clone().unwrap_or_else(tracing::Span::none);
        let missing = self.fabric_checklist(&key, view, &parent).await;
        if !missing.is_empty() {
            self.block(change, &missing, &format!("{} waits for the check-in checklist", key.kind.command()), &me);
            return;
        }
        if phase == Phase::Open && !self.may_advance() {
            return;
        }
        change.named = None;
        tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-checklist-complete", node = %me, command = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the check-in checklist is complete"));
        match phase {
            Phase::Commit => {
                change.phase = Some(Phase::Open);
                change.key = Some(StateKey { kind: StateCommand::OpenTraffic, ..key });
            }
            Phase::Open => {
                // The fact is durable before anything acts on it: the fabric-primary's own mesh's
                // ready-for-traffic row first, then the state, which latches the fact and lets the door
                // admit workflows; the declarer applies the event at every other mesh primary after.
                if let Err(why) = self.put_ready_row(&parent, &me).await {
                    let missing = [Missing { who: me.mesh.clone(), why: "ready-row-unwritten" }];
                    self.block(change, &missing, &format!("ready-for-traffic puts the fabric-primary's ready-for-traffic row on the store root first ({why})"), &me);
                    return;
                }
                self.enter(change, FabricState::ReadyForTraffic, &me);
                let notice = StateKey { kind: StateCommand::OpenTraffic, ..key };
                if !change.notified {
                    change.notified = true;
                    self.notify_traffic_opened(&notice, parent.clone(), me);
                }
                change.root = None;
                change.key = None;
                change.phase = None;
            }
        }
    }

    /// The fabric-primary's own ready-for-traffic row, put among its mesh's status rows on the store root
    /// before it enters ready-for-traffic (`Records::put_ready_row`), under one span in the change. `Err`
    /// names what refused it: the fabric stays where it is and nothing is latched.
    async fn put_ready_row(&self, parent: &tracing::Span, me: &PathName) -> Result<(), String> {
        let span = tracing::info_span!(parent: parent, "rdm.node_admin.fabric.update.via-ready-row-put", node = %me, fabric_id = %self.env.fabric_id, mesh = %me.mesh, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal");
        let put = self.env.records.put_ready_row(&self.env.fabric_id).instrument(span.clone()).await;
        match &put {
            Ok(reply) => {
                span.record("outcome", reply.name());
                span.in_scope(|| tracing::info!("the fabric-primary's ready-for-traffic row is on the store root: the fabric may enter ready-for-traffic"));
            }
            Err(why) => {
                span.record("outcome", "refused").record("detail", why.as_str());
                span.in_scope(|| tracing::info!("the fabric-primary's ready-for-traffic row could not be put: the fabric does not enter ready-for-traffic"));
            }
        }
        put.map(|_| ())
    }

    /// One turn of a fabric-level checklist: open it, command the mesh primaries not yet
    /// commanded as their exact birth, run this admin's own mesh as its mesh primary. The planned
    /// births not checked in, named; empty is complete.
    async fn fabric_checklist(self: &Arc<Self>, key: &StateKey, view: &Topology, parent: &tracing::Span) -> Vec<Missing> {
        let me = self.held_name_in(view);
        if self.fabric_book.open(key) {
            tracing::info_span!(parent: parent, "rdm.node_admin.fabric.update.via-checklist-opened", node = %me, command = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the fabric-primary opened a checklist"));
        }
        // A builds store that cannot be read plans nothing: the checklist stays open, named, and is
        // never planned from the view in its place.
        let meshes: Vec<String> = match self.env.builds.current().await {
            Ok(Some(b)) => b.topology.meshes.keys().cloned().collect(),
            Ok(None) => view.meshes.iter().map(|m| m.name.clone()).collect(),
            Err(e) => return vec![unreadable(&e)],
        };
        let mut planned = BTreeMap::new();
        let mut unresolved = Vec::new();
        for mesh in &meshes {
            match view.node_type_primary(mesh, NodeKind::NodeAdmin) {
                Some(p) if p.incarnation_id.is_some() => {
                    planned.insert(p.node_id.clone(), Expected { incarnation: p.incarnation_id.clone().expect("checked"), name: p.name.to_string() });
                }
                _ => unresolved.push(Missing { who: mesh.clone(), why: "no-primary" }),
            }
        }
        self.fabric_book.expect(key, planned, unresolved);
        // This admin's own mesh: it is the mesh primary of it, so it runs that mesh's checklist itself.
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

    /// The one notice to the application: after the open-traffic checklist completed and the fabric
    /// is ready-for-traffic. It gates nothing.
    fn notify_traffic_opened(&self, key: &StateKey, parent: tracing::Span, me: PathName) {
        let (hook, contexts) = (self.env.hooks.traffic_opened.clone(), self.env.contexts.clone());
        let state_key = (key.fabric_id.clone(), key.build_id.clone(), key.attempt);
        tokio::spawn(async move {
            let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-traffic-opened-hook", node = %me, build_id = %state_key.1, attempt = state_key.2, bound = hook.is_some(), "otel.kind" = "internal");
            async {
                let Some(hook) = hook else {
                    tracing::info!("no traffic_opened notice is registered");
                    return;
                };
                let context = match crate::build::BuildId::parse(&state_key.1) {
                    Ok(build) => contexts.get(&build, state_key.2).await.ok().flatten().unwrap_or_default(),
                    Err(e) => {
                        tracing::info!(build_id = %state_key.1, error = %e, "the checklist's Build id is not a Crockford id: no call context is held for it");
                        Default::default()
                    }
                };
                hook(StateSyncHandoff { fabric_id: state_key.0.clone(), build_id: state_key.1.clone(), attempt: state_key.2, operation: StateCommand::OpenTraffic.operation(&state_key.0), context }).await;
                tracing::info!("the application was told the fabric opened traffic");
            }
            .instrument(span)
            .await
        });
    }

    // ------------------------------------------------------------ commands down, one hop

    /// Send the down op of `key` to the planned birth `to`, on its own task, as one Owed call
    /// (node-rpc.md §43, i146 e3): the delivery layer resends it until the birth answers, and the call
    /// ends only on that answer, on the birth's proven exit or removal, or when the checklist closes (this
    /// owner's cancellation). The book it was taken from commands it once per exact birth; an op the
    /// peer refused waits for an eligible event.
    fn spawn_command(self: &Arc<Self>, key: StateKey, fabric_level: bool, to: NodeId, expected: Expected, event: &'static str, parent: tracing::Span) {
        let this = self.clone();
        tokio::spawn(async move {
            let me = this.held_name().await;
            let book = if fabric_level { &this.fabric_book } else { &this.mesh_book };
            let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-state-command", node = %me, command = key.kind.command(), to = %expected.name, event, outcome = tracing::field::Empty, "otel.kind" = "internal");
            let req = key.kind.down(&key, to.clone(), expected.incarnation.clone());
            // The command hook runs in the path of the command it announces, before the call goes out.
            this.publish_hook(&key, false, &to, &expected.incarnation).instrument(span.clone()).await;
            // Armed before the checklist is read: a close during the read is seen by the wait.
            let mut closed = book.subscribe();
            closed.borrow_and_update();
            let owed = CallOptions { budget: Budget::Owed, ..CallOptions::default() };
            let target = NodeTarget::ExactNode(to.clone());
            let call = this.env.client.call::<Status>(&target, &req, &owed).instrument(span.clone());
            tokio::pin!(call);
            let out = loop {
                if !book.is_open(&key) {
                    break None;
                }
                tokio::select! {
                    (out, _) = &mut call => break Some(out),
                    changed = closed.changed() => {
                        if changed.is_err() {
                            break None;
                        }
                    }
                }
            };
            let Some(out) = out else {
                // Dropping the call ends its owed request: the checklist it commands is closed.
                span.record("outcome", "cancelled: the checklist closed");
                return;
            };
            match delivery(&out) {
                Delivery::Answered(r) => {
                    span.record("outcome", r.name());
                    book.answered(&key, &to, &expected.incarnation);
                }
                Delivery::Undelivered { why, typed } => {
                    span.record("outcome", why.as_str());
                    // Wake the checklist waiter so the current view is checked against what was sent while the call was out.
                    if book.undelivered(&key, &to, &expected.incarnation, typed) && fabric_level {
                        this.change_wake.notify_one();
                    }
                }
            }
        });
    }

    // ----------------------------------------------------------------- the mesh primary

    /// Drive `key` as this mesh's primary: command every planned member of this mesh, run this
    /// admin's own action, and when the checklist is complete report upward. `owes_upward`: the
    /// checklist came down from the fabric-primary, which the completion is owed to; `false` when this
    /// admin is the fabric-primary and the mesh checklist is its own. A driver drives one opening of the
    /// checklist: a checklist closed and opened again (a tenure that ended and a change that opens it again)
    /// gets a driver of its own, and the driver of the closed opening ends.
    fn spawn_mesh_driver(self: &Arc<Self>, key: StateKey, owes_upward: bool, parent: tracing::Span) {
        let Some(opening) = self.mesh_book.opening(&key) else { return };
        {
            let mut drivers = self.drivers.lock().unwrap();
            if drivers.get(&key) == Some(&opening) {
                return;
            }
            drivers.insert(key.clone(), opening);
        }
        let this = self.clone();
        tokio::spawn(async move {
            let me = this.held_name().await;
            let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-mesh-checklist", node = %me, command = key.kind.command(), build_id = %key.build_id, attempt = key.attempt, superseded_by = tracing::field::Empty, own_action = tracing::field::Empty, "otel.kind" = "internal");
            this.drive_mesh(&key, owes_upward, opening).instrument(span.clone()).await;
            let mut drivers = this.drivers.lock().unwrap();
            if drivers.get(&key) == Some(&opening) {
                drivers.remove(&key);
            }
        });
    }

    async fn drive_mesh(self: &Arc<Self>, key: &StateKey, owes_upward: bool, opening: u64) {
        let mesh = self.env.me.mesh.clone();
        let mut own_done = self.acted.lock().unwrap().contains(key) || self.env.acted.acted(key);
        if own_done {
            // A checklist run again: this admin's own action is stored, and its check-in is made from that.
            tracing::Span::current().record("own_action", "from-record");
        }
        let checklist_span = tracing::Span::current();
        // The wait is armed before the read: a checklist change, a view change or a birth addressing
        // this primary that happens during the read is seen by the wait, never lost, and nothing
        // here runs on a timer.
        let (mut book_tick, mut view_tick) = (self.mesh_book.subscribe(), self.env.records.subscribe_view());
        loop {
            book_tick.borrow_and_update();
            view_tick.borrow_and_update();
            if self.mesh_book.opening(key) != Some(opening) {
                tracing::info!("the opening of the checklist this driver drives is closed: an opening of it again has a driver of its own");
                return;
            }
            let view = self.env.topology.read().await.clone();
            let me = self.held_name_in(&view);
            if !view.node_type_primary(&mesh, NodeKind::NodeAdmin).is_some_and(|n| n.node_id == self.env.node_id) {
                tracing::info!("this admin no longer holds its mesh's primary seat: the checklist is the next primary's");
                self.mesh_book.close_opening(key, opening);
                return;
            }
            // A builds store that cannot be read plans nothing this turn, named: the planned births
            // are never taken from the view in its place.
            let accepted = match self.env.builds.current().await {
                Ok(b) => b,
                Err(e) => {
                    tracing::info_span!("rdm.node_admin.fabric.reject.via-build-store-unreadable", node = %me, command = key.kind.command(), mesh = %mesh, error = %e, "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("the current Build could not be read: this mesh checklist plans nothing until it can"));
                    tokio::select! {
                        _ = book_tick.changed() => {}
                        _ = view_tick.changed() => {}
                    }
                    continue;
                }
            };
            // A checklist the current Build moved past commands nothing more and owes no completion: the
            // checklist of the current Build and attempt is the one its primaries run now.
            if let Some(b) = &accepted {
                if let Some(why) = state_superseded_by(key, b, &*self.env.builds, &me.to_string()).await {
                    checklist_span.record("superseded_by", why.as_str());
                    tracing::info!(superseded_by = %why, "the current Build moved past this checklist: it commands nothing more and owes no completion");
                    self.mesh_book.close_opening(key, opening);
                    return;
                }
            }
            let paths: Vec<PathName> = match &accepted {
                Some(b) => crate::checklist::planned(&b.topology, &mesh),
                None => view.members().filter(|n| n.mesh == mesh).map(|n| n.name.clone()).collect(),
            };
            let mut planned = BTreeMap::new();
            let mut unresolved = Vec::new();
            for path in paths.iter().filter(|p| **p != me) {
                match view.members().find(|n| &n.name == path) {
                    Some(n) if n.incarnation_id.is_some() => {
                        planned.insert(n.node_id.clone(), Expected { incarnation: n.incarnation_id.clone().expect("checked"), name: n.name.to_string() });
                    }
                    _ => unresolved.push(Missing { who: path.to_string(), why: "no-birth-heard" }),
                }
            }
            self.mesh_book.expect(key, planned, unresolved);
            for (node_id, expected, event) in self.mesh_book.take_due(key, &view) {
                self.spawn_command(key.clone(), false, node_id, expected, event, checklist_span.clone());
            }
            if !own_done {
                match run_action(&*self.env.actions, &me, key.kind).await {
                    Ok(()) => {
                        own_done = true;
                        self.acted.lock().unwrap().insert(key.clone());
                        self.env.acted.record(key);
                        checklist_span.record("own_action", "run");
                    }
                    Err(e) => {
                        tracing::info_span!("rdm.node_admin.fabric.update.via-state-action-failed", node = %me, command = key.kind.command(), error = %e, "otel.kind" = "internal")
                            .in_scope(|| tracing::info!("this admin's own action failed: it owes no check-in"));
                        self.mesh_book.close_opening(key, opening);
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
                // The check-in hook runs when the completion becomes owed: once per checklist, however often it is sent.
                self.publish_hook(key, true, &self.env.node_id, &self.env.incarnation).await;
                self.env.declarer.owe_in(Key::completion(key), Authority::FabricPrimary, req, crate::build_claim::current_context().traceparent);
                self.env.wake.poke();
            }
            false => {
                self.fabric_book.check_in_local(key, &self.env.node_id, &self.env.incarnation);
                self.change_wake.notify_one();
                tracing::info!("this admin's own mesh checklist is complete");
            }
        }
    }

    // ------------------------------------------------- the checklist of an adopted status (R-S3)

    /// The fabric-primary's view of a mesh's primary changed (a mesh primary appeared, moved or
    /// was recovered): the down op goes to it, so it re-sends what it owes this admin. Not the
    /// takeover alone: a primary this admin has not addressed in this view is addressed now. A
    /// view where this admin is not the fabric-primary addresses nobody and forgets who it did.
    pub fn address_changed_mesh_primaries(&self, view: &Topology, is_fabric_primary: bool) {
        let targets = self.changed_mesh_primaries(view, is_fabric_primary);
        if !targets.is_empty() {
            let me = self.held_name_in(view);
            send_down(&me, "fabric", &view.fabric.name, targets, &self.env.client);
        }
    }

    /// The mesh primaries to send the down op now, recorded as addressed.
    pub(crate) fn changed_mesh_primaries(&self, view: &Topology, is_fabric_primary: bool) -> Vec<(String, Option<Node>)> {
        let mut adopted = self.adopted.lock().unwrap();
        if !is_fabric_primary {
            adopted.addressed.clear();
            return Vec::new();
        }
        let mut targets = Vec::new();
        let mut present = Vec::new();
        for m in &view.meshes {
            let Some(p) = view.node_type_primary(&m.name, NodeKind::NodeAdmin).filter(|p| p.node_id != self.env.node_id) else { continue };
            present.push(m.name.clone());
            let now = Addressed::of(p, m);
            if adopted.addressed.get(&m.name) != Some(&now) {
                adopted.addressed.insert(m.name.clone(), now);
                targets.push((m.name.clone(), Some(p.clone())));
            }
        }
        adopted.addressed.retain(|mesh, _| present.contains(mesh));
        targets
    }

    /// No status is adopted and waiting: no checklist of an adopted status is open.
    pub fn adopted_end(&self) {
        let mut adopted = self.adopted.lock().unwrap();
        adopted.mesh = None;
        adopted.fabric = None;
    }

    /// One step of the checklists of the adopted statuses. `awaiting` says which adopted statuses still
    /// wait for their checklist; the answer says which checklists are complete now (so the publisher may
    /// republish). A checklist opens on its first step: its down op goes to every planned birth but this
    /// admin, and its checklist counts only the check-ins heard from then on. The mesh checklist's
    /// check-in is a planned birth's digest heard ready-for-traffic after the checklist began; the
    /// fabric checklist's is a report from the CURRENT mesh primary's birth. The fabric-primary clears
    /// `degraded` only on such a report, never on a topology reading.
    pub fn adopted_step(&self, awaiting: Announced, i: AdoptedInputs<'_>) -> Complete {
        let mut adopted = self.adopted.lock().unwrap();
        let me = self.held_name_in(i.view);
        let mesh_name = me.mesh.clone();
        let mut out = Complete::default();
        if !awaiting.mesh_awaiting_checklist {
            adopted.mesh = None;
        } else {
            let first = adopted.mesh.is_none();
            let checklist = adopted.mesh.get_or_insert_with(|| AdoptedChecklist { began: Instant::now(), held: None });
            if first {
                let targets: Vec<(String, Option<Node>)> = i.planned.iter().flatten().filter(|p| **p != me).map(|p| (p.to_string(), i.view.members().find(|n| &n.name == p).cloned())).collect();
                send_down(&me, "mesh", &mesh_name, targets, &self.env.client);
            }
            let missing = match &i.planned {
                None => vec![Missing { who: mesh_name.clone(), why: "accepted-build-unreadable" }],
                Some(planned) => {
                    let me_ready = i.view.own(&self.env.node_id).is_some_and(|n| n.status == crate::model::NodeStatus::ReadyForTraffic);
                    let (heard, known) = (i.book.heard_direct_since(checklist.began), i.book.all());
                    missing_members(planned, &me, me_ready, &heard, &known, &|id| i.book.is_deleted(id))
                }
            };
            if missing.is_empty() {
                let waited_ms = checklist.began.elapsed().as_millis() as u64;
                tracing::info_span!("rdm.node_admin.mesh.update.via-checklist-complete", node = %me, mesh = %mesh_name, members = i.planned.as_ref().map(Vec::len).unwrap_or(0), waited_ms)
                    .in_scope(|| tracing::info!("every planned birth checked in: the adopted status is republished"));
                out.mesh = true;
            } else {
                let now = crate::checklist::named(&missing);
                if checklist.held.as_deref() != Some(now.as_str()) {
                    tracing::info_span!("rdm.node_admin.mesh.update.via-checklist-held", node = %me, mesh = %mesh_name, missing = %now, waited_ms = checklist.began.elapsed().as_millis() as u64)
                        .in_scope(|| tracing::info!("the checklist is held: a planned birth has not checked in"));
                    checklist.held = Some(now);
                }
            }
        }
        if !awaiting.fabric_awaiting_checklist {
            adopted.fabric = None;
        } else {
            let Adopted { fabric, addressed, .. } = &mut *adopted;
            let first = fabric.is_none();
            let checklist = fabric.get_or_insert_with(|| AdoptedChecklist { began: Instant::now(), held: None });
            // A Dead mesh reports nothing: its final primary's end is its own node.delete
            // (mesh-delete.md step 7), as the accepted topology leaves it out of `entry_blockers`.
            let live = |m: &&crate::model::Mesh| m.status != crate::model::ScopeStatus::Dead;
            let meshes: Vec<(String, Option<IncarnationId>, bool)> = i
                .view
                .meshes
                .iter()
                .filter(live)
                .map(|m| {
                    let p = i.view.node_type_primary(&m.name, NodeKind::NodeAdmin);
                    (m.name.clone(), p.and_then(|p| p.incarnation_id.clone()), p.is_some_and(|p| p.node_id == self.env.node_id))
                })
                .collect();
            if first {
                let targets: Vec<(String, Option<Node>)> = i.view.meshes.iter().filter(live).filter(|m| m.name != mesh_name).map(|m| (m.name.clone(), i.view.node_type_primary(&m.name, NodeKind::NodeAdmin).cloned())).collect();
                for (mesh, p) in &targets {
                    if let (Some(p), Some(m)) = (p, i.view.meshes.iter().find(|m| &m.name == mesh)) {
                        addressed.insert(mesh.clone(), Addressed::of(p, m));
                    }
                }
                send_down(&me, "fabric", &i.view.fabric.name, targets, &self.env.client);
            }
            let my_mesh_done = !awaiting.mesh_awaiting_checklist || out.mesh;
            let reports = reports_of(&self.env.records.declared.lock().unwrap(), i.view, meshes.iter().map(|(m, _, _)| m.as_str()));
            let missing = fabric_missing(&meshes, my_mesh_done, &reports);
            if missing.is_empty() {
                let waited_ms = checklist.began.elapsed().as_millis() as u64;
                let cleared = self.env.records.set_adopted_degraded(false);
                tracing::info_span!("rdm.node_admin.fabric.update.via-checklist-complete", node = %me, meshes = meshes.len(), waited_ms, degraded_cleared = cleared)
                    .in_scope(|| tracing::info!("every mesh primary reported for this checklist"));
                out.fabric = true;
            } else {
                let now = crate::checklist::named(&missing);
                if checklist.held.as_deref() != Some(now.as_str()) {
                    tracing::info_span!("rdm.node_admin.fabric.update.via-checklist-held", node = %me, missing = %now, waited_ms = checklist.began.elapsed().as_millis() as u64)
                        .in_scope(|| tracing::info!("the fabric checklist is held: a planned birth has not reported"));
                    checklist.held = Some(now);
                }
            }
        }
        out
    }

    // ----------------------------------------------------------------- served by an admin

    /// The completions this admin owes for a checklist the accepted Build has moved past are withdrawn,
    /// before the declarer sends anything: a superseded checklist's completion is never sent.
    pub async fn withdraw_superseded(&self) {
        if self.env.declarer.state_completions().is_empty() {
            return;
        }
        let me = self.held_name().await;
        let accepted = match self.env.builds.current().await {
            Ok(Some(b)) => b,
            Ok(None) => return,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.fabric.reject.via-build-store-unreadable", node = %me, error = %e, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the current Build could not be read: no completion is withdrawn this tick"));
                return;
            }
        };
        withdraw_superseded_completions(&self.env.declarer, &me, &accepted, &*self.env.builds).await;
    }

    /// `node_id` sent this admin a Status call: an undelivered state command to it is eligible again.
    pub fn heard_from(&self, node_id: &NodeId) {
        self.mesh_book.heard_from(node_id);
        self.fabric_book.heard_from(node_id);
        self.change_wake.notify_one();
    }

    /// A state op addressed to this admin, decided from `view`. `own` answers it as a plain member.
    pub async fn serve(self: &Arc<Self>, view: &Topology, sender: Option<&Node>, peer: Option<iroh::PublicKey>, req: &StatusRequest, own: Option<&Arc<crate::node_self::NodeSelf>>) -> StatusReply {
        let Some(op) = StateOp::of(req) else { return rdm_core::reply::status::not_ready(format!("{} is not a state op", req.op())) };
        let me = self.held_name_in(view).to_string();
        let reply = if op.is_down { self.serve_down(view, sender, peer, req, &op, own).await } else { self.serve_up(view, req, &op) };
        tracing::info_span!(
            "rdm.node_admin.status.update.via-state-op",
            node = %me,
            op = req.op(),
            command = op.kind.command(),
            sender = %sender.map(|n| n.name.to_string()).unwrap_or_default(),
            build_id = %op.build_id,
            attempt = op.attempt,
            outcome = reply.name(),
            "otel.kind" = "internal"
        )
        .in_scope(|| tracing::info!("a state op was decided"));
        reply
    }

    /// A state command naming this admin, served by what this admin is in its own view, whoever
    /// calls (node-rpc.md §14): a mesh-primary runs its mesh's checklist and reports it to the
    /// fabric-primary; any other admin takes it as a member and checks in with its mesh's primary
    /// (fabric-state-commit.md: "Each member checks in with its primary, node → mesh-primary →
    /// fabric-primary"). The fabric-primary carries checklists down and is not a receiver of one.
    #[allow(clippy::too_many_arguments)]
    async fn serve_down(self: &Arc<Self>, view: &Topology, sender: Option<&Node>, peer: Option<iroh::PublicKey>, req: &StatusRequest, op: &StateOp<'_>, own: Option<&Arc<crate::node_self::NodeSelf>>) -> StatusReply {
        let Some(me) = view.own(&self.env.node_id) else {
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "a view holding this admin".into() });
        };
        let me_name = self.held_name_in(view);
        // Answered by the admin the call reached (R-RX5: no receiver refuses a downward call for the
        // node id it names); the exact birth it names is checked, so a call that reached another
        // birth is refused naming the one held.
        if let Some(held) = me.incarnation_id.as_ref().filter(|h| *h != op.incarnation) {
            return rdm_core::reply::stale(held.clone());
        }
        if *op.fabric_id != self.env.fabric_id {
            return rdm_core::reply::status::rejected_stale_fabric(self.env.fabric_id.clone());
        }
        let want = op.kind.operation(op.fabric_id);
        if op.operation != want {
            return rdm_core::reply::status::not_ready(format!("{me_name}: {} names operation {}, the checklist's is {want}", req.op(), op.operation));
        }
        if me.is_fabric_primary {
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: format!("a mesh-primary the fabric-primary commands, or a member of a mesh: {me_name} holds the fabric seat and carries {} down", req.op()) });
        }
        if me.is_primary {
            // This admin is a mesh primary: it runs its mesh's checklist and reports to the fabric-primary.
            let key = op.key();
            self.env.wake.addressed_by(sender.map(|n| &n.node_id), peer);
            if self.mesh_book.open(&key) {
                self.spawn_mesh_driver(key, true, tracing::Span::current());
                rdm_core::reply::applied()
            } else {
                // The checklist is held. A fabric-primary asking again holds another tenure's checklist (a
                // successor, or the holder healed from a cut-off, i146 Q45): a completion this admin
                // already made is owed again from its record, and the checklist rebuilds.
                self.env.declarer.owe_again(&me_name, &crate::status_declare::Key::completion(&key));
                self.env.wake.poke();
                rdm_core::reply::already_applied()
            }
        } else {
            // A member: it checks in with the primary that commanded it (fabric-state-commit.md, checklist
            // sequence: "M->>N: commit-state", "N->>M: state-committed"): the caller, which a new
            // primary is before this admin's view names it. Only a caller the view does not hold is
            // checked in at the mesh's primary as the view names it.
            let commander = sender.filter(|s| s.node_id != me.node_id).map(|s| s.node_id.clone()).or_else(|| view.node_type_primary(&me.mesh, NodeKind::NodeAdmin).filter(|p| p.node_id != me.node_id).map(|p| p.node_id.clone()));
            match (own, commander) {
                (Some(own), Some(commander)) => own.serve(Some(&commander), None, req).await.expect("a state op"),
                (None, _) => rdm_core::reply::status::not_ready(format!("{me_name} has not yet joined its mesh; {} is refused", req.op())),
                (Some(_), None) => rdm_core::reply::status::not_ready(format!("{me_name}: its view holds no caller and names no primary of {} to check {} in to", me.mesh, req.op())),
            }
        }
    }

    /// A check-in, decided for the subject it names as this admin's view holds it, whoever calls
    /// (node-rpc.md §14): a mesh-primary's for its mesh counts at the fabric-primary, a member's at
    /// its mesh's primary.
    fn serve_up(&self, view: &Topology, req: &StatusRequest, op: &StateOp<'_>) -> StatusReply {
        let Some(me) = view.own(&self.env.node_id) else {
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "a view holding this admin".into() });
        };
        let me_name = self.held_name_in(view).to_string();
        let Some(subject) = view.births().find(|n| n.node_id == *op.node_id) else {
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
        };
        let held = Some((subject.incarnation_id.as_ref(), subject.name.to_string()));
        let held = held.as_ref().map(|(i, n)| (*i, n.as_str()));
        if me.is_fabric_primary && subject.kind == NodeKind::NodeAdmin && subject.is_primary && subject.mesh != me.mesh {
            let reply = self.fabric_book.check_in(&me_name, held, req);
            self.change_wake.notify_one();
            reply
        } else if me.is_primary && subject.mesh == me.mesh && subject.node_id != me.node_id {
            self.mesh_book.check_in(&me_name, held, req)
        } else {
            rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: format!("the fabric-primary, or the mesh-primary of {}", subject.mesh) })
        }
    }
}

/// The adopted status checklist's down op to each target, once, on its own task: the answer is named in
/// a span and nothing is retried. A target the view holds no birth of is named too. The op is a read
/// ([`ProbeNodeState`](rdm_core::status::StatusRequest::ProbeNodeState); node-status.md: `node.get`
/// manufactures no transition): the member re-publishes its presence and answers its state. A member
/// off its mesh is the tickle ladder's to command back (node-recovery.md step 2, Q87), never this
/// op's. A failed send is answered by nobody: no member leaves the checklist by a timer, and a new
/// primary re-sends.
fn send_down(me: &PathName, scope: &'static str, of: &str, targets: Vec<(String, Option<Node>)>, client: &Arc<NodeRpcClient>) {
    for (who, node) in targets {
        let (me, of, client) = (me.clone(), of.to_string(), client.clone());
        // The span opens before the call, so the call and the target's serve chain are its children.
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-state-down", node = %me, mesh = %of, scope, target = %who, outcome = tracing::field::Empty);
        tokio::spawn(
            async move {
                let outcome = match &node {
                    None => "no-birth-in-view".to_string(),
                    Some(n) => match n.incarnation_id.clone() {
                        None => "birth-has-no-incarnation".to_string(),
                        Some(incarnation) => {
                            // `node.get` of the exact birth: it reasserts itself and answers its state.
                            let birth = rdm_client::ExactBirth { target: NodeTarget::ExactNode(n.node_id.clone()), node_id: n.node_id.clone(), incarnation };
                            match rdm_client::NodeRpc::new(&client).get(&birth, &CallOptions::default()).await {
                                Ok(reply) => reply.name().to_string(),
                                Err(end) => end.rpc_name().to_string(),
                            }
                        }
                    },
                };
                tracing::Span::current().record("outcome", outcome.as_str());
                tracing::info!("the checklist's down op re-sent");
            }
            .instrument(span),
        );
    }
}

/// The meshes among `meshes` that are not ready-for-traffic in `view`, with the nodes behind each. A
/// mesh's state is the one the view projects (`Topology::mesh_unready` decides it).
fn mesh_blockers(view: &Topology, meshes: &[String], planned: &std::collections::BTreeMap<String, std::collections::BTreeSet<PathName>>) -> Vec<Missing> {
    let mut out = Vec::new();
    for mesh in meshes {
        match view.meshes.iter().find(|m| &m.name == mesh) {
            Some(m) => {
                let why = view.mesh_unready(mesh, planned.get(mesh));
                if !why.is_empty() {
                    out.extend(why);
                } else if m.status != crate::model::ScopeStatus::ReadyForTraffic {
                    out.push(Missing { who: mesh.clone(), why: "not-ready" });
                }
            }
            None => out.push(Missing { who: mesh.clone(), why: "no-mesh" }),
        }
    }
    out
}

/// The `sync_state` call: the application's function with the call's fields, its answer (or the
/// failure it reports by name, Q158) held for the fabric's change to judge. No application work answers at once, naming why.
async fn run_sync_state(hook: Option<SyncState>, me: PathName, sent: StateSyncHandoff, progress: Arc<Mutex<Progress>>, parent: tracing::Span) {
    let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-sync-state-hook", node = %me, build_id = %sent.build_id, attempt = sent.attempt, operation = %sent.operation, reason = tracing::field::Empty, outcome = tracing::field::Empty, "otel.kind" = "internal");
    tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-sync-state-called", node = %me, build_id = %sent.build_id, attempt = sent.attempt, operation = %sent.operation, bound = matches!(hook, Some(SyncState::Hook(_))), "otel.kind" = "internal")
        .in_scope(|| tracing::info!("sync_state is called with the state fields"));
    let answer = async {
        match hook {
            Some(SyncState::Hook(f)) => {
                match f(sent.clone()).await {
                    Ok(answer) => {
                        tracing::Span::current().record("outcome", "state-synced");
                        Progress::Answered(answer)
                    }
                    Err(failed) => {
                        tracing::Span::current().record("outcome", "failed").record("reason", failed.reason.as_str());
                        Progress::Failed(failed.reason)
                    }
                }
            }
            Some(SyncState::NoAppWork) => {
                tracing::Span::current().record("reason", "no-app-work").record("outcome", "state-synced");
                Progress::Answered(sent.clone())
            }
            None => {
                // Refused at start; unreachable.
                std::future::pending::<Progress>().await
            }
        }
    }
    .instrument(span)
    .await;
    *progress.lock().unwrap() = answer;
}

/// The blocker a builds store that could not be read names: the store's own error, never read as
/// "no accepted Build".
fn unreadable(e: &crate::build_state::BuildStateError) -> Missing {
    Missing { who: format!("build ({e})"), why: "build-store-unreadable" }
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

    /// CONTRACT (node-rpc-envelope.md, "Fabric status command pairs": "The state's command is scoped to the accepted
    /// Build, attempt and planned exact births"; states.md, The check-in checklist: it "names the accepted
    /// Build's PLANNED births"): a mesh checklist is superseded, named, once the accepted Build moved past
    /// it: a later attempt of the same Build, or a Build accepted after the checklist's own. A checklist of the
    /// accepted Build and attempt is current, and so is a checklist ahead of what this admin holds (an
    /// attempt it has not seen claimed, a Build accepted after the one it holds, or one its log has not
    /// learned): the command that opened it came ahead of this admin's log. Must NOT happen: a superseded
    /// checklist still commanding (gate red, tree b24a4303, fabric_states__rounds: bld-9abb attempt 1's
    /// mesh checklist, trace 95adb727, sent commit-state to mesh1.admin.2 at 816848 after the two-mesh
    /// Build bld-7c91 attempt 2 was accepted and its own checklist's commit-state was applied at 816816,
    /// trace a04036d1).
    #[tokio::test]
    async fn a_state_key_the_current_build_moved_past_is_superseded_and_one_ahead_of_this_admins_log_is_not() {
        use crate::build_state::{BuildAccepted, BuildProjection, MemoryBuildStateAdapter};
        let builds = MemoryBuildStateAdapter::new();
        let topology = FabricTopology { fabric: "fabric1".into(), meshes: BTreeMap::new() };
        let (one, two) = (crate::build::BuildId::parse("vm0khprh5mtj").unwrap(), crate::build::BuildId::parse("amrrv6tggykq").unwrap());
        for (b, at) in [(&one, 1_000), (&two, 2_000)] {
            builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: topology.clone(), submitted_change: None, submitted_at_ms: at }).await.unwrap();
        }
        let (b1, b2) = (builds.read_build(&one).await.unwrap(), builds.read_build(&two).await.unwrap());
        let fabric = FabricId::mint();
        let key = |b: &crate::build::BuildId, attempt: u32| StateKey { kind: StateCommand::StateCommit, fabric_id: fabric.clone(), build_id: b.to_string(), attempt };
        let (one_at_1, one_at_2, two_at_1) = (BuildProjection { attempt: 1, ..b1.clone() }, BuildProjection { attempt: 2, ..b1.clone() }, BuildProjection { attempt: 1, ..b2.clone() });
        assert_eq!(superseded_by(&key(&one, 1), &one_at_1, None), None, "the accepted Build and attempt: current");
        assert!(superseded_by(&key(&one, 1), &one_at_2, None).is_some(), "a later attempt of the same Build supersedes the checklist");
        assert_eq!(superseded_by(&key(&one, 3), &one_at_2, None), None, "an attempt ahead of the one this admin holds: its command came first");
        assert!(superseded_by(&key(&one, 1), &two_at_1, Some(&one_at_1)).is_some(), "a Build accepted after the checklist's own supersedes it");
        assert_eq!(superseded_by(&key(&two, 1), &one_at_1, Some(&two_at_1)), None, "a checklist of a Build accepted after the one this admin holds: its log lags");
        assert_eq!(superseded_by(&key(&crate::build::BuildId::parse("wvwg5gsy0868").unwrap(), 1), &two_at_1, None), None, "a checklist of a Build this admin's log has not learned: its command came ahead");
    }

    /// CONTRACT (node-rpc-envelope.md, "Fabric status command pairs": "The state's command is scoped to the accepted
    /// Build, attempt and planned exact births"): a mesh primary's owed completion of a checklist the
    /// accepted Build moved past is withdrawn when the checklist is superseded, by the same rule that stops
    /// the checklist commanding ([`superseded_by`]): a later attempt of the accepted Build, or a Build
    /// accepted after the checklist's own. The completion of the accepted Build's current attempt, of an
    /// attempt ahead of what this admin holds, and of a Build this admin's log has not learned stays
    /// owed. Must NOT happen: a superseded checklist's completion kept owed until a seat moves (gate red
    /// b24a4303, mesh_elections__sticky_seat: mesh2.admin.1 declared Round(.../2) and was answered
    /// NotReady "round holds 3, reported 2", then sent it again on every eligible event).
    #[tokio::test]
    async fn an_owed_completion_of_a_state_key_the_accepted_build_moved_past_is_withdrawn_when_it_is_superseded() {
        use crate::build_state::{BuildAccepted, BuildProjection, MemoryBuildStateAdapter};
        let builds = MemoryBuildStateAdapter::new();
        let topology = FabricTopology { fabric: "fabric1".into(), meshes: BTreeMap::new() };
        let (one, two) = (crate::build::BuildId::parse("vm0khprh5mtj").unwrap(), crate::build::BuildId::parse("amrrv6tggykq").unwrap());
        for (b, at) in [(&one, 1_000), (&two, 2_000)] {
            builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: topology.clone(), submitted_change: None, submitted_at_ms: at }).await.unwrap();
        }
        let accepted = BuildProjection { attempt: 2, ..builds.read_build(&two).await.unwrap() };
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let fabric = FabricId::mint();
        let declarer = Declarer::new();
        let owe = |build: &str, attempt: u32| {
            let state_key = StateKey { kind: StateCommand::StateCommit, fabric_id: fabric.clone(), build_id: build.to_string(), attempt };
            declarer.owe_in(Key::completion(&state_key), Authority::FabricPrimary, state_key.kind.up(&state_key, NodeId::mint(), IncarnationId::parse(&rdm_core::fixture_crockford60("inc")).unwrap()), None);
            Key::completion(&state_key)
        };
        let (earlier_attempt, earlier_build) = (owe("amrrv6tggykq", 1), owe("vm0khprh5mtj", 2));
        let (current, ahead, unlearned) = (owe("amrrv6tggykq", 2), owe("amrrv6tggykq", 3), owe("wvwg5gsy0868", 1));
        assert_eq!(declarer.owed(), 5);

        let capture = crate::span_capture::Capture::new("i143-s42-state-superseded");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        withdraw_superseded_completions(&declarer, &me, &accepted, &builds).await;
        let mut owed = declarer.owed_keys();
        owed.sort();
        let mut kept = vec![current, ahead, unlearned];
        kept.sort();
        assert_eq!(owed, kept, "the completions of an earlier attempt ({earlier_attempt:?}) and of an earlier Build ({earlier_build:?}) are withdrawn; a current, an ahead and an unlearned checklist's stay owed");
        // Each withdrawal is one span naming the completion and why the checklist is superseded.
        let spans = capture.named("rdm.node_admin.status.remove.via-completion-superseded");
        assert_eq!(spans.len(), 2, "one span per withdrawn completion: {spans:?}");
        for (key, why) in [(&earlier_attempt, "is at attempt 2, past this checklist's attempt 1"), (&earlier_build, "was accepted after this checklist's Build vm0khprh5mtj")] {
            let span = spans.iter().find(|s| s.attributes.get("key") == Some(&format!("{key:?}"))).unwrap_or_else(|| panic!("no span names {key:?}: {spans:?}"));
            assert!(span.attributes.get("superseded_by").is_some_and(|a| a.contains(why)), "{key:?} is withdrawn because {why}: {span:?}");
            eprintln!("span {} trace_id={} {:?}", span.name, span.trace_id, span.attributes);
        }
    }

    /// CONTRACT (fabric-state-sync.md, sequence: "all planned meshes ready; enter state-sync", then
    /// "state-sync hook (state + trace context)"; states.md, Resize and executable replacement: "A resize or an executable replacement puts the
    /// fabric back to pending"): a Build that changes the accepted topology while the fabric's
    /// state-sync has not gone out holds the change. The application is asked to sync nothing of a
    /// Build whose planned meshes the fabric did not find ready, and once state-sync went out the
    /// fabric is back to pending. Must NOT happen (gate red `e4s2-604f6d1c`, rounds.rs:201, "the
    /// application is called once per round of state-sync", left 2): `sync_state` called for the
    /// two-mesh Build at attempt 0 inside the one-mesh Build's state-sync (span `9962960417b0f787`,
    /// trace `9449f79f`), then again in that Build's own state-sync.
    #[tokio::test]
    async fn a_topology_accepted_before_state_sync_went_out_holds_the_change_and_asks_the_application_nothing() {
        use crate::accepted::MeshTopology;
        use crate::build::MeshDesired;
        use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let mesh = |m: &str| (m.to_string(), MeshTopology::of(&MeshDesired::of(m, [(NodeKind::NodeAdmin, 1)])));
        let one_mesh = FabricTopology { fabric: "fabric1".into(), meshes: [mesh("mesh1")].into() };
        let mut two_meshes = one_mesh.clone();
        two_meshes.meshes.extend([mesh("mesh2")]);
        let (one, two) = (crate::build::BuildId::parse("vm0khprh5mtj").unwrap(), crate::build::BuildId::parse("amrrv6tggykq").unwrap());
        builds.publish_accepted(&BuildAccepted { build_id: one.clone(), topology: one_mesh.clone(), submitted_change: None, submitted_at_ms: 1_000 }).await.unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: two.clone(), topology: two_meshes.clone(), submitted_change: None, submitted_at_ms: 2_000 }).await.unwrap();
        let current = builds.current().await.unwrap().expect("a current Build");
        assert_eq!((&current.build_id, current.attempt), (&two, 0), "the two-mesh Build is accepted and no attempt of it is claimed yet");
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = {
            let calls = calls.clone();
            SyncState::Hook(Arc::new(move |handoff: StateSyncHandoff| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(handoff) })
            }))
        };
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let records = Arc::new(Records::default());
        let fabric_states = FabricStates::new(StatesEnv {
            me: "mesh1.admin.1".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            fabric_id: FabricId::mint(),
            topology: Arc::new(RwLock::new(projected(Vec::new()))),
            client: Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new()))),
            builds: builds.clone(),
            contexts: Arc::new(AttemptContexts::in_memory()),
            declarer: Declarer::new(),
            wake: records.wake.clone(),
            records: records.clone(),
            hooks: FabricHooks { sync_state: Some(hook), traffic_opened: None },
            actions: Arc::new(NoScratchpad),
            acted: Arc::default(),
        });
        // The one-mesh Build's change entered state-sync; the frame that says so has not gone out yet.
        records.set_published_fabric_state(Some(FabricState::Pending));
        records.set_fabric_state(FabricState::StateSync);
        let mut change = fabric_states.change.lock().await;
        change.tracked = Some(one_mesh.clone());
        fabric_states.step_state_sync(&mut change).await;
        assert!(
            change.sync.is_none(),
            "no sync_state call is made for the two-mesh Build inside the one-mesh Build's state-sync: {:?}",
            change.sync.as_ref().map(|run| (run.sent.build_id.clone(), run.sent.attempt))
        );
        assert_eq!(records.fabric_state(), FabricState::StateSync, "the move back to pending waits until state-sync went out");
        // State-sync went out: the changed topology puts the fabric back to pending.
        records.set_published_fabric_state(Some(FabricState::StateSync));
        fabric_states.step_state_sync(&mut change).await;
        assert_eq!(records.fabric_state(), FabricState::Pending, "the changed topology puts the fabric back to pending");
        drop(change);
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0, "the application was asked to sync nothing");
    }

    /// The checklists of the admin `me`, as `view` holds it, over `builds` and `records`: a client on an
    /// endpoint of its own whose resolver names nobody, and `actions` for its own checklist work.
    async fn states_of(me: &Node, view: Topology, builds: Arc<dyn BuildStateAdapter>, records: Arc<Records>, fabric_id: FabricId, actions: Arc<dyn StateActions>) -> Arc<FabricStates> {
        states_resolving(me, view, builds, records, fabric_id, actions, Arc::new(rdm_transport::StaticResolver::new())).await
    }

    /// [`states_of`] whose client resolves through `resolver`.
    async fn states_resolving(me: &Node, view: Topology, builds: Arc<dyn BuildStateAdapter>, records: Arc<Records>, fabric_id: FabricId, actions: Arc<dyn StateActions>, resolver: Arc<rdm_transport::StaticResolver>) -> Arc<FabricStates> {
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        FabricStates::new(StatesEnv {
            me: me.name.clone(),
            node_id: me.node_id.clone(),
            incarnation: me.incarnation_id.clone().expect("a birth"),
            fabric_id,
            topology: Arc::new(RwLock::new(view)),
            client: Arc::new(NodeRpcClient::new(ep, resolver)),
            builds,
            contexts: Arc::new(AttemptContexts::in_memory()),
            declarer: Declarer::new(),
            wake: records.wake.clone(),
            records,
            hooks: FabricHooks { sync_state: Some(SyncState::NoAppWork), traffic_opened: None },
            actions,
            acted: Arc::default(),
        })
    }

    /// The two-mesh fabric's accepted Build, with attempts 1 to `attempts` claimed.
    async fn two_mesh_build(attempts: u32) -> (Arc<crate::build_state::MemoryBuildStateAdapter>, crate::build::BuildId) {
        use crate::accepted::MeshTopology;
        use crate::build::MeshDesired;
        use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let mesh = |m: &str| (m.to_string(), MeshTopology::of(&MeshDesired::of(m, [(NodeKind::NodeAdmin, 1)])));
        let id = crate::build::BuildId::parse("amrrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology { fabric: "fabric1".into(), meshes: [mesh("mesh1"), mesh("mesh2")].into() }, submitted_change: None, submitted_at_ms: 1_000 }).await.unwrap();
        for attempt in 1..=attempts {
            claim(&builds, &id, attempt).await;
        }
        (builds, id)
    }

    /// Open (after the first) and claim `attempt` of `id`.
    async fn claim(builds: &crate::build_state::MemoryBuildStateAdapter, id: &crate::build::BuildId, attempt: u32) {
        use crate::build_state::{AttemptOpened, AttemptReason, BuildAttemptClaim};
        if attempt > 1 {
            builds.open_attempt(&AttemptOpened { mesh: None, build_id: id.clone(), attempt, reason: AttemptReason::Restart, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 0 }).await.unwrap();
        }
        builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
    }

    /// The fabric-primary `mesh1.admin.1` and mesh2's primary, both ready-for-traffic.
    fn fabric_primary_and_mesh2() -> (Node, Node) {
        let mut fp = node("mesh1.admin.1", NodeStatus::ReadyForTraffic, true);
        fp.is_fabric_primary = true;
        (fp, node("mesh2.admin.1", NodeStatus::ReadyForTraffic, true))
    }

    /// The state command spans this capture holds toward `to`, by the event that made each due.
    fn commands_to(capture: &crate::span_capture::Capture, to: &str) -> Vec<String> {
        capture.named("rdm.node_admin.fabric.update.via-state-command").into_iter().filter(|s| s.attributes.get("to").map(String::as_str) == Some(to)).map(|s| s.attributes.get("event").cloned().unwrap_or_default()).collect()
    }

    /// [`commands_to`] once `n` command spans toward `to` have ended, or what ended within 10 s: a
    /// command's span ends with its task, which runs beside the cell.
    async fn commands_ended_to(capture: &crate::span_capture::Capture, to: &str, n: usize) -> Vec<String> {
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let ended = commands_to(capture, to);
            if ended.len() >= n || tokio::time::Instant::now() >= until {
                return ended;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// CONTRACT (i146 R7, the state commands' owner; states.md, The check-in checklist: "A new primary,
    /// through election or recovery, re-sends the current state's command. Members already done check in again,
    /// and the checklist rebuilds itself"; node-rpc.md §43: "Each owner defines how its pending
    /// commands, check-ins and responses are reconstructed from durable evidence with their operation
    /// identity"): a fabric-primary that holds no checklist book (a restart) and hears its fabric in
    /// state-commit reconstructs its pending commands from the published state and the accepted Build.
    /// It opens the checklist of the accepted Build's current attempt and commands mesh2's primary under
    /// that checklist's key. Must NOT happen: a checklist of another attempt, or a planned primary left
    /// uncommanded because the book that commanded it before is gone.
    #[tokio::test]
    async fn a_fabric_primary_with_no_checklists_reconstructs_its_state_commands_from_the_published_state_and_the_accepted_build() {
        let capture = crate::span_capture::Capture::new("i146-e3-state-commands-reconstructed");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (builds, id) = two_mesh_build(2).await;
        let (fp, m2) = fabric_primary_and_mesh2();
        let view = projected(vec![fp.clone(), m2.clone()]);
        let records = Arc::new(Records::default());
        let fabric_states = states_of(&fp, view.clone(), builds.clone(), records.clone(), FabricId::mint(), Arc::new(NoScratchpad)).await;
        fabric_states.change_step(ChangeInputs { is_fabric_primary: true, heard: Some("state-commit"), view: &view }).await;
        assert_eq!(records.fabric_state(), FabricState::StateCommit, "the published state is adopted");
        tokio::task::yield_now().await;
        fabric_states.fabric_book_reset();
        tokio::task::yield_now().await;
        let opened = capture.named("rdm.node_admin.fabric.update.via-checklist-opened");
        assert!(opened.iter().any(|s| s.attributes.get("command").map(String::as_str) == Some("commit-state") && s.attributes.get("build_id") == Some(&id.to_string()) && s.attributes.get("attempt").map(String::as_str) == Some("2")), "the checklist of the accepted Build's attempt 2 is opened: {opened:?}");
        assert_eq!(commands_ended_to(&capture, "mesh2.admin.1", 1).await, ["first"], "mesh2's primary is commanded once, under the reconstructed checklist");
    }

    /// CONTRACT (i146 R7; node-rpc-envelope.md, Fabric status command pairs: "The state's command is scoped to the
    /// accepted Build, attempt and planned exact births"): a lifecycle operation on a primary runs as an
    /// attempt of the accepted Build, and a new attempt re-keys the checklist: every planned primary is
    /// commanded again under the new attempt, one whose command the old attempt's checklist had answered
    /// among them. Must NOT happen: a primary left uncommanded because the old attempt's checklist answered it.
    #[tokio::test]
    async fn a_new_attempt_of_the_accepted_build_commands_every_planned_primary_again_under_its_own_state_key() {
        let capture = crate::span_capture::Capture::new("i146-e3-state-commands-new-attempt");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (builds, id) = two_mesh_build(1).await;
        let (fp, m2) = fabric_primary_and_mesh2();
        let view = projected(vec![fp.clone(), m2.clone()]);
        let fabric_id = FabricId::mint();
        let records = Arc::new(Records::default());
        let fabric_states = states_of(&fp, view.clone(), builds.clone(), records.clone(), fabric_id.clone(), Arc::new(NoScratchpad)).await;
        fabric_states.change_step(ChangeInputs { is_fabric_primary: true, heard: Some("state-commit"), view: &view }).await;
        let first = StateKey { kind: StateCommand::StateCommit, fabric_id, build_id: id.to_string(), attempt: 1 };
        fabric_states.fabric_book.answered(&first, &m2.node_id, m2.incarnation_id.as_ref().unwrap());
        claim(&builds, &id, 2).await;
        fabric_states.change_step(ChangeInputs { is_fabric_primary: true, heard: Some("state-commit"), view: &view }).await;
        tokio::task::yield_now().await;
        fabric_states.fabric_book_reset();
        tokio::task::yield_now().await;
        let attempts: Vec<String> = capture.named("rdm.node_admin.fabric.update.via-checklist-opened").into_iter().filter_map(|s| s.attributes.get("attempt").cloned()).collect();
        assert_eq!(attempts, ["1", "2"], "a checklist per attempt");
        assert_eq!(commands_ended_to(&capture, "mesh2.admin.1", 2).await, ["first", "first"], "mesh2's primary is commanded again under attempt 2's checklist");
    }

    /// CONTROL (i146 e3, R8; node-rpc.md §43: "Retry stops on the ack, on the receiver's proven exit or
    /// accepted removal, or on explicit cancellation or retargeting by the owning workflow; never on a
    /// timer, never on absence from a view"): a state command is one Owed call the delivery layer
    /// resends until the commanded primary answers. mesh2's primary is planned and never answers (its
    /// endpoint answers nothing); the one call is resent after the 10 s a bounded call ends on,
    /// the target reads unreachable and then reachable again in the view, and the checklist book sends
    /// nothing beside the call: one command hook, no `reachable-again`. Closing the checklist cancels the
    /// call. Must NOT happen: the call ending on its own deadline and the book commanding the
    /// primary again when its route returns (the per-protocol retry loop this owner had).
    #[tokio::test]
    async fn a_state_command_is_one_owed_call_and_a_returned_route_sends_nothing_beside_it() {
        let capture = crate::span_capture::Capture::new("i146-e3-state-command-control");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (builds, _) = two_mesh_build(1).await;
        let (fp, m2) = fabric_primary_and_mesh2();
        let ready = projected(vec![fp.clone(), m2.clone()]);
        let gone = projected(vec![fp.clone(), Node { status: NodeStatus::PendingReconnect, ..m2.clone() }]);
        // mesh2's primary resolves to an endpoint nobody holds, at a port nobody listens on.
        let resolver = Arc::new(rdm_transport::StaticResolver::new());
        let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        resolver.insert(rdm_transport::ResolvedNode { node_id: m2.node_id.clone(), name: m2.name.clone(), endpoint_id: iroh::SecretKey::generate().public(), transport_addr: silent.local_addr().unwrap(), incarnation: m2.incarnation_id.clone().unwrap() });
        let fabric_states = states_resolving(&fp, ready.clone(), builds, Arc::new(Records::default()), FabricId::mint(), Arc::new(NoScratchpad), resolver).await;
        let commanded = tokio::time::Instant::now();
        fabric_states.change_step(ChangeInputs { is_fabric_primary: true, heard: Some("state-commit"), view: &ready }).await;
        let resends = || rdm_transport::DeliveryStats::get(&fabric_states.env.client.delivery().stats().resends);
        // A bounded call ends 10 s in (`CallOptions::default`). The Owed call is resent after that mark,
        // whatever the cadence: one attempt is bounded by the staleness floor, and its backoff is capped there.
        tokio::time::sleep_until(commanded + std::time::Duration::from_secs(10)).await;
        let at_mark = resends();
        let floor = rdm_core::cadence::staleness_floor();
        tokio::time::timeout(floor * 2 + std::time::Duration::from_secs(10), async {
            while resends() <= at_mark {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the command to mesh2's primary is still out past the 10 s a bounded call ends on, and resent ({} resends, {at_mark} at the mark)", resends()));
        for view in [&gone, &gone, &ready, &ready] {
            fabric_states.change_step(ChangeInputs { is_fabric_primary: true, heard: Some("state-commit"), view }).await;
            tokio::task::yield_now().await;
        }
        let hooks = || capture.named("rdm.node_admin.fabric.update.via-state-hook").into_iter().filter(|s| s.attributes.get("command").map(String::as_str) == Some("commit-state")).count();
        assert_eq!(hooks(), 1, "one command was sent to mesh2's primary: the returned route sends nothing beside the call");
        fabric_states.fabric_book_reset();
        // The command's span ends when its task drops the call: awaited, never assumed after a fixed number of turns.
        let commands = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let ended = capture.named("rdm.node_admin.fabric.update.via-state-command");
                if !ended.is_empty() {
                    break ended;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("closing the checklist ends the command's call"));
        assert_eq!(commands.len(), 1, "one command, ended by the checklist's close: {commands:?}");
        assert!(commands[0].attributes.get("outcome").is_some_and(|o| o.contains("cancelled")), "closing the checklist cancels the owed call: {commands:?}");
        let calls: std::collections::BTreeSet<String> = capture.named("rdm.node_rpc.delivery.update.via-resend").into_iter().filter_map(|s| s.attributes.get("delivery_id").cloned()).collect();
        assert_eq!(calls.len(), 1, "every resend is the one call's: {calls:?}");
    }

    /// The state actions this cell counts.
    #[derive(Default)]
    struct Counted(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl StateActions for Counted {
        async fn commit_state(&self, _node: &PathName) -> Result<(), String> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn open_traffic(&self, _node: &PathName) -> Result<(), String> {
            Ok(())
        }
    }

    /// CONTRACT (i146 R7: the already-applied answer by operation identity to a resend; node-rpc-envelope.md,
    /// Fabric status command pairs: a successful admission reply is Applied or AlreadyApplied): a mesh primary
    /// that holds a checklist answers the same checklist's command again `AlreadyApplied`, by the checklist's key,
    /// and runs its own action once. A lost reply to the command is regenerated so: the resend is
    /// answered from the checklist this admin holds. Must NOT happen: `Applied` twice, or the action twice.
    #[tokio::test]
    async fn a_mesh_primary_answers_a_state_command_it_holds_already_applied_and_runs_its_action_once() {
        let (builds, id) = two_mesh_build(1).await;
        let (fp, m2) = fabric_primary_and_mesh2();
        let view = projected(vec![fp.clone(), m2.clone()]);
        let fabric_id = FabricId::mint();
        let actions = Arc::new(Counted::default());
        let fabric_states = states_of(&m2, view.clone(), builds, Arc::new(Records::default()), fabric_id.clone(), actions.clone()).await;
        let key = StateKey { kind: StateCommand::StateCommit, fabric_id, build_id: id.to_string(), attempt: 1 };
        let req = StateCommand::StateCommit.down(&key, m2.node_id.clone(), m2.incarnation_id.clone().unwrap());
        assert_eq!(fabric_states.serve(&view, Some(&fp), None, &req, None).await, StatusReply::Applied, "the checklist's first command is admitted");
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert_eq!(fabric_states.serve(&view, Some(&fp), None, &req, None).await, StatusReply::AlreadyApplied, "the same checklist again is the checklist already held");
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert_eq!(actions.0.load(std::sync::atomic::Ordering::SeqCst), 1, "the mesh primary's own action runs once for the checklist");
    }

    /// CONTRACT (R-S2): only an admission ends a state command. A refusal by name rests on the receiver's own view, which
    /// heals, and a call that got no reply never reached it: both wait for an eligible event. Must NOT happen: a
    /// refusal that is final, which strands the checklist when a new primary commands members that still name the old one.
    #[test]
    fn only_an_admission_ends_a_state_command() {
        for admitted in [StatusReply::Applied, StatusReply::AlreadyApplied] {
            assert!(matches!(delivery_of(Some(&admitted), "Reply"), Delivery::Answered(_)), "{admitted:?}");
        }
        let refusal = StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed: "the mesh-primary of mesh1".into() } };
        assert!(matches!(delivery_of(Some(&refusal), "Reply"), Delivery::Undelivered { typed: true, .. }), "a refusal by name is undelivered, typed");
        let not_ready = StatusReply::NotReady { reason: "authority not filled".into() };
        assert!(matches!(delivery_of(Some(&not_ready), "Reply"), Delivery::Undelivered { typed: true, .. }));
        assert!(matches!(delivery_of(None, "NotSent"), Delivery::Undelivered { typed: false, .. }), "no reply: untyped");
    }

    /// CONTRACT (R-RC1): a replace is a delete and a create at the same path.name with a new node
    /// id, so while the retired birth's row is still in a view, two rows carry this admin's
    /// path.name. A state op's receiver finds its own row by its node id, the identity it was born
    /// with. What must NOT happen: the retired birth's row taken as this admin's, which refuses a
    /// state command naming this admin "the subject node itself" and strands the checklist.
    #[test]
    fn an_admin_finds_its_own_row_by_node_id_while_its_superseded_birth_is_still_in_the_view() {
        let retired = node("mesh1.admin.1", NodeStatus::ConnectionsDeleting, false);
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
        assert!(one_pending.node_type_primary("mesh1", NodeKind::NodeAdmin).is_some(), "mesh1 has its primary");
        let blockers: Vec<String> = mesh_blockers(&one_pending, &meshes, &planned).iter().map(|m| m.to_string()).collect();
        assert_eq!(blockers, ["mesh1.rpc.1(not-ready)"], "the one node that is not ready is what blocks entry");
    }

    /// CONTRACT (states.md, Resize and executable replacement: a Build that changes the accepted topology
    /// puts the fabric back to pending; a primary change or a stop "leaves the topology unchanged"): an
    /// admin that held the current Build as heard on gossip (no executables) and, fabric-primary now, reads
    /// it from the store root (executables named) holds two copies of one Build's topology. The fabric
    /// stays ready-for-traffic; a different Build that names a new executable puts it back to pending.
    /// Must NOT happen: the same Build read from another copy taken as "the accepted topology changed",
    /// the fabric reset to pending by a hand-over (a lost fabric-primary's heir held in state-commit).
    #[tokio::test]
    async fn the_same_build_read_from_another_copy_is_no_resize_and_a_new_executable_is() {
        use crate::accepted::MeshTopology;
        use crate::build::MeshDesired;
        use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let exe = |sha: &str| rdm_core::binding::Binding { launch_id: "rpc_node".into(), executable: "/b/rpc".into(), sha256: sha.into(), image: None };
        let topology = |sha: Option<&str>| {
            let mut m = MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]));
            let rpc: PathName = "mesh1.rpc.1".parse().unwrap();
            let mut c = m.path_config(&rpc).cloned().unwrap();
            c.executable = sha.map(exe);
            m.set_path_config(&rpc, c).unwrap();
            FabricTopology { fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m)].into() }
        };
        let a = crate::build::BuildId::parse("amrrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: a.clone(), topology: topology(Some("aa")), submitted_change: None, submitted_at_ms: 1_000 }).await.unwrap();
        claim(&builds, &a, 1).await;
        let (fp, _) = fabric_primary_and_mesh2();
        let view = projected(vec![fp.clone()]);
        let records = Arc::new(Records::default());
        let states = states_of(&fp, view, builds.clone(), records.clone(), FabricId::mint(), Arc::new(NoScratchpad)).await;
        records.set_fabric_state(FabricState::ReadyForTraffic);
        records.set_published_fabric_state(Some(FabricState::ReadyForTraffic));
        {
            let mut change = states.change.lock().await;
            // As heard on gossip: the same Build, no executable on its entries.
            change.tracked = Some(topology(None));
            change.tracked_build = Some(a.clone());
            states.step_ready(&mut change).await;
        }
        assert_eq!(records.fabric_state(), FabricState::ReadyForTraffic, "the same Build read from the store root is no resize: the fabric stays ready-for-traffic");
        let b = crate::build::BuildId::parse("bnrrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: topology(Some("bb")), submitted_change: None, submitted_at_ms: 2_000 }).await.unwrap();
        claim(&builds, &b, 1).await;
        {
            let mut change = states.change.lock().await;
            states.step_ready(&mut change).await;
        }
        assert_eq!(records.fabric_state(), FabricState::Pending, "a new Build that names a new executable puts the fabric back to pending");
    }

    /// CONTRACT (states.md, Resize and executable replacement: "A resize or an executable replacement puts the
    /// fabric back to pending"; build-deployment.md §3: a member is replaced when its row records another
    /// executable than its entry names): day 0's entry for the Day-0 admin names no executable, and its row
    /// records the one it runs. A Build whose request names, for that path, the executable its row records
    /// is neither a resize nor a replacement: the fabric stays ready-for-traffic and the change holds the
    /// new Build. A later Build naming another executable for it is a replacement and puts the fabric back
    /// to pending. Must NOT happen: a request naming what the admin already runs taken as "the accepted
    /// topology changed", the fabric reset to pending for a Build that has nothing to do.
    #[tokio::test]
    async fn a_request_naming_the_executable_the_row_records_is_no_replacement_and_another_executable_is() {
        use crate::accepted::MeshTopology;
        use crate::build::MeshDesired;
        use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
        use crate::node_rows::WriteRecord;
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let admin: PathName = "mesh1.admin.1".parse().unwrap();
        let exe = |sha: &str| rdm_core::binding::Binding { launch_id: "node_admin".into(), executable: "/b/admin".into(), sha256: sha.into(), image: None };
        let day0 = MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]));
        let topology = |sha: Option<&str>| {
            let mut m = day0.clone();
            let mut c = m.path_config(&admin).cloned().unwrap();
            c.executable = sha.map(exe);
            m.set_path_config(&admin, c).unwrap();
            FabricTopology { fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m)].into() }
        };
        let (fp, _) = fabric_primary_and_mesh2();
        // The Day-0 admin's row records the executable it runs.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        let mut row = crate::storage::NodeRow::new(admin.clone(), fp.node_id.clone(), NodeStatus::ReadyForTraffic);
        row.executable = Some(exe("aa"));
        rows.write_record(&row).await.unwrap();
        let a = crate::build::BuildId::parse("amrrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: a.clone(), topology: topology(None), submitted_change: None, submitted_at_ms: 1_000 }).await.unwrap();
        claim(&builds, &a, 1).await;
        let view = projected(vec![fp.clone()]);
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(rows.clone());
        let states = states_of(&fp, view, builds.clone(), records.clone(), FabricId::mint(), Arc::new(NoScratchpad)).await;
        records.set_fabric_state(FabricState::ReadyForTraffic);
        records.set_published_fabric_state(Some(FabricState::ReadyForTraffic));
        {
            let mut change = states.change.lock().await;
            states.step_ready(&mut change).await;
            assert_eq!(change.tracked_build, Some(a.clone()), "day 0's Build is the topology the fabric changes under");
        }
        let b = crate::build::BuildId::parse("bnrrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: topology(Some("aa")), submitted_change: None, submitted_at_ms: 2_000 }).await.unwrap();
        claim(&builds, &b, 1).await;
        assert!(!states.accepted().await, "a request naming the executable the row records puts nothing back at its acceptance");
        {
            let mut change = states.change.lock().await;
            states.step_ready(&mut change).await;
            assert_eq!(change.tracked_build, Some(b.clone()), "the change holds the new Build's topology");
        }
        assert_eq!(records.fabric_state(), FabricState::ReadyForTraffic, "the executable the row records is no replacement: the fabric stays ready-for-traffic");
        let c = crate::build::BuildId::parse("cprrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: c.clone(), topology: topology(Some("cc")), submitted_change: None, submitted_at_ms: 3_000 }).await.unwrap();
        claim(&builds, &c, 1).await;
        assert!(states.accepted().await, "another executable than the row records is a replacement: the fabric is back to pending at its acceptance");
        assert_eq!(records.fabric_state(), FabricState::Pending, "an executable replacement puts the fabric back to pending");
    }

    /// CONTRACT (i146 Q45; states.md, The check-in checklist: "A new primary, through election or recovery,
    /// re-sends the current state's command. Members already done check in again"; `fabric_book_reset`: "a checklist
    /// it opens again re-runs this admin's own mesh checklist, whose members check in again from their
    /// record"): the fabric-primary's own mesh checklist is open, its planned member commanded and not yet
    /// checked in, when the tenure that opened it ends. Opened again, the checklist is driven again: its own
    /// action from the record, and the member commanded again under the reopened checklist. Must NOT
    /// happen: the reopened checklist with no driver, its member never commanded again, so the fabric holds
    /// in state-commit on the fabric-primary's own check-in (release run 5 at 7e13f37e, owed_check_ins
    /// state-commit, "mesh1.admin.1(no-check-in)" for 57 s).
    #[tokio::test]
    async fn a_fabric_primary_whose_tenure_ended_drives_its_own_mesh_checklist_again_when_it_opens_it_again() {
        use crate::accepted::MeshTopology;
        use crate::build::MeshDesired;
        use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
        let capture = crate::span_capture::Capture::new("i146-e3-reopened-checklist-driven");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let id = crate::build::BuildId::parse("amrrv6tggykq").unwrap();
        let meshes = [
            ("mesh1".to_string(), MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]))),
            ("mesh2".to_string(), MeshTopology::of(&MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1)]))),
        ];
        builds.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology { fabric: "fabric1".into(), meshes: meshes.into() }, submitted_change: None, submitted_at_ms: 1_000 }).await.unwrap();
        claim(&builds, &id, 1).await;
        let (fp, m2) = fabric_primary_and_mesh2();
        let member = node("mesh1.rpc.1", NodeStatus::ReadyForTraffic, false);
        let view = projected(vec![fp.clone(), member, m2]);
        let records = Arc::new(Records::default());
        let fabric_states = states_of(&fp, view.clone(), builds.clone(), records.clone(), FabricId::mint(), Arc::new(NoScratchpad)).await;
        fabric_states.change_step(ChangeInputs { is_fabric_primary: true, heard: Some("state-commit"), view: &view }).await;
        let settle = || async {
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        };
        settle().await;
        // The tenure ends: its checklists and its own mesh's checklists close, and the checklist's driver runs on the
        // close before the change opens the checklist again.
        fabric_states.fabric_book_reset();
        settle().await;
        fabric_states.change_step(ChangeInputs { is_fabric_primary: true, heard: Some("state-commit"), view: &view }).await;
        settle().await;
        // Closing the reopened checklist ends what it holds open, so every span of the cell is read.
        fabric_states.fabric_book_reset();
        let ended = commands_ended_to(&capture, "mesh1.rpc.1", 2).await;
        assert_eq!(ended.len(), 2, "the member is commanded under each opening of the checklist: {ended:?}");
        let own = mesh_checklists_ended(&capture, 2).await;
        assert_eq!(own, ["run", "from-record"], "the reopened checklist has a driver of its own, which takes this admin's action from its record");
    }

    use crate::model::{MeshId, NodeStatus as NS};
    use rdm_transport::topology::DigestBook;

    fn member(name: &str, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = NS::ReadyForTraffic;
        n.incarnation_id = Some(IncarnationId::mint());
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        n
    }

    /// A degraded fabric of two meshes, holding `nodes`.
    fn view(nodes: Vec<Node>) -> Topology {
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Degraded, provider: ProviderKind::Process },
            meshes: ["mesh1", "mesh2"].iter().map(|m| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::ReadyForTraffic }).collect(),
            nodes,
        }
    }

    fn digest(p: &PathName, status: MemberStatus) -> rdm_core::MeshDigest {
        rdm_core::MeshDigest {
            held_hash: Default::default(),
            fabric_id: FabricId::mint(),
            node: rdm_core::MeshNode {
                node_id: NodeId::mint(),
                name: p.clone(),
                endpoint_id: crate::model::EndpointId(String::new()),
                transport_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                runtime: None,
            },
            status,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 0,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            config: Default::default(),
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        }
    }

    /// The checklists of `me` over `view` and `records`, with a Build log that holds nothing.
    async fn adopted_states_of(me: &Node, view: Topology, records: Arc<Records>) -> Arc<FabricStates> {
        states_of(me, view, Arc::new(crate::build_state::MemoryBuildStateAdapter::new()), records, FabricId::mint(), Arc::new(NoScratchpad)).await
    }

    /// The `via-state-down` spans toward `target` once `n` have ended, or what ended within 15 s: a
    /// down op's span ends with its task, which runs beside the cell.
    async fn down_ops_ended_to(capture: &crate::span_capture::Capture, target: &str, n: usize) -> Vec<crate::span_capture::Captured> {
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            let ended: Vec<_> = capture.named("rdm.node_admin.mesh.update.via-state-down").into_iter().filter(|s| s.attributes.get("target").map(String::as_str) == Some(target)).collect();
            if ended.len() >= n || tokio::time::Instant::now() >= until {
                return ended;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// CONTRACT (states.md, "The check-in checklist": "A new primary, through election or recovery,
    /// re-sends the current state's command. Members already done check in again, and the checklist rebuilds
    /// itself"; Q87: the checklist's down op is a read): the checklist a new mesh primary runs for the status
    /// it adopted is run by this admin's checklists, the one owner of every checklist. The down op goes to
    /// every planned birth but this admin, once per opening of the checklist; a digest heard before the
    /// checklist began is no check-in, and the checklist completes on a digest heard after it began. A status
    /// adopted again opens the checklist again and the checklist rebuilds itself: the down op goes again,
    /// and the digest that completed the earlier opening counts for nothing. Must NOT happen: a second
    /// owner with a checklist of its own, or a checklist carried from one opening to the next.
    #[tokio::test]
    async fn an_adopted_status_checklist_is_this_admins_states_own_and_rebuilds_per_opening() {
        let capture = crate::span_capture::Capture::new("i143-one-checklist-owner");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (me, rpc) = (member("mesh1.admin.1", true, false), member("mesh1.rpc.1", false, false));
        let v = view(vec![me.clone(), rpc.clone()]);
        let fabric_states = adopted_states_of(&me, v.clone(), Arc::new(Records::default())).await;
        let book = DigestBook::default();
        let awaiting = Announced { mesh_awaiting_checklist: true, fabric_awaiting_checklist: false };
        let planned = vec![me.name.clone(), rpc.name.clone()];
        let inputs = || AdoptedInputs { view: &v, planned: Some(planned.clone()), book: &book };
        let held = || capture.named("rdm.node_admin.mesh.update.via-checklist-held").into_iter().map(|s| s.attributes.get("missing").cloned().unwrap_or_default()).collect::<Vec<_>>();
        // The member was heard before the checklist began: held, not checked in.
        let mut heard = digest(&rpc.name, MemberStatus::ReadyForTraffic);
        assert!(book.record(heard.clone()));
        assert!(!fabric_states.adopted_step(awaiting, inputs()).mesh, "a digest heard before the checklist is not a check-in");
        assert_eq!(held(), ["mesh1.rpc.1(not-heard-since-opening)"], "the hold names the planned birth that has not checked in");
        heard.digest_seq += 1;
        assert!(book.record(heard.clone()));
        assert!(fabric_states.adopted_step(awaiting, inputs()).mesh, "every planned birth checked in after the checklist began");
        let complete = capture.named("rdm.node_admin.mesh.update.via-checklist-complete");
        assert_eq!(complete.len(), 1, "the checklist completed once: {complete:?}");
        assert_eq!(complete[0].attributes.get("members").map(String::as_str), Some("2"));
        // The adopted status went out: no checklist is open. Adopted again, the checklist opens and the checklist rebuilds itself.
        fabric_states.adopted_end();
        assert!(!fabric_states.adopted_step(awaiting, inputs()).mesh, "the checklist rebuilt: the digest that completed the earlier opening is no check-in");
        assert_eq!(held().len(), 2, "the hold is named again for the new opening");
        heard.digest_seq += 1;
        assert!(book.record(heard));
        assert!(fabric_states.adopted_step(awaiting, inputs()).mesh);
        let down = down_ops_ended_to(&capture, "mesh1.rpc.1", 2).await;
        assert_eq!(down.len(), 2, "the down op went to the planned member once per opening: {down:?}");
        assert!(down.iter().all(|s| s.attributes.get("scope").map(String::as_str) == Some("mesh") && s.attributes.get("mesh").map(String::as_str) == Some("mesh1")), "{down:?}");
    }

    /// CONTRACT (R-S2): the fabric-primary sends the down op to a mesh's primary when its view of
    /// that primary appears or changes, once each, and to nobody when it is not the fabric-primary.
    #[tokio::test]
    async fn fabric_primary_addresses_a_mesh_primary_when_its_view_of_it_changes() {
        let me = member("mesh1.admin.1", true, true);
        let first = member("mesh2.admin.1", true, false);
        let v0 = view(vec![me.clone()]);
        let fabric_states = adopted_states_of(&me, v0.clone(), Arc::new(Records::default())).await;
        assert!(fabric_states.changed_mesh_primaries(&v0, true).is_empty(), "no other mesh primary in view: nobody to address");

        let v1 = view(vec![me.clone(), first.clone()]);
        let sent: Vec<String> = fabric_states.changed_mesh_primaries(&v1, true).into_iter().map(|(m, _)| m).collect();
        assert_eq!(sent, vec!["mesh2".to_string()], "a mesh primary gained by the view is addressed");
        assert!(fabric_states.changed_mesh_primaries(&v1, true).is_empty(), "the same view addresses nobody twice");

        let mut recovered = first.clone();
        recovered.incarnation_id = Some(IncarnationId::mint());
        let v2 = view(vec![me.clone(), recovered]);
        assert_eq!(fabric_states.changed_mesh_primaries(&v2, true).len(), 1, "a new birth of the mesh primary is addressed");

        // The primary was in view before its endpoint was: gaining the endpoint it is resolved by
        // is a change, so the down op goes again once the receiver can resolve the sender.
        let mut early = first.clone();
        early.endpoint_id = None;
        let mut v_early = view(vec![me.clone(), early.clone()]);
        let fabric_states = adopted_states_of(&me, v_early.clone(), Arc::new(Records::default())).await;
        assert_eq!(fabric_states.changed_mesh_primaries(&v_early, true).len(), 1);
        assert!(fabric_states.changed_mesh_primaries(&v_early, true).is_empty());
        for n in &mut v_early.nodes {
            if n.name == early.name {
                n.endpoint_id = Some(crate::model::EndpointId("ep-late".into()));
            }
        }
        assert_eq!(fabric_states.changed_mesh_primaries(&v_early, true).len(), 1, "gaining the endpoint it is resolved by is a change");
    }

    // @feature: node-lifecycle
    #[tokio::test]
    async fn successor_fabric_primary_keeps_degraded_until_every_mesh_primary_reports_for_its_checklist() {
        // The seat moved to mesh2's primary while mesh1 was being recovered; the new holder adopted degraded.
        let (me, recovered) = (member("mesh2.admin.1", true, true), member("mesh1.admin.2", true, false));
        let v = view(vec![me.clone(), recovered.clone()]);
        let records = Arc::new(Records::default());
        records.set_adopted_degraded(true);
        let book = DigestBook::default();
        let fabric_states = adopted_states_of(&me, v.clone(), records.clone()).await;
        let awaiting = Announced { mesh_awaiting_checklist: false, fabric_awaiting_checklist: true };
        let inputs = || AdoptedInputs { view: &v, planned: None, book: &book };
        // A ready primary of a newer birth is in the view; nobody has reported to this holder.
        let done = fabric_states.adopted_step(awaiting, inputs());
        assert!(!done.fabric && records.adopted_degraded(), "the topology reading alone does not clear degraded");
        // A report from a birth that is not the primary now does not either.
        records.declared.lock().unwrap().record_report("mesh1", crate::status_rpc::Reporter::Birth(IncarnationId::mint()), Instant::now());
        let done = fabric_states.adopted_step(awaiting, inputs());
        assert!(!done.fabric && records.adopted_degraded(), "a report of another birth is not the current primary's");
        // The current primary re-declares to the new holder: the checklist is complete and degraded clears.
        records.declared.lock().unwrap().record_report("mesh1", crate::status_rpc::Reporter::Birth(recovered.incarnation_id.clone().unwrap()), Instant::now());
        let done = fabric_states.adopted_step(awaiting, inputs());
        assert!(done.fabric && !records.adopted_degraded(), "every mesh primary reported for this checklist");
    }

    /// CONTRACT (mesh-delete.md: the mesh row goes Dead at mesh.deleted, and its final primary's end is its
    /// own node.delete, step 7; Luke 2026-10-24: mesh death is read from the Mesh record, never from a view
    /// that has not caught up): a fabric-primary that took over after mesh3's mesh.delete adopts the
    /// fabric's status and holds it until every mesh primary reports. mesh3 is Dead, its final primary
    /// still a live member of the view that never reports: the adopted status completes on mesh1's report
    /// alone. Must NOT happen: the adopted status held for a Dead mesh's report, forever.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_successor_fabric_primary_never_waits_for_a_dead_meshs_report() {
        let (me, other, last) = (member("mesh2.admin.1", true, true), member("mesh1.admin.2", true, false), member("mesh3.admin.1", true, false));
        let mut v = view(vec![me.clone(), other.clone(), last]);
        v.meshes.push(Mesh { id: Some(MeshId::mint()), name: "mesh3".into(), status: ScopeStatus::Dead });
        let records = Arc::new(Records::default());
        records.set_adopted_degraded(true);
        let book = DigestBook::default();
        let adopted = adopted_states_of(&me, v.clone(), records.clone()).await;
        let awaiting = Announced { mesh_awaiting_checklist: false, fabric_awaiting_checklist: true };
        let inputs = || AdoptedInputs { view: &v, planned: None, book: &book };
        assert!(!adopted.adopted_step(awaiting, inputs()).fabric, "mesh1's primary has not reported yet");
        records.declared.lock().unwrap().record_report("mesh1", crate::status_rpc::Reporter::Birth(other.incarnation_id.clone().unwrap()), Instant::now());
        assert!(adopted.adopted_step(awaiting, inputs()).fabric, "every mesh that is not Dead reported: the Dead mesh3 is no mesh to report");
    }

    // @feature: node-lifecycle
    #[tokio::test]
    async fn elected_mesh_primary_completes_its_checklist_only_when_every_planned_birth_checks_in_after_it_began() {
        let (me, rpc) = (member("mesh1.admin.1", true, false), member("mesh1.rpc.1", false, false));
        let v = view(vec![me.clone(), rpc.clone()]);
        let book = DigestBook::default();
        let fabric_states = adopted_states_of(&me, v.clone(), Arc::new(Records::default())).await;
        let awaiting = Announced { mesh_awaiting_checklist: true, fabric_awaiting_checklist: false };
        let planned = vec![me.name.clone(), rpc.name.clone()];
        let inputs = || AdoptedInputs { view: &v, planned: Some(planned.clone()), book: &book };
        // The member was heard before the checklist began (it is held, but it has not checked in).
        let before = digest(&rpc.name, MemberStatus::ReadyForTraffic);
        assert!(book.record(before.clone()));
        let done = fabric_states.adopted_step(awaiting, inputs());
        assert!(!done.mesh, "a digest heard before the checklist is not a check-in");
        // Its next heartbeat arrives after the checklist began: it has checked in.
        let mut again = before;
        again.digest_seq += 1;
        assert!(book.record(again));
        let done = fabric_states.adopted_step(awaiting, inputs());
        assert!(done.mesh, "every planned birth checked in after the checklist began");
        // An unreadable accepted Build holds the checklist by name.
        let unreadable = adopted_states_of(&me, v.clone(), Arc::new(Records::default())).await;
        let done = unreadable.adopted_step(awaiting, AdoptedInputs { view: &v, planned: None, book: &book });
        assert!(!done.mesh);
    }

    /// The `own_action` of each ended `via-mesh-checklist` span, in end order, once `n` have ended or
    /// within 10 s.
    async fn mesh_checklists_ended(capture: &crate::span_capture::Capture, n: usize) -> Vec<String> {
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let ended: Vec<String> = capture.named("rdm.node_admin.fabric.update.via-mesh-checklist").into_iter().map(|s| s.attributes.get("own_action").cloned().unwrap_or_default()).collect();
            if ended.len() >= n || tokio::time::Instant::now() >= until {
                return ended;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// What the pages say a Reconcile variant does to a ready-for-traffic fabric at its acceptance.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Back {
        /// A resize or an executable replacement: back to pending (states.md "Resize and executable replacement").
        ToPending,
        /// Neither: the fabric stays ready-for-traffic.
        Stays,
    }

    /// CONTRACT (states.md "Resize and executable replacement": "A resize or an executable replacement puts the
    /// fabric back to pending"; build.md: a topology-changing request compiles to a new complete Build;
    /// build-deployment.md §3: a member is replaced when its row records another executable than its entry
    /// names): the Reconcile variant matrix's acceptance rows (the drive's rows, and why no cell held the gap,
    /// are tests/reconcile_variants.rs). Each request is compiled by the door's compiler against a Day-0
    /// fabric (the Day-0 admin's entry names no executable, its row records `aa`; mesh1.rpc.1's entry and row
    /// name `rr`), accepted while the fabric is ready-for-traffic, and the fabric's state read after the
    /// acceptance and after the ready-for-traffic step. A request naming the executable a row records, an
    /// unchanged request, a tags-only and a placement-only request (`PlacementMeta` declares no constraint) are
    /// neither a resize nor a replacement: the fabric stays ready-for-traffic. A path added or removed (a
    /// resize) and another executable (a replacement, the Day-0 admin's included) put it back to pending. A
    /// storage-only change of a held path is neither (proposal executable-bindings-at-create.md rule 5: a
    /// storage or placement change re-births a path only "with its executable"): the fabric stays
    /// ready-for-traffic. Must NOT happen: a request naming what a node already runs, or changing only a
    /// held path's storage, reset the fabric to pending for a Build with nothing to do (Rafka 141).
    #[tokio::test]
    async fn a_reconcile_puts_the_fabric_back_to_pending_only_for_a_resize_or_an_executable_replacement() {
        use crate::accepted::{compile_against, MeshTopology, Recorded, RecordedRow, TopologyChange};
        use crate::build::MeshDesired;
        use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
        use crate::node_rows::WriteRecord;
        use rdm_core::meta::{PathConfig, PersistentRetireDisposition, PlacementMeta, StorageMeta};
        type Edit = fn(&mut Vec<(PathName, PathConfig)>);
        let exe = |id: &str, sha: &str| rdm_core::binding::Binding { launch_id: id.into(), executable: format!("/b/{id}-{sha}").into(), sha256: sha.into(), image: None };
        let admin: PathName = "mesh1.admin.1".parse().unwrap();
        let rpc: PathName = "mesh1.rpc.1".parse().unwrap();
        let held = |admin_entry: Option<&str>| {
            let mut m = MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]));
            let mut c = m.path_config(&admin).cloned().unwrap();
            c.executable = admin_entry.map(|sha| exe("node_admin", sha));
            m.set_path_config(&admin, c).unwrap();
            let mut c = m.path_config(&rpc).cloned().unwrap();
            c.executable = Some(exe("rpc_node", "rr"));
            m.set_path_config(&rpc, c).unwrap();
            FabricTopology { fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m)].into() }
        };
        fn at<'a>(r: &'a mut [(PathName, PathConfig)], p: &str) -> &'a mut PathConfig {
            &mut r.iter_mut().find(|(q, _)| q.to_string() == p).expect("the request lists the path").1
        }
        let rows: [(&str, Option<&str>, Edit, Back); 10] = [
            ("the Day-0 admin named on the executable its row records (its entry names none)", None, |r| at(r, "mesh1.admin.1").executable = Some(rdm_core::binding::Binding { launch_id: "node_admin".into(), executable: "/b/node_admin-aa".into(), sha256: "aa".into(), image: None }), Back::Stays),
            ("every path named on the executable its entry names", Some("aa"), |_| {}, Back::Stays),
            ("the unchanged request: every held path naming none", None, |r| r.iter_mut().for_each(|(_, c)| c.executable = None), Back::Stays),
            ("tags only", None, |r| at(r, "mesh1.rpc.1").tags = ["zone_a".to_string()].into(), Back::Stays),
            ("placement only (PlacementMeta declares no constraint)", None, |r| at(r, "mesh1.rpc.1").placement = PlacementMeta::default(), Back::Stays),
            ("another executable for mesh1.rpc.1 (a replacement)", None, |r| at(r, "mesh1.rpc.1").executable = Some(rdm_core::binding::Binding { launch_id: "rpc_node".into(), executable: "/b/rpc_node-cc".into(), sha256: "cc".into(), image: None }), Back::ToPending),
            ("another executable for the Day-0 admin (a replacement)", None, |r| at(r, "mesh1.admin.1").executable = Some(rdm_core::binding::Binding { launch_id: "node_admin".into(), executable: "/b/node_admin-cc".into(), sha256: "cc".into(), image: None }), Back::ToPending),
            ("a new path (a resize)", None, |r| {
                let c = at(r, "mesh1.rpc.1").clone();
                r.push(("mesh1.rpc.2".parse().unwrap(), c));
            }, Back::ToPending),
            ("a removed path (a resize)", None, |r| r.retain(|(p, _)| p.to_string() != "mesh1.rpc.1"), Back::ToPending),
            ("storage only", None, |r| at(r, "mesh1.rpc.1").storage = StorageMeta::Persistent { on_retire: PersistentRetireDisposition::Preserve }, Back::Stays),
        ];
        let mut red = Vec::new();
        for (what, admin_entry, edit, want) in rows {
            let builds = Arc::new(MemoryBuildStateAdapter::new());
            let topology = held(admin_entry);
            let a = crate::build::BuildId::parse("amrrv6tggykq").unwrap();
            builds.publish_accepted(&BuildAccepted { build_id: a.clone(), topology: topology.clone(), submitted_change: None, submitted_at_ms: 1_000 }).await.unwrap();
            claim(&builds, &a, 1).await;
            let (fp, _) = fabric_primary_and_mesh2();
            let store = Arc::new(crate::storage::MemoryNodesStorage::default());
            let mut recorded = Recorded::new();
            for (path, node_id, sha, id) in [(&admin, fp.node_id.clone(), "aa", "node_admin"), (&rpc, NodeId::mint(), "rr", "rpc_node")] {
                let mut row = crate::storage::NodeRow::new(path.clone(), node_id.clone(), NodeStatus::ReadyForTraffic);
                row.executable = Some(exe(id, sha));
                store.write_record(&row).await.unwrap();
                recorded.insert(path.clone(), RecordedRow { node_id, sha256: Some(sha.into()) });
            }
            let view = projected(vec![fp.clone()]);
            let records = Arc::new(Records::default());
            let _ = records.contacts.set(store.clone());
            let states = states_of(&fp, view.clone(), builds.clone(), records.clone(), FabricId::mint(), Arc::new(NoScratchpad)).await;
            records.set_fabric_state(FabricState::ReadyForTraffic);
            records.set_published_fabric_state(Some(FabricState::ReadyForTraffic));
            {
                let mut change = states.change.lock().await;
                states.step_ready(&mut change).await;
            }
            let mut nodes: Vec<(PathName, PathConfig)> = topology.meshes.values().flat_map(|m| m.path_config_by_path.iter().map(|(p, c)| (p.clone(), c.clone()))).collect();
            edit(&mut nodes);
            let change = TopologyChange::Reconcile { based_on: a.clone(), nodes };
            let next = compile_against(&topology, &change, &view, &recorded).unwrap_or_else(|e| panic!("{what}: the request compiles: {e}"));
            let b = crate::build::BuildId::parse("bnrrv6tggykq").unwrap();
            builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: next.clone(), submitted_change: Some(change), submitted_at_ms: 2_000 }).await.unwrap();
            claim(&builds, &b, 1).await;
            let at_acceptance = states.accepted().await;
            {
                let mut change = states.change.lock().await;
                states.step_ready(&mut change).await;
            }
            let now = records.fabric_state();
            let changed = next != topology;
            let got = match (at_acceptance, now) {
                (true, FabricState::Pending) => Back::ToPending,
                (false, FabricState::ReadyForTraffic) => Back::Stays,
                other => {
                    red.push(format!("{what}: the acceptance said put-back={} and the fabric holds {:?}", other.0, other.1));
                    continue;
                }
            };
            if want != got {
                red.push(format!("{what}: the pages say {want:?}, the fabric {got:?} (the compiled topology {} the accepted one)", if changed { "differs from" } else { "is" }));
            }
        }
        assert!(red.is_empty(), "Reconcile variant rows that differ from the pages:\n  {}", red.join("\n  "));
    }
}
