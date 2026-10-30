//! The fabric's states, run by the fabric-primary (states.md; lifecycles/states.md, "The fabric's ready").
//!
//! - The fabric-primary alone moves the fabric: `pending` → `ready` once every mesh the
//!   accepted Build names is ready (Q174). The FabricStatus frame carrying each state is
//!   authored by it only, by the status loop that reads the state it holds.
//! - The application gets one notice, `on_fabric_ready`, gating nothing (Q175).
//! - A new primary, through election or recovery, republishes the status it adopted only after the
//!   checklist of that adopted status (states.md, "The check-in checklist"; R-S3): its down op is the
//!   read `ProbeNodeState` (Q87) ([`FabricStates::adopted_step`]).

use crate::accepted::FabricTopology;
use crate::admin::Records;
use crate::build_claim::AttemptContexts;
use crate::build_state::BuildStateAdapter;
use crate::fabric_state::FabricState;
use crate::model::{FabricId, IncarnationId, Node, NodeId, NodeKind, PathName};
use crate::checklist::{fabric_missing, missing_members, reports_of, Missing};
use crate::topology::Topology;
use crate::wiring::{FabricHooks, FabricReadyCtx};
use rdm_transport::topology::{Announced, DigestView};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::Instrument;

/// The current Build as one status turn read it ([`BuildStateAdapter::current`]).
pub type TurnRead = Result<Option<crate::build_state::BuildProjection>, crate::build_state::BuildStateError>;

/// What the status loop hands the fabric-primary's step each turn.
pub struct ChangeInputs<'a> {
    /// Whether this admin holds the fabric-primary role now.
    pub is_fabric_primary: bool,
    /// The state of the last FabricStatus frame this admin heard (`None`: none heard).
    pub heard: Option<&'a str>,
    /// This admin's view.
    pub view: &'a Topology,
    /// The current Build as this turn read it, once ([`BuildStateAdapter::current`]): every step the turn
    /// takes is decided on this read, and none reads the Build again. `Err` names a store that could not be read.
    pub current: &'a TurnRead,
}

/// The shared handles one admin's fabric states read and act through.
pub(crate) struct StatesEnv {
    /// This admin's `path.name`.
    pub me: PathName,
    /// This admin's node id.
    pub node_id: NodeId,
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
    /// The records whose fabric state the projection reads.
    pub records: Arc<Records>,
    /// The application hooks.
    pub hooks: FabricHooks,
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
    /// The digest view: the check-ins of the mesh checklist are the digests heard since it began.
    pub digest_view: &'a DigestView,
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

#[derive(Default)]
struct Change {
    adopted: bool,
    root: Option<tracing::Span>,
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

/// One admin's fabric states: as the fabric-primary the change, as a new primary the checklists of the
/// statuses it adopted.
pub struct FabricStates {
    env: StatesEnv,
    change: tokio::sync::Mutex<Change>,
    /// Poked when the change's next turn is due.
    change_wake: tokio::sync::Notify,
    /// The checklists of the statuses this admin adopted as a new primary.
    adopted: Mutex<Adopted>,
}

impl FabricStates {
    /// The fabric states of the admin `env` describes.
    pub(crate) fn new(env: StatesEnv) -> Arc<Self> {
        Arc::new(Self { env, change: tokio::sync::Mutex::new(Change::default()), change_wake: tokio::sync::Notify::new(), adopted: Mutex::new(Adopted::default()) })
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

    // ------------------------------------------------------------------ the fabric-primary

    /// One turn of the fabric-primary's change. Cheap and never blocking: the application's notice
    /// runs on its own task.
    pub async fn change_step(self: &Arc<Self>, inputs: ChangeInputs<'_>) {
        let mut change = self.change.lock().await;
        let heard_state = inputs.heard.and_then(|h| FabricState::parse(h).or_else(|| (h == "degraded").then_some(FabricState::Ready)));
        if !inputs.is_fabric_primary {
            // A non-primary serves the state the fabric-primary published and runs nothing.
            *change = Change::default();
            self.env.records.set_fabric_state(heard_state.unwrap_or(FabricState::Pending));
            return;
        }
        let me = self.held_name_in(inputs.view);
        if !change.adopted {
            // The role was taken: the state the previous primary published is adopted, not re-decided.
            change.adopted = true;
            let adopted = heard_state.unwrap_or(FabricState::Pending);
            self.env.records.set_fabric_state(adopted);
            tracing::info_span!("rdm.node_admin.fabric.update.via-state-adopted", node = %me, state = adopted.as_str(), heard = inputs.heard.unwrap_or("none"), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the fabric-primary role was taken: the published state is adopted"));
        }
        // A step that moved the fabric is followed by the next at once. Every step is decided on the
        // turn's one read of the current Build.
        for _ in 0..4 {
            let before = self.env.records.fabric_state();
            match before {
                FabricState::Pending => self.step_pending(&mut change, inputs.view, inputs.current).await,
                FabricState::Ready => self.step_ready(&mut change, inputs.current).await,
            }
            if before == self.env.records.fabric_state() {
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
    /// the one the fabric changes under, so the fabric is ready again only when every mesh of the new
    /// topology is. A new attempt of the same Build changes no topology and moves nothing here.
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
                change.root = None;
                self.enter(change, FabricState::Pending, me);
                true
            }
            Some(_) => false,
        }
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

    async fn step_ready(self: &Arc<Self>, change: &mut Change, current: &TurnRead) {
        let me = self.held_name().await;
        match current {
            Ok(Some(build)) => {
                self.resized(change, build, &me).await;
            }
            Ok(None) => {}
            Err(e) => self.block(change, &[unreadable(&e)], "ready reads the current Build", &me),
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

    fn enter(&self, change: &mut Change, next: FabricState, me: &PathName) {
        let from = self.env.records.fabric_state();
        self.env.records.set_fabric_state(next);
        self.env.records.set_fabric_blocker(None);
        change.named = None;
        let parent = change.root.clone().unwrap_or_else(tracing::Span::none);
        tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-state-entered", node = %me, fabric_id = %self.env.fabric_id, from = from.as_str(), to = next.as_str(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the fabric-primary entered a fabric state"));
        self.env.records.wake.poke();
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

    /// Whether the current Build `build` is a fabric.delete: the fabric changes no more and is never made
    /// ready for the delete (build.md §1). The blocker names the delete.
    fn deleting(&self, change: &mut Change, build: &crate::build_state::BuildProjection, me: &PathName) -> bool {
        if !build.deletes_fabric() {
            return false;
        }
        self.block(change, &[Missing { who: "build".into(), why: "fabric-delete" }], &format!("fabric.delete Build {} is the current Build: the fabric is not made ready", build.build_id), me);
        true
    }

    async fn step_pending(self: &Arc<Self>, change: &mut Change, view: &Topology, current: &TurnRead) {
        let me = self.held_name_in(view);
        let accepted = match current {
            Ok(b) => b.as_ref(),
            Err(e) => return self.block(change, &[unreadable(e)], "ready names the current Build", &me),
        };
        if accepted.is_some_and(|b| self.deleting(change, b, &me)) {
            return;
        }
        let mut blockers = self.entry_blockers(view, accepted.map(|b| &b.topology));
        // During an executable replacement a member still on its old executable does not satisfy its
        // planned replacement: the fabric changes once every planned member runs its entry's executable
        // (states.md "Resize and executable replacement"), as the node rows record what each runs.
        if let (Some(build), Some(rows)) = (accepted.filter(|b| b.submitted_change.as_ref().is_some_and(crate::accepted::TopologyChange::plans_replacements)), self.env.records.contacts.get()) {
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
            self.block(change, &blockers, "ready waits for every mesh to be ready", &me);
            return;
        }
        let Some(build) = accepted else {
            self.block(change, &[Missing { who: "build".into(), why: "no-accepted-build" }], "ready names the accepted Build", &me);
            return;
        };
        // The change names the accepted Build's attempt: a Build whose first attempt no executor has
        // claimed yet has none, and the change names the attempt that exists.
        if build.attempt == 0 {
            self.block(change, &[Missing { who: "build".into(), why: "no-attempt-claimed" }], "ready names the accepted Build's attempt", &me);
            return;
        }
        if !self.may_advance() {
            return;
        }
        let root = tracing::info_span!(parent: None, "rdm.node_admin.fabric.update.via-change", node = %me, fabric_id = %self.env.fabric_id, build_id = %build.build_id.log(), adopted = false);
        change.tracked = Some(build.topology.clone());
        change.tracked_build = Some(build.build_id.clone());
        change.named = None;
        tracing::info_span!(parent: &root, "rdm.node_admin.fabric.update.via-every-mesh-ready", node = %me, build_id = %build.build_id.log(), attempt = build.attempt, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("every mesh is ready: the fabric is ready"));
        change.root = Some(root.clone());
        self.enter(change, FabricState::Ready, &me);
        if !change.notified {
            change.notified = true;
            self.notify_fabric_ready(build, root, me);
        }
        change.root = None;
    }

    /// The meshes not ready and the mesh primaries that have not reported, named. Empty:
    /// every mesh is ready (R-H1). A mesh's state is the one the view projects
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

    /// The one notice to the application: the fabric is ready (Q175, `on_fabric_ready`). It is
    /// fire and forget and gates nothing.
    fn notify_fabric_ready(&self, build: &crate::build_state::BuildProjection, parent: tracing::Span, me: PathName) {
        let (hook, contexts) = (self.env.hooks.on_fabric_ready.clone(), self.env.contexts.clone());
        let (fabric_id, build_id, attempt) = (self.env.fabric_id.clone(), build.build_id.clone(), build.attempt);
        tokio::spawn(async move {
            let span = tracing::info_span!(parent: &parent, "rdm.node_admin.fabric.update.via-on-fabric-ready", node = %me, build_id = %build_id.log(), attempt, bound = hook.is_some(), "otel.kind" = "internal");
            async {
                let Some(hook) = hook else {
                    tracing::info!("no on_fabric_ready hook is registered");
                    return;
                };
                let context = contexts.get(&build_id, attempt).await.ok().flatten().unwrap_or_default();
                hook(FabricReadyCtx { fabric_id, build_id: build_id.to_string(), attempt, context }).await;
                tracing::info!("the application was told the fabric is ready");
            }
            .instrument(span)
            .await
        });
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
    /// check-in is a planned birth's digest heard ready after the checklist began; the
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
                    let me_ready = i.view.own(&self.env.node_id).is_some_and(|n| n.status == crate::model::NodeStatus::Ready);
                    let (heard, known) = (i.digest_view.heard_direct_since(checklist.began), i.digest_view.all());
                    // A birth the view holds stopped is at rest by command and holds no checklist (Q240).
                    let stopped = |p: &PathName| i.view.members().any(|n| &n.name == p && n.stopped);
                    missing_members(planned, &me, me_ready, &heard, &known, &|id| i.digest_view.is_deleted(id), &stopped)
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
}

/// The notice to the application that a mesh is ready (Q175, `on_mesh_ready`): called by
/// the mesh-primary once its mesh's ready is on its Mesh record (Q144), on its own task. It
/// is fire and forget and gates nothing.
pub(crate) fn notify_mesh_ready(hook: Option<crate::wiring::MeshReadyNotice>, ctx: crate::wiring::MeshReadyCtx) {
    tokio::spawn(async move {
        let span = tracing::info_span!("rdm.node_admin.mesh.update.via-on-mesh-ready", mesh = %ctx.mesh, mesh_id = %ctx.mesh_id, bound = hook.is_some(), "otel.kind" = "internal");
        async {
            let Some(hook) = hook else {
                tracing::info!("no on_mesh_ready hook is registered");
                return;
            };
            hook(ctx).await;
            tracing::info!("the application was told the mesh is ready");
        }
        .instrument(span)
        .await
    });
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

/// The meshes among `meshes` that are not ready in `view`, with the nodes behind each. A
/// mesh's state is the one the view projects (`Topology::mesh_unready` decides it).
fn mesh_blockers(view: &Topology, meshes: &[String], planned: &std::collections::BTreeMap<String, std::collections::BTreeSet<PathName>>) -> Vec<Missing> {
    let mut out = Vec::new();
    for mesh in meshes {
        match view.meshes.iter().find(|m| &m.name == mesh) {
            Some(m) => {
                let why = view.mesh_unready(mesh, planned.get(mesh));
                if !why.is_empty() {
                    out.extend(why);
                } else if m.status != crate::model::ScopeStatus::Ready {
                    out.push(Missing { who: mesh.clone(), why: "not-ready" });
                }
            }
            None => out.push(Missing { who: mesh.clone(), why: "no-mesh" }),
        }
    }
    out
}

fn unreadable(e: &crate::build_state::BuildStateError) -> Missing {
    Missing { who: format!("build ({e})"), why: "build-store-unreadable" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::MemberStatus;
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
            let status = if t.mesh_unready(m, None).is_empty() { ScopeStatus::Ready } else { ScopeStatus::Pending };
            t.meshes.push(Mesh { id: None, name: m.into(), status });
        }
        t
    }

    /// The fabric states of the admin `me`, as `view` holds it, over `builds` and `records`: a client on
    /// an endpoint of its own whose resolver names nobody.
    async fn states_of(me: &Node, view: Topology, builds: Arc<dyn BuildStateAdapter>, records: Arc<Records>, fabric_id: FabricId) -> Arc<FabricStates> {
        states_resolving(me, view, builds, records, fabric_id, Arc::new(rdm_transport::StaticResolver::new())).await
    }

    /// [`states_of`] whose client resolves through `resolver`.
    async fn states_resolving(me: &Node, view: Topology, builds: Arc<dyn BuildStateAdapter>, records: Arc<Records>, fabric_id: FabricId, resolver: Arc<rdm_transport::StaticResolver>) -> Arc<FabricStates> {
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        FabricStates::new(StatesEnv {
            me: me.name.clone(),
            node_id: me.node_id.clone(),
            fabric_id,
            topology: Arc::new(RwLock::new(view)),
            client: Arc::new(NodeRpcClient::new(ep, resolver)),
            builds,
            contexts: Arc::new(AttemptContexts::in_memory()),
            records,
            hooks: FabricHooks::no_app_work(),
        })
    }

    /// Open (after the first) and claim `attempt` of `id`.
    async fn claim(builds: &crate::build_state::MemoryBuildStateAdapter, id: &crate::build::BuildId, attempt: u32) {
        use crate::build_state::{AttemptOpened, AttemptReason, BuildAttemptClaim};
        if attempt > 1 {
            builds.open_attempt(&AttemptOpened { mesh: None, build_id: id.clone(), attempt, reason: AttemptReason::Stop, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 0, context: None }).await.unwrap();
        }
        builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
    }

    /// The fabric-primary `mesh1.admin.1` and mesh2's primary, both ready.
    fn fabric_primary_and_mesh2() -> (Node, Node) {
        let mut fp = node("mesh1.admin.1", NodeStatus::Ready, true);
        fp.is_fabric_primary = true;
        (fp, node("mesh2.admin.1", NodeStatus::Ready, true))
    }

    /// Every mesh is ready as the fabric-primary decides it: its own mesh declared ready,
    /// `mesh2`'s primary reported, and pending went out.
    fn every_mesh_ready(records: &Records, mesh2: Option<&Node>) {
        records.set_declare_gate(true);
        if let Some(m2) = mesh2 {
            records.declared.lock().unwrap().record_report("mesh2", crate::status_rpc::Reporter::Birth(m2.incarnation_id.clone().unwrap()), Instant::now());
        }
        records.set_published_fabric_state(Some(FabricState::Pending));
    }

    tokio::task_local! {
        /// Set on the task that runs a status turn in [`CountedReads`]' cells: a read of the current Build
        /// counts only on that task, never on a checklist driver or command the turn spawns.
        static ON_THE_TURN: ();
    }

    /// A Build log whose current-Build reads ([`BuildStateAdapter::current_build`], the index read every
    /// [`BuildStateAdapter::current`] starts with) are counted on the status turn's own task.
    struct CountedReads {
        inner: Arc<crate::build_state::MemoryBuildStateAdapter>,
        reads: std::sync::atomic::AtomicUsize,
    }

    impl CountedReads {
        fn reads(&self) -> usize {
            self.reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl BuildStateAdapter for CountedReads {
        async fn publish_accepted(&self, a: &crate::build_state::BuildAccepted) -> Result<(), crate::build_state::BuildStateError> {
            self.inner.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &crate::build_state::AttemptOpened) -> Result<(), crate::build_state::BuildStateError> {
            self.inner.open_attempt(o).await
        }
        async fn read_build(&self, id: &crate::build::BuildId) -> Result<crate::build_state::BuildProjection, crate::build_state::BuildStateError> {
            self.inner.read_build(id).await
        }
        async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
            if ON_THE_TURN.try_with(|_| ()).is_ok() {
                self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.current_build().await
        }
        async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, crate::build_state::BuildStateError> {
            self.inner.newest_accepted_at_ms().await
        }
        async fn list_active(&self) -> Result<Vec<crate::build_state::BuildProjection>, crate::build_state::BuildStateError> {
            self.inner.list_active().await
        }
        async fn claim_attempt(&self, c: &crate::build_state::BuildAttemptClaim) -> Result<crate::build_state::ClaimOutcome, crate::build_state::BuildStateError> {
            self.inner.claim_attempt(c).await
        }
        async fn adopt_claim(&self, c: &crate::build_state::BuildAttemptClaim) -> Result<(), crate::build_state::BuildStateError> {
            self.inner.adopt_claim(c).await
        }
        async fn append_step_receipt(&self, r: &crate::build_state::BuildStepReceipt) -> Result<(), crate::build_state::BuildStateError> {
            self.inner.append_step_receipt(r).await
        }
        async fn append_attempt_receipt(&self, r: &crate::build_state::BuildAttemptReceipt) -> Result<(), crate::build_state::BuildStateError> {
            self.inner.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<crate::build_state::BuildFact>, crate::build_state::BuildStateError> {
            self.inner.facts().await
        }
        async fn committed_facts(&self, build: Option<&crate::build::BuildId>) -> Result<Vec<crate::build_state::BuildFact>, crate::build_state::BuildStateError> {
            self.inner.committed_facts(build).await
        }
        async fn recovery_of(&self, path: &PathName, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(crate::build::BuildId, Option<String>, u32, String)>, crate::build_state::BuildStateError> {
            self.inner.recovery_of(path, node_id, incarnation).await
        }
        async fn mesh_deleted_of(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<(crate::build::BuildId, u32, String)>, crate::build_state::BuildStateError> {
            self.inner.mesh_deleted_of(node_id, incarnation).await
        }
        async fn exits_owed(&self, build_id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, crate::build_state::BuildStateError> {
            self.inner.exits_owed(build_id, mesh).await
        }
        async fn forget(&self, id: &crate::build::BuildId) -> Result<(), crate::build_state::BuildStateError> {
            self.inner.forget(id).await
        }
    }

    /// CONTRACT (states.md "The fabric's ready": "Once every member has checked in under the check-in
    /// checklist, the fabric is ready"; the status loop reads the current Build once per turn,
    /// `admin::boot`): one status turn of the fabric-primary reads the current Build once, at its start,
    /// and every step of the fabric's change it takes in that turn is decided on that read, however many
    /// steps the turn takes: every mesh ready, then ready. On a Build store whose read
    /// costs ~100 ms (an application's own backend), each further read is that much longer between every
    /// mesh ready and ready. Must NOT happen: a step, or the checklist a step
    /// runs, reading the current Build again inside the turn.
    #[tokio::test]
    async fn one_status_turn_reads_the_current_build_once_however_many_steps_of_the_change_it_takes() {
        use crate::accepted::MeshTopology;
        use crate::build::MeshDesired;
        use crate::build_state::BuildAccepted;
        let capture = crate::span_capture::Capture::new("one-build-read-per-turn");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let builds = Arc::new(CountedReads { inner: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()), reads: Default::default() });
        let id = crate::build::BuildId::parse("amrrv6tggykq").unwrap();
        let mesh1 = ("mesh1".to_string(), MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1)])));
        builds.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [mesh1].into() }, submitted_change: None, submitted_at_ms: 1_000 }).await.unwrap();
        claim(&builds.inner, &id, 1).await;
        // The fabric-primary alone in its one-mesh fabric, its mesh ready; pending went out.
        let (fp, _) = fabric_primary_and_mesh2();
        let view = projected(vec![fp.clone()]);
        let records = Arc::new(Records::default());
        every_mesh_ready(&records, None);
        let fabric_states = states_of(&fp, view.clone(), builds.clone(), records.clone(), FabricId::mint()).await;
        // One status turn: the current Build read once, then the fabric's change stepped on that read.
        let turn = || {
            let (builds, fabric_states, view) = (builds.clone(), fabric_states.clone(), view.clone());
            ON_THE_TURN.scope((), async move {
                let before = builds.reads();
                let read = builds.current().await;
                fabric_states.change_step(ChangeInputs { is_fabric_primary: true, heard: Some("pending"), view: &view, current: &read }).await;
                builds.reads() - before
            })
        };
        let mut reads = Vec::new();
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        // Turns until the fabric is ready: every mesh is ready, so the change enters it.
        while records.fabric_state() != FabricState::Ready && tokio::time::Instant::now() < until {
            reads.push(turn().await);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(records.fabric_state(), FabricState::Ready, "the change entered ready: {:?}", records.fabric_blocker());
        assert_eq!(capture.named("rdm.node_admin.fabric.update.via-every-mesh-ready").len(), 1, "every mesh ready is named once");
        assert!(reads.iter().all(|n| *n == 1), "each status turn reads the current Build once: reads per turn {reads:?}");
    }

    /// CONTRACT (R-RC1): a replace is a delete and a create at the same path.name with a new node
    /// id, so while the deleted birth's row is still in a view, two rows carry this admin's
    /// path.name. A state op's receiver finds its own row by its node id, the identity it was born
    /// with. What must NOT happen: the deleted birth's row taken as this admin's, which refuses a
    /// state command naming this admin "the subject node itself" and strands the checklist.
    #[test]
    fn an_admin_finds_its_own_row_by_node_id_while_its_superseded_birth_is_still_in_the_view() {
        let deleted = node("mesh1.admin.1", NodeStatus::ConnectionsDeleting, false);
        let re_born = node("mesh1.admin.1", NodeStatus::Ready, false);
        let view = projected(vec![deleted.clone(), re_born.clone(), node("mesh1.admin.2", NodeStatus::Ready, true)]);
        assert_eq!(view.members().filter(|n| n.name == re_born.name).count(), 2, "both births of mesh1.admin.1 are in the view");
        let me = view.own(&re_born.node_id).expect("this admin is in its view");
        assert_eq!(me.node_id, re_born.node_id, "the re-born admin's own row, not the deleted birth's {}", deleted.node_id);
    }

    /// CONTRACT (R-H1): the fabric is ready only when every mesh is ready, and a
    /// mesh is ready only when every node of it is. A mesh with a primary and one node not ready blocks
    /// entry and names that node; the mesh whose nodes are all ready blocks nothing.
    #[test]
    fn a_mesh_with_one_node_not_ready_blocks_the_fabric_from_being_ready() {
        let meshes = vec!["mesh1".to_string(), "mesh2".to_string()];
        let planned = std::collections::BTreeMap::new();
        let ready = projected(vec![
            node("mesh1.admin.1", NodeStatus::Ready, true),
            node("mesh1.rpc.1", NodeStatus::Ready, false),
            node("mesh2.admin.1", NodeStatus::Ready, true),
        ]);
        assert!(mesh_blockers(&ready, &meshes, &planned).is_empty(), "every node of both meshes is ready: nothing blocks entry");
        let one_pending = projected(vec![
            node("mesh1.admin.1", NodeStatus::Ready, true),
            node("mesh1.rpc.1", NodeStatus::Pending, false),
            node("mesh2.admin.1", NodeStatus::Ready, true),
        ]);
        assert!(one_pending.node_type_primary("mesh1", NodeKind::NodeAdmin).is_some(), "mesh1 has its primary");
        let blockers: Vec<String> = mesh_blockers(&one_pending, &meshes, &planned).iter().map(|m| m.to_string()).collect();
        assert_eq!(blockers, ["mesh1.rpc.1(not-ready)"], "the one node that is not ready is what blocks entry");
    }

    /// CONTRACT (states.md:16, Q240): a mesh is ready when every planned birth of the accepted Build that
    /// is not stopped is ready. A stopped birth (the view holds it `Leaving` and `stopped`) is at rest by
    /// command and holds neither its mesh nor the fabric's checklist; a mesh whose every rpc birth is
    /// stopped is ready. A planned birth that is not stopped and not ready still
    /// blocks, named. Must NOT happen: the stopped birth named `not-ready`, the fabric held pending until
    /// it starts or is deleted (flame b7f9ecec2fec32f356d022f98bdfb374).
    #[test]
    fn a_stopped_birth_holds_neither_its_mesh_nor_the_fabrics_checklist() {
        let stopped = |name: &str| {
            let mut n = node(name, NodeStatus::Leaving, false);
            n.stopped = true;
            n
        };
        let meshes = vec!["mesh1".to_string(), "mesh2".to_string()];
        let paths = |names: &[&str]| names.iter().map(|n| n.parse::<PathName>().unwrap()).collect::<std::collections::BTreeSet<_>>();
        let planned: std::collections::BTreeMap<String, std::collections::BTreeSet<PathName>> =
            [("mesh1".to_string(), paths(&["mesh1.admin.1"])), ("mesh2".to_string(), paths(&["mesh2.admin.1", "mesh2.rpc.1", "mesh2.rpc.2"]))].into_iter().collect();
        let one_stopped = projected(vec![
            node("mesh1.admin.1", NodeStatus::Ready, true),
            node("mesh2.admin.1", NodeStatus::Ready, true),
            stopped("mesh2.rpc.1"),
            node("mesh2.rpc.2", NodeStatus::Ready, false),
        ]);
        assert_eq!(one_stopped.mesh_unready("mesh2", planned.get("mesh2")), Vec::<Missing>::new(), "the stopped mesh2.rpc.1 holds nothing of mesh2");
        assert_eq!(mesh_blockers(&one_stopped, &meshes, &planned), Vec::<Missing>::new(), "a stopped birth holds no checklist: nothing blocks the fabric");
        let all_stopped = projected(vec![
            node("mesh1.admin.1", NodeStatus::Ready, true),
            node("mesh2.admin.1", NodeStatus::Ready, true),
            stopped("mesh2.rpc.1"),
            stopped("mesh2.rpc.2"),
        ]);
        assert_eq!(mesh_blockers(&all_stopped, &meshes, &planned), Vec::<Missing>::new(), "a mesh whose every rpc birth is stopped is ready, chosen by command");
        let one_pending = projected(vec![
            node("mesh1.admin.1", NodeStatus::Ready, true),
            node("mesh2.admin.1", NodeStatus::Ready, true),
            stopped("mesh2.rpc.1"),
            node("mesh2.rpc.2", NodeStatus::Pending, false),
        ]);
        let blockers: Vec<String> = mesh_blockers(&one_pending, &meshes, &planned).iter().map(|m| m.to_string()).collect();
        assert_eq!(blockers, ["mesh2.rpc.2(not-ready)"], "the birth that is not stopped and not ready still blocks, the stopped one is not named");
    }

    /// CONTRACT (states.md, Resize and executable replacement: a Build that changes the accepted topology
    /// puts the fabric back to pending; a primary change or a stop "leaves the topology unchanged"): an
    /// admin that held the current Build as heard on gossip (no executables) and, fabric-primary now, reads
    /// it from the store root (executables named) holds two copies of one Build's topology. The fabric
    /// stays ready; a different Build that names a new executable puts it back to pending.
    /// Must NOT happen: the same Build read from another copy taken as "the accepted topology changed",
    /// the fabric reset to pending by a hand-over.
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
            let mut c = m.spec(&rpc).cloned().unwrap();
            c.executable = sha.map(exe);
            m.set_spec(c).unwrap();
            FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m)].into() }
        };
        let a = crate::build::BuildId::parse("amrrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: a.clone(), topology: topology(Some("aa")), submitted_change: None, submitted_at_ms: 1_000 }).await.unwrap();
        claim(&builds, &a, 1).await;
        let (fp, _) = fabric_primary_and_mesh2();
        let view = projected(vec![fp.clone()]);
        let records = Arc::new(Records::default());
        let states = states_of(&fp, view, builds.clone(), records.clone(), FabricId::mint()).await;
        records.set_fabric_state(FabricState::Ready);
        records.set_published_fabric_state(Some(FabricState::Ready));
        {
            let mut change = states.change.lock().await;
            // As heard on gossip: the same Build, no executable on its entries.
            change.tracked = Some(topology(None));
            change.tracked_build = Some(a.clone());
            states.step_ready(&mut change, &states.env.builds.current().await).await;
        }
        assert_eq!(records.fabric_state(), FabricState::Ready, "the same Build read from the store root is no resize: the fabric stays ready");
        let b = crate::build::BuildId::parse("bnrrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: topology(Some("bb")), submitted_change: None, submitted_at_ms: 2_000 }).await.unwrap();
        claim(&builds, &b, 1).await;
        {
            let mut change = states.change.lock().await;
            states.step_ready(&mut change, &states.env.builds.current().await).await;
        }
        assert_eq!(records.fabric_state(), FabricState::Pending, "a new Build that names a new executable puts the fabric back to pending");
    }

    /// CONTRACT (states.md, Resize and executable replacement: "A resize or an executable replacement puts the
    /// fabric back to pending"; build-deployment.md §3: a member is replaced when its row records another
    /// executable than its entry names): day 0's entry for the Day-0 admin names no executable, and its row
    /// records the one it runs. A Build whose request names, for that path, the executable its row records
    /// is neither a resize nor a replacement: the fabric stays ready and the change holds the
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
            let mut c = m.spec(&admin).cloned().unwrap();
            c.executable = sha.map(exe);
            m.set_spec(c).unwrap();
            FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m)].into() }
        };
        let (fp, _) = fabric_primary_and_mesh2();
        // The Day-0 admin's row records the executable it runs.
        let rows = Arc::new(crate::storage::MemoryNodesStorage::default());
        let mut row = crate::storage::NodeRow::new(admin.clone(), fp.node_id.clone(), NodeStatus::Ready);
        row.executable = Some(exe("aa"));
        rows.write_record(&row).await.unwrap();
        let a = crate::build::BuildId::parse("amrrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: a.clone(), topology: topology(None), submitted_change: None, submitted_at_ms: 1_000 }).await.unwrap();
        claim(&builds, &a, 1).await;
        let view = projected(vec![fp.clone()]);
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(rows.clone());
        let states = states_of(&fp, view, builds.clone(), records.clone(), FabricId::mint()).await;
        records.set_fabric_state(FabricState::Ready);
        records.set_published_fabric_state(Some(FabricState::Ready));
        {
            let mut change = states.change.lock().await;
            states.step_ready(&mut change, &states.env.builds.current().await).await;
            assert_eq!(change.tracked_build, Some(a.clone()), "day 0's Build is the topology the fabric changes under");
        }
        let b = crate::build::BuildId::parse("bnrrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: topology(Some("aa")), submitted_change: None, submitted_at_ms: 2_000 }).await.unwrap();
        claim(&builds, &b, 1).await;
        assert!(!states.accepted().await, "a request naming the executable the row records puts nothing back at its acceptance");
        {
            let mut change = states.change.lock().await;
            states.step_ready(&mut change, &states.env.builds.current().await).await;
            assert_eq!(change.tracked_build, Some(b.clone()), "the change holds the new Build's topology");
        }
        assert_eq!(records.fabric_state(), FabricState::Ready, "the executable the row records is no replacement: the fabric stays ready");
        let c = crate::build::BuildId::parse("cprrv6tggykq").unwrap();
        builds.publish_accepted(&BuildAccepted { build_id: c.clone(), topology: topology(Some("cc")), submitted_change: None, submitted_at_ms: 3_000 }).await.unwrap();
        claim(&builds, &c, 1).await;
        assert!(states.accepted().await, "another executable than the row records is a replacement: the fabric is back to pending at its acceptance");
        assert_eq!(records.fabric_state(), FabricState::Pending, "an executable replacement puts the fabric back to pending");
    }

    use crate::model::{MeshId, NodeStatus as NS};
    use rdm_transport::topology::DigestView;

    fn member(name: &str, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = NS::Ready;
        n.incarnation_id = Some(IncarnationId::mint());
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        n
    }

    /// A degraded fabric of two meshes, holding `nodes`.
    fn view(nodes: Vec<Node>) -> Topology {
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Degraded, provider: ProviderKind::Process },
            meshes: ["mesh1", "mesh2"].iter().map(|m| Mesh { id: Some(MeshId::mint()), name: (*m).into(), status: ScopeStatus::Ready }).collect(),
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
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        }
    }

    /// The checklists of `me` over `view` and `records`, with a Build log that holds nothing.
    async fn adopted_states_of(me: &Node, view: Topology, records: Arc<Records>) -> Arc<FabricStates> {
        states_of(me, view, Arc::new(crate::build_state::MemoryBuildStateAdapter::new()), records, FabricId::mint()).await
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
        let digest_view = DigestView::default();
        let awaiting = Announced { mesh_awaiting_checklist: true, fabric_awaiting_checklist: false };
        let planned = vec![me.name.clone(), rpc.name.clone()];
        let inputs = || AdoptedInputs { view: &v, planned: Some(planned.clone()), digest_view: &digest_view };
        let held = || capture.named("rdm.node_admin.mesh.update.via-checklist-held").into_iter().map(|s| s.attributes.get("missing").cloned().unwrap_or_default()).collect::<Vec<_>>();
        // The member was heard before the checklist began: held, not checked in.
        let mut heard = digest(&rpc.name, MemberStatus::Ready);
        assert!(digest_view.record(heard.clone()));
        assert!(!fabric_states.adopted_step(awaiting, inputs()).mesh, "a digest heard before the checklist is not a check-in");
        assert_eq!(held(), ["mesh1.rpc.1(not-heard-since-opening)"], "the hold names the planned birth that has not checked in");
        heard.digest_seq += 1;
        assert!(digest_view.record(heard.clone()));
        assert!(fabric_states.adopted_step(awaiting, inputs()).mesh, "every planned birth checked in after the checklist began");
        let complete = capture.named("rdm.node_admin.mesh.update.via-checklist-complete");
        assert_eq!(complete.len(), 1, "the checklist completed once: {complete:?}");
        assert_eq!(complete[0].attributes.get("members").map(String::as_str), Some("2"));
        // The adopted status went out: no checklist is open. Adopted again, the checklist opens and the checklist rebuilds itself.
        fabric_states.adopted_end();
        assert!(!fabric_states.adopted_step(awaiting, inputs()).mesh, "the checklist rebuilt: the digest that completed the earlier opening is no check-in");
        assert_eq!(held().len(), 2, "the hold is named again for the new opening");
        heard.digest_seq += 1;
        assert!(digest_view.record(heard));
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
        // The role moved to mesh2's primary while mesh1 was being recovered; the new primary adopted degraded.
        let (me, recovered) = (member("mesh2.admin.1", true, true), member("mesh1.admin.2", true, false));
        let v = view(vec![me.clone(), recovered.clone()]);
        let records = Arc::new(Records::default());
        records.set_adopted_degraded(true);
        let digest_view = DigestView::default();
        let fabric_states = adopted_states_of(&me, v.clone(), records.clone()).await;
        let awaiting = Announced { mesh_awaiting_checklist: false, fabric_awaiting_checklist: true };
        let inputs = || AdoptedInputs { view: &v, planned: None, digest_view: &digest_view };
        // A ready primary of a newer birth is in the view; nobody has reported to this holder.
        let done = fabric_states.adopted_step(awaiting, inputs());
        assert!(!done.fabric && records.adopted_degraded(), "the topology reading alone does not clear degraded");
        // A report from a birth that is not the primary now does not either.
        records.declared.lock().unwrap().record_report("mesh1", crate::status_rpc::Reporter::Birth(IncarnationId::mint()), Instant::now());
        let done = fabric_states.adopted_step(awaiting, inputs());
        assert!(!done.fabric && records.adopted_degraded(), "a report of another birth is not the current primary's");
        // The current primary re-declares to the new primary: the checklist is complete and degraded clears.
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
        let digest_view = DigestView::default();
        let adopted = adopted_states_of(&me, v.clone(), records.clone()).await;
        let awaiting = Announced { mesh_awaiting_checklist: false, fabric_awaiting_checklist: true };
        let inputs = || AdoptedInputs { view: &v, planned: None, digest_view: &digest_view };
        assert!(!adopted.adopted_step(awaiting, inputs()).fabric, "mesh1's primary has not reported yet");
        records.declared.lock().unwrap().record_report("mesh1", crate::status_rpc::Reporter::Birth(other.incarnation_id.clone().unwrap()), Instant::now());
        assert!(adopted.adopted_step(awaiting, inputs()).fabric, "every mesh that is not Dead reported: the Dead mesh3 is no mesh to report");
    }

    // @feature: node-lifecycle
    #[tokio::test]
    async fn elected_mesh_primary_completes_its_checklist_only_when_every_planned_birth_checks_in_after_it_began() {
        let (me, rpc) = (member("mesh1.admin.1", true, false), member("mesh1.rpc.1", false, false));
        let v = view(vec![me.clone(), rpc.clone()]);
        let digest_view = DigestView::default();
        let fabric_states = adopted_states_of(&me, v.clone(), Arc::new(Records::default())).await;
        let awaiting = Announced { mesh_awaiting_checklist: true, fabric_awaiting_checklist: false };
        let planned = vec![me.name.clone(), rpc.name.clone()];
        let inputs = || AdoptedInputs { view: &v, planned: Some(planned.clone()), digest_view: &digest_view };
        // The member was heard before the checklist began (it is held, but it has not checked in).
        let before = digest(&rpc.name, MemberStatus::Ready);
        assert!(digest_view.record(before.clone()));
        let done = fabric_states.adopted_step(awaiting, inputs());
        assert!(!done.mesh, "a digest heard before the checklist is not a check-in");
        // Its next heartbeat arrives after the checklist began: it has checked in.
        let mut again = before;
        again.digest_seq += 1;
        assert!(digest_view.record(again));
        let done = fabric_states.adopted_step(awaiting, inputs());
        assert!(done.mesh, "every planned birth checked in after the checklist began");
        // An unreadable accepted Build holds the checklist by name.
        let unreadable = adopted_states_of(&me, v.clone(), Arc::new(Records::default())).await;
        let done = unreadable.adopted_step(awaiting, AdoptedInputs { view: &v, planned: None, digest_view: &digest_view });
        assert!(!done.mesh);
    }

    /// CONTRACT (states.md:16, Q240): a stopped birth holds no checklist. An elected mesh-primary's
    /// checklist over a planned birth its view holds stopped (`Leaving`, `stopped`: it sends no digest)
    /// completes once every other planned birth checked in. Must NOT happen: the checklist held on the
    /// stopped birth as `no-birth-heard` until it starts or is deleted.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn an_elected_mesh_primarys_checklist_is_not_held_by_a_stopped_birth() {
        let (me, rpc) = (member("mesh1.admin.1", true, false), member("mesh1.rpc.1", false, false));
        let mut at_rest = member("mesh1.rpc.2", false, false);
        at_rest.status = NS::Leaving;
        at_rest.stopped = true;
        let v = view(vec![me.clone(), rpc.clone(), at_rest.clone()]);
        let digest_view = DigestView::default();
        let fabric_states = adopted_states_of(&me, v.clone(), Arc::new(Records::default())).await;
        let awaiting = Announced { mesh_awaiting_checklist: true, fabric_awaiting_checklist: false };
        let planned = vec![me.name.clone(), rpc.name.clone(), at_rest.name.clone()];
        let inputs = || AdoptedInputs { view: &v, planned: Some(planned.clone()), digest_view: &digest_view };
        assert!(!fabric_states.adopted_step(awaiting, inputs()).mesh, "mesh1.rpc.1 has not checked in");
        assert!(digest_view.record(digest(&rpc.name, MemberStatus::Ready)));
        assert!(fabric_states.adopted_step(awaiting, inputs()).mesh, "every planned birth that is not stopped checked in: the stopped mesh1.rpc.2 holds nothing");
    }

    /// What the pages say a Reconcile variant does to a ready fabric at its acceptance.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Back {
        /// A resize or an executable replacement: back to pending (states.md "Resize and executable replacement").
        ToPending,
        /// Neither: the fabric stays ready.
        Stays,
    }

    /// CONTRACT (states.md "Resize and executable replacement": "A resize or an executable replacement puts the
    /// fabric back to pending"; build.md: a topology-changing request compiles to a new complete Build;
    /// build-deployment.md §3: a member is replaced when its row records another executable than its entry
    /// names): the Reconcile variant matrix's acceptance rows (the drive's rows, and why no cell held the gap,
    /// are tests/reconcile_variants.rs). Each request is compiled by the door's compiler against a Day-0
    /// fabric (the Day-0 admin's entry names no executable, its row records `aa`; mesh1.rpc.1's entry and row
    /// name `rr`), accepted while the fabric is ready, and the fabric's state read after the
    /// acceptance and after the ready step. A request naming the executable a row records, an
    /// unchanged request, a tags-only and a placement-only request (`PlacementMeta` declares no constraint) are
    /// neither a resize nor a replacement: the fabric stays ready. A path added or removed (a
    /// resize) and another executable (a replacement, the Day-0 admin's included) put it back to pending. A
    /// storage-only change of a held path is neither (proposal executable-bindings-at-create.md rule 5: a
    /// storage or placement change re-births a path only "with its executable"): the fabric stays
    /// ready. Must NOT happen: a request naming what a node already runs, or changing only a
    /// held path's storage, reset the fabric to pending for a Build with nothing to do (Rafka 141).
    #[tokio::test]
    async fn a_reconcile_puts_the_fabric_back_to_pending_only_for_a_resize_or_an_executable_replacement() {
        use crate::accepted::{compile_against, MeshTopology, Recorded, RecordedRow, TopologyChange};
        use crate::build::MeshDesired;
        use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
        use crate::node_rows::WriteRecord;
        use rdm_core::meta::{NodeSpec, PersistentDisposition, PlacementMeta, StorageMeta};
        type Edit = fn(&mut Vec<NodeSpec>);
        let exe = |id: &str, sha: &str| rdm_core::binding::Binding { launch_id: id.into(), executable: format!("/b/{id}-{sha}").into(), sha256: sha.into(), image: None };
        let admin: PathName = "mesh1.admin.1".parse().unwrap();
        let rpc: PathName = "mesh1.rpc.1".parse().unwrap();
        let held = |admin_entry: Option<&str>| {
            let mut m = MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]));
            let mut c = m.spec(&admin).cloned().unwrap();
            c.executable = admin_entry.map(|sha| exe("node_admin", sha));
            m.set_spec(c).unwrap();
            let mut c = m.spec(&rpc).cloned().unwrap();
            c.executable = Some(exe("rpc_node", "rr"));
            m.set_spec(c).unwrap();
            FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), m)].into() }
        };
        fn at<'a>(r: &'a mut [NodeSpec], p: &str) -> &'a mut NodeSpec {
            r.iter_mut().find(|s| s.path.to_string() == p).expect("the request lists the path")
        }
        let rows: [(&str, Option<&str>, Edit, Back); 10] = [
            ("the Day-0 admin named on the executable its row records (its entry names none)", None, |r| at(r, "mesh1.admin.1").executable = Some(rdm_core::binding::Binding { launch_id: "node_admin".into(), executable: "/b/node_admin-aa".into(), sha256: "aa".into(), image: None }), Back::Stays),
            ("every path named on the executable its entry names", Some("aa"), |_| {}, Back::Stays),
            ("the unchanged request: every held path naming none", None, |r| r.iter_mut().for_each(|s| s.executable = None), Back::Stays),
            ("tags only", None, |r| at(r, "mesh1.rpc.1").tags = ["zone_a".to_string()].into(), Back::Stays),
            ("placement only (PlacementMeta declares no constraint)", None, |r| at(r, "mesh1.rpc.1").placement = PlacementMeta::default(), Back::Stays),
            ("another executable for mesh1.rpc.1 (a replacement)", None, |r| at(r, "mesh1.rpc.1").executable = Some(rdm_core::binding::Binding { launch_id: "rpc_node".into(), executable: "/b/rpc_node-cc".into(), sha256: "cc".into(), image: None }), Back::ToPending),
            ("another executable for the Day-0 admin (a replacement)", None, |r| at(r, "mesh1.admin.1").executable = Some(rdm_core::binding::Binding { launch_id: "node_admin".into(), executable: "/b/node_admin-cc".into(), sha256: "cc".into(), image: None }), Back::ToPending),
            ("a new path (a resize)", None, |r| {
                let spec = at(r, "mesh1.rpc.1").clone();
                r.push(NodeSpec { path: "mesh1.rpc.2".parse().unwrap(), ..spec });
            }, Back::ToPending),
            ("a removed path (a resize)", None, |r| r.retain(|s| s.path.to_string() != "mesh1.rpc.1"), Back::ToPending),
            ("storage only", None, |r| at(r, "mesh1.rpc.1").storage_persistence = StorageMeta::Persistent { disposition: PersistentDisposition::Preserve }, Back::Stays),
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
                let mut row = crate::storage::NodeRow::new(path.clone(), node_id.clone(), NodeStatus::Ready);
                row.executable = Some(exe(id, sha));
                store.write_record(&row).await.unwrap();
                recorded.insert(path.clone(), RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id, sha256: Some(sha.into()) });
            }
            let view = projected(vec![fp.clone()]);
            let records = Arc::new(Records::default());
            let _ = records.contacts.set(store.clone());
            let states = states_of(&fp, view.clone(), builds.clone(), records.clone(), FabricId::mint()).await;
            records.set_fabric_state(FabricState::Ready);
            records.set_published_fabric_state(Some(FabricState::Ready));
            {
                let mut change = states.change.lock().await;
                states.step_ready(&mut change, &states.env.builds.current().await).await;
            }
            let mut nodes: Vec<NodeSpec> = topology.meshes.values().flat_map(|m| m.spec_by_path.values().cloned()).collect();
            edit(&mut nodes);
            let change = TopologyChange::Reconcile { topology_storage_provider: None, based_on: a.clone(), nodes };
            let next = compile_against(&topology, &change, &view, &recorded).unwrap_or_else(|e| panic!("{what}: the request compiles: {e}"));
            let b = crate::build::BuildId::parse("bnrrv6tggykq").unwrap();
            builds.publish_accepted(&BuildAccepted { build_id: b.clone(), topology: next.clone(), submitted_change: Some(change), submitted_at_ms: 2_000 }).await.unwrap();
            claim(&builds, &b, 1).await;
            let at_acceptance = states.accepted().await;
            {
                let mut change = states.change.lock().await;
                states.step_ready(&mut change, &states.env.builds.current().await).await;
            }
            let now = records.fabric_state();
            let changed = next != topology;
            let got = match (at_acceptance, now) {
                (true, FabricState::Pending) => Back::ToPending,
                (false, FabricState::Ready) => Back::Stays,
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

    // @feature: on-mesh-ready
    #[test]
    fn notify_mesh_ready_delivers_mesh_ready_ctx_on_own_task_and_emits_bound_attribute() {
        // docs/architecture/fabric-node-hooks.md:125 — "Once its mesh's ready is on its Mesh
        // record (states.md#who-sets-each-state, Q144), the mesh-primary calls `on_mesh_ready`
        // once, with `MeshReadyCtx { fabric_id, mesh_id, mesh }`, on a task of its own. The notice
        // gates nothing in RDM."
        let capture = crate::span_capture::Capture::new("notify-mesh-ready-unit");
        let dispatch = capture.dispatch.clone();
        let d = dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let fabric_id = FabricId::mint();
        let m1_id = MeshId::mint();
        let m2_id = MeshId::mint();
        let m2_id_for_assert = m2_id.clone();
        let fabric_id_for_assert = fabric_id.clone();

        let received = rt.block_on(async move {
            notify_mesh_ready(
                None,
                crate::wiring::MeshReadyCtx {
                    fabric_id: fabric_id.clone(),
                    mesh_id: m1_id,
                    mesh: "mesh1".into(),
                },
            );

            let (tx, rx) = tokio::sync::oneshot::channel();
            let tx_slot = std::sync::Mutex::new(Some(tx));
            let hook: crate::wiring::MeshReadyNotice = Arc::new(move |ctx| {
                if let Some(tx) = tx_slot.lock().unwrap().take() {
                    let _ = tx.send(ctx);
                }
                Box::pin(async {})
            });
            notify_mesh_ready(
                Some(hook),
                crate::wiring::MeshReadyCtx {
                    fabric_id,
                    mesh_id: m2_id,
                    mesh: "mesh2".into(),
                },
            );
            rx.await.expect("on_mesh_ready invoked on its own task")
        });
        drop(rt);

        assert_eq!(received.fabric_id, fabric_id_for_assert);
        assert_eq!(received.mesh_id, m2_id_for_assert);
        assert_eq!(received.mesh, "mesh2");

        let spans = capture.named("rdm.node_admin.mesh.update.via-on-mesh-ready");
        assert!(
            spans.iter().any(|s| {
                s.attributes.get("mesh").map(String::as_str) == Some("mesh1")
                    && s.attributes.get("bound").map(String::as_str) == Some("false")
            }),
            "expected unbound via-on-mesh-ready span for mesh1: {spans:?}"
        );
        assert!(
            spans.iter().any(|s| {
                s.attributes.get("mesh").map(String::as_str) == Some("mesh2")
                    && s.attributes.get("mesh_id").map(String::as_str)
                        == Some(m2_id_for_assert.as_str())
                    && s.attributes.get("bound").map(String::as_str) == Some("true")
            }),
            "expected bound via-on-mesh-ready span for mesh2: {spans:?}"
        );
    }

    // @feature: on-fabric-ready
    #[test]
    fn notify_fabric_ready_delivers_attempt_context_once_skips_when_adopting_ready_and_fires_again_after_resize_re_enters_ready() {
        // docs/architecture/fabric-node-hooks.md:131 — "After the ready state is set, the
        // fabric-primary sends the application one `on_fabric_ready` notice
        // (lifecycles/states.md#the-fabrics-ready), with
        // `FabricReadyCtx { fabric_id, build_id, attempt, context }`: the accepted Build and
        // attempt whose change made the fabric ready and that attempt's observability context."
        // docs/architecture/lifecycles/states.md:187 — "When the fabric-primary changes, the new
        // fabric-primary adopts the published fabric status; it does not reset to pending and does
        // not re-send `on_fabric_ready`."
        use crate::accepted::MeshTopology;
        use crate::build::MeshDesired;
        use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};

        let capture = crate::span_capture::Capture::new("notify-fabric-ready-unit");
        let dispatch = capture.dispatch.clone();
        let d = dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let fabric_id = FabricId::mint();
        let build_a = crate::build::BuildId::parse("amrrv6tggykq").unwrap();
        let build_b = crate::build::BuildId::parse("bnrrv6tggykq").unwrap();
        let build_a_for_assert = build_a.clone();
        let build_b_for_assert = build_b.clone();
        let fabric_id_for_assert = fabric_id.clone();

        let (first_ctx, second_ctx) = rt.block_on(async move {
            let builds = Arc::new(MemoryBuildStateAdapter::new());
            let contexts = Arc::new(AttemptContexts::in_memory());
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<FabricReadyCtx>();
            let hooks = FabricHooks {
                on_fabric_ready: Some(Arc::new(move |ctx| {
                    let _ = tx.send(ctx);
                    Box::pin(async {})
                })),
                ..FabricHooks::no_app_work()
            };

            let mesh1_a = (
                "mesh1".to_string(),
                MeshTopology::of(&MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1)])),
            );
            builds
                .publish_accepted(&BuildAccepted {
                    build_id: build_a.clone(),
                    topology: FabricTopology {
                        section_of: None,
                        pinned: Vec::new(),
                        storage_change: None,
                        topology_storage_provider: None,
                        fabric: "fabric1".into(),
                        meshes: [mesh1_a].into(),
                    },
                    submitted_change: None,
                    submitted_at_ms: 1_000,
                })
                .await
                .unwrap();
            claim(&builds, &build_a, 1).await;
            let ctx_a = rdm_core::context::CallContext {
                traceparent: Some(
                    "00-11111111111111111111111111111111-2222222222222222-01".into(),
                ),
                tracestate: None,
                baggage: Some("build=a".into()),
                caller_system: Some("test".into()),
            };
            contexts.put(&build_a, 1, &ctx_a).await.unwrap();

            let (fp, _) = fabric_primary_and_mesh2();
            let view_a = projected(vec![fp.clone()]);
            let records = Arc::new(Records::default());
            every_mesh_ready(&records, None);

            let ep = rdm_transport::endpoint::bind(
                iroh::SecretKey::generate(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .unwrap();
            let states = FabricStates::new(StatesEnv {
                me: fp.name.clone(),
                node_id: fp.node_id.clone(),
                fabric_id: fabric_id.clone(),
                topology: Arc::new(RwLock::new(view_a.clone())),
                client: Arc::new(NodeRpcClient::new(
                    ep.clone(),
                    Arc::new(rdm_transport::StaticResolver::new()),
                )),
                builds: builds.clone(),
                contexts: contexts.clone(),
                records: records.clone(),
                hooks: hooks.clone(),
            });

            // 1. Entry from Pending to Ready fires `on_fabric_ready` once with `build_a`'s CallContext.
            let read_a = builds.current().await;
            states
                .change_step(ChangeInputs {
                    is_fabric_primary: true,
                    heard: Some("pending"),
                    view: &view_a,
                    current: &read_a,
                })
                .await;
            assert_eq!(records.fabric_state(), FabricState::Ready);
            let first_ctx = rx.recv().await.expect("first on_fabric_ready delivered");
            records.set_published_fabric_state(Some(FabricState::Ready));

            // 2. Subsequent turns while Ready do not fire `on_fabric_ready` again.
            states
                .change_step(ChangeInputs {
                    is_fabric_primary: true,
                    heard: Some("ready"),
                    view: &view_a,
                    current: &read_a,
                })
                .await;
            assert!(rx.try_recv().is_err(), "no duplicate on_fabric_ready while staying Ready");

            // 3. A successor fabric-primary adopting `heard = Some("ready")` does not re-send `on_fabric_ready`.
            let successor_records = Arc::new(Records::default());
            every_mesh_ready(&successor_records, None);
            successor_records.set_published_fabric_state(Some(FabricState::Ready));
            let successor_states = FabricStates::new(StatesEnv {
                me: fp.name.clone(),
                node_id: fp.node_id.clone(),
                fabric_id: fabric_id.clone(),
                topology: Arc::new(RwLock::new(view_a.clone())),
                client: Arc::new(NodeRpcClient::new(
                    ep.clone(),
                    Arc::new(rdm_transport::StaticResolver::new()),
                )),
                builds: builds.clone(),
                contexts: contexts.clone(),
                records: successor_records.clone(),
                hooks,
            });
            successor_states
                .change_step(ChangeInputs {
                    is_fabric_primary: true,
                    heard: Some("ready"),
                    view: &view_a,
                    current: &read_a,
                })
                .await;
            assert_eq!(successor_records.fabric_state(), FabricState::Ready);
            assert!(
                rx.try_recv().is_err(),
                "adopting Ready on takeover does not re-send on_fabric_ready"
            );

            // 4. Accepting a resize Build `build_b` resets the fabric to Pending, and when the new
            //    topology becomes Ready, `on_fabric_ready` fires again with `build_b`'s CallContext.
            let mesh1_b = (
                "mesh1".to_string(),
                MeshTopology::of(&MeshDesired::of(
                    "mesh1",
                    [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)],
                )),
            );
            builds
                .publish_accepted(&BuildAccepted {
                    build_id: build_b.clone(),
                    topology: FabricTopology {
                        section_of: None,
                        pinned: Vec::new(),
                        storage_change: None,
                        topology_storage_provider: None,
                        fabric: "fabric1".into(),
                        meshes: [mesh1_b].into(),
                    },
                    submitted_change: None,
                    submitted_at_ms: 2_000,
                })
                .await
                .unwrap();
            claim(&builds, &build_b, 1).await;
            claim(&builds, &build_b, 2).await;
            let ctx_b = rdm_core::context::CallContext {
                traceparent: Some(
                    "00-33333333333333333333333333333333-4444444444444444-01".into(),
                ),
                tracestate: None,
                baggage: Some("build=b".into()),
                caller_system: Some("resize".into()),
            };
            contexts.put(&build_b, 2, &ctx_b).await.unwrap();

            assert!(states.accepted().await, "resize puts fabric back to Pending");
            assert_eq!(records.fabric_state(), FabricState::Pending);
            records.set_published_fabric_state(Some(FabricState::Pending));

            let view_b = projected(vec![
                fp,
                node("mesh1.rpc.1", NodeStatus::Ready, false),
            ]);
            let read_b = builds.current().await;
            states
                .change_step(ChangeInputs {
                    is_fabric_primary: true,
                    heard: Some("pending"),
                    view: &view_b,
                    current: &read_b,
                })
                .await;
            assert_eq!(records.fabric_state(), FabricState::Ready);
            let second_ctx = rx
                .recv()
                .await
                .expect("second on_fabric_ready delivered after resize re-enters Ready");

            ep.close().await;
            (first_ctx, second_ctx)
        });
        drop(rt);

        assert_eq!(first_ctx.fabric_id, fabric_id_for_assert);
        assert_eq!(first_ctx.build_id, build_a_for_assert.to_string());
        assert_eq!(first_ctx.attempt, 1);
        assert_eq!(
            first_ctx.context.baggage.as_deref(),
            Some("build=a")
        );

        assert_eq!(second_ctx.fabric_id, fabric_id_for_assert);
        assert_eq!(second_ctx.build_id, build_b_for_assert.to_string());
        assert_eq!(second_ctx.attempt, 2);
        assert_eq!(
            second_ctx.context.baggage.as_deref(),
            Some("build=b")
        );

        let on_ready = capture.named("rdm.node_admin.fabric.update.via-on-fabric-ready");
        assert_eq!(
            on_ready.len(),
            2,
            "via-on-fabric-ready emitted once per entry to Ready (not on adoption): {on_ready:?}"
        );
        let build_a_log = build_a_for_assert.log().to_string();
        let build_b_log = build_b_for_assert.log().to_string();
        assert!(
            on_ready.iter().any(|s| {
                s.attributes.get("build_id").map(String::as_str)
                    == Some(build_a_log.as_str())
                    && s.attributes.get("attempt").map(String::as_str) == Some("1")
                    && s.attributes.get("bound").map(String::as_str) == Some("true")
            }),
            "expected via-on-fabric-ready for build_a attempt 1"
        );
        assert!(
            on_ready.iter().any(|s| {
                s.attributes.get("build_id").map(String::as_str)
                    == Some(build_b_log.as_str())
                    && s.attributes.get("attempt").map(String::as_str) == Some("2")
                    && s.attributes.get("bound").map(String::as_str) == Some("true")
            }),
            "expected via-on-fabric-ready for build_b attempt 2"
        );
    }
}
