//! The digest view (`DigestView`): every birth a node holds, its liveness and its overlays,
//! and the routing watch over it.


use anyhow::Result;
use rdm_core::{IncarnationId, LifecycleOp, MeshDigest, NodeId, Primary, PrimaryRole, DELETED_RETENTION};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdm_core::cadence::staleness_floor;

use crate::{location::*, status::*, primary::*, topology::*};

/// Whether a forwarded digest carries its status: a member projection entry (a topology read) does;
/// a join digest, like every heartbeat, does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Carried {
    Status,
    NoStatus,
}

/// How a held member was last heard. Only `Direct` and `Forwarded` are liveness: a word received
/// on the mesh channel or the backbone. `Topology` is a member known only from forwarded topology
/// (a delta, a full, a replay, an entry answer): it is held, and never ages to silent for want of
/// a forwarded frame (gossip.md §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heard {
    /// The member's own digest, received here.
    Direct,
    /// A peer mesh's member listed in a mesh topology received on the backbone: it ages from that
    /// receipt at the staleness floor, as a member heard directly does (R-G2: a node-admin judges a
    /// peer mesh from direct backbone receipt).
    Forwarded,
    /// Known only from topology forwarded onto the mesh channel.
    Topology,
}

/// What a view holds of one stopped birth (`NodeStopped`).
#[derive(Debug, Clone)]
pub(crate) struct StopMark {
    /// The stopped birth.
    pub(crate) incarnation: IncarnationId,
    /// The `digest_seq` of the digest this view held of it when it dropped it, 0 when it held none.
    pub(crate) held_seq: u64,
    /// The digest this view held of it when it dropped it: a node-admin keeps showing the birth.
    pub(crate) held: Option<MeshDigest>,
    /// The latest digest of the stopped birth this view refused since, with when it was heard and
    /// how: what the mesh-primary holds the birth from again at `node.joined`, and what a member
    /// holds an own-mesh birth from again when its projection ends the stop.
    pub(crate) latest: Option<(MeshDigest, Instant, Heard)>,
    /// The highest version, and its publisher, at which a projection of the birth's mesh this view
    /// applied carried the stop. A later projection of that source listing the birth moved past it.
    pub(crate) carried: Option<(String, u64)>,
}

/// One projection frame as its receiver applies it: who published it, on which channel, and what
/// the receiver held of that source before and after it. With the version at which a projection
/// carried a stop ([`StopMark::carried`]), what tells a projection that moved past a stop from one
/// that never carried it here.
#[derive(Debug, Clone, Default)]
pub(crate) struct Target {
    /// The source's publisher (its exact birth): the version's revision.
    pub(crate) publisher: String,
    /// Where the frame arrived.
    pub(crate) channel: &'static str,
    /// The source held before the frame, `(publisher, version)`, when one was held.
    pub(crate) held: Option<(String, u64)>,
    /// The version the frame carries.
    pub(crate) version: u64,
    /// The stopped births `(node id, incarnation)` whose stop the held source carried before the frame.
    pub(crate) prior_stops: BTreeSet<(String, String)>,
    /// The stopped births whose stop the held source carries after the frame.
    pub(crate) post_stops: BTreeSet<(String, String)>,
}

impl Target {
    /// The stop births a source projection carries.
    pub(crate) fn stops_of(full: Option<&crate::snapshot::Full>) -> BTreeSet<(String, String)> {
        full.map(|f| f.in_flight().into_iter().filter(LifecycleOp::is_stop).map(|o| (o.node_id.to_string(), o.incarnation.to_string())).collect()).unwrap_or_default()
    }

    /// The version held before the frame, for spans: -1 when none was held.
    pub(crate) fn held_version(&self) -> i64 {
        self.held.as_ref().map(|(_, v)| *v as i64).unwrap_or(-1)
    }
}

/// One member as the view holds it: its latest digest, when it was heard and how.
pub(crate) type HeldMember = (MeshDigest, Instant, Heard);

/// The members a [`DigestView`] holds by NodeId, with each birth also found by its endpoint key and
/// by its (path, name). Every write goes through [`Self::insert`] and [`Self::remove`], which keep the
/// two lookups in step with the map; a read of the map is a read of it as before (`Deref`).
///
/// Two births can share an endpoint key (a node re-born over its inherited data dir keeps the key
/// and takes a new NodeId) or a (path, name) (a successor at the same (path, name) while its predecessor is
/// still held). Each key lists its births in the order the view first held each birth, or held its
/// changed birth (what ticks [`DigestView::birth_changes`]); a fresher digest of the same birth keeps
/// its place. A lookup answers the last of them: the birth the view took most recently.
#[derive(Debug, Default)]
pub(crate) struct HeldMembers {
    pub(crate) map: HashMap<String, HeldMember>,
    pub(crate) by_endpoint: HashMap<String, Vec<String>>,
    pub(crate) by_name: HashMap<rdm_core::PathName, Vec<String>>,
}

impl std::ops::Deref for HeldMembers {
    type Target = HashMap<String, HeldMember>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl HeldMembers {
    /// Hold `entry` for `node_id`, as `HashMap::insert`.
    pub(crate) fn insert(&mut self, node_id: String, entry: HeldMember) -> Option<HeldMember> {
        let same_birth = self.map.get(&node_id).is_some_and(|(held, _, _)| held.node == entry.0.node);
        if !same_birth {
            self.unlist(&node_id);
            self.by_endpoint.entry(entry.0.node.endpoint_id.0.clone()).or_default().push(node_id.clone());
            self.by_name.entry(entry.0.node.location()).or_default().push(node_id.clone());
        }
        self.map.insert(node_id, entry)
    }

    /// Drop `node_id`, as `HashMap::remove`.
    pub(crate) fn remove(&mut self, node_id: &str) -> Option<HeldMember> {
        self.unlist(node_id);
        self.map.remove(node_id)
    }

    /// Take `node_id` out of the lookups of the birth held for it.
    pub(crate) fn unlist(&mut self, node_id: &str) {
        let Some((held, _, _)) = self.map.get(node_id) else { return };
        if let Some(ids) = self.by_endpoint.get_mut(&held.node.endpoint_id.0) {
            ids.retain(|id| id != node_id);
            if ids.is_empty() {
                self.by_endpoint.remove(&held.node.endpoint_id.0);
            }
        }
        if let Some(ids) = self.by_name.get_mut(&held.node.location()) {
            ids.retain(|id| id != node_id);
            if ids.is_empty() {
                self.by_name.remove(&held.node.location());
            }
        }
    }

    /// The member held at endpoint key `endpoint_id` (see the type's note for two births at one key).
    pub(crate) fn at_endpoint(&self, endpoint_id: &str) -> Option<&HeldMember> {
        self.by_endpoint.get(endpoint_id).and_then(|ids| ids.last()).and_then(|id| self.map.get(id))
    }

    /// The member held at `name` (see the type's note for two births at one (path, name)).
    pub(crate) fn at_name(&self, name: &rdm_core::PathName) -> Option<&HeldMember> {
        self.by_name.get(name).and_then(|ids| ids.last()).and_then(|id| self.map.get(id))
    }

    /// Restart every member's silence at `now`. No lookup key moves.
    pub(crate) fn restart_silence(&mut self, now: Instant) {
        for (_, at, _) in self.map.values_mut() {
            *at = now;
        }
    }
}

/// The latest digest held per logical node: when it was taken, and how it was heard.
#[derive(Debug, Clone)]
pub struct DigestView {
    /// Each member's latest digest, when it was heard and how. An own-mesh birth's `status` here is
    /// never read: its one status is `member_statuses`'s, and every read that hands out a digest
    /// takes it from there ([`Self::hand_out`]).
    pub(crate) inner: Arc<Mutex<HeldMembers>>,
    /// Ticks when a member's birth (`MeshDigest::node`) is first held or
    /// changes, or a departure is accepted; a fresher digest of the same
    /// birth does not tick.
    pub(crate) births: Arc<tokio::sync::watch::Sender<u64>>,
    /// Ticks when a held member is first held, changes its birth or its status, or is heard again
    /// after it had gone silent past the staleness floor (on its own digest or a forwarded copy), and
    /// when a birth held only from its mesh-primary's projection changes its status: what a local
    /// waiter on "that node is ready" or "that node is reachable again" wakes on. A local observation
    /// of the view; nothing is sent.
    pub(crate) heard: Arc<tokio::sync::watch::Sender<u64>>,
    /// Ticks when an open lifecycle overlay is added or cleared, or a departure is accepted: the
    /// overlays and departures a mesh primary's mesh topology carries. A local observation; nothing is
    /// sent.
    pub(crate) overlays: Arc<tokio::sync::watch::Sender<u64>>,
    /// Ticks each time a digest is taken, a fresher digest of a held birth included (its name,
    /// config, load or `digest_seq` moved): what a local waiter on "that node published again"
    /// wakes on. A local observation of the view; nothing is sent.
    pub(crate) digests: Arc<tokio::sync::watch::Sender<u64>>,
    /// Ticks when the pruner's sweep ([`DigestView::mark_silence`]) finds the births held silent past
    /// the staleness floor moved: one fell silent, or one held silent is no longer (heard again,
    /// left, or its silence restarted). The one clock of a held birth's silence (Q254): what the view
    /// rebuild waits on in place of a poll. A local observation; nothing is sent.
    pub(crate) silence: Arc<tokio::sync::watch::Sender<u64>>,
    /// The births, by `(node id, incarnation)`, the pruner's last sweep found silent past the
    /// staleness floor: held directly or on the backbone and unheard since, or held only from the
    /// mesh-primary's projection since it was first held.
    pub(crate) silent: Arc<Mutex<BTreeSet<(String, String)>>>,
    /// Proven departures by NodeId, with when this view accepted each: held
    /// for `retention` of local age, then forgotten.
    pub(crate) deleted: Arc<Mutex<HashMap<String, (LifecycleOp, Instant)>>>,
    /// The exact births this admin's node recovery holds deleted from its verdict until their
    /// `NodeDeleted` is applied here, by NodeId (node-recovery.md step 3: "A birth that comes back once
    /// its verdict is not `reached`, during its replacement's birth or after it, is a deleted birth").
    /// Local to this view: refused on receipt, never projected or published.
    pub(crate) recovering: Arc<Mutex<HashMap<String, IncarnationId>>>,
    /// The deletions this process executed and published as `NodeDeleted`, by the deleted NodeId: its
    /// mesh's topology carries each in `TopologyFull.deleted` for the retention, whatever mesh the deleted
    /// birth was in (gossip.md §3.1 `NodeDeleted`: the "executor's own mesh channel; backbone
    /// Members.deleted"). A deletion this view only heard is not one of them.
    pub(crate) executed_deletions: Arc<Mutex<std::collections::HashSet<String>>>,
    /// The departures the deleted target refused, by the NodeId of the departure held: each operation
    /// key refused by name once. Its publisher carries it in every mesh topology for the retention, and a
    /// later copy is a copy of a refusal already made. Forgotten with the departure it was refused by.
    pub(crate) refused_departures: Arc<Mutex<HashMap<String, BTreeSet<(String, rdm_core::OperationId)>>>>,
    /// Open lifecycle overlays by operation key: the node is held, and not routable.
    pub(crate) in_flight: Arc<Mutex<HashMap<(String, rdm_core::OperationId), LifecycleOp>>>,
    /// The deleted births heard again on this node's mesh channel by their own digest, by (NodeId,
    /// incarnation): each refused, each owed a report to the fabric-primary (Q128).
    pub(crate) returned: Arc<Mutex<BTreeMap<(String, String), MeshDigest>>>,
    /// When this node's own loop last ran ([`Self::mark_awake`]); `None` for a view no loop reads. A gap
    /// between two of its runs longer than the staleness floor is time this process was not running.
    pub(crate) awake: Arc<Mutex<Option<Instant>>>,
    /// The stopped births (`NodeStopped`) by NodeId: what this view held of each when it dropped it,
    /// and the latest heartbeat of it refused since. No heartbeat of the stopped birth is taken; only
    /// the mesh-primary's `node.joined` ([`Self::joined`]), its projection moving past the stop
    /// (`View::apply_full`), a later incarnation of the NodeId or a departure ends the stop.
    pub(crate) stopped_marks: Arc<Mutex<HashMap<String, StopMark>>>,
    /// Whether this view's holder is its mesh's primary now (the same flag its view holds): the
    /// primary authors its members' statuses; every other member takes them from its projection.
    pub(crate) primary: Arc<std::sync::atomic::AtomicBool>,
    /// The holder's own mesh: an own-mesh birth's status comes from that mesh's primary (gossip.md
    /// §3.1 `TopologyFull`). Empty for a view no topology view holds: no birth is own-mesh to it.
    pub(crate) own_mesh: Arc<Mutex<String>>,
    /// The holder's own NodeId, once it published: its own birth's word about itself is its own.
    pub(crate) own: Arc<Mutex<Option<String>>>,
    /// This process's explicitly supplied predecessor incarnation, never inferred from its location.
    pub(crate) own_predecessor: Arc<Mutex<Option<IncarnationId>>>,
    /// The status this view holds for each own-mesh birth, by NodeId: what the mesh-primary's
    /// projection said, or, while this view's holder is the primary, what it authored. The only copy:
    /// no held digest carries an own-mesh birth's status.
    pub(crate) member_statuses: Arc<Mutex<HashMap<String, (IncarnationId, rdm_core::MemberStatus)>>>,
    /// The own-mesh births this view holds only from its mesh-primary's projection, by NodeId: the
    /// birth as the projection carried it (its identity, endpoint and runtime fact: gossip.md §3,
    /// "needed by an authority-bearing admin to manage that birth"), and the instant this view first
    /// held it. The member's own Digest never reached this view, so it is unheard since that instant
    /// (node-recovery.md "Detection and proof" step 1). A projection carries no liveness: a later
    /// entry of the same birth moves nothing of it, and a version that no longer lists it is no
    /// departure. The member's own digest supersedes it; its departure or stop removes it.
    pub(crate) projected: Arc<Mutex<HashMap<String, (MeshDigest, Instant)>>>,
    /// The lifecycle overlays that ended here, by complete operation identity and exact birth: a
    /// later copy of the same overlay (a snapshot built before the rejoin, the primary's publication
    /// re-applying an operation whose `Complete` is not receipted yet) opens nothing.
    pub(crate) ended_overlays: Arc<Mutex<std::collections::HashSet<crate::receive_order::LifecycleIdentity>>>,
    /// The path name of the node holding this view, named by the refusals it writes.
    pub(crate) observer_name: Arc<Mutex<String>>,
    /// The name the mesh-primary's row last gave this birth (`ApplyNodeName`, R12), shared with its
    /// the topology view; `None` while the birth holds its launch name.
    pub(crate) held_name: Arc<Mutex<Option<rdm_core::PathName>>>,
    /// The mesh names the accepted Build holds, once this node holds the Build: the authority on
    /// which meshes the fabric has.
    pub(crate) accepted_meshes: Arc<Mutex<Option<BTreeSet<String>>>>,
    /// Ticks when the mesh names the accepted Build holds change ([`Self::set_accepted_meshes`]): the
    /// view's rebuild reads them ([`RoutingWatch`]). A local observation; nothing is sent.
    pub(crate) accepted: Arc<tokio::sync::watch::Sender<u64>>,
    pub(crate) retention: Duration,
    /// The staleness floor.
    pub(crate) staleness_floor: Duration,
    /// The primaries this node holds (a digest, a `TopologyFull` projection, an entry read): an input to every election.
    pub primaries: PrimaryView,
    /// The mesh and fabric statuses this node holds (`Frame::MeshStatus`, `Frame::FabricStatus`, a
    /// replay or an entry answer): application routing reads a birth's mesh status here.
    pub statuses: StatusView,
    pub(crate) on_mesh_deleted: MeshDeletedHook,
}

#[derive(Clone, Default)]
pub(crate) struct MeshDeletedHook(pub(crate) Arc<Mutex<Option<Arc<dyn Fn(&str) + Send + Sync>>>>);

impl std::fmt::Debug for MeshDeletedHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MeshDeletedHook")
    }
}

impl Default for DigestView {
    fn default() -> Self {
        Self::with_retention(DELETED_RETENTION)
    }
}

/// The routing facts a view holds, watched: a held member first held, its birth or status changed,
/// heard again after silence; a held birth silent past the staleness floor, marked by the pruner's
/// sweep ([`DigestView::mark_silence`]); a lifecycle overlay opened or closed; a departure taken or
/// forgotten; a mesh status taken; the meshes the accepted Build names. What application
/// routing's view rebuild wakes on, so a fact is applied on receipt (states.md "Routing"); a caller
/// adds the watches of its own inputs ([`Self::watching`]). A local observation of the view; nothing
/// is sent.
pub struct RoutingWatch {
    /// Each watch by the name a wake reports. A watch whose sender is gone moves nothing again and
    /// leaves the list; the others are still waited on.
    pub(crate) watches: Vec<(&'static str, tokio::sync::watch::Receiver<u64>)>,
}

impl RoutingWatch {
    /// `self`, also waiting on `watch`, named `name` when it moves.
    pub fn watching(mut self, name: &'static str, watch: tokio::sync::watch::Receiver<u64>) -> Self {
        self.watches.push((name, watch));
        self
    }

    /// Every fact moved so far is seen: called right before a rebuild reads the view, so a fact taken
    /// while it reads wakes the next one.
    pub fn seen(&mut self) {
        for (_, w) in self.watches.iter_mut() {
            w.borrow_and_update();
        }
    }

    /// Returns when a fact moved since [`RoutingWatch::seen`], naming the watch that moved (`heard`,
    /// `births`, `overlays`, `meshes`, `silence`, `accepted`, or a name given to [`Self::watching`]). A
    /// watch whose sender is gone leaves the wait and the others are waited on; with every one gone,
    /// nothing can move, and this never returns.
    pub async fn moved(&mut self) -> &'static str {
        loop {
            if self.watches.is_empty() {
                return std::future::pending().await;
            }
            let (moved, at, _) = futures_util::future::select_all(self.watches.iter_mut().map(|(name, w)| Box::pin(async move { w.changed().await.map(|()| *name) }))).await;
            match moved {
                Ok(name) => return name,
                Err(_) => {
                    self.watches.remove(at);
                }
            }
        }
    }
}

/// A lifecycle overlay (`NodeStopped`, or a `NodeDeleted` other than the departure held) of a birth
/// already held deleted: it arrived after the birth left (its mesh's `MeshLeft`, or its own
/// `NodeDeleted`) and resurrects nothing.
pub(crate) fn reject_deleted_overlay(observer: &str, op: &LifecycleOp, frame: &'static str) {
    tracing::info_span!(
        "rdm.mesh.topology.reject.via-deleted-birth",
        observer,
        node_path = %op.path, node_name = %op.name,
        node_id = %op.node_id,
        incarnation_id = %op.incarnation.as_str(),
        frame,
        build_id = %op.build_id,
        operation = %op.operation.action, operation_subject = ?op.operation.subject,
    )
    .in_scope(|| tracing::info!("a lifecycle overlay of a deleted birth: refused, the departure stands"));
}

/// Why a digest was not taken.
pub(crate) fn reject_deleted(d: &MeshDigest, via: &'static str) {
    tracing::info_span!(
        "rdm.mesh.topology.reject.via-deleted-birth",
        node_path = %d.node.path, node_name = %d.node.name,
        node_id = %d.node.node_id,
        incarnation_id = %d.node.incarnation.as_str(),
        via,
    )
    .in_scope(|| tracing::info!("a digest of a deleted node: refused, the departure stands"));
}

/// How long a member has gone unheard at `now`: the time since its receipt, a member heard directly
/// or on the backbone alike. A member known only from forwarded topology never ages, whatever status
/// it is listed with (R-G2: forwarded topology carries topology, never liveness).
pub(crate) fn unheard(at: &Instant, heard: Heard, now: Instant) -> Duration {
    match heard {
        Heard::Topology => Duration::ZERO,
        _ => now.saturating_duration_since(*at),
    }
}

/// One birth has one runtime: a digest of the held incarnation that names
/// another runtime locator or control domain is refused by name, and the
/// held one stays.
pub(crate) fn runtime_changed(held: &MeshDigest, d: &MeshDigest) -> bool {
    let changed = held.node.incarnation == d.node.incarnation
        && held.node.runtime.is_some()
        && d.node.runtime.is_some()
        && held.node.runtime != d.node.runtime;
    if changed {
        tracing::info_span!(
            "rdm.mesh.runtime.reject.via-locator-changed",
            node_path = %d.node.path, node_name = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.as_str(),
            held_locator_fingerprint = %held.node.runtime.as_ref().map(|r| r.locator_fingerprint()).unwrap_or_default(),
            offered_locator_fingerprint = %d.node.runtime.as_ref().map(|r| r.locator_fingerprint()).unwrap_or_default(),
        )
        .in_scope(|| tracing::info!("a birth offered another runtime"));
    }
    changed
}

impl DigestView {
    /// The path name of the node holding this view, when set by its topology view.
    pub fn observer_name(&self) -> Option<String> {
        let name = self.observer_name.lock().unwrap();
        (!name.is_empty()).then(|| name.clone())
    }

    /// The [`PathName`](rdm_core::PathName) the mesh-primary's row last gave this view's
    /// holder (`ApplyNodeName`, R12), or `None` while the holder has its launch name.
    pub fn held_path_name(&self) -> Option<rdm_core::PathName> {
        self.held_name.lock().unwrap().clone()
    }

    pub(crate) fn with_retention(retention: Duration) -> Self {
        Self::with_floor(retention, staleness_floor())
    }

    /// A view with this staleness floor.
    pub(crate) fn with_floor(retention: Duration, staleness_floor: Duration) -> Self {
        Self {
            inner: Arc::default(),
            births: Arc::new(tokio::sync::watch::Sender::new(0)),
            heard: Arc::new(tokio::sync::watch::Sender::new(0)),
            overlays: Arc::new(tokio::sync::watch::Sender::new(0)),
            digests: Arc::new(tokio::sync::watch::Sender::new(0)),
            silence: Arc::new(tokio::sync::watch::Sender::new(0)),
            silent: Arc::default(),
            deleted: Arc::default(),
            recovering: Arc::default(),
            executed_deletions: Arc::default(),
            refused_departures: Arc::default(),
            in_flight: Arc::default(),
            awake: Arc::default(),
            returned: Arc::default(),
            stopped_marks: Arc::default(),
            primary: Arc::default(),
            own_mesh: Arc::default(),
            own: Arc::default(),
            member_statuses: Arc::default(),
            projected: Arc::default(),
            ended_overlays: Arc::default(),
            observer_name: Arc::default(),
            own_predecessor: Arc::default(),
            held_name: Arc::default(),
            accepted_meshes: Arc::default(),
            accepted: Arc::new(tokio::sync::watch::Sender::new(0)),
            retention,
            staleness_floor,
            primaries: PrimaryView::default(),
            statuses: StatusView::default(),
            on_mesh_deleted: MeshDeletedHook::default(),
        }
    }

    /// The staleness floor: a member unheard for longer is `PendingReconnect`.
    pub fn staleness_floor(&self) -> Duration {
        self.staleness_floor
    }

    pub(crate) fn expire_deleted(&self) {
        self.expire_deleted_at(Instant::now())
    }

    /// Forget each departure held longer than the retention at `now`. A departure forgotten moves the
    /// overlays watch where the retention's clock is read: the view no longer holds the birth
    /// deleted, which its rebuild reads.
    pub(crate) fn expire_deleted_at(&self, now: Instant) {
        let expired = {
            let mut deleted = self.deleted.lock().unwrap();
            let before = deleted.len();
            deleted.retain(|_, (_, at)| now.duration_since(*at) < self.retention);
            self.refused_departures.lock().unwrap().retain(|id, _| deleted.contains_key(id));
            deleted.len() < before
        };
        if expired {
            self.overlays.send_modify(|v| *v += 1);
        }
    }

    /// Has this node been accepted as deleted, within the retention?
    pub fn is_deleted(&self, node_id: &str) -> bool {
        self.is_deleted_at(node_id, Instant::now())
    }

    /// Hold the exact birth `(node_id, incarnation)` deleted at this view from its node recovery's
    /// verdict (node-recovery.md step 3; Q342): its digests are refused by name on every channel, as a
    /// deleted birth's are, until the recovery's `NodeDeleted` is applied here. Nothing is published.
    pub fn hold_recovering(&self, node_id: &NodeId, incarnation: &IncarnationId) {
        let held = self.recovering.lock().unwrap().insert(node_id.to_string(), incarnation.clone());
        if held.as_ref() != Some(incarnation) {
            tracing::info_span!(
                "rdm.mesh.topology.update.via-recovering-held",
                observer = %self.observer_name.lock().unwrap(),
                node_id = %node_id,
                incarnation_id = %incarnation.as_str(),
            )
            .in_scope(|| tracing::info!("the birth's verdict is not reached: held deleted at this view from its recovery's start"));
        }
    }

    /// Whether the exact birth is held deleted from its recovery's verdict ([`Self::hold_recovering`]).
    pub fn is_recovering(&self, node_id: &str, incarnation: &IncarnationId) -> bool {
        self.recovering.lock().unwrap().get(node_id) == Some(incarnation)
    }

    /// `d` refused by name when its birth is held deleted from its recovery's verdict.
    pub(crate) fn refuses_recovering(&self, d: &MeshDigest, via: &'static str) -> bool {
        if !self.is_recovering(d.node.node_id.as_str(), &d.node.incarnation) {
            return false;
        }
        reject_deleted(d, via);
        true
    }

    /// Is `node_id` held as deleted at `now`?
    pub(crate) fn is_deleted_at(&self, node_id: &str, now: Instant) -> bool {
        self.expire_deleted_at(now);
        self.deleted.lock().unwrap().contains_key(node_id)
    }

    /// Accept a proven departure: the birth leaves the view, the NodeId is
    /// held deleted for the retention, and every overlay of the operation
    /// clears. `false` when the departure was already held.
    pub fn depart(&self, op: LifecycleOp) -> bool {
        self.depart_at(op, Instant::now())
    }

    /// Accept the departure `op` at `now`.
    pub fn depart_at(&self, op: LifecycleOp, now: Instant) -> bool {
        self.expire_deleted_at(now);
        let id = op.node_id.to_string();
        {
            let mut deleted = self.deleted.lock().unwrap();
            match crate::receive_order::node_deleted_receipt(deleted.get(&id).map(|(held, _)| held), &op) {
                crate::receive_order::NodeDeletedReceipt::Apply => {}
                crate::receive_order::NodeDeletedReceipt::Duplicate => return false,
                crate::receive_order::NodeDeletedReceipt::AlreadyDeleted => {
                    let first_refusal = self.refused_departures.lock().unwrap().entry(id.clone()).or_default().insert(op.key());
                    drop(deleted);
                    if first_refusal {
                        reject_deleted_overlay(&self.observer_name.lock().unwrap(), &op, "node-deleted");
                    }
                    return false;
                }
            }
            deleted.insert(id.clone(), (op.clone(), now));
        }
        self.recovering.lock().unwrap().remove(&id);
        delete_incarnation(&op.node_id, &op.incarnation);
        self.end_stop(op.node_id.as_str());
        self.in_flight.lock().unwrap().remove(&op.key());
        self.overlays_moved();
        self.member_statuses.lock().unwrap().remove(&id);
        self.projected.lock().unwrap().remove(&id);
        let removed = self.inner.lock().unwrap().remove(&id).is_some();
        tracing::info_span!(
            "rdm.mesh.topology.remove.via-node-deleted",
            observer = %self.observer_name.lock().unwrap(),
            node_path = %op.path, node_name = %op.name,
            node_id = %op.node_id,
            incarnation_id = %op.incarnation.as_str(),
            build_id = %op.build_id,
            operation = %op.operation.action, operation_subject = ?op.operation.subject,
            was_held = removed,
        )
        .in_scope(|| tracing::info!("the exact birth has left: removed from the view, held deleted"));
        self.births.send_modify(|v| *v += 1);
        true
    }

    /// The removal overlays (`NodeDeleting`) the Build facts ended, closed: an overlay is keyed by its
    /// operation (gossip.md: `(build_id, operation)` is the only operation identity; the overlay rides
    /// `TopologyFull.in_flight` while the operation has not completed), so it closes when its
    /// operation's verdict is terminal or its Build is forgotten (`forgotten`). `by_verdict` names each
    /// removal whose operation has a terminal verdict, by the overlay's key, with the verdict's name. A
    /// copy of a closed overlay opens nothing again.
    /// Stops are not removals: their own facts end them.
    pub fn deletions_from_facts(&self, by_verdict: &std::collections::BTreeMap<(String, rdm_core::OperationId), &'static str>, forgotten: &BTreeSet<String>) {
        let mut cleared = Vec::new();
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            let mut ended = self.ended_overlays.lock().unwrap();
            in_flight.retain(|_, o| {
                if o.is_stop() {
                    return true;
                }
                let by = if let Some(by) = by_verdict.get(&o.key()) {
                    *by
                } else if forgotten.contains(&o.build_id) {
                    "build-forgotten"
                } else {
                    return true;
                };
                ended.insert(crate::receive_order::LifecycleIdentity::of(o));
                cleared.push((o.clone(), by));
                false
            });
        }
        if !cleared.is_empty() {
            self.overlays_moved();
            self.births.send_modify(|v| *v += 1);
        }
        for (op, by) in cleared {
            tracing::info_span!(
                "rdm.mesh.topology.update.via-deleting-closed-by-facts",
                node_path = %op.path, node_name = %op.name,
                node_id = %op.node_id,
                incarnation_id = %op.incarnation.as_str(),
                build_id = %op.build_id,
                operation = %op.operation.action, operation_subject = ?op.operation.subject,
                by,
            )
            .in_scope(|| tracing::info!("the Build facts end the removal: its overlay closes and the node is held as its digests say"));
        }
    }

    /// The mesh-primary took node.started of the birth `incarnation` of `node_id`: its
    /// DeclareNodeState(Ready), applied by its StatusAuthority (states.md "Who sets each
    /// state": node Ready is set by "the node's mesh-primary" from "the node's node.started
    /// report, after every start gate"). The birth's status is Ready; its projection says so on
    /// its next publication. Only for a birth of this view's own mesh: a mesh-primary sets its own mesh's
    /// statuses. `false` when nothing moved.
    ///
    /// `name` is the birth's `(path, name)` as the report's decision named it, read only when this view
    /// holds no digest of the birth (a primary born while the member was
    /// silent decides its report from durable rows, Q80): the birth is held ready before its
    /// digest arrives, and that digest, a join's or a heartbeat, finds the status held. A held digest's
    /// name wins.
    pub fn started(&self, node_id: &NodeId, incarnation: &IncarnationId, name: &rdm_core::PathName, mesh: &str) -> bool {
        let held = self.inner.lock().unwrap().get(node_id.as_str()).filter(|(d, _, _)| d.node.incarnation == *incarnation).map(|(d, _, _)| (d.node.location(), d.node.mesh.clone()));
        let (name, mesh) = held.unwrap_or_else(|| (name.clone(), mesh.to_string()));
        let name = Some(name);
        let own_mesh = self.own_mesh.lock().unwrap().clone();
        let Some(name) = name.filter(|_| !own_mesh.is_empty() && mesh == own_mesh) else { return false };
        let prior = self.author_status(node_id.as_str(), incarnation, rdm_core::MemberStatus::Ready);
        tracing::info_span!(
            "rdm.mesh.topology.update.from-node-started",
            node = %self.observer_name.lock().unwrap(),
            member_path = %name.path, member_name = %name.name,
            node_id = %node_id,
            incarnation_id = %incarnation.as_str(),
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
            status = "Ready",
        )
        .in_scope(|| tracing::info!("the mesh-primary took node.started: the birth is ready; its projection says so"));
        true
    }

    /// Accept a stopped birth (`op` is `node.stop:<path>`, naming the birth that stopped): the node
    /// leaves the view and is not routable, and no heartbeat of that birth is taken again
    /// (`stopped_refuses`). Only the mesh-primary's `node.joined` ([`Self::joined`]), its projection
    /// moving past the stop, a later incarnation of the NodeId or a departure ends it. `false` when
    /// already held, when the node deleted, when a later birth of the node is held (the stop is of
    /// an older birth), or when the birth is this view's own holder: a node's own digest view holds itself,
    /// and its own stop is its own state (a start's topology read carries it before `node.joined`).
    pub fn stopped(&self, op: LifecycleOp) -> bool {
        self.stopped_carried(op, None)
    }

    /// [`Self::stopped`], from a `NodeStopped` whose primary publisher changed the subject at
    /// `carried`'s version (`(publisher, topology_version)`): the stop is held as carried there, so a
    /// later projection of that publisher listing the birth moves past it even when no projection
    /// carrying the stop reached this view.
    pub(crate) fn stopped_carried(&self, op: LifecycleOp, carried: Option<(String, u64)>) -> bool {
        if self.is_deleted(op.node_id.as_str()) {
            reject_deleted_overlay(&self.observer_name.lock().unwrap(), &op, "node-stopped");
            return false;
        }
        if self.own.lock().unwrap().as_deref() == Some(op.node_id.as_str()) {
            return false;
        }
        let held_digest = self.get(op.node_id.as_str()).map(|(d, _)| d).or_else(|| self.projected.lock().unwrap().get(op.node_id.as_str()).map(|(d, _)| d.clone()));
        let held = held_digest.as_ref().map(|d| (d.node.incarnation.clone(), d.digest_seq, d.status));
        if held.as_ref().is_some_and(|(inc, _, _)| *inc != op.incarnation) {
            return false;
        }
        let new = {
            let mut held = self.in_flight.lock().unwrap();
            let ended = self.ended_overlays.lock().unwrap().contains(&crate::receive_order::LifecycleIdentity::of(&op));
            if crate::receive_order::admit_overlay("node-stopped", &op, held.get(&op.key()), ended) {
                held.insert(op.key(), op.clone());
                true
            } else {
                // A completed operation must not enrich a later stop's provenance either.
                if ended { return false; }
                false
            }
        };
        if !new {
            if carried.is_some() {
                if let Some(m) = self.stopped_marks.lock().unwrap().get_mut(op.node_id.as_str()) {
                    if m.incarnation == op.incarnation && m.carried.is_none() {
                        m.carried = carried;
                    }
                }
            }
            return false;
        }
        self.overlays_moved();
        let seq = held.as_ref().map(|(_, seq, _)| *seq).unwrap_or(0);
        self.stopped_marks.lock().unwrap().insert(op.node_id.to_string(), StopMark { incarnation: op.incarnation.clone(), held_seq: seq, held: held_digest, latest: None, carried: carried.clone() });
        // A stopped birth's status is the overlay: the status held for it ends with it.
        self.member_statuses.lock().unwrap().remove(op.node_id.as_str());
        self.projected.lock().unwrap().remove(op.node_id.as_str());
        let removed = self.inner.lock().unwrap().remove(op.node_id.as_str()).is_some();
        tracing::info_span!(
            "rdm.mesh.topology.remove.via-node-stopped",
            node_path = %op.path, node_name = %op.name,
            node_id = %op.node_id,
            incarnation_id = %op.incarnation.as_str(),
            build_id = %op.build_id,
            operation = %op.operation.action, operation_subject = ?op.operation.subject,
            was_held = removed,
            held_status = held.as_ref().map(|(_, _, st)| format!("{st:?}")).unwrap_or_default(),
            held_digest_seq = seq,
            observer = %self.observer_name.lock().unwrap(),
            publisher = %carried.as_ref().map(|(p, _)| p.as_str()).unwrap_or(""),
            topology_version = carried.as_ref().map(|(_, v)| *v as i64).unwrap_or(-1),
        )
        .in_scope(|| tracing::info!("the exact birth is stopped: dropped from the view; no heartbeat of it is taken until its mesh-primary's node.joined ends the stop"));
        self.births.send_modify(|v| *v += 1);
        true
    }

    /// Whether `d`, arriving via `via`, is refused because its birth is stopped. Every digest of
    /// the stopped incarnation is refused by name, whatever its status or sequence (gossip.md,
    /// "Node-stopped lifecycle overlay": the stop stays until the next lifecycle op on that exact
    /// birth). A digest of another birth of the node ends the stop and is taken by the rules of any
    /// birth.
    pub(crate) fn stopped_refuses(&self, d: &MeshDigest, via: &'static str, now: Instant) -> bool {
        let held_seq = {
            let mut marks = self.stopped_marks.lock().unwrap();
            let Some(mark) = marks.get_mut(d.node.node_id.as_str()) else { return false };
            if mark.incarnation != d.node.incarnation {
                None
            } else {
                if mark.latest.as_ref().is_none_or(|(l, _, _)| d.digest_seq > l.digest_seq) && d.digest_seq > mark.held_seq {
                    let heard = match via {
                        "heartbeat" => Heard::Direct,
                        "forwarded" | "join" => Heard::Forwarded,
                        _ => Heard::Topology,
                    };
                    mark.latest = Some((d.clone(), now, heard));
                }
                Some(mark.held_seq)
            }
        };
        // The status the digest carried: a projection entry's (`topology`, `forwarded`); a heartbeat
        // and a join digest carry none.
        let carried_status = if matches!(via, "topology" | "forwarded") { format!("{:?}", d.status) } else { String::new() };
        let Some(held_seq) = held_seq else {
            self.end_stop(d.node.node_id.as_str());
            tracing::info_span!("rdm.mesh.topology.update.via-node-started", node_path = %d.node.path, node_name = %d.node.name, node_id = %d.node.node_id, incarnation_id = %d.node.incarnation.as_str(), "type" = %d.node.r#type, node_state = ?d.node_state, digest_seq = d.digest_seq, via)
                .in_scope(|| tracing::info!("a later birth of a stopped node is heard: the stop of the older birth is over"));
            return false;
        };
        tracing::info_span!(
            "rdm.mesh.topology.reject.via-stopped-birth",
            node = %self.observer_name.lock().unwrap(),
            member_path = %d.node.path, member_name = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.as_str(),
            node_state = ?d.node_state,
            entry_status = %carried_status,
            digest_seq = d.digest_seq,
            held_digest_seq = held_seq,
            via,
        )
        .in_scope(|| tracing::info!("a digest of a stopped birth: refused, the stop stands until its mesh-primary's node.joined"));
        true
    }

    /// The mesh-primary took `node.joined` of the birth `op` names on its start (node-start.md step 2:
    /// "node.start clears NodeStopped at node.joined"; states.md: node pending is set by the node's
    /// mesh-primary from the node.joined report): the birth is held `Pending`, and its projection says
    /// so on its next publication. A stop of that exact birth ends first, and the birth is held again from
    /// the latest digest heard of it, under `op`'s name (the row's, which the join canonicalized). With
    /// no stop held the own-mesh birth this view holds of exactly that incarnation is set `Pending` as it
    /// stands. `false` when this view holds no stop of that birth and no digest of it of its own mesh.
    pub fn joined(&self, op: &LifecycleOp) -> bool {
        self.take_joined(op, None)
    }

    /// [`Self::joined`] of a birth whose Pending its mesh-primary recorded when it accepted the
    /// birth's join report (node-mesh-join.md step 10: "Every later report of that join, its retries
    /// and its `NodeJoined` included, belongs to that Pending and starts no other"): `node.mesh.joined`
    /// applies as already applied. A stop of that exact birth ends here all the same ("NodeStopped for
    /// that birth is cleared at `node.mesh.joined`"), and the birth is held again with
    /// `status`, the state its mesh-primary holds for it: Pending, or the Ready it declared
    /// since. Never set back to Pending.
    pub fn joined_already_applied(&self, op: &LifecycleOp, status: rdm_core::MemberStatus) -> bool {
        self.take_joined(op, Some(status))
    }

    /// `node.mesh.joined` of the birth `op` names: held `Pending` ([`Self::joined`]), or with the status
    /// its mesh-primary already holds for it (`already`, [`Self::joined_already_applied`]).
    pub(crate) fn take_joined(&self, op: &LifecycleOp, already: Option<rdm_core::MemberStatus>) -> bool {
        let status = already.unwrap_or(rdm_core::MemberStatus::Pending);
        let mark = self.stopped_marks.lock().unwrap().get(op.node_id.as_str()).cloned().filter(|m| m.incarnation == op.incarnation);
        let cleared: Vec<rdm_core::OperationId> = self.in_flight().into_iter().filter(|o| mark.is_some() && o.is_stop() && o.node_id == op.node_id).map(|o| o.operation).collect();
        let (held_again, held_digest_seq) = match &mark {
            Some(mark) => {
                // The overlay clears before the birth is held again: a projection built in between
                // lists neither, never both.
                self.end_stop(op.node_id.as_str());
                let again = mark.latest.clone().map(|(d, _, _)| d).or(mark.held.clone()).map(|mut d| {
                    { let at = op.location(); d.node.path = at.path; d.node.name = at.name; }
                    d
                });
                if let Some(d) = &again {
                    self.inner.lock().unwrap().insert(op.node_id.to_string(), (d.clone(), Instant::now(), Heard::Direct));
                    self.author_status(op.node_id.as_str(), &op.incarnation, status);
                    self.heard.send_modify(|v| *v += 1);
                    self.births.send_modify(|v| *v += 1);
                    self.digests.send_modify(|v| *v += 1);
                }
                (again.is_some(), again.as_ref().map(|d| d.digest_seq).unwrap_or(0))
            }
            None => {
                let own_mesh = self.own_mesh.lock().unwrap().clone();
                let held = self.inner.lock().unwrap().get(op.node_id.as_str()).filter(|(d, _, _)| d.node.incarnation == op.incarnation && !own_mesh.is_empty() && d.node.mesh == own_mesh).map(|(d, _, _)| d.digest_seq);
                let Some(seq) = held else { return false };
                self.author_status(op.node_id.as_str(), &op.incarnation, status);
                (true, seq)
            }
        };
        tracing::info_span!(
            "rdm.mesh.topology.update.via-node-joined",
            node = %self.observer_name.lock().unwrap(),
            member_path = %op.path, member_name = %op.name,
            node_id = %op.node_id,
            incarnation_id = %op.incarnation.as_str(),
            build_id = %op.build_id,
            operation = %op.operation.action, operation_subject = ?op.operation.subject,
            stops_cleared = ?cleared,
            status = ?status,
            outcome = if already.is_some() { "already-applied" } else { "applied" },
            held_again,
            held_digest_seq,
        )
        .in_scope(|| tracing::info!("the mesh-primary took node.joined: the birth is held with the status its mesh-primary holds, its stop, if one was held, is over; its projection says so"));
        true
    }

    /// The mesh-primary applied its own mesh's member's report of `status` for its birth `incarnation`
    /// of `node_id` (node-status.md "Status report": "A node reports to its mesh's primary, which sets
    /// its status"): held for the birth, so its projection carries it from its next publication. Its
    /// node.started is [`Self::started`]. `false` when the birth is not of this view's own mesh or this
    /// view holds no digest of exactly that birth.
    pub fn reported(&self, node_id: &NodeId, incarnation: &IncarnationId, status: rdm_core::MemberStatus) -> bool {
        let own_mesh = self.own_mesh.lock().unwrap().clone();
        let Some(name) = self.inner.lock().unwrap().get(node_id.as_str()).filter(|(d, _, _)| d.node.incarnation == *incarnation && !own_mesh.is_empty() && d.node.mesh == own_mesh).map(|(d, _, _)| d.node.location()) else {
            return false;
        };
        let prior = self.author_status(node_id.as_str(), incarnation, status);
        tracing::info_span!(
            "rdm.mesh.topology.update.from-status-report",
            node = %self.observer_name.lock().unwrap(),
            member_path = %name.path, member_name = %name.name,
            node_id = %node_id,
            incarnation_id = %incarnation.as_str(),
            status = ?status,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
        )
        .in_scope(|| tracing::info!("the mesh-primary applied its member's status report: the birth's status is set from it; its projection says so"));
        true
    }

    /// The mesh-primary admitted the command `op` (`node.connection.delete`, `stop-node`) on the birth of its own
    /// mesh `op` names, and sets that birth `status` (states.md "Who sets each state": node
    /// connections-deleting at "the admission of `node.connection.delete { scope: All }`; inside
    /// node.stop the visible status is stopping", node stopping at "the stop call's admission"): held
    /// for the birth, so its projection carries it from its next publication. No heartbeat says it (gossip.md
    /// §3.1: a Digest "carries no status"). `false` when the birth is not of this view's own mesh or
    /// this view holds no digest of exactly that birth.
    pub fn admitted(&self, op: &LifecycleOp, status: rdm_core::MemberStatus) -> bool {
        let own_mesh = self.own_mesh.lock().unwrap().clone();
        let held = !own_mesh.is_empty() && op.mesh == own_mesh && self.inner.lock().unwrap().get(op.node_id.as_str()).is_some_and(|(d, _, _)| d.node.incarnation == op.incarnation);
        if !held {
            return false;
        }
        let prior = self.author_status(op.node_id.as_str(), &op.incarnation, status);
        tracing::info_span!(
            "rdm.mesh.topology.update.from-command-admitted",
            node = %self.observer_name.lock().unwrap(),
            member_path = %op.path, member_name = %op.name,
            node_id = %op.node_id,
            incarnation_id = %op.incarnation.as_str(),
            build_id = %op.build_id,
            operation = %op.operation.action, operation_subject = ?op.operation.subject,
            status = ?status,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
        )
        .in_scope(|| tracing::info!("the mesh-primary admitted the command: the birth's status is set from it; its projection says so"));
        true
    }

    /// The status this view holds for the own-mesh birth `incarnation` of `node_id`: what its
    /// mesh-primary set, here or in the projection this view took. `None` when it holds none for that
    /// exact birth.
    pub fn member_status(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<rdm_core::MemberStatus> {
        self.member_statuses.lock().unwrap().get(node_id.as_str()).filter(|(inc, _)| inc == incarnation).map(|(_, s)| *s)
    }

    /// This view's holder took its mesh's primary role and adopts `status`, the state its mesh's
    /// primary set for the own-mesh birth `incarnation` of `node_id` at `name`, as the primary's status
    /// rows hold it (node-status-heartbeat-connections.md §A.3: "the successor acquires the held member
    /// statuses and publishes a complete projection under its own revision before any delta";
    /// fabric-authority.md §7: a takeover hydrates the lifecycle state that exists, it is not a
    /// transition). Held for the birth whether or not its heartbeat is held yet: the projection carries
    /// it from its next publication and a heartbeat heard later takes it. Nothing else of the birth moves and
    /// nothing else ends. `Err` names why nothing is adopted: a birth of another mesh, this view's own
    /// birth or a predecessor at its own (path, name), a deleted or stopped birth, or another birth of
    /// the node this view holds. `Ok` carries
    /// the status held before.
    pub fn adopt_status(&self, node_id: &NodeId, incarnation: &IncarnationId, mesh: &str, status: rdm_core::MemberStatus) -> Result<Option<rdm_core::MemberStatus>, &'static str> {
        let own_mesh = self.own_mesh.lock().unwrap().clone();
        if own_mesh.is_empty() || mesh != own_mesh {
            return Err("another-mesh");
        }
        if self.own.lock().unwrap().as_deref() == Some(node_id.as_str()) {
            return Err("own-birth");
        }
        if self.own_predecessor.lock().unwrap().as_ref() == Some(incarnation) {
            return Err("own-predecessor-birth");
        }
        if self.is_deleted(node_id.as_str()) {
            return Err("deleted");
        }
        if self.stopped_marks.lock().unwrap().get(node_id.as_str()).is_some_and(|m| m.incarnation == *incarnation) {
            return Err("stopped");
        }
        let other_birth = self.inner.lock().unwrap().get(node_id.as_str()).is_some_and(|(d, _, _)| d.node.incarnation != *incarnation)
            || self.member_statuses.lock().unwrap().get(node_id.as_str()).is_some_and(|(inc, _)| inc != incarnation);
        if other_birth {
            return Err("another-birth-held");
        }
        Ok(self.author_status(node_id.as_str(), incarnation, status))
    }

    /// Set `status` for the birth `incarnation` of `node_id`, as its mesh-primary sets it (states.md
    /// "Who sets each state"): held for the birth, its one copy, which its projection and every read
    /// of its digest carry. A move ticks `heard`. Returns the status held for that birth before
    /// (`None` when none was).
    pub(crate) fn author_status(&self, node_id: &str, incarnation: &IncarnationId, status: rdm_core::MemberStatus) -> Option<rdm_core::MemberStatus> {
        let prior = self.member_statuses.lock().unwrap().insert(node_id.to_string(), (incarnation.clone(), status)).filter(|(inc, _)| inc == incarnation).map(|(_, s)| s);
        if prior != Some(status) {
            self.heard.send_modify(|v| *v += 1);
        }
        prior
    }

    /// Whether the birth `d` names is stopped in this view (the exact incarnation).
    pub(crate) fn is_stopped(&self, d: &MeshDigest) -> bool {
        self.stopped_marks.lock().unwrap().get(d.node.node_id.as_str()).is_some_and(|m| m.incarnation == d.node.incarnation)
    }

    /// A projection frame applied: every stopped birth whose stop the source carried, before the
    /// frame at the version held or after it at the frame's version, is marked carried at the highest
    /// such version of that publisher.
    pub(crate) fn note_stops_carried(&self, target: &Target) {
        let mut marks = self.stopped_marks.lock().unwrap();
        let mut note = |key: &(String, String), publisher: &str, version: u64| {
            if let Some(m) = marks.get_mut(&key.0).filter(|m| m.incarnation.as_str() == key.1) {
                if m.carried.as_ref().is_none_or(|(p, v)| p != publisher || version > *v) {
                    m.carried = Some((publisher.to_string(), version));
                }
            }
        };
        if let Some((publisher, version)) = &target.held {
            for key in &target.prior_stops {
                note(key, publisher, *version);
            }
        }
        for key in &target.post_stops {
            note(key, &target.publisher, target.version);
        }
    }

    /// The `(publisher, version)` at which a projection this view applied carried the stop of `d`.
    pub(crate) fn stop_carried_at(&self, d: &MeshDigest) -> Option<(String, u64)> {
        self.stopped_marks.lock().unwrap().get(d.node.node_id.as_str()).filter(|m| m.incarnation == d.node.incarnation).and_then(|m| m.carried.clone())
    }

    /// What the projection frame `target` describes does to the stop of the birth `d` lists: `None`
    /// when this view holds no stop of that exact birth. The projection moved, in order, past the stop
    /// when a projection of the source this view applied carried the stop at some version, this frame
    /// is newer than that version, and the projection after it no longer carries the stop; then the
    /// stop ends here (`Some(true)`, [`Self::stop_ended_in_projection`]). A refresh at the version that
    /// carried it, or a projection that never carried it here, is refused by name (`Some(false)`). A
    /// new publisher's projection is the next revision of the source (receivers install its full first):
    /// the receive path took it as newer than every publication of the mesh it held (R-RX3).
    /// Decided and taken under one hold of the stop marks: a node's backbone and mesh channel each
    /// apply their projections under their own receiver, and the stop ends on one of them, once.
    pub(crate) fn stopped_entry(&self, d: &MeshDigest, target: &Target) -> Option<bool> {
        let key = (d.node.node_id.to_string(), d.node.incarnation.to_string());
        let mark = {
            let mut marks = self.stopped_marks.lock().unwrap();
            let held = marks.get(d.node.node_id.as_str()).filter(|m| m.incarnation == d.node.incarnation)?;
            let moved = held.carried.as_ref().is_some_and(|(publisher, version)| *publisher != target.publisher || target.version > *version) && !target.post_stops.contains(&key);
            if !moved {
                drop(marks);
                self.refuse_in_projection(d, target);
                return Some(false);
            }
            marks.remove(d.node.node_id.as_str()).expect("the mark is held under this hold")
        };
        self.stop_ended_in_projection(d, target, mark);
        Some(true)
    }

    /// The mesh-primary's projection of the stopped birth `d`'s mesh moved, in order, from a version
    /// carrying its stop to `version` listing it: its primary took `node.joined`, and the stop ends
    /// here. An own-mesh birth's projection entry gives only its status (its liveness, endpoints and
    /// runtime come from its own Digest), so the birth is held again, as `node.joined` holds it at
    /// the primary, from the latest digest of it this view refused while the stop stood: with the
    /// projection's status, and the instant and channel that digest was heard on (the projection
    /// refreshes no liveness). With none refused, its next heartbeat holds it.
    pub(crate) fn stop_ended_in_projection(&self, d: &MeshDigest, target: &Target, mark: StopMark) {
        let cleared: Vec<rdm_core::OperationId> = self.in_flight().into_iter().filter(|o| o.is_stop() && o.node_id == d.node.node_id).map(|o| o.operation).collect();
        let carried = mark.carried.as_ref().map(|(_, v)| *v as i64).unwrap_or(-1);
        let latest = mark.latest;
        self.end_stop(d.node.node_id.as_str());
        let held_again = match latest {
            // Its status is the projection's: the caller takes it next ([`Self::take_projected_status`]),
            // into the one copy, and names the move there.
            Some((again, at, heard)) if self.own_mesh_member(d) => {
                let seq = again.digest_seq;
                self.inner.lock().unwrap().insert(d.node.node_id.to_string(), (again, at, heard));
                self.heard.send_modify(|v| *v += 1);
                self.births.send_modify(|v| *v += 1);
                self.digests.send_modify(|v| *v += 1);
                Some(seq)
            }
            _ => None,
        };
        tracing::info_span!(
            "rdm.mesh.topology.update.via-stop-ended-in-projection",
            node = %self.observer_name.lock().unwrap(),
            member_path = %d.node.path, member_name = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.as_str(),
            stops_cleared = ?cleared,
            publisher = %target.publisher,
            channel = target.channel,
            held_version = target.held_version(),
            stop_carried_at = carried,
            topology_version = target.version,
            status = ?d.status,
            held_again = held_again.is_some(),
            held_digest_seq = held_again.unwrap_or(0),
        )
        .in_scope(|| tracing::info!("the mesh-primary's projection moved from the version carrying the stop to one listing the birth: the stop is over"));
    }

    /// A projection entry of the stopped birth `d` from a version that never carried its stop here:
    /// published before the stop, or a refresh at the version held. Refused by name; the stop stands.
    pub(crate) fn refuse_in_projection(&self, d: &MeshDigest, target: &Target) {
        tracing::info_span!(
            "rdm.mesh.topology.reject.via-stopped-birth",
            node = %self.observer_name.lock().unwrap(),
            member_path = %d.node.path, member_name = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.as_str(),
            status = ?d.status,
            digest_seq = d.digest_seq,
            via = "projection",
            publisher = %target.publisher,
            channel = target.channel,
            held_version = target.held_version(),
            topology_version = target.version,
            stop_carried_at = self.stop_carried_at(d).map(|(_, v)| v as i64).unwrap_or(-1),
        )
        .in_scope(|| tracing::info!("a projection entry of a stopped birth from a version that never carried its stop here: refused, the stop stands"));
    }

    /// Whether `d` is a birth of this view's own mesh other than its holder: its status comes from
    /// that mesh's primary (gossip.md §3.1 `TopologyFull`).
    /// The holder is known by its node id from the moment it joins (the id it joined under), and by its
    /// own published birth once it publishes.
    pub(crate) fn is_own_lineage(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.own.lock().unwrap().as_deref() == Some(node_id.as_str())
            || *self.observer_name.lock().unwrap() == node_id.as_str()
            || self.own_predecessor.lock().unwrap().as_ref() == Some(incarnation)
    }

    pub(crate) fn own_mesh_member(&self, d: &MeshDigest) -> bool {
        let own_mesh = self.own_mesh.lock().unwrap();
        let itself = self.is_own_lineage(&d.node.node_id, &d.node.incarnation);
        !own_mesh.is_empty() && d.node.mesh == *own_mesh && !itself
    }

    /// `ds` as every read hands them out: each birth with the primary records this view holds for it
    /// ([`Self::recorded_primaries`]), and an own-mesh birth ([`Self::own_mesh_member`]) with the one
    /// status this view holds for it ([`Self::projected_status`]), whatever its held digest says. A
    /// mesh-primary's `TopologyFull` projection is such a read, so it carries the recorded primaries (gossip.md
    /// "A primary change on gossip"). Called with no hold of `inner`.
    pub(crate) fn hand_out<'a>(&self, ds: impl IntoIterator<Item = &'a mut MeshDigest>) {
        let own_mesh = self.own_mesh.lock().unwrap().clone();
        let statuses = self.member_statuses.lock().unwrap();
        for d in ds {
            self.recorded_primaries(d);
            if own_mesh.is_empty() {
                continue;
            }
            let itself = self.is_own_lineage(&d.node.node_id, &d.node.incarnation);
            if d.node.mesh == own_mesh && !itself {
                d.status = statuses.get(d.node.node_id.as_str()).filter(|(inc, _)| *inc == d.node.incarnation).map(|(_, s)| *s).unwrap_or(rdm_core::MemberStatus::Pending);
            }
        }
    }

    /// The mesh-primary's projection lists the own-mesh birth `d` with its status (gossip.md §3.1:
    /// own-mesh members receive statuses through the primary's versioned member
    /// projection): the status is held for the birth, and the held digest, liveness, endpoints and
    /// runtime as its own heartbeat said, takes that status. Nothing else of `d` is taken. `false`
    /// when nothing moved.
    pub(crate) fn take_projected_status(&self, d: &MeshDigest, target: &Target) -> bool {
        // This node's own birth is its own word: no projection moves the status it holds of itself.
        if !self.own_mesh_member(d) || self.is_deleted(d.node.node_id.as_str()) || self.is_stopped(d) {
            return false;
        }
        // The birth itself, as the projection carries it, when this view holds no digest of it.
        self.hold_projected_at(d, Instant::now());
        let prior = {
            let mut statuses = self.member_statuses.lock().unwrap();
            let prior = statuses.get(d.node.node_id.as_str()).filter(|(inc, _)| *inc == d.node.incarnation).map(|(_, s)| *s);
            if prior == Some(d.status) {
                return false;
            }
            statuses.insert(d.node.node_id.to_string(), (d.node.incarnation.clone(), d.status));
            prior
        };
        let held = self.inner.lock().unwrap().get(d.node.node_id.as_str()).is_some_and(|(h, _, _)| h.node.incarnation == d.node.incarnation);
        // The status moved for a birth held by its own heartbeat or held only from this projection
        // ([`Self::projected_at`]): the view lists either with it.
        self.heard.send_modify(|v| *v += 1);
        tracing::info_span!(
            "rdm.mesh.topology.update.via-projected-status",
            node = %self.observer_name.lock().unwrap(),
            member_path = %d.node.path, member_name = %d.node.name,
            member_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.as_str(),
            status = ?d.status,
            prior = prior.map(|p| format!("{p:?}")).unwrap_or_default(),
            heartbeat_held = held,
            publisher = %target.publisher,
            channel = target.channel,
            topology_version = target.version,
        )
        .in_scope(|| tracing::info!("an own-mesh member's status taken from its mesh-primary's projection; its liveness stays its own heartbeat's"));
        true
    }

    /// Whether this view's overlay holds the exact birth `incarnation` of `node_id` stopped (its stop mark).
    pub fn holds_stopped(&self, node_id: &str, incarnation: &IncarnationId) -> bool {
        self.stopped_marks.lock().unwrap().get(node_id).is_some_and(|m| &m.incarnation == incarnation)
    }

    /// The last digest this view held of each stopped birth: a node-admin keeps showing the birth,
    /// `Leaving`, in its view though the digest view dropped it. A birth this digest view never held a digest of
    /// (a late joiner) is not listed.
    pub fn stopped_digests(&self) -> Vec<MeshDigest> {
        self.stopped_marks.lock().unwrap().values().filter_map(|m| m.held.clone()).collect()
    }

    /// Hold the own-mesh birth `d` its mesh-primary's projection lists, received at `now`, when this
    /// view holds no digest of it (`DigestView::projected`): first held at `now`, or kept as first
    /// held when this exact birth is held already (a projection is no liveness); a later birth of
    /// the node replaces an earlier one, first held at `now`.
    pub fn hold_projected_at(&self, d: &MeshDigest, now: Instant) {
        if self.inner.lock().unwrap().contains_key(d.node.node_id.as_str()) {
            return;
        }
        let inserted = {
            let mut projected = self.projected.lock().unwrap();
            match projected.get_mut(d.node.node_id.as_str()) {
                Some((held, _)) if held.node.incarnation == d.node.incarnation => false,
                Some((held, _)) if held.node.supersedes.as_ref() == Some(&d.node.incarnation) => false,
                _ => {
                    projected.insert(d.node.node_id.to_string(), (d.clone(), now));
                    true
                }
            }
        };
        if inserted {
            self.births.send_modify(|v| *v += 1);
        }
    }

    /// The own-mesh birth of `node_id` this view holds only from its mesh-primary's projection, and
    /// how long it has gone unheard at `now`: since this view first held it. `None` once the
    /// member's own digest is held, or the node deleted or stopped.
    pub fn projected_at(&self, node_id: &str, now: Instant) -> Option<(MeshDigest, Duration)> {
        let heard = self.inner.lock().unwrap().contains_key(node_id);
        let stopped = self.stopped_marks.lock().unwrap().contains_key(node_id);
        if heard || stopped || self.is_deleted_at(node_id, now) {
            return None;
        }
        let mut held = self.projected.lock().unwrap().get(node_id).map(|(d, at)| (d.clone(), now.saturating_duration_since(*at)));
        self.hand_out(held.iter_mut().map(|(d, _)| d));
        held
    }

    /// Every own-mesh birth this view holds only from its mesh-primary's projection
    /// ([`DigestView::projected_at`]), each with how long it has gone unheard at `now`.
    pub fn projected_unheard_at(&self, now: Instant) -> Vec<(MeshDigest, Duration)> {
        self.woken();
        let ids: Vec<String> = self.projected.lock().unwrap().keys().cloned().collect();
        ids.iter().filter_map(|id| self.projected_at(id, now)).collect()
    }

    /// The open stop overlays of stopped births this view never held a digest of (an admin that
    /// joined after the stop and read the overlay from the mesh topology): the only facts it has of
    /// them are the overlay's own, the node's path, id and incarnation.
    pub fn stopped_unheard(&self) -> Vec<LifecycleOp> {
        let marks = self.stopped_marks.lock().unwrap();
        self.in_flight
            .lock()
            .unwrap()
            .values()
            .filter(|op| op.is_stop() && marks.get(op.node_id.as_str()).is_some_and(|m| m.incarnation == op.incarnation && m.held.is_none()))
            .cloned()
            .collect()
    }

    /// The stop of `node_id` is over (its mesh-primary took node.joined, a later birth was heard, or
    /// it deleted): its overlay and mark clear.
    pub(crate) fn end_stop(&self, node_id: &str) {
        let unmarked = self.stopped_marks.lock().unwrap().remove(node_id).is_some();
        let cleared = {
            let mut in_flight = self.in_flight.lock().unwrap();
            let mut ended = self.ended_overlays.lock().unwrap();
            let before = in_flight.len();
            in_flight.retain(|_, o| {
                let gone = o.is_stop() && o.node_id.as_str() == node_id;
                if gone {
                    ended.insert(crate::receive_order::LifecycleIdentity::of(o));
                }
                !gone
            });
            in_flight.len() != before
        };
        // The view lists a stopped birth from its mark ([`Self::stopped_digests`]) and its overlay
        // ([`Self::stopped_unheard`]): either one gone moves the overlay watch.
        if cleared || unmarked {
            self.overlays_moved();
        }
    }

    /// An overlay was added or cleared, or a departure accepted: the overlay watch ticks.
    pub(crate) fn overlays_moved(&self) {
        self.overlays.send_modify(|v| *v += 1);
    }

    /// Ticks each time an open lifecycle overlay is added or cleared, or a departure is accepted:
    /// what a mesh primary's publication wakes on when its mesh topology's overlays moved.
    pub fn overlay_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.overlays.subscribe()
    }

    /// Accept an open lifecycle overlay: the node stays held and stops being
    /// routable. `false` when the overlay was already held or the node has
    /// already deleted.
    pub fn deleting(&self, op: LifecycleOp) -> bool {
        if self.is_deleted(op.node_id.as_str()) {
            return false;
        }
        // Applied once however many copies arrive (gossip.md `NodeDeleting`): a copy of an overlay
        // the Build facts ended opens nothing again.
        let new = {
            let mut held = self.in_flight.lock().unwrap();
            let ended = self.ended_overlays.lock().unwrap().contains(&crate::receive_order::LifecycleIdentity::of(&op));
            if !crate::receive_order::admit_overlay("node-deleting", &op, held.get(&op.key()), ended) { return false; }
            held.insert(op.key(), op.clone()).is_none()
        };
        if new {
            self.overlays_moved();
            // What this receiver holds of the node as the overlay opens: found, in what status, and
            // not routable (the overlay is what makes it so).
            let held_status = self.get(op.node_id.as_str()).map(|(d, _)| format!("{:?}", d.status));
            tracing::info_span!(
                "rdm.mesh.topology.update.via-node-deleting",
                node_path = %op.path, node_name = %op.name,
                node_id = %op.node_id,
                build_id = %op.build_id,
                operation = %op.operation.action, operation_subject = ?op.operation.subject,
                held = held_status.is_some(),
                held_status = held_status.as_deref().unwrap_or("not held"),
                routable = self.routable(op.node_id.as_str()),
            )
            .in_scope(|| tracing::info!("the node is being removed: held, not routable"));
            self.births.send_modify(|v| *v += 1);
        }
        new
    }

    /// Hold the mesh names the accepted Build names: the authority on which meshes the fabric has.
    /// A change of them moves the accepted watch ([`RoutingWatch`]).
    pub fn set_accepted_meshes(&self, meshes: BTreeSet<String>) {
        let changed = self.accepted_meshes.lock().unwrap().replace(meshes.clone()).as_ref() != Some(&meshes);
        if changed {
            self.accepted.send_modify(|v| *v += 1);
        }
    }

    /// Whether the accepted Build names `mesh`; `None` while this node holds no Build.
    pub fn accepted_has_mesh(&self, mesh: &str) -> Option<bool> {
        self.accepted_meshes.lock().unwrap().as_ref().map(|m| m.contains(mesh))
    }

    /// The open overlays this view holds.
    pub fn in_flight(&self) -> Vec<LifecycleOp> {
        let mut v: Vec<LifecycleOp> = self.in_flight.lock().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.key().cmp(&b.key()));
        v
    }

    /// The departures this view holds within the retention.
    pub fn deleted(&self) -> Vec<LifecycleOp> {
        self.expire_deleted();
        let mut v: Vec<LifecycleOp> = self.deleted.lock().unwrap().values().map(|(op, _)| op.clone()).collect();
        v.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        v
    }

    /// The open overlays and retained departures of `mesh`'s own nodes, and the retained deletions this
    /// process executed in any mesh: what that mesh's primary publishes for it (gossip.md §2 rules:
    /// "lifecycle events are published by the executor on its own mesh channel and reach the backbone
    /// only inside Members.in_flight and Members.deleted").
    pub fn overlays_of(&self, mesh: &str) -> (Vec<LifecycleOp>, Vec<LifecycleOp>) {
        let deleted = self.deleted();
        let executed = {
            let mut executed = self.executed_deletions.lock().unwrap();
            executed.retain(|id| deleted.iter().any(|op| op.node_id.as_str() == id));
            executed.clone()
        };
        let in_flight = self.in_flight().into_iter().filter(|op| op.mesh == mesh).collect();
        let deleted = deleted.into_iter().filter(|op| op.mesh == mesh || executed.contains(op.node_id.as_str())).collect();
        (in_flight, deleted)
    }

    /// Hold `op` as a deletion this process executed: its `NodeDeleted` is this process's to carry in
    /// its mesh's topology ([`Self::overlays_of`]).
    pub fn note_executed_deletion(&self, op: &LifecycleOp) {
        self.executed_deletions.lock().unwrap().insert(op.node_id.to_string());
    }

    /// May application routing select this node (states.md "Routing"): only while the birth is
    /// ready and its mesh is ready, as this view holds them. Anything else is
    /// a routing stop from the moment this view holds the fact that says so: no held digest, a
    /// departure, an open lifecycle overlay (NodeStopped, NodeDeleting), a held
    /// status other than ready, or a mesh status not held ready. A stop is
    /// never a barrier: the same birth's later ready, or its mesh's, restores selection.
    /// The resolver and the Node RPC client never read this; only application routing does.
    pub fn routable(&self, node_id: &str) -> bool {
        if self.is_deleted(node_id) || self.in_flight.lock().unwrap().values().any(|op| op.node_id.as_str() == node_id) {
            return false;
        }
        let Some(mesh) = self.get(node_id).filter(|(d, _)| d.status == rdm_core::MemberStatus::Ready).map(|(d, _)| d.node.mesh) else {
            return false;
        };
        self.statuses.mesh(&mesh).is_some_and(|m| m.status == MESH_READY)
    }

    /// Hold `d` as its member's latest word, unless it is older than what is
    /// held: a digest of the same birth whose `digest_seq` is no higher than the
    /// held one's (the birth's own heartbeat order; Rafka-time stamps never
    /// decide it), or a digest of the birth the held one supersedes. Gossip can deliver
    /// a digest late; a late one never refreshes a silent member or reverts
    /// its status. A digest of a deleted node is refused: the departure
    /// stands for the retention, whatever incarnation the digest names.
    /// `false` when `d` was not taken.
    pub fn record(&self, d: MeshDigest) -> bool {
        self.record_at(d, Instant::now())
    }

    /// [`Self::record`], heard at `now`. `d` is a heartbeat: the member's liveness, endpoints and
    /// runtime fact. A heartbeat of a stopped birth is refused by name. For a birth of this view's own
    /// mesh, its status is the mesh-primary's (gossip.md §3.1: a Digest "carries no status: a member's
    /// status comes only from its mesh-primary"): the held status is the one this view holds for the
    /// birth (`Self::projected_status`), which the primary sets from node.joined, node.started and
    /// the commands it admits, and every other member takes from its primary's projection. No
    /// heartbeat moves it, at the primary or anywhere.
    pub fn record_at(&self, d: MeshDigest, now: Instant) -> bool {
        let node = self.observer_name.lock().unwrap().clone();
        self.record_with(d, now, &node, "local")
    }

    /// The shared heartbeat installation, with receipt context for its outcome spans.
    pub(crate) fn record_with(&self, d: MeshDigest, now: Instant, node: &str, channel: &str) -> bool {
        if self.is_deleted_at(d.node.node_id.as_str(), now) {
            reject_deleted(&d, "mesh-channel");
            // Its own word on its mesh's channel, emitted after its NodeDeleted: the deleted birth runs (a
            // digest it sent before it exited, delivered late, is not). The node that refused it reports it
            // to the fabric-primary, which terminates its runtime (node-investigation.md, Q110, Q128).
            let after_deletion = self.deleted.lock().unwrap().get(d.node.node_id.as_str()).is_some_and(|(op, _)| op.incarnation == d.node.incarnation && d.emitted_at_rafka_ms > op.event_at_rafka_ms);
            if after_deletion {
                self.returned.lock().unwrap().entry((d.node.node_id.to_string(), d.node.incarnation.to_string())).or_insert_with(|| d.clone());
            }
            return false;
        }
        if self.refuses_recovering(&d, "mesh-channel") {
            return false;
        }
        if self.stopped_refuses(&d, "heartbeat", now) {
            return false;
        }
        // An own-mesh birth's status is not its heartbeat's: it is the one this view holds for it, and
        // no move of it is a move of the digest.
        let own_mesh = self.own_mesh_member(&d);

        let mut inner = self.inner.lock().unwrap();
        if let Some((held, _, _)) = inner.get(d.node.node_id.as_str()) {
            if !crate::receive_order::admit_digest(held, &d, node, channel) {
                return false;
            }
            if runtime_changed(held, &d) || (held.node.incarnation != d.node.incarnation && held.node.supersedes.as_ref() == Some(&d.node.incarnation)) {
                return false;
            }
        }
        let birth_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node);
        let heard_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, at, _)| held.node != d.node || (!own_mesh && held.status != d.status) || now.saturating_duration_since(*at) > self.staleness_floor);
        self.take_primaries(&d);
        inner.insert(d.node.node_id.to_string(), (d, now, Heard::Direct));
        drop(inner);
        if heard_changed {
            self.heard.send_modify(|v| *v += 1);
        }
        if birth_changed {
            self.births.send_modify(|v| *v += 1);
        }
        self.digests.send_modify(|v| *v += 1);
        true
    }

    /// `d` with the primary records this view holds for its exact birth: its node type's primary of its
    /// mesh at that record's revision, the fabric-primary at that record's, none it does not hold.
    pub fn recorded_primaries(&self, d: &mut MeshDigest) {
        let n = &d.node;
        d.is_primary = self.primaries.node_type_primary(&n.mesh, &n.r#type).filter(|h| h.is_birth(&n.node_id, &n.incarnation)).map(|h| h.revision);
        d.is_fabric_primary = self.primaries.fabric().filter(|h| h.is_birth(&n.node_id, &n.incarnation)).map(|h| h.revision);
    }

    /// Hold the primary records `d` carries (gossip.md "A primary change on gossip"): its birth as its
    /// node type's primary of its mesh at `is_primary`, as the fabric-primary at
    /// `is_fabric_primary`. A record is held only when it supersedes the one held (a later
    /// revision); who is primary is the election's, from what the view holds.
    pub(crate) fn take_primaries(&self, d: &MeshDigest) {
        let n = &d.node;
        let record = |revision| Primary { mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation.clone(), revision };
        if let Some(revision) = d.is_primary {
            self.primaries.take(PrimaryRole::of_type(&n.r#type), &record(revision));
        }
        if let Some(revision) = d.is_fabric_primary {
            self.primaries.take(PrimaryRole::FabricPrimary, &record(revision));
        }
    }


    /// Hold `d` as forwarded by its mesh's primary, which only forwards a
    /// member it hears: an equal copy of the held digest keeps the member
    /// heard (the primary's word that it still is). Otherwise as [`Self::record`].
    pub(crate) fn record_forwarded(&self, d: MeshDigest) -> bool {
        self.record_forwarded_at(d, Instant::now())
    }

    /// Hold `d` as topology forwarded onto the mesh channel (a peer Mesh's projection, loads
    /// omitted, or an entry answer). A forwarded word is not liveness: it never refreshes when a
    /// held member was heard, and a member held only from forwards never ages to silent. A
    /// member already heard keeps its instant and how it was heard; only its digest moves on.
    pub(crate) fn record_topology(&self, d: MeshDigest) -> bool {
        self.record_topology_at(d, Instant::now())
    }

    /// `Self::record_topology`, received at `now`.
    pub fn record_topology_at(&self, d: MeshDigest, now: Instant) -> bool {
        if self.is_deleted_at(d.node.node_id.as_str(), now) {
            reject_deleted(&d, "forwarded");
            return false;
        }
        if self.refuses_recovering(&d, "forwarded") {
            return false;
        }
        if self.stopped_refuses(&d, "topology", now) {
            return false;
        }
        let own_mesh = self.own_mesh_member(&d);
        let mut inner = self.inner.lock().unwrap();
        let mut held_entry = None;
        if let Some((held, at, heard)) = inner.get(d.node.node_id.as_str()) {
            if runtime_changed(held, &d) {
                return false;
            }
            let older = if held.node.incarnation == d.node.incarnation {
                d.digest_seq < held.digest_seq
            } else {
                held.node.supersedes.as_ref() == Some(&d.node.incarnation)
            };
            if older {
                return false;
            }
            held_entry = Some((*at, *heard));
        }
        let birth_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node);
        let heard_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node || (!own_mesh && held.status != d.status));
        let (at, heard) = held_entry.unwrap_or((now, Heard::Topology));
        self.take_primaries(&d);
        inner.insert(d.node.node_id.to_string(), (d.clone(), at, heard));
        drop(inner);
        if heard_changed {
            self.heard.send_modify(|v| *v += 1);
        }
        if birth_changed {
            self.births.send_modify(|v| *v += 1);
        }
        self.digests.send_modify(|v| *v += 1);
        true
    }

    /// `Self::record_forwarded`, heard at `now`.
    pub fn record_forwarded_at(&self, d: MeshDigest, now: Instant) -> bool {
        self.record_forwarded_inner(d, now, Carried::Status)
    }

    /// Hold the digest a birth presented at its join (`JoinNode`, at the admin that deployed it or
    /// commanded its start), as forwarded: its address and liveness, and no status, for a join digest
    /// carries none. An own-mesh birth keeps the status this view holds for it, and the join digest
    /// authors none: its Pending is its mesh-primary's, recorded once when it accepts the join report
    /// (node-mesh-join.md step 10), through its status authority ([`Self::reported`]).
    pub fn record_joined(&self, d: MeshDigest) -> bool {
        self.record_forwarded_inner(d, Instant::now(), Carried::NoStatus)
    }

    pub(crate) fn record_forwarded_inner(&self, d: MeshDigest, now: Instant, carried: Carried) -> bool {
        if self.is_deleted_at(d.node.node_id.as_str(), now) {
            reject_deleted(&d, "forwarded");
            return false;
        }
        if self.refuses_recovering(&d, if carried == Carried::Status { "forwarded" } else { "join" }) {
            return false;
        }
        if self.stopped_refuses(&d, if carried == Carried::Status { "forwarded" } else { "join" }, now) {
            return false;
        }
        let own_mesh = self.own_mesh_member(&d);

        let mut inner = self.inner.lock().unwrap();
        if let Some((held, _, _)) = inner.get(d.node.node_id.as_str()) {
            if runtime_changed(held, &d) {
                return false;
            }
            let older = if held.node.incarnation == d.node.incarnation {
                d.digest_seq < held.digest_seq
            } else {
                held.node.supersedes.as_ref() == Some(&d.node.incarnation)
            };
            if older {
                return false;
            }
        }
        let birth_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, _, _)| held.node != d.node);
        // A forwarded copy is its forwarder's word that the member is heard (it forwards only what it
        // hears): a birth held silent past the staleness floor is heard again here, as on its own digest.
        let mut heard_changed = inner.get(d.node.node_id.as_str()).is_none_or(|(held, at, heard)| held.node != d.node || (!own_mesh && held.status != d.status) || unheard(at, *heard, now) > self.staleness_floor);
        self.take_primaries(&d);
        inner.insert(d.node.node_id.to_string(), (d.clone(), now, Heard::Forwarded));
        drop(inner);
        // An own-mesh birth held from what its mesh-primary serves (a topology read, each entry its
        // status): that status is held. A join digest carries none: the held status stands, and the
        // birth's Pending is its mesh-primary's, recorded when it accepted the join report
        // (node-mesh-join.md step 10), never this digest's.
        if own_mesh && carried == Carried::Status {
            let prior = self.member_statuses.lock().unwrap().insert(d.node.node_id.to_string(), (d.node.incarnation.clone(), d.status)).filter(|(inc, _)| *inc == d.node.incarnation).map(|(_, s)| s);
            heard_changed |= prior != Some(d.status);
        }
        if heard_changed {
            self.heard.send_modify(|v| *v += 1);
        }
        if birth_changed {
            self.births.send_modify(|v| *v += 1);
        }
        self.digests.send_modify(|v| *v += 1);
        true
    }

    /// Ticks each time a member's birth is first held or changes (a new
    /// incarnation, a moved address, a runtime published): what a process's
    /// live resolver is fed from. A fresher digest of the same birth does not.
    pub fn birth_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.births.subscribe()
    }

    /// The routing facts this view holds, watched ([`RoutingWatch`]).
    pub fn routing_watch(&self) -> RoutingWatch {
        RoutingWatch {
            watches: vec![
                ("heard", self.heard.subscribe()),
                ("births", self.births.subscribe()),
                ("overlays", self.overlays.subscribe()),
                ("meshes", self.statuses.mesh_changes()),
                ("silence", self.silence.subscribe()),
                ("accepted", self.accepted.subscribe()),
            ],
        }
    }

    /// Ticks each time a member is first held, changes its birth or its status, or is heard again
    /// after it had been silent past the staleness floor. The value is a count: a waiter that
    /// recorded it before an event sees the count move after the event, however the two
    /// interleave with its subscribing.
    pub fn heard_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.heard.subscribe()
    }

    /// Ticks each time a digest is taken (any of [`Self::record`], a forwarded or a topology copy
    /// that moved the held digest). The value is a count, as [`Self::heard_changes`].
    pub fn digest_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.digests.subscribe()
    }

    /// The digests heard within `fresh`.
    pub fn current(&self, fresh: Duration) -> Vec<MeshDigest> {
        self.current_at(fresh, Instant::now())
    }

    /// Digests heard within `fresh` of `now`, whatever status each holds: a member held `Leaving`
    /// is current like any member (gossip.md §6.1: a stopped status by itself and silence are not
    /// departure proof), and every mesh topology carries it as held.
    pub(crate) fn current_at(&self, fresh: Duration, now: Instant) -> Vec<MeshDigest> {
        self.woken();
        let mut ds: Vec<MeshDigest> = self.inner.lock().unwrap().values().filter(|(_, at, fw)| unheard(at, *fw, now) <= fresh).map(|(d, _, _)| d.clone()).collect();
        self.hand_out(ds.iter_mut());
        ds
    }

    /// The digests this node received straight from their members (their own word, on the channel
    /// they publish to) at or after `since`: the members that checked in since a checklist began. A
    /// member known only from forwarded topology, or last heard before `since`, is not here.
    pub fn heard_direct_since(&self, since: Instant) -> Vec<MeshDigest> {
        let mut ds: Vec<MeshDigest> = self.inner.lock().unwrap().values().filter(|(_, at, heard)| *heard == Heard::Direct && *at >= since).map(|(d, _, _)| d.clone()).collect();
        self.hand_out(ds.iter_mut());
        ds
    }

    /// The silence clock of every member held, restarted at `now`: a node back on its channel after a
    /// stop heard nothing while it was off it, so no member's silence is measured from before.
    pub fn restart_silence(&self) {
        let now = Instant::now();
        self.inner.lock().unwrap().restart_silence(now);
        for (_, at) in self.projected.lock().unwrap().values_mut() {
            *at = now;
        }
    }

    /// The deleted births heard again by their own digest and not yet reported ([`Self::returned_reported`]).
    pub fn returned_births(&self) -> Vec<MeshDigest> {
        self.returned.lock().unwrap().values().cloned().collect()
    }

    /// The returned birth `(node_id, incarnation)` was reported to the fabric-primary: it is owed nothing more.
    pub fn returned_reported(&self, node_id: &str, incarnation: &str) {
        self.returned.lock().unwrap().remove(&(node_id.to_string(), incarnation.to_string()));
    }

    /// This node's own loop ran at `now` (`Self::woken` first): what a gap is measured from.
    pub fn mark_awake(&self) {
        self.woken();
        *self.awake.lock().unwrap() = Some(Instant::now());
    }

    /// A node that was not running for longer than the staleness floor (its process frozen) measured
    /// no silence in that gap: on its first read after it, every member's silence restarts, so it acts
    /// on a member only after a full floor of silence it measured awake (Q131).
    pub(crate) fn woken(&self) {
        let now = Instant::now();
        let gap = {
            let mut awake = self.awake.lock().unwrap();
            match *awake {
                Some(last) if now.saturating_duration_since(last) > self.staleness_floor => {
                    *awake = Some(now);
                    now.saturating_duration_since(last)
                }
                _ => return,
            }
        };
        self.restart_silence();
        tracing::info_span!("rdm.mesh.topology.update.via-woken", gap_ms = gap.as_millis() as u64, staleness_ms = self.staleness_floor.as_millis() as u64)
            .in_scope(|| tracing::info!("this node was not running for longer than the staleness floor: every member's silence restarts now"));
    }

    /// The member's latest digest and how long it has been silent.
    pub fn get(&self, node_id: &str) -> Option<(MeshDigest, Duration)> {
        self.get_at(node_id, Instant::now())
    }

    /// Whether `incarnation`, a birth of `node_id` this view saw replaced as `mesh`'s mesh-primary, has
    /// gone unheard since it was replaced: an unheard mesh-primary, the fabric-primary's to investigate
    /// (node-recovery.md step 2). A replaced birth heard after it was replaced is one more node-admin of
    /// its mesh, and its later silence is its mesh-primary's.
    pub fn unheard_since_replaced(&self, mesh: &str, node_id: &str, incarnation: &IncarnationId, now: Instant) -> bool {
        let Some(replaced_at) = self.primaries.replaced_at(PrimaryRole::MeshPrimary, mesh, incarnation) else { return false };
        match self.get_at(node_id, now).filter(|(d, _)| &d.node.incarnation == incarnation) {
            Some((_, unheard)) => now.checked_sub(unheard).is_none_or(|heard| heard <= replaced_at),
            None => true,
        }
    }

    /// The member's latest digest and how long it has gone unheard at `now`.
    pub fn get_at(&self, node_id: &str, now: Instant) -> Option<(MeshDigest, Duration)> {
        self.woken();
        let mut held = self.inner.lock().unwrap().get(node_id).map(|(d, at, fw)| (d.clone(), unheard(at, *fw, now)));
        self.hand_out(held.iter_mut().map(|(d, _)| d));
        held
    }

    /// The birth held at endpoint key `endpoint_id`, as [`Self::get`] answers for its NodeId: a map
    /// read. Two births at one key (a node re-born over its inherited data dir): the one this view
    /// took most recently (`HeldMembers`).
    pub fn get_by_endpoint(&self, endpoint_id: &str) -> Option<(MeshDigest, Duration)> {
        self.get_by_endpoint_at(endpoint_id, Instant::now())
    }

    /// [`Self::get_by_endpoint`] at `now`.
    pub fn get_by_endpoint_at(&self, endpoint_id: &str, now: Instant) -> Option<(MeshDigest, Duration)> {
        self.woken();
        let mut held = self.inner.lock().unwrap().at_endpoint(endpoint_id).map(|(d, at, fw)| (d.clone(), unheard(at, *fw, now)));
        self.hand_out(held.iter_mut().map(|(d, _)| d));
        held
    }

    /// The birth held at `name`, as [`Self::get`] answers for its NodeId: a map read. Two births at
    /// one (path, name) (a successor while its predecessor is still held): the one this view took most
    /// recently (`HeldMembers`).
    pub fn get_by_path_name(&self, name: &rdm_core::PathName) -> Option<(MeshDigest, Duration)> {
        self.get_by_path_name_at(name, Instant::now())
    }

    /// [`Self::get_by_path_name`] at `now`.
    pub fn get_by_path_name_at(&self, name: &rdm_core::PathName, now: Instant) -> Option<(MeshDigest, Duration)> {
        self.woken();
        let mut held = self.inner.lock().unwrap().at_name(name).map(|(d, at, fw)| (d.clone(), unheard(at, *fw, now)));
        self.hand_out(held.iter_mut().map(|(d, _)| d));
        held
    }

    /// The members of `own_mesh`, other than `me`, this view holds and does not hear within the
    /// staleness floor, whose silence no lifecycle event it applied explains (gossip.md §6, "A cut-off
    /// view", Q58): `(node_id, name, silent_ms)`. Expected silence is left out: a member held `Leaving`
    /// (its stop admitted), a deleted birth; a stopped birth left the
    /// digest view with `NodeStopped` and is not here either. A view hearing no other member and holding one of
    /// these is cut off; a view holding none is not, however many members it stopped.
    pub fn unheard_unexplained(&self, own_mesh: &str, me: &str) -> Vec<(String, String, u64)> {
        self.woken();
        let now = Instant::now();
        let floor = self.staleness_floor();
        let held: Vec<(MeshDigest, Duration)> = self.inner.lock().unwrap().values().map(|(d, at, fw)| (d.clone(), unheard(at, *fw, now))).collect();
        held.into_iter()
            .filter(|(d, silent)| *silent > floor && d.node.mesh == own_mesh && d.node.node_id.to_string() != me)
            .filter(|(d, _)| d.status != rdm_core::MemberStatus::Leaving && !self.is_deleted_at(d.node.node_id.as_str(), now))
            .map(|(d, silent)| (d.node.node_id.to_string(), d.node.node_id.to_string(), silent.as_millis() as u64))
            .collect()
    }

    /// The pruner's sweep of silence ([`Self::mark_silence_at`] now).
    pub fn mark_silence(&self) -> bool {
        self.mark_silence_at(Instant::now())
    }

    /// The pruner's sweep at `now` (Q254: one clock, the pruner's; its mark is a view change): every
    /// departure past the retention is forgotten (`Self::expire_deleted_at`), and the births held
    /// silent past the staleness floor are read, each held birth by its unheard time and each birth
    /// held only from its mesh-primary's projection by the time since it was first held. When they
    /// differ from the last sweep's, the silence watch moves ([`RoutingWatch`]): the view is rebuilt on
    /// the mark, never on a poll. `true` when it moved.
    pub fn mark_silence_at(&self, now: Instant) -> bool {
        self.expire_deleted_at(now);
        let floor = self.staleness_floor;
        let mut silent: BTreeSet<(String, String)> = self
            .inner
            .lock()
            .unwrap()
            .values()
            .filter(|(_, at, fw)| unheard(at, *fw, now) > floor)
            .map(|(d, _, _)| (d.node.node_id.to_string(), d.node.incarnation.as_str().to_string()))
            .collect();
        let projected: Vec<String> = self.projected.lock().unwrap().keys().cloned().collect();
        for id in projected {
            if let Some((d, _)) = self.projected_at(&id, now).filter(|(_, unheard)| *unheard > floor) {
                silent.insert((d.node.node_id.to_string(), d.node.incarnation.as_str().to_string()));
            }
        }
        let moved = {
            let mut held = self.silent.lock().unwrap();
            let moved = *held != silent;
            if moved {
                *held = silent;
            }
            moved
        };
        if moved {
            self.silence.send_modify(|v| *v += 1);
        }
        moved
    }

    /// The node ids of the births of `mesh` held, read under the lock: no digest is copied, only its id
    /// (Q268: a count of a mesh's births never copies the view).
    pub fn node_ids_of_mesh(&self, mesh: &str) -> Vec<String> {
        self.inner.lock().unwrap().values().filter(|(d, _, _)| d.node.mesh == mesh).map(|(d, _, _)| d.node.node_id.as_str().to_string()).collect()
    }

    /// Whether a birth of `mesh` is held whose mesh id is set and is not `mesh_id`, read under the lock
    /// (Q268): nothing is copied.
    pub fn holds_birth_of_mesh_under_another_id(&self, mesh: &str, mesh_id: &Option<rdm_core::MeshId>) -> bool {
        self.inner.lock().unwrap().values().any(|(d, _, _)| d.node.mesh == mesh && d.mesh_id.is_some() && d.mesh_id != *mesh_id)
    }

    /// The births each held digest names as replaced (`supersedes`), read under the view's lock with
    /// nothing copied: what an election's lost set takes from the digests (Q271).
    pub fn superseded(&self) -> Vec<rdm_core::IncarnationId> {
        self.inner.lock().unwrap().values().filter_map(|(d, _, _)| d.node.supersedes.clone()).collect()
    }

    /// Every digest held.
    pub fn all(&self) -> Vec<MeshDigest> {
        let mut ds: Vec<MeshDigest> = self.inner.lock().unwrap().values().map(|(d, _, _)| d.clone()).collect();
        self.hand_out(ds.iter_mut());
        ds
    }

    /// How long it has been since `mesh` was last heard on the backbone at `now`: the youngest
    /// receipt of any of its members listed in a mesh topology received here. `None` when no member of
    /// the mesh was ever received that way (a mesh known only from forwarded topology, an entry
    /// answer or this node's own mesh has no backbone receipt: topology is not liveness, R-G2).
    pub fn mesh_unheard(&self, mesh: &str, now: Instant) -> Option<Duration> {
        self.inner
            .lock()
            .unwrap()
            .values()
            .filter(|(d, _, heard)| d.node.mesh == mesh && *heard == Heard::Forwarded)
            .map(|(_, at, _)| now.saturating_duration_since(*at))
            .min()
    }

    /// The meshes with a backbone receipt held (see [`Self::mesh_unheard`]).
    pub fn backbone_meshes(&self) -> BTreeSet<String> {
        self.inner.lock().unwrap().values().filter(|(_, _, heard)| *heard == Heard::Forwarded).map(|(d, _, _)| d.node.mesh.clone()).collect()
    }

    /// `mesh`'s current primary node-admin: the exact birth the held primary record names, the
    /// canonical LeadershipView's answer as its digest, its mesh-primary's projection or an entry read
    /// carried it), with that birth's latest digest. Its status and how long it has gone unheard
    /// do not move the role (only a new record or a proven loss does), so neither is read here.
    pub fn mesh_primary(&self, mesh: &str) -> Result<MeshDigest, NoMeshPrimary> {
        let primary = self.primaries.mesh(mesh).ok_or_else(|| NoMeshPrimary::NoPrimaryRecord { mesh: mesh.to_string() })?;
        if self.primaries.gone().contains(&primary.incarnation) {
            return Err(NoMeshPrimary::PrimaryGone { primary });
        }
        match self.get(&primary.node_id.to_string()) {
            Some((d, _)) if d.node.incarnation == primary.incarnation => Ok(d),
            Some((d, _)) => Err(NoMeshPrimary::PrimaryNotHeld { held_incarnation: Some(d.node.incarnation), deleted: false, primary }),
            None => Err(NoMeshPrimary::PrimaryNotHeld { held_incarnation: None, deleted: self.is_deleted(&primary.node_id.to_string()), primary }),
        }
    }

    /// Every held member with how long it has gone unheard at `now` (a deleted birth is not held).
    pub(crate) fn silent_held(&self, now: Instant) -> Vec<(MeshDigest, Duration)> {
        self.woken();
        let mut held: Vec<(MeshDigest, Duration)> = self.inner.lock().unwrap().values().map(|(d, at, fw)| (d.clone(), unheard(at, *fw, now))).collect();
        self.hand_out(held.iter_mut().map(|(d, _)| d));
        held
    }

    /// Depart every remaining birth of `mesh` this digest view still holds (in the view, projected or
    /// stopped by its stop), so a deleted mesh's final mesh topology or `MeshLeft` leaves no crashed
    /// predecessor birth of the mesh in the active topology and holds it deleted for the retention.
    pub fn depart_mesh_births(&self, mesh: &str, build_id: &str, at_rafka_ms: u64) {
        let mut roster: BTreeMap<String, (IncarnationId, rdm_core::PathName, String)> = BTreeMap::new();
        for d in self.all().into_iter().filter(|d| d.node.mesh == mesh) {
            roster.insert(d.node.node_id.to_string(), (d.node.incarnation.clone(), d.node.location(), d.node.r#type));
        }
        for (d, _) in self.projected_unheard_at(Instant::now()).into_iter().filter(|(d, _)| d.node.mesh == mesh) {
            roster.entry(d.node.node_id.to_string()).or_insert((d.node.incarnation.clone(), d.node.location(), d.node.r#type));
        }
        for d in self.stopped_digests().into_iter().filter(|d| d.node.mesh == mesh) {
            roster.entry(d.node.node_id.to_string()).or_insert((d.node.incarnation.clone(), d.node.location(), d.node.r#type));
        }
        for op in self.in_flight().into_iter().filter(|op| op.mesh == mesh) {
            roster.entry(op.node_id.to_string()).or_insert((op.incarnation.clone(), op.location(), op.r#type));
        }
        let own = self.own.lock().unwrap().clone();
        let mut changed = false;
        for (id, (incarnation, path, r#type)) in roster {
            if own.as_deref() == Some(id.as_str()) {
                continue;
            }
            let Ok(node_id) = NodeId::parse(&id) else { continue };
            let op = LifecycleOp { r#type, mesh: mesh.to_string(), build_id: build_id.to_string(), operation: rdm_core::OperationId::node("node.delete", &node_id), node_id, incarnation, path: path.path.clone(), name: path.name.clone(), event_at_rafka_ms: at_rafka_ms, traceparent: None, tracestate: None };
            if self.depart_quiet(op) {
                changed = true;
            }
        }
        self.statuses.forget_mesh(mesh);
        if let Some(cb) = self.on_mesh_deleted.0.lock().unwrap().clone() {
            cb(mesh);
        }
        if changed {
            self.births.send_modify(|v| *v += 1);
        }
    }

    pub(crate) fn depart_quiet(&self, op: LifecycleOp) -> bool {
        let id = op.node_id.to_string();
        {
            let mut deleted = self.deleted.lock().unwrap();
            if deleted.contains_key(&id) {
                return false;
            }
            deleted.insert(id.clone(), (op.clone(), Instant::now()));
        }
        delete_incarnation(&op.node_id, &op.incarnation);
        self.end_stop(op.node_id.as_str());
        self.in_flight.lock().unwrap().retain(|_, o| o.node_id != op.node_id);
        self.overlays_moved();
        self.member_statuses.lock().unwrap().remove(&id);
        self.projected.lock().unwrap().remove(&id);
        self.inner.lock().unwrap().remove(&id);
        true
    }
}
