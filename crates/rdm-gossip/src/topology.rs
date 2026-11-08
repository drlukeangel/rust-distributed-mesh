//! Hierarchical fabric topology over iroh-gossip (HyParView + Plumtree;
//! never a home-made delivery layer). rafka `gossip.md` §6:
//!
//! - each mesh has its own topology channel ([`mesh_channel`]); a member
//!   publishes its [`MeshDigest`] there, and only there;
//! - node-admins alone also join the fabric's backbone ([`backbone_channel`]),
//!   through every node-admin they know, whatever its mesh;
//! - a mesh's admin primary publishes its mesh's members on the backbone; a
//!   peer mesh's primary forwards those onto its own mesh channel; the fabric
//!   primary alone publishes the fabric's status, which peer primaries forward;
//! - so every node holds every mesh's nodes without a fabric-wide topology
//!   channel. A member the publisher stops forwarding goes silent everywhere.
//!
//! Every member heard is registered with the endpoint at its gossip address
//! (its one endpoint), so HyParView reaches a peer it learned by id.

use rdm_transport::clock::SharedClock;
use crate::aggregate::{Gap, PublisherId, HeldAggregate};
use anyhow::Result;
use rdm_core::{FabricId, IncarnationId, MeshDigest, MeshId, NodeId, Primary, PrimaryRole};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;


use crate::{frame::*, status::*, primary::*, view::*, digest_view::*};

/// How often a node's publish loop marks its heartbeat in the trace (the digest itself goes out
/// every gossip interval).
pub const HEARTBEAT_EVERY: Duration = Duration::from_secs(5);

/// A node's topology view: the cache of the fabric's topology every node holds (gossip.md §3),
/// filled by the channels it joins ([`crate::channel::Channels`]) and its topology read.
#[derive(Clone)]
pub struct TopologyView {
    pub(crate) view: View,
    pub(crate) fabric: FabricId,
    pub(crate) mesh_id: MeshId,
    /// The digest view this topology view holds.
    pub digest_view: DigestView,
    /// The Rafka-time this process composes; every gossip stamp on this topology view reads it.
    pub(crate) clock: SharedClock,
    /// The name the mesh-primary's row last gave this birth (`UpdateName`, R12): carried in every
    /// digest this topology view publishes from then on. `None` while the birth holds its launch name.
    pub(crate) name: Arc<Mutex<Option<rdm_core::PathName>>>,
    /// This node's own mesh [`StatusPublisher`], shared with its [`Backbone`].
    pub(crate) mesh_status_publisher: Arc<Mutex<StatusPublisher>>,
}

impl TopologyView {
    /// The topology view of `node`, of `mesh` (`mesh_id` names its channel) in `fabric`: the cache
    /// every node holds (gossip.md §3), empty until its channels and its topology read fill it.
    /// `clock` is the Rafka-time the process composes: every timestamp read from it. `name` is the name
    /// the node's row gave it, handed by the node's own record that holds it (rdm-node's `HeldName`,
    /// gossip-by-kind.md §4.3): every digest this view publishes carries it.
    pub fn new(fabric: &FabricId, mesh: &str, mesh_id: &MeshId, node: &str, clock: SharedClock, name: Arc<Mutex<Option<rdm_core::PathName>>>) -> Self {
        let mut view = View::new(mesh, node);
        view.held_name = name.clone();
        view.digest_view.held_name = name.clone();
        let mesh_status_publisher = Arc::new(Mutex::new(StatusPublisher::new(node, StatusScope::Mesh(mesh.to_string()))));
        // Its own mesh's births are held under its own mesh's id.
        view.digest_view.hold_mesh_id(mesh, Some(mesh_id));
        let me = Self { digest_view: view.digest_view.clone(), view, fabric: fabric.clone(), mesh_id: mesh_id.clone(), clock, name, mesh_status_publisher };
        me.watch_meshes();
        me
    }


    /// Hand `fact` straight to this admin's own mesh [`StatusPublisher`] as its adopted held fact
    /// (Q272; node-create.md "A node-admin created into a live mesh" step 4), never through the gossip receive view.
    pub fn adopt_mesh_status(&self, fact: StatusFact) -> bool {
        self.mesh_status_publisher.lock().unwrap().adopt(fact)
    }

    /// The status fact this admin's own mesh [`StatusPublisher`] holds, if any.
    pub fn adopted_mesh_status(&self) -> Option<StatusFact> {
        self.mesh_status_publisher.lock().unwrap().held()
    }

    /// This topology view's fabric identity.
    pub fn fabric_id(&self) -> &FabricId { &self.fabric }

    /// A node's local query of the topology it holds (query.md §2; Q232): every digest it heard within
    /// the staleness floor that matches every pair of `filter`, by `(path, name)`, with no call. The pairs
    /// name only what gossip carries ([`rdm_core::filter::GOSSIPED`]), matched as `node.get` matches a
    /// row ([`rdm_core::filter::Filter::matches`]); a pair naming a row-only property (`tags`,
    /// `config.<key>`, `executable`, ...) or one the node schema refuses is `Err` naming the pair, the
    /// row-only one naming `node.get`, whatever digests would have matched. A refusal is the one
    /// `rdm.mesh.node.serve.via-topology-view` span, naming the pairs and why; an answer is a local
    /// read of what this node holds, on every routing decision, and emits nothing, as
    /// [`DigestView::get_by_path_name`] does.
    pub fn nodes_get(&self, filter: &rdm_core::filter::Filter) -> std::result::Result<Vec<MeshDigest>, String> {
        if let Some(reason) = filter.local_refusal() {
            tracing::info_span!(
                "rdm.mesh.node.serve.via-topology-view",
                node = %self.node_name(),
                read = "topology_view.nodes_get",
                properties = %filter.properties(),
                outcome = "rejected-filter",
                detail = reason.as_str(),
                "otel.kind" = "internal",
            )
            .in_scope(|| tracing::info!("the local query names a pair the topology this node holds does not carry; nothing is matched"));
            return Err(reason);
        }
        let mut nodes: Vec<MeshDigest> = self.digest_view.current(self.digest_view.staleness_floor()).into_iter().filter(|d| filter.matches(d)).collect();
        nodes.sort_by(|a, b| a.node.location().cmp(&b.node.location()));
        Ok(nodes)
    }

    /// Evidence of what this node holds: a mesh it first hears, or hears
    /// again, and a mesh none of whose members it still hears.
    pub(crate) fn watch_meshes(&self) {
        let (digest_view, via, view) = (self.digest_view.clone(), self.view.via.clone(), self.view.clone());
        tokio::spawn(async move {
            let mut held: BTreeSet<String> = BTreeSet::new();
            let mut heard: BTreeSet<String> = BTreeSet::new();
            loop {
                digest_view.mark_awake();
                let node = view.current_name();
                let own_mesh = view.mesh.clone();
                let current = digest_view.current(digest_view.staleness_floor());
                let now_heard: BTreeSet<String> = current.iter().map(|d| d.node.node_id.to_string()).collect();
                // Q58: a member whose silence a lifecycle event this view applied explains is expected
                // silence, neither stale nor counted: a stop admitted (`Leaving`) or applied (the stopped
                // birth left the digest view), a deletion applied (the birth left the digest view).
                // What remains is a member of this node's own mesh the view still holds and does not hear.
                let unexplained = digest_view.unheard_unexplained(&own_mesh, &node);
                for (id, _, silent_ms) in unexplained.iter().filter(|(id, _, _)| heard.contains(id)) {
                    tracing::info_span!("rdm.mesh.topology.update.via-member-stale", node = %node, member_id = %id, silent_ms, staleness_ms = digest_view.staleness_floor().as_millis() as u64)
                        .in_scope(|| tracing::info!("a member is no longer heard within the staleness floor: held, not current"));
                }
                // Q254: this sweep is the one clock of a held birth's silence. A birth crossing the
                // staleness floor, either way, or a departure past its retention, moves the routing
                // watch here, so the view is rebuilt on the mark and on no poll.
                digest_view.mark_silence();
                heard = now_heard;
                let now: BTreeSet<String> = current.into_iter().map(|d| d.node.mesh).collect();
                for m in now.difference(&held) {
                    let v = via.lock().unwrap().get(m).copied().unwrap_or("mesh-channel");
                    tracing::info_span!("rdm.mesh.topology.update.via-mesh-learned", node = %node, mesh = %m, via = v)
                        .in_scope(|| tracing::info!("this node holds the mesh"));
                }
                for m in held.difference(&now) {
                    tracing::info_span!("rdm.mesh.topology.update.via-mesh-silent", node = %node, mesh = %m)
                        .in_scope(|| tracing::info!("no member of the mesh is heard"));
                    via.lock().unwrap().remove(m);
                }
                held = now;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }

    /// This node's launch path name.
    pub fn node(&self) -> &str {
        &self.view.node
    }

    /// This node's mesh name (`<mesh>`).
    pub fn mesh_name(&self) -> &str {
        &self.view.mesh
    }

    /// This node's minted mesh id.
    pub fn mesh_id(&self) -> &MeshId {
        &self.mesh_id
    }

    /// This node's current path name, as it publishes itself: the name its row gave it, else its launch name.
    pub fn node_name(&self) -> String {
        self.view.current_name()
    }

    /// Hold (or drop) this node as its mesh's primary: while it is, it authors its own mesh's member
    /// statuses from what its members report to it and replays what it holds to a neighbour coming
    /// up; otherwise it takes them from its primary's projection (gossip.md §3.1 `TopologyFull`).
    /// [`Backbone::set_mesh_primary`] sets it with the role; a launcher that executes its mesh's
    /// node lifecycle with no backbone sets it here.
    pub fn set_mesh_primary(&self, primary: bool) {
        self.view.primary.store(primary, Ordering::Relaxed);
    }

    /// How many meshes this node holds a live member of.
    pub fn meshes_held(&self) -> usize {
        self.digest_view.current(self.digest_view.staleness_floor()).into_iter().map(|d| d.node.mesh).collect::<BTreeSet<_>>().len()
    }

    /// The fabric status this node holds: the last one published, until the next change.
    pub fn fabric_status(&self) -> Option<FabricStatus> {
        self.view.digest_view.statuses.fabric()
    }

    /// The status of `mesh` this node holds: the last one published, until the next change.
    pub fn mesh_status(&self, mesh: &str) -> Option<MeshStatus> {
        self.view.digest_view.statuses.mesh(mesh)
    }

    /// Every mesh status this node holds, by mesh name: each the last one published for its mesh.
    pub fn mesh_statuses(&self) -> Vec<(String, MeshStatus)> {
        self.view.digest_view.statuses.meshes()
    }

    /// The statuses this node holds, as the frames it serves on a join answer
    /// (`entry::EntryAnswer::statuses`, original publisher and instant kept).
    pub fn status_frames(&self, me: &str) -> Vec<Frame> {
        self.view.digest_view.statuses.held_frames(&self.fabric, me)
    }

    /// Hold the statuses an entry pull answered.
    pub fn learn_statuses(&self, frames: &[Frame]) {
        for f in frames {
            if let Frame::MeshStatus { mesh, status, changed_at_rafka_ms, .. } = f.data() {
                if *mesh == self.view.mesh {
                    self.adopt_mesh_status(StatusFact { status: status.clone(), changed_at_rafka_ms: *changed_at_rafka_ms });
                }
            }
            self.view.take_status(f, &self.fabric, "entry-answer");
        }
    }

    /// The primaries this node holds.
    pub fn primaries(&self) -> &PrimaryView {
        &self.view.digest_view.primaries
    }

    /// Whether the exact birth is held deleted from its node recovery's verdict
    /// ([`DigestView::hold_recovering`]).
    pub fn is_recovering(&self, node_id: &str, incarnation: &IncarnationId) -> bool {
        self.view.digest_view.is_recovering(node_id, incarnation)
    }

    /// Hold the primary records an entry read named (`topology_read`): each only if it supersedes what
    /// this node holds.
    pub fn learn_primaries(&self, records: &[(PrimaryRole, Primary, bool)], via: &'static str) {
        for (role, primary, gone) in records {
            self.view.note_primary(role.clone(), primary, via);
            if *gone {
                self.view.digest_view.primaries.mark_gone(primary.incarnation.clone());
            }
        }
    }

    /// The Rafka-time this process composed.
    pub fn clock(&self) -> &SharedClock {
        &self.clock
    }

    /// The name this birth publishes: the one its row gave it, as this view was handed it
    /// ([`Self::new`]), else `birth`'s.
    pub fn held_name(&self, birth: &rdm_core::PathName) -> rdm_core::PathName {
        self.name.lock().unwrap().clone().unwrap_or_else(|| birth.clone())
    }

    /// `d` as this birth reports it: with the name it was told, when it was told it.
    pub fn reported(&self, d: &MeshDigest) -> MeshDigest {
        let mut d = d.clone();
        if let Some(n) = self.name.lock().unwrap().as_ref() {
            { let at = n.clone(); d.node.path = at.path; d.node.name = at.name; }
        }
        d
    }

    /// The topology this node holds, one aggregate per mesh (Node RPC op `0x1E` serves it), each at
    /// the publisher and version it holds it at and with the content that version was published
    /// with: this node's own mesh too, as its primary last put it into the mesh (gossip.md §3.1: a
    /// version names one content; "A frame equal in revision to the held one but different in
    /// content is an inconsistency, refused by name"). A member the view heard since that
    /// publication is served at the version the next publication bumps to, never under this one. A mesh
    /// this node holds no published aggregate of is absent, but for its own mesh at its primary
    /// before the first publication.
    pub fn held_topology(&self, own: &MeshDigest) -> Vec<HeldAggregate> {
        self.held_topology_of(own, None)
    }

    /// [`Self::held_topology`] of `of` alone when one is named (a read of one mesh, Q266): no other
    /// mesh's source is copied to be dropped.
    pub fn held_topology_of(&self, own: &MeshDigest, of: Option<&str>) -> Vec<HeldAggregate> {
        // A held source is topology, served as published: whether its members are alive is the
        // reader's to learn (a read writes them as topology and refreshes no liveness).
        let mut held = self.view.held_sources_of(of);
        let mesh = &self.view.mesh;
        // A deletion this node executed of a peer mesh's node (the fabric-primary's node recovery of a
        // lost mesh-primary's mesh, Q335) is served with the node listed deleted: a mesh's aggregate
        // lists only its own mesh's nodes (Q390), so that mesh's next mesh-primary holds the deletion
        // from its start read and its own aggregate then lists it.
        for s in held.iter_mut().filter(|s| &s.mesh != mesh) {
            s.digests = self.digest_view.with_executed_deletions(&s.mesh, std::mem::take(&mut s.digests));
        }
        // A mesh's primary that has not published yet still holds its members in its view: it serves
        // them as its own source at version 0 (the version before any publication, the one its
        // publisher reports), never as nothing. Its first publication supersedes it.
        if self.view.primary.load(Ordering::Relaxed) && of.is_none_or(|want| want == mesh.as_str()) && held.iter().all(|s| &s.mesh != mesh) {
            let mut digests: Vec<MeshDigest> = self
                .digest_view
                .current(self.digest_view.staleness_floor())
                .into_iter()
                .filter(|d| &d.node.mesh == mesh && d.fabric_id == self.fabric && d.node.node_id != own.node.node_id)
                .collect();
            // Its own digest as every read hands it out: with the primary records the view holds for it.
            let mut own = own.clone();
            self.digest_view.recorded_primaries(&mut own);
            digests.push(own.clone());
            let (in_flight, deleted) = self.digest_view.overlays_of(mesh);
            let digests = self.digest_view.aggregate_nodes(mesh, digests, &in_flight, &deleted);
            held.push(HeldAggregate { mesh: mesh.clone(), mesh_id: Some(self.mesh_id().clone()), topology_version: 0, digests });
        } else if !self.view.primary.load(Ordering::Relaxed) && of.is_none_or(|want| want == mesh.as_str()) && held.iter().any(|s| &s.mesh == mesh) {
            // An ordinary node's own mesh is what it holds of it: each node from its own Digest, and the
            // stops and departures it took said by each node's status (gossip.md §3.3, Q387, Q390: nothing
            // of its own mesh reaches it on its mesh channel), served at the version of the read it wrote.
            let read = held.iter().position(|s| &s.mesh == mesh).map(|i| held.remove(i));
            let mut digests: Vec<MeshDigest> = self
                .digest_view
                .current(self.digest_view.staleness_floor())
                .into_iter()
                .filter(|d| &d.node.mesh == mesh && d.fabric_id == self.fabric && d.node.node_id != own.node.node_id)
                .collect();
            let mut own = own.clone();
            self.digest_view.recorded_primaries(&mut own);
            digests.push(own.clone());
            let (in_flight, deleted) = self.digest_view.overlays_of(mesh);
            let digests = self.digest_view.aggregate_nodes(mesh, digests, &in_flight, &deleted);
            let topology_version = read.map(|s| s.topology_version).expect("the read of its own mesh is held");
            held.push(HeldAggregate { mesh: mesh.clone(), mesh_id: Some(self.mesh_id().clone()), topology_version, digests });
        }
        held
    }

    /// The sources whose copy is desynchronized now, with the gap that did it.
    pub fn desynced_sources(&self) -> Vec<(String, Gap)> {
        self.view.copies.lock().unwrap().desynced()
    }

    /// The member record of the birth at `endpoint` (its iroh key) in a copy this node holds in its
    /// topology view, whichever channel or read wrote it. The copy is the member projection (gossip.md
    /// §3.1); the heartbeat view is not read. `None` when no copy this node holds names the key.
    pub fn projected_member_at(&self, endpoint: &str) -> Option<MeshDigest> {
        self.view.copies.lock().unwrap().member_at(endpoint)
    }

    /// The copy of `mesh` this node holds in its topology view, whichever channel or read wrote it (its
    /// Mesh channel, the backbone or a topology read): its `topology_version` and its nodes. `None`
    /// when it holds no copy of `mesh`.
    pub fn held_copy(&self, mesh: &str) -> Option<(u64, Vec<MeshDigest>)> {
        let copies = self.view.copies.lock().unwrap();
        let (_, version) = copies.held_version(mesh)?;
        Some((version, copies.held_full(mesh)?.digests()))
    }

    /// The version this node holds of `mesh`, with the label of its author when one was heard.
    pub fn held_source_version(&self, mesh: &str) -> Option<(Option<PublisherId>, u64)> {
        self.view.copies.lock().unwrap().held_version(mesh)
    }

    /// This node's own Mesh's current primary node-admin: the birth its held primary record names
    /// ([`DigestView::mesh_primary`]). The node tops up from it and from no other.
    pub fn mesh_primary(&self) -> Result<MeshDigest, NoMeshPrimary> {
        self.digest_view.mesh_primary(&self.view.mesh)
    }

    /// A node-admin with no live primary to read its mesh's runtime facts from (`reason`: its primary
    /// record names a birth proven gone, or it is its mesh's primary) takes the runtime fact of each
    /// birth of its mesh it holds with none from that birth's record in node-storage, by `read` (Q404
    /// A): it never calls the node. One span per birth read, `source=node-storage`. Once no birth is
    /// owed its fact, the `no-runtime-fact` gap on its own mesh ends; `true` while one still is (no
    /// record of that exact birth is stored yet, or the store did not answer), so it reads again on the
    /// one backoff rule. A node that is no node-admin, or owes nothing, reads nothing.
    pub async fn runtimes_from_node_records(&self, read: &NodeRecordRead, reason: &'static str) -> bool {
        if !self.view.reads_runtime_facts() {
            return false;
        }
        let mesh = self.view.mesh.clone();
        let owed = self.digest_view.without_runtime_fact(&mesh);
        if !owed.is_empty() {
            let node = self.view.current_name();
            for (node_id, incarnation, answer) in read(owed).await {
                match answer {
                    Ok(Some(runtime)) => {
                        let data_dir = runtime.data_dir.clone().unwrap_or_default();
                        let held = runtime.runtime.is_some() && self.digest_view.hold_runtime(&node_id, &incarnation, runtime);
                        tracing::info_span!("rdm.mesh.runtime.update.via-node-record", node = %node, node_id = %node_id, incarnation_id = %incarnation.as_str(), source = "node-storage", reason, data_dir = %data_dir, held)
                            .in_scope(|| tracing::info!("a birth's runtime fact is taken from its node record: no live primary holds it to read"));
                    }
                    Ok(None) => {
                        tracing::info_span!("rdm.mesh.runtime.reject.via-no-node-record", node = %node, node_id = %node_id, incarnation_id = %incarnation.as_str(), source = "node-storage", reason)
                            .in_scope(|| tracing::info!("node-storage holds no record of this exact birth: its runtime fact is read again"));
                    }
                    Err(error) => {
                        tracing::info_span!("rdm.mesh.runtime.reject.via-node-record-unreadable", node = %node, node_id = %node_id, incarnation_id = %incarnation.as_str(), source = "node-storage", reason, error = %error)
                            .in_scope(|| tracing::info!("the birth's node record could not be read: its runtime fact is read again"));
                    }
                }
            }
        }
        let still = !self.digest_view.without_runtime_fact(&mesh).is_empty();
        if !still {
            self.view.copies.lock().unwrap().end_gap(&mesh, &Gap::NoRuntimeFact);
        }
        still
    }
}

/// Why a node cannot address its Mesh's current primary: each a distinct fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoMeshPrimary {
    /// The node holds no primary record of the mesh.
    NoPrimaryRecord {
        /// The mesh whose record is not held.
        mesh: String,
    },
    /// The record names a birth whose digest the node does not hold.
    PrimaryNotHeld {
        /// The record held.
        primary: Primary,
        /// The birth of the same node the node holds instead, when it holds one.
        held_incarnation: Option<IncarnationId>,
        /// Whether the node holds the primary's node as deleted.
        deleted: bool,
    },
    /// The record's birth is held as proven gone.
    PrimaryGone {
        /// The record held.
        primary: Primary,
    },
}

impl NoMeshPrimary {
    /// The span reason naming this fact.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::NoPrimaryRecord { .. } => "no-primary-record-held",
            Self::PrimaryNotHeld { .. } => "primary-not-held",
            Self::PrimaryGone { .. } => "primary-gone",
        }
    }

    /// The primary record held, when one is.
    pub fn primary(&self) -> Option<&Primary> {
        match self {
            Self::NoPrimaryRecord { .. } => None,
            Self::PrimaryNotHeld { primary, .. } | Self::PrimaryGone { primary } => Some(primary),
        }
    }
}

impl std::fmt::Display for NoMeshPrimary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPrimaryRecord { mesh } => write!(f, "no primary record of mesh {mesh} is held"),
            Self::PrimaryNotHeld { primary, held_incarnation: Some(other), .. } => write!(f, "the primary record names {primary}; the node holds birth {} of that node instead", other.as_str()),
            Self::PrimaryNotHeld { primary, held_incarnation: None, deleted } => write!(f, "the primary record names {primary}; no digest of that node is held{}", if *deleted { " (held deleted)" } else { "" }),
            Self::PrimaryGone { primary } => write!(f, "the primary record names {primary}, a birth held as proven gone"),
        }
    }
}

/// What a top-up read wrote to the topology view.
#[derive(Debug, Clone, Default)]
pub struct TopUpDone {
    /// `(mesh, topology_version)` of each mesh written.
    pub written: Vec<(String, u64)>,
}

/// Reads the topology of `meshes` from a mesh primary and writes it to the given topology view
/// (the topology read, over Node RPC): the topology view holds no Node RPC client, the process
/// composition hands it one.
pub type TopUpFetch = std::sync::Arc<dyn Fn(crate::channel::Channels, MeshDigest, Vec<String>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<TopUpDone, String>> + Send>> + Send + Sync>;

/// One exact birth's runtime as its node record answers it: `Ok(None)` when node-storage holds no
/// record of that exact birth, `Err` the store's failure.
pub type RecordedRuntime = (NodeId, IncarnationId, Result<Option<rdm_core::NodeRuntime>, String>);

/// Reads the runtime and data dir of each exact birth from its record in node-storage (Q404 A): a
/// point read of each record, never a call to the node. The topology view holds no store: a
/// node-admin's process composition hands it the read.
pub type NodeRecordRead = std::sync::Arc<dyn Fn(Vec<(NodeId, IncarnationId)>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<RecordedRuntime>> + Send>> + Send + Sync>;

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use rdm_transport::clock::SharedClock;
    #[allow(unused_imports)]
    use crate::forward_path::ForwardPath;
    #[allow(unused_imports)]
    use crate::aggregate::{Chunk, Delta, Forwarder, Full, Gap, Moved, PublisherId, AggregateReceiver, HeldAggregate, SourceVersion, Taken};
    #[allow(unused_imports)]
    use anyhow::Result;
    #[allow(unused_imports)]
    use bytes::Bytes;
    #[allow(unused_imports)]
    use futures_lite::StreamExt as _;
    #[allow(unused_imports)]
    use iroh::address_lookup::MemoryLookup;
    #[allow(unused_imports)]
    use iroh::{Endpoint, EndpointAddr};
    #[allow(unused_imports)]
    use iroh_gossip::api::{Event, GossipSender};
    #[allow(unused_imports)]
    use iroh_gossip::net::Gossip;
    #[allow(unused_imports)]
    use iroh_gossip::proto::TopicId;
    #[allow(unused_imports)]
    use rdm_core::{FabricId, IncarnationId, LifecycleOp, MeshDigest, MeshId, NodeId, Primary, PrimaryRole, StateHook, DELETED_RETENTION};
    #[allow(unused_imports)]
    use serde::{Deserialize, Serialize};
    #[allow(unused_imports)]
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    #[allow(unused_imports)]
    use std::sync::atomic::{AtomicBool, Ordering};
    #[allow(unused_imports)]
    use std::sync::{Arc, Mutex};
    #[allow(unused_imports)]
    use std::time::{Duration, Instant};
    #[allow(unused_imports)]
    use rdm_core::cadence::{backbone_gossip_interval, gossip_interval, staleness_floor};
    #[allow(unused_imports)]
    use crate::{frame::*, location::*, status::*, primary::*, channel::*, view::*, digest_view::*, backbone::*};
    fn test_author() -> crate::envelope::Birth {
        crate::envelope::Birth { fabric: rdm_core::FabricId::parse("fab000000001").unwrap(), node: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() }
    }
    fn test_view(mesh: &str, node: &str) -> super::View {
        let view = super::View::new(mesh, node);
        view.forwarder.lock().unwrap().set_author(test_author());
        view
    }

    #[test]
    fn status_reinforcements_retain_context_while_new_facts_take_the_current_held_revision() {
        let mut publisher = super::StatusPublisher::new("mesh1.node-admin.1", super::StatusScope::Mesh("mesh1".into()));
        let author = test_author();
        publisher.set_role(true); publisher.author = Some((author.clone(), 1));
        let now = std::time::Instant::now();
        let first_state = super::StatusFact { status: "pending".into(), changed_at_rafka_ms: 0 };
        let first = publisher.observe("pending", Some(first_state), true, now, 100).unwrap();
        publisher.author = Some((author, 2));
        let repeated = publisher.due(now + super::STATUS_EVERY).unwrap();
        assert_eq!(first.context(), repeated.context(), "a reinforcement is the original event even when the held committed row advanced");
        let changed = publisher.observe("ready", None, true, now + super::STATUS_EVERY, 101).unwrap();
        assert_eq!(changed.context().unwrap().primary_revision(), Some(2), "a new fact takes the current held committed revision");
    }

    /// CONTRACT (Q272): a mesh status held without its author's context (a hand-off's state or a
    /// self-applied one, held before the publisher sends it) is replaced by the same fact under that
    /// context, so the node holds and serves the authored frame. Must NOT happen: the publisher's
    /// authored send of the fact refused as a duplicate, leaving a held frame no join answer encodes
    /// (fabric_takeover's adoption launch at lane 3: "its answer does not encode").
    #[test]
    fn the_same_mesh_status_under_its_authors_context_replaces_one_held_without() {
        let fabric = rdm_core::FabricId::parse("fab000000001").unwrap();
        let view = super::StatusView::default();
        let plain = super::Frame::MeshStatus { mesh: "mesh1".into(), status: "ready".into(), publisher: "mesh1.node-admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 40 };
        assert!(view.take(&plain, &fabric));
        assert!(view.held_frames(&fabric, "mesh1.node-admin.1").is_empty(), "a status held without its author's context is not served");
        let mut publisher = super::StatusPublisher::new("mesh1.node-admin.1", super::StatusScope::Mesh("mesh1".into()));
        publisher.set_role(true);
        publisher.author = Some((test_author(), 1));
        let heard = super::StatusFact { status: "ready".into(), changed_at_rafka_ms: 40 };
        let sent = publisher.observe("ready", Some(heard), true, std::time::Instant::now(), 50).expect("the held state goes out once the checklist completes");
        assert!(sent.context().is_some(), "the publisher's send is authored");
        assert!(view.take(&sent, &fabric), "the authored frame of the same fact replaces the plain one");
        assert!(!view.take(&sent, &fabric), "the same authored frame again is a duplicate");
        assert!(view.held_frames(&fabric, "mesh1.node-admin.1").iter().all(|f| f.context().is_some()), "every frame the node serves carries its author's context");
    }

    use super::*;
    use rdm_core::{EndpointId, model::NodeStatus, MeshNode};

    /// CONTRACT (gossip.md "A primary change on gossip": "The successor's next `Digest` carries
    /// `is_primary` (and `is_fabric_primary`) at the next revision, its normal announcement"; elections
    /// §3: the recorded primary, its exact birth and its revision, is an election input): a digest
    /// carrying its birth's primary revision and fabric-primary revision is held by the view as those
    /// records; a later digest of another birth at a lower revision replaces neither; one at a higher
    /// revision does. What must NOT happen: a stale announcement replacing a later record.
    /// CONTRACT (Q131, fabric-recovery.md step 2: silence "in its own view"; the heard-again red at
    /// 0acba6d97: mesh1.node-admin.2, thawed, read mesh1.node-admin.1 silent past the floor and opened its
    /// recovery): a node whose own loop stopped for longer than the staleness floor measured no silence
    /// in that gap. On its first read after waking, no member is unheard past the floor, and a member is
    /// unheard again only after a full floor measured awake. A view no loop marks awake measures silence
    /// as before. Must NOT happen: a thawed node reading its whole mesh silent the moment it wakes.
    #[test]
    fn a_node_that_was_not_running_past_the_floor_measures_no_silence_in_that_gap() {
        let floor = Duration::from_millis(150);
        let member = birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1);
        let awake = DigestView::with_floor(Duration::from_secs(60), floor);
        assert!(awake.record(member.clone()));
        awake.mark_awake();
        std::thread::sleep(floor * 2);
        assert!(awake.unheard_unexplained("mesh1", "mesh1.node-admin.2").is_empty(), "silence restarts on waking");
        std::thread::sleep(floor * 2);
        awake.mark_awake();
        let bare = DigestView::with_floor(Duration::from_secs(60), floor);
        assert!(bare.record(member.clone()));
        std::thread::sleep(floor * 2);
        assert_eq!(bare.unheard_unexplained("mesh1", "mesh1.node-admin.2").len(), 1, "a view no loop marks awake measures silence as before");
        let marked = DigestView::with_floor(Duration::from_secs(60), floor);
        assert!(marked.record(member));
        for _ in 0..8 {
            marked.mark_awake();
            std::thread::sleep(floor / 4);
        }
        assert_eq!(marked.unheard_unexplained("mesh1", "mesh1.node-admin.2").len(), 1, "a full floor of silence measured awake is silence");
    }

    #[test]
    fn a_digest_carries_its_births_primary_records_and_the_view_holds_the_latest() {
        let digest_view = DigestView::default();
        let (a, ai) = (NodeId::parse("000000000002").unwrap(), IncarnationId::mint());
        let (b, bi) = (NodeId::parse("000000000001").unwrap(), IncarnationId::mint());
        let mut da = digest(&a, &ai, None, NodeStatus::Ready, 1);
        da.set_primary(Some(3));
        da.set_fabric_primary(Some(7));
        assert!(digest_view.record(da.clone()));
        let held = |role| match role { PrimaryRole::FabricPrimary => digest_view.primaries.fabric(), _ => digest_view.primaries.node_type_primary("mesh1", "node") };
        assert_eq!(held(PrimaryRole::NodePrimary("node".into())), Some(Primary { mesh: "mesh1".into(), node_id: a.clone(), incarnation: ai.clone(), revision: 3 }));
        assert_eq!(held(PrimaryRole::FabricPrimary).map(|h| (h.node_id, h.revision)), Some((a.clone(), 7)));
        let mut db = digest(&b, &bi, None, NodeStatus::Ready, 1);
        { let at = rdm_core::PathName::new("mesh1.node", "2").unwrap(); db.node.path = at.path; db.node.name = at.name; }
        db.node.mesh = "mesh1".into();
        db.set_primary(Some(2));
        assert!(digest_view.record(db.clone()));
        assert_eq!(held(PrimaryRole::NodePrimary("node".into())).map(|h| (h.node_id, h.revision)), Some((a.clone(), 3)), "a lower revision replaces nothing");
        db.digest_seq += 1;
        db.set_primary(Some(4));
        assert!(digest_view.record(db));
        assert_eq!(held(PrimaryRole::NodePrimary("node".into())).map(|h| (h.node_id, h.revision)), Some((b, 4)), "a higher revision is the record held");
        assert_eq!(held(PrimaryRole::FabricPrimary).map(|h| (h.node_id, h.revision)), Some((a, 7)), "the fabric-primary record is its own");
    }

    /// CONTRACT (gossip.md "A primary change on gossip": "the node records it on its own row, and the
    /// mesh-primary's next `TopologyFull` projection carries it, its normal publication"; lifecycles/node-primary-update.md
    /// step 5; Q387: a member's own mesh reaches it by a read): a member of mesh1 holds the primary records
    /// its mesh-primary's read carries, its own birth's among them, though the read's copy of its own
    /// digest is older than the one it holds.
    /// Must NOT happen: a node whose mesh-primary named it the `node` primary holding no record
    /// of it.
    #[test]
    fn a_member_holds_the_primary_records_its_mesh_primarys_projection_carries() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = test_view("mesh1", "mesh1.node.1");
        let publisher = PublisherId { incarnation: IncarnationId::mint(), node_id: NodeId::mint() };
        // The member holds its mesh-primary from its entry read.
        view.digest_view.primaries.take(PrimaryRole::MeshPrimary, &Primary { mesh: "mesh1".into(), node_id: publisher.node_id.clone(), incarnation: publisher.incarnation.clone(), revision: 1 });
        let (me, my_inc) = (NodeId::parse("000000000002").unwrap(), IncarnationId::mint());
        let (other, other_inc) = (NodeId::parse("000000000003").unwrap(), IncarnationId::mint());
        assert!(view.digest_view.record(digest(&me, &my_inc, None, NodeStatus::Ready, 5)));
        let mut mine = digest(&me, &my_inc, None, NodeStatus::Ready, 3);
        mine.set_primary(Some(1));
        let mut theirs = digest(&other, &other_inc, None, NodeStatus::Ready, 1);
        { let at = rdm_core::PathName::new("mesh1.node", "2").unwrap(); theirs.node.path = at.path; theirs.node.name = at.name; }
        theirs.node.mesh = "mesh1".into();
        let read = crate::aggregate::Chunk { mesh: "mesh1".into(), mesh_id: None, author: None, topology_version: 1, time_ms: 1, chunk_index: 0, chunk_count: 1, digests: vec![mine, theirs], runtimes: Vec::new() };
        assert!(matches!(view.take_read_chunk(read, Side::Read, &fabric), Taken::Written(_)), "the read is installed");
        assert_eq!(view.digest_view.primaries.node_type_primary("mesh1", "node"), Some(Primary { mesh: "mesh1".into(), node_id: me, incarnation: my_inc, revision: 1 }), "the member holds the record its mesh-primary's read carries");
    }

    /// CONTRACT (node-recovery.md step 2: "Only the mesh-primary acts. ... For an unheard mesh-primary
    /// the fabric-primary runs it"): a node-admin the election replaced as its mesh's primary while it
    /// was unheard is an unheard mesh-primary; one heard after it was replaced is one more node-admin of
    /// its mesh, and its later silence is its mesh-primary's.
    /// Must NOT happen: a birth replaced by the election key while live, heard Ready again and then
    /// silent, judged an unheard mesh-primary by the mesh-primary's view and investigated by no one.
    #[test]
    fn a_replaced_mesh_primary_heard_again_is_one_more_node_admin_of_its_mesh() {
        let view = test_view("mesh1", "mesh1.node-admin.2");
        let (old, old_inc) = (NodeId::parse("000000000009").unwrap(), IncarnationId::mint());
        let (new, new_inc) = (NodeId::parse("000000000002").unwrap(), IncarnationId::mint());
        let held = |id: &NodeId, inc: &IncarnationId, revision| Primary { mesh: "mesh1".into(), node_id: id.clone(), incarnation: inc.clone(), revision };
        assert!(view.digest_view.record(digest(&old, &old_inc, None, NodeStatus::Ready, 1)));
        view.digest_view.primaries.take(PrimaryRole::MeshPrimary, &held(&old, &old_inc, 1));
        assert!(!view.digest_view.unheard_since_replaced("mesh1", old.as_str(), &old_inc, Instant::now()), "a birth never replaced is no unheard mesh-primary");
        std::thread::sleep(Duration::from_millis(2));
        view.digest_view.primaries.take(PrimaryRole::MeshPrimary, &held(&new, &new_inc, 2));
        assert!(view.digest_view.unheard_since_replaced("mesh1", old.as_str(), &old_inc, Instant::now()), "replaced and unheard since: the fabric-primary's");
        std::thread::sleep(Duration::from_millis(2));
        assert!(view.digest_view.record(digest(&old, &old_inc, None, NodeStatus::Ready, 2)));
        assert!(!view.digest_view.unheard_since_replaced("mesh1", old.as_str(), &old_inc, Instant::now()), "heard after it was replaced: its mesh-primary's");
        assert!(!view.digest_view.unheard_since_replaced("mesh1", old.as_str(), &IncarnationId::mint(), Instant::now()), "another birth at the node id was never replaced here");
    }

    fn digest(node_id: &NodeId, incarnation: &IncarnationId, supersedes: Option<IncarnationId>, status: NodeStatus, at: u64) -> MeshDigest {
        MeshDigest { 
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode { mesh: "mesh1".into(), r#type: "node".into(),
                node_id: node_id.clone(),
                path: "mesh1.node".into(), name: "1".into(),
                endpoint_id: EndpointId("key".into()),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation: incarnation.clone(),
                supersedes,
            },
            status,
            emitted_at_rafka_ms: at,
            digest_seq: at,
            primary: Vec::new(),
        }
    }

    /// Q271: the births the held digests name as replaced are read under the view's lock, the same set
    /// a copy of every digest gives.
    #[test]
    fn the_superseded_births_are_the_ones_every_held_digest_names() {
        let digest_view = DigestView::default();
        let (replaced, new) = (IncarnationId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&NodeId::mint(), &new, Some(replaced.clone()), NodeStatus::Ready, 10)));
        assert!(digest_view.record(digest(&NodeId::mint(), &IncarnationId::mint(), None, NodeStatus::Ready, 11)));
        let copied: Vec<IncarnationId> = digest_view.all().into_iter().filter_map(|d| d.node.supersedes).collect();
        assert_eq!(digest_view.superseded(), vec![replaced]);
        assert_eq!(digest_view.superseded(), copied, "the same set as a copy of every digest");
    }

    /// CONTRACT (gossip.md §3.1 "DOES bump / DOES NOT bump"; Q394: load and the gossip counters never
    /// ride gossip): the topology view the version orders drops a Digest's heartbeat order and stamp
    /// alone, so a later heartbeat of the same birth, status and primary roles is no topology change,
    /// and a changed status or primary role is one. Must NOT happen: every heartbeat moving the version.
    #[test]
    fn a_later_heartbeat_of_the_same_birth_is_no_topology_change_and_a_changed_status_or_role_is_one() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let first = digest(&id, &inc, None, NodeStatus::Ready, 1);
        let later = digest(&id, &inc, None, NodeStatus::Ready, 2);
        assert_ne!(first, later);
        assert_eq!(crate::aggregate::topology_view(&later), crate::aggregate::topology_view(&first), "a heartbeat is no topology change");
        let moved = digest(&id, &inc, None, NodeStatus::ConnectionsDeleting, 2);
        assert_ne!(crate::aggregate::topology_view(&moved), crate::aggregate::topology_view(&first), "a status change is one");
        let mut primary = later.clone();
        primary.set_primary(Some(1));
        assert_ne!(crate::aggregate::topology_view(&primary), crate::aggregate::topology_view(&first), "a primary role taken is one");
    }

    /// CONTRACT (rafka-v2 `register_peer_location_if_fresher`): a restart keeps the key and binds a
    /// fresh port; the newer birth's digest replaces the key's address, and a late digest of the
    /// old birth never puts the old socket back.
    #[test]
    fn a_newer_birth_replaces_its_keys_address_and_an_older_digest_never_restores_it() {
        let key = iroh::SecretKey::generate().public();
        // One logical node and two births of it: the second supersedes the first and starts its
        // own heartbeat sequence again at 1.
        let node_id = NodeId::mint();
        let (first, second) = (IncarnationId::mint(), IncarnationId::mint());
        let at = |port: u16, incarnation: &IncarnationId, supersedes: Option<IncarnationId>, seq: u64| {
            let mut d = digest(&node_id, incarnation, supersedes, NodeStatus::Ready, seq);
            d.node.endpoint_id = EndpointId(key.to_string());
            d.node.transport_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            d
        };
        let (marks, lookup) = (LocationMarks::default(), MemoryLookup::new());
        let addrs = |l: &MemoryLookup| l.get_endpoint_info(key).map(|e| e.ip_addrs().cloned().collect::<Vec<_>>()).unwrap_or_default();
        register_location(&marks, &lookup, &at(41001, &first, None, 40));
        register_location(&marks, &lookup, &at(41002, &second, Some(first.clone()), 1));
        assert_eq!(addrs(&lookup), vec![std::net::SocketAddr::from(([127, 0, 0, 1], 41002))], "the newer birth replaces, never joins, the old socket");
        register_location(&marks, &lookup, &at(41001, &first, None, 41));
        assert_eq!(addrs(&lookup), vec![std::net::SocketAddr::from(([127, 0, 0, 1], 41002))], "a late digest of the old birth never restores its socket");
    }

    /// CONTRACT (ops-naming, `node.deleted`): a new node over the data dir of a deleted one keeps
    /// the endpoint key and sets no `supersedes`, so the location view cannot tell the deleted
    /// instance's late digest from a newer one. The departure of `(node.id, incarnation)` targets
    /// that incarnation in the view: its digest is refused by name and the key keeps the new
    /// instance's socket. Must NOT happen: the key pointed back at the deleted instance's old port.
    #[test]
    fn a_deleted_incarnations_late_digest_never_moves_its_keys_address_back() {
        let key = iroh::SecretKey::generate().public();
        let (old_node, new_node) = (NodeId::mint(), NodeId::mint());
        let (old, new) = (IncarnationId::mint(), IncarnationId::mint());
        let at = |node: &NodeId, port: u16, incarnation: &IncarnationId, seq: u64| {
            let mut d = digest(node, incarnation, None, NodeStatus::Ready, seq);
            d.node.endpoint_id = EndpointId(key.to_string());
            d.node.transport_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            d
        };
        let (marks, lookup) = (LocationMarks::default(), MemoryLookup::new());
        let addrs = |l: &MemoryLookup| l.get_endpoint_info(key).map(|e| e.ip_addrs().cloned().collect::<Vec<_>>()).unwrap_or_default();
        let (port_old, port_new) = (std::net::SocketAddr::from(([127, 0, 0, 1], 41011)), std::net::SocketAddr::from(([127, 0, 0, 1], 41012)));
        register_location(&marks, &lookup, &at(&old_node, 41011, &old, 40));
        register_location(&marks, &lookup, &at(&new_node, 41012, &new, 1));
        assert_eq!(addrs(&lookup), vec![port_new], "the new instance's digest replaces the key's address");
        let digest_view = DigestView::default();
        assert!(digest_view.depart(op(&old_node, &old, "node.delete")));
        assert!(register_location(&marks, &lookup, &at(&old_node, 41011, &old, 41)).is_none());
        assert_eq!(addrs(&lookup), vec![port_new], "a digest of the deleted incarnation is refused: {port_old} is not restored");
    }

    /// CONTRACT (deployment-provider.md: "Changing the locator or control domain for the same
    /// incarnation is a named inconsistency and refusal"; Q387: a runtime is taken from the join report,
    /// a topology read or the node's own launch, never from gossip): a birth's runtime held once stays
    /// its runtime; another locator or control domain for the same incarnation is refused by name and
    /// its data dir, unknown until then, is added; a restart's new birth brings a new runtime, and the
    /// superseded birth's never returns. Must NOT happen: a held birth's runtime replaced by another.
    #[test]
    fn a_birth_keeps_its_runtime_and_a_restart_brings_a_new_one() {
        use rdm_core::{NodeRuntime, RuntimeFact, RuntimeLocator, RuntimeProvider};
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fact = |pid: u32, domain: &str| RuntimeFact {
            deployment_id: rdm_core::fixture_crockford60("dep"),
            provider: RuntimeProvider::Process,
            control_domain: domain.into(),
            locator: RuntimeLocator::Process { pid, start: 7 },
        };
        let of = |f: RuntimeFact, dir: Option<&str>| NodeRuntime { runtime: Some(f), data_dir: dir.map(String::from) };
        let digest_view = DigestView::default();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        let first = digest(&id, &birth, None, NodeStatus::Ready, 100);
        assert!(digest_view.record(first.clone()));
        assert!(digest_view.hold_runtime(&id, &birth, of(fact(11, "process:a:1"), None)));
        // Same birth, another locator or another control domain: refused by name, the held one stays.
        for other in [fact(12, "process:a:1"), fact(11, "process:b:1")] {
            assert!(!digest_view.hold_runtime(&id, &birth, of(other.clone(), Some("/data/other"))), "{other:?}");
        }
        assert_eq!(digest_view.runtime_of(&id, &birth), Some(of(fact(11, "process:a:1"), None)));
        let refused = spans.named("rdm.mesh.runtime.reject.via-locator-changed");
        assert_eq!(refused.len(), 2, "each other runtime refused by name: {refused:?}");
        assert!(refused.iter().all(|f| f["node_id"] == id.to_string() && f["incarnation_id"] == birth.as_str()));
        // The same runtime again adds the data dir the held birth lacked.
        assert!(digest_view.hold_runtime(&id, &birth, of(fact(11, "process:a:1"), Some("/data/a"))));
        assert_eq!(digest_view.runtime_of(&id, &birth), Some(of(fact(11, "process:a:1"), Some("/data/a"))));
        // A restart: a new birth with a new runtime is current; the old birth's fact never returns.
        let next = IncarnationId::mint();
        assert!(digest_view.record(digest(&id, &next, Some(birth.clone()), NodeStatus::Ready, 300)));
        assert!(digest_view.hold_runtime(&id, &next, of(fact(13, "process:a:1"), None)));
        assert!(!digest_view.record(first), "the superseded birth is refused");
        assert!(!digest_view.hold_runtime(&id, &birth, of(fact(11, "process:a:1"), Some("/data/a"))), "and so is its runtime");
        assert_eq!(digest_view.runtime_of(&id, &next), Some(of(fact(13, "process:a:1"), None)));
        assert_eq!(digest_view.runtime_of(&id, &birth), None, "the old birth's runtime is not held");
    }

    /// CONTRACT (gossip.md §3.1 `Digest`: "a node-admin takes them from join reports, topology reads and
    /// node records"; Q400, Q404 A): at a node-admin, a Digest of a birth of its mesh it holds no runtime
    /// fact of is taken, names the gap `no-runtime-fact` on its own mesh with one span and signals the
    /// top-up, a read of its mesh-primary's view, or, with no live primary to read from, of the birth's
    /// node record; each further such Digest signals it again; once a read holds the birth's runtime
    /// fact, its Digests signal nothing. A data dir alone is no runtime fact. The mesh-primary signals
    /// too: a birth whose join another primary took has its fact only in its node record. A node that is
    /// no node-admin and a birth held leaving signal nothing. Must NOT happen: a node-admin holding a
    /// birth with no runtime fact that never reads it again, so its not-ready check names "no runtime
    /// fact is held of it" until its Build's WaitForNodeReady ends (gate 103bbe528: twelve cells,
    /// fabric_states `a_two_mesh_fabric_is_ready_once_every_mesh_is_ready` mesh1.node-admin.2 among
    /// them; gate c5a174e33: a restarted lone fabric-primary, mesh-primary of its mesh, in node_own_config).
    #[test]
    fn a_node_admin_reads_its_mesh_primarys_view_again_at_each_digest_of_a_birth_it_holds_no_runtime_fact_of() {
        use rdm_core::{NodeRuntime, RuntimeFact, RuntimeLocator, RuntimeProvider};
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        // A view whose own birth, as its own Digest holds it, is of `r#type`.
        let view_of = |r#type: &str| {
            let view = test_view("mesh1", "mesh1.node-admin.2");
            let mut own = digest(&NodeId::mint(), &IncarnationId::mint(), None, NodeStatus::Pending, 1);
            (own.node.r#type, own.node.path, own.node.name) = (r#type.into(), format!("mesh1.{}", r#type), "2".into());
            *view.digest_view.own.lock().unwrap() = Some(own.node.node_id.to_string());
            assert!(view.digest_view.record(own));
            view
        };
        let signalled = |view: &super::View| rt.block_on(async { tokio::time::timeout(Duration::from_millis(20), view.desync.notified()).await.is_ok() });
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let at = |seq: u64, status: NodeStatus| {
            let mut d = digest(&id, &inc, None, status, seq);
            d.node.name = "7".into();
            d
        };
        let view = view_of("node-admin");
        for seq in [1, 2] {
            let carried = view.take(&Frame::digest(&at(seq, NodeStatus::Ready)), &fabric, "mesh-channel");
            assert_eq!(carried.len(), 1, "Digest {seq} of the birth is taken");
            assert!(signalled(&view), "Digest {seq} of a birth with no runtime fact signals the top-up");
        }
        assert_eq!(view.copies.lock().unwrap().desynced(), vec![("mesh1".to_string(), Gap::NoRuntimeFact)], "the gap is named on its own mesh");
        assert_eq!(view.digest_view.without_runtime_fact("mesh1"), vec![(id.clone(), inc.clone())], "the birth is owed its runtime fact");
        let raised = spans.named("rdm.mesh.topology.update.via-no-runtime-fact");
        assert_eq!(raised.len(), 1, "one span per gap: {raised:?}");
        assert_eq!((raised[0]["node_id"].as_str(), raised[0]["digest_seq"].as_str()), (id.as_str(), "1"));
        assert_eq!(Gap::NoRuntimeFact.to_string(), "no-runtime-fact", "the top-up's reasons name it");
        // A data dir alone is no runtime fact.
        assert!(view.digest_view.hold_runtime(&id, &inc, NodeRuntime { runtime: None, data_dir: Some("/data/7".into()) }));
        view.take(&Frame::digest(&at(3, NodeStatus::Ready)), &fabric, "mesh-channel");
        assert!(signalled(&view), "a data dir alone leaves the fact owed");
        // A read supplies the fact: the birth's Digests signal nothing more.
        let fact = RuntimeFact { deployment_id: rdm_core::fixture_crockford60("dep"), provider: RuntimeProvider::Process, control_domain: "process:a:1".into(), locator: RuntimeLocator::Process { pid: 7, start: 7 } };
        assert!(view.digest_view.hold_runtime(&id, &inc, NodeRuntime { runtime: Some(fact), data_dir: None }));
        assert!(view.digest_view.without_runtime_fact("mesh1").is_empty(), "nothing is owed once the fact is held");
        view.take(&Frame::digest(&at(4, NodeStatus::Ready)), &fabric, "mesh-channel");
        assert!(!signalled(&view), "a birth whose runtime fact is held signals nothing");
        // The mesh-primary signals too (Q404 A: it reads the birth's node record).
        let primary = view_of("node-admin");
        primary.primary.store(true, Ordering::Relaxed);
        assert_eq!(primary.take(&Frame::digest(&at(1, NodeStatus::Ready)), &fabric, "mesh-channel").len(), 1, "the mesh-primary takes the birth");
        assert!(signalled(&primary), "the mesh-primary: a birth with no runtime fact signals the top-up");
        assert_eq!(primary.copies.lock().unwrap().desynced(), vec![("mesh1".to_string(), Gap::NoRuntimeFact)], "the mesh-primary: the gap is named on its own mesh");
        // A node that is no node-admin and a birth held leaving signal nothing.
        let node = view_of("node");
        let leaving = view_of("node-admin");
        for (what, v, status) in [("a node that is no node-admin", &node, NodeStatus::Ready), ("a birth held leaving", &leaving, NodeStatus::Leaving)] {
            assert_eq!(v.take(&Frame::digest(&at(1, status)), &fabric, "mesh-channel").len(), 1, "{what}: the birth is taken");
            assert!(!signalled(v), "{what}: nothing is signalled");
            assert!(v.copies.lock().unwrap().desynced().is_empty(), "{what}: no gap");
        }
        assert_eq!(spans.named("rdm.mesh.topology.update.via-no-runtime-fact").len(), 2, "one span per gap, the mesh-primary's included");
    }

    /// CONTRACT (gossip.md §3.1 `Digest`: "a node-admin takes them from join reports, topology reads and
    /// node records"; Q404 A: "The node-admin reads the missing runtime directly from that node's record
    /// in `node-storage`. It does not contact the node."): a node-admin with no live primary to read
    /// from (its primary record names a birth proven gone, or it is its mesh's primary) holds each birth
    /// of its mesh it holds with no runtime fact with the runtime and data dir of that birth's node
    /// record, one span per birth naming it and `source=node-storage`; a birth whose record is not
    /// stored, or could not be read, stays owed and is read again; the `no-runtime-fact` gap on its own
    /// mesh ends once none is owed, and a gap of another cause stays until a read. A birth whose fact is
    /// held is not read; a node that is no node-admin reads nothing. Must NOT happen: a recovering admin
    /// of a lost mesh, or a restarted lone fabric-primary, holding births whose runtime facts are only in
    /// their node records, its not-ready check naming "no runtime fact is held of it" until it times out
    /// (gate c5a174e33: node_records, fabric_delete, recovery_admin_adopts, node_own_config,
    /// declared_secrets).
    #[test]
    fn a_node_admin_with_no_live_primary_takes_a_births_runtime_fact_from_its_node_record() {
        use rdm_core::{NodeRuntime, RuntimeFact, RuntimeLocator, RuntimeProvider};
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        // The topology view's own task runs on this runtime.
        let _entered = rt.enter();
        let fact = |pid: u32| RuntimeFact { deployment_id: rdm_core::fixture_crockford60("dep"), provider: RuntimeProvider::Process, control_domain: "process:a:1".into(), locator: RuntimeLocator::Process { pid, start: 7 } };
        // A topology view whose own birth, as its own Digest holds it, is of `r#type`.
        let view_of = |r#type: &str| {
            let m = TopologyView::new(&fabric, "mesh1", &MeshId::mint(), "mesh1.node-admin.2", rdm_transport::clock::os_clock(), Default::default());
            m.view.forwarder.lock().unwrap().set_author(test_author());
            let mut own = digest(&NodeId::mint(), &IncarnationId::mint(), None, NodeStatus::Pending, 1);
            (own.node.r#type, own.node.path, own.node.name) = (r#type.into(), format!("mesh1.{}", r#type), "2".into());
            *m.digest_view.own.lock().unwrap() = Some(own.node.node_id.to_string());
            assert!(m.digest_view.record(own));
            m
        };
        let birth = |name: &str| {
            let (id, inc) = (NodeId::mint(), IncarnationId::mint());
            let mut d = digest(&id, &inc, None, NodeStatus::Ready, 1);
            d.node.name = name.into();
            (id, inc, d)
        };
        // node-storage: each node's record by its id; a birth with none is not stored.
        let store: Arc<Mutex<HashMap<String, Result<NodeRuntime, String>>>> = Default::default();
        let asked: Arc<Mutex<Vec<Vec<String>>>> = Default::default();
        let read: NodeRecordRead = {
            let (store, asked) = (store.clone(), asked.clone());
            Arc::new(move |births: Vec<(NodeId, IncarnationId)>| {
                let (store, asked) = (store.clone(), asked.clone());
                Box::pin(async move {
                    let mut ids: Vec<String> = births.iter().map(|(id, _)| id.to_string()).collect();
                    ids.sort();
                    asked.lock().unwrap().push(ids);
                    let answers: Vec<RecordedRuntime> = births.into_iter().map(|(id, inc)| { let answer = store.lock().unwrap().get(id.as_str()).cloned().transpose(); (id, inc, answer) }).collect();
                    answers
                })
            })
        };
        let sorted = |ids: &[&NodeId]| { let mut v: Vec<String> = ids.iter().map(|i| i.to_string()).collect(); v.sort(); v };
        let m = view_of("node-admin");
        let ((a, a_inc, a_d), (b, b_inc, b_d), (c, c_inc, c_d), (d, d_inc, d_d)) = (birth("1"), birth("2"), birth("3"), birth("4"));
        for x in [&a_d, &b_d, &c_d, &d_d] {
            assert_eq!(m.view.take(&Frame::digest(x), &fabric, "mesh-channel").len(), 1, "the birth is taken");
        }
        assert_eq!(m.desynced_sources(), vec![("mesh1".to_string(), Gap::NoRuntimeFact)], "the gap is named on its own mesh");
        // c's fact is held (its join report): it is not read.
        assert!(m.digest_view.hold_runtime(&c, &c_inc, NodeRuntime { runtime: Some(fact(3)), data_dir: None }));
        store.lock().unwrap().insert(a.to_string(), Ok(NodeRuntime { runtime: Some(fact(1)), data_dir: Some("/data/a".into()) }));
        store.lock().unwrap().insert(d.to_string(), Err("the store did not answer".into()));

        // a's record holds its fact; b's is not stored yet; d's cannot be read: read again.
        assert!(rt.block_on(m.runtimes_from_node_records(&read, "primary-gone")), "b and d are still owed: read again");
        assert_eq!(asked.lock().unwrap().clone(), vec![sorted(&[&a, &b, &d])], "only the births owed a fact are read");
        assert_eq!(m.digest_view.runtime_of(&a, &a_inc), Some(NodeRuntime { runtime: Some(fact(1)), data_dir: Some("/data/a".into()) }), "a is held with its record's runtime and data dir");
        let taken = spans.named("rdm.mesh.runtime.update.via-node-record");
        assert_eq!(taken.len(), 1, "{taken:?}");
        assert_eq!(
            (taken[0]["node_id"].as_str(), taken[0]["incarnation_id"].as_str(), taken[0]["source"].as_str(), taken[0]["reason"].as_str(), taken[0]["held"].as_str()),
            (a.as_str(), a_inc.as_str(), "node-storage", "primary-gone", "true"),
            "the span names the birth and its source"
        );
        let missing = spans.named("rdm.mesh.runtime.reject.via-no-node-record");
        assert_eq!((missing.len(), missing[0]["node_id"].as_str(), missing[0]["source"].as_str()), (1, b.as_str(), "node-storage"));
        let unreadable = spans.named("rdm.mesh.runtime.reject.via-node-record-unreadable");
        assert_eq!((unreadable.len(), unreadable[0]["node_id"].as_str(), unreadable[0]["error"].as_str()), (1, d.as_str(), "the store did not answer"));
        assert_eq!(m.desynced_sources(), vec![("mesh1".to_string(), Gap::NoRuntimeFact)], "the gap stays while a fact is owed");

        // b's record is stored and d's is readable now: both are held, and the gap ends.
        store.lock().unwrap().insert(b.to_string(), Ok(NodeRuntime { runtime: Some(fact(2)), data_dir: None }));
        store.lock().unwrap().insert(d.to_string(), Ok(NodeRuntime { runtime: Some(fact(4)), data_dir: None }));
        assert!(!rt.block_on(m.runtimes_from_node_records(&read, "this-node-is-the-mesh-primary")), "nothing is owed");
        assert_eq!(asked.lock().unwrap().last().cloned(), Some(sorted(&[&b, &d])));
        assert!(m.digest_view.without_runtime_fact("mesh1").is_empty(), "every birth's fact is held");
        assert_eq!(m.digest_view.runtime_of(&b, &b_inc).and_then(|r| r.runtime), Some(fact(2)));
        assert_eq!(m.digest_view.runtime_of(&d, &d_inc).and_then(|r| r.runtime), Some(fact(4)));
        assert!(m.desynced_sources().is_empty(), "the no-runtime-fact gap ends once none is owed");
        let taken = spans.named("rdm.mesh.runtime.update.via-node-record");
        assert_eq!(taken.len(), 3);
        assert!(taken[1..].iter().all(|s| s["reason"] == "this-node-is-the-mesh-primary" && s["source"] == "node-storage"), "{taken:?}");

        // Nothing owed: nothing is read. A gap of another cause stays until a read installs the mesh.
        m.view.copies.lock().unwrap().mark_desynced("mesh1", Gap::Version { held: 1, base: 3 });
        assert!(!rt.block_on(m.runtimes_from_node_records(&read, "primary-gone")));
        assert_eq!(asked.lock().unwrap().len(), 2, "no birth is owed: no record is read");
        assert_eq!(m.desynced_sources(), vec![("mesh1".to_string(), Gap::Version { held: 1, base: 3 })], "a version gap is not ended by a record read");

        // A node that is no node-admin manages no runtime: it reads nothing.
        let node = view_of("node");
        let (_, _, e_d) = birth("5");
        assert_eq!(node.view.take(&Frame::digest(&e_d), &fabric, "mesh-channel").len(), 1);
        assert!(!rt.block_on(node.runtimes_from_node_records(&read, "primary-gone")));
        assert_eq!(asked.lock().unwrap().len(), 2, "a node that is no node-admin reads no record");
    }

    #[test]
    fn a_late_digest_never_refreshes_a_member_or_reverts_its_status() {
        let digest_view = DigestView::default();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        let t0 = Instant::now();
        let later = t0 + Duration::from_millis(30);
        assert!(digest_view.record_at(digest(&id, &birth, None, NodeStatus::Leaving, 200), t0));
        assert!(!digest_view.record_at(digest(&id, &birth, None, NodeStatus::Ready, 100), later), "an older digest of the same birth");
        assert!(!digest_view.record_at(digest(&id, &birth, None, NodeStatus::Ready, 200), later), "a duplicate");
        let (held, age) = digest_view.get_at(id.as_str(), later).unwrap();
        assert_eq!(held.status, NodeStatus::Leaving, "the status is not reverted");
        assert_eq!(age, Duration::from_millis(30), "the member's unheard time is not reset");
        assert!(digest_view.record_at(digest(&id, &birth, None, NodeStatus::Leaving, 300), later), "a newer digest is taken");
    }

    /// `publisher`'s exact birth in the tests' fabric: the author of an aggregate.
    fn author_of(publisher: &PublisherId) -> crate::envelope::Birth {
        crate::envelope::Birth::publisher(&FabricId::parse("fab000000001").unwrap(), publisher)
    }

    /// `mesh`'s aggregate listing `nodes`, one chunk, authored by one fixed mesh-primary at version 1.
    fn mesh_topology(mesh: &str, nodes: Vec<MeshDigest>) -> Frame {
        let publisher = PublisherId { incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() };
        Frame::aggregate(mesh, None, &author_of(&publisher), 1, 1, 0, 1, nodes)
    }

    /// `d` as an aggregate lists it with `status` (Q390: a stop, a removal or a departure is the
    /// node's status).
    fn listed(d: &MeshDigest, status: NodeStatus) -> MeshDigest {
        MeshDigest { status, ..d.clone() }
    }

    fn op(id: &NodeId, inc: &IncarnationId, action: &str) -> LifecycleOp {
        LifecycleOp { r#type: "node".into(), mesh: "mesh1".into(),
            build_id: "b1".into(),
            operation: rdm_core::OperationId::new(action, rdm_core::OperationSubject::Birth { node_id: id.clone(), incarnation: inc.clone() }),
            node_id: id.clone(),
            incarnation: inc.clone(),
            path: "mesh1.node".into(), name: "1".into(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        }
    }

    /// `mesh`'s status as its primary published it, held by `digest_view` (the frame a peer receives).
    fn hold_mesh_status(digest_view: &DigestView, mesh: &str, status: &str, changed_at_rafka_ms: u64) {
        let f = Frame::MeshStatus { mesh: mesh.into(), status: status.into(), publisher: format!("{mesh}.node-admin.1"), forwarded_by: None, changed_at_rafka_ms };
        assert!(digest_view.statuses.take(&f, &FabricId::parse("fab000000001").unwrap()), "{f:?}");
    }

    /// CONTRACT (states.md "Routing"): application routing selects a birth only while it is
    /// ready and its mesh is ready; anything else is a routing stop, and a
    /// later ready for the same exact birth restores selection. A `Pending` birth of a
    /// ready mesh is not selected; its own ready selects it; its `ConnectionsDeleting` stops it; the
    /// same birth saying ready again selects it again. Must NOT happen: a `Pending`
    /// birth selected, or a stop that outlives the birth's later ready.
    #[test]
    fn a_birth_is_routable_only_while_it_says_ready_and_a_later_ready_restores_it() {
        let digest_view = DigestView::default();
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Pending, 100)));
        assert!(!digest_view.routable(id.as_str()), "a Pending birth is a routing stop");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 110)));
        assert!(digest_view.routable(id.as_str()), "the birth and its mesh are ready");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::ConnectionsDeleting, 120)));
        assert!(!digest_view.routable(id.as_str()), "a ConnectionsDeleting birth is a routing stop");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 130)), "the same birth's later ready is taken");
        assert!(digest_view.routable(id.as_str()), "a routing stop is no barrier: the later ready restores selection");
    }

    /// CONTRACT (states.md "Routing"): a ready birth is selected only while its mesh's
    /// status this node holds is ready. No mesh status held, a mesh `pending`, `leaving`
    /// or `dead`, or another mesh's ready: not selected. The mesh's later
    /// ready restores selection from the moment it is held. Must NOT happen: a birth of a
    /// mesh not held ready selected.
    #[test]
    fn a_ready_birth_is_routable_only_while_its_mesh_is_held_ready() {
        let digest_view = DigestView::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 100)));
        assert!(!digest_view.routable(id.as_str()), "no status of mesh1 is held: mesh1 is not held ready");
        hold_mesh_status(&digest_view, "mesh2", "ready", 5);
        assert!(!digest_view.routable(id.as_str()), "another mesh's ready selects nothing in mesh1");
        hold_mesh_status(&digest_view, "mesh1", "pending", 10);
        assert!(!digest_view.routable(id.as_str()), "a pending mesh is a routing stop");
        hold_mesh_status(&digest_view, "mesh1", "ready", 20);
        assert!(digest_view.routable(id.as_str()), "the mesh's ready, received, restores selection");
        hold_mesh_status(&digest_view, "mesh1", "leaving", 30);
        assert!(!digest_view.routable(id.as_str()), "a leaving mesh is a routing stop on receipt, whatever its member says");
        hold_mesh_status(&digest_view, "mesh1", "dead", 40);
        assert!(!digest_view.routable(id.as_str()), "a dead mesh is a routing stop");
    }

    /// CONTRACT (states.md "Routing", gossip.md MeshStatus): the mesh primary that observes its mesh
    /// leaving sends that change at once on the existing MeshStatus send, and a peer that takes the
    /// frame stops selecting the mesh's births on receipt, whatever each member still says. Must NOT
    /// happen: the change waiting for anything, or a peer still selecting a birth of the leaving mesh.
    #[test]
    fn a_mesh_primary_observing_its_mesh_leaving_sends_it_at_once_and_a_peer_stops_routing_on_receipt() {
        let (now, fabric) = (Instant::now(), FabricId::parse("fab000000001").unwrap());
        let mut publisher = StatusPublisher::new("mesh1.node-admin.1", StatusScope::Mesh("mesh1".into()));
        publisher.set_role(true);
        let peer = DigestView::default();
        let held = StatusFact { status: "ready".into(), changed_at_rafka_ms: 10 };
        let ready = publisher.observe("ready", Some(held), true, now, 10).expect("the held ready is sent once its checklist completes");
        assert!(peer.statuses.take(&ready, &fabric));
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(peer.record(digest(&id, &inc, None, NodeStatus::Ready, 100)));
        assert!(peer.routable(id.as_str()));
        let leaving = publisher.observe("leaving", None, true, now, 20).expect("the mesh's leaving is a change, sent at once");
        assert!(matches!(&leaving, Frame::MeshStatus { status, publisher, .. } if status == "leaving" && publisher == "mesh1.node-admin.1"), "{leaving:?}");
        assert!(peer.statuses.take(&leaving, &fabric));
        assert!(!peer.routable(id.as_str()), "a peer holding the mesh leaving selects none of its births, though the member still says ready");
    }

    /// Whether `w` says a routing fact moved, without waiting.
    async fn moved_now(w: &mut RoutingWatch) -> bool {
        tokio::select! {
            biased;
            _ = w.moved() => true,
            _ = std::future::ready(()) => false,
        }
    }

    /// CONTRACT (states.md "Routing": a stop is applied on receipt): the view's routing watch moves
    /// when a mesh status is taken, a held birth says another status, or an overlay opens; a repeat
    /// of the held mesh status moves nothing. Must NOT happen: a mesh status that changes what routing
    /// selects taken silently, so a view waits for its poll to apply it.
    #[tokio::test]
    async fn a_mesh_status_taken_moves_the_views_routing_watch() {
        let (digest_view, fabric) = (DigestView::default(), FabricId::parse("fab000000001").unwrap());
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 100)));
        let mut w = digest_view.routing_watch();
        w.seen();
        assert!(!moved_now(&mut w).await, "nothing moved yet");
        let f = Frame::MeshStatus { mesh: "mesh1".into(), status: "leaving".into(), publisher: "mesh1.node-admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 };
        assert!(digest_view.statuses.take(&f, &fabric));
        assert!(moved_now(&mut w).await, "a mesh status taken moves the routing watch");
        w.seen();
        assert!(!digest_view.statuses.take(&f, &fabric), "the same change again is taken once");
        assert!(!moved_now(&mut w).await, "a repeat moves nothing");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::ConnectionsDeleting, 110)));
        assert!(moved_now(&mut w).await, "a birth saying another status moves it");
    }

    #[test]
    fn a_deleting_overlay_keeps_the_node_held_and_not_routable_and_a_departure_removes_it() {
        let digest_view = DigestView::default();
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 100)));
        assert!(digest_view.routable(id.as_str()));
        let o = op(&id, &inc, "node.delete");
        assert!(digest_view.deleting(o.clone()));
        assert!(!digest_view.deleting(o.clone()), "the same overlay twice is applied once");
        assert!(digest_view.get(id.as_str()).is_some(), "still held");
        assert!(!digest_view.routable(id.as_str()), "not routable");
        assert_eq!(digest_view.in_flight(), vec![o.clone()]);
        assert!(digest_view.depart(o.clone()));
        assert!(digest_view.get(id.as_str()).is_none(), "removed from the view");
        assert!(digest_view.in_flight().is_empty(), "the overlay cleared with the departure");
        assert_eq!(digest_view.deleted(), vec![o.clone()]);
        assert!(!digest_view.depart(o), "a second copy of the departure is a no-op");
    }

    /// The mesh-primary's report-taking of `node.started` of the birth `inc` of `id`: its
    /// DeclareNodeState(Ready) applied by its StatusAuthority.
    fn node_started(digest_view: &DigestView, id: &NodeId, inc: &IncarnationId) {
        digest_view.started(id, inc, &rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1");
    }

    /// CONTRACT (node-status-heartbeat-connections.md §A.3: "the successor acquires the held member
    /// statuses and publishes a complete projection under its own revision before any delta";
    /// fabric-authority.md §7: a takeover hydrates, it is not a transition; Q387: a status is held
    /// only with the node's Digest): the view of mesh1's new primary adopts a held status onto an
    /// own-mesh birth whose Digest it holds, and leaves a birth it does not hold to its own Digest. Nothing is adopted, each by its named reason, for a birth of
    /// another mesh, a predecessor at the holder's own (path, name), a deleted birth, or another birth of
    /// a node whose later birth the digest view holds. Must NOT happen: an adoption moving a birth the digest view
    /// holds of a node at another incarnation, or a peer mesh's member.
    #[test]
    fn a_new_mesh_primarys_view_adopts_held_statuses_of_its_own_mesh_births_only() {
        let digest_view = DigestView::default();
        *digest_view.own_mesh.lock().unwrap() = "mesh1".into();
        *digest_view.observer_name.lock().unwrap() = "mesh1.node-admin.1".into();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert_eq!(digest_view.adopt_status(&id, &inc, "mesh1", NodeStatus::Ready), Ok(None), "a birth not held: nothing to adopt onto");
        assert_eq!(digest_view.member_status(&id, &inc), None, "no status without the node's Digest");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 10)));
        assert_eq!(digest_view.adopt_status(&id, &inc, "mesh1", NodeStatus::Ready), Ok(Some(NodeStatus::Ready)), "held: the row's status, the same as its Digest's");
        assert_eq!(digest_view.member_status(&id, &inc), Some(NodeStatus::Ready));
        assert_eq!(digest_view.adopt_status(&NodeId::mint(), &IncarnationId::mint(), "mesh2", NodeStatus::Ready), Err("another-mesh"));
        // The holder's own predecessor is known by its exact birth, never by a path it stood at.
        let predecessor = IncarnationId::mint();
        *digest_view.own_predecessor.lock().unwrap() = Some(predecessor.clone());
        assert_eq!(digest_view.adopt_status(&NodeId::mint(), &predecessor, "mesh1", NodeStatus::Ready), Err("own-predecessor-birth"));
        assert_eq!(digest_view.adopt_status(&id, &IncarnationId::mint(), "mesh1", NodeStatus::Leaving), Err("another-birth-held"), "an earlier birth of a node held at a later one");
        assert_eq!(digest_view.member_status(&id, &inc), Some(NodeStatus::Ready), "and the held birth's status stands");
        let (gone, gone_inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.depart(op(&gone, &gone_inc, "node.delete")));
        assert_eq!(digest_view.adopt_status(&gone, &gone_inc, "mesh1", NodeStatus::Ready), Err("deleted"));
    }

    /// CONTRACT (gossip.md "Node-stopped lifecycle overlay"; node-start.md step 2): a `NodeStopped`
    /// for the exact birth drops it from the view and keeps it unroutable; no digest of the stopped
    /// birth brings it back, whatever its status or sequence and whichever `via` it arrives through
    /// (a late `ConnectionsDeleting`, an older heartbeat, a pending one with a higher sequence). Only the
    /// mesh-primary's `node.joined` ends the stop: the birth is held again `Pending`, as the same
    /// birth, and its later heartbeat moves on from there; a departure ends the stop for good.
    #[test]
    fn a_stopped_birth_leaves_the_view_and_only_node_joined_brings_it_back() {
        let digest_view = DigestView::default();
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 100)));
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::ConnectionsDeleting, 110)));
        let o = op(&id, &inc, "node.stop");
        assert!(digest_view.stopped(o.clone()));
        assert!(!digest_view.stopped(o.clone()), "the same event twice is applied once");
        assert!(digest_view.get(id.as_str()).is_none(), "dropped from the view");
        assert!(!digest_view.routable(id.as_str()));
        assert_eq!(digest_view.in_flight(), vec![o.clone()]);
        assert!(digest_view.deleted().is_empty(), "a stop departs nothing");
        for late in [digest(&id, &inc, None, NodeStatus::ConnectionsDeleting, 111), digest(&id, &inc, None, NodeStatus::Ready, 105), digest(&id, &inc, None, NodeStatus::Pending, 109), digest(&id, &inc, None, NodeStatus::Pending, 120)] {
            assert!(!digest_view.record(late.clone()), "{late:?}");
            assert!(!digest_view.record_forwarded(late.clone()), "{late:?}");
            assert!(!digest_view.record_topology(late.clone()), "{late:?}");
        }
        assert!(digest_view.get(id.as_str()).is_none(), "no digest brought it back");
        assert_eq!(digest_view.in_flight(), vec![o.clone()], "the stop stands");
        assert!(!digest_view.joined(&op(&id, &IncarnationId::mint(), "node.start")), "node.joined of another birth ends nothing");
        assert!(digest_view.joined(&op(&id, &inc, "node.start")), "the mesh-primary's node.joined ends the stop");
        let (held, _) = digest_view.get(id.as_str()).expect("held again");
        assert_eq!((held.node.incarnation.clone(), held.status, held.digest_seq), (inc.clone(), NodeStatus::Pending, 120), "the same birth, Pending, from the latest heartbeat heard of it");
        assert!(digest_view.in_flight().is_empty(), "node.joined closes the stop");
        assert!(!digest_view.routable(id.as_str()), "the joined birth is Pending: a routing stop until it is ready");
        assert!(!digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 118)), "a heartbeat older than the one it is held from is not taken");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 125)));
        assert!(digest_view.routable(id.as_str()), "its ready restores selection");
        // A departure ends a stop for good.
        // A later stop of the same birth is another occurrence of node.stop (a Build has no attempts, Q344).
        let o2 = op(&id, &inc, "node.stop");
        assert!(digest_view.stopped(LifecycleOp { operation: o2.operation.clone().in_occurrence(rdm_core::OperationOccurrenceId::mint()), ..o2 }));
        assert!(digest_view.depart(op(&id, &inc, "node.delete")));
        assert!(digest_view.in_flight().is_empty(), "the departure cleared the stop overlay");
        assert!(!digest_view.record(digest(&id, &inc, None, NodeStatus::Pending, 400)), "a deleted node never returns");
    }

    /// CONTRACT (gossip.md §3.1 `TopologyFull`; R-HB1): a non-primary node-admin of mesh1 takes its own
    /// mesh's statuses from its primary's backbone mesh topology, and a stop's end only when that
    /// projection moves, in order, past the stop. Holding mesh1.node.1 stopped from the
    /// primary's event, which carries v2 (the version its stop changed the birth at): v1
    /// again (a refresh at the version held, built before the stop) is refused by name; v2 carries
    /// the stop; v3 lists neither (the primary's stop cleared before it held the birth again) and
    /// ends nothing; v2 again (earlier than the version held) is ignored by the receive path (R-RX3); v4
    /// lists the birth `Pending` and ends the stop, carried at v2; the birth is held again from the
    /// latest heartbeat of it refused while the stop stood, with that status. The heartbeat of the
    /// birth then gives liveness and nothing of its status, which is the projection's `Pending`, then its
    /// `Ready`. Must NOT happen: a stop ended by a projection that never carried it, by a
    /// refresh, or by a heartbeat; an own-mesh status taken from a heartbeat.
    #[test]
    fn a_non_primary_admin_ends_a_stop_only_when_its_primarys_projection_moves_past_it() {
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = test_view("mesh1", "mesh1.node-admin.2");
        let publisher = PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let b = |status, seq| digest(&id, &inc, None, status, seq);
        let stop = op(&id, &inc, "node.stop");
        // A stop the aggregate carries is the node listed stopped (Q390).
        let mesh_topology = |version: u64, mut digests: Vec<MeshDigest>, stopped: Vec<LifecycleOp>| {
            digests.extend(stopped.iter().map(|_| listed(&b(NodeStatus::ConnectionsDeleting, 2), NodeStatus::Stopped)));
            Frame::aggregate("mesh1", None, &author_of(&publisher), version, version, 0, 1, digests)
        };
        let status = || view.digest_view.get(id.as_str()).map(|(d, _)| d.status);
        assert!(view.digest_view.record(b(NodeStatus::Ready, 1)));
        assert_eq!(status(), Some(NodeStatus::Ready), "the node's own Digest gives its status");
        hear(&mut view.order.lock().unwrap(), &view, &mesh_topology(1, vec![b(NodeStatus::Ready, 1)], vec![]), &fabric, Side::Backbone);
        assert_eq!(status(), Some(NodeStatus::Ready), "the projection's status");
        assert!(view.digest_view.record(b(NodeStatus::ConnectionsDeleting, 2)));
        assert_eq!(status(), Some(NodeStatus::ConnectionsDeleting), "its own later Digest moves its status");

        // The event from the primary, carrying the version its stop changed the birth at (v2).
        view.digest_view.primaries.take(PrimaryRole::MeshPrimary, &Primary { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: publisher.incarnation.clone(), revision: 1 });
        view.take(&event_by(StatusEvent::Stopped, stop.clone(), &publisher, 2), &fabric, "mesh-channel");
        assert!(view.digest_view.get(id.as_str()).is_none() && view.digest_view.in_flight() == vec![stop.clone()]);
        hear(&mut view.order.lock().unwrap(), &view, &mesh_topology(1, vec![b(NodeStatus::Ready, 1)], vec![]), &fabric, Side::Backbone);
        assert_eq!(view.digest_view.in_flight(), vec![stop.clone()], "a refresh at the version held, built before the stop, ends nothing");
        hear(&mut view.order.lock().unwrap(), &view, &mesh_topology(2, vec![], vec![stop.clone()]), &fabric, Side::Backbone);
        hear(&mut view.order.lock().unwrap(), &view, &mesh_topology(3, vec![], vec![]), &fabric, Side::Backbone);
        assert_eq!(view.digest_view.in_flight(), vec![stop.clone()], "a version listing neither the stop nor the birth ends nothing");
        assert!(!hear(&mut view.order.lock().unwrap(), &view, &mesh_topology(2, vec![], vec![stop.clone()]), &fabric, Side::Backbone), "v2 again, earlier than the v3 held, is ignored by the receive path");
        assert!(!view.digest_view.record(b(NodeStatus::ConnectionsDeleting, 3)), "a late Digest of the stopped cycle is refused");
        hear(&mut view.order.lock().unwrap(), &view, &mesh_topology(4, vec![b(NodeStatus::Pending, 3)], vec![]), &fabric, Side::Backbone);
        assert!(view.digest_view.in_flight().is_empty(), "the projection moved past the stop: it is over");
        assert_eq!(view.digest_view.get(id.as_str()).map(|(d, _)| (d.digest_seq, d.status)), Some((3, NodeStatus::ConnectionsDeleting)), "held again from the latest heartbeat it refused, with that Digest's status");
        assert!(view.digest_view.record(b(NodeStatus::Ready, 4)));
        assert_eq!(status(), Some(NodeStatus::Ready), "its own Digest's Ready");
        hear(&mut view.order.lock().unwrap(), &view, &mesh_topology(5, vec![b(NodeStatus::Ready, 4)], vec![]), &fabric, Side::Backbone);
        assert_eq!(status(), Some(NodeStatus::Ready));

        let refused = spans.named("rdm.mesh.topology.reject.via-stopped-birth");
        let projection: Vec<(String, String, String)> = refused.iter().filter(|f| f["via"] == "projection").map(|f| (f["held_version"].clone(), f["topology_version"].clone(), f["stop_carried_at"].clone())).collect();
        assert!(projection.is_empty(), "the framework ignores the repeated v1 before it can reach the stopped-entry data rule: {refused:?}");
        assert!(spans.named("rdm.mesh.topology.update.via-duplicate").iter().any(|f| f["frame"] == "mesh-topology" && f["identity"].contains("topology_version=1")), "the v1 repeat is refused by identity, never re-applied");
        assert!(refused.iter().any(|f| f["via"] == "heartbeat" && f["digest_seq"] == "3"), "{refused:?}");
        let ended = spans.named("rdm.mesh.topology.update.via-stop-ended-in-projection");
        assert_eq!(ended.iter().map(|f| (f["held_version"].clone(), f["topology_version"].clone(), f["stop_carried_at"].clone())).collect::<Vec<_>>(), vec![("3".to_string(), "4".to_string(), "2".to_string())], "ended once, at v4 over the v3 held, the stop carried at v2: {ended:?}");
    }

    /// CONTRACT (gossip.md §3.1 `TopologyFull`): a node's own birth is its own word. The mesh-primary's
    /// projection lists every member, the receiving node among them; the status it holds of itself
    /// stays the one it published, whether the projection arrives before its first publication (its
    /// name) or after it (its NodeId). Must NOT happen: a node taking a lagging status of itself from
    /// the projection.
    #[test]
    fn a_node_takes_no_status_of_itself_from_its_primarys_projection() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        // A node joins under its node id: before its first publication it knows itself by that id.
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let view = test_view("mesh1", id.as_str());
        let itself = |status, seq| digest(&id, &inc, None, status, seq);
        // Its own mesh's list reaches a node by a topology read alone (Q387: no aggregate on its mesh channel).
        let read = |version: u64, d: MeshDigest| crate::aggregate::Chunk { mesh: "mesh1".into(), mesh_id: None, author: None, topology_version: version, time_ms: version, chunk_index: 0, chunk_count: 1, digests: vec![d], runtimes: Vec::new() };
        assert!(matches!(view.take_read_chunk(read(1, itself(NodeStatus::Pending, 1)), Side::Read, &fabric), Taken::Written(_)));
        assert!(view.digest_view.member_status(&id, &inc).is_none(), "before its first publication, by the node id it joined under");
        *view.digest_view.own.lock().unwrap() = Some(id.to_string());
        assert!(view.digest_view.record(itself(NodeStatus::Ready, 2)));
        assert!(matches!(view.take_read_chunk(read(2, itself(NodeStatus::Leaving, 1)), Side::Read, &fabric), Taken::Written(_)));
        assert_eq!(view.digest_view.get(id.as_str()).map(|(d, _)| d.status), Some(NodeStatus::Ready), "after it, by its NodeId: its own status stands");
    }

    /// CONTRACT (gossip.md §3: "the mesh topology/current birth projection preserves the current
    /// RuntimeFact and required current runtime metadata needed by an authority-bearing admin to
    /// manage that birth"; §3.1 `TopologyFull`: "what every node-admin holds of every Mesh", where "for an
    /// own-mesh birth, liveness, endpoints with freshness and the runtime fact come from its Digest and
    /// status from the projection"; node-recovery.md "Detection and proof" step 1: "Every node-admin
    /// keeps the silence bookkeeping (`first_seen`), so a node-admin that takes the role mid-window
    /// continues the window already running"): a node-admin of mesh1 that is not its primary, whose
    /// primary's projection lists mesh1.node.1 though the member's own Digest never reached it, holds
    /// the birth from that projection, unheard since it first held it. A later version listing it
    /// again does not refresh its silence, and the primary dropping the silent member from its
    /// projection (no departure) does not forget it. The member's own digest, once heard, is the
    /// birth's word; a departure removes a projected birth. Must NOT happen: the view holding nothing
    /// of a member its primary's projection listed, so an heir that never heard the member keeps no
    /// window for it; or a projection refreshing a silence.
    #[test]
    fn an_admin_holds_an_own_mesh_birth_its_primarys_projection_lists_unheard_since_it_first_held_it() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = test_view("mesh1", "mesh1.node-admin.2");
        let publisher = PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        // A departure the aggregate carries is the node listed deleted (Q390).
        let mesh_topology = |version: u64, mut digests: Vec<MeshDigest>, deleted: Vec<MeshDigest>| {
            digests.extend(deleted.iter().map(|d| listed(d, NodeStatus::Deleted)));
            Frame::aggregate("mesh1", None, &author_of(&publisher), version, version, 0, 1, digests)
        };
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let (gone, gone_inc) = (NodeId::mint(), IncarnationId::mint());
        let member = digest(&id, &inc, None, NodeStatus::Ready, 4);
        let mut other = digest(&gone, &gone_inc, None, NodeStatus::Ready, 4);
        { let at = rdm_core::PathName::new("mesh1.node", "2").unwrap(); other.node.path = at.path; other.node.name = at.name; }
        other.node.mesh = "mesh1".into();
        view.take_topology_frame(&mesh_topology(1, vec![member.clone(), other.clone()], vec![]), &fabric, Side::Backbone, &mut view.order.lock().unwrap());
        let first_held_by = Instant::now();
        assert!(view.digest_view.get(id.as_str()).is_none(), "a projection is no liveness: no digest of the member is held");
        let held = |now: Instant| view.digest_view.projected_at(id.as_str(), now).map(|(d, unheard)| (d.node.incarnation.clone(), now.checked_sub(unheard)));
        let (incarnation, since) = held(Instant::now()).expect("the birth its primary's projection lists is held from that projection");
        assert_eq!(incarnation, inc);
        assert!(since.is_some_and(|s| s <= first_held_by), "unheard since it was first held");
        std::thread::sleep(Duration::from_millis(20));
        view.take_topology_frame(&mesh_topology(2, vec![member.clone(), other.clone()], vec![]), &fabric, Side::Backbone, &mut view.order.lock().unwrap());
        assert!(held(Instant::now()).is_some_and(|(_, s)| s.is_some_and(|s| s <= first_held_by)), "a later version listing it again does not refresh its silence");
        view.take_topology_frame(&mesh_topology(3, vec![other.clone()], vec![]), &fabric, Side::Backbone, &mut view.order.lock().unwrap());
        assert!(held(Instant::now()).is_some_and(|(_, s)| s.is_some_and(|s| s <= first_held_by)), "the primary dropping the silent member from its projection is no departure: the birth stays held");
        let listed: Vec<rdm_core::PathName> = view.digest_view.projected_unheard_at(Instant::now()).into_iter().map(|(d, _)| d.node.location()).collect();
        assert_eq!(listed.len(), 2, "both projected births are held: {listed:?}");
        assert!(view.digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 5)), "the member's own digest is taken");
        assert!(view.digest_view.projected_at(id.as_str(), Instant::now()).is_none(), "its own digest is the birth's word: it is held from the projection no more");
        view.take_topology_frame(&mesh_topology(4, vec![], vec![other.clone()]), &fabric, Side::Backbone, &mut view.order.lock().unwrap());
        assert!(view.digest_view.is_deleted(gone.as_str()), "the node listed deleted departs");
        assert!(view.digest_view.projected_at(gone.as_str(), Instant::now()).is_none(), "a departure removes the projected birth");
        assert!(view.digest_view.projected_unheard_at(Instant::now()).is_empty(), "nothing is held from the projection any more");
    }

    /// CONTRACT (gossip.md §6): a member held only from its mesh-primary's projection is a repair
    /// target with its silence since first held; under an open removal, or held `Leaving`, it is none,
    /// and an open removal's key is one its channels forget. Must NOT happen: an address under removal
    /// handed to a channel, or a projected member left out of repair because it was never heard.
    #[test]
    fn a_projected_member_is_a_repair_target_unless_leaving_or_under_an_open_removal() {
        let view = View::new("mesh1", "mesh1.node-admin.1");
        let (id, inc, leaving_id, gone, gone_inc) = (NodeId::mint(), IncarnationId::mint(), NodeId::mint(), NodeId::mint(), IncarnationId::mint());
        let projected = |id: &NodeId, inc: &IncarnationId, status: NodeStatus| {
            let mut d = digest(id, inc, None, status, 1);
            d.node.endpoint_id = EndpointId(iroh::SecretKey::generate().public().to_string());
            d
        };
        let first_held = Instant::now() - Duration::from_secs(60);
        let member = projected(&id, &inc, NodeStatus::Pending);
        view.digest_view.hold_projected_at(&member, first_held);
        let leaving_inc = IncarnationId::mint();
        view.digest_view.hold_projected_at(&projected(&leaving_id, &leaving_inc, NodeStatus::Leaving), first_held);
        let doomed = projected(&gone, &gone_inc, NodeStatus::Ready);
        view.digest_view.hold_projected_at(&doomed, first_held);
        assert!(view.digest_view.deleting(op(&gone, &gone_inc, "node.delete")), "the removal opens over the projected birth");

        let held = crate::channel::held_view(&view.digest_view, |_| true);
        let targets: Vec<&str> = held.targets.iter().map(|t| t.node_id.as_str()).collect();
        assert_eq!(targets, vec![id.as_str()], "the projected member alone: not the one held Leaving, not the one under removal");
        assert!(held.targets[0].silent_for >= Duration::from_secs(60), "its silence runs from when it was first held");
        assert_eq!(held.deleted.iter().map(|k| k.to_string()).collect::<Vec<_>>(), vec![doomed.node.endpoint_id.0.clone()], "the channel forgets the key under removal");
    }

    /// CONTRACT (gossip.md §3.1 `TopologyFull`: the mesh-primary is the author of its own mesh's
    /// statuses): the mesh-primary authors its own mesh's statuses, a rule keyed on the receiver's own
    /// role. A mesh topology of its own mesh from another publisher (a predecessor's last one, arriving
    /// after this node took the role), on the backbone (Q387: an aggregate never travels on its own
    /// channel), moves none of them. Must NOT happen: the primary's authored status overwritten by a
    /// projection copy.
    #[test]
    fn a_mesh_primary_takes_no_status_of_its_own_mesh_from_another_publishers_projection() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = test_view("mesh1", "mesh1.node-admin.2");
        view.primary.store(true, Ordering::Relaxed);
        let predecessor = PublisherId { incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() };
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(view.digest_view.record(digest(&id, &inc, None, NodeStatus::Pending, 5)), "the primary holds its member's heartbeat");
        assert!(view.digest_view.started(&id, &inc, &rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1"), "and sets it ready from its node.started");
        let late = Frame::aggregate("mesh1", None, &author_of(&predecessor), 9, 9, 0, 1, vec![digest(&id, &inc, None, NodeStatus::ConnectionsDeleting, 6)]);
        view.take_topology_frame(&late, &fabric, Side::Backbone, &mut view.order.lock().unwrap());
        assert_eq!(view.digest_view.get(id.as_str()).map(|(d, _)| (d.status, d.digest_seq)), Some((NodeStatus::Ready, 5)), "the authored status stands");
        assert_eq!(view.digest_view.member_status(&id, &inc), Some(NodeStatus::Ready));
    }

    /// CONTRACT (gossip.md §3.1 `TopologyDelta`: `added`, `changed` and `removed`; a node under a stop
    /// says so by its status, Q390): a differential's entry of a birth clears that exact birth's stop
    /// overlay in the held projection: its source lists a stopped birth again only after node.joined
    /// cleared the stop. A deleting overlay of the birth, and a stop of another incarnation, stay. The
    /// forwarder's differential for a joined birth carries it in `added` (the projection at the stop did
    /// not list it).
    #[test]
    fn a_delta_listing_a_birth_again_clears_only_that_births_stop_overlay() {
        use crate::aggregate::{Delta, Full};
        let (id, inc, other) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        let stop = op(&id, &inc, "node.stop");
        let old_stop = LifecycleOp { build_id: "b0".into(), ..op(&id, &other, "node.stop") };
        let deleting = op(&id, &inc, "node.delete");
        let mut held = Full::new(Vec::new(), vec![stop.clone(), old_stop.clone(), deleting.clone()], Vec::new());
        held.apply(&Delta { added: vec![digest(&id, &inc, None, NodeStatus::Pending, 9)], ..Delta::default() }).expect("the differential applies");
        let mut left = held.in_flight();
        left.sort_by(|a, b| a.key().cmp(&b.key()));
        let mut want = vec![old_stop, deleting];
        want.sort_by(|a, b| a.key().cmp(&b.key()));
        assert_eq!(left, want, "only the exact birth's stop overlay is cleared");
        let at_stop = Full::new(Vec::new(), vec![stop.clone()], Vec::new());
        let joined = Full::new(vec![digest(&id, &inc, None, NodeStatus::Pending, 9)], Vec::new(), Vec::new());
        let delta = at_stop.delta_to(&joined);
        assert_eq!((delta.added.len(), delta.changed.len(), delta.removed.len()), (1, 0, 0), "the clear travels as the birth's own added entry: {delta:?}");
        let mut receiver = at_stop.clone();
        receiver.apply(&delta).expect("the differential applies");
        assert!(receiver.same_topology(&joined), "the receiver holds what the source holds");
    }

    /// CONTRACT (Q387, Q197: the mesh-primary takes a node's status from the op stream the node sends it
    /// and decides from that, never from gossip): at the mesh-primary of mesh1, a held node's status
    /// moves by its reports alone; its Digests give liveness and move no status, from memory or decoded
    /// from the wire, even one that says connections-deleting. Its first Digest holds it with the status
    /// it carried. Must NOT happen: an older Digest, delivered after node.started, setting the node back
    /// to Pending at its mesh-primary (fabric_atlas, a pre-check refused no-secondary 9 ms after
    /// mesh2.node.2's Digest 12 set its Ready back to Pending).
    #[test]
    fn a_held_nodes_status_moves_by_its_reports_alone_at_its_mesh_primary() {
        let digest_view = DigestView::default();
        *digest_view.own_mesh.lock().unwrap() = "mesh1".into();
        digest_view.primary.store(true, Ordering::Relaxed);
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let status = |digest_view: &DigestView| digest_view.get(id.as_str()).map(|(d, _)| d.status);
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Pending, 100)), "the first Digest holds the node");
        assert_eq!(status(&digest_view), Some(NodeStatus::Pending), "with the status it carried");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 101)), "a later Digest is taken as liveness");
        assert_eq!(status(&digest_view), Some(NodeStatus::Pending), "and moves no status at the mesh-primary");
        node_started(&digest_view, &id, &inc);
        assert_eq!(status(&digest_view), Some(NodeStatus::Ready), "node.started, its report, sets it ready");
        assert!(digest_view.routable(id.as_str()));
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Pending, 102)), "a Digest delivered late");
        assert_eq!(status(&digest_view), Some(NodeStatus::Ready), "sets nothing back");
        let heard = Frame::decode(&Frame::digest(&digest(&id, &inc, None, NodeStatus::ConnectionsDeleting, 120)).encode()).unwrap().digest_of("mesh1").expect("a Digest names its birth from its details");
        assert_eq!(heard.status, NodeStatus::ConnectionsDeleting, "the Digest carries its status on the wire");
        assert!(digest_view.record(heard));
        assert_eq!(status(&digest_view), Some(NodeStatus::Ready), "nor does one from the wire");
        assert!(digest_view.reported(&id, &inc, NodeStatus::ConnectionsDeleting), "its report of connections-deleting");
        assert_eq!(status(&digest_view), Some(NodeStatus::ConnectionsDeleting), "moves it");
    }

    /// CONTRACT (states.md "Who sets each state": node pending is set by "the node's mesh-primary"
    /// from "the node's node.joined report on the start call"): at the mesh-primary of mesh1, a
    /// node.joined of a held birth sets it Pending though no stop mark is held, and the birth stays
    /// unroutable until node.started of the same birth sets it ready. A node.joined of a
    /// birth this view holds nothing of sets nothing. Must NOT happen: node.joined leaving the birth
    /// ready.
    #[test]
    fn a_node_joined_with_no_stop_held_sets_its_held_birth_pending() {
        let digest_view = DigestView::default();
        *digest_view.own_mesh.lock().unwrap() = "mesh1".into();
        digest_view.primary.store(true, Ordering::Relaxed);
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Pending, 100)));
        node_started(&digest_view, &id, &inc);
        assert!(digest_view.routable(id.as_str()), "the started birth is ready");
        assert!(!digest_view.joined(&op(&NodeId::mint(), &IncarnationId::mint(), "node.start")), "node.joined of a birth this view holds nothing of sets nothing");
        assert!(digest_view.joined(&op(&id, &inc, "node.start")), "the node.joined sets its birth Pending");
        assert_eq!(digest_view.get(id.as_str()).map(|(d, _)| d.status), Some(NodeStatus::Pending), "held Pending from node.joined");
        assert_eq!(digest_view.member_status(&id, &inc), Some(NodeStatus::Pending), "the status the projection carries is Pending");
        assert!(!digest_view.routable(id.as_str()), "a Pending birth is not routable");
        let joined = spans.named("rdm.mesh.topology.update.via-node-joined");
        assert_eq!(joined.iter().map(|f| (f["status"].clone(), f["stops_cleared"].clone())).collect::<Vec<_>>(), vec![("Pending".to_string(), "[]".to_string())], "one node.joined, Pending, no stop cleared: {joined:#?}");
        node_started(&digest_view, &id, &inc);
        assert!(digest_view.routable(id.as_str()), "node.started of the same birth: ready again");
    }

    /// CONTRACT (states.md "Who sets each state": node Ready is set by "the node's
    /// mesh-primary" from "the node's node.started report, after every start gate"; gossip.md `Digest`,
    /// Q387: the Digest carries the node's status): at mesh1's primary, a birth held Pending from its
    /// join digest and report, then ready by its node.started and its own ready Digest, is ready in every
    /// read that hands out its digest. Must NOT happen: `get`,
    /// `current`, `all`, `heard_direct_since`, `silent_held` or `routable` answering Pending after
    /// node.started, or the projection publishing it Pending.
    #[test]
    fn a_digest_held_after_node_started_leaves_the_started_birth_ready() {
        let digest_view = DigestView::default();
        *digest_view.own_mesh.lock().unwrap() = "mesh1".into();
        digest_view.primary.store(true, Ordering::Relaxed);
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record_joined(digest(&id, &inc, None, NodeStatus::Pending, 100)), "the join report");
        assert_eq!(digest_view.member_status(&id, &inc), Some(NodeStatus::Pending), "the join digest carries its node's status");
        assert!(digest_view.reported(&id, &inc, NodeStatus::Pending), "its mesh-primary records the accepted join report's Pending");
        assert_eq!(digest_view.member_status(&id, &inc), Some(NodeStatus::Pending), "held Pending from the join report");
        assert!(digest_view.started(&id, &inc, &rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1"), "node.started of the birth");
        // The node's own Digest after it wrote Ready.
        let since = Instant::now();
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 101)));
        let ready = Some(NodeStatus::Ready);
        assert_eq!(digest_view.member_status(&id, &inc), ready, "node.started set it");
        assert_eq!(digest_view.get(id.as_str()).map(|(d, _)| d.status), ready, "get");
        assert_eq!(digest_view.current(Duration::from_secs(5)).into_iter().find(|d| d.node.node_id == id).map(|d| d.status), ready, "current: the projection's members");
        assert_eq!(digest_view.all().into_iter().find(|d| d.node.node_id == id).map(|d| d.status), ready, "all");
        assert_eq!(digest_view.heard_direct_since(since).into_iter().find(|d| d.node.node_id == id).map(|d| d.status), ready, "heard_direct_since");
        assert_eq!(digest_view.silent_held(Instant::now()).into_iter().find(|(d, _)| d.node.node_id == id).map(|(d, _)| d.status), ready, "silent_held");
        assert!(digest_view.routable(id.as_str()), "the started birth of a ready mesh is routable");
    }

    /// CONTRACT (states.md "Who sets each state": node Ready is set by "the node's
    /// mesh-primary" from "the node's node.started report, after every start gate"; Q80 = a): a
    /// mesh-primary that takes the node.started of a birth of its own mesh it holds no digest of yet (a
    /// primary born while the node was silent decides its declaration from durable rows, Q11) spans
    /// `from-node-started` and holds no status for it until its Digest (Q387), which carries the Ready
    /// the node wrote: every read then hands the birth out ready. Must NOT happen: the re-born primary applying
    /// a resumed member's Ready, authoring nothing, then publishing the member Pending for good
    /// (kept/e6s7-i144-e4-re-born-admin-start-unheld, traces 8e6533b70a42c1e26c5786b9de02718a and
    /// b9363650a79356392e9361154673417d).
    #[test]
    fn a_node_started_taken_before_the_births_digest_holds_it_ready_when_the_digest_arrives() {
        let digest_view = DigestView::default();
        *digest_view.own_mesh.lock().unwrap() = "mesh1".into();
        digest_view.primary.store(true, Ordering::Relaxed);
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.get(id.as_str()).is_none(), "the view holds no digest of the birth");
        assert!(digest_view.started(&id, &inc, &rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1"), "node.started of a birth not held yet");
        assert_eq!(digest_view.member_status(&id, &inc), None, "no status before its Digest");
        let started = spans.named("rdm.mesh.topology.update.from-node-started");
        assert_eq!(started.iter().map(|f| (f["member_path"].clone(), f["member_name"].clone())).collect::<Vec<_>>(), vec![("mesh1.node".to_string(), "1".to_string())], "from-node-started names the birth: {started:#?}");
        assert!(digest_view.record_joined(digest(&id, &inc, None, NodeStatus::Ready, 100)), "its digest arrives with the Ready it wrote");
        assert_eq!(digest_view.member_status(&id, &inc), Some(NodeStatus::Ready), "nothing is seeded Pending");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 101)), "a heartbeat");
        let ready = Some(NodeStatus::Ready);
        assert_eq!(digest_view.member_status(&id, &inc), ready, "the held status stands");
        assert_eq!(digest_view.get(id.as_str()).map(|(d, _)| d.status), ready, "get hands it out ready");
        assert_eq!(digest_view.current(Duration::from_secs(5)).into_iter().find(|d| d.node.node_id == id).map(|d| d.status), ready, "current: the projection's members");
        assert!(digest_view.routable(id.as_str()), "the started birth of a ready mesh is routable");
        // A held digest's name wins over the one the caller names: here the caller's names another mesh.
        let (other, other_inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&other, &other_inc, None, NodeStatus::Pending, 100)));
        assert!(digest_view.started(&other, &other_inc, &rdm_core::PathName::new("mesh2.node", "9").unwrap(), "mesh2"), "the held digest's name, of this view's mesh, decides");
        assert_eq!(digest_view.member_status(&other, &other_inc), ready);
    }

    /// CONTRACT (Q387: a node of the mesh ends a stop by the stopped node's own Digest of its next
    /// cycle): a late Digest of the stopped cycle is refused; the node's own Pending after it ends the
    /// stop, and an overlay that ended stays ended: the stop re-sent by an aggregate built before it opens
    /// nothing. Must NOT happen: a stop ended by a Digest of the cycle it stopped, or a started node
    /// stopped again by a copy of its old stop.
    #[test]
    fn an_ended_stop_overlay_is_not_opened_again_by_a_later_copy() {
        let digest_view = DigestView::default();
        *digest_view.own_mesh.lock().unwrap() = "mesh1".into();
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 100)));
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::ConnectionsDeleting, 110)));
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Pending, 120)));
        assert!(digest_view.started(&id, &inc, &rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1"), "node.started of the birth");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 125)));
        assert!(digest_view.routable(id.as_str()), "no overlay holds it: the ready birth of a ready mesh is selected");
        let stop = op(&id, &inc, "node.stop");
        assert!(digest_view.stopped(stop.clone()));
        assert!(!digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 126)), "a late Digest of the stopped cycle ends no stop");
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Pending, 130)), "its own Digest of its next cycle ends the stop");
        assert!(digest_view.in_flight().is_empty(), "the stop overlay ended");
        assert!(!digest_view.stopped(stop), "an aggregate built before the stop ended does not stop the node again");
        assert!(digest_view.get(id.as_str()).is_some());
    }

    /// CONTRACT (Q387, Q197): at the stopped node's own mesh-primary its Digest of its next cycle ends
    /// no stop: the primary takes the node's report, node.joined, which does. Must NOT happen: the
    /// mesh-primary acting on gossip for a stop it ran.
    #[test]
    fn at_its_mesh_primary_only_node_joined_ends_a_stop() {
        let digest_view = DigestView::default();
        *digest_view.own_mesh.lock().unwrap() = "mesh1".into();
        digest_view.primary.store(true, Ordering::Relaxed);
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 100)));
        let stop = op(&id, &inc, "node.stop");
        assert!(digest_view.stopped(stop.clone()));
        assert!(!digest_view.record(digest(&id, &inc, None, NodeStatus::Pending, 130)), "its next-cycle Digest is refused at its mesh-primary");
        assert_eq!(digest_view.in_flight(), vec![stop], "the stop stands");
        assert!(digest_view.joined(&op(&id, &inc, "node.start")), "node.joined, the node's report, ends it");
        assert!(digest_view.in_flight().is_empty());
    }

    /// CONTRACT: a stop of an older birth than the one held, and a stop of a deleted node, are refused;
    /// a node this view holds nothing of is marked stopped all the same (a late joiner reads it from
    /// the mesh topology's open overlays) and refuses every heartbeat of that birth.
    #[test]
    fn a_stop_of_another_birth_is_refused_and_one_for_an_unheld_node_still_stops_it() {
        let digest_view = DigestView::default();
        let (id, old, new) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &new, Some(old.clone()), NodeStatus::Ready, 100)));
        assert!(!digest_view.stopped(op(&id, &old, "node.stop")), "the stop names a birth the held one superseded");
        assert!(digest_view.get(id.as_str()).is_some());
        let late_joiner = DigestView::default();
        assert!(late_joiner.stopped(op(&id, &new, "node.stop")));
        assert!(!late_joiner.record(digest(&id, &new, None, NodeStatus::ConnectionsDeleting, 90)));
        assert!(!late_joiner.record(digest(&id, &new, None, NodeStatus::Pending, 91)), "a heartbeat ends no stop");
        assert!(late_joiner.joined(&op(&id, &new, "node.start")), "node.joined ends it");
        assert_eq!(late_joiner.get(id.as_str()).map(|(d, _)| (d.status, d.digest_seq)), Some((NodeStatus::Pending, 91)), "held again from the latest heartbeat it refused");
    }

    /// CONTRACT: a stop is an open overlay of its mesh's topology (`TopologyFull.in_flight`), recognised
    /// by its operation, so a node that never heard the frame reads the node as stopped from the mesh topology.
    #[test]
    fn a_stop_overlay_rides_the_aggregate_and_is_told_by_its_operation() {
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let digest_view = DigestView::default();
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 100)));
        let stop = op(&id, &inc, "node.stop");
        assert!(stop.is_stop());
        assert!(!op(&id, &inc, "node.delete").is_stop());
        assert!(digest_view.stopped(stop.clone()));
        let (in_flight, deleted) = digest_view.overlays_of("mesh1");
        assert_eq!(in_flight, vec![stop]);
        assert!(deleted.is_empty());
    }

    #[test]
    fn a_deleted_node_id_never_returns_under_any_incarnation_until_the_retention_passes() {
        let digest_view = DigestView::with_retention(Duration::from_millis(40));
        let (id, inc, next) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        let t0 = Instant::now();
        digest_view.record_at(digest(&id, &inc, None, NodeStatus::Ready, 100), t0);
        digest_view.depart_at(op(&id, &inc, "node.delete"), t0);
        assert!(!digest_view.record_at(digest(&id, &inc, None, NodeStatus::Leaving, 900), t0), "the deleted birth's late digest");
        assert!(!digest_view.record_forwarded_at(digest(&id, &next, Some(inc.clone()), NodeStatus::Ready, 950), t0), "a later incarnation of the deleted NodeId");
        assert!(digest_view.is_deleted_at(id.as_str(), t0));
        let after = t0 + Duration::from_millis(60);
        assert!(!digest_view.is_deleted_at(id.as_str(), after), "forgotten after the retention");
        assert!(digest_view.record_at(digest(&id, &next, Some(inc), NodeStatus::Ready, 960), after), "after the retention the id is unknown, not deleted");
    }

    /// Which routing watch moved, without waiting: `None` when none did.
    async fn moved_named(w: &mut RoutingWatch) -> Option<&'static str> {
        tokio::select! {
            biased;
            which = w.moved() => Some(which),
            _ = std::future::ready(()) => None,
        }
    }

    /// CONTRACT (Q254, Luke 2026-10-28: "One clock, the pruner's: its member-stale mark is a view
    /// change"): the pruner's sweep (`DigestView::mark_silence_at`) moves the routing watch, naming
    /// `silence`, when a held birth goes unheard past the staleness floor, when a silent birth is
    /// heard again, and when a birth held only from its mesh-primary's projection goes unheard past the
    /// floor since it was first held; a departure forgotten at the sweep, past its retention, moves
    /// `overlays`. A sweep that finds the same silence moves nothing. Must NOT happen: a birth falling
    /// silent that moves no watch (the view then shows it only on a poll), or a sweep that moves the
    /// watch on every pass (a poll by another name).
    #[tokio::test]
    async fn the_pruners_sweep_moves_the_routing_watch_when_a_birth_crosses_the_staleness_floor_and_on_nothing_else() {
        let floor = Duration::from_millis(100);
        let t0 = Instant::now();

        let digest_view = DigestView::with_floor(Duration::from_secs(3600), floor);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record_at(digest(&id, &inc, None, NodeStatus::Ready, 100), t0));
        let mut w = digest_view.routing_watch();
        w.seen();
        assert!(!digest_view.mark_silence_at(t0 + floor / 2), "heard within the floor: no birth is silent");
        assert_eq!(moved_named(&mut w).await, None, "a sweep that finds no silence moves nothing");
        assert!(digest_view.mark_silence_at(t0 + floor * 2), "unheard past the floor: the sweep marks the birth");
        assert_eq!(moved_named(&mut w).await, Some("silence"), "the mark moves the routing watch");
        w.seen();
        assert!(!digest_view.mark_silence_at(t0 + floor * 3), "still silent: the same mark");
        assert_eq!(moved_named(&mut w).await, None, "a sweep that finds the same silence moves nothing");
        let t1 = t0 + floor * 4;
        assert!(digest_view.record_at(digest(&id, &inc, None, NodeStatus::Ready, 101), t1));
        w.seen();
        assert!(digest_view.mark_silence_at(t1), "heard again: the birth is silent no more");
        assert_eq!(moved_named(&mut w).await, Some("silence"), "the silence ending moves the routing watch at the sweep");

        let digest_view = DigestView::with_floor(Duration::from_secs(3600), floor);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        digest_view.hold_projected_at(&digest(&id, &inc, None, NodeStatus::Ready, 1), t0);
        let mut w = digest_view.routing_watch();
        w.seen();
        assert!(!digest_view.mark_silence_at(t0 + floor / 2), "held from the projection within the floor");
        assert_eq!(moved_named(&mut w).await, None);
        assert!(digest_view.mark_silence_at(t0 + floor * 2), "held only from the projection, unheard past the floor since first held");
        assert_eq!(moved_named(&mut w).await, Some("silence"));

        let digest_view = DigestView::with_floor(Duration::from_millis(40), floor);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record_at(digest(&id, &inc, None, NodeStatus::Ready, 100), t0));
        assert!(digest_view.depart_at(op(&id, &inc, "node.delete"), t0));
        let mut w = digest_view.routing_watch();
        w.seen();
        digest_view.mark_silence_at(t0 + Duration::from_millis(20));
        assert_eq!(moved_named(&mut w).await, None, "the departure is held within its retention");
        digest_view.mark_silence_at(t0 + Duration::from_millis(60));
        assert_eq!(moved_named(&mut w).await, Some("overlays"), "the departure forgotten past its retention, at the sweep, moves the routing watch");
        assert!(!digest_view.is_deleted_at(id.as_str(), t0 + Duration::from_millis(60)));
    }

    /// CONTRACT (Q254, rdm#172: each input the admin's view reads raises its own event where it
    /// changes): the writes a mesh topology's install makes move the routing watch when they change
    /// what the view lists, and only then. A status taken from the mesh-primary's projection for a
    /// birth held only from that projection moves `heard`; a forwarded copy of a birth held silent past
    /// the staleness floor (the backbone's word after a cut heals) moves `heard`, one within the floor
    /// nothing; a stop mark that ends with no in-flight overlay left moves `overlays`. Must NOT happen:
    /// a fact the view lists changed with no watch moved (the view then shows it only when something
    /// else moves).
    #[tokio::test]
    async fn each_aggregate_write_of_a_listed_fact_moves_the_routing_watch_and_a_repeat_moves_nothing() {
        let floor = Duration::from_millis(100);
        let t0 = Instant::now();

        let digest_view = DigestView::with_floor(Duration::from_secs(3600), floor);
        *digest_view.own_mesh.lock().unwrap() = "mesh1".into();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.hold_projected_at(&digest(&id, &inc, None, NodeStatus::Pending, 1), t0));
        let mut w = digest_view.routing_watch();
        w.seen();
        assert!(digest_view.hold_projected_at(&digest(&id, &inc, None, NodeStatus::Ready, 2), t0));
        assert!(digest_view.get(id.as_str()).is_none(), "the birth is held only from its mesh-primary's projection");
        assert_eq!(moved_named(&mut w).await, Some("heard"), "its status moving moves the routing watch");
        w.seen();
        assert!(!digest_view.hold_projected_at(&digest(&id, &inc, None, NodeStatus::Ready, 3), t0));
        assert_eq!(moved_named(&mut w).await, None, "the same status taken again moves nothing");

        let digest_view = DigestView::with_floor(Duration::from_secs(3600), floor);
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record_forwarded_at(digest(&id, &inc, None, NodeStatus::Ready, 10), t0));
        let mut w = digest_view.routing_watch();
        w.seen();
        assert!(digest_view.record_forwarded_at(digest(&id, &inc, None, NodeStatus::Ready, 11), t0 + floor / 2));
        assert_eq!(moved_named(&mut w).await, None, "a forwarded copy of a birth heard within the floor moves nothing");
        assert!(digest_view.record_forwarded_at(digest(&id, &inc, None, NodeStatus::Ready, 12), t0 + floor / 2 + floor * 2));
        assert_eq!(moved_named(&mut w).await, Some("heard"), "a forwarded copy of a birth silent past the floor: heard again");

        let digest_view = DigestView::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 10)));
        assert!(digest_view.stopped(op(&id, &inc, "node.stop")));
        assert_eq!(digest_view.stopped_digests().len(), 1, "the stopped birth is listed from its mark");
        digest_view.in_flight.lock().unwrap().clear();
        let mut w = digest_view.routing_watch();
        w.seen();
        digest_view.end_stop(id.as_str());
        assert!(digest_view.stopped_digests().is_empty());
        assert_eq!(moved_named(&mut w).await, Some("overlays"), "a stop mark ended with no overlay held moves the routing watch");
    }

    /// CONTRACT (Q254, rdm#172): the meshes the accepted Build names are an input of the admin's view
    /// (a mesh it does not name and that has no member is not in the view): a change of them moves the
    /// routing watch, named `accepted`, and the same set held again moves nothing. Must NOT happen: the
    /// view keeping a mesh the accepted Build dropped until something else moves.
    #[tokio::test]
    async fn the_meshes_the_accepted_build_names_move_the_routing_watch_when_they_change() {
        let digest_view = DigestView::default();
        let mut w = digest_view.routing_watch();
        w.seen();
        digest_view.set_accepted_meshes(BTreeSet::from(["mesh1".to_string()]));
        assert_eq!(moved_named(&mut w).await, Some("accepted"), "the meshes the accepted Build names, first held");
        w.seen();
        digest_view.set_accepted_meshes(BTreeSet::from(["mesh1".to_string()]));
        assert_eq!(moved_named(&mut w).await, None, "the same meshes again move nothing");
        digest_view.set_accepted_meshes(BTreeSet::from(["mesh1".to_string(), "mesh2".to_string()]));
        assert_eq!(moved_named(&mut w).await, Some("accepted"), "a mesh added");
    }

    /// CONTRACT (Q406, Q407: a node-type primary gossips its role in its own Digest's `primary`, and
    /// that Digest moves no member's birth or status): holding a primary record moves the routing
    /// watch, named `primaries`, whether the record is taken on its own or from the role a Digest of
    /// its birth carries; the same record again, or one it does not supersede, moves nothing; the
    /// other watches still name themselves. Must NOT happen: a routing waiter (the type's election, an
    /// admin's view rebuild, an application's forward) that does not wake when a primary record is
    /// held, so it learns the new primary only when something else moves.
    #[tokio::test]
    async fn holding_a_new_primary_record_moves_the_routing_watch_and_a_record_not_held_moves_nothing() {
        let digest_view = DigestView::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let record = |revision| Primary { mesh: "mesh1".into(), node_id: id.clone(), incarnation: inc.clone(), revision };
        let mut w = digest_view.routing_watch();
        w.seen();
        assert!(matches!(digest_view.primaries.take(PrimaryRole::MeshPrimary, &record(2)), RoleTaken::Held { .. }));
        assert_eq!(moved_named(&mut w).await, Some("primaries"), "a record held moves the routing watch");
        w.seen();
        assert_eq!(digest_view.primaries.take(PrimaryRole::MeshPrimary, &record(2)), RoleTaken::Same);
        assert_eq!(moved_named(&mut w).await, None, "the record already held moves nothing");
        assert!(matches!(digest_view.primaries.take(PrimaryRole::MeshPrimary, &record(1)), RoleTaken::Refused { .. }));
        assert_eq!(moved_named(&mut w).await, None, "a record that does not supersede the one held moves nothing");
        hold_mesh_status(&digest_view, "mesh1", "ready", 10);
        assert_eq!(moved_named(&mut w).await, Some("meshes"), "a mesh status taken still names its own watch");

        // The type's election reaches every node as the role on the new primary's own Digest: the
        // birth is held already, so the Digest moves no birth or status, only the record.
        let digest_view = DigestView::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 10)));
        let mut w = digest_view.routing_watch();
        w.seen();
        let mut elected = digest(&id, &inc, None, NodeStatus::Ready, 11);
        elected.primary = rdm_core::RoleRevision::of_type("node", Some(1), None);
        assert!(digest_view.record(elected));
        assert_eq!(digest_view.primaries.node_type_primary("mesh1", "node").map(|p| (p.node_id, p.revision)), Some((id.clone(), 1)), "the role the Digest carries is held");
        assert_eq!(moved_named(&mut w).await, Some("primaries"), "the record the Digest carries moves the routing watch, and nothing else does");
        w.seen();
        let mut again = digest(&id, &inc, None, NodeStatus::Ready, 12);
        again.primary = rdm_core::RoleRevision::of_type("node", Some(1), None);
        assert!(digest_view.record(again));
        assert_eq!(moved_named(&mut w).await, None, "its next Digest, the same role at the same revision, moves nothing");
    }

    /// CONTRACT (rdm#172): a watch of the routing watch whose sender is gone leaves the wait, and the
    /// others still wake it. Must NOT happen: one closed watch holding the wait for good, so that no
    /// fact any other watch moves wakes the view's rebuild again.
    #[tokio::test]
    async fn a_closed_watch_leaves_the_routing_watch_and_every_other_still_wakes_it() {
        let digest_view = DigestView::default();
        let (gone, rx) = tokio::sync::watch::channel(0u64);
        let mut w = digest_view.routing_watch().watching("gone", rx);
        w.seen();
        drop(gone);
        assert_eq!(moved_named(&mut w).await, None, "the closed watch neither wakes the wait nor holds it");
        hold_mesh_status(&digest_view, "mesh2", "ready", 10);
        assert_eq!(moved_named(&mut w).await, Some("meshes"), "a mesh status taken still wakes it");
        w.seen();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 10)));
        assert!(moved_named(&mut w).await.is_some(), "a birth first held still wakes it");
    }

    /// A departure is its node listed deleted (Q390): a mesh-primary's aggregate lists only its own
    /// mesh's, and they pack into chunks that each fit one message.
    #[test]
    fn departures_and_overlays_pack_into_frames_that_fit_and_a_publisher_carries_only_its_mesh() {
        let digest_view = DigestView::default();
        let depart = |mesh: &str| {
            let (id, inc) = (NodeId::mint(), IncarnationId::mint());
            let mut d = digest(&id, &inc, None, NodeStatus::Ready, 1);
            let mut o = op(&id, &inc, "node.delete");
            d.node.mesh = mesh.into();
            o.mesh = mesh.into();
            { let at = rdm_core::PathName::new(&format!("{mesh}.node"), "1").unwrap(); d.node.path = at.path.clone(); o.path = at.path; }
            assert!(digest_view.record(d));
            assert!(digest_view.depart(o));
        };
        depart("mesh2");
        for _ in 0..100 {
            depart("mesh1");
        }
        let (in_flight, deleted) = digest_view.overlays_of("mesh1");
        assert!(in_flight.is_empty());
        assert_eq!(deleted.len(), 100, "mesh2's departure is not mesh1's to publish");
        let nodes = digest_view.aggregate_nodes("mesh1", Vec::new(), &in_flight, &deleted);
        assert_eq!(nodes.len(), 100, "each departure is its node listed");
        assert!(nodes.iter().all(|d| d.status == NodeStatus::Deleted && d.node.mesh == "mesh1"), "listed deleted, of mesh1 only");
        let runs = pack(nodes, |run| mesh_topology("mesh1", run));
        assert!(runs.len() > 1, "a hundred departures do not fit one message");
        assert_eq!(runs.iter().map(Vec::len).sum::<usize>(), 100, "every departure travels");
        for r in &runs {
            assert!(mesh_topology("mesh1", r.clone()).encode().len() <= MAX_FRAME, "a run fits one message");
        }
    }

    #[test]
    fn a_departure_heard_before_the_birth_still_refuses_it() {
        let digest_view = DigestView::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.depart(op(&id, &inc, "node.delete")));
        assert!(!digest_view.record(digest(&id, &inc, None, NodeStatus::Ready, 100)));
        assert!(!digest_view.deleting(op(&id, &inc, "node.delete")), "no overlay on a deleted node");
    }

    #[test]
    fn members_are_packed_into_frames_that_fit_one_gossip_message() {
        // Nodes under a removal and deleted nodes are listed by their status beside the others (Q390).
        let ds: Vec<MeshDigest> = (0..100).map(|i| {
            let status = match i { 0..3 => NodeStatus::Deleting, 3..8 => NodeStatus::Deleted, _ => NodeStatus::Ready };
            digest(&NodeId::mint(), &IncarnationId::mint(), None, status, i)
        }).collect();
        let runs = pack(ds.clone(), |run| mesh_topology("mesh1", run));
        assert!(runs.len() > 1, "a hundred nodes do not fit one message");
        assert_eq!(runs.iter().map(Vec::len).sum::<usize>(), 100, "every node travels");
        for r in &runs {
            assert!(mesh_topology("mesh1", r.clone()).encode().len() <= MAX_FRAME, "a run fits one message");
        }
    }

    /// CONTRACT (R-G2): a member known only from topology forwarded onto the mesh channel is held
    /// and never ages to silent; a forwarded copy of a member already heard leaves when it was
    /// heard alone, so a forwarded word is never liveness.
    #[test]
    fn forwarded_topology_holds_a_member_and_refreshes_no_liveness() {
        let digest_view = DigestView::default();
        let (topo, heard, birth) = (NodeId::mint(), NodeId::mint(), IncarnationId::mint());
        let t0 = Instant::now();
        assert!(digest_view.record_topology(digest(&topo, &birth, None, NodeStatus::Ready, 10)));
        assert!(digest_view.record_at(digest(&heard, &birth, None, NodeStatus::Ready, 10), t0));
        let far = t0 + Duration::from_secs(3600);
        assert_eq!(digest_view.get_at(topo.as_str(), far).unwrap().1, Duration::ZERO, "topology never ages");
        assert!(digest_view.current_at(digest_view.staleness_floor(), far).iter().any(|d| d.node.node_id == topo), "a topology member stays current");
        let before = digest_view.get_at(heard.as_str(), t0 + Duration::from_secs(10)).unwrap().1;
        assert!(digest_view.record_topology(digest(&heard, &birth, None, NodeStatus::Ready, 10)), "an equal forwarded copy");
        assert!(digest_view.record_topology(digest(&heard, &birth, None, NodeStatus::ConnectionsDeleting, 11)), "a newer forwarded copy");
        let after = digest_view.get_at(heard.as_str(), t0 + Duration::from_secs(10)).unwrap().1;
        assert_eq!(before, after, "a forwarded copy does not refresh when the member was heard");
        assert!(after >= Duration::from_secs(10), "the member stays as old as its last direct receipt");
    }

    /// CONTRACT (R-G2: forwarded topology carries topology, never liveness; gossip.md §6.1: a stopped
    /// status by itself and silence are not departure proof): a member known only from topology never
    /// ages, whatever status the topology lists it with. One listed `Leaving` is held and current as
    /// long as the topology holds it, like one listed ready. Must NOT happen: a topology-known member
    /// that is `Leaving` and an hour old, silent past the staleness floor and absent from `current_at`
    /// while a ready one of the same age stays.
    #[test]
    fn a_member_known_only_from_topology_never_ages_whatever_status_it_is_listed_with() {
        let digest_view = DigestView::default();
        let t0 = Instant::now();
        let listed: Vec<(NodeId, NodeStatus)> = [NodeStatus::Ready, NodeStatus::ConnectionsDeleting, NodeStatus::Leaving].into_iter().map(|s| (NodeId::mint(), s)).collect();
        for (id, status) in &listed {
            assert!(digest_view.record_topology_at(digest(id, &IncarnationId::mint(), None, *status, 10), t0));
        }
        let far = t0 + Duration::from_secs(3600);
        for (id, status) in &listed {
            assert_eq!(digest_view.get_at(id.as_str(), far).unwrap().1, Duration::ZERO, "a topology member listed {status:?} never ages");
        }
        let current = digest_view.current_at(digest_view.staleness_floor(), far);
        assert!(listed.iter().all(|(id, _)| current.iter().any(|d| &d.node.node_id == id)), "every topology member stays current: {current:?}");
    }

    #[test]
    fn a_forwarded_copy_keeps_a_member_heard_and_never_reverts_it() {
        let digest_view = DigestView::default();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        let t0 = Instant::now();
        let later = t0 + Duration::from_millis(30);
        assert!(digest_view.record_at(digest(&id, &birth, None, NodeStatus::Ready, 200), t0));
        assert!(digest_view.record_forwarded_at(digest(&id, &birth, None, NodeStatus::Ready, 200), later), "an equal forwarded copy");
        assert_eq!(digest_view.get_at(id.as_str(), later).unwrap().1, Duration::ZERO, "keeps the member heard");
        assert!(!digest_view.record_forwarded_at(digest(&id, &birth, None, NodeStatus::Pending, 100), later), "an older copy is not taken");
    }

    /// CONTRACT: every digest the view takes ticks `digest_changes`, a fresher digest of the same
    /// birth and status included (a renamed node's next digest is one); a digest the view refuses
    /// as older does not tick.
    #[test]
    fn every_taken_digest_ticks_digest_changes_and_a_refused_one_does_not() {
        let digest_view = DigestView::default();
        let mut digests = digest_view.digest_changes();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &birth, None, NodeStatus::Ready, 100)));
        assert!(digests.has_changed().unwrap(), "the first digest ticks");
        digests.borrow_and_update();
        let mut renamed = digest(&id, &birth, None, NodeStatus::Ready, 101);
        renamed.node.name = "2".into();
        assert!(digest_view.record(renamed));
        assert!(digests.has_changed().unwrap(), "a fresher digest of the same birth and status ticks");
        digests.borrow_and_update();
        assert!(!digest_view.record(digest(&id, &birth, None, NodeStatus::Ready, 100)));
        assert!(!digests.has_changed().unwrap(), "a refused older digest does not tick");
        assert!(digest_view.record_forwarded(digest(&id, &birth, None, NodeStatus::Ready, 102)));
        assert!(digests.has_changed().unwrap(), "a forwarded fresher digest ticks");
        digests.borrow_and_update();
        assert!(digest_view.record_topology(digest(&id, &birth, None, NodeStatus::Ready, 103)));
        assert!(digests.has_changed().unwrap(), "a topology copy that moved the held digest ticks");
    }

    #[test]
    fn a_new_or_changed_birth_ticks_and_a_fresher_digest_of_the_same_birth_does_not() {
        let digest_view = DigestView::default();
        let mut births = digest_view.birth_changes();
        let (id, first, second) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &first, None, NodeStatus::Pending, 100)));
        assert!(births.has_changed().unwrap(), "first held");
        births.borrow_and_update();
        assert!(digest_view.record(digest(&id, &first, None, NodeStatus::Ready, 200)));
        assert!(!births.has_changed().unwrap(), "the same birth, fresher and with a new status");
        assert!(digest_view.record_forwarded(digest(&id, &second, Some(first.clone()), NodeStatus::Pending, 50)));
        assert!(births.has_changed().unwrap(), "the next birth");
    }

    #[test]
    fn a_successor_birth_is_taken_and_its_predecessors_late_digests_are_not() {
        let digest_view = DigestView::default();
        let (id, first, second) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        assert!(digest_view.record(digest(&id, &first, None, NodeStatus::Ready, 500)));
        assert!(digest_view.record(digest(&id, &second, Some(first.clone()), NodeStatus::Ready, 100)), "a new birth, whatever its clock");
        assert!(!digest_view.record(digest(&id, &first, None, NodeStatus::Ready, 900)), "the superseded birth's late digest");
        assert_eq!(digest_view.get(id.as_str()).unwrap().0.node.incarnation, second);
    }

    /// CONTRACT (R-G2, gossip.md §3.2 and §6, node-recovery.md step 1): a node-admin judges a
    /// peer mesh's member from direct backbone receipt, and a member held from a backbone mesh topology
    /// ages from that receipt at the shared staleness floor, exactly as a member heard on its own
    /// mesh channel: no grace is added for it. Its silence is the real time since the receipt.
    #[test]
    fn a_member_held_from_a_backbone_aggregate_ages_at_the_staleness_floor() {
        let digest_view = DigestView::default();
        let floor = staleness_floor();
        let (direct, backbone) = (NodeId::mint(), NodeId::mint());
        let t0 = Instant::now();
        digest_view.record_at(digest(&direct, &IncarnationId::mint(), None, NodeStatus::Ready, 100), t0);
        assert!(digest_view.record_forwarded_at(digest(&backbone, &IncarnationId::mint(), None, NodeStatus::Ready, 100), t0));
        let within = t0 + floor;
        assert_eq!(digest_view.current_at(floor, within).len(), 2, "both are heard within the staleness floor");
        let past = t0 + floor + Duration::from_millis(1);
        let heard: Vec<String> = digest_view.current_at(floor, past).into_iter().map(|d| d.node.node_id.to_string()).collect();
        assert!(heard.is_empty(), "past the staleness floor neither the direct nor the backbone-held member is heard: {heard:?}");
        assert_eq!(digest_view.get_at(backbone.as_str(), past).unwrap().1, floor + Duration::from_millis(1), "its silence is the time since its backbone receipt");
        let silent: Vec<Duration> = digest_view.silent_held(past).into_iter().map(|(_, s)| s).collect();
        assert_eq!(silent, vec![floor + Duration::from_millis(1); 2], "every held member's silence is measured from its receipt");
    }

    /// CONTRACT (gossip.md §6.1: a stopped status by itself and silence are not departure proof;
    /// node-identity-and-state.md §4.1: `Leaving` is a status the routing rule reads; gossip.md §3.1:
    /// `TopologyFull.digests` are the digests as held): a member held `Leaving` stays current, and so in
    /// every mesh topology, for as long as any member does: until it has been silent past the staleness
    /// floor. One gossip interval and a millisecond after its last heartbeat it is current, heard
    /// directly or forwarded. Must NOT happen: a member whose status is `Leaving` and whose heartbeat is
    /// one interval old absent from `TopologyFull`, so a live node declared Leaving oscillates in and out of
    /// every peer's list with its own heartbeat.
    #[test]
    fn a_member_held_leaving_is_current_until_it_is_silent_past_the_staleness_floor() {
        let digest_view = DigestView::default();
        let (direct, forwarded) = (NodeId::mint(), NodeId::mint());
        let t0 = Instant::now();
        digest_view.record_at(digest(&direct, &IncarnationId::mint(), None, NodeStatus::Leaving, 100), t0);
        digest_view.record_forwarded_at(digest(&forwarded, &IncarnationId::mint(), None, NodeStatus::Leaving, 100), t0);
        let floor = digest_view.staleness_floor();
        assert_eq!(digest_view.current_at(floor, t0 + gossip_interval()).len(), 2, "within the interval it is current");
        assert_eq!(digest_view.current_at(floor, t0 + gossip_interval() + Duration::from_millis(1)).len(), 2, "a Leaving status is not a departure: one interval on it is still current");
        assert!(digest_view.current_at(floor, t0 + floor + Duration::from_millis(1)).is_empty(), "silent past the staleness floor it is no longer current, as any member");
    }

    /// The documented defaults: a 30 s staleness floor, a 2 s gossip interval and a 5 s backbone interval
    /// (gossip.md §3.2; Q267).
    #[test]
    fn the_windows_default_to_the_documented_values() {
        if std::env::var("RDM_STALENESS_MS").is_err() {
            assert_eq!(staleness_floor(), Duration::from_secs(30));
        }
        if std::env::var("RDM_GOSSIP_INTERVAL_MS").is_err() {
            assert_eq!(gossip_interval(), Duration::from_secs(2));
        }
        if std::env::var("RDM_BACKBONE_INTERVAL_MS").is_err() {
            assert_eq!(backbone_gossip_interval(), Duration::from_secs(5));
        }
    }

    /// A member of this node's own mesh is heard directly on the mesh channel. A mesh topology carrying
    /// it as well (its mesh primary's backbone `TopologyFull`) leaves it held as heard directly: it is
    /// among the members that checked in on its own channel (`heard_direct_since`), and a peer
    /// mesh's member in the same kind of mesh topology is held as heard on the backbone.
    #[test]
    fn an_own_mesh_member_in_an_aggregate_is_still_heard_directly() {
        let view = test_view("mesh1", "mesh1.node-admin.1");
        let fabric = FabricId::parse("fab000000001").unwrap();
        let id = NodeId::mint();
        let d = digest(&id, &IncarnationId::mint(), None, NodeStatus::Ready, 100);
        assert!(view.digest_view.record(d.clone()));
        let _ = view.take_topology_frame(&mesh_topology("mesh1", vec![d]), &fabric, Side::Backbone, &mut view.order.lock().unwrap());
        let (_, at, forwarded) = view.digest_view.inner.lock().unwrap().get(id.as_str()).cloned().unwrap();
        assert!(forwarded == Heard::Direct, "its own mesh's member is held as heard directly, never forwarded");
        let _ = at;
        // A peer mesh's member in the same kind of mesh topology is forwarded.
        let peer = NodeId::mint();
        let mut p = digest(&peer, &IncarnationId::mint(), None, NodeStatus::Ready, 100);
        { let at = rdm_core::PathName::new("mesh2.node", "1").unwrap(); p.node.path = at.path; p.node.name = at.name; }
        p.node.mesh = "mesh2".into();
        let _ = view.take_topology_frame(&mesh_topology("mesh2", vec![p]), &fabric, Side::Backbone, &mut view.order.lock().unwrap());
        assert!(view.digest_view.inner.lock().unwrap().get(peer.as_str()).unwrap().2 == Heard::Forwarded, "a peer mesh's member is forwarded");
    }

    /// CONTRACT: the version a node holds of a source Mesh names a projection its view already holds. A
    /// reader that sees version `k` and then reads the view finds the change of version `k` (or a
    /// later one), never an older one: a delta moves the version and the view together. Each version
    /// moves the member's address (its port names the version).
    #[test]
    fn a_held_source_version_never_runs_ahead_of_the_view_it_names() {
        let view = test_view("mesh1", "mesh1.node.1");
        let fabric = FabricId::parse("fab000000001").unwrap();
        let id = NodeId::mint();
        let incarnation = IncarnationId::mint();
        let at = |n: u64| {
            let mut d = digest(&id, &incarnation, None, NodeStatus::Ready, 1);
            { let at = rdm_core::PathName::new("mesh2.node", "1").unwrap(); d.node.path = at.path; d.node.name = at.name; }
            d.node.mesh = "mesh2".into();
            d.node.transport_addr = format!("127.0.0.1:{}", 10_000 + n).parse().unwrap();
            d
        };
        let publisher = PublisherId { incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() };
        // The node reads mesh2 at v1 (Q387: no full on its mesh channel), then the deltas move it.
        assert!(matches!(view.take_read_chunk(read_of("mesh2", 1, 1, vec![at(1)]), Side::Read, &fabric), Taken::Written(_)));
        const LAST: u64 = 20_000;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader = {
            let (view, id, stop) = (view.clone(), id.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut behind = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let Some((_, version)) = view.copies.lock().unwrap().held_version("mesh2") else { continue };
                    let held = view.digest_view.get(id.as_str()).map(|(d, _)| d.node.transport_addr.port() as u64 - 10_000).unwrap_or(0);
                    if held < version {
                        behind.push((version, held));
                    }
                }
                behind
            })
        };
        for v in 2..=LAST {
            let delta = delta_of("mesh2", &publisher, v - 1, v, v, vec![at(v)], vec![], vec![]);
            let _ = view.take_topology_frame(&delta, &fabric, Side::MeshChannel, &mut view.order.lock().unwrap());
        }
        stop.store(true, Ordering::Relaxed);
        let behind = reader.join().unwrap();
        assert!(behind.is_empty(), "a reader saw a version whose change the view did not hold yet: {} times, first (version, held) {:?}", behind.len(), behind.first());
    }

    /// CONTRACT (i143 export gate: a failed rejoin is bounded and never marks Dead): a channel left
    /// alone is refed on a doubling schedule that never waits more than one staleness floor and
    /// reaches it; the schedule is the refeed's only state, so no failed rejoin touches a member.
    #[test]
    fn a_failed_rejoin_is_retried_on_a_bounded_schedule() {
        let floor = staleness_floor();
        let mut wait = backbone_gossip_interval();
        let mut waits = vec![wait];
        for _ in 0..64 {
            wait = refeed_backoff(wait);
            waits.push(wait);
        }
        assert!(waits.windows(2).all(|w| w[1] >= w[0]), "never shrinks while alone: {waits:?}");
        assert!(waits.iter().all(|w| *w <= floor), "never waits past one staleness floor: {waits:?}");
        assert_eq!(*waits.last().unwrap(), floor, "settles at one floor, retried forever");
    }

    #[test]
    fn a_peer_that_leaves_the_live_list_and_returns_is_joined_again() {
        let joined = Mutex::new(BTreeSet::new());
        let (a, b) = (iroh::SecretKey::generate().public(), iroh::SecretKey::generate().public());
        assert_eq!(rejoin(&joined, &[a, b]), vec![a, b], "both joined");
        assert!(rejoin(&joined, &[a, b]).is_empty(), "a live peer is joined once");
        assert!(rejoin(&joined, &[b]).is_empty(), "a leaves the live list");
        assert_eq!(rejoin(&joined, &[a, b]), vec![a], "a returns and is joined again");
    }

    /// CONTRACT (Q58): a member this view stopped is expected silence, neither stale nor counted, and
    /// node.joined holds the birth again. A member that falls silent with no event this view applied is
    /// unexplained.
    #[test]
    fn a_member_this_view_stopped_is_expected_silence_and_a_silent_one_is_not() {
        let digest_view = DigestView::default();
        *digest_view.own_mesh.lock().unwrap() = "mesh1".into();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let mut d = digest(&id, &inc, None, NodeStatus::Ready, 100);
        { let at = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(); d.node.path = at.path; d.node.name = at.name; }
        d.node.mesh = "mesh1".into();
        let long_ago = Instant::now().checked_sub(Duration::from_secs(600)).expect("the clock has run ten minutes");
        assert!(digest_view.record_at(d.clone(), long_ago));
        assert_eq!(digest_view.unheard_unexplained("mesh1", "mesh1.node-admin.1").len(), 1, "a member silent with no event is unexplained");
        let mut again = digest(&id, &inc, None, NodeStatus::Ready, 200);
        { let at = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(); again.node.path = at.path; again.node.name = at.name; }
        again.node.mesh = "mesh1".into();
        assert!(digest_view.record(again), "heard again");
        assert!(digest_view.unheard_unexplained("mesh1", "mesh1.node-admin.1").is_empty(), "heard: nothing unexplained");
        assert!(digest_view.stopped(op(&id, &inc, "node.stop")), "the stop this view applied");
        assert!(digest_view.unheard_unexplained("mesh1", "mesh1.node-admin.1").is_empty(), "a stopped member is expected silence");
        assert!(digest_view.joined(&op(&id, &inc, "node.start")), "its start's node.joined holds it again");
        assert!(digest_view.unheard_unexplained("mesh1", "mesh1.node-admin.1").is_empty());
        let mut later = digest(&id, &inc, None, NodeStatus::Pending, 300);
        { let at = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(); later.node.path = at.path; later.node.name = at.name; }
        later.node.mesh = "mesh1".into();
        assert!(digest_view.record(later));
        assert!(digest_view.unheard_unexplained("mesh1", "mesh1.node-admin.1").is_empty());
    }

    fn born(mesh: &str, r#type: &str, ordinal: u32, at: u64) -> MeshDigest {
        let mut d = digest(&NodeId::mint(), &IncarnationId::mint(), None, NodeStatus::Ready, at);
        { let at = rdm_core::PathName::numbered(mesh, r#type, ordinal); d.node.path = at.path; d.node.name = at.name; }
        d.node.mesh = mesh.into();
        d.node.r#type = r#type.to_string();
        d
    }

    /// CONTRACT: `mesh-left` departs every birth the mesh held, through the departure a node's own
    /// stop takes: the births leave the active topology and are held deleted for the deleted
    /// retention and not a moment longer; nothing is kept of the mesh;
    /// hearing it again departs nothing a second time.

    /// CONTRACT (R4, Luke 2026-10-11; lifecycles/mesh-delete.md "Every stopping member publishes its
    /// own Leaving … carries that same Leaving digest in its final `TopologyFull` over the
    /// node-admin backbone channel"): a peer mesh learns that a leaving mesh's members are leaving
    /// only from each member's own state. A node that joins a peer mesh after those members said
    /// ConnectionsDeleting and Leaving holds their own states from its mesh primary's topology read (op
    /// `0x1E`) and forwarder replay, and selects none of them for new work; a member still saying
    /// Ready is routed by that state alone, with no mesh-level hold.
    #[test]
    fn a_node_joining_after_the_leaving_meshs_members_said_connections_deleting_and_leaving_holds_their_own_states_and_routes_none_of_them() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let m1 = MeshId::mint();
        let (admin, connections_deleting, leaving) = (born("mesh1", "node-admin", 1, 1), born("mesh1", "node", 1, 1), born("mesh1", "node", 2, 1));
        let publisher = PublisherId { incarnation: admin.node.incarnation.clone(), node_id: admin.node.node_id.clone() };
        let mesh_topology = |version: u64, digests: Vec<MeshDigest>| Frame::aggregate("mesh1", Some(m1.clone()), &crate::envelope::Birth::publisher(&FabricId::parse("fab000000001").unwrap(), &publisher.clone()), version, version, 0, 1, digests);
        let said = |d: &MeshDigest, status: NodeStatus| {
            let mut d = d.clone();
            d.status = status;
            d.digest_seq += 1;
            d.emitted_at_rafka_ms += 1;
            d
        };
        // mesh2's primary holds mesh1 and forwards it into mesh2; mesh-leaving is heard; then the
        // leaving primary's next mesh topology carries its members' own ConnectionsDeleting and Leaving.
        let primary = test_view("mesh2", "mesh2.node-admin.1");
        primary.primary.store(true, Ordering::Relaxed);
        // mesh1's primary published mesh1 ready before the leave; the joiner learns it
        // from the replay.
        // As a peer receives it: authored by its publisher.
        primary.take(&Frame::MeshStatus { mesh: "mesh1".into(), status: "ready".into(), publisher: "mesh1.node-admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 5 }.authored(&test_author(), 1, 5), &fabric, "backbone");
        for (version, digests) in [(1, vec![admin.clone(), connections_deleting.clone(), leaving.clone()]), (2, vec![admin.clone(), said(&connections_deleting, NodeStatus::ConnectionsDeleting), said(&leaving, NodeStatus::Leaving)])] {
            let taken = primary.take_topology_frame(&mesh_topology(version, digests), &fabric, Side::Backbone, &mut primary.order.lock().unwrap());
            let written = taken.source.expect("a complete full written to the topology view");
            let _ = primary.forwarder.lock().unwrap().source("mesh2.node-admin.1", &written, version);
        }
        // A new mesh2 node joins: it reads the primary's topology (0x1E), then hears its replay.
        let joiner = test_view("mesh2", "mesh2.node.9");
        for s in primary.held_sources_of(None) {
            let chunk = crate::aggregate::Chunk { mesh: s.mesh.clone(), mesh_id: s.mesh_id.clone(), author: None, topology_version: s.topology_version, time_ms: 1, chunk_index: 0, chunk_count: 1, digests: s.full().digests(), runtimes: Vec::new() };
            let prior = Prior::of(&joiner.copies.lock().unwrap(), &chunk.mesh);
            let taken = joiner.copies.lock().unwrap().take_read_chunk(chunk);
            if let crate::aggregate::Taken::Written(w) = taken {
                joiner.written(&w, &fabric, Side::Read, prior);
            }
        }
        let mut replay = primary.digest_view.statuses.held_frames(&fabric, "mesh2.node-admin.1");
        replay.extend(primary.forwarder.lock().unwrap().replay(3));
        for f in &replay {
            match f.data() {
                Frame::TopologyFull { .. } | Frame::TopologyDelta { .. } => {
                    joiner.take_topology_frame(f, &fabric, Side::MeshChannel, &mut joiner.order.lock().unwrap());
                }
                _ => {
                    joiner.take(f, &fabric, "mesh-channel");
                }
            }
        }
        let status = |d: &MeshDigest| joiner.digest_view.get(d.node.node_id.as_str()).map(|(h, _)| h.status);
        assert_eq!(
            (status(&admin), status(&connections_deleting), status(&leaving)),
            (Some(NodeStatus::Ready), Some(NodeStatus::ConnectionsDeleting), Some(NodeStatus::Leaving)),
            "the joiner installed each member at its address with the member's own state"
        );
        assert!(!joiner.digest_view.routable(connections_deleting.node.node_id.as_str()), "a member saying ConnectionsDeleting takes no new work");
        assert!(!joiner.digest_view.routable(leaving.node.node_id.as_str()), "a member saying Leaving takes no new work");
        assert!(joiner.digest_view.routable(admin.node.node_id.as_str()), "a member still saying Ready, of a mesh the joiner holds ready, is selected");
    }

    /// CONTRACT (gossip.md §3.3: "The mesh-primary puts that same delta into its Mesh"; "Mesh delete":
    /// the last admin's final mesh topology is its last gossip): a primary whose copy writes a deleted
    /// mesh's final full from the backbone puts the delta it wrote into its Mesh, from the version the
    /// copy held, which is the version it last forwarded. The write departs the mesh's births and
    /// leaves what the forwarder forwarded; a node of the Mesh at that version applies the differential
    /// and holds the last admin deleted. The forwarder forgets the mesh at its `MeshLeft` receipt.
    /// Must NOT happen: the final differential from base 0, and a version gap at a node that holds the
    /// version before it.
    #[test]
    fn a_deleted_meshs_final_mesh_topology_goes_into_the_mesh_as_the_differential_from_the_version_last_published() {
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let m1 = MeshId::mint();
        let (admin, rpc) = (born("mesh1", "node-admin", 1, 1), born("mesh1", "node", 1, 1));
        let publisher = PublisherId { incarnation: admin.node.incarnation.clone(), node_id: admin.node.node_id.clone() };
        // mesh1's aggregate by its last admin: each node with its status (Q390).
        let mesh_topology = |version: u64, digests: Vec<MeshDigest>| Frame::aggregate("mesh1", Some(m1.clone()), &author_of(&publisher), version, version, 0, 1, digests);
        let mut last = admin.clone();
        last.status = NodeStatus::Deleting;
        last.digest_seq += 1;
        let primary = test_view("mesh2", "mesh2.node-admin.1");
        primary.primary.store(true, Ordering::Relaxed);
        primary.forwarder.lock().unwrap().set_role(true);
        let member = test_view("mesh2", "mesh2.node.1");
        // What the primary puts into its Mesh for a full its copy wrote from the backbone (backbone.rs).
        let forward = |frame: &Frame| -> Vec<Frame> {
            let taken = primary.take_topology_frame(frame, &fabric, Side::Backbone, &mut primary.order.lock().unwrap());
            let written = taken.source.expect("a complete full written to the topology view");
            let (mesh, version) = (written.mesh.clone(), written.topology_version);
            let held = primary.forwarder.lock().unwrap().forwarded_version(&mesh);
            assert_eq!(held, (version > 1).then_some(1), "the write leaves what the forwarder last forwarded of {mesh}");
            assert_eq!(written.base_version, held.unwrap_or(0), "the copy held what the forwarder last forwarded");
            match primary.forwarder.lock().unwrap().source("mesh2.node-admin.1", &written, version) {
                crate::aggregate::Forward::Delta(frames) => frames,
                crate::aggregate::Forward::Nothing(why) => panic!("mesh1 at v{version} goes into the Mesh: {why}"),
            }
        };
        let base_of = |frames: &[Frame]| -> Vec<(u64, u64)> {
            frames.iter().map(|f| match f.data() { Frame::TopologyDelta { base_version, .. } => (*base_version, f.delta_version().expect("an authored differential")), other => panic!("not a differential: {other:?}") }).collect()
        };
        let first = forward(&mesh_topology(1, vec![admin.clone(), rpc.clone()]));
        assert_eq!(base_of(&first), vec![(0, 1)], "mesh1's first differential into the Mesh is from 0");
        for f in &first {
            member.take_topology_frame(f, &fabric, Side::MeshChannel, &mut member.order.lock().unwrap());
        }
        assert_eq!(member.copies.lock().unwrap().held_version("mesh1").map(|(_, v)| v), Some(1));

        // mesh1's last admin's final aggregate: itself the only node not listed deleted, deleting; the
        // other node listed deleted.
        let last_one = forward(&mesh_topology(2, vec![last.clone(), listed(&rpc, NodeStatus::Deleted)]));
        assert_eq!(base_of(&last_one), vec![(1, 2)], "the final differential is from the version last published, never from 0");
        assert!(primary.digest_view.is_deleted(admin.node.node_id.as_str()), "the primary's write holds the last admin deleted");
        for f in &last_one {
            member.take_topology_frame(f, &fabric, Side::MeshChannel, &mut member.order.lock().unwrap());
        }
        assert!(spans.named("rdm.mesh.topology.update.via-gap").is_empty(), "no node of the Mesh takes a version gap: {:?}", spans.named("rdm.mesh.topology.update.via-gap"));
        assert_eq!(member.copies.lock().unwrap().held_version("mesh1").map(|(_, v)| v), Some(2), "the member applied the final differential at its base");
        assert!(member.digest_view.is_deleted(admin.node.node_id.as_str()), "the member holds the last admin deleted");

        // The MeshLeft receipt: the forwarder forgets the mesh, and so does the one copy of it, so no read
        // is served a mesh that left (canonical_ids red at d8fdf110e: the fabric-primary served the removed
        // mesh2 to the recreated mesh2's first admin, which swept its births).
        assert_eq!(primary.copies.lock().unwrap().held_version("mesh1").map(|(_, v)| v), Some(2), "before MeshLeft the copy holds the final full");
        primary.digest_view.mesh_left("mesh1", "b9", 9);
        assert_eq!(primary.forwarder.lock().unwrap().forwarded_version("mesh1"), None, "at MeshLeft the forwarder forgets what it forwarded of mesh1");
        assert_eq!(primary.copies.lock().unwrap().held_version("mesh1"), None, "at MeshLeft the copy of mesh1 is forgotten");
        assert!(primary.held_sources_of(Some("mesh1")).is_empty(), "a read of the primary is served nothing of mesh1");
    }

    /// mesh2's nodes at v41 (three, Ready) and at v43: the first connections-deleting, the second
    /// gone, a fourth added.
    fn mesh2_at_41_and_43() -> (Vec<MeshDigest>, Vec<MeshDigest>) {
        let (n1, n2, n3, n4) = (born("mesh2", "node", 1, 1), born("mesh2", "node", 2, 1), born("mesh2", "node", 3, 1), born("mesh2", "node", 4, 1));
        let v43 = vec![listed(&n1, NodeStatus::ConnectionsDeleting), n3.clone(), n4];
        (vec![n1, n2, n3], v43)
    }

    /// `(base_version, topology_version, added, changed, removed)` of a one-chunk differential.
    fn moved_by(frames: &[Frame]) -> (u64, u64, usize, usize, usize) {
        assert_eq!(frames.len(), 1, "one chunk");
        let Frame::TopologyDelta { base_version, added, changed, removed, .. } = frames[0].data() else { panic!("a differential") };
        (*base_version, frames[0].delta_version().expect("an authored differential"), added.len(), changed.len(), removed.len())
    }

    /// CONTRACT (gossip.md §3.3 "A node-admin holds one copy of each peer Mesh and writes only the
    /// delta": "a node-admin that holds source version 41 and receives 43 writes only the difference,
    /// `base_version = 41, topology_version = 43` ... The mesh-primary puts that same delta into its
    /// Mesh"; Q409, Q410): mesh1's mesh-primary holds mesh2 at v41 in its topology view and the
    /// backbone brings mesh2's full at v43. The full writes only its difference from the copy:
    /// `rdm.mesh.topology.update.via-full` names base 41, version 43 and one node added, one changed and
    /// one removed; the copy is then at 43 and holds exactly the full's nodes; the differential the
    /// mesh-primary puts into mesh1 is that delta from 41; and mesh1's ordinary node holding 41 applies
    /// it at its base and holds what the copy holds. Must NOT happen: a second copy the forwarder diffs
    /// against; a full that replaces the copy; a forwarded base other than the version the copy held.
    #[test]
    fn a_node_admin_holding_a_peer_mesh_at_41_writes_only_the_delta_of_its_full_at_43_and_forwards_that_delta() {
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let mesh2_primary = publisher_of(&born("mesh2", "node-admin", 1, 1));
        let (v41, v43) = mesh2_at_41_and_43();
        let primary = test_view("mesh1", "mesh1.node-admin.1");
        primary.primary.store(true, Ordering::Relaxed);
        primary.forwarder.lock().unwrap().set_role(true);
        let member = test_view("mesh1", "mesh1.node.1");
        let forward = |frame: &Frame| -> (Box<crate::aggregate::Written>, Vec<Frame>) {
            let written = primary.take_topology_frame(frame, &fabric, Side::Backbone, &mut primary.order.lock().unwrap()).source.expect("mesh2's full is written to the topology view");
            match primary.forwarder.lock().unwrap().source("mesh1.node-admin.1", &written, written.topology_version) {
                crate::aggregate::Forward::Delta(frames) => (written, frames),
                crate::aggregate::Forward::Nothing(why) => panic!("mesh2 at v{} goes into mesh1: {why}", written.topology_version),
            }
        };
        let (added, first) = forward(&aggregate_of("mesh2", &mesh2_primary, 41, v41.clone()));
        assert_eq!((added.first, added.base_version, moved_by(&first)), (true, 0, (0, 41, 3, 0, 0)), "mesh2's first full adds it to the topology view and goes in from 0");
        for f in &first {
            member.take_topology_frame(f, &fabric, Side::MeshChannel, &mut member.order.lock().unwrap());
        }

        let (written, frames) = forward(&aggregate_of("mesh2", &mesh2_primary, 43, v43.clone()));
        assert_eq!((written.first, written.base_version, written.topology_version, written.delta.added.len(), written.delta.changed.len(), written.delta.removed.len()), (false, 41, 43, 1, 1, 1), "the full at 43 writes only its difference from the copy at 41");
        let full_spans: Vec<(String, String, String, String, String, String)> = spans
            .named("rdm.mesh.topology.update.via-full")
            .into_iter()
            .filter(|f| f["node"] == "mesh1.node-admin.1" && f["mesh"] == "mesh2" && f["topology_version"] == "43")
            .map(|f| (f["channel"].clone(), f["base_version"].clone(), f["added"].clone(), f["changed"].clone(), f["removed"].clone(), f["members"].clone()))
            .collect();
        assert_eq!(full_spans, vec![("backbone".to_string(), "41".to_string(), "1".to_string(), "1".to_string(), "1".to_string(), "3".to_string())], "via-full carries the write's base and the diff's counts");
        {
            let copies = primary.copies.lock().unwrap();
            assert_eq!(copies.held_version("mesh2").map(|(_, v)| v), Some(43), "the copy is at 43");
            let held = copies.held_full("mesh2").expect("the copy of mesh2");
            assert!(held.same_topology(&Full::new(v43.clone(), vec![], vec![])), "the copy holds exactly the full's nodes: {:?}", held.digests());
        }
        assert_eq!(moved_by(&frames), (41, 43, 1, 1, 1), "the differential put into mesh1 is the delta the full wrote, from the version the copy held");

        for f in &frames {
            member.take_topology_frame(f, &fabric, Side::MeshChannel, &mut member.order.lock().unwrap());
        }
        assert!(spans.named("rdm.mesh.topology.update.via-gap").is_empty(), "the member takes no gap: {:?}", spans.named("rdm.mesh.topology.update.via-gap"));
        let copies = member.copies.lock().unwrap();
        assert_eq!(copies.held_version("mesh2").map(|(_, v)| v), Some(43), "mesh1's node applied 41 -> 43 at its base");
        assert!(copies.held_full("mesh2").expect("the member's copy").same_topology(&Full::new(v43, vec![], vec![])), "the member holds what the mesh-primary's copy holds");
    }

    /// CONTRACT (gossip.md §3.3: "A node-admin that takes the mesh-primary role already holds what its
    /// Mesh reached and forwards from its copy"; rule 26: "a node-admin that becomes the primary forwards
    /// from the copy it holds, as a differential, never a full (Q409)"; Q409: every node-admin joins the
    /// backbone at its boot, and a takeover tops up nothing): mesh1's mesh-primary forwards mesh2 at v41
    /// to mesh1's ordinary node; mesh1's other node-admin, no primary, writes the same full to its own
    /// copy from the backbone and forwards nothing. It takes the role; mesh2's full at v43 writes its
    /// delta from 41 to its copy, and the differential it puts into mesh1 has base 41, the version its
    /// copy held. mesh1's node, at 41, writes it at its base: no gap, no top-up. Must NOT happen: the
    /// new mesh-primary pushing what it holds at the takeover; a differential from 0 or from another
    /// base than the copy's; the ordinary node desynchronized by the takeover.
    #[test]
    fn a_node_admin_taking_the_mesh_primary_role_forwards_from_the_copy_it_holds_and_its_mesh_writes_it_with_no_top_up() {
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let mesh2_primary = publisher_of(&born("mesh2", "node-admin", 1, 1));
        let (v41, v43) = mesh2_at_41_and_43();
        let (old, next, member) = (test_view("mesh1", "mesh1.node-admin.1"), test_view("mesh1", "mesh1.node-admin.2"), test_view("mesh1", "mesh1.node.1"));
        old.primary.store(true, Ordering::Relaxed);
        old.forwarder.lock().unwrap().set_role(true);
        let full41 = aggregate_of("mesh2", &mesh2_primary, 41, v41);
        let written = old.take_topology_frame(&full41, &fabric, Side::Backbone, &mut old.order.lock().unwrap()).source.expect("the old primary writes mesh2");
        let crate::aggregate::Forward::Delta(first) = old.forwarder.lock().unwrap().source("mesh1.node-admin.1", &written, 1) else { panic!("mesh2 goes into mesh1") };
        for f in &first {
            member.take_topology_frame(f, &fabric, Side::MeshChannel, &mut member.order.lock().unwrap());
        }
        assert_eq!(member.copies.lock().unwrap().held_version("mesh2").map(|(_, v)| v), Some(41));
        // mesh1's other node-admin holds the backbone from its boot: its copy writes the same full.
        // It holds no role, so its forward path decides nothing (forward_path.rs).
        let at_next = next.take_topology_frame(&full41, &fabric, Side::Backbone, &mut next.order.lock().unwrap()).source.expect("the other node-admin writes mesh2");
        assert_eq!((at_next.first, at_next.topology_version, next.forwarder.lock().unwrap().role()), (true, 41, None), "its copy holds mesh2 at 41, and it holds no role");

        // The old primary is lost; the other node-admin takes the role, and tops up nothing.
        next.primary.store(true, Ordering::Relaxed);
        assert!(next.forwarder.lock().unwrap().set_role(true));
        assert_eq!(next.forwarder.lock().unwrap().forwarded_version("mesh2"), None, "taking the role forwards nothing of what it holds");
        let written = next.take_topology_frame(&aggregate_of("mesh2", &mesh2_primary, 43, v43.clone()), &fabric, Side::Backbone, &mut next.order.lock().unwrap()).source.expect("the new primary writes mesh2 at 43");
        assert_eq!((written.base_version, written.topology_version), (41, 43), "the copy held 41");
        let crate::aggregate::Forward::Delta(frames) = next.forwarder.lock().unwrap().source("mesh1.node-admin.2", &written, 2) else { panic!("the move goes into mesh1") };
        assert_eq!(moved_by(&frames), (41, 43, 1, 1, 1), "the new primary's first differential is from the version its copy held");
        for f in &frames {
            member.take_topology_frame(f, &fabric, Side::MeshChannel, &mut member.order.lock().unwrap());
        }
        assert!(spans.named("rdm.mesh.topology.update.via-gap").is_empty(), "the ordinary node takes no gap: {:?}", spans.named("rdm.mesh.topology.update.via-gap"));
        assert!(member.copies.lock().unwrap().desynced().is_empty(), "nothing is topped up");
        let copies = member.copies.lock().unwrap();
        assert_eq!(copies.held_version("mesh2").map(|(_, v)| v), Some(43), "the ordinary node at 41 writes the new primary's delta at its base");
        assert!(copies.held_full("mesh2").expect("the member's copy").same_topology(&Full::new(v43, vec![], vec![])));
    }

    /// CONTRACT (gossip.md §3.1 "four orderings": a peer mesh's member keeps the Digest sequence this view
    /// accepted of it, a read's or a join's; §3.3, Q409: a full writes only its delta to the one copy):
    /// a node-admin holds mesh2 at v10 from a backbone full, whose entries carry no Digest sequence; a
    /// topology read of v10 carries the member's sequence (7), and the digest view holds it. The read's
    /// delta from the copy is empty, and the copy keeps the read's Digest order of the birth; so mesh2's
    /// next differential (10 -> 11, the member's new address) moves the member the digest view holds.
    /// Must NOT happen: the copy keeping the backbone full's sequence 0, so the differential's moved
    /// birth is older than the digest view's and the view keeps the old address (publication_readrepair
    /// red at d8fdf110e).
    #[test]
    fn a_read_at_the_held_version_leaves_its_digest_order_in_the_copy_so_the_next_delta_moves_the_view() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = test_view("mesh1", "mesh1.node-admin.2");
        let mesh2_primary = publisher_of(&born("mesh2", "node-admin", 1, 1));
        let mut member = born("mesh2", "node", 1, 7);
        member.digest_seq = 7;
        assert!(hear(&mut view.order.lock().unwrap(), &view, &aggregate_of("mesh2", &mesh2_primary, 10, vec![member.clone()]), &fabric, Side::Backbone));
        assert!(matches!(view.take_read_chunk(read_of("mesh2", 10, 10, vec![member.clone()]), Side::ReadPeer, &fabric), Taken::Written(w) if w.delta.is_empty()), "the read of the version held writes an empty delta");
        assert_eq!(view.copies.lock().unwrap().held_full("mesh2").and_then(|f| f.digests().into_iter().find(|d| d.node.node_id == member.node.node_id)).map(|d| d.digest_seq), Some(7), "the copy keeps the read's Digest order of the birth");
        let mut moved = member.clone();
        moved.node.transport_addr = "127.0.0.1:12011".parse().unwrap();
        let by = publisher_of(&born("mesh1", "node-admin", 1, 1));
        assert!(hear(&mut view.order.lock().unwrap(), &view, &delta_of("mesh2", &by, 10, 11, 11, vec![moved.clone()], vec![], vec![]), &fabric, Side::MeshChannel));
        assert_eq!(view.copies.lock().unwrap().held_version("mesh2").map(|(_, v)| v), Some(11));
        assert_eq!(view.digest_view.get(member.node.node_id.as_str()).map(|(d, _)| d.node.transport_addr), Some(moved.node.transport_addr), "the differential moves the member the digest view holds");
    }

    /// Two node-admins of mesh1, both Ready: `(lower NodeId, higher NodeId)`, named admin.2 and
    /// admin.1 so the role can sit on either.
    fn two_admins(digest_view: &DigestView, at: Instant) -> (MeshDigest, MeshDigest) {
        let (mut a, mut b) = (NodeId::mint(), NodeId::mint());
        if b < a {
            std::mem::swap(&mut a, &mut b);
        }
        let mut lower = digest(&a, &IncarnationId::mint(), None, NodeStatus::Ready, 10);
        { let at = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(); lower.node.path = at.path; lower.node.name = at.name; }
        lower.node.mesh = "mesh1".into();
        let mut higher = digest(&b, &IncarnationId::mint(), None, NodeStatus::Ready, 10);
        { let at = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(); higher.node.path = at.path; higher.node.name = at.name; }
        higher.node.mesh = "mesh1".into();
        assert!(digest_view.record_at(lower.clone(), at) && digest_view.record_at(higher.clone(), at));
        (lower, higher)
    }

    fn primary_of(d: &MeshDigest, revision: u64) -> Primary {
        Primary { mesh: d.node.mesh.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision }
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md Part B1):
    /// a non-admin node type's primary record is held under its own (mesh, kind), apart from the mesh-primary
    /// role of the same mesh, by the same supersede rule; a committed update of a node-type primary role is held under the node type too. Must NOT happen: an rpc
    /// node-type primary's record overwriting the mesh-primary record of its mesh.
    #[test]
    fn a_node_type_primary_is_held_apart_from_its_meshs_mesh_primary() {
        
        let primaries = PrimaryView::default();
        let admin = Primary { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 4 };
        let rpc = Primary { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 1 };
        assert!(matches!(primaries.take(PrimaryRole::MeshPrimary, &admin), RoleTaken::Held { .. }));
        assert!(matches!(primaries.take(PrimaryRole::NodePrimary("node".into()), &rpc), RoleTaken::Held { previous: None }));
        assert_eq!(primaries.mesh("mesh1"), Some(admin.clone()), "the mesh-primary record stays its own");
        assert_eq!(primaries.node_type_primary("mesh1", "node"), Some(rpc.clone()));
        assert_eq!(primaries.node_type_primary("mesh1", "node-admin"), Some(admin.clone()), "the node-admin node-type primary role is the mesh-primary role");
        assert_eq!(primaries.node_type_primary("mesh1", "broker"), None);
        let older = Primary { revision: 0, ..rpc.clone() };
        assert!(matches!(primaries.take(PrimaryRole::NodePrimary("node".into()), &older), RoleTaken::Refused { .. }), "an older record is refused by the one supersede rule");
        // An update of the node-type primary is held under the node type.
        let next = Primary { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 2 };
        let taken = primaries.take_together(&[(PrimaryRole::NodePrimary("node".into()), next.clone())]);
        assert!(matches!(taken.as_slice(), [RoleTaken::Held { .. }]), "{taken:?}");
        assert_eq!(primaries.node_type_primary("mesh1", "node"), Some(next));
        assert_eq!(primaries.mesh("mesh1"), Some(admin), "and the mesh-primary record is untouched");
    }

    /// CONTRACT (gossip.md §3.3, R-A2): a member tops up from its own mesh's current primary, the
    /// birth its held primary record names, never from an election of its own. With the role on the
    /// admin whose NodeId is the higher of two Ready admins, the member's primary is that primary.
    #[test]
    fn a_members_mesh_primary_is_the_recorded_primary_not_the_lowest_ready_admin() {
        let digest_view = DigestView::default();
        let (_lower, primary) = two_admins(&digest_view, Instant::now());
        digest_view.primaries.take(PrimaryRole::MeshPrimary, &primary_of(&primary, 3));
        assert_eq!(digest_view.mesh_primary("mesh1").ok().map(|d| d.node.location()), Some(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()));
    }

    /// CONTRACT (R-EL1): a primary that is ConnectionsDeleting keeps its role, so the member still tops up from it.
    #[test]
    fn a_connections_deleting_mesh_primary_is_still_the_members_mesh_primary() {
        let digest_view = DigestView::default();
        let (lower, primary) = two_admins(&digest_view, Instant::now());
        digest_view.primaries.take(PrimaryRole::MeshPrimary, &primary_of(&lower, 3));
        let mut connections_deleting = lower.clone();
        connections_deleting.status = NodeStatus::ConnectionsDeleting;
        connections_deleting.digest_seq += 1;
        connections_deleting.emitted_at_rafka_ms += 1;
        assert!(digest_view.record(connections_deleting));
        let _ = primary;
        assert_eq!(digest_view.mesh_primary("mesh1").ok().map(|d| d.node.location()), Some(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap()));
    }

    /// CONTRACT: the primary's digest aged past the staleness floor does not move the role:
    /// only a new record or a proven loss does, so the member still addresses the primary.
    #[test]
    fn a_mesh_primary_unheard_past_the_staleness_floor_is_still_the_members_mesh_primary() {
        let digest_view = DigestView::default();
        let long_ago = Instant::now().checked_sub(Duration::from_secs(600)).expect("the clock has run ten minutes");
        let (lower, _higher) = two_admins(&digest_view, long_ago);
        digest_view.primaries.take(PrimaryRole::MeshPrimary, &primary_of(&lower, 3));
        assert_eq!(digest_view.mesh_primary("mesh1").ok().map(|d| d.node.location()), Some(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap()));
    }

    /// CONTRACT: a node that cannot address its mesh's primary says which fact stops it, with the
    /// record it holds: no record of its mesh, a record naming a birth it holds no digest of (or a
    /// later birth of that node), and a record naming a birth held as proven gone.
    #[test]
    fn a_mesh_primary_that_cannot_be_addressed_names_why() {
        let digest_view = DigestView::default();
        assert_eq!(digest_view.mesh_primary("mesh1").unwrap_err(), NoMeshPrimary::NoPrimaryRecord { mesh: "mesh1".into() });
        let unheard = Primary { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 1 };
        digest_view.primaries.take(PrimaryRole::MeshPrimary, &unheard);
        assert_eq!(digest_view.mesh_primary("mesh1").unwrap_err(), NoMeshPrimary::PrimaryNotHeld { primary: unheard, held_incarnation: None, deleted: false });

        let digest_view = DigestView::default();
        let (lower, _) = two_admins(&digest_view, Instant::now());
        let mut earlier = primary_of(&lower, 2);
        earlier.incarnation = IncarnationId::mint();
        digest_view.primaries.take(PrimaryRole::MeshPrimary, &earlier);
        let err = digest_view.mesh_primary("mesh1").unwrap_err();
        assert_eq!((err.reason(), err.primary()), ("primary-not-held", Some(&earlier)));
        assert_eq!(err, NoMeshPrimary::PrimaryNotHeld { primary: earlier, held_incarnation: Some(lower.node.incarnation.clone()), deleted: false });
        let held = primary_of(&lower, 3);
        digest_view.primaries.take(PrimaryRole::MeshPrimary, &held);
        assert_eq!(digest_view.mesh_primary("mesh1").map(|d| d.node.node_id), Ok(lower.node.node_id.clone()));
        digest_view.primaries.mark_gone(held.incarnation.clone());
        assert_eq!(digest_view.mesh_primary("mesh1").unwrap_err(), NoMeshPrimary::PrimaryGone { primary: held });
    }

    /// Every span opened while the capture is the thread's subscriber: its name and its fields.
    #[derive(Clone, Default)]
    struct OpenedSpans(Arc<Mutex<Vec<(String, BTreeMap<String, String>)>>>);

    struct SpanFields<'a>(&'a mut BTreeMap<String, String>);

    impl tracing::field::Visit for SpanFields<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_string(), format!("{value:?}").trim_matches('"').to_string());
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for OpenedSpans {
        fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _id: &tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            let mut fields = BTreeMap::new();
            attrs.record(&mut SpanFields(&mut fields));
            self.0.lock().unwrap().push((attrs.metadata().name().to_string(), fields));
        }
    }

    impl OpenedSpans {
        fn named(&self, name: &str) -> Vec<BTreeMap<String, String>> {
            self.0.lock().unwrap().iter().filter(|(n, _)| n == name).map(|(_, f)| f.clone()).collect()
        }
    }

    /// Opens the span every thread of the capture cell below opens, at one callsite.
    fn open_shared_callsite() {
        tracing::info_span!("capture.shared-callsite").in_scope(|| {});
    }

    /// CONTRACT: a cell's capture receives the span its own thread opens at a callsite that another
    /// thread, one with no subscriber of its own (an uncaptured cell's), opened first. tracing-core
    /// caches a callsite's interest for the whole process when the callsite is first hit; with one
    /// dispatcher registered it asks only the thread that hit it (tracing-core 0.1.36 callsite.rs,
    /// `Rebuilder::JustOne`), and that thread answers never. Must NOT happen: a cell's own span lost
    /// because an uncaptured cell hit its callsite first (the mesh-left cell lost every
    /// `rdm.mesh.topology.reject.via-mesh-left` span in the whole-crate run of gate e588aaea, where
    /// `an_old_aggregate_of_a_mesh_the_build_no_longer_names_installs_nothing` opens that callsite
    /// with no subscriber, and passed alone).
    #[test]
    fn a_capture_receives_its_own_span_at_a_callsite_another_thread_hit_first() {
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        std::thread::spawn(open_shared_callsite).join().unwrap();
        open_shared_callsite();
        assert_eq!(spans.named("capture.shared-callsite").len(), 1, "the capture's own span reached it");
    }

    fn op_at(path: rdm_core::PathName, mesh: &str, r#type: &str, id: &NodeId, inc: &IncarnationId, action: &str) -> LifecycleOp {
        LifecycleOp { path: path.path.clone(), name: path.name.clone(), mesh: mesh.into(), r#type: r#type.to_string(), ..op(id, inc, action) }
    }

    /// CONTRACT (gossip.md "Mesh delete": after mesh.deleted the last admin publishes itself "until it
    /// is stopped or deleted ... From the stop on the fabric-primary is that mesh's publisher of record
    /// ... the stopped admin in `in_flight` as NodeStopped ... until a start hands publishing back to
    /// the admin or a delete moves the birth to `deleted` as NodeDeleted ... After the deleted
    /// retention the mesh is empty and nothing publishes for it again"): a deleted mesh's listing is
    /// read from the view and the digest view alone. While the digest view holds its last admin's digest nothing is
    /// listed; from the admin's stop on the mesh's stops and retained departures are (its members'
    /// departures from the mesh.delete with them). A start hands it back: nothing is listed while the
    /// admin's digest is held again. Stopped again, then deleted: the stop, then the departure. Once
    /// the view names the mesh no more (its last birth deleted and the Build does not name it), its
    /// departures stay listed until the deleted retention ends them (the handover stem's republishes
    /// after mesh-left, `mesh_lifecycle__fabric_primary_handover` at :512). The own mesh and a mesh the
    /// view names live are never listed, nor a mesh the view does not name that holds only a stop
    /// (`deleted_mesh_aggregate::a_mesh_created_again_under_the_deleted_meshs_name_ends_the_deleted_meshs_aggregate`:
    /// "does not return"). After the deleted retention nothing is. Must NOT happen: a mesh listed while
    /// its admin is held unstopped, after a start, or after its departures aged out; a deleted mesh
    /// whose view entry went silent before its retention ended.

    /// CONTRACT (R-RX1, Luke 2026-10-12: no receiver refuses an input because of who sent it; gossip.md
    /// §3.1: a mesh status is "the held status, until the next change"): a mesh status is taken for
    /// what it carries, whatever its publisher's name resolves to here. The old mesh primary X deleted
    /// (its replace-node NodeDeleted is applied first); X's mesh status is taken by the node-admin
    /// joining the live mesh at X's (path, name) (a new birth Y, not yet in its own view), by a member
    /// where no live birth holds the name, by a birth its row renamed off the name, and by a node that
    /// holds Y's live birth under the name. The held status orders it: a change older than the one
    /// held is not taken, a repeat of it is not taken again. Must NOT happen: a status refused for its
    /// publisher's name (`via-deleted-birth` with `frame=mesh-status`).
    #[test]
    fn a_mesh_status_is_taken_whatever_its_publishers_name_resolves_to_and_an_older_change_is_not() {
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let (x, x_inc) = (NodeId::mint(), IncarnationId::mint());
        let departure = op_at(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", &x, &x_inc, "node.delete");
        let forwarder_id = NodeId::mint();
        let status_at = |changed_at_rafka_ms: u64, status: &str| Frame::MeshStatus { mesh: "mesh2".into(), status: status.into(), publisher: "mesh2.node-admin.1".into(), forwarded_by: Some(forwarder_id.to_string()), changed_at_rafka_ms };
        let held = Some(MeshStatus { status: "ready".into(), publisher: "mesh2.node-admin.1".into(), changed_at_rafka_ms: 5 });

        let joiner = test_view("mesh2", "mesh2.node-admin.1");
        let member = test_view("mesh2", "mesh2.node.1");
        let renamed = test_view("mesh2", "mesh2.node-admin.1");
        *renamed.held_name.lock().unwrap() = Some(rdm_core::PathName::new("mesh2.node-admin.old", "1").unwrap());
        let (y, y_inc) = (NodeId::mint(), IncarnationId::mint());
        let mut y_birth = digest(&y, &y_inc, None, NodeStatus::Pending, 100);
        { let at = rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(); y_birth.node.path = at.path; y_birth.node.name = at.name; }
        y_birth.node.mesh = "mesh2".into();
        let peer = test_view("mesh2", "mesh2.node.2");
        assert!(peer.digest_view.record(y_birth));
        for (view, via) in [(&joiner, "entry-answer"), (&member, "mesh-channel"), (&renamed, "mesh-channel"), (&peer, "mesh-channel")] {
            assert!(view.digest_view.depart(departure.clone()));
            view.take(&status_at(5, "ready"), &fabric, via);
            assert_eq!(view.digest_view.statuses.mesh("mesh2"), held, "{}: the status is taken whatever its publisher's name resolves to", view.node);
            view.take(&status_at(3, "pending"), &fabric, via);
            assert_eq!(view.digest_view.statuses.mesh("mesh2"), held, "{}: an older change is not taken", view.node);
        }
        let refused: Vec<_> = spans.named("rdm.mesh.topology.reject.via-deleted-birth").into_iter().filter(|f| f.get("frame").map(String::as_str) == Some("mesh-status")).collect();
        assert!(refused.is_empty(), "no status is refused for its publisher's name: {refused:?}");
        let heard: Vec<(String, String)> = spans.named("rdm.mesh.topology.update.via-mesh-status").into_iter().map(|f| (f["node"].clone(), f["changed_at_rafka_ms"].clone())).collect();
        assert_eq!(heard.len(), 8, "each status heard at each node opens its span, none is refused before it: {heard:?}");
    }

    /// CONTRACT (R-NS1 change 3): a NodeStopped arriving after its birth's mesh left, and a
    /// NodeDeleted other than the departure held, are refused by the deleted target by name and
    /// resurrect nothing; a copy of the departure held (every mesh topology carries it for the retention)
    /// is a copy, not a refusal.
    #[test]
    fn a_lifecycle_overlay_arriving_after_its_mesh_left_is_refused_by_the_deleted_retention_by_name() {
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let digest_view = DigestView::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let mesh_id = MeshId::mint();
        let mut d = digest(&id, &inc, None, NodeStatus::Leaving, 100);
        { let at = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(); d.node.path = at.path; d.node.name = at.name; }
        d.node.mesh = "mesh1".into();
        digest_view.hold_mesh_id(&d.node.mesh, Some(&mesh_id));
        assert!(digest_view.record(d));
        assert!(digest_view.depart(LifecycleOp { build_id: "b1".into(), ..op_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", &id, &inc, "node.delete") }));
        let late_stop = op_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", &id, &inc, "node.stop");
        let late_stop = LifecycleOp { build_id: "b0".into(), ..late_stop };
        assert!(!digest_view.stopped(late_stop.clone()), "a late NodeStopped opens nothing");
        assert!(digest_view.in_flight().is_empty() && digest_view.get(id.as_str()).is_none(), "nothing is resurrected");
        let late_delete = LifecycleOp { build_id: "b0".into(), ..op_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", &id, &inc, "node.delete") };
        assert!(!digest_view.depart(late_delete.clone()), "a late NodeDeleted departs nothing again");
        let held = digest_view.deleted().into_iter().next().unwrap();
        assert!(!digest_view.depart(held), "a copy of the departure held");
        let refused = spans.named("rdm.mesh.topology.reject.via-deleted-birth");
        assert_eq!(refused.len(), 2, "one refusal per late frame, none for the copy: {refused:?}");
        assert_eq!((refused[0]["frame"].as_str(), refused[0]["operation"].as_str(), refused[0]["build_id"].as_str()), ("node-stopped", "node.stop", "b0"));
        assert_eq!((refused[1]["frame"].as_str(), refused[1]["operation"].as_str()), ("node-deleted", "node.delete"));
        assert!(refused.iter().all(|f| f["node_id"] == id.to_string() && f["incarnation_id"] == inc.as_str()));
    }

    /// CONTRACT (R-NS1 change 3): a departure refused by the deleted target is refused by name once.
    /// Its publisher carries it in every mesh topology for the retention (it never held the departure this
    /// view holds), so each later copy of the same refused departure is a copy of a refusal already
    /// made: refused again, opening no second span. Must NOT happen: one refusal span per mesh topology
    /// copy (canonical_ids: 44 refusals of one departure at one admin).
    #[test]
    fn a_departure_refused_by_the_deleted_retention_is_refused_by_name_once_and_its_copies_silently() {
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let digest_view = DigestView::default();
        let (id, inc) = (NodeId::mint(), IncarnationId::mint());
        let mesh_id = MeshId::mint();
        let mut d = digest(&id, &inc, None, NodeStatus::Leaving, 100);
        { let at = rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(); d.node.path = at.path; d.node.name = at.name; }
        d.node.mesh = "mesh2".into();
        digest_view.hold_mesh_id(&d.node.mesh, Some(&mesh_id));
        assert!(digest_view.record(d));
        assert!(digest_view.depart(LifecycleOp { build_id: "b1".into(), ..op_at(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", &id, &inc, "node.delete") }));
        let replaced = LifecycleOp { build_id: "b2".into(), ..op_at(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", &id, &inc, "node.delete") };
        for copy in 0..3 {
            assert!(!digest_view.depart(replaced.clone()), "copy {copy} of the refused departure departs nothing again");
        }
        let refused = spans.named("rdm.mesh.topology.reject.via-deleted-birth");
        assert_eq!(refused.len(), 1, "one refusal by name, its copies silent: {refused:?}");
        assert_eq!((refused[0]["frame"].as_str(), refused[0]["operation"].as_str(), refused[0]["build_id"].as_str()), ("node-deleted", "node.delete", "b2"));
        // Another departure of the same birth is a fact of its own: refused by name once.
        let deleted = LifecycleOp { build_id: "b3".into(), ..op_at(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", &id, &inc, "node.delete") };
        assert!(!digest_view.depart(deleted.clone()) && !digest_view.depart(deleted));
        assert_eq!(spans.named("rdm.mesh.topology.reject.via-deleted-birth").len(), 2, "each refused departure once");
    }

    /// A birth of `path` with a fresh NodeId and incarnation, ready, heartbeat `seq`.
    fn birth_at(path: rdm_core::PathName, mesh: &str, r#type: &str, seq: u64) -> MeshDigest {
        let mut d = digest(&NodeId::mint(), &IncarnationId::mint(), None, NodeStatus::Ready, seq);
        { let at = path; d.node.path = at.path; d.node.name = at.name; }
        d.node.mesh = mesh.into();
        d.node.r#type = r#type.to_string();
        d
    }

    /// One complete aggregate of `mesh` authored by `publisher` at `version`, published at Rafka-time
    /// `version`, listing `nodes` (a stop, removal or departure is the node's status, Q390).
    fn aggregate_of(mesh: &str, publisher: &PublisherId, version: u64, nodes: Vec<MeshDigest>) -> Frame {
        Frame::aggregate(mesh, None, &author_of(publisher), version, version, 0, 1, nodes)
    }

    /// One complete topology read of `mesh` at `version`, served at Rafka-time `at` (a node's own
    /// mesh reaches it by a read alone, Q387).
    fn read_of(mesh: &str, version: u64, at: u64, nodes: Vec<MeshDigest>) -> crate::aggregate::Chunk {
        crate::aggregate::Chunk { mesh: mesh.into(), mesh_id: None, author: None, topology_version: version, time_ms: at, chunk_index: 0, chunk_count: 1, digests: nodes, runtimes: Vec::new() }
    }

    fn publisher_of(d: &MeshDigest) -> PublisherId {
        PublisherId { incarnation: d.node.incarnation.clone(), node_id: d.node.node_id.clone() }
    }

    /// The birth `p` names, of the test fabric.
    fn birth_of(p: &PublisherId) -> crate::envelope::Birth {
        crate::envelope::Birth { fabric: FabricId::parse("fab000000001").unwrap(), node: p.node_id.clone(), incarnation: p.incarnation.clone() }
    }

    /// `kind`'s frame of `op` at `version`, published by `by`: its envelope's source.
    fn event_by(kind: StatusEvent, op: LifecycleOp, by: &PublisherId, version: u64) -> Frame {
        kind.frame(op, version).authored(&birth_of(by), 0, 1)
    }

    /// The one-chunk differential of `mesh` from `base_version` to `version`, authored by the
    /// forwarding mesh-primary `by` at `at`: `added` with their details, `changed` with their status
    /// and primary roles, `removed` by their exact births.
    fn delta_of(mesh: &str, by: &PublisherId, base_version: u64, version: u64, at: u64, added: Vec<MeshDigest>, changed: Vec<MeshDigest>, removed: Vec<MeshDigest>) -> Frame {
        Frame::TopologyDelta {
            mesh: mesh.into(),
            mesh_id: None,
            base_version,
            chunk_index: 0,
            chunk_count: 1,
            added: added.iter().map(rdm_core::wire::NodeEntry::of).collect(),
            changed: changed.iter().map(rdm_core::wire::NodeStatusEntry::of).collect(),
            removed: removed.iter().map(|d| rdm_core::framing::Birth { node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }).collect(),
        }
        .authored_delta(&birth_of(by), version, at)
    }

    /// `f`, a `TopologyFull`, stamped `at` in Rafka-time: its envelope's `time`, re-authored by the same
    /// author at the same version (a differential is authored at its time, [`delta_of`]).
    fn stamped(f: Frame, at: u64) -> Frame {
        if let (Frame::TopologyFull { .. }, Some(ctx)) = (f.data(), f.context()) {
            let (author, version) = (ctx.source.clone(), ctx.version().expect("an aggregate carries its version"));
            return f.into_data().authored_aggregate(&author, version, at);
        }
        f
    }

    /// `f` heard on `side` of `view` as a channel's receive path hands it on: the one ordering check
    /// of that channel first (`order`, R-RX3), then the view. `false` when the check ignored it.
    fn hear(order: &mut crate::receive_order::ReceiveOrder, view: &View, f: &Frame, fabric: &FabricId, side: Side) -> bool {
        if !admitted(order, f, &view.node, side.name()) {
            return false;
        }
        match f.data() {
            Frame::TopologyFull { .. } | Frame::TopologyDelta { .. } => {
                view.take_topology_frame(f, fabric, side, order);
            }
            _ => {
                view.take(f, fabric, side.name());
            }
        }
        true
    }

    /// CONTRACT (R-RX3, Luke 2026-10-13: "a frame whose order key is earlier than the newest the
    /// receiver holds for the same source is ignored, once, in the gossip receive path", with no
    /// channel qualifier; gossip.md §3.1: "a topology read install move[s] the held source version"):
    /// a node-admin holds one version per source, consulted by the backbone, its mesh channel and
    /// its topology-read install. For its own mesh (mesh2) and a peer mesh (mesh1), mesh2.node-admin.2
    /// read-installs the publisher's v5, listing the member Ready; the backbone then delivers the
    /// same publisher's v4, listing it Pending: nothing changes, and the ignore span names the
    /// backbone, the source, the version held and the one offered; the backbone then delivers v5,
    /// and nothing changes. Must NOT happen: the member leaving Ready (the bf44123e build_op run on
    /// mesh2.node-admin.2: read install 5bcc7547cc444f97, v4 install 2854055386b9a057, revert
    /// 7a563158be93f335, v5 3dd8d04dfcf8c2b9); a version a read installed left unconsulted by the
    /// backbone.
    #[test]
    fn a_frame_older_than_the_version_a_topology_read_installed_is_ignored_on_the_backbone_and_the_member_never_leaves_ready() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = test_view("mesh2", "mesh2.node-admin.2");
        // The node's receive path as it runs: the backbone through the node's one held version per
        // source, and the topology-read install through the same.
        let hears = |f: &Frame| hear(&mut view.order.lock().unwrap(), &view, f, &fabric, Side::Backbone);
        let reads = |chunk: crate::aggregate::Chunk| view.take_read_chunk(chunk, Side::Read, &fabric);
        let status_of = |d: &MeshDigest| {
            if d.node.mesh == view.mesh {
                view.digest_view.member_status(&d.node.node_id, &d.node.incarnation)
            } else {
                view.digest_view.get(d.node.node_id.as_str()).map(|(h, _)| h.status)
            }
        };
        for mesh in ["mesh2", "mesh1"] {
            let publisher = birth_at(rdm_core::PathName::new(format!("{mesh}.node-admin"), "1").unwrap(), mesh, "node-admin", 1);
            let ready = birth_at(rdm_core::PathName::new(format!("{mesh}.node"), "1").unwrap(), mesh, "node", 3);
            let pending = MeshDigest { status: NodeStatus::Pending, ..ready.clone() };
            let read = crate::aggregate::Chunk { mesh: mesh.into(), mesh_id: None, author: None, topology_version: 5, time_ms: 1, chunk_index: 0, chunk_count: 1, digests: vec![publisher.clone(), ready.clone()], runtimes: Vec::new() };
            assert!(matches!(reads(read), Taken::Written(_)), "{mesh}: the read installs v5");
            assert_eq!(status_of(&ready), Some(NodeStatus::Ready), "{mesh}: the read holds the member Ready");
            let v4 = stamped(aggregate_of(mesh, &publisher_of(&publisher), 4, vec![publisher.clone(), pending.clone()]), 40);
            assert!(!hears(&v4), "{mesh}: the backbone's v4 is older than the v5 the read installed: ignored");
            assert_eq!(status_of(&ready), Some(NodeStatus::Ready), "{mesh}: the member never leaves Ready");
            let ignored = spans.named("rdm.mesh.topology.reject.via-stale");
            assert!(
                ignored.iter().any(|f| f["channel"] == "backbone" && f["frame"] == "mesh-topology" && f["subject"] == mesh && f["held_key"].ends_with("topology_version=5") && f["frame_key"] == "topology_version=4"),
                "{mesh}: the ignore span names the backbone, the source, the version held and the one offered: {ignored:#?}"
            );
            let v5 = stamped(aggregate_of(mesh, &publisher_of(&publisher), 5, vec![publisher.clone(), ready.clone()]), 50);
            assert!(hears(&v5), "{mesh}: the backbone's v5 is not older than what is held");
            assert_eq!(status_of(&ready), Some(NodeStatus::Ready), "{mesh}: the member never leaves Ready");
        }
        let left_ready: Vec<_> = spans.named("rdm.mesh.topology.update.via-digest-status").into_iter().chain(spans.named("rdm.mesh.topology.update.via-member-state")).filter(|f| f.get("status").is_some_and(|s| s != "Ready")).collect();
        assert!(left_ready.is_empty(), "no member status moves off Ready: {left_ready:#?}");
    }

    /// CONTRACT (gossip.md §3.1 `Digest`, Q387: inside its mesh a node's status comes only from its own
    /// Digest, ordered by that Digest's sequence; rdm#337): a topology read carries each node of the
    /// reader's own mesh as its Digest, status included. A read whose copy is older than the node's own
    /// Digest the reader holds moves nothing; a read whose copy is later moves it. Must NOT happen: a
    /// compute holding a ready broker Pending because its mesh-primary's read carried an older status
    /// (Rafka's seed red: 34 s Pending, trace 93736c299b600c5b9ccad7115730355a).
    #[test]
    fn a_topology_read_never_moves_a_status_behind_the_nodes_own_digest() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let view = test_view("mesh1", "mesh1.compute.1");
        let publisher = birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1);
        let ready = birth_at(rdm_core::PathName::new("mesh1.broker", "1").unwrap(), "mesh1", "broker", 3);
        let pending = MeshDigest { status: NodeStatus::Pending, ..ready.clone() };
        let held = |d: &MeshDigest| view.digest_view.member_status(&d.node.node_id, &d.node.incarnation);
        let read_at = |version: u64, broker: &MeshDigest| crate::aggregate::Chunk { mesh: "mesh1".into(), mesh_id: None, author: None, topology_version: version, time_ms: version, chunk_index: 0, chunk_count: 1, digests: vec![publisher.clone(), broker.clone()], runtimes: Vec::new() };

        assert!(matches!(view.take_read_chunk(read_at(5, &pending), Side::Read, &fabric), Taken::Written(_)), "the read at v5 installs");
        assert_eq!(held(&ready), Some(NodeStatus::Pending), "v5 holds the broker Pending, its Digest as the primary held it");

        // The broker's own later Digest, Ready, reaches this node first.
        let own = MeshDigest { digest_seq: ready.digest_seq + 2, ..ready.clone() };
        assert!(view.digest_view.record(own.clone()), "the broker's own later Digest is held");
        assert_eq!(held(&ready), Some(NodeStatus::Ready), "its own Digest's Ready");

        // The top-up read at v7 carries the primary's older copy, Pending.
        assert!(matches!(view.take_read_chunk(read_at(7, &pending), Side::Read, &fabric), Taken::Written(_)), "the top-up read installs v7");
        assert_eq!(held(&ready), Some(NodeStatus::Ready), "the older copy moves nothing");

        // A read whose copy is later than the Digest held moves it.
        let later = MeshDigest { status: NodeStatus::ConnectionsDeleting, digest_seq: own.digest_seq + 1, ..ready.clone() };
        assert!(matches!(view.take_read_chunk(read_at(8, &later), Side::Read, &fabric), Taken::Written(_)), "the read at v8 installs");
        assert_eq!(held(&ready), Some(NodeStatus::ConnectionsDeleting), "the later copy is the node's later Digest");
    }

    /// CONTRACT (R-RX3, Luke 2026-10-12: "a message received out of order with a previous, earlier
    /// version is ignored"; Q389: the mesh's version orders it, whichever mesh-primary sent it; Q391: a
    /// successor continues the version; R-RX1: no refusal by who published): once a channel took a
    /// mesh's newer version, a publication of that mesh at a lower version is ignored by the receive path
    /// and the receiver keeps what it holds, whoever published either. Two receivers:
    /// - a peer mesh primary on the backbone holds mesh2 v1 from mesh2.node-admin.1; mesh2.node-admin.1 is
    ///   deleted and mesh2.node-admin.2 becomes mesh2's primary (R-RX4: nobody else publishes mesh2) and
    ///   continues at v2, which is installed; mesh2.node-admin.1's last mesh topology, v1, arrives after it
    ///   and is ignored; the deleted primary is not held again (rafka-v2 s41's red, trace
    ///   6cf5759abdce42d61c414560c772b308);
    /// - an ordinary node reads mesh1 at v5 (an aggregate travels on the backbone only, Q387); on its
    ///   mesh channel mesh1.node-admin.2's differential 5 to 6 (after the role moved) applies over it; a
    ///   late read and mesh1.node-admin.1's late delta below v6 are each ignored;
    /// - no mesh is marked desynchronized: nothing is repaired, for nothing is missing.
    /// Must NOT happen: a receiver switched back to the earlier publication's projection; a frame
    /// refused by who published it; an earlier publication read as a version gap.
    #[test]
    fn a_mesh_topology_below_the_version_its_channel_holds_of_the_mesh_is_ignored_and_never_switches_the_receiver_back() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let held = |view: &View, mesh: &str| view.copies.lock().unwrap().held_version(mesh);

        // The backbone: a peer mesh primary holds mesh2 from its primary, then from its successor.
        let peer = test_view("mesh4", "mesh4.node-admin.1");
        let mut backbone = crate::receive_order::ReceiveOrder::default();
        let (admin2, admin2b, rpc) = (birth_at(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", 1), [birth_at(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", 1), birth_at(rdm_core::PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node", 1)]);
        let mesh2_full = || vec![admin2.clone(), rpc[0].clone(), rpc[1].clone()];
        assert!(hear(&mut backbone, &peer, &stamped(aggregate_of("mesh2", &publisher_of(&admin2), 1, mesh2_full()), 100), &fabric, Side::Backbone));
        // mesh2.node-admin.1 is deleted and mesh2.node-admin.2 is established as the mesh's primary.
        // Its aggregate lists the deleted primary deleted (Q390).
        assert!(hear(&mut backbone, &peer, &stamped(aggregate_of("mesh2", &publisher_of(&admin2b), 2, vec![admin2b.clone(), rpc[0].clone(), rpc[1].clone(), listed(&admin2, NodeStatus::Deleted)]), 300), &fabric, Side::Backbone));
        assert_eq!(held(&peer, "mesh2"), Some((Some(publisher_of(&admin2b)), 2)), "the successor primary's mesh topology of mesh2, at the version it continued, is installed");
        // The former primary's last mesh topology, below the successor's version, arrives late.
        assert!(!hear(&mut backbone, &peer, &stamped(aggregate_of("mesh2", &publisher_of(&admin2), 1, mesh2_full()), 400), &fabric, Side::Backbone), "mesh2.node-admin.1's last mesh topology is ignored, however late its stamp");
        assert_eq!(held(&peer, "mesh2"), Some((Some(publisher_of(&admin2b)), 2)), "the peer keeps the successor primary's mesh topology of mesh2");
        assert!(peer.digest_view.is_deleted(admin2.node.node_id.as_str()) && peer.digest_view.get(admin2.node.node_id.as_str()).is_none(), "the deleted primary is not held again");

        // An ordinary node's mesh channel: mesh1's role moves from mesh1.node-admin.1 to mesh1.node-admin.2.
        let member = test_view("mesh9", "mesh1.node.9");
        let (p1, p2, rpc1) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1));
        assert!(matches!(member.take_read_chunk(read_of("mesh1", 5, 500, vec![p1.clone(), p2.clone(), rpc1.clone()]), Side::Read, &fabric), Taken::Written(_)), "the read of mesh1 at v5 installs");
        let moved = delta_of("mesh1", &publisher_of(&p2), 5, 6, 700, vec![], vec![], vec![p1.clone()]);
        assert!(hear(&mut member.order.lock().unwrap(), &member, &moved, &fabric, Side::MeshChannel));
        assert_eq!(held(&member, "mesh1"), Some((Some(publisher_of(&p2)), 6)), "the successor's differential applies at the version held");
        assert!(matches!(member.take_read_chunk(read_of("mesh1", 5, 800, vec![p1.clone(), rpc1.clone()]), Side::Read, &fabric), Taken::Refused(crate::aggregate::Refusal::OlderThanHeld { held_version: 6, offered_version: 5 })), "a read below the held version is ignored, however late it was served");
        let late_delta = delta_of("mesh1", &publisher_of(&p1), 4, 5, 800, vec![], vec![], vec![p2.clone()]);
        assert!(!hear(&mut member.order.lock().unwrap(), &member, &late_delta, &fabric, Side::MeshChannel), "the predecessor's delta below the held version is ignored");
        assert_eq!(held(&member, "mesh1"), Some((Some(publisher_of(&p2)), 6)), "the member keeps the successor's projection");
        assert!(member.copies.lock().unwrap().desynced().is_empty(), "nothing is marked desynchronized: {:?}", member.copies.lock().unwrap().desynced());
        assert!(peer.copies.lock().unwrap().desynced().is_empty());
    }

    /// CONTRACT (R-RX3, R-RX1, Luke 2026-10-12; Q389, Q391: the mesh's version orders it, whichever
    /// mesh-primary sent it, and a successor continues it): a publication is ordered by its version alone,
    /// never by whether its publisher is held stopped or deleted here. A newer version, published before its
    /// publisher stopped, is taken for what it carries. Three receivers:
    /// - the fabric-primary on the backbone (rafka-v2 s42's successor, scratchpad m2a1.log:390-401): it
    ///   holds mesh1.node-admin.1's v7 listing mesh1.node-admin.2 Ready and executed mesh1.node-admin.1's stop; v8,
    ///   carrying mesh1.node-admin.2's stop, is taken, so mesh1.node-admin.2 is held stopped, and mesh1.node-admin.1's
    ///   own entry in it is refused by the stop the fabric-primary executed (`via-stopped-birth`, via
    ///   `projection`); mesh1.node-admin.3 takes the role and its full, continuing at v9, is installed;
    ///   mesh1.node-admin.1's late republish of v8 is ignored by the receive path;
    /// - an own-mesh member holds mesh1.node-admin.1's stop from the fabric-primary's event on its mesh
    ///   channel, stamped at the version the fabric-primary holds of mesh1 (v7); a read of mesh1's v8,
    ///   listing it Ready, is taken: the projection is the mesh's next word, newer than the stop, and
    ///   the stop ends in it (`via-stop-ended-in-projection`);
    /// - a node-admin that holds mesh1.node-admin.1 deleted (deleted) takes its v8 the same way; the
    ///   deleted publisher is not held again (its own entry is refused by name, the departure stands).
    /// Must NOT happen: a receiver kept on an older version than one it is offered; a publication refused
    /// because its publisher is held stopped or deleted; a receiver switched back to an earlier
    /// publication after the successor's full.
    #[test]
    fn a_newer_version_published_before_its_publisher_stopped_is_taken_and_one_below_the_successors_version_is_ignored() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let held = |view: &View| view.copies.lock().unwrap().held_version("mesh1");
        let (p, x, rpc, successor) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1), birth_at(rdm_core::PathName::new("mesh1.node-admin", "3").unwrap(), "mesh1", "node-admin", 1));
        let fabric_primary = birth_at(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", 1);
        let stop_of = |d: &MeshDigest| op_at(d.node.location(), &d.node.mesh, &d.node.r#type, &d.node.node_id, &d.node.incarnation, "node.stop");
        let stop_p = stop_of(&p);

        // The fabric-primary, on the backbone.
        let fp = test_view("mesh2", "mesh2.node-admin.1");
        let mut backbone = crate::receive_order::ReceiveOrder::default();
        assert!(hear(&mut backbone, &fp, &aggregate_of("mesh1", &publisher_of(&p), 7, vec![p.clone(), x.clone(), rpc.clone()]), &fabric, Side::Backbone));
        assert!(fp.digest_view.stopped(stop_p.clone()), "the fabric-primary executed mesh1.node-admin.1's stop");
        // v8 lists mesh1.node-admin.2 stopped (Q390).
        assert!(hear(&mut backbone, &fp, &aggregate_of("mesh1", &publisher_of(&p), 8, vec![p.clone(), rpc.clone(), listed(&x, NodeStatus::Stopped)]), &fabric, Side::Backbone));
        assert_eq!(held(&fp), Some((Some(publisher_of(&p)), 8)), "a newer version of the held revision, published before its publisher stopped, is taken");
        assert!(fp.digest_view.holds_stopped(x.node.node_id.as_str(), &x.node.incarnation) && fp.digest_view.get(x.node.node_id.as_str()).is_none() && !fp.digest_view.routable(x.node.node_id.as_str()), "the stop v8 carries is held: mesh1.node-admin.2 is no longer held Ready");
        assert!(fp.digest_view.in_flight().contains(&stop_p) && fp.digest_view.get(p.node.node_id.as_str()).is_none(), "the publisher's own stop stands: v8 never carried it");
        assert!(hear(&mut backbone, &fp, &stamped(aggregate_of("mesh1", &publisher_of(&successor), 9, vec![successor.clone(), rpc.clone(), listed(&p, NodeStatus::Stopped), listed(&x, NodeStatus::Stopped)]), 20), &fabric, Side::Backbone));
        assert_eq!(held(&fp), Some((Some(publisher_of(&successor)), 9)), "the successor's full, continuing the mesh's version, is installed");
        assert!(!hear(&mut backbone, &fp, &aggregate_of("mesh1", &publisher_of(&p), 8, vec![p.clone(), x.clone(), rpc.clone()]), &fabric, Side::Backbone), "the predecessor's late v8, below the successor's v9, is ignored");
        assert_eq!(held(&fp), Some((Some(publisher_of(&successor)), 9)), "the receiver keeps the successor's full");
        assert!(fp.digest_view.holds_stopped(x.node.node_id.as_str(), &x.node.incarnation) && fp.digest_view.in_flight().contains(&stop_p), "both stops stand");

        // An own-mesh member: its mesh's list from reads (Q387), the stop the fabric-primary's on its mesh channel.
        let member = test_view("mesh9", "mesh1.node.9");
        assert!(matches!(member.take_read_chunk(read_of("mesh1", 7, 7, vec![p.clone(), rpc.clone()]), Side::Read, &fabric), Taken::Written(_)));
        assert!(hear(&mut member.order.lock().unwrap(), &member, &event_by(StatusEvent::Stopped, stop_p.clone(), &publisher_of(&fabric_primary), 7), &fabric, Side::MeshChannel));
        assert!(member.digest_view.in_flight().contains(&stop_p), "the fabric-primary's stop is held");
        assert!(matches!(member.take_read_chunk(read_of("mesh1", 8, 8, vec![p.clone(), rpc.clone()]), Side::Read, &fabric), Taken::Written(_)));
        assert_eq!(held(&member), Some((None, 8)), "a newer version is taken from a read too");
        assert!(!member.digest_view.in_flight().contains(&stop_p), "the projection is the source's next word: the stop another publisher carried ends");

        // A node-admin that holds the publisher deleted, on the backbone.
        let gone = test_view("mesh2", "mesh2.node-admin.3");
        let mut gone_backbone = crate::receive_order::ReceiveOrder::default();
        assert!(hear(&mut gone_backbone, &gone, &aggregate_of("mesh1", &publisher_of(&p), 7, vec![p.clone(), x.clone(), rpc.clone()]), &fabric, Side::Backbone));
        assert!(gone.digest_view.depart(op_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", &p.node.node_id, &p.node.incarnation, "node.delete")), "mesh1.node-admin.1's departure is held");
        assert!(hear(&mut gone_backbone, &gone, &aggregate_of("mesh1", &publisher_of(&p), 8, vec![p.clone(), rpc.clone(), listed(&x, NodeStatus::Stopped)]), &fabric, Side::Backbone));
        assert_eq!(held(&gone), Some((Some(publisher_of(&p)), 8)), "a deleted publisher's newer version of the held revision is taken");
        assert!(gone.digest_view.holds_stopped(x.node.node_id.as_str(), &x.node.incarnation) && !gone.digest_view.routable(x.node.node_id.as_str()), "the stop it carries is held");
        assert!(gone.digest_view.is_deleted(p.node.node_id.as_str()) && gone.digest_view.get(p.node.node_id.as_str()).is_none(), "the deleted publisher is not held again");
        let deleted = spans.named("rdm.mesh.topology.reject.via-deleted-birth");
        assert!(deleted.iter().any(|f| f.get("node_id").map(String::as_str) == Some(p.node.node_id.as_str()) && f.get("via").map(String::as_str) == Some("forwarded")), "the deleted publisher's own entry is refused by its exact birth: {deleted:#?}");

        let in_projection = spans.named("rdm.mesh.topology.reject.via-stopped-birth");
        assert_eq!(
            in_projection.iter().filter(|f| f["via"] == "projection").map(|f| (f["node"].clone(), (f["member_path"].clone(), f["member_name"].clone()), f["topology_version"].clone())).collect::<Vec<_>>(),
            vec![("mesh2.node-admin.1".to_string(), ("mesh1.node-admin".to_string(), "1".to_string()), "8".to_string())],
            "the stopped publisher's own entry in v8 is refused by name at the fabric-primary, which executed its stop: {in_projection:#?}"
        );
        let stop_ended = spans.named("rdm.mesh.topology.update.via-stop-ended-in-projection");
        assert_eq!(stop_ended.iter().map(|f| (f["node"].clone(), (f["member_path"].clone(), f["member_name"].clone()), f["publisher"].clone())).collect::<Vec<_>>(), vec![("mesh1.node.9".to_string(), ("mesh1.node-admin".to_string(), "1".to_string()), String::new())], "a read names no author: {stop_ended:#?}");
    }

    /// CONTRACT (R-RX1, Q389, Q391; gossip.md §3.3): an ordinary node of mesh2 holds mesh1's
    /// projection of mesh1.node-admin.1's lost birth at v7 from a read, and the primary record its entry
    /// read gave it names that birth. mesh1 recovers as itself and its new primary birth continues the
    /// mesh's version; mesh2's primary forwards the move as the differential 7 to 8 (the lost birth
    /// removed, the new one added) and then 8 to 9: each applies at exactly its base, whichever
    /// mesh-primary of mesh1 sent it, with no primary evidence for the new birth and no repair. A read of
    /// the lost birth's v7, below the held version, arrives after them and is ignored by the receive path.
    /// Must NOT happen: a publication refused for a publisher the primary record does not name; a mesh
    /// marked desynchronized with nothing missing (mesh_recover
    /// `a_lost_mesh_recovers_as_itself_under_the_same_build`: mesh2.node.1-3 refused 35 frames each).
    #[test]
    fn a_new_mesh_primary_continues_the_mesh_version_on_the_mesh_channel_with_no_primary_record_and_the_lost_births_earlier_full_is_ignored() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let member = test_view("mesh2", "mesh2.node.1");
        let (lost, recovered, rpc) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1));
        assert!(matches!(member.digest_view.primaries.take(PrimaryRole::MeshPrimary, &primary_of(&lost, 1)), RoleTaken::Held { .. }), "the entry read's primary record of mesh1");
        // mesh1's list reaches the member by its read (Q387: no full on the mesh channel).
        assert!(matches!(member.take_read_chunk(read_of("mesh1", 7, 70, vec![lost.clone(), rpc.clone()]), Side::Read, &fabric), Taken::Written(_)));
        assert_eq!(member.copies.lock().unwrap().held_version("mesh1"), Some((None, 7)));
        let moved = delta_of("mesh1", &publisher_of(&recovered), 7, 8, 90, vec![recovered.clone()], vec![], vec![lost.clone()]);
        assert!(hear(&mut member.order.lock().unwrap(), &member, &moved, &fabric, Side::MeshChannel));
        assert_eq!(member.copies.lock().unwrap().held_version("mesh1"), Some((Some(publisher_of(&recovered)), 8)), "the new mesh-primary's differential applies at the held version: no primary evidence is asked of its publisher");
        let delta = delta_of("mesh1", &publisher_of(&recovered), 8, 9, 95, vec![], vec![], vec![rpc.clone()]);
        assert!(hear(&mut member.order.lock().unwrap(), &member, &delta, &fabric, Side::MeshChannel));
        assert_eq!(member.copies.lock().unwrap().held_version("mesh1"), Some((Some(publisher_of(&recovered)), 9)), "its delta applies at exactly its base");
        assert!(matches!(member.take_read_chunk(read_of("mesh1", 7, 99, vec![lost.clone(), rpc.clone()]), Side::Read, &fabric), Taken::Refused(crate::aggregate::Refusal::OlderThanHeld { .. })), "a read of the lost birth's v7, below the held version, is ignored, however late it was served");
        assert_eq!(member.copies.lock().unwrap().held_version("mesh1"), Some((Some(publisher_of(&recovered)), 9)));
        assert!(member.copies.lock().unwrap().desynced().is_empty(), "nothing is marked for repair: {:?}", member.copies.lock().unwrap().desynced());
    }

    /// CONTRACT (R-RX3, Luke 2026-10-13: "a frame whose order key is earlier than the newest the
    /// receiver holds for the same source is ignored, once, in the gossip receive path"; i143 s53:
    /// out-of-order frames are ignored only in receive_order.rs; Q389: the mesh's version orders it): every
    /// earlier frame of the mesh is ignored by the receive path before the aggregate receiver reads it, so
    /// the receiver orders nothing by version. An ordinary node of mesh1 holds mesh1's v7 from a read
    /// and applies mesh1.node-admin.1's 7→8 delta; then:
    /// - a copy of that delta is taken by the receive path (its key equals the held one) and the
    ///   receiver absorbs it as a copy (`via-duplicate`), with no gap;
    /// - a v6 aggregate, a 6→7 delta and a topology read of v6 are each ignored by the receive path, the
    ///   ignore span naming the frame, the mesh, the version held and the one offered;
    /// - the top-up installs the publisher's v9 and its 9→11 delta applies; the 8→9 delta the top-up
    ///   already covered is ignored by the receive path.
    /// Must NOT happen: an earlier frame of the mesh reaching the receiver (a `via-aggregate`
    /// refusal, or a `via-duplicate` for anything but the copy); the held projection moved
    /// back; a source marked desynchronized.
    #[test]
    fn an_earlier_full_delta_or_read_of_the_mesh_is_ignored_by_the_receive_path_before_the_aggregate_receiver_reads_it() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let member = test_view("mesh9", "mesh1.node.9");
        let hears = |f: &Frame| hear(&mut member.order.lock().unwrap(), &member, f, &fabric, Side::MeshChannel);
        let reads = |chunk: crate::aggregate::Chunk| member.take_read_chunk(chunk, Side::Read, &fabric);
        let held = || member.copies.lock().unwrap().held_version("mesh1");
        let (admin, rpc1, rpc2) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1), birth_at(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", 1));
        let p = publisher_of(&admin);
        let full = |version: u64, at: u64| stamped(aggregate_of("mesh1", &p, version, vec![admin.clone(), rpc1.clone(), rpc2.clone()]), at);
        let delta = |base_version: u64, topology_version: u64, at: u64| delta_of("mesh1", &p, base_version, topology_version, at, vec![], vec![], vec![]);
        let read = |version: u64| crate::aggregate::Chunk { mesh: "mesh1".into(), mesh_id: None, author: None, topology_version: version, time_ms: version, chunk_index: 0, chunk_count: 1, digests: vec![admin.clone(), rpc1.clone(), rpc2.clone()], runtimes: Vec::new() };

        assert!(matches!(reads(read(7)), Taken::Written(_)), "the read installs v7");
        assert!(hears(&delta(7, 8, 800)));
        assert_eq!(held(), Some((Some(p.clone()), 8)), "the delta applied at exactly its base");
        assert!(hears(&delta(7, 8, 800)), "a copy of the applied delta carries the held key: taken");
        let copies = spans.named("rdm.mesh.topology.update.via-duplicate");
        assert!(copies.len() == 1 && copies[0]["identity"] == format!("mesh1 {} base_version=7 topology_version=8", birth_of(&p).uri()) && copies[0]["outcome"] == "ignored", "the receiver absorbs the copy: {copies:#?}");

        assert!(!hears(&full(6, 600)), "the held publisher's v6 aggregate is ignored by the receive path");
        assert!(!hears(&delta(6, 7, 650)), "the held publisher's 6→7 delta is ignored by the receive path");
        let refused = reads(read(6));
        assert!(matches!(refused, Taken::Refused(crate::aggregate::Refusal::OlderThanHeld { held_version: 8, offered_version: 6 })), "a read of v6 is ignored by the receive path: {refused:?}");
        assert_eq!(held(), Some((Some(p.clone()), 8)), "nothing earlier moved the held projection back");
        let ignored = spans.named("rdm.mesh.topology.reject.via-stale");
        for (frame, held, offered) in [
            ("mesh-topology", "topology_version=8", "topology_version=6"),
            ("mesh-topology-delta", "topology_version=8", "topology_version=7"),
            ("topology-read", "topology_version=8", "topology_version=6"),
        ] {
            assert!(ignored.iter().any(|f| f["frame"] == frame && f["subject"] == "mesh1" && f["held_key"] == held && f["frame_key"] == offered), "the receive path names the ignored {frame}, its source and both keys: {ignored:#?}");
        }

        assert!(matches!(reads(read(9)), Taken::Written(_)), "the top-up installs v9");
        assert!(hears(&delta(9, 11, 1100)), "the publisher's next delta continues from the read's baseline");
        assert_eq!(held(), Some((Some(p.clone()), 11)));
        assert!(!hears(&delta(8, 9, 900)), "the delta the top-up already covered is ignored by the receive path");
        assert_eq!(held(), Some((Some(p.clone()), 11)), "the held projection stands");

        assert!(spans.named("rdm.mesh.topology.reject.via-aggregate").is_empty(), "no earlier frame reached the receiver: {:#?}", spans.named("rdm.mesh.topology.reject.via-aggregate"));
        assert_eq!(spans.named("rdm.mesh.topology.update.via-duplicate").len(), 1, "the receiver absorbed the copy and nothing else");
        assert!(member.copies.lock().unwrap().desynced().is_empty(), "nothing is marked desynchronized: {:?}", member.copies.lock().unwrap().desynced());
    }

    /// CONTRACT (R-RX3, gossip.md §3.1: "a frame whose order key is earlier than the newest the
    /// receiver holds from that source is ignored ... whichever channel or read it arrives by": one held
    /// version per source; `node.topology.get` serves each source "at the publisher and version it holds
    /// it at"): a node-admin that is not its mesh's primary holds a peer mesh (mesh1) from its start read
    /// and hears it on the backbone. The backbone runs ahead (v9, v10: mesh1's members deleted down to its
    /// last admin); a late read of v9 is then ignored as earlier than the held v10. Its topology read
    /// serves mesh1 at v10 with the last
    /// admin its only member. Must NOT happen: the read serving the mesh channel's v8 with all four
    /// births (rdm#154: mesh3.node-admin.2 served mesh1@9 with four births for 30 s while it held v22,
    /// rdm.mesh.topology.serve.via-read members=mesh1@9:mesh1.node.2,mesh1.node-admin.2,mesh1.node.1,mesh1.node-admin.1).
    #[test]
    fn a_non_primary_admins_read_serves_the_newest_version_it_holds_of_a_peer_mesh_whichever_channel_installed_it() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let admin = test_view("mesh3", "mesh3.node-admin.2");
        let hears = |f: &Frame, side: Side| hear(&mut admin.order.lock().unwrap(), &admin, f, &fabric, side);
        let (a1, a2, r1, r2) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1), birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1), birth_at(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", 1));
        let a2 = { let mut d = a2; d.node.path = "mesh1.node-admin".into(); d.node.name = "2".into(); d };
        let p = publisher_of(&a1);
        let all = vec![a1.clone(), a2.clone(), r1.clone(), r2.clone()];
        let full = |version: u64, digests: Vec<MeshDigest>| aggregate_of("mesh1", &p, version, digests);

        // As mesh3.node-admin.2 heard them (rdm#154): its read's v8 first, then the backbone's.
        assert!(matches!(admin.take_read_chunk(read_of("mesh1", 8, 8, all.clone()), Side::Read, &fabric), Taken::Written(_)), "the read's v8 is taken");
        assert!(hears(&full(8, all.clone()), Side::Backbone));
        assert!(hears(&full(9, vec![a1.clone(), a2.clone()]), Side::Backbone));
        assert!(hears(&full(10, vec![a1.clone()]), Side::Backbone));
        assert!(matches!(admin.take_read_chunk(read_of("mesh1", 9, 9, vec![a1.clone(), a2.clone()]), Side::Read, &fabric), Taken::Refused(crate::aggregate::Refusal::OlderThanHeld { .. })), "a late read of v9 is earlier than the held v10");

        let served: Vec<_> = admin.held_sources_of(None).into_iter().filter(|s| s.mesh == "mesh1").collect();
        assert_eq!(served.len(), 1, "one source of mesh1 is served: {served:#?}");
        let names: Vec<rdm_core::PathName> = served[0].digests.iter().map(|d| d.node.location()).collect();
        assert_eq!((served[0].topology_version, names.clone()), (10, vec![rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()]), "the read serves the version this node holds, v10, with the last admin its only member: {names:?} at v{}", served[0].topology_version);
    }

    /// CONTRACT (R-RX1, Luke 2026-10-12: no receiver refuses an input because of who sent it; R-RX3:
    /// lifecycle facts order by `topology_version`, per subject mesh, whichever of its mesh-primaries or
    /// executors published them, Q389, Q391): a `NodeStopped` or `NodeDeleting` is taken for what it
    /// carries, from a publisher with no primary record held, from one the held primary record does not
    /// name, and from one held stopped. A fact below the newest its channel holds of the mesh is ignored
    /// by the receive path, whoever published either. Must NOT happen: a status event refused for who
    /// published it (`via-non-primary-publisher`), or a source marked for repair by it.
    #[test]
    fn a_status_event_is_taken_whoever_published_it_and_one_below_the_newest_of_its_mesh_is_ignored() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let (primary, other, stopped_admin) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node-admin", "3").unwrap(), "mesh1", "node-admin", 1));
        let subject = |name: &str| birth_at(rdm_core::PathName::new("mesh1.node", name).unwrap(), "mesh1", "node", 1);
        let op_of = |s: &MeshDigest, operation: &str| op_at(s.node.location(), &s.node.mesh, &s.node.r#type, &s.node.node_id, &s.node.incarnation, operation);
        let event = |kind: StatusEvent, s: &MeshDigest, operation: &str, by: &MeshDigest, version: u64| event_by(kind, op_of(s, operation), &publisher_of(by), version);

        // No primary record held: taken.
        let member = test_view("mesh1", "mesh1.node.9");
        let mut channel = crate::receive_order::ReceiveOrder::default();
        let s1 = subject("1");
        assert!(hear(&mut channel, &member, &event(StatusEvent::Stopped, &s1, "node.stop", &primary, 4), &fabric, Side::MeshChannel));
        assert_eq!(member.digest_view.in_flight().len(), 1, "no primary record is asked for: the stop is held");

        // The primary record names mesh1.node-admin.1: mesh1.node-admin.2's events are taken too.
        assert!(matches!(member.digest_view.primaries.take(PrimaryRole::MeshPrimary, &primary_of(&primary, 1)), RoleTaken::Held { .. }));
        let s2 = subject("2");
        assert!(hear(&mut channel, &member, &event(StatusEvent::Deleting, &s2, "node.delete", &other, 7), &fabric, Side::MeshChannel));
        assert_eq!(member.digest_view.in_flight().len(), 2, "a publisher the primary record does not name moves its subject: {:?}", member.digest_view.in_flight());

        // A publisher held stopped: its event is taken.
        assert!(member.digest_view.stopped(op_at(rdm_core::PathName::new("mesh1.node-admin", "3").unwrap(), "mesh1", "node-admin", &stopped_admin.node.node_id, &stopped_admin.node.incarnation, "node.stop")));
        let s3 = subject("3");
        assert!(hear(&mut channel, &member, &event(StatusEvent::Deleting, &s3, "node.delete", &stopped_admin, 8), &fabric, Side::MeshChannel));
        assert!(member.digest_view.in_flight().iter().any(|op| op.node_id == s3.node.node_id), "a publisher held stopped moves its subject");

        // Below the newest of its mesh: ignored, whoever published it.
        let s4 = subject("4");
        let before = member.digest_view.in_flight().len();
        assert!(!hear(&mut channel, &member, &event(StatusEvent::Stopped, &s4, "node.stop", &other, 5), &fabric, Side::MeshChannel), "a fact of mesh1 at v5 after its v8 is ignored");
        assert_eq!(member.digest_view.in_flight().len(), before, "nothing moved");
        assert!(!hear(&mut channel, &member, &event(StatusEvent::Stopped, &s4, "node.stop", &primary, 9), &fabric, Side::MeshChannel), "the version-refused identity is already seen, whoever carries its repeat");
        // A new operation, above the mesh's newest, is taken whoever publishes it.
        let mut fresh_op = op_of(&s4, "node.stop");
        fresh_op.operation = fresh_op.operation.clone().in_occurrence(rdm_core::OperationOccurrenceId::mint());
        let fresh = event_by(StatusEvent::Stopped, fresh_op, &publisher_of(&primary), 9);
        assert!(hear(&mut channel, &member, &fresh, &fabric, Side::MeshChannel), "a new fact of mesh1 at v9, above its v8, is taken");
        assert!(member.digest_view.in_flight().iter().any(|op| op.node_id == s4.node.node_id));

        assert!(spans.named("rdm.mesh.topology.reject.via-non-primary-publisher").is_empty(), "no status event is refused for who published it");
        assert!(member.copies.lock().unwrap().desynced().is_empty(), "nothing is marked for repair");
    }

    /// CONTRACT (node-status-heartbeat-connections.md §C "late stopped after starting": "a NodeStopped
    /// older than the starting fact (lower `topology_version`) is refused"; gossip.md `NodeStopped`:
    /// "`topology_version` the version of the subject's mesh at which the subject changed"; Q389): a
    /// node-admin holds mesh1's aggregate v5 (on the backbone, the one channel an aggregate travels on,
    /// Q387) listing mesh1.node.1 again after its start. A `NodeStopped` of that birth stamped v4 arrives
    /// late: it is refused by name, naming the held version, and the birth stays held. The same event
    /// stamped v6 (after the held projection) is taken; so is the fabric-primary's stop stamped v5, the
    /// version it holds of mesh1: the projection at v5 predates it. Must NOT happen: a late stop dropping
    /// a started birth; a stop at the held version refused.
    #[test]
    fn a_node_stopped_older_than_the_held_projection_of_its_mesh_listing_the_birth_is_refused_by_name() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let fabric = FabricId::parse("fab000000001").unwrap();
        let (primary, fabric_primary) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", 1));
        let rpc = birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 3);
        let stop = op_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", &rpc.node.node_id, &rpc.node.incarnation, "node.stop");
        let member = |name: &str| {
            let v = test_view("mesh9", name);
            v.digest_view.primaries.take(PrimaryRole::MeshPrimary, &primary_of(&primary, 1));
            v.digest_view.primaries.take(PrimaryRole::FabricPrimary, &primary_of(&fabric_primary, 1));
            v.take_topology_frame(&aggregate_of("mesh1", &publisher_of(&primary), 5, vec![primary.clone(), rpc.clone()]), &fabric, Side::Backbone, &mut v.order.lock().unwrap());
            assert!(v.digest_view.get(rpc.node.node_id.as_str()).is_some(), "the birth is held from the projection");
            v
        };
        let late = member("mesh1.node.7");
        late.take(&event_by(StatusEvent::Stopped, stop.clone(), &publisher_of(&primary), 4), &fabric, "mesh-channel");
        assert!(late.digest_view.in_flight().is_empty() && late.digest_view.get(rpc.node.node_id.as_str()).is_some(), "a stop older than the projection listing the birth is refused; the birth stays held");
        let newer = member("mesh1.node.8");
        newer.take(&event_by(StatusEvent::Stopped, stop.clone(), &publisher_of(&primary), 6), &fabric, "mesh-channel");
        assert_eq!(newer.digest_view.in_flight(), vec![stop.clone()], "a stop newer than the held projection is taken");
        let executor = member("mesh1.node.9");
        executor.take(&event_by(StatusEvent::Stopped, stop.clone(), &publisher_of(&fabric_primary), 5), &fabric, "mesh-channel");
        assert_eq!(executor.digest_view.in_flight(), vec![stop.clone()], "a stop at the held version is newer than the projection at it: taken");
        let refused = spans.named("rdm.mesh.topology.reject.via-stop-older-than-projection");
        assert_eq!(refused.iter().map(|f| (f["node"].clone(), f["topology_version"].clone(), f["held_version"].clone())).collect::<Vec<_>>(), vec![("mesh1.node.7".to_string(), "4".to_string(), "5".to_string())], "{refused:#?}");
    }

    /// CONTRACT (gossip.md `NodeStopped`: "`topology_version` the version of the subject's mesh at which
    /// the subject changed"; Q389: a mesh's version is not keyed on who published it): a
    /// mesh9 node holds mesh2 at v5 from its forwarder. The fabric-primary, executing mesh2.node.1's stop,
    /// stamps it at v5, the version it holds of mesh2: the node takes it, and holds mesh2.node.1 stopped.
    /// mesh2's next projection, v6 listing mesh2.node.1 again, is taken and ends the stop, and mesh2's
    /// mesh-primary's next fact, at v6, is taken. Must NOT happen: an executor's stop stamped with its own
    /// mesh's counter, which then orders mesh2's own facts and projections below it.
    #[test]
    fn an_executor_stop_at_the_version_it_holds_of_the_mesh_is_taken_and_the_meshs_next_projection_after_it() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let member = test_view("mesh9", "mesh9.node.1");
        let mut channel = crate::receive_order::ReceiveOrder::default();
        let (forwarder, fp, mp2) = (birth_at(rdm_core::PathName::new("mesh9.node-admin", "1").unwrap(), "mesh9", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", 1));
        let (n1, n2) = (birth_at(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", 1), birth_at(rdm_core::PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node", 1));
        let by = publisher_of(&forwarder);
        assert!(hear(&mut channel, &member, &delta_of("mesh2", &by, 0, 5, 1, vec![mp2.clone(), n1.clone(), n2.clone()], vec![], vec![]), &fabric, Side::MeshChannel));
        assert_eq!(member.copies.lock().unwrap().held_version("mesh2").map(|(_, v)| v), Some(5));
        let stop = op_at(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", &n1.node.node_id, &n1.node.incarnation, "node.stop");
        assert!(hear(&mut channel, &member, &event_by(StatusEvent::Stopped, stop.clone(), &publisher_of(&fp), 5), &fabric, Side::MeshChannel), "the fabric-primary's stop at mesh2's v5 is taken");
        assert!(member.digest_view.in_flight().contains(&stop) && member.digest_view.get(n1.node.node_id.as_str()).is_none(), "mesh2.node.1 is held stopped");
        let mut started = n1.clone();
        started.status = NodeStatus::Pending;
        assert!(hear(&mut channel, &member, &delta_of("mesh2", &by, 5, 6, 2, vec![], vec![started], vec![]), &fabric, Side::MeshChannel), "mesh2's next projection is taken");
        assert_eq!(member.copies.lock().unwrap().held_version("mesh2").map(|(_, v)| v), Some(6));
        assert!(!member.digest_view.in_flight().contains(&stop) && member.digest_view.get(n1.node.node_id.as_str()).is_some(), "v6 lists mesh2.node.1 again: its stop ends");
        let deleting = op_at(rdm_core::PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node", &n2.node.node_id, &n2.node.incarnation, "node.delete");
        assert!(hear(&mut channel, &member, &event_by(StatusEvent::Deleting, deleting.clone(), &publisher_of(&mp2), 6), &fabric, Side::MeshChannel), "mesh2's mesh-primary's fact at v6 is taken");
        assert!(member.digest_view.in_flight().contains(&deleting));
        assert!(member.copies.lock().unwrap().desynced().is_empty());
    }

    /// CONTRACT (gossip.md "A node that missed a lifecycle event recovers it from `TopologyFull.in_flight`
    /// or `TopologyFull.deleted`, through the next delta, full or top-up"; Q390: a deletion is the node's
    /// status in a differential's `changed`): `rdm.mesh.topology.update.via-delta` records `deleted`, the
    /// count of that mesh's nodes the receiver holds listed deleted after the apply, as
    /// `via-full` and `via-topology-version` record it. A mesh9 node holds mesh2 from its
    /// forwarder's differentials: v5 lists three nodes, v6 and v7 each list one more deleted, and v8 no
    /// longer lists the first. Must NOT happen: a node that recovered a peer mesh's deletions through
    /// differentials alone, with no span saying it holds them (fabric_primary_handover, gate
    /// 2163cb745: mesh2.node.2 took mesh1's last deletions by deltas only).
    #[test]
    fn a_differential_records_the_deleted_nodes_the_receiver_holds_after_it_applies() {
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let member = test_view("mesh9", "mesh9.node.1");
        let mut channel = crate::receive_order::ReceiveOrder::default();
        let (forwarder, mp2) = (birth_at(rdm_core::PathName::new("mesh9.node-admin", "1").unwrap(), "mesh9", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", 1));
        let (n1, n2) = (birth_at(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", 1), birth_at(rdm_core::PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node", 1));
        let deleted = |d: &MeshDigest| MeshDigest { status: NodeStatus::Deleted, ..d.clone() };
        let by = publisher_of(&forwarder);
        assert!(hear(&mut channel, &member, &delta_of("mesh2", &by, 0, 5, 1, vec![mp2.clone(), n1.clone(), n2.clone()], vec![], vec![]), &fabric, Side::MeshChannel));
        assert!(hear(&mut channel, &member, &delta_of("mesh2", &by, 5, 6, 2, vec![], vec![deleted(&n1)], vec![]), &fabric, Side::MeshChannel));
        assert!(hear(&mut channel, &member, &delta_of("mesh2", &by, 6, 7, 3, vec![], vec![deleted(&n2)], vec![]), &fabric, Side::MeshChannel));
        assert!(hear(&mut channel, &member, &delta_of("mesh2", &by, 7, 8, 4, vec![], vec![], vec![n1.clone()]), &fabric, Side::MeshChannel));
        assert_eq!(member.copies.lock().unwrap().held_version("mesh2").map(|(_, v)| v), Some(8));
        let applied: Vec<(String, String, String, String)> = spans
            .named("rdm.mesh.topology.update.via-delta")
            .into_iter()
            .filter(|f| f["node"] == "mesh9.node.1" && f["mesh"] == "mesh2")
            .map(|f| (f["topology_version"].clone(), f["changed"].clone(), f["removed"].clone(), f["deleted"].clone()))
            .collect();
        let row = |v: &str, changed: &str, removed: &str, deleted: &str| (v.to_string(), changed.to_string(), removed.to_string(), deleted.to_string());
        assert_eq!(applied, vec![row("5", "0", "0", "0"), row("6", "1", "0", "1"), row("7", "1", "0", "2"), row("8", "0", "1", "1")], "each differential records the deleted nodes held after it: {applied:#?}");
    }

    /// Two receive paths of one node (its backbone and its mesh channel) applying a projection of
    /// the stopped birth `d` at once, each having found the stop held and the projection past it.
    fn both_paths_end_the_stop(digest_view: &DigestView, d: &MeshDigest, target: &Target) {
        for _ in 0..2 {
            digest_view.stopped_entry(d, target);
        }
    }

    /// CONTRACT (gossip.md "Node-stopped lifecycle overlay": the stop stays "until the next lifecycle
    /// op on that exact birth"; node_stop gate 4679736d, mesh1.node-admin.2: the stop of mesh1.node.1 ended
    /// twice, trace 09b25fab91c3239df30466a89f81d507 on the mesh channel and
    /// 4b4eac62071c2a5054e464c5f3a429af on the backbone, the second naming `stop_carried_at=-1`): a
    /// node-admin applies its primary's projection on its backbone and on its mesh channel, each under
    /// its own receiver. When both find the stop held and the projection past it, the stop ends once:
    /// one span, carrying the version the stop was carried at, and the birth held again once, from
    /// the latest heartbeat refused while the stop stood. Must NOT happen: a second end of a stop
    /// already ended, naming no carried version.
    #[test]
    fn a_stop_two_receive_paths_find_moved_past_at_once_ends_once() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = OpenedSpans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let view = test_view("mesh1", "mesh1.node-admin.2");
        let primary = birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1);
        view.digest_view.primaries.take(PrimaryRole::MeshPrimary, &primary_of(&primary, 1));
        let rpc = birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 5);
        let stop = op_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", &rpc.node.node_id, &rpc.node.incarnation, "node.stop");
        assert!(view.digest_view.record(rpc.clone()));
        view.take(&event_by(StatusEvent::Stopped, stop.clone(), &publisher_of(&primary), 11), &FabricId::parse("fab000000001").unwrap(), "mesh-channel");
        let mut late = rpc.clone();
        late.digest_seq = 6;
        assert!(!view.digest_view.record(late), "the stopped birth's heartbeat is refused and kept as the latest");
        let mut listed = rpc.clone();
        listed.status = NodeStatus::Pending;
        let target = Target { publisher: publisher_of(&primary).to_string(), channel: "mesh-channel", held: Some((publisher_of(&primary).to_string(), 11)), version: 12, ..Target::default() };
        both_paths_end_the_stop(&view.digest_view, &listed, &target);
        assert!(view.digest_view.in_flight().is_empty(), "the stop is over");
        let ended = spans.named("rdm.mesh.topology.update.via-stop-ended-in-projection");
        assert_eq!(ended.iter().map(|f| (f["stop_carried_at"].clone(), f["held_again"].clone(), f["held_digest_seq"].clone())).collect::<Vec<_>>(), vec![("11".to_string(), "true".to_string(), "6".to_string())], "ended once: {ended:#?}");
    }

    /// CONTRACT (gossip.md §3.1, the order keys: "`TopologyFull`: its envelope's `sequence`, the mesh's
    /// version, per mesh, whichever mesh-primary sent it (Q389); the publisher reads its envelope `time`
    /// in the same critical section that assigns the version, so a version's time is never earlier than
    /// the one before it"; rejoin_commander trace 75adf49598edd1c7d4a975013f554216): `Backbone::publish`
    /// reads `published_at_rafka_ms` and queues the forwarder's decision under `self.version.lock()` and
    /// `self.forwarder.lock()`, and `Backbone::publish_deleted_meshes` reads `published_at_rafka_ms` under
    /// `self.deleted_versions.lock()`. A concurrent publish or neighbour-up `replay` racing with `now_rafka_ms()`
    /// cannot step between the clock read and the version/forwarder update: `v1` and `v2` are stamped and
    /// forwarded in version order, and a member hearing the mesh channel installs both versions in order.
    /// Must NOT happen: `now_rafka_ms()` read outside `self.version.lock()` / `self.forwarder.lock()` so a
    /// concurrent publish or replay stamps `v1` at 471 ms while `v2` is stamped at 459 ms, causing
    /// `ReceiveOrder` to ignore `TopologyDelta(1 -> 2)` as `via-stale`.
    #[tokio::test]
    async fn interleaved_backbone_publishes_and_neighbour_up_replay_stamp_and_forward_in_version_order() {
        use rdm_transport::clock::{Clock, SharedClock};
        use iroh::protocol::Router;
        use std::sync::atomic::AtomicU64;
        use std::sync::OnceLock;

        #[derive(Debug)]
        struct RacingClock {
            now: AtomicU64,
            version_lock: OnceLock<Arc<Mutex<SourceVersion>>>,
            forwarder_lock: OnceLock<Arc<Mutex<crate::aggregate::Forwarder>>>,
            replayed_during_stamp: Mutex<Vec<Frame>>,
        }

        impl Clock for RacingClock {
            fn now_rafka_ms(&self) -> u64 {
                let tick = self.now.fetch_add(1, Ordering::SeqCst);
                let stamp = match tick {
                    0 => 450,
                    1 => 459,
                    _ => 471 + tick,
                };
                // On the v2 publish's clock read (459 ms), a concurrent racer tries to enter the version
                // and forwarder locks: if `now_rafka_ms()` was read outside those locks, a neighbour-up
                // replay (or a second publish) steps in at 471 ms while the forwarder still holds v1.
                if stamp == 459 {
                    if let Some(f_lock) = self.forwarder_lock.get() {
                        if let Ok(mut f) = f_lock.try_lock() {
                            *self.replayed_during_stamp.lock().unwrap() = f.replay(471);
                        }
                    }
                    if let Some(v_lock) = self.version_lock.get() {
                        assert!(v_lock.try_lock().is_err(), "published_at_rafka_ms must be read while Backbone::version is held");
                    }
                }
                stamp
            }
        }

        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let (admin, rpc1, rpc2) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1), birth_at(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", 1));
        let p = publisher_of(&admin);

        let transport = iroh::endpoint::QuicTransportConfig::builder().build();
        let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();

        let racing = Arc::new(RacingClock {
            now: AtomicU64::new(0),
            version_lock: OnceLock::new(),
            forwarder_lock: OnceLock::new(),
            replayed_during_stamp: Mutex::new(Vec::new()),
        });
        let clock: SharedClock = racing.clone();
        let m = TopologyView::new(&fabric, "mesh1", &MeshId::mint(), "mesh1.node-admin.1", clock, Default::default());
        let m_channels = crate::channel::Channels::new(&gossip, &ep, &m).unwrap();
        m_channels.join(crate::channel::ChannelId::mesh(m.fabric_id(), m.mesh_id()), vec![]).await.unwrap();
        m_channels.set_publisher(&rdm_core::PublisherId { incarnation: admin.node.incarnation.clone(), node_id: admin.node.node_id.clone() });
        m_channels.join(crate::channel::ChannelId::backbone(&fabric), vec![]).await.unwrap();
        let bb = m_channels.backbone().expect("the backbone is joined");
        bb.set_mesh_primary(true);
        let _ = racing.version_lock.set(bb.version.clone());
        let _ = racing.forwarder_lock.set(m.view.forwarder.clone());

        assert!(bb.publish(&m, vec![admin.clone(), rpc1.clone()]).await);
        assert!(bb.publish(&m, vec![admin.clone(), rpc1.clone(), rpc2.clone()]).await);
        assert!(racing.replayed_during_stamp.lock().unwrap().is_empty(), "replay could not interleave inside the stamp critical section");
        assert_eq!(m.view.forwarder.lock().unwrap().forwarded_version("mesh1"), Some(2), "the forwarder holds its own publication of v2, taken under the version lock");
        assert!(spans.named("rdm.mesh.topology.reject.via-stale").is_empty(), "no frame was ignored as earlier");
    }

    /// CONTRACT (gossip.md receive rules, `TopologyFull`: "`P(topology_version)` against the mesh's
    /// held floor, ..., including a topology read"; #3211 red 2, gate 19d4afbee release,
    /// `mock_control_loss_recovers_without_status_heartbeat`: 485.567 mesh2.node-admin.2, mesh2's primary,
    /// published mesh2 v28; 485.811 it wrote a topology read of its own v25 served by
    /// mesh2.node-admin.1 (the spans then named `via-aggregate-installed` and `via-read-install`), and
    /// broker.3 went Pending fleet-wide): a primary's own publications are its floor for its own
    /// source, so a topology read naming its own exact birth below the version it published is refused
    /// `OlderThanHeld` naming that version, and a read of the version it published is not refused for
    /// its order. Must NOT happen: a primary writing an older image of its own mesh over what it
    /// publishes.
    #[tokio::test]
    async fn a_primary_refuses_a_read_of_its_own_source_below_the_version_it_published() {
        use iroh::protocol::Router;
        let spans = OpenedSpans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let fabric = FabricId::parse("fab000000001").unwrap();
        let (admin, rpc1, rpc2, rpc3) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1), birth_at(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", 1), birth_at(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", 1));
        let p = publisher_of(&admin);
        let transport = iroh::endpoint::QuicTransportConfig::builder().build();
        let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let m = TopologyView::new(&fabric, "mesh1", &MeshId::mint(), "mesh1.node-admin.1", rdm_transport::clock::os_clock(), Default::default());
        let m_channels = crate::channel::Channels::new(&gossip, &ep, &m).unwrap();
        m_channels.join(crate::channel::ChannelId::mesh(m.fabric_id(), m.mesh_id()), vec![]).await.unwrap();
        m_channels.set_publisher(&p.clone());
        m_channels.join(crate::channel::ChannelId::backbone(&fabric), vec![]).await.unwrap();
        let bb = m_channels.backbone().expect("the backbone is joined");
        bb.set_mesh_primary(true);
        assert!(bb.publish(&m, vec![admin.clone(), rpc1.clone()]).await);
        assert!(bb.publish(&m, vec![admin.clone(), rpc1.clone(), rpc2.clone()]).await);
        assert!(bb.publish(&m, vec![admin.clone(), rpc1.clone(), rpc2.clone(), rpc3.clone()]).await);
        let published = bb.own.lock().unwrap().as_ref().map(|(v, _)| *v).expect("published");
        assert!(published >= 3, "three material changes bump the version three times: {published}");
        let read = |version: u64| crate::aggregate::Chunk { mesh: "mesh1".into(), mesh_id: None, author: None, topology_version: version, time_ms: version, chunk_index: 0, chunk_count: 1, digests: vec![admin.clone(), rpc1.clone()], runtimes: Vec::new() };
        let taken = m_channels.take_read_chunk(read(1), "mesh1");
        assert!(
            matches!(taken, Taken::Refused(crate::aggregate::Refusal::OlderThanHeld { held_version, offered_version: 1 }) if held_version == published),
            "a read of its own v1 below its published v{published} is refused: {taken:?}"
        );
        assert!(m.held_copy("mesh1").is_none_or(|(v, _)| v != 1), "its own older image is not written to the topology view");
        let refused: Vec<_> = spans.named("rdm.mesh.topology.reject.via-stale").into_iter().filter(|f| f.get("frame").map(String::as_str) == Some("topology-read")).collect();
        assert_eq!(refused.len(), 1, "one refusal, named: {refused:#?}");
        assert_eq!((refused[0]["node"].as_str(), refused[0]["held_key"].clone(), refused[0]["frame_key"].as_str()), ("mesh1.node-admin.1", format!("topology_version={published}"), "topology_version=1"));
        assert!(spans.named("rdm.mesh.topology.update.via-full").iter().all(|f| f.get("channel").map(String::as_str) != Some("topology-read")), "no read is written");
    }

    /// CONTRACT (gate 7965f026, `admin_top_up`: `mesh1.node-admin.2` called `GetTopology` on `mesh1.node-admin.1`
    /// while `mesh1.node-admin.1` served `topology_version = 0` before its first `anti_entropy` publication;
    /// `Frame::TopologyFull(mesh1, topology_version = 1)` arrived on `mesh-channel` and `backbone` 600 µs
    /// before the `Side::Read` `topology_version = 0` chunk, advancing `ReceiveOrder` to `v1` while
    /// `View::apply_full` recorded only `take_projected_status` for own-mesh members; `View::take_read_chunk`
    /// refused `v0` as `OlderThanHeld { held_version: 1, offered_version: 0 }` without recording the
    /// own-mesh member digests into `DigestView`, leaving `mesh1.node-admin.2` with the primary record
    /// `mesh1:m1w2kypnr9xf@nejxnr0r5rwd (revision 1)` and `digest_view.get(primary) == None`, so top-up
    /// refused `primary-not-held`): when a `Side::Read` chunk of the node's own mesh arrives below
    /// the version its copy already holds from `mesh-channel` / `backbone`, the copy and `order` stay
    /// at `held_version` (`OlderThanHeld`), and the own-mesh member digests the read
    /// carries are still held in `DigestView` (preserving the projected status from `held_version`), so
    /// the primary named by the read's `Primaries` frame is held and `mesh_primary()` resolves it.
    #[test]
    fn an_own_mesh_topology_read_overtaken_by_gossip_keeps_the_newer_projection_and_holds_the_mesh_primarys_digest() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let (mut primary, mut admin2) = (birth_at(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 1), birth_at(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", 1));
        primary.status = NodeStatus::Ready;
        admin2.status = NodeStatus::Pending;
        let p = publisher_of(&primary);

        let view = test_view("mesh1", "mesh1.node-admin.2");
        assert!(view.digest_view.record(admin2.clone()));

        // Gossip v1 arrives on the backbone, where a node-admin hears its own mesh's aggregate (Q387),
        // while the v0 topology-read stream is in flight.
        let gossip_v1 = stamped(
            aggregate_of("mesh1", &p, 1, vec![primary.clone(), admin2.clone()]),
            100,
        );
        assert!(hear(&mut view.order.lock().unwrap(), &view, &gossip_v1, &fabric, Side::Backbone));
        assert_eq!(view.copies.lock().unwrap().held_version("mesh1"), Some((Some(p.clone()), 1)));

        // The in-flight topology read served at v0 arrives on Side::Read, followed by the primary record.
        let read_v0 = crate::aggregate::Chunk { mesh: "mesh1".into(), mesh_id: None, author: None, topology_version: 0, time_ms: 1, chunk_index: 0, chunk_count: 1, digests: vec![primary.clone(), admin2.clone()], runtimes: Vec::new() };
        let taken = view.take_read_chunk(read_v0, Side::Read, &fabric);
        assert!(
            matches!(taken, Taken::Refused(crate::aggregate::Refusal::OlderThanHeld { held_version: 1, offered_version: 0 })),
            "the source projection stays at v1: {taken:?}"
        );
        assert_eq!(view.copies.lock().unwrap().held_version("mesh1"), Some((Some(p), 1)));

        assert!(matches!(view.digest_view.primaries.take(PrimaryRole::MeshPrimary, &primary_of(&primary, 1)), RoleTaken::Held { .. }));
        let resolved = view.digest_view.mesh_primary("mesh1").expect("the primary's digest from the own-mesh topology read is held in the digest view");
        assert_eq!(resolved.node.node_id, primary.node.node_id);
        assert_eq!(resolved.status, NodeStatus::Ready, "the projected status from v1 is preserved");
    }

    /// A birth of `name` at endpoint key `key`, digest number `seq`.
    fn digest_at(name: rdm_core::PathName, mesh: &str, r#type: &str, key: &str, id: &NodeId, inc: &IncarnationId, seq: u64) -> MeshDigest {
        let mut d = digest(id, inc, None, NodeStatus::Ready, seq);
        { let at = name; d.node.path = at.path; d.node.name = at.name; }
        d.node.mesh = mesh.into();
        d.node.r#type = r#type.to_string();
        d.node.endpoint_id = EndpointId(key.into());
        d
    }

    /// The lookups by endpoint key and by (path, name) answer what a scan of `all()` finds: for every
    /// key one birth holds, that birth's digest and the silence `get` gives its NodeId; for a key two
    /// births hold, one of them; for a key no held birth has, nothing.
    fn assert_lookups_agree(view: &DigestView, gone: &[(rdm_core::PathName, &str)]) {
        let now = Instant::now();
        let all = view.all();
        for d in &all {
            let key = d.node.endpoint_id.0.as_str();
            let at_key: Vec<&MeshDigest> = all.iter().filter(|x| x.node.endpoint_id.0 == key).collect();
            let (by_key, silent) = view.get_by_endpoint_at(key, now).expect("a held birth is found by its endpoint key");
            assert!(at_key.contains(&&by_key), "the endpoint lookup answers a birth a scan finds at {key}");
            if at_key.len() == 1 {
                assert_eq!(view.get_at(by_key.node.node_id.as_str(), now), Some((by_key.clone(), silent)), "the endpoint lookup answers what get answers for its NodeId");
            }
            let at_name: Vec<&MeshDigest> = all.iter().filter(|x| x.node.location() == d.node.location()).collect();
            let (by_name, silent) = view.get_by_path_name_at(&d.node.location(), now).expect("a held birth is found by its (path, name)");
            assert!(at_name.contains(&&by_name), "the (path, name) lookup answers a birth a scan finds at {:?}", d.node.name);
            if at_name.len() == 1 {
                assert_eq!(view.get_at(by_name.node.node_id.as_str(), now), Some((by_name, silent)), "the (path, name) lookup answers what get answers for its NodeId");
            }
        }
        for (name, key) in gone {
            if !all.iter().any(|d| d.node.endpoint_id.0 == *key) {
                assert!(view.get_by_endpoint(key).is_none(), "no held birth has the key {key}");
            }
            if !all.iter().any(|d| d.node.location() == *name) {
                assert!(view.get_by_path_name(name).is_none(), "no held birth has the path/name {name:?}");
            }
        }
    }

    /// CONTRACT: a birth is found by its endpoint key and by its (path, name) with a map read that
    /// answers what a scan of `all()` finds, through every write to the view: a digest heard, a
    /// newer one, a forwarded, topology or join copy, a stop and the node.joined that ends it (which
    /// renames the birth), a successor at the same (path, name) with a new key, and the departure, stop or mesh deletion that removes a birth.
    #[test]
    fn a_birth_is_found_by_its_endpoint_key_and_its_path_name_through_every_write() {
        let view = DigestView::default();
        let (a, a_inc) = (NodeId::mint(), IncarnationId::mint());
        let (b, b_inc) = (NodeId::mint(), IncarnationId::mint());
        let (c, c_inc) = (NodeId::mint(), IncarnationId::mint());
        let gone = [(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "key-a"), (rdm_core::PathName::new("mesh1.node", "2").unwrap(), "key-b"), (rdm_core::PathName::new("mesh1.node", "3").unwrap(), "key-c"), (rdm_core::PathName::new("mesh1.node", "4").unwrap(), "key-c2"), (rdm_core::PathName::new("mesh1.node", "9").unwrap(), "key-z")];
        assert_lookups_agree(&view, &gone);

        // A digest heard, then a newer one of the same birth.
        assert!(view.record_at(digest_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", "key-a", &a, &a_inc, 1), Instant::now()));
        assert_lookups_agree(&view, &gone);
        assert!(view.record_at(digest_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", "key-a", &a, &a_inc, 2), Instant::now()));
        assert_eq!(view.get_by_endpoint("key-a").unwrap().0.digest_seq, 2, "the newer digest is the one found");
        assert_eq!(view.get_by_path_name(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).unwrap().0.digest_seq, 2);
        assert_lookups_agree(&view, &gone);

        // A forwarded, a topology and a join copy.
        assert!(view.record_forwarded_at(digest_at(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", "key-b", &b, &b_inc, 1), Instant::now()));
        assert_lookups_agree(&view, &gone);
        assert!(view.record_topology_at(digest_at(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", "key-b", &b, &b_inc, 2), Instant::now()));
        assert_lookups_agree(&view, &gone);
        assert!(view.record_joined(digest_at(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", "key-c", &c, &c_inc, 1)));
        assert_lookups_agree(&view, &gone);

        // A stop removes the birth; the node.joined that ends it holds it again, at the name it carries.
        assert!(view.stopped(op_at(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", &c, &c_inc, "node.stop")));
        assert!(view.get_by_endpoint("key-c").is_none(), "a stopped birth is not found by its key");
        assert!(view.get_by_path_name(&rdm_core::PathName::new("mesh1.node", "3").unwrap()).is_none(), "nor by its (path, name)");
        assert_lookups_agree(&view, &gone);
        assert!(view.joined(&op_at(rdm_core::PathName::new("mesh1.node", "4").unwrap(), "mesh1", "node", &c, &c_inc, "node.start")));
        assert_eq!(view.get_by_path_name(&rdm_core::PathName::new("mesh1.node", "4").unwrap()).unwrap().0.node.node_id, c, "held again at the name node.joined gives it");
        assert!(view.get_by_path_name(&rdm_core::PathName::new("mesh1.node", "3").unwrap()).is_none(), "and no longer at the old one");
        assert_lookups_agree(&view, &gone);

        // A successor at the same (path, name) over a newly allocated data dir: a new NodeId and key.
        let (a2, a2_inc) = (NodeId::mint(), IncarnationId::mint());
        assert!(view.record_at(digest_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", "key-a2", &a2, &a2_inc, 1), Instant::now()));
        assert_eq!(view.get_by_path_name(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).unwrap().0.node.node_id, a2, "of two births at one (path, name), the one taken most recently");
        assert_eq!(view.get_by_endpoint("key-a").unwrap().0.node.node_id, a, "the predecessor is still found by its own key");
        // A newer heartbeat of the predecessor keeps its place: the successor is still the one found.
        assert!(view.record_at(digest_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", "key-a", &a, &a_inc, 3), Instant::now()));
        assert_eq!(view.get_by_path_name(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).unwrap().0.node.node_id, a2);
        assert_lookups_agree(&view, &gone);

        // The predecessors' departures leave the successor found.
        assert!(view.depart_at(op_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", &a, &a_inc, "node.delete"), Instant::now()));
        assert!(view.get_by_endpoint("key-a").is_none(), "a deleted birth is not found by its key");
        assert_eq!(view.get_by_path_name(&rdm_core::PathName::new("mesh1.node", "1").unwrap()).unwrap().0.node.node_id, a2);
        assert!(view.depart_at(op_at(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", &b, &b_inc, "node.delete"), Instant::now()));
        assert!(view.get_by_endpoint("key-b").is_none(), "a deleted birth is not found by its key");
        assert_lookups_agree(&view, &gone);

        // The mesh's deletion departs every birth left.
        view.depart_mesh_births("mesh1", "b9", 9);
        assert!(view.all().is_empty());
        for (name, key) in &gone {
            assert!(view.get_by_endpoint(key).is_none() && view.get_by_path_name(name).is_none(), "nothing is found once the mesh is deleted");
        }
    }

    /// What `projected_member_at` found before the endpoint index: a scan of every source, by mesh
    /// name, of its members by node id.
    fn scanned_member_at(rx: &AggregateReceiver, key: &str) -> Option<MeshDigest> {
        rx.sources().into_iter().find_map(|(_, _, _, full)| full.digests().into_iter().find(|d| d.node.endpoint_id.0 == key))
    }

    /// CONTRACT: a source projection's member is found by its endpoint key with a map read that
    /// answers what a scan of the sources finds (the first source by mesh name, of two members at one
    /// key the first by node id), through every move of a source: its install, a delta that adds a
    /// member, moves a member's key or removes one, and a load-free copy.
    #[test]
    fn a_projected_member_is_found_by_its_endpoint_key_as_a_scan_of_the_sources_finds_it() {
        let p = |mesh: &str| PublisherId { incarnation: IncarnationId::mint(), node_id: NodeId::mint() };
        let (p1, p2) = (p("mesh1"), p("mesh2"));
        let chunk = |mesh: &str, _publisher: &PublisherId, digests: Vec<MeshDigest>| Chunk { mesh: mesh.into(), mesh_id: None, author: None, topology_version: 1, time_ms: 1, chunk_index: 0, chunk_count: 1, digests: digests, runtimes: Vec::new() };
        let (a, b, c) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        let inc = IncarnationId::mint();
        let keys = ["key-a", "key-b", "key-c", "key-a2", "key-z"];
        let mut rx = AggregateReceiver::default();
        let agree = |rx: &AggregateReceiver| {
            for key in keys {
                assert_eq!(rx.member_at(key), scanned_member_at(rx, key), "the endpoint lookup answers what a scan finds at {key}");
            }
        };
        agree(&rx);
        assert!(matches!(rx.take_chunk(chunk("mesh2", &p2, vec![digest_at(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", "key-a", &a, &inc, 1)])), Taken::Written(_)));
        assert!(matches!(rx.take_chunk(chunk("mesh1", &p1, vec![digest_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", "key-a", &b, &inc, 1), digest_at(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", "key-b", &c, &inc, 1)])), Taken::Written(_)));
        assert_eq!(rx.member_at("key-a").unwrap().node.node_id, b, "the first source by mesh name");
        agree(&rx);
        // A delta that adds a member at a held key, moves a member's key and removes one.
        let changed = vec![digest_at(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", "key-b", &a, &inc, 1), digest_at(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", "key-a2", &b, &inc, 2)];
        assert!(matches!(rx.take_delta("mesh1", &birth_of(&p1), 1, 2, &Delta { added: changed, ..Delta::default() }), Moved::Applied { .. }));
        assert_eq!(rx.member_at("key-a").unwrap().node.node_id, a, "the moved key is found in the next source");
        agree(&rx);
        let birth = |id: &NodeId| rdm_core::framing::Birth { node_id: id.clone(), incarnation: inc.clone() };
        assert!(matches!(rx.take_delta("mesh1", &birth_of(&p1), 2, 3, &Delta { removed: vec![birth(&c), birth(&b)], ..Delta::default() }), Moved::Applied { .. }));
        agree(&rx);
        // A full rebuilt from its members (a source aggregate's, a read's) finds what its full finds.
        let full = rx.held_full("mesh1").unwrap().clone();
        let rebuilt = Full::new(full.digests(), full.in_flight(), full.deleted());
        for key in keys {
            assert_eq!(rebuilt.member_at(key).map(|d| d.node.node_id.clone()), full.member_at(key).map(|d| d.node.node_id.clone()));
        }
        rx.remove("mesh2");
        agree(&rx);
    }

    /// The chunk an aggregate frame is, as its receiver takes it: its author, version and time from its
    /// envelope (gossip.md §3.1 `TopologyFull`, Q389).
    fn chunk_of_aggregate(f: &Frame) -> Chunk {
        let context = f.context().expect("an aggregate is authored");
        let Frame::TopologyFull { mesh, mesh_id, chunk_index, chunk_count, nodes } = f.data() else { panic!("not an aggregate: {f:?}") };
        Chunk { mesh: mesh.clone(), mesh_id: mesh_id.clone(), author: Some(context.source.clone()), topology_version: context.version().expect("an aggregate's version"), time_ms: context.time_ms, chunk_index: *chunk_index, chunk_count: *chunk_count, digests: nodes.iter().map(|n| n.digest(&context.source.fabric, mesh)).collect(), runtimes: Vec::new() }
    }

    /// CONTRACT (gossip.md §3.1 `TopologyFull`, Q389, Q390): a receiver assembles an aggregate's chunks
    /// by (mesh, version, author), the envelope's `subject`, `sequence` and `source`: two authors'
    /// chunks at one version are two sets, neither completing the other; a set installs once it holds
    /// every chunk, naming its author; that author's exact repeat is a duplicate, and its later `time` at
    /// the held version is a new publication (refreshed). Must NOT happen: one author's chunk completing
    /// another's set; a partial set installed; a repeat taken as new.
    #[test]
    fn an_aggregates_chunks_assemble_by_mesh_version_and_author() {
        use crate::aggregate::Refusal;
        let (a, b) = (PublisherId { incarnation: IncarnationId::mint(), node_id: NodeId::mint() }, PublisherId { incarnation: IncarnationId::mint(), node_id: NodeId::mint() });
        let (x, y) = (born("mesh2", "node", 1, 1), born("mesh2", "node", 2, 1));
        let of = |p: &PublisherId, at: u64, index: u32, nodes: Vec<MeshDigest>| chunk_of_aggregate(&Frame::aggregate("mesh2", None, &author_of(p), 5, at, index, 2, nodes));
        let mut rx = AggregateReceiver::default();
        assert!(matches!(rx.take_chunk(of(&a, 50, 0, vec![x.clone()])), Taken::Waiting { held: 1, of: 2 }));
        assert!(matches!(rx.take_chunk(of(&b, 50, 1, vec![y.clone()])), Taken::Waiting { held: 1, of: 2 }), "another author's chunk at the one version is its own set");
        assert!(rx.held_version("mesh2").is_none(), "no set is complete");
        let Taken::Written(i) = rx.take_chunk(of(&a, 50, 1, vec![y.clone()])) else { panic!("the first author's set completes") };
        assert_eq!((i.publisher.clone(), i.topology_version, i.time_ms, i.full.member_count()), (Some(a.clone()), 5, 50, 2));
        assert!(matches!(rx.take_chunk(of(&a, 50, 0, vec![x.clone()])), Taken::Refused(Refusal::Duplicate)), "its exact repeat is a duplicate");
        let Taken::Written(j) = rx.take_chunk(of(&b, 50, 0, vec![x.clone()])) else { panic!("the second author's set completes on its own chunks") };
        assert_eq!((j.publisher.clone(), j.refreshed), (Some(b.clone()), true), "the same content at the held version, its own publication");
        assert!(matches!(rx.take_chunk(of(&a, 60, 0, vec![x.clone()])), Taken::Waiting { .. }), "a later time of the first author is a new publication");
        let Taken::Written(k) = rx.take_chunk(of(&a, 60, 1, vec![y.clone()])) else { panic!("the new publication completes") };
        assert_eq!((k.time_ms, k.refreshed), (60, true));
        assert_eq!(rx.held_version("mesh2"), Some((Some(a), 5)));
    }

    /// CONTRACT (gossip.md §3.1 `TopologyFull`, Q390): the aggregate carries no `in_flight` or `deleted`:
    /// a node of its mesh under a stop is listed `stopped`, one under a removal `deleting`, a deleted one
    /// `deleted` for the deleted retention; a node of another mesh is not listed. A peer's receiver of
    /// that aggregate holds each by its status: the stop, the removal and the departure. Must NOT happen:
    /// an operation the aggregate carries beside its nodes; a stop, removal or departure lost on the
    /// backbone for want of a list.
    #[test]
    fn the_aggregate_lists_a_stopped_a_deleting_and_a_deleted_node_by_status_and_a_receiver_holds_each() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let primary = test_view("mesh1", "mesh1.node-admin.1");
        let admin = born("mesh1", "node-admin", 1, 1);
        let (stopping, removing, gone, ready) = (born("mesh1", "node", 1, 1), born("mesh1", "node", 2, 1), born("mesh1", "node", 3, 1), born("mesh1", "node", 4, 1));
        let other = born("mesh2", "node", 1, 1);
        let op_of = |d: &MeshDigest, action: &str| op_at(d.node.location(), &d.node.mesh, &d.node.r#type, &d.node.node_id, &d.node.incarnation, action);
        for d in [&admin, &stopping, &removing, &gone, &ready, &other] {
            assert!(primary.digest_view.record(d.clone()));
        }
        assert!(primary.digest_view.stopped(op_of(&stopping, "node.stop")));
        assert!(primary.digest_view.deleting(op_of(&removing, "node.delete")));
        assert!(primary.digest_view.depart(op_of(&gone, "node.delete")));
        assert!(primary.digest_view.depart(op_of(&other, "node.delete")));
        // What the mesh-primary publishes (`Backbone::own_full`): its members, the stops, removals and
        // departures it holds said by each node's status.
        let (in_flight, deleted) = primary.digest_view.overlays_of("mesh1");
        let members = vec![admin.clone(), removing.clone(), ready.clone()];
        let nodes = primary.digest_view.aggregate_nodes("mesh1", members, &in_flight, &deleted);
        let status_of = |d: &MeshDigest| nodes.iter().find(|n| n.node.node_id == d.node.node_id).map(|n| n.status);
        assert_eq!(
            (status_of(&admin), status_of(&stopping), status_of(&removing), status_of(&gone), status_of(&ready), status_of(&other)),
            (Some(NodeStatus::Ready), Some(NodeStatus::Stopped), Some(NodeStatus::Deleting), Some(NodeStatus::Deleted), Some(NodeStatus::Ready), None),
            "each node by its status, its own mesh's only: {nodes:?}"
        );
        let aggregate = Frame::aggregate("mesh1", None, &author_of(&publisher_of(&admin)), 3, 30, 0, 1, nodes.clone());
        let Frame::TopologyFull { nodes: listed, .. } = aggregate.data() else { unreachable!() };
        assert_eq!(listed.len(), 5, "one list of nodes, nothing beside it");

        let peer = test_view("mesh2", "mesh2.node-admin.1");
        assert!(hear(&mut peer.order.lock().unwrap(), &peer, &aggregate, &fabric, Side::Backbone));
        assert!(peer.digest_view.holds_stopped(stopping.node.node_id.as_str(), &stopping.node.incarnation), "the node listed stopped is held stopped");
        assert!(peer.digest_view.in_flight().iter().any(|op| !op.is_stop() && op.node_id == removing.node.node_id && op.incarnation == removing.node.incarnation), "the node listed deleting is held under its removal");
        assert!(!peer.digest_view.routable(removing.node.node_id.as_str()), "a node under removal takes no new work");
        assert!(peer.digest_view.is_deleted(gone.node.node_id.as_str()) && peer.digest_view.get(gone.node.node_id.as_str()).is_none(), "the node listed deleted departs");
        assert!(peer.digest_view.get(ready.node.node_id.as_str()).is_some_and(|(d, _)| d.status == NodeStatus::Ready), "a ready node is held ready");
    }

    /// CONTRACT (gossip.md "Node-stopped lifecycle overlay", §3.1 `TopologyFull`, Q390): mesh1.node-admin.2,
    /// not its mesh's primary, holds mesh1.node.1 from its own Digest. Its mesh-primary's aggregate lists
    /// the node `stopped`, with no operation identity; then the primary's `NodeStopped` of that stop
    /// arrives on the mesh channel, naming its operation (a standalone stop's occurrence). The frame is
    /// the same stop: the admin keeps the digest it held of the birth when it dropped it, so the view it
    /// serves lists the birth `stopped` with its endpoint, and the mesh-primary's `node.joined` holds the
    /// birth again from it and ends the stop under both identities: a late copy of the frame opens
    /// nothing. Must NOT happen: the frame replacing what the admin held of the stopped birth, so that it
    /// serves no stopped entry and, as the next mesh-primary, holds no endpoint to start the birth by
    /// (the gate's node_stop reds at 9d8d01079: `held_digest_seq` 0 on the second `via-node-stopped`).
    #[test]
    fn a_node_stopped_after_the_aggregate_listed_the_stop_keeps_the_stopped_births_digest() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let primary = born("mesh1", "node-admin", 1, 1);
        let node = born("mesh1", "node", 1, 7);
        let (id, inc) = (node.node.node_id.clone(), node.node.incarnation.clone());
        let admin = test_view("mesh1", "mesh1.node-admin.2");
        assert!(admin.digest_view.record(node.clone()));
        let mut listed = node.clone();
        listed.status = NodeStatus::Stopped;
        let aggregate = Frame::aggregate("mesh1", None, &author_of(&publisher_of(&primary)), 2, 2, 0, 1, vec![primary.clone(), listed]);
        assert!(hear(&mut admin.order.lock().unwrap(), &admin, &aggregate, &fabric, Side::Backbone));
        assert!(admin.digest_view.holds_stopped(id.as_str(), &inc), "the node the aggregate lists stopped is held stopped");
        let mut stop = op_at(node.node.location(), "mesh1", "node", &id, &inc, "node.stop");
        stop.operation = stop.operation.clone().in_occurrence(rdm_core::OperationOccurrenceId::mint());
        let frame = event_by(StatusEvent::Stopped, stop.clone(), &publisher_of(&primary), 2);
        let mut channel = crate::receive_order::ReceiveOrder::default();
        assert!(hear(&mut channel, &admin, &frame, &fabric, Side::MeshChannel));
        assert!(admin.digest_view.in_flight().iter().any(|op| op.key() == stop.key()), "the frame's identity is held with the stop");
        let kept = admin.digest_view.held_digest_of(&stop).expect("the digest held of the birth when it was dropped");
        assert_eq!((kept.digest_seq, &kept.node.endpoint_id), (7, &node.node.endpoint_id), "the frame keeps what the admin held of the stopped birth");
        let (in_flight, deleted) = admin.digest_view.overlays_of("mesh1");
        let served = admin.digest_view.aggregate_nodes("mesh1", Vec::new(), &in_flight, &deleted);
        assert_eq!(served.iter().map(|d| (d.node.node_id.clone(), d.status, d.digest_seq)).collect::<Vec<_>>(), vec![(id.clone(), NodeStatus::Stopped, 7)], "the view it serves lists the birth stopped");
        assert!(admin.digest_view.joined(&op_at(node.node.location(), "mesh1", "node", &id, &inc, "node.start")), "node.joined ends the stop");
        assert_eq!(admin.digest_view.get(id.as_str()).map(|(d, _)| (d.digest_seq, d.status)), Some((7, NodeStatus::Pending)), "the birth is held again from the digest kept");
        assert!(admin.digest_view.in_flight().is_empty(), "both identities of the stop end with it");
        assert!(!admin.digest_view.stopped(stop), "a late copy of the frame opens nothing");
    }

    /// CONTRACT (gossip.md "Mesh delete": "its final mesh topology, the mesh deleted and itself deleting",
    /// "receivers hold the mesh for the deleted retention from that final mesh topology"; Q390): a
    /// deleted mesh's final aggregate lists its last node-admin `deleting` beside the nodes it deleted,
    /// listed `deleted` for the deleted retention. A peer node-admin that receives it holds the last
    /// admin deleted with the rest, so that it resolves Gone. Must NOT happen: the deleted nodes the
    /// aggregate lists taken for nodes of the mesh, so that the final aggregate is never told and the
    /// last admin is held deleting for good (the gate's fabric_primary_handover red at 9d8d01079: mesh1's
    /// final aggregate v19 installed `members 4, deleted 3, deleting 1`, no `via-node-deleted` of its last
    /// admin).
    #[test]
    fn a_final_aggregate_listing_the_deleted_nodes_beside_its_last_admin_deleting_departs_the_last_admin() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let mesh1_id = rdm_core::MeshId::mint();
        let last = born("mesh1", "node-admin", 1, 1);
        let gone = born("mesh1", "node", 1, 1);
        let peer = test_view("mesh2", "mesh2.node-admin.1");
        let aggregate = |version: u64, nodes: Vec<MeshDigest>| Frame::aggregate("mesh1", Some(mesh1_id.clone()), &author_of(&publisher_of(&last)), version, version, 0, 1, nodes);
        assert!(hear(&mut peer.order.lock().unwrap(), &peer, &aggregate(1, vec![last.clone(), gone.clone()]), &fabric, Side::Backbone));
        let listed = |d: &MeshDigest, status: NodeStatus| MeshDigest { status, ..d.clone() };
        assert!(hear(&mut peer.order.lock().unwrap(), &peer, &aggregate(2, vec![listed(&last, NodeStatus::Deleting), listed(&gone, NodeStatus::Deleted)]), &fabric, Side::Backbone));
        assert!(peer.digest_view.is_deleted(gone.node.node_id.as_str()), "the node the final aggregate lists deleted departs");
        assert!(peer.digest_view.is_deleted(last.node.node_id.as_str()), "the last admin, the only node not listed deleted, departs with the final aggregate");
        assert!(peer.digest_view.get(last.node.node_id.as_str()).is_none(), "the last admin is not held as a node");
    }

    /// CONTRACT (proposal edit 12's check, "preserve delivery of changed primary records"; gossip.md "A
    /// primary change on gossip"): each node entry of an aggregate carries the primary roles the node
    /// holds, and a receiver takes the primary records from them (`take_primaries`): the mesh-primary's
    /// role and a node-type primary's, at their revisions; a later version carrying a higher revision
    /// moves the record. Must NOT happen: a primary record lost when the aggregate dropped its digests.
    #[test]
    fn a_node_takes_its_primary_records_from_the_aggregates_node_entries() {
        let fabric = FabricId::parse("fab000000001").unwrap();
        let mut admin = born("mesh1", "node-admin", 1, 1);
        admin.set_primary(Some(3));
        let mut node = born("mesh1", "node", 1, 1);
        node.set_primary(Some(4));
        let peer = test_view("mesh2", "mesh2.node-admin.1");
        let aggregate = |version: u64, nodes: Vec<MeshDigest>| Frame::aggregate("mesh1", None, &author_of(&publisher_of(&admin)), version, version, 0, 1, nodes);
        assert!(hear(&mut peer.order.lock().unwrap(), &peer, &aggregate(1, vec![admin.clone(), node.clone()]), &fabric, Side::Backbone));
        assert_eq!(peer.digest_view.primaries.mesh("mesh1"), Some(primary_of(&admin, 3)), "the mesh-primary's record, from its own entry");
        assert_eq!(peer.digest_view.primaries.node_type_primary("mesh1", "node"), Some(primary_of(&node, 4)), "the node-type primary's record, from its entry");
        let mut moved = node.clone();
        moved.set_primary(Some(5));
        assert!(hear(&mut peer.order.lock().unwrap(), &peer, &aggregate(2, vec![admin.clone(), moved]), &fabric, Side::Backbone));
        assert_eq!(peer.digest_view.primaries.node_type_primary("mesh1", "node"), Some(primary_of(&node, 5)), "a later revision moves the record");
    }

    /// CONTRACT (lead 0002, Q335, Q390: a mesh's aggregate lists only its own mesh's nodes): the
    /// fabric-primary deletes a peer mesh's node while that mesh holds no mesh-primary. It serves the
    /// deletion it holds: its read of that mesh lists the node deleted. The mesh's next mesh-primary's
    /// start read holds the node deleted, and its first aggregate lists it deleted. Must NOT happen: a new
    /// frame, channel or field for it; the deletion lost because the fabric-primary's own mesh's aggregate
    /// no longer carries another mesh's nodes.
    #[test]
    fn a_peer_mesh_node_the_fabric_primary_deleted_reaches_that_meshs_next_mesh_primary_by_its_start_read() {
        // A topology view watches its meshes on the runtime it is made on.
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _runtime = rt.enter();
        let fabric = FabricId::parse("fab000000001").unwrap();
        let (lost, x, rpc) = (born("mesh1", "node-admin", 1, 1), born("mesh1", "node", 1, 1), born("mesh1", "node", 2, 1));
        // The fabric-primary, mesh2's primary, holds mesh1's last aggregate and forwards it.
        let fp = TopologyView::new(&fabric, "mesh2", &MeshId::mint(), "mesh2.node-admin.1", rdm_transport::clock::os_clock(), Default::default());
        fp.view.primary.store(true, Ordering::Relaxed);
        fp.view.forwarder.lock().unwrap().set_author(test_author());
        let taken = fp.view.take_topology_frame(&Frame::aggregate("mesh1", None, &author_of(&publisher_of(&lost)), 7, 70, 0, 1, vec![lost.clone(), x.clone(), rpc.clone()]), &fabric, Side::Backbone, &mut fp.view.order.lock().unwrap());
        let written = taken.source.expect("a complete peer full written to the topology view");
        let _ = fp.view.forwarder.lock().unwrap().source("mesh2.node-admin.1", &written, 70);
        // mesh1's mesh-primary is lost; the fabric-primary deletes mesh1.node.1 (node recovery, Q335).
        let delete = op_at(x.node.location(), "mesh1", "node", &x.node.node_id, &x.node.incarnation, "node.delete");
        fp.digest_view.note_executed_deletion(&delete);
        fp.view.take(&Frame::NodeDeleted { op: delete.clone(), forwarded_by: None }, &fabric, "mesh-channel");
        assert!(fp.digest_view.is_deleted(x.node.node_id.as_str()), "the fabric-primary's view holds the deletion");
        let fp_own = born("mesh2", "node-admin", 1, 1);
        let served = fp.held_topology_of(&fp_own, Some("mesh1"));
        assert_eq!(served.len(), 1);
        let status = |s: &HeldAggregate, d: &MeshDigest| s.digests.iter().find(|n| n.node.node_id == d.node.node_id).map(|n| n.status);
        assert_eq!((served[0].topology_version, status(&served[0], &x), status(&served[0], &rpc)), (7, Some(NodeStatus::Deleted), Some(NodeStatus::Ready)), "its read serves the deletion it holds of that mesh's node");

        // mesh1's next mesh-primary reads the fabric-primary at its start.
        let mp = TopologyView::new(&fabric, "mesh1", &MeshId::mint(), "mesh1.node-admin.2", rdm_transport::clock::os_clock(), Default::default());
        let read = read_of("mesh1", served[0].topology_version, 80, served[0].digests.clone());
        assert!(matches!(mp.view.take_read_chunk(read, Side::ReadPeer, &fabric), Taken::Written(_)));
        assert!(mp.digest_view.is_deleted(x.node.node_id.as_str()), "its start read holds the node deleted");
        mp.view.primary.store(true, Ordering::Relaxed);
        // Its first aggregate (`Backbone::own_full`): its members and what it holds deleted, by status.
        let me = born("mesh1", "node-admin", 2, 1);
        let (in_flight, deleted) = mp.digest_view.overlays_of("mesh1");
        let nodes = mp.digest_view.aggregate_nodes("mesh1", vec![me, rpc.clone()], &in_flight, &deleted);
        assert_eq!(nodes.iter().find(|n| n.node.node_id == x.node.node_id).map(|n| (n.node.incarnation.clone(), n.status)), Some((x.node.incarnation.clone(), NodeStatus::Deleted)), "its first aggregate lists the node deleted: {nodes:?}");
    }
}
