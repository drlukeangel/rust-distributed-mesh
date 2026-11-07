//! The node-admin backbone (gossip.md §3): its channel and the node-admin's publishing role:
//! the full topology of its mesh and its status announcements.


use crate::snapshot::{Full, PublisherId, SourceVersion};
use anyhow::Result;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::net::Gossip;
use rdm_core::{FabricId, LifecycleOp, MeshDigest, Primary, PrimaryRole};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;


use crate::{frame::*, status::*, channel::*, digest_view::*, topology::*};

/// Which of a backbone's adopted statuses still wait for their checklist to complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Announced {
    /// The mesh status still waits for its checklist to complete.
    pub mesh_awaiting_checklist: bool,
    /// The fabric status still waits for its checklist to complete.
    pub fabric_awaiting_checklist: bool,
}

/// A node-admin's place on the backbone. What it hears there it holds; while
/// it is its mesh's primary it publishes its mesh's members and forwards the
/// other meshes' onto its mesh channel; while it is the fabric primary it
/// publishes the fabric's status.
#[derive(Clone)]
pub struct Backbone {
    pub(crate) channel: Channel,
    pub(crate) mesh: String,
    pub(crate) fabric: FabricId,
    pub(crate) publishing: Arc<AtomicBool>,
    pub(crate) status_publishing: Arc<AtomicBool>,
    pub(crate) channels: crate::channel::Channels,
    pub(crate) mesh_status: Arc<Mutex<StatusPublisher>>,
    pub(crate) fabric_status: Arc<Mutex<StatusPublisher>>,
    /// Wakes the status sender when a change starts its reinforcement.
    pub(crate) wake: Arc<tokio::sync::Notify>,
    /// This admin's exact identity as a publisher: its path name and the birth that holds it.
    pub(crate) publisher: PublisherId,
    /// Whether this admin, holding its mesh's primary role, has published its mesh's aggregate on the
    /// backbone since it took the role: a successor's first act (elections §4.1), before it
    /// investigates a lost birth of its mesh. Cleared when the role moves.
    pub(crate) published_as_primary: Arc<AtomicBool>,
    /// Wakes those waiting for [`Self::published_as_primary`].
    pub(crate) published_wake: Arc<tokio::sync::Notify>,
    /// The version counter of this Mesh's topology, and the complete projection it was last bumped on.
    pub(crate) version: Arc<Mutex<SourceVersion>>,
    /// This Mesh as the mesh topology last published it (publisher, version, full): the forwarder's
    /// own-Mesh source.
    pub(crate) own: Arc<Mutex<Option<(u64, Full)>>>,
    /// Test seam ([`Backbone::withhold_announcements`]): asked before each announcement this
    /// [`Withhold`] covers is handed to a channel. The product sets none.
    pub(crate) withhold: Arc<std::sync::OnceLock<Withhold>>,
}

/// Test seam (R-NS2: "Final mesh topologies and leadership announcements are best-effort dissemination"):
/// whether an announcement is withheld from a channel, asked with the announcement's name and the
/// channel (`"backbone"` or `"mesh"`). A `true` answer skips that one send and nothing else: the
/// frame, the channel and the delivery path are untouched. It models the announcement lost on its way
/// out, as a stop's transport cut loses what gossip has not yet pushed. The announcements asked:
/// - `stopping-aggregate`: the mesh topology a node-admin that publishes its mesh's topology sends
///   once more before its stop's transport cut, itself stopping (node-stop.md step 4);
/// - `aggregate-in-flight`: a publication's mesh topology of this admin's own mesh while it
///   carries a lifecycle operation in flight;
/// - `aggregate`: every publication's mesh topology of this admin's own mesh, on the backbone and
///   into its own mesh (`mesh`).
pub type Withhold = Arc<dyn Fn(&'static str, &'static str) -> bool + Send + Sync>;

/// The status frames `me` owes on this turn ([`Backbone::announce_statuses`]): its own mesh's while
/// it holds its mesh-primary role, and the fabric's while it holds the fabric-primary role.
#[allow(clippy::too_many_arguments)]
pub fn status_announcements(
    fabric: &FabricId,
    me: &PublisherId,
    own_mesh: &str,
    digest_view: &DigestView,
    mesh_pub: &mut StatusPublisher,
    fabric_pub: &mut StatusPublisher,
    mesh_status: &str,
    fabric_status: &str,
    mesh_checklist: bool,
    fabric_checklist: bool,
    now: Instant,
    now_ms: u64,
) -> (Vec<Frame>, Announced) {
    let held_meshes = digest_view.statuses.held_meshes_snapshot();
    let heard_mesh = held_meshes.get(own_mesh).map(|h| StatusFact { status: h.status.clone(), changed_at_rafka_ms: h.changed_at_rafka_ms });
    let heard_fabric = digest_view.statuses.fabric().map(|h| StatusFact { status: h.status, changed_at_rafka_ms: h.changed_at_rafka_ms });
    let author = crate::envelope::Birth::publisher(fabric, me);
    let owned = |h: Primary| h.is_birth(&author.node, &author.incarnation).then_some((author.clone(), h.revision));
    mesh_pub.author = digest_view.primaries.mesh(own_mesh).and_then(&owned);
    fabric_pub.author = digest_view.primaries.fabric().and_then(&owned);
    let frames: Vec<Frame> = [
        if mesh_pub.author.is_some() { mesh_pub.observe(mesh_status, heard_mesh, mesh_checklist, now, now_ms) } else { None },
        if fabric_pub.author.is_some() { fabric_pub.observe(fabric_status, heard_fabric, fabric_checklist, now, now_ms) } else { None },
    ]
    .into_iter()
    .flatten()
    .collect();
    (frames, Announced { mesh_awaiting_checklist: mesh_pub.awaiting_checklist(), fabric_awaiting_checklist: fabric_pub.awaiting_checklist() })
}

impl Backbone {
    pub(crate) fn current_name(&self) -> String {
        self.channels.topology_view.node_name()
    }

    pub(crate) fn current_publisher(&self) -> PublisherId {
        self.publisher.clone()
    }

    /// Test seam: hand this backbone the [`Withhold`] it asks before each announcement it covers
    /// (set once; the product never calls it).
    pub fn withhold_announcements(&self, withhold: Withhold) {
        let _ = self.withhold.set(withhold);
    }

    /// Whether the [`Withhold`] this backbone holds withholds `announcement` from `channel`; with
    /// none held, never.
    pub fn withholds(&self, announcement: &'static str, channel: &'static str) -> bool {
        let withheld = self.withhold.get().is_some_and(|w| w(announcement, channel));
        if withheld {
            tracing::info_span!("rdm.mesh.backbone.update.via-announcement-withheld", node = %self.current_name(), mesh = %self.mesh, announcement, channel)
                .in_scope(|| tracing::info!("the test seam withholds this announcement from the channel: nothing is sent on it"));
        }
        withheld
    }

    /// Leave the backbone channel: this admin is a neighbour of no one on it and sends and hears
    /// nothing there until [`Backbone::start`]. Resolves once the channel is left.
    pub async fn stop(&self) {
        self.channel.stop().await;
    }

    /// Join the backbone channel again through the peers it knows.
    pub async fn start(&self) {
        self.channel.start().await;
    }

    /// Join the backbone with the author's exact birth. Every publication retains this supplied
    /// node id and incarnation; no receiver or forwarder reconstructs them from a name.
    pub(crate) async fn join(gossip: &Gossip, endpoint: &Endpoint, channels: &crate::channel::Channels, mesh: &str, author: PublisherId, seeds: Vec<EndpointAddr>) -> Result<Self> {
        // The node is named by its node id on its channel and status spans (no name rides with the author, Q309).
        let node = author.node_id.as_str();
        let birth = crate::envelope::Birth::publisher(&channels.topology_view.fabric, &author);
        *channels.author.lock().unwrap() = Some(birth.clone());
        channels.topology_view.view.forwarder.lock().unwrap().set_author(birth);
        let (m, own) = (channels.clone(), mesh.to_string());
        let on_frame: Arc<dyn Fn(Frame, &mut crate::receive_order::ReceiveOrder) + Send + Sync> = Arc::new(move |frame: Frame, order: &mut crate::receive_order::ReceiveOrder| {
            let me = m.topology_view.view.current_name();
            if matches!(frame.data(), Frame::TopologyFull { .. } | Frame::TopologyDelta { .. }) {
                let taken = m.topology_view.view.take_snapshot(&frame, &m.topology_view.fabric, Side::Backbone, order);
                // A mesh primary's clock follows its current fabric-primary's mesh topology.
                if let Some(p) = &taken.publication {
                    let verdict = crate::discipline::from_fabric_primary_members(&me, m.topology_view.view.primary.load(Ordering::Relaxed), &m.topology_view.view.digest_view.primaries, &p.publisher, p.publisher_digest.as_ref(), p.published_at_rafka_ms);
                    crate::discipline::offer(&m.topology_view.clock, &me, "fabric-primary-mesh-topology", verdict);
                }
                for d in &taken.carried {
                    m.mesh().register(d);
                }
                // A complete snapshot of a source Mesh: the forwarding primary says what moved
                // since it last published that source into its own Mesh. The forward path decides
                // nothing unless this node holds the forwarder role, read under its lock.
                if let Some((source_mesh, publisher, version, full)) = taken.source {
                    let is_final_delete = full.digests().len() == 1
                        && full.digests()[0].status == rdm_core::model::NodeStatus::Leaving
                        && rdm_core::is_node_admin(&full.digests()[0].node.r#type)
                        && full
                            .in_flight()
                            .iter()
                            .any(|op| !op.is_stop() && op.node_id == full.digests()[0].node.node_id);
                    let _ = m.forward_path.source_with_clock(&me, &source_mesh, &publisher, version, &full, || m.topology_view.clock.now_rafka_ms());
                    if is_final_delete {
                        m.topology_view.view.forwarder.lock().unwrap().remove(&source_mesh);
                    }
                }
                return;
            }
            for d in m.topology_view.view.take(&frame, &m.topology_view.fabric, "backbone") {
                m.mesh().register(&d);
            }
            if let Some(f) = forward_of(frame, &me, &own) {
                m.forward_path.carry(&me, f);
            }
        });
        let (replay_view, replay_fabric, replay_author, replay_clock) = (channels.topology_view.view.clone(), channels.topology_view.fabric.clone(), channels.author.clone(), channels.topology_view.clock.clone());
        let replay: Arc<dyn Fn() -> Replayed + Send + Sync> = Arc::new(move || {
            if replay_view.primary.load(Ordering::Relaxed) {
                let replay_me = replay_view.current_name();
                let frames = replay_view.digest_view.statuses.held_frames(&replay_fabric, &replay_me);
                Replayed::direct(frames, replay_author.lock().unwrap().as_ref().expect("backbone owns its author birth"), replay_clock.now_rafka_ms())
            } else {
                Replayed::default()
            }
        });
        let (digest_view, own_mesh) = (channels.topology_view.digest_view.clone(), mesh.to_string());
        let targets: Arc<dyn Fn() -> Held + Send + Sync> =
            Arc::new(move || held_view(&digest_view, |d| d.node.mesh != own_mesh && rdm_core::is_node_admin(&d.node.r#type)));
        let channel = Channel::join(gossip, endpoint, crate::channel::ChannelId::backbone(&channels.topology_view.fabric).topic(), channels.topology_view.fabric.as_str(), node, "backbone", seeds, channels.location.clone(), channels.topology_view.view.order.clone(), on_frame, targets, replay, channels.author.clone(), channels.topology_view.clock.clone()).await?;
        channels.topology_view.mesh_status_publisher.lock().unwrap().node = node.to_string();
        let me = Self {
            channel,
            mesh: mesh.to_string(),
            fabric: channels.topology_view.fabric.clone(),
            publishing: Arc::default(),
            status_publishing: Arc::default(),
            channels: channels.clone(),
            mesh_status: channels.topology_view.mesh_status_publisher.clone(),
            fabric_status: Arc::new(Mutex::new(StatusPublisher::new(node, StatusScope::Fabric(channels.topology_view.fabric.clone())))),
            wake: Arc::default(),
            publisher: author,
            published_as_primary: Arc::default(),
            published_wake: Arc::default(),
            version: Arc::new(Mutex::new(SourceVersion::with_publication_counter(channels.topology_view.view.forwarder.lock().unwrap().publication_counter()))),
            own: Arc::default(),
            withhold: Arc::default(),
        };
        me.spawn_status_sender();
        Ok(me)
    }

    /// Hand `fact` straight to this admin's own mesh [`StatusPublisher`] as its adopted held fact
    /// (Q272; node-create.md "A node-admin created into a live mesh" step 4), never through the gossip receive view.
    pub fn adopt_mesh_status(&self, fact: StatusFact) -> bool {
        let taken = self.mesh_status.lock().unwrap().adopt(fact);
        if taken {
            self.wake.notify_one();
        }
        taken
    }

    /// The status fact this admin's own mesh [`StatusPublisher`] holds, if any.
    pub fn adopted_mesh_status(&self) -> Option<StatusFact> {
        self.mesh_status.lock().unwrap().held()
    }

    /// Sends the reinforcing repeats of a status change, one per second until its fifth send. It
    /// sleeps with nothing due and wakes on a change: no status traffic without a change.
    pub(crate) fn spawn_status_sender(&self) {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                let mesh_next = me.mesh_status.lock().unwrap().next_due();
                let fabric_next = me.fabric_status.lock().unwrap().next_due();
                let next = [mesh_next, fabric_next].into_iter().flatten().min();
                match next {
                    Some(at) => {
                        tokio::select! {
                            _ = me.wake.notified() => {}
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(at)) => {}
                        }
                    }
                    None => me.wake.notified().await,
                }
                let now = Instant::now();
                let mesh_due = me.mesh_status.lock().unwrap().due(now);
                let fabric_due = me.fabric_status.lock().unwrap().due(now);
                let due: Vec<Frame> = [mesh_due, fabric_due].into_iter().flatten().collect();
                for f in due {
                    me.send_status(&f).await;
                }
            }
        });
    }

    /// One status send: the backbone, the author's own mesh channel, and its own view.
    pub(crate) async fn send_status(&self, f: &Frame) {
        let (scope, status, changed_at) = match f.data() {
            Frame::MeshStatus { mesh, status, changed_at_rafka_ms, .. } => (format!("mesh:{mesh}"), status.as_str(), *changed_at_rafka_ms),
            Frame::FabricStatus { fabric, status, changed_at_rafka_ms, .. } => (format!("fabric:{fabric}"), status.as_str(), *changed_at_rafka_ms),
            _ => return,
        };
        tracing::info_span!("rdm.mesh.fabric.update.via-status-send", node = %self.current_name(), scope = %scope, status, changed_at_rafka_ms = changed_at, committed_revision = f.context().and_then(|c| c.primary_revision()).unwrap_or(0), sequence = f.context().and_then(|c| c.sequence.as_deref()).unwrap_or(""))
            .in_scope(|| tracing::info!("status sent"));
        let _ = self.channel.broadcast(f).await;
        let _ = self.channels.forward(f).await;
        let _ = self.channels.topology_view.view.take(f, &self.fabric, "mesh-channel");
    }

    /// The statuses as this node's topology states them now. While this node is its mesh's primary
    /// its mesh's status is sent when it changed, and while it is the fabric primary the fabric's;
    /// a status that did not change sends nothing. A change is sent at once and reinforced by the
    /// status sender (five sends in all). A status adopted from the previous publisher is sent
    /// only once that scope's checklist is complete (`mesh_checklist` / `fabric_checklist`), with its old
    /// instant. The answer says which adopted statuses still wait for their checklist.
    pub async fn announce_statuses(&self, mesh_status: &str, fabric_status: &str, mesh_checklist: bool, fabric_checklist: bool) -> Announced {
        let (now, now_ms) = (Instant::now(), self.channels.topology_view.clock.now_rafka_ms());
        let me = self.current_publisher();
        let digest_view = &self.channels.topology_view.view.digest_view;
        let held_meshes = digest_view.statuses.held_meshes_snapshot();
        let heard_mesh = held_meshes.get(&self.mesh).map(|h| StatusFact { status: h.status.clone(), changed_at_rafka_ms: h.changed_at_rafka_ms });
        let heard_fabric = digest_view.statuses.fabric().map(|h| StatusFact { status: h.status, changed_at_rafka_ms: h.changed_at_rafka_ms });
        let author = crate::envelope::Birth::publisher(&self.fabric, &me);
        let owned = |h: Primary| h.is_birth(&author.node, &author.incarnation).then_some((author.clone(), h.revision));
        let mesh_author = digest_view.primaries.mesh(&self.mesh).and_then(&owned);
        let fabric_author = digest_view.primaries.fabric().and_then(&owned);
        let (mesh_frame, mesh_awaiting_checklist) = {
            let mut mesh_pub = self.mesh_status.lock().unwrap();
            mesh_pub.author = mesh_author;
            let frame = if mesh_pub.author.is_some() { mesh_pub.observe(mesh_status, heard_mesh, mesh_checklist, now, now_ms) } else { None };
            (frame, mesh_pub.awaiting_checklist())
        };
        let (fabric_frame, fabric_awaiting_checklist) = {
            let mut fabric_pub = self.fabric_status.lock().unwrap();
            fabric_pub.author = fabric_author;
            let frame = if fabric_pub.author.is_some() { fabric_pub.observe(fabric_status, heard_fabric, fabric_checklist, now, now_ms) } else { None };
            (frame, fabric_pub.awaiting_checklist())
        };
        let first: Vec<Frame> = [mesh_frame, fabric_frame].into_iter().flatten().collect();
        if !first.is_empty() {
            self.wake.notify_one();
        }
        for f in first {
            self.send_status(&f).await;
        }
        Announced { mesh_awaiting_checklist, fabric_awaiting_checklist }
    }

    pub(crate) fn role(flag: &AtomicBool, on: bool) -> Option<&'static str> {
        (flag.swap(on, Ordering::Relaxed) != on).then_some(if on { "start" } else { "stop" })
    }

    /// The publisher and version of `mesh`'s complete snapshot this admin holds from the backbone.
    pub fn held_source(&self, mesh: &str) -> Option<(PublisherId, u64)> {
        self.channels.topology_view.view.backbone_rx.lock().unwrap().held_version(mesh)
    }

    /// Whether this backbone publishes its mesh's members now (it is the mesh's primary).
    pub fn is_mesh_primary(&self) -> bool {
        self.publishing.load(Ordering::Relaxed)
    }

    /// Be (or stop being) this mesh's publisher and forwarder: its primary.
    pub fn set_mesh_primary(&self, primary: bool) {
        if primary {
            // The mesh's version is the mesh's, whoever its primary is (Q389, Q391): taking the role,
            // this node continues it from the last aggregate of its mesh it installed.
            let held = self.channels.topology_view.view.order.lock().unwrap().installed_snapshot(&self.mesh).map(|(_, version, full, _)| (version, full.clone()));
            if let Some((version, full)) = held {
                self.version.lock().unwrap().continue_from(version, full);
            }
        }
        self.channels.topology_view.set_mesh_primary(primary);
        // A forwarder taking the role starts from what its Mesh holds of each peer mesh: what this node
        // holds of it from its own mesh channel, or a read of its own Mesh (gossip.md §3.3). The role,
        // its baseline and its term switch together under the forwarder's lock; then each peer mesh this
        // node holds goes in as the differential from there.
        let held_in_mesh: Vec<(String, PublisherId, u64, Full)> = self.channels.topology_view.view.mesh_rx.lock().unwrap().sources_in_mesh().into_iter().filter(|(m, ..)| m != &self.mesh).collect();
        let forwarder_changed = self.channels.forward_path.set_role(primary, &held_in_mesh);
        if forwarder_changed && primary {
            self.forward_held_now();
        }
        self.mesh_status.lock().unwrap().set_role(primary);
        if let Some(role) = Self::role(&self.publishing, primary) {
            self.published_as_primary.store(false, Ordering::Relaxed);
            tracing::info_span!("rdm.mesh.backbone.update.via-aggregate-publisher", node = %self.current_name(), mesh = %self.mesh, role)
                .in_scope(|| tracing::info!("mesh topology publication"));
        }
        if forwarder_changed {
            let role = if primary { "start" } else { "stop" };
            tracing::info_span!("rdm.mesh.backbone.update.via-forwarder", node = %self.current_name(), mesh = %self.mesh, role)
                .in_scope(|| tracing::info!("peer meshes forwarded onto this mesh's channel"));
        }
    }

    /// Whether this admin holds its mesh's primary role and has published its mesh's aggregate on the
    /// backbone since it took it (elections §4.1: a successor's first act, before it investigates a lost
    /// birth of its mesh as any member).
    pub fn published_as_primary(&self) -> bool {
        self.publishing.load(Ordering::Relaxed) && self.published_as_primary.load(Ordering::Relaxed)
    }

    /// Resolves once [`Self::published_as_primary`] holds.
    pub async fn until_published_as_primary(&self) {
        loop {
            let woken = self.published_wake.notified();
            if self.published_as_primary() {
                return;
            }
            woken.await;
        }
    }

    /// Whether this backbone publishes the fabric's status now (it is the fabric's primary).
    pub fn is_fabric_primary(&self) -> bool {
        self.status_publishing.load(Ordering::Relaxed)
    }

    /// Be (or stop being) the fabric-status publisher: the fabric primary.
    pub fn set_fabric_primary(&self, primary: bool) {
        self.fabric_status.lock().unwrap().set_role(primary);
        if let Some(role) = Self::role(&self.status_publishing, primary) {
            tracing::info_span!("rdm.mesh.fabric.update.via-status-publisher", node = %self.current_name(), fabric = %self.fabric, role)
                .in_scope(|| tracing::info!("fabric status publication"));
        }
    }

    /// One publication: this mesh's `members` (while its primary) on the backbone, as one
    /// chunked snapshot at the Mesh's `topology_version` (gossip.md §3.1). The version bumps when
    /// the normalized projection changed since the last publication and never otherwise, so a quiet Mesh
    /// republishes the same version as passive anti-entropy. The same projection is this Mesh's
    /// own source: what its members need of it goes into the Mesh as a delta or a full. A status
    /// is not part of a publication: [`Backbone::announce_statuses`] sends one when it changes.
    /// Returns whether it published: `false` when this node does not hold the publisher role, and
    /// nothing was sent.
    pub async fn publish(&self, topology_view: &TopologyView, members: Vec<MeshDigest>) -> bool {
        if !self.publishing.load(Ordering::Relaxed) {
            return false;
        }
        let me_name = self.current_name();
        let publisher = self.current_publisher();
        // The projection is read, versioned, stamped and queued onto this mesh's own channel under
        // one hold of the version (gossip.md §3.1: "the publisher reads `published_at_rafka_ms` in
        // the same critical section that assigns `topology_version` and `snapshot_id`, so a
        // version's stamp is never earlier than the one before it"): a status event stamped in
        // between ([`Backbone::publish_status_event`]) or a neighbour-up replay on the mesh channel
        // is either wholly before it or wholly after.
        let (full, version, snapshot_id, sent, bumped) = {
            let mut v = self.version.lock().unwrap();
            let full = self.own_full(topology_view, members);
            let before = v.version();
            let version = v.observe(&full);
            // What this primary publishes is its floor for its own source (gossip.md receive rules:
            // "against the installed floor, including a topology read"), moved under the same hold.
            self.channels.topology_view.view.order.lock().unwrap().published(&self.mesh, version);
            let snapshot_id = v.next_snapshot_id();
            *self.own.lock().unwrap() = Some((version, full.clone()));
            // Test seam: the projection withheld from its own mesh's channel ([`Withhold`] `aggregate`,
            // `mesh`): nothing is put into the mesh.
            let sent = if self.withholds("aggregate", "mesh") {
                topology_view.clock.now_rafka_ms()
            } else {
                self.channels.forward_path.source_with_clock(&me_name, &self.mesh, &publisher, version, &full, || topology_view.clock.now_rafka_ms())
            };
            (full, version, snapshot_id, sent, version != before)
        };
        if bumped {
            tracing::info_span!("rdm.mesh.backbone.update.via-topology-version", node = %me_name, mesh = %self.mesh, publisher = %publisher, topology_version = version, members = full.member_count())
                .in_scope(|| tracing::info!("the normalized projection of this mesh changed: its topology_version bumped"));
        }
        let mesh = self.mesh.clone();
        let withheld = (!full.in_flight().is_empty() && self.withholds("aggregate-in-flight", "backbone")) || self.withholds("aggregate", "backbone");
        for frame in crate::snapshot::chunks_of(&full, |digests, in_flight, deleted, chunk_index, chunk_count| Frame::TopologyFull {
            mesh: mesh.clone(),
            mesh_id: full.mesh_id(),
            publisher: publisher.clone(),
            forwarded_by: None,
            topology_version: version,
            published_at_rafka_ms: sent,
            snapshot_id,
            chunk_index,
            chunk_count,
            digests,
            in_flight,
            deleted,
        }.authored(&crate::envelope::Birth::publisher(&self.fabric, &publisher), 0, sent))
        .into_iter()
        .filter(|_| !withheld)
        {
            let _ = self.channel.broadcast(&frame).await;
        }
        // The first publication after the role was taken is the successor's first act (elections §4.1):
        // its mesh's aggregate on the backbone names it mesh-primary; its own mesh's update was queued
        // above and goes out asynchronously.
        if !self.published_as_primary.swap(true, Ordering::Relaxed) {
            tracing::info_span!("rdm.mesh.backbone.update.via-primary-aggregate", node = %me_name, mesh = %self.mesh, publisher = %publisher, topology_version = version, withheld)
                .in_scope(|| tracing::info!("the mesh-primary's first aggregate on the backbone since it took the role"));
            self.published_wake.notify_waiters();
        }
        true
    }

    /// This Mesh's members as a publication puts them into its mesh topology: every member of
    /// this Mesh the view heard within the staleness floor.
    pub fn own_members(&self, topology_view: &TopologyView) -> Vec<MeshDigest> {
        topology_view.digest_view.current(topology_view.digest_view.staleness_floor()).into_iter().filter(|d| d.node.mesh == self.mesh).collect()
    }

    /// This Mesh's topology as a publication puts it out: `members` with this mesh's own overlays and
    /// departures, and the deletions this admin executed in a peer mesh ([`DigestView::overlays_of`]);
    /// a peer mesh's other overlays and departures travel in that mesh's topology.
    pub(crate) fn own_full(&self, topology_view: &TopologyView, members: Vec<MeshDigest>) -> Full {
        let (mut in_flight, deleted) = topology_view.digest_view.overlays_of(&self.mesh);
        if members.len() == 1
            && rdm_core::is_node_admin(&members[0].node.r#type)
            && members[0].status != rdm_core::model::NodeStatus::Leaving
        {
            let last_id = &members[0].node.node_id;
            in_flight.retain(|op| op.is_stop() || &op.node_id != last_id);
        }
        Full::new(members, in_flight, deleted).of_mesh(Some(topology_view.mesh_id().clone()))
    }

    /// Resolves when the next publication is due: at `deadline`, the anti-entropy timer, or
    /// at once while this admin is its Mesh's publisher and the Mesh's normalized projection is no
    /// longer the one its `topology_version` was last bumped on (gossip.md, `TopologyFull`: "at once on a
    /// material change"). The projection is looked at when the view says something it carries
    /// moved (a member's birth or status, a member heard again, an overlay or a departure), never
    /// on a timer.
    pub async fn publication_wake(&self, topology_view: &TopologyView, deadline: tokio::time::Instant) {
        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);
        let digest_view = &topology_view.digest_view;
        let (mut heard, mut births, mut overlays, mut primaries) = (digest_view.heard_changes(), digest_view.birth_changes(), digest_view.overlay_changes(), digest_view.primaries.subscribe());
        loop {
            heard.borrow_and_update();
            births.borrow_and_update();
            overlays.borrow_and_update();
            primaries.borrow_and_update();
            if self.publishing.load(Ordering::Relaxed) {
                let full = self.own_full(topology_view, self.own_members(topology_view));
                let published = self.own.lock().unwrap().as_ref().map(|(v, _)| *v).unwrap_or(0);
                let (moved, held) = {
                    let v = self.version.lock().unwrap();
                    // A status event stamped a version no publication has carried yet: the projection
                    // carrying it goes out at once.
                    (v.would_bump(&full) || published != v.version(), v.version())
                };
                if moved {
                    tracing::info_span!("rdm.mesh.backbone.update.via-material-change", node = %self.current_name(), mesh = %self.mesh, held_topology_version = held, members = full.member_count(), in_flight = full.in_flight().len(), deleted = full.deleted().len())
                        .in_scope(|| tracing::info!("this mesh's normalized projection moved since its last publication: it is published at once"));
                    return;
                }
            }
            let ok = tokio::select! {
                _ = &mut sleep => return,
                r = heard.changed() => r.is_ok(),
                r = births.changed() => r.is_ok(),
                r = overlays.changed() => r.is_ok(),
                // A primary record this view holds changed: the projection carries it (step 5).
                r = primaries.changed() => r.is_ok(),
            };
            if !ok {
                (&mut sleep).await;
                return;
            }
        }
    }

    /// Each peer mesh this primary holds (from the backbone, or a read of another mesh's node), put into
    /// its Mesh as the differential from what the Mesh holds (gossip.md §3.3): on taking the role. Of a
    /// mesh both hold, the higher version goes in.
    pub(crate) fn forward_held_now(&self) {
        let me_name = self.current_name();
        let mut held: std::collections::BTreeMap<String, (PublisherId, u64, Full)> = Default::default();
        let backbone = self.channels.topology_view.view.backbone_rx.lock().unwrap().sources();
        let read = self.channels.topology_view.view.mesh_rx.lock().unwrap().sources();
        for (mesh, publisher, version, full) in backbone.into_iter().chain(read).filter(|(m, ..)| m != &self.mesh) {
            if held.get(&mesh).is_none_or(|(_, v, _)| *v < version) {
                held.insert(mesh, (publisher, version, full));
            }
        }
        for (mesh, (publisher, version, full)) in held {
            self.channels.forward_path.source_with_clock(&me_name, &mesh, &publisher, version, &full, || self.channels.topology_view.clock.now_rafka_ms());
        }
    }

    /// Publish the status event `kind` of `op`'s birth, which this admin executed (R-HB1 "The version
    /// field": "Each status fact carries its subject's `topology_version`"; §A.2: "A status fact
    /// carries the `topology_version` at which its subject changed"). The event is applied to this
    /// admin's own view, then stamped with its publisher and the version at which the subject changed,
    /// then broadcast on its own mesh channel only (the backbone carries it as the overlay of its
    /// mesh's topology):
    /// - this mesh's primary, for its own mesh: the version its projection takes with the event
    ///   applied, observed and versioned under one hold, so its next publication carries exactly that
    ///   version, carrying the event;
    /// - the fabric-primary stopping a deleted mesh's birth (`Backbone::publishes_deleted_mesh_of`):
    ///   from the stop on it is that mesh's publisher of record (gossip.md "Mesh delete"), so the
    ///   version is the deleted mesh's topology's with the stop listed, and that mesh topology leaves on
    ///   the backbone at once, from this event (mesh-delete.md "Command, completion and gossip
    ///   messages": "F->>M: node.stop", then "F->>B: deleted mesh's topology: NodeStopped in
    ///   in_flight"), never waiting for the publication, which a delete applied right behind it would
    ///   find already deleted;
    /// - otherwise (the executor of a peer mesh's node, the fabric-primary among them; nobody
    ///   publishes a live mesh on its behalf, R-RX4): this admin's own publisher at the version it
    ///   holds, and the span names the role.
    ///
    /// Returns the publisher and version stamped.
    pub async fn publish_status_event(&self, kind: StatusEvent, op: &LifecycleOp) -> (PublisherId, u64) {
        let topology_view = &self.channels.topology_view;
        let me_name = self.current_name();
        let publisher = self.current_publisher();
        let (role, version) = if op.mesh == self.mesh && self.publishing.load(Ordering::Relaxed) {
            let mut v = self.version.lock().unwrap();
            self.channels.topology_view.view.apply_status_event(kind, op, None, "executed");
            let full = self.own_full(topology_view, self.own_members(topology_view));
            let version = v.observe(&full);
            self.channels.topology_view.view.order.lock().unwrap().published(&self.mesh, version);
            ("mesh-primary", version)
        } else {
            self.channels.topology_view.view.apply_status_event(kind, op, None, "executed");
            ("executor", self.version.lock().unwrap().version())
        };
        tracing::info_span!(
            "rdm.mesh.topology.update.via-status-event",
            node = %me_name,
            frame = kind.name(),
            subject_path = %op.path, subject_name = %op.name,
            subject_id = %op.node_id,
            incarnation_id = %op.incarnation.as_str(),
            build_id = %op.build_id,
            operation = %op.operation.action, operation_subject = ?op.operation.subject,
            publisher = %publisher,
            topology_version = version,
            role,
        )
        .in_scope(|| tracing::info!("a status event this admin executed: applied to its own view, stamped with the version at which its subject changed, broadcast on its mesh channel"));
        if let Err(e) = self.channels.mesh().broadcast(&kind.frame(op.clone(), publisher.clone(), version)).await {
            tracing::info!(error = %e, frame = kind.name(), "the status event was not sent on the mesh channel");
        }
        (publisher, version)
    }

    /// This admin was elected primary, or, as its mesh's primary, recorded another node type's
    /// primary (R-MT1 Q3): it holds the record. No frame announces it: its next `Digest` carries it,
    /// and the mesh-primary's `TopologyFull` projection.
    pub fn hold_primary(&self, role: PrimaryRole, primary: Primary) {
        self.channels.topology_view.view.note_primary(role, &primary, "own");
    }


    /// Join the backbone through every node-admin known, once each ([`Channels::join`] with
    /// [`ChannelId::backbone`](crate::channel::ChannelId::backbone) on a held backbone).
    pub(crate) async fn join_admins(&self, admins: Vec<EndpointAddr>) {
        if let Ok(n) = self.channel.join_peers(admins).await {
            if n > 0 {
                tracing::info_span!("rdm.mesh.connection.update.via-backbone-peers-joined", node = %self.current_name(), peers = n)
                    .in_scope(|| tracing::info!("backbone peers joined"));
            }
        }
    }
}
