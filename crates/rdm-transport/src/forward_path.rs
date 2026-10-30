//! The path a forwarding mesh primary's frames take onto its own mesh channel.
//!
//! ONE ordered sender carries every frame the primary puts on its mesh channel for the backbone:
//! the forwarder's decisions (the backbone receiver, the own-mesh publication, the role fulls) and the
//! lifecycle frames it carries in. A decision is made under the forwarder's lock and queued while
//! that lock is still held, so the queue's order is the decision order; a single task empties the
//! queue, finishing every frame of a decision (a chunked full is one decision) before it takes the
//! next. A receiver applies a delta only at exactly the base it holds for that source, and installs
//! a full only once it holds every chunk: what depends on a frame is the later frames of the SAME
//! source and publisher, and one task sending one decision at a time keeps them in order and
//! contiguous. The queue is unbounded and never drops: topology forwards are low-rate.
//!
//! Only the mesh's primary (holding the forwarder role) decides or carries anything. The role
//! and its term live under the forwarder's lock: taking or losing it clears the baseline and begins
//! a new term in one step, every decision reads the role under that same lock, and each queued
//! entry carries the term it was decided in. The sender drops an entry whose term is not the
//! current one: what a node decided as primary is never sent once it no longer is.
//!
//! The forwarder advances its baseline when it decides, before the send. A frame the channel
//! refuses, or a queue whose task is gone, sets `owes_full`: the next publication of the primary
//! puts the full of every source into the mesh (the role's mechanism), with no timer and no retry.

use crate::topology::Frame;
use crate::snapshot::{Forward, Forwarder, Full, PublisherId};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Where a forwarded frame goes: the mesh channel's broadcast.
pub(crate) type Sink = Arc<dyn Fn(Frame) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send + Sync>;

/// One entry of the queue, in the order it was made.
/// `revision` is the term of the forwarder role the entry was decided in.
enum Job {
    /// What the forwarder decided; `source` names the single source of a `source` decision.
    Forwarded { me: String, revision: u64, source: Option<(String, PublisherId, u64)>, out: Forward },
    /// A lifecycle frame this primary carries into its mesh.
    Carried { me: String, revision: u64, frame: Frame },
    /// A neighbour-up replay (statuses, primaries, and the forwarder's published fulls), decided and
    /// queued under the forwarder's lock so no delta decided after it can pass it on the channel.
    Replayed { me: String, revision: Option<u64>, frames: Vec<Frame> },
}

/// The receiver half of a [`ForwardPath::pair`], handed to [`ForwardPath::spawn_sender`].
pub(crate) struct ForwardRx(mpsc::UnboundedReceiver<Job>);

/// The forwarder, and the one queue its frames are sent from.
#[derive(Clone)]
pub(crate) struct ForwardPath {
    forwarder: Arc<Mutex<Forwarder>>,
    queue: mpsc::UnboundedSender<Job>,
    owes_full: Arc<AtomicBool>,
}

impl ForwardPath {
    /// The path over `forwarder`, sending to `sink` from one task that lives as long as any clone.
    /// `owes_full` is the flag whose set state makes the next publication a full of every source.
    #[cfg(test)]
    pub(crate) fn new(forwarder: Arc<Mutex<Forwarder>>, owes_full: Arc<AtomicBool>, sink: Sink) -> Self {
        forwarder.lock().unwrap().set_author(test_author());
        let (me, rx) = Self::pair(forwarder, owes_full);
        me.spawn_sender(rx, sink);
        me
    }

    /// Create the path and its receiver before the channel's sink is bound, so the channel's
    /// neighbour-up replay closure can queue onto it from the moment the channel subscribes.
    pub(crate) fn pair(forwarder: Arc<Mutex<Forwarder>>, owes_full: Arc<AtomicBool>) -> (Self, ForwardRx) {
        let (queue, rx) = mpsc::unbounded_channel();
        (Self { forwarder, queue, owes_full }, ForwardRx(rx))
    }

    /// Start the single ordered sender task emptying `rx` into `sink`.
    pub(crate) fn spawn_sender(&self, rx: ForwardRx, sink: Sink) {
        tokio::spawn(send_in_order(rx.0, sink, self.owes_full.clone(), self.forwarder.clone()));
    }

    /// Decide what `source_mesh` moving to `version` puts into the mesh, and queue it behind every
    /// frame queued before it.
    #[cfg(test)]
    pub(crate) fn source(&self, me: &str, source_mesh: &str, publisher: &PublisherId, version: u64, full: &Full, now_ms: u64) {
        let _ = self.source_with_clock(me, source_mesh, publisher, version, full, || now_ms);
    }

    /// Decide what `source_mesh` moving to `version` puts into the mesh, reading `now_ms` inside
    /// the forwarder lock so no replay or concurrent forward can interleave between the clock read
    /// and the forwarder's version update, and queue it behind every frame queued before it.
    pub(crate) fn source_with_clock(&self, me: &str, source_mesh: &str, publisher: &PublisherId, version: u64, full: &Full, now_ms: impl FnOnce() -> u64) -> u64 {
        let mut forwarder = self.forwarder.lock().unwrap();
        let now_ms = now_ms();
        let Some(revision) = forwarder.role() else { return now_ms };
        let out = forwarder.source(me, source_mesh, publisher, version, full, now_ms);
        if matches!(out, Forward::Nothing(_)) {
            return now_ms;
        }
        self.enqueue(Job::Forwarded { me: me.to_string(), revision, source: Some((source_mesh.to_string(), publisher.clone(), version)), out });
        now_ms
    }

    /// The first publication of every source in `held` (taking the role, or owing a full): one
    /// decision, queued behind every frame queued before it.
    #[cfg(test)]
    pub(crate) fn fulls(&self, me: &str, node_mesh: &str, reason: &'static str, held: &[(String, PublisherId, u64, Full)], now_ms: u64) {
        let _ = self.fulls_with_clock(me, node_mesh, reason, held, || now_ms);
    }

    /// [`Self::fulls`], reading `now_ms` inside the forwarder lock.
    pub(crate) fn fulls_with_clock(&self, me: &str, node_mesh: &str, reason: &'static str, held: &[(String, PublisherId, u64, Full)], now_ms: impl FnOnce() -> u64) -> u64 {
        let mut forwarder = self.forwarder.lock().unwrap();
        let now_ms = now_ms();
        let Some(revision) = forwarder.role() else { return now_ms };
        let frames = forwarder.fulls(me, held, now_ms);
        let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
        tracing::info_span!("rdm.mesh.topology.update.via-forwarded-full", node = %me, mesh = %node_mesh, reason, sources = held.len(), chunks = frames.len(), bytes)
            .in_scope(|| tracing::info!("the full of every source put into this mesh, loads omitted"));
        self.enqueue(Job::Forwarded { me: me.to_string(), revision, source: None, out: Forward::Full(frames) });
        now_ms
    }

    /// Take (`true`) or lose (`false`) the forwarder role: this mesh's primary. A forwarder that
    /// takes or loses the role has published nothing into its mesh: under the forwarder's lock its
    /// baseline is cleared, a new term begins, and the first publication of every source after
    /// taking it is owed as a full (gossip.md §3.3). Returns whether the role changed.
    pub(crate) fn set_role(&self, on: bool) -> bool {
        let mut forwarder = self.forwarder.lock().unwrap();
        let changed = forwarder.set_role(on);
        if changed {
            self.owes_full.store(on, Ordering::Relaxed);
        }
        changed
    }

    /// The flag whose set state makes the next publication a full of every source.
    pub(crate) fn owes_full(&self) -> Arc<AtomicBool> {
        self.owes_full.clone()
    }

    /// Carry a lifecycle frame into the mesh, behind every frame queued before it.
    pub(crate) fn carry(&self, me: &str, frame: Frame) {
        let forwarder = self.forwarder.lock().unwrap();
        let Some(revision) = forwarder.role() else { return };
        self.enqueue(Job::Carried { me: me.to_string(), revision, frame });
    }

    /// Queue a neighbour-up replay (statuses, primaries, and the forwarder's published fulls) under
    /// the forwarder's lock, behind every frame queued before it and ahead of any delta decided
    /// after it. Returns `(frames, full_chunks, bytes)`.
    pub(crate) fn replay(&self, me: &str, mut frames: Vec<Frame>, now_ms: impl FnOnce() -> u64) -> (usize, usize, usize) {
        let mut forwarder = self.forwarder.lock().unwrap();
        let now_ms = now_ms();
        let revision = forwarder.role();
        if revision.is_some() {
            frames.extend(forwarder.replay(me, now_ms));
        }
        if frames.is_empty() {
            return (0, 0, 0);
        }
        let frames: Vec<_> = frames.into_iter().map(|f| forwarder.author_frame(f, now_ms)).collect();
        let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
        let fulls = frames.iter().filter(|f| matches!(f.data(), Frame::MeshTopology { .. })).count();
        let len = frames.len();
        self.enqueue(Job::Replayed { me: me.to_string(), revision, frames });
        (len, fulls, bytes)
    }

    /// Queue `job`, called while the lock that decided it is held. A queue whose task is gone
    /// sends nothing; the span names it and the next publication is a full.
    fn enqueue(&self, job: Job) {
        if let Err(mpsc::error::SendError(job)) = self.queue.send(job) {
            let (me, frames) = match job {
                Job::Forwarded { me, out, .. } => (me, frames_of(out)),
                Job::Carried { me, frame, .. } => (me, vec![frame]),
                Job::Replayed { me, frames, .. } => (me, frames),
            };
            let cause = anyhow::anyhow!("the forward queue of node {me} has no sender task");
            for frame in &frames {
                refused(&self.owes_full, &me, frame, &cause);
            }
        }
    }
}

fn frames_of(out: Forward) -> Vec<Frame> {
    match out {
        Forward::Full(frames) => frames,
        Forward::Delta(frame) => vec![*frame],
        Forward::Nothing(_) => Vec::new(),
    }
}

/// The source a forward frame belongs to: mesh, publisher and the version it moves the source to.
fn identity(frame: &Frame) -> Option<(&str, &PublisherId, u64)> {
    match frame.data() {
        Frame::MeshTopology { mesh, publisher, topology_version, .. } => Some((mesh, publisher, *topology_version)),
        Frame::MeshTopologyDelta { mesh, source_publisher, topology_version, .. } => Some((mesh, source_publisher, *topology_version)),
        _ => None,
    }
}

/// The channel (or the queue) did not take `frame`. A frame of a source leaves the mesh without
/// something it may hold a base for: the next publication is a full. The span names what failed.
fn refused(owes_full: &AtomicBool, me: &str, frame: &Frame, cause: &anyhow::Error) {
    let cause = format!("{cause:#}");
    match identity(frame) {
        Some((mesh, publisher, version)) => {
            owes_full.store(true, Ordering::Relaxed);
            tracing::info_span!("rdm.mesh.topology.update.via-forward-send-failed", node = %me, mesh = %mesh, source_publisher = %publisher, topology_version = version, cause = %cause)
                .in_scope(|| tracing::warn!("a forward was not sent: the next publication puts the full of every source into the mesh"));
        }
        None => {
            tracing::info_span!("rdm.mesh.topology.update.via-forward-send-failed", node = %me, mesh = "", source_publisher = "", topology_version = 0u64, cause = %cause)
                .in_scope(|| tracing::warn!("a carried frame was not sent"));
        }
    }
}

/// The one sender: each entry in the order it was queued, every frame of it before the next. An
/// entry decided in a term of the forwarder role that is not the current one is dropped, named:
/// the node lost the role (or lost and retook it, owing a full) after deciding it.
async fn send_in_order(mut rx: mpsc::UnboundedReceiver<Job>, sink: Sink, owes_full: Arc<AtomicBool>, forwarder: Arc<Mutex<Forwarder>>) {
    while let Some(job) = rx.recv().await {
        let revision = match &job {
            Job::Carried { revision, .. } | Job::Forwarded { revision, .. } => Some(*revision),
            Job::Replayed { revision, .. } => *revision,
        };
        if let Some(revision) = revision {
            let current = forwarder.lock().unwrap().role();
            if current != Some(revision) {
                role_dropped(job, revision, current);
                continue;
            }
        }
        match job {
            Job::Carried { me, frame, .. } => {
                if let Err(cause) = sink(frame.clone()).await {
                    refused(&owes_full, &me, &frame, &cause);
                }
            }
            Job::Forwarded { me, source, out, .. } => send_forward(&sink, &owes_full, me, source, out).await,
            Job::Replayed { me, frames, .. } => send_replayed(&sink, &owes_full, me, frames).await,
        }
    }
}

/// An entry decided in term `revision` of the forwarder role reached the sender in `current` (none:
/// the node no longer holds the role). It is not sent; the span names what was dropped.
fn role_dropped(job: Job, revision: u64, current: Option<u64>) {
    let (me, kind, frames) = match job {
        Job::Forwarded { me, out, .. } => (me, "forwarded", frames_of(out)),
        Job::Carried { me, frame, .. } => (me, "carried", vec![frame]),
        Job::Replayed { me, frames, .. } => (me, "replayed", frames),
    };
    let sources: BTreeSet<String> = frames.iter().filter_map(|f| identity(f).map(|(mesh, publisher, version)| format!("{mesh}:{publisher}@{version}"))).collect();
    let current = current.map(|c| c.to_string()).unwrap_or_else(|| "none".to_string());
    tracing::info_span!(
        "rdm.mesh.topology.reject.via-forwarder-role-dropped",
        node = %me,
        kind = %kind,
        decided_in_revision = revision,
        current_revision = %current,
        frames = frames.len(),
        sources = %sources.into_iter().collect::<Vec<_>>().join(","),
    )
    .in_scope(|| tracing::info!("decided while this node was its mesh's primary, reached the sender after it lost that role: not sent"));
}

/// Put what a neighbour-up replay queued onto the mesh channel, in order.
async fn send_replayed(sink: &Sink, owes_full: &AtomicBool, me: String, frames: Vec<Frame>) {
    let mut refused_sources: BTreeSet<String> = BTreeSet::new();
    for frame in &frames {
        if let Some((mesh, ..)) = identity(frame) {
            if refused_sources.contains(mesh) {
                continue;
            }
            if let Err(cause) = sink(frame.clone()).await {
                refused_sources.insert(mesh.to_string());
                refused(owes_full, &me, frame, &cause);
            }
        } else if let Err(cause) = sink(frame.clone()).await {
            refused(owes_full, &me, frame, &cause);
        }
    }
}

/// Put what the forwarder decided onto the mesh channel, and name it.
async fn send_forward(sink: &Sink, owes_full: &AtomicBool, me: String, source: Option<(String, PublisherId, u64)>, out: Forward) {
    let frames: Vec<Frame> = match out {
        Forward::Full(frames) => {
            if let Some((mesh, publisher, version)) = &source {
                let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
                tracing::info_span!("rdm.mesh.topology.update.via-forwarded-full", node = %me, mesh = %mesh, source_publisher = %publisher, topology_version = *version, reason = "first-for-source", chunks = frames.len(), bytes)
                    .in_scope(|| tracing::info!("the first publication of this source into the mesh is a full, loads omitted"));
            }
            frames
        }
        Forward::Delta(frame) => {
            if let (Frame::MeshTopologyDelta { base_version, changed, removed, in_flight, deleted, .. }, Some((mesh, publisher, version))) = (frame.data(), &source) {
                tracing::info_span!(
                    "rdm.mesh.topology.update.via-forwarded-delta",
                    node = %me,
                    mesh = %mesh,
                    source_publisher = %publisher,
                    base_version = *base_version,
                    topology_version = *version,
                    changed = changed.len(),
                    removed = removed.len(),
                    in_flight = in_flight.len(),
                    deleted = deleted.len(),
                )
                .in_scope(|| tracing::info!("a source moved: one delta from the version last published into this mesh"));
            }
            vec![*frame]
        }
        Forward::Nothing(_) => return,
    };
    // A source one of whose frames was refused sends no more of this decision: a full missing a
    // chunk is never installed, and the next publication is a full.
    let mut refused_sources: BTreeSet<String> = BTreeSet::new();
    for frame in &frames {
        let Some((mesh, ..)) = identity(frame) else { continue };
        if refused_sources.contains(mesh) {
            continue;
        }
        if let Err(cause) = sink(frame.clone()).await {
            refused_sources.insert(mesh.to_string());
            refused(owes_full, &me, frame, &cause);
        }
    }
}

#[cfg(test)]
fn test_author() -> crate::envelope::Birth {
    crate::envelope::Birth { fabric: rdm_core::FabricId::parse("fab000000001").unwrap(), node: rdm_core::NodeId::parse("0123456789ab").unwrap(), incarnation: rdm_core::IncarnationId::parse("yrz49j303yh4").unwrap() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId};
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn digest(ordinal: u32, id: &NodeId, birth: &IncarnationId, seq: u64) -> MeshDigest {
        MeshDigest { held_hash: Default::default(),
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode {
                node_id: id.clone(),
                name: format!("mesh1.rpc.{ordinal}").parse().unwrap(),
                endpoint_id: EndpointId(format!("key{ordinal}")),
                transport_addr: format!("127.0.0.1:{}", 41_000 + ordinal).parse().unwrap(),
                incarnation: birth.clone(),
                supersedes: None,
                runtime: None,
            },
            status: MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            emitted_at_rafka_ms: seq,
            digest_seq: seq,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            data_dir: None, is_primary: None, is_fabric_primary: None
        }
    }

    /// The source mesh's members at `version`: one member whose digest moves with the version, so
    /// every version is a delta from the one before.
    fn source_at(ids: &[(NodeId, IncarnationId)], version: u64) -> Full {
        let mut d = digest(1, &ids[0].0, &ids[0].1, 1);
        d.status = if version % 2 == 0 { MemberStatus::Ready } else { MemberStatus::ConnectionsDeleting };
        d.node.transport_addr = format!("127.0.0.1:{}", 42_000 + version).parse().unwrap();
        Full::new(vec![d], Vec::new(), Vec::new())
    }

    fn publisher() -> PublisherId {
        PublisherId { node: "mesh2.admin.1".into(), incarnation: IncarnationId::mint(), node_id: rdm_core::NodeId::mint() }
    }

    /// Every frame the sink was handed, in the order it was handed it. The send of the first
    /// frame handed to it takes `first_takes`; a send in `fail` returns an error.
    #[derive(Clone, Default)]
    struct Recorder {
        seen: Arc<Mutex<Vec<(u64, u64)>>>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        first_takes: Duration,
        fail: Arc<Mutex<Vec<usize>>>,
        /// A carried `Digest` is recorded as `(1, 1)`.
        carried_marker: bool,
    }

    impl Recorder {
        fn sink(&self) -> Sink {
            let me = self.clone();
            Arc::new(move |f: Frame| {
                let me = me.clone();
                Box::pin(async move {
                    let n = me.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if n == 0 {
                        tokio::time::sleep(me.first_takes).await;
                    }
                    if me.fail.lock().unwrap().contains(&n) {
                        anyhow::bail!("the channel refused call {n}");
                    }
                    let key = match f.data() {
                        Frame::MeshTopologyDelta { base_version, topology_version, .. } => (*base_version, *topology_version),
                        Frame::MeshTopology { topology_version, .. } => (0, *topology_version),
                        Frame::Digest { .. } if me.carried_marker => (1, 1),
                        other => panic!("not a forward: {other:?}"),
                    };
                    me.seen.lock().unwrap().push(key);
                    Ok(())
                })
            })
        }

        async fn seen_after(&self, n: usize) -> Vec<(u64, u64)> {
            for _ in 0..200 {
                if self.seen.lock().unwrap().len() >= n {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            self.seen.lock().unwrap().clone()
        }
    }

    /// CONTRACT: forwards decided in order (a full at v10, then deltas v10 to v11 and v11 to v12)
    /// reach the mesh channel in the order they were decided, whatever the first send takes: a
    /// receiver applies a delta only at exactly its base, so v12 ahead of v11 drops v11.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn forwards_decided_in_order_reach_the_channel_in_that_order() {
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let rec = Recorder { first_takes: Duration::from_millis(100), ..Recorder::default() };
        let path = ForwardPath::new(Arc::default(), Arc::default(), rec.sink());
        path.set_role(true);
        let p = publisher();
        path.source("mesh2.admin.1", "mesh1", &p, 10, &source_at(&ids, 10), 1);
        path.source("mesh2.admin.1", "mesh1", &p, 11, &source_at(&ids, 11), 2);
        path.source("mesh2.admin.1", "mesh1", &p, 12, &source_at(&ids, 12), 3);
        assert_eq!(rec.seen_after(3).await, vec![(0, 10), (10, 11), (11, 12)]);
    }

    /// Every span opened while the capture is the thread's subscriber: its name and its fields.
    #[derive(Clone, Default)]
    struct Spans(Arc<Mutex<Vec<(String, std::collections::BTreeMap<String, String>)>>>);

    struct Fields<'a>(&'a mut std::collections::BTreeMap<String, String>);

    impl tracing::field::Visit for Fields<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Spans {
        fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _id: &tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
            let mut fields = Default::default();
            attrs.record(&mut Fields(&mut fields));
            self.0.lock().unwrap().push((attrs.metadata().name().to_string(), fields));
        }
    }

    impl Spans {
        async fn named(&self, name: &str) -> Vec<std::collections::BTreeMap<String, String>> {
            for _ in 0..200 {
                if self.0.lock().unwrap().iter().any(|(n, _)| n == name) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            self.0.lock().unwrap().iter().filter(|(n, _)| n == name).map(|(_, f)| f.clone()).collect()
        }
    }

    /// CONTRACT: a forward the mesh channel refuses leaves no silent gap. The refusal is named by a
    /// span carrying the node, mesh, source publisher, version and cause, and the primary owes the
    /// mesh a full: its next publication puts the full of every source into it.
    #[tokio::test]
    async fn a_refused_forward_is_named_and_the_next_publication_owes_a_full() {
        let spans = Spans::default();
        let _capture = crate::test_span_capture::install(spans.clone());
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        // The channel refuses the second send: the delta v10 to v11.
        let rec = Recorder { fail: Arc::new(Mutex::new(vec![1])), ..Recorder::default() };
        let owes = Arc::new(AtomicBool::new(false));
        let path = ForwardPath::new(Arc::default(), owes.clone(), rec.sink());
        path.set_role(true);
        owes.store(false, Ordering::Relaxed);
        let p = publisher();
        path.source("mesh2.admin.1", "mesh1", &p, 10, &source_at(&ids, 10), 1);
        assert_eq!(rec.seen_after(1).await, vec![(0, 10)]);
        assert!(!owes.load(Ordering::Relaxed), "a forward that was sent owes nothing");
        path.source("mesh2.admin.1", "mesh1", &p, 11, &source_at(&ids, 11), 2);

        let failed = spans.named("rdm.mesh.topology.update.via-forward-send-failed").await;
        assert_eq!(failed.len(), 1, "the refusal is named once");
        let f = &failed[0];
        assert_eq!(f["node"], "mesh2.admin.1");
        assert_eq!(f["mesh"], "mesh1");
        assert_eq!(f["source_publisher"], p.to_string());
        assert_eq!(f["topology_version"], "11");
        assert!(f["cause"].contains("the channel refused call 1"), "the cause is the channel's own: {}", f["cause"]);
        assert!(owes.load(Ordering::Relaxed), "the next publication owes the mesh a full");
    }

    /// CONTRACT: a queue whose sender task is gone is named, never silent, and owes a full.
    #[tokio::test]
    async fn a_queue_with_no_sender_task_is_named_and_owes_a_full() {
        let spans = Spans::default();
        let _capture = crate::test_span_capture::install(spans.clone());
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let owes = Arc::new(AtomicBool::new(false));
        let (queue, rx) = mpsc::unbounded_channel();
        drop(rx);
        let forwarder = Arc::new(Mutex::new(Forwarder::default()));
        forwarder.lock().unwrap().set_author(test_author());
        let path = ForwardPath { forwarder, queue, owes_full: owes.clone() };
        path.forwarder.lock().unwrap().set_role(true);
        let p = publisher();
        path.source("mesh2.admin.1", "mesh1", &p, 10, &source_at(&ids, 10), 1);
        let failed = spans.named("rdm.mesh.topology.update.via-forward-send-failed").await;
        assert_eq!(failed.len(), 1);
        assert!(failed[0]["cause"].contains("no sender task"), "{}", failed[0]["cause"]);
        assert!(owes.load(Ordering::Relaxed));
    }

    /// A sink whose first send blocks until the test lets it go; every frame it is handed is
    /// recorded once the send completes.
    #[derive(Clone)]
    struct Gate {
        seen: Arc<Mutex<Vec<(u64, u64)>>>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Semaphore>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Gate {
        fn new() -> Self {
            Self { seen: Arc::default(), entered: Arc::default(), release: Arc::new(tokio::sync::Semaphore::new(0)), calls: Arc::default() }
        }

        fn sink(&self) -> Sink {
            let me = self.clone();
            Arc::new(move |f: Frame| {
                let me = me.clone();
                Box::pin(async move {
                    if me.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        me.entered.notify_one();
                        me.release.acquire().await.unwrap().forget();
                    }
                    let key = match f.data() {
                        Frame::MeshTopologyDelta { base_version, topology_version, .. } => (*base_version, *topology_version),
                        Frame::MeshTopology { topology_version, .. } => (0, *topology_version),
                        Frame::Digest { .. } => (1, 1),
                        other => panic!("not a forward: {other:?}"),
                    };
                    me.seen.lock().unwrap().push(key);
                    Ok(())
                })
            })
        }
    }

    /// CONTRACT: a forward decided while this node was the primary and still queued when it loses
    /// the role is never sent: the role dropped between the decision and the send, and a node that
    /// is no longer its mesh's primary puts nothing into the mesh. The full v10 is on the channel
    /// before the role drops; the delta v10 to v11 and a carried frame queued behind it are not.
    /// Taking the role again sends the full of the new term, behind nothing of the old one.
    #[tokio::test]
    async fn a_forward_queued_before_the_role_dropped_is_never_sent() {
        let spans = Spans::default();
        let _capture = crate::test_span_capture::install(spans.clone());
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let gate = Gate::new();
        let path = ForwardPath::new(Arc::default(), Arc::default(), gate.sink());
        let p = publisher();
        path.set_role(true);
        path.source("mesh2.admin.1", "mesh1", &p, 10, &source_at(&ids, 10), 1);
        gate.entered.notified().await;
        path.source("mesh2.admin.1", "mesh1", &p, 11, &source_at(&ids, 11), 2);
        path.carry("mesh2.admin.1", Frame::Digest { digest: digest(1, &ids[0].0, &ids[0].1, 1) });
        path.set_role(false);
        gate.release.add_permits(1);
        path.set_role(true);
        path.fulls("mesh2.admin.1", "mesh2", "full-due", &[("mesh1".to_string(), p.clone(), 12, source_at(&ids, 12))], 3);
        for _ in 0..400 {
            if gate.seen.lock().unwrap().contains(&(0, 12)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(*gate.seen.lock().unwrap(), vec![(0, 10), (0, 12)], "nothing queued before the role dropped reaches the channel");
        let dropped = spans.named("rdm.mesh.topology.reject.via-forwarder-role-dropped").await;
        let named: Vec<(String, String, String, String)> = dropped.iter().map(|f| (f["kind"].clone(), f["decided_in_revision"].clone(), f["current_revision"].clone(), f["sources"].clone())).collect();
        assert_eq!(
            named,
            vec![("forwarded".to_string(), "1".to_string(), "3".to_string(), format!("mesh1:{p}@11")), ("carried".to_string(), "1".to_string(), "3".to_string(), String::new())],
            "each dropped entry is named with the term it was decided in and the term that dropped it"
        );
    }

    /// CONTRACT: a node that does not hold the role decides nothing: a source moving, a role full
    /// and a carried frame put nothing into the mesh, and leave no baseline behind.
    #[tokio::test]
    async fn a_node_without_the_role_forwards_nothing() {
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let gate = Gate::new();
        gate.release.add_permits(1);
        let path = ForwardPath::new(Arc::default(), Arc::default(), gate.sink());
        let p = publisher();
        path.source("mesh2.admin.1", "mesh1", &p, 10, &source_at(&ids, 10), 1);
        path.fulls("mesh2.admin.1", "mesh2", "full-due", &[("mesh1".to_string(), p.clone(), 10, source_at(&ids, 10))], 1);
        path.carry("mesh2.admin.1", Frame::Digest { digest: digest(1, &ids[0].0, &ids[0].1, 1) });
        assert!(path.forwarder.lock().unwrap().snapshots().is_empty(), "no baseline is recorded without the role");
        path.set_role(true);
        path.source("mesh2.admin.1", "mesh1", &p, 11, &source_at(&ids, 11), 2);
        for _ in 0..400 {
            if !gate.seen.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(*gate.seen.lock().unwrap(), vec![(0, 11)], "the first publication after taking the role is a full, and nothing before it was sent");
    }

    /// CONTRACT: every producer shares the one order: a role full, a carried lifecycle frame, a
    /// neighbour-up replay and a delta reach the channel in the order they were queued, whatever
    /// the first send takes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_producer_shares_one_order() {
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let rec = Recorder { first_takes: Duration::from_millis(100), carried_marker: true, ..Recorder::default() };
        let path = ForwardPath::new(Arc::default(), Arc::default(), rec.sink());
        path.set_role(true);
        let p = publisher();
        path.fulls("mesh2.admin.1", "mesh2", "full-due", &[("mesh1".to_string(), p.clone(), 10, source_at(&ids, 10))], 1);
        path.carry("mesh2.admin.1", Frame::Digest { digest: digest(1, &ids[0].0, &ids[0].1, 1) });
        let (replayed, fulls, _) = path.replay("mesh2.admin.1", Vec::new(), || 2);
        assert_eq!((replayed, fulls), (1, 1));
        path.source("mesh2.admin.1", "mesh1", &p, 11, &source_at(&ids, 11), 3);
        assert_eq!(rec.seen_after(4).await, vec![(0, 10), (1, 1), (0, 10), (10, 11)]);
    }

    /// CONTRACT: a chunked full stays contiguous on the channel: the chunks of one decision are
    /// all sent before the next decision's frames, even while the first chunk's send is slow, and
    /// a source decided between two others keeps its place.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_chunked_full_is_sent_whole_before_the_decision_after_it() {
        let members: Vec<(NodeId, IncarnationId)> = (0..150).map(|_| (NodeId::mint(), IncarnationId::mint())).collect();
        let big = |version: u64| {
            let mut digests: Vec<MeshDigest> = members.iter().enumerate().map(|(k, (id, b))| digest(k as u32 + 1, id, b, 1)).collect();
            if version > 10 {
                digests[0].status = MemberStatus::ConnectionsDeleting;
            }
            Full::new(digests, Vec::new(), Vec::new())
        };
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let rec = Recorder { first_takes: Duration::from_millis(100), ..Recorder::default() };
        let path = ForwardPath::new(Arc::default(), Arc::default(), rec.sink());
        path.set_role(true);
        let (p, q) = (publisher(), publisher());
        let chunks = crate::snapshot::chunks_of(&big(10), |d, i, dep, ci, cc| Frame::MeshTopology { mesh: "mesh1".into(), publisher: p.clone(), forwarded_by: Some("mesh2.admin.1".into()), topology_version: 10, published_at_rafka_ms: 1, snapshot_id: 1, chunk_index: ci, chunk_count: cc, digests: d, in_flight: i, deleted: dep }.authored(&test_author(), 0, 1)).len();
        assert!(chunks > 1, "the full spans {chunks} chunks");
        path.source("mesh2.admin.1", "mesh1", &p, 10, &big(10), 1);
        path.source("mesh2.admin.1", "mesh3", &q, 5, &source_at(&ids, 5), 2);
        path.source("mesh2.admin.1", "mesh1", &p, 11, &big(11), 3);
        let seen = rec.seen_after(chunks + 1 + 1).await;
        let mut expected: Vec<(u64, u64)> = vec![(0, 10); chunks];
        expected.push((0, 5));
        expected.push((10, 11));
        assert_eq!(seen, expected);
    }
}
