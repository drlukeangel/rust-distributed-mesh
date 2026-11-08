//! The path a forwarding mesh primary's frames take onto its own mesh channel.
//!
//! ONE ordered sender carries every frame the primary puts on its mesh channel for the backbone:
//! the forwarder's differentials of the peer meshes it holds and the lifecycle frames it carries in.
//! A decision is made under the forwarder's lock and queued while that lock is still held, so the
//! queue's order is the decision order; a single task empties the queue, one decision at a time. A
//! receiver applies a delta only at exactly the base it holds for that mesh: what depends on a
//! frame is the later frames of the SAME mesh, and one task sending them in order keeps them in
//! order. The queue is unbounded and never drops: topology forwards are low-rate.
//!
//! Only the mesh's primary (holding the forwarder role) decides or carries anything. The role
//! and its term live under the forwarder's lock: taking or losing it forgets the term's forwards and
//! begins a new term in one step, every decision reads the role under that same lock, and each queued
//! entry carries the term it was decided in. The sender drops an entry whose term is not the
//! current one: what a node decided as primary is never sent once it no longer is.
//!
//! What the forwarder decides from is the delta a complete full wrote to the one copy in the topology
//! view (Q409): it holds no copy of its own. It advances the version it forwarded when it decides,
//! before the send. A frame the channel
//! refuses, or a queue whose task is gone, is named by its span and nothing else: a node that missed
//! it is behind the next differential's base and repairs by a read (gossip.md §3.3), with no full,
//! no timer and no retry. A differential in chunks is sent chunk by chunk; once the channel refuses
//! one, the rest of it is not sent, since no receiver can apply it without every chunk.

use crate::frame::Frame;
use crate::aggregate::{Forward, Forwarder, Full, Written};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Where a forwarded frame goes: the mesh channel's broadcast.
pub(crate) type Sink = Arc<dyn Fn(Frame) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send + Sync>;

/// One entry of the queue, in the order it was made.
/// `revision` is the term of the forwarder role the entry was decided in.
enum Job {
    /// What the forwarder decided; `source` names the peer mesh and version of a `source` decision.
    Forwarded { me: String, revision: u64, source: Option<(String, u64)>, out: Forward },
    /// A lifecycle frame this primary carries into its mesh.
    Carried { me: String, revision: u64, frame: Frame },
    /// A neighbour-up replay (statuses, primaries, and the forwarder's last differential of each peer
    /// mesh), decided and queued under the forwarder's lock so no delta decided after it can pass it
    /// on the channel.
    Replayed { me: String, revision: Option<u64>, frames: Vec<Frame> },
}

/// The receiver half of a [`ForwardPath::pair`], handed to [`ForwardPath::spawn_sender`].
pub(crate) struct ForwardRx(mpsc::UnboundedReceiver<Job>);

/// The forwarder, and the one queue its frames are sent from.
#[derive(Clone)]
pub(crate) struct ForwardPath {
    forwarder: Arc<Mutex<Forwarder>>,
    queue: mpsc::UnboundedSender<Job>,
}

impl ForwardPath {
    /// The path over `forwarder`, sending to `sink` from one task that lives as long as any clone.
    #[cfg(test)]
    pub(crate) fn new(forwarder: Arc<Mutex<Forwarder>>, sink: Sink) -> Self {
        forwarder.lock().unwrap().set_author(test_author());
        let (me, rx) = Self::pair(forwarder);
        me.spawn_sender(rx, sink);
        me
    }

    /// Create the path and its receiver before the channel's sink is bound, so the channel's
    /// neighbour-up replay closure can queue onto it from the moment the channel subscribes.
    pub(crate) fn pair(forwarder: Arc<Mutex<Forwarder>>) -> (Self, ForwardRx) {
        let (queue, rx) = mpsc::unbounded_channel();
        (Self { forwarder, queue }, ForwardRx(rx))
    }

    /// Start the single ordered sender task emptying `rx` into `sink`.
    pub(crate) fn spawn_sender(&self, rx: ForwardRx, sink: Sink) {
        tokio::spawn(send_in_order(rx.0, sink, self.forwarder.clone()));
    }

    /// Decide what the full `written` to the topology view puts into the mesh, and queue it behind
    /// every frame queued before it.
    #[cfg(test)]
    pub(crate) fn source(&self, me: &str, written: &Written, now_ms: u64) {
        let _ = self.source_with_clock(me, written, || now_ms);
    }

    /// Decide what the full `written` to the topology view puts into the mesh (the delta it wrote,
    /// from the version this Mesh holds), reading `now_ms` inside the forwarder lock so no replay or
    /// concurrent forward can interleave between the clock read and the forwarder's version update,
    /// and queue it behind every frame queued before it.
    pub(crate) fn source_with_clock(&self, me: &str, written: &Written, now_ms: impl FnOnce() -> u64) -> u64 {
        let mut forwarder = self.forwarder.lock().unwrap();
        let now_ms = now_ms();
        let Some(revision) = forwarder.role() else { return now_ms };
        let out = forwarder.source(me, written, now_ms);
        if matches!(out, Forward::Nothing(_)) {
            return now_ms;
        }
        self.enqueue(Job::Forwarded { me: me.to_string(), revision, source: Some((written.mesh.clone(), written.topology_version)), out });
        now_ms
    }

    /// Hold this primary's publication of its own mesh at `version` for a topology read of it: its own
    /// nodes publish themselves, so nothing goes on its mesh channel (gossip.md §3.3).
    pub(crate) fn hold_own(&self, version: u64, full: &Full) {
        self.forwarder.lock().unwrap().hold_own(version, full);
    }

    /// Take (`true`) or lose (`false`) the forwarder role: this mesh's primary. Under the forwarder's
    /// lock the term's forwards are forgotten and a new term begins; taking it, the node forwards from
    /// the copy it holds of each peer mesh (gossip.md §3.3, Q409). Returns whether the role changed.
    pub(crate) fn set_role(&self, on: bool) -> bool {
        self.forwarder.lock().unwrap().set_role(on)
    }

    /// Carry a lifecycle frame into the mesh, behind every frame queued before it.
    pub(crate) fn carry(&self, me: &str, frame: Frame) {
        let forwarder = self.forwarder.lock().unwrap();
        let Some(revision) = forwarder.role() else { return };
        self.enqueue(Job::Carried { me: me.to_string(), revision, frame });
    }

    /// Queue a neighbour-up replay (statuses, primaries, and the forwarder's last differential of each
    /// peer mesh) under the forwarder's lock, behind every frame queued before it and ahead of any delta
    /// decided after it. Returns `(frames, deltas, bytes)`.
    pub(crate) fn replay(&self, me: &str, mut frames: Vec<Frame>, now_ms: impl FnOnce() -> u64) -> (usize, usize, usize) {
        let mut forwarder = self.forwarder.lock().unwrap();
        let now_ms = now_ms();
        let revision = forwarder.role();
        if revision.is_some() {
            frames.extend(forwarder.replay(now_ms));
        }
        if frames.is_empty() {
            return (0, 0, 0);
        }
        let frames: Vec<_> = frames.into_iter().map(|f| forwarder.author_frame(f, now_ms)).collect();
        let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
        let deltas = frames.iter().filter(|f| matches!(f.data(), Frame::TopologyDelta { .. })).count();
        let len = frames.len();
        self.enqueue(Job::Replayed { me: me.to_string(), revision, frames });
        (len, deltas, bytes)
    }

    /// Queue `job`, called while the lock that decided it is held. A queue whose task is gone
    /// sends nothing; the span names it.
    fn enqueue(&self, job: Job) {
        if let Err(mpsc::error::SendError(job)) = self.queue.send(job) {
            let (me, frames) = match job {
                Job::Forwarded { me, out, .. } => (me, frames_of(out)),
                Job::Carried { me, frame, .. } => (me, vec![frame]),
                Job::Replayed { me, frames, .. } => (me, frames),
            };
            let cause = anyhow::anyhow!("the forward queue of node {me} has no sender task");
            for frame in &frames {
                refused(&me, frame, &cause);
            }
        }
    }
}

fn frames_of(out: Forward) -> Vec<Frame> {
    match out {
        Forward::Delta(frames) => frames,
        Forward::Nothing(_) => Vec::new(),
    }
}

/// The source a forward frame belongs to: the mesh and the version it moves the mesh to.
fn identity(frame: &Frame) -> Option<(&str, u64)> {
    match frame.data() {
        Frame::TopologyDelta { mesh, .. } => Some((mesh, frame.delta_version().unwrap_or_default())),
        _ => None,
    }
}

/// The channel (or the queue) did not take `frame`. The span names what failed; a node that missed
/// a differential is behind the next one's base and repairs by a read.
fn refused(me: &str, frame: &Frame, cause: &anyhow::Error) {
    let cause = format!("{cause:#}");
    match identity(frame) {
        Some((mesh, version)) => {
            tracing::info_span!("rdm.mesh.topology.update.via-forward-send-failed", node = %me, mesh = %mesh, topology_version = version, cause = %cause)
                .in_scope(|| tracing::warn!("a forward was not sent: a node behind it repairs by a read at the next differential"));
        }
        None => {
            tracing::info_span!("rdm.mesh.topology.update.via-forward-send-failed", node = %me, mesh = "", topology_version = 0u64, cause = %cause)
                .in_scope(|| tracing::warn!("a carried frame was not sent"));
        }
    }
}

/// The one sender: each entry in the order it was queued, every frame of it before the next. An
/// entry decided in a term of the forwarder role that is not the current one is dropped, named:
/// the node lost the role (or lost and retook it) after deciding it.
async fn send_in_order(mut rx: mpsc::UnboundedReceiver<Job>, sink: Sink, forwarder: Arc<Mutex<Forwarder>>) {
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
                    refused(&me, &frame, &cause);
                }
            }
            Job::Forwarded { me, source, out, .. } => send_forward(&sink, me, source, out).await,
            Job::Replayed { me, frames, .. } => send_replayed(&sink, me, frames).await,
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
    let sources: BTreeSet<String> = frames.iter().filter_map(|f| identity(f).map(|(mesh, version)| format!("{mesh}@{version}"))).collect();
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
async fn send_replayed(sink: &Sink, me: String, frames: Vec<Frame>) {
    let mut refused_sources: BTreeSet<String> = BTreeSet::new();
    for frame in &frames {
        if let Some((mesh, ..)) = identity(frame) {
            if refused_sources.contains(mesh) {
                continue;
            }
            if let Err(cause) = sink(frame.clone()).await {
                refused_sources.insert(mesh.to_string());
                refused(&me, frame, &cause);
            }
        } else if let Err(cause) = sink(frame.clone()).await {
            refused(&me, frame, &cause);
        }
    }
}

/// Put what the forwarder decided onto the mesh channel, and name it: the differential's chunks in
/// order, none after the first the channel refuses.
async fn send_forward(sink: &Sink, me: String, source: Option<(String, u64)>, out: Forward) {
    let frames: Vec<Frame> = match out {
        Forward::Delta(frames) => frames,
        Forward::Nothing(_) => return,
    };
    if let (Some(Frame::TopologyDelta { base_version, chunk_count, .. }), Some((mesh, version))) = (frames.first().map(Frame::data), &source) {
        let count = |f: &Frame| match f.data() {
            Frame::TopologyDelta { added, changed, removed, .. } => [added.len(), changed.len(), removed.len()],
            _ => [0; 3],
        };
        let [added, changed, removed] = frames.iter().map(count).fold([0; 3], |a, b| std::array::from_fn(|k| a[k] + b[k]));
        tracing::info_span!(
            "rdm.mesh.topology.update.via-forwarded-delta",
            node = %me,
            mesh = %mesh,
            base_version = *base_version,
            topology_version = *version,
            chunk_count = *chunk_count,
            added,
            changed,
            removed,
        )
        .in_scope(|| tracing::info!("a peer mesh moved: one differential from the version last published into this mesh"));
    }
    for frame in &frames {
        if let Err(cause) = sink(frame.clone()).await {
            refused(&me, frame, &cause);
            return;
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
    use crate::frame::MAX_FRAME;
    use rdm_core::{EndpointId, FabricId, IncarnationId, model::NodeStatus, MeshDigest, MeshNode, NodeId};
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn digest(ordinal: u32, id: &NodeId, birth: &IncarnationId, seq: u64) -> MeshDigest {
        MeshDigest { 
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode { mesh: "mesh1".into(), r#type: "node".into(),
                node_id: id.clone(),
                path: "mesh1.node".into(), name: (ordinal.to_string()).to_string(),
                endpoint_id: EndpointId(format!("key{ordinal}")),
                transport_addr: format!("127.0.0.1:{}", 41_000 + ordinal).parse().unwrap(),
                incarnation: birth.clone(),
                supersedes: None,
            },
            status: NodeStatus::Ready,
            emitted_at_rafka_ms: seq,
            digest_seq: seq,
            primary: Vec::new(),
        }
    }

    /// The source mesh's members at `version`: one member whose digest moves with the version, so
    /// every version is a delta from the one before.
    fn source_at(ids: &[(NodeId, IncarnationId)], version: u64) -> Full {
        let mut d = digest(1, &ids[0].0, &ids[0].1, 1);
        d.status = if version % 2 == 0 { NodeStatus::Ready } else { NodeStatus::ConnectionsDeleting };
        d.node.transport_addr = format!("127.0.0.1:{}", 42_000 + version).parse().unwrap();
        Full::new(vec![d])
    }

    /// The node-admin's one copy of each peer mesh in its topology view (Q409): what a complete full
    /// writes, and so what its forwarder forwards.
    #[derive(Default)]
    struct Copies {
        held: crate::aggregate::AggregateReceiver,
        published: u64,
    }

    impl Copies {
        /// `mesh`'s complete full at `version`, a new publication of it, written to the copy: its
        /// delta from the version held.
        fn write(&mut self, mesh: &str, version: u64, full: &Full) -> Written {
            self.published += 1;
            let chunk = crate::aggregate::Chunk { mesh: mesh.into(), mesh_id: None, author: Some(test_author()), topology_version: version, time_ms: self.published, chunk_index: 0, chunk_count: 1, digests: full.digests(), runtimes: Vec::new() };
            match self.held.take_chunk(chunk) {
                crate::aggregate::Taken::Written(w) => *w,
                other => panic!("{mesh}'s full at {version} is written: {other:?}"),
            }
        }
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
                        Frame::TopologyDelta { base_version, .. } => (*base_version, f.delta_version().expect("an authored differential")),
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

    /// CONTRACT: forwards decided in order (the first differential, from 0 to v10, then v10 to v11
    /// and v11 to v12) reach the mesh channel in the order they were decided, whatever the first
    /// send takes: a receiver applies a delta only at exactly its base, so v12 ahead of v11 drops v11.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn forwards_decided_in_order_reach_the_channel_in_that_order() {
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let rec = Recorder { first_takes: Duration::from_millis(100), ..Recorder::default() };
        let path = ForwardPath::new(Arc::default(), rec.sink());
        let mut copies = Copies::default();
        path.set_role(true);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 10, &source_at(&ids, 10)), 1);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 11, &source_at(&ids, 11)), 2);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 12, &source_at(&ids, 12)), 3);
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
    /// span carrying the node, mesh, version and cause; nothing full follows it.
    /// The baseline moved: the next differential is from v11, so a node that missed v11 is behind its
    /// base and repairs by a read (gossip.md §3.3).
    #[tokio::test]
    async fn a_refused_forward_is_named_and_the_next_differential_is_from_its_version() {
        let spans = Spans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        // The channel refuses the second send: the delta v10 to v11.
        let rec = Recorder { fail: Arc::new(Mutex::new(vec![1])), ..Recorder::default() };
        let path = ForwardPath::new(Arc::default(), rec.sink());
        let mut copies = Copies::default();
        path.set_role(true);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 10, &source_at(&ids, 10)), 1);
        assert_eq!(rec.seen_after(1).await, vec![(0, 10)]);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 11, &source_at(&ids, 11)), 2);

        let failed = spans.named("rdm.mesh.topology.update.via-forward-send-failed").await;
        assert_eq!(failed.len(), 1, "the refusal is named once");
        let f = &failed[0];
        assert_eq!(f["node"], "mesh2.node-admin.1");
        assert_eq!(f["mesh"], "mesh1");
        assert_eq!(f["topology_version"], "11");
        assert!(f["cause"].contains("the channel refused call 1"), "the cause is the channel's own: {}", f["cause"]);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 12, &source_at(&ids, 12)), 3);
        assert_eq!(rec.seen_after(2).await, vec![(0, 10), (11, 12)], "nothing full follows a refusal; the next differential is from the version decided");
    }

    /// CONTRACT: a queue whose sender task is gone is named, never silent.
    #[tokio::test]
    async fn a_queue_with_no_sender_task_is_named() {
        let spans = Spans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let (queue, rx) = mpsc::unbounded_channel();
        drop(rx);
        let forwarder = Arc::new(Mutex::new(Forwarder::default()));
        forwarder.lock().unwrap().set_author(test_author());
        let path = ForwardPath { forwarder, queue };
        path.forwarder.lock().unwrap().set_role(true);
        path.source("mesh2.node-admin.1", &Copies::default().write("mesh1", 10, &source_at(&ids, 10)), 1);
        let failed = spans.named("rdm.mesh.topology.update.via-forward-send-failed").await;
        assert_eq!(failed.len(), 1);
        assert!(failed[0]["cause"].contains("no sender task"), "{}", failed[0]["cause"]);
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
                        Frame::TopologyDelta { base_version, .. } => (*base_version, f.delta_version().expect("an authored differential")),
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
    /// is no longer its mesh's primary puts nothing into the mesh. The differential to v10 is on the
    /// channel before the role drops; the delta v10 to v11 and a carried frame queued behind it are
    /// not. Taking the role again, the new term forwards from the copy it holds (v11, Q409) and its
    /// next differential is from there, behind nothing of the old term: a node of the mesh at v10 is
    /// behind its base and repairs by a read (gossip.md §3.3).
    #[tokio::test]
    async fn a_forward_queued_before_the_role_dropped_is_never_sent() {
        let spans = Spans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let gate = Gate::new();
        let path = ForwardPath::new(Arc::default(), gate.sink());
        let mut copies = Copies::default();
        path.set_role(true);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 10, &source_at(&ids, 10)), 1);
        gate.entered.notified().await;
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 11, &source_at(&ids, 11)), 2);
        path.carry("mesh2.node-admin.1", Frame::digest(&digest(1, &ids[0].0, &ids[0].1, 1)));
        path.set_role(false);
        gate.release.add_permits(1);
        path.set_role(true);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 12, &source_at(&ids, 12)), 3);
        for _ in 0..400 {
            if gate.seen.lock().unwrap().contains(&(11, 12)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(*gate.seen.lock().unwrap(), vec![(0, 10), (11, 12)], "nothing queued before the role dropped reaches the channel; the new term forwards from the copy held");
        let dropped = spans.named("rdm.mesh.topology.reject.via-forwarder-role-dropped").await;
        let named: Vec<(String, String, String, String)> = dropped.iter().map(|f| (f["kind"].clone(), f["decided_in_revision"].clone(), f["current_revision"].clone(), f["sources"].clone())).collect();
        assert_eq!(
            named,
            vec![("forwarded".to_string(), "1".to_string(), "3".to_string(), "mesh1@11".to_string()), ("carried".to_string(), "1".to_string(), "3".to_string(), String::new())],
            "each dropped entry is named with the term it was decided in and the term that dropped it"
        );
    }

    /// CONTRACT: a node that does not hold the role decides nothing: a source moving and a carried
    /// frame put nothing into the mesh, and leave nothing forwarded behind. Its copy still holds what
    /// the full wrote (v10): taking the role, it forwards the next move from there (Q409).
    #[tokio::test]
    async fn a_node_without_the_role_forwards_nothing() {
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let gate = Gate::new();
        gate.release.add_permits(1);
        let path = ForwardPath::new(Arc::default(), gate.sink());
        let mut copies = Copies::default();
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 10, &source_at(&ids, 10)), 1);
        path.carry("mesh2.node-admin.1", Frame::digest(&digest(1, &ids[0].0, &ids[0].1, 1)));
        assert_eq!(path.forwarder.lock().unwrap().forwarded_version("mesh1"), None, "nothing is forwarded without the role");
        path.set_role(true);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 11, &source_at(&ids, 11)), 2);
        for _ in 0..400 {
            if !gate.seen.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(*gate.seen.lock().unwrap(), vec![(10, 11)], "the first differential after taking the role is from the copy held, and nothing before it was sent");
    }

    /// CONTRACT: every producer shares the one order: a differential, a carried lifecycle frame, a
    /// neighbour-up replay (the last differential again) and the next differential reach the channel
    /// in the order they were queued, whatever the first send takes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_producer_shares_one_order() {
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let rec = Recorder { first_takes: Duration::from_millis(100), carried_marker: true, ..Recorder::default() };
        let path = ForwardPath::new(Arc::default(), rec.sink());
        let mut copies = Copies::default();
        path.set_role(true);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 10, &source_at(&ids, 10)), 1);
        path.carry("mesh2.node-admin.1", Frame::digest(&digest(1, &ids[0].0, &ids[0].1, 1)));
        let (replayed, deltas, _) = path.replay("mesh2.node-admin.1", Vec::new(), || 2);
        assert_eq!((replayed, deltas), (1, 1), "the replay is the last differential of mesh1");
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 11, &source_at(&ids, 11)), 3);
        assert_eq!(rec.seen_after(4).await, vec![(0, 10), (1, 1), (0, 10), (10, 11)]);
    }

    /// CONTRACT (gossip.md §3.3, rule 26, Q409): a node-admin taking the role forwards from the copy it
    /// holds of each peer mesh (the version it holds from the backbone, which its mesh's nodes reached
    /// from the forwarder before it), and the peer mesh's next move goes in as the delta its next full
    /// writes to that copy, its base the version held, whichever mesh-primary of the peer mesh sent it
    /// (Q389). A republication of the version held sends nothing; nothing full is sent, and nothing is
    /// topped up.
    #[tokio::test]
    async fn a_node_admin_taking_the_role_forwards_from_the_copy_it_holds() {
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let gate = Gate::new();
        gate.release.add_permits(1);
        let path = ForwardPath::new(Arc::default(), gate.sink());
        let mut copies = Copies::default();
        let _ = copies.write("mesh1", 7, &source_at(&ids, 7));
        path.set_role(true);
        path.source("mesh2.node-admin.2", &copies.write("mesh1", 7, &source_at(&ids, 7)), 1);
        path.source("mesh2.node-admin.2", &copies.write("mesh1", 9, &source_at(&ids, 9)), 2);
        for _ in 0..400 {
            if !gate.seen.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(*gate.seen.lock().unwrap(), vec![(7, 9)], "the held version is not sent again; the move is one differential from it");
    }

    /// mesh1's `n` members at `version`: from v11 the first is connections-deleting.
    fn members_at(members: &[(NodeId, IncarnationId)], version: u64) -> Full {
        let mut digests: Vec<MeshDigest> = members.iter().enumerate().map(|(k, (id, b))| digest(k as u32 + 1, id, b, 1)).collect();
        if version > 10 {
            digests[0].status = NodeStatus::ConnectionsDeleting;
        }
        Full::new(digests)
    }

    /// CONTRACT (gossip.md §Size: "a differential that does not fit one message is sent in chunks"):
    /// a differential of 400 members, too large for one message, reaches the mesh channel as all of
    /// its chunks, in order, before anything decided after it; its span names the chunk count; nothing
    /// is named too large. Another mesh decided between keeps its place, and the next differential of
    /// the first mesh is from its version. Must NOT happen: a differential too large for one message
    /// not sent; another frame between its chunks.
    #[tokio::test]
    async fn a_differential_larger_than_one_message_reaches_the_channel_as_its_chunks_in_order() {
        let spans = Spans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let members: Vec<(NodeId, IncarnationId)> = (0..400).map(|_| (NodeId::mint(), IncarnationId::mint())).collect();
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let rec = Recorder::default();
        let path = ForwardPath::new(Arc::default(), rec.sink());
        let mut copies = Copies::default();
        path.set_role(true);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 10, &members_at(&members, 10)), 1);
        path.source("mesh2.node-admin.1", &copies.write("mesh3", 5, &source_at(&ids, 5)), 2);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 11, &members_at(&members, 11)), 3);
        let forwarded = spans.named("rdm.mesh.topology.update.via-forwarded-delta").await;
        let chunks: usize = forwarded.iter().find(|f| f["mesh"] == "mesh1" && f["topology_version"] == "10").expect("the differential is named")["chunk_count"].parse().unwrap();
        assert!(chunks > 1, "400 members do not fit one message: {chunks} chunk(s)");
        let mut expected = vec![(0, 10); chunks];
        expected.extend([(0, 5), (10, 11)]);
        assert_eq!(rec.seen_after(chunks + 2).await, expected, "every chunk, in order, then what was decided after it");
        assert!(spans.0.lock().unwrap().iter().all(|(n, _)| n != "rdm.mesh.topology.reject.via-delta-too-large"), "nothing is too large");
    }

    /// CONTRACT (gossip.md §Size: "one node entry that cannot fit one message is not sent
    /// (`rdm.mesh.topology.reject.via-delta-too-large`), and a node behind it tops up"): a differential
    /// with one node whose name alone exceeds a message is named, with the mesh, base, version and the
    /// entry's size, and nothing of it is sent; nothing full replaces it, and the baseline moves, so the
    /// next differential of that mesh is from its version and a node behind it repairs by a read
    /// (gossip.md §3.3). Another mesh decided between keeps its place. Must NOT happen: part of a
    /// differential sent without the entry, as if complete.
    #[tokio::test]
    async fn one_node_entry_too_large_for_one_message_is_named_and_its_differential_not_sent() {
        let spans = Spans::default();
        let _capture = rdm_transport::test_span_capture::install(spans.clone());
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let (huge_id, huge_birth) = (NodeId::mint(), IncarnationId::mint());
        let mut huge = digest(2, &huge_id, &huge_birth, 1);
        huge.node.name = "n".repeat(MAX_FRAME + 1);
        let with_huge = |version: u64| {
            let mut full = source_at(&ids, version).digests();
            full.push(huge.clone());
            Full::new(full)
        };
        let rec = Recorder::default();
        let path = ForwardPath::new(Arc::default(), rec.sink());
        let mut copies = Copies::default();
        path.set_role(true);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 10, &with_huge(10)), 1);
        path.source("mesh2.node-admin.1", &copies.write("mesh3", 5, &source_at(&ids, 5)), 2);
        path.source("mesh2.node-admin.1", &copies.write("mesh1", 11, &source_at(&ids, 11)), 3);
        assert_eq!(rec.seen_after(2).await, vec![(0, 5), (10, 11)], "nothing of the differential with the oversize entry is sent; the next is from its version");
        let named = spans.named("rdm.mesh.topology.reject.via-delta-too-large").await;
        assert_eq!(named.len(), 1, "the entry that fits no message is named once");
        assert_eq!((named[0]["mesh"].as_str(), named[0]["base_version"].as_str(), named[0]["topology_version"].as_str()), ("mesh1", "0", "10"));
        assert!(named[0]["frame_bytes"].parse::<usize>().unwrap() > MAX_FRAME, "the span names the entry's size: {:?}", named[0]);
    }
}
