//! The path a forwarding mesh primary's decisions take onto its own mesh channel.
//!
//! ONE ordered sender carries every decision. A decision is made under the forwarder's lock and
//! enqueued while that lock is still held, so the queue's order is the decision order; a single
//! task drains it, finishing every frame of a decision (a chunked full is one decision) before it
//! takes the next. A receiver applies a delta only at exactly the base it holds for that source,
//! and installs a full only once it holds every chunk: what depends on a frame is the later frames
//! of the SAME source and publisher, and one task sending one decision at a time keeps them in
//! order and contiguous. The frames of other sources between them change nothing a receiver holds.
//!
//! The forwarder advances its baseline when it decides, before the send. A frame the channel
//! refuses (or a queue that is gone) makes the forwarder forget that source, so the next forward of
//! it is a full: never a delta from a base the mesh did not receive.

use crate::membership::Frame;
use crate::snapshot::{Forward, Forwarder, Full, PublisherId};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Where a forwarded frame goes: the mesh channel's broadcast.
pub(crate) type Sink = Arc<dyn Fn(Frame) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send + Sync>;

/// One decision, in the order it was made.
struct Job {
    me: String,
    /// The source mesh and publisher of a single-source decision; the frames name theirs.
    source: Option<(String, PublisherId, u64)>,
    out: Forward,
}

/// The forwarder, and the one queue its decisions are sent from.
#[derive(Clone)]
pub(crate) struct ForwardPath {
    forwarder: Arc<Mutex<Forwarder>>,
    queue: mpsc::UnboundedSender<Job>,
}

impl ForwardPath {
    /// The path over `forwarder`, sending to `sink` from one task that lives as long as any clone.
    pub(crate) fn new(forwarder: Arc<Mutex<Forwarder>>, sink: Sink) -> Self {
        let (queue, rx) = mpsc::unbounded_channel();
        tokio::spawn(send_in_order(rx, sink, forwarder.clone()));
        Self { forwarder, queue }
    }

    /// Decide what `source_mesh` moving to `version` puts into the mesh, and queue it behind every
    /// decision made before it.
    pub(crate) fn source(&self, me: &str, source_mesh: &str, publisher: &PublisherId, version: u64, full: &Full, now_ms: u64) {
        let mut forwarder = self.forwarder.lock().unwrap();
        let out = forwarder.source(me, source_mesh, publisher, version, full, now_ms);
        if matches!(out, Forward::Nothing(_)) {
            return;
        }
        let job = Job { me: me.to_string(), source: Some((source_mesh.to_string(), publisher.clone(), version)), out };
        self.enqueue(&mut forwarder, job);
    }

    /// The first publication of every source in `held` (taking the seat): one decision, queued
    /// behind every decision made before it.
    pub(crate) fn fulls(&self, me: &str, node_mesh: &str, held: &[(String, PublisherId, u64, Full)], now_ms: u64) {
        let mut forwarder = self.forwarder.lock().unwrap();
        let frames = forwarder.fulls(me, held, now_ms);
        let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
        tracing::info_span!("rdm.mesh.membership.update.via-forwarded-full", node = %me, mesh = %node_mesh, reason = "seat", sources = held.len(), chunks = frames.len(), bytes)
            .in_scope(|| tracing::info!("the full of every source put into this mesh, loads omitted"));
        let job = Job { me: me.to_string(), source: None, out: Forward::Full(frames) };
        self.enqueue(&mut forwarder, job);
    }

    /// Queue `job` while the lock that decided it is held. A queue that is gone sends nothing: the
    /// sources it carried are forgotten like any refused send.
    fn enqueue(&self, forwarder: &mut Forwarder, job: Job) {
        if let Err(mpsc::error::SendError(job)) = self.queue.send(job) {
            let cause = anyhow::anyhow!("the forward queue of node {} is closed", job.me);
            for frame in frames_of(&job.out) {
                refused(Some(forwarder), &job.me, frame, &cause);
            }
        }
    }
}

fn frames_of(out: &Forward) -> Vec<&Frame> {
    match out {
        Forward::Full(frames) => frames.iter().collect(),
        Forward::Delta(frame) => vec![frame.as_ref()],
        Forward::Nothing(_) => Vec::new(),
    }
}

/// The source a forward frame belongs to: mesh, publisher and the version it moves the source to.
fn identity(frame: &Frame) -> Option<(&str, &PublisherId, u64)> {
    match frame {
        Frame::Members { mesh, publisher, topology_version, .. } => Some((mesh, publisher, *topology_version)),
        Frame::MembersDelta { mesh, source_publisher, topology_version, .. } => Some((mesh, source_publisher, *topology_version)),
        _ => None,
    }
}

/// The channel (or the queue) did not take `frame`: the forwarder forgets its source, so the next
/// forward of it is a full, and the span names what failed.
fn refused(forwarder: Option<&mut Forwarder>, me: &str, frame: &Frame, cause: &anyhow::Error) {
    let Some((mesh, publisher, version)) = identity(frame) else { return };
    if let Some(forwarder) = forwarder {
        forwarder.remove(mesh);
    }
    tracing::info_span!("rdm.mesh.membership.update.via-forward-failed", node = %me, mesh = %mesh, source_publisher = %publisher, topology_version = version, cause = %format!("{cause:#}"))
        .in_scope(|| tracing::warn!("a forward was not sent: the next forward of this source is a full"));
}

/// The one sender: each decision in the order it was queued, every frame of it before the next.
async fn send_in_order(mut rx: mpsc::UnboundedReceiver<Job>, sink: Sink, forwarder: Arc<Mutex<Forwarder>>) {
    while let Some(job) = rx.recv().await {
        send_forward(&sink, &forwarder, job).await;
    }
}

/// Put what the forwarder decided onto the mesh channel, and name it.
async fn send_forward(sink: &Sink, forwarder: &Mutex<Forwarder>, job: Job) {
    let Job { me, source, out } = job;
    let frames: Vec<Frame> = match out {
        Forward::Full(frames) => {
            if let Some((mesh, publisher, version)) = &source {
                let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
                tracing::info_span!("rdm.mesh.membership.update.via-forwarded-full", node = %me, mesh = %mesh, source_publisher = %publisher, topology_version = *version, reason = "first-for-source", chunks = frames.len(), bytes)
                    .in_scope(|| tracing::info!("the first publication of this source into the mesh is a full, loads omitted"));
            }
            frames
        }
        Forward::Delta(frame) => {
            if let (Frame::MembersDelta { base_version, changed, removed, in_flight, departed, .. }, Some((mesh, publisher, version))) = (frame.as_ref(), &source) {
                tracing::info_span!(
                    "rdm.mesh.membership.update.via-forwarded-delta",
                    node = %me,
                    mesh = %mesh,
                    source_publisher = %publisher,
                    base_version = *base_version,
                    topology_version = *version,
                    changed = changed.len(),
                    removed = removed.len(),
                    in_flight = in_flight.len(),
                    departed = departed.len(),
                )
                .in_scope(|| tracing::info!("a source moved: one delta from the version last published into this mesh"));
            }
            vec![*frame]
        }
        Forward::Nothing(_) => return,
    };
    // A source one of whose frames was refused sends no more of this decision: a full missing a
    // chunk is never installed, and the source's next forward is a full.
    let mut refused_sources: BTreeSet<String> = BTreeSet::new();
    for frame in &frames {
        let Some((mesh, ..)) = identity(frame) else { continue };
        if refused_sources.contains(mesh) {
            continue;
        }
        if let Err(cause) = sink(frame.clone()).await {
            refused_sources.insert(mesh.to_string());
            refused(Some(&mut forwarder.lock().unwrap()), &me, frame, &cause);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId};
    use std::time::Duration;

    fn digest(ordinal: u32, id: &NodeId, birth: &IncarnationId, seq: u64) -> MeshDigest {
        MeshDigest {
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
            status: MemberStatus::ReadyForTraffic,
            admin_api_base: None,
            emitted_at_rafka_ms: seq,
            digest_seq: seq,
            mesh_id: None,
            in_flight: None,
            extra: Default::default(),
            load: None,
            gossip: None,
            data_dir: None,
        }
    }

    /// The source mesh's members at `version`: one member whose digest moves with the version, so
    /// every version is a delta from the one before.
    fn source_at(ids: &[(NodeId, IncarnationId)], version: u64) -> Full {
        let mut d = digest(1, &ids[0].0, &ids[0].1, 1);
        d.status = if version % 2 == 0 { MemberStatus::ReadyForTraffic } else { MemberStatus::Draining };
        d.node.transport_addr = format!("127.0.0.1:{}", 42_000 + version).parse().unwrap();
        Full::new(vec![d], Vec::new(), Vec::new())
    }

    fn publisher() -> PublisherId {
        PublisherId { node: "mesh2.admin.1".into(), incarnation: IncarnationId::mint() }
    }

    /// Every frame the sink was handed, in the order it was handed it. The send of the first
    /// frame handed to it takes `first_takes`; a send in `fail` returns an error.
    #[derive(Clone, Default)]
    struct Recorder {
        seen: Arc<Mutex<Vec<(u64, u64)>>>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        first_takes: Duration,
        fail: Arc<Mutex<Vec<usize>>>,
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
                    let key = match &f {
                        Frame::MembersDelta { base_version, topology_version, .. } => (*base_version, *topology_version),
                        Frame::Members { topology_version, .. } => (0, *topology_version),
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
        let path = ForwardPath::new(Arc::default(), rec.sink());
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

    /// CONTRACT: a forward the mesh channel refuses leaves no silent gap. The forwarder forgets
    /// what it published of that source, so the next forward of it is a full, and the refusal is
    /// named by a span carrying the node, mesh, source publisher, version and cause.
    #[tokio::test]
    async fn a_refused_forward_resets_its_source_and_the_next_forward_is_a_full() {
        use tracing_subscriber::layer::SubscriberExt;
        let spans = Spans::default();
        let _capture = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        // The channel refuses the second send: the delta v10 to v11.
        let rec = Recorder { fail: Arc::new(Mutex::new(vec![1])), ..Recorder::default() };
        let forwarder: Arc<Mutex<Forwarder>> = Arc::default();
        let path = ForwardPath::new(forwarder.clone(), rec.sink());
        let p = publisher();
        path.source("mesh2.admin.1", "mesh1", &p, 10, &source_at(&ids, 10), 1);
        assert_eq!(rec.seen_after(1).await, vec![(0, 10)]);
        path.source("mesh2.admin.1", "mesh1", &p, 11, &source_at(&ids, 11), 2);

        let failed = spans.named("rdm.mesh.membership.update.via-forward-failed").await;
        assert_eq!(failed.len(), 1, "the refusal is named once");
        let f = &failed[0];
        assert_eq!(f["node"], "mesh2.admin.1");
        assert_eq!(f["mesh"], "mesh1");
        assert_eq!(f["source_publisher"], p.to_string());
        assert_eq!(f["topology_version"], "11");
        assert!(f["cause"].contains("the channel refused call 1"), "the cause is the channel's own: {}", f["cause"]);
        assert_eq!(forwarder.lock().unwrap().published_version("mesh1"), None, "what was published of the source is forgotten");

        path.source("mesh2.admin.1", "mesh1", &p, 12, &source_at(&ids, 12), 3);
        assert_eq!(rec.seen_after(2).await, vec![(0, 10), (0, 12)], "the next forward of the source is a full");
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
                digests[0].status = MemberStatus::Draining;
            }
            Full::new(digests, Vec::new(), Vec::new())
        };
        let ids = vec![(NodeId::mint(), IncarnationId::mint())];
        let rec = Recorder { first_takes: Duration::from_millis(100), ..Recorder::default() };
        let path = ForwardPath::new(Arc::default(), rec.sink());
        let (p, q) = (publisher(), publisher());
        let chunks = crate::snapshot::chunks_of(&big(10), |d, i, dep, ci, cc| Frame::Members { mesh: "mesh1".into(), publisher: p.clone(), forwarded_by: None, topology_version: 10, published_at_rafka_ms: 1, snapshot_id: 1, chunk_index: ci, chunk_count: cc, digests: d, in_flight: i, departed: dep }).len();
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
