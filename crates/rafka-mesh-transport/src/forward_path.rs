//! The path a forwarding mesh primary's decisions take onto its own mesh channel.

use crate::membership::Frame;
use crate::snapshot::{Forward, Forwarder, Full, PublisherId};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Where a forwarded frame goes: the mesh channel's broadcast.
pub(crate) type Sink = Arc<dyn Fn(Frame) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send + Sync>;

/// The forwarder and the sink its decisions are sent to.
#[derive(Clone)]
pub(crate) struct ForwardPath {
    forwarder: Arc<Mutex<Forwarder>>,
    sink: Sink,
}

impl ForwardPath {
    pub(crate) fn new(forwarder: Arc<Mutex<Forwarder>>, sink: Sink) -> Self {
        Self { forwarder, sink }
    }

    /// Decide what `source_mesh` moving to `version` puts into the mesh, and send it from its own task.
    pub(crate) fn source_spawned(&self, me: &str, source_mesh: &str, publisher: &PublisherId, version: u64, full: &Full, now_ms: u64) {
        let out = self.forwarder.lock().unwrap().source(me, source_mesh, publisher, version, full, now_ms);
        let (sink, me, source_mesh, publisher) = (self.sink.clone(), me.to_string(), source_mesh.to_string(), publisher.clone());
        tokio::spawn(async move { send_forward(&sink, &me, &source_mesh, &publisher, version, out).await });
    }

    /// Decide as [`ForwardPath::source_spawned`] and send before returning.
    pub(crate) async fn source_awaited(&self, me: &str, source_mesh: &str, publisher: &PublisherId, version: u64, full: &Full, now_ms: u64) {
        let out = self.forwarder.lock().unwrap().source(me, source_mesh, publisher, version, full, now_ms);
        send_forward(&self.sink, me, source_mesh, publisher, version, out).await;
    }
}

/// Put what the forwarder decided for `source_mesh` onto the mesh channel, and name it.
pub(crate) async fn send_forward(sink: &Sink, me: &str, source_mesh: &str, publisher: &PublisherId, version: u64, out: Forward) {
    match out {
        Forward::Full(frames) => {
            let bytes: usize = frames.iter().map(|f| f.encode().len()).sum();
            tracing::info_span!("rdm.mesh.membership.update.via-forwarded-full", node = %me, mesh = %source_mesh, source_publisher = %publisher, topology_version = version, reason = "first-for-source", chunks = frames.len(), bytes)
                .in_scope(|| tracing::info!("the first publication of this source into the mesh is a full, loads omitted"));
            for f in frames {
                let _ = sink(f).await;
            }
        }
        Forward::Delta(frame) => {
            if let Frame::MembersDelta { base_version, changed, removed, in_flight, departed, .. } = frame.as_ref() {
                tracing::info_span!(
                    "rdm.mesh.membership.update.via-forwarded-delta",
                    node = %me,
                    mesh = %source_mesh,
                    source_publisher = %publisher,
                    base_version = *base_version,
                    topology_version = version,
                    changed = changed.len(),
                    removed = removed.len(),
                    in_flight = in_flight.len(),
                    departed = departed.len(),
                )
                .in_scope(|| tracing::info!("a source moved: one delta from the version last published into this mesh"));
            }
            let _ = sink(*frame).await;
        }
        Forward::Nothing(_) => {}
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
        path.source_spawned("mesh2.admin.1", "mesh1", &p, 10, &source_at(&ids, 10), 1);
        path.source_spawned("mesh2.admin.1", "mesh1", &p, 11, &source_at(&ids, 11), 2);
        path.source_spawned("mesh2.admin.1", "mesh1", &p, 12, &source_at(&ids, 12), 3);
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
        path.source_spawned("mesh2.admin.1", "mesh1", &p, 10, &source_at(&ids, 10), 1);
        assert_eq!(rec.seen_after(1).await, vec![(0, 10)]);
        path.source_spawned("mesh2.admin.1", "mesh1", &p, 11, &source_at(&ids, 11), 2);

        let failed = spans.named("rdm.mesh.membership.update.via-forward-failed").await;
        assert_eq!(failed.len(), 1, "the refusal is named once");
        let f = &failed[0];
        assert_eq!(f["node"], "mesh2.admin.1");
        assert_eq!(f["mesh"], "mesh1");
        assert_eq!(f["source_publisher"], p.to_string());
        assert_eq!(f["topology_version"], "11");
        assert!(f["cause"].contains("the channel refused call 1"), "the cause is the channel's own: {}", f["cause"]);
        assert_eq!(forwarder.lock().unwrap().published_version("mesh1"), None, "what was published of the source is forgotten");

        path.source_spawned("mesh2.admin.1", "mesh1", &p, 12, &source_at(&ids, 12), 3);
        assert_eq!(rec.seen_after(2).await, vec![(0, 10), (0, 12)], "the next forward of the source is a full");
    }
}
