//! `FetchBuildFacts` (op `0x1F`) and the Ready check's recovery of missing control facts (i143 R-G6),
//! over real Node RPC between in-process node-admins: what a responder serves from its own Build log,
//! what a caller counts as hydration, and how the Hydrator rotates, backs off and repeats.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_node_admin_core::accepted::FabricTopology;
use rafka_node_admin_core::admin::hydration_blocker;
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_facts_read::{fetch_build_facts, BuildFactsDoor, FetchFailure};
use rafka_node_admin_core::build_state::{AttemptOpened, AttemptReason, AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, LocalBuildLog, MemoryBuildStateAdapter};
use rafka_node_admin_core::hydrate::{HydrationBlocker, Hydrator, Responder, Responders};
use rafka_node_admin_core::model::{FabricId, PathName};
use rafka_node_rpc::{NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::build_facts::{BuildFacts, BuildFactsReply};
use rafka_mesh_entity::{IncarnationId, NodeId};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A node-admin's Build state: its log (whose newest Build in progress or completed is its current
/// Build, R-BI3) and its entry floor (the Build the entry named, and the attempt to reach).
struct Admin {
    name: PathName,
    log: Arc<MemoryBuildStateAdapter>,
    floor: Arc<Mutex<Option<(BuildId, u32)>>>,
}

async fn admin(name: &str, _fabric: &FabricId) -> Admin {
    Admin { name: name.parse().unwrap(), log: Arc::new(MemoryBuildStateAdapter::new()), floor: Arc::default() }
}

/// The entry `a` was served named Build `id` at `attempt`: the answering admin's current Build.
fn entry_named(a: &Admin, id: &BuildId, attempt: u32) {
    *a.floor.lock().unwrap() = Some((id.clone(), attempt));
}

/// `a` holds Build `id` through attempt `attempts`, converged: its current Build.
async fn holds(a: &Admin, id: &BuildId, attempts: u32) {
    a.log.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
    for attempt in 1..=attempts {
        if attempt > 1 {
            a.log.open_attempt(&AttemptOpened { build_id: id.clone(), attempt, reason: AttemptReason::Restart, action: None, opened_by: "x".into(), opened_at_ms: 0 }).await.unwrap();
        }
        a.log.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
        a.log.append_attempt_receipt(&BuildAttemptReceipt { build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    }
}

/// One node on the wire: its endpoint, a client that resolves the nodes it is given, and what it answers.
struct Node {
    ep: iroh::Endpoint,
    client: Arc<NodeRpcClient>,
    resolver: Arc<StaticResolver>,
    resolved: ResolvedNode,
    calls: Arc<AtomicU32>,
    _router: Router,
}

/// A node serving `FetchBuildFacts` with `serve`, counting the reads it is asked.
async fn node_serving<F, Fut>(name: &str, serve: F) -> Node
where
    F: Fn(rafka_node_rpc_contract::build_facts::BuildFactsRequest, rafka_node_rpc::stream::ReplySink<BuildFacts, rafka_node_rpc::stream::NotStarted>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<BuildFactsReply, rafka_node_rpc::HandlerFault>> + Send + 'static,
{
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver.clone()));
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let calls = Arc::new(AtomicU32::new(0));
    let counted = calls.clone();
    let server = ServerBuilder::new()
        .serve_stream::<BuildFacts, _, _>(rafka_node_rpc_contract::catalog::OpOwner::Product("rdm".into()), move |_peer, req, sink| {
            counted.fetch_add(1, Ordering::SeqCst);
            serve(req, sink)
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .expect("the catalog seals");
    let router = Router::builder(ep.clone()).accept(rafka_node_rpc::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id, name: name.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation };
    Node { ep, client, resolver, resolved, calls, _router: router }
}

/// A node answering from `a`'s own Build state, as a node-admin does.
async fn node_of(a: &Admin) -> Node {
    let door = Arc::new(BuildFactsDoor { me: a.name.clone(), builds: a.log.clone(), floor: a.floor.clone() });
    node_serving(&a.name.to_string(), move |req, sink| {
        let door = door.clone();
        async move { Ok(door.serve(req, sink).await) }
    })
    .await
}

fn responders(list: Vec<&Node>) -> Responders {
    let list: Vec<Responder> = list.into_iter().map(|n| Responder { name: n.resolved.name.clone(), node_id: n.resolved.node_id.clone() }).collect();
    Arc::new(move || {
        let list = list.clone();
        Box::pin(async move { list })
    })
}

/// The caller: its own state and a client that resolves `peers`.
async fn caller(name: &str, fabric: &FabricId, peers: &[&Node]) -> (Admin, Node) {
    let a = admin(name, fabric).await;
    let n = node_serving(name, |_, _| async { Ok(BuildFactsReply::NotReady { reason: "a caller".into() }) }).await;
    for p in peers {
        n.resolver.insert(p.resolved.clone());
    }
    (a, n)
}

fn hydrator(c: &(Admin, Node), responders: Responders, entry: Option<rafka_node_admin_core::hydrate::EntryRepeat>) -> Hydrator {
    Hydrator::new(c.0.name.clone(), c.1.client.clone(), c.0.log.clone() as Arc<dyn LocalBuildLog>, responders, entry)
}

/// Drive the Ready check's turn until it finds no blocker, recording each distinct blocker kind.
async fn drive(a: &Admin, h: &mut Hydrator, within: Duration) -> Result<Vec<&'static str>, Vec<&'static str>> {
    let until = Instant::now() + within;
    let mut seen: Vec<&'static str> = Vec::new();
    loop {
        let floor = a.floor.lock().unwrap().clone();
        let blocker = hydration_blocker(&a.name, &*a.log, floor).await;
        if let Some(b) = &blocker {
            if seen.last() != Some(&b.kind()) {
                seen.push(b.kind());
            }
        }
        h.tick(blocker.as_ref()).await;
        if blocker.is_none() {
            return Ok(seen);
        }
        if Instant::now() > until {
            return Err(seen);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}


const FETCH: &str = "rdm.node_admin.build.update.via-fetch-facts";
const FETCH_INCOMPLETE: &str = "rdm.node_admin.build.reject.via-fetch-incomplete";
const FETCH_FAILED: &str = "rdm.node_admin.build.reject.via-fetch-failed";
const ENTRY_REPEAT: &str = "rdm.node_admin.fabric.update.via-entry-repeat";
const CALL: &str = "rdm.node_rpc.request.update.via-call";
const DIAL: &str = "rdm.node_rpc.connection.create.via-dial";

/// One finished span as the cell's capture exported it.
struct Seen {
    name: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    start: std::time::SystemTime,
    attributes: BTreeMap<String, String>,
}

impl Seen {
    fn attr(&self, key: &str) -> &str {
        self.attributes.get(key).map(String::as_str).unwrap_or("<absent>")
    }
}

/// A cell's root span, as its trace and span ids.
struct Story {
    trace_id: String,
    span_id: String,
}

fn story_of(root: &tracing::Span) -> Story {
    let traceparent = rafka_mesh_telemetry::traceparent_of(root).expect("the cell's root span has a trace context: the capture is installed on this thread");
    let mut parts = traceparent.split('-');
    Story { trace_id: parts.nth(1).expect("a traceparent names its trace").into(), span_id: parts.next().expect("a traceparent names its span").into() }
}

/// This cell's own span capture: the product span pipeline (`rafka_mesh_telemetry::span_capture`: its span filter and
/// `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher installed on every thread of the cell's
/// own runtime, so two cells in one test process never share spans.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rafka_mesh_telemetry::SpanCapture,
    dispatch: tracing::Dispatch,
    /// True when the runtime's worker threads hold no dispatcher: only the thread that runs the story does.
    bare_workers: bool,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture("build-facts-read", exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)), bare_workers: false }
}

/// A capture whose runtime's workers hold no dispatcher, as a `#[tokio::test(flavor = "multi_thread")]` runtime's do.
fn capture_with_bare_workers() -> Capture {
    let mut cap = capture();
    cap.bare_workers = true;
    cap
}

/// Runs the cell's story as a child of a root span named `$name`, on the capture's runtime, and gives back the root's
/// ids and what the story returned.
macro_rules! story {
    ($cap:expr, $name:literal, $body:expr) => {
        $cap.run(async {
            let root = tracing::info_span!($name);
            let story = story_of(&root);
            let out = tracing::Instrument::instrument($body, root).await;
            (story, out)
        })
    };
}

impl Capture {
    /// The cell's runtime (four workers, as the cell always had): every worker thread (and the caller's, while it runs)
    /// emits into this capture only. The runtime is dropped when the story ends, so no task of the story outlives it
    /// and every span it opened has ended when `spans` reads.
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let bare = self.bare_workers;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .on_thread_start(move || {
                if !bare {
                    std::mem::forget(tracing::dispatcher::set_default(&d));
                }
            })
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    fn finished(&self) -> Vec<Seen> {
        let mut seen: Vec<Seen> = self
            .exporter
            .get_finished_spans()
            .unwrap_or_default()
            .into_iter()
            .map(|s| Seen {
                name: s.name.to_string(),
                trace_id: s.span_context.trace_id().to_string(),
                span_id: s.span_context.span_id().to_string(),
                parent_span_id: s.parent_span_id.to_string(),
                start: s.start_time,
                attributes: s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect(),
            })
            .collect();
        seen.sort_by_key(|s| s.start);
        seen
    }

    /// Every span the story's runtime ended, once they have all reached the exporter.
    fn spans(&self, story: Story) -> Spans {
        self.capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let all = self.finished();
        crate::assert_own_story(all.iter().map(|s| s.name.as_str()));
        Spans { all, story }
    }
}

/// A cell leaves its trace: every span its capture holds (name, ids, attributes) goes to
/// `target/i143-testkit/build_facts_read/<cell>.<pid>.spans.json`, whether the cell passed or panicked, so a red
/// reads from its own spans (did the fetch complete on the transport, did the facts arrive and the Build stay
/// unreadable) and never from a rerun.
impl Drop for Capture {
    fn drop(&mut self) {
        let flushed = self.capture.flush(Duration::from_secs(10));
        let spans: Vec<serde_json::Value> = self
            .finished()
            .into_iter()
            .map(|s| {
                serde_json::json!({
                    "name": s.name,
                    "trace_id": s.trace_id,
                    "span_id": s.span_id,
                    "parent_span_id": s.parent_span_id,
                    "start_us": s.start.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0),
                    "attributes": s.attributes,
                })
            })
            .collect();
        let cell = std::thread::current().name().unwrap_or("unnamed").replace("::", "__");
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit/build_facts_read");
        let path = dir.join(format!("{cell}.{}.spans.json", std::process::id()));
        let written = std::fs::create_dir_all(&dir).and_then(|_| std::fs::write(&path, serde_json::to_vec_pretty(&spans).unwrap()));
        eprintln!("build_facts_read: the cell's {} spans -> {} (flush: {flushed:?}, write: {written:?})", spans.len(), path.display());
    }
}

/// What a cell's capture holds, and the root its story ran under.
struct Spans {
    all: Vec<Seen>,
    story: Story,
}

impl Spans {
    /// Every span named `name`, in the order it began.
    fn named(&self, name: &str) -> Vec<&Seen> {
        self.all.iter().filter(|s| s.name == name).collect()
    }

    /// Every span named `name`, each a direct child of the cell's root: the trace the caller of the Hydrator was in is
    /// the trace of the request the Hydrator spawned.
    fn children(&self, name: &str) -> Vec<&Seen> {
        let found = self.named(name);
        for s in &found {
            assert!(
                s.trace_id == self.story.trace_id && s.parent_span_id == self.story.span_id,
                "a {name} span {:?} is span {} of trace {} under parent {}, not a child of the cell's root (span {} of trace {}); the capture holds:\n{}",
                s.attributes,
                s.span_id,
                s.trace_id,
                s.parent_span_id,
                self.story.span_id,
                self.story.trace_id,
                self.render()
            );
        }
        found
    }

    /// The `via-call` spans that are children of `fetch`: the one call the fetch made.
    fn calls_of(&self, fetch: &Seen) -> Vec<&Seen> {
        self.named(CALL).into_iter().filter(|c| c.parent_span_id == fetch.span_id && c.trace_id == fetch.trace_id).collect()
    }

    fn render(&self) -> String {
        self.all.iter().map(|s| format!("  {} trace={} span={} parent={} {:?}", s.name, s.trace_id, s.span_id, s.parent_span_id, s.attributes)).collect::<Vec<_>>().join("\n")
    }
}

/// A fetch span names the blocker it served, the responder it asked and the facts it read.
/// A fetch that read nothing records no `facts`.
fn assert_fetch(spans: &Spans, fetch: &Seen, node: &str, id: &BuildId, blocker: &str, target: &str, facts: Option<u32>) {
    let held = (fetch.attr("node"), fetch.attr("build_id"), fetch.attr("blocker"), fetch.attr("target"), fetch.attr("facts"));
    let facts = facts.map_or("<absent>".to_string(), |n| n.to_string());
    assert_eq!(held, (node, id.as_str(), blocker, target, facts.as_str()), "the fetch span's story;\n{}", spans.render());
    let calls = spans.calls_of(fetch);
    assert_eq!(calls.len(), 1, "the fetch made one call, the child of its span;\n{}", spans.render());
}

// @feature: node-lifecycle
/// CONTRACT: a joiner holds the Build its entry reply named (the answering admin's current Build)
/// and none of its facts, so it holds no current Build. The Ready check names the typed blocker (the
/// named Build wanted), FetchBuildFacts retrieves the Build's facts from the responder, they are
/// absorbed into the local Build log, the Build is current and the blocker clears. Its trace: the one
/// fetch span is a child of the span the Ready check ran under, names the blocker
/// (`no-current-build-wanted`), the responder and the six facts it read, and holds the one call it made.
#[test]
fn a_build_the_entry_named_is_held_current_once_its_facts_are_fetched() {
    let cap = capture();
    let (story, id) = story!(cap, "rdm.test.build.update.via-entry-named-build", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let full = admin("mesh1.admin.1", &fabric).await;
        holds(&full, &id, 2).await;
        let responder = node_of(&full).await;
        let joiner = caller("mesh1.admin.2", &fabric, &[&responder]).await;
        entry_named(&joiner.0, &id, 2);
        assert_eq!(joiner.0.log.current_build().await.unwrap().into_build_id(), None, "no Build is held: none is current");
        let b = hydration_blocker(&joiner.0.name, &*joiner.0.log, joiner.0.floor.lock().unwrap().clone()).await.unwrap();
        assert!(matches!(&b, HydrationBlocker::NoCurrentBuild { wanted: Some(w), .. } if *w == id), "{b:?}");

        let mut h = hydrator(&joiner, responders(vec![&responder]), None);
        let seen = drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates");
        assert_eq!(seen, vec!["no-current-build-wanted"], "the one blocker the joiner named");
        assert_eq!(joiner.0.log.current_build().await.unwrap().into_build_id(), Some(id.clone()), "the fetched Build is current");
        assert_eq!(joiner.0.log.read_build(&id).await.unwrap().attempt, 2, "the joiner's own log holds the Build through the floor");
        assert_eq!(responder.calls.load(Ordering::SeqCst), 1, "one read");
        for n in [&responder, &joiner.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert_eq!(fetches.len(), 1, "one fetch;\n{}", spans.render());
    assert_fetch(&spans, fetches[0], "mesh1.admin.2", &id, "no-current-build-wanted", "mesh1.admin.1", Some(6));
}

// @feature: node-lifecycle
/// CONTRACT: the stream is a snapshot of the responder's facts for the one Build asked, `End` is
/// explicit about completeness, and a Build it holds no fact of is a typed refusal. Its trace: the
/// two reads are two call spans under the cell's root, the first of which opened a stream.
#[test]
fn the_responder_streams_one_builds_facts_and_names_an_unknown_build() {
    let cap = capture();
    let (story, ()) = story!(cap, "rdm.test.build.serve.via-streamed-facts", async {
        let fabric = FabricId::mint();
        let (id, other) = (BuildId::mint(), BuildId::mint());
        let full = admin("mesh1.admin.1", &fabric).await;
        holds(&full, &id, 3).await;
        holds(&full, &other, 1).await;
        let responder = node_of(&full).await;
        let c = caller("mesh1.admin.2", &fabric, &[&responder]).await;
        let t = NodeTarget::ExactNode(responder.resolved.node_id.clone());

        let got = fetch_build_facts(&c.1.client, &t, &id).await.expect("the read completes");
        assert!(got.complete, "a Ready responder holding the Build whole says so");
        assert!(got.facts.iter().all(|f| *f.build_id() == id), "only the Build asked for");
        assert_eq!(got.facts.len(), 1 + 3 + 2 + 3, "accepted, three claims, two opens, three receipts");
        let none = fetch_build_facts(&c.1.client, &t, &BuildId::mint()).await.unwrap_err();
        assert!(matches!(none, FetchFailure::UnknownBuild(_)), "{none}");
        for n in [&responder, &c.1] {
            n.ep.close().await;
        }
    });
    let spans = cap.spans(story);
    let calls = spans.children(CALL);
    assert_eq!(calls.len(), 2, "two reads, two calls;\n{}", spans.render());
    assert_eq!(calls[0].attr("outcome"), "stream-open", "the read of the Build it holds opened a stream;\n{}", spans.render());
}

// @feature: node-lifecycle
/// CONTRACT: a responder whose own holdings are not whole (it holds the Build's facts but is behind
/// the floor of its own entry) says `complete = false` though the facts it holds would reach the floor; the caller
/// absorbs nothing from it and rotates to the next responder; only the full holdings of the second count.
/// Its trace: two fetch spans under the cell's root, the first to the partial responder ending
/// `responder-incomplete` beside a `via-fetch-incomplete` rejection, the second to the full one.
#[test]
fn an_incomplete_responder_is_passed_over_and_only_full_holdings_count() {
    let cap = capture();
    let (story, id) = story!(cap, "rdm.test.build.update.via-incomplete-responder", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let partial = admin("mesh1.admin.1", &fabric).await;
        holds(&partial, &id, 2).await;
        entry_named(&partial, &id, 3);
        let full = admin("mesh1.admin.2", &fabric).await;
        holds(&full, &id, 2).await;
        let (first, second) = (node_of(&partial).await, node_of(&full).await);
        let joiner = caller("mesh1.admin.3", &fabric, &[&first, &second]).await;
        entry_named(&joiner.0, &id, 2);

        let got = fetch_build_facts(&joiner.1.client, &NodeTarget::ExactNode(first.resolved.node_id.clone()), &id).await.unwrap();
        assert!(!got.complete, "the first responder says its holdings are not whole");
        assert!(joiner.0.log.facts().await.unwrap().is_empty(), "a read alone absorbs nothing");

        let mut h = hydrator(&joiner, responders(vec![&first, &second]), None);
        drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates from the second");
        assert_eq!(first.calls.load(Ordering::SeqCst), 2, "the first responder was asked (once by the read above, once by the Hydrator) and passed over");
        assert_eq!(second.calls.load(Ordering::SeqCst), 1, "the second responder's full holdings hydrated it");
        assert_eq!(joiner.0.log.read_build(&id).await.unwrap().attempt, 2);
        for n in [&first, &second, &joiner.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert_eq!(fetches.len(), 2, "one fetch per responder asked;\n{}", spans.render());
    assert_fetch(&spans, fetches[0], "mesh1.admin.3", &id, "no-current-build-wanted", "mesh1.admin.1", Some(6));
    assert_eq!(fetches[0].attr("outcome"), "responder-incomplete", "the partial responder's holdings were not absorbed;\n{}", spans.render());
    assert_fetch(&spans, fetches[1], "mesh1.admin.3", &id, "no-current-build-wanted", "mesh1.admin.2", Some(6));
    let incomplete = spans.children(FETCH_INCOMPLETE);
    assert_eq!(incomplete.len(), 1, "one rejection;\n{}", spans.render());
    assert_eq!((incomplete[0].attr("target"), incomplete[0].attr("build_id")), ("mesh1.admin.1", id.as_str()));
}

// @feature: node-lifecycle
/// CONTRACT: a stream that ends before its `End` is never hydration: nothing is absorbed, the blocker
/// stands, the target is left alone for its backoff, and the same request is made again from the
/// beginning and then counts. Its trace: the cut read is a call span under the cell's root, the
/// Hydrator's repeated read is one fetch span (three facts) holding its own call.
#[test]
fn an_interrupted_stream_never_counts_and_the_read_is_made_again() {
    let cap = capture();
    let (story, id) = story!(cap, "rdm.test.build.update.via-interrupted-stream", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let full = admin("mesh1.admin.1", &fabric).await;
        holds(&full, &id, 1).await;
        let door = Arc::new(BuildFactsDoor { me: full.name.clone(), builds: full.log.clone(), floor: full.floor.clone() });
        let asked = Arc::new(AtomicU32::new(0));
        let responder = {
            let (door, asked) = (door.clone(), asked.clone());
            node_serving("mesh1.admin.1", move |req, sink| {
                let (door, n) = (door.clone(), asked.fetch_add(1, Ordering::SeqCst));
                async move {
                    if n == 0 {
                        // The first read: Started and one chunk, then the stream is cut.
                        let mut sink = sink.started(BuildFactsReply::Started).await.map_err(|e| rafka_node_rpc::HandlerFault::invariant_broken(format!("{e:?}")))?;
                        let (messages, _) = rafka_node_admin_core::fabric_builds::encode_chunks(full_facts(&door).await);
                        sink.data(BuildFactsReply::Facts { build_id: id_of(&req), chunk_index: 0, chunk_count: messages.len() as u32 + 1, facts: messages[0].to_vec() })
                            .await
                            .map_err(|e| rafka_node_rpc::HandlerFault::invariant_broken(format!("{e:?}")))?;
                        return Err(rafka_node_rpc::HandlerFault::invariant_broken("the stream is cut"));
                    }
                    Ok(door.serve(req, sink).await)
                }
            })
            .await
        };
        let joiner = caller("mesh1.admin.2", &fabric, &[&responder]).await;
        entry_named(&joiner.0, &id, 1);

        let cut = fetch_build_facts(&joiner.1.client, &NodeTarget::ExactNode(responder.resolved.node_id.clone()), &id).await.unwrap_err();
        assert!(matches!(cut, FetchFailure::Interrupted(_)), "{cut}");
        assert!(joiner.0.log.facts().await.unwrap().is_empty(), "an interrupted stream absorbs nothing");

        let mut h = hydrator(&joiner, responders(vec![&responder]), None);
        drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("the repeated read hydrates");
        assert_eq!(asked.load(Ordering::SeqCst), 2, "the cut read, then the read made again");
        assert_eq!(joiner.0.log.current_build().await.unwrap().into_build_id(), Some(id.clone()));
        for n in [&responder, &joiner.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert_eq!(fetches.len(), 1, "the Hydrator read once; the cut read was the cell's own;\n{}", spans.render());
    assert_fetch(&spans, fetches[0], "mesh1.admin.2", &id, "no-current-build-wanted", "mesh1.admin.1", Some(3));
    assert_eq!(spans.named(CALL).len(), 2, "the cut read's call and the fetch's call;\n{}", spans.render());
}

async fn full_facts(door: &BuildFactsDoor) -> Vec<rafka_node_admin_core::build_state::BuildFact> {
    door.builds.facts().await.unwrap()
}

fn id_of(req: &rafka_node_rpc_contract::build_facts::BuildFactsRequest) -> String {
    let rafka_node_rpc_contract::build_facts::BuildFactsRequest::FetchBuildFacts { build_id } = req;
    build_id.clone()
}

// @feature: node-lifecycle
/// CONTRACT: the fabric-primary is asked first. When it cannot answer (it refuses NotReady, or its
/// peer does not resolve), the next known node-admin is asked, and its holdings hydrate the joiner. The
/// fabric-primary is not killed: it is made unreachable by what it answers. Its trace: four fetch spans
/// under the cell's root (the refusing fabric-primary, the node-admin that answered, the unresolvable
/// peer, the node-admin that answered), each refusal beside a `via-fetch-failed` rejection naming its reason.
#[test]
fn an_unreachable_fabric_primary_rotates_to_another_node_admin() {
    let cap = capture();
    let (story, id) = story!(cap, "rdm.test.build.update.via-unreachable-fabric-primary", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let other = admin("mesh1.admin.2", &fabric).await;
        holds(&other, &id, 1).await;
        let not_ready = node_serving("mesh1.admin.1", |_, _| async { Ok(BuildFactsReply::NotReady { reason: "the fabric-primary holds no Build state yet".into() }) }).await;
        let second = node_of(&other).await;
        let joiner = caller("mesh1.admin.3", &fabric, &[&not_ready, &second]).await;
        entry_named(&joiner.0, &id, 1);
        let mut h = hydrator(&joiner, responders(vec![&not_ready, &second]), None);
        drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates from the second");
        assert_eq!(not_ready.calls.load(Ordering::SeqCst), 1, "the fabric-primary was asked first, once");
        assert_eq!(second.calls.load(Ordering::SeqCst), 1);

        // Its peer does not resolve at all: the read ends Unreached and the next responder answers.
        let ghost = Responder { name: "mesh1.admin.9".parse().unwrap(), node_id: NodeId::mint() };
        let joiner2 = caller("mesh1.admin.4", &fabric, &[&second]).await;
        entry_named(&joiner2.0, &id, 1);
        let list = vec![ghost, Responder { name: second.resolved.name.clone(), node_id: second.resolved.node_id.clone() }];
        let responders: Responders = Arc::new(move || {
            let list = list.clone();
            Box::pin(async move { list })
        });
        let mut h2 = hydrator(&joiner2, responders, None);
        drive(&joiner2.0, &mut h2, Duration::from_secs(10)).await.expect("hydrates past the unresolvable fabric-primary");
        for n in [&not_ready, &second, &joiner.1, &joiner2.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert_eq!(fetches.len(), 4, "two fetches per joiner;\n{}", spans.render());
    assert_fetch(&spans, fetches[0], "mesh1.admin.3", &id, "no-current-build-wanted", "mesh1.admin.1", None);
    assert_fetch(&spans, fetches[1], "mesh1.admin.3", &id, "no-current-build-wanted", "mesh1.admin.2", Some(3));
    assert_fetch(&spans, fetches[2], "mesh1.admin.4", &id, "no-current-build-wanted", "mesh1.admin.9", None);
    assert_fetch(&spans, fetches[3], "mesh1.admin.4", &id, "no-current-build-wanted", "mesh1.admin.2", Some(3));
    assert_eq!((fetches[0].attr("outcome"), fetches[2].attr("outcome")), ("not-ready", "unreached"), "the reasons the two first responders gave;\n{}", spans.render());
    let failed = spans.children(FETCH_FAILED);
    let reasons: Vec<(&str, &str)> = failed.iter().map(|f| (f.attr("target"), f.attr("reason"))).collect();
    assert_eq!(reasons, vec![("mesh1.admin.1", "not-ready"), ("mesh1.admin.9", "unreached")], "{}", spans.render());
}

// @feature: node-lifecycle
/// CONTRACT: a same-key restart whose entry names a Build its own log no longer holds the facts of
/// names the blocker (the named Build wanted) and fetches the facts. Its trace: the one fetch span is
/// a child of the span the Ready check ran under, names the blocker (`no-current-build-wanted`), the
/// responder and the six facts it read.
#[test]
fn a_restart_that_lost_the_builds_facts_fetches_them() {
    let cap = capture();
    let (story, id) = story!(cap, "rdm.test.build.update.via-restart-lost-facts", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let full = admin("mesh1.admin.1", &fabric).await;
        holds(&full, &id, 2).await;
        let responder = node_of(&full).await;
        let restarted = caller("mesh1.admin.2", &fabric, &[&responder]).await;
        entry_named(&restarted.0, &id, 2);
        let mut h = hydrator(&restarted, responders(vec![&responder]), None);
        let seen = drive(&restarted.0, &mut h, Duration::from_secs(10)).await.expect("hydrates");
        assert_eq!(seen, vec!["no-current-build-wanted"]);
        assert_eq!(restarted.0.log.read_build(&id).await.unwrap().attempt, 2);
        for n in [&responder, &restarted.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert_eq!(fetches.len(), 1, "one fetch;\n{}", spans.render());
    assert_fetch(&spans, fetches[0], "mesh1.admin.2", &id, "no-current-build-wanted", "mesh1.admin.1", Some(6));
}

// @feature: node-lifecycle
/// CONTRACT: a Hydrator turn that runs on a runtime whose worker threads hold no dispatcher (the runtime a
/// `#[tokio::test(flavor = "multi_thread")]` cell gets: only the thread that called `block_on` holds the
/// capture) still has its whole story captured and ended when the story ends: the fetch it spawns runs on
/// the dispatcher of the turn that started it, the dial its call starts ends on the call's dispatcher, and
/// the endpoints the story bound hold none of its spans. Must NOT happen: a span of the story lost because
/// a worker thread picked its task up, or held open past the story by a task of the transport. Its trace:
/// the cell's root ended; the one fetch span is its child, naming the blocker and the six facts it read;
/// the fetch holds its one call and the call holds its one dial.
#[test]
fn a_fetch_spawned_onto_a_worker_that_holds_no_dispatcher_is_still_captured() {
    let cap = capture_with_bare_workers();
    let (story, id) = story!(cap, "rdm.test.build.update.via-worker-without-dispatcher", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let full = admin("mesh1.admin.1", &fabric).await;
        holds(&full, &id, 2).await;
        let responder = node_of(&full).await;
        let restarted = caller("mesh1.admin.2", &fabric, &[&responder]).await;
        entry_named(&restarted.0, &id, 2);
        let mut h = hydrator(&restarted, responders(vec![&responder]), None);
        drive(&restarted.0, &mut h, Duration::from_secs(10)).await.expect("hydrates");
        assert_eq!(restarted.0.log.read_build(&id).await.unwrap().attempt, 2);
        for n in [&responder, &restarted.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert_eq!(fetches.len(), 1, "the fetch ran on a worker that holds no dispatcher and was still captured;\n{}", spans.render());
    assert_fetch(&spans, fetches[0], "mesh1.admin.2", &id, "no-current-build-wanted", "mesh1.admin.1", Some(6));
    let call = spans.calls_of(fetches[0])[0];
    let dials: Vec<&Seen> = spans.named(DIAL).into_iter().filter(|d| d.parent_span_id == call.span_id && d.trace_id == call.trace_id).collect();
    assert_eq!(dials.len(), 1, "the call's one dial is its child;\n{}", spans.render());
    let roots: Vec<&Seen> = spans.named("rdm.test.build.update.via-worker-without-dispatcher").into_iter().filter(|r| r.span_id == spans.story.span_id).collect();
    assert_eq!(roots.len(), 1, "the cell's root ended with its story: nothing outlived the story holding it;\n{}", spans.render());
}

// @feature: node-lifecycle
/// CONTRACT: a local Build behind the entry's attempt floor names that blocker; the responder's
/// complete holdings reach the floor and clear it. A responder behind the floor is passed over.
/// Its trace: two fetch spans under the cell's root, both for `behind-floor`, the first to the responder
/// behind the floor (three facts, absorbed, not enough) and the second to the one ahead (nine facts).
#[test]
fn a_build_behind_the_entry_floor_is_fetched_up_to_it() {
    let cap = capture();
    let (story, id) = story!(cap, "rdm.test.build.update.via-entry-floor", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let behind = admin("mesh1.admin.1", &fabric).await;
        holds(&behind, &id, 1).await;
        let ahead = admin("mesh1.admin.2", &fabric).await;
        holds(&ahead, &id, 3).await;
        let (first, second) = (node_of(&behind).await, node_of(&ahead).await);
        let joiner = caller("mesh1.admin.3", &fabric, &[&first, &second]).await;
        holds(&joiner.0, &id, 1).await;
        *joiner.0.floor.lock().unwrap() = Some((id.clone(), 3));
        let mut h = hydrator(&joiner, responders(vec![&first, &second]), None);
        let seen = drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates");
        assert_eq!(seen, vec!["behind-floor"]);
        assert_eq!(joiner.0.log.read_build(&id).await.unwrap().attempt, 3);
        assert_eq!(first.calls.load(Ordering::SeqCst), 1, "the responder behind the floor gave facts that did not clear it, and was not asked again");
        for n in [&first, &second, &joiner.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert_eq!(fetches.len(), 2, "one fetch per responder asked;\n{}", spans.render());
    assert_fetch(&spans, fetches[0], "mesh1.admin.3", &id, "behind-floor", "mesh1.admin.1", Some(3));
    assert_eq!(fetches[0].attr("outcome"), "absorbed", "the responder behind the floor was complete, and not enough;\n{}", spans.render());
    assert_fetch(&spans, fetches[1], "mesh1.admin.3", &id, "behind-floor", "mesh1.admin.2", Some(9));
}

// @feature: node-lifecycle
/// CONTRACT: with no current Build and no Build named by an entry, nothing is fetched: the control
/// entry is retrieved again, and only once that names a Build is its facts read. Its trace: the entry
/// retrievals are `via-entry-repeat` spans under the cell's root (the first three for
/// `no-current-build`, each `retrieved`), then one fetch span for `no-current-build-wanted`.
#[test]
fn an_unidentified_build_is_never_fetched_the_entry_is_retrieved_again() {
    let cap = capture();
    let (story, id) = story!(cap, "rdm.test.fabric.update.via-unidentified-build", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let full = admin("mesh1.admin.1", &fabric).await;
        holds(&full, &id, 1).await;
        let responder = node_of(&full).await;
        let joiner = caller("mesh1.admin.2", &fabric, &[&responder]).await;
        let entries = Arc::new(AtomicU32::new(0));
        let entry: rafka_node_admin_core::hydrate::EntryRepeat = {
            let (entries, floor, id) = (entries.clone(), joiner.0.floor.clone(), id.clone());
            Arc::new(move || {
                let (entries, floor, id) = (entries.clone(), floor.clone(), id.clone());
                Box::pin(async move {
                    // The first retrievals find the maker without a current Build; the third names it.
                    if entries.fetch_add(1, Ordering::SeqCst) >= 2 {
                        *floor.lock().unwrap() = Some((id, 1));
                    }
                    Ok(())
                })
            })
        };
        let mut h = hydrator(&joiner, responders(vec![&responder]), Some(entry));
        let seen = drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("hydrates once the entry names the Build");
        assert_eq!(seen, vec!["no-current-build", "no-current-build-wanted"]);
        assert!(entries.load(Ordering::SeqCst) >= 3, "the entry was retrieved until it named a Build");
        assert_eq!(responder.calls.load(Ordering::SeqCst), 1, "nothing was fetched before the Build was identified");
        for n in [&responder, &joiner.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let repeats = spans.children(ENTRY_REPEAT);
    assert!(repeats.len() >= 3, "the entry was retrieved at least three times;\n{}", spans.render());
    for r in &repeats[..3] {
        assert_eq!((r.attr("node"), r.attr("blocker"), r.attr("outcome")), ("mesh1.admin.2", "no-current-build", "retrieved"), "{}", spans.render());
    }
    let fetches = spans.children(FETCH);
    assert_eq!(fetches.len(), 1, "the Build was fetched once, after it was named;\n{}", spans.render());
    assert_fetch(&spans, fetches[0], "mesh1.admin.2", &id, "no-current-build-wanted", "mesh1.admin.1", Some(3));
}

#[test]
fn the_backoff_is_bounded() {
    use rafka_node_admin_core::hydrate::{BACKOFF_BASE, BACKOFF_CAP};
    assert!(BACKOFF_BASE < BACKOFF_CAP);
    assert!(BACKOFF_CAP <= Duration::from_secs(5));
}
