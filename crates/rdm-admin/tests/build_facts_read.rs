//! `FetchBuildFacts`, `FetchCurrentBuild` and `FetchBuildSection` (op `0x1F`) and the Ready check's recovery of missing
//! control facts (i143 R-G6), over real Node RPC between in-process node-admins: what a responder
//! serves from its own Build log, what a caller counts as a fetched Build, and how the FetchBuild
//! rotates, backs off and repeats.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_admin::accepted::FabricTopology;
use rdm_admin::admin::node_admin_not_ready;
use rdm_admin::build::BuildId;
use rdm_admin::build_facts_read::{fetch_build_facts, fetch_current_build, BuildFactsDoor, FetchFailure};
use rdm_admin::build_state::{AttemptOpened, AttemptReason, AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, LocalBuildLog, MemoryBuildStateAdapter};
use rdm_admin::fetch_build::{NodeAdminNotReady, FetchBuild, Responder, Responders};
use rdm_admin::model::{FabricId, PathName};
use rdm_transport::{NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::build_facts::{BuildFacts, BuildFactsReply};
use rdm_core::{IncarnationId, NodeId};
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
    a.log.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
    for attempt in 1..=attempts {
        if attempt > 1 {
            a.log.open_attempt(&AttemptOpened { mesh: None, build_id: id.clone(), attempt, reason: AttemptReason::Stop, action: None, opened_by: "x".into(), opened_at_ms: 0, context: None }).await.unwrap();
        }
        a.log.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
        a.log.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
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
    F: Fn(rdm_core::build_facts::BuildFactsRequest, rdm_transport::stream::ReplySink<BuildFacts, rdm_transport::stream::NotStarted>) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<BuildFactsReply, rdm_transport::HandlerFault>> + Send + 'static,
{
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver.clone()));
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let calls = Arc::new(AtomicU32::new(0));
    let counted = calls.clone();
    let server = ServerBuilder::new()
        .serve_stream::<BuildFacts, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req, sink| {
            counted.fetch_add(1, Ordering::SeqCst);
            serve(req, sink)
        })
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .expect("the catalog is built");
    let router = Router::builder(ep.clone()).accept(rdm_transport::ALPN, server).spawn();
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

fn fetch_build(c: &(Admin, Node), responders: Responders) -> FetchBuild {
    FetchBuild::new(c.0.name.clone(), c.1.client.clone(), c.0.log.clone() as Arc<dyn LocalBuildLog>, responders)
}

/// Drive the Ready check's turn until it finds no blocker, recording each distinct blocker kind.
async fn drive(a: &Admin, h: &mut FetchBuild, within: Duration) -> Result<Vec<&'static str>, Vec<&'static str>> {
    let until = Instant::now() + within;
    let mut seen: Vec<&'static str> = Vec::new();
    loop {
        let floor = a.floor.lock().unwrap().clone();
        let blocker = node_admin_not_ready(&a.name, &*a.log, floor).await;
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
const CALL: &str = "rdm.node_rpc.request.update.via-call";
const DIAL: &str = "rdm.node_rpc.connection.create.via-dial";

/// One finished span as the cell's capture exported it.
struct Seen {
    name: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    /// The spans this one follows from (trace id, span id): a dial's link to the call that started it.
    links: Vec<(String, String)>,
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
    let traceparent = rdm_transport::traceparent_of(root).expect("the cell's root span has a trace context: the capture is installed on this thread");
    let mut parts = traceparent.split('-');
    Story { trace_id: parts.nth(1).expect("a traceparent names its trace").into(), span_id: parts.next().expect("a traceparent names its span").into() }
}

/// This cell's own span capture: the product span pipeline (`rdm_transport::span_capture`: its span filter and
/// `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher installed on every thread of the cell's
/// own runtime, so two cells in one test process never share spans.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
    /// True when the runtime's worker threads hold no dispatcher: only the thread that runs the story does.
    bare_workers: bool,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("build-facts-read", exporter.clone());
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
                links: s.links.iter().map(|l| (l.span_context.trace_id().to_string(), l.span_context.span_id().to_string())).collect(),
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
/// `$TMPDIR/i143-testkit/build_facts_read/<cell>.<pid>.spans.json`, whether the cell passed or panicked, so a red
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
        let dir = std::env::temp_dir().join("i143-testkit/build_facts_read");
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

    /// Every span named `name`, each a direct child of the cell's root: the trace the caller of the FetchBuild was in is
    /// the trace of the request the FetchBuild spawned.
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
    let logged = id.log().to_string();
    assert_eq!(held, (node, logged.as_str(), blocker, target, facts.as_str()), "the fetch span's story;\n{}", spans.render());
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
        let b = node_admin_not_ready(&joiner.0.name, &*joiner.0.log, joiner.0.floor.lock().unwrap().clone()).await.unwrap();
        assert!(matches!(&b, NodeAdminNotReady::NoCurrentBuild { wanted: Some(w), .. } if *w == id), "{b:?}");

        let mut h = fetch_build(&joiner, responders(vec![&responder]));
        let seen = drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("fetches the Build");
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
/// CONTRACT (states.md "The check-in checklist", Q276): a primary taking its role reads the accepted
/// Build from the fabric-primary before it counts its fresh checklist: `FetchCurrentBuild`, the start's
/// Build floor read again, owed until answered. Its answer is taken once (`absorbed`), and the primary's
/// own log then holds the fabric-primary's current Build.
#[test]
fn a_primary_taking_its_role_reads_the_fabric_primarys_current_build_once() {
    let cap = capture();
    let (story, id) = story!(cap, "rdm.test.build.update.via-role-taken-read", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let fabric_primary = admin("mesh1.admin.1", &fabric).await;
        holds(&fabric_primary, &id, 2).await;
        let responder = node_of(&fabric_primary).await;
        let taking = caller("mesh2.admin.1", &fabric, &[&responder]).await;
        assert_eq!(taking.0.log.current_build().await.unwrap().into_build_id(), None, "the primary holds no Build before the read");
        let mut h = fetch_build(&taking, responders(vec![&responder]));
        assert!(!h.absorbed().await, "nothing is taken before a read is sent");
        let read = NodeAdminNotReady::NoCurrentBuild { me: taking.0.name.clone(), wanted: None };
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if h.absorbed().await {
                break;
            }
            assert!(Instant::now() < until, "the read was answered within 10 s");
            h.tick(Some(&read)).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!h.absorbed().await, "the answer is taken once");
        assert_eq!(taking.0.log.current_build().await.unwrap().into_build_id(), Some(id.clone()), "the primary's log holds the fabric-primary's current Build");
        assert_eq!(responder.calls.load(Ordering::SeqCst), 1, "one read");
        for n in [&responder, &taking.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    assert_eq!(spans.children(FETCH).len(), 1, "one read, under the role-taking turn;\n{}", spans.render());
    let _ = id;
}

/// `a` holds Build `id` accepted over L4's shape (ten meshes of three node-admins and seventeen rpc
/// nodes: larger than one message whole), attempt 1 converged.
async fn holds_l4(a: &Admin, id: &BuildId) -> FabricTopology {
    use rdm_admin::accepted::MeshTopology;
    use rdm_admin::build::MeshDesired;
    let mut topology = FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap());
    topology.meshes = (1..=10).map(|i| (format!("mesh{i}"), MeshTopology::of(&MeshDesired::of(format!("mesh{i}"), [(rdm_core::NodeKind::NodeAdmin, 3), (rdm_core::NodeKind::RpcNode, 17)])))).collect();
    a.log.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: topology.clone(), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
    a.log.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    a.log.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    topology
}

/// The meshes each acceptance a read carried holds a section of.
fn sections_read(facts: &[rdm_admin::build_state::BuildFact]) -> Vec<Vec<String>> {
    facts.iter().filter_map(|f| match f {
        rdm_admin::build_state::BuildFact::Accepted(a) => Some(a.topology.meshes.keys().cloned().collect()),
        _ => None,
    }).collect()
}

// @feature: node-lifecycle
/// CONTRACT (INDEX Q284: "An admin starting, or a mesh-primary taking its role (Q276), pulls its own
/// mesh's section from the fabric-primary (`FetchBuildSection { build_id, mesh }` on 0x1F, chunked as
/// any fact). Only the fabric-primary, or a successor reading the journal, holds the whole Build"): a
/// responder holding an acceptance larger than one message serves a section read that mesh's section
/// only, of the Build named or of its current one, whole; a facts read every mesh's section; and a
/// responder holding only another mesh's section says its answer is not whole.
#[test]
fn a_section_read_streams_one_meshs_section_of_an_acceptance_too_large_for_one_message() {
    use rdm_admin::build_facts_read::fetch_build_section;
    let cap = capture();
    let (_story, ()) = story!(cap, "rdm.test.build.serve.via-streamed-section", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let primary = admin("mesh1.admin.1", &fabric).await;
        let whole = holds_l4(&primary, &id).await;
        let responder = node_of(&primary).await;
        let c = caller("mesh2.admin.2", &fabric, &[&responder]).await;
        let t = NodeTarget::ExactNode(responder.resolved.node_id.clone());

        for named in [Some(&id), None] {
            let got = fetch_build_section(&c.1.client, &t, named, "mesh2").await.expect("the section read completes");
            assert!(got.complete, "the section asked for is held: the answer is whole");
            assert_eq!(got.build_id, id, "the End names the Build read");
            assert_eq!(sections_read(&got.facts), vec![vec!["mesh2".to_string()]], "mesh2's section only");
            let rdm_admin::build_state::BuildFact::Accepted(a) = &got.facts[0] else { panic!("the acceptance first") };
            assert_eq!(a.topology.meshes["mesh2"], whole.meshes["mesh2"], "the mesh's topology whole");
            assert_eq!(a.topology.mesh_names(), whole.meshes.keys().cloned().collect(), "naming every mesh of the Build");
            assert_eq!(got.facts.len(), 3, "the section, the claim and the receipt");
        }
        let all = fetch_build_facts(&c.1.client, &t, &id).await.expect("the facts read completes");
        assert!(all.complete);
        assert_eq!(sections_read(&all.facts).len(), 10, "every mesh's section, never the whole");

        // An admin holding only mesh1's section is asked for mesh2's.
        let other = admin("mesh1.admin.2", &fabric).await;
        other.log.absorb_facts(&fetch_build_section(&c.1.client, &t, Some(&id), "mesh1").await.unwrap().facts).await;
        let partial = node_of(&other).await;
        c.1.resolver.insert(partial.resolved.clone());
        let got = fetch_build_section(&c.1.client, &NodeTarget::ExactNode(partial.resolved.node_id.clone()), Some(&id), "mesh2").await;
        assert!(got.as_ref().map_or(true, |g| !g.complete && sections_read(&g.facts).is_empty()), "no section of mesh2 held: not whole");
        for n in [&responder, &partial, &c.1] {
            n.ep.close().await;
        }
    });
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

        let mut h = fetch_build(&joiner, responders(vec![&first, &second]));
        drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("fetches from the second");
        assert_eq!(first.calls.load(Ordering::SeqCst), 2, "the first responder was asked (once by the read above, once by the FetchBuild) and passed over");
        assert_eq!(second.calls.load(Ordering::SeqCst), 1, "the second responder's full holdings supplied it");
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
    assert_eq!((incomplete[0].attr("target"), incomplete[0].attr("build_id")), ("mesh1.admin.1", id.log().to_string().as_str()));
}

// @feature: node-lifecycle
/// CONTRACT: a stream that ends before its `End` is never a fetched Build: nothing is absorbed, the blocker
/// stands, the target is left alone for its backoff, and the same request is made again from the
/// beginning and then counts. Its trace: the cut read is a call span under the cell's root, the
/// FetchBuild's repeated read is one fetch span (three facts) holding its own call.
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
                        let mut sink = sink.started(BuildFactsReply::Started).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                        let (messages, _) = rdm_admin::fabric_builds::encode_chunks(full_facts(&door).await);
                        sink.data(BuildFactsReply::Facts { build_id: id_of(&req), chunk_index: 0, chunk_count: messages.len() as u32 + 1, facts: messages[0].to_vec() })
                            .await
                            .map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                        return Err(rdm_transport::HandlerFault::invariant_broken("the stream is cut"));
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

        let mut h = fetch_build(&joiner, responders(vec![&responder]));
        drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("the repeated read fetches the Build");
        assert_eq!(asked.load(Ordering::SeqCst), 2, "the cut read, then the read made again");
        assert_eq!(joiner.0.log.current_build().await.unwrap().into_build_id(), Some(id.clone()));
        for n in [&responder, &joiner.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert_eq!(fetches.len(), 1, "the FetchBuild read once; the cut read was the cell's own;\n{}", spans.render());
    assert_fetch(&spans, fetches[0], "mesh1.admin.2", &id, "no-current-build-wanted", "mesh1.admin.1", Some(3));
    assert_eq!(spans.named(CALL).len(), 2, "the cut read's call and the fetch's call;\n{}", spans.render());
}

async fn full_facts(door: &BuildFactsDoor) -> Vec<rdm_admin::build_state::BuildFact> {
    door.builds.facts().await.unwrap()
}

fn id_of(req: &rdm_core::build_facts::BuildFactsRequest) -> String {
    match req {
        rdm_core::build_facts::BuildFactsRequest::FetchBuildFacts { build_id } => build_id.clone(),
        other => panic!("this responder is read by FetchBuildFacts only, asked {}", other.op()),
    }
}

/// A responder's Build state whose current accepted Build moves once it has been read: the first
/// `current_build` answers the store's current Build and then a newer Build, `newer`, is accepted
/// into the store, so every read of the pointer after the first (between any two chunks of the
/// stream it selected) finds `newer`.
struct PointerMovesOnceRead {
    inner: Arc<MemoryBuildStateAdapter>,
    newer: BuildId,
    moved: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl BuildStateAdapter for PointerMovesOnceRead {
    async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.publish_accepted(a).await
    }
    async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.open_attempt(o).await
    }
    async fn read_build(&self, id: &BuildId) -> Result<rdm_admin::build_state::BuildProjection, rdm_admin::build_state::BuildStateError> {
        self.inner.read_build(id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
        let current = self.inner.current_build().await?;
        if !self.moved.swap(true, Ordering::SeqCst) {
            let newer = BuildAccepted { build_id: self.newer.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 1 };
            self.inner.publish_accepted(&newer).await?;
        }
        Ok(current)
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<rdm_admin::build_state::BuildProjection>, rdm_admin::build_state::BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<rdm_admin::build_state::ClaimOutcome, rdm_admin::build_state::BuildStateError> {
        self.inner.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &rdm_admin::build_state::BuildStepReceipt) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.append_step_receipt(r).await
    }
    async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.append_attempt_receipt(r).await
    }
    async fn facts(&self) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        self.inner.facts().await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), rdm_admin::build_state::BuildStateError> {
        self.inner.forget(id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, rdm_admin::build_state::BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, rdm_admin::build_state::BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, rdm_admin::build_state::BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
}

// @feature: node-lifecycle
/// CONTRACT (node-rpc-envelope.md "Build-facts, op `0x1F`": "the responder selects its current
/// accepted Build once for the request and streams that Build's facts, every chunk and End naming
/// its id"): a `FetchCurrentBuild` whose responder's current accepted Build moves to a newer Build
/// once the read has selected it streams the Build it selected to the end: more than one chunk,
/// every chunk and the End naming that Build, only its facts, and the End complete. Must NOT
/// happen: a chunk or the End naming the Build the pointer moved to, or facts of two Builds in one
/// stream. Its trace: the read is one call span under the cell's root, and the responder's serve
/// span names the request and the Build it selected.
#[test]
fn a_current_build_read_streams_the_build_it_selected_to_the_end() {
    let cap = capture();
    let (story, (selected, newer)) = story!(cap, "rdm.test.build.serve.via-current-build-selected-once", async {
        let fabric = FabricId::mint();
        let (selected, newer) = (BuildId::mint(), BuildId::mint());
        let full = admin("mesh1.admin.1", &fabric).await;
        holds(&full, &selected, 60).await;
        let moving = Arc::new(PointerMovesOnceRead { inner: full.log.clone(), newer: newer.clone(), moved: std::sync::atomic::AtomicBool::new(false) });
        let door = Arc::new(BuildFactsDoor { me: full.name.clone(), builds: moving.clone(), floor: full.floor.clone() });
        let responder = node_serving("mesh1.admin.1", move |req, sink| {
            let door = door.clone();
            async move { Ok(door.serve(req, sink).await) }
        })
        .await;
        let c = caller("mesh1.admin.2", &fabric, &[&responder]).await;
        let t = NodeTarget::ExactNode(responder.resolved.node_id.clone());
        let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Overall(Duration::from_secs(5)), ..Default::default() };
        let mut stream = match c.1.client.call_stream::<BuildFacts>(&t, &rdm_core::build_facts::BuildFactsRequest::FetchCurrentBuild, &opts).await {
            Ok((s, _)) => s.read_only(),
            Err((outcome, _)) => panic!("the FetchCurrentBuild read was not streamed: {outcome:?}"),
        };
        let (mut chunks, mut end, mut facts) = (Vec::new(), None, Vec::new());
        while let Some(item) = stream.next().await {
            match item {
                rdm_transport::stream::StreamItem::Frame(_, BuildFactsReply::Started) => {}
                rdm_transport::stream::StreamItem::Frame(_, BuildFactsReply::Facts { build_id, facts: bytes, .. }) => {
                    chunks.push(build_id);
                    facts.extend(rdm_admin::fabric_builds::BuildMessage::from_bytes(&bytes).expect("a chunk decodes").facts);
                }
                rdm_transport::stream::StreamItem::Frame(_, BuildFactsReply::End { build_id, complete, .. }) => end = Some((build_id, complete)),
                rdm_transport::stream::StreamItem::Frame(_, other) => panic!("an unexpected frame in the stream: {other:?}"),
                rdm_transport::stream::StreamItem::Failed(f) => panic!("the stream failed before its End: {f:?}"),
            }
        }
        assert_eq!(full.log.current_build().await.unwrap().into_build_id(), Some(newer.clone()), "the responder's current accepted Build moved once the read selected its Build");
        assert!(chunks.len() >= 2, "the selected Build's facts take more than one chunk: {} arrived", chunks.len());
        let named: std::collections::BTreeSet<&String> = chunks.iter().collect();
        assert_eq!(named.into_iter().cloned().collect::<Vec<_>>(), vec![selected.to_string()], "every chunk names the Build the read selected, never the Build the pointer moved to ({newer})");
        assert_eq!(end, Some((selected.to_string(), true)), "the End names the Build the read selected, complete");
        assert!(facts.iter().all(|f| *f.build_id() == selected), "only the selected Build's facts are streamed");
        for n in [&responder, &c.1] {
            n.ep.close().await;
        }
        (selected, newer)
    });
    let spans = cap.spans(story);
    let serves = spans.named("rdm.node_admin.build.serve.via-fetch-facts");
    assert_eq!(serves.len(), 1, "one serve;\n{}", spans.render());
    assert_eq!(
        (serves[0].attr("request"), serves[0].attr("build_id"), serves[0].attr("outcome")),
        ("fetch-current-build", selected.to_string().as_str(), "served"),
        "the serve span names the request and the Build it selected, never {newer};\n{}",
        spans.render()
    );
}

// @feature: node-lifecycle
/// CONTRACT: the fabric-primary is asked first. When it cannot answer (it refuses NotReady, or its
/// peer does not resolve), the next known node-admin is asked, and its holdings supply the joiner. The
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
        let mut h = fetch_build(&joiner, responders(vec![&not_ready, &second]));
        drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("fetches from the second");
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
        let mut h2 = fetch_build(&joiner2, responders);
        drive(&joiner2.0, &mut h2, Duration::from_secs(10)).await.expect("fetches past the unresolvable fabric-primary");
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
        let mut h = fetch_build(&restarted, responders(vec![&responder]));
        let seen = drive(&restarted.0, &mut h, Duration::from_secs(10)).await.expect("fetches the Build");
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
/// CONTRACT: a FetchBuild turn that runs on a runtime whose worker threads hold no dispatcher (the runtime a
/// `#[tokio::test(flavor = "multi_thread")]` cell gets: only the thread that called `block_on` holds the
/// capture) still has its whole story captured and ended when the story ends: the fetch it spawns runs on
/// the dispatcher of the turn that started it, the dial its call starts ends on the call's dispatcher, and
/// the endpoints the story bound hold none of its spans. Must NOT happen: a span of the story lost because
/// a worker thread picked its task up, or held open past the story by a task of the transport. Its trace:
/// the cell's root ended; the one fetch span is its child, naming the blocker and the six facts it read;
/// the fetch holds its one call, and its one dial, a root of its own trace, is linked (follows_from) to that call.
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
        let mut h = fetch_build(&restarted, responders(vec![&responder]));
        drive(&restarted.0, &mut h, Duration::from_secs(10)).await.expect("fetches the Build");
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
    let dials: Vec<&Seen> = spans.named(DIAL).into_iter().filter(|d| d.links.contains(&(call.trace_id.clone(), call.span_id.clone()))).collect();
    assert_eq!(dials.len(), 1, "the call's one dial is linked to it (follows_from), a root of its own trace;\n{}", spans.render());
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
        let mut h = fetch_build(&joiner, responders(vec![&first, &second]));
        let seen = drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("fetches the Build");
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
/// CONTRACT (node-rpc-envelope.md "Build-facts, op `0x1F`": "no-pointer, which names no Build, reads
/// `FetchCurrentBuild` [...] or answers `NoCurrentBuild` and the blocker stays"): a joiner whose entry
/// named no Build holds no current Build and names the no-pointer blocker (`no-current-build`). It
/// reads it with `FetchCurrentBuild`: while the responder holds no current Build the read is answered
/// `NoCurrentBuild`, nothing is absorbed and the blocker stays on the responder's backoff; once the
/// responder holds one, the read streams that Build's facts, whose id the End names, they are
/// absorbed and the blocker clears. Its trace: every read is a fetch span under the cell's root for
/// `no-current-build`, the first ending `no-current-build` beside a `via-fetch-failed` rejection of
/// that reason, the last ending `absorbed` and naming the Build and its three facts.
#[test]
fn a_build_no_entry_named_is_read_as_the_responders_current_build() {
    let cap = capture();
    let (story, id) = story!(cap, "rdm.test.build.update.via-current-build-read", async {
        let fabric = FabricId::mint();
        let id = BuildId::mint();
        let full = admin("mesh1.admin.1", &fabric).await;
        let responder = node_of(&full).await;
        let joiner = caller("mesh1.admin.2", &fabric, &[&responder]).await;
        let b = node_admin_not_ready(&joiner.0.name, &*joiner.0.log, None).await.unwrap();
        assert!(matches!(&b, NodeAdminNotReady::NoCurrentBuild { wanted: None, .. }), "{b:?}");

        let mut h = fetch_build(&joiner, responders(vec![&responder]));
        let stays = drive(&joiner.0, &mut h, Duration::from_millis(600)).await;
        assert_eq!(stays, Err(vec!["no-current-build"]), "the responder holds no current Build: the blocker stays");
        assert!(joiner.0.log.facts().await.unwrap().is_empty(), "a NoCurrentBuild answer absorbs nothing");
        let refused = responder.calls.load(Ordering::SeqCst);
        assert!(refused >= 1, "the blocker was read while the responder held no current Build");

        holds(&full, &id, 1).await;
        let seen = drive(&joiner.0, &mut h, Duration::from_secs(10)).await.expect("reads the responder's current Build once it holds one");
        assert_eq!(seen, vec!["no-current-build"], "the one blocker the joiner named");
        assert_eq!(joiner.0.log.current_build().await.unwrap().into_build_id(), Some(id.clone()), "the Build read is current");
        for n in [&responder, &joiner.1] {
            n.ep.close().await;
        }
        id
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert!(fetches.len() >= 2, "a refused read, then the read that streamed the Build;\n{}", spans.render());
    let (first, last) = (fetches[0], fetches[fetches.len() - 1]);
    assert_eq!((first.attr("blocker"), first.attr("outcome"), first.attr("build_id")), ("no-current-build", "no-current-build", "<absent>"), "{}", spans.render());
    assert_fetch(&spans, last, "mesh1.admin.2", &id, "no-current-build", "mesh1.admin.1", Some(3));
    assert_eq!(last.attr("outcome"), "absorbed", "{}", spans.render());
    let failed = spans.children(FETCH_FAILED);
    assert!(!failed.is_empty() && failed.iter().all(|f| (f.attr("target"), f.attr("reason")) == ("mesh1.admin.1", "no-current-build")), "{}", spans.render());
}

/// A responder whose `FetchCurrentBuild` stream names two Builds: `chunks` names the Build of each
/// chunk in order, `end` the Build its End names. Each chunk carries real facts of the Build it names.
async fn two_builds_responder(name: &str, chunks: Vec<BuildId>, end: BuildId) -> Node {
    let mut frames: Vec<(String, Vec<u8>)> = Vec::new();
    for b in &chunks {
        let a = admin("mesh1.admin.9", &FabricId::mint()).await;
        holds(&a, b, 1).await;
        let (messages, _) = rdm_admin::fabric_builds::encode_chunks(a.log.facts().await.unwrap());
        frames.push((b.to_string(), messages[0].to_vec()));
    }
    let frames = Arc::new(frames);
    let end = end.to_string();
    node_serving(name, move |_, sink| {
        let (frames, end) = (frames.clone(), end.clone());
        async move {
            fn fault(e: impl std::fmt::Debug) -> rdm_transport::HandlerFault {
                rdm_transport::HandlerFault::invariant_broken(format!("{e:?}"))
            }
            let mut sink = sink.started(BuildFactsReply::Started).await.map_err(fault)?;
            let count = frames.len() as u32;
            for (i, (b, facts)) in frames.iter().enumerate() {
                sink.data(BuildFactsReply::Facts { build_id: b.clone(), chunk_index: i as u32, chunk_count: count, facts: facts.clone() }).await.map_err(fault)?;
            }
            Ok(BuildFactsReply::End { build_id: end, facts: 3 * count, chunks: count, complete: true })
        }
    })
    .await
}

// @feature: node-lifecycle
/// CONTRACT (Q98: "the caller refuses a stream whose chunks name two Builds, as an incomplete
/// fetch"): a `FetchCurrentBuild` stream whose chunks name two Builds, or whose End names a Build its
/// chunks do not, is never a fetched Build: the read fails `incomplete`, nothing of either Build is
/// absorbed and the no-pointer blocker stays on the responder's backoff. Must NOT happen: facts of
/// two Builds absorbed from one stream. Its trace: every read of the blocker is a fetch span under
/// the cell's root ending `incomplete`, beside a `via-fetch-failed` rejection of that reason.
#[test]
fn a_current_build_stream_naming_two_builds_is_refused_as_incomplete() {
    let cap = capture();
    let (story, ()) = story!(cap, "rdm.test.build.reject.via-two-builds-in-one-stream", async {
        let fabric = FabricId::mint();
        let (a, b) = (BuildId::mint(), BuildId::mint());
        let mixed_chunks = two_builds_responder("mesh1.admin.1", vec![a.clone(), b.clone()], a.clone()).await;
        let mixed_end = two_builds_responder("mesh1.admin.3", vec![a.clone()], b.clone()).await;
        let joiner = caller("mesh1.admin.2", &fabric, &[&mixed_chunks, &mixed_end]).await;

        for (n, how) in [(&mixed_chunks, "its chunks name two Builds"), (&mixed_end, "its End names a Build its chunks do not")] {
            let read = fetch_current_build(&joiner.1.client, &NodeTarget::ExactNode(n.resolved.node_id.clone())).await;
            assert!(matches!(&read, Err(FetchFailure::Incomplete(_))), "a stream where {how} is refused as incomplete: {read:?}");
        }

        let mut h = fetch_build(&joiner, responders(vec![&mixed_chunks, &mixed_end]));
        let stays = drive(&joiner.0, &mut h, Duration::from_millis(600)).await;
        assert_eq!(stays, Err(vec!["no-current-build"]), "the blocker stays");
        assert!(joiner.0.log.facts().await.unwrap().is_empty(), "nothing of either Build is absorbed");
        for n in [&mixed_chunks, &mixed_end, &joiner.1] {
            n.ep.close().await;
        }
    });
    let spans = cap.spans(story);
    let fetches = spans.children(FETCH);
    assert!(fetches.len() >= 2, "each responder was read;\n{}", spans.render());
    assert!(fetches.iter().all(|f| (f.attr("blocker"), f.attr("outcome")) == ("no-current-build", "incomplete")), "{}", spans.render());
    let failed = spans.children(FETCH_FAILED);
    assert_eq!(failed.len(), fetches.len(), "{}", spans.render());
    assert!(failed.iter().all(|f| f.attr("reason") == "incomplete"), "{}", spans.render());
}

#[test]
fn the_backoff_is_bounded() {
    use rdm_admin::fetch_build::{BACKOFF_BASE, BACKOFF_CAP};
    assert!(BACKOFF_BASE < BACKOFF_CAP);
    assert!(BACKOFF_CAP <= Duration::from_secs(5));
}
