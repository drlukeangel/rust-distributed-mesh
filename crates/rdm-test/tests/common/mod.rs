//! The deployment smoke shared by every provider (i143.e2.s3 process,
//! i143.e2.s4 container): a node is deployed through the create
//! `DeploymentPipeline`, every step leaves a receipt, and every step runs as a
//! child span of the pipeline span carrying `step`, `build_id`, `provider`,
//! `outcome`, `attempt` and `elapsed_ms`.
//!
//! The admin side is in-process (a gossip seed plus a Node RPC client); the
//! node is a real `rdm-rpc-node` runtime started by the provider the
//! `MESH_SPAWN_TYPE` value selects.
//!
//! The pieces (admin side, observer, sink, span capture) are shared with the
//! e2.s5 re-run and delete tests.

#![allow(dead_code)]

pub mod admin_view;

/// `(unwritten, handed)` when a process's exit ended its evidence flush at its bound and named its loss
/// on stderr (i146 Q42; Luke 2026-10-17: the bound is the contract, a named, bounded loss).
pub fn named_loss(stderr: &str) -> Option<(usize, usize)> {
    let line = stderr.lines().find(|l| l.contains("the evidence flush ended at its bound"))?;
    let (_, tail) = line.split_once("): ")?;
    let mut words = tail.split_whitespace();
    let unwritten = words.next()?.parse().ok()?;
    let handed = words.nth(1)?.parse().ok()?;
    Some((unwritten, handed))
}

/// The loss a process named whole at its exit's bound, when its span file holds `written` spans: some
/// spans named unwritten, and with the written ones they account for every span it handed.
pub fn loss_named_whole(stderr: &str, written: usize) -> Option<(usize, usize)> {
    named_loss(stderr).filter(|(unwritten, handed)| *unwritten > 0 && written + unwritten >= *handed)
}

/// The probe's `via-probe` root span from its span files in `evidence`, or `None` when the probe's exit
/// ended its evidence flush at its bound with the loss named whole on `stderr` (i146 Q42, Q60: the bound is
/// the contract, a named, bounded loss; the span file is opened by its first export, so a flush that ended
/// before any export left none). Any other absence panics with what the directory held.
pub fn probe_root_span_or_named_loss(evidence: &std::path::Path, stderr: &str) -> Option<serde_json::Value> {
    let files: Vec<std::path::PathBuf> = std::fs::read_dir(evidence)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("rdm-rpc-probe.") && n.to_string_lossy().ends_with(".spans.jsonl")))
        .collect();
    let spans: Vec<serde_json::Value> = files.iter().flat_map(|p| std::fs::read_to_string(p).unwrap_or_default().lines().filter_map(|l| serde_json::from_str(l).ok()).collect::<Vec<_>>()).collect();
    if let Some(root) = spans.iter().find(|s| s["name"] == "rdm.node_rpc.proof_store.resolve.via-probe") {
        return Some(root.clone());
    }
    match loss_named_whole(stderr, spans.len()) {
        Some((unwritten, handed)) => {
            println!("the probe's exit flush ended at its bound: {} written and {unwritten} named unwritten of {handed} handed", spans.len());
            None
        }
        None => panic!("the probe left no via-probe span and named no loss whole: {} spans in {files:?}: {spans:?}; stderr: {stderr}", spans.len()),
    }
}

#[cfg(test)]
mod probe_root_span_tests {
    use super::probe_root_span_or_named_loss as read;

    fn empty_dir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("rdm-probe-root-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// A probe whose exit flush ended at its bound before its first export left no span file and named the loss.
    #[test]
    fn exit_flush_ended_at_its_bound_before_any_export_is_a_named_loss() {
        let d = empty_dir("loss");
        let stderr = "telemetry: the evidence flush ended at its bound (2000 ms): 3 of 3 spans not confirmed written to the evidence file\n";
        assert!(read(&d, stderr).is_none());
        let _ = std::fs::remove_dir_all(&d);
    }

    /// No span file and no loss named is still a failure.
    #[test]
    #[should_panic(expected = "named no loss whole")]
    fn no_span_file_and_no_loss_named_is_refused() {
        let d = empty_dir("none");
        read(&d, "");
    }

    /// The root span in a span file is returned.
    #[test]
    fn root_span_in_the_file_is_returned() {
        let d = empty_dir("root");
        std::fs::write(d.join("rdm-rpc-probe.1.spans.jsonl"), "{\"name\":\"rdm.node_rpc.proof_store.resolve.via-probe\",\"trace_id\":\"t\"}\n").unwrap();
        assert_eq!(read(&d, "").unwrap()["trace_id"], "t");
        let _ = std::fs::remove_dir_all(&d);
    }
}

use rdm_core::{FabricId, IncarnationId, NodeId};
use rdm_admin::accepted::{FabricTopology, MeshTopology};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::deployment::endpoint::RPC_NODE;
use rdm_admin::deployment::pipeline::{
    CreateRequest, CreateStep, DeploymentPipeline, LaunchTemplate, Timeouts, TopologySink,
};
use rdm_admin::deployment::provider::{DeployError, DeploymentStatus, FabricPolicy, TerminationMode};
use rdm_admin::model::{Node, NodeStatus, PathName};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

/// Every span: name, parent name, and its recorded fields, keyed by the sequence number of its
/// creation; the third part ticks whenever a span is created or a field recorded, so a cell awaits a
/// span fact instead of looking again and again; the fourth holds, per span key, the sequence number
/// of its close, from the same counter, so a cell reads whether one span closed before another began.
#[derive(Clone, Default)]
pub struct Spans(pub Arc<Mutex<HashMap<u64, (String, Option<String>, BTreeMap<String, String>)>>>, Arc<Mutex<(u64, HashMap<u64, u64>)>>, Arc<tokio::sync::watch::Sender<u64>>, pub Arc<Mutex<HashMap<u64, u64>>>);

impl Spans {
    /// Resolves once a span named `name` holds fields `wanted` says yes to.
    pub async fn recorded(&self, name: &str, wanted: impl Fn(&BTreeMap<String, String>) -> bool) {
        let mut ticks = self.2.subscribe();
        loop {
            let held = self.0.lock().unwrap().values().any(|(n, _, f)| n == name && wanted(f));
            if held {
                return;
            }
            ticks.changed().await.expect("the capture is held by the cell");
        }
    }
}

struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl tracing::field::Visit for Fields<'_> {
    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
        self.0.insert(f.name().into(), v.into());
    }
    fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
        self.0.insert(f.name().into(), v.to_string());
    }
    fn record_i64(&mut self, f: &tracing::field::Field, v: i64) {
        self.0.insert(f.name().into(), v.to_string());
    }
    fn record_bool(&mut self, f: &tracing::field::Field, v: bool) {
        self.0.insert(f.name().into(), v.to_string());
    }
    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        self.0.insert(f.name().into(), format!("{v:?}"));
    }
}

impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> tracing_subscriber::Layer<S> for Spans {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        let parent = ctx.span(id).and_then(|s| s.parent().map(|p| p.name().to_string()));
        // The registry recycles a closed span's id: a span is kept under its own sequence number,
        // never under an id a later span may take.
        let key = {
            let mut live = self.1.lock().unwrap();
            live.0 += 1;
            let key = live.0;
            live.1.insert(id.into_u64(), key);
            key
        };
        self.0.lock().unwrap().insert(key, (attrs.metadata().name().into(), parent, fields));
        self.2.send_modify(|t| *t += 1);
    }
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let key = self.1.lock().unwrap().1.get(&id.into_u64()).copied();
        if let Some(key) = key {
            if let Some((_, _, fields)) = self.0.lock().unwrap().get_mut(&key) {
                values.record(&mut Fields(fields));
            }
            self.2.send_modify(|t| *t += 1);
        }
    }
    fn on_close(&self, id: tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
        let closed = {
            let mut live = self.1.lock().unwrap();
            live.1.remove(&id.into_u64()).map(|key| {
                live.0 += 1;
                (key, live.0)
            })
        };
        if let Some((key, at)) = closed {
            self.3.lock().unwrap().insert(key, at);
        }
    }
}

/// How many threads the runtime of a [`Capture::run`] has: the flavor the cell's own test needs.
#[derive(Clone, Copy, Debug)]
pub enum Threads {
    /// One thread: everything the cell starts (admin side, in-process node, transport) shares it.
    Current,
    /// A multi-thread runtime with this many workers.
    Workers(usize),
}

thread_local! {
    /// The artifact dir of the [`Capture::run`] driving this thread: where [`template`] points the
    /// runtimes a cell launches (`RDM_EVIDENCE_DIR`), so their spans land beside the cell's.
    static ARTIFACT_DIR: std::cell::RefCell<Option<std::path::PathBuf>> = const { std::cell::RefCell::new(None) };
}

/// The one span capture of a testkit cell. Every span the cell's runtime emits, on whichever of its
/// threads, goes to two layers of one subscriber: [`Spans`] (the name/parent/field map the cell's
/// assertions read) and the product span pipeline (`rdm_transport::span_capture`: product's
/// filter and its `BatchSpanProcessor`) in front of the JSONL evidence exporter, at
/// `$TMPDIR/i143-testkit/<cell>/testkit.<pid>-<ns>.spans.jsonl`. A process the cell launches
/// through [`template`] writes its own JSONL into the same dir. The capture is flushed (bounded) when
/// it drops, so a cell that panics leaves its trace.
pub struct Capture {
    pub spans: Spans,
    pub dir: std::path::PathBuf,
    dispatch: tracing::Dispatch,
    capture: rdm_transport::SpanCapture,
}

/// The capture of the cell named `cell` (its test function's name).
pub fn capture(cell: &str) -> Capture {
    let dir = std::env::temp_dir().join("i143-testkit").join(cell);
    let exporter = rdm_transport::jsonl_span_exporter(&dir, "testkit").unwrap_or_else(|e| panic!("the span artifact dir {} is unusable: {e}", dir.display()));
    let spans = Spans::default();
    let (capture, layer) = rdm_transport::span_capture(cell, exporter);
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(spans.clone()).with(layer));
    Capture { spans, dir, dispatch, capture }
}

impl Capture {
    /// Run `f` to completion on a runtime of its own whose every thread (and the calling one)
    /// dispatches to this capture.
    pub fn run<F: std::future::Future>(&self, threads: Threads, f: F) -> F::Output {
        let mut builder = match threads {
            Threads::Current => tokio::runtime::Builder::new_current_thread(),
            Threads::Workers(n) => {
                let mut b = tokio::runtime::Builder::new_multi_thread();
                b.worker_threads(n);
                b
            }
        };
        let d = self.dispatch.clone();
        let rt = builder.enable_all().on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d))).build().expect("the cell's runtime");
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        ARTIFACT_DIR.with(|a| *a.borrow_mut() = Some(self.dir.clone()));
        let out = rt.block_on(f);
        ARTIFACT_DIR.with(|a| *a.borrow_mut() = None);
        out
    }

    /// Every span ended before this call has reached the JSONL artifact when it returns `Ok`.
    pub fn flush(&self) -> Result<(), String> {
        self.capture.flush(Duration::from_secs(10))
    }
}

impl Drop for Capture {
    /// Never panics: a panic here while the cell unwinds would abort and lose the trace.
    fn drop(&mut self) {
        match self.flush() {
            Ok(()) => eprintln!("spans of this cell: {}", self.dir.display()),
            Err(e) => eprintln!("spans of this cell ({}) did not all reach the artifact: {e}", self.dir.display()),
        }
    }
}

/// A runtime the cell launched through [`template`] stopped and left its trace: its JSONL file in
/// the cell's artifact dir (`rdm-rpc-node.<pid>-<ns>.spans.jsonl`) holds its boot span and the
/// span of the stop it took. A runtime killed before its batch export leaves the file empty.
pub fn assert_launched_node_stopped_with_its_spans(dir: &std::path::Path, pid: u32) {
    let prefix = format!("rdm-rpc-node.{pid}-");
    let file = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("the cell's artifact dir {} is unreadable: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(&prefix) && n.ends_with(".spans.jsonl")))
        .unwrap_or_else(|| panic!("runtime {pid} left no span file in {}: RDM_EVIDENCE_DIR never reached it", dir.display()));
    let names: Vec<String> = std::fs::read_to_string(&file)
        .unwrap()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v["name"].as_str().map(str::to_string))
        .collect();
    for want in ["rdm.mesh.node.create.via-deployment", "rdm.mesh.node.delete.via-signal"] {
        assert!(names.iter().any(|n| n == want), "runtime {pid} stopped without {want} in {} ({} spans: {names:?})", file.display(), names.len());
    }
}

/// Every record published, and every name removed, in order.
#[derive(Default)]
pub struct Published {
    pub nodes: Mutex<Vec<Node>>,
    pub removed: Mutex<Vec<PathName>>,
    /// The exact births removed from their path, in order.
    pub removed_births: Mutex<Vec<(PathName, NodeId, IncarnationId)>>,
}
impl TopologySink for Published {
    fn publish(&self, node: Node) {
        self.nodes.lock().unwrap().push(node);
    }
    fn remove(&self, name: &PathName) {
        self.removed.lock().unwrap().push(name.clone());
    }
    fn remove_birth(&self, name: &PathName, node_id: &NodeId, incarnation: &IncarnationId) {
        self.removed_births.lock().unwrap().push((name.clone(), node_id.clone(), incarnation.clone()));
    }
}

pub use rdm_test::rig::*;

/// A launch template for `rdm-rpc-node` whose launcher is `admin`: its seed, its fabric and its
/// data root, so every node launched from it is stopped when `admin` drops.
pub fn template(admin: &AdminSide) -> LaunchTemplate {
    let data_root = admin.data_root().to_path_buf();
    // The fabric's one store root, handed to every launch (R14).
    let store_dir = data_root.join("store");
    std::fs::create_dir_all(&store_dir).expect("the store root");
    LaunchTemplate { fabric: "fabric1".into(),
        fabric_id: admin.fabric.clone(),
        executable: env!("CARGO_BIN_EXE_rdm-rpc-node").into(),
        seeds: vec![admin.seed.clone()],
        launcher: admin.launcher.clone(),
        env: [(rdm_core::launch::ENV_MESH_ID.to_string(), TEST_MESH_ID.to_string())]
            .into_iter()
            .chain(ARTIFACT_DIR.with(|a| a.borrow().clone()).map(|d| ("RDM_EVIDENCE_DIR".to_string(), d.display().to_string())))
            .collect(),
        data_root,
        store_dir,
        issuing: None,
        roots: Default::default(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
        binding: None,
    }
}

/// Accept one Build of `topology` and return its id.
pub async fn publish_build(builds: &MemoryBuildStateAdapter, topology: FabricTopology) -> BuildId {
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology, submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    build_id
}

/// mesh1 with one admin and one rpc node: what the smoke deploys toward.
pub fn add_node() -> FabricTopology {
    FabricTopology { pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), MeshTopology::of(&rdm_admin::build::MeshDesired::of("mesh1", [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 1)])))].into() }
}

/// What `deploy_through_every_step` found.
pub enum Smoke {
    Passed,
    /// The host cannot run the provider; the reason is named.
    Unsupported(String),
}

/// The e2.s3 smoke for the provider `spawn_type` names (the
/// `MESH_SPAWN_TYPE` value): one rpc node deployed through every create
/// step, every receipt present, every step span attributed.
/// Removes every container and the network of one fabric when dropped, however the test ends.
pub struct ContainerFabricCleanup(pub String);

impl Drop for ContainerFabricCleanup {
    fn drop(&mut self) {
        let docker = |args: &[&str]| std::process::Command::new("docker").args(args).output().ok().map(|o| String::from_utf8_lossy(&o.stdout).to_string()).unwrap_or_default();
        for id in docker(&["ps", "-aq", "--filter", &format!("label=rafka.fabric={}", self.0)]).split_whitespace() {
            // Its output is the evidence of a failed smoke: kept before the container goes.
            if let Ok(o) = std::process::Command::new("docker").args(["logs", id]).output() {
                let path = std::env::temp_dir().join(format!("{}-{id}.container.log", self.0));
                if std::fs::write(&path, [&o.stdout[..], &o.stderr[..]].concat()).is_ok() {
                    eprintln!("container {id} of fabric {}: log kept at {}", self.0, path.display());
                }
            }
            docker(&["rm", "-f", id]);
        }
        docker(&["network", "rm", &format!("rafka-{}", self.0)]);
    }
}

/// `spans` is the cell's own capture ([`Capture::spans`]): the smoke runs inside its [`Capture::run`].
pub async fn deploy_through_every_step(spawn_type: &str, spans: &Spans) -> Smoke {

    let fabric_id = FabricId::mint();
    let fabric = format!("fab-{fabric_id}");
    // A failed smoke leaves nothing running: every container of the fabric and its network go.
    let _cleanup = (spawn_type == "container").then(|| ContainerFabricCleanup(fabric.clone()));
    let policy = FabricPolicy::bootstrap(Some(spawn_type)).expect("a known MESH_SPAWN_TYPE");
    let prepared = match rdm_admin::deployment::prepare(policy, &fabric).await {
        Ok(p) => p,
        Err(DeployError::Unsupported { reason, .. }) => return Smoke::Unsupported(reason),
        Err(e) => panic!("preparing the {spawn_type} provider failed: {e}"),
    };
    let provider = prepared.provider.clone();
    let expected_provider = format!("{:?}", provider.kind());

    // The admin side, on the address the provider's runtimes can reach.
    let admin = admin_side(prepared.admin_ip, &fabric_id).await;
    let observer = &admin.observer;
    let template = template(&admin);
    let data_root = template.data_root.clone();
    let builds = MemoryBuildStateAdapter::new();
    let build_id = publish_build(&builds, add_node()).await;
    let node_name = "mesh1.rpc.1".parse().unwrap();
    let sink = Published::default();
    let pipeline = DeploymentPipeline {
        provider: &*provider,
        joins: &admin.joins,
        observer,
        sink: &sink,
        lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };

    let created = pipeline
        .create(&CreateRequest { build_id: build_id.clone(), attempt: 1, node: node_name, spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None })
        .await;
    let created = match created {
        Ok(c) => c,
        Err(e) => panic!("pipeline failed: {e}"),
    };

    // The node is live, published Pending then Ready, at the assigned endpoints.
    assert_eq!(provider.inspect(&created.handle).await, DeploymentStatus::Running);
    let published = sink.nodes.lock().unwrap().clone();
    assert_eq!(published.iter().map(|n| n.status).collect::<Vec<_>>(), vec![NodeStatus::Pending, NodeStatus::Ready]);
    let reported = created.node.transport_addr.expect("a created node carries the address it reported");
    assert!(reported.port() != 0 && !reported.ip().is_unspecified(), "{reported} is not an address a peer can dial");

    // A container node lives in its own network namespace, at its own
    // address on the fabric network: nothing on the host holds its ports.
    if let Some(c) = &prepared.container {
        let pid = created.handle.pid.expect("a container handle carries its init pid");
        // /proc/<pid>/net/* shows the namespace of <pid> and is readable by
        // any user (the ns/net link of another user's process is not).
        let interfaces = |p: &str| -> Vec<String> {
            std::fs::read_to_string(format!("/proc/{p}/net/dev"))
                .unwrap()
                .lines()
                .skip(2)
                .filter_map(|l| l.split(':').next().map(|i| i.trim().to_string()))
                .collect()
        };
        let mut node_ifs = interfaces(&pid.to_string());
        node_ifs.sort();
        assert_eq!(node_ifs, vec!["eth0".to_string(), "lo".to_string()], "the node sees only its own namespace's interfaces");
        assert_ne!(interfaces("self"), interfaces(&pid.to_string()), "the node shares the host network namespace");
        let (first, last) = c.network().node_range();
        let addr = created.node.transport_addr.expect("a created node carries its transport address");
        let std::net::IpAddr::V4(ip) = addr.ip() else { panic!("{addr} is not IPv4") };
        assert!(ip >= first && ip <= last, "{addr} is outside the fabric network {:?}", c.network());
        use rdm_admin::deployment::container::{netns_holds_udp, netns_udp_sockets};
        assert_eq!(netns_holds_udp(pid, addr), Ok(true), "the node's own namespace holds {addr}");
        assert_eq!(netns_holds_udp(std::process::id(), addr), Ok(false), "the host namespace holds {addr}");
        // One Iroh UDP socket on the node's address: the transport, and nothing else bound there
        // (the namespace also carries the container runtime's own loopback DNS socket).
        let udp: Vec<_> = netns_udp_sockets(pid).unwrap().into_iter().filter(|a| a.ip() == addr.ip() || a.ip().is_unspecified()).collect();
        assert_eq!(udp, vec![addr], "the process holds exactly one Iroh UDP socket on its address: {udp:?}");
    }

    // Every step has exactly one Complete receipt, in pipeline order.
    let view = builds.read_build(&build_id).await.unwrap();
    let receipts: Vec<_> = view.steps.iter().map(|r| (r.step.as_str(), &r.outcome, r.operation.as_str(), r.attempt)).collect();
    let want: Vec<_> = CreateStep::ORDER.iter().map(|s| (s.name(), &StepOutcome::Complete, "node.create:mesh1.rpc.1", 1)).collect();
    assert_eq!(receipts, want);

    // One child span per step under the pipeline span, each fully attributed.
    let all = spans.0.lock().unwrap().clone();
    assert!(all.values().any(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-pipeline"
        && f.get("build_id") == Some(&build_id.log().to_string())));
    for step in CreateStep::ORDER {
        let (_, parent, f) = all
            .values()
            .find(|(n, _, f)| n == "rdm.node_admin.deployment.update.via-step" && f.get("step").map(String::as_str) == Some(step.name()))
            .unwrap_or_else(|| panic!("no span for step {}", step.name()));
        assert_eq!(parent.as_deref(), Some("rdm.node_admin.deployment.update.via-pipeline"), "{}", step.name());
        assert_eq!(f.get("build_id"), Some(&build_id.log().to_string()), "{}", step.name());
        assert_eq!(f.get("provider").map(String::as_str), Some(expected_provider.as_str()), "{}", step.name());
        assert_eq!(f.get("outcome").map(String::as_str), Some("complete"), "{}", step.name());
        assert_eq!(f.get("attempt").map(String::as_str), Some("1"), "{}", step.name());
        assert!(f.get("elapsed_ms").is_some_and(|v| v.parse::<u64>().is_ok()), "{}: elapsed_ms {f:?}", step.name());
    }

    // The exact runtime: what the provider recorded is what the runtime
    // read, and any admin of the same control domain controls it from the
    // published fact alone (i143.e4.s16).
    use rdm_admin::deployment::provider::{adopt, AdoptRefusal};
    let fact = created.handle.fact().expect("the provider's handle names its runtime exactly");
    assert_eq!(fact.control_domain, provider.control_domain());
    let data_dir = std::path::PathBuf::from(created.node.data_dir.clone().expect("the node's data dir"));
    assert_eq!(rdm_core::RuntimeFact::read_record(&data_dir).unwrap().unwrap(), fact, "the record the runtime publishes");
    match &fact.locator {
        rdm_core::RuntimeLocator::Container { id } => {
            assert_eq!(id.len(), 64, "the immutable container id, not its name: {id}");
            // A (reusable) container name is no locator.
            let named = rdm_core::RuntimeFact { locator: rdm_core::RuntimeLocator::Container { id: format!("rafka-{}", created.node.name) }, ..fact.clone() };
            assert!(matches!(adopt(&*provider, &named), Err(AdoptRefusal::Invalid { .. })));
        }
        rdm_core::RuntimeLocator::Process { pid, start } => {
            assert_eq!(Some(*start), rdm_core::runtime::process_start_token(*pid));
        }
    }
    let elsewhere = rdm_core::RuntimeFact { control_domain: format!("{}-another", fact.control_domain), ..fact.clone() };
    assert!(matches!(adopt(&*provider, &elsewhere), Err(AdoptRefusal::ForeignControlDomain { .. })), "a locator of another domain is refused");
    let adopted = adopt(&*provider, &fact).expect("the same domain adopts the published runtime");
    assert_eq!(provider.inspect(&adopted).await, DeploymentStatus::Running);
    provider.terminate(&adopted, TerminationMode::Graceful { grace: Duration::from_secs(5) }).await.unwrap();
    assert!(matches!(provider.inspect(&created.handle).await, DeploymentStatus::Exited { .. }));
    if let Some(c) = &prepared.container {
        c.remove_fabric().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&data_root);
    Smoke::Passed
}

/// The admin side's Ping (op `0x01`), so a node's data-plane call to it is answered: in RDM
/// standalone a node's data-plane connection is one the testkit originates (connections.md "Two
/// planes, two words"), and the cell's admin side is a destination it reaches.
pub fn serve_ping(b: rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder {
    use rdm_core::ping::{Ping, PingReply, PingRequest};
    b.serve::<Ping, _, _>(rdm_core::catalog::OpOwner::Core, |_peer, req: PingRequest| async move {
        let PingRequest::Ping { payload, .. } = req;
        Ok(PingReply::Pong { payload })
    })
}

/// A node command (op 0x22, i144 R22) to the birth `target` reaches, read through its ordered reply
/// stream: every frame in order, or the failure that ended it. A refusal is its one frame.
pub async fn node_command(client: &rdm_transport::NodeRpcClient, target: &rdm_transport::NodeTarget, req: &rdm_core::node_command::NodeCommandRequest, opts: &rdm_transport::CallOptions) -> Result<Vec<rdm_core::node_command::NodeCommandReply>, String> {
    use rdm_core::outcome::RpcOutcome;
    use rdm_transport::stream::StreamItem;
    let (mut stream, _) = match client.call_stream::<rdm_core::node_command::NodeCommand>(target, req, opts).await.map(|(s, e)| (s.read_only(), e)) {
        Ok(s) => s,
        Err((RpcOutcome::Reply(r), _)) => return Ok(vec![r.into_value()]),
        Err((other, _)) => return Err(format!("{} ended {}: {other:?}", req.op(), other.name())),
    };
    let mut frames = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            StreamItem::Frame(kind, frame) => {
                frames.push(frame);
                if matches!(kind, rdm_core::streaming::FrameKind::Terminal | rdm_core::streaming::FrameKind::Refusal(_)) {
                    return Ok(frames);
                }
            }
            StreamItem::Failed(f) => return Err(format!("{} broke after {frames:?}: {f:?}", req.op())),
        }
    }
    Err(format!("{} ended without its terminal after {frames:?}", req.op()))
}

/// A node command (op 0x22) to the exact birth `target` reaches, as its mesh-primary commands it: the
/// command's completion (its last frame; a refusal is its one frame).
pub async fn node_command_completion(client: &rdm_transport::NodeRpcClient, target: &rdm_transport::NodeTarget, req: &rdm_core::node_command::NodeCommandRequest) -> rdm_core::node_command::NodeCommandReply {
    let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..Default::default() };
    match node_command(client, target, req, &opts).await {
        Ok(mut frames) => frames.pop().expect("a command's stream holds at least its completion"),
        Err(e) => panic!("{} did not complete: {e}", req.op()),
    }
}

/// Old cell request builders retain their assertion vocabulary; their command hop is 0x22.
/// The fixture primary records progress before acknowledging recovery, as the product owner does.
pub async fn lifecycle_reply(client: &rdm_transport::NodeRpcClient, target: &rdm_transport::NodeTarget, req: &rdm_core::status::StatusRequest, opts: &rdm_transport::CallOptions, digest_view: Option<&rdm_transport::topology::DigestView>) -> Option<rdm_core::status::StatusReply> {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply as N, NodeCommandRequest as Q};
    use rdm_core::status::{StatusReply as R, StatusRequest as S};
    use rdm_transport::stream::StreamItem;
    let command = match req {
        S::StopNode { node_id, incarnation, build_id, attempt, operation } => Q::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: build_id.clone(), attempt: *attempt, operation: operation.clone() },
        S::StartNode { node_id, incarnation, build_id, attempt, operation } => Q::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: build_id.clone(), attempt: *attempt, operation: operation.clone() },
        // node.mesh.leave is a node command on 0x22 (Q205): its Status position is reserved.
        S::LeaveMesh { node_id, incarnation, build_id, attempt, operation } => Q::LeaveMesh { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: build_id.clone(), attempt: *attempt, operation: operation.clone() },
        _ => return None,
    };
    let (node_id, incarnation) = command.subject();
    let mut stream = match client.call_stream::<NodeCommand>(target, &command, opts).await {
        Ok((s, _)) => s,
        Err((rdm_core::outcome::RpcOutcome::Reply(r), _)) => return Some(match r.into_value() {
            N::RejectedStaleIncarnation { held } => R::RejectedStaleIncarnation { held },
            N::NotReady { reason } => R::NotReady { reason },
            N::RejectedInvalidNodeTransition { current } => R::RejectedInvalidNodeTransition { current },
            N::BusyHoldingConnections { connections } => R::BusyHoldingConnections { connections },
            other => panic!("{} refused: {other:?}", command.op()),
        }),
        Err((other, _)) => panic!("{} did not open: {other:?}", command.op()),
    };
    let mut receipt = None;
    let mut joined = false;
    while let Some(item) = stream.next().await {
        let StreamItem::Frame(_, frame) = item else { panic!("{} stream broke: {item:?}", command.op()) };
        let reply = match frame {
            // A stop's primitives' acknowledgements and node.mesh.left (Q205): progress.
            N::CommandAccepted { .. } | N::ConnectionsDeleting { .. } | N::Leaving { .. } | N::StopMeshLeft { .. } => None,
            N::StopConnectionsDeleted { receipt: r } => { receipt = Some(r); None }
            N::NodeJoined => {
                joined = true;
                if let Some(digest_view) = digest_view { digest_view.reported(node_id, incarnation, rdm_core::MemberStatus::Pending); }
                None
            }
            N::NodeStarted => {
                if let Some((digest_view, (held, _))) = digest_view.and_then(|b| b.get(node_id.as_str()).map(|h| (b, h))) { digest_view.started(node_id, incarnation, &held.node.name); }
                Some(if joined { R::Started } else { R::AlreadyApplied })
            }
            N::NodeStopped => {
                if let Some(digest_view) = digest_view { digest_view.reported(node_id, incarnation, rdm_core::MemberStatus::Leaving); }
                Some(R::Stopped { receipt: receipt.take().expect("stop reports its closure before NodeStopped") })
            }
            N::Failed { step, reason, .. } => Some(R::StartFailed { step, reason }),
            N::MeshLeft { node_id, incarnation, left } => Some(R::MeshLeft { node_id, incarnation, left }),
            N::BusyHoldingConnections { connections } => Some(R::BusyHoldingConnections { connections }),
            N::NotReady { reason } => Some(R::NotReady { reason }),
            N::RejectedInvalidNodeTransition { current } => Some(R::RejectedInvalidNodeTransition { current }),
            N::RejectedStaleIncarnation { held } => Some(R::RejectedStaleIncarnation { held }),
            other => panic!("unexpected frame for {}: {other:?}", command.op()),
        };
        stream.accepted().await;
        if reply.is_some() { return reply; }
    }
    panic!("{} stream has no terminal", command.op())
}

/// A cell commanding a real mesh-primary uses that primary's actual command owner.
pub async fn root_lifecycle_reply(root: &rdm_admin::admin::Running, launch: &rdm_core::launch::Launch, req: &rdm_core::status::StatusRequest) -> Option<rdm_core::status::StatusReply> {
    use rdm_core::status::{StatusRequest as Q, StatusReply as R};
    use rdm_admin::deployment::pipeline::{CommandContext, CommandAdmission as A};
    let (start, build_id, attempt, operation) = match req {
        Q::StopNode { build_id, attempt, operation, .. } => (false, build_id, attempt, operation),
        Q::StartNode { build_id, attempt, operation, .. } => (true, build_id, attempt, operation),
        _ => return None,
    };
    let door = root.status_door().expect("the primary's command owner");
    let subject = door.topology.read().await.births().find(|n| n.node_id == launch.node_id).cloned().expect("the primary holds the commanded birth");
    let ctx = CommandContext { build_id: build_id.clone(), attempt: *attempt, operation: operation.clone() };
    Some(match door.carry_lifecycle(&subject, start, &ctx).await {
        A::Started {} => R::Started,
        A::Stopped { receipt } => R::Stopped { receipt },
        A::StartFailed { step, reason, .. } => R::StartFailed { step, reason },
        other => panic!("{} did not finish: {other:?}", req.op()),
    })
}
