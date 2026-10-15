//! The deployment smoke shared by every provider (i143.e2.s3 process,
//! i143.e2.s4 container): a node is deployed through the create
//! `DeploymentPipeline`, every step leaves a receipt, and every step runs as a
//! child span of the pipeline span carrying `step`, `build_id`, `provider`,
//! `outcome`, `attempt` and `elapsed_ms`.
//!
//! The admin side is in-process (a gossip seed plus a Node RPC client); the
//! node is a real `rafka-rpc-node` runtime started by the provider the
//! `MESH_SPAWN_TYPE` value selects.
//!
//! The pieces (admin side, observer, sink, span capture) are shared with the
//! e2.s5 re-run and retire tests.

#![allow(dead_code)]

pub mod admin_view;

use rdm_core::{FabricId, IncarnationId, NodeId};
use rafka_node_admin_core::accepted::{FabricTopology, MeshTopology};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{
    CreateRequest, CreateStep, DeploymentPipeline, LaunchTemplate, Timeouts, TopologySink,
};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentStatus, FabricPolicy, TerminationMode};
use rafka_node_admin_core::model::{Node, NodeStatus, PathName};
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
/// assertions read) and the product span pipeline (`rafka_mesh_telemetry::span_capture`: product's
/// filter and its `BatchSpanProcessor`) in front of the JSONL evidence exporter, at
/// `target/i143-testkit/<cell>/testkit.<pid>-<ns>.spans.jsonl`. A process the cell launches
/// through [`template`] writes its own JSONL into the same dir. The capture is flushed (bounded) when
/// it drops, so a cell that panics leaves its trace.
pub struct Capture {
    pub spans: Spans,
    pub dir: std::path::PathBuf,
    dispatch: tracing::Dispatch,
    capture: rafka_mesh_telemetry::SpanCapture,
}

/// The capture of the cell named `cell` (its test function's name).
pub fn capture(cell: &str) -> Capture {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit").join(cell);
    let exporter = rafka_mesh_telemetry::jsonl_span_exporter(&dir, "testkit").unwrap_or_else(|e| panic!("the span artifact dir {} is unusable: {e}", dir.display()));
    let spans = Spans::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture(cell, exporter);
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
/// the cell's artifact dir (`rafka-rpc-node.<pid>-<ns>.spans.jsonl`) holds its boot span and the
/// span of the stop it took. A runtime killed before its batch export leaves the file empty.
pub fn assert_launched_node_stopped_with_its_spans(dir: &std::path::Path, pid: u32) {
    let prefix = format!("rafka-rpc-node.{pid}-");
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

pub use rafka_node_rpc_testkit::rig::*;

/// The fabric `fabric`'s store under `store`, as its first state-commit left it: the fabric-primary
/// published the Fabric row, so a node of the fabric opened on it writes to it directly
/// (`StoreRoot::open`; states.md, Rounds). A cell that reads a node's rows from the store root runs
/// its node in a fabric past its first state-commit.
pub async fn store_past_state_commit(store: &std::path::Path, fabric: &FabricId) {
    let root = rafka_node_admin_core::store_root::StoreRoot::at(&store.join(fabric.to_string()));
    let fabric_store = rafka_node_admin_core::fabric_storage::FileFabricStorage::open(&root, &rdm_core::IncarnationId::mint()).await.expect("the fabric store opens");
    rafka_node_admin_core::fabric_storage::FabricStorage::claim_identity(&fabric_store, &rafka_node_admin_core::fabric_storage::FabricIdentity { fabric_id: fabric.clone(), name: "fabric1".into() }).await.expect("the Fabric row is published").expect("the fabric name is free");
}

/// A launch template for `rafka-rpc-node` whose launcher is `admin`: its seed, its fabric and its
/// data root, so every node launched from it is stopped when `admin` drops.
pub fn template(admin: &AdminSide) -> LaunchTemplate {
    let data_root = admin.data_root().to_path_buf();
    // The fabric's one store root, handed to every launch (R14).
    let store_dir = data_root.join("store");
    std::fs::create_dir_all(&store_dir).expect("the store root");
    LaunchTemplate {
        fabric: "fabric1".into(),
        fabric_id: admin.fabric.clone(),
        executable: env!("CARGO_BIN_EXE_rafka-rpc-node").into(),
        seeds: vec![admin.seed.clone()],
        launcher: admin.launcher.clone(),
        env: [(rdm_core::launch::ENV_MESH_ID.to_string(), TEST_MESH_ID.to_string())]
            .into_iter()
            .chain(ARTIFACT_DIR.with(|a| a.borrow().clone()).map(|d| ("RDM_EVIDENCE_DIR".to_string(), d.display().to_string())))
            .collect(),
        data_root,
        store_dir,
        mesh_issuer: None,
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
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
    FabricTopology { fabric: "fabric1".into(), meshes: [("mesh1".to_string(), MeshTopology::of(&rafka_node_admin_core::build::MeshDesired::of("mesh1", [(rdm_core::NodeKind::NodeAdmin, 1), (rdm_core::NodeKind::RpcNode, 1)])))].into() }
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
    let prepared = match rafka_node_admin_core::deployment::prepare(policy, &fabric).await {
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
        lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };

    let created = pipeline
        .create(&CreateRequest { build_id: build_id.clone(), attempt: 1, node: node_name, spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() })
        .await;
    let created = match created {
        Ok(c) => c,
        Err(e) => panic!("pipeline failed: {e}"),
    };

    // The node is live, published Pending then ReadyForTraffic, at the assigned endpoints.
    assert_eq!(provider.inspect(&created.handle).await, DeploymentStatus::Running);
    let published = sink.nodes.lock().unwrap().clone();
    assert_eq!(published.iter().map(|n| n.status).collect::<Vec<_>>(), vec![NodeStatus::Pending, NodeStatus::ReadyForTraffic]);
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
        use rafka_node_admin_core::deployment::container::{netns_holds_udp, netns_udp_sockets};
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
    use rafka_node_admin_core::deployment::provider::{adopt, AdoptRefusal};
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
pub fn serve_ping(b: rafka_node_rpc::ServerBuilder) -> rafka_node_rpc::ServerBuilder {
    use rdm_core::ping::{Ping, PingReply, PingRequest};
    b.serve::<Ping, _, _>(rdm_core::catalog::OpOwner::Core, |_peer, req: PingRequest| async move {
        let PingRequest::Ping { payload, .. } = req;
        Ok(PingReply::Pong { payload })
    })
}
