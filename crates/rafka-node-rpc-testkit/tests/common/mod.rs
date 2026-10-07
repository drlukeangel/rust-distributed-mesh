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

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId};
use rafka_mesh_transport::membership::Membership;
use rafka_node_admin_core::accepted::{FabricTopology, MeshTopology};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{
    CreateRequest, CreateStep, DeploymentPipeline, LaunchTemplate, NodeObserver, Timeouts, TopologySink,
};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentStatus, FabricPolicy, TerminationMode};
use rafka_node_admin_core::model::{Node, NodeStatus, PathName};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

/// Every span: name, parent name, and its recorded fields.
#[derive(Clone, Default)]
pub struct Spans(pub Arc<Mutex<HashMap<u64, (String, Option<String>, BTreeMap<String, String>)>>>);

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
        self.0.lock().unwrap().insert(id.into_u64(), (attrs.metadata().name().into(), parent, fields));
    }
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if let Some((_, _, fields)) = self.0.lock().unwrap().get_mut(&id.into_u64()) {
            values.record(&mut Fields(fields));
        }
    }
}

/// Every record published, and every name removed, in order.
#[derive(Default)]
pub struct Published {
    pub nodes: Mutex<Vec<Node>>,
    pub removed: Mutex<Vec<PathName>>,
}
impl TopologySink for Published {
    fn publish(&self, node: Node) {
        self.nodes.lock().unwrap().push(node);
    }
    fn remove(&self, name: &PathName) {
        self.removed.lock().unwrap().push(name.clone());
    }
}

/// Joined = the node's own digest for exactly this birth reached the admin's
/// membership; ready = an Ping answered; drained =
/// this birth's digest says `Draining` with nothing in flight, or `Leaving`;
/// admission closed = no Ping runs any more.
pub struct LiveMesh {
    pub membership: Membership,
    pub client: NodeRpcClient,
    pub resolver: Arc<StaticResolver>,
}

impl LiveMesh {
    fn echo_target(&self, node: &Node) -> Result<NodeTarget, String> {
        let endpoint_id = node.endpoint_id.as_ref().ok_or("no fabric id")?.0.parse::<iroh::PublicKey>().map_err(|e| e.to_string())?;
        self.resolver.insert(ResolvedNode {
            node_id: node.node_id.clone(),
            name: node.name.clone(),
            endpoint_id,
            incarnation: node.incarnation_id.clone().ok_or("no incarnation")?,
            transport_addr: node.transport_addr.ok_or("no transport address")?,
        });
        Ok(NodeTarget::ExactNode(node.node_id.clone()))
    }

    /// One Ping. On loopback a live endpoint answers in milliseconds; the
    /// budget bounds a dial to an endpoint that is gone.
    async fn echo(&self, target: &NodeTarget) -> RpcOutcome<PingReply> {
        let opts = CallOptions {
            budget: rafka_node_rpc::Budget::Overall(Duration::from_millis(500)),
            ..CallOptions::default()
        };
        let req = PingRequest::Ping { payload: b"ready?".to_vec() };
        self.client.call::<Ping>(target, &req, &opts).await.0
    }
}

#[async_trait::async_trait]
impl NodeObserver for LiveMesh {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<rafka_node_admin_core::deployment::pipeline::Publication> {
        self.membership
            .book
            .get(node_id.as_str())
            .filter(|(d, _)| &d.node.incarnation == incarnation)
            .map(|(d, _)| rafka_node_admin_core::deployment::pipeline::Publication { runtime: d.node.runtime, data_dir: d.data_dir })
    }

    async fn ready(&self, node: &Node) -> Result<(), String> {
        let target = self.echo_target(node)?;
        {
            match self.echo(&target).await {
                RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. }) => {}
                other => return Err(format!("{} answered {}", node.name, other.name())),
            }
        }
        Ok(())
    }

    async fn drained(&self, node: &Node) -> bool {
        use rafka_mesh_entity::MemberStatus;
        self.membership.book.get(node.node_id.as_str()).is_some_and(|(d, _)| {
            Some(&d.node.incarnation) == node.incarnation_id.as_ref()
                && (d.status == MemberStatus::Leaving
                    || (d.status == MemberStatus::Draining && d.extra.get("in_flight").map(String::as_str) == Some("0")))
        })
    }

    async fn departed(&self, node: &Node) -> bool {
        use rafka_mesh_entity::MemberStatus;
        self.membership
            .book
            .get(node.node_id.as_str())
            .is_some_and(|(d, _)| Some(&d.node.incarnation) == node.incarnation_id.as_ref() && d.status == MemberStatus::Leaving)
    }

    async fn admission_closed(&self, node: &Node) -> Result<(), String> {
        let target = self.echo_target(node)?;
        {
            match self.echo(&target).await {
                RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. }) => {
                    return Err(format!("{} still runs new calls", node.name))
                }
                // A typed Draining (the handler never ran), or nothing admits the call.
                _ => {}
            }
        }
        Ok(())
    }
}

/// The id of the one mesh these functional fabrics hold (`mesh1`).
pub const TEST_MESH_ID: &str = "meshd0000001";

/// The admin side of a fabric: one endpoint serving gossip on `ip`, used as
/// the nodes' membership seed, and the observer over it.
pub struct AdminSide {
    pub observer: LiveMesh,
    pub seed: (String, SocketAddr),
    _router: Router,
}

pub async fn admin_side(ip: std::net::IpAddr, fabric: &FabricId) -> AdminSide {
    let admin_ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), SocketAddr::new(ip, 0)).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(admin_ep.clone());
    let router = Router::builder(admin_ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let membership = Membership::join(&gossip, &admin_ep, fabric, "mesh1", &MeshId::parse(TEST_MESH_ID).unwrap(), "mesh1.admin.1", vec![]).await.unwrap();
    let addr: SocketAddr = admin_ep.bound_sockets().into_iter().find(|a| a.ip() == ip).unwrap();
    let resolver = Arc::new(StaticResolver::new());
    AdminSide {
        observer: LiveMesh { membership, client: NodeRpcClient::new(admin_ep.clone(), resolver.clone()), resolver },
        seed: (admin_ep.id().to_string(), addr),
        _router: router,
    }
}

/// A launch template for `rafka-rpc-node` with a fresh data root.
pub fn template(fabric: &FabricId, seed: (String, SocketAddr)) -> LaunchTemplate {
    LaunchTemplate {
        fabric: "fabric1".into(),
        fabric_id: fabric.clone(),
        executable: env!("CARGO_BIN_EXE_rafka-rpc-node").into(),
        seeds: vec![seed],
        env: [(rafka_mesh_entity::launch::ENV_MESH_ID.to_string(), TEST_MESH_ID.to_string())].into_iter().collect(),
        data_root: std::env::temp_dir().join(format!("i143-e2-{}", NodeId::mint())),
    }
}

/// Accept one Build of `topology` and return its id.
pub async fn publish_build(builds: &MemoryBuildStateAdapter, topology: FabricTopology) -> BuildId {
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology, submitted_change: None, traceparent: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    build_id
}

/// mesh1 with one admin and one rpc node: what the smoke deploys toward.
pub fn add_node() -> FabricTopology {
    FabricTopology { fabric: "fabric1".into(), meshes: [("mesh1".to_string(), MeshTopology::of("mesh1", 1, 1))].into() }
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
            docker(&["rm", "-f", id]);
        }
        docker(&["network", "rm", &format!("rafka-{}", self.0)]);
    }
}

pub async fn deploy_through_every_step(spawn_type: &str) -> Smoke {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));

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
    let template = template(&fabric_id, admin.seed.clone());
    let data_root = template.data_root.clone();
    let builds = MemoryBuildStateAdapter::new();
    let build_id = publish_build(&builds, add_node()).await;
    let node_name = "mesh1.rpc.1".parse().unwrap();
    let allocator = Mutex::new(prepared.allocator);
    let sink = Published::default();
    let pipeline = DeploymentPipeline {
        provider: &*provider,
        allocator: &allocator,
        observer,
        sink: &sink,
        lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };

    let created = pipeline
        .create(&CreateRequest { build_id: build_id.clone(), attempt: 1, node: node_name, spec: &RPC_NODE, restart_of: None })
        .await;
    let created = match created {
        Ok(c) => c,
        Err(e) => panic!("pipeline failed: {e}"),
    };

    // The node is live, published Pending then ReadyForTraffic, at the assigned endpoints.
    assert_eq!(provider.inspect(&created.handle).await, DeploymentStatus::Running);
    let published = sink.nodes.lock().unwrap().clone();
    assert_eq!(published.iter().map(|n| n.status).collect::<Vec<_>>(), vec![NodeStatus::Pending, NodeStatus::ReadyForTraffic]);
    let held = allocator.lock().unwrap().held(&created.node.name).cloned().expect("the allocator holds the node");
    assert_eq!(created.node.transport_addr, Some(held.transport));

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
    let want: Vec<_> = CreateStep::ORDER.iter().map(|s| (s.name(), &StepOutcome::Complete, "create-node:mesh1.rpc.1", 1)).collect();
    assert_eq!(receipts, want);

    // One child span per step under the pipeline span, each fully attributed.
    let all = spans.0.lock().unwrap().clone();
    assert!(all.values().any(|(n, _, f)| n == "rafka.node_admin.deployment.update.via-pipeline"
        && f.get("build_id") == Some(&build_id.0)));
    for step in CreateStep::ORDER {
        let (_, parent, f) = all
            .values()
            .find(|(n, _, f)| n == "rafka.node_admin.deployment.update.via-step" && f.get("step").map(String::as_str) == Some(step.name()))
            .unwrap_or_else(|| panic!("no span for step {}", step.name()));
        assert_eq!(parent.as_deref(), Some("rafka.node_admin.deployment.update.via-pipeline"), "{}", step.name());
        assert_eq!(f.get("build_id"), Some(&build_id.0), "{}", step.name());
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
    assert_eq!(rafka_mesh_entity::RuntimeFact::read_record(&data_dir).unwrap().unwrap(), fact, "the record the runtime publishes");
    match &fact.locator {
        rafka_mesh_entity::RuntimeLocator::Container { id } => {
            assert_eq!(id.len(), 64, "the immutable container id, not its name: {id}");
            // A (reusable) container name is no locator.
            let named = rafka_mesh_entity::RuntimeFact { locator: rafka_mesh_entity::RuntimeLocator::Container { id: format!("rafka-{}", created.node.name) }, ..fact.clone() };
            assert!(matches!(adopt(&*provider, &named), Err(AdoptRefusal::Invalid { .. })));
        }
        rafka_mesh_entity::RuntimeLocator::Process { pid, start } => {
            assert_eq!(Some(*start), rafka_mesh_entity::runtime::process_start_token(*pid));
        }
    }
    let elsewhere = rafka_mesh_entity::RuntimeFact { control_domain: format!("{}-another", fact.control_domain), ..fact.clone() };
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
