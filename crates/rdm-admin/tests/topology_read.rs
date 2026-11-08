//! `GetTopology` (op `0x1E`) over real Node RPC between two in-process nodes: what a node serves
//! from what it holds, how a caller installs it, and what it refuses by name.

use rdm_core::topology::PrimaryKind;
use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{EndpointId, FabricId, IncarnationId, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_gossip::topology::TopologyView;
use rdm_gossip::aggregate::{Chunk, HeldAggregate, Taken};
use rdm_admin::topology_read::{get_topology, aggregate_replies, top_up_fetch, TopologyService, TopologyFailure, TopologySlot};
use rdm_transport::{NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::catalog::OpOwner;
use rdm_core::protocol::NodeProtocol;
use rdm_core::topology::{SourceVersion, Topology, TopologyReply, TopologyRequest};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};

fn member_of(fabric: &FabricId, mesh: &str, r#type: &str, ordinal: u32) -> MeshDigest {
    MeshDigest { 
        fabric_id: fabric.clone(),
        node: MeshNode { mesh: mesh.to_string(), r#type: r#type.to_string(),
            node_id: NodeId::mint(),
            path: format!("{mesh}.{}", r#type), name: (ordinal.to_string()).to_string(),
            endpoint_id: EndpointId(format!("key-{mesh}-{}-{ordinal}-{}", r#type, "x".repeat(40))),
            transport_addr: format!("127.0.0.1:{}", 20000 + ordinal).parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
        },
        status: rdm_core::model::NodeStatus::Ready,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        primary: Vec::new(),
    }
}

/// One node: an endpoint serving Node RPC and gossip, its topology view, and a topology slot.
struct Node {
    ep: iroh::Endpoint,
    topology_view: TopologyView,
    channels: rdm_gossip::channel::Channels,
    client: NodeRpcClient,
    resolver: Arc<StaticResolver>,
    slot: TopologySlot,
    gossip: iroh_gossip::net::Gossip,
    resolved: ResolvedNode,
    /// The rafka-time this node adopted: 5 000 000 ms, nowhere near its OS clock.
    time: rdm_transport::clock::RafkaTime,
    _router: Router,
}

async fn node(fabric: &FabricId, mesh: &str, name: rdm_core::PathName, r#type: &str, extra: impl FnOnce(ServerBuilder) -> ServerBuilder) -> Node {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let topology_view = rdm_gossip::topology::TopologyView::new(fabric, mesh, &MeshId::mint(), node_id.as_str(), Arc::new(time.clone()), Default::default());
    let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).unwrap();
    channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![]).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = NodeRpcClient::new(ep.clone(), resolver.clone());
    let slot: TopologySlot = Arc::new(OnceLock::new());
    let server = extra(rdm_admin::topology_read::serve(ServerBuilder::new(), slot.clone()))
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .expect("the catalog is built");
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).accept(rdm_transport::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { mesh: mesh.into(), r#type: r#type.to_string(), node_id, path: name.path.clone(), name: name.name.clone(), endpoint_id: ep.id(), transport_addr: addr, incarnation };
    Node { ep, topology_view, channels, client, resolver, slot, gossip, resolved, time, _router: router }
}

fn own_digest(n: &Node, fabric: &FabricId) -> MeshDigest {
    let mut d = member_of(fabric, "mesh1", "node", 1);
    d.node.node_id = n.resolved.node_id.clone();
    d.node.name = n.resolved.name.clone();
    d.node.incarnation = n.resolved.incarnation.clone();
    d.node.mesh = n.resolved.mesh.clone();
    d.node.r#type = n.resolved.r#type.clone();
    d.node.endpoint_id = EndpointId(n.resolved.endpoint_id.to_string());
    d.node.transport_addr = n.resolved.transport_addr;
    d
}

fn open(n: &Node, fabric: &FabricId) {
    let d = own_digest(n, fabric);
    let _ = n.slot.set(Arc::new(TopologyService::new(n.topology_view.clone(), Arc::new(move || d.clone()), n.time.clone())));
}

/// `s` held by `m` as a complete aggregate its primary put into the mesh.
fn hold(m: &rdm_gossip::channel::Channels, s: &HeldAggregate) {
    for r in aggregate_replies(s, &|_| None) {
        let c: Chunk = rdm_admin::topology_read::chunk_of(r, m.topology_view().fabric_id(), 0).expect("an aggregate reply is one chunk");
        let t = m.take_read_chunk(c, "peer");
        assert!(!matches!(t, Taken::Refused(_)), "{t:?}");
    }
}

fn aggregate(mesh: &str, version: u64, digests: Vec<MeshDigest>) -> HeldAggregate {
    HeldAggregate { mesh: mesh.into(), mesh_id: None, topology_version: version, digests }
}

fn target_of(from: &Node, to: &Node) -> NodeTarget {
    from.resolver.insert(to.resolved.clone());
    NodeTarget::ExactNode(to.resolved.node_id.clone())
}

// @feature: node-lifecycle
/// CONTRACT: a node that has not yet opened its `TopologyService` (it is mid-birth, serving calls
/// while its join is outstanding) answers NotReady by name, and the caller installs nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_mid_birth_answers_not_ready_and_the_caller_installs_nothing() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
    let t = target_of(&caller, &server);
    let err = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap_err();
    assert!(matches!(&err, TopologyFailure::NotReady(r) if r.contains("no topology view")), "{err}");
    assert!(caller.topology_view.held_source_version("mesh1").is_none());
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a mesh the node holds no published aggregate of is absent from its answer, its own
/// mesh included: a node whose mesh primary has put no version into the mesh answers with the
/// meshes it does hold (none is a complete, empty answer), and a read naming the absent mesh is
/// UnknownMesh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_with_no_published_aggregate_is_absent_from_the_answer() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
    open(&server, &fabric);
    let t = target_of(&caller, &server);
    let empty = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap();
    assert!(empty.installed.is_empty() && empty.meshes == 0, "nothing held: an empty, complete answer");

    let mesh2: Vec<MeshDigest> = (1..=2).map(|k| member_of(&fabric, "mesh2", "node", k)).collect();
    hold(&server.channels, &aggregate("mesh2", 7, mesh2));
    let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap();
    assert_eq!((read.meshes, read.installed.len()), (1, 1));
    assert_eq!(read.installed[0].mesh, "mesh2");
    assert!(caller.topology_view.held_source_version("mesh1").is_none(), "no version of the own mesh is invented");

    let err = get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh1"), None).await.unwrap_err();
    assert_eq!(err, TopologyFailure::Refused("the target holds no mesh mesh1".into()));
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a mesh primary that has published nothing yet still holds its mesh's members in its
/// view, and a read answers with them as its own mesh at version 0: the
/// caller installs them. A read is never an empty answer for a primary that holds members.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_that_has_published_nothing_serves_the_members_its_view_holds() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
    open(&server, &fabric);
    server.channels.set_publisher(&rdm_core::PublisherId { incarnation: server.resolved.incarnation.clone(), node_id: server.resolved.node_id.clone() });
    server.channels.join(rdm_gossip::channel::ChannelId::backbone(server.topology_view.fabric_id()), vec![]).await.unwrap();
    let backbone = server.channels.backbone().expect("the backbone is joined");
    backbone.set_mesh_primary(true);
    server.topology_view.digest_view.record(member_of(&fabric, "mesh1", "node", 2));
    let t = target_of(&caller, &server);
    let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap();
    assert_eq!((read.meshes, read.installed.len()), (1, 1), "the primary's own mesh is served");
    assert_eq!((read.installed[0].mesh.as_str(), read.installed[0].topology_version), ("mesh1", 0));
    assert_eq!(read.installed[0].members.len(), 2, "its own digest and the member its view holds");
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: an ordinary member answers with the topology it holds: its own mesh as its primary
/// last published it, at that version, and every source mesh it holds at that mesh's version
/// (Q389). The caller installs each mesh, and a read of a source version the
/// caller already holds is Unchanged, a mesh the node does not hold is UnknownMesh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_member_serves_the_topology_it_holds_and_answers_unchanged_and_unknown_mesh() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
    open(&server, &fabric);
    hold(&server.channels, &aggregate("mesh1", 4, vec![own_digest(&server, &fabric)]));
    let mesh2: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh2", "node", k)).collect();
    hold(&server.channels, &aggregate("mesh2", 7, mesh2.clone()));
    let t = target_of(&caller, &server);

    let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap();
    assert_eq!(read.meshes, 2);
    let names = |mesh: &str| -> Vec<rdm_core::PathName> {
        let mut v: Vec<rdm_core::PathName> = read.installed.iter().find(|m| m.mesh == mesh).unwrap().members.iter().map(|d| d.node.location()).collect();
        v.sort();
        v
    };
    assert_eq!(names("mesh2"), vec![rdm_core::PathName::new("mesh2.node", "1").unwrap(), rdm_core::PathName::new("mesh2.node", "2").unwrap(), rdm_core::PathName::new("mesh2.node", "3").unwrap()]);
    assert_eq!(names("mesh1"), vec![rdm_core::PathName::new("mesh1.node", "1").unwrap()], "its own mesh: version 4 as its primary published it");
    assert_eq!(caller.topology_view.held_source_version("mesh2").map(|(_, v)| v), Some(7), "installed at the mesh's version");
    assert_eq!(caller.topology_view.held_source_version("mesh1").map(|(_, v)| v), Some(4));

    let held = SourceVersion { topology_version: 7 };
    let again = get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh2"), Some(held)).await.unwrap();
    assert!(again.installed.is_empty());
    assert_eq!(again.unchanged, vec![("mesh2".to_string(), 7)]);
    assert_eq!(again.meshes, 1, "End counts the meshes answered, Unchanged included");

    let behind = SourceVersion { topology_version: 6 };
    let newer = get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh2"), Some(behind)).await.unwrap();
    assert!(newer.unchanged.is_empty() && newer.installed.len() == 1, "a caller behind the node's version is sent the aggregate");

    let err = get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh9"), None).await.unwrap_err();
    assert_eq!(err, TopologyFailure::Refused("the target holds no mesh mesh9".into()));
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT (gates e6s8 c82f213e2 and e6s7 19f396938: `mesh1.node-admin.2`'s first start read its
/// launcher's topology, served at `topology_version = 0` before the launcher's first publication;
/// that publication, `topology_version = 1`, arrived on the backbone first and the read's chunk was
/// refused `OlderThanHeld { held_version: 1, offered_version: 0 }`, so the read reported no mesh
/// installed and the admin took itself for its mesh's first admin, never Ready): a read whose
/// aggregate of a mesh is older than the complete one the caller already installed of the same
/// source leaves that newer aggregate installed (node-start.md step 4, "Install only complete
/// source aggregates"), and the read reports it, at the version and with the members the caller
/// holds, as the mesh the read leaves installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_of_an_aggregate_older_than_the_held_one_reports_the_aggregate_the_caller_holds() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "node-admin", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "node-admin", |b| b).await);
    open(&server, &fabric);
    let first = member_of(&fabric, "mesh1", "node-admin", 1);
    let second = member_of(&fabric, "mesh1", "node-admin", 2);
    // The target serves the version before the one the caller already holds of the same source.
    hold(&server.channels, &aggregate("mesh1", 1, vec![first.clone()]));
    hold(&caller.channels, &aggregate("mesh1", 2, vec![first.clone(), second.clone()]));
    let t = target_of(&caller, &server);

    let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap();
    assert_eq!(caller.topology_view.held_source_version("mesh1").map(|(_, v)| v), Some(2), "the held projection stands");
    let m = read.installed.iter().find(|m| m.mesh == "mesh1").expect("the read reports the mesh it leaves installed: the newer aggregate the caller holds");
    assert_eq!(m.topology_version, 2, "at the version the caller holds, never the one served");
    let mut ids: Vec<String> = m.members.iter().map(|d| d.node.node_id.to_string()).collect();
    ids.sort();
    let mut want = vec![first.node.node_id.to_string(), second.node.node_id.to_string()];
    want.sort();
    assert_eq!(ids, want, "with the members of the aggregate the caller holds");
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT (gossip.md §3.3: reads initialise and repair views; Q387: a node-admin takes a birth's
/// runtime and data dir from a topology read, never from gossip): a topology larger than one message
/// is chunked by the one packing gossip uses, every chunk under the reply ceiling and the gossip
/// message bound with each node's runtime at the largest fact a birth may record and a long data dir,
/// and the caller installs the mesh whole: every member, once, each with the runtime and data dir the
/// read carried. Must NOT happen: a chunk over a bound, or a read that leaves a node-admin without
/// the runtime it manages a birth with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_topology_streams_in_chunks_under_the_ceiling_and_installs_whole() {
    use rdm_core::runtime::MAX_FACT_BYTES;
    use rdm_core::{NodeRuntime, RuntimeFact, RuntimeLocator, RuntimeProvider};
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
    open(&server, &fabric);
    hold(&server.channels, &aggregate("mesh1", 1, vec![]));
    let big: Vec<MeshDigest> = (1..=300).map(|k| member_of(&fabric, "mesh2", "node", k)).collect();
    let source = aggregate("mesh2", 3, big);
    // The largest fact a birth may record: a container id in a long domain.
    let mut fact = RuntimeFact { deployment_id: "d".repeat(12), provider: RuntimeProvider::Container, control_domain: String::new(), locator: RuntimeLocator::Container { id: "f".repeat(64) } };
    let base = serde_json::to_vec(&fact).unwrap().len();
    fact.control_domain = "c".repeat(MAX_FACT_BYTES - base);
    assert_eq!(fact.validate(), Ok(()), "exactly at the bound");
    let runtime = NodeRuntime { runtime: Some(fact), data_dir: Some(format!("/var/lib/rafka/{}", "x".repeat(64))) };
    let chunks = aggregate_replies(&source, &|_| Some(runtime.clone()));
    assert!(chunks.len() > 2, "{} chunks", chunks.len());
    for c in &chunks {
        let len = Topology::encode_reply(c).unwrap().len();
        assert!(len <= Topology::MAX_REPLY_FRAME_BYTES, "a chunk of {len} bytes is over the ceiling");
        assert!(len <= rdm_gossip::frame::MAX_FRAME, "a chunk of {len} bytes is over the gossip message bound");
    }
    hold(&server.channels, &source);
    for d in &source.digests {
        assert!(server.topology_view.digest_view.hold_runtime(&d.node.node_id, &d.node.incarnation, runtime.clone()));
    }
    let t = target_of(&caller, &server);
    let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh2"), None).await.unwrap();
    let m = &read.installed[0];
    let mut ids: Vec<String> = m.members.iter().map(|d| d.node.node_id.to_string()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!((m.members.len(), ids.len()), (300, 300), "every member, once");
    for d in &source.digests {
        assert_eq!(caller.topology_view.digest_view.runtime_of(&d.node.node_id, &d.node.incarnation).as_ref(), Some(&runtime), "the read carried {}'s runtime and data dir", d.node.node_id);
    }
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: an aggregate whose chunks do not all arrive is never installed: the caller's held
/// projection stands and the read ends without installing the mesh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_missing_a_chunk_is_never_installed() {
    let fabric = FabricId::mint();
    let sent: Arc<Mutex<u32>> = Arc::default();
    let counted = sent.clone();
    let server = node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await;
    // A server that streams the first chunk of a two-chunk aggregate and then ends the stream.
    let lossy_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let (lid, linc) = (NodeId::mint(), IncarnationId::mint());
    let lossy = ServerBuilder::new()
        .serve_stream::<Topology, _, _>(OpOwner::Product("rdm".into()), move |_peer, _req: TopologyRequest, sink| {
            let counted = counted.clone();
            async move {
                let mut sink = sink.started(TopologyReply::Started).await.unwrap();
                sink.data(TopologyReply::Aggregate {
                    mesh: "mesh2".into(),
                    mesh_id: None,
                    topology_version: 1,
                    chunk_index: 0,
                    chunk_count: 2,
                    nodes: vec![],
                })
                .await
                .unwrap();
                *counted.lock().unwrap() += 1;
                Ok(TopologyReply::End { meshes: 1 })
            }
        })
        .build(ServedBirth { node_id: lid.to_string(), incarnation: linc.to_string() })
        .unwrap();
    let _router = Router::builder(lossy_ep.clone()).accept(rdm_transport::ALPN, lossy).spawn();
    let lossy_node = ResolvedNode { mesh: "mesh1".into(), r#type: "node".into(), node_id: lid.clone(), path: "mesh1.node".into(), name: "9".into(), endpoint_id: lossy_ep.id(), transport_addr: lossy_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap(), incarnation: linc };
    server.resolver.insert(lossy_node);
    let read = get_topology(&server.client, &NodeTarget::ExactNode(lid), "mesh1", &server.channels, None, None).await.unwrap();
    assert_eq!(*sent.lock().unwrap(), 1);
    assert!(read.installed.is_empty(), "one chunk of two installs nothing");
    assert!(server.topology_view.held_source_version("mesh2").is_none());
    server.ep.close().await;
    lossy_ep.close().await;
}

/// This cell's own span capture: the product span pipeline (`rdm_transport::span_capture`: its span filter and
/// `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher installed on every
/// thread of the cell's own runtime, so cells sharing a test process never share spans.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture(cell: &str) -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture(&format!("topology-read-{cell}"), exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d))).build().unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    /// `(name, attributes)` of every finished span.
    fn spans(&self) -> Vec<(String, std::collections::BTreeMap<String, String>)> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let finished = self.exporter.get_finished_spans().unwrap();
        crate::assert_own_story(finished.iter().map(|s| s.name.as_ref()));
        finished
            .into_iter()
            .map(|s| (s.name.to_string(), s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect()))
            .collect()
    }

    fn named(&self, name: &str) -> Vec<std::collections::BTreeMap<String, String>> {
        self.spans().into_iter().filter(|(n, _)| n == name).map(|(_, a)| a).collect()
    }
}

// @feature: node-lifecycle
/// CONTRACT: a request naming a mesh the node never heard of is answered `UnknownMesh` naming it,
/// installs nothing, and the node's serve span records the `unknown-mesh` outcome for that request
/// (`requested` names the mesh) with no mesh or aggregate streamed.
#[test]
fn a_request_naming_a_mesh_the_node_never_heard_is_unknown_mesh_and_streams_nothing() {
    let cap = capture("unknown-mesh");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
        open(&server, &fabric);
        hold(&server.channels, &aggregate("mesh2", 3, vec![member_of(&fabric, "mesh2", "node", 1)]));
        let t = target_of(&caller, &server);
        let err = get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh77"), None).await.unwrap_err();
        assert_eq!(err, TopologyFailure::Refused("the target holds no mesh mesh77".into()));
        assert!(caller.topology_view.held_source_version("mesh2").is_none() && caller.topology_view.held_source_version("mesh77").is_none());
        server.ep.close().await;
        caller.ep.close().await;
    });
    let serves = cap.named("rdm.mesh.topology.serve.via-read");
    assert_eq!(serves.len(), 1, "{serves:?}");
    assert_eq!((serves[0]["outcome"].as_str(), serves[0]["requested"].as_str()), ("unknown-mesh", "mesh77"));
    assert!(!serves[0].contains_key("aggregates"), "nothing was streamed: {:?}", serves[0]);
    assert!(cap.named("rdm.mesh.topology.update.via-read-install").is_empty(), "nothing was installed");
}

// @feature: node-lifecycle
/// CONTRACT (Q389: the mesh's version is the mesh's, whichever mesh-primary sent it): `since` is the
/// version of the mesh the caller holds. A version behind the node's is answered with the aggregate,
/// and the serve span counts one aggregate and no unchanged; the version the node holds is Unchanged.
/// A `since` with no mesh named is ignored: every mesh is streamed.
#[test]
fn a_since_behind_the_held_version_is_answered_with_the_aggregate_and_the_held_version_unchanged() {
    let cap = capture("since-revision");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
        open(&server, &fabric);
        hold(&server.channels, &aggregate("mesh2", 7, (1..=2).map(|k| member_of(&fabric, "mesh2", "node", k)).collect()));
        let t = target_of(&caller, &server);
        let behind = SourceVersion { topology_version: 6 };
        let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh2"), Some(behind)).await.unwrap();
        assert!(read.unchanged.is_empty() && read.installed.len() == 1, "{read:?}");
        assert_eq!(caller.topology_view.held_source_version("mesh2").map(|(_, v)| v), Some(7), "installed at the mesh's version");
        // The version held is Unchanged; a `since` with no mesh named is ignored.
        let held = SourceVersion { topology_version: 7 };
        let unchanged = get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh2"), Some(held.clone())).await.unwrap();
        assert_eq!(unchanged.unchanged, vec![("mesh2".to_string(), 7)]);
        let all = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, Some(held)).await.unwrap();
        assert!(all.unchanged.is_empty() && all.installed.len() == 1, "since is honoured only with a mesh named: {all:?}");
        server.ep.close().await;
        caller.ep.close().await;
    });
    let serves = cap.named("rdm.mesh.topology.serve.via-read");
    let counts: Vec<(&str, &str, &str)> = serves.iter().map(|a| (a["requested"].as_str(), a["aggregates"].as_str(), a["unchanged"].as_str())).collect();
    assert_eq!(counts, vec![("mesh2", "1", "0"), ("mesh2", "0", "1"), ("*", "1", "0")], "{serves:?}");
}

// @feature: node-lifecycle
/// CONTRACT (R-G2): a read from a node of another mesh whose source of the reader's own mesh is
/// stale installs topology and no liveness. The members of the reader's own mesh are returned to
/// the reader (to map and sweep) but are held nowhere in its view, so a dead one is never heard
/// as alive; a third mesh's members are held as topology. The reader's install spans name the
/// answering node and the mesh.
#[test]
fn a_read_from_a_peer_mesh_returns_the_readers_own_mesh_but_holds_none_of_it_as_heard() {
    let cap = capture("peer-read");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh2", rdm_core::PathName::new("mesh2.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
        open(&server, &fabric);
        let own: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh1", "node", k)).collect();
        let third: Vec<MeshDigest> = (1..=2).map(|k| member_of(&fabric, "mesh3", "node", k)).collect();
        hold(&server.channels, &aggregate("mesh1", 9, own.clone()));
        hold(&server.channels, &aggregate("mesh3", 2, third.clone()));
        let t = target_of(&caller, &server);
        let read = get_topology(&caller.client, &t, "mesh2", &caller.channels, None, None).await.unwrap();
        let members_of = |mesh: &str| read.installed.iter().find(|m| m.mesh == mesh).map(|m| m.members.len());
        assert_eq!((members_of("mesh1"), members_of("mesh3")), (Some(3), Some(2)), "the reader is given every mesh the node holds");
        for d in &own {
            assert!(caller.topology_view.digest_view.get(d.node.node_id.as_str()).is_none(), "{:?} of the reader's own mesh is not held as heard from a peer mesh's stale source", d.node.name);
        }
        for d in &third {
            assert!(caller.topology_view.digest_view.get(d.node.node_id.as_str()).is_some(), "{:?} of a third mesh is held as topology", d.node.name);
        }
        server.ep.close().await;
        caller.ep.close().await;
    });
    let installs = cap.named("rdm.mesh.topology.update.via-read-install");
    // The reader installs: every install span names the one reading node, by its node id.
    let mut got: Vec<(&str, &str)> = installs.iter().map(|a| (a["mesh"].as_str(), a["members"].as_str())).collect();
    got.sort();
    assert_eq!(got, vec![("mesh1", "3"), ("mesh3", "2")], "{installs:?}");
    assert!(installs.iter().all(|a| !a["node"].is_empty() && a["node"] == installs[0]["node"]), "{installs:?}");
}

// @feature: node-lifecycle
/// CONTRACT: a topology read of a mesh that has a birth being deleted carries the deletion: the
/// node serves the held source's in-flight operation with the member still listed, and the reader
/// holds the member as deleting (its overlay names the operation) rather than as a plain member.
#[test]
fn a_read_while_a_birth_of_the_mesh_is_being_deleted_carries_the_deleting_overlay() {
    let cap = capture("deleting");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
        open(&server, &fabric);
        let mut members: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh3", "node", k)).collect();
        // The node under its delete says so by its status (Q390): the list carries no overlay beside it.
        members[1].status = rdm_core::model::NodeStatus::Deleting;
        let deleting = members[1].clone();
        hold(&server.channels, &aggregate("mesh3", 4, members.clone()));
        let t = target_of(&caller, &server);
        let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh3"), None).await.unwrap();
        assert_eq!(read.installed[0].members.len(), 3, "the deleting member is still listed");
        assert!(read.installed[0].members.iter().any(|d| d.node.node_id == deleting.node.node_id && d.status == rdm_core::model::NodeStatus::Deleting), "listed deleting");
        let (in_flight, _) = caller.topology_view.digest_view.overlays_of("mesh3");
        assert!(matches!(in_flight.as_slice(), [op] if !op.is_stop() && op.node_id == deleting.node.node_id && op.incarnation == deleting.node.incarnation), "the reader holds the deletion as an overlay: {in_flight:?}");
        assert!(!caller.topology_view.digest_view.routable(deleting.node.node_id.as_str()), "a node under its delete is not routable");
        server.ep.close().await;
        caller.ep.close().await;
    });
    let serve = cap.named("rdm.mesh.topology.serve.via-read");
    assert_eq!(serve.len(), 1);
    assert_eq!((serve[0]["outcome"].as_str(), serve[0]["aggregates"].as_str()), ("served", "1"));
}

// @feature: node-lifecycle
/// CONTRACT (Q390): an admin that joins after a node was stopped never heard the node's digest; a
/// `GetTopology` read of the mesh lists the node stopped, the reader holds the birth stopped from it,
/// and the reader's view lists the node `Leaving` and stopped, never absent.
#[test]
fn a_late_reader_lists_a_stopped_node_from_the_stopped_status_a_read_carries() {
    let cap = capture("stopped-late");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
        open(&server, &fabric);
        let mut members: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh3", "node", k)).collect();
        // The mesh primary's list after the stop: the stopped node listed stopped, with its identity.
        members[1].status = rdm_core::model::NodeStatus::Stopped;
        let stopped = members[1].clone();
        hold(&server.channels, &aggregate("mesh3", 4, members));
        let t = target_of(&caller, &server);
        get_topology(&caller.client, &t, "mesh1", &caller.channels, Some("mesh3"), None).await.unwrap();
        assert!(caller.topology_view.digest_view.get(stopped.node.node_id.as_str()).is_none(), "the reader holds the stopped node by its stop, never as a node heard");
        assert!(caller.topology_view.digest_view.holds_stopped(stopped.node.node_id.as_str(), &stopped.node.incarnation), "the reader holds the birth stopped");
        assert!(caller.topology_view.digest_view.stopped_digests().iter().any(|d| d.node.node_id == stopped.node.node_id), "the stop keeps the listed birth's identity");
        let view = rdm_admin::admin::project("fabric1", &fabric, rdm_admin::model::ProviderKind::Process, &caller.topology_view.digest_view, &rdm_admin::admin::Records::default());
        let n = view.node(&stopped.node.location()).expect("the stopped node is listed");
        assert_eq!((n.status, n.stopped, n.routable), (rdm_admin::model::NodeStatus::Leaving, true, false));
        server.ep.close().await;
        caller.ep.close().await;
    });
}

fn stored_of(d: &MeshDigest) -> rdm_core::topology::StoredNode {
    rdm_core::topology::StoredNode { mesh: d.node.mesh.clone(), r#type: d.node.r#type.clone(), node_id: d.node.node_id.clone(), path: d.node.path.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.clone(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr }
}

/// `n` serves the births `rows` as its stored map.
fn open_with_stored(n: &Node, fabric: &FabricId, rows: Vec<rdm_core::topology::StoredNode>) {
    let d = own_digest(n, fabric);
    let source: rdm_admin::topology_read::StoredSource = Arc::new(move || {
        let rows = rows.clone();
        Box::pin(async move { Ok(rdm_admin::topology_read::StoredMap { nodes: rows, mesh_ids: [("mesh1".to_string(), MeshId::parse("04raj09p3zp7").unwrap())].into_iter().collect() }) })
    });
    let _ = n.slot.set(Arc::new(TopologyService::new(n.topology_view.clone(), Arc::new(move || d.clone()), n.time.clone()).with_stored(source)));
}

// @feature: node-lifecycle
/// CONTRACT: a node that holds no gossiped aggregate of a mesh but stores births of it answers that
/// mesh with its stored map, marked as having no version: the reader gets the births to reach and
/// installs nothing. The stored mesh holds no source version at the reader, none of its nodes is in
/// the reader's view, and a mesh the node does hold an aggregate of is answered by the aggregate, not
/// the stored map. The node never answers its own mesh from the stored map. The serve span counts
/// the stored mesh and streams no aggregate of it.
#[test]
fn a_stored_answer_is_never_installed_as_current_topology() {
    let cap = capture("stored");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh2", rdm_core::PathName::new("mesh2.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
        let lost: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh1", "node", k)).collect();
        let held: Vec<MeshDigest> = (1..=2).map(|k| member_of(&fabric, "mesh3", "node", k)).collect();
        let own: Vec<MeshDigest> = vec![member_of(&fabric, "mesh2", "node", 9)];
        // The node stores births of the lost mesh, of a mesh it holds an aggregate of, and of its own.
        let rows: Vec<_> = lost.iter().chain(held.iter()).chain(own.iter()).map(stored_of).collect();
        open_with_stored(&server, &fabric, rows);
        hold(&server.channels, &aggregate("mesh3", 5, held.clone()));
        let t = target_of(&caller, &server);
        let read = get_topology(&caller.client, &t, "mesh2", &caller.channels, None, None).await.unwrap();
        assert_eq!(read.installed.iter().map(|m| m.mesh.as_str()).collect::<Vec<_>>(), vec!["mesh3"], "the held aggregate is answered as an aggregate");
        assert_eq!(read.stored.len(), 1, "only the lost mesh is answered from the stored map: {:?}", read.stored);
        assert_eq!((read.stored[0].mesh.as_str(), read.stored[0].nodes.len()), ("mesh1", 3));
        assert_eq!(read.stored[0].mesh_id, Some(MeshId::parse("04raj09p3zp7").unwrap()), "the stored mesh carries the id the node stored for it");
        assert_eq!(read.meshes, 2, "End counts the aggregate mesh and the stored mesh");
        assert!(caller.topology_view.held_source_version("mesh1").is_none(), "a stored map carries no version, so no source is held for it");
        for d in &lost {
            assert!(caller.topology_view.digest_view.get(d.node.node_id.as_str()).is_none(), "{:?} of a stored map is not held by the reader", d.node.name);
        }
        // Asking for the stored mesh by name is answered the same way; a mesh stored nowhere is unknown.
        let one = get_topology(&caller.client, &t, "mesh2", &caller.channels, Some("mesh1"), None).await.unwrap();
        assert!(one.installed.is_empty() && one.stored.len() == 1 && one.meshes == 1, "{one:?}");
        let err = get_topology(&caller.client, &t, "mesh2", &caller.channels, Some("mesh8"), None).await.unwrap_err();
        assert_eq!(err, TopologyFailure::Refused("the target holds no mesh mesh8".into()));
        server.ep.close().await;
        caller.ep.close().await;
    });
    let serves = cap.named("rdm.mesh.topology.serve.via-read");
    let rows: Vec<(&str, &str, &str, &str)> = serves.iter().map(|a| (a["requested"].as_str(), a["outcome"].as_str(), a.get("stored").map(|s| s.as_str()).unwrap_or("-"), a.get("aggregates").map(|s| s.as_str()).unwrap_or("-"))).collect();
    assert_eq!(rows, vec![("*", "served", "1", "1"), ("mesh1", "served", "1", "0"), ("mesh8", "unknown-mesh", "-", "-")], "{serves:?}");
    let installs = cap.named("rdm.mesh.topology.update.via-read-install");
    assert!(installs.iter().all(|a| a["mesh"] == "mesh3"), "nothing of the stored mesh was installed: {installs:?}");
}

// @feature: node-lifecycle
/// CONTRACT (node-rpc-envelope.md Topology 0x1E: `Primaries { kind, primary, gone }`, `RafkaTime { ms,
/// kind }`): a read is served with the rafka-time the node adopted (never its own clock), which primary
/// the node is in its own view (none, its mesh's mesh-primary, the fabric-primary), and the mesh-primary
/// and fabric-primary records it holds, which `get_topology` installs into the caller's view; another
/// node type's primary rides its digest and its mesh-primary's projection, not the read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_is_served_with_the_adopted_rafka_time_and_the_nodes_primaries() {
    use rdm_core::{PrimaryRole, Primary};
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
    open(&server, &fabric);
    let t = target_of(&caller, &server);
    let held = |role| (role, Primary { mesh: "mesh1".into(), node_id: server.resolved.node_id.clone(), incarnation: server.resolved.incarnation.clone(), revision: 1 });

    let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap();
    let served = read.rafka_time.expect("every accepted read carries rafka-time");
    assert!((5_000_000..5_060_000).contains(&served.ms), "the node's adopted time, not an OS clock: {}", served.ms);
    assert_eq!(served.kind, None, "a node that is no primary answers none");

    let (role, primary) = held(PrimaryRole::MeshPrimary);
    server.topology_view.primaries().take(role, &primary);
    let rpc_primary = Primary { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 2 };
    let broker_primary = Primary { mesh: "mesh2".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 3 };
    server.topology_view.primaries().take(PrimaryRole::NodePrimary("node".into()), &rpc_primary);
    server.topology_view.primaries().take(PrimaryRole::NodePrimary("broker".into()), &broker_primary);
    let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap();
    assert_eq!(read.rafka_time.unwrap().kind, Some(PrimaryKind::MeshPrimary));
    assert_eq!(caller.topology_view.primaries().mesh("mesh1"), Some(primary.clone()));
    // The read carries the mesh-primaries and the fabric-primary (node-rpc-envelope.md `Primaries`); every
    // other type's primary rides its digest and its mesh-primary's projection (`is_primary`).
    assert_eq!(caller.topology_view.primaries().node_type_primary("mesh1", "node"), None, "another type's primary is not carried by the read: {rpc_primary:?}");
    assert_eq!(caller.topology_view.primaries().node_type_primary("mesh2", "broker"), None, "nor a peer mesh's: {broker_primary:?}");

    let (role, primary) = held(PrimaryRole::FabricPrimary);
    server.topology_view.primaries().take(role, &primary);
    let read = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap();
    assert_eq!(read.rafka_time.unwrap().kind, Some(PrimaryKind::FabricPrimary), "the fabric-primary outranks the mesh-primary it also is");
    assert_eq!(caller.topology_view.primaries().fabric(), Some(primary));
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a node that has adopted no rafka-time has none to serve: a read is NotReady naming
/// why, and the stream never starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_holding_no_rafka_time_answers_not_ready_by_name() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", |b| b).await);
    let d = own_digest(&server, &fabric);
    let _ = server.slot.set(Arc::new(TopologyService::new(server.topology_view.clone(), Arc::new(move || d.clone()), rdm_transport::clock::RafkaTime::unadopted())));
    let t = target_of(&caller, &server);
    let err = get_topology(&caller.client, &t, "mesh1", &caller.channels, None, None).await.unwrap_err();
    assert!(matches!(&err, TopologyFailure::NotReady(r) if r.contains("holds no rafka-time")), "{err}");
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a member adopts the rafka-time of whichever node answers; a mesh primary adopts only
/// from a fabric-primary role, and refuses the answer of a replica (`served-by-a-replica`) or of
/// another mesh primary (`a-mesh-primary-takes-only-the-fabric-primary`) by name, keeping the
/// time it holds.
#[test]
fn a_mesh_primary_refuses_the_time_of_a_replica_and_a_mesh_primary_and_adopts_a_fabric_primarys() {
    let cap = capture("rafka-time-verdict");
    let (caller_id, server_id) = cap.run(async {
        use rdm_core::{PrimaryRole, Primary};
        use rdm_admin::rafka_time::Refusal;
        use rdm_admin::topology_read::TimeNotAdopted;
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh2", rdm_core::PathName::new("mesh2.node", "1").unwrap(), "node", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "node-admin", |b| b).await);
        open(&server, &fabric);
        caller.time.adopt(9_000_000);
        let t = target_of(&caller, &server);
        let caller_id = caller.resolved.node_id.clone();
        // The caller holds its mesh's primary role in its own records.
        caller.topology_view.primaries().take(PrimaryRole::MeshPrimary, &Primary { mesh: "mesh1".into(), node_id: caller_id.clone(), incarnation: caller.resolved.incarnation.clone(), revision: 1 });
        let pull = || async {
            let read = get_topology(&caller.client, &t, "mesh2", &caller.channels, None, None).await.unwrap();
            read.adopt_rafka_time(&caller.time, &caller.topology_view, caller_id.as_str(), "mesh1", &caller_id, server.resolved.node_id.as_str())
        };
        assert_eq!(pull().await.unwrap_err(), TimeNotAdopted::Refused(Refusal::ServedByAReplica));
        assert!(caller.time.now_ms() >= 9_000_000 && caller.time.now_ms() < 9_060_000, "the replica's 5 000 000 was not taken");

        server.topology_view.primaries().take(PrimaryRole::MeshPrimary, &Primary { mesh: "mesh2".into(), node_id: server.resolved.node_id.clone(), incarnation: server.resolved.incarnation.clone(), revision: 1 });
        assert_eq!(pull().await.unwrap_err(), TimeNotAdopted::Refused(Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary));

        server.topology_view.primaries().take(PrimaryRole::FabricPrimary, &Primary { mesh: "mesh2".into(), node_id: server.resolved.node_id.clone(), incarnation: server.resolved.incarnation.clone(), revision: 1 });
        let adopted = pull().await.unwrap();
        assert_eq!(adopted.previous_ms.map(|p| p >= 9_000_000), Some(true));
        assert!(adopted.stalls_ms > 3_000_000, "the fabric-primary's earlier time is behind: the reader stalls, it never steps back: {adopted:?}");
        assert!(caller.time.now_ms() >= 9_000_000, "never back");
        server.ep.close().await;
        caller.ep.close().await;
        (caller_id, server.resolved.node_id.clone())
    });
    let refused = cap.named("rdm.mesh.entry.reject.via-rafka-time-from-non-primary");
    let reasons: Vec<(&str, &str, &str)> = refused.iter().map(|a| (a["node"].as_str(), a["kind"].as_str(), a["reason"].as_str())).collect();
    assert_eq!(reasons, vec![(caller_id.as_str(), "none", "served-by-a-replica"), (caller_id.as_str(), "mesh-primary", "a-mesh-primary-takes-only-the-fabric-primary")]);
    let adopted = cap.named("rdm.mesh.entry.update.via-rafka-time-adopted");
    assert_eq!(adopted.len(), 1, "only the fabric-primary's answer was adopted: {adopted:?}");
    assert_eq!((adopted[0]["source"].as_str(), adopted[0]["via"].as_str(), adopted[0]["kind"].as_str(), adopted[0]["served_by"].as_str()), ("pull", "get-topology", "fabric-primary", server_id.as_str()));
}

// @feature: node-lifecycle
/// CONTRACT (gossip.md §3.3: a desynchronized receiver "pulls the current full fabric topology from
/// its own current mesh-primary with GetTopology, installs it atomically, records the publisher and
/// version baseline, and resumes"; lifecycles/node-topology-get.md: the top-up "is a read/connection
/// repair, not a stop, restart or new admission"; origin-ordering.md, rafka-time: adopted at every
/// JoinNode, and "between pulls a node keeps its rafka-time disciplined from its own authority's
/// heartbeats only"): a member that tops up a desynchronized source from its mesh's primary installs
/// the complete aggregate and keeps the rafka-time it holds. Nothing is adopted, so its reading is not
/// stalled and its discipline window and slew are not cleared. Must NOT happen: the top-up re-anchors
/// the member to the served time (gate e588aaea, rafka_time_discipline: mesh1.node.1's version-gap
/// top-up adopted mesh1.node-admin.1's served time with stalls_ms=1501, erasing the 1.5 s the cell set, so
/// the discipline never slewed; trace 1074d9baac76f1c2e8088641cbbea279).
#[test]
fn a_member_topping_up_a_desynced_source_installs_it_and_keeps_its_rafka_time() {
    let cap = capture("top-up-keeps-time");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "node-admin", |b| b).await, node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", |b| b).await);
        open(&server, &fabric);
        hold(&server.channels, &aggregate("mesh2", 7, (1..=2).map(|k| member_of(&fabric, "mesh2", "node", k)).collect()));
        // The member stands far ahead of the primary's 5 000 000 ms: an adoption would stall it.
        caller.time.adopt(9_000_000);
        let _ = target_of(&caller, &server);
        let mut primary = own_digest(&server, &fabric);
        primary.node.name = server.resolved.name.clone();
        let fetch = top_up_fetch(Arc::new(NodeRpcClient::new(caller.ep.clone(), caller.resolver.clone())));
        let done = fetch(caller.channels.clone(), primary, vec!["mesh2".into()]).await.unwrap();
        assert_eq!(done.installed, vec![("mesh2".to_string(), 7)], "the top-up installed the complete aggregate");
        let before = caller.time.now_ms();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let after = caller.time.now_ms();
        assert!(after >= before + 50, "the member's rafka-time runs on from what it held, unstalled: {before} -> {after}");
        server.ep.close().await;
        caller.ep.close().await;
    });
    assert!(cap.named("rdm.mesh.entry.update.via-rafka-time-adopted").is_empty(), "a top-up adopts no rafka-time: {:?}", cap.named("rdm.mesh.entry.update.via-rafka-time-adopted"));
    let installed = cap.named("rdm.mesh.topology.update.via-read-install");
    assert_eq!(installed.iter().map(|a| (a["mesh"].as_str(), a["topology_version"].as_str())).collect::<Vec<_>>(), vec![("mesh2", "7")], "{installed:?}");
}

// @feature: rafka-time
/// CONTRACT: docs/architecture/fabric-node-hooks.md:150
/// "RDM exposes it to the application through one public, cloneable `RafkaTime` handle with
/// `now_ms()`. The app gets the handle from the running node returned by RDM's start, and from
/// `NodeReadyCtx`. It is the same instance every RDM stamp reads; there is no second clock."
#[test]
fn mesh_primary_pulls_rafka_time_from_new_fabric_primary_on_primary_change_into_shared_handle() {
    let cap = capture("rafka-time-shared-handle-pull");
    let (primary_id, fp_id) = cap.run(async {
        use rdm_core::{Primary, PrimaryRole};
        let fabric = FabricId::mint();
        let (new_fp, mesh2_primary) = (
            node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "node-admin", |b| b).await,
            node(&fabric, "mesh2", rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "node-admin", |b| b).await,
        );
        new_fp.time.adopt(12_000_000);
        open(&new_fp, &fabric);
        hold(
            &new_fp.channels,
            &aggregate("mesh1", 1, vec![own_digest(&new_fp, &fabric)]),
        );
        new_fp.topology_view.primaries().take(
            PrimaryRole::FabricPrimary,
            &Primary {
                mesh: "mesh1".into(),
                node_id: new_fp.resolved.node_id.clone(),
                incarnation: new_fp.resolved.incarnation.clone(),
                revision: 2,
            },
        );

        let t = target_of(&mesh2_primary, &new_fp);
        let mesh2_primary_id = mesh2_primary.resolved.node_id.clone();
        mesh2_primary.topology_view.primaries().take(
            PrimaryRole::MeshPrimary,
            &Primary {
                mesh: "mesh2".into(),
                node_id: mesh2_primary_id.clone(),
                incarnation: mesh2_primary.resolved.incarnation.clone(),
                revision: 1,
            },
        );

        let app_clock = mesh2_primary.time.clone();
        assert!(app_clock.now_ms() < 6_000_000, "starts at 5 000 000 ms");

        let read = get_topology(
            &mesh2_primary.client,
            &t,
            "mesh2",
            &mesh2_primary.channels,
            Some("mesh1"),
            None,
        )
        .await
        .expect("topology read from new fabric-primary succeeds");
        let adopted = read
            .adopt_rafka_time(
                &mesh2_primary.time,
                &mesh2_primary.topology_view,
                mesh2_primary_id.as_str(),
                "mesh2",
                &mesh2_primary_id,
                new_fp.resolved.node_id.as_str(),
            )
            .expect("mesh-primary adopts rafka-time served by fabric-primary");

        assert!(adopted.reference_ms >= 12_000_000);
        assert!(
            app_clock.now_ms() >= 12_000_000,
            "the cloned RafkaTime handle sees the new fabric-primary's 12 000 000 ms: {}",
            app_clock.now_ms()
        );
        new_fp.ep.close().await;
        mesh2_primary.ep.close().await;
        (mesh2_primary_id, new_fp.resolved.node_id.clone())
    });
    let adopted = cap.named("rdm.mesh.entry.update.via-rafka-time-adopted");
    assert_eq!(adopted.len(), 1, "expected via-rafka-time-adopted span: {adopted:?}");
    assert_eq!(adopted[0]["node"].as_str(), primary_id.as_str());
    assert_eq!(adopted[0]["source"].as_str(), "pull");
    assert_eq!(adopted[0]["via"].as_str(), "get-topology");
    assert_eq!(adopted[0]["kind"].as_str(), "fabric-primary");
    assert_eq!(adopted[0]["served_by"].as_str(), fp_id.as_str());
}

// @feature: node-topology-get
/// CONTRACT (docs/architecture/query.md §2: `node.topology.get` answers what the target node holds;
/// Q266: a read of one mesh copies that mesh's source alone, does not build the stored map when the
/// requested mesh has a held aggregate, and reads the lost incarnations once per read rather than once
/// per primary record): under concurrent readers asking for single meshes (`mesh1`, `mesh2`, `mesh3`)
/// and the whole fabric (`None`), every one-mesh read answers the exact same topology version,
/// members, primary records, and rafka-time kind as the whole read answers for that mesh.
#[test]
fn a_one_mesh_topology_read_answers_what_the_whole_read_answers_for_that_mesh_under_concurrent_readers() {
    let cap = capture("one-mesh-matches-whole-concurrent");
    cap.run(async {
        use rdm_core::{Primary, PrimaryRole};
        use std::sync::atomic::{AtomicUsize, Ordering};
        let fabric = FabricId::mint();
        let server = node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "node-admin", |b| b).await;
        let stored_calls = Arc::new(AtomicUsize::new(0));
        let gone_calls = Arc::new(AtomicUsize::new(0));
        let lost_inc = IncarnationId::mint();
        {
            let d = own_digest(&server, &fabric);
            let stored_row = stored_of(&member_of(&fabric, "mesh4", "node", 1));
            let sc = stored_calls.clone();
            let stored: rdm_admin::topology_read::StoredSource = Arc::new(move || {
                sc.fetch_add(1, Ordering::SeqCst);
                let rows = vec![stored_row.clone()];
                Box::pin(async move {
                    Ok(rdm_admin::topology_read::StoredMap {
                        nodes: rows,
                        mesh_ids: [("mesh4".to_string(), MeshId::mint())].into_iter().collect(),
                    })
                })
            });
            let gc = gone_calls.clone();
            let lost = lost_inc.clone();
            let gone: rdm_admin::topology_read::GoneSource = Arc::new(move || {
                gc.fetch_add(1, Ordering::SeqCst);
                [lost.clone()].into_iter().collect()
            });
            let _ = server.slot.set(Arc::new(
                TopologyService::new(server.topology_view.clone(), Arc::new(move || d.clone()), server.time.clone())
                    .with_stored(stored)
                    .with_gone(gone),
            ));
        }

        let m1: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh1", "node", k)).collect();
        let m2: Vec<MeshDigest> = (1..=4).map(|k| member_of(&fabric, "mesh2", "node", k)).collect();
        let m3: Vec<MeshDigest> = (1..=2).map(|k| member_of(&fabric, "mesh3", "node", k)).collect();
        hold(&server.channels, &aggregate("mesh1", 3, m1));
        hold(&server.channels, &aggregate("mesh2", 7, m2));
        hold(&server.channels, &aggregate("mesh3", 11, m3));

        let fp_primary = Primary { mesh: "mesh1".into(), node_id: server.resolved.node_id.clone(), incarnation: server.resolved.incarnation.clone(), revision: 2 };
        let mp1 = fp_primary.clone();
        let mp2 = Primary { mesh: "mesh2".into(), node_id: NodeId::mint(), incarnation: lost_inc.clone(), revision: 4 };
        let mp3 = Primary { mesh: "mesh3".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 5 };
        server.topology_view.primaries().take(PrimaryRole::FabricPrimary, &fp_primary);
        server.topology_view.primaries().take(PrimaryRole::MeshPrimary, &mp1);
        server.topology_view.primaries().take(PrimaryRole::MeshPrimary, &mp2);
        server.topology_view.primaries().take(PrimaryRole::MeshPrimary, &mp3);

        // First: a single-mesh read of a held aggregate does not build the stored map, and reads gone once.
        let probe = node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "9").unwrap(), "node", |b| b).await;
        let t_probe = target_of(&probe, &server);
        let one_m2 = get_topology(&probe.client, &t_probe, "mesh1", &probe.channels, Some("mesh2"), None).await.unwrap();
        assert_eq!(stored_calls.load(Ordering::SeqCst), 0, "a held mesh's read never builds the stored map");
        assert_eq!(gone_calls.load(Ordering::SeqCst), 1, "gone incarnations are read once per read, not once per primary record");
        assert_eq!(one_m2.installed.len(), 1);

        let whole_caller = node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", "8").unwrap(), "node", |b| b).await;
        let t_whole = target_of(&whole_caller, &server);
        let whole = get_topology(&whole_caller.client, &t_whole, "mesh1", &whole_caller.channels, None, None).await.unwrap();
        assert_eq!(stored_calls.load(Ordering::SeqCst), 1, "the whole read builds the stored map once");
        assert_eq!(gone_calls.load(Ordering::SeqCst), 2, "the whole read also reads gone once");

        let member_ids = |installed: &[rdm_admin::topology_read::ReadMesh], mesh: &str| -> (u64, Vec<String>) {
            let m = installed.iter().find(|x| x.mesh == mesh).unwrap_or_else(|| panic!("missing {mesh}"));
            let mut ids: Vec<String> = m.members.iter().map(|d| d.node.node_id.to_string()).collect();
            ids.sort();
            (m.topology_version, ids)
        };
        let expected: std::collections::BTreeMap<String, (u64, Vec<String>)> = ["mesh1", "mesh2", "mesh3"]
            .into_iter()
            .map(|m| (m.to_string(), member_ids(&whole.installed, m)))
            .collect();

        // Concurrent readers asking for mesh1, mesh2, mesh3, and None simultaneously.
        let mut readers = Vec::new();
        for idx in 0..12u32 {
            readers.push(node(&fabric, "mesh1", rdm_core::PathName::new("mesh1.node", (10 + idx).to_string()).unwrap(), "node", |b| b).await);
        }
        let mut tasks = Vec::new();
        for (idx, r) in readers.iter().enumerate() {
            let t = target_of(r, &server);
            let client = r.client.clone();
            let view = r.channels.clone();
            let ask = match idx % 4 {
                0 => Some("mesh1"),
                1 => Some("mesh2"),
                2 => Some("mesh3"),
                _ => None,
            };
            let exp = expected.clone();
            let primaries_expected = whole.primaries.clone();
            let fp_expected = fp_primary.clone();
            let mp1_expected = mp1.clone();
            let mp2_expected = mp2.clone();
            let mp3_expected = mp3.clone();
            tasks.push(tokio::spawn(async move {
                let read = get_topology(&client, &t, "mesh1", &view, ask, None).await.unwrap();
                assert_eq!(read.rafka_time.expect("rafka-time served").kind, Some(PrimaryKind::FabricPrimary));
                assert_eq!(read.primaries, primaries_expected, "every read answers the exact same primary records and gone flags as the whole read");
                assert!(read.primaries.iter().any(|(s, p, gone)| *s == PrimaryRole::MeshPrimary && *p == mp2_expected && *gone), "mp2 is marked gone in read.roles");
                match ask {
                    Some(m) => {
                        assert_eq!(read.meshes, 1);
                        assert_eq!(read.installed.len(), 1);
                        assert!(read.stored.is_empty());
                        assert_eq!(member_ids(&read.installed, m), exp[m], "one-mesh read of {m} matches whole read");
                    }
                    None => {
                        assert_eq!(read.meshes, 4);
                        assert_eq!(read.installed.len(), 3);
                        assert_eq!(read.stored.len(), 1);
                        for m in ["mesh1", "mesh2", "mesh3"] {
                            assert_eq!(member_ids(&read.installed, m), exp[m], "whole read of {m} matches reference");
                        }
                    }
                }
                assert_eq!(view.topology_view().primaries().fabric(), Some(fp_expected));
                assert_eq!(view.topology_view().primaries().mesh("mesh1"), Some(mp1_expected));
                assert_eq!(view.topology_view().primaries().mesh("mesh2"), Some(mp2_expected));
                assert_eq!(view.topology_view().primaries().mesh("mesh3"), Some(mp3_expected));
            }));
        }
        for task in tasks {
            task.await.expect("concurrent reader succeeded");
        }
        assert_eq!(gone_calls.load(Ordering::SeqCst), 2 + 12, "every concurrent read invoked GoneSource once");
        assert_eq!(stored_calls.load(Ordering::SeqCst), 1 + 3, "only the 3 whole-fabric reads among the 12 concurrent reads built the stored map");

        for r in readers {
            r.ep.close().await;
        }
        whole_caller.ep.close().await;
        probe.ep.close().await;
        server.ep.close().await;
    });
    let serves = cap.named("rdm.mesh.topology.serve.via-read");
    assert_eq!(serves.len(), 14, "2 initial + 12 concurrent reads served: {serves:?}");
    assert!(serves.iter().all(|s| s["outcome"] == "served"), "{serves:?}");
}
