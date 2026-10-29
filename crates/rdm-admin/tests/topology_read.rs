//! `GetTopology` (op `0x1E`) over real Node RPC between two in-process nodes: what a node serves
//! from what it holds, how a caller installs it, and what it refuses by name.

use rdm_core::topology::PrimaryKind;
use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, PublisherId};
use rdm_transport::topology::TopologyView;
use rdm_transport::snapshot::{Chunk, SourceSnapshot, Taken};
use rdm_admin::topology_read::{get_topology, snapshot_replies, top_up_fetch, TopologyDoor, TopologyFailure, TopologySlot};
use rdm_transport::{NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::catalog::OpOwner;
use rdm_core::protocol::NodeProtocol;
use rdm_core::topology::{SourceVersion, Topology, TopologyReply, TopologyRequest};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};

fn member_of(fabric: &FabricId, mesh: &str, kind: &str, ordinal: u32) -> MeshDigest {
    MeshDigest { held_hash: Default::default(),
        fabric_id: fabric.clone(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: format!("{mesh}.{kind}.{ordinal}").parse().unwrap(),
            endpoint_id: EndpointId(format!("key-{mesh}-{kind}-{ordinal}-{}", "x".repeat(40))),
            transport_addr: format!("127.0.0.1:{}", 20000 + ordinal).parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::Ready,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

/// One node: an endpoint serving Node RPC and gossip, its topology view, and a topology slot.
struct Node {
    ep: iroh::Endpoint,
    topology_view: TopologyView,
    client: NodeRpcClient,
    resolver: Arc<StaticResolver>,
    slot: TopologySlot,
    gossip: iroh_gossip::net::Gossip,
    resolved: ResolvedNode,
    /// The rafka-time this node adopted: 5 000 000 ms, nowhere near its OS clock.
    time: rdm_transport::clock::RafkaTime,
    _router: Router,
}

async fn node(fabric: &FabricId, mesh: &str, name: &str, extra: impl FnOnce(ServerBuilder) -> ServerBuilder) -> Node {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let topology_view = TopologyView::join(&gossip, &ep, fabric, mesh, &MeshId::mint(), name, Arc::new(time.clone()), vec![], &[]).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = NodeRpcClient::new(ep.clone(), resolver.clone());
    let slot: TopologySlot = Arc::new(OnceLock::new());
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let server = extra(rdm_admin::topology_read::serve(ServerBuilder::new(), slot.clone()))
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .expect("the catalog is built");
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).accept(rdm_transport::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id, name: name.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation };
    Node { ep, topology_view, client, resolver, slot, gossip, resolved, time, _router: router }
}

fn own_digest(n: &Node, fabric: &FabricId) -> MeshDigest {
    let mut d = member_of(fabric, "mesh1", "rpc", 1);
    d.node.node_id = n.resolved.node_id.clone();
    d.node.name = n.resolved.name.clone();
    d.node.incarnation = n.resolved.incarnation.clone();
    d
}

fn open(n: &Node, fabric: &FabricId) {
    let d = own_digest(n, fabric);
    let _ = n.slot.set(Arc::new(TopologyDoor::new(n.topology_view.clone(), Arc::new(move || d.clone()), n.time.clone())));
}

fn publisher(node: &str) -> PublisherId {
    PublisherId { node: node.into(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("birth-1")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() }
}

/// `s` held by `m` as a complete snapshot its primary put into the mesh.
fn hold(m: &TopologyView, s: &SourceSnapshot) {
    for r in snapshot_replies(s, 1) {
        let TopologyReply::Snapshot { mesh, publisher, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, deleted: deleted } = r else { unreachable!() };
        let t = m.take_read_chunk(Chunk {
            mesh,
            publisher,
            forwarded_by: Some("topology-read".into()),
            topology_version,
            snapshot_id,
            chunk_index,
            chunk_count,
            digests: digests.into_iter().map(MeshDigest::from).collect(),
            in_flight,
            deleted,
        }, "peer");
        assert!(!matches!(t, Taken::Refused(_)), "{t:?}");
    }
}

fn snap(mesh: &str, p: &PublisherId, version: u64, digests: Vec<MeshDigest>) -> SourceSnapshot {
    SourceSnapshot { mesh: mesh.into(), publisher: p.clone(), topology_version: version, digests, in_flight: vec![], deleted: vec![] }
}

fn target_of(from: &Node, to: &Node) -> NodeTarget {
    from.resolver.insert(to.resolved.clone());
    NodeTarget::ExactNode(to.resolved.node_id.clone())
}

// @feature: node-lifecycle
/// CONTRACT: a node that has not yet opened its topology door (it is mid-birth, serving calls
/// while its join is outstanding) answers NotReady by name, and the caller installs nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_mid_birth_answers_not_ready_and_the_caller_installs_nothing() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    let t = target_of(&caller, &server);
    let err = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap_err();
    assert!(matches!(&err, TopologyFailure::NotReady(r) if r.contains("no topology view")), "{err}");
    assert!(caller.topology_view.held_source_version("mesh1").is_none());
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a mesh the node holds no published snapshot of is absent from its answer, its own
/// mesh included: a node whose mesh primary has put no version into the mesh answers with the
/// meshes it does hold (none is a complete, empty answer), and a read naming the absent mesh is
/// UnknownMesh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_with_no_published_snapshot_is_absent_from_the_answer() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    open(&server, &fabric);
    let t = target_of(&caller, &server);
    let empty = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap();
    assert!(empty.installed.is_empty() && empty.meshes == 0, "nothing held: an empty, complete answer");

    let mesh2: Vec<MeshDigest> = (1..=2).map(|k| member_of(&fabric, "mesh2", "rpc", k)).collect();
    hold(&server.topology_view, &snap("mesh2", &publisher("mesh2.admin.1"), 7, mesh2));
    let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap();
    assert_eq!((read.meshes, read.installed.len()), (1, 1));
    assert_eq!(read.installed[0].mesh, "mesh2");
    assert!(caller.topology_view.held_source_version("mesh1").is_none(), "no version of the own mesh is invented");

    let err = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh1"), None).await.unwrap_err();
    assert_eq!(err, TopologyFailure::Refused("the target holds no mesh mesh1".into()));
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a mesh primary that has published nothing yet still holds its mesh's members in its
/// view, and a read answers with them as its own mesh at version 0 under its own publisher: the
/// caller installs them. A read is never an empty answer for a primary that holds members.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_primary_that_has_published_nothing_serves_the_members_its_view_holds() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    open(&server, &fabric);
    let backbone = rdm_transport::topology::Backbone::join(&server.gossip, &server.ep, &server.topology_view, "mesh1", rdm_core::PublisherId { node: "mesh1.rpc.1".to_string(), incarnation: server.resolved.incarnation.clone(), node_id: server.resolved.node_id.clone() }, vec![]).await.unwrap();
    backbone.set_mesh_primary(true);
    server.topology_view.digest_view.record(member_of(&fabric, "mesh1", "rpc", 2));
    let t = target_of(&caller, &server);
    let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap();
    assert_eq!((read.meshes, read.installed.len()), (1, 1), "the primary's own mesh is served");
    assert_eq!((read.installed[0].mesh.as_str(), read.installed[0].topology_version), ("mesh1", 0));
    assert_eq!(read.installed[0].members.len(), 2, "its own digest and the member its view holds");
    assert_eq!(read.installed[0].publisher.node, "mesh1.rpc.1");
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: an ordinary member answers with the topology it holds: its own mesh as its primary
/// last published it, at that version, and every source mesh it holds at that source's
/// publisher and version. The caller installs each mesh, and a read of a source version the
/// caller already holds is Unchanged, a mesh the node does not hold is UnknownMesh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_member_serves_the_topology_it_holds_and_answers_unchanged_and_unknown_mesh() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    open(&server, &fabric);
    let (own_pub, remote_pub) = (publisher("mesh1.admin.1"), publisher("mesh2.admin.1"));
    hold(&server.topology_view, &snap("mesh1", &own_pub, 4, vec![own_digest(&server, &fabric)]));
    let mesh2: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh2", "rpc", k)).collect();
    hold(&server.topology_view, &snap("mesh2", &remote_pub, 7, mesh2.clone()));
    let t = target_of(&caller, &server);

    let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap();
    assert_eq!(read.meshes, 2);
    let names = |mesh: &str| -> Vec<String> {
        let mut v: Vec<String> = read.installed.iter().find(|m| m.mesh == mesh).unwrap().members.iter().map(|d| d.node.name.to_string()).collect();
        v.sort();
        v
    };
    assert_eq!(names("mesh2"), vec!["mesh2.rpc.1", "mesh2.rpc.2", "mesh2.rpc.3"]);
    assert_eq!(names("mesh1"), vec!["mesh1.rpc.1"], "its own mesh: version 4 as its primary published it");
    assert_eq!(caller.topology_view.held_source_version("mesh2"), Some((remote_pub.clone(), 7)), "installed at the source's publisher and version");
    assert_eq!(caller.topology_view.held_source_version("mesh1"), Some((own_pub.clone(), 4)));

    let held = SourceVersion { publisher: remote_pub.clone(), topology_version: 7 };
    let again = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh2"), Some(held)).await.unwrap();
    assert!(again.installed.is_empty());
    assert_eq!(again.unchanged, vec![("mesh2".to_string(), 7)]);
    assert_eq!(again.meshes, 1, "End counts the meshes answered, Unchanged included");

    let behind = SourceVersion { publisher: remote_pub.clone(), topology_version: 6 };
    let newer = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh2"), Some(behind)).await.unwrap();
    assert!(newer.unchanged.is_empty() && newer.installed.len() == 1, "a caller behind the node's version is sent the snapshot");

    let err = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh9"), None).await.unwrap_err();
    assert_eq!(err, TopologyFailure::Refused("the target holds no mesh mesh9".into()));
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT (gates e6s8 c82f213e2 and e6s7 19f396938: `mesh1.admin.2`'s first start read its
/// launcher's topology, served at `topology_version = 0` before the launcher's first publication;
/// that publication, `topology_version = 1`, arrived on the backbone first and the read's chunk was
/// refused `OlderThanHeld { held_version: 1, offered_version: 0 }`, so the read reported no mesh
/// installed and the admin took itself for its mesh's first admin, never Ready): a read whose
/// snapshot of a mesh is older than the complete one the caller already installed of the same
/// source leaves that newer snapshot installed (node-start.md step 4, "Install only complete
/// source snapshots"), and the read reports it, at the version and with the members the caller
/// holds, as the mesh the read leaves installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_of_a_snapshot_older_than_the_held_one_reports_the_snapshot_the_caller_holds() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.admin.1", |b| b).await, node(&fabric, "mesh1", "mesh1.admin.2", |b| b).await);
    open(&server, &fabric);
    let own_pub = publisher("mesh1.admin.1");
    let first = member_of(&fabric, "mesh1", "admin", 1);
    let second = member_of(&fabric, "mesh1", "admin", 2);
    // The target serves the version before the one the caller already holds of the same source.
    hold(&server.topology_view, &snap("mesh1", &own_pub, 1, vec![first.clone()]));
    hold(&caller.topology_view, &snap("mesh1", &own_pub, 2, vec![first.clone(), second.clone()]));
    let t = target_of(&caller, &server);

    let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap();
    assert_eq!(caller.topology_view.held_source_version("mesh1"), Some((own_pub.clone(), 2)), "the held projection stands");
    let m = read.installed.iter().find(|m| m.mesh == "mesh1").expect("the read reports the mesh it leaves installed: the newer snapshot the caller holds");
    assert_eq!((&m.publisher, m.topology_version), (&own_pub, 2), "at the version the caller holds, never the one served");
    let mut ids: Vec<String> = m.members.iter().map(|d| d.node.node_id.to_string()).collect();
    ids.sort();
    let mut want = vec![first.node.node_id.to_string(), second.node.node_id.to_string()];
    want.sort();
    assert_eq!(ids, want, "with the members of the snapshot the caller holds");
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a topology larger than one message is chunked by the one packing gossip uses, every
/// chunk under the reply ceiling and the gossip message bound, and the caller installs the mesh
/// whole: every member, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_topology_streams_in_chunks_under_the_ceiling_and_installs_whole() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    open(&server, &fabric);
    hold(&server.topology_view, &snap("mesh1", &publisher("mesh1.admin.1"), 1, vec![]));
    let big: Vec<MeshDigest> = (1..=300).map(|k| member_of(&fabric, "mesh2", "rpc", k)).collect();
    let source = snap("mesh2", &publisher("mesh2.admin.1"), 3, big);
    let chunks = snapshot_replies(&source, 1);
    assert!(chunks.len() > 2, "{} chunks", chunks.len());
    for c in &chunks {
        let len = Topology::encode_reply(c).unwrap().len();
        assert!(len <= Topology::MAX_REPLY_FRAME_BYTES, "a chunk of {len} bytes is over the ceiling");
        assert!(len <= rdm_transport::topology::MAX_FRAME, "a chunk of {len} bytes is over the gossip message bound");
    }
    hold(&server.topology_view, &source);
    let t = target_of(&caller, &server);
    let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh2"), None).await.unwrap();
    let m = &read.installed[0];
    let mut ids: Vec<String> = m.members.iter().map(|d| d.node.node_id.to_string()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!((m.members.len(), ids.len()), (300, 300), "every member, once");
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a snapshot whose chunks do not all arrive is never installed: the caller's held
/// projection stands and the read ends without installing the mesh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_missing_a_chunk_is_never_installed() {
    let fabric = FabricId::mint();
    let sent: Arc<Mutex<u32>> = Arc::default();
    let counted = sent.clone();
    let server = node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await;
    // A server that streams the first chunk of a two-chunk snapshot and then ends the stream.
    let lossy_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let (lid, linc) = (NodeId::mint(), IncarnationId::mint());
    let lossy = ServerBuilder::new()
        .serve_stream::<Topology, _, _>(OpOwner::Product("rdm".into()), move |_peer, _req: TopologyRequest, sink| {
            let counted = counted.clone();
            async move {
                let mut sink = sink.started(TopologyReply::Started).await.unwrap();
                sink.data(TopologyReply::Snapshot {
                    mesh: "mesh2".into(),
                    publisher: publisher("mesh2.admin.1"),
                    topology_version: 1,
                    snapshot_id: 1,
                    chunk_index: 0,
                    chunk_count: 2,
                    digests: vec![],
                    in_flight: vec![],
                    deleted: vec![],
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
    let lossy_node = ResolvedNode { node_id: lid.clone(), name: "mesh1.rpc.9".parse().unwrap(), endpoint_id: lossy_ep.id(), transport_addr: lossy_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap(), incarnation: linc };
    server.resolver.insert(lossy_node);
    let read = get_topology(&server.client, &NodeTarget::ExactNode(lid), "mesh1", &server.topology_view, None, None).await.unwrap();
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
/// (`requested` names the mesh) with no mesh or snapshot streamed.
#[test]
fn a_request_naming_a_mesh_the_node_never_heard_is_unknown_mesh_and_streams_nothing() {
    let cap = capture("unknown-mesh");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
        open(&server, &fabric);
        hold(&server.topology_view, &snap("mesh2", &publisher("mesh2.admin.1"), 3, vec![member_of(&fabric, "mesh2", "rpc", 1)]));
        let t = target_of(&caller, &server);
        let err = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh77"), None).await.unwrap_err();
        assert_eq!(err, TopologyFailure::Refused("the target holds no mesh mesh77".into()));
        assert!(caller.topology_view.held_source_version("mesh2").is_none() && caller.topology_view.held_source_version("mesh77").is_none());
        server.ep.close().await;
        caller.ep.close().await;
    });
    let serves = cap.named("rdm.mesh.topology.serve.via-read");
    assert_eq!(serves.len(), 1, "{serves:?}");
    assert_eq!((serves[0]["outcome"].as_str(), serves[0]["requested"].as_str()), ("unknown-mesh", "mesh77"));
    assert!(!serves[0].contains_key("snapshots"), "nothing was streamed: {:?}", serves[0]);
    assert!(cap.named("rdm.mesh.topology.update.via-read-install").is_empty(), "nothing was installed");
}

// @feature: node-lifecycle
/// CONTRACT: `since` is the source version the caller holds, scoped by the publisher's exact
/// birth. The same topology_version from another publisher revision is not held: the node streams the
/// snapshot (never Unchanged), and the serve span counts one snapshot and no unchanged. A `since`
/// with no mesh named is ignored: every mesh is streamed.
#[test]
fn a_since_from_another_publisher_revision_is_answered_with_the_snapshot_not_unchanged() {
    let cap = capture("since-revision");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
        open(&server, &fabric);
        let publisher1 = publisher("mesh2.admin.1");
        hold(&server.topology_view, &snap("mesh2", &publisher1, 7, (1..=2).map(|k| member_of(&fabric, "mesh2", "rpc", k)).collect()));
        let t = target_of(&caller, &server);
        let publisher2 = PublisherId { node: "mesh2.admin.1".into(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("birth-2")).unwrap(), node_id: rdm_core::NodeId::parse("0123456789ab").unwrap() };
        let other_publisher = SourceVersion { publisher: publisher2, topology_version: 7 };
        let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh2"), Some(other_publisher.clone())).await.unwrap();
        assert!(read.unchanged.is_empty() && read.installed.len() == 1, "{read:?}");
        assert_eq!(caller.topology_view.held_source_version("mesh2"), Some((publisher1.clone(), 7)), "installed at the publisher it was held from");
        // The same version from the held publisher is Unchanged; a `since` with no mesh named is ignored.
        let held = SourceVersion { publisher: publisher1, topology_version: 7 };
        let unchanged = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh2"), Some(held.clone())).await.unwrap();
        assert_eq!(unchanged.unchanged, vec![("mesh2".to_string(), 7)]);
        let all = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, Some(held)).await.unwrap();
        assert!(all.unchanged.is_empty() && all.installed.len() == 1, "since is honoured only with a mesh named: {all:?}");
        server.ep.close().await;
        caller.ep.close().await;
    });
    let serves = cap.named("rdm.mesh.topology.serve.via-read");
    let counts: Vec<(&str, &str, &str)> = serves.iter().map(|a| (a["requested"].as_str(), a["snapshots"].as_str(), a["unchanged"].as_str())).collect();
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
        let (server, caller) = (node(&fabric, "mesh2", "mesh2.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
        open(&server, &fabric);
        let own: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh1", "rpc", k)).collect();
        let third: Vec<MeshDigest> = (1..=2).map(|k| member_of(&fabric, "mesh3", "rpc", k)).collect();
        hold(&server.topology_view, &snap("mesh1", &publisher("mesh1.admin.1"), 9, own.clone()));
        hold(&server.topology_view, &snap("mesh3", &publisher("mesh3.admin.1"), 2, third.clone()));
        let t = target_of(&caller, &server);
        let read = get_topology(&caller.client, &t, "mesh2", &caller.topology_view, None, None).await.unwrap();
        let members_of = |mesh: &str| read.installed.iter().find(|m| m.mesh == mesh).map(|m| m.members.len());
        assert_eq!((members_of("mesh1"), members_of("mesh3")), (Some(3), Some(2)), "the reader is given every mesh the node holds");
        for d in &own {
            assert!(caller.topology_view.digest_view.get(d.node.node_id.as_str()).is_none(), "{} of the reader's own mesh is not held as heard from a peer mesh's stale source", d.node.name);
        }
        for d in &third {
            assert!(caller.topology_view.digest_view.get(d.node.node_id.as_str()).is_some(), "{} of a third mesh is held as topology", d.node.name);
        }
        server.ep.close().await;
        caller.ep.close().await;
    });
    let installs = cap.named("rdm.mesh.topology.update.via-read-install");
    let mut got: Vec<(&str, &str, &str)> = installs.iter().map(|a| (a["node"].as_str(), a["mesh"].as_str(), a["members"].as_str())).collect();
    got.sort();
    assert_eq!(got, vec![("mesh1.rpc.2", "mesh1", "3"), ("mesh1.rpc.2", "mesh3", "2")], "{installs:?}");
}

// @feature: node-lifecycle
/// CONTRACT: a topology read of a mesh that has a birth being deleted carries the deletion: the
/// node serves the held source's in-flight operation with the member still listed, and the reader
/// holds the member as deleting (its overlay names the operation) rather than as a plain member.
#[test]
fn a_read_while_a_birth_of_the_mesh_is_being_deleted_carries_the_deleting_overlay() {
    let cap = capture("retiring");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
        open(&server, &fabric);
        let members: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh3", "rpc", k)).collect();
        let retiring = &members[1];
        let op = rdm_core::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "node.delete:mesh3.rpc.2".into(),
            node_id: retiring.node.node_id.clone(),
            incarnation: retiring.node.incarnation.clone(),
            name: retiring.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        };
        let mut s = snap("mesh3", &publisher("mesh3.admin.1"), 4, members.clone());
        s.in_flight = vec![op.clone()];
        hold(&server.topology_view, &s);
        let t = target_of(&caller, &server);
        let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh3"), None).await.unwrap();
        assert_eq!(read.installed[0].members.len(), 3, "the retiring member is still listed");
        let (in_flight, _) = caller.topology_view.digest_view.overlays_of("mesh3");
        assert_eq!(in_flight, vec![op], "the reader holds the deletion as an overlay");
        server.ep.close().await;
        caller.ep.close().await;
    });
    let serve = cap.named("rdm.mesh.topology.serve.via-read");
    assert_eq!(serve.len(), 1);
    assert_eq!((serve[0]["outcome"].as_str(), serve[0]["snapshots"].as_str()), ("served", "1"));
}

// @feature: node-lifecycle
/// CONTRACT: an admin that joins after a node was stopped never heard the node's digest; a
/// `GetTopology` read of the mesh carries the stop overlay, the reader holds the birth stopped from
/// it, and the reader's view lists the node `Leaving` and stopped, never absent.
#[test]
fn a_late_reader_lists_a_stopped_node_from_the_stop_overlay_a_read_carries() {
    let cap = capture("stopped-late");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
        open(&server, &fabric);
        let members: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh3", "rpc", k)).collect();
        let stopped = &members[1];
        let op = rdm_core::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "node.stop:mesh3.rpc.2".into(),
            node_id: stopped.node.node_id.clone(),
            incarnation: stopped.node.incarnation.clone(),
            name: stopped.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        };
        // The mesh primary's snapshot after the stop: the stopped birth's digest is dropped, its overlay stays.
        let kept: Vec<MeshDigest> = members.iter().filter(|d| d.node.node_id != stopped.node.node_id).cloned().collect();
        let mut s = snap("mesh3", &publisher("mesh3.admin.1"), 4, kept);
        s.in_flight = vec![op.clone()];
        hold(&server.topology_view, &s);
        let t = target_of(&caller, &server);
        get_topology(&caller.client, &t, "mesh1", &caller.topology_view, Some("mesh3"), None).await.unwrap();
        assert!(caller.topology_view.digest_view.get(stopped.node.node_id.as_str()).is_none(), "the reader never held the stopped node's digest");
        assert_eq!(caller.topology_view.digest_view.stopped_unheard(), vec![op]);
        let view = rdm_admin::admin::project("fabric1", &fabric, rdm_admin::model::ProviderKind::Process, &caller.topology_view.digest_view, &rdm_admin::admin::Records::default());
        let n = view.node(&stopped.node.name).expect("the stopped node is listed");
        assert_eq!((n.status, n.stopped, n.routable), (rdm_admin::model::NodeStatus::Leaving, true, false));
        server.ep.close().await;
        caller.ep.close().await;
    });
}

fn stored_of(d: &MeshDigest) -> rdm_core::topology::StoredNode {
    rdm_core::topology::StoredNode { node_id: d.node.node_id.clone(), name: d.node.name.to_string(), endpoint_id: d.node.endpoint_id.clone(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr }
}

/// `n` serves the births `rows` as its stored map.
fn open_with_stored(n: &Node, fabric: &FabricId, rows: Vec<rdm_core::topology::StoredNode>) {
    let d = own_digest(n, fabric);
    let source: rdm_admin::topology_read::StoredSource = Arc::new(move || {
        let rows = rows.clone();
        Box::pin(async move { Ok(rdm_admin::topology_read::StoredMap { nodes: rows, mesh_ids: [("mesh1".to_string(), MeshId::parse("04raj09p3zp7").unwrap())].into_iter().collect() }) })
    });
    let _ = n.slot.set(Arc::new(TopologyDoor::new(n.topology_view.clone(), Arc::new(move || d.clone()), n.time.clone()).with_stored(source)));
}

// @feature: node-lifecycle
/// CONTRACT: a node that holds no gossiped snapshot of a mesh but stores births of it answers that
/// mesh with its stored map, marked as having no version: the reader gets the births to reach and
/// installs nothing. The stored mesh holds no source version at the reader, none of its nodes is in
/// the reader's view, and a mesh the node does hold a snapshot of is answered by the snapshot, not
/// the stored map. The node never answers its own mesh from the stored map. The serve span counts
/// the stored mesh and streams no snapshot of it.
#[test]
fn a_stored_answer_is_never_installed_as_current_topology() {
    let cap = capture("stored");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh2", "mesh2.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
        let lost: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh1", "rpc", k)).collect();
        let held: Vec<MeshDigest> = (1..=2).map(|k| member_of(&fabric, "mesh3", "rpc", k)).collect();
        let own: Vec<MeshDigest> = vec![member_of(&fabric, "mesh2", "rpc", 9)];
        // The node stores births of the lost mesh, of a mesh it holds a snapshot of, and of its own.
        let rows: Vec<_> = lost.iter().chain(held.iter()).chain(own.iter()).map(stored_of).collect();
        open_with_stored(&server, &fabric, rows);
        hold(&server.topology_view, &snap("mesh3", &publisher("mesh3.admin.1"), 5, held.clone()));
        let t = target_of(&caller, &server);
        let read = get_topology(&caller.client, &t, "mesh2", &caller.topology_view, None, None).await.unwrap();
        assert_eq!(read.installed.iter().map(|m| m.mesh.as_str()).collect::<Vec<_>>(), vec!["mesh3"], "the held snapshot is answered as a snapshot");
        assert_eq!(read.stored.len(), 1, "only the lost mesh is answered from the stored map: {:?}", read.stored);
        assert_eq!((read.stored[0].mesh.as_str(), read.stored[0].nodes.len()), ("mesh1", 3));
        assert_eq!(read.stored[0].mesh_id, Some(MeshId::parse("04raj09p3zp7").unwrap()), "the stored mesh carries the id the node stored for it");
        assert_eq!(read.meshes, 2, "End counts the snapshot mesh and the stored mesh");
        assert!(caller.topology_view.held_source_version("mesh1").is_none(), "a stored map carries no version, so no source is held for it");
        for d in &lost {
            assert!(caller.topology_view.digest_view.get(d.node.node_id.as_str()).is_none(), "{} of a stored map is not held by the reader", d.node.name);
        }
        // Asking for the stored mesh by name is answered the same way; a mesh stored nowhere is unknown.
        let one = get_topology(&caller.client, &t, "mesh2", &caller.topology_view, Some("mesh1"), None).await.unwrap();
        assert!(one.installed.is_empty() && one.stored.len() == 1 && one.meshes == 1, "{one:?}");
        let err = get_topology(&caller.client, &t, "mesh2", &caller.topology_view, Some("mesh8"), None).await.unwrap_err();
        assert_eq!(err, TopologyFailure::Refused("the target holds no mesh mesh8".into()));
        server.ep.close().await;
        caller.ep.close().await;
    });
    let serves = cap.named("rdm.mesh.topology.serve.via-read");
    let rows: Vec<(&str, &str, &str, &str)> = serves.iter().map(|a| (a["requested"].as_str(), a["outcome"].as_str(), a.get("stored").map(|s| s.as_str()).unwrap_or("-"), a.get("snapshots").map(|s| s.as_str()).unwrap_or("-"))).collect();
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
    use rdm_core::{NodeKind, Seat, Primary};
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    open(&server, &fabric);
    let t = target_of(&caller, &server);
    let held = |seat| (seat, Primary { mesh: "mesh1".into(), node_id: server.resolved.node_id.clone(), incarnation: server.resolved.incarnation.clone(), revision: 1 });

    let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap();
    let served = read.rafka_time.expect("every accepted read carries rafka-time");
    assert!((5_000_000..5_060_000).contains(&served.ms), "the node's adopted time, not an OS clock: {}", served.ms);
    assert_eq!(served.kind, None, "a node that is no primary answers none");

    let (seat, holder) = held(Seat::MeshPrimary);
    server.topology_view.primaries().take(seat, &holder);
    let rpc_holder = Primary { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 2 };
    let broker_holder = Primary { mesh: "mesh2".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision: 3 };
    server.topology_view.primaries().take(Seat::Cohort(NodeKind::RpcNode), &rpc_holder);
    server.topology_view.primaries().take(Seat::Cohort(NodeKind::Broker), &broker_holder);
    let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap();
    assert_eq!(read.rafka_time.unwrap().kind, Some(PrimaryKind::MeshPrimary));
    assert_eq!(caller.topology_view.primaries().mesh("mesh1"), Some(holder.clone()));
    // The read carries the mesh-primaries and the fabric-primary (node-rpc-envelope.md `Primaries`); every
    // other type's primary rides its digest and its mesh-primary's projection (`is_primary`).
    assert_eq!(caller.topology_view.primaries().cohort("mesh1", NodeKind::RpcNode), None, "another type's primary is not carried by the read: {rpc_holder:?}");
    assert_eq!(caller.topology_view.primaries().cohort("mesh2", NodeKind::Broker), None, "nor a peer mesh's: {broker_holder:?}");

    let (seat, holder) = held(Seat::FabricPrimary);
    server.topology_view.primaries().take(seat, &holder);
    let read = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap();
    assert_eq!(read.rafka_time.unwrap().kind, Some(PrimaryKind::FabricPrimary), "the fabric-primary outranks the mesh-primary it also is");
    assert_eq!(caller.topology_view.primaries().fabric(), Some(holder));
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a node that has adopted no rafka-time has none to serve: a read is NotReady naming
/// why, and the stream never starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_holding_no_rafka_time_answers_not_ready_by_name() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    let d = own_digest(&server, &fabric);
    let _ = server.slot.set(Arc::new(TopologyDoor::new(server.topology_view.clone(), Arc::new(move || d.clone()), rdm_transport::clock::RafkaTime::unadopted())));
    let t = target_of(&caller, &server);
    let err = get_topology(&caller.client, &t, "mesh1", &caller.topology_view, None, None).await.unwrap_err();
    assert!(matches!(&err, TopologyFailure::NotReady(r) if r.contains("holds no rafka-time")), "{err}");
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a member adopts the rafka-time of whichever node answers; a mesh primary adopts only
/// from a fabric-primary seat, and refuses the answer of a replica (`served-by-a-replica`) or of
/// another mesh primary (`a-mesh-primary-takes-only-the-fabric-primary`) by name, keeping the
/// time it holds.
#[test]
fn a_mesh_primary_refuses_the_time_of_a_replica_and_a_mesh_primary_and_adopts_a_fabric_primarys() {
    let cap = capture("rafka-time-verdict");
    cap.run(async {
        use rdm_core::{Seat, Primary};
        use rdm_admin::rafka_time::Refusal;
        use rdm_admin::topology_read::TimeNotAdopted;
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh2", "mesh2.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.admin.2", |b| b).await);
        open(&server, &fabric);
        caller.time.adopt(9_000_000);
        let t = target_of(&caller, &server);
        let caller_id = NodeId::mint();
        // The caller holds its mesh's primary seat in its own records.
        caller.topology_view.primaries().take(Seat::MeshPrimary, &Primary { mesh: "mesh1".into(), node_id: caller_id.clone(), incarnation: IncarnationId::mint(), revision: 1 });
        let pull = || async {
            let read = get_topology(&caller.client, &t, "mesh2", &caller.topology_view, None, None).await.unwrap();
            read.adopt_rafka_time(&caller.time, &caller.topology_view, "mesh1.admin.2", "mesh1", &caller_id, "mesh2.rpc.1")
        };
        assert_eq!(pull().await.unwrap_err(), TimeNotAdopted::Refused(Refusal::ServedByAReplica));
        assert!(caller.time.now_ms() >= 9_000_000 && caller.time.now_ms() < 9_060_000, "the replica's 5 000 000 was not taken");

        server.topology_view.primaries().take(Seat::MeshPrimary, &Primary { mesh: "mesh2".into(), node_id: server.resolved.node_id.clone(), incarnation: server.resolved.incarnation.clone(), revision: 1 });
        assert_eq!(pull().await.unwrap_err(), TimeNotAdopted::Refused(Refusal::AMeshPrimaryTakesOnlyTheFabricPrimary));

        server.topology_view.primaries().take(Seat::FabricPrimary, &Primary { mesh: "mesh2".into(), node_id: server.resolved.node_id.clone(), incarnation: server.resolved.incarnation.clone(), revision: 1 });
        let adopted = pull().await.unwrap();
        assert_eq!(adopted.previous_ms.map(|p| p >= 9_000_000), Some(true));
        assert!(adopted.stalls_ms > 3_000_000, "the fabric-primary's earlier time is behind: the reader stalls, it never steps back: {adopted:?}");
        assert!(caller.time.now_ms() >= 9_000_000, "never back");
        server.ep.close().await;
        caller.ep.close().await;
    });
    let refused = cap.named("rdm.mesh.entry.reject.via-rafka-time-from-non-primary");
    let reasons: Vec<(&str, &str, &str)> = refused.iter().map(|a| (a["node"].as_str(), a["kind"].as_str(), a["reason"].as_str())).collect();
    assert_eq!(reasons, vec![("mesh1.admin.2", "none", "served-by-a-replica"), ("mesh1.admin.2", "mesh-primary", "a-mesh-primary-takes-only-the-fabric-primary")]);
    let adopted = cap.named("rdm.mesh.entry.update.via-rafka-time-adopted");
    assert_eq!(adopted.len(), 1, "only the fabric-primary's answer was adopted: {adopted:?}");
    assert_eq!((adopted[0]["source"].as_str(), adopted[0]["via"].as_str(), adopted[0]["kind"].as_str(), adopted[0]["served_by"].as_str()), ("pull", "get-topology", "fabric-primary", "mesh2.rpc.1"));
}

// @feature: node-lifecycle
/// CONTRACT (gossip.md §3.3: a desynchronized receiver "pulls the current full fabric topology from
/// its own current mesh-primary with GetTopology, installs it atomically, records the publisher and
/// version baseline, and resumes"; lifecycles/node-topology-get.md: the top-up "is a read/connection
/// repair, not a stop, restart or new admission"; origin-ordering.md, rafka-time: adopted at every
/// JoinNode, and "between pulls a node keeps its rafka-time disciplined from its own authority's
/// heartbeats only"): a member that tops up a desynchronized source from its mesh's primary installs
/// the complete snapshot and keeps the rafka-time it holds. Nothing is adopted, so its reading is not
/// stalled and its discipline window and slew are not cleared. Must NOT happen: the top-up re-anchors
/// the member to the served time (gate e588aaea, rafka_time_discipline: mesh1.rpc.1's version-gap
/// top-up adopted mesh1.admin.1's served time with stalls_ms=1501, erasing the 1.5 s the cell set, so
/// the discipline never slewed; trace 1074d9baac76f1c2e8088641cbbea279).
#[test]
fn a_member_topping_up_a_desynced_source_installs_it_and_keeps_its_rafka_time() {
    let cap = capture("top-up-keeps-time");
    cap.run(async {
        let fabric = FabricId::mint();
        let (server, caller) = (node(&fabric, "mesh1", "mesh1.admin.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await);
        open(&server, &fabric);
        hold(&server.topology_view, &snap("mesh2", &publisher("mesh2.admin.1"), 7, (1..=2).map(|k| member_of(&fabric, "mesh2", "rpc", k)).collect()));
        // The member stands far ahead of the primary's 5 000 000 ms: an adoption would stall it.
        caller.time.adopt(9_000_000);
        let _ = target_of(&caller, &server);
        let mut primary = own_digest(&server, &fabric);
        primary.node.name = server.resolved.name.clone();
        let fetch = top_up_fetch(Arc::new(NodeRpcClient::new(caller.ep.clone(), caller.resolver.clone())));
        let done = fetch(caller.topology_view.clone(), primary, vec!["mesh2".into()]).await.unwrap();
        assert_eq!(done.installed, vec![("mesh2".to_string(), 7)], "the top-up installed the complete snapshot");
        let before = caller.time.now_ms();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let after = caller.time.now_ms();
        assert!(after >= before + 50, "the member's rafka-time runs on from what it held, unstalled: {before} -> {after}");
        server.ep.close().await;
        caller.ep.close().await;
    });
    assert!(cap.named("rdm.mesh.entry.update.via-rafka-time-adopted").is_empty(), "a top-up adopts no rafka-time: {:?}", cap.named("rdm.mesh.entry.update.via-rafka-time-adopted"));
    let installed = cap.named("rdm.mesh.topology.update.via-read-install");
    assert_eq!(installed.iter().map(|a| (a["node"].as_str(), a["mesh"].as_str(), a["topology_version"].as_str())).collect::<Vec<_>>(), vec![("mesh1.rpc.1", "mesh2", "7")], "{installed:?}");
}

// @feature: rafka-time
/// CONTRACT: docs/architecture/fabric-node-hooks.md:150
/// "RDM exposes it to the application through one public, cloneable `RafkaTime` handle with
/// `now_ms()`. The app gets the handle from the running node returned by RDM's start, and from
/// `NodeReadyCtx`. It is the same instance every RDM stamp reads; there is no second clock."
#[test]
fn mesh_primary_pulls_rafka_time_from_new_fabric_primary_on_primary_change_into_shared_handle() {
    let cap = capture("rafka-time-shared-handle-pull");
    cap.run(async {
        use rdm_core::{Primary, Seat};
        let fabric = FabricId::mint();
        let (new_fp, mesh2_primary) = (
            node(&fabric, "mesh1", "mesh1.admin.2", |b| b).await,
            node(&fabric, "mesh2", "mesh2.admin.1", |b| b).await,
        );
        new_fp.time.adopt(12_000_000);
        open(&new_fp, &fabric);
        hold(
            &new_fp.topology_view,
            &snap("mesh1", &publisher("mesh1.admin.2"), 1, vec![own_digest(&new_fp, &fabric)]),
        );
        new_fp.topology_view.primaries().take(
            Seat::FabricPrimary,
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
            Seat::MeshPrimary,
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
            &mesh2_primary.topology_view,
            Some("mesh1"),
            None,
        )
        .await
        .expect("topology read from new fabric-primary succeeds");
        let adopted = read
            .adopt_rafka_time(
                &mesh2_primary.time,
                &mesh2_primary.topology_view,
                "mesh2.admin.1",
                "mesh2",
                &mesh2_primary_id,
                "mesh1.admin.2",
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
    });
    let adopted = cap.named("rdm.mesh.entry.update.via-rafka-time-adopted");
    assert_eq!(adopted.len(), 1, "expected via-rafka-time-adopted span: {adopted:?}");
    assert_eq!(adopted[0]["node"].as_str(), "mesh2.admin.1");
    assert_eq!(adopted[0]["source"].as_str(), "pull");
    assert_eq!(adopted[0]["via"].as_str(), "get-topology");
    assert_eq!(adopted[0]["kind"].as_str(), "fabric-primary");
    assert_eq!(adopted[0]["served_by"].as_str(), "mesh1.admin.2");
}
