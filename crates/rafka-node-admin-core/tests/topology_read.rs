//! `GetTopology` (op `0x1E`) over real Node RPC between two in-process nodes: what a node serves
//! from what it holds, how a caller installs it, and what it refuses by name.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId, PublisherId};
use rafka_mesh_transport::membership::Membership;
use rafka_mesh_transport::snapshot::{Chunk, SourceSnapshot, Taken};
use rafka_node_admin_core::topology_read::{get_topology, get_topology_when_ready, snapshot_replies, TopologyDoor, TopologyFailure, TopologySlot};
use rafka_node_rpc::{NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_node_rpc_contract::topology::{SourceVersion, Topology, TopologyReply, TopologyRequest};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};

fn member(mesh: &str, kind: &str, ordinal: u32) -> MeshDigest {
    member_of(&FabricId::mint(), mesh, kind, ordinal)
}

fn member_of(fabric: &FabricId, mesh: &str, kind: &str, ordinal: u32) -> MeshDigest {
    MeshDigest {
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
        status: MemberStatus::ReadyForTraffic,
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        extra: Default::default(),
    }
}

/// One node: an endpoint serving Node RPC and gossip, its membership, and a topology slot.
struct Node {
    ep: iroh::Endpoint,
    membership: Membership,
    client: NodeRpcClient,
    resolver: Arc<StaticResolver>,
    slot: TopologySlot,
    resolved: ResolvedNode,
    _router: Router,
}

async fn node(fabric: &FabricId, mesh: &str, name: &str, extra: impl FnOnce(ServerBuilder) -> ServerBuilder) -> Node {
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let membership = Membership::join(&gossip, &ep, fabric, mesh, &MeshId::mint(), name, rafka_mesh_transport::clock::os_clock(), vec![]).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let client = NodeRpcClient::new(ep.clone(), resolver.clone());
    let slot: TopologySlot = Arc::new(OnceLock::new());
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let server = extra(rafka_node_admin_core::topology_read::serve(ServerBuilder::new(), slot.clone()))
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .expect("the catalog seals");
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip).accept(rafka_node_rpc::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id, name: name.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation };
    Node { ep, membership, client, resolver, slot, resolved, _router: router }
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
    let _ = n.slot.set(Arc::new(TopologyDoor::new(n.membership.clone(), Arc::new(move || d.clone()))));
}

fn publisher(node: &str) -> PublisherId {
    PublisherId { node: node.into(), incarnation: IncarnationId("birth-1".into()) }
}

/// `s` held by `m` as a complete snapshot its primary put into the mesh.
fn hold(m: &Membership, s: &SourceSnapshot) {
    for r in snapshot_replies(s, 1) {
        let TopologyReply::Snapshot { mesh, publisher, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed } = r else { unreachable!() };
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
            departed,
        });
        assert!(!matches!(t, Taken::Refused(_)), "{t:?}");
    }
}

fn snap(mesh: &str, p: &PublisherId, version: u64, digests: Vec<MeshDigest>) -> SourceSnapshot {
    SourceSnapshot { mesh: mesh.into(), publisher: p.clone(), topology_version: version, digests, in_flight: vec![], departed: vec![] }
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
    let err = get_topology(&caller.client, &t, &caller.membership, None, None).await.unwrap_err();
    assert!(matches!(&err, TopologyFailure::NotReady(r) if r.contains("no membership")), "{err}");
    assert!(caller.membership.held_source_version("mesh1").is_none());
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a node that holds no source version of its own mesh (its mesh primary has put none
/// into the mesh) answers NotReady naming that mesh, not an empty topology.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_holding_no_version_of_its_own_mesh_answers_not_ready_naming_the_mesh() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    open(&server, &fabric);
    let t = target_of(&caller, &server);
    let err = get_topology(&caller.client, &t, &caller.membership, None, None).await.unwrap_err();
    assert!(matches!(&err, TopologyFailure::NotReady(r) if r.contains("own mesh mesh1")), "{err}");
    server.ep.close().await;
    caller.ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: an ordinary member answers with the topology it holds: its own mesh from its book at
/// the version its primary last published, and every source mesh it holds at that source's
/// publisher and version. The caller installs each mesh, and a read of a source version the
/// caller already holds is Unchanged, a mesh the node does not hold is UnknownMesh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_member_serves_the_topology_it_holds_and_answers_unchanged_and_unknown_mesh() {
    let fabric = FabricId::mint();
    let (server, caller) = (node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    open(&server, &fabric);
    let (own_pub, remote_pub) = (publisher("mesh1.admin.1"), publisher("mesh2.admin.1"));
    hold(&server.membership, &snap("mesh1", &own_pub, 4, vec![]));
    let mesh2: Vec<MeshDigest> = (1..=3).map(|k| member_of(&fabric, "mesh2", "rpc", k)).collect();
    hold(&server.membership, &snap("mesh2", &remote_pub, 7, mesh2.clone()));
    let t = target_of(&caller, &server);

    let read = get_topology(&caller.client, &t, &caller.membership, None, None).await.unwrap();
    assert_eq!(read.meshes, 2);
    let names = |mesh: &str| -> Vec<String> {
        let mut v: Vec<String> = read.installed.iter().find(|m| m.mesh == mesh).unwrap().members.iter().map(|d| d.node.name.to_string()).collect();
        v.sort();
        v
    };
    assert_eq!(names("mesh2"), vec!["mesh2.rpc.1", "mesh2.rpc.2", "mesh2.rpc.3"]);
    assert_eq!(names("mesh1"), vec!["mesh1.rpc.1"], "its own mesh: the node itself, from its own digest");
    assert_eq!(caller.membership.held_source_version("mesh2"), Some((remote_pub.clone(), 7)), "installed at the source's publisher and version");
    assert_eq!(caller.membership.held_source_version("mesh1"), Some((own_pub.clone(), 4)));

    let held = SourceVersion { publisher: remote_pub.clone(), topology_version: 7 };
    let again = get_topology(&caller.client, &t, &caller.membership, Some("mesh2"), Some(held)).await.unwrap();
    assert!(again.installed.is_empty());
    assert_eq!(again.unchanged, vec![("mesh2".to_string(), 7)]);
    assert_eq!(again.meshes, 1, "End counts the meshes answered, Unchanged included");

    let behind = SourceVersion { publisher: remote_pub.clone(), topology_version: 6 };
    let newer = get_topology(&caller.client, &t, &caller.membership, Some("mesh2"), Some(behind)).await.unwrap();
    assert!(newer.unchanged.is_empty() && newer.installed.len() == 1, "a caller behind the node's version is sent the snapshot");

    let err = get_topology(&caller.client, &t, &caller.membership, Some("mesh9"), None).await.unwrap_err();
    assert_eq!(err, TopologyFailure::Refused("the target holds no mesh mesh9".into()));
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
    hold(&server.membership, &snap("mesh1", &publisher("mesh1.admin.1"), 1, vec![]));
    let big: Vec<MeshDigest> = (1..=300).map(|k| member_of(&fabric, "mesh2", "rpc", k)).collect();
    let source = snap("mesh2", &publisher("mesh2.admin.1"), 3, big);
    let chunks = snapshot_replies(&source, 1);
    assert!(chunks.len() > 2, "{} chunks", chunks.len());
    for c in &chunks {
        let len = Topology::encode_reply(c).unwrap().len();
        assert!(len <= Topology::MAX_REPLY_FRAME_BYTES, "a chunk of {len} bytes is over the ceiling");
        assert!(len <= rafka_mesh_transport::membership::MAX_FRAME, "a chunk of {len} bytes is over the gossip message bound");
    }
    hold(&server.membership, &source);
    let t = target_of(&caller, &server);
    let read = get_topology(&caller.client, &t, &caller.membership, Some("mesh2"), None).await.unwrap();
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
    let lossy_ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
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
                    departed: vec![],
                })
                .await
                .unwrap();
                *counted.lock().unwrap() += 1;
                Ok(TopologyReply::End { meshes: 1 })
            }
        })
        .seal(ServedBirth { node_id: lid.to_string(), incarnation: linc.0.clone() })
        .unwrap();
    let _router = Router::builder(lossy_ep.clone()).accept(rafka_node_rpc::ALPN, lossy).spawn();
    let lossy_node = ResolvedNode { node_id: lid.clone(), name: "mesh1.rpc.9".parse().unwrap(), endpoint_id: lossy_ep.id(), transport_addr: lossy_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap(), incarnation: linc };
    server.resolver.insert(lossy_node);
    let read = get_topology(&server.client, &NodeTarget::ExactNode(lid), &server.membership, None, None).await.unwrap();
    assert_eq!(*sent.lock().unwrap(), 1);
    assert!(read.installed.is_empty(), "one chunk of two installs nothing");
    assert!(server.membership.held_source_version("mesh2").is_none());
    server.ep.close().await;
    lossy_ep.close().await;
}

// @feature: node-lifecycle
/// CONTRACT: a birth whose maker has not yet put a version of its own mesh into the mesh (it
/// serves calls the moment it is Ready, before it takes its mesh's seat) is answered NotReady,
/// asks again, and installs once the maker holds one; a maker that never does is reported as
/// NotReady after the attempts, installing nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_birth_asks_again_while_its_maker_answers_not_ready_then_installs() {
    let fabric = FabricId::mint();
    let (maker, born) = (node(&fabric, "mesh1", "mesh1.admin.1", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.1", |b| b).await);
    open(&maker, &fabric);
    let t = target_of(&born, &maker);
    let late = maker.membership.clone();
    let seat = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        hold(&late, &snap("mesh1", &publisher("mesh1.admin.1"), 1, vec![]));
    });
    let read = get_topology_when_ready(&born.client, &t, &born.membership, None, None, 5).await.unwrap();
    seat.await.unwrap();
    assert_eq!(read.installed.len(), 1);
    assert_eq!(born.membership.held_source_version("mesh1"), Some((publisher("mesh1.admin.1"), 1)));

    let (silent, other) = (node(&fabric, "mesh1", "mesh1.admin.2", |b| b).await, node(&fabric, "mesh1", "mesh1.rpc.2", |b| b).await);
    open(&silent, &fabric);
    let t = target_of(&other, &silent);
    let err = get_topology_when_ready(&other.client, &t, &other.membership, None, None, 2).await.unwrap_err();
    assert!(matches!(err, TopologyFailure::NotReady(_)), "{err}");
    assert!(other.membership.held_source_version("mesh1").is_none());
    for n in [&maker, &born, &silent, &other] {
        n.ep.close().await;
    }
}
