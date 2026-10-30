//! The reads on op `0x23` (Q285; node-rpc-envelope.md "Read, op `0x23`"), over real Node RPC: a
//! server streams rows packed by the one packer the node-admin and every node serve with (`rdm_client::read_frames`),
//! and the caller (`NodeRpc::read` and `NodeRpc::connections_get`, what `node.get`, every plural read and
//! `node.connection.get` call) collects them, counting the read only on a valid `End`.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::filter::{Entity, Filter};
use rdm_core::read::{Read, ReadReply, ReadRequest, Rows};
use rdm_core::views::{ConnectionView, ConnectionsView, NodeView};
use rdm_core::{ConnectionId, IncarnationId, NodeId};
use rdm_transport::stream::{NotStarted, ReplySink};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use std::net::SocketAddr;
use std::sync::Arc;

/// A node row of `mesh` at `ordinal`, its config carrying `note` bytes.
fn node(mesh: &str, ordinal: usize, note: usize) -> NodeView {
    let name = format!("{mesh}.rpc.{ordinal}");
    let mut n: NodeView = serde_json::from_value(serde_json::json!({
        "name": name, "kind": "rpc_node", "mesh": mesh, "node_id": NodeId::mint(), "endpoint_id": null,
        "incarnation_id": null, "deployment_id": null, "provider": "process", "data_dir": null, "status": "ready",
        "is_primary": false, "is_fabric_primary": false, "admin_api_base": null, "transport_addr": null, "listeners": [],
        "declared": null, "stopped": false, "load": null, "gossip": null, "tags": [], "config": {}, "data_dir_lost": null,
        "data_dir_unreadable": null, "types": [], "held": [], "held_hash": vec![0u8; 32], "cert": null, "executable": null,
        "storage_persistence": null, "topology_storage_provider": null,
    }))
    .expect("a node row");
    n.config.insert("note".into(), "n".repeat(note));
    n
}

/// `meshes` meshes of `per` rows each, in the read's stable order (by path.name).
fn rows(meshes: usize, per: usize, note: usize) -> Vec<NodeView> {
    let mut out: Vec<NodeView> = (1..=meshes).flat_map(|m| (1..=per).map(move |o| node(&format!("mesh{m}"), o, note))).collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// How the served stream ends.
#[derive(Clone, Copy)]
enum Ending {
    /// Every frame, then `End` counting them.
    Whole,
    /// The first frame only, then a fault: the stream is cut before its `End`.
    Cut,
}

/// A served node and a caller's client that resolves it.
struct Served {
    client: Arc<NodeRpcClient>,
    target: NodeTarget,
    _router: Router,
    _caller: iroh::Endpoint,
}

/// Serve `held` as `node.get` answers it: the rows matching the read's filter, packed and streamed.
async fn serve(held: Vec<NodeView>, ending: Ending) -> Served {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let server = ServerBuilder::new()
        .serve_stream::<Read, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req: ReadRequest, sink: ReplySink<Read, NotStarted>| {
            let held = held.clone();
            async move {
                let ReadRequest::Get { entity, filter } = req else {
                    return Ok(ReadReply::RejectedFilter { reason: "Get only".into() });
                };
                if let Some(reason) = filter.refusal(entity) {
                    return Ok(ReadReply::RejectedFilter { reason });
                }
                let matched: Vec<NodeView> = held.into_iter().filter(|n| filter.matches(n)).collect();
                let count = matched.len() as u32;
                let frames = match rdm_client::read_frames(Rows::Nodes(matched).into_rows(), entity.read()) {
                    Ok(f) => f,
                    Err(over) => return Ok(over),
                };
                let n = frames.len() as u32;
                let Ok(mut sink) = sink.started(ReadReply::Started { read: entity.read().into() }).await else {
                    return Ok(ReadReply::NotReady { reason: "the caller left".into() });
                };
                for (i, f) in frames.into_iter().enumerate() {
                    if matches!(ending, Ending::Cut) && i == 1 {
                        return Err(rdm_transport::HandlerFault::invariant_broken("the read is cut after its first frame"));
                    }
                    if sink.data(f).await.is_err() {
                        return Ok(ReadReply::NotReady { reason: "the caller left".into() });
                    }
                }
                Ok(ReadReply::End { rows: count, frames: n })
            }
        })
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .expect("the catalog is built");
    let router = Router::builder(ep.clone()).accept(rdm_transport::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let caller = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: node_id.clone(), name: "mesh1.admin.1".parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation });
    let client = Arc::new(NodeRpcClient::new(caller.clone(), resolver));
    Served { client, target: NodeTarget::ExactNode(node_id), _router: router, _caller: caller }
}

async fn read(s: &Served, filter: Filter) -> Result<Vec<NodeView>, rdm_client::CallEnd> {
    match rdm_client::NodeRpc::new(&s.client).read(&s.target, Entity::Nodes, &filter, &CallOptions::default()).await? {
        Rows::Nodes(n) => Ok(n),
        other => panic!("node.get answered another entity's rows: {other:?}"),
    }
}

// @feature: node-lifecycle
/// CONTRACT (Q285 proof: "one unfiltered request answers all 200 L4 nodes, and again when they are one
/// mesh; a list several times larger; a filter and the order across frames"): one read answers every
/// row, in the read's order, across as many frames as the rows take: L4's shape (10 meshes of 20, about
/// 465 bytes a row, past one 64 KiB frame), the same 200 rows in one mesh, a list five times that, and
/// a filter whose rows straddle frames.
#[tokio::test]
async fn one_read_answers_every_row_across_frames_in_order() {
    for (meshes, per) in [(10, 20), (1, 200), (10, 100)] {
        let held = rows(meshes, per, 300);
        let s = serve(held.clone(), Ending::Whole).await;
        let got = read(&s, Filter::new()).await.unwrap_or_else(|e| panic!("{meshes}x{per}: the whole read: {e}"));
        assert_eq!(got.len(), meshes * per, "{meshes}x{per}: every row");
        assert_eq!(got, held, "{meshes}x{per}: the rows in the read's order");
        let whole: usize = held.iter().map(|n| serde_json::to_vec(n).unwrap().len()).sum();
        assert!(whole > 64 * 1024, "{meshes}x{per}: the answer is past one frame ({whole} bytes of JSON)");
    }
    let held = rows(10, 100, 300);
    let s = serve(held.clone(), Ending::Whole).await;
    let mesh7 = read(&s, Filter::new().eq("mesh", ["mesh7"])).await.expect("a filtered read");
    assert_eq!(mesh7, held.iter().filter(|n| n.mesh == "mesh7").cloned().collect::<Vec<_>>(), "the filter's rows, in order");
}

// @feature: node-lifecycle
/// CONTRACT (Q285: "a cut or a deadline is a failed read ... a row that fits no frame `ReadOverBound`
/// naming the row ... a row the packer rejects is never omitted"; "an unknown pair `INVALID_ARGUMENT`
/// naming it"): a stream cut after its first frame is a failed read, never the rows that arrived; a
/// row larger than a frame is refused naming it, and nothing of the read is returned; an unknown pair is
/// refused naming it.
#[tokio::test]
async fn a_cut_stream_a_row_past_the_frame_and_an_unknown_pair_are_failed_reads() {
    let cut = serve(rows(10, 20, 300), Ending::Cut).await;
    match read(&cut, Filter::new()).await {
        Err(rdm_client::CallEnd::Indeterminate { .. }) => {}
        other => panic!("a stream cut before its End is a failed read, not {other:?}"),
    }
    let mut held = rows(1, 3, 10);
    held[1].config.insert("note".into(), "n".repeat(70 * 1024));
    let over = serve(held, Ending::Whole).await;
    match read(&over, Filter::new()).await {
        Err(rdm_client::CallEnd::Rejected { reason, detail }) => assert!(reason == "read-over-bound" && detail.contains("mesh1.rpc.2"), "named: {reason}: {detail}"),
        other => panic!("a row past the frame is refused naming it, not {other:?}"),
    }
    let s = serve(rows(1, 3, 10), Ending::Whole).await;
    match read(&s, Filter::new().eq("zone", ["eu-1"])).await {
        Err(rdm_client::CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains("(zone, [eu-1])"), "named: {reason}: {detail}"),
        other => panic!("an unknown pair is refused naming it, not {other:?}"),
    }
}

/// `total` connection rows of `mesh1.rpc.1`, alternating `direct` and `proxy`, each carrying `reason`
/// bytes so the whole list spans several 64 KiB frames.
fn connection_rows(total: usize, reason: usize) -> Vec<ConnectionView> {
    (1..=total)
        .map(|i| ConnectionView {
            id: ConnectionId::mint().to_string(),
            source: "mesh1.rpc.1".into(),
            destination: format!("mesh1.rpc.{}", (i % 5) + 2),
            kind: if i % 2 == 0 { "proxy".into() } else { "direct".into() },
            state: "connected".into(),
            carrier: (i % 2 == 0).then(|| "mesh1.admin.1".into()),
            reason: (reason > 0).then(|| "r".repeat(reason)),
            logged_at_ms: i as u64,
        })
        .collect()
}

/// Serve `held` as `node.connection.get { node }` answers it on op `0x23`: the exact birth's own
/// connections matching `filter`, packed by `rdm_client::read_frames` and streamed.
async fn serve_connections(node_name: &str, complete: bool, held: Vec<ConnectionView>, ending: Ending) -> (Served, rdm_client::ExactBirth) {
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
    let held_birth = incarnation.clone();
    let held_name = node_name.to_string();
    let server = ServerBuilder::new()
        .serve_stream::<Read, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_peer, req: ReadRequest, sink: ReplySink<Read, NotStarted>| {
            let held = held.clone();
            let held_birth = held_birth.clone();
            let held_name = held_name.clone();
            async move {
                let op = req.op();
                let ReadRequest::GetConnections { incarnation: asked, filter, .. } = req else {
                    return Ok(ReadReply::RejectedFilter { reason: format!("{op} is GetConnections (with its node and incarnation), never Get") });
                };
                if asked != held_birth {
                    let stale = rdm_core::reply::stale(held_birth);
                    return Ok(ReadReply::NotReady { reason: format!("{held_name} answered {op} with {}", stale.name()) });
                }
                if let Some(reason) = filter.refusal(Entity::Connections) {
                    return Ok(ReadReply::RejectedFilter { reason });
                }
                let matched: Vec<ConnectionView> = held.into_iter().filter(|c| filter.matches(c)).collect();
                let count = matched.len() as u32;
                let view = ConnectionsView { node: held_name, complete, connections: matched };
                let frames = match rdm_client::read_frames(Rows::Connections(view).into_rows(), op) {
                    Ok(f) => f,
                    Err(over) => return Ok(over),
                };
                let n = frames.len() as u32;
                let Ok(mut sink) = sink.started(ReadReply::Started { read: op.into() }).await else {
                    return Ok(ReadReply::NotReady { reason: "the caller left".into() });
                };
                for (i, f) in frames.into_iter().enumerate() {
                    if matches!(ending, Ending::Cut) && i == 1 {
                        return Err(rdm_transport::HandlerFault::invariant_broken("the connections read is cut after its first frame"));
                    }
                    if sink.data(f).await.is_err() {
                        return Ok(ReadReply::NotReady { reason: "the caller left".into() });
                    }
                }
                Ok(ReadReply::End { rows: count, frames: n })
            }
        })
        .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
        .expect("the catalog is built");
    let router = Router::builder(ep.clone()).accept(rdm_transport::ALPN, server).spawn();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let caller = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: node_id.clone(), name: node_name.parse().unwrap(), endpoint_id: ep.id(), transport_addr: addr, incarnation: incarnation.clone() });
    let client = Arc::new(NodeRpcClient::new(caller.clone(), resolver));
    let target = NodeTarget::ExactNode(node_id.clone());
    let birth = rdm_client::ExactBirth { target: target.clone(), node_id, incarnation };
    (Served { client, target, _router: router, _caller: caller }, birth)
}

// @feature: node-lifecycle
/// CONTRACT (Q285 item 7: `node.connection.get` streams on op `0x23` in `Connections` frames): a node
/// holding more connection rows than one 64 KiB frame answers them all in one read, in order, with
/// `node` and `complete` as held; a filter matches across frames in order; a wrong birth is refused;
/// and a cut stream is a failed read.
#[tokio::test]
async fn connections_get_streams_every_row_across_frames_in_order_and_refuses_wrong_birth_or_cut() {
    let held = connection_rows(300, 350);
    let whole: usize = held.iter().map(|c| serde_json::to_vec(c).unwrap().len()).sum();
    assert!(whole > 64 * 1024, "the held connections exceed one 64 KiB frame ({whole} bytes of JSON)");

    let (s, birth) = serve_connections("mesh1.rpc.1", false, held.clone(), Ending::Whole).await;
    let got = rdm_client::NodeRpc::new(&s.client).connections_get(&birth, &Filter::new(), &CallOptions::default()).await.expect("the whole connections read");
    assert_eq!((got.node.as_str(), got.complete, got.connections.len()), ("mesh1.rpc.1", false, held.len()));
    assert_eq!(got.connections, held, "every connection row in order across frames");

    let proxies = rdm_client::NodeRpc::new(&s.client).connections_get(&birth, &Filter::new().eq("kind", ["proxy"]).eq("destination", ["mesh1.rpc.3"]), &CallOptions::default()).await.expect("a filtered connections read");
    let expected: Vec<ConnectionView> = held.iter().filter(|c| c.kind == "proxy" && c.destination == "mesh1.rpc.3").cloned().collect();
    assert!(!expected.is_empty());
    assert_eq!((proxies.node.as_str(), proxies.complete, proxies.connections), ("mesh1.rpc.1", false, expected), "the filter's rows across frames, in order");

    let empty = rdm_client::NodeRpc::new(&s.client).connections_get(&birth, &Filter::new().eq("destination", ["mesh9.rpc.9"]), &CallOptions::default()).await.expect("an empty filtered connections read");
    assert_eq!((empty.node.as_str(), empty.complete, empty.connections.is_empty()), ("mesh1.rpc.1", false, true), "zero matching rows still carry the answering node and complete");

    let wrong_birth = rdm_client::ExactBirth { incarnation: IncarnationId::mint(), ..birth.clone() };
    match rdm_client::NodeRpc::new(&s.client).connections_get(&wrong_birth, &Filter::new(), &CallOptions::default()).await {
        Err(rdm_client::CallEnd::Rejected { reason, detail }) => assert!(reason == "not-ready" && detail.contains("rejected-stale-incarnation"), "a wrong birth is refused: {reason}: {detail}"),
        other => panic!("a wrong birth must be refused, not {other:?}"),
    }

    let (cut, cut_birth) = serve_connections("mesh1.rpc.1", true, held, Ending::Cut).await;
    match rdm_client::NodeRpc::new(&cut.client).connections_get(&cut_birth, &Filter::new(), &CallOptions::default()).await {
        Err(rdm_client::CallEnd::Indeterminate { .. }) => {}
        other => panic!("a connections stream cut before its End is a failed read, not {other:?}"),
    }
}
