//! Functional cell for `node.tickle` (`docs/architecture/lifecycles/node-tickle.md`).

use rdm_admin::model::{
    Fabric, FabricId, IncarnationId, Node, NodeId, NodeStatus, ProviderKind, ScopeStatus,
};
use rdm_admin::topology::Topology;
use rdm_admin::workflows::node_tickle::{tickle, Tickle, TickleClient};
use rdm_core::catalog::OpOwner;
use rdm_core::forward::{Forward, ForwardReply, ForwardRequest, NotSentCause};
use rdm_core::protocol::NodeProtocol;
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rdm_transport::{
    HandlerFault, NodeRpcClient, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt as _;

struct Captured {
    attributes: BTreeMap<String, String>,
}

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

impl Capture {
    fn new(cell: &str) -> Self {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (capture, layer) = rdm_transport::span_capture(cell, exporter.clone());
        Self {
            exporter,
            capture,
            dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
        }
    }

    fn named(&self, name: &str) -> Vec<Captured> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| Captured {
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }
}

fn ready_node(name: &str) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.node_id = NodeId::mint();
    n.incarnation_id = Some(IncarnationId::mint());
    n.status = NodeStatus::Ready;
    n
}

// @feature: node-tickle
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:5
/// "A primitive. `node.tickle` asks one node, by node.id and incarnation, whether anyone can reach it:
///  one probe direct, and if that fails the same probe through up to two live peers of the node's mesh,
///  sent at once (R-SW1). It answers `reached`, with what the node said, or `unreached`. It never
///  answers dead"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:26
/// "Target: One node, by `(node.id, incarnation)`; every probe names the same pair, and only that pair's answer counts"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:27
/// "Probe: `ProbeNodeState { node_id, incarnation }` on 0x1B, a bounded call with a deadline; the node
///  answers `Current { state }`, `CurrentStopped` or `Blocked { reason, retry_on }`"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:28
/// "Ladder: Direct first. On failure, the same probe carried by Forward through up to two eligible
///  live peers of the node's mesh, both at once; one failed peer probe does not cancel the other"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:29
/// "Ends: Any answer from the node, under the probed incarnation, ends the ladder and cancels the
///  probes still running"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:30
/// "Result: `reached { answer }`: the node is alive and said what it is; `unreached`: nobody reached
///  it within the ladder. Unreached is never death"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:31
/// "What follows: The caller's: on `unreached` the investigation's verdict is the inspect's answer,
///  read before the tickle; on `reached` for a node that is off its channel it commands node.mesh.join;
///  mesh investigation decides at 60 s"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:32
/// "Spans: `rdm.node.tickle.resolve.via-direct` for the direct probe, `via-peer` per carrier, each with
///  the answer or the failure; `rdm.node.tickle.returns-unknown.via-unreached` when the ladder ends with
///  no answer"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:36
/// "1. The caller holds the node's id and incarnation and a reason to ask: silence past the staleness floor"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:40
/// "2. Direct: `ProbeNodeState { node_id, incarnation }` to the node's held address, with the probe's deadline."
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:41
/// "3. On no answer, the same probe through up to two eligible live peers of the node's mesh, both
///  sent at once, each a `Forward` whose inner op is the probe; a peer that cannot carry it is
///  `InnerNotSent` and counts as no answer."
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:44
/// "4. The first answer from the node, under the probed incarnation, ends the ladder: `reached { answer }`,
///  the other probes cancelled. No answer within the ladder: `unreached`."
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:46
/// "5. The caller acts on the result per its own page. Nothing on this page writes a row, changes a
///  primary or publishes a frame."
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:51
/// "The direct probe, op `0x1B` (27): ... `ProbeNodeState { node_id, incarnation }`"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:71
/// "`{ \"Current\": { \"node_id\": \"k8pag1qx95gw\", \"incarnation\": \"0000000000000008\", \"state\": \"Ready\" } }`"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:74
/// "A stopped node answers `CurrentStopped { node_id, incarnation }`; a node held at its hook answers
///  `Blocked { node_id, incarnation, reason, retry_on }`. All three are `reached`."
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:77
/// "The same probe carried through a peer, op `0x1A` (26): the inner bytes are the probe above, the
///  peer makes one direct inner call and chooses no other target"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:100
/// "The carrier's reply is `Forwarded { inner }` with the node's answer inside, or `InnerNotSent { reason }`
///  when the carrier could not reach it. The tickle has no gossip of its own: the caller publishes
///  nothing for a probe, an answer or a failure. The probed node re-publishes its presence on the
///  channel it is on, the check-in it owes its primary, and answers its current state; while its
///  `pre_node_ready` hook is Blocked it answers `Blocked` and re-publishes nothing."
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:138
/// "The primitive has no failure of its own: `unreached` is a result, not an error, and it is never
///  recorded as the node's death."
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:146
/// "Direct probe | caller | `rdm.node.tickle.resolve.via-direct` | node, node_id, incarnation, answer or failure, deadline"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:147
/// "Peer probe | caller, per carrier | `rdm.node.tickle.resolve.via-peer` | carrier, answer or failure"
/// CONTRACT: docs/architecture/lifecycles/node-tickle.md:148
/// "Ladder result | caller | the direct or peer span that carried the answer; `rdm.node.tickle.returns-unknown.via-unreached` when none did | what the caller does next"
#[test]
fn node_tickle_probes_direct_then_up_to_two_mesh_peers_via_forward_and_records_unreached_when_none_answer(
) {
    let capture = Capture::new("node-tickle-functional");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let admin = ready_node("mesh1.admin.1");
        let mut target = ready_node("mesh1.rpc.1");
        target.status = NodeStatus::PendingReconnect;
        let target_id = target.node_id.clone();
        let target_inc = target.incarnation_id.clone().unwrap();

        let peer1 = ready_node("mesh1.rpc.2");
        let peer2 = ready_node("mesh1.rpc.3");
        let peer3 = ready_node("mesh1.rpc.4");

        let view = Topology {
            fabric: Fabric {
                id: FabricId::mint(),
                name: "fab1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: vec![
                admin.clone(),
                target.clone(),
                peer1.clone(),
                peer2.clone(),
                peer3.clone(),
            ],
        };

        // Peer 1 always answers InnerNotSent.
        let p1_key = iroh::SecretKey::generate();
        let p1_ep = rdm_transport::endpoint::bind(p1_key.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let p1_addr = p1_ep.bound_sockets()[0];
        let p1_srv = ServerBuilder::new()
            .serve::<Forward, _, _>(OpOwner::Core, move |_peer, _req: ForwardRequest| async {
                Ok::<_, HandlerFault>(ForwardReply::InnerNotSent {
                    reason: "connect-refused".into(),
                    cause: NotSentCause::Connection,
                })
            })
            .build(ServedBirth {
                node_id: peer1.node_id.to_string(),
                incarnation: peer1.incarnation_id.as_ref().unwrap().as_str().to_string(),
            })
            .unwrap();
        let r1 = iroh::protocol::Router::builder(p1_ep.clone())
            .accept(rdm_transport::ALPN, p1_srv)
            .spawn();

        // Peer 2 answers Forwarded(CurrentStopped) when `peer2_reaches` is true, and InnerNotSent when false.
        let peer2_reaches = Arc::new(AtomicBool::new(true));
        let peer2_reaches_srv = peer2_reaches.clone();
        let expected_target = target_id.clone();
        let expected_inc = target_inc.clone();
        let p2_key = iroh::SecretKey::generate();
        let p2_ep = rdm_transport::endpoint::bind(p2_key.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let p2_addr = p2_ep.bound_sockets()[0];
        let p2_srv = ServerBuilder::new()
            .serve::<Forward, _, _>(OpOwner::Core, move |_peer, req: ForwardRequest| {
                let reaches = peer2_reaches_srv.load(Ordering::SeqCst);
                let expected_target = expected_target.clone();
                let expected_inc = expected_inc.clone();
                async move {
                    let ForwardRequest::Forward {
                        target,
                        inner_op,
                        inner,
                        ..
                    } = req;
                    assert_eq!(target, expected_target);
                    assert_eq!(inner_op, Status::OP);
                    let decoded = Status::decode_request(&inner).unwrap();
                    assert_eq!(
                        decoded,
                        StatusRequest::ProbeNodeState {
                            node_id: expected_target.clone(),
                            incarnation: expected_inc.clone(),
                        }
                    );
                    if reaches {
                        let reply = StatusReply::CurrentStopped {
                            node_id: expected_target,
                            incarnation: expected_inc,
                        };
                        Ok::<_, HandlerFault>(ForwardReply::Forwarded {
                            inner: Status::encode_reply(&reply).unwrap(),
                        })
                    } else {
                        Ok(ForwardReply::InnerNotSent {
                            reason: "connect-refused".into(),
                            cause: NotSentCause::Connection,
                        })
                    }
                }
            })
            .build(ServedBirth {
                node_id: peer2.node_id.to_string(),
                incarnation: peer2.incarnation_id.as_ref().unwrap().as_str().to_string(),
            })
            .unwrap();
        let r2 = iroh::protocol::Router::builder(p2_ep.clone())
            .accept(rdm_transport::ALPN, p2_srv)
            .spawn();

        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode {
            node_id: peer1.node_id.clone(),
            name: peer1.name.clone(),
            endpoint_id: p1_key.public(),
            transport_addr: p1_addr,
            incarnation: peer1.incarnation_id.clone().unwrap(),
        });
        resolver.insert(ResolvedNode {
            node_id: peer2.node_id.clone(),
            name: peer2.name.clone(),
            endpoint_id: p2_key.public(),
            transport_addr: p2_addr,
            incarnation: peer2.incarnation_id.clone().unwrap(),
        });

        let admin_ep = rdm_transport::endpoint::bind(
            iroh::SecretKey::generate(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let client = Arc::new(NodeRpcClient::new(admin_ep.clone(), resolver));
        let tickle_client = TickleClient {
            client,
            me_id: admin.node_id.clone(),
            me: admin.name.to_string(),
            connections: None,
            commands: None,
        };

        // 1. Peer 1 fails InnerNotSent while Peer 2 answers CurrentStopped -> Reached via mesh1.rpc.3.
        let res1 = tickle(&target, &view, &tickle_client)
            .await
            .expect("tickle 1");
        match res1 {
            Tickle::Reached { answer, carrier } => {
                assert_eq!(carrier, Some("mesh1.rpc.3".into()));
                assert_eq!(
                    answer,
                    StatusReply::CurrentStopped {
                        node_id: target_id.clone(),
                        incarnation: target_inc.clone(),
                    }
                );
            }
            other => panic!("expected Reached via mesh1.rpc.3, got {other:?}"),
        }

        // 2. Neither direct nor either of the 2 peers answers -> Unreached naming the 2 carriers asked.
        peer2_reaches.store(false, Ordering::SeqCst);
        let res2 = tickle(&target, &view, &tickle_client)
            .await
            .expect("tickle 2");
        match res2 {
            Tickle::Unreached(unreached) => {
                assert_eq!(unreached.carriers(), "mesh1.rpc.2,mesh1.rpc.3");
                unreached.next("inspect");
            }
            other => panic!("expected Unreached, got {other:?}"),
        }

        let _ = r1.shutdown().await;
        let _ = r2.shutdown().await;
        p1_ep.close().await;
        p2_ep.close().await;
        admin_ep.close().await;
    });

    drop(rt);

    let direct_spans = capture.named("rdm.node.tickle.resolve.via-direct");
    assert_eq!(direct_spans.len(), 2);

    let peer_spans = capture.named("rdm.node.tickle.resolve.via-peer");
    assert!(
        peer_spans
            .iter()
            .any(|s| s.attributes.get("carrier").map(String::as_str) == Some("mesh1.rpc.3")
                && s.attributes.get("answer").map(String::as_str) == Some("current-stopped")),
        "peer span must record carrier=mesh1.rpc.3 and answer=current-stopped"
    );

    let unreached_spans = capture.named("rdm.node.tickle.returns-unknown.via-unreached");
    assert_eq!(unreached_spans.len(), 1);
    assert_eq!(
        unreached_spans[0]
            .attributes
            .get("carriers")
            .map(String::as_str),
        Some("mesh1.rpc.2,mesh1.rpc.3")
    );
    assert_eq!(
        unreached_spans[0]
            .attributes
            .get("next")
            .map(String::as_str),
        Some("inspect")
    );
}
