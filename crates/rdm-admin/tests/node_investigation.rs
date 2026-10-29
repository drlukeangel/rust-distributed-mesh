//! Functional cell for `node-investigation` (`docs/architecture/lifecycles/node-investigation.md`).

use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::DeploymentProvider;
use rdm_admin::model::{
    Fabric, FabricId, IncarnationId, Node, NodeId, NodeStatus, ProviderKind, ScopeStatus,
};
use rdm_admin::node_investigation::{
    inspect_birth_with_handle, investigate, span_inspection, Finding,
};
use rdm_admin::offline::TickleClient;
use rdm_admin::storage::{
    ConnectionsStorage, MemoryConnectionsStorage, MemoryNodesStorage, NodesStorage, RuntimeRow,
};
use rdm_admin::topology::Topology;
use rdm_core::catalog::OpOwner;
use rdm_core::connections::{ConnectionEnd, ConnectionKind, ConnectionState, NodeConnection};
use rdm_core::forward::{Forward, ForwardReply, ForwardRequest, NotSentCause};
use rdm_core::protocol::NodeProtocol;
use rdm_core::runtime::ExitRecord;
use rdm_core::status::{NodeState, Status, StatusReply, StatusRequest};
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

// @feature: node-investigation
/// CONTRACT: docs/architecture/lifecycles/node-investigation.md:5
/// "Node investigation is the one function every caller uses on one exact birth, ending in
///  one verdict: `reached`, `running`, `os-stopped`, `exited` or `unproven`. Its steps, in order:
///  1. Read the by-destination connections index (connections.md §3.1). It names the peers to try;
///     it never proves the node is reachable.
///  2. node.runtime.inspect. `exited` or `os-stopped` is the verdict; no tickle is sent.
///  3. node.tickle, direct.
///  4. The same tickle via those peers. Unreached, the verdict is the inspect's answer: `running`, or
///     `unproven` for `unavailable`.
///  Only an answer from the exact birth counts as `reached`." (Q273)
#[test]
fn node_investigation_inspects_first_and_tickles_direct_then_through_its_leads_unless_the_runtime_is_proven_gone(
) {
    let capture = Capture::new("node-investigation-functional");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let dir = std::env::temp_dir().join(format!(
        "rdm-node-investigation-{}",
        rdm_core::IncarnationId::mint()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    rt.block_on(async {
        let admin = ready_node("mesh1.admin.1");
        let mut target = ready_node("mesh1.rpc.1");
        target.status = NodeStatus::PendingReconnect;
        let target_id = target.node_id.clone();
        let target_inc = target.incarnation_id.clone().unwrap();

        let lead_carrier = ready_node("mesh2.rpc.1");
        let lead_id = lead_carrier.node_id.clone();
        let lead_inc = lead_carrier.incarnation_id.clone().unwrap();

        let view = Topology {
            fabric: Fabric {
                id: FabricId::mint(),
                name: "fab1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), target.clone(), lead_carrier.clone()],
        };

        // 1. Seed the by-destination connections index with an active Direct row from mesh2.rpc.1 to mesh1.rpc.1.
        let connections = Arc::new(MemoryConnectionsStorage::default());
        connections
            .put_connection(&NodeConnection {
                id: rdm_core::ConnectionId::mint(),
                source: ConnectionEnd {
                    name: lead_carrier.name.clone(),
                    node_id: lead_id.clone(),
                    incarnation: Some(lead_inc.clone()),
                },
                destination: ConnectionEnd {
                    name: target.name.clone(),
                    node_id: target_id.clone(),
                    incarnation: Some(target_inc.clone()),
                },
                kind: ConnectionKind::Direct,
                alpn: rdm_core::connections::ALPN.to_vec(),
                state: ConnectionState::Connected,
                carrier: None,
                recovery: None,
                reason: None,
                logged_at_ms: 1,
            })
            .await
            .unwrap();

        // 2. Bind the lead carrier's NodeRpcServer serving Forward.
        let carrier_reaches = Arc::new(AtomicBool::new(true));
        let carrier_reaches_srv = carrier_reaches.clone();
        let expected_target = target_id.clone();
        let expected_inc = target_inc.clone();

        let carrier_key = iroh::SecretKey::generate();
        let carrier_ep =
            rdm_transport::endpoint::bind(carrier_key.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
        let carrier_addr = carrier_ep.bound_sockets()[0];
        let server = ServerBuilder::new()
            .serve::<Forward, _, _>(OpOwner::Core, move |_peer, req: ForwardRequest| {
                let reaches = carrier_reaches_srv.load(Ordering::SeqCst);
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
                        let reply = StatusReply::Current {
                            node_id: expected_target,
                            incarnation: expected_inc,
                            state: NodeState::Ready,
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
                node_id: lead_id.to_string(),
                incarnation: lead_inc.as_str().to_string(),
            })
            .unwrap();
        let router = iroh::protocol::Router::builder(carrier_ep.clone())
            .accept(rdm_transport::ALPN, server)
            .spawn();

        // Admin resolver holds `mesh2.rpc.1` (lead carrier), NOT `mesh1.rpc.1`, so step 2 direct fails NotSent immediately.
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode {
            node_id: lead_id.clone(),
            name: lead_carrier.name.clone(),
            endpoint_id: carrier_key.public(),
            transport_addr: carrier_addr,
            incarnation: lead_inc.clone(),
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
            connections: Some(connections),
            commands: None,
        };

        // Case A: the inspect answers running, then the lead carrier reaches the exact birth -> `Reached`.
        let provider_asked = Arc::new(AtomicBool::new(false));
        let provider_asked_a = provider_asked.clone();
        let found_a = investigate(&target, &view, &tickle_client, async move {
            provider_asked_a.store(true, Ordering::SeqCst);
            Finding::Running
        })
        .await;
        assert_eq!(
            found_a,
            Finding::Reached {
                answer: StatusReply::Current {
                    node_id: target_id.clone(),
                    incarnation: target_inc.clone(),
                    state: NodeState::Ready,
                },
                carrier: Some("mesh2.rpc.1".into()),
            }
        );
        assert!(
            provider_asked.load(Ordering::SeqCst),
            "node.runtime.inspect runs first, right after the index read"
        );

        // Case B: the inspect proves the runtime exited -> its verdict, and no tickle is sent.
        carrier_reaches.store(false, Ordering::SeqCst);

        let provider = ProcessDeploymentProvider::new();
        let dep_id = rdm_core::fixture_crockford60("dep-investigate-1");
        ExitRecord {
            deployment_id: dep_id.clone(),
            incarnation: target_inc.as_str().to_string(),
            code: 4,
            reason: "gossip refused".into(),
        }
        .write(&dir)
        .unwrap();

        let nodes_store = MemoryNodesStorage::default();
        nodes_store
            .put_runtime(&RuntimeRow {
                node_id: target_id.clone(),
                name: target.name.clone(),
                incarnation_id: target_inc.clone(),
                runtime: rdm_core::RuntimeFact {
                    deployment_id: dep_id,
                    provider: rdm_core::RuntimeProvider::Process,
                    control_domain: provider.control_domain(),
                    locator: rdm_core::RuntimeLocator::Process {
                        pid: 999_999,
                        start: 1,
                    },
                },
                data_dir: Some(dir.display().to_string()),
            })
            .await
            .unwrap();

        let digest_view = rdm_transport::topology::DigestView::default();
        let target_b = target.clone();
        let found_b = investigate(&target, &view, &tickle_client, async move {
            let (outcome, handle) =
                inspect_birth_with_handle(&provider, &digest_view, Some(&nodes_store), &target_b)
                    .await;
            span_inspection(
                "mesh1.admin.1",
                &target_b,
                &outcome,
                handle.as_ref(),
                "node-investigation",
            );
            outcome.into()
        })
        .await;
        assert_eq!(found_b, Finding::Exited { code: Some(4) });

        let _ = router.shutdown().await;
        carrier_ep.close().await;
        admin_ep.close().await;
    });

    drop(rt);
    let _ = std::fs::remove_dir_all(&dir);

    let direct_spans = capture.named("rdm.node.tickle.resolve.via-direct");
    assert_eq!(direct_spans.len(), 1, "case B sends no tickle: the inspect proved the runtime exited");
    assert_eq!(
        direct_spans[0].attributes.get("leads").map(String::as_str),
        Some("mesh2.rpc.1")
    );

    let peer_spans = capture.named("rdm.node.tickle.resolve.via-peer");
    assert_eq!(peer_spans.len(), 1);
    assert_eq!(
        peer_spans[0].attributes.get("carrier").map(String::as_str),
        Some("mesh2.rpc.1")
    );
    assert_eq!(
        peer_spans[0].attributes.get("lead").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        peer_spans[0].attributes.get("answer").map(String::as_str),
        Some("current")
    );

    let unreached_spans = capture.named("rdm.node.tickle.returns-unknown.via-unreached");
    assert!(unreached_spans.is_empty(), "A was reached and B sent no tickle: {} unreached spans", unreached_spans.len());

    let inspect_spans = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
    assert_eq!(inspect_spans.len(), 1);
    assert_eq!(
        inspect_spans[0]
            .attributes
            .get("verdict")
            .map(String::as_str),
        Some("exited")
    );
    assert_eq!(
        inspect_spans[0]
            .attributes
            .get("exit_code")
            .map(String::as_str),
        Some("4")
    );
}
