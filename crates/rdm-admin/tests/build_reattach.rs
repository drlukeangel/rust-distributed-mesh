//! Functional test for `build-reattach` (`docs/architecture/node-rpc-envelope.md:1330`, `docs/architecture/build.md:1`).

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome, StepWrite};
use rdm_admin::model::{MeshId, PathName};
use rdm_client::{BuildCarrier, BuildFrame};
use rdm_core::build::{Build, BuildChange, BuildReply, BuildRequest, BuildSubmit, Disposition};
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::{
    HandlerFault, NodeRpcClient, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver,
};
use std::collections::BTreeMap;
use std::net::SocketAddr;
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

/// CONTRACT: docs/architecture/node-rpc-envelope.md "Frames, durable events and reattachment": "Every progress frame (Step, Blocked) carries the key `(build_id, mesh, operation, step)` and receivers drop duplicates by that key. A caller watching a Build whose connection closes (a Build stream ends with its connection, `StreamingProtocol::ENDS_WITH_ITS_CONNECTION`) reattaches by `build_id`: `build.get [id: build_id]` for the events, then the outer stream again, resuming from the events; the break is never a defect and never a new execution"
/// docs/architecture/node-rpc-envelope.md "`FabricPrimaryMoved { build_id, fabric_primary }`, the typed frame naming the new fabric-primary | deliberate outer-stream end naming the new fabric-primary; caller resubmits the same build_id there"
#[test]
fn build_carrier_follow_reattaches_through_builds_get_and_resubmit_deduplicating_step_receipts() {
    let capture = Capture::new(
        "build_carrier_follow_reattaches_through_builds_get_and_resubmit_deduplicating_step_receipts",
    );
    let dispatch = capture.dispatch.clone();
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
        .build()
        .unwrap();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    rt.block_on(async move {
        let executor_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let log = Arc::new(rdm_admin::build_run::FramedBuilds::stepping(Arc::new(MemoryBuildStateAdapter::default()), &rdm_core::FabricId::mint(), &executor_birth.node_id, "mesh1"));
        let build_id = BuildId::parse("cq2tyb03krac").unwrap();
        let topology = FabricTopology::root("fabric1", "mesh1", MeshId::parse("meshd0000001").unwrap());
        log.publish_accepted(&BuildAccepted {
            build_id: build_id.clone(),
            topology,
            change: rdm_core::build::BuildChange::DeleteFabric,
            source: None, time: 1000,
        })
        .await
        .unwrap();
        for step in ["AllocateIdentity", "WaitForMeshJoin"] {
            log.write_step(&StepWrite {
                build_id: build_id.clone(),
                mesh: None,
                operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())),
                step: step.into(),
                outcome: StepOutcome::Complete,
            })
            .await
            .unwrap();
        }

        let ep = rdm_transport::endpoint::bind(
            SecretKey::generate(),
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        )
        .await
        .unwrap();
        let (node_id, incarnation) = (executor_birth.node_id.clone(), executor_birth.incarnation.clone());
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let served_me = me.clone();
        let served_node_id = node_id.clone();
        let served_log = log.clone();
        let served_id = build_id.to_string();

        let server = ServerBuilder::new()
            .serve_stream::<Build, _, _>(
                rdm_core::catalog::OpOwner::Product("rdm".into()),
                move |_peer, req, sink| {
                    let (log, me, bid, primary_id) = (served_log.clone(), served_me.clone(), served_id.clone(), served_node_id.clone());
                    async move {
                        let gone = |e| HandlerFault::invariant_broken(format!("{e:?}"));
                        match req {
                            BuildRequest::Create {
                                submit: BuildSubmit::Change(_),
                            } => {
                                let mut sink = sink
                                    .started(BuildReply::Started {
                                        build_id: bid.clone(),
                                        disposition: Disposition::Created,
                                    })
                                    .await
                                    .map_err(gone)?;
                                sink.data(BuildReply::Step {
                                    build_id: bid.clone(),
                                    mesh: "mesh1".into(),
                                    operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())).to_string(),
                                    step: "AllocateIdentity".into(),
                                    result: rdm_core::build::StepResult::Complete,
                                })
                                .await
                                .map_err(gone)?;
                                Ok(BuildReply::FabricPrimaryMoved {
                                    build_id: bid,
                                    fabric_primary: Some(primary_id.clone()),
                                })
                            }
                            BuildRequest::GetBuilds { filter } => {
                                rdm_admin::build_op::serve_builds(&me, &*log, filter, sink).await
                            }
                            BuildRequest::Create {
                                submit: BuildSubmit::Resubmit { build_id: r_id },
                            } => {
                                assert_eq!(r_id, bid);
                                let mut sink = sink
                                    .started(BuildReply::Started {
                                        build_id: bid.clone(),
                                        disposition: Disposition::Attached,
                                    })
                                    .await
                                    .map_err(gone)?;
                                // Replay WaitForMeshJoin (already read via build.get) and then emit Complete.
                                sink.data(BuildReply::Step {
                                    build_id: bid.clone(),
                                    mesh: "mesh1".into(),
                                    operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())).to_string(),
                                    step: "WaitForMeshJoin".into(),
                                    result: rdm_core::build::StepResult::Complete,
                                })
                                .await
                                .map_err(gone)?;
                                sink.data(BuildReply::Step {
                                    build_id: bid.clone(),
                                    mesh: "mesh1".into(),
                                    operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())).to_string(),
                                    step: "Complete".into(),
                                    result: rdm_core::build::StepResult::Complete,
                                })
                                .await
                                .map_err(gone)?;
                                Ok(BuildReply::Complete { build_id: bid })
                            }
                            other => Ok(rdm_core::reply::build::rejected(
                                "unserved".into(),
                                format!("unexpected {other:?}"),
                            )),
                        }
                    }
                },
            )
            .build(ServedBirth {
                node_id: node_id.to_string(),
                incarnation: incarnation.to_string(),
            })
            .unwrap();

        let router = Router::builder(ep.clone())
            .accept(rdm_transport::ALPN, server)
            .spawn();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();

        let cep = rdm_transport::endpoint::bind(
            SecretKey::generate(),
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        )
        .await
        .unwrap();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { r#type: "node-admin".into(), mesh: "mesh1".into(),
            node_id: node_id.clone(),
            path: me.path.clone(), name: me.name.clone(),
            endpoint_id: ep.id(),
            transport_addr: addr,
            incarnation,
        });
        let client = Arc::new(NodeRpcClient::new(cep.clone(), resolver).with_caller_system("rdm"));
        let carrier = BuildCarrier::at_node(client, node_id);

        let initial_stream = carrier
            .create(BuildChange::DeleteFabric)
            .await
            .expect("initial create call is accepted");

        let followed = carrier
            .follow(initial_stream)
            .await
            .expect("follow reattaches through build.get and resubmit to completion");

        assert_eq!(followed.reattached, 1);
        let op = rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&PathName::new("mesh1.node", "1").unwrap()));
        assert_eq!(
            followed.frames,
            vec![
                BuildFrame::Step {
                    mesh: "mesh1".into(),
                    operation: op.clone().to_string(),
                    step: "AllocateIdentity".into(),
                    result: rdm_core::build::StepResult::Complete,
                },
                BuildFrame::FabricPrimaryMoved {
                    fabric_primary: Some(executor_birth.node_id.clone()),
                },
                BuildFrame::Step {
                    mesh: "mesh1".into(),
                    operation: op.clone().to_string(),
                    step: "WaitForMeshJoin".into(),
                    result: rdm_core::build::StepResult::Complete,
                },
                BuildFrame::Step {
                    mesh: "mesh1".into(),
                    operation: op.to_string(),
                    step: "Complete".into(),
                    result: rdm_core::build::StepResult::Complete,
                },
                BuildFrame::Complete,
            ]
        );

        let _ = router.shutdown().await;
        cep.close().await;
        ep.close().await;
    });
    drop(rt);

    let calls = capture.named("rdm.node_rpc.request.update.via-call");
    assert_eq!(
        calls.len(),
        3,
        "initial create, build.get reattach read, and resubmit stream call"
    );
    let dial = capture.named("rdm.node_rpc.connection.create.via-dial");
    assert!(!dial.is_empty());
}
