//! Functional test for `build-reattach` (`docs/architecture/node-rpc-envelope.md:1330`, `docs/architecture/build.md:1`).

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    BuildAccepted, BuildAttemptClaim, BuildStateAdapter, BuildStepReceipt, MemoryBuildStateAdapter,
    StepOutcome,
};
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

/// CONTRACT: docs/architecture/node-rpc-envelope.md:1388 "Every progress frame (Step, Blocked) carries the receipt key `(build_id, attempt, operation, step)` and receivers drop duplicates by that key. A caller watching a Build whose connection closes (`Budget::Stream`'s one end) reattaches by `build_id`: `build.get [id: build_id]` for the receipts, then the outer stream again, resuming from the receipts; the break is never a defect and never a new execution (Node RPC §34, §43)."
/// docs/architecture/node-rpc-envelope.md:1403 "`FabricPrimaryMoved { build_id, fabric_primary }`, the typed frame naming the new fabric-primary | deliberate outer-stream end naming the new fabric-primary; caller resubmits the same build_id there"
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
        let log = Arc::new(MemoryBuildStateAdapter::default());
        let build_id = BuildId::parse("cq2tyb03krac").unwrap();
        let topology = FabricTopology::root("fabric1", "mesh1", MeshId::parse("meshd0000001").unwrap());
        log.publish_accepted(&BuildAccepted {
            build_id: build_id.clone(),
            topology,
            submitted_change: None,
            submitted_at_ms: 1000,
        })
        .await
        .unwrap();
        log.claim_attempt(&BuildAttemptClaim {
            claimant: None,
            mesh: None,
            build_id: build_id.clone(),
            attempt: 1,
            executor: "mesh1.admin.1".into(),
        })
        .await
        .unwrap();
        for step in ["AllocateIdentity", "WaitForMeshJoin"] {
            log.append_step_receipt(&BuildStepReceipt {
                build_id: build_id.clone(),
                attempt: 1,
                operation: "node.create:mesh1.rpc.1".into(),
                step: step.into(),
                outcome: StepOutcome::Complete,
                output: None,
                executor: Some("mesh1.admin.1".into()),
                mesh: None,
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
        let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let served_me = me.clone();
        let served_log = log.clone();
        let served_id = build_id.to_string();

        let server = ServerBuilder::new()
            .serve_stream::<Build, _, _>(
                rdm_core::catalog::OpOwner::Product("rdm".into()),
                move |_peer, req, sink| {
                    let (log, me, bid) = (served_log.clone(), served_me.clone(), served_id.clone());
                    async move {
                        let gone = |e| HandlerFault::invariant_broken(format!("{e:?}"));
                        match req {
                            BuildRequest::Create {
                                submit: BuildSubmit::Change(_),
                            } => {
                                let mut sink = sink
                                    .started(BuildReply::Started {
                                        build_id: bid.clone(),
                                        attempt: 1,
                                        disposition: Disposition::Created,
                                    })
                                    .await
                                    .map_err(gone)?;
                                sink.data(BuildReply::Step {
                                    build_id: bid.clone(),
                                    attempt: 1,
                                    operation: "node.create:mesh1.rpc.1".into(),
                                    step: "AllocateIdentity".into(),
                                    receipt: None,
                                })
                                .await
                                .map_err(gone)?;
                                Ok(BuildReply::FabricPrimaryMoved {
                                    build_id: bid,
                                    fabric_primary: Some(me.to_string()),
                                })
                            }
                            BuildRequest::GetBuilds { filter } => {
                                rdm_admin::build_op::serve_builds(&me, &*log, filter, sink).await
                            }
                            BuildRequest::Create {
                                submit: BuildSubmit::Resubmit { build_id: r_id, from_attempt },
                            } => {
                                assert_eq!(r_id, bid);
                                assert_eq!(from_attempt, 1);
                                let mut sink = sink
                                    .started(BuildReply::Started {
                                        build_id: bid.clone(),
                                        attempt: 1,
                                        disposition: Disposition::Attached,
                                    })
                                    .await
                                    .map_err(gone)?;
                                // Replay WaitForMeshJoin (already read via build.get) and then emit Complete.
                                sink.data(BuildReply::Step {
                                    build_id: bid.clone(),
                                    attempt: 1,
                                    operation: "node.create:mesh1.rpc.1".into(),
                                    step: "WaitForMeshJoin".into(),
                                    receipt: None,
                                })
                                .await
                                .map_err(gone)?;
                                sink.data(BuildReply::Step {
                                    build_id: bid.clone(),
                                    attempt: 1,
                                    operation: "node.create:mesh1.rpc.1".into(),
                                    step: "Complete".into(),
                                    receipt: None,
                                })
                                .await
                                .map_err(gone)?;
                                Ok(BuildReply::Complete {
                                    build_id: bid,
                                    attempt: 1,
                                })
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
        resolver.insert(ResolvedNode {
            node_id,
            name: me.clone(),
            endpoint_id: ep.id(),
            transport_addr: addr,
            incarnation,
        });
        let client = Arc::new(NodeRpcClient::new(cep.clone(), resolver).with_caller_system("rdm"));
        let carrier = BuildCarrier::new(client, me.clone());

        let initial_stream = carrier
            .create(BuildChange::Stop {
                node: "mesh1.rpc.1".parse().unwrap(),
            })
            .await
            .expect("initial create call is accepted");

        let followed = carrier
            .follow(initial_stream)
            .await
            .expect("follow reattaches through build.get and resubmit to completion");

        assert_eq!(followed.reattached, 1);
        let op = "node.create:mesh1.rpc.1".to_string();
        assert_eq!(
            followed.frames,
            vec![
                BuildFrame::Step {
                    attempt: 1,
                    operation: op.clone(),
                    step: "AllocateIdentity".into(),
                },
                BuildFrame::FabricPrimaryMoved {
                    fabric_primary: Some(me.to_string()),
                },
                BuildFrame::Step {
                    attempt: 1,
                    operation: op.clone(),
                    step: "WaitForMeshJoin".into(),
                },
                BuildFrame::Step {
                    attempt: 1,
                    operation: op,
                    step: "Complete".into(),
                },
                BuildFrame::Complete { attempt: 1 },
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
