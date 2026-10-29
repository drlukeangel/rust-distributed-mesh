//! Functional cells for `pre-node-ready` (`docs/architecture/fabric-node-hooks.md:29`).

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingReply, PingRequest};
use rdm_core::{
    EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId,
    NodeKind, PathName,
};
use rdm_node::node_hooks::{
    AcceptingPrimarySource, Addressed, Birth, End, Events, HookOutcome, NoAcceptingPrimary,
    NodeHooks, NodeReadyCtx, OnNodeReady, PreNodeReady, RetryOn, StartKind, Step,
};
use rdm_transport::topology::TopologyView;
use rdm_transport::{
    CallOptions, CancelToken, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder,
    StaticResolver, ALPN,
};
use std::sync::{Arc, Mutex};

struct TargetWaitHook {
    target_node_id: NodeId,
    attempts: Mutex<Vec<u32>>,
}

#[async_trait::async_trait]
impl PreNodeReady for TargetWaitHook {
    async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
        self.attempts.lock().unwrap().push(ctx.attempt);
        let reachable = ctx
            .topology_view
            .digest_view
            .current(ctx.topology_view.digest_view.staleness_floor())
            .iter()
            .any(|d| d.node.node_id == self.target_node_id);
        if reachable {
            HookOutcome::Ok
        } else {
            HookOutcome::Blocked {
                reason: format!("target {} not yet heard", self.target_node_id),
                retry_on: RetryOn::TargetReachable {
                    node_id: self.target_node_id.clone(),
                },
            }
        }
    }
}

// @feature: pre-node-ready
/// CONTRACT: docs/architecture/fabric-node-hooks.md:88
/// "Ok | start continues at node start step 4 and ends with node.started
///  Blocked { reason, retry_on } | readiness remains blocked; the reason is named in the blocker
///  evidence ... the hook runs again only when retry_on occurs"
#[test]
fn pre_node_ready_gates_app_rpc_with_not_ready_while_blocked_and_opens_after_retry_event_passes() {
    let dir = std::env::temp_dir().join(format!("rdm-pre-node-ready-func-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-pre-node-ready-functional")
        .expect("evidence capture starts");

    capture.run(2, async {
        let server_id = NodeId::mint();
        let server_inc = IncarnationId::mint();
        let server_name = PathName::new("mesh1", NodeKind::RpcNode, 1);
        let server_key = SecretKey::generate();
        let server_ep = rdm_transport::endpoint::bind(
            server_key.clone(),
            "127.0.0.1:0".parse().expect("addr"),
        )
        .await
        .expect("bind server ep");
        let server_addr = server_ep
            .bound_sockets()
            .into_iter()
            .find(|a| a.is_ipv4())
            .expect("ipv4");

        let target_node_id = NodeId::mint();
        let hook = Arc::new(TargetWaitHook {
            target_node_id: target_node_id.clone(),
            attempts: Mutex::new(Vec::new()),
        });
        let hooks = NodeHooks::new(hook.clone());
        let gate = hooks.gate();

        let rpc_server = ServerBuilder::new()
            .serve_gated::<Ping, _, _>(
                OpOwner::Core,
                gate.clone(),
                |_peer, req: PingRequest| async move {
                    let PingRequest::Ping { payload } = req;
                    Ok(PingReply::Pong { payload })
                },
            )
            .build(ServedBirth {
                node_id: server_id.to_string(),
                incarnation: server_inc.to_string(),
            })
            .expect("build server");
        let router = Router::builder(server_ep.clone())
            .accept(ALPN, rpc_server)
            .spawn();

        let caller_ep = rdm_transport::endpoint::bind(
            SecretKey::generate(),
            "127.0.0.1:0".parse().expect("addr"),
        )
        .await
        .expect("bind caller ep");
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode {
            node_id: server_id.clone(),
            name: server_name.clone(),
            endpoint_id: server_key.public(),
            transport_addr: server_addr,
            incarnation: server_inc.clone(),
        });
        let client = Arc::new(NodeRpcClient::new(caller_ep.clone(), resolver));

        let gossip = iroh_gossip::net::Gossip::builder().spawn(server_ep.clone());
        let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
        let _ = rafka_time.adopt(1_000);
        let fabric_id = FabricId::mint();
        let mesh_id = MeshId::mint();
        let topology_view = TopologyView::join(
            &gossip,
            &server_ep,
            &fabric_id,
            "mesh1",
            &mesh_id,
            &server_name.to_string(),
            Arc::new(rafka_time.clone()),
            Vec::new(),
            &[],
        )
        .await
        .expect("join topology view");

        let driver = hooks
            .driver(
                NodeReadyCtx {
                    start_kind: StartKind::FirstStart,
                    birth: Birth {
                        name: server_name.clone(),
                        node_id: server_id.clone(),
                        incarnation: server_inc.clone(),
                        fabric_id: fabric_id.clone(),
                        mesh_id,
                        kind: NodeKind::RpcNode,
                    },
                    accepting_primary: AcceptingPrimarySource::None(NoAcceptingPrimary::Day0Root),
                    client: client.clone(),
                    topology_view: topology_view.clone(),
                    cancel: CancelToken::new(),
                    rafka_time,
                    member_cert: None,
                    attempt: 0,
                    inherited: false,
                },
                Events::new(topology_view.digest_view.clone(), Addressed::new()),
                tracing::Span::none(),
            )
            .expect("driver");

        // 1. Attempt 1 blocks on `TargetReachable` because `target_node_id` is not in `digest_view`.
        let step1 = driver.attempt().await;
        assert!(matches!(step1, Step::Blocked(_)));
        assert!(!gate.is_open());

        // 2. While `pre_node_ready` is blocked, calling the gated op over Node RPC returns typed
        //    `NotReady` and emits `rdm.node_rpc.request.reject.via-pre-node-ready-not-passed`.
        let (blocked_out, _) = client
            .call::<Ping>(
                &NodeTarget::ExactNode(server_id.clone()),
                &PingRequest::Ping {
                    payload: b"gated".to_vec(),
                },
                &CallOptions::default(),
            )
            .await;
        match blocked_out {
            RpcOutcome::Reply(r) => match r.into_value() {
                PingReply::NotReady { reason } => {
                    assert!(
                        reason.contains("pre_node_ready hook has not returned Ok"),
                        "{reason}"
                    );
                }
                other => panic!("expected PingReply::NotReady while blocked, got {other:?}"),
            },
            other => panic!("expected Reply, got {other:?}"),
        }

        // 3. Record `target_node_id`'s digest into `digest_view` and run the driver: `Events::wait`
        //    wakes on `TargetReachable`, attempt 2 returns `Ok`, and the gate opens.
        let digest_view = topology_view.digest_view.clone();
        let driver_for_run = driver.clone();
        let run_handle = tokio::spawn(async move { driver_for_run.run(step1).await });
        assert!(digest_view.record(MeshDigest {
            held_hash: Default::default(),
            fabric_id,
            node: MeshNode {
                node_id: target_node_id,
                name: PathName::new("mesh1", NodeKind::RpcNode, 2),
                endpoint_id: EndpointId(SecretKey::generate().public().to_string()),
                transport_addr: "127.0.0.1:42000".parse().unwrap(),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                runtime: None,
            },
            status: MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 1_000,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        }));
        let end = run_handle.await.expect("join driver run");
        assert_eq!(end, End::Passed);
        assert!(gate.is_open());
        assert_eq!(*hook.attempts.lock().unwrap(), vec![1, 2]);

        // 4. Now that `pre_node_ready` has returned `Ok`, the gated op serves normally.
        let (ok_out, _) = client
            .call::<Ping>(
                &NodeTarget::ExactNode(server_id),
                &PingRequest::Ping {
                    payload: b"passed".to_vec(),
                },
                &CallOptions::default(),
            )
            .await;
        match ok_out {
            RpcOutcome::Reply(r) => {
                assert_eq!(
                    r.into_value(),
                    PingReply::Pong {
                        payload: b"passed".to_vec()
                    }
                );
            }
            other => panic!("expected Reply::Pong after gate opened, got {other:?}"),
        }

        let _ = router.shutdown().await;
        caller_ep.close().await;
        server_ep.close().await;
    });

    let spans = capture.spans().expect("captured spans");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        spans.iter().any(|s| {
            s["name"] == "rdm.node_rpc.request.reject.via-pre-node-ready-not-passed"
                && s["attributes"]["protocol"] == "ping"
        }),
        "expected rdm.node_rpc.request.reject.via-pre-node-ready-not-passed span: {spans:?}"
    );
    assert!(
        spans.iter().any(|s| {
            s["name"] == "rdm.mesh.node.reject.via-pre-node-ready-blocked"
                && s["attributes"]["cleared_by"] == "target-reachable"
        }),
        "expected via-pre-node-ready-blocked span with cleared_by=target-reachable: {spans:?}"
    );
}

struct OkPreHook;
#[async_trait::async_trait]
impl PreNodeReady for OkPreHook {
    async fn pre_node_ready(&self, _ctx: &NodeReadyCtx) -> HookOutcome {
        HookOutcome::Ok
    }
}

struct PeerCallingAfterHook {
    entered: Arc<tokio::sync::Notify>,
    proceed: Arc<tokio::sync::Notify>,
    peer_id: NodeId,
    pong_tx: Mutex<Option<tokio::sync::oneshot::Sender<Vec<u8>>>>,
}

#[async_trait::async_trait]
impl OnNodeReady for PeerCallingAfterHook {
    async fn on_node_ready(&self, ctx: &NodeReadyCtx) {
        self.entered.notify_one();
        self.proceed.notified().await;
        let (out, _) = ctx
            .client
            .call::<Ping>(
                &NodeTarget::ExactNode(self.peer_id.clone()),
                &PingRequest::Ping {
                    payload: b"from-on-node-ready".to_vec(),
                },
                &CallOptions::default(),
            )
            .await;
        if let RpcOutcome::Reply(r) = out {
            if let PingReply::Pong { payload } = r.into_value() {
                if let Some(tx) = self.pong_tx.lock().unwrap().take() {
                    let _ = tx.send(payload);
                }
            }
        }
    }
}

// @feature: on-node-ready
/// CONTRACT: docs/architecture/fabric-node-hooks.md:119
/// "After the node reports node.started, RDM calls `on_node_ready` once, with the node's
/// `NodeReadyCtx`, on a task of its own. It is for optional work that does not gate readiness."
#[test]
fn on_node_ready_runs_after_readiness_opens_without_gating_app_rpc_and_can_call_peer_over_node_rpc() {
    let dir = std::env::temp_dir().join(format!("rdm-on-node-ready-func-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-on-node-ready-functional")
        .expect("evidence capture starts");

    let node_name = PathName::new("mesh1", NodeKind::RpcNode, 1);
    let node_name_for_assert = node_name.clone();
    capture.run(2, async move {
        let node_id = NodeId::mint();
        let node_inc = IncarnationId::mint();
        let node_key = SecretKey::generate();
        let node_ep = rdm_transport::endpoint::bind(
            node_key.clone(),
            "127.0.0.1:0".parse().expect("addr"),
        )
        .await
        .expect("bind node ep");
        let node_addr = node_ep
            .bound_sockets()
            .into_iter()
            .find(|a| a.is_ipv4())
            .expect("ipv4");

        let peer_id = NodeId::mint();
        let peer_inc = IncarnationId::mint();
        let peer_name = PathName::new("mesh1", NodeKind::RpcNode, 2);
        let peer_key = SecretKey::generate();
        let peer_ep = rdm_transport::endpoint::bind(
            peer_key.clone(),
            "127.0.0.1:0".parse().expect("addr"),
        )
        .await
        .expect("bind peer ep");
        let peer_addr = peer_ep
            .bound_sockets()
            .into_iter()
            .find(|a| a.is_ipv4())
            .expect("ipv4");

        let entered = Arc::new(tokio::sync::Notify::new());
        let proceed = Arc::new(tokio::sync::Notify::new());
        let (pong_tx, pong_rx) = tokio::sync::oneshot::channel();
        let after_hook = Arc::new(PeerCallingAfterHook {
            entered: entered.clone(),
            proceed: proceed.clone(),
            peer_id: peer_id.clone(),
            pong_tx: Mutex::new(Some(pong_tx)),
        });
        let hooks = NodeHooks::new(Arc::new(OkPreHook)).with_on_node_ready(after_hook);
        let gate = hooks.gate();

        let node_server = ServerBuilder::new()
            .serve_gated::<Ping, _, _>(
                OpOwner::Core,
                gate.clone(),
                |_peer, req: PingRequest| async move {
                    let PingRequest::Ping { payload } = req;
                    Ok(PingReply::Pong { payload })
                },
            )
            .build(ServedBirth {
                node_id: node_id.to_string(),
                incarnation: node_inc.to_string(),
            })
            .expect("build node server");
        let node_router = Router::builder(node_ep.clone())
            .accept(ALPN, node_server)
            .spawn();

        let peer_server = ServerBuilder::new()
            .serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
                let PingRequest::Ping { payload } = req;
                Ok(PingReply::Pong { payload })
            })
            .build(ServedBirth {
                node_id: peer_id.to_string(),
                incarnation: peer_inc.to_string(),
            })
            .expect("build peer server");
        let peer_router = Router::builder(peer_ep.clone())
            .accept(ALPN, peer_server)
            .spawn();

        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode {
            node_id: node_id.clone(),
            name: node_name.clone(),
            endpoint_id: node_key.public(),
            transport_addr: node_addr,
            incarnation: node_inc.clone(),
        });
        resolver.insert(ResolvedNode {
            node_id: peer_id,
            name: peer_name,
            endpoint_id: peer_key.public(),
            transport_addr: peer_addr,
            incarnation: peer_inc,
        });
        let client = Arc::new(NodeRpcClient::new(node_ep.clone(), resolver.clone()));
        let external_client = Arc::new(NodeRpcClient::new(peer_ep.clone(), resolver));

        let gossip = iroh_gossip::net::Gossip::builder().spawn(node_ep.clone());
        let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
        let _ = rafka_time.adopt(1_000);
        let fabric_id = FabricId::mint();
        let mesh_id = MeshId::mint();
        let topology_view = TopologyView::join(
            &gossip,
            &node_ep,
            &fabric_id,
            "mesh1",
            &mesh_id,
            &node_name.to_string(),
            Arc::new(rafka_time.clone()),
            Vec::new(),
            &[],
        )
        .await
        .expect("join topology view");

        let driver = hooks
            .driver(
                NodeReadyCtx {
                    start_kind: StartKind::FirstStart,
                    birth: Birth {
                        name: node_name,
                        node_id: node_id.clone(),
                        incarnation: node_inc,
                        fabric_id,
                        mesh_id,
                        kind: NodeKind::RpcNode,
                    },
                    accepting_primary: AcceptingPrimarySource::None(NoAcceptingPrimary::Day0Root),
                    client,
                    topology_view: topology_view.clone(),
                    cancel: CancelToken::new(),
                    rafka_time,
                    member_cert: None,
                    attempt: 0,
                    inherited: false,
                },
                Events::new(topology_view.digest_view.clone(), Addressed::new()),
                tracing::Span::none(),
            )
            .expect("driver");

        // 1. `pre_node_ready` passes and opens the gate; `spawn_on_node_ready` launches on its own task.
        assert!(matches!(driver.attempt().await, Step::Passed));
        assert!(gate.is_open());
        driver.spawn_on_node_ready();
        entered.notified().await;

        // 2. While `on_node_ready` is still blocked waiting on `proceed`, the node is already serving
        //    gated app RPCs (`on_node_ready` does not gate readiness).
        let (served_while_after_running, _) = external_client
            .call::<Ping>(
                &NodeTarget::ExactNode(node_id),
                &PingRequest::Ping {
                    payload: b"already-ready".to_vec(),
                },
                &CallOptions::default(),
            )
            .await;
        match served_while_after_running {
            RpcOutcome::Reply(r) => {
                assert_eq!(
                    r.into_value(),
                    PingReply::Pong {
                        payload: b"already-ready".to_vec()
                    }
                );
            }
            other => panic!("expected Pong while on_node_ready is running, got {other:?}"),
        }

        // 3. Unblock `on_node_ready` and verify it calls the peer over `ctx.client`.
        proceed.notify_one();
        let peer_pong = pong_rx.await.expect("on_node_ready received peer pong");
        assert_eq!(peer_pong, b"from-on-node-ready".to_vec());

        let _ = node_router.shutdown().await;
        let _ = peer_router.shutdown().await;
        node_ep.close().await;
        peer_ep.close().await;
    });

    let spans = capture.spans().expect("captured spans");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        spans.iter().any(|s| {
            s["name"] == "rdm.mesh.node.update.via-on-node-ready"
                && s["attributes"]["node"] == node_name_for_assert.to_string()
        }),
        "expected rdm.mesh.node.update.via-on-node-ready span: {spans:?}"
    );
}
