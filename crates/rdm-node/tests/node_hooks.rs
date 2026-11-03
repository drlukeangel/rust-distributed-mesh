//! Functional cells for `pre-node-ready` (`docs/architecture/fabric-node-hooks.md:29`), `on_node_ready`
//! and `on_call_failed`.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingReply, PingRequest};
use rdm_core::{
    EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId,
    PathName,
};
use rdm_node::held_name::HeldName;
use rdm_node::node_hooks::{
    AcceptingPrimarySource, Addressed, Birth, CallFailedBirth, CallFailedCtx, CallFailedReason, End,
    Events, HookOutcome, NoAcceptingPrimary, NodeHooks, NodeReadyCtx, OnCallFailed, OnNodeReady,
    PreNodeReady, RetryOn, StartKind, Step,
};
use rdm_transport::topology::TopologyView;
use rdm_transport::{
    CallOptions, CancelToken, NodeRpcClient, NodeTarget, Resend, ResolvedNode, ServedBirth,
    ServerBuilder, StaticResolver, TimeLimit, ALPN,
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
        let server_name = PathName::numbered("mesh1", "node", 1);
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
        resolver.insert(ResolvedNode { r#type: "node".into(), mesh: "mesh1".to_owned(),
            node_id: server_id.clone(),
            path: server_name.path.clone(), name: server_name.name.clone(),
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
            server_id.as_str(),
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
                        path: server_name.path.clone(), name: server_name.name.clone(),
                        node_id: server_id.clone(),
                        incarnation: server_inc.clone(),
                        fabric_id: fabric_id.clone(),
                        mesh_id,
                        r#type: "node".into(),
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
            node: MeshNode { mesh: "mesh1".to_string(), r#type: "node".into(),
                node_id: target_node_id,
                path: format!("{}.{}", "mesh1", "node"), name: (2).to_string(),
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

    let node_name = PathName::numbered("mesh1", "node", 1);
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
        let peer_name = PathName::numbered("mesh1", "node", 2);
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
        resolver.insert(ResolvedNode { r#type: "node".into(), mesh: "mesh1".to_owned(),
            node_id: node_id.clone(),
            path: node_name.path.clone(), name: node_name.name.clone(),
            endpoint_id: node_key.public(),
            transport_addr: node_addr,
            incarnation: node_inc.clone(),
        });
        resolver.insert(ResolvedNode { r#type: "node".into(), mesh: "mesh1".to_owned(),
            node_id: peer_id,
            path: peer_name.path.clone(), name: peer_name.name.clone(),
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
            node_id.as_str(),
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
                        path: node_name.path.clone(), name: node_name.name.clone(),
                        node_id: node_id.clone(),
                        incarnation: node_inc,
                        fabric_id,
                        mesh_id,
                        r#type: "node".into(),
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
                && s["attributes"]["node_path"] == node_name_for_assert.path
                && s["attributes"]["node_name"] == node_name_for_assert.name
        }),
        "expected rdm.mesh.node.update.via-on-node-ready span: {spans:?}"
    );
}

/// The application's `on_call_failed` in the cells: every notice it is told, and a signal per notice.
struct RecordCallFailed {
    told: Mutex<Vec<CallFailedCtx>>,
    tx: tokio::sync::mpsc::UnboundedSender<()>,
}

#[async_trait::async_trait]
impl OnCallFailed for RecordCallFailed {
    async fn on_call_failed(&self, ctx: &CallFailedCtx) {
        self.told.lock().unwrap().push(ctx.clone());
        let _ = self.tx.send(());
    }
}

// @feature: on-call-failed
/// CONTRACT: docs/architecture/fabric-node-hooks.md "on_call_failed" (Q314, Q322)
/// "When a call this node opened ends with its attempts spent, RDM calls `on_call_failed` once, with
///  `CallFailedCtx { sender, receiver, reason }`, on a task of its own, after the call has ended to its
///  owner. `sender` and `receiver` are each the birth's `(node_id, incarnation, path, name)`; `reason` is
///  the call's stored end, `NotSent` with its reason (nothing reached the receiver) or `Indeterminate`
///  with its reason"
/// A call to a birth whose endpoint is gone spends its two attempts and ends `NotSent` to its owner;
/// the node's hook is then told once, naming this node's birth, the receiver's birth and the very
/// reason the owner holds, under one `rdm.mesh.node.update.via-on-call-failed` span. A call its
/// receiver answers is never told. Must NOT happen: no notice for the spent call, a second notice for
/// it, a notice for the answered call, or a notice naming another birth or reason.
#[test]
fn a_spent_call_calls_on_call_failed_once_with_both_births_and_its_reason_and_an_answered_call_never_does() {
    let dir = std::env::temp_dir().join(format!("rdm-on-call-failed-func-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-on-call-failed-functional")
        .expect("evidence capture starts");

    let (told, sender, gone) = capture.run(2, async {
        // The receiver that answers: Ping served.
        let live_id = NodeId::mint();
        let live_inc = IncarnationId::mint();
        let live_key = SecretKey::generate();
        let live_ep = rdm_transport::endpoint::bind(live_key.clone(), "127.0.0.1:0".parse().expect("addr")).await.expect("bind live ep");
        let live_addr = live_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("ipv4");
        let server = ServerBuilder::new()
            .serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
                let PingRequest::Ping { payload } = req;
                Ok(PingReply::Pong { payload })
            })
            .build(ServedBirth { node_id: live_id.to_string(), incarnation: live_inc.to_string() })
            .expect("build server");
        let router = Router::builder(live_ep.clone()).accept(ALPN, server).spawn();
        // The receiver whose endpoint is gone: bound, then closed, so nothing answers at its address.
        let gone_key = SecretKey::generate();
        let gone_ep = rdm_transport::endpoint::bind(gone_key.clone(), "127.0.0.1:0".parse().expect("addr")).await.expect("bind gone ep");
        let gone_addr = gone_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("ipv4");
        gone_ep.close().await;
        let gone = ResolvedNode { r#type: "node".into(), mesh: "mesh1".to_owned(),
            node_id: NodeId::mint(),
            path: format!("{}.{}", "mesh1", "node"), name: (2).to_string(),
            endpoint_id: gone_key.public(),
            transport_addr: gone_addr,
            incarnation: IncarnationId::mint(),
        };
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { r#type: "node".into(), mesh: "mesh1".to_owned(),
            node_id: live_id.clone(),
            path: format!("{}.{}", "mesh1", "node"), name: (1).to_string(),
            endpoint_id: live_key.public(),
            transport_addr: live_addr,
            incarnation: live_inc.clone(),
        });
        resolver.insert(gone.clone());

        // The calling node: its hook registered as every node's is, through its NodeHooks.
        let sender = CallFailedBirth { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), path: format!("{}.{}", "mesh1", "gateway"), name: (1).to_string()};
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let hook = Arc::new(RecordCallFailed { told: Mutex::new(Vec::new()), tx });
        let hooks = NodeHooks::none().with_on_call_failed(hook.clone());
        let observer = hooks.call_failed_observer(sender.node_id.clone(), sender.incarnation.clone(), HeldName::of_birth(sender.location())).expect("on_call_failed is registered");
        let caller_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().expect("addr")).await.expect("bind caller ep");
        let client = NodeRpcClient::new(caller_ep.clone(), resolver).with_call_failed_observer(observer);

        // 1. The answered call ends `Reply`: nothing is told.
        let (answered, _) = client.call::<Ping>(&NodeTarget::ExactNode(live_id.clone()), &PingRequest::Ping { payload: b"answered".to_vec() }, &CallOptions::default()).await;
        assert!(matches!(answered, RpcOutcome::Reply(_)), "the live receiver answers: {answered:?}");

        // 2. The spent call: two attempts, each bounded at 500 ms, to the gone endpoint.
        let opts = CallOptions { resend: Resend { attempts: 2, time_limit: TimeLimit(std::time::Duration::from_millis(500)) }, ..CallOptions::default() };
        let (spent, _) = client.call::<Ping>(&NodeTarget::ExactNode(gone.node_id.clone()), &PingRequest::Ping { payload: b"spent".to_vec() }, &opts).await;
        let reason = match &spent {
            RpcOutcome::NotSent(n) => CallFailedReason::NotSent(n.reason().clone()),
            other => panic!("a call to a gone endpoint ends NotSent once its attempts are spent: {other:?}"),
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv()).await.expect("on_call_failed is called on its own task").expect("told");
        // A second notice, or one for the answered call, would be on its own task by now: the two
        // calls have ended and their reports were made before each returned.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let told = hook.told.lock().unwrap().clone();
        assert_eq!(told.len(), 1, "one notice, for the spent call only: {told:?}");
        assert_eq!(told[0].reason, reason, "the reason the owner holds");
        router.shutdown().await.ok();
        caller_ep.close().await;
        (told, sender, gone)
    });

    let ctx = &told[0];
    assert_eq!(ctx.sender, sender, "this node's birth");
    assert_eq!(ctx.receiver, Some(CallFailedBirth { node_id: gone.node_id.clone(), incarnation: gone.incarnation.clone(), path: gone.path.clone(), name: gone.name.clone()}), "the birth the call was addressed to");
    let spans = capture.spans().expect("captured spans");
    let _ = std::fs::remove_dir_all(&dir);
    let notices: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.node.update.via-on-call-failed").collect();
    assert_eq!(notices.len(), 1, "one notice span: {notices:?}");
    let a = &notices[0]["attributes"];
    assert_eq!((a["node_id"].as_str(), a["receiver_node_id"].as_str(), a["outcome"].as_str()), (Some(sender.node_id.as_str()), Some(gone.node_id.as_str()), Some("NotSent")), "{a:?}");
    eprintln!("span rdm.mesh.node.update.via-on-call-failed {}", notices[0]);
}
