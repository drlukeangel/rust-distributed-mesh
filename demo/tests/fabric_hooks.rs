//! The fabric hooks a consumer registers, on public RDM surfaces only (fabric-node-hooks.md;
//! fabric-state-sync.md). Its `sync_state` drives work on another node and answers `StateSynced` only
//! once that node answered: Rafka's day 0, where node-admin's state-sync hook asks the gateway to
//! perform the system-org creations and waits for their answers. Its `traffic_opened` notice, after the
//! fabric is ready-for-traffic, asks that node to sync again: Rafka's broker re-sync starts there.
//!
//! The work is the testkit's op `0x74` (`BeforeReadyPull`), which stands in for a product's own op: the
//! gateway counts each call it serves and answers with the count.
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, StateSyncHandoff, SyncState, TrafficOpenedNotice, Wiring};
use rdm_core::catalog::OpOwner;
use rdm_core::{IncarnationId, NodeId};
use rdm_node::before_ready::{BeforeReady, BeforeReadyCtx, BeforeReadyHook, HookOutcome};
use rdm_test::before_ready_probe::{BeforeReadyPull, PullReply, PullRequest, Row};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(60);

fn config(dir: &std::path::Path) -> AdminConfig {
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.display().to_string()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .unwrap()
}

/// Where the hooks send their work: the admin's own Node RPC client and the gateway's node id, handed
/// over once both exist. The fabric-primary calls `sync_state` from its own change, so the hook waits for
/// the gateway it drives.
type Gateway = tokio::sync::watch::Receiver<Option<(Arc<NodeRpcClient>, NodeId)>>;

/// One call of the work to the gateway: the count the gateway answered.
async fn work(gateway: &mut Gateway, what: &str) -> u64 {
    let (client, node) = gateway.wait_for(Option::is_some).await.expect("the test hands the gateway over").clone().expect("present");
    loop {
        let (out, _) = client.call::<BeforeReadyPull>(&NodeTarget::ExactNode(node.clone()), &PullRequest::Pull, &CallOptions::default()).await;
        match out.reply().map(|r| r.value().clone()) {
            Some(PullReply::Rows { rows }) => {
                let served = rows.first().map(|r| r.version).unwrap_or(0);
                tracing::info_span!(target: "rshape", "rafka.fabric.state.update.via-gateway-work", work = what, served)
                    .in_scope(|| tracing::info!("the gateway answered the hook's work"));
                return served;
            }
            // A gateway not yet ready answers nothing yet: the hook asks again.
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

/// The gateway's `before_ready`: nothing to pull.
struct Ready;
#[async_trait::async_trait]
impl BeforeReady for Ready {
    async fn before_ready(&self, _ctx: &BeforeReadyCtx) -> HookOutcome {
        HookOutcome::Ok
    }
}

/// The gateway, launched as the root admin's member, serving the work on op `0x74`.
async fn gateway(root: &Running, dir: &std::path::Path, shared: &std::path::Path, served: Arc<AtomicU64>) -> (rdm_node::node::RunningNode, NodeId) {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("demo-hooks")).unwrap();
    fact.write_record(dir).unwrap();
    let key = rdm_node::node::load_or_mint_key(dir).unwrap();
    let launch = rdm_core::launch::Launch {
        fabric: config(shared).fabric,
        fabric_id: d.fabric_id.clone(),
        name: "mesh1.gateway.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        declared: Vec::new(),
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(rdm_core::launch::Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.into(),
        store_dir: shared.into(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let _ = root.runner.joins.expect(rdm_admin::join::Deployed { listeners: Vec::new(),
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: fact,
        data_dir: dir.display().to_string(),
        last_accepted_name: None,
        held: false,
    });
    let node_id = launch.node_id.clone();
    let running = rdm_node::node::start_with_rafka_time_before_ready_and(
        &launch,
        rdm_transport::clock::RafkaTime::unadopted(),
        BeforeReadyHook::new(Arc::new(Ready)),
        |s| s,
        None,
        move |b, _| {
            b.serve::<BeforeReadyPull, _, _>(OpOwner::Testkit, move |_, _| {
                let n = served.fetch_add(1, Ordering::SeqCst) + 1;
                async move { Ok(PullReply::Rows { rows: vec![Row { version: n, value: "work".into() }] }) }
            })
        },
        &[],
    )
    .await
    .unwrap();
    (running, node_id)
}

/// CONTRACT (fabric-node-hooks.md `sync_state`, `traffic_opened`; fabric-state-sync.md: the fabric
/// leaves state-sync only on the application's `StateSynced` for the checklist): the fabric-primary's
/// `sync_state` hook asks the gateway, another node, for the work and answers `StateSynced` only once
/// the gateway answered; the fabric then reaches ready-for-traffic, and the `traffic_opened` notice asks
/// the gateway again. The gateway serves the work twice, the state-sync call first. Must NOT happen: the
/// fabric ready-for-traffic before the gateway answered the state-sync work, or a notice before it.
#[test]
fn sync_state_drives_work_on_the_gateway_and_traffic_opened_asks_it_again() {
    let dir = std::env::temp_dir().join(format!("demo-hooks-{}", NodeId::mint()));
    let capture = rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-fabric-hooks").unwrap();
    capture.run(4, async {
        let (hand, gateway_rx) = tokio::sync::watch::channel(None);
        // What each hook saw the gateway answer, in the order the hooks ran.
        let answered: Arc<Mutex<Vec<(&'static str, u64)>>> = Default::default();
        let sync_state = {
            let (gateway_rx, answered) = (gateway_rx.clone(), answered.clone());
            SyncState::hook(move |handoff: StateSyncHandoff| {
                let (mut gateway_rx, answered) = (gateway_rx.clone(), answered.clone());
                async move {
                    let served = work(&mut gateway_rx, "state-sync").await;
                    answered.lock().unwrap().push(("state-sync", served));
                    handoff
                }
            })
        };
        let traffic_opened: TrafficOpenedNotice = {
            let (gateway_rx, answered) = (gateway_rx.clone(), answered.clone());
            Arc::new(move |_handoff: StateSyncHandoff| {
                let (mut gateway_rx, answered) = (gateway_rx.clone(), answered.clone());
                Box::pin(async move {
                    let served = work(&mut gateway_rx, "re-sync").await;
                    answered.lock().unwrap().push(("re-sync", served));
                })
            })
        };
        let root_dir = dir.join("admin");
        let root = start_with(
            config(&root_dir),
            Wiring { fabric_hooks: FabricHooks { sync_state: Some(sync_state), traffic_opened: Some(traffic_opened) }, ..Wiring::no_certs() },
        )
        .await
        .unwrap();
        // The Day-0 Build's attempt is claimed and the change reaches state-sync, where the hook waits for
        // the gateway. Then this admin reconciles nothing more: the gateway below is launched by hand, so
        // no drift pass takes it for surplus.
        tokio::time::timeout(BOUND, async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.node_admin.fabric.update.via-sync-state-called") {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("sync_state is called at the Day-0 state-sync");
        root.stop_reconciling();

        // The gateway comes up while the fabric waits in state-sync; the hooks get where to send.
        let served = Arc::new(AtomicU64::new(0));
        let (g, gateway_id) = gateway(&root, &dir.join("gateway.1"), &root_dir, served.clone()).await;
        hand.send(Some((root.node_rpc.client.clone(), gateway_id))).unwrap();

        // The state-sync work holds the fabric in state-sync until the gateway answered it; the notice
        // comes only once the fabric is ready-for-traffic, and asks the gateway again.
        tokio::time::timeout(BOUND, async {
            while answered.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the state-sync work, then traffic_opened's");
        assert_eq!(*answered.lock().unwrap(), vec![("state-sync", 1), ("re-sync", 2)], "the state-sync work first, then the re-sync");
        assert_eq!(served.load(Ordering::SeqCst), 2, "the gateway served the work twice");

        let spans = capture.spans().unwrap();
        let named = |name: &str| spans.iter().filter(|s| s["name"] == name).count();
        assert_eq!(named("rdm.node_admin.fabric.update.via-sync-state-hook"), 1, "sync_state is called once for the checklist");
        assert_eq!(named("rdm.node_admin.fabric.update.via-traffic-opened-hook"), 1, "traffic_opened is told once");
        g.stop().await;
    });
    let _ = std::fs::remove_dir_all(&dir);
}
