//! The fabric hook a consumer registers, on public RDM surfaces only (fabric-node-hooks.md
//! `traffic_opened`; Q171). Its `traffic_opened` notice, after the fabric is ready-for-traffic, drives
//! work on another node: Rafka's day 0, its own work, starts there and asks the gateway to perform the
//! system-org creations.
//!
//! The work is the testkit's op `0x74` (`BeforeReadyPull`), which stands in for a product's own op: the
//! gateway counts each call it serves and answers with the count.
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, TrafficOpened, TrafficOpenedNotice, Wiring};
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

/// Where the notice sends its work: the admin's own Node RPC client and the gateway's node id, handed
/// over once both exist. The fabric-primary sends the notice from its own change, so the notice waits for
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

/// CONTRACT (fabric-node-hooks.md `traffic_opened`: "After the ready-for-traffic state is set, the
/// fabric-primary sends the application one `traffic_opened` notice"; Q171): the fabric-primary's one
/// notice, sent once the Day-0 fabric is ready-for-traffic, asks the gateway, another node, for the work;
/// the gateway serves it once. Must NOT happen: a second notice, or the notice before ready-for-traffic.
#[test]
fn traffic_opened_drives_work_on_the_gateway() {
    let dir = std::env::temp_dir().join(format!("demo-hooks-{}", NodeId::mint()));
    let capture = rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-fabric-hooks").unwrap();
    capture.run(4, async {
        let (hand, gateway_rx) = tokio::sync::watch::channel(None);
        // What the notice saw the gateway answer.
        let answered: Arc<Mutex<Vec<(&'static str, u64)>>> = Default::default();
        let traffic_opened: TrafficOpenedNotice = {
            let (gateway_rx, answered) = (gateway_rx.clone(), answered.clone());
            Arc::new(move |_opened: TrafficOpened| {
                let (mut gateway_rx, answered) = (gateway_rx.clone(), answered.clone());
                Box::pin(async move {
                    let served = work(&mut gateway_rx, "day-0").await;
                    answered.lock().unwrap().push(("day-0", served));
                })
            })
        };
        let root_dir = dir.join("admin");
        let root = start_with(config(&root_dir), Wiring { fabric_hooks: FabricHooks { traffic_opened: Some(traffic_opened) }, ..Wiring::no_certs() }).await.unwrap();
        // The Day-0 change reaches ready-for-traffic, and the notice waits for the gateway. Then this admin
        // reconciles nothing more: the gateway below is launched by hand, so no drift pass takes it for surplus.
        tokio::time::timeout(BOUND, async {
            while !capture.spans().unwrap().iter().any(|s| s["name"] == "rdm.node_admin.fabric.update.via-state-entered" && s["attributes"]["to"] == "ready-for-traffic") {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the Day-0 fabric reaches ready-for-traffic");
        root.stop_reconciling();

        // The gateway comes up; the notice gets where to send.
        let served = Arc::new(AtomicU64::new(0));
        let (g, gateway_id) = gateway(&root, &dir.join("gateway.1"), &root_dir, served.clone()).await;
        hand.send(Some((root.node_rpc.client.clone(), gateway_id))).unwrap();
        tokio::time::timeout(BOUND, async {
            while answered.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the notice's work");
        assert_eq!(*answered.lock().unwrap(), vec![("day-0", 1)], "the notice drove the work once");
        assert_eq!(served.load(Ordering::SeqCst), 1, "the gateway served the work once");

        let spans = capture.spans().unwrap();
        let named = |name: &str| spans.iter().filter(|s| s["name"] == name).count();
        assert_eq!(named("rdm.node_admin.fabric.update.via-traffic-opened-hook"), 1, "traffic_opened is told once");
        g.stop().await;
    });
    let _ = std::fs::remove_dir_all(&dir);
}
