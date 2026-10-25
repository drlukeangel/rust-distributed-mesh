//! Demo acceptance: the consumer's node-admin opens and accepts connections on the shape's data-plane
//! protocol (`demo/echo`, `rshape_consumer::data_alpn`), as an embedding's node-admin does with its own
//! app protocols (Rafka's admin to its brokers).
//!
//! Why missed: every demo cell had only role nodes (rpc nodes, gateways) dial on `demo/echo`; the
//! consumer's `rshape-node-admin` registers no app protocol, and `rdm_admin::entry::run_on` never hands
//! it its `Running`, so no demo flow had an admin, let alone the Day-0 admin (which made no JoinNode
//! and held no member cert), present a cert on an app-protocol dial. Rafka 141 was the first to.

use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::{IncarnationId, NodeId};
use rdm_node::connections::{Acceptor, Birth, Dialer};
use rdm_transport::{NodeResolver, NodeTarget};
use std::path::Path;
use std::time::Duration;

const BOUND: Duration = Duration::from_secs(30);

fn config(dir: &Path) -> AdminConfig {
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.display().to_string()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .unwrap()
}

/// A gateway of mesh1 the Day-0 admin launched, `demo/echo` registered, admitting on the testkit's root.
async fn gateway(root: &Running, dir: &Path, shared: &Path) -> rdm_node::node::RunningNode {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("demo-admin-app")).unwrap();
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
        roots: vec![rdm_test::test_certs::test_root_public()],
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let _ = root.runner.joins.expect(rdm_admin::join::Deployed {
        listeners: Vec::new(),
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
    rdm_node::node::start_with_rafka_time_before_ready_alpns_and(&launch, rdm_transport::clock::RafkaTime::unadopted(), rdm_node::before_ready::BeforeReadyHook::none(), |s| s, None, |b, _| b, &[], &[rshape_consumer::data_alpn()]).await.unwrap()
}

fn birth(n: &rdm_core::digest::MeshNode) -> Birth {
    Birth { name: n.name.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation.clone() }
}

/// `from` opens on `demo/echo` to `to` and writes `payload`; `to_acceptor`'s app answers it named
/// `echo <to>: <payload>`.
async fn exchange(from: &Dialer, to: &rdm_core::digest::MeshNode, to_acceptor: &mut Acceptor, payload: &str) {
    let answer_as = to.name.to_string();
    let accepted = async {
        let conn = tokio::time::timeout(BOUND, to_acceptor.accept()).await.ok().flatten().unwrap_or_else(|| panic!("{answer_as}'s app accepted no connection"));
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();
        let bytes = recv.read_to_end(1 << 16).await.unwrap();
        send.write_all(format!("echo {answer_as}: {}", String::from_utf8_lossy(&bytes)).as_bytes()).await.unwrap();
        send.finish().unwrap();
        let _ = conn.closed().await;
    };
    let opened = async {
        let conn = match tokio::time::timeout(BOUND, from.open(&birth(to))).await {
            Ok(Ok(c)) => c,
            other => panic!("a dial to {} on demo/echo is refused: {other:?}", to.name),
        };
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(payload.as_bytes()).await.unwrap();
        send.finish().unwrap();
        let answered = recv.read_to_end(1 << 16).await.unwrap();
        drop(conn);
        String::from_utf8(answered).unwrap()
    };
    let (answered, ()) = tokio::select! {
        r = async { tokio::join!(opened, accepted) } => r,
        () = tokio::time::sleep(BOUND * 2) => panic!("the exchange with {} never ended", to.name),
    };
    assert_eq!(answered, format!("echo {}: {payload}", to.name));
}

/// CONTRACT (fabric-certs.md §When a cert is issued, §On a connection; node-connections-admit.md): the
/// consumer's Day-0 node-admin, holding the fabric's root, registers `demo/echo`; it dials a gateway it
/// launched on it and is admitted (the gateway's app answers), and the gateway's dial to it is admitted
/// (the admin's app answers). Must NOT happen: the Day-0 admin's dial refused `no-credential`.
#[test]
fn the_consumers_day0_admin_and_a_gateway_exchange_streams_on_the_data_plane_protocol() {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("demo-admin-app-{}", NodeId::mint()));
        let root = start_with(config(&dir), Wiring { alpns: vec![rshape_consumer::data_alpn()], certs: rdm_test::test_certs::root_choice(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.unwrap();
        root.stop_reconciling();
        let (admin_dialer, mut admin_acceptor) = root.connections(b"demo/echo").expect("the admin's demo/echo handle").split();
        let gw = gateway(&root, &dir.join("mesh1.gateway.1"), &dir).await;
        let (gw_dialer, mut gw_acceptor) = gw.connections(b"demo/echo").expect("the gateway's demo/echo handle").split();
        let admin = root.birth();
        let gw_birth = gw.digest.node.clone();
        tokio::time::timeout(BOUND, async {
            while root.node_rpc.resolver.resolve(&NodeTarget::ExactNode(gw_birth.node_id.clone())).is_err() || gw.node_rpc.resolver.resolve(&NodeTarget::ExactNode(admin.node_id.clone())).is_err() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the admin and the gateway resolve each other");

        exchange(&admin_dialer, &gw_birth, &mut gw_acceptor, "from the Day-0 admin").await;
        exchange(&gw_dialer, &admin, &mut admin_acceptor, "from the gateway").await;

        gw.stop().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
}
