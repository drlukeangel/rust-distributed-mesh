//! Demo acceptance (`i148`): two `rpc_node`s and one `gateway` exercise the data-plane
//! connections surface (`rdm_node::connections`) and fault recovery in the separately built,
//! pinned consumer (`demo/Cargo.toml`), with no direct `iroh` access and no private RDM hook.
//!
//! Each of the six rows of the Demo acceptance hazard table is one test cell asserting the
//! connection rows written on the shared store root, the typed outcomes returned, and the
//! stream payloads delivered; the three-node flame (`rpc.1` → `gateway.1` → `rpc.2`) is
//! captured and verified alongside the assertions.

use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::connections::{ConnectionKind as CoreKind, ConnectionState, NodeConnection};
use rdm_core::reconnect::next_due;
use rdm_core::{IncarnationId, NodeId, NodeKind, PathName};
use rdm_node::connections::{
    Alpn, Birth, Closed, ConnectError, Connection, ConnectionKind, Connections, StreamError,
};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rdm_core::views::{ConnectionView, ConnectionsView};
use rdm_test::connection_faults::{block_direct, fault_pre_opened_once, unblock_direct, PreOpenedFault};
use rdm_transport::{CallOptions, NodeResolver, NodeTarget};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

const ALPN_A: &[u8] = b"demo/framing/1";
const ALPN_B: &[u8] = b"demo/bulk/1";
const BOUND: Duration = Duration::from_secs(30);
static CELL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn config(dir: &Path) -> AdminConfig {
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.display().to_string()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .unwrap()
}

fn forwardable_gateway(alpn: &'static [u8]) -> Alpn {
    Alpn {
        name: alpn.to_vec(),
        proxy: true,
        carriers: vec![NodeKind::Gateway],
    }
}

fn birth_of(node: &rdm_node::node::RunningNode) -> Birth {
    Birth {
        name: node.digest.node.name.clone(),
        node_id: node.digest.node.node_id.clone(),
        incarnation: node.digest.node.incarnation.clone(),
    }
}

async fn boot_admin_with_committed_store_root(root_dir: &Path) -> (Running, PathBuf) {
    let root = start_with(
        config(root_dir),
        // The fabric runs with certs, as a consumer's does: the root admin holds the testkit's root
        // (fabric-certs.md: `CertChoice::Ca`), issues every birth its member cert at join, and every
        // acceptor admits a product connection on the dialer's chain.
        Wiring {
            fabric_hooks: FabricHooks::no_app_work(),
            certs: rdm_test::test_certs::root_choice(),
            ..Wiring::default()
        },
    )
    .await
    .unwrap();
    let fabric_id = root.digest.lock().unwrap().fabric_id.clone();
    let fabric_root = root_dir.join(fabric_id.to_string());
    let identity = fabric_root.join(rdm_node::store_root::FABRIC_FILE);
    tokio::time::timeout(BOUND, async {
        while rdm_node::store_log::read_record(&identity, "fabric-record", None).is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the Day-0 admin appends the Fabric record's line to fabric.jsonl so StoreRoot opens on the shared store-root");
    root.stop_reconciling().await;
    (root, fabric_root)
}

async fn spawn_member(
    root: &Running,
    dir: &Path,
    shared: &Path,
    name: &str,
    alpns: Vec<Alpn>,
) -> rdm_node::node::RunningNode {
    spawn_member_with_time(
        root,
        dir,
        shared,
        name,
        alpns,
        rdm_transport::clock::RafkaTime::unadopted(),
        None,
    )
    .await
}

async fn spawn_member_with_time(
    root: &Running,
    dir: &Path,
    shared: &Path,
    name: &str,
    alpns: Vec<Alpn>,
    time: rdm_transport::clock::RafkaTime,
    previous: Option<(NodeId, IncarnationId)>,
) -> rdm_node::node::RunningNode {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    let fact =
        rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("demo-conn")).unwrap();
    fact.write_record(dir).unwrap();
    let key = rdm_node::node::load_or_mint_key(dir).unwrap();
    let (node_id, incarnation, supersedes) = match previous {
        Some((nid, prev_inc)) => (nid, IncarnationId::mint(), Some(prev_inc)),
        None => (NodeId::mint(), IncarnationId::mint(), None),
    };
    let launch = rdm_core::launch::Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: config(shared).fabric,
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id,
        incarnation,
        supersedes: supersedes.clone(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        declared: Vec::new(),
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(rdm_core::launch::Launcher {
            name: d.node.name.clone(),
            node_id: d.node.node_id.clone(),
            incarnation: d.node.incarnation.clone(),
        }),
        data_dir: dir.into(),
        store_dir: shared.into(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: vec![rdm_test::test_certs::test_root_public()],
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let _ = root.runner.joins.expect(rdm_admin::join::Deployed { listeners: Vec::new(),
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: fact,
        data_dir: dir.display().to_string(),
        last_accepted_name: None,
        held: false,
    });
    rdm_node::node::start_with_rafka_time_pre_node_ready_alpns_and(
        &launch,
        time,
        rdm_node::node_hooks::NodeHooks::none(),
        |s| s,
        None,
        |b, _| b,
        &[],
        &alpns,
    )
    .await
    .unwrap()
}

async fn wait_for_members(nodes: &[&rdm_node::node::RunningNode], names: &[&str]) {
    let paths: Vec<PathName> = names.iter().map(|n| n.parse().unwrap()).collect();
    tokio::time::timeout(BOUND, async {
        loop {
            if nodes.iter().all(|n| {
                paths.iter().all(|p| {
                    n.node_rpc
                        .resolver
                        .resolve(&NodeTarget::CurrentPath(p.clone()))
                        .is_ok()
                })
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all nodes must resolve every member in the mesh");
}

fn alpn_hex(alpn: &[u8]) -> String {
    alpn.iter().map(|b| format!("{b:02x}")).collect()
}

/// One connection entry's place on the store root: its key in the root's `connections.jsonl`, where every
/// fact is a line and the entry is the key's latest line (R-SR1).
struct RowAt {
    file: PathBuf,
    key: String,
}

impl RowAt {
    fn display(&self) -> String {
        format!("{}#{}", self.file.display(), self.key)
    }
}

/// The entry at `at`: the key's latest line, unless that line forgot it (`removed`).
fn read_row(at: &RowAt) -> Option<NodeConnection> {
    let lines = rdm_node::store_log::read_file(&at.file).ok()?;
    let line = lines.into_iter().rev().find(|l| l.kind() == "node-connection" && l.key == at.key)?;
    if line.record.get("removed").and_then(|r| r.as_bool()) == Some(true) {
        return None;
    }
    serde_json::from_value(line.record).ok()
}

async fn wait_for_row(at: &RowAt, expected: ConnectionState) -> NodeConnection {
    tokio::time::timeout(BOUND, async {
        loop {
            if let Some(row) = read_row(at) {
                if row.state == expected {
                    break row;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {expected:?} at {}", at.display()))
}

fn row_at(fabric_root: &Path, source: &str, destination: &str, alpn: &[u8], kind: &str) -> RowAt {
    RowAt {
        file: fabric_root.join(rdm_node::store_root::CONNECTIONS_FILE),
        key: format!("{source}--{destination}--{}--{kind}", alpn_hex(alpn)),
    }
}

fn direct_index_path(fabric_root: &Path, source: &str, destination: &str, alpn: &[u8]) -> RowAt {
    row_at(fabric_root, source, destination, alpn, "direct")
}

fn proxy_index_path(fabric_root: &Path, source: &str, destination: &str, alpn: &[u8]) -> RowAt {
    row_at(fabric_root, source, destination, alpn, "proxy")
}

/// The raw connection log of the node at `source`: every fact it wrote, oldest first.
fn read_history(fabric_root: &Path, source: &str) -> Vec<NodeConnection> {
    rdm_node::storage::connection_history(fabric_root, source).unwrap_or_default()
}

async fn open_pair(
    dialer_conns: &Connections,
    acceptor_conns: &mut Connections,
    dest: &Birth,
) -> (Connection, Connection) {
    let client = tokio::time::timeout(BOUND, dialer_conns.open(dest))
        .await
        .unwrap()
        .unwrap();
    let server = tokio::time::timeout(BOUND, acceptor_conns.accept())
        .await
        .unwrap()
        .unwrap();
    (client, server)
}

async fn establish_direct_edge(
    fabric_root: &Path,
    source: &str,
    dialer_conns: &Connections,
    acceptor_conns: &mut Connections,
    dest: &Birth,
    alpn: &[u8],
) {
    let (client, server) = open_pair(dialer_conns, acceptor_conns, dest).await;
    assert_eq!(client.kind(), ConnectionKind::Direct);
    let (mut c_send, mut c_recv) = client.open_bi().await.unwrap();
    c_send.write_all(b"warmup").await.unwrap();
    c_send.finish().unwrap();
    let (mut s_send, mut s_recv) = server.accept_bi().await.unwrap();
    assert_eq!(s_recv.read_to_end(64).await.unwrap(), b"warmup");
    s_send.write_all(b"warmed").await.unwrap();
    s_send.finish().unwrap();
    assert_eq!(c_recv.read_to_end(64).await.unwrap(), b"warmed");
    let path = direct_index_path(fabric_root, source, &dest.name.to_string(), alpn);
    wait_for_row(&path, ConnectionState::Connected).await;
}

/// What the node's `node.connection.get` answers for a stream Proxy that is still open after its
/// route was replaced by Direct (connections.md §10: the tunnel outlives the Proxy's
/// `Disconnected(direct-restored)` write).
const TUNNEL_PAST_ITS_ROUTE: &str = "route: deleted";

/// `node.connection.get { node }` of the exact birth `node`, sent by `root`'s Node RPC client, answered
/// within `bound`. The node answers once every observation handed to its connections writer has
/// landed, a Direct Connected's owed `Proxy Disconnected(direct-restored)` write included, so the
/// answer is the held projection new opens resolve over (connections.md §10: "Only then does
/// Direct become effective"). Panics naming the read when it is not answered within `bound` or is
/// answered with anything but the node's connections.
async fn wait_for_held_connections(root: &Running, node: &Birth, bound: Duration) -> ConnectionsView {
    let what = format!("node.connection.get of {} ({})", node.name, node.node_id);
    let req = StatusRequest::Get {
        entity: rdm_core::filter::Entity::Connections,
        node: Some((node.node_id.clone(), node.incarnation.clone())),
        filter: rdm_core::filter::Filter::new(),
    };
    let (out, _) = tokio::time::timeout(
        bound,
        root.node_rpc.client.call::<Status>(
            &NodeTarget::ExactNode(node.node_id.clone()),
            &req,
            &CallOptions::default(),
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("{what}: not answered within {bound:?}"));
    match out {
        RpcOutcome::Reply(r) => match r.into_value() {
            StatusReply::Connections { connections } => connections,
            other => panic!("{what}: answered {other:?}"),
        },
        other => panic!("{what}: ended {other:?}"),
    }
}

/// The entry `id` in `held`, by the id `node.connection.get` names it by.
fn held_entry<'a>(held: &'a ConnectionsView, id: &rdm_core::ConnectionId) -> Option<&'a ConnectionView> {
    held.connections.iter().find(|c| c.id == id.to_string())
}

async fn delete_connections_command(
    root: &Running,
    birth: &Birth,
    scope: rdm_core::status::ConnectionScope,
) -> Vec<rdm_core::ConnectionId> {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
    use rdm_transport::stream::StreamItem;
    let req = NodeCommandRequest::DeleteConnections {
        node_id: birth.node_id.clone(),
        incarnation: birth.incarnation.clone(),
        scope,
    };
    let (mut stream, _) = root
        .node_rpc
        .client
        .call_stream::<NodeCommand>(
            &NodeTarget::ExactNode(birth.node_id.clone()),
            &req,
            &rdm_transport::CallOptions::default(),
        )
        .await
        .map(|(s, e)| (s.read_only(), e))
        .expect("DeleteConnections NodeCommand stream must open");
    let mut frames = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            StreamItem::Frame(kind, frame) => {
                frames.push(frame);
                if matches!(
                    kind,
                    rdm_core::streaming::FrameKind::Terminal
                        | rdm_core::streaming::FrameKind::Refusal(_)
                ) {
                    break;
                }
            }
            StreamItem::Failed(f) => panic!("DeleteConnections stream failed: {f:?}"),
        }
    }
    match frames.pop() {
        Some(NodeCommandReply::ConnectionsDeleted { closed, .. }) => closed,
        other => panic!("expected ConnectionsDeleted terminal frame, got: {other:?}"),
    }
}

/// Row 1 — `false Proxy establishment`:
/// Carrier admission alone, application bytes, EOF, a wrong birth and a handshake lapse each
/// create **no** `Proxy Connected`; the destination's `Opened` at the exact birth does. With the
/// Direct not sent and gateway.1 the one carrier, an open no carrier carries ends with no active
/// route, naming the Direct's reason (connections.md §6, Q227).
#[test]
fn false_proxy_establishment_creates_no_proxy_connected_until_the_destinations_exact_birth_answers_opened() {
    let _serial = CELL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    const ROW1_ALPNS: [&[u8]; 6] = [
        b"demo/framing/1",
        b"demo/framing/2",
        b"demo/framing/3",
        b"demo/framing/4",
        b"demo/framing/5",
        b"demo/framing/6",
    ];
    let dir = std::env::temp_dir().join(format!("demo-conn-row1-{}", NodeId::mint()));
    let capture =
        rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-conn-row1").unwrap();
    capture.run(4, async {
        let root_dir = dir.join("admin");
        let (root, fabric_root) = boot_admin_with_committed_store_root(&root_dir).await;
        let alpn_defs: Vec<Alpn> = ROW1_ALPNS.iter().map(|a| forwardable_gateway(a)).collect();
        let gw1 = spawn_member(
            &root,
            &dir.join("gw1"),
            &root_dir,
            "mesh1.gateway.1",
            alpn_defs.clone(),
        )
        .await;
        let rpc1 = spawn_member(
            &root,
            &dir.join("rpc1"),
            &root_dir,
            "mesh1.rpc.1",
            alpn_defs.clone(),
        )
        .await;
        let rpc2 = spawn_member(
            &root,
            &dir.join("rpc2"),
            &root_dir,
            "mesh1.rpc.2",
            alpn_defs,
        )
        .await;
        wait_for_members(
            &[&gw1, &rpc1, &rpc2],
            &["mesh1.gateway.1", "mesh1.rpc.1", "mesh1.rpc.2"],
        )
        .await;

        let gw1_birth = birth_of(&gw1);
        let rpc2_birth = birth_of(&rpc2);
        let rpc2_name = rpc2_birth.name.clone();

        let mut rpc1_conns = Vec::new();
        let mut gw1_conns = Vec::new();
        let mut rpc2_conns = Vec::new();
        for alpn in ROW1_ALPNS {
            let r1 = rpc1.connections(alpn).unwrap();
            let mut g1 = gw1.connections(alpn).unwrap();
            let mut r2 = rpc2.connections(alpn).unwrap();
            establish_direct_edge(&fabric_root, "mesh1.rpc.1", &r1, &mut g1, &gw1_birth, alpn)
                .await;
            establish_direct_edge(&fabric_root, "mesh1.gateway.1", &g1, &mut r2, &rpc2_birth, alpn)
                .await;
            block_direct(&rpc1, &rpc2_name, alpn);
            rpc1_conns.push(r1);
            gw1_conns.push(g1);
            rpc2_conns.push(r2);
        }

        // 1. Carrier admission alone (gateway.1 admits rpc.1's tunnel, then closes before destination Opened):
        let alpn1 = ROW1_ALPNS[0];
        block_direct(&gw1, &rpc2_name, alpn1);
        let err1 = rpc1_conns[0].open(&rpc2_birth).await.unwrap_err();
        assert!(
            matches!(err1, ConnectError::NoActiveRoute { direct: Some(_) }),
            "carrier admission alone without reaching destination must fail before Opened, with no active route once the one carrier carried nothing, got: {err1:?}"
        );
        assert!(
            read_row(&proxy_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", alpn1)).is_none(),
            "carrier admission alone must write no Proxy Connected row"
        );
        unblock_direct(&gw1, &rpc2_name, alpn1);

        // 2. Application bytes on the control stream before Opened:
        let alpn2 = ROW1_ALPNS[1];
        fault_pre_opened_once(
            &rpc2,
            alpn2,
            PreOpenedFault::ApplicationBytes(b"premature-app-frame".to_vec()),
        );
        let err2 = rpc1_conns[1].open(&rpc2_birth).await.unwrap_err();
        assert!(
            matches!(err2, ConnectError::NoActiveRoute { direct: Some(_) }),
            "application bytes before Opened must fail handshake, with no active route once the one carrier carried nothing, got: {err2:?}"
        );
        assert!(
            read_row(&proxy_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", alpn2)).is_none(),
            "application bytes before Opened must write no Proxy Connected row"
        );

        // 3. EOF on the control stream before Opened:
        let alpn3 = ROW1_ALPNS[2];
        fault_pre_opened_once(&rpc2, alpn3, PreOpenedFault::Eof);
        let err3 = rpc1_conns[2].open(&rpc2_birth).await.unwrap_err();
        assert!(
            matches!(err3, ConnectError::NoActiveRoute { direct: Some(_) }),
            "EOF before Opened must fail handshake, with no active route once the one carrier carried nothing, got: {err3:?}"
        );
        assert!(
            read_row(&proxy_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", alpn3)).is_none(),
            "EOF before Opened must write no Proxy Connected row"
        );

        // 4. Wrong birth before Opened:
        let alpn4 = ROW1_ALPNS[3];
        let wrong_birth = Birth {
            incarnation: IncarnationId::mint(),
            ..rpc2_birth.clone()
        };
        let err4 = rpc1_conns[3].open(&wrong_birth).await.unwrap_err();
        assert!(
            matches!(
                err4,
                ConnectError::WrongIncarnation { ref held, ref found }
                    if held == &wrong_birth.incarnation && found == &rpc2_birth.incarnation
            ),
            "wrong birth at destination must return WrongIncarnation, got: {err4:?}"
        );
        assert!(
            read_row(&proxy_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", alpn4)).is_none(),
            "a wrong birth must write no Proxy Connected row"
        );

        // 5. Handshake lapse before Opened:
        let alpn5 = ROW1_ALPNS[4];
        fault_pre_opened_once(&rpc2, alpn5, PreOpenedFault::HandshakeLapse);
        let err5 = rpc1_conns[4].open(&rpc2_birth).await.unwrap_err();
        assert!(
            matches!(err5, ConnectError::NoActiveRoute { direct: Some(_) }),
            "handshake lapse before Opened must fail handshake, with no active route once the one carrier carried nothing, got: {err5:?}"
        );
        assert!(
            read_row(&proxy_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", alpn5)).is_none(),
            "a handshake lapse must write no Proxy Connected row"
        );

        // 6. Now destination's exact birth answers Opened: Proxy Connected is written and carries bytes.
        let alpn6 = ROW1_ALPNS[5];
        let (client, server) = open_pair(&rpc1_conns[5], &mut rpc2_conns[5], &rpc2_birth).await;
        assert_eq!(
            client.kind(),
            ConnectionKind::Proxy {
                via: gw1_birth.clone()
            }
        );
        let proxy_file = proxy_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", alpn6);
        let proxy_row = wait_for_row(&proxy_file, ConnectionState::Connected).await;
        assert_eq!(proxy_row.kind, CoreKind::Proxy);
        assert_eq!(proxy_row.state, ConnectionState::Connected);
        assert_eq!(proxy_row.alpn, alpn6);
        assert_eq!(proxy_row.carrier.as_ref().unwrap().name, gw1_birth.name);

        let (mut c_send, mut c_recv) = client.open_bi().await.unwrap();
        c_send.write_all(b"exact-birth-opened").await.unwrap();
        c_send.finish().unwrap();
        let (mut s_send, mut s_recv) = server.accept_bi().await.unwrap();
        assert_eq!(
            s_recv.read_to_end(64).await.unwrap(),
            b"exact-birth-opened"
        );
        s_send.write_all(b"ack:exact-birth-opened").await.unwrap();
        s_send.finish().unwrap();
        assert_eq!(
            c_recv.read_to_end(64).await.unwrap(),
            b"ack:exact-birth-opened"
        );

        drop(client);
        drop(server);
        drop(rpc1_conns);
        drop(gw1_conns);
        drop(rpc2_conns);
        rpc1.stop().await;
        rpc2.stop().await;
        gw1.stop().await;
        root.leave().await;
    });
}

/// Row 2 — `application outcome confused with transport success`:
/// Establish the tunnel, apply a counter mutation, lose its reply: RDM reports no application
/// success and replays nothing; the counter shows one execution.
#[test]
fn tunnel_stream_reply_loss_after_counter_mutation_reports_error_and_replays_nothing() {
    let _serial = CELL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("demo-conn-row2-{}", NodeId::mint()));
    let capture =
        rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-conn-row2").unwrap();
    capture.run(4, async {
        let root_dir = dir.join("admin");
        let (root, fabric_root) = boot_admin_with_committed_store_root(&root_dir).await;
        let gw1 = spawn_member(
            &root,
            &dir.join("gw1"),
            &root_dir,
            "mesh1.gateway.1",
            vec![forwardable_gateway(ALPN_A)],
        )
        .await;
        let rpc1 = spawn_member(
            &root,
            &dir.join("rpc1"),
            &root_dir,
            "mesh1.rpc.1",
            vec![forwardable_gateway(ALPN_A)],
        )
        .await;
        let rpc2 = spawn_member(
            &root,
            &dir.join("rpc2"),
            &root_dir,
            "mesh1.rpc.2",
            vec![forwardable_gateway(ALPN_A)],
        )
        .await;
        wait_for_members(
            &[&gw1, &rpc1, &rpc2],
            &["mesh1.gateway.1", "mesh1.rpc.1", "mesh1.rpc.2"],
        )
        .await;

        let gw1_birth = birth_of(&gw1);
        let rpc2_birth = birth_of(&rpc2);
        let rpc2_name = rpc2_birth.name.clone();

        let rpc1_a = rpc1.connections(ALPN_A).unwrap();
        let mut gw1_a = gw1.connections(ALPN_A).unwrap();
        let mut rpc2_a = rpc2.connections(ALPN_A).unwrap();

        establish_direct_edge(&fabric_root, "mesh1.rpc.1", &rpc1_a, &mut gw1_a, &gw1_birth, ALPN_A)
            .await;
        establish_direct_edge(
            &fabric_root,
            "mesh1.gateway.1",
            &gw1_a,
            &mut rpc2_a,
            &rpc2_birth,
            ALPN_A,
        )
        .await;

        block_direct(&rpc1, &rpc2_name, ALPN_A);
        let (client, server) = open_pair(&rpc1_a, &mut rpc2_a, &rpc2_birth).await;
        assert_eq!(
            client.kind(),
            ConnectionKind::Proxy {
                via: gw1_birth.clone()
            }
        );

        let counter = Arc::new(AtomicU64::new(0));
        let counter_srv = counter.clone();

        let srv_task = tokio::spawn(async move {
            let (mut s_send, mut s_recv) = server.accept_bi().await.unwrap();
            let req = s_recv.read_to_end(64).await.unwrap();
            assert_eq!(req, b"increment-counter");
            counter_srv.fetch_add(1, Ordering::SeqCst);
            // Lose the reply by resetting the reply stream before writing any reply bytes.
            s_send.reset(42).unwrap();
            server
        });

        let (mut c_send, mut c_recv) = client.open_bi().await.unwrap();
        c_send.write_all(b"increment-counter").await.unwrap();
        c_send.finish().unwrap();
        let read_outcome = c_recv.read_to_end(64).await;
        assert!(
            matches!(
                read_outcome,
                Err(StreamError::Reset { code: 42 }) | Err(StreamError::Closed(_))
            ),
            "losing the reply on the carried stream must surface an error to the caller, got: {read_outcome:?}"
        );
        let server = srv_task.await.unwrap();
        // Give the runtime a moment to verify RDM never replays the stream automatically.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "RDM replays nothing: the counter mutation executed exactly once"
        );

        drop(client);
        drop(server);
        drop(rpc1_a);
        drop(gw1_a);
        drop(rpc2_a);
        rpc1.stop().await;
        rpc2.stop().await;
        gw1.stop().await;
        root.leave().await;
    });
}

/// Row 3 — `ALPN interference`:
/// Register A and B between the same births; restore A while B uses its Proxy: B's route and
/// streams are unchanged.
#[test]
fn restoring_direct_on_alpn_a_leaves_alpn_bs_proxy_route_and_streams_unchanged() {
    let _serial = CELL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("demo-conn-row3-{}", NodeId::mint()));
    let capture =
        rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-conn-row3").unwrap();
    capture.run(4, async {
        let root_dir = dir.join("admin");
        let (root, fabric_root) = boot_admin_with_committed_store_root(&root_dir).await;
        let gw1 = spawn_member(
            &root,
            &dir.join("gw1"),
            &root_dir,
            "mesh1.gateway.1",
            vec![forwardable_gateway(ALPN_A), forwardable_gateway(ALPN_B)],
        )
        .await;
        let rpc1 = spawn_member(
            &root,
            &dir.join("rpc1"),
            &root_dir,
            "mesh1.rpc.1",
            vec![forwardable_gateway(ALPN_A), forwardable_gateway(ALPN_B)],
        )
        .await;
        let rpc2 = spawn_member(
            &root,
            &dir.join("rpc2"),
            &root_dir,
            "mesh1.rpc.2",
            vec![forwardable_gateway(ALPN_A), forwardable_gateway(ALPN_B)],
        )
        .await;
        wait_for_members(
            &[&gw1, &rpc1, &rpc2],
            &["mesh1.gateway.1", "mesh1.rpc.1", "mesh1.rpc.2"],
        )
        .await;

        let gw1_birth = birth_of(&gw1);
        let rpc1_birth = birth_of(&rpc1);
        let rpc2_birth = birth_of(&rpc2);
        let rpc2_name = rpc2_birth.name.clone();

        let mut rpc1_a = rpc1.connections(ALPN_A).unwrap();
        let rpc1_b = rpc1.connections(ALPN_B).unwrap();
        let mut gw1_a = gw1.connections(ALPN_A).unwrap();
        let mut gw1_b = gw1.connections(ALPN_B).unwrap();
        let mut rpc2_a = rpc2.connections(ALPN_A).unwrap();
        let mut rpc2_b = rpc2.connections(ALPN_B).unwrap();

        establish_direct_edge(&fabric_root, "mesh1.rpc.1", &rpc1_a, &mut gw1_a, &gw1_birth, ALPN_A)
            .await;
        establish_direct_edge(
            &fabric_root,
            "mesh1.gateway.1",
            &gw1_a,
            &mut rpc2_a,
            &rpc2_birth,
            ALPN_A,
        )
        .await;
        establish_direct_edge(&fabric_root, "mesh1.rpc.1", &rpc1_b, &mut gw1_b, &gw1_birth, ALPN_B)
            .await;
        establish_direct_edge(
            &fabric_root,
            "mesh1.gateway.1",
            &gw1_b,
            &mut rpc2_b,
            &rpc2_birth,
            ALPN_B,
        )
        .await;

        // Block Direct on both ALPN_A and ALPN_B from rpc.1 to rpc.2; establish Proxy on both.
        block_direct(&rpc1, &rpc2_name, ALPN_A);
        block_direct(&rpc1, &rpc2_name, ALPN_B);

        let (proxy_a_client, proxy_a_server) = open_pair(&rpc1_a, &mut rpc2_a, &rpc2_birth).await;
        assert_eq!(
            proxy_a_client.kind(),
            ConnectionKind::Proxy {
                via: gw1_birth.clone()
            }
        );

        let (proxy_b_client, proxy_b_server) = open_pair(&rpc1_b, &mut rpc2_b, &rpc2_birth).await;
        assert_eq!(
            proxy_b_client.kind(),
            ConnectionKind::Proxy {
                via: gw1_birth.clone()
            }
        );
        let proxy_a_file =
            proxy_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", ALPN_A);
        let proxy_b_file =
            proxy_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", ALPN_B);
        wait_for_row(&proxy_a_file, ConnectionState::Connected).await;
        wait_for_row(&proxy_b_file, ConnectionState::Connected).await;

        // Open a live stream on B's Proxy before A restores Direct.
        let (mut b_send, mut b_recv) = proxy_b_client.open_bi().await.unwrap();
        b_send.write_all(b"b-before-a-restores").await.unwrap();
        let (mut bs_send, mut bs_recv) = proxy_b_server.accept_bi().await.unwrap();
        let mut buf = [0u8; 19];
        bs_recv.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"b-before-a-restores");

        // Unblock ALPN_A only (ALPN_B remains blocked!) and trigger inbound recovery on ALPN_A by having rpc.2 dial rpc.1 on ALPN_A.
        unblock_direct(&rpc1, &rpc2_name, ALPN_A);
        let (in_c, in_s) = open_pair(&rpc2_a, &mut rpc1_a, &rpc1_birth).await;
        drop(in_c);
        drop(in_s);

        // Drain the reconnect probe connection accepted by rpc.2 on ALPN_A, and wait for A's Proxy
        // index to be Disconnected(direct-restored) on disk while B's Proxy index stays Connected.
        let probe_on_a = tokio::time::timeout(BOUND, rpc2_a.accept())
            .await
            .expect("rpc.1's reconnect scheduler must probe rpc.2 on ALPN_A")
            .unwrap();
        drop(probe_on_a);

        let row_a = wait_for_row(&proxy_a_file, ConnectionState::Disconnected).await;
        assert_eq!(row_a.reason.as_deref(), Some("direct-restored"));
        wait_for_row(
            &direct_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", ALPN_A),
            ConnectionState::Connected,
        )
        .await;

        let row_b = read_row(&proxy_b_file).unwrap();
        assert_eq!(
            row_b.state,
            ConnectionState::Connected,
            "restoring ALPN_A must leave ALPN_B's Proxy Connected"
        );

        // The row on disk is the first of the write's files; Direct is effective once the write has
        // landed in rpc.1's held projection, which its own node.connection.get answers after.
        let held = wait_for_held_connections(&root, &rpc1_birth, BOUND).await;
        let a_tunnel = held_entry(&held, proxy_a_client.id())
            .unwrap_or_else(|| panic!("rpc.1 must still hold ALPN_A's open Proxy tunnel: {held:?}"));
        assert_eq!(
            a_tunnel.reason.as_deref(),
            Some(TUNNEL_PAST_ITS_ROUTE),
            "ALPN_A's Proxy is no longer rpc.1's route once Direct is restored: {held:?}"
        );
        let b_proxy = held_entry(&held, proxy_b_client.id())
            .unwrap_or_else(|| panic!("rpc.1 must hold ALPN_B's Proxy: {held:?}"));
        assert_eq!(
            (b_proxy.kind.as_str(), b_proxy.state.as_str(), b_proxy.reason.as_deref()),
            ("proxy", "connected", None),
            "restoring ALPN_A must leave ALPN_B's Proxy rpc.1's route: {held:?}"
        );
        assert!(
            held.connections.iter().any(|c| c.kind == "direct"
                && c.state == "connected"
                && c.destination == "mesh1.rpc.2"),
            "rpc.1 must hold its restored Direct to rpc.2: {held:?}"
        );

        // B's in-flight stream continues while A has restored Direct!
        b_send.write_all(b"-and-after").await.unwrap();
        b_send.finish().unwrap();
        assert_eq!(bs_recv.read_to_end(64).await.unwrap(), b"-and-after");
        bs_send.write_all(b"b-proxy-intact").await.unwrap();
        bs_send.finish().unwrap();
        assert_eq!(b_recv.read_to_end(64).await.unwrap(), b"b-proxy-intact");

        // Next open on A is Direct; next open on B is still Proxy (via gateway.1)!
        let (next_a_c, next_a_s) = open_pair(&rpc1_a, &mut rpc2_a, &rpc2_birth).await;
        assert_eq!(next_a_c.kind(), ConnectionKind::Direct);

        let (next_b_c, next_b_s) = open_pair(&rpc1_b, &mut rpc2_b, &rpc2_birth).await;
        assert_eq!(
            next_b_c.kind(),
            ConnectionKind::Proxy {
                via: gw1_birth.clone()
            }
        );

        drop(next_a_c);
        drop(next_a_s);
        drop(next_b_c);
        drop(next_b_s);
        drop(proxy_a_client);
        drop(proxy_a_server);
        drop(proxy_b_client);
        drop(proxy_b_server);
        rpc1.stop().await;
        rpc2.stop().await;
        gw1.stop().await;
        root.leave().await;
    });
}

/// Row 4 — `incomplete connection abstraction`:
/// Concurrent streams from both ends with distinct payloads: pairing, finish, reset and stop
/// codes hold through the carrier.
#[test]
fn concurrent_streams_from_both_ends_preserve_pairing_finish_reset_and_stop_codes_through_the_carrier() {
    let _serial = CELL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("demo-conn-row4-{}", NodeId::mint()));
    let capture =
        rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-conn-row4").unwrap();
    capture.run(4, async {
        let root_dir = dir.join("admin");
        let (root, fabric_root) = boot_admin_with_committed_store_root(&root_dir).await;
        let gw1 = spawn_member(
            &root,
            &dir.join("gw1"),
            &root_dir,
            "mesh1.gateway.1",
            vec![forwardable_gateway(ALPN_A)],
        )
        .await;
        let rpc1 = spawn_member(
            &root,
            &dir.join("rpc1"),
            &root_dir,
            "mesh1.rpc.1",
            vec![forwardable_gateway(ALPN_A)],
        )
        .await;
        let rpc2 = spawn_member(
            &root,
            &dir.join("rpc2"),
            &root_dir,
            "mesh1.rpc.2",
            vec![forwardable_gateway(ALPN_A)],
        )
        .await;
        wait_for_members(
            &[&gw1, &rpc1, &rpc2],
            &["mesh1.gateway.1", "mesh1.rpc.1", "mesh1.rpc.2"],
        )
        .await;

        let gw1_birth = birth_of(&gw1);
        let rpc2_birth = birth_of(&rpc2);
        let rpc2_name = rpc2_birth.name.clone();

        let rpc1_a = rpc1.connections(ALPN_A).unwrap();
        let mut gw1_a = gw1.connections(ALPN_A).unwrap();
        let mut rpc2_a = rpc2.connections(ALPN_A).unwrap();

        establish_direct_edge(&fabric_root, "mesh1.rpc.1", &rpc1_a, &mut gw1_a, &gw1_birth, ALPN_A)
            .await;
        establish_direct_edge(
            &fabric_root,
            "mesh1.gateway.1",
            &gw1_a,
            &mut rpc2_a,
            &rpc2_birth,
            ALPN_A,
        )
        .await;

        block_direct(&rpc1, &rpc2_name, ALPN_A);
        let (client, server) = open_pair(&rpc1_a, &mut rpc2_a, &rpc2_birth).await;
        assert_eq!(
            client.kind(),
            ConnectionKind::Proxy {
                via: gw1_birth.clone()
            }
        );

        // 1. Concurrent streams opened from both ends (2 from client, 2 from server) with distinct payloads:
        let c_open = async {
            let (mut s1, mut r1) = client.open_bi().await.unwrap();
            s1.write_all(b"c2s-stream-1").await.unwrap();
            s1.finish().unwrap();
            let (mut s2, mut r2) = client.open_bi().await.unwrap();
            s2.write_all(b"c2s-stream-2").await.unwrap();
            s2.finish().unwrap();
            let mut got = vec![
                r1.read_to_end(64).await.unwrap(),
                r2.read_to_end(64).await.unwrap(),
            ];
            got.sort();
            assert_eq!(
                got,
                vec![b"reply:c2s-stream-1".to_vec(), b"reply:c2s-stream-2".to_vec()]
            );
        };
        let s_accept = async {
            for _ in 0..2 {
                let (mut s, mut r) = server.accept_bi().await.unwrap();
                let payload = r.read_to_end(64).await.unwrap();
                let mut reply = b"reply:".to_vec();
                reply.extend_from_slice(&payload);
                s.write_all(&reply).await.unwrap();
                s.finish().unwrap();
            }
        };
        let s_open = async {
            let (mut s1, mut r1) = server.open_bi().await.unwrap();
            s1.write_all(b"s2c-stream-1").await.unwrap();
            s1.finish().unwrap();
            let (mut s2, mut r2) = server.open_bi().await.unwrap();
            s2.write_all(b"s2c-stream-2").await.unwrap();
            s2.finish().unwrap();
            let mut got = vec![
                r1.read_to_end(64).await.unwrap(),
                r2.read_to_end(64).await.unwrap(),
            ];
            got.sort();
            assert_eq!(
                got,
                vec![b"reply:s2c-stream-1".to_vec(), b"reply:s2c-stream-2".to_vec()]
            );
        };
        let c_accept = async {
            for _ in 0..2 {
                let (mut s, mut r) = client.accept_bi().await.unwrap();
                let payload = r.read_to_end(64).await.unwrap();
                let mut reply = b"reply:".to_vec();
                reply.extend_from_slice(&payload);
                s.write_all(&reply).await.unwrap();
                s.finish().unwrap();
            }
        };
        tokio::time::timeout(BOUND, async {
            tokio::join!(c_open, s_accept, s_open, c_accept);
        })
        .await
        .unwrap();

        // 2. Stream reset code propagation through the carrier:
        let (mut rs_send, _rs_recv) = client.open_bi().await.unwrap();
        rs_send.write_all(b"reset-me").await.unwrap();
        let (_srv_send, mut srv_recv) = server.accept_bi().await.unwrap();
        let mut head = [0u8; 8];
        srv_recv.read_exact(&mut head).await.unwrap();
        assert_eq!(&head, b"reset-me");
        rs_send.reset(77).unwrap();
        assert_eq!(
            srv_recv.read_to_end(64).await.unwrap_err(),
            StreamError::Reset { code: 77 },
            "SendStream::reset(code) must propagate its exact code through the carrier"
        );

        // 3. Stream stop code propagation through the carrier:
        let (mut st_send, _st_recv) = client.open_bi().await.unwrap();
        st_send.write_all(b"stop-me").await.unwrap();
        let (_srv_send2, mut srv_recv2) = server.accept_bi().await.unwrap();
        let mut head2 = [0u8; 7];
        srv_recv2.read_exact(&mut head2).await.unwrap();
        assert_eq!(&head2, b"stop-me");
        srv_recv2.stop(88).unwrap();
        let stop_err = tokio::time::timeout(BOUND, st_send.stopped())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(
            stop_err,
            StreamError::Stopped { code: 88 },
            "RecvStream::stop(code) must propagate its exact code through the carrier"
        );

        drop(client);
        drop(server);
        rpc1.stop().await;
        rpc2.stop().await;
        gw1.stop().await;
        root.leave().await;
    });
}

/// Row 5 — `deletion breaks lifecycle control`:
/// Restore Direct during a long Proxy stream: that stream continues, the next `open` is Direct,
/// `node.connection.delete` by the tunnel's id still closes it. Also captures and verifies the
/// three-node flame (`rpc.1` → `gateway.1` → `rpc.2`).
#[test]
fn direct_restoration_during_a_proxy_stream_leaves_the_stream_and_connections_delete_intact() {
    let _serial = CELL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("demo-conn-row5-{}", NodeId::mint()));
    let capture =
        rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-conn-row5").unwrap();
    capture.run(4, async {
        let root_dir = dir.join("admin");
        let (root, fabric_root) = boot_admin_with_committed_store_root(&root_dir).await;
        let gw1 = spawn_member(
            &root,
            &dir.join("gw1"),
            &root_dir,
            "mesh1.gateway.1",
            vec![forwardable_gateway(ALPN_A)],
        )
        .await;
        let rpc1 = spawn_member(
            &root,
            &dir.join("rpc1"),
            &root_dir,
            "mesh1.rpc.1",
            vec![forwardable_gateway(ALPN_A)],
        )
        .await;
        let rpc2 = spawn_member(
            &root,
            &dir.join("rpc2"),
            &root_dir,
            "mesh1.rpc.2",
            vec![forwardable_gateway(ALPN_A)],
        )
        .await;
        wait_for_members(
            &[&gw1, &rpc1, &rpc2],
            &["mesh1.gateway.1", "mesh1.rpc.1", "mesh1.rpc.2"],
        )
        .await;

        let gw1_birth = birth_of(&gw1);
        let rpc1_birth = birth_of(&rpc1);
        let rpc2_birth = birth_of(&rpc2);
        let rpc2_name = rpc2_birth.name.clone();

        let mut rpc1_a = rpc1.connections(ALPN_A).unwrap();
        let mut gw1_a = gw1.connections(ALPN_A).unwrap();
        let mut rpc2_a = rpc2.connections(ALPN_A).unwrap();

        let flow = tracing::info_span!(
            "rdm.demo.connection.proxy_to_direct",
            source = "mesh1.rpc.1",
            carrier = "mesh1.gateway.1",
            destination = "mesh1.rpc.2"
        );

        establish_direct_edge(&fabric_root, "mesh1.rpc.1", &rpc1_a, &mut gw1_a, &gw1_birth, ALPN_A)
            .await;
        establish_direct_edge(
            &fabric_root,
            "mesh1.gateway.1",
            &gw1_a,
            &mut rpc2_a,
            &rpc2_birth,
            ALPN_A,
        )
        .await;

        block_direct(&rpc1, &rpc2_name, ALPN_A);
        let proxy_client =
            tracing::Instrument::instrument(rpc1_a.open(&rpc2_birth), flow.clone())
                .await
                .unwrap();
        let proxy_server = tokio::time::timeout(BOUND, rpc2_a.accept()).await.unwrap().unwrap();
        let tunnel_id = proxy_client.id().clone();
        assert_eq!(
            proxy_client.kind(),
            ConnectionKind::Proxy {
                via: gw1_birth.clone()
            }
        );
        let proxy_file =
            proxy_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", ALPN_A);
        wait_for_row(&proxy_file, ConnectionState::Connected).await;

        // Open a long-running stream on the Proxy before Direct is restored.
        let (mut long_send, mut long_recv) = proxy_client.open_bi().await.unwrap();
        long_send
            .write_all(b"chunk-before-direct-restored:")
            .await
            .unwrap();
        let (mut srv_long_send, mut srv_long_recv) = proxy_server.accept_bi().await.unwrap();
        let mut first_part = [0u8; 29];
        srv_long_recv.read_exact(&mut first_part).await.unwrap();
        assert_eq!(&first_part, b"chunk-before-direct-restored:");

        // Restore Direct by unblocking rpc.1 -> rpc.2 and having rpc.2 dial rpc.1 (inbound recovery trigger).
        unblock_direct(&rpc1, &rpc2_name, ALPN_A);
        let in_c =
            tracing::Instrument::instrument(rpc2_a.open(&rpc1_birth), flow.clone())
                .await
                .unwrap();
        let in_s = tokio::time::timeout(BOUND, rpc1_a.accept()).await.unwrap().unwrap();
        drop(in_c);
        drop(in_s);

        // Drain the reconnect probe connection accepted by rpc.2 on ALPN_A, and wait for the Proxy
        // row to become Disconnected(direct-restored) on disk.
        let probe_on_a = tokio::time::timeout(BOUND, rpc2_a.accept())
            .await
            .expect("rpc.1's reconnect scheduler must probe rpc.2 on ALPN_A")
            .unwrap();
        drop(probe_on_a);

        let deleted_row = wait_for_row(&proxy_file, ConnectionState::Disconnected).await;
        assert_eq!(deleted_row.reason.as_deref(), Some("direct-restored"));
        wait_for_row(
            &direct_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", ALPN_A),
            ConnectionState::Connected,
        )
        .await;

        // The row on disk is the first of the write's files; Direct is effective once the write has
        // landed in rpc.1's held projection, which its own node.connection.get answers after.
        let held = wait_for_held_connections(&root, &rpc1_birth, BOUND).await;
        let tunnel = held_entry(&held, &tunnel_id)
            .unwrap_or_else(|| panic!("rpc.1 must still hold the open Proxy tunnel: {held:?}"));
        assert_eq!(
            tunnel.reason.as_deref(),
            Some(TUNNEL_PAST_ITS_ROUTE),
            "the Proxy is no longer rpc.1's route once Direct is restored: {held:?}"
        );
        assert!(
            !held.connections.iter().any(|c| c.kind == "proxy" && c.reason.is_none()),
            "rpc.1 holds no Proxy route to rpc.2 once Direct is restored: {held:?}"
        );
        assert!(
            held.connections.iter().any(|c| c.kind == "direct"
                && c.state == "connected"
                && c.destination == "mesh1.rpc.2"),
            "rpc.1 must hold its restored Direct to rpc.2: {held:?}"
        );

        // 1. The long-running Proxy stream continues across Direct restoration!
        long_send
            .write_all(b"chunk-after-direct-restored")
            .await
            .unwrap();
        srv_long_send
            .write_all(b"ack-from-long-proxy-stream")
            .await
            .unwrap();
        let mut second_part = [0u8; 27];
        srv_long_recv.read_exact(&mut second_part).await.unwrap();
        assert_eq!(&second_part, b"chunk-after-direct-restored");
        let mut ack_part = [0u8; 26];
        long_recv.read_exact(&mut ack_part).await.unwrap();
        assert_eq!(&ack_part, b"ack-from-long-proxy-stream");

        // 2. The next open() goes Direct!
        let direct_client =
            tracing::Instrument::instrument(rpc1_a.open(&rpc2_birth), flow.clone())
                .await
                .unwrap();
        let direct_server = tokio::time::timeout(BOUND, rpc2_a.accept()).await.unwrap().unwrap();
        assert_eq!(direct_client.kind(), ConnectionKind::Direct);

        // 3. `node.connection.delete` by the tunnel's ConnectionId still closes the live tunnel and its stream!
        let closed = delete_connections_command(
            &root,
            &rpc1_birth,
            rdm_core::status::ConnectionScope::Connections(vec![tunnel_id.clone()]),
        )
        .await;
        assert!(
            closed.contains(&tunnel_id),
            "DeleteConnections by tunnel_id must close the still-live Proxy connection, closed={closed:?}"
        );
        let closed_reason = tokio::time::timeout(BOUND, proxy_client.closed())
            .await
            .expect("the tunnel must close when deleted by its ConnectionId");
        assert_eq!(closed_reason, Closed::ConnectionsDeleted);

        // And the new Direct connection is untouched by deleting the tunnel's ConnectionId!
        let (mut dc_send, mut dc_recv) = direct_client.open_bi().await.unwrap();
        dc_send.write_all(b"direct-still-alive").await.unwrap();
        dc_send.finish().unwrap();
        let (mut ds_send, mut ds_recv) = direct_server.accept_bi().await.unwrap();
        assert_eq!(
            ds_recv.read_to_end(64).await.unwrap(),
            b"direct-still-alive"
        );
        ds_send.write_all(b"ack:direct-still-alive").await.unwrap();
        ds_send.finish().unwrap();
        assert_eq!(
            dc_recv.read_to_end(64).await.unwrap(),
            b"ack:direct-still-alive"
        );

        drop(direct_client);
        drop(direct_server);
        drop(proxy_client);
        drop(proxy_server);
        rpc1.stop().await;
        rpc2.stop().await;
        gw1.stop().await;
        root.leave().await;

        drop(flow);
        let spans = capture.spans().unwrap();
        if let Ok(dump) = std::env::var("RDM_DEMO_SPANS_OUT") {
            std::fs::write(dump, serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
        }
        assert!(
            spans
                .iter()
                .any(|s| s["name"] == "rdm.node.connection.create.via-tunnel"
                    && s["attributes"]["source"] == "mesh1.rpc.1"
                    && s["attributes"]["carrier"] == "mesh1.gateway.1"
                    && s["attributes"]["destination"] == "mesh1.rpc.2"),
            "three-node trace must include source/carrier via-tunnel spans"
        );
        assert!(
            spans
                .iter()
                // Each acceptor admits on the dialer's cert chain (node-connections-admit.md: via-admitted
                // names the cert's expiry), never by name.
                .filter(|s| s["name"] == "rdm.node.connection.serve.via-admitted" && {
                    let e = &s["attributes"]["cert_expires_at"];
                    e.as_u64().or_else(|| e.as_str().and_then(|e| e.parse().ok())).is_some_and(|e| e > 0)
                })
                .count()
                >= 3,
            "three-node trace must include the three cert admission spans across source, carrier, and destination"
        );
        assert!(
            !spans.iter().any(|s| s["name"] == "rdm.node.connection.serve.via-no-certs-configured"),
            "a fabric with certs admits no connection by name"
        );
    });
}

/// Row 6 — `restart resets reconnect` (connections.md §9.3, §13 C, Q227):
/// Restart the source before its persisted `next_due`: the restarted birth reads no row back and
/// holds no reconnect series, so it makes no attempt before that due time or after it; its next
/// failed dial is a new series at attempt 1 on the ALPN it dialled, and once Direct is reachable its
/// own reconciler dials that ALPN once.
#[test]
fn source_restart_before_persisted_next_due_holds_no_series_and_its_next_failure_starts_at_attempt_one_on_the_right_alpn() {
    let _serial = CELL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("demo-conn-row6-{}", NodeId::mint()));
    let capture =
        rdm_transport::EvidenceCapture::start(&dir.join("evidence"), "demo-conn-row6").unwrap();
    capture.run(4, async {
        let root_dir = dir.join("admin");
        let (root, fabric_root) = boot_admin_with_committed_store_root(&root_dir).await;
        let gw1 = spawn_member(
            &root,
            &dir.join("gw1"),
            &root_dir,
            "mesh1.gateway.1",
            vec![forwardable_gateway(ALPN_A), forwardable_gateway(ALPN_B)],
        )
        .await;
        let rpc1_dir = dir.join("rpc1");
        let rpc1 = spawn_member(
            &root,
            &rpc1_dir,
            &root_dir,
            "mesh1.rpc.1",
            vec![forwardable_gateway(ALPN_A), forwardable_gateway(ALPN_B)],
        )
        .await;
        let rpc2 = spawn_member(
            &root,
            &dir.join("rpc2"),
            &root_dir,
            "mesh1.rpc.2",
            vec![forwardable_gateway(ALPN_A), forwardable_gateway(ALPN_B)],
        )
        .await;
        wait_for_members(
            &[&gw1, &rpc1, &rpc2],
            &["mesh1.gateway.1", "mesh1.rpc.1", "mesh1.rpc.2"],
        )
        .await;

        let gw1_birth = birth_of(&gw1);
        let rpc1_birth = birth_of(&rpc1);
        let rpc2_birth = birth_of(&rpc2);
        let rpc2_name = rpc2_birth.name.clone();

        let rpc1_a = rpc1.connections(ALPN_A).unwrap();
        let rpc1_b = rpc1.connections(ALPN_B).unwrap();
        let mut gw1_a = gw1.connections(ALPN_A).unwrap();
        let mut gw1_b = gw1.connections(ALPN_B).unwrap();
        let _rpc2_a = rpc2.connections(ALPN_A).unwrap();
        let mut rpc2_b = rpc2.connections(ALPN_B).unwrap();

        establish_direct_edge(&fabric_root, "mesh1.rpc.1", &rpc1_a, &mut gw1_a, &gw1_birth, ALPN_A)
            .await;
        establish_direct_edge(&fabric_root, "mesh1.rpc.1", &rpc1_b, &mut gw1_b, &gw1_birth, ALPN_B)
            .await;
        establish_direct_edge(
            &fabric_root,
            "mesh1.gateway.1",
            &gw1_b,
            &mut rpc2_b,
            &rpc2_birth,
            ALPN_B,
        )
        .await;

        // Block Direct on ALPN_B from rpc.1 -> rpc.2, open Proxy on ALPN_B (recording Direct
        // Failed ordinal 1 + Proxy Connected), and wait for rpc.1's reconnect scheduler (while
        // Direct remains blocked on rpc.1) to advance the persisted Direct Failed row to
        // ordinal 3 (~4 s backoff) so restarting before `next_due` has a wide deterministic window!
        block_direct(&rpc1, &rpc2_name, ALPN_B);
        let (proxy_b_client, proxy_b_server) = open_pair(&rpc1_b, &mut rpc2_b, &rpc2_birth).await;
        assert_eq!(
            proxy_b_client.kind(),
            ConnectionKind::Proxy {
                via: gw1_birth.clone()
            }
        );

        let direct_b_file = direct_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", ALPN_B);
        let failed_row = tokio::time::timeout(BOUND, async {
            loop {
                if let Some(row) = read_row(&direct_b_file) {
                    if row.state == ConnectionState::Failed
                        && row.recovery.map(|r| r.attempt_ordinal) == Some(3)
                    {
                        break row;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Direct Failed row at ordinal 3 must be persisted on the shared store root");
        assert_eq!(failed_row.state, ConnectionState::Failed);
        assert_eq!(
            failed_row.alpn, ALPN_B,
            "the persisted Direct Failed row must name ALPN_B"
        );
        let recovery = failed_row.recovery.expect("Direct Failed row carries recovery");
        assert_eq!(recovery.attempt_ordinal, 3);
        let due_ms = next_due(&failed_row).expect("Direct Failed row has next_due");
        assert!(
            due_ms >= failed_row.logged_at_ms + 4_000,
            "ordinal 3 backoff places next_due at least 4 s after logged_at_ms"
        );

        // Stop rpc.1 (`DeleteConnections { All }` + `stop_commanded`) so its Direct Failed row
        // remains on disk across the restart while its old registry is disarmed.
        let prev_birth = (
            rpc1.digest.node.node_id.clone(),
            rpc1.digest.node.incarnation.clone(),
        );
        let _ = delete_connections_command(
            &root,
            &rpc1_birth,
            rdm_core::status::ConnectionScope::All,
        )
        .await;
        drop(proxy_b_client);
        drop(proxy_b_server);
        drop(rpc1_a);
        drop(rpc1_b);
        rpc1.stop_commanded().await;

        let history_before_restart = read_history(&fabric_root, "mesh1.rpc.1");
        let direct_events_before = history_before_restart
            .iter()
            .filter(|r| r.destination.name == rpc2_name && r.kind == CoreKind::Direct)
            .count();

        // Restart rpc.1 before its persisted `next_due`.
        let rpc1_restarted = spawn_member_with_time(
            &root,
            &rpc1_dir,
            &root_dir,
            "mesh1.rpc.1",
            vec![forwardable_gateway(ALPN_A), forwardable_gateway(ALPN_B)],
            rdm_transport::clock::RafkaTime::unadopted(),
            Some(prev_birth),
        )
        .await;
        wait_for_members(
            &[&gw1, &rpc1_restarted, &rpc2],
            &["mesh1.gateway.1", "mesh1.rpc.1", "mesh1.rpc.2"],
        )
        .await;
        let _rpc1_a2 = rpc1_restarted.connections(ALPN_A).unwrap();
        let rpc1_b2 = rpc1_restarted.connections(ALPN_B).unwrap();
        let restarted_birth = birth_of(&rpc1_restarted);

        let now_after_boot = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(
            now_after_boot + 400 < due_ms,
            "restart completed well before persisted next_due (now={now_after_boot}, due={due_ms})"
        );

        // The restarted birth holds no reconnect series (§9.3, §13 C): nothing dials rpc.2 on ALPN_B
        // before the persisted `next_due`, nor through it and two reconciler ticks (500 ms) past it.
        let quiet = Duration::from_millis(due_ms - now_after_boot + 1_000);
        assert!(
            tokio::time::timeout(quiet, rpc2_b.accept()).await.is_err(),
            "the restarted source must make no attempt from the first birth's Direct Failed row, before or after its next_due"
        );
        let direct_events_past_due = read_history(&fabric_root, "mesh1.rpc.1")
            .iter()
            .filter(|r| r.destination.name == rpc2_name && r.kind == CoreKind::Direct)
            .count();
        assert_eq!(
            direct_events_past_due, direct_events_before,
            "no Direct row toward rpc.2 is written from the first birth's series after the restart"
        );

        // Its next failed dial on ALPN_B starts a new series at attempt 1, naming the restarted birth,
        // and its carrier is proved again (§13 B).
        block_direct(&rpc1_restarted, &rpc2_name, ALPN_B);
        let (proxy_again, proxy_again_server) = open_pair(&rpc1_b2, &mut rpc2_b, &rpc2_birth).await;
        assert_eq!(
            proxy_again.kind(),
            ConnectionKind::Proxy {
                via: gw1_birth.clone()
            }
        );
        let new_series = tokio::time::timeout(BOUND, async {
            loop {
                if let Some(row) = read_row(&direct_b_file) {
                    if row.state == ConnectionState::Failed
                        && row.source.incarnation.as_ref() == Some(&restarted_birth.incarnation)
                    {
                        break row;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the restarted birth's failed dial on ALPN_B writes its own Direct Failed row");
        assert_eq!(new_series.alpn, ALPN_B, "the new series is on ALPN_B, the ALPN it dialled");
        assert_eq!(
            new_series.recovery.map(|r| r.attempt_ordinal),
            Some(1),
            "the restarted birth's first failure is attempt 1 of a new series, never the first birth's ordinal 4"
        );
        assert!(
            read_row(&direct_index_path(&fabric_root, "mesh1.rpc.1", "mesh1.rpc.2", ALPN_A)).is_none(),
            "a reconnect on ALPN_B writes nothing on ALPN_A (§9.4)"
        );

        // Direct reachable again: the restarted birth's own reconciler dials ALPN_B once at its due.
        unblock_direct(&rpc1_restarted, &rpc2_name, ALPN_B);
        let accepted_on_b = tokio::time::timeout(BOUND, rpc2_b.accept())
            .await
            .expect("the restarted birth's reconciler dials ALPN_B once its own series falls due")
            .unwrap();
        assert_eq!(accepted_on_b.kind(), ConnectionKind::Direct);
        let direct_after = wait_for_row(&direct_b_file, ConnectionState::Connected).await;
        assert_eq!(direct_after.alpn, ALPN_B);
        assert_eq!(
            direct_after.source.incarnation.as_ref(),
            Some(&restarted_birth.incarnation),
            "the Direct Connected row names the restarted birth"
        );

        // No duplicate attempt: one Failed (attempt 1) and one Connected from the restarted birth.
        assert!(
            tokio::time::timeout(Duration::from_millis(1_000), rpc2_b.accept()).await.is_err(),
            "the reconciler dials once, with no duplicate attempt"
        );
        let restarted_directs: Vec<ConnectionState> = read_history(&fabric_root, "mesh1.rpc.1")
            .into_iter()
            .filter(|r| {
                r.destination.name == rpc2_name
                    && r.kind == CoreKind::Direct
                    && r.source.incarnation.as_ref() == Some(&restarted_birth.incarnation)
            })
            .map(|r| r.state)
            .collect();
        assert_eq!(
            restarted_directs,
            vec![ConnectionState::Failed, ConnectionState::Connected],
            "the restarted birth's Direct history toward rpc.2 is its one failure and its one reconnect"
        );

        drop(accepted_on_b);
        drop(proxy_again);
        drop(proxy_again_server);
        rpc1_restarted.stop().await;
        rpc2.stop().await;
        gw1.stop().await;
        root.leave().await;
    });
}
