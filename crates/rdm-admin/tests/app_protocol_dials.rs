//! Why missed: no cell paired a birth that made no JoinNode, as the DIALER, with an acceptor that
//! judges certs. `admin_app_alpns.rs` has the Day-0 admin only accept, under `Wiring::no_certs()`, so
//! its member dialer passes on the bytes `b"cert-v1"` by name; `member_cert.rs` dials between two
//! launched rpc nodes, each holding its join's cert; `rdm-test`'s `member_cert_admin.rs` asserted the
//! Day-0 root's `member_cert == None` as the contract (the gap held as a passing cell); and no admin
//! executable (`rdm-node-admin`, `faulted-node-admin`, the demo's `rshape-node-admin`, each
//! `rdm_admin::entry::run_on`) can take its app protocols' `Connections`, so no estate ever had an admin
//! open an app-protocol connection. The first dial a Day-0 admin made on a registered app ALPN was
//! Rafka's (Rafka 141: `rdm.node.connection.reject.via-refused{source=mesh1.admin.1 cert_expires_at=0
//! reason=no-credential}`).
//!
//! The matrix (fabric-certs.md §When a cert is issued, §On a connection; node-connections-admit.md):
//! for each birth kind a node-admin can hold in process, and for an ordinary node, the birth holds a
//! member cert that verifies member → issuer → CA → root, a dial it makes on a registered app ALPN to a
//! node admitting on the fabric's root is admitted, and that node's dial to it is admitted. Every
//! acceptor here judges certs on the root (`CertAdmitter`), never by name. The negative rows: a dialer
//! presenting no cert is refused `no-credential`, one presenting an expired cert is refused `expired`.

use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::certs::CertChoice;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::certs::{self, BirthIdentity, CaKey, IssuingKey, SigningKey, VerifyingKey};
use rdm_core::digest::MeshNode;
use rdm_core::{EndpointId, IncarnationId, NodeId, NodeKind};
use rdm_transport::clock::RafkaTime;
use rdm_transport::connections::{Acceptor, Alpn, Birth, ConnectError, ConnectionsRegistry, Dialer, MemoryConnectionsSink};
use rdm_transport::{LiveNodeResolver, ResolvedNode, StaticResolver};
use std::sync::Arc;
use std::time::Duration;

const APP: &[u8] = b"app-protocol-dials/1";
const BOUND: Duration = Duration::from_secs(60);

/// The fabric's root every row's admins hold and every acceptor admits on.
fn root() -> SigningKey {
    SigningKey::from_bytes(&[0x3c; 32])
}

fn root_public() -> VerifyingKey {
    root().verifying_key()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

fn app_alpn() -> Alpn {
    Alpn { name: APP.to_vec(), proxy: false, carriers: vec![NodeKind::RpcNode] }
}

fn temp_dir(what: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("{what}-{}", NodeId::mint()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// The Day-0 root: the fabric's first node-admin, holding the root, with the app protocol registered.
async fn day0_root(dir: &std::path::Path) -> Running {
    let running = start_with(day0_config(dir), Wiring { alpns: vec![app_alpn()], certs: CertChoice::Ca { secret: root(), issuing: None }, fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() })
        .await
        .expect("the Day-0 admin comes up");
    running.stop_reconciling().await;
    running
}

/// A launch of `name` by `launcher` into its mesh, admitting on the fabric's root (`RDM_ROOTS`), its
/// deployment expected by the launcher's join door.
fn launched_by(launcher: &Running, name: &str, launcher_dir: &std::path::Path) -> (rdm_core::launch::Launch, std::path::PathBuf) {
    let d = launcher.digest.lock().unwrap().clone();
    let dir = temp_dir("app-dials-launched");
    let runtime = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("app-dials")).unwrap();
    runtime.write_record(&dir).unwrap();
    let key = rdm_node::node::load_or_mint_key(&dir).unwrap();
    let launch = rdm_core::launch::Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: if name.contains(".admin.") { vec![("control".into(), "127.0.0.1:0".parse().unwrap())] } else { vec![] },
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(rdm_core::launch::Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.clone(),
        store_dir: launcher_dir.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: vec![root_public()],
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    };
    let _ = launcher.runner.joins.expect(rdm_admin::join::Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: EndpointId(key.public().to_string()),
        runtime,
        data_dir: dir.display().to_string(),
        last_accepted_name: None,
        held: false,
        listeners: Vec::new(),
    });
    (launch, dir)
}

/// One end of the matrix: a birth with the app protocol registered on its own registry.
struct End {
    birth: MeshNode,
    resolver: Arc<LiveNodeResolver>,
    dialer: Dialer,
    acceptor: Acceptor,
    member_cert: Option<Vec<u8>>,
}

impl End {
    fn of_admin(r: &Running) -> End {
        let (dialer, acceptor) = r.connections(APP).expect("the app takes its protocol's Connections from the admin").split();
        End { birth: r.birth(), resolver: r.node_rpc.resolver.clone(), dialer, acceptor, member_cert: r.member_cert.clone() }
    }

    fn of_node(n: &rdm_node::node::RunningNode) -> End {
        let (dialer, acceptor) = n.connections(APP).expect("the node's app takes its protocol's Connections").split();
        End { birth: n.digest.node.clone(), resolver: n.node_rpc.resolver.clone(), dialer, acceptor, member_cert: Some(n.member_cert.clone()) }
    }
}

fn conn_birth(b: &MeshNode) -> Birth {
    Birth { name: b.name.clone(), node_id: b.node_id.clone(), incarnation: b.incarnation.clone() }
}

fn resolved(b: &MeshNode) -> ResolvedNode {
    ResolvedNode { node_id: b.node_id.clone(), name: b.name.clone(), endpoint_id: b.endpoint_id.0.parse().unwrap(), transport_addr: b.transport_addr, incarnation: b.incarnation.clone() }
}

/// A node of `mesh1` serving the app protocol and admitting on the fabric's root, presenting a member
/// cert under that root whose expiry is `expiry_ms` (`None`: it presents nothing).
struct Peer {
    birth: MeshNode,
    registry: ConnectionsRegistry,
    acceptor: Acceptor,
    _router: iroh::protocol::Router,
}

async fn peer(name: &str, expiry_ms: Option<u64>, admin: &MeshNode) -> Peer {
    let key = iroh::SecretKey::generate();
    let ep = rdm_transport::endpoint::bind_with_alpns(key.clone(), "127.0.0.1:0".parse().unwrap(), &[APP.to_vec()]).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("the peer binds an IPv4 socket");
    let birth = MeshNode {
        node_id: NodeId::mint(),
        name: name.parse().unwrap(),
        endpoint_id: EndpointId(key.public().to_string()),
        transport_addr: addr,
        incarnation: IncarnationId::mint(),
        supersedes: None,
        runtime: None,
    };
    let cert = match expiry_ms {
        None => Vec::new(),
        Some(expiry) => {
            let now = now_ms();
            let ca = CaKey::mint(&root(), certs::CA_CERT_TTL_SECS, now);
            let issuing = IssuingKey::mint(&ca, "mesh1", now, now + certs::ISSUING_CERT_TTL_SECS * 1000).expect("within the CA's life");
            let identity = BirthIdentity { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), name: birth.name.clone(), mesh: "mesh1".into(), endpoint_key: birth.endpoint_id.clone() };
            certs::cert_to_bytes(&certs::issue(&issuing, &identity, now, expiry).expect("a member cert under the root"))
        }
    };
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(resolved(admin));
    let sink = Arc::new(MemoryConnectionsSink::new(conn_birth(&birth)));
    let (registry, handlers) = ConnectionsRegistry::new(ep.clone(), resolver, sink, &[app_alpn()], cert).unwrap();
    registry.admit_on(vec![root_public()], RafkaTime::unadopted());
    let mut rb = iroh::protocol::Router::builder(ep);
    for (alpn, handler) in handlers {
        rb = rb.accept(alpn, handler);
    }
    let (_, acceptor) = registry.connections(APP).unwrap().split();
    Peer { birth, registry, acceptor, _router: rb.spawn() }
}

/// The birth holds a member cert for exactly itself whose chain verifies member → issuer → CA → root.
fn assert_holds_chained_cert(end: &End) {
    let who = end.birth.name.to_string();
    let bytes = end.member_cert.clone().unwrap_or_default();
    let signed = certs::cert_from_bytes(&bytes).unwrap_or_else(|_| panic!("{who} holds no member cert ({} bytes): every birth holds one (fabric-certs.md §When a cert is issued)", bytes.len()));
    let c = &signed.cert;
    assert_eq!(
        (c.node_id.to_string(), c.incarnation.as_str(), c.name.to_string(), c.endpoint.0.as_str()),
        (end.birth.node_id.to_string(), end.birth.incarnation.as_str(), who.clone(), end.birth.endpoint_id.0.as_str()),
        "{who}'s member cert is issued for exactly its birth"
    );
    assert_eq!(certs::verify(&signed, &[root_public()], now_ms(), 0), Ok(()), "{who}'s member cert verifies to the fabric's root");
}

/// `from` dials `to` on the app protocol and is admitted: the connection reaches `to`'s app.
async fn assert_dial_admitted(from: &Dialer, from_name: &str, to: &MeshNode, to_acceptor: &mut Acceptor) {
    let accepted = async { tokio::time::timeout(BOUND, to_acceptor.accept()).await };
    let opened = async { tokio::time::timeout(BOUND, from.open(&conn_birth(to))).await };
    let (opened, accepted) = tokio::join!(opened, accepted);
    let conn = match opened {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => panic!("{from_name}'s dial to {} on an app protocol is refused: {e:?}", to.name),
        Err(_) => panic!("{from_name}'s dial to {} on an app protocol never ended", to.name),
    };
    let reached = accepted.ok().flatten().unwrap_or_else(|| panic!("{from_name}'s admitted connection never reached {}'s app", to.name));
    assert_eq!(reached.id(), conn.id(), "{}'s app accepts {from_name}'s admitted connection", to.name);
}

/// The two directions between `end` and a peer admitting on the root.
async fn assert_both_directions_with_a_peer(mut end: End) {
    let p = peer("mesh1.rpc.9", Some(now_ms() + 3_600_000), &end.birth).await;
    end.resolver.apply(resolved(&p.birth), None);
    let mut p_acceptor = p.acceptor;
    let who = end.birth.name.to_string();
    assert_dial_admitted(&end.dialer, &who, &p.birth, &mut p_acceptor).await;
    match tokio::time::timeout(BOUND, p.registry.probe_direct(&conn_birth(&end.birth), APP)).await {
        Ok(Ok(_)) => {}
        other => panic!("a peer's dial to {who} on an app protocol is refused: {other:?}"),
    }
    let _ = tokio::time::timeout(BOUND, end.acceptor.accept()).await.expect("the peer's admitted connection reaches the app").expect("the acceptor is open");
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}

/// CONTRACT (fabric-certs.md §When a cert is issued: "a new mesh's first node-admin is deployed | the
/// maker, which holds the root or an issuing cert that may mint for that mesh"; §On a connection;
/// node-connections-admit.md): the Day-0 root, which made no JoinNode, holds a member cert for exactly
/// its birth chained to the root; its dial on a registered app ALPN to a node admitting on the root is
/// admitted, and that node's dial to it is admitted. Must NOT happen: the Day-0 root refused
/// `no-credential` on its own dial (Rafka 141).
#[test]
fn a_day0_root_holds_a_chained_member_cert_and_its_app_protocol_dials_are_admitted_both_ways() {
    rt().block_on(async {
        let dir = temp_dir("app-dials-day0");
        let root = day0_root(&dir).await;
        let end = End::of_admin(&root);
        assert_holds_chained_cert(&end);
        assert_both_directions_with_a_peer(end).await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// CONTRACT (fabric-certs.md §When a cert is issued: "a node's JoinNode is accepted | the accepting
/// primary: the maker for a node-admin"): an admin launched by the Day-0 root holds the member cert its
/// join was issued, chained to the root; its app-protocol dials are admitted both ways.
#[test]
fn a_launched_admin_holds_its_joins_member_cert_and_its_app_protocol_dials_are_admitted_both_ways() {
    rt().block_on(async {
        let dir = temp_dir("app-dials-launched-admin");
        let root = day0_root(&dir).await;
        let (launch, admin_dir) = launched_by(&root, "mesh1.admin.2", &dir);
        let env = launch.to_env();
        let admin = start_with(
            AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment"),
            Wiring { alpns: vec![app_alpn()], certs: CertChoice::NoCerts, fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() },
        )
        .await
        .expect("the launched admin comes up");
        admin.stop_reconciling().await;
        let end = End::of_admin(&admin);
        assert_holds_chained_cert(&end);
        assert_both_directions_with_a_peer(end).await;
        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&admin_dir);
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// CONTRACT (fabric-certs.md §When a cert is issued: "the mesh-primary for an ordinary node"): an
/// ordinary node the Day-0 root launched holds its join's member cert, chained to the root; its
/// app-protocol dials are admitted both ways, and so are the dials between it and the Day-0 root.
#[test]
fn an_ordinary_node_holds_its_joins_member_cert_and_its_app_protocol_dials_are_admitted_both_ways() {
    rt().block_on(async {
        let dir = temp_dir("app-dials-node");
        let root = day0_root(&dir).await;
        let (launch, node_dir) = launched_by(&root, "mesh1.rpc.1", &dir);
        let node = rdm_node::node::start_with_rafka_time_pre_node_ready_alpns_and(&launch, RafkaTime::unadopted(), rdm_node::node_hooks::NodeHooks::none(), |s| s, None, |b, _| b, &[], &[app_alpn()])
            .await
            .expect("the node starts");
        let end = End::of_node(&node);
        assert_holds_chained_cert(&end);
        assert_both_directions_with_a_peer(end).await;
        node.stop().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&node_dir);
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// CONTRACT (fabric-certs.md §On a connection; node-connections-admit.md, Failure and receipts): the
/// Day-0 root and a node it launched dial each other on a registered app ALPN, each admitted on the
/// other's root: the Day-0 admin to an rpc node of its own mesh, and back.
#[test]
fn the_day0_root_and_an_ordinary_node_it_launched_dial_each_other_on_an_app_protocol() {
    rt().block_on(async {
        let dir = temp_dir("app-dials-day0-node");
        let root = day0_root(&dir).await;
        let (launch, node_dir) = launched_by(&root, "mesh1.rpc.1", &dir);
        let node = rdm_node::node::start_with_rafka_time_pre_node_ready_alpns_and(&launch, RafkaTime::unadopted(), rdm_node::node_hooks::NodeHooks::none(), |s| s, None, |b, _| b, &[], &[app_alpn()])
            .await
            .expect("the node starts");
        let mut a = End::of_admin(&root);
        let mut n = End::of_node(&node);
        a.resolver.apply(resolved(&n.birth), None);
        n.resolver.apply(resolved(&a.birth), None);
        assert_dial_admitted(&a.dialer, "mesh1.admin.1", &n.birth, &mut n.acceptor).await;
        assert_dial_admitted(&n.dialer, "mesh1.rpc.1", &a.birth, &mut a.acceptor).await;
        node.stop().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&node_dir);
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// CONTRACT (node-connections-admit.md, Failure and receipts: a dialer that presents no cert is
/// refused `no-credential`; Verification: an expired cert is refused by the chain's own check,
/// `expired`): a node-admin's app protocol refuses, by name, a dialer holding no cert and a dialer whose
/// cert expired more than a staleness floor ago; neither connection reaches the app.
#[test]
fn an_admins_app_protocol_refuses_a_dialer_holding_no_cert_or_an_expired_one_by_name() {
    rt().block_on(async {
        let dir = temp_dir("app-dials-refused");
        let root = day0_root(&dir).await;
        let mut end = End::of_admin(&root);
        let skew = rdm_core::cadence::staleness_floor().as_millis() as u64;
        for (name, expiry, reason) in [("mesh1.rpc.7", None, "no-credential"), ("mesh1.rpc.8", Some(now_ms() - 3 * skew), "expired")] {
            let p = peer(name, expiry, &end.birth).await;
            match tokio::time::timeout(BOUND, p.registry.probe_direct(&conn_birth(&end.birth), APP)).await {
                Ok(Err(ConnectError::Refused { reason: got })) => assert_eq!(got, reason, "{name} is refused by name"),
                other => panic!("{name} ({reason}) must be refused by name: {other:?}"),
            }
            assert!(tokio::time::timeout(Duration::from_millis(300), end.acceptor.accept()).await.is_err(), "{name}'s refused connection never reaches the app");
        }
        root.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
}
