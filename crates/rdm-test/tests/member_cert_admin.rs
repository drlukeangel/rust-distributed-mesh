//! Fabric certs on node-admins (i143.e12.s22): a node-admin is started with an explicit cert
//! choice; the Day-0 root makes no JoinNode and, its mesh's first node-admin and its own maker,
//! holds the member cert its own issuer issued for exactly its birth, which a node admitting on the
//! fabric's root admits on an app ALPN; and an admin launched by another admin holds the member cert
//! that admin issued for exactly its birth, verifying to the root, in its pre_node_ready context and
//! on its running handle.

use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, NodeId};
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::node_hooks::{NodeHooks, PreNodeReadyState};
use rdm_admin::certs::CertChoice;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use rdm_core::certs;
use rdm_test::test_certs::{root_choice, test_root_public};
use rdm_transport::connections::{Alpn, Birth, ConnectionsRegistry, MemoryConnectionsSink};
use rdm_transport::{ResolvedNode, StaticResolver};
use std::sync::Arc;
use std::time::Duration;

const WRONG: Duration = Duration::from_secs(60);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" => Some(dir.clone()),
        "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

// @feature: node-lifecycle
/// CONTRACT: a node-admin whose `Wiring` makes no cert choice is refused at start, naming the
/// missing choice, before it binds or stores anything; it never comes up with certs silently off.
#[test]
fn a_node_admin_started_with_no_cert_choice_is_refused_at_start_by_name() {
    let cap = crate::common::capture("a_node_admin_started_with_no_cert_choice_is_refused_at_start_by_name");
    cap.run(crate::common::Threads::Current, async move {
        let dir = std::env::temp_dir().join(format!("member-cert-unchosen-{}", NodeId::mint()));
        let started = start_with(day0_config(&dir), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await;
        let why = started.err().expect("no cert choice must be refused");
        assert!(why.contains("no cert choice") && why.contains("CertChoice::NoCerts"), "the refusal names the missing choice and the way out: {why}");
        assert!(!dir.join("node-key").exists(), "the refusal came before the admin minted its identity");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT (fabric-certs.md, When a cert is issued: "a new mesh's first node-admin is deployed |
/// the maker, which holds the root or an issuing cert that may mint for that mesh"): the Day-0 root
/// made no JoinNode, so its context carries none (`None`), and, its own maker, it holds on its handle
/// the member cert its own issuer issued for exactly its birth, verifying to the root. An admin it launches is issued a member cert by the root for
/// exactly that birth, on the root's rafka-time and verifying to the root, and finds the same bytes
/// in its hook's context and on its handle.
#[test]
fn a_launched_admin_holds_the_member_cert_its_launcher_issued_and_the_day_zero_root_holds_its_own() {
    let cap = crate::common::capture("a_launched_admin_holds_the_member_cert_its_launcher_issued_and_the_day_zero_root_holds_its_own");
    cap.run(crate::common::Threads::Current, async move {
        let root_dir = std::env::temp_dir().join(format!("member-cert-root-{}", NodeId::mint()));
        let joiner_dir = std::env::temp_dir().join(format!("member-cert-joiner-{}", NodeId::mint()));
        let root_pre_node_ready = TestPreNodeReady::new(Mode::Local);
        let root_cfg = day0_config(&root_dir);
        let (fabric, fabric_id) = (root_cfg.fabric.clone(), root_cfg.fabric_id.clone());
        let root = start_with(root_cfg, Wiring { node_hooks: NodeHooks::new(root_pre_node_ready.clone()), certs: root_choice(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling();
        tokio::time::timeout(WRONG, root.pre_node_ready().unwrap().settled()).await.expect("the root's hook returns");
        let own = root.member_cert.clone().expect("the Day-0 root, its own maker, holds a member cert of its own");
        let own_signed = certs::cert_from_bytes(&own).expect("the root's own issuer signed it");
        let me = root.birth();
        assert_eq!(
            (own_signed.cert.node_id.to_string(), own_signed.cert.incarnation.as_str(), own_signed.cert.name.to_string(), own_signed.cert.mesh.as_str(), own_signed.cert.endpoint.0.as_str()),
            (me.node_id.to_string(), me.incarnation.as_str(), "mesh1.admin.1".to_string(), "mesh1", me.endpoint_id.0.as_str()),
            "issued for exactly the Day-0 root's birth"
        );
        assert_eq!(certs::verify(&own_signed, &[test_root_public()], root.rafka_time.now_ms(), 0), Ok(()), "member to issuer, issuer to CA, CA to the root");
        assert_eq!(root_pre_node_ready.member_certs.lock().unwrap().clone(), vec![None], "its context: it made no JoinNode");

        let (root_digest, root_mesh_id) = {
            let d = root.digest.lock().unwrap().clone();
            (d.clone(), d.mesh_id.clone().expect("the root holds its mesh id"))
        };
        std::fs::create_dir_all(&joiner_dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&joiner_dir).unwrap();
        let key = rdm_test::node::load_or_mint_key(&joiner_dir).unwrap();
        let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric,
            fabric_id,
            name: "mesh1.admin.2".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
            seeds: vec![(root_digest.node.endpoint_id.0.clone(), root_digest.node.transport_addr)],
            launcher: Some(Launcher { name: root_digest.node.name.clone(), node_id: root_digest.node.node_id.clone(), incarnation: root_digest.node.incarnation.clone() }),
            data_dir: joiner_dir.clone(),
            store_dir: joiner_dir.clone(),
            mesh_id: Some(root_mesh_id),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        let runtime = rdm_core::runtime::await_own_record(&joiner_dir, Duration::from_secs(10)).unwrap();
        let _ = root.runner.joins.expect(rdm_admin::join::Deployed {
            name: launch.name.clone(),
            node_id: launch.node_id.clone(),
            incarnation: launch.incarnation.clone(),
            supersedes: None,
            endpoint_id: rdm_core::EndpointId(key.public().to_string()),
            runtime,
            data_dir: joiner_dir.display().to_string(),
            last_accepted_name: None,
            held: false,
            listeners: Vec::new(),
        });
        let env = launch.to_env();
        let joiner_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let joiner_pre_node_ready = TestPreNodeReady::new(Mode::Local);
        let before = root.rafka_time.now_ms();
        let joiner = start_with(joiner_cfg, Wiring { node_hooks: NodeHooks::new(joiner_pre_node_ready.clone()), certs: CertChoice::NoCerts, fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.expect("the launched admin comes up");
        let after = root.rafka_time.now_ms();
        assert_eq!(tokio::time::timeout(WRONG, joiner.pre_node_ready().unwrap().settled()).await.expect("the hook returns"), PreNodeReadyState::Passed);

        let bytes = joiner.member_cert.clone().expect("a launched admin joined, so it holds the cert its launcher issued");
        let signed = certs::cert_from_bytes(&bytes).expect("the root issued a signed member cert");
        let cert = &signed.cert;
        assert_eq!(
            (cert.node_id.to_string(), cert.incarnation.as_str(), cert.name.to_string(), cert.mesh.as_str(), cert.endpoint.0.as_str()),
            (launch.node_id.to_string(), launch.incarnation.as_str(), "mesh1.admin.2".to_string(), "mesh1", key.public().to_string().as_str()),
            "issued for exactly this birth"
        );
        let life = certs::MEMBER_CERT_TTL_SECS * 1000;
        assert!((before + life..=after + life).contains(&cert.expiry_ms), "issued on the root's rafka-time: {before} <= {} <= {after}", cert.expiry_ms - life);
        assert_eq!(certs::verify(&signed, &[test_root_public()], after, 0), Ok(()), "verifies to its issuer, the issuer to the CA and the CA to the root");
        assert_eq!(joiner_pre_node_ready.member_certs.lock().unwrap().clone(), vec![Some(bytes)], "the hook's context carries the handle's bytes");

        joiner.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&root_dir);
        let _ = std::fs::remove_dir_all(&joiner_dir);
    })
}

fn app_alpn() -> Alpn {
    Alpn { name: b"member-cert-day0/1".to_vec(), forwardable: false, carriers: vec![rdm_core::NodeKind::RpcNode] }
}

// @feature: node-lifecycle
/// CONTRACT (fabric-certs.md §On a connection: "On every inbound connection of an ALPN the embedding
/// app registered at `bind`, the acceptor reads the dialer's member cert ... and admits or drops the
/// connection"; When a cert is issued, the first node-admin of a mesh, by its maker): the Day-0 root's
/// dial on an app ALPN presents the member cert its own issuer issued it, and a node admitting on the
/// fabric's root admits it. Must NOT happen: the Day-0 root refused `no-credential` (Rafka 141).
#[test]
fn the_day_zero_roots_dial_on_an_app_alpn_presents_its_own_member_cert_and_is_admitted() {
    let cap = crate::common::capture("the_day_zero_roots_dial_on_an_app_alpn_presents_its_own_member_cert_and_is_admitted");
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("member-cert-day0-dial-{}", NodeId::mint()));
        let alpn = app_alpn();
        let root = start_with(day0_config(&dir), Wiring { alpns: vec![alpn.clone()], certs: root_choice(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling();
        let (opener, _) = root.connections(&alpn.name).expect("the app takes its protocol's Connections from the admin").split();

        // A node of mesh1 serving the app ALPN, admitting on the fabric's root.
        let key = iroh::SecretKey::generate();
        let ep = rdm_transport::endpoint::bind_with_alpns(key.clone(), "127.0.0.1:0".parse().unwrap(), std::slice::from_ref(&alpn.name)).await.unwrap();
        let peer = Birth { name: "mesh1.rpc.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let sink = Arc::new(MemoryConnectionsSink::new(peer.clone()));
        let (registry, handlers) = ConnectionsRegistry::new(ep.clone(), Arc::new(StaticResolver::new()), sink, std::slice::from_ref(&alpn), Vec::new()).unwrap();
        registry.admit_on(vec![test_root_public()], rdm_transport::clock::RafkaTime::unadopted());
        let mut rb = iroh::protocol::Router::builder(ep.clone());
        for (name, handler) in handlers {
            rb = rb.accept(name, handler);
        }
        let router = rb.spawn();
        let (_, mut app) = registry.connections(&alpn.name).unwrap().split();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("the peer binds an IPv4 socket");
        root.node_rpc.resolver.apply(ResolvedNode { node_id: peer.node_id.clone(), name: peer.name.clone(), endpoint_id: key.public(), transport_addr: addr, incarnation: peer.incarnation.clone() }, None);

        let accepted = tokio::spawn(async move { app.accept().await });
        let opened = tokio::time::timeout(WRONG, opener.open(&peer)).await.expect("the dial ends");
        let conn = match opened {
            Ok(conn) => conn,
            Err(e) => panic!("the Day-0 root's dial on an app ALPN is admitted on the fabric's root: {e:?}"),
        };
        let reached = tokio::time::timeout(WRONG, accepted).await.expect("the admitted connection reaches the app").unwrap();
        assert_eq!(reached.map(|c| c.id().clone()), Some(conn.id().clone()), "the peer's app accepts the Day-0 root's admitted connection");

        let _ = router.shutdown().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}
