//! Fabric certs (i143.e12.s22, i145 e1): the accepting authority issues a member cert for exactly the
//! birth whose `JoinNode` it accepts, signed by its mesh's issuing key on its own rafka-time, and the
//! cert reaches the birth in the join answer, its `pre_node_ready` context and its running handle. The
//! cells decode it (`rdm_core::certs`) and verify it to the testkit's root.

use crate::common::{admin_side_issuing, AdminSide};
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId};
use rdm_admin::node_hooks::NodeHooks;
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use rdm_test::node;
use rdm_core::certs::{self, CaKey, IssuingKey};
use rdm_test::test_certs::{root_choice, test_root_public};
use std::time::Duration;

/// A reference no OS clock reads: 1970-03-23.
const PRIMARY_REFERENCE_MS: u64 = 7_000_000_000;

fn launch_in(dir: &std::path::Path, fabric: &FabricId, admin: &AdminSide, incarnation: IncarnationId, supersedes: Option<IncarnationId>, node_id: NodeId) -> Launch {
    Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: fabric.clone(),
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id,
        incarnation,
        supersedes,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![admin.seed.clone()],
        launcher: Some(admin.launcher.clone()),
        data_dir: dir.to_path_buf(),
        store_dir: dir.to_path_buf(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    }
}

fn birth_dir(what: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{what}-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
    dir
}

// @feature: node-lifecycle
/// CONTRACT: a node whose join the authority accepts holds a member cert issued for exactly that
/// birth: its node id, incarnation, path.name, mesh and endpoint key, expiring a member cert's life
/// after the issuing authority's rafka-time (the 7 000 000 000 ms lineage, never an OS clock), and
/// verifying to its issuer and the issuer to the root. The bytes on the running handle are the bytes
/// the authority issued.
#[test]
fn a_node_admitted_by_its_mesh_issuer_holds_a_member_cert_for_exactly_its_birth() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_node_admitted_by_its_mesh_issuer_holds_a_member_cert_for_exactly_its_birth") {
        return;
    }
    let cap = crate::common::capture("a_node_admitted_by_its_mesh_issuer_holds_a_member_cert_for_exactly_its_birth");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let dir = birth_dir("member-cert-birth");
        let fabric = FabricId::mint();
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(PRIMARY_REFERENCE_MS), root_choice()).await;
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, NodeId::mint());
        admin.deployed(&launch, &key);
        let running = node::start(&launch, |b, _| b).await.expect("the admitted node starts");

        let signed = certs::cert_from_bytes(&running.member_cert).expect("the handle holds a signed member cert");
        let cert = &signed.cert;
        assert_eq!(
            (cert.node_id.to_string(), cert.incarnation.as_str(), cert.name.to_string(), cert.mesh.as_str(), cert.endpoint.0.as_str()),
            (launch.node_id.to_string(), launch.incarnation.as_str(), "mesh1.rpc.1".to_string(), "mesh1", key.public().to_string().as_str()),
            "the cert names exactly this birth"
        );
        let issued: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.cert.create.via-join").map(|(_, _, f)| f.clone()).collect();
        assert_eq!(issued.len(), 1, "one issuance for one join: {issued:?}");
        assert_eq!((issued[0]["outcome"].as_str(), issued[0]["cert_len"].as_str()), ("issued", running.member_cert.len().to_string().as_str()), "{issued:?}");
        let issued_at: u64 = issued[0]["issued_at_rafka_ms"].parse().unwrap();
        assert!((PRIMARY_REFERENCE_MS..PRIMARY_REFERENCE_MS + 60_000).contains(&issued_at), "issued on the authority's rafka-time: {issued_at}");
        assert_eq!(cert.expiry_ms, issued_at + certs::MEMBER_CERT_TTL_SECS * 1000, "a member cert's life from its issue");
        assert_eq!(issued[0]["expires_at_rafka_ms"], cert.expiry_ms.to_string());
        assert_eq!(certs::verify(&signed, &[test_root_public()], issued_at, 0), Ok(()), "verifies to its issuer, the issuer to the CA and the CA to the root");

        running.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT: every JoinNode issues a fresh member cert. The same node (same node id and endpoint
/// key) restarted as a new incarnation is issued a cert naming the new incarnation and expiring
/// later than its predecessor's; it never reuses the predecessor's bytes.
#[test]
fn a_restarted_node_is_issued_a_fresh_member_cert_for_its_new_incarnation() {
    let cap = crate::common::capture("a_restarted_node_is_issued_a_fresh_member_cert_for_its_new_incarnation");
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = birth_dir("member-cert-restart");
        let fabric = FabricId::mint();
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(PRIMARY_REFERENCE_MS), root_choice()).await;
        let key = node::load_or_mint_key(&dir).unwrap();
        let node_id = NodeId::mint();
        let first_launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, node_id.clone());
        admin.deployed(&first_launch, &key);
        let first = node::start(&first_launch, |b, _| b).await.expect("the first birth starts");
        let first_bytes = first.member_cert.clone();
        let first_cert = certs::cert_from_bytes(&first_bytes).unwrap().cert;
        first.stop().await;

        let second_launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), Some(first_launch.incarnation.clone()), node_id);
        admin.deployed(&second_launch, &key);
        let second = node::start(&second_launch, |b, _| b).await.expect("the restarted birth starts");
        let second_cert = certs::cert_from_bytes(&second.member_cert).unwrap().cert;

        assert_eq!(first_cert.incarnation, first_launch.incarnation);
        assert_eq!(second_cert.incarnation, second_launch.incarnation, "the restart's cert names the new incarnation");
        assert_ne!(first_bytes, second.member_cert, "a restart never reuses its predecessor's cert");
        assert_eq!((&first_cert.node_id, &first_cert.endpoint), (&second_cert.node_id, &second_cert.endpoint), "the same node and key");
        assert!(second_cert.expiry_ms > first_cert.expiry_ms, "issued later on rafka-time: expiring {} then {}", first_cert.expiry_ms, second_cert.expiry_ms);

        second.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT: the member cert is in the `pre_node_ready` context the first attempt runs with,
/// the same bytes the running handle returns.
#[test]
fn the_member_cert_is_in_the_pre_node_ready_context_and_equals_the_handles() {
    let cap = crate::common::capture("the_member_cert_is_in_the_pre_node_ready_context_and_equals_the_handles");
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = birth_dir("member-cert-ctx");
        let fabric = FabricId::mint();
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(PRIMARY_REFERENCE_MS), root_choice()).await;
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, NodeId::mint());
        admin.deployed(&launch, &key);
        let pre_node_ready = TestPreNodeReady::new(Mode::Local);
        let running = node::start_pre_node_ready(&launch, NodeHooks::new(pre_node_ready.clone()), |b, _| b).await.expect("the node starts");

        let seen = pre_node_ready.member_certs.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "the hook ran once");
        assert_eq!(seen[0].as_deref(), Some(running.member_cert.as_slice()), "the hook's context carries the handle's bytes");
        assert_eq!(certs::cert_from_bytes(&running.member_cert).unwrap().cert.name.to_string(), "mesh1.rpc.1");

        running.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT: an authority explicitly configured with no certs admits a birth with an empty member
/// cert and a span that names the choice; the empty bytes are the answer, not an absence.
#[test]
fn a_mesh_primary_configured_with_no_certs_admits_the_birth_with_an_empty_cert_and_says_so() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_mesh_primary_configured_with_no_certs_admits_the_birth_with_an_empty_cert_and_says_so") {
        return;
    }
    let cap = crate::common::capture("a_mesh_primary_configured_with_no_certs_admits_the_birth_with_an_empty_cert_and_says_so");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let dir = birth_dir("member-cert-none");
        let fabric = FabricId::mint();
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(PRIMARY_REFERENCE_MS), rdm_admin::certs::CertChoice::NoCerts).await;
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, NodeId::mint());
        admin.deployed(&launch, &key);
        let running = node::start(&launch, |b, _| b).await.expect("the node starts");
        assert!(running.member_cert.is_empty(), "no certs: empty bytes");
        let named: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.cert.create.via-no-certs-configured").map(|(_, _, f)| f.clone()).collect();
        assert_eq!(named.len(), 1, "a span names the choice: {named:?}");
        running.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// One birth of mesh1 at `name`, launched by `admin` with the root set `roots` and the product ALPN
/// `alpn` registered, started and joined.
async fn alpn_birth(admin: &AdminSide, fabric: &FabricId, name: &str, roots: Vec<rdm_core::certs::VerifyingKey>, alpn: &rdm_transport::connections::Alpn) -> (rdm_node::node::RunningNode, std::path::PathBuf) {
    let dir = birth_dir("member-cert-roots");
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = Launch { name: name.parse().unwrap(), roots, ..launch_in(&dir, fabric, admin, IncarnationId::mint(), None, NodeId::mint()) };
    admin.deployed(&launch, &key);
    let running = rdm_node::node::start_with_rafka_time_pre_node_ready_alpns_and(&launch, rdm_transport::clock::RafkaTime::unadopted(), rdm_node::node_hooks::NodeHooks::none(), |s| s, None, |b, _| b, &[], std::slice::from_ref(alpn))
        .await
        .expect("the birth starts");
    (running, dir)
}

fn birth_of(n: &rdm_node::node::RunningNode) -> rdm_transport::connections::Birth {
    rdm_transport::connections::Birth { name: n.digest.node.name.clone(), node_id: n.digest.node.node_id.clone(), incarnation: n.digest.node.incarnation.clone() }
}

// @feature: node-lifecycle
/// CONTRACT (node-connections-admit.md Verification; fabric-certs.md §chain: "against the fabric's
/// root set, the anchors every node holds"; fabric-ca-renew.md "Launch"): a node admits on the root set
/// its launch carries (`RDM_ROOTS`). Its peer presenting the member cert its join was issued (chained
/// to the testkit's root) is admitted; the same peer presenting a cert of another root is refused
/// `ca-bad-signature`. A node launched with no root set (its launcher configured with no certs)
/// admits that same foreign cert by name. Must NOT happen: a node with a root set admitting what does
/// not chain to it.
#[test]
fn a_node_admits_on_the_root_set_its_launch_carries() {
    use rdm_transport::NodeResolver as _;
    let cap = crate::common::capture("a_node_admits_on_the_root_set_its_launch_carries");
    cap.run(crate::common::Threads::Workers(4), async move {
        let alpn = rdm_transport::connections::Alpn { name: b"member-cert-roots/1".to_vec(), proxy: false, carriers: vec![rdm_core::NodeKind::RpcNode] };
        let fabric = FabricId::mint();
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(PRIMARY_REFERENCE_MS), root_choice()).await;
        let (acceptor, a_dir) = alpn_birth(&admin, &fabric, "mesh1.rpc.1", vec![test_root_public()], &alpn).await;
        let (dialer, d_dir) = alpn_birth(&admin, &fabric, "mesh1.rpc.2", vec![test_root_public()], &alpn).await;
        let (no_certs, n_dir) = alpn_birth(&admin, &fabric, "mesh1.rpc.3", vec![], &alpn).await;
        let dialing = dialer.data_connections_for_testkit();
        tokio::time::timeout(Duration::from_secs(30), async {
            while [&acceptor, &no_certs].iter().any(|n| dialer.node_rpc.resolver.resolve(&rdm_transport::NodeTarget::ExactNode(n.digest.node.node_id.clone())).is_err()) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the dialer resolves both peers");

        assert!(dialing.probe_direct(&birth_of(&acceptor), &alpn.name).await.is_ok(), "the join's cert chains to the launch's root: admitted");

        let other = rdm_core::certs::mint_key();
        let issuer = IssuingKey::mint(&CaKey::mint(&other, certs::CA_CERT_TTL_SECS, PRIMARY_REFERENCE_MS), "mesh1", PRIMARY_REFERENCE_MS, PRIMARY_REFERENCE_MS + certs::ISSUING_CERT_TTL_SECS * 1000).expect("within the CA's life");
        let own = certs::cert_from_bytes(&dialer.member_cert).unwrap().cert;
        let birth = certs::BirthIdentity { node_id: own.node_id, incarnation: own.incarnation, mesh: own.mesh, name: own.name, endpoint_key: own.endpoint };
        let foreign = certs::issue(&issuer, &birth, PRIMARY_REFERENCE_MS, PRIMARY_REFERENCE_MS + 3_600_000).unwrap();
        dialing.set_member_cert(certs::cert_to_bytes(&foreign));
        match dialing.probe_direct(&birth_of(&acceptor), &alpn.name).await {
            Err(rdm_transport::connections::ConnectError::Refused { reason }) => assert_eq!(reason, "ca-bad-signature"),
            other => panic!("a cert of another root must be refused by name: {other:?}"),
        }
        assert!(dialing.probe_direct(&birth_of(&no_certs), &alpn.name).await.is_ok(), "a node launched with no root set admits by name");

        for n in [acceptor, dialer, no_certs] {
            n.stop().await;
        }
        for d in [a_dir, d_dir, n_dir] {
            let _ = std::fs::remove_dir_all(&d);
        }
    })
}
