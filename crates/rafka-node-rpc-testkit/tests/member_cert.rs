//! Fabric certs (i143.e12.s22): the accepting authority's signer issues a member cert for exactly
//! the birth whose `JoinNode` it accepts, on its own rafka-time, and the cert reaches the birth in
//! the join answer, its `hydrate_before_ready` context and its running handle. RDM never parses
//! the bytes: the cells decode them with the test signer's own reader.

use crate::common::{admin_side_issuing, AdminSide, Spans};
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId};
use rafka_node_admin_core::app_hydration::Hydration;
use rafka_node_rpc_testkit::hydrate_probe::{Mode, TestHydrator};
use rafka_node_rpc_testkit::node;
use rafka_node_rpc_testkit::test_certs::{RefusingCertSigner, TestCertSigner, TestMemberCert};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

/// A reference no OS clock reads: 1970-03-23.
const AUTHORITY_REFERENCE_MS: u64 = 7_000_000_000;

fn launch_in(dir: &std::path::Path, fabric: &FabricId, admin: &AdminSide, incarnation: IncarnationId, supersedes: Option<IncarnationId>, node_id: NodeId) -> Launch {
    Launch {
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
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
        mesh_issuer: None,
    }
}

fn birth_dir(what: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{what}-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
    dir
}

// @feature: node-lifecycle
/// CONTRACT: a node whose join the authority accepts holds a member cert issued for exactly that
/// birth: its node id, incarnation, path.name, mesh and endpoint key, stamped with the issuing
/// authority's rafka-time (the 7 000 000 000 ms lineage, never an OS clock). The bytes on the
/// running handle are the bytes the signer wrote.
#[tokio::test]
async fn a_node_admitted_by_an_issuing_authority_holds_a_member_cert_for_exactly_its_birth() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let dir = birth_dir("member-cert-birth");
    let fabric = FabricId::mint();
    let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), Arc::new(TestCertSigner)).await;
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, NodeId::mint());
    admin.deployed(&launch, &key);
    let running = node::start(&launch, |b, _| b).await.expect("the admitted node starts");

    let cert = TestMemberCert::decode(&running.member_cert).expect("the handle holds the signer's bytes");
    assert_eq!(
        (cert.node_id.as_str(), cert.incarnation.as_str(), cert.name.as_str(), cert.mesh.as_str(), cert.endpoint_key.as_str()),
        (launch.node_id.to_string().as_str(), launch.incarnation.0.as_str(), "mesh1.rpc.1", "mesh1", key.public().to_string().as_str()),
        "the cert names exactly this birth"
    );
    assert!((AUTHORITY_REFERENCE_MS..AUTHORITY_REFERENCE_MS + 60_000).contains(&cert.issued_at_ms), "issued on the authority's rafka-time: {}", cert.issued_at_ms);
    let issued: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.cert.create.via-join").map(|(_, _, f)| f.clone()).collect();
    assert_eq!(issued.len(), 1, "one issuance for one join: {issued:?}");
    assert_eq!((issued[0]["outcome"].as_str(), issued[0]["cert_len"].as_str()), ("issued", running.member_cert.len().to_string().as_str()), "{issued:?}");
    assert_eq!(issued[0]["issued_at_rafka_ms"], cert.issued_at_ms.to_string());

    running.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// @feature: node-lifecycle
/// CONTRACT: every JoinNode issues a fresh member cert. The same node (same node id and endpoint
/// key) restarted as a new incarnation is issued a cert naming the new incarnation and stamped
/// later than its predecessor's; it never reuses the predecessor's bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_node_is_issued_a_fresh_member_cert_for_its_new_incarnation() {
    let dir = birth_dir("member-cert-restart");
    let fabric = FabricId::mint();
    let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), Arc::new(TestCertSigner)).await;
    let key = node::load_or_mint_key(&dir).unwrap();
    let node_id = NodeId::mint();
    let first_launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, node_id.clone());
    admin.deployed(&first_launch, &key);
    let first = node::start(&first_launch, |b, _| b).await.expect("the first birth starts");
    let first_bytes = first.member_cert.clone();
    let first_cert = TestMemberCert::decode(&first_bytes).unwrap();
    first.stop(Duration::ZERO).await;

    let second_launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), Some(first_launch.incarnation.clone()), node_id);
    admin.deployed(&second_launch, &key);
    let second = node::start(&second_launch, |b, _| b).await.expect("the restarted birth starts");
    let second_cert = TestMemberCert::decode(&second.member_cert).unwrap();

    assert_eq!(first_cert.incarnation, first_launch.incarnation.0);
    assert_eq!(second_cert.incarnation, second_launch.incarnation.0, "the restart's cert names the new incarnation");
    assert_ne!(first_bytes, second.member_cert, "a restart never reuses its predecessor's cert");
    assert_eq!((first_cert.node_id.as_str(), first_cert.endpoint_key.as_str()), (second_cert.node_id.as_str(), second_cert.endpoint_key.as_str()), "the same node and key");
    assert!(second_cert.issued_at_ms > first_cert.issued_at_ms, "issued later on rafka-time: {} then {}", first_cert.issued_at_ms, second_cert.issued_at_ms);

    second.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// @feature: node-lifecycle
/// CONTRACT: a signer that refuses to issue refuses the join by name. The node ends at start
/// naming the signer's refusal, is never ready, and the authority installed nothing for its key:
/// its membership book holds no digest of the node. What must NOT happen: the join retried as if the
/// authority were not ready, or the birth admitted without a cert.
#[tokio::test]
async fn a_refused_issuance_refuses_the_join_by_name_and_the_node_is_never_admitted() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let dir = birth_dir("member-cert-refused");
    let fabric = FabricId::mint();
    let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), Arc::new(RefusingCertSigner::new("signer-offline", "the signing key is not loaded"))).await;
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, NodeId::mint());
    admin.deployed(&launch, &key);

    let started = tokio::time::timeout(Duration::from_secs(4), node::start(&launch, |b, _| b)).await.expect("the refusal ends the start at once; it is not retried as a not-ready join");
    let why = started.err().expect("a refused issuance must end the node").to_string();
    assert!(why.contains("was refused its join") && why.contains("signer-offline") && why.contains("the signing key is not loaded"), "the refusal names the signer's refusal: {why}");
    assert!(admin.observer.membership.book.get(launch.node_id.as_str()).is_none(), "nothing was installed for the refused birth");
    let refusals: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.cert.reject.via-signer-refusal").map(|(_, _, f)| f.clone()).collect();
    assert_eq!(refusals.len(), 1, "one refusal span: {refusals:?}");
    assert_eq!(refusals[0]["refusal"], "signer-offline");
    let joins: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.node.update.via-join").map(|(_, _, f)| f.clone()).collect();
    assert_eq!(joins.len(), 1, "the join was answered once: {joins:?}");
    assert_eq!(joins[0]["outcome"], "cert-refused");
    let _ = std::fs::remove_dir_all(&dir);
}

// @feature: node-lifecycle
/// CONTRACT: the member cert is in the `hydrate_before_ready` context the first attempt runs with,
/// the same bytes the running handle returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_member_cert_is_in_the_hydrate_context_and_equals_the_handles() {
    let dir = birth_dir("member-cert-ctx");
    let fabric = FabricId::mint();
    let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), Arc::new(TestCertSigner)).await;
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, NodeId::mint());
    admin.deployed(&launch, &key);
    let hydrator = TestHydrator::new(Mode::Local);
    let running = node::start_hydrating(&launch, Hydration::new(hydrator.clone()), |b, _| b).await.expect("the node starts");

    let seen = hydrator.member_certs.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "the hook ran once");
    assert_eq!(seen[0].as_deref(), Some(running.member_cert.as_slice()), "the hook's context carries the handle's bytes");
    assert_eq!(TestMemberCert::decode(&running.member_cert).unwrap().name, "mesh1.rpc.1");

    running.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&dir);
}

// @feature: node-lifecycle
/// CONTRACT: an authority explicitly configured with no certs admits a birth with an empty member
/// cert and a span that names the choice; the empty bytes are the answer, not an absence.
#[tokio::test]
async fn an_authority_configured_with_no_certs_admits_the_birth_with_an_empty_cert_and_says_so() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let dir = birth_dir("member-cert-none");
    let fabric = FabricId::mint();
    let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), Arc::new(rafka_node_admin_core::certs::NoCerts)).await;
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, NodeId::mint());
    admin.deployed(&launch, &key);
    let running = node::start(&launch, |b, _| b).await.expect("the node starts");
    assert!(running.member_cert.is_empty(), "no certs: empty bytes");
    let named: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.cert.create.via-no-certs-configured").map(|(_, _, f)| f.clone()).collect();
    assert_eq!(named.len(), 1, "a span names the choice: {named:?}");
    running.stop(Duration::ZERO).await;
    let _ = std::fs::remove_dir_all(&dir);
}
