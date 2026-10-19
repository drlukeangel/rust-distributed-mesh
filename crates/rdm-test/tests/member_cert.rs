//! Fabric certs (i143.e12.s22, i145 e1): the accepting authority issues a member cert for exactly the
//! birth whose `JoinNode` it accepts, signed by its mesh's issuing key on its own rafka-time, and the
//! cert reaches the birth in the join answer, its `before_ready` context and its running handle. The
//! cells decode it (`rdm_core::certs`) and verify it to the testkit's root.

use crate::common::{admin_side_issuing, AdminSide};
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId};
use rdm_admin::before_ready::BeforeReadyHook;
use rdm_test::before_ready_probe::{Mode, TestBeforeReady};
use rdm_test::node;
use rdm_core::certs::{self, IssuingKey};
use rdm_test::test_certs::{root_choice, test_root, test_root_public};
use std::time::Duration;

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
        store_dir: dir.to_path_buf(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
        issuing: None,
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
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
fn a_node_admitted_by_an_issuing_authority_holds_a_member_cert_for_exactly_its_birth() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_node_admitted_by_an_issuing_authority_holds_a_member_cert_for_exactly_its_birth") {
        return;
    }
    let cap = crate::common::capture("a_node_admitted_by_an_issuing_authority_holds_a_member_cert_for_exactly_its_birth");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let dir = birth_dir("member-cert-birth");
        let fabric = FabricId::mint();
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), root_choice()).await;
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
        assert!((AUTHORITY_REFERENCE_MS..AUTHORITY_REFERENCE_MS + 60_000).contains(&issued_at), "issued on the authority's rafka-time: {issued_at}");
        assert_eq!(cert.expiry_ms, issued_at + certs::MEMBER_CERT_TTL_SECS * 1000, "a member cert's life from its issue");
        assert_eq!(issued[0]["expires_at_rafka_ms"], cert.expiry_ms.to_string());
        assert_eq!(certs::verify(&signed, &test_root_public(), issued_at, 0), Ok(()), "verifies to its issuer and the issuer to the root");

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
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), root_choice()).await;
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
/// CONTRACT (fabric-certs.md: "A refused issuance refuses the join by name; the birth gets no cert and
/// is not admitted"): an authority that holds another mesh's issuing material and no root is refused
/// the member cert (`no-root-to-mint`), and that refuses the join by name. The node ends at start
/// naming the refusal, is never ready, and the authority installed nothing for its key: its
/// membership book holds no digest of the node. What must NOT happen: the join retried as if the
/// authority were not ready, or the birth admitted without a cert.
#[test]
fn a_refused_issuance_refuses_the_join_by_name_and_the_node_is_never_admitted() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "a_refused_issuance_refuses_the_join_by_name_and_the_node_is_never_admitted") {
        return;
    }
    let cap = crate::common::capture("a_refused_issuance_refuses_the_join_by_name_and_the_node_is_never_admitted");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let dir = birth_dir("member-cert-refused");
        let fabric = FabricId::mint();
        // mesh2's issuing material and no root: it signs nothing for mesh1.
        let mesh2 = IssuingKey::mint(&test_root(), "mesh2", certs::ISSUING_CERT_TTL_SECS, AUTHORITY_REFERENCE_MS);
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), rdm_admin::certs::CertChoice::Issuer { key: mesh2 }).await;
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, NodeId::mint());
        admin.deployed(&launch, &key);

        let started = tokio::time::timeout(Duration::from_secs(4), node::start(&launch, |b, _| b)).await.expect("the refusal ends the start at once; it is not retried as a not-ready join");
        let why = started.err().expect("a refused issuance must end the node").to_string();
        assert!(why.contains("was refused its join") && why.contains("no-root-to-mint") && why.contains("signs nothing for mesh mesh1"), "the refusal names the chain's refusal: {why}");
        assert!(admin.observer.membership.book.get(launch.node_id.as_str()).is_none(), "nothing was installed for the refused birth");
        let refusals: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.cert.reject.via-refused").map(|(_, _, f)| f.clone()).collect();
        assert_eq!(refusals.len(), 1, "one refusal span: {refusals:?}");
        assert_eq!(refusals[0]["refusal"], "no-root-to-mint");
        let joins: Vec<_> = spans.0.lock().unwrap().values().filter(|(n, _, _)| n == "rdm.node_admin.node.update.via-join").map(|(_, _, f)| f.clone()).collect();
        assert_eq!(joins.len(), 1, "the join was answered once: {joins:?}");
        assert_eq!(joins[0]["outcome"], "cert-refused");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

// @feature: node-lifecycle
/// CONTRACT: the member cert is in the `before_ready` context the first attempt runs with,
/// the same bytes the running handle returns.
#[test]
fn the_member_cert_is_in_the_before_ready_context_and_equals_the_handles() {
    let cap = crate::common::capture("the_member_cert_is_in_the_before_ready_context_and_equals_the_handles");
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = birth_dir("member-cert-ctx");
        let fabric = FabricId::mint();
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), root_choice()).await;
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = launch_in(&dir, &fabric, &admin, IncarnationId::mint(), None, NodeId::mint());
        admin.deployed(&launch, &key);
        let before_ready = TestBeforeReady::new(Mode::Local);
        let running = node::start_before_ready(&launch, BeforeReadyHook::new(before_ready.clone()), |b, _| b).await.expect("the node starts");

        let seen = before_ready.member_certs.lock().unwrap().clone();
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
fn an_authority_configured_with_no_certs_admits_the_birth_with_an_empty_cert_and_says_so() {
    // A scoped subscriber: tracing's process-wide callsite interest cache is not shared with the
    // cells that run the same callsites without one.
    if crate::own_process::delegated(module_path!(), "an_authority_configured_with_no_certs_admits_the_birth_with_an_empty_cert_and_says_so") {
        return;
    }
    let cap = crate::common::capture("an_authority_configured_with_no_certs_admits_the_birth_with_an_empty_cert_and_says_so");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Current, async move {
        let dir = birth_dir("member-cert-none");
        let fabric = FabricId::mint();
        let admin = admin_side_issuing("127.0.0.1".parse().unwrap(), &fabric, Some(AUTHORITY_REFERENCE_MS), rdm_admin::certs::CertChoice::NoCerts).await;
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
