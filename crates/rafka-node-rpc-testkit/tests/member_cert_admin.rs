//! Fabric certs on node-admins (i143.e12.s22): a node-admin is started with an explicit cert
//! choice, the Day-0 root makes no JoinNode and holds no member cert, and an admin launched by
//! another admin holds the member cert that admin's signer issued for exactly its birth, in its
//! hydrate context and on its running handle.

use rafka_mesh_entity::launch::{Launch, Launcher};
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_admin_core::admin::{start_with, AdminConfig};
use rafka_node_admin_core::app_hydration::{Hydration, HydrationState};
use rafka_node_admin_core::certs::CertChoice;
use rafka_node_admin_core::wiring::{FabricHooks, Wiring};
use rafka_node_rpc_testkit::hydrate_probe::{Mode, TestHydrator};
use rafka_node_rpc_testkit::test_certs::{TestCertSigner, TestMemberCert};
use std::sync::Arc;
use std::time::Duration;

const WRONG: Duration = Duration::from_secs(60);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" => Some(dir.clone()),
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
#[tokio::test]
async fn a_node_admin_started_with_no_cert_choice_is_refused_at_start_by_name() {
    let dir = std::env::temp_dir().join(format!("member-cert-unchosen-{}", NodeId::mint()));
    let started = start_with(day0_config(&dir), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await;
    let why = started.err().expect("no cert choice must be refused");
    assert!(why.contains("no cert choice") && why.contains("CertChoice::NoCerts"), "the refusal names the missing choice and the way out: {why}");
    assert!(!dir.join("node-key").exists(), "the refusal came before the admin minted its identity");
    let _ = std::fs::remove_dir_all(&dir);
}

// @feature: node-lifecycle
/// CONTRACT: the Day-0 root made no JoinNode, so it holds no member cert (`None`, not empty bytes)
/// in its context or on its handle. An admin it launches is issued a member cert by the root's
/// signer for exactly that birth, on the root's rafka-time, and finds the same bytes in its hook's
/// context and on its handle.
#[tokio::test]
async fn a_launched_admin_holds_the_member_cert_its_launcher_issued_and_the_day_zero_root_holds_none() {
    let root_dir = std::env::temp_dir().join(format!("member-cert-root-{}", NodeId::mint()));
    let joiner_dir = std::env::temp_dir().join(format!("member-cert-joiner-{}", NodeId::mint()));
    let root_hydrator = TestHydrator::new(Mode::Local);
    let root_cfg = day0_config(&root_dir);
    let (fabric, fabric_id) = (root_cfg.fabric.clone(), root_cfg.fabric_id.clone());
    let root = start_with(root_cfg, Wiring { hydration: Hydration::new(root_hydrator.clone()), certs: CertChoice::Signer(Arc::new(TestCertSigner)), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.expect("the Day-0 admin comes up");
    root.stop_reconciling();
    tokio::time::timeout(WRONG, root.hydration().unwrap().settled()).await.expect("the root's hook returns");
    assert_eq!(root.member_cert, None, "the Day-0 root made no JoinNode");
    assert_eq!(root_hydrator.member_certs.lock().unwrap().clone(), vec![None], "its context says so");

    let (root_digest, root_mesh_id) = {
        let d = root.digest.lock().unwrap().clone();
        (d.clone(), d.mesh_id.clone().expect("the root holds its mesh id"))
    };
    std::fs::create_dir_all(&joiner_dir).unwrap();
    RuntimeFact::of_this_process("cell").unwrap().write_record(&joiner_dir).unwrap();
    let key = rafka_node_rpc_testkit::node::load_or_mint_key(&joiner_dir).unwrap();
    let launch = Launch {
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
        mesh_id: Some(root_mesh_id),
        mesh_issuer: None,
    };
    let runtime = rafka_mesh_entity::runtime::await_own_record(&joiner_dir, Duration::from_secs(10)).unwrap();
    let _ = root.runner.joins.expect(rafka_node_admin_core::join::Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rafka_mesh_entity::EndpointId(key.public().to_string()),
        runtime,
        data_dir: joiner_dir.display().to_string(),
    });
    let env = launch.to_env();
    let joiner_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
    let joiner_hydrator = TestHydrator::new(Mode::Local);
    let before = root.rafka_time.now_ms();
    let joiner = start_with(joiner_cfg, Wiring { hydration: Hydration::new(joiner_hydrator.clone()), certs: CertChoice::NoCerts, fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.expect("the launched admin comes up");
    let after = root.rafka_time.now_ms();
    assert_eq!(tokio::time::timeout(WRONG, joiner.hydration().unwrap().settled()).await.expect("the hook returns"), HydrationState::Passed);

    let bytes = joiner.member_cert.clone().expect("a launched admin joined, so it holds the cert its launcher issued");
    let cert = TestMemberCert::decode(&bytes).expect("the root's signer wrote it");
    assert_eq!(
        (cert.node_id.as_str(), cert.incarnation.as_str(), cert.name.as_str(), cert.mesh.as_str(), cert.endpoint_key.as_str()),
        (launch.node_id.to_string().as_str(), launch.incarnation.0.as_str(), "mesh1.admin.2", "mesh1", key.public().to_string().as_str()),
        "issued for exactly this birth"
    );
    assert!((before..=after).contains(&cert.issued_at_ms), "issued on the root's rafka-time: {before} <= {} <= {after}", cert.issued_at_ms);
    assert_eq!(joiner_hydrator.member_certs.lock().unwrap().clone(), vec![Some(bytes)], "the hook's context carries the handle's bytes");

    joiner.leave().await;
    root.leave().await;
    let _ = std::fs::remove_dir_all(&root_dir);
    let _ = std::fs::remove_dir_all(&joiner_dir);
}
