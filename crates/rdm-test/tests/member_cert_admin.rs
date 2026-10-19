//! Fabric certs on node-admins (i143.e12.s22): a node-admin is started with an explicit cert
//! choice, the Day-0 root makes no JoinNode and holds no member cert, and an admin launched by
//! another admin holds the member cert that admin issued for exactly its birth, verifying to the
//! root, in its before_ready context and on its running handle.

use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, NodeId};
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::before_ready::{BeforeReadyHook, BeforeReadyState};
use rdm_admin::certs::CertChoice;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_test::before_ready_probe::{Mode, TestBeforeReady};
use rdm_core::certs;
use rdm_test::test_certs::{root_choice, test_root_public};
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
/// CONTRACT: the Day-0 root made no JoinNode, so it holds no member cert (`None`, not empty bytes)
/// in its context or on its handle. An admin it launches is issued a member cert by the root for
/// exactly that birth, on the root's rafka-time and verifying to the root, and finds the same bytes
/// in its hook's context and on its handle.
#[test]
fn a_launched_admin_holds_the_member_cert_its_launcher_issued_and_the_day_zero_root_holds_none() {
    let cap = crate::common::capture("a_launched_admin_holds_the_member_cert_its_launcher_issued_and_the_day_zero_root_holds_none");
    cap.run(crate::common::Threads::Current, async move {
        let root_dir = std::env::temp_dir().join(format!("member-cert-root-{}", NodeId::mint()));
        let joiner_dir = std::env::temp_dir().join(format!("member-cert-joiner-{}", NodeId::mint()));
        let root_before_ready = TestBeforeReady::new(Mode::Local);
        let root_cfg = day0_config(&root_dir);
        let (fabric, fabric_id) = (root_cfg.fabric.clone(), root_cfg.fabric_id.clone());
        let root = start_with(root_cfg, Wiring { before_ready: BeforeReadyHook::new(root_before_ready.clone()), certs: root_choice(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling();
        tokio::time::timeout(WRONG, root.before_ready().unwrap().settled()).await.expect("the root's hook returns");
        assert_eq!(root.member_cert, None, "the Day-0 root made no JoinNode");
        assert_eq!(root_before_ready.member_certs.lock().unwrap().clone(), vec![None], "its context says so");

        let (root_digest, root_mesh_id) = {
            let d = root.digest.lock().unwrap().clone();
            (d.clone(), d.mesh_id.clone().expect("the root holds its mesh id"))
        };
        std::fs::create_dir_all(&joiner_dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&joiner_dir).unwrap();
        let key = rdm_test::node::load_or_mint_key(&joiner_dir).unwrap();
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
            store_dir: joiner_dir.clone(),
            mesh_id: Some(root_mesh_id),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
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
        });
        let env = launch.to_env();
        let joiner_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let joiner_before_ready = TestBeforeReady::new(Mode::Local);
        let before = root.rafka_time.now_ms();
        let joiner = start_with(joiner_cfg, Wiring { before_ready: BeforeReadyHook::new(joiner_before_ready.clone()), certs: CertChoice::NoCerts, fabric_hooks: FabricHooks::no_app_work(), ..Wiring::default() }).await.expect("the launched admin comes up");
        let after = root.rafka_time.now_ms();
        assert_eq!(tokio::time::timeout(WRONG, joiner.before_ready().unwrap().settled()).await.expect("the hook returns"), BeforeReadyState::Passed);

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
        assert_eq!(joiner_before_ready.member_certs.lock().unwrap().clone(), vec![Some(bytes)], "the hook's context carries the handle's bytes");

        joiner.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&root_dir);
        let _ = std::fs::remove_dir_all(&joiner_dir);
    })
}
