//! A node-admin reports its node.started at every start (node-status.md "Status report": "A node
//! reports to its mesh's primary, which sets its status"; states.md "Who sets each state": node
//! Ready is set by "the node's mesh-primary" from "the node's node.started report, after every
//! start gate").
//!
//! CONTRACT (Q47, Q93; node-start.md step 5): the first start owes a Ready declaration. A subsequent
//! commanded start reports NodeJoined and NodeStarted on 0x22; its primary records Ready before
//! returning the completion. "The node also declares Ready to its mesh-primary as an owed message, as
//! it declares Stopped (Q36, Q87); frame and declaration apply once (Q49)." Q93 supersedes Q47's line
//! "The open start sends no separate readiness declaration."
//!
//! The mesh's primary is a real Day-0 node-admin in this process; it launches the second admin and
//! commands its stop and start. Its own process: the cell holds process-wide state (the stop flag).

use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, NodeId, PathName};
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::node_hooks::{HookOutcome, NodeHooks};
use rdm_admin::join::Deployed;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::status::{StatusReply, StatusRequest};
use rdm_test::pre_node_ready_probe::{Mode, TestPreNodeReady};
use std::collections::BTreeMap;
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);
/// What a declaration owed at a report is given: the declarer's checklist runs on the report's wake.
const DECLARED: Duration = Duration::from_secs(10);

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

/// A launch of `name` by `root`, its data dir holding the runtime record its provider would write.
/// A launch of `name` by `root` over its own data dir `dir`, on the root's store root `store`: every
/// admin of a fabric reads the one Build storage (Q292).
fn launch_by(root: &Running, name: PathName, dir: &std::path::Path, store: &std::path::Path) -> Launch {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
    Launch { initial_properties: Default::default(), topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        mesh: "mesh1".into(),
        r#type: "node-admin".into(),
        path: name.path.clone(), name: name.name.clone(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(Launcher { mesh: d.node.mesh.clone(), path: d.node.path.clone(), name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.to_path_buf(),
        store_dir: store.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    }
}

/// What `root` verifies the join of `launch`'s birth against.
fn deployed(launch: &Launch, held: Option<&PathName>) -> Deployed {
    let key = rdm_test::node::load_or_mint_key(&launch.data_dir).unwrap();
    Deployed { mesh: launch.mesh.clone(), r#type: launch.r#type.clone(),
        path: launch.path.clone(), name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: rdm_core::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).unwrap(),
        data_dir: launch.data_dir.display().to_string(),
        last_accepted_name: held.cloned(),
        held: held.is_some(),
        listeners: Vec::new(),
    }
}

async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != MemberStatus::Ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published Ready"));
}

fn command(launch: &Launch, start: bool) -> StatusRequest {
    let (node_id, incarnation, build_id) = (launch.node_id.clone(), launch.incarnation.clone(), "xtccsd8180sr".to_string());
    if start {
        StatusRequest::StartNode { node_id, incarnation, build_id, operation: rdm_core::OperationId::node("node.start", &launch.node_id) }
    } else {
        StatusRequest::StopNode { node_id, incarnation, build_id, operation: rdm_core::OperationId::node("node.stop", &launch.node_id) }
    }
}

/// `req` sent by `root` over Node RPC to the birth `launch` names, as its mesh's primary sends it:
/// the commanded admin holds the call's endpoint key, which its start joins back through
/// (node-start.md step 2).
async fn call(root: &Running, launch: &Launch, req: &StatusRequest) -> StatusReply {
    let opts = rdm_transport::CallOptions { send: rdm_transport::Send::with_time_limit(WRONG + rdm_transport::TRANSPORT_LIMIT), ..Default::default() };
    if let Some(reply) = crate::common::root_lifecycle_reply(root, launch, req).await { return reply; }
    match root.node_rpc.client.call::<rdm_core::status::Status>(&rdm_transport::NodeTarget::ExactNode(launch.node_id.clone()), req, &opts).await.0 {
        rdm_core::outcome::RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} to {:?} did not end in a reply: {other:?}", req.op(), launch.name),
    }
}

/// The finished own-state declarations `node` sent: each one's target and outcome.
fn own_declarations(spans: &crate::common::Spans, node: &PathName) -> Vec<BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.node_admin.status.update.via-declare" && f.get("node_path") == Some(&node.path) && f.get("node_name") == Some(&node.name) && f.get("key").is_some_and(|k| k.contains("OwnState")) && f.get("finished").map(String::as_str) == Some("true")).map(|(_, _, f)| f.clone()).collect()
}

#[test]
fn a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again() {
    if crate::own_process::delegated(module_path!(), "a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again") {
        return;
    }
    let cap = crate::common::capture("a_node_admins_start_after_its_stop_reports_its_node_started_to_its_mesh_primary_again");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("admin-start-declares-{}", NodeId::mint()));
        let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
        std::fs::create_dir_all(&root_dir).unwrap();
        // The mesh's primary: a Day-0 node-admin that does not reconcile (it would delete the admin this
        // cell launches in its own process).
        let root = start_with(day0_config(&root_dir), Wiring { node_hooks: NodeHooks::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        tokio::time::timeout(WRONG, async {
            while root.topology_view.primaries().mesh("mesh1").is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the Day-0 admin takes mesh1's primary role");
        let root_id = root.digest.lock().unwrap().node.node_id.to_string();

        let launch = launch_by(&root, PathName::new("mesh1.node-admin", "2").unwrap(), &admin_dir, &root_dir);
        let _ = root.runner.joins.expect(deployed(&launch, None));
        let env = launch.to_env();
        let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let passing = NodeHooks::new(TestPreNodeReady::new(Mode::Fixed(HookOutcome::Ok)));
        let admin = start_with(admin_cfg, Wiring { node_hooks: passing, fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the launched admin comes up");
        until_ready("mesh1.node-admin.2 (first start)", || admin.digest.lock().unwrap().clone()).await;
        let first = tokio::time::timeout(DECLARED, async {
            loop {
                let held = own_declarations(&spans, &launch.location());
                if !held.is_empty() {
                    return held;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("mesh1.node-admin.2 declares its first start's node.started");
        assert_eq!(first.iter().map(|f| f["to_node_id"].clone()).collect::<Vec<_>>(), vec![root_id.clone()], "to its mesh's primary: {first:#?}");

        assert!(matches!(call(&root, &launch, &command(&launch, false)).await, StatusReply::Stopped { .. }), "the launched admin stops");
        let _ = root.runner.joins.expect(deployed(&launch, Some(&launch.location())));
        assert_eq!(call(&root, &launch, &command(&launch, true)).await, StatusReply::Started, "the launched admin starts after its stop");
        until_ready("mesh1.node-admin.2 (start after stop)", || admin.digest.lock().unwrap().clone()).await;
        tokio::time::timeout(DECLARED, spans.recorded("rdm.node_admin.node.update.via-command-frame", |f| f.get("node_id").map(String::as_str) == Some(launch.node_id.as_str()) && f.get("frame").map(String::as_str) == Some("node-started"))).await.expect("the mesh-primary records NodeStarted on the open command stream");
        let status_service = root.status_service().unwrap();
        assert_eq!(status_service.declared.lock().unwrap().node(&launch.node_id, &launch.incarnation), Some(rdm_core::status::NodeState::Ready));
        let started = tokio::time::timeout(DECLARED, async {
            loop {
                let held: Vec<_> = own_declarations(&spans, &launch.location()).into_iter().filter(|f| f.get("key").is_some_and(|v| v.contains("Ready")) && f.get("operation").map(String::as_str) == Some("node.start")).collect();
                if !held.is_empty() {
                    return held;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the start declares its Ready naming the start, beside its NodeStarted frame");
        assert_eq!(started.len(), 1, "the start's Ready is declared once: {started:#?}");
        assert_eq!(started[0]["to_node_id"], root_id, "to its mesh's primary: {started:#?}");
        assert!(matches!(started[0]["outcome"].as_str(), "applied" | "already-applied"), "frame and declaration apply once: the declaration is applied or answered already applied: {started:#?}");
        let first_again: Vec<_> = own_declarations(&spans, &launch.location()).into_iter().filter(|f| f.get("key").is_some_and(|v| v.contains("Ready")) && f.get("operation").is_none()).collect();
        assert_eq!(first_again, first, "the first start's declaration stands beside it: {first_again:#?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
