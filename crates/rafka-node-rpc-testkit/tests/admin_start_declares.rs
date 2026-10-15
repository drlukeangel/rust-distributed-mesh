//! A node-admin reports its node.started at every start (node-status.md "Status report": "A node
//! reports to its mesh's primary, which sets its status"; states.md "Who sets each state": node
//! ReadyForTraffic is set by "the node's mesh-primary" from "the node's node.started report, after every
//! start gate").
//!
//! CONTRACT: a node-admin's start after its stop owes its own `DeclareNodeState(ReadyForTraffic)` to its
//! mesh's primary afresh: once its node.started is reported (`via-ready` under its start's root), it
//! sends the declaration again and the primary answers it by name, as it did after the admin's first
//! start. What must NOT happen: the declaration its first cycle sent standing for the next cycle's, so
//! the primary never takes the restarted birth's node.started (a restart it holds open stays open, and
//! the birth's status stays what node.joined set).
//!
//! The mesh's primary is a real Day-0 node-admin in this process; it launches the second admin and
//! commands its stop and start. Its own process: the cell holds process-wide state (the parking flag).

use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, NodeId, PathName};
use rafka_node_admin_core::admin::{start_with, AdminConfig, Running};
use rafka_node_admin_core::app_hydration::{HookOutcome, Hydration};
use rafka_node_admin_core::join::Deployed;
use rafka_node_admin_core::wiring::{FabricHooks, Wiring};
use rdm_core::status::{StatusReply, StatusRequest};
use rafka_node_rpc_testkit::hydrate_probe::{Mode, TestHydrator};
use std::collections::BTreeMap;
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);
/// What a declaration owed at a report is given: the declarer's round runs on the report's wake.
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
fn launch_by(root: &Running, name: &str, dir: &std::path::Path) -> Launch {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
    Launch {
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![("control".into(), "127.0.0.1:0".parse().unwrap())],
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.to_path_buf(),
        store_dir: dir.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        mesh_issuer: None,
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    }
}

/// What `root` verifies the join of `launch`'s birth against.
fn deployed(launch: &Launch, held: Option<&PathName>) -> Deployed {
    let key = rafka_node_rpc_testkit::node::load_or_mint_key(&launch.data_dir).unwrap();
    Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: rdm_core::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).unwrap(),
        data_dir: launch.data_dir.display().to_string(),
        last_accepted_name: held.cloned(),
        held: held.is_some(),
    }
}

async fn until_ready(what: &str, digest: impl Fn() -> MeshDigest) {
    let waited = async {
        while digest().status != MemberStatus::ReadyForTraffic {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::time::timeout(WRONG, waited).await.unwrap_or_else(|_| panic!("{what} never published ReadyForTraffic"));
}

fn command(launch: &Launch, start: bool) -> StatusRequest {
    let (node_id, incarnation, build_id) = (launch.node_id.clone(), launch.incarnation.clone(), "xtccsd8180sr".to_string());
    if start {
        StatusRequest::StartNode { node_id, incarnation, build_id, attempt: 2, operation: format!("node.start:{}", launch.name) }
    } else {
        StatusRequest::StopNode { node_id, incarnation, build_id, attempt: 1, operation: format!("node.stop:{}", launch.name) }
    }
}

/// `req` sent by `root` over Node RPC to the birth `launch` names, as its mesh's primary sends it:
/// the commanded admin holds the call's endpoint key, which its start joins back through
/// (node-start.md step 2).
async fn call(root: &Running, launch: &Launch, req: &StatusRequest) -> StatusReply {
    let opts = rafka_node_rpc::CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: WRONG }, ..Default::default() };
    match root.node_rpc.client.call::<rdm_core::status::Status>(&rafka_node_rpc::NodeTarget::ExactNode(launch.node_id.clone()), req, &opts).await.0 {
        rdm_core::outcome::RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} to {} did not end in a reply: {other:?}", req.op(), launch.name),
    }
}

/// The finished own-state declarations `node` sent: each one's target and outcome.
fn own_declarations(spans: &crate::common::Spans, node: &str) -> Vec<BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.node_admin.status.update.via-declare" && f.get("node").map(String::as_str) == Some(node) && f.get("key").is_some_and(|k| k.contains("OwnState")) && f.get("finished").map(String::as_str) == Some("true")).map(|(_, _, f)| f.clone()).collect()
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
        // The mesh's primary: a Day-0 node-admin that does not reconcile (it would retire the admin this
        // cell launches in its own process).
        let root = start_with(day0_config(&root_dir), Wiring { hydration: Hydration::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling();
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        tokio::time::timeout(WRONG, async {
            while root.membership.seats().mesh("mesh1").is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the Day-0 admin takes mesh1's primary seat");
        let root_name = root.digest.lock().unwrap().node.name.to_string();

        let launch = launch_by(&root, "mesh1.admin.2", &admin_dir);
        let _ = root.runner.joins.expect(deployed(&launch, None));
        let env = launch.to_env();
        let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let passing = Hydration::new(TestHydrator::new(Mode::Fixed(HookOutcome::Ok)));
        let admin = start_with(admin_cfg, Wiring { hydration: passing, fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the launched admin comes up");
        until_ready("mesh1.admin.2 (first start)", || admin.digest.lock().unwrap().clone()).await;
        let first = tokio::time::timeout(DECLARED, async {
            loop {
                let held = own_declarations(&spans, "mesh1.admin.2");
                if !held.is_empty() {
                    return held;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("mesh1.admin.2 declares its first start's node.started");
        assert_eq!(first.iter().map(|f| f["to"].clone()).collect::<Vec<_>>(), vec![root_name.clone()], "to its mesh's primary: {first:#?}");

        assert!(matches!(call(&root, &launch, &command(&launch, false)).await, StatusReply::Stopped { .. }), "the launched admin stops");
        let _ = root.runner.joins.expect(deployed(&launch, Some(&launch.name)));
        assert_eq!(call(&root, &launch, &command(&launch, true)).await, StatusReply::Started, "the launched admin starts after its stop");
        until_ready("mesh1.admin.2 (start after stop)", || admin.digest.lock().unwrap().clone()).await;
        let again = tokio::time::timeout(DECLARED, async {
            loop {
                let held = own_declarations(&spans, "mesh1.admin.2");
                if held.len() > first.len() {
                    return held;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("mesh1.admin.2's start after its stop reported no node.started to its mesh's primary within {DECLARED:?}: its declarations are only its first start's {first:#?}"));
        let last = again.iter().find(|f| !first.contains(f)).expect("a declaration after the start");
        assert_eq!(last["to"], root_name, "to its mesh's primary: {again:#?}");
        assert!(["applied", "already-applied"].contains(&last["outcome"].as_str()), "the primary answered it by name: {last:#?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
