//! node.start is one workflow (lifecycles/node-start.md, "End-to-end sequence"; lifecycles/node-create.md:
//! create composes the whole node.start with FirstStart, a start after a stop runs it with AfterStop).
//!
//! CONTRACT: the four start paths run the same start steps in the same order, each step a child of
//! its start's root span (`rdm.mesh.node.create.via-boot` for a first start,
//! `rdm.mesh.node.update.via-start` for a start after a stop): a node-admin's first start (launched by
//! another admin), the same node-admin's start after its stop, an rpc node's first start and the same
//! rpc node's start after its stop. The steps are the join (`via-join-admitted`), the mesh channel
//! (`via-membership-joined`), the topology (`via-topology-taken`), the `hydrate_before_ready` attempt
//! (`via-hydrate-attempt`) and the report (`via-ready`). What must NOT happen: a start path that steps
//! the sequence on its own, so that a step is missing from its trace, runs outside its start's root,
//! or comes in another order than in the other paths.
//!
//! The authority is a real Day-0 node-admin in this process; it launches the second admin and the rpc
//! node, and commands their stops and starts. Its own process: the cell holds process-wide state (the
//! parking flag) and sets the gate's cadence (`tools/test-runner/src/lib.rs` CADENCE).

use rafka_mesh_entity::launch::{Launch, Launcher};
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{IncarnationId, MemberStatus, MeshDigest, NodeId, PathName};
use rafka_node_admin_core::admin::{start_with, AdminConfig, Running};
use rafka_node_admin_core::app_hydration::{HookOutcome, Hydration};
use rafka_node_admin_core::join::Deployed;
use rafka_node_admin_core::wiring::{FabricHooks, Wiring};
use rafka_node_rpc_contract::status::{StatusReply, StatusRequest};
use rafka_node_rpc_testkit::hydrate_probe::{Mode, TestHydrator};
use std::time::Duration;

/// The gate's cadence (`tools/test-runner/src/lib.rs` CADENCE).
const CADENCE: [(&str, &str); 3] = [("RDM_STALENESS_MS", "3000"), ("RDM_GOSSIP_INTERVAL_MS", "500"), ("RDM_BACKBONE_INTERVAL_MS", "500")];
/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);

/// The start steps, in the order every start path runs them.
const STEPS: [&str; 5] = [
    "rdm.mesh.node.update.via-join-admitted",
    "rdm.mesh.node.update.via-membership-joined",
    "rdm.mesh.node.update.via-topology-taken",
    "rdm.mesh.node.update.via-hydrate-attempt",
    "rdm.mesh.node.update.via-ready",
];
const FIRST_START: &str = "rdm.mesh.node.create.via-boot";
const AFTER_STOP: &str = "rdm.mesh.node.update.via-start";

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

/// A hook that passes at once: every start runs exactly one attempt.
fn passing() -> Hydration {
    Hydration::new(TestHydrator::new(Mode::Fixed(HookOutcome::Ok)))
}

/// A launch of `name` by `root`, its data dir holding the runtime record its provider would write.
fn launch_by(root: &Running, name: &str, dir: &std::path::Path, listeners: Vec<(String, std::net::SocketAddr)>) -> Launch {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    RuntimeFact::of_this_process("cell").unwrap().write_record(dir).unwrap();
    Launch {
        fabric: "fabric1".into(),
        fabric_id: d.fabric_id.clone(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners,
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.to_path_buf(),
        store_dir: dir.to_path_buf(),
        mesh_id: d.mesh_id.clone(),
        mesh_issuer: None,
    }
}

/// What `root` verifies the join of `launch`'s birth against: at its first start (`held: false`), or
/// at its start after a stop (`held: true`, the name it last accepted for the birth).
fn deployed(launch: &Launch, held: Option<&PathName>) -> Deployed {
    let key = rafka_node_rpc_testkit::node::load_or_mint_key(&launch.data_dir).unwrap();
    Deployed {
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rafka_mesh_entity::EndpointId(key.public().to_string()),
        runtime: rafka_mesh_entity::runtime::await_own_record(&launch.data_dir, Duration::from_secs(10)).unwrap(),
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

fn stop(launch: &Launch) -> StatusRequest {
    StatusRequest::StopNode { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), build_id: "bld_start_sequence".into(), attempt: 1, operation: format!("stop-node:{}", launch.name) }
}

fn start(launch: &Launch) -> StatusRequest {
    StatusRequest::StartNode { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), build_id: "bld_start_sequence".into(), attempt: 2, operation: format!("start-node:{}", launch.name) }
}

/// The start steps of `node` that are children of a span named `root`, in the order they began.
fn steps_under(spans: &crate::common::Spans, node: &str, root: &str) -> Vec<String> {
    let held = spans.0.lock().unwrap();
    let mut found: Vec<(u64, String)> = held
        .iter()
        .filter(|(_, (name, parent, fields))| STEPS.contains(&name.as_str()) && parent.as_deref() == Some(root) && fields.get("node").map(String::as_str) == Some(node))
        .map(|(seq, (name, _, _))| (*seq, name.clone()))
        .collect();
    found.sort();
    found.into_iter().map(|(_, n)| n).collect()
}

#[test]
fn every_start_path_runs_the_same_start_steps_in_the_same_order() {
    if crate::own_process::delegated(module_path!(), "every_start_path_runs_the_same_start_steps_in_the_same_order") {
        return;
    }
    for (k, v) in CADENCE {
        std::env::set_var(k, v);
    }
    let cap = crate::common::capture("every_start_path_runs_the_same_start_steps_in_the_same_order");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("start-sequence-{}", NodeId::mint()));
        let (root_dir, admin_dir, rpc_dir) = (base.join("admin1"), base.join("admin2"), base.join("rpc1"));
        std::fs::create_dir_all(&root_dir).unwrap();

        // The authority: a Day-0 node-admin. Its accepted Build names only itself, and a reconciling
        // root would retire the nodes this cell launches in its own process: it does not reconcile.
        let root = start_with(day0_config(&root_dir), Wiring { hydration: Hydration::none(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling();
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        let root_id = root.digest.lock().unwrap().node.node_id.clone();

        // A node-admin's first start: launched by the root.
        let admin_launch = launch_by(&root, "mesh1.admin.2", &admin_dir, vec![("control".into(), "127.0.0.1:0".parse().unwrap())]);
        let _ = root.runner.joins.expect(deployed(&admin_launch, None));
        let env = admin_launch.to_env();
        let admin_cfg = AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment");
        let admin = start_with(admin_cfg, Wiring { hydration: passing(), fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the launched admin comes up");
        until_ready("mesh1.admin.2 (first start)", || admin.digest.lock().unwrap().clone()).await;

        // An rpc node's first start: launched by the root.
        let rpc_launch = launch_by(&root, "mesh1.rpc.1", &rpc_dir, vec![]);
        let _ = root.runner.joins.expect(deployed(&rpc_launch, None));
        let rpc = rafka_node_rpc_testkit::node::start_hydrating(&rpc_launch, passing(), |b, _| b).await.expect("the rpc node comes up");
        until_ready("mesh1.rpc.1 (first start)", || {
            let mut d = rpc.digest.clone();
            d.status = *rpc.status.lock().unwrap();
            d
        })
        .await;

        // The node-admin's stop and its start after the stop, commanded by the root.
        let own = admin.node_self();
        assert!(matches!(own.serve(Some(&root_id), None, &stop(&admin_launch)).await, Some(StatusReply::Stopped { .. })), "the launched admin stops");
        let _ = root.runner.joins.expect(deployed(&admin_launch, Some(&admin_launch.name)));
        let started = tokio::time::timeout(WRONG, own.serve(Some(&root_id), None, &start(&admin_launch))).await.expect("the admin's start answers");
        assert_eq!(started, Some(StatusReply::Started), "the launched admin starts after its stop");
        until_ready("mesh1.admin.2 (start after stop)", || admin.digest.lock().unwrap().clone()).await;

        // The rpc node's stop and its start after the stop, commanded by the root.
        assert!(matches!(rpc.own.serve(Some(&root_id), None, &stop(&rpc_launch)).await, Some(StatusReply::Stopped { .. })), "the rpc node stops");
        let _ = root.runner.joins.expect(deployed(&rpc_launch, Some(&rpc_launch.name)));
        let started = tokio::time::timeout(WRONG, rpc.own.serve(Some(&root_id), None, &start(&rpc_launch))).await.expect("the rpc node's start answers");
        assert_eq!(started, Some(StatusReply::Started), "the rpc node starts after its stop");

        let paths = [
            ("node-admin first start", steps_under(&spans, "mesh1.admin.2", FIRST_START)),
            ("node-admin start after stop", steps_under(&spans, "mesh1.admin.2", AFTER_STOP)),
            ("rpc node first start", steps_under(&spans, "mesh1.rpc.1", FIRST_START)),
            ("rpc node start after stop", steps_under(&spans, "mesh1.rpc.1", AFTER_STOP)),
        ];
        let report: Vec<String> = paths.iter().map(|(path, steps)| format!("{path}: {steps:?}")).collect();
        for (path, steps) in &paths {
            assert_eq!(steps, &STEPS.map(String::from).to_vec(), "{path} runs the start steps of every start path, in their order, under its start's root; all four:\n{}", report.join("\n"));
        }

        drop(rpc);
        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
