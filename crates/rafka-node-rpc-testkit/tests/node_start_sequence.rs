//! node.start is one workflow (lifecycles/node-start.md, "End-to-end sequence"; lifecycles/node-create.md:
//! create composes the whole node.start with FirstStart, a start after a stop runs it with AfterStop).
//!
//! The design (node-start.md steps 2-5): join the accepting authority and consume its cert and
//! rafka-time answer, join the mesh channel, then node.joined; run `hydrate_before_ready` (the app
//! "reads no topology snapshot as a prerequisite"); obtain complete topology through
//! node.topology.get; fetch Build facts only when the node is below the authority's floor, "after
//! the hook and before node.started"; "Refresh rafka-time **last**, so it is current when the node
//! re-enters"; emit node.started.
//!
//! CONTRACT: the four start paths run the same seven start steps in that order, each a child of its
//! start's root span (`rdm.mesh.node.create.via-boot` for a first start,
//! `rdm.mesh.node.update.via-start` for a start after a stop), and each step begins only after the
//! step before it has closed: a node-admin's first start (launched by another admin), the same
//! node-admin's start after its stop, an rpc node's first start and the same rpc node's start after
//! its stop. The steps are the join (`via-join-admitted`), the mesh channel, node.joined
//! (`via-membership-joined`), the `hydrate_before_ready` attempt (`via-hydrate-attempt`), the
//! topology (`via-topology-taken`), the Build floor (`via-build-floor`, run on every start: an rpc
//! node holds no Build log and owes none), the rafka-time refresh (`via-rafka-time-refreshed`) and
//! the report (`via-ready`). What must NOT happen: a topology read before the hook returned, a
//! rafka-time adopted after the topology and before the floor and never again, a start with no
//! floor step, or a start path that steps the sequence on its own, so that a step is missing from
//! its trace, runs outside its start's root, overlaps the step before it, or comes in another order
//! than in the other paths.
//!
//! The authority is a real Day-0 node-admin in this process; it launches the second admin and the rpc
//! node, and commands their stops and starts over Node RPC, so each start after a stop joins it back
//! through its call (node-start.md step 2). Its own process: the cell holds process-wide state (the
//! parking flag).

use rdm_core::launch::{Launch, Launcher};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{IncarnationId, MemberStatus, MeshDigest, NodeId, PathName};
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::app_hydration::{HookOutcome, Hydration};
use rdm_admin::join::Deployed;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::status::{StatusReply, StatusRequest};
use rafka_node_rpc_testkit::hydrate_probe::{Mode, TestHydrator};
use std::time::Duration;

/// What a step that ends at once on a correct node is given before the cell fails by name.
const WRONG: Duration = Duration::from_secs(60);

/// The start steps, in the order every start path runs them (node-start.md steps 2-5).
const STEPS: [&str; 7] = [
    "rdm.mesh.node.update.via-join-admitted",
    "rdm.mesh.node.update.via-membership-joined",
    "rdm.mesh.node.update.via-hydrate-attempt",
    "rdm.mesh.node.update.via-topology-taken",
    "rdm.mesh.node.update.via-build-floor",
    "rdm.mesh.node.update.via-rafka-time-refreshed",
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
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir).unwrap();
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
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
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

fn stop(launch: &Launch) -> StatusRequest {
    StatusRequest::StopNode { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), build_id: "am93paxn71zh".into(), attempt: 1, operation: format!("node.stop:{}", launch.name) }
}

/// `req` sent by `root` over Node RPC to the birth `launch` names, as a commanding node-admin sends
/// it: the commanded node holds the call's endpoint key, which its start joins back through.
async fn command(root: &Running, launch: &Launch, req: &StatusRequest) -> StatusReply {
    let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: WRONG }, ..Default::default() };
    match root.node_rpc.client.call::<rdm_core::status::Status>(&rdm_transport::NodeTarget::ExactNode(launch.node_id.clone()), req, &opts).await.0 {
        rdm_core::outcome::RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} to {} did not end in a reply: {other:?}", req.op(), launch.name),
    }
}

fn start(launch: &Launch) -> StatusRequest {
    StatusRequest::StartNode { node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone(), build_id: "am93paxn71zh".into(), attempt: 2, operation: format!("node.start:{}", launch.name) }
}

/// The start steps of `node` that are children of a span named `root`, in the order they began:
/// each with the sequence number of its begin and of its close (`None` while it is open).
fn steps_under(spans: &crate::common::Spans, node: &str, root: &str) -> Vec<(String, u64, Option<u64>)> {
    let held = spans.0.lock().unwrap();
    let closed = spans.3.lock().unwrap();
    let mut found: Vec<(u64, String)> = held
        .iter()
        .filter(|(_, (name, parent, fields))| STEPS.contains(&name.as_str()) && parent.as_deref() == Some(root) && fields.get("node").map(String::as_str) == Some(node))
        .map(|(seq, (name, _, _))| (*seq, name.clone()))
        .collect();
    found.sort();
    found.into_iter().map(|(seq, n)| (n, seq, closed.get(&seq).copied())).collect()
}

/// Where `steps` breaks the sequence: a step that began before the step ahead of it closed.
fn overlaps(steps: &[(String, u64, Option<u64>)]) -> Vec<String> {
    steps
        .windows(2)
        .filter(|w| w[0].2.is_none_or(|closed| w[1].1 < closed))
        .map(|w| format!("{} began (#{}) before {} closed ({})", w[1].0, w[1].1, w[0].0, w[0].2.map_or("never".to_string(), |c| format!("#{c}"))))
        .collect()
}

#[test]
fn every_start_path_runs_the_same_start_steps_in_the_same_order() {
    if crate::own_process::delegated(module_path!(), "every_start_path_runs_the_same_start_steps_in_the_same_order") {
        return;
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

        // The node-admin's stop and its start after the stop, commanded by the root over Node RPC.
        assert!(matches!(command(&root, &admin_launch, &stop(&admin_launch)).await, StatusReply::Stopped { .. }), "the launched admin stops");
        let _ = root.runner.joins.expect(deployed(&admin_launch, Some(&admin_launch.name)));
        let started = tokio::time::timeout(WRONG, command(&root, &admin_launch, &start(&admin_launch))).await.expect("the admin's start answers");
        assert_eq!(started, StatusReply::Started, "the launched admin starts after its stop");
        until_ready("mesh1.admin.2 (start after stop)", || admin.digest.lock().unwrap().clone()).await;

        // The rpc node's stop and its start after the stop, commanded by the root over Node RPC.
        assert!(matches!(command(&root, &rpc_launch, &stop(&rpc_launch)).await, StatusReply::Stopped { .. }), "the rpc node stops");
        let _ = root.runner.joins.expect(deployed(&rpc_launch, Some(&rpc_launch.name)));
        let started = tokio::time::timeout(WRONG, command(&root, &rpc_launch, &start(&rpc_launch))).await.expect("the rpc node's start answers");
        assert_eq!(started, StatusReply::Started, "the rpc node starts after its stop");

        let paths = [
            ("node-admin first start", steps_under(&spans, "mesh1.admin.2", FIRST_START)),
            ("node-admin start after stop", steps_under(&spans, "mesh1.admin.2", AFTER_STOP)),
            ("rpc node first start", steps_under(&spans, "mesh1.rpc.1", FIRST_START)),
            ("rpc node start after stop", steps_under(&spans, "mesh1.rpc.1", AFTER_STOP)),
        ];
        let report: Vec<String> = paths.iter().map(|(path, steps)| format!("{path}: {steps:?}")).collect();
        for (path, steps) in &paths {
            let names: Vec<String> = steps.iter().map(|(n, _, _)| n.clone()).collect();
            assert_eq!(names, STEPS.map(String::from).to_vec(), "{path} runs the start steps of every start path, in their order, under its start's root; all four:\n{}", report.join("\n"));
            assert_eq!(overlaps(steps), Vec::<String>::new(), "{path} begins each start step only after the step before it closed; all four:\n{}", report.join("\n"));
        }

        drop(rpc);
        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
