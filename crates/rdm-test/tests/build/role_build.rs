//! i143.e11.s3 process E2E: the role binaries on the base are born by node-admin as
//! `NodeKind::{Broker, Gateway, Compute}` path.names (PRD §13.1).
//!
//! One fresh fabric; one `build.create` asks for `{node_admin: 1, broker: 1, gateway: 1,
//! compute: 1}`; from public surfaces only:
//! - the Build completes and every desired node is `ready` under its kind;
//! - every node type of each mesh has exactly one primary; the fabric exactly one fabric primary;
//! - every role's boot and ready spans name its kind, and each boot runs under the deployment
//!   step that launched it;
//! - a second Build without the compute deletes `mesh1.compute.1` under that Build, and the
//!   other roles keep their birth.

use crate::build_log;
use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, NodeView};
use rdm_test::estate::{wait_for, descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-shapes".into(),
        subfeature: "role-build".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn kind_of(kind: &str) -> NodeKind {
    match kind {
        "broker" => NodeKind::Broker,
        "gateway" => NodeKind::Gateway,
        "compute" => NodeKind::Compute,
        other => panic!("{other} is not a role kind"),
    }
}

/// `(kind, count)` of mesh1 beside its one node-admin.
async fn build(estate: &Estate, label: &str, roles: &[(&str, u32)]) -> (String, Vec<Value>) {
    let mesh = MeshDesired::of("mesh1", std::iter::once((NodeKind::NodeAdmin, 1)).chain(roles.iter().map(|(kind, n)| (kind_of(kind), *n))));
    let build_id = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh] }).await.build_id.to_string();
    estate.artifact(&format!("build-{label}.json"), &build_log::build_value(&estate, &build_id).await);
    let mut want: BTreeSet<String> = ["mesh1.admin.1".to_string()].into();
    for (kind, n) in roles {
        want.extend((1..=*n).map(|i| format!("mesh1.{kind}.{i}")));
    }
    let nodes = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(15)).await);
    for n in &nodes {
        let name = n["name"].as_str().unwrap();
        let kind = name.split('.').nth(1).unwrap();
        let kind = if kind == "admin" { "node_admin" } else { kind };
        assert_eq!(n["kind"], kind, "{label}: {name} is born under its kind: {n}");
        assert_eq!(n["status"], "ready", "{label}: {n}");
    }
    (build_id, nodes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn roles_are_born_by_build_under_their_kind() {
    let mut estate = Estate::bootstrap(owner("roles_are_born_by_build_under_their_kind"), "fabric1", "mesh1").await;
    let roles = [("broker", 1), ("gateway", 1), ("compute", 1)];
    let (first, nodes) = build(&estate, "roles", &roles).await;
    estate.artifact("nodes.json", &json!(nodes));

    // One primary per node type (a node type of one is its own primary); one fabric primary, the admin.
    let mut primaries: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for n in &nodes {
        let e = primaries.entry(n["kind"].as_str().unwrap().to_string()).or_default();
        if n["is_primary"] == true {
            e.push(n["name"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(primaries.keys().cloned().collect::<Vec<_>>(), ["broker", "compute", "gateway", "node_admin"]);
    for (node_type, p) in &primaries {
        assert_eq!(p.len(), 1, "node type {node_type} has primaries {p:?}");
    }
    let fabric_primaries: Vec<&Value> = nodes.iter().filter(|n| n["is_fabric_primary"] == true).collect();
    assert_eq!(fabric_primaries.len(), 1, "{fabric_primaries:?}");
    assert_eq!(fabric_primaries[0]["kind"], "node_admin");

    // Every node, whatever its kind, publishes its own CPU and RAM in its mesh digest, and the
    // node-admin's view serves it. The load travels on the node's own digest only (the primary's
    // mesh topology omits loads, rafka-mesh-transport topology.rs `publish`), so the view serves it once
    // this admin has heard that digest, which can follow the status the mesh topology carried.
    let names: BTreeSet<String> = nodes.iter().map(|n| n["name"].as_str().unwrap().to_string()).collect();
    let loaded: Vec<Value> = wait_for("every node's own digest, with its load, in the admin's view", Duration::from_secs(15), || async {
        let now = Estate::values(&estate.settles_to_named(&names, Duration::from_secs(15)).await);
        now.iter().all(|n| n["load"]["ram_used_bytes"].as_u64().is_some_and(|b| b > 0)).then_some(now)
    })
    .await;
    for n in &loaded {
        let l = &n["load"];
        assert!(l["ram_used_bytes"].as_u64().is_some_and(|b| b > 0), "{} carries its resident memory: {n}", n["name"]);
        assert!(l["ram_budget_bytes"].as_u64() >= l["ram_used_bytes"].as_u64(), "{} memory ceiling: {n}", n["name"]);
        assert!(l["cpu_budget_millicores"].as_u64().is_some_and(|c| c >= 1000), "{} cpu ceiling: {n}", n["name"]);
        assert!(l["cpu_used_millicores"].as_u64().is_some(), "{} cpu in use: {n}", n["name"]);
    }

    // The same digest carries the node's mesh-channel counts (R-L2): it has heard the members of
    // its mesh, holds a gossip neighbour, and has sent and decoded topology frames.
    // A digest is a periodic sample: the Build is ready on declarations, before every member has
    // published a sample taken after the others joined. Read `node.get` until the samples are in.
    let sampled = |ns: &[NodeView]| ns.iter().all(|n| n.gossip.as_ref().is_some_and(|g| g.neighbours >= 1 && g.frames_received >= 1 && g.frames_sent >= 1));
    let gossip_nodes = Estate::values(&estate.until_view("every node published a digest sampled after the others joined", Duration::from_secs(15), |views| sampled(views).then(|| views.to_vec())).await);
    estate.artifact("nodes-gossip.json", &json!(gossip_nodes));
    for n in &gossip_nodes {
        let g = &n["gossip"];
        assert!(g["heard"].as_u64().is_some_and(|h| h >= 1), "{} has heard members: {n}", n["name"]);
        assert!(g["neighbours"].as_u64().is_some_and(|k| k >= 1), "{} has an active neighbour: {n}", n["name"]);
        assert!(g["frames_sent"].as_u64().is_some_and(|f| f >= 1), "{} sent frames: {n}", n["name"]);
        assert!(g["frames_received"].as_u64().is_some_and(|f| f >= 1), "{} decoded frames: {n}", n["name"]);
    }

    // The second Build drops the compute: its path is deleted, the others keep their birth.
    let births = |ns: &[Value]| -> BTreeMap<String, String> {
        ns.iter().map(|n| (n["name"].as_str().unwrap().to_string(), n["incarnation_id"].as_str().unwrap_or("").to_string())).collect()
    };
    let before = births(&nodes);
    // The initial fabric refuses a shrink until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let (second, after) = build(&estate, "without-compute", &[("broker", 1), ("gateway", 1)]).await;
    let after = births(&after);
    assert!(!after.contains_key("mesh1.compute.1"));
    for (name, birth) in &after {
        assert_eq!(before.get(name), Some(birth), "{name} was restarted by the shrink");
    }

    // Evidence, by parent span id, after every process flushed.
    estate.stop().await;
    let spans = estate.spans();
    let accepted = |id: &str| {
        named(&spans, "rdm.node_admin.build.create.via-node-rpc")
            .into_iter()
            .find(|s| s["attributes"]["build_id"] == id)
            .unwrap_or_else(|| panic!("no span accepted build {id}"))
            .clone()
    };
    let (first_accepted, second_accepted) = (accepted(&first), accepted(&second));
    for (kind, _) in &roles {
        let node = format!("mesh1.{kind}.1");
        let step = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .find(|s| s["attributes"]["node"] == node.as_str() && s["attributes"]["step"] == "DeployRuntime" && s["attributes"]["build_id"] == first.as_str())
            .unwrap_or_else(|| panic!("no DeployRuntime step for {node}"))
            .clone();
        assert!(descends_from(&spans, &step, &first_accepted), "{node}'s deployment runs under the Build that asked for it");
        let boot = named(&spans, "rdm.mesh.node.create.via-deployment")
            .into_iter()
            .find(|s| s["attributes"]["node"] == node.as_str())
            .unwrap_or_else(|| panic!("{node} wrote no boot span"))
            .clone();
        assert_eq!(boot["attributes"]["kind"], *kind, "{node}'s boot names its kind: {boot}");
        assert!(descends_from(&spans, &boot, &step), "{node} booted under the step that launched it");
        let ready = named(&spans, "rdm.mesh.node.update.via-ready")
            .into_iter()
            .find(|s| s["attributes"]["node"] == node.as_str())
            .unwrap_or_else(|| panic!("{node} wrote no ready span"))
            .clone();
        assert_eq!(ready["attributes"]["kind"], *kind, "{node}'s ready span names its kind: {ready}");
    }
    let delete = named(&spans, "rdm.node_admin.node.delete.via-build")
        .into_iter()
        .find(|s| s["attributes"]["node"] == "mesh1.compute.1" && s["attributes"]["build_id"] == second.as_str())
        .unwrap_or_else(|| panic!("no removal of mesh1.compute.1 under build {second}"))
        .clone();
    assert!(descends_from(&spans, &delete, &second_accepted), "the compute's removal descends from the second Build");
    estate.record_trace_url(first_accepted["trace_id"].as_str().unwrap_or(""));
}

/// The one node executable of node-admin's built-in set: every node_type but `node_admin` runs it.
const NODE_EXECUTABLE: &str = "rdm-node";

/// CONTRACT: with no explicit executable binding, node-admin's process provider launches its
/// built-in set, which is two executables: the node-admin and the one node executable. A broker, a
/// gateway and a compute born by one Build each run that one executable, and each boots as the
/// node_type its launch names: the boot span (`rdm.mesh.node.create.via-deployment`) names the
/// executable the process runs and the kind it took from its launch, and runs under the
/// DeployRuntime step that launched it. No product role binary is launched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_with_no_executable_binding_launches_every_role_from_the_one_node_executable() {
    let mut estate = Estate::bootstrap(owner("a_build_with_no_executable_binding_launches_every_role_from_the_one_node_executable"), "fabric1", "mesh1").await;
    let roles = [("broker", 1), ("gateway", 1), ("compute", 1)];
    let (build_id, nodes) = build(&estate, "roles", &roles).await;
    estate.artifact("nodes.json", &json!(nodes));
    estate.stop().await;

    let spans = estate.spans();
    let accepted = named(&spans, "rdm.node_admin.build.create.via-node-rpc")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == build_id.as_str())
        .unwrap_or_else(|| panic!("no span accepted build {build_id}"))
        .clone();
    let mut launched = BTreeMap::new();
    for (kind, _) in &roles {
        let node = format!("mesh1.{kind}.1");
        let step = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .find(|s| s["attributes"]["node"] == node.as_str() && s["attributes"]["step"] == "DeployRuntime" && s["attributes"]["build_id"] == build_id.as_str())
            .unwrap_or_else(|| panic!("no DeployRuntime step for {node}"))
            .clone();
        assert!(descends_from(&spans, &step, &accepted), "{node}'s launch runs under the Build that asked for it");
        let boot = named(&spans, "rdm.mesh.node.create.via-deployment")
            .into_iter()
            .find(|s| s["attributes"]["node"] == node.as_str())
            .unwrap_or_else(|| panic!("{node} wrote no boot span"))
            .clone();
        assert!(descends_from(&spans, &boot, &step), "{node} booted under the DeployRuntime step that launched it");
        assert_eq!(boot["attributes"]["executable"], NODE_EXECUTABLE, "{node} runs the one node executable of the built-in set: {boot}");
        assert_eq!(boot["attributes"]["kind"], *kind, "{node} booted as the node_type its launch names: {boot}");
        launched.insert(node, (boot["attributes"]["executable"].clone(), boot["attributes"]["kind"].clone(), boot["trace_id"].clone()));
    }
    estate.artifact("launched.json", &json!(launched));
    estate.record_trace_url(accepted["trace_id"].as_str().unwrap_or(""));
}
