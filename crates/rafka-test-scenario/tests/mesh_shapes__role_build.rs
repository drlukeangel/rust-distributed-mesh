//! i143.e11.s3 process E2E: the role binaries on the base are born by node-admin as
//! `NodeKind::{Broker, Gateway, Compute}` path.names (PRD §13.1).
//!
//! One fresh fabric; one `POST /api/build` asks for `{node_admin: 1, broker: 1, gateway: 1,
//! compute: 1}`; from public surfaces only:
//! - the Build completes and every desired node is `ready-for-traffic` under its kind;
//! - every role cohort has exactly one primary; the fabric exactly one fabric primary;
//! - every role's boot and ready spans name its kind, and each boot runs under the deployment
//!   step that launched it;
//! - a second Build without the compute retires `mesh1.compute.1` under that Build, and the
//!   other roles keep their birth.

use rafka_test_scenario::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-shapes".into(),
        subfeature: "role-build".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "roles_are_born_by_build_under_their_kind".into(),
    }
}

/// `(kind, count)` of mesh1 beside its one node-admin.
async fn build(estate: &Estate, label: &str, roles: &[(&str, u32)]) -> (String, Vec<Value>) {
    let mut mesh = json!({"name": "mesh1", "node_admin": 1});
    for (kind, n) in roles {
        mesh[*kind] = json!(n);
    }
    let (status, accepted) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh]})).await;
    assert_eq!(status, 202, "{label}: {accepted}");
    let build_id = accepted["build_id"].as_str().unwrap().to_string();
    let build = estate.await_build(&build_id, Duration::from_secs(120)).await;
    estate.artifact(&format!("build-{label}.json"), &build);
    let mut want: BTreeSet<String> = ["mesh1.admin.1".to_string()].into();
    for (kind, n) in roles {
        want.extend((1..=*n).map(|i| format!("mesh1.{kind}.{i}")));
    }
    let nodes = estate.settled(&want, Duration::from_secs(15)).await;
    for n in &nodes {
        let name = n["name"].as_str().unwrap();
        let kind = name.split('.').nth(1).unwrap();
        let kind = if kind == "admin" { "node_admin" } else { kind };
        assert_eq!(n["kind"], kind, "{label}: {name} is born under its kind: {n}");
        assert_eq!(n["status"], "ready-for-traffic", "{label}: {n}");
    }
    (build_id, nodes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn roles_are_born_by_build_under_their_kind() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let roles = [("broker", 1), ("gateway", 1), ("compute", 1)];
    let (first, nodes) = build(&estate, "roles", &roles).await;
    estate.artifact("nodes.json", &json!(nodes));

    // One primary per cohort (a cohort of one is its own primary); one fabric primary, the admin.
    let mut primaries: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for n in &nodes {
        let e = primaries.entry(n["kind"].as_str().unwrap().to_string()).or_default();
        if n["is_primary"] == true {
            e.push(n["name"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(primaries.keys().cloned().collect::<Vec<_>>(), ["broker", "compute", "gateway", "node_admin"]);
    for (cohort, p) in &primaries {
        assert_eq!(p.len(), 1, "cohort {cohort} has primaries {p:?}");
    }
    let fabric_primaries: Vec<&Value> = nodes.iter().filter(|n| n["is_fabric_primary"] == true).collect();
    assert_eq!(fabric_primaries.len(), 1, "{fabric_primaries:?}");
    assert_eq!(fabric_primaries[0]["kind"], "node_admin");

    // The second Build drops the compute: its path is retired, the others keep their birth.
    let births = |ns: &[Value]| -> BTreeMap<String, String> {
        ns.iter().map(|n| (n["name"].as_str().unwrap().to_string(), n["incarnation_id"].as_str().unwrap_or("").to_string())).collect()
    };
    let before = births(&nodes);
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
        named(&spans, "rdm.node_admin.build.create.via-rest")
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
