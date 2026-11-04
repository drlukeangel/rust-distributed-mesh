//! i143.e3.s3 process E2E: the SN, MN and MM proof shapes (PRD §1.12–13,
//! §10) converge through Build alone.
//!
//! Each shape runs on a fresh fabric: the first `rdm-node-admin` boots it,
//! one `build.create` asks for the shape, and the test then checks, from
//! the typed objects and the evidence only, that:
//! - the Build completes and every desired node is `ready`;
//! - every node type of each mesh has exactly one `is_primary` and the fabric exactly one
//!   `is_fabric_primary`, a node-admin that is its own mesh's admin primary;
//! - the evidence links Build -> deployment -> lifecycle by parent span id,
//!   across processes: every node this Build created booted under the
//!   `DeployRuntime` step that launched it, which runs under a section run of
//!   the Build (`build.update.via-reconcile`; an MM's second mesh's members run
//!   on that mesh's own primary), under a fabric-primary's drive of the Build,
//!   the first section in the trace of the request that accepted it; and every
//!   node's `Pending -> Ready` transition descends from a section run too.
//!
//! The shapes run one after another in one test, each on its own fabric.

use crate::build_log;
use rdm_client::{FabricDesired, MeshDesired};
use rdm_test::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

fn owner(shape: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-shapes".into(),
        subfeature: "shape-reconcile".into(),
        rung: shape.to_uppercase(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: format!("shape_{shape}_converges_through_build"),
    }
}

/// `(mesh, node_admin, rpc_node)` per mesh.
async fn converge(shape: &str, meshes: &[(&str, u32, u32)]) {
    let estate = Estate::bootstrap(owner(shape), "fabric1", "mesh1").await;
    let desired = FabricDesired { fabric: "fabric1".into(), meshes: meshes.iter().map(|(m, a, r)| MeshDesired::of(*m, [("node-admin", *a), ("node", *r)])).collect() };
    let build_id = estate.resize(&desired).await.build_id.to_string();
    let build = build_log::build_value(&estate, &build_id).await;
    estate.artifact("build.json", &build);

    // Every desired node is live and ready; nothing else exists.
    let mut want: Vec<rdm_core::PathName> = Vec::new();
    for (m, a, r) in meshes {
        want.extend((1..=*a).map(|i| rdm_core::PathName::new(format!("{m}.node-admin"), i.to_string()).unwrap()));
        want.extend((1..=*r).map(|i| rdm_core::PathName::new(format!("{m}.node"), i.to_string()).unwrap()));
    }
    want.sort();
    let views = estate.settles_to_named(&want.iter().cloned().collect(), Duration::from_secs(15)).await;
    let nodes = Estate::values(&views);
    estate.artifact("nodes.json", &json!(nodes));

    // One primary per node type, one fabric primary (an admin primary).
    let mut primaries: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for n in &nodes {
        let r#type = (rdm_test::estate::mesh_of(&n).as_str().unwrap().to_string(), n["type"].as_str().unwrap().to_string());
        let e = primaries.entry(r#type).or_default();
        if n["is_primary"] == true {
            e.push(n["node_id"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(primaries.len(), meshes.len() * 2, "{shape}: every declared node type is present");
    for (r#type, p) in &primaries {
        assert_eq!(p.len(), 1, "{shape}: node type {:?} has primaries {p:?}", r#type);
    }
    let fabric_primaries: Vec<&Value> = nodes.iter().filter(|n| n["is_fabric_primary"] == true).collect();
    assert_eq!(fabric_primaries.len(), 1, "{shape}: fabric primaries {fabric_primaries:?}");
    let fp = fabric_primaries[0];
    assert_eq!((fp["type"].as_str(), fp["is_primary"].as_bool()), (Some("node-admin"), Some(true)), "{shape}: {fp}");
    let fabric = serde_json::to_value(estate.fabric_view().await).expect("a view serializes");
    assert_eq!(fabric["fabric_primary"], fp["node_id"], "{shape}: {fabric}");
    for m in fabric["meshes"].as_array().unwrap() {
        assert!(m["admin_api_base"].as_str().is_some_and(|b| !b.is_empty()), "{shape}: mesh {m} names its application contact");
    }

    // Build -> deployment -> lifecycle, by parent span id. Every process
    // flushes its evidence on exit: stop the estate first.
    let mut estate = estate;
    estate.stop().await;
    let spans = estate.spans();
    let accepted_span = named(&spans, "rdm.node_admin.build.create.via-node-rpc")
        .into_iter()
        .find(|s| s["attributes"]["build_id"] == build_id.as_str())
        .unwrap_or_else(|| panic!("{shape}: no span accepted build {build_id}"))
        .clone();
    // The fabric primary runs the section that creates the node-admins; a new mesh's own primary runs
    // a later section for its members. Each pass of the Build drive is its own root, joined by build_id
    // (telemetry.md §4): every section runs under a drive of its Build, and one ends complete.
    let reconciles: Vec<Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .filter(|s| s["attributes"]["build_id"] == build_id.as_str())
        .cloned()
        .collect();
    assert!(reconciles.iter().any(|r| r["attributes"]["outcome"] == "complete"), "{shape}: no section run of {build_id} ended complete");
    let drives: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-drive").into_iter().filter(|d| d["attributes"]["build_id"] == build_id.as_str()).collect();
    assert!(drives.iter().all(|d| d["parent_span_id"].as_str().unwrap_or("").is_empty()), "{shape}: each drive pass is a root: {drives:?}");
    for r in &reconciles {
        assert!(drives.iter().any(|d| descends_from(&spans, r, d)), "{shape}: every section runs under a drive of its Build: {r}");
    }
    let bootstrap_location = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
    let created: Vec<_> = views.iter().filter(|n| n.location() != bootstrap_location).collect();
    for node in created {
        let step = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .find(|s| s["attributes"]["node_path"] == node.path && s["attributes"]["node_name"] == node.name && s["attributes"]["step"] == "DeployRuntime" && s["attributes"]["build_id"] == build_id.as_str())
            .unwrap_or_else(|| panic!("{shape}: no DeployRuntime step for {node:?}"))
            .clone();
        assert!(reconciles.iter().any(|r| descends_from(&spans, &step, r)), "{shape}: {node:?}'s DeployRuntime runs under a reconcile of the Build");
        let boot = named(&spans, "rdm.mesh.node.create.via-deployment")
            .into_iter()
            .find(|s| s["attributes"]["node_id"] == node.node_id.as_str())
            .unwrap_or_else(|| panic!("{shape}: {node:?} wrote no boot span"))
            .clone();
        assert!(descends_from(&spans, &boot, &step), "{shape}: {node:?} booted under the step that launched it");
        let lifecycle = named(&spans, "rdm.node_admin.lifecycle.update.via-transition")
            .into_iter()
            .find(|s| s["attributes"]["target"] == node.node_id.as_str())
            .unwrap_or_else(|| panic!("{shape}: no lifecycle transition for {node:?}"))
            .clone();
        assert!(reconciles.iter().any(|r| descends_from(&spans, &lifecycle, r)), "{shape}: {node:?}'s lifecycle transition descends from a reconcile of the Build");
    }
    estate.record_trace_url(accepted_span["trace_id"].as_str().unwrap_or(""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sn_mn_and_mm_converge_through_build_with_one_primary_per_node_type() {
    converge("sn", &[("mesh1", 1, 1)]).await;
    converge("mn", &[("mesh1", 2, 3)]).await;
    converge("mm", &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
}
