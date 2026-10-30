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
//!   `DeployRuntime` step that launched it, which runs under a reconcile of
//!   the Build (an MM's second mesh's members run on that mesh's own primary,
//!   a later attempt), which descends from the request that accepted it; and
//!   every node's `Pending -> Ready` transition descends from it too.
//!
//! The shapes run one after another in one test, each on its own fabric.

use crate::build_log;
use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired};
use rdm_test::estate::{claim_decider, descends_from, named, Estate, Owner};
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
    let desired = FabricDesired { fabric: "fabric1".into(), meshes: meshes.iter().map(|(m, a, r)| MeshDesired::of(*m, [(NodeKind::NodeAdmin, *a), (NodeKind::RpcNode, *r)])).collect() };
    let build_id = estate.reconcile(&desired).await.build_id.to_string();
    let build = build_log::build_value(&estate.carrier().await, &build_id).await;
    estate.artifact("build.json", &build);

    // Every desired node is live and ready; nothing else exists.
    let mut want: Vec<String> = Vec::new();
    for (m, a, r) in meshes {
        want.extend((1..=*a).map(|i| format!("{m}.admin.{i}")));
        want.extend((1..=*r).map(|i| format!("{m}.rpc.{i}")));
    }
    want.sort();
    let nodes = Estate::values(&estate.settles_to_named(&want.iter().cloned().collect(), Duration::from_secs(15)).await);
    estate.artifact("nodes.json", &json!(nodes));

    // One primary per node type, one fabric primary (an admin primary).
    let mut primaries: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for n in &nodes {
        let node_type = (n["mesh"].as_str().unwrap().to_string(), n["kind"].as_str().unwrap().to_string());
        let e = primaries.entry(node_type).or_default();
        if n["is_primary"] == true {
            e.push(n["name"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(primaries.len(), meshes.len() * 2, "{shape}: every declared node type is present");
    for (node_type, p) in &primaries {
        assert_eq!(p.len(), 1, "{shape}: node type {node_type:?} has primaries {p:?}");
    }
    let fabric_primaries: Vec<&Value> = nodes.iter().filter(|n| n["is_fabric_primary"] == true).collect();
    assert_eq!(fabric_primaries.len(), 1, "{shape}: fabric primaries {fabric_primaries:?}");
    let fp = fabric_primaries[0];
    assert_eq!((fp["kind"].as_str(), fp["is_primary"].as_bool()), (Some("node_admin"), Some(true)), "{shape}: {fp}");
    let fabric = serde_json::to_value(estate.fabric_view().await).expect("a view serializes");
    assert_eq!(fabric["fabric_primary"], fp["name"], "{shape}: {fabric}");
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
    // The fabric primary runs the attempt that creates the node-admins; a
    // new mesh's own primary runs a later attempt for its members. Every
    // attempt descends from the accepting request, and the last converges.
    let reconciles: Vec<Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .filter(|s| s["attributes"]["build_id"] == build_id.as_str())
        .cloned()
        .collect();
    assert!(reconciles.iter().any(|r| r["attributes"]["outcome"] == "converged"), "{shape}: no converged reconcile of {build_id}");
    // The accepting fabric-primary holds the request's context: every attempt it decides descends
    // from the request; an attempt decided after the role moved starts its own trace.
    let accepting = claim_decider(&spans, build_id.as_str(), "1").expect("attempt 1 was claimed");
    for r in &reconciles {
        if claim_decider(&spans, build_id.as_str(), r["attributes"]["attempt"].as_str().unwrap_or_default()).is_none_or(|d| d == accepting) {
            assert!(descends_from(&spans, r, &accepted_span), "{shape}: every reconcile the accepting fabric-primary decided descends from the accepting request: {r}");
        } else {
            assert_eq!(r["parent_span_id"].as_str().unwrap_or(""), "", "{shape}: an attempt decided after the role moved starts its own trace: {r}");
        }
    }
    let created: Vec<&String> = want.iter().filter(|n| *n != "mesh1.admin.1").collect();
    for node in created {
        let step = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .find(|s| s["attributes"]["node"] == node.as_str() && s["attributes"]["step"] == "DeployRuntime" && s["attributes"]["build_id"] == build_id.as_str())
            .unwrap_or_else(|| panic!("{shape}: no DeployRuntime step for {node}"))
            .clone();
        assert!(reconciles.iter().any(|r| descends_from(&spans, &step, r)), "{shape}: {node}'s DeployRuntime runs under a reconcile of the Build");
        let boot = named(&spans, "rdm.mesh.node.create.via-deployment")
            .into_iter()
            .find(|s| s["attributes"]["node"] == node.as_str())
            .unwrap_or_else(|| panic!("{shape}: {node} wrote no boot span"))
            .clone();
        assert!(descends_from(&spans, &boot, &step), "{shape}: {node} booted under the step that launched it");
        let lifecycle = named(&spans, "rdm.node_admin.lifecycle.update.via-transition")
            .into_iter()
            .find(|s| s["attributes"]["target"] == node.as_str())
            .unwrap_or_else(|| panic!("{shape}: no lifecycle transition for {node}"))
            .clone();
        assert!(reconciles.iter().any(|r| descends_from(&spans, &lifecycle, r)), "{shape}: {node}'s lifecycle transition descends from a reconcile of the Build");
    }
    estate.record_trace_url(accepted_span["trace_id"].as_str().unwrap_or(""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sn_mn_and_mm_converge_through_build_with_one_primary_per_node_type() {
    converge("sn", &[("mesh1", 1, 1)]).await;
    converge("mn", &[("mesh1", 2, 3)]).await;
    converge("mm", &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
}
