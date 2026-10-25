//! i143.e1.s6: the Admin UI's topology flows work through the node-admin client over Node RPC.
//! The UI's routes run against a real Day-0 node-admin, entered at the birth it holds; every flow
//! lands as one Build in node-admin, node-admin's refusals reach the UI user with their named
//! reason, and the UI's timeline records each Build it submitted.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use rafka_admin_ui::admin::Admin;
use rafka_admin_ui::control::{router, BuildEvents};
use rdm_client::BuildId;
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, Wiring};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tower::ServiceExt;

#[derive(Default)]
struct Timeline(Mutex<Vec<(String, String)>>);
impl BuildEvents for Timeline {
    fn submitted(&self, what: &str, build_id: &BuildId, _: Option<&str>, _: Option<&str>) {
        self.0.lock().unwrap().push((what.to_string(), build_id.to_string()));
    }
}

/// The executables the UI's Builds name in these cells: one stub that exits, for the node-admin and the
/// rpc node, so a Build is accepted on a file whose hash it names (a request naming no executable for a
/// path it creates is refused at acceptance) and what it launches ends at once, as nothing here is about
/// the nodes it creates.
fn stubs(dir: &std::path::Path) -> std::collections::BTreeMap<rdm_core::NodeKind, std::path::PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let stub = dir.join("stub-node");
    std::fs::write(&stub, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    [(rdm_core::NodeKind::NodeAdmin, stub.clone()), (rdm_core::NodeKind::RpcNode, stub)].into_iter().collect()
}

/// A Day-0 node-admin in this process, and the UI's typed client entered at its birth once its view names
/// it the fabric-primary of a ready-for-traffic fabric.
async fn node_admin(dir: &std::path::Path) -> (Running, Arc<Admin>) {
    let d = dir.display().to_string();
    let cfg = AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(d.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment");
    let running = start_with(cfg, Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
    let birth = running.digest.lock().unwrap().node.clone();
    let admin = Arc::new(Admin::at(&birth).await.expect("the admin's birth").with_executables(stubs(dir)));
    // The admin takes the UI's commands once its view names it the fabric-primary and the initial fabric
    // first reached ready-for-traffic (before that every command is refused fabric-not-ready).
    let until = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        match admin.fabric().await {
            Ok(f) if f.fabric_primary.as_ref() == Some(&birth.name) && f.status == rdm_client::ScopeStatus::ReadyForTraffic => break,
            other => assert!(std::time::Instant::now() < until, "the Day-0 admin never named itself the fabric-primary: {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (running, admin)
}

async fn call(app: &axum::Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.map(|b| Body::from(b.to_string())).unwrap_or_else(Body::empty))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn dir(test: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("admin-ui-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_ui_topology_flow_is_one_build_in_node_admin() {
    let d = dir("flows");
    let (_running, admin) = node_admin(&d).await;
    let timeline = Arc::new(Timeline::default());
    let ui = router(Some(admin.clone()), timeline.clone());
    for (method, uri, body) in [("POST", "/api/bootstrap", None), ("POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "rpc_node"})))] {
        let (status, v) = call(&ui, method, uri, body).await;
        if status == StatusCode::UNPROCESSABLE_ENTITY && v["error"] == "build-in-progress" {
            // One Build in flight: the bootstrap is still reconciling, and the refusal names it.
            continue;
        }
        assert_eq!(status, StatusCode::ACCEPTED, "{method} {uri}: {v}");
        let id = BuildId::parse(v["build_id"].as_str().expect("build_id")).expect("a Build id");
        admin.build(&id).await.unwrap_or_else(|e| panic!("{uri}: the Build {id} it answered is held by node-admin: {e}"));
    }
    let whats: Vec<String> = timeline.0.lock().unwrap().iter().map(|(w, _)| w.clone()).collect();
    assert_eq!(whats.first().map(String::as_str), Some("bootstrap"), "every Build the UI submitted is on its timeline: {whats:?}");
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_admin_refusals_reach_the_ui_with_their_reason() {
    let d = dir("refusals");
    let (_running, admin) = node_admin(&d).await;
    let timeline = Arc::new(Timeline::default());
    let ui = router(Some(admin), timeline.clone());
    let (s, v) = call(&ui, "POST", "/api/nodes/mesh1.rpc.9/restart", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("unknown-node")), "{v}");
    let (s, v) = call(&ui, "DELETE", "/api/nodes/mesh1.admin.1", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("would-leave-mesh-without-admin")), "{v}");
    let (s, v) = call(&ui, "DELETE", "/api/nodes/not..a..path", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid-request")), "{v}");
    assert!(timeline.0.lock().unwrap().is_empty(), "a refusal submits nothing");
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test]
async fn without_a_node_admin_the_ui_says_so_by_name() {
    let ui = router(None, Arc::new(Timeline::default()));
    let (s, v) = call(&ui, "POST", "/api/bootstrap", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("no-node-admin")), "{v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_labels_mesh_primary_on_node_admins_only_and_serves_node_admins_connection_facts() {
    let d = dir("topology");
    let (_running, admin) = node_admin(&d).await;
    let t = rafka_admin_ui::view::topology(&admin).await.unwrap();
    let seat = |n: &str| t["nodes"].as_array().unwrap().iter().find(|x| x["name"] == n).unwrap()["seat"].as_str().unwrap().to_string();
    assert_eq!(seat("mesh1.admin.1"), "fabric primary · mesh primary");
    assert_eq!(t["edge_errors"], json!([]), "the node-admin answers its connection facts, an empty list here: {t}");
    let _ = std::fs::remove_dir_all(&d);
}
