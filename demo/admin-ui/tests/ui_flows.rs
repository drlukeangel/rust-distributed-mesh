//! i143.e1.s6: the Admin UI's topology flows work through the node-admin client over Node RPC.
//! The UI's routes run against a real Day-0 node-admin, entered at the birth it holds; every flow
//! lands as one Build in node-admin, node-admin's refusals reach the UI user with their named
//! reason, and the UI's timeline records each Build it submitted.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use rdm_admin_ui::admin::Admin;
use rdm_admin_ui::control::{router, BuildEvents};
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
    [(rdm_core::NodeKind::NodeAdmin, stub.clone()), (rdm_core::NodeKind::Gateway, stub.clone()), (rdm_core::NodeKind::Broker, stub)].into_iter().collect()
}

/// A Day-0 node-admin in this process, and the UI's typed client entered at its birth once its view names
/// it the fabric-primary of a ready fabric.
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
    // The admin takes the UI's commands once its view names it the fabric-primary and the Day-0 fabric
    // reached ready (before that the Day-0 Build is in progress).
    let until = std::time::Instant::now() + Duration::from_secs(20);
    let day0 = loop {
        match admin.fabric().await {
            Ok(f) if f.fabric_primary.as_ref() == Some(&birth.name) && f.status == rdm_client::ScopeStatus::Ready => break f.build_id,
            other => assert!(std::time::Instant::now() < until, "the Day-0 admin never named itself the fabric-primary: {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    // The Day-0 Build is Complete from its own ready, one Build at a time: the UI's Builds are
    // submitted once it is.
    if let Some(day0) = day0 {
        until_complete(&admin, "the Day-0 Build", &BuildId::parse(&day0).expect("the Day-0 Build's id")).await;
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
    for (method, uri, body) in [("POST", "/api/bootstrap", None), ("POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "broker"})))] {
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
    let (s, v) = call(&ui, "POST", "/api/nodes/mesh1.broker.9/stop", None).await;
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
    let t = rdm_admin_ui::view::topology(&admin).await.unwrap();
    let role = |n: &str| t["nodes"].as_array().unwrap().iter().find(|x| x["name"] == n).unwrap()["role"].as_str().unwrap().to_string();
    assert_eq!(role("mesh1.admin.1"), "fabric primary · mesh primary");
    assert_eq!(t["connection_errors"], json!([]), "the node-admin answers its connection facts, an empty list here: {t}");
    let _ = std::fs::remove_dir_all(&d);
}

/// How long a Build the UI submitted may take to be Complete: its births' launches and check-ins, and the
/// fabric's change to ready.
const BOUND: Duration = Duration::from_secs(180);

/// Poll `build.get` of `id` until it is Complete within [`BOUND`]; a Failed Build, or one not Complete in
/// time, is the defect, named with what `build.get` and `fabric.get` say then.
async fn until_complete(admin: &Admin, what: &str, id: &BuildId) {
    let until = std::time::Instant::now() + BOUND;
    loop {
        let got = admin.build(id).await;
        match &got {
            Ok(b) if b.phase == rdm_core::build::BuildPhase::Complete => return,
            Ok(b) if b.phase == rdm_core::build::BuildPhase::Failed => panic!("{what}: Build {id} failed: {b:?}"),
            _ => {}
        }
        if std::time::Instant::now() >= until {
            panic!("{what}: Build {id} is not Complete within {BOUND:?}: build.get {got:?}; fabric.get {:?}", admin.fabric().await);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// `POST uri` on the UI, accepted: the Build it answered.
async fn accepted(ui: &axum::Router, uri: &str, body: Option<Value>) -> BuildId {
    let (status, v) = call(ui, "POST", uri, body).await;
    assert_eq!(status, StatusCode::ACCEPTED, "POST {uri}: {v}");
    BuildId::parse(v["build_id"].as_str().expect("build_id")).expect("a Build id")
}

/// The fabric is ready, as `fabric.get` at the UI's admin says.
async fn ready(admin: &Admin, what: &str) {
    let until = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match admin.fabric().await {
            Ok(f) if f.status == rdm_client::ScopeStatus::Ready => return,
            other => assert!(std::time::Instant::now() < until, "{what}: the fabric is not ready: {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// CONTRACT (states.md "Resize and executable replacement": only a resize or an executable replacement puts
/// the fabric back to pending, and a Build that did is Complete once its own ready state is set;
/// Q154, never blocked for ever): the UI's resubmit of the current topology, the Day-0 admin named on the
/// executable its row records (day 0's entry names none), is one Build with nothing to execute, Complete
/// within the bound with the fabric ready. Must NOT happen: that Build Pending at attempt 1
/// with no step for ever (Rafka 141).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resubmitting_the_current_topology_on_the_day0_admin_completes_with_the_fabric_ready() {
    let d = dir("resubmit");
    let (_running, admin) = node_admin(&d).await;
    let timeline = Arc::new(Timeline::default());
    let ui = router(Some(admin.clone()), timeline.clone());
    let id = accepted(&ui, "/api/builds/resubmit", None).await;
    until_complete(&admin, "the resubmitted topology", &id).await;
    ready(&admin, "after the resubmitted topology").await;
    let id = accepted(&ui, "/api/builds/resubmit", None).await;
    until_complete(&admin, "the topology resubmitted again", &id).await;
    ready(&admin, "after the topology resubmitted again").await;
    let whats: Vec<String> = timeline.0.lock().unwrap().iter().map(|(w, _)| w.clone()).collect();
    assert_eq!(whats, ["resubmit topology", "resubmit topology"], "both Builds are on the UI's timeline");
    let _ = std::fs::remove_dir_all(&d);
}

/// The consumer's Day-0 node-admin (`rshape-node-admin`) as its own process, started as
/// scripts/demo-estate.sh starts it, from a copy of the consumer's executables under `root/bin`: every node
/// it launches runs from there, so dropping this stops exactly the estate's processes.
struct ConsumerEstate {
    root: std::path::PathBuf,
    bin: std::path::PathBuf,
}

impl ConsumerEstate {
    /// The consumer's executables, beside this test's own (`<target>/debug/deps/..`), copied under a new
    /// root, and the Day-0 admin started from there; the UI's admin entered at the birth it printed once
    /// it names itself the fabric-primary of a ready fabric.
    async fn start(test: &str) -> (ConsumerEstate, Arc<Admin>) {
        let built = std::env::current_exe().unwrap().parent().and_then(|deps| deps.parent()).map(std::path::Path::to_path_buf).expect("the test's target directory");
        let root = dir(test);
        let bin = root.join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(root.join("store")).unwrap();
        for exe in ["rshape-node-admin", "rshape-gateway"] {
            let from = built.join(exe);
            std::fs::copy(&from, bin.join(exe)).unwrap_or_else(|e| panic!("the consumer's {exe} is built beside this test ({}): {e}", from.display()));
        }
        let estate = ConsumerEstate { root: root.clone(), bin: bin.clone() };
        let log = root.join("admin1.log");
        let mut day0 = std::process::Command::new(bin.join("rshape-node-admin"))
            .envs([
                ("MESH_SPAWN_TYPE", "process".to_string()),
                ("RDM_FABRIC", "fabric1".into()),
                ("RDM_MESH", "mesh1".into()),
                ("RDM_DATA_DIR", root.join("mesh1.admin.1").display().to_string()),
                ("RDM_STORE_DIR", root.join("store").display().to_string()),
                ("RDM_ESTATE_ROOT", root.display().to_string()),
                ("RDM_BIN_DIR", bin.display().to_string()),
                ("RDM_NODE_ADMIN_API_BIND", "127.0.0.1:0".into()),
            ])
            .stdout(std::fs::File::create(&log).unwrap())
            .stderr(std::fs::File::create(root.join("admin1.err")).unwrap())
            .stdin(std::process::Stdio::null())
            .spawn()
            .expect("the consumer's Day-0 node-admin starts");
        let until = std::time::Instant::now() + Duration::from_secs(120);
        let birth = loop {
            let printed = std::fs::read_to_string(&log).unwrap_or_default();
            if let Some(birth) = printed.lines().find_map(rdm_client::birth_of_line) {
                break birth.unwrap_or_else(|e| panic!("{e}"));
            }
            let exited = day0.try_wait().ok().flatten();
            if exited.is_some() || std::time::Instant::now() >= until {
                let err = std::fs::read_to_string(root.join("admin1.err")).unwrap_or_default();
                let tail: Vec<&str> = err.lines().rev().take(20).collect();
                panic!("the Day-0 node-admin printed no birth (exited: {exited:?}) within 120 s: stdout {printed:?}; its stderr ends {tail:#?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let executables = [(rdm_core::NodeKind::NodeAdmin, bin.join("rshape-node-admin")), (rdm_core::NodeKind::Gateway, bin.join("rshape-gateway"))].into_iter().collect();
        let admin = Arc::new(Admin::at(&birth).await.expect("the admin's birth").with_executables(executables));
        let until = std::time::Instant::now() + Duration::from_secs(60);
        let day0 = loop {
            match admin.fabric().await {
                Ok(f) if f.fabric_primary.as_ref() == Some(&birth.name) && f.status == rdm_client::ScopeStatus::Ready => break f.build_id,
                other => assert!(std::time::Instant::now() < until, "the Day-0 admin never named itself the fabric-primary of a ready fabric: {other:?}"),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        // The Day-0 Build is Complete from its own ready, one Build at a time: the UI's Builds are
        // submitted once it is.
        if let Some(day0) = day0 {
            until_complete(&admin, "the Day-0 Build", &BuildId::parse(&day0).expect("the Day-0 Build's id")).await;
        }
        (estate, admin)
    }

    /// `executable` copied beside it with one byte appended: the same program under another sha256.
    fn another_build(&self, exe: &str) -> std::path::PathBuf {
        use std::io::Write;
        let to = self.bin.join(format!("{exe}-next"));
        std::fs::copy(self.bin.join(exe), &to).unwrap();
        std::fs::OpenOptions::new().append(true).open(&to).unwrap().write_all(&[0]).unwrap();
        to
    }
}

impl Drop for ConsumerEstate {
    fn drop(&mut self) {
        let pattern = format!("{}/", self.bin.display());
        for signal in ["-TERM", "-KILL"] {
            let _ = std::process::Command::new("pkill").args([signal, "-f", &pattern]).status();
            for _ in 0..40 {
                if !std::process::Command::new("pgrep").args(["-f", &pattern]).status().is_ok_and(|s| s.success()) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// CONTRACT (build-deployment.md §3: a request naming a different executable for an existing path is that
/// node's replacement, node.delete then node.create at the path; states.md "Resize and executable
/// replacement"; Q154): on the consumer's own estate, the UI adds a gateway (Complete), resubmits the
/// current topology (Complete, nothing executed) and replaces the gateway's executable with another build
/// of it (Complete); after each the fabric is ready, and after the last the gateway's row
/// records the new executable. Must NOT happen: a Build the UI submitted never Complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adding_resubmitting_and_replacing_a_gateway_through_the_ui_each_completes() {
    let (estate, admin) = ConsumerEstate::start("replace").await;
    let ui = router(Some(admin.clone()), Arc::new(Timeline::default()));
    let id = accepted(&ui, "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "gateway"}))).await;
    until_complete(&admin, "the gateway added", &id).await;
    ready(&admin, "after the gateway added").await;
    let id = accepted(&ui, "/api/builds/resubmit", None).await;
    until_complete(&admin, "the topology resubmitted", &id).await;
    ready(&admin, "after the topology resubmitted").await;
    let next = estate.another_build("rshape-gateway");
    let id = accepted(&ui, "/api/nodes/mesh1.gateway.1/replace", Some(json!({"executable": next}))).await;
    until_complete(&admin, "the gateway's executable replaced", &id).await;
    ready(&admin, "after the gateway's executable replaced").await;
    let sha = rdm_core::binding::sha256_file(&next).unwrap();
    let rows = admin.rows().await.expect("fabric.nodes.get");
    let gateway = rows.iter().find(|n| n.name.to_string() == "mesh1.gateway.1" && n.status == rdm_client::NodeStatus::Ready).unwrap_or_else(|| panic!("mesh1.gateway.1 is ready: {rows:?}"));
    assert_eq!(gateway.executable.as_ref().map(|b| b.sha256.clone()), Some(sha), "the replaced gateway's row records the executable its request named: {gateway:?}");
    drop(estate);
}
