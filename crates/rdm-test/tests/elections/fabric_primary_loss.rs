//! Q122 (Luke, 2026-10-22): the fabric-primary unheard past the staleness floor is taken by nobody
//! (fabric-recovery.md "The loss of the fabric-primary while a node-admin lives"; elections §3, §4.1).
//! The lowest Ready node-admin of its mesh runs fabric recovery and takes no primary role;
//! node investigation decides what becomes of the lost birth, and node recovery re-creates the
//! node-admin at its path.name; when that node is Ready the workflow restores it as
//! mesh-primary and fabric-primary, and the node records both on its own row.
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "fabric-primary-loss".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}
fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}
fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}
fn signal(pid: u32, sig: &str) {
    let ok = std::process::Command::new("kill").args([sig, &pid.to_string()]).status().expect("kill runs").success();
    assert!(ok, "kill {sig} {pid}");
}
/// The fabric-primary's name and whether it also holds its mesh's role, from a view.
fn primaries(nodes: &[Value]) -> Option<(String, bool)> {
    nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| (s(&n["name"]), n["is_primary"] == true))
}

/// CONTRACT (Q122; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" steps 1-4;
/// elections §3, §4.1): a two-mesh estate; mesh1.admin.1, the fabric-primary and mesh1's primary, has its
/// exact runtime held (SIGSTOP) past the staleness floor. From the mark until the workflow restores the
/// fabric-primary, no view names a fabric-primary or a primary node-admin of mesh1: mesh1.admin.2, the
/// lowest Ready node-admin of the mesh, runs fabric recovery and takes no primary role. Node
/// investigation reads the held runtime (not `reached`), so node recovery re-creates the node-admin at
/// mesh1.admin.1 with a new node id; once it is Ready the workflow restores it as mesh-primary
/// and fabric-primary. What must NOT happen: mesh1.admin.2 or a node of mesh2 named fabric-primary, or
/// mesh1.admin.2 named mesh1's primary, while the workflow runs. The held birth stays held for the whole
/// cell (what a returning old birth meets is not this cell's); the teardown kills it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_held_past_the_floor_is_restored_at_its_path_name_by_fabric_recovery_its_mesh_runs() {
    let test = "a_fabric_primary_held_past_the_floor_is_restored_at_its_path_name_by_fabric_recovery_its_mesh_runs";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2), ("mesh2", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 2, 2), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let floor = rdm_transport::topology::staleness_floor();
    let fp = "mesh1.admin.1";
    let standby = "mesh1.admin.2";
    let before = estate.node_view(fp).await;
    let nodes0 = estate.node_values().await;
    assert_eq!(primaries(&nodes0), Some((fp.to_string(), true)), "the Day-0 admin holds both roles: {nodes0:#?}");
    // Control is read through the standby from here: a held admin answers nothing.
    estate.enter(&Estate::path(standby)).await;
    let pid = estate.bootstrap_pid().unwrap_or_else(|| panic!("{fp} is the bootstrap admin and the estate holds its pid"));

    let held_at = now_ns();
    signal(pid, "-STOP");
    // Past the mark: nobody holds either role, in every sampled view, until a node at mesh1.admin.1 with a
    // new node id is Ready and holds both. The standby keeps no atlas: it forwards each read to the
    // mesh-primary its view names (query.md §1; Q243), so the views answered are a primary's, and the
    // standby's own view holding no primary is its refusal naming none (read from its spans below).
    let restored = estate
        .until_view("the node re-created at mesh1.admin.1 is Ready, mesh-primary and fabric-primary", floor * 12 + Duration::from_secs(120), |views| {
            let ns = Estate::values(views);
            let primaries: Vec<String> = ns.iter().filter(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).collect();
            let mesh1_primary: Vec<String> = ns.iter().filter(|n| n["kind"] == "node_admin" && n["is_primary"] == true && s(&n["name"]).starts_with("mesh1.")).map(|n| s(&n["name"])).collect();
            assert!(primaries.iter().all(|h| h == fp), "only the node at {fp} is ever fabric-primary: {primaries:?}");
            assert!(mesh1_primary.iter().all(|h| h == fp), "only the node at {fp} is ever mesh1's primary: {mesh1_primary:?}");
            ns.iter()
                .find(|n| n["name"] == fp && s(&n["node_id"]) != before.node_id.to_string() && n["status"] == "ready" && n["is_fabric_primary"] == true && n["is_primary"] == true)
                .cloned()
        })
        .await;
    // The held birth is the test's to end; nothing in the workflow terminates an os-stopped runtime.
    signal(pid, "-KILL");
    estate.stop().await;
    let spans = estate.spans();
    // The standby's own view, sampled by every read sent to it, held no primary of mesh1 while fabric
    // recovery ran: a read refused naming none to send it on to.
    let restored_at = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == fp && at(sp) >= held_at).map(at).min().unwrap_or(u64::MAX);
    let sampled_vacant = spans.iter().any(|sp| {
        let name = sp["name"].as_str().unwrap_or_default();
        name.starts_with("rdm.node_admin.") && name.ends_with(".serve.via-forward") && s(&sp["attributes"]["node"]) == standby && s(&sp["attributes"]["outcome"]) == "not-ready" && s(&sp["attributes"]["mesh_primary"]).is_empty() && s(&sp["attributes"]["fabric_primary"]).is_empty() && at(sp) >= held_at && at(sp) < restored_at
    });
    assert!(sampled_vacant, "a view was sampled with neither role held while fabric recovery ran");
    let ran: Vec<Value> = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| at(sp) >= held_at).cloned().collect();
    assert!(ran.iter().any(|sp| s(&sp["attributes"]["node"]) == standby && s(&sp["attributes"]["arm"]) == "own-mesh" && s(&sp["attributes"]["outcome"]).starts_with("restored")), "{standby} ran fabric recovery in its own mesh and restored the node at {fp}: {ran:#?}");
    assert!(ran.iter().all(|sp| s(&sp["attributes"]["node"]) == standby), "nobody else ran it: {ran:#?}");
    let verdict = ran.iter().find_map(|sp| sp["attributes"]["verdict"].as_str().map(str::to_string)).unwrap_or_default();
    assert!(!verdict.is_empty() && verdict != "reached", "node investigation decided the re-create: verdict {verdict:?}");
    let created = named(&spans, "rdm.node_admin.node.create.via-build").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == fp && at(sp) >= held_at).count();
    assert!(created >= 1, "node recovery re-created the node-admin at {fp}");
    let installed = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == fp && at(sp) >= held_at).count();
    assert!(installed >= 1, "the restored node installed both roles on the workflow's call");
    assert_ne!(s(&restored["node_id"]), before.node_id.to_string(), "a new node id at the same path.name");
}

/// CONTRACT (fabric-recovery.md intro: "while any node-admin lives, the fabric-primary moves and is
/// brought back without a person"; "The launched node-admin is a new node at the same path.name: a new
/// node.id and incarnation and a new port, with the same endpoint key over the inherited data dir";
/// node-admin-lifecycle.md §4.4, "fabric-primary is launched again into its own mesh"; elections §4.3:
/// "the node-admin at the fabric-primary's path.name is re-created and is mesh-primary and
/// fabric-primary again when it is Ready"; Q156: "a launch that finds a living admin is
/// refused, never an ordinary admin at the primary's name"; Q199): mesh1 has two node-admins; the
/// fabric-primary mesh1.admin.1's process is killed (SIGKILL) and a person starts a node-admin again over
/// its data dir, as the topology-storage restart canaries do, while mesh1.admin.2 lives. The launch is
/// refused `NameHeld` naming mesh1.admin.2 and exits, having written nothing. Read through mesh1.admin.2,
/// within the bound of the held-primary cell above, `fabric.get` names the fabric-primary at
/// mesh1.admin.1, and the views name exactly one birth at mesh1.admin.1, ready, mesh-primary
/// and fabric-primary: the one mesh1.admin.2's fabric recovery re-created. What must NOT happen: the
/// launch taking the path.name, two births contending for mesh1.admin.1 (a node.delete of the lost birth
/// refused `name-held` by the started one, again and again), or mesh1 left with no fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_started_again_on_its_data_dir_while_a_node_admin_lives_ends_with_one_birth_at_its_path_that_is_fabric_primary() {
    let test = "a_fabric_primary_started_again_on_its_data_dir_while_a_node_admin_lives_ends_with_one_birth_at_its_path_that_is_fabric_primary";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 1)]).await;
    estate.settled_views(&[("mesh1", 2, 1)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let floor = rdm_transport::topology::staleness_floor();
    let fp = "mesh1.admin.1";
    let standby = "mesh1.admin.2";
    let before = estate.node_view(fp).await;
    let nodes0 = estate.node_values().await;
    assert_eq!(primaries(&nodes0), Some((fp.to_string(), true)), "the Day-0 admin holds both roles: {nodes0:#?}");
    // Control is read through the standby from here: the killed admin answers nothing.
    estate.enter(&Estate::path(standby)).await;
    let killed_at = now_ns();
    estate.kill_bootstrap();
    let (status, stderr) = estate.admin_launch_refused(&estate.bootstrap_data_dir("mesh1"), &[], Duration::from_secs(60));
    assert!(!status.success() && stderr.contains("NameHeld") && stderr.contains(standby), "the node-admin started on the fabric-primary's data dir is refused NameHeld naming {standby}: {status}: {stderr}");
    let within = floor * 12 + Duration::from_secs(120);
    let killed = before.node_id.to_string();
    let one = estate
        .until_view_at(&Estate::path(standby), &format!("one birth at {fp}, not the killed {killed}, ready, mesh-primary and fabric-primary"), within, |views| {
            let ns = Estate::values(views);
            let at_fp: Vec<&Value> = ns.iter().filter(|n| n["name"] == fp && n["status"] != "deleted").collect();
            (at_fp.len() == 1 && s(&at_fp[0]["node_id"]) != killed && at_fp[0]["status"] == "ready" && at_fp[0]["is_fabric_primary"] == true && at_fp[0]["is_primary"] == true).then(|| at_fp[0].clone())
        })
        .await;
    let named_fp = estate.fabric_view_at(&Estate::path(standby)).await.and_then(|f| f.fabric_primary).map(|p| p.to_string());
    assert_eq!(named_fp.as_deref(), Some(fp), "fabric.get at {standby} names the fabric-primary at {fp}");
    assert_ne!(s(&one["node_id"]), before.node_id.to_string(), "the birth at {fp} is not the killed one");
    estate.stop().await;
    let spans = estate.spans();
    let refused: Vec<&Value> = spans.iter().filter(|sp| at(sp) >= killed_at && s(&sp["attributes"]["error"]).contains("name-held")).collect();
    assert!(refused.len() < 3, "no node.delete of the lost birth is refused name-held again and again: {} refusals, first {:#?}", refused.len(), refused.first());
}
