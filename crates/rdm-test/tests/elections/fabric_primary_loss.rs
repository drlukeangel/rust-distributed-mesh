//! Q89 (Luke, 2026-10-20): the loss of the fabric-primary is one playbook, whatever stopped it
//! (fabric-recovery.md "The loss of the fabric-primary while a node-admin lives"; elections §4.3, §5).
//! Its seats move at the staleness floor to its own mesh's standby, who publishes the mesh's aggregate
//! again; the lost fabric-primary, heard again, is commanded back through node.mesh.join by the interim
//! and commands nobody itself; one floor after the node at its path.name is Ready the interim hands
//! every seat the row took back to it. The interim holds the seats, never the storage.
use rdm_client::NodeStatus;
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
/// The fabric-primary's name and whether it also holds its mesh's seat, from a view.
fn seats(nodes: &[Value]) -> Option<(String, bool)> {
    nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| (s(&n["name"]), n["is_primary"] == true))
}

/// CONTRACT (Q89; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" steps 1-4;
/// elections §4.3, §5): a two-mesh estate; mesh1.admin.1, the fabric-primary and mesh1's primary, has its
/// exact runtime held (SIGSTOP) past the staleness floor. Past the floor mesh1.admin.2, the standby of its
/// own mesh, is mesh-primary and fabric-primary and publishes mesh1's aggregate again. Released,
/// mesh1.admin.1 is commanded back by the interim through node.mesh.join and runs its start sequence
/// (hook, cert, rafka-time) as the same birth. One floor after it is ready-for-traffic the interim hands
/// both primaries back to it, and the fabric is resolved to the fabric-primary at the original path.name,
/// the same node.id and endpoint. What must NOT happen: a terminate, a new birth at mesh1.admin.1, the
/// primaries left with the interim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_held_past_the_floor_is_superseded_by_its_meshs_standby_and_gets_its_seats_back() {
    let test = "a_fabric_primary_held_past_the_floor_is_superseded_by_its_meshs_standby_and_gets_its_seats_back";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2), ("mesh2", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 2, 2), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let floor = rdm_transport::membership::staleness_floor();
    let fp = "mesh1.admin.1";
    let standby = "mesh1.admin.2";
    let before = estate.node_view(fp).await;
    let nodes0 = estate.node_values().await;
    assert_eq!(seats(&nodes0), Some((fp.to_string(), true)), "the Day-0 admin holds both seats: {nodes0:#?}");
    // Control is read through the standby from here: a held admin answers nothing.
    estate.enter(&Estate::path(standby)).await;
    // The Day-0 admin is the harness's own process: its pid is the estate's record, not a provider's.
    let pid = estate.bootstrap_pid().unwrap_or_else(|| panic!("{fp} is the bootstrap admin and the estate holds its pid"));

    // The hold, past the floor.
    let held_at = now_ns();
    signal(pid, "-STOP");
    let taken = estate
        .until_view("the standby of the fabric-primary's own mesh holds both seats within a floor of the mark", floor * 3 + Duration::from_secs(30), |views| {
            let ns = Estate::values(views);
            (seats(&ns) == Some((standby.to_string(), true))).then_some(ns)
        })
        .await;
    assert_eq!(taken.iter().filter(|n| n["is_fabric_primary"] == true).count(), 1, "one fabric-primary while the row stands: {taken:#?}");

    // The release: the lost fabric-primary comes back as a member under the interim.
    let released_at = now_ns();
    signal(pid, "-CONT");
    let back = estate
        .until_node(fp, "the held fabric-primary is ready-for-traffic again as the same birth", floor * 8 + Duration::from_secs(60), |n| {
            n.filter(|n| n.status == NodeStatus::ReadyForTraffic && n.node_id == before.node_id && n.incarnation_id == before.incarnation_id).cloned()
        })
        .await;
    assert_eq!(back.incarnation_id, before.incarnation_id, "{fp} came back as itself");
    // The hand-back, one floor after it is ready.
    let home = estate
        .until_view("the interim hands both seats back to the node at the original path.name", floor * 4 + Duration::from_secs(60), |views| {
            let ns = Estate::values(views);
            (seats(&ns) == Some((fp.to_string(), true))).then_some(ns)
        })
        .await;
    assert_eq!(home.iter().filter(|n| n["is_fabric_primary"] == true).count(), 1, "one fabric-primary after the hand-back: {home:#?}");
    estate.stop().await;
    let spans = estate.spans();
    let republished = named(&spans, "rdm.mesh.backbone.update.via-aggregate-publisher").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == standby && at(sp) >= held_at && at(sp) < released_at).count();
    assert!(republished > 0, "the standby publishes mesh1's aggregate again as its primary (via-aggregate-publisher) while the fabric-primary is held");
    // The commands sent (a look that sent nothing is spanned too, `sent = false`, with why).
    let commanded: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-mesh-join-commanded").into_iter().filter(|sp| at(sp) >= released_at && sp["attributes"]["sent"] != "false" && sp["attributes"]["sent"] != false).map(|sp| format!("{}:{}", s(&sp["attributes"]["node"]), s(&sp["attributes"]["outcome"]))).collect();
    assert!(commanded.iter().any(|c| c == &format!("{fp}:mesh-joined")), "the interim commands the returned fabric-primary back through node.mesh.join: {commanded:?}");
    let terminated = named(&spans, "rdm.node_admin.deployment.delete.via-fence").into_iter().filter(|sp| s(&sp["attributes"]["outcome"]) == "terminated").count();
    assert_eq!(terminated, 0, "a held runtime is never terminated");
    let created = named(&spans, "rdm.node_admin.node.create.via-build").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == fp && at(sp) >= held_at).count();
    assert_eq!(created, 0, "no new birth at {fp}: the same birth came back");
}
