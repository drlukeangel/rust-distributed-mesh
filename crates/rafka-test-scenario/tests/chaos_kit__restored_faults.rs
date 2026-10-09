//! The chaos kit's burst kill, disk fill, partition subset, link flap and inbound drop, each
//! applied end to end on a real process estate: the kit's fault span, the OS or view consequence,
//! and the heal.
//!
//! The fabric-primary is never a target: each cell passes it as protected. The network cells need
//! `iptables` (root, or `sudo -n`); where the host cannot, the fault answers
//! `LinkRefusal::Unavailable` and the cell skips by that reason, or fails with it when
//! `RDM_REQUIRE_NETFAULT=1`.

use rafka_test_scenario::disk_faults::DiskFull;
use rafka_test_scenario::elections::advertised_fabric_primaries;
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::link_faults::{flap_link, FirewallInbound, LinkRefusal, PartitionSubset};
use rafka_test_scenario::process_faults::{burst_kill, gone, ExactRuntime};
use serde_json::{json, Value};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "chaos-kit".into(),
        subfeature: "restored-faults".into(),
        rung: "MN".into(),
        provider: "process".into(),
        test: test.into(),
    }
}

fn ready(n: &Value) -> bool {
    n["status"] == "ready-for-traffic"
}

fn attr(sp: &Value, k: &str) -> String {
    sp["attributes"][k].as_str().unwrap_or_default().to_string()
}

/// mesh1 with two node-admins and three rpc nodes, all ready; this process's own spans go to the
/// estate's evidence dir beside the nodes' (and to OTLP when configured).
async fn estate(test: &str) -> (Estate, Option<rafka_mesh_telemetry::TelemetryGuard>) {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), SETTLE).await;
    wait_for("2 admins and 3 rpc nodes ready", SETTLE, || async { (estate.nodes().await.iter().filter(|n| ready(n)).count() == 5).then_some(()) }).await;
    std::env::set_var("RDM_EVIDENCE_DIR", &estate.evidence);
    let guard = rafka_mesh_telemetry::init_evidence_telemetry("rafka-chaos-kit");
    (estate, guard)
}

async fn published(estate: &Estate, node: &str) -> ExactRuntime {
    let dir = estate.data_dir_of(node).await;
    ExactRuntime::published(std::path::Path::new(&dir)).unwrap_or_else(|r| panic!("{node}: no exact runtime published in {dir}: {r:?}"))
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// Skip a network cell by the host's named reason; any other refusal is a failure.
fn host_cannot(why: &LinkRefusal) -> bool {
    match why {
        LinkRefusal::Unavailable { reason } if std::env::var("RDM_REQUIRE_NETFAULT").is_err() => {
            eprintln!("skip: {reason}");
            true
        }
        other => panic!("the fault was refused: {other:?}"),
    }
}

async fn finish(mut estate: Estate, guard: Option<rafka_mesh_telemetry::TelemetryGuard>) -> Vec<Value> {
    estate.stop().await;
    drop(guard);
    estate.spans()
}

async fn back_ready(estate: &Estate, node: &str, node_id: &Value) -> Value {
    wait_for(&format!("{node} ready again as the same node"), SETTLE, || async { estate.node_opt(node).await.filter(|n| ready(n) && n["node_id"] == *node_id) }).await
}

/// CONTRACT: a burst kill signals two rpc nodes' exact runtimes together, each acknowledged by its
/// exit; the third rpc node and the fabric-primary node-admin (protected) are untouched; the view
/// stops holding either victim's birth as ready. What must NOT happen: a signal to a protected or
/// unnamed process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_kill_takes_two_exact_runtimes_together_and_spares_the_rest() {
    if crate::own_process::delegated(module_path!(), "burst_kill_takes_two_exact_runtimes_together_and_spares_the_rest") {
        return;
    }
    let (estate, guard) = estate("burst_kill_takes_two_exact_runtimes_together_and_spares_the_rest").await;
    let nodes = estate.nodes().await;
    let fp = advertised_fabric_primaries(&nodes).into_iter().next().expect("a fabric primary is advertised");
    // The fabric primary's runtime: its published fact, or (the self-adopted bootstrap admin
    // publishes none) the OS's own record of the process this estate started.
    let protected = match ExactRuntime::published(std::path::Path::new(&estate.data_dir_of(&fp).await)) {
        Ok(r) => r,
        Err(_) => {
            let pid = estate.bootstrap_pid().expect("the bootstrap admin runs");
            ExactRuntime { deployment_id: String::new(), control_domain: rafka_test_scenario::process_faults::local_control_domain(), pid, start: rafka_test_scenario::process_faults::start_token(pid).expect("it runs") }
        }
    };
    let (a, b, spared) = (published(&estate, "mesh1.rpc.1").await, published(&estate, "mesh1.rpc.2").await, published(&estate, "mesh1.rpc.3").await);
    let births: Vec<Value> = ["mesh1.rpc.1", "mesh1.rpc.2"].iter().map(|n| nodes.iter().find(|x| x["name"] == *n).unwrap()["incarnation_id"].clone()).collect();
    let done = tokio::task::spawn_blocking({
        let (targets, protected) = (vec![a.clone(), b.clone()], vec![protected.clone()]);
        move || burst_kill(&targets, &protected)
    })
    .await
    .unwrap()
    .expect("the burst applies");
    assert!(done.killed.iter().all(|k| k.exited), "{done:?}");
    assert!(gone(a.pid) && gone(b.pid), "both victims exited");
    assert!(!gone(spared.pid) && !gone(protected.pid), "the spared rpc node and the protected fabric primary still run");
    for (name, birth) in [("mesh1.rpc.1", &births[0]), ("mesh1.rpc.2", &births[1])] {
        wait_for(&format!("{name}'s birth no longer ready in the view"), SETTLE, || async { estate.node_opt(name).await.filter(|n| !ready(n) || n["incarnation_id"] != *birth).map(|_| ()).or_else(|| None) }).await;
    }
    let spans = finish(estate, guard).await;
    let burst: Vec<&Value> = named(&spans, "rdm.testkit.fault.update.via-process-burst");
    assert_eq!(burst.len(), 1, "one burst span: {burst:?}");
    assert_eq!(attr(burst[0], "targets"), "2");
    assert!(attr(burst[0], "outcome").contains("\"applied\":\"kill\""), "{}", attr(burst[0], "outcome"));
}

/// CONTRACT: a disk fill really allocates its bounded filler inside ONE node's own data dir, the
/// node keeps serving (a bounded filler is not an exhausted host), and the heal removes the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disk_fill_allocates_in_one_nodes_data_dir_and_heals_by_removal() {
    if crate::own_process::delegated(module_path!(), "disk_fill_allocates_in_one_nodes_data_dir_and_heals_by_removal") {
        return;
    }
    let (estate, guard) = estate("disk_fill_allocates_in_one_nodes_data_dir_and_heals_by_removal").await;
    let dir = std::path::PathBuf::from(estate.data_dir_of("mesh1.rpc.2").await);
    let other = std::path::PathBuf::from(estate.data_dir_of("mesh1.rpc.1").await);
    let fill = DiskFull::fill(&dir, 16 << 20).expect("16 MiB fits");
    assert!(fill.allocated >= 16 << 20, "{fill:?}");
    assert!(fill.path.starts_with(&dir) && !other.join(rafka_test_scenario::disk_faults::FILLER).exists(), "only the named node's dir holds a filler");
    let held = estate.node("mesh1.rpc.2").await;
    assert!(ready(&held), "a bounded filler leaves the node serving: {held}");
    let path = fill.path.clone();
    drop(fill);
    assert!(!path.exists(), "the heal removed the filler");
    let spans = finish(estate, guard).await;
    let fills = named(&spans, "rdm.testkit.fault.update.via-disk-fill");
    assert!(fills.iter().any(|sp| attr(sp, "outcome").starts_with("allocated")), "{fills:?}");
    assert!(named(&spans, "rdm.testkit.fault.remove.via-heal").iter().any(|sp| attr(sp, "fault") == "disk-fill"));
}

/// CONTRACT: a partition subset cuts two rpc nodes from every other node: while it stands the view
/// stops holding either as ready; the fabric-primary is protected and refused by name; when the cut
/// is dropped both are ready again as the same nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_subset_cuts_a_node_set_from_the_rest_until_healed() {
    if crate::own_process::delegated(module_path!(), "partition_subset_cuts_a_node_set_from_the_rest_until_healed") {
        return;
    }
    let (estate, guard) = estate("partition_subset_cuts_a_node_set_from_the_rest_until_healed").await;
    let nodes = estate.nodes().await;
    let fp = advertised_fabric_primaries(&nodes);
    let ids: Vec<Value> = ["mesh1.rpc.1", "mesh1.rpc.2"].iter().map(|n| nodes.iter().find(|x| x["name"] == *n).unwrap()["node_id"].clone()).collect();
    assert_eq!(PartitionSubset::start(&nodes, &fp, &fp).err(), Some(LinkRefusal::Protected { name: fp[0].clone() }), "the fabric primary is never a target");
    let cut = match PartitionSubset::start(&nodes, &names(&["mesh1.rpc.1", "mesh1.rpc.2"]), &fp) {
        Ok(c) => c,
        Err(why) => {
            host_cannot(&why);
            finish(estate, guard).await;
            return;
        }
    };
    for n in ["mesh1.rpc.1", "mesh1.rpc.2"] {
        wait_for(&format!("{n} not ready in the view under the cut"), SETTLE, || async { estate.node_opt(n).await.filter(|x| !ready(x)).map(|_| ()) }).await;
    }
    drop(cut);
    back_ready(&estate, "mesh1.rpc.1", &ids[0]).await;
    back_ready(&estate, "mesh1.rpc.2", &ids[1]).await;
    let spans = finish(estate, guard).await;
    assert!(named(&spans, "rdm.testkit.fault.update.via-partition-subset").iter().any(|sp| attr(sp, "outcome").starts_with("cut:")));
    assert!(named(&spans, "rdm.testkit.fault.remove.via-heal").iter().any(|sp| attr(sp, "fault") == "partition-subset"));
}

/// CONTRACT: a link between one rpc node and the rest flaps three times, each cut shorter than the
/// staleness floor and healed between; afterwards the same node is ready with the same node id and
/// the estate holds no ghost birth. Each cut and each heal leaves a span.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn link_flap_cuts_and_heals_on_schedule_and_leaves_the_same_birth() {
    if crate::own_process::delegated(module_path!(), "link_flap_cuts_and_heals_on_schedule_and_leaves_the_same_birth") {
        return;
    }
    let (estate, guard) = estate("link_flap_cuts_and_heals_on_schedule_and_leaves_the_same_birth").await;
    let nodes = estate.nodes().await;
    let fp = advertised_fabric_primaries(&nodes);
    let victim = "mesh1.rpc.1";
    let rest: Vec<String> = nodes.iter().filter_map(|n| n["name"].as_str()).filter(|n| *n != victim).map(String::from).collect();
    let before = nodes.iter().find(|n| n["name"] == victim).unwrap().clone();
    let out = tokio::task::spawn_blocking({
        let (nodes, rest) = (nodes.clone(), rest.clone());
        move || flap_link(&nodes, &names(&[victim]), &rest, &fp, 3, Duration::from_millis(1200), Duration::from_millis(800))
    })
    .await
    .unwrap();
    let flapped = match out {
        Ok(f) => f,
        Err(why) => {
            host_cannot(&why);
            finish(estate, guard).await;
            return;
        }
    };
    assert_eq!(flapped.cycles, 3);
    let back = back_ready(&estate, victim, &before["node_id"]).await;
    assert_eq!(back["incarnation_id"], before["incarnation_id"], "a flap shorter than the floor leaves the same birth");
    assert_eq!(estate.nodes().await.len(), nodes.len(), "no ghost birth");
    let spans = finish(estate, guard).await;
    assert_eq!(named(&spans, "rdm.testkit.fault.update.via-link-flap").len(), 1);
    assert_eq!(named(&spans, "rdm.testkit.fault.remove.via-heal").iter().filter(|sp| attr(sp, "fault") == "link-flap").count(), 3, "one heal per cycle");
}

/// CONTRACT: inbound UDP to one rpc node is dropped while its own sends are not: the view stops
/// holding it as ready; when the drop is removed it is ready again as the same node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn firewall_inbound_deafens_one_node_until_healed() {
    if crate::own_process::delegated(module_path!(), "firewall_inbound_deafens_one_node_until_healed") {
        return;
    }
    let (estate, guard) = estate("firewall_inbound_deafens_one_node_until_healed").await;
    let nodes = estate.nodes().await;
    let fp = advertised_fabric_primaries(&nodes);
    let victim = "mesh1.rpc.2";
    let id = nodes.iter().find(|n| n["name"] == victim).unwrap()["node_id"].clone();
    let drop_in = match FirewallInbound::start(&nodes, victim, &fp) {
        Ok(f) => f,
        Err(why) => {
            host_cannot(&why);
            finish(estate, guard).await;
            return;
        }
    };
    wait_for(&format!("{victim} not ready under the inbound drop"), SETTLE, || async { estate.node_opt(victim).await.filter(|x| !ready(x)).map(|_| ()) }).await;
    drop(drop_in);
    back_ready(&estate, victim, &id).await;
    let spans = finish(estate, guard).await;
    assert!(named(&spans, "rdm.testkit.fault.update.via-inbound-drop").iter().any(|sp| attr(sp, "target") == victim));
    assert!(named(&spans, "rdm.testkit.fault.remove.via-heal").iter().any(|sp| attr(sp, "fault") == "inbound-drop"));
}
