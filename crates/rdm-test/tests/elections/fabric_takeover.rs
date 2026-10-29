//! i144 e6.2 process E2E: the fabric-primary is restored without a person when its mesh is lost and no
//! surviving admin can inspect it: fabric recovery, run from the backbone (Q122; fabric-recovery.md "The
//! loss of the fabric-primary while a node-admin lives").
//!
//! The lost mesh is made uninspectable with the testkit's provider-domain cut on every surviving
//! admin: each one's provider reports a foreign control domain and refuses the lost runtimes' locators,
//! so its look at the lost mesh's admins answers `unproven`. A lost mesh the survivors can inspect is
//! recovered the same way (`fabric_primary::losing_the_fabric_primarys_whole_mesh_is_fabric_recovery_run_from_the_backbone`).

use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use rdm_test::netfault::{udp_ports, Partition};
use serde_json::{json, Value};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "fabric-takeover".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// A faulted-admin estate (every admin serves the fault door) of `shape`, born ready.
async fn estate_of(test: &str, shape: &[(&str, u32, u32)]) -> Estate {
    let sha = candidate_sha();
    let estate = Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"], &[]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", shape).await;
    estate.settles_to(shape, Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    estate
}

/// Make mesh1 uninspectable to every node-admin outside it (the provider-domain cut), then send
/// `signal` to every process of mesh1, the Day-0 admin included. The pids signalled.
async fn lose_mesh1(estate: &Estate, nodes: &[Value], signal: &str) -> Vec<u64> {
    for n in nodes.iter().filter(|n| n["mesh"] != "mesh1" && n["kind"] == "node_admin") {
        Door::at(estate, &s(&n["name"])).await.arm("foreign-domain", json!({"kind": "provider-domain"})).await;
    }
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["name"] != "mesh1.admin.1") {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    pids.extend(estate.bootstrap_pid().map(u64::from));
    if signal == "-KILL" {
        estate.fabric_ready();
    }
    for pid in &pids {
        assert!(std::process::Command::new("kill").args([signal, &pid.to_string()]).status().unwrap().success(), "kill {signal} {pid}");
    }
    pids
}

/// Make mesh1's runtimes uninspectable to every node-admin outside it while each can still launch and
/// inspect its own (the provider-unproven cut, by pid: their inspection answers `Unknown`), then SIGKILL
/// every process of mesh1, the Day-0 admin included. The pids killed.
async fn lose_mesh1_unproven(estate: &Estate, nodes: &[Value]) -> Vec<u64> {
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["name"] != "mesh1.admin.1") {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    pids.extend(estate.bootstrap_pid().map(u64::from));
    for n in nodes.iter().filter(|n| n["mesh"] != "mesh1" && n["kind"] == "node_admin") {
        Door::at(estate, &s(&n["name"])).await.arm("mesh1-unproven", json!({"kind": "provider-unproven", "pids": pids})).await;
    }
    estate.fabric_ready();
    for pid in &pids {
        assert!(std::process::Command::new("kill").args(["-KILL", &pid.to_string()]).status().unwrap().success(), "kill -KILL {pid}");
    }
    pids
}

/// CONTRACT (Q122; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" step 2,
/// "Outside its mesh": "No answer from any of them by the decision step starts fabric recovery at the
/// lowest Ready mesh-primary, exactly as in the own-mesh arm"; "It takes no primary role";
/// step 4: the node at the fabric-primary's path.name, Ready, is restored as mesh-primary and
/// fabric-primary): mesh1 is the fabric-primary's mesh. mesh2's admins cannot inspect mesh1's runtimes
/// (the provider-unproven cut: their inspection answers `Unknown`), and every process of mesh1 is killed. mesh2's primary probes mesh1's admins,
/// each `unreachable`. Fabric recovery does not start before the decision step (twice the floor); at
/// it, mesh2's primary runs it from the backbone, its node investigation of the lost birth not
/// `reached`, and the node re-created at mesh1.admin.1 is restored as mesh-primary and fabric-primary.
/// Must NOT happen: fabric recovery started before the decision step, any node but the one at
/// mesh1.admin.1 named fabric-primary, or fabric recovery run by another admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_lost_with_its_mesh_and_uninspectable_is_recovered_from_the_backbone_at_the_decision_step() {
    let test = "a_fabric_primary_lost_with_its_mesh_and_uninspectable_is_recovered_from_the_backbone_at_the_decision_step";
    let mut estate = estate_of(test, &[("mesh1", 2, 1), ("mesh2", 2, 1)]).await;
    let nodes = estate.node_values().await;
    let fp = "mesh1.admin.1";
    let holder = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    assert_eq!(s(&holder["name"]), fp, "the Day-0 admin holds the fabric-primary: {nodes:#?}");
    let old_node_id = s(&holder["node_id"]);
    let backbone = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["is_primary"] == true && n["kind"] == "node_admin").expect("mesh2's primary")["name"]);
    estate.enter(&Estate::path(&backbone)).await;
    lose_mesh1_unproven(&estate, &nodes).await;
    let killed_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    let floor = rdm_transport::topology::staleness_floor();
    let decision = floor * 2;
    estate
        .until_view(&format!("the node re-created at {fp} is restored as mesh-primary and fabric-primary"), decision + floor * 20 + Duration::from_secs(120), |views| {
            let ns = Estate::values(views);
            let holders: Vec<String> = ns.iter().filter(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).collect();
            assert!(holders.iter().all(|h| h == fp), "only the node at {fp} is ever fabric-primary: {holders:?}");
            ns.iter().find(|n| n["name"] == fp && s(&n["node_id"]) != old_node_id && n["status"] == "ready" && n["is_fabric_primary"] == true && n["is_primary"] == true).map(|_| ())
        })
        .await;
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let ran: Vec<Value> = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| at(sp) >= killed_ns).cloned().collect();
    let run = ran.iter().find(|sp| attr(sp, "node") == backbone && attr(sp, "arm") == "backbone" && attr(sp, "outcome").starts_with("restored")).unwrap_or_else(|| panic!("{backbone} ran fabric recovery from the backbone and restored {fp}: {ran:#?}"));
    assert!(ran.iter().all(|sp| attr(sp, "node") == backbone), "nobody else ran it: {ran:#?}");
    assert!(at(run) >= killed_ns + decision.as_nanos() as u64, "fabric recovery started before the decision step ({decision:?}): {run}");
    assert_ne!(attr(run, "verdict"), "reached", "the lost birth was never reached: {run}");
    let probes: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-probe").into_iter().filter(|sp| attr(sp, "node") == backbone && sp["attributes"]["mesh"] == "mesh1" && at(sp) >= killed_ns).collect();
    assert!(probes.len() >= 2 && probes.iter().all(|sp| sp["attributes"]["outcome"] != "admin-alive"), "{backbone} probed mesh1's admins twice, none answering, before the decision: {probes:#?}");
    let restore = named(&spans, "rdm.node_admin.primary.update.via-fabric-recovery").into_iter().find(|sp| attr(sp, "node") == backbone && attr(sp, "to") == fp && attr(sp, "outcome") == "restored").cloned();
    assert!(restore.is_some(), "the workflow's call restored the node at {fp}");
    estate.record_trace_url(s(&run["trace_id"]).as_str());
}

/// CONTRACT (node-recovery.md sequence step 3: node.delete of the target "as a target with no RPC to
/// the old process and no stop frames. When the verdict was `running`, the first step is
/// `TerminateRuntime`"; Q111: nothing waits for a proof): mesh1, the fabric-primary's mesh, is lost and
/// its runtimes live in a control domain mesh2's admins do not control (the provider-domain cut), as a
/// lost host's would. mesh2's admin, running fabric recovery from the backbone, deletes the lost
/// mesh1.admin.1 all the same: its `TerminateRuntime` reads the runtime `unavailable` and terminates
/// nothing, and its row is written `Deleted`. Must NOT happen: the delete failing on the foreign
/// runtime, or a terminate sent to it. (The cut also makes this host's own launches read foreign, so
/// the re-create is `a_lost_meshs_node_admin_is_re_created_by_fabric_recovery_over_a_new_dir_with_inherited_false`'s.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_fabric_primary_whose_runtime_lives_in_another_control_domain_is_deleted_without_a_terminate() {
    let test = "a_lost_fabric_primary_whose_runtime_lives_in_another_control_domain_is_deleted_without_a_terminate";
    let mut estate = estate_of(test, &[("mesh1", 1, 1), ("mesh2", 1, 1)]).await;
    let nodes = estate.node_values().await;
    let old_id = s(&nodes.iter().find(|n| n["name"] == "mesh1.admin.1").expect("mesh1.admin.1")["node_id"]);
    let backbone = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin").expect("mesh2's admin")["name"]);
    lose_mesh1(&estate, &nodes, "-KILL").await;
    estate.enter(&Estate::path(&backbone)).await;
    let floor = rdm_transport::topology::staleness_floor();
    let deleted = wait_for("the lost mesh1.admin.1's row is written Deleted", floor * 12 + Duration::from_secs(60), || async {
        let rows = spans_where(&estate.spans(), "rdm.node_admin.node.delete.via-node-row", "subject", "mesh1.admin.1");
        rows.into_iter().find(|r| r.contains(&old_id) && r.contains("row-deleted"))
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    let terminates: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["node"] == "mesh1.admin.1" && sp["attributes"]["step"] == "TerminateRuntime").collect();
    assert!(!terminates.is_empty() && terminates.iter().all(|sp| s(&sp["attributes"]["outcome"]) == "complete"), "the delete's TerminateRuntime completes: {terminates:#?}");
    let inspected: Vec<&Value> = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().filter(|sp| sp["attributes"]["node"] == "mesh1.admin.1" && sp["attributes"]["caller"] == "terminate").collect();
    assert!(!inspected.is_empty() && inspected.iter().all(|sp| sp["attributes"]["verdict"] == "unavailable"), "the foreign runtime reads unavailable and nothing is terminated: {inspected:#?}");
    eprintln!("deleted without a terminate: {deleted}");
}

async fn new_birth_within(estate: &Estate, node: &str, old: &str, within: Duration) -> Option<rdm_core::views::NodeView> {
    let until = std::time::Instant::now() + within;
    while std::time::Instant::now() < until {
        let found = estate.node_views().await.into_iter().find(|n| n.name.to_string() == node && n.node_id.to_string() != old && n.status == rdm_core::views::NodeStatus::Ready);
        if found.is_some() {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

/// What the create pipeline told the process `pid` about its data dir: its `RDM_INHERITED_DATA_DIR`.
fn inherited_env(pid: u64) -> Option<String> {
    let environ = std::fs::read(format!("/proc/{pid}/environ")).ok()?;
    environ.split(|b| *b == 0).filter_map(|kv| std::str::from_utf8(kv).ok()).find_map(|kv| kv.strip_prefix("RDM_INHERITED_DATA_DIR=").map(str::to_string))
}

/// The spans named `name` whose `attributes[key]` is `value`, each as one line of its fields.
fn spans_where(spans: &[Value], name: &str, key: &str, value: &str) -> Vec<String> {
    named(spans, name).into_iter().filter(|sp| s(&sp["attributes"][key]) == value).map(|sp| format!("{} {}", sp["trace_id"], sp["attributes"])).collect()
}

/// CONTRACT (Q122; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" step 3:
/// node recovery re-creates the lost node "over a newly allocated data dir, `inherited = false`" when
/// the investigation's provider step did not find the old runtime exited (`R-FM2`); step 4: the node
/// at the fabric-primary's path.name, Ready, is restored as mesh-primary and fabric-primary):
/// mesh1 is the fabric-primary's mesh and is lost, its runtimes uninspectable from mesh2. mesh2's admin
/// runs fabric recovery from the backbone: its node.delete of the lost birth writes the old row
/// `Deleted`, and its node.create makes a new node.id at mesh1.admin.1, ready, over a data
/// dir that is not the old birth's, with a newly minted endpoint key, told `RDM_INHERITED_DATA_DIR=0`;
/// the old birth's data dir is left as it was. The workflow restores that node as mesh-primary and
/// fabric-primary, and it installs the roles. Must NOT happen: the old birth's data dir or endpoint
/// key reused, or any other node named fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_meshs_node_admin_is_re_created_by_fabric_recovery_over_a_new_dir_with_inherited_false() {
    let test = "a_lost_meshs_node_admin_is_re_created_by_fabric_recovery_over_a_new_dir_with_inherited_false";
    let mut estate = estate_of(test, &[("mesh1", 1, 1), ("mesh2", 1, 1)]).await;
    let nodes = estate.node_values().await;
    let old = nodes.iter().find(|n| n["name"] == "mesh1.admin.1").cloned().expect("mesh1.admin.1");
    assert_eq!(old["is_fabric_primary"], json!(true), "the Day-0 admin holds the fabric-primary: {nodes:#?}");
    let (old_id, old_endpoint) = (s(&old["node_id"]), s(&old["endpoint_id"]));
    let old_dir = estate.bootstrap_data_dir("mesh1");
    let old_key = std::fs::read(old_dir.join("node-key")).expect("the old birth's endpoint key");
    let backbone = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin").expect("mesh2's admin")["name"]);
    lose_mesh1_unproven(&estate, &nodes).await;
    estate.enter(&Estate::path(&backbone)).await;
    let floor = rdm_transport::topology::staleness_floor();
    let recreated = new_birth_within(&estate, "mesh1.admin.1", &old_id, floor * 12 + Duration::from_secs(120)).await;
    let told = match &recreated {
        Some(_) => inherited_env(estate.pid_of("mesh1.admin.1").await),
        None => None,
    };
    let restored = match &recreated {
        Some(n) => {
            let id = n.node_id.to_string();
            wait_for(&format!("the fabric's views name {id} mesh-primary and fabric-primary"), floor * 12 + Duration::from_secs(60), || async {
                let views = estate.node_views().await;
                assert!(views.iter().filter(|v| v.is_fabric_primary).all(|v| v.name.to_string() == "mesh1.admin.1"), "only the node at mesh1.admin.1 is ever fabric-primary");
                views.iter().any(|v| v.node_id.to_string() == id && v.is_fabric_primary && v.is_primary).then_some(())
            })
            .await;
            true
        }
        None => false,
    };
    estate.stop().await;
    let spans = estate.spans();
    let claims = spans_where(&spans, "rdm.node_admin.node.create.via-node-row", "subject", "mesh1.admin.1");
    let steps = spans_where(&spans, "rdm.node_admin.deployment.update.via-step", "node", "mesh1.admin.1");
    let Some(new) = recreated else {
        panic!("mesh1.admin.1 was not re-created by fabric recovery; name claims {claims:#?}; create steps {steps:#?}");
    };
    assert_ne!(new.endpoint_id.as_deref(), Some(old_endpoint.as_str()), "a newly minted endpoint key: {new:?}");
    let new_dir = new.data_dir.clone().expect("the re-created admin's data dir");
    assert_ne!(std::path::Path::new(&new_dir), old_dir.as_path(), "a newly allocated data dir: {new:?}");
    assert_eq!(std::fs::read(old_dir.join("node-key")).ok(), Some(old_key), "the old birth's data dir is left as it was");
    assert_eq!(told.as_deref(), Some("0"), "the re-created admin was told inherited = false");
    // The lost birth holds nothing any more: its node.delete wrote its row Deleted.
    let deleted = spans_where(&spans, "rdm.node_admin.node.delete.via-node-row", "subject", "mesh1.admin.1");
    assert!(deleted.iter().any(|r| r.contains(&old_id) && r.contains("row-deleted")), "the lost birth's row is written Deleted by its node.delete: {deleted:#?}");
    // The workflow, run by mesh2's admin from the backbone, restored the re-created admin.
    let ran = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == backbone && sp["attributes"]["arm"] == "backbone" && s(&sp["attributes"]["outcome"]).starts_with("restored")).count();
    assert!(restored && ran >= 1, "{backbone} ran fabric recovery from the backbone and restored mesh1.admin.1 {}", new.node_id);
    estate.record_trace_url(deleted[0].split(' ').next().unwrap_or("").trim_matches('"'));
}

/// CONTRACT (mesh-investigation.md step 5: "over the inherited data dir when the verdict is `exited`";
/// node-recovery.md "Retained": "the inherited data dir and endpoint key when the verdict is
/// `exited`"): mesh1 holds the fabric-primary and is lost; mesh2's admin can inspect it, so the old birth's
/// node.delete reads its runtime exited at its `TerminateRuntime`, and mesh2's primary re-creates
/// mesh1.admin.1 over the old birth's data dir: a new node.id at mesh1.admin.1, ready, with
/// the old birth's endpoint key, told `RDM_INHERITED_DATA_DIR=1`. Must NOT happen: a newly allocated
/// data dir or a newly minted key for an admin whose runtime exited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_meshs_node_admin_is_re_created_over_its_inherited_dir_when_its_runtime_exited() {
    let test = "a_lost_meshs_node_admin_is_re_created_over_its_inherited_dir_when_its_runtime_exited";
    let mut estate = estate_of(test, &[("mesh1", 1, 1), ("mesh2", 1, 1)]).await;
    let nodes = estate.node_values().await;
    let old = nodes.iter().find(|n| n["name"] == "mesh1.admin.1").cloned().expect("mesh1.admin.1");
    let (old_id, old_endpoint) = (s(&old["node_id"]), s(&old["endpoint_id"]));
    let old_dir = estate.bootstrap_data_dir("mesh1");
    let winner_name = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin").cloned().expect("mesh2's admin")["name"]);
    // Lose mesh1, inspectable from mesh2: no provider-domain cut.
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["name"] != "mesh1.admin.1") {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    for pid in pids {
        estate.kill_pid(pid as u32);
    }
    estate.kill_bootstrap();
    estate.enter(&Estate::path(&winner_name)).await;
    let floor = rdm_transport::topology::staleness_floor();
    let recreated = new_birth_within(&estate, "mesh1.admin.1", &old_id, floor * 12 + Duration::from_secs(120)).await;
    let told = match &recreated {
        Some(_) => inherited_env(estate.pid_of("mesh1.admin.1").await),
        None => None,
    };
    estate.stop().await;
    let spans = estate.spans();
    let steps = spans_where(&spans, "rdm.node_admin.deployment.update.via-step", "node", "mesh1.admin.1");
    let Some(new) = recreated else {
        panic!("mesh1.admin.1 was not re-created; create steps {steps:#?}");
    };
    assert_eq!(new.endpoint_id.as_deref(), Some(old_endpoint.as_str()), "the old birth's endpoint key, loaded from its data dir: {new:?}");
    assert_eq!(new.data_dir.as_deref().map(std::path::PathBuf::from), Some(old_dir.clone()), "the old birth's data dir: {new:?}");
    assert_eq!(told.as_deref(), Some("1"), "the re-created admin was told inherited = true");
    let read = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == old_id && s(&sp["attributes"]["caller"]) == "terminate").cloned().expect("the old birth's node.delete read its runtime at its TerminateRuntime");
    assert_eq!(s(&read["attributes"]["verdict"]), "exited", "{read}");
    estate.record_trace_url(&s(&read["trace_id"]));
}

/// The fabric-primary's birth as the store root holds it: whether its row is written `deleted`, and
/// which node id the path.name's name is held by (`None`: released).
fn stored_birth(root: &std::path::Path, _mesh: &str, node: &str, node_id: &str) -> (Option<String>, Option<String>) {
    let read = |p: std::path::PathBuf| std::fs::read(p).ok().and_then(|b| serde_json::from_slice::<Value>(&b).ok());
    let status = rdm_admin::store_log::read_record(&root.join(rdm_admin::store_root::NODES_FILE), "node-record", Some(node_id)).map(|r| s(&r["record"]["status"]));
    let name = read(root.join(rdm_admin::store_root::LOCKS_DIR).join("name").join(node)).map(|r| s(&r["record"]["node_id"]));
    (status, name)
}

/// CONTRACT (fabric-recovery.md step 3, Q153: "On `running`, the investigation's report stands and the
/// executor writes nothing: no row, no name release, no mark against the birth in its own view, no
/// create. A runtime the executor can inspect running but cannot reach is unreachable from the
/// executor, not lost"): mesh2's admin that is neither mesh2's primary nor the fabric-primary hears and is
/// heard by no node (all its UDP dropped) while the fabric-primary mesh1.admin.1 runs. The cut is held
/// past mesh investigation's decision step on mesh1 and past the backbone arm's node investigation of
/// the fabric-primary, which reads its runtime `running`, and two gossip intervals more. Throughout, the
/// store root holds the fabric-primary's row live and its name held by its node id; no view marks its
/// birth deleted; nothing is opened for it; and within a floor of the heal every admin's view names
/// mesh1's members ready and mesh1.admin.1, the same birth, mesh1's one primary and the one
/// fabric-primary, with no wait on any view's retention. Must NOT happen: the fabric-primary's row
/// written deleted or its name released, a `via-deleted-birth` / `via-node-deleted` for its birth, a
/// re-create Build of it, or a view without it after the heal.
///
/// The cut admin is kept to its decision step by the testkit's claimed-attempt cut on the fabric-primary:
/// mesh2's primary, which reaches it through no peer, reads its runtime `running` and submits its recovery
/// at once (node-recovery.md step 3, Q111); the drift goes up and the fabric-primary opens the attempt
/// (Q118), and its claim by mesh2's primary is held before its dispatch, so the cut admin is not re-created
/// (Q181) while the cut holds. What the cut admin, mesh2's primary in its own view, does to mesh2's rpc
/// nodes is the own-mesh arm's (Q111): with no fabric-primary in its view its recoveries fail by name
/// (Q118). The views are read for mesh1 and the fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backbone_executor_that_reads_the_fabric_primary_running_writes_nothing() {
    let test = "a_backbone_executor_that_reads_the_fabric_primary_running_writes_nothing";
    let fp = "mesh1.admin.1";
    let shape = [("mesh1", 2, 2), ("mesh2", 2, 2)];
    let mut estate = estate_of(test, &shape).await;
    let nodes = estate.node_values().await;
    let holder = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    assert_eq!(s(&holder["name"]), fp, "the Day-0 admin holds the fabric-primary: {nodes:#?}");
    let (fp_id, fp_inc) = (s(&holder["node_id"]), s(&holder["incarnation_id"]));
    let victim_row = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] != true && n["is_fabric_primary"] != true).cloned().expect("a mesh2 node-admin that is neither primary");
    let (victim, victim_id) = (s(&victim_row["name"]), s(&victim_row["node_id"]));
    let mesh2_primary = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2's primary")["name"]);
    let admins: Vec<String> = nodes.iter().filter(|n| n["kind"] == "node_admin").map(|n| s(&n["name"])).collect();
    let everyone: Vec<String> = nodes.iter().map(|n| s(&n["name"])).collect();
    let mesh1: Vec<String> = nodes.iter().filter(|n| n["mesh"] == "mesh1").map(|n| s(&n["name"])).collect();
    let others: Vec<String> = everyone.iter().filter(|n| **n != victim).cloned().collect();
    estate.enter(&Estate::path(fp)).await;
    let root = estate.fabric_store_root();
    let intact = || {
        let (status, name) = stored_birth(&root, "mesh1", fp, &fp_id);
        assert!(status.as_deref().is_some_and(|st| st != "deleted"), "the fabric-primary's row stays live on the store root: {status:?}");
        assert_eq!(name.as_deref(), Some(fp_id.as_str()), "the fabric-primary's name stays held by its node id on the store root");
    };
    intact();

    let floor = rdm_transport::topology::staleness_floor();
    let gossip = rdm_transport::topology::gossip_interval();
    let now_ns = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    // The cut admin judges mesh1 from direct backbone receipt only, and that receipt alone starts its
    // clock on mesh1 (gossip.md §3.2 "Peer-mesh silence uses direct backbone receipt at node-admins, never
    // forwarded topology"; mesh-investigation.md: "Only that receipt starts or resets t"): mesh1 as its mesh
    // channel and its join's topology read hand it is topology, never silent. The cut follows its first
    // backbone install of mesh1.
    wait_for(&format!("{victim} holds mesh1 by direct backbone receipt before the cut"), floor * 2, || async {
        named(&estate.spans(), "rdm.mesh.topology.update.via-snapshot-installed").into_iter().find(|sp| attr(sp, "node") == victim && attr(sp, "channel") == "backbone" && attr(sp, "mesh") == "mesh1").map(|_| ())
    })
    .await;
    // The first attempt claimed after the cut, the cut admin's recovery, is held before its dispatch.
    let fp_door = Door::at(&estate, fp).await;
    fp_door.arm("cut-admin-recovery", json!({"kind": "claimed-attempt"})).await;
    let cut = match Partition::start(&udp_ports(&nodes, &[victim.clone()]), &udp_ports(&nodes, &others)) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot drop {victim}'s UDP: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let cut_at = now_ns();
    // Held past mesh investigation's decision step on mesh1, at the cut admin.
    let decided = wait_for(&format!("{victim} reaches mesh investigation's decision step on mesh1 under the cut"), floor * 12 + Duration::from_secs(60), || async {
        intact();
        named(&estate.spans(), "rdm.node_admin.mesh.update.via-probe-verdict").into_iter().find(|sp| at(sp) >= cut_at && attr(sp, "node") == victim && attr(sp, "mesh") == "mesh1" && attr(sp, "outcome") == "unreachable").map(at)
    })
    .await;
    // And past the backbone arm's node investigation of the fabric-primary: its inspection first, then,
    // unreached, the inspection's answer is the verdict (node-investigation.md, Q273).
    wait_for(&format!("{victim}'s node investigation of {fp} reaches its verdict, unreached"), floor * 12 + Duration::from_secs(60), || async {
        intact();
        named(&estate.spans(), "rdm.node.tickle.returns-unknown.via-unreached").into_iter().find(|sp| at(sp) >= decided && attr(sp, "caller") == victim && attr(sp, "node_id") == fp_id && attr(sp, "next") == "verdict").map(|_| ())
    })
    .await;
    // Two gossip intervals more: the inspection's verdict, and whatever the executor does on it (the kept
    // run's delete wrote the row 1.5 s after the inspection; a node recovery opened on it writes in tens of
    // milliseconds here).
    let hold_until = std::time::Instant::now() + gossip * 2;
    while std::time::Instant::now() < hold_until {
        intact();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // What kept the cut admin alive: mesh2's primary's recovery of it, opened by the fabric-primary and
    // claimed by mesh2's primary, held before its dispatch.
    let claimed = fp_door.wait_held("cut-admin-recovery").await;
    assert_eq!(s(&claimed["hit"]["executor"]), mesh2_primary, "the held claim is mesh2's primary's: {claimed}");
    let bare = |id: String| id.replace('-', "").trim_start_matches("bid_").to_string();
    let spans_now = estate.spans();
    let opened: Vec<&Value> = named(&spans_now, "rdm.node_admin.build.update.via-recovery")
        .into_iter()
        .filter(|sp| at(sp) >= cut_at && attr(sp, "node_id") == victim_id && bare(attr(sp, "build_id")) == bare(s(&claimed["hit"]["build_id"])) && attr(sp, "attempt") == claimed["hit"]["attempt"].to_string())
        .collect();
    assert!(!opened.is_empty(), "the held attempt is the recovery of {victim} the fabric-primary opened: {claimed}");
    drop(cut);
    let healed = std::time::Instant::now();
    let healed_ns = now_ns();
    let held_ms = (now_ns() - cut_at) / 1_000_000;

    // Within a floor of the heal (and the sweep's gossip interval), every admin's view converges.
    for admin in &admins {
        let within = (floor + gossip).saturating_sub(healed.elapsed());
        wait_for(&format!("{admin}'s view holds mesh1 ready and {fp} ({fp_id}) mesh1's one primary and the one fabric-primary within a floor of the heal"), within, || async {
            intact();
            let v = estate.node_values_at(admin).await;
            let fps: Vec<String> = v.iter().filter(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).collect();
            let primaries: Vec<String> = v.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).collect();
            let ready = mesh1.iter().all(|name| v.iter().any(|n| n["name"] == name.as_str() && n["status"] == "ready"));
            let same = v.iter().any(|n| n["name"] == fp && s(&n["node_id"]) == fp_id && s(&n["incarnation_id"]) == fp_inc && n["is_primary"] == true && n["is_fabric_primary"] == true);
            (ready && same && fps == vec![fp.to_string()] && primaries == vec![fp.to_string()]).then_some(())
        })
        .await;
    }
    // Within the same floor the cut admin's backbone arm hears the fabric-primary and ends; its span, read
    // below, is written at that end, so the estate is not stopped before it.
    let within = (floor + gossip).saturating_sub(healed.elapsed());
    wait_for(&format!("{victim}'s backbone arm on {fp} ends within a floor of the heal"), within, || async {
        named(&estate.spans(), "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().any(|sp| at(sp) >= cut_at && attr(sp, "node") == victim).then_some(())
    })
    .await;
    intact();
    // The teardown begins here: what its signals cause (an admin that outlives another submits its recovery)
    // is the teardown's work, not the executor's, so what is read below ends at this instant.
    let stop_at = now_ns();
    estate.stop().await;
    let spans = estate.spans();
    let before_stop = |sp: &Value| at(sp) < stop_at;
    // Nothing was opened, deleted or marked against the fabric-primary's birth, in any process.
    let opened: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| before_stop(sp) && attr(sp, "node_id") == fp_id).collect();
    assert!(opened.is_empty(), "no recovery was opened for the running fabric-primary: {opened:#?}");
    let rows: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| before_stop(sp) && attr(sp, "node_id") == fp_id).collect();
    assert!(rows.is_empty(), "no row of the running fabric-primary was written deleted: {rows:#?}");
    let marks: Vec<&Value> = spans
        .iter()
        .filter(|sp| before_stop(sp))
        .filter(|sp| sp["name"].as_str().is_some_and(|n| n.ends_with(".via-deleted-birth") || n.ends_with(".via-node-deleted") || n.ends_with(".via-node-deleting")))
        .filter(|sp| sp["attributes"].to_string().contains(&fp_id) || sp["attributes"].to_string().contains(&fp_inc))
        .collect();
    assert!(marks.is_empty(), "no view marks the running fabric-primary's birth: {marks:#?}");
    // The backbone arm ran on the fabric-primary at the cut admin, and its verdict was `running`.
    let ran: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| at(sp) >= cut_at && attr(sp, "node") == victim).collect();
    // The span's `verdict` is its last investigation's (after the heal, `reached`); each `running` read is
    // an event of it.
    let read_running = |sp: &Value| sp["events"].as_array().is_some_and(|ev| ev.iter().any(|e| s(&e["attributes"]["verdict"]) == "running" && s(&e["attributes"]["subject"]) == fp));
    assert!(ran.iter().any(|sp| attr(sp, "arm") == "backbone" && read_running(sp)), "{victim} ran fabric recovery's backbone arm on {fp} and read it running: {ran:#?}");
    eprintln!("the cut held {held_ms} ms; {victim}'s backbone arm: {:?}", ran.iter().map(|sp| (attr(sp, "arm"), read_running(sp), attr(sp, "outcome"))).collect::<Vec<_>>());

    // Q209 (node-recovery.md "Detection and the verdict": "After a retryable recovery-submit failure, that
    // exact birth becomes eligible again after a backoff doubling from the gossip interval to the staleness
    // floor"): the cut admin, mesh2's primary in its own view with no fabric-primary in it, fails each
    // recovery of the mesh2 births it cannot reach by name (Q118). Per birth, each failed submit records its
    // backoff, at most the floor, and the next submit of that birth, with its provider inspection, starts
    // no earlier than the backoff after the failed one ended. Must NOT happen: a failed submit made again,
    // with an inspection, on every tick (the held commit's flame: 22 inspections in 5 s).
    let end = |sp: &Value| sp["end_unix_nano"].as_u64().unwrap_or(0);
    let in_cut = |sp: &&Value| at(sp) >= cut_at && at(sp) <= healed_ns;
    let mut cut_submits: std::collections::BTreeMap<String, Vec<&Value>> = std::collections::BTreeMap::new();
    for sp in named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(in_cut).filter(|sp| attr(sp, "submitter") == victim && attr(sp, "node").starts_with("mesh2.")) {
        cut_submits.entry(attr(sp, "node_id")).or_default().push(sp);
    }
    let mut paced = 0usize;
    for (node_id, submits) in &mut cut_submits {
        submits.sort_by_key(|sp| at(sp));
        for pair in submits.windows(2) {
            let backoff_ms: u64 = attr(pair[0], "backoff_ms").parse().unwrap_or_else(|_| panic!("{victim}'s failed submit of {node_id} records its backoff: {}", pair[0]));
            assert!(Duration::from_millis(backoff_ms) <= floor, "{victim}'s backoff of {node_id} is at most the staleness floor: {backoff_ms} ms");
            let gap_ms = at(pair[1]).saturating_sub(end(pair[0])) / 1_000_000;
            assert!(gap_ms + 2 >= backoff_ms, "{victim} submitted {node_id} again {gap_ms} ms after a failed submit whose backoff was {backoff_ms} ms: {} then {}", pair[0], pair[1]);
            paced += 1;
        }
        let inspections = named(&spans, "rdm.node_admin.node.resolve.via-provider-inspection").into_iter().filter(in_cut).filter(|sp| attr(sp, "inspector") == victim && attr(sp, "node_id") == *node_id).count();
        assert!(inspections <= submits.len() + 1, "{victim} inspected {node_id} only for a submit: {inspections} inspections, {} submits", submits.len());
        eprintln!("Q209: {victim} under the {held_ms} ms cut: {node_id} submitted {} times (every tick: {}), inspected {inspections} times, backoffs {:?} ms", submits.len(), held_ms / gossip.as_millis().max(1) as u64, submits.iter().map(|sp| attr(sp, "backoff_ms")).collect::<Vec<_>>());
    }
    assert!(paced > 0, "{victim} failed a recovery of a mesh2 birth and submitted it again under the cut, so the backoff was read: {cut_submits:?}");
    // The held recovery of the cut admin is accepted and unfinished: mesh2's primary follows it and never
    // submits it as competing work (Q209); every open the fabric-primary makes for that birth names one attempt.
    let attempts: std::collections::BTreeSet<(String, String)> = named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| at(sp) >= cut_at && before_stop(sp) && attr(sp, "node_id") == victim_id).map(|sp| (bare(attr(sp, "build_id")), attr(sp, "attempt"))).collect();
    assert_eq!(attempts.len(), 1, "one attempt for {victim}'s accepted, unfinished recovery: {attempts:?}");
    let mut followed: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| at(sp) >= cut_at && before_stop(sp) && attr(sp, "submitter") == mesh2_primary && attr(sp, "node_id") == victim_id).collect();
    followed.sort_by_key(|sp| at(sp));
    assert!(followed.windows(2).all(|p| at(p[1]) >= end(p[0])), "{mesh2_primary} never submits {victim} while its submit is in flight: {followed:#?}");
    if let Some(run) = ran.first() {
        estate.record_trace_url(&s(&run["trace_id"]));
    }
}

/// CONTRACT (fabric-recovery.md step 3: "A runtime the executor can inspect running but cannot reach is
/// unreachable from the executor, not lost; a running fabric-primary its own mesh cannot reach is the
/// own-mesh arm's (Q153)"): mesh1's other node-admin, the lowest Ready one in its own view once
/// the fabric-primary is unheard, hears and is heard by no node (every datagram to or from its port
/// dropped) while the fabric-primary mesh1.admin.1 runs and the rest of mesh1 reaches it. The own-mesh arm's
/// node investigation of the fabric-primary has no eligible peer to tickle through, goes to the provider and
/// reads it `running`; held two gossip intervals past that read, the store root holds the fabric-primary's
/// row live and its name held by its node id, nothing is opened against its birth, and after the heal it
/// is the same birth, mesh1's primary and the fabric-primary. Must NOT happen: the fabric-primary's
/// `TerminateRuntime`, its row written deleted, its name released, or a recovery opened for its birth by
/// the isolated admin.
///
/// The isolated admin is kept to its provider read by the testkit's claimed-attempt cut on the
/// fabric-primary: mesh1's primary, the fabric-primary itself, reaches it through no peer, reads its
/// runtime `running` and recovers it at once (node-recovery.md step 3, Q111), and its claim of that
/// attempt is held before its dispatch, so the isolated admin is not re-created while the cut holds. Its
/// own ladder on the fabric-primary may ask peers its view still holds ready in the cut's first floor,
/// each bounded by the call's deadline (node-tickle.md "Probe").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_own_mesh_executor_that_reaches_the_fabric_primary_through_no_peer_and_reads_it_running_writes_nothing() {
    let test = "an_own_mesh_executor_that_reaches_the_fabric_primary_through_no_peer_and_reads_it_running_writes_nothing";
    let fp = "mesh1.admin.1";
    let shape = [("mesh1", 2, 2), ("mesh2", 2, 2)];
    let mut estate = estate_of(test, &shape).await;
    let nodes = estate.node_values().await;
    let holder = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    assert_eq!(s(&holder["name"]), fp, "the Day-0 admin holds the fabric-primary: {nodes:#?}");
    let (fp_id, fp_inc) = (s(&holder["node_id"]), s(&holder["incarnation_id"]));
    let victim = s(&nodes.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["name"] != fp).expect("mesh1's other node-admin")["name"]);
    estate.enter(&Estate::path(fp)).await;
    let root = estate.fabric_store_root();
    let intact = || {
        let (status, name) = stored_birth(&root, "mesh1", fp, &fp_id);
        assert!(status.as_deref().is_some_and(|st| st != "deleted"), "the fabric-primary's row stays live on the store root: {status:?}");
        assert_eq!(name.as_deref(), Some(fp_id.as_str()), "the fabric-primary's name stays held by its node id on the store root");
    };
    intact();

    let floor = rdm_transport::topology::staleness_floor();
    let gossip = rdm_transport::topology::gossip_interval();
    let now_ns = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    // The first attempt claimed after the cut, the isolated admin's recovery, is held before its dispatch.
    let fp_door = Door::at(&estate, fp).await;
    fp_door.arm("isolated-admin-recovery", json!({"kind": "claimed-attempt"})).await;
    let cut = match Partition::isolate(&udp_ports(&nodes, &[victim.clone()])) {
        Ok(p) => p,
        Err(why) if std::env::var("RDM_REQUIRE_NETFAULT").as_deref() != Ok("1") => {
            eprintln!("SKIPPED by name: the host cannot drop {victim}'s UDP: {why}");
            estate.stop().await;
            return;
        }
        Err(why) => panic!("RDM_REQUIRE_NETFAULT: {why}"),
    };
    let cut_at = now_ns();
    // Held past the own-mesh arm's node investigation of the fabric-primary: its inspection first, then,
    // unreached through no peer, the inspection's answer is the verdict (node-investigation.md, Q273).
    wait_for(&format!("{victim}'s own-mesh investigation of {fp} reaches its verdict, unreached"), floor * 6 + Duration::from_secs(60), || async {
        intact();
        named(&estate.spans(), "rdm.node.tickle.returns-unknown.via-unreached").into_iter().find(|sp| at(sp) >= cut_at && attr(sp, "caller") == victim && attr(sp, "node_id") == fp_id && attr(sp, "next") == "verdict").map(|_| ())
    })
    .await;
    // Two gossip intervals more: the inspection's verdict and whatever the executor does on it.
    let hold_until = std::time::Instant::now() + gossip * 2;
    while std::time::Instant::now() < hold_until {
        intact();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(cut);
    let healed = std::time::Instant::now();
    let held_ms = (now_ns() - cut_at) / 1_000_000;

    // Within a floor of the heal, the fabric-primary is the same birth, mesh1's primary and the one fabric-primary.
    let within = (floor + gossip).saturating_sub(healed.elapsed());
    wait_for(&format!("{fp} ({fp_id}) is mesh1's primary and the fabric-primary within a floor of the heal"), within, || async {
        intact();
        let v = estate.node_values_at(fp).await;
        v.iter().any(|n| n["name"] == fp && s(&n["node_id"]) == fp_id && s(&n["incarnation_id"]) == fp_inc && n["is_primary"] == true && n["is_fabric_primary"] == true).then_some(())
    })
    .await;
    intact();
    // The teardown begins here. estate.stop() signals the fabric-primary first, and an admin that outlives it
    // reads it exited and may open its recovery: that is the teardown's work, not the executor's, so what is
    // read against the running fabric-primary below ends at this instant.
    let stop_at = now_ns();
    estate.stop().await;
    let spans = estate.spans();
    let terminated: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| attr(sp, "step") == "TerminateRuntime" && attr(sp, "node") == fp && at(sp) >= cut_at && at(sp) < stop_at).collect();
    assert!(terminated.is_empty(), "the running fabric-primary's runtime was not terminated: {terminated:#?}");
    let opened: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-recovery").into_iter().filter(|sp| attr(sp, "node_id") == fp_id && attr(sp, "outcome") == "admitted" && at(sp) < stop_at).collect();
    assert!(opened.is_empty(), "no recovery was opened for the running fabric-primary: {opened:#?}");
    // The isolated admin's ladder went to the provider reached through no carrier: `via-unreached` is the
    // ladder nobody answered, direct or through a peer, after the inspection (Q273), whose answer is then
    // the verdict. Its `carriers` are the peers it asked (offline.rs
    // `carriers`: node-tickle.md step 3's eligible live peers, which its view may still hold ready
    // in the cut's first floor), never ones that reached it. The fabric-primary's process runs, so the
    // provider reads it `running` (the arm's own span, which names the verdict, stays open while it looks
    // again a floor on).
    let ladders: Vec<&Value> = named(&spans, "rdm.node.tickle.returns-unknown.via-unreached").into_iter().filter(|sp| at(sp) >= cut_at && attr(sp, "caller") == victim && attr(sp, "node_id") == fp_id && attr(sp, "next") == "verdict").collect();
    assert!(!ladders.is_empty(), "{victim}'s ladder on {fp} reached nobody and its verdict is the inspection's");
    eprintln!("the cut held {held_ms} ms; {victim}'s ladders on {fp}: {}", ladders.len());
    estate.record_trace_url(&s(&ladders[0]["trace_id"]));
}

/// CONTRACT (Q156; fabric-recovery.md "Nothing alive": "No node-admin is alive, the fabric's other nodes
/// alive or not. Nothing inside the fabric brings an admin back: a person or a script launches a
/// node-admin at the lost fabric-primary's path.name, with no launcher, given the fabric id, its mesh and
/// the store root ... It adopts what the store holds and mints no fabric or mesh id; it is a new birth
/// with its own node.id ... Before any recovery mutation other than the claim itself, it claims the
/// fabric-recovery attempt of the current Build for that mesh ... Otherwise the claimed attempt releases
/// the name, re-creates the birth as itself, adopts rafka-time from live members, and restores it as
/// fabric-primary by step 4 once Ready"): a one-mesh fabric's sole node-admin, the
/// fabric-primary, is killed while its two members serve. A person launches a node-admin at
/// mesh1.admin.1 over a new data dir, handed the fabric id, mesh1, the store root and the custody in its
/// environment. It claims the attempt before it writes anything else, releases the lost birth's name,
/// takes it under a new node.id, joins the members, adopts rafka-time from one of them and is the
/// fabric-primary and mesh1's primary; its attempt closed, it then grows mesh1 by a Build. Must NOT happen: a second fabric (a new fabric or mesh id on the
/// store), a write of the lost birth's row before the claim, a member re-created, or a member declaring
/// anything but ready after the loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sole_admin_lost_with_its_members_serving_is_adopted_by_a_launch_at_its_path_name_that_becomes_fabric_primary() {
    let test = "the_sole_admin_lost_with_its_members_serving_is_adopted_by_a_launch_at_its_path_name";
    let fp = "mesh1.admin.1";
    let shape = [("mesh1", 1, 2)];
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let nodes = estate.node_values().await;
    let holder = nodes.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("a fabric-primary");
    assert_eq!(s(&holder["name"]), fp, "the Day-0 admin holds the fabric-primary: {nodes:#?}");
    let lost_id = s(&holder["node_id"]);
    let mesh_id = estate.mesh_view("mesh1").await.id.map(|i| i.to_string());
    let members: Vec<(String, String)> = nodes.iter().filter(|n| n["kind"] != "node_admin").map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect();
    assert_eq!(members.len(), 2, "two members serve: {nodes:#?}");
    let custody = rdm_test::test_certs::custody_of(&estate.bootstrap_data_dir("mesh1"), "mesh1");
    let store = rdm_test::estate::estate_store_dir(&estate.root);
    let fabrics_before: Vec<String> = std::fs::read_dir(&store).unwrap().flatten().map(|e| e.file_name().to_string_lossy().to_string()).collect();

    let now_ns = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.kill_bootstrap();
    let lost_at = now_ns();
    let data_dir = estate.root.join("adopted-mesh1.admin.1");
    let fabric_id = estate.fabric_id.clone();
    let mut env: Vec<(&str, String)> = vec![("RDM_NODE_NAME", fp.into()), ("RDM_FABRIC_ID", fabric_id.clone()), ("RDM_MESH", "mesh1".into())];
    env.extend(custody);
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let adopted = estate.restart_admin_with(&data_dir, &env);
    assert_eq!(adopted.to_string(), fp, "the launch is born at the lost fabric-primary's path.name");
    estate.enter(&adopted).await;
    let after = estate.settles_to(&shape, Duration::from_secs(120)).await;
    let after: Vec<Value> = after.iter().map(|n| serde_json::to_value(n).unwrap()).collect();
    let new = after.iter().find(|n| n["name"] == fp).cloned().expect("a node at mesh1.admin.1");
    assert_ne!(s(&new["node_id"]), lost_id, "a new birth with its own node.id: {new}");
    wait_for("the adopted node-admin is the fabric-primary and mesh1's primary", Duration::from_secs(60), || async {
        let v = estate.node_values().await;
        v.iter().any(|n| n["name"] == fp && s(&n["node_id"]) != lost_id && n["is_fabric_primary"] == true && n["is_primary"] == true).then_some(())
    })
    .await;
    for (name, inc) in &members {
        assert!(after.iter().any(|n| s(&n["name"]) == *name && s(&n["incarnation_id"]) == *inc && n["status"] == "ready"), "{name} keeps its birth {inc}, ready: {after:#?}");
    }
    assert_eq!(estate.mesh_view("mesh1").await.id.map(|i| i.to_string()), mesh_id, "mesh1 keeps its MeshId");
    // The adoption's attempt closed at its Ready: the adopted fabric-primary runs the next Build on mesh1.
    let grown = [("mesh1", 1, 3)];
    estate.reconcile_shape("fabric1", &grown).await;
    estate.settles_to(&grown, Duration::from_secs(120)).await;
    let fabrics_after: Vec<String> = std::fs::read_dir(&store).unwrap().flatten().map(|e| e.file_name().to_string_lossy().to_string()).collect();
    assert_eq!(fabrics_after, fabrics_before, "no second fabric on the store");
    estate.stop().await;

    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let claims: Vec<&Value> = named(&spans, "rdm.node_admin.fabric.update.via-adoption-claim").into_iter().filter(|sp| at(sp) >= lost_at).collect();
    assert!(claims.len() == 1 && attr(claims[0], "outcome") == "won" && attr(claims[0], "lost_node_id") == lost_id, "one claim of the fabric-recovery attempt for the lost birth, won: {claims:#?}");
    let first_write = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| at(sp) >= lost_at && attr(sp, "node_id") == lost_id).map(at).min().expect("the lost birth's name released");
    assert!(at(claims[0]) < first_write, "the claim precedes the release of the lost birth's name");
    let pulled: Vec<&Value> = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted").into_iter().filter(|sp| at(sp) >= lost_at && attr(sp, "node") == fp).collect();
    assert!(pulled.len() == 1 && attr(pulled[0], "source") == "pull", "rafka-time adopted from a live member: {pulled:#?}");
    let left: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-declare-own")
        .into_iter()
        .filter(|sp| at(sp) >= lost_at && members.iter().any(|(m, _)| attr(sp, "node") == *m) && !attr(sp, "state").contains("Ready"))
        .collect();
    assert!(left.is_empty(), "no member left ready: {left:#?}");
    println!("trace_id={}", claims[0]["trace_id"].as_str().unwrap_or(""));
}
