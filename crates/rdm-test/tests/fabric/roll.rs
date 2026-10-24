//! product=mesh, feature=fabric-lifecycle, subfeature=roll, rung=MM, provider=process.
//!
//! A roll (proposal roll.md, R1-R5): `build.create { Change(Roll { bindings }) }` puts a running fabric's
//! members onto a new executable set as one Build. The set becomes the accepted topology's launch set,
//! and each member whose row records another sha256 for its kind is replaced at its `path.name`: its
//! `node.delete` (the whole stop, the connections delete inside it; a primary with a successor moved
//! first), then `node.create` over its kept data dir launching the set's binding. A mesh with one
//! node-admin gains a second first (R4); a type's only node is replaced with no update (R5). The Build is
//! `Complete` only after the fabric climbed.
//!
//! Every estate runs the testkit's faulted node-admin (`faults::binding_set`), so a cell can hold an
//! executor at a cut through its fault door. The new set is the same programs copied with one byte
//! appended: another sha256, the same behaviour, as a new build of them would be.

use crate::build_log;
use rdm_client::binding::{sha256_file, BindingSet, Candidate};
use rdm_client::{BuildFrame, CallEnd, Change, PathName, ScopeStatus};
use rdm_test::estate::{named, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(180);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "fabric-lifecycle".into(), subfeature: "roll".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// A fabric of the faulted node-admin born at `shape` (each `(mesh, node_admin, rpc_node)`), ready for
/// traffic: the set it started on is its launch set.
async fn fabric_of(test: &str, shape: &[(&str, u32, u32)]) -> (Estate, BindingSet) {
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let estate = Estate::bootstrap_external(owner(test), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", shape).await;
    estate.settles_to(shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    (estate, set)
}

/// `set` with the binding of each launch id in `kinds` copied under the estate's root with one byte
/// appended: the same program under another sha256, the set a roll moves to.
fn rolled(estate: &Estate, set: &BindingSet, kinds: &[&str]) -> BindingSet {
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    let dir = estate.root.join("rolled");
    std::fs::create_dir_all(&dir).unwrap();
    let mut next = set.clone();
    next.candidate = Candidate { sha: format!("{}-rolled", set.candidate.sha), build: "roll".into() };
    for b in next.bindings.iter_mut().filter(|b| kinds.contains(&b.launch_id.as_str())) {
        let to = dir.join(format!("{}-rolled", b.launch_id));
        std::fs::copy(&b.executable, &to).unwrap();
        std::fs::OpenOptions::new().append(true).open(&to).unwrap().write_all(&[0]).unwrap();
        std::fs::set_permissions(&to, std::fs::Permissions::from_mode(0o755)).unwrap();
        b.sha256 = sha256_file(&to).unwrap();
        b.executable = to;
    }
    next
}

/// A roll's stream followed to its terminal across the fabric-primary's own replacement: a stream that
/// ends `FabricPrimaryMoved`, or breaks, is resubmitted by its build_id at the fabric-primary a node-admin
/// of `admins` still answering names. Every frame, and whether the stream ended `FabricPrimaryMoved`.
async fn follow_roll(estate: &Estate, stream: rdm_client::BuildStream, admins: &[&str]) -> (Vec<BuildFrame>, bool) {
    let build = stream.build_id().clone();
    let followed = rdm_client::follow_with(stream, |id, _from| async move {
        let deadline = tokio::time::Instant::now() + SETTLE;
        loop {
            for admin in admins {
                // The admin's node.get first: a path the roll re-created holds a birth the estate learns
                // from a view that lists it, so each pass reaches the admins born since the last.
                if estate.node_views_at(&Estate::path(admin)).await.is_none() {
                    continue;
                }
                let Some(view) = estate.fabric_view_at(&Estate::path(admin)).await else { continue };
                let Some(fp) = view.fabric_primary else { continue };
                if estate.node_views_at(&fp).await.is_none() {
                    continue;
                }
                if let Ok(again) = estate.carrier_at(&fp).await.reattach(&id).await {
                    return Ok(again);
                }
            }
            assert!(tokio::time::Instant::now() < deadline, "no node-admin of {admins:?} named a fabric-primary that resubmitted Build {id}");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .unwrap_or_else(|e| panic!("roll {build} could not be followed to its terminal: {e}"));
    match followed.frames.last() {
        Some(BuildFrame::Complete { .. }) => {}
        other => panic!("roll {build} ended {other:?}: {:?}", followed.frames),
    }
    let moved = followed.frames.iter().any(|f| matches!(f, BuildFrame::FabricPrimaryMoved { .. }));
    (followed.frames, moved)
}

/// The executable `node`'s process runs.
async fn exe_of(estate: &Estate, node: &str) -> std::path::PathBuf {
    let pid = estate.pid_of(node).await;
    std::fs::read_link(format!("/proc/{pid}/exe")).unwrap_or_else(|e| panic!("{node} (pid {pid}): /proc exe: {e}"))
}

/// The binding of `kind`'s launch id in `set`.
fn exe_in(set: &BindingSet, kind: &str) -> std::path::PathBuf {
    set.bindings.iter().find(|b| b.launch_id == kind).unwrap_or_else(|| panic!("the set binds {kind}")).executable.clone()
}

/// What a member was before the roll: its view and its data dir.
#[derive(Debug, Clone)]
struct Member {
    view: Value,
    data_dir: String,
}

async fn members(estate: &Estate) -> BTreeMap<String, Member> {
    let mut out = BTreeMap::new();
    for v in estate.node_values().await {
        let name = s(&v["name"]);
        let data_dir = estate.data_dir(&name).await;
        out.insert(name, Member { view: v, data_dir });
    }
    out
}

/// Every member of `before` is at its `path.name` again, ReadyForTraffic as a new node (new node.id and
/// incarnation) over its kept data dir with its endpoint key, running `set`'s binding of its kind.
async fn replaced_on(estate: &Estate, before: &BTreeMap<String, Member>, set: &BindingSet, which: impl Fn(&str) -> bool) {
    for (name, m) in before.iter().filter(|(n, _)| which(n)) {
        let now = estate.until_node(name, &format!("{name} ready as a new node"), SETTLE, |n| {
            let v = serde_json::to_value(n?).expect("a view serializes");
            (v["status"] == "ready-for-traffic" && v["node_id"] != m.view["node_id"]).then_some(v)
        })
        .await;
        assert_ne!(now["incarnation_id"], m.view["incarnation_id"], "{name}: a new birth");
        assert_eq!(now["endpoint_id"], m.view["endpoint_id"], "{name}: its endpoint key kept, loaded from its kept data dir");
        assert_eq!(estate.data_dir(name).await, m.data_dir, "{name}: created over its kept data dir");
        let kind = s(&m.view["kind"]);
        assert_eq!(exe_of(estate, name).await, exe_in(set, &kind), "{name} runs the set's {kind}");
    }
}

/// The sha256 of `kind`'s launch id in `set`.
fn sha_in(set: &BindingSet, kind: &str) -> String {
    set.bindings.iter().find(|b| b.launch_id == kind).unwrap_or_else(|| panic!("the set binds {kind}")).sha256.clone()
}

/// The Node record of `node_id` in `mesh` on the estate's store root, as written.
fn record_of(estate: &Estate, mesh: &str, node_id: &str) -> Value {
    let file = estate.fabric_store_root().join(format!("nodes/{mesh}/records/{node_id}.json"));
    serde_json::from_slice(&std::fs::read(&file).unwrap_or_else(|e| panic!("{}: {e}", file.display()))).unwrap()
}

/// The index, in the Build's receipts (attempt order, then the order each was written), of the first
/// receipt of `operation`'s `step`.
fn first_receipt(receipts: &[rdm_core::build::StepReceipt], operation: &str, step: &str) -> usize {
    receipts.iter().position(|r| r.operation == operation && r.step == step).unwrap_or_else(|| panic!("the Build holds no {operation} {step}: {:?}", receipts.iter().map(|r| format!("{} {}", r.operation, r.step)).collect::<Vec<_>>()))
}

fn receipts_of<'a>(receipts: &'a [rdm_core::build::StepReceipt], operation: &str, step: &str) -> Vec<&'a rdm_core::build::StepReceipt> {
    receipts.iter().filter(|r| r.operation == operation && r.step == step).collect()
}

/// The fabric-primary now, the fabric's status as it holds it, and the Build whose topology it accepted.
async fn fabric_at_its_primary(estate: &Estate) -> (String, ScopeStatus, Option<String>) {
    let fp = estate.node_views().await.into_iter().find(|n| n.is_fabric_primary).map(|n| n.name.to_string()).expect("a fabric-primary");
    let view = rdm_client::Fabrics::new(estate.carrier_at(&Estate::path(&fp)).await).get().await.unwrap_or_else(|e| panic!("fabric.get at {fp}: {e}"));
    (fp, view.status, view.build_id)
}

/// CONTRACT (proposal roll.md steps 1-5, 8; R1-R3; states.md "Resize and roll"): a fabric of two meshes,
/// two node-admins in each, rolled onto a new set (node_admin and rpc_node both new builds). Every member
/// comes back at its `path.name` as a new node.id and incarnation over its kept data dir with its
/// endpoint key, running the set's binding of its kind. In each mesh its non-primary node-admin is
/// deleted before its primary one, and node-admins before rpc nodes. mesh1 holds the fabric-primary: the
/// caller's stream ends `FabricPrimaryMoved`, the same build_id resubmitted at the new fabric-primary
/// completes there, and the old fabric-primary's node is re-created on the set. The Build is `Complete`
/// only once the fabric climbed: at that moment the fabric-primary holds it ready-for-traffic under the
/// roll. After the roll, an ordinary create (a resize) from a replaced admin launches the set's
/// binary, and the resize's Build carries the set forward.
/// Must NOT happen: a member left on its old binary, a successor over a new data dir, a primary
/// replaced before its type's other members, or the roll complete before the climb.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_mesh_fabric_rolls_every_member_onto_the_set_at_its_path_over_its_kept_data_dir() {
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 1)];
    let (estate, set) = fabric_of("a_two_mesh_fabric_rolls_every_member_onto_the_set_at_its_path_over_its_kept_data_dir", &shape).await;
    let next = rolled(&estate, &set, &["node_admin", "rpc_node"]);
    let before = members(&estate).await;
    let fp_before = before.iter().find(|(_, m)| m.view["is_fabric_primary"] == true).map(|(n, _)| n.clone()).expect("a fabric-primary");
    assert!(fp_before.starts_with("mesh1."), "the fabric-primary is mesh1's: {fp_before}");

    let stream = estate.submit_change(Change::Roll { bindings: next.clone() }).await;
    let build = stream.build_id().to_string();
    let (frames, moved) = follow_roll(&estate, stream, &["mesh2.admin.1", "mesh2.admin.2", "mesh1.admin.1", "mesh1.admin.2"]).await;
    assert!(moved, "the stream ended FabricPrimaryMoved at the fabric-primary's own replacement, and the same build_id completed at the new fabric-primary: {frames:?}");
    // Complete only after the climb: the fabric-primary holds the fabric ready-for-traffic under the roll now.
    let (fp, status, accepted) = fabric_at_its_primary(&estate).await;
    assert_eq!((status, accepted.as_deref()), (ScopeStatus::ReadyForTraffic, Some(build.as_str())), "the roll completed only after the fabric climbed under it ({fp})");

    replaced_on(&estate, &before, &next, |_| true).await;
    let receipts = estate.carrier().await.get(&rdm_client::BuildId::parse(&build).unwrap()).await.unwrap_or_else(|e| panic!("build.get {build}: {e}")).steps;
    for mesh in ["mesh1", "mesh2"] {
        let admin = |primary: bool| before.iter().find(|(n, m)| n.starts_with(&format!("{mesh}.admin.")) && (m.view["is_primary"] == true) == primary).map(|(n, _)| n.clone()).expect("an admin");
        let (secondary, primary) = (admin(false), admin(true));
        let deleted = |n: &str| first_receipt(&receipts, &format!("node.delete:{n}"), "NodeDeleted");
        assert!(deleted(&secondary) < deleted(&primary), "{mesh}: its non-primary admin {secondary} is replaced before its primary {primary}");
        assert!(deleted(&primary) < deleted(&format!("{mesh}.rpc.1")), "{mesh}: node-admins before the other types");
        for n in before.keys().filter(|n| n.starts_with(&format!("{mesh}."))) {
            assert_eq!(receipts_of(&receipts, &format!("node.create:{n}"), "Complete").len(), 1, "{n} is created once");
        }
    }

    // After the roll an ordinary create from a replaced admin launches the set's binary, and the
    // resize's Build carries the set forward.
    let grown = [("mesh1", 2, 2), ("mesh2", 2, 2)];
    let resize = estate.reconcile_shape("fabric1", &grown).await;
    estate.settles_to(&grown, SETTLE).await;
    for n in ["mesh1.rpc.2", "mesh2.rpc.2"] {
        assert_eq!(exe_of(&estate, n).await, exe_in(&next, "rpc_node"), "{n}, created after the roll, runs the set's rpc node");
    }
    let resized = build_log::build_value(&estate.carrier().await, &resize.build_id.to_string()).await;
    assert_eq!(resized["topology"]["bindings"], serde_json::to_value(&next).unwrap(), "the resize's Build carries the roll's set");
    estate.shutdown().await;
}

/// CONTRACT (proposal roll.md step 6, R4 arm A; step 5 and R5 arm (a); node-primary-update.md
/// "No-successor cases": "the delete of a type's only node in its mesh ... is re-created at the same
/// `path.name` (node recovery, a roll's replacement); the type has no primary until the election names
/// the new birth ReadyForTraffic"): a fabric of one mesh with one node-admin and one rpc node is rolled.
/// The roll creates `mesh1.admin.2` first, from the set, and it is ReadyForTraffic and primary before
/// the original admin is replaced; it stays in the accepted topology. The only rpc node runs the whole
/// sequence: its `node.delete` first, with the stop and its connections delete inside it, no standalone
/// `node.connections.delete` or `node.mesh.leave` before it, no `no-secondary` refusal at any step;
/// re-created at `mesh1.rpc.1`, it is its type's primary once ReadyForTraffic.
/// Must NOT happen: the only admin replaced with no other listening admin, or the roll refused or
/// failed `no-secondary`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_with_one_admin_gains_a_second_first_and_its_only_rpc_node_is_replaced_with_no_update() {
    let shape = [("mesh1", 1, 1)];
    let (estate, set) = fabric_of("a_mesh_with_one_admin_gains_a_second_first_and_its_only_rpc_node_is_replaced_with_no_update", &shape).await;
    let next = rolled(&estate, &set, &["node_admin", "rpc_node"]);
    let before = members(&estate).await;

    let stream = estate.submit_change(Change::Roll { bindings: next.clone() }).await;
    let build = stream.build_id().to_string();
    // The added admin is learned while the original still serves: the stream moves to it.
    let added = estate.until_node("mesh1.admin.2", "mesh1.admin.2 ready", SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::ReadyForTraffic).map(|n| n.clone())).await;
    let (frames, moved) = follow_roll(&estate, stream, &["mesh1.admin.2", "mesh1.admin.1"]).await;
    assert!(moved, "the fabric-primary moved to the added admin: {frames:?}");

    replaced_on(&estate, &before, &next, |_| true).await;
    let rpc = estate.until_node("mesh1.rpc.1", "mesh1.rpc.1 primary", SETTLE, |n| n.filter(|n| n.is_primary && n.status == rdm_client::NodeStatus::ReadyForTraffic).map(|n| n.clone())).await;
    assert_ne!(serde_json::to_value(&rpc.node_id).unwrap(), before["mesh1.rpc.1"].view["node_id"], "the rpc type's primary is the new birth");

    let receipts = estate.carrier().await.get(&rdm_client::BuildId::parse(&build).unwrap()).await.unwrap_or_else(|e| panic!("build.get {build}: {e}")).steps;
    assert!(first_receipt(&receipts, "node.create:mesh1.admin.2", "Complete") < first_receipt(&receipts, "node.delete:mesh1.admin.1", "NodeDeleting"), "the second admin is ReadyForTraffic before the original is replaced");
    let rpc_delete: Vec<&str> = receipts.iter().filter(|r| r.operation == "node.delete:mesh1.rpc.1").map(|r| r.step.as_str()).collect();
    assert!(rpc_delete.contains(&"StopNode") && rpc_delete.contains(&"TerminateRuntime"), "the only rpc node's node.delete composes the whole stop: {rpc_delete:?}");
    assert!(!receipts.iter().any(|r| r.operation.starts_with("node.connections.delete:") || r.operation.starts_with("node.mesh.leave:")), "no standalone connections delete or mesh leave precedes a delete");
    let topology = build_log::build_value(&estate.carrier().await, &build).await;
    assert!(topology["topology"]["meshes"]["mesh1"]["nodes"].as_array().is_some_and(|n| n.iter().any(|p| p == "mesh1.admin.2")), "the added admin stays in the accepted topology: {}", topology["topology"]);
    assert_eq!(exe_of(&estate, "mesh1.admin.2").await, exe_in(&next, "node_admin"), "{} runs the set's node-admin", added.name);
    let mut estate = estate;
    estate.stop().await;
    let spans = estate.spans();
    assert!(named(&spans, "rdm.node_admin.build.reject.via-no-secondary").is_empty(), "no step was refused no-secondary");
    assert!(!spans.iter().any(|sp| sp.to_string().contains("cannot transfer: no secondary")), "no step failed no-secondary");
}

/// CONTRACT (proposal roll.md step 2, R3: "an ordinary create (a resize, an added node) from a replaced
/// or an unreplaced admin launches the new set ... and a node recovery still runs its row's bin"; "An
/// admin whose executable already hashes to the set's is not replaced and launches from the new set
/// all the same"; R6: "the Day-0 claim hashes the bootstrap admin's running executable ... and the
/// binding is recorded"): a roll whose set changes only the rpc node replaces every rpc node and no
/// node-admin, the bootstrap admin included: its row records the set's node-admin from its Day-0 claim,
/// and the fabric-primary does not move. Then a resize, an admin
/// restart and an ordinary node.create: every launch, by an admin the roll did not replace, runs the
/// set's rpc node, and the resize's Build carried the set; a member killed after it is recovered on the
/// binary its row records, the set's.
/// Must NOT happen: an unreplaced admin launching from the set it started with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_a_roll_an_unreplaced_admin_launches_the_set_and_a_recovery_runs_its_rows() {
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 1)];
    let (estate, set) = fabric_of("after_a_roll_an_unreplaced_admin_launches_the_set_and_a_recovery_runs_its_rows", &shape).await;
    let next = rolled(&estate, &set, &["rpc_node"]);
    let before = members(&estate).await;
    let stream = estate.submit_change(Change::Roll { bindings: next.clone() }).await;
    let (frames, moved) = follow_roll(&estate, stream, &["mesh2.admin.1", "mesh2.admin.2", "mesh1.admin.2", "mesh1.admin.1"]).await;
    assert!(!moved, "no node-admin is replaced, so the fabric-primary does not move: {frames:?}");
    replaced_on(&estate, &before, &next, |n| n.contains(".rpc.")).await;
    for (name, m) in before.iter().filter(|(n, _)| n.contains(".admin.")) {
        assert_eq!(estate.node_value(name).await["node_id"], m.view["node_id"], "{name}'s row records the set's node-admin: not replaced");
    }
    let day0 = record_of(&estate, "mesh1", &s(&before["mesh1.admin.1"].view["node_id"]));
    assert_eq!(day0["record"]["executable"]["sha256"], json!(sha_in(&set, "node_admin")), "the bootstrap admin's Day-0 claim recorded the binding it runs: {day0}");

    // A resize: mesh2's admins, none replaced, launch the set's rpc node.
    let grown = [("mesh1", 2, 1), ("mesh2", 2, 2)];
    let resize = estate.reconcile_shape("fabric1", &grown).await;
    estate.settles_to(&grown, SETTLE).await;
    assert_eq!(exe_of(&estate, "mesh2.rpc.2").await, exe_in(&next, "rpc_node"), "an unreplaced admin's create runs the set's rpc node");
    let resized = build_log::build_value(&estate.carrier().await, &resize.build_id.to_string()).await;
    assert_eq!(resized["topology"]["bindings"], serde_json::to_value(&next).unwrap(), "the resize's Build carries the roll's set");

    // An admin restart, then an ordinary node.create from it.
    let secondary = before.iter().find(|(n, m)| n.starts_with("mesh2.admin.") && m.view["is_primary"] != true).map(|(n, _)| n.clone()).expect("mesh2's other admin");
    let mut restart = estate.nodes_obj().await.restart(&Estate::path(&secondary)).await.unwrap_or_else(|e| panic!("node.restart {secondary}: {e}"));
    Estate::complete(&mut restart).await;
    let mut created = estate.nodes_obj().await.create(&rdm_client::NodeSpec { mesh: "mesh2".into(), kind: rdm_client::LaunchKind::RpcNode }).await.unwrap_or_else(|e| panic!("node.create in mesh2: {e}"));
    Estate::complete(&mut created).await;
    let added = estate.node_values().await.into_iter().map(|v| s(&v["name"])).filter(|n| n.starts_with("mesh2.rpc.") && n != "mesh2.rpc.1" && n != "mesh2.rpc.2").max().expect("the created rpc node");
    estate.until_node(&added, &format!("{added} ready"), SETTLE, |n| n.filter(|n| n.status == rdm_client::NodeStatus::ReadyForTraffic).map(|_| ())).await;
    assert_eq!(exe_of(&estate, &added).await, exe_in(&next, "rpc_node"), "a create after an admin restart runs the set's rpc node");

    // A node recovery runs the binary its row records: the set's.
    let victim = estate.node_value("mesh1.rpc.1").await;
    estate.kill_node("mesh1.rpc.1").await;
    estate.until_node("mesh1.rpc.1", "mesh1.rpc.1 recovered", SETTLE + crate::node_recovery_due(), |n| {
        let v = serde_json::to_value(n?).expect("a view serializes");
        (v["status"] == "ready-for-traffic" && v["node_id"] != victim["node_id"]).then_some(())
    })
    .await;
    assert_eq!(exe_of(&estate, "mesh1.rpc.1").await, exe_in(&next, "rpc_node"), "the recovered node runs its row's binary, the set's");
    estate.shutdown().await;
}

/// CONTRACT (proposal roll.md step 3, "Interruptions resume from observed state": "predecessor deleted
/// and the successor not yet created ...: the remaining work is the create ... No member is replaced
/// twice"; step 7: a member lost during the roll is node recovery): the executor of mesh2's rpc node's
/// replacement (mesh2's primary admin, which the roll does not replace) is held by its fault door at the
/// rpc node's `node.deleted` and killed once the fabric-primary's log holds that `NodeDeleted`. The
/// roll goes on: mesh2's other admin takes the mesh, the rpc node is created once at its path over its
/// kept data dir on the set, and the Build holds one `NodeDeleted` and one create `Complete` for it.
/// Must NOT happen: the rpc node deleted twice, created twice, or created over a new data dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_lost_after_a_members_delete_leaves_its_create_alone_to_the_next_attempt() {
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 1)];
    let (estate, set) = fabric_of("an_executor_lost_after_a_members_delete_leaves_its_create_alone_to_the_next_attempt", &shape).await;
    let next = rolled(&estate, &set, &["rpc_node"]);
    let before = members(&estate).await;
    let p = before.iter().find(|(n, m)| n.starts_with("mesh2.admin.") && m.view["is_primary"] == true).map(|(n, _)| n.clone()).expect("mesh2's primary");
    let na2 = before.iter().find(|(n, m)| n.starts_with("mesh2.admin.") && m.view["is_primary"] != true).map(|(n, _)| n.clone()).expect("mesh2's other admin");
    const NODE: &str = "mesh2.rpc.1";
    let door = Door::at(&estate, &p).await;
    door.arm("rpc-deleted", json!({"kind": "event", "event": "deleted", "node": NODE})).await;

    let stream = estate.submit_change(Change::Roll { bindings: next.clone() }).await;
    let build = stream.build_id().to_string();
    let held = door.wait_held("rpc-deleted").await;
    assert_eq!((s(&held["hit"]["build_id"]), s(&held["hit"]["operation"])), (build.clone(), format!("node.delete:{NODE}")), "{p} is held at the rpc node's node.deleted: {held}");
    // The fabric-primary now (the roll replaced the bootstrap admin, whose row records no executable,
    // before mesh2's members) holds the NodeDeleted before P is killed.
    let fp = estate.fabric_view_at(&Estate::path(&na2)).await.and_then(|v| v.fabric_primary).expect("mesh2's other admin names the fabric-primary");
    estate.node_views_at(&fp).await.expect("the fabric-primary answers node.get");
    build_log::build_fact(&estate.carrier_at(&fp).await, &build, &format!("{NODE}'s NodeDeleted"), SETTLE, |b| b.steps.iter().any(|r| r.operation == format!("node.delete:{NODE}") && r.step == "NodeDeleted"))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    estate.kill_node(&p).await;

    follow_roll(&estate, stream, &["mesh1.admin.2", "mesh1.admin.1", "mesh2.admin.1", "mesh2.admin.2"]).await;
    replaced_on(&estate, &before, &next, |n| n == NODE).await;
    let receipts = estate.carrier().await.get(&rdm_client::BuildId::parse(&build).unwrap()).await.unwrap_or_else(|e| panic!("build.get {build}: {e}")).steps;
    assert_eq!(receipts_of(&receipts, &format!("node.delete:{NODE}"), "NodeDeleted").len(), 1, "{NODE} is deleted once");
    assert_eq!(receipts_of(&receipts, &format!("node.create:{NODE}"), "Complete").len(), 1, "{NODE} is created once");
    estate.shutdown().await;
}

/// CONTRACT (proposal roll.md step 3: "successor created and its receipt not written (the row names a
/// live birth on the set's sha): nothing remains for it ... No member is replaced twice"): the executor
/// of mesh2's rpc node's replacement is held by its fault door at the create's `Complete`, the successor
/// already ReadyForTraffic on the set, and killed. The next attempt runs nothing for the rpc node: the
/// successor born then is the one at the path when the roll completes, and the Build holds one
/// `NodeDeleted` of it.
/// Must NOT happen: the successor deleted or a third birth created at the path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_executor_lost_after_a_successors_check_in_leaves_nothing_for_it() {
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 1)];
    let (estate, set) = fabric_of("an_executor_lost_after_a_successors_check_in_leaves_nothing_for_it", &shape).await;
    let next = rolled(&estate, &set, &["rpc_node"]);
    let before = members(&estate).await;
    let p = before.iter().find(|(n, m)| n.starts_with("mesh2.admin.") && m.view["is_primary"] == true).map(|(n, _)| n.clone()).expect("mesh2's primary");
    const NODE: &str = "mesh2.rpc.1";
    let door = Door::at(&estate, &p).await;
    door.arm("rpc-created", json!({"kind": "receipt", "step": "Complete", "operation": format!("node.create:{NODE}")})).await;

    let stream = estate.submit_change(Change::Roll { bindings: next.clone() }).await;
    let build = stream.build_id().to_string();
    door.wait_held("rpc-created").await;
    let successor = estate.until_node(NODE, &format!("{NODE}'s successor ready"), SETTLE, |n| {
        let v = serde_json::to_value(n?).expect("a view serializes");
        (v["status"] == "ready-for-traffic" && v["node_id"] != before[NODE].view["node_id"]).then_some(v)
    })
    .await;
    estate.kill_node(&p).await;

    follow_roll(&estate, stream, &["mesh1.admin.2", "mesh1.admin.1", "mesh2.admin.1", "mesh2.admin.2"]).await;
    let now = estate.node_value(NODE).await;
    assert_eq!(now["node_id"], successor["node_id"], "the successor whose create was never receipted is the member: nothing ran for it again");
    assert_eq!(exe_of(&estate, NODE).await, exe_in(&next, "rpc_node"));
    let receipts = estate.carrier().await.get(&rdm_client::BuildId::parse(&build).unwrap()).await.unwrap_or_else(|e| panic!("build.get {build}: {e}")).steps;
    assert_eq!(receipts_of(&receipts, &format!("node.delete:{NODE}"), "NodeDeleted").len(), 1, "{NODE} is deleted once");
    estate.shutdown().await;
}

/// The refusal a call ended with.
fn rejected(what: &str, call: Result<rdm_client::BuildStream, CallEnd>) -> (String, String) {
    match call {
        Err(CallEnd::Rejected { reason, detail }) => (reason, detail),
        Err(other) => panic!("{what}: expected a refusal by name, got {other}"),
        Ok(s) => panic!("{what}: the fabric-primary accepted Build {}", s.build_id()),
    }
}

/// CONTRACT (proposal roll.md step 1: "Refused `fabric-not-ready` until the fabric first reached
/// ready-for-traffic, as every recipe is ... Any defect is refused by name and nothing is accepted"):
/// while the initial fabric is held in state-sync a roll is refused `fabric-not-ready`, the detail naming
/// the roll and the state. Once ready, a set whose rpc node does not hash to its sha and a set that binds
/// no rpc node (the topology runs one) are each refused `bindings-refused`, naming the defect and the
/// launch id; nothing is accepted, and `Fabric.build_id` does not move.
/// Must NOT happen: a roll accepted before the initial fabric is ready, or onto a set that cannot
/// launch a member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_roll_is_refused_by_name_before_the_fabric_is_ready_and_for_each_defect_of_its_set() {
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let test = "a_roll_is_refused_by_name_before_the_fabric_is_ready_and_for_each_defect_of_its_set";
    let estate = Estate::bootstrap_external_with_env(owner(test), "fabric1", "mesh1", &set, &sha, &["rpc_node"], &[("RDM_TEST_APP", "hold")]).await.expect("the faulted-admin binding set is accepted");
    let door = Door::at(&estate, "mesh1.admin.1").await;
    let shape = [("mesh1", 2, 1)];
    let first = estate.reconcile_shape("fabric1", &shape).await.build_id.to_string();
    estate.settled_views(&shape).await;
    door.wait_held("state-sync").await;
    let next = rolled(&estate, &set, &["rpc_node"]);
    let carrier = estate.carrier().await;
    let (reason, detail) = rejected("a roll in state-sync", carrier.create(Change::Roll { bindings: next.clone() }).await);
    assert_eq!(reason, "fabric-not-ready", "{detail}");
    assert!(detail.contains("roll") && detail.contains("state-sync"), "the detail names the roll and the state: {detail}");

    door.release("state-sync").await;
    estate.until_fabric_ready(SETTLE).await;
    let mut mismatched = next.clone();
    mismatched.bindings.iter_mut().filter(|b| b.launch_id == "rpc_node").for_each(|b| b.sha256 = "0".repeat(64));
    let (reason, detail) = rejected("a set that does not hash to its sha", estate.carrier().await.create(Change::Roll { bindings: mismatched }).await);
    assert_eq!(reason, "bindings-refused", "{detail}");
    assert!(detail.contains("hash-mismatch") && detail.contains("rpc_node"), "the defect and the launch id are named: {detail}");
    let unbound = BindingSet { launch_ids: vec!["node_admin".into()], bindings: next.bindings.iter().filter(|b| b.launch_id == "node_admin").cloned().collect(), ..next.clone() };
    let (reason, detail) = rejected("a set that binds no rpc node", estate.carrier().await.create(Change::Roll { bindings: unbound }).await);
    assert_eq!(reason, "bindings-refused", "{detail}");
    assert!(detail.contains("not-required-by-the-set") && detail.contains("rpc_node"), "{detail}");
    assert_eq!(estate.accepted_build_id().await, first, "nothing is accepted");
    estate.shutdown().await;
}

/// CONTRACT (proposal roll.md R6: "with `RDM_EXECUTABLE_BINDINGS` set, that hash must equal the set's
/// `node_admin` sha, else the start is refused by name (a valid binding file proves what was configured,
/// not what started this process)"): a bootstrap admin started with a set whose `node_admin` binding is
/// another executable than the one running is refused at start, naming both hashes, and bootstraps
/// nothing: no Node record is written. Must NOT happen: the admin comes up recording a binding it does
/// not run.
#[test]
fn a_bootstrap_admin_whose_set_binds_another_node_admin_than_it_runs_is_refused_and_bootstraps_nothing() {
    let cap = crate::common::capture("a_bootstrap_admin_whose_set_binds_another_node_admin_than_it_runs_is_refused_and_bootstraps_nothing");
    cap.run(crate::common::Threads::Current, async move {
        let dir = std::env::temp_dir().join(format!("roll-r6-refused-{}", rdm_core::NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        // The set binds the testkit's faulted node-admin; this process is the cell binary.
        let sha = candidate_sha();
        let set = binding_set(&sha);
        let file = dir.join("executable-bindings.json");
        std::fs::write(&file, serde_json::to_vec_pretty(&set).unwrap()).unwrap();
        let (d, f) = (dir.display().to_string(), file.display().to_string());
        let cfg = rdm_admin::admin::AdminConfig::from_env(|k| match k {
            "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(d.clone()),
            "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
            "MESH_SPAWN_TYPE" => Some("process".into()),
            "RDM_EXECUTABLE_BINDINGS" => Some(f.clone()),
            "RDM_EXECUTABLE_CANDIDATE" => Some(sha.clone()),
            _ => None,
        })
        .expect("the set is a valid configuration");
        let wiring = rdm_admin::wiring::Wiring { certs: rdm_test::test_certs::root_choice(), fabric_hooks: rdm_admin::wiring::FabricHooks::no_app_work(), ..rdm_admin::wiring::Wiring::default() };
        let why = rdm_admin::admin::start_with(cfg, wiring).await.err().expect("a set binding another node-admin than the one running is refused");
        assert!(why.contains("refusing to start") && why.contains(&sha_in(&set, "node_admin")) && why.contains("hashes to"), "the refusal names the running executable's hash and the set's: {why}");
        let records: Vec<_> = walk(&dir).into_iter().filter(|p| p.components().any(|c| c.as_os_str() == "records")).collect();
        assert!(records.is_empty(), "nothing bootstrapped: no Node record is written: {records:?}");
        let _ = std::fs::remove_dir_all(&dir);
    })
}

/// Every file under `dir`.
fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

/// CONTRACT (proposal roll.md R6: "with no set, the running executable's hash is recorded as the
/// built-in binding. A built-in launch records the executable it resolved from `bin_dir`, hashed, the
/// same way"): on a fabric started with no binding set, the bootstrap admin's row records its running
/// executable's sha256 and an rpc node's row the sha256 of the `bin_dir` executable it was launched
/// from. Must NOT happen: a row that records nothing it runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_a_built_in_fabric_every_row_records_the_hash_of_the_executable_its_birth_runs() {
    let estate = Estate::bootstrap(owner("on_a_built_in_fabric_every_row_records_the_hash_of_the_executable_its_birth_runs"), "fabric1", "mesh1").await;
    let shape = [("mesh1", 1, 1)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, SETTLE).await;
    for name in ["mesh1.admin.1", "mesh1.rpc.1"] {
        let record = record_of(&estate, "mesh1", &s(&estate.node_value(name).await["node_id"]));
        // The bootstrap admin is the estate's own process, launched by no pipeline: the binary it starts.
        let exe = if name == "mesh1.admin.1" { rdm_test::estate::binary("rdm-node-admin") } else { exe_of(&estate, name).await };
        assert_eq!(record["record"]["executable"]["sha256"], json!(sha256_file(&exe).unwrap()), "{name}'s row records the hash of {} it runs: {record}", exe.display());
    }
    estate.shutdown().await;
}

/// CONTRACT (proposal roll.md step 2: "the forwarding of `bindings_env` to a launched admin goes; the
/// bootstrap env is day 0's input only"; the container cell: "a replacement admin launches from the
/// current Build's set after the bootstrap admin's `RDM_EXECUTABLE_BINDINGS` file is gone, and the
/// executable directories its container mounts are read-only (a write inside them refused), mounted at
/// their host paths"): on a container fabric, the bootstrap admin's bindings file removed, a roll of the
/// node-admins replaces each in a container that mounts the set's executable directory at its host path,
/// read-only, a write inside it refused; a resize after it launches from the set. Opt-in: the
/// container-proof step (`RDM_CONTAINER_PROOF=1`, or `RDM_REQUIRE_CONTAINER=1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_container_fabrics_replacement_admin_launches_from_the_builds_set_and_reads_it_read_only() {
    if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
        eprintln!("SKIP container roll: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
        return;
    }
    let test = "a_container_fabrics_replacement_admin_launches_from_the_builds_set_and_reads_it_read_only";
    let sha = candidate_sha();
    let mut set = binding_set(&sha);
    for b in set.bindings.iter_mut() {
        b.image = Some(rdm_test::estate::RUNTIME_IMAGE.into());
    }
    let estate = Estate::bootstrap_external(Owner { provider: "container".into(), ..owner(test) }, "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the container set is accepted");
    // Two meshes, as every roll cell: the estate relearns each re-created admin from a live view.
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 1)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    std::fs::remove_file(estate.root.join("executable-bindings.json")).expect("the bootstrap admin's bindings file");
    let next = rolled(&estate, &set, &["node_admin", "rpc_node"]);
    let before = members(&estate).await;
    let stream = estate.submit_change(Change::Roll { bindings: next.clone() }).await;
    follow_roll(&estate, stream, &["mesh2.admin.1", "mesh2.admin.2", "mesh1.admin.1", "mesh1.admin.2"]).await;
    let dir = exe_in(&next, "node_admin").parent().unwrap().to_path_buf();
    for name in ["mesh1.admin.1", "mesh1.admin.2", "mesh2.admin.1", "mesh2.admin.2"] {
        estate.until_node(name, &format!("{name} replaced"), SETTLE, |n| {
            let v = serde_json::to_value(n?).expect("a view serializes");
            (v["status"] == "ready-for-traffic" && v["node_id"] != before[name].view["node_id"]).then_some(())
        })
        .await;
        let id = estate.container_of(name).await.unwrap_or_else(|| panic!("{name} runs in a container"));
        let mounts = std::process::Command::new("docker").args(["inspect", "--format", "{{json .Mounts}}", &id]).output().unwrap();
        let mounts: Value = serde_json::from_slice(&mounts.stdout).unwrap();
        let held = mounts.as_array().unwrap().iter().find(|m| m["Source"] == json!(dir.display().to_string())).unwrap_or_else(|| panic!("{name} mounts the set's executable directory {}: {mounts}", dir.display()));
        assert_eq!((held["Destination"].clone(), held["RW"].clone()), (json!(dir.display().to_string()), json!(false)), "{name}: at its host path, read-only: {held}");
        let wrote = std::process::Command::new("docker").args(["exec", &id, "/bin/sh", "-c", &format!("echo x > {}/written", dir.display())]).output();
        assert!(wrote.map(|o| !o.status.success()).unwrap_or(true), "{name}: a write inside the mounted executable directory is refused");
    }
    let grown = [("mesh1", 2, 2), ("mesh2", 2, 1)];
    estate.reconcile_shape("fabric1", &grown).await;
    estate.settles_to(&grown, SETTLE).await;
    let added = record_of(&estate, "mesh1", &s(&estate.node_value("mesh1.rpc.2").await["node_id"]));
    assert_eq!(added["record"]["executable"]["sha256"], json!(sha_in(&next, "rpc_node")), "the resize launched the set's rpc node: {added}");
    estate.shutdown().await;
}
