//! A node's executable replacement (build-deployment.md §3; Q377-Q381, D10-D15): a `Create` naming a
//! held path and name of a node that is not a node-admin, with an executable other than the one its row
//! records, is replaced by RDM in its mesh's section, one path at a time: the node-type primary's role
//! moves first when another node of its type is in the mesh, the held birth is deleted, and a node is
//! created at the same path and name with the Build entry's executable, config and tags, over the held
//! birth's data dir when the entry's `storage_persistence` is persistent. The same executable, and a
//! node-admin's path, stay `name-held`.
use rdm_client::{BuildFrame, FabricDesired, MeshDesired, NodeStatus, NodeView};
use rdm_core::binding::Binding;
use rdm_core::build::{BuildChange, Nodes};
use rdm_core::meta::{PersistentDisposition, StoragePersistence};
use rdm_core::PathName;
use rdm_test::estate::{named, wait_for_observed, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-build".into(),
        subfeature: "executable-replacement".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn at(path: &str, name: &str) -> PathName {
    PathName::new(path, name).unwrap()
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn persistent() -> StoragePersistence {
    StoragePersistence::Persistent { disposition: PersistentDisposition::Preserve }
}

/// A fabric of mesh1 (its one node-admin, the fabric-primary) and, when `mesh` is `mesh2`, mesh2 with
/// its own node-admin; `mesh` holds `types`, each `(type, count)`. Ready.
async fn estate_with(test: &str, mesh: &str, types: &[(&str, u32)]) -> Estate {
    let estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    let held = |name: &str| MeshDesired::of(name, std::iter::once(("node-admin".to_string(), 1)).chain(types.iter().filter(|_| name == mesh).map(|(t, n)| (t.to_string(), *n))));
    let meshes = if mesh == "mesh1" { vec![held("mesh1")] } else { vec![held("mesh1"), held(mesh)] };
    estate.resize(&FabricDesired { fabric: "fabric1".into(), meshes }).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate
}

/// The testkit's executable for `type`, copied by a child process (no write fd of this process, which
/// a fork by another cell's thread would inherit: ETXTBSY) into `<estate root>/another-bin`: another
/// path, the same bytes and sha256, so another binding than the row records (D14).
fn another_executable(estate: &Estate, r#type: &str) -> Binding {
    let from = estate.executables.of(r#type);
    let dir = estate.root.join("another-bin");
    let to = dir.join(from.executable.file_name().expect("an executable file"));
    copy_executable(&from.executable, &to);
    Binding { executable: to, ..from }
}

fn copy_executable(from: &std::path::Path, to: &std::path::Path) {
    let dir = to.parent().expect("a directory").display().to_string();
    let ok = std::process::Command::new("sh").args(["-c", "mkdir -p \"$1\" && cp \"$2\" \"$3\" && chmod 755 \"$3\"", "copy", &dir, &from.display().to_string(), &to.display().to_string()]).status().unwrap();
    assert!(ok.success(), "{} is copied to {}", from.display(), to.display());
}

/// The held node `held` named again whole by a Create's Nodes schema, on `executable`, its storage
/// `persistence`: what Rafka sends to redeploy a node's binary (executable-replacement.md §5).
fn entry_of(held: &NodeView, executable: Binding, persistence: StoragePersistence) -> Nodes {
    Nodes { path: held.path.clone(), name: held.name.clone(), mesh: held.mesh(), r#type: held.r#type.clone(), storage_persistence: Some(persistence), executable: Some(executable), tags: held.tags.clone(), config: held.config.clone() }
}

fn create(nodes: Vec<Nodes>) -> BuildChange {
    BuildChange::Create { meshes: Vec::new(), nodes }
}

/// The index of the first frame of `operation` (at `step`, when named) that ended complete.
fn frame(frames: &[BuildFrame], operation: &str, step: Option<&str>) -> Option<usize> {
    frames.iter().position(|f| matches!(f, BuildFrame::Step { operation: o, step: t, result, .. } if o == operation && step.is_none_or(|w| w == t) && matches!(result, rdm_core::build::StepResult::Complete)))
}

fn op(action: &str, node_id: &rdm_core::NodeId) -> String {
    format!("{action}:{node_id}")
}

/// The node at `path` once a birth other than `old` stands there Ready.
async fn new_birth(estate: &Estate, path: &PathName, old: &rdm_core::NodeId) -> NodeView {
    estate.until_node(path, &format!("{path:?} holds a new node.id, ready"), Duration::from_secs(60), |n| n.filter(|n| n.node_id != *old && n.state == NodeStatus::Ready).cloned()).await
}

// @feature: fabric-build
/// CONTRACT (build-deployment.md §3; build.md §2 gate "A created node's path and name are free, or held
/// by a node that is not a node-admin and named with another executable"): mesh2's one broker is named
/// again by a Create with another executable. The Build deletes its held birth and creates a new node.id
/// at the same path and name, on the new executable, Ready, and only then is the Build Complete: the
/// create's `WaitForNodeReady` and `Complete` steps are framed before its terminal. The old node.id's
/// record stands Deleted. Must NOT happen: the Create refused `name-held`, the Build Complete with the old
/// birth still at the path, or the new node on the old executable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_naming_a_held_brokers_path_with_another_executable_replaces_the_node() {
    let cell = "a_create_naming_a_held_brokers_path_with_another_executable_replaces_the_node";
    let mut estate = estate_with(cell, "mesh2", &[("broker", 1)]).await;
    let path = at("mesh2.broker", "1");
    let before = estate.node_view(&path).await;
    let executable = another_executable(&estate, "broker");
    let stream = estate.submit_change(create(vec![entry_of(&before, executable.clone(), persistent())])).await;
    let done = estate.follow(stream).await;
    let after = new_birth(&estate, &path, &before.node_id).await;
    assert_eq!(after.executable.as_ref(), Some(&executable), "{path:?}'s new node runs the Create's executable: {after:?}");
    assert_ne!(after.incarnation, before.incarnation, "a new incarnation: {after:?}");
    let created = op("node.create", &after.node_id);
    let ready = frame(&done.frames, &created, Some("WaitForNodeReady")).unwrap_or_else(|| panic!("the new node's create waited for its Ready inside Build {}: {:?}", done.build_id, done.frames));
    let complete = frame(&done.frames, &created, Some("Complete")).expect("its create completed");
    let deleted = frame(&done.frames, &op("node.delete", &before.node_id), Some("Complete")).unwrap_or_else(|| panic!("the held birth {} was deleted by the Build: {:?}", before.node_id, done.frames));
    assert!(deleted < ready && ready < complete, "the held birth is deleted, then the new node created to Ready, all inside the Build: delete {deleted}, ready {ready}, complete {complete}");
    assert!(matches!(done.frames.last(), Some(BuildFrame::Complete)), "the Build is Complete after the new node is Ready: {:?}", done.frames.last());
    assert_eq!(estate.node_view_by_id(&before.node_id).await.state, NodeStatus::Deleted, "the held birth's record stands Deleted");
    estate.stop().await;
    let spans = estate.spans();
    let replaced: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-replacement").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == after.node_id.as_str()).collect();
    assert_eq!(replaced.len(), 1, "the new node's create names the birth it replaces once: {replaced:?}");
    assert_eq!(s(&replaced[0]["attributes"]["predecessor_node_id"]), before.node_id.as_str(), "{}", replaced[0]);
    estate.record_trace_url(replaced[0]["trace_id"].as_str().unwrap());
}

// @feature: fabric-build
/// CONTRACT (build-deployment.md §3: "The new node takes the held birth's data dir when its own entry's
/// `storage_persistence` is `persistent` and the directory exists; otherwise it gets a new one"; Q378,
/// Q381): mesh1's broker and gateway are both named again with another executable, the broker's entry
/// `Persistent`, the gateway's `Ephemeral`. The broker's new node runs on the old broker's data dir; the
/// gateway's new node on a new one. Must NOT happen: the persistent node started on a new dir, or the
/// ephemeral one on its predecessor's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_persistent_replacement_takes_the_old_data_dir_and_an_ephemeral_one_a_new_dir() {
    let cell = "a_persistent_replacement_takes_the_old_data_dir_and_an_ephemeral_one_a_new_dir";
    let mut estate = estate_with(cell, "mesh1", &[("broker", 1), ("gateway", 1)]).await;
    let (broker, gateway) = (at("mesh1.broker", "1"), at("mesh1.gateway", "1"));
    let (b, g) = (estate.node_view(&broker).await, estate.node_view(&gateway).await);
    let (b_dir, g_dir) = (b.storage.data_dir.clone().expect("the broker's data dir"), g.storage.data_dir.clone().expect("the gateway's data dir"));
    let nodes = vec![entry_of(&b, another_executable(&estate, "broker"), persistent()), entry_of(&g, another_executable(&estate, "gateway"), StoragePersistence::Ephemeral)];
    let stream = estate.submit_change(create(nodes)).await;
    estate.follow(stream).await;
    let (b2, g2) = (new_birth(&estate, &broker, &b.node_id).await, new_birth(&estate, &gateway, &g.node_id).await);
    assert_eq!(b2.storage.data_dir.as_deref(), Some(b_dir.as_str()), "the persistent broker's new node runs on its predecessor's data dir: {b2:?}");
    assert_ne!(g2.storage.data_dir.as_deref(), Some(g_dir.as_str()), "the ephemeral gateway's new node runs on a new data dir: {g2:?}");
    assert!(g2.storage.data_dir.is_some(), "and it has one: {g2:?}");
    estate.stop().await;
    let spans = estate.spans();
    let inherited = |id: &rdm_core::NodeId| named(&spans, "rdm.node_admin.node.create.via-replacement").into_iter().find(|sp| s(&sp["attributes"]["node_id"]) == id.as_str()).map(|sp| sp["attributes"]["inherited"].clone()).unwrap_or_else(|| panic!("the create of {id} names its replacement"));
    assert_eq!(inherited(&b2.node_id), "true", "the broker's create names the dir it took");
    assert_eq!(inherited(&g2.node_id), "false", "the gateway's create names that it took none");
}

// @feature: fabric-build
/// CONTRACT (build-deployment.md §3: "RDM then replaces the node in the mesh's section, one path at a
/// time: the node-type primary's role moves first when another node of its type is in the mesh"; D12):
/// mesh2 holds two brokers, one its type's primary; one Create names both with another executable, the
/// primary first. The non-primary is replaced first, its new node Ready, and only then is the primary's
/// role moved, by node.primary.update, to that new node, and the primary deleted and created again. The
/// type's nodes are never both gone. Every node of mesh2 then holds that new node as the broker primary,
/// and node.get answers it the one primary broker: the re-created primary path's create commands no
/// primary, its mesh-primary reading the answer of the move it commanded (Q433, Q355), whether or not
/// the move's gossip reached it first. Must NOT happen: the primary deleted first, both deleted before
/// either is created again, the role moved to a node about to be deleted, or a replacement's create
/// commanding the broker primary over the move (rdm#379: two brokers primary at one revision,
/// http://192.168.68.99:16687/trace/7e060490d2dee5d68ad6294ba7662efa).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_brokers_are_replaced_one_at_a_time_the_primary_last_its_role_moved_to_the_replaced_one() {
    let cell = "two_brokers_are_replaced_one_at_a_time_the_primary_last_its_role_moved_to_the_replaced_one";
    let mut estate = estate_with(cell, "mesh2", &[("broker", 2)]).await;
    let views = [estate.node_view(&at("mesh2.broker", "1")).await, estate.node_view(&at("mesh2.broker", "2")).await];
    let primary = views.iter().find(|v| v.is_primary).cloned().unwrap_or_else(|| panic!("one broker is its type's primary: {views:?}"));
    let other = views.iter().find(|v| !v.is_primary).cloned().expect("the other broker");
    let nodes = vec![entry_of(&primary, another_executable(&estate, "broker"), persistent()), entry_of(&other, another_executable(&estate, "broker"), persistent())];
    let stream = estate.submit_change(create(nodes)).await;
    let done = estate.follow(stream).await;
    let other2 = new_birth(&estate, &other.location(), &other.node_id).await;
    let primary2 = new_birth(&estate, &primary.location(), &primary.node_id).await;
    let other_deleted = frame(&done.frames, &op("node.delete", &other.node_id), Some("Complete")).expect("the non-primary's delete");
    let other_created = frame(&done.frames, &op("node.create", &other2.node_id), Some("Complete")).expect("the non-primary's create");
    let primary_first = frame(&done.frames, &op("node.delete", &primary.node_id), None).expect("the primary's delete");
    let primary_created = frame(&done.frames, &op("node.create", &primary2.node_id), Some("Complete")).expect("the primary's create");
    assert!(other_deleted < other_created && other_created < primary_first && primary_first < primary_created, "one path at a time, the primary last, the non-primary's new node Ready before the primary's delete begins: {other_deleted} {other_created} {primary_first} {primary_created}: {:?}", done.frames);
    // Every node of mesh2 holds the node the role moved to as the broker primary, and node.get answers
    // it the one primary broker.
    let mesh2 = [at("mesh2.node-admin", "1"), other.location(), primary.location()];
    let want = Some(vec![other2.node_id.clone()]);
    wait_for_observed("every node of mesh2 holds the non-primary's new node as the broker primary, the one node.get answers primary", Duration::from_secs(15), || async {
        let held = estate.node_type_primary_held_at(&mesh2, "mesh2", "broker").await;
        let answered: Vec<rdm_core::NodeId> = estate.node_views().await.into_iter().filter(|n| n.mesh() == "mesh2" && n.r#type == "broker" && n.is_primary).map(|n| n.node_id).collect();
        if held.iter().all(|(_, h)| *h == want) && answered == [other2.node_id.clone()] { Ok(()) } else { Err(format!("want {}; held {held:?}; node.get primary brokers {answered:?}", other2.node_id)) }
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    // The one broker primary a create commanded is the type's first, at the estate's own creation:
    // neither replacement's create commands one (Q433).
    let replaced = [other2.node_id.as_str(), primary2.node_id.as_str()];
    let creates: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-create").into_iter().filter(|sp| s(&sp["attributes"]["mesh"]) == "mesh2" && s(&sp["attributes"]["type"]) == "broker").collect();
    let commanded: Vec<&Value> = creates.iter().copied().filter(|sp| s(&sp["attributes"]["outcome"]) == "updated").collect();
    assert_eq!(commanded.len(), 1, "one create commanded the broker primary, the type's first: {commanded:#?}");
    assert!(views.iter().any(|v| s(&commanded[0]["attributes"]["subject_id"]) == v.node_id.as_str()), "it was one of the estate's first brokers: {}", commanded[0]);
    let recreated = creates.iter().find(|sp| s(&sp["attributes"]["subject_id"]) == primary2.node_id.as_str()).unwrap_or_else(|| panic!("the re-created primary path's create decided its first primary: {creates:#?}"));
    assert!(["answered", "held"].contains(&s(&recreated["attributes"]["outcome"]).as_str()), "the re-created primary path's create read the move, commanding nothing: {recreated}");
    assert!(creates.iter().filter(|sp| replaced.contains(&s(&sp["attributes"]["subject_id"]).as_str())).all(|sp| s(&sp["attributes"]["outcome"]) != "updated"), "no replacement's create commands a primary: {creates:#?}");
    let build_id = done.build_id.to_string();
    let moved: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-command").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == build_id && s(&sp["attributes"]["outcome"]) == "updated").collect();
    assert_eq!(moved.len(), 1, "the Build moves one primary role, the broker primary's: {moved:?}");
    assert_eq!(s(&moved[0]["attributes"]["primary_node_id"]), primary.node_id.as_str(), "the role moved off the primary: {}", moved[0]);
    assert!(s(&moved[0]["attributes"]["detail"]).contains(other2.node_id.as_str()), "the role moved to the non-primary's new node {}, already replaced: {}", other2.node_id, moved[0]);
    estate.record_trace_url(moved[0]["trace_id"].as_str().unwrap());
}

// @feature: fabric-build
/// CONTRACT (states.md "Resize and a new executable": "The mesh cannot report ready, and the fabric
/// cannot change, until the Build's last planned node is ready"; "the fabric's ready": "When every mesh
/// the accepted Build names is ready, the fabric-primary sets the fabric ready"): a Build replacing a
/// node's executable sets the fabric ready, and sends `on_fabric_ready`, only after the path's new node
/// is Ready. Must NOT happen: the held birth, still standing at acceptance, counted as the Build's
/// planned node, so the notice fires before the replacement runs (Rafka's Build tdvaw00axcxm: the notice
/// at +0.2 s, the last new node at +12 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_builds_fabric_ready_comes_after_its_new_node_is_ready() {
    let cell = "a_replacement_builds_fabric_ready_comes_after_its_new_node_is_ready";
    let mut estate = estate_with(cell, "mesh1", &[("broker", 1)]).await;
    let path = at("mesh1.broker", "1");
    let before = estate.node_view(&path).await;
    let stream = estate.submit_change(create(vec![entry_of(&before, another_executable(&estate, "broker"), persistent())])).await;
    let done = estate.follow(stream).await;
    let after = new_birth(&estate, &path, &before.node_id).await;
    assert_ne!(after.node_id, before.node_id, "a new node stands at the path");
    estate.stop().await;
    let spans = estate.spans();
    let build_id = done.build_id.to_string();
    let of_build = |name: &str| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == build_id).cloned().collect() };
    let ready = of_build("rdm.node_admin.fabric.update.via-every-mesh-ready");
    assert_eq!(ready.len(), 1, "one fabric ready for the Build: {ready:?}");
    // The new node's Ready: its declaration, as its mesh-primary took it.
    let new_node_ready: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-declaration").into_iter().filter(|sp| s(&sp["attributes"]["sender"]) == after.node_id.as_str() && s(&sp["attributes"]["state"]).eq_ignore_ascii_case("ready")).collect();
    assert_eq!(new_node_ready.len(), 1, "the new node's one Ready: {new_node_ready:?}");
    assert!(
        ready[0]["start_unix_nano"].as_u64() >= new_node_ready[0]["start_unix_nano"].as_u64(),
        "the fabric is set ready only after the new node is Ready: fabric ready at {}, the new node Ready at {}",
        ready[0]["start_unix_nano"],
        new_node_ready[0]["start_unix_nano"]
    );
    let notice = of_build("rdm.node_admin.fabric.update.via-on-fabric-ready");
    assert_eq!(notice.len(), 1, "one notice: {notice:?}");
    estate.record_trace_url(ready[0]["trace_id"].as_str().unwrap());
}

// @feature: fabric-build
/// CONTRACT (build-deployment.md §3; node-primary-update.md "No-successor cases": a type's only node
/// skips the role move): mesh1's one broker, its type's only node and so its primary, is named again with
/// another executable. It is deleted and created again with no node.primary.update, and its new node
/// holds the type's primary once Ready. Must NOT happen: the Build refused `no-secondary`, or a role move
/// commanded for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_types_only_node_is_replaced_with_no_role_move() {
    let cell = "a_types_only_node_is_replaced_with_no_role_move";
    let mut estate = estate_with(cell, "mesh1", &[("broker", 1)]).await;
    let path = at("mesh1.broker", "1");
    let before = estate.node_view(&path).await;
    assert!(before.is_primary, "the type's only node is its primary: {before:?}");
    let stream = estate.submit_change(create(vec![entry_of(&before, another_executable(&estate, "broker"), persistent())])).await;
    let done = estate.follow(stream).await;
    let after = new_birth(&estate, &path, &before.node_id).await;
    assert!(frame(&done.frames, &op("node.create", &after.node_id), Some("Complete")).is_some(), "the new node's create is the Build's: {:?}", done.frames);
    let primary = estate.until_node(&path, "the new node holds its type's primary", Duration::from_secs(30), |n| n.filter(|n| n.node_id == after.node_id && n.is_primary).cloned()).await;
    assert_eq!(primary.node_id, after.node_id);
    estate.stop().await;
    let spans = estate.spans();
    let build_id = done.build_id.to_string();
    let moved: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-command").into_iter().filter(|sp| s(&sp["attributes"]["build_id"]) == build_id).collect();
    assert!(moved.is_empty(), "no role move for a type's only node: {moved:?}");
}

// @feature: fabric-build
/// CONTRACT (build-deployment.md §3: the same executable at a held path stays `name-held`, and a
/// node-admin's executable is replaced by the primitives, each its own call; D10): a Create naming
/// mesh1's broker with the very executable its row records is refused `name-held`, and so is a Create
/// naming mesh1's node-admin with another executable. Nothing is accepted. Must NOT happen: either
/// accepted as a replacement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_executable_or_a_node_admins_path_stays_name_held() {
    let cell = "the_same_executable_or_a_node_admins_path_stays_name_held";
    let mut estate = estate_with(cell, "mesh1", &[("broker", 1)]).await;
    let broker = estate.node_view(&at("mesh1.broker", "1")).await;
    let admin = estate.node_view(&at("mesh1.node-admin", "1")).await;
    let same = broker.executable.clone().expect("the broker's row records its executable");
    let builds = estate.build_ids().await;
    for (what, entry) in [("the broker on its own executable", entry_of(&broker, same, persistent())), ("the node-admin on another executable", entry_of(&admin, another_executable(&estate, "node-admin"), persistent()))] {
        match estate.carrier().await.create(create(vec![entry])).await {
            Ok(stream) => panic!("{what} is accepted as Build {}: it stays name-held", stream.build_id()),
            Err(e) => assert!(format!("{e}").contains("name-held"), "{what} is refused name-held: {e}"),
        }
    }
    assert_eq!(estate.build_ids().await, builds, "nothing is accepted");
    assert_eq!(estate.node_view(&broker.location()).await.node_id, broker.node_id, "the broker stands");
    estate.stop().await;
}

// @feature: fabric-build
/// CONTRACT (build-deployment.md §3; executable-replacement.md §3.5: "A replacement whose create fails
/// after its delete ends the Build `incomplete` at that create. A resubmit of the same Build runs what is
/// not complete, with the pinned dir"): mesh1's persistent broker is named again with another
/// executable while its data dir holds a directory where the create writes the new node's key
/// (`node-key.new`). The held birth is deleted, the create over its data dir fails at `PrepareStorage`,
/// and the Build ends incomplete at that create. With the directory gone, a resubmit of the same Build
/// creates the node at the path, on the deleted birth's data dir, and the Build is Complete. Must NOT
/// happen: the delete run again, the resubmit's node on a new dir, or a second Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_whose_create_fails_after_its_delete_is_finished_by_a_resubmit_on_the_old_dir() {
    let cell = "a_replacement_whose_create_fails_after_its_delete_is_finished_by_a_resubmit_on_the_old_dir";
    let mut estate = estate_with(cell, "mesh1", &[("broker", 1)]).await;
    let path = at("mesh1.broker", "1");
    let before = estate.node_view(&path).await;
    let dir = before.storage.data_dir.clone().expect("the broker's data dir");
    // The create over the inherited dir writes `node-key.new` and renames it over `node-key` (Q358):
    // a directory there refuses the write, on that dir alone.
    let blocker = std::path::Path::new(&dir).join("node-key.new");
    std::fs::create_dir_all(blocker.join("held")).unwrap();
    let executable = another_executable(&estate, "broker");
    let stream = estate.submit_change(create(vec![entry_of(&before, executable.clone(), persistent())])).await;
    let first = estate.follow_to_terminal(stream).await;
    let sections = match first.frames.last() {
        Some(BuildFrame::Incomplete { sections }) => sections.clone(),
        other => panic!("the Build ends incomplete at its create: {other:?} of {:?}", first.frames),
    };
    assert!(frame(&first.frames, &op("node.delete", &before.node_id), Some("Complete")).is_some(), "the held birth was deleted first: {:?}", first.frames);
    assert!(sections.iter().any(|u| u.operation.starts_with("node.create:") && u.reason.contains("node-key.new")), "the section ends at the replacement's create over the old dir: {sections:?}");
    std::fs::remove_dir_all(&blocker).unwrap();
    let stream = estate.resubmit(&first.build_id).await;
    let done = estate.follow(stream).await;
    assert_eq!(done.build_id, first.build_id, "the resubmit is the same Build");
    let after = new_birth(&estate, &path, &before.node_id).await;
    assert_eq!(after.storage.data_dir.as_deref(), Some(dir.as_str()), "the resubmit's node runs on the deleted birth's data dir: {after:?}");
    assert_eq!(after.executable.as_ref(), Some(&executable), "on the Create's executable: {after:?}");
    estate.stop().await;
    let spans = estate.spans();
    let deletes = named(&spans, "rdm.node_admin.node.delete.via-build").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == before.node_id.as_str()).count();
    assert_eq!(deletes, 1, "the held birth's delete ran once: the resubmit runs no step of it again");
    let replaced: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-replacement").into_iter().filter(|sp| s(&sp["attributes"]["node_id"]) == after.node_id.as_str()).collect();
    assert_eq!(replaced.len(), 2, "the failed create and the resubmit's each name the birth they replace: {replaced:?}");
    assert!(replaced.iter().all(|sp| s(&sp["attributes"]["predecessor_data_dir"]) == dir && sp["attributes"]["inherited"] == "true"), "every attempt takes the deleted birth's dir: {replaced:?}");
}
