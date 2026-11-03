//! i143.e12.s6 process E2E: the fabric states (states.md; lifecycles/states.md, "The fabric's ready").
//!
//! A real estate whose node-admin is the testkit's, carrying the testkit application. The fabric-primary
//! alone moves the fabric: pending, then ready once every mesh is ready (Q174),
//! and the application is told `on_mesh_ready` and `on_fabric_ready` (Q175). A cell holds a fabric short
//! of ready by holding a planned node's `pre_node_ready` (`rdm_test::write_pre_node_ready_knob`).
//! Evidence is read from the spans every process wrote.

use rdm_client::{BuildFrame, BuildStream, FabricDesired, FabricView, MeshDesired, ScopeStatus, PathName};
use rdm_test::typed::BuildSpec;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, binding_set_with_roles, candidate_sha};
use serde_json::{json, Value};
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-states".into(),
        subfeature: "check-ins".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn flag(v: &Value) -> bool {
    v == true || v == "true"
}

/// A numeric span attribute, exported as a number or as its decimal string.
fn num(v: &Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_str()?.parse().ok())
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

async fn estate_with(test: &str) -> Estate {
    let sha = candidate_sha();
    Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"])
        .await
        .expect("the faulted-admin binding set is accepted")
}

/// [`estate_with`] whose launch ids also bind the product role binaries (broker, gateway, compute).
async fn estate_with_roles(test: &str) -> Estate {
    let sha = candidate_sha();
    Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set_with_roles(&sha), &sha, &["broker", "gateway", "compute"])
        .await
        .expect("the faulted-admin binding set is accepted")
}

/// Await the fabric under `build` in `state`, as `fabric.get` at the admin the estate asks reports it:
/// the Build the fabric accepted is the one whose change reached the state.
async fn until_fabric(estate: &Estate, state: ScopeStatus, build: Option<&str>, within: Duration) -> FabricView {
    let what = format!("the fabric is {state:?}{}", build.map(|b| format!(" under {b}")).unwrap_or_default());
    estate.fabrics_obj().await.fact(&what, within, |f| f.status == state && build.is_none_or(|b| f.build_id.as_deref() == Some(b))).await.unwrap_or_else(|end| panic!("{end}"))
}

/// The spans of the change whose root carries `build_id`.
fn change_of<'a>(spans: &'a [Value], build_id: &str) -> (&'a Value, Vec<&'a Value>) {
    let root = named(spans, "rdm.node_admin.fabric.update.via-change")
        .into_iter()
        .find(|r| r["attributes"]["build_id"] == build_id)
        .unwrap_or_else(|| panic!("no change of {build_id}"));
    let mut mine: Vec<&Value> = spans.iter().filter(|sp| sp["trace_id"] == root["trace_id"] && start(sp) >= start(root)).collect();
    mine.sort_by_key(|sp| start(sp));
    (root, mine)
}

/// CONTRACT (states.md "The fabric's ready"; Q174, Q175): a fabric whose Build adds a second mesh goes back
/// to pending, and is ready once every mesh is ready, set by the fabric-primary
/// alone; no node is commanded to open traffic and none checks in. Each mesh-primary tells the
/// application `on_mesh_ready` once its mesh is ready, and the fabric-primary's one
/// `on_fabric_ready` follows ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_mesh_fabric_is_ready_once_every_mesh_is_ready() {
    let test = "a_two_mesh_fabric_is_ready_once_every_mesh_is_ready";
    let estate = estate_with(test).await;
    let members = [("node", "1"), ("node", "2"), ("node", "3")];
    two_mesh_change(estate, &[("node-admin".to_string(), 2), ("node".to_string(), 3)], &members).await;
}

/// CONTRACT: a two-mesh fabric whose meshes each hold the product role nodes (a broker, a gateway
/// and a compute) changes to ready exactly as the node fabric does: once every mesh is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_mesh_fabric_of_role_nodes_is_ready_once_every_mesh_is_ready() {
    let test = "a_two_mesh_fabric_of_role_nodes_is_ready_once_every_mesh_is_ready";
    let estate = estate_with_roles(test).await;
    let members = [("broker", "1"), ("gateway", "1"), ("compute", "1")];
    two_mesh_change(estate, &[("node-admin".to_string(), 2), ("broker".to_string(), 1), ("gateway".to_string(), 1), ("compute".to_string(), 1)], &members).await;
}

/// CONTRACT: the mesh-primary of a role-node mesh dies and the election names its mesh's other admin
/// mesh-primary. Each role node of that mesh (broker, gateway, compute) sees the mesh-primary move to
/// another birth, owes its state again (the `via-mesh-primary-moved` span, naming the new mesh-primary) and
/// declares it to the new mesh-primary (`via-declare-own`, definitive), the same re-owe the node
/// makes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn role_nodes_declare_their_state_again_to_the_mesh_primary_the_election_names() {
    let test = "role_nodes_declare_their_state_again_to_the_mesh_primary_the_election_names";
    let mut estate = estate_with_roles(test).await;
    let mesh = |m: &str| MeshDesired::of(m, [("node-admin", 2), ("broker", 1), ("gateway", 1), ("compute", 1)]);
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await;
    let want: std::collections::BTreeSet<PathName> = ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| PathName::new(&format!("{m}.node-admin"), &i.to_string()).unwrap()).chain(["broker", "gateway", "compute"].iter().map(move |r#type| PathName::new(&format!("{m}.{}", r#type), "1").unwrap()))).collect();
    let nodes = estate.settles_to_named(&want, Duration::from_secs(60)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let fp = nodes.iter().find(|n| n.is_fabric_primary).expect("one fabric primary");
    let old = nodes.iter().find(|n| n.mesh != fp.mesh && n.is_primary && n.r#type == "node-admin").expect("another mesh's primary admin");
    let heir = nodes.iter().find(|n| n.mesh == old.mesh && n.r#type == "node-admin" && n.node_id != old.node_id).expect("another admin in the lost mesh");
    let heir_id = heir.node_id.as_str();
    estate.kill_node(&old.location()).await;
    let roles: Vec<_> = nodes.iter().filter(|n| n.mesh == old.mesh && matches!(n.r#type.as_str(), "broker" | "gateway" | "compute")).map(|n| n.node_id.to_string()).collect();
    // A role node's re-declaration to the new mesh-primary is a call it makes: no object reports it, and the
    // span the node records is the only place the fact lives. Both source and destination are IDs.
    wait_for("every role node of the mesh declared its state to the new primary", Duration::from_secs(90), || {
        let spans = estate.spans();
        let done = roles.iter().all(|r| {
            named(&spans, "rdm.node_rpc.status.update.via-declare-own").iter().any(|d| s(&d["attributes"]["node"]) == *r && s(&d["attributes"]["to"]) == heir_id && flag(&d["attributes"]["definitive"]))
        });
        async move { done.then_some(()) }
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    for r in &roles {
        let moved: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-mesh-primary-moved").into_iter().filter(|m| s(&m["attributes"]["node"]) == *r).collect();
        assert!(moved.iter().any(|m| s(&m["attributes"]["to"]) == heir_id), "{r} saw the mesh-primary move to {heir_id}: {:?}", moved.iter().map(|m| &m["attributes"]).collect::<Vec<_>>());
    }
    estate.record_trace_url(&s(&named(&spans, "rdm.node_rpc.status.update.via-mesh-primary-moved").first().expect("a re-owe")["trace_id"]));
}

/// The two-mesh change over meshes of `shape` (their node-admins and members), whose members are named
/// `members` under each mesh.
async fn two_mesh_change(mut estate: Estate, shape: &[(String, u32)], members: &[(&str, &str)]) {
    let mesh = |m: &str| MeshDesired::of(m, shape.iter().cloned());
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1")] }).await;
    let two_meshes = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await.build_id.to_string();
    let want: std::collections::BTreeSet<PathName> = ["mesh1", "mesh2"]
        .iter()
        .flat_map(|m| (1..=2).map(move |i| PathName::new(&format!("{m}.node-admin"), &i.to_string()).unwrap()).chain(members.iter().map(move |(kind, name)| PathName::new(&format!("{m}.{kind}"), *name).unwrap())))
        .collect();
    estate.settles_to_named(&want, Duration::from_secs(60)).await;
    until_fabric(&estate, ScopeStatus::Ready, Some(two_meshes.as_str()), Duration::from_secs(60)).await;
    let nodes = estate.node_values().await;
    estate.artifact("nodes.json", &json!(nodes));
    let primaries: std::collections::BTreeSet<String> = nodes.iter().filter(|n| n["type"] == "node-admin" && n["is_primary"] == true).map(|n| s(&n["node_id"])).collect();
    let fabric_id = estate.fabric_view().await.id.to_string();
    estate.stop().await;
    let spans = estate.spans();
    assert_eq!(primaries.len(), 2, "one primary per mesh: {primaries:?}");
    let (root, change) = change_of(&spans, &two_meshes);
    let at = |name: &str, f: &dyn Fn(&Value) -> bool| -> &Value { change.iter().copied().find(|sp| sp["name"] == name && f(sp)).unwrap_or_else(|| panic!("no {name} in the change")) };
    let entered = |to: &str| at("rdm.node_admin.fabric.update.via-state-entered", &|sp| sp["attributes"]["to"] == to);
    let every_mesh_ready = at("rdm.node_admin.fabric.update.via-every-mesh-ready", &|_| true);
    let notice = at("rdm.node_admin.fabric.update.via-on-fabric-ready", &|_| true);
    assert!(flag(&notice["attributes"]["bound"]), "the testkit application registers on_fabric_ready: {notice}");
    let order = [("every mesh ready", start(every_mesh_ready)), ("ready", start(entered("ready"))), ("on_fabric_ready notice", start(notice))];
    for w in order.windows(2) {
        assert!(w[0].1 <= w[1].1, "{} came after {}: {order:?}", w[0].0, w[1].0);
    }
    // No node is commanded to open traffic and none checks in (Q174).
    for gone in ["rdm.node_admin.fabric.update.via-state-command", "rdm.node_admin.fabric.update.via-member-check-in", "rdm.node_admin.fabric.update.via-mesh-checklist", "rdm.node_admin.fabric.update.via-state-hook", "rdm.mesh.topology.update.via-state-frame"] {
        assert!(named(&spans, gone).is_empty(), "{gone} is in the evidence: the open-traffic checklist ran");
    }
    // Each mesh's primary told the application once that its mesh is ready (Q175).
    let mesh_ready = named(&spans, "rdm.node_admin.mesh.update.via-on-mesh-ready");
    for m in ["mesh1", "mesh2"] {
        let told: Vec<&&Value> = mesh_ready.iter().filter(|sp| sp["attributes"]["mesh"] == m).collect();
        assert_eq!(told.len(), 1, "{m}'s primary told on_mesh_ready once: {told:?}");
        assert!(flag(&told[0]["attributes"]["bound"]), "the testkit application registers on_mesh_ready: {}", told[0]);
    }
    let _ = &want;

    // The fabric states went out as FabricStatus authored by the fabric-primary only.
    let sends: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| sp["attributes"]["scope"].as_str().is_some_and(|x| x.contains("fabric")) && start(sp) >= start(root)).collect();
    assert!(!sends.is_empty(), "FabricStatus sends are in the evidence");
    let publisher_roles = named(&spans, "rdm.mesh.fabric.update.via-status-publisher");
    for send in &sends {
        let role = publisher_roles.iter().filter(|event| event["attributes"]["node"] == send["attributes"]["node"] && event["attributes"]["fabric"] == fabric_id && start(event) <= start(send)).max_by_key(|event| start(event)).expect("the sender recorded its publisher role");
        assert_eq!(role["attributes"]["role"], "start", "the sender held the fabric status publisher role when it sent: {send}, role: {role}");
    }
    // The resize put the fabric back to pending at its acceptance, before the change's root opened: the
    // states are read over every FabricStatus send of the run.
    let mut every: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| sp["attributes"]["scope"].as_str().is_some_and(|x| x.contains("fabric"))).collect();
    every.sort_by_key(|sp| start(sp));
    let mut seen: Vec<String> = Vec::new();
    for sp in &every {
        let st = s(&sp["attributes"]["status"]);
        if seen.last() != Some(&st) {
            seen.push(st);
        }
    }
    let change_states: Vec<&str> = vec!["pending", "ready"];
    assert!(seen.windows(2).any(|w| w == change_states), "FabricStatus went pending, ready: {seen:?}");
    estate.record_trace_url(&s(&root["trace_id"]));
}

/// Every row under the fabric's store root, sorted: each record of a store file as
/// `<file>#<kind>/<key>` (its latest line), and every other file as a path relative to the root. The
/// directories the stores open, an empty store file and the layout marker are not rows.
fn rows_on(root: &std::path::Path) -> Vec<String> {
    fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let p = e.path();
            let rel = p.strip_prefix(root).unwrap().display().to_string();
            if p.is_dir() {
                walk(&p, root, out);
            } else if rel.ends_with(".jsonl") && dir == root {
                let latest = rdm_admin::store_log::latest_in(&p).unwrap_or_else(|e| panic!("{} reads whole: {e}", p.display()));
                out.extend(latest.into_keys().map(|(kind, key)| format!("{rel}#{kind}/{key}")));
            } else if rel != rdm_admin::store_root::LAYOUT && !rel.ends_with(".reclaim") {
                out.push(rel);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// Wait until the fabric is ready under the change of `build`.
async fn ready_under(estate: &Estate, build: &str) {
    until_fabric(estate, ScopeStatus::Ready, Some(build), Duration::from_secs(90)).await;
}

/// Read `stream`'s frames until the drive reports its Build Blocked naming `node`, bounded by `within`. A
/// `Complete` before it is the defect: the Build said Complete while a planned node was not ready.
async fn until_blocked_on(stream: &mut BuildStream, node: &rdm_core::NodeId, within: Duration) -> BuildFrame {
    let build = stream.build_id().to_string();
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let frame = match tokio::time::timeout_at(deadline, stream.next()).await {
            Err(_) => panic!("Build {build} reported no Blocked naming {node:?} within {within:?}"),
            Ok(None) => panic!("the stream of Build {build} ended before it reported Blocked naming {node:?}"),
            Ok(Some(Err(e))) => panic!("the stream of Build {build} broke before it reported Blocked naming {node:?}: {e}"),
            Ok(Some(Ok(frame))) => frame,
        };
        match &frame {
            BuildFrame::Blocked { reason, .. } if reason.contains(node.as_str()) => return frame,
            BuildFrame::Complete { .. } | BuildFrame::Failed { .. } | BuildFrame::FabricPrimaryMoved { .. } => panic!("Build {build} ended {frame:?} before it reported Blocked naming {node:?}"),
            _ => {}
        }
    }
}

/// `stream` followed to its `Complete` (`Estate::follow`), bounded by `within`: `what` names the wait.
async fn followed(estate: &Estate, stream: BuildStream, what: &str, within: Duration) -> rdm_test::typed::Reconciled {
    tokio::time::timeout(within, estate.follow(stream)).await.unwrap_or_else(|_| panic!("{what}: no Complete within {within:?}"))
}

/// CONTRACT (Q154; states.md "Resize and executable replacement": "Its Build is Complete only when the fabric is
/// ready again from this Build's own change, as fabric.create's is"; build.md §10: "A Build
/// that put the fabric back to pending completes when its ready state is set, not with its
/// last step"; fabric-node-hooks.md: "A node never reaches Ready while its hook has not returned
/// `Ok`"): a resize whose mesh2.node.1 is held at its `pre_node_ready` reports Blocked waiting for that
/// node, and while `fabric.get` says pending under it neither its stream nor `build.get` says Complete;
/// released, it is Complete only after the fabric entered ready under it. Must NOT happen: a
/// Build Complete while the fabric it put back to pending has not changed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resize_is_complete_only_after_its_own_ready_state_is_set() {
    let test = "a_resize_is_complete_only_after_its_own_ready_state_is_set";
    let mut estate = estate_with(test).await;
    // Day 0's Build is Complete; the resize's mesh2.node.1 is held at its pre_node_ready from its first attempt.
    rdm_test::write_pre_node_ready_knob(&estate.root, &rdm_core::PathName::new("mesh2.node", "1").unwrap(), "held by the cell").unwrap();
    let shape = |m: &str| MeshDesired::of(m, [("node-admin", 1), ("node", 1)]);
    let mut stream = estate.submit(&BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![shape("mesh1"), shape("mesh2")] })).await;
    let build = stream.build_id().to_string();
    let held = estate.until_node(&PathName::new("mesh2.node", "1").unwrap(), "the held node has been allocated", Duration::from_secs(90), |node| node.map(|n| n.node_id.clone())).await;
    let blocked = until_blocked_on(&mut stream, &held, Duration::from_secs(90)).await;
    estate.artifact("blocked-on-the-held-node.json", &json!(format!("{blocked:?}")));
    // The node is held: through the window fabric.get says pending under the Build, and neither the
    // Build's stream nor build.get says Complete.
    let id = rdm_client::BuildId::parse(&build).unwrap();
    let carrier = estate.carrier().await;
    let held_until = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < held_until {
        let fabric = estate.fabric_view().await;
        assert_eq!((fabric.status, fabric.build_id.as_deref()), (ScopeStatus::Pending, Some(build.as_str())), "the fabric is held pending under {build}");
        let got = carrier.build(&id).await.unwrap_or_else(|e| panic!("build.get [id: {build}]: {e}"));
        assert_ne!(got.phase, rdm_core::build::BuildPhase::Complete, "build.get says {build} Complete at attempt {} while fabric.get says pending under it", got.attempt);
        if let Ok(Some(frame)) = tokio::time::timeout(Duration::from_millis(250), stream.next()).await {
            let frame = frame.unwrap_or_else(|e| panic!("the stream of Build {build} broke while the fabric was held pending: {e}"));
            assert!(!matches!(frame, BuildFrame::Complete { .. } | BuildFrame::Failed { .. }), "Build {build} ended {frame:?} while fabric.get says pending under it");
        }
    }
    rdm_test::write_pre_node_ready_knob(&estate.root, &rdm_core::PathName::new("mesh2.node", "1").unwrap(), "pass").unwrap();
    let done = followed(&estate, stream, &format!("Build {build} after mesh2.node.1's pre_node_ready was released"), Duration::from_secs(90)).await;
    assert_eq!(carrier.build(&id).await.unwrap_or_else(|e| panic!("build.get [id: {build}]: {e}")).phase, rdm_core::build::BuildPhase::Complete, "build.get says {build} Complete once its stream did");
    ready_under(&estate, &build).await;
    estate.stop().await;

    // The order, from the spans: the fabric entered ready under this Build's change before the
    // attempt that completed it was dispatched.
    let spans = estate.spans();
    let (_, change) = change_of(&spans, &build);
    let ready = change.iter().find(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-state-entered" && sp["attributes"]["to"] == "ready").unwrap_or_else(|| panic!("the fabric entered ready under the change of {build}"));
    let completed = named(&spans, "rdm.node_admin.build.update.via-dispatch").into_iter().find(|sp| sp["attributes"]["build_id"] == build.as_str() && sp["attributes"]["outcome"] == "complete").unwrap_or_else(|| panic!("the attempt that completed {build} is spanned"));
    let Some(BuildFrame::Complete { attempt }) = done.frames.last() else { panic!("Build {build} ended {:?}", done.frames.last()) };
    assert_eq!(num(&completed["attributes"]["attempt"]), Some(u64::from(*attempt)), "the dispatch span is of the attempt the stream reported Complete: {completed}");
    assert!(start(ready) <= start(completed), "the fabric entered ready under {build} ({}) before the attempt that completed it was dispatched ({}): {completed}", start(ready), start(completed));
    estate.record_trace_url(&s(&ready["trace_id"]));
}

/// CONTRACT (fabric-node-hooks.md `on_build_started`, `on_build_ended`, Q256: "Each time a Build's first
/// attempt is claimed ... the fabric-primary sends the application one `on_build_started` notice"; "Each
/// time a Build's terminal state is recorded, the fabric-primary sends the application one
/// `on_build_ended` notice"; "A Build that put the fabric back to pending completes when its ready state is
/// set, so its `Complete` notice follows its `on_fabric_ready`"; "Each `on_build_started` is followed by
/// one `on_build_ended` for the same opening"): a resize (mesh1.node.1 added, the fabric back to pending)
/// and a Create of one more node (Q342: a Build adds or removes nodes) each send one
/// `on_build_started` (attempt 1, reason `requested`) and after it one `on_build_ended` `Complete`
/// naming the attempt that completed it, and the testkit application is told each once; the resize's
/// ending follows its `on_fabric_ready`. Must NOT happen: an opening or ending the application is never
/// told of or told twice, or an ending before its opening.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_build_sends_one_started_then_one_ended_and_a_resizes_ending_follows_its_on_fabric_ready() {
    let test = "every_build_sends_one_started_then_one_ended_and_a_resizes_ending_follows_its_on_fabric_ready";
    let mut estate = estate_with(test).await;
    let resize = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1), ("node", 1)])] }).await;
    let resize_id = resize.build_id.to_string();
    ready_under(&estate, &resize_id).await;
    // The second Build: a Create of one node (a Build states only the nodes it adds or removes, Q342).
    let one = estate.create_node_request("mesh1", "node").await;
    let stream = estate.submit_change(rdm_client::Change::Create { meshes: Vec::new(), nodes: vec![one] }).await;
    let created = estate.follow(stream).await;
    let created_id = created.build_id.to_string();
    estate.stop().await;

    let spans = estate.spans();
    let completed_at = |r: &rdm_test::typed::Reconciled| match r.frames.last() {
        Some(BuildFrame::Complete { attempt }) => u64::from(*attempt),
        other => panic!("Build {} ended {other:?}", r.build_id),
    };
    let of = |name: &str, build: &str| -> Vec<Value> { named(&spans, name).into_iter().filter(|sp| sp["attributes"]["build_id"] == build).cloned().collect() };
    for (what, build, done) in [("the resize", &resize_id, &resize), ("the Create of one node", &created_id, &created)] {
        let started = of("rdm.node_admin.build.update.via-on-build-started", build);
        let ended = of("rdm.node_admin.build.update.via-on-build-ended", build);
        assert_eq!((started.len(), ended.len()), (1, 1), "{what} {build} sends one on_build_started and one on_build_ended: {started:?} {ended:?}");
        let (st, en) = (&started[0], &ended[0]);
        assert_eq!((s(&st["attributes"]["outcome"]), num(&st["attributes"]["attempt"]), s(&st["attributes"]["reason"])), ("sent".to_string(), Some(1), "requested".to_string()), "{what}'s opening is its first attempt's claim: {st}");
        assert_eq!((s(&en["attributes"]["outcome"]), num(&en["attributes"]["attempt"]), s(&en["attributes"]["state"])), ("sent".to_string(), Some(completed_at(done)), "complete".to_string()), "{what}'s ending names the attempt that completed it: {en}");
        assert!(start(st) <= start(en), "{what}'s opening ({}) comes before its ending ({})", start(st), start(en));
        for app in ["rdm.testkit.app.update.via-on-build-started", "rdm.testkit.app.update.via-on-build-ended"] {
            assert_eq!(of(app, build).len(), 1, "the application is told {app} once for {what} {build}");
        }
        eprintln!("span rdm.node_admin.build.update.via-on-build-started {what} trace_id={} attributes={}", s(&st["trace_id"]), st["attributes"]);
        eprintln!("span rdm.node_admin.build.update.via-on-build-ended {what} trace_id={} attributes={}", s(&en["trace_id"]), en["attributes"]);
    }
    // Each Build adds a node, so each put the fabric back to pending.
    let resets = named(&spans, "rdm.node_admin.fabric.update.via-state-reset");
    for build in [&resize_id, &created_id] {
        assert!(resets.iter().any(|sp| sp["attributes"]["build_id"] == build.as_str()), "the Build {build} put the fabric back to pending");
    }
    // The resize's ending follows its on_fabric_ready.
    let (_, change) = change_of(&spans, &resize_id);
    let ready_notice = change.iter().find(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-on-fabric-ready").unwrap_or_else(|| panic!("the change of {resize_id} sent on_fabric_ready"));
    let resize_ended = &of("rdm.node_admin.build.update.via-on-build-ended", &resize_id)[0];
    assert!(start(ready_notice) <= start(resize_ended), "the resize's on_build_ended ({}) follows its on_fabric_ready ({})", start(resize_ended), start(ready_notice));
    estate.record_trace_url(&s(&resize_ended["trace_id"]));
}

/// CONTRACT (fabric-create.md, Entry: "Day 0 is the first node-admin and its own mesh, nothing else. Every
/// further mesh, and every other node, is created by a Build accepted after the fabric's first
/// ready (Q170)"; Q171: every writer writes its rows to the store root from Day 0;
/// node-identity-and-state.md §2.7, The write: "it writes each create and update at once"; R14 item 9).
///
/// Day 0: its one node, the fabric-primary mesh1.node-admin.1 (mesh1's primary too), up and ready, the fabric's
/// store root holds every row Day 0 wrote: the Fabric row, mesh1's Mesh row, mesh1.node-admin.1's Node record (its
/// declared states its own Ready alone: the Day-0 root joins nobody, Q97) and the Day-0 Build's
/// committed facts (Q61); no connections row, since it originated no data-plane connection (connections.md
/// "The data plane is the connections log", R-CR6).
///
/// After the first ready a growth Build adds mesh2 (its node-admin and a node) and an rpc
/// node to mesh1, and each of its writers writes straight to the store: mesh2's primary its Mesh record,
/// mesh2.node.1 the connection it originates to mesh1.node.1. `fabric/` has one writer, the
/// fabric-primary (R14 item 2); the Fabric row carries no pointer to the newest Build (R-BI3): the growth
/// Build is written once to the builds store after the Day-0 Build and mesh2's primary reads it as the
/// current Build. The growth is a resize, which puts the fabric back to pending (states.md, Resize and
/// executable replacement) before its build.create door answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_store_root_holds_every_day_0_row_and_a_growth_writes_straight_to_it() {
    let test = "the_store_root_holds_every_day_0_row_and_a_growth_writes_straight_to_it";
    let mut estate = estate_with(test).await;
    let root = estate.fabric_store_root();
    // The first Build is the Day-0 Build (fabric-create.md, Q170), Complete once its own ready
    // state is set (Q154): the estate followed it there.
    let first = estate.accepted_build_id().await;
    let want: std::collections::BTreeSet<PathName> = [PathName::new("mesh1.node-admin", "1").unwrap()].into_iter().collect();
    let nodes = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(60)).await);
    ready_under(&estate, &first).await;
    let before = rows_on(&root);
    estate.artifact("rows-before-ready.json", &json!(before));
    let holds = |rows: &[String], prefix: &str| rows.iter().any(|r| r.starts_with(prefix));
    for n in &nodes {
        let name = s(&n["node_id"]);
        let location: PathName = rdm_test::estate::location(&n);
        let want: &[&str] = &["Ready"];
        // A mesh-primary's own Ready has no Applied reply to be durable before (states.md): its
        // writer records it on its own record after the view shows it, so the cell reads until it is there.
        let read = || {
            let record: Value = estate.node_record(&s(&n["node_id"])).unwrap_or_else(|| panic!("the record of {name} is in nodes.jsonl"));
            let birth = record["record"]["births"].as_array().into_iter().flatten().find(|b| b["incarnation_id"] == n["incarnation_id"]).cloned().unwrap_or_default();
            (birth["declared"].clone(), record)
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let (declared, record) = loop {
            let (declared, record) = read();
            if declared == json!(want) || std::time::Instant::now() > deadline {
                break (declared, record);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(declared, json!(want), "{name}'s record holds the state its ready recorded within 30 s: {record}");
    }
    assert!(holds(&before, "fabric.jsonl#fabric-record/"), "the Fabric row stands on the root: {before:#?}");
    assert!(!before.iter().any(|r| r.starts_with("locks/name/")), "no name is a lock on the root: {before:#?}");
    assert_eq!(before.iter().filter(|r| r.starts_with("meshes.jsonl#mesh-record/")).count(), 1, "mesh1's record stands on the root: {before:#?}");
    assert!(holds(&before, "builds.jsonl#build-record/"), "Q61: the Day-0 Build's committed facts stand on the root: {before:#?}");
    assert!(!holds(&before, "connections.jsonl#"), "the Day-0 root originated no data-plane connection, so no connections row stands on the root: {before:#?}");
    // build.md §5.3: nothing beside the per-fabric roots holds the fabric's name.
    assert!(!root.parent().expect("the store root sits under the store_dir").join("fabrics").exists(), "no fabric name index stands beside the roots");
    let after = rows_on(&root);
    estate.artifact("rows-after-ready.json", &json!(after));
    // Under fabric/ the fabric-primary holds nothing: the Fabric record is fabric.jsonl's, the Build journal
    // builds.jsonl's (R-BI6), and no per-role row stands beside them (Part C2).
    let only_identity_builds_and_events = |rows: &[String]| !rows.iter().any(|r| r.starts_with("fabric/")) && rows.iter().filter(|r| r.starts_with("fabric.jsonl#")).all(|r| r.starts_with("fabric.jsonl#fabric-record/"));
    assert!(only_identity_builds_and_events(&after), "the Fabric row carries no pointer to the newest Build and no primary row stands beside it: {after:#?}");
    // Each node records its own address in its own part of its record, at the birth its mesh's primary
    // records on the record's other part (Q333).
    let addressed = |mesh: &str, id: &str| {
        let (mesh, id) = (mesh.to_string(), id.to_string());
        let estate = &estate;
        async move {
            let addressed = || {
                let record = estate.node_record(&id)?;
                let birth = record["record"]["births"].as_array().and_then(|b| b.last()).and_then(|b| b["incarnation_id"].as_str()).map(str::to_string)?;
                let own = estate.node_properties(&rdm_core::NodeId::parse(&id).ok()?)?;
                (own.birth.incarnation_id.as_str() == birth && own.transport_addr.is_some()).then_some(record)
            };
            let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while addressed().is_none() && std::time::Instant::now() < until {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(addressed().is_some(), "{mesh}: {id} recorded its address in its own part of its record at its birth: {:?} {:?}", estate.node_record(&id), rdm_core::NodeId::parse(&id).ok().and_then(|n| estate.node_properties(&n)));
        }
    };
    let fp_id = s(&nodes[0]["node_id"]);
    addressed("mesh1", &fp_id).await;

    // The growth Build, accepted after the first ready (Q170), adds mesh2 and a node to
    // mesh1: every row its nodes write goes straight to the store.
    let shape = |m: &str, rpc: u32| MeshDesired::of(m, [("node-admin", 1), ("node", rpc)]);
    let second = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![shape("mesh1", 1), shape("mesh2", 1)] }).await.build_id.to_string();
    let mut want_later = want.clone();
    want_later.extend([("mesh1.node", "1"), ("mesh2.node-admin", "1"), ("mesh2.node", "1")].into_iter().map(|(path, name)| PathName::new(path, name).unwrap()));
    let later_nodes = estate.settles_to_named(&want_later, Duration::from_secs(60)).await;
    let source = later_nodes.iter().find(|n| n.mesh == "mesh2" && n.r#type == "node").expect("source RPC");
    let destination = later_nodes.iter().find(|n| n.mesh == "mesh1" && n.r#type == "node").expect("destination RPC");
    let source_target = format!("exact:{}", source.node_id);
    let destination_target = format!("exact:{}", destination.node_id);
    ready_under(&estate, &second).await;
    // mesh2.node.1 learns of mesh1.node.1 from the MeshTopologyDelta its mesh-primary forwards (gossip.md §3.3),
    // not from the admin's view the cell settled on: the origination waits until mesh2.node.1's own
    // resolver holds the destination.
    wait_for("mesh2.node.1's own resolver holds mesh1.node.1", Duration::from_secs(30), || async {
        let r = estate.probe(&["resolve", "--target", &source_target, "--query", &destination_target]);
        (r["reply"]["resolution"] == "found").then_some(())
    })
    .await;
    let straight = estate.probe(&["originate", "--target", &source_target, "--destination", &destination_target, "--key", "after-commit"]);
    estate.artifact("originate-after-growth.json", &straight);
    assert_eq!(straight["call_outcome"].as_str(), Some("reply"), "mesh2.node.1 originated its connection to mesh1.node.1: {straight}");
    for node in &later_nodes {
        addressed(&node.mesh, node.node_id.as_str()).await;
    }
    // The originated connection's row is owed on mesh2.node.1's writer queue and lands after the open
    // answered (Q169): the store is read once it stands, within the bound.
    let connection_written = || estate.connection_history(&source.node_id).iter().any(|fact| fact["source"]["node_id"] == source.node_id.as_str() && fact["destination"]["node_id"] == destination.node_id.as_str());
    let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !connection_written() && std::time::Instant::now() < until {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let later = rows_on(&root);
    estate.artifact("rows-after-growth.json", &json!(later));
    assert!(!later.iter().any(|r| r.starts_with("locks/name/")), "no mesh's name is a lock on the root: {later:#?}");
    assert_eq!(later.iter().filter(|r| r.starts_with("meshes.jsonl#mesh-record/")).count(), 2, "each mesh's record stands on the root: {later:#?}");
    assert!(connection_written(), "mesh2.node.1 wrote the connection it originated straight to the store: {later:#?}");
    for node in later_nodes.iter().filter(|n| n.node_id != source.node_id) {
        assert!(estate.connection_history(&node.node_id).is_empty(), "{node:?} originated no connection, so it wrote no connections row; the control plane writes none: {later:#?}");
    }
    // The fabric-primary accepted the growth Build after the first ready: the Build is its own
    // entity in the builds store, so no pointer row under `fabric/` names it, and mesh2's primary reads it
    // as the current Build.
    assert!(only_identity_builds_and_events(&later), "accepting the growth Build writes no pointer row under `fabric/` for it: {later:#?}");
    let other_fabric = estate.fabric_view_at(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).await.expect("mesh2.node-admin.1 answers fabric.get for this Fabric");
    estate.artifact("mesh2-admin-fabric-after-growth.json", &serde_json::to_value(&other_fabric).expect("a view serializes"));
    assert_eq!(other_fabric.build_id.as_deref(), Some(second.as_str()), "mesh2's primary reads the growth Build as the current Build: {other_fabric:?}");
    estate.stop().await;
    let spans = estate.spans();

    // The growth put the fabric back to pending inside the acceptance: the fabric-primary's reset for it
    // starts before its build.create door answers it (the door's span ends at the answer).
    let door_span = named(&spans, "rdm.node_admin.build.create.via-node-rpc").into_iter().find(|sp| sp["attributes"]["build_id"] == second.as_str()).unwrap_or_else(|| panic!("the growth Build's build.create door is spanned"));
    let resets = named(&spans, "rdm.node_admin.fabric.update.via-state-reset");
    let reset = resets.iter().find(|sp| sp["attributes"]["build_id"] == second.as_str()).unwrap_or_else(|| panic!("the fabric-primary put the fabric back to pending for the growth Build {second}: resets {:?}", resets.iter().map(|sp| &sp["attributes"]).collect::<Vec<_>>()));
    assert_eq!((s(&reset["attributes"]["node_path"]), s(&reset["attributes"]["node_name"]), s(&reset["attributes"]["from"])), ("mesh1.node-admin".to_string(), "1".to_string(), "ready".to_string()), "the fabric-primary put the ready fabric back to pending: {reset}");
    let answered = door_span["end_unix_nano"].as_u64().unwrap();
    assert!(start(reset) <= answered, "the fabric is pending before the door answers the growth Build: reset at {}, door answered at {answered} ({} us later than the answer): {reset}", start(reset), start(reset).saturating_sub(answered) / 1000);

    // The growth Build was written once, by the fabric-primary, after the Day-0 Build (R-BI3: the Build is
    // its own entity, in acceptance order).
    let accepted = named(&spans, "rdm.node_admin.build.create.via-accepted");
    let written = accepted.iter().find(|sp| sp["attributes"]["build_id"] == second.as_str()).unwrap_or_else(|| panic!("the growth Build's acceptance is spanned: {:?}", accepted.iter().map(|sp| &sp["attributes"]).collect::<Vec<_>>()));
    assert_eq!((s(&written["attributes"]["node"]), s(&written["attributes"]["previous_build_id"])), (fp_id.clone(), first.clone()), "the fabric-primary wrote the growth Build after the Day-0 Build: {written}");
    assert!(accepted.iter().all(|sp| sp["attributes"]["node"] == fp_id), "only the fabric-primary writes a Build: {:?}", accepted.iter().map(|sp| (&sp["attributes"]["node"], &sp["attributes"]["via"])).collect::<Vec<_>>());

    let opened = named(&spans, "rdm.mesh.node.create.via-connections-opened").into_iter().find(|sp| sp["attributes"]["node_path"] == source.path && sp["attributes"]["node_name"] == source.name).expect("mesh2.node.1 opened its connections store");
    assert_eq!(opened["attributes"]["writes_to"], "store-root", "a node writes to the store root: {opened}");
    let (first_root, _) = change_of(&spans, &first);
    estate.record_trace_url(&s(&first_root["trace_id"]));
}

/// CONTRACT (states.md "The fabric's ready": the fabric is ready once every planned mesh is
/// ready (Q174); gossip.md §3.2: timers only for heartbeat and anti-entropy): the Day-0 Build's
/// change finds every mesh ready when its attempt is claimed; a Build adding a member to a ready mesh puts
/// the fabric back to pending, and the fabric-primary finds every mesh ready when the last planned member's
/// ready is applied, and announces that member's node-type primary then, never on its next gossip interval. The estate runs at
/// the default gossip interval (2 s), so a wait on that interval is a gap of up to 2 s; the bound is a
/// quarter of it. Must NOT happen: the fabric, or the node-type primary's announcement, waiting for the
/// gossip interval after the claim or the member's ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fabric_is_ready_and_announces_the_node_type_primary_at_the_last_planned_members_ready() {
    let test = "the_fabric_is_ready_and_announces_the_node_type_primary_at_the_last_planned_members_ready";
    let gossip = Duration::from_millis(2000);
    let sha = candidate_sha();
    let env = [("RDM_GOSSIP_INTERVAL_MS", "2000"), ("RDM_BACKBONE_INTERVAL_MS", "2000"), ("RDM_STALENESS_MS", "30000")];
    let mut estate = Estate::bootstrap_on_with_env(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"], &env).await.expect("the faulted-admin binding set is accepted");
    // The Day-0 Build's change ends first: the admin alone, whose attempt's claim is the last thing the fabric waits on.
    until_fabric(&estate, ScopeStatus::Ready, None, Duration::from_secs(60)).await;
    let build = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1), ("node", 1)])] }).await.build_id.to_string();
    until_fabric(&estate, ScopeStatus::Ready, Some(build.as_str()), Duration::from_secs(60)).await;
    let nodes = estate.node_views().await;
    let rpc = nodes.iter().find(|n| n.mesh == "mesh1" && n.r#type == "node").expect("the `node` fixture");
    let primary = nodes.iter().find(|n| n.mesh == "mesh1" && n.r#type == "node-admin" && n.is_primary).expect("mesh primary");
    estate.stop().await;
    let spans = estate.spans();
    let ready = named(&spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|d| d["attributes"]["op"] == "declare-node-state" && d["attributes"]["state"] == "Ready" && d["attributes"]["sender"] == rpc.node_id.as_str() && d["attributes"]["outcome"] == "applied")
        .min_by_key(|d| start(d))
        .expect("mesh1.node.1's ready was applied by its mesh-primary");
    let (_, change) = change_of(&spans, &build);
    let entered = change.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-every-mesh-ready").unwrap_or_else(|| panic!("every mesh was ready under {build}"));
    let announced = named(&spans, "rdm.mesh.primary.update.via-announcement")
        .into_iter()
        .filter(|a| a["attributes"]["role"] == "node-primary" && a["attributes"]["via"] == "own" && a["attributes"]["node"] == primary.node_id.as_str())
        .min_by_key(|a| start(a))
        .expect("the mesh-primary announced mesh1's `node` primary");
    // The Day-0 Build: the first Build whose change found every mesh ready, and its attempt's claim. The
    // attempt is claimed when the claim decision ends (its in-flight row and its claim written): the
    // fabric-primary's reaction is measured from there, the claim's own reads and writes are not the wait.
    let day0 = named(&spans, "rdm.node_admin.fabric.update.via-change").into_iter().filter(|r| r["attributes"]["build_id"] != build.as_str()).min_by_key(|r| start(r)).map(|r| s(&r["attributes"]["build_id"])).expect("the Day-0 Build's change");
    let (_, day0_change) = change_of(&spans, &day0);
    let day0_entered = day0_change.iter().copied().find(|sp| sp["name"] == "rdm.node_admin.fabric.update.via-every-mesh-ready").expect("every mesh was ready under the Day-0 change");
    let day0_claimed = named(&spans, "rdm.node_admin.build.update.via-claim-decision").into_iter().filter(|c| c["attributes"]["build_id"] == day0.as_str() && c["attributes"]["outcome"] == "won").min_by_key(|c| start(c)).expect("the Day-0 Build's attempt was claimed");
    let ms = |from: &Value, to: &Value| (start(to) as i128 - start(from) as i128) / 1_000_000;
    let claim_decided = day0_claimed["end_unix_nano"].as_u64().expect("the Day-0 claim decision span carries its end");
    let claim_decision_ms = (claim_decided as i128 - start(day0_claimed) as i128) / 1_000_000;
    let (to_checklist, to_announced, claim_to_checklist) = (ms(ready, entered), ms(ready, announced), (start(day0_entered) as i128 - claim_decided as i128) / 1_000_000);
    estate.artifact("gaps.json", &json!({ "ready_to_every_mesh_ready_ms": to_checklist, "ready_to_announced_ms": to_announced, "day0_claim_to_every_mesh_ready_ms": claim_to_checklist, "day0_claim_decision_ms": claim_decision_ms, "gossip_interval_ms": gossip.as_millis() as u64 }));
    eprintln!("ready -> every mesh ready {to_checklist} ms, ready -> node-primary announced {to_announced} ms, Day-0 claim decided -> every mesh ready {claim_to_checklist} ms (the claim decision itself {claim_decision_ms} ms; gossip interval {} ms)", gossip.as_millis());
    let bound = (gossip / 4).as_millis() as i128;
    assert!(claim_to_checklist < bound, "the Day-0 fabric found every mesh ready {claim_to_checklist} ms after its Build's attempt was claimed (the claim decision ended), not within {bound} ms: it waited for the gossip interval");
    assert!(to_checklist < bound, "the fabric found every mesh ready {to_checklist} ms after mesh1.node.1's ready was applied, not within {bound} ms: it waited for the gossip interval");
    assert!(to_announced < bound, "the `node` primary was announced {to_announced} ms after mesh1.node.1's ready was applied, not within {bound} ms: it waited for the gossip interval");
    estate.record_trace_url(&s(&entered["trace_id"]));
}
