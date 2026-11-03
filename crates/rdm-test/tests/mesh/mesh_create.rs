//! i143.e4.s6 process E2E: the mesh creation contract (PRD §1.15, §12.1).
//!
//! desired {mesh1} -> {mesh1, mesh2}, through one fabric Build:
//! - the fabric primary creates mesh2's first node-admin; mesh2's own admin primary (that
//!   first admin) creates mesh2's second;
//! - mesh2's own admin primary creates mesh2's members (the same Build, a
//!   later attempt claimed by that admin);
//! - the test reads mesh2's control endpoint from the advertised topology, asks
//!   from there to grow and shrink mesh2 (`node.create`, `node.delete`, which the
//!   fabric-primary accepts); mesh2's admin primary executes those Builds.
//!
//! "Who executed" is read from each Build as the Build log holds it (`executor` of the
//! attempt) and from the evidence: every `node.create.via-build` /
//! `node.delete.via-build` span sits under a `build.update.via-reconcile`
//! whose `executor` is the expected admin, by parent span id.

use crate::build_log;
use rdm_core::{NodeId, PathName};
use rdm_client::{FabricDesired, MeshDesired, NodeStatus};
use rdm_test::estate::{claim_decider, descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-create".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "creating_a_mesh_hands_its_members_to_its_own_primary".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn location(path: &str, name: &str) -> PathName {
    PathName::new(path, name).unwrap()
}

fn actor_is(span: &Value, at: &PathName) -> bool {
    span["attributes"]["node_path"] == at.path && span["attributes"]["node_name"] == at.name
}

/// The fabric primary `observer` had resolved when `at` (a span) started: its own latest
/// `via-fabric-recompute` before that moment. The role moves as meshes grow (the lowest ready
/// NodeId fabric-wide wins), so "the fabric primary" is a fact at a time, never one name for the
/// whole test.
fn fabric_primary_seen_by(spans: &[Value], observer: &str, at: &Value) -> String {
    let t = at["start_unix_nano"].as_u64().unwrap_or(u64::MAX);
    named(spans, "rdm.mesh.election.resolve.via-fabric-recompute")
        .into_iter()
        .filter(|r| r["attributes"]["observer_node_id"] == observer && r["start_unix_nano"].as_u64().is_some_and(|s| s <= t))
        .max_by_key(|r| r["start_unix_nano"].as_u64())
        .map(|r| s(&r["attributes"]["primary"]))
        .unwrap_or_else(|| panic!("{observer} resolved no fabric primary before {}", at["name"]))
}

/// The span of `span_name` for `node` under `build_id`.
fn operation<'a>(spans: &'a [Value], span_name: &str, node: &PathName, build_id: &str) -> &'a Value {
    named(spans, span_name)
        .into_iter()
        .find(|s| actor_is(s, node) && s["attributes"]["build_id"] == build_id)
        .unwrap_or_else(|| panic!("no {span_name} for {node:?} under {build_id}"))
}

/// The admin whose reconcile ran `span_name` for `node` under `build_id`.
fn executed_by(spans: &[Value], span_name: &str, node: &PathName, build_id: &str) -> String {
    let op = named(spans, span_name)
        .into_iter()
        .find(|s| actor_is(s, node) && s["attributes"]["build_id"] == build_id)
        .unwrap_or_else(|| panic!("no {span_name} for {node:?} under {build_id}"))
        .clone();
    let reconcile = named(spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|r| r["attributes"]["build_id"] == build_id && descends_from(spans, &op, r))
        .unwrap_or_else(|| panic!("{node:?}: no reconcile of {build_id} above its operation"));
    s(&reconcile["attributes"]["executor_node_id"])
}

/// The executor of the last attempt of `build` (the Build view's JSON) that ran a step. The Build put the
/// fabric back to pending, so it is Complete only once the fabric is ready again from its own
/// change (Q154; states.md "Resize and executable replacement"); no page makes that Complete an attempt of
/// its own. The fabric-primary sets ready once every mesh the accepted Build names is ready and
/// its attempt is claimed (states.md "The fabric's ready"): a removal's meshes are ready at the
/// claim, so its run may be planned on a fabric already ready from this Build and end Complete
/// itself, while a run planned on a pending fabric hands back and the next attempt completes the Build.
fn last_run_executor(build: &Value) -> String {
    let stepped = build["steps"].as_array().into_iter().flatten().filter_map(|r| r["attempt"].as_u64()).max().expect("the Build ran steps");
    s(&build["attempt_executors"][stepped.to_string()]["node_id"])
}

/// Build `id` complete in the Build log of the admin the estate asks, as the JSON the view serializes to.
async fn built(estate: &Estate, id: &str) -> Value {
    let build = build_log::build_fact(&estate, id, "complete", Duration::from_secs(120), |b| b.state == rdm_admin::build_state::BuildState::Complete).await.unwrap_or_else(|end| panic!("{end}"));
    serde_json::to_value(build).expect("a Build view serializes")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creating_a_mesh_hands_its_members_to_its_own_primary() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let mesh = |m: &str| MeshDesired::of(m, [("node-admin", 2), ("node", 3)]);
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1")] }).await;
    // mesh2's rows are written to the store at once (node-identity-and-state.md §2.7, Q171), where
    // mesh2's own primary finds the row the fabric-primary wrote. The Build is Complete only after
    // the fabric's change from it (Q154; build.md §10); the wait records that the fabric has started.
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    // {mesh1} -> {mesh1, mesh2}
    let create = estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await.build_id.to_string();
    let build = built(&estate, &create).await;
    let want: std::collections::BTreeSet<PathName> = ["mesh1", "mesh2"]
        .iter()
        .flat_map(|m| (1..=2).map(move |i| location(&format!("{m}.node-admin"), &i.to_string())).chain((1..=3).map(move |i| location(&format!("{m}.node"), &i.to_string()))))
        .collect();
    let nodes = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(15)).await);
    assert_eq!(nodes.iter().filter(|n| n["is_fabric_primary"] == true).count(), 1, "one fabric primary once the view settles");
    let mesh2_primary = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).map(|n| s(&n["node_id"])).unwrap();
    let node_at = |path: &str, name: &str| nodes.iter().find(|node| node["name"]["path"] == path && node["name"]["name"] == name).expect("settled node at explicit location");
    let first_admin = node_at("mesh2.node-admin", "1");
    let day0_admin = node_at("mesh1.node-admin", "1");
    assert_eq!(last_run_executor(&build), mesh2_primary, "the Build's last run ran on mesh2's own primary: {build:#}");

    // mesh2's control endpoint, from the advertised topology only.
    let fabric = serde_json::to_value(estate.fabric_view().await).expect("a view serializes");
    let mesh2_base = fabric["meshes"].as_array().unwrap().iter().find(|m| m["name"] == "mesh2").map(|m| s(&m["admin_api_base"])).unwrap();
    let mesh2_view = serde_json::to_value(estate.mesh_view("mesh2").await).expect("a view serializes");
    assert_eq!(s(&mesh2_view["admin_api_base"]), mesh2_base);
    assert_eq!(s(&mesh2_view["primary_admin"]), mesh2_primary);
    // The created mesh is a new identity, distinct from every mesh the fabric already had.
    let m1 = serde_json::to_value(estate.mesh_view("mesh1").await).expect("a view serializes");
    let mesh2_id = s(&mesh2_view["id"]);
    assert!(!mesh2_id.is_empty() && mesh2_id != s(&m1["id"]), "mesh2 is a new identity: {mesh2_view}");
    let fabric_admin = estate.admin.clone();
    estate.enter_by_id(&NodeId::parse(&mesh2_primary).unwrap()).await;

    // The initial fabric refuses the removal of a node until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // Grow and shrink mesh2 from its own endpoint: the fabric-primary accepts the call, mesh2's primary executes the Build.
    let mut growing = estate.create_node("mesh2", "node").await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of a node in mesh2: {e}"));
    let grow = Estate::complete(&mut growing).await.expect("the call opened a Build").build_id.to_string();
    let b = built(&estate, &grow).await;
    assert_eq!(last_run_executor(&b), mesh2_primary, "{b:#}");
    estate.until_node(&rdm_core::PathName::new("mesh2.node", "4").unwrap(), "mesh2.node.4 ready", Duration::from_secs(30), |n| n.filter(|n| n.status == NodeStatus::Ready).map(|_| ())).await;
    let mut shrinking = estate.nodes_obj().await.delete(&rdm_core::PathName::new("mesh2.node", "4").unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of mesh2.node.4: {e}"));
    let shrink = Estate::complete(&mut shrinking).await.expect("the call opened a Build").build_id.to_string();
    let b = built(&estate, &shrink).await;
    assert_eq!(last_run_executor(&b), mesh2_primary, "{b:#}");
    estate.until_node(&rdm_core::PathName::new("mesh2.node", "4").unwrap(), "mesh2.node.4 deleted", Duration::from_secs(30), |n| n.is_none().then_some(())).await;

    estate.artifact("nodes.json", &json!(estate.node_values().await));
    estate.enter(&fabric_admin).await;
    estate.stop().await;
    let spans = estate.spans();
    // The first admin of a mesh that has none is created by whoever held the fabric-primary role when the
    // create ran: the executor's own view at that moment. Day 0's fabric primary may hand the role
    // to a lower NodeId in mesh2 as soon as mesh2's first admin is ready, so a name read after the
    // test is not the creator's role. The second admin is created by mesh2's own primary, which at
    // that moment is the only admin mesh2 has (node-admin-lifecycle.md 4.3).
    let op = operation(&spans, "rdm.node_admin.node.create.via-build", &location("mesh2.node-admin", "1"), &create);
    let creator = executed_by(&spans, "rdm.node_admin.node.create.via-build", &location("mesh2.node-admin", "1"), &create);
    assert_eq!(fabric_primary_seen_by(&spans, &creator, op), creator, "mesh2.node-admin.1: created by the admin holding the fabric-primary role when it ran");
    assert_eq!(executed_by(&spans, "rdm.node_admin.node.create.via-build", &location("mesh2.node-admin", "2"), &create), s(&first_admin["node_id"]), "mesh2.node-admin.2: created by mesh2's own primary");
    for name in ["1", "2", "3"] {
        let r = location("mesh2.node", name);
        assert_eq!(executed_by(&spans, "rdm.node_admin.node.create.via-build", &r, &create), mesh2_primary, "{r:?}: mesh2's primary creates its members");
    }
    assert_eq!(executed_by(&spans, "rdm.node_admin.node.create.via-build", &location("mesh2.node", "4"), &grow), mesh2_primary);
    assert_eq!(executed_by(&spans, "rdm.node_admin.node.delete.via-build", &location("mesh2.node", "4"), &shrink), mesh2_primary);
    // The Pending hand-off (e4.s11): once mesh2's first admin is born, the fabric primary applies
    // MeshStatus::Pending at that exact birth, and only then does the shape proceed. Evidence names
    // the sender's birth, the target's birth, the exact mesh identity and the applied state.
    let handoffs: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|h| h["attributes"]["mesh"] == "mesh2").collect();
    let applied: Vec<&&Value> = handoffs.iter().filter(|h| h["attributes"]["outcome"] == "applied").collect();
    assert_eq!(applied.len(), 1, "one Pending applied at mesh2's bootstrap admin: {handoffs:?}");
    let handoff = applied[0];
    for k in ["node_id", "incarnation", "target_path", "target_name", "target_node_id", "target_incarnation", "mesh_id", "key"] {
        assert!(handoff["attributes"][k].as_str().is_some_and(|v| !v.is_empty()), "the hand-off names {k}: {handoff}");
    }
    assert_eq!(handoff["attributes"]["target_node_id"], first_admin["node_id"]);
    assert_eq!(handoff["attributes"]["target_incarnation"], first_admin["incarnation_id"]);
    assert_eq!(handoff["attributes"]["state"], "Pending");
    let handed = handoff["end_unix_nano"].as_u64().unwrap();
    let shape_started = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| c["attributes"]["build_id"] == create.as_str() && [location("mesh2.node-admin", "2"), location("mesh2.node", "1"), location("mesh2.node", "2"), location("mesh2.node", "3")].iter().any(|at| actor_is(c, at)))
        .filter_map(|c| c["start_unix_nano"].as_u64())
        .min()
        .expect("the shape's creates");
    assert!(handed <= shape_started, "Pending Applied ({handed}) precedes Build(shape) ({shape_started})");
    // The target decided it through the same door as every declaration, and accepting Pending did
    // not make it the elected mesh primary.
    let decided: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|d| actor_is(d, &location("mesh2.node-admin", "1")) && d["attributes"]["op"] == "apply-mesh-state" && d["attributes"]["outcome"] == "applied")
        .collect();
    assert_eq!(decided.len(), 1, "{decided:?}");
    assert_eq!(decided[0]["attributes"]["receiver_is_primary"], "false", "accepting Pending is not an election: {}", decided[0]);
    let accepted = named(&spans, "rdm.node_admin.build.create.via-node-rpc").into_iter().find(|sp| sp["attributes"]["build_id"] == create.as_str()).unwrap().clone();
    // The accepting fabric-primary holds the request's context: every attempt it decides descends
    // from the request. The role moves to mesh2's lower NodeId during the creation; an attempt the
    // new fabric-primary decides starts its own trace (the context is the old primary's local record).
    let accepting = claim_decider(&spans, create.as_str(), "1").expect("attempt 1 of the creation Build was claimed");
    let mut continued = 0;
    for r in named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| r["attributes"]["build_id"] == create.as_str()) {
        if claim_decider(&spans, create.as_str(), r["attributes"]["attempt"].as_str().unwrap_or_default()).is_none_or(|d| d == accepting) {
            assert!(descends_from(&spans, r, &accepted), "an attempt the accepting fabric-primary decided descends from its request: {r}");
            continued += 1;
        } else {
            assert_eq!(r["parent_span_id"].as_str().unwrap_or(""), "", "an attempt decided after the role moved starts its own trace: {r}");
        }
    }
    assert!(continued >= 1, "the first attempt continues the request's trace");
    // node-identity-and-state.md §2.7: "The mesh's primary node-admin writes its row at creation", the
    // name checked "against the mesh records at the key `(path, name)`", the same MeshId writing its own
    // row again accepted; R13: the fabric-primary writes as the acting primary of a mesh with no
    // primary; the mesh's row is its create's first durable write. mesh1's row is written by its Day-0
    // primary. mesh2's is written at CreateMesh by the fabric-primary that creates mesh2's first admin,
    // before that admin's node row is written; mesh2's own primary writing it again finds its row. No
    // write is refused across creation and a write again.
    let mesh_writes = named(&spans, "rdm.node_admin.mesh.create.via-mesh-row");
    let written = |mesh: &str, id: &str| {
        mesh_writes
            .iter()
            .find(|c| c["attributes"]["mesh"] == mesh && s(&c["attributes"]["mesh_id"]) == id && c["attributes"]["moment"] == "write" && c["attributes"]["outcome"] == "row-written")
            .copied()
            .unwrap_or_else(|| panic!("{mesh}'s row is written: {mesh_writes:?}"))
    };
    let m1_write = written("mesh1", &s(&m1["id"]));
    assert_eq!(m1_write["attributes"]["node"], day0_admin["node_id"], "mesh1's row is written by its Day-0 primary: {m1_write}");
    let m2_write = written("mesh2", &mesh2_id);
    assert_eq!(s(&m2_write["attributes"]["node"]), creator, "mesh2's row is written by the fabric-primary that creates its first admin, the acting primary of a mesh with none: {m2_write}");
    let admin_row = named(&spans, "rdm.node_admin.node.create.via-node-row")
        .into_iter()
        .find(|c| c["attributes"]["subject_path"] == "mesh2.node-admin" && c["attributes"]["subject_name"] == "1" && c["attributes"]["moment"] == "create")
        .unwrap_or_else(|| panic!("mesh2.node-admin.1's row is written"));
    assert!(
        m2_write["end_unix_nano"].as_u64().unwrap() <= admin_row["start_unix_nano"].as_u64().unwrap(),
        "mesh2's row is its first durable write: it ends before mesh2.node-admin.1's row is written: {m2_write} {admin_row}"
    );
    let rewrites: Vec<&&Value> = mesh_writes.iter().filter(|c| c["attributes"]["mesh"] == "mesh2" && nodes.iter().any(|n| n["mesh"] == "mesh2" && n["type"] == "node-admin" && n["node_id"] == c["attributes"]["node"])).collect();
    assert!(!rewrites.is_empty() && rewrites.iter().all(|c| s(&c["attributes"]["mesh_id"]) == mesh2_id && c["attributes"]["outcome"] == "row-held-by-mesh"), "mesh2's own primary writes its row again and finds the row the fabric-primary wrote: {rewrites:?}");
    eprintln!("trace {}: rdm.node_admin.mesh.create.via-mesh-row mesh=mesh2 mesh_id={mesh2_id} node={creator} outcome=row-written before node.create.via-node-row subject=mesh2.node-admin.1", s(&m2_write["trace_id"]));
    assert!(mesh_writes.iter().all(|c| c["attributes"]["outcome"] == "row-written" || c["attributes"]["outcome"] == "row-held-by-mesh"), "no mesh's write is refused: {mesh_writes:?}");
    // node-identity-and-state.md §2.7: "The fabric-primary writes the fabric's row at Day 0, idempotent
    // for the same FabricId", its first write of the fabric; a later fabric-primary's write of the same
    // FabricId finds its own row and writes nothing.
    let fabric_id = s(&fabric["id"]);
    let fabric_writes = named(&spans, "rdm.node_admin.fabric.create.via-fabric-row");
    let first = fabric_writes
        .iter()
        .filter(|c| s(&c["attributes"]["fabric_id"]) == fabric_id && c["attributes"]["moment"] == "write")
        .min_by_key(|c| c["start_unix_nano"].as_u64())
        .unwrap_or_else(|| panic!("the fabric-primary writes the fabric's row: {fabric_writes:?}"));
    assert_eq!(first["attributes"]["outcome"], "row-written", "{first}");
    assert!(
        fabric_writes.iter().all(|c| s(&c["attributes"]["fabric_id"]) == fabric_id && (c["attributes"]["outcome"] == "row-written" || c["attributes"]["outcome"] == "row-held-by-fabric")),
        "no fabric write is refused or names another Fabric: {fabric_writes:?}"
    );
    eprintln!("trace {}: rdm.node_admin.fabric.create.via-fabric-row fabric_id={fabric_id} outcome=row-written", s(&first["trace_id"]));
    estate.record_trace_url(accepted["trace_id"].as_str().unwrap_or(""));
}
