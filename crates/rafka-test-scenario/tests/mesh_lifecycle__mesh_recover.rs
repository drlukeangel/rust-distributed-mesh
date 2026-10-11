//! i143.e4.s7 process E2E: the mesh recovery contract (PRD §1.15, §12.2).
//!
//! desired {mesh1, mesh2}, observed {mesh2's admins, mesh1's members} -> recover mesh1. A Build B is
//! active (growing mesh1, executed by mesh1's admin primary, the fabric-primary) when both of
//! mesh1's node-admins and every birth B's first attempt launched are killed; the three members
//! born before B stay alive. From public surfaces only:
//! - control is found again through the advertised endpoints;
//! - B completes under its own id: a surviving admin takes it over and reconstructs mesh1;
//! - mesh1 is recovered as itself: the same mesh id and the same node names;
//! - every node-admin of mesh1 is a new birth (a new incarnation); the members that stayed alive
//!   keep theirs;
//! - mesh2 was never touched (every mesh2 node keeps its incarnation).
//!
//! Evidence: the new fabric-primary's peer-mesh investigation finds, through a member of mesh1
//! that answers, that mesh1's node-admins cannot be reached (`carrier-edge-lost`), records the
//! recovery verdict (`via-rebirth`), and only then creates mesh1's first node-admin; a reconcile of B by a
//! mesh2 admin names mesh1's admin primary as the previous executor.

use rafka_test_scenario::estate::{claim_decider, descends_from, named, own_fabric_at, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-recover".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_lost_mesh_recovers_as_itself_under_the_same_build".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

async fn get(base: &str, path: &str) -> Option<Value> {
    let r = reqwest::Client::new().get(format!("{base}{path}")).timeout(Duration::from_secs(2)).send().await.ok()?;
    r.status().is_success().then_some(())?;
    r.json().await.ok()
}

fn names(meshes: &[(&str, u32, u32)]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (m, a, r) in meshes {
        out.extend((1..=*a).map(|i| format!("{m}.admin.{i}")));
        out.extend((1..=*r).map(|i| format!("{m}.rpc.{i}")));
    }
    out
}

/// name -> incarnation
fn births(nodes: &[Value]) -> BTreeMap<String, String> {
    nodes.iter().map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect()
}

/// CONTRACT (mesh-investigation.md step 5: "Recovery ONLY if the latest probe answers
/// `carrier-edge-lost`: a mesh-x member reports that it cannot reach its own node-admin ... Record
/// the verdict before opening the recovery attempt"; step 6: "A recovery verdict starts Mesh
/// recovery, on the SAME accepted Build and mesh-x's own MeshId"; mesh-recovery.md step 1: "The
/// peer-mesh investigation reaches its recovery verdict"; R-D1): mesh1 loses both node-admins
/// mid-Build while three of its members live on. The seat moves
/// to mesh2's primary, whose investigation probes mesh1 through a member that answers, hears
/// `carrier-edge-lost` for each node-admin, records the recovery verdict, and only then creates mesh1's first
/// node-admin under B; mesh1's new primary finishes B. Must NOT happen: a node of mesh1 created by B
/// before the recovery verdict (the fence holds from the moment the seat is taken), a new MeshId, a
/// member that stayed alive born again, or mesh2 touched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_mesh_recovers_as_itself_under_the_same_build() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let shape = |r1: u32| json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": r1},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 3},
    ]});
    let (_, a) = estate.post("/api/build", &shape(3)).await;
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let before = estate.settled(&names(&[("mesh1", 2, 3), ("mesh2", 2, 3)]), Duration::from_secs(15)).await;
    let (_, mesh1) = estate.get("/api/meshes/mesh1").await;
    let mesh1_id = s(&mesh1["id"]);
    // mesh1's members are grown by mesh1's admin primary (the lowest NodeId of its admins).
    let mesh1_primary = before.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).unwrap();
    let (_, fabric) = estate.get("/api/fabric").await;
    let advertised: Vec<String> = fabric["meshes"].as_array().unwrap().iter().map(|m| s(&m["admin_api_base"])).collect();

    // B: grow mesh1. Once its first attempt is claimed, lose mesh1 entirely. Its births run one
    // after another (~55 ms each on the process provider), so six of them keep the first attempt
    // running across several polls; two finished inside one 100 ms poll.
    let (status, a) = estate.post("/api/build", &shape(9)).await;
    assert_eq!(status, 202, "{a}");
    let b = s(&a["build_id"]);
    // Active fabric-wide: the surviving mesh's admin already sees mesh1's
    // admin holding B's first attempt (a claim that never left the lost mesh
    // would leave the successor nothing to take over: it would claim afresh).
    let mesh2_base = advertised.iter().find(|b| **b != estate.admin).cloned().unwrap();
    wait_for("mesh2's admin sees B's first attempt held by mesh1's admin", Duration::from_secs(30), || async {
        let v = get(&mesh2_base, &format!("/api/builds?id={b}")).await?;
        (v["attempt"].as_u64() == Some(1) && v["state"] == "running" && v["executor"] == mesh1_primary.as_str()).then_some(())
    })
    .await;
    // mesh1's node-admins and every birth B's first attempt launched; the members born before B
    // (rpc.1..3) stay alive, so a member of mesh1 can say it cannot reach its own node-admin.
    let kept: BTreeSet<String> = (1..=3).map(|i| format!("mesh1.rpc.{i}")).collect();
    for (dir, pid) in estate.live_runtimes() {
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        let path = name.split('-').next().unwrap_or_default().to_string();
        if path.starts_with("mesh1.") && !kept.contains(&path) {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
    }
    estate.kill_bootstrap();
    let killed_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // Control, through the advertised endpoints only.
    let base = wait_for("a surviving admin answers", Duration::from_secs(30), || {
        let advertised = advertised.clone();
        let fabric_id = estate.fabric_id.clone();
        async move {
            for base in &advertised {
                if own_fabric_at(base, &fabric_id).await.is_some() {
                    return Some(base.clone());
                }
            }
            None
        }
    })
    .await;
    estate.admin = base;

    // B completes under its own id, and mesh1 is back as itself.
    let done = estate.await_build(&b, Duration::from_secs(180)).await;
    assert!(done["attempt"].as_u64().unwrap() > 1, "a successor continued B: {done:#}");
    let after = estate.settled(&names(&[("mesh1", 2, 9), ("mesh2", 2, 3)]), Duration::from_secs(60)).await;
    let (_, mesh1) = estate.get("/api/meshes/mesh1").await;
    assert_eq!(s(&mesh1["id"]), mesh1_id, "mesh1 recovered as itself: {mesh1:#}");
    // ...in the recovered mesh's own admins too: they carry its identity.
    for admin in after.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin") {
        let own = get(&s(&admin["admin_api_base"]), "/api/meshes/mesh1").await.unwrap();
        assert_eq!(s(&own["id"]), mesh1_id, "{} serves mesh1 under its own identity: {own:#}", admin["name"]);
    }
    let (old, new) = (births(&before), births(&after));
    for (name, incarnation) in &old {
        let now_inc = &new[name];
        if name.starts_with("mesh1.admin.") {
            assert_ne!(now_inc, incarnation, "{name} is a new birth");
        } else if kept.contains(name) {
            assert_eq!(now_inc, incarnation, "{name} stayed alive through the loss and keeps its birth");
        } else {
            assert_eq!(now_inc, incarnation, "{name} was never touched");
        }
    }

    // Stop through whoever holds the fabric now.
    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.artifact("nodes.json", &json!(after));
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    // The recovery verdict: the new fabric-primary's probes of mesh1 went through a member that
    // answered and reported mesh1's node-admins lost, the latest probe decided, and the recovery opened.
    let rebirth = named(&spans, "rdm.node_admin.mesh.create.via-rebirth").into_iter().find(|r| attr(r, "mesh") == "mesh1" && attr(r, "node").starts_with("mesh2.")).unwrap_or_else(|| panic!("the recovery of mesh1 opened (via-rebirth)")).clone();
    let probes: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-probe").into_iter().filter(|p| attr(p, "mesh") == "mesh1" && attr(p, "node") == attr(&rebirth, "node") && at(p) > killed_ns && at(p) < at(&rebirth)).collect();
    let last = probes.iter().max_by_key(|p| at(p)).unwrap_or_else(|| panic!("mesh1 was probed by the new fabric-primary before its recovery verdict"));
    assert_eq!(attr(last, "outcome"), "carrier-edge-lost", "the latest probe: a member of mesh1 cannot reach its node-admins: {probes:#?}");
    assert!(kept.contains(&attr(last, "carrier")), "the carrier is a member that stayed alive: {last}");
    assert!(
        named(&spans, "rdm.node_admin.mesh.update.via-probe-verdict").iter().any(|v| attr(v, "mesh") == "mesh1" && attr(v, "outcome") == "carrier-edge-lost" && at(v) <= at(&rebirth)),
        "the verdict was recorded before the recovery attempt opened"
    );
    // The fence held from the seat take: no node of mesh1 was created by B before the verdict.
    let early: Vec<String> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .chain(named(&spans, "rdm.node_admin.node.update.via-build"))
        .filter(|c| attr(c, "build_id") == b && attr(c, "node").starts_with("mesh1.") && at(c) > killed_ns && at(c) < at(&rebirth))
        .map(|c| format!("{} attempt {} at {}", attr(c, "node"), attr(c, "attempt"), at(c)))
        .collect();
    assert!(early.is_empty(), "nothing of mesh1 is created before its recovery verdict (via-rebirth at {}): {early:?}", at(&rebirth));
    // B was accepted, and its attempt 1 claimed, by the fabric-primary of before the loss. The cell
    // killed that admin (SIGKILL), and its exporter writes in batches (200 ms): what it had not written
    // went with it, so nothing here reads that admin's spans unless it decided the takeover itself.
    let accepting = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("a fabric-primary before the loss");
    let takeover = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|r| {
            let a = &r["attributes"];
            a["build_id"] == b.as_str() && a["previous_executor"] == mesh1_primary.as_str() && a["executor"].as_str().is_some_and(|e| e.starts_with("mesh2."))
        })
        .unwrap_or_else(|| panic!("no takeover of {b} by a mesh2 admin"))
        .clone();
    // The attempt context lives with the fabric-primary that accepted B: a takeover attempt that
    // admin decided descends from B's request; one decided after the seat moved starts its own
    // trace (ruling R-X1: the context is the accepting primary's local record).
    let attempt = takeover["attributes"]["attempt"].as_str().map(String::from).unwrap_or_else(|| takeover["attributes"]["attempt"].to_string());
    let decider = claim_decider(&spans, b.as_str(), &attempt).unwrap_or_else(|| panic!("the claim of takeover attempt {attempt} of {b} has its decision span"));
    if decider == accepting {
        let accepted = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == b.as_str()).expect("B's request").clone();
        assert!(descends_from(&spans, &takeover, &accepted), "a takeover the accepting fabric-primary decided descends from B's request");
    } else {
        assert_eq!(takeover["parent_span_id"].as_str().unwrap_or(""), "", "a takeover decided after the seat moved starts its own trace: {takeover}");
    }
    // Each recovered admin held every surviving member's runtime before it
    // committed Ready (i143.e4.s16): mesh2's five births at least.
    for admin in after.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "node_admin") {
        let ready = named(&spans, "rdm.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == admin["name"] && sp["attributes"]["incarnation_id"] == admin["incarnation_id"])
            .unwrap_or_else(|| panic!("{} reports ready", admin["name"]))
            .clone();
        let held = ready["attributes"]["runtime_facts_held"].as_u64().or_else(|| ready["attributes"]["runtime_facts_held"].as_str().and_then(|v| v.parse().ok())).unwrap_or(0);
        assert!(held >= 5, "{} held {held} runtime facts before Ready: {ready}", admin["name"]);
    }
    eprintln!("mesh_recover: rebirth trace {} | latest probe trace {} | takeover trace {}", s(&rebirth["trace_id"]), s(&last["trace_id"]), s(&takeover["trace_id"]));
    estate.record_trace_url(takeover["trace_id"].as_str().unwrap_or(""));
}
