//! i143.e4.s8 process E2E: the mesh replacement contract (PRD §12.3).
//!
//! desired {mesh1, mesh2} -> {mesh2, mesh3} through one Build: mesh1 leaves (mesh-leave.md: the
//! owner outside mesh1 runs `shutdown-mesh:<mesh_id>`, mesh1's primary drains and stops every other
//! member, the owner drains and stops mesh1's final primary and records `MeshLeft`), and mesh3 is
//! created under a new identity. This is a desired change, never a recovery: no proven-drift Build
//! names mesh1, nothing recreates a mesh1 node, and the scenario carries its own manifest
//! (subfeature `mesh-replace`).

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-replace".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "replacing_a_mesh_retires_the_old_one_and_creates_a_new_identity".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn mesh(name: &str) -> Value {
    json!({"name": name, "node_admin": 2, "rpc_node": 2})
}

fn names(meshes: &[&str]) -> BTreeSet<String> {
    meshes
        .iter()
        .flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1"), format!("{m}.rpc.2")])
        .collect()
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn end_ns(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}

fn attr_u(sp: &Value, k: &str) -> u64 {
    sp["attributes"][k].as_str().and_then(|v| v.parse().ok()).or_else(|| sp["attributes"][k].as_u64()).unwrap_or(0)
}

fn alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_a_mesh_retires_the_old_one_and_creates_a_new_identity() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let before = estate.settled(&names(&["mesh1", "mesh2"]), Duration::from_secs(30)).await;
    let (_, fabric) = estate.get("/api/fabric").await;
    let fabric_id = s(&fabric["id"]);
    let (_, m1) = estate.get("/api/meshes/mesh1").await;
    let (_, m2) = estate.get("/api/meshes/mesh2").await;
    let (mesh1_id, mesh2_id) = (s(&m1["id"]), s(&m2["id"]));
    let mesh1_pids: Vec<(String, u64)> = {
        let mut v = Vec::new();
        for n in before.iter().filter(|n| n["mesh"] == "mesh1") {
            let name = s(&n["name"]);
            // The bootstrap admin was adopted on day 0: its pid is the estate's own child.
            let pid = match estate.bootstrap_pid() {
                Some(p) if name == "mesh1.admin.1" => u64::from(p),
                _ => estate.pid_of(&name).await,
            };
            v.push((name, pid));
        }
        v
    };

    // The Node rows mesh1's admin holds, read from its data dir while it runs.
    let mesh1_ids: Vec<(String, String)> = before.iter().filter(|n| n["mesh"] == "mesh1").map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
    let mesh1_dir = estate.data_dir_of("mesh1.admin.1").await;

    // Control moves to mesh2 before mesh1 goes: its admins advertise it in the topology.
    let mesh2_admin = before
        .iter()
        .find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["status"] == "ready-for-traffic")
        .map(|n| s(&n["admin_api_base"]))
        .expect("a ready mesh2 admin");
    estate.admin = mesh2_admin.clone();

    // {mesh1, mesh2} -> {mesh2, mesh3}: one Build.
    let (status, b) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh2"), mesh("mesh3")]})).await;
    assert_eq!(status, 202, "{b}");
    let b = s(&b["build_id"]);
    estate.await_build(&b, Duration::from_secs(240)).await;
    let after = estate.settled(&names(&["mesh2", "mesh3"]), Duration::from_secs(60)).await;

    // Identities: the Fabric and mesh2 keep theirs; mesh3 is a new mesh; mesh1 is gone.
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["id"]), fabric_id, "the Fabric keeps its id");
    let (_, m2_after) = estate.get("/api/meshes/mesh2").await;
    assert_eq!(s(&m2_after["id"]), mesh2_id, "an untouched mesh keeps its id");
    let (_, m3) = estate.get("/api/meshes/mesh3").await;
    let mesh3_id = s(&m3["id"]);
    assert!(!mesh3_id.is_empty() && mesh3_id != mesh1_id && mesh3_id != mesh2_id, "mesh3 is a new identity: {m3}");
    let (status, gone) = estate.get("/api/meshes/mesh1").await;
    assert_eq!(status, 404, "mesh1 is no longer a mesh of the fabric: {gone}");
    assert!(!after.iter().any(|n| n["mesh"] == "mesh1"), "no mesh1 node remains: {after:#?}");
    for (name, pid) in &mesh1_pids {
        wait_for(&format!("{name}'s runtime is gone"), Duration::from_secs(30), || async { (!alive(*pid)).then_some(()) }).await;
    }

    // Dead is final and its members resolve Gone: a mesh2 ordinary node's own live resolver answers
    // `gone` for every departed mesh1 birth, never `unknown`, and nothing dials it.
    for (name, id) in mesh1_ids.iter() {
        let query = format!("exact:{id}");
        wait_for(&format!("mesh2.rpc.1's resolver answers Gone for departed {name}"), Duration::from_secs(30), || async {
            let r = estate.probe(&["resolve", "--target", "path:mesh2.rpc.1", "--query", &query]);
            (r["reply"]["resolution"] == "gone").then_some(())
        })
        .await;
    }
    // The durable Node rows are not deleted: mesh1's admin still holds its own row and a contact
    // row for every other birth it launched.
    assert!(std::path::Path::new(&format!("{mesh1_dir}/nodes/self.json")).exists(), "mesh1.admin.1's own Node row remains in {mesh1_dir}");
    for (name, id) in mesh1_ids.iter().filter(|(n, _)| n != "mesh1.admin.1") {
        assert!(std::path::Path::new(&format!("{mesh1_dir}/nodes/contacts/{id}.json")).exists(), "the Node row of {name} ({id}) remains in {mesh1_dir}");
    }
    let (_, fabric_now) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric_now["admin_api_base"]);
    let owner_name = {
        let spans = estate.spans();
        let reconciles = named(&spans, "rdm.node_admin.build.update.via-reconcile");
        let r = reconciles.iter().find(|sp| sp["attributes"]["build_id"] == b.as_str() && sp["attributes"]["operations"].as_str().is_some_and(|o| o.contains("shutdown-mesh:"))).cloned().expect("a reconcile executed the shutdown-mesh operation");
        s(&r["attributes"]["executor"])
    };
    // The MeshLeft receipt, read from the live Build projection (the admin API): one Complete
    // receipt under the outer operation, covering every birth of the mesh.
    let (_, build_now) = estate.get(&format!("/api/builds?id={b}")).await;
    let left_receipts: Vec<Value> = build_now["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["step"] == "MeshLeft").collect();
    let other_receipts: Vec<Value> = build_now["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["step"] == "OtherMembersExited").collect();
    estate.stop().await;
    let spans = estate.spans();
    assert_eq!(other_receipts.len(), 1, "one OtherMembersExited receipt, recorded by mesh1's primary: {build_now}");
    assert_eq!(other_receipts[0]["output"]["receipts"].as_array().map(|r| r.len()), Some(3), "a terminal receipt for every birth of mesh1 but its final primary");
    assert_eq!(left_receipts.len(), 1, "one MeshLeft receipt under {b} (owner {owner_name})");
    assert_eq!(left_receipts[0]["outcome"], json!("complete"), "{}", left_receipts[0]);
    assert_eq!(left_receipts[0]["operation"], format!("shutdown-mesh:{mesh1_id}"), "keyed by the mesh's minted id");
    assert_eq!(left_receipts[0]["output"]["receipts"].as_array().map(|r| r.len()), Some(4), "a terminal receipt for every birth of mesh1, the final primary included: {}", left_receipts[0]);

    // mesh1 left through the mesh-leave workflow under B: every member drained, stopped and proven
    // exited in the documented order, and none of the node-removal steps ran (the Node rows stay).
    let steps = named(&spans, "rdm.node_admin.deployment.update.via-step");
    let order_of = |name: &str| -> Vec<(String, u64, u64)> {
        let mut v: Vec<&Value> = steps.iter().copied().filter(|sp| sp["attributes"]["node"] == name && sp["attributes"]["build_id"] == b.as_str()).collect();
        v.sort_by_key(|sp| sp["start_unix_nano"].as_u64());
        v.iter().map(|sp| (s(&sp["attributes"]["step"]), sp["start_unix_nano"].as_u64().unwrap_or(0), sp["end_unix_nano"].as_u64().unwrap_or(0))).collect()
    };
    for (name, _) in &mesh1_pids {
        let order = order_of(name);
        let names: Vec<&str> = order.iter().map(|(n, _, _)| n.as_str()).collect();
        assert_eq!(names, ["DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft", "TerminateRuntime"], "{name}: drain-node, node-drained, stop-node, node-left, then the provider's exit proof; no node-removal step");
        let mine: Vec<&Value> = steps.iter().copied().filter(|sp| sp["attributes"]["node"] == name.as_str() && sp["attributes"]["build_id"] == b.as_str()).collect();
        assert!(mine.iter().all(|sp| sp["attributes"]["outcome"] == "complete"), "{name}: every step completed");
    }
    // Tiers: the ordinary nodes' workflows overlap in time; the other node-admin starts only after
    // the last ordinary node's exit is proven; the final primary's drain starts after the other
    // admin's exit is proven and the handoff was validated.
    let member = |name: &str| -> (u64, u64) {
        let sp = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().find(|sp| sp["attributes"]["node"] == name && sp["attributes"]["operation"].as_str().is_some_and(|o| o.starts_with("shutdown-mesh:"))).cloned().unwrap_or_else(|| panic!("no shutdown-member span for {name}"));
        (start_ns(&sp), end_ns(&sp))
    };
    let terminal_end = |name: &str| order_of(name).iter().find(|(n, _, _)| n == "TerminateRuntime").map(|(_, _, e)| *e).unwrap();
    let drain_start = |name: &str| order_of(name).iter().find(|(n, _, _)| n == "DrainNode").map(|(_, st, _)| *st).unwrap();
    let (r1, r2) = (member("mesh1.rpc.1"), member("mesh1.rpc.2"));
    assert!(r1.0 < r2.1 && r2.0 < r1.1, "the ordinary nodes' workflows overlap in time: {r1:?} {r2:?}");
    let tier1_last = terminal_end("mesh1.rpc.1").max(terminal_end("mesh1.rpc.2"));
    let others: Vec<String> = mesh1_pids.iter().map(|(n, _)| n.clone()).filter(|n| n.contains(".admin.")).collect();
    // The final primary is the one whose workflow the owner ran: its shutdown-member span is not in the mesh-primary's tier.
    let handoff = named(&spans, "rdm.node_admin.mesh.update.via-handoff-validated").into_iter().find(|sp| sp["attributes"]["operation"].as_str().is_some_and(|o| o.starts_with("shutdown-mesh:"))).cloned().expect("the handoff was validated");
    assert_eq!(handoff["attributes"]["outcome"], "applied", "the owner validated the mesh-leave handoff: {handoff}");
    let final_name = others.iter().max_by_key(|n| drain_start(n)).unwrap().clone();
    let tier2: Vec<&String> = others.iter().filter(|n| **n != final_name).collect();
    for n in &tier2 {
        assert!(drain_start(n) > tier1_last, "{n} (a node-admin) drained only after every ordinary node's exit was proven: {} <= {tier1_last}", drain_start(n));
    }
    let tier2_last = tier2.iter().map(|n| terminal_end(n)).max().unwrap_or(tier1_last);
    assert!(drain_start(&final_name) > tier2_last && drain_start(&final_name) > end_ns(&handoff), "the final primary {final_name} drained after every other exit and after the validated handoff");
    // The gossip hooks: mesh-leaving by the owner, mesh-leave by the still-running primary, mesh-left
    // by the owner after the final exit; each heard by another mesh's nodes.
    for hook in ["via-mesh-leaving", "via-mesh-leave", "via-mesh-left"] {
        let published = named(&spans, &format!("rdm.node_admin.mesh.{}.{hook}", "update"));
        assert!(!published.is_empty(), "the {hook} hook ran");
    }
    let recorded = named(&spans, "rdm.node_admin.mesh.update.via-exit-manifest-recorded");
    assert!(recorded.iter().any(|sp| attr_u(sp, "receipts") == 3 && attr_u(sp, "roster") == 4), "the mesh primary recorded the exit manifest of its three other members: {recorded:?}");
    let left_recorded = named(&spans, "rdm.node_admin.mesh.update.via-mesh-left-receipt");
    assert!(left_recorded.iter().any(|sp| attr_u(sp, "receipts") == 4 && attr_u(sp, "roster") == 4), "the owner recorded MeshLeft over all four births: {left_recorded:?}");
    let heard = |name: &str| -> Vec<&Value> { named(&spans, &format!("rdm.mesh.membership.update.{name}")).into_iter().filter(|sp| s(&sp["attributes"]["node"]).starts_with("mesh2.")).collect() };
    let leaving = heard("via-mesh-leaving");
    assert!(leaving.iter().any(|sp| attr_u(sp, "members_marked") > 0), "a mesh2 node marked mesh1's births Leaving: {leaving:?}");
    let left = heard("via-mesh-left");
    assert!(left.iter().any(|sp| attr_u(sp, "births_departed") > 0), "a mesh2 node held mesh1's births departed on mesh-left: {left:?}");
    let last_exit = tier2_last.max(terminal_end(&final_name));
    assert!(left.iter().all(|sp| start_ns(sp) >= last_exit.saturating_sub(1)), "mesh-left was heard only after the last exact exit was proven");
    // Gossip delivers asynchronously: the order the spec fixes is publication before the first drain.
    let published_leaving = named(&spans, "rdm.node_admin.mesh.update.via-mesh-leaving").into_iter().map(|sp| start_ns(sp)).min().expect("mesh-leaving was published");
    let first_drain = ["mesh1.rpc.1", "mesh1.rpc.2"].iter().map(|n| drain_start(n)).min().unwrap();
    assert!(published_leaving < first_drain, "mesh-leaving was published ({published_leaving}) before any member drained ({first_drain})");

    // A desired change, not a recovery.
    assert!(
        !named(&spans, "rdm.node_admin.build.create.via-proven-drift").iter().any(|sp| s(&sp["attributes"]["scope"]).contains("mesh1")),
        "no recovery Build names mesh1"
    );
    let created = named(&spans, "rdm.node_admin.node.create.via-build");
    assert!(!created.iter().any(|sp| sp["attributes"]["build_id"] == b.as_str() && s(&sp["attributes"]["node"]).starts_with("mesh1.")), "B recreates nothing in mesh1");
    for n in names(&["mesh3"]) {
        assert!(created.iter().any(|sp| sp["attributes"]["build_id"] == b.as_str() && sp["attributes"]["node"] == n.as_str()), "B created {n}");
    }
}
