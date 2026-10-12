//! i143.e4.s8, i143.e12.s49 process E2E: deleting the fabric-primary's own mesh hands the
//! fabric-primary seat to a surviving mesh first, then deletes the mesh. The fabric does not move;
//! its primary does. mesh.delete ends at mesh.deleted with the mesh-primary alive; its end is a
//! separate node.delete, with the fabric-primary as executor.
//!
//! mesh-delete.md, steps 5-7: "`mesh.deleted`: when every member has its exact exit proof, the
//! mesh-primary, still alive, writes the mesh row deleted and reports mesh.deleted to the
//! fabric-primary ... After mesh.deleted the mesh-primary keeps publishing its aggregate, itself the
//! only member and the mesh deleted ... A separate node.delete deletes the mesh-primary. The
//! fabric-primary is its executor, proves its exit and, as acting primary of a mesh that has no
//! primary, writes its row deleted; from that admin's stop on it publishes the deleted mesh's
//! aggregate." R-MD1 items 2-3: mesh-left requires complete exact-birth terminal coverage, and
//! "Peers retain the Dead mesh and its final birth while that birth lives": the fabric-primary
//! publishes MeshLeft only once the final birth's node.delete has its exact exit proof.
//!
//! desired {mesh1, mesh2, mesh3} -> {mesh2, mesh3} through one Build, mesh1 holding the fabric seat.
//! A desired change, never a recovery: no proven-drift Build names mesh1 and nothing recreates a
//! mesh1 node (subfeature `fabric-primary-handover`). Then `DELETE /api/nodes/<mesh1's last admin>`.
//!
//! R-NS1 (gossip.md, "Mesh delete"): after mesh.deleted the last admin, still its mesh's primary,
//! keeps publishing its aggregate with itself the only member and MeshStatus Dead, and every peer
//! holds the deleted mesh with that one member while it lives; from its stop on the fabric-primary
//! publishes the mesh's own Members aggregate, and its delete moves the birth to `departed`. The
//! seat lands on the lowest NodeId among mesh2's and mesh3's primaries (the election's, never
//! chosen here). Every receiver read is made at BOTH surviving meshes, the fabric-primary's own and
//! the other, so every run covers both placements.

use rafka_test_scenario::estate::{descends_from, named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "fabric-primary-handover".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "deleting_the_fabric_primarys_mesh_hands_the_seat_over_first_then_deletes_it".into(),
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

/// CONTRACT: deleting the mesh that holds the fabric seat hands the seat to a surviving mesh's
/// primary first, then deletes the mesh. The mesh.delete ends at mesh.deleted: the fabric-primary
/// validates the handoff and records its completion receipt, and stops nothing of mesh1's final
/// primary, which stays alive, publishing mesh1's aggregate with itself the only member and
/// MeshStatus Dead, while every peer holds mesh1 with that one member. Its end is a separate
/// node.delete the fabric-primary executes: node.stop, exact exit proof, its row Deleted,
/// NodeDeleted in `departed`, then the fabric-primary's own aggregate for mesh1; only then, with
/// every birth proven exited, mesh-left. No MeshLeft is published between the handoff and the final
/// birth's exit proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_the_fabric_primarys_mesh_hands_the_seat_over_first_then_deletes_it() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2"), mesh("mesh3")]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let before = estate.settled(&names(&["mesh1", "mesh2", "mesh3"]), Duration::from_secs(30)).await;
    // The rows mesh1's primary writes are durable only once the fabric's first state-commit
    // published them (states.md, Rounds: "Before state-commit, RDM holds its rows in the
    // scratchpad"; R14 item 9): the fabric is ready-for-traffic under this Build before mesh1 goes.
    wait_for("the fabric is ready-for-traffic: its state-commit published every node's rows", Duration::from_secs(90), || async {
        (estate.get("/api/fabric").await.1["status"] == "ready-for-traffic").then_some(())
    })
    .await;
    let (_, fabric) = estate.get("/api/fabric").await;
    let fabric_id = s(&fabric["id"]);
    let (_, m1) = estate.get("/api/meshes/mesh1").await;
    let (_, m2) = estate.get("/api/meshes/mesh2").await;
    let (_, m3) = estate.get("/api/meshes/mesh3").await;
    let (mesh1_id, mesh2_id, mesh3_id) = (s(&m1["id"]), s(&m2["id"]), s(&m3["id"]));
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
    let mesh1_bare: String = mesh1_id.split_once('_').map(|(_, r)| r).unwrap_or(&mesh1_id).replace('-', "").to_lowercase();

    // Control moves to mesh2 before mesh1 goes: its admins advertise it in the topology.
    let mesh2_admin = before
        .iter()
        .find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["status"] == "ready-for-traffic")
        .map(|n| s(&n["admin_api_base"]))
        .expect("a ready mesh2 admin");
    estate.admin = mesh2_admin.clone();

    // {mesh1, mesh2, mesh3} -> {mesh2, mesh3}: one Build. It completes at mesh.deleted.
    let (status, b) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh2"), mesh("mesh3")]})).await;
    assert_eq!(status, 202, "{b}");
    let b = s(&b["build_id"]);
    estate.await_build(&b, Duration::from_secs(240)).await;
    let survivors_named = names(&["mesh2", "mesh3"]);
    let after_b = wait_for("the view settles on mesh2 and mesh3 ready, and at most one mesh1 node", Duration::from_secs(60), || async {
        let nodes = estate.nodes().await;
        let ours: BTreeSet<String> = nodes.iter().filter(|n| n["mesh"] != "mesh1").filter_map(|n| n["name"].as_str().map(str::to_string)).collect();
        let ready = nodes.iter().filter(|n| n["mesh"] != "mesh1").all(|n| n["status"] == "ready-for-traffic");
        (ours == survivors_named && ready && nodes.iter().filter(|n| n["mesh"] == "mesh1").count() <= 1).then_some(nodes)
    })
    .await;
    // mesh.deleted leaves mesh1's final primary alive: the one mesh1 node the view still holds.
    let Some(last) = after_b.iter().find(|n| n["mesh"] == "mesh1").cloned() else {
        estate.stop().await;
        let spans = estate.spans();
        let stopped: Vec<String> = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .filter(|sp| sp["attributes"]["build_id"] == b.as_str() && s(&sp["attributes"]["node"]).starts_with("mesh1.admin.") && sp["attributes"]["step"] == "TerminateRuntime")
            .map(|sp| format!("{} TerminateRuntime trace {}", s(&sp["attributes"]["node"]), s(&sp["trace_id"])))
            .collect();
        let inside: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().filter(|sp| sp["attributes"]["build_id"] == b.as_str()).map(|sp| s(&sp["attributes"]["node"])).collect();
        panic!("mesh.delete ends at mesh.deleted with the mesh-primary alive, but no mesh1 node is left after build {b}: the fabric-primary stopped the final primary inside the mesh.delete ({stopped:?}; shutdown-member workflows under {b}: {inside:?})");
    };
    let final_name = s(&last["name"]);
    assert!(final_name.starts_with("mesh1.admin."), "the mesh1 node left alive is a node-admin, the final primary: {last}");
    let final_pid = mesh1_pids.iter().find(|(n, _)| *n == final_name).map(|(_, p)| *p).unwrap();
    assert!(alive(final_pid), "mesh1's final primary {final_name} still runs after mesh.deleted");
    let (_, fabric_now) = estate.get("/api/fabric").await;
    let fabric_primary = s(&fabric_now["fabric_primary"]);
    estate.admin = s(&fabric_now["admin_api_base"]);

    // Peers hold the deleted mesh with its last admin while it lives (R-NS1 item 1, R-MD1 item 3):
    // an admin of each surviving mesh, read on its own control API, holds mesh1 deleted with that one
    // node. The admin lives on here until its node.delete, so this interval is held, not raced.
    let survivors = ["mesh2", "mesh3"];
    for m in survivors {
        for admin in after_b.iter().filter(|n| n["mesh"] == m && n["kind"] == "node_admin") {
            let base = s(&admin["admin_api_base"]);
            let who = s(&admin["name"]);
            wait_for(&format!("{who} holds mesh1 dead with {final_name} its only node"), Duration::from_secs(30), || async {
                let (status, v) = estate.http_get(&base, "/api/meshes/mesh1").await;
                (status == 200 && v["status"] == "dead" && v["nodes"] == json!([final_name.clone()])).then_some(())
            })
            .await;
        }
    }
    assert!(alive(final_pid), "mesh1's final primary {final_name} still runs while every peer holds it");

    // The completion receipt of mesh.deleted, read from the live Build projection: every birth of mesh1
    // but its final primary, which is not deleted inside its own mesh.delete; no MeshLeft yet.
    let steps_of = |build: &Value, step: &str| -> Vec<Value> { build["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["step"] == step).collect() };
    let (_, build_b) = estate.get(&format!("/api/builds?id={b}")).await;
    let deleted_receipts = steps_of(&build_b, "MeshDeleted");
    assert_eq!(deleted_receipts.len(), 1, "one MeshDeleted completion receipt under {b}: {build_b}");
    assert_eq!(deleted_receipts[0]["operation"], format!("shutdown-mesh:{mesh1_bare}"), "keyed by the mesh's minted id: {}", deleted_receipts[0]);
    assert_eq!(deleted_receipts[0]["output"]["receipts"].as_array().map(Vec::len), Some(3), "a terminal receipt for every birth of mesh1 but its final primary: {}", deleted_receipts[0]);
    assert_eq!(s(&deleted_receipts[0]["output"]["final_primary"]["node"]), final_name, "{}", deleted_receipts[0]);
    assert!(steps_of(&build_b, "MeshLeft").is_empty(), "no MeshLeft while the final primary lives: {build_b}");

    // The final primary's end: an ordinary node.delete, a separate call, the fabric-primary its executor.
    let (status, c) = estate.delete(&format!("/api/nodes/{final_name}")).await;
    assert_eq!(status, 202, "{c}");
    let c = s(&c["build_id"]);
    estate.await_build(&c, Duration::from_secs(120)).await;
    let after = estate.settled(&names(&["mesh2", "mesh3"]), Duration::from_secs(60)).await;

    // Identities: the Fabric, mesh2 and mesh3 keep theirs; mesh1 is gone.
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["id"]), fabric_id, "the Fabric keeps its id");
    let (_, m2_after) = estate.get("/api/meshes/mesh2").await;
    assert_eq!(s(&m2_after["id"]), mesh2_id, "an untouched mesh keeps its id");
    let (_, m3_after) = estate.get("/api/meshes/mesh3").await;
    assert_eq!(s(&m3_after["id"]), mesh3_id, "an untouched mesh keeps its id");
    wait_for("mesh1 is no longer a mesh of the fabric", Duration::from_secs(30), || async { (estate.get("/api/meshes/mesh1").await.0 == 404).then_some(()) }).await;
    assert!(!after.iter().any(|n| n["mesh"] == "mesh1"), "no mesh1 node remains: {after:#?}");
    for (name, pid) in &mesh1_pids {
        wait_for(&format!("{name}'s runtime is gone"), Duration::from_secs(30), || async { (!alive(*pid)).then_some(()) }).await;
    }

    // Deleted is final and its members resolve Gone: an ordinary node of each surviving mesh answers
    // `gone` from its own live resolver for every departed mesh1 birth, never `unknown`.
    for rx in ["mesh2.rpc.1", "mesh3.rpc.1"] {
        for (name, id) in mesh1_ids.iter() {
            let query = format!("exact:{id}");
            let target = format!("path:{rx}");
            wait_for(&format!("{rx}'s resolver answers Gone for departed {name}"), Duration::from_secs(30), || async {
                let r = estate.probe(&["resolve", "--target", &target, "--query", &query]);
                (r["reply"]["resolution"] == "gone").then_some(())
            })
            .await;
        }
    }
    // R-MN1 (mesh-delete.md "Rows", steps 3 and 7): every birth of mesh1 is deleted and no row is
    // removed. Each mesh1 birth's Node row stays on the fabric's store root at its path.name
    // (`nodes/mesh1/rows/<path.name>`, R14), written Deleted under its node id: the members' by
    // mesh1's primary, the final primary's by the fabric-primary in its node.delete. A departed birth
    // never answers, so mesh-left drops the contact of every birth its proof set names.
    assert!(std::path::Path::new(&format!("{mesh1_dir}/nodes/self.json")).exists(), "mesh1.admin.1's own Node row remains in {mesh1_dir}");
    let shared = estate.fabric_store_root().join("nodes").join("mesh1");
    for (name, id) in mesh1_ids.iter() {
        let at = shared.join("rows").join(format!("{name}.json"));
        let row: Value = std::fs::read(&at).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_else(|| panic!("the Node row of {name} ({id}) remains at {}", at.display()));
        assert_eq!((s(&row["record"]["node_id"]), s(&row["record"]["status"])), (id.clone(), "deleted".to_string()), "{name}'s Node row is written Deleted: {row}");
        let contact = shared.join("contacts").join(format!("{id}.json"));
        assert!(!contact.exists(), "mesh-left dropped the contact of departed {name} ({id}): {} remains", contact.display());
    }
    // states.md: the mesh's `deleting` is entered by the leave-mesh down op, and "one commit stores
    // the row, applies it, gossips it and answers the op": mesh1's primary stored its own mesh row
    // Leaving when it admitted leave-mesh, in mesh1's status store on the fabric's store root (R14).
    let mesh1_statuses = estate.fabric_store_root().join("status").join("mesh1").join("meshes");
    let leaving_row = mesh1_statuses.join(format!("{mesh1_bare}-Leaving.json"));
    let row: Value = std::fs::read(&leaving_row).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_else(|| panic!("mesh1's primary stored no Leaving row for its mesh at {}: {:?}", leaving_row.display(), std::fs::read_dir(&mesh1_statuses).map(|d| d.filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>())));
    assert_eq!((s(&row["record"]["mesh_id"]), s(&row["record"]["state"])), (mesh1_bare.clone(), "Leaving".to_string()), "{row}");
    // mesh-left, once the final birth's exit was proven: one Complete MeshLeft receipt under the
    // mesh.delete's operation, covering every birth of the mesh, the final primary included.
    let (_, build_b) = estate.get(&format!("/api/builds?id={b}")).await;
    let left_receipts = steps_of(&build_b, "MeshLeft");
    let other_receipts = steps_of(&build_b, "OtherMembersExited");
    estate.stop().await;
    let spans = estate.spans();
    assert_eq!(other_receipts.len(), 1, "one OtherMembersExited receipt, recorded by mesh1's primary: {build_b}");
    assert_eq!(other_receipts[0]["output"]["receipts"].as_array().map(|r| r.len()), Some(3), "a terminal receipt for every birth of mesh1 but its final primary");
    assert_eq!(left_receipts.len(), 1, "one MeshLeft receipt under {b}: {build_b}");
    assert_eq!(left_receipts[0]["outcome"], json!("complete"), "{}", left_receipts[0]);
    assert_eq!(left_receipts[0]["operation"], format!("shutdown-mesh:{mesh1_bare}"), "keyed by the mesh's minted id");
    assert_eq!(left_receipts[0]["output"]["receipts"].as_array().map(|r| r.len()), Some(4), "a terminal receipt for every birth of mesh1, the final primary included: {}", left_receipts[0]);

    // The mesh.delete Build's executor is the fabric-primary; so is the node.delete's.
    let reconciles = named(&spans, "rdm.node_admin.build.update.via-reconcile");
    let converged = |build: &str, op: &str| -> String {
        let r = reconciles.iter().find(|sp| sp["attributes"]["build_id"] == build && sp["attributes"]["outcome"] == "converged" && sp["attributes"]["operations"].as_str().is_some_and(|o| o.contains(op))).cloned().unwrap_or_else(|| panic!("a reconcile converged {op} under {build}"));
        s(&r["attributes"]["executor"])
    };
    let owner_name = converged(&b, "shutdown-mesh:");
    let delete_executor = converged(&c, &format!("retire-node:{final_name}"));
    assert_eq!(delete_executor, fabric_primary, "the fabric-primary executes the final primary's node.delete (mesh1 has no other admin)");

    // mesh1's members left through the member workflows under B: every member drained, stopped and
    // proven exited in the documented order, and none of the node-removal steps ran.
    let steps = named(&spans, "rdm.node_admin.deployment.update.via-step");
    let order_in = |name: &str, build: &str| -> Vec<(String, u64, u64)> {
        let mut v: Vec<&Value> = steps.iter().copied().filter(|sp| sp["attributes"]["node"] == name && sp["attributes"]["build_id"] == build).collect();
        v.sort_by_key(|sp| sp["start_unix_nano"].as_u64());
        v.iter().map(|sp| (s(&sp["attributes"]["step"]), sp["start_unix_nano"].as_u64().unwrap_or(0), sp["end_unix_nano"].as_u64().unwrap_or(0))).collect()
    };
    let members: Vec<String> = mesh1_pids.iter().map(|(n, _)| n.clone()).filter(|n| *n != final_name).collect();
    for name in &members {
        let order = order_in(name, &b);
        let got: Vec<&str> = order.iter().map(|(n, _, _)| n.as_str()).collect();
        assert_eq!(got, ["DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft", "TerminateRuntime"], "{name}: drain-node, node-drained, stop-node, node-left, then the provider's exit proof; no node-removal step");
    }
    // 1. mesh.deleted ends the mesh.delete: nothing of the final primary is stopped under B.
    assert!(order_in(&final_name, &b).is_empty(), "the mesh.delete runs no step on its final primary {final_name}: {:?}", order_in(&final_name, &b));
    let inside: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().filter(|sp| sp["attributes"]["node"] == final_name.as_str()).collect();
    assert!(inside.is_empty(), "no shutdown-member workflow of the final primary {final_name}: {inside:?}");
    // Tiers: the ordinary nodes' workflows overlap in time; the other node-admin starts only after the
    // last ordinary node's exit is proven.
    let member = |name: &str| -> (u64, u64) {
        let sp = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().find(|sp| sp["attributes"]["node"] == name && sp["attributes"]["operation"].as_str().is_some_and(|o| o.starts_with("shutdown-mesh:"))).cloned().unwrap_or_else(|| panic!("no shutdown-member span for {name}"));
        (start_ns(&sp), end_ns(&sp))
    };
    let terminal_end = |name: &str, build: &str| order_in(name, build).iter().find(|(n, _, _)| n == "TerminateRuntime").map(|(_, _, e)| *e).unwrap_or_else(|| panic!("{name} has a TerminateRuntime step under {build}"));
    let drain_start = |name: &str, build: &str| order_in(name, build).iter().find(|(n, _, _)| n == "DrainNode").map(|(_, st, _)| *st).unwrap_or_else(|| panic!("{name} has a DrainNode step under {build}"));
    let (r1, r2) = (member("mesh1.rpc.1"), member("mesh1.rpc.2"));
    assert!(r1.0 < r2.1 && r2.0 < r1.1, "the ordinary nodes' workflows overlap in time: {r1:?} {r2:?}");
    let tier1_last = terminal_end("mesh1.rpc.1", &b).max(terminal_end("mesh1.rpc.2", &b));
    let tier2: Vec<&String> = members.iter().filter(|n| n.contains(".admin.")).collect();
    assert_eq!(tier2.len(), 1, "one other node-admin of mesh1: {members:?}");
    for n in &tier2 {
        assert!(drain_start(n, &b) > tier1_last, "{n} (a node-admin) drained only after every ordinary node's exit was proven");
    }
    let handoff = named(&spans, "rdm.node_admin.mesh.update.via-handoff-validated").into_iter().find(|sp| sp["attributes"]["operation"].as_str().is_some_and(|o| o.starts_with("shutdown-mesh:"))).cloned().expect("the handoff was validated");
    assert_eq!(handoff["attributes"]["outcome"], "applied", "the owner validated the mesh-leave handoff: {handoff}");
    // The completion follows the validated handoff, at the fabric-primary that owns the mesh.delete.
    let completion = named(&spans, "rdm.node_admin.mesh.update.via-mesh-deleted-receipt").into_iter().find(|sp| s(&sp["attributes"]["mesh_id"]) == mesh1_bare).cloned().unwrap_or_else(|| panic!("the fabric-primary recorded mesh.deleted's completion receipt for mesh1"));
    assert!(start_ns(&completion) >= end_ns(&handoff), "the completion receipt follows the validated handoff: {completion}");
    assert_eq!((attr_u(&completion, "receipts"), attr_u(&completion, "roster"), s(&completion["attributes"]["final_primary"])), (3, 4, final_name.clone()), "{completion}");
    assert_eq!(s(&completion["attributes"]["node"]), owner_name, "the mesh.delete's owner records its completion: {completion}");
    eprintln!("bullet 1: mesh.deleted completion trace {} by {owner_name}, handoff trace {}", s(&completion["trace_id"]), s(&handoff["trace_id"]));

    // The gossip hooks: mesh-leaving by the owner, mesh-leave (mesh.deleted) by the still-running primary.
    for hook in ["via-mesh-leaving", "via-mesh-leave", "via-mesh-left"] {
        assert!(!named(&spans, &format!("rdm.node_admin.mesh.update.{hook}")).is_empty(), "the {hook} hook ran");
    }
    let recorded = named(&spans, "rdm.node_admin.mesh.update.via-exit-manifest-recorded");
    assert!(recorded.iter().any(|sp| attr_u(sp, "receipts") == 3 && attr_u(sp, "roster") == 4), "the mesh primary recorded the exit manifest of its three other members: {recorded:?}");
    let heard = |m: &str, name: &str| -> Vec<&Value> { named(&spans, &format!("rdm.mesh.membership.update.{name}")).into_iter().filter(|sp| s(&sp["attributes"]["node"]).starts_with(&format!("{m}."))).collect() };
    for m in survivors {
        let leaving = heard(m, "via-mesh-leaving");
        assert!(!leaving.is_empty(), "a {m} node heard mesh-leaving: {spans_len} spans", spans_len = spans.len());
        // R4: no mesh-level hold at a peer. A node learns mesh1's members are leaving from each
        // member's own Draining and Leaving in what it installs, and routes none of them meanwhile.
        let states: Vec<&Value> = heard(m, "via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]).starts_with("mesh1.")).collect();
        let leaving_states: Vec<&Value> = states.iter().copied().filter(|sp| matches!(s(&sp["attributes"]["status"]).as_str(), "Draining" | "Leaving")).collect();
        assert!(!leaving_states.is_empty(), "a {m} node held a mesh1 member's own Draining or Leaving: {states:?}");
        assert!(leaving_states.iter().all(|sp| s(&sp["attributes"]["routable"]) == "false" || sp["attributes"]["routable"] == json!(false)), "a mesh1 member saying Draining or Leaving is not routable at {m}: {leaving_states:?}");
        assert!(leaving_states.iter().any(|sp| s(&sp["attributes"]["node"]).starts_with(&format!("{m}.rpc.")) && sp["attributes"]["channel"] == "mesh-channel"), "a {m} ordinary node held a mesh1 member's own state from the mesh channel: {leaving_states:?}");
    }
    let routable = |sp: &Value| s(&sp["attributes"]["routable"]) == "true" || sp["attributes"]["routable"] == json!(true);
    let fp_mesh = fabric_primary.split('.').next().unwrap_or_default().to_string();
    let other_mesh = if fp_mesh == "mesh2" { "mesh3" } else { "mesh2" };
    let at_mesh = |sp: &Value, m: &str, kind: &str| s(&sp["attributes"]["node"]).starts_with(&format!("{m}.{kind}."));
    let installed = named(&spans, "rdm.mesh.membership.update.via-snapshot-installed");

    // 1. While it lives: after mesh.deleted (its mesh-leave hook) the last admin keeps publishing
    // mesh1's aggregate with itself the only member and MeshStatus Dead, and an admin of each surviving
    // mesh installs mesh1 from that aggregate with that one member.
    let deleted_at = named(&spans, "rdm.node_admin.mesh.update.via-mesh-leave").into_iter().filter(|sp| s(&sp["attributes"]["publisher"]) == final_name).map(|sp| start_ns(sp)).min().expect("the last admin reported mesh.deleted (its mesh-leave hook)");
    let final_exit = terminal_end(&final_name, &c);
    let lone: Vec<&Value> = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && s(&sp["attributes"]["node"]) == final_name && attr_u(sp, "members") == 1 && start_ns(sp) > deleted_at).collect();
    assert!(!lone.is_empty(), "after mesh.deleted the last admin {final_name} kept publishing mesh1's aggregate with itself the only member");
    for m in survivors {
        let held: Vec<&Value> = installed.iter().copied().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["channel"] == "backbone" && at_mesh(sp, m, "admin") && s(&sp["attributes"]["publisher"]).starts_with(&format!("{final_name}@")) && attr_u(sp, "members") == 1 && start_ns(sp) > deleted_at && start_ns(sp) < final_exit).collect();
        assert!(!held.is_empty(), "an admin of {m} held mesh1 from its last admin {final_name}'s own aggregate, itself the only member, while it lived (fabric-primary's mesh {fp_mesh})");
        eprintln!("bullet 1: {m} held mesh1 with {final_name} alone: trace {} at {}", s(&held[0]["trace_id"]), s(&held[0]["attributes"]["node"]));
    }
    let dead_row: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-dead-row").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name && s(&sp["attributes"]["mesh_id"]) == mesh1_bare).collect();
    assert!(dead_row.iter().any(|sp| s(&sp["attributes"]["outcome"]) == "applied" && start_ns(sp) <= deleted_at), "the last admin {final_name} committed mesh1's row Dead at mesh.deleted, before its mesh-leave hook: {dead_row:?}");
    let dead_sent: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name && sp["attributes"]["scope"] == "mesh:mesh1" && sp["attributes"]["status"] == "dead").collect();
    assert!(dead_sent.iter().any(|sp| start_ns(sp) < final_exit), "the last admin {final_name} sent MeshStatus Dead for mesh1 while it lived: {dead_sent:?}");
    eprintln!("bullet 1: {final_name} lone aggregate trace {}, MeshStatus Dead trace {}", s(&lone[0]["trace_id"]), dead_sent.first().map(|sp| s(&sp["trace_id"])).unwrap_or_default());
    // No MeshLeft between the handoff and the final birth's exit proof (R-MD1 item 2), at the
    // fabric-primary or at any receiver.
    let early_left: Vec<String> = named(&spans, "rdm.node_admin.mesh.update.via-mesh-left")
        .into_iter()
        .chain(named(&spans, "rdm.mesh.membership.update.via-mesh-left"))
        .filter(|sp| start_ns(sp) < final_exit)
        .map(|sp| format!("{} at {} ({})", s(&sp["name"]), s(&sp["attributes"]["node"]) + &s(&sp["attributes"]["publisher"]), start_ns(sp)))
        .collect();
    assert!(early_left.is_empty(), "no MeshLeft before the final birth's exit proof at {final_exit}: {early_left:?}");

    // 2. The final primary's node.delete under C, executed by the fabric-primary: its whole node.stop
    // (the seats it holds stay with it, no admin of its mesh is left to take them), the provider's
    // exact exit proof, its row Deleted, NodeDeleted, its removal.
    let order = order_in(&final_name, &c);
    let got: Vec<&str> = order.iter().map(|(n, _, _)| n.as_str()).collect();
    assert_eq!(got, ["NodeDeleting", "SeatTransfer", "DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft", "TerminateRuntime", "NodeDeleted", "RemoveTopologyMembership", "Complete"], "{final_name}: the ordinary node.delete, node.stop first, then the exit proof");
    let c_steps: Vec<&Value> = steps.iter().copied().filter(|sp| sp["attributes"]["node"] == final_name.as_str() && sp["attributes"]["build_id"] == c.as_str()).collect();
    assert!(c_steps.iter().all(|sp| sp["attributes"]["outcome"] == "complete"), "{final_name}: every step of its node.delete completed: {c_steps:?}");
    let row_deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| s(&sp["attributes"]["subject"]) == final_name && s(&sp["attributes"]["node"]) == fabric_primary).collect();
    assert!(row_deleted.iter().any(|sp| sp["attributes"]["outcome"] == "row-deleted" && start_ns(sp) >= final_exit), "the fabric-primary {fabric_primary} wrote {final_name}'s row Deleted after its exit proof: {row_deleted:?}");
    let node_deleted = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().find(|sp| s(&sp["attributes"]["node"]) == final_name && s(&sp["attributes"]["build_id"]) == c).cloned().unwrap_or_else(|| panic!("NodeDeleted was published for {final_name} under {c}"));
    assert!(start_ns(&node_deleted) >= final_exit, "NodeDeleted follows the exit proof: {node_deleted}");
    // Then the fabric-primary's own aggregate for mesh1: publisher itself, the birth in `departed`.
    let acting: Vec<&Value> = named(&spans, "rdm.mesh.backbone.update.via-acting-primary").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && s(&sp["attributes"]["node"]) == fabric_primary && attr_u(sp, "departed") >= 1 && start_ns(sp) >= start_ns(&node_deleted)).collect();
    assert!(!acting.is_empty(), "after NodeDeleted the fabric-primary {fabric_primary} published mesh1's own aggregate with the deleted birth in departed");
    eprintln!("bullet 2: node.delete of {final_name} by {fabric_primary}: TerminateRuntime then NodeDeleted trace {}, acting aggregate trace {} (departed {})", s(&node_deleted["trace_id"]), s(&acting[0]["trace_id"]), attr_u(acting[0], "departed"));
    // mesh-left once every birth is proven exited: the receipt over all four births, then the hook,
    // at the fabric-primary, after the final birth's exit proof.
    let left_recorded: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-mesh-left-receipt").into_iter().filter(|sp| attr_u(sp, "receipts") == 4 && attr_u(sp, "roster") == 4).collect();
    assert!(left_recorded.iter().any(|sp| start_ns(sp) >= final_exit), "the fabric-primary recorded MeshLeft over all four births after the final birth's exit proof: {left_recorded:?}");
    let left_hook = named(&spans, "rdm.node_admin.mesh.update.via-mesh-left").into_iter().find(|sp| s(&sp["attributes"]["mesh_id"]) == mesh1_bare).cloned().expect("mesh-left was published for mesh1");
    assert_eq!(s(&left_hook["attributes"]["publisher"]), fabric_primary, "{left_hook}");
    eprintln!("bullet 2: mesh-left trace {} at {}", s(&left_hook["trace_id"]), start_ns(&left_hook));
    // mesh-left drops the contacts of the births its proof set names, the final primary included.
    let dropped = named(&spans, "rdm.node_admin.node.remove.via-mesh-left-contact")
        .into_iter()
        .find(|sp| sp["attributes"]["operation"] == format!("shutdown-mesh:{mesh1_bare}").as_str() && sp["attributes"]["error"].is_null())
        .cloned()
        .unwrap_or_else(|| panic!("mesh-left dropped mesh1's contacts under shutdown-mesh:{mesh1_bare}"));
    for (name, id) in mesh1_ids.iter() {
        assert!(s(&dropped["attributes"]["dropped"]).split(", ").any(|d| d == format!("{name} {id}")), "mesh-left dropped the contact of {name} ({id}): {dropped}");
    }
    // R-NS1 item 2: "Its last aggregate before a stop carries itself Leaving": after its drain and
    // before its stop's cut, the last admin publishes mesh1's aggregate once more, itself the only
    // member and Leaving; after the cut it publishes nothing (node-stop.md step 4, R-ON1).
    let final_cut = named(&spans, "rdm.node_admin.node.connections.delete.via-stop-cut").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name).map(|sp| start_ns(sp)).min().expect("the final primary's connections were cut by its stop");
    let final_drain = drain_start(&final_name, &c);
    let leaving_aggregate: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-leaving-aggregate").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name).collect();
    assert!(
        leaving_aggregate.iter().any(|sp| sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["status"] == "Leaving" && attr_u(sp, "members") == 1 && (sp["attributes"]["published"] == json!(true) || sp["attributes"]["published"] == "true") && start_ns(sp) > final_drain && end_ns(sp) <= final_cut),
        "the last admin {final_name} published mesh1's aggregate carrying itself Leaving after its drain and before its stop's cut: {leaving_aggregate:?}"
    );
    let after_cut: Vec<String> = named(&spans, "rdm.mesh.node.update.via-leave-announcement").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name && start_ns(sp) > final_cut).map(|sp| format!("{}:{}", s(&sp["attributes"]["channel"]), s(&sp["attributes"]["outcome"]))).collect();
    assert!(after_cut.is_empty(), "the stopped final primary {final_name} announced nothing after its stop's cut: {after_cut:?}");
    let final_stopped_at = order.iter().find(|(n, _, _)| n == "StopNode").map(|(_, _, e)| *e).expect("the final primary's stop was answered");
    let final_after_stop: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]) == final_name && start_ns(sp) > final_stopped_at).collect();
    assert!(final_after_stop.iter().all(|sp| !routable(sp)), "no node holds the final primary {final_name} routable after its stop: {final_after_stop:?}");

    // 3. Gone after mesh-left, and nothing resurrects it: once a node of a surviving mesh applied
    // mesh-left, no mesh1 birth is held by it again.
    let mesh_left_at: Vec<(String, u64)> = named(&spans, "rdm.mesh.membership.update.via-mesh-left").into_iter().filter(|sp| survivors.iter().any(|m| s(&sp["attributes"]["node"]).starts_with(&format!("{m}."))) && attr_u(sp, "births_departed") > 0).map(|sp| (s(&sp["attributes"]["node"]), start_ns(sp))).collect();
    for m in survivors {
        assert!(mesh_left_at.iter().any(|(n, _)| n.starts_with(&format!("{m}.admin."))) && mesh_left_at.iter().any(|(n, _)| n.starts_with(&format!("{m}.rpc."))), "an admin and an ordinary node of {m} departed mesh1's births on mesh-left: {mesh_left_at:?}");
        let dropped: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-mesh-left").into_iter().filter(|sp| s(&sp["attributes"]["node"]).starts_with(&format!("{m}.admin.")) && attr_u(sp, "births_departed") > 0).collect();
        assert!(dropped.iter().any(|sp| !s(&sp["attributes"]["status_dropped"]).is_empty()), "an admin of {m} dropped mesh1's held status on mesh-left: {dropped:?}");
    }
    let resurrected: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]).starts_with("mesh1.") && mesh_left_at.iter().any(|(n, at)| s(&sp["attributes"]["node"]) == *n && start_ns(sp) > *at)).collect();
    assert!(resurrected.is_empty(), "a mesh1 birth was held again after mesh-left: {resurrected:?}");
    let after_left = |sp: &Value| mesh_left_at.iter().any(|(n, at)| (s(&sp["attributes"]["node"]) == *n || s(&sp["attributes"]["holder"]) == *n) && start_ns(sp) > *at);
    let reinstalled: Vec<&Value> = installed.iter().copied().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && after_left(sp)).collect();
    assert!(reinstalled.is_empty(), "a node of a surviving mesh installed a mesh1 aggregate after mesh-left: {reinstalled:?}");
    let late_refused: Vec<String> = ["rdm.mesh.membership.reject.via-departed-birth", "rdm.mesh.membership.reject.via-mesh-not-in-build"]
        .into_iter()
        .flat_map(|n| named(&spans, n))
        .filter(|sp| after_left(sp) && (sp["attributes"]["mesh"] == "mesh1" || s(&sp["attributes"]["node"]).starts_with("mesh1.")))
        .map(|sp| format!("{}:{}", s(&sp["name"]), s(&sp["attributes"]["holder"]) + &s(&sp["attributes"]["node"])))
        .collect();
    eprintln!("late mesh1 frames refused by name at the surviving meshes after their mesh-left (fabric-primary's mesh {fp_mesh}, other {other_mesh}): {late_refused:?}");
    for m in survivors {
        let left = heard(m, "via-mesh-left");
        assert!(left.iter().all(|sp| start_ns(sp) >= final_exit.saturating_sub(1)), "mesh-left was heard at {m} only after the last exact exit was proven");
    }

    // gossip.md, node-stopped lifecycle overlay: mesh1's primary publishes NodeStopped from each
    // member's matching stopped reply, for every member it stops, and a new mesh1 version carries it.
    let published_stopped = named(&spans, "rdm.node_admin.node.update.via-node-stopped");
    let stopped_at = |m: &str| -> u64 {
        published_stopped.iter().find(|sp| s(&sp["attributes"]["node"]) == m && s(&sp["attributes"]["operation"]) == format!("stop-node:{m}")).map(|sp| start_ns(sp)).unwrap_or_else(|| panic!("mesh1's primary published NodeStopped for {m}: {published_stopped:?}"))
    };
    let mesh1_publications: Vec<u64> = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1").map(|sp| start_ns(sp)).collect();
    let mesh1_wakes: Vec<u64> = named(&spans, "rdm.mesh.backbone.update.via-material-change").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1").map(|sp| start_ns(sp)).collect();
    assert!(!mesh1_wakes.is_empty(), "mesh1's primary's round was woken by its own projection's move");
    for m in &members {
        let at = stopped_at(m);
        assert!(mesh1_publications.iter().any(|p| *p >= at), "mesh1's primary published a new version after {m}'s NodeStopped at {at}: publications {mesh1_publications:?}");
        let after: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]) == *m && start_ns(sp) > at).collect();
        assert!(after.iter().all(|sp| !routable(sp)), "no node holds {m} routable after its NodeStopped: {after:?}");
    }
    // The Leaving row is the leave-mesh admission's commit, before the first member drains; the
    // order the spec fixes is mesh-leaving's publication before the first drain.
    let leaving_commit: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-leaving-row").into_iter().filter(|sp| s(&sp["attributes"]["mesh_id"]) == mesh1_bare).collect();
    assert!(leaving_commit.iter().any(|sp| s(&sp["attributes"]["node"]) == final_name && s(&sp["attributes"]["outcome"]) == "applied"), "mesh1's primary {final_name} applied its mesh row Leaving at leave-mesh: {leaving_commit:?}");
    let first_member_drain = ["mesh1.rpc.1", "mesh1.rpc.2"].iter().map(|n| drain_start(n, &b)).min().unwrap();
    assert!(leaving_commit.iter().all(|sp| end_ns(sp) < first_member_drain), "the Leaving row was committed before any member drained: {leaving_commit:?}");
    let published_leaving = named(&spans, "rdm.node_admin.mesh.update.via-mesh-leaving").into_iter().map(|sp| start_ns(sp)).min().expect("mesh-leaving was published");
    assert!(published_leaving < first_member_drain, "mesh-leaving was published ({published_leaving}) before any member drained ({first_member_drain})");

    // R-ST2: mesh1 held the fabric seat, so it was handed to an outside mesh-primary first. In
    // order: the command, the successor's commit at the next epoch, new-fabric-primary, the
    // completion, the incumbent's confirmation; only then leave-mesh, mesh-leaving and the tiers.
    let one = |name: &str| -> Value {
        let v = named(&spans, name);
        assert_eq!(v.len(), 1, "exactly one {name} span: {v:?}");
        v[0].clone()
    };
    let (committed, confirmed) = (one("rdm.node_admin.fabric.update.via-handover-committed"), one("rdm.node_admin.fabric.update.via-handover-confirmed"));
    let announced = named(&spans, "rdm.mesh.seat.update.via-announce-new-fabric-primary");
    assert_eq!(announced.len(), 1, "new-fabric-primary was published once: {announced:?}");
    assert_eq!(attr_u(&committed, "committed_epoch"), attr_u(&committed, "expected_epoch") + 1, "{committed}");
    assert_eq!(attr_u(&confirmed, "committed_epoch"), attr_u(&committed, "committed_epoch"), "the incumbent confirmed the committed epoch");
    assert_eq!(s(&committed["attributes"]["operation"]), format!("fabric-primary-handover:{mesh1_id}"));
    let first_call = named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| attr_u(sp, "op") == 0x21 && start_ns(sp) < start_ns(&committed)).map(|sp| start_ns(sp)).min().expect("TakeFabricPrimary was called before the commit");
    assert!(first_call < start_ns(&committed) && start_ns(&committed) < start_ns(&announced[0]) && start_ns(&announced[0]) < start_ns(&confirmed), "command, commit, new-fabric-primary, confirmation in order");
    let taken_calls = named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| attr_u(sp, "op") == 0x21 && start_ns(sp) > start_ns(&announced[0]) && start_ns(sp) <= start_ns(&confirmed)).count();
    assert!(taken_calls >= 1, "FabricPrimaryTaken was called after new-fabric-primary and before the confirmation");
    assert!(start_ns(&confirmed) < published_leaving, "the confirmation precedes mesh-leaving, LeaveMesh and every drain");
    assert!(named(&spans, "rdm.node_admin.fabric.reject.via-handover").is_empty(), "no handover call was refused");
    assert!(named(&spans, "rdm.node_admin.mesh.update.via-seat-inside-leaving-mesh").is_empty());
    assert_eq!(s(&committed["attributes"]["successor"]).split(':').next(), Some(fabric_primary.split('.').next().unwrap()), "the committed successor's mesh holds the seat afterwards");

    // A desired change, not a recovery.
    assert!(!named(&spans, "rdm.node_admin.build.create.via-proven-drift").iter().any(|sp| s(&sp["attributes"]["scope"]).contains("mesh1")), "no recovery Build names mesh1");
    let created = named(&spans, "rdm.node_admin.node.create.via-build");
    assert!(!created.iter().any(|sp| (sp["attributes"]["build_id"] == b.as_str() || sp["attributes"]["build_id"] == c.as_str()) && s(&sp["attributes"]["node"]).starts_with("mesh1.")), "nothing recreates a mesh1 node");

    // rafka-time: the fabric seat moved from the retired mesh1 to the lowest NodeId among the
    // remaining mesh primaries (R-A2). The other remaining mesh primary, which does not hold the seat,
    // pulled once from the new fabric-primary and adopted its time under the mesh-primary rule.
    let holder = fabric_primary.clone();
    let holder_id = after.iter().find(|n| s(&n["name"]) == holder).map(|n| s(&n["node_id"])).unwrap_or_else(|| panic!("the fabric primary {holder} is in the settled view"));
    assert!(["mesh2.admin.1", "mesh3.admin.1"].contains(&holder.as_str()), "the seat left the retired mesh1 for a remaining mesh primary: {holder}");
    let other = if holder == "mesh2.admin.1" { "mesh3.admin.1" } else { "mesh2.admin.1" };
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let moved: Vec<&Value> = named(&spans, "rdm.mesh.entry.resolve.via-fabric-seat-moved").into_iter().filter(|sp| attr(sp, "node") == other && attr(sp, "to") == holder_id).collect();
    assert_eq!(moved.len(), 1, "{other} pulled once from {holder} when the fabric seat moved to it: {moved:?}");
    assert_eq!(attr(moved[0], "outcome"), "adopted", "{:?}", moved[0]);
    let adopted = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted");
    let child = adopted.iter().find(|a| descends_from(&spans, a, moved[0])).expect("the adoption is a child of the pull");
    assert_eq!(
        (attr(child, "node").as_str(), attr(child, "source").as_str(), attr(child, "via").as_str(), attr(child, "seat").as_str(), attr(child, "served_by").as_str()),
        (other, "pull", "get-topology", "fabric-primary", holder.as_str())
    );

    // gossip.md "New-fabric-primary and contact addresses": every surviving mesh hears
    // new-fabric-primary naming the exact committed holder birth at the committed epoch, with that
    // birth's own endpoint key, QUIC address and application contact.
    let h = after.iter().find(|n| s(&n["name"]) == holder).unwrap();
    let (succ_mesh, succ_rest) = s(&committed["attributes"]["successor"]).split_once(':').map(|(m, r)| (m.to_string(), r.to_string())).unwrap();
    let (succ_id, succ_inc) = succ_rest.split_once(':').map(|(i, c)| (i.to_string(), c.to_string())).unwrap();
    let exact = format!("{succ_mesh}:{succ_id}@{succ_inc} (epoch {})", attr_u(&committed, "committed_epoch"));
    assert_eq!((succ_id.as_str(), succ_inc.as_str()), (s(&h["node_id"]).as_str(), s(&h["incarnation_id"]).as_str()), "the committed successor is the holder's exact birth: {h}");
    let announce = &announced[0];
    assert_eq!(s(&announce["attributes"]["holder"]), exact, "new-fabric-primary names the committed holder birth and epoch: {announce}");
    assert_eq!(s(&announce["attributes"]["endpoint_id"]), s(&h["endpoint_id"]), "the announced endpoint key is the holder's: {announce} {h}");
    for m in ["mesh2", "mesh3"] {
        let heard: Vec<&Value> = named(&spans, "rdm.mesh.seat.update.via-new-fabric-primary").into_iter().filter(|sp| s(&sp["attributes"]["node"]).starts_with(&format!("{m}.")) && s(&sp["attributes"]["node"]) != holder).collect();
        assert!(!heard.is_empty(), "{m}: an admin other than the holder heard new-fabric-primary on the backbone");
        for sp in &heard {
            let a = &sp["attributes"];
            assert_eq!(s(&a["holder"]), exact, "{m}: the frame names the exact holder birth: {sp}");
            assert_ne!(s(&a["outcome"]), "refused-not-newer", "{m}: the committed holder is taken: {sp}");
            assert_eq!(s(&a["via"]), "backbone", "{m}: heard on the backbone: {sp}");
            for k in ["endpoint_id", "transport_addr", "admin_api_base"] {
                assert_eq!(s(&a[k]), s(&announce["attributes"][k]), "{m}: {k} is the holder's own, as announced: {sp}");
            }
        }
    }
}
