//! i143.e4.s8 process E2E: deleting the fabric-primary's own mesh hands the fabric-primary seat to a
//! surviving mesh first, then deletes the mesh. The fabric does not move; its primary does.
//!
//! fabric-primary-handover.md: "R-ST2 transfers fabric leadership before the incumbent mesh leaves.
//! It completes before any mesh teardown. The new fabric-primary then runs the existing mesh-leave
//! workflow, including the final departing primary's stop."
//!
//! mesh-leave.md: the owner outside the mesh runs `shutdown-mesh:<mesh_id>`; the mesh's primary
//! drains and stops every other member; the owner drains and stops the mesh's final primary and
//! records `MeshLeft`.
//!
//! desired {mesh1, mesh2, mesh3} -> {mesh2, mesh3} through one Build, mesh1 holding the fabric seat.
//! A desired change, never a recovery: no proven-drift Build names mesh1 and nothing recreates a
//! mesh1 node (subfeature `fabric-primary-handover`).
//!
//! R-NS1 (gossip.md, mesh-leave section): after mesh.deleted the last admin, still its mesh's
//! primary, keeps publishing its aggregate; from its stop on the fabric-primary is that mesh's
//! publisher of record and publishes the mesh's own Members aggregate on the backbone, publisher
//! itself, `digests` empty, the stopped admin in `in_flight` as NodeStopped. A node of the surviving
//! mesh that does not hold the fabric seat holds the deleted mesh with its last admin, then holds it
//! stopped, then gone after MeshLeft. The seat lands on the lowest NodeId among mesh2's and mesh3's
//! primaries (the election's, never chosen here). Every receiver read is made at BOTH surviving
//! meshes, the fabric-primary's own and the other, so every run covers both placements.

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_the_fabric_primarys_mesh_hands_the_seat_over_first_then_deletes_it() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2"), mesh("mesh3")]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let before = estate.settled(&names(&["mesh1", "mesh2", "mesh3"]), Duration::from_secs(30)).await;
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

    // Control moves to mesh2 before mesh1 goes: its admins advertise it in the topology.
    let mesh2_admin = before
        .iter()
        .find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["status"] == "ready-for-traffic")
        .map(|n| s(&n["admin_api_base"]))
        .expect("a ready mesh2 admin");
    estate.admin = mesh2_admin.clone();

    // {mesh1, mesh2, mesh3} -> {mesh2, mesh3}: one Build.
    let (status, b) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh2"), mesh("mesh3")]})).await;
    assert_eq!(status, 202, "{b}");
    let b = s(&b["build_id"]);
    estate.await_build(&b, Duration::from_secs(240)).await;
    let after = estate.settled(&names(&["mesh2", "mesh3"]), Duration::from_secs(60)).await;

    // Identities: the Fabric, mesh2 and mesh3 keep theirs; mesh1 is gone.
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["id"]), fabric_id, "the Fabric keeps its id");
    let (_, m2_after) = estate.get("/api/meshes/mesh2").await;
    assert_eq!(s(&m2_after["id"]), mesh2_id, "an untouched mesh keeps its id");
    let (_, m3_after) = estate.get("/api/meshes/mesh3").await;
    assert_eq!(s(&m3_after["id"]), mesh3_id, "an untouched mesh keeps its id");
    let (status, gone) = estate.get("/api/meshes/mesh1").await;
    assert_eq!(status, 404, "mesh1 is no longer a mesh of the fabric: {gone}");
    assert!(!after.iter().any(|n| n["mesh"] == "mesh1"), "no mesh1 node remains: {after:#?}");
    for (name, pid) in &mesh1_pids {
        wait_for(&format!("{name}'s runtime is gone"), Duration::from_secs(30), || async { (!alive(*pid)).then_some(()) }).await;
    }

    // Dead is final and its members resolve Gone: an ordinary node of each surviving mesh (the
    // fabric-primary's and the other) answers `gone` from its own live resolver for every departed
    // mesh1 birth, never `unknown`, and nothing dials it.
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
    // The durable Node rows are not deleted: mesh1's admin still holds its own row in its data dir,
    // and the fabric's store root holds the contact row mesh1's primary wrote for every mesh1 birth
    // (R14: `nodes/<mesh>/` on the shared root, written by that mesh's primary).
    assert!(std::path::Path::new(&format!("{mesh1_dir}/nodes/self.json")).exists(), "mesh1.admin.1's own Node row remains in {mesh1_dir}");
    let contacts = estate.fabric_store_root().join("nodes").join("mesh1").join("contacts");
    for (name, id) in mesh1_ids.iter() {
        assert!(contacts.join(format!("{id}.json")).exists(), "the contact row of {name} ({id}) remains in {}", contacts.display());
    }
    // states.md: the mesh's `leaving` is entered by the leave-mesh down op, and "one commit stores
    // the row, applies it, gossips it and answers the op"; fabric-node-lifecycle.md (2.7): the mesh's
    // primary "keeps that mesh's row ... and writes every status". mesh1's primary stored its own
    // mesh row Leaving when it admitted leave-mesh: the keyed row is in mesh1's status store on the
    // fabric's store root (R14: `status/<mesh>/`).
    let mesh1_bare: String = mesh1_id.split_once('_').map(|(_, r)| r).unwrap_or(&mesh1_id).replace('-', "").to_lowercase();
    let mesh1_statuses = estate.fabric_store_root().join("status").join("mesh1").join("meshes");
    let leaving_row = mesh1_statuses.join(format!("{mesh1_bare}-Leaving.json"));
    let row: Value = std::fs::read(&leaving_row).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_else(|| panic!("mesh1's primary stored no Leaving row for its mesh at {}: {:?}", leaving_row.display(), std::fs::read_dir(&mesh1_statuses).map(|d| d.filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>())));
    assert_eq!((s(&row["record"]["mesh_id"]), s(&row["record"]["state"])), (mesh1_bare.clone(), "Leaving".to_string()), "{row}");
    let (_, fabric_now) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric_now["admin_api_base"]);
    // The MeshLeft receipt, read from the live Build projection (the admin API): one Complete
    // receipt under the outer operation, covering every birth of the mesh.
    let (_, build_now) = estate.get(&format!("/api/builds?id={b}")).await;
    let left_receipts: Vec<Value> = build_now["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["step"] == "MeshLeft").collect();
    let other_receipts: Vec<Value> = build_now["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["step"] == "OtherMembersExited").collect();
    estate.stop().await;
    let spans = estate.spans();
    // The executor's reconcile span ends after the run returned its terminal, which can be after the
    // Build was read Complete: it is read from the evidence every process flushed on exit.
    let owner_name = {
        let reconciles = named(&spans, "rdm.node_admin.build.update.via-reconcile");
        let r = reconciles.iter().find(|sp| sp["attributes"]["build_id"] == b.as_str() && sp["attributes"]["outcome"] == "converged" && sp["attributes"]["operations"].as_str().is_some_and(|o| o.contains("shutdown-mesh:"))).cloned().expect("a reconcile converged the shutdown-mesh operation");
        s(&r["attributes"]["executor"])
    };
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
    // Every receiver read below is made at both surviving meshes: whichever the election seated, one
    // is the fabric-primary's mesh and the other is not, so each run covers both placements.
    let survivors = ["mesh2", "mesh3"];
    let heard = |m: &str, name: &str| -> Vec<&Value> { named(&spans, &format!("rdm.mesh.membership.update.{name}")).into_iter().filter(|sp| s(&sp["attributes"]["node"]).starts_with(&format!("{m}."))).collect() };
    for m in survivors {
        let leaving = heard(m, "via-mesh-leaving");
        assert!(!leaving.is_empty(), "a {m} node heard mesh-leaving: {spans_len} spans", spans_len = spans.len());
        // R4: no mesh-level hold at a peer. A node learns mesh1's members are leaving from each member's
        // own Draining and Leaving in what it installs, and routes none of them while they say so.
        let states: Vec<&Value> = heard(m, "via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]).starts_with("mesh1.")).collect();
        let leaving_states: Vec<&Value> = states.iter().copied().filter(|sp| matches!(s(&sp["attributes"]["status"]).as_str(), "Draining" | "Leaving")).collect();
        assert!(!leaving_states.is_empty(), "a {m} node held a mesh1 member's own Draining or Leaving: {states:?}");
        assert!(leaving_states.iter().all(|sp| s(&sp["attributes"]["routable"]) == "false" || sp["attributes"]["routable"] == json!(false)), "a mesh1 member saying Draining or Leaving is not routable at {m}: {leaving_states:?}");
        // The forwarder leg: the mesh's ordinary nodes, which hear no backbone, hold it from what their
        // mesh primary forwards onto the mesh channel.
        assert!(leaving_states.iter().any(|sp| s(&sp["attributes"]["node"]).starts_with(&format!("{m}.rpc.")) && sp["attributes"]["channel"] == "mesh-channel"), "a {m} ordinary node held a mesh1 member's own state from the mesh channel: {leaving_states:?}");
    }
    let routable = |sp: &Value| s(&sp["attributes"]["routable"]) == "true" || sp["attributes"]["routable"] == json!(true);
    // R-NS1 acceptance, stated on receipt: a peer that receives a valid NodeStopped excludes that exact
    // birth from routing; on MeshLeft it resolves the birth Gone whether or not it saw Stopped (the
    // resolver reads above); a late NodeStopped restores nothing (read below). This teardown is
    // immediate, so Running -> Gone is valid at a peer: the stopped phase is observed only in the cell
    // that holds it (`a_stopped_final_primary_is_held_stopped_by_the_other_mesh`). Read here: no node
    // holds the final primary routable once its stop was answered.
    let fabric_primary = s(&fabric_now["fabric_primary"]).to_string();
    let fp_mesh = fabric_primary.split('.').next().unwrap_or_default().to_string();
    let final_stopped_at = order_of(&final_name).iter().find(|(n, _, _)| n == "StopNode").map(|(_, _, e)| *e).expect("the final primary's stop was answered");
    let final_after_stop: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]) == final_name && start_ns(sp) > final_stopped_at).collect();
    assert!(final_after_stop.iter().all(|sp| !routable(sp)), "no node holds the final primary {final_name} routable after its stop: {final_after_stop:?}");
    // R-NS1 (gossip.md, mesh-leave section): "After mesh.deleted the mesh's last node-admin, still its
    // primary, keeps publishing its aggregate on the backbone with itself as the only member ... From the
    // stop on the fabric-primary is that mesh's publisher of record ...: it publishes the dead mesh's own
    // Members aggregate on the backbone, publisher itself, `digests` empty, the stopped admin in
    // `in_flight` as NodeStopped ... Receivers handle it as that mesh's aggregate". Read at both surviving
    // meshes: the fabric-primary's own, and the other, which hears the fabric-primary's mesh channel never.
    let other_mesh = if fp_mesh == "mesh2" { "mesh3" } else { "mesh2" };
    let at_mesh = |sp: &Value, m: &str, kind: &str| s(&sp["attributes"]["node"]).starts_with(&format!("{m}.{kind}."));
    let installed = named(&spans, "rdm.mesh.membership.update.via-snapshot-installed");
    let mesh1_installed = |m: &str, publisher: &str, members: u64| -> Vec<&Value> {
        installed.iter().copied().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["channel"] == "backbone" && at_mesh(sp, m, "admin") && s(&sp["attributes"]["publisher"]).starts_with(&format!("{publisher}@")) && attr_u(sp, "members") == members).collect()
    };
    // 1. While it lives: the last admin keeps publishing mesh1's aggregate, itself the only member once
    // every other member stopped, and the other mesh holds mesh1 from that admin's own aggregate. Which
    // of its last versions a peer hears before the stop's cut is gossip delivery inside a window of tens
    // of milliseconds at this cadence, so the hold is read from any version it published.
    // mesh.deleted is its mesh-leave hook; the fabric-primary's drain that follows moves its own status,
    // a material change its round publishes at once.
    let deleted_at = named(&spans, "rdm.node_admin.mesh.update.via-mesh-leave").into_iter().filter(|sp| s(&sp["attributes"]["publisher"]) == final_name).map(|sp| start_ns(sp)).min().expect("the last admin reported mesh.deleted (its mesh-leave hook)");
    let lone_published = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().any(|sp| sp["attributes"]["mesh"] == "mesh1" && s(&sp["attributes"]["node"]) == final_name && attr_u(sp, "members") == 1 && start_ns(sp) > deleted_at);
    assert!(lone_published, "after mesh.deleted the last admin {final_name} kept publishing mesh1's aggregate with itself the only member");
    for m in survivors {
        assert!((1..=4).any(|n| !mesh1_installed(m, &final_name, n).is_empty()), "an admin of {m} held mesh1 from its last admin {final_name}'s own aggregate (fabric-primary's mesh {fp_mesh})");
    }
    // R-NS1 item 1: "After mesh.deleted the mesh row is Dead and its last node-admin, still its primary,
    // keeps publishing ... MeshStatus Dead"; ops-naming mesh.delete: mesh.deleted is "reported by the
    // mesh-primary, still alive, from its members' exit proofs; mesh is dead". The last admin commits
    // its mesh row Dead before its mesh-leave hook, publishes MeshStatus Dead (the same status frame and
    // reinforcement every status change uses) while it lives (before its exit is proven).
    let final_exit = terminal_end(&final_name);
    let dead_row: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-dead-row").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name && s(&sp["attributes"]["mesh_id"]) == mesh1_bare).collect();
    assert!(dead_row.iter().any(|sp| s(&sp["attributes"]["outcome"]) == "applied" && start_ns(sp) <= deleted_at), "the last admin {final_name} committed mesh1's row Dead at mesh.deleted, before its mesh-leave hook: {dead_row:?}");
    let dead_sent: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name && sp["attributes"]["scope"] == "mesh:mesh1" && sp["attributes"]["status"] == "dead").collect();
    assert!(dead_sent.iter().any(|sp| start_ns(sp) < final_exit), "the last admin {final_name} sent MeshStatus Dead for mesh1 while it lived: {dead_sent:?}");
    // A peer's hold of mesh1 Dead is not read here: in this immediate teardown the last admin is cut
    // tens of milliseconds after it sends, and no gossip delivery bound exists for that window (R-NS1
    // acceptance, R-NS1b Q1 = A). It is read in the held-stop cell, where the last admin lives on.
    // R-NS1 item 2 (gossip.md mesh-leave section): "Its last aggregate before a stop carries itself
    // Leaving." Its drain is answered, then before the stop's cut (node-stop.md step 3) the last admin
    // publishes mesh1's aggregate once more, itself the only member and Leaving; after the cut it
    // publishes nothing (step 4, R-ON1).
    let final_cut = named(&spans, "rdm.node_admin.node.connections.delete.via-stop-cut").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name).map(|sp| start_ns(sp)).min().expect("the final primary's connections were cut by its stop");
    let final_drain = drain_start(&final_name);
    let leaving_aggregate: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-leaving-aggregate").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name).collect();
    assert!(
        leaving_aggregate.iter().any(|sp| sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["status"] == "Leaving" && attr_u(sp, "members") == 1 && (sp["attributes"]["published"] == json!(true) || sp["attributes"]["published"] == "true") && start_ns(sp) > final_drain && end_ns(sp) <= final_cut),
        "the last admin {final_name} published mesh1's aggregate carrying itself Leaving after its drain and before its stop's cut: {leaving_aggregate:?}"
    );
    // 2. Stopped: from the stop on the fabric-primary is mesh1's publisher of record, on the ordinary
    // anti-entropy cadence (R-NS1 item 2). Here MeshLeft follows the stop within tens of milliseconds, so
    // neither its acting aggregate nor a peer's stopped hold is owed before MeshLeft (R-NS1 acceptance:
    // Running -> Gone is valid for an immediate teardown); both are read in the cell that holds the
    // stopped phase.
    // 3. Gone after delete, and 4. nothing resurrects it: once a node of a surviving mesh applied
    // mesh-left, no mesh1 birth is held by it again (a late frame is refused by the departed fence, by name).
    let mesh_left_at: Vec<(String, u64)> = named(&spans, "rdm.mesh.membership.update.via-mesh-left").into_iter().filter(|sp| survivors.iter().any(|m| s(&sp["attributes"]["node"]).starts_with(&format!("{m}."))) && attr_u(sp, "births_departed") > 0).map(|sp| (s(&sp["attributes"]["node"]), start_ns(sp))).collect();
    for m in survivors {
        assert!(mesh_left_at.iter().any(|(n, _)| n.starts_with(&format!("{m}.admin."))) && mesh_left_at.iter().any(|(n, _)| n.starts_with(&format!("{m}.rpc."))), "an admin and an ordinary node of {m} departed mesh1's births on mesh-left: {mesh_left_at:?}");
        // MeshLeft keeps nothing of mesh1 (gossip.md: the mesh left), its held MeshStatus included (Dead,
        // when it arrived): it is dropped there and replayed to no joiner after.
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
    // A stopped birth gossips nothing (node-stop.md step 4): once its connections are cut, the parked
    // final primary announces no Leaving on any channel when its process is ended.
    let cut_at = named(&spans, "rdm.node_admin.node.connections.delete.via-stop-cut").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name).map(|sp| start_ns(sp)).min().expect("the final primary's connections were cut by its stop");
    let after_cut: Vec<String> = named(&spans, "rdm.mesh.node.update.via-leave-announcement").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name && start_ns(sp) > cut_at).map(|sp| format!("{}:{}", s(&sp["attributes"]["channel"]), s(&sp["attributes"]["outcome"]))).collect();
    assert!(after_cut.is_empty(), "the parked final primary {final_name} announced nothing after its stop's cut: {after_cut:?}");
    // gossip.md, node-stopped lifecycle overlay: mesh1's primary publishes NodeStopped from each member's
    // matching stopped reply, for every member it stops. The overlay is a material change (gossip.md
    // "an in_flight overlay added or cleared" bumps topology_version), and the Members row publishes
    // "at once on a material change": the primary's round is woken by its own projection's move
    // (via-material-change, proven per event in rafka-mesh-transport round_wake; another wake of the same
    // round may win the race) and publishes a new mesh1 version carrying the overlay for EVERY member it
    // stops, the other node-admin included.
    let stopped_members: Vec<String> = ["mesh1.rpc.1".to_string(), "mesh1.rpc.2".to_string()].into_iter().chain(tier2.iter().map(|n| n.to_string())).collect();
    let published_stopped = named(&spans, "rdm.node_admin.node.update.via-node-stopped");
    let stopped_at = |m: &str| -> u64 {
        published_stopped.iter().find(|sp| s(&sp["attributes"]["node"]) == m && s(&sp["attributes"]["operation"]) == format!("stop-node:{m}")).map(|sp| start_ns(sp)).unwrap_or_else(|| panic!("mesh1's primary published NodeStopped for {m}: {published_stopped:?}"))
    };
    let mesh1_publications: Vec<u64> = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1").map(|sp| start_ns(sp)).collect();
    let mesh1_wakes: Vec<u64> = named(&spans, "rdm.mesh.backbone.update.via-material-change").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1").map(|sp| start_ns(sp)).collect();
    assert!(!mesh1_wakes.is_empty(), "mesh1's primary's round was woken by its own projection's move");
    for m in &stopped_members {
        let at = stopped_at(m);
        assert!(mesh1_publications.iter().any(|p| *p >= at), "mesh1's primary published a new version after {m}'s NodeStopped at {at}: publications {mesh1_publications:?}");
        // On receipt: once its NodeStopped was published no node holds it routable; whether a peer saw
        // the stopped phase before MeshLeft is not ordered (R-NS1 acceptance), so it is not read here.
        let after: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]) == *m && start_ns(sp) > at).collect();
        assert!(after.iter().all(|sp| !routable(sp)), "no node holds {m} routable after its NodeStopped: {after:?}");
    }
    let last_exit = tier2_last.max(terminal_end(&final_name));
    for m in survivors {
        let left = heard(m, "via-mesh-left");
        assert!(left.iter().any(|sp| attr_u(sp, "births_departed") > 0), "a {m} node held mesh1's births departed on mesh-left: {left:?}");
        assert!(left.iter().all(|sp| start_ns(sp) >= last_exit.saturating_sub(1)), "mesh-left was heard at {m} only after the last exact exit was proven");
    }
    // The Leaving row is the leave-mesh admission's commit: put and applied before the admission is
    // answered, so before the first member drains.
    let leaving_commit: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-leaving-row").into_iter().filter(|sp| s(&sp["attributes"]["mesh_id"]) == mesh1_bare).collect();
    assert!(leaving_commit.iter().any(|sp| s(&sp["attributes"]["node"]) == final_name && s(&sp["attributes"]["outcome"]) == "applied"), "mesh1's primary {final_name} applied its mesh row Leaving at leave-mesh: {leaving_commit:?}");
    let first_member_drain = ["mesh1.rpc.1", "mesh1.rpc.2"].iter().map(|n| drain_start(n)).min().unwrap();
    assert!(leaving_commit.iter().all(|sp| end_ns(sp) < first_member_drain), "the Leaving row was committed before any member drained: {leaving_commit:?}");
    // Gossip delivers asynchronously: the order the spec fixes is publication before the first drain.
    let published_leaving = named(&spans, "rdm.node_admin.mesh.update.via-mesh-leaving").into_iter().map(|sp| start_ns(sp)).min().expect("mesh-leaving was published");
    let first_drain = ["mesh1.rpc.1", "mesh1.rpc.2"].iter().map(|n| drain_start(n)).min().unwrap();
    assert!(published_leaving < first_drain, "mesh-leaving was published ({published_leaving}) before any member drained ({first_drain})");

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
    let first_leaving = named(&spans, "rdm.node_admin.mesh.update.via-mesh-leaving").into_iter().map(|sp| start_ns(sp)).min().expect("mesh-leaving was published");
    assert!(start_ns(&confirmed) < first_leaving, "the confirmation precedes mesh-leaving, LeaveMesh and every drain");
    assert!(first_leaving < drain_start("mesh1.rpc.1").min(drain_start("mesh1.rpc.2")));
    assert!(named(&spans, "rdm.node_admin.fabric.reject.via-handover").is_empty(), "no handover call was refused");
    assert!(named(&spans, "rdm.node_admin.mesh.update.via-seat-inside-leaving-mesh").is_empty());
    assert_eq!(s(&committed["attributes"]["successor"]).split(':').next(), Some(s(&fabric_now["fabric_primary"]).split('.').next().unwrap()), "the committed successor's mesh holds the seat afterwards");

    // A desired change, not a recovery.
    assert!(
        !named(&spans, "rdm.node_admin.build.create.via-proven-drift").iter().any(|sp| s(&sp["attributes"]["scope"]).contains("mesh1")),
        "no recovery Build names mesh1"
    );
    let created = named(&spans, "rdm.node_admin.node.create.via-build");
    assert!(!created.iter().any(|sp| sp["attributes"]["build_id"] == b.as_str() && s(&sp["attributes"]["node"]).starts_with("mesh1.")), "B recreates nothing in mesh1");

    // rafka-time: the fabric seat moved from the retired mesh1 to the lowest NodeId among the
    // remaining mesh primaries (R-A2), mesh2's or mesh3's whichever drew it. The other remaining
    // mesh primary, which does not hold the seat, pulled once from the new fabric-primary and
    // adopted its time under the mesh-primary rule.
    let holder = s(&fabric_now["fabric_primary"]);
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
    // birth's own endpoint key, QUIC address and application contact; the holder those contacts
    // name is the one the other surviving mesh primary reached and adopted from above.
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
