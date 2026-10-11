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
//! primaries; mesh2 and mesh3 are both born before the leave, so either placement is the same
//! receiver path and the cell asserts it against whichever mesh does not hold the seat.

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
    // states.md: the mesh's `leaving` is entered by the leave-mesh down op, and "one commit stores
    // the row, applies it, gossips it and answers the op"; fabric-node-lifecycle.md (2.7): the mesh's
    // primary "keeps that mesh's row ... and writes every status". mesh1's primary stored its own
    // mesh row Leaving when it admitted leave-mesh: the keyed row is in its status store.
    let mesh1_bare: String = mesh1_id.split_once('_').map(|(_, r)| r).unwrap_or(&mesh1_id).replace('-', "").to_lowercase();
    let leaving_row = format!("{mesh1_dir}/status/meshes/{mesh1_bare}-Leaving.json");
    let row: Value = std::fs::read(&leaving_row).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_else(|| panic!("mesh1's primary stored no Leaving row for its mesh at {leaving_row}: {:?}", std::fs::read_dir(format!("{mesh1_dir}/status/meshes")).map(|d| d.filter_map(|e| e.ok()).map(|e| e.file_name()).collect::<Vec<_>>())));
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
    let heard = |name: &str| -> Vec<&Value> { named(&spans, &format!("rdm.mesh.membership.update.{name}")).into_iter().filter(|sp| s(&sp["attributes"]["node"]).starts_with("mesh2.")).collect() };
    let leaving = heard("via-mesh-leaving");
    assert!(!leaving.is_empty(), "a mesh2 node heard mesh-leaving: {spans_len} spans", spans_len = spans.len());
    // R4: no mesh-level hold at a peer. A mesh2 node learns mesh1's members are leaving from each
    // member's own Draining and Leaving in what it installs, and routes none of them while they say so.
    let states: Vec<&Value> = heard("via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]).starts_with("mesh1.")).collect();
    let leaving_states: Vec<&Value> = states.iter().copied().filter(|sp| matches!(s(&sp["attributes"]["status"]).as_str(), "Draining" | "Leaving")).collect();
    assert!(!leaving_states.is_empty(), "a mesh2 node held a mesh1 member's own Draining or Leaving: {states:?}");
    assert!(leaving_states.iter().all(|sp| s(&sp["attributes"]["routable"]) == "false" || sp["attributes"]["routable"] == json!(false)), "a mesh1 member saying Draining or Leaving is not routable at mesh2: {leaving_states:?}");
    // The forwarder leg: mesh2's ordinary nodes, which hear no backbone, hold it from what their
    // mesh primary forwards onto the mesh channel.
    assert!(leaving_states.iter().any(|sp| s(&sp["attributes"]["node"]).starts_with("mesh2.rpc.") && sp["attributes"]["channel"] == "mesh-channel"), "a mesh2 ordinary node held a mesh1 member's own state from the mesh channel: {leaving_states:?}");
    let parked = |sp: &Value| sp["attributes"]["parked"] == "true" || sp["attributes"]["parked"] == json!(true);
    // The final primary's Leaving and parked state reach every node from its stopped reply (node-stop.md
    // steps 4-5): the owner, the fabric-primary, publishes NodeStopped for it, and an admin and an ordinary
    // node of the fabric-primary's own mesh hold it Leaving and parked, not routable anywhere.
    let fabric_primary = s(&fabric_now["fabric_primary"]).to_string();
    let fp_mesh = fabric_primary.split('.').next().unwrap_or_default().to_string();
    let final_parked: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]) == final_name && parked(sp)).collect();
    let held_in = |kind: &str| final_parked.iter().any(|sp| s(&sp["attributes"]["node"]).starts_with(&format!("{fp_mesh}.{kind}.")));
    assert!(held_in("admin"), "an admin of the fabric-primary's mesh {fp_mesh} held the final primary {final_name} parked: {final_parked:?}");
    assert!(held_in("rpc"), "an ordinary node of the fabric-primary's mesh {fp_mesh} held the final primary {final_name} parked: {final_parked:?}");
    assert!(final_parked.iter().all(|sp| s(&sp["attributes"]["routable"]) == "false" || sp["attributes"]["routable"] == json!(false)), "the parked final primary is not routable anywhere: {final_parked:?}");
    // R-NS1 (gossip.md, mesh-leave section): "After mesh.deleted the mesh's last node-admin, still its
    // primary, keeps publishing its aggregate on the backbone with itself as the only member ... From the
    // stop on the fabric-primary is that mesh's publisher of record ...: it publishes the dead mesh's own
    // Members aggregate on the backbone, publisher itself, `digests` empty, the stopped admin in
    // `in_flight` as NodeStopped ... Receivers handle it as that mesh's aggregate". Read at the surviving
    // mesh that does not hold the fabric seat, which hears the fabric-primary's own mesh channel never.
    let other_mesh = if fp_mesh == "mesh2" { "mesh3" } else { "mesh2" };
    let at_other = |sp: &Value, kind: &str| s(&sp["attributes"]["node"]).starts_with(&format!("{other_mesh}.{kind}."));
    let installed = named(&spans, "rdm.mesh.membership.update.via-snapshot-installed");
    let mesh1_installed = |publisher: &str, members: u64| -> Vec<&Value> {
        installed.iter().copied().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["channel"] == "backbone" && at_other(sp, "admin") && s(&sp["attributes"]["publisher"]).starts_with(&format!("{publisher}@")) && attr_u(sp, "members") == members).collect()
    };
    // 1. While it lives: the last admin keeps publishing mesh1's aggregate, itself the only member once
    // every other member stopped, and the other mesh holds mesh1 from that admin's own aggregate. Which
    // of its last versions a peer hears before the stop's cut is gossip delivery inside a window of tens
    // of milliseconds at this cadence, so the hold is read from any version it published.
    let lone_published = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().any(|sp| sp["attributes"]["mesh"] == "mesh1" && s(&sp["attributes"]["node"]) == final_name && attr_u(sp, "members") == 1);
    assert!(lone_published, "the last admin {final_name} published mesh1's aggregate with itself the only member");
    assert!((1..=4).any(|m| !mesh1_installed(&final_name, m).is_empty()), "an admin of {other_mesh} held mesh1 from its last admin {final_name}'s own aggregate");
    // 2. Stopped: the fabric-primary publishes mesh1's own aggregate (no digests, the stop in in_flight),
    // and the other mesh holds the last admin parked from it: an admin from the backbone, an ordinary
    // node from its mesh primary (forwarded, or the entry pull it served). The fabric-primary's MeshLeft
    // follows within tens of milliseconds and gossip does not order the two, so a node that hears
    // MeshLeft first refuses the later frame by name (read below) instead.
    let acting: Vec<&Value> = named(&spans, "rdm.mesh.backbone.update.via-acting-primary").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && s(&sp["attributes"]["node"]) == fabric_primary && attr_u(sp, "in_flight") >= 1 && attr_u(sp, "departed") == 0).collect();
    assert!(!acting.is_empty(), "the fabric-primary {fabric_primary} published mesh1's own aggregate as its publisher of record once mesh1 had no primary");
    let from_fp: Vec<&Value> = installed.iter().copied().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && (at_other(sp, "admin") || at_other(sp, "rpc")) && s(&sp["attributes"]["publisher"]).starts_with(&format!("{fabric_primary}@")) && attr_u(sp, "members") == 0).collect();
    assert!(!from_fp.is_empty(), "a node of {other_mesh} installed mesh1's aggregate published by the fabric-primary {fabric_primary}, no digests: {:?}", installed.iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && at_other(sp, "admin")).collect::<Vec<_>>());
    let held_at_other: Vec<&Value> = final_parked.iter().copied().filter(|sp| (at_other(sp, "admin") && sp["attributes"]["channel"] == "backbone") || (at_other(sp, "rpc") && matches!(s(&sp["attributes"]["channel"]).as_str(), "mesh-channel" | "topology-read"))).collect();
    assert!(!held_at_other.is_empty(), "a node of {other_mesh} held the final primary {final_name} parked from mesh1's aggregate the fabric-primary published: {final_parked:?}");
    // 3. Gone after delete, and 4. nothing resurrects it: once a node of the other mesh applied mesh-left,
    // no mesh1 birth is held by it again (a late frame is refused by the departed fence, by name).
    let mesh_left_at: Vec<(String, u64)> = named(&spans, "rdm.mesh.membership.update.via-mesh-left").into_iter().filter(|sp| s(&sp["attributes"]["node"]).starts_with(&format!("{other_mesh}.")) && attr_u(sp, "births_departed") > 0).map(|sp| (s(&sp["attributes"]["node"]), start_ns(sp))).collect();
    assert!(mesh_left_at.iter().any(|(n, _)| n.starts_with(&format!("{other_mesh}.admin."))) && mesh_left_at.iter().any(|(n, _)| n.starts_with(&format!("{other_mesh}.rpc."))), "an admin and an ordinary node of {other_mesh} departed mesh1's births on mesh-left: {mesh_left_at:?}");
    let resurrected: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]).starts_with("mesh1.") && mesh_left_at.iter().any(|(n, at)| s(&sp["attributes"]["node"]) == *n && start_ns(sp) > *at)).collect();
    assert!(resurrected.is_empty(), "a mesh1 birth was held again after mesh-left: {resurrected:?}");
    let after_left = |sp: &Value| mesh_left_at.iter().any(|(n, at)| (s(&sp["attributes"]["node"]) == *n || s(&sp["attributes"]["holder"]) == *n) && start_ns(sp) > *at);
    let reinstalled: Vec<&Value> = installed.iter().copied().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && after_left(sp)).collect();
    assert!(reinstalled.is_empty(), "a node of {other_mesh} installed a mesh1 aggregate after mesh-left: {reinstalled:?}");
    let late_refused: Vec<String> = ["rdm.mesh.membership.reject.via-departed-birth", "rdm.mesh.membership.reject.via-mesh-not-in-build"]
        .into_iter()
        .flat_map(|n| named(&spans, n))
        .filter(|sp| after_left(sp) && (sp["attributes"]["mesh"] == "mesh1" || s(&sp["attributes"]["node"]).starts_with("mesh1.")))
        .map(|sp| format!("{}:{}", s(&sp["name"]), s(&sp["attributes"]["holder"]) + &s(&sp["attributes"]["node"])))
        .collect();
    eprintln!("late mesh1 frames refused by name at {other_mesh} after its mesh-left: {late_refused:?}");
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
    // round may win the race) and publishes a new mesh1 version carrying the overlay, so EVERY member it
    // stops, the other node-admin included, is held Leaving and parked, not routable, at a mesh2 admin
    // (backbone) and a mesh2 ordinary node, from mesh2's primary: forwarded on the mesh channel, or,
    // when the ordinary node finds a version gap in what was forwarded, the entry pull that closes
    // it (gossip.md, the entry pull): a topology read its own mesh primary serves. The pull's span
    // (`rdm.mesh.entry.update.via-top-up`) is emitted once its install is done, so the first one on
    // that node at or after the held state is the read that installed it; it must name mesh2's
    // primary as the server and mesh1 as installed.
    let top_ups = named(&spans, "rdm.mesh.entry.update.via-top-up");
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
        let held: Vec<&Value> = leaving_states.iter().copied().filter(|sp| s(&sp["attributes"]["member"]) == *m && parked(sp)).collect();
        assert!(held.iter().any(|sp| s(&sp["attributes"]["node"]).starts_with("mesh2.admin.") && sp["attributes"]["channel"] == "backbone"), "a mesh2 admin held {m} parked from mesh1's backbone Members.in_flight: {held:?}");
        let from_mesh2_primary = |sp: &&Value| -> bool {
            let node = s(&sp["attributes"]["node"]);
            if !node.starts_with("mesh2.rpc.") {
                return false;
            }
            match s(&sp["attributes"]["channel"]).as_str() {
                "mesh-channel" => true,
                "topology-read" => top_ups.iter().filter(|t| s(&t["attributes"]["node"]) == node && start_ns(t) >= start_ns(sp)).min_by_key(|t| start_ns(t)).is_some_and(|t| {
                    s(&t["attributes"]["served_by"]).starts_with("mesh2.admin.") && s(&t["attributes"]["installed"]).split(',').any(|i| i.starts_with("mesh1@"))
                }),
                _ => false,
            }
        };
        assert!(held.iter().any(from_mesh2_primary), "a mesh2 ordinary node held {m} parked from mesh2's primary (its mesh channel, or the entry pull it served): {held:?}");
    }
    let left = heard("via-mesh-left");
    assert!(left.iter().any(|sp| attr_u(sp, "births_departed") > 0), "a mesh2 node held mesh1's births departed on mesh-left: {left:?}");
    let last_exit = tier2_last.max(terminal_end(&final_name));
    assert!(left.iter().all(|sp| start_ns(sp) >= last_exit.saturating_sub(1)), "mesh-left was heard only after the last exact exit was proven");
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
