//! i143.e8.s6 acceptance (rafka-v2 #2784, hardened 2026-10-07), CHAOS-CONTAINER layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2784-chaos-container`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest, rpc ledger and every process's spans land under it, feature `i143-2784`).
//!
//! The faults are `rafka_test_scenario::container_faults`: real `docker network disconnect` /
//! `connect` of exact container ids, observed through `docker inspect`. The silence is judged by
//! `rafka_test_scenario::wedge` (family `PartitionHeal`).

use rafka_test_scenario::container_faults::{self, Inspected, Silenced};
use rafka_test_scenario::elections::seats_as_expected;
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::wedge::*;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

const CELL: &str = "container_network_partitions_heals_preserves_live_runtimes";
const NODE_CELL: &str = "container_node_unheard_heals_serves_same_birth";

/// What is silenced: every container of the peer mesh without the fabric-primary seat, or one rpc
/// node of it (its mesh-mates stay heard by the rest of the estate).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cut {
    PeerMesh,
    Node,
}

fn owner(cell: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2784".into(),
        subfeature: "container-faults".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: cell.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2784/chaos-container").join(cell),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn unheard(n: &Value) -> bool {
    matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))
}

fn node_in<'a>(nodes: &'a [Value], name: &str) -> Option<&'a Value> {
    nodes.iter().find(|n| n["name"] == name)
}

/// The three legs to one exact node: Direct, ViaPeer through `carrier`, and NoActiveRoute.
fn legs(estate: &Estate, target_id: &str, carrier: &str) -> Value {
    let exact = format!("exact:{target_id}");
    json!({
        "direct": estate.probe(&["get", "--target", &exact, "--key", "iso"]),
        "via_peer": estate.probe(&["get", "--target", &exact, "--via", &format!("path:{carrier}"), "--key", "iso"]),
        "no_active_route": estate.probe(&["get", "--target", &exact, "--no-route", "--key", "iso"]),
    })
}

/// A leg reduced to what must be stable while the silence holds: the typed outcome and the leg.
/// A ViaPeer leg is held to whether it replied: its certainty is the carrier's own edge fact, which
/// the first forward into the cut teaches the carrier (`ReplyDeadline`, then `CarrierEdgeLost`).
fn leg_shape(l: &Value) -> Value {
    if l["route"] == "via-peer" {
        return json!({"replied": l["outcome"] == "Reply", "route": l["route"]});
    }
    json!({"outcome": l["outcome"], "route": l["route"]})
}

fn replied(l: &Value) -> bool {
    l["outcome"] == "Reply"
}

/// Every isolated container as the Docker daemon holds it.
fn inspect_all(silenced: &Silenced) -> BTreeMap<String, Inspected> {
    silenced.members.iter().map(|u| (u.node.clone(), container_faults::inspect(&u.id).unwrap_or_else(|e| panic!("{}: {e}", u.node)))).collect()
}

/// The fixed part of a read taken while the silence holds.
fn silence_marker(inspected: &BTreeMap<String, Inspected>, rules: &BTreeMap<String, String>, nodes: &[Value], legs: &Value) -> String {
    let containers: BTreeMap<&String, Value> = inspected.iter().map(|(n, i)| (n, json!({"id": i.id, "status": i.status, "pid": i.pid, "started_at": i.started_at, "restart_count": i.restart_count, "networks": i.networks}))).collect();
    let view: BTreeMap<&String, Value> = inspected.keys().map(|n| (n, json!({"unheard": node_in(nodes, n).is_some_and(unheard), "incarnation_id": node_in(nodes, n).map(|x| x["incarnation_id"].clone())}))).collect();
    json!({"containers": containers, "rules": rules, "view": view, "direct": leg_shape(&legs["direct"]), "via_peer": leg_shape(&legs["via_peer"]), "no_active_route": leg_shape(&legs["no_active_route"])}).to_string()
}

/// Every admin the view names ready, as `(name, control API base)`.
fn ready_admins(nodes: &[Value]) -> Vec<(String, String)> {
    nodes.iter().filter(|n| n["kind"] == "node_admin" && n["status"] == "ready-for-traffic").filter_map(|n| n["admin_api_base"].as_str().map(|b| (s(&n["name"]), b.to_string()))).collect()
}

async fn try_get(base: &str, path: &str) -> Option<Value> {
    let r = reqwest::Client::new().get(format!("{base}{path}")).timeout(Duration::from_secs(3)).send().await.ok()?;
    if r.status().as_u16() != 200 {
        return None;
    }
    r.json().await.ok()
}

/// The (path, incarnation) set a view holds ready for traffic.
fn births(nodes: &[Value]) -> BTreeSet<(String, String)> {
    nodes.iter().filter(|n| n["status"] == "ready-for-traffic").map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect()
}

/// Healed: every admin the entry view names ready holds the same births as the entry view, every
/// mesh of the fabric ready-for-traffic in its `/api/fabric`, and the same Fabric.build_id.
/// `Err` names what a view still lacks.
async fn converged(estate: &Estate, want: &BTreeSet<(String, String)>, meshes: &[&str], build_id: &str) -> Result<Vec<String>, String> {
    let nodes = estate.nodes().await;
    if &births(&nodes) != want {
        return Err(format!("entry view births differ: missing {:?}, extra {:?}", want.difference(&births(&nodes)).collect::<Vec<_>>(), births(&nodes).difference(want).collect::<Vec<_>>()));
    }
    let mut agreeing = Vec::new();
    for (name, base) in ready_admins(&nodes) {
        let Some(v) = try_get(&base, "/api/nodes").await else { return Err(format!("{name} ({base}) does not answer /api/nodes")) };
        let theirs = v["nodes"].as_array().cloned().unwrap_or_default();
        if &births(&theirs) != want {
            return Err(format!("{name} holds other births: missing {:?}, extra {:?}", want.difference(&births(&theirs)).collect::<Vec<_>>(), births(&theirs).difference(want).collect::<Vec<_>>()));
        }
        let Some(f) = try_get(&base, "/api/fabric").await else { return Err(format!("{name} does not answer /api/fabric")) };
        if f["build_id"].as_str() != Some(build_id) {
            return Err(format!("{name} holds Fabric.build_id {} not {build_id}", f["build_id"]));
        }
        let heard: BTreeSet<String> = f["meshes"].as_array().into_iter().flatten().filter(|m| m["status"] == "ready-for-traffic").map(|m| s(&m["name"])).collect();
        if let Some(m) = meshes.iter().find(|m| !heard.contains(**m)) {
            return Err(format!("{name} does not hold {m} ready-for-traffic: a peer mesh unheard ({heard:?})"));
        }
        agreeing.push(name);
    }
    Ok(agreeing)
}

fn attr<'a>(sp: &'a Value, k: &str) -> &'a str {
    sp["attributes"][k].as_str().unwrap_or_default()
}

fn span_row(sp: &Value, keys: &[&str]) -> Value {
    let mut attrs = serde_json::Map::new();
    for k in keys {
        attrs.insert((*k).into(), sp["attributes"][*k].clone());
    }
    json!({"name": sp["name"], "trace_id": sp["trace_id"], "span_id": sp["span_id"], "parent_span_id": sp["parent_span_id"], "service": sp["service"], "start_unix_nano": start(sp), "attributes": attrs})
}

/// CONTRACT (#2784): on a real Docker estate of two meshes (two node-admins and two rpc nodes
/// each), every container of the peer mesh that does not hold the fabric-primary seat is silenced
/// against the rest of the estate (a packet filter in each container's own network namespace) and
/// later released. While it is silenced: `docker inspect` shows each of those containers still
/// running (same id, same init, no restart, no exit) and attached to the fabric network; the other
/// mesh's view marks every one of them unheard; a Direct call to one of them ends `NotSent`
/// (deadline), a ViaPeer call through a node of the other mesh ends without a reply (first
/// `Indeterminate` while the carrier still holds the edge, then `NotSent` naming the carrier's lost
/// edge, and never `Indeterminate` again once the carrier has named it), a NoActiveRoute call is
/// `NotSent`, and no serve span exists for them; the advertised seats equal what the public
/// candidates compute, the fabric-primary stays put, and the Build gains no attempt and
/// `Fabric.build_id` does not move; the control plane creates, retires and re-births nothing. After
/// the release every silenced node is back as the same birth (same incarnation, same container),
/// every admin's view holds the same births and every mesh ready-for-traffic, and a Direct and a
/// ViaPeer call to the silenced node are served by the same birth with the value stored before the
/// cut (the healthy control, run before the cut as well). What must NOT happen: a container
/// stopped, removed or replaced, a node declared dead and re-born, a terminate, a retire, or an
/// election move because of the silence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn container_network_partitions_heals_preserves_live_runtimes() {
    run_isolation(CELL, Cut::PeerMesh).await;
}

/// CONTRACT (#2784): the same cut on ONE rpc node of the peer mesh without the seat. The node alone
/// is unheard: its mesh-mates, both mesh's admins and the fabric-primary stay ready-for-traffic in
/// the entry view, no admin and no other node is re-born, the silenced container runs throughout
/// (same init, no restart), a Direct, ViaPeer and NoActiveRoute call to it end without a reply with
/// no serve span, and after the release the same birth serves the value stored before the cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn container_node_unheard_heals_serves_same_birth() {
    run_isolation(NODE_CELL, Cut::Node).await;
}

async fn run_isolation(cell: &'static str, cut: Cut) {
    assert_eq!(std::env::var("MESH_SPAWN_TYPE").as_deref(), Ok("container"), "this cell runs only on the container provider (MESH_SPAWN_TYPE=container); a process run never stands in for it");
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let mesh = |m: &str| json!({"name": m, "node_admin": 2, "rpc_node": 2});
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1")]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(&s(&a["build_id"]), Duration::from_secs(120)).await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    let build_id = s(&a["build_id"]);
    estate.await_build(&build_id, Duration::from_secs(120)).await;
    let all: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=2).map(move |i| format!("{m}.rpc.{i}")))).collect();
    let nodes = estate.settled(&all, Duration::from_secs(60)).await;

    // The side that is cut off is the mesh without the fabric-primary seat; control is read through
    // an admin of the other side.
    let fabric_primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("the view names a fabric primary");
    let kept_mesh = fabric_primary.split('.').next().unwrap().to_string();
    let cut_mesh = if kept_mesh == "mesh1" { "mesh2" } else { "mesh1" }.to_string();
    let kept_admin = nodes.iter().find(|n| n["mesh"] == kept_mesh.as_str() && n["kind"] == "node_admin" && n["status"] == "ready-for-traffic").and_then(|n| n["admin_api_base"].as_str()).expect("a kept admin answers").to_string();
    estate.admin = kept_admin;
    let nodes = estate.nodes().await;
    let mesh_members: Vec<String> = nodes.iter().filter(|n| n["mesh"] == cut_mesh.as_str() && n["status"] == "ready-for-traffic").map(|n| s(&n["name"])).collect();
    assert_eq!(mesh_members.len(), 4, "{cut_mesh} holds its four members ready: {mesh_members:?}");
    let isolated_names: Vec<String> = match cut {
        Cut::PeerMesh => mesh_members.clone(),
        Cut::Node => vec![format!("{cut_mesh}.rpc.1")],
    };
    let silenced_subject = if cut == Cut::Node { isolated_names[0].clone() } else { cut_mesh.clone() };
    let target = node_in(&nodes, &format!("{cut_mesh}.rpc.1")).cloned().expect("the isolated mesh's rpc node");
    let (target_name, target_id, target_inc) = (s(&target["name"]), s(&target["node_id"]), s(&target["incarnation_id"]));
    let carrier = format!("{kept_mesh}.rpc.1");
    let want_births = births(&nodes);
    let fabric_before = estate.get("/api/fabric").await.1;
    let fabric_build = s(&fabric_before["build_id"]);
    assert_eq!(fabric_build, build_id, "Fabric.build_id names the creation Build: {fabric_before}");
    let attempt_of = |b: &Value| b["attempt"].as_u64().unwrap_or(0);
    let attempt_before = attempt_of(&estate.get(&format!("/api/builds?id={fabric_build}")).await.1);
    let (seats_before, seats_detail_before) = match seats_as_expected(&nodes) { Ok(()) => (true, String::new()), Err(e) => (false, e) };

    // The healthy control, run first in the same capture: the same three legs to the same node
    // reach it and are served by it; its value is stored for the heal to find.
    let put = estate.probe(&["put", "--target", &format!("exact:{target_id}"), "--key", "iso", "--value", "before-isolation"]);
    assert_eq!(put["outcome"], "Reply", "{put}");
    assert_eq!(put["reply"]["incarnation_id"], target_inc.as_str(), "{put}");
    let control = legs(&estate, &target_id, &carrier);
    assert!(replied(&control["direct"]) && replied(&control["via_peer"]), "the healthy control reaches the node by both routes: {control}");
    assert_eq!(control["via_peer"]["route"], "via-peer", "{control}");
    assert_eq!(control["no_active_route"]["outcome"], "NotSent", "{control}");
    assert_eq!(control["direct"]["reply"]["executing_node"], target_id.as_str(), "{control}");

    // The provider's view of every container before the cut.
    let containers_before: BTreeMap<String, String> = isolated_names.iter().map(|n| (n.clone(), estate.container_of(n).unwrap_or_else(|| panic!("{n}: no running container")))).collect();
    let network = container_faults::network_of(&estate);

    // The cut: every container of the mesh silenced against the rest of the estate, held. Each keeps
    // its interface and stays attached to the fabric network.
    let isolated_admins: Vec<(String, String)> = nodes.iter().filter(|n| n["mesh"] == cut_mesh.as_str() && n["kind"] == "node_admin").filter_map(|n| n["admin_api_base"].as_str().map(|b| (s(&n["name"]), b.to_string()))).collect();
    let silenced = container_faults::silence(&estate, &isolated_names, &fabric_primary).expect("the isolated mesh's containers are silenced");
    let cut_at = now_nanos();
    let rules_at_cut = silenced.rules().expect("the packet filters are listed");
    let primitive = json!({"docker": "run --net container:<id> --cap-add NET_ADMIN iptables-restore", "network": network, "chain": container_faults::SILENCE_CHAIN, "silenced": silenced, "rules": rules_at_cut});
    let mut ev = Evidence::new(Family::PartitionHeal, format!("network:silence-{silenced_subject}"));
    ev.primitive = Some(Primitive { armed: true, ack: primitive.clone() });
    let after_cut = inspect_all(&silenced);
    for (n, i) in &after_cut {
        assert!(i.networks.contains(&network) && i.running, "{n}: the silence keeps the container running and attached to {network}: {i:?}");
    }

    // The membership consequence, seen from the other mesh.
    let wait_for_unheard = format!("{cut_mesh} unheard in the view of {kept_mesh}");
    let silent_view: Vec<Value> = wait_for(&wait_for_unheard, Duration::from_secs(90), || async {
        let nodes = estate.nodes().await;
        isolated_names.iter().all(|n| node_in(&nodes, n).is_some_and(unheard)).then_some(nodes)
    })
    .await;

    // Reads while silent: provider state, the view, and the three legs.
    let mut reads = Vec::new();
    let mut silence = Vec::new();
    for _ in 0..3 {
        let l = legs(&estate, &target_id, &carrier);
        let inspected = inspect_all(&silenced);
        let rules = silenced.rules().expect("the packet filters are listed");
        let nodes_now = estate.nodes().await;
        // The silenced side's own view, read from its admins' control APIs over TCP.
        let mut own_views = serde_json::Map::new();
        for (name, base) in &isolated_admins {
            let v = try_get(base, "/api/nodes").await;
            let seen: Value = match v {
                Some(v) => json!(v["nodes"].as_array().into_iter().flatten().map(|n| (s(&n["name"]), s(&n["status"]))).collect::<BTreeMap<_, _>>()),
                None => json!("unanswered"),
            };
            own_views.insert(name.clone(), seen);
        }
        reads.push(silence_marker(&inspected, &rules, &nodes_now, &l));
        silence.push(json!({"legs": l, "containers": inspected, "rules": rules, "own_views": own_views, "view": isolated_names.iter().map(|n| json!({"node": n, "status": node_in(&nodes_now, n).map(|x| x["status"].clone())})).collect::<Vec<_>>()}));
    }
    let last = inspect_all(&silenced);
    let held_at_last_read = silenced.active().expect("the packet filters are listed") && last.values().all(|i| i.networks.contains(&network));
    ev.fault = Some(FaultAck { held: after_cut.values().all(|i| i.networks.contains(&network)) && rules_at_cut.values().all(|r| r.contains(container_faults::SILENCE_CHAIN)), names: Some(json!(silenced.members.iter().map(|u| json!({"node": u.node, "container": u.id})).collect::<Vec<_>>())), held_at_last_read });
    let reached = silence.iter().any(|r| replied(&r["legs"]["direct"]) || replied(&r["legs"]["via_peer"]));
    let heard_ready = silence.iter().any(|r| r["view"].as_array().unwrap().iter().any(|v| v["status"] == "ready-for-traffic"));
    ev.progress = Some(Progress { reads: reads.clone(), complete_while_held: heard_ready || reached });
    ev.routing = Some(Routing {
        expected_routable: false,
        observed_routable: reached,
        observed: silence.iter().map(|r| format!("direct {} via-peer {} none {}", r["legs"]["direct"]["outcome"], r["legs"]["via_peer"]["outcome"], r["legs"]["no_active_route"]["outcome"])).collect::<Vec<_>>().join("; "),
    });
    let nodes_during = estate.nodes().await;
    let (seats_during, seats_detail_during) = match seats_as_expected(&nodes_during) { Ok(()) => (true, String::new()), Err(e) => (false, e) };
    let provider_alive_during = last.values().all(|i| i.running && i.status == "running");
    let attempt_during = attempt_of(&estate.get(&format!("/api/builds?id={fabric_build}")).await.1);
    let fabric_during = estate.get("/api/fabric").await.1;
    let fp_of = |nodes: &[Value]| nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).unwrap_or_default();
    let inc_during = node_in(&nodes_during, &target_name).map(|n| s(&n["incarnation_id"])).unwrap_or_default();

    // The heal.
    let healed_at = {
        silenced.lift().expect("every container's packet filter is restored");
        now_nanos()
    };
    let after_heal = inspect_all(&silenced);
    let hold_ended = !silenced.rules().expect("the packet filters are listed").values().any(|r| r.contains(container_faults::SILENCE_CHAIN));
    ev.release = Some(Release { acked: true, hold_ended });

    // Convergence: coverage (every mesh heard by every admin), topology (the same births, the
    // same Build pointer), routes (the formerly isolated node served by the same birth).
    let conv_started = std::time::Instant::now();
    let agreeing = wait_for("every admin holds the healed fabric", Duration::from_secs(150), || async { converged(&estate, &want_births, &["mesh1", "mesh2"], &fabric_build).await.ok() }).await;
    let converged_ms = conv_started.elapsed().as_millis() as u64;
    let healed = legs(&estate, &target_id, &carrier);
    let kept = estate.probe(&["get", "--target", &format!("exact:{target_id}"), "--key", "iso"]);
    let nodes_after = estate.nodes().await;
    let (seats_after, seats_detail_after) = match seats_as_expected(&nodes_after) { Ok(()) => (true, String::new()), Err(e) => (false, e) };
    let attempt_after = attempt_of(&estate.get(&format!("/api/builds?id={fabric_build}")).await.1);
    let fabric_after = estate.get("/api/fabric").await.1;
    ev.recovery = Some(Recovery { work_complete: replied(&healed["direct"]) && replied(&healed["via_peer"]), marker_after: json!({"containers": after_heal.iter().map(|(n, i)| (n.clone(), json!({"status": i.status, "networks": i.networks}))).collect::<BTreeMap<_, _>>(), "direct": leg_shape(&healed["direct"]), "via_peer": leg_shape(&healed["via_peer"]), "ready": births(&nodes_after).len()}).to_string() });
    ev.control = Some(Control {
        seats_as_expected: [seats_before, seats_during, seats_after],
        seats_detail: [seats_detail_before.clone(), seats_detail_during.clone(), seats_detail_after.clone()].join(" | "),
        fabric_primary: [fabric_primary.clone(), fp_of(&nodes_during), fp_of(&nodes_after)],
        authority_may_move: false,
        incarnation: [target_inc.clone(), inc_during.clone(), node_in(&nodes_after, &target_name).map(|n| s(&n["incarnation_id"])).unwrap_or_default()],
        attempts: [attempt_before, attempt_during],
        exact_runtime_alive: provider_alive_during,
        replaced_during: inc_during != target_inc || attempt_during != attempt_before || s(&fabric_during["build_id"]) != fabric_build,
    });
    let containers_after: BTreeMap<String, String> = isolated_names.iter().map(|n| (n.clone(), estate.container_of(n).unwrap_or_default())).collect();
    let mut rec = Reconciliation::default();
    rec.check("every isolated node runs in the container it ran in before the cut", containers_after == containers_before, format!("before {containers_before:?}, after {containers_after:?}"));
    rec.check(
        "every isolated container ran throughout: same init, same start, no restart, no exit",
        silenced.members.iter().all(|u| {
            let (a, b) = (&after_cut[&u.node], &after_heal[&u.node]);
            a.running && b.running && a.pid == b.pid && a.started_at == b.started_at && b.restart_count == 0 && b.exit_code == 0 && !b.oom_killed
        }),
        format!("{after_heal:?}"),
    );
    rec.check("every birth is back, the same set the cut found", births(&nodes_after) == want_births, format!("{:?}", births(&nodes_after).symmetric_difference(&want_births).collect::<Vec<_>>()));
    rec.check("the value stored before the cut is served by the same birth after the heal", kept["reply"]["result"] == json!({"found": true, "value": "before-isolation"}) && kept["reply"]["incarnation_id"] == target_inc.as_str() && kept["reply"]["executing_node"] == target_id.as_str(), kept.to_string());
    rec.check("Fabric.build_id is the Build it was, and that Build gained no attempt", s(&fabric_after["build_id"]) == fabric_build && attempt_after == attempt_before, format!("attempts {attempt_before} -> {attempt_during} -> {attempt_after}"));
    rec.check("the fabric primary is the seat holder it was", fp_of(&nodes_after) == fabric_primary, fp_of(&nodes_after));
    rec.check("every admin's view agrees on the healed fabric", !agreeing.is_empty(), format!("{agreeing:?}"));
    rec.check("the advertised seats equal the public candidates' after the heal", seats_after, seats_detail_after.clone());
    ev.reconciliation = Some(rec);

    estate.stop().await;
    let spans = estate.spans();

    // Evidence from the exported spans.
    let resolves: Vec<&Value> = spans.iter().filter(|sp| sp["name"] == "rdm.node_rpc.route.resolve.via-connections" && sp["attributes"]["target"] == target_id.as_str()).collect();
    let by = |route: &str, replied: bool| resolves.iter().filter(|sp| sp["attributes"]["route"] == route && (sp["attributes"]["outcome"] == "Reply") == replied).cloned().collect::<Vec<_>>();
    let serves: Vec<&Value> = named(&spans, "rdm.node_rpc.proof_store.serve.via-request").into_iter().filter(|sp| sp["attributes"]["node_id"] == target_id.as_str()).collect();
    let silent_traces: BTreeSet<String> = resolves.iter().filter(|sp| sp["attributes"]["outcome"] != "Reply" && start(sp) > cut_at && start(sp) < healed_at).map(|sp| s(&sp["trace_id"])).collect();
    let served_in_silence: Vec<&&Value> = serves.iter().filter(|sp| silent_traces.contains(&s(&sp["trace_id"]))).collect();
    let silent_resolves: Vec<&&Value> = resolves.iter().filter(|sp| start(sp) > cut_at && start(sp) < healed_at).collect();
    let silent_by = |route: &str| silent_resolves.iter().filter(|sp| sp["attributes"]["route"] == route).count();
    let reads_n = silence.len();
    let isolated_set: BTreeSet<&str> = isolated_names.iter().map(String::as_str).collect();
    let mutations: Vec<Value> = spans
        .iter()
        .filter(|sp| start(sp) > cut_at && start(sp) < healed_at)
        .filter(|sp| {
            let n = sp["name"].as_str().unwrap_or_default();
            let on_isolated = isolated_set.contains(sp["attributes"]["node"].as_str().unwrap_or_default());
            (n.starts_with("rdm.node_admin.node.") && (n.contains(".create.") || n.contains(".delete.") || n.contains(".retire.") || n.contains(".update.via-build")) && on_isolated)
                || (n == "rdm.node_admin.deployment.update.via-pipeline" && on_isolated)
                || (n == "rdm.node_admin.build.update.via-reconcile" && sp["attributes"]["build_id"] == fabric_build.as_str())
        })
        .map(|sp| span_row(sp, &["node", "build_id", "attempt", "pipeline", "action", "outcome"]))
        .collect();
    let elections_after_cut: Vec<&Value> = named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute").into_iter().filter(|sp| start(sp) > cut_at && start(sp) < healed_at).collect();
    // A kept-side admin hears every member of its mesh and the fabric primary throughout: it never
    // names another winner. The silenced side's admins hear only their own mesh and elect among it.
    let moved: Vec<&&Value> = elections_after_cut.iter().filter(|sp| !isolated_set.contains(attr(sp, "observer")) && sp["attributes"]["winner_path"] != fabric_primary.as_str()).collect();
    let silenced_side_elections: Vec<&&Value> = elections_after_cut.iter().filter(|sp| isolated_set.contains(attr(sp, "observer"))).collect();

    let mut rec2 = ev.reconciliation.take().unwrap();
    rec2.check("no call reached the isolated node while it was silent: no serve span in the silent legs' traces", served_in_silence.is_empty() && !silent_traces.is_empty(), format!("{} silent traces, {} served", silent_traces.len(), served_in_silence.len()));
    rec2.check("the silent legs resolved Direct, ViaPeer and NoActiveRoute, none replying", silent_by("direct") == reads_n && silent_by("via-peer") == reads_n && silent_by("no-active-route") == reads_n && silent_resolves.iter().all(|sp| sp["attributes"]["outcome"] != "Reply"), format!("{:?}", silent_resolves.iter().map(|sp| (sp["attributes"]["route"].clone(), sp["attributes"]["outcome"].clone())).collect::<Vec<_>>()));
    // The ViaPeer certainty is the carrier's own edge fact: the first forward into the cut may end
    // `Indeterminate(ReplyDeadline)`; once the carrier has named its lost edge no later leg is
    // `Indeterminate` again, and the last leg names the carrier and the target.
    let via: Vec<(String, String)> = silence.iter().map(|r| (s(&r["legs"]["via_peer"]["outcome"]), s(&r["legs"]["via_peer"]["reason"]))).collect();
    let edge_named = |v: &(String, String)| v.0 == "NotSent" && v.1.contains("CarrierEdgeLost");
    let first_named = via.iter().position(edge_named);
    let via_ok = first_named.is_some_and(|i| via[i..].iter().all(edge_named))
        && via[..first_named.unwrap_or(0)].iter().all(|v| v.0 == "Indeterminate" && v.1 == "ReplyDeadline")
        && via.last().is_some_and(|v| v.1.contains(&format!("{carrier} -> {target_name}")));
    rec2.check("each silent ViaPeer leg is Indeterminate(ReplyDeadline) until the carrier names its lost edge to the target, then NotSent(CarrierEdgeLost) every time", via_ok, format!("{via:?}"));
    if cut == Cut::Node {
        let still: BTreeSet<(String, String)> = births(&nodes_during).into_iter().filter(|(n, _)| !isolated_set.contains(n.as_str())).collect();
        let want_rest: BTreeSet<(String, String)> = want_births.iter().filter(|(n, _)| !isolated_set.contains(n.as_str())).cloned().collect();
        rec2.check("every node other than the silenced one stays ready-for-traffic as the same birth while it is silent", still == want_rest, format!("differ: {:?}", still.symmetric_difference(&want_rest).collect::<Vec<_>>()));
    }
    rec2.check("the healthy legs before and after the silence replied Direct and ViaPeer", by("direct", true).len() >= 3 && by("via-peer", true).len() >= 2, format!("direct {} via-peer {}", by("direct", true).len(), by("via-peer", true).len()));
    rec2.check("the control plane created, retired, re-attempted and re-birthed nothing while the silence held", mutations.is_empty(), format!("{mutations:?}"));
    rec2.check("no kept-side admin named another fabric primary while the silence held", moved.is_empty(), format!("{} elections, {} by kept-side admins moved", elections_after_cut.len(), moved.len()));
    let own_ok = cut == Cut::Node || silence.iter().all(|r| {
        isolated_admins.iter().all(|(a, _)| {
            let v = &r["own_views"][a];
            isolated_names.iter().all(|n| v[n] == "ready-for-traffic") && v.as_object().is_some_and(|m| m.iter().filter(|(n, _)| !isolated_names.contains(n)).all(|(_, st)| st == "pending-reconnect" || st == "dead"))
        })
    });
    rec2.check("each silenced admin hears its own mesh ready and the other mesh unheard (a whole-mesh cut)", own_ok, silence.iter().map(|r| r["own_views"].to_string()).collect::<Vec<_>>().join(" | "));
    ev.reconciliation = Some(rec2);
    for sp in serves.iter() {
        ev.dispatches.push(Dispatch { birth: s(&sp["attributes"]["incarnation_id"]), current_birth: target_inc.clone(), after_supersession: false });
    }

    let verdict = judge(&ev);
    let (verdict_json, refusal) = match &verdict {
        Ok(v) => (serde_json::to_value(v).unwrap(), None),
        Err(r) => (Value::Null, Some(r.to_string())),
    };
    let result = json!({
        "cell": cell,
        "provider": estate.owner.provider,
        "network": network,
        "kept_mesh": kept_mesh,
        "isolated_mesh": cut_mesh,
        "silenced_subject": silenced_subject,
        "fabric_primary": fabric_primary,
        "target": {"name": target_name, "node_id": target_id, "incarnation_id": target_inc},
        "carrier": carrier,
        "healthy_control": control,
        "put": put,
        "containers_before": containers_before,
        "silenced": silenced,
        "rules_at_cut": rules_at_cut,
        "provider_inspect": {"after_cut": after_cut, "during_silence": last, "after_heal": after_heal},
        "view_when_unheard": silent_view.iter().filter(|n| isolated_names.contains(&s(&n["name"]))).map(|n| json!({"node": n["name"], "status": n["status"]})).collect::<Vec<_>>(),
        "silence": silence,
        "heal": {"converged_ms": converged_ms, "agreeing_admins": agreeing, "healed_legs": healed, "kept": kept},
        "seats": {"before": seats_before, "during": seats_during, "after": seats_after, "detail_during": seats_detail_during},
        "attempts": [attempt_before, attempt_during, attempt_after],
        "cut_unix_nano": cut_at,
        "healed_unix_nano": healed_at,
        "route_resolve_spans": resolves.iter().map(|sp| span_row(sp, &["target", "route", "outcome", "carrier"])).collect::<Vec<_>>(),
        "carried_inner_spans": named(&spans, "rdm.node_rpc.request.serve.via-carried-inner").into_iter().filter(|sp| sp["attributes"]["target"] == target_id.as_str()).map(|sp| span_row(sp, &["target"])).collect::<Vec<_>>(),
        "serve_spans": serves.iter().map(|sp| span_row(sp, &["node_id", "incarnation_id", "op", "outcome"])).collect::<Vec<_>>(),
        "elections_during_silence": elections_after_cut.iter().map(|sp| span_row(sp, &["observer", "inputs", "winner_node_id", "winner_path", "previous_node_id"])).collect::<Vec<_>>(),
        "silenced_side_elections": silenced_side_elections.len(),
        "mutations_during_silence": mutations,
        "evidence": ev,
        "verdict": verdict_json,
        "refusal": refusal,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    if let Err(r) = verdict {
        panic!("the detector refused the isolation of {silenced_subject}:\n{r}");
    }
}
