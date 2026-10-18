//! i143.e8.s6 acceptance (rafka-v2 #2784, hardened 2026-10-07), CHAOS-CONTAINER layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2784-chaos-container`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest, rpc ledger and every process's spans land under it, feature `i143-2784`).
//!
//! The faults are `rdm_test::container_faults`: real `docker network disconnect` /
//! `connect` of exact container ids, observed through `docker inspect`. The silence is judged by
//! `rdm_test::wedge` (family `PartitionHeal`).

use rdm_test::container_faults::{self, Inspected, Silenced};
use rdm_test::elections::seats_as_expected;
use rdm_client::{FabricView, NodeView, PathName, ScopeStatus};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::typed::fabric_view_of;
use rdm_test::wedge::*;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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
async fn legs(estate: &Estate, target_id: &str, carrier: &str) -> Value {
    let exact = format!("exact:{target_id}");
    let calls = [
        ("direct", vec!["get".to_string(), "--target".into(), exact.clone(), "--key".into(), "iso".into()]),
        ("via_peer", vec!["get".to_string(), "--target".into(), exact.clone(), "--via".into(), format!("path:{carrier}"), "--key".into(), "iso".into()]),
        ("no_active_route", vec!["get".to_string(), "--target".into(), exact, "--no-route".into(), "--key".into(), "iso".into()]),
    ];
    let probe = estate.probe_handle();
    let birth = estate.entry_birth();
    tokio::task::spawn_blocking(move || {
        // A sample retains its Direct → ViaPeer → NoActiveRoute sequence.
        // The three complete samples are independent and run concurrently below.
        let mut replies = serde_json::Map::new();
        for (leg, args) in calls {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            replies.insert(leg.to_string(), probe.run(&birth, &args).expect("the route probe answers by name"));
        }
        Value::Object(replies)
    }).await.expect("the route sample completed")
}

/// A leg reduced to what must be stable while the silence holds: the typed outcome and the leg.
/// A ViaPeer leg is held to whether it replied: its certainty is the carrier's own edge fact,
/// `NotSent(CarrierEdgeLost)`.
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
    silenced.inspect().expect("the exact containers can be inspected")
}

/// The fixed part of a read taken while the silence holds.
fn silence_marker(inspected: &BTreeMap<String, Inspected>, rules: &BTreeMap<String, String>, nodes: &[Value], legs: &Value) -> String {
    let containers: BTreeMap<&String, Value> = inspected.iter().map(|(n, i)| (n, json!({"id": i.id, "status": i.status, "pid": i.pid, "started_at": i.started_at, "restart_count": i.restart_count, "networks": i.networks}))).collect();
    let view: BTreeMap<&String, Value> = inspected.keys().map(|n| (n, json!({"unheard": node_in(nodes, n).is_some_and(unheard), "incarnation_id": node_in(nodes, n).map(|x| x["incarnation_id"].clone())}))).collect();
    json!({"containers": containers, "rules": rules, "view": view, "direct": leg_shape(&legs["direct"]), "via_peer": leg_shape(&legs["via_peer"]), "no_active_route": leg_shape(&legs["no_active_route"])}).to_string()
}

/// Every admin the view names ready, by its path.name.
fn ready_admins(nodes: &[Value]) -> Vec<String> {
    nodes.iter().filter(|n| n["kind"] == "node_admin" && n["status"] == "ready-for-traffic").map(|n| s(&n["name"])).collect()
}

/// The views as the JSON the cell records.
fn values(views: &[NodeView]) -> Vec<Value> {
    views.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect()
}

/// `fabric.get` at the admin the estate asks, as the JSON the cell records.
async fn fabric_json(estate: &Estate) -> Value {
    serde_json::to_value(estate.fabric_view().await).expect("a view serializes")
}

/// The admin `admin`'s own `node.get`, as JSON; `None` when it does not answer for this Fabric.
async fn nodes_at(estate: &Estate, admin: &str) -> Option<Vec<Value>> {
    let path: PathName = admin.parse().ok()?;
    estate.node_views_at(&path).await.map(|v| values(&v))
}

/// Read an isolated admin through its own network namespace. The host's UDP leg stays
/// cut; this probe binds the held container address and calls the exact held birth locally.
async fn local_nodes_at(before: &[Value], silenced: &Silenced, name: &str) -> Option<Vec<Value>> {
    let Some(member) = silenced.members.iter().find(|m| m.node == name) else {
        return None;
    };
    let view: NodeView = serde_json::from_value(node_in(before, name)?.clone()).ok()?;
    let birth = rdm_client::birth_of_view(&view)?;
    let probe = rdm_test::estate::binary("rafka-rpc-probe");
    let output = tokio::process::Command::new("docker")
        .args(["exec", "-e", &format!("RDM_PROBE_BIND={}", member.ip), &member.id])
        .arg(probe)
        .args(["--admin-birth", &serde_json::to_string(&birth).unwrap(), "nodes", "--target", &format!("exact:{}", birth.node_id)])
        .output().await.expect("execute the isolated admin's local Node RPC observer");
    assert!(output.status.success(), "{name}: local node.get observer: {}", String::from_utf8_lossy(&output.stderr));
    let answer: Value = serde_json::from_slice(&output.stdout).expect("the local observer's JSON reply");
    assert_eq!(answer["outcome"], "Reply", "{name}: {answer}");
    answer["nodes"].as_array().cloned()
}

/// The admin `admin`'s own `fabric.get`; `None` when it does not answer for this Fabric.
async fn fabric_at(estate: &Estate, admin: &str) -> Option<FabricView> {
    let path: PathName = admin.parse().ok()?;
    estate.fabric_view_at(&path).await
}

/// The (path, incarnation) set a view holds ready for traffic.
fn births(nodes: &[Value]) -> BTreeSet<(String, String)> {
    nodes.iter().filter(|n| n["status"] == "ready-for-traffic").map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect()
}

/// Healed: every admin the entry view names ready holds the same births as the entry view, every
/// mesh of the fabric ready-for-traffic in its own `fabric.get`, and the same the current build_id.
/// `Err` names what a view still lacks.
async fn converged(estate: &Estate, want: &BTreeSet<(String, String)>, meshes: &[&str], build_id: &str) -> Result<Vec<String>, String> {
    let nodes = estate.node_values().await;
    if &births(&nodes) != want {
        return Err(format!("entry view births differ: missing {:?}, extra {:?}", want.difference(&births(&nodes)).collect::<Vec<_>>(), births(&nodes).difference(want).collect::<Vec<_>>()));
    }
    let mut agreeing = Vec::new();
    for name in ready_admins(&nodes) {
        let Some(theirs) = nodes_at(estate, &name).await else { return Err(format!("{name} does not answer node.get")) };
        if &births(&theirs) != want {
            return Err(format!("{name} holds other births: missing {:?}, extra {:?}", want.difference(&births(&theirs)).collect::<Vec<_>>(), births(&theirs).difference(want).collect::<Vec<_>>()));
        }
        let Some(f) = fabric_at(estate, &name).await else { return Err(format!("{name} does not answer fabric.get")) };
        if f.build_id.as_deref() != Some(build_id) {
            return Err(format!("{name} holds the current build_id {:?} not {build_id}", f.build_id));
        }
        let heard: BTreeSet<String> = f.meshes.iter().filter(|m| m.status == ScopeStatus::ReadyForTraffic).map(|m| m.name.clone()).collect();
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
/// (deadline), a ViaPeer call through a node of the other mesh ends `NotSent` naming the
/// carrier's lost edge (`CarrierEdgeLost`, every time, never `Indeterminate`), a NoActiveRoute call is
/// `NotSent`, and no serve span exists for them; the advertised seats equal what the public
/// candidates compute, the fabric-primary stays put, and the Build gains no attempt and
/// the current build_id does not move; the control plane creates, retires and re-births nothing. After
/// the release every silenced node is back as the same birth (same incarnation, same container),
/// every admin's view holds the same births and every mesh ready-for-traffic, and a Direct and a
/// ViaPeer call to the silenced node are served by the same birth with the value stored before the
/// cut (the healthy control, run before the cut as well). What must NOT happen: a container
/// stopped, removed or replaced, a node declared dead and re-born, a terminate, a retire, or an
/// election move because of the silence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn container_network_partitions_heals_preserves_live_runtimes() {
    if crate::own_process::delegated(module_path!(), "container_network_partitions_heals_preserves_live_runtimes") {
        return;
    }
    run_isolation(CELL, Cut::PeerMesh).await;
}

/// CONTRACT (#2784): the same cut on ONE rpc node of the peer mesh without the seat. The node alone
/// is unheard: its mesh-mates, both mesh's admins and the fabric-primary stay ready-for-traffic in
/// the entry view, no admin and no other node is re-born, the silenced container runs throughout
/// (same init, no restart), a Direct, ViaPeer and NoActiveRoute call to it end without a reply with
/// no serve span, and after the release the same birth serves the value stored before the cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn container_node_unheard_heals_serves_same_birth() {
    if crate::own_process::delegated(module_path!(), "container_node_unheard_heals_serves_same_birth") {
        return;
    }
    run_isolation(NODE_CELL, Cut::Node).await;
}

async fn run_isolation(cell: &'static str, cut: Cut) {
    assert_eq!(std::env::var("MESH_SPAWN_TYPE").as_deref(), Ok("container"), "this cell runs only on the container provider (MESH_SPAWN_TYPE=container); a process run never stands in for it");
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    let build_id = estate.reconcile_shape("fabric1", &[("mesh1", 2, 2), ("mesh2", 2, 2)]).await.build_id.to_string();
    let all: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=2).map(move |i| format!("{m}.rpc.{i}")))).collect();
    let nodes = values(&estate.settles_to_named(&all, Duration::from_secs(60)).await);

    // The side that is cut off is the mesh without the fabric-primary seat; control is read through
    // an admin of the other side.
    let fabric_primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("the view names a fabric primary");
    let kept_mesh = fabric_primary.split('.').next().unwrap().to_string();
    let cut_mesh = if kept_mesh == "mesh1" { "mesh2" } else { "mesh1" }.to_string();
    let kept = nodes.iter().find(|n| n["mesh"] == kept_mesh.as_str() && n["kind"] == "node_admin" && n["status"] == "ready-for-traffic" && n["admin_api_base"].is_string()).expect("a kept admin answers").clone();
    let kept_admin = s(&kept["name"]);
    estate.enter(&Estate::path(&kept_admin)).await;
    let nodes = estate.node_values().await;
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
    let fabric_before = fabric_json(&estate).await;
    let fabric_build = s(&fabric_before["build_id"]);
    assert_eq!(fabric_build, build_id, "the current build_id names the creation Build: {fabric_before}");
    let attempt_before = estate.attempt_of_build(&fabric_build).await;
    let (seats_before, seats_detail_before) = match seats_as_expected(&nodes) { Ok(()) => (true, String::new()), Err(e) => (false, e) };

    // The healthy control, run first in the same capture: the same three legs to the same node
    // reach it and are served by it; its value is stored for the heal to find.
    let put = estate.probe(&["put", "--target", &format!("exact:{target_id}"), "--key", "iso", "--value", "before-isolation"]);
    assert_eq!(put["outcome"], "Reply", "{put}");
    assert_eq!(put["reply"]["incarnation_id"], target_inc.as_str(), "{put}");
    let control = legs(&estate, &target_id, &carrier).await;
    assert!(replied(&control["direct"]) && replied(&control["via_peer"]), "the healthy control reaches the node by both routes: {control}");
    assert_eq!(control["via_peer"]["route"], "via-peer", "{control}");
    assert_eq!(control["no_active_route"]["outcome"], "NotSent", "{control}");
    assert_eq!(control["direct"]["reply"]["executing_node"], target_id.as_str(), "{control}");

    // The provider's view of every container before the cut.
    let containers_before: BTreeMap<String, String> = isolated_names.iter().map(|n| (n.clone(), estate.container_of(n).unwrap_or_else(|| panic!("{n}: no running container")))).collect();
    let network = container_faults::network_of(&estate);

    // The cut: every container of the mesh silenced against the rest of the estate, held. Each keeps
    // its interface and stays attached to the fabric network.
    let isolated_admins: Vec<String> = nodes.iter().filter(|n| n["mesh"] == cut_mesh.as_str() && n["kind"] == "node_admin").map(|n| s(&n["name"])).collect();
    // Q50 A: this is a pre-expiry healing proof. The cut has its own release deadline,
    // independent of the probes, and its plan is refused before any filter is installed.
    let floor = rdm_core::cadence::staleness_floor();
    let hold_floors = std::env::var("RDM_HUNG_HOLD_FLOORS").ok().and_then(|v| v.trim().parse::<u64>().ok()).filter(|n| *n >= 1).unwrap_or(10);
    let hold = Duration::from_millis(u64::try_from(floor.as_millis()).expect("the declared floor fits milliseconds").saturating_mul(hold_floors));
    let planned_silence = floor + Duration::from_secs(20);
    assert!(planned_silence < hold, "planned silence {planned_silence:?} must be under the declared hold {hold_floors} × {floor:?} = {hold:?}");
    let silenced = container_faults::prepare_silence(&estate, &isolated_names, &fabric_primary).expect("the exact containers and addresses are resolved before the cut");
    let silence_started = std::time::Instant::now();
    let release_at = silence_started + planned_silence;
    silenced.install().expect("the isolated mesh's containers are silenced");
    let (lift_now, release) = std::sync::mpsc::channel();
    let lift = {
        let silenced = silenced.clone();
        std::thread::spawn(move || {
            let completed = release.recv_timeout(release_at.saturating_duration_since(std::time::Instant::now())).is_ok();
            silenced.lift().expect("every container's packet filter is restored");
            (now_nanos(), silence_started.elapsed(), completed)
        })
    };
    let cut_at = now_nanos();
    let rules_at_cut = silenced.rules().expect("the packet filters are listed");
    let primitive = json!({"docker": "exec --user 0 --privileged <exact-id> xtables-multi iptables-restore", "network": network, "chain": container_faults::SILENCE_CHAIN, "silenced": silenced, "rules": rules_at_cut});
    let mut ev = Evidence::new(Family::PartitionHeal, format!("network:silence-{silenced_subject}"));
    ev.primitive = Some(Primitive { armed: true, ack: primitive.clone() });
    let after_cut = inspect_all(&silenced);
    for (n, i) in &after_cut {
        assert!(i.networks.contains(&network) && i.running, "{n}: the silence keeps the container running and attached to {network}: {i:?}");
    }

    // The membership consequence, seen from the other mesh.
    // A peer mesh that goes unheard is read in the other mesh's view. A lone silent member is heard
    // missing by its own mesh (the other mesh's view of a peer mesh's members is the topology its
    // mesh primary forwards, which never carries liveness), so its mesh-mate admin is the observer.
    let observer = match cut {
        Cut::PeerMesh => kept_admin.clone(),
        Cut::Node => ready_admins(&nodes).into_iter().find(|n| n.starts_with(&format!("{cut_mesh}.")) && !isolated_names.contains(n)).expect("a ready node-admin of the silenced node's own mesh"),
    };
    let observed = |observer: String| {
        let estate = &estate;
        async move { nodes_at(estate, &observer).await.unwrap_or_default() }
    };
    let wait_for_unheard = format!("{silenced_subject} unheard in the view of {}", if cut == Cut::Node { &cut_mesh } else { &kept_mesh });
    let silent_view: Vec<Value> = wait_for(&wait_for_unheard, Duration::from_secs(90), || async {
        let nodes = observed(observer.clone()).await;
        isolated_names.iter().all(|n| node_in(&nodes, n).is_some_and(unheard)).then_some(nodes)
    })
    .await;

    // Three independent observations run together; each retains all three routes,
    // exact provider reads, filters and both sides' own views.
    let observe = || async {
        let provider = {
            let silenced = silenced.clone();
            tokio::task::spawn_blocking(move || {
                let inspected = inspect_all(&silenced);
                let rules = silenced.rules().expect("the packet filters are listed");
                (inspected, rules)
            })
        };
        let own = async {
            let mut own_views = serde_json::Map::new();
            let mut local = tokio::task::JoinSet::new();
            for name in &isolated_admins {
                if cut == Cut::PeerMesh {
                    let (nodes, silenced, name) = (nodes.clone(), silenced.clone(), name.clone());
                    local.spawn(async move { let seen = local_nodes_at(&nodes, &silenced, &name).await; (name, seen) });
                    continue;
                }
                let seen = nodes_at(&estate, name).await;
                own_views.insert(name.clone(), match seen {
                    Some(v) => json!(v.iter().map(|n| (s(&n["name"]), s(&n["status"]))).collect::<BTreeMap<_, _>>()),
                    None => json!("unanswered"),
                });
            }
            while let Some(read) = local.join_next().await {
                let (name, seen) = read.expect("the isolated admin's own view read completes");
                own_views.insert(name, match seen {
                    Some(v) => json!(v.iter().map(|n| (s(&n["name"]), s(&n["status"]))).collect::<BTreeMap<_, _>>()),
                    None => json!("unanswered"),
                });
            }
            own_views
        };
        let (l, provider, nodes_now, own_views) = tokio::join!(legs(&estate, &target_id, &carrier), provider, observed(observer.clone()), own);
        let (inspected, rules) = provider.expect("the exact provider and filter observations complete");
        let marker = silence_marker(&inspected, &rules, &nodes_now, &l);
        let sample = json!({"legs": l, "containers": inspected, "rules": rules, "own_views": own_views, "view": isolated_names.iter().map(|n| json!({"node": n, "status": node_in(&nodes_now, n).map(|x| x["status"].clone())})).collect::<Vec<_>>()});
        (marker, sample)
    };
    let (first, second, third) = tokio::join!(observe(), observe(), observe());
    let (reads, silence): (Vec<_>, Vec<_>) = [first, second, third].into_iter().unzip();
    // These independent last reads all belong to the held observation, not a
    // sequence of extra RPCs after the route samples have completed.
    let last_provider = {
        let silenced = silenced.clone();
        tokio::task::spawn_blocking(move || (inspect_all(&silenced), silenced.rules().expect("the packet filters are listed")))
    };
    let (last_provider, nodes_during, attempt_during, fabric_during) = tokio::join!(
        last_provider, estate.node_values(), estate.attempt_of_build(&fabric_build), fabric_json(&estate)
    );
    let (last, last_rules) = last_provider.expect("the last exact provider and filter reads complete");
    let held_at_last_read = last_rules.values().all(|r| r.contains(&format!("-A INPUT -j {}", container_faults::SILENCE_CHAIN)) && r.contains(&format!("-A OUTPUT -j {}", container_faults::SILENCE_CHAIN))) && last.values().all(|i| i.networks.contains(&network));
    ev.fault = Some(FaultAck { held: after_cut.values().all(|i| i.networks.contains(&network)) && rules_at_cut.values().all(|r| r.contains(container_faults::SILENCE_CHAIN)), names: Some(json!(silenced.members.iter().map(|u| json!({"node": u.node, "container": u.id})).collect::<Vec<_>>())), held_at_last_read });
    let reached = silence.iter().any(|r| replied(&r["legs"]["direct"]) || replied(&r["legs"]["via_peer"]));
    let heard_ready = silence.iter().any(|r| r["view"].as_array().unwrap().iter().any(|v| v["status"] == "ready-for-traffic"));
    ev.progress = Some(Progress { reads: reads.clone(), complete_while_held: heard_ready || reached });
    ev.routing = Some(Routing {
        expected_routable: false,
        observed_routable: reached,
        observed: silence.iter().map(|r| format!("direct {} via-peer {} none {}", r["legs"]["direct"]["outcome"], r["legs"]["via_peer"]["outcome"], r["legs"]["no_active_route"]["outcome"])).collect::<Vec<_>>().join("; "),
    });
    let (seats_during, seats_detail_during) = match seats_as_expected(&nodes_during) { Ok(()) => (true, String::new()), Err(e) => (false, e) };
    let provider_alive_during = last.values().all(|i| i.running && i.status == "running");
    let fp_of = |nodes: &[Value]| nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).unwrap_or_default();
    let inc_during = node_in(&nodes_during, &target_name).map(|n| s(&n["incarnation_id"])).unwrap_or_default();

    // The heal. A deadline release also runs if an observation has not returned.
    let _ = lift_now.send(());
    let (healed_at, actual_silence, completed) = lift.join().expect("the cut's release completed");
    std::fs::write(dir.join("held-observations.json"), serde_json::to_vec_pretty(&json!({
        "planned_silence_ms": planned_silence.as_millis(), "hold_ms": hold.as_millis(),
        "actual_silence_ms": actual_silence.as_millis(), "observations_completed_before_release": completed,
        "silence": silence, "last_provider": last, "last_rules": last_rules,
    })).unwrap()).unwrap();
    assert!(actual_silence < hold, "the cut was lifted after {actual_silence:?}, past its declared {hold:?} hold");
    assert!(completed, "the observations exceeded the planned {planned_silence:?} silence; the cut was lifted at its release deadline");
    let after_heal = inspect_all(&silenced);
    let hold_ended = !silenced.rules().expect("the packet filters are listed").values().any(|r| r.contains(container_faults::SILENCE_CHAIN));
    ev.release = Some(Release { acked: true, hold_ended });

    // Convergence: coverage (every mesh heard by every admin), topology (the same births, the
    // same current Build), routes (the formerly isolated node served by the same birth).
    let conv_started = std::time::Instant::now();
    let agreeing = wait_for("every admin holds the healed fabric", Duration::from_secs(150), || async { converged(&estate, &want_births, &["mesh1", "mesh2"], &fabric_build).await.ok() }).await;
    let converged_ms = conv_started.elapsed().as_millis() as u64;
    let healed = legs(&estate, &target_id, &carrier).await;
    let kept = estate.probe(&["get", "--target", &format!("exact:{target_id}"), "--key", "iso"]);
    let nodes_after = estate.node_values().await;
    let (seats_after, seats_detail_after) = match seats_as_expected(&nodes_after) { Ok(()) => (true, String::new()), Err(e) => (false, e) };
    let attempt_after = estate.attempt_of_build(&fabric_build).await;
    let fabric_after = fabric_json(&estate).await;
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
    rec.check("the current build_id is the Build it was, and that Build gained no attempt", s(&fabric_after["build_id"]) == fabric_build && attempt_after == attempt_before, format!("attempts {attempt_before} -> {attempt_during} -> {attempt_after}"));
    rec.check("the fabric primary is the seat holder it was", fp_of(&nodes_after) == fabric_primary, fp_of(&nodes_after));
    rec.check("every admin's view agrees on the healed fabric", !agreeing.is_empty(), format!("{agreeing:?}"));
    rec.check("the advertised seats equal the public candidates' after the heal", seats_after, seats_detail_after.clone());
    ev.reconciliation = Some(rec);

    estate.stop().await;
    let spans = estate.spans();

    // Evidence from the exported spans.
    let all_resolves: Vec<&Value> = spans.iter().filter(|sp| sp["name"] == "rdm.node_rpc.route.resolve.via-connections" && sp["attributes"]["target"] == target_id.as_str()).collect();
    // The cell's own legs are the probe's; the control plane's calls to the same node are recorded apart.
    let resolves: Vec<&Value> = all_resolves.iter().copied().filter(|sp| sp["service"] == "rafka-rpc-probe").collect();
    let control_plane_resolves: Vec<&Value> = all_resolves.iter().copied().filter(|sp| sp["service"] != "rafka-rpc-probe" && start(sp) > cut_at && start(sp) < healed_at).collect();
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
    // A silent ViaPeer leg never replies and ends `NotSent(CarrierEdgeLost)` naming the carrier's
    // edge to the target: the carrier bounds its inner call by the origin's remaining budget, so
    // its answer always arrives first. The sequence is recorded.
    let via: Vec<(String, String)> = silence.iter().map(|r| (s(&r["legs"]["via_peer"]["outcome"]), s(&r["legs"]["via_peer"]["reason"]))).collect();
    let via_typed = via.iter().all(|v| v.0 == "NotSent" && v.1.contains(&format!("CarrierEdgeLost(\"{carrier} -> {target_name} ")));
    rec2.check("each silent ViaPeer leg ends NotSent(CarrierEdgeLost) naming the carrier and the target", via_typed, format!("{via:?}"));
    if cut == Cut::Node {
        let still: BTreeSet<(String, String)> = births(&nodes_during).into_iter().filter(|(n, _)| !isolated_set.contains(n.as_str())).collect();
        let want_rest: BTreeSet<(String, String)> = want_births.iter().filter(|(n, _)| !isolated_set.contains(n.as_str())).cloned().collect();
        rec2.check("every node other than the silenced one stays ready-for-traffic as the same birth while it is silent", still == want_rest, format!("differ: {:?}", still.symmetric_difference(&want_rest).collect::<Vec<_>>()));
    }
    rec2.check("the healthy legs before and after the silence replied Direct and ViaPeer", by("direct", true).len() >= 3 && by("via-peer", true).len() >= 2, format!("direct {} via-peer {}", by("direct", true).len(), by("via-peer", true).len()));
    rec2.check("the control plane created, retired, re-attempted and re-birthed nothing while the silence held", mutations.is_empty(), format!("{mutations:?}"));
    rec2.check("no kept-side admin named another fabric primary while the silence held", moved.is_empty(), format!("{} elections, {} by kept-side admins moved", elections_after_cut.len(), moved.len()));
    let own_ok = cut == Cut::Node || silence.iter().all(|r| {
        isolated_admins.iter().all(|a| {
            let v = &r["own_views"][a.as_str()];
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
        "cell": cell, "planned_silence_ms": planned_silence.as_millis(), "hold_ms": hold.as_millis(), "actual_silence_ms": actual_silence.as_millis(),
        "provider": estate.owner.provider,
        "network": network,
        "kept_mesh": kept_mesh,
        "isolated_mesh": cut_mesh,
        "silenced_subject": silenced_subject,
        "via_peer_sequence": via,
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
        "control_plane_resolves_during_silence": control_plane_resolves.iter().map(|sp| span_row(sp, &["target", "route", "outcome", "carrier", "protocol"])).collect::<Vec<_>>(),
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

async fn mesh_id_of(estate: &Estate, mesh: &str) -> String {
    estate.mesh_view(mesh).await.id.map(|id| id.to_string()).unwrap_or_default()
}

const LOSS_CELL: &str = "container_peer_mesh_lost_recovers_under_same_mesh_id";

/// CONTRACT (#2784): on a real Docker estate of two meshes (two node-admins and two rpc nodes each)
/// the peer mesh that does not hold the fabric seat loses both node-admins at once (`docker kill` of the
/// exact container ids) while its two rpc nodes run. The provider shows each admin not running. The
/// fabric primary hears nothing of the mesh; its investigation asks the members, which cannot reach
/// their own node-admin, and it keeps the seat, opens a later attempt of the SAME accepted Build and
/// runs the mesh-create flow for the lost mesh's first admin under its EXISTING MeshId; the new mesh
/// primary creates the second admin. A mesh whose every container is gone has no member to say so and is
/// held, never reborn (the process cell `peer_mesh_whose_members_all_stay_silent_is_held_not_reborn`).
/// Afterwards both admins are new births in new containers, the lost mesh's rpc nodes and the surviving
/// mesh's births are the births they were (never re-created), the current build_id is the accepted Build, no
/// MeshId is minted, and a call to an rpc node of the recovered mesh is served. What must NOT happen: the
/// fabric primary killed, a new Build or MeshId, a surviving member re-created, the seat moving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn container_peer_mesh_lost_recovers_under_same_mesh_id() {
    if crate::own_process::delegated(module_path!(), "container_peer_mesh_lost_recovers_under_same_mesh_id") {
        return;
    }
    assert_eq!(std::env::var("MESH_SPAWN_TYPE").as_deref(), Ok("container"), "this cell runs only on the container provider (MESH_SPAWN_TYPE=container); a process run never stands in for it");
    let cell = LOSS_CELL;
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let accepted = estate.reconcile_shape("fabric1", &[("mesh1", 2, 2), ("mesh2", 2, 2)]).await.build_id.to_string();
    let all: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=2).map(move |i| format!("{m}.rpc.{i}")))).collect();
    let before = values(&estate.settles_to_named(&all, Duration::from_secs(60)).await);
    let holder = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let kept_mesh = holder.split('.').next().unwrap().to_string();
    let lost_mesh = if kept_mesh == "mesh1" { "mesh2" } else { "mesh1" }.to_string();
    let (lost_mesh_id, kept_mesh_id) = (mesh_id_of(&estate, &lost_mesh).await, mesh_id_of(&estate, &kept_mesh).await);

    // The mesh to lose holds no seat; control goes through the fabric primary, which the fault never touches.
    
    estate.enter(&Estate::path(&holder)).await;

    // The mesh to lose: its containers, by exact id, before the fault.
    let lost_names: Vec<String> = all.iter().filter(|n| n.starts_with(&format!("{lost_mesh}."))).cloned().collect();
    let containers_before: BTreeMap<String, String> = lost_names.iter().map(|n| (n.clone(), estate.container_of(n).unwrap_or_else(|| panic!("{n}: no running container")))).collect();
    let lost_admins: Vec<String> = lost_names.iter().filter(|n| n.contains(".admin.")).cloned().collect();
    let lost_births: BTreeMap<String, String> = births(&before).into_iter().filter(|(n, _)| lost_admins.contains(n)).collect();
    let lost_mesh_rpc_births: BTreeSet<(String, String)> = births(&before).into_iter().filter(|(n, _)| lost_names.contains(n) && !lost_admins.contains(n)).collect();
    let kept_births: BTreeSet<(String, String)> = births(&before).into_iter().filter(|(n, _)| n.starts_with(&format!("{kept_mesh}."))).collect();
    let attempt_before = estate.attempt_of_build(&accepted).await;
    let fault_at = now_nanos();

    // THE FAULT: both admins at once; each kill is the exact container.
    let mut killed = Vec::new();
    for n in &lost_admins {
        if estate.bootstrap_pid().is_some() && *n == format!("{}.admin.1", "mesh1") {
            estate.kill_bootstrap();
        } else {
            container_faults::docker(&["kill", &containers_before[n]]).unwrap_or_else(|e| panic!("{n}: {e}"));
        }
        killed.push(json!({"node": n, "container": containers_before[n]}));
    }
    assert_eq!(killed.len(), 2, "both node-admins of {lost_mesh} were killed: {killed:?}");
    let inspected_after_kill: BTreeMap<String, Value> = containers_before
        .iter()
        .filter(|(n, _)| lost_admins.contains(n))
        .map(|(n, id)| {
            let v = match container_faults::inspect(id) {
                Ok(i) => json!({"running": i.running, "status": i.status, "exit_code": i.exit_code}),
                Err(e) => json!({"removed": e}),
            };
            (n.clone(), v)
        })
        .collect();
    for (n, v) in &inspected_after_kill {
        assert!(v["running"] != true, "{n}: the killed container is not running: {v}");
    }

    // RECOVERY through the rectifier: later attempts of the SAME Build, every lost member reborn.
    let done = wait_for("the accepted Build completes a later attempt and every lost birth is reborn", Duration::from_secs(300), || {
        let (estate, accepted, lost_births) = (&estate, accepted.clone(), lost_births.clone());
        async move {
            let b = estate.builds_obj().await.get(&rdm_client::BuildId::parse(&accepted).ok()?).await.ok()?;
            let later = b.phase == rdm_core::build::BuildPhase::Complete && u64::from(b.attempt) > attempt_before;
            let reborn = births(&estate.node_values().await).iter().filter(|(n, i)| lost_births.get(n).is_some_and(|old| old != i)).count() == lost_births.len();
            (later && reborn).then_some(b)
        }
    })
    .await;
    let after = values(&estate.settles_to_named(&all, Duration::from_secs(60)).await);
    let fabric_after = fabric_json(&estate).await;
    assert_eq!(s(&fabric_after["build_id"]), accepted, "no new topology Build: the current build_id is the accepted Build");
    assert_eq!(mesh_id_of(&estate, &lost_mesh).await, lost_mesh_id, "{lost_mesh} recovered under its own MeshId");
    assert_eq!(mesh_id_of(&estate, &kept_mesh).await, kept_mesh_id);
    assert_eq!(after.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])), Some(holder.clone()), "the seat stayed in {kept_mesh}");
    let kept_after: BTreeSet<(String, String)> = births(&after).into_iter().filter(|(n, _)| n.starts_with(&format!("{kept_mesh}."))).collect();
    assert_eq!(kept_after, kept_births, "the surviving mesh's births were never re-created");
    let containers_after: BTreeMap<String, String> = lost_names.iter().map(|n| (n.clone(), estate.container_of(n).unwrap_or_default())).collect();
    for n in &lost_admins {
        assert!(!containers_after[n].is_empty() && containers_after[n] != containers_before[n], "{n}: runs in a new container ({} -> {})", containers_before[n], containers_after[n]);
    }
    for n in lost_names.iter().filter(|n| !lost_admins.contains(n)) {
        assert_eq!(containers_after[n], containers_before[n], "{n}: the rpc node that outlived its mesh's admins runs in the container it ran in");
    }
    let lost_mesh_rpc_after: BTreeSet<(String, String)> = births(&after).into_iter().filter(|(n, _)| lost_names.contains(n) && !lost_admins.contains(n)).collect();
    assert_eq!(lost_mesh_rpc_after, lost_mesh_rpc_births, "the lost mesh's rpc nodes were never re-created");

    // A recovered rpc node serves.
    let target = after.iter().find(|n| s(&n["name"]) == format!("{lost_mesh}.rpc.1")).cloned().expect("recovered rpc node");
    let put = estate.probe(&["put", "--target", &format!("exact:{}", s(&target["node_id"])), "--key", "lost", "--value", "after-recovery"]);
    assert_eq!(put["outcome"], "Reply", "{put}");
    assert_eq!(put["reply"]["incarnation_id"], target["incarnation_id"], "{put}");

    estate.stop().await;
    let spans = estate.spans();
    let at = |sp: &Value| start(sp);
    // The pending hand-off to the lost mesh's first admin names the EXISTING MeshId and ran at the fabric primary.
    let handoffs: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff")
        .into_iter()
        .filter(|h| attr(h, "mesh") == lost_mesh && at(h) > fault_at && (attr(h, "outcome") == "applied" || attr(h, "outcome") == "already-applied"))
        .collect();
    let first = handoffs.iter().max_by_key(|h| at(h)).copied().expect("Pending handed to the recovery admin after the fault");
    assert_eq!(attr(first, "mesh_id"), lost_mesh_id, "Pending names the same MeshId: it joined, it never minted");
    assert_eq!(attr(first, "node"), holder, "the surviving fabric primary ran the first admin's birth");
    let recovery_admin = attr(first, "target").to_string();
    // The second admin is created by the NEW mesh primary, never by the fabric primary.
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| attr(r, "build_id") == accepted).collect();
    let creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| attr(c, "build_id") == accepted && attr(c, "node").starts_with(&format!("{lost_mesh}.admin.")) && attr(c, "node") != recovery_admin && at(c) > at(first))
        .collect();
    assert!(!creates.is_empty(), "the second admin of {lost_mesh} was created after the first: {creates:?}");
    for c in &creates {
        // The reconcile the create ran in is the one in its own trace: a drift lane's attempt number
        // also names an attempt of the fabric lane (R-BI7).
        let executor = reconciles.iter().find(|r| r["trace_id"] == c["trace_id"] && attr(r, "attempt") == attr(c, "attempt")).map(|r| attr(r, "executor")).expect("the create ran inside a reconcile");
        assert_eq!(executor, recovery_admin, "{} was created by the new mesh primary, not by {holder}", attr(c, "node"));
    }
    // No survivor was created or deleted by any attempt after the fault.
    let touched: Vec<String> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .chain(named(&spans, "rdm.node_admin.node.delete.via-build"))
        .filter(|sp| attr(sp, "build_id") == accepted && attr(sp, "node").starts_with(&format!("{kept_mesh}.")) && at(sp) > fault_at)
        .map(|sp| attr(sp, "node").to_string())
        .collect();
    assert!(touched.is_empty(), "no member of the surviving mesh was created or retired after the fault: {touched:?}");

    let result = json!({
        "cell": cell,
        "provider": estate.owner.provider,
        "accepted_build_id": accepted,
        "fabric_primary": holder,
        "fabric_primary_before_and_after": holder,
        "lost_mesh": lost_mesh,
        "lost_mesh_id": lost_mesh_id,
        "containers_before": containers_before,
        "killed": killed,
        "inspected_after_kill": inspected_after_kill,
        "containers_after": containers_after,
        "attempt_before_fault": attempt_before,
        "final_build": {"build_id": done.build_id.as_str(), "phase": format!("{:?}", done.phase), "attempt": done.attempt, "executor": done.executor, "reason": done.reason},
        "recovery_admin": recovery_admin,
        "pending_handoff": span_row(first, &["mesh", "mesh_id", "node", "target", "outcome"]),
        "fault_unix_nano": fault_at,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

const RETIRE_CELL: &str = "container_primary_mesh_retired_hands_off_authority_before_its_containers_end";

/// CONTRACT (#2784, fabric loss): on a real Docker estate of two meshes (two node-admins and three
/// rpc nodes each) the mesh that holds the fabric seat is lost the way an operator loses it: one
/// Build retires it, executed by an admin outside it. While the Build runs, every surviving admin
/// is asked who holds the fabric and the advertised endpoint is asked at once; an advertised
/// endpoint is always a control endpoint of a birth of this run. Fabric authority moves to a ready
/// admin of the surviving mesh, through an election a surviving admin announces before the former
/// holder's terminate step ends; the former holder's containers are then gone from the Docker
/// daemon's running set (each one's terminal state read from the daemon, or the container removed),
/// while every surviving container is the one it was (same id, same init, same start, running) and
/// every survivor answers for the Fabric from the new holder. What must NOT happen: the former
/// holder's containers ended before the hand-off, a survivor's container replaced, an advertised
/// endpoint that belongs to no birth of the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn container_primary_mesh_retired_hands_off_authority_before_its_containers_end() {
    if crate::own_process::delegated(module_path!(), "container_primary_mesh_retired_hands_off_authority_before_its_containers_end") {
        return;
    }
    assert_eq!(std::env::var("MESH_SPAWN_TYPE").as_deref(), Ok("container"), "this cell runs only on the container provider (MESH_SPAWN_TYPE=container); a process run never stands in for it");
    let cell = RETIRE_CELL;
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = [("mesh1", 2, 3), ("mesh2", 2, 3)];
    estate.reconcile_shape("fabric1", &shape).await;
    let before = values(&estate.settled_views(&shape).await);
    // The initial fabric refuses a mesh delete until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let holder = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let lost_mesh = holder.split('.').next().unwrap().to_string();
    let survivor_mesh = if lost_mesh == "mesh1" { "mesh2" } else { "mesh1" }.to_string();

    // Every container of the run before the loss, as the daemon holds it.
    let mut containers_before: BTreeMap<String, (String, Inspected)> = BTreeMap::new();
    for n in &before {
        let name = s(&n["name"]);
        let id = estate.container_of(&name).unwrap_or_else(|| panic!("{name}: no running container"));
        let ins = container_faults::inspect(&id).unwrap_or_else(|e| panic!("{name}: {e}"));
        containers_before.insert(name, (id, ins));
    }
    let admin_bases: BTreeMap<String, String> = before.iter().filter(|n| n["kind"] == "node_admin").map(|n| (s(&n["name"]), s(&n["admin_api_base"]))).collect();
    let run_bases: BTreeSet<String> = admin_bases.values().cloned().collect();
    assert_eq!(run_bases.len(), 4, "four admins, four control endpoints: {run_bases:?}");
    let survivors: Vec<String> = admin_bases.keys().filter(|n| n.starts_with(&format!("{survivor_mesh}."))).cloned().collect();
    let survivor_bases: BTreeSet<String> = survivors.iter().map(|n| admin_bases[n].clone()).collect();
    estate.enter(&Estate::path(&survivors[0])).await;
    let fabric_id = estate.fabric_id.clone();
    let peers = estate.peers().await;

    // Every survivor is asked (its own fabric.get), continuously through the loss, who holds the
    // fabric, and the holder it names is asked straight after.
    let samples: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (samples, stop, fabric_id, peers) = (samples.clone(), stop.clone(), fabric_id.clone(), peers.clone());
        let asked: Vec<PathName> = survivors.iter().map(|n| n.parse().expect("a survivor's path.name")).collect();
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                for from in &asked {
                    let at = now_nanos();
                    let row = match fabric_view_of(&peers, from, &fabric_id).await {
                        None => json!({"at_unix_nano": at, "asked": from.to_string(), "answered": false}),
                        Some(f) => match f.fabric_primary {
                            None => json!({"at_unix_nano": at, "asked": from.to_string(), "answered": true, "fabric_primary": "", "advertised": f.admin_api_base}),
                            Some(holder) => {
                                let followed = fabric_view_of(&peers, &holder, &fabric_id).await;
                                json!({"at_unix_nano": at, "asked": from.to_string(), "answered": true, "fabric_primary": holder.to_string(), "advertised": f.admin_api_base, "advertised_answers": followed.is_some()})
                            }
                        },
                    };
                    samples.lock().unwrap().push(row);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
    };

    // THE HAND-OFF AND LOSS, through the rectifier: one Build retiring the holder's mesh.
    let started = now_nanos();
    let build_id = estate.reconcile_shape("fabric1", &[(survivor_mesh.as_str(), 2, 3)]).await.build_id.to_string();
    let build_complete = now_nanos();
    // mesh-delete.md steps 5–7: mesh.deleted leaves its reporting primary alive.
    // The caller deletes that exact held final primary in a separate node.delete.
    let delete = estate.submit_change(rdm_client::Change::RemoveNode { node: Estate::path(&holder) }).await;
    let delete_build_id = estate.follow(delete).await.build_id.to_string();
    // The former holder's containers: terminal at the daemon (not running, or removed).
    let mut lost_terminal = BTreeMap::new();
    for (name, (id, _)) in containers_before.iter().filter(|(n, _)| n.starts_with(&format!("{lost_mesh}."))) {
        let id = id.clone();
        let terminal = wait_for(&format!("{name}'s container is terminal at the daemon"), Duration::from_secs(60), || {
            let id = id.clone();
            async move {
                match container_faults::inspect(&id) {
                    Ok(i) if i.running => None,
                    Ok(i) => Some(json!({"status": i.status, "exit_code": i.exit_code})),
                    Err(e) if e.contains("No such") || e.contains("no such") => Some(json!({"removed": e})),
                    Err(e) => panic!("{name}: the daemon could not be read: {e}"),
                }
            }
        })
        .await;
        lost_terminal.insert(name.clone(), terminal);
    }
    let after = values(&estate.settled_views(&[(survivor_mesh.as_str(), 2, 3)]).await);
    let consistent = std::sync::atomic::AtomicUsize::new(0);
    let agrees = || async {
        for name in &survivors {
            let f = fabric_view_of(&peers, &name.parse().ok()?, &fabric_id).await?;
            let (h, advertised) = (f.fabric_primary.clone()?, f.admin_api_base.clone().unwrap_or_default());
            let own = fabric_view_of(&peers, &h, &fabric_id).await?;
            if !h.to_string().starts_with(&format!("{survivor_mesh}.admin.")) || own.fabric_primary.as_ref() != Some(&h) || !survivor_bases.contains(&advertised) {
                return None;
            }
        }
        Some(())
    };
    wait_for("every survivor advertises one live holder that agrees, ten samples running", Duration::from_secs(30), || async {
        match agrees().await {
            Some(()) => (consistent.fetch_add(1, Ordering::SeqCst) + 1 >= 10).then_some(()),
            None => {
                consistent.store(0, Ordering::SeqCst);
                None
            }
        }
    })
    .await;
    stop.store(true, Ordering::SeqCst);
    sampler.await.unwrap();
    let fabric_now = fabric_json(&estate).await;
    let new_holder = s(&fabric_now["fabric_primary"]);
    let new_base = s(&fabric_now["admin_api_base"]);
    assert!(new_holder.starts_with(&format!("{survivor_mesh}.admin.")), "the new holder {new_holder} is an admin of {survivor_mesh}");
    assert!(after.iter().any(|n| s(&n["name"]) == new_holder && s(&n["admin_api_base"]) == new_base), "the new holder {new_holder} is a ready admin of the surviving view and its advertised endpoint is its own: {after:#?}");
    assert!(survivor_bases.contains(&new_base), "the authority endpoint belongs to a surviving birth of this run: {new_base}");
    // Every surviving container is the one it was.
    let mut survivors_inspected = BTreeMap::new();
    for (name, (id, was)) in containers_before.iter().filter(|(n, _)| n.starts_with(&format!("{survivor_mesh}."))) {
        let now = container_faults::inspect(id).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(now.running && now.pid == was.pid && now.started_at == was.started_at && now.restart_count == 0, "{name}: the surviving container is the one it was: {was:?} -> {now:?}");
        assert_eq!(estate.container_of(name).as_deref(), Some(id.as_str()), "{name} still runs in container {id}");
        survivors_inspected.insert(name.clone(), json!({"id": id, "pid": now.pid, "started_at": now.started_at}));
    }

    estate.enter(&Estate::path(&new_holder)).await;
    estate.stop().await;
    let spans = estate.spans();
    let rows = samples.lock().unwrap().clone();
    let outside: Vec<&Value> = rows.iter().filter(|r| r["advertised"].as_str().is_some_and(|a| !run_bases.contains(a))).collect();
    assert!(outside.is_empty(), "an advertised fabric endpoint is a port of no birth of this run: {outside:#?}");
    let after_complete = rows.iter().filter(|r| r["at_unix_nano"].as_u64().unwrap() > build_complete && r["asked"].is_string()).count();
    assert!(after_complete > 0, "the survivors were asked after the Build completed");
    // The hand-off precedes the former holder's end: a surviving admin announced the new fabric
    // primary before the retire pipeline's terminate step of the former holder ended.
    let recompute = named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute");
    let handed_off: Vec<&&Value> = recompute.iter().filter(|sp| attr(sp, "election_level") == "fabric_primary" && attr(sp, "winner_mesh") == survivor_mesh && attr(sp, "previous").starts_with(&format!("{lost_mesh}."))).collect();
    assert!(!handed_off.is_empty(), "a surviving admin announced the new fabric primary of {survivor_mesh}, succeeding {lost_mesh}'s");
    let first_handoff = handed_off.iter().map(|sp| start(sp)).min().unwrap();
    let steps = named(&spans, "rdm.node_admin.deployment.update.via-step");
    let holder_terminate: Vec<&Value> = steps.iter().copied().filter(|sp| attr(sp, "step") == "TerminateRuntime" && attr(sp, "node") == holder && attr(sp, "build_id") == delete_build_id).collect();
    assert_eq!(holder_terminate.len(), 1, "the retire pipeline terminated the former holder {holder} once under {delete_build_id}: {holder_terminate:?}");
    assert_eq!(attr(holder_terminate[0], "outcome"), "complete");
    let terminated_at = holder_terminate[0]["end_unix_nano"].as_u64().unwrap();
    assert!(first_handoff < terminated_at, "fabric authority moved ({first_handoff}) before the former holder's runtime was gone ({terminated_at})");

    let result = json!({
        "cell": cell,
        "provider": estate.owner.provider,
        "build_id": build_id,
        "former_holder": holder,
        "delete_build_id": delete_build_id,
        "lost_mesh": lost_mesh,
        "survivor_mesh": survivor_mesh,
        "new_holder": new_holder,
        "authority_endpoint": new_base,
        "started_unix_nano": started,
        "build_complete_unix_nano": build_complete,
        "first_handoff_announcement_unix_nano": first_handoff,
        "former_holder_terminate_ended_unix_nano": terminated_at,
        "lost_containers_terminal": lost_terminal,
        "survivor_containers": survivors_inspected,
        "samples": {"total": rows.len(), "after_complete": after_complete, "advertised_outside_run": outside.len()},
        "refused_follow_ups": rows.iter().filter(|r| r["advertised"].as_str().is_some() && r["advertised_answers"] == false).count(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

const EXACT_CELL: &str = "container_fault_backend_pauses_and_kills_exact_container_recovers_current_birth";

fn num(v: &Value) -> u64 {
    v.as_u64().or_else(|| v.as_str().and_then(|a| a.parse().ok())).unwrap_or(0)
}

/// This process's own spans: the product span pipeline (`install_span_capture`: its span filter and
/// `BatchSpanProcessor`) in front of an in-memory exporter behind the global subscriber, so the
/// fault backend's and the detector's spans are exactly what this cell emitted.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    service: String,
}

fn capture(cell: &str) -> Capture {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let service = format!("i143-2784-{cell}");
    let capture = rdm_transport::install_span_capture(&service, exporter.clone()).expect("one global subscriber per cell process");
    Capture { exporter, capture, service }
}

impl Capture {
    fn spans(&self) -> Vec<Value> {
        self.capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let finished = self.exporter.get_finished_spans().unwrap();
        crate::assert_own_story(finished.iter().map(|s| s.name.as_ref()));
        finished
            .into_iter()
            .map(|sp| {
                let attributes: serde_json::Map<String, Value> = sp.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": sp.name,
                    "trace_id": sp.span_context.trace_id().to_string(),
                    "span_id": sp.span_context.span_id().to_string(),
                    "parent_span_id": sp.parent_span_id.to_string(),
                    "resource": {"service.name": self.service},
                    "attributes": attributes,
                })
            })
            .collect()
    }
}



fn seats(nodes: &[Value]) -> (bool, String) {
    match seats_as_expected(nodes) {
        Ok(()) => (true, String::new()),
        Err(e) => (false, e),
    }
}

fn fabric_primary_of(nodes: &[Value]) -> String {
    rdm_test::elections::advertised_fabric_primaries(nodes).join(",")
}

/// CONTRACT (#2784): the container fault backend acts on the exact container the daemon holds for
/// a birth (Docker id + Fabric label + node label + start time), never on a name alone, and the
/// rectifier recovers the current birth. A container under another start time, another node's
/// label or another Fabric's label is refused by name with the container untouched. A paused rpc
/// node is alive and silent: the daemon acknowledges the pause, the view stops calling it ready,
/// calls to it never complete, and no replacement, re-attempt or new birth follows; the unpause
/// returns the SAME birth in the SAME container, which serves the value stored before the pause
/// (the semantic wedge detector judges it). A killed rpc node and a killed node-admin that is
/// neither mesh-primary nor fabric-primary are terminal by the daemon's observed exit only, and each
/// is a lost member of a mesh with a live mesh-primary (node-recovery.md, mesh-recovery.md:8-9): its
/// mesh-primary submits the recovery as the next attempt of the SAME Build (reason recovery,
/// its delete, create and update operations, no Build minted), the path is re-created as a new
/// NodeId and incarnation in a new container at the same path.name over the inherited data dir
/// through the create pipeline (every step a child of it), the old NodeId never follows the path, and exactly one
/// container serves each path. What must NOT happen: a command reaching a container
/// the fact does not name, a paused container replaced, a Build minted for drift, the
/// fabric-primary node-admin touched, or a mesh left without a node-admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn container_fault_backend_pauses_and_kills_exact_container_recovers_current_birth() {
    if crate::own_process::delegated(module_path!(), "container_fault_backend_pauses_and_kills_exact_container_recovers_current_birth") {
        return;
    }
    use container_faults::{ExactContainer, Fault, Refusal};
    assert_eq!(std::env::var("MESH_SPAWN_TYPE").as_deref(), Ok("container"), "this cell runs only on the container provider (MESH_SPAWN_TYPE=container); a process run never stands in for it");
    let cell = EXACT_CELL;
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    // Three node-admins: the bootstrap admin is the harness's own control address and is never
    // touched, and a launched admin that is neither primary is always left to kill.
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 3, 3)]).await;
    estate.settled_views(&[("mesh1", 3, 3)]).await;
    let floor = rdm_transport::membership::staleness_floor();
    let build_id = estate.accepted_build_id().await;
    let nodes0 = estate.node_values().await;
    let rpcs: Vec<Value> = nodes0.iter().filter(|n| n["kind"] == "rpc_node").cloned().collect();
    assert_eq!(rpcs.len(), 3);
    let (hold_n, kill_n) = (&rpcs[0], &rpcs[1]);
    let (hold, kill) = (s(&hold_n["name"]), s(&kill_n["name"]));
    let attempt_before = estate.attempt_of_build(&build_id).await;

    // Survivor evidence, complete before any authority disappears: a value stored on each victim.
    let hold_exact = format!("exact:{}", s(&hold_n["node_id"]));
    let kill_exact = format!("exact:{}", s(&kill_n["node_id"]));
    for (exact, inc) in [(&hold_exact, &hold_n["incarnation_id"]), (&kill_exact, &kill_n["incarnation_id"])] {
        let put = estate.probe(&["put", "--target", exact, "--key", "83", "--value", "before-fault"]);
        assert_eq!(put["outcome"], "Reply", "{put}");
        assert_eq!(put["reply"]["incarnation_id"], *inc, "{put}");
    }
    let survivors_before: BTreeSet<String> = nodes0.iter().map(|n| s(&n["node_id"])).collect();

    // ---- Forged identities: refused by name, nothing sent.
    let hold_c = ExactContainer::of(&estate, &hold).expect("the held node's container");
    let restarted = hold_c.with_started_at("2001-01-01T00:00:00Z").apply(Fault::Pause);
    assert!(matches!(restarted, Err(Refusal::NotThisContainer { .. })), "a container under another start time is refused: {restarted:?}");
    let other_node = hold_c.with_node(&kill).apply(Fault::Kill);
    assert!(matches!(other_node, Err(Refusal::WrongNode { .. })), "{other_node:?}");
    let other_fabric = hold_c.with_fabric("another-fabric").apply(Fault::Kill);
    assert!(matches!(other_fabric, Err(Refusal::WrongFabric { .. })), "{other_fabric:?}");
    let gone = ExactContainer { id: "0".repeat(64), ..hold_c.clone() }.apply(Fault::Kill);
    assert!(matches!(gone, Err(Refusal::NoSuchContainer { .. })), "{gone:?}");
    assert_eq!(container_faults::inspect(&hold_c.id).unwrap().status, "running", "nothing was sent: the container still runs");
    assert!(hold_c.check().is_ok(), "the exact container is untouched");

    // ---- Hold: docker pause of the exact container, judged by the semantic detector.
    let mut ev = Evidence::new(Family::SilentRuntime, format!("silent-runtime:pause-{hold}"));
    let nodes_pre = estate.node_values().await;
    let (seats_before, detail_before) = seats(&nodes_pre);
    let fp_before = fabric_primary_of(&nodes_pre);
    let paused = hold_c.apply(Fault::Pause).expect("the exact container is paused");
    assert_eq!(paused.status_after, "paused");
    ev.primitive = Some(Primitive { armed: true, ack: serde_json::to_value(&paused).unwrap() });
    let silent_view = wait_for(&format!("{hold} no longer ready in the public view"), floor * 2 + Duration::from_secs(30), || async { estate.node_value_opt(&hold).await.filter(|n| n["status"] != "ready-for-traffic") }).await;
    let mut reads = Vec::new();
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        let probe = estate.probe(&["get", "--target", &hold_exact, "--key", "83"]);
        let i = container_faults::inspect(&hold_c.id).unwrap();
        reads.push(json!({"status": i.status, "pid": i.pid, "cpu_ticks": rdm_test::process_faults::cpu_ticks(i.pid as u32), "outcome": probe["outcome"]}).to_string());
        outcomes.push(probe);
    }
    let held_at_last_read = container_faults::inspect(&hold_c.id).unwrap().status == "paused";
    ev.fault = Some(FaultAck { held: true, names: Some(json!({"container": hold_c.id, "started_at": hold_c.started_at, "view_status": silent_view["status"]})), held_at_last_read });
    ev.progress = Some(Progress { reads, complete_while_held: outcomes.iter().any(|o| o["outcome"] == "Reply") });
    ev.routing = Some(Routing { expected_routable: false, observed_routable: outcomes.iter().any(|o| o["outcome"] == "Reply"), observed: format!("view {}; calls {}", silent_view["status"], outcomes.iter().map(|o| s(&o["outcome"])).collect::<Vec<_>>().join(",")) });
    let nodes_during = estate.node_values().await;
    let (seats_during, detail_during) = seats(&nodes_during);
    let during = nodes_during.iter().find(|n| n["name"] == hold.as_str()).cloned();
    let attempt_during = estate.attempt_of_build(&build_id).await;
    let alive = hold_c.check().is_ok();
    let replaced = during.as_ref().is_none_or(|n| n["node_id"] != hold_n["node_id"]);
    let released = hold_c.apply(Fault::Unpause).expect("the pause is released");
    ev.release = Some(Release { acked: true, hold_ended: released.status_after == "running" });
    let back = wait_for(&format!("{hold} back as its own birth"), floor * 2 + Duration::from_secs(30), || async {
        estate.node_value_opt(&hold).await.filter(|n| n["incarnation_id"] == hold_n["incarnation_id"] && n["status"] == "ready-for-traffic")
    })
    .await;
    let kept = estate.probe(&["get", "--target", &hold_exact, "--key", "83"]);
    ev.recovery = Some(Recovery { work_complete: kept["outcome"] == "Reply", marker_after: json!({"status": container_faults::inspect(&hold_c.id).unwrap().status, "outcome": kept["outcome"]}).to_string() });
    let nodes_after = estate.node_values().await;
    let (seats_after, detail_after) = seats(&nodes_after);
    ev.control = Some(Control {
        seats_as_expected: [seats_before, seats_during, seats_after],
        seats_detail: [detail_before, detail_during, detail_after.clone()].join(" | "),
        fabric_primary: [fp_before, fabric_primary_of(&nodes_during), fabric_primary_of(&nodes_after)],
        authority_may_move: false,
        incarnation: [s(&hold_n["incarnation_id"]), during.as_ref().map(|n| s(&n["incarnation_id"])).unwrap_or_default(), s(&back["incarnation_id"])],
        attempts: [attempt_before, attempt_during],
        exact_runtime_alive: alive,
        replaced_during: replaced,
    });
    let mut rec = Reconciliation::default();
    rec.check("the value stored before the pause is served after it", kept["reply"]["result"] == json!({"found": true, "value": "before-fault"}), kept.to_string());
    rec.check("the same birth serves it", kept["reply"]["incarnation_id"] == hold_n["incarnation_id"] && kept["reply"]["executing_node"] == hold_n["node_id"], kept.to_string());
    rec.check("the exact container is the one that was paused (same id, same start)", ExactContainer::of(&estate, &hold).as_ref() == Ok(&hold_c), format!("{hold_c:?}"));
    rec.check("the Build gained no attempt", estate.attempt_of_build(&build_id).await == attempt_before, format!("attempt {attempt_before}"));
    rec.check("the seats equal the public candidates' after the release", seats_after, detail_after);
    ev.reconciliation = Some(rec);

    // ---- Kill: the exact container of another rpc node exits; the rectifier recovers the path.
    let kill_c = ExactContainer::of(&estate, &kill).expect("the killed node's container");
    let killed = kill_c.apply(Fault::Kill).expect("the exact container is killed");
    assert!(killed.exited && killed.exit_code == 137, "the daemon observed the SIGKILL exit: {killed:?}");
    assert!(matches!(kill_c.apply(Fault::Kill), Err(Refusal::AlreadyExited { .. }) | Err(Refusal::NoSuchContainer { .. })), "a second kill finds nothing running");
    let replacement = wait_for("a new logical node holds the path", Duration::from_secs(180), || async {
        let n = estate.node_value_opt(&kill).await?;
        (n["status"] == "ready-for-traffic" && n["node_id"] != kill_n["node_id"]).then_some(n)
    })
    .await;
    let replacement_c = ExactContainer::of(&estate, &kill).expect("the replacement's container");
    assert_ne!(replacement_c.id, kill_c.id, "the replacement runs in a new container");
    let fresh = estate.probe(&["get", "--target", &format!("path:{kill}"), "--key", "83"]);
    assert_eq!(fresh["reply"]["result"], json!({"found": true, "value": "before-fault"}), "the recovered node serves the store of the data dir it inherited: {fresh}");
    let old = estate.probe(&["get", "--target", &kill_exact, "--key", "83"]);
    assert_eq!((&old["outcome"], &old["reason"], &old["route"]), (&json!("NotSent"), &json!("Resolve(Unknown)"), &json!("direct")), "exact:<old> never follows the recovered node: {old}");
    assert_eq!(estate.live_containers().iter().filter(|(n, _)| *n == kill).count(), 1, "one container serves the path");

    // ---- A node-admin that is neither mesh primary nor fabric primary: killed by exact container.
    let nodes = estate.node_values().await;
    let admin_n = nodes.iter().find(|n| n["kind"] == "node_admin" && n["is_primary"] == false && n["is_fabric_primary"] == false && n["name"] != "mesh1.admin.1").cloned().unwrap_or_else(|| panic!("a non-primary node-admin: {nodes:?}"));
    let admin = s(&admin_n["name"]);
    let admin_c = ExactContainer::of(&estate, &admin).expect("the admin's container");
    let admin_killed = admin_c.apply(Fault::Kill).expect("the admin's exact container is killed");
    assert!(admin_killed.exited);
    let admin_new = wait_for("a new logical node holds the admin's path", Duration::from_secs(180), || async {
        let n = estate.node_value_opt(&admin).await?;
        (n["status"] == "ready-for-traffic" && n["node_id"] != admin_n["node_id"]).then_some(n)
    })
    .await;
    assert_eq!(admin_new["endpoint_id"], admin_n["endpoint_id"], "the recovered node-admin holds the endpoint key of the data dir it inherited: {admin_new}");
    // Ready in the view is the new birth's own digest; the recovery is done when its mesh-primary's
    // submit followed the recovery's operations to the attempt's terminal frame.
    wait_for("the node-admin's recovery is followed to its terminal", Duration::from_secs(60), || async {
        named(&estate.spans(), "rdm.node_admin.build.update.via-node-recovery").into_iter().any(|sp| attr(sp, "node") == admin && attr(sp, "outcome").starts_with("complete: attempt")).then_some(())
    })
    .await;
    estate.settled_views(&[("mesh1", 3, 3)]).await;
    let nodes_end = estate.node_values().await;
    let survivors_after: BTreeSet<String> = nodes_end.iter().map(|n| s(&n["node_id"])).collect();
    let untouched: Vec<&String> = survivors_before.iter().filter(|id| ![s(&kill_n["node_id"]), s(&admin_n["node_id"])].contains(id)).collect();
    assert!(untouched.iter().all(|id| survivors_after.contains(*id)), "no other node was replaced: before {survivors_before:?} after {survivors_after:?}");
    let seats_end = seats(&nodes_end);
    assert!(seats_end.0, "{}", seats_end.1);
    assert_eq!(estate.accepted_build_id().await, build_id, "the current build_id is the Build it was");
    estate.stop().await;

    // ---- Evidence: the estate's exported spans and this process's own.
    let spans = estate.spans();
    // The recoveries are attempts of mesh1's drift lane (R-BI7: its mesh-primary runs them in its own
    // mesh's lane, numbered from 1 there; the fabric lane's attempt number is not theirs).
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| attr(r, "build_id") == build_id && attr(r, "lane") == "mesh1").collect();
    let recovery_for = |path: &str| -> Value {
        reconciles
            .iter()
            .find(|r| attr(r, "reason") == "recovery" && attr(r, "operations") == format!("node.delete:{path},node.create:{path}"))
            .cloned()
            .cloned()
            .unwrap_or_else(|| panic!("a recovery attempt of {build_id} recovering {path}: {reconciles:?}"))
    };
    let (rpc_rec, admin_rec) = (recovery_for(&kill), recovery_for(&admin));
    assert!(named(&spans, "rdm.node_admin.build.create.via-proven-drift").is_empty(), "no Build is minted for drift");
    // The pipeline in the recovery reconcile's own trace: a lane's attempt number also names an
    // attempt of the fabric lane, whose create pipeline of the same path is the node's first birth.
    let pipeline_of = |path: &str, attempt: u64, rec: &Value| -> Value {
        named(&spans, "rdm.node_admin.deployment.update.via-pipeline")
            .into_iter()
            .find(|p| p["trace_id"] == rec["trace_id"] && attr(p, "pipeline") == "create" && attr(p, "node") == path && attr(p, "build_id") == build_id && num(&p["attributes"]["attempt"]) == attempt)
            .cloned()
            .unwrap_or_else(|| panic!("the recovery's create pipeline span of {path} at attempt {attempt} in the recovery's trace"))
    };
    let mut recoveries = Vec::new();
    for (path, rec) in [(&kill, &rpc_rec), (&admin, &admin_rec)] {
        let attempt = num(&rec["attributes"]["attempt"]);
        let pipe = pipeline_of(path, attempt, rec);
        let steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|st| st["parent_span_id"] == pipe["span_id"]).collect();
        assert!(!steps.is_empty(), "{path}: step spans are children of the create pipeline");
        assert!(steps.iter().all(|st| st["trace_id"] == pipe["trace_id"] && attr(st, "node") == path.as_str()), "{path}: every step shares the pipeline's trace");
        recoveries.push(json!({
            "path": path, "attempt": rec["attributes"]["attempt"], "reason": rec["attributes"]["reason"], "action": rec["attributes"]["action"], "operations": rec["attributes"]["operations"],
            "reconcile": {"trace_id": rec["trace_id"], "span_id": rec["span_id"], "parent_span_id": rec["parent_span_id"], "service": rec["service"]},
            "pipeline": {"trace_id": pipe["trace_id"], "span_id": pipe["span_id"], "parent_span_id": pipe["parent_span_id"], "provider": pipe["attributes"]["provider"]},
            "steps": steps.iter().map(|st| json!({"step": st["attributes"]["step"], "outcome": st["attributes"]["outcome"], "span_id": st["span_id"], "parent_span_id": st["parent_span_id"], "trace_id": st["trace_id"]})).collect::<Vec<_>>(),
        }));
    }
    // The dispatches to the held node's births.
    for sp in named(&spans, "rdm.node_rpc.proof_store.serve.via-request").into_iter().filter(|sp| attr(sp, "node_id") == s(&hold_n["node_id"])) {
        ev.dispatches.push(Dispatch { birth: attr(sp, "incarnation_id").to_string(), current_birth: s(&hold_n["incarnation_id"]), after_supersession: false });
    }
    assert!(!ev.dispatches.is_empty(), "the held node's birth exported serve spans for the calls that reached it");
    let verdict = judge(&ev).unwrap_or_else(|r| panic!("the detector refused the paused container: {r}"));
    let own = cap.spans();
    let fault_spans: Vec<&Value> = named(&own, "rdm.testkit.fault.update.via-container-command");
    for f in ["pause", "unpause", "kill"] {
        assert!(fault_spans.iter().any(|sp| attr(sp, "fault") == f && attr(sp, "outcome").contains("applied")), "an applied `{f}` fault span was emitted by the backend");
    }
    let refused_spans = fault_spans.iter().filter(|sp| ["not_this_container", "wrong_node", "wrong_fabric", "no_such_container"].iter().any(|r| attr(sp, "outcome").contains(r))).count();
    assert!(refused_spans >= 4, "the four refusals left spans: {refused_spans}");
    let verdict_span = named(&own, "rdm.scenario.wedge.resolve.via-detector");
    assert!(!verdict_span.is_empty(), "the detector's verdict span was emitted");

    let result = json!({
        "cell": cell,
        "provider": estate.owner.provider,
        "build_id": build_id,
        "attempt_before": attempt_before,
        "refusals": {"other_start": format!("{restarted:?}"), "other_node": format!("{other_node:?}"), "other_fabric": format!("{other_fabric:?}"), "no_such_container": format!("{gone:?}")},
        "hold": {"node": hold, "container": hold_c, "paused": paused, "released": released, "view_while_held": silent_view, "calls_while_held": outcomes, "after_release": kept, "verdict": serde_json::to_value(&verdict).unwrap(), "evidence": serde_json::to_value(&ev).unwrap()},
        "kill": {"node": kill, "killed": killed, "old": {"node_id": kill_n["node_id"], "incarnation_id": kill_n["incarnation_id"], "container": kill_c.id}, "replacement": {"node_id": replacement["node_id"], "incarnation_id": replacement["incarnation_id"], "container": replacement_c.id}, "fresh": fresh["reply"], "old_exact_probe": old},
        "admin_kill": {"node": admin, "killed": admin_killed, "old_node_id": admin_n["node_id"], "new_node_id": admin_new["node_id"], "was_fabric_primary": admin_n["is_fabric_primary"], "was_mesh_primary": admin_n["is_primary"]},
        "recoveries": recoveries,
        "backend_spans": fault_spans.iter().map(|sp| json!({"fault": attr(sp, "fault"), "container": attr(sp, "container"), "outcome": attr(sp, "outcome"), "trace_id": sp["trace_id"], "span_id": sp["span_id"], "service": sp["resource"]["service.name"]})).collect::<Vec<_>>(),
        "detector_spans": verdict_span.iter().map(|sp| json!({"trace_id": sp["trace_id"], "span_id": sp["span_id"], "parent_span_id": sp["parent_span_id"]})).collect::<Vec<_>>(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&own).unwrap()).unwrap();
}
