//! i143 acceptance (rafka-v2 #2892, finding 2), CHAOS-PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2892-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest and every process's spans land under it, feature `i143-2892`, test the
//! cell's name).
//!
//! The fabric-primary's mesh is lost the way a caller loses it: a Build deletes the mesh that
//! holds the fabric seat (`{mesh1, mesh2}` -> the other mesh), executed by an admin outside it.
//! Fabric authority leaves with that Build, through the rectifier; the former holder's runtimes
//! end only after, by the delete pipeline's own terminate. While it runs, every survivor admin
//! is asked who holds the fabric and the advertised endpoint is asked at once (the request the
//! original failure refused); each pair is recorded with the run's own map of control endpoints.

use rdm_client::{NodeStatus, NodeView};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::typed::fabric_view_of;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CELL: &str = "surviving_admins_reproduce_primary_loss_with_complete_evidence";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2892".into(),
        subfeature: "fabric-primary-loss".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: CELL.into(),
    }
}

fn acceptance_dir() -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2892/chaos-process").join(CELL),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

fn mesh(name: &str) -> (&str, u32, u32) {
    (name, 2, 3)
}

/// One process of the run, as the control APIs and the host describe it.
async fn describe(estate: &Estate, node: &Value) -> Value {
    let name = s(&node["name"]);
    let pid = match estate.bootstrap_pid() {
        Some(p) if name == "mesh1.admin.1" => u64::from(p),
        _ => estate.pid_of(&name).await,
    };
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let start_token = stat.rsplit(')').next().and_then(|r| r.split_whitespace().nth(19)).unwrap_or("").to_string();
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).map(|b| String::from_utf8_lossy(&b).replace('\0', " ")).unwrap_or_default();
    let data_dir = s(&node["data_dir"]);
    let deployment = std::fs::read_to_string(format!("{data_dir}/deployment.json")).ok().and_then(|t| serde_json::from_str::<Value>(&t).ok());
    json!({
        "name": name, "mesh": node["mesh"], "kind": node["kind"], "node_id": node["node_id"], "incarnation_id": node["incarnation_id"],
        "endpoint_id": node["endpoint_id"], "status": node["status"], "is_primary": node["is_primary"], "is_fabric_primary": node["is_fabric_primary"],
        "admin_api_base": node["admin_api_base"], "transport_addr": node["transport_addr"], "listeners": node["listeners"],
        "data_dir": data_dir, "data_dir_exists": std::path::Path::new(&data_dir).is_dir(), "node_key_present": std::path::Path::new(&format!("{data_dir}/node-key")).is_file(),
        "deployment_json": deployment,
        "host": {"pid": pid, "start_token": start_token, "cmdline": cmdline, "alive": alive(pid)},
    })
}

/// CONTRACT (#2892): when the mesh that holds the fabric is deleted through a Build, the fabric
/// authority every surviving admin advertises is, at every instant it is asked, a control
/// endpoint of a birth of this run (never a port outside the run's data dirs); once the Build
/// is complete the authority is a ready admin of the surviving mesh that answers for this
/// Fabric, from every survivor, repeatedly; the former holder's runtimes are gone only after
/// the surviving admins named the new holder, and each survivor's evidence runs past the loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn surviving_admins_reproduce_primary_loss_with_complete_evidence() {
    let dir = acceptance_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let both = [mesh("mesh1"), mesh("mesh2")];
    estate.reconcile_shape("fabric1", &both).await;
    let before: Vec<Value> = estate.settled_views(&both).await.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect();

    // The seat's holder and the mesh that outlives it.
    let holder = estate.fabric_view().await.fabric_primary.expect("the fabric names its primary").to_string();
    let lost_mesh = holder.split('.').next().unwrap().to_string();
    let survivor_mesh = if lost_mesh == "mesh1" { "mesh2" } else { "mesh1" }.to_string();

    // Every process of the run before the loss: API/control endpoints, data-dir bindings, host.
    let mut described: BTreeMap<String, Value> = BTreeMap::new();
    for n in &before {
        described.insert(s(&n["name"]), describe(&estate, n).await);
    }
    let run_bases: BTreeSet<String> = described.values().filter(|d| d["kind"] == "node_admin").map(|d| s(&d["admin_api_base"])).collect();
    let admin_base_owner: BTreeMap<String, String> = described.values().filter(|d| d["kind"] == "node_admin").map(|d| (s(&d["admin_api_base"]), s(&d["name"]))).collect();
    assert_eq!(run_bases.len(), 4, "four admins, four control endpoints: {run_bases:?}");
    let lost_pids: Vec<(String, u64)> = described.values().filter(|d| d["mesh"] == lost_mesh.as_str()).map(|d| (s(&d["name"]), d["host"]["pid"].as_u64().unwrap())).collect();
    let survivors: Vec<String> = described.values().filter(|d| d["mesh"] == survivor_mesh.as_str() && d["kind"] == "node_admin").map(|d| s(&d["name"])).collect();
    let survivor_pids: BTreeMap<String, u64> = described.values().filter(|d| survivors.contains(&s(&d["name"]))).map(|d| (s(&d["name"]), d["host"]["pid"].as_u64().unwrap())).collect();
    let survivor_bases: BTreeSet<String> = survivors.iter().map(|n| s(&described[n]["admin_api_base"])).collect();
    // Control moves to the surviving mesh's admins before the loss: they execute the delete.
    estate.enter(&Estate::path(&survivors[0])).await;
    let fabric_id = estate.fabric_id.clone();

    // Every survivor is asked, continuously through the loss, who holds the fabric, and the
    // advertised endpoint is asked straight after: the pair the original failure broke.
    let samples: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (samples, stop, fabric_id) = (samples.clone(), stop.clone(), fabric_id.clone());
        let asked: Vec<(String, String)> = survivors.iter().map(|n| (n.clone(), s(&described[n]["admin_api_base"]))).collect();
        let peers = estate.peers().await;
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                for (from, from_base) in &asked {
                    let at = now_ns();
                    let row = match fabric_view_of(&peers, &from.parse().unwrap(), &fabric_id).await {
                        None => json!({"at_unix_nano": at, "asked": from_base, "answered": false}),
                        Some(f) => {
                            let advertised = f.admin_api_base.clone().unwrap_or_default();
                            let holder = f.fabric_primary.as_ref().map(|h| h.to_string()).unwrap_or_default();
                            if advertised.is_empty() {
                                json!({"at_unix_nano": at, "asked": from_base, "answered": true, "fabric_primary": holder, "advertised": null})
                            } else {
                                // The advertised holder is asked straight after, by the name the view gave.
                                let followed = match f.fabric_primary.as_ref() {
                                    Some(h) => fabric_view_of(&peers, h, &fabric_id).await,
                                    None => None,
                                };
                                json!({
                                    "at_unix_nano": at, "asked": from_base, "answered": true, "fabric_primary": holder, "advertised": advertised,
                                    "advertised_answers": followed.is_some(), "advertised_names": followed.as_ref().map(|f| f.fabric_primary.as_ref().map(|h| h.to_string())),
                                    "advertised_agrees": followed.as_ref().is_some_and(|f| f.fabric_primary.as_ref().map(|h| h.to_string()).as_deref() == Some(holder.as_str()) && f.admin_api_base.as_deref() == Some(advertised.as_str())),
                                })
                            }
                        }
                    };
                    samples.lock().unwrap().push(row);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
    };

    // THE HAND-OFF, through the rectifier: one Build retiring the holder's mesh. A mesh.delete is refused
    // until the initial fabric has first reached ready (states.md "The initial fabric before
    // ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let started = now_ns();
    let build_id = estate.reconcile_shape("fabric1", &[mesh(&survivor_mesh)]).await.build_id.to_string();
    let build_complete = now_ns();
    // mesh.deleted leaves the lost mesh's last admin alive; its end is its own node.delete, which the
    // fabric-primary executes and whose TerminateRuntime proves the exit (mesh-delete.md step 7).
    let last = estate
        .until_view("the lost mesh's last admin alive after mesh.deleted, its members deleted", Duration::from_secs(60), |views| {
            let left: Vec<&NodeView> = views.iter().filter(|n| n.mesh == lost_mesh).collect();
            (left.len() == 1 && left[0].status == NodeStatus::Ready).then(|| left[0].name.clone())
        })
        .await;
    let mut delete = estate.nodes_obj().await.delete(&last).await.unwrap_or_else(|e| panic!("the node.delete of the lost mesh's last admin {last} was refused: {e}"));
    Estate::complete(&mut delete).await;
    // The former holder's runtimes: terminal, observed by the host.
    for (name, pid) in &lost_pids {
        assert!(!alive(*pid), "{name}'s runtime {pid} is gone once the mesh.delete and the last admin's node.delete are complete");
    }
    // Authority after the loss: every survivor names one ready holder of the surviving mesh, and
    // that holder answers for this Fabric and agrees, repeatedly.
    let after: Vec<Value> = estate.settled_views(&[(&survivor_mesh, 2, 3)]).await.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect();
    let consistent = std::sync::atomic::AtomicUsize::new(0);
    let agrees = || async {
        for name in &survivors {
            let f = estate.fabric_view_at(&name.parse().unwrap()).await?;
            let (holder, advertised) = (f.fabric_primary.clone()?, f.admin_api_base.clone().unwrap_or_default());
            let own = estate.fabric_view_at(&holder).await?;
            if !holder.to_string().starts_with(&format!("{survivor_mesh}.admin.")) || own.fabric_primary.as_ref() != Some(&holder) || !survivor_bases.contains(&advertised) {
                return None;
            }
        }
        Some(())
    };
    let settled = wait_for("every survivor advertises one live holder that agrees, ten samples running", Duration::from_secs(30), || async {
        match agrees().await {
            Some(()) => (consistent.fetch_add(1, Ordering::SeqCst) + 1 >= 10).then_some(()),
            None => {
                consistent.store(0, Ordering::SeqCst);
                None
            }
        }
    })
    .await;
    let _ = settled;
    stop.store(true, Ordering::SeqCst);
    sampler.await.unwrap();
    let fabric_now = estate.fabric_view().await;
    let new_holder = fabric_now.fabric_primary.as_ref().map(|h| h.to_string()).unwrap_or_default();
    let new_base = fabric_now.admin_api_base.clone().unwrap_or_default();
    let holder_record = after.iter().find(|n| s(&n["name"]) == new_holder).unwrap_or_else(|| panic!("the new holder {new_holder} is a ready admin of the surviving view: {after:#?}")).clone();
    assert_eq!(s(&holder_record["admin_api_base"]), new_base, "the advertised authority is the holder's own control endpoint");
    assert!(survivor_bases.contains(&new_base), "the authority endpoint belongs to a surviving birth of this run: {new_base} not in {survivor_bases:?}");
    let mut survivors_after = BTreeMap::new();
    for n in after.iter().filter(|n| n["kind"] == "node_admin") {
        let name = s(&n["name"]);
        let base = s(&n["admin_api_base"]);
        let fabric_view = estate.fabric_view_at(&name.parse().unwrap()).await.map(|f| serde_json::to_value(f).expect("a view serializes"));
        let nodes_view = estate.node_values_at(&name).await;
        survivors_after.insert(name.clone(), json!({"admin_api_base": base, "node": describe(&estate, n).await, "fabric": fabric_view, "nodes_seen": nodes_view.iter().map(|v| json!({"name": v["name"], "status": v["status"], "is_primary": v["is_primary"], "is_fabric_primary": v["is_fabric_primary"], "admin_api_base": v["admin_api_base"]})).collect::<Vec<_>>()}));
    }

    estate.enter(&Estate::path(&new_holder)).await;
    estate.stop().await;
    let spans = estate.spans();

    // Every pair asked during the loss: an advertised endpoint is always one of the run's own
    // control endpoints, and every refusal is accounted for by the birth that held the port.
    let rows = samples.lock().unwrap().clone();
    let outside: Vec<&Value> = rows.iter().filter(|r| r["advertised"].as_str().is_some_and(|a| !run_bases.contains(a))).collect();
    assert!(outside.is_empty(), "an advertised fabric endpoint is a port of no birth of this run: {outside:#?}");
    let refused: Vec<Value> = rows
        .iter()
        .filter(|r| r["advertised"].as_str().is_some() && r["advertised_answers"] == false)
        .map(|r| json!({"row": r, "port_belonged_to": admin_base_owner.get(&s(&r["advertised"])), "former_holder_mesh": lost_mesh}))
        .collect();
    let after_complete: Vec<&Value> = rows.iter().filter(|r| r["at_unix_nano"].as_u64().unwrap() > build_complete && r["asked"].is_string()).collect();
    assert!(!after_complete.is_empty(), "the survivors were asked after the Build completed");

    // The hand-off precedes the former holder's end: a surviving admin announced the new fabric
    // primary (survivor mesh) before the former holder's runtime was gone (the delete pipeline's terminate step ended).
    let recompute = named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute");
    let handed_off: Vec<&&Value> = recompute.iter().filter(|sp| sp["attributes"]["election_level"] == "fabric_primary" && sp["attributes"]["winner_mesh"] == survivor_mesh.as_str() && s(&sp["attributes"]["previous"]).starts_with(&format!("{lost_mesh}."))).collect();
    assert!(!handed_off.is_empty(), "a surviving admin announced the new fabric primary of {survivor_mesh}, succeeding {lost_mesh}'s");
    let first_handoff = handed_off.iter().map(|sp| sp["start_unix_nano"].as_u64().unwrap()).min().unwrap();
    let steps = named(&spans, "rdm.node_admin.deployment.update.via-step");
    let holder_terminate: Vec<&Value> = steps.iter().copied().filter(|sp| sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["node"] == holder.as_str() && sp["attributes"]["build_id"] == build_id.as_str()).collect();
    assert_eq!(holder_terminate.len(), 1, "the delete pipeline terminated the former holder {holder} once under {build_id}: {holder_terminate:?}");
    assert_eq!(holder_terminate[0]["attributes"]["outcome"], "complete");
    let terminate_began = holder_terminate[0]["start_unix_nano"].as_u64().unwrap();
    let terminated_at = holder_terminate[0]["end_unix_nano"].as_u64().unwrap();
    assert!(first_handoff < terminated_at, "fabric authority moved ({first_handoff}) before the former holder's runtime was gone ({terminated_at}; the terminate began {terminate_began})");

    // Evidence completeness: every survivor's span file runs past the loss, and the files are
    // attributable to the pids the host reported.
    let mut evidence = BTreeMap::new();
    for e in std::fs::read_dir(&estate.evidence).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        let Some(pid) = name.strip_suffix(".spans.jsonl").and_then(|n| n.split('.').nth(1)).and_then(|p| p.split('-').next()).and_then(|p| p.parse::<u64>().ok()) else { continue };
        let lines: Vec<Value> = std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str(l).ok()).collect();
        let last_end = lines.iter().filter_map(|l| l["end_unix_nano"].as_u64()).max().unwrap_or(0);
        evidence.insert(pid, json!({"file": name, "spans": lines.len(), "service": lines.first().map(|l| l["service"].clone()), "last_end_unix_nano": last_end}));
    }
    for (name, pid) in &survivor_pids {
        let ev = evidence.get(pid).unwrap_or_else(|| panic!("survivor {name} (pid {pid}) left no span file: {evidence:#?}"));
        assert!(ev["last_end_unix_nano"].as_u64().unwrap() > build_complete, "survivor {name}'s spans run past the loss ({}), last span ended {}", build_complete, ev["last_end_unix_nano"]);
    }
    let _ = estate.spans();

    let result = json!({
        "cell": CELL,
        "build_id": build_id,
        "former_holder": holder,
        "lost_mesh": lost_mesh,
        "survivor_mesh": survivor_mesh,
        "new_holder": new_holder,
        "primary_endpoint": new_base,
        "started_unix_nano": started,
        "build_complete_unix_nano": build_complete,
        "first_handoff_announcement_unix_nano": first_handoff,
        "former_holder_terminate_began_unix_nano": terminate_began,
        "former_holder_runtime_gone_unix_nano": terminated_at,
        "before": described,
        "survivors_after": survivors_after,
        "survivor_control_endpoints": survivor_bases,
        "samples": {"total": rows.len(), "after_complete": after_complete.len(), "advertised_outside_run": outside.len(), "refused_follow_ups": refused},
        "evidence_by_pid": evidence,
        "lost_runtimes": lost_pids.iter().map(|(n, p)| json!({"name": n, "pid": p, "alive": alive(*p)})).collect::<Vec<_>>(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
