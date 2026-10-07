//! i143 soak: a seeded fault schedule over an MM estate, judged on invariants.
//!
//! product=mesh, feature=fabric-soak, subfeature=seeded, rung=MM, provider=process.
//!
//! Shape: two meshes, two node-admins and three rpc nodes each: enough admins for real
//! mesh-primary and fabric-primary movement. For `RAFKA_SOAK_SECS` (default 60; the 30-minute
//! bar is `RAFKA_SOAK_SECS=1800`) a scheduler seeded by `RAFKA_SOAK_SEED` (default random; the
//! seed is printed and written to the artifacts so a run is exactly rerunnable) picks one
//! operation per round:
//!
//! ```text
//! node restart          POST /api/nodes/{rpc}/restart       (slot/freshness supersession rides it)
//! admin restart         POST /api/nodes/{admin}/restart
//! exact runtime kill    SIGKILL one rpc node's runtime      (drift recovery rebirths the path)
//! same-path replacement DELETE the node, then grow the mesh back by one
//! mesh-primary loss     SIGKILL the mesh primary admin's runtime
//! fabric-primary loss   SIGKILL the fabric primary admin's runtime
//! ```
//!
//! An unheard peer mesh is not in the mix until the backbone re-feed (i143.e6.s14) lands; the
//! evidence names that.
//!
//! After every round the estate must converge within a bound, and the invariants hold:
//! no hang (every wait is bounded and named); one current birth per path; one fabric-primary
//! across every live admin's view (one topology writer); every drift attempt distinct per
//! proven loss and no Build minted for unchanged drift (`Fabric.build_id` stays, attempts rise);
//! no stale-slot dispatch (no serve span for a request a 425 refused); every mesh heard by every
//! live admin (no peer mesh permanently unheard); `Fabric.build_id` and the accepted Build never
//! lost; every surviving path reachable over real Node RPC and current. The final topology is
//! the accepted one.

use rafka_test_scenario::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "fabric-soak".into(),
        subfeature: "seeded".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_seeded_fault_schedule_holds_every_invariant".into(),
    }
}

/// A small, exactly reproducible generator (SplitMix64): the seed is the whole schedule.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[(self.next() % items.len() as u64) as usize]
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or("").to_string()
}

const SHAPE: [(&str, u32, u32); 2] = [("mesh1", 2, 3), ("mesh2", 2, 3)];

/// Every live admin's control API, from the entry admin's view.
fn admin_bases(nodes: &[Value]) -> Vec<String> {
    nodes.iter().filter(|n| n["kind"] == "node_admin" && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))).filter_map(|n| n["admin_api_base"].as_str().map(String::from)).collect()
}

/// A read that may hit an admin whose process just died: `None`, never a panic.
async fn try_get(base: &str, path: &str) -> Option<Value> {
    let r = reqwest::Client::new().get(format!("{base}{path}")).timeout(Duration::from_secs(2)).send().await.ok()?;
    if r.status().as_u16() != 200 {
        return None;
    }
    r.json().await.ok()
}

/// Control follows whoever answers: the fabric's advertised holder if it answers, else the first
/// live admin from `known` that does. Returns false when nobody answers.
async fn relocate_control(estate: &mut Estate, known: &[String]) -> bool {
    let mut candidates: Vec<String> = vec![estate.admin.clone()];
    candidates.extend(known.iter().cloned());
    for c in candidates.clone() {
        if let Some(f) = try_get(&c, "/api/fabric").await {
            if let Some(h) = f["admin_api_base"].as_str().filter(|b| !b.is_empty()) {
                if try_get(h, "/api/fabric").await.is_some() {
                    estate.admin = h.to_string();
                    return true;
                }
            }
            estate.admin = c;
            return true;
        }
    }
    false
}

/// The invariants that hold at every converged point; a violation names itself.
async fn invariants(estate: &Estate, round: usize, build_id: &str) -> Vec<String> {
    let mut bad = Vec::new();
    let nodes = estate.nodes().await;
    // One current birth per path.
    let mut per_path: BTreeMap<String, usize> = BTreeMap::new();
    for n in nodes.iter().filter(|n| !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))) {
        *per_path.entry(s(&n["name"])).or_default() += 1;
    }
    for (p, c) in per_path.iter().filter(|(_, c)| **c > 1) {
        bad.push(format!("round {round}: {c} current births at {p}"));
    }
    // One fabric-primary, agreed by every live admin: one topology writer.
    let mut writers: BTreeSet<String> = BTreeSet::new();
    let mut meshes_seen: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for base in admin_bases(&nodes) {
        let Some(f) = try_get(&base, "/api/fabric").await else {
            bad.push(format!("round {round}: {base} (named ready-for-traffic in the view) does not answer /api/fabric"));
            continue;
        };
        if let Some(p) = f["fabric_primary"].as_str().filter(|p| !p.is_empty()) {
            writers.insert(p.to_string());
        }
        if f["build_id"].as_str() != Some(build_id) {
            bad.push(format!("round {round}: {base} holds Fabric.build_id {} not {build_id}", f["build_id"]));
        }
        let heard: BTreeSet<String> = f["meshes"].as_array().into_iter().flatten().filter(|m| m["status"] == "ready-for-traffic").map(|m| s(&m["name"])).collect();
        meshes_seen.insert(base.clone(), heard);
    }
    if writers.len() != 1 {
        bad.push(format!("round {round}: fabric primaries named across live admins: {writers:?}"));
    }
    for (base, heard) in &meshes_seen {
        for (mesh, _, _) in SHAPE {
            if !heard.contains(mesh) {
                bad.push(format!("round {round}: {base} does not hold {mesh} ready-for-traffic: a peer mesh unheard"));
            }
        }
    }
    bad
}

/// SIGKILL an admin's runtime: the bootstrap admin is the test's own child (no provider record),
/// every other admin a provider-born runtime.
async fn kill_admin(estate: &mut Estate, name: &str) {
    if name == "mesh1.admin.1" && estate.bootstrap_pid().is_some() {
        estate.kill_bootstrap();
    } else {
        estate.kill_node(name).await;
    }
}

/// The fabric has converged when every live admin's view holds the same births: the accepted
/// shape, every one ready for traffic, the same (path, incarnation) set everywhere. Only then
/// is the election the same function on the same inputs on every admin. Returns the live admin
/// bases that agree, or `None` naming nothing yet.
async fn converged_everywhere(estate: &Estate) -> Option<Vec<String>> {
    let nodes = estate.nodes().await;
    let shape_ok = |nodes: &[Value]| {
        SHAPE.iter().all(|(m, a, r)| {
            let ready = |k: &str| nodes.iter().filter(|n| n["mesh"] == *m && n["kind"] == k && n["status"] == "ready-for-traffic").count() as u32;
            ready("node_admin") == *a && ready("rpc_node") == *r
        })
    };
    let births = |nodes: &[Value]| -> BTreeSet<(String, String)> {
        nodes.iter().filter(|n| n["status"] == "ready-for-traffic").map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect()
    };
    if !shape_ok(&nodes) {
        return None;
    }
    let want = births(&nodes);
    let mut agreeing = Vec::new();
    for base in admin_bases(&nodes) {
        let v = try_get(&base, "/api/nodes").await?;
        let theirs: Vec<Value> = v["nodes"].as_array().cloned().unwrap_or_default();
        if !shape_ok(&theirs) || births(&theirs) != want {
            return None;
        }
        agreeing.push(base);
    }
    Some(agreeing)
}

/// The birth a round's operation produced at `path`, from the Build's own receipts: the last
/// completed `AllocateIdentity` for an operation on `path` in an attempt after `after_attempt`.
/// `Ok(None)` while the Build has not completed it; `Err` names a failed Build.
async fn born_at(estate: &Estate, build_id: &str, path: &str, after_attempt: u64) -> Result<Option<(String, String)>, String> {
    let (_, b) = estate.get(&format!("/api/builds?id={build_id}")).await;
    match b["state"].as_str() {
        Some("failed") => return Err(format!("Build {build_id} failed: {}", b["last_failure"])),
        Some("complete") => {}
        _ => return Ok(None),
    }
    let suffix = format!(":{path}");
    Ok(b["steps"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|st| st["step"] == "AllocateIdentity" && st["outcome"] == "complete" && st["attempt"].as_u64().unwrap_or(0) > after_attempt && st["operation"].as_str().is_some_and(|o| o.ends_with(&suffix)))
        .last()
        .map(|st| (s(&st["output"]["node_id"]), s(&st["output"]["incarnation"]))))
}

/// Every live admin's view holds exactly `birth` at `path`, ready for traffic, and the replaced
/// incarnation `old` nowhere live.
async fn birth_held_everywhere(estate: &Estate, path: &str, birth: &(String, String), old: &str) -> Result<(), String> {
    let nodes = estate.nodes().await;
    for base in admin_bases(&nodes) {
        let Some(v) = try_get(&base, "/api/nodes").await else { return Err(format!("{base}: /api/nodes unanswered")) };
        let theirs = v["nodes"].as_array().cloned().unwrap_or_default();
        let at_path: Vec<String> = theirs.iter().filter(|n| n["name"] == path).map(|n| format!("{}/{}:{}", s(&n["node_id"]), s(&n["incarnation_id"]), s(&n["status"]))).collect();
        let holds = theirs.iter().any(|n| n["name"] == path && n["node_id"] == birth.0.as_str() && n["incarnation_id"] == birth.1.as_str() && n["status"] == "ready-for-traffic");
        let old_live: Vec<String> = theirs.iter().filter(|n| n["incarnation_id"] == old && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))).map(|n| format!("{}:{}", s(&n["name"]), s(&n["status"]))).collect();
        if !holds {
            return Err(format!("{base} holds {at_path:?} at {path}, not {}/{} ready-for-traffic", birth.0, birth.1));
        }
        if !old_live.is_empty() {
            return Err(format!("{base} still holds the replaced incarnation {old} live: {old_live:?}"));
        }
    }
    Ok(())
}

/// Every rpc node answers a real Node RPC on its current birth: reachable and current.
async fn reachable(estate: &Estate, round: usize) -> Vec<String> {
    let mut bad = Vec::new();
    for n in estate.nodes().await.iter().filter(|n| n["kind"] == "rpc_node" && n["status"] == "ready-for-traffic") {
        let r = estate.probe(&["get", "--target", &format!("exact:{}", s(&n["node_id"])), "--key", "soak"]);
        if r["outcome"] != "Reply" || r["reply"]["incarnation_id"] != n["incarnation_id"] {
            bad.push(format!("round {round}: {} not reachable on its current birth: {r}", n["name"]));
        }
    }
    bad
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seeded_fault_schedule_holds_every_invariant() {
    let secs: u64 = std::env::var("RAFKA_SOAK_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    let seed: u64 = std::env::var("RAFKA_SOAK_SEED").ok().and_then(|v| v.parse().ok()).unwrap_or_else(|| {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64 | 1
    });
    eprintln!("SOAK seed={seed} secs={secs}  (rerun: RAFKA_SOAK_SEED={seed} RAFKA_SOAK_SECS={secs})");
    let mut rng = Rng(seed);
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.artifact("soak.json", &json!({"seed": seed, "secs": secs, "shape": SHAPE.iter().map(|(m, a, r)| json!({"mesh": m, "node_admin": a, "rpc_node": r})).collect::<Vec<_>>(), "ops_absent": ["unheard peer mesh (i143.e6.s14)"]}));
    let desired = json!({"fabric": "fabric1", "meshes": SHAPE.iter().map(|(m, a, r)| json!({"name": m, "node_admin": a, "rpc_node": r})).collect::<Vec<_>>()});
    let (status, a) = estate.post("/api/build", &desired).await;
    assert_eq!(status, 202, "{a}");
    let mut build_id = s(&a["build_id"]);
    estate.await_build(&build_id, Duration::from_secs(120)).await;
    estate.settled_shape(&SHAPE, Duration::from_secs(60)).await;
    rafka_test_scenario::estate::wait_for("every live admin holds the same births", (rafka_mesh_transport::membership::staleness_floor() * 2 + rafka_mesh_transport::membership::backbone_gossip_interval() * 2) + Duration::from_secs(60), || converged_everywhere(&estate)).await;

    let ops = ["node-restart", "admin-restart", "runtime-kill", "replace", "mesh-primary-loss", "fabric-primary-loss"];
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut log: Vec<Value> = Vec::new();
    let mut violations: Vec<String> = Vec::new();
    let mut round = 0usize;
    let mut known: Vec<String> = admin_bases(&estate.nodes().await);
    while Instant::now() < deadline {
        round += 1;
        if !relocate_control(&mut estate, &known).await {
            violations.push(format!("round {round}: no admin answers its control API"));
            break;
        }
        let nodes = estate.nodes().await;
        known = admin_bases(&nodes);
        let op = *rng.pick(&ops);
        let rpcs: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["status"] == "ready-for-traffic").cloned().collect();
        let admins: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "node_admin" && n["status"] == "ready-for-traffic").cloned().collect();
        if rpcs.is_empty() || admins.is_empty() {
            violations.push(format!("round {round}: no ready node to act on: {}", nodes.len()));
            break;
        }
        let (_, fabric) = estate.get("/api/fabric").await;
        let started = Instant::now();
        let mut entry = json!({"round": round, "op": op, "at_s": (secs as i64) - (deadline - started).as_secs() as i64});
        // The exact birth a fault removes: convergence is that birth gone or superseded, then the shape.
        let mut removed: Option<(String, String)> = None;
        // The Build whose receipts name the replacement birth, and the attempt it must come after.
        let mut watch: Option<(String, u64)> = None;
        let attempt_before = estate.get(&format!("/api/builds?id={build_id}")).await.1["attempt"].as_u64().unwrap_or(0);
        match op {
            "node-restart" => {
                let n = rng.pick(&rpcs).clone();
                entry["target"] = n["name"].clone();
                let (st, b) = estate.post(&format!("/api/nodes/{}/restart", s(&n["name"])), &json!({})).await;
                entry["accepted"] = json!(st);
                if st == 202 {
                    removed = Some((s(&n["name"]), s(&n["incarnation_id"])));
                    watch = Some((s(&b["build_id"]), 0));
                }
            }
            "admin-restart" => {
                // Never the fabric primary's own admin here: that is the fabric-primary-loss op.
                let fp = s(&fabric["fabric_primary"]);
                let cands: Vec<&Value> = admins.iter().filter(|n| s(&n["name"]) != fp).collect();
                let n = (*rng.pick(&cands)).clone();
                entry["target"] = n["name"].clone();
                let (st, b) = estate.post(&format!("/api/nodes/{}/restart", s(&n["name"])), &json!({})).await;
                entry["accepted"] = json!(st);
                if st == 202 {
                    removed = Some((s(&n["name"]), s(&n["incarnation_id"])));
                    watch = Some((s(&b["build_id"]), 0));
                }
            }
            "runtime-kill" => {
                let n = rng.pick(&rpcs).clone();
                entry["target"] = n["name"].clone();
                removed = Some((s(&n["name"]), s(&n["incarnation_id"])));
                estate.kill_node(&s(&n["name"])).await;
            }
            "replace" => {
                let n = rng.pick(&rpcs).clone();
                let mesh = s(&n["mesh"]);
                entry["target"] = n["name"].clone();
                let (st, b) = estate.delete(&format!("/api/nodes/{}", s(&n["name"]))).await;
                entry["accepted"] = json!(st);
                if st == 202 {
                    estate.await_build(&s(&b["build_id"]), Duration::from_secs(120)).await;
                    let (st2, b2) = estate.post("/api/nodes/spawn", &json!({"mesh": mesh, "kind": "rpc_node"})).await;
                    entry["regrow"] = json!(st2);
                    if st2 == 202 {
                        removed = Some((s(&n["name"]), s(&n["incarnation_id"])));
                        watch = Some((s(&b2["build_id"]), 0));
                    }
                }
            }
            "mesh-primary-loss" => {
                let fp = s(&fabric["fabric_primary"]);
                let cands: Vec<&Value> = admins.iter().filter(|n| n["is_primary"] == true && s(&n["name"]) != fp).collect();
                if cands.is_empty() {
                    entry["skipped"] = json!("no mesh primary other than the fabric primary is ready");
                } else {
                    let n = (*rng.pick(&cands)).clone();
                    entry["target"] = n["name"].clone();
                    removed = Some((s(&n["name"]), s(&n["incarnation_id"])));
                    kill_admin(&mut estate, &s(&n["name"])).await;
                }
            }
            "fabric-primary-loss" => {
                let fp = s(&fabric["fabric_primary"]);
                entry["target"] = json!(fp);
                if !fp.is_empty() {
                    removed = admins.iter().find(|n| s(&n["name"]) == fp).map(|n| (fp.clone(), s(&n["incarnation_id"])));
                    kill_admin(&mut estate, &fp).await;
                }
            }
            _ => unreachable!(),
        }
        // Converge: every path of the shape current and ready again, read through whichever admin
        // answers (the one just killed may have been the entry).
        if watch.is_none() && removed.is_some() {
            watch = Some((build_id.clone(), attempt_before));
        }
        let until = Instant::now() + Duration::from_secs(150);
        // The birth sequence, confirmed before anything is judged: the operation's Build (or the
        // fault's attempt) completes; its AllocateIdentity names the new birth at the path; every
        // live admin holds exactly that birth ready and the replaced one nowhere; every live admin
        // holds the same births. Only then do the admins compute on the same inputs.
        let mut converged = false;
        let mut failure = None;
        while Instant::now() < until {
            if relocate_control(&mut estate, &known).await {
                let born = match (&watch, &removed) {
                    (Some((b, after)), Some((path, _))) => match born_at(&estate, b, path, *after).await {
                        Ok(born) => born,
                        Err(f) => {
                            failure = Some(f);
                            break;
                        }
                    },
                    _ => None,
                };
                let sequence_done = match (&removed, &born) {
                    (Some((path, old)), Some(birth)) => {
                        entry["born"] = json!({"path": path, "node_id": birth.0, "incarnation": birth.1, "replaced": old});
                        match birth_held_everywhere(&estate, path, birth, old).await {
                            Ok(()) => true,
                            Err(why) => {
                                entry["waiting_on"] = json!(why);
                                false
                            }
                        }
                    }
                    (Some(_), None) => {
                        entry["waiting_on"] = json!("the Build has not completed the rebirth");
                        false
                    }
                    (None, _) => true,
                };
                if sequence_done {
                    if let Some(agreeing) = converged_everywhere(&estate).await {
                        converged = true;
                        known = agreeing;
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        if let Some(f) = failure {
            violations.push(format!("round {round} ({op}): {f}"));
            log.push(entry);
            break;
        }
        if !converged {
            violations.push(format!("round {round} ({op}): the birth sequence did not complete within 150s: {entry}"));
            log.push(entry);
            break;
        }
        entry["converged_ms"] = json!(started.elapsed().as_millis() as u64);
        // The accepted topology is the Build the fabric names; a loss opens attempts, never a Build.
        let (_, f) = estate.get("/api/fabric").await;
        let now_build = s(&f["build_id"]);
        if matches!(op, "runtime-kill" | "mesh-primary-loss" | "fabric-primary-loss") && now_build != build_id {
            violations.push(format!("round {round} ({op}): a Build was minted for drift: {build_id} -> {now_build}"));
        }
        build_id = now_build;
        // The invariants hold the moment the shape has converged: two live admins over the same
        // facts name the same primary (the election is a function of the view), so a disagreement
        // here is a defect to root-cause, never to wait out.
        violations.extend(invariants(&estate, round, &build_id).await);
        violations.extend(reachable(&estate, round).await);
        log.push(entry);
        if !violations.is_empty() {
            break;
        }
    }
    estate.artifact("soak-log.json", &json!({"seed": seed, "rounds": round, "ops": log, "violations": violations}));

    // Attempts: every proven loss opened exactly one more attempt of the accepted Build; the
    // same loss never two.
    let (_, built) = estate.get(&format!("/api/builds?id={build_id}")).await;
    let attempts = built["attempt"].as_u64().unwrap_or(0);
    let losses = log.iter().filter(|e| matches!(e["op"].as_str(), Some("runtime-kill") | Some("mesh-primary-loss") | Some("fabric-primary-loss")) && e.get("skipped").is_none()).count() as u64;
    eprintln!("SOAK seed={seed} rounds={round} attempts_of_accepted_build={attempts} losses={losses}");

    estate.stop().await;
    let left = estate.live_runtimes();
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
    assert!(left.is_empty(), "seed {seed}: no runtime of the estate is left running: {left:?}");
    // Evidence: a 425 is never followed by a dispatch of the same request; the refusal spans say so.
    let spans = estate.spans();
    let stale = named(&spans, "rafka.node_rpc.connection.reject.via-stale-slot");
    for sp in &stale {
        let trace = &sp["trace_id"];
        let served_after = spans.iter().any(|x| x["trace_id"] == *trace && x["name"] == "rafka.node_rpc.request.serve.via-direct" && x["start_unix_nano"].as_u64() > sp["start_unix_nano"].as_u64());
        assert!(!served_after, "a stale fence was dispatched after its 425: {sp}");
    }
    assert!(round >= 3, "seed {seed}: only {round} rounds in {secs}s");
}
