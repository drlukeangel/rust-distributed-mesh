//! i143.e4.s4 process E2E: the cohort election matrix (PRD §11).
//!
//! For every declared cohort of a mesh, from public surfaces only (each
//! admin's `GET /api/nodes`):
//! - settled -> exactly one primary;
//! - restart a non-primary -> the incumbent does not flap;
//! - grow and shrink of non-primaries -> the incumbent stays;
//! - kill the current primary (SIGKILL) -> exactly one successor, and the
//!   killed path coming back does not take it over;
//! - legal removal of the primary -> exactly one successor;
//! - partition -> a transient split is permitted; heal -> one primary, the
//!   same in every admin's view.
//!
//! "Does not flap" is sampled: the cohort's primary is read every 100 ms for
//! a few seconds after the step settles and must never be anything else.
//!
//! Evidence: every successor is announced by a
//! `rafka.mesh.election.resolve.via-recompute` span naming the cohort, the
//! new primary and the previous one.
//!
//! The partition drops UDP between the two sides' endpoint ports on loopback
//! (`iptables`, as root or through `sudo -n`). Where that is unavailable the
//! partition case is a named skip; `RAFKA_REQUIRE_NETFAULT=1` (CI) makes it a
//! failure.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::{Duration, Instant};

fn owner(test: &str, rung: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "cohort-election".into(),
        rung: rung.into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

type Cohort = (String, String);

fn cohort(mesh: &str, kind: &str) -> Cohort {
    (mesh.into(), kind.into())
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// Each cohort's primaries in one view.
fn primaries(nodes: &[Value]) -> BTreeMap<Cohort, Vec<String>> {
    let mut out: BTreeMap<Cohort, Vec<String>> = BTreeMap::new();
    for n in nodes {
        let e = out.entry((s(&n["mesh"]), s(&n["kind"]))).or_default();
        if n["is_primary"] == true {
            e.push(s(&n["name"]));
        }
    }
    out
}

/// The one primary of `c` in `nodes`, if exactly one.
fn primary_of(nodes: &[Value], c: &Cohort) -> Option<String> {
    match primaries(nodes).get(c).map(Vec::as_slice) {
        Some([one]) => Some(one.clone()),
        _ => None,
    }
}

fn fabric_primary(nodes: &[Value]) -> Option<String> {
    let fp: Vec<String> = nodes.iter().filter(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).collect();
    (fp.len() == 1).then(|| fp[0].clone())
}

fn status_of(nodes: &[Value], name: &str) -> String {
    nodes.iter().find(|n| n["name"] == name).map(|n| s(&n["status"])).unwrap_or_default()
}

async fn build(estate: &Estate, meshes: &[(&str, u32, u32)]) {
    let desired = json!({
        "fabric": "fabric1",
        "meshes": meshes.iter().map(|(m, a, r)| json!({"name": m, "node_admin": a, "rpc_node": r})).collect::<Vec<_>>(),
    });
    let (status, accepted) = estate.post("/api/build", &desired).await;
    assert_eq!(status, 202, "{accepted}");
    estate.await_build(accepted["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
}

async fn accepted_build(estate: &Estate, (status, v): (u16, Value)) {
    assert_eq!(status, 202, "{v}");
    estate.await_build(v["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
}

/// Every cohort of `nodes` has exactly one primary and every node is ready.
fn settled(nodes: &[Value]) -> bool {
    nodes.iter().all(|n| n["status"] == "ready-for-traffic") && primaries(nodes).values().all(|p| p.len() == 1)
}

/// For `hold`, every sample of `base`'s view keeps exactly `want` as the
/// primary of each listed cohort (no flap, never two, never none).
async fn steady(estate: &Estate, base: &str, label: &str, want: &[(Cohort, &str)], hold: Duration) {
    let until = Instant::now() + hold;
    while Instant::now() < until {
        let nodes = estate.nodes_at(base).await;
        let p = primaries(&nodes);
        for (c, name) in want {
            assert_eq!(p.get(c).cloned().unwrap_or_default(), vec![name.to_string()], "{label}: cohort {c:?} flapped: {nodes:#?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// SIGKILL the runtime of `node` (process provider: its `deployment.json`).
fn kill(nodes: &[Value], node: &str) {
    let n = nodes.iter().find(|n| n["name"] == node).unwrap_or_else(|| panic!("no node {node}"));
    let dir = n["data_dir"].as_str().unwrap_or_else(|| panic!("{node} advertises no data dir: {n}"));
    let d: Value = serde_json::from_slice(&std::fs::read(format!("{dir}/deployment.json")).unwrap()).unwrap();
    let pid = d["pid"].as_u64().unwrap_or_else(|| panic!("{node}: no pid in {d}"));
    let ok = Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success();
    assert!(ok, "kill -9 {pid} ({node})");
}

/// Wait until `base` sees `gone` dead and exactly one other primary of `c`.
async fn successor(estate: &Estate, base: &str, c: &Cohort, gone: &str) -> String {
    wait_for(&format!("one successor to {gone}"), Duration::from_secs(30), || async {
        let nodes = estate.nodes_at(base).await;
        let alive_gone = nodes.iter().any(|n| n["name"] == gone && n["status"] != "dead");
        primary_of(&nodes, c).filter(|p| p != gone && !alive_gone)
    })
    .await
}

/// The election span that announced `primary` for `c` after `previous`.
fn announced(spans: &[Value], c: &Cohort, primary: &str, previous: &str) -> bool {
    named(spans, "rafka.mesh.election.resolve.via-recompute").iter().any(|s| {
        let a = &s["attributes"];
        a["mesh"] == c.0.as_str() && a["kind"] == c.1.as_str() && a["primary"] == primary && a["previous"] == previous
    })
}

/// UDP between two sets of loopback ports dropped, until dropped itself.
struct Partition {
    chain: String,
    sudo: bool,
}

impl Partition {
    fn iptables(sudo: bool, args: &[&str]) -> Result<(), String> {
        let mut c = if sudo { Command::new("sudo") } else { Command::new("iptables") };
        if sudo {
            c.args(["-n", "iptables"]);
        }
        let out = c.arg("-w").args(args).output().map_err(|e| e.to_string())?;
        if out.status.success() {
            Ok(())
        } else {
            Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
        }
    }

    /// `Err` names why the host cannot partition (a skip, or a CI failure).
    fn start(a: &[u16], b: &[u16]) -> Result<Self, String> {
        let chain = format!("RAFKA-PART-{}", std::process::id());
        let sudo = Self::iptables(false, &["-L", "INPUT", "-n"]).is_err();
        Self::iptables(sudo, &["-N", &chain]).map_err(|e| format!("iptables unavailable: {e}"))?;
        let p = Self { chain, sudo };
        Self::iptables(sudo, &["-I", "INPUT", "-j", &p.chain]).map_err(|e| format!("iptables: {e}"))?;
        for x in a {
            for y in b {
                for (from, to) in [(x, y), (y, x)] {
                    let (f, t) = (from.to_string(), to.to_string());
                    Self::iptables(sudo, &["-A", &p.chain, "-i", "lo", "-p", "udp", "--sport", &f, "--dport", &t, "-j", "DROP"])
                        .map_err(|e| format!("iptables: {e}"))?;
                }
            }
        }
        Ok(p)
    }
}

impl Drop for Partition {
    fn drop(&mut self) {
        let _ = Self::iptables(self.sudo, &["-D", "INPUT", "-j", &self.chain]);
        let _ = Self::iptables(self.sudo, &["-F", &self.chain]);
        let _ = Self::iptables(self.sudo, &["-X", &self.chain]);
    }
}

fn udp_ports(nodes: &[Value], names: &[String]) -> Vec<u16> {
    let mut out = Vec::new();
    for n in nodes.iter().filter(|n| names.contains(&s(&n["name"]))) {
        for e in n["endpoints"].as_array().unwrap() {
            if e["slot"] != "control" {
                out.push(s(&e["addr"]).rsplit(':').next().unwrap().parse().unwrap());
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_mn_cohort_elects_one_primary_through_the_matrix() {
    let mut estate = Estate::bootstrap(owner("every_mn_cohort_elects_one_primary", "MN"), "fabric1", "mesh1").await;
    let base1 = estate.admin.clone();
    let (rpc, admin) = (cohort("mesh1", "rpc_node"), cohort("mesh1", "node_admin"));
    let hold = Duration::from_secs(4);

    // settled -> exactly one primary per cohort
    build(&estate, &[("mesh1", 2, 3)]).await;
    let nodes = estate.nodes().await;
    assert!(settled(&nodes), "settled MN: {nodes:#?}");
    let rpc_p = primary_of(&nodes, &rpc).unwrap();
    let admin_p = primary_of(&nodes, &admin).unwrap();
    assert_eq!(fabric_primary(&nodes).as_deref(), Some(admin_p.as_str()), "the fabric primary is mesh1's admin primary");

    // restart a non-primary -> no flap
    let other_rpc: Vec<String> = nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["is_primary"] == false).map(|n| s(&n["name"])).collect();
    accepted_build(&estate, estate.post(&format!("/api/nodes/{}/restart", other_rpc[1]), &json!({})).await).await;
    steady(&estate, &base1, "restart non-primary", &[(rpc.clone(), &rpc_p), (admin.clone(), &admin_p)], hold).await;

    // grow, then shrink back -> the incumbent stays
    build(&estate, &[("mesh1", 3, 5)]).await;
    steady(&estate, &base1, "grow", &[(rpc.clone(), &rpc_p), (admin.clone(), &admin_p)], hold).await;
    build(&estate, &[("mesh1", 2, 3)]).await;
    steady(&estate, &base1, "shrink", &[(rpc.clone(), &rpc_p), (admin.clone(), &admin_p)], hold).await;

    // kill the current primary -> one successor; its path coming back does not take over
    kill(&estate.nodes().await, &rpc_p);
    let succ = successor(&estate, &base1, &rpc, &rpc_p).await;
    build(&estate, &[("mesh1", 2, 3)]).await;
    assert_eq!(status_of(&estate.nodes().await, &rpc_p), "ready-for-traffic", "the killed path is recreated");
    steady(&estate, &base1, "the killed primary's path is back", &[(rpc.clone(), &succ), (admin.clone(), &admin_p)], hold).await;

    // legal removal of the primary -> one successor
    accepted_build(&estate, estate.delete(&format!("/api/nodes/{succ}")).await).await;
    let nodes = estate.nodes().await;
    let succ2 = primary_of(&nodes, &rpc).unwrap_or_else(|| panic!("one successor after removing {succ}: {nodes:#?}"));
    assert_ne!(succ2, succ);
    build(&estate, &[("mesh1", 2, 3)]).await;
    steady(&estate, &base1, "after removal", &[(rpc.clone(), &succ2), (admin.clone(), &admin_p)], hold).await;

    // partition -> transient split permitted; heal -> one primary in every view
    let nodes = estate.nodes().await;
    let admin2 = nodes.iter().find(|n| n["kind"] == "node_admin" && n["name"] != admin_p.as_str()).unwrap();
    let (admin2_name, base2) = (s(&admin2["name"]), s(&admin2["admin_api_base"]));
    let lone_rpc = nodes.iter().find(|n| n["kind"] == "rpc_node" && n["name"] != succ2.as_str()).map(|n| s(&n["name"])).unwrap();
    let side_a = vec![admin_p.clone(), lone_rpc.clone()];
    let side_b: Vec<String> = nodes.iter().map(|n| s(&n["name"])).filter(|n| !side_a.contains(n)).collect();
    match Partition::start(&udp_ports(&nodes, &side_a), &udp_ports(&nodes, &side_b)) {
        Err(why) if std::env::var("RAFKA_REQUIRE_NETFAULT").as_deref() == Ok("1") => {
            panic!("RAFKA_REQUIRE_NETFAULT=1 but this host cannot partition: {why}")
        }
        Err(why) => eprintln!("SKIP partition case: {why}"),
        Ok(partition) => {
            // Each side elects from what it can hear: the split is visible.
            wait_for("side A elects its own rpc primary", Duration::from_secs(30), || async {
                (primary_of(&estate.nodes_at(&base1).await, &rpc).as_deref() == Some(lone_rpc.as_str())).then_some(())
            })
            .await;
            wait_for("side B elects its own admin primary", Duration::from_secs(30), || async {
                (primary_of(&estate.nodes_at(&base2).await, &admin).as_deref() == Some(admin2_name.as_str())).then_some(())
            })
            .await;
            for base in [&base1, &base2] {
                for (c, p) in primaries(&estate.nodes_at(base).await) {
                    assert!(p.len() <= 1, "{base}: a view never names two primaries of {c:?}: {p:?}");
                }
            }
            drop(partition);
            for base in [&base1, &base2] {
                wait_for(&format!("{base} heals to one primary per cohort"), Duration::from_secs(60), || async {
                    let nodes = estate.nodes_at(base).await;
                    (settled(&nodes)
                        && primary_of(&nodes, &rpc).as_deref() == Some(succ2.as_str())
                        && primary_of(&nodes, &admin).as_deref() == Some(admin_p.as_str()))
                    .then_some(())
                })
                .await;
            }
            steady(&estate, &base2, "healed", &[(rpc.clone(), &succ2), (admin.clone(), &admin_p)], hold).await;
        }
    }

    estate.artifact("nodes.json", &json!(estate.nodes().await));
    estate.stop().await;
    let spans = estate.spans();
    assert!(announced(&spans, &rpc, &succ, &rpc_p), "the successor to the killed {rpc_p} was announced");
    assert!(announced(&spans, &rpc, &succ2, &succ), "the successor to the removed {succ} was announced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_meshs_admin_cohort_elects_without_moving_the_fabric() {
    let mut estate = Estate::bootstrap(owner("a_second_meshs_admin_cohort_elects", "MM"), "fabric1", "mesh1").await;
    let base1 = estate.admin.clone();
    let admin2 = cohort("mesh2", "node_admin");
    let hold = Duration::from_secs(4);

    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    let nodes = estate.nodes().await;
    assert!(settled(&nodes), "settled MM: {nodes:#?}");
    let fp = fabric_primary(&nodes).unwrap();
    let p = primary_of(&nodes, &admin2).unwrap();
    let mut cohorts: BTreeSet<Cohort> = BTreeSet::new();
    cohorts.extend(primaries(&nodes).into_keys());
    assert_eq!(cohorts.len(), 4, "four declared cohorts");

    // kill the mesh2 admin primary -> one successor; the fabric primary stays
    kill(&nodes, &p);
    let succ = successor(&estate, &base1, &admin2, &p).await;
    assert_eq!(fabric_primary(&estate.nodes().await).as_deref(), Some(fp.as_str()));
    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    steady(&estate, &base1, "mesh2's killed admin is back", &[(admin2.clone(), &succ)], hold).await;

    // legal removal of the mesh2 admin primary -> one successor
    accepted_build(&estate, estate.delete(&format!("/api/nodes/{succ}")).await).await;
    let nodes = estate.nodes().await;
    let succ2 = primary_of(&nodes, &admin2).unwrap_or_else(|| panic!("one successor after removing {succ}: {nodes:#?}"));
    assert_ne!(succ2, succ);
    assert_eq!(fabric_primary(&nodes).as_deref(), Some(fp.as_str()));
    build(&estate, &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    steady(&estate, &base1, "mesh2 after removal", &[(admin2.clone(), &succ2)], hold).await;

    estate.stop().await;
    let spans = estate.spans();
    assert!(announced(&spans, &admin2, &succ, &p), "the successor to the killed {p} was announced");
    assert!(announced(&spans, &admin2, &succ2, &succ), "the successor to the removed {succ} was announced");
}
