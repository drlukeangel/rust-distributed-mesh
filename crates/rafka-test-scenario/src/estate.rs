//! Blackbox estate harness for the i143 e2e canaries.
//!
//! Everything here goes through public surfaces only (`docs/i143/design.md`):
//! the `rafka-node-admin` process and its control API, the `rafka-rpc-probe`
//! binary and the JSONL evidence files. No internal map is read.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Ownership metadata of one run (mesh product taxonomy, PRD §16).
#[derive(Debug, Clone)]
pub struct Owner {
    pub product: String,
    pub feature: String,
    pub subfeature: String,
    pub rung: String,
    pub provider: String,
    pub test: String,
}

/// `RAFKA_ARTIFACTS_DIR`, else this crate's `tests/artifacts`.
pub fn artifacts_root() -> PathBuf {
    std::env::var("RAFKA_ARTIFACTS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/artifacts"))
}

/// Directory holding the built binaries: `RAFKA_BIN_DIR`, else the cargo
/// target dir's profile directory this test binary was built into.
pub fn bin_dir() -> PathBuf {
    if let Ok(d) = std::env::var("RAFKA_BIN_DIR") {
        return PathBuf::from(d);
    }
    // A test binary lives in target/<profile>/deps; a bin in target/<profile>.
    let exe = std::env::current_exe().expect("own executable path");
    let dir = exe.parent().expect("exe dir").to_path_buf();
    if dir.file_name().is_some_and(|n| n == "deps") {
        dir.parent().expect("target/<profile>").to_path_buf()
    } else {
        dir
    }
}

pub fn binary(name: &str) -> PathBuf {
    let p = bin_dir().join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(
        p.exists(),
        "RED: binary `{name}` is not built at {} — the generic Mesh product does not exist yet \
         (node-admin: i143.e1/e2; rpc node + probe: i143.e7.s3)",
        p.display()
    );
    p
}

/// Poll `check` until it yields `Some`, failing at `deadline` with `what`.
/// Recovery is inferred only from observed state, never from elapsed time.
pub async fn wait_for<T, F, Fut>(what: &str, within: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + within;
    loop {
        if let Some(v) = check().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out after {within:?} waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub struct Estate {
    pub owner: Owner,
    pub root: PathBuf,
    pub artifacts: PathBuf,
    pub evidence: PathBuf,
    pub admin: String,
    bootstrap: Option<Child>,
    http: reqwest::Client,
}

impl Estate {
    /// Start the first node-admin of `fabric` (bootstrap selects the provider)
    /// and wait for its advertised control API base.
    pub async fn bootstrap(owner: Owner, fabric: &str, mesh: &str) -> Self {
        let artifacts = artifacts_root().join(&owner.feature).join(&owner.test);
        let _ = std::fs::remove_dir_all(&artifacts);
        let evidence = artifacts.join("spans");
        std::fs::create_dir_all(&evidence).unwrap();
        let root = std::env::temp_dir().join(format!("i143-{}-{}", owner.test, std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        let mut child = Command::new(binary("rafka-node-admin"))
            .env("MESH_SPAWN_TYPE", &owner.provider)
            .env("RAFKA_FABRIC", fabric)
            .env("RAFKA_MESH", mesh)
            .env("RAFKA_DATA_DIR", root.join(format!("{mesh}.admin.1")))
            .env("RAFKA_BIN_DIR", bin_dir())
            .env("RAFKA_EVIDENCE_DIR", &evidence)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn rafka-node-admin");
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(base) = line.strip_prefix("RAFKA_NODE_ADMIN_API_BASE=") {
                    let _ = tx.send(base.trim().to_string());
                }
            }
        });
        let admin = rx
            .recv_timeout(Duration::from_secs(30))
            .expect("bootstrap node-admin never advertised RAFKA_NODE_ADMIN_API_BASE");
        let estate = Self { owner, root, artifacts, evidence, admin, bootstrap: Some(child), http: reqwest::Client::new() };
        estate.write_manifest();
        estate
    }

    fn write_manifest(&self) {
        let o = &self.owner;
        self.artifact(
            "manifest.json",
            &json!({
                "product": o.product, "feature": o.feature, "subfeature": o.subfeature,
                "rung": o.rung, "provider": o.provider, "test": o.test,
                "seed": null, "control_api": self.admin,
            }),
        );
    }

    pub fn artifact(&self, name: &str, v: &Value) {
        std::fs::write(self.artifacts.join(name), serde_json::to_vec_pretty(v).unwrap()).unwrap();
    }

    pub fn append_ledger(&self, entry: &Value) {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.artifacts.join("rpc-ledger.jsonl"))
            .unwrap();
        writeln!(f, "{entry}").unwrap();
    }

    pub async fn get(&self, path: &str) -> (u16, Value) {
        let r = self.http.get(format!("{}{path}", self.admin)).send().await.expect("control API reachable");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    /// `GET path` on another admin's control API (`base`).
    pub async fn http_get(&self, base: &str, path: &str) -> (u16, Value) {
        let r = self.http.get(format!("{base}{path}")).send().await.expect("control API reachable");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    pub async fn post(&self, path: &str, body: &Value) -> (u16, Value) {
        let r = self.http.post(format!("{}{path}", self.admin)).json(body).send().await.expect("control API reachable");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    pub async fn delete(&self, path: &str) -> (u16, Value) {
        let r = self.http.delete(format!("{}{path}", self.admin)).send().await.expect("control API reachable");
        let status = r.status().as_u16();
        (status, r.json().await.unwrap_or(Value::Null))
    }

    /// Wait until Build `id` is `complete`; a `failed` Build fails the test.
    pub async fn await_build(&self, id: &str, within: Duration) -> Value {
        wait_for(&format!("build {id} complete"), within, || async {
            let (_, b) = self.get(&format!("/api/builds?id={id}")).await;
            match b["state"].as_str() {
                Some("complete") => Some(b),
                Some("failed") => panic!("build {id} failed: {b:#}"),
                _ => None,
            }
        })
        .await
    }

    pub async fn nodes(&self) -> Vec<Value> {
        let (status, v) = self.get("/api/nodes").await;
        assert_eq!(status, 200, "GET /api/nodes: {v}");
        v["nodes"].as_array().cloned().unwrap_or_default()
    }

    /// `GET /api/nodes` from the admin serving `base` (another admin's view).
    pub async fn nodes_at(&self, base: &str) -> Vec<Value> {
        let r = self.http.get(format!("{base}/api/nodes")).send().await.expect("control API reachable");
        assert_eq!(r.status().as_u16(), 200, "GET {base}/api/nodes");
        let v: Value = r.json().await.unwrap_or(Value::Null);
        v["nodes"].as_array().cloned().unwrap_or_default()
    }

    /// The view once every cohort holds exactly its desired count of nodes
    /// (`(mesh, node_admin, rpc_node)`), every one ready for traffic. A
    /// Build's shape is counts, not paths: shrink retires non-primaries, so
    /// which ordinals remain depends on where the seats are.
    pub async fn settled_shape(&self, meshes: &[(&str, u32, u32)], within: Duration) -> Vec<Value> {
        let want: std::collections::BTreeMap<(String, String), usize> = meshes
            .iter()
            .flat_map(|(m, a, r)| [((m.to_string(), "node_admin".to_string()), *a as usize), ((m.to_string(), "rpc_node".to_string()), *r as usize)])
            .collect();
        let until = Instant::now() + within;
        loop {
            let nodes = self.nodes().await;
            let mut have: std::collections::BTreeMap<(String, String), usize> = std::collections::BTreeMap::new();
            for n in &nodes {
                *have.entry((n["mesh"].as_str().unwrap_or("").into(), n["kind"].as_str().unwrap_or("").into())).or_default() += 1;
            }
            if have == want && nodes.iter().all(|n| n["status"] == "ready-for-traffic") {
                return nodes;
            }
            if Instant::now() >= until {
                panic!("the view never settled on {want:?} within {within:?}; last view: {have:?}: {nodes:#?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// `node`'s data dir. Only the admin that launched a node advertises it,
    /// and that is whichever admin executed its creation, so every live
    /// admin's view is asked.
    pub async fn data_dir_of(&self, node: &str) -> String {
        let bases: Vec<String> = std::iter::once(self.admin.clone())
            .chain(self.nodes().await.iter().filter(|n| n["kind"] == "node_admin" && n["status"] == "ready-for-traffic").filter_map(|n| n["admin_api_base"].as_str().map(String::from)))
            .collect();
        for base in bases {
            let Ok(r) = self.http.get(format!("{base}/api/nodes")).timeout(Duration::from_secs(2)).send().await else { continue };
            let v: Value = r.json().await.unwrap_or(Value::Null);
            let dir = v["nodes"].as_array().into_iter().flatten().find(|n| n["name"] == node).and_then(|n| n["data_dir"].as_str().map(String::from));
            if let Some(dir) = dir {
                return dir;
            }
        }
        panic!("no live admin advertises {node}'s data dir")
    }

    /// The pid of `node`'s runtime (process provider: its `deployment.json`).
    pub async fn pid_of(&self, node: &str) -> u64 {
        let dir = self.data_dir_of(node).await;
        let d: Value = serde_json::from_slice(&std::fs::read(format!("{dir}/deployment.json")).unwrap()).unwrap();
        d["pid"].as_u64().unwrap_or_else(|| panic!("{node}: no pid in {d}"))
    }

    /// SIGKILL `node`'s runtime (a fault: it flushes nothing).
    pub async fn kill_node(&self, node: &str) {
        let pid = self.pid_of(node).await;
        let ok = std::process::Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success();
        assert!(ok, "kill -9 {pid} ({node})");
    }

    /// The view once it holds exactly `names`, every one ready for traffic.
    /// A Build can complete on another admin (a mesh's own primary) before
    /// this admin hears the last member it created: the view converges
    /// within gossip delay, and a view that does not within `within` fails.
    pub async fn settled(&self, names: &std::collections::BTreeSet<String>, within: Duration) -> Vec<Value> {
        let deadline = Instant::now() + within;
        loop {
            let nodes = self.nodes().await;
            let have: std::collections::BTreeSet<String> = nodes.iter().filter_map(|n| n["name"].as_str().map(str::to_string)).collect();
            if &have == names && nodes.iter().all(|n| n["status"] == "ready-for-traffic") {
                return nodes;
            }
            if Instant::now() >= deadline {
                let seen: Vec<String> = nodes.iter().map(|n| format!("{}={}", n["name"], n["status"])).collect();
                panic!("the view never settled on {names:?} within {within:?}; last view: {seen:?}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn node(&self, name: &str) -> Value {
        self.nodes().await.into_iter().find(|n| n["name"] == name).unwrap_or_else(|| panic!("no node {name}"))
    }

    /// Run one probe invocation and record it in the RPC ledger.
    pub fn probe(&self, args: &[&str]) -> Value {
        let out = Command::new(binary("rafka-rpc-probe"))
            .arg("--admin")
            .arg(&self.admin)
            .args(args)
            .env("RAFKA_EVIDENCE_DIR", &self.evidence)
            .output()
            .expect("run rafka-rpc-probe");
        let line = String::from_utf8_lossy(&out.stdout);
        let v: Value = serde_json::from_str(line.trim())
            .unwrap_or_else(|e| panic!("probe {args:?} printed no JSON ({e}): {line} / {}", String::from_utf8_lossy(&out.stderr)));
        self.append_ledger(&json!({ "args": args, "result": v }));
        v
    }

    /// Every span every process of this estate wrote.
    pub fn spans(&self) -> Vec<Value> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(&self.evidence).into_iter().flatten().flatten() {
            if e.path().to_string_lossy().ends_with(".spans.jsonl") {
                let text = std::fs::read_to_string(e.path()).unwrap_or_default();
                out.extend(text.lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()));
            }
        }
        out
    }

    /// The trace URL for `trace_id` when a Jaeger query base is configured.
    pub fn record_trace_url(&self, trace_id: &str) {
        let base = std::env::var("JAEGER_QUERY_URL").unwrap_or_else(|_| "http://localhost:16686".into());
        std::fs::write(self.artifacts.join("trace-url.txt"), format!("{base}/trace/{trace_id}\n")).unwrap();
    }

    /// Stop the whole estate through the runtime-administration route of the
    /// admin that holds the fabric now (as `self.admin` sees it: drift
    /// recovery can move the fabric back to a recovered mesh) and wait until
    /// every runtime of the estate has exited. Every process flushes its
    /// evidence on exit, so read `spans()` after this.
    pub async fn stop(&mut self) {
        let (_, fabric) = self.get("/api/fabric").await;
        if let Some(holder) = fabric["admin_api_base"].as_str().filter(|b| !b.is_empty()) {
            self.admin = holder.to_string();
        }
        let _ = self.http.post(format!("{}/api/shutdown", self.admin)).json(&json!({})).send().await;
        if let Some(mut c) = self.bootstrap.take() {
            let _ = tokio::task::spawn_blocking(move || c.wait()).await;
        }
        let until = Instant::now() + Duration::from_secs(30);
        while Instant::now() < until && !self.live_runtimes().is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// SIGKILL the bootstrap admin (a fault: it flushes nothing).
    pub fn kill_bootstrap(&mut self) {
        if let Some(mut c) = self.bootstrap.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Every runtime a provider started for this estate that still runs:
    /// `(data dir, pid)` from each node's `deployment.json` (process provider).
    pub fn live_runtimes(&self) -> Vec<(PathBuf, u32)> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(&self.root).into_iter().flatten().flatten() {
            let Ok(raw) = std::fs::read_to_string(e.path().join("deployment.json")) else { continue };
            let Some(pid) = serde_json::from_str::<Value>(&raw).ok().and_then(|v| v["pid"].as_u64()) else { continue };
            if Path::new(&format!("/proc/{pid}")).exists() && !std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default().contains(") Z ") {
                out.push((e.path(), pid as u32));
            }
        }
        out
    }

    /// [`Self::stop`], consuming the estate.
    pub async fn shutdown(mut self) {
        self.stop().await;
    }
}

impl Drop for Estate {
    fn drop(&mut self) {
        // Whatever still runs after the test (a failed run, or a fabric whose
        // control moved) is stopped here, never left behind.
        let sweep = |e: &Estate| {
            for (_, pid) in e.live_runtimes() {
                let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
            }
        };
        if self.bootstrap.is_none() {
            sweep(self);
        }
        if let Some(mut c) = self.bootstrap.take() {
            // Best-effort fabric shutdown on a failed run, then the bootstrap process.
            if let Some(hostport) = self.admin.strip_prefix("http://") {
                if let Ok(mut s) = std::net::TcpStream::connect(hostport.trim_end_matches('/')) {
                    let _ = s.set_write_timeout(Some(Duration::from_secs(2)));
                    let _ = write!(
                        s,
                        "POST /api/shutdown HTTP/1.1\r\nHost: {hostport}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                    );
                }
            }
            // Give it the time to stop what it started before forcing it.
            let until = Instant::now() + Duration::from_secs(20);
            while Instant::now() < until && matches!(c.try_wait(), Ok(None)) {
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = c.kill();
            let _ = c.wait();
            sweep(self);
        }
    }
}

/// Spans by name.
pub fn named<'a>(spans: &'a [Value], name: &str) -> Vec<&'a Value> {
    spans.iter().filter(|s| s["name"] == name).collect()
}

/// True when `child` descends from `ancestor` by `parent_span_id` links.
pub fn descends_from(spans: &[Value], child: &Value, ancestor: &Value) -> bool {
    let mut cur = child.clone();
    for _ in 0..64 {
        let parent = cur["parent_span_id"].as_str().unwrap_or("");
        if parent.is_empty() {
            return false;
        }
        if parent == ancestor["span_id"].as_str().unwrap_or("-") {
            return true;
        }
        match spans.iter().find(|s| s["span_id"] == parent) {
            Some(p) => cur = p.clone(),
            None => return false,
        }
    }
    false
}
