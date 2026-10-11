//! The fault door of a faulted node-admin, as a scenario drives it (i143.e8.s2, #2780): found by
//! the path the admin recorded it at (`<root>/faults/<name>.door`), armed, observed through
//! `GET /faults` and released. Shared by every cell that holds a real admin at a named cut.

use crate::estate::{bin_dir, wait_for};
use rafka_node_admin_client::binding::{sha256_file, Binding, BindingSet, Candidate};
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

/// One faulted admin's door (`rafka_node_rpc_testkit::admin_faults::router`), found by the path
/// the admin recorded it at.
#[derive(Clone)]
pub struct Door {
    pub name: String,
    /// The fault door.
    pub base: String,
    /// The admin's control API: the Build state it holds is the one it wrote.
    pub api: String,
    pub http: reqwest::Client,
}

impl Door {
    pub async fn open(root: &Path, name: &str, api: &str) -> Door {
        let file = root.join("faults").join(format!("{name}.door"));
        let base = wait_for(&format!("{name} records its fault door at {}", file.display()), Duration::from_secs(60), || async { std::fs::read_to_string(&file).ok().filter(|b| !b.is_empty()) }).await;
        Door { name: name.into(), base, api: api.into(), http: reqwest::Client::new() }
    }

    pub async fn arm(&self, id: &str, spec: Value) -> Value {
        let mut body = spec;
        body["id"] = json!(id);
        let r = self.http.post(format!("{}/faults/arm", self.base)).json(&body).send().await.unwrap_or_else(|e| panic!("{}: arm {id}: {e}", self.name));
        let status = r.status().as_u16();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        assert_eq!(status, 200, "{}: arming `{id}` is acknowledged: {v}", self.name);
        assert_eq!(v["armed"], id, "{v}");
        v
    }

    pub async fn state(&self) -> Value {
        let r = self.http.get(format!("{}/faults", self.base)).send().await.unwrap_or_else(|e| panic!("{}: fault state: {e}", self.name));
        r.json().await.unwrap()
    }

    pub async fn cut(&self, id: &str) -> Value {
        self.state().await["cuts"][id].clone()
    }

    /// The acknowledgement that the injection is active: the cut holds a call, and which.
    pub async fn wait_held(&self, id: &str) -> Value {
        wait_for(&format!("{}: cut `{id}` holds a call", self.name), Duration::from_secs(90), || async {
            let c = self.cut(id).await;
            (c["held"] == true && !c["hit"].is_null()).then_some(c)
        })
        .await
    }

    pub async fn release(&self, id: &str) -> Value {
        let r = self.http.post(format!("{}/faults/release", self.base)).json(&json!({"id": id})).send().await.unwrap_or_else(|e| panic!("{}: release {id}: {e}", self.name));
        let status = r.status().as_u16();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        assert_eq!(status, 200, "{}: releasing `{id}` is acknowledged: {v}", self.name);
        assert_eq!(v["released"], id, "{v}");
        v
    }
}

/// The candidate SHA: `I143_CANDIDATE_SHA`, else this checkout's HEAD.
pub fn candidate_sha() -> String {
    if let Ok(s) = std::env::var("I143_CANDIDATE_SHA") {
        return s;
    }
    let out = std::process::Command::new("git").args(["rev-parse", "HEAD"]).current_dir(env!("CARGO_MANIFEST_DIR")).output().expect("git rev-parse HEAD");
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

/// The executable `name` in `dir`, built, present and built from the source this tree holds now
/// ([`crate::estate::binary_is_fresh`]): a stale one is refused by name with its rebuild command.
pub fn executable_in(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    assert!(p.exists(), "RED: executable `{name}` is not built at {} (cargo build -p rafka-node-rpc-testkit --bins)", p.display());
    if let Err(why) = crate::estate::binary_is_fresh(name, &p) {
        panic!("{why}");
    }
    p.canonicalize().unwrap()
}

/// The estate's executables: the faulted node-admin, and the testkit rpc node the admin launches.
pub fn binding_set(sha: &str) -> BindingSet {
    let exe = |name: &str| executable_in(&bin_dir(), name);
    let bind = |id: &str, name: &str| {
        let path = exe(name);
        Binding { launch_id: id.into(), sha256: sha256_file(&path).unwrap(), executable: path, image: None }
    };
    BindingSet {
        candidate: Candidate { sha: sha.into(), build: "rafka-node-rpc-testkit".into() },
        launch_ids: vec!["node_admin".into(), "rpc_node".into()],
        bindings: vec![bind("node_admin", "faulted-node-admin"), bind("rpc_node", "rafka-rpc-node")],
    }
}

/// [`binding_set`] with the product role executables bound too: the broker, gateway and compute
/// binaries the node base builds, launched as `broker`, `gateway` and `compute`.
pub fn binding_set_with_roles(sha: &str) -> BindingSet {
    let mut set = binding_set(sha);
    for role in ["broker", "gateway", "compute"] {
        let path = bin_dir().join(format!("rafka-{role}"));
        assert!(path.exists(), "RED: executable `rafka-{role}` is not built at {} (cargo build -p rafka-broker -p rafka-gateway -p rafka-compute)", path.display());
        let path = path.canonicalize().unwrap();
        set.launch_ids.push(role.into());
        set.bindings.push(Binding { launch_id: role.into(), sha256: sha256_file(&path).unwrap(), executable: path, image: None });
    }
    set
}

#[cfg(test)]
mod tests {
    use super::*;

    // CONTRACT: the faulted estate's executables are judged fresh like every other binary an estate
    // launches (`estate::binary_is_fresh`): a faulted-node-admin older than a source file its dep-info
    // lists is refused by name with its rebuild command, never launched as this tree's code.
    #[test]
    fn a_faulted_node_admin_older_than_its_source_is_refused_by_name() {
        let dir = std::env::temp_dir().join(format!("rdm-stale-faulted-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (src, exe) = (dir.join("leave.rs"), dir.join("faulted-node-admin"));
        std::fs::write(&src, "fn main() {}").unwrap();
        std::fs::write(&exe, "bin").unwrap();
        std::fs::write(exe.with_extension("d"), format!("{}: {}\n", exe.display(), src.display())).unwrap();
        let set = |path: &std::path::Path, secs: u64| std::fs::OpenOptions::new().write(true).open(path).unwrap().set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs)).unwrap();
        set(&exe, 2_000);
        set(&src, 1_000);
        assert_eq!(executable_in(&dir, "faulted-node-admin"), exe.canonicalize().unwrap(), "a fresh executable is launched");
        set(&src, 3_000);
        let refused = std::panic::catch_unwind(|| executable_in(&dir, "faulted-node-admin"));
        let _ = std::fs::remove_dir_all(&dir);
        let why = match refused {
            Ok(p) => panic!("a stale faulted-node-admin was handed to the estate: {}", p.display()),
            Err(e) => e.downcast_ref::<String>().cloned().unwrap_or_default(),
        };
        assert!(why.contains("REFUSED: faulted-node-admin") && why.contains("leave.rs") && why.contains("cargo build -p rafka-node-rpc-testkit --bin faulted-node-admin"), "{why}");
    }
}
