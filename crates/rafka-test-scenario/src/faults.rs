//! The fault door of a faulted node-admin, as a scenario drives it (i143.e8.s2, #2780): the testkit
//! Node RPC op `0x75` (`rafka_node_rpc_testkit::fault_door`), reached by the admin's `path.name` on the
//! estate's Node RPC client; armed, observed and released. Shared by every cell that holds a real admin
//! at a named cut. A cut that must be in force before the admin's Node RPC door is ready is armed from
//! `<root>/faults/<name>.boot.json` instead.

use crate::estate::{bin_dir, wait_for, wait_for_observed, Estate};
use rafka_node_admin_client::binding::{sha256_file, Binding, BindingSet, Candidate};
use rafka_node_admin_client::{PathName, Peers};
use rafka_node_rpc_testkit::admin_faults::CutSpec;
use rafka_node_rpc_testkit::fault_door::{self, FaultReply, FaultRequest, FaultState};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

/// One faulted admin's fault door, reached over Node RPC by the admin's `path.name`.
#[derive(Clone)]
pub struct Door {
    pub name: String,
    path: PathName,
    peers: Arc<Peers>,
}

impl Door {
    /// The door of the faulted admin `name`, once it answers. The admin's Node RPC door answers NotReady
    /// until it is ready (node-rpc-envelope.md, "Node RPC readiness gate"), and an admin the estate did not
    /// start is dialled from the views the estate reads, so until then each turn reads them again.
    pub async fn at(estate: &Estate, name: &str) -> Door {
        let door = Door { name: name.into(), path: Estate::path(name), peers: estate.peers().await };
        wait_for_observed(&format!("{name}'s fault door answers over Node RPC"), Duration::from_secs(60), || async {
            match door.ask(&FaultRequest::State).await {
                Ok(FaultReply::State(_)) => Ok(()),
                other => {
                    let _ = estate.try_node_views().await;
                    Err(format!("{other:?}"))
                }
            }
        })
        .await;
        door
    }

    async fn ask(&self, req: &FaultRequest) -> Result<FaultReply, String> {
        fault_door::ask(self.peers.carrier_at(self.path.clone()).client(), &self.path, req).await
    }

    /// Arm `spec` (a cut as JSON: `{"kind": .., ..}`) under `id`; the acknowledgement, as JSON.
    pub async fn arm(&self, id: &str, spec: Value) -> Value {
        let cut: CutSpec = serde_json::from_value(spec.clone()).unwrap_or_else(|e| panic!("{}: `{id}` is not a cut: {spec}: {e}", self.name));
        match self.ask(&FaultRequest::arm(id, cut)).await {
            Ok(FaultReply::Armed { node, id: armed, spec }) => {
                assert_eq!(armed, id, "{}: the door armed another cut", self.name);
                json!({"armed": armed, "node": node, "spec": spec})
            }
            other => panic!("{}: arming `{id}` is acknowledged: {other:?}", self.name),
        }
    }

    /// Every cut and what it held, and the consequence counters, as JSON.
    pub async fn state(&self) -> Value {
        match self.ask(&FaultRequest::State).await {
            Ok(FaultReply::State(s)) => state_value(&s),
            other => panic!("{}: fault state: {other:?}", self.name),
        }
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

    /// The acknowledgement that the hold ended after a release: the cut no longer holds a call.
    pub async fn wait_released(&self, id: &str) -> bool {
        wait_for(&format!("{}: the hold of cut `{id}` ended", self.name), Duration::from_secs(30), || async { (self.cut(id).await["held"] == false).then_some(true) }).await
    }

    pub async fn release(&self, id: &str) -> Value {
        match self.ask(&FaultRequest::Release { id: id.into() }).await {
            Ok(FaultReply::Released { node, id: released, was_held, held_ms }) => {
                assert_eq!(released, id, "{}: the door released another cut", self.name);
                json!({"released": released, "node": node, "was_held": was_held, "held_ms": held_ms})
            }
            other => panic!("{}: releasing `{id}` is acknowledged: {other:?}", self.name),
        }
    }
}

/// The door's state as the cells read it: `{"node", "cuts": {<id>: {"spec", "armed", "held", "released",
/// "hit", "held_for_ms", "seen_while_held"}}, "receipts_seen"}`, each hit one flat object of its fields.
fn state_value(s: &FaultState) -> Value {
    let cuts: serde_json::Map<String, Value> = s
        .cuts
        .iter()
        .map(|(id, c)| {
            let row = json!({
                "spec": c.spec, "armed": c.armed, "held": c.held, "released": c.released, "hit": c.hit.as_ref().map(|h| h.detail()),
                "held_for_ms": c.held_for_ms, "seen_while_held": c.seen_while_held,
            });
            (id.clone(), row)
        })
        .collect();
    json!({"node": s.node, "cuts": cuts, "receipts_seen": s.receipts_seen})
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
    assert!(p.exists(), "RED: executable `{name}` is not built at {} (cargo build -p {} --bins)", p.display(), crate::estate::package_of(name));
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

/// [`binding_set`] with the launch ids `broker`, `gateway` and `compute` bound too, each to the one
/// node executable the node base builds (`rafka-node`), which takes its node_type from the launch.
pub fn binding_set_with_roles(sha: &str) -> BindingSet {
    let mut set = binding_set(sha);
    let path = executable_in(&bin_dir(), "rafka-node");
    let sha256 = sha256_file(&path).unwrap();
    for role in ["broker", "gateway", "compute"] {
        set.launch_ids.push(role.into());
        set.bindings.push(Binding { launch_id: role.into(), sha256: sha256.clone(), executable: path.clone(), image: None });
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
