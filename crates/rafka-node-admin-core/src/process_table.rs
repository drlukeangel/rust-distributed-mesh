//! The local process table: the node lifecycle ownership that used to live in
//! the Admin UI's process map (i143.e1.s1, gap R4).
//!
//! It owns every child process it registered, their metadata and their spawn
//! directories: registration, graceful-then-forced termination, and reaping of
//! children that exited on their own. Behaviour is the Admin UI's, moved
//! unchanged; Build and the deployment providers replace its callers later.

use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Child;
use tokio::sync::Mutex;

/// What the table knows about one spawned child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnedMeta {
    pub node_type: String,
    pub mesh_id: String,
    pub pid: u32,
    /// The child's pre-minted transport identity (Iroh public key).
    pub node_id_hex: String,
    pub bind_port: u16,
    pub spawned_at_ms: u64,
}

/// How a termination ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    /// The child exited within the grace period after the kill signal.
    Graceful,
    /// The grace period expired and the child was killed outright.
    Forced,
}

impl Termination {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Graceful => "graceful",
            Self::Forced => "forced",
        }
    }
}

/// A terminated child, as reported to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Terminated {
    pub pid: u32,
    pub how: Termination,
    pub meta: Option<SpawnedMeta>,
}

/// A child that exited without being terminated through the table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reaped {
    pub name: String,
    pub exit_code: i32,
}

/// The grace period between the kill signal and a forced kill.
pub const TERMINATION_GRACE: Duration = Duration::from_secs(5);

pub struct ProcessTable {
    spawn_root: PathBuf,
    processes: DashMap<String, Mutex<Child>>,
    spawned: DashMap<String, SpawnedMeta>,
}

impl ProcessTable {
    /// `spawn_root` holds one data directory per child, named after it.
    pub fn new(spawn_root: impl Into<PathBuf>) -> Self {
        Self { spawn_root: spawn_root.into(), processes: DashMap::new(), spawned: DashMap::new() }
    }

    pub fn spawn_root(&self) -> &Path {
        &self.spawn_root
    }

    /// The data directory of child `name`.
    pub fn spawn_dir(&self, name: &str) -> PathBuf {
        self.spawn_root.join(name)
    }

    /// Take ownership of a spawned child.
    pub fn register(&self, name: impl Into<String>, child: Child, meta: SpawnedMeta) {
        let name = name.into();
        self.processes.insert(name.clone(), Mutex::new(child));
        self.spawned.insert(name, meta);
    }

    /// Read access to the live children.
    pub fn processes(&self) -> &DashMap<String, Mutex<Child>> {
        &self.processes
    }

    /// Read access to the children's metadata.
    pub fn spawned(&self) -> &DashMap<String, SpawnedMeta> {
        &self.spawned
    }

    pub fn names(&self) -> Vec<String> {
        self.processes.iter().map(|e| e.key().clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.spawned.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spawned.is_empty()
    }

    /// Terminate child `name`: kill signal, wait up to [`TERMINATION_GRACE`],
    /// then a forced kill; remove its metadata and data directory.
    pub async fn terminate(&self, name: &str) -> Result<Terminated, String> {
        let (_, child) = self.processes.remove(name).ok_or_else(|| format!("no subprocess named {name}"))?;
        let mut child = child.into_inner();
        let pid = child.id().unwrap_or(0);
        let _ = child.start_kill();
        let how = match tokio::time::timeout(TERMINATION_GRACE, child.wait()).await {
            Ok(_) => Termination::Graceful,
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                Termination::Forced
            }
        };
        let dir = self.spawn_dir(name);
        if let Err(e) = tokio::fs::remove_dir_all(&dir).await {
            tracing::warn!(dir = %dir.display(), error = %e, "failed to remove subprocess data dir");
        }
        let meta = self.spawned.remove(name).map(|(_, m)| m);
        Ok(Terminated { pid, how, meta })
    }

    /// Forget every child that already exited, deleting its data directory
    /// (it holds the child's secret key), then sweep orphan data directories
    /// that belong to no registered child.
    pub async fn reap_exited(&self) -> Vec<Reaped> {
        let mut reaped = Vec::new();
        for name in self.names() {
            let exited = match self.processes.get(&name) {
                Some(entry) => entry.value().lock().await.try_wait().ok().flatten(),
                None => None,
            };
            if let Some(status) = exited {
                self.processes.remove(&name);
                self.spawned.remove(&name);
                let dir = self.spawn_dir(&name);
                if let Err(e) = tokio::fs::remove_dir_all(&dir).await {
                    tracing::warn!(dir = %dir.display(), error = %e, "reaper: data dir cleanup failed");
                }
                reaped.push(Reaped { name, exit_code: status.code().unwrap_or(-1) });
            }
        }
        if let Ok(mut rd) = tokio::fs::read_dir(&self.spawn_root).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !self.processes.contains_key(&name) && !self.spawned.contains_key(&name) {
                    let _ = tokio::fs::remove_dir_all(entry.path()).await;
                }
            }
        }
        reaped
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn root() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!("proc-table-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn meta(pid: u32) -> SpawnedMeta {
        SpawnedMeta { node_type: "x".into(), mesh_id: "mesh1".into(), pid, node_id_hex: "k".into(), bind_port: 1, spawned_at_ms: 0 }
    }

    fn spawn(table: &ProcessTable, name: &str, cmd: &str, args: &[&str]) {
        std::fs::create_dir_all(table.spawn_dir(name)).unwrap();
        let child = tokio::process::Command::new(cmd).args(args).spawn().unwrap();
        let pid = child.id().unwrap();
        table.register(name, child, meta(pid));
    }

    #[tokio::test]
    async fn terminate_kills_forgets_and_removes_the_data_dir() {
        let t = ProcessTable::new(root());
        spawn(&t, "n1", "sleep", &["30"]);
        assert_eq!(t.names(), vec!["n1".to_string()]);
        let done = t.terminate("n1").await.unwrap();
        assert_eq!(done.how, Termination::Graceful);
        assert_eq!(done.meta.as_ref().map(|m| m.mesh_id.as_str()), Some("mesh1"));
        assert!(t.is_empty() && t.names().is_empty());
        assert!(!t.spawn_dir("n1").exists());
        assert_eq!(t.terminate("n1").await, Err("no subprocess named n1".into()));
    }

    #[tokio::test]
    async fn reaper_forgets_exited_children_and_sweeps_orphans() {
        let t = ProcessTable::new(root());
        spawn(&t, "quick", "true", &[]);
        spawn(&t, "slow", "sleep", &["30"]);
        std::fs::create_dir_all(t.spawn_dir("orphan")).unwrap();
        let mut reaped = Vec::new();
        for _ in 0..100 {
            reaped.extend(t.reap_exited().await);
            if !reaped.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(reaped, vec![Reaped { name: "quick".into(), exit_code: 0 }]);
        assert_eq!(t.names(), vec!["slow".to_string()]);
        assert!(!t.spawn_dir("quick").exists());
        assert!(!t.spawn_dir("orphan").exists(), "orphan dirs are swept");
        assert!(t.spawn_dir("slow").exists());
        t.terminate("slow").await.unwrap();
    }
}
