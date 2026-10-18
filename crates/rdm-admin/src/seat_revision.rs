//! The path fence: before a Build creates a birth at a path, whatever the world still runs there
//! is asked, never assumed (silence never authorizes a replacement; only a provider inspection
//! that says the exact runtime exited is proof).
//!
//! The decision is here; the questions it asks of the world are a [`PathProbe`]: the running
//! admin answers them over Node RPC, the entry protocol and its provider, a functional test
//! answers them from a fixture. The spans are the same either way.

use crate::deployment::provider::DeploymentStatus;
use crate::model::{Node, PathName};

/// What fencing a path's previous birth found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceOutcome {
    /// Nothing runs there any more: a new birth may take the path. `gone` is the previous
    /// birth when the provider inspected its exact runtime as exited: that inspection is the
    /// proof its departure is published from. `None` when there was no birth, or this admin
    /// holds no runtime for it (no proof, so no departure is published).
    Clear {
        /// The previous birth, when its exact runtime was inspected as exited.
        gone: Option<Node>,
    },
    /// The previous birth answers directly: it is alive and keeps the path.
    Alive,
    /// The previous birth does not answer, but a runtime at the path still runs: silence is
    /// never death proof, so it is held and keeps the path. Only an explicit retire or replace
    /// may terminate a running runtime.
    Held,
    /// The runtimes recorded at the path could not be read (`reason` names the directory or
    /// record that failed): nothing proves the path is free, so no birth is created over it.
    RecordsUnreadable {
        /// What could not be read, and why.
        reason: String,
    },
}

/// The questions the fence asks of the world.
#[async_trait::async_trait]
pub trait PathProbe: Send + Sync {
    /// Does this birth answer directly (Node RPC Ping for an rpc node, an entry pull for an admin)?
    async fn answers(&self, node: &Node) -> bool;
    /// The provider's inspection of this birth's exact runtime; `Err` names why it has none.
    async fn inspect(&self, node: &Node) -> Result<DeploymentStatus, String>;
    /// Every runtime recorded at `path` in this provider's domain, as `(where, inspection)`;
    /// `Err` names the directory or record that could not be read.
    async fn recorded(&self, path: &PathName) -> Result<Vec<(String, DeploymentStatus)>, String>;
}

/// Fence `path` before a new birth, given the birth this view holds there (`prev`).
pub async fn fence(path: &PathName, prev: Option<Node>, probe: &dyn PathProbe) -> FenceOutcome {
    let Some(prev) = prev else {
        // The view holds no birth at the path: the world is asked, not assumed.
        let recorded = match probe.recorded(path).await {
            Ok(r) => r,
            Err(reason) => {
                tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = "", outcome = "records-unreadable", reason = %reason)
                    .in_scope(|| tracing::info!("the view holds no birth at this path and the runtimes recorded there cannot be read: nothing proves it free"));
                return FenceOutcome::RecordsUnreadable { reason };
            }
        };
        // A runtime recorded there that runs, or that the operating system holds suspended, exists:
        // held, never replaced on silence (deployment-provider.md §2.5).
        for (at, status) in &recorded {
            let outcome = match status {
                DeploymentStatus::Running => "held-running-unheard",
                DeploymentStatus::OsStopped => "held-os-stopped-unheard",
                DeploymentStatus::Exited { .. } | DeploymentStatus::Unknown => continue,
            };
            tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = "", outcome, data_dir = %at)
                .in_scope(|| tracing::info!("the view holds no birth at this path, but a runtime recorded there still exists: held, never replaced on silence"));
            return FenceOutcome::Held;
        }
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = "", outcome = "no-predecessor-held", runtime_records = recorded.len())
            .in_scope(|| tracing::info!("the view holds no birth at this path and no recorded runtime there runs: nothing to fence"));
        return FenceOutcome::Clear { gone: None };
    };
    let incarnation = prev.incarnation_id.clone().map(|i| i.to_string()).unwrap_or_default();
    if prev.incarnation_id.is_none() {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = "", outcome = "no-birth-held")
            .in_scope(|| tracing::info!("the view holds a path with no birth at it: nothing to fence"));
        return FenceOutcome::Clear { gone: None };
    }
    // The plan was made on an older view: the path's birth is live here now (heard again through
    // a reborn forwarder, as the attempt ran). It keeps the path; a create over it is a duplicate.
    if prev.status.is_live() {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "predecessor-live")
            .in_scope(|| tracing::info!("the view holds a live birth at this path: it stays, never created over"));
        return FenceOutcome::Alive;
    }
    // Unheard is not dead: a healthy member's digests can stop arriving for a window (its
    // forwarder left, its connections are timing out). A predecessor that answers is alive.
    if probe.answers(&prev).await {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "answers")
            .in_scope(|| tracing::info!("the previous birth is unheard but answers directly: it stays"));
        return FenceOutcome::Alive;
    }
    let (outcome, proven) = match probe.inspect(&prev).await {
        Err(e) => (format!("not-found: {e}"), false),
        Ok(DeploymentStatus::Running) => {
            tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "held-running")
                .in_scope(|| tracing::info!("the previous birth is unheard but its exact runtime still runs: held, never replaced on silence"));
            return FenceOutcome::Held;
        }
        // Suspended by the operating system: neither hung nor gone (deployment-provider.md §2.5).
        Ok(DeploymentStatus::OsStopped) => {
            tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "held-os-stopped")
                .in_scope(|| tracing::info!("the previous birth is unheard and the operating system holds its exact runtime suspended: held, never replaced on silence"));
            return FenceOutcome::Held;
        }
        // Only an inspection that says the runtime exited is proof; `Unknown` fences the path
        // (nothing answers there) but publishes no departure.
        Ok(DeploymentStatus::Exited { code }) => (format!("not-running: Exited {{ code: {code:?} }}"), true),
        Ok(DeploymentStatus::Unknown) => ("not-running: Unknown".to_string(), false),
    };
    tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = %outcome, proven_gone = proven)
        .in_scope(|| tracing::info!("the previous birth at this path is fenced before a new one"));
    FenceOutcome::Clear { gone: proven.then_some(prev) }
}

/// The data dirs under `data_root` recorded for `path` (`<path>-<node id>`). Read on the blocking
/// pool (`blocking::blocking_io`); a root or entry that cannot be read is an error naming it,
/// never an empty answer.
pub(crate) async fn recorded_dirs(data_root: &std::path::Path, path: &PathName) -> Result<Vec<std::path::PathBuf>, String> {
    let (root, prefix) = (data_root.to_path_buf(), format!("{path}-"));
    crate::blocking::blocking_io(move || {
        let named = |e: std::io::Error| format!("{}: {e}", root.display());
        let mut out = Vec::new();
        for entry in std::fs::read_dir(&root).map_err(named)? {
            let entry = entry.map_err(named)?;
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                out.push(entry.path());
            }
        }
        Ok(out)
    })
    .await
}

/// The runtime record `dir` holds (`None`: it holds none), read on the blocking pool; a record
/// that cannot be read is an error naming the file.
pub(crate) async fn runtime_record(dir: &std::path::Path) -> Result<Option<rdm_core::RuntimeFact>, String> {
    let dir = dir.to_path_buf();
    crate::blocking::blocking_io(move || rdm_core::RuntimeFact::read_record(&dir).transpose()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{op_waits_on_the_blocking_pool, runtime_runs_while_a_fifo_is_read, tempdir};

    /// CONTRACT: the scan of a path's recorded data dirs waits on the blocking pool, never on a
    /// runtime worker.
    #[test]
    fn the_recorded_dirs_scan_waits_on_the_blocking_pool() {
        let root = tempdir("fence-scan");
        std::fs::create_dir_all(root.join("mesh1.rpc.1-n1")).unwrap();
        let path: PathName = "mesh1.rpc.1".parse().unwrap();
        let (waited, found) = op_waits_on_the_blocking_pool(Box::pin(async move { (root, path) }), |(root, path)| Box::pin(async move { recorded_dirs(&root, &path).await }));
        assert!(waited, "the scan finished on the runtime's thread while the blocking pool was held");
        assert_eq!(found.unwrap().len(), 1);
    }

    /// CONTRACT: a data root that cannot be read is an error naming it, never "nothing recorded".
    #[tokio::test]
    async fn an_unreadable_data_root_is_an_error_naming_it() {
        let root = tempdir("fence-root").join("not-a-dir");
        std::fs::write(&root, b"a file where the data root should be").unwrap();
        let err = recorded_dirs(&root, &"mesh1.rpc.1".parse().unwrap()).await.expect_err("an unreadable root is not an empty answer");
        assert!(err.contains(&root.display().to_string()), "{err}");
    }

    /// CONTRACT: the runtime record is read on the blocking pool: a runtime worker never waits on
    /// its file.
    #[test]
    fn the_runtime_record_is_read_on_the_blocking_pool() {
        let dir = tempdir("fence-record");
        let fact = rdm_core::RuntimeFact {
            deployment_id: rdm_core::fixture_crockford60("dep-1"),
            provider: rdm_core::runtime::RuntimeProvider::Process,
            control_domain: "process:test".into(),
            locator: rdm_core::runtime::RuntimeLocator::Process { pid: 1, start: 1 },
        };
        let bytes = serde_json::to_vec(&fact).unwrap();
        let fifo = dir.join(rdm_core::runtime::RUNTIME_FILE);
        let (heard, read) = runtime_runs_while_a_fifo_is_read(fifo, bytes, Box::pin(async move { runtime_record(&dir).await }));
        assert!(heard, "the runtime stopped while the record was read");
        assert_eq!(read.unwrap(), Some(fact));
    }

    /// CONTRACT: a runtime record that cannot be read is an error naming the file, never "no
    /// runtime recorded here".
    #[tokio::test]
    async fn an_unreadable_runtime_record_is_an_error_naming_the_file() {
        let dir = tempdir("fence-unreadable");
        std::fs::create_dir_all(dir.join(rdm_core::runtime::RUNTIME_FILE)).unwrap();
        let err = runtime_record(&dir).await.expect_err("an unreadable record is not an absent one");
        assert!(err.contains(rdm_core::runtime::RUNTIME_FILE), "{err}");
    }

    struct Unreadable;
    #[async_trait::async_trait]
    impl PathProbe for Unreadable {
        async fn answers(&self, _: &Node) -> bool {
            false
        }
        async fn inspect(&self, _: &Node) -> Result<DeploymentStatus, String> {
            Err("none".into())
        }
        async fn recorded(&self, _: &PathName) -> Result<Vec<(String, DeploymentStatus)>, String> {
            Err("/data/mesh1.rpc.1-n1/runtime.json: Is a directory".into())
        }
    }

    /// CONTRACT: a path whose recorded runtimes cannot be read is never cleared for a new birth:
    /// the fence answers why.
    #[tokio::test]
    async fn a_path_whose_records_cannot_be_read_is_never_cleared() {
        let out = fence(&"mesh1.rpc.1".parse().unwrap(), None, &Unreadable).await;
        assert!(matches!(&out, FenceOutcome::RecordsUnreadable { reason } if reason.contains("runtime.json")), "{out:?}");
    }
}
