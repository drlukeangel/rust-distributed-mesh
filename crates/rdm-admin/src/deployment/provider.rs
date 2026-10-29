//! Deployment provider policy (`docs/architecture/deployment-provider.md` §1–§2).
//!
//! `MESH_SPAWN_TYPE=process|container` is read once, at the first node-admin
//! bootstrap, and normalised into the fabric's typed policy. Later admins
//! inherit it. An unknown value is a hard bootstrap refusal, never a silent
//! fallback; a joining admin whose local value disagrees with the fabric
//! refuses to start by name; a Build never names a provider.

use crate::model::ProviderKind;
use std::fmt;

/// The bootstrap selector.
pub(crate) const SPAWN_TYPE_ENV: &str = "MESH_SPAWN_TYPE";

/// Why a deployment policy is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyRefusal {
    /// `MESH_SPAWN_TYPE` holds a value that names no provider.
    UnknownProvider {
        /// The value that names no provider.
        value: String,
    },
    /// A joining admin's local value conflicts with the established fabric policy.
    ProviderMismatch {
        /// The provider the established fabric policy names.
        fabric: ProviderKind,
        /// The provider the joining admin's local value names.
        local: ProviderKind,
    },
}

impl fmt::Display for PolicyRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownProvider { value } => {
                write!(f, "{SPAWN_TYPE_ENV}={value:?} names no deployment provider (expected process or container)")
            }
            Self::ProviderMismatch { fabric, local } => write!(
                f,
                "this admin's {SPAWN_TYPE_ENV} selects {local:?} but the fabric's established provider is {fabric:?}; refusing deployment authority"
            ),
        }
    }
}

/// Parse one `MESH_SPAWN_TYPE` value. Case and surrounding whitespace are
/// normalised; anything else is refused by name. `None` (unset) is `None`.
pub fn parse(value: Option<&str>) -> Result<Option<ProviderKind>, PolicyRefusal> {
    let Some(raw) = value else { return Ok(None) };
    match raw.trim().to_ascii_lowercase().as_str() {
        "process" => Ok(Some(ProviderKind::Process)),
        "container" => Ok(Some(ProviderKind::Container)),
        _ => Err(PolicyRefusal::UnknownProvider { value: raw.to_string() }),
    }
}

/// The fabric's deployment policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FabricPolicy {
    /// The provider that runs the fabric's nodes.
    pub provider: ProviderKind,
}

impl FabricPolicy {
    /// The first admin of a fabric: its `MESH_SPAWN_TYPE` decides; unset is `process`.
    pub fn bootstrap(env_value: Option<&str>) -> Result<Self, PolicyRefusal> {
        Ok(Self { provider: parse(env_value)?.unwrap_or(ProviderKind::Process) })
    }

    /// A later admin joining a fabric whose policy is `established`. Unset
    /// inherits; an equal value is fine; anything else is refused by name.
    pub fn inherit(established: FabricPolicy, local_env_value: Option<&str>) -> Result<Self, PolicyRefusal> {
        match parse(local_env_value)? {
            None => Ok(established),
            Some(p) if p == established.provider => Ok(established),
            Some(local) => Err(PolicyRefusal::ProviderMismatch { fabric: established.provider, local }),
        }
    }
}


// ---------------------------------------------------------------------------
// The provider contract (mesh-control-plane.md §6). A provider realises or
// deletes one runtime from an already-resolved launch; it decides nothing.
// ---------------------------------------------------------------------------

use crate::model::{DeploymentId, PathName};
use rdm_core::{RuntimeFact, RuntimeLocator, RuntimeProvider};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Everything already decided by node-admin: identity, endpoints, storage, env.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedNodeLaunch {
    /// The node to run.
    pub node: PathName,
    /// The deployment the runtime belongs to.
    pub deployment_id: DeploymentId,
    /// The executable to run.
    pub executable: PathBuf,
    /// The arguments to run it with.
    pub args: Vec<String>,
    /// The environment to run it with: RDM's own launch variables, then RDM's fixed pass-through.
    pub env: BTreeMap<String, String>,
    /// The declared secrets set in this runtime's environment (Q150): those whose declaration names
    /// the node's kind, by name, each value's bytes as hex. Held so its debug form never shows a value;
    /// the provider decodes each at the spawn and writes it nowhere else.
    pub secrets: BTreeMap<String, rdm_core::launch::SecretHex>,
    /// Every declared name, set here or not: a process runtime never inherits one from its launcher's
    /// environment, as it never inherits `RDM_*` or the trace context.
    pub withhold: Vec<String>,
    /// The node's data directory.
    pub data_dir: PathBuf,
    /// The advertised sockets the runtime must bind: its Iroh transport and its
    /// listeners. The provider honours them; it never invents one.
    pub transport: std::net::SocketAddr,
    /// The non-Iroh listeners the runtime must bind, by name.
    pub listeners: Vec<(String, std::net::SocketAddr)>,
}

/// What a provider knows of one exact launch ([`DeploymentProvider::find`]): three distinct facts,
/// never folded into one "nothing" (lifecycles/node-create.md: exact provider proof of a runtime's
/// state, even for a deployment that never joined).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Found {
    /// The launch's runtime runs: its exact handle.
    Running(DeploymentHandle),
    /// The launch started a runtime, and that runtime has exited: its exact handle when the launch
    /// record names one, so a successor's delete inspects that exact runtime for its exited verdict.
    Exited(Option<DeploymentHandle>),
    /// No runtime of the launch was ever started in this provider's control domain.
    NeverLaunched,
}

impl Found {
    /// The handle of a runtime that runs.
    pub fn running(self) -> Option<DeploymentHandle> {
        match self {
            Found::Running(h) => Some(h),
            Found::Exited(_) | Found::NeverLaunched => None,
        }
    }

    /// The fact's name, as spans and refusals say it.
    pub fn name(&self) -> &'static str {
        match self {
            Found::Running(_) => "running",
            Found::Exited(_) => "exited",
            Found::NeverLaunched => "never-launched",
        }
    }
}

/// A realised runtime, exactly (`docs/architecture/deployment-provider.md` §2.3): a pid only
/// together with its start token, a container by its immutable id, each within its provider control
/// domain.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentHandle {
    /// The deployment the runtime belongs to.
    pub deployment_id: DeploymentId,
    /// The provider that realised the runtime.
    pub provider: ProviderKind,
    /// Process id (process provider); a container's init pid (container provider).
    pub pid: Option<u32>,
    /// The kernel's start time of `pid` (process provider): a recycled pid never matches.
    #[serde(default)]
    pub start: Option<u64>,
    /// The immutable container id (container provider).
    pub container: Option<String>,
    /// The provider control domain the locator means something in.
    #[serde(default)]
    pub domain: Option<String>,
}

impl DeploymentHandle {
    /// The published fact of this runtime; `None` when the handle is not exact.
    pub fn fact(&self) -> Option<RuntimeFact> {
        let locator = match self.provider {
            ProviderKind::Process => RuntimeLocator::Process { pid: self.pid?, start: self.start? },
            ProviderKind::Container => RuntimeLocator::Container { id: self.container.clone()? },
        };
        let fact = RuntimeFact {
            deployment_id: self.deployment_id.to_string(),
            provider: match self.provider {
                ProviderKind::Process => RuntimeProvider::Process,
                ProviderKind::Container => RuntimeProvider::Container,
            },
            control_domain: self.domain.clone()?,
            locator,
        };
        fact.validate().ok()?;
        Some(fact)
    }
}

/// Why a published runtime fact is not adopted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptRefusal {
    /// The fact is malformed or inexact.
    Invalid {
        /// Why the fact is refused.
        reason: String,
    },
    /// Another provider realised it.
    OtherProvider {
        /// The provider the fact names.
        fact: &'static str,
        /// This provider.
        here: ProviderKind,
    },
    /// Its locator means something only in another control domain.
    ForeignControlDomain {
        /// The control domain the fact names.
        fact_domain: String,
        /// This provider's control domain.
        here: String,
    },
}

impl AdoptRefusal {
    /// The span reason (`rdm.node_admin.runtime.reject.via-<reason>`).
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Invalid { .. } => "invalid-fact",
            Self::OtherProvider { .. } => "other-provider",
            Self::ForeignControlDomain { .. } => "foreign-control-domain",
        }
    }
}

impl fmt::Display for AdoptRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { reason } => write!(f, "the runtime fact is not exact: {reason}"),
            Self::OtherProvider { fact, here } => write!(f, "a {fact} runtime cannot be controlled by the {here:?} provider"),
            Self::ForeignControlDomain { fact_domain, here } => write!(
                f,
                "the runtime lives in control domain {} and this provider controls {}; refusing to act on its locator",
                rdm_core::runtime::fingerprint(fact_domain),
                rdm_core::runtime::fingerprint(here)
            ),
        }
    }
}

/// `fact` as a handle `provider` may act on, or the named reason it may not
/// (`docs/architecture/deployment-provider.md` §2.3).
pub fn adopt(provider: &dyn DeploymentProvider, fact: &RuntimeFact) -> Result<DeploymentHandle, AdoptRefusal> {
    fact.validate().map_err(|e| AdoptRefusal::Invalid { reason: e.to_string() })?;
    let kind = match fact.provider {
        RuntimeProvider::Process => ProviderKind::Process,
        RuntimeProvider::Container => ProviderKind::Container,
    };
    if kind != provider.kind() {
        return Err(AdoptRefusal::OtherProvider { fact: fact.provider.as_str(), here: provider.kind() });
    }
    let here = provider.control_domain();
    if fact.control_domain != here {
        return Err(AdoptRefusal::ForeignControlDomain { fact_domain: fact.control_domain.clone(), here });
    }
    let (pid, start, container) = match &fact.locator {
        RuntimeLocator::Process { pid, start } => (Some(*pid), Some(*start), None),
        RuntimeLocator::Container { id } => (None, None, Some(id.clone())),
    };
    let deployment_id = fact.deployment().map_err(|e| AdoptRefusal::Invalid { reason: e.to_string() })?;
    Ok(DeploymentHandle { deployment_id, provider: kind, pid, start, container, domain: Some(here) })
}

/// How a runtime is stopped (`docs/architecture/deployment-provider.md` §2.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationMode {
    /// SIGTERM (or the provider's stop), then a forced kill after `grace`.
    Graceful {
        /// How long to wait before the forced kill.
        grace: std::time::Duration,
    },
    /// Kill the runtime at once.
    Immediate,
}

/// `TerminateRuntime` of a runtime read at its own instant (node-runtime-inspect.md: node recovery's
/// `TerminateRuntime` asks "at the step's own instant, before it asks the provider to terminate";
/// node-recovery.md step 3, Q145): the provider's `terminate(mode)` on every read but `OsStopped`,
/// which recovery leaves as it is; of an exited runtime the provider removes what it left. The answer
/// is what the read at the step's instant found; a terminate the provider refuses is the error, by name.
pub async fn terminate_unless_os_stopped(provider: &dyn DeploymentProvider, handle: &DeploymentHandle, mode: TerminationMode) -> Result<DeploymentStatus, String> {
    let found = provider.inspect(handle).await;
    if found != DeploymentStatus::OsStopped {
        provider.terminate(handle, mode).await.map_err(|e| e.to_string())?;
    }
    Ok(found)
}

/// `status` of `fact`'s runtime with the exit code its own record proves. A process this admin did
/// not launch and found dead reads `Exited { code: None }`: the process runtime recorded a
/// deliberate exit in its data dir, keyed to its deployment and incarnation, and only that exact
/// record turns the unknown code into the recorded one. A container's code is Docker's own.
/// `Err` names the exit record that could not be read.
pub async fn with_exit_code(status: DeploymentStatus, fact: &RuntimeFact, data_dir: Option<&std::path::Path>, incarnation: &str) -> Result<DeploymentStatus, String> {
    match (status, fact.provider, data_dir) {
        (DeploymentStatus::Exited { code: None }, RuntimeProvider::Process, Some(dir)) => {
            // Read on the blocking pool (`blocking::blocking_io`).
            let (dir, deployment, incarnation) = (dir.to_path_buf(), fact.deployment_id.clone(), incarnation.to_string());
            let code = crate::blocking::blocking_io(move || rdm_core::runtime::ExitRecord::proven_code(&dir, &deployment, &incarnation)).await?;
            Ok(DeploymentStatus::Exited { code })
        }
        (other, _, _) => Ok(other),
    }
}

/// What node.runtime.inspect found of the exact runtime (`docs/architecture/lifecycles/node-runtime-inspect.md`
/// "Result"): its three answers and `unavailable { reason }`, which is neither.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inspected {
    /// `exited { code }`: the exit code, when the provider's terminal status or the runtime's own exit
    /// record carries one (node-runtime-inspect.md "Result").
    Exited(Option<i32>),
    /// The exact runtime runs, however silent.
    Running,
    /// The operating system holds the exact runtime suspended (deployment-provider.md §2.5).
    OsStopped,
    /// Nothing proven (no runtime fact, an adoption the provider refused, an unreadable row or exit
    /// record, the provider's `Unknown`): why.
    Unavailable(String),
}

/// node.runtime.inspect of the exact runtime `fact` names, the birth `incarnation`'s, by `provider`
/// (`docs/architecture/lifecycles/node-runtime-inspect.md` steps 2 and 5): the fact adopted in this
/// provider's control domain ([`adopt`]) and inspected, the code the runtime's own exit record proves
/// read from `data_dir` ([`with_exit_code`]), and only the runtime's own terminal status an exit. The
/// one inspect every caller runs: the primary watch's ladder, the `mesh-primary`'s recovery tick, the
/// `fabric-primary`'s drift pass, the create's read of the path's previous birth. A fact the provider
/// refuses to adopt, an unreadable exit record and the provider's `Unknown` are each `Unavailable`,
/// named why.
pub async fn inspect_runtime(provider: &dyn DeploymentProvider, fact: &RuntimeFact, data_dir: Option<&str>, incarnation: &str) -> Inspected {
    use DeploymentStatus as S;
    let handle = match adopt(provider, fact) {
        Ok(handle) => handle,
        Err(e) => return Inspected::Unavailable(format!("the provider refused to adopt its runtime: {e}")),
    };
    let inspected = provider.inspect(&handle).await;
    match with_exit_code(inspected, fact, data_dir.map(std::path::Path::new), incarnation).await {
        Ok(S::Exited { code }) => Inspected::Exited(code),
        Ok(S::Running) => Inspected::Running,
        Ok(S::OsStopped) => Inspected::OsStopped,
        Ok(S::Unknown) => Inspected::Unavailable("the provider cannot tell its runtime's state: Unknown".into()),
        Err(e) => Inspected::Unavailable(format!("its exit record cannot be read: {e}")),
    }
}

/// What a provider knows of the exact runtime (deployment-provider.md §2.5): one meaning per state
/// for a process and a container alike; a caller reads this, never `/proc` or Docker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploymentStatus {
    /// The exact runtime exists, is nonterminal and is not observed OS-suspended. It proves neither
    /// responsiveness nor readiness: a sleeping task, a runtime still starting and a deadlocked one
    /// all read `Running`.
    Running,
    /// The exact runtime exists and the operating system holds every task of it suspended (a
    /// process in state `T`/`t`; a paused container). Observed suspension: it is not the lifecycle
    /// status `stopped`, proves no completed `node.stop`, and is never exited verdict.
    OsStopped,
    /// The exact runtime exited.
    Exited {
        /// The exit code, when its record proves one.
        code: Option<i32>,
    },
    /// The exact runtime's state cannot be told: an unreadable, partial or inconclusive inspection.
    Unknown,
}

/// Why a provider call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeployError {
    /// The runtime could not be started.
    Spawn {
        /// The node.
        node: String,
        /// Why it failed.
        reason: String,
    },
    /// The runtime could not be stopped.
    Terminate {
        /// The deployment.
        deployment: String,
        /// Why it failed.
        reason: String,
    },
    /// The provider cannot run on this host (named, never a silent pass).
    Unsupported {
        /// The provider.
        provider: ProviderKind,
        /// Why it cannot run.
        reason: String,
    },
}

impl std::fmt::Display for DeployError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn { node, reason } => write!(f, "deploying {node} failed: {reason}"),
            Self::Terminate { deployment, reason } => write!(f, "terminating deployment {deployment} failed: {reason}"),
            Self::Unsupported { provider, reason } => write!(f, "{provider:?} provider is unsupported on this host: {reason}"),
        }
    }
}

/// Realises and deletes runtimes (`docs/architecture/deployment-provider.md` §2): starts a launch,
/// stops a runtime and reports what it knows of one.
#[async_trait::async_trait]
pub trait DeploymentProvider: Send + Sync {
    /// The provider's kind.
    fn kind(&self) -> ProviderKind;
    /// The provider control domain this provider's locators mean something in.
    fn control_domain(&self) -> String;
    /// Start the launch `spec` and return the exact handle of its runtime. The launch's record in
    /// its data dir (`write_launch_record`) is durable before the runtime's executable runs, so a
    /// runtime that runs always has its record and an executor lost at any point of the spawn
    /// leaves [`find`](Self::find) a record to read.
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError>;
    /// Stop the runtime `handle` as `mode` says.
    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError>;
    /// The state of the runtime `handle`.
    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus;

    /// Stage one of the stop ladder: ask the runtime to delete its connections and stop
    /// (SIGTERM). A runtime already gone is not an error.
    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError>;

    /// What became of this exact launch (`spec.deployment_id`) in this provider's control domain
    /// ([`Found`]): its runtime runs (a re-run after a crash mid-`DeployRuntime` adopts it instead of
    /// starting a second one), it ran and has exited, or it was never launched.
    /// `Err` names the record of the launch that could not be read, or a state this provider cannot
    /// place in any of the three.
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<Found, DeployError>;

    /// The host address the runtime of `birth` (its deployment, at `node`) binds on, with port 0 (the
    /// operating system assigns the port; the node reports it at its join). Loopback for a process; a
    /// container's own address on the fabric network, which belongs to the birth that holds it.
    /// `Err` names why no address can be given (a container network the daemon cannot list, or
    /// one with no address left).
    async fn bind_ip(&self, _node: &crate::model::PathName, _birth: &crate::model::DeploymentId) -> Result<std::net::IpAddr, DeployError> {
        Ok(std::net::IpAddr::from([127, 0, 0, 1]))
    }

    /// Every runtime this provider started that it has not seen exit: what a
    /// fabric shutdown stops, including a runtime whose create failed after
    /// `DeployRuntime`.
    fn launched(&self) -> Vec<DeploymentHandle> {
        Vec::new()
    }

    /// Moves each time a runtime this provider started is seen to exit: the reconcile loop waits on it
    /// (Q254: a provider exit is an event), whose drive's departure proof reads the exit. Default: a
    /// watch that never moves (a provider that sees no exit of its own).
    fn exits(&self) -> tokio::sync::watch::Receiver<u64> {
        tokio::sync::watch::channel(0).1
    }

    /// The runtime's last words, for a named failure when it stopped early.
    /// Default: the tail of `stderr.log` in its data dir, read on the blocking pool; a log that
    /// cannot be read is named, never an empty detail.
    async fn failure_detail(&self, _handle: &DeploymentHandle, data_dir: &std::path::Path) -> String {
        let path = data_dir.join("stderr.log");
        crate::blocking::blocking_io(move || match std::fs::read_to_string(&path) {
            Ok(text) => tail(&text, 5),
            Err(e) => format!("{}: {e}", path.display()),
        })
        .await
    }
}

/// Write the launch record `file` of a launch in `data_dir`: written to a temporary file, fsynced,
/// renamed over `file`, and the directory fsynced, so a record that exists is whole and survives
/// the loss of the executor that wrote it. Both providers write it before the runtime's executable
/// runs. A blocking call: run it on the blocking pool.
pub(crate) fn write_launch_record(data_dir: &std::path::Path, file: &str, record: &serde_json::Value) -> Result<(), String> {
    let tmp = data_dir.join(format!("{file}.tmp"));
    crate::durable_file::write_file(data_dir, &tmp, &data_dir.join(file), record.to_string().as_bytes()).map_err(|e| e.to_string())
}

/// The launch hand-off a launched node-admin starts holding its launcher's runtime rows from
/// (`CreateRequest::held_runtimes`): written in its data dir beside the launch record, before it runs.
pub const HELD_RUNTIMES_FILE: &str = "held-runtimes.json";
/// The format of the held-runtimes hand-off. Another format is refused by name.
const HELD_RUNTIMES_FORMAT: &str = "held-runtimes/1";

/// Hand the node-admin launched in `data_dir` the runtime rows `rows` its launcher holds: written
/// whole and durable ([`HELD_RUNTIMES_FILE`]) before its executable runs, as the launch record is.
pub(crate) async fn write_held_runtimes(data_dir: &std::path::Path, rows: &[crate::storage::RuntimeRow]) -> Result<(), String> {
    let (dir, record) = (data_dir.to_path_buf(), serde_json::json!({ "format": HELD_RUNTIMES_FORMAT, "rows": rows }));
    crate::blocking::blocking_io(move || write_launch_record(&dir, HELD_RUNTIMES_FILE, &record)).await
}

/// The runtime rows the launcher of the node-admin in `data_dir` handed it (`write_held_runtimes`):
/// none when it handed none. A hand-off this build cannot read is refused by name.
pub async fn held_runtimes_in(data_dir: &std::path::Path) -> Result<Vec<crate::storage::RuntimeRow>, String> {
    let path = data_dir.join(HELD_RUNTIMES_FILE);
    let read = path.clone();
    let bytes = match crate::blocking::blocking_io(move || std::fs::read(&read)).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("{}: the runtime rows this node-admin's launcher handed it cannot be read: {e}", path.display())),
    };
    #[derive(serde::Deserialize)]
    struct HeldRuntimes {
        format: String,
        rows: serde_json::Value,
    }
    let held: HeldRuntimes = serde_json::from_slice(&bytes).map_err(|e| format!("{}: not a held-runtimes hand-off this build reads: {e}", path.display()))?;
    if held.format != HELD_RUNTIMES_FORMAT {
        return Err(format!("{}: the held-runtimes hand-off is {:?}, and this build reads {HELD_RUNTIMES_FORMAT:?} only", path.display(), held.format));
    }
    serde_json::from_value(held.rows).map_err(|e| format!("{}: the {HELD_RUNTIMES_FORMAT} rows are not runtime rows this build reads: {e}", path.display()))
}

/// The last `n` lines of `text`, joined with ` | `.
pub fn tail(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    lines[lines.len().saturating_sub(n)..].join(" | ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A provider whose locators any admin of the same pool can control
    /// (a remotely controllable provider domain): authority is not bound to
    /// the host that launched a runtime.
    struct RemotePool(&'static str);

    #[async_trait::async_trait]
    impl DeploymentProvider for RemotePool {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            format!("remote:{}", self.0)
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "not in this test".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
            DeploymentStatus::Unknown
        }
        async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
            Ok(Found::NeverLaunched)
        }
    }

    fn fact(domain: &str) -> RuntimeFact {
        RuntimeFact {
            deployment_id: rdm_core::fixture_crockford60("dep-1"),
            provider: RuntimeProvider::Process,
            control_domain: domain.into(),
            locator: RuntimeLocator::Process { pid: 4242, start: 17 },
        }
    }

    /// CONTRACT (`held-runtimes/1`, a durable format: the launcher's build writes it, the launched
    /// admin's build reads it): the hand-off in a launched node-admin's data dir is
    /// `held-runtimes.json`, exactly `{"format": "held-runtimes/1", "rows": [<runtime row>...]}`, read
    /// back to the rows written; a data dir with none hands over no rows; a hand-off of another format
    /// is refused by name, naming the format it holds and the one this build reads. Must NOT happen: a
    /// hand-off this build cannot read answered as no rows.
    #[tokio::test]
    async fn the_held_runtimes_hand_off_is_held_runtimes_1_and_another_format_is_refused_by_name() {
        let dir = crate::test_support::tempdir("held-runtimes");
        assert_eq!(held_runtimes_in(&dir).await.unwrap(), Vec::new(), "a data dir with no hand-off holds no rows");
        let rows = vec![crate::storage::RuntimeRow { node_id: crate::model::NodeId::mint(), name: "mesh1.rpc.1".parse().unwrap(), incarnation_id: crate::model::IncarnationId::mint(), runtime: fact("process:host-a"), data_dir: Some("/data/mesh1.rpc.1".into()) }];
        write_held_runtimes(&dir, &rows).await.unwrap();
        let written: serde_json::Value = serde_json::from_slice(&std::fs::read(dir.join(HELD_RUNTIMES_FILE)).unwrap()).unwrap();
        assert_eq!(written, serde_json::json!({ "format": "held-runtimes/1", "rows": serde_json::to_value(&rows).unwrap() }), "the hand-off's shape");
        assert_eq!(held_runtimes_in(&dir).await.unwrap(), rows, "the launched admin reads the rows its launcher wrote");
        std::fs::write(dir.join(HELD_RUNTIMES_FILE), br#"{"format":"held-runtimes/2","rows":[]}"#).unwrap();
        let refused = held_runtimes_in(&dir).await.unwrap_err();
        assert!(refused.contains("held-runtimes/2") && refused.contains("held-runtimes/1"), "refused naming both formats: {refused}");
    }

    #[test]
    fn a_published_runtime_is_adopted_only_within_its_provider_and_control_domain() {
        let launcher_pool = RemotePool("pool-a");
        let successor_pool = RemotePool("pool-a");
        // Published by one admin's provider, adopted by another admin of the same domain.
        let h = adopt(&successor_pool, &fact(&launcher_pool.control_domain())).unwrap();
        assert_eq!((h.pid, h.start, h.domain.as_deref()), (Some(4242), Some(17), Some("remote:pool-a")));
        assert_eq!(h.fact(), Some(fact("remote:pool-a")), "the adopted handle names the same runtime");
        // Another domain: refused by name, never acted on.
        let other = adopt(&RemotePool("pool-b"), &fact("remote:pool-a")).unwrap_err();
        assert!(matches!(other, AdoptRefusal::ForeignControlDomain { .. }), "{other}");
        assert_eq!(other.reason(), "foreign-control-domain");
        // Another provider's runtime, or an inexact one.
        let container = RuntimeFact {
            provider: RuntimeProvider::Container,
            locator: RuntimeLocator::Container { id: "a".repeat(64) },
            ..fact("remote:pool-a")
        };
        assert!(matches!(adopt(&successor_pool, &container), Err(AdoptRefusal::OtherProvider { .. })));
        let pid_only = RuntimeFact { locator: RuntimeLocator::Process { pid: 4242, start: 0 }, ..fact("remote:pool-a") };
        assert!(matches!(adopt(&successor_pool, &pid_only), Err(AdoptRefusal::Invalid { .. })));
    }

    #[test]
    fn known_values_normalise_and_unset_bootstraps_process() {
        assert_eq!(FabricPolicy::bootstrap(Some("container")).unwrap().provider, ProviderKind::Container);
        assert_eq!(FabricPolicy::bootstrap(Some(" Process ")).unwrap().provider, ProviderKind::Process);
        assert_eq!(FabricPolicy::bootstrap(None).unwrap().provider, ProviderKind::Process);
    }

    #[test]
    fn an_unknown_value_is_a_hard_bootstrap_refusal_with_a_named_reason() {
        for v in ["vm", "", "docker", "processes", "kubernetes"] {
            let err = FabricPolicy::bootstrap(Some(v)).unwrap_err();
            assert_eq!(err, PolicyRefusal::UnknownProvider { value: v.into() });
            assert!(err.to_string().contains("MESH_SPAWN_TYPE"), "{err}");
        }
    }

    #[test]
    fn a_joining_admin_inherits_or_refuses_by_name() {
        let fabric = FabricPolicy::bootstrap(Some("container")).unwrap();
        assert_eq!(FabricPolicy::inherit(fabric, None), Ok(fabric));
        assert_eq!(FabricPolicy::inherit(fabric, Some("CONTAINER")), Ok(fabric));
        assert_eq!(
            FabricPolicy::inherit(fabric, Some("process")),
            Err(PolicyRefusal::ProviderMismatch { fabric: ProviderKind::Container, local: ProviderKind::Process })
        );
        assert!(matches!(FabricPolicy::inherit(fabric, Some("vm")), Err(PolicyRefusal::UnknownProvider { .. })));
    }

    /// CONTRACT: the default failure detail reads `stderr.log` on the blocking pool.
    #[test]
    fn the_failure_detail_is_read_on_the_blocking_pool() {
        let dir = crate::test_support::tempdir("failure-detail-fifo");
        let fifo = dir.join("stderr.log");
        let h = DeploymentHandle { deployment_id: DeploymentId::parse(&rdm_core::fixture_crockford60("dep-a")).unwrap(), provider: ProviderKind::Process, pid: None, start: None, container: None, domain: None };
        let (heard, detail) = crate::test_support::runtime_runs_while_a_fifo_is_read(fifo, b"boom\n".to_vec(), Box::pin(async move { RemotePool("d").failure_detail(&h, &dir).await }));
        assert!(heard, "the runtime stopped while stderr.log was read");
        assert_eq!(detail, "boom");
    }

    /// CONTRACT: a `stderr.log` that cannot be read is named in the detail, never an empty one.
    #[tokio::test]
    async fn an_unreadable_stderr_log_is_named_in_the_failure_detail() {
        let dir = crate::test_support::tempdir("failure-detail-unreadable");
        std::fs::create_dir_all(dir.join("stderr.log")).unwrap();
        let h = DeploymentHandle { deployment_id: DeploymentId::parse(&rdm_core::fixture_crockford60("dep-a")).unwrap(), provider: ProviderKind::Process, pid: None, start: None, container: None, domain: None };
        let detail = RemotePool("d").failure_detail(&h, &dir).await;
        assert!(detail.contains("stderr.log"), "{detail:?}");
    }
}

#[cfg(test)]
mod exit_code_tests {
    use super::*;
    use rdm_core::runtime::{ExitRecord, TRANSPORT_STOPPED_EXIT_CODE};

    fn process_fact(deployment: &str) -> RuntimeFact {
        RuntimeFact { deployment_id: deployment.into(), provider: RuntimeProvider::Process, control_domain: "d".into(), locator: RuntimeLocator::Process { pid: 1, start: 1 } }
    }

    /// CONTRACT: a successor admin reads an unknown exit code (`Exited { code: None }`) as the
    /// process runtime's own recorded code only from the exact record of that deployment and
    /// incarnation; no record, another birth's record, or a provider-given code leave it as it was.
    #[tokio::test]
    async fn an_unknown_exit_code_is_proven_only_by_the_births_own_record() {
        let dir = std::env::temp_dir().join(format!("rafka-exited-verdict-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let unknown = || DeploymentStatus::Exited { code: None };
        assert_eq!(with_exit_code(unknown(), &process_fact("dep-a"), Some(&dir), "inc-a").await.unwrap(), unknown(), "no record proves nothing");
        ExitRecord { deployment_id: "dep-a".into(), incarnation: "inc-a".into(), code: TRANSPORT_STOPPED_EXIT_CODE, reason: "gossip refused".into() }.write(&dir).unwrap();
        assert_eq!(with_exit_code(unknown(), &process_fact("dep-a"), Some(&dir), "inc-a").await.unwrap(), DeploymentStatus::Exited { code: Some(4) });
        assert_eq!(with_exit_code(unknown(), &process_fact("dep-b"), Some(&dir), "inc-a").await.unwrap(), unknown(), "another deployment's record");
        assert_eq!(with_exit_code(unknown(), &process_fact("dep-a"), Some(&dir), "inc-b").await.unwrap(), unknown(), "another incarnation's record");
        assert_eq!(with_exit_code(unknown(), &process_fact("dep-a"), None, "inc-a").await.unwrap(), unknown(), "no data dir known");
        assert_eq!(with_exit_code(DeploymentStatus::Exited { code: Some(137) }, &process_fact("dep-a"), Some(&dir), "inc-a").await.unwrap(), DeploymentStatus::Exited { code: Some(137) }, "a code the provider saw stands");
        assert_eq!(with_exit_code(DeploymentStatus::Running, &process_fact("dep-a"), Some(&dir), "inc-a").await.unwrap(), DeploymentStatus::Running);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT: the exit record is read on the blocking pool, never on a runtime worker.
    #[test]
    fn the_exit_record_is_read_on_the_blocking_pool() {
        let dir = crate::test_support::tempdir("exited-verdict-fifo");
        let bytes = serde_json::to_vec(&ExitRecord { deployment_id: "dep-a".into(), incarnation: "inc-a".into(), code: TRANSPORT_STOPPED_EXIT_CODE, reason: "r".into() }).unwrap();
        let fifo = dir.join("exit.json");
        let (heard, proof) = crate::test_support::runtime_runs_while_a_fifo_is_read(fifo, bytes, Box::pin(async move { with_exit_code(DeploymentStatus::Exited { code: None }, &process_fact("dep-a"), Some(&dir), "inc-a").await }));
        assert!(heard, "the runtime stopped while the exit record was read");
        assert_eq!(proof.unwrap(), DeploymentStatus::Exited { code: Some(4) });
    }

    /// CONTRACT: an exit record that cannot be read is an error naming the file, never an
    /// unknown exit code (which would turn a proven transport stop into a replacement).
    #[tokio::test]
    async fn an_unreadable_exit_record_is_an_error_naming_the_file() {
        let dir = crate::test_support::tempdir("exited-verdict-unreadable");
        std::fs::create_dir_all(dir.join("exit.json")).unwrap();
        let err = with_exit_code(DeploymentStatus::Exited { code: None }, &process_fact("dep-a"), Some(&dir), "inc-a").await.expect_err("an unreadable record is not an absent one");
        assert!(err.contains("exit.json"), "{err}");
    }
}
