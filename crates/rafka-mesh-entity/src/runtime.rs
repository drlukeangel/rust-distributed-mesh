//! The exact runtime of a birth (i143.e4.s16).
//!
//! A [`RuntimeFact`] names the one provider runtime realising one process
//! birth: its deployment, the provider, the provider control domain the
//! locator is meaningful in, and an exact, non-reusable locator. It rides the
//! birth itself ([`crate::MeshNode::runtime`]), so it is current membership
//! state, not Build history: any admin that hears the birth knows where and
//! how its runtime can be inspected or stopped, whoever launched it.
//!
//! - process: `process:<boot id>:<pid namespace>` + pid + process-start token
//!   (the kernel's start time of that pid), so a recycled pid never matches;
//! - container: `container:<daemon id>` + the immutable container id, never
//!   the (deterministic, reusable) container name.
//!
//! A locator is meaningful only inside its control domain: a fact from
//! another domain is refused, never acted on. The fact is a locator, not a
//! credential, and it is immutable for its birth: the same incarnation
//! offering another locator or domain is refused by name.
//!
//! Who produces it: the provider, right after it realised the runtime, writes
//! the fact to the birth's data dir ([`RUNTIME_FILE`]); the runtime checks the
//! record describes itself and publishes it with its birth. The first admin
//! of a fabric, which nobody launched, adopts its own process
//! ([`RuntimeFact::of_this_process`]).

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;

/// The record a provider writes into a birth's data dir.
pub const RUNTIME_FILE: &str = "runtime.json";

/// The largest encoded fact: it rides every digest, so it stays small.
pub const MAX_FACT_BYTES: usize = 384;

/// Which provider realised the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeProvider {
    Process,
    Container,
}

impl RuntimeProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Process => "process",
            Self::Container => "container",
        }
    }
}

/// The exact runtime inside its control domain.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum RuntimeLocator {
    /// A pid and the kernel's start time of that process (clock ticks since
    /// boot): together they never name a later process.
    Process { pid: u32, start: u64 },
    /// The immutable container id (64 lowercase hex).
    Container { id: String },
}

impl RuntimeLocator {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Process { .. } => "process-pid-start",
            Self::Container { .. } => "container-id",
        }
    }
}

/// One birth's exact runtime.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeFact {
    pub deployment_id: String,
    pub provider: RuntimeProvider,
    /// Opaque, compared by equality: where `locator` means something.
    pub control_domain: String,
    pub locator: RuntimeLocator,
}

/// Why a fact is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeFactError {
    /// A field is missing or malformed.
    Malformed { reason: String },
    /// The encoded fact exceeds [`MAX_FACT_BYTES`].
    TooLarge { bytes: usize },
    /// The provider and the locator disagree.
    ProviderMismatch { provider: RuntimeProvider, locator: &'static str },
    /// The record describes another runtime than the one reading it.
    NotThisRuntime { reason: String },
}

impl fmt::Display for RuntimeFactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed { reason } => write!(f, "malformed runtime fact: {reason}"),
            Self::TooLarge { bytes } => write!(f, "runtime fact of {bytes} bytes exceeds {MAX_FACT_BYTES}"),
            Self::ProviderMismatch { provider, locator } => write!(f, "a {} runtime fact cannot carry a {locator} locator", provider.as_str()),
            Self::NotThisRuntime { reason } => write!(f, "the runtime record does not describe this runtime: {reason}"),
        }
    }
}

impl RuntimeFact {
    /// Every field present and well formed, the provider matching the
    /// locator, the whole within [`MAX_FACT_BYTES`].
    pub fn validate(&self) -> Result<(), RuntimeFactError> {
        let bad = |reason: &str| Err(RuntimeFactError::Malformed { reason: reason.into() });
        if self.deployment_id.trim().is_empty() {
            return bad("no deployment id");
        }
        if self.control_domain.trim().is_empty() {
            return bad("no provider control domain");
        }
        match (&self.locator, self.provider) {
            (RuntimeLocator::Process { pid, start }, RuntimeProvider::Process) => {
                if *pid == 0 || *start == 0 {
                    return bad("a process locator needs a pid and its start token");
                }
            }
            (RuntimeLocator::Container { id }, RuntimeProvider::Container) => {
                if id.len() != 64 || !id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
                    return bad("a container locator is the 64-hex immutable container id");
                }
            }
            (l, p) => return Err(RuntimeFactError::ProviderMismatch { provider: p, locator: l.kind() }),
        }
        let bytes = serde_json::to_vec(self).map(|v| v.len()).unwrap_or(usize::MAX);
        if bytes > MAX_FACT_BYTES {
            return Err(RuntimeFactError::TooLarge { bytes });
        }
        Ok(())
    }

    /// A stable short fingerprint of the control domain, for evidence.
    pub fn domain_fingerprint(&self) -> String {
        fingerprint(&self.control_domain)
    }

    /// A stable short fingerprint of the locator, for evidence.
    pub fn locator_fingerprint(&self) -> String {
        fingerprint(&serde_json::to_string(&self.locator).unwrap_or_default())
    }

    /// The fact of the process reading it, under `deployment_id`: how a
    /// runtime nobody launched (the first admin) adopts itself.
    /// This process's runtime as the container it runs in: its immutable id (from its own mount
    /// table) in `control_domain`, the container runtime that runs it.
    pub fn of_this_container(deployment_id: &str, control_domain: &str) -> Result<Self, String> {
        let id = this_container_id().ok_or("this process runs in no container (no container id in /proc/self/mountinfo)")?;
        let fact = Self { deployment_id: deployment_id.into(), provider: RuntimeProvider::Container, control_domain: control_domain.into(), locator: RuntimeLocator::Container { id } };
        fact.validate().map_err(|e| e.to_string())?;
        Ok(fact)
    }

    pub fn of_this_process(deployment_id: &str) -> Result<Self, String> {
        let pid = std::process::id();
        let start = process_start_token(pid).ok_or_else(|| format!("no start token for pid {pid}"))?;
        let fact = Self {
            deployment_id: deployment_id.into(),
            provider: RuntimeProvider::Process,
            control_domain: process_control_domain()?,
            locator: RuntimeLocator::Process { pid, start },
        };
        fact.validate().map_err(|e| e.to_string())?;
        Ok(fact)
    }

    /// Check a provider-written record describes the runtime reading it:
    /// for a process, its own pid, start token and domain; for a container,
    /// a container id its hostname (the id's short form) begins.
    pub fn verify_is_this_runtime(&self) -> Result<(), RuntimeFactError> {
        self.validate()?;
        let not = |reason: String| Err(RuntimeFactError::NotThisRuntime { reason });
        match &self.locator {
            RuntimeLocator::Process { pid, start } => {
                let me = std::process::id();
                if *pid != me {
                    return not(format!("it names pid {pid}, this process is {me}"));
                }
                if process_start_token(me) != Some(*start) {
                    return not(format!("pid {pid}'s start token is not {start}"));
                }
                match process_control_domain() {
                    Ok(d) if d == self.control_domain => Ok(()),
                    Ok(d) => not(format!("it names domain {}, this process runs in {d}", self.control_domain)),
                    Err(e) => not(e),
                }
            }
            RuntimeLocator::Container { id } => {
                let host = std::fs::read_to_string("/etc/hostname").unwrap_or_default();
                let host = host.trim();
                if host.len() >= 12 && id.starts_with(host) {
                    Ok(())
                } else {
                    not(format!("container {} is not this container ({host})", &id[..12.min(id.len())]))
                }
            }
        }
    }

    /// Write this fact as `dir`'s runtime record (atomically).
    pub fn write_record(&self, dir: &Path) -> Result<(), String> {
        let tmp = dir.join(format!("{RUNTIME_FILE}.tmp"));
        let body = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        std::fs::write(&tmp, body).map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, dir.join(RUNTIME_FILE)).map_err(|e| format!("{RUNTIME_FILE}: {e}"))
    }

    /// `dir`'s runtime record, if one is written.
    pub fn read_record(dir: &Path) -> Option<Result<Self, String>> {
        let raw = std::fs::read(dir.join(RUNTIME_FILE)).ok()?;
        Some(serde_json::from_slice(&raw).map_err(|e| format!("{RUNTIME_FILE}: {e}")))
    }
}

/// The record the provider writes for the runtime reading it, once it is
/// written and describes this runtime (a record an earlier birth left in the
/// same data dir does not); a named refusal after `within`.
pub fn await_own_record(dir: &Path, within: std::time::Duration) -> Result<RuntimeFact, String> {
    let until = std::time::Instant::now() + within;
    let mut last = format!("no {RUNTIME_FILE} in {}", dir.display());
    loop {
        match RuntimeFact::read_record(dir) {
            Some(Ok(f)) => match f.verify_is_this_runtime() {
                Ok(()) => return Ok(f),
                Err(e) => last = e.to_string(),
            },
            Some(Err(e)) => last = e,
            None => {}
        }
        if std::time::Instant::now() >= until {
            return Err(format!("the provider wrote no runtime record for this runtime within {within:?}: {last}"));
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// The first 12 hex of the blake3 of `s`.
pub fn fingerprint(s: &str) -> String {
    blake3::hash(s.as_bytes()).to_hex()[..12].to_string()
}

/// The kernel's start time of `pid` (field 22 of `/proc/<pid>/stat`, clock
/// ticks since boot); `None` when it does not run or on another OS.
pub fn process_start_token(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name is parenthesised and may hold spaces: count after it.
    let rest = stat.rsplit_once(')')?.1;
    rest.split_whitespace().nth(19)?.parse().ok()
}

/// This host's process control domain: its boot and its pid namespace. A pid
/// and start token mean one process only within the same boot and namespace.
pub fn process_control_domain() -> Result<String, String> {
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map_err(|e| format!("boot id: {e}"))?;
    let ns = std::fs::read_link("/proc/self/ns/pid").map_err(|e| format!("pid namespace: {e}"))?;
    let ns = ns.display().to_string();
    let ns = ns.trim_start_matches("pid:[").trim_end_matches(']');
    Ok(format!("process:{}:{ns}", boot.trim()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn container(id: &str) -> RuntimeFact {
        RuntimeFact {
            deployment_id: "dep-1".into(),
            provider: RuntimeProvider::Container,
            control_domain: "container:daemon-a".into(),
            locator: RuntimeLocator::Container { id: id.into() },
        }
    }

    #[test]
    fn this_process_adopts_itself_with_its_start_token_and_domain() {
        let me = RuntimeFact::of_this_process("dep-1").unwrap();
        assert_eq!(me.verify_is_this_runtime(), Ok(()));
        let RuntimeLocator::Process { pid, start } = me.locator else { panic!() };
        assert_eq!(pid, std::process::id());
        assert_eq!(Some(start), process_start_token(pid));
        assert!(me.control_domain.starts_with("process:"));
        // The same pid with another start token is another process.
        let reused = RuntimeFact { locator: RuntimeLocator::Process { pid, start: start + 1 }, ..me.clone() };
        assert!(matches!(reused.verify_is_this_runtime(), Err(RuntimeFactError::NotThisRuntime { .. })));
        let elsewhere = RuntimeFact { control_domain: "process:another-host:1".into(), ..me };
        assert!(matches!(elsewhere.verify_is_this_runtime(), Err(RuntimeFactError::NotThisRuntime { .. })));
    }

    #[test]
    fn a_fact_is_exact_or_refused_by_name() {
        assert_eq!(container(&"a".repeat(64)).validate(), Ok(()));
        // A container name is not an immutable id.
        assert!(matches!(container("rafka-mesh1-rpc-1-dep").validate(), Err(RuntimeFactError::Malformed { .. })));
        let pid_only = RuntimeFact {
            provider: RuntimeProvider::Process,
            locator: RuntimeLocator::Process { pid: 42, start: 0 },
            ..container(&"a".repeat(64))
        };
        assert!(pid_only.validate().unwrap_err().to_string().contains("start token"));
        let crossed = RuntimeFact { provider: RuntimeProvider::Process, ..container(&"a".repeat(64)) };
        assert!(matches!(crossed.validate(), Err(RuntimeFactError::ProviderMismatch { .. })));
        let huge = RuntimeFact { control_domain: "x".repeat(MAX_FACT_BYTES), ..container(&"a".repeat(64)) };
        assert!(matches!(huge.validate(), Err(RuntimeFactError::TooLarge { .. })));
    }

    #[test]
    fn a_record_round_trips_through_the_data_dir() {
        let dir = std::env::temp_dir().join(format!("rafka-runtime-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        assert!(RuntimeFact::read_record(&dir).is_none());
        let f = container(&"b".repeat(64));
        f.write_record(&dir).unwrap();
        assert_eq!(RuntimeFact::read_record(&dir).unwrap().unwrap(), f);
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(f.domain_fingerprint().len(), 12);
        assert_ne!(f.locator_fingerprint(), container(&"c".repeat(64)).locator_fingerprint());
    }
}

/// The immutable id of the container this process runs in: the container runtime mounts the
/// container's own files (`/etc/hostname`, `/etc/hosts`) from `.../containers/<id>/`, and the mount
/// table names that path. `None` outside a container.
pub fn this_container_id() -> Option<String> {
    let table = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    container_id_in_mountinfo(&table)
}

fn container_id_in_mountinfo(table: &str) -> Option<String> {
    table.lines().find_map(|line| {
        let parts: Vec<&str> = line.split('/').collect();
        parts.windows(2).find_map(|w| (w[0] == "containers" && w[1].len() == 64 && w[1].chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())).then(|| w[1].to_string()))
    })
}

#[cfg(test)]
mod container_id_tests {
    use super::container_id_in_mountinfo;

    #[test]
    fn a_container_reads_its_id_from_its_own_mount_table_and_a_host_process_reads_none() {
        let id = "8495c98a3c3b1f0d6e2a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
        let table = format!("1 0 0:1 / / rw - overlay overlay rw\n2 1 259:2 /var/lib/docker/containers/{id}/hostname /etc/hostname rw - ext4 /dev/x rw\n");
        assert_eq!(container_id_in_mountinfo(&table).as_deref(), Some(id));
        assert_eq!(container_id_in_mountinfo("24 1 259:2 / / rw - ext4 /dev/nvme0n1p2 rw\n"), None);
        assert_eq!(container_id_in_mountinfo("2 1 259:2 /var/lib/docker/containers/ABC/hostname /etc/hostname rw\n"), None, "never a short or malformed id");
    }
}

/// The exit code of a runtime whose mesh transport stopped for good (`TRANSPORT_STOPPED`): a
/// named terminal reason, reserved. No other exit uses it, and no code is ever inferred as this
/// one from a network symptom.
pub const TRANSPORT_STOPPED_EXIT_CODE: i32 = 4;

/// The record a process runtime writes into its own data dir before it exits on purpose.
pub const EXIT_FILE: &str = "exit.json";

/// A runtime's own statement of why it ended, keyed to the one deployment and incarnation that
/// wrote it. It is the proof a successor admin reads for a process it did not launch, whose exit
/// code its provider cannot see.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitRecord {
    pub deployment_id: String,
    pub incarnation: String,
    pub code: i32,
    pub reason: String,
}

impl ExitRecord {
    /// Write this record as `dir`'s exit record (atomically).
    pub fn write(&self, dir: &Path) -> Result<(), String> {
        let tmp = dir.join(format!("{EXIT_FILE}.tmp"));
        let body = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        std::fs::write(&tmp, body).map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, dir.join(EXIT_FILE)).map_err(|e| format!("{EXIT_FILE}: {e}"))
    }

    /// The exit code `dir` proves for exactly this `deployment_id` and `incarnation`: `None` when
    /// no record is written or the record is another birth's.
    pub fn proven_code(dir: &Path, deployment_id: &str, incarnation: &str) -> Option<i32> {
        let raw = std::fs::read(dir.join(EXIT_FILE)).ok()?;
        let r: Self = serde_json::from_slice(&raw).ok()?;
        (r.deployment_id == deployment_id && r.incarnation == incarnation).then_some(r.code)
    }

    /// Remove `dir`'s exit record (a new birth in the data dir starts without its predecessor's).
    pub fn clear(dir: &Path) {
        let _ = std::fs::remove_file(dir.join(EXIT_FILE));
    }
}

/// The identity this process exits under: its data dir, deployment and incarnation.
#[derive(Debug, Clone)]
pub struct OwnExit {
    pub data_dir: std::path::PathBuf,
    pub deployment_id: String,
    pub incarnation: String,
}

static OWN_EXIT: std::sync::OnceLock<OwnExit> = std::sync::OnceLock::new();

/// Name the identity this process records its intentional exit under. Set once, at boot.
pub fn set_own_exit(own: OwnExit) {
    let _ = OWN_EXIT.set(own);
}

/// The process's mesh transport stopped for good: record why, durably, in the data dir, then
/// exit with [`TRANSPORT_STOPPED_EXIT_CODE`]. The record is what lets a successor admin prove the
/// reason for a process it did not launch.
pub fn exit_transport_stopped(reason: &str) -> ! {
    match OWN_EXIT.get() {
        Some(own) => {
            let record = ExitRecord { deployment_id: own.deployment_id.clone(), incarnation: own.incarnation.clone(), code: TRANSPORT_STOPPED_EXIT_CODE, reason: reason.to_string() };
            if let Err(e) = record.write(&own.data_dir) {
                eprintln!("the exit record could not be written to {}: {e}", own.data_dir.display());
            }
        }
        None => eprintln!("this process named no exit identity; no exit record is written"),
    }
    std::process::exit(TRANSPORT_STOPPED_EXIT_CODE)
}

#[cfg(test)]
mod exit_record_tests {
    use super::*;

    #[test]
    fn an_exit_record_proves_its_own_birth_and_no_other() {
        let dir = std::env::temp_dir().join(format!("rafka-exit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(ExitRecord::proven_code(&dir, "dep-a", "inc-a"), None);
        ExitRecord { deployment_id: "dep-a".into(), incarnation: "inc-a".into(), code: TRANSPORT_STOPPED_EXIT_CODE, reason: "gossip refused".into() }.write(&dir).unwrap();
        assert_eq!(ExitRecord::proven_code(&dir, "dep-a", "inc-a"), Some(4));
        assert_eq!(ExitRecord::proven_code(&dir, "dep-b", "inc-a"), None, "another deployment's record proves nothing");
        assert_eq!(ExitRecord::proven_code(&dir, "dep-a", "inc-b"), None, "another incarnation's record proves nothing");
        ExitRecord::clear(&dir);
        assert_eq!(ExitRecord::proven_code(&dir, "dep-a", "inc-a"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
