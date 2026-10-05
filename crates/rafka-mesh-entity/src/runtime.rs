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
