//! The process fault backend (i143.e8.s5, #2783): kill, stop (hold) and continue (release) of an
//! EXACT runtime on the process provider.
//!
//! This module is an independent oracle: it reads the published record as JSON and asks the OS
//! itself (`/proc`), and depends on none of the product's crates.
//!
//! The target is the runtime a birth published: its provider control domain, its pid and the
//! kernel's start token of that pid ([`ExactRuntime`], read from the birth's `runtime.json`, the
//! fact a successor adopts from). A pid alone is never a target. Before every signal the backend
//! asks the OS: the control domain must be this host's and the pid's start token must equal the
//! published one; otherwise nothing is signalled and the typed [`Refusal`] names why. A signal is
//! acknowledged by the OS observation it must leave (a stopped process state, an exit) and the
//! result carries it ([`Applied`]).
//!
//! "Hold" is a stop that stays in force until the matching [`Fault::Continue`]: a stopped runtime
//! is alive and silent, and silence never proves death. Only an observed exit is a terminal fact.
//!
//! Each application is one span, `rdm.testkit.fault.update.via-process-signal`, carrying the
//! fault, the exact runtime and the typed outcome.

use serde::Serialize;
use serde_json::{json, Value};
use std::path::Path;
use std::time::{Duration, Instant};

/// One birth's exact process runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExactRuntime {
    pub deployment_id: String,
    pub control_domain: String,
    pub pid: u32,
    pub start: u64,
}

/// What a fault does to the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Fault {
    /// SIGKILL: the runtime flushes nothing.
    Kill,
    /// SIGSTOP: the runtime is held, alive and silent.
    Stop,
    /// SIGCONT: the hold is released.
    Continue,
}

impl Fault {
    pub fn name(self) -> &'static str {
        match self {
            Fault::Kill => "kill",
            Fault::Stop => "stop",
            Fault::Continue => "continue",
        }
    }

    fn signal(self) -> i32 {
        match self {
            Fault::Kill => 9,
            Fault::Stop => 19,
            Fault::Continue => 18,
        }
    }
}

/// Why a fault was not applied. Nothing was signalled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "refusal", rename_all = "snake_case")]
pub enum Refusal {
    /// The data dir holds no runtime record, or one that does not parse.
    NoPublishedFact { reason: String },
    /// The published fact is not a process runtime.
    NotAProcess { provider: String },
    /// The fact's control domain is not this host's: its pid means nothing here.
    ForeignDomain { published: String, local: String },
    /// The process at the pid started at another time: the pid is another process now.
    NotThisRuntime { pid: u32, published_start: u64, observed_start: u64 },
    /// No process runs at the pid.
    AlreadyExited { pid: u32 },
    /// The signal was refused by the OS.
    SignalFailed { pid: u32, errno: i32 },
    /// The signal was sent and the OS did not show its consequence within the bound.
    NotAcknowledged { fault: Fault, pid: u32, state_after: Option<char> },
}

/// The OS observation that acknowledged an applied fault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Applied {
    pub fault: Fault,
    pub runtime: ExactRuntime,
    /// The process state letter after the signal (`T` stopped, `S`/`R` running); `None` once gone.
    pub state_after: Option<char>,
    /// The process is gone (every thread exited): the terminal observation of a kill.
    pub exited: bool,
}

pub type Outcome = Result<Applied, Refusal>;

impl ExactRuntime {
    /// The runtime the birth in `data_dir` published (`runtime.json`).
    pub fn published(data_dir: &Path) -> Result<Self, Refusal> {
        let file = data_dir.join("runtime.json");
        let raw = std::fs::read(&file).map_err(|e| Refusal::NoPublishedFact { reason: format!("{}: {e}", file.display()) })?;
        let v: Value = serde_json::from_slice(&raw).map_err(|e| Refusal::NoPublishedFact { reason: format!("{}: {e}", file.display()) })?;
        let provider = v["provider"].as_str().unwrap_or_default();
        if provider != "process" {
            return Err(Refusal::NotAProcess { provider: provider.into() });
        }
        let field = |what: &str, x: Option<u64>| x.ok_or_else(|| Refusal::NoPublishedFact { reason: format!("{}: no {what}", file.display()) });
        Ok(Self {
            deployment_id: v["deployment_id"].as_str().unwrap_or_default().to_string(),
            control_domain: v["control_domain"].as_str().filter(|d| !d.is_empty()).ok_or_else(|| Refusal::NoPublishedFact { reason: format!("{}: no control_domain", file.display()) })?.to_string(),
            pid: field("pid", v["locator"]["pid"].as_u64())? as u32,
            start: field("start", v["locator"]["start"].as_u64())?,
        })
    }

    /// The same runtime under another start token: what a recycled pid looks like.
    pub fn with_start(&self, start: u64) -> Self {
        Self { start, ..self.clone() }
    }

    /// Is this exact runtime alive: its domain is this host's and the pid still has its start token.
    pub fn check(&self) -> Result<(), Refusal> {
        let local = local_control_domain();
        if self.control_domain != local {
            return Err(Refusal::ForeignDomain { published: self.control_domain.clone(), local });
        }
        match start_token(self.pid) {
            None => Err(Refusal::AlreadyExited { pid: self.pid }),
            Some(s) if s != self.start => Err(Refusal::NotThisRuntime { pid: self.pid, published_start: self.start, observed_start: s }),
            Some(_) if gone(self.pid) => Err(Refusal::AlreadyExited { pid: self.pid }),
            Some(_) => Ok(()),
        }
    }

    /// Apply `fault` to this exact runtime and wait for the OS to acknowledge it.
    pub fn apply(&self, fault: Fault) -> Outcome {
        let span = tracing::info_span!(
            "rdm.testkit.fault.update.via-process-signal",
            fault = fault.name(),
            pid = self.pid,
            start = self.start,
            control_domain = %self.control_domain,
            deployment_id = %self.deployment_id,
            outcome = tracing::field::Empty,
        );
        let _g = span.enter();
        let out = self.apply_inner(fault);
        span.record("outcome", tracing::field::display(match &out {
            Ok(a) => json!({"applied": a.fault.name(), "state_after": a.state_after.map(String::from), "exited": a.exited}),
            Err(r) => serde_json::to_value(r).unwrap_or(Value::Null),
        }));
        out
    }

    fn apply_inner(&self, fault: Fault) -> Outcome {
        self.check()?;
        // SAFETY: kill(2) of a pid whose start token was just checked; no memory is touched.
        let rc = unsafe { libc::kill(self.pid as i32, fault.signal()) };
        if rc != 0 {
            return Err(Refusal::SignalFailed { pid: self.pid, errno: std::io::Error::last_os_error().raw_os_error().unwrap_or(0) });
        }
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let state = proc_state(self.pid);
            let exited = gone(self.pid);
            let acked = match fault {
                Fault::Kill => exited,
                Fault::Stop => state == Some('T'),
                Fault::Continue => state.is_some_and(|c| c != 'T'),
            };
            if acked || Instant::now() >= until {
                return if acked {
                    Ok(Applied { fault, runtime: self.clone(), state_after: if exited { None } else { state }, exited })
                } else {
                    Err(Refusal::NotAcknowledged { fault, pid: self.pid, state_after: state })
                };
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// This host's process control domain: its boot and its pid namespace. A pid and start token mean
/// one process only within the same boot and namespace.
pub fn local_control_domain() -> String {
    let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map(|b| b.trim().to_string()).unwrap_or_else(|e| format!("unknown:{e}"));
    let ns = std::fs::read_link("/proc/self/ns/pid").map(|n| n.display().to_string()).unwrap_or_default();
    format!("process:{boot}:{}", ns.trim_start_matches("pid:[").trim_end_matches(']'))
}

/// The kernel's start time of `pid` (field 22 of `/proc/<pid>/stat`, clock ticks since boot).
pub fn start_token(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit_once(')')?.1.split_whitespace().nth(19)?.parse().ok()
}

/// The OS process state letter of `pid` (`R`, `S`, `T` stopped, `Z`...), or `None` when gone.
pub fn proc_state(pid: u32) -> Option<char> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit(')').next()?.split_whitespace().next()?.chars().next()
}

/// utime + stime ticks the process has used: a stopped process uses none.
pub fn cpu_ticks(pid: u32) -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let f: Vec<&str> = stat.rsplit(')').next().unwrap_or("").split_whitespace().collect();
    f.get(11).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) + f.get(12).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)
}

/// Every task of the thread group has exited (the leader alone reads as a zombie early).
pub fn gone(pid: u32) -> bool {
    match std::fs::read_dir(format!("/proc/{pid}/task")) {
        Ok(tasks) => !tasks.flatten().any(|t| std::fs::read_to_string(t.path().join("stat")).is_ok_and(|s| s.rsplit(')').next().is_some_and(|r| !r.trim_start().starts_with('Z')))),
        Err(_) => true,
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn the_oracle_agrees_with_the_products_own_token_and_domain() {
        let me = std::process::id();
        assert_eq!(Some(start_token(me).unwrap()), rafka_mesh_entity::runtime::process_start_token(me));
        assert_eq!(local_control_domain(), rafka_mesh_entity::runtime::process_control_domain().unwrap());
    }

    use std::process::{Command, Stdio};

    fn sleeper() -> (std::process::Child, ExactRuntime) {
        let c = Command::new("sleep").arg("60").stdout(Stdio::null()).spawn().unwrap();
        let pid = c.id();
        let rt = ExactRuntime { deployment_id: "dep".into(), control_domain: local_control_domain(), pid, start: start_token(pid).unwrap() };
        (c, rt)
    }

    #[test]
    fn a_forged_start_or_foreign_domain_signals_nothing_and_the_exact_runtime_is_stopped_continued_and_killed() {
        let (mut child, rt) = sleeper();
        let forged = rt.with_start(rt.start + 1);
        assert!(matches!(forged.apply(Fault::Kill), Err(Refusal::NotThisRuntime { .. })));
        let foreign = ExactRuntime { control_domain: "process:another-host:1".into(), ..rt.clone() };
        assert!(matches!(foreign.apply(Fault::Kill), Err(Refusal::ForeignDomain { .. })));
        assert!(!gone(rt.pid), "nothing was signalled");
        let stopped = rt.apply(Fault::Stop).unwrap();
        assert_eq!(stopped.state_after, Some('T'));
        assert_eq!(rt.apply(Fault::Continue).unwrap().exited, false);
        let killed = rt.apply(Fault::Kill).unwrap();
        assert!(killed.exited);
        let _ = child.wait();
        assert!(matches!(rt.apply(Fault::Kill), Err(Refusal::AlreadyExited { .. })));
    }
}
