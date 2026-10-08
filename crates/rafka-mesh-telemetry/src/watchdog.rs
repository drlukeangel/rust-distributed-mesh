//! The runtime watchdog: proof, from the process itself, of a window in which its async runtime
//! ran nothing. A tokio task stamps a heartbeat every [`BEAT`]; a plain OS thread (outside the
//! runtime, so it runs when the runtime does not) reads it. When the stamp is older than
//! [`STALL`], the thread samples what every thread of the process is doing (`/proc/self/task`:
//! state and kernel wait channel); when the runtime runs again it logs the stall's length, the
//! runqueue wait the process accrued in it, and the samples. Telemetry only: nothing acts on it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How often the runtime stamps its heartbeat.
pub const BEAT: Duration = Duration::from_millis(100);
/// A heartbeat older than this is a stall.
pub const STALL: Duration = Duration::from_millis(500);

fn now_ms(origin: Instant) -> u64 {
    origin.elapsed().as_millis() as u64
}

/// The summed runqueue wait of every thread of this process, in ms (`/proc/self/task/*/schedstat`).
fn runqueue_wait_ms() -> u64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else { return 0 };
    let mut ns = 0u64;
    for t in tasks.flatten() {
        if let Ok(s) = std::fs::read_to_string(t.path().join("schedstat")) {
            ns += s.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        }
    }
    ns / 1_000_000
}

/// What every thread of this process is doing now: `comm:state:wchan` with a count each, the most
/// common first (state R = running, S = sleeping, D = uninterruptible; wchan = the kernel
/// function a sleeping thread waits in).
pub fn thread_states() -> String {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else { return String::new() };
    let mut counts: std::collections::BTreeMap<String, u32> = std::collections::BTreeMap::new();
    for t in tasks.flatten() {
        let p = t.path();
        let comm = std::fs::read_to_string(p.join("comm")).map(|c| c.trim().to_string()).unwrap_or_default();
        let state = std::fs::read_to_string(p.join("stat")).ok().and_then(|s| s.rsplit(')').next().and_then(|r| r.split_whitespace().next()).map(str::to_string)).unwrap_or_default();
        let wchan = std::fs::read_to_string(p.join("wchan")).map(|w| w.trim().to_string()).unwrap_or_default();
        // A thread in uninterruptible sleep is named by what it is doing: its syscall and the file
        // behind the descriptor it passed (readable for this process's own threads).
        let doing = if state == "D" { blocked_call(&p) } else { String::new() };
        *counts.entry(format!("{comm}:{state}:{}{doing}", if wchan.is_empty() || wchan == "0" { "-" } else { &wchan })).or_default() += 1;
    }
    let mut v: Vec<(String, u32)> = counts.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1));
    v.into_iter().map(|(k, n)| format!("{k}x{n}")).collect::<Vec<_>>().join(" ")
}

/// `[<syscall> fd=<n> <file>]` for a thread blocked in a syscall that names a descriptor;
/// `[syscall <nr>]` for any other. The numbering is the build target's.
fn blocked_call(task: &std::path::Path) -> String {
    let s = match std::fs::read_to_string(task.join("syscall")) {
        Ok(s) => s,
        Err(e) => return format!("[syscall unreadable: {e}]"),
    };
    let mut f = s.split_whitespace();
    let Some(nr) = f.next().and_then(|n| n.parse::<i64>().ok()) else { return format!("[{}]", s.trim()) };
    // (name, takes a descriptor first)
    #[cfg(target_arch = "aarch64")]
    let call: Option<(&str, bool)> = match nr {
        56 => Some(("openat", false)),
        79 => Some(("newfstatat", false)),
        291 => Some(("statx", false)),
        78 => Some(("readlinkat", false)),
        48 => Some(("faccessat", false)),
        57 => Some(("close", true)),
        63 => Some(("read", true)),
        64 => Some(("write", true)),
        65 => Some(("readv", true)),
        66 => Some(("writev", true)),
        67 => Some(("pread64", true)),
        68 => Some(("pwrite64", true)),
        82 => Some(("fsync", true)),
        83 => Some(("fdatasync", true)),
        46 => Some(("ftruncate", true)),
        38 => Some(("renameat", false)),
        276 => Some(("renameat2", false)),
        35 => Some(("unlinkat", false)),
        _ => None,
    };
    #[cfg(not(target_arch = "aarch64"))]
    let call: Option<(&str, bool)> = match nr {
        0 => Some(("read", true)),
        1 => Some(("write", true)),
        3 => Some(("close", true)),
        17 => Some(("pread64", true)),
        18 => Some(("pwrite64", true)),
        19 => Some(("readv", true)),
        20 => Some(("writev", true)),
        74 => Some(("fsync", true)),
        75 => Some(("fdatasync", true)),
        77 => Some(("ftruncate", true)),
        82 => Some(("rename", false)),
        87 => Some(("unlink", false)),
        257 => Some(("openat", false)),
        264 => Some(("renameat", false)),
        316 => Some(("renameat2", false)),
        _ => None,
    };
    let args: Vec<u64> = f.by_ref().take(6).filter_map(|a| u64::from_str_radix(a.trim_start_matches("0x"), 16).ok()).collect();
    // A call that takes a path passes its address as the first (open, stat, unlink, rename) or
    // second (the *at calls) argument; the address is in this very process, so it is read back
    // through /proc/self/mem, which refuses an unreadable address instead of faulting.
    if let Some(path_arg) = path_arg_index(nr) {
        let name = call.map(|(n, _)| n.to_string()).unwrap_or_else(|| format!("syscall {nr}"));
        return match args.get(path_arg).copied().and_then(read_c_string) {
            Some(p) => format!("[{name} {p}]"),
            None => format!("[{name}]"),
        };
    }
    let fd = args.first().map(|a| *a as i64);
    match (call, fd) {
        (None, _) => format!("[syscall {nr}]"),
        (Some((n, true)), Some(fd)) => {
            let file = std::fs::read_link(format!("/proc/self/fd/{fd}")).map(|p| p.display().to_string()).unwrap_or_else(|_| "?".into());
            format!("[{n} fd={fd} {file}]")
        }
        (Some((n, _)), _) => format!("[{n}]"),
    }
}

/// Which argument of syscall `nr` is a path pointer (the *at calls take a directory descriptor first).
fn path_arg_index(nr: i64) -> Option<usize> {
    #[cfg(target_arch = "aarch64")]
    return match nr {
        56 | 35 | 38 | 276 | 78 | 79 | 291 | 48 | 34 | 33 => Some(1),
        _ => None,
    };
    #[cfg(not(target_arch = "aarch64"))]
    return match nr {
        257 | 262 | 263 | 264 | 316 | 332 | 269 => Some(1),
        2 | 4 | 6 | 82 | 87 | 21 => Some(0),
        _ => None,
    };
}

/// The NUL-terminated string at `addr` in this process, up to 256 bytes.
fn read_c_string(addr: u64) -> Option<String> {
    use std::os::unix::fs::FileExt as _;
    let mem = std::fs::File::open("/proc/self/mem").ok()?;
    let mut buf = [0u8; 256];
    let n = mem.read_at(&mut buf, addr).ok()?;
    let end = buf[..n].iter().position(|b| *b == 0).unwrap_or(n);
    Some(String::from_utf8_lossy(&buf[..end]).into_owned())
}

/// Start the watchdog on the current tokio runtime; `None` outside one.
pub fn spawn() -> Option<()> {
    let handle = tokio::runtime::Handle::try_current().ok()?;
    let origin = Instant::now();
    let beat = Arc::new(AtomicU64::new(0));
    let b = beat.clone();
    handle.spawn(async move {
        loop {
            b.store(now_ms(origin), Ordering::Release);
            tokio::time::sleep(BEAT).await;
        }
    });
    std::thread::Builder::new()
        .name("rdm-watchdog".into())
        .spawn(move || {
            let mut stalled: Option<(u64, u64, Vec<String>)> = None;
            let mut profiled = false;
            let mut frozen_ms = 0u64;
            loop {
                let slept_from = Instant::now();
                std::thread::sleep(BEAT);
                // This thread runs outside the runtime: if its own short sleep overran by more than
                // the stall threshold, the whole process was not running (SIGSTOP, a frozen
                // cgroup, a suspended host), and the window belongs to that, not to the runtime.
                let overran = slept_from.elapsed().saturating_sub(BEAT);
                if overran > STALL {
                    frozen_ms += overran.as_millis() as u64;
                }
                let last = beat.load(Ordering::Acquire);
                let now = now_ms(origin);
                match &mut stalled {
                    None if last > 0 && now.saturating_sub(last) > STALL.as_millis() as u64 => {
                        let states = thread_states();
                        // Diagnosis runs only (RDM_STALL_PERF set): profile this process while
                        // its workers are busy, once, into the evidence directory.
                        if states.contains(":R:") && !profiled {
                            if let (Ok(_), Ok(dir)) = (std::env::var("RDM_STALL_PERF"), std::env::var("RDM_EVIDENCE_DIR")) {
                                profiled = true;
                                let out = format!("{dir}/stall-{}-{now}.perf.data", std::process::id());
                                let r = std::process::Command::new("perf")
                                    .args(["record", "-q", "-F", "499", "--call-graph", "dwarf,16384", "-p", &std::process::id().to_string(), "-o", &out, "--", "sleep", "1"])
                                    .stdout(std::process::Stdio::null())
                                    .stderr(std::process::Stdio::null())
                                    .spawn();
                                tracing::warn!(step = "runtime-stall-profile", path = %out, started = r.is_ok(), "profiling the stalled runtime's busy workers");
                            }
                        }
                        stalled = Some((last, runqueue_wait_ms(), vec![states]));
                    }
                    Some((_, _, samples)) if now.saturating_sub(last) > STALL.as_millis() as u64 => {
                        if samples.len() < 4 {
                            samples.push(thread_states());
                        }
                    }
                    Some((from, rq0, samples)) => {
                        let stall_ms = last.saturating_sub(*from);
                        if frozen_ms > 0 {
                            tracing::info!(
                                step = "process-frozen",
                                stall_ms,
                                frozen_ms,
                                "the whole process was not running for {frozen_ms} ms (stopped from outside); the runtime itself did not stall"
                            );
                        } else {
                            tracing::warn!(
                                step = "runtime-stall",
                                stall_ms,
                                runqueue_wait_ms = runqueue_wait_ms().saturating_sub(*rq0),
                                threads = %samples.join(" | "),
                                "the async runtime ran nothing for {stall_ms} ms"
                            );
                        }
                        stalled = None;
                        frozen_ms = 0;
                    }
                    None => frozen_ms = 0,
                }
            }
        })
        .ok()?;
    Some(())
}
