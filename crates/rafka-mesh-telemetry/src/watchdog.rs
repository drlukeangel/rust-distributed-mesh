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
        *counts.entry(format!("{comm}:{state}:{}", if wchan.is_empty() || wchan == "0" { "-" } else { &wchan })).or_default() += 1;
    }
    let mut v: Vec<(String, u32)> = counts.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1));
    v.into_iter().map(|(k, n)| format!("{k}x{n}")).collect::<Vec<_>>().join(" ")
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
            loop {
                std::thread::sleep(BEAT);
                let last = beat.load(Ordering::Acquire);
                let now = now_ms(origin);
                match &mut stalled {
                    None if last > 0 && now.saturating_sub(last) > STALL.as_millis() as u64 => {
                        stalled = Some((last, runqueue_wait_ms(), vec![thread_states()]));
                    }
                    Some((_, _, samples)) if now.saturating_sub(last) > STALL.as_millis() as u64 => {
                        if samples.len() < 4 {
                            samples.push(thread_states());
                        }
                    }
                    Some((from, rq0, samples)) => {
                        let stall_ms = last.saturating_sub(*from);
                        tracing::warn!(
                            step = "runtime-stall",
                            stall_ms,
                            runqueue_wait_ms = runqueue_wait_ms().saturating_sub(*rq0),
                            threads = %samples.join(" | "),
                            "the async runtime ran nothing for {stall_ms} ms"
                        );
                        stalled = None;
                    }
                    None => {}
                }
            }
        })
        .ok()?;
    Some(())
}
