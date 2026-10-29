//! Hang capture for the testkit's own test executable (i143.e12.s34, #3060). Compiled in only for a
//! build with `RUSTFLAGS="--cfg tokio_unstable --cfg tokio_taskdump"` and tokio's `taskdump`
//! feature (`--features tokio/taskdump` on that one cargo command); every other build, the product
//! role binaries included, compiles [`watched`] as a plain `.await`. Even then it is off unless
//! `RDM_TASKDUMP_AFTER_MS` is set: it has no default.
//!
//! A watched start or stop that has not finished `RDM_TASKDUMP_AFTER_MS` after it began prints to
//! stderr, under a header naming the test thread:
//! - its own await chain (the future under the test's `block_on` is no task, so `Handle::dump`
//!   never holds it): one re-poll of the watched future inside `Trace::capture`;
//! - every task of the test's runtime (`Handle::dump`): the gossip actor, the router's run task,
//!   the endpoint and connection drivers, whichever side of the handshake is waiting.

use std::future::Future;

/// `fut`, awaited; with the capture build and `RDM_TASKDUMP_AFTER_MS` set, a hang is dumped.
#[cfg(not(all(tokio_unstable, tokio_taskdump)))]
pub async fn watched<F: Future>(_what: &'static str, fut: F) -> F::Output {
    fut.await
}

/// `fut`, awaited; when `RDM_TASKDUMP_AFTER_MS` elapses before `fut` finishes, dumps the watched future's await chain and the runtime's tasks to stderr.
#[cfg(all(tokio_unstable, tokio_taskdump))]
pub async fn watched<F: Future>(what: &'static str, fut: F) -> F::Output {
    use std::fmt::Write;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::Waker;
    use std::time::Duration;
    use tokio::runtime::dump::Trace;

    let Some(after) = std::env::var("RDM_TASKDUMP_AFTER_MS").ok().and_then(|v| v.parse::<u64>().ok()).map(Duration::from_millis) else {
        return fut.await;
    };
    let test = std::thread::current().name().unwrap_or("<unnamed thread>").to_string();
    struct Fire {
        fired: AtomicBool,
        waker: Mutex<Option<Waker>>,
    }
    let fire = Arc::new(Fire { fired: AtomicBool::new(false), waker: Mutex::new(None) });
    let handle = tokio::runtime::Handle::current();
    // Every task of the runtime, untied: it fires even if this step finished and the test hangs at
    // a later await, and it dies with the test's runtime when the test ends.
    {
        let (rt, test) = (handle.clone(), test.clone());
        handle.spawn(async move {
            tokio::time::sleep(after).await;
            match tokio::time::timeout(Duration::from_secs(10), rt.dump()).await {
                Ok(dump) => {
                    let mut out = String::new();
                    for (i, task) in dump.tasks().iter().enumerate() {
                        let _ = writeln!(out, "--- task {i} (id {}):\n{}", task.id(), task.trace());
                    }
                    eprintln!("=== RDM_TASKDUMP test {test} still running {after:?} after its {what} began: every task of its runtime\n{out}=== RDM_TASKDUMP end");
                }
                Err(_) => eprintln!("=== RDM_TASKDUMP test {test}: Handle::dump did not resolve in 10s: a runtime worker is blocked over 250 ms"),
            }
        });
    }
    // The step's own await chain, tied to the step: the watched future, re-polled once inside a
    // capture if it has not finished by then.
    let watchdog = {
        let (fire, test) = (fire.clone(), test.clone());
        handle.spawn(async move {
            tokio::time::sleep(after).await;
            eprintln!("=== RDM_TASKDUMP {what} in test {test}: not done after {after:?}");
            fire.fired.store(true, Ordering::SeqCst);
            if let Some(w) = fire.waker.lock().unwrap().take() {
                w.wake();
            }
        })
    };
    let mut fut = std::pin::pin!(fut);
    let out = Trace::root(std::future::poll_fn(|cx| {
        *fire.waker.lock().unwrap() = Some(cx.waker().clone());
        if fire.fired.swap(false, Ordering::SeqCst) {
            let (polled, trace) = Trace::capture(|| fut.as_mut().poll(cx));
            eprintln!("=== RDM_TASKDUMP {what} in test {test}: its own await chain\n{trace}\n=== RDM_TASKDUMP own chain end");
            if polled.is_ready() {
                return polled;
            }
        }
        fut.as_mut().poll(cx)
    }))
    .await;
    watchdog.abort();
    out
}
