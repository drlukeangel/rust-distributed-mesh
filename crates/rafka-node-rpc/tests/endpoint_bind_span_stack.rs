//! A bound endpoint leaves no span entered on any runtime worker.
//!
//! iroh's `Endpoint::builder().bind()` enters its `endpoint` span with a guard it holds across an
//! `.await`. On a multi-thread runtime the bind resumes on another worker than it started on, so
//! the exit is a no-op there and the span stays on the first worker's span stack with no
//! reference behind it. The next span that worker opens takes it as its contextual parent, and
//! once that span closes, `tracing-subscriber` panics "tried to clone a span that already closed".

use iroh::SecretKey;
use rafka_node_rpc::endpoint::bind;
use tracing_subscriber::layer::SubscriberExt;

const WORKERS: usize = 4;

/// CONTRACT: after a burst of concurrent binds on a multi-thread runtime, a task running on any
/// worker, outside every span, sees no current span. A current span there is an `endpoint` span
/// iroh entered on one worker and exited on another.
#[test]
fn concurrent_binds_leave_no_entered_span_on_any_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(tracing_subscriber::fmt::layer().with_writer(std::io::sink))).unwrap();
    // Every worker is checked from inside the runtime's own park hook: a worker parks only when it
    // holds no task, so the current span there must be none. Once all WORKERS are parked at the same
    // time, each one has parked after its last task: every worker is checked, whatever the load.
    let parked = Arc::new(AtomicUsize::new(0));
    // Each worker's span stack as of its latest park: a parked worker's latest park came after its
    // last task, so with every worker parked at once, this is each one's state after all its work.
    let checked: Arc<Mutex<std::collections::HashMap<std::thread::ThreadId, Option<&'static str>>>> = Arc::default();
    let (p, c) = (parked.clone(), checked.clone());
    let u = parked.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .on_thread_park(move || {
            c.lock().unwrap().insert(std::thread::current().id(), tracing::Span::current().metadata().map(|m| m.name()));
            p.fetch_add(1, Ordering::SeqCst);
        })
        .on_thread_unpark(move || {
            u.fetch_sub(1, Ordering::SeqCst);
        })
        .build()
        .unwrap();
    let endpoints = rt.block_on(async {
        let mut binds = tokio::task::JoinSet::new();
        for _ in 0..24 {
            binds.spawn(async { bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap() });
        }
        let mut endpoints = Vec::new();
        while let Some(ep) = binds.join_next().await {
            endpoints.push(ep.unwrap());
        }
        endpoints
    });
    // Wait (off the runtime) until every worker is parked at once: each then holds no task.
    while !(parked.load(Ordering::SeqCst) == WORKERS && checked.lock().unwrap().len() == WORKERS) {
        std::thread::yield_now();
    }
    let checked = std::mem::take(&mut *checked.lock().unwrap());
    assert_eq!(checked.len(), WORKERS, "every worker parked and was checked");
    let stale: Vec<_> = checked.iter().filter_map(|(t, s)| s.map(|s| (*t, s))).collect();
    assert!(stale.is_empty(), "a span is still entered on a worker with no task inside it: {stale:?}");
    rt.block_on(async {
        for ep in endpoints {
            ep.close().await;
        }
    });
}
