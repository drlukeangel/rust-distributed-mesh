//! A bound endpoint leaves no span entered on any runtime worker.
//!
//! iroh's `Endpoint::builder().bind()` enters its `endpoint` span with a guard it holds across an
//! `.await`. On a multi-thread runtime the bind resumes on another worker than it started on, so
//! the exit is a no-op there and the span stays on the first worker's span stack with no
//! reference behind it. The next span that worker opens takes it as its contextual parent, and
//! once that span closes, `tracing-subscriber` panics "tried to clone a span that already closed".

use iroh::SecretKey;
use rafka_node_rpc::endpoint::bind;
use std::collections::HashSet;
use tracing_subscriber::layer::SubscriberExt;

const WORKERS: usize = 4;

/// CONTRACT: after a burst of concurrent binds on a multi-thread runtime, a task running on any
/// worker, outside every span, sees no current span. A current span there is an `endpoint` span
/// iroh entered on one worker and exited on another.
#[test]
fn concurrent_binds_leave_no_entered_span_on_any_worker() {
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(tracing_subscriber::fmt::layer().with_writer(std::io::sink))).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(WORKERS).enable_all().build().unwrap();
    rt.block_on(async {
        let mut binds = tokio::task::JoinSet::new();
        for _ in 0..24 {
            binds.spawn(async { bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap() });
        }
        let mut endpoints = Vec::new();
        while let Some(ep) = binds.join_next().await {
            endpoints.push(ep.unwrap());
        }
        // Visit every worker: a task per worker is parked until all have been seen.
        let mut seen = HashSet::new();
        let mut stale = Vec::new();
        for _ in 0..200 {
            let mut probes = tokio::task::JoinSet::new();
            for _ in 0..32 {
                probes.spawn(async {
                    tokio::task::yield_now().await;
                    (std::thread::current().id(), tracing::Span::current().metadata().map(|m| m.name()))
                });
            }
            while let Some(r) = probes.join_next().await {
                let (thread, current) = r.unwrap();
                seen.insert(thread);
                if let Some(name) = current {
                    stale.push((thread, name));
                }
            }
            if seen.len() >= WORKERS {
                break;
            }
        }
        assert!(seen.len() >= WORKERS, "probed only {} of {WORKERS} workers", seen.len());
        assert!(stale.is_empty(), "a span is still entered on a worker with no task inside it: {stale:?}");
        for ep in endpoints {
            ep.close().await;
        }
    });
}
