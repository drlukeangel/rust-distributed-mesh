//! Blocking calls off the async runtime: a disk call (or any call that blocks) runs on tokio's
//! blocking pool, and the work that issued it can count what its calls took ([`IoTime`]).

/// Run `f`, a blocking disk call (a write that ends in an fsync, a read, a directory made or looked
/// at), on tokio's blocking pool. No
/// worker thread waits on the disk, and no lock a worker could block on is held across it: a
/// journal commit that takes seconds delays only the caller awaiting it. Inside [`io_time_with`] the call
/// adds what it took to that work's [`IoTime`]: the wait from its issue until a blocking thread began
/// it, and its run on that thread, including while the call is still queued or running if the caller
/// awaiting it is cancelled.
///
/// tokio cancels a blocking task only when its runtime shuts down, before the task begins, and
/// nothing here aborts one: a cancelled call is a caller being dropped with its runtime. It is named
/// (`rdm.node.disk_call.update.via-shutdown`, with how long ago it was issued) and never answers,
/// so the runtime drops its caller with everything else; no caller ever sees a disk call that never
/// ran.
pub async fn blocking_io<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    let slot = std::sync::Arc::new(std::sync::Mutex::new(CallTiming {
        issued: std::time::Instant::now(),
        began: None,
        finished: None,
    }));
    let _ = IO_TIME.try_with(|t| t.register(slot.clone()));
    let slot_run = slot.clone();
    let run = tokio::task::spawn_blocking(move || {
        slot_run.lock().unwrap().began = Some(std::time::Instant::now());
        let v = f();
        slot_run.lock().unwrap().finished = Some(std::time::Instant::now());
        v
    });
    match run.await {
        Ok(v) => v,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(_) => {
            let (issue_age_ms, began) = {
                let s = slot.lock().unwrap();
                (s.issued.elapsed().as_millis() as u64, s.began.is_some())
            };
            tracing::info_span!("rdm.node.disk_call.update.via-shutdown", issue_age_ms, began)
                .in_scope(|| tracing::info!("a blocking disk call was cancelled by its runtime's shutdown: it never answers, and its caller is dropped with the runtime"));
            std::future::pending::<T>().await
        }
    }
}

tokio::task_local! {
    /// The blocking time of the work [`io_time`] runs.
    static IO_TIME: IoTime;
}

#[derive(Debug)]
struct CallTiming {
    issued: std::time::Instant,
    began: Option<std::time::Instant>,
    finished: Option<std::time::Instant>,
}

/// What the [`blocking_io`] calls of one piece of work took: how many there were, how long they
/// waited for a blocking thread, and how long they ran on one. Shared with [`io_time_with`] so a
/// step cancelled while a blocking call is still queued or running still records its count and time.
#[derive(Debug, Default, Clone)]
pub struct IoTime {
    calls: std::sync::Arc<std::sync::Mutex<Vec<std::sync::Arc<std::sync::Mutex<CallTiming>>>>>,
}

impl IoTime {
    fn register(&self, slot: std::sync::Arc<std::sync::Mutex<CallTiming>>) {
        self.calls.lock().unwrap().push(slot);
    }

    fn totals(&self) -> (u64, std::time::Duration, std::time::Duration) {
        let now = std::time::Instant::now();
        let list = self.calls.lock().unwrap();
        let mut queued = std::time::Duration::ZERO;
        let mut ran = std::time::Duration::ZERO;
        for slot in list.iter() {
            let s = slot.lock().unwrap();
            match (s.began, s.finished) {
                (None, _) => queued += now.saturating_duration_since(s.issued),
                (Some(b), None) => {
                    queued += b.saturating_duration_since(s.issued);
                    ran += now.saturating_duration_since(b);
                }
                (Some(b), Some(f)) => {
                    queued += b.saturating_duration_since(s.issued);
                    ran += f.saturating_duration_since(b);
                }
            }
        }
        (list.len() as u64, queued, ran)
    }

    /// How many blocking calls the work made (completed or in flight).
    pub fn calls(&self) -> u64 {
        self.calls.lock().unwrap().len() as u64
    }

    /// How long they waited, from issue, for a blocking thread to begin them, in milliseconds.
    pub fn queued_ms(&self) -> u64 {
        self.totals().1.as_millis() as u64
    }

    /// How long they ran on a blocking thread, in milliseconds.
    pub fn ran_ms(&self) -> u64 {
        self.totals().2.as_millis() as u64
    }
}

/// Run `work`, recording its [`blocking_io`] calls into `io`, awaited on this task.
pub async fn io_time_with<F: std::future::Future>(io: IoTime, work: F) -> F::Output {
    IO_TIME.scope(io, work).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake};

    /// The spans opened on this thread while installed: each one's name and its `u64` fields.
    #[derive(Default)]
    struct Opened(Mutex<Vec<(String, Vec<(String, u64)>)>>);

    struct Fields(Vec<(String, u64)>);
    impl tracing::field::Visit for Fields {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0.push((field.name().to_string(), value));
        }
        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
    }

    struct Recorder(Arc<Opened>);
    impl tracing::Subscriber for Recorder {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let mut fields = Fields(Vec::new());
            span.record(&mut fields);
            let mut opened = self.0 .0.lock().unwrap();
            opened.push((span.metadata().name().to_string(), fields.0));
            tracing::span::Id::from_u64(opened.len() as u64)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }

    /// CONTRACT (tokio cancels a blocking task only when its runtime shuts down, before the task
    /// begins; the work awaiting it is being dropped with that runtime): a disk call issued while
    /// the runtime is shutting down, its one blocking thread still held, is cancelled, never run.
    /// Its caller does not panic: the cancellation is named (with how long ago the call was issued)
    /// and the call never answers, so the runtime drops its caller with everything else. Seen as
    /// `blocking_io` panicking "a blocking disk call was cancelled" at an admin's stop (i144,
    /// record_store.rs).
    #[test]
    fn a_disk_call_cancelled_by_its_runtimes_shutdown_is_named_and_never_answers() {
        let rt = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap();
        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        drop(rt.spawn_blocking(move || {
            let _ = held_tx.send(());
            let _ = release_rx.recv();
        }));
        held_rx.recv().unwrap();

        // The cancellation's callsite takes its interest from the recorder too, never only from the
        // thread that hit it first (`rdm_transport::enable_callsites`).
        rdm_transport::enable_callsites();
        let opened = Arc::new(Opened::default());
        let waker = Arc::new(Noop).into();
        let mut cx = Context::from_waker(&waker);
        // The runtime shuts down with its one blocking thread held; a call issued in its context now
        // is cancelled before it begins.
        let handle = rt.handle().clone();
        rt.shutdown_timeout(std::time::Duration::from_millis(50));
        let mut call = Box::pin(blocking_io(|| 7u32));

        let named = |o: &Opened| o.0.lock().unwrap().iter().find(|(n, _)| n == "rdm.node.disk_call.update.via-shutdown").cloned();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while named(&opened).is_none() && std::time::Instant::now() < until {
            let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tracing::subscriber::with_default(Recorder(opened.clone()), || {
                    let _in = handle.enter();
                    call.as_mut().poll(&mut cx)
                })
            }));
            assert!(matches!(polled, Ok(Poll::Pending)), "the cancelled call's caller is never answered and never panics: {:?}", polled.map(|p| p.is_ready()));
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let (_, fields) = named(&opened).expect("the cancellation is named");
        assert!(fields.iter().any(|(k, _)| k == "issue_age_ms"), "it names how long ago the call was issued: {fields:?}");
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call.as_mut().poll(&mut cx)));
        assert!(matches!(polled, Ok(Poll::Pending)), "polled again, it still never answers");
        release_tx.send(()).unwrap();
    }
}
