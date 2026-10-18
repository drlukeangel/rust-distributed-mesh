//! Blocking calls off the async runtime: a disk call (or any call that blocks) runs on tokio's
//! blocking pool, and the work that issued it can count what its calls took ([`IoTime`]).

/// Run `f`, a blocking disk call (a write that ends in an fsync, a read, a directory made or looked
/// at), on tokio's blocking pool. No
/// worker thread waits on the disk, and no lock a worker could block on is held across it: a
/// journal commit that takes seconds delays only the caller awaiting it. Inside [`io_time`] the call
/// adds what it took to that work's [`IoTime`]: the wait from its issue until a blocking thread began
/// it, and its run on that thread, including while the call is still queued or running if the caller
/// awaiting it is cancelled.
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
        Err(e) => panic!("a blocking disk call was cancelled: {e}"),
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
