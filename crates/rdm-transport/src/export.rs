//! Where telemetry export runs, and what an unreachable collector costs the process: nothing.
//!
//! Export runs on one dedicated OS thread with its own single-threaded runtime (`ExportRuntime`),
//! never on the application's runtime: a collector that is down, silent or unroutable blocks only
//! that thread. The batch processors reach it through bounded queues that drop on overflow; the
//! exporter wrappers (`WatchedSpans`, `WatchedLogs`) turn every failed batch into a count and
//! name each outage once, when it starts and when it ends. A caller that must wait for the
//! queue to empty (a guard dropping, a process about to exit) waits at most `EXIT_FLUSH_BOUND`.

use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogBatch, LogExporter};
use opentelemetry_sdk::runtime::{Runtime, RuntimeChannel};
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// The longest a caller waits for queued telemetry to leave: a guard's `Drop` and
/// [`crate::flush_before_exit`]. Past it the wait is abandoned and the process goes on; it is not
/// configurable (the SDK reads its own bound from `OTEL_BSP_EXPORT_TIMEOUT`, which is why the
/// bound is held here and not there).
pub(crate) const EXIT_FLUSH_BOUND: Duration = Duration::from_secs(2);

/// The longest one export request to the collector may take before it counts as failed.
pub(crate) const EXPORT_TIMEOUT: Duration = Duration::from_secs(3);

/// Spans and log records an unreachable collector made the exporters drop, since process start.
static DROPPED_BY_OUTAGE: AtomicU64 = AtomicU64::new(0);

/// How many signals' exports are failing now.
static SIGNALS_DOWN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Whether the last export of any signal failed and none has succeeded since: what is queued for
/// the collector is going to be dropped, so no exit waits for it.
pub(crate) fn collector_is_down() -> bool {
    SIGNALS_DOWN.load(Ordering::SeqCst) > 0
}

/// Spans and log records dropped because an export to the collector failed (an outage). Records
/// dropped because a queue was full are named by the SDK's own one-time warning.
pub(crate) fn dropped_by_outage() -> u64 {
    DROPPED_BY_OUTAGE.load(Ordering::Relaxed)
}

/// Set by the export thread as it begins running the export runtime.
static EXPORT_THREAD_RUNNING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The dedicated export runtime: a current-thread tokio runtime blocked on its own OS thread.
#[derive(Debug, Clone)]
pub(crate) struct ExportRuntime {
    handle: tokio::runtime::Handle,
}

impl ExportRuntime {
    /// The process's one export runtime, started on first use. It is built on the caller's thread
    /// and handed to its own thread to run: the caller never waits for that thread to start (i146
    /// Q33); what is spawned on it before then runs once it does.
    pub fn get() -> &'static ExportRuntime {
        static RT: OnceLock<ExportRuntime> = OnceLock::new();
        RT.get_or_init(|| {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("the telemetry export runtime");
            let handle = rt.handle().clone();
            std::thread::Builder::new()
                .name("rdm-telemetry-export".into())
                .spawn(move || {
                    EXPORT_THREAD_RUNNING.store(true, Ordering::SeqCst);
                    rt.block_on(std::future::pending::<()>())
                })
                .expect("the telemetry export thread");
            ExportRuntime { handle }
        })
    }

    /// Whether the export thread has begun running the export runtime: an exit flush that finds it
    /// not yet running names it, since every export waits on it.
    pub(crate) fn running() -> bool {
        EXPORT_THREAD_RUNNING.load(Ordering::SeqCst)
    }

    /// Enter the export runtime's context: exporters built inside it run their requests and the
    /// HTTP client's connection tasks on this runtime.
    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.handle.enter()
    }
}

impl Runtime for ExportRuntime {
    fn spawn<F>(&self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        drop(self.handle.spawn(future));
    }

    fn delay(&self, duration: Duration) -> impl std::future::Future<Output = ()> + Send + 'static {
        let _g = self.handle.enter();
        tokio::time::sleep(duration)
    }
}

impl RuntimeChannel for ExportRuntime {
    type Receiver<T: std::fmt::Debug + Send> = tokio_stream::wrappers::ReceiverStream<T>;
    type Sender<T: std::fmt::Debug + Send> = tokio::sync::mpsc::Sender<T>;

    fn batch_message_channel<T: std::fmt::Debug + Send>(&self, capacity: usize) -> (Self::Sender<T>, Self::Receiver<T>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
        (sender, tokio_stream::wrappers::ReceiverStream::new(receiver))
    }
}

/// One signal's view of its collector: up or down, and the lines that name the change.
#[derive(Debug)]
struct Outage {
    signal: &'static str,
    endpoint: String,
    down: AtomicBool,
    dropped: AtomicU64,
}

impl Outage {
    fn new(signal: &'static str, endpoint: &str) -> Arc<Self> {
        Arc::new(Self { signal, endpoint: endpoint.to_string(), down: AtomicBool::new(false), dropped: AtomicU64::new(0) })
    }

    fn observe(&self, count: usize, failure: Option<String>) {
        match failure {
            Some(why) => {
                DROPPED_BY_OUTAGE.fetch_add(count as u64, Ordering::Relaxed);
                self.dropped.fetch_add(count as u64, Ordering::Relaxed);
                if !self.down.swap(true, Ordering::Relaxed) {
                    SIGNALS_DOWN.fetch_add(1, Ordering::SeqCst);
                    eprintln!("telemetry: the OTLP {} export to {} is failing, dropping until it recovers: {why}", self.signal, self.endpoint);
                }
            }
            None => {
                if self.down.swap(false, Ordering::Relaxed) {
                    SIGNALS_DOWN.fetch_sub(1, Ordering::SeqCst);
                    let dropped = self.dropped.swap(0, Ordering::Relaxed);
                    eprintln!("telemetry: the OTLP {} export to {} recovered after dropping {dropped}", self.signal, self.endpoint);
                }
            }
        }
    }
}

/// A span exporter that reports an outage once per transition and counts what it drops.
#[derive(Debug)]
pub(crate) struct WatchedSpans<E> {
    inner: E,
    outage: Arc<Outage>,
}

impl<E> WatchedSpans<E> {
    pub fn new(inner: E, endpoint: &str) -> Self {
        Self { inner, outage: Outage::new("span", endpoint) }
    }
}

impl<E: SpanExporter> SpanExporter for WatchedSpans<E> {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let count = batch.len();
        let result = self.inner.export(batch).await;
        self.outage.observe(count, result.as_ref().err().map(|e| e.to_string()));
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

/// A log exporter that reports an outage once per transition and counts what it drops.
#[derive(Debug)]
pub(crate) struct WatchedLogs<E> {
    inner: E,
    outage: Arc<Outage>,
}

impl<E> WatchedLogs<E> {
    pub fn new(inner: E, endpoint: &str) -> Self {
        Self { inner, outage: Outage::new("log", endpoint) }
    }
}

impl<E: LogExporter> LogExporter for WatchedLogs<E> {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let count = batch.iter().count();
        let result = self.inner.export(batch).await;
        self.outage.observe(count, result.as_ref().err().map(|e| e.to_string()));
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

/// Run each job on a thread of its own and wait at most `EXIT_FLUSH_BOUND` for all of them; when the
/// collector is known to be down they are not waited for at all. Past the bound the threads are
/// left behind (the process goes on, or exits and takes them) and the wait is named on stderr.
pub fn bounded(what: &str, jobs: Vec<Box<dyn FnOnce() + Send>>) {
    if !bounded_until(what, jobs, true, std::time::Instant::now() + EXIT_FLUSH_BOUND) {
        name_abandoned(what);
    }
}

/// `what`, a wait on the collector, was abandoned at [`EXIT_FLUSH_BOUND`]: named on stderr with what the
/// collector's outage dropped so far.
pub(crate) fn name_abandoned(what: &str) {
    name_on_stderr(format!("telemetry: {what} did not finish within {} ms; abandoned, {} spans and log records dropped by the outage so far", EXIT_FLUSH_BOUND.as_millis(), dropped_by_outage()));
}

/// The longest the exit waits for one line it names a bound or a loss with on stderr.
const NAMED_ON_STDERR_WITHIN: Duration = Duration::from_millis(250);

/// Set once a line of the exit was not written within [`NAMED_ON_STDERR_WITHIN`]: stderr is held, and
/// no later line of the exit waits on it.
static STDERR_HELD: AtomicBool = AtomicBool::new(false);

/// Name `line` on stderr from the exit flush, never letting stderr hold the exit. A launcher may hold
/// stderr (a full pipe it does not read, a held disk): the fmt log's thread ([`crate::log_sink`]) is
/// then blocked inside a stderr write, holding std's stderr lock, and an `eprintln!` on the exiting
/// thread waits on that lock for good. The line is written on a thread of its own and waited for at
/// most [`NAMED_ON_STDERR_WITHIN`]; past it the line is left with its thread (the exit takes both),
/// and every later line of the exit is too.
pub(crate) fn name_on_stderr(line: String) {
    if STDERR_HELD.load(Ordering::SeqCst) {
        return;
    }
    let (done, written) = std::sync::mpsc::channel::<()>();
    let spawned = std::thread::Builder::new().name("rdm-exit-flush".into()).spawn(move || {
        eprintln!("{line}");
        let _ = done.send(());
    });
    if spawned.is_ok() && written.recv_timeout(NAMED_ON_STDERR_WITHIN).is_err() {
        STDERR_HELD.store(true, Ordering::SeqCst);
    }
}

/// [`bounded`] up to `deadline`, one deadline for every wait of an exit flush, so the flush ends within
/// one [`EXIT_FLUSH_BOUND`]. A job that never waits on the collector (the evidence file's flush) passes
/// `skip_when_collector_down = false` and is waited for whatever the collector's state (i146 Q42).
/// Whether every job finished by `deadline` (a wait skipped because the collector is down counts as
/// finished: it was never owed); the caller names a wait that did not.
pub(crate) fn bounded_until(what: &str, jobs: Vec<Box<dyn FnOnce() + Send>>, skip_when_collector_down: bool, deadline: std::time::Instant) -> bool {
    let (done, finished) = std::sync::mpsc::channel::<()>();
    let mut started = 0;
    for job in jobs {
        let done = done.clone();
        let spawned = std::thread::Builder::new().name("rdm-exit-flush".into()).spawn(move || {
            job();
            let _ = done.send(());
        });
        match spawned {
            Ok(_) => started += 1,
            Err(e) => eprintln!("telemetry: {what} could not start a thread: {e}"),
        }
    }
    if skip_when_collector_down && collector_is_down() {
        return true;
    }
    for _ in 0..started {
        if finished.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())).is_err() {
            return false;
        }
    }
    true
}

/// Which `tracing` events the exporter's own machinery may put in a log, a span or the collector:
/// none of the SDK's or the HTTP stack's (hyper, h2, tower), bar the SDK's one-time WARN that names a full
/// queue. An export failure reaches stderr as the single line [`WatchedSpans`]/[`WatchedLogs`]
/// print per outage transition, and never re-enters the pipeline that failed.
pub(crate) fn admits_source(meta: &tracing::Metadata<'_>) -> bool {
    let t = meta.target();
    let internal = t.starts_with("opentelemetry") || t.starts_with("h2") || t.starts_with("hyper") || t.starts_with("tower") || t.starts_with("reqwest");
    !(internal && !(t.starts_with("opentelemetry") && *meta.level() == tracing::Level::WARN))
}
