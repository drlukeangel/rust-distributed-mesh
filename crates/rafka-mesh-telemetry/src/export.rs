//! Where telemetry export runs, and what an unreachable collector costs the process: nothing.
//!
//! Export runs on one dedicated OS thread with its own single-threaded runtime ([`ExportRuntime`]),
//! never on the application's runtime: a collector that is down, silent or unroutable parks only
//! that thread. The batch processors reach it through bounded queues that drop on overflow; the
//! exporter wrappers ([`WatchedSpans`], [`WatchedLogs`]) turn every failed batch into a count and
//! name each outage once, when it starts and when it ends. A caller that must wait for the
//! queue to drain (a guard dropping, a process about to exit) waits at most [`DRAIN_BOUND`].

use futures_util::future::BoxFuture;
use opentelemetry_sdk::export::logs::{LogBatch, LogExporter};
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
use opentelemetry_sdk::logs::{LogError, LogResult};
use opentelemetry_sdk::runtime::{Runtime, RuntimeChannel};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// The longest a caller waits for queued telemetry to leave: a guard's `Drop` and
/// [`crate::flush_before_exit`]. Past it the wait is abandoned and the process goes on; it is not
/// configurable (the SDK reads its own bound from `OTEL_BSP_EXPORT_TIMEOUT`, which is why the
/// bound is held here and not there).
pub const DRAIN_BOUND: Duration = Duration::from_secs(2);

/// The longest one export request to the collector may take before it counts as failed.
pub const EXPORT_TIMEOUT: Duration = Duration::from_secs(3);

/// Spans and log records an unreachable collector made the exporters drop, since process start.
static DROPPED_BY_OUTAGE: AtomicU64 = AtomicU64::new(0);

/// Spans and log records dropped because an export to the collector failed (an outage). Records
/// dropped because a queue was full are named by the SDK's own one-time warning.
pub fn dropped_by_outage() -> u64 {
    DROPPED_BY_OUTAGE.load(Ordering::Relaxed)
}

/// The dedicated export runtime: a current-thread tokio runtime parked on its own OS thread.
#[derive(Debug, Clone)]
pub struct ExportRuntime {
    handle: tokio::runtime::Handle,
}

impl ExportRuntime {
    /// The process's one export runtime, started on first use.
    pub fn get() -> &'static ExportRuntime {
        static RT: OnceLock<ExportRuntime> = OnceLock::new();
        RT.get_or_init(|| {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::Builder::new()
                .name("rdm-telemetry-export".into())
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("the telemetry export runtime");
                    let _ = tx.send(rt.handle().clone());
                    rt.block_on(std::future::pending::<()>());
                })
                .expect("the telemetry export thread");
            ExportRuntime { handle: rx.recv().expect("the telemetry export runtime handle") }
        })
    }

    /// Enter the export runtime's context: exporters built inside it create their connections
    /// (tonic's channel worker) on this runtime.
    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.handle.enter()
    }
}

impl Runtime for ExportRuntime {
    type Interval = tokio_stream::wrappers::IntervalStream;
    type Delay = std::pin::Pin<Box<tokio::time::Sleep>>;

    fn interval(&self, duration: Duration) -> Self::Interval {
        let _g = self.handle.enter();
        tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(duration))
    }

    fn spawn(&self, future: BoxFuture<'static, ()>) {
        drop(self.handle.spawn(future));
    }

    fn delay(&self, duration: Duration) -> Self::Delay {
        let _g = self.handle.enter();
        Box::pin(tokio::time::sleep(duration))
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
                    eprintln!("telemetry: the OTLP {} export to {} is failing, dropping until it recovers: {why}", self.signal, self.endpoint);
                }
            }
            None => {
                if self.down.swap(false, Ordering::Relaxed) {
                    let dropped = self.dropped.swap(0, Ordering::Relaxed);
                    eprintln!("telemetry: the OTLP {} export to {} recovered after dropping {dropped}", self.signal, self.endpoint);
                }
            }
        }
    }
}

/// A span exporter that reports an outage once per transition and counts what it drops.
#[derive(Debug)]
pub struct WatchedSpans<E> {
    inner: E,
    outage: Arc<Outage>,
}

impl<E> WatchedSpans<E> {
    pub fn new(inner: E, endpoint: &str) -> Self {
        Self { inner, outage: Outage::new("span", endpoint) }
    }
}

impl<E: SpanExporter> SpanExporter for WatchedSpans<E> {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        let count = batch.len();
        let sent = self.inner.export(batch);
        let outage = self.outage.clone();
        Box::pin(async move {
            let result = sent.await;
            outage.observe(count, result.as_ref().err().map(|e| e.to_string()));
            result
        })
    }

    fn shutdown(&mut self) {
        self.inner.shutdown();
    }

    fn force_flush(&mut self) -> BoxFuture<'static, ExportResult> {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

/// A log exporter that reports an outage once per transition and counts what it drops.
#[derive(Debug)]
pub struct WatchedLogs<E> {
    inner: E,
    outage: Arc<Outage>,
}

impl<E> WatchedLogs<E> {
    pub fn new(inner: E, endpoint: &str) -> Self {
        Self { inner, outage: Outage::new("log", endpoint) }
    }
}

#[async_trait::async_trait]
impl<E: LogExporter> LogExporter for WatchedLogs<E> {
    async fn export(&mut self, batch: LogBatch<'_>) -> LogResult<()> {
        let count = batch.iter().count();
        let result = self.inner.export(batch).await;
        self.outage.observe(count, result.as_ref().err().map(|e: &LogError| e.to_string()));
        result
    }

    fn shutdown(&mut self) {
        self.inner.shutdown();
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

/// Run each job on a thread of its own and wait at most [`DRAIN_BOUND`] for all of them. Past the
/// bound the threads are left behind (the process goes on, or exits and takes them) and the wait
/// is named on stderr.
pub fn bounded(what: &str, jobs: Vec<Box<dyn FnOnce() + Send>>) {
    let (done, finished) = std::sync::mpsc::channel::<()>();
    let mut started = 0;
    for job in jobs {
        let done = done.clone();
        let spawned = std::thread::Builder::new().name("rdm-telemetry-drain".into()).spawn(move || {
            job();
            let _ = done.send(());
        });
        match spawned {
            Ok(_) => started += 1,
            Err(e) => eprintln!("telemetry: {what} could not start a thread: {e}"),
        }
    }
    let deadline = std::time::Instant::now() + DRAIN_BOUND;
    for _ in 0..started {
        if finished.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())).is_err() {
            eprintln!("telemetry: {what} did not finish within {} ms; abandoned, {} spans and log records dropped by the outage so far", DRAIN_BOUND.as_millis(), dropped_by_outage());
            return;
        }
    }
}

/// Which `tracing` events the exporter's own machinery may put in a log, a span or the collector:
/// none of the SDK's, tonic's or the HTTP stack's, bar the SDK's one-time WARN that names a full
/// queue. An export failure reaches stderr as the single line [`WatchedSpans`]/[`WatchedLogs`]
/// print per outage transition, and never re-enters the pipeline that failed.
pub fn admits_source(meta: &tracing::Metadata<'_>) -> bool {
    let t = meta.target();
    let internal = t.starts_with("opentelemetry") || t.starts_with("tonic") || t.starts_with("h2") || t.starts_with("hyper") || t.starts_with("tower") || t.starts_with("reqwest");
    !(internal && !(t.starts_with("opentelemetry") && *meta.level() == tracing::Level::WARN))
}
