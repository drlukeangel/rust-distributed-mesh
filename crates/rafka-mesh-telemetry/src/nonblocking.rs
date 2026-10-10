//! A span processor whose `on_end` only hands the finished span to a thread of its own.
//!
//! Spans end inside whatever code drops them: a runtime worker, a transport driver's poll. The
//! stock `SimpleSpanProcessor` exports there, under one mutex shared by every thread that ends a
//! span, so one slow export (formatting, a lock, a write) holds every other thread's span end.
//! Here the ending thread performs one lock-free channel send and returns; a dedicated thread
//! drains the channel in order and runs the exporter.

use opentelemetry::trace::{TraceError, TraceResult};
use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
use opentelemetry_sdk::trace::{Span, SpanProcessor};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};

enum Msg {
    Span(SpanData),
    Flush(Sender<()>),
    Stop,
}

struct Inner {
    tx: Sender<Msg>,
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// Cloning shares the one writer thread; `shutdown` on any clone stops it after every span handed
/// over before it has been exported.
#[derive(Clone)]
pub struct NonBlockingSpanProcessor {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for NonBlockingSpanProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonBlockingSpanProcessor").finish_non_exhaustive()
    }
}

impl NonBlockingSpanProcessor {
    /// Start the export thread named `name`. Spans are exported in the order they ended, each
    /// batch being whatever has already queued, never waiting for more.
    pub fn start<E: SpanExporter + 'static>(name: &str, mut exporter: E) -> std::io::Result<Self> {
        let (tx, rx) = channel::<Msg>();
        let thread = std::thread::Builder::new().name(name.into()).spawn(move || {
            let mut stop = false;
            while !stop {
                let Ok(first) = rx.recv() else { break };
                let mut batch = Vec::new();
                let mut acks = Vec::new();
                let mut next = Some(first);
                while let Some(msg) = next.take() {
                    match msg {
                        Msg::Span(s) => batch.push(s),
                        Msg::Flush(ack) => acks.push(ack),
                        Msg::Stop => stop = true,
                    }
                    if !stop {
                        next = rx.try_recv().ok();
                    }
                }
                if !batch.is_empty() {
                    if let Err(e) = futures_executor_block_on(exporter.export(batch)) {
                        eprintln!("telemetry: span export failed: {e}");
                    }
                }
                for ack in acks {
                    let _ = ack.send(());
                }
            }
            exporter.shutdown();
        })?;
        Ok(Self { inner: Arc::new(Inner { tx, thread: Mutex::new(Some(thread)) }) })
    }
}

fn futures_executor_block_on<F: std::future::Future>(f: F) -> F::Output {
    // The export futures here are ready values; a parked-thread waker is all they need.
    struct ThreadWaker(std::thread::Thread);
    impl std::task::Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = std::task::Context::from_waker(&waker);
    let mut f = std::pin::pin!(f);
    loop {
        if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
        std::thread::park();
    }
}

impl SpanProcessor for NonBlockingSpanProcessor {
    fn on_start(&self, _span: &mut Span, _cx: &opentelemetry::Context) {}

    fn on_end(&self, span: SpanData) {
        if span.span_context.is_sampled() {
            let _ = self.inner.tx.send(Msg::Span(span));
        }
    }

    /// Returns once every span handed over before the call has been exported.
    fn force_flush(&self) -> TraceResult<()> {
        let (ack_tx, ack_rx) = channel();
        self.inner.tx.send(Msg::Flush(ack_tx)).map_err(|_| TraceError::Other("the span export thread is stopped".into()))?;
        ack_rx.recv().map_err(|_| TraceError::Other("the span export thread stopped before the flush".into()))
    }

    /// Exports every span handed over before the call, shuts the exporter down and joins the
    /// thread. Calling it again is a no-op.
    fn shutdown(&self) -> TraceResult<()> {
        let _ = self.inner.tx.send(Msg::Stop);
        let handle = self.inner.thread.lock().map_err(|_| TraceError::Other("span export thread handle poisoned".into()))?.take();
        if let Some(h) = handle {
            let _ = h.join();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{Tracer, TracerProvider as _};
    use opentelemetry_sdk::testing::trace::InMemorySpanExporter;

    /// CONTRACT: every span that ended before the flush is exported, in the order it ended.
    #[test]
    fn flush_exports_every_span_that_ended_before_it() {
        let exporter = InMemorySpanExporter::default();
        let processor = NonBlockingSpanProcessor::start("test-export", exporter.clone()).unwrap();
        let provider = opentelemetry_sdk::trace::TracerProvider::builder().with_span_processor(processor.clone()).build();
        let tracer = provider.tracer("t");
        for i in 0..500 {
            drop(tracer.start(format!("s{i}")));
        }
        processor.force_flush().unwrap();
        let names: Vec<String> = exporter.get_finished_spans().unwrap().into_iter().map(|s| s.name.to_string()).collect();
        assert_eq!(names, (0..500).map(|i| format!("s{i}")).collect::<Vec<_>>());
        provider.shutdown().unwrap();
    }
}
