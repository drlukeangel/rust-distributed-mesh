//! Ending a span never blocks the thread that ended it, whatever the sink behind it is doing.

use opentelemetry::trace::{Tracer, TracerProvider as _};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use std::sync::mpsc;
use std::time::Duration;

/// An exporter that holds every export until the test lets it go.
#[derive(Debug)]
struct Stalled {
    entered: mpsc::Sender<()>,
    release: std::sync::Arc<std::sync::Mutex<mpsc::Receiver<()>>>,
}

impl SpanExporter for Stalled {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        let _ = self.entered.send(());
        let _ = self.release.lock().unwrap().recv();
        Ok(())
    }
}

/// CONTRACT: with the sink behind the evidence processor stalled inside an export, a thread that
/// ends a span returns at once, and again for a second span while the first export is still held.
#[test]
fn span_end_returns_while_the_sink_is_stalled() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let exporter = Stalled { entered: entered_tx, release: std::sync::Arc::new(std::sync::Mutex::new(release_rx)) };
    let provider = SdkTracerProvider::builder().with_span_processor(rdm_transport::span_processor("stalled", exporter)).build();
    let tracer = provider.tracer("span-end-nonblocking");
    let (done_tx, done_rx) = mpsc::channel();
    let t = tracer.clone();
    let first = std::thread::spawn(move || {
        t.start("rdm.test.span.end.first");
        let _ = done_tx.send("first");
    });
    // The first span's export is now held inside the sink .
    let _ = entered_rx.recv_timeout(Duration::from_secs(5));
    let (done2_tx, done2_rx) = mpsc::channel();
    let t = tracer.clone();
    let second = std::thread::spawn(move || {
        t.start("rdm.test.span.end.second");
        let _ = done2_tx.send("second");
    });
    let first_returned = done_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    let second_returned = done2_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    // Let every held export go before asserting, so a failure does not hang the process.
    for _ in 0..4 {
        let _ = release_tx.send(());
    }
    let _ = first.join();
    let _ = second.join();
    assert!(first_returned, "the thread that ended the first span was held by the sink's export");
    assert!(second_returned, "the thread that ended the second span waited on the first span's export");
    drop(release_tx);
    let _ = provider.shutdown();
}

/// CONTRACT: when the queue in front of a stalled sink overflows, a flush answers with an error
/// (the SDK cannot queue the flush request) rather than `Ok`, and the processor's counters show
/// every span handed over.
#[test]
fn a_flush_on_an_overflowed_queue_is_an_error_never_ok() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let exporter = Stalled { entered: entered_tx, release: std::sync::Arc::new(std::sync::Mutex::new(release_rx)) };
    let processor = rdm_transport::span_processor("overflow", exporter);
    let loss = processor.loss();
    let provider = SdkTracerProvider::builder().with_span_processor(processor).build();
    let tracer = provider.tracer("overflow");
    drop(tracer.start("first"));
    entered_rx.recv_timeout(Duration::from_secs(10)).expect("the first export never started");
    for _ in 0..70_000 {
        drop(tracer.start("fill"));
    }
    let flushed = provider.force_flush();
    for _ in 0..64 {
        let _ = release_tx.send(());
    }
    assert_eq!(loss.handed(), 70_001);
    assert!(flushed.is_err(), "a flush on a full queue answered Ok: {flushed:?}");
    drop(release_tx);
}
