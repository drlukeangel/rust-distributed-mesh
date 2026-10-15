//! Where the fmt log lines go. The node's diagnostic log is a file (stderr redirected by the
//! launcher) on a disk the kernel may hold for seconds; a line written by the thread that logged it
//! stalls that thread, which is a runtime worker. A line is handed to a thread of its own over an
//! unbounded channel, so a held disk delays the file and nothing else; the lines are written in
//! the order they were handed over and [`LogSink::flush`] returns once every earlier one is in the sink.

use std::io::Write;
use std::sync::mpsc::{channel, Sender};

enum Msg {
    Line(Vec<u8>),
    Flush(Sender<()>),
}

/// The fmt layer's writer over `sink`.
#[derive(Clone)]
pub(crate) struct LogSink {
    tx: Sender<Msg>,
}

impl LogSink {
    pub(crate) fn start<W: Write + Send + 'static>(mut sink: W) -> Self {
        let (tx, rx) = channel::<Msg>();
        let spawned = std::thread::Builder::new().name("rdm-log".into()).spawn(move || {
            for msg in rx {
                match msg {
                    Msg::Line(bytes) => {
                        let _ = sink.write_all(&bytes);
                    }
                    Msg::Flush(ack) => {
                        let _ = sink.flush();
                        let _ = ack.send(());
                    }
                }
            }
        });
        if let Err(e) = spawned {
            eprintln!("telemetry: the log thread could not start: {e}; log lines are dropped");
        }
        Self { tx }
    }

    /// Returns once every line handed over before the call is in the sink.
    pub(crate) fn flush(&self) {
        let (ack, done) = channel();
        if self.tx.send(Msg::Flush(ack)).is_ok() {
            let _ = done.recv();
        }
    }
}

/// One event's bytes, handed over when the fmt layer drops it.
pub(crate) struct LogLine {
    tx: Sender<Msg>,
    buf: Vec<u8>,
}

impl Write for LogLine {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for LogLine {
    fn drop(&mut self) {
        if !self.buf.is_empty() {
            let _ = self.tx.send(Msg::Line(std::mem::take(&mut self.buf)));
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LogLine;
    fn make_writer(&'a self) -> LogLine {
        LogLine { tx: self.tx.clone(), buf: Vec::new() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
    use std::time::{Duration, Instant};

    /// A disk the kernel holds: every write takes `HOLD`.
    struct HeldDisk(Arc<AtomicUsize>);
    const HOLD: Duration = Duration::from_millis(250);
    impl Write for HeldDisk {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            std::thread::sleep(HOLD);
            self.0.fetch_add(buf.iter().filter(|b| **b == b'\n').count(), Ordering::SeqCst);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// CONTRACT: a log line written while the log's disk is held for 250 ms does not hold the
    /// runtime: the only worker keeps ticking a 10 ms timer within 100 ms of its schedule while
    /// five lines are logged from a task on that same worker, and after `flush` all five lines are
    /// in the sink.
    #[test]
    fn a_held_log_disk_never_stalls_the_runtime_worker() {
        let lines = Arc::new(AtomicUsize::new(0));
        let sink = LogSink::start(HeldDisk(lines.clone()));
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_time().build().unwrap();
        let worst = rt.block_on(async {
            let subscriber = tracing_subscriber::fmt().with_writer(sink.clone()).finish();
            let ticker = tokio::spawn(async {
                let mut worst = Duration::ZERO;
                for _ in 0..60 {
                    let t = Instant::now();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    worst = worst.max(t.elapsed().saturating_sub(Duration::from_millis(10)));
                }
                worst
            });
            let emitter = tokio::spawn(tracing::instrument::WithSubscriber::with_subscriber(async {
                for i in 0..5 {
                    tracing::info!(i, "a line");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }, subscriber));
            emitter.await.unwrap();
            ticker.await.unwrap()
        });
        sink.flush();
        assert_eq!(lines.load(Ordering::SeqCst), 5, "every line handed over reaches the sink");
        assert!(worst < Duration::from_millis(100), "the worker was held {worst:?} by a log write");
    }
}
