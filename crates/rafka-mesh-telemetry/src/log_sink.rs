//! Where the fmt log lines go. The node's diagnostic log is a file (stderr redirected by the
//! launcher) on a disk the kernel may hold for seconds; a line written by the thread that logged it
//! stalls that thread, which is a runtime worker.

use std::io::Write;
use std::sync::Mutex;

/// The fmt layer's writer over `sink`.
#[derive(Clone)]
pub(crate) struct LogSink {
    sink: std::sync::Arc<Mutex<Box<dyn Write + Send>>>,
}

impl LogSink {
    pub(crate) fn start<W: Write + Send + 'static>(sink: W) -> Self {
        Self { sink: std::sync::Arc::new(Mutex::new(Box::new(sink))) }
    }

    /// Returns once every line handed over before the call is in the sink.
    pub(crate) fn flush(&self) {
        let _ = self.sink.lock().map(|mut s| s.flush());
    }
}

pub(crate) struct LogLine(LogSink);

impl Write for LogLine {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.sink.lock().map_err(|e| std::io::Error::other(e.to_string()))?.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.sink.lock().map_err(|e| std::io::Error::other(e.to_string()))?.flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LogLine;
    fn make_writer(&'a self) -> LogLine {
        LogLine(self.clone())
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
