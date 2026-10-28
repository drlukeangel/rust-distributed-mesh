//! Where the fmt log lines go. The node's diagnostic log is a file (stderr redirected by the
//! launcher) on a disk the kernel may hold for seconds; a line written by the thread that logged it
//! stalls that thread, which is a runtime worker. A line is handed to a thread of its own over an
//! unbounded channel, so a held disk delays the file and nothing else; the lines are written in
//! the order they were handed over and [`LogSink::flush_until`] confirms them within the exit's deadline.

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
    pub(crate) fn start<W: Write + Send + 'static>(sink: W) -> Self {
        Self::start_opening(move || Some(sink))
    }

    /// [`LogSink::start`] over the writer `open` makes on the sink's own thread, so the caller never
    /// waits on a disk to open it (i146 Q33). `None` (its reason named by `open`): lines are dropped
    /// and every flush still returns.
    pub(crate) fn start_opening<W: Write + Send + 'static>(open: impl FnOnce() -> Option<W> + Send + 'static) -> Self {
        let (tx, rx) = channel::<Msg>();
        let spawned = std::thread::Builder::new().name("rdm-log".into()).spawn(move || {
            let Some(mut sink) = open() else {
                for msg in rx {
                    if let Msg::Flush(ack) = msg {
                        let _ = ack.send(());
                    }
                }
                return;
            };
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

    /// Flush the lines already handed over, within the exit flush's remaining
    /// deadline. A held writer leaves them unconfirmed instead of holding the exit.
    pub(crate) fn flush_until(&self, deadline: std::time::Instant) -> bool {
        let (ack, done) = channel();
        self.tx.send(Msg::Flush(ack)).is_ok()
            && done.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())).is_ok()
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
    use std::sync::{atomic::{AtomicUsize, Ordering}, Arc, Condvar, Mutex};
    use std::time::Instant;

    /// A disk the kernel holds: a write on the log's own thread waits until the test releases the
    /// disk. Every write records the thread that made it; a write from any other thread is not held,
    /// so a line written on the runtime's worker shows up as written, by that worker, at once.
    struct HeldDisk {
        lines: Arc<AtomicUsize>,
        writers: Arc<Mutex<Vec<String>>>,
        released: Arc<(Mutex<bool>, Condvar)>,
    }
    impl Write for HeldDisk {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let writer = std::thread::current().name().unwrap_or_default().to_string();
            if writer == "rdm-log" {
                let (held, wake) = &*self.released;
                let _released = wake.wait_while(held.lock().unwrap(), |released| !*released).unwrap();
            }
            self.writers.lock().unwrap().push(writer);
            self.lines.fetch_add(buf.iter().filter(|b| **b == b'\n').count(), Ordering::SeqCst);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// CONTRACT: a log line written while the log's disk is held does not hold the runtime: five
    /// lines logged from a task on the runtime's only worker all return while the disk still holds
    /// the first one (no line has reached the disk), and once the disk is released, `flush` confirms
    /// all five, every one written by the log's own thread.
    #[test]
    fn a_held_log_disk_never_stalls_the_runtime_worker() {
        let (lines, writers) = (Arc::new(AtomicUsize::new(0)), Arc::new(Mutex::new(Vec::new())));
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        let sink = LogSink::start(HeldDisk { lines: lines.clone(), writers: writers.clone(), released: released.clone() });
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(1).build().unwrap();
        rt.block_on(async {
            crate::enable_callsites();
            let subscriber = tracing_subscriber::fmt().with_writer(sink.clone()).finish();
            tokio::spawn(tracing::instrument::WithSubscriber::with_subscriber(async {
                for i in 0..5 {
                    tracing::info!(i, "a line");
                }
            }, subscriber))
            .await
            .unwrap();
        });
        assert_eq!(lines.load(Ordering::SeqCst), 0, "the five calls returned with the disk still held; lines written on the way: {:?}", writers.lock().unwrap());
        *released.0.lock().unwrap() = true;
        released.1.notify_all();
        assert!(sink.flush_until(Instant::now() + crate::export::EXIT_FLUSH_BOUND), "the log flush completes within the process bound");
        assert_eq!(lines.load(Ordering::SeqCst), 5, "every line handed over reaches the sink");
        assert!(writers.lock().unwrap().iter().all(|w| w == "rdm-log"), "every line is written by the log's own thread: {:?}", writers.lock().unwrap());
    }
}
