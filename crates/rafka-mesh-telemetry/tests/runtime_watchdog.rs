//! The runtime watchdog proves a window in which the async runtime ran nothing.

use std::io::Write;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);
impl Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// CONTRACT: a runtime blocked for 800 ms (every worker held in a blocking call) is reported once
/// it runs again, with a stall of at least the watchdog's threshold and the threads' states
/// sampled while it was blocked; a runtime that runs is never reported.
#[test]
fn a_blocked_runtime_is_reported_with_its_length_and_its_threads() {
    let buf = Buf::default();
    let w = buf.clone();
    tracing::subscriber::set_global_default(tracing_subscriber::fmt().with_writer(move || w.clone()).with_ansi(false).finish()).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    rt.block_on(async {
        rafka_mesh_telemetry::watchdog::spawn().expect("inside a runtime");
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let quiet = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(!quiet.contains("runtime-stall"), "a running runtime is never reported: {quiet}");
        std::thread::sleep(std::time::Duration::from_millis(800));
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    });
    let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let line = out.lines().find(|l| l.contains("runtime-stall")).unwrap_or_else(|| panic!("no stall reported: {out}"));
    let stall: u64 = line.split("stall_ms=").nth(1).and_then(|r| r.split_whitespace().next()).and_then(|v| v.parse().ok()).expect("stall_ms");
    assert!(stall >= 500, "{line}");
    assert!(line.contains("threads="), "{line}");
}
