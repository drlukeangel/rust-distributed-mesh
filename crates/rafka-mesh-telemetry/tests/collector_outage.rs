//! A collector that is down, silent or unroutable changes nothing the process does.
//!
//! The child half of each cell is this same test binary re-run with `RDM_OUTAGE_CHILD` set: it
//! initialises telemetry against the broken collector, emits spans and log lines from a
//! multi-threaded runtime, drains and exits the way a node does. The parent half asserts what the
//! outside sees: the child ends within its bound, its emitting never waited, and its stderr names
//! the outage once, not once per span.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CHILD: &str = "RDM_OUTAGE_CHILD";
/// A child that has emitted its spans ends within this: the drain's bound plus start-up.
const EXIT_BOUND: Duration = Duration::from_secs(5);
const SPANS: usize = 2000;

/// The child half: runs only when the parent set `RDM_OUTAGE_CHILD`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outage_child() {
    if std::env::var(CHILD).is_err() {
        return;
    }
    let evidence = std::env::temp_dir().join(format!("rdm-outage-evidence-{}", std::process::id()));
    std::env::set_var("RDM_EVIDENCE_DIR", &evidence);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rdm-outage-child");
    let started = Instant::now();
    for i in 0..SPANS {
        let span = tracing::info_span!("rdm.test.outage.update.via-child", i);
        span.in_scope(|| tracing::info!(i, "inside the span"));
    }
    // The emitting is all this prints for the parent: it never waited on the collector.
    println!("EMIT_MS={}", started.elapsed().as_millis());
    // A second of outage with spans queued, then the exit a node makes.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    rafka_mesh_telemetry::flush_before_exit();
    drop(telemetry);
    println!("DRAINED_MS={}", started.elapsed().as_millis());
    let _ = std::fs::remove_dir_all(&evidence);
    std::process::exit(0);
}

/// Run the child against `endpoint`; its exit time, stdout and stderr.
fn run_child(endpoint: &str) -> (Duration, String, String) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        // The child runs this test alone: its libtest name carries the module path of this file.
        .args(["--exact", &format!("{}::outage_child", module_path!().split_once("::").expect("a module of the test crate").1), "--nocapture"])
        .env(CHILD, "1")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
        .env_remove("RUST_LOG")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (mut out, mut err) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    let out_reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = out.read_to_string(&mut s);
        s
    });
    let err_reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = err.read_to_string(&mut s);
        s
    });
    let started = Instant::now();
    let give_up = started + Duration::from_secs(40);
    let mut killed = false;
    let status = loop {
        if let Some(st) = child.try_wait().unwrap() {
            break st;
        }
        if Instant::now() > give_up {
            child.kill().unwrap();
            killed = true;
            break child.wait().unwrap();
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let took = started.elapsed();
    let (out, err) = (out_reader.join().unwrap(), err_reader.join().unwrap());
    assert!(!killed, "the child was still running after {took:?}; it never exited: stdout={out} stderr tail={}", &err[err.len().saturating_sub(600)..]);
    assert!(status.success(), "the child failed: {status:?}\n{out}\n{err}");
    (took, out, err)
}

fn number(out: &str, key: &str) -> u128 {
    out.lines().find_map(|l| l.strip_prefix(key)?.strip_prefix('=')?.trim().parse().ok()).unwrap_or_else(|| panic!("{key} missing from the child's output: {out}"))
}

/// CONTRACT: against a collector that refuses, never answers, or is unroutable, a process that
/// emits spans and log lines ends within its drain bound, emitting 2000 spans never waits on the
/// collector, and stderr names the outage once per signal, never once per span or batch.
fn outage_changes_nothing(endpoint: &str) {
    let (took, out, err) = run_child(endpoint);
    assert!(took < EXIT_BOUND + Duration::from_secs(3), "the child took {took:?} to end against {endpoint} (stdout: {out})");
    let drained = number(&out, "DRAINED_MS");
    assert!(drained < 1200 + EXIT_BOUND.as_millis(), "the drain took until {drained} ms against {endpoint}");
    let emit = number(&out, "EMIT_MS");
    assert!(emit < 2000, "emitting {SPANS} spans took {emit} ms against {endpoint}: export waited on the collector");
    let failing = err.lines().filter(|l| l.contains("export to") && l.contains("is failing")).count();
    assert!(failing <= 2, "the outage was named {failing} times (one per signal at most):\n{err}");
    let sdk_lines = err.lines().filter(|l| l.contains("ExportError") || l.contains("BatchSpanProcessor") || l.contains("BatchLogProcessor")).count();
    assert_eq!(sdk_lines, 0, "the SDK's own export-error lines reached stderr:\n{err}");
}

#[test]
fn a_refusing_collector_changes_nothing() {
    outage_changes_nothing("http://127.0.0.1:9");
}

#[test]
fn a_collector_that_accepts_and_never_answers_changes_nothing() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    listener.set_nonblocking(true).unwrap();
    let held = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (s, h) = (stop.clone(), held.clone());
    let accepter = std::thread::spawn(move || {
        while !s.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok((c, _)) = listener.accept() {
                h.lock().unwrap().push(c);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    outage_changes_nothing(&endpoint);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    accepter.join().unwrap();
}

#[test]
fn an_unroutable_collector_changes_nothing() {
    outage_changes_nothing("http://10.255.255.1:4317");
}
