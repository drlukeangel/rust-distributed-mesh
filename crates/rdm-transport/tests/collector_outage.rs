//! A collector that is down, silent or unroutable changes nothing the process does.
//!
//! The child half of each cell is this same test binary re-run with `RDM_OUTAGE_CHILD` set: it
//! initialises telemetry against the broken collector, emits spans and log lines from a
//! multi-threaded runtime, flushes and exits the way a node does. The parent half starts the child
//! with the collector endpoint and a private evidence directory in its environment, and asserts
//! what the outside sees: the child ends within its bound, its emitting never waited, and its
//! stderr names the outage once, not once per span.

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
    // Where the child's wall time goes, for the parent's message: the test entered, the
    // telemetry was initialised, the exit began.
    println!("ENTERED_AT_MS={}", unix_ms());
    // What the host's disk did around this child: its IO pressure at the start and the exit, and the
    // child's own block-IO delay (i146 Q33, a stall named `evidence-file` is a disk wait only if these
    // cover it).
    println!("IO_PRESSURE_START={}", io_pressure());
    println!("DELAYACCT={}", std::fs::read_to_string("/proc/sys/kernel/task_delayacct").map(|s| s.trim().to_string()).unwrap_or_else(|e| format!("unreadable: {e}")));
    println!("BLKIO_START={}", blkio_ticks());
    let init = Instant::now();
    let telemetry = rdm_transport::init_evidence_telemetry("rdm-outage-child");
    println!("INIT_MS={}", init.elapsed().as_millis());
    println!("INIT_STEPS={}", rdm_transport::init_steps().map(|s| s.to_string()).unwrap_or_default());
    println!("BLKIO_AFTER_INIT={}", blkio_ticks());
    let started = Instant::now();
    for i in 0..SPANS {
        let span = tracing::info_span!("rdm.test.outage.update.via-child", i);
        span.in_scope(|| tracing::info!(i, "inside the span"));
    }
    // The emitting is all this prints for the parent: it never waited on the collector.
    println!("EMIT_MS={}", started.elapsed().as_millis());
    // A second of outage with spans queued, then the exit a node makes.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    println!("EXITING_AT_MS={}", unix_ms());
    rdm_transport::flush_before_exit();
    drop(telemetry);
    println!("DRAINED_MS={}", started.elapsed().as_millis());
    println!("IO_PRESSURE_EXIT={}", io_pressure());
    println!("BLKIO_EXIT={}", blkio_ticks());
    std::process::exit(0);
}

/// The host's IO pressure, both lines of `/proc/pressure/io` joined by ` | `, or why it is unreadable.
fn io_pressure() -> String {
    std::fs::read_to_string("/proc/pressure/io").map(|s| s.lines().collect::<Vec<_>>().join(" | ")).unwrap_or_else(|e| format!("unreadable: {e}"))
}

/// This process's block-IO delay: `/proc/self/stat` field 42, `delayacct_blkio_ticks` (clock ticks;
/// 0 while the kernel's delay accounting is off), or why it is unreadable.
fn blkio_ticks() -> String {
    let stat = match std::fs::read_to_string("/proc/self/stat") {
        Ok(s) => s,
        Err(e) => return format!("unreadable: {e}"),
    };
    // Fields 3 on follow the parenthesised command name, which may itself hold spaces.
    match stat.rfind(')').and_then(|at| stat[at + 1..].split_whitespace().nth(42 - 3)) {
        Some(ticks) => ticks.to_string(),
        None => format!("field 42 absent: {stat}"),
    }
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
}

/// Run the child against `endpoint`; its exit time, stdout, stderr and the evidence directory this
/// call names, where the child writes its spans: the caller removes it once the cell passes, and a
/// failing cell keeps it.
fn run_child(endpoint: &str) -> (Duration, String, String, std::path::PathBuf) {
    static CHILDREN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let evidence = std::env::temp_dir().join(format!("rdm-outage-evidence-{}-{}", std::process::id(), CHILDREN.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
    let spawned_at = unix_ms();
    let mut child = Command::new(std::env::current_exe().unwrap())
        // The child runs this test alone: its libtest name carries the module path of this file.
        .args(["--exact", &format!("{}::outage_child", module_path!().split_once("::").expect("a module of the test crate").1), "--nocapture"])
        .env(CHILD, "1")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
        .env("RDM_EVIDENCE_DIR", &evidence)
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
    let ended_at = unix_ms();
    let (mut out, err) = (out_reader.join().unwrap(), err_reader.join().unwrap());
    out.push_str(&format!("SPAWNED_AT_MS={spawned_at}\nENDED_AT_MS={ended_at}\n"));
    assert!(!killed, "the child was still running after {took:?}; it never exited: stdout={out} stderr tail={}", &err[err.len().saturating_sub(600)..]);
    assert!(status.success(), "the child failed: {status:?}\n{out}\n{err}");
    (took, out, err, evidence)
}

/// The host's IO pressure at the child's start and exit and the child's block-IO delay ticks at its
/// start, after its telemetry init and at its exit, as the child printed them, with the kernel's
/// delay-accounting state (the ticks read 0 while it is off).
fn io_evidence(out: &str) -> String {
    let line = |key: &str| out.lines().find_map(|l| l.strip_prefix(&format!("{key}="))).unwrap_or("not printed").to_string();
    format!(
        "io pressure at start [{}], at exit [{}]; block-IO delay ticks at start {}, after init {}, at exit {} (task_delayacct={})",
        line("IO_PRESSURE_START"),
        line("IO_PRESSURE_EXIT"),
        line("BLKIO_START"),
        line("BLKIO_AFTER_INIT"),
        line("BLKIO_EXIT"),
        line("DELAYACCT"),
    )
}

/// The slowest step of the child's telemetry init (its `INIT_STEPS` line) and that step's own span
/// from the child's evidence (`rdm.mesh.telemetry.create.via-init-step`), or why it is not there.
fn slowest_init_step(out: &str, evidence: &std::path::Path) -> String {
    let steps = out.lines().find_map(|l| l.strip_prefix("INIT_STEPS=")).unwrap_or_default();
    let slowest = steps.split(' ').filter_map(|s| s.split_once('=')).max_by_key(|(_, ms)| ms.trim_end_matches("ms").parse::<u64>().unwrap_or(0));
    let Some((step, ms)) = slowest else { return format!("no step report in the child's output (INIT_STEPS={steps:?})") };
    let lines: Vec<String> = std::fs::read_dir(evidence).into_iter().flatten().flatten().filter(|e| e.path().to_string_lossy().ends_with(".spans.jsonl")).flat_map(|e| std::fs::read_to_string(e.path()).unwrap_or_default().lines().map(str::to_string).collect::<Vec<_>>()).collect();
    let span = lines
        .iter()
        .find(|l| l.contains("rdm.mesh.telemetry.create.via-init-step") && l.contains(&format!("\"step\":\"{step}\"")))
        .cloned()
        .unwrap_or_else(|| format!("not in the child's evidence {}", evidence.display()));
    format!("slowest step {step} {ms}, its span {span}; every step: {steps}")
}

fn number(out: &str, key: &str) -> u128 {
    out.lines().find_map(|l| l.strip_prefix(key)?.strip_prefix('=')?.trim().parse().ok()).unwrap_or_else(|| panic!("{key} missing from the child's output: {out}"))
}

/// CONTRACT: against a collector that refuses, never answers, or is unroutable, a process that
/// emits spans and log lines ends within its drain bound, emitting 2000 spans never waits on the
/// collector, and stderr names the outage once per signal, never once per span or batch.
fn outage_changes_nothing(endpoint: &str) {
    let (took, out, err, evidence) = run_child(endpoint);
    let at = |k: &str| number(&out, k);
    assert!(
        took < EXIT_BOUND + Duration::from_secs(3),
        "the child took {took:?} to end against {endpoint}: spawn to test entry {} ms, telemetry init {} ms ({}; {}), test entry to exit begun {} ms, exit begun to process ended {} ms (stdout: {out})",
        at("ENTERED_AT_MS").saturating_sub(at("SPAWNED_AT_MS")),
        at("INIT_MS"),
        slowest_init_step(&out, &evidence),
        io_evidence(&out),
        at("EXITING_AT_MS").saturating_sub(at("ENTERED_AT_MS")),
        at("ENDED_AT_MS").saturating_sub(at("EXITING_AT_MS")),
    );
    let drained = number(&out, "DRAINED_MS");
    assert!(drained < 1200 + EXIT_BOUND.as_millis(), "the drain took until {drained} ms against {endpoint}");
    let emit = number(&out, "EMIT_MS");
    assert!(emit < 2000, "emitting {SPANS} spans took {emit} ms against {endpoint}: export waited on the collector");
    let failing = err.lines().filter(|l| l.contains("export to") && l.contains("is failing")).count();
    assert!(failing <= 2, "the outage was named {failing} times (one per signal at most):\n{err}");
    let sdk_lines = err.lines().filter(|l| l.contains("ExportError") || l.contains("BatchSpanProcessor") || l.contains("BatchLogProcessor")).count();
    assert_eq!(sdk_lines, 0, "the SDK's own export-error lines reached stderr:\n{err}");
    let _ = std::fs::remove_dir_all(&evidence);
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
