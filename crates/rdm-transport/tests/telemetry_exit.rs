//! The exit drain keeps a process's own evidence (i146 Q42): a span emitted just before the exit
//! reaches the evidence file whatever the collector's state, within one drain bound, and an evidence
//! file whose open blocks ends the drain at that bound, named.
//!
//! The child half is this same test binary re-run with `RDM_EXIT_CHILD` set to the case: it
//! initialises telemetry, emits its spans and exits through `flush_before_exit` the way a short
//! process does. The parent reads the child's evidence file and its stderr.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CHILD: &str = "RDM_EXIT_CHILD";
const SERVICE: &str = "rdm-exit-child";
/// The exit drain's bound in `rdm_transport::export` (2 s): the evidence flush and the collector's share it.
const EXIT_FLUSH_BOUND: Duration = Duration::from_secs(2);
/// The spans the collector-down child emits just before its last: enough that their write outlasts an
/// exit that does not wait for it.
const BURST: usize = 5000;

/// `<pid>-<pid namespace inode>`: the tag the evidence file of this process is named by.
fn process_tag() -> String {
    let ns = std::fs::read_link("/proc/self/ns/pid").ok().and_then(|l| l.to_string_lossy().trim_start_matches("pid:[").trim_end_matches(']').parse::<u64>().ok()).unwrap_or(0);
    format!("{}-{ns}", std::process::id())
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
}

/// The child half: runs only when the parent set `RDM_EXIT_CHILD` to its case.
#[test]
fn exit_child() {
    let Ok(case) = std::env::var(CHILD) else { return };
    let dir = std::path::PathBuf::from(std::env::var("RDM_EVIDENCE_DIR").expect("the parent names the evidence directory"));
    if case == "blocked-open" {
        let fifo = dir.join(format!("{SERVICE}.{}.spans.jsonl", process_tag()));
        assert!(Command::new("mkfifo").arg(&fifo).status().expect("mkfifo runs").success(), "mkfifo {}", fifo.display());
    }
    let _telemetry = rdm_transport::init_evidence_telemetry(SERVICE);
    if case == "collector-down" {
        tracing::info_span!("rdm.test.telemetry.update.via-first-span").in_scope(|| tracing::info!("a span the first export carries to the refusing collector"));
        // Past the batch delay (200 ms) that first export has failed against the refusing collector,
        // which marks the collector down; stderr names it before EXITING.
        std::thread::sleep(Duration::from_millis(1000));
        for i in 0..BURST {
            tracing::info_span!("rdm.test.telemetry.update.via-burst-span", i).in_scope(|| {});
        }
    }
    tracing::info_span!("rdm.test.telemetry.update.via-last-span").in_scope(|| tracing::info!("the last span before the exit"));
    eprintln!("EXITING_AT_MS={}", unix_ms());
    rdm_transport::flush_before_exit();
    std::process::exit(0);
}

/// Run the child for `case` with `env`; its stdout, its stderr, when it ended, and its evidence directory.
fn run_child(case: &str, env: &[(&str, &str)]) -> (String, String, u128, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("rdm-exit-{case}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    // The child runs this test alone: its libtest name carries the module path of this file.
    cmd.args(["--exact", &format!("{}::exit_child", module_path!().split_once("::").expect("a module of the test crate").1), "--nocapture"])
        .env(CHILD, case)
        // main.rs leaves a child's environment as its parent gave it.
        .env("RDM_TEST_OWN_PROCESS", "1")
        .env("RDM_EVIDENCE_DIR", &dir)
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("RUST_LOG")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().unwrap();
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
    // A child that never ends: this is how long the cell waits to say so.
    let give_up = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(st) = child.try_wait().unwrap() {
            break st;
        }
        if Instant::now() > give_up {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("the {case} child never ended: stderr {}", err_reader.join().unwrap());
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let ended = unix_ms();
    let (out, err) = (out_reader.join().unwrap(), err_reader.join().unwrap());
    assert!(status.success(), "the {case} child failed: {status:?}\n{out}\n{err}");
    (out, err, ended, dir)
}

fn evidence_of(dir: &std::path::Path) -> String {
    std::fs::read_dir(dir).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().ends_with(".spans.jsonl")).map(|e| std::fs::read_to_string(e.path()).unwrap_or_default()).collect()
}

/// CONTRACT (i146 Q42): a process whose collector is down (its first export to a refusing collector
/// failed, named on stderr before it exits) emits a burst of spans and its last span and exits at once,
/// and every one of them is in its evidence file: the exit drain flushes the evidence file whatever the
/// collector's state. Must NOT happen: the drain returning at once because the collector is down, the
/// process exiting with spans still queued.
#[test]
fn a_process_whose_collector_is_down_keeps_every_span_it_emits_before_it_exits_at_once() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let (_, err, _, dir) = run_child("collector-down", &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")]);
    let exiting = err.find("EXITING_AT_MS=").expect("the child reached its exit");
    assert!(err[..exiting].contains("telemetry: the OTLP span export to"), "the collector was found down before the exit (the case's precondition): {err}");
    let evidence = evidence_of(&dir);
    let burst = evidence.lines().filter(|l| l.contains("rdm.test.telemetry.update.via-burst-span")).count();
    assert_eq!(burst, BURST, "every span of the burst is in the evidence file: stderr {err}");
    assert!(evidence.contains("rdm.test.telemetry.update.via-last-span"), "the last span before the exit is in the evidence file: stderr {err}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (i146 Q42, Q33): an evidence file whose open blocks (a FIFO with no reader) at the exit
/// ends the exit drain at its one bound, named on stderr as the evidence flush, and the process ends.
/// Init waited for nothing (the open is the export thread's). Must NOT happen: an exit held past the
/// bound, a drain that waits twice, or a blocked flush that names nothing.
#[test]
fn an_evidence_file_whose_open_blocks_at_the_exit_ends_the_exit_flush_at_its_bound_named() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let (_, err, ended, dir) = run_child("blocked-open", &[]);
    let exiting: u128 = err.lines().find_map(|l| l.strip_prefix("EXITING_AT_MS=")).and_then(|v| v.trim().parse().ok()).expect("the child reached its exit");
    assert!(err.contains("telemetry: the evidence flush did not finish within 2000 ms"), "the blocked flush is named: {err}");
    let drained = Duration::from_millis((ended - exiting) as u64);
    assert!(drained < EXIT_FLUSH_BOUND + Duration::from_millis(500), "the drain ended at its one bound: {drained:?} from the exit to the process's end; stderr {err}");
    let _ = std::fs::remove_dir_all(&dir);
}
