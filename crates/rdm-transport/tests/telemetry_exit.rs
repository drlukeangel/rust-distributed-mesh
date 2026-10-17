//! The exit drain keeps a process's own evidence (i146 Q42): a span emitted just before the exit
//! reaches the evidence file whatever the collector's state, within one drain bound, and an evidence
//! file whose open blocks ends the drain at that bound, named.
//!
//! The child half is this same test binary re-run with `RDM_EXIT_CHILD` set to the case: it
//! initialises telemetry, emits its spans and exits through `flush_before_exit` the way a short
//! process does. The parent reads the child's evidence file and its stderr.

use std::io::{BufRead, Read, Write};
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
        // The parent releases this child only after it reads the named collector outage. The
        // shared export thread may still be opening/writing evidence: elapsed time proves nothing.
        std::io::stdin().read_exact(&mut [0u8; 1]).expect("the parent observed the collector outage");
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
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().unwrap();
    let (mut out, err) = (child.stdout.take().unwrap(), child.stderr.take().unwrap());
    let mut release = child.stdin.take();
    let awaiting_outage = case == "collector-down";
    let out_reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = out.read_to_string(&mut s);
        s
    });
    let err_reader = std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(err);
        let mut s = String::new();
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if awaiting_outage && line.contains("telemetry: the OTLP span export to") {
                        if let Some(mut stdin) = release.take() {
                            stdin.write_all(&[1]).expect("release the child after the named outage");
                        }
                    }
                    s.push_str(&line);
                }
            }
        }
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

/// `(unwritten, handed)` when the exit drain's evidence flush ended at its bound and named its loss.
fn named_loss(stderr: &str) -> Option<(usize, usize)> {
    let line = stderr.lines().find(|l| l.contains("the evidence flush ended at its bound"))?;
    let (_, tail) = line.split_once("): ")?;
    let mut words = tail.split_whitespace();
    let unwritten = words.next()?.parse().ok()?;
    let handed = words.nth(1)?.parse().ok()?;
    Some((unwritten, handed))
}

fn evidence_of(dir: &std::path::Path) -> String {
    std::fs::read_dir(dir).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().ends_with(".spans.jsonl")).map(|e| std::fs::read_to_string(e.path()).unwrap_or_default()).collect()
}

/// CONTRACT (i146 Q42; Luke 2026-10-17: the bound is the contract, a named, bounded loss): a process
/// whose collector is down (its first export to a refusing collector failed, named on stderr before it
/// exits) emits a burst of spans and its last span and exits at once. The exit drain flushes the
/// evidence file whatever the collector's state, within its one bound: every span handed is in the
/// evidence file, or the flush ended at its bound and named how many of them it did not confirm
/// written. Must NOT happen: the drain returning at once because the collector is down, a span lost
/// unnamed, an exit held past the bound.
#[test]
fn a_process_whose_collector_is_down_keeps_every_span_it_emits_before_it_exits_at_once() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let (_, err, ended, dir) = run_child("collector-down", &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")]);
    let exiting = err.find("EXITING_AT_MS=").expect("the child reached its exit");
    assert!(err[..exiting].contains("telemetry: the OTLP span export to"), "the collector was found down before the exit (the case's precondition): {err}");
    let exited_at: u128 = err.lines().find_map(|l| l.strip_prefix("EXITING_AT_MS=")).and_then(|v| v.trim().parse().ok()).expect("the exit's time");
    let drained = Duration::from_millis((ended - exited_at) as u64);
    assert!(drained < EXIT_FLUSH_BOUND + Duration::from_millis(500), "the drain ended within its one bound: {drained:?}; stderr {err}");
    let evidence = evidence_of(&dir);
    let burst = evidence.lines().filter(|l| l.contains("rdm.test.telemetry.update.via-burst-span")).count();
    match named_loss(&err) {
        None => {
            assert_eq!(burst, BURST, "every span of the burst is in the evidence file: stderr {err}");
            assert!(evidence.contains("rdm.test.telemetry.update.via-last-span"), "the last span before the exit is in the evidence file: stderr {err}");
        }
        Some((unwritten, handed)) => {
            let written = evidence.lines().filter(|l| !l.trim().is_empty()).count();
            assert!(handed >= BURST + 2 && written + unwritten >= handed, "a bounded loss is named whole: {written} written and {unwritten} named unwritten of {handed} handed; stderr {err}");
        }
    }
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
    assert!(named_loss(&err).is_some(), "the blocked flush ended at its bound and named its loss: {err}");
    let drained = Duration::from_millis((ended - exiting) as u64);
    assert!(drained < EXIT_FLUSH_BOUND + Duration::from_millis(500), "the drain ended at its one bound: {drained:?} from the exit to the process's end; stderr {err}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Q42's second exit shape: the process has no earlier export to open its evidence file. Its one
/// application span ends immediately before the drain, which must create and write the file.
#[test]
fn a_process_that_emits_one_span_and_exits_immediately_leaves_its_span_file() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let (_, err, ended, dir) = run_child("immediate", &[]);
    let exiting: u128 = err.lines().find_map(|l| l.strip_prefix("EXITING_AT_MS=")).and_then(|v| v.trim().parse().ok()).expect("the child reached its exit");
    let spans = evidence_of(&dir);
    assert_eq!(spans.lines().filter(|line| line.contains("rdm.test.telemetry.update.via-last-span")).count(), 1, "the first flush creates the file and writes its one application span: {err}");
    assert!(Duration::from_millis((ended - exiting) as u64) < EXIT_FLUSH_BOUND + Duration::from_millis(500), "the immediate exit shares Q42's existing drain bound: {err}");
    let _ = std::fs::remove_dir_all(&dir);
}
