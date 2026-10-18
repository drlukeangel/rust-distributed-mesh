//! The exit flush keeps a process's own evidence (i146 Q42): a span emitted just before the exit
//! reaches the evidence file whatever the collector's state, within one bound, and an evidence file
//! whose open blocks ends the flush at that bound, named. The bound holds the node's own times (i146
//! Q60): the flush returns and the exit is called by it; the kernel's teardown after the exit's call
//! (a thread still inside a filesystem call) is recorded with the threads' states, never bounded.
//!
//! The child half is this same test binary re-run with `RDM_EXIT_CHILD` set to the case: it
//! initialises telemetry, emits its spans and exits through `flush_before_exit` the way a short
//! process does. The parent reads the child's evidence file and its stderr.

use std::io::{BufRead, Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CHILD: &str = "RDM_EXIT_CHILD";
const SERVICE: &str = "rdm-exit-child";
/// The exit flush's bound, `rdm_transport::export::EXIT_FLUSH_BOUND` (2 s): the evidence flush and the
/// collector's share it.
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

/// The file the parent writes beside the child's evidence: every sample of the child's threads, a
/// line `<unix ms> <thread>:<state>:<wchan>` each.
const THREAD_STATES: &str = "thread-states";

/// Where the time from the child's exit line to its end went: the exit flush, the exit's call (both
/// the child's own lines), the process's end as this parent's waiting thread saw it, and what the
/// child's threads were doing in two windows: the flush (from the exit line to the exit's call) and
/// the teardown (from the exit's call to the end seen).
fn exit_story(err: &str, dir: &std::path::Path, exiting: u128, ended: u128) -> String {
    let flushed: Option<u128> = err.lines().find_map(|l| l.strip_prefix("EXIT_FLUSH_RETURNED_AFTER_MS=")).and_then(|v| v.trim().parse().ok());
    let called: Option<u128> = err.lines().find_map(|l| l.strip_prefix("EXIT_CALLED_AT_MS=")).and_then(|v| v.trim().parse().ok());
    let call = called.unwrap_or(ended);
    format!(
        "the exit flush returned after {} ms, the exit was called at +{} ms, the process's end was seen at +{} ms; the child's threads, sampled every 20 ms (thread:state:wait channel first..last ms from the exit line, samples): in the flush [{}]; in the teardown [{}]",
        flushed.map(|f| f.to_string()).unwrap_or_else(|| "?".into()),
        called.map(|c| c.saturating_sub(exiting).to_string()).unwrap_or_else(|| "?".into()),
        ended.saturating_sub(exiting),
        threads_between(dir, exiting, exiting, call),
        threads_between(dir, exiting, call, ended + 1)
    )
}

/// The child's thread samples in `[from, to)` (unix ms), by thread, state and wait channel: the
/// first and last sample of each, in ms from `origin`, and how many.
fn threads_between(dir: &std::path::Path, origin: u128, from: u128, to: u128) -> String {
    let mut seen = std::collections::BTreeMap::<String, (u128, u128, u32)>::new();
    for line in std::fs::read_to_string(dir.join(THREAD_STATES)).unwrap_or_default().lines() {
        let Some((at, key)) = line.split_once(' ') else { continue };
        let Ok(at) = at.parse::<u128>() else { continue };
        if at < from || at >= to {
            continue;
        }
        let rel = at.saturating_sub(origin);
        let e = seen.entry(key.to_string()).or_insert((rel, rel, 0));
        e.1 = rel;
        e.2 += 1;
    }
    seen.iter().map(|(k, (first, last, n))| format!("{k} {first}..{last} x{n}")).collect::<Vec<_>>().join(", ")
}

/// The node's own times at its exit, held to the bound (i146 Q60): the exit flush returned by it and
/// the exit was called at it; the kernel's teardown after the call is recorded on the cell's own span
/// (`rdm.test.telemetry.update.via-exit-teardown`, in the binary's evidence directory), never bounded.
fn held_to_its_bound(case: &str, err: &str, dir: &std::path::Path, exiting: u128, ended: u128) {
    let line = |key: &str| err.lines().find_map(|l| l.strip_prefix(key)).and_then(|v| v.trim().parse::<u128>().ok());
    let flushed = line("EXIT_FLUSH_RETURNED_AFTER_MS=").unwrap_or_else(|| panic!("the child named no exit flush's return: {err}"));
    let called = line("EXIT_CALLED_AT_MS=").unwrap_or_else(|| panic!("the child named no exit's call: {err}")).saturating_sub(exiting);
    let story = exit_story(err, dir, exiting, ended);
    {
        let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-exit-teardown").expect("the cell's evidence file can be created");
        tracing::info_span!("rdm.test.telemetry.update.via-exit-teardown", case, flushed_ms = flushed as u64, called_ms = called as u64, ended_ms = ended.saturating_sub(exiting) as u64, teardown = %story)
            .in_scope(|| tracing::info!("the kernel's teardown after the exit's call, recorded unbounded"));
        drop(capture);
    }
    let bound = (EXIT_FLUSH_BOUND + Duration::from_millis(500)).as_millis();
    assert!(flushed < bound, "the exit flush returned by its bound: after {flushed} ms; {story}; stderr {err}");
    assert!(called < bound, "the exit was called at its bound: at +{called} ms; {story}; stderr {err}");
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
    let flush_started = Instant::now();
    rdm_transport::flush_before_exit();
    eprintln!("EXIT_FLUSH_RETURNED_AFTER_MS={}", flush_started.elapsed().as_millis());
    // On the pipe, never a file: a write to a file can wait on the filesystem's journal, and this
    // line is the moment the exit is called.
    eprintln!("EXIT_CALLED_AT_MS={}", unix_ms());
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
    // The process's end, as a thread blocked in wait() sees it the moment the kernel reports it. A
    // child that never ends: this is how long the cell waits to say so.
    let pid = child.id();
    let (ended_tx, ended_rx) = std::sync::mpsc::channel();
    let ended_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // While the child runs, every 20 ms: each of its threads' state and kernel wait channel, from
    // /proc, so a slow exit names what the child's threads were doing (a thread in `D` waits on I/O).
    let states = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(u128, String)>::new()));
    let sampler = {
        let (ended_flag, states) = (ended_flag.clone(), states.clone());
        std::thread::spawn(move || {
            while !ended_flag.load(std::sync::atomic::Ordering::Relaxed) {
                if let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) {
                    for t in tasks.flatten() {
                        let stat = std::fs::read_to_string(t.path().join("stat")).unwrap_or_default();
                        let state = stat.rsplit_once(") ").and_then(|(_, rest)| rest.split_whitespace().next()).unwrap_or("?").to_string();
                        let wchan = std::fs::read_to_string(t.path().join("wchan")).unwrap_or_default();
                        let comm = std::fs::read_to_string(t.path().join("comm")).unwrap_or_default();
                        states.lock().unwrap().push((unix_ms(), format!("{}:{state}:{}", comm.trim(), if wchan.is_empty() || wchan == "0" { "-" } else { wchan.trim() })));
                    }
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    };
    std::thread::spawn(move || {
        let status = child.wait();
        let _ = ended_tx.send((status, unix_ms()));
    });
    let (status, ended) = match ended_rx.recv_timeout(Duration::from_secs(20)) {
        Ok((status, ended)) => {
            ended_flag.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = sampler.join();
            // The samples, each `<unix ms> <thread>:<state>:<wchan>`, beside the evidence for a bound's message.
            let seen = states.lock().unwrap().iter().map(|(at, k)| format!("{at} {k}")).collect::<Vec<_>>().join("\n");
            let _ = std::fs::write(dir.join(THREAD_STATES), seen);
            (status.unwrap(), ended)
        }
        Err(_) => {
            // The child this cell spawned, not yet reaped (its wait has not returned).
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
            panic!("the {case} child never ended: stderr {}", err_reader.join().unwrap());
        }
    };
    let (out, err) = (out_reader.join().unwrap(), err_reader.join().unwrap());
    assert!(status.success(), "the {case} child failed: {status:?}\n{out}\n{err}");
    (out, err, ended, dir)
}

/// `(unwritten, handed)` when the exit's evidence flush ended at its bound and named its loss.
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
/// exits) emits a burst of spans and its last span and exits at once. The exit flush writes the
/// evidence file whatever the collector's state, within its one bound: every span handed is in the
/// evidence file, or the flush ended at its bound and named how many of them it did not confirm
/// written; the flush returns and the exit is called by the bound (Q60). Must NOT happen: the flush
/// returning at once because the collector is down, a span lost unnamed, the flush's return or the
/// exit's call past the bound.
#[test]
fn a_process_whose_collector_is_down_keeps_every_span_it_emits_before_it_exits_at_once() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let (_, err, ended, dir) = run_child("collector-down", &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")]);
    let exiting = err.find("EXITING_AT_MS=").expect("the child reached its exit");
    assert!(err[..exiting].contains("telemetry: the OTLP span export to"), "the collector was found down before the exit (the case's precondition): {err}");
    let exited_at: u128 = err.lines().find_map(|l| l.strip_prefix("EXITING_AT_MS=")).and_then(|v| v.trim().parse().ok()).expect("the exit's time");
    held_to_its_bound("collector-down", &err, &dir, exited_at, ended);
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
/// ends the exit flush at its one bound, named on stderr as the evidence flush, and the exit is called
/// at it (Q60). Init waited for nothing (the open is the export thread's). Must NOT happen: the flush's
/// return or the exit's call past the bound, a flush that waits twice, or a blocked flush that names
/// nothing.
#[test]
fn an_evidence_file_whose_open_blocks_at_the_exit_ends_the_exit_flush_at_its_bound_named() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let (_, err, ended, dir) = run_child("blocked-open", &[]);
    let exiting: u128 = err.lines().find_map(|l| l.strip_prefix("EXITING_AT_MS=")).and_then(|v| v.trim().parse().ok()).expect("the child reached its exit");
    assert!(named_loss(&err).is_some(), "the blocked flush ended at its bound and named its loss: {err}");
    held_to_its_bound("blocked-open", &err, &dir, exiting, ended);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Q42's second exit shape: the process has no earlier export to open its evidence file. Its one
/// application span ends immediately before the exit flush, which must create and write the file, or
/// end at its bound with the loss named whole (Luke 2026-10-17: the bound is the contract, a named,
/// bounded loss); the flush returns and the exit is called by the bound (Q60).
#[test]
fn a_process_that_emits_one_span_and_exits_immediately_leaves_its_span_file() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let (_, err, ended, dir) = run_child("immediate", &[]);
    let exiting: u128 = err.lines().find_map(|l| l.strip_prefix("EXITING_AT_MS=")).and_then(|v| v.trim().parse().ok()).expect("the child reached its exit");
    let spans = evidence_of(&dir);
    match named_loss(&err) {
        None => assert_eq!(spans.lines().filter(|line| line.contains("rdm.test.telemetry.update.via-last-span")).count(), 1, "the first flush creates the file and writes its one application span: {err}"),
        Some((unwritten, handed)) => {
            let written = spans.lines().filter(|l| !l.trim().is_empty()).count();
            assert!(unwritten > 0 && written + unwritten >= handed, "a bounded loss is named whole: {written} written and {unwritten} named unwritten of {handed} handed; stderr {err}");
        }
    }
    held_to_its_bound("immediate", &err, &dir, exiting, ended);
    let _ = std::fs::remove_dir_all(&dir);
}
