//! Telemetry init waits on no file and no thread (i146 Q33): production calls it inside `run_with`,
//! so whatever it waits on delays every node's start.
//!
//! The child half is this same test binary re-run with `RDM_INIT_CHILD` set. Its evidence file and
//! its transport diagnostic file are FIFOs with no reader: an open of either for writing blocks until
//! a reader comes, the way a held disk blocks a create, but forever, so an init that opens a file on
//! its own thread never returns. The child prints `INIT_RETURNED_MS` once init returns, and only then
//! opens a reader on each FIFO; it emits a span and flushes, so the evidence file's opening, on the
//! export thread, is proven to still carry the spans.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CHILD: &str = "RDM_INIT_CHILD";
const SERVICE: &str = "rdm-init-child";

/// `<pid>-<pid namespace inode>`: the tag the evidence file of this process is named by.
fn process_tag() -> String {
    let ns = std::fs::read_link("/proc/self/ns/pid").ok().and_then(|l| l.to_string_lossy().trim_start_matches("pid:[").trim_end_matches(']').parse::<u64>().ok()).unwrap_or(0);
    format!("{}-{ns}", std::process::id())
}

fn mkfifo(path: &std::path::Path) {
    let status = Command::new("mkfifo").arg(path).status().expect("mkfifo runs");
    assert!(status.success(), "mkfifo {} failed: {status:?}", path.display());
}

/// Read `fifo` to its end on a thread of its own; the bytes read, once its writer closes it.
fn read_fifo(fifo: std::path::PathBuf) -> std::thread::JoinHandle<usize> {
    std::thread::spawn(move || {
        let mut f = std::fs::File::open(&fifo).expect("the FIFO opens for reading");
        let mut bytes = Vec::new();
        let _ = f.read_to_end(&mut bytes);
        bytes.len()
    })
}

/// The child half: runs only when the parent set `RDM_INIT_CHILD`.
#[test]
fn init_child() {
    if std::env::var(CHILD).is_err() {
        return;
    }
    let dir = std::path::PathBuf::from(std::env::var("RDM_EVIDENCE_DIR").expect("the parent names the evidence directory"));
    let evidence = dir.join(format!("{SERVICE}.{}.spans.jsonl", process_tag()));
    mkfifo(&evidence);
    let diag = std::path::PathBuf::from(std::env::var("RDM_DATA_DIR").expect("the parent names the data directory")).join(rdm_core::TRANSPORT_DIAG_FILE);
    let started = Instant::now();
    let telemetry = rdm_transport::init_evidence_telemetry(SERVICE);
    println!("INIT_RETURNED_MS={}", started.elapsed().as_millis());
    println!("INIT_STEPS={}", rdm_transport::init_steps().map(|s| s.to_string()).unwrap_or_default());
    let (evidence_read, diag_read) = (read_fifo(evidence), read_fifo(diag));
    tracing::info_span!("rdm.test.telemetry.create.via-init-child").in_scope(|| tracing::info!("a span after init"));
    rdm_transport::flush_before_exit();
    drop(telemetry);
    println!("EVIDENCE_READ_BYTES={}", evidence_read.join().unwrap());
    // The diagnostic file's writer holds it open until the process ends: its header is what was
    // written, and the reader is not joined.
    let _ = diag_read;
    std::process::exit(0);
}

/// CONTRACT (i146 Q33): telemetry init returns while neither the evidence file nor the transport
/// diagnostic file can be opened (each a FIFO with no reader, an open that blocks until one comes):
/// init opens neither on its own thread and waits for no thread to start. The evidence file is opened
/// on the export thread once a reader is there, and the child's span reaches it. Must NOT happen: the
/// child still in init at the bound (an open, or a wait, on init's thread), or an evidence file that
/// carried nothing.
#[test]
fn telemetry_init_returns_while_its_files_cannot_be_opened() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let root = std::env::temp_dir().join(format!("rdm-telemetry-init-{}", std::process::id()));
    let (evidence, data) = (root.join("evidence"), root.join("data"));
    std::fs::create_dir_all(&evidence).unwrap();
    std::fs::create_dir_all(&data).unwrap();
    mkfifo(&data.join(rdm_core::TRANSPORT_DIAG_FILE));
    let mut child = Command::new(std::env::current_exe().unwrap())
        // The child runs this test alone: its libtest name carries the module path of this file.
        .args(["--exact", &format!("{}::init_child", module_path!().split_once("::").expect("a module of the test crate").1), "--nocapture"])
        .env(CHILD, "1")
        // main.rs leaves a child's environment as its parent gave it.
        .env("RDM_TEST_OWN_PROCESS", "1")
        .env("RDM_EVIDENCE_DIR", &evidence)
        .env("RDM_DATA_DIR", &data)
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
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
    // A child that never leaves init never exits: this is how long the cell waits to say so.
    let give_up = Instant::now() + Duration::from_secs(20);
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
    let (out, err) = (out_reader.join().unwrap(), err_reader.join().unwrap());
    let line = |key: &str| out.lines().find_map(|l| l.strip_prefix(&format!("{key}="))).map(str::to_string);
    assert!(
        line("INIT_RETURNED_MS").is_some(),
        "telemetry init never returned while its files could not be opened (killed={killed}): it opens a file or waits on init's thread; stdout={out} stderr tail={}",
        &err[err.len().saturating_sub(600)..]
    );
    assert!(!killed && status.success(), "the child failed after init returned: {status:?}, killed={killed}\n{out}\n{err}");
    let carried: usize = line("EVIDENCE_READ_BYTES").and_then(|n| n.parse().ok()).unwrap_or(0);
    assert!(carried > 0, "the evidence file, opened on the export thread, carried the child's spans: stdout={out}");
    println!("telemetry init returned in {} ms with both files unopenable; steps: {}", line("INIT_RETURNED_MS").unwrap_or_default(), line("INIT_STEPS").unwrap_or_default());
    let _ = std::fs::remove_dir_all(&root);
}
