//! The transport diagnostic sink (R-QH1): the iroh and noq handshake and path event lines of
//! this process, and the node's own dial, accept and bind lines, in one file of the node's own
//! log directory, `<RDM_DATA_DIR>/transport-diag.log` (a process with no data directory writes
//! its own `<service>.<pid>-<pid ns>.transport-diag.log` in `RDM_EVIDENCE_DIR`, beside its span files). A red gate keeps every node directory's log files, so both ends of
//! a failed handshake are kept together.
//!
//! It exists only in an evidence run: a process given `RDM_EVIDENCE_DIR` (the test estate sets it
//! on the bootstrap admin, the admins it restarts and the probe; an admin passes its own on to every
//! node it launches). A process without it, every production node, installs no layer: no noq or
//! iroh callsite is enabled for it and no file is written.
//!
//! It is a separate sink: nothing here reaches span export or the OTLP log bridge (R-TL1), and
//! nothing here reaches the stderr log. Lines are formatted by the thread that emitted them and
//! written by a thread of their own ([`LogSink`]), so a held disk never holds a runtime worker.
//! It observes only: no transport setting, timeout or retry depends on it.
//!
//! What a pair of these files separates, for one dial: the client's and the server's handshake
//! events (no server reply, a reply sent and never received, or received and not processed), the
//! server's endpoint lines for a datagram it dropped before any connection took it, and both sides'
//! error chains. It records events only and enables no iroh or noq span: an enabled span exists for
//! every layer of the process, and one iroh entered across an `.await` and left on a worker becomes
//! the parent of noq's `drive` span, created under noq's connection lock, where tracing-subscriber's
//! "tried to clone a span that already closed" poisons that lock.

use crate::log_sink::LogSink;
use std::io::Write as _;
use tracing::Subscriber;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer};

pub use rdm_core::TRANSPORT_DIAG_FILE;

/// What the sink carries besides its own target: the iroh router's accept, the endpoint and its
/// socket (bind, path selection, the direct addresses a dial replaced), the per-remote path state,
/// noq's connection driver, every noq-proto connection line at debug (path added, packet dropped,
/// handshake failed, closed) and the noq-proto endpoint at trace (initial connection ids, a datagram
/// dropped before a connection takes it, refused or stale initials). `iroh::socket::transports::relay`,
/// the net report, address lookup and gossip are not the handshake and are off.
pub(crate) const DIRECTIVES: &[&str] = &[
    "transport_diag=trace",
    "rdm_transport::pool=info",
    "rdm_transport::server=info",
    "iroh::protocol=trace",
    "iroh::endpoint=debug",
    "iroh::socket=debug",
    "iroh::socket::remote_map=debug",
    "iroh::socket::transports::relay=off",
    "noq=debug",
    "noq_proto=debug",
    "noq_proto::endpoint=trace",
    "noq_udp=debug",
];

/// The diagnostic file, in an evidence run (`evidence`, `RDM_EVIDENCE_DIR`, set): [`TRANSPORT_DIAG_FILE`]
/// in the node's own log directory (`data_dir`, `RDM_DATA_DIR`). A process with no node log
/// directory (the probe) writes beside its span files in the evidence directory, which every probe
/// of the run shares, so its file is its own and named as they are:
/// `<service>.<process>.transport-diag.log`, `process` being this process's tag ([`crate::process_tag`]).
/// Outside an evidence run there is none, and the sink is not installed.
pub(crate) fn file_path(data_dir: Option<&str>, evidence: Option<&str>, service: &str, process: &str) -> Option<std::path::PathBuf> {
    let some = |v: Option<&str>| v.filter(|v| !v.is_empty()).map(std::path::PathBuf::from);
    let evidence = some(evidence)?;
    Some(match some(data_dir) {
        Some(dir) => dir.join(TRANSPORT_DIAG_FILE),
        None => evidence.join(format!("{service}.{process}.{TRANSPORT_DIAG_FILE}")),
    })
}

/// The filter of the formatted lines: [`DIRECTIVES`] and nothing else, whatever RUST_LOG says.
pub(crate) fn filter() -> EnvFilter {
    DIRECTIVES.iter().fold(EnvFilter::new("off"), |f, d| f.add_directive(d.parse().expect("static directive")))
}

/// The run and the process every line of the file belongs to, as the file's first line: the
/// run (`RDM_EVIDENCE_DIR`, one directory per test, shared by the bootstrap admin and every node it
/// launched), the fabric, the mesh, the node, its birth and what that birth supersedes, the
/// transport address it was given, its data directory, pid and the wall clock.
///
/// The node is the one the launch names (`RDM_NODE_NAME`). A process launched with no node name,
/// the bootstrap admin, is named by its node log directory, which the estate names after the node
/// (`<mesh>.admin.1`); its mesh is the one it is given (`RDM_MESH`).
fn header(service: &str) -> String {
    let env = |k: &str| std::env::var(k).unwrap_or_default();
    let node = std::env::var("RDM_NODE_NAME").ok().filter(|v| !v.is_empty()).unwrap_or_else(|| {
        std::env::var_os("RDM_DATA_DIR").and_then(|d| std::path::PathBuf::from(d).file_name().map(|n| n.to_string_lossy().into_owned())).unwrap_or_default()
    });
    let mesh = std::env::var("RDM_MESH").ok().filter(|v| !v.is_empty()).unwrap_or_else(|| node.split('.').next().filter(|_| node.contains('.')).unwrap_or_default().to_string());
    let unix_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    format!(
        "transport-diag run={:?} fabric={:?} fabric_id={:?} mesh={mesh:?} node={node:?} node_id={:?} incarnation={:?} supersedes={:?} transport_addr={:?} data_dir={:?} service={service:?} pid={} started_unix_ms={unix_ms}\n",
        env("RDM_EVIDENCE_DIR"),
        env("RDM_FABRIC"),
        env("RDM_FABRIC_ID"),
        env("RDM_NODE_ID"),
        env("RDM_INCARNATION_ID"),
        env("RDM_SUPERSEDES"),
        env("RDM_TRANSPORT_ADDR"),
        env("RDM_DATA_DIR"),
        std::process::id(),
    )
}

/// The sink's layers and its writer over the diagnostic file (append), or `None` when the process
/// has no directory for it. The file is created, opened and headed on the sink's own thread, never
/// the caller's (i146 Q33); one that cannot be opened is named on stderr there and its lines dropped.
pub(crate) fn start<S>(service: &str) -> Option<(impl Layer<S> + Send + Sync + 'static, LogSink)>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let env = |k: &str| std::env::var(k).ok();
    let path = file_path(env("RDM_DATA_DIR").as_deref(), env("RDM_EVIDENCE_DIR").as_deref(), service, &crate::process_tag())?;
    let dir = path.parent().map(std::path::Path::to_path_buf).unwrap_or_default();
    let head = header(service);
    let sink = LogSink::start_opening(move || {
        let opened = std::fs::create_dir_all(&dir).and_then(|()| std::fs::OpenOptions::new().create(true).append(true).open(&path));
        match opened {
            Ok(mut file) => {
                let _ = file.write_all(head.as_bytes());
                Some(file)
            }
            Err(e) => {
                eprintln!("telemetry: the transport diagnostic file {} could not be opened: {e}; transport diagnostics are off", path.display());
                None
            }
        }
    });
    // Events only: the layer enables no span of iroh or noq. A span it enabled would exist for every
    // layer of the process, and iroh enters some of its spans with a guard held across an `.await`
    // (`endpoint.rs` `bind_under_no_span`): on a multi-thread runtime the exit runs on another worker,
    // the span stays on the first worker's stack after it closes, and noq's `drive` span, created
    // while noq holds its connection's state lock, takes it as its parent. tracing-subscriber then
    // panics "tried to clone a span that already closed" under that lock, the lock is poisoned and
    // every later stream operation of that connection panics (the in-process cells that hung only
    // in evidence runs, and Rafka 141's MM-new stall, 2026-10-26).
    use tracing_subscriber::filter::FilterExt;
    let lines = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_thread_names(true)
        .with_writer(sink.clone())
        .with_filter(tracing_subscriber::filter::filter_fn(|m| m.is_event()).and(filter()));
    Some((lines, sink))
}

#[cfg(test)]
mod tests {
    use super::file_path;
    use std::path::PathBuf;

    /// CONTRACT: in an evidence run a node writes `transport-diag.log` in its own log directory;
    /// a process with no node log directory (the probe) writes beside its span files in the evidence
    /// directory, in a file of its own named as they are, `<service>.<pid>-<pid ns>.transport-diag.log`,
    /// so the probes of one run, which share that directory, never interleave lines in one file; and
    /// outside an evidence run there is no file. What must NOT happen: two processes appending to one
    /// shared diagnostic file whose lines do not say which process wrote them.
    #[test]
    fn each_process_writes_its_own_diagnostic_file_and_none_outside_an_evidence_run() {
        assert_eq!(file_path(Some("/d/mesh1.rpc.1-x"), Some("/e"), "rdm-rpc-node", "7-9"), Some(PathBuf::from("/d/mesh1.rpc.1-x/transport-diag.log")));
        assert_eq!(file_path(None, Some("/e"), "rdm-rpc-probe", "7-9"), Some(PathBuf::from("/e/rdm-rpc-probe.7-9.transport-diag.log")));
        assert_eq!(file_path(Some(""), Some("/e"), "rdm-rpc-probe", "8-9"), Some(PathBuf::from("/e/rdm-rpc-probe.8-9.transport-diag.log")));
        assert_eq!(file_path(Some("/d/mesh1.rpc.1-x"), None, "rdm-rpc-node", "7-9"), None);
        assert_eq!(file_path(Some("/d/mesh1.rpc.1-x"), Some(""), "rdm-rpc-node", "7-9"), None);
    }
}
