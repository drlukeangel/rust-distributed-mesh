//! The transport diagnostic sink (R-QH1): the iroh and noq handshake, path and packet lines of
//! this process, and the node's own dial, accept and bind lines, in one file of the node's own
//! log directory, `<RDM_DATA_DIR>/transport-diag.log` (a process with no data directory writes
//! it in `RDM_EVIDENCE_DIR`). A red gate keeps every node directory's log files, so both ends of
//! a failed handshake are kept together.
//!
//! It is a separate sink: nothing here reaches span export or the OTLP log bridge (R-TL1), and
//! nothing here reaches the stderr log. Lines are formatted by the thread that emitted them and
//! written by a thread of their own ([`LogSink`]), so a held disk never holds a runtime worker.
//! It observes only: no transport setting, timeout or retry depends on it.
//!
//! What a pair of these files separates, for one dial: the client's `noq.packet sent` lines for
//! the Initial space and the server's `noq.packet received` lines for the same handshake (no
//! server reply, or a reply sent and never received, or received and not processed), the server's
//! endpoint lines for a datagram it dropped before any connection took it, and both sides'
//! error chains.

use crate::log_sink::LogSink;
use std::fmt::Write as _;
use std::io::Write as _;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer};

/// The diagnostic file's name in the node's log directory.
pub const TRANSPORT_DIAG_FILE: &str = "transport-diag.log";

/// The target of the node's own diagnostic lines (dial, accept, bind): routed to this sink only.
/// It shares no prefix with the crates span export admits (`rafka`, `rshape`, `rdm`, `main`).
pub const TRANSPORT_DIAG_TARGET: &str = "transport_diag";

/// What the sink carries besides its own target: the iroh router's accept, the endpoint and its
/// socket (bind, path selection, the direct addresses a dial replaced), the per-remote path state,
/// noq's connection driver, every noq-proto connection line at debug (path added, packet dropped,
/// handshake failed, closed) and the noq-proto endpoint at trace (initial connection ids, a datagram
/// dropped before a connection takes it, refused or stale initials). `iroh::socket::transports::relay`,
/// the net report, address lookup and gossip are not the handshake and are off.
pub(crate) const DIRECTIVES: &[&str] = &[
    "transport_diag=trace",
    "rafka_node_rpc::pool=info",
    "rafka_node_rpc::server=info",
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

/// The directory the diagnostic file goes in: the node's own log directory, else the evidence
/// directory; with neither there is no file.
pub(crate) fn dir() -> Option<std::path::PathBuf> {
    ["RDM_DATA_DIR", "RDM_EVIDENCE_DIR"].iter().find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty())).map(std::path::PathBuf::from)
}

/// The filter of the formatted lines: [`DIRECTIVES`] and nothing else, whatever RUST_LOG says.
pub(crate) fn filter() -> EnvFilter {
    DIRECTIVES.iter().fold(EnvFilter::new("off"), |f, d| f.add_directive(d.parse().expect("static directive")))
}

/// The run and the process every line of the file belongs to, as the file's first line: the
/// run (`RDM_EVIDENCE_DIR`, one directory per test, shared by the bootstrap admin and every node it
/// launched), the fabric, the node, its birth and what that birth supersedes, the transport address
/// it was given, its data directory, pid and the wall clock.
fn header(service: &str) -> String {
    let env = |k: &str| std::env::var(k).unwrap_or_default();
    let unix_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    format!(
        "transport-diag run={:?} fabric={:?} fabric_id={:?} node={:?} node_id={:?} incarnation={:?} supersedes={:?} transport_addr={:?} data_dir={:?} service={service:?} pid={} started_unix_ms={unix_ms}\n",
        env("RDM_EVIDENCE_DIR"),
        env("RDM_FABRIC"),
        env("RDM_FABRIC_ID"),
        env("RDM_NODE_NAME"),
        env("RDM_NODE_ID"),
        env("RDM_INCARNATION_ID"),
        env("RDM_SUPERSEDES"),
        env("RDM_TRANSPORT_ADDR"),
        env("RDM_DATA_DIR"),
        std::process::id(),
    )
}

/// Open the diagnostic file (append) and return the sink's layers and its writer, or `None` when
/// the process has no directory for it or the file cannot be opened (named on stderr).
pub(crate) fn start<S>(service: &str) -> Option<(impl Layer<S> + Send + Sync + 'static, LogSink)>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let dir = dir()?;
    let path = dir.join(TRANSPORT_DIAG_FILE);
    let opened = std::fs::create_dir_all(&dir).and_then(|()| std::fs::OpenOptions::new().create(true).append(true).open(&path));
    let mut file = match opened {
        Ok(f) => f,
        Err(e) => {
            eprintln!("telemetry: the transport diagnostic file {} could not be opened: {e}; transport diagnostics are off", path.display());
            return None;
        }
    };
    let _ = file.write_all(header(service).as_bytes());
    let sink = LogSink::start(file);
    let lines = tracing_subscriber::fmt::layer().with_ansi(false).with_target(true).with_thread_names(true).with_writer(sink.clone()).with_filter(filter());
    let packets = PacketLayer { sink: sink.clone() }.with_filter(tracing_subscriber::filter::filter_fn(PacketLayer::admits));
    Some((lines.and_then(packets), sink))
}

/// The noq packet spans of the handshake, one line each: `send{space, pn, path_id}` as
/// `noq.packet sent`, `recv{space, pn}` (an authenticated, decrypted packet) as `noq.packet received`
/// and `first recv` (the server's first Initial of a connection) as `noq.packet first-received`,
/// each with the noq connection handle (`drive{id}`) it belongs to. Only the Initial and Handshake
/// spaces are written: the handshake is the evidence, and application data is every packet after it.
struct PacketLayer {
    sink: LogSink,
}

/// The noq connection handle a span belongs to, kept in the `drive` span's extensions.
struct Drive(String);

#[derive(Default)]
struct Fields(String, Option<String>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let v = format!("{value:?}");
        if field.name() == "space" {
            self.1 = Some(v.clone());
        }
        let _ = write!(self.0, " {}={v}", field.name());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_debug(field, &format_args!("{value}"));
    }
}

impl PacketLayer {
    fn admits(meta: &tracing::Metadata<'_>) -> bool {
        meta.is_span() && (meta.target().starts_with("noq_proto::connection") && matches!(meta.name(), "send" | "recv" | "first recv") || meta.target().starts_with("noq::connection") && meta.name() == "drive")
    }
}

impl<S> Layer<S> for PacketLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        let meta = attrs.metadata();
        if meta.name() == "drive" {
            if let Some(span) = ctx.span(id) {
                span.extensions_mut().insert(Drive(fields.0.trim().to_string()));
            }
            return;
        }
        if fields.1.as_deref().is_some_and(|s| s.contains("Data")) {
            return;
        }
        let conn = ctx
            .span(id)
            .and_then(|s| s.scope().skip(1).find_map(|p| p.extensions().get::<Drive>().map(|d| d.0.clone())))
            .unwrap_or_default();
        let what = match meta.name() {
            "send" => "sent",
            "recv" => "received",
            _ => "first-received",
        };
        // The fmt lines' clock and format, so both kinds of line sort together.
        let mut ts = String::new();
        let _ = tracing_subscriber::fmt::time::FormatTime::format_time(&tracing_subscriber::fmt::time::SystemTime, &mut tracing_subscriber::fmt::format::Writer::new(&mut ts));
        let thread = std::thread::current();
        let line = format!("{ts}  TRACE noq.packet {what} conn{{{conn}}}{} thread={}\n", fields.0, thread.name().unwrap_or("-"));
        let mut w = tracing_subscriber::fmt::MakeWriter::make_writer(&self.sink);
        let _ = w.write_all(line.as_bytes());
    }
}
