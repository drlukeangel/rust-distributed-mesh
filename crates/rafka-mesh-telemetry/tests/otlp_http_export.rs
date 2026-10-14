//! The OTLP exporter speaks HTTP: protobuf spans and log records POSTed to the collector's signal
//! paths, not gRPC.
//!
//! The collector here is a bare `std::net::TcpListener` that reads each request's head and body,
//! records the request line and content type, and answers `200`. It is the stand-in for the
//! collector's HTTP door, not an HTTP server of the crate under test.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

/// What one request to the collector carried.
#[derive(Debug, Clone)]
pub(crate) struct Seen {
    pub(crate) request_line: String,
    content_type: Option<String>,
    pub(crate) body: Vec<u8>,
}

/// Read requests from `stream` until it closes, recording each and answering `200`.
fn serve(mut stream: TcpStream, seen: Arc<Mutex<Vec<Seen>>>) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let head_end = loop {
            if let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().unwrap_or_default().to_string();
        let header = |name: &str| lines.clone().find_map(|l| l.split_once(':').filter(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.trim().to_string()));
        let content_length: usize = header("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
        let content_type = header("content-type");
        while buf.len() < head_end + content_length {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let rest = buf.split_off(head_end + content_length);
        let body = buf.split_off(head_end);
        buf = rest;
        seen.lock().unwrap().push(Seen { request_line, content_type, body });
        if stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 0\r\n\r\n").is_err() {
            return;
        }
    }
}

/// The service name the process under test exports as.
const SERVICE: &str = "rdm-otlp-http-export";

/// A collector on a loopback port of this process: every request it receives is recorded.
pub(crate) struct Collector {
    pub(crate) endpoint: String,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Collector {
    pub(crate) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let collector_seen = seen.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let seen = collector_seen.clone();
                std::thread::spawn(move || serve(stream, seen));
            }
        });
        Self { endpoint, seen }
    }

    /// The environment of the process that exports to this collector: its endpoint, and no evidence
    /// directory.
    fn exporter_env(&self) -> [(&'static str, Option<&std::ffi::OsStr>); 2] {
        [("OTEL_EXPORTER_OTLP_ENDPOINT", Some(std::ffi::OsStr::new(&self.endpoint))), ("RDM_EVIDENCE_DIR", None)]
    }

    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

/// The exporting half, run by the delegated child whose environment names the collector: one span
/// carrying one WARN log line, flushed the way a node flushes before it exits.
fn export_one_span_and_one_log_line() {
    let guard = rafka_mesh_telemetry::init_evidence_telemetry(SERVICE).expect("the OTLP exporter is configured by OTEL_EXPORTER_OTLP_ENDPOINT");
    let span = tracing::info_span!("rdm.test.otlp_export.update.via-http-proto");
    span.in_scope(|| tracing::warn!("a warning the log exporter sends"));
    drop(span);
    rafka_mesh_telemetry::flush_before_exit();
    drop(guard);
}

/// CONTRACT: a process that emits a span and a WARN log line against a collector at
/// `OTEL_EXPORTER_OTLP_ENDPOINT` sends exactly what OTLP/HTTP defines: one `POST /v1/traces` and
/// one `POST /v1/logs`, each over HTTP/1.1 with a non-empty `application/x-protobuf` body. Any
/// other first line (an HTTP/2 preface, a bare `POST /`) is the exporter on the wrong protocol or
/// the wrong path.
#[test]
fn an_exporter_posts_protobuf_spans_and_log_records_to_the_http_signal_paths() {
    let collector = Collector::start();
    if !crate::own_process::delegated_with_env(module_path!(), "an_exporter_posts_protobuf_spans_and_log_records_to_the_http_signal_paths", &collector.exporter_env()) {
        return export_one_span_and_one_log_line();
    }
    let (endpoint, seen) = (collector.endpoint.clone(), collector.seen());
    let lines: Vec<&str> = seen.iter().map(|s| s.request_line.as_str()).collect();
    for path in ["/v1/traces", "/v1/logs"] {
        let want = format!("POST {path} HTTP/1.1");
        let got = seen.iter().find(|s| s.request_line == want).unwrap_or_else(|| panic!("no `{want}` reached the collector at {endpoint}; its requests were {lines:?}"));
        assert_eq!(got.content_type.as_deref(), Some("application/x-protobuf"), "`{want}` is not protobuf");
        assert!(!got.body.is_empty(), "`{want}` carried an empty body");
    }
}

/// CONTRACT: what the collector receives names its sender. The span request and the log request
/// both carry the process's resource, `service.name` set to the name the process initialised
/// telemetry with, so the trace store files the spans under that service. Protobuf strings travel
/// as their UTF-8 bytes, so the key and the name are found in the body as they are.
#[test]
fn an_exporter_sends_the_process_resource_with_spans_and_log_records() {
    let collector = Collector::start();
    if !crate::own_process::delegated_with_env(module_path!(), "an_exporter_sends_the_process_resource_with_spans_and_log_records", &collector.exporter_env()) {
        return export_one_span_and_one_log_line();
    }
    let (endpoint, seen) = (collector.endpoint.clone(), collector.seen());
    let has = |body: &[u8], needle: &str| body.windows(needle.len()).any(|w| w == needle.as_bytes());
    for path in ["/v1/traces", "/v1/logs"] {
        let want = format!("POST {path} HTTP/1.1");
        let got = seen.iter().find(|s| s.request_line == want).unwrap_or_else(|| panic!("no `{want}` reached the collector at {endpoint}"));
        assert!(has(&got.body, "service.name") && has(&got.body, SERVICE), "`{want}` carries no resource naming `{SERVICE}` as its service: {} body bytes", got.body.len());
    }
}
