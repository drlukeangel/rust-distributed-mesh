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
struct Seen {
    request_line: String,
    content_type: Option<String>,
    body_len: usize,
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
        buf.drain(..head_end + content_length);
        seen.lock().unwrap().push(Seen { request_line, content_type, body_len: content_length });
        if stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 0\r\n\r\n").is_err() {
            return;
        }
    }
}

/// CONTRACT: a process that emits a span and a WARN log line against a collector at
/// `OTEL_EXPORTER_OTLP_ENDPOINT` sends exactly what OTLP/HTTP defines: one `POST /v1/traces` and
/// one `POST /v1/logs`, each over HTTP/1.1 with a non-empty `application/x-protobuf` body. Any
/// other first line (an HTTP/2 preface, a bare `POST /`) is the exporter on the wrong protocol or
/// the wrong path.
#[test]
fn an_exporter_posts_protobuf_spans_and_log_records_to_the_http_signal_paths() {
    if crate::own_process::delegated(module_path!(), "an_exporter_posts_protobuf_spans_and_log_records_to_the_http_signal_paths") {
        return;
    }
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

    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint);
    std::env::remove_var("RDM_EVIDENCE_DIR");
    let guard = rafka_mesh_telemetry::init_evidence_telemetry("rdm-otlp-http-export").expect("the OTLP exporter is configured by OTEL_EXPORTER_OTLP_ENDPOINT");
    let span = tracing::info_span!("rdm.test.otlp_export.update.via-http-proto");
    span.in_scope(|| tracing::warn!("a warning the log exporter sends"));
    drop(span);
    rafka_mesh_telemetry::flush_before_exit();
    drop(guard);

    let seen = seen.lock().unwrap().clone();
    let lines: Vec<&str> = seen.iter().map(|s| s.request_line.as_str()).collect();
    for path in ["/v1/traces", "/v1/logs"] {
        let want = format!("POST {path} HTTP/1.1");
        let got = seen.iter().find(|s| s.request_line == want).unwrap_or_else(|| panic!("no `{want}` reached the collector at {endpoint}; its requests were {lines:?}"));
        assert_eq!(got.content_type.as_deref(), Some("application/x-protobuf"), "`{want}` is not protobuf");
        assert!(got.body_len > 0, "`{want}` carried an empty body");
    }
}
