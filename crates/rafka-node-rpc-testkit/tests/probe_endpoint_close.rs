//! Functional: the probe closes its one socket before it exits, whatever the call did.
//!
//! A `--via` call whose exact target the admin's view does not hold sends nothing: the probe's
//! endpoint is bound and never used. An endpoint dropped open makes iroh abort the process
//! ungracefully, and the probe's span file (`rdm.node_rpc.proof_store.resolve.via-probe` and all)
//! is lost with it, so a scenario that reads the call's trace id from that file finds none.

use rafka_mesh_entity::NodeId;
use std::io::{Read, Write};
use std::net::TcpListener;

/// A one-route admin: every request is answered with the same node view.
fn serve_view(view: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{view}", view.len());
        }
    });
    base
}

/// CONTRACT: a probe asked to carry a call through a resolvable carrier to an exact node its view
/// does not hold prints `NotSent` with the `via-peer` route, exits without the iroh "dropped
/// without calling `Endpoint::close`" abort, and leaves its `via-probe` span in the evidence
/// directory. What must NOT happen: an empty span file, or the ungraceful-abort line on stderr.
#[test]
fn probe_with_unresolved_target_via_carrier_leaves_its_span() {
    let carrier = NodeId::mint().to_string();
    let key = iroh::SecretKey::generate().public().to_string();
    let view = serde_json::json!({"nodes": [{
        "node_id": carrier, "name": "mesh1.gateway.1", "endpoint_id": key, "transport_addr": "127.0.0.1:9", "incarnation_id": "c0ffee00c0ffee00c0ffee00c0ffee00",
    }]})
    .to_string();
    let admin = serve_view(view);
    let evidence = std::env::temp_dir().join(format!("rdm-probe-close-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&evidence);
    let ghost = NodeId::mint().to_string();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-rpc-probe"))
        .args(["--admin", &admin, "put", "--target", &format!("exact:{ghost}"), "--key", "k", "--value", "v", "--via", "path:mesh1.gateway.1"])
        .env("RDM_EVIDENCE_DIR", &evidence)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| panic!("the probe printed no JSON ({e}): {stdout} / {stderr}"));
    assert_eq!((v["outcome"].as_str(), v["route"].as_str()), (Some("NotSent"), Some("via-peer")), "{v}");
    assert!(!stderr.contains("dropped without calling"), "the probe's endpoint was dropped open: {stderr}");
    let spans: String = std::fs::read_dir(&evidence)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("rafka-rpc-probe."))
        .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
        .collect();
    assert!(spans.contains("rdm.node_rpc.proof_store.resolve.via-probe"), "the probe left no via-probe span; evidence dir holds: {spans:?}");
    let _ = std::fs::remove_dir_all(&evidence);
}

/// CONTRACT: a probe asked to carry a call through a carrier its view does not hold (the carrier left
/// the fabric while the call was being aimed at it) sends nothing and says so as the client's own
/// `NotSent(Resolve(..))` on the `via-peer` route, exactly as it does for a target the view does not
/// hold; it closes its socket and leaves its `via-probe` span. What must NOT happen: a `Refused` line
/// with no typed outcome, an exit status 2, the ungraceful-abort line, or a lost span file.
#[test]
fn probe_with_unresolved_carrier_sends_nothing_and_leaves_its_span() {
    let present = NodeId::mint().to_string();
    let key = iroh::SecretKey::generate().public().to_string();
    let view = serde_json::json!({"nodes": [{
        "node_id": present, "name": "mesh1.broker.1", "endpoint_id": key, "transport_addr": "127.0.0.1:9", "incarnation_id": "c0ffee00c0ffee00c0ffee00c0ffee00",
    }]})
    .to_string();
    let admin = serve_view(view);
    let evidence = std::env::temp_dir().join(format!("rdm-probe-close-carrier-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&evidence);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-rpc-probe"))
        .args(["--admin", &admin, "put", "--target", &format!("exact:{present}"), "--key", "k", "--value", "v", "--via", "path:mesh1.gateway.9"])
        .env("RAFKA_EVIDENCE_DIR", &evidence)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| panic!("the probe printed no JSON ({e}): {stdout} / {stderr}"));
    assert_eq!((v["outcome"].as_str(), v["route"].as_str()), (Some("NotSent"), Some("via-peer")), "{v}");
    assert!(v["reason"].as_str().is_some_and(|r| r.contains("Resolve")), "the NotSent names the resolve failure: {v}");
    assert!(out.status.success(), "a call that sends nothing is a typed outcome, not a probe failure: {:?}", out.status);
    assert!(!stderr.contains("dropped without calling"), "the probe's endpoint was dropped open: {stderr}");
    let spans: String = std::fs::read_dir(&evidence)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("rafka-rpc-probe."))
        .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
        .collect();
    assert!(spans.contains("rdm.node_rpc.proof_store.resolve.via-probe"), "the probe left no via-probe span; evidence dir holds: {spans:?}");
    let _ = std::fs::remove_dir_all(&evidence);
}
