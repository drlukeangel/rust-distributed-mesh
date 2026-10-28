//! Functional: the probe closes its one socket before it exits, whatever the call did.
//!
//! A `--via` call whose exact target the admin's view does not hold sends nothing: the probe's
//! endpoint is bound, reads the entry admin's view, and carries no call. An endpoint dropped open makes iroh abort the process
//! ungracefully, and the probe's span file (`rdm.node_rpc.proof_store.resolve.via-probe` and all)
//! is lost with it, so a scenario that reads the call's trace id from that file finds none.

use crate::common::admin_view::{node, serve_on_own_runtime};
use rdm_core::{IncarnationId, NodeId};

fn root_span_or_named_loss(evidence: &std::path::Path, stderr: &str) {
    let _ = crate::common::probe_root_span_or_named_loss(evidence, stderr);
}

/// CONTRACT: a probe asked to carry a call through a resolvable carrier to an exact node its view
/// does not hold prints `NotSent` with the `via-peer` route, exits without the iroh "dropped
/// without calling `Endpoint::close`" abort, and leaves its `via-probe` span in the evidence
/// directory, or ends its exit's evidence flush at its bound with the loss named whole (i146 Q42). What
/// must NOT happen: a span file without the root and no loss named, or the ungraceful-abort line on stderr.
#[test]
fn probe_with_unresolved_target_via_carrier_leaves_its_span() {
    let key = iroh::SecretKey::generate().public().to_string();
    let (rt, admin) = serve_on_own_runtime(vec![node("mesh1.gateway.1", &NodeId::mint(), Some(key), Some("127.0.0.1:9".parse().unwrap()), Some(IncarnationId::mint()))]);
    let evidence = std::env::temp_dir().join(format!("rdm-probe-close-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&evidence);
    let ghost = NodeId::mint().to_string();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rdm-rpc-probe"))
        .args(["--admin-birth", &admin.birth_arg(), "put", "--target", &format!("exact:{ghost}"), "--key", "k", "--value", "v", "--via", "path:mesh1.gateway.1"])
        .env("RDM_EVIDENCE_DIR", &evidence)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| panic!("the probe printed no JSON ({e}): {stdout} / {stderr}"));
    assert_eq!((v["outcome"].as_str(), v["route"].as_str()), (Some("NotSent"), Some("via-peer")), "{v}");
    assert!(!stderr.contains("dropped without calling"), "the probe's endpoint was dropped open: {stderr}");
    root_span_or_named_loss(&evidence, &stderr);
    rt.block_on(admin.shutdown());
    let _ = std::fs::remove_dir_all(&evidence);
}

/// CONTRACT: a probe asked to carry a call through a carrier its view does not hold (the carrier left
/// the fabric while the call was being aimed at it) sends nothing and says so as the client's own
/// `NotSent(Resolve(..))` on the `via-peer` route, exactly as it does for a target the view does not
/// hold; it closes its socket and leaves its `via-probe` span, or names its loss whole at its exit's
/// bound (i146 Q42). What must NOT happen: a `Refused` line with no typed outcome, an exit status 2, the
/// ungraceful-abort line, or a span file lost with no loss named.
#[test]
fn probe_with_unresolved_carrier_sends_nothing_and_leaves_its_span() {
    let present = NodeId::mint();
    let key = iroh::SecretKey::generate().public().to_string();
    let (rt, admin) = serve_on_own_runtime(vec![node("mesh1.broker.1", &present, Some(key), Some("127.0.0.1:9".parse().unwrap()), Some(IncarnationId::mint()))]);
    let evidence = std::env::temp_dir().join(format!("rdm-probe-close-carrier-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&evidence);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rdm-rpc-probe"))
        .args(["--admin-birth", &admin.birth_arg(), "put", "--target", &format!("exact:{present}"), "--key", "k", "--value", "v", "--via", "path:mesh1.gateway.9"])
        .env("RDM_EVIDENCE_DIR", &evidence)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| panic!("the probe printed no JSON ({e}): {stdout} / {stderr}"));
    assert_eq!((v["outcome"].as_str(), v["route"].as_str()), (Some("NotSent"), Some("via-peer")), "{v}");
    assert!(v["reason"].as_str().is_some_and(|r| r.contains("Resolve")), "the NotSent names the resolve failure: {v}");
    assert!(out.status.success(), "a call that sends nothing is a typed outcome, not a probe failure: {:?}", out.status);
    assert!(!stderr.contains("dropped without calling"), "the probe's endpoint was dropped open: {stderr}");
    root_span_or_named_loss(&evidence, &stderr);
    rt.block_on(admin.shutdown());
    let _ = std::fs::remove_dir_all(&evidence);
}
