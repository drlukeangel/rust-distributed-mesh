//! Functional: the probe refuses a node-view entry it cannot read, by name.
//!
//! The probe dials from the admin's node view (`GET /api/nodes`). A reader that does not recognise a
//! value refuses with a named reason and never falls through (rafka-v2 CLAUDE.md, "DURABLE", second-order
//! rule): an entry whose `incarnation_id` is not a Crockford id used to vanish from the resolver, so the
//! call aimed at it came back as a resolve failure that named nothing, and a fixture with a bad birth
//! became a stem that waited forever for a dial.

use crate::probe_endpoint_close::serve_view;
use rafka_mesh_entity::{IncarnationId, NodeId};

/// CONTRACT: a probe whose admin view holds an entry with an `incarnation_id` that is not a Crockford id
/// refuses the whole call: exit status 2, one `Refused` line whose reason names that entry's node id, the
/// field `incarnation_id`, the value it held and the parse error the id type gives, and the probe's
/// `via-probe` span carries the same refusal. What must NOT happen: a `NotSent`/`Resolve` outcome, exit
/// status 0, or a refusal that makes the reader open the view to learn which entry failed.
#[test]
fn probe_with_unreadable_incarnation_refuses_naming_the_entry() {
    let bad_node = NodeId::mint().to_string();
    let bad_birth = "deadbeef01234567";
    let parse_error = IncarnationId::parse(bad_birth).unwrap_err().to_string();
    let key = iroh::SecretKey::generate().public().to_string();
    let view = serde_json::json!({"nodes": [{
        "node_id": bad_node, "name": "mesh1.rpc.1", "endpoint_id": key, "transport_addr": "127.0.0.1:9", "incarnation_id": bad_birth,
    }]})
    .to_string();
    let admin = serve_view(view);
    let evidence = std::env::temp_dir().join(format!("rdm-probe-view-entry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&evidence);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-rpc-probe"))
        .args(["--admin", &admin, "get", "--target", &format!("exact:{bad_node}"), "--key", "k"])
        .env("RDM_EVIDENCE_DIR", &evidence)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| panic!("the probe printed no JSON ({e}): {stdout} / {stderr}"));
    assert_eq!(v["outcome"].as_str(), Some("Refused"), "an unreadable view entry refuses the call: {v}");
    assert_eq!(out.status.code(), Some(2), "a refused call exits non-zero: {v}");
    let reason = v["reason"].as_str().unwrap_or_default();
    for part in [bad_node.as_str(), "incarnation_id", bad_birth, parse_error.as_str()] {
        assert!(reason.contains(part), "the refusal names {part:?}: {reason:?}");
    }
    let spans: String = std::fs::read_dir(&evidence)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("rafka-rpc-probe."))
        .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
        .collect();
    assert!(spans.contains("rdm.node_rpc.proof_store.resolve.via-probe") && spans.contains(bad_node.as_str()) && spans.contains("incarnation_id"), "the via-probe span carries the refusal; evidence dir holds: {spans:?}");
    let _ = std::fs::remove_dir_all(&evidence);
}
