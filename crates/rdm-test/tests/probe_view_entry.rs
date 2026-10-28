//! Functional: the probe refuses a node of the admin's view it cannot read, by name.
//!
//! The probe dials from the entry admin's view, read over Node RPC (`fabric.get`, `node.get`). A reader
//! that does not recognise a value refuses with a named reason and never falls through (rafka-v2
//! CLAUDE.md, "DURABLE", second-order rule): a node whose endpoint is no Iroh key would otherwise
//! vanish from the resolver, so the call aimed at it would come back as a resolve failure that named
//! nothing, and a fixture with a bad birth would become a stem that waited forever for a dial. The view's
//! ids and incarnations are typed on the wire, so the endpoint, carried as text, is the field a view
//! node can hold unreadable.

use crate::common::admin_view::{node, serve_on_own_runtime};
use rdm_core::{IncarnationId, NodeId};

/// CONTRACT: a probe whose admin view holds a node with an `endpoint_id` that is not an Iroh key
/// refuses the whole call: exit status 2, one `Refused` line whose reason names that node's id, the
/// field `endpoint_id`, the value it held and the parse error the key type gives, and the probe's
/// `via-probe` span carries the same refusal, or its exit ended its evidence flush at its bound with the loss
/// named whole (i146 Q42, Q60). The probe closes its endpoint before it refuses (it was
/// bound to read the view). What must NOT happen: a `NotSent`/`Resolve` outcome, exit status 0, a
/// refusal that makes the reader open the view to learn which node failed, or the ungraceful-abort line.
#[test]
fn probe_with_unreadable_endpoint_refuses_naming_the_node() {
    let bad_node = NodeId::mint();
    let bad_endpoint = "not-an-iroh-key";
    let parse_error = bad_endpoint.parse::<iroh::PublicKey>().unwrap_err().to_string();
    let (rt, admin) = serve_on_own_runtime(vec![node("mesh1.rpc.1", &bad_node, Some(bad_endpoint.into()), Some("127.0.0.1:9".parse().unwrap()), Some(IncarnationId::mint()))]);
    let evidence = std::env::temp_dir().join(format!("rdm-probe-view-entry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&evidence);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rdm-rpc-probe"))
        .args(["--admin-birth", &admin.birth_arg(), "get", "--target", &format!("exact:{bad_node}"), "--key", "k"])
        .env("RDM_EVIDENCE_DIR", &evidence)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| panic!("the probe printed no JSON ({e}): {stdout} / {stderr}"));
    assert_eq!(v["outcome"].as_str(), Some("Refused"), "an unreadable view node refuses the call: {v}");
    assert_eq!(out.status.code(), Some(2), "a refused call exits non-zero: {v}");
    let reason = v["reason"].as_str().unwrap_or_default();
    for part in [bad_node.to_string().as_str(), "endpoint_id", bad_endpoint, parse_error.as_str()] {
        assert!(reason.contains(part), "the refusal names {part:?}: {reason:?}");
    }
    assert!(!stderr.contains("dropped without calling"), "the probe's endpoint was dropped open: {stderr}");
    let Some(probe) = crate::common::probe_root_span_or_named_loss(&evidence, &stderr) else {
        // The exit's evidence flush ended at its bound, the loss named whole (i146 Q42): the printed refusal above is the proof.
        rt.block_on(admin.shutdown());
        let _ = std::fs::remove_dir_all(&evidence);
        return;
    };
    let refused = probe["events"].as_array().into_iter().flatten().find(|e| e["name"] == "the probe refused the call");
    assert_eq!(refused.map(|e| e["attributes"]["refusal"].as_str()), Some(Some(reason)), "the via-probe span carries the refusal the probe printed: {probe}");
    eprintln!("probe_view_entry trace_id={} span={} refusal={reason}", probe["trace_id"].as_str().unwrap_or_default(), probe["name"].as_str().unwrap_or_default());
    rt.block_on(admin.shutdown());
    let _ = std::fs::remove_dir_all(&evidence);
}
