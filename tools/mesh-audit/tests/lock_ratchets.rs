//! #2927 acceptance: every lock ratchet is an executable check, a planted violation fails
//! exactly that ratchet, and the RDM tree is green under all nineteen.

use rafka_mesh_audit::lock::{check, check_one, Ratchet, Violation};
use rafka_mesh_audit::workspace_root;
use std::path::{Path, PathBuf};

/// A copy of the RDM sources the ratchets read, with one planted edit: the tree is green
/// before the edit, so a red after it belongs to the edit alone.
struct Planted(PathBuf);

impl Planted {
    fn of_tree() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!("lock-ratchets-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&dir);
        let root = workspace_root();
        for scope in ["crates", "tools/mesh-audit/src", "demo"] {
            copy_rs(&root.join(scope), &dir.join(scope));
        }
        Self(dir)
    }
    fn root(&self) -> &Path {
        &self.0
    }
    fn write(&self, rel: &str, text: &str) {
        let p = self.0.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, text).unwrap();
    }
    fn edit(&self, rel: &str, from: &str, to: &str) {
        let p = self.0.join(rel);
        let s = std::fs::read_to_string(&p).unwrap();
        assert_eq!(s.matches(from).count(), 1, "{rel}: `{from}` is not one site");
        std::fs::write(p, s.replace(from, to)).unwrap();
    }
    fn delete(&self, rel: &str) {
        std::fs::remove_file(self.0.join(rel)).unwrap();
    }
}

impl Drop for Planted {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn copy_rs(from: &Path, to: &Path) {
    let Ok(rd) = std::fs::read_dir(from) else { return };
    for e in rd.flatten() {
        let p = e.path();
        let name = e.file_name();
        if p.is_dir() {
            if name != "target" && name != ".git" {
                copy_rs(&p, &to.join(name));
            }
        } else if p.extension().is_some_and(|x| x == "rs") {
            std::fs::create_dir_all(to).unwrap();
            std::fs::copy(&p, to.join(name)).unwrap();
        }
    }
}

fn only(found: Vec<Violation>, ratchet: Ratchet) -> Vec<Violation> {
    assert!(!found.is_empty(), "{}: the planted violation was not found", ratchet.name());
    assert!(found.iter().all(|v| v.ratchet() == ratchet), "{}: a planted violation reached other ratchets: {found:?}", ratchet.name());
    found
}

#[test]
fn the_rdm_tree_is_green_under_every_ratchet() {
    let found = check(&workspace_root());
    assert!(found.is_empty(), "{}", found.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("\n"));
}

#[test]
fn every_ratchet_has_a_name_and_runs() {
    let root = workspace_root();
    for r in Ratchet::ALL {
        assert!(!r.name().is_empty());
        assert!(check_one(&root, r).is_empty(), "{} is red on the tree", r.name());
    }
}

#[test]
fn tag_is_descriptive_only_fails_a_runtime_read_of_a_descriptive_map() {
    let t = Planted::of_tree();
    t.write("crates/rdm-admin/src/planted.rs", "fn decide(d: &MeshDigest) -> bool { d.extra.get(\"in_flight\").is_some() }\n");
    let v = only(check(t.root()), Ratchet::TagIsDescriptiveOnly);
    assert!(matches!(&v[0], Violation::Token { file, token, .. } if file.ends_with("planted.rs") && token == ".extra.get("));
}

#[test]
fn resource_behavior_is_typed_meta_fails_a_behavior_boolean_outside_ingress() {
    let t = Planted::of_tree();
    t.write("crates/rdm-admin/src/planted.rs", "pub struct Launch { pub stateful: bool }\n");
    only(check(t.root()), Ratchet::ResourceBehaviorIsTypedMeta);
    // The ingress normalization input is the one place a legacy intent may be named.
    let clean = Planted::of_tree();
    assert!(check_one(clean.root(), Ratchet::ResourceBehaviorIsTypedMeta).is_empty());
}

#[test]
fn accepted_build_has_explicit_node_spec_fails_when_the_build_loses_its_create_spec_or_its_proof() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-admin/src/accepted.rs", "fn every_materialized_path_carries_exactly_one_create_spec", "fn every_materialized_path_carries_a_name");
    only(check(t.root()), Ratchet::AcceptedBuildHasExplicitNodeSpec);
}

#[test]
fn legacy_defaults_are_ingress_only_fails_a_default_applied_in_the_pipeline() {
    let t = Planted::of_tree();
    t.write("crates/rdm-admin/src/deployment/planted.rs", "fn storage(kind: NodeKind) -> StorageMeta { NodeSpec::default_for(kind).storage }\n");
    only(check(t.root()), Ratchet::LegacyDefaultsAreIngressOnly);
}

#[test]
fn provider_interprets_neither_meta_nor_tags_fails_a_provider_reading_meta() {
    let t = Planted::of_tree();
    t.write("crates/rdm-admin/src/deployment/process.rs", "fn release(meta: &StorageMeta) {}\n");
    only(check(t.root()), Ratchet::ProviderInterpretsNeitherMetaNorTags);
}

#[test]
fn no_transport_tag_vocabulary_fails_a_tag_owner_in_an_active_source() {
    let t = Planted::of_tree();
    // The deleted name is assembled here so this file never carries it as a token.
    let deleted = format!("{}Owner", "Tag");
    t.write("crates/rdm-transport/src/planted.rs", &format!("use rdm_core::catalog::{deleted};\n"));
    let v = only(check(t.root()), Ratchet::NoTransportTagVocabulary);
    assert!(matches!(&v[0], Violation::Token { token, .. } if *token == deleted));
    // A serde discriminator named `tag` is JSON vocabulary, not transport vocabulary.
    let s = Planted::of_tree();
    s.write("crates/rdm-transport/src/planted.rs", "#[serde(tag = \"kind\")]\npub enum A { B }\n");
    assert!(check_one(s.root(), Ratchet::NoTransportTagVocabulary).is_empty());
}

#[test]
fn node_delete_attempts_rpc_stop_first_fails_when_termination_precedes_the_stop() {
    let t = Planted::of_tree();
    t.edit(
        "crates/rdm-admin/src/deployment/pipeline.rs",
        "        Self::NodeDeleting,\n        Self::StopNode,",
        "        Self::NodeDeleting,",
    );
    t.edit("crates/rdm-admin/src/deployment/pipeline.rs", "        Self::TerminateRuntime,", "        Self::TerminateRuntime,\n        Self::StopNode,");
    only(check(t.root()), Ratchet::DeleteAttemptsRpcStopFirst);
}

#[test]
fn node_delete_has_bounded_failure_arms_fails_the_older_four_arm_draft() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-admin/src/deployment/pipeline.rs", "    /// The birth, or the op, refused the call by name (a stale incarnation, an unserved op).\n    Refused {\n        /// The refusal's name.\n        reply: String,\n        /// Standard code of the original typed command outcome.\n        #[serde(default = \"rdm_core::outcome::legacy_failed_code\")]\n        code: rdm_core::outcome::Code,\n    },\n", "");
    let v = only(check(t.root()), Ratchet::DeleteHasBoundedFailureArms);
    assert!(matches!(&v[0], Violation::Arms { found, .. } if !found.iter().any(|a| a == "Refused")));
}

#[test]
fn drift_silence_never_deletes_a_node_fails_without_its_proof() {
    let t = Planted::of_tree();
    t.edit(
        "crates/rdm-admin/tests/drift_convergence.rs",
        "fn a_silent_node_whose_runtime_runs_opens_no_attempt_and_is_never_replaced",
        "fn a_silent_node_is_replaced",
    );
    only(check(t.root()), Ratchet::DriftSilenceIsNotDeleteProof);
}

#[test]
fn node_delete_receipt_names_command_arm_fails_an_untagged_outcome() {
    let t = Planted::of_tree();
    t.edit(
        "crates/rdm-admin/src/deployment/pipeline.rs",
        "#[serde(tag = \"arm\", rename_all = \"kebab-case\", deny_unknown_fields)]\npub enum CommandAdmission",
        "#[serde(rename_all = \"kebab-case\", deny_unknown_fields)]\npub enum CommandAdmission",
    );
    only(check(t.root()), Ratchet::DeleteReceiptNamesCommandArm);
}

#[test]
fn connections_deleted_is_not_refusal_fails_when_the_receipt_is_gone() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/status.rs", "        receipt: Option<ConnectionsReceipt>,\n", "        receipt: Option<u64>,\n");
    only(check(t.root()), Ratchet::ConnectionsDeletedIsNotRefusal);
}

#[test]
fn forward_preserves_context_fails_without_its_proof() {
    let t = Planted::of_tree();
    t.delete("crates/rdm-transport/tests/context.rs");
    only(check(t.root()), Ratchet::ForwardPreservesContext);
}

#[test]
fn postcard_section_bounds_fails_when_the_declared_length_is_no_longer_refused_first() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/framing.rs", "fn oversize_is_refused_from_the_declared_length_before_the_body", "fn oversize_is_refused");
    only(check(t.root()), Ratchet::PostcardSectionBounds);
}

#[test]
fn typed_protocol_identity_fails_a_string_id_in_a_request() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/status.rs", "/// The birth probed.\n        node_id: NodeId,", "/// The birth probed.\n        node_id: String,");
    let v = only(check(t.root()), Ratchet::TypedProtocolIdentity);
    assert!(matches!(&v[0], Violation::Token { token, .. } if token == "node_id: String"));
}

#[test]
fn typed_protocol_evidence_fails_untyped_held_evidence() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/status.rs", "/// The mesh id held.\n        held: MeshId,", "/// The mesh id held.\n        held: String,");
    only(check(t.root()), Ratchet::TypedProtocolEvidence);
}

#[test]
fn status_wire_is_atomic_fails_a_moved_count_or_a_lost_fixture() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/status.rs", "const REPLY_VARIANTS: u32 = 63;", "const REPLY_VARIANTS: u32 = 64;");
    only(check(t.root()), Ratchet::StatusWireIsAtomic);
    let f = Planted::of_tree();
    f.delete("crates/rdm-core/tests/status_wire.rs");
    only(check(f.root()), Ratchet::StatusWireIsAtomic);
}

#[test]
fn forward_wire_is_atomic_fails_a_moved_count_or_a_lost_fixture() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/forward.rs", "const REPLY_VARIANTS: u32 = 14;", "const REPLY_VARIANTS: u32 = 15;");
    only(check(t.root()), Ratchet::ForwardWireIsAtomic);
    let f = Planted::of_tree();
    f.delete("crates/rdm-core/tests/forward_wire.rs");
    only(check(f.root()), Ratchet::ForwardWireIsAtomic);
}

#[test]
fn node_command_wire_is_atomic_fails_a_moved_count_or_a_lost_fixture() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/node_command.rs", "const REPLY_VARIANTS: u32 = 23;", "const REPLY_VARIANTS: u32 = 24;");
    only(check(t.root()), Ratchet::NodeCommandWireIsAtomic);
    let f = Planted::of_tree();
    f.delete("crates/rdm-core/tests/node_command_wire.rs");
    only(check(f.root()), Ratchet::NodeCommandWireIsAtomic);
}

#[test]
fn transport_header_wire_is_atomic_fails_a_moved_golden_or_a_moved_bound() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/framing.rs", "(47, 88, 37)", "(47, 88, 38)");
    only(check(t.root()), Ratchet::TransportHeaderWireIsAtomic);
    let g = Planted::of_tree();
    g.edit("crates/rdm-core/src/framing.rs", r#"format!("3901{BIRTH}0201{BIRTH}0100")"#, r#"format!("3901{BIRTH}0201{BIRTH}0101")"#);
    only(check(g.root()), Ratchet::TransportHeaderWireIsAtomic);
}

#[test]
fn one_new_liveness_primitive_fails_a_served_echo_or_an_unreserved_echo_row() {
    let t = Planted::of_tree();
    t.write("crates/rdm-test/src/planted.rs", "fn serve(b: ServerBuilder) -> ServerBuilder { b.serve::<Echo, _, _>(OpOwner::Core, echo) }\n");
    only(check(t.root()), Ratchet::OneNewLivenessPrimitive);
    let r = Planted::of_tree();
    r.edit("crates/rdm-core/src/catalog.rs", "family: \"echo\".into(), owner: OpOwner::Core, state: OpState::Reserved", "family: \"echo\".into(), owner: OpOwner::Core, state: OpState::Live");
    only(check(r.root()), Ratchet::OneNewLivenessPrimitive);
}


#[test]
fn rdm_runtime_emits_customer_span_rejected_by_ownership_ratchet() {
    let t = Planted::of_tree();
    assert!(check(t.root()).is_empty(), "the unmodified copied tree passes");
    t.write("crates/rdm-transport/src/planted.rs", "fn emit() { tracing::info_span!(\"rafka.node_rpc.request.serve.via-direct\"); }\n");
    let found = only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
    assert!(matches!(&found[0], Violation::Token { file, line: 1, token, .. }
        if file.ends_with("planted.rs") && token == "rafka.node_rpc.request.serve.via-direct"));
}

#[test]
fn ownership_scan_reads_emitters_preserves_customer_harness_and_fixture_prose() {
    let t = Planted::of_tree();
    t.write("crates/rdm-transport/src/planted.rs", r#"
// tracing::info_span!("rafka.node_rpc.fixture.serve.via-comment");
const FIXTURE: &str = "rafka.node_rpc.request.serve.via-direct";
#[cfg(test)] mod tests { fn fixture() { tracing::info_span!("rafka.mesh.fixture.serve.via-test"); } }
fn emit() { tracing::info_span!("rdm.node_rpc.request.serve.via-direct"); }
"#);
    t.write("crates/rdm-test/src/planted.rs", "fn emit() { tracing::info_span!(\"rafka.chaos.fault.create.via-injection\"); }\n");
    assert!(check_one(t.root(), Ratchet::RdmSpansAreRdmPrefixed).is_empty());
    t.write("crates/rdm-transport/src/planted.rs", "fn emit() { tracing::span!(tracing::Level::INFO, \"rafka.chaos.fault.create.via-injection\"); }\n");
    only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
}


#[test]
fn rdm_macro_emits_customer_span_rejected_by_ownership_ratchet() {
    let t = Planted::of_tree();
    t.write("crates/rdm-transport/src/planted.rs", r#"
macro_rules! emit { () => { tracing::info_span!(concat!("rafka.", "node_rpc.request.serve.via-direct")); }; }
"#);
    only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
}


#[test]
fn rdm_instrument_emits_unnamed_span_rejected_by_ownership_ratchet() {
    let t = Planted::of_tree();
    t.write("crates/rdm-transport/src/planted.rs", "#[tracing::instrument] fn emit() {}\n");
    only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
}

#[test]
fn rdm_instrument_names_owner_accepts_native_target_and_rejects_customer_name() {
    let t = Planted::of_tree();
    t.write("crates/rdm-transport/src/planted.rs", r#"
fn emit() { tracing::info_span!(target: "rafka.customer-target", "rdm.node_rpc.request.serve.via-direct"); }
"#);
    assert!(check(t.root()).is_empty());
    t.write("crates/rdm-transport/src/planted.rs", "#[tracing::instrument(name = \"rafka.node_rpc.request.serve.via-direct\")] fn emit() {}\n");
    only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
}

#[test]
fn no_json_on_the_wire_fails_a_json_codec_in_a_gossip_frame_path() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-transport/src/topology.rs", "crate::envelope::encode(context, frame)", "serde_json::to_vec(self)");
    let v = only(check(t.root()), Ratchet::NoJsonOnTheWire);
    assert!(v.iter().any(|v| matches!(v, Violation::Token { file, token, .. } if file.ends_with("topology.rs") && token == "serde_json::to_vec")), "{v:?}");

    let envelope = Planted::of_tree();
    envelope.edit("crates/rdm-transport/src/envelope.rs", "wire::encode(&(context.attributes(dataschema)?, data))", "serde_json::to_vec(&(context.attributes(dataschema)?, data))");
    only(check(envelope.root()), Ratchet::NoJsonOnTheWire);

    // The Build topic too, and a direct postcard call beside the shared codec.
    let t = Planted::of_tree();
    t.edit("crates/rdm-admin/src/fabric_builds.rs", "rdm_transport::wire::decode(bytes)", "serde_json::from_slice(bytes).map_err(|e| rdm_transport::wire::WireError::new(e.to_string()))");
    only(check(t.root()), Ratchet::NoJsonOnTheWire);
    let t = Planted::of_tree();
    t.edit("crates/rdm-admin/src/fabric_builds.rs", "rdm_transport::wire::encode(&wire)", "postcard::to_allocvec(&wire).map_err(|e| rdm_transport::wire::WireError::new(e.to_string()))");
    only(check(t.root()), Ratchet::NoJsonOnTheWire);

    // A Node RPC source takes no JSON either.
    let t = Planted::of_tree();
    t.write("crates/rdm-core/src/planted.rs", "fn frame(v: &Req) -> Vec<u8> { serde_json::to_vec(v).unwrap() }\n");
    only(check(t.root()), Ratchet::NoJsonOnTheWire);
}

#[test]
fn no_json_on_the_wire_reads_only_non_test_code_and_holds_its_proof_cell() {
    // A test module may build a JSON expectation: the scan stops at `#[cfg(test)]`.
    let t = Planted::of_tree();
    let clean = std::fs::read_to_string(t.root().join("crates/rdm-transport/src/wire.rs")).unwrap();
    t.write("crates/rdm-transport/src/wire.rs", &format!("{clean}\n#[cfg(test)]\nmod tests {{ fn json() {{ let _ = serde_json::json!(1); }} }}\n"));
    assert!(check_one(t.root(), Ratchet::NoJsonOnTheWire).is_empty());

    // Deleting or renaming the proof cell is itself a violation.
    let t = Planted::of_tree();
    t.edit(
        "crates/rdm-admin/tests/gossip_frames_wire.rs",
        "fn every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling",
        "fn every_frame_round_trips",
    );
    only(check(t.root()), Ratchet::NoJsonOnTheWire);
}

/// CONTRACT: a span guard bound with `enter()` or `entered()` and still alive at an `.await` of
/// its block is refused in non-test code, including code below an item-level `#[cfg(test)]`; a
/// guard released before the await, one that never meets an await, a discarded `let _ =` and a
/// test module are not.
#[test]
fn no_span_guard_across_await_refuses_a_guard_alive_at_an_await() {
    let t = Planted::of_tree();
    t.write("crates/rdm-admin/src/planted.rs", "async fn f(span: tracing::Span) {\n    let _g = span.enter();\n    work().await;\n}\n");
    let v = only(check(t.root()), Ratchet::NoSpanGuardAcrossAwait);
    assert!(matches!(&v[0], Violation::Token { file, line: 2, token, .. } if file.ends_with("planted.rs") && token.contains("held across the .await at line 3")), "{v:?}");
    t.write("crates/rdm-admin/src/planted.rs", "async fn f(span: tracing::Span) {\n    let g = span.clone().entered();\n    if ok() {\n        step();\n    }\n    other().await;\n}\n");
    only(check(t.root()), Ratchet::NoSpanGuardAcrossAwait);    // An item-level `#[cfg(test)]` gates its one item: the guard below it is still read.
    t.write("crates/rdm-admin/src/planted.rs", "#[cfg(test)]\nfn helper() {}\n\nasync fn f(span: tracing::Span) {\n    let _g = span.enter();\n    work().await;\n}\n");
    let v = only(check(t.root()), Ratchet::NoSpanGuardAcrossAwait);
    assert!(matches!(&v[0], Violation::Token { file, line: 5, .. } if file.ends_with("planted.rs")), "{v:?}");
}

#[test]
fn no_span_guard_across_await_accepts_scoped_dropped_discarded_and_test_guards() {
    let t = Planted::of_tree();
    t.write(
        "crates/rdm-admin/src/planted.rs",
        r#"
async fn scoped(span: tracing::Span) {
    {
        let _g = span.enter();
        step();
    }
    work().await;
}
async fn dropped(span: tracing::Span) {
    let g = span.enter();
    step();
    drop(g);
    work().await;
}
async fn discarded(span: tracing::Span) {
    let _ = span.enter();
    work().await;
}
async fn instrumented(span: tracing::Span) {
    use tracing::Instrument;
    work().instrument(span).await;
}
#[cfg(test)]
mod tests {
    async fn held(span: tracing::Span) {
        let _g = span.enter();
        work().await;
    }
}
"#,
    );
    assert!(check_one(t.root(), Ratchet::NoSpanGuardAcrossAwait).is_empty());
}

/// CONTRACT: the rename ratchet reads non-test code only, by the one rule every non-test scan
/// shares: a `#[cfg(test)]` item is skipped and nothing else. A test double's forward of
/// `.release_node_name(` inside a `#[cfg(test)]` module is not a rename door; a real door below an
/// item-level `#[cfg(test)]` (a gated helper, not the test module) is still read and refused.
#[test]
fn one_rename_write_path_reads_only_non_test_code() {
    let t = Planted::of_tree();
    t.write(
        "crates/rdm-admin/src/planted.rs",
        "pub fn real() {}\n\n#[cfg(test)]\nmod tests {\n    struct Double(MemoryNodesStorage);\n    impl Double {\n        async fn release_node_name(&self, n: &PathName, by: &NodeId) -> bool {\n            self.0.release_node_name(n, by).await\n        }\n    }\n}\n",
    );
    assert!(check_one(t.root(), Ratchet::OneRenameWritePath).is_empty(), "{:?}", check_one(t.root(), Ratchet::OneRenameWritePath));

    t.write(
        "crates/rdm-admin/src/planted.rs",
        "#[cfg(test)]\npub(crate) fn tempdir() -> u8 {\n    0\n}\n\nasync fn take(s: &dyn NodesStorage, n: &PathName, by: &NodeId) {\n    let _ = s.release_node_name(n, by).await;\n}\n",
    );
    let v = only(check(t.root()), Ratchet::OneRenameWritePath);
    assert!(matches!(&v[0], Violation::Token { file, line: 7, token, .. } if file.ends_with("planted.rs") && token == ".release_node_name("), "{v:?}");
}

#[test]
fn one_rename_write_path_fails_a_release_outside_move_row_or_a_second_rename_door() {
    let t = Planted::of_tree();
    t.write("crates/rdm-admin/src/planted.rs", "async fn take(s: &dyn NodesStorage, n: &PathName, by: &NodeId) { let _ = s.release_node_name(n, by).await; }\n");
    let v = only(check(t.root()), Ratchet::OneRenameWritePath);
    assert!(matches!(&v[0], Violation::Token { file, token, .. } if file.ends_with("planted.rs") && token == ".release_node_name("));
    let d = Planted::of_tree();
    d.edit("crates/rdm-admin/src/node_rows.rs", "pub async fn move_row(storage: &dyn NodesStorage,", "pub async fn rename(storage: &dyn NodesStorage,");
    only(check(d.root()), Ratchet::OneRenameWritePath);
}

#[test]
fn build_wire_refuses_a_moved_position_or_a_lost_golden() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/build.rs", "const REPLY_VARIANTS: u32 = 30;", "const REPLY_VARIANTS: u32 = 31;");
    only(check(t.root()), Ratchet::BuildWireIsAtomic);
    let f = Planted::of_tree();
    f.delete("crates/rdm-core/tests/build_wire.rs");
    only(check(f.root()), Ratchet::BuildWireIsAtomic);
}

#[test]
fn store_layout_wire_refuses_a_moved_format_a_moved_key_or_a_lost_golden() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-node/src/store_root.rs", "pub const LAYOUT_VALUE: &str = \"r-sr1\";", "pub const LAYOUT_VALUE: &str = \"r-sr2\";");
    only(check(t.root()), Ratchet::StoreLayoutWireIsAtomic);
    let l = Planted::of_tree();
    l.edit("crates/rdm-node/src/store_log/tests.rs", "fn a_line_states_its_own_length_first_and_its_checksum_last", "fn changed_line_cell");
    only(check(l.root()), Ratchet::StoreLayoutWireIsAtomic);
    let k = Planted::of_tree();
    k.edit("crates/rdm-admin/src/build_state.rs", "BuildFact::Accepted(_) => \"accepted\".to_string(),", "BuildFact::Accepted(_) => \"acceptance\".to_string(),");
    only(check(k.root()), Ratchet::StoreLayoutWireIsAtomic);
    let f = Planted::of_tree();
    f.edit("crates/rdm-admin/src/build_state.rs", "fn the_store_layout_is_frozen_byte_for_byte", "fn changed_store_layout_cell");
    only(check(f.root()), Ratchet::StoreLayoutWireIsAtomic);
}

#[test]
fn handover_wire_refuses_a_moved_position_or_a_lost_golden() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-core/src/handover.rs", "const REPLY_VARIANTS: u32 = 13;", "const REPLY_VARIANTS: u32 = 14;");
    only(check(t.root()), Ratchet::PrimaryUpdateWireIsAtomic);
    let f = Planted::of_tree();
    f.edit("crates/rdm-core/tests/handover_wire.rs", "fn failed_primary_update_retains_the_typed_build_code_in_its_appended_position", "fn changed_failed_primary_update_cell");
    only(check(f.root()), Ratchet::PrimaryUpdateWireIsAtomic);
}

#[test]
fn gossip_envelope_wire_is_atomic_holds_context_goldens_and_receive_controls() {
    let t = Planted::of_tree();
    t.edit("crates/rdm-transport/src/envelope.rs", "pub traceparent: Option<String>", "pub lost_parent: Option<String>");
    only(check(t.root()), Ratchet::GossipEnvelopeWireIsAtomic);
    let g = Planted::of_tree();
    g.edit("crates/rdm-transport/tests/digest_frames_wire.rs", "fn the_traced_envelope_wire_matches_the_hand_golden_and_complete_ceiling", "fn missing_traced_golden");
    only(check(g.root()), Ratchet::GossipEnvelopeWireIsAtomic);
}
