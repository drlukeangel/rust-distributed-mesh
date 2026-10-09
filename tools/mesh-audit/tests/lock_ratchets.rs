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
    t.write("crates/rafka-node-admin-core/src/planted.rs", "fn decide(d: &MeshDigest) -> bool { d.extra.get(\"in_flight\").is_some() }\n");
    let v = only(check(t.root()), Ratchet::TagIsDescriptiveOnly);
    assert!(matches!(&v[0], Violation::Token { file, token, .. } if file.ends_with("planted.rs") && token == ".extra.get("));
}

#[test]
fn resource_behavior_is_typed_meta_fails_a_behavior_boolean_outside_ingress() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-admin-core/src/planted.rs", "pub struct Launch { pub stateful: bool }\n");
    only(check(t.root()), Ratchet::ResourceBehaviorIsTypedMeta);
    // The ingress normalization input is the one place a legacy intent may be named.
    let clean = Planted::of_tree();
    assert!(check_one(clean.root(), Ratchet::ResourceBehaviorIsTypedMeta).is_empty());
}

#[test]
fn accepted_build_has_explicit_node_meta_fails_when_the_build_loses_its_meta_or_its_proof() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-admin-core/src/accepted.rs", "fn every_materialized_path_carries_exactly_one_node_meta", "fn every_materialized_path_carries_a_name");
    only(check(t.root()), Ratchet::AcceptedBuildHasExplicitNodeMeta);
}

#[test]
fn legacy_defaults_are_ingress_only_fails_a_default_applied_in_the_pipeline() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-admin-core/src/deployment/planted.rs", "fn storage(kind: NodeKind) -> StorageMeta { NodeMeta::default_for(kind).storage }\n");
    only(check(t.root()), Ratchet::LegacyDefaultsAreIngressOnly);
}

#[test]
fn provider_interprets_neither_meta_nor_tags_fails_a_provider_reading_meta() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-admin-core/src/deployment/process.rs", "fn release(meta: &StorageMeta) {}\n");
    only(check(t.root()), Ratchet::ProviderInterpretsNeitherMetaNorTags);
}

#[test]
fn no_transport_tag_vocabulary_fails_a_tag_owner_in_an_active_source() {
    let t = Planted::of_tree();
    // The retired name is assembled here so this file never carries it as a token.
    let retired = format!("{}Owner", "Tag");
    t.write("crates/rafka-node-rpc/src/planted.rs", &format!("use rafka_node_rpc_contract::catalog::{retired};\n"));
    let v = only(check(t.root()), Ratchet::NoTransportTagVocabulary);
    assert!(matches!(&v[0], Violation::Token { token, .. } if *token == retired));
    // A serde discriminator named `tag` is JSON vocabulary, not transport vocabulary.
    let s = Planted::of_tree();
    s.write("crates/rafka-node-rpc/src/planted.rs", "#[serde(tag = \"kind\")]\npub enum A { B }\n");
    assert!(check_one(s.root(), Ratchet::NoTransportTagVocabulary).is_empty());
}

#[test]
fn retire_attempts_rpc_drain_first_fails_when_termination_precedes_the_drain() {
    let t = Planted::of_tree();
    t.edit(
        "crates/rafka-node-admin-core/src/deployment/pipeline.rs",
        "        Self::DrainNode,\n        Self::AwaitNodeDrained,",
        "        Self::AwaitNodeDrained,",
    );
    t.edit("crates/rafka-node-admin-core/src/deployment/pipeline.rs", "        Self::TerminateRuntime,", "        Self::TerminateRuntime,\n        Self::DrainNode,");
    only(check(t.root()), Ratchet::RetireAttemptsRpcDrainFirst);
}

#[test]
fn retire_has_bounded_failure_arms_fails_the_older_four_arm_draft() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-admin-core/src/deployment/pipeline.rs", "    /// The birth, or the op fence, refused the call by name (a stale incarnation, an unserved op).\n    Refused {\n        /// The refusal's name.\n        reply: String,\n    },\n", "");
    let v = only(check(t.root()), Ratchet::RetireHasBoundedFailureArms);
    assert!(matches!(&v[0], Violation::Arms { found, .. } if !found.iter().any(|a| a == "Refused")));
}

#[test]
fn drift_silence_is_not_retire_authority_fails_without_its_proof() {
    let t = Planted::of_tree();
    t.edit(
        "crates/rafka-node-admin-core/tests/drift_convergence.rs",
        "fn a_silent_node_whose_runtime_runs_opens_no_attempt_and_is_never_replaced",
        "fn a_silent_node_is_replaced",
    );
    only(check(t.root()), Ratchet::DriftSilenceIsNotRetireAuthority);
}

#[test]
fn retire_receipt_names_drain_arm_fails_an_untagged_outcome() {
    let t = Planted::of_tree();
    t.edit(
        "crates/rafka-node-admin-core/src/deployment/pipeline.rs",
        "#[serde(tag = \"arm\", rename_all = \"kebab-case\")]\npub enum CommandAdmission",
        "#[serde(rename_all = \"kebab-case\")]\npub enum CommandAdmission",
    );
    only(check(t.root()), Ratchet::RetireReceiptNamesDrainArm);
}

#[test]
fn drain_success_is_not_refusal_fails_when_the_applied_reply_is_gone() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-rpc-contract/src/status.rs", "NodeDrainingApplied {\n        /// The work still in flight.\n        in_flight: u64,\n    }", "NodeDrainingApplied");
    only(check(t.root()), Ratchet::DrainSuccessIsNotRefusal);
}

#[test]
fn forward_preserves_context_fails_without_its_proof() {
    let t = Planted::of_tree();
    t.delete("crates/rafka-node-rpc/tests/context.rs");
    only(check(t.root()), Ratchet::ForwardPreservesContext);
}

#[test]
fn postcard_section_bounds_fails_when_the_declared_length_is_no_longer_refused_first() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-rpc-contract/src/framing.rs", "fn oversize_is_refused_from_the_declared_length_before_the_body", "fn oversize_is_refused");
    only(check(t.root()), Ratchet::PostcardSectionBounds);
}

#[test]
fn typed_protocol_identity_fails_a_string_id_in_a_request() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-rpc-contract/src/status.rs", "/// The birth probed.\n        node_id: NodeId,", "/// The birth probed.\n        node_id: String,");
    let v = only(check(t.root()), Ratchet::TypedProtocolIdentity);
    assert!(matches!(&v[0], Violation::Token { token, .. } if token == "node_id: String"));
}

#[test]
fn typed_protocol_evidence_fails_untyped_held_evidence() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-rpc-contract/src/status.rs", "/// The mesh id held.\n        held: MeshId,", "/// The mesh id held.\n        held: String,");
    only(check(t.root()), Ratchet::TypedProtocolEvidence);
}

#[test]
fn status_reseal_is_atomic_fails_a_moved_count_or_a_lost_fixture() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-rpc-contract/src/status.rs", "const REPLY_VARIANTS: u32 = 16;", "const REPLY_VARIANTS: u32 = 17;");
    only(check(t.root()), Ratchet::StatusResealIsAtomic);
    let f = Planted::of_tree();
    f.delete("crates/rafka-node-rpc-contract/tests/status_wire.rs");
    only(check(f.root()), Ratchet::StatusResealIsAtomic);
}

#[test]
fn forward_reseal_is_atomic_fails_a_moved_count_or_a_lost_fixture() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-rpc-contract/src/forward.rs", "const REPLY_VARIANTS: u32 = 14;", "const REPLY_VARIANTS: u32 = 15;");
    only(check(t.root()), Ratchet::ForwardResealIsAtomic);
    let f = Planted::of_tree();
    f.delete("crates/rafka-node-rpc-contract/tests/forward_wire.rs");
    only(check(f.root()), Ratchet::ForwardResealIsAtomic);
}

#[test]
fn one_new_liveness_primitive_fails_a_served_echo_or_an_unretired_echo_row() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-rpc-testkit/src/planted.rs", "fn serve(b: ServerBuilder) -> ServerBuilder { b.serve::<Echo, _, _>(OpOwner::Core, echo) }\n");
    only(check(t.root()), Ratchet::OneNewLivenessPrimitive);
    let r = Planted::of_tree();
    r.edit("crates/rafka-node-rpc-contract/src/catalog.rs", "family: \"echo\".into(), owner: OpOwner::Core, state: OpState::Retired", "family: \"echo\".into(), owner: OpOwner::Core, state: OpState::Live");
    only(check(r.root()), Ratchet::OneNewLivenessPrimitive);
}


#[test]
fn rdm_runtime_emits_customer_span_rejected_by_ownership_ratchet() {
    let t = Planted::of_tree();
    assert!(check(t.root()).is_empty(), "the unmodified copied tree passes");
    t.write("crates/rafka-node-rpc/src/planted.rs", "fn emit() { tracing::info_span!(\"rafka.node_rpc.request.serve.via-direct\"); }\n");
    let found = only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
    assert!(matches!(&found[0], Violation::Token { file, line: 1, token, .. }
        if file.ends_with("planted.rs") && token == "rafka.node_rpc.request.serve.via-direct"));
}

#[test]
fn ownership_scan_reads_emitters_preserves_customer_harness_and_fixture_prose() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-rpc/src/planted.rs", r#"
// tracing::info_span!("rafka.node_rpc.fixture.serve.via-comment");
const FIXTURE: &str = "rafka.node_rpc.request.serve.via-direct";
#[cfg(test)] mod tests { fn fixture() { tracing::info_span!("rafka.mesh.fixture.serve.via-test"); } }
fn emit() { tracing::info_span!("rdm.node_rpc.request.serve.via-direct"); }
"#);
    t.write("crates/rafka-chaos/src/planted.rs", "fn emit() { tracing::info_span!(\"rafka.chaos.fault.create.via-injection\"); }\n");
    assert!(check_one(t.root(), Ratchet::RdmSpansAreRdmPrefixed).is_empty());
    t.write("crates/rafka-node-rpc/src/planted.rs", "fn emit() { tracing::span!(tracing::Level::INFO, \"rafka.chaos.fault.create.via-injection\"); }\n");
    only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
}


#[test]
fn rdm_macro_emits_customer_span_rejected_by_ownership_ratchet() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-rpc/src/planted.rs", r#"
macro_rules! emit { () => { tracing::info_span!(concat!("rafka.", "node_rpc.request.serve.via-direct")); }; }
"#);
    only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
}


#[test]
fn rdm_instrument_emits_unnamed_span_rejected_by_ownership_ratchet() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-rpc/src/planted.rs", "#[tracing::instrument] fn emit() {}\n");
    only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
}

#[test]
fn rdm_instrument_names_owner_accepts_native_target_and_rejects_customer_name() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-rpc/src/planted.rs", r#"
fn emit() { tracing::info_span!(target: "rafka.customer-target", "rdm.node_rpc.request.serve.via-direct"); }
"#);
    assert!(check(t.root()).is_empty());
    t.write("crates/rafka-node-rpc/src/planted.rs", "#[tracing::instrument(name = \"rafka.node_rpc.request.serve.via-direct\")] fn emit() {}\n");
    only(check(t.root()), Ratchet::RdmSpansAreRdmPrefixed);
}

#[test]
fn no_json_on_the_wire_fails_a_json_codec_in_a_gossip_frame_path() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-mesh-transport/src/membership.rs", "crate::wire::encode(self).expect(\"frame serializes\")", "serde_json::to_vec(self).expect(\"frame serializes\")");
    let v = only(check(t.root()), Ratchet::NoJsonOnTheWire);
    assert!(v.iter().any(|v| matches!(v, Violation::Token { file, token, .. } if file.ends_with("membership.rs") && token == "serde_json::to_vec")), "{v:?}");

    // The Build topic too, and a direct postcard call beside the shared codec.
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-admin-core/src/fabric_builds.rs", "rafka_mesh_transport::wire::decode(bytes)", "serde_json::from_slice(bytes).map_err(|e| rafka_mesh_transport::wire::WireError::new(e.to_string()))");
    only(check(t.root()), Ratchet::NoJsonOnTheWire);
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-admin-core/src/fabric_builds.rs", "rafka_mesh_transport::wire::encode(&wire)", "postcard::to_allocvec(&wire).map_err(|e| rafka_mesh_transport::wire::WireError::new(e.to_string()))");
    only(check(t.root()), Ratchet::NoJsonOnTheWire);

    // A Node RPC source takes no JSON either.
    let t = Planted::of_tree();
    t.write("crates/rafka-node-rpc-contract/src/planted.rs", "fn frame(v: &Req) -> Vec<u8> { serde_json::to_vec(v).unwrap() }\n");
    only(check(t.root()), Ratchet::NoJsonOnTheWire);
}

#[test]
fn no_json_on_the_wire_reads_only_non_test_code_and_holds_its_proof_cell() {
    // A test module may build a JSON expectation: the scan stops at `#[cfg(test)]`.
    let t = Planted::of_tree();
    let clean = std::fs::read_to_string(t.root().join("crates/rafka-mesh-transport/src/wire.rs")).unwrap();
    t.write("crates/rafka-mesh-transport/src/wire.rs", &format!("{clean}\n#[cfg(test)]\nmod tests {{ fn json() {{ let _ = serde_json::json!(1); }} }}\n"));
    assert!(check_one(t.root(), Ratchet::NoJsonOnTheWire).is_empty());

    // Deleting or renaming the proof cell is itself a violation.
    let t = Planted::of_tree();
    t.edit(
        "crates/rafka-node-admin-core/tests/i143_acceptance_rw1.rs",
        "fn every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling",
        "fn every_frame_round_trips",
    );
    only(check(t.root()), Ratchet::NoJsonOnTheWire);
}

/// CONTRACT: a span guard bound with `enter()` or `entered()` and still alive at an `.await` of
/// its block is refused in non-test code; a guard released before the await, one that never meets
/// an await, a discarded `let _ =` and a test module are not.
#[test]
fn no_span_guard_across_await_refuses_a_guard_alive_at_an_await() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-admin-core/src/planted.rs", "async fn f(span: tracing::Span) {\n    let _g = span.enter();\n    work().await;\n}\n");
    let v = only(check(t.root()), Ratchet::NoSpanGuardAcrossAwait);
    assert!(matches!(&v[0], Violation::Token { file, line: 2, token, .. } if file.ends_with("planted.rs") && token.contains("held across the .await at line 3")), "{v:?}");
    t.write("crates/rafka-node-admin-core/src/planted.rs", "async fn f(span: tracing::Span) {\n    let g = span.clone().entered();\n    if ok() {\n        step();\n    }\n    other().await;\n}\n");
    only(check(t.root()), Ratchet::NoSpanGuardAcrossAwait);
}

#[test]
fn no_span_guard_across_await_accepts_scoped_dropped_discarded_and_test_guards() {
    let t = Planted::of_tree();
    t.write(
        "crates/rafka-node-admin-core/src/planted.rs",
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
