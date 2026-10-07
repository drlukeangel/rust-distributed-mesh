//! #2927 acceptance: every lock ratchet is an executable check, a planted violation fails
//! exactly that ratchet, and the RDM tree is green under all seventeen.

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
        for scope in ["crates", "tools/mesh-audit/src"] {
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
        "        Self::MarkDraining,\n        Self::WaitForDrain,",
        "        Self::WaitForDrain,",
    );
    t.edit("crates/rafka-node-admin-core/src/deployment/pipeline.rs", "        Self::TerminateRuntime,", "        Self::TerminateRuntime,\n        Self::MarkDraining,");
    only(check(t.root()), Ratchet::RetireAttemptsRpcDrainFirst);
}

#[test]
fn retire_has_bounded_failure_arms_fails_the_older_four_arm_draft() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-admin-core/src/deployment/pipeline.rs", "    Refused { reply: String },\n", "");
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
        "#[serde(tag = \"arm\", rename_all = \"kebab-case\")]\npub enum DrainOutcome",
        "#[serde(rename_all = \"kebab-case\")]\npub enum DrainOutcome",
    );
    only(check(t.root()), Ratchet::RetireReceiptNamesDrainArm);
}

#[test]
fn drain_success_is_not_refusal_fails_when_the_applied_reply_is_gone() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-rpc-contract/src/status.rs", "NodeDrainingApplied { in_flight: u64 }", "NodeDrainingApplied");
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
    t.edit("crates/rafka-node-rpc-contract/src/status.rs", "ProbeNodeState { node_id: NodeId, incarnation: IncarnationId }", "ProbeNodeState { node_id: String, incarnation: IncarnationId }");
    let v = only(check(t.root()), Ratchet::TypedProtocolIdentity);
    assert!(matches!(&v[0], Violation::Token { token, .. } if token == "node_id: String"));
}

#[test]
fn typed_protocol_evidence_fails_untyped_held_evidence() {
    let t = Planted::of_tree();
    t.edit("crates/rafka-node-rpc-contract/src/status.rs", "RejectedStaleMesh { held: MeshId }", "RejectedStaleMesh { held: String }");
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
fn one_new_liveness_primitive_fails_a_served_echo_or_an_unretired_echo_row() {
    let t = Planted::of_tree();
    t.write("crates/rafka-node-rpc-testkit/src/planted.rs", "fn serve(b: ServerBuilder) -> ServerBuilder { b.serve::<Echo, _, _>(OpOwner::Core, echo) }\n");
    only(check(t.root()), Ratchet::OneNewLivenessPrimitive);
    let r = Planted::of_tree();
    r.edit("crates/rafka-node-rpc-contract/src/catalog.rs", "family: \"echo\".into(), owner: OpOwner::Core, state: OpState::Retired", "family: \"echo\".into(), owner: OpOwner::Core, state: OpState::Live");
    only(check(r.root()), Ratchet::OneNewLivenessPrimitive);
}
