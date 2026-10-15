//! The 2026-10-07 lock's ratchets (rafka-v2 #2927): each rule of the lock as a mechanical
//! check over the workspace, named as the issue names it. A ratchet either scans sources for
//! a token the lock bans in a scope, or holds a structural fact the lock froze (an enum's
//! arms, a cell by name, a wire count). Every violation is named with its file and line, and a
//! ratchet whose subject is missing says so by name rather than passing vacuously.
//!
//! Behavioral rules (a stop attempted before termination, a carried call keeping its
//! context, node.connections.deleted distinct from refusal, silence never retiring) are proven by their
//! cells; the ratchet for such a rule holds the cell in the tree by name and the structural
//! facts the cell relies on, so deleting or renaming the proof is itself a violation.

use std::fmt;
use std::path::{Path, PathBuf};

/// The #2927 ratchets, the telemetry ownership ratchet, R-W1's wire ratchet and the span-guard ratchet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Ratchet {
    TagIsDescriptiveOnly,
    ResourceBehaviorIsTypedMeta,
    AcceptedBuildHasExplicitPathConfig,
    LegacyDefaultsAreIngressOnly,
    ProviderInterpretsNeitherMetaNorTags,
    NoTransportTagVocabulary,
    RetireAttemptsRpcStopFirst,
    RetireHasBoundedFailureArms,
    DriftSilenceIsNotRetireAuthority,
    RetireReceiptNamesCommandArm,
    ConnectionsDeletedIsNotRefusal,
    ForwardPreservesContext,
    PostcardSectionBounds,
    TypedProtocolIdentity,
    TypedProtocolEvidence,
    StatusResealIsAtomic,
    ForwardResealIsAtomic,
    OneNewLivenessPrimitive,
    RdmSpansAreRdmPrefixed,
    NoJsonOnTheWire,
    NoSpanGuardAcrossAwait,
    OneRenameWritePath,
}

impl Ratchet {
    pub const ALL: [Ratchet; 22] = [
        Self::TagIsDescriptiveOnly,
        Self::ResourceBehaviorIsTypedMeta,
        Self::AcceptedBuildHasExplicitPathConfig,
        Self::LegacyDefaultsAreIngressOnly,
        Self::ProviderInterpretsNeitherMetaNorTags,
        Self::NoTransportTagVocabulary,
        Self::RetireAttemptsRpcStopFirst,
        Self::RetireHasBoundedFailureArms,
        Self::DriftSilenceIsNotRetireAuthority,
        Self::RetireReceiptNamesCommandArm,
        Self::ConnectionsDeletedIsNotRefusal,
        Self::ForwardPreservesContext,
        Self::PostcardSectionBounds,
        Self::TypedProtocolIdentity,
        Self::TypedProtocolEvidence,
        Self::StatusResealIsAtomic,
        Self::ForwardResealIsAtomic,
        Self::OneNewLivenessPrimitive,
        Self::RdmSpansAreRdmPrefixed,
        Self::NoJsonOnTheWire,
        Self::NoSpanGuardAcrossAwait,
        Self::OneRenameWritePath,
    ];

    /// The name the issue gives the ratchet.
    pub fn name(self) -> &'static str {
        match self {
            Self::TagIsDescriptiveOnly => "tag_is_descriptive_only",
            Self::ResourceBehaviorIsTypedMeta => "resource_behavior_is_typed_meta",
            Self::AcceptedBuildHasExplicitPathConfig => "accepted_build_has_explicit_path_config",
            Self::LegacyDefaultsAreIngressOnly => "legacy_defaults_are_ingress_only",
            Self::ProviderInterpretsNeitherMetaNorTags => "provider_interprets_neither_meta_nor_tags",
            Self::NoTransportTagVocabulary => "no_transport_tag_vocabulary",
            Self::RetireAttemptsRpcStopFirst => "retire_attempts_rpc_stop_first",
            Self::RetireHasBoundedFailureArms => "retire_has_bounded_failure_arms",
            Self::DriftSilenceIsNotRetireAuthority => "drift_silence_is_not_retire_authority",
            Self::RetireReceiptNamesCommandArm => "retire_receipt_names_command_arm",
            Self::ConnectionsDeletedIsNotRefusal => "connections_deleted_is_not_refusal",
            Self::ForwardPreservesContext => "forward_preserves_context",
            Self::PostcardSectionBounds => "postcard_section_bounds",
            Self::TypedProtocolIdentity => "typed_protocol_identity",
            Self::TypedProtocolEvidence => "typed_protocol_evidence",
            Self::StatusResealIsAtomic => "status_reseal_is_atomic",
            Self::ForwardResealIsAtomic => "forward_reseal_is_atomic",
            Self::OneNewLivenessPrimitive => "one_new_liveness_primitive",
            Self::RdmSpansAreRdmPrefixed => "rdm_spans_are_rdm_prefixed",
            Self::NoJsonOnTheWire => "no_json_on_the_wire",
            Self::NoSpanGuardAcrossAwait => "no_span_guard_across_await",
            Self::OneRenameWritePath => "one_rename_write_path",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// `file:line` carries `token`, which the ratchet bans in that scope.
    Token { ratchet: Ratchet, file: String, line: usize, token: String },
    /// `file` must carry `what` (a frozen fact or a proof cell) and does not.
    Missing { ratchet: Ratchet, file: String, what: String },
    /// `file` must name exactly `expected` and names `found` instead.
    Arms { ratchet: Ratchet, file: String, expected: Vec<String>, found: Vec<String> },
}

impl Violation {
    pub fn ratchet(&self) -> Ratchet {
        match self {
            Self::Token { ratchet, .. } | Self::Missing { ratchet, .. } | Self::Arms { ratchet, .. } => *ratchet,
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token { ratchet, file, line, token } => write!(f, "{}: {file}:{line} carries `{token}`", ratchet.name()),
            Self::Missing { ratchet, file, what } => write!(f, "{}: {file} lacks {what}", ratchet.name()),
            Self::Arms { ratchet, file, expected, found } => {
                write!(f, "{}: {file} names the arms {found:?}; the lock froze {expected:?}", ratchet.name())
            }
        }
    }
}

/// The active sources the transport vocabulary rule covers: every crate that speaks the
/// envelope, and this audit's own sources.
pub const ACTIVE_TRANSPORT_SOURCES: &[&str] = &[
    "crates/rafka-node-rpc-contract",
    "crates/rafka-node-rpc",
    "crates/rafka-node-rpc-testkit",
    "crates/rdm-node",
    "crates/rafka-node-admin-core",
    "crates/rdm-client",
    "crates/rafka-test-scenario",
    "tools/mesh-audit",
];

/// Transport vocabulary the lock retired: `Op` names a wire family, `Tag` is not a wire concept.
pub const TRANSPORT_TAG_TOKENS: &[&str] = &[
    "TagOwner",
    "TagState",
    "UNSERVED_TAG",
    "UnservedTag",
    "TESTKIT_TAGS",
    "via-unserved-tag",
    "protocol.tag",
    "const TAG: u8",
    "::TAG",
    "head_tag",
    "inner_tag",
];

/// The runtime sources where a descriptive annotation is never read to decide.
/// The one writer of a node row's move (`move_row`).
pub const NODE_ROWS: &str = "crates/rafka-node-admin-core/src/node_rows.rs";
/// The nodes store: the claim and the release are defined and implemented here.
pub const NODES_STORAGE: &str = "crates/rafka-node-admin-core/src/storage.rs";

pub const RUNTIME_SOURCES: &[&str] = &[
    "crates/rdm-core/src",
    "crates/rafka-mesh-transport/src",
    "crates/rafka-node-rpc/src",
    "crates/rafka-node-rpc-testkit/src",
    "crates/rdm-node/src",
    "crates/rafka-node-admin-core/src",
    "crates/rdm-client/src",
];

/// A read of a descriptive map (`extra`, `tags`) in runtime code.
pub const DESCRIPTIVE_READ_TOKENS: &[&str] = &[".extra.get(", ".extra[", ".extra.contains_key(", ".tags.get(", ".tags[", ".tags.contains("];

/// Behavior-driving booleans the lock moved to `StorageMeta`.
pub const UNTYPED_BEHAVIOR_TOKENS: &[&str] = &["stateful: bool", "permanent: bool", "stateful: Option<bool>"];

/// The one file that may carry a legacy `stateful` intent: the ingress normalization input.
pub const INGRESS_META: &str = "crates/rdm-core/src/meta.rs";

/// Where a per-kind default or a normalization may be applied: before Build acceptance only.
pub const INGRESS_SOURCES: &[&str] = &[
    "crates/rdm-core/src/meta.rs",
    "crates/rafka-node-admin-core/src/accepted.rs",
    "crates/rafka-node-admin-core/src/build.rs",
];
pub const DEFAULT_TOKENS: &[&str] = &["default_for(", "normalize_path_config("];

/// The provider implementations: concrete actions on exact locators, policy-blind.
pub const PROVIDER_SOURCES: &[&str] = &[
    "crates/rafka-node-admin-core/src/deployment/provider.rs",
    "crates/rafka-node-admin-core/src/deployment/process.rs",
    "crates/rafka-node-admin-core/src/deployment/container.rs",
];
pub const PROVIDER_BANNED_TOKENS: &[&str] = &["PathConfig", "StorageMeta", "PlacementMeta", ".extra", ".tags", "stateful"];

/// The internal wire paths (R-W1, Luke 2026-10-08): every gossip frame and every Node RPC frame is
/// postcard. These sources carry no JSON codec in their non-test code.
pub const NON_WIRE_CORE_FILES: &[&str] = &["binding.rs", "cadence.rs", "certs.rs", "connections.rs", "digest.rs", "dto.rs", "ids.rs", "lifecycle.rs", "model.rs", "names.rs", "path.rs", "publisher.rs", "runtime.rs", "seat.rs"];
pub const WIRE_SOURCES: &[&str] = &[
    "crates/rafka-mesh-transport/src/membership.rs",
    "crates/rafka-mesh-transport/src/snapshot.rs",
    "crates/rafka-mesh-transport/src/chunking.rs",
    "crates/rafka-mesh-transport/src/wire.rs",
    "crates/rafka-node-admin-core/src/fabric_builds.rs",
    "crates/rafka-node-admin-core/src/wire.rs",
    "crates/rafka-node-admin-core/src/join.rs",
    "crates/rdm-node/src/topology_read.rs",
    "crates/rdm-node/src/join.rs",
    "crates/rdm-node/src/wire.rs",
    "crates/rdm-core/src/wire.rs",
    "crates/rdm-core/src",
    "crates/rafka-node-rpc/src",
];
/// A JSON codec: bytes or text produced or read as JSON. `serde_json::to_value`/`from_value`
/// convert between structures and put no JSON on a wire, so they are not in this list.
pub const JSON_CODEC_TOKENS: &[&str] = &[
    "use serde_json",
    "serde_json::to_vec",
    "serde_json::to_string",
    "serde_json::to_writer",
    "serde_json::from_slice",
    "serde_json::from_str",
    "serde_json::from_reader",
    "serde_json::Serializer",
    "serde_json::Deserializer",
];

/// The one source that converts a step receipt's JSON `output` (the REST and journal shape) to the
/// typed result its step produced: it names `serde_json::Value` and nothing else may.
pub const JSON_VALUE_EDGE: &str = "crates/rafka-node-admin-core/src/wire.rs";

/// The gossip topics' frame codecs go through the one shared codec, never `postcard` directly.
pub const GOSSIP_FRAME_SOURCES: &[&str] = &["crates/rafka-mesh-transport/src/membership.rs", "crates/rafka-node-admin-core/src/fabric_builds.rs"];
pub const WIRE_CODEC: &str = "crates/rafka-mesh-transport/src/wire.rs";
pub const WIRE_CELLS: &str = "crates/rafka-node-admin-core/tests/gossip_frames_wire.rs";
/// Where a span guard held across an `.await` is refused: every workspace source that is not a test.
pub const SPAN_GUARD_SOURCES: &[&str] = &["crates/", "demo/", "tools/"];
/// The join (`JoinNode`, 0x1D): its serve and call sites, and the cell that proves its postcard shape.
pub const JOIN_SOURCE: &str = "crates/rafka-node-admin-core/src/join.rs";
pub const JOIN_CELLS: &str = "crates/rafka-node-admin-core/tests/join_wire.rs";
/// The topology read (`GetTopology`, 0x1E): its serve and install sites, and the cell that proves its postcard shape.
pub const TOPOLOGY_SOURCE: &str = "crates/rdm-node/src/topology_read.rs";
pub const SEAT_FRAME_CELLS: &str = "crates/rafka-mesh-transport/tests/seat_frames_wire.rs";
pub const TOPOLOGY_CELLS: &str = "crates/rdm-core/tests/topology_wire.rs";
/// The Build-facts read (`FetchBuildFacts`, 0x1F): its serve and absorb sites, and the cell that proves its postcard shape.
pub const BUILD_FACTS_SOURCE: &str = "crates/rafka-node-admin-core/src/build_facts_read.rs";
pub const BUILD_FACTS_HYDRATE: &str = "crates/rafka-node-admin-core/src/hydrate.rs";
pub const BUILD_FACTS_CELLS: &str = "crates/rdm-core/tests/build_facts_wire.rs";
/// node.primary.update (`PrimaryUpdate` / `UpdatePrimary`, 0x21): the holder writes the one committed seat-transfer row, the successor installs from it and answers `PrimaryUpdated`; its door and the cells that prove its postcard shape and the seat frames.
pub const HANDOVER_SOURCE: &str = "crates/rafka-node-admin-core/src/fabric_handover.rs";
pub const HANDOVER_CELLS: &str = "crates/rdm-core/tests/handover_wire.rs";

pub const PIPELINE: &str = "crates/rafka-node-admin-core/src/deployment/pipeline.rs";
pub const ACCEPTED: &str = "crates/rafka-node-admin-core/src/accepted.rs";
pub const STATUS: &str = "crates/rdm-core/src/status.rs";
pub const FORWARD: &str = "crates/rdm-core/src/forward.rs";
pub const FORWARD_WIRE: &str = "crates/rdm-core/tests/forward_wire.rs";
/// The frozen 0x1A counts.
pub const FORWARD_REQUESTS: u32 = 1;
pub const FORWARD_REPLIES: u32 = 14;
pub const STATUS_WIRE: &str = "crates/rdm-core/tests/status_wire.rs";
pub const FRAMING: &str = "crates/rdm-core/src/framing.rs";
pub const CATALOG: &str = "crates/rdm-core/src/catalog.rs";
pub const CONTEXT_CELLS: &str = "crates/rafka-node-rpc/tests/context.rs";
pub const RETIRE_CELLS: &str = "crates/rafka-node-rpc-testkit/tests/node_delete_pipeline.rs";
pub const DRIFT_CELLS: &str = "crates/rafka-node-admin-core/tests/drift_convergence.rs";
pub const PROBE_APPLY_CELLS: &str = "crates/rafka-node-rpc-testkit/tests/status_probe_apply.rs";

/// The typed arms of the landed `CommandAdmission` (what a directed `node.connections.delete`,
/// `stop-node` or `start-node` call established at the exact birth).
pub const COMMAND_ARMS: [&str; 9] = ["Admitted", "AlreadyAdmitted", "Stopped", "ConnectionsDeleted", "Started", "StartFailed", "NotSent", "Indeterminate", "Refused"];

/// The frozen 0x1B counts.
pub const STATUS_REQUESTS: u32 = 30;
pub const STATUS_REPLIES: u32 = 51;

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        let name = e.file_name();
        if p.is_dir() {
            if name != "target" && name != ".git" {
                rust_files(&p, out);
            }
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// The code of `line` without its `//` comment and without a serde attribute (a serde `tag`
/// names a JSON discriminator field, not a wire concept).
fn code(line: &str) -> &str {
    if line.trim_start().starts_with("#[serde(") {
        return "";
    }
    match line.find("//") {
        Some(i) => &line[..i],
        None => line,
    }
}

fn rel(root: &Path, file: &Path) -> String {
    file.strip_prefix(root).unwrap_or(file).display().to_string()
}

fn read(root: &Path, file: &str) -> Option<String> {
    std::fs::read_to_string(root.join(file)).ok()
}

/// Every `.rs` file under `root` whose workspace-relative path starts with one of `scopes`.
fn files_in(root: &Path, scopes: &[&str]) -> Vec<PathBuf> {
    let mut all = Vec::new();
    rust_files(root, &mut all);
    all.sort();
    all.into_iter().filter(|f| scopes.iter().any(|s| rel(root, f).starts_with(s))).collect()
}

fn scan_tokens(root: &Path, files: &[PathBuf], tokens: &[&str], ratchet: Ratchet, out: &mut Vec<Violation>) {
    let me = Path::new(file!()).file_name().unwrap_or_default().to_owned();
    for f in files {
        if rel(root, f).starts_with("tools/mesh-audit") && f.file_name() == Some(me.as_os_str()) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(f) else { continue };
        for (i, line) in text.lines().enumerate() {
            let c = code(line);
            for t in tokens {
                if c.contains(t) {
                    out.push(Violation::Token { ratchet, file: rel(root, f), line: i + 1, token: (*t).to_string() });
                }
            }
        }
    }
}

/// A source's declared shape: doc comment lines dropped, whitespace removed and a trailing comma
/// before a closing brace removed, so a needle names an item whatever its layout.
fn shape_of(text: &str) -> String {
    let code: String = text.lines().filter(|l| !l.trim_start().starts_with("///")).collect::<Vec<_>>().join("\n");
    code.split_whitespace().collect::<String>().replace(",}", "}")
}

fn require(root: &Path, file: &str, needles: &[&str], ratchet: Ratchet, out: &mut Vec<Violation>) {
    match read(root, file) {
        None => out.push(Violation::Missing { ratchet, file: file.into(), what: "the file itself".into() }),
        Some(text) => {
            let shape = shape_of(&text);
            for n in needles {
                if !shape.contains(&shape_of(n)) {
                    out.push(Violation::Missing { ratchet, file: file.into(), what: format!("`{n}`") });
                }
            }
        }
    }
}

/// The variant names of `pub enum <name>` in `text`, in declaration order.
fn enum_arms(text: &str, name: &str) -> Option<Vec<String>> {
    let start = text.find(&format!("pub enum {name} {{"))?;
    let body = &text[start..];
    let end = body.find("\n}")?;
    let mut arms = Vec::new();
    for line in body[..end].lines().skip(1) {
        let t = line.trim_start();
        if t.starts_with("///") || t.starts_with("#[") || t.is_empty() {
            continue;
        }
        if line.starts_with("    ") && !line.starts_with("     ") {
            let arm: String = t.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
            if !arm.is_empty() {
                arms.push(arm);
            }
        }
    }
    Some(arms)
}

/// The lines of `text` with every `#[cfg(test)]` item blanked, in place, so line numbers hold:
/// the one rule every non-test scan shares. The attribute gates exactly the next item (its other
/// attributes, then a `;`-ended item or a braced item to its closing brace); code after that item
/// is read again, so an item-level `#[cfg(test)]` (a gated helper) hides nothing below it, and a
/// test module at the end of a file is skipped whole.
pub fn non_test_lines(text: &str) -> Vec<&str> {
    let mut lines: Vec<&str> = text.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        if !lines[i].trim_start().starts_with("#[cfg(test)]") {
            i += 1;
            continue;
        }
        let mut depth: i32 = 0;
        let mut opened = false;
        while i < lines.len() {
            let c = code(lines[i]);
            let t = c.trim();
            lines[i] = "";
            i += 1;
            if !opened && (t.is_empty() || (t.starts_with("#[") && t.ends_with(']'))) {
                continue;
            }
            depth += c.matches('{').count() as i32 - c.matches('}').count() as i32;
            opened |= c.contains('{');
            if (opened && depth <= 0) || (!opened && t.ends_with(';')) {
                break;
            }
        }
    }
    lines
}

/// `scan_tokens` over each file's non-test code ([`non_test_lines`]).
fn scan_non_test_tokens(root: &Path, files: &[PathBuf], tokens: &[&str], ratchet: Ratchet, out: &mut Vec<Violation>) {
    for f in files {
        let Ok(text) = std::fs::read_to_string(f) else { continue };
        for (i, line) in non_test_lines(&text).into_iter().enumerate() {
            let c = code(line);
            for t in tokens {
                if c.contains(t) {
                    out.push(Violation::Token { ratchet, file: rel(root, f), line: i + 1, token: (*t).to_string() });
                }
            }
        }
    }
}

/// The guards a source binds with `span.enter()` / `span.entered()` that are still alive at an
/// `.await` of the same block, as `(line, guard name)` pairs of the first such await's line.
/// Only non-test code is scanned ([`non_test_lines`]). A guard dropped by name before the await
/// is not held across it.
pub fn span_guards_across_await(text: &str) -> Vec<(usize, usize, String)> {
    let lines = non_test_lines(text);
    let end = lines.len();
    let mut found = Vec::new();
    for (i, line) in lines[..end].iter().enumerate() {
        let c = code(line);
        let Some(name) = guard_binding(c) else { continue };
        let mut depth: i32 = c.matches('{').count() as i32 - c.matches('}').count() as i32;
        for (j, later) in lines[i + 1..end].iter().enumerate() {
            let l = code(later);
            if l.contains(&format!("drop({name})")) {
                break;
            }
            if l.contains(".await") {
                found.push((i + 1, i + 2 + j, name.clone()));
                break;
            }
            depth += l.matches('{').count() as i32 - l.matches('}').count() as i32;
            if depth < 0 {
                break;
            }
        }
    }
    found
}

/// The name a statement binds a span guard to: `let _g = span.enter();` or `let g = s.entered();`.
fn guard_binding(code: &str) -> Option<String> {
    let t = code.trim_start();
    let rest = t.strip_prefix("let ")?;
    let rest = rest.strip_prefix("mut ").unwrap_or(rest);
    let name: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    if name.is_empty() || name == "_" {
        return None;
    }
    let after = rest[name.len()..].trim_start();
    let rhs = after.strip_prefix('=')?;
    let rhs = rhs.trim_end();
    (rhs.ends_with(".enter();") || rhs.ends_with(".entered();")).then_some(name)
}

/// Every violation of `ratchet` in the workspace at `root`.
pub fn check_one(root: &Path, ratchet: Ratchet) -> Vec<Violation> {
    let mut out = Vec::new();
    match ratchet {
        Ratchet::RdmSpansAreRdmPrefixed => {
            require(root, "crates/rafka-node-rpc/src/server.rs", &["rdm.node_rpc.request.serve.via-direct"], ratchet, &mut out);
            match crate::telemetry::emitters(root) {
                Ok(emitters) => for e in emitters {
                    // The retained chaos harness is a customer-owned exception, not a
                    // license for generic substrate code to emit arbitrary rafka.* spans.
                    let customer_chaos = e.file.starts_with("crates/rafka-chaos/src/") && e.name.starts_with("rafka.chaos.");
                    if !e.name.starts_with("rdm.") && !customer_chaos {
                        out.push(Violation::Token { ratchet, file: e.file, line: e.line, token: e.name });
                    }
                },
                Err(e) => out.push(Violation::Missing { ratchet, file: "runtime Rust sources".into(), what: format!("parseable instrumentation: {e}") }),
            }
        }
        Ratchet::NoSpanGuardAcrossAwait => {
            for f in files_in(root, SPAN_GUARD_SOURCES) {
                let file = rel(root, &f);
                if file.contains("/tests/") || file.contains("/benches/") || file.contains("/examples/") {
                    continue;
                }
                let Ok(text) = std::fs::read_to_string(&f) else { continue };
                for (line, await_line, name) in span_guards_across_await(&text) {
                    out.push(Violation::Token { ratchet, file: file.clone(), line, token: format!("let {name} = <span>.enter() held across the .await at line {await_line}") });
                }
            }
        }
        Ratchet::TagIsDescriptiveOnly => {
            scan_tokens(root, &files_in(root, RUNTIME_SOURCES), DESCRIPTIVE_READ_TOKENS, ratchet, &mut out);
        }
        Ratchet::ResourceBehaviorIsTypedMeta => {
            let files: Vec<PathBuf> = files_in(root, RUNTIME_SOURCES).into_iter().filter(|f| rel(root, f) != INGRESS_META).collect();
            scan_tokens(root, &files, UNTYPED_BEHAVIOR_TOKENS, ratchet, &mut out);
        }
        Ratchet::AcceptedBuildHasExplicitPathConfig => {
            require(
                root,
                ACCEPTED,
                &["path_config_by_path", "PathConfigMismatch", "fn every_materialized_path_carries_exactly_one_path_config"],
                ratchet,
                &mut out,
            );
        }
        Ratchet::LegacyDefaultsAreIngressOnly => {
            let files: Vec<PathBuf> = files_in(root, RUNTIME_SOURCES).into_iter().filter(|f| !INGRESS_SOURCES.contains(&rel(root, f).as_str())).collect();
            scan_tokens(root, &files, DEFAULT_TOKENS, ratchet, &mut out);
        }
        Ratchet::ProviderInterpretsNeitherMetaNorTags => {
            let files: Vec<PathBuf> = PROVIDER_SOURCES.iter().map(|p| root.join(p)).filter(|p| p.exists()).collect();
            scan_tokens(root, &files, PROVIDER_BANNED_TOKENS, ratchet, &mut out);
        }
        Ratchet::NoTransportTagVocabulary => {
            scan_tokens(root, &files_in(root, ACTIVE_TRANSPORT_SOURCES), TRANSPORT_TAG_TOKENS, ratchet, &mut out);
        }
        Ratchet::RetireAttemptsRpcStopFirst => {
            require(root, RETIRE_CELLS, &["fn retire_runs_every_step_in_order", "RetireStep::StopNode"], ratchet, &mut out);
            if let Some(text) = read(root, PIPELINE) {
                match (text.find("Self::StopNode,"), text.find("Self::TerminateRuntime,")) {
                    (Some(a), Some(b)) if a < b => {}
                    _ => out.push(Violation::Missing { ratchet, file: PIPELINE.into(), what: "StopNode ordered before TerminateRuntime in the retire steps".into() }),
                }
            } else {
                out.push(Violation::Missing { ratchet, file: PIPELINE.into(), what: "the file itself".into() });
            }
        }
        Ratchet::RetireHasBoundedFailureArms => match read(root, PIPELINE).as_deref().and_then(|t| enum_arms(t, "CommandAdmission")) {
            Some(found) if found == COMMAND_ARMS => {}
            Some(found) => out.push(Violation::Arms { ratchet, file: PIPELINE.into(), expected: COMMAND_ARMS.iter().map(|s| s.to_string()).collect(), found }),
            None => out.push(Violation::Missing { ratchet, file: PIPELINE.into(), what: "`pub enum CommandAdmission`".into() }),
        },
        Ratchet::DriftSilenceIsNotRetireAuthority => {
            require(root, DRIFT_CELLS, &["fn a_silent_node_whose_runtime_runs_opens_no_attempt_and_is_never_replaced"], ratchet, &mut out);
        }
        Ratchet::RetireReceiptNamesCommandArm => {
            require(root, PIPELINE, &["#[serde(tag = \"arm\", rename_all = \"kebab-case\", deny_unknown_fields)]\npub enum CommandAdmission"], ratchet, &mut out);
            require(root, RETIRE_CELLS, &["StopNode"], ratchet, &mut out);
        }
        Ratchet::ConnectionsDeletedIsNotRefusal => {
            require(root, STATUS, &["ConnectionsDeleting { reason: String }", "ConnectionsDeleted {", "receipt: Option<ConnectionsReceipt>"], ratchet, &mut out);
            require(root, PROBE_APPLY_CELLS, &["fn a_node_admin_probes_and_deletes_the_connections_of_the_exact_birth_over_the_status_family"], ratchet, &mut out);
        }
        Ratchet::ForwardPreservesContext => {
            require(root, CONTEXT_CELLS, &["fn a_carried_call_keeps_the_origins_trace_and_caller_system_through_the_hop"], ratchet, &mut out);
        }
        Ratchet::PostcardSectionBounds => {
            require(
                root,
                FRAMING,
                &["TooLarge { op: u8, declared: u64, max: usize }", "fn oversize_is_refused_from_the_declared_length_before_the_body"],
                ratchet,
                &mut out,
            );
        }
        Ratchet::TypedProtocolIdentity => {
            if let Some(text) = read(root, STATUS) {
                if let Some(start) = text.find("pub enum StatusRequest {") {
                    let body = &text[start..];
                    let body = &body[..body.find("\n}").unwrap_or(body.len())];
                    for (i, line) in body.lines().enumerate() {
                        for t in ["node_id: String", "mesh_id: String", "fabric_id: String", "incarnation: String"] {
                            if line.contains(t) {
                                let line_no = text[..start].lines().count() + i;
                                out.push(Violation::Token { ratchet, file: STATUS.into(), line: line_no, token: t.into() });
                            }
                        }
                    }
                } else {
                    out.push(Violation::Missing { ratchet, file: STATUS.into(), what: "`pub enum StatusRequest`".into() });
                }
            } else {
                out.push(Violation::Missing { ratchet, file: STATUS.into(), what: "the file itself".into() });
            }
        }
        Ratchet::TypedProtocolEvidence => {
            require(
                root,
                STATUS,
                &[
                    "RejectedStaleIncarnation { held: IncarnationId }",
                    "RejectedStaleMesh { held: MeshId }",
                    "RejectedStaleFabric { held: FabricId }",
                    "RejectedInvalidNodeTransition { current: NodeState }",
                    "RejectedInvalidMeshTransition { current: MeshState }",
                ],
                ratchet,
                &mut out,
            );
            if let Some(text) = read(root, STATUS) {
                for (i, line) in text.lines().enumerate() {
                    for t in ["held: String", "current: String"] {
                        if code(line).contains(t) {
                            out.push(Violation::Token { ratchet, file: STATUS.into(), line: i + 1, token: t.into() });
                        }
                    }
                }
            }
        }
        Ratchet::StatusResealIsAtomic => {
            require(
                root,
                STATUS,
                &[&format!("const REQUEST_VARIANTS: u32 = {STATUS_REQUESTS};"), &format!("const REPLY_VARIANTS: u32 = {STATUS_REPLIES};")],
                ratchet,
                &mut out,
            );
            require(
                root,
                STATUS_WIRE,
                &[
                    "fn requests_match_the_frozen_thirty_variant_wire_schema",
                    "fn replies_match_the_frozen_fifty_one_variant_wire_schema",
                    "fn nested_enum_discriminants_and_fields_are_frozen_too",
                    "Status::REQUEST_VARIANTS",
                    "Status::REPLY_VARIANTS",
                ],
                ratchet,
                &mut out,
            );
        }
        Ratchet::ForwardResealIsAtomic => {
            require(
                root,
                FORWARD,
                &[&format!("const REQUEST_VARIANTS: u32 = {FORWARD_REQUESTS};"), &format!("const REPLY_VARIANTS: u32 = {FORWARD_REPLIES};")],
                ratchet,
                &mut out,
            );
            require(
                root,
                FORWARD_WIRE,
                &[
                    "fn the_request_matches_the_frozen_one_variant_wire_schema",
                    "fn replies_match_the_frozen_fourteen_variant_wire_schema",
                    "Forward::REQUEST_VARIANTS",
                    "Forward::REPLY_VARIANTS",
                ],
                ratchet,
                &mut out,
            );
        }
        Ratchet::NoJsonOnTheWire => {
            let files: Vec<PathBuf> = files_in(root, WIRE_SOURCES).into_iter().filter(|f| !NON_WIRE_CORE_FILES.iter().any(|n| rel(root, f) == format!("crates/rdm-core/src/{n}"))).collect();
            if files.is_empty() {
                out.push(Violation::Missing { ratchet, file: WIRE_SOURCES.join(", "), what: "the wire sources themselves".into() });
            }
            scan_non_test_tokens(root, &files, JSON_CODEC_TOKENS, ratchet, &mut out);
            let no_edge: Vec<PathBuf> = files.iter().filter(|f| rel(root, f) != JSON_VALUE_EDGE).cloned().collect();
            scan_non_test_tokens(root, &no_edge, &["serde_json::Value"], ratchet, &mut out);
            scan_non_test_tokens(root, &files_in(root, GOSSIP_FRAME_SOURCES), &["postcard::"], ratchet, &mut out);
            require(root, WIRE_CODEC, &["postcard::to_allocvec", "postcard::take_from_bytes", "bytes left after the frame"], ratchet, &mut out);
            require(root, "crates/rafka-mesh-transport/src/membership.rs", &["crate::wire::encode", "crate::wire::decode", "rdm.mesh.membership.reject.via-undecodable-frame", "Seated { seat: Seat, holder: SeatHolder }", "Concern { seat: Seat, node_id: NodeId"], ratchet, &mut out);
            require(root, SEAT_FRAME_CELLS, &["fn seat_frames_match_the_frozen_wire_schema"], ratchet, &mut out);
            require(root, "crates/rafka-node-admin-core/src/fabric_builds.rs", &["rafka_mesh_transport::wire::encode", "rafka_mesh_transport::wire::decode", "rdm.node_admin.build.reject.via-undecodable-fact"], ratchet, &mut out);
            require(root, JOIN_SOURCE, &["crate::wire::answer_to_wire"], ratchet, &mut out);
            require(root, "crates/rdm-node/src/join.rs", &["WireDigest::from", "crate::wire::answer_from_wire"], ratchet, &mut out);
            require(root, JOIN_CELLS, &["fn a_join_request_and_its_answer_round_trip_through_postcard"], ratchet, &mut out);
            require(root, TOPOLOGY_SOURCE, &["chunks_of", "take_read_chunk", "ProjectionEntry::from", "rdm.mesh.topology.serve.via-read", "rdm.mesh.topology.update.via-read-install"], ratchet, &mut out);
            require(root, TOPOLOGY_CELLS, &["fn replies_match_the_frozen_fourteen_variant_wire_schema"], ratchet, &mut out);
            require(root, BUILD_FACTS_SOURCE, &["encode_chunks", "BuildMessage::from_bytes", "rdm.node_admin.build.serve.via-fetch-facts"], ratchet, &mut out);
            require(root, BUILD_FACTS_HYDRATE, &["absorb_facts", "rdm.node_admin.build.update.via-fetch-facts"], ratchet, &mut out);
            require(root, BUILD_FACTS_CELLS, &["fn replies_match_the_frozen_ten_variant_wire_schema"], ratchet, &mut out);
            require(root, HANDOVER_SOURCE, &["rdm.node_admin.seat.update.via-seat-row-written", "rdm.node_admin.seat.update.via-primary-updated", "rdm.node_admin.seat.reject.via-primary-update", "put_seat_transfer"], ratchet, &mut out);
            require(root, HANDOVER_CELLS, &["fn requests_match_the_frozen_three_variant_wire_schema", "fn replies_match_the_frozen_seventeen_variant_wire_schema"], ratchet, &mut out);
            require(root, SEAT_FRAME_CELLS, &["fn new_fabric_primary_matches_the_frozen_wire_schema", "fn primary_updated_matches_the_frozen_wire_schema", "fn primary_updating_matches_the_frozen_wire_schema"], ratchet, &mut out);
            require(root, WIRE_CELLS, &["fn every_gossip_frame_and_build_message_round_trips_through_postcard_under_the_ceiling"], ratchet, &mut out);
        }
        Ratchet::OneRenameWritePath => {
            // A node row moves through one writer (R13: node.update is one write path):
            // `node_rows::move_row` claims the new name and releases the old key. Nothing else
            // releases a node row's key, and no second rename door stands beside it.
            let files: Vec<PathBuf> = files_in(root, RUNTIME_SOURCES).into_iter().filter(|f| !rel(root, f).ends_with(NODE_ROWS) && !rel(root, f).ends_with(NODES_STORAGE)).collect();
            scan_non_test_tokens(root, &files, &[".release_node_row("], ratchet, &mut out);
            scan_tokens(root, &files_in(root, &[NODE_ROWS]), &["pub async fn rename(", "fn rename_steps("], ratchet, &mut out);
            require(root, NODE_ROWS, &["pub(crate) async fn move_row(storage: &dyn NodesStorage,"], ratchet, &mut out);
        }
        Ratchet::OneNewLivenessPrimitive => {
            // Echo (0x11) is retired forever and peer-tickle (0x19) is a product's transitional
            // op: nothing in RDM serves or calls either; ping (0x01) is the one liveness primitive.
            let files = files_in(root, RUNTIME_SOURCES);
            scan_tokens(root, &files, &["serve::<Echo", "Echo::OP", "echo::Echo", "<Echo as", "PeerTickle", "peer_tickle::"], ratchet, &mut out);
            require(root, CATALOG, &["op: 0x11, family: \"echo\".into(), owner: OpOwner::Core, state: OpState::Retired"], ratchet, &mut out);
        }
    }
    out
}

/// Every violation of every ratchet, in ratchet order.
pub fn check(root: &Path) -> Vec<Violation> {
    Ratchet::ALL.iter().flat_map(|r| check_one(root, *r)).collect()
}
