//! Legacy role-binary disposition gate (i143.e0.s1, PRD §4 / R21).
//!
//! RDM's `bridge` binary predates the generic Mesh product. It carries exactly
//! one disposition in the checked-in audit doc. `broker`, `gateway`, `compute`
//! and `registry` are the e11 role binaries built on `rafka-node-base`; none of
//! them stands in as a node of a proof shape (SN/MN/MM): the proof estate uses
//! generic node-admins and RPC proof nodes only.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

/// The legacy binary named by PRD §4 and ownership §17 that no role replaced.
pub const LEGACY_BINARIES: &[&str] = &["bridge"];

/// The audit doc that carries the disposition table.
pub const AUDIT_DOC: &str = "docs/i143/e0-workspace-audit.md";

/// One of the three dispositions PRD §4 allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Kept as a legacy/example program, outside the import manifest.
    RetainedExample,
    /// Source of generic behavior deliberately extracted into generic crates.
    ExtractionInput,
    /// Dead code: deleted from the workspace.
    DeadDeleted,
}

impl Disposition {
    pub fn parse(cell: &str) -> Option<Self> {
        match cell.trim().trim_matches('`') {
            "retained-example" => Some(Self::RetainedExample),
            "extraction-input" => Some(Self::ExtractionInput),
            "dead-deleted" => Some(Self::DeadDeleted),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    AuditDocMissing(PathBuf),
    /// A legacy binary has no row in the disposition table.
    Unclassified(String),
    /// A legacy binary has more than one row.
    Duplicate { binary: String, rows: usize },
    /// A row's disposition cell is not one of the three dispositions.
    UnknownDisposition { binary: String, cell: String },
    /// A row names something that is not a legacy binary.
    UnknownBinary(String),
    /// `dead-deleted`, but the crate directory or workspace member remains.
    DeadButPresent(String),
    /// retained/extraction, but the crate is gone from the workspace.
    LiveButAbsent(String),
    /// A proof-shape declaration names a legacy role.
    LegacyRoleInProofShape { file: PathBuf, line: usize, role: String },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuditDocMissing(p) => write!(f, "audit doc {} is missing", p.display()),
            Self::Unclassified(b) => write!(f, "legacy binary `{b}` has no disposition row"),
            Self::Duplicate { binary, rows } => {
                write!(f, "legacy binary `{binary}` has {rows} disposition rows, expected exactly one")
            }
            Self::UnknownDisposition { binary, cell } => write!(
                f,
                "legacy binary `{binary}` has disposition `{cell}`; expected retained-example | extraction-input | dead-deleted"
            ),
            Self::UnknownBinary(b) => write!(f, "disposition row names `{b}`, which is not a legacy binary"),
            Self::DeadButPresent(b) => write!(f, "`{b}` is dead-deleted but its crate or workspace member remains"),
            Self::LiveButAbsent(b) => write!(f, "`{b}` is retained but its crate is gone from the workspace"),
            Self::LegacyRoleInProofShape { file, line, role } => write!(
                f,
                "{}:{line}: proof shape names legacy role `{role}`; proof shapes use node-admin and rpc nodes only",
                file.display()
            ),
        }
    }
}

/// Parse the disposition table: every markdown row whose first cell is a
/// back-ticked binary name and whose second cell is the disposition, under the
/// `## Legacy binary dispositions` heading.
pub fn parse_disposition_rows(doc: &str) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    let mut in_section = false;
    for line in doc.lines() {
        if line.starts_with("## ") {
            in_section = line.trim() == "## Legacy binary dispositions";
            continue;
        }
        if !in_section || !line.trim_start().starts_with('|') {
            continue;
        }
        let cells: Vec<&str> = line.trim().trim_matches('|').split('|').map(str::trim).collect();
        if cells.len() < 2 {
            continue;
        }
        let first = cells[0];
        if !(first.starts_with('`') && first.ends_with('`') && first.len() > 2) {
            continue; // header or separator row
        }
        rows.push((first.trim_matches('`').to_string(), cells[1].to_string()));
    }
    rows
}

/// Workspace members, read from the root `Cargo.toml` `members = [...]` list.
pub fn workspace_members(root: &Path) -> Vec<String> {
    let text = std::fs::read_to_string(root.join("Cargo.toml")).unwrap_or_default();
    let Some(start) = text.find("members") else { return Vec::new() };
    let rest = &text[start..];
    let (Some(open), Some(close)) = (rest.find('['), rest.find(']')) else { return Vec::new() };
    rest[open + 1..close]
        .split(',')
        .map(|m| m.trim().trim_matches('"').to_string())
        .filter(|m| !m.is_empty() && !m.starts_with('#'))
        .collect()
}

/// Check the disposition table against the workspace.
pub fn check_dispositions(root: &Path) -> Vec<Violation> {
    let doc_path = root.join(AUDIT_DOC);
    let Ok(doc) = std::fs::read_to_string(&doc_path) else {
        return vec![Violation::AuditDocMissing(doc_path)];
    };
    let members = workspace_members(root);
    let mut by_binary: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut out = Vec::new();
    for (binary, cell) in parse_disposition_rows(&doc) {
        if !LEGACY_BINARIES.contains(&binary.as_str()) {
            out.push(Violation::UnknownBinary(binary));
            continue;
        }
        by_binary.entry(binary).or_default().push(cell);
    }
    for &binary in LEGACY_BINARIES {
        let present = root.join(binary).join("Cargo.toml").exists()
            || members.iter().any(|m| m == binary);
        match by_binary.get(binary).map(Vec::as_slice) {
            None | Some([]) => out.push(Violation::Unclassified(binary.into())),
            Some([cell]) => match Disposition::parse(cell) {
                None => out.push(Violation::UnknownDisposition { binary: binary.into(), cell: cell.clone() }),
                Some(Disposition::DeadDeleted) if present => out.push(Violation::DeadButPresent(binary.into())),
                Some(Disposition::RetainedExample | Disposition::ExtractionInput) if !present => {
                    out.push(Violation::LiveButAbsent(binary.into()))
                }
                Some(_) => {}
            },
            Some(rows) => out.push(Violation::Duplicate { binary: binary.into(), rows: rows.len() }),
        }
    }
    out
}

/// Files that declare proof shapes: scenario YAML under any
/// `crates/*/scenarios/` directory, and the node-admin shape/topology sources.
pub fn proof_shape_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(crates) = std::fs::read_dir(root.join("crates")) else { return files };
    for krate in crates.flatten() {
        let scen = krate.path().join("scenarios");
        walk(&scen, &mut |p| {
            if matches!(p.extension().and_then(|e| e.to_str()), Some("yaml" | "yml")) {
                files.push(p.to_path_buf());
            }
        });
        let src = krate.path().join("src");
        walk(&src, &mut |p| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.starts_with("shape") || name == "topology.rs" {
                files.push(p.to_path_buf());
            }
        });
    }
    files.sort();
    files
}

fn walk(dir: &Path, f: &mut dyn FnMut(&Path)) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(&p, f);
        } else {
            f(&p);
        }
    }
}

/// A legacy role named as a whole word (`broker`, `rafka-broker`, `Broker`)
/// in a proof-shape declaration is a violation.
pub fn scan_proof_shape_text(file: &Path, text: &str) -> Vec<Violation> {
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let lower = line.to_ascii_lowercase();
        for &role in LEGACY_BINARIES {
            if contains_word(&lower, role) {
                out.push(Violation::LegacyRoleInProofShape {
                    file: file.to_path_buf(),
                    line: i + 1,
                    role: role.into(),
                });
            }
        }
    }
    out
}

fn contains_word(hay: &str, word: &str) -> bool {
    let is_ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    hay.match_indices(word).any(|(at, _)| {
        let before = hay[..at].chars().next_back();
        let after = hay[at + word.len()..].chars().next();
        !before.is_some_and(is_ident) && !after.is_some_and(is_ident)
    })
}

pub fn check_proof_shapes(root: &Path) -> Vec<Violation> {
    proof_shape_files(root)
        .into_iter()
        .flat_map(|f| {
            let text = std::fs::read_to_string(&f).unwrap_or_default();
            scan_proof_shape_text(&f, &text)
        })
        .collect()
}
