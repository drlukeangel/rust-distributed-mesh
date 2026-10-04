//! Mechanical transport-parity gate (i143.e0.s2).
//!
//! Implements the "Mechanical completeness gate" of rafka-v2
//! `docs/plans/i143-transport-parity.md`:
//!
//! 1. compute the rafka-v2 range from the fixed base through the proposed
//!    parity-through SHA (`git log base..through`, ancestry only, never dates);
//! 2. include the declared non-ancestor divergence inputs;
//! 3. identify every commit in the range touching the ledger's boundary paths;
//! 4. require exactly one disposition for every such commit;
//! 5. reject duplicate and ambiguous rows;
//! 6. reject eligibility while any row is `MIRROR pending`;
//! 7. record the parity-through SHA and the ledger digest.
//!
//! The ledger is the single source of truth for the base, the review-through
//! tip, the boundary paths and the rows; this module only reads it.

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The four dispositions the ledger allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Disposition {
    #[serde(rename = "MIRROR")]
    Mirror,
    #[serde(rename = "MIRROR pending")]
    MirrorPending,
    #[serde(rename = "RAFKA DOMAIN")]
    RafkaDomain,
    #[serde(rename = "RAFKA AUTH ONLY")]
    RafkaAuthOnly,
}

impl Disposition {
    fn parse(bold: &str) -> Option<Self> {
        match bold.trim() {
            "MIRROR" => Some(Self::Mirror),
            "MIRROR pending" => Some(Self::MirrorPending),
            "RAFKA DOMAIN" => Some(Self::RafkaDomain),
            "RAFKA AUTH ONLY" => Some(Self::RafkaAuthOnly),
            _ => None,
        }
    }
}

/// Which ledger table a row came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Section {
    /// `## Divergence inputs`: a commit that is NOT an ancestor of the base.
    Divergence,
    /// Any other table (the post-base ledger and later row tables).
    Range,
}

/// One row as written in the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerRow {
    pub sha_prefix: String,
    pub issue: String,
    pub change: String,
    /// Every bold `**...**` segment of the disposition cell that names a
    /// disposition. Exactly one is legal.
    pub dispositions: Vec<Disposition>,
    pub disposition_cell: String,
    pub section: Section,
    pub line: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ledger {
    pub base: Option<String>,
    pub review_through: Option<String>,
    pub boundary: Vec<String>,
    pub rows: Vec<LedgerRow>,
}

/// Parse the ledger markdown.
pub fn parse_ledger(text: &str) -> Ledger {
    let mut base = None;
    let mut review_through = None;
    let mut boundary = Vec::new();
    let mut rows = Vec::new();
    let mut heading = String::new();
    let mut in_boundary_block = false;

    for (i, line) in text.lines().enumerate() {
        let t = line.trim();
        if let Some(h) = t.strip_prefix("## ") {
            heading = h.trim().to_string();
            in_boundary_block = false;
            continue;
        }
        if t.starts_with("**Fixed audit base:**") {
            base = first_backticked(t);
        } else if t.starts_with("**Review-through tip") {
            review_through = first_backticked(t);
        }
        if heading == "Boundary" {
            if t.starts_with("```") {
                in_boundary_block = !in_boundary_block;
                continue;
            }
            if in_boundary_block {
                if let Some(tok) = t.split_whitespace().next() {
                    if let Some(prefix) = tok.strip_suffix("/**") {
                        boundary.push(prefix.to_string());
                    }
                }
            }
            continue;
        }
        if !t.starts_with('|') {
            continue;
        }
        let cells: Vec<&str> = t.trim_matches('|').split('|').map(str::trim).collect();
        if cells.len() < 4 {
            continue;
        }
        let Some(prefix) = cells[0].strip_prefix('`').and_then(|c| c.strip_suffix('`')) else {
            continue;
        };
        if !(7..=40).contains(&prefix.len()) || !prefix.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        let cell = cells[3..].join(" | ");
        rows.push(LedgerRow {
            sha_prefix: prefix.to_ascii_lowercase(),
            issue: cells[1].to_string(),
            change: cells[2].to_string(),
            dispositions: bold_segments(&cell).iter().filter_map(|b| Disposition::parse(b)).collect(),
            disposition_cell: cell,
            section: if heading == "Divergence inputs" { Section::Divergence } else { Section::Range },
            line: i + 1,
        });
    }
    Ledger { base, review_through, boundary, rows }
}

fn first_backticked(s: &str) -> Option<String> {
    let start = s.find('`')? + 1;
    let len = s[start..].find('`')?;
    Some(s[start..start + len].to_string())
}

fn bold_segments(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(a) = rest.find("**") {
        let after = &rest[a + 2..];
        let Some(b) = after.find("**") else { break };
        out.push(after[..b].to_string());
        rest = &after[b + 2..];
    }
    out
}

/// A gate failure. Every variant blocks the gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Violation {
    /// A boundary commit in the range has no row.
    Unclassified { sha: String, subject: String },
    /// Two or more rows resolve to the same commit.
    Duplicate { sha: String, lines: Vec<usize> },
    /// A row's disposition cell names zero or several dispositions.
    AmbiguousDisposition { sha_prefix: String, line: usize, cell: String },
    /// A row's SHA does not resolve to exactly one commit.
    UnresolvableSha { sha_prefix: String, line: usize, reason: String },
    /// A post-base row whose commit is an ancestor of the base.
    PreBaseRow { sha: String, line: usize },
    /// A divergence input that IS an ancestor of the base (ancestry rule).
    DivergenceIsBaseAncestor { sha: String, line: usize },
    /// A post-base row whose commit is not in `base..through`.
    RowOutsideRange { sha: String, line: usize },
    /// Rows in `MIRROR pending` block import eligibility.
    MirrorPending { sha: String, line: usize },
    /// The ledger names no base, or no boundary path, or a ref does not resolve.
    Ledger { reason: String },
    /// A connections.md §13 acceptance clause has no `## Connections parity` row.
    ConnectionsClauseUnmapped { clause: String },
    /// A connections row names no owner (an i143 story for MIRROR / MIRROR pending).
    ConnectionsRowWithoutOwner { clause: String, line: usize },
    /// A connections row names no carrying proof.
    ConnectionsRowWithoutProof { clause: String, line: usize },
    /// Two rows map the same connections clause.
    ConnectionsDuplicate { clause: String, lines: Vec<usize> },
    /// A MIRROR row (SHA or clause) that does not name the RDM SHA (`RDM `<sha>``).
    MirrorWithoutRdmProof { row: String, line: usize },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unclassified { sha, subject } => write!(f, "unclassified boundary commit {sha} ({subject})"),
            Self::Duplicate { sha, lines } => write!(f, "duplicate rows for {sha} at ledger lines {lines:?}"),
            Self::AmbiguousDisposition { sha_prefix, line, cell } => {
                write!(f, "line {line}: row {sha_prefix} does not name exactly one disposition: {cell}")
            }
            Self::UnresolvableSha { sha_prefix, line, reason } => {
                write!(f, "line {line}: row {sha_prefix} does not resolve to one commit: {reason}")
            }
            Self::PreBaseRow { sha, line } => {
                write!(f, "line {line}: {sha} is an ancestor of the base; it is not a post-base change")
            }
            Self::DivergenceIsBaseAncestor { sha, line } => write!(
                f,
                "line {line}: divergence input {sha} is an ancestor of the base; a divergence input must not be"
            ),
            Self::RowOutsideRange { sha, line } => {
                write!(f, "line {line}: {sha} is not in base..through and is not a declared divergence input")
            }
            Self::MirrorPending { sha, line } => write!(f, "line {line}: {sha} is MIRROR pending; import is blocked"),
            Self::Ledger { reason } => write!(f, "ledger: {reason}"),
            Self::ConnectionsClauseUnmapped { clause } => {
                write!(f, "connections clause {clause} (connections.md §13) has no Connections parity row")
            }
            Self::ConnectionsRowWithoutOwner { clause, line } => write!(
                f,
                "line {line}: connections row {clause} names no owner (MIRROR/MIRROR pending rows name an i143.eN.sM story)"
            ),
            Self::ConnectionsRowWithoutProof { clause, line } => {
                write!(f, "line {line}: connections row {clause} names no carrying proof")
            }
            Self::ConnectionsDuplicate { clause, lines } => {
                write!(f, "connections clause {clause} has rows at ledger lines {lines:?}")
            }
            Self::MirrorWithoutRdmProof { row, line } => {
                write!(f, "line {line}: row {row} is MIRROR but names no RDM SHA (`RDM `<sha>``) and carrying test")
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportRow {
    pub sha: String,
    pub sha_prefix: String,
    pub issue: String,
    pub disposition: Option<Disposition>,
    pub section: Section,
    pub in_range: bool,
    pub touches_boundary: bool,
    pub line: usize,
}

/// The machine-readable report (PRD §19 e0 artifact; import handoff fields).
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub rafka_parity_base: String,
    pub rafka_parity_through: String,
    pub parity_ledger_digest: String,
    pub boundary_paths: Vec<String>,
    pub boundary_commits: usize,
    pub rows: Vec<ReportRow>,
    pub connections: Vec<ConnectionsRow>,
    pub dispositions: BTreeMap<String, usize>,
    pub mirror_pending: usize,
    pub violations: Vec<Violation>,
    pub eligible: bool,
}

/// Inputs to one scan.
pub struct ScanInput<'a> {
    pub repo: &'a Path,
    pub ledger_text: &'a str,
    /// Overrides the ledger's review-through tip.
    pub through: Option<&'a str>,
    /// rafka-v2 `docs/architecture/connections.md`; `None` skips the
    /// connections-parity gate (the CLI always supplies it).
    pub connections_text: Option<&'a str>,
}

/// One `## Connections parity` row: an i66.e3 acceptance clause mapped to its
/// RDM owner story and carrying proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectionsRow {
    pub clause: String,
    pub source: String,
    pub owner: String,
    pub proof: String,
    pub disposition: Option<Disposition>,
    pub line: usize,
}

/// `CONN-<letter>` for every `### <letter>. ...` heading under
/// `## 13. Required acceptance tests` in connections.md.
pub fn required_connection_clauses(connections_md: &str) -> Vec<String> {
    let mut in_section = false;
    let mut out = Vec::new();
    for line in connections_md.lines() {
        let t = line.trim();
        if let Some(h) = t.strip_prefix("## ") {
            in_section = h.starts_with("13.") && h.contains("acceptance tests");
            continue;
        }
        if !in_section {
            continue;
        }
        if let Some(h) = t.strip_prefix("### ") {
            if let Some((letter, _)) = h.split_once('.') {
                if !letter.is_empty() && letter.chars().all(|c| c.is_ascii_uppercase()) {
                    out.push(format!("CONN-{letter}"));
                }
            }
        }
    }
    out
}

fn is_placeholder(cell: &str) -> bool {
    matches!(cell.trim().trim_matches('`'), "" | "-" | "—" | "TBD" | "tbd" | "?" | "none")
}

/// `i143.e<N>.s<M>` somewhere in the owner cell.
fn names_i143_story(owner: &str) -> bool {
    owner.match_indices("i143.e").any(|(at, _)| {
        let rest = &owner[at + "i143.e".len()..];
        let n = rest.chars().take_while(char::is_ascii_digit).count();
        n > 0 && rest[n..].starts_with(".s") && rest[n + 2..].chars().next().is_some_and(|c| c.is_ascii_digit())
    })
}

/// A MIRROR row records the RDM SHA as ``RDM `<hex>` ``.
fn names_rdm_sha(cell: &str) -> bool {
    cell.match_indices("RDM `").any(|(at, m)| {
        let rest = &cell[at + m.len()..];
        let n = rest.chars().take_while(char::is_ascii_hexdigit).count();
        (7..=40).contains(&n) && rest[n..].starts_with('`')
    })
}

/// Check the `## Connections parity` rows against connections.md §13.
pub fn check_connections(ledger_text: &str, connections_md: &str) -> (Vec<ConnectionsRow>, Vec<Violation>) {
    let mut rows = Vec::new();
    let mut violations = Vec::new();
    let mut in_section = false;
    for (i, line) in ledger_text.lines().enumerate() {
        let t = line.trim();
        if let Some(h) = t.strip_prefix("## ") {
            in_section = h.starts_with("Connections parity");
            continue;
        }
        if !in_section || !t.starts_with('|') {
            continue;
        }
        let cells: Vec<&str> = t.trim_matches('|').split('|').map(str::trim).collect();
        let Some(clause) = cells.first().and_then(|c| c.strip_prefix('`')).and_then(|c| c.strip_suffix('`')) else {
            continue;
        };
        if !clause.starts_with("CONN-") || cells.len() < 5 {
            continue;
        }
        let cell = cells[4..].join(" | ");
        let ds: Vec<Disposition> = bold_segments(&cell).iter().filter_map(|b| Disposition::parse(b)).collect();
        let row = ConnectionsRow {
            clause: clause.to_string(),
            source: cells[1].to_string(),
            owner: cells[2].to_string(),
            proof: cells[3].to_string(),
            disposition: if ds.len() == 1 { Some(ds[0]) } else { None },
            line: i + 1,
        };
        if ds.len() != 1 {
            violations.push(Violation::AmbiguousDisposition { sha_prefix: row.clause.clone(), line: row.line, cell: cell.clone() });
        }
        let needs_story = matches!(row.disposition, Some(Disposition::Mirror | Disposition::MirrorPending));
        if is_placeholder(&row.owner) || (needs_story && !names_i143_story(&row.owner)) {
            violations.push(Violation::ConnectionsRowWithoutOwner { clause: row.clause.clone(), line: row.line });
        }
        if is_placeholder(&row.proof) {
            violations.push(Violation::ConnectionsRowWithoutProof { clause: row.clause.clone(), line: row.line });
        }
        match row.disposition {
            Some(Disposition::MirrorPending) => {
                violations.push(Violation::MirrorPending { sha: row.clause.clone(), line: row.line })
            }
            Some(Disposition::Mirror) if !names_rdm_sha(&cell) && !names_rdm_sha(&row.proof) => {
                violations.push(Violation::MirrorWithoutRdmProof { row: row.clause.clone(), line: row.line })
            }
            _ => {}
        }
        rows.push(row);
    }
    let mut by_clause: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for r in &rows {
        by_clause.entry(&r.clause).or_default().push(r.line);
    }
    for (clause, lines) in &by_clause {
        if lines.len() > 1 {
            violations.push(Violation::ConnectionsDuplicate { clause: clause.to_string(), lines: lines.clone() });
        }
    }
    for clause in required_connection_clauses(connections_md) {
        if !by_clause.contains_key(clause.as_str()) {
            violations.push(Violation::ConnectionsClauseUnmapped { clause });
        }
    }
    if required_connection_clauses(connections_md).is_empty() {
        violations.push(Violation::Ledger {
            reason: "connections doc has no `## 13. Required acceptance tests` clauses".into(),
        });
    }
    (rows, violations)
}

struct Git<'a>(&'a Path);

impl Git<'_> {
    fn run(&self, args: &[&str]) -> Result<String, String> {
        let out = Command::new("git")
            .arg("-C")
            .arg(self.0)
            .args(args)
            .output()
            .map_err(|e| format!("git {}: {e}", args.join(" ")))?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
        } else {
            Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
        }
    }

    fn resolve(&self, rev: &str) -> Result<String, String> {
        self.run(&["rev-parse", "--verify", "--quiet", "--end-of-options", &format!("{rev}^{{commit}}")])
            .and_then(|s| if s.is_empty() { Err("unknown revision".into()) } else { Ok(s) })
            .map_err(|e| if e.is_empty() { "unknown revision".into() } else { e })
    }

    /// Ancestry by `git merge-base --is-ancestor`, never by date.
    fn is_ancestor(&self, a: &str, b: &str) -> bool {
        Command::new("git")
            .arg("-C")
            .arg(self.0)
            .args(["merge-base", "--is-ancestor", a, b])
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    fn touches(&self, sha: &str, boundary: &[String]) -> bool {
        let files = self.run(&["diff-tree", "--no-commit-id", "--name-only", "-r", "-m", sha]).unwrap_or_default();
        files.lines().any(|f| boundary.iter().any(|b| f == b || f.starts_with(&format!("{b}/"))))
    }

    fn subject(&self, sha: &str) -> String {
        self.run(&["log", "-1", "--format=%s", sha]).unwrap_or_default()
    }
}

/// Run the gate.
pub fn scan(input: &ScanInput<'_>) -> Report {
    let git = Git(input.repo);
    let ledger = parse_ledger(input.ledger_text);
    let digest: String = Sha256::digest(input.ledger_text.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
    let mut violations = Vec::new();

    let base = match ledger.base.as_deref().map(|b| git.resolve(b)) {
        Some(Ok(b)) => b,
        Some(Err(e)) => {
            violations.push(Violation::Ledger { reason: format!("fixed audit base does not resolve: {e}") });
            String::new()
        }
        None => {
            violations.push(Violation::Ledger { reason: "no **Fixed audit base:** line".into() });
            String::new()
        }
    };
    let through_ref = input.through.map(str::to_string).or(ledger.review_through.clone());
    let through = match through_ref.as_deref().map(|t| git.resolve(t)) {
        Some(Ok(t)) => t,
        Some(Err(e)) => {
            violations.push(Violation::Ledger { reason: format!("parity-through does not resolve: {e}") });
            String::new()
        }
        None => {
            violations.push(Violation::Ledger { reason: "no parity-through SHA given or in the ledger".into() });
            String::new()
        }
    };
    if ledger.boundary.is_empty() {
        violations.push(Violation::Ledger { reason: "no boundary paths under ## Boundary".into() });
    }

    // 1+3: boundary commits in base..through, by ancestry.
    let mut boundary_commits: BTreeSet<String> = BTreeSet::new();
    if !base.is_empty() && !through.is_empty() && !ledger.boundary.is_empty() {
        let mut args = vec!["log".to_string(), "--no-merges".into(), "--format=%H".into(), format!("{base}..{through}"), "--".into()];
        args.extend(ledger.boundary.iter().cloned());
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        match git.run(&args) {
            Ok(out) => boundary_commits.extend(out.lines().map(str::to_string)),
            Err(e) => violations.push(Violation::Ledger { reason: format!("git log base..through: {e}") }),
        }
    }

    // 2+4+5: resolve every row.
    let mut rows = Vec::new();
    let mut by_sha: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut mirror_pending = 0;
    let mut dispositions: BTreeMap<String, usize> = BTreeMap::new();
    for row in &ledger.rows {
        let disposition = match row.dispositions.as_slice() {
            [d] => Some(*d),
            _ => {
                violations.push(Violation::AmbiguousDisposition {
                    sha_prefix: row.sha_prefix.clone(),
                    line: row.line,
                    cell: row.disposition_cell.clone(),
                });
                None
            }
        };
        let sha = match git.resolve(&row.sha_prefix) {
            Ok(s) => s,
            Err(reason) => {
                violations.push(Violation::UnresolvableSha { sha_prefix: row.sha_prefix.clone(), line: row.line, reason });
                continue;
            }
        };
        by_sha.entry(sha.clone()).or_default().push(row.line);
        let base_ancestor = !base.is_empty() && git.is_ancestor(&sha, &base);
        let in_range = !base.is_empty() && !through.is_empty() && !base_ancestor && git.is_ancestor(&sha, &through);
        match row.section {
            Section::Divergence if base_ancestor => {
                violations.push(Violation::DivergenceIsBaseAncestor { sha: sha.clone(), line: row.line })
            }
            Section::Range if base_ancestor => violations.push(Violation::PreBaseRow { sha: sha.clone(), line: row.line }),
            Section::Range if !in_range && !base.is_empty() && !through.is_empty() => {
                violations.push(Violation::RowOutsideRange { sha: sha.clone(), line: row.line })
            }
            _ => {}
        }
        if let Some(d) = disposition {
            *dispositions.entry(serde_json::to_value(d).unwrap().as_str().unwrap().to_string()).or_default() += 1;
            if d == Disposition::Mirror && !names_rdm_sha(&row.disposition_cell) {
                violations.push(Violation::MirrorWithoutRdmProof { row: row.sha_prefix.clone(), line: row.line });
            }
            if d == Disposition::MirrorPending {
                mirror_pending += 1;
                violations.push(Violation::MirrorPending { sha: sha.clone(), line: row.line });
            }
        }
        rows.push(ReportRow {
            touches_boundary: git.touches(&sha, &ledger.boundary),
            sha,
            sha_prefix: row.sha_prefix.clone(),
            issue: row.issue.clone(),
            disposition,
            section: row.section,
            in_range,
            line: row.line,
        });
    }
    for (sha, lines) in &by_sha {
        if lines.len() > 1 {
            violations.push(Violation::Duplicate { sha: sha.clone(), lines: lines.clone() });
        }
    }
    let mut connections = Vec::new();
    if let Some(md) = input.connections_text {
        let (rows, v) = check_connections(input.ledger_text, md);
        mirror_pending += rows.iter().filter(|r| r.disposition == Some(Disposition::MirrorPending)).count();
        connections = rows;
        violations.extend(v);
    }
    for sha in &boundary_commits {
        if !by_sha.contains_key(sha) {
            violations.push(Violation::Unclassified { sha: sha.clone(), subject: git.subject(sha) });
        }
    }

    Report {
        rafka_parity_base: base,
        rafka_parity_through: through,
        parity_ledger_digest: format!("sha256:{digest}"),
        boundary_paths: ledger.boundary,
        boundary_commits: boundary_commits.len(),
        eligible: violations.is_empty(),
        rows,
        connections,
        dispositions,
        mirror_pending,
        violations,
    }
}

/// Read a ledger file and scan.
/// `connections` defaults to `<repo>/docs/architecture/connections.md`; an
/// unreadable connections doc is a gate violation, not a skip.
pub fn scan_files(repo: &Path, ledger: &Path, through: Option<&str>, connections: Option<&Path>) -> std::io::Result<Report> {
    let text = std::fs::read_to_string(ledger)?;
    let conn_path = connections.map(Path::to_path_buf).unwrap_or_else(|| default_connections(repo));
    let conn = std::fs::read_to_string(&conn_path);
    let mut report = scan(&ScanInput {
        repo,
        ledger_text: &text,
        through,
        connections_text: Some(conn.as_deref().unwrap_or("")),
    });
    if let Err(e) = conn {
        report.violations.push(Violation::Ledger {
            reason: format!("connections doc {} is unreadable: {e}", conn_path.display()),
        });
        report.eligible = false;
    }
    Ok(report)
}

/// Default connections architecture doc inside a rafka-v2 checkout.
pub fn default_connections(repo: &Path) -> PathBuf {
    repo.join("docs/architecture/connections.md")
}

/// Default ledger location inside a rafka-v2 checkout.
pub fn default_ledger(repo: &Path) -> PathBuf {
    repo.join("docs/plans/i143-transport-parity.md")
}
