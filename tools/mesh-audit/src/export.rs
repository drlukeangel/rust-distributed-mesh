//! The i143 export certifier (e10.s2, #2790; PRD §14, §15).
//!
//! i141 pins one exact RDM SHA. This module certifies that one candidate: it is eligible only when
//! the transport-parity scan ([`crate::parity::scan`]) is eligible and every owner's job receipt is
//! `ok` at that SHA. Every job in both sections of the acceptance registry
//! (`tools/mesh-audit/i143-acceptance-jobs.json`, `jobs` and `rshape_jobs`) is an owner, and its
//! receipt is the one `scripts/i143-acceptance-gate.sh` writes: `target/i143-acceptance/jobs/<job>.json`
//! for `jobs`, `target/i143-rshape/jobs/<job>.json` for `rshape_jobs`. A receipt certifies only when
//! its outcome is `ok` with no cell skipped by name (a skip proves nothing), it is no learning run's (`learning: true`), its `source_sha` is the candidate, it ran with no uncommitted path, and every artifact
//! it recorded still hashes to the sha256 it recorded. Anything else is refused by name, and every
//! refusal is named in one run, never just the first.
//!
//! The import manifest's §14 cells (`docs/plans/i143-import-manifest.md`) are proven by the gates'
//! own records at the candidate, not by registry jobs. `scripts/rdm-gate-at.sh` leaves, in the
//! deploy directory, `gates-<lane>-<short>.log` (fast) and `gates-<lane>-<short>release.log`
//! (release), each beside its `.log.d` (`jobs`: every job's executables and libtest arguments;
//! `artifacts.json`: each executable's package and target; `<job>.log`: its libtest summary), and
//! `discovery-<short>.json` (every test name per package and target). A gate log certifies only when
//! its first line names the candidate as its full HEAD with no uncommitted path, it ends `DONE` and
//! holds no `FAIL` line. A §14 cell is proven when it resolves to exactly one discovered test in its
//! path's package and a job of a certified log selects it (a filter matches, no `--skip` does) and
//! that job's libtest summaries are `ok` with nothing ignored. The gates run with
//! `RDM_REQUIRE_NETFAULT=1`, so a cell that cannot cut fails instead of passing with a named skip.

use crate::parity::{Report, Violation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

/// The two registry sections whose every job is an owner, and where the runner writes each one's receipt.
pub const SECTIONS: [(&str, &str); 2] = [("jobs", "target/i143-acceptance/jobs"), ("rshape_jobs", "target/i143-rshape/jobs")];

/// A job receipt as `scripts/i143-acceptance-gate.sh` writes it.
#[derive(Debug, Clone, Deserialize)]
pub struct Receipt {
    pub job: String,
    pub source_sha: String,
    pub dirty_paths: u64,
    pub outcome: String,
    /// Written by a learning soak run (`I143_SOAKS_LEARN=1`): it certifies nothing. A receipt written
    /// before the field reads as no learning run.
    #[serde(default)]
    pub learning: bool,
    pub cells: Vec<ReceiptCell>,
}

/// One executed cell of a receipt, with every artifact path and its sha256.
#[derive(Debug, Clone, Deserialize)]
pub struct ReceiptCell {
    pub name: String,
    pub outcome: String,
    /// The skip's name and reason, when the cell skipped by name.
    #[serde(default)]
    pub skip: Option<String>,
    #[serde(default)]
    pub artifacts: BTreeMap<String, String>,
}

/// Why a candidate is not certified. Every variant blocks the export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Refusal {
    /// The candidate is not one exact 40-hex commit SHA.
    CandidateNotASha { candidate: String },
    /// A parity ledger row is `MIRROR pending`.
    ParityMirrorPending { sha: String },
    /// A boundary commit in the parity range has no ledger row.
    ParityUnclassified { sha: String, subject: String },
    /// Any other parity gate failure.
    ParityIneligible { violation: String },
    /// The registry has no `jobs` section, or a section is not an object.
    Registry { reason: String },
    /// An owner's job has no receipt.
    ReceiptMissing { job: String, path: String },
    /// An owner's receipt does not read as the runner's receipt.
    ReceiptUnreadable { job: String, path: String, reason: String },
    /// The receipt at an owner's path names another job.
    ReceiptForAnotherJob { job: String, names: String },
    /// The receipt's outcome is not `ok`.
    ReceiptNotOk { job: String, outcome: String },
    /// The receipt was written at another source SHA than the candidate.
    ReceiptAtAnotherSha { job: String, source_sha: String },
    /// The receipt was written with uncommitted paths in its checkout.
    ReceiptDirty { job: String, dirty_paths: u64 },
    /// The receipt was written by a learning soak run (`learning: true`): it certifies nothing.
    ReceiptLearning { job: String },
    /// A cell of an `ok` receipt was skipped by name: it proved nothing.
    ReceiptCellSkipped { job: String, cell: String, skip: String },
    /// An artifact the receipt recorded is gone.
    ArtifactMissing { job: String, cell: String, path: String },
    /// An artifact the receipt recorded no longer hashes to its recorded sha256.
    ArtifactChanged { job: String, cell: String, path: String, recorded: String, now: String },
    /// The import manifest names no §14 cell.
    ManifestCells { reason: String },
    /// No log of this gate (`fast` or `release`) was written at the candidate.
    GateLogMissing { gate: String, pattern: String },
    /// A gate log at the candidate does not read as the gate's log, or its `.log.d` does not.
    GateUnreadable { log: String, reason: String },
    /// A gate log at the candidate names another full HEAD.
    GateAtAnotherSha { log: String, head: String },
    /// A gate log at the candidate was written with uncommitted paths in its checkout.
    GateDirty { log: String, dirty_paths: u64 },
    /// A gate log at the candidate failed jobs, or never reached `DONE`.
    GateRed { log: String, failed: Vec<String>, done: bool },
    /// The gate's test discovery at the candidate is missing.
    DiscoveryMissing { path: String },
    /// A §14 cell is not a test the gate discovered: every name it could be is listed.
    CellNotDiscovered { cell: String, looked_for: Vec<String> },
    /// A §14 cell names more than one discovered test.
    CellAmbiguous { cell: String, tests: Vec<String> },
    /// No job of a certified gate log ran a §14 cell.
    CellRunByNoJob { cell: String, test: String, reason: String },
    /// The only jobs that ran a §14 cell ignored tests: an ignored cell proves nothing.
    CellJobIgnored { cell: String, log: String, job: String, summary: String },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CandidateNotASha { candidate } => write!(f, "candidate {candidate:?} is not one exact 40-hex commit SHA"),
            Self::ParityMirrorPending { sha } => write!(f, "parity row {sha} is MIRROR pending"),
            Self::ParityUnclassified { sha, subject } => write!(f, "boundary commit {sha} ({subject}) has no parity row"),
            Self::ParityIneligible { violation } => write!(f, "parity: {violation}"),
            Self::Registry { reason } => write!(f, "registry: {reason}"),
            Self::ReceiptMissing { job, path } => write!(f, "{job}: no receipt at {path}"),
            Self::ReceiptUnreadable { job, path, reason } => write!(f, "{job}: receipt {path} does not read: {reason}"),
            Self::ReceiptForAnotherJob { job, names } => write!(f, "{job}: the receipt names job {names}"),
            Self::ReceiptNotOk { job, outcome } => write!(f, "{job}: the receipt's outcome is {outcome}, not ok"),
            Self::ReceiptAtAnotherSha { job, source_sha } => write!(f, "{job}: the receipt was written at {source_sha}, not the candidate"),
            Self::ReceiptDirty { job, dirty_paths } => write!(f, "{job}: the receipt was written with {dirty_paths} uncommitted paths in its checkout"),
            Self::ReceiptLearning { job } => write!(f, "{job}: the receipt was written by a learning run (learning: true); it certifies nothing"),
            Self::ReceiptCellSkipped { job, cell, skip } => write!(f, "{job}/{cell}: skipped by name ({skip}); a skip proves nothing"),
            Self::ArtifactMissing { job, cell, path } => write!(f, "{job}/{cell}: artifact {path} is missing"),
            Self::ArtifactChanged { job, cell, path, recorded, now } => {
                write!(f, "{job}/{cell}: artifact {path} hashes to {now}, the receipt recorded {recorded}")
            }
            Self::ManifestCells { reason } => write!(f, "import manifest: {reason}"),
            Self::GateLogMissing { gate, pattern } => write!(f, "{gate} gate: no log {pattern} at the candidate"),
            Self::GateUnreadable { log, reason } => write!(f, "gate log {log} does not read: {reason}"),
            Self::GateAtAnotherSha { log, head } => write!(f, "gate log {log} was written at {head}, not the candidate"),
            Self::GateDirty { log, dirty_paths } => write!(f, "gate log {log} was written with {dirty_paths} uncommitted paths in its checkout"),
            Self::GateRed { log, failed, done } => {
                write!(f, "gate log {log} is red: failed {failed:?}{}", if *done { "" } else { ", and it never reached DONE" })
            }
            Self::DiscoveryMissing { path } => write!(f, "the gate's test discovery {path} is missing"),
            Self::CellNotDiscovered { cell, looked_for } => write!(f, "§14 cell {cell} is no discovered test; looked for {looked_for:?}"),
            Self::CellAmbiguous { cell, tests } => write!(f, "§14 cell {cell} names more than one discovered test: {tests:?}"),
            Self::CellRunByNoJob { cell, test, reason } => write!(f, "§14 cell {cell} ({test}) was run by no job of a certified gate log: {reason}"),
            Self::CellJobIgnored { cell, log, job, summary } => {
                write!(f, "§14 cell {cell} was run only by {job} of {log}, which ignored tests ({summary}); an ignored cell proves nothing")
            }
        }
    }
}

/// One owner's receipt the manifest certifies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CertifiedReceipt {
    pub job: String,
    pub section: String,
    pub path: String,
    pub sha256: String,
}

/// One gate log the manifest certifies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CertifiedGateLog {
    /// `fast` or `release`.
    pub gate: String,
    pub path: String,
    pub sha256: String,
}

/// One §14 cell and the gate job that proved it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProvenCell {
    pub cell: String,
    /// The discovered test, `<package>/<target>: <libtest name>`.
    pub test: String,
    pub log: String,
    pub job: String,
}

/// The export manifest: the one candidate, the parity it was scanned through, every owner's receipt,
/// every gate log at the candidate and the §14 cells they proved.
#[derive(Debug, Clone, Serialize)]
pub struct ExportManifest {
    pub candidate_sha: String,
    pub rafka_parity_base: String,
    pub rafka_parity_through: String,
    pub parity_ledger_digest: String,
    pub mirror_pending: usize,
    pub owners: usize,
    pub receipts: Vec<CertifiedReceipt>,
    pub gate_logs: Vec<CertifiedGateLog>,
    pub cells: Vec<ProvenCell>,
    pub refusals: Vec<Refusal>,
    pub eligible: bool,
}

/// Inputs to one certification.
pub struct CertifyInput<'a> {
    /// The one candidate RDM SHA, 40 hex.
    pub candidate: &'a str,
    /// The parity scan's report.
    pub parity: &'a Report,
    /// The acceptance registry.
    pub registry: &'a serde_json::Value,
    /// The RDM root: receipts and their relative artifact paths resolve against it.
    pub root: &'a Path,
    /// The directory the gates write their logs and test discovery into (`$HOME/rust-distributed-mesh-deploy`).
    pub gates: &'a Path,
    /// The import manifest's §14 cells, each `<repo path>.rs::<test path>` ([`manifest_cells`]).
    pub cells: &'a [String],
}

fn is_sha(s: &str) -> bool {
    s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

fn sha256_of(path: &Path) -> std::io::Result<String> {
    Ok(Sha256::digest(std::fs::read(path)?).iter().map(|b| format!("{b:02x}")).collect())
}

fn parity_refusals(report: &Report) -> Vec<Refusal> {
    let mut out: Vec<Refusal> = report
        .violations
        .iter()
        .map(|v| match v {
            Violation::MirrorPending { sha, .. } => Refusal::ParityMirrorPending { sha: sha.clone() },
            Violation::Unclassified { sha, subject } => Refusal::ParityUnclassified { sha: sha.clone(), subject: subject.clone() },
            other => Refusal::ParityIneligible { violation: other.to_string() },
        })
        .collect();
    if out.is_empty() && !report.eligible {
        out.push(Refusal::ParityIneligible { violation: "the parity report is not eligible".into() });
    }
    out
}

/// The owners: every job of both registry sections (an `_`-prefixed key is a section's note).
pub fn owners(registry: &serde_json::Value) -> Result<Vec<(String, &'static str, &'static str)>, Refusal> {
    let mut out = Vec::new();
    for (section, dir) in SECTIONS {
        match registry.get(section) {
            None if section == "jobs" => return Err(Refusal::Registry { reason: "no `jobs` section".into() }),
            None => {}
            Some(serde_json::Value::Object(jobs)) => {
                out.extend(jobs.keys().filter(|k| !k.starts_with('_')).map(|k| (k.clone(), section, dir)));
            }
            Some(_) => return Err(Refusal::Registry { reason: format!("`{section}` is not an object") }),
        }
    }
    Ok(out)
}

fn check_receipt(job: &str, path: &Path, shown: &str, candidate: &str, root: &Path, refusals: &mut Vec<Refusal>) -> Option<String> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            refusals.push(Refusal::ReceiptMissing { job: job.into(), path: shown.into() });
            return None;
        }
        Err(e) => {
            refusals.push(Refusal::ReceiptUnreadable { job: job.into(), path: shown.into(), reason: e.to_string() });
            return None;
        }
    };
    let receipt: Receipt = match serde_json::from_slice(&bytes) {
        Ok(r) => r,
        Err(e) => {
            refusals.push(Refusal::ReceiptUnreadable { job: job.into(), path: shown.into(), reason: e.to_string() });
            return None;
        }
    };
    let before = refusals.len();
    if receipt.job != job {
        refusals.push(Refusal::ReceiptForAnotherJob { job: job.into(), names: receipt.job.clone() });
    }
    if receipt.outcome != "ok" {
        refusals.push(Refusal::ReceiptNotOk { job: job.into(), outcome: receipt.outcome.clone() });
    }
    if receipt.source_sha != candidate {
        refusals.push(Refusal::ReceiptAtAnotherSha { job: job.into(), source_sha: receipt.source_sha.clone() });
    }
    if receipt.dirty_paths != 0 {
        refusals.push(Refusal::ReceiptDirty { job: job.into(), dirty_paths: receipt.dirty_paths });
    }
    if receipt.learning {
        refusals.push(Refusal::ReceiptLearning { job: job.into() });
    }
    for cell in &receipt.cells {
        if cell.outcome == "skipped" || cell.skip.is_some() {
            let skip = cell.skip.clone().unwrap_or_else(|| "skipped".into());
            refusals.push(Refusal::ReceiptCellSkipped { job: job.into(), cell: cell.name.clone(), skip });
        }
        for (artifact, recorded) in &cell.artifacts {
            let p = Path::new(artifact);
            let full: PathBuf = if p.is_absolute() { p.to_path_buf() } else { root.join(p) };
            match sha256_of(&full) {
                Ok(now) if &now == recorded => {}
                Ok(now) => refusals.push(Refusal::ArtifactChanged {
                    job: job.into(),
                    cell: cell.name.clone(),
                    path: artifact.clone(),
                    recorded: recorded.clone(),
                    now,
                }),
                Err(_) => refusals.push(Refusal::ArtifactMissing { job: job.into(), cell: cell.name.clone(), path: artifact.clone() }),
            }
        }
    }
    (refusals.len() == before).then(|| Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect())
}

/// Certify one candidate: eligible only when the parity report is eligible and every owner's
/// receipt is `ok` at the candidate, with no uncommitted path, with every artifact still hashing.
pub fn certify(input: &CertifyInput<'_>) -> ExportManifest {
    let mut refusals = Vec::new();
    if !is_sha(input.candidate) {
        refusals.push(Refusal::CandidateNotASha { candidate: input.candidate.into() });
    }
    refusals.extend(parity_refusals(input.parity));
    let mut receipts = Vec::new();
    let owners = match owners(input.registry) {
        Ok(o) if o.is_empty() => {
            refusals.push(Refusal::Registry { reason: "the registry names no job".into() });
            o
        }
        Ok(o) => o,
        Err(r) => {
            refusals.push(r);
            Vec::new()
        }
    };
    for (job, section, dir) in &owners {
        let shown = format!("{dir}/{job}.json");
        let path = input.root.join(&shown);
        if let Some(sha256) = check_receipt(job, &path, &shown, input.candidate, input.root, &mut refusals) {
            receipts.push(CertifiedReceipt { job: job.clone(), section: section.to_string(), path: shown, sha256 });
        }
    }
    let (gate_logs, cells) = gate_proof(input, &mut refusals);
    ExportManifest {
        candidate_sha: input.candidate.into(),
        rafka_parity_base: input.parity.rafka_parity_base.clone(),
        rafka_parity_through: input.parity.rafka_parity_through.clone(),
        parity_ledger_digest: input.parity.parity_ledger_digest.clone(),
        mirror_pending: input.parity.mirror_pending,
        owners: owners.len(),
        eligible: refusals.is_empty(),
        receipts,
        gate_logs,
        cells,
        refusals,
    }
}

/// The gate logs at the candidate and the §14 cells they prove.
fn gate_proof(input: &CertifyInput<'_>, refusals: &mut Vec<Refusal>) -> (Vec<CertifiedGateLog>, Vec<ProvenCell>) {
    if input.cells.is_empty() {
        refusals.push(Refusal::ManifestCells { reason: "the §14 section names no cell".into() });
    }
    let logs = gate_logs_at(input.gates, input.candidate);
    let mut certified = Vec::new();
    let mut good: Vec<GateRecord> = Vec::new();
    let mut discovery_missing: Vec<String> = Vec::new();
    for gate in ["fast", "release"] {
        let of_gate: Vec<&(String, &str, String)> = logs.iter().filter(|(_, g, _)| *g == gate).collect();
        if of_gate.is_empty() {
            let short = &input.candidate[..input.candidate.len().min(8)];
            let suffix = if gate == "release" { "release" } else { "" };
            refusals.push(Refusal::GateLogMissing { gate: gate.into(), pattern: format!("gates-<lane>-{short}{suffix}.log") });
        }
        for (name, gate, short) in of_gate {
            let path = input.gates.join(name);
            let before = refusals.len();
            let text = match std::fs::read_to_string(&path) {
                Ok(t) => t,
                Err(e) => {
                    refusals.push(Refusal::GateUnreadable { log: name.clone(), reason: e.to_string() });
                    continue;
                }
            };
            let first = text.lines().next().unwrap_or_default();
            match gate_head(first) {
                None => refusals.push(Refusal::GateUnreadable {
                    log: name.clone(),
                    reason: format!("its first line is not `[gate] head=<40-hex> dirty=<n>`: {first:?}"),
                }),
                Some((head, dirty)) => {
                    if head != input.candidate {
                        refusals.push(Refusal::GateAtAnotherSha { log: name.clone(), head });
                    }
                    if dirty != 0 {
                        refusals.push(Refusal::GateDirty { log: name.clone(), dirty_paths: dirty });
                    }
                }
            }
            let failed: Vec<String> =
                text.lines().filter(|l| l.starts_with("FAIL")).filter_map(|l| l.split_whitespace().nth(1)).map(str::to_string).collect();
            let done = text.lines().any(|l| l.starts_with("DONE"));
            if !failed.is_empty() || !done {
                refusals.push(Refusal::GateRed { log: name.clone(), failed, done });
            }
            let discovery_name = format!("discovery-{short}.json");
            let discovery = match std::fs::read(input.gates.join(&discovery_name)) {
                Ok(b) => match serde_json::from_slice::<serde_json::Value>(&b) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        refusals.push(Refusal::GateUnreadable { log: name.clone(), reason: format!("{discovery_name}: {e}") });
                        None
                    }
                },
                Err(_) => {
                    if !discovery_missing.contains(&discovery_name) {
                        refusals.push(Refusal::DiscoveryMissing { path: discovery_name.clone() });
                        discovery_missing.push(discovery_name);
                    }
                    None
                }
            };
            let dir = input.gates.join(format!("{name}.d"));
            let read = |file: &str, refusals: &mut Vec<Refusal>| match std::fs::read_to_string(dir.join(file)) {
                Ok(t) => Some(t),
                Err(e) => {
                    refusals.push(Refusal::GateUnreadable { log: name.clone(), reason: format!("{name}.d/{file}: {e}") });
                    None
                }
            };
            let artifacts = read("artifacts.json", refusals);
            let jobs = read("jobs", refusals);
            if refusals.len() != before {
                continue;
            }
            let (Some(discovery), Some(artifacts), Some(jobs)) = (discovery, artifacts, jobs) else { continue };
            let exes = test_executables(&artifacts);
            let jobs = parse_jobs(&jobs, &exes);
            let sha256 = match sha256_of(&path) {
                Ok(s) => s,
                Err(e) => {
                    refusals.push(Refusal::GateUnreadable { log: name.clone(), reason: e.to_string() });
                    continue;
                }
            };
            certified.push(CertifiedGateLog { gate: gate.to_string(), path: name.clone(), sha256 });
            good.push(GateRecord { name: name.clone(), dir, discovery, exes, jobs });
        }
    }
    let mut proven = Vec::new();
    if let Some(first) = good.first() {
        for cell in input.cells {
            match resolve_cell(cell, first) {
                Err(r) => refusals.push(r),
                Ok((package, target, name)) => match prove_cell(&good, &package, &target, &name, ignored_in_source(input.root, cell)) {
                    Ok((log, job)) => proven.push(ProvenCell { cell: cell.clone(), test: format!("{package}/{target}: {name}"), log, job }),
                    Err(CellUnproven::Ignored { log, job, summary }) => refusals.push(Refusal::CellJobIgnored { cell: cell.clone(), log, job, summary }),
                    Err(CellUnproven::NoJob(reason)) => {
                        refusals.push(Refusal::CellRunByNoJob { cell: cell.clone(), test: format!("{package}/{target}: {name}"), reason })
                    }
                },
            }
        }
    }
    (certified, proven)
}

/// One certified gate log, read: its `.log.d`, its test discovery, its build's test executables and its jobs.
struct GateRecord {
    name: String,
    dir: PathBuf,
    discovery: serde_json::Value,
    exes: Vec<TestExe>,
    jobs: Vec<GateJob>,
}

/// One test executable of the gate's build (`artifacts.json`).
struct TestExe {
    package: String,
    target: String,
    /// The package's directory, as the gate's checkout spells it.
    crate_dir: String,
    executable: String,
}

/// One gate job (`jobs`): its name and every test executable its command runs, with that run's libtest arguments.
struct GateJob {
    name: String,
    runs: Vec<Run>,
}

/// One test executable a job runs and the libtest arguments it gives it.
struct Run {
    package: String,
    target: String,
    filters: Vec<String>,
    skips: Vec<String>,
    exact: bool,
}

impl Run {
    /// Whether libtest selects `name`: a filter matches (or there is none) and no `--skip` does.
    fn selects(&self, name: &str) -> Result<(), String> {
        let matches = |p: &str| if self.exact { name == p } else { name.contains(p) };
        if !self.filters.is_empty() && !self.filters.iter().any(|f| matches(f)) {
            return Err(format!("its filters {:?} do not match", self.filters));
        }
        match self.skips.iter().find(|s| matches(s)) {
            Some(s) => Err(format!("--skip {s} excludes it")),
            None => Ok(()),
        }
    }
}

/// A gate log at the candidate: every `gates-<lane>-<short>.log` (fast) and `gates-<lane>-<short>release.log`
/// (release) in `dir` whose short SHA is a prefix of the candidate, as (file name, gate, short).
fn gate_logs_at(dir: &Path, candidate: &str) -> Vec<(String, &'static str, String)> {
    let mut out: Vec<(String, &'static str, String)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let stem = name.strip_prefix("gates-")?.strip_suffix(".log")?;
            let (stem, gate) = match stem.strip_suffix("release") {
                Some(s) => (s, "release"),
                None => (stem, "fast"),
            };
            let (_, short) = stem.rsplit_once('-')?;
            let hex = short.len() >= 7 && short.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase());
            (hex && candidate.starts_with(short)).then(|| (name.clone(), gate, short.to_string()))
        })
        .collect();
    out.sort();
    out
}

/// A gate log's first line, `[gate] head=<40-hex> dirty=<n>`.
fn gate_head(line: &str) -> Option<(String, u64)> {
    let rest = line.strip_prefix("[gate] head=")?;
    let (head, dirty) = rest.split_once(" dirty=")?;
    (is_sha(head)).then_some(())?;
    Some((head.to_string(), dirty.trim().parse().ok()?))
}

/// A package id's name, as `scripts/rdm-gate-at.sh` reads it: `<source>#<name>@<version>`, or `<source>/<name>#<version>`.
fn package_of(package_id: &str) -> String {
    match package_id.split_once('#') {
        Some((_, after)) if after.contains('@') => after.split('@').next().unwrap_or_default().to_string(),
        _ => package_id.split('#').next().unwrap_or_default().trim_end_matches('/').rsplit('/').next().unwrap_or_default().to_string(),
    }
}

/// Every libtest executable of the gate's build: a compiler artifact with an executable, built for test.
fn test_executables(artifacts: &str) -> Vec<TestExe> {
    artifacts
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|m| m["reason"] == "compiler-artifact" && m["profile"]["test"] == true)
        .filter_map(|m| {
            let executable = m["executable"].as_str().filter(|e| !e.is_empty())?.to_string();
            let manifest = m["manifest_path"].as_str()?;
            Some(TestExe {
                package: package_of(m["package_id"].as_str()?),
                target: m["target"]["name"].as_str()?.to_string(),
                crate_dir: manifest.rsplit_once('/').map(|(d, _)| d).unwrap_or_default().to_string(),
                executable,
            })
        })
        .collect()
}

/// Every job of the gate's `jobs` file and the test executables its command runs.
fn parse_jobs(text: &str, exes: &[TestExe]) -> Vec<GateJob> {
    text.lines()
        .filter_map(|line| {
            let (name, cmd) = line.split_once(' ')?;
            let mut at: Vec<(usize, &TestExe)> = exes
                .iter()
                .flat_map(|e| {
                    cmd.match_indices(e.executable.as_str())
                        .filter(|(i, _)| cmd[i + e.executable.len()..].chars().next().is_none_or(|c| c.is_whitespace() || ")&;|".contains(c)))
                        .map(move |(i, _)| (i, e))
                })
                .collect();
            at.sort_by_key(|(i, _)| *i);
            at.dedup_by_key(|(i, _)| *i);
            let runs = at.into_iter().map(|(i, e)| run_of(e, &cmd[i + e.executable.len()..])).collect();
            Some(GateJob { name: name.to_string(), runs })
        })
        .collect()
}

/// The libtest arguments after one executable in a job's command, up to the shell's next separator.
fn run_of(exe: &TestExe, args: &str) -> Run {
    let mut run = Run { package: exe.package.clone(), target: exe.target.clone(), filters: Vec::new(), skips: Vec::new(), exact: false };
    let cut = |t: &str| match t.find(|c| ")&;|".contains(c)) {
        Some(i) => (t[..i].to_string(), true),
        None => (t.to_string(), false),
    };
    let mut tokens = args.split_whitespace();
    while let Some(t) = tokens.next() {
        let (t, stop) = cut(t);
        if t == "--skip" && !stop {
            if let Some(n) = tokens.next() {
                let (n, stop) = cut(n);
                if !n.is_empty() {
                    run.skips.push(n);
                }
                if stop {
                    break;
                }
            }
        } else if t == "--exact" {
            run.exact = true;
        } else if !t.is_empty() && !t.starts_with('-') {
            run.filters.push(t);
        }
        if stop {
            break;
        }
    }
    run
}

/// A §14 cell's package, discovery target and libtest name: a `src/` file is a module of its
/// package's lib; a `tests/` file is its own test executable or a module of the package's `main`.
fn resolve_cell(cell: &str, gate: &GateRecord) -> Result<(String, String, String), Refusal> {
    let not = |looked_for: Vec<String>| Refusal::CellNotDiscovered { cell: cell.to_string(), looked_for };
    let Some((file, rest)) = cell.split_once("::") else { return Err(not(vec!["a cell is `<path>.rs::<test path>`".into()])) };
    let split = ["/src/", "/tests/"].iter().filter_map(|s| file.find(s).map(|i| (i, *s))).min();
    let Some((i, sep)) = split else { return Err(not(vec![format!("{file} is in no package's src/ or tests/")])) };
    let (crate_dir, inner) = (&file[..i], &file[i + sep.len()..]);
    let Some(package) = gate.exes.iter().find(|e| e.crate_dir == crate_dir || e.crate_dir.ends_with(&format!("/{crate_dir}"))).map(|e| e.package.clone())
    else {
        return Err(not(vec![format!("no package of the gate's build is at {crate_dir}")]));
    };
    let targets = gate.discovery.get(&package).and_then(|t| t.as_object()).cloned().unwrap_or_default();
    let module = inner.strip_suffix(".rs").unwrap_or(inner);
    let mut candidates: Vec<(String, String)> = Vec::new();
    if sep == "/src/" {
        let module = match module {
            "lib" | "main" => String::new(),
            m => m.strip_suffix("/mod").unwrap_or(m).replace('/', "::"),
        };
        let name = if module.is_empty() { rest.to_string() } else { format!("{module}::{rest}") };
        for (t, v) in &targets {
            if v["kind"].as_str().unwrap_or_default().split(',').any(|k| k == "lib") {
                candidates.push((t.clone(), name.clone()));
            }
        }
    } else {
        let stem = module.rsplit('/').next().unwrap_or(module);
        if targets.get(stem).is_some_and(|v| v["kind"].as_str().unwrap_or_default().split(',').any(|k| k == "test")) {
            candidates.push((stem.to_string(), rest.to_string()));
        }
        if targets.contains_key("main") {
            candidates.push(("main".to_string(), format!("{stem}::{rest}")));
        }
    }
    let show = |(t, n): &(String, String)| format!("{package}/{t}: {n}");
    let found: Vec<&(String, String)> = candidates
        .iter()
        .filter(|(t, n)| targets[t.as_str()]["tests"].as_array().is_some_and(|a| a.iter().any(|x| x.as_str() == Some(n.as_str()))))
        .collect();
    match found.as_slice() {
        [] => Err(not(if candidates.is_empty() { vec![format!("{package} has no target a {sep} file runs in")] } else { candidates.iter().map(show).collect() })),
        [(t, n)] => Ok((package.clone(), t.clone(), n.clone())),
        many => Err(Refusal::CellAmbiguous { cell: cell.to_string(), tests: many.iter().map(|c| show(c)).collect() }),
    }
}

/// Why no job of a certified gate log proved a cell.
enum CellUnproven {
    /// Every job that ran it ignored tests: the first.
    Ignored { log: String, job: String, summary: String },
    NoJob(String),
}

/// One libtest summary line of a job's log.
struct Summary {
    line: String,
    ok: bool,
    failed: u64,
    ignored: u64,
}

fn summaries(log: &str) -> Vec<Summary> {
    log.lines()
        .filter_map(|l| l.find("test result: ").map(|i| l[i..].trim_end().to_string()))
        .map(|line| {
            let body = &line["test result: ".len()..];
            let count = |what: &str| {
                body.split(';').map(str::trim).find_map(|p| p.strip_suffix(what)).and_then(|n| n.trim().rsplit(' ').next()?.parse().ok()).unwrap_or(0)
            };
            Summary { ok: body.starts_with("ok."), failed: count(" failed"), ignored: count(" ignored"), line }
        })
        .collect()
}

/// Whether the §14 `cell` (`<file>.rs::<test path>`, under `root`) is itself `#[ignore]`d in its source:
/// only then does a run's ignored count say it did not run. Libtest ignores no test but an `#[ignore]`d
/// one, so the ignored tests of a run that selects a cell carrying no `#[ignore]` are other cells.
/// A source that cannot be read is read as ignored: nothing is proven from it.
pub fn ignored_in_source(root: &Path, cell: &str) -> bool {
    let Some((file, path)) = cell.split_once(".rs::") else { return true };
    let Ok(text) = std::fs::read_to_string(root.join(format!("{file}.rs"))) else { return true };
    let name = path.rsplit("::").next().unwrap_or(path);
    let lines: Vec<&str> = text.lines().collect();
    let Some(at) = lines.iter().position(|l| l.contains(&format!("fn {name}("))) else { return true };
    lines[..at].iter().rev().map(|l| l.trim()).take_while(|l| l.starts_with("#[") || l.starts_with("///") || l.starts_with("//")).any(|l| l.starts_with("#[ignore"))
}

/// The first job of a certified log that selects the test and whose summaries are clean, as (log, job).
/// A summary that ignored tests proves the cell only when the cell itself is not `#[ignore]`d
/// (`cell_ignored`, [`ignored_in_source`]).
fn prove_cell(gates: &[GateRecord], package: &str, target: &str, name: &str, cell_ignored: bool) -> Result<(String, String), CellUnproven> {
    let mut ignored: Option<CellUnproven> = None;
    let mut why: Vec<String> = Vec::new();
    for gate in gates {
        for job in &gate.jobs {
            let runs: Vec<&Run> = job.runs.iter().filter(|r| r.package == package && r.target == target).collect();
            if runs.is_empty() {
                continue;
            }
            let refused: Vec<String> = runs.iter().filter_map(|r| r.selects(name).err()).collect();
            if refused.len() == runs.len() {
                why.push(format!("{} ({}): {}", job.name, gate.name, refused.join(", ")));
                continue;
            }
            let text = std::fs::read_to_string(gate.dir.join(format!("{}.log", job.name))).unwrap_or_default();
            let sums = summaries(&text);
            if sums.len() < job.runs.len() {
                why.push(format!("{} ({}) selects it, but its log holds {} libtest summaries for {} executables", job.name, gate.name, sums.len(), job.runs.len()));
                continue;
            }
            if let Some(s) = sums.iter().find(|s| s.ignored > 0).filter(|_| cell_ignored) {
                ignored.get_or_insert(CellUnproven::Ignored { log: gate.name.clone(), job: job.name.clone(), summary: s.line.clone() });
                continue;
            }
            if let Some(s) = sums.iter().find(|s| !s.ok || s.failed > 0) {
                why.push(format!("{} ({}) selects it, but its summary is not clean: {}", job.name, gate.name, s.line));
                continue;
            }
            return Ok((gate.name.clone(), job.name.clone()));
        }
    }
    Err(ignored.unwrap_or_else(|| CellUnproven::NoJob(if why.is_empty() { "no job runs its executable".into() } else { why.join("; ") })))
}

/// The import manifest's §14 cells: every `<path>.rs::<test path>` its §14 section names, `T/` and
/// `A/` written out and a `…::<test path>` taking the file before it.
pub fn manifest_cells(text: &str) -> Vec<String> {
    let expand = |f: &str| match (f.strip_prefix("T/"), f.strip_prefix("A/")) {
        (Some(r), _) => format!("crates/rdm-test/tests/{r}"),
        (_, Some(r)) => format!("crates/rdm-admin/{r}"),
        _ => f.to_string(),
    };
    let mut out: Vec<String> = Vec::new();
    let mut in_section = false;
    let mut last_file: Option<String> = None;
    for line in text.lines() {
        if line.starts_with("## ") {
            in_section = line.starts_with("## §14");
            last_file = None;
            continue;
        }
        if !in_section {
            continue;
        }
        for tok in line.split('`').skip(1).step_by(2) {
            let (file, rest) = if let Some(rest) = tok.strip_prefix("…::") {
                match &last_file {
                    Some(f) => (f.clone(), rest),
                    None => continue,
                }
            } else {
                let (f, rest) = tok.split_once("::").unwrap_or((tok, ""));
                let f = expand(f);
                if !f.ends_with(".rs") {
                    continue;
                }
                (f, rest)
            };
            last_file = Some(file.clone());
            if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':') {
                continue;
            }
            let cell = format!("{file}::{rest}");
            if !out.contains(&cell) {
                out.push(cell);
            }
        }
    }
    out
}

/// The import manifest the certifier reads the §14 cells from.
pub fn default_manifest() -> PathBuf {
    crate::workspace_root().join("docs/plans/i143-import-manifest.md")
}

/// The directory the gates write their logs and test discovery into.
pub fn default_gates() -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join("rust-distributed-mesh-deploy")
}

/// The registry the acceptance runner reads.
pub fn default_registry() -> PathBuf {
    crate::workspace_root().join("tools/mesh-audit/i143-acceptance-jobs.json")
}
