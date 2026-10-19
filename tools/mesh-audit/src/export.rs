//! The i143 export certifier (e10.s2, #2790; PRD §14, §15).
//!
//! i141 pins one exact RDM SHA. This module certifies that one candidate: it is eligible only when
//! the transport-parity scan ([`crate::parity::scan`]) is eligible and every owner's job receipt is
//! `ok` at that SHA. Every job in both sections of the acceptance registry
//! (`tools/mesh-audit/i143-acceptance-jobs.json`, `jobs` and `rshape_jobs`) is an owner, and its
//! receipt is the one `scripts/i143-acceptance-gate.sh` writes: `target/i143-acceptance/jobs/<job>.json`
//! for `jobs`, `target/i143-rshape/jobs/<job>.json` for `rshape_jobs`. A receipt certifies only when
//! its outcome is `ok` with no cell skipped by name (a skip proves nothing), its `source_sha` is the candidate, it ran with no uncommitted path, and every artifact
//! it recorded still hashes to the sha256 it recorded. Anything else is refused by name, and every
//! refusal is named in one run, never just the first.

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
    /// A cell of an `ok` receipt was skipped by name: it proved nothing.
    ReceiptCellSkipped { job: String, cell: String, skip: String },
    /// An artifact the receipt recorded is gone.
    ArtifactMissing { job: String, cell: String, path: String },
    /// An artifact the receipt recorded no longer hashes to its recorded sha256.
    ArtifactChanged { job: String, cell: String, path: String, recorded: String, now: String },
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
            Self::ReceiptCellSkipped { job, cell, skip } => write!(f, "{job}/{cell}: skipped by name ({skip}); a skip proves nothing"),
            Self::ArtifactMissing { job, cell, path } => write!(f, "{job}/{cell}: artifact {path} is missing"),
            Self::ArtifactChanged { job, cell, path, recorded, now } => {
                write!(f, "{job}/{cell}: artifact {path} hashes to {now}, the receipt recorded {recorded}")
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

/// The export manifest: the one candidate, the parity it was scanned through, every owner's receipt.
#[derive(Debug, Clone, Serialize)]
pub struct ExportManifest {
    pub candidate_sha: String,
    pub rafka_parity_base: String,
    pub rafka_parity_through: String,
    pub parity_ledger_digest: String,
    pub mirror_pending: usize,
    pub owners: usize,
    pub receipts: Vec<CertifiedReceipt>,
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
    ExportManifest {
        candidate_sha: input.candidate.into(),
        rafka_parity_base: input.parity.rafka_parity_base.clone(),
        rafka_parity_through: input.parity.rafka_parity_through.clone(),
        parity_ledger_digest: input.parity.parity_ledger_digest.clone(),
        mirror_pending: input.parity.mirror_pending,
        owners: owners.len(),
        eligible: refusals.is_empty(),
        receipts,
        refusals,
    }
}

/// The registry the acceptance runner reads.
pub fn default_registry() -> PathBuf {
    crate::workspace_root().join("tools/mesh-audit/i143-acceptance-jobs.json")
}
