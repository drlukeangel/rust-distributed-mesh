//! i143.e0.s2 acceptance: the parity validator FAILS on an unclassified
//! boundary commit, a duplicate or ambiguous row, or any `MIRROR pending`;
//! the range and the ancestry rule use `git merge-base --is-ancestor`, never
//! dates. Every case runs against a throwaway fixture git repo.

use rafka_mesh_audit::parity::{self, Disposition, ScanInput, Violation};
use std::path::{Path, PathBuf};
use std::process::Command;

struct Repo {
    dir: PathBuf,
    /// Pre-base commit with a FUTURE date (an ancestor of the base).
    pre: String,
    base: String,
    /// Post-base transport change.
    c1: String,
    /// Post-base node-base change.
    c2: String,
    /// Post-base docs-only change (off the boundary).
    c3: String,
    /// Post-base boundary change with a 2001 date (in range by ancestry).
    old_dated: String,
    /// Side-branch boundary change forked before the base, merged after it.
    diverge: String,
    tip: String,
}

fn git(dir: &Path, args: &[&str], date: Option<&str>) -> String {
    let mut c = Command::new("git");
    c.arg("-C").arg(dir).args(args);
    c.env("GIT_AUTHOR_NAME", "t").env("GIT_AUTHOR_EMAIL", "t@t").env("GIT_COMMITTER_NAME", "t").env("GIT_COMMITTER_EMAIL", "t@t");
    if let Some(d) = date {
        c.env("GIT_AUTHOR_DATE", d).env("GIT_COMMITTER_DATE", d);
    }
    let out = c.output().expect("git runs");
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn commit(dir: &Path, file: &str, msg: &str, date: Option<&str>) -> String {
    let p = dir.join(file);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    let prev = std::fs::read_to_string(&p).unwrap_or_default();
    std::fs::write(&p, format!("{prev}{msg}\n")).unwrap();
    git(dir, &["add", "-A"], None);
    git(dir, &["commit", "-q", "-m", msg], date);
    git(dir, &["rev-parse", "HEAD"], None)
}

fn fixture() -> Repo {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!("parity-fixture-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    git(&dir, &["init", "-q", "-b", "main"], None);
    commit(&dir, "README.md", "root", Some("2020-01-01T00:00:00Z"));
    let pre = commit(&dir, "crates/rafka-mesh-ops/src/f.rs", "pre-base framing", Some("2099-01-01T00:00:00Z"));
    // The side branch forks here, before the base.
    git(&dir, &["branch", "side"], None);
    let base = commit(&dir, "crates/rafka-mesh-transport/src/lib.rs", "base", Some("2020-02-01T00:00:00Z"));
    let c1 = commit(&dir, "crates/rafka-mesh-transport/src/lib.rs", "c1 transport", Some("2020-03-01T00:00:00Z"));
    let c2 = commit(&dir, "crates/rafka-node-base/src/lib.rs", "c2 node-base", Some("2020-03-02T00:00:00Z"));
    let c3 = commit(&dir, "docs/x.md", "c3 docs only", Some("2020-03-03T00:00:00Z"));
    let old_dated = commit(&dir, "crates/rafka-reach-reroute/src/lib.rs", "old-dated reroute", Some("2001-01-01T00:00:00Z"));
    git(&dir, &["checkout", "-q", "side"], None);
    let diverge = commit(&dir, "crates/rafka-node-base/src/pool.rs", "diverge pool", Some("2019-06-01T00:00:00Z"));
    git(&dir, &["checkout", "-q", "main"], None);
    git(&dir, &["merge", "-q", "--no-ff", "-m", "merge side", "side"], Some("2020-04-01T00:00:00Z"));
    let tip = git(&dir, &["rev-parse", "HEAD"], None);
    Repo { dir, pre, base, c1, c2, c3, old_dated, diverge, tip }
}

impl Drop for Repo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn short(s: &str) -> &str {
    &s[..10]
}

fn ledger(r: &Repo, divergence: &str, rows: &str) -> String {
    format!(
        "# ledger\n\n**Fixed audit base:** `{base}`  \n**Review-through tip for this reconciliation:** `{tip}`\n\n## Boundary\n\n```text\ncrates/rafka-mesh-transport/**\ncrates/rafka-node-base/** transport-facing code:\n  bi-stream dispatch\ncrates/rafka-reach-reroute/**\ncrates/rafka-mesh-ops/** framing\n```\n\n## Divergence inputs\n\n| Rafka SHA | issue | change | disposition / proof |\n|---|---|---|---|\n{divergence}\n## Post-base ledger\n\n| Rafka SHA | issue | change | disposition / proof |\n|---|---|---|---|\n{rows}",
        base = short(&r.base),
        tip = short(&r.tip),
    )
}

fn row(sha: &str, disp: &str) -> String {
    let proof = if disp == "**MIRROR**" { "RDM `0123abcd99`, test `carrying_test`" } else { "proof text" };
    format!("| `{}` | #1 | change | {disp}. {proof} |\n", short(sha))
}

fn complete(r: &Repo) -> (String, String) {
    (
        row(&r.diverge, "**RAFKA DOMAIN**"),
        [row(&r.c1, "**MIRROR**"), row(&r.c2, "**RAFKA DOMAIN**"), row(&r.old_dated, "**RAFKA AUTH ONLY**")].concat(),
    )
}

fn scan(r: &Repo, text: &str) -> parity::Report {
    parity::scan(&ScanInput { repo: &r.dir, ledger_text: text, through: None, connections_text: None })
}

#[test]
fn complete_ledger_is_eligible() {
    let r = fixture();
    let (d, rows) = complete(&r);
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert_eq!(rep.violations, vec![], "{:#?}", rep.violations);
    assert!(rep.eligible);
    assert_eq!(rep.mirror_pending, 0);
    assert_eq!(rep.boundary_commits, 4, "c1, c2, old-dated, diverge");
    assert_eq!(rep.rafka_parity_base, r.base);
    assert_eq!(rep.rafka_parity_through, r.tip);
    assert!(rep.parity_ledger_digest.starts_with("sha256:") && rep.parity_ledger_digest.len() == 7 + 64);
}

#[test]
fn unclassified_boundary_commit_fails() {
    let r = fixture();
    let (d, _) = complete(&r);
    let rows = [row(&r.c1, "**MIRROR**"), row(&r.old_dated, "**RAFKA DOMAIN**")].concat();
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert!(!rep.eligible);
    assert_eq!(rep.violations, vec![Violation::Unclassified { sha: r.c2.clone(), subject: "c2 node-base".into() }]);
}

#[test]
fn off_boundary_commit_needs_no_row() {
    let r = fixture();
    let (d, rows) = complete(&r);
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert!(rep.rows.iter().all(|x| x.sha != r.c3));
    assert!(!rep.violations.iter().any(|v| matches!(v, Violation::Unclassified { sha, .. } if *sha == r.c3)));
}

#[test]
fn duplicate_row_fails() {
    let r = fixture();
    let (d, rows) = complete(&r);
    let rows = format!("{rows}{}", row(&r.c1, "**RAFKA DOMAIN**"));
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert!(!rep.eligible);
    assert!(matches!(&rep.violations[..], [Violation::Duplicate { sha, lines }] if *sha == r.c1 && lines.len() == 2), "{:#?}", rep.violations);
}

#[test]
fn ambiguous_row_naming_two_dispositions_fails() {
    let r = fixture();
    let (d, _) = complete(&r);
    let rows = [row(&r.c1, "**MIRROR** or **RAFKA DOMAIN**"), row(&r.c2, "**RAFKA DOMAIN**"), row(&r.old_dated, "**MIRROR**")].concat();
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert!(!rep.eligible);
    assert!(matches!(&rep.violations[..], [Violation::AmbiguousDisposition { sha_prefix, .. }] if sha_prefix == short(&r.c1)), "{:#?}", rep.violations);
}

#[test]
fn ambiguous_row_naming_no_disposition_fails() {
    let r = fixture();
    let (d, _) = complete(&r);
    let rows = [row(&r.c1, "mirrored later"), row(&r.c2, "**RAFKA DOMAIN**"), row(&r.old_dated, "**MIRROR**")].concat();
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert!(matches!(&rep.violations[..], [Violation::AmbiguousDisposition { .. }]), "{:#?}", rep.violations);
}

#[test]
fn unknown_sha_fails() {
    let r = fixture();
    let (d, rows) = complete(&r);
    let rows = format!("{rows}| `{}` | #9 | x | **MIRROR** |\n", "deadbeef00");
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert!(matches!(&rep.violations[..], [Violation::UnresolvableSha { sha_prefix, .. }] if sha_prefix == "deadbeef00"), "{:#?}", rep.violations);
}

#[test]
fn mirror_pending_fails_and_is_counted() {
    let r = fixture();
    let (d, _) = complete(&r);
    let rows = [row(&r.c1, "**MIRROR pending**"), row(&r.c2, "**RAFKA DOMAIN**"), row(&r.old_dated, "**MIRROR**")].concat();
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert!(!rep.eligible);
    assert_eq!(rep.mirror_pending, 1);
    assert_eq!(rep.violations, vec![Violation::MirrorPending { sha: r.c1.clone(), line: rep.rows[1].line }]);
    assert_eq!(rep.rows[1].disposition, Some(Disposition::MirrorPending));
}

#[test]
fn range_is_ancestry_not_dates() {
    let r = fixture();
    let (d, rows) = complete(&r);
    let rep = scan(&r, &ledger(&r, &d, &rows));
    // The 2001-dated commit is after the base by ancestry: in range.
    assert!(rep.rows.iter().any(|x| x.sha == r.old_dated && x.in_range));
    // The 2099-dated commit is an ancestor of the base: not in range, no row needed.
    assert!(!rep.violations.iter().any(|v| matches!(v, Violation::Unclassified { sha, .. } if *sha == r.pre)));
    // The 2019-dated side-branch commit is not an ancestor of the base.
    assert!(rep.rows.iter().any(|x| x.sha == r.diverge && x.in_range));
}

#[test]
fn divergence_input_that_is_a_base_ancestor_fails() {
    let r = fixture();
    let (d, rows) = complete(&r);
    let d = format!("{d}{}", row(&r.pre, "**RAFKA DOMAIN**"));
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert_eq!(rep.violations, vec![Violation::DivergenceIsBaseAncestor { sha: r.pre.clone(), line: rep.rows[1].line }]);
}

#[test]
fn post_base_row_for_a_pre_base_commit_fails() {
    let r = fixture();
    let (d, rows) = complete(&r);
    let rows = format!("{rows}{}", row(&r.pre, "**MIRROR**"));
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert!(matches!(&rep.violations[..], [Violation::PreBaseRow { sha, .. }] if *sha == r.pre), "{:#?}", rep.violations);
}

#[test]
fn through_override_narrows_the_range() {
    let r = fixture();
    let rows = row(&r.c1, "**MIRROR**");
    let text = ledger(&r, "", &rows);
    let rep = parity::scan(&ScanInput { repo: &r.dir, ledger_text: &text, through: Some(&r.c1), connections_text: None });
    assert_eq!(rep.violations, vec![]);
    assert_eq!(rep.boundary_commits, 1);
    assert_eq!(rep.rafka_parity_through, r.c1);
}

#[test]
fn row_past_the_through_sha_fails() {
    let r = fixture();
    let rows = [row(&r.c1, "**MIRROR**"), row(&r.c2, "**MIRROR**")].concat();
    let text = ledger(&r, "", &rows);
    let rep = parity::scan(&ScanInput { repo: &r.dir, ledger_text: &text, through: Some(&r.c1), connections_text: None });
    assert!(matches!(&rep.violations[..], [Violation::RowOutsideRange { sha, .. }] if *sha == r.c2), "{:#?}", rep.violations);
}

#[test]
fn ledger_without_base_or_boundary_fails() {
    let r = fixture();
    let rep = scan(&r, "# empty ledger\n");
    assert!(!rep.eligible);
    assert!(rep.violations.iter().all(|v| matches!(v, Violation::Ledger { .. })));
    assert!(rep.violations.len() >= 2);
}

#[test]
fn mirror_row_without_rdm_sha_fails() {
    let r = fixture();
    let (d, _) = complete(&r);
    let rows = [
        format!("| `{}` | #1 | change | **MIRROR**. proved somewhere |\n", short(&r.c1)),
        row(&r.c2, "**RAFKA DOMAIN**"),
        row(&r.old_dated, "**MIRROR**"),
    ]
    .concat();
    let rep = scan(&r, &ledger(&r, &d, &rows));
    assert!(matches!(&rep.violations[..], [Violation::MirrorWithoutRdmProof { row, .. }] if row == short(&r.c1)), "{:#?}", rep.violations);
}
