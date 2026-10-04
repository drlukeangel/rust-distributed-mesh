//! i143.e0.s3 acceptance: the parity scanner fails while any i66.e3
//! connections clause (connections.md §13 acceptance tests A–L) has no ledger
//! row, or a row lacks an owner or a proof mapping; MIRROR rows must name the
//! RDM SHA. No carrier/reconnect design is read or produced here: the scanner
//! only maps clauses to owners.

use rafka_mesh_audit::parity::{self, Disposition, Violation};

const CONNECTIONS_MD: &str = "# Connections\n\n## 12. Observability\n\n### X. not a clause\n\n## 13. Required acceptance tests\n\n### A. One discovered proxy serves every consumer of the same destination\n\ntext\n\n### B. Proxy survives source restart\n\n### C. Failed reconnect backoff survives restart\n\n## 14. Ownership and RDM target\n\n### Z. not a clause either\n";

const HEAD: &str = "## Connections parity (i66.e3 → RDM)\n\n| clause | i66.e3 source | RDM owner | carrying proof | disposition |\n|---|---|---|---|---|\n";

fn ledger(rows: &str) -> String {
    format!("# ledger\n\n{HEAD}{rows}\n## Import handoff\n")
}

const A: &str = "| `CONN-A` | connections.md §13 A | i143.e6.s5 (#2771) | functional: proxy reused without a direct ladder | **MIRROR pending** |\n";
const B: &str = "| `CONN-B` | connections.md §13 B | i143.e4.s3 (#2756) | functional: restart keeps Proxy | **MIRROR pending** |\n";
const C: &str = "| `CONN-C` | connections.md §13 C | i143.e4.s3 (#2756) | functional: restart keeps backoff | **MIRROR pending** |\n";

fn check(rows: &str) -> (Vec<parity::ConnectionsRow>, Vec<Violation>) {
    parity::check_connections(&ledger(rows), CONNECTIONS_MD)
}

#[test]
fn required_clauses_are_section_13_headings_only() {
    assert_eq!(parity::required_connection_clauses(CONNECTIONS_MD), vec!["CONN-A", "CONN-B", "CONN-C"]);
}

#[test]
fn complete_mapping_has_only_pending_violations() {
    let (rows, v) = check(&format!("{A}{B}{C}"));
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|r| r.disposition == Some(Disposition::MirrorPending)));
    assert_eq!(v.len(), 3);
    assert!(v.iter().all(|x| matches!(x, Violation::MirrorPending { .. })), "{v:?}");
}

#[test]
fn unmapped_clause_fails() {
    let (_, v) = check(&format!("{A}{C}"));
    assert!(v.contains(&Violation::ConnectionsClauseUnmapped { clause: "CONN-B".into() }), "{v:?}");
}

#[test]
fn row_without_owner_fails() {
    let b = B.replace("i143.e4.s3 (#2756)", "—");
    let (_, v) = check(&format!("{A}{b}{C}"));
    assert!(v.iter().any(|x| matches!(x, Violation::ConnectionsRowWithoutOwner { clause, .. } if clause == "CONN-B")), "{v:?}");
}

#[test]
fn pending_row_owned_by_something_other_than_an_i143_story_fails() {
    let b = B.replace("i143.e4.s3 (#2756)", "the reconnect team");
    let (_, v) = check(&format!("{A}{b}{C}"));
    assert!(v.iter().any(|x| matches!(x, Violation::ConnectionsRowWithoutOwner { clause, .. } if clause == "CONN-B")), "{v:?}");
}

#[test]
fn row_without_proof_fails() {
    let c = C.replace("functional: restart keeps backoff", "TBD");
    let (_, v) = check(&format!("{A}{B}{c}"));
    assert!(v.iter().any(|x| matches!(x, Violation::ConnectionsRowWithoutProof { clause, .. } if clause == "CONN-C")), "{v:?}");
}

#[test]
fn duplicate_clause_fails() {
    let (_, v) = check(&format!("{A}{B}{C}{A}"));
    assert!(v.iter().any(|x| matches!(x, Violation::ConnectionsDuplicate { clause, lines } if clause == "CONN-A" && lines.len() == 2)), "{v:?}");
}

#[test]
fn ambiguous_connections_disposition_fails() {
    let a = A.replace("**MIRROR pending**", "**MIRROR pending** / **RAFKA DOMAIN**");
    let (_, v) = check(&format!("{a}{B}{C}"));
    assert!(v.iter().any(|x| matches!(x, Violation::AmbiguousDisposition { sha_prefix, .. } if sha_prefix == "CONN-A")), "{v:?}");
}

#[test]
fn mirror_row_without_rdm_sha_fails() {
    let a = A.replace("**MIRROR pending**", "**MIRROR**");
    let (_, v) = check(&format!("{a}{B}{C}"));
    assert!(v.iter().any(|x| matches!(x, Violation::MirrorWithoutRdmProof { row, .. } if row == "CONN-A")), "{v:?}");
}

#[test]
fn mirror_row_with_rdm_sha_and_test_passes() {
    let a = A.replace("**MIRROR pending**", "**MIRROR**. RDM `0123abcd99` `proxy_is_reused_without_a_direct_ladder`");
    let (_, v) = check(&format!("{a}{B}{C}"));
    assert_eq!(v.len(), 2, "only B and C are pending: {v:?}");
}

#[test]
fn rafka_domain_row_needs_a_named_owner_but_not_an_i143_story() {
    let c = "| `CONN-C` | connections.md §13 C | rafka i66.e3 (#2722b) | no route barrier exists in RDM | **RAFKA DOMAIN** |\n";
    let (_, v) = check(&format!("{A}{B}{c}"));
    assert_eq!(v.len(), 2, "{v:?}");
}

#[test]
fn missing_connections_doc_is_a_ledger_violation() {
    let dir = std::env::temp_dir().join(format!("conn-parity-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("ledger.md"), ledger(A)).unwrap();
    let rep = parity::scan_files(&dir, &dir.join("ledger.md"), None, Some(&dir.join("missing-connections.md"))).unwrap();
    assert!(rep.violations.iter().any(|v| matches!(v, Violation::Ledger { reason } if reason.contains("connections"))), "{:?}", rep.violations);
    let _ = std::fs::remove_dir_all(&dir);
}
