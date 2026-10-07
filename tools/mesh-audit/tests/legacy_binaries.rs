//! i143.e0.s1 acceptance: every legacy binary has exactly one disposition in
//! the checked-in audit doc, and no legacy role binary is a proof-shape node.

use rafka_mesh_audit::legacy::{self, Violation};
use rafka_mesh_audit::workspace_root;
use std::path::Path;

fn render(v: &[Violation]) -> String {
    v.iter().map(|v| format!("  - {v}\n")).collect()
}

#[test]
fn every_legacy_binary_has_exactly_one_disposition() {
    let v = legacy::check_dispositions(&workspace_root());
    assert!(v.is_empty(), "legacy-binary disposition gate failed:\n{}", render(&v));
}

#[test]
fn no_legacy_role_binary_is_a_proof_shape_node() {
    let v = legacy::check_proof_shapes(&workspace_root());
    assert!(v.is_empty(), "proof-shape gate failed:\n{}", render(&v));
}

// ---- the gate itself fails on each violation it exists to catch ----

fn fixture(doc: &str, members: &[&str], dirs: &[&str]) -> tempdir::Dir {
    let d = tempdir::Dir::new();
    let list: Vec<String> = members.iter().map(|m| format!("\"{m}\"")).collect();
    std::fs::write(d.path().join("Cargo.toml"), format!("[workspace]\nmembers = [{}]\n", list.join(", "))).unwrap();
    for dir in dirs {
        std::fs::create_dir_all(d.path().join(dir)).unwrap();
        std::fs::write(d.path().join(dir).join("Cargo.toml"), "").unwrap();
    }
    let _ = doc;
    d
}

const HEAD: &str = "## Legacy binary dispositions\n\n| binary | disposition | why |\n|---|---|---|\n";

fn rows(extra: &str) -> String {
    format!("{HEAD}{extra}")
}

const ALL: &[&str] = &["bridge"];

#[test]
fn gate_passes_a_complete_table() {
    let doc = rows("| `bridge` | extraction-input | x |\n");
    let d = fixture(&doc, ALL, ALL);
    assert_eq!(legacy::check_dispositions_in(d.path(), &doc), vec![]);
}

#[test]
fn gate_fails_an_unclassified_binary() {
    let doc = rows("");
    let d = fixture(&doc, ALL, ALL);
    assert_eq!(legacy::check_dispositions_in(d.path(), &doc), vec![Violation::Unclassified("bridge".into())]);
}

#[test]
fn gate_fails_a_duplicate_row() {
    let doc = rows("| `bridge` | extraction-input | x |\n| `bridge` | retained-example | y |\n");
    let d = fixture(&doc, ALL, ALL);
    assert_eq!(legacy::check_dispositions_in(d.path(), &doc),
        vec![Violation::Duplicate { binary: "bridge".into(), rows: 2 }]
    );
}

#[test]
fn gate_fails_an_unknown_disposition() {
    let doc = rows("| `bridge` | keep-for-now | x |\n");
    let d = fixture(&doc, ALL, ALL);
    assert_eq!(legacy::check_dispositions_in(d.path(), &doc),
        vec![Violation::UnknownDisposition { binary: "bridge".into(), cell: "keep-for-now".into() }]
    );
}

#[test]
fn gate_fails_dead_code_left_in_the_workspace() {
    let doc = rows("| `bridge` | dead-deleted | x |\n");
    let d = fixture(&doc, ALL, ALL);
    assert_eq!(legacy::check_dispositions_in(d.path(), &doc), vec![Violation::DeadButPresent("bridge".into())]);
}

#[test]
fn gate_passes_a_deleted_binary_that_is_gone() {
    let doc = rows("| `bridge` | dead-deleted | x |\n");
    let d = fixture(&doc, &[], &[]);
    assert_eq!(legacy::check_dispositions_in(d.path(), &doc), vec![]);
}

#[test]
fn gate_fails_a_retained_binary_that_is_gone() {
    let doc = rows("| `bridge` | retained-example | x |\n");
    let d = fixture(&doc, &[], &[]);
    assert_eq!(legacy::check_dispositions_in(d.path(), &doc), vec![Violation::LiveButAbsent("bridge".into())]);
}

#[test]
fn proof_shape_scan_names_a_legacy_role_node() {
    let yaml = "shape: MN\nmeshes:\n  - name: mesh1\n    node_admin: 2\n    bridge: 3\n";
    let v = legacy::scan_proof_shape_text(Path::new("s.yaml"), yaml);
    assert_eq!(
        v,
        vec![Violation::LegacyRoleInProofShape { file: "s.yaml".into(), line: 5, role: "bridge".into() }]
    );
}

#[test]
fn proof_shape_scan_ignores_substrings_and_role_nodes() {
    let yaml = "shape: MN\nmeshes:\n  - name: mesh1\n    node_admin: 2\n    rpc_node: 3\n    broker: 1\n# bridges is not a role\n";
    assert_eq!(legacy::scan_proof_shape_text(Path::new("s.yaml"), yaml), vec![]);
}

#[test]
fn proof_shape_scan_reads_scenario_dirs() {
    let d = tempdir::Dir::new();
    std::fs::create_dir_all(d.path().join("crates/x/scenarios")).unwrap();
    std::fs::write(d.path().join("crates/x/scenarios/a.yaml"), "rpc_node: 1\nbridge: 1\n").unwrap();
    let v = legacy::check_proof_shapes(d.path());
    assert_eq!(v.len(), 1, "{v:?}");
}

mod tempdir {
    use std::path::{Path, PathBuf};
    pub struct Dir(PathBuf);
    impl Dir {
        pub fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "mesh-audit-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        pub fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
