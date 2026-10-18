//! i143 1b: the storage seam rule refuses a product source that names the file store outside the
//! file backend's own modules and outside cfg(test), naming each site, and passes on the RDM tree.

use rafka_mesh_audit::storage_seam::{check, Violation};
use rafka_mesh_audit::workspace_root;
use std::path::{Path, PathBuf};

struct Tree(PathBuf);

impl Tree {
    fn new(files: &[(&str, &str)]) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!("storage-seam-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&dir);
        for (path, text) in files {
            let p = dir.join(path);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, text).unwrap();
        }
        Self(dir)
    }
    fn root(&self) -> &Path {
        &self.0
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// CONTRACT (i143 1b, Arch: the seam "enforced per landing in fast by a static ratchet"): RDM's
/// own tree names the file store only in the file backend's modules and in cfg(test) items.
#[test]
fn the_rdm_tree_reaches_the_file_store_only_through_the_file_backend() {
    let v = check(&workspace_root());
    assert!(v.is_empty(), "{}", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("\n"));
}

/// CONTRACT: a product source outside the file backend's modules that names the file store (a
/// `FileRecords`, a `StoreRoot`, the `FileBackend`, the `FileJournal`, any `File…Storage`) is refused,
/// naming the file, the line and the name, one site each. Must NOT happen: a boot or a workflow
/// constructing a file store past the six boundaries unnoticed.
#[test]
fn a_file_store_named_outside_the_file_backend_is_refused_at_its_site() {
    let t = Tree::new(&[(
        "crates/rdm-admin/src/admin/boot.rs",
        "fn boot() {\n    let root = crate::store_root::StoreRoot::open(&dir, &id);\n    let rows: crate::record_store::FileRecords = todo!();\n    let s = crate::storage::FileMeshStorage::open(&root);\n    let b = crate::storage_backend::FileBackend::at(&dir);\n    let j = crate::build_state::FileJournal::open_on_root(&root);\n}\n",
    )]);
    let v = check(t.root());
    let sites: Vec<(usize, &str)> = v.iter().map(|x| (x.line, x.name.as_str())).collect();
    assert_eq!(sites, vec![(2, "StoreRoot"), (3, "FileRecords"), (4, "FileMeshStorage"), (5, "FileBackend"), (6, "FileJournal")], "{v:?}");
    assert!(v.iter().all(|x| x.file == "crates/rdm-admin/src/admin/boot.rs"), "{v:?}");
    let shown = v[0].to_string();
    assert!(shown.contains("crates/rdm-admin/src/admin/boot.rs:2") && shown.contains("`StoreRoot`") && shown.contains("six storage boundaries"), "{shown}");
}

/// CONTRACT: the file backend's own modules may name the file store, and so may an item gated on
/// test (`#[cfg(test)]` module, `#[cfg(all(test, ...))]`, `#[test]` and `#[tokio::test]` cells);
/// a comment, a doc or a string naming it is no reach; a test crate is not a product source.
#[test]
fn the_file_backend_cfg_test_items_and_prose_may_name_the_file_store() {
    let t = Tree::new(&[
        ("crates/rdm-admin/src/storage_backend.rs", "fn open() { let r = StoreRoot::open(); let f = FileMeshStorage::open(&r); }\n"),
        ("crates/rdm-node/src/store_root.rs", "pub struct StoreRoot;\n"),
        (
            "crates/rdm-admin/src/admin.rs",
            "/// Opens a `FileJournal` through the backend.\nfn run() { let s = \"StoreRoot\"; } // FileRecords\n#[cfg(test)]\nmod tests { fn t() { let r = StoreRoot::at(&d); } }\n#[cfg(all(test, feature = \"file-backend\"))]\nmod more { use crate::storage::FileNodesStorage; }\n#[cfg(feature = \"file-backend\")]\n#[tokio::test]\nasync fn cell() { let j = FileJournal::open_on_root(); }\nimpl X {\n    #[test]\n    fn c() { let f = FileFabricStorage::open(); }\n}\n",
        ),
        ("crates/rdm-test/src/estate.rs", "fn e() { let r = StoreRoot::at(&d); }\n"),
        ("crates/rdm-admin/tests/cells.rs", "fn c() { let r = StoreRoot::at(&d); }\n"),
    ]);
    let v: Vec<Violation> = check(t.root());
    assert!(v.is_empty(), "{v:?}");
}

/// CONTRACT: a `cfg` that is not a test gate (`#[cfg(feature = "file-backend")]`, `#[cfg(not(test))]`)
/// exempts nothing: a feature-gated item outside the file backend is still a reach past the seam.
#[test]
fn a_feature_gate_or_a_not_test_gate_exempts_nothing() {
    let t = Tree::new(&[(
        "crates/rdm-admin/src/fabric_handover.rs",
        "#[cfg(feature = \"file-backend\")]\npub struct Stores { pub mesh: Arc<crate::storage::FileMeshStorage> }\n#[cfg(not(test))]\nfn f() { let r = StoreRoot::at(&d); }\n",
    )]);
    let sites: Vec<(usize, String)> = check(t.root()).into_iter().map(|x| (x.line, x.name)).collect();
    assert_eq!(sites, vec![(2, "FileMeshStorage".to_string()), (4, "StoreRoot".to_string())]);
}
