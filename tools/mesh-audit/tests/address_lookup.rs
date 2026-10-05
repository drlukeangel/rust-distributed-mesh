//! i143.e4.s13 acceptance: each address-lookup rule has a failing fixture and
//! the rules pass on the RDM tree (rafka-v2 #2816).

use rafka_mesh_audit::address_lookup::{check, Violation};
use rafka_mesh_audit::workspace_root;
use std::path::{Path, PathBuf};

struct Tree(PathBuf);

impl Tree {
    fn new(files: &[(&str, &str)]) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!("address-lookup-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
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

#[test]
fn the_n0_preset_with_relay_disabled_is_an_n0_lookup() {
    let t = Tree::new(&[("crates/legacy/src/lib.rs", "let e = Endpoint::builder(presets::N0DisableRelay);\n")]);
    assert_eq!(
        check(t.root()),
        vec![Violation::N0Lookup { file: "crates/legacy/src/lib.rs".into(), line: 1, token: "presets::N0".into() }]
    );
}

#[test]
fn dns_and_pkarr_lookups_are_refused_anywhere() {
    let t = Tree::new(&[
        ("crates/a/src/lib.rs", "use iroh::address_lookup::DnsAddressLookup;\n"),
        ("crates/b/tests/t.rs", "\nlet p = PkarrPublisher::n0_dns();\n"),
    ]);
    let v = check(t.root());
    assert!(v.contains(&Violation::N0Lookup { file: "crates/a/src/lib.rs".into(), line: 1, token: "DnsAddressLookup".into() }), "{v:?}");
    assert!(v.contains(&Violation::N0Lookup { file: "crates/b/tests/t.rs".into(), line: 2, token: "PkarrPublisher".into() }), "{v:?}");
}

#[test]
fn a_canonical_source_never_uses_the_legacy_plane_and_a_legacy_one_may() {
    let t = Tree::new(&[
        ("crates/rafka-node-admin-core/src/admin.rs", "let t = rafka_mesh_transport::IrohMeshTransport::new(k, a, true);\n"),
        ("crates/rafka-mesh-transport/src/membership.rs", "use iroh_mdns_address_lookup::MdnsAddressLookup;\n"),
        ("crates/rafka-node-base/src/lib.rs", "let t = IrohMeshTransport::new(k, a, true);\n"),
    ]);
    let v = check(t.root());
    assert_eq!(v.len(), 2, "{v:?}");
    assert!(v.iter().all(|x| matches!(x, Violation::LegacyPlane { .. })), "{v:?}");
    assert!(!v.iter().any(|x| matches!(x, Violation::LegacyPlane { file, .. } if file.contains("rafka-node-base"))));
}

#[test]
fn a_comment_naming_a_token_is_not_a_use() {
    let t = Tree::new(&[("crates/a/src/lib.rs", "// not presets::N0DisableRelay: it applies N0\nlet x = 1;\n")]);
    assert_eq!(check(t.root()), vec![]);
}

#[test]
fn the_rdm_tree_configures_no_n0_lookup_and_keeps_canonical_crates_off_the_legacy_plane() {
    let v = check(&workspace_root());
    assert!(v.is_empty(), "{}", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join("\n"));
}
