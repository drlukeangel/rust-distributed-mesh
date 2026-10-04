//! i143.e0.s4 acceptance: each PRD §4 dependency rule has a failing fixture
//! (a throwaway cargo workspace that breaks exactly that rule) and the rules
//! pass on the RDM tree.

use rafka_mesh_audit::deps::{self, Violation};
use rafka_mesh_audit::workspace_root;
use std::path::PathBuf;

/// A fixture: `members` are workspace crates under `ws/`, `outside` are crates
/// beside the workspace (non-members, like a rafka-v2 checkout or a registry
/// crate). Each crate is `(name, [(dep, kind)])`, kind = "" | "dev".
struct Fixture(PathBuf);

impl Fixture {
    fn new(members: &[(&str, &[(&str, &str)])], outside: &[(&str, &[(&str, &str)])]) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!("dep-rules-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
        let _ = std::fs::remove_dir_all(&dir);
        let all: Vec<&str> = members.iter().chain(outside).map(|(n, _)| *n).collect();
        let loc = |name: &str| -> String {
            if members.iter().any(|(n, _)| *n == name) { format!("ws/{name}") } else { format!("outside/{name}") }
        };
        for (name, deps) in members.iter().chain(outside) {
            let here = dir.join(loc(name));
            std::fs::create_dir_all(here.join("src")).unwrap();
            std::fs::write(here.join("src/lib.rs"), "").unwrap();
            let mut normal = String::new();
            let mut dev = String::new();
            for (dep, kind) in *deps {
                assert!(all.contains(dep), "fixture names unknown crate {dep}");
                let line = format!("{dep} = {{ path = \"../../{}\" }}\n", loc(dep));
                if *kind == "dev" { dev.push_str(&line) } else { normal.push_str(&line) }
            }
            std::fs::write(
                here.join("Cargo.toml"),
                format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\n{normal}\n[dev-dependencies]\n{dev}"),
            )
            .unwrap();
        }
        let list: Vec<String> = members.iter().map(|(n, _)| format!("\"{n}\"")).collect();
        std::fs::write(dir.join("ws/Cargo.toml"), format!("[workspace]\nresolver = \"2\"\nmembers = [{}]\n", list.join(", "))).unwrap();
        Self(dir)
    }
    fn root(&self) -> PathBuf {
        self.0.join("ws")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn check(f: &Fixture) -> Vec<Violation> {
    let v = deps::check(&f.root());
    assert!(!v.iter().any(|x| matches!(x, Violation::Metadata(_))), "{v:?}");
    v
}

const NONE: &[(&str, &str)] = &[];

#[test]
fn rules_hold_on_the_rdm_tree() {
    let v = deps::check(&workspace_root());
    assert!(v.is_empty(), "dependency rules failed on the tree:\n{}", v.iter().map(|x| format!("  - {x}\n")).collect::<String>());
}

#[test]
fn clean_fixture_passes() {
    let f = Fixture::new(
        &[
            ("rafka-node-rpc-contract", NONE),
            ("rafka-mesh-entity", &[("rafka-node-rpc-contract", "")]),
            ("rafka-node-admin-client", &[("rafka-node-rpc-contract", "")]),
            ("rafka-chaos", &[("rafka-node-admin-client", "")]),
        ],
        &[],
    );
    assert_eq!(check(&f), vec![]);
}

#[test]
fn rule1_generic_package_on_a_rafka_v2_domain_crate_fails() {
    let f = Fixture::new(&[("rafka-mesh-transport", &[("rafka-gateway-core", "")])], &[("rafka-gateway-core", NONE)]);
    assert_eq!(
        check(&f),
        vec![Violation::DomainCrate {
            package: "rafka-mesh-transport".into(),
            domain: "rafka-gateway-core".into(),
            path: vec!["rafka-mesh-transport".into(), "rafka-gateway-core".into()],
        }]
    );
}

#[test]
fn rule1_is_transitive_through_a_non_generic_member() {
    let f = Fixture::new(
        &[("rafka-node-rpc", &[("helper", "")]), ("helper", &[("rafka-reader", "")])],
        &[("rafka-reader", NONE)],
    );
    assert_eq!(
        check(&f),
        vec![Violation::DomainCrate {
            package: "rafka-node-rpc".into(),
            domain: "rafka-reader".into(),
            path: vec!["rafka-node-rpc".into(), "helper".into(), "rafka-reader".into()],
        }]
    );
}

#[test]
fn rule2_contract_on_iroh_fails() {
    let f = Fixture::new(&[("rafka-node-rpc-contract", &[("bytes-ish", "")])], &[("bytes-ish", &[("iroh", "")]), ("iroh", NONE)]);
    assert_eq!(
        check(&f),
        vec![Violation::ContractIroh {
            iroh: "iroh".into(),
            path: vec!["rafka-node-rpc-contract".into(), "bytes-ish".into(), "iroh".into()],
        }]
    );
}

#[test]
fn rule2_ignores_a_dev_only_iroh() {
    let f = Fixture::new(&[("rafka-node-rpc-contract", &[("iroh", "dev")])], &[("iroh", NONE)]);
    assert_eq!(check(&f), vec![]);
}

#[test]
fn rule3_mesh_entity_on_application_ef_fails() {
    let f = Fixture::new(&[("rafka-mesh-entity", &[("rafka-entity-framework", "")])], &[("rafka-entity-framework", NONE)]);
    let v = check(&f);
    assert!(
        v.contains(&Violation::MeshEntityApplicationEf {
            crate_name: "rafka-entity-framework".into(),
            path: vec!["rafka-mesh-entity".into(), "rafka-entity-framework".into()],
        }),
        "{v:?}"
    );
}

#[test]
fn rule4_chaos_on_node_admin_core_fails() {
    let f = Fixture::new(
        &[("rafka-chaos", &[("rafka-node-admin-core", ""), ("rafka-node-admin-client", "")]), ("rafka-node-admin-core", NONE), ("rafka-node-admin-client", NONE)],
        &[],
    );
    assert_eq!(
        check(&f),
        vec![Violation::PrivateInterface { package: "rafka-chaos".into(), dependency: "rafka-node-admin-core".into() }]
    );
}

#[test]
fn rule4_scenario_on_node_base_fails() {
    let f = Fixture::new(&[("rafka-test-scenario", &[("rafka-node-base", "")]), ("rafka-node-base", NONE)], &[]);
    assert_eq!(
        check(&f),
        vec![Violation::PrivateInterface { package: "rafka-test-scenario".into(), dependency: "rafka-node-base".into() }]
    );
}
