//! i143.e10.s1 static ratchet over the public API of the RDM packages a consumer imports
//! (the approved set of `scripts/i143-rshape-build-consumer.sh`).
//!
//! `approved_packages_document_every_public_item`: each approved package opens with a `//!` crate
//! doc and `#![deny(missing_docs)]`, so a public item without rustdoc fails that package's build.
//! The approved list here must equal the consumer script's list, so a package cannot be imported
//! without its API being documented. Each package states its version in its manifest.

use std::path::{Path, PathBuf};

const APPROVED: [&str; 9] = [
    "rafka-node-base",
    "rafka-node-admin-core",
    "rafka-mesh-transport",
    "rafka-mesh-telemetry",
    "rafka-node-rpc-testkit",
    "rafka-node-rpc",
    "rafka-node-rpc-contract",
    "rafka-node-admin-client",
    "rafka-mesh-entity",
];

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

/// The packages the consumer script approves, read from its `APPROVED = {...}` set.
fn script_approved() -> Vec<String> {
    let script = read("scripts/i143-rshape-build-consumer.sh");
    let start = script.find("APPROVED = {").expect("the consumer script names its approved set");
    let end = start + script[start..].find('}').expect("the approved set closes");
    let mut names: Vec<String> = script[start..end].split('"').skip(1).step_by(2).map(str::to_string).collect();
    names.sort();
    names
}

/// The first violation of the frozen-API rule in a package's `lib.rs`, named.
fn violation(package: &str, lib: &str) -> Option<String> {
    if !lib.starts_with("//!") {
        return Some(format!("{package}: lib.rs does not open with a `//!` crate doc"));
    }
    if !lib.lines().any(|l| l.trim() == "#![deny(missing_docs)]") {
        return Some(format!("{package}: lib.rs does not carry `#![deny(missing_docs)]`"));
    }
    None
}

#[test]
fn approved_packages_document_every_public_item() {
    let mut approved: Vec<String> = APPROVED.iter().map(|s| s.to_string()).collect();
    approved.sort();
    assert_eq!(script_approved(), approved, "the consumer script's approved set differs from this ratchet's");
    for package in APPROVED {
        let lib = read(&format!("crates/{package}/src/lib.rs"));
        if let Some(v) = violation(package, &lib) {
            panic!("{v}");
        }
        let manifest = read(&format!("crates/{package}/Cargo.toml"));
        assert!(manifest.lines().any(|l| l.starts_with("version = \"")), "{package}: Cargo.toml states no version");
    }
}

#[test]
fn undocumented_package_is_refused_by_name() {
    let missing_deny = "//! A crate.\npub fn f() {}\n";
    assert_eq!(violation("p", missing_deny).unwrap(), "p: lib.rs does not carry `#![deny(missing_docs)]`");
    let missing_doc = "#![deny(missing_docs)]\npub fn f() {}\n";
    assert_eq!(violation("p", missing_doc).unwrap(), "p: lib.rs does not open with a `//!` crate doc");
    assert!(violation("p", "//! A crate.\n#![deny(missing_docs)]\n").is_none());
}
