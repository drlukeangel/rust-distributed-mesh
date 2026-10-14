//! R-BI5 (Luke 2026-10-12, "build-id is a crockford id, we only have one id type, never a hex"):
//! a Build id and a deployment id are minted as every other id is, 12 lowercase Crockford
//! characters, and a stored `bld-<hex>` or 32-hex value is an old row that reading refuses by name.

use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::model::DeploymentId;

const CROCKFORD: &str = "0123456789abcdefghjkmnpqrstvwxyz";

/// CONTRACT: a minted Build id is 12 characters of the lowercase Crockford alphabet, bare (no `bld-`).
#[test]
fn a_minted_build_id_is_twelve_lowercase_crockford_characters() {
    for _ in 0..200 {
        let id = BuildId::mint().to_string();
        assert_eq!(id.len(), 12, "{id}");
        assert!(id.chars().all(|c| CROCKFORD.contains(c)), "{id}");
    }
}

/// CONTRACT: a minted deployment id is 12 characters of the lowercase Crockford alphabet.
#[test]
fn a_minted_deployment_id_is_twelve_lowercase_crockford_characters() {
    for _ in 0..200 {
        let id = DeploymentId::mint().to_string();
        assert_eq!(id.len(), 12, "{id}");
        assert!(id.chars().all(|c| CROCKFORD.contains(c)), "{id}");
    }
}

/// CONTRACT: a stored `bld-<24 hex>` Build id is refused by name: the error names the kind and the value.
#[test]
fn a_stored_bld_hex_build_id_is_refused_by_name() {
    let old = "bld-0123456789abcdef01234567";
    let e = serde_json::from_str::<BuildId>(&format!("\"{old}\"")).unwrap_err().to_string();
    assert!(e.contains("build id") && e.contains(old), "{e}");
}

/// CONTRACT: a stored 32-hex deployment id is refused by name: the error names the kind and the value.
#[test]
fn a_stored_hex_deployment_id_is_refused_by_name() {
    let old = "0123456789abcdef0123456789abcdef";
    let e = serde_json::from_str::<DeploymentId>(&format!("\"{old}\"")).unwrap_err().to_string();
    assert!(e.contains("deployment id") && e.contains(old), "{e}");
}
