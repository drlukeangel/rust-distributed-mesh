//! The testkit's fabric root: every estate's bootstrap admin, and every node-admin a person starts on
//! an estate, is given this root (`RDM_CA_SECRET`), so a cell verifies a fabric's cert chain to it.
//! An in-process admin side issues with it through [`root_choice`].

use rdm_core::certs::{CaKey, SigningKey, VerifyingKey};

/// The testkit's fabric root.
pub fn test_root() -> SigningKey {
    SigningKey::from_bytes(&[0x5a; 32])
}

/// The testkit's fabric root as `RDM_CA_SECRET` carries it.
pub fn test_root_hex() -> String {
    rdm_core::certs::encode_key(&test_root())
}

/// The public key a cell verifies the testkit fabric's certs to.
pub fn test_root_public() -> VerifyingKey {
    test_root().verifying_key()
}

/// The testkit fabric's CA, minted once per process under [`test_root`]: every in-process admin side
/// holds this one, as every admin of a fabric holds its one CA (fabric-certs.md §Custody).
pub fn test_ca() -> CaKey {
    static CA: std::sync::OnceLock<CaKey> = std::sync::OnceLock::new();
    CA.get_or_init(|| CaKey::mint(&test_root(), rdm_core::certs::CA_CERT_TTL_SECS, 0)).clone()
}

/// The cert choice of an admin holding the testkit's root.
pub fn root_choice() -> rdm_admin::certs::CertChoice {
    rdm_admin::certs::CertChoice::Ca { secret: test_root(), issuing: None }
}

/// The custody a person holds for a running fabric and hands an adoption launch in its environment
/// (fabric-certs.md §Custody; fabric-recovery.md "Nothing alive", Q156): the root and the fabric CA an
/// admin's data dir `dir` keeps (`ca-secret`, `fabric-ca`), with `mesh`'s issuing material minted under
/// that CA, as `RDM_CA_SECRET`, `RDM_FABRIC_CA_KEY`, `RDM_ISSUING_KEY` and `RDM_ISSUING_CERT`.
pub fn custody_of(dir: &std::path::Path, mesh: &str) -> Vec<(&'static str, String)> {
    let read = |file: &str| std::fs::read_to_string(dir.join(file)).unwrap_or_else(|e| panic!("{}: {e}", dir.join(file).display()));
    let root = rdm_core::certs::decode_key(read("ca-secret").trim()).expect("the kept root decodes");
    let kept = read("fabric-ca");
    let mut lines = kept.lines();
    let (key, cert) = (lines.next().expect("the kept CA key"), lines.next().expect("the kept CA cert"));
    let ca = CaKey::from_parts(rdm_core::certs::decode_key(key).expect("the kept CA key decodes"), rdm_core::certs::decode_ca_cert(cert).expect("the kept CA cert decodes"));
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64);
    let m = rdm_admin::certs::CertIssuer::with_root(root, ca, None, time).issuing_material(mesh).expect("the mesh's issuing material").expect("a root holder hands issuing material");
    vec![("RDM_CA_SECRET", m.root.0), ("RDM_FABRIC_CA_KEY", m.ca_key.0), ("RDM_ISSUING_KEY", m.key.0), ("RDM_ISSUING_CERT", m.cert)]
}
