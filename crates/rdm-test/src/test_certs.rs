//! The testkit's fabric root: every estate's bootstrap admin, and every node-admin a person starts on
//! an estate, is given this root (`RDM_CA_SECRET`), so a cell verifies a fabric's cert chain to it.
//! An in-process admin side issues with it through [`root_choice`].

use rdm_core::certs::{SigningKey, VerifyingKey};

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

/// The cert choice of an admin holding the testkit's root.
pub fn root_choice() -> rdm_admin::certs::CertChoice {
    rdm_admin::certs::CertChoice::Ca { secret: test_root() }
}
