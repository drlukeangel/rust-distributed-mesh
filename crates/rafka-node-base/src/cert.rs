//! Node certificates — the trust layer on top of iroh identity.
//!
//! iroh authenticates a node's *transport identity* (its NodeId = ed25519
//! PublicKey) at the QUIC layer, but says nothing about whether that node is
//! *authorized* on this mesh, or what *role* it has. node-admin is the CA: it signs
//! a `NodeCert { node_name, node_type, expiry }` for every node it spawns. A node
//! presents its cert (in its gossip digest); peers verify it against the CA pubkey +
//! the node's `node_name`. No valid cert → the node never enters topology → it
//! cannot communicate.
//!
//! **The cert binds the `node_name`, NOT the `node_id`.** The node_id is the node's
//! MUTABLE transport identity (it rotates across a restart to dodge iroh's same-id
//! reconnect wedge — see admin-ui `restart_one`). The `node_name` (`<mesh>.<type>.<N>`)
//! is the node's IMMUTABLE logical identity. Binding the cert to the name makes it
//! durable across identity rotations: node-admin issues it ONCE, and it stays valid
//! no matter how often the node_id changes — so a node that rotates while node-admin
//! is unreachable is NOT locked out for lack of a re-issued cert.
//!
//! Security note: a name-bound cert authorizes a *name*, not a specific key, so it
//! does not by itself prove the presenter holds the key currently bound to that name.
//! In the cooperative model (node-admin spawns every node; only it holds the CA) that
//! is sufficient. Adversarial/multi-tenant hardening would additionally sign each
//! digest with the node's current key — a separate enhancement.
//!
//! The CA key is a single ed25519 keypair (reusing iroh's crypto). It is the SHARED
//! ROOT: the same CA key can be distributed to every mesh's node-admin so a node in
//! mesh-A verifies a node from mesh-B against the same root — the cross-mesh
//! requirement, baked in from the start.

use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

/// The signed payload: binds a node's stable `node_name` to its role + an expiry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeCert {
    /// The node's IMMUTABLE logical name (`<mesh>.<type>.<N>`) — the durable handle
    /// the authorization is bound to (survives node_id rotation).
    pub node_name: String,
    /// role: gateway / broker / compute / registry / node-admin.
    pub node_type: String,
    /// unix-ms; the cert is invalid after this instant.
    pub expiry_ms: u64,
}

/// A `NodeCert` plus the CA's signature over it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedCert {
    pub cert: NodeCert,
    pub sig: Signature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertError {
    BadSignature,
    NodeNameMismatch,
    Expired,
}

impl CertError {
    pub fn as_str(&self) -> &'static str {
        match self {
            CertError::BadSignature => "bad-signature",
            CertError::NodeNameMismatch => "node-name-mismatch",
            CertError::Expired => "expired",
        }
    }
}

/// Canonical bytes the CA signs / a verifier checks (postcard of the payload).
fn cert_bytes(cert: &NodeCert) -> Vec<u8> {
    postcard::to_allocvec(cert).unwrap_or_default()
}

/// node-admin (the CA) issues a cert for a node, binding its STABLE `node_name`.
pub fn issue_cert(
    ca: &SecretKey,
    node_name: &str,
    node_type: &str,
    ttl_secs: u64,
    now_ms: u64,
) -> SignedCert {
    let cert = NodeCert {
        node_name: node_name.to_string(),
        node_type: node_type.to_string(),
        expiry_ms: now_ms.saturating_add(ttl_secs.saturating_mul(1000)),
    };
    let sig = ca.sign(&cert_bytes(&cert));
    SignedCert { cert, sig }
}

/// Verify a presented cert: CA signature valid, the cert's `node_name` matches the
/// presenter's name, and not expired.
pub fn verify_cert(
    signed: &SignedCert,
    ca_pub: &PublicKey,
    presenter_node_name: &str,
    now_ms: u64,
) -> Result<(), CertError> {
    ca_pub
        .verify(&cert_bytes(&signed.cert), &signed.sig)
        .map_err(|_| CertError::BadSignature)?;
    if signed.cert.node_name != presenter_node_name {
        return Err(CertError::NodeNameMismatch);
    }
    if now_ms > signed.cert.expiry_ms {
        return Err(CertError::Expired);
    }
    Ok(())
}

/// Hex-encode a signed cert for transport via env var.
pub fn encode_cert(signed: &SignedCert) -> String {
    hex::encode(postcard::to_allocvec(signed).unwrap_or_default())
}

/// Decode a hex-encoded signed cert.
pub fn decode_cert(hex_str: &str) -> Option<SignedCert> {
    let bytes = hex::decode(hex_str.trim()).ok()?;
    postcard::from_bytes(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ca() -> SecretKey {
        SecretKey::from_bytes(&[7u8; 32])
    }

    #[test]
    fn valid_cert_verifies() {
        let ca = ca();
        let name = "mesh1.broker.1";
        let signed = issue_cert(&ca, name, "broker", 3600, 1000);
        assert_eq!(verify_cert(&signed, &ca.public(), name, 2000), Ok(()));
    }

    #[test]
    fn survives_identity_rotation() {
        // The whole point: the same cert verifies regardless of the node's (changed)
        // node_id, because it binds the stable name.
        let ca = ca();
        let name = "mesh1.broker.1";
        let signed = issue_cert(&ca, name, "broker", 3600, 1000);
        // node rotated its node_id — cert (bound to name) is still valid.
        assert_eq!(verify_cert(&signed, &ca.public(), name, 2000), Ok(()));
    }

    #[test]
    fn wrong_ca_rejected() {
        let ca = ca();
        let other_ca = SecretKey::from_bytes(&[11u8; 32]);
        let name = "mesh1.broker.1";
        let signed = issue_cert(&ca, name, "broker", 3600, 1000);
        assert_eq!(verify_cert(&signed, &other_ca.public(), name, 2000), Err(CertError::BadSignature));
    }

    #[test]
    fn tampered_payload_rejected() {
        let ca = ca();
        let name = "mesh1.broker.1";
        let mut signed = issue_cert(&ca, name, "broker", 3600, 1000);
        signed.cert.node_type = "gateway".to_string(); // tamper after signing
        assert_eq!(verify_cert(&signed, &ca.public(), name, 2000), Err(CertError::BadSignature));
    }

    #[test]
    fn node_name_mismatch_rejected() {
        let ca = ca();
        let signed = issue_cert(&ca, "mesh1.broker.1", "broker", 3600, 1000);
        // a node presenting someone else's valid cert under a different name.
        assert_eq!(verify_cert(&signed, &ca.public(), "mesh1.broker.2", 2000), Err(CertError::NodeNameMismatch));
    }

    #[test]
    fn expired_rejected() {
        let ca = ca();
        let signed = issue_cert(&ca, "mesh1.broker.1", "broker", 1, 1000); // expiry = 2000
        assert_eq!(verify_cert(&signed, &ca.public(), "mesh1.broker.1", 5000), Err(CertError::Expired));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let ca = ca();
        let name = "mesh1.compute.3";
        let signed = issue_cert(&ca, name, "compute", 3600, 1000);
        let hexed = encode_cert(&signed);
        let back = decode_cert(&hexed).expect("decode");
        assert_eq!(verify_cert(&back, &ca.public(), name, 2000), Ok(()));
    }
}
