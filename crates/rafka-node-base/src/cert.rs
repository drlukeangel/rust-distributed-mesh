//! Node certificates — the trust layer on top of iroh identity.
//!
//! iroh authenticates a node's *identity* (its NodeId = ed25519 PublicKey) at the
//! transport layer, but says nothing about whether that node is *authorized* to
//! be on this mesh, or what *role* it has. node-admin is the CA: it signs a
//! `NodeCert { node_id, node_type, expiry }` for every node it spawns. A node
//! presents its cert (in its gossip digest); peers verify it against the CA
//! pubkey + the iroh-authenticated NodeId. No valid cert → the node never enters
//! topology → it cannot communicate.
//!
//! The CA key is a single ed25519 keypair (reusing iroh's crypto). It is the
//! SHARED ROOT: the same CA key can be distributed to every mesh's node-admin so
//! a node in mesh-A verifies a node from mesh-B against the same root — the
//! cross-mesh requirement, baked in from the start.

use iroh::{PublicKey, SecretKey, Signature};
use serde::{Deserialize, Serialize};

/// The signed payload: binds a node's iroh NodeId to its role + an expiry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeCert {
    /// hex-encoded iroh PublicKey — the node's cryptographic identity.
    pub node_id: String,
    /// role: gateway / broker / compute / registry / admin-ui.
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
    NodeIdMismatch,
    Expired,
}

impl CertError {
    pub fn as_str(&self) -> &'static str {
        match self {
            CertError::BadSignature => "bad-signature",
            CertError::NodeIdMismatch => "node-id-mismatch",
            CertError::Expired => "expired",
        }
    }
}

/// Canonical bytes the CA signs / a verifier checks (postcard of the payload).
fn cert_bytes(cert: &NodeCert) -> Vec<u8> {
    postcard::to_allocvec(cert).unwrap_or_default()
}

/// node-admin (the CA) issues a cert for a node.
pub fn issue_cert(
    ca: &SecretKey,
    node_id: &str,
    node_type: &str,
    ttl_secs: u64,
    now_ms: u64,
) -> SignedCert {
    let cert = NodeCert {
        node_id: node_id.to_string(),
        node_type: node_type.to_string(),
        expiry_ms: now_ms.saturating_add(ttl_secs.saturating_mul(1000)),
    };
    let sig = ca.sign(&cert_bytes(&cert));
    SignedCert { cert, sig }
}

/// Verify a presented cert: CA signature valid, the cert's node_id matches the
/// presenter's (iroh-authenticated) NodeId, and not expired.
pub fn verify_cert(
    signed: &SignedCert,
    ca_pub: &PublicKey,
    presenter_node_id: &str,
    now_ms: u64,
) -> Result<(), CertError> {
    ca_pub
        .verify(&cert_bytes(&signed.cert), &signed.sig)
        .map_err(|_| CertError::BadSignature)?;
    if signed.cert.node_id != presenter_node_id {
        return Err(CertError::NodeIdMismatch);
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

    fn ca_and_node() -> (SecretKey, SecretKey) {
        // Deterministic keys for stable tests.
        (SecretKey::from_bytes(&[7u8; 32]), SecretKey::from_bytes(&[9u8; 32]))
    }

    #[test]
    fn valid_cert_verifies() {
        let (ca, node) = ca_and_node();
        let nid = node.public().to_string();
        let signed = issue_cert(&ca, &nid, "broker", 3600, 1000);
        assert_eq!(verify_cert(&signed, &ca.public(), &nid, 2000), Ok(()));
    }

    #[test]
    fn wrong_ca_rejected() {
        let (ca, node) = ca_and_node();
        let other_ca = SecretKey::from_bytes(&[11u8; 32]);
        let nid = node.public().to_string();
        let signed = issue_cert(&ca, &nid, "broker", 3600, 1000);
        // verifying against a DIFFERENT CA pubkey must fail (forged authority).
        assert_eq!(verify_cert(&signed, &other_ca.public(), &nid, 2000), Err(CertError::BadSignature));
    }

    #[test]
    fn tampered_payload_rejected() {
        let (ca, node) = ca_and_node();
        let nid = node.public().to_string();
        let mut signed = issue_cert(&ca, &nid, "broker", 3600, 1000);
        signed.cert.node_type = "gateway".to_string(); // tamper after signing
        assert_eq!(verify_cert(&signed, &ca.public(), &nid, 2000), Err(CertError::BadSignature));
    }

    #[test]
    fn node_id_mismatch_rejected() {
        let (ca, node) = ca_and_node();
        let nid = node.public().to_string();
        let signed = issue_cert(&ca, &nid, "broker", 3600, 1000);
        // a different node presenting someone else's valid cert (cert-stuffing).
        assert_eq!(verify_cert(&signed, &ca.public(), "someoneelse", 2000), Err(CertError::NodeIdMismatch));
    }

    #[test]
    fn expired_rejected() {
        let (ca, node) = ca_and_node();
        let nid = node.public().to_string();
        let signed = issue_cert(&ca, &nid, "broker", 1, 1000); // expiry = 2000
        assert_eq!(verify_cert(&signed, &ca.public(), &nid, 5000), Err(CertError::Expired));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let (ca, node) = ca_and_node();
        let nid = node.public().to_string();
        let signed = issue_cert(&ca, &nid, "compute", 3600, 1000);
        let hexed = encode_cert(&signed);
        let back = decode_cert(&hexed).expect("decode");
        assert_eq!(verify_cert(&back, &ca.public(), &nid, 2000), Ok(()));
    }
}
