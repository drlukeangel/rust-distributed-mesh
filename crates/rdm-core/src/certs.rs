//! The fabric's cert chain (fabric-certs.md): its format, issue and verify.
//!
//! The chain is root → mesh issuing cert → member cert. The root signs each mesh's
//! [`IssuingCert`]; a mesh's [`IssuingKey`] signs its members' [`NodeCert`]s; every [`SignedCert`]
//! carries its whole chain (the member cert signed by the issuing key, and the issuing cert signed
//! by the root), so a verifier holds only the root's public key. Keys are ed25519
//! (`ed25519-dalek`, the keys iroh's are made of: the same 32 secret bytes are the same key).
//!
//! A member cert names its exact birth: `path.name`, mesh, node id, incarnation and endpoint key,
//! and its expiry (fabric-certs.md "When a cert is issued": every JoinNode issues a fresh one, so a
//! new birth never holds its predecessor's cert). Validity is judged on the clock the caller hands
//! in (rafka-time), with one skew allowance past each expiry.

use crate::model::{EndpointId, IncarnationId, NodeId, PathName};
pub use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use ed25519_dalek::{Signer, Verifier};
use serde::{Deserialize, Serialize};

/// A member cert's lifetime, in seconds (fabric-certs.md §Tracking, `RDM_MEMBER_CERT_TTL_SECS`'s
/// default). The policy carried in the entry pull is i145 e3's (R9).
pub const MEMBER_CERT_TTL_SECS: u64 = 86_400;

/// A mesh issuing cert's lifetime, in seconds (fabric-certs.md §Tracking,
/// `RDM_ISSUING_CERT_TTL_SECS`'s default).
pub const ISSUING_CERT_TTL_SECS: u64 = 30 * 86_400;

/// The exact birth a member cert is issued to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BirthIdentity {
    /// The birth's node id.
    pub node_id: NodeId,
    /// The birth's incarnation.
    pub incarnation: IncarnationId,
    /// The birth's `path.name`.
    pub name: PathName,
    /// The birth's mesh name.
    pub mesh: String,
    /// The birth's fabric endpoint key.
    pub endpoint_key: EndpointId,
}

/// A signer's refusal to issue, carried by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertRefusal {
    /// The refusal's stable name (what an app greps for).
    pub name: String,
    /// What the signer says about it.
    pub detail: String,
}

impl CertRefusal {
    /// A refusal named `name`.
    pub fn new(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self { name: name.into(), detail: detail.into() }
    }
}

impl std::fmt::Display for CertRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.detail)
    }
}

/// Why an issuance did not produce bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueFailure {
    /// This admin holds no rafka-time yet, so it has no issue time to mint on.
    NoRafkaTime(String),
    /// The signer refused.
    Refused(CertRefusal),
}

/// A fresh ed25519 key, from 32 random bytes.
pub fn mint_key() -> SigningKey {
    SigningKey::from_bytes(&rand::random::<[u8; 32]>())
}

/// The root-signed issuing certificate binding a mesh to its issuing public key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuingCert {
    /// The mesh whose members the issuing key signs.
    pub mesh: String,
    /// The issuing key's public half.
    pub issuer_pub: VerifyingKey,
    /// When the issuing cert expires, rafka-time ms.
    pub expiry_ms: u64,
}

/// An [`IssuingCert`] and the root's signature over its postcard bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedIssuingCert {
    /// The issuing cert.
    pub cert: IssuingCert,
    /// The root's signature.
    pub sig: Signature,
}

/// A mesh's issuing material: the key a node-admin of the mesh signs member certs with, and its
/// root-signed cert. Held in memory; handed to a launched admin in its environment.
#[derive(Clone)]
pub struct IssuingKey {
    secret: SigningKey,
    cert: SignedIssuingCert,
}

impl std::fmt::Debug for IssuingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuingKey").field("cert", &self.cert).finish_non_exhaustive()
    }
}

impl IssuingKey {
    /// A fresh issuing key for `mesh`, its cert signed by `root` and valid `ttl_secs` from `now_ms`.
    pub fn mint(root: &SigningKey, mesh: &str, ttl_secs: u64, now_ms: u64) -> IssuingKey {
        let secret = mint_key();
        let cert = IssuingCert { mesh: mesh.to_string(), issuer_pub: secret.verifying_key(), expiry_ms: now_ms.saturating_add(ttl_secs.saturating_mul(1000)) };
        let sig = root.sign(&issuing_cert_bytes(&cert));
        IssuingKey { secret, cert: SignedIssuingCert { cert, sig } }
    }

    /// The issuing material `secret` and `cert` make, as a launched admin reads them.
    pub fn from_parts(secret: SigningKey, cert: SignedIssuingCert) -> IssuingKey {
        IssuingKey { secret, cert }
    }

    /// The root-signed issuing cert.
    pub fn cert(&self) -> &SignedIssuingCert {
        &self.cert
    }

    /// The mesh it signs for.
    pub fn mesh(&self) -> &str {
        &self.cert.cert.mesh
    }

    /// When it expires, rafka-time ms.
    pub fn expiry_ms(&self) -> u64 {
        self.cert.cert.expiry_ms
    }

    /// The issuing key's secret.
    pub fn secret(&self) -> &SigningKey {
        &self.secret
    }
}

/// The signed payload of a member cert: the exact birth it is issued to, and its expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCert {
    /// The birth's `path.name`.
    pub name: PathName,
    /// The birth's mesh.
    pub mesh: String,
    /// The birth's node id.
    pub node_id: NodeId,
    /// The birth's incarnation.
    pub incarnation: IncarnationId,
    /// The birth's fabric endpoint key.
    pub endpoint: EndpointId,
    /// When the cert expires, rafka-time ms.
    pub expiry_ms: u64,
}

/// A [`NodeCert`] signed by its mesh's issuing key, carrying the whole chain (the issuing cert).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedCert {
    /// The member cert.
    pub cert: NodeCert,
    /// The issuing key's signature.
    pub sig: Signature,
    /// The issuing cert, signed by the root.
    pub issuer: SignedIssuingCert,
}

/// Why the chain refuses a cert, issued or presented: each by its stable name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertError {
    /// The issuing key does not verify the member cert.
    BadSignature,
    /// The member cert has expired.
    Expired,
    /// The root does not verify the issuing cert.
    IssuerBadSignature,
    /// The issuing cert has expired.
    IssuerExpired,
    /// The member's mesh is not the issuing cert's.
    IssuerMeshMismatch,
    /// The member cert would expire after its issuer.
    MemberOutlivesIssuer,
    /// A node-admin without the root was asked to sign for another mesh.
    NoRootToMint,
}

impl CertError {
    /// The refusal's stable name.
    pub fn as_str(&self) -> &'static str {
        match self {
            CertError::BadSignature => "bad-signature",
            CertError::Expired => "expired",
            CertError::IssuerBadSignature => "issuer-bad-signature",
            CertError::IssuerExpired => "issuer-expired",
            CertError::IssuerMeshMismatch => "issuer-mesh-mismatch",
            CertError::MemberOutlivesIssuer => "member-outlives-issuer",
            CertError::NoRootToMint => "no-root-to-mint",
        }
    }
}

impl std::fmt::Display for CertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::error::Error for CertError {}

/// The bytes the issuing key signs and a verifier checks: the member cert's postcard.
pub fn cert_bytes(cert: &NodeCert) -> Vec<u8> {
    postcard::to_allocvec(cert).expect("a member cert encodes")
}

/// The bytes the root signs and a verifier checks: the issuing cert's postcard.
pub fn issuing_cert_bytes(cert: &IssuingCert) -> Vec<u8> {
    postcard::to_allocvec(cert).expect("an issuing cert encodes")
}

/// The member cert for the exact birth `birth`, signed by `issuer`, expiring at `expiry_ms`. The
/// issuing admin cuts the expiry to its issuer's first; one that still outlives it is refused.
pub fn issue(issuer: &IssuingKey, birth: &BirthIdentity, expiry_ms: u64) -> Result<SignedCert, CertError> {
    if birth.mesh != issuer.mesh() || birth.name.mesh != issuer.mesh() {
        return Err(CertError::IssuerMeshMismatch);
    }
    if expiry_ms > issuer.expiry_ms() {
        return Err(CertError::MemberOutlivesIssuer);
    }
    let cert = NodeCert { name: birth.name.clone(), mesh: birth.mesh.clone(), node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), endpoint: birth.endpoint_key.clone(), expiry_ms };
    let sig = issuer.secret.sign(&cert_bytes(&cert));
    Ok(SignedCert { cert, sig, issuer: issuer.cert.clone() })
}

/// Verify `signed`'s chain to `root` at `now_ms`, refusing at the first failure, in
/// fabric-certs.md's order: the root verifies the issuing cert ([`CertError::IssuerBadSignature`]);
/// the issuing cert has not expired ([`CertError::IssuerExpired`]); the member's mesh is the issuing
/// cert's ([`CertError::IssuerMeshMismatch`]); the issuing key verifies the member cert
/// ([`CertError::BadSignature`]); the member cert has not expired ([`CertError::Expired`]). An expiry
/// is passed only when `now_ms` is past it by more than `skew_ms` (one staleness floor).
pub fn verify(signed: &SignedCert, root: &VerifyingKey, now_ms: u64, skew_ms: u64) -> Result<(), CertError> {
    root.verify(&issuing_cert_bytes(&signed.issuer.cert), &signed.issuer.sig).map_err(|_| CertError::IssuerBadSignature)?;
    if now_ms > signed.issuer.cert.expiry_ms.saturating_add(skew_ms) {
        return Err(CertError::IssuerExpired);
    }
    if signed.cert.mesh != signed.issuer.cert.mesh || signed.cert.name.mesh != signed.issuer.cert.mesh {
        return Err(CertError::IssuerMeshMismatch);
    }
    signed.issuer.cert.issuer_pub.verify(&cert_bytes(&signed.cert), &signed.sig).map_err(|_| CertError::BadSignature)?;
    if now_ms > signed.cert.expiry_ms.saturating_add(skew_ms) {
        return Err(CertError::Expired);
    }
    Ok(())
}

/// A signed member cert's bytes, as `JoinControl::member_cert` carries them.
pub fn cert_to_bytes(signed: &SignedCert) -> Vec<u8> {
    postcard::to_allocvec(signed).expect("a signed cert encodes")
}

/// The signed member cert `bytes` carry, or why they are not one.
pub fn cert_from_bytes(bytes: &[u8]) -> Result<SignedCert, String> {
    postcard::from_bytes(bytes).map_err(|e| format!("not a signed member cert ({} bytes): {e}", bytes.len()))
}

/// A signed member cert, hex-encoded for an environment variable.
pub fn encode_cert(signed: &SignedCert) -> String {
    hex::encode(cert_to_bytes(signed))
}

/// The signed member cert a hex string encodes, or why it is not one.
pub fn decode_cert(hex_str: &str) -> Result<SignedCert, String> {
    cert_from_bytes(&hex::decode(hex_str.trim()).map_err(|e| format!("a signed member cert is not hex: {e}"))?)
}

/// A signed issuing cert, hex-encoded for an environment variable (`RDM_ISSUING_CERT`).
pub fn encode_issuing_cert(signed: &SignedIssuingCert) -> String {
    hex::encode(postcard::to_allocvec(signed).expect("a signed issuing cert encodes"))
}

/// The signed issuing cert a hex string encodes, or why it is not one.
pub fn decode_issuing_cert(hex_str: &str) -> Result<SignedIssuingCert, String> {
    let bytes = hex::decode(hex_str.trim()).map_err(|e| format!("a signed issuing cert is not hex: {e}"))?;
    postcard::from_bytes(&bytes).map_err(|e| format!("not a signed issuing cert ({} bytes): {e}", bytes.len()))
}

/// An ed25519 secret, hex-encoded for an environment variable or a file (`RDM_CA_SECRET`,
/// `RDM_ISSUING_KEY`, `ca-secret`).
pub fn encode_key(key: &SigningKey) -> String {
    hex::encode(key.to_bytes())
}

/// The ed25519 secret a hex string encodes, or why it is not one.
pub fn decode_key(hex_str: &str) -> Result<SigningKey, String> {
    let bytes = hex::decode(hex_str.trim()).map_err(|e| format!("an ed25519 secret is not hex: {e}"))?;
    let bytes: [u8; 32] = bytes.as_slice().try_into().map_err(|_| format!("an ed25519 secret is 32 bytes, not {}", bytes.len()))?;
    Ok(SigningKey::from_bytes(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }

    fn sample_issuer(root_key: &SigningKey) -> IssuingKey {
        IssuingKey::mint(root_key, "mesh1", ISSUING_CERT_TTL_SECS, 1000)
    }

    fn birth(name: &str) -> BirthIdentity {
        let name: PathName = name.parse().unwrap();
        BirthIdentity { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), mesh: name.mesh.clone(), name, endpoint_key: EndpointId("k".into()) }
    }

    /// The member cert of `name`, `ttl_secs` from `now_ms`.
    fn issue_cert(issuer: &IssuingKey, name: &str, ttl_secs: u64, now_ms: u64) -> Result<SignedCert, CertError> {
        issue(issuer, &birth(name), now_ms.saturating_add(ttl_secs.saturating_mul(1000)))
    }

    fn verify_cert_sig_expiry(signed: &SignedCert, root_pub: &VerifyingKey, now_ms: u64) -> Result<(), CertError> {
        verify(signed, root_pub, now_ms, 0)
    }

    #[test]
    fn a_member_signed_by_its_meshs_issuer_verifies_under_the_root() {
        let root_key = root();
        let issuer = sample_issuer(&root_key);
        let signed = issue_cert(&issuer, "mesh1.broker.1", 3600, 1000).expect("issue");
        assert_eq!(verify_cert_sig_expiry(&signed, &root_key.verifying_key(), 2000), Ok(()));
    }

    #[test]
    fn an_issuer_cert_not_signed_by_the_root_is_refused_issuer_bad_signature() {
        let root_key = root();
        let wrong_root = SigningKey::from_bytes(&[11u8; 32]);
        let issuer = sample_issuer(&wrong_root);
        let signed = issue_cert(&issuer, "mesh1.broker.1", 3600, 1000).expect("issue");
        assert_eq!(verify_cert_sig_expiry(&signed, &root_key.verifying_key(), 2000), Err(CertError::IssuerBadSignature));
    }

    #[test]
    fn an_expired_issuer_cert_is_refused_issuer_expired() {
        let root_key = root();
        let issuer = IssuingKey::mint(&root_key, "mesh1", 10, 1000); // expiry = 11_000 ms
        let signed = issue_cert(&issuer, "mesh1.broker.1", 5, 1000).expect("issue"); // member expiry = 6_000 ms
        assert_eq!(verify_cert_sig_expiry(&signed, &root_key.verifying_key(), 12_000), Err(CertError::IssuerExpired));
    }

    #[test]
    fn a_member_named_for_another_mesh_is_refused_issuer_mesh_mismatch() {
        let root_key = root();
        let issuer = sample_issuer(&root_key);

        // issue refuses to mint it
        let mint_res = issue_cert(&issuer, "mesh2.broker.1", 3600, 1000);
        assert_eq!(mint_res.unwrap_err(), CertError::IssuerMeshMismatch);

        // verify also refuses if the name was modified/mismatched
        let mut signed = issue_cert(&issuer, "mesh1.broker.1", 3600, 1000).expect("issue");
        signed.cert.name = "mesh2.broker.1".parse().unwrap();
        assert_eq!(verify_cert_sig_expiry(&signed, &root_key.verifying_key(), 2000), Err(CertError::IssuerMeshMismatch));
    }

    #[test]
    fn a_member_signature_by_another_key_is_refused_bad_signature() {
        let root_key = root();
        let issuer = sample_issuer(&root_key);
        let mut signed = issue_cert(&issuer, "mesh1.broker.1", 3600, 1000).expect("issue");
        signed.cert.incarnation = IncarnationId::mint(); // tamper payload
        assert_eq!(verify_cert_sig_expiry(&signed, &root_key.verifying_key(), 2000), Err(CertError::BadSignature));
    }

    #[test]
    fn an_expired_member_is_refused_expired() {
        let root_key = root();
        let issuer = sample_issuer(&root_key);
        let signed = issue_cert(&issuer, "mesh1.broker.1", 1, 1000).expect("issue"); // expiry = 2000 ms
        assert_eq!(verify_cert_sig_expiry(&signed, &root_key.verifying_key(), 5000), Err(CertError::Expired));
    }

    #[test]
    fn a_member_outliving_its_issuer_is_never_minted() {
        let root_key = root();
        let issuer = IssuingKey::mint(&root_key, "mesh1", 100, 1000); // expiry = 101_000 ms
        let res = issue_cert(&issuer, "mesh1.broker.1", 200, 1000); // member expiry = 201_000 ms
        assert_eq!(res.unwrap_err(), CertError::MemberOutlivesIssuer);
    }

    #[test]
    fn a_member_of_another_mesh_verifies_under_the_one_root() {
        let root_key = root();
        let mesh2_issuer = IssuingKey::mint(&root_key, "mesh2", ISSUING_CERT_TTL_SECS, 1000);
        let mesh2_member = issue_cert(&mesh2_issuer, "mesh2.broker.1", 3600, 1000).expect("issue");
        assert_eq!(verify_cert_sig_expiry(&mesh2_member, &root_key.verifying_key(), 2000), Ok(()));
    }

    #[test]
    fn a_restarted_node_admin_s_earlier_certs_still_verify() {
        let root_key = root();
        let boot1_issuer = IssuingKey::mint(&root_key, "mesh1", ISSUING_CERT_TTL_SECS, 1000);
        let boot1_member = issue_cert(&boot1_issuer, "mesh1.broker.1", 3600, 1000).expect("issue");

        // Admin restarts at t=2000 and mints a new in-memory issuing key
        let boot2_issuer = IssuingKey::mint(&root_key, "mesh1", ISSUING_CERT_TTL_SECS, 2000);
        assert_ne!(boot1_issuer.cert().cert.issuer_pub, boot2_issuer.cert().cert.issuer_pub);

        // The earlier member cert still verifies because it carries its own root-signed issuing cert
        assert_eq!(verify_cert_sig_expiry(&boot1_member, &root_key.verifying_key(), 2500), Ok(()));
    }

    /// CONTRACT: (i138 e3, GH#2209) a cert's expiry is judged against the clock its caller
    /// hands it, never one it reads itself: the same cert is valid at an instant before its
    /// expiry and expired at one after it. What must NOT happen: the verifier reading a clock
    /// of its own, which would put the local clock back into a fleet judgement.
    #[test]
    fn a_cert_is_judged_against_the_clock_it_is_given() {
        let root_key = root();
        let issuer = IssuingKey::mint(&root_key, "mesh1", ISSUING_CERT_TTL_SECS, 1_000_000);
        let signed = issue_cert(&issuer, "mesh1.broker.1", 60, 1_000_000).expect("issue"); // expiry 1_060_000
        assert_eq!(verify_cert_sig_expiry(&signed, &root_key.verifying_key(), 1_059_999), Ok(()));
        assert_eq!(verify_cert_sig_expiry(&signed, &root_key.verifying_key(), 1_060_001), Err(CertError::Expired));
    }

    /// CONTRACT (fabric-certs.md "When a cert is issued", proposal Q6): a member cert names its exact
    /// birth, its `path.name`, mesh, node id, incarnation and endpoint key, and verifies as it was
    /// issued; a new birth at the same `path.name` is issued its own.
    #[test]
    fn a_member_cert_names_its_exact_birth() {
        let root_key = root();
        let issuer = sample_issuer(&root_key);
        let b = birth("mesh1.broker.1");
        let signed = issue(&issuer, &b, 3_601_000).expect("issue");
        assert_eq!((&signed.cert.name, &signed.cert.mesh, &signed.cert.node_id, &signed.cert.incarnation, &signed.cert.endpoint), (&b.name, &b.mesh, &b.node_id, &b.incarnation, &b.endpoint_key));
        assert_eq!(verify_cert_sig_expiry(&signed, &root_key.verifying_key(), 2000), Ok(()));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let root_key = root();
        let issuer = sample_issuer(&root_key);
        let signed = issue_cert(&issuer, "mesh1.compute.3", 3600, 1000).expect("issue");
        let hexed = encode_cert(&signed);
        let back = decode_cert(&hexed).expect("decode");
        assert_eq!(verify_cert_sig_expiry(&back, &root_key.verifying_key(), 2000), Ok(()));
        assert_eq!(cert_from_bytes(&cert_to_bytes(&signed)), Ok(signed));
    }

    #[test]
    fn valid_cert_accepted() {
        let root_key = SigningKey::from_bytes(&[13u8; 32]);
        let issuer = IssuingKey::mint(&root_key, "mesh1", ISSUING_CERT_TTL_SECS, 1000);
        let signed = issue_cert(&issuer, "mesh1.gateway.1", 3600, 1000).expect("issue");
        assert_eq!(verify(&signed, &root_key.verifying_key(), 2000, 0), Ok(()));
    }

    #[test]
    fn wrong_root_rejected() {
        let root_key = SigningKey::from_bytes(&[13u8; 32]);
        let other_root = SigningKey::from_bytes(&[19u8; 32]);
        let issuer = IssuingKey::mint(&other_root, "mesh1", ISSUING_CERT_TTL_SECS, 1000);
        let signed = issue_cert(&issuer, "mesh1.broker.1", 3600, 1000).expect("issue");
        assert_eq!(verify(&signed, &root_key.verifying_key(), 2000, 0), Err(CertError::IssuerBadSignature));
    }

    #[test]
    fn expired_cert_rejected() {
        let root_key = SigningKey::from_bytes(&[13u8; 32]);
        let issuer = IssuingKey::mint(&root_key, "mesh1", ISSUING_CERT_TTL_SECS, 1000);
        let signed = issue_cert(&issuer, "mesh1.compute.1", 1, 1000).expect("issue");
        assert_eq!(verify(&signed, &root_key.verifying_key(), 5000, 0), Err(CertError::Expired));
    }

    #[test]
    fn signed_issuing_cert_encode_decode_roundtrips() {
        let root_key = SigningKey::from_bytes(&[13u8; 32]);
        let issuer = IssuingKey::mint(&root_key, "mesh2", ISSUING_CERT_TTL_SECS, 1000);
        let encoded = encode_issuing_cert(issuer.cert());
        let decoded = decode_issuing_cert(&encoded).expect("decode issuing cert");
        assert_eq!(decoded, *issuer.cert());
        let reassembled = IssuingKey::from_parts(decode_key(&encode_key(issuer.secret())).expect("decode issuing key"), decoded);
        assert_eq!(reassembled.mesh(), "mesh2");
        assert_eq!(reassembled.cert(), issuer.cert());
    }

    /// CONTRACT (fabric-certs.md "Time": "A presented cert is judged with a skew allowance of one
    /// staleness floor (`RDM_STALENESS_MS`) on either side of its validity"): an expiry is passed only
    /// when the clock is past it by more than the allowance, the member's and the issuer's alike.
    /// Must NOT happen: a cert refused within one floor of its expiry, or admitted past it.
    #[test]
    fn a_cert_within_one_floor_past_its_expiry_is_admitted_and_one_past_it_is_refused() {
        let root_key = root();
        let issuer = IssuingKey::mint(&root_key, "mesh1", 10, 1000); // issuer expiry 11_000
        let member = issue_cert(&issuer, "mesh1.broker.1", 5, 1000).expect("issue"); // member expiry 6_000
        let floor = 3_000;
        assert_eq!(verify(&member, &root_key.verifying_key(), 9_000, floor), Ok(()), "3 s past the member's expiry, within the floor");
        assert_eq!(verify(&member, &root_key.verifying_key(), 9_001, floor), Err(CertError::Expired));
        assert_eq!(verify(&member, &root_key.verifying_key(), 14_001, floor), Err(CertError::IssuerExpired), "past the issuer's expiry and the floor");
    }

    /// CONTRACT (fabric-certs.md "The chain", i145 PRD e1): every refusal is named, and the names are
    /// stable: each is what an app and a span carry.
    #[test]
    fn each_refusal_is_named() {
        let named = [
            (CertError::BadSignature, "bad-signature"),
            (CertError::Expired, "expired"),
            (CertError::IssuerBadSignature, "issuer-bad-signature"),
            (CertError::IssuerExpired, "issuer-expired"),
            (CertError::IssuerMeshMismatch, "issuer-mesh-mismatch"),
            (CertError::MemberOutlivesIssuer, "member-outlives-issuer"),
            (CertError::NoRootToMint, "no-root-to-mint"),
        ];
        for (error, name) in named {
            assert_eq!((error.as_str(), error.to_string()), (name, name.to_string()), "{error:?}");
        }
    }

    /// CONTRACT (fabric-certs.md "The chain": "never issues a member cert that outlives its issuer
    /// (`MemberOutlivesIssuer`)"): a member cert asked to outlive its issuer is refused by name; one
    /// cut to its issuer's expiry is issued, and verifies to the end of its issuer's life.
    #[test]
    fn a_member_cert_never_outlives_its_issuer() {
        let root_key = root();
        let issuer = IssuingKey::mint(&root_key, "mesh1", 100, 1000); // expiry 101_000
        assert_eq!(issue(&issuer, &birth("mesh1.rpc.1"), 101_001), Err(CertError::MemberOutlivesIssuer));
        let cut = issue(&issuer, &birth("mesh1.rpc.1"), issuer.expiry_ms()).expect("cut to the issuer's expiry");
        assert_eq!(cut.cert.expiry_ms, issuer.cert().cert.expiry_ms);
        assert_eq!(verify(&cut, &root_key.verifying_key(), 101_000, 0), Ok(()));
    }

    /// CONTRACT (i145 e1, arch 2026-10-19): an iroh `SecretKey` and the ed25519 key of its 32 bytes are
    /// one secret: the same public key, and a signature by either verifies under the other, so the
    /// material RDM's environment carries reads the same as an iroh key or an RDM key.
    #[test]
    fn an_iroh_secret_key_and_the_ed25519_key_of_its_bytes_are_one_secret() {
        let bytes = [42u8; 32];
        let iroh_key = iroh_base::SecretKey::from_bytes(&bytes);
        let key = SigningKey::from_bytes(&bytes);
        assert_eq!(iroh_key.public().as_bytes(), key.verifying_key().as_bytes(), "the same public key");
        assert_eq!(iroh_key.to_bytes(), key.to_bytes(), "the same 32 secret bytes");
        let msg = b"the fabric root signs this";
        let by_iroh = iroh_key.sign(msg);
        assert!(key.verifying_key().verify(msg, &Signature::from_bytes(&by_iroh.to_bytes())).is_ok(), "iroh's signature verifies under the ed25519 key");
        let by_rdm = key.sign(msg);
        assert!(iroh_key.public().verify(msg, &iroh_base::Signature::from_bytes(&by_rdm.to_bytes())).is_ok(), "the ed25519 signature verifies under iroh's key");
    }
}
