//! Deterministic cert signers for the testkit. The bytes encode the birth and the issue time, so a
//! test asserts on them without any key material: RDM never parses a cert, this module is the
//! test's own reader of what its own signer wrote.

use rafka_node_admin_core::certs::{BirthIdentity, CertRefusal, CertSigner};

/// The member cert [`TestCertSigner`] issues, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestMemberCert {
    /// The birth's node id.
    pub node_id: String,
    /// The birth's incarnation.
    pub incarnation: String,
    /// The birth's `path.name`.
    pub name: String,
    /// The birth's mesh.
    pub mesh: String,
    /// The birth's endpoint key.
    pub endpoint_key: String,
    /// The issuing admin's rafka-time when it was issued.
    pub issued_at_ms: u64,
}

impl TestMemberCert {
    /// The bytes [`TestCertSigner`] writes for this cert.
    pub fn encode(&self) -> Vec<u8> {
        format!(
            "testcert/v1;node_id={};incarnation={};name={};mesh={};endpoint_key={};issued_at_ms={}",
            self.node_id, self.incarnation, self.name, self.mesh, self.endpoint_key, self.issued_at_ms
        )
        .into_bytes()
    }

    /// The cert `bytes` spell, or why they are not one.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(bytes).map_err(|e| format!("not utf-8: {e}"))?;
        let rest = text.strip_prefix("testcert/v1;").ok_or_else(|| format!("not a test member cert: `{text}`"))?;
        let field = |k: &str| -> Result<String, String> {
            rest.split(';').find_map(|p| p.strip_prefix(&format!("{k}="))).map(str::to_string).ok_or_else(|| format!("test member cert has no `{k}`: `{text}`"))
        };
        Ok(Self {
            node_id: field("node_id")?,
            incarnation: field("incarnation")?,
            name: field("name")?,
            mesh: field("mesh")?,
            endpoint_key: field("endpoint_key")?,
            issued_at_ms: field("issued_at_ms")?.parse().map_err(|e| format!("issued_at_ms: {e}"))?,
        })
    }
}

/// The issuing material [`TestCertSigner`] makes for a mesh, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestMeshIssuer {
    /// The mesh the material is for.
    pub mesh: String,
    /// The issuing admin's rafka-time when it was issued.
    pub issued_at_ms: u64,
}

impl TestMeshIssuer {
    /// The bytes [`TestCertSigner`] writes for this material.
    pub fn encode(&self) -> Vec<u8> {
        format!("testissuer/v1;mesh={};issued_at_ms={}", self.mesh, self.issued_at_ms).into_bytes()
    }

    /// The material `bytes` spell, or why they are not it.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(bytes).map_err(|e| format!("not utf-8: {e}"))?;
        let rest = text.strip_prefix("testissuer/v1;mesh=").ok_or_else(|| format!("not test issuing material: `{text}`"))?;
        let (mesh, at) = rest.split_once(";issued_at_ms=").ok_or_else(|| format!("test issuing material has no issued_at_ms: `{text}`"))?;
        Ok(Self { mesh: mesh.to_string(), issued_at_ms: at.parse().map_err(|e| format!("issued_at_ms: {e}"))? })
    }
}

/// Issues [`TestMemberCert`]s and [`TestMeshIssuer`]s: the bytes encode the birth and the issue time.
pub struct TestCertSigner;

impl CertSigner for TestCertSigner {
    fn issue_member(&self, birth: &BirthIdentity, now_ms: u64) -> Result<Vec<u8>, CertRefusal> {
        Ok(TestMemberCert {
            node_id: birth.node_id.to_string(),
            incarnation: birth.incarnation.0.clone(),
            name: birth.name.to_string(),
            mesh: birth.mesh.clone(),
            endpoint_key: birth.endpoint_key.0.clone(),
            issued_at_ms: now_ms,
        }
        .encode())
    }

    fn issue_mesh_issuer(&self, mesh: &str, now_ms: u64) -> Result<Vec<u8>, CertRefusal> {
        Ok(TestMeshIssuer { mesh: mesh.to_string(), issued_at_ms: now_ms }.encode())
    }
}

/// Refuses every issuance, by the name and detail it was built with.
pub struct RefusingCertSigner {
    /// The refusal's name.
    pub name: String,
    /// The refusal's detail.
    pub detail: String,
}

impl RefusingCertSigner {
    /// A signer that refuses as `name`: `detail`.
    pub fn new(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self { name: name.into(), detail: detail.into() }
    }
}

impl CertSigner for RefusingCertSigner {
    fn issue_member(&self, _birth: &BirthIdentity, _now_ms: u64) -> Result<Vec<u8>, CertRefusal> {
        Err(CertRefusal::new(self.name.clone(), self.detail.clone()))
    }

    fn issue_mesh_issuer(&self, _mesh: &str, _now_ms: u64) -> Result<Vec<u8>, CertRefusal> {
        Err(CertRefusal::new(self.name.clone(), self.detail.clone()))
    }
}
