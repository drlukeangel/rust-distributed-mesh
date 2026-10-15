//! Core birth-identity and cert-refusal types shared between `rdm-core`, `rdm-node`, and `rdm-admin`.

use crate::model::{EndpointId, IncarnationId, NodeId, PathName};

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
