//! Fabric certs: the signing capability an embedding app gives a node-admin.
//!
//! RDM decides when a cert is issued and to which exact birth, and carries the bytes to that birth.
//! The app owns the cert format, the keys and every use of the cert. RDM never holds or reads key
//! material and never parses, verifies or interprets a cert: the bytes are opaque, and identity
//! inside the mesh is the iroh endpoint key.
//!
//! - A birth's `JoinNode` is accepted by the admin that deployed it. That admin calls
//!   [`CertSigner::issue_member`] for the exact birth BEFORE it admits it, and the bytes travel in
//!   the join answer (`JoinControl::member_cert`). A [`CertRefusal`] refuses the join by name: the
//!   birth is not admitted (nothing is installed for its key, `WaitForBind` is not completed).
//! - A mesh's first node-admin is launched with [`CertSigner::issue_mesh_issuer`]'s bytes
//!   (`Launch::mesh_issuer`, `RDM_MESH_ISSUER`).
//!
//! Issue time and expiry are the issuing admin's rafka-time: every call is given `now_ms` read from
//! the admin's own [`RafkaTime`](rafka_mesh_transport::clock::RafkaTime). The issuing side already
//! holds it; an admin that does not yet hold rafka-time issues nothing and says so.
//!
//! A node-admin is started with an explicit [`CertChoice`]: an app's signer, or [`NoCerts`] by
//! name. A [`Wiring`](crate::wiring::Wiring) that makes no choice is refused at start.

use crate::model::{EndpointId, IncarnationId, NodeId, PathName};
use rafka_mesh_transport::clock::RafkaTime;
use std::sync::Arc;

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

/// The app's signing capability. Implementations hold the keys; RDM holds none.
pub trait CertSigner: Send + Sync {
    /// A member cert for this exact birth, as opaque bytes. `now_ms` is the issuing admin's
    /// rafka-time: the issue time and the expiry are minted on it.
    fn issue_member(&self, birth: &BirthIdentity, now_ms: u64) -> Result<Vec<u8>, CertRefusal>;
    /// The issuing material a new mesh's first node-admin is launched with. `now_ms` is the
    /// issuing admin's rafka-time.
    fn issue_mesh_issuer(&self, mesh: &str, now_ms: u64) -> Result<Vec<u8>, CertRefusal>;
}

/// The explicit choice of no certs: every issuance yields empty bytes, and a span names it.
pub struct NoCerts;

impl CertSigner for NoCerts {
    fn issue_member(&self, birth: &BirthIdentity, now_ms: u64) -> Result<Vec<u8>, CertRefusal> {
        tracing::info_span!("rdm.node_admin.cert.create.via-no-certs-configured", node = %birth.name, node_id = %birth.node_id, incarnation_id = %birth.incarnation.0, issued_at_rafka_ms = now_ms)
            .in_scope(|| tracing::info!("this node-admin is configured with no certs: the member cert is empty"));
        Ok(Vec::new())
    }

    fn issue_mesh_issuer(&self, mesh: &str, now_ms: u64) -> Result<Vec<u8>, CertRefusal> {
        tracing::info_span!("rdm.node_admin.cert.create.via-no-certs-configured", mesh, issued_at_rafka_ms = now_ms)
            .in_scope(|| tracing::info!("this node-admin is configured with no certs: the mesh issuer is empty"));
        Ok(Vec::new())
    }
}

/// What a node-admin is configured with for certs. There is no default signer: `Unchosen` is
/// refused at start by name.
#[derive(Default)]
pub enum CertChoice {
    /// Nothing was chosen.
    #[default]
    Unchosen,
    /// No certs, by explicit choice ([`NoCerts`]).
    NoCerts,
    /// The app's signer.
    Signer(Arc<dyn CertSigner>),
}

impl CertChoice {
    /// The signer this choice names, or the refusal that a node-admin was started with none.
    pub fn resolve(self) -> Result<Arc<dyn CertSigner>, String> {
        match self {
            Self::Unchosen => Err("this node-admin was started with no cert choice: set Wiring::certs to CertChoice::Signer(..) or CertChoice::NoCerts".into()),
            Self::NoCerts => Ok(Arc::new(NoCerts)),
            Self::Signer(s) => Ok(s),
        }
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

/// The signer together with the rafka-time it mints on: what the join door and the deployment
/// executor call.
#[derive(Clone)]
pub struct CertIssuer {
    signer: Arc<dyn CertSigner>,
    time: RafkaTime,
}

impl CertIssuer {
    /// `signer`, issuing on `time`.
    pub fn new(signer: Arc<dyn CertSigner>, time: RafkaTime) -> Self {
        Self { signer, time }
    }

    /// The explicit choice of no certs, on `time`.
    pub fn no_certs(time: RafkaTime) -> Self {
        Self::new(Arc::new(NoCerts), time)
    }

    fn now(&self, what: &str) -> Result<u64, IssueFailure> {
        self.time.try_now_ms().ok_or_else(|| IssueFailure::NoRafkaTime(format!("this admin holds no rafka-time to issue {what} on")))
    }

    /// The member cert for `birth`, on this admin's rafka-time now.
    pub fn member(&self, birth: &BirthIdentity) -> Result<Vec<u8>, IssueFailure> {
        let span = tracing::info_span!(
            "rdm.node_admin.cert.create.via-join",
            node = %birth.name,
            node_id = %birth.node_id,
            incarnation_id = %birth.incarnation.0,
            issued_at_rafka_ms = tracing::field::Empty,
            cert_len = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let _g = span.enter();
        let now_ms = match self.now("a member cert") {
            Ok(n) => n,
            Err(e) => {
                span.record("outcome", "no-rafka-time");
                return Err(e);
            }
        };
        span.record("issued_at_rafka_ms", now_ms);
        match self.signer.issue_member(birth, now_ms) {
            Ok(bytes) => {
                span.record("cert_len", bytes.len() as u64);
                span.record("outcome", "issued");
                Ok(bytes)
            }
            Err(r) => {
                span.record("outcome", "refused");
                tracing::info_span!("rdm.node_admin.cert.reject.via-signer-refusal", node = %birth.name, node_id = %birth.node_id, incarnation_id = %birth.incarnation.0, refusal = %r.name, detail = %r.detail)
                    .in_scope(|| tracing::warn!("the signer refused to issue a member cert: the join is refused"));
                Err(IssueFailure::Refused(r))
            }
        }
    }

    /// The issuing material `mesh`'s first node-admin is launched with, on this admin's
    /// rafka-time now.
    pub fn mesh_issuer(&self, mesh: &str) -> Result<Vec<u8>, IssueFailure> {
        let span = tracing::info_span!("rdm.node_admin.cert.create.via-mesh-birth", mesh, issued_at_rafka_ms = tracing::field::Empty, cert_len = tracing::field::Empty, outcome = tracing::field::Empty);
        let _g = span.enter();
        let now_ms = match self.now("a mesh issuer") {
            Ok(n) => n,
            Err(e) => {
                span.record("outcome", "no-rafka-time");
                return Err(e);
            }
        };
        span.record("issued_at_rafka_ms", now_ms);
        match self.signer.issue_mesh_issuer(mesh, now_ms) {
            Ok(bytes) => {
                span.record("cert_len", bytes.len() as u64);
                span.record("outcome", "issued");
                Ok(bytes)
            }
            Err(r) => {
                span.record("outcome", "refused");
                tracing::info_span!("rdm.node_admin.cert.reject.via-signer-refusal", mesh, refusal = %r.name, detail = %r.detail).in_scope(|| tracing::warn!("the signer refused to issue a mesh issuer: the mesh's first admin is not launched"));
                Err(IssueFailure::Refused(r))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn birth() -> BirthIdentity {
        BirthIdentity { node_id: NodeId::mint(), incarnation: IncarnationId::mint(), name: "mesh1.rpc.1".parse().unwrap(), mesh: "mesh1".into(), endpoint_key: EndpointId("k".into()) }
    }

    // @feature: node-lifecycle
    #[test]
    fn an_issuer_whose_admin_holds_no_rafka_time_issues_nothing_and_says_so() {
        let issuer = CertIssuer::no_certs(RafkaTime::unadopted());
        match issuer.member(&birth()) {
            Err(IssueFailure::NoRafkaTime(why)) => assert!(why.contains("no rafka-time"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(issuer.mesh_issuer("mesh2"), Err(IssueFailure::NoRafkaTime(_))));
    }

    // @feature: node-lifecycle
    #[test]
    fn the_explicit_no_certs_choice_issues_empty_bytes_and_an_unchosen_wiring_resolves_to_a_named_refusal() {
        let time = RafkaTime::unadopted();
        time.adopt(5);
        let issuer = CertIssuer::new(CertChoice::NoCerts.resolve().unwrap(), time);
        assert_eq!(issuer.member(&birth()), Ok(Vec::new()));
        assert_eq!(issuer.mesh_issuer("mesh2"), Ok(Vec::new()));
        let refused = CertChoice::default().resolve().err().expect("no choice is refused");
        assert!(refused.contains("no cert choice") && refused.contains("NoCerts"), "{refused}");
    }
}
