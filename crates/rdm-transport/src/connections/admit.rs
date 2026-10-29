//! `node.connection.create` (node-connections-admit.md): the acceptor's verification of the member
//! cert a dialer presents on the first bi-stream of an inbound connection of a registered product
//! ALPN, before anything is served on it. It runs inside the accept loop RDM owns and on no Node RPC
//! or gossip connection (RDM never registers it for its own two ALPNs, `validate_alpns`). Not Node
//! RPC's execution-capacity admission (`crate::admission`, node-rpc.md §17).

use super::{write_control_msg, Alpn, Birth, ConnectionsRegistry, CLOSE_CODE_REFUSED, MAX_CONTROL_FRAME};
use crate::clock::RafkaTime;
use rdm_core::certs::VerifyingKey;
use std::sync::{Arc, RwLock};
use rdm_core::connections::{ConnectionKind as CoreKind, ConnectionState, NodeConnection};
use rdm_core::ConnectionId;
use serde::{Deserialize, Serialize};

/// Verification of a dialer's presented member cert during `node.connection.create`.
pub trait Admitter: Send + Sync {
    /// Verify `cert` presented by `caller` on `alpn`.
    ///
    /// Returns `Ok((cert_expires_at_ms, clock_name))` when admitted, or `Err(reason)` when refused.
    fn verify(&self, caller: &Birth, alpn: &[u8], cert: &[u8]) -> Result<(u64, &'static str), String>;

    /// Whether this admitter judges no cert and admits every connection by name: a node started
    /// with no certs (node-connections-admit.md `NoCerts`). Its admissions are spanned
    /// `via-no-certs-configured`, never `via-admitted`.
    fn admits_by_name(&self) -> bool {
        false
    }

    /// The clock this admitter judges expiries on, as its `verify` names it (node-connections-admit.md
    /// Verification: `rafka-time`, else `local-bootstrap`), so a refusal's span names the clock it was
    /// judged on too. An admitter that judges no cert names `no-certs`.
    fn clock(&self) -> &'static str {
        "no-certs"
    }
}

/// The `NoCerts` admitter (`node-connections-admit.md`: "admits every connection by name, spanned
/// `via-no-certs-configured`").
#[derive(Debug, Default)]
pub struct NoCertsAdmitter;

impl Admitter for NoCertsAdmitter {
    fn verify(&self, _caller: &Birth, _alpn: &[u8], _cert: &[u8]) -> Result<(u64, &'static str), String> {
        Ok((0, "no-certs"))
    }

    fn admits_by_name(&self) -> bool {
        true
    }
}

/// The fabric's root set this node verifies presented member certs against (fabric-certs.md §chain:
/// the anchors every node holds, one, or two during a root rotation). One shared holder, read afresh
/// by every admission, so `node.roots.update` replaces it at runtime and the next admission is judged
/// on the new set.
#[derive(Clone, Default)]
pub struct RootSet(Arc<RwLock<Vec<VerifyingKey>>>);

impl std::fmt::Debug for RootSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.current().iter().map(|k| rdm_core::launch::encode_hex(k.as_bytes()))).finish()
    }
}

impl RootSet {
    /// A holder of `roots`.
    pub fn new(roots: Vec<VerifyingKey>) -> Self {
        Self(Arc::new(RwLock::new(roots)))
    }

    /// The set held now.
    pub fn current(&self) -> Vec<VerifyingKey> {
        self.0.read().unwrap().clone()
    }

    /// Hold `roots` from now on: every later admission is judged on them.
    pub fn replace(&self, roots: Vec<VerifyingKey>) {
        *self.0.write().unwrap() = roots;
    }
}

/// The verification of `node.connection.create` (node-connections-admit.md Verification): the
/// presented member cert's chain to the [`RootSet`] held at the admission, both expiries judged on
/// this node's rafka-time (the local clock, `local-bootstrap`, before it holds one) with one staleness
/// floor of skew on either side. A refusal is named by the chain's own check
/// ([`rdm_core::certs::CertError`]); bytes that are no cert are `no-credential`.
pub struct CertAdmitter {
    roots: RootSet,
    time: RafkaTime,
}

impl CertAdmitter {
    /// The verifier judging on `roots`, as they stand at each admission, and on `time`.
    pub fn new(roots: RootSet, time: RafkaTime) -> Self {
        Self { roots, time }
    }
}

impl Admitter for CertAdmitter {
    fn verify(&self, _caller: &Birth, _alpn: &[u8], cert: &[u8]) -> Result<(u64, &'static str), String> {
        let Ok(signed) = rdm_core::certs::cert_from_bytes(cert) else {
            return Err(NO_CREDENTIAL.to_string());
        };
        let (now_ms, clock) = match self.time.try_now_ms() {
            Some(ms) => (ms, RAFKA_TIME),
            None => (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0), LOCAL_BOOTSTRAP),
        };
        let skew_ms = rdm_core::cadence::staleness_floor().as_millis() as u64;
        // The chain against the node's whole root set, its checks in the page's order (fabric-certs.md
        // §chain): the first failure names the refusal.
        rdm_core::certs::verify(&signed, &self.roots.current(), now_ms, skew_ms).map(|()| (signed.cert.expiry_ms, clock)).map_err(|e| e.as_str().to_string())
    }

    fn clock(&self) -> &'static str {
        if self.time.is_adopted() { RAFKA_TIME } else { LOCAL_BOOTSTRAP }
    }
}

/// The two clocks an admission is judged on (node-connections-admit.md Verification): the node's
/// rafka-time, else the local clock before it holds one.
const RAFKA_TIME: &str = "rafka-time";
const LOCAL_BOOTSTRAP: &str = "local-bootstrap";

/// The expiry (`cert_expires_at`) of the cert a dialer presented, as its bytes carry it: 0 when the
/// bytes are no cert (a `no-credential` refusal names no expiry).
pub(crate) fn presented_expiry(cert: &[u8]) -> u64 {
    rdm_core::certs::cert_from_bytes(cert).map_or(0, |signed| signed.cert.expiry_ms)
}

/// What a dialer writes first on the connection's control stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PresentFrame {
    pub(super) caller: Birth,
    pub(super) cert: Vec<u8>,
    /// The opener's W3C context (telemetry.md §3): observability only, never read to admit.
    pub(super) traceparent: Option<String>,
    pub(super) tracestate: Option<String>,
}

/// The acceptor's verdict, written back on the same stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum AdmitVerdict {
    Admitted,
    Refused { reason: String },
}

/// An admitted connection's control stream, and what its dialer presented.
pub(super) struct Admitted {
    pub(super) ctrl_send: iroh::endpoint::SendStream,
    pub(super) ctrl_recv: iroh::endpoint::RecvStream,
    pub(super) present: PresentFrame,
}

/// The one reason a dialer that presented no cert is refused under (node-connections-admit.md,
/// Failure and receipts; empty and undecodable bytes alike).
pub(super) const NO_CREDENTIAL: &str = "no-credential";

/// What the dialer presented on the control stream.
enum Presented {
    Frame(PresentFrame),
    /// The stream finished with nothing, or with bytes that are no presentation.
    Nothing,
    /// The read could not complete (the stream reset, the connection gone): neither verdict.
    Gone,
}

async fn read_present(recv: &mut iroh::endpoint::RecvStream) -> Presented {
    use iroh::endpoint::ReadExactError;
    let ended = |e: ReadExactError| match e {
        ReadExactError::FinishedEarly(_) => Presented::Nothing,
        ReadExactError::ReadError(_) => Presented::Gone,
    };
    let mut len_buf = [0u8; 4];
    match recv.read(&mut len_buf[..1]).await {
        Ok(None) | Ok(Some(0)) => return Presented::Nothing,
        Ok(Some(_)) => {}
        Err(_) => return Presented::Gone,
    }
    if let Err(e) = recv.read_exact(&mut len_buf[1..]).await {
        return ended(e);
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_CONTROL_FRAME {
        return Presented::Nothing;
    }
    let mut buf = vec![0u8; len];
    if let Err(e) = recv.read_exact(&mut buf).await {
        return ended(e);
    }
    postcard::from_bytes(&buf).map_or(Presented::Nothing, Presented::Frame)
}

/// Run the step on one inbound `connection` of `alpn`: `Some` when it is admitted and its verdict
/// written; `None` when it was refused or the dialer left before presenting. The dialer has one
/// staleness floor from the accept to open the control stream and present on it.
pub(super) async fn admit(registry: &ConnectionsRegistry, alpn: &Alpn, connection: &iroh::endpoint::Connection) -> Option<Admitted> {
    let floor = rdm_core::cadence::staleness_floor();
    let deadline = tokio::time::Instant::now() + floor;
    let (mut ctrl_send, mut ctrl_recv) = match tokio::time::timeout_at(deadline, connection.accept_bi()).await {
        Ok(Ok(bi)) => bi,
        Ok(Err(_)) => return None,
        Err(_) => {
            refuse(registry, alpn, connection, None, None, NO_CREDENTIAL).await;
            return None;
        }
    };
    let present = match tokio::time::timeout_at(deadline, read_present(&mut ctrl_recv)).await {
        Ok(Presented::Frame(p)) => p,
        Ok(Presented::Gone) => return None,
        Ok(Presented::Nothing) | Err(_) => {
            refuse(registry, alpn, connection, Some(&mut ctrl_send), None, NO_CREDENTIAL).await;
            return None;
        }
    };
    let own = registry.inner.sink.own_birth();
    let alpn_str = String::from_utf8_lossy(&alpn.name).into_owned();
    let (verify_res, by_name) = {
        let admitter = registry.inner.admitter.lock().unwrap().clone();
        (admitter.verify(&present.caller, &alpn.name, &present.cert), admitter.admits_by_name())
    };
    match verify_res {
        Err(reason) => {
            refuse(registry, alpn, connection, Some(&mut ctrl_send), Some(&present), &reason).await;
            None
        }
        Ok((cert_expires_at, clock)) => {
            // A node started with no certs judged no cert: it admitted by name, and says so.
            let span = if by_name {
                tracing::info_span!("rdm.node.connection.serve.via-no-certs-configured", source = %present.caller.name, destination = %own.name, connection = connection.stable_id(), alpn = %alpn_str)
            } else {
                tracing::info_span!(
                    "rdm.node.connection.serve.via-admitted",
                    source = %present.caller.name,
                    destination = %own.name,
                    connection = connection.stable_id(),
                    alpn = %alpn_str,
                    cert_expires_at,
                    clock,
                )
            };
            if let Some(tp) = present.traceparent.as_deref() {
                crate::set_parent(&span, tp);
            }
            span.in_scope(|| if by_name { tracing::info!("inbound connection admitted by name: this node holds no certs") } else { tracing::info!("inbound connection admitted") });
            if write_control_msg(&mut ctrl_send, &AdmitVerdict::Admitted).await.is_err() {
                return None;
            }
            Some(Admitted { ctrl_send, ctrl_recv, present })
        }
    }
}

/// Refuse `connection` with `reason` (node-connections-admit.md step 4): the verdict written back on
/// the control stream when there is one, the `Refused` fact recorded with the birth this node's
/// the topology view names for the dialer's key as its source (a key it cannot name gets the span and no
/// row), then the connection closed with 412 carrying the reason. A dialer that presented waits for
/// its verdict, so the close waits up to a floor for the dialer to read it and leave first.
async fn refuse(
    registry: &ConnectionsRegistry,
    alpn: &Alpn,
    connection: &iroh::endpoint::Connection,
    ctrl_send: Option<&mut iroh::endpoint::SendStream>,
    present: Option<&PresentFrame>,
    reason: &str,
) {
    let floor = rdm_core::cadence::staleness_floor();
    let own = registry.inner.sink.own_birth();
    let key = connection.remote_id();
    let dialer = registry.inner.resolver.by_endpoint(&key).map(|r| Birth { name: r.name, node_id: r.node_id, incarnation: r.incarnation });
    let source = dialer.as_ref().map_or_else(|| key.fmt_short().to_string(), |b| b.name.to_string());
    // The same fields as an admission's span (node-connections-admit.md Spans): the presented cert's
    // expiry (0 when nothing presented parses as a cert) and the clock the admitter judges on.
    let cert_expires_at = present.map_or(0, |p| presented_expiry(&p.cert));
    let clock = registry.inner.admitter.lock().unwrap().clock();
    let span = tracing::info_span!(
        "rdm.node.connection.reject.via-refused",
        source = %source,
        destination = %own.name,
        connection = connection.stable_id(),
        alpn = %String::from_utf8_lossy(&alpn.name),
        cert_expires_at,
        clock,
        reason = %reason,
    );
    if let Some(tp) = present.and_then(|p| p.traceparent.as_deref()) {
        crate::set_parent(&span, tp);
    }
    span.in_scope(|| tracing::info!("inbound connection refused at node.connection.create"));
    let answered = ctrl_send.is_some();
    if let Some(send) = ctrl_send {
        let _ = write_control_msg(send, &AdmitVerdict::Refused { reason: reason.to_string() }).await;
        let _ = send.finish();
    }
    if let Some(dialer) = dialer {
        let refused_row = NodeConnection {
            id: ConnectionId::mint(),
            source: dialer.to_end(),
            destination: own.to_end(),
            kind: CoreKind::Direct,
            alpn: alpn.name.clone(),
            state: ConnectionState::Refused,
            carrier: None,
            recovery: None,
            reason: Some(reason.to_string()),
            logged_at_ms: 0,
        };
        // Owed, never awaited (Q169): the refusal closes the connection at once.
        span.in_scope(|| registry.inner.sink.record_row(super::owed(refused_row)));
    }
    if answered && present.is_some() {
        let _ = tokio::time::timeout(floor, connection.closed()).await;
    }
    connection.close(iroh::endpoint::VarInt::from_u32(CLOSE_CODE_REFUSED), reason.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::certs::{
        BirthIdentity, CA_CERT_TTL_SECS, CaKey, ISSUING_CERT_TTL_SECS, IssuingKey, SigningKey,
        cert_to_bytes, issue,
    };
    use rdm_core::{EndpointId, IncarnationId, NodeId, NodeKind, PathName};

    /// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:12 — "a correctly signed,
    /// unexpired cert of this fabric is admitted whoever presents it, and the step reads no node
    /// kind, no primary and no sender identity."
    /// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:23 — "Verification | The
    /// member's signature against its issuer's key; the issuer's signature against the fabric CA's
    /// key and the CA's against a root of the node's set; the issuer's mesh is the first segment
    /// of the member's path.name; the three expiries (CA, issuer, member), judged on rafka-time
    /// (else `local-bootstrap`) with one staleness floor of skew on either side"
    /// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:24 — "Result | `admitted`:
    /// the connection is served. `refused { reason }` with `no-credential`, `ca-bad-signature`,
    /// `ca-expired`, `issuer-bad-signature`, `issuer-expired`, `issuer-mesh-mismatch`,
    /// `bad-signature` or `expired`"
    /// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:27 — "`NoCerts` | A
    /// node-admin started `NoCerts` admits every connection by name, spanned
    /// `via-no-certs-configured`; an empty cert presented to a fabric with certs is
    /// `no-credential`"
    #[test]
    fn cert_admitter_and_no_certs_admitter_verify_chain_expiries_mesh_and_root_set_replacement() {
        let caller = Birth {
            name: PathName::new("mesh1", NodeKind::Gateway, 1),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
        };

        // 1. NoCertsAdmitter admits by name on clock "no-certs".
        let no_certs = NoCertsAdmitter;
        assert!(no_certs.admits_by_name());
        assert_eq!(no_certs.clock(), "no-certs");
        assert_eq!(
            no_certs.verify(&caller, b"app/1", b""),
            Ok((0, "no-certs"))
        );

        // 2. CertAdmitter switches from local-bootstrap to rafka-time once RafkaTime is adopted.
        let root_key = SigningKey::from_bytes(&[11u8; 32]);
        let roots = RootSet::new(vec![root_key.verifying_key()]);
        let time = RafkaTime::unadopted();
        let admitter = CertAdmitter::new(roots.clone(), time.clone());
        assert!(!admitter.admits_by_name());
        assert_eq!(admitter.clock(), "local-bootstrap");
        time.adopt(10_000);
        assert_eq!(admitter.clock(), "rafka-time");

        // 3. Empty or malformed cert bytes are refused as `no-credential` with presented_expiry 0.
        assert_eq!(presented_expiry(b""), 0);
        assert_eq!(
            admitter.verify(&caller, b"app/1", b""),
            Err("no-credential".to_string())
        );
        assert_eq!(
            admitter.verify(&caller, b"app/1", b"not-a-cert"),
            Err("no-credential".to_string())
        );

        // 4. Valid member cert under mesh1 issuer and root is admitted and returns its expiry and clock,
        // whoever presents it (reading no node kind, no primary, and no sender identity).
        let ca = CaKey::mint(&root_key, CA_CERT_TTL_SECS, 1_000);
        let issuer = IssuingKey::mint(
            &ca,
            "mesh1",
            1_000,
            1_000 + ISSUING_CERT_TTL_SECS * 1000,
        )
        .expect("mint mesh1 issuer");
        let birth_id = BirthIdentity {
            node_id: caller.node_id.clone(),
            incarnation: caller.incarnation.clone(),
            mesh: "mesh1".to_string(),
            name: caller.name.clone(),
            endpoint_key: EndpointId("ep-gw-1".to_string()),
        };
        let signed = issue(&issuer, &birth_id, 5_000, 65_000).expect("issue member cert");
        let cert_bytes = cert_to_bytes(&signed);
        assert_eq!(presented_expiry(&cert_bytes), 65_000);
        for other_kind in [
            NodeKind::Gateway,
            NodeKind::NodeAdmin,
            NodeKind::from_segment("rpc").unwrap(),
        ] {
            let presenter = Birth {
                name: PathName::new("mesh2", other_kind, 9),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
            };
            assert_eq!(
                admitter.verify(&presenter, b"app/1", &cert_bytes),
                Ok((65_000, "rafka-time"))
            );
        }

        // 5. Advancing rafka-time past expiry + staleness_floor refuses with `expired`.
        let skew_ms = rdm_core::cadence::staleness_floor().as_millis() as u64;
        time.adopt(65_000 + skew_ms + 1_000);
        assert_eq!(
            admitter.verify(&caller, b"app/1", &cert_bytes),
            Err("expired".to_string())
        );

        // 6. Mesh mismatch between issuer ("mesh1") and member path ("mesh2.gateway.1") is refused `issuer-mesh-mismatch`.
        time.adopt(10_000);
        let mut wrong_mesh_signed = signed.clone();
        wrong_mesh_signed.cert.name = PathName::new("mesh2", NodeKind::Gateway, 1);
        wrong_mesh_signed.cert.mesh = "mesh2".to_string();
        let wrong_mesh_bytes = cert_to_bytes(&wrong_mesh_signed);
        assert_eq!(
            admitter.verify(&caller, b"app/1", &wrong_mesh_bytes),
            Err("issuer-mesh-mismatch".to_string())
        );

        // 7. RootSet::replace dynamically updates the trusted roots, refusing certs under an untrusted root with `ca-bad-signature`.
        let other_root = SigningKey::from_bytes(&[22u8; 32]);
        roots.replace(vec![other_root.verifying_key()]);
        assert_eq!(
            admitter.verify(&caller, b"app/1", &cert_bytes),
            Err("ca-bad-signature".to_string())
        );
    }
}

