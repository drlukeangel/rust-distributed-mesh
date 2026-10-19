//! `node.connections.admit` (node-connections-admit.md): the acceptor's verification of the member
//! cert a dialer presents on the first bi-stream of an inbound connection of a registered product
//! ALPN, before anything is served on it. It runs inside the accept loop RDM owns and on no Node RPC
//! or gossip connection (RDM never registers it for its own two ALPNs, `validate_alpns`). Not Node
//! RPC's execution-capacity admission (`crate::admission`, node-rpc.md §17).

use super::{read_control_msg, write_control_msg, Alpn, Birth, ConnectionsRegistry, CLOSE_CODE_REFUSED};
use rdm_core::connections::{ConnectionKind as CoreKind, ConnectionState, NodeConnection};
use rdm_core::ConnectionId;
use serde::{Deserialize, Serialize};

/// Verification of a dialer's presented member cert during `node.connections.admit`.
pub trait Admitter: Send + Sync {
    /// Verify `cert` presented by `caller` on `alpn`.
    ///
    /// Returns `Ok((cert_expires_at_ms, clock_name))` when admitted, or `Err(reason)` when refused.
    fn verify(&self, caller: &Birth, alpn: &[u8], cert: &[u8]) -> Result<(u64, &'static str), String>;
}

/// The `NoCerts` admitter (`node-connections-admit.md`: "admits every connection by name, spanned
/// `via-no-certs-configured`").
#[derive(Debug, Default)]
pub struct NoCertsAdmitter;

impl Admitter for NoCertsAdmitter {
    fn verify(&self, _caller: &Birth, _alpn: &[u8], _cert: &[u8]) -> Result<(u64, &'static str), String> {
        Ok((0, "no-certs"))
    }
}

/// What a dialer writes first on the connection's control stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PresentFrame {
    pub(super) caller: Birth,
    pub(super) cert: Vec<u8>,
    pub(super) traceparent: Option<String>,
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

/// Run the step on one inbound `connection` of `alpn`: `Some` when it is admitted and its verdict
/// written; `None` when it was refused (the verdict written, the `Refused` fact recorded, the
/// connection closed) or never presented.
pub(super) async fn admit(registry: &ConnectionsRegistry, alpn: &Alpn, connection: &iroh::endpoint::Connection) -> Option<Admitted> {
    let floor = rdm_core::cadence::staleness_floor();
    let Ok(Ok((mut ctrl_send, mut ctrl_recv))) = tokio::time::timeout(floor, connection.accept_bi()).await else {
        return None;
    };
    let present: PresentFrame = match tokio::time::timeout(floor, read_control_msg(&mut ctrl_recv)).await {
        Ok(Ok(Some(p))) => p,
        _ => return None,
    };
    let own = registry.inner.sink.own_birth();
    let alpn_str = String::from_utf8_lossy(&alpn.name).into_owned();
    let verify_res = registry.inner.admitter.lock().unwrap().verify(&present.caller, &alpn.name, &present.cert);
    match verify_res {
        Err(reason) => {
            let span = tracing::info_span!(
                "rdm.node.connections.reject.via-refused",
                source = %present.caller.name,
                destination = %own.name,
                connection = connection.stable_id(),
                alpn = %alpn_str,
                cert_expires_at = 0u64,
                clock = "no-certs",
                reason = %reason,
            );
            if let Some(tp) = present.traceparent.as_deref() {
                crate::set_parent(&span, tp);
            }
            span.in_scope(|| tracing::info!("inbound connection refused at node.connections.admit"));
            let refused_row = NodeConnection {
                id: ConnectionId::mint(),
                source: present.caller.to_end(),
                destination: own.to_end(),
                kind: CoreKind::Direct,
                alpn: alpn.name.clone(),
                state: ConnectionState::Refused,
                carrier: None,
                recovery: None,
                reason: Some(reason.clone()),
                logged_at_ms: 0,
            };
            let _ = registry.inner.sink.record_row(refused_row).await;
            let _ = write_control_msg(&mut ctrl_send, &AdmitVerdict::Refused { reason: reason.clone() }).await;
            let _ = ctrl_send.finish();
            let _ = tokio::time::timeout(floor, connection.closed()).await;
            connection.close(iroh::endpoint::VarInt::from_u32(CLOSE_CODE_REFUSED), reason.as_bytes());
            None
        }
        Ok((cert_expires_at, clock)) => {
            let span = tracing::info_span!(
                "rdm.node.connections.create.via-admitted",
                source = %present.caller.name,
                destination = %own.name,
                connection = connection.stable_id(),
                alpn = %alpn_str,
                cert_expires_at,
                clock,
            );
            if let Some(tp) = present.traceparent.as_deref() {
                crate::set_parent(&span, tp);
            }
            span.in_scope(|| tracing::info!("inbound connection admitted"));
            if write_control_msg(&mut ctrl_send, &AdmitVerdict::Admitted).await.is_err() {
                return None;
            }
            Some(Admitted { ctrl_send, ctrl_recv, present })
        }
    }
}
