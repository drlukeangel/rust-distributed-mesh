//! The node's connections reconciler: the owed Proxy delete and the dial an accept wakes
//! (`docs/architecture/connections.md` §9, §10; Q348).
//!
//! Every node runs it internally in `rdm-node`. Node-admin does not monitor Proxy routes and does not
//! send reconnect commands.
//!
//! A lost Direct is dialled on `Send` and nothing else (Q344, Q348): the open that lost it dials
//! within its attempts, and when they are spent the dial ends to the application by name and its
//! `on_call_failed` is told. No schedule is derived from a `Direct Failed` row, and nothing here dials
//! again because of one.
//!
//! What this module does:
//!
//! - **The owed Proxy delete** (§10): a Direct `Connected` beside this source's own active Proxy on
//!   the same ALPN owes `Proxy Disconnected(direct-restored)`; a write that was refused is attempted
//!   again each tick while owed.
//! - **The accept trigger** (§9, §13 G, R-CN1): a connection accepted from the exact destination birth
//!   this node reaches through an active Proxy wakes one dial of this node's own toward that birth, on
//!   `Send`. One pending dial per `(destination, destination_incarnation, alpn)`; an accept arriving
//!   while it is pending coalesces into it, and at most one such dial runs at a time on the node. A
//!   dial whose attempts are spent ends the item: the next accept wakes another.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdm_core::connections::ConnectionEnd;
use rdm_core::{IncarnationId, NodeId};
use rdm_transport::ResolvedNode;
use rdm_transport::connections::{Birth, ConnectError, ConnectionsRegistry};

use crate::connections_writer::ConnectionsWriter;

/// How often the reconciler attempts an owed Proxy delete whose write was refused, without an
/// explicit wake.
pub const RECONCILER_TICK: Duration = Duration::from_millis(500);

/// One direct dial of this node's default data plane toward an exact destination birth, on `Send`
/// (`connections.md` §9), answering the call outcome's name for the attempt's span.
pub type RecoveryDial =
    Arc<dyn Fn(ConnectionEnd) -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync>;

/// One direct dial of this node's data plane toward an exact destination birth on a specific
/// `alpn`, on `Send` (`connections.md` §9), answering the outcome's name for the attempt's span.
pub type AlpnRecoveryDial = Arc<
    dyn Fn(ConnectionEnd, Vec<u8>) -> Pin<Box<dyn Future<Output = String> + Send>> + Send + Sync,
>;

/// One dial an accept woke.
#[derive(Clone)]
pub(crate) struct Pending {
    pub(crate) destination: ConnectionEnd,
    pub(crate) alpn: Vec<u8>,
    pub(crate) traceparent: Option<String>,
}

/// The reconciler's state owned by [`ConnectionsWriter`].
pub(crate) struct ReconnectState {
    pub(crate) recovery: Mutex<HashMap<(NodeId, IncarnationId, Vec<u8>), Pending>>,
    pub(crate) wake: tokio::sync::Notify,
    pub(crate) attempt_gate: tokio::sync::Mutex<()>,
    pub(crate) in_flight: AtomicUsize,
    pub(crate) max_concurrent: AtomicUsize,
}

impl Default for ReconnectState {
    fn default() -> Self {
        Self {
            recovery: Mutex::new(HashMap::new()),
            wake: tokio::sync::Notify::new(),
            attempt_gate: tokio::sync::Mutex::new(()),
            in_flight: AtomicUsize::new(0),
            max_concurrent: AtomicUsize::new(0),
        }
    }
}

/// Adapt a legacy single-ALPN [`RecoveryDial`] into an [`AlpnRecoveryDial`].
pub fn adapt_recovery_dial(dial: RecoveryDial) -> AlpnRecoveryDial {
    Arc::new(move |destination: ConnectionEnd, _alpn: Vec<u8>| dial(destination))
}

/// Build an [`AlpnRecoveryDial`] that dials registered data-plane ALPNs through `registry`
/// ([`ConnectionsRegistry::probe_direct`]) and falls back to `fallback` for the control/default ALPN.
pub fn registry_recovery_dial(
    registry: &ConnectionsRegistry,
    fallback: Option<RecoveryDial>,
) -> AlpnRecoveryDial {
    let reg = registry.clone();
    Arc::new(move |destination: ConnectionEnd, alpn: Vec<u8>| {
        let reg = reg.clone();
        let fallback = fallback.clone();
        Box::pin(async move {
            if reg.has_alpn(&alpn) {
                let Some(inc) = destination.incarnation.clone() else {
                    return "NotSent".to_string();
                };
                let birth = Birth { mesh: destination.mesh.clone(), r#type: destination.r#type.clone(),
                    path: destination.path.clone(), name: destination.name.clone(),
                    node_id: destination.node_id,
                    incarnation: inc,
                };
                match reg.probe_direct(&birth, &alpn).await {
                    Ok(conn) => {
                        // The dial names the entry it opened, its row owed (Q169).
                        tracing::Span::current().record("entry", conn.id().to_string().as_str());
                        "Reply".to_string()
                    }
                    Err(ConnectError::Refused { .. }) => "Refused".to_string(),
                    Err(_) => "NotSent".to_string(),
                }
            } else if let Some(fb) = fallback {
                fb(destination).await
            } else {
                "NotSent".to_string()
            }
        })
    })
}

/// The destinations of the dials an accept woke and that have not ended.
pub(crate) fn recovery_pending(writer: &ConnectionsWriter) -> Vec<ConnectionEnd> {
    let mut out: Vec<ConnectionEnd> = writer.reconnect_state().recovery.lock().unwrap().values().map(|p| p.destination.clone()).collect();
    out.sort_by_key(|d| (d.node_id.to_string(), d.incarnation.as_ref().map(ToString::to_string)));
    out
}

/// End the dial `key` names, naming why: once per dial.
fn spend(
    writer: &ConnectionsWriter,
    key: &(NodeId, IncarnationId, Vec<u8>),
    source: &NodeId,
    destination: &ConnectionEnd,
    reason: &'static str,
) {
    if writer.reconnect_state().recovery.lock().unwrap().remove(key).is_some() {
        spent_span(source, destination, reason);
    }
}

/// A woken dial ended, without a dial of its own or after it (`connections.md` §12).
fn spent_span(source: &NodeId, destination: &ConnectionEnd, reason: &'static str) {
    tracing::info_span!(
        "rdm.node_admin.connection.remove.via-recovery-attempt",
        source_node_id = %source,
        destination_id = %destination.node_id, destination_path = %destination.path, destination_name = %destination.name,
        destination_node_id = %destination.node_id,
        destination_incarnation = %destination.incarnation.as_ref().map(ToString::to_string).unwrap_or_default(),
        reason,
    )
    .in_scope(|| tracing::info!("the dial the accept woke has ended"));
}

/// The node's one connections reconciler (`connections.md` §9, §10).
#[derive(Clone)]
pub struct Reconciler {
    writer: Arc<ConnectionsWriter>,
}

impl std::fmt::Debug for Reconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reconciler").finish()
    }
}

impl Reconciler {
    /// Create a reconciler over `writer`.
    pub fn new(writer: Arc<ConnectionsWriter>) -> Self {
        Self { writer }
    }

    /// Dials an accept woke that are executing on this node.
    pub fn in_flight_attempts(&self) -> usize {
        self.writer.reconnect_state().in_flight.load(Ordering::SeqCst)
    }

    /// Peak concurrent woken dials observed on this node.
    pub fn max_concurrent_attempts(&self) -> usize {
        self.writer.reconnect_state().max_concurrent.load(Ordering::SeqCst)
    }

    /// Make every dial an accept woke, one at a time on this node.
    pub async fn run_pending(&self, dial: &AlpnRecoveryDial) {
        run_pending(&self.writer, dial).await;
    }
}

/// Make every dial an accept woke on `writer`, at most one at a time on the node, each once on `Send`
/// (`connections.md` §9; Q348). Its Direct `Connected` row is the data plane's own, and the owed Proxy
/// delete follows it (§10); a dial whose attempts are spent ends here, and nothing dials again until the
/// next accept wakes it.
pub async fn run_pending(writer: &ConnectionsWriter, dial: &AlpnRecoveryDial) {
    let state = writer.reconnect_state();
    let keys: Vec<(NodeId, IncarnationId, Vec<u8>)> = state.recovery.lock().unwrap().keys().cloned().collect();
    for key in keys {
        // The node-wide single-attempt gate: at most one woken dial at a time on this node.
        let _attempt_guard = state.attempt_gate.lock().await;
        let Some(Pending { destination, alpn, traceparent }) = state.recovery.lock().unwrap().get(&key).cloned() else {
            continue;
        };
        let source = writer.own().node_id;
        let ended = {
            let held_arc = writer.held();
            let held = held_arc.lock().unwrap();
            if held.end_superseded(&destination) == Some(true) {
                Some("destination-superseded")
            } else if matches!(
                held.active_direct_for(&source, &destination.node_id, &alpn),
                Some(Some(connection)) if connection.destination.incarnation == destination.incarnation
            ) {
                Some("direct-connected")
            } else {
                None
            }
        };
        if let Some(reason) = ended {
            spend(writer, &key, &source, &destination, reason);
            continue;
        }
        let cur = state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        state.max_concurrent.fetch_max(cur, Ordering::SeqCst);
        let span = tracing::info_span!(
            "rdm.node_admin.connection.create.via-recovery-dial",
            source_node_id = %source,
            destination_id = %destination.node_id, destination_path = %destination.path, destination_name = %destination.name,
            destination_node_id = %destination.node_id,
            destination_incarnation = %destination.incarnation.as_ref().map(ToString::to_string).unwrap_or_default(),
            outcome = tracing::field::Empty,
            fact = tracing::field::Empty,
            entry = tracing::field::Empty,
        );
        if let Some(tp) = &traceparent {
            rdm_transport::set_parent(&span, tp);
        }
        let outcome = tracing::Instrument::instrument(dial(destination.clone(), alpn.clone()), span.clone()).await;
        state.in_flight.fetch_sub(1, Ordering::SeqCst);
        // The dial's own result decides, in memory (Q169): its row is owed and lands behind it.
        let connected = outcome == "Reply";
        span.record("outcome", outcome.as_str());
        span.record("fact", if connected { "connected" } else { "failed" });
        span.in_scope(|| tracing::info!("this node dialled its own connection toward the destination birth an accept woke it for"));
        drop(span);
        spend(writer, &key, &source, &destination, if connected { "direct-connected" } else { "dial-spent" });
    }
}

/// Spawn the node's one connections reconciler (`connections.md` §9, §10) with an
/// [`AlpnRecoveryDial`].
pub fn spawn_alpn_reconciler(
    writer: &ConnectionsWriter,
    every: Duration,
    dial: AlpnRecoveryDial,
) -> tokio::task::JoinHandle<()> {
    spawn_reconciler_inner(writer, every, Some(dial))
}

/// Spawn the node's one connections reconciler (`connections.md` §9, §10) with an optional
/// single-ALPN [`RecoveryDial`].
pub fn spawn_reconciler(
    writer: &ConnectionsWriter,
    every: Duration,
    dial: Option<RecoveryDial>,
) -> tokio::task::JoinHandle<()> {
    spawn_reconciler_inner(writer, every, dial.map(adapt_recovery_dial))
}

fn spawn_reconciler_inner(
    writer: &ConnectionsWriter,
    every: Duration,
    dial: Option<AlpnRecoveryDial>,
) -> tokio::task::JoinHandle<()> {
    let w = writer.clone();
    writer.runtime_handle().spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(every) => {}
                _ = w.reconnect_state().wake.notified() => {}
            }
            if !w.owed().is_empty() {
                let _ = w.settle_owed().await;
            }
            if let Some(dial) = &dial {
                run_pending(&w, dial).await;
            }
        }
    })
}

/// Handle an inbound connection accepted from `peer` on `alpn` (`connections.md` §9.4, §10, §13 G).
pub(crate) fn accepted_on_alpn(
    writer: &ConnectionsWriter,
    peer: &Birth,
    alpn: &[u8],
    peer_short: &str,
) {
    let destination = peer.to_end();
    let (source, proxied) = {
        let held_arc = writer.held();
        let held = held_arc.lock().unwrap();
        let Some(proxy) = held
            .own_active_proxies()
            .into_iter()
            .find(|p| p.destination.node_id == destination.node_id && p.alpn.as_slice() == alpn)
        else {
            return;
        };
        let direct = matches!(
            held.active_direct_for(&proxy.source.node_id, &destination.node_id, &proxy.alpn),
            Some(Some(_))
        );
        (
            proxy.source.node_id.clone(),
            (proxy.destination.clone(), proxy.alpn.clone(), direct),
        )
    };
    let (named, proxy_alpn, direct) = proxied;
    let key = (
        destination.node_id.clone(),
        peer.incarnation.clone(),
        proxy_alpn.clone(),
    );
    let span = tracing::info_span!(
        "rdm.node_admin.connection.update.via-accept-trigger",
        source_node_id = %source,
        destination_id = %destination.node_id, destination_path = %destination.path, destination_name = %destination.name,
        destination_node_id = %destination.node_id,
        destination_incarnation = %peer.incarnation,
        proxy_destination_node_id = %named.node_id,
        proxy_destination_incarnation = %named.incarnation.as_ref().map(ToString::to_string).unwrap_or_default(),
        peer = %peer_short,
        outcome = tracing::field::Empty,
    );
    let outcome = if (&named.node_id, &named.incarnation)
        != (&destination.node_id, &destination.incarnation)
    {
        "another-birth"
    } else if direct {
        "direct-connected"
    } else {
        match writer.reconnect_state().recovery.lock().unwrap().entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if e.get().traceparent.is_none() {
                    e.get_mut().traceparent = rdm_transport::traceparent_of(&span);
                }
                "coalesced"
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(Pending {
                    destination: destination.clone(),
                    alpn: proxy_alpn,
                    traceparent: rdm_transport::traceparent_of(&span),
                });
                "pending"
            }
        }
    };
    span.record("outcome", outcome);
    span.in_scope(|| {
        tracing::info!(
            "an accept from the destination: a recovery trigger for this node's own dial, never a Direct entry"
        )
    });
    if matches!(outcome, "pending" | "coalesced") {
        writer.reconnect_state().wake.notify_one();
    }
}

/// Handle an inbound connection accepted from `node` across any active Proxy to `node.node_id`.
pub(crate) fn accepted_resolved(writer: &ConnectionsWriter, node: &ResolvedNode) {
    let alpns: Vec<Vec<u8>> = {
        let held_arc = writer.held();
        let held = held_arc.lock().unwrap();
        held.own_active_proxies()
            .into_iter()
            .filter(|p| p.destination.node_id == node.node_id)
            .map(|p| p.alpn.clone())
            .collect()
    };
    let birth = Birth { mesh: node.mesh.clone(), r#type: node.r#type.clone(),
        path: node.path.clone(), name: node.name.clone(),
        node_id: node.node_id.clone(),
        incarnation: node.incarnation.clone(),
    };
    let peer_short = node.endpoint_id.fmt_short().to_string();
    for alpn in alpns {
        accepted_on_alpn(writer, &birth, &alpn, &peer_short);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryConnectionsStorage;
    use rdm_core::connections::{ConnectionKind, ConnectionState, ConnectionsHeld, DirectRecovery, NodeConnection};
    use rdm_core::ConnectionId;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct FieldVisitor(BTreeMap<String, String>);

    impl tracing::field::Visit for FieldVisitor {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}").trim_matches('"').to_string());
        }
    }

    #[derive(Clone, Default)]
    struct SpanRecorder {
        next_id: Arc<AtomicUsize>,
        spans: Arc<Mutex<BTreeMap<u64, (String, BTreeMap<String, String>)>>>,
    }

    impl tracing::Subscriber for SpanRecorder {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed) as u64 + 1;
            let mut v = FieldVisitor::default();
            attrs.record(&mut v);
            self.spans
                .lock()
                .unwrap()
                .insert(id, (attrs.metadata().name().to_string(), v.0));
            tracing::span::Id::from_u64(id)
        }
        fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
            let mut v = FieldVisitor::default();
            values.record(&mut v);
            if let Some((_, fields)) = self.spans.lock().unwrap().get_mut(&span.into_u64()) {
                fields.extend(v.0);
            }
        }
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, _event: &tracing::Event<'_>) {}
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    // @feature: connections-reconnect
    /// CONTRACT (connections.md §9, §13 G, R-CN1; Q348): a source holding a `Direct Failed` row and an
    /// active Proxy to a destination has nothing pending from the row: no dial is scheduled from it.
    /// An accept from the exact destination birth wakes one dial (`pending`), a second accept coalesces
    /// into it, and an accept from another birth is ignored (`another-birth`). The reconciler makes the
    /// woken dial once; a dial whose attempts are spent ends the item (`dial-spent`) and the next pass
    /// dials nothing. A second accept wakes a dial again, whose `Reply` ends it `direct-connected`.
    #[test]
    fn an_accept_wakes_one_dial_and_a_spent_dial_is_not_made_again() {
        let recorder = SpanRecorder::default();
        let dispatch = tracing::Dispatch::new(recorder.clone());
        let _guard = tracing::dispatcher::set_default(&dispatch);
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("build current-thread runtime");
        rt.block_on(async {
            let end = |r#type: &str, name: u32, incarnation: IncarnationId| ConnectionEnd {
                mesh: "mesh1".into(), r#type: r#type.into(), path: format!("mesh1.{type}", type = r#type), name: name.to_string(), node_id: NodeId::mint(), incarnation: Some(incarnation),
            };
            let src = end("gateway", 1, IncarnationId::mint());
            let carrier = end("gateway", 2, IncarnationId::mint());
            let dst_inc = IncarnationId::mint();
            let dst = end("broker", 1, dst_inc.clone());
            let alpn = b"app-reconnect/1".to_vec();
            let writer = Arc::new(ConnectionsWriter::new(src.clone(), Arc::new(MemoryConnectionsStorage::default()), Arc::new(Mutex::new(ConnectionsHeld::new()))));
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
            let row = |kind: ConnectionKind, state: ConnectionState, carrier: Option<ConnectionEnd>, recovery: Option<DirectRecovery>, at: u64| NodeConnection {
                id: ConnectionId::mint(), source: src.clone(), destination: dst.clone(), kind, alpn: alpn.clone(), state, carrier, recovery, reason: None, logged_at_ms: at,
            };
            writer.record(row(ConnectionKind::Direct, ConnectionState::Failed, None, Some(DirectRecovery { recovery_round: 1, attempt_ordinal: 1 }), now)).await.expect("record failed");
            writer.record(row(ConnectionKind::Proxy, ConnectionState::Connected, Some(carrier), None, now + 1)).await.expect("record proxy");
            assert!(recovery_pending(&writer).is_empty(), "a Direct Failed row schedules no dial (Q348)");

            let birth = |incarnation: IncarnationId| Birth { mesh: dst.mesh.clone(), r#type: dst.r#type.clone(), path: dst.path.clone(), name: dst.name.clone(), node_id: dst.node_id.clone(), incarnation };
            accepted_on_alpn(&writer, &birth(dst_inc.clone()), &alpn, "peer-cur");
            accepted_on_alpn(&writer, &birth(dst_inc.clone()), &alpn, "peer-cur");
            accepted_on_alpn(&writer, &birth(IncarnationId::mint()), &alpn, "peer-stale");
            assert_eq!(recovery_pending(&writer), vec![dst.clone()], "one dial woken per destination birth and ALPN");

            let outcomes = Arc::new(Mutex::new(vec!["NotSent".to_string(), "Reply".to_string()]));
            let calls = Arc::new(AtomicUsize::new(0));
            let (o, c) = (outcomes.clone(), calls.clone());
            let dial: AlpnRecoveryDial = Arc::new(move |_dest, _alpn| {
                let (o, c) = (o.clone(), c.clone());
                Box::pin(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    o.lock().unwrap().remove(0)
                })
            });
            let reconciler = Reconciler::new(writer.clone());
            reconciler.run_pending(&dial).await;
            assert_eq!(calls.load(Ordering::SeqCst), 1, "the woken dial is made once");
            assert!(recovery_pending(&writer).is_empty(), "a spent dial ends the item");
            reconciler.run_pending(&dial).await;
            assert_eq!(calls.load(Ordering::SeqCst), 1, "nothing dials again after a spent dial");

            accepted_on_alpn(&writer, &birth(dst_inc.clone()), &alpn, "peer-cur");
            reconciler.run_pending(&dial).await;
            assert_eq!(calls.load(Ordering::SeqCst), 2, "the next accept wakes another dial");
            assert_eq!((reconciler.in_flight_attempts(), reconciler.max_concurrent_attempts()), (0, 1));
            assert!(recovery_pending(&writer).is_empty());
        });
        drop(rt);

        let recorded: Vec<(String, BTreeMap<String, String>)> = recorder.spans.lock().unwrap().values().cloned().collect();
        let has = |name: &str, key: &str, value: &str| recorded.iter().any(|(n, a)| n == name && a.get(key).map(String::as_str) == Some(value));
        for outcome in ["pending", "coalesced", "another-birth"] {
            assert!(has("rdm.node_admin.connection.update.via-accept-trigger", "outcome", outcome), "via-accept-trigger outcome={outcome}");
        }
        assert!(has("rdm.node_admin.connection.remove.via-recovery-attempt", "reason", "dial-spent"), "the spent dial is named");
        assert!(has("rdm.node_admin.connection.remove.via-recovery-attempt", "reason", "direct-connected"), "the answered dial is named");
    }

}

