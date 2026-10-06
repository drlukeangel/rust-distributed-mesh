//! The Node RPC client (node-rpc.md §20–§24; ownership amendment §6).
//!
//! One call is one attempt against one exact target. Pre-commit work —
//! resolve, slot/freshness check, dial, `open_bi`, writing the request —
//! spends the send budget and can only end `NotSent`. The commit cut is the
//! complete request written *and* the request direction finished cleanly; an
//! unfinished request is reset with `499 FRAME_NOT_SENT`. After the cut the
//! call ends `Reply`, `Unserved` (a `421`) or `Indeterminate`. Node RPC never
//! dials a second slot, peer or target inside one call and never replays.

use crate::pool::{DialError, DialSpec, Failpoint, Pool, PoolKey};
use crate::resolve::{NodeResolver, NodeTarget};
use iroh::endpoint::{ReadError, ReadToEndError, VarInt, WriteError};
use iroh::Endpoint;
use rafka_mesh_entity::{FreshnessToken, NodeId};
use rafka_node_rpc_contract::codes::ResetCode;
use rafka_node_rpc_contract::framing::{decode_single_frame, encode_request, MAX_VARINT_LEN};
use rafka_node_rpc_contract::outcome::{EarlyRefusal, IndeterminateReason, NotSentReason, PreCommit, RequestFinished, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

/// The two budget shapes (ownership amendment §6.1).
#[derive(Debug, Clone, Copy)]
pub enum Budget {
    /// One deadline over resolve, dial, send and reply; never reset between phases.
    Overall(Duration),
    /// A connect + complete-send bound, then a reply budget that starts at the commit cut.
    Split { send: Duration, reply: Duration },
}

#[derive(Debug, Clone)]
pub struct CallOptions {
    pub budget: Budget,
    /// Use this slot (default: the first slot the target advertises).
    pub slot: Option<String>,
    /// Dial exactly this slot under exactly this freshness token; a
    /// superseded token is refused before commit.
    pub pin: Option<(String, FreshnessToken)>,
    /// Failpoint: write part of the request, then reset it with 499.
    pub cut_before_finish: bool,
    /// The caller's execution scope: part of the pool identity, so two
    /// scopes never share a pooled connection.
    pub scope: Option<String>,
    /// Failpoint: a dial this call starts stops after connecting, before it
    /// is checked against the resolver and pooled.
    pub after_connect: Option<Arc<Failpoint>>,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            budget: Budget::Overall(Duration::from_secs(10)),
            slot: None,
            pin: None,
            cut_before_finish: false,
            scope: None,
            after_connect: None,
        }
    }
}

/// Which exact leg a call used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallEvidence {
    pub node_id: NodeId,
    pub slot: String,
    pub addr: SocketAddr,
    pub freshness: FreshnessToken,
    pub committed: bool,
    /// The connection the call rode (`stable_id`), once it had one.
    pub connection: Option<usize>,
    /// The connection came from the pool rather than a dial this call waited on.
    pub reused: bool,
}

pub struct NodeRpcClient {
    endpoint: Endpoint,
    resolver: Arc<dyn NodeResolver>,
    pool: Pool,
}

/// How the reply direction is decoded: after the commit cut, or after an
/// early refusal that stopped the request before it finished.
pub enum Decode<'a> {
    Committed(rafka_node_rpc_contract::outcome::Committed, &'a [u8]),
    Early(EarlyRefusal, Option<&'a [u8]>),
}

/// Where an invocation stands after the commit-cut phase.
pub(crate) enum Phase<R> {
    Done(RpcOutcome<R>, Option<CallEvidence>),
    Committed(Opened),
}

/// A committed invocation: the commit proof and the reply direction.
pub(crate) struct Opened {
    pub(crate) committed: rafka_node_rpc_contract::outcome::Committed,
    pub(crate) recv: iroh::endpoint::RecvStream,
    pub(crate) evidence: CallEvidence,
    pub(crate) key: PoolKey,
    pub(crate) conn: iroh::endpoint::Connection,
    pub(crate) reply_deadline: Instant,
}

/// What came back after the commit cut, before protocol decoding.
enum Committed {
    Reply(Vec<u8>),
    Reset(u64),
    Lost(IndeterminateReason),
}

impl NodeRpcClient {
    pub fn new(endpoint: Endpoint, resolver: Arc<dyn NodeResolver>) -> Self {
        Self { endpoint, resolver, pool: Pool::default() }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Every pooled connection's key.
    pub fn pooled(&self) -> Vec<PoolKey> {
        self.pool.keys()
    }

    /// Invoke protocol `P` on `target`.
    pub async fn call<P: NodeProtocol>(
        &self,
        target: &NodeTarget,
        req: &P::Request,
        opts: &CallOptions,
    ) -> (RpcOutcome<P::Reply>, Option<CallEvidence>) {
        let pre = PreCommit::begin(P::TAG);
        let payload = match P::encode_request(req) {
            Ok(p) => p,
            Err(e) => return (pre.not_sent(NotSentReason::Connection(format!("request does not encode: {}", e.0))), None),
        };
        self.invoke_raw::<P::Reply, _>(target, P::TAG, payload, P::MAX_REPLY_FRAME_BYTES, opts, |d| match d {
            Decode::Committed(c, bytes) => c.reply::<P>(bytes),
            Decode::Early(e, bytes) => e.reply::<P>(bytes),
        })
        .await
    }

    /// Everything up to the commit cut: resolve, slot/freshness, dial, open_bi, the request
    /// write and the clean finish. Ends `Done` with a pre-commit or early-refusal outcome, or
    /// `Committed` holding the reply direction.
    pub(crate) async fn open<R, E>(
        &self,
        target: &NodeTarget,
        tag: u8,
        payload: Vec<u8>,
        max_reply: usize,
        opts: &CallOptions,
        early_decode: E,
    ) -> Phase<R>
    where
        E: FnOnce(EarlyRefusal, Option<&[u8]>) -> RpcOutcome<R>,
    {
        let start = Instant::now();
        let (send_deadline, overall) = match opts.budget {
            Budget::Overall(d) => (start + d, Some(start + d)),
            Budget::Split { send, .. } => (start + send, None),
        };
        let pre = PreCommit::begin(tag);
        let node = match self.resolver.resolve(target) {
            Ok(n) => n,
            Err(f) => return Phase::Done(pre.not_sent(NotSentReason::Resolve(f)), None),
        };
        // Whatever this node's record no longer names leaves the pool now.
        self.pool.purge_stale(&node);
        // The exact slot target and its freshness.
        let want = opts.pin.as_ref().map(|(s, _)| s.clone()).or_else(|| opts.slot.clone());
        let slot = match want {
            Some(s) => node.endpoints.iter().find(|e| e.slot == s).cloned(),
            None => node.endpoints.first().cloned(),
        };
        let Some(slot) = slot else {
            let s = opts.slot.clone().or(opts.pin.as_ref().map(|p| p.0.clone())).unwrap_or_default();
            return Phase::Done(pre.not_sent(NotSentReason::Superseded { slot: s }), None);
        };
        if let Some((_, token)) = &opts.pin {
            if *token != slot.freshness {
                tracing::info_span!("rafka.node_rpc.connection.reject.via-stale-slot", slot = %slot.slot, node = %node.node_id)
                    .in_scope(|| tracing::info!("pinned freshness token is superseded; refused before commit"));
                return Phase::Done(pre.not_sent(NotSentReason::Superseded { slot: slot.slot.clone() }), None);
            }
        }
        let mut evidence = CallEvidence {
            node_id: node.node_id.clone(),
            slot: slot.slot.clone(),
            addr: slot.addr,
            freshness: slot.freshness.clone(),
            committed: false,
            connection: None,
            reused: false,
        };
        let key = PoolKey {
            scope: opts.scope.clone(),
            peer: node.transport_id,
            incarnation: node.incarnation.clone(),
            slot: slot.slot.clone(),
            freshness: slot.freshness.clone(),
        };
        let spec = DialSpec {
            endpoint: self.endpoint.clone(),
            resolver: self.resolver.clone(),
            target: target.clone(),
            addr: slot.addr,
            deadline: send_deadline,
            failpoint: opts.after_connect.clone(),
        };
        let conn = match self.pool.get_or_dial(&key, spec).await {
            Ok((c, reused)) => {
                evidence.connection = Some(c.stable_id());
                evidence.reused = reused;
                c
            }
            Err(DialError::Superseded) => return Phase::Done(pre.not_sent(NotSentReason::Superseded { slot: slot.slot.clone() }), Some(evidence)),
            Err(DialError::Deadline) => return Phase::Done(pre.not_sent(NotSentReason::Deadline), Some(evidence)),
            Err(DialError::Failed(e)) => return Phase::Done(pre.not_sent(NotSentReason::Connection(e)), Some(evidence)),
        };
        let (mut send, mut recv) = match timeout_at(send_deadline, conn.open_bi()).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                self.pool.broken(&key, &conn);
                return Phase::Done(pre.not_sent(NotSentReason::Connection(e.to_string())), Some(evidence));
            }
            Err(_) => return Phase::Done(pre.not_sent(NotSentReason::Deadline), Some(evidence)),
        };
        let frame = encode_request(tag, &payload);
        let frame_not_sent = VarInt::from_u32(ResetCode::FrameNotSent.code());
        if opts.cut_before_finish {
            let half = (frame.len() / 2).max(1);
            let _ = send.write_all(&frame[..half]).await;
            let _ = send.reset(frame_not_sent);
            let (_, out) = pre.cut_before_finish();
            return Phase::Done(out, Some(evidence));
        }
        let written = timeout_at(send_deadline, send.write_all(&frame)).await;
        if let Ok(Err(WriteError::Stopped(stop))) = &written {
            // The server refused before this request finished (node-rpc.md §27):
            // a healthy answer on a healthy connection.
            self.pool.healthy(&key);
            if stop.into_inner() == u64::from(ResetCode::UnservedTag.code()) {
                return Phase::Done(pre.unserved_before_finish(), Some(evidence));
            }
            let bytes = timeout_at(send_deadline, recv.read_to_end(max_reply + MAX_VARINT_LEN)).await;
            let early = pre.stopped_before_finish();
            let out = match bytes {
                Ok(Ok(b)) => match decode_single_frame(&b, max_reply) {
                    Ok(payload) => early_decode(early, Some(payload)),
                    Err(_) => early_decode(early, None),
                },
                _ => early_decode(early, None),
            };
            return Phase::Done(out, Some(evidence));
        }
        if !matches!(written, Ok(Ok(()))) {
            if let Ok(Err(WriteError::ConnectionLost(_))) = &written {
                self.pool.broken(&key, &conn);
            }
            let _ = send.reset(frame_not_sent);
            let (_, out) = pre.cut_before_finish();
            return Phase::Done(out, Some(evidence));
        }
        let finished = send.finish().is_ok();
        let Some(proof) = RequestFinished::after_clean_finish(frame.len(), frame.len(), finished) else {
            let _ = send.reset(frame_not_sent);
            let (_, out) = pre.cut_before_finish();
            return Phase::Done(out, Some(evidence));
        };
        let committed = pre.commit(proof);
        evidence.committed = true;
        let reply_deadline = match (opts.budget, overall) {
            (_, Some(d)) => d,
            (Budget::Split { reply, .. }, None) => Instant::now() + reply,
            (Budget::Overall(_), None) => unreachable!(),
        };
        Phase::Committed(Opened { committed, recv, evidence, key, conn, reply_deadline })
    }

    /// Invoke a raw tag (any tag, served or not) — the unknown-tag cell uses it.
    pub async fn invoke_raw<R, D>(
        &self,
        target: &NodeTarget,
        tag: u8,
        payload: Vec<u8>,
        max_reply: usize,
        opts: &CallOptions,
        decode: D,
    ) -> (RpcOutcome<R>, Option<CallEvidence>)
    where
        D: FnOnce(Decode<'_>) -> RpcOutcome<R>,
    {
        let decode = std::sync::Mutex::new(Some(decode));
        let take = || decode.lock().unwrap().take().expect("decode runs once");
        let Opened { committed, mut recv, evidence, key, conn, reply_deadline } =
            match self.open(target, tag, payload, max_reply, opts, |early, bytes| take()(Decode::Early(early, bytes))).await {
                Phase::Done(out, evidence) => return (out, evidence),
                Phase::Committed(o) => o,
            };
        let decode = take();
        let got = match timeout_at(reply_deadline, recv.read_to_end(max_reply + MAX_VARINT_LEN)).await {
            Err(_) => Committed::Lost(IndeterminateReason::ReplyDeadline),
            Ok(Ok(bytes)) => Committed::Reply(bytes),
            Ok(Err(ReadToEndError::Read(ReadError::Reset(c)))) => Committed::Reset(c.into_inner()),
            Ok(Err(ReadToEndError::TooLong)) => Committed::Lost(IndeterminateReason::ProtocolViolation("reply longer than the protocol ceiling".into())),
            Ok(Err(e)) => Committed::Lost(IndeterminateReason::ReplyLost(e.to_string())),
        };
        // Pool health from what came back (never reachability).
        match &got {
            Committed::Reply(_) | Committed::Reset(_) => self.pool.healthy(&key),
            Committed::Lost(IndeterminateReason::ReplyDeadline) => self.pool.timed_out(&key, &conn),
            Committed::Lost(IndeterminateReason::ReplyLost(_)) => self.pool.broken(&key, &conn),
            Committed::Lost(_) => {}
        }
        let out = match got {
            Committed::Reply(bytes) => match decode_single_frame(&bytes, max_reply) {
                Ok(payload) => decode(Decode::Committed(committed, payload)),
                Err(e) => committed.indeterminate(IndeterminateReason::ProtocolViolation(format!("reply frame: {e:?}"))),
            },
            Committed::Reset(c) => committed.reset(c),
            Committed::Lost(r) => committed.indeterminate(r),
        };
        (out, Some(evidence))
    }
}
