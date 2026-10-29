//! The proof product's own Node RPC families, composed per role (i143.e11.s8). A family is the
//! product's: tagged in the product's own ledger rows, served only by the kinds that own it, and
//! unserved (`421`) everywhere else in the same built catalog.

use rdm_transport::{HandlerFault, NodeRpcClient, PeerContext, ServerBuilder};
use rdm_core::catalog::{LedgerEntry, OpOwner, OpState};
use rdm_core::outcome::{MalformedKind, ReplyKind};
use rdm_core::protocol::NodeProtocol;
use rdm_core::NodeKind;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// `broker_data` (i142 U6): the product's one carried unary family, served by brokers only.
/// Forwardable: a gateway that cannot reach the broker directly carries it through a peer.
pub struct BrokerData;

/// The requests of the `broker_data` family.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerDataRequest {
    /// Append `value` under `key`; the broker answers the offset it took.
    ///
    /// Position 0 on the wire.
    Append {
        /// The key to append under.
        key: String,
        /// The bytes to append.
        value: Vec<u8>,
    },
    /// Read what `key` holds.
    ///
    /// Position 1 on the wire.
    Read {
        /// The key to read.
        key: String,
    },
}

/// The replies of the `broker_data` family: the two successes and the typed Node RPC refusals.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerDataReply {
    /// The append succeeded.
    ///
    /// Position 0 on the wire.
    Appended {
        /// The key appended under.
        key: String,
        /// The offset the append took.
        offset: u64,
        /// The node that served the append.
        served_by: String,
    },
    /// The read's answer.
    ///
    /// Position 1 on the wire.
    Value {
        /// The key read.
        key: String,
        /// What the key holds, `None` when nothing was appended under it.
        value: Option<Vec<u8>>,
        /// The node that served the read.
        served_by: String,
    },
    // The typed Node RPC refusals (node-rpc.md §34), one variant each.
    /// The peer the call needed could not be resolved.
    ///
    /// Position 2 on the wire.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The node is not ready to serve.
    ///
    /// Position 3 on the wire.
    NotReady {
        /// Why the node is not ready.
        reason: String,
    },
    /// The node is at its admission bound.
    ///
    /// Position 4 on the wire.
    Busy {
        /// Which bound the node is at.
        reason: String,
    },
    /// The node is connections-deleting and takes no new work.
    ///
    /// Position 5 on the wire.
    ConnectionsDeleting {
        /// Why the node refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 6 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    ///
    /// Position 7 on the wire.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
}

impl NodeProtocol for BrokerData {
    const OP: u8 = 0x40;
    const NAME: &'static str = "broker-data";
    const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    const FORWARDABLE: bool = true;
    type Request = BrokerDataRequest;
    type Reply = BrokerDataReply;

    rdm_core::protocol_variants! {
        request BrokerDataRequest { Append, Read }
        reply BrokerDataReply {
            Appended = "appended" => ReplyKind::Success,
            Value = "value" => ReplyKind::Success,
            @reserved,
        }
    }
}

/// Appending under this key never replies: the reply-loss arm of a certainty cell.
pub const PROOF_HANG: &str = "proof:hang";
/// Appending under this key is a handler fault (423): the fault arm of a certainty cell.
pub const PROOF_FAULT: &str = "proof:fault";

/// The product's ledger rows: its own allocations, beside the core ledger.
pub(crate) fn product_ledger() -> Vec<LedgerEntry> {
    vec![LedgerEntry { op: BrokerData::OP, family: BrokerData::NAME.into(), owner: OpOwner::Product(crate::PRODUCT.into()), state: OpState::Live }]
}

/// The families `kind` serves, composed into `b`; `served_by` is this node's id, named in every reply. Every kind carries the forwardable families
/// for others (a gateway is the carrier of `broker_data`); only a broker serves it.
pub(crate) fn for_kind(kind: NodeKind, served_by: &str, b: ServerBuilder, _client: Arc<NodeRpcClient>) -> ServerBuilder {
    let b = b.ledger(product_ledger()).carry::<BrokerData>();
    match kind {
        NodeKind::Broker => {
            let store: Arc<std::sync::Mutex<std::collections::BTreeMap<String, Vec<Vec<u8>>>>> = Default::default();
            let me = served_by.to_string();
            b.serve::<BrokerData, _, _>(OpOwner::Product(crate::PRODUCT.into()), move |_peer: PeerContext, req: BrokerDataRequest| {
                let (store, me) = (store.clone(), me.clone());
                async move {
                    let mut s = store.lock().map_err(|e| HandlerFault::invariant_broken(format!("store: {e}")))?;
                    Ok(match req {
                        // The proof family's staging keys (i143.e11.s6): a certainty cell asks the
                        // broker to hang or to fault by the key it appends under.
                        BrokerDataRequest::Append { key, .. } if key == PROOF_HANG => {
                            drop(s);
                            std::future::pending().await
                        }
                        BrokerDataRequest::Append { key, .. } if key == PROOF_FAULT => {
                            return Err(HandlerFault::invariant_broken("the proof fault key"));
                        }
                        BrokerDataRequest::Append { key, value } => {
                            let log = s.entry(key.clone()).or_default();
                            log.push(value);
                            let offset = (log.len() - 1) as u64;
                            tracing::info_span!("rdm.node_rpc.broker_data.serve.via-append", key = %key, offset, served_by = %me).in_scope(|| tracing::info!("appended"));
                            BrokerDataReply::Appended { key, offset, served_by: me }
                        }
                        BrokerDataRequest::Read { key } => {
                            let value = s.get(&key).and_then(|l| l.last().cloned());
                            BrokerDataReply::Value { key, value, served_by: me }
                        }
                    })
                }
            })
        }
        _ => b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::outcome::Code;
    use rdm_core::protocol::DecodeFailure;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    /// Every variant's postcard bytes, decoded back, and its classification and name, against the
    /// pins taken from the hand-written declaration: one listing of every mismatch.
    fn pinned<P: NodeProtocol>(requests: &[(P::Request, &str)], replies: &[(P::Reply, &str, &str, Code)]) -> Vec<String>
    where
        P::Request: PartialEq + std::fmt::Debug,
        P::Reply: PartialEq + std::fmt::Debug,
    {
        let mut off = Vec::new();
        for (i, (r, want)) in requests.iter().enumerate() {
            let got = hex(&P::encode_request(r).expect("a request encodes"));
            if got != *want {
                off.push(format!("request {i}: {got} (pinned {want})"));
            }
            assert_eq!(P::decode_request(&P::encode_request(r).unwrap()).as_ref(), Ok(r), "request {i} decodes to itself");
        }
        for (i, (r, want, detail, code)) in replies.iter().enumerate() {
            let got = hex(&P::encode_reply(r).expect("a reply encodes"));
            if got != *want {
                off.push(format!("reply {i}: {got} (pinned {want})"));
            }
            assert_eq!(P::decode_reply(&P::encode_reply(r).unwrap()).as_ref(), Ok(r), "reply {i} decodes to itself");
            assert_eq!((P::reply_detail(r), P::classify_reply(r).code()), (*detail, *code), "reply {i}: its name and code");
        }
        assert_eq!((P::REQUEST_VARIANTS as usize, P::REPLY_VARIANTS as usize), (requests.len(), replies.len()), "one pin per variant");
        assert_eq!(P::decode_request(&[P::REQUEST_VARIANTS as u8]).err(), Some(DecodeFailure::UnknownVariant), "the request count is the first unknown position");
        assert_eq!(P::decode_reply(&[P::REPLY_VARIANTS as u8]).err(), Some(DecodeFailure::UnknownVariant), "the reply count is the first unknown position");
        off
    }

    /// CONTRACT: `BrokerData`'s wire is the one its hand-written declaration had: each request and
    /// reply variant encodes to the pinned bytes and decodes back, each reply keeps its name and code,
    /// the variant counts are 2 and 8, and the six reserved constructors build their own variants.
    #[test]
    fn broker_data_keeps_every_variant_on_its_pinned_bytes() {
        let s = || "r".to_string();
        let requests = [(BrokerDataRequest::Append { key: "k".into(), value: vec![7] }, "00016b0107"), (BrokerDataRequest::Read { key: "k".into() }, "01016b")];
        let replies = [
            (BrokerDataReply::Appended { key: "k".into(), offset: 3, served_by: "n".into() }, "00016b03016e", "appended", Code::Ok),
            (BrokerDataReply::Value { key: "k".into(), value: Some(vec![7]), served_by: "n".into() }, "01016b010107016e", "value", Code::Ok),
            (BrokerDataReply::PeerUnresolved { reason: s() }, "020172", "peer-unresolved", Code::NotFound),
            (BrokerDataReply::NotReady { reason: s() }, "030172", "not-ready", Code::Unavailable),
            (BrokerDataReply::Busy { reason: s() }, "040172", "busy", Code::Unavailable),
            (BrokerDataReply::ConnectionsDeleting { reason: s() }, "050172", "connections-deleting", Code::Unavailable),
            (BrokerDataReply::Malformed { kind: MalformedKind::TooLarge }, "0600", "malformed", Code::ResourceExhausted),
            (BrokerDataReply::Unauthorized { reason: s() }, "070172", "unauthorized", Code::PermissionDenied),
        ];
        let off = pinned::<BrokerData>(&requests, &replies);
        assert!(off.is_empty(), "the wire moved:\n{}", off.join("\n"));
        assert_eq!(
            [BrokerData::peer_unresolved(s()), BrokerData::not_ready(s()), BrokerData::busy(s()), BrokerData::connections_deleting(s()), BrokerData::malformed(MalformedKind::TooLarge), BrokerData::unauthorized(s())],
            [replies[2].0.clone(), replies[3].0.clone(), replies[4].0.clone(), replies[5].0.clone(), replies[6].0.clone(), replies[7].0.clone()],
            "each reserved constructor builds its own variant"
        );
    }

    /// CONTRACT: `BrokerData`, served as a broker serves it (`for_kind`), passes every unary check of
    /// the conformance harness: each named refusal answered, the request over its ceiling refused
    /// `Malformed(TooLarge)`, a duplicate delivery suppressed, a reply lost after acceptance delivered
    /// once. Its evidence: one trace per check, under `rdm.test.conformance.serve.via-check`.
    #[test]
    fn broker_data_passes_the_conformance_harness() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/rdm-test");
        std::fs::create_dir_all(&dir).expect("the evidence directory can be created");
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-test-conformance-broker-data").expect("the cell's evidence file can be created");
        let report = capture.run(4, async {
            let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
            let client = Arc::new(NodeRpcClient::new(ep, Arc::new(rdm_transport::StaticResolver::new())));
            let fixture = crate::conformance::Unary::<BrokerData> {
                serve: Arc::new(move |b| for_kind(NodeKind::Broker, "conformance-broker", b, client.clone())),
                answered: BrokerDataRequest::Append { key: "k".into(), value: vec![7] },
                refusals: Vec::new(),
            };
            crate::conformance::unary(&fixture).await
        });
        println!("{}", report.summary());
        assert!(report.failed().is_empty(), "{}", report.summary());
    }
}
