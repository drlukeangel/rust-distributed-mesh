//! The proof product's own Node RPC families, composed per role (i143.e11.s8). A family is the
//! product's: tagged in the product's own ledger rows, served only by the kinds that own it, and
//! unserved (`421`) everywhere else in the same sealed catalog.

use rafka_node_rpc::{HandlerFault, NodeRpcClient, PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::{LedgerEntry, OpOwner, OpState};
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_mesh_entity::NodeKind;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// `broker_data` (i142 U6): the product's one carried unary family, served by brokers only.
/// Forwardable: a gateway that cannot reach the broker directly carries it through a peer.
pub struct BrokerData;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerDataRequest {
    /// Append `value` under `key`; the broker answers the offset it took.
    Append { key: String, value: Vec<u8> },
    /// Read what `key` holds.
    Read { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerDataReply {
    Appended { key: String, offset: u64, served_by: String },
    Value { key: String, value: Option<Vec<u8>>, served_by: String },
    // The typed Node RPC refusals (node-rpc.md §34), one variant each.
    PeerUnresolved { reason: String },
    NotReady { reason: String },
    Busy { reason: String },
    Draining { reason: String },
    Malformed { kind: MalformedKind },
    Unauthorized { reason: String },
}

impl NodeProtocol for BrokerData {
    const OP: u8 = 0x20;
    const NAME: &'static str = "broker-data";
    const MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024;
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    const FORWARDABLE: bool = true;
    const REQUEST_VARIANTS: u32 = 2;
    const REPLY_VARIANTS: u32 = 8;
    type Request = BrokerDataRequest;
    type Reply = BrokerDataReply;

    fn classify_reply(reply: &Self::Reply) -> ReplyKind {
        match reply {
            BrokerDataReply::Appended { .. } | BrokerDataReply::Value { .. } => ReplyKind::Success,
            BrokerDataReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            BrokerDataReply::NotReady { .. } => ReplyKind::NotReady,
            BrokerDataReply::Busy { .. } => ReplyKind::Busy,
            BrokerDataReply::Draining { .. } => ReplyKind::Draining,
            BrokerDataReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            BrokerDataReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> Self::Reply {
        BrokerDataReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> Self::Reply {
        BrokerDataReply::NotReady { reason }
    }
    fn busy(reason: String) -> Self::Reply {
        BrokerDataReply::Busy { reason }
    }
    fn draining(reason: String) -> Self::Reply {
        BrokerDataReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> Self::Reply {
        BrokerDataReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> Self::Reply {
        BrokerDataReply::Unauthorized { reason }
    }
}

/// Appending under this key never replies: the reply-loss arm of a certainty cell.
pub const PROOF_HANG: &str = "proof:hang";
/// Appending under this key is a handler fault (423): the fault arm of a certainty cell.
pub const PROOF_FAULT: &str = "proof:fault";

/// The product's ledger rows: its own allocations, beside the core ledger.
pub fn product_ledger() -> Vec<LedgerEntry> {
    vec![LedgerEntry { op: BrokerData::OP, family: BrokerData::NAME.into(), owner: OpOwner::Product(crate::PRODUCT.into()), state: OpState::Live }]
}

/// The families `kind` serves, composed into `b`; `served_by` is this node's id, named in every reply. Every kind carries the forwardable families
/// for others (a gateway is the carrier of `broker_data`); only a broker serves it.
pub fn for_kind(kind: NodeKind, served_by: &str, b: ServerBuilder, _client: Arc<NodeRpcClient>) -> ServerBuilder {
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
                            tracing::info_span!("rafka.node_rpc.broker_data.serve.via-append", key = %key, offset, served_by = %me).in_scope(|| tracing::info!("appended"));
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
