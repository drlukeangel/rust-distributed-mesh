//! The fabric control projection of Build state (PRD §1.4, §7;
//! mesh-control-plane.md §3–§4.1).
//!
//! Every node-admin of a fabric holds the same Build facts: each fact an admin
//! appends (intent, attempt claim, step receipt, attempt receipt, forget) is
//! applied to its own [`MemoryBuildStateAdapter`] and broadcast once on the
//! fabric's Build topic over iroh-gossip; every other admin absorbs it. A
//! successor therefore continues a Build from its own live projection, never
//! from the executing admin's disk.
//!
//! An admin that gains a neighbour on the topic (one joining, or one
//! reconnecting after a partition) sends it the facts of every active Build
//! it holds, to its direct neighbours only (`broadcast_neighbors`): a fact
//! broadcast before a member was connected is otherwise never delivered to
//! it, and a successor that misses a Build cannot continue it.
//!
//! Facts fold deterministically and absorb idempotently: intents and claims
//! are insert-and-fail, receipts deduplicate. Split-primary safety comes from
//! reconciliation over pinned intents and idempotent operation keys, not from
//! the claim alone (mesh-control-plane.md §4.1).

use crate::build::BuildId;
use crate::build_state::{
    BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildIntentFact, BuildProjection, BuildStateAdapter, BuildStateError,
    BuildStepReceipt, ClaimOutcome, MemoryBuildStateAdapter,
};
use futures_lite::StreamExt as _;
use iroh::address_lookup::MemoryLookup;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use std::sync::Arc;

/// One message on the Build topic. The nonce makes every send distinct, so
/// a catch-up resend is never taken for a message already seen.
#[derive(serde::Serialize, serde::Deserialize)]
struct Wire {
    nonce: u64,
    facts: Vec<BuildFact>,
}

/// iroh-gossip's message limit (`DEFAULT_MAX_MESSAGE_SIZE`): a larger frame
/// is refused by the receiving connection, which drops it, and with it every
/// topic sharing that connection.
pub const MAX_MESSAGE_BYTES: usize = 4096;

fn encode(facts: Vec<BuildFact>) -> Result<bytes::Bytes, BuildStateError> {
    let bytes = serde_json::to_vec(&Wire { nonce: rand::random(), facts }).map_err(|e| BuildStateError::Io(e.to_string()))?;
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(BuildStateError::Io(format!(
            "a Build message of {} bytes exceeds the gossip limit of {MAX_MESSAGE_BYTES}",
            bytes.len()
        )));
    }
    Ok(bytes.into())
}

/// `facts` packed, in order, into messages that each fit the gossip limit.
/// A fact that fits no message on its own is left out and named.
pub fn encode_chunks(facts: Vec<BuildFact>) -> (Vec<bytes::Bytes>, Vec<BuildStateError>) {
    let (mut out, mut refused, mut batch) = (Vec::new(), Vec::new(), Vec::new());
    for f in facts {
        batch.push(f);
        if encode(batch.clone()).is_ok() {
            continue;
        }
        let last = batch.pop().expect("just pushed");
        if !batch.is_empty() {
            out.push(encode(std::mem::take(&mut batch)).expect("fitted before the last fact"));
        }
        match encode(vec![last.clone()]) {
            Ok(_) => batch.push(last),
            Err(e) => refused.push(e),
        }
    }
    if !batch.is_empty() {
        out.push(encode(batch).expect("fitted"));
    }
    (out, refused)
}

/// The facts of every active Build `local` holds, in append order.
async fn active_facts(local: &MemoryBuildStateAdapter) -> Vec<BuildFact> {
    let active: std::collections::BTreeSet<BuildId> = match local.list_active().await {
        Ok(a) => a.into_iter().map(|b| b.build_id).collect(),
        Err(_) => return Vec::new(),
    };
    local.facts().await.unwrap_or_default().into_iter().filter(|f| active.contains(f.build_id())).collect()
}

/// While the Build topic has had no neighbour for a whole window, hand it
/// every peer it has known again, once per window (`gossip.md` §6): an admin
/// whose neighbours all dropped is otherwise never dialed again, and a Build
/// it accepted in the meantime never reaches the fabric. Nothing is sent.
fn refeed(
    fabric: String,
    sender: Arc<tokio::sync::RwLock<GossipSender>>,
    known: Arc<std::sync::Mutex<Vec<iroh::EndpointId>>>,
    neighbors: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>>,
) {
    use rafka_mesh_transport::membership::{PUBLISH_EVERY, SILENT_AFTER};
    tokio::spawn(async move {
        let mut alone_since: Option<std::time::Instant> = None;
        loop {
            tokio::time::sleep(PUBLISH_EVERY).await;
            if !neighbors.lock().unwrap().is_empty() {
                alone_since = None;
                continue;
            }
            let since = *alone_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() < SILENT_AFTER {
                continue;
            }
            let mut peers = known.lock().unwrap().clone();
            peers.sort();
            peers.dedup();
            if peers.is_empty() {
                continue;
            }
            let s = sender.read().await.clone();
            let joined = s.join_peers(peers.clone()).await.is_ok();
            tracing::info_span!("rafka.mesh.connection.update.via-refeed", channel = "builds", fabric = %fabric, peers = peers.len(), joined)
                .in_scope(|| tracing::info!("no neighbour for a window: every known peer handed to the Build topic again"));
            alone_since = Some(std::time::Instant::now());
        }
    });
}

/// The fabric's Build topic.
pub fn build_topic(fabric: &str) -> TopicId {
    TopicId::from_bytes(*blake3::hash(format!("rafka-fabric-builds:{fabric}").as_bytes()).as_bytes())
}

/// A node-admin's Build state: its own copy of the fabric's Build facts.
pub struct FabricBuildStateAdapter {
    local: Arc<MemoryBuildStateAdapter>,
    sender: Arc<tokio::sync::RwLock<GossipSender>>,
}

impl FabricBuildStateAdapter {
    /// Join `fabric`'s Build topic through `peers` (the other node-admins).
    /// With peers, it returns once connected to at least one of them, so the
    /// facts they append from then on reach this admin.
    pub async fn join(gossip: &Gossip, endpoint: &Endpoint, fabric: &str, peers: Vec<EndpointAddr>) -> Result<Self, BuildStateError> {
        let io = |e: String| BuildStateError::Io(format!("fabric Build topic {fabric}: {e}"));
        let lookup = MemoryLookup::new();
        for p in &peers {
            lookup.add_endpoint_info(p.clone());
        }
        endpoint.address_lookup().map_err(|e| io(e.to_string()))?.add(lookup);
        let ids = peers.iter().map(|p| p.id).collect();
        let topic = if peers.is_empty() {
            gossip.subscribe(build_topic(fabric), ids).await
        } else {
            gossip.subscribe_and_join(build_topic(fabric), ids).await
        }
        .map_err(|e| io(e.to_string()))?;
        let (sender, mut receiver) = topic.split();
        let sender = Arc::new(tokio::sync::RwLock::new(sender));
        let local = Arc::new(MemoryBuildStateAdapter::new());
        let (absorb, shared) = (local.clone(), sender.clone());
        let (fabric_name, gossip, topic_id) = (fabric.to_string(), gossip.clone(), build_topic(fabric));
        let seed_ids: Vec<iroh::EndpointId> = peers.iter().map(|p| p.id).collect();
        let known_peers: Arc<std::sync::Mutex<Vec<iroh::EndpointId>>> = Arc::new(std::sync::Mutex::new(seed_ids.clone()));
        let neighbors: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>> = Arc::default();
        refeed(fabric.to_string(), shared.clone(), known_peers.clone(), neighbors.clone());
        tokio::spawn(async move {
            loop {
                // Receive until the subscription lags or ends; iroh-gossip
                // closes a lagging subscriber and expects it to be re-opened.
                let reason = loop {
                    match receiver.next().await {
                        Some(Ok(Event::Received(m))) => match serde_json::from_slice::<Wire>(&m.content) {
                            Ok(w) => absorb.absorb(&w.facts),
                            Err(e) => tracing::info_span!("rafka.node_admin.build.reject.via-undecodable-fact", fabric = %fabric_name, error = %e)
                                .in_scope(|| tracing::info!("a Build fact from the fabric does not decode")),
                        },
                        Some(Ok(Event::NeighborUp(peer))) => {
                            known_peers.lock().unwrap().push(peer);
                            neighbors.lock().unwrap().insert(peer);
                            // Sent from its own task: the receive loop never waits on the actor.
                            let (absorb, shared, fabric_name) = (absorb.clone(), shared.clone(), fabric_name.clone());
                            tokio::spawn(async move {
                                let facts = active_facts(&absorb).await;
                                let _span = tracing::info_span!(
                                    "rafka.node_admin.build.update.via-neighbor-up",
                                    fabric = %fabric_name,
                                    peer = %peer,
                                    facts = facts.len(),
                                );
                                let (messages, refused) = encode_chunks(facts);
                                for e in refused {
                                    tracing::info_span!("rafka.node_admin.build.reject.via-oversized-fact", fabric = %fabric_name, detail = %e)
                                        .in_scope(|| tracing::info!("a Build fact does not fit one gossip message"));
                                }
                                let sender = shared.read().await.clone();
                                for bytes in messages {
                                    let _ = sender.broadcast_neighbors(bytes).await;
                                }
                            });
                        }
                        Some(Ok(Event::NeighborDown(peer))) => {
                            neighbors.lock().unwrap().remove(&peer);
                        }
                        Some(Ok(Event::Lagged)) => break "lagged: events were dropped".to_string(),
                        Some(Err(e)) => break format!("subscription error: {e}"),
                        None => break "subscription ended".to_string(),
                    }
                };
                neighbors.lock().unwrap().clear();
                let known = {
                    let mut k = known_peers.lock().unwrap();
                    k.sort();
                    k.dedup();
                    k.clone()
                };
                let span = tracing::info_span!("rafka.node_admin.build.update.via-resubscribe", fabric = %fabric_name, reason = %reason, peers = known.len());
                // A refused subscribe means the gossip actor itself has stopped.
                let reopened = match gossip.subscribe(topic_id, known.clone()).await {
                    Ok(t) => t,
                    Err(e) => {
                        span.in_scope(|| tracing::info!(error = %e, "gossip has stopped; the Build topic ends"));
                        return;
                    }
                };
                span.in_scope(|| tracing::info!("Build topic subscription re-opened"));
                let (s, r) = reopened.split();
                *shared.write().await = s;
                receiver = r;
            }
        });
        Ok(Self { local, sender })
    }

    async fn broadcast(&self, fact: BuildFact) -> Result<(), BuildStateError> {
        let sender = self.sender.read().await.clone();
        sender.broadcast(encode(vec![fact])?).await.map_err(|e| BuildStateError::Io(format!("broadcasting Build fact: {e}")))
    }
}

#[async_trait::async_trait]
impl BuildStateAdapter for FabricBuildStateAdapter {
    async fn publish_intent(&self, intent: &BuildIntentFact) -> Result<(), BuildStateError> {
        self.local.publish_intent(intent).await?;
        self.broadcast(BuildFact::Intent(intent.clone())).await
    }

    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.local.read_build(build_id).await
    }

    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.local.list_active().await
    }

    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        let outcome = self.local.claim_attempt(claim).await?;
        if outcome == ClaimOutcome::Won {
            self.broadcast(BuildFact::Claim(claim.clone())).await?;
        }
        Ok(outcome)
    }

    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.local.append_step_receipt(receipt).await?;
        self.broadcast(BuildFact::Step(receipt.clone())).await
    }

    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.local.append_attempt_receipt(receipt).await?;
        self.broadcast(BuildFact::Attempt(receipt.clone())).await
    }

    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.local.facts().await
    }

    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.local.forget(build_id).await?;
        self.broadcast(BuildFact::Forget { build_id: build_id.clone() }).await
    }
}
