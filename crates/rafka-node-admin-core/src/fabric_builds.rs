//! The fabric control projection of Build state (PRD §1.4, §7;
//! mesh-control-plane.md §3–§4.1).
//!
//! Every node-admin of a fabric holds the same Build facts: each fact an admin
//! appends (intent, attempt claim, step receipt, attempt receipt, forget) is
//! applied to its own local log (`builds.storage`) and broadcast once on the
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
use crate::accepted::AcceptedStore;
use crate::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter,
    BuildStateError, BuildStepReceipt, ClaimOutcome, LocalBuildLog,
};
use crate::fabric_storage::FabricRecord;
use futures_lite::StreamExt as _;
use iroh::address_lookup::MemoryLookup;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use std::sync::Arc;

/// One message on the Build topic. The nonce makes every send distinct, so
/// a catch-up resend is never taken for a message already seen.
///
/// `fabric` is the Fabric record (`Fabric.build_id`): Fabric control state of
/// its own, sent alone when the pointer moves and to a new neighbour, never
/// inside a run of Build facts. A neighbour's catch-up hands it the record and
/// the facts of the Build it names and of every active Build.
#[derive(serde::Serialize, serde::Deserialize)]
struct Wire {
    nonce: u64,
    #[serde(default)]
    facts: Vec<BuildFact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fabric: Option<FabricRecord>,
    /// A fabric shutdown in force: Fabric control state, sent alone when initiated and to a new
    /// neighbour first (fabric-mesh-lifecycle.md §11.1). Never a Build fact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shutdown: Option<crate::fabric_storage::FabricShutdown>,
}

fn encode_shutdown(s: &crate::fabric_storage::FabricShutdown) -> Result<bytes::Bytes, BuildStateError> {
    serde_json::to_vec(&Wire { nonce: rand::random(), facts: Vec::new(), fabric: None, shutdown: Some(s.clone()) })
        .map(Into::into)
        .map_err(|e| BuildStateError::Io(e.to_string()))
}

fn encode_fabric(r: &FabricRecord) -> Result<bytes::Bytes, BuildStateError> {
    serde_json::to_vec(&Wire { nonce: rand::random(), facts: Vec::new(), fabric: Some(r.clone()), shutdown: None })
        .map(Into::into)
        .map_err(|e| BuildStateError::Io(e.to_string()))
}

/// iroh-gossip's frame limit (`DEFAULT_MAX_MESSAGE_SIZE`): a frame of this
/// many bytes or more is refused at write, which closes the connection, and
/// with it every topic sharing that connection.
pub const GOSSIP_FRAME_LIMIT: usize = 4096;

/// The largest Build message payload. The frame is the payload plus the
/// message envelope (two enum tags, the 32-byte message id, the payload's
/// length prefix, the delivery scope and round: about 40 bytes); 64 bytes
/// are kept for it.
pub const MAX_MESSAGE_BYTES: usize = GOSSIP_FRAME_LIMIT - 64;

fn encode(facts: Vec<BuildFact>) -> Result<bytes::Bytes, BuildStateError> {
    let bytes = serde_json::to_vec(&Wire { nonce: rand::random(), facts, fabric: None, shutdown: None }).map_err(|e| BuildStateError::Io(e.to_string()))?;
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(BuildStateError::Io(format!(
            "a Build message of {} bytes exceeds the gossip payload limit of {MAX_MESSAGE_BYTES} (frame limit {GOSSIP_FRAME_LIMIT})",
            bytes.len()
        )));
    }
    Ok(bytes.into())
}

/// `facts` packed, in order, into messages that each fit the gossip limit.
/// A fact that fits no message on its own is left out and named.
pub fn encode_chunks(facts: Vec<BuildFact>) -> (Vec<bytes::Bytes>, Vec<BuildStateError>) {
    // Each encoding draws a fresh nonce, whose length varies: a batch is sent
    // as the bytes that were measured to fit, never encoded a second time.
    let (mut out, mut refused, mut batch) = (Vec::new(), Vec::new(), Vec::new());
    let mut fitted: Option<bytes::Bytes> = None;
    for f in facts {
        batch.push(f);
        if let Ok(b) = encode(batch.clone()) {
            fitted = Some(b);
            continue;
        }
        let last = batch.pop().expect("just pushed");
        batch.clear();
        out.extend(fitted.take());
        match encode(vec![last.clone()]) {
            Ok(b) => {
                batch.push(last);
                fitted = Some(b);
            }
            Err(e) => refused.push(e),
        }
    }
    out.extend(fitted);
    (out, refused)
}

/// The facts of every active Build `local` holds, and of `also` (the Build the pointer names),
/// in append order.
async fn active_facts(local: &dyn LocalBuildLog, also: Option<BuildId>) -> Vec<BuildFact> {
    let mut active: std::collections::BTreeSet<BuildId> = match local.list_active().await {
        Ok(a) => a.into_iter().map(|b| b.build_id).collect(),
        Err(_) => return Vec::new(),
    };
    active.extend(also);
    match local.facts().await {
        Ok(f) => f.into_iter().filter(|f| active.contains(f.build_id())).collect(),
        Err(_) => Vec::new(),
    }
}

fn refeed(
    fabric: String,
    sender: Arc<tokio::sync::RwLock<GossipSender>>,
    known: Arc<std::sync::Mutex<Vec<iroh::EndpointId>>>,
    neighbors: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>>,
) {
    use rafka_mesh_transport::membership::{backbone_gossip_interval, staleness_floor};
    tokio::spawn(async move {
        let mut alone_since: Option<std::time::Instant> = None;
        loop {
            tokio::time::sleep(backbone_gossip_interval()).await;
            if !neighbors.lock().unwrap().is_empty() {
                alone_since = None;
                continue;
            }
            let since = *alone_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() < staleness_floor() {
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
pub fn build_topic(fabric: &rafka_mesh_entity::FabricId) -> TopicId {
    TopicId::from_bytes(*blake3::hash(format!("rafka-fabric-builds:{fabric}").as_bytes()).as_bytes())
}

/// A node-admin's Build state: its own copy of the fabric's Build facts.
pub struct FabricBuildStateAdapter {
    local: Arc<dyn LocalBuildLog>,
    sender: Arc<tokio::sync::RwLock<GossipSender>>,
    /// Every peer known on the Build topic (seeds, neighbours, admins heard).
    known: Arc<std::sync::Mutex<Vec<iroh::EndpointId>>>,
    /// The admins joined while they stayed live.
    joined: std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>,
}

impl FabricBuildStateAdapter {
    /// Join `fabric`'s Build topic through `peers` (the other node-admins), as
    /// bootstrap hints: it returns at once, connected or not.
    ///
    /// `accepted` is this admin's `Fabric.build_id` holder: every Fabric record heard on the
    /// topic is offered to it, and a new neighbour is handed the one it holds first. `node` names
    /// this admin in evidence.
    pub async fn join(
        gossip: &Gossip,
        endpoint: &Endpoint,
        fabric: &rafka_mesh_entity::FabricId,
        peers: Vec<EndpointAddr>,
        local: Arc<dyn LocalBuildLog>,
        accepted: Arc<AcceptedStore>,
        shutdown: Arc<crate::shutdown::ShutdownControl>,
        node: String,
    ) -> Result<Self, BuildStateError> {
        let io = |e: String| BuildStateError::Io(format!("fabric Build topic {fabric}: {e}"));
        let lookup = MemoryLookup::new();
        for p in &peers {
            lookup.add_endpoint_info(p.clone());
        }
        endpoint.address_lookup().map_err(|e| io(e.to_string()))?.add(lookup);
        // Seeds are bootstrap hints, never a start barrier: a recorded seed can be dead (an admin
        // that restarted binds a new mesh port), and a dead seed must not keep this admin from the
        // point where it can take part in recovery. Subscribe locally; the seeds are dialled as the
        // topic's first peers and every admin heard later is joined through `join_admins`/refeed.
        let ids = peers.iter().map(|p| p.id).collect();
        let topic = gossip.subscribe(build_topic(fabric), ids).await.map_err(|e| io(e.to_string()))?;
        let (sender, mut receiver) = topic.split();
        let sender = Arc::new(tokio::sync::RwLock::new(sender));
        let (absorb, shared, store, held_shutdown) = (local.clone(), sender.clone(), accepted.clone(), shutdown.clone());
        let (fabric_name, gossip, topic_id) = (fabric.to_string(), gossip.clone(), build_topic(fabric));
        let seed_ids: Vec<iroh::EndpointId> = peers.iter().map(|p| p.id).collect();
        let known_peers: Arc<std::sync::Mutex<Vec<iroh::EndpointId>>> = Arc::new(std::sync::Mutex::new(seed_ids.clone()));
        let neighbors: Arc<std::sync::Mutex<std::collections::BTreeSet<iroh::EndpointId>>> = Arc::default();
        refeed(fabric.to_string(), shared.clone(), known_peers.clone(), neighbors.clone());
        let known_peers_handle = known_peers.clone();
        let seed_ids_set: std::collections::BTreeSet<iroh::EndpointId> = seed_ids.iter().copied().collect();
        tokio::spawn(async move {
            loop {
                // Receive until the subscription lags or ends; iroh-gossip
                // closes a lagging subscriber and expects it to be re-opened.
                let reason = loop {
                    match receiver.next().await {
                        Some(Ok(Event::Received(m))) => match serde_json::from_slice::<Wire>(&m.content) {
                            Ok(w) => {
                                let heard_fabric = w.fabric;
                                if let Some(sd) = w.shutdown {
                                    if let Err(e) = held_shutdown.learn(sd, "gossip", &m.delivered_from.to_string()) {
                                        tracing::info_span!("rafka.node_admin.fabric.reject.via-shutdown-unpersisted", node = %node, error = %e)
                                            .in_scope(|| tracing::info!("a fabric shutdown could not be persisted"));
                                    }
                                }
                                absorb.absorb_facts(&w.facts);
                                if let Some(r) = heard_fabric {
                                    store.learn(r, &*absorb, &m.delivered_from.to_string()).await;
                                } else if !w.facts.is_empty() {
                                    store.resolve_wanted(&*absorb).await;
                                }
                            }
                            Err(e) => tracing::info_span!("rafka.node_admin.build.reject.via-undecodable-fact", fabric = %fabric_name, error = %e)
                                .in_scope(|| tracing::info!("a Build fact from the fabric does not decode")),
                        },
                        Some(Ok(Event::NeighborUp(peer))) => {
                            known_peers.lock().unwrap().push(peer);
                            neighbors.lock().unwrap().insert(peer);
                            // Sent from its own task: the receive loop never waits on the actor.
                            let (absorb, shared, fabric_name) = (absorb.clone(), shared.clone(), fabric_name.clone());
                            let (record, pointer) = (store.record().ok().flatten(), store.build_id());
                            let in_force = held_shutdown.held();
                            tokio::spawn(async move {
                                // A shutdown in force first: the neighbour comes up frozen.
                                if let Some(sd) = in_force.as_ref().and_then(|sd| encode_shutdown(sd).ok()) {
                                    let _ = shared.read().await.clone().broadcast_neighbors(sd).await;
                                }
                                // The Fabric record first, as its own message; then the facts of the
                                // Build it names and of every active Build.
                                if let Some(r) = record.as_ref().and_then(|r| encode_fabric(r).ok()) {
                                    let _ = shared.read().await.clone().broadcast_neighbors(r).await;
                                }
                                let facts = active_facts(&*absorb, pointer.clone()).await;
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
        Ok(Self { local, sender, known: known_peers_handle, joined: std::sync::Mutex::new(seed_ids_set) })
    }

    /// Join the Build topic to every live node-admin `admins` names, as the
    /// backbone does (`gossip.md` §6): each once while it stays live, again
    /// when it returns. An admin that accepted a Build while cut off reaches
    /// every admin directly when it heals, and the NeighborUp catch-up hands
    /// each one its active Builds; one neighbour alone would leave the rest to
    /// HyParView's shuffle (a minute per peer). Returns how many were joined.
    pub async fn join_admins(&self, admins: Vec<EndpointAddr>) -> usize {
        let live: Vec<iroh::EndpointId> = admins.iter().map(|a| a.id).collect();
        let fresh = rafka_mesh_transport::membership::rejoin(&self.joined, &live);
        if fresh.is_empty() {
            return 0;
        }
        self.known.lock().unwrap().extend(fresh.iter().copied());
        let sender = self.sender.read().await.clone();
        match sender.join_peers(fresh.clone()).await {
            Ok(()) => fresh.len(),
            Err(_) => 0,
        }
    }

    /// Broadcast a fabric shutdown on the control channel (fabric-mesh-lifecycle.md §11.1).
    pub async fn publish_shutdown(&self, shutdown: &crate::fabric_storage::FabricShutdown) -> Result<(), BuildStateError> {
        let sender = self.sender.read().await.clone();
        sender.broadcast(encode_shutdown(shutdown)?).await.map_err(|e| BuildStateError::Io(format!("broadcasting fabric shutdown: {e}")))
    }

    async fn broadcast(&self, fact: BuildFact) -> Result<(), BuildStateError> {
        let sender = self.sender.read().await.clone();
        sender.broadcast(encode(vec![fact])?).await.map_err(|e| BuildStateError::Io(format!("broadcasting Build fact: {e}")))
    }
}

#[async_trait::async_trait]
impl BuildStateAdapter for FabricBuildStateAdapter {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        self.local.publish_accepted(accepted).await?;
        self.broadcast(BuildFact::Accepted(accepted.clone())).await
    }

    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        self.local.open_attempt(opened).await?;
        self.broadcast(BuildFact::Opened(opened.clone())).await
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


    async fn publish_fabric(&self, record: &FabricRecord) -> Result<(), BuildStateError> {
        let sender = self.sender.read().await.clone();
        sender.broadcast(encode_fabric(record)?).await.map_err(|e| BuildStateError::Io(format!("broadcasting the Fabric record: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::BuildAttemptClaim;

    /// Packing measures each batch once and sends those bytes: a fresh
    /// nonce of another length can never push a sent message over the limit.
    #[test]
    fn every_packed_message_fits_whatever_nonce_it_drew() {
        for round in 0..200u32 {
            let facts: Vec<BuildFact> = (0..60u32)
                .map(|i| {
                    BuildFact::Claim(BuildAttemptClaim {
                        build_id: BuildId::mint(),
                        attempt: i,
                        executor: format!("mesh{}.admin.{}", round, "x".repeat(((i * 37 + round) % 90) as usize)),
                    })
                })
                .collect();
            let (messages, refused) = encode_chunks(facts);
            assert!(refused.is_empty());
            assert!(messages.iter().all(|m| m.len() <= MAX_MESSAGE_BYTES), "every message fits");
            let sent: usize = messages.iter().map(|m| serde_json::from_slice::<serde_json::Value>(m).unwrap()["facts"].as_array().unwrap().len()).sum();
            assert_eq!(sent, 60, "no fact lost");
        }
    }
}
