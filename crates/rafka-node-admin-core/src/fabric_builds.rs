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

fn encode(facts: Vec<BuildFact>) -> Result<bytes::Bytes, BuildStateError> {
    serde_json::to_vec(&Wire { nonce: rand::random(), facts }).map(Into::into).map_err(|e| BuildStateError::Io(e.to_string()))
}

/// The facts of every active Build `local` holds, in append order.
async fn active_facts(local: &MemoryBuildStateAdapter) -> Vec<BuildFact> {
    let active: std::collections::BTreeSet<BuildId> = match local.list_active().await {
        Ok(a) => a.into_iter().map(|b| b.build_id).collect(),
        Err(_) => return Vec::new(),
    };
    local.facts().await.unwrap_or_default().into_iter().filter(|f| active.contains(f.build_id())).collect()
}

/// The fabric's Build topic.
pub fn build_topic(fabric: &str) -> TopicId {
    TopicId::from_bytes(*blake3::hash(format!("rafka-fabric-builds:{fabric}").as_bytes()).as_bytes())
}

/// A node-admin's Build state: its own copy of the fabric's Build facts.
pub struct FabricBuildStateAdapter {
    local: Arc<MemoryBuildStateAdapter>,
    sender: GossipSender,
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
        let local = Arc::new(MemoryBuildStateAdapter::new());
        let (absorb, catch_up) = (local.clone(), sender.clone());
        let fabric_name = fabric.to_string();
        tokio::spawn(async move {
            while let Some(ev) = receiver.next().await {
                match ev {
                    Ok(Event::Received(m)) => match serde_json::from_slice::<Wire>(&m.content) {
                        Ok(w) => absorb.absorb(&w.facts),
                        Err(e) => tracing::info_span!("rafka.node_admin.build.reject.via-undecodable-fact", fabric = %fabric_name, error = %e)
                            .in_scope(|| tracing::info!("a Build fact from the fabric does not decode")),
                    },
                    Ok(Event::NeighborUp(peer)) => {
                        let facts = active_facts(&absorb).await;
                        let span = tracing::info_span!(
                            "rafka.node_admin.build.update.via-neighbor-up",
                            fabric = %fabric_name,
                            peer = %peer,
                            facts = facts.len(),
                        );
                        if !facts.is_empty() {
                            if let Ok(bytes) = encode(facts) {
                                let _ = catch_up.broadcast_neighbors(bytes).await;
                            }
                        }
                        drop(span);
                    }
                    _ => {}
                }
            }
        });
        Ok(Self { local, sender })
    }

    async fn broadcast(&self, fact: BuildFact) -> Result<(), BuildStateError> {
        self.sender.broadcast(encode(vec![fact])?).await.map_err(|e| BuildStateError::Io(format!("broadcasting Build fact: {e}")))
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
