//! Fabric membership over iroh-gossip (HyParView + Plumtree; never a
//! hand-rolled delivery layer). Every member broadcasts its [`MeshDigest`] on
//! the fabric's membership topic; every member keeps the latest digest per
//! logical node it heard.

use anyhow::Result;
use bytes::Bytes;
use futures_lite::StreamExt as _;
use iroh::address_lookup::MemoryLookup;
use iroh::{Endpoint, EndpointAddr};
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use rafka_mesh_entity::MeshDigest;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The fabric's membership topic.
pub fn fabric_topic(fabric: &str) -> TopicId {
    TopicId::from_bytes(*blake3::hash(format!("rafka-fabric-membership:{fabric}").as_bytes()).as_bytes())
}

/// Tell `endpoint` where `peers` are (no discovery service runs).
pub fn learn_addresses(endpoint: &Endpoint, peers: &[EndpointAddr]) -> Result<()> {
    let lookup = MemoryLookup::new();
    for p in peers {
        lookup.add_endpoint_info(p.clone());
    }
    endpoint.address_lookup().map_err(|e| anyhow::anyhow!("address_lookup: {e}"))?.add(lookup);
    Ok(())
}

/// `RAFKA_LEAVE_LINGER_MS` (default 1000): how long a stopping node keeps
/// announcing `Leaving` before it closes. Every node kind takes the same one.
pub fn leave_linger_from_env() -> Duration {
    Duration::from_millis(std::env::var("RAFKA_LEAVE_LINGER_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000))
}

/// How often a leaving node repeats its `Leaving` during the linger.
pub const LEAVE_EVERY: Duration = Duration::from_millis(200);

/// Announce a departure: `say` the `Leaving` digest now and again every
/// `every` until `linger` has passed, then return. Bounded by `linger`: a
/// `say` that does not finish in time is abandoned. Returns how many
/// announcements finished.
///
/// iroh-gossip acknowledges nothing and closing drops what is unsent, so one
/// announcement can be lost; repeating it for the linger is the node's own
/// shutdown, not a delivery layer (nothing is retried after the node leaves).
pub async fn announce_leaving<F, Fut>(linger: Duration, every: Duration, mut say: F) -> u32
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let until = tokio::time::Instant::now() + linger;
    let mut said = 0;
    loop {
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return said;
        }
        if tokio::time::timeout(left, say()).await.is_ok() {
            said += 1;
        }
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return said;
        }
        tokio::time::sleep(every.min(left)).await;
    }
}

/// The latest digest heard per logical node.
#[derive(Debug, Default, Clone)]
pub struct DigestBook {
    inner: Arc<Mutex<HashMap<String, (MeshDigest, Instant)>>>,
}

impl DigestBook {
    /// Hold `d` as its member's latest word, unless it is older than what is
    /// held: a digest of the same birth emitted no later than the held one,
    /// or a digest of the birth the held one supersedes. Gossip can deliver
    /// a digest late; a late one never refreshes a silent member or reverts
    /// its status. `false` when `d` was not taken.
    pub fn record(&self, d: MeshDigest) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some((held, _)) = inner.get(&d.node.node_id.0) {
            let older = if held.node.incarnation == d.node.incarnation {
                d.emitted_unix_ms <= held.emitted_unix_ms
            } else {
                held.node.supersedes.as_ref() == Some(&d.node.incarnation)
            };
            if older {
                return false;
            }
        }
        inner.insert(d.node.node_id.0.clone(), (d, Instant::now()));
        true
    }

    /// Digests heard within `fresh` of now.
    pub fn current(&self, fresh: Duration) -> Vec<MeshDigest> {
        self.inner.lock().unwrap().values().filter(|(_, at)| at.elapsed() <= fresh).map(|(d, _)| d.clone()).collect()
    }

    pub fn get(&self, node_id: &str) -> Option<(MeshDigest, Duration)> {
        self.inner.lock().unwrap().get(node_id).map(|(d, at)| (d.clone(), at.elapsed()))
    }

    pub fn all(&self) -> Vec<MeshDigest> {
        self.inner.lock().unwrap().values().map(|(d, _)| d.clone()).collect()
    }
}

/// A joined membership topic.
///
/// iroh-gossip closes a subscriber that falls behind (`Lagged`) and expects
/// it to be re-opened; a subscription can also end. Either way this member
/// re-subscribes through its seeds and every member it has heard, so it
/// never silently stops hearing the fabric.
#[derive(Clone)]
pub struct Membership {
    sender: Arc<tokio::sync::RwLock<GossipSender>>,
    pub book: DigestBook,
}

/// Why a subscription was re-opened, if it was.
fn ended(ev: Option<Result<Event, iroh_gossip::api::ApiError>>) -> Option<String> {
    match ev {
        None => Some("subscription ended".into()),
        Some(Err(e)) => Some(format!("subscription error: {e}")),
        Some(Ok(Event::Lagged)) => Some("lagged: events were dropped".into()),
        Some(Ok(_)) => None,
    }
}

impl Membership {
    /// Join `fabric`'s membership topic through `seeds`.
    pub async fn join(gossip: &Gossip, endpoint: &Endpoint, fabric: &str, seeds: Vec<EndpointAddr>) -> Result<Self> {
        learn_addresses(endpoint, &seeds)?;
        let topic_id = fabric_topic(fabric);
        let topic = gossip.subscribe(topic_id, seeds.iter().map(|s| s.id).collect()).await?;
        let (sender, mut receiver) = topic.split();
        let sender = Arc::new(tokio::sync::RwLock::new(sender));
        let book = DigestBook::default();
        let (b, shared, gossip, fabric) = (book.clone(), sender.clone(), gossip.clone(), fabric.to_string());
        let seed_ids: Vec<iroh::EndpointId> = seeds.iter().map(|s| s.id).collect();
        tokio::spawn(async move {
            loop {
                let reason = loop {
                    let ev = receiver.next().await;
                    if let Some(Ok(Event::Received(m))) = &ev {
                        if let Some(d) = MeshDigest::decode(&m.content) {
                            if d.fabric == fabric {
                                b.record(d);
                            }
                        }
                        continue;
                    }
                    if let Some(r) = ended(ev) {
                        break r;
                    }
                };
                // Re-open through the seeds and every member heard so far.
                let mut peers = seed_ids.clone();
                peers.extend(b.all().iter().filter_map(|d| d.node.fabric_id.0.parse::<iroh::EndpointId>().ok()));
                peers.sort();
                peers.dedup();
                let span = tracing::info_span!("rafka.mesh.membership.update.via-resubscribe", fabric = %fabric, reason = %reason, peers = peers.len());
                // A refused subscribe means the gossip actor itself has stopped.
                let reopened = match gossip.subscribe(topic_id, peers.clone()).await {
                    Ok(t) => t,
                    Err(e) => {
                        span.in_scope(|| tracing::info!(error = %e, "gossip has stopped; membership ends"));
                        return;
                    }
                };
                span.in_scope(|| tracing::info!("membership subscription re-opened"));
                let (s, r) = reopened.split();
                *shared.write().await = s;
                receiver = r;
            }
        });
        Ok(Self { sender, book })
    }

    /// Broadcast one digest (and record it locally).
    pub async fn publish(&self, d: &MeshDigest) -> Result<()> {
        self.book.record(d.clone());
        let sender = self.sender.read().await.clone();
        sender.broadcast(Bytes::from(d.encode())).await?;
        Ok(())
    }

    /// Re-broadcast `digest()` every `every` until the returned handle is aborted.
    pub fn publish_every<F>(&self, every: Duration, digest: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn() -> MeshDigest + Send + 'static,
    {
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                let _ = me.publish(&digest()).await;
                tokio::time::sleep(every).await;
            }
        })
    }

    /// Ask gossip to connect to more peers.
    pub async fn join_peers(&self, endpoint: &Endpoint, peers: Vec<EndpointAddr>) -> Result<()> {
        learn_addresses(endpoint, &peers)?;
        let sender = self.sender.read().await.clone();
        sender.join_peers(peers.iter().map(|p| p.id).collect()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::{EndpointSet, FabricId, IncarnationId, MemberStatus, MeshNode, NodeId};

    fn digest(node_id: &NodeId, incarnation: &IncarnationId, supersedes: Option<IncarnationId>, status: MemberStatus, at: u64) -> MeshDigest {
        MeshDigest {
            fabric: "fabric1".into(),
            node: MeshNode {
                node_id: node_id.clone(),
                name: "mesh1.rpc.1".parse().unwrap(),
                fabric_id: FabricId("key".into()),
                incarnation: incarnation.clone(),
                supersedes,
                endpoints: EndpointSet(vec![]),
            },
            status,
            admin_api_base: None,
            emitted_unix_ms: at,
            extra: Default::default(),
        }
    }

    #[test]
    fn a_late_digest_never_refreshes_a_member_or_reverts_its_status() {
        let book = DigestBook::default();
        let (id, birth) = (NodeId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &birth, None, MemberStatus::Leaving, 200)));
        std::thread::sleep(Duration::from_millis(30));
        assert!(!book.record(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 100)), "an older digest of the same birth");
        assert!(!book.record(digest(&id, &birth, None, MemberStatus::ReadyForTraffic, 200)), "a duplicate");
        let (held, age) = book.get(&id.0).unwrap();
        assert_eq!(held.status, MemberStatus::Leaving, "the status is not reverted");
        assert!(age >= Duration::from_millis(30), "the member's silence is not reset");
        assert!(book.record(digest(&id, &birth, None, MemberStatus::Leaving, 300)), "a newer digest is taken");
    }

    #[test]
    fn a_successor_birth_is_taken_and_its_predecessors_late_digests_are_not() {
        let book = DigestBook::default();
        let (id, first, second) = (NodeId::mint(), IncarnationId::mint(), IncarnationId::mint());
        assert!(book.record(digest(&id, &first, None, MemberStatus::ReadyForTraffic, 500)));
        assert!(book.record(digest(&id, &second, Some(first.clone()), MemberStatus::ReadyForTraffic, 100)), "a new birth, whatever its clock");
        assert!(!book.record(digest(&id, &first, None, MemberStatus::ReadyForTraffic, 900)), "the superseded birth's late digest");
        assert_eq!(book.get(&id.0).unwrap().0.node.incarnation, second);
    }

    /// The first `Leaving` is lost; one of the later announcements in the
    /// linger still reaches the fabric.
    #[tokio::test]
    async fn a_lost_first_leaving_is_followed_by_one_that_arrives() {
        let said = Arc::new(Mutex::new(0u32));
        let heard = Arc::new(Mutex::new(0u32));
        let (s, h) = (said.clone(), heard.clone());
        announce_leaving(Duration::from_millis(300), Duration::from_millis(50), move || {
            let (s, h) = (s.clone(), h.clone());
            async move {
                let n = {
                    let mut s = s.lock().unwrap();
                    *s += 1;
                    *s
                };
                if n > 1 {
                    *h.lock().unwrap() += 1;
                }
            }
        })
        .await;
        assert!(*heard.lock().unwrap() >= 1, "only the lost first Leaving was said ({} said)", said.lock().unwrap());
    }

    /// Announcing never keeps the node past its linger, even when an
    /// announcement never finishes.
    #[tokio::test]
    async fn announcing_never_outlives_the_linger() {
        let linger = Duration::from_millis(300);
        let start = Instant::now();
        let n = announce_leaving(linger, Duration::from_millis(50), || std::future::pending::<()>()).await;
        let took = start.elapsed();
        assert!(took < linger + Duration::from_millis(150), "a stuck announcement held the node {took:?}");
        assert_eq!(n, 0, "no announcement finished");

        let start = Instant::now();
        let n = announce_leaving(linger, Duration::from_millis(50), || async {}).await;
        let took = start.elapsed();
        assert!(took < linger + Duration::from_millis(150), "repeating held the node {took:?}");
        assert!((2..=8).contains(&n), "{n} announcements in a 300 ms linger at 50 ms");
    }
}
