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

/// The latest digest heard per logical node.
#[derive(Debug, Default, Clone)]
pub struct DigestBook {
    inner: Arc<Mutex<HashMap<String, (MeshDigest, Instant)>>>,
}

impl DigestBook {
    pub fn record(&self, d: MeshDigest) {
        self.inner.lock().unwrap().insert(d.node.node_id.0.clone(), (d, Instant::now()));
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
