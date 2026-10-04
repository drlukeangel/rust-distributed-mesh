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
#[derive(Clone)]
pub struct Membership {
    sender: GossipSender,
    pub book: DigestBook,
}

impl Membership {
    /// Join `fabric`'s membership topic through `seeds`.
    pub async fn join(gossip: &Gossip, endpoint: &Endpoint, fabric: &str, seeds: Vec<EndpointAddr>) -> Result<Self> {
        learn_addresses(endpoint, &seeds)?;
        let topic = gossip.subscribe(fabric_topic(fabric), seeds.iter().map(|s| s.id).collect()).await?;
        let (sender, mut receiver) = topic.split();
        let book = DigestBook::default();
        let b = book.clone();
        let fabric = fabric.to_string();
        tokio::spawn(async move {
            while let Some(ev) = receiver.next().await {
                if let Ok(Event::Received(m)) = ev {
                    if let Some(d) = MeshDigest::decode(&m.content) {
                        if d.fabric == fabric {
                            b.record(d);
                        }
                    }
                }
            }
        });
        Ok(Self { sender, book })
    }

    /// Broadcast one digest (and record it locally).
    pub async fn publish(&self, d: &MeshDigest) -> Result<()> {
        self.book.record(d.clone());
        self.sender.broadcast(Bytes::from(d.encode())).await?;
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
        self.sender.join_peers(peers.iter().map(|p| p.id).collect()).await?;
        Ok(())
    }
}
