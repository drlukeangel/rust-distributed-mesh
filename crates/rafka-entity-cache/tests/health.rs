use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use rafka_entity_cache::health::{test_channel, subscribe_cache_events, CacheEvent};
use rafka_entity_cache::traits::{BrokerRecord, BrokerRpc, CacheTier, CachedEntity, MeshGossip};
use rafka_entity_cache::EntityCache;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FuncEntity1 { id: String }
impl CachedEntity for FuncEntity1 {
    type Key = String;
    fn key(&self) -> String { self.id.clone() }
    fn topic_path(_: Option<&str>) -> Cow<'static, str> { Cow::Borrowed("topic") }
    fn entity_kind() -> &'static str { "func_kind_1" }
    fn put_op() -> u16 { 1 }
    fn delete_op() -> u16 { 2 }
    fn get_op() -> u16 { 3 }
    fn snapshot_op() -> u16 { 4 }
    fn delete_retention() -> Duration { Duration::from_secs(60) }
    fn cache_tier() -> CacheTier { CacheTier::Functional }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FeatEntity1 { id: String }
impl CachedEntity for FeatEntity1 {
    type Key = String;
    fn key(&self) -> String { self.id.clone() }
    fn topic_path(_: Option<&str>) -> Cow<'static, str> { Cow::Borrowed("topic") }
    fn entity_kind() -> &'static str { "feat_kind_1" }
    fn put_op() -> u16 { 1 }
    fn delete_op() -> u16 { 2 }
    fn get_op() -> u16 { 3 }
    fn snapshot_op() -> u16 { 4 }
    fn delete_retention() -> Duration { Duration::from_secs(60) }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FuncEntity2 { id: String }
impl CachedEntity for FuncEntity2 {
    type Key = String;
    fn key(&self) -> String { self.id.clone() }
    fn topic_path(_: Option<&str>) -> Cow<'static, str> { Cow::Borrowed("topic") }
    fn entity_kind() -> &'static str { "func_kind_2" }
    fn put_op() -> u16 { 1 }
    fn delete_op() -> u16 { 2 }
    fn get_op() -> u16 { 3 }
    fn snapshot_op() -> u16 { 4 }
    fn delete_retention() -> Duration { Duration::from_secs(60) }
    fn cache_tier() -> CacheTier { CacheTier::Functional }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UniqueEntity { id: String }
impl CachedEntity for UniqueEntity {
    type Key = String;
    fn key(&self) -> String { self.id.clone() }
    fn topic_path(_: Option<&str>) -> Cow<'static, str> { Cow::Borrowed("topic") }
    fn entity_kind() -> &'static str { "unique_kind" }
    fn put_op() -> u16 { 1 }
    fn delete_op() -> u16 { 2 }
    fn get_op() -> u16 { 3 }
    fn snapshot_op() -> u16 { 4 }
    fn delete_retention() -> Duration { Duration::from_secs(60) }
    fn cache_tier() -> CacheTier { CacheTier::Functional }
}

struct StubBroker;
impl BrokerRpc for StubBroker {
    fn next_record(&self, _: &str) -> impl std::future::Future<Output = Result<BrokerRecord, String>> + Send {
        async { Err("stub".into()) }
    }
    fn put_entity<E: CachedEntity>(&self, _: u16, _: &E, _: bool) -> impl std::future::Future<Output = Result<u64, String>> + Send {
        async { Ok(1) }
    }
    fn delete_entity<E: CachedEntity>(&self, _: u16, _: &E::Key) -> impl std::future::Future<Output = Result<u64, String>> + Send {
        async { Ok(2) }
    }
}

struct StubMesh;
impl MeshGossip for StubMesh {
    fn broadcast_update<E: CachedEntity>(&self, _: &E::Key, _: u64, _: Option<&E>) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }
}

#[tokio::test]
async fn functional_tier_cache_reports_functional_in_event() {
    let mut rx = subscribe_cache_events();
    let cache: EntityCache<FuncEntity1, StubBroker, StubMesh> = EntityCache::new(Arc::new(StubBroker), Arc::new(StubMesh), "topic".into());
    cache.mark_ready();

    loop {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Ok(CacheEvent::Ready { entity_kind, tier })) => {
                if entity_kind == "func_kind_1" {
                    assert_eq!(tier, CacheTier::Functional);
                    break;
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) => {}
            Err(_) => panic!("timeout"),
        }
    }
}

#[tokio::test]
async fn feature_tier_cache_reports_feature_in_event() {
    let mut rx = subscribe_cache_events();
    let cache: EntityCache<FeatEntity1, StubBroker, StubMesh> = EntityCache::new(Arc::new(StubBroker), Arc::new(StubMesh), "topic".into());
    cache.mark_ready();

    loop {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Ok(CacheEvent::Ready { entity_kind, tier })) => {
                if entity_kind == "feat_kind_1" {
                    assert_eq!(tier, CacheTier::Feature);
                    break;
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) => {}
            Err(_) => panic!("timeout"),
        }
    }
}

#[tokio::test]
async fn mark_ready_publishes_ready_event() {
    let mut rx = subscribe_cache_events();
    let cache: EntityCache<FuncEntity2, StubBroker, StubMesh> = EntityCache::new(Arc::new(StubBroker), Arc::new(StubMesh), "topic".into());
    cache.mark_ready();
    
    loop {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Ok(CacheEvent::Ready { entity_kind, tier })) => {
                if entity_kind == "func_kind_2" {
                    assert_eq!(tier, CacheTier::Functional);
                    break;
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) => {}
            Err(_) => panic!("timeout"),
        }
    }
}

struct TestHydrationGate {
    tx: tokio::sync::watch::Sender<bool>,
    rx: tokio::sync::watch::Receiver<bool>,
}
impl TestHydrationGate {
    fn new(mut rx_events: tokio::sync::broadcast::Receiver<CacheEvent>) -> Self {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let gate = Self { tx, rx };
        let tx_clone = gate.tx.clone();
        tokio::spawn(async move {
            let mut ready_set = std::collections::HashSet::new();
            let target_set: std::collections::HashSet<&'static str> = [
                "compiled_acl",
                "service_account_credential",
                "iam_group"
            ].into_iter().collect();

            while let Ok(event) = rx_events.recv().await {
                if let CacheEvent::Ready { entity_kind, tier: CacheTier::Functional } = event {
                    if target_set.contains(entity_kind) {
                        ready_set.insert(entity_kind);
                        if ready_set.len() == target_set.len() {
                            let _ = tx_clone.send(true);
                            break;
                        }
                    }
                }
            }
        });
        gate
    }
    async fn wait(&self) {
        let mut rx = self.rx.clone();
        let _ = rx.wait_for(|v| *v).await;
    }
}

#[tokio::test]
async fn gate_opens_when_all_three_functional_caches_emit_ready() {
    let (tx, rx_events) = test_channel();
    let gate = TestHydrationGate::new(rx_events);
    
    let _ = tx.send(CacheEvent::Ready { entity_kind: "compiled_acl", tier: CacheTier::Functional });
    let _ = tx.send(CacheEvent::Ready { entity_kind: "service_account_credential", tier: CacheTier::Functional });
    let _ = tx.send(CacheEvent::Ready { entity_kind: "iam_group", tier: CacheTier::Functional });

    let result = tokio::time::timeout(Duration::from_millis(500), gate.wait()).await;
    assert!(result.is_ok(), "gate did not open");
}

#[tokio::test]
async fn gate_does_not_open_until_all_three_emit() {
    let (tx, rx_events) = test_channel();
    let gate = TestHydrationGate::new(rx_events);
    
    let _ = tx.send(CacheEvent::Ready { entity_kind: "compiled_acl", tier: CacheTier::Functional });
    let _ = tx.send(CacheEvent::Ready { entity_kind: "service_account_credential", tier: CacheTier::Functional });

    let result = tokio::time::timeout(Duration::from_millis(100), gate.wait()).await;
    assert!(result.is_err(), "gate opened prematurely");

    let _ = tx.send(CacheEvent::Ready { entity_kind: "iam_group", tier: CacheTier::Functional });
    let result2 = tokio::time::timeout(Duration::from_millis(500), gate.wait()).await;
    assert!(result2.is_ok(), "gate did not open after third emit");
}

#[tokio::test]
async fn degraded_event_debounced_at_10s_per_entity_kind() {
    tokio::time::pause();

    let mut rx = subscribe_cache_events();
    let cache: EntityCache<UniqueEntity, StubBroker, StubMesh> = EntityCache::new(Arc::new(StubBroker), Arc::new(StubMesh), "topic".into());
    cache.mark_ready();

    // Drain Ready event
    while let Ok(ev) = rx.try_recv() {
        if let CacheEvent::Ready { entity_kind, .. } = ev {
            if entity_kind == "unique_kind" { break; }
        }
    }

    let cache_arc = Arc::new(cache);
    let handle = cache_arc.clone().spawn_tailer(rafka_entity_cache::traits::TailScope::Global, None);

    let mut ev1 = None;
    for _ in 0..20 {
        if let Ok(CacheEvent::Degraded { entity_kind, reason, .. }) = rx.try_recv() {
            if entity_kind == "unique_kind" {
                ev1 = Some(reason);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(ev1.unwrap(), "stub");

    tokio::time::sleep(Duration::from_secs(1)).await;
    
    while let Ok(ev) = rx.try_recv() {
        if let CacheEvent::Degraded { entity_kind, .. } = ev {
            if entity_kind == "unique_kind" {
                panic!("should be debounced within 10s");
            }
        }
    }

    handle.abort();
}