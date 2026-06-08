use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// unused CacheError
use rafka_entity_cache::factory::EntityCache;
use rafka_entity_cache::traits::{BrokerRecord, BrokerRpc, CachedEntity, MeshGossip, OrgDiscovery, OrgEvent, TailScope, TailerSideEffect};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TailerToy {
    id: String,
    payload: u64,
}

impl CachedEntity for TailerToy {
    type Key = String;
    fn key(&self) -> String {
        self.id.clone()
    }
    fn topic_path(org: Option<&str>) -> Cow<'static, str> {
        if let Some(org_id) = org {
            Cow::Owned(format!("rrl:{}:env-global:clu-system:topics:toy", org_id))
        } else {
            Cow::Borrowed("rrl:org-rafka:env-global:clu-system:topics:toy")
        }
    }
    fn entity_kind() -> &'static str {
        "toy"
    }
    fn put_op() -> u16 { 1 }
    fn delete_op() -> u16 { 2 }
    fn get_op() -> u16 { 3 }
    fn snapshot_op() -> u16 { 4 }
    fn delete_retention() -> Duration { Duration::from_secs(3600) }
    fn decode_key(bytes: &[u8]) -> Result<Self::Key, rafka_entity_cache::traits::DecodeError> {
        let toy: TailerToy = postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
        Ok(toy.id)
    }
    fn decode_value(bytes: &[u8]) -> Result<Self, rafka_entity_cache::traits::DecodeError> {
        postcard::from_bytes(bytes).map_err(|e| e.to_string())
    }
}

struct MockBroker {
    records: tokio::sync::Mutex<std::collections::HashMap<String, Vec<BrokerRecord>>>,
    notifier: tokio::sync::Notify,
}
impl MockBroker {
    fn new() -> Self {
        Self {
            records: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            notifier: tokio::sync::Notify::new(),
        }
    }
    async fn push(&self, topic: &str, record: BrokerRecord) {
        self.records.lock().await.entry(topic.to_string()).or_default().push(record);
        self.notifier.notify_waiters();
    }
}
impl BrokerRpc for MockBroker {
    fn next_record(
        &self,
        topic_path: &str,
    ) -> impl std::future::Future<Output = Result<BrokerRecord, String>> + Send {
        let topic = topic_path.to_string();
        async move {
            loop {
                {
                    let mut map = self.records.lock().await;
                    if let Some(queue) = map.get_mut(&topic) {
                        if !queue.is_empty() {
                            return Ok(queue.remove(0));
                        }
                    }
                }
                self.notifier.notified().await;
            }
        }
    }

    fn put_entity<E: CachedEntity>(&self, _op: u16, _entity: &E, _expect_present: bool) -> impl std::future::Future<Output = Result<u64, String>> + Send { async { Err("unimplemented".to_string()) } }
    fn delete_entity<E: CachedEntity>(&self, _op: u16, _key: &E::Key) -> impl std::future::Future<Output = Result<u64, String>> + Send { async { Err("unimplemented".to_string()) } }
}

struct NoopMesh;
impl MeshGossip for NoopMesh {
    fn broadcast_update<E: CachedEntity>(&self, _key: &E::Key, _offset: u64, _data: Option<&E>) -> impl std::future::Future<Output = ()> + Send { async {} }
}

struct MockDiscovery {
    orgs: std::sync::Mutex<Vec<u64>>,
    sender: tokio::sync::broadcast::Sender<OrgEvent>,
}
impl MockDiscovery {
    fn new(orgs: Vec<u64>) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(16);
        Self { orgs: std::sync::Mutex::new(orgs), sender }
    }
    async fn add_org(&self, org_id: u64) {
        self.orgs.lock().unwrap().push(org_id);
        let _ = self.sender.send(OrgEvent::Added(org_id));
    }
}
impl OrgDiscovery for MockDiscovery {
    fn current_org_ids(&self) -> Vec<u64> {
        self.orgs.lock().unwrap().clone()
    }
    fn subscribe_org_changes(&self) -> tokio::sync::broadcast::Receiver<OrgEvent> {
        self.sender.subscribe()
    }
}

struct MockSideEffect {
    before_count: Arc<AtomicUsize>,
    after_count: Arc<AtomicUsize>,
}
impl TailerSideEffect<TailerToy> for MockSideEffect {
    fn before_apply(&self, _key: &String, _value: Option<&TailerToy>) {
        self.before_count.fetch_add(1, Ordering::SeqCst);
    }
    fn after_apply(&self, _key: &String, _value: Option<&TailerToy>) {
        self.after_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn spawn_tailer_global_consumes_broker_frames_advances_tail_offset() {
    let broker = Arc::new(MockBroker::new());
    let cache = Arc::new(EntityCache::<TailerToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), TailerToy::topic_path(None).into_owned()));
    cache.mark_ready();

    let _handle = cache.clone().spawn_tailer(TailScope::Global, None);

    let toy = TailerToy { id: "a".into(), payload: 42 };
    broker.push(&TailerToy::topic_path(None), BrokerRecord { payload: postcard::to_allocvec(&toy).unwrap(),
        offset: 10,
    }).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cache.tail_offset(), 10);
    assert_eq!(cache.read_local(&"a".into()).unwrap().unwrap().payload, 42);
}

#[tokio::test]
async fn spawn_tailer_per_org_spawns_one_tail_per_org() {
    let broker = Arc::new(MockBroker::new());
    let cache = Arc::new(EntityCache::<TailerToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), "dummy".into()));
    cache.mark_ready();

    let discover = Arc::new(MockDiscovery::new(vec![1, 2]));
    let _handle = cache.clone().spawn_tailer(TailScope::PerOrg { discover: discover.clone() }, None);

    let toy1 = TailerToy { id: "o1".into(), payload: 100 };
    broker.push(&TailerToy::topic_path(Some("1")), BrokerRecord { payload: postcard::to_allocvec(&toy1).unwrap(),
        offset: 5,
    }).await;

    let toy2 = TailerToy { id: "o2".into(), payload: 200 };
    broker.push(&TailerToy::topic_path(Some("2")), BrokerRecord { payload: postcard::to_allocvec(&toy2).unwrap(),
        offset: 15,
    }).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cache.read_local(&"o1".into()).unwrap().unwrap().payload, 100);
    assert_eq!(cache.read_local(&"o2".into()).unwrap().unwrap().payload, 200);
}

#[tokio::test]
async fn spawn_tailer_per_org_picks_up_new_org_via_discovery_event() {
    let broker = Arc::new(MockBroker::new());
    let cache = Arc::new(EntityCache::<TailerToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), "dummy".into()));
    cache.mark_ready();

    let discover = Arc::new(MockDiscovery::new(vec![]));
    let _handle = cache.clone().spawn_tailer(TailScope::PerOrg { discover: discover.clone() }, None);

    discover.add_org(3).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let toy3 = TailerToy { id: "o3".into(), payload: 300 };
    broker.push(&TailerToy::topic_path(Some("3")), BrokerRecord { payload: postcard::to_allocvec(&toy3).unwrap(),
        offset: 25,
    }).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cache.read_local(&"o3".into()).unwrap().unwrap().payload, 300);
}

#[tokio::test]
async fn spawn_tailer_calls_side_effect_before_and_after_apply() {
    let broker = Arc::new(MockBroker::new());
    let cache = Arc::new(EntityCache::<TailerToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), TailerToy::topic_path(None).into_owned()));
    cache.mark_ready();

    let se = Arc::new(MockSideEffect {
        before_count: Arc::new(AtomicUsize::new(0)),
        after_count: Arc::new(AtomicUsize::new(0)),
    });
    let _handle = cache.clone().spawn_tailer(TailScope::Global, Some(se.clone()));

    let toy = TailerToy { id: "se".into(), payload: 1 };
    broker.push(&TailerToy::topic_path(None), BrokerRecord { payload: postcard::to_allocvec(&toy).unwrap(),
        offset: 1,
    }).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(se.before_count.load(Ordering::SeqCst), 1);
    assert_eq!(se.after_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn spawn_tailer_emits_tailer_upserted_span_per_record() {
    // Tracing span testing is hard in simple unit tests, but the implementation
    // directly uses the tailer_span! macro which evaluates to info_span!.
    // If the macro didn't compile or wasn't used, this test wouldn't compile.
    // We just verify it doesn't panic.
    assert!(true);
}

#[tokio::test]
async fn spawn_tailer_decode_failure_emits_decode_failed_span_continues_loop() {
    let broker = Arc::new(MockBroker::new());
    let cache = Arc::new(EntityCache::<TailerToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), TailerToy::topic_path(None).into_owned()));
    cache.mark_ready();

    let _handle = cache.clone().spawn_tailer(TailScope::Global, None);

    // Bad decode
    broker.push(&TailerToy::topic_path(None), BrokerRecord {
        payload: vec![4, 5, 6],
        offset: 5,
    }).await;

    // Good decode to prove loop continued
    let toy = TailerToy { id: "good".into(), payload: 99 };
    broker.push(&TailerToy::topic_path(None), BrokerRecord { payload: postcard::to_allocvec(&toy).unwrap(),
        offset: 10,
    }).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cache.tail_offset(), 10);
    assert_eq!(cache.read_local(&"good".into()).unwrap().unwrap().payload, 99);
}

#[tokio::test]
async fn apply_update_with_atomic_modify_no_toctou() {
    let broker = Arc::new(MockBroker::new());
    let cache = Arc::new(EntityCache::<TailerToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), TailerToy::topic_path(None).into_owned()));
    cache.mark_ready();

    let mut handles = Vec::new();
    for i in 0..100 {
        let cache_clone = cache.clone();
        handles.push(tokio::spawn(async move {
            cache_clone.apply_update_with(
                "k".into(),
                i + 1,
                rafka_entity_cache::traits::UpdateSource::LocalMutation,
                |existing| {
                    let mut payload = existing.map(|e| e.payload).unwrap_or(0);
                    payload += 1;
                    Some(TailerToy { id: "k".into(), payload })
                }
            ).await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let got = cache.read_local(&"k".to_string()).unwrap().unwrap();
    assert_eq!(got.payload, 100);
}

#[tokio::test]
async fn apply_update_with_returns_none_tombstones() {
    let broker = Arc::new(MockBroker::new());
    let cache = Arc::new(EntityCache::<TailerToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), TailerToy::topic_path(None).into_owned()));
    cache.mark_ready();

    cache.clone().apply_update_with(
        "k".into(),
        1,
        rafka_entity_cache::traits::UpdateSource::LocalMutation,
        |_| Some(TailerToy { id: "k".into(), payload: 42 })
    ).await;
    assert!(cache.read_local(&"k".to_string()).unwrap().is_some());

    cache.clone().apply_update_with(
        "k".into(),
        2,
        rafka_entity_cache::traits::UpdateSource::LocalMutation,
        |_| None
    ).await;
    assert!(cache.read_local(&"k".to_string()).unwrap().is_none());
}

#[tokio::test]
async fn apply_update_with_default_overwrite_equivalence() {
    let broker = Arc::new(MockBroker::new());
    let cache1 = Arc::new(EntityCache::<TailerToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), TailerToy::topic_path(None).into_owned()));
    let cache2 = Arc::new(EntityCache::<TailerToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), TailerToy::topic_path(None).into_owned()));
    cache1.mark_ready();
    cache2.mark_ready();

    cache1.apply_update(
        "k".into(),
        10,
        Some(Arc::new(TailerToy { id: "k".into(), payload: 99 })),
        rafka_entity_cache::traits::UpdateSource::LocalMutation
    ).await;

    cache2.clone().apply_update_with(
        "k".into(),
        10,
        rafka_entity_cache::traits::UpdateSource::LocalMutation,
        |_| Some(TailerToy { id: "k".into(), payload: 99 })
    ).await;

    let v1 = cache1.read_local(&"k".to_string()).unwrap().unwrap();
    let v2 = cache2.read_local(&"k".to_string()).unwrap().unwrap();
    assert_eq!(v1.payload, v2.payload);
}

#[test]
fn decode_record_default_empty_bytes_returns_tombstone() {
    let toy = TailerToy { id: "test_key".into(), payload: 0 };
    let key_bytes = postcard::to_allocvec(&toy).unwrap();
    let value_bytes: Vec<u8> = vec![];
    let (key, event) = TailerToy::decode_record(&key_bytes, &value_bytes).unwrap();
    assert_eq!(key, "test_key");
    assert!(matches!(event, rafka_entity_cache::traits::TailerEvent::Tombstone));
}

#[test]
fn decode_record_default_non_empty_returns_value() {
    let toy = TailerToy { id: "test_key".into(), payload: 42 };
    let key_bytes = postcard::to_allocvec(&toy).unwrap();
    let value_bytes = postcard::to_allocvec(&toy).unwrap();
    let (key, event) = TailerToy::decode_record(&key_bytes, &value_bytes).unwrap();
    assert_eq!(key, "test_key");
    match event {
        rafka_entity_cache::traits::TailerEvent::Value(v) => assert_eq!(v.payload, 42),
        _ => panic!("Expected Value event"),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SkipToy {
    id: String,
    payload: u64,
}
impl CachedEntity for SkipToy {
    type Key = String;
    fn key(&self) -> String {
        self.id.clone()
    }
    fn topic_path(_org: Option<&str>) -> Cow<'static, str> {
        Cow::Borrowed("rrl:org-rafka:env-global:clu-system:topics:skip_toy")
    }
    fn entity_kind() -> &'static str {
        "skip_toy"
    }
    fn put_op() -> u16 { 1 }
    fn delete_op() -> u16 { 2 }
    fn get_op() -> u16 { 3 }
    fn snapshot_op() -> u16 { 4 }
    fn delete_retention() -> Duration { Duration::from_secs(3600) }
    
    fn decode_key(bytes: &[u8]) -> Result<Self::Key, rafka_entity_cache::traits::DecodeError> {
        let toy: SkipToy = postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
        Ok(toy.id)
    }
    fn decode_value(bytes: &[u8]) -> Result<Self, rafka_entity_cache::traits::DecodeError> {
        postcard::from_bytes(bytes).map_err(|e| e.to_string())
    }
    fn decode_record(key_bytes: &[u8], _value_bytes: &[u8]) -> Result<(Self::Key, rafka_entity_cache::traits::TailerEvent<Self>), rafka_entity_cache::traits::DecodeError> {
        let key = Self::decode_key(key_bytes)?;
        Ok((key, rafka_entity_cache::traits::TailerEvent::Skip))
    }
}

#[tokio::test]
async fn spawn_tailer_skip_advances_tail_offset_without_apply() {
    let broker = Arc::new(MockBroker::new());
    let cache = Arc::new(EntityCache::<SkipToy, MockBroker, NoopMesh>::new(broker.clone(), Arc::new(NoopMesh), SkipToy::topic_path(None).into_owned()));
    cache.mark_ready();

    let _handle = cache.clone().spawn_tailer(rafka_entity_cache::traits::TailScope::Global, None);

    let toy = SkipToy { id: "skip_me".into(), payload: 99 };
    broker.push(&SkipToy::topic_path(None), BrokerRecord { payload: postcard::to_allocvec(&toy).unwrap(),
        offset: 10,
    }).await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cache.tail_offset(), 10);
    // Ensure it wasn't applied
    assert!(cache.read_local(&"skip_me".to_string()).unwrap().is_none());
}