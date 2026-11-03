//! A node-admin's Build floor read from the Build storage (Q292; i143 R-G6): the Ready check names a
//! typed blocker for the Build it misses, and its `FetchBuild` reads the Build's committed records from
//! the storage the fabric-primary writes, never a copy a peer serves, absorbs them into the admin's own
//! log, and makes the read again after a failure, spaced by its backoff.

use rdm_admin::accepted::FabricTopology;
use rdm_admin::admin::node_admin_not_ready;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildFact, BuildStateAdapter, BuildStateError, FactDerivation, LocalBuildLog, MemoryBuildStateAdapter};
use rdm_admin::fetch_build::{FetchBuild, NodeAdminNotReady};
use rdm_admin::model::PathName;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A node-admin's Build state: its own log beside the fabric's Build storage (whose newest Build in
/// progress or completed is its current Build, R-BI3), and its entry floor (the Build the entry named).
struct Admin {
    name: PathName,
    log: Arc<MemoryBuildStateAdapter>,
    floor: Mutex<Option<BuildId>>,
}

fn admin(name: PathName, storage: Arc<dyn LocalBuildLog>) -> Admin {
    Admin { name, log: Arc::new(MemoryBuildStateAdapter::new().persisted_in(storage)), floor: Mutex::new(None) }
}

/// The Build storage holds Build `id`, complete: the store's current Build.
async fn stored(store: &MemoryBuildStateAdapter, id: &BuildId) {
    let fp = rdm_core::framing::Birth { node_id: rdm_core::NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() };
    store.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 }).await.unwrap();
    crate::end_build(store, id, &fp, rdm_admin::events_storage::EndedState::Complete).await;
}

/// Drive the Ready check's turn until it finds no blocker, recording each distinct blocker kind.
async fn drive(a: &Admin, h: &mut FetchBuild, within: Duration) -> Result<Vec<&'static str>, Vec<&'static str>> {
    let until = Instant::now() + within;
    let mut seen: Vec<&'static str> = Vec::new();
    loop {
        let floor = a.floor.lock().unwrap().clone();
        let blocker = node_admin_not_ready(&a.name, &*a.log, floor).await;
        if let Some(b) = &blocker {
            if seen.last() != Some(&b.kind()) {
                seen.push(b.kind());
            }
        }
        h.tick(blocker.as_ref()).await;
        if blocker.is_none() {
            return Ok(seen);
        }
        if Instant::now() > until {
            return Err(seen);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn read_of(a: &Admin) -> FetchBuild {
    FetchBuild::new(a.name.clone(), a.log.clone() as Arc<dyn LocalBuildLog>)
}

/// CONTRACT (Q292; node-start.md step 4): a joiner holds the Build its entry reply named and none of its
/// records, so it holds no current Build. The Ready check names the typed blocker (the named Build
/// wanted); the floor reads that Build's committed records from the Build storage into the joiner's
/// own log, the Build is current and the blocker clears.
#[tokio::test]
async fn a_build_the_entry_named_is_held_current_once_its_committed_records_are_read() {
    let store = Arc::new(MemoryBuildStateAdapter::new());
    let id = BuildId::mint();
    stored(&store, &id).await;
    let joiner = admin(PathName::new("mesh1.node-admin", "2").unwrap(), store.clone());
    *joiner.floor.lock().unwrap() = Some(id.clone());
    let b = node_admin_not_ready(&joiner.name, &*joiner.log, joiner.floor.lock().unwrap().clone()).await.unwrap();
    assert!(matches!(&b, NodeAdminNotReady::NoCurrentBuild { wanted: Some(w), .. } if *w == id), "{b:?}");
    let mut h = read_of(&joiner);
    let seen = drive(&joiner, &mut h, Duration::from_secs(10)).await.expect("reads the Build");
    assert_eq!(seen, vec!["no-current-build-wanted"], "the one blocker the joiner named");
    assert_eq!(joiner.log.current_build().await.unwrap().into_build_id(), Some(id.clone()), "the read Build is current");
}

/// CONTRACT (states.md "The check-in checklist"; Q292): a primary taking its role reads the storage's
/// current Build before it counts its fresh checklist, the start's Build floor read again, owed until
/// read. Its answer is taken once (`absorbed`), and the primary's own log then holds the current Build.
#[tokio::test]
async fn a_primary_taking_its_role_reads_the_storages_current_build_once() {
    let store = Arc::new(MemoryBuildStateAdapter::new());
    let id = BuildId::mint();
    stored(&store, &id).await;
    let taking = admin(PathName::new("mesh2.node-admin", "1").unwrap(), store.clone());
    assert!(taking.log.facts().await.unwrap().is_empty(), "the primary holds no Build record before the read");
    let mut h = read_of(&taking);
    assert!(!h.absorbed().await, "nothing is taken before a read is made");
    let read = NodeAdminNotReady::NoCurrentBuild { me_path: taking.name.path.clone(), me_name: taking.name.name.clone(), wanted: None };
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if h.absorbed().await {
            break;
        }
        assert!(Instant::now() < until, "the read was made within 10 s");
        h.tick(Some(&read)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!h.absorbed().await, "the answer is taken once");
    assert_eq!(taking.log.current_build().await.unwrap().into_build_id(), Some(id), "the primary's log holds the storage's current Build");
}

/// CONTRACT (node-start.md step 4; Q292): a restarted admin whose own log lost the Build's records reads
/// the Build the entry named from the storage; the blocker names it.
#[tokio::test]
async fn a_lost_build_is_read_from_the_storage_by_the_entry_floor() {
    let store = Arc::new(MemoryBuildStateAdapter::new());
    let id = BuildId::mint();
    stored(&store, &id).await;
    let restarted = admin(PathName::new("mesh1.node-admin", "3").unwrap(), store.clone());
    *restarted.floor.lock().unwrap() = Some(id.clone());
    let mut h = read_of(&restarted);
    let seen = drive(&restarted, &mut h, Duration::from_secs(10)).await.expect("reads the lost Build");
    assert_eq!(seen, vec!["no-current-build-wanted"], "the blocker named the Build the entry named");
    assert_eq!(restarted.log.current_build().await.unwrap().into_build_id(), Some(id));
}

/// A Build storage that cannot be read until it is let: every committed read fails, named, never
/// answered empty.
struct Unreadable {
    inner: Arc<MemoryBuildStateAdapter>,
    open: AtomicBool,
}

#[async_trait::async_trait]
impl BuildStateAdapter for Unreadable {
    async fn publish_accepted(&self, a: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(a).await
    }
    async fn read_build(&self, id: &BuildId) -> Result<rdm_admin::build_state::BuildProjection, BuildStateError> {
        self.inner.read_build(id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
    async fn accepted_newest_first(&self) -> Result<Vec<rdm_admin::build::BuildId>, rdm_admin::build_state::BuildStateError> {
        self.inner.accepted_newest_first().await
    }
    fn events(&self) -> std::sync::Arc<dyn rdm_admin::events_storage::EventsStorage> {
        self.inner.events()
    }
}

#[async_trait::async_trait]
impl LocalBuildLog for Unreadable {
    async fn absorb_facts(&self, facts: &[BuildFact]) {
        self.inner.absorb_facts(facts).await
    }
    async fn derive_on_every_fact(&self, derive: FactDerivation) {
        self.inner.derive_on_every_fact(derive).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
        if !self.open.load(Ordering::SeqCst) {
            return Err(BuildStateError::Io("the Build storage cannot be read: the cell holds it shut".into()));
        }
        self.inner.committed_facts(build).await
    }
}

/// CONTRACT (Q292: "an unreadable store is an error, never an empty Build"; R-G6's backoff): while the
/// Build storage cannot be read, the joiner stays blocked, absorbs nothing and reads again; once it can
/// be read, the next read clears the blocker.
#[tokio::test]
async fn an_unreadable_storage_leaves_the_floor_blocked_and_is_read_again() {
    let inner = Arc::new(MemoryBuildStateAdapter::new());
    let id = BuildId::mint();
    stored(&inner, &id).await;
    let store = Arc::new(Unreadable { inner, open: AtomicBool::new(false) });
    let joiner = admin(PathName::new("mesh1.node-admin", "2").unwrap(), store.clone());
    *joiner.floor.lock().unwrap() = Some(id.clone());
    let mut h = read_of(&joiner);
    assert!(drive(&joiner, &mut h, Duration::from_millis(600)).await.is_err(), "blocked while the storage cannot be read");
    assert!(joiner.log.facts().await.unwrap().is_empty(), "nothing is absorbed from a failed read");
    store.open.store(true, Ordering::SeqCst);
    drive(&joiner, &mut h, Duration::from_secs(10)).await.expect("the next read, past its backoff, clears the blocker");
    assert_eq!(joiner.log.current_build().await.unwrap().into_build_id(), Some(id));
}
