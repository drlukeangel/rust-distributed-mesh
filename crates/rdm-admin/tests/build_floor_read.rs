//! A node-admin's Build floor read from the Build storage (Q292; i143 R-G6): the Ready check names a
//! typed blocker for the control facts it misses, and its `FetchBuild` reads the Build's committed
//! records from the storage the fabric-primary writes, never a copy a peer serves, absorbs them into
//! the admin's own log, and makes the read again after a failure, spaced by its backoff.

use rdm_admin::accepted::FabricTopology;
use rdm_admin::admin::node_admin_not_ready;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{AttemptOpened, AttemptOutcome, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStateAdapter, BuildStateError, BuildStepReceipt, LocalBuildLog, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::fetch_build::{FetchBuild, NodeAdminNotReady};
use rdm_admin::model::PathName;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A node-admin's Build state: its own log (whose newest Build in progress or completed is its
/// current Build, R-BI3) and its entry floor (the Build the entry named, and the attempt to reach).
struct Admin {
    name: PathName,
    log: Arc<MemoryBuildStateAdapter>,
    floor: Mutex<Option<(BuildId, u32)>>,
}

fn admin(name: &str) -> Admin {
    Admin { name: name.parse().unwrap(), log: Arc::new(MemoryBuildStateAdapter::new()), floor: Mutex::new(None) }
}

/// The Build storage holds Build `id` through attempt `attempts`, converged: the store's current Build.
async fn stored(store: &MemoryBuildStateAdapter, id: &BuildId, attempts: u32) {
    store.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
    for attempt in 1..=attempts {
        if attempt > 1 {
            store.open_attempt(&AttemptOpened { mesh: None, build_id: id.clone(), attempt, reason: AttemptReason::Stop, action: None, opened_by: "x".into(), opened_at_ms: 0, context: None }).await.unwrap();
        }
        store.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
        store.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: id.clone(), attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    }
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

fn read_of(a: &Admin, store: Arc<dyn BuildStateAdapter>) -> FetchBuild {
    FetchBuild::new(a.name.clone(), store, a.log.clone() as Arc<dyn LocalBuildLog>)
}

/// CONTRACT (Q292; node-start.md step 4): a joiner holds the Build its entry reply named and none of its
/// records, so it holds no current Build. The Ready check names the typed blocker (the named Build
/// wanted); the floor reads that Build's committed records from the Build storage into the joiner's
/// own log, the Build is current through the entry's floor and the blocker clears.
#[tokio::test]
async fn a_build_the_entry_named_is_held_current_once_its_committed_records_are_read() {
    let store = Arc::new(MemoryBuildStateAdapter::new());
    let id = BuildId::mint();
    stored(&store, &id, 2).await;
    let joiner = admin("mesh1.admin.2");
    *joiner.floor.lock().unwrap() = Some((id.clone(), 2));
    let b = node_admin_not_ready(&joiner.name, &*joiner.log, joiner.floor.lock().unwrap().clone()).await.unwrap();
    assert!(matches!(&b, NodeAdminNotReady::NoCurrentBuild { wanted: Some(w), .. } if *w == id), "{b:?}");
    let mut h = read_of(&joiner, store.clone());
    let seen = drive(&joiner, &mut h, Duration::from_secs(10)).await.expect("reads the Build");
    assert_eq!(seen, vec!["no-current-build-wanted"], "the one blocker the joiner named");
    assert_eq!(joiner.log.current_build().await.unwrap().into_build_id(), Some(id.clone()), "the read Build is current");
    assert_eq!(joiner.log.read_build(&id).await.unwrap().attempt, 2, "the joiner's own log holds the Build through the floor");
}

/// CONTRACT (Q292, V's release red at c164d693b: a 4,487-byte step receipt past one 4,032-byte Build
/// message left a re-created admin's every read incomplete): a Build whose records include a failed
/// step's reason far past any message is read whole into the joiner's log, and the joiner's floor
/// clears. Must NOT happen: a record dropped or cut for its size, or the read counted incomplete.
#[tokio::test]
async fn a_build_with_a_record_far_past_one_message_is_read_whole() {
    let store = Arc::new(MemoryBuildStateAdapter::new());
    let id = BuildId::mint();
    stored(&store, &id, 1).await;
    let reason = "x".repeat(9_000);
    let failed = BuildStepReceipt { mesh: None, build_id: id.clone(), attempt: 1, operation: "node.create:mesh2.admin.1".into(), step: "WaitForNodeReady".into(), outcome: StepOutcome::Failed { reason: reason.clone(), code: rdm_core::outcome::Code::DeadlineExceeded }, output: None, executor: None };
    store.append_step_receipt(&failed).await.unwrap();
    let joiner = admin("mesh2.admin.1");
    *joiner.floor.lock().unwrap() = Some((id.clone(), 1));
    let mut h = read_of(&joiner, store.clone());
    drive(&joiner, &mut h, Duration::from_secs(10)).await.expect("reads the Build");
    let held = joiner.log.facts().await.unwrap();
    assert!(held.iter().any(|f| matches!(f, BuildFact::Step(s) if s.outcome == failed.outcome)), "the 9,000-byte reason is held whole");
}

/// CONTRACT (states.md "The check-in checklist"; Q292): a primary taking its role reads the storage's
/// current Build before it counts its fresh checklist, the start's Build floor read again, owed until
/// read. Its answer is taken once (`absorbed`), and the primary's own log then holds the current Build.
#[tokio::test]
async fn a_primary_taking_its_role_reads_the_storages_current_build_once() {
    let store = Arc::new(MemoryBuildStateAdapter::new());
    let id = BuildId::mint();
    stored(&store, &id, 2).await;
    let taking = admin("mesh2.admin.1");
    assert_eq!(taking.log.current_build().await.unwrap().into_build_id(), None, "the primary holds no Build before the read");
    let mut h = read_of(&taking, store.clone());
    assert!(!h.absorbed().await, "nothing is taken before a read is made");
    let read = NodeAdminNotReady::NoCurrentBuild { me: taking.name.clone(), wanted: None };
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

/// CONTRACT (node-start.md step 4; Q292): a restarted admin whose own log lost the Build's records,
/// and one whose log is behind the entry's attempt floor, each read the Build from the storage up to
/// the floor; the blocker names which.
#[tokio::test]
async fn a_lost_or_behind_build_is_read_up_to_the_entry_floor() {
    let store = Arc::new(MemoryBuildStateAdapter::new());
    let id = BuildId::mint();
    stored(&store, &id, 3).await;
    // Behind: the admin's own log holds attempt 1 only.
    let behind = admin("mesh1.admin.2");
    stored(&behind.log, &id, 1).await;
    *behind.floor.lock().unwrap() = Some((id.clone(), 3));
    let mut h = read_of(&behind, store.clone());
    let seen = drive(&behind, &mut h, Duration::from_secs(10)).await.expect("reads up to the floor");
    assert_eq!(seen, vec!["behind-floor"], "the blocker named the floor");
    assert_eq!(behind.log.read_build(&id).await.unwrap().attempt, 3);
    // Lost: a restart's log holds nothing of the Build the entry named.
    let restarted = admin("mesh1.admin.3");
    *restarted.floor.lock().unwrap() = Some((id.clone(), 3));
    let mut h = read_of(&restarted, store.clone());
    drive(&restarted, &mut h, Duration::from_secs(10)).await.expect("reads the lost Build");
    assert_eq!(restarted.log.read_build(&id).await.unwrap().attempt, 3);
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
    async fn open_attempt(&self, o: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(o).await
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
    async fn list_active(&self) -> Result<Vec<rdm_admin::build_state::BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<rdm_admin::build_state::ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &BuildStepReceipt) -> Result<(), BuildStateError> {
        self.inner.append_step_receipt(r).await
    }
    async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(r).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn committed_facts(&self, build: Option<&BuildId>) -> Result<Vec<BuildFact>, BuildStateError> {
        if !self.open.load(Ordering::SeqCst) {
            return Err(BuildStateError::Io("the Build storage cannot be read (the cell's fault)".into()));
        }
        self.inner.committed_facts(build).await
    }
    async fn forget(&self, id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(id).await
    }
    async fn recovery_of(&self, path: &PathName, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
}

/// CONTRACT (Q292: "an unreadable store is an error, never an empty Build"; R-G6's backoff): while the
/// Build storage cannot be read, the joiner stays blocked, absorbs nothing and reads again; once it can
/// be read, the next read clears the blocker.
#[tokio::test]
async fn an_unreadable_storage_leaves_the_floor_blocked_and_is_read_again() {
    let inner = Arc::new(MemoryBuildStateAdapter::new());
    let id = BuildId::mint();
    stored(&inner, &id, 1).await;
    let store = Arc::new(Unreadable { inner, open: AtomicBool::new(false) });
    let joiner = admin("mesh1.admin.2");
    *joiner.floor.lock().unwrap() = Some((id.clone(), 1));
    let mut h = read_of(&joiner, store.clone());
    assert!(drive(&joiner, &mut h, Duration::from_millis(600)).await.is_err(), "blocked while the storage cannot be read");
    assert!(joiner.log.facts().await.unwrap().is_empty(), "nothing is absorbed from a failed read");
    store.open.store(true, Ordering::SeqCst);
    drive(&joiner, &mut h, Duration::from_secs(10)).await.expect("the next read, past its backoff, clears the blocker");
    assert_eq!(joiner.log.current_build().await.unwrap().into_build_id(), Some(id));
}
