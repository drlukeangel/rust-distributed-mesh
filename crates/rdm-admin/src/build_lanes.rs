//! The Build lanes on the fabric's store root (Q28 A, Q35 x; i144 e6.1). A lane is the facts one seat
//! writes: `fabric/builds/` the fabric seat's, `meshes/<mesh>/builds/` that mesh's primary seat's
//! (R-BI6, R14). Each fact is its own key, `rev-<R>/<seq>.json`: `R` the lane's own seat revision its
//! writer holds, `seq` its place under that revision, both 20-digit zero-padded so a listing sorts in
//! order. One Build fact per key (`build-fact/1`), inserted through `insert_claimed` at the
//! revision's next sequence: two writers never both take one key, and nothing is checked and then
//! appended under a process lock. A reader reconstructs the lane from its keys in order.

use crate::build_state::{rev_dir, seq_key, BuildFact, LaneId, LaneKey, REV_PREFIX};
use crate::record_store::FileRecords;
use crate::storage_error::StorageError;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// The format of one Build fact at its lane key.
pub const FACT_FORMAT: &str = "build-fact/1";
const FORMATS: [&str; 1] = [FACT_FORMAT];

/// `lane`'s directory on the fabric's store root.
pub fn lane_dir(lane: &LaneId, root: &crate::store_root::StoreRoot) -> PathBuf {
    match lane {
        LaneId::Fabric => root.fabric_builds(),
        LaneId::Mesh(m) => root.mesh_builds(m),
    }
}

/// What an append to a lane came to.
#[derive(Debug, Clone, PartialEq)]
pub enum Appended {
    /// The fact stands at its key.
    At(LaneKey),
    /// Another writer's fact took the key first: nothing was written. The caller reads the lane
    /// again and decides against what it holds now.
    Taken(LaneKey),
}

/// One revision of a lane as this process has read it.
#[derive(Clone)]
struct Rev {
    store: FileRecords,
    /// The next sequence not yet read.
    next: u64,
}

/// A read of one lane past each revision's cursor, not yet taken ([`Lanes::peek_new`], [`Lanes::take`]).
pub struct LaneRead {
    lane: LaneId,
    revs: Vec<RevRead>,
}

/// What a [`LaneRead`] found in one revision: from the cursor it began at to the next unread sequence.
struct RevRead {
    revision: u64,
    from: u64,
    next: u64,
    facts: Vec<(LaneKey, BuildFact)>,
}

/// The Build lanes of one fabric's store root, as one process reads and writes them.
pub struct Lanes {
    root: crate::store_root::StoreRoot,
    writer: crate::model::IncarnationId,
    revs: std::sync::Mutex<BTreeMap<(LaneId, u64), Rev>>,
}

impl std::fmt::Debug for Lanes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lanes").field("root", &self.root.dir()).finish()
    }
}

impl Lanes {
    /// The lanes on `root`, written by the process born as `writer`.
    pub fn new(root: crate::store_root::StoreRoot, writer: crate::model::IncarnationId) -> Self {
        Self { root, writer, revs: std::sync::Mutex::new(BTreeMap::new()) }
    }

    fn held(&self, lane: &LaneId, revision: u64) -> Option<Rev> {
        self.revs.lock().unwrap_or_else(|e| e.into_inner()).get(&(lane.clone(), revision)).cloned()
    }

    fn keep(&self, lane: &LaneId, revision: u64, rev: Rev) {
        self.revs.lock().unwrap_or_else(|e| e.into_inner()).insert((lane.clone(), revision), rev);
    }

    /// `revision` of `lane`, opened on its first use.
    async fn rev(&self, lane: &LaneId, revision: u64) -> Result<Rev, StorageError> {
        if let Some(r) = self.held(lane, revision) {
            return Ok(r);
        }
        // A Build fact is durable on the root when it is committed, before the fabric's first
        // state-commit too (Q61, build.md §5.2): a lane is never held in the scratchpad.
        let store = FileRecords::open(&lane_dir(lane, &self.root), &rev_dir(revision), &self.writer).await?;
        let opened = Rev { store, next: 1 };
        let mut revs = self.revs.lock().unwrap_or_else(|e| e.into_inner());
        Ok(revs.entry((lane.clone(), revision)).or_insert(opened).clone())
    }

    /// The revisions of `lane` on the store root, ascending: the names of its revision directories.
    async fn revisions(&self, lane: &LaneId) -> Result<Vec<u64>, StorageError> {
        let dir = lane_dir(lane, &self.root);
        let opened: Vec<u64> = self.revs.lock().unwrap_or_else(|e| e.into_inner()).keys().filter(|(l, _)| l == lane).map(|(_, r)| *r).collect();
        let mut found = crate::blocking::blocking_io(move || -> Result<Vec<u64>, StorageError> {
            let entries = match std::fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(e) => return Err(StorageError::Io { file: dir.display().to_string(), reason: e.to_string() }),
            };
            let mut revs = Vec::new();
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if let Some(r) = name.strip_prefix(REV_PREFIX).filter(|r| r.len() == 20).and_then(|r| r.parse::<u64>().ok()) {
                    revs.push(r);
                }
            }
            Ok(revs)
        })
        .await?;
        found.extend(opened);
        found.sort_unstable();
        found.dedup();
        Ok(found)
    }

    /// The fact at `key`, `None` when none was written there.
    pub async fn entry(&self, key: &LaneKey) -> Result<Option<BuildFact>, StorageError> {
        let rev = self.rev(&key.lane, key.revision).await?;
        read_entry(&rev.store, key.seq).await
    }

    /// Every fact of `lane` not read before, in order: each revision ascending, from its next unread
    /// sequence to its first unwritten one. [`Self::peek_new`], then [`Self::take`].
    pub async fn read_new(&self, lane: &LaneId) -> Result<Vec<(LaneKey, BuildFact)>, StorageError> {
        let read = self.peek_new(lane).await?;
        Ok(self.take(read))
    }

    /// What [`Self::read_new`] would answer, read from the disk without moving any revision's cursor:
    /// a read that holds no lock, taken (or not) by [`Self::take`].
    pub async fn peek_new(&self, lane: &LaneId) -> Result<LaneRead, StorageError> {
        let mut revs = Vec::new();
        for revision in self.revisions(lane).await? {
            let rev = self.rev(lane, revision).await?;
            let (from, mut next, mut facts) = (rev.next, rev.next, Vec::new());
            // The revision's keys are read in runs, one blocking call each, to its first unwritten key.
            loop {
                let keys: Vec<String> = (next..next + RUN_KEYS).map(seq_key).collect();
                let run = rev.store.read_tagged_run(keys, &FORMATS).await?;
                let whole = run.len() as u64 == RUN_KEYS;
                for (format, record) in run {
                    facts.push((LaneKey { lane: lane.clone(), revision, seq: next }, fact_of(&rev.store, next, &format, record)?));
                    next += 1;
                }
                if !whole {
                    break;
                }
            }
            revs.push(RevRead { revision, from, next, facts });
        }
        Ok(LaneRead { lane: lane.clone(), revs })
    }

    /// Take `read`: each revision whose cursor is still where the read began moves past what the read
    /// found, and its facts are answered, in order. A revision another take moved meanwhile answers
    /// nothing of this read: that take answered its facts, and the next read begins where it left off.
    pub fn take(&self, read: LaneRead) -> Vec<(LaneKey, BuildFact)> {
        let mut out = Vec::new();
        let mut revs = self.revs.lock().unwrap_or_else(|e| e.into_inner());
        for r in read.revs {
            let Some(rev) = revs.get_mut(&(read.lane.clone(), r.revision)) else { continue };
            if rev.next != r.from {
                continue;
            }
            rev.next = r.next;
            out.extend(r.facts);
        }
        out
    }

    /// The key `revision` of `lane` takes next, after [`Self::read_new`] of the lane.
    pub async fn next_key(&self, lane: &LaneId, revision: u64) -> Result<LaneKey, StorageError> {
        let rev = self.rev(lane, revision).await?;
        Ok(LaneKey { lane: lane.clone(), revision, seq: rev.next })
    }

    /// Insert `fact` at `revision`'s next sequence of `lane`, after [`Self::read_new`] of the lane:
    /// `At` its key; `Taken` when another writer's fact took the key first (nothing written).
    pub async fn append(&self, lane: &LaneId, revision: u64, fact: &BuildFact) -> Result<Appended, StorageError> {
        let mut rev = self.rev(lane, revision).await?;
        let key = LaneKey { lane: lane.clone(), revision, seq: rev.next };
        let at = key.to_string();
        if rev.store.insert_claimed(&seq_key(rev.next), FACT_FORMAT, fact, move |standing: Option<&BuildFact>| match standing {
            None => Ok(true),
            Some(_) => Err(format!("{at} holds another writer's fact")),
        }).await? {
            rev.next += 1;
            self.keep(lane, revision, rev);
            return Ok(Appended::At(key));
        }
        match read_entry(&rev.store, rev.next).await? {
            Some(_) => Ok(Appended::Taken(key)),
            None => Err(StorageError::Io { file: format!("{}/{key}.json", self.root.dir().display()), reason: "the lane key was live at the insert and holds nothing at the read after it: nothing removes a lane key".into() }),
        }
    }
}

async fn read_entry(store: &FileRecords, seq: u64) -> Result<Option<BuildFact>, StorageError> {
    let Some((format, record)) = store.read_tagged(&seq_key(seq), &FORMATS).await? else { return Ok(None) };
    fact_of(store, seq, &format, record).map(Some)
}

/// The fact `record`, in `format`, at `seq` of `store`'s revision.
fn fact_of(store: &FileRecords, seq: u64, format: &str, record: serde_json::Value) -> Result<BuildFact, StorageError> {
    serde_json::from_value(record).map_err(|e| StorageError::Unrecognised { file: format!("{}/{}.json", store.store_name(), seq_key(seq)), reason: format!("{format}: {e}") })
}

/// The keys a lane read asks the store for in one blocking call: a revision's run of facts is read
/// in calls of this many keys, to its first unwritten one.
const RUN_KEYS: u64 = 256;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::{BuildAttemptReceipt, AttemptOutcome};

    fn fact(n: u32) -> BuildFact {
        BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), attempt: n, outcome: AttemptOutcome::Converged })
    }

    fn by() -> rdm_core::handover::HandoverBirth {
        rdm_core::handover::HandoverBirth { mesh: "mesh2".into(), node_id: rdm_core::NodeId::parse("200000000000").unwrap(), incarnation: rdm_core::IncarnationId::parse(&rdm_core::fixture_crockford60("n")).unwrap() }
    }

    fn lanes(d: &std::path::Path) -> Lanes {
        Lanes::new(crate::store_root::StoreRoot::at(d), crate::model::IncarnationId::mint())
    }

    /// CONTRACT (Q28 A: "a mesh-audit ratchet for the store layout"; Q35 x: the keys and rows): the
    /// store root's layout, byte for byte: a Build fact at `fabric/builds/rev-<R>/<seq>.json`
    /// (`build-fact/1`), a seat move at `seat-moves/<T>--<node.id>--<incarnation>.json` (`seat-move/1`) and a mesh's in-flight row naming its claim's lane
    /// key (`mesh-in-flight/2`). A layout that moves moves this cell with it (`durable format moved`).
    #[tokio::test]
    async fn the_store_layout_is_frozen_byte_for_byte() {
        let d = crate::test_support::tempdir("store-layout");
        let root = crate::store_root::StoreRoot::at(&d);
        let l = lanes(&d);
        assert_eq!(l.append(&LaneId::Fabric, 7, &fact(1)).await.unwrap(), Appended::At(LaneKey { lane: LaneId::Fabric, revision: 7, seq: 1 }));
        let read = |rel: &str| String::from_utf8(std::fs::read(d.join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))).unwrap();
        assert_eq!(
            read("fabric/builds/rev-00000000000000000007/00000000000000000001.json"),
            format!(r#"{{"format":"build-fact/1","record":{{"fact":"attempt","build_id":"{}","attempt":1,"outcome":"converged"}}}}"#, rdm_core::fixture_crockford60("b1"))
        );
        let storage = crate::fabric_storage::FileFabricStorage::open(&root, &crate::model::IncarnationId::mint()).await.unwrap();
        let moved = crate::fabric_storage::SeatMoveRow {
            seats: vec![crate::fabric_storage::MovedSeat { seat: rdm_core::seat::transfer::Seat::FabricPrimary, from_revision: 0, revision: 1, holder: Some(by()), from: None }],
            cause: crate::fabric_storage::MoveCause::Election,
            by: by(),
        };
        use crate::fabric_storage::FabricStorage as _;
        assert!(matches!(crate::fabric_storage::commit_seat_move(&storage, &moved).await.unwrap(), crate::fabric_storage::MoveCommit::Committed(crate::fabric_storage::SeatMove { t: 1, .. })));
        let birth = format!(r#"{{"mesh":"mesh2","node_id":"200000000000","incarnation":"{}"}}"#, rdm_core::fixture_crockford60("n"));
        assert_eq!(
            read(&format!("seat-moves/00000000000000000001--200000000000--{}.json", rdm_core::fixture_crockford60("n"))),
            format!(r#"{{"format":"seat-move/1","record":{{"seats":[{{"seat":"FabricPrimary","from_revision":0,"revision":1,"holder":{birth},"from":null}}],"cause":"election","by":{birth}}}}}"#)
        );
        use crate::storage::MeshStorage as _;
        let claims = crate::storage::FileMeshStorage::open(&root, &crate::model::IncarnationId::mint()).await.unwrap();
        let row = crate::mesh_claims::InFlight { build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), lane: None, attempt: 1, claimant: "mesh1.admin.1".into(), lane_key: Some("fabric/builds/rev-00000000000000000007/00000000000000000002".into()) };
        assert_eq!(claims.take_in_flight("mesh2", &row).await.unwrap(), crate::mesh_claims::Taken::Won);
        assert_eq!(
            read("meshes/mesh2/claims/in-flight.json"),
            format!(r#"{{"format":"mesh-in-flight/2","record":{{"build_id":"{}","attempt":1,"claimant":"mesh1.admin.1","lane_key":"fabric/builds/rev-00000000000000000007/00000000000000000002"}}}}"#, rdm_core::fixture_crockford60("b1"))
        );
    }

    /// CONTRACT (Q28 A cells: "two processes appending to one lane race on the same seq key and exactly
    /// one commits"): two writers on one store root insert a fact at the same next key of one revision
    /// at once: exactly one stands there, the other is told `Taken`, writes nothing, and reads the
    /// winner's fact before its own lands at the next key. Must NOT happen: two facts at one key, or a
    /// fact overwritten.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_writers_racing_on_one_lane_key_commit_exactly_one() {
        for round in 0..16 {
            let d = crate::test_support::tempdir("lane-race");
            let (a, b) = (std::sync::Arc::new(lanes(&d)), std::sync::Arc::new(lanes(&d)));
            let (ra, rb) = tokio::join!(
                tokio::spawn({ let a = a.clone(); async move { a.append(&LaneId::Fabric, 3, &fact(1)).await.unwrap() } }),
                tokio::spawn({ let b = b.clone(); async move { b.append(&LaneId::Fabric, 3, &fact(2)).await.unwrap() } }),
            );
            let (ra, rb) = (ra.unwrap(), rb.unwrap());
            let key = LaneKey { lane: LaneId::Fabric, revision: 3, seq: 1 };
            let (winner, loser) = match (&ra, &rb) {
                (Appended::At(k), Appended::Taken(t)) if *k == key && *t == key => (a.clone(), (b.clone(), fact(2))),
                (Appended::Taken(t), Appended::At(k)) if *k == key && *t == key => (b.clone(), (a.clone(), fact(1))),
                other => panic!("round {round}: exactly one writer stands at {key}: {other:?}"),
            };
            let _ = winner;
            let (loser, its) = loser;
            let read = loser.read_new(&LaneId::Fabric).await.unwrap();
            assert_eq!(read.len(), 1, "round {round}: the loser reads the winner's fact");
            assert_eq!(loser.append(&LaneId::Fabric, 3, &its).await.unwrap(), Appended::At(LaneKey { seq: 2, ..key.clone() }), "round {round}: then its own lands at the next key");
        }
    }

    /// CONTRACT (the Build journal's read path; i144 e6, `fabric_primary.rs:204`): a lane read from the
    /// disk (`peek_new`) moves no cursor; its take moves each revision's cursor only from where the read
    /// began. Two reads of the same facts: the first taken answers them, the second answers nothing,
    /// and a read after both begins past them. Must NOT happen: a fact answered twice, or one skipped.
    #[tokio::test]
    async fn a_lane_read_moves_no_cursor_until_taken_and_is_taken_once_from_where_it_began() {
        let d = crate::test_support::tempdir("lane-read-take");
        let (writer, reader) = (lanes(&d), lanes(&d));
        let lane = LaneId::Fabric;
        assert!(matches!(writer.append(&lane, 3, &fact(1)).await.unwrap(), Appended::At(_)));
        assert!(matches!(writer.append(&lane, 3, &fact(2)).await.unwrap(), Appended::At(_)));
        let attempts = |read: &[(LaneKey, BuildFact)]| read.iter().map(|(_, f)| match f { BuildFact::Attempt(a) => a.attempt, _ => 0 }).collect::<Vec<_>>();
        // Two reads of the same facts, neither taken yet: a read moves no cursor.
        let (first, second) = (reader.peek_new(&lane).await.unwrap(), reader.peek_new(&lane).await.unwrap());
        assert_eq!(attempts(&reader.take(first)), vec![1, 2], "the first read taken answers what it found");
        assert!(reader.take(second).is_empty(), "the second began where the first's take moved from: it answers nothing");
        // A read after both begins past them: nothing answered twice, nothing skipped.
        assert!(matches!(writer.append(&lane, 3, &fact(3)).await.unwrap(), Appended::At(_)));
        assert_eq!(attempts(&reader.read_new(&lane).await.unwrap()), vec![3]);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (the Build journal's read path; i144 e6, `fabric_primary.rs:204`): a read of a lane
    /// costs the blocking pool a call per revision, never a call per fact: a reader catching up on 40
    /// facts another writer appended to one revision lists the revisions, opens the revision's store on
    /// its first read and reads the revision's run of facts in one call; catching up on 40 more, the
    /// store already open, it lists and reads the run. Must NOT happen: a blocking call per fact read.
    #[tokio::test]
    async fn a_lane_read_catching_up_on_forty_facts_makes_one_blocking_call_for_their_revision() {
        let d = crate::test_support::tempdir("lane-read-run");
        let (writer, reader) = (lanes(&d), lanes(&d));
        let lane = LaneId::Fabric;
        let mut appended = 0;
        for round in [3u64, 2] {
            for _ in 0..40 {
                appended += 1;
                assert!(matches!(writer.append(&lane, 3, &fact(appended)).await.unwrap(), Appended::At(_)));
            }
            let io = crate::blocking::IoTime::default();
            let read = crate::blocking::io_time_with(io.clone(), reader.read_new(&lane)).await.unwrap();
            assert_eq!(read.len(), 40, "every fact appended since the last read is read");
            assert_eq!(io.calls(), round, "a read of one revision's run of 40 facts: {} blocking calls", io.calls());
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (Q35 x cells: "the lane reconstructed in order from its keys"): a writer appends under
    /// revision 3, a successor under revision 4; a reader opened afterwards reconstructs the lane in
    /// key order, revision 3 then revision 4. A key not in the lane's shape is refused by name. Must
    /// NOT happen: the lane read out of order.
    #[tokio::test]
    async fn the_lane_is_read_in_key_order_each_revision_ascending() {
        let d = crate::test_support::tempdir("lane-order");
        let (old, successor) = (lanes(&d), lanes(&d));
        let lane = LaneId::Mesh("mesh1".into());
        for n in 1..=3 {
            assert!(matches!(old.append(&lane, 3, &fact(n)).await.unwrap(), Appended::At(_)));
        }
        assert!(matches!(successor.append(&lane, 4, &fact(6)).await.unwrap(), Appended::At(LaneKey { revision: 4, seq: 1, .. })));
        let reader = lanes(&d);
        let read: Vec<(String, u32)> = reader.read_new(&lane).await.unwrap().into_iter().map(|(k, f)| (k.to_string(), match f { BuildFact::Attempt(a) => a.attempt, _ => 0 })).collect();
        assert_eq!(
            read,
            vec![
                ("meshes/mesh1/builds/rev-00000000000000000003/00000000000000000001".to_string(), 1),
                ("meshes/mesh1/builds/rev-00000000000000000003/00000000000000000002".to_string(), 2),
                ("meshes/mesh1/builds/rev-00000000000000000003/00000000000000000003".to_string(), 3),
                ("meshes/mesh1/builds/rev-00000000000000000004/00000000000000000001".to_string(), 6),
            ],
            "the lane in key order"
        );
        assert_eq!("meshes/mesh1/builds/rev-00000000000000000003/00000000000000000004".parse::<LaneKey>().unwrap(), LaneKey { lane, revision: 3, seq: 4 });
        assert!("meshes/mesh1/builds/rev-3/4".parse::<LaneKey>().is_err(), "a key not in the lane's shape is refused by name");
    }

    /// CONTRACT (i146 Q61 A; build.md §5.2: "A recovery-critical Build fact (the accepted Build, an
    /// attempt claim, a step receipt, a completion) is not committed until it is durable in storage that
    /// survives the loss of the current fabric-primary"): before the fabric's first state-commit, each
    /// process opening the root with its own scratchpad unpublished, a Build fact appended to a lane
    /// stands on the root when its append returns and another process reads it there; a mesh's attempt
    /// claim stands on the root when it is taken; and neither scratchpad's publication writes a lane
    /// key or a claim. Must NOT happen: a lane fact or an attempt claim held in a process's scratchpad.
    #[tokio::test]
    async fn before_the_first_state_commit_a_lane_fact_and_a_claim_stand_on_the_root() {
        let d = crate::test_support::tempdir("lane-before-commit");
        let (pad_old, pad_new) = (crate::record_store::Scratchpad::new(), crate::record_store::Scratchpad::new());
        let held = |pad: &std::sync::Arc<crate::record_store::Scratchpad>| crate::store_root::StoreRoot::held_at(&d, pad.clone());
        let (old, successor) = (Lanes::new(held(&pad_old), crate::model::IncarnationId::mint()), Lanes::new(held(&pad_new), crate::model::IncarnationId::mint()));
        let lane = LaneId::Fabric;
        assert_eq!(old.append(&lane, 3, &fact(1)).await.unwrap(), Appended::At(LaneKey { lane: lane.clone(), revision: 3, seq: 1 }));
        assert!(d.join("fabric/builds/rev-00000000000000000003/00000000000000000001.json").exists(), "the fact stands on the root when its append returns");
        assert_eq!(successor.read_new(&lane).await.unwrap().len(), 1, "another process reads it on the root");
        use crate::storage::MeshStorage as _;
        let claims = crate::storage::FileMeshStorage::open(&held(&pad_new), &crate::model::IncarnationId::mint()).await.unwrap();
        let row = crate::mesh_claims::InFlight { build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), lane: None, attempt: 1, claimant: "mesh1.admin.1".into(), lane_key: None };
        assert_eq!(claims.take_in_flight("mesh2", &row).await.unwrap(), crate::mesh_claims::Taken::Won);
        assert!(d.join("meshes/mesh2/claims/in-flight.json").exists(), "the attempt claim stands on the root when it is taken");
        for pad in [pad_old, pad_new] {
            let published = pad.publish().await.unwrap();
            assert!(published.stores.iter().all(|s| !s.store.contains("builds") && !s.store.contains("claims")), "no lane key or claim is held for a publication: {published:?}");
        }
        let _ = std::fs::remove_dir_all(&d);
    }
}
