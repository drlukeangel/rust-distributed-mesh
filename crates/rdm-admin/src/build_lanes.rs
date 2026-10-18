//! The Build lanes on the fabric's store root (Q28 A, Q35 x; i144 e6.1). A lane is the facts one seat
//! writes: `fabric/builds/` the fabric seat's, `meshes/<mesh>/builds/` that mesh's primary seat's
//! (R-BI6, R14). Each fact is its own key, `rev-<R>/<seq>.json`: `R` the lane's own seat revision its
//! writer holds, `seq` its place under that revision, both 20-digit zero-padded so a listing sorts in
//! order. One Build fact per key (`build-fact/1`), inserted through `insert_claimed` at the
//! revision's next sequence: two writers never both take one key, and nothing is checked and then
//! appended under a process lock.
//!
//! The move of the lane's seat seals the revision it moved from: `lane-seal/1` at the first sequence
//! the sealer wins. A writer meeting the seal is refused stale and appends nothing past it, and a
//! reader reconstructs the lane from its keys in order, each revision up to its seal, never past it.

use crate::build_state::BuildFact;
use crate::record_store::{FileRecords, PublishGate, StorageError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// The format of one Build fact at its lane key.
pub const FACT_FORMAT: &str = "build-fact/1";
/// The format of a revision's seal at its lane key.
pub const SEAL_FORMAT: &str = "lane-seal/1";
const FORMATS: [&str; 2] = [FACT_FORMAT, SEAL_FORMAT];
const REV_PREFIX: &str = "rev-";

/// A lane: the fabric seat's, or one mesh's primary seat's.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LaneId {
    /// `fabric/builds/`, written under the fabric seat's revision.
    Fabric,
    /// `meshes/<mesh>/builds/`, written under that mesh's primary seat's revision.
    Mesh(String),
}

impl LaneId {
    /// The lane's directory on the fabric's store root.
    pub fn dir(&self, root: &crate::store_root::StoreRoot) -> PathBuf {
        match self {
            Self::Fabric => root.fabric_builds(),
            Self::Mesh(m) => root.mesh_builds(m),
        }
    }

    /// The lane's directory under the store root, as a key names it.
    pub fn name(&self) -> String {
        match self {
            Self::Fabric => "fabric/builds".into(),
            Self::Mesh(m) => format!("meshes/{m}/builds"),
        }
    }
}

/// One entry's key: its lane, the seat revision it was written under, its sequence there.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LaneKey {
    /// The lane.
    pub lane: LaneId,
    /// The lane's seat revision the entry was written under.
    pub revision: u64,
    /// Its place under that revision, from 1.
    pub seq: u64,
}

impl std::fmt::Display for LaneKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.lane.name(), rev_dir(self.revision), seq_key(self.seq))
    }
}

impl std::str::FromStr for LaneKey {
    type Err = String;

    /// A key as [`LaneKey`]'s `Display` writes it; anything else is refused by name.
    fn from_str(s: &str) -> Result<Self, String> {
        let bad = || format!("{s:?} is not a lane key (fabric/builds/rev-<R>/<seq> or meshes/<mesh>/builds/rev-<R>/<seq>)");
        let parts: Vec<&str> = s.split('/').collect();
        let (lane, rev, seq) = match parts.as_slice() {
            ["fabric", "builds", rev, seq] => (LaneId::Fabric, *rev, *seq),
            ["meshes", mesh, "builds", rev, seq] if !mesh.is_empty() => (LaneId::Mesh(mesh.to_string()), *rev, *seq),
            _ => return Err(bad()),
        };
        let revision = rev.strip_prefix(REV_PREFIX).filter(|r| r.len() == 20).and_then(|r| r.parse().ok()).ok_or_else(bad)?;
        let seq = Some(seq).filter(|q| q.len() == 20).and_then(|q| q.parse().ok()).filter(|q| *q > 0).ok_or_else(bad)?;
        Ok(Self { lane, revision, seq })
    }
}

/// The seal of one revision of a lane (`lane-seal/1`): nothing of `revision` commits past it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneSeal {
    /// The revision sealed.
    pub revision: u64,
    /// The seat move (its `T` in `seat-moves/`) that moved the lane's seat from `revision`.
    pub sealed_by: u64,
    /// The exact birth that wrote the seal.
    pub by: rdm_core::handover::HandoverBirth,
}

/// One entry of a lane.
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// A Build fact.
    Fact(BuildFact),
    /// The revision's seal.
    Seal(LaneSeal),
}

/// What an append to a lane came to.
#[derive(Debug, Clone, PartialEq)]
pub enum Appended {
    /// The fact stands at its key.
    At(LaneKey),
    /// The revision is sealed: nothing was written, and nothing of it is written again.
    Sealed {
        /// The seal's key.
        at: LaneKey,
        /// The seal.
        seal: LaneSeal,
    },
    /// Another writer's fact took the key first: nothing was written. The caller reads the lane
    /// again and decides against what it holds now.
    Taken(LaneKey),
}

/// What a seal of a revision came to ([`Lanes::seal`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Sealed {
    /// The seal's key.
    pub at: LaneKey,
    /// The seal standing there.
    pub seal: LaneSeal,
    /// The facts of the revision the seal found below it and read past, in order.
    pub passed: Vec<(LaneKey, BuildFact)>,
}

fn rev_dir(revision: u64) -> String {
    format!("{REV_PREFIX}{revision:020}")
}

fn seq_key(seq: u64) -> String {
    format!("{seq:020}")
}

/// One revision of a lane as this process has read it.
#[derive(Clone)]
struct Rev {
    store: FileRecords,
    /// The next sequence not yet read.
    next: u64,
    /// The seal, once read: its sequence and record.
    sealed: Option<(u64, LaneSeal)>,
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
    sealed: Option<(u64, LaneSeal)>,
    facts: Vec<(LaneKey, BuildFact)>,
}

/// The Build lanes of one fabric's store root, as one process reads and writes them.
pub struct Lanes {
    root: crate::store_root::StoreRoot,
    writer: crate::model::IncarnationId,
    /// The gate the fabric lane's held facts are published through at the fabric's first
    /// state-commit: only while this process holds the fabric seat (R14 item 2).
    fabric_gate: std::sync::OnceLock<PublishGate>,
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
        Self { root, writer, fabric_gate: std::sync::OnceLock::new(), revs: std::sync::Mutex::new(BTreeMap::new()) }
    }

    /// The gate the fabric lane's held facts are published through (set once).
    pub fn set_fabric_gate(&self, gate: PublishGate) {
        let _ = self.fabric_gate.set(gate.clone());
        for ((lane, _), rev) in self.revs.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            if *lane == LaneId::Fabric {
                rev.store.set_publish_gate(gate.clone());
            }
        }
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
        let store = self.root.records(&lane.dir(&self.root), &rev_dir(revision), &self.writer).await?;
        if *lane == LaneId::Fabric {
            if let Some(gate) = self.fabric_gate.get() {
                store.set_publish_gate(gate.clone());
            }
        }
        let opened = Rev { store, next: 1, sealed: None };
        let mut revs = self.revs.lock().unwrap_or_else(|e| e.into_inner());
        Ok(revs.entry((lane.clone(), revision)).or_insert(opened).clone())
    }

    /// The revisions of `lane` on the store root, ascending: the names of its revision directories.
    async fn revisions(&self, lane: &LaneId) -> Result<Vec<u64>, StorageError> {
        let dir = lane.dir(&self.root);
        let opened: Vec<u64> = self.revs.lock().unwrap_or_else(|e| e.into_inner()).keys().filter(|(l, _)| l == lane).map(|(_, r)| *r).collect();
        let mut found = crate::record_store::blocking_io(move || -> Result<Vec<u64>, StorageError> {
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

    /// The entry at `key`, `None` when none was written there.
    pub async fn entry(&self, key: &LaneKey) -> Result<Option<Entry>, StorageError> {
        let rev = self.rev(&key.lane, key.revision).await?;
        read_entry(&rev.store, key.seq).await
    }

    /// Every fact of `lane` not read before, in order: each revision ascending, from its next unread
    /// sequence to its first unwritten one or its seal. [`Self::peek_new`], then [`Self::take`].
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
            if rev.sealed.is_some() {
                continue;
            }
            let (from, mut next, mut sealed, mut facts) = (rev.next, rev.next, None, Vec::new());
            while let Some(e) = read_entry(&rev.store, next).await? {
                match e {
                    Entry::Fact(f) => facts.push((LaneKey { lane: lane.clone(), revision, seq: next }, f)),
                    Entry::Seal(s) => {
                        sealed = Some((next, s));
                        break;
                    }
                }
                next += 1;
            }
            revs.push(RevRead { revision, from, next, sealed, facts });
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
            if rev.next != r.from || rev.sealed.is_some() {
                continue;
            }
            rev.next = r.next;
            rev.sealed = r.sealed;
            out.extend(r.facts);
        }
        out
    }

    /// The seal of `revision` of `lane` this process has read, with its key.
    pub fn seal_of(&self, lane: &LaneId, revision: u64) -> Option<(LaneKey, LaneSeal)> {
        self.held(lane, revision).and_then(|r| r.sealed.map(|(seq, s)| (LaneKey { lane: lane.clone(), revision, seq }, s)))
    }

    /// The key `revision` of `lane` takes next, after [`Self::read_new`] of the lane.
    pub async fn next_key(&self, lane: &LaneId, revision: u64) -> Result<LaneKey, StorageError> {
        let rev = self.rev(lane, revision).await?;
        Ok(LaneKey { lane: lane.clone(), revision, seq: rev.next })
    }

    /// Insert `fact` at `revision`'s next sequence of `lane`, after [`Self::read_new`] of the lane:
    /// `At` its key; `Sealed` when the revision's seal stands there (refused stale, nothing written);
    /// `Taken` when another writer's fact took the key first (nothing written).
    pub async fn append(&self, lane: &LaneId, revision: u64, fact: &BuildFact) -> Result<Appended, StorageError> {
        let mut rev = self.rev(lane, revision).await?;
        if let Some((seq, seal)) = rev.sealed.clone() {
            return Ok(Appended::Sealed { at: LaneKey { lane: lane.clone(), revision, seq }, seal });
        }
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
            Some(Entry::Seal(seal)) => {
                rev.sealed = Some((rev.next, seal.clone()));
                self.keep(lane, revision, rev);
                Ok(Appended::Sealed { at: key, seal })
            }
            Some(Entry::Fact(_)) => Ok(Appended::Taken(key)),
            None => Err(StorageError::Io { file: format!("{}/{key}.json", self.root.dir().display()), reason: "the lane key was live at the insert and holds nothing at the read after it: nothing removes a lane key".into() }),
        }
    }

    /// Seal `seal.revision` of `lane` at the first sequence the seal wins, after [`Self::read_new`] of
    /// the lane: the seal standing there and its key (this one, or one already standing, and then
    /// nothing is written), with every fact of the revision the seal found below it and read past,
    /// in order (the old writer's last facts: they commit).
    pub async fn seal(&self, lane: &LaneId, seal: &LaneSeal) -> Result<Sealed, StorageError> {
        let revision = seal.revision;
        let mut passed = Vec::new();
        loop {
            let mut rev = self.rev(lane, revision).await?;
            if let Some((seq, standing)) = rev.sealed.clone() {
                return Ok(Sealed { at: LaneKey { lane: lane.clone(), revision, seq }, seal: standing, passed });
            }
            let key = LaneKey { lane: lane.clone(), revision, seq: rev.next };
            let at = key.to_string();
            if rev.store.insert_claimed(&seq_key(rev.next), SEAL_FORMAT, seal, move |standing: Option<&LaneSeal>| match standing {
                None => Ok(true),
                Some(_) => Err(format!("{at} holds another seal")),
            }).await? {
                rev.sealed = Some((rev.next, seal.clone()));
                self.keep(lane, revision, rev);
                return Ok(Sealed { at: key, seal: seal.clone(), passed });
            }
            // The key holds an entry the seal did not see: the old writer's last fact (below the
            // seal, so it commits) or another sealer's seal. Read on and seal after it.
            match read_entry(&rev.store, rev.next).await? {
                Some(Entry::Seal(standing)) => {
                    rev.sealed = Some((rev.next, standing.clone()));
                    self.keep(lane, revision, rev);
                    return Ok(Sealed { at: key, seal: standing, passed });
                }
                Some(Entry::Fact(f)) => {
                    passed.push((key, f));
                    rev.next += 1;
                    self.keep(lane, revision, rev);
                }
                None => return Err(StorageError::Io { file: format!("{}/{key}.json", self.root.dir().display()), reason: "the lane key was live at the seal's insert and holds nothing at the read after it: nothing removes a lane key".into() }),
            }
        }
    }
}

async fn read_entry(store: &FileRecords, seq: u64) -> Result<Option<Entry>, StorageError> {
    let file = || format!("{}/{}.json", store.store_name(), seq_key(seq));
    let Some((format, record)) = store.read_tagged(&seq_key(seq), &FORMATS).await? else { return Ok(None) };
    let decoded = if format == FACT_FORMAT { serde_json::from_value(record).map(Entry::Fact) } else { serde_json::from_value(record).map(Entry::Seal) };
    decoded.map(Some).map_err(|e| StorageError::Unrecognised { file: file(), reason: format!("{format}: {e}") })
}

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
    /// (`build-fact/1`), a seal at a mesh lane's key (`lane-seal/1 { revision, sealed_by, by }`), a seat
    /// move at `seat-moves/<T>.json` (`seat-move/1`) and a mesh's in-flight row naming its claim's lane
    /// key (`mesh-in-flight/2`). A layout that moves moves this cell with it (`durable format moved`).
    #[tokio::test]
    async fn the_store_layout_is_frozen_byte_for_byte() {
        let d = crate::record_store::tempdir("store-layout");
        let root = crate::store_root::StoreRoot::at(&d);
        let l = lanes(&d);
        assert_eq!(l.append(&LaneId::Fabric, 7, &fact(1)).await.unwrap(), Appended::At(LaneKey { lane: LaneId::Fabric, revision: 7, seq: 1 }));
        let mesh = LaneId::Mesh("mesh1".into());
        l.seal(&mesh, &LaneSeal { revision: 3, sealed_by: 2, by: by() }).await.unwrap();
        let read = |rel: &str| String::from_utf8(std::fs::read(d.join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))).unwrap();
        assert_eq!(
            read("fabric/builds/rev-00000000000000000007/00000000000000000001.json"),
            format!(r#"{{"format":"build-fact/1","record":{{"fact":"attempt","build_id":"{}","attempt":1,"outcome":"converged"}}}}"#, rdm_core::fixture_crockford60("b1"))
        );
        assert_eq!(
            read("meshes/mesh1/builds/rev-00000000000000000003/00000000000000000001.json"),
            format!(r#"{{"format":"lane-seal/1","record":{{"revision":3,"sealed_by":2,"by":{{"mesh":"mesh2","node_id":"200000000000","incarnation":"{}"}}}}}}"#, rdm_core::fixture_crockford60("n"))
        );
        let storage = crate::fabric_storage::FileFabricStorage::open(&root, &crate::model::IncarnationId::mint()).await.unwrap();
        let moved = crate::fabric_storage::SeatMoveRow {
            seats: vec![crate::fabric_storage::MovedSeat { seat: rdm_core::seat::transfer::Seat::FabricPrimary, from_revision: 0, revision: 1, holder: Some(by()), from: None }],
            cause: crate::fabric_storage::MoveCause::Election,
            by: by(),
        };
        use crate::fabric_storage::FabricStorage as _;
        assert!(matches!(storage.commit_seat_move(&moved).await.unwrap(), crate::fabric_storage::MoveCommit::Committed(crate::fabric_storage::SeatMove { t: 1, .. })));
        let birth = format!(r#"{{"mesh":"mesh2","node_id":"200000000000","incarnation":"{}"}}"#, rdm_core::fixture_crockford60("n"));
        assert_eq!(
            read("seat-moves/00000000000000000001.json"),
            format!(r#"{{"format":"seat-move/1","record":{{"seats":[{{"seat":"FabricPrimary","from_revision":0,"revision":1,"holder":{birth},"from":null}}],"cause":"election","by":{birth}}}}}"#)
        );
        let claims = crate::mesh_claims::MeshClaims::on_root(root, crate::model::IncarnationId::mint());
        let row = crate::mesh_claims::InFlight { build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap(), lane: None, attempt: 1, claimant: "mesh1.admin.1".into(), lane_key: Some("fabric/builds/rev-00000000000000000007/00000000000000000002".into()) };
        assert_eq!(claims.take("mesh2", &row).await.unwrap(), crate::mesh_claims::Taken::Won);
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
            let d = crate::record_store::tempdir("lane-race");
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
        let d = crate::record_store::tempdir("lane-read-take");
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

    /// CONTRACT (Q28 A, Q35 x cells: "an old-authority writer meeting the seal is refused stale and
    /// nothing past it is readable"; "the lane reconstructed in order from its keys"): an old writer
    /// appends under revision 3; the move seals 3 at the first sequence it wins, after the old writer's
    /// last fact, which commits below it; the old writer's next append meets the seal and is refused
    /// `Sealed`, nothing written; the successor appends under revision 4; a reader opened afterwards
    /// reconstructs the lane in key order, revision 3 to its seal then revision 4, and a fact planted
    /// past the seal is never read. Must NOT happen: a fact of revision 3 after its seal, or the lane
    /// read out of order.
    #[tokio::test]
    async fn an_old_writer_meeting_the_seal_is_refused_stale_and_the_lane_is_read_in_order_to_each_seal() {
        let d = crate::record_store::tempdir("lane-seal");
        let (old, sealer, successor) = (lanes(&d), lanes(&d), lanes(&d));
        let lane = LaneId::Mesh("mesh1".into());
        assert!(matches!(old.append(&lane, 3, &fact(1)).await.unwrap(), Appended::At(_)));
        assert!(matches!(old.append(&lane, 3, &fact(2)).await.unwrap(), Appended::At(_)));
        let seal = LaneSeal { revision: 3, sealed_by: 1, by: by() };
        sealer.read_new(&lane).await.unwrap();
        // The old writer's last fact lands between the sealer's read and its seal: the seal reads past it.
        assert!(matches!(old.append(&lane, 3, &fact(3)).await.unwrap(), Appended::At(_)));
        let sealed = sealer.seal(&lane, &seal).await.unwrap();
        assert_eq!((sealed.at.seq, sealed.passed.len()), (4, 1), "the seal takes the first free key, after the fact it read past: {sealed:?}");
        let refused = old.append(&lane, 3, &fact(4)).await.unwrap();
        assert!(matches!(&refused, Appended::Sealed { at, seal: s } if at.seq == 4 && *s == seal), "{refused:?}");
        assert!(matches!(old.append(&lane, 3, &fact(5)).await.unwrap(), Appended::Sealed { .. }), "and every append after it");
        successor.read_new(&lane).await.unwrap();
        assert!(matches!(successor.append(&lane, 4, &fact(6)).await.unwrap(), Appended::At(LaneKey { revision: 4, seq: 1, .. })));
        // A fact planted past the seal (no writer can put one there) is never read.
        std::fs::write(lane.dir(&crate::store_root::StoreRoot::at(&d)).join("rev-00000000000000000003").join("00000000000000000005.json"), serde_json::to_vec(&serde_json::json!({"format": FACT_FORMAT, "record": fact(9)})).unwrap()).unwrap();
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
            "the lane in key order, each revision to its seal"
        );
        assert_eq!(reader.seal_of(&lane, 3).map(|(k, s)| (k.seq, s)), Some((4, seal)));
        assert_eq!("meshes/mesh1/builds/rev-00000000000000000003/00000000000000000004".parse::<LaneKey>().unwrap(), LaneKey { lane, revision: 3, seq: 4 });
        assert!("meshes/mesh1/builds/rev-3/4".parse::<LaneKey>().is_err(), "a key not in the lane's shape is refused by name");
    }
}
