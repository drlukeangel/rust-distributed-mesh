//! `fabric.storage`: node-admin's Fabric control state behind a storage boundary
//! (fabric-mesh-lifecycle.md §11.1, §13).
//!
//! It holds the Fabric's identity row. The Fabric row carries no pointer to the newest Build (R-BI3): the
//! current Build is read from the Build entities themselves
//! ([`crate::build_state::BuildStateAdapter::current_build`]). `fabric/` on the fabric's store root
//! has one writer, the fabric-primary (R14 item 2: "Fabric-primary: `fabric/`. ... Keys never
//! overlap across writers"): it writes the identity.
//! Every other admin reads it there, and holds the identity it learns before the fabric's first
//! state-commit (its join answer, the Build topic) as its own runtime state, never as a row here.
//!
//! Every seat move of the fabric is one row of ONE fabric-wide sequence (Q35 x):
//! `seat-moves/<T>--<node.id>--<incarnation>.json` on the fabric's store root, `T` a 20-digit
//! zero-padded sequence and the key its writer's birth, so no write replaces another (Q109). The
//! writer folds every earlier move and writes its row at the next `T` ([`commit_seat_move`]); every
//! reader folds the rows in key order and ignores a row that does not follow ([`fold`]); the storage
//! stores and returns what it is given. A move of two seats is one row. A committed node.primary.update or node.fabric-primary.update is one such row
//! ([`SeatMoveRow`], `seat-move/1`), written by the seats' row writer (the holder for the node-admin
//! cohort, the mesh-primary for every other cohort); the takeover of the fabric seat past the hold is
//! one too. Each seat keeps its own revision as data. A resuming executor or a restarted holder reads
//! the move of its operation there ([`SeatTransferRow`] is that move as the handover reads it). The
//! seat-transfer rows under `meshes/<mesh>/seat-transfers/`, the per-seat rows (`fabric/seats/`,
//! `fabric-seat/1`) and the handover stages of the retired fabric.primary.update (`fabric/handovers/`,
//! `fabric-handover/1`) are retired: a store root holding one is refused by name.
//!
//! The Fabric's own state is its [`FabricIdentity`] row (written once). The pointer rows an older build
//! wrote (`fabric/pointers/`, `fabric-pointer/1`) are retired: a store root holding one is refused by name.
//!
//! Two implementations: [`MemoryFabricStorage`] for runs that need no restart survival, and
//! [`FileFabricStorage`], one record per file on the fabric's store root, each write a temp file,
//! fsync and atomic rename. A file this build does not recognise is refused by name, never read as
//! a value.

use async_trait::async_trait;
use crate::model::FabricId;
use serde::{Deserialize, Serialize};

use std::sync::Mutex;

pub use rdm_core::model::FabricIdentity;

/// What a claim of the fabric's name wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FabricClaim {
    /// The identity row was written: the key was free.
    Written,
    /// This Fabric's own identity row stands: nothing is written (Day 0's claim met again by the
    /// fabric-primary's row writer, a restart, a successor fabric-primary).
    Held,
}

/// Another Fabric holds the fabric's name: nothing is written (node-identity-and-state.md §2.8, `NameHeld`
/// naming a different FabricId; R-FN1: the name's claim at `<store_dir>/fabrics/<name>`, or the
/// identity row at the fabric's key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricNameHeld {
    /// The fabric name claimed.
    pub name: String,
    /// The Fabric whose identity row holds the key.
    pub by: FabricId,
}

impl std::fmt::Display for FabricNameHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the fabric name {} is held by Fabric {}: fabric.storage holds that Fabric's claim of the name, and a held claim is never rewritten; a deleted fabric releases it", self.name, self.by)
    }
}

impl std::error::Error for FabricNameHeld {}

/// What a claim of `identity` meets in `held`, the identity row at the fabric's key.
fn claim_fabric_against(held: Option<&FabricIdentity>, identity: &FabricIdentity) -> Result<FabricClaim, FabricNameHeld> {
    match held {
        None => Ok(FabricClaim::Written),
        Some(h) if h.fabric_id == identity.fabric_id => Ok(FabricClaim::Held),
        Some(h) => Err(FabricNameHeld { name: identity.name.clone(), by: h.fabric_id.clone() }),
    }
}

/// A committed node.primary.update as the handover reads it (proposal Part C1 step 5: "The writer
/// writes the one seat row at the next epoch(s) naming the successor. This is the commit"): every seat
/// the holder moved to its successor at the next revision, and the update it commits. It is the
/// [`SeatMoveRow`] of the update's operation: the commit. The holder's authority ends at
/// these revisions, the successor installs from it, and an identical call replays it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatTransferRow {
    /// The update's operation identity, which names its move.
    pub operation: String,
    /// The Build whose stop, delete, restart, replace or connections delete the update precedes.
    pub build_id: String,
    /// That attempt.
    pub attempt: u32,
    /// The former holder of every seat moved.
    pub holder: rdm_core::handover::HandoverBirth,
    /// The successor the pre-check named, which installs from the move.
    pub successor: rdm_core::handover::HandoverBirth,
    /// The seats and the revisions the holder held them at.
    pub expected: Vec<rdm_core::handover::SeatRevision>,
    /// Every seat moved, at its committed revision.
    pub seats: Vec<rdm_core::seat::transfer::CommittedSeat>,
    /// The exact birth that writes the move: the holder for the node-admin cohort, the mesh-primary
    /// for every other cohort.
    pub by: rdm_core::handover::HandoverBirth,
}

impl SeatTransferRow {
    /// The seat-move row that commits this transfer: each seat from the revision the holder held it
    /// at to its committed one.
    pub fn seat_move(&self) -> SeatMoveRow {
        let from = |seat: &rdm_core::seat::transfer::Seat| self.expected.iter().find(|e| &e.seat == seat).map(|e| e.expected_revision);
        SeatMoveRow {
            seats: self.seats.iter().map(|c| MovedSeat { seat: c.seat.clone(), from_revision: from(&c.seat).unwrap_or(c.revision.saturating_sub(1)), revision: c.revision, holder: Some(c.holder.clone()), from: Some(self.holder.clone()) }).collect(),
            cause: MoveCause::Handover { operation: self.operation.clone(), build_id: self.build_id.clone(), attempt: self.attempt },
            by: self.by.clone(),
        }
    }

    /// The transfer a committed handover is; `None` for an election, a takeover or a move of no seat.
    pub fn of_move(row: &SeatMoveRow) -> Option<Self> {
        let MoveCause::Handover { operation, build_id, attempt } = &row.cause else { return None };
        let first = row.seats.first()?;
        Some(Self {
            operation: operation.clone(),
            build_id: build_id.clone(),
            attempt: *attempt,
            holder: first.from.clone()?,
            successor: first.holder.clone()?,
            expected: row.seats.iter().map(|m| rdm_core::handover::SeatRevision { seat: m.seat.clone(), expected_revision: m.from_revision }).collect(),
            seats: row.seats.iter().map(|m| Some(rdm_core::seat::transfer::CommittedSeat { seat: m.seat.clone(), holder: m.holder.clone()?, revision: m.revision })).collect::<Option<Vec<_>>>()?,
            by: row.by.clone(),
        })
    }
}

/// One seat a seat move moves (Q35: `(seat, from_revision, revision, holder, from)`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovedSeat {
    /// The seat.
    pub seat: rdm_core::seat::transfer::Seat,
    /// The revision the writer folded it at.
    pub from_revision: u64,
    /// Its revision from this move: its own counter, stepped by one.
    pub revision: u64,
    /// Its holder from this move; `None` when the move leaves the seat vacant (i144 Q51: a takeover
    /// vacates the lost mesh's primary seat, which its mesh's next admin fills by the ordinary rule).
    pub holder: Option<rdm_core::handover::HandoverBirth>,
    /// Its holder before this move; `None` for a seat's first seating.
    pub from: Option<rdm_core::handover::HandoverBirth>,
}

/// What moved the seats of one seat move (Q48: every seat change is a row).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MoveCause {
    /// The ordinary rule seated its winner (elections §3, §4): day 0's first seating, a seat filled
    /// after its holder's proven exit, a seat a holder took again after its successor's exit.
    Election,
    /// A committed node.primary.update or node.fabric-primary.update: the update's identity.
    Handover {
        /// The update's operation identity.
        operation: String,
        /// Its Build.
        build_id: String,
        /// Its attempt.
        attempt: u32,
    },
    /// The takeover of the fabric seat past the hold (elections §5).
    Takeover,
}

/// The birth the fabric seat was taken from and the birth the takeover seated, with the move's `t`,
/// when the fabric seat's latest move in `moves` is a takeover (elections §5): the lost fabric-primary,
/// whose mesh's node-admin the new fabric-primary re-creates from the fabric lane (elections §4.3,
/// node-recovery.md `R-FM2`) and to whose path.name the seat is owed back (fabric-recovery.md step 4),
/// and the seat's holder on the store root. `None` when that move is an election or a handover, or no
/// move seated the fabric seat.
pub fn fabric_seat_taken_from(moves: &[SeatMove]) -> Option<(u64, rdm_core::handover::HandoverBirth, rdm_core::handover::HandoverBirth)> {
    let (t, cause, moved) = moves.iter().rev().find_map(|m| m.row.seats.iter().find(|s| s.seat == rdm_core::seat::transfer::Seat::FabricPrimary).map(|s| (m.t, &m.row.cause, s)))?;
    if *cause != MoveCause::Takeover {
        return None;
    }
    Some((t, moved.from.clone()?, moved.holder.clone()?))
}

/// One row of the fabric's seat-move sequence (`seat-move/1`, Q35 x): the seats it moves, what moved
/// them, and the exact birth that wrote it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatMoveRow {
    /// Every seat moved, one row however many.
    pub seats: Vec<MovedSeat>,
    /// What moved them.
    pub cause: MoveCause,
    /// The exact birth that wrote the row.
    pub by: rdm_core::handover::HandoverBirth,
}

/// A committed seat move: its place `t` in the fabric-wide sequence, and its row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatMove {
    /// Its place in the sequence, from 1.
    pub t: u64,
    /// The row.
    pub row: SeatMoveRow,
}

/// The fabric's seat moves folded in order: each seat's revision as the moves left it.
#[derive(Debug, Clone)]
struct MoveLog {
    next: u64,
    moves: Vec<SeatMove>,
    seats: std::collections::HashMap<rdm_core::seat::transfer::Seat, u64>,
}

impl Default for MoveLog {
    fn default() -> Self {
        Self { next: 1, moves: Vec::new(), seats: Default::default() }
    }
}

impl MoveLog {
    /// `rows` folded in order of `t`, as every reader folds them (Q109): a row that does not follow the
    /// fold is ignored. The next `t` is past every row stored, an ignored one included, so a write
    /// never lands on a stored key.
    fn fold(rows: &[SeatMove]) -> Self {
        let mut rows = rows.to_vec();
        rows.sort_by(|a, b| (a.t, move_key(a.t, &a.row.by)).cmp(&(b.t, move_key(b.t, &b.row.by))));
        let mut log = Self::default();
        for m in &rows {
            if log.admits(&m.row).is_ok() {
                log.take(m.t, m.row.clone());
            }
        }
        log.next = rows.last().map_or(1, |m| m.t + 1);
        log
    }

    /// Whether `row` follows the fold at the log's end (Q48 A): each seat steps its own revision by
    /// one, from exactly the revision the moves before it left it at (0 for a seat no move has seated).
    /// A move computed against any other view does not, naming the seat.
    fn admits(&self, row: &SeatMoveRow) -> Result<(), String> {
        if row.seats.is_empty() {
            return Err("the move moves no seat".into());
        }
        for m in &row.seats {
            if m.from_revision.checked_add(1) != Some(m.revision) {
                return Err(format!("the {} seat moves from revision {} to {}: a seat's revision steps by one", m.seat.name(), m.from_revision, m.revision));
            }
            let logged = self.seats.get(&m.seat).copied().unwrap_or(0);
            if m.from_revision != logged {
                return Err(format!("the {} seat moves from revision {} and the moves before it left it at revision {logged}", m.seat.name(), m.from_revision));
            }
        }
        Ok(())
    }

    fn take(&mut self, t: u64, row: SeatMoveRow) {
        for m in &row.seats {
            self.seats.insert(m.seat.clone(), m.revision);
        }
        self.moves.push(SeatMove { t, row });
        self.next = t + 1;
    }

    fn transfer(&self, operation: &str) -> Option<SeatTransferRow> {
        self.moves.iter().rev().filter_map(|m| SeatTransferRow::of_move(&m.row)).find(|r| r.operation == operation)
    }
}

/// What the writer's commit of a seat move came to ([`commit_seat_move`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveCommit {
    /// The row is written at its `t`.
    Committed(SeatMove),
    /// The row does not follow the writer's fold of the moves before it, naming why: a seat it moves
    /// moved meanwhile. Nothing is written.
    Refused(String),
}

/// Commit `row` as the fabric's next seat move: the writer folds every earlier move and decides
/// (node-primary-update.md: "inserted at the next `T` once the writer has folded every earlier move"),
/// then writes the row at the next `t`. A row that does not follow the fold is not written. The
/// storage decides nothing (Q109).
pub async fn commit_seat_move(storage: &dyn FabricStorage, row: &SeatMoveRow) -> Result<MoveCommit, FabricStorageError> {
    let _one = COMMITS.lock().await;
    let log = MoveLog::fold(&storage.seat_moves().await?);
    if let Err(why) = log.admits(row) {
        return Ok(MoveCommit::Refused(why));
    }
    let moved = SeatMove { t: log.next, row: row.clone() };
    storage.put_seat_move(&moved).await?;
    Ok(MoveCommit::Committed(moved))
}

/// The writer's commits, one at a time: each folds the row the one before it wrote, so no two of its
/// rows take one `t` (Q109: no write replaces another).
static COMMITS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Every seat move as the readers fold them (Q109): the rows `storage` holds, in key order, a row
/// that does not follow the fold ignored.
pub async fn folded_moves(storage: &dyn FabricStorage) -> Result<Vec<SeatMove>, FabricStorageError> {
    Ok(fold(&storage.seat_moves().await?))
}

/// `rows` as every reader folds them (Q109): in key order, a row that does not follow the fold ignored.
pub fn fold(rows: &[SeatMove]) -> Vec<SeatMove> {
    MoveLog::fold(rows).moves
}

/// The move of `operation` among the folded moves, as the handover reads it: `None` when none was
/// committed (the transfer never happened).
pub async fn seat_transfer(storage: &dyn FabricStorage, operation: &str) -> Result<Option<SeatTransferRow>, FabricStorageError> {
    Ok(MoveLog::fold(&storage.seat_moves().await?).transfer(operation))
}

/// Every move of an operation among the folded moves, in order, as the handover reads them.
pub async fn seat_transfers(storage: &dyn FabricStorage) -> Result<Vec<SeatTransferRow>, FabricStorageError> {
    Ok(folded_moves(storage).await?.iter().filter_map(|m| SeatTransferRow::of_move(&m.row)).collect())
}

/// Why a `fabric.storage` read or write failed.
pub use crate::storage_error::StorageError as FabricStorageError;

/// Claim the fabric's name for `identity.fabric_id` and keep `identity` as the Fabric's identity row
/// (node-identity-and-state.md §2.8): the writer reads the claim at the fabric's `(path, name)` (R-FN1:
/// `<store_dir>/fabrics/<name>`) and claims it, then the identity row at the fabric's key (Q109: the
/// storage decides nothing). A free key takes the row (`Written`); this Fabric's own row there writes
/// nothing (`Held`); another Fabric holds it: `FabricNameHeld` naming that Fabric, and nothing of this
/// fabric is written.
pub async fn claim_identity(storage: &dyn FabricStorage, identity: &FabricIdentity) -> Result<Result<FabricClaim, FabricNameHeld>, FabricStorageError> {
    let named = match claim_fabric_against(storage.name_claim(&identity.name).await?.as_ref(), identity) {
        Ok(c) => c,
        Err(held) => return Ok(Err(held)),
    };
    if named == FabricClaim::Written {
        storage.put_name_claim(identity).await?;
    }
    let own = match claim_fabric_against(storage.fabric().await?.as_ref(), identity) {
        Ok(c) => c,
        Err(held) => return Ok(Err(held)),
    };
    if own == FabricClaim::Written {
        storage.put_identity(identity).await?;
    }
    Ok(Ok(if named == FabricClaim::Written || own == FabricClaim::Written { FabricClaim::Written } else { FabricClaim::Held }))
}

/// fabric.delete's release of the fabric's name (R-FN1: "a deleted fabric releases it"): the name's
/// claim goes when it is `identity`'s Fabric's, and another Fabric's claim there is left as it is.
/// Returns whether it went. The hook s59's fabric.delete calls; nothing else releases a fabric's name.
pub async fn release_name(storage: &dyn FabricStorage, identity: &FabricIdentity) -> Result<bool, FabricStorageError> {
    if !storage.name_claim(&identity.name).await?.is_some_and(|h| h.fabric_id == identity.fabric_id) {
        return Ok(false);
    }
    storage.remove_name_claim(&identity.name).await?;
    Ok(true)
}

/// node-admin's Fabric control state: each kind of row written on its own and never read back to be
/// written again.
#[async_trait]
pub trait FabricStorage: Send + Sync {
    /// The Fabric's identity row; `None` while no identity is held.
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError>;
    /// The claim of the fabric name `name` at the fabric's `(path, name)` (R-FN1:
    /// `<store_dir>/fabrics/<name>`, above the per-fabric roots): the Fabric that holds it, if any.
    async fn name_claim(&self, name: &str) -> Result<Option<FabricIdentity>, FabricStorageError>;
    /// Write `identity` as the claim of its name, as given (Q109: storage stores and returns what it is
    /// given); the claim is the writer's ([`claim_identity`]).
    async fn put_name_claim(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError>;
    /// Remove the claim of the fabric name `name`.
    async fn remove_name_claim(&self, name: &str) -> Result<(), FabricStorageError>;
    /// Write `identity` as the Fabric's identity row, as given.
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError>;
    /// Write `moved`, a row of the fabric's seat-move sequence, at its `t`, as given (Q109: storage
    /// stores and returns what it is given). The writer folded the moves before it
    /// ([`commit_seat_move`]).
    async fn put_seat_move(&self, moved: &SeatMove) -> Result<(), FabricStorageError>;
    /// Every seat move this storage holds, in order of `t`, as written.
    async fn seat_moves(&self) -> Result<Vec<SeatMove>, FabricStorageError>;
    /// The committed fabric seat record this storage reads (set once at boot, once membership holds
    /// the seat records): the `fabric/` rows it holds before the fabric's first state-commit (its
    /// Fabric identity, written while it held the fabric seat) are written only while the record
    /// still says this writer holds the fabric seat; a writer whose seat moved first drops them,
    /// named (R14 item 2).
    fn set_seat_source(&self, seat: crate::fabric_builds::SeatSource);
}

/// A fabric store held in memory.
#[derive(Default)]
pub struct MemoryFabricStorage {
    identity: Mutex<Option<FabricIdentity>>,
    moves: Mutex<Vec<SeatMove>>,
    /// The fabric-primary seat's cut-off the identity writes read.
    cut_off: std::sync::Arc<crate::fabric_handover::SeatCutOff>,
    /// The committed fabric seat record the identity writes read, once set.
    seat: std::sync::OnceLock<crate::fabric_builds::SeatSource>,
}

impl std::fmt::Debug for MemoryFabricStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryFabricStorage").field("identity", &self.identity).field("moves", &self.moves).field("cut_off", &self.cut_off).field("seat", &self.seat.get().map(|_| "set")).finish()
    }
}

impl MemoryFabricStorage {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store whose identity writes are cut off by `cut_off` (the fabric-primary seat's scope).
    pub fn cut_off_by(cut_off: std::sync::Arc<crate::fabric_handover::SeatCutOff>) -> Self {
        Self { cut_off, ..Self::default() }
    }

    /// A write of the Fabric identity or its name (`fabric/<what>`): refused while the fabric-primary
    /// seat's writes are cut off, and, once the seat record is read, while it names another live
    /// birth (`NotSeatHolder`, naming it). Before the record is set (Day 0's claim, ahead of
    /// membership) no record governs the write.
    fn identity_write(&self, what: &str) -> Result<(), FabricStorageError> {
        self.cut_off.fabric_write(|| format!("{FABRIC_DIR}/{what}"))?;
        match self.seat.get().map(|s| (s.read)()) {
            Some(crate::fabric_builds::SeatRecord::Moved { by, to }) => Err(FabricStorageError::NotSeatHolder {
                file: format!("{FABRIC_DIR}/{what}"),
                reason: format!("fabric-primary seat: {by}; the holder is {}", to.as_deref().unwrap_or("a birth whose path.name is not known here")),
            }),
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl FabricStorage for MemoryFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError> {
        Ok(self.identity.lock().unwrap().clone())
    }
    fn set_seat_source(&self, seat: crate::fabric_builds::SeatSource) {
        let _ = self.seat.set(seat);
    }
    // The identity slot is this store's claim of the name too: one fabric per memory store.
    async fn name_claim(&self, name: &str) -> Result<Option<FabricIdentity>, FabricStorageError> {
        Ok(self.identity.lock().unwrap().clone().filter(|i| i.name == name))
    }
    async fn put_name_claim(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError> {
        self.identity_write("name")?;
        *self.identity.lock().unwrap() = Some(identity.clone());
        Ok(())
    }
    async fn remove_name_claim(&self, name: &str) -> Result<(), FabricStorageError> {
        self.identity_write("name")?;
        let mut held = self.identity.lock().unwrap();
        if held.as_ref().is_some_and(|i| i.name == name) {
            *held = None;
        }
        Ok(())
    }
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError> {
        self.identity_write("identity")?;
        *self.identity.lock().unwrap() = Some(identity.clone());
        Ok(())
    }
    async fn put_seat_move(&self, moved: &SeatMove) -> Result<(), FabricStorageError> {
        let mut moves = self.moves.lock().unwrap();
        moves.retain(|m| (m.t, &m.row.by) != (moved.t, &moved.row.by));
        moves.push(moved.clone());
        moves.sort_by(|a, b| (a.t, move_key(a.t, &a.row.by)).cmp(&(b.t, move_key(b.t, &b.row.by))));
        Ok(())
    }
    async fn seat_moves(&self) -> Result<Vec<SeatMove>, FabricStorageError> {
        Ok(self.moves.lock().unwrap().clone())
    }
}

/// The directory inside the fabric's store root.
pub const FABRIC_DIR: &str = "fabric";
#[cfg(feature = "file-backend")]
/// The retired pointer rows (`fabric-pointer/1`, one per move of the pointer an older build kept on
/// the Fabric row): a store root holding one is refused by name (R-BI3).
const RETIRED_POINTERS_DIR: &str = "fabric/pointers";
#[cfg(feature = "file-backend")]
const RETIRED_POINTER_FORMAT: &str = "fabric-pointer/1";
#[cfg(feature = "file-backend")]
/// The retired per-seat rows (`fabric-seat/1`): the leadership view is folded from the transfer rows.
const RETIRED_SEATS_DIR: &str = "fabric/seats";
#[cfg(feature = "file-backend")]
const RETIRED_SEAT_FORMAT: &str = "fabric-seat/1";
#[cfg(feature = "file-backend")]
/// The retired stages of fabric.primary.update (`fabric-handover/1`): node.fabric-primary.update
/// leaves one transfer row.
const RETIRED_HANDOVERS_DIR: &str = "fabric/handovers";
#[cfg(feature = "file-backend")]
const RETIRED_HANDOVER_FORMAT: &str = "fabric-handover/1";
#[cfg(feature = "file-backend")]
/// The retired seat-transfer rows under `meshes/<holder mesh>/` (`fabric-seat-transfer/1`): a store
/// root holding one is refused by name.
const REFUSED_TRANSFERS_DIR: &str = "seat-transfers";
#[cfg(feature = "file-backend")]
const REFUSED_TRANSFER_FORMAT: &str = "fabric-seat-transfer/1";
/// The fabric's seat moves on its store root, `seat-moves/<T>--<node.id>--<incarnation>.json`.
pub const SEAT_MOVES_DIR: &str = "seat-moves";
/// The format of one seat move.
pub const SEAT_MOVE_FORMAT: &str = "seat-move/1";
#[cfg(feature = "file-backend")]
const IDENTITY_KEY: &str = "identity";
#[cfg(feature = "file-backend")]
/// The retired whole-record file (`fabric.json`, format `fabric-record/1`): identity and a Build in
/// one row. A data dir holding it is refused by name.
const RETIRED_RECORD_KEY: &str = "fabric";
#[cfg(feature = "file-backend")]
const IDENTITY_FORMAT: &str = "fabric-identity/1";
#[cfg(feature = "file-backend")]
/// A fabric's claim of its name at `<store_dir>/fabrics/<name>` (R-FN1): the claiming Fabric's identity.
const NAME_FORMAT: &str = "fabric-name/1";

#[cfg(feature = "file-backend")]
/// One row per file under `<store root>/fabric/` (`crate::store_root`): `identity.json`
/// (`crate::record_store`); the fabric's seat moves under `<store root>/seat-moves/`.
#[derive(Debug)]
pub struct FileFabricStorage {
    /// `<store_dir>/fabrics/`: the fabrics' name index above the per-fabric roots (R-FN1), where this
    /// fabric's claim of its name is the first of its rows.
    names: crate::record_store::FileRecords,
    records: crate::record_store::FileRecords,
    /// `<store root>/seat-moves/`: one key per seat move, `<T>`, and the moves this process holds
    /// until the fabric's first state-commit.
    moves: std::sync::Arc<MoveStore>,
    writer: crate::model::IncarnationId,
    /// Where this writer reads whether it holds the fabric seat: the committed fabric seat record
    /// (`crate::admin::fabric_seat_source`). Set once at start.
    seat: SeatSlot,
}

#[cfg(feature = "file-backend")]
/// The fabric seat source a [`FileFabricStorage`] reads when its held `fabric/` rows are published.
#[derive(Default, Clone)]
struct SeatSlot(std::sync::Arc<std::sync::OnceLock<crate::fabric_builds::SeatSource>>);

#[cfg(feature = "file-backend")]
impl SeatSlot {
    /// Whether the writer born as `writer` may write `fabric/`: the committed fabric seat record
    /// names it, names none (Day 0, before any record) or names a birth proven gone. `Err` says why
    /// not.
    fn open(&self, writer: &crate::model::IncarnationId) -> Result<(), String> {
        match self.0.get().map(|s| (s.read)()) {
            None => Err(format!("fabric-primary seat: this writer ({}) has no fabric seat record to read, so it cannot show it holds the seat", writer.as_str())),
            Some(crate::fabric_builds::SeatRecord::Moved { by, to }) => Err(format!("fabric-primary seat: {by}; the holder is {}", to.as_deref().unwrap_or("a birth whose path.name is not known here"))),
            Some(crate::fabric_builds::SeatRecord::Open { .. }) => Ok(()),
        }
    }
}

#[cfg(feature = "file-backend")]
impl std::fmt::Debug for SeatSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.get().is_some() { "SeatSlot(set)" } else { "SeatSlot(unset)" })
    }
}

#[cfg(feature = "file-backend")]
impl FileFabricStorage {
    /// Open the store under the fabric's store root, written by the process born as `writer`; a
    /// directory holding a retired record is refused by name.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &crate::model::IncarnationId) -> Result<Self, FabricStorageError> {
        let own_data_dir = root.dir();
        // The name index opens first: held in the scratchpad, it is the first store the fabric's
        // state-commit publishes.
        let names = root.fabric_names(writer).await?;
        let records = root.records(own_data_dir, FABRIC_DIR, writer).await?;
        if records.holds(RETIRED_RECORD_KEY).await? {
            return Err(FabricStorageError::Unrecognised {
                file: own_data_dir.join(FABRIC_DIR).join("fabric.json").display().to_string(),
                reason: format!("the whole-record format fabric-record/1 is retired; this build reads {IDENTITY_FORMAT} rows"),
            });
        }
        refuse_retired(&own_data_dir.join(RETIRED_POINTERS_DIR), &format!("the pointer row format {RETIRED_POINTER_FORMAT} is retired: the Fabric row carries no pointer to the newest Build, and the current Build is read from the builds store")).await?;
        refuse_retired(&own_data_dir.join(RETIRED_SEATS_DIR), &format!("the per-seat row format {RETIRED_SEAT_FORMAT} is retired: the leadership view is folded from the fabric's seat moves under {SEAT_MOVES_DIR}/")).await?;
        refuse_retired(&own_data_dir.join(RETIRED_HANDOVERS_DIR), &format!("the handover stage format {RETIRED_HANDOVER_FORMAT} is retired: node.fabric-primary.update is one seat move under {SEAT_MOVES_DIR}/")).await?;
        let meshes = own_data_dir.join("meshes");
        let transfer_dirs = crate::blocking::blocking_io(move || -> Vec<std::path::PathBuf> {
            let mut dirs: Vec<std::path::PathBuf> = std::fs::read_dir(&meshes).map(|e| e.flatten().map(|e| e.path().join(REFUSED_TRANSFERS_DIR)).filter(|d| d.is_dir()).collect()).unwrap_or_default();
            dirs.sort();
            dirs
        })
        .await;
        for dir in transfer_dirs {
            refuse_retired(&dir, &format!("the seat-transfer row format {REFUSED_TRANSFER_FORMAT} is retired: every seat move is one row of the fabric's sequence under {SEAT_MOVES_DIR}/ ({SEAT_MOVE_FORMAT})")).await?;
        }
        let moves = MoveStore::open(root, writer).await?;
        Ok(Self { names, records, moves, writer: writer.clone(), seat: SeatSlot::default() })
    }

    /// The records the fabric-primary seat writes (R14 item 2): the fabric's claim of its name, and
    /// the Fabric identity and shutdown. What a node.fabric-primary.update moving that seat cuts off,
    /// and what a publication writes only while the committed fabric seat record names this writer.
    /// Seat moves are the fabric's one sequence: each commits by its own insert.
    pub fn fabric_primary_records(&self) -> [&crate::record_store::FileRecords; 2] {
        [&self.names, &self.records]
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl FabricStorage for FileFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError> {
        self.records.read(IDENTITY_KEY, IDENTITY_FORMAT).await
    }
    fn set_seat_source(&self, seat: crate::fabric_builds::SeatSource) {
        let _ = self.seat.0.set(seat);
        for store in self.fabric_primary_records() {
            let (slot, writer) = (self.seat.clone(), self.writer.clone());
            store.set_publish_gate(std::sync::Arc::new(move || slot.open(&writer)));
        }
    }
    async fn name_claim(&self, name: &str) -> Result<Option<FabricIdentity>, FabricStorageError> {
        self.names.read(name_key(&self.names, name)?, NAME_FORMAT).await
    }
    async fn put_name_claim(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError> {
        self.names.write(name_key(&self.names, &identity.name)?, NAME_FORMAT, identity).await
    }
    async fn remove_name_claim(&self, name: &str) -> Result<(), FabricStorageError> {
        self.names.remove(name_key(&self.names, name)?).await.map(|_| ())
    }
    async fn put_identity(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError> {
        self.records.write(IDENTITY_KEY, IDENTITY_FORMAT, identity).await
    }
    async fn put_seat_move(&self, moved: &SeatMove) -> Result<(), FabricStorageError> {
        self.moves.put(moved).await
    }
    async fn seat_moves(&self) -> Result<Vec<SeatMove>, FabricStorageError> {
        self.moves.rows().await
    }
}

#[cfg(feature = "file-backend")]
/// The fabric's seat moves on its store root (`seat-moves/<T>--<node.id>--<incarnation>`), as one
/// process reads and writes them:
/// each row stored at its `t` as given. Before the fabric's first state-commit every row a process
/// writes is held in its scratchpad (R14) and written at its `t` at the publication.
struct MoveStore {
    records: crate::record_store::FileRecords,
    /// `Some` while this process holds its moves. Locked across a read of the root and across the
    /// publication, so every read sees each row either held or written.
    held: tokio::sync::Mutex<Option<Vec<SeatMove>>>,
}

#[cfg(feature = "file-backend")]
impl std::fmt::Debug for MoveStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoveStore").field("records", &self.records).finish()
    }
}

#[cfg(feature = "file-backend")]
impl MoveStore {
    async fn open(root: &crate::store_root::StoreRoot, writer: &crate::model::IncarnationId) -> Result<std::sync::Arc<Self>, FabricStorageError> {
        let records = crate::record_store::FileRecords::open(root.dir(), SEAT_MOVES_DIR, writer).await?;
        let store = std::sync::Arc::new(Self { records, held: tokio::sync::Mutex::new(Some(Vec::new())) });
        if !root.scratchpad().is_some_and(|s| s.enrol(store.clone())) {
            *store.held.lock().await = None;
        }
        Ok(store)
    }

    /// The rows on the root, then this process's held ones, in key order: by `t`, then by writer.
    async fn rows(&self) -> Result<Vec<SeatMove>, FabricStorageError> {
        let held = self.held.lock().await;
        let mut rows = Vec::new();
        for (key, row) in self.records.list_keyed::<SeatMoveRow>(SEAT_MOVE_FORMAT).await? {
            let t = move_key_t(&key).ok_or_else(|| FabricStorageError::Unrecognised { file: format!("{SEAT_MOVES_DIR}/{key}.json"), reason: "the key names no t".into() })?;
            rows.push(SeatMove { t, row });
        }
        rows.extend(held.iter().flatten().cloned());
        rows.sort_by(|a, b| (a.t, move_key(a.t, &a.row.by)).cmp(&(b.t, move_key(b.t, &b.row.by))));
        rows.dedup_by(|a, b| (a.t, &a.row.by) == (b.t, &b.row.by));
        Ok(rows)
    }

    async fn put(&self, moved: &SeatMove) -> Result<(), FabricStorageError> {
        let mut held = self.held.lock().await;
        if let Some(held) = held.as_mut() {
            held.push(moved.clone());
            return Ok(());
        }
        self.records.write(&move_key(moved.t, &moved.row.by), SEAT_MOVE_FORMAT, &moved.row).await
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl crate::record_store::HeldStore for MoveStore {
    async fn publish(&self) -> Result<crate::record_store::StorePublished, crate::storage_error::StorageError> {
        let mut done = crate::record_store::StorePublished { store: SEAT_MOVES_DIR.into(), ..Default::default() };
        let mut held = self.held.lock().await;
        let Some(rows) = held.as_ref() else {
            return Ok(done);
        };
        for moved in rows {
            self.records.write(&move_key(moved.t, &moved.row.by), SEAT_MOVE_FORMAT, &moved.row).await?;
            done.written += 1;
        }
        *held = None;
        Ok(done)
    }
}

/// The seals a birth `by` owes on a Build lane before it appends under a revision (Q35): for every
/// committed move of the lane's seat from an earlier revision, that revision's seal naming the move.
pub fn seals_owed(storage: std::sync::Arc<dyn FabricStorage>, by: rdm_core::handover::HandoverBirth) -> crate::build_state::SealsOwed {
    std::sync::Arc::new(move |lane: crate::build_state::LaneId, below: u64| {
        let (storage, by) = (storage.clone(), by.clone());
        Box::pin(async move {
            let seat = match &lane {
                crate::build_state::LaneId::Fabric => rdm_core::seat::transfer::Seat::FabricPrimary,
                crate::build_state::LaneId::Mesh(mesh) => rdm_core::seat::transfer::Seat::MeshPrimary { mesh: mesh.clone() },
            };
            let moves = crate::fabric_storage::folded_moves(&*storage).await.map_err(|e| format!("fabric.storage, the fabric's seat moves could not be read for the seals of {}: {e}", lane.name()))?;
            Ok(moves.iter().flat_map(|m| m.row.seats.iter().filter(|s| s.seat == seat && s.from_revision < below).map(|s| crate::build_state::LaneSeal { revision: s.from_revision, sealed_by: m.t, by: by.clone() }).collect::<Vec<_>>()).collect())
        })
    })
}

#[cfg(feature = "file-backend")]
/// The key of the seat move at `t`: 20-digit zero-padded, so a listing sorts in order.
/// A seat move's key: its `t`, then its writer's exact birth, so no writer's row replaces another's
/// (Q109: "the row's key names its writer's birth with T").
fn move_key(t: u64, by: &rdm_core::handover::HandoverBirth) -> String {
    format!("{t:020}--{}--{}", by.node_id, by.incarnation.as_str())
}

/// The `t` a seat move's key names.
fn move_key_t(key: &str) -> Option<u64> {
    key.split("--").next()?.parse().ok()
}

#[cfg(feature = "file-backend")]
/// One claim of `identity` at `key` of `records`: the filesystem's own atomic link of the finished row
/// to the key, which fails when the key is live, and then the decision against the row standing there
/// (this Fabric's own: `Held`; another Fabric's: refused, naming it). Held before the fabric's first
/// state-commit, the claim is decided again at the publication against the row then standing at the
/// key. A key that is not one path segment is refused by name: it names no row of the store.
/// A store root an older build left rows of a retired format in `dir` on is refused by name, the
/// first row named and `reason` saying what retired it (the pointer rows, R-BI3; the per-seat rows and
/// the handover stages, proposal Part C2). An empty or absent directory holds no row.
async fn refuse_retired(dir: &std::path::Path, reason: &str) -> Result<(), FabricStorageError> {
    let (dir, reason) = (dir.to_path_buf(), reason.to_string());
    crate::blocking::blocking_io(move || {
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(FabricStorageError::Io { file: dir.display().to_string(), reason: e.to_string() }),
        };
        let mut rows: Vec<std::path::PathBuf> = entries.filter_map(|e| e.ok()).map(|e| e.path()).filter(|p| p.extension().is_some_and(|x| x == "json")).collect();
        rows.sort();
        match rows.first() {
            None => Ok(()),
            Some(row) => Err(FabricStorageError::Unrecognised { file: row.display().to_string(), reason }),
        }
    })
    .await
}

#[cfg(feature = "file-backend")]
/// The fabric name `name` as the key of its claim: one path segment, or no key.
fn name_key<'a>(records: &crate::record_store::FileRecords, name: &'a str) -> Result<&'a str, FabricStorageError> {
    if name.is_empty() || name.starts_with('.') || name.contains(['/', '\\']) {
        return Err(FabricStorageError::Io { file: format!("{}/{name}", records.store_name()), reason: format!("the fabric name {name:?} is not one path segment, so it keys no claim of the fabric's name") });
    }
    Ok(name)
}

#[cfg(all(test, feature = "file-backend"))]
mod tests {
    use super::*;

    fn birth(node: &str, inc: &str) -> rdm_core::handover::HandoverBirth {
        rdm_core::handover::HandoverBirth { mesh: "mesh1".into(), node_id: rdm_core::NodeId::parse(node).unwrap(), incarnation: rdm_core::IncarnationId::parse(&rdm_core::fixture_crockford60(inc)).unwrap() }
    }

    /// The transfer of `operation` from `p` to `n` of the mesh seat at `mesh_from` and, when given,
    /// the fabric seat at `fabric_from`.
    fn transfer(operation: &str, mesh_from: u64, fabric_from: Option<u64>) -> SeatTransferRow {
        use rdm_core::seat::transfer::Seat;
        let (p, n) = (birth("100000000000", "p"), birth("200000000000", "n"));
        let mut moved = vec![(Seat::MeshPrimary { mesh: "mesh1".into() }, mesh_from)];
        moved.extend(fabric_from.map(|f| (Seat::FabricPrimary, f)));
        SeatTransferRow {
            operation: operation.into(),
            build_id: "b1".into(),
            attempt: 2,
            holder: p.clone(),
            successor: n.clone(),
            expected: moved.iter().map(|(seat, from)| rdm_core::handover::SeatRevision { seat: seat.clone(), expected_revision: *from }).collect(),
            seats: moved.iter().map(|(seat, from)| rdm_core::seat::transfer::CommittedSeat { seat: seat.clone(), holder: n.clone(), revision: from + 1 }).collect(),
            by: p,
        }
    }

    fn committed(c: MoveCommit) -> SeatMove {
        match c {
            MoveCommit::Committed(m) => m,
            MoveCommit::Refused(why) => panic!("refused: {why}"),
        }
    }

    /// CONTRACT (Q35 x: "a two-seat move is one row, atomic"; node-primary-update.md "Where the row
    /// lives": the row "at the next `T` once the writer has folded every earlier move"): a node.primary.update moving both seats is
    /// ONE row of the fabric's seat-move sequence, `seat-moves/00000000000000000001` in the
    /// `seat-move/1` shape, each seat stepped by one from the revision it was held at; a reopened store
    /// folds it back and finds it by its operation, in both stores. A format this build does not know
    /// is refused by name. Must NOT happen: a two-seat move written as two rows, a revision stepping by
    /// more than one, a row keyed by its holder's mesh.
    #[tokio::test]
    async fn a_two_seat_move_is_one_row_of_the_fabrics_sequence_with_both_revisions_stepped() {
        let row = transfer("node.connections.delete:mesh1.admin.1", 0, Some(0));
        let d = tempdir();
        let s = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::mint()).await.unwrap();
        let memory = MemoryFabricStorage::new();
        for store in [&s as &dyn FabricStorage, &memory] {
            let m = committed(commit_seat_move(store, &row.seat_move()).await.unwrap());
            assert_eq!(m.t, 1, "the fabric's first seat move");
            assert_eq!(m.row.seats.iter().map(|x| (x.seat.name(), x.from_revision, x.revision)).collect::<Vec<_>>(), vec![("mesh-primary", 0, 1), ("fabric-primary", 0, 1)], "both seats in the one row, each stepped by one");
            assert_eq!(seat_transfer(store, "node.connections.delete:mesh1.admin.1").await.unwrap(), Some(row.clone()), "the move read back as the transfer it commits");
            assert_eq!(seat_transfer(store, "node.stop:mesh1.admin.1").await.unwrap(), None);
        }
        let reopened = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::mint()).await.unwrap();
        assert_eq!(seat_transfers(&reopened).await.unwrap(), vec![row.clone()]);
        let files: Vec<String> = std::fs::read_dir(d.join(SEAT_MOVES_DIR)).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).filter(|f| f.ends_with(".json")).collect();
        let key = format!("{}.json", move_key(1, &row.seat_move().by));
        assert_eq!(files, vec![key.clone()], "one row, keyed by its t and its writer's birth");
        assert!(!d.join("meshes").join("mesh1").join("seat-transfers").exists(), "no row under the holder's mesh");
        let file = d.join(SEAT_MOVES_DIR).join(&key);
        let text = std::fs::read_to_string(&file).unwrap();
        assert!(text.starts_with(r#"{"format":"seat-move/1","record":{"seats":[{"seat":{"MeshPrimary":{"mesh":"mesh1"}},"from_revision":0,"revision":1"#), "{text}");
        std::fs::write(&file, br#"{"format":"seat-move/9","record":{}}"#).unwrap();
        let fresh = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::mint()).await.unwrap();
        assert!(matches!(fresh.seat_moves().await, Err(FabricStorageError::Unrecognised { .. })));
    }

    /// CONTRACT (Q48 A: "every seat change is one row in seat-moves, including a seat filled by the
    /// ordinary rule after a proven exit ... and day 0's first seating"; "a vacancy filled by the rule
    /// writes seat-moves/<T>; a move computed against the pre-fill revision is refused by the fold";
    /// Q109: the writer's fold):
    /// day 0's election seats P in mesh1's and the fabric's seats as ONE row of kind `election`, each
    /// from revision 0 with no previous holder. P hands mesh1's seat to N (revision 2); N's exit is
    /// proven, and the rule fills the vacancy with M: an `election` row from N at revision 2 to M at 3,
    /// written before it is announced. N's handover computed against revision 2 is refused by the fold,
    /// naming the seat and the revision the moves left it at; so is a second election computed
    /// against the pre-fill view, whose refusal folds the moves into the seat book (M at 3). Must NOT
    /// happen: a seat's revision advancing with no row, or a move from a revision the log no longer
    /// holds committing.
    #[tokio::test]
    async fn a_vacancy_filled_by_the_rule_writes_its_seat_move_and_a_move_from_the_pre_fill_revision_is_refused() {
        use crate::fabric_handover::elect;
        use rdm_core::Seat;
        let d = tempdir();
        let s = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::mint()).await.unwrap();
        let book = rdm_transport::membership::DigestBook::default();
        let holder = |b: &rdm_core::handover::HandoverBirth, revision: u64| rdm_core::SeatHolder { mesh: b.mesh.clone(), node_id: b.node_id.clone(), incarnation: b.incarnation.clone(), revision };
        let (p, n, m) = (birth("100000000000", "p"), birth("200000000000", "n"), birth("300000000000", "m"));
        let none = crate::election::Incumbency::default();
        let day0 = elect(&s, &book.seats, &p, &none, vec![(Seat::MeshPrimary, holder(&p, 1)), (Seat::FabricPrimary, holder(&p, 1))]).await;
        assert_eq!(day0.len(), 2, "day 0's first seating is committed and announced");
        let moves = s.seat_moves().await.unwrap();
        assert_eq!((moves.len(), moves[0].row.cause.clone(), moves[0].row.seats.iter().map(|x| (x.from_revision, x.revision, x.from.clone())).collect::<Vec<_>>()), (1, MoveCause::Election, vec![(0, 1, None), (0, 1, None)]), "one election row, both seats from revision 0 and no holder");
        assert!(matches!(commit_seat_move(&s, &transfer("node.stop:mesh1.admin.1", 1, None).seat_move()).await.unwrap(), MoveCommit::Committed(SeatMove { t: 2, .. })), "P hands mesh1's seat to N at revision 2");
        // N's exit is proven: the rule fills the vacancy with M, from N's record at revision 2.
        let decided = crate::election::Incumbency { meshes: [("mesh1".to_string(), holder(&n, 2))].into(), ..Default::default() };
        let filled = elect(&s, &book.seats, &m, &decided, vec![(Seat::MeshPrimary, holder(&m, 3))]).await;
        assert_eq!(filled, vec![(Seat::MeshPrimary, holder(&m, 3))], "the fill is committed before it is announced");
        let row = s.seat_moves().await.unwrap().pop().unwrap();
        assert_eq!((row.t, row.row.cause.clone(), row.row.seats[0].from_revision, row.row.seats[0].revision, row.row.seats[0].from.clone(), row.row.by.clone()), (3, MoveCause::Election, 2, 3, Some(n.clone()), m.clone()), "the fill's row: seat-moves/3, from N at 2 to M at 3, written by M");
        assert!(d.join(SEAT_MOVES_DIR).join(format!("{}.json", move_key(3, &m))).is_file());
        // A move computed against the pre-fill revision is refused by the fold.
        let mut stale = transfer("node.restart:mesh1.admin.2", 2, None);
        stale.holder = n.clone();
        stale.by = n.clone();
        assert!(matches!(commit_seat_move(&s, &stale.seat_move()).await.unwrap(), MoveCommit::Refused(why) if why.contains("mesh-primary") && why.contains("from revision 2") && why.contains("left it at revision 3")));
        let late = elect(&s, &book.seats, &p, &decided, vec![(Seat::MeshPrimary, holder(&p, 3))]).await;
        assert!(late.is_empty(), "an election computed against the pre-fill view commits nothing and announces nothing");
        assert_eq!(book.seats.mesh("mesh1"), Some(holder(&m, 3)), "its refusal folds the moves into the seat book");
        assert_eq!(s.seat_moves().await.unwrap().len(), 3, "nothing else was written");
    }

    /// CONTRACT (rulings INDEX Q109: "storage stores and returns what it is given, and decides
    /// nothing"; "the writer, node-admin, folds every earlier move and decides"; "every reader folds the
    /// stored rows the same way, and a row that does not follow the fold is ignored by every reader,
    /// refused by no storage"): a row written straight to the store that does not follow the fold (a
    /// seat moved from a revision the moves already left) is stored as given, in both stores; every
    /// reader's fold ignores it, so no transfer reads it; the writer's next row goes past it at the
    /// next `t`. A row whose revision does not step by one is refused by the writer and not written.
    /// Must NOT happen: a store refusing a row, a reader taking an ignored row, a write over a stored key.
    #[tokio::test]
    async fn a_row_that_does_not_follow_the_fold_is_stored_and_every_reader_ignores_it() {
        let d = tempdir();
        let file = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::mint()).await.unwrap();
        let memory = MemoryFabricStorage::new();
        for store in [&file as &dyn FabricStorage, &memory] {
            committed(commit_seat_move(store, &transfer("node.stop:mesh1.admin.1", 0, None).seat_move()).await.unwrap());
            // A superseded writer's row, from the revision the first move already left.
            let behind = SeatMove { t: 2, row: transfer("node.restart:mesh1.admin.1", 0, None).seat_move() };
            store.put_seat_move(&behind).await.unwrap();
            assert_eq!(store.seat_moves().await.unwrap().len(), 2, "the store holds the row it was given");
            assert_eq!(folded_moves(store).await.unwrap().iter().map(|m| m.t).collect::<Vec<_>>(), vec![1], "every reader's fold ignores it");
            assert_eq!(seat_transfer(store, "node.restart:mesh1.admin.1").await.unwrap(), None, "no transfer reads it");
            let next = committed(commit_seat_move(store, &transfer("node.stop:mesh1.admin.2", 1, None).seat_move()).await.unwrap());
            assert_eq!(next.t, 3, "the writer's next row goes past the stored key");
            let mut bad = transfer("node.stop:mesh1.admin.3", 2, None).seat_move();
            bad.seats[0].revision = 6;
            assert!(matches!(commit_seat_move(store, &bad).await.unwrap(), MoveCommit::Refused(why) if why.contains("steps by one")));
            assert_eq!(store.seat_moves().await.unwrap().len(), 3, "a refused row is not written");
        }
    }

    /// CONTRACT (Q109: "the row's key names its writer's birth with T", so no write replaces another;
    /// node-primary-update.md: "inserted at the next `T` once the writer has folded every earlier
    /// move"): one writer's commits made at once, each moving its own seat, each stand at their own
    /// `t`, in both stores. Must NOT happen: two of them folded at one end and written at one key, the
    /// later replacing the earlier.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_writers_commits_made_at_once_each_stand_at_their_own_t() {
        let d = tempdir();
        let file: std::sync::Arc<dyn FabricStorage> = std::sync::Arc::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::mint()).await.unwrap());
        let memory: std::sync::Arc<dyn FabricStorage> = std::sync::Arc::new(MemoryFabricStorage::new());
        let by = birth("100000000000", "p");
        for store in [file, memory] {
            let commits = (1..=16u64)
                .map(|i| {
                    let (store, by) = (store.clone(), by.clone());
                    tokio::spawn(async move {
                        let seat = MovedSeat { seat: rdm_core::seat::transfer::Seat::MeshPrimary { mesh: format!("mesh{i}") }, from_revision: 0, revision: 1, holder: Some(by.clone()), from: None };
                        committed(commit_seat_move(&*store, &SeatMoveRow { seats: vec![seat], cause: MoveCause::Election, by }).await.unwrap()).t
                    })
                })
                .collect::<Vec<_>>();
            let mut ts = Vec::new();
            for c in commits {
                ts.push(c.await.unwrap());
            }
            ts.sort();
            assert_eq!(ts, (1..=16).collect::<Vec<u64>>(), "each commit took its own t");
            assert_eq!(folded_moves(&*store).await.unwrap().len(), 16, "every row stands: none replaced another");
        }
    }

    #[tokio::test]
    async fn the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name() {
        let d = tempdir();
        let s = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
        // The fabric seat holder: no seat record governs this fixture's writer.
        s.set_seat_source(crate::fabric_builds::SeatSource::unrecorded());
        let fabric = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
        assert_eq!(crate::fabric_storage::claim_identity(&s, &fabric).await.unwrap(), Ok(FabricClaim::Written));
        let reopened = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap();
        assert_eq!(reopened.fabric().await.unwrap(), Some(fabric));
        std::fs::write(d.join(FABRIC_DIR).join("identity.json"), br#"{"format":"fabric-identity/9","record":{}}"#).unwrap();
        assert!(matches!(reopened.fabric().await, Err(FabricStorageError::Unrecognised { .. })));
        std::fs::write(d.join(FABRIC_DIR).join("identity.json"), b"not json").unwrap();
        assert!(matches!(reopened.fabric().await, Err(FabricStorageError::Unrecognised { .. })));
    }

    #[tokio::test]
    async fn the_file_storage_refuses_the_retired_whole_record_by_name() {
        let d = tempdir();
        std::fs::create_dir_all(d.join(FABRIC_DIR)).unwrap();
        std::fs::write(d.join(FABRIC_DIR).join("fabric.json"), br#"{"format":"fabric-record/1","record":{}}"#).unwrap();
        let err = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap_err();
        assert!(matches!(&err, FabricStorageError::Unrecognised { file, reason } if file.ends_with("fabric.json") && reason.contains("fabric-record/1")), "{err}");
    }

    /// CONTRACT (proposal Part C2: "The per-seat rows under `fabric/seats` go"; Part D: the handover
    /// stages of fabric.primary.update go; CLAUDE.md: "a reader that does not RECOGNISE a value must
    /// REFUSE with a named reason"): a store root an older build left a per-seat row (`fabric/seats/`,
    /// `fabric-seat/1`) or a handover stage (`fabric/handovers/`, `fabric-handover/1`) on is refused by
    /// name, the row and the retired format named, and is never read as a root with no seat rows.
    #[tokio::test]
    async fn the_file_storage_refuses_a_retired_seat_or_handover_row_by_name() {
        for (dir, file, format) in [("seats", "fabric-primary--00000000000000000007-100000000000.json", "fabric-seat/1"), ("handovers", "fabric-primary-handover_a-sent.json", "fabric-handover/1")] {
            let d = tempdir();
            std::fs::create_dir_all(d.join(FABRIC_DIR).join(dir)).unwrap();
            std::fs::write(d.join(FABRIC_DIR).join(dir).join(file), format!(r#"{{"format":"{format}","record":{{}}}}"#)).unwrap();
            let err = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap_err();
            assert!(matches!(&err, FabricStorageError::Unrecognised { file: f, reason } if f.ends_with(file) && reason.contains(format)), "{err}");
        }
    }

    /// CONTRACT (R-BI3: "The Fabric row carries no pointer to the newest Build"): the pointer rows
    /// (`fabric/pointers/`, format `fabric-pointer/1`) are retired. A store root an older build left
    /// them on is refused by name, the row and the retired format named, and is never read as a
    /// Fabric with no pointer.
    #[tokio::test]
    async fn the_file_storage_refuses_a_retired_pointer_row_by_name() {
        let d = tempdir();
        std::fs::create_dir_all(d.join(FABRIC_DIR).join("pointers")).unwrap();
        std::fs::write(d.join(FABRIC_DIR).join("pointers").join("00000000000000000010-bld-1.json"), br#"{"format":"fabric-pointer/1","record":{"build_id":"3qcfk1kq47m9","submitted_at_ms":10}}"#).unwrap();
        let err = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap_err();
        assert!(matches!(&err, FabricStorageError::Unrecognised { file, reason } if file.ends_with("00000000000000000010-bld-1.json") && reason.contains("fabric-pointer/1")), "{err}");
    }

    /// CONTRACT (node-identity-and-state.md §2.8: the fabric-primary claims the fabric's name "at
    /// its `(path, name)` ... idempotent for the same FabricId and `NameHeld` naming
    /// a different one"): a free key takes the identity; the same Fabric claiming again writes nothing;
    /// another Fabric's claim is refused naming the holder, and the held identity is never rewritten.
    /// The memory store and the file store each run their own check.
    #[tokio::test]
    async fn the_fabric_name_is_claimed_once_by_its_writer_for_both_stores() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, Box::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&tempdir()), &crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap()).await.unwrap())] {
            let first = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            let other = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &first).await.unwrap(), Ok(FabricClaim::Written));
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &first).await.unwrap(), Ok(FabricClaim::Held), "the same Fabric claiming again writes nothing");
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &other).await.unwrap(), Err(FabricNameHeld { name: "fabric1".into(), by: first.fabric_id.clone() }), "another Fabric's claim is refused naming the holder");
            assert_eq!(s.fabric().await.unwrap(), Some(first.clone()), "the held identity is never rewritten");
        }
    }

    /// CONTRACT (R-FN1: a name is unique per (path, kind) for every entity, the fabric included; the
    /// fabric's (path, name) claim sits at the path level, above the per-fabric store roots
    /// (`<store_dir>/fabrics/<name>`), and a second fabric of the same name under that path is refused
    /// `NameHeld { by }`; Q109: the writer reads the claim and writes it; R14 item 1: the per-fabric root is unchanged, the index sits beside it;
    /// CLAUDE.md: a refused claim writes nothing of the entity): under one store_dir, fabric A claims
    /// fabric1 and its state-commit publishes the claim. Fabric B, another FabricId with its own root
    /// under the same store_dir, claims fabric1: refused naming A, and nothing of B is written, neither
    /// at the index nor under B's root, even once B's scratchpad is published. Must NOT happen: a second fabric of one name under one store_dir taking the name because its
    /// identity row lives under its own root.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_second_fabric_of_a_held_name_under_one_store_dir_is_refused_naming_the_holder() {
        let d = tempdir();
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric = |name: &str| FabricIdentity { fabric_id: FabricId::mint(), name: name.into() };
        let (a, b) = (fabric("fabric1"), fabric("fabric1"));
        let open = |id: &FabricIdentity| {
            let (store, id) = (store.clone(), id.fabric_id.clone());
            async move {
                let root = crate::store_root::StoreRoot::open(&store, &id).unwrap();
                let s = FileFabricStorage::open(&root, &crate::model::IncarnationId::mint()).await.unwrap();
                (root, s)
            }
        };
        let (root_a, store_a) = open(&a).await;
        assert_eq!(crate::fabric_storage::claim_identity(&store_a, &a).await.unwrap(), Ok(FabricClaim::Written), "A claims fabric1");
        root_a.scratchpad().expect("a root with no Fabric row holds a scratchpad").publish().await.unwrap();
        let (root_b, store_b) = open(&b).await;
        let capture = crate::span_capture::Capture::new("i143-3089-fabric-name-index");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        assert_eq!(crate::admin::claim_fabric_name(&store_b, &b, &crate::model::NodeId::mint()).await, Ok(Err(FabricNameHeld { name: "fabric1".into(), by: a.fabric_id.clone() })), "B's claim of fabric1 under the same store_dir is refused naming A");
        let spans = capture.named("rdm.node_admin.fabric.create.via-fabric-row");
        assert_eq!(spans.len(), 1, "{spans:?}");
        assert_eq!((spans[0].attributes.get("outcome").map(String::as_str), spans[0].attributes.get("holder").cloned()), (Some("name-held"), Some(a.fabric_id.to_string())), "the claim's span names the refusal and its holder: {spans:?}");
        eprintln!("span rdm.node_admin.fabric.create.via-fabric-row trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
        let published_b = root_b.scratchpad().expect("a root with no Fabric row holds a scratchpad").publish().await.unwrap();
        assert_eq!(published_b.written(), 0, "nothing of B is written: {published_b:?}");
        assert!(!store.join(b.fabric_id.to_string()).join(FABRIC_DIR).join("identity.json").exists(), "B's identity row is not written under its root");
        let index: serde_json::Value = serde_json::from_slice(&std::fs::read(store.join("fabrics").join("fabric1.json")).expect("the name's index row is on the store_dir")).unwrap();
        assert_eq!(index["record"]["fabric_id"].as_str(), Some(a.fabric_id.to_string().as_str()), "the index row at <store_dir>/fabrics/fabric1 is A's: {index}");
    }

    /// CONTRACT (R-FN1: "a deleted fabric releases it"; the hook s59's fabric.delete calls): a
    /// fabric's release of its name lets the name go only when the claim is its own: another Fabric's
    /// release of it is refused (`false`) and the claim stands; the holder's release removes it, and
    /// another Fabric then claims the name. The memory store (its identity slot) and the file store
    /// (the index at `<store_dir>/fabrics/<name>`) each run their own check.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_fabrics_release_lets_go_of_its_own_name_claim_only() {
        let d = tempdir();
        for (s, store) in [(Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, "memory"), (Box::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::mint()).await.unwrap()), "file")] {
            let (first, other) = (FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }, FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() });
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &first).await.unwrap(), Ok(FabricClaim::Written), "{store}");
            assert!(!crate::fabric_storage::release_name(&*s, &other).await.unwrap(), "{store}: another Fabric's release of the name is refused");
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &other).await.unwrap(), Err(FabricNameHeld { name: "fabric1".into(), by: first.fabric_id.clone() }), "{store}: the claim stands");
            assert!(crate::fabric_storage::release_name(&*s, &first).await.unwrap(), "{store}: the holder's release lets the name go");
            if store == "file" {
                assert!(!d.join(crate::store_root::FABRIC_NAMES).join("fabric1.json").exists(), "the index row is removed");
            }
        }
    }

    fn tempdir() -> std::path::PathBuf {
        crate::test_support::tempdir("fabric-storage")
    }
}
