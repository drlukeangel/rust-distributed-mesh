//! `fabric.storage`: node-admin's Fabric control state behind a storage boundary
//! (fabric-mesh-lifecycle.md §11.1, §13).
//!
//! It holds the Fabric's identity row and, once a fabric shutdown begins, the [`FabricShutdown`]
//! record. The Fabric row carries no pointer to the newest Build (R-BI3): the
//! current Build is read from the Build entities themselves
//! ([`crate::build_state::BuildStateAdapter::current_build`]). `fabric/` on the fabric's store root
//! has one writer, the fabric-primary (R14 item 2: "Fabric-primary: `fabric/`. ... Keys never
//! overlap across writers"): it writes the identity and the shutdown it initiates.
//! Every other admin reads them there, and holds the identity it learns before the fabric's first
//! state-commit (its join answer, the Build topic) as its own runtime state, never as a row here. A
//! shutdown is never deleted while its Fabric lives, and an admin that starts on a store root holding
//! one comes up frozen.
//!
//! node.primary.update and node.fabric-primary.update leave ONE [`SeatTransferRow`], written by the
//! holder of the seats as its mesh's row writer (the approved proposal
//! node-primary-update-fabric-mesh-and-node-type.md, Part C2: "The one committed row is the holder's
//! transfer row under its own mesh on the fabric's shared store root,
//! `meshes/<holder mesh>/seat-transfers/<operation>` ... whether one seat or both move; an outside
//! successor of the fabric seat reads it there. The per-seat rows under `fabric/seats` go ... the
//! leadership view is folded from the transfer rows"). The row is the commit and the fence: the
//! holder's authority ends at its epochs, an identical call replays it, and a resuming executor or a
//! restarted holder point-reads it there. The per-seat rows (`fabric/seats/`, `fabric-seat/1`) and the
//! handover stages of the retired fabric.primary.update (`fabric/handovers/`, `fabric-handover/1`) are
//! retired: a store root holding one is refused by name.
//!
//! The Fabric's own state is two kinds of row, each its own fact written on its own and never read
//! back to be written again: the [`FabricIdentity`] (written once) and the [`FabricShutdown`] (first
//! one kept). The pointer rows an older build wrote (`fabric/pointers/`, `fabric-pointer/1`) are
//! retired: a store root holding one is refused by name.
//!
//! Two implementations: [`MemoryFabricStorage`] for runs that need no restart survival, and
//! [`FileFabricStorage`], one record per file on the fabric's store root, each write a temp file,
//! fsync and atomic rename. A file this build does not recognise is refused by name, never read as
//! a value.

use async_trait::async_trait;
use crate::model::FabricId;
use serde::{Deserialize, Serialize};

use std::sync::Mutex;

/// The Fabric's immutable identity row: written once, by Day 0 or by the admin that first hears it,
/// and never rewritten.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricIdentity {
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// The fabric's name.
    pub name: String,
}

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

/// The one row the holder of the seats writes when it commits node.primary.update (proposal Part C1
/// step 5: "The writer writes the one seat row at the next epoch(s) naming the successor. This is the
/// commit"): every seat it moves to its successor at the next epoch, and the update it commits. The row
/// is the commit and the fence: the holder's authority ends at these epochs, the successor installs
/// from it, and an identical call replays it. Its key is the operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatTransferRow {
    /// The update's operation identity (`node.primary.update:<build>:<operation>`), the row's key.
    pub operation: String,
    /// The fabric.
    pub fabric: String,
    /// The Build whose stop, delete, restart, replace or connections delete the update precedes.
    pub build_id: String,
    /// That attempt.
    pub attempt: u32,
    /// The former holder: the exact birth that wrote the row.
    pub holder: rafka_node_rpc_contract::handover::HandoverBirth,
    /// The successor the holder's pre-check named, which installs from the row.
    pub successor: rafka_node_rpc_contract::handover::HandoverBirth,
    /// The seats and the revisions the holder held them at.
    pub expected: Vec<rafka_node_rpc_contract::handover::SeatRevision>,
    /// Every seat moved, at its committed revision.
    pub seats: Vec<rafka_mesh_entity::seat::transfer::CommittedSeat>,
}

/// The key of the seat-transfer row of `operation`.
fn transfer_key(operation: &str) -> String {
    operation.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect()
}

impl SeatTransferRow {
    fn key(&self) -> String {
        transfer_key(&self.operation)
    }
}

/// A fabric shutdown in progress. `initiated_by` names the fabric-primary that began it;
/// `initiated_at_ms` is a diagnostic and takes no part in authority or ordering. The record holds no
/// drain progress: every admin derives the freeze barrier (every live admin `Draining`) and runtime
/// disappearance from its own view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricShutdown {
    /// The admin that initiated the shutdown.
    pub initiated_by: String,
    /// The node id of that admin.
    pub initiated_by_node_id: String,
    /// When the shutdown was initiated, in milliseconds since the Unix epoch.
    pub initiated_at_ms: u64,
}

/// Why a `fabric.storage` read or write failed.
pub use crate::record_store::StorageError as FabricStorageError;

/// node-admin's Fabric control state: each kind of row written on its own and never read back to be
/// written again.
#[async_trait]
pub trait FabricStorage: Send + Sync {
    /// The Fabric's identity row; `None` while no identity is held.
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError>;
    /// Claim the fabric's name for `identity.fabric_id` and keep `identity` as the Fabric's identity row:
    /// the claim is here, inside the adapter (node-identity-and-state.md §2.8): insert-and-fail on the
    /// fabric's `(path, name)`, at the path level above the per-fabric roots (R-FN1:
    /// `<store_dir>/fabrics/<name>`), then the identity row at the fabric's key. A free key takes the
    /// row (`Written`); this Fabric's own row there is idempotent (`Held`, nothing written); another
    /// Fabric holds it: `FabricNameHeld` naming that Fabric, and nothing of this fabric is written. A
    /// held claim is never rewritten.
    async fn claim_identity(&self, identity: &FabricIdentity) -> Result<Result<FabricClaim, FabricNameHeld>, FabricStorageError>;
    /// fabric.delete's release of the fabric's name (R-FN1: "a deleted fabric releases it"): the
    /// name's claim goes when it is `identity`'s Fabric's, and another Fabric's claim there is left as
    /// it is. Returns whether it went. The hook s59's fabric.delete calls; nothing else releases a
    /// fabric's name.
    async fn release_name(&self, identity: &FabricIdentity) -> Result<bool, FabricStorageError>;
    /// The shutdown held, when one is.
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError>;
    /// Hold `shutdown` if none is held (insert-and-fail on its key): the first shutdown held is
    /// kept, a later one never replaces it, and it is never removed. Returns the record now held.
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError>;
    /// Put the one row of a node.primary.update this admin committed as the holder, under its own mesh
    /// (the row's holder's): a blind put of its own key (the operation). The caller refuses another
    /// payload for the operation before it writes.
    async fn put_seat_transfer(&self, row: &SeatTransferRow) -> Result<(), FabricStorageError>;
    /// The seat-transfer row the primary of `holder_mesh` committed for `operation`: a point read of
    /// its key; `None` when none was ever written (the transfer never happened).
    async fn seat_transfer(&self, holder_mesh: &str, operation: &str) -> Result<Option<SeatTransferRow>, FabricStorageError>;
    /// Every seat-transfer row on the store, every mesh's.
    async fn seat_transfers(&self) -> Result<Vec<SeatTransferRow>, FabricStorageError>;
}

/// A fabric store held in memory.
#[derive(Debug, Default)]
pub struct MemoryFabricStorage {
    identity: Mutex<Option<FabricIdentity>>,
    shutdown: Mutex<Option<FabricShutdown>>,
    transfers: Mutex<Vec<SeatTransferRow>>,
}

impl MemoryFabricStorage {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl FabricStorage for MemoryFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError> {
        Ok(self.identity.lock().unwrap().clone())
    }
    async fn claim_identity(&self, identity: &FabricIdentity) -> Result<Result<FabricClaim, FabricNameHeld>, FabricStorageError> {
        // The look and the insert are one step under the identity's lock.
        let mut held = self.identity.lock().unwrap();
        let claimed = claim_fabric_against(held.as_ref(), identity);
        if claimed == Ok(FabricClaim::Written) {
            *held = Some(identity.clone());
        }
        Ok(claimed)
    }
    async fn release_name(&self, identity: &FabricIdentity) -> Result<bool, FabricStorageError> {
        // The identity slot is this store's claim of the name: it goes only when it is this Fabric's.
        let mut held = self.identity.lock().unwrap();
        let ours = held.as_ref().is_some_and(|h| h.fabric_id == identity.fabric_id);
        if ours {
            *held = None;
        }
        Ok(ours)
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        Ok(self.shutdown.lock().unwrap().clone())
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        Ok(self.shutdown.lock().unwrap().get_or_insert_with(|| shutdown.clone()).clone())
    }
    async fn put_seat_transfer(&self, row: &SeatTransferRow) -> Result<(), FabricStorageError> {
        let mut rows = self.transfers.lock().unwrap();
        rows.retain(|r| (r.holder.mesh.as_str(), r.key()) != (row.holder.mesh.as_str(), row.key()));
        rows.push(row.clone());
        Ok(())
    }
    async fn seat_transfer(&self, holder_mesh: &str, operation: &str) -> Result<Option<SeatTransferRow>, FabricStorageError> {
        let key = transfer_key(operation);
        Ok(self.transfers.lock().unwrap().iter().find(|r| r.holder.mesh == holder_mesh && r.key() == key).cloned())
    }
    async fn seat_transfers(&self) -> Result<Vec<SeatTransferRow>, FabricStorageError> {
        Ok(self.transfers.lock().unwrap().clone())
    }
}

/// The directory inside the fabric's store root.
pub const FABRIC_DIR: &str = "fabric";
/// The retired pointer rows (`fabric-pointer/1`, one per move of the pointer an older build kept on
/// the Fabric row): a store root holding one is refused by name (R-BI3).
const RETIRED_POINTERS_DIR: &str = "fabric/pointers";
const RETIRED_POINTER_FORMAT: &str = "fabric-pointer/1";
/// The retired per-seat rows (`fabric-seat/1`): the leadership view is folded from the transfer rows.
const RETIRED_SEATS_DIR: &str = "fabric/seats";
const RETIRED_SEAT_FORMAT: &str = "fabric-seat/1";
/// The retired stages of fabric.primary.update (`fabric-handover/1`): node.fabric-primary.update
/// leaves one transfer row.
const RETIRED_HANDOVERS_DIR: &str = "fabric/handovers";
const RETIRED_HANDOVER_FORMAT: &str = "fabric-handover/1";
/// Under `meshes/<holder mesh>/` on the store root: the seat-transfer rows that mesh's primary committed.
const TRANSFERS_DIR: &str = "seat-transfers";
const TRANSFER_FORMAT: &str = "fabric-seat-transfer/1";
const IDENTITY_KEY: &str = "identity";
const SHUTDOWN_KEY: &str = "shutdown";
/// The retired whole-record file (`fabric.json`, format `fabric-record/1`): identity and a Build in
/// one row. A data dir holding it is refused by name.
const RETIRED_RECORD_KEY: &str = "fabric";
const IDENTITY_FORMAT: &str = "fabric-identity/1";
/// A fabric's claim of its name at `<store_dir>/fabrics/<name>` (R-FN1): the claiming Fabric's identity.
const NAME_FORMAT: &str = "fabric-name/1";
const SHUTDOWN_FORMAT: &str = "fabric-shutdown/1";

/// One row per file under `<store root>/fabric/` (`crate::store_root`): `identity.json` and
/// `shutdown.json` (`crate::record_store`); the seat-transfer rows under
/// `<store root>/meshes/<holder mesh>/seat-transfers/`.
#[derive(Debug)]
pub struct FileFabricStorage {
    /// `<store_dir>/fabrics/`: the fabrics' name index above the per-fabric roots (R-FN1), where this
    /// fabric's claim of its name is the first of its rows.
    names: crate::record_store::FileRecords,
    records: crate::record_store::FileRecords,
    /// `<store root>/meshes/`: each mesh's seat-transfer rows under it.
    meshes_dir: std::path::PathBuf,
    /// The store root: the seat-transfer stores open on it as they are first used.
    root: crate::store_root::StoreRoot,
    writer: crate::model::IncarnationId,
    /// The seat-transfer store of each mesh opened so far.
    transfers: Mutex<std::collections::BTreeMap<String, std::sync::Arc<crate::record_store::FileRecords>>>,
    /// Where this writer reads whether it holds the fabric seat: the committed fabric seat record
    /// (`crate::admin::fabric_seat_source`). Set once at start.
    seat: SeatSlot,
}

/// The fabric seat source a [`FileFabricStorage`] reads when its held `fabric/` rows are published.
#[derive(Default, Clone)]
struct SeatSlot(std::sync::Arc<std::sync::OnceLock<crate::fabric_builds::SeatSource>>);

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

impl std::fmt::Debug for SeatSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.get().is_some() { "SeatSlot(set)" } else { "SeatSlot(unset)" })
    }
}

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
        refuse_retired(&own_data_dir.join(RETIRED_SEATS_DIR), &format!("the per-seat row format {RETIRED_SEAT_FORMAT} is retired: the leadership view is folded from the seat-transfer rows under meshes/<holder mesh>/seat-transfers/")).await?;
        refuse_retired(&own_data_dir.join(RETIRED_HANDOVERS_DIR), &format!("the handover stage format {RETIRED_HANDOVER_FORMAT} is retired: node.fabric-primary.update leaves one seat-transfer row under its holder's mesh")).await?;
        Ok(Self {
            names,
            records,
            meshes_dir: own_data_dir.join("meshes"),
            root: root.clone(),
            writer: writer.clone(),
            transfers: Mutex::new(Default::default()),
            seat: SeatSlot::default(),
        })
    }

    /// The committed fabric seat record this writer reads (set once). The `fabric/` rows it holds in
    /// its scratchpad (its Fabric identity, written while it held the fabric seat)
    /// are published only while the record still says it does; a writer whose seat moved before the
    /// publication drops them, named (R14 item 2).
    pub fn set_seat_source(&self, seat: crate::fabric_builds::SeatSource) {
        let _ = self.seat.0.set(seat);
        for store in self.fabric_primary_records() {
            let (slot, writer) = (self.seat.clone(), self.writer.clone());
            store.set_publish_gate(std::sync::Arc::new(move || slot.open(&writer)));
        }
    }

    /// The seat-transfer rows of `mesh`: `meshes/<mesh>/seat-transfers/`.
    async fn transfers_of(&self, mesh: &str) -> Result<std::sync::Arc<crate::record_store::FileRecords>, FabricStorageError> {
        if let Some(r) = self.transfers.lock().unwrap().get(mesh) {
            return Ok(r.clone());
        }
        let opened = std::sync::Arc::new(self.root.records(&self.meshes_dir.join(mesh), TRANSFERS_DIR, &self.writer).await?);
        Ok(self.transfers.lock().unwrap().entry(mesh.to_string()).or_insert(opened).clone())
    }

    /// Every mesh directory on the root that holds seat-transfer rows.
    fn transfer_meshes(&self) -> Result<Vec<String>, FabricStorageError> {
        let entries = match std::fs::read_dir(&self.meshes_dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(FabricStorageError::Io { file: self.meshes_dir.display().to_string(), reason: e.to_string() }),
        };
        let mut meshes: Vec<String> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().join(TRANSFERS_DIR).is_dir())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        meshes.sort();
        Ok(meshes)
    }

    /// The records the fabric-primary seat writes (R14 item 2): the fabric's claim of its name, and
    /// the Fabric identity and shutdown. What a node.fabric-primary.update moving that seat cuts off,
    /// and what a publication writes only while the committed fabric seat record names this writer.
    /// Seat-transfer rows live under the holder's own mesh.
    pub fn fabric_primary_records(&self) -> [&crate::record_store::FileRecords; 2] {
        [&self.names, &self.records]
    }
}

#[async_trait]
impl FabricStorage for FileFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricIdentity>, FabricStorageError> {
        self.records.read(IDENTITY_KEY, IDENTITY_FORMAT).await
    }
    async fn claim_identity(&self, identity: &FabricIdentity) -> Result<Result<FabricClaim, FabricNameHeld>, FabricStorageError> {
        // R-FN1: the fabric's (path, name) claim, an insert-and-fail at `<store_dir>/fabrics/<name>`
        // above the per-fabric roots, before anything of the fabric is written under its own root.
        let named = match claimed_insert(&self.names, &identity.name, NAME_FORMAT, identity).await? {
            Ok(c) => c,
            Err(held) => return Ok(Err(held)),
        };
        // The identity row at the fabric's own key, under its root.
        let own = match claimed_insert(&self.records, IDENTITY_KEY, IDENTITY_FORMAT, identity).await? {
            Ok(c) => c,
            Err(held) => return Ok(Err(held)),
        };
        Ok(Ok(if named == FabricClaim::Written || own == FabricClaim::Written { FabricClaim::Written } else { FabricClaim::Held }))
    }
    async fn release_name(&self, identity: &FabricIdentity) -> Result<bool, FabricStorageError> {
        let ours = self.names.read::<FabricIdentity>(&identity.name, NAME_FORMAT).await?.is_some_and(|h| h.fabric_id == identity.fabric_id);
        if ours {
            let fabric_id = identity.fabric_id.clone();
            self.names.remove_released(&identity.name, NAME_FORMAT, move |h: &FabricIdentity| h.fabric_id == fabric_id).await?;
        }
        Ok(ours)
    }
    async fn shutdown(&self) -> Result<Option<FabricShutdown>, FabricStorageError> {
        self.records.read(SHUTDOWN_KEY, SHUTDOWN_FORMAT).await
    }
    async fn put_shutdown(&self, shutdown: &FabricShutdown) -> Result<FabricShutdown, FabricStorageError> {
        self.records.insert(SHUTDOWN_KEY, SHUTDOWN_FORMAT, shutdown).await?;
        self.records.read(SHUTDOWN_KEY, SHUTDOWN_FORMAT).await?.ok_or_else(|| FabricStorageError::Io { file: "shutdown.json".into(), reason: "the shutdown row vanished after its insert".into() })
    }
    async fn put_seat_transfer(&self, row: &SeatTransferRow) -> Result<(), FabricStorageError> {
        self.transfers_of(&row.holder.mesh).await?.write(&row.key(), TRANSFER_FORMAT, row).await
    }
    async fn seat_transfer(&self, holder_mesh: &str, operation: &str) -> Result<Option<SeatTransferRow>, FabricStorageError> {
        self.transfers_of(holder_mesh).await?.read(&transfer_key(operation), TRANSFER_FORMAT).await
    }
    async fn seat_transfers(&self) -> Result<Vec<SeatTransferRow>, FabricStorageError> {
        let mut all = Vec::new();
        for mesh in self.transfer_meshes()? {
            all.extend(self.transfers_of(&mesh).await?.list::<SeatTransferRow>(TRANSFER_FORMAT).await?);
        }
        Ok(all)
    }
}

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
    crate::record_store::blocking_io(move || {
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

async fn claimed_insert(records: &crate::record_store::FileRecords, key: &str, format: &str, identity: &FabricIdentity) -> Result<Result<FabricClaim, FabricNameHeld>, FabricStorageError> {
    if key.is_empty() || key.starts_with('.') || key.contains(['/', '\\']) {
        return Err(FabricStorageError::Io { file: format!("{}/{key}", records.store_name()), reason: format!("the fabric name {key:?} is not one path segment, so it keys no claim of the fabric's name") });
    }
    let claim = identity.clone();
    if records.insert_claimed(key, format, identity, move |standing| claim_fabric_against(standing, &claim).map(|c| c == FabricClaim::Written).map_err(|held| held.to_string())).await? {
        return Ok(Ok(FabricClaim::Written));
    }
    let held: FabricIdentity = records.read(key, format).await?.ok_or_else(|| FabricStorageError::Io { file: format!("{}/{key}.json", records.store_name()), reason: "the claim's insert met a live key that no read finds".into() })?;
    Ok(claim_fabric_against(Some(&held), identity))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shutdown() -> FabricShutdown {
        FabricShutdown { initiated_by: "mesh1.admin.1".into(), initiated_by_node_id: "n1".into(), initiated_at_ms: 10 }
    }

    #[tokio::test]
    async fn a_held_shutdown_is_never_replaced_or_removed() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, {
            let d = tempdir();
            Box::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("test")).unwrap()).await.unwrap())
        }] {
            assert_eq!(s.shutdown().await.unwrap(), None);
            assert_eq!(s.put_shutdown(&shutdown()).await.unwrap(), shutdown());
            let other = FabricShutdown { initiated_by: "mesh2.admin.1".into(), ..shutdown() };
            assert_eq!(s.put_shutdown(&other).await.unwrap(), shutdown(), "the first initiation is kept");
        }
    }

    /// CONTRACT (proposal Part C2: "The one committed row is the holder's transfer row under its own
    /// mesh on the fabric's shared store root, `meshes/<holder mesh>/seat-transfers/<operation>` ...
    /// whether one seat or both move"): a node.primary.update moving both seats is ONE row in the
    /// `fabric-seat-transfer/1` shape, keyed by its operation under the holder's mesh, and a reopened
    /// store reads it back. A format this build does not know is refused.
    #[tokio::test]
    async fn a_seat_transfer_is_one_row_under_its_holders_mesh() {
        use rafka_mesh_entity::Seat::{FabricPrimary, MeshPrimary};
        let birth = |node: &str, inc: &str| rafka_node_rpc_contract::handover::HandoverBirth { mesh: "mesh1".into(), node_id: rafka_mesh_entity::NodeId::parse(node).unwrap(), incarnation: rafka_mesh_entity::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60(inc)).unwrap() };
        let n = |revision: u64| rafka_mesh_entity::SeatHolder { mesh: "mesh1".into(), node_id: rafka_mesh_entity::NodeId::parse("200000000000").unwrap(), incarnation: rafka_mesh_entity::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("n")).unwrap(), revision };
        let row = SeatTransferRow {
            operation: "drain-node:mesh1.admin.1".into(),
            fabric: "fabric1".into(),
            build_id: "b1".into(),
            attempt: 2,
            holder: birth("100000000000", "p"),
            successor: birth("200000000000", "n"),
            expected: vec![rafka_node_rpc_contract::handover::SeatRevision { seat: rafka_node_rpc_contract::handover::Seat::MeshPrimary { mesh: "mesh1".into() }, expected_revision: 3 }],
            seats: vec![rafka_mesh_entity::seat::transfer::CommittedSeat::of(MeshPrimary, &n(4)), rafka_mesh_entity::seat::transfer::CommittedSeat::of(FabricPrimary, &n(8))],
        };
        let d = tempdir();
        let s = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("test")).unwrap()).await.unwrap();
        s.put_seat_transfer(&row).await.unwrap();
        let reopened = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("test")).unwrap()).await.unwrap();
        assert_eq!(reopened.seat_transfers().await.unwrap(), vec![row.clone()]);
        // The holder's mesh holds the row (R-SR1=A): a point read of its key there, none elsewhere.
        assert_eq!(reopened.seat_transfer("mesh1", "drain-node:mesh1.admin.1").await.unwrap(), Some(row.clone()));
        assert_eq!(reopened.seat_transfer("mesh2", "drain-node:mesh1.admin.1").await.unwrap(), None);
        assert_eq!(reopened.seat_transfer("mesh1", "stop-node:mesh1.admin.1").await.unwrap(), None);
        assert!(!d.join(FABRIC_DIR).join("seat-transfers").exists(), "a seat-transfer row under fabric/");
        let file = d.join("meshes").join("mesh1").join("seat-transfers").join("drain-node_mesh1_admin_1.json");
        let text = std::fs::read_to_string(&file).unwrap();
        assert!(text.starts_with(r#"{"format":"fabric-seat-transfer/1","record":{"operation":"drain-node:mesh1.admin.1""#), "{text}");
        std::fs::write(&file, br#"{"format":"fabric-seat-transfer/9","record":{}}"#).unwrap();
        assert!(matches!(reopened.seat_transfers().await, Err(FabricStorageError::Unrecognised { .. })));
    }

    #[tokio::test]
    async fn the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name() {
        let d = tempdir();
        let s = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("test")).unwrap()).await.unwrap();
        // The fabric seat holder: no seat record governs this fixture's writer.
        s.set_seat_source(crate::fabric_builds::SeatSource::unrecorded());
        let fabric = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
        assert_eq!(s.claim_identity(&fabric).await.unwrap(), Ok(FabricClaim::Written));
        s.put_shutdown(&shutdown()).await.unwrap();
        let reopened = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("test")).unwrap()).await.unwrap();
        assert_eq!(reopened.fabric().await.unwrap(), Some(fabric));
        assert_eq!(reopened.shutdown().await.unwrap(), Some(shutdown()));
        std::fs::write(d.join(FABRIC_DIR).join("shutdown.json"), br#"{"format":"fabric-shutdown/9","record":{}}"#).unwrap();
        assert!(matches!(reopened.shutdown().await, Err(FabricStorageError::Unrecognised { .. })));
        std::fs::write(d.join(FABRIC_DIR).join("shutdown.json"), b"not json").unwrap();
        assert!(matches!(reopened.shutdown().await, Err(FabricStorageError::Unrecognised { .. })));
    }

    #[tokio::test]
    async fn the_file_storage_refuses_the_retired_whole_record_by_name() {
        let d = tempdir();
        std::fs::create_dir_all(d.join(FABRIC_DIR)).unwrap();
        std::fs::write(d.join(FABRIC_DIR).join("fabric.json"), br#"{"format":"fabric-record/1","record":{}}"#).unwrap();
        let err = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("test")).unwrap()).await.unwrap_err();
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
            let err = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("test")).unwrap()).await.unwrap_err();
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
        let err = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::model::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("test")).unwrap()).await.unwrap_err();
        assert!(matches!(&err, FabricStorageError::Unrecognised { file, reason } if file.ends_with("00000000000000000010-bld-1.json") && reason.contains("fabric-pointer/1")), "{err}");
    }

    /// CONTRACT (node-identity-and-state.md §2.8: the fabric-primary claims the fabric's name "by
    /// insert-and-fail on its `(path, name)` ... idempotent for the same FabricId and `NameHeld` naming
    /// a different one"): a free key takes the identity; the same Fabric claiming again writes nothing;
    /// another Fabric's claim is refused naming the holder, and the held identity is never rewritten.
    /// The memory store and the file store each run their own check.
    #[tokio::test]
    async fn the_fabric_name_is_claimed_once_in_the_adapter_for_both_stores() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, Box::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&tempdir()), &crate::model::IncarnationId::parse(&rafka_mesh_entity::fixture_crockford60("test")).unwrap()).await.unwrap())] {
            let first = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            let other = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            assert_eq!(s.claim_identity(&first).await.unwrap(), Ok(FabricClaim::Written));
            assert_eq!(s.claim_identity(&first).await.unwrap(), Ok(FabricClaim::Held), "the same Fabric claiming again writes nothing");
            assert_eq!(s.claim_identity(&other).await.unwrap(), Err(FabricNameHeld { name: "fabric1".into(), by: first.fabric_id.clone() }), "another Fabric's claim is refused naming the holder");
            assert_eq!(s.fabric().await.unwrap(), Some(first.clone()), "the held identity is never rewritten");
        }
    }

    /// CONTRACT (R-FN1: "A name is unique per (path, kind) for every entity, the fabric included. The
    /// fabric's (path, name) claim is an insert-and-fail at the path level, above the per-fabric store
    /// roots (`<store_dir>/fabrics/<name>`). A second fabric of the same name under that path is refused
    /// `NameHeld { by }`"; R14 item 1: the per-fabric root is unchanged, the index sits beside it;
    /// CLAUDE.md: a refused claim writes nothing of the entity): under one store_dir, fabric A claims
    /// fabric1 and its state-commit publishes the claim. Fabric B, another FabricId with its own root
    /// under the same store_dir, claims fabric1: refused naming A, and nothing of B is written, neither
    /// at the index nor under B's root, even once B's scratchpad is published. Fabric C claims fabric1
    /// at once with A, both before their state-commit: C's claim wins against what C sees, and C's
    /// publication, after A's, decides it again at the index: refused, naming A, the index row A's.
    /// Must NOT happen: a second fabric of one name under one store_dir taking the name because its
    /// identity row lives under its own root.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_second_fabric_of_a_held_name_under_one_store_dir_is_refused_naming_the_holder() {
        let d = tempdir();
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric = |name: &str| FabricIdentity { fabric_id: FabricId::mint(), name: name.into() };
        let (a, b, c) = (fabric("fabric1"), fabric("fabric1"), fabric("fabric1"));
        let open = |id: &FabricIdentity| {
            let (store, id) = (store.clone(), id.fabric_id.clone());
            async move {
                let root = crate::store_root::StoreRoot::open(&store, &id).unwrap();
                let s = FileFabricStorage::open(&root, &crate::model::IncarnationId::mint()).await.unwrap();
                (root, s)
            }
        };
        let ((root_a, store_a), (root_c, store_c)) = (open(&a).await, open(&c).await);
        assert_eq!(store_a.claim_identity(&a).await.unwrap(), Ok(FabricClaim::Written), "A claims fabric1");
        assert_eq!(store_c.claim_identity(&c).await.unwrap(), Ok(FabricClaim::Written), "C claims fabric1 at once with A, before either publishes");
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
        let published_c = root_c.scratchpad().expect("a root with no Fabric row holds a scratchpad").publish().await.unwrap();
        let summary = published_c.summary();
        assert!(summary.contains("fabrics refused 1") && summary.contains(&a.fabric_id.to_string()), "C's held claim of fabric1 is refused at its publication, naming A: {summary}");
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
            assert_eq!(s.claim_identity(&first).await.unwrap(), Ok(FabricClaim::Written), "{store}");
            assert!(!s.release_name(&other).await.unwrap(), "{store}: another Fabric's release of the name is refused");
            assert_eq!(s.claim_identity(&other).await.unwrap(), Err(FabricNameHeld { name: "fabric1".into(), by: first.fabric_id.clone() }), "{store}: the claim stands");
            assert!(s.release_name(&first).await.unwrap(), "{store}: the holder's release lets the name go");
            if store == "file" {
                assert!(!d.join(crate::store_root::FABRIC_NAMES).join("fabric1.json").exists(), "the index row is removed");
            }
        }
    }

    fn tempdir() -> std::path::PathBuf {
        crate::record_store::tempdir("fabric-storage")
    }
}
