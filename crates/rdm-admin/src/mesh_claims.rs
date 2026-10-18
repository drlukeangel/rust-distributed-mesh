//! One attempt in flight per mesh (R-BI7, proposal `build-enitty-updates-plus-otherstuff.md` Part C
//! caveats 2 and 3): "A topology Build's operation on mesh X and mesh X's own drift attempt cannot
//! both be open: whichever claims first on mesh X's rows wins, the other is refused build-in-progress
//! naming the holder."
//!
//! Mesh X's rows are `meshes/<mesh>/claims/` on the fabric's shared store root (R14), and the
//! decision is ONE key there, [`IN_FLIGHT`]: the claimant of an attempt that acts in the mesh
//! inserts it by the filesystem's atomic link (`FileRecords::insert`, insert-and-fail), so two
//! claimants on two nodes (the fabric-primary claiming a topology operation, the mesh-primary
//! claiming its drift attempt) never both hold it. The row names the attempt it holds the mesh for,
//! `(build_id, lane, attempt)`, and the claimant. It is released at that attempt's verdict by a
//! removal that takes it only while it still names that attempt. Nothing is read and written back:
//! the take is an insert of the key, the release a removal of the same key.
//!
//! The row decides; the facts do not move here: the claim fact is appended to the Build log and
//! broadcast on the Build topic only after the take won, exactly as before.

use crate::build::BuildId;
use crate::record_store::FileRecords;
use crate::storage_error::StorageError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The key of a mesh's in-flight row under `meshes/<mesh>/claims/`.
pub const IN_FLIGHT: &str = "in-flight";

/// The attempt a mesh's in-flight row holds the mesh for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlight {
    /// The Build.
    pub build_id: BuildId,
    /// The lane the attempt is numbered in: `None` the fabric lane (a topology Build's attempts),
    /// `Some(mesh)` that mesh's drift lane (`build_state::AttemptOpened::mesh`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<String>,
    /// The attempt, numbered in its lane.
    pub attempt: u32,
    /// The admin that claimed it (a node-admin's path.name): the holder a refusal names.
    pub claimant: String,
    /// The lane key of the attempt's claim (Q35, `crate::build_lanes::LaneKey`): the row is void when
    /// that key holds another entry. `None` for rows held in one process, where no lane is keyed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane_key: Option<String>,
}

impl InFlight {
    /// Whether `self` and `other` are the same attempt, whoever claimed it.
    pub fn same_attempt(&self, other: &InFlight) -> bool {
        self.build_id == other.build_id && self.lane == other.lane && self.attempt == other.attempt
    }
}

/// What a take of a mesh's in-flight row came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Taken {
    /// The row names this attempt: inserted now, or standing from an earlier take of the same attempt.
    Won,
    /// Another attempt holds the mesh: the row standing, naming it and its claimant.
    Held(InFlight),
}

/// The format of a mesh's in-flight row. A row in the retired `mesh-in-flight/1` (no lane key) is
/// refused by name.
const FORMAT: &str = "mesh-in-flight/2";

enum Backing {
    /// `meshes/<mesh>/claims/` on the store root, one store per mesh, opened on its first use.
    Root { root: crate::store_root::StoreRoot, writer: crate::model::IncarnationId, stores: tokio::sync::Mutex<BTreeMap<String, FileRecords>> },
    /// One process's rows.
    Memory(std::sync::Mutex<BTreeMap<String, InFlight>>),
}

/// The in-flight rows of every mesh, as this admin reaches them.
pub struct MeshClaims {
    backing: Backing,
}

impl MeshClaims {
    /// The rows on the fabric's store root, `meshes/<mesh>/claims/`, written by the process born as
    /// `writer`.
    pub fn on_root(root: crate::store_root::StoreRoot, writer: crate::model::IncarnationId) -> Self {
        Self { backing: Backing::Root { root, writer, stores: tokio::sync::Mutex::new(BTreeMap::new()) } }
    }

    /// The rows held in this process only: a control plane built with no store root (a fixture).
    pub fn in_memory() -> Self {
        Self { backing: Backing::Memory(std::sync::Mutex::new(BTreeMap::new())) }
    }

    async fn store(&self, mesh: &str) -> Result<Option<FileRecords>, StorageError> {
        let Backing::Root { root, writer, stores } = &self.backing else { return Ok(None) };
        let mut stores = stores.lock().await;
        if let Some(s) = stores.get(mesh) {
            return Ok(Some(s.clone()));
        }
        // An attempt claim is a Build fact, durable on the root when it is taken, before the
        // fabric's first state-commit too (Q61, build.md §5.2).
        let s = FileRecords::open(&root.meshes(mesh), "claims", writer).await?;
        stores.insert(mesh.to_string(), s.clone());
        Ok(Some(s))
    }

    /// Take `mesh`'s in-flight row for `claim`: the insert of the key decides. A row standing for the
    /// same attempt is that attempt's own (a repeat take, by any claimant): `Won`, and the claim of
    /// the attempt itself is decided by the Build log. A row standing for another attempt is
    /// `Held`, naming it. A row the insert met and the read no longer finds was released between
    /// the two: refused by name, never answered as either outcome.
    pub async fn take(&self, mesh: &str, claim: &InFlight) -> Result<Taken, StorageError> {
        let Some(store) = self.store(mesh).await? else {
            let Backing::Memory(rows) = &self.backing else { unreachable!("a store-root backing opens a store") };
            let mut rows = rows.lock().unwrap_or_else(|e| e.into_inner());
            return Ok(match rows.get(mesh) {
                Some(held) if held.same_attempt(claim) => Taken::Won,
                Some(held) => Taken::Held(held.clone()),
                None => {
                    rows.insert(mesh.to_string(), claim.clone());
                    Taken::Won
                }
            });
        };
        if store.insert(IN_FLIGHT, FORMAT, claim).await? {
            return Ok(Taken::Won);
        }
        match store.read::<InFlight>(IN_FLIGHT, FORMAT).await? {
            Some(held) if held.same_attempt(claim) => Ok(Taken::Won),
            Some(held) => Ok(Taken::Held(held)),
            None => Err(StorageError::Io {
                file: format!("{}/{IN_FLIGHT}.json", store.store_name()),
                reason: format!("mesh {mesh}'s in-flight row was live at the insert for attempt {} of {} and gone at the read after it: its holder's verdict released it between the two; nothing is claimed, and the next claim decides on the free row", claim.attempt, claim.build_id),
            }),
        }
    }

    /// Release `mesh`'s in-flight row held for `attempt`, at that attempt's verdict: the row goes only
    /// while it names that attempt, so a release never removes another attempt's row. Its one writer
    /// is the admin that durably recorded the verdict (the seat that holds the attempt's lane), so no
    /// two releases of one row race.
    pub async fn release(&self, mesh: &str, attempt: &InFlight) -> Result<(), StorageError> {
        let Some(store) = self.store(mesh).await? else {
            let Backing::Memory(rows) = &self.backing else { unreachable!("a store-root backing opens a store") };
            let mut rows = rows.lock().unwrap_or_else(|e| e.into_inner());
            if rows.get(mesh).is_some_and(|held| held.same_attempt(attempt)) {
                rows.remove(mesh);
            }
            return Ok(());
        };
        if !store.read::<InFlight>(IN_FLIGHT, FORMAT).await?.is_some_and(|held| held.same_attempt(attempt)) {
            return Ok(());
        }
        let attempt = attempt.clone();
        store.remove_released::<InFlight>(IN_FLIGHT, FORMAT, move |held| held.same_attempt(&attempt)).await
    }

    /// Point `mesh`'s in-flight row, standing for `row`'s attempt, at `row`'s lane key when it names
    /// another (Q35: the row names its claim's lane key): the standing row goes while it is that exact
    /// row, and `row` is inserted. Nothing when the row names `row`'s key already or another attempt.
    /// A row another attempt inserts between the two is refused by name: this attempt holds the mesh
    /// no more.
    pub async fn repoint(&self, mesh: &str, row: &InFlight) -> Result<(), StorageError> {
        let Some(store) = self.store(mesh).await? else {
            let Backing::Memory(rows) = &self.backing else { unreachable!("a store-root backing opens a store") };
            let mut rows = rows.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(held) = rows.get_mut(mesh).filter(|h| h.same_attempt(row)) {
                held.lane_key = row.lane_key.clone();
            }
            return Ok(());
        };
        let Some(standing) = store.read::<InFlight>(IN_FLIGHT, FORMAT).await?.filter(|h| h.same_attempt(row) && h.lane_key != row.lane_key) else { return Ok(()) };
        let exact = standing.clone();
        store.remove_released::<InFlight>(IN_FLIGHT, FORMAT, move |held| *held == exact).await?;
        if store.insert(IN_FLIGHT, FORMAT, row).await? {
            return Ok(());
        }
        Err(StorageError::Io {
            file: format!("{}/{IN_FLIGHT}.json", store.store_name()),
            reason: format!("mesh {mesh}'s in-flight row for attempt {} of {} was moved from lane key {} to {} and another attempt took the mesh between the two: this attempt holds it no more", row.attempt, row.build_id, standing.lane_key.as_deref().unwrap_or("none"), row.lane_key.as_deref().unwrap_or("none")),
        })
    }

    /// The attempt holding `mesh`, when one does: a point read of its row.
    pub async fn held(&self, mesh: &str) -> Result<Option<InFlight>, StorageError> {
        match self.store(mesh).await? {
            Some(store) => store.read(IN_FLIGHT, FORMAT).await,
            None => {
                let Backing::Memory(rows) = &self.backing else { unreachable!("a store-root backing opens a store") };
                Ok(rows.lock().unwrap_or_else(|e| e.into_inner()).get(mesh).cloned())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::IncarnationId;
    use crate::store_root::StoreRoot;

    fn attempt(build_id: &BuildId, lane: Option<&str>, attempt: u32, claimant: &str) -> InFlight {
        InFlight { build_id: build_id.clone(), lane: lane.map(str::to_string), attempt, claimant: claimant.into(), lane_key: None }
    }

    /// The same script against two claimants: whatever backs them, one attempt holds a mesh at a time.
    async fn one_attempt_in_flight_per_mesh(a: &MeshClaims, b: &MeshClaims) {
        let build = BuildId::mint();
        let topology_op = attempt(&build, None, 2, "mesh1.admin.1");
        let drift = attempt(&build, Some("mesh2"), 1, "mesh2.admin.1");
        assert_eq!(a.take("mesh2", &topology_op).await.unwrap(), Taken::Won, "the first claimant on mesh2 holds it");
        assert_eq!(b.take("mesh2", &drift).await.unwrap(), Taken::Held(topology_op.clone()), "a second attempt on mesh2 is refused, naming the attempt and the claimant that hold it");
        assert_eq!(b.take("mesh1", &attempt(&build, Some("mesh1"), 1, "mesh1.admin.1")).await.unwrap(), Taken::Won, "mesh1 is held apart from mesh2");
        assert_eq!(a.take("mesh2", &topology_op).await.unwrap(), Taken::Won, "a repeat take of the attempt holding the mesh is that attempt's own row");
        b.release("mesh2", &drift).await.unwrap();
        assert_eq!(b.held("mesh2").await.unwrap(), Some(topology_op.clone()), "a release names its attempt: another attempt's row stands");
        a.release("mesh2", &topology_op).await.unwrap();
        assert_eq!(a.held("mesh2").await.unwrap(), None, "the verdict of the attempt holding mesh2 releases it");
        assert_eq!(b.take("mesh2", &drift).await.unwrap(), Taken::Won, "mesh2 is free: the drift attempt holds it");
        assert_eq!(a.held("mesh2").await.unwrap(), Some(drift), "each claimant reads the one row");
    }

    /// CONTRACT (R-BI7 caveat 3, R14 writer ladder): two node-admins on two processes reach one
    /// store root. The fabric-primary's topology attempt on mesh2 takes mesh2's in-flight row first;
    /// mesh2's mesh-primary's drift attempt is then refused, naming that attempt and its claimant,
    /// while mesh1 is taken apart. Only the verdict of the attempt holding mesh2 releases it, and the
    /// row is the one file `meshes/mesh2/claims/in-flight.json` every process reads. Must NOT happen:
    /// two attempts holding one mesh, or a release that removes another attempt's row.
    #[tokio::test]
    async fn a_second_attempt_on_a_mesh_is_held_off_by_the_first_claim_on_the_store_root() {
        let d = crate::record_store::tempdir("mesh-claims");
        let root = StoreRoot::at(&d);
        let (a, b) = (MeshClaims::on_root(root.clone(), IncarnationId::mint()), MeshClaims::on_root(root.clone(), IncarnationId::mint()));
        one_attempt_in_flight_per_mesh(&a, &b).await;
        assert!(root.meshes("mesh2").join("claims").join(format!("{IN_FLIGHT}.json")).is_file(), "the row is on the store root under meshes/mesh2/claims/");
        let later = MeshClaims::on_root(root, IncarnationId::mint());
        assert_eq!(later.held("mesh2").await.unwrap().map(|h| h.claimant), Some("mesh2.admin.1".to_string()), "a process that opens the root later reads the same row");
    }

    /// CONTRACT: the claims a control plane built with no store root holds decide the same way.
    #[tokio::test]
    async fn the_in_memory_claims_decide_as_the_store_root_does() {
        let shared = std::sync::Arc::new(MeshClaims::in_memory());
        one_attempt_in_flight_per_mesh(&shared, &shared).await;
    }
}
