//! One attempt in flight per mesh (R-BI7, proposal `build-enitty-updates-plus-otherstuff.md` Part C
//! caveats 2 and 3): "A topology Build's operation on mesh X and mesh X's own drift attempt cannot
//! both be open: whichever claims first on mesh X's rows wins, the other is refused build-in-progress
//! naming the holder."
//!
//! The decision is ONE row of mesh X, its in-flight row, kept by the mesh storage
//! ([`crate::storage::MeshStorage::take_in_flight`]): the claimant of an attempt that acts in the mesh
//! inserts it, insert-and-fail, so two claimants on two nodes (the fabric-primary claiming a topology
//! operation, the mesh-primary claiming its drift attempt) never both hold it. The row names the
//! attempt it holds the mesh for, `(build_id, lane, attempt)`, and the claimant. It is released at that
//! attempt's verdict by a removal that takes it only while it still names that attempt. Nothing is
//! read and written back: the take is an insert of the key, the release a removal of the same key.
//! The file backend keeps it at `meshes/<mesh>/claims/in-flight` on the fabric's shared store root (R14).
//!
//! The row decides; the facts do not move here: the claim fact is appended to the Build log and
//! broadcast on the Build topic only after the take won, exactly as before.

use crate::build::BuildId;
use serde::{Deserialize, Serialize};

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
    /// The lane key of the attempt's claim (Q35, `crate::build_state::LaneKey`): the row is void when
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MeshStorage;

    fn attempt(build_id: &BuildId, lane: Option<&str>, attempt: u32, claimant: &str) -> InFlight {
        InFlight { build_id: build_id.clone(), lane: lane.map(str::to_string), attempt, claimant: claimant.into(), lane_key: None }
    }

    /// The same script against two claimants: whatever backs them, one attempt holds a mesh at a time.
    async fn one_attempt_in_flight_per_mesh(a: &dyn MeshStorage, b: &dyn MeshStorage) {
        let build = BuildId::mint();
        let topology_op = attempt(&build, None, 2, "mesh1.admin.1");
        let drift = attempt(&build, Some("mesh2"), 1, "mesh2.admin.1");
        assert_eq!(a.take_in_flight("mesh2", &topology_op).await.unwrap(), Taken::Won, "the first claimant on mesh2 holds it");
        assert_eq!(b.take_in_flight("mesh2", &drift).await.unwrap(), Taken::Held(topology_op.clone()), "a second attempt on mesh2 is refused, naming the attempt and the claimant that hold it");
        assert_eq!(b.take_in_flight("mesh1", &attempt(&build, Some("mesh1"), 1, "mesh1.admin.1")).await.unwrap(), Taken::Won, "mesh1 is held apart from mesh2");
        assert_eq!(a.take_in_flight("mesh2", &topology_op).await.unwrap(), Taken::Won, "a repeat take of the attempt holding the mesh is that attempt's own row");
        b.release_in_flight("mesh2", &drift).await.unwrap();
        assert_eq!(b.in_flight("mesh2").await.unwrap(), Some(topology_op.clone()), "a release names its attempt: another attempt's row stands");
        a.release_in_flight("mesh2", &topology_op).await.unwrap();
        assert_eq!(a.in_flight("mesh2").await.unwrap(), None, "the verdict of the attempt holding mesh2 releases it");
        assert_eq!(b.take_in_flight("mesh2", &drift).await.unwrap(), Taken::Won, "mesh2 is free: the drift attempt holds it");
        assert_eq!(a.in_flight("mesh2").await.unwrap(), Some(drift), "each claimant reads the one row");
    }

    /// CONTRACT (R-BI7 caveat 3, R14 writer ladder): two node-admins on two processes reach one
    /// store root. The fabric-primary's topology attempt on mesh2 takes mesh2's in-flight row first;
    /// mesh2's mesh-primary's drift attempt is then refused, naming that attempt and its claimant,
    /// while mesh1 is taken apart. Only the verdict of the attempt holding mesh2 releases it, and the
    /// row is the one file `meshes/mesh2/claims/in-flight.json` every process reads. Must NOT happen:
    /// two attempts holding one mesh, or a release that removes another attempt's row.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_second_attempt_on_a_mesh_is_held_off_by_the_first_claim_on_the_store_root() {
        use crate::model::IncarnationId;
        use crate::storage::FileMeshStorage;
        use crate::store_root::StoreRoot;
        let d = crate::test_support::tempdir("mesh-claims");
        let root = StoreRoot::at(&d);
        let (a, b) = (FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap(), FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap());
        one_attempt_in_flight_per_mesh(&a, &b).await;
        assert!(root.meshes("mesh2").join("claims").join("in-flight.json").is_file(), "the row is on the store root under meshes/mesh2/claims/");
        let later = FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap();
        assert_eq!(later.in_flight("mesh2").await.unwrap().map(|h| h.claimant), Some("mesh2.admin.1".to_string()), "a process that opens the root later reads the same row");
    }

    /// CONTRACT: the in-flight rows of a memory mesh storage decide as the store root's do.
    #[tokio::test]
    async fn the_in_memory_claims_decide_as_the_store_root_does() {
        let shared = crate::storage::MemoryMeshStorage::default();
        one_attempt_in_flight_per_mesh(&shared, &shared).await;
    }
}
