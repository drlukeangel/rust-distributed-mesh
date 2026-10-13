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
use crate::record_store::StorageError;
use serde::{Deserialize, Serialize};

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

/// The in-flight rows of every mesh, as this admin reaches them.
pub struct MeshClaims {
    _backing: (),
}

impl MeshClaims {
    /// The rows on the fabric's store root, `meshes/<mesh>/claims/`, written by the process born as
    /// `writer`.
    pub fn on_root(_root: crate::store_root::StoreRoot, _writer: crate::model::IncarnationId) -> Self {
        Self { _backing: () }
    }

    /// The rows held in this process only: a control plane built with no store root (a fixture).
    pub fn in_memory() -> Self {
        Self { _backing: () }
    }

    /// Take `mesh`'s in-flight row for `claim`.
    pub async fn take(&self, _mesh: &str, _claim: &InFlight) -> Result<Taken, StorageError> {
        Ok(Taken::Won)
    }

    /// Release `mesh`'s in-flight row held for `attempt`.
    pub async fn release(&self, _mesh: &str, _attempt: &InFlight) -> Result<(), StorageError> {
        Ok(())
    }

    /// The attempt holding `mesh`, when one does.
    pub async fn held(&self, _mesh: &str) -> Result<Option<InFlight>, StorageError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::IncarnationId;
    use crate::store_root::StoreRoot;

    fn attempt(build_id: &BuildId, lane: Option<&str>, attempt: u32, claimant: &str) -> InFlight {
        InFlight { build_id: build_id.clone(), lane: lane.map(str::to_string), attempt, claimant: claimant.into() }
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
