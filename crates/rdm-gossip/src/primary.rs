//! The held primaries: the fabric-primary, each mesh's mesh-primary and each node type's
//! node-type primary as a node holds them.


use rdm_core::{IncarnationId, Primary, PrimaryRole};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};



/// The primaries a node holds: each role's record with the latest revision, until a record that
/// supersedes it (`Primary::supersedes`). A record that does not is refused by name.
#[derive(Debug, Clone)]
pub struct PrimaryView {
    pub(crate) inner: Arc<Mutex<HeldRoles>>,
    /// Ticks whenever a record is held: whoever persists the records, or looks at the primaries, wakes on it.
    pub(crate) changed: Arc<tokio::sync::watch::Sender<u64>>,
    /// When this view was made: the fabric-primary's record is held from then until `HeldRoles::fabric_since`
    /// says it changed primary.
    pub(crate) created: Instant,
}

impl Default for PrimaryView {
    fn default() -> Self {
        Self { inner: Arc::default(), changed: Arc::new(tokio::sync::watch::Sender::new(0)), created: Instant::now() }
    }
}

#[derive(Debug, Default)]
pub(crate) struct HeldRoles {
    pub(crate) fabric: Option<Primary>,
    /// When the fabric-primary's record last changed primary here, by every writer (`put`).
    pub(crate) fabric_since: Option<Instant>,
    pub(crate) meshes: HashMap<String, Primary>,
    /// Every other (mesh, node type) node-type primary record, by (mesh, kind).
    pub(crate) node_type_primaries: HashMap<(String, String), Primary>,
    /// Primaries' exact births a peer told this node are proven gone (an entry read): the proof
    /// the peer holds, which this node never heard itself.
    pub(crate) gone: std::collections::HashSet<IncarnationId>,
    /// Each birth a record held here named before a record naming another birth replaced it, by the
    /// record's role and mesh, with the instant it was replaced here: the primaries this view saw
    /// change, so a birth that was never primary here is told apart from one an update moved.
    pub(crate) replaced: HashMap<(PrimaryRole, String, IncarnationId), Instant>,
}

/// The role and mesh a record of `role` in `mesh` is held under: a mesh's node-admin type is its
/// mesh-primary, and the fabric-primary is one per fabric.
pub(crate) fn held_under(role: PrimaryRole, mesh: &str) -> (PrimaryRole, String) {
    match role {
        PrimaryRole::FabricPrimary => (PrimaryRole::FabricPrimary, String::new()),
        PrimaryRole::MeshPrimary => (PrimaryRole::MeshPrimary, mesh.to_string()),
        PrimaryRole::NodePrimary(r#type) => (PrimaryRole::NodePrimary(r#type), mesh.to_string()),
    }
}

impl HeldRoles {
    /// The record held for `role` of `mesh` (the mesh is ignored for the fabric-primary role).
    pub(crate) fn get(&self, role: PrimaryRole, mesh: &str) -> Option<Primary> {
        match role {
            PrimaryRole::FabricPrimary => self.fabric.clone(),
            PrimaryRole::MeshPrimary => self.meshes.get(mesh).cloned(),
            PrimaryRole::NodePrimary(r#type) => self.node_type_primaries.get(&(mesh.to_string(), r#type)).cloned(),
        }
    }

    /// Hold `record` for `role` of `mesh`.
    pub(crate) fn put(&mut self, role: PrimaryRole, mesh: &str, record: Primary) {
        if let Some(held) = self.get(role.clone(), mesh).filter(|h| h.incarnation != record.incarnation) {
            let (role, mesh) = held_under(role.clone(), mesh);
            self.replaced.insert((role, mesh, held.incarnation), Instant::now());
        }
        match role {
            PrimaryRole::FabricPrimary => {
                if self.fabric.as_ref().is_none_or(|held| held.node_id != record.node_id) {
                    self.fabric_since = Some(Instant::now());
                }
                self.fabric = Some(record);
            }
            PrimaryRole::MeshPrimary => {
                self.meshes.insert(mesh.to_string(), record);
            }
            PrimaryRole::NodePrimary(r#type) => {
                self.node_type_primaries.insert((mesh.to_string(), r#type), record);
            }
        }
    }
}

/// What [`PrimaryView::take`] did with a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleTaken {
    /// The record was held; `previous` is the record it superseded.
    Held {
        /// The record it superseded.
        previous: Option<Primary>,
    },
    /// The record is the one already held.
    Same,
    /// The record does not supersede the one held, which is named.
    Refused {
        /// The record held, which this one does not supersede.
        held: Primary,
    },
}

impl PrimaryView {
    /// Hold `primary` for `role` if it supersedes what is held.
    pub fn take(&self, role: PrimaryRole, primary: &Primary) -> RoleTaken {
        let mut held = self.inner.lock().unwrap();
        match held.get(role.clone(), &primary.mesh) {
            Some(c) if &c == primary => RoleTaken::Same,
            Some(c) if !primary.supersedes(&c) => RoleTaken::Refused { held: c },
            previous => {
                held.put(role, &primary.mesh, primary.clone());
                self.changed.send_modify(|v| *v += 1);
                RoleTaken::Held { previous }
            }
        }
    }

    /// How long this view has held the fabric-primary's current primary as the primary: a primary is unheard
    /// past the staleness floor only once its view has held it as the primary for a floor (a record
    /// learned a moment ago, after a heal, names a primary whose silence predates it).
    pub fn fabric_held_for(&self) -> Duration {
        self.inner.lock().unwrap().fabric_since.unwrap_or(self.created).elapsed()
    }

    /// Hold every record of one node.primary.update together, under one lock: either every record
    /// supersedes the one held and all are held, or none is (the records a stale update names stay as
    /// they are). Returns what happened per record.
    pub fn take_together(&self, records: &[(PrimaryRole, Primary)]) -> Vec<RoleTaken> {
        let mut held = self.inner.lock().unwrap();
        let decided: Vec<RoleTaken> = records
            .iter()
            .map(|(role, r)| match held.get(role.clone(), &r.mesh) {
                Some(h) if &h == r => RoleTaken::Same,
                Some(h) if !r.supersedes(&h) => RoleTaken::Refused { held: h },
                previous => RoleTaken::Held { previous },
            })
            .collect();
        if decided.iter().any(|t| matches!(t, RoleTaken::Refused { .. })) {
            return records.iter().map(|(role, r)| RoleTaken::Refused { held: held.get(role.clone(), &r.mesh).unwrap_or_else(|| r.clone()) }).collect();
        }
        for (role, r) in records {
            held.put(role.clone(), &r.mesh, r.clone());
        }
        if decided.iter().any(|t| matches!(t, RoleTaken::Held { .. })) {
            self.changed.send_modify(|v| *v += 1);
        }
        decided
    }

    /// Whether a record of `role` in `mesh` held here named `incarnation` and a record naming another
    /// birth replaced it: the birth was primary here and is no more.
    pub fn replaced(&self, role: PrimaryRole, mesh: &str, incarnation: &IncarnationId) -> bool {
        let (role, mesh) = held_under(role, mesh);
        self.inner.lock().unwrap().replaced.contains_key(&(role, mesh, incarnation.clone()))
    }

    /// When a record of `role` in `mesh` naming `incarnation` was last replaced here, if it was.
    pub fn replaced_at(&self, role: PrimaryRole, mesh: &str, incarnation: &IncarnationId) -> Option<Instant> {
        let (role, mesh) = held_under(role, mesh);
        self.inner.lock().unwrap().replaced.get(&(role, mesh, incarnation.clone())).copied()
    }

    /// The fabric-primary's held record.
    pub fn fabric(&self) -> Option<Primary> {
        self.inner.lock().unwrap().fabric.clone()
    }


    /// Hold that the peer proved `incarnation`, a primary's birth, gone.
    pub fn mark_gone(&self, incarnation: IncarnationId) {
        if self.inner.lock().unwrap().gone.insert(incarnation) {
            self.changed.send_modify(|v| *v += 1);
        }
    }

    /// The primaries' births this node was told are gone.
    pub fn gone(&self) -> std::collections::HashSet<IncarnationId> {
        self.inner.lock().unwrap().gone.clone()
    }

    /// A receiver that wakes each time a record is held.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// The held record of `mesh`'s primary role.
    pub fn mesh(&self, mesh: &str) -> Option<Primary> {
        self.inner.lock().unwrap().meshes.get(mesh).cloned()
    }

    /// Every mesh primary record held, by mesh name.
    pub fn meshes(&self) -> std::collections::BTreeMap<String, Primary> {
        self.inner.lock().unwrap().meshes.iter().map(|(m, h)| (m.clone(), h.clone())).collect()
    }

    /// The held record of `mesh`'s `kind` node-type primary role (`node_admin`: its mesh-primary role).
    pub fn node_type_primary(&self, mesh: &str, r#type: &str) -> Option<Primary> {
        self.inner.lock().unwrap().get(PrimaryRole::of_type(r#type), mesh)
    }

    /// Every other node type's primary record held, by (mesh, kind).
    pub fn node_type_primaries(&self) -> std::collections::BTreeMap<(String, String), Primary> {
        self.inner.lock().unwrap().node_type_primaries.iter().map(|(k, h)| (k.clone(), h.clone())).collect()
    }

}
