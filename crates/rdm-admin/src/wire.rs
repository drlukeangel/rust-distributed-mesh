//! The typed shape of a Build message (`fabric_builds::BuildMessage`, the check a Build fact passes
//! before it commits; no call carries one, Q292): every type here is positional postcard
//! (`rdm_transport::wire`), carrying every field and no internally tagged enum.
//!
//! The domain types (`BuildFact`, `FabricIdentity`, the accepted topology) are JSON elsewhere: the
//! REST views, the local journal and the tests read `"fact"`, `"kind"`, `"action"`,
//! `"persistence"` keys and absent optional fields. They keep that shape; these mirrors are what
//! travels. A conversion is one `From` per direction, so a field added to a domain type without
//! its mirror does not compile.


use crate::accepted::{FabricTopology, MeshTopology};
use rdm_core::build::BuildChange;
use crate::build::BuildId;
use crate::build_state::{BuildAccepted, BuildFact};
use crate::fabric_builds::BuildMessage;
use crate::model::{FabricId, MeshId, PathName};
use rdm_core::meta::{NodeSpec, PersistentDisposition, StoragePersistence};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Why a Build record cannot be put on the wire. Names the Build it concerns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WireRefusal {
    pub build_id: BuildId,
    pub reason: String,
}

impl std::fmt::Display for WireRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a record of Build {} has no wire shape: {}", self.build_id, self.reason)
    }
}

// ---- the Build fact ------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
pub(crate) enum WireFact {
    Accepted(WireAccepted),
    Forget { build_id: BuildId },
    /// One mesh's section of an accepted Build (Q284), appended last: what travels in place of the
    /// whole acceptance, which only the fabric-primary records.
    Section(WireSection),
}

/// A section of an accepted Build (Q284): the Build's id, fabric, `topology_storage_provider` and
/// acceptance time, its change as it came in, every mesh it names by name and id, and the meshes the section holds (one, or none
/// for a Build with no mesh or none touched), with their pinned births.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireSection {
    build_id: BuildId,
    fabric: String,
    topology_storage_provider: Option<String>,
    time: u64,
    change: BuildChange,
    named: BTreeMap<String, MeshId>,
    meshes: Vec<WireMesh>,
    pinned: Vec<(PathName, crate::build::PinnedBirth)>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WireAccepted {
    build_id: BuildId,
    topology: WireTopology,
    change: BuildChange,
    time: u64,
    /// The record's writer (appended).
    source: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct WireTopology {
    fabric: String,
    meshes: BTreeMap<String, WireMesh>,
    /// The birth each node the Build deletes held at acceptance (build.md §4.4), appended last.
    pinned: Vec<(PathName, crate::build::PinnedBirth)>,
}

#[derive(Serialize, Deserialize)]
struct WireMesh {
    name: String,
    mesh_id: MeshId,
    nodes: BTreeSet<PathName>,
    spec_by_path: BTreeMap<PathName, WireSpec>,
}

/// A create spec as a Build message carries it: its storage, positional. The executable, the
/// tags and the config never ride gossip (proposal executable-bindings-at-create.md rule 7; Q166): the
/// store root's record of the accepted Build carries them.
#[derive(Serialize, Deserialize)]
struct WireSpec {
    mesh: String,
    r#type: String,
    storage: WireStorage,
}

#[derive(Serialize, Deserialize)]
enum WireStorage {
    Ephemeral,
    Persistent { disposition: PersistentDisposition },
}

impl From<&NodeSpec> for WireSpec {
    fn from(m: &NodeSpec) -> Self {
        let storage = match m.storage_persistence {
            StoragePersistence::Ephemeral => WireStorage::Ephemeral,
            StoragePersistence::Persistent { disposition } => WireStorage::Persistent { disposition },
        };
        Self { mesh: m.mesh.clone(), r#type: m.r#type.clone(), storage }
    }
}
impl WireSpec {
    /// The create spec at `path` heard in a Build message: it names no executable, no tags and no config
    /// (proposal executable-bindings-at-create.md rule 7; Q166): the store root's record of the accepted
    /// Build is the one that does.
    fn at(self, path: PathName) -> NodeSpec {
        let storage_persistence = match self.storage {
            WireStorage::Ephemeral => StoragePersistence::Ephemeral,
            WireStorage::Persistent { disposition } => StoragePersistence::Persistent { disposition },
        };
        NodeSpec { path: path.path.clone(), name: path.name.clone(), mesh: self.mesh, r#type: self.r#type.clone(), storage_persistence, executable: None, tags: Default::default(), config: Default::default() }
    }
}
impl From<&MeshTopology> for WireMesh {
    fn from(m: &MeshTopology) -> Self {
        Self { name: m.name.clone(), mesh_id: m.mesh_id.clone(), nodes: m.nodes.clone(), spec_by_path: m.spec_by_path.iter().map(|(p, meta)| (p.clone(), meta.into())).collect() }
    }
}
impl From<WireMesh> for MeshTopology {
    fn from(m: WireMesh) -> Self {
        Self { name: m.name, mesh_id: m.mesh_id, nodes: m.nodes, spec_by_path: m.spec_by_path.into_iter().map(|(p, w)| (p.clone(), w.at(p))).collect() }
    }
}
impl From<&FabricTopology> for WireTopology {
    fn from(t: &FabricTopology) -> Self {
        let meshes = t
            .meshes
            .iter()
            .map(|(k, m)| (k.clone(), m.into()))
            .collect();
        Self { fabric: t.fabric.clone(), meshes, pinned: t.pinned.clone() }
    }
}
impl From<WireTopology> for FabricTopology {
    fn from(t: WireTopology) -> Self {
        let meshes = t
            .meshes
            .into_iter()
            .map(|(k, m)| (k, m.into()))
            .collect();
        // The Build topic carries no topology_storage_provider: the journal's record of the accepted
        // Build is the one that does.
        Self { fabric: t.fabric, meshes, topology_storage_provider: None, pinned: t.pinned, created: Vec::new(), section_of: None }
    }
}

// ---- a step's output -----------------------------------------------------------------------


// ---- conversions ---------------------------------------------------------------------------

/// `fact` as its wire shape, or the refusal naming the step whose output has none.
pub(crate) fn fact_to_wire(fact: &BuildFact) -> Result<WireFact, WireRefusal> {
    Ok(match fact {
        BuildFact::Accepted(a) if a.topology.section_of.is_some() => WireFact::Section(WireSection {
            build_id: a.build_id.clone(),
            fabric: a.topology.fabric.clone(),
            topology_storage_provider: a.topology.topology_storage_provider.clone(),
            time: a.time,
            change: a.change.clone(),
            named: a.topology.section_of.clone().unwrap_or_default(),
            meshes: a.topology.meshes.values().map(WireMesh::from).collect(),
            pinned: a.topology.pinned.clone(),
        }),
        BuildFact::Accepted(a) => WireFact::Accepted(WireAccepted {
            build_id: a.build_id.clone(),
            topology: (&a.topology).into(),
            change: a.change.clone(),
            time: a.time,
            source: a.source.clone(),
        }),
        BuildFact::Forget { build_id } => WireFact::Forget { build_id: build_id.clone() },
    })
}

/// The Build fact a wire fact carries.
pub(crate) fn fact_from_wire(w: WireFact) -> Result<BuildFact, WireRefusal> {
    Ok(match w {
        WireFact::Accepted(a) => BuildFact::Accepted(BuildAccepted {
            build_id: a.build_id,
            topology: a.topology.into(),
            change: a.change,
            source: a.source, time: a.time,
        }),
        WireFact::Forget { build_id } => BuildFact::Forget { build_id },
        WireFact::Section(c) => BuildFact::Accepted(BuildAccepted {
            build_id: c.build_id,
            topology: FabricTopology {
                fabric: c.fabric,
                meshes: c.meshes.into_iter().map(|m| (m.name.clone(), m.into())).collect(),
                topology_storage_provider: c.topology_storage_provider,
                pinned: c.pinned,
                created: Vec::new(), section_of: Some(c.named),
            },
            change: c.change,
            source: None, time: c.time,
        }),
    })
}

/// The Fabric identity at the reserved position of [`WireMessage`]: kept so the frame keeps its bytes.
#[derive(Serialize, Deserialize)]
struct WireFabricIdentity {
    fabric_id: FabricId,
    name: String,
}

/// A Build message in its typed shape: one postcard frame.
#[derive(Serialize, Deserialize)]
pub(crate) struct WireMessage {
    nonce: u64,
    facts: Vec<WireFact>,
    /// Reserved position: the removed Fabric record of the Build channel (Q185). Sent as none; a frame
    /// carrying one is refused by name. Kept so the frame keeps its bytes.
    fabric: Option<WireFabricIdentity>,
}

impl WireMessage {
    /// `m` in its wire shape, or the refusal naming the step whose output has none.
    pub(crate) fn of(m: &BuildMessage) -> Result<Self, WireRefusal> {
        Ok(Self { nonce: m.nonce, facts: m.facts.iter().map(fact_to_wire).collect::<Result<_, _>>()?, fabric: None })
    }

    /// The message this frame carries, or why it is not one: a frame carrying the removed Fabric
    /// record (the reserved position) is refused by name, as a fact this build does not read is.
    pub(crate) fn into_message(self) -> Result<BuildMessage, String> {
        if let Some(r) = self.fabric {
            return Err(format!("a Build message carrying the Fabric record of Fabric {} ({}): the Build channel's record is removed (Q185) and its position is reserved", r.fabric_id, r.name));
        }
        Ok(BuildMessage { nonce: self.nonce, facts: self.facts.into_iter().map(fact_from_wire).collect::<Result<_, _>>().map_err(|e| e.to_string())? })
    }
}

// ---- the join answer -----------------------------------------------------------------------

pub use rdm_node::wire::{answer_from_wire, answer_to_wire, BuildFloor, JoinAnswer, JoinControl};
