//! `fabric.storage`: node-admin's Fabric control state behind a storage boundary
//! (fabric-mesh-lifecycle.md §11.1, §13).
//!
//! It holds the Fabric's record. The Fabric record carries no pointer to the newest Build (R-BI3): the
//! current Build is read from the Build entities themselves
//! ([`crate::build_state::BuildStateAdapter::current_build`]). The Fabric record has one writer, the
//! fabric-primary (R14 item 2): it writes the identity, never again, and the fabric events applied,
//! the record written whole with each event (storage-whole-records.md). Every other admin reads it,
//! and holds the identity it learns (its join answer) as its own runtime state,
//! never as a record here.
//!
//! Two implementations: [`MemoryFabricStorage`] for runs that need no restart survival, and
//! [`FileFabricStorage`]: each write of the Fabric record one line of the root's `fabric.jsonl`
//! (R-SR1 to R-SR3, `crate::store_log`), the latest line the current record. A line this build does
//! not recognise is refused by name, never read as a value.

use async_trait::async_trait;
use crate::model::FabricId;
use serde::{Deserialize, Serialize};

use std::sync::Mutex;

pub use rdm_core::model::FabricIdentity;

/// What a write of the fabric's identity row did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FabricWritten {
    /// The identity row was written: the root held none.
    Written,
    /// This Fabric's own identity row stands: nothing is written (Day 0's write met again by the
    /// fabric-primary's row writer, a restart, a successor fabric-primary).
    Held,
}

/// The root holds another Fabric's identity row: nothing is written, and a held identity is never
/// rewritten (a root is one fabric's, `<store_dir>/<fabric_id>/`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricIdentityHeld {
    /// The fabric name written.
    pub name: String,
    /// The Fabric whose identity row the root holds.
    pub by: FabricId,
}

impl std::fmt::Display for FabricIdentityHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the fabric store holds the identity row of Fabric {}, so the identity of the fabric {} is not written: a held identity is never rewritten", self.by, self.name)
    }
}

impl std::error::Error for FabricIdentityHeld {}

/// What a write of `identity` meets in `held`, the identity row the root holds.
fn identity_against(held: Option<&FabricIdentity>, identity: &FabricIdentity) -> Result<FabricWritten, FabricIdentityHeld> {
    match held {
        None => Ok(FabricWritten::Written),
        Some(h) if h.fabric_id == identity.fabric_id => Ok(FabricWritten::Held),
        Some(h) => Err(FabricIdentityHeld { name: identity.name.clone(), by: h.fabric_id.clone() }),
    }
}

/// Why a `fabric.storage` read or write failed.
pub use crate::storage_error::StorageError as FabricStorageError;

/// Keep `identity` as the Fabric's identity row (node-identity-and-state.md §2.8; build.md §5.3:
/// "`FabricIdentity { fabric_id, name }`, written once"): the writer reads the row the root holds (Q109:
/// the storage decides nothing). A root holding none takes the row (`Written`); this Fabric's own row
/// there writes nothing (`Held`); another Fabric's row: `FabricIdentityHeld` naming that Fabric, and nothing
/// is written.
pub async fn write_identity(storage: &dyn FabricStorage, identity: &FabricIdentity) -> Result<Result<FabricWritten, FabricIdentityHeld>, FabricStorageError> {
    let own = match identity_against(storage.fabric().await?.map(|r| r.identity()).as_ref(), identity) {
        Ok(c) => c,
        Err(held) => return Ok(Err(held)),
    };
    if own == FabricWritten::Written {
        storage.put_fabric(&FabricRecord::new(identity.clone())).await?;
    }
    Ok(Ok(own))
}

use crate::fabric_state::FabricState;

/// The Fabric record (storage-whole-records.md, "The objects"; the Fabrics schema): the fabric's
/// identity, immutable inside it, and its one `state`, rewritten whole by the fabric-primary from the
/// copy it caches, each move of the state also an event in the events store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricRecord {
    /// The fabric's id: the record's key.
    pub fabric_id: FabricId,
    /// The fabric's name.
    pub name: String,
    /// The fabric's state: it only moves forward, pending -> ready.
    pub state: FabricState,
    /// The application's node types (the Fabrics schema's `types`), each extending `node` and
    /// overriding its defaults: fixed by Day 0's `RDM_NODE_TYPES` and written by the bootstrap admin
    /// with the record; never named `node` or `node-admin` (RDM's two).
    #[serde(default)]
    pub types: Vec<rdm_core::NodeType>,
    /// The deployment provider that runs the fabric's nodes: fabric policy, fixed by Day 0's
    /// `MESH_SPAWN_TYPE` and written by the bootstrap admin with the record. An adoption launch takes
    /// it from here and refuses a differing `MESH_SPAWN_TYPE` by name (Q156). A record written before
    /// it was carried names none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<crate::model::ProviderKind>,
    /// The fabric's `topology_storage_provider` (storage.md §2): the registered name of the backend
    /// RDM's five record stores are written to, the fabric's intent. Written by the bootstrap admin on
    /// Day 0 from `RDM_TOPOLOGY_STORAGE_PROVIDER`, and by the fabric-primary at a storage change's
    /// commit. A record written before the field read as `file`, the one backend there was; `None`
    /// (written as `null`) while no Day 0 has named one.
    #[serde(default = "file_provider")]
    pub topology_storage_provider: Option<String>,
    /// Each provider the fabric left, in order (storage.md §2): kept, frozen at its commit, never
    /// deleted by RDM.
    #[serde(default)]
    pub previous_topology_storage_providers: Vec<String>,
    /// The fabric CA's row (fabric-ca-renew.md "fabric.ca.renew": "write the CA row"), written by the
    /// fabric-primary at each CA renewal; `None` before the first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca: Option<CaRow>,
    /// The root rotation's row (fabric-ca-renew.md "fabric.root.update": "the roots row is durable and
    /// a successor continues from it"); `None` while no rotation was ever asked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roots: Option<RootsRow>,
}

/// The fabric CA's row (fabric-ca-renew.md "fabric.ca.renew"): the CA's expiry, the one it replaced,
/// and each mesh's issuer receipt under it; the renewal is complete when every mesh of the view has one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaRow {
    /// When the current CA expires, rafka-time ms.
    pub expires_at: u64,
    /// When the CA it replaced expires, rafka-time ms.
    pub previous_expires_at: u64,
    /// Each mesh's issuer re-minted under this CA: the issuer's expiry, by mesh name.
    #[serde(default)]
    pub issuer_receipts: std::collections::BTreeMap<String, u64>,
}

/// The root rotation's row (fabric-ca-renew.md "fabric.root.update"): its phase, the roots of the
/// rotation as public keys in hex (the old first, then the new), and the bound it must retire by,
/// the old anchor's own expiry (the expiry of the last CA it signed), past which `fabric.get` surfaces
/// `roots: stalled`. The root secret is never in a row (fabric-certs.md §Custody).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootsRow {
    /// The rotation's phase.
    pub phase: rdm_core::status::RootsPhase,
    /// The old root and the new, each an ed25519 public key as hex.
    pub roots: Vec<String>,
    /// The old anchor's own expiry, rafka-time ms.
    pub expires_at: u64,
}

/// The `topology_storage_provider` of a record written before the field: RDM's file backend.
pub(crate) fn file_provider() -> Option<String> {
    Some(rdm_core::launch::TOPOLOGY_STORAGE_FILE.into())
}

impl FabricRecord {
    /// The record of `identity`: `pending`, declaring no type.
    pub fn new(identity: FabricIdentity) -> Self {
        Self { fabric_id: identity.fabric_id, name: identity.name, state: FabricState::Pending, types: Vec::new(), provider: None, topology_storage_provider: None, previous_topology_storage_providers: Vec::new(), ca: None, roots: None }
    }

    /// The fabric's identity.
    pub fn identity(&self) -> FabricIdentity {
        FabricIdentity { fabric_id: self.fabric_id.clone(), name: self.name.clone() }
    }
}

/// node-admin's Fabric control state: the Fabric record.
#[async_trait]
pub trait FabricStorage: Send + Sync {
    /// The Fabric record; `None` while none is held.
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError>;
    /// Write `record` whole as the Fabric record, as given (Q109: storage stores and returns what it is
    /// given).
    async fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError>;
}

/// A fabric store held in memory.
#[derive(Debug, Default)]
pub struct MemoryFabricStorage {
    record: Mutex<Option<FabricRecord>>,
}

impl MemoryFabricStorage {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl FabricStorage for MemoryFabricStorage {
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        Ok(self.record.lock().unwrap().clone())
    }
    async fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError> {
        *self.record.lock().unwrap() = Some(record.clone());
        Ok(())
    }
}

#[cfg(feature = "file-backend")]
/// The Fabric record's lines in `fabric.jsonl`, at the fabric's id.
const RECORD_FORMAT: &str = "fabric-record/3";

#[cfg(feature = "file-backend")]
/// The Fabric record's lines in the root's `fabric.jsonl` (`crate::store_root`).
#[derive(Debug)]
pub struct FileFabricStorage {
    records: crate::store_log::StoreLog,
    writer: crate::store_log::LogWriter,
}

#[cfg(feature = "file-backend")]
impl FileFabricStorage {
    /// Open the store under the fabric's store root, written by `writer`.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &crate::store_log::LogWriter) -> Result<Self, FabricStorageError> {
        let records = root.log(crate::store_root::FABRIC_FILE, crate::store_log::Formats::new().with::<FabricRecord>(RECORD_FORMAT)).await?;
        Ok(Self { records, writer: writer.clone() })
    }

    /// Finishes every write of the fabric's record this store has issued (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        self.records.finish_issued_writes().await;
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl FabricStorage for FileFabricStorage {
    /// The latest Fabric record line: one fabric per root, so the root's one record kind.
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError> {
        let lines = self.records.latest_of(crate::store_root::FABRIC_RECORD_KIND).await?;
        match lines.as_slice() {
            [] => Ok(None),
            [line] => line.decode(self.records.path()).map(Some),
            more => Err(FabricStorageError::Unrecognised { file: self.records.path().display().to_string(), reason: format!("it holds the records of {} fabrics ({}); a root holds one", more.len(), more.iter().map(|l| l.subject()).collect::<Vec<_>>().join(", ")) }),
        }
    }
    async fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError> {
        self.records.append(&self.writer, crate::store_root::FABRIC_RECORD_KIND, RECORD_FORMAT, &record.fabric_id.to_string(), record).await
    }
}

#[cfg(all(test, feature = "file-backend"))]
mod tests {
    use super::*;

    /// CONTRACT (build.md §5.3; storage.md §2; Q365): the Fabric record's line in the root's
    /// `fabric.jsonl` is frozen byte for byte: a CloudEvents event whose `source` is its writer, `type`
    /// `fabric-record`, `dataschema` `fabric-record/3`, `subject` the fabric's id and `data` the whole
    /// record; its `id` and `time` the write's. A layout that moves moves this cell with it (`durable
    /// format moved`). Must NOT happen: a line whose attributes or record bytes moved while this cell
    /// still passes.
    #[tokio::test]
    async fn the_fabric_store_line_is_frozen_byte_for_byte() {
        let d = tempdir();
        let writer = crate::store_log::LogWriter::mint();
        let s = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &writer).await.unwrap();
        let fabric_id = FabricId::parse(&rdm_core::fixture_crockford60("fabric-1")).unwrap();
        s.put_fabric(&FabricRecord::new(FabricIdentity { fabric_id: fabric_id.clone(), name: "fabric1".into() })).await.unwrap();
        let file = d.join(crate::store_root::FABRIC_FILE);
        let lines = crate::store_log::read_file(&file).unwrap();
        assert_eq!(lines.len(), 1);
        let attributes = crate::store_log::Attributes { id: lines[0].attributes.id.clone(), source: writer.source(), r#type: "fabric-record".into(), subject: Some(fabric_id.to_string()), time: None, dataschema: "fabric-record/3".into(), traceparent: None, tracestate: None };
        let record = format!(r#"{{"fabric_id":"{fabric_id}","name":"fabric1","state":"pending","types":[],"topology_storage_provider":null,"previous_topology_storage_providers":[]}}"#);
        assert_eq!(String::from_utf8(std::fs::read(&file).unwrap()).unwrap(), String::from_utf8(crate::store_log::encode_line(&attributes, record.as_bytes())).unwrap());
    }

    /// CONTRACT (storage.md §2 "The property": `topology_storage_provider` is on the Fabric row beside
    /// `previous_topology_storage_providers`): a record written before the fields reads `file` and no
    /// previous provider; a record written with them reads back as written, an unnamed one as none.
    #[test]
    fn a_fabric_record_written_before_the_provider_reads_file() {
        let old: FabricRecord = serde_json::from_value(serde_json::json!({"fabric_id": FabricId::mint().to_string(), "name": "fabric1", "state": "pending"})).unwrap();
        assert_eq!((old.topology_storage_provider.as_deref(), old.previous_topology_storage_providers.len()), (Some("file"), 0));
        let mut moved = FabricRecord::new(old.identity());
        assert_eq!(serde_json::from_value::<FabricRecord>(serde_json::to_value(&moved).unwrap()).unwrap().topology_storage_provider, None, "a record naming none reads none, never file");
        (moved.topology_storage_provider, moved.previous_topology_storage_providers) = (Some("app".into()), vec!["file".into()]);
        assert_eq!(serde_json::from_value::<FabricRecord>(serde_json::to_value(&moved).unwrap()).unwrap(), moved);
    }

    /// CONTRACT (R-SR2, R-SR3): every write of the Fabric record is a line of `fabric.jsonl`, the latest
    /// the record a reopen reads; a line in a `dataschema` this build does not read, or a line that is not
    /// one, refuses the read by name. Must NOT happen: an earlier write removed, or an unrecognised line
    /// read as a value.
    #[tokio::test]
    async fn the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_dataschema_by_name() {
        let d = tempdir();
        let s = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::store_log::LogWriter::mint()).await.unwrap();
        let fabric = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
        assert_eq!(crate::fabric_storage::write_identity(&s, &fabric).await.unwrap(), Ok(FabricWritten::Written));
        let mut ready = FabricRecord::new(fabric.clone());
        ready.state = FabricState::Ready;
        s.put_fabric(&ready).await.unwrap();
        let reopened = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::store_log::LogWriter::mint()).await.unwrap();
        assert_eq!(reopened.fabric().await.unwrap(), Some(ready), "the latest line is the record");
        let lines = crate::store_log::read_file(&d.join(crate::store_root::FABRIC_FILE)).unwrap();
        assert_eq!(lines.len(), 2, "both writes stand as lines");
        let writer = crate::store_log::LogWriter::mint();
        let mut line = vec![b'\n'];
        line.extend(crate::store_log::encode_object(&crate::store_log::Attributes::of(&writer, "fabric-record", "fabric-record/9", &fabric.fabric_id.to_string()), b"{}"));
        line.push(b'\n');
        std::fs::OpenOptions::new().append(true).open(d.join(crate::store_root::FABRIC_FILE)).and_then(|mut f| std::io::Write::write_all(&mut f, &line)).unwrap();
        let e = reopened.fabric().await.unwrap_err();
        assert!(matches!(&e, FabricStorageError::Unrecognised { reason, .. } if reason.contains("fabric-record/9")), "{e}");
    }

    /// CONTRACT (build.md §5.3: "`FabricIdentity { fabric_id, name }`, written once"): a root holding no
    /// identity takes it; the same Fabric writing again writes nothing; another Fabric's identity is
    /// refused naming the one the root holds, and the held identity is never rewritten. The memory store
    /// and the file store each run their own check.
    #[tokio::test]
    async fn the_fabric_identity_is_written_once_by_its_writer_for_both_stores() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, Box::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&tempdir()), &crate::store_log::LogWriter::mint()).await.unwrap())] {
            let first = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            let other = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            assert_eq!(crate::fabric_storage::write_identity(&*s, &first).await.unwrap(), Ok(FabricWritten::Written));
            assert_eq!(crate::fabric_storage::write_identity(&*s, &first).await.unwrap(), Ok(FabricWritten::Held), "the same Fabric claiming again writes nothing");
            assert_eq!(crate::fabric_storage::write_identity(&*s, &other).await.unwrap(), Err(FabricIdentityHeld { name: "fabric1".into(), by: first.fabric_id.clone() }), "another Fabric's claim is refused naming the holder");
            assert_eq!(s.fabric().await.unwrap().map(|r| r.identity()), Some(first.clone()), "the held identity is never rewritten");
        }
    }

    /// CONTRACT (build.md §5.3: "`FabricIdentity { fabric_id, name }`, written once"; i152 s5: nothing
    /// checks a fabric name across one shared store root): under one store_dir, fabric A and fabric B,
    /// two FabricIds of one name each with its own root, each write their own identity row under their
    /// own root, and nothing is written beside the roots. Must NOT happen: a claim of the fabric's name
    /// above the roots (`<store_dir>/fabrics/`), or B refused because A holds the name.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn two_fabrics_of_one_name_under_one_store_dir_each_write_their_own_identity_and_nothing_beside_the_roots() {
        let d = tempdir();
        let store = d.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let fabric = |name: &str| FabricIdentity { fabric_id: FabricId::mint(), name: name.into() };
        let (a, b) = (fabric("fabric1"), fabric("fabric1"));
        for f in [&a, &b] {
            let root = crate::store_root::StoreRoot::open(&store, &f.fabric_id).unwrap();
            let s = FileFabricStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap();
            assert_eq!(crate::admin::write_fabric_row(&s, f, &crate::model::NodeId::mint()).await, Ok(Ok(FabricWritten::Written)), "Fabric {} writes its own identity", f.fabric_id);
            assert_eq!(s.fabric().await.unwrap().map(|r| r.identity()), Some(f.clone()), "the root holds its own fabric's identity");
        }
        let mut beside: Vec<String> = std::fs::read_dir(&store).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
        beside.sort();
        let mut roots = vec![a.fabric_id.to_string(), b.fabric_id.to_string()];
        roots.sort();
        assert_eq!(beside, roots, "the store_dir holds the two roots and nothing beside them");
    }

    fn tempdir() -> std::path::PathBuf {
        crate::test_support::tempdir("fabric-storage")
    }
}
