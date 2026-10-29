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
//! (R-SR1 to R-SR3, `crate::store_log`), the latest line the current record; and the fabric's claim of
//! its name in the fabrics' name index above the roots, `<store_dir>/fabrics/<name>` (R-FN1). A line
//! this build does not recognise is refused by name, never read as a value.

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
    let own = match claim_fabric_against(storage.fabric().await?.map(|r| r.identity()).as_ref(), identity) {
        Ok(c) => c,
        Err(held) => return Ok(Err(held)),
    };
    if own == FabricClaim::Written {
        storage.put_fabric(&FabricRecord::new(identity.clone())).await?;
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

/// The Fabric record (storage-whole-records.md, "The objects"): the fabric's identity, immutable inside
/// it, and the fabric events applied (`ready`, `shutdown-initiated:<by>`), each once, in
/// the order applied. Kept at the fabric's id and written whole by the fabric-primary from the copy it
/// caches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FabricRecord {
    /// The fabric's id: the record's key.
    pub fabric_id: FabricId,
    /// The fabric's name, claimed above the per-fabric roots.
    pub name: String,
    /// The fabric events applied, each once.
    #[serde(default)]
    pub events: Vec<String>,
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
/// the old anchor's own expiry (the expiry of the last CA it signed), past which `fabrics.get` surfaces
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
    /// The record of `identity`, no event applied yet.
    pub fn new(identity: FabricIdentity) -> Self {
        Self { fabric_id: identity.fabric_id, name: identity.name, events: Vec::new(), provider: None, topology_storage_provider: None, previous_topology_storage_providers: Vec::new(), ca: None, roots: None }
    }

    /// The fabric's identity.
    pub fn identity(&self) -> FabricIdentity {
        FabricIdentity { fabric_id: self.fabric_id.clone(), name: self.name.clone() }
    }
}

/// node-admin's Fabric control state: the Fabric record and the claim of the fabric's name.
#[async_trait]
pub trait FabricStorage: Send + Sync {
    /// The Fabric record; `None` while none is held.
    async fn fabric(&self) -> Result<Option<FabricRecord>, FabricStorageError>;
    /// Write `record` whole as the Fabric record, as given (Q109: storage stores and returns what it is
    /// given).
    async fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError>;
    /// The claim of the fabric name `name` at the fabric's `(path, name)` (R-FN1:
    /// `<store_dir>/fabrics/<name>`, above the per-fabric roots): the Fabric that holds it, if any.
    async fn name_claim(&self, name: &str) -> Result<Option<FabricIdentity>, FabricStorageError>;
    /// Write `identity` as the claim of its name, as given; the claim is the writer's
    /// ([`claim_identity`]).
    async fn put_name_claim(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError>;
    /// Remove the claim of the fabric name `name`.
    async fn remove_name_claim(&self, name: &str) -> Result<(), FabricStorageError>;
}

/// A fabric store held in memory.
#[derive(Debug, Default)]
pub struct MemoryFabricStorage {
    record: Mutex<Option<FabricRecord>>,
    claim: Mutex<Option<FabricIdentity>>,
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
    async fn name_claim(&self, name: &str) -> Result<Option<FabricIdentity>, FabricStorageError> {
        Ok(self.claim.lock().unwrap().clone().filter(|i| i.name == name))
    }
    async fn put_name_claim(&self, identity: &FabricIdentity) -> Result<(), FabricStorageError> {
        *self.claim.lock().unwrap() = Some(identity.clone());
        Ok(())
    }
    // One fabric per memory store: its record goes with its name's claim.
    async fn remove_name_claim(&self, name: &str) -> Result<(), FabricStorageError> {
        let mut held = self.claim.lock().unwrap();
        if held.as_ref().is_some_and(|i| i.name == name) {
            *held = None;
            *self.record.lock().unwrap() = None;
        }
        Ok(())
    }
}

#[cfg(feature = "file-backend")]
/// The Fabric record's lines in `fabric.jsonl`, at the fabric's id.
const RECORD_FORMAT: &str = "fabric-record/2";
#[cfg(feature = "file-backend")]
/// A fabric's claim of its name at `<store_dir>/fabrics/<name>` (R-FN1): the claiming Fabric's identity.
const NAME_FORMAT: &str = "fabric-name/1";

#[cfg(feature = "file-backend")]
/// The Fabric record's lines in the root's `fabric.jsonl` (`crate::store_root`), and the fabric's claim
/// of its name in the fabrics' name index above the roots (`crate::record_store`).
#[derive(Debug)]
pub struct FileFabricStorage {
    /// `<store_dir>/fabrics/`: the fabrics' name index above the per-fabric roots (R-FN1), where this
    /// fabric's claim of its name is the first of its rows.
    names: crate::record_store::FileRecords,
    records: crate::store_log::StoreLog,
    writer: crate::store_log::LogWriter,
}

#[cfg(feature = "file-backend")]
impl FileFabricStorage {
    /// Open the store under the fabric's store root, written by `writer`.
    pub async fn open(root: &crate::store_root::StoreRoot, writer: &crate::store_log::LogWriter) -> Result<Self, FabricStorageError> {
        // The name index opens first: the fabric's name claim is its first write.
        let names = root.fabric_names(&writer.incarnation_id).await?;
        let records = root.log(crate::store_root::FABRIC_FILE, crate::store_log::Formats::new().with::<FabricRecord>(RECORD_FORMAT)).await?;
        Ok(Self { names, records, writer: writer.clone() })
    }

    /// Finishes every write of the fabric's name claim and record this store has issued
    /// (node-primary-update.md step 3).
    pub async fn finish_issued_writes(&self) {
        self.names.finish_issued_writes().await;
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
            more => Err(FabricStorageError::Unrecognised { file: self.records.path().display().to_string(), reason: format!("it holds the records of {} fabrics ({}); a root holds one", more.len(), more.iter().map(|l| l.key.as_str()).collect::<Vec<_>>().join(", ")) }),
        }
    }
    async fn put_fabric(&self, record: &FabricRecord) -> Result<(), FabricStorageError> {
        self.records.append(&self.writer, RECORD_FORMAT, &record.fabric_id.to_string(), record).await
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

    /// CONTRACT (storage.md §2 "The property": `topology_storage_provider` is on the Fabric row beside
    /// `previous_topology_storage_providers`): a record written before the fields reads `file` and no
    /// previous provider; a record written with them reads back as written, an unnamed one as none.
    #[test]
    fn a_fabric_record_written_before_the_provider_reads_file() {
        let old: FabricRecord = serde_json::from_value(serde_json::json!({"fabric_id": FabricId::mint().to_string(), "name": "fabric1", "events": []})).unwrap();
        assert_eq!((old.topology_storage_provider.as_deref(), old.previous_topology_storage_providers.len()), (Some("file"), 0));
        let mut moved = FabricRecord::new(old.identity());
        assert_eq!(serde_json::from_value::<FabricRecord>(serde_json::to_value(&moved).unwrap()).unwrap().topology_storage_provider, None, "a record naming none reads none, never file");
        (moved.topology_storage_provider, moved.previous_topology_storage_providers) = (Some("app".into()), vec!["file".into()]);
        assert_eq!(serde_json::from_value::<FabricRecord>(serde_json::to_value(&moved).unwrap()).unwrap(), moved);
    }

    /// CONTRACT (R-SR2, R-SR3): every write of the Fabric record is a line of `fabric.jsonl`, the latest
    /// the record a reopen reads; a line in a format this build does not read, or a line that is not
    /// one, refuses the read by name. Must NOT happen: an earlier write removed, or an unrecognised line
    /// read as a value.
    #[tokio::test]
    async fn the_file_storage_reloads_what_it_wrote_and_refuses_an_unknown_format_by_name() {
        let d = tempdir();
        let s = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::store_log::LogWriter::mint()).await.unwrap();
        let fabric = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
        assert_eq!(crate::fabric_storage::claim_identity(&s, &fabric).await.unwrap(), Ok(FabricClaim::Written));
        let mut ready = FabricRecord::new(fabric.clone());
        ready.events.push("ready".into());
        s.put_fabric(&ready).await.unwrap();
        let reopened = FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::store_log::LogWriter::mint()).await.unwrap();
        assert_eq!(reopened.fabric().await.unwrap(), Some(ready), "the latest line is the record");
        let lines = crate::store_log::read_file(&d.join(crate::store_root::FABRIC_FILE)).unwrap();
        assert_eq!(lines.len(), 2, "both writes stand as lines");
        let op = crate::store_log::Op { node_id: "n".into(), incarnation_id: "i".into(), seq: 1 };
        let mut line = vec![b'\n'];
        line.extend(crate::store_log::encode_object(&op, "fabric-record/9", &fabric.fabric_id.to_string(), b"{}"));
        line.push(b'\n');
        std::fs::OpenOptions::new().append(true).open(d.join(crate::store_root::FABRIC_FILE)).and_then(|mut f| std::io::Write::write_all(&mut f, &line)).unwrap();
        let e = reopened.fabric().await.unwrap_err();
        assert!(matches!(&e, FabricStorageError::Unrecognised { reason, .. } if reason.contains("fabric-record/9")), "{e}");
    }

    /// CONTRACT (node-identity-and-state.md §2.8: the fabric-primary claims the fabric's name "at
    /// its `(path, name)` ... idempotent for the same FabricId and `NameHeld` naming
    /// a different one"): a free key takes the identity; the same Fabric claiming again writes nothing;
    /// another Fabric's claim is refused naming the holder, and the held identity is never rewritten.
    /// The memory store and the file store each run their own check.
    #[tokio::test]
    async fn the_fabric_name_is_claimed_once_by_its_writer_for_both_stores() {
        for s in [Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, Box::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&tempdir()), &crate::store_log::LogWriter::mint()).await.unwrap())] {
            let first = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            let other = FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() };
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &first).await.unwrap(), Ok(FabricClaim::Written));
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &first).await.unwrap(), Ok(FabricClaim::Held), "the same Fabric claiming again writes nothing");
            assert_eq!(crate::fabric_storage::claim_identity(&*s, &other).await.unwrap(), Err(FabricNameHeld { name: "fabric1".into(), by: first.fabric_id.clone() }), "another Fabric's claim is refused naming the holder");
            assert_eq!(s.fabric().await.unwrap().map(|r| r.identity()), Some(first.clone()), "the held identity is never rewritten");
        }
    }

    /// CONTRACT (R-FN1: a name is unique per (path, kind) for every entity, the fabric included; the
    /// fabric's (path, name) claim sits at the path level, above the per-fabric store roots
    /// (`<store_dir>/fabrics/<name>`), and a second fabric of the same name under that path is refused
    /// `NameHeld { by }`; Q109: the writer reads the claim and writes it; R14 item 1: the per-fabric root is unchanged, the index sits beside it;
    /// CLAUDE.md: a refused claim writes nothing of the entity): under one store_dir, fabric A claims
    /// fabric1. Fabric B, another FabricId with its own root under the same store_dir, claims fabric1:
    /// refused naming A, and nothing of B is written, neither at the index nor under B's root. Must NOT happen: a second fabric of one name under one store_dir taking the name because its
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
                let s = FileFabricStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap();
                (root, s)
            }
        };
        let (_root_a, store_a) = open(&a).await;
        assert_eq!(crate::fabric_storage::claim_identity(&store_a, &a).await.unwrap(), Ok(FabricClaim::Written), "A claims fabric1");
        let (_root_b, store_b) = open(&b).await;
        let capture = crate::span_capture::Capture::new("i143-3089-fabric-name-index");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        assert_eq!(crate::admin::claim_fabric_name(&store_b, &b, &crate::model::NodeId::mint()).await, Ok(Err(FabricNameHeld { name: "fabric1".into(), by: a.fabric_id.clone() })), "B's claim of fabric1 under the same store_dir is refused naming A");
        let spans = capture.named("rdm.node_admin.fabric.create.via-fabric-row");
        assert_eq!(spans.len(), 1, "{spans:?}");
        assert_eq!((spans[0].attributes.get("outcome").map(String::as_str), spans[0].attributes.get("holder").cloned()), (Some("name-held"), Some(a.fabric_id.to_string())), "the claim's span names the refusal and its holder: {spans:?}");
        eprintln!("span rdm.node_admin.fabric.create.via-fabric-row trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
        assert!(crate::store_log::read_file(&store.join(b.fabric_id.to_string()).join(crate::store_root::FABRIC_FILE)).unwrap().is_empty(), "B's Fabric record is not written under its root");
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
        for (s, store) in [(Box::new(MemoryFabricStorage::new()) as Box<dyn FabricStorage>, "memory"), (Box::new(FileFabricStorage::open(&crate::store_root::StoreRoot::at(&d), &crate::store_log::LogWriter::mint()).await.unwrap()), "file")] {
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
