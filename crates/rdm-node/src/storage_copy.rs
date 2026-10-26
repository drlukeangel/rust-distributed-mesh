//! The copy of a storage change (storage.md §2, "Changing it", step 3): "The fabric-primary, the one
//! reader of every store, appends every line of every store on the old provider to the new one in
//! source order, dead births' and deleted nodes' records included, and every name claim and in-flight
//! row with its exact claimant. A copy resumed after a lost answer recognises the lines already there
//! and appends none twice; a new provider holding lines the old one does not is refused by name, and
//! nothing switches."
//!
//! A provider answers the copy through [`ProviderLines`]: its five stores as their lines, each with the
//! op its writer wrote it with, and its claims as their content. It stores and returns what it is
//! given and decides nothing (Q109); [`copy`] decides. The copy runs once changes are blocked and the
//! outstanding writes have finished (steps 1 and 2), so neither provider changes beneath it.
//!
//! [`copy`] first verifies that each of the new provider's stores holds a prefix of the old one's
//! lines, line for line, and that every claim it holds is the old one's with the same content; a
//! store or a claim that is not is refused by name, naming the store or the claim, the key and the ops
//! that differ, never a record, and nothing is appended. It then appends the old provider's lines
//! after that prefix, store by store in source order, and inserts the claims the new provider lacks.
//! The new provider's verified lines are the copy's progress: a copy resumed after a lost answer
//! verifies them again and appends only what follows.

use crate::storage_error::StorageError;
use crate::store_log::{Line, LockKind, LockTaken, Op, StoreLocks, StoreLog};
use async_trait::async_trait;
use rdm_core::{BuildId, ChangeId};
use std::path::{Path, PathBuf};

/// The span of a copy: the change, both providers, the source position verified and the lines copied
/// (i149 plan, "OTLP span-assertion bar").
pub const COPY_SPAN: &str = "rdm.node_admin.build.update.via-storage-copy";

/// One of the five stores of a provider (storage.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Store {
    /// `fabric.storage`.
    Fabric,
    /// `mesh.storage`.
    Meshes,
    /// `nodes.storage`.
    Nodes,
    /// `connections.storage`.
    Connections,
    /// `builds.storage`.
    Builds,
}

impl Store {
    /// The five stores, in the order a copy takes them.
    pub const ALL: [Store; 5] = [Store::Fabric, Store::Meshes, Store::Nodes, Store::Connections, Store::Builds];

    /// The store's name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Fabric => "fabric",
            Self::Meshes => "meshes",
            Self::Nodes => "nodes",
            Self::Connections => "connections",
            Self::Builds => "builds",
        }
    }

    /// The store's file under a file provider's root.
    pub fn file(self) -> &'static str {
        use crate::store_root::{BUILDS_FILE, CONNECTIONS_FILE, FABRIC_FILE, MESHES_FILE, NODES_FILE};
        match self {
            Self::Fabric => FABRIC_FILE,
            Self::Meshes => MESHES_FILE,
            Self::Nodes => NODES_FILE,
            Self::Connections => CONNECTIONS_FILE,
            Self::Builds => BUILDS_FILE,
        }
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Which claim: the fabric's claim of its name, the claim of a mesh name or a node path.name, or a
/// mesh's in-flight row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClaimKind {
    /// The fabric's claim of its name (R-FN1).
    FabricName,
    /// The claim of a mesh name or a node path.name.
    Name,
    /// The one attempt in flight on a mesh.
    InFlight,
    /// A storage change's `commit` record for `(build_id, change_id)` (storage.md §2 step 4), named
    /// [`rdm_core::build::StorageCommit::name`]: inserted on the new provider by the switch, read by
    /// whoever resumes the change. Not a claim a copy carries: [`ProviderLines::claims`] lists none.
    Commit,
}

impl std::fmt::Display for ClaimKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::FabricName => "fabric name claim",
            Self::Name => "name claim",
            Self::InFlight => "in-flight row",
            Self::Commit => "commit record",
        })
    }
}

/// One claim a provider holds: its kind, its name and its content as written, which names its claimant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Claim {
    /// Which claim.
    pub kind: ClaimKind,
    /// The name claimed: the fabric's, a mesh's or a node's name, or the mesh an in-flight row holds.
    pub name: String,
    /// The claim as written, byte for byte.
    pub content: Vec<u8>,
}

/// What an insert of a claim came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimTaken {
    /// This insert wrote the claim.
    Won,
    /// The claim stands: its content.
    Held(Vec<u8>),
}

/// A provider's five stores as their lines, and its claims: what a copy reads from the old provider and
/// appends to the new one. Persistence only: every method stores or returns what it is given.
#[async_trait]
pub trait ProviderLines: Send + Sync {
    /// Every line of `store`, in source order, each with its op.
    async fn lines(&self, store: Store) -> Result<Vec<Line>, StorageError>;
    /// Append `line` to `store` as its writer wrote it, its op kept.
    async fn append_line(&self, store: Store, line: &Line) -> Result<(), StorageError>;
    /// Every claim that stands, the fabric's claim of its name included.
    async fn claims(&self) -> Result<Vec<Claim>, StorageError>;
    /// Insert `claim` as written: `Won` when this insert wrote it, else `Held` with the standing content.
    async fn insert_claim(&self, claim: &Claim) -> Result<ClaimTaken, StorageError>;
    /// The content of the claim `kind` of `name`, `None` when it does not stand: how a storage change's
    /// `commit` record is read (storage.md §2 step 4).
    async fn claim(&self, kind: ClaimKind, name: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.claims().await?.into_iter().find(|c| c.kind == kind && c.name == name).map(|c| c.content))
    }
}

/// The change a copy is a step of, and the registered names of its two providers.
#[derive(Debug, Clone)]
pub struct CopyRun {
    /// The Build that carries the change.
    pub build_id: BuildId,
    /// The change.
    pub change_id: ChangeId,
    /// The provider copied from: the fabric's current one.
    pub source: String,
    /// The provider copied to: the one the Build names.
    pub destination: String,
}

/// What a copy did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copied {
    /// How many lines of the old provider, over its five stores, the new one holds verified in source
    /// order: all of them once the copy finishes.
    pub source_position: u64,
    /// How many lines this copy appended.
    pub copied: u64,
}

/// Why a copy did not finish.
#[derive(Debug)]
pub enum CopyError {
    /// The new provider's lines or claims are not the old one's, named; nothing was appended and
    /// nothing switches.
    Refused(String),
    /// A provider did not answer: neither copied nor refused; the copy is resumed.
    Storage(StorageError),
}

impl std::fmt::Display for CopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(why) => f.write_str(why),
            Self::Storage(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for CopyError {}

/// `op` as a refusal names it: `<node id>/<incarnation>#<seq>`.
fn named(op: Option<&Op>) -> String {
    op.map(|o| format!("{}/{}#{}", o.node_id, o.incarnation_id, o.seq)).unwrap_or_else(|| "no op".into())
}

/// Whether `a` and `b` are one write: the same op, format, key and record.
fn same_line(a: &Line, b: &Line) -> bool {
    a.op == b.op && a.format == b.format && a.key == b.key && a.record == b.record
}

/// Why `destination`'s lines of `store` are not a prefix of `source`'s, or `None` when they are.
fn mismatch(run: &CopyRun, store: Store, source: &[Line], destination: &[Line]) -> Option<(usize, String)> {
    let at = destination.iter().zip(source).position(|(d, s)| !same_line(d, s)).or((destination.len() > source.len()).then_some(source.len()))?;
    let d = &destination[at];
    let why = match source.get(at) {
        None => format!("its {store} store holds line {} at key {:?}, op {}, which the old provider `{}` does not hold (it holds {} lines)", at + 1, d.key, named(d.op.as_ref()), run.source, source.len()),
        Some(s) if s.op == d.op => format!("its {store} store's line {} at key {:?}, op {}, differs from the old provider `{}`'s line of that op", at + 1, d.key, named(d.op.as_ref()), run.source),
        Some(s) => format!("its {store} store's line {} is op {} at key {:?} where the old provider `{}` holds op {} at key {:?}", at + 1, named(d.op.as_ref()), d.key, run.source, named(s.op.as_ref()), s.key),
    };
    Some((at, why))
}

fn refused(run: &CopyRun, why: String) -> CopyError {
    CopyError::Refused(format!("the new topology storage provider `{}` is not a copy of the old one `{}` in source order: {why}; nothing is appended and nothing switches", run.destination, run.source))
}

/// Copy every line of every store of `source` to `destination` in source order, then every claim with
/// its exact claimant (storage.md §2 Copy), recorded on the [`COPY_SPAN`] span. `destination` must hold
/// a prefix of `source`'s lines in each store and only claims `source` holds with the same content,
/// else the copy is refused by name and nothing is appended; it appends what follows that prefix, so a
/// copy resumed after a lost answer appends nothing twice.
pub async fn copy(run: &CopyRun, source: &dyn ProviderLines, destination: &dyn ProviderLines) -> Result<Copied, CopyError> {
    let span = tracing::info_span!(
        "rdm.node_admin.build.update.via-storage-copy",
        build_id = %run.build_id.log(),
        change_id = %run.change_id,
        source = %run.source,
        destination = %run.destination,
        source_position = tracing::field::Empty,
        copied = tracing::field::Empty,
        outcome = tracing::field::Empty,
        cause = tracing::field::Empty,
    );
    let mut position = 0u64;
    let mut copied = 0u64;
    let result = copy_in(run, source, destination, &mut position, &mut copied).await;
    span.record("source_position", position).record("copied", copied);
    match &result {
        Ok(()) => span.record("outcome", "copied"),
        Err(CopyError::Refused(why)) => span.record("outcome", "refused").record("cause", why.as_str()),
        Err(CopyError::Storage(e)) => span.record("outcome", "indeterminate").record("cause", e.to_string()),
    };
    result.map(|()| Copied { source_position: position, copied })
}

async fn copy_in(run: &CopyRun, source: &dyn ProviderLines, destination: &dyn ProviderLines, position: &mut u64, copied: &mut u64) -> Result<(), CopyError> {
    let mut stores = Vec::with_capacity(Store::ALL.len());
    for store in Store::ALL {
        let from = source.lines(store).await.map_err(CopyError::Storage)?;
        let to = destination.lines(store).await.map_err(CopyError::Storage)?;
        if let Some((at, why)) = mismatch(run, store, &from, &to) {
            *position += at as u64;
            return Err(refused(run, why));
        }
        *position += to.len() as u64;
        stores.push((store, from, to.len()));
    }
    let held = source.claims().await.map_err(CopyError::Storage)?;
    let present = destination.claims().await.map_err(CopyError::Storage)?;
    for claim in &present {
        match held.iter().find(|c| c.kind == claim.kind && c.name == claim.name) {
            None => return Err(refused(run, format!("it holds the {} {:?}, which the old provider does not", claim.kind, claim.name))),
            Some(c) if c.content != claim.content => return Err(refused(run, format!("its {} {:?} names another claimant than the old provider's", claim.kind, claim.name))),
            Some(_) => {}
        }
    }
    for (store, from, verified) in &stores {
        for line in &from[*verified..] {
            destination.append_line(*store, line).await.map_err(CopyError::Storage)?;
            *position += 1;
            *copied += 1;
        }
    }
    for claim in held.iter().filter(|c| !present.iter().any(|p| p.kind == c.kind && p.name == c.name)) {
        match destination.insert_claim(claim).await.map_err(CopyError::Storage)? {
            ClaimTaken::Won => {}
            ClaimTaken::Held(content) if content == claim.content => {}
            ClaimTaken::Held(_) => return Err(CopyError::Refused(format!("the new topology storage provider `{}`'s {} {:?} was taken by another claimant during the copy: every line of the old provider `{}` is appended, the claim is not, and nothing switches", run.destination, claim.kind, claim.name, run.source))),
        }
    }
    Ok(())
}

/// RDM's file provider's lines and claims for one fabric, on the store dir it was registered at: the
/// five store files and the `locks/` of its root (`<store_dir>/<fabric_id>/`), and the fabric's claim
/// of its name in the fabrics' name index beside the roots (`<store_dir>/fabrics/`).
pub struct FileProviderLines {
    stores: Vec<(Store, StoreLog)>,
    locks: StoreLocks,
    names: PathBuf,
    fabric_id: crate::model::FabricId,
}

impl FileProviderLines {
    /// The file provider registered as `provider`, on the store dir `store_dir`, for the fabric
    /// `fabric_id`: its root opened (created when absent), each store file folded on a span naming
    /// `provider`.
    pub async fn open(provider: &str, store_dir: &Path, fabric_id: &crate::model::FabricId) -> Result<Self, StorageError> {
        let root = crate::store_root::StoreRoot::open(store_dir, fabric_id)?;
        let mut stores = Vec::with_capacity(Store::ALL.len());
        for store in Store::ALL {
            stores.push((store, StoreLog::open_for(provider, &root.dir().join(store.file()), crate::store_log::Formats::any()).await?));
        }
        let copier = crate::store_log::LogWriter::new(&crate::model::NodeId::mint(), &crate::model::IncarnationId::mint());
        Ok(Self { stores, locks: StoreLocks::at(root.dir(), &copier), names: store_dir.join(crate::store_root::FABRIC_NAMES), fabric_id: fabric_id.clone() })
    }

    fn log(&self, store: Store) -> &StoreLog {
        &self.stores.iter().find(|(s, _)| *s == store).expect("every store is opened").1
    }

    fn name_path(&self, name: &str) -> PathBuf {
        self.names.join(format!("{name}.json"))
    }
}

/// The fabric a claim of a fabric's name names: its record's `fabric_id`.
fn claimed_fabric(path: &Path, content: &[u8]) -> Result<Option<String>, StorageError> {
    let v: serde_json::Value = serde_json::from_slice(content).map_err(|e| StorageError::Unrecognised { file: path.display().to_string(), reason: format!("a fabric name claim that does not parse: {e}") })?;
    Ok(v["record"]["fabric_id"].as_str().map(str::to_string))
}

#[async_trait]
impl ProviderLines for FileProviderLines {
    async fn lines(&self, store: Store) -> Result<Vec<Line>, StorageError> {
        self.log(store).lines().await
    }

    async fn append_line(&self, store: Store, line: &Line) -> Result<(), StorageError> {
        self.log(store).append_line(line).await
    }

    async fn claims(&self) -> Result<Vec<Claim>, StorageError> {
        let mut claims = Vec::new();
        let (names, fabric) = (self.names.clone(), self.fabric_id.to_string());
        claims.extend(
            crate::blocking::blocking_io(move || -> Result<Vec<Claim>, StorageError> {
                let entries = match std::fs::read_dir(&names) {
                    Ok(e) => e,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                    Err(e) => return Err(StorageError::Io { file: names.display().to_string(), reason: e.to_string() }),
                };
                let mut out = Vec::new();
                for entry in entries {
                    let entry = entry.map_err(|e| StorageError::Io { file: names.display().to_string(), reason: e.to_string() })?;
                    let file = entry.file_name().to_string_lossy().into_owned();
                    let Some(name) = file.strip_suffix(".json").filter(|_| !file.starts_with('.')) else { continue };
                    let content = std::fs::read(entry.path()).map_err(|e| StorageError::Io { file: entry.path().display().to_string(), reason: e.to_string() })?;
                    if claimed_fabric(&entry.path(), &content)?.as_deref() == Some(fabric.as_str()) {
                        out.push(Claim { kind: ClaimKind::FabricName, name: name.to_string(), content });
                    }
                }
                Ok(out)
            })
            .await?,
        );
        for (kind, lock) in [(ClaimKind::Name, LockKind::Name), (ClaimKind::InFlight, LockKind::InFlight)] {
            claims.extend(self.locks.held(lock).await?.into_iter().map(|(name, content)| Claim { kind, name, content }));
        }
        claims.sort();
        Ok(claims)
    }

    async fn claim(&self, kind: ClaimKind, name: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let lock = match kind {
            ClaimKind::Name => LockKind::Name,
            ClaimKind::InFlight => LockKind::InFlight,
            ClaimKind::Commit => LockKind::Commit,
            ClaimKind::FabricName => return Ok(self.claims().await?.into_iter().find(|c| c.kind == kind && c.name == name).map(|c| c.content)),
        };
        self.locks.read(lock, name).await
    }

    async fn insert_claim(&self, claim: &Claim) -> Result<ClaimTaken, StorageError> {
        let lock = match claim.kind {
            ClaimKind::Name => LockKind::Name,
            ClaimKind::InFlight => LockKind::InFlight,
            ClaimKind::Commit => LockKind::Commit,
            ClaimKind::FabricName => {
                let (dir, path, content) = (self.names.clone(), self.name_path(&claim.name), claim.content.clone());
                let tmp = self.names.join(format!(".{}.storage-copy-{}.json.tmp", claim.name, std::process::id()));
                return crate::blocking::blocking_io(move || {
                    std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
                    if crate::record_store::insert_file(&dir, &tmp, &path, &content)? {
                        return Ok(ClaimTaken::Won);
                    }
                    std::fs::read(&path).map(ClaimTaken::Held).map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })
                })
                .await;
            }
        };
        Ok(match self.locks.take(lock, &claim.name, &claim.content).await? {
            LockTaken::Won => ClaimTaken::Won,
            LockTaken::Held(content) => ClaimTaken::Held(content),
        })
    }
}
