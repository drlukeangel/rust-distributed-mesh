//! The copy of a storage change (storage.md §2, "Changing it", step 3): "The fabric-primary, the one
//! reader of every store, appends every line of every store on the old provider to the new one in
//! source order, dead births' and deleted nodes' records included. A copy resumed after a lost answer
//! recognises the lines already there and appends none twice; a new provider holding lines the old one does not is refused by name, and
//! nothing switches."
//!
//! A provider answers the copy through [`ProviderLines`]: its stores as their lines, each with the
//! op its writer wrote it with. It stores and returns what it is given and decides nothing (Q109); [`copy`] decides. The copy runs once changes are blocked and the
//! outstanding writes have finished (steps 1 and 2), so neither provider changes beneath it.
//!
//! [`copy`] first verifies that each of the new provider's stores holds a prefix of the old one's
//! lines, line for line; a store that does not is refused by name, naming the store, the key and the
//! ops that differ, never a record, and nothing is appended. It then appends the old provider's lines
//! after that prefix, store by store in source order.
//! The new provider's verified lines are the copy's progress: a copy resumed after a lost answer
//! verifies them again and appends only what follows.

use crate::storage_error::StorageError;
use crate::store_log::{Line, Op, StoreLog};
use async_trait::async_trait;
use rdm_core::ChangeId;
use std::path::Path;

/// The span of a copy: the change, both providers, the source position verified and the lines copied
/// (i149 plan, "OTLP span-assertion bar").
pub const COPY_SPAN: &str = "rdm.node_admin.storage.update.via-copy";

/// One of the stores of a provider (storage.md §2).
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
    /// `events.storage`.
    Events,
}

impl Store {
    /// The stores, in the order a copy takes them.
    pub const ALL: [Store; 6] = [Store::Fabric, Store::Meshes, Store::Nodes, Store::Connections, Store::Builds, Store::Events];

    /// The store's name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Fabric => "fabric",
            Self::Meshes => "meshes",
            Self::Nodes => "nodes",
            Self::Connections => "connections",
            Self::Builds => "builds",
            Self::Events => "events",
        }
    }

    /// The store's file under a file provider's root.
    pub fn file(self) -> &'static str {
        use crate::store_root::{BUILDS_FILE, CONNECTIONS_FILE, EVENTS_FILE, FABRIC_FILE, MESHES_FILE, NODES_FILE};
        match self {
            Self::Fabric => FABRIC_FILE,
            Self::Meshes => MESHES_FILE,
            Self::Nodes => NODES_FILE,
            Self::Connections => CONNECTIONS_FILE,
            Self::Builds => BUILDS_FILE,
            Self::Events => EVENTS_FILE,
        }
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A provider's stores as their lines: what a copy reads from the old provider and appends to the new
/// one. Persistence only: every method stores or returns what it is given.
#[async_trait]
pub trait ProviderLines: Send + Sync {
    /// Every line of `store`, in source order, each with its op.
    async fn lines(&self, store: Store) -> Result<Vec<Line>, StorageError>;
    /// Append `line` to `store` as its writer wrote it, its op kept.
    async fn append_line(&self, store: Store, line: &Line) -> Result<(), StorageError>;
}

/// The change a copy is a step of, and the registered names of its two providers.
#[derive(Debug, Clone)]
pub struct CopyRun {
    /// The change.
    pub change_id: ChangeId,
    /// The provider copied from: the fabric's current one.
    pub source: String,
    /// The provider copied to: the one the change names.
    pub destination: String,
}

/// What a copy did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copied {
    /// How many lines of the old provider, over its stores, the new one holds verified in source
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
    /// A provider did not answer ([`StorageError::answered`] false): neither copied nor refused; the
    /// copy is resumed.
    Storage(StorageError),
    /// A provider answered with a named error (it refused by name, or holds what this build does not
    /// recognise), named with the provider: the copy fails, and nothing switches.
    Failed(String),
}

impl std::fmt::Display for CopyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(why) | Self::Failed(why) => f.write_str(why),
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

/// Copy every line of every store of `source` to `destination` in source order (storage.md §2 Copy),
/// recorded on the [`COPY_SPAN`] span. `destination` must hold a prefix of `source`'s lines in each
/// store, else the copy is refused by name and nothing is appended; it appends what follows that prefix, so a
/// copy resumed after a lost answer appends nothing twice.
pub async fn copy(run: &CopyRun, source: &dyn ProviderLines, destination: &dyn ProviderLines) -> Result<Copied, CopyError> {
    let span = tracing::info_span!(
        "rdm.node_admin.storage.update.via-copy",
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
        Err(CopyError::Failed(why)) => span.record("outcome", "failed").record("cause", why.as_str()),
        Err(CopyError::Storage(e)) => span.record("outcome", "indeterminate").record("cause", e.to_string()),
    };
    result.map(|()| Copied { source_position: position, copied })
}

/// `e`, an error `provider` gave the copy: answered, the copy fails naming it; else no answer.
fn of(provider: &str) -> impl Fn(StorageError) -> CopyError + '_ {
    move |e| if e.answered() { CopyError::Failed(format!("the topology storage provider `{provider}` answered the copy with an error: {e}; nothing switches")) } else { CopyError::Storage(e) }
}

async fn copy_in(run: &CopyRun, source: &dyn ProviderLines, destination: &dyn ProviderLines, position: &mut u64, copied: &mut u64) -> Result<(), CopyError> {
    let (from_source, from_destination) = (of(&run.source), of(&run.destination));
    let mut stores = Vec::with_capacity(Store::ALL.len());
    for store in Store::ALL {
        let from = source.lines(store).await.map_err(&from_source)?;
        let to = destination.lines(store).await.map_err(&from_destination)?;
        if let Some((at, why)) = mismatch(run, store, &from, &to) {
            *position += at as u64;
            return Err(refused(run, why));
        }
        *position += to.len() as u64;
        stores.push((store, from, to.len()));
    }
    for (store, from, verified) in &stores {
        for line in &from[*verified..] {
            destination.append_line(*store, line).await.map_err(&from_destination)?;
            *position += 1;
            *copied += 1;
        }
    }
    Ok(())
}

/// RDM's file provider's lines for one fabric, on the store dir it was registered at: the store files
/// of its root (`<store_dir>/<fabric_id>/`).
pub struct FileProviderLines {
    stores: Vec<(Store, StoreLog)>,
}

impl FileProviderLines {
    /// The file provider registered as `provider`, on the store dir `store_dir`, for the fabric
    /// `fabric_id`: its root opened (created when absent), each store file folded on a span naming
    /// `provider`.
    pub async fn open(provider: &str, store_dir: &Path, fabric_id: &crate::model::FabricId) -> Result<Self, StorageError> {
        // The root is made (when absent) and fsynced on the blocking pool: no worker thread waits on the disk.
        let (dir, id) = (store_dir.to_path_buf(), fabric_id.clone());
        let root = crate::blocking::blocking_io(move || crate::store_root::StoreRoot::open(&dir, &id)).await?;
        let mut stores = Vec::with_capacity(Store::ALL.len());
        for store in Store::ALL {
            stores.push((store, StoreLog::open_for(provider, &root.dir().join(store.file()), crate::store_log::Formats::any()).await?));
        }
        Ok(Self { stores })
    }

    fn log(&self, store: Store) -> &StoreLog {
        &self.stores.iter().find(|(s, _)| *s == store).expect("every store is opened").1
    }
}

#[async_trait]
impl ProviderLines for FileProviderLines {
    async fn lines(&self, store: Store) -> Result<Vec<Line>, StorageError> {
        self.log(store).lines().await
    }

    async fn append_line(&self, store: Store, line: &Line) -> Result<(), StorageError> {
        self.log(store).append_line(line).await
    }

}
