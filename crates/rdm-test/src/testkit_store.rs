//! The testkit store: a testkit-only key/value oracle on op `0x70` (i143.e7.s3).
//!
//! Ping proves a call reached a node; the testkit store proves WHICH birth it
//! reached and what state that birth holds. Its file lives in the node's data
//! dir, so a same-node restart (same data dir) still holds every value and a
//! replacement (fresh data dir) holds none. Every reply names the executing
//! node, its mesh and incarnation, and where the call
//! arrived on, and the op, so a test reads where a call landed from the reply
//! itself.
//!
//! `0x70` is a testkit family (`OpOwner::Testkit`): only the testkit rpc
//! node serves it, never a product binary. Each mutation writes the whole
//! store to a temp file, fsyncs it and renames it over the store file before
//! it answers, so an answered mutation survives a crash.

use crate::held_name::HeldName;
use rdm_core::launch::Launch;
use rdm_transport::{HandlerFault, PeerContext, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::{MalformedKind, ReplyKind};
use rdm_core::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The testkit store protocol on op `0x70`.
pub struct TestkitStore;

/// The longest key, in bytes.
pub const MAX_KEY_BYTES: usize = 256;
/// The longest value, in bytes.
pub const MAX_VALUE_BYTES: usize = 64 * 1024;

/// A testkit store call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TestkitStoreRequest {
    /// Read `key`.
    ///
    /// Position 0 on the wire.
    Get {
        /// The key to read.
        key: Vec<u8>,
    },
    /// Write `value` under `key`.
    ///
    /// Position 1 on the wire.
    Put {
        /// The key to write.
        key: Vec<u8>,
        /// The value to write.
        value: Vec<u8>,
    },
    /// Delete `key`.
    ///
    /// Position 2 on the wire.
    Delete {
        /// The key to delete.
        key: Vec<u8>,
    },
    /// Swap when the held value equals `expected` (`None`: the key is absent);
    /// `new: None` deletes the key.
    ///
    /// Position 3 on the wire.
    CompareAndSwap {
        /// The key to swap.
        key: Vec<u8>,
        /// The value the key must hold; `None` when it must be absent.
        expected: Option<Vec<u8>>,
        /// The value to store; `None` deletes the key.
        new: Option<Vec<u8>>,
    },
}

impl TestkitStoreRequest {
    /// The operation the request is.
    pub fn op(&self) -> TestkitStoreOp {
        match self {
            Self::Get { .. } => TestkitStoreOp::Get,
            Self::Put { .. } => TestkitStoreOp::Put,
            Self::Delete { .. } => TestkitStoreOp::Delete,
            Self::CompareAndSwap { .. } => TestkitStoreOp::CompareAndSwap,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Get { key, .. } | Self::Put { key, .. } | Self::Delete { key, .. } | Self::CompareAndSwap { key, .. } => key,
        }
    }
}

/// The operation a request or a reply concerns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TestkitStoreOp {
    /// Read.
    ///
    /// Position 0 on the wire.
    Get,
    /// Write.
    ///
    /// Position 1 on the wire.
    Put,
    /// Delete.
    ///
    /// Position 2 on the wire.
    Delete,
    /// Compare and swap.
    ///
    /// Position 3 on the wire.
    CompareAndSwap,
}

impl TestkitStoreOp {
    /// The operation's name as it appears in replies and spans.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Delete => "delete",
            Self::CompareAndSwap => "compare-and-swap",
        }
    }
}

/// Where a call executed, as the executing node knows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// The executing node's id.
    pub node_id: String,
    /// The executing node's separate path.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// The executing node's mesh.
    pub mesh: String,
    /// The executing node's incarnation id.
    pub incarnation_id: String,
    /// The operation executed.
    pub op: TestkitStoreOp,
}

impl Provenance {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

/// A testkit store answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TestkitStoreReply {
    /// The value of the key.
    ///
    /// Position 0 on the wire.
    Value {
        /// Where the call executed.
        at: Provenance,
        /// The value held.
        value: Vec<u8>,
    },
    /// The key is absent.
    ///
    /// Position 1 on the wire.
    Absent {
        /// Where the call executed.
        at: Provenance,
    },
    /// The value was stored.
    ///
    /// Position 2 on the wire.
    Stored {
        /// Where the call executed.
        at: Provenance,
    },
    /// The key was deleted.
    ///
    /// Position 3 on the wire.
    Deleted {
        /// Where the call executed.
        at: Provenance,
    },
    /// The swap was made.
    ///
    /// Position 4 on the wire.
    Swapped {
        /// Where the call executed.
        at: Provenance,
    },
    /// The held value differs from the expected one.
    ///
    /// Position 5 on the wire.
    Mismatch {
        /// Where the call executed.
        at: Provenance,
        /// The value held, `None` when absent.
        current: Option<Vec<u8>>,
    },
    /// A key or value over its limit; nothing was read or written.
    ///
    /// Position 6 on the wire.
    TooLarge {
        /// Where the call executed.
        at: Provenance,
        /// The field over its limit.
        field: String,
        /// The limit.
        limit: u32,
        /// The size received.
        got: u32,
    },
    /// The store file could not be read or written; nothing changed.
    ///
    /// Position 7 on the wire.
    StoreFailed {
        /// Where the call executed.
        at: Provenance,
        /// Why the store failed.
        reason: String,
    },
    /// The peer the call needed could not be resolved.
    ///
    /// Position 8 on the wire.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// The node is not ready to serve.
    ///
    /// Position 9 on the wire.
    NotReady {
        /// Why the node is not ready.
        reason: String,
    },
    /// The node is at its admission bound.
    ///
    /// Position 10 on the wire.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// The node is connections-deleting and takes no new work.
    ///
    /// Position 11 on the wire.
    ConnectionsDeleting {
        /// Why it refuses new work.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 12 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// The caller is not allowed this call.
    ///
    /// Position 13 on the wire.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
}

impl TestkitStoreReply {
    /// The executing node's provenance, on every reply the store itself made.
    pub fn provenance(&self) -> Option<&Provenance> {
        match self {
            Self::Value { at, .. }
            | Self::Absent { at }
            | Self::Stored { at }
            | Self::Deleted { at }
            | Self::Swapped { at }
            | Self::Mismatch { at, .. }
            | Self::TooLarge { at, .. }
            | Self::StoreFailed { at, .. } => Some(at),
            _ => None,
        }
    }
}

impl NodeProtocol for TestkitStore {
    const OP: u8 = 0x70;
    const NAME: &'static str = "testkit-store";
    // A compare-and-swap carries a key and two values.
    const MAX_REQUEST_FRAME_BYTES: usize = MAX_KEY_BYTES + 2 * MAX_VALUE_BYTES + 1024;
    const MAX_REPLY_FRAME_BYTES: usize = MAX_VALUE_BYTES + 2048;
    const FORWARDABLE: bool = true;
    const REQUEST_VARIANTS: u32 = 4;
    const REPLY_VARIANTS: u32 = 14;

    type Request = TestkitStoreRequest;
    type Reply = TestkitStoreReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            TestkitStoreReply::Value { .. } => "value",
            TestkitStoreReply::Absent { .. } => "absent",
            TestkitStoreReply::Stored { .. } => "stored",
            TestkitStoreReply::Deleted { .. } => "deleted",
            TestkitStoreReply::Swapped { .. } => "swapped",
            TestkitStoreReply::Mismatch { .. } => "mismatch",
            TestkitStoreReply::TooLarge { .. } => "too-large",
            TestkitStoreReply::StoreFailed { .. } => "store-failed",
            TestkitStoreReply::PeerUnresolved { .. } => "peer-unresolved",
            TestkitStoreReply::NotReady { .. } => "not-ready",
            TestkitStoreReply::Busy { .. } => "busy",
            TestkitStoreReply::ConnectionsDeleting { .. } => "connections-deleting",
            TestkitStoreReply::Malformed { .. } => "malformed",
            TestkitStoreReply::Unauthorized { .. } => "unauthorized",
        }
    }


    fn classify_reply(reply: &TestkitStoreReply) -> ReplyKind {
        match reply {
            TestkitStoreReply::Value { .. } => ReplyKind::Success,
            TestkitStoreReply::Absent { .. } => ReplyKind::Success,
            TestkitStoreReply::Stored { .. } => ReplyKind::Success,
            TestkitStoreReply::Deleted { .. } => ReplyKind::Success,
            TestkitStoreReply::Swapped { .. } => ReplyKind::Success,
            TestkitStoreReply::Mismatch { .. } => ReplyKind::Success,
            TestkitStoreReply::TooLarge { .. } => ReplyKind::new(rdm_core::outcome::Code::ResourceExhausted),
            TestkitStoreReply::StoreFailed { .. } => ReplyKind::new(rdm_core::outcome::Code::Internal),
            TestkitStoreReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            TestkitStoreReply::NotReady { .. } => ReplyKind::NotReady,
            TestkitStoreReply::Busy { .. } => ReplyKind::Busy,
            TestkitStoreReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            TestkitStoreReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            TestkitStoreReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }

    fn peer_unresolved(reason: String) -> TestkitStoreReply {
        TestkitStoreReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> TestkitStoreReply {
        TestkitStoreReply::NotReady { reason }
    }
    fn busy(reason: String) -> TestkitStoreReply {
        TestkitStoreReply::Busy { reason }
    }
    fn connections_deleting(reason: String) -> TestkitStoreReply {
        TestkitStoreReply::ConnectionsDeleting { reason }
    }
    fn malformed(kind: MalformedKind) -> TestkitStoreReply {
        TestkitStoreReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> TestkitStoreReply {
        TestkitStoreReply::Unauthorized { reason }
    }
}

/// The store file inside a node's data dir.
pub(crate) const STORE_FILE: &str = "testkit-store.json";
const FORMAT: &str = "testkit-store/1";

#[derive(Serialize, Deserialize)]
struct Stored {
    format: String,
    /// hex(key) -> hex(value)
    entries: BTreeMap<String, String>,
}

/// One node's testkit store, held in memory and in its data dir.
#[derive(Debug)]
pub struct FileTestkitStore {
    path: PathBuf,
    map: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl FileTestkitStore {
    /// Open the store in `data_dir`: what a previous incarnation of this node
    /// wrote there, or empty. A file this build does not recognise is refused
    /// by name, never read as empty.
    pub fn open(data_dir: &Path) -> Result<Self, String> {
        let path = data_dir.join(STORE_FILE);
        let map = match std::fs::read(&path) {
            Ok(bytes) => {
                let stored: Stored = serde_json::from_slice(&bytes).map_err(|e| format!("{} is not a testkit store: {e}", path.display()))?;
                if stored.format != FORMAT {
                    return Err(format!("{} has format {:?}; this build reads {FORMAT:?}", path.display(), stored.format));
                }
                let mut map = BTreeMap::new();
                for (k, v) in stored.entries {
                    let key = hex::decode(&k).map_err(|e| format!("{}: key {k:?}: {e}", path.display()))?;
                    let value = hex::decode(&v).map_err(|e| format!("{}: value of {k:?}: {e}", path.display()))?;
                    map.insert(key, value);
                }
                map
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(format!("reading {}: {e}", path.display())),
        };
        Ok(Self { path, map: Mutex::new(map) })
    }

    /// The store file's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Persist `next` (temp, fsync, rename, directory fsync); only then does
    /// it become the held map.
    fn persist(&self, held: &mut BTreeMap<Vec<u8>, Vec<u8>>, next: BTreeMap<Vec<u8>, Vec<u8>>) -> Result<(), String> {
        use std::io::Write as _;
        let stored = Stored { format: FORMAT.into(), entries: next.iter().map(|(k, v)| (hex::encode(k), hex::encode(v))).collect() };
        let bytes = serde_json::to_vec(&stored).map_err(|e| format!("encoding {}: {e}", self.path.display()))?;
        let tmp = self.path.with_extension("json.tmp");
        let io = |what: &str, e: std::io::Error| format!("{what} {}: {e}", tmp.display());
        let mut f = std::fs::File::create(&tmp).map_err(|e| io("creating", e))?;
        f.write_all(&bytes).map_err(|e| io("writing", e))?;
        f.sync_all().map_err(|e| io("syncing", e))?;
        std::fs::rename(&tmp, &self.path).map_err(|e| format!("renaming {} over {}: {e}", tmp.display(), self.path.display()))?;
        if let Some(dir) = self.path.parent().and_then(|d| std::fs::File::open(d).ok()) {
            let _ = dir.sync_all();
        }
        *held = next;
        Ok(())
    }

    /// Execute one request against this store; `at` names where.
    pub fn apply(&self, req: TestkitStoreRequest, at: Provenance) -> TestkitStoreReply {
        let too_large = |field: &str, limit: usize, got: usize| TestkitStoreReply::TooLarge {
            at: at.clone(),
            field: field.into(),
            limit: limit as u32,
            got: got as u32,
        };
        if req.key().len() > MAX_KEY_BYTES {
            return too_large("key", MAX_KEY_BYTES, req.key().len());
        }
        for (field, v) in match &req {
            TestkitStoreRequest::Put { value, .. } => vec![("value", Some(value))],
            TestkitStoreRequest::CompareAndSwap { expected, new, .. } => vec![("expected", expected.as_ref()), ("new", new.as_ref())],
            _ => vec![],
        } {
            if let Some(v) = v.filter(|v| v.len() > MAX_VALUE_BYTES) {
                return too_large(field, MAX_VALUE_BYTES, v.len());
            }
        }
        let mut held = self.map.lock().unwrap();
        let write = |held: &mut BTreeMap<Vec<u8>, Vec<u8>>, next, ok: TestkitStoreReply| match self.persist(held, next) {
            Ok(()) => ok,
            Err(reason) => TestkitStoreReply::StoreFailed { at: at.clone(), reason },
        };
        match req {
            TestkitStoreRequest::Get { key, .. } => match held.get(&key) {
                Some(v) => TestkitStoreReply::Value { at, value: v.clone() },
                None => TestkitStoreReply::Absent { at },
            },
            TestkitStoreRequest::Put { key, value, .. } => {
                let mut next = held.clone();
                next.insert(key, value);
                write(&mut held, next, TestkitStoreReply::Stored { at: at.clone() })
            }
            TestkitStoreRequest::Delete { key, .. } => {
                if !held.contains_key(&key) {
                    return TestkitStoreReply::Absent { at };
                }
                let mut next = held.clone();
                next.remove(&key);
                write(&mut held, next, TestkitStoreReply::Deleted { at: at.clone() })
            }
            TestkitStoreRequest::CompareAndSwap { key, expected, new, .. } => {
                let current = held.get(&key).cloned();
                if current != expected {
                    return TestkitStoreReply::Mismatch { at, current };
                }
                let mut next = held.clone();
                match new {
                    Some(v) => next.insert(key, v),
                    None => next.remove(&key),
                };
                write(&mut held, next, TestkitStoreReply::Swapped { at: at.clone() })
            }
        }
    }
}

/// One failpoint of a testkit-store call: armed for the NEXT call to cross it, it holds that call's
/// handler, signals that it holds (`reached`), and waits for `release`. Shaped like
/// [`rdm_transport::Failpoint`], which it carries. Testkit only; the product handler has none.
#[derive(Default)]
pub struct ApplyCut {
    armed: AtomicBool,
    holds: AtomicU64,
    point: rdm_transport::Failpoint,
}

impl ApplyCut {
    /// The next call to cross this cut holds there.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Resolves once a handler is held at this cut: the active-injection acknowledgement.
    pub async fn reached(&self) {
        self.point.reached.notified().await
    }

    /// Let the held handler go on.
    pub fn release(&self) {
        self.point.release.notify_one()
    }

    /// How many handlers have held here since the process began.
    pub fn holds(&self) -> u64 {
        self.holds.load(Ordering::SeqCst)
    }

    async fn pass(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.holds.fetch_add(1, Ordering::SeqCst);
            self.point.reached.notify_one();
            self.point.release.notified().await;
        }
    }
}

/// The two apply cuts of a testkit-store call and the counters that say what crossed them.
#[derive(Default)]
pub struct ApplyCuts {
    /// The handler is running, the request complete and finished, nothing applied yet.
    pub before_apply: ApplyCut,
    /// The mutation is applied and stored; the reply is not yet returned.
    pub after_apply: ApplyCut,
    /// Handlers that began (every dispatched call enters once).
    pub entered: AtomicU64,
    /// Mutations (put, delete, swap) applied and stored.
    pub applied: AtomicU64,
}

/// Serve `store` (opened from `launch`'s data dir). Every reply's provenance
/// names this birth, by the name it holds at that call (`name`).
pub fn serve(b: ServerBuilder, store: Arc<FileTestkitStore>, launch: &Launch, name: &HeldName) -> ServerBuilder {
    serve_with_cuts(b, store, launch, name, Arc::new(ApplyCuts::default()))
}

/// [`serve`], with the handler crossing `cuts` before and after its apply.
pub fn serve_with_cuts(b: ServerBuilder, store: Arc<FileTestkitStore>, launch: &Launch, name: &HeldName, cuts: Arc<ApplyCuts>) -> ServerBuilder {
    let (node_id, mesh, incarnation, name) = (launch.node_id.to_string(), launch.mesh.clone(), launch.incarnation.to_string(), name.clone());
    b.serve::<TestkitStore, _, _>(OpOwner::Testkit, move |_peer: PeerContext, req: TestkitStoreRequest| {
        let held = name.now();
        let at = Provenance {
            node_id: node_id.clone(),
            path: held.path,
            name: held.name,
            mesh: mesh.clone(),
            incarnation_id: incarnation.clone(),
            op: req.op(),
        };
        let (store, cuts) = (store.clone(), cuts.clone());
        async move {
            let span = tracing::info_span!(
                "rdm.node_rpc.testkit_store.serve.via-request",
                node_path = %at.path, node_name = %at.name,
                node_id = %at.node_id,
                incarnation_id = %at.incarnation_id,
                op = at.op.as_str(),
                outcome = tracing::field::Empty,
            );
            // The span covers the whole handler, so a held call's span lasts as long as it holds.
            let work = {
                let span = span.clone();
                async move {
                    cuts.entered.fetch_add(1, Ordering::SeqCst);
                    cuts.before_apply.pass().await;
                    let reply = tokio::task::spawn_blocking(move || store.apply(req, at))
                        .await
                        .map_err(|e| HandlerFault::invariant_broken(format!("testkit store task: {e}")))?;
                    span.record("outcome", outcome_name(&reply));
                    if matches!(reply, TestkitStoreReply::Stored { .. } | TestkitStoreReply::Deleted { .. } | TestkitStoreReply::Swapped { .. }) {
                        cuts.applied.fetch_add(1, Ordering::SeqCst);
                    }
                    cuts.after_apply.pass().await;
                    Ok::<_, HandlerFault>(reply)
                }
            };
            use tracing::Instrument;
            work.instrument(span).await
        }
    })
}

fn outcome_name(r: &TestkitStoreReply) -> &'static str {
    match r {
        TestkitStoreReply::Value { .. } => "value",
        TestkitStoreReply::Absent { .. } => "absent",
        TestkitStoreReply::Stored { .. } => "stored",
        TestkitStoreReply::Deleted { .. } => "deleted",
        TestkitStoreReply::Swapped { .. } => "swapped",
        TestkitStoreReply::Mismatch { .. } => "mismatch",
        TestkitStoreReply::TooLarge { .. } => "too-large",
        TestkitStoreReply::StoreFailed { .. } => "store-failed",
        _ => "refused",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(op: TestkitStoreOp) -> Provenance {
        Provenance {
            node_id: "n1".into(),
            path: "mesh1.node".into(), name: "1".into(),
            mesh: "mesh1".into(),
            incarnation_id: "i1".into(),
            op,
        }
    }

    fn dir() -> PathBuf {
        static MADE: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());
        extern "C" fn remove_made() {
            for d in MADE.lock().unwrap_or_else(|e| e.into_inner()).drain(..) {
                let _ = std::fs::remove_dir_all(d);
            }
        }
        let d = std::env::temp_dir().join(format!("testkit-store-{}-{}", std::process::id(), rand_suffix()));
        std::fs::create_dir_all(&d).unwrap();
        let mut made = MADE.lock().unwrap();
        if made.is_empty() {
            // Every directory a test of this binary made is removed when the binary exits.
            unsafe { libc::atexit(remove_made) };
        }
        made.push(d.clone());
        d
    }

    /// Unique within the process: tests run in parallel threads, and two calls in one clock tick would
    /// otherwise share a directory and rename each other's temporary file away.
    fn rand_suffix() -> String {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{}-{n}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
    }

    fn get(s: &FileTestkitStore, k: &[u8]) -> TestkitStoreReply {
        s.apply(TestkitStoreRequest::Get { key: k.to_vec() }, at(TestkitStoreOp::Get))
    }

    #[test]
    fn every_op_answers_its_typed_result_and_names_where_it_ran() {
        let d = dir();
        let s = FileTestkitStore::open(&d).unwrap();
        assert_eq!(get(&s, b"k"), TestkitStoreReply::Absent { at: at(TestkitStoreOp::Get) });
        let put = |k: &[u8], v: &[u8]| s.apply(TestkitStoreRequest::Put { key: k.into(), value: v.into() }, at(TestkitStoreOp::Put));
        assert_eq!(put(b"k", b"v1"), TestkitStoreReply::Stored { at: at(TestkitStoreOp::Put) });
        assert_eq!(get(&s, b"k"), TestkitStoreReply::Value { at: at(TestkitStoreOp::Get), value: b"v1".to_vec() });
        let cas = |e: Option<&[u8]>, n: Option<&[u8]>| {
            s.apply(
                TestkitStoreRequest::CompareAndSwap { key: b"k".to_vec(), expected: e.map(|v| v.to_vec()), new: n.map(|v| v.to_vec()) },
                at(TestkitStoreOp::CompareAndSwap),
            )
        };
        assert_eq!(cas(Some(b"nope"), Some(b"v2")), TestkitStoreReply::Mismatch { at: at(TestkitStoreOp::CompareAndSwap), current: Some(b"v1".to_vec()) });
        assert_eq!(cas(Some(b"v1"), Some(b"v2")), TestkitStoreReply::Swapped { at: at(TestkitStoreOp::CompareAndSwap) });
        assert_eq!(cas(Some(b"v2"), None), TestkitStoreReply::Swapped { at: at(TestkitStoreOp::CompareAndSwap) }, "new: None deletes");
        assert_eq!(get(&s, b"k"), TestkitStoreReply::Absent { at: at(TestkitStoreOp::Get) });
        assert_eq!(cas(None, Some(b"v3")), TestkitStoreReply::Swapped { at: at(TestkitStoreOp::CompareAndSwap) }, "expected: None means absent");
        assert_eq!(cas(None, Some(b"v4")), TestkitStoreReply::Mismatch { at: at(TestkitStoreOp::CompareAndSwap), current: Some(b"v3".to_vec()) });
        let del = |k: &[u8]| s.apply(TestkitStoreRequest::Delete { key: k.into() }, at(TestkitStoreOp::Delete));
        assert_eq!(del(b"k"), TestkitStoreReply::Deleted { at: at(TestkitStoreOp::Delete) });
        assert_eq!(del(b"k"), TestkitStoreReply::Absent { at: at(TestkitStoreOp::Delete) });
    }

    #[test]
    fn the_same_data_dir_keeps_every_value_and_a_fresh_one_holds_none() {
        let d = dir();
        let first = FileTestkitStore::open(&d).unwrap();
        first.apply(TestkitStoreRequest::Put { key: b"41".to_vec(), value: b"before-restart".to_vec() }, at(TestkitStoreOp::Put));
        drop(first);
        let restarted = FileTestkitStore::open(&d).unwrap();
        assert_eq!(get(&restarted, b"41"), TestkitStoreReply::Value { at: at(TestkitStoreOp::Get), value: b"before-restart".to_vec() });
        let replacement = FileTestkitStore::open(&dir()).unwrap();
        assert_eq!(get(&replacement, b"41"), TestkitStoreReply::Absent { at: at(TestkitStoreOp::Get) });
    }

    #[test]
    fn an_oversized_key_or_value_is_refused_by_name_and_writes_nothing() {
        let d = dir();
        let s = FileTestkitStore::open(&d).unwrap();
        let r = s.apply(TestkitStoreRequest::Get { key: vec![0; MAX_KEY_BYTES + 1] }, at(TestkitStoreOp::Get));
        assert_eq!(r, TestkitStoreReply::TooLarge { at: at(TestkitStoreOp::Get), field: "key".into(), limit: 256, got: 257 });
        let r = s.apply(TestkitStoreRequest::Put { key: b"k".to_vec(), value: vec![0; MAX_VALUE_BYTES + 1] }, at(TestkitStoreOp::Put));
        assert_eq!(r, TestkitStoreReply::TooLarge { at: at(TestkitStoreOp::Put), field: "value".into(), limit: 65536, got: 65537 });
        let r = s.apply(
            TestkitStoreRequest::CompareAndSwap { key: b"k".to_vec(), expected: None, new: Some(vec![0; MAX_VALUE_BYTES + 1]) },
            at(TestkitStoreOp::CompareAndSwap),
        );
        assert_eq!(r, TestkitStoreReply::TooLarge { at: at(TestkitStoreOp::CompareAndSwap), field: "new".into(), limit: 65536, got: 65537 });
        assert!(!d.join(STORE_FILE).exists(), "nothing was written");
        let at_limit = s.apply(TestkitStoreRequest::Put { key: vec![1; MAX_KEY_BYTES], value: vec![2; MAX_VALUE_BYTES] }, at(TestkitStoreOp::Put));
        assert_eq!(at_limit, TestkitStoreReply::Stored { at: at(TestkitStoreOp::Put) }, "exactly at the limits is accepted");
    }

    #[test]
    fn a_store_file_this_build_does_not_recognise_is_refused_by_name() {
        let d = dir();
        std::fs::write(d.join(STORE_FILE), br#"{"format":"testkit-store/9","entries":{}}"#).unwrap();
        let e = FileTestkitStore::open(&d).unwrap_err();
        assert!(e.contains("testkit-store/9") && e.contains(STORE_FILE), "{e}");
        std::fs::write(d.join(STORE_FILE), b"garbage").unwrap();
        assert!(FileTestkitStore::open(&d).unwrap_err().contains("is not a testkit store"));
    }

    #[test]
    fn a_failed_write_changes_nothing_and_names_the_file() {
        let d = dir();
        let s = FileTestkitStore::open(&d).unwrap();
        s.apply(TestkitStoreRequest::Put { key: b"k".to_vec(), value: b"v1".to_vec() }, at(TestkitStoreOp::Put));
        // A directory where the temp file goes makes the write fail.
        std::fs::create_dir_all(d.join(STORE_FILE).with_extension("json.tmp")).unwrap();
        let r = s.apply(TestkitStoreRequest::Put { key: b"k".to_vec(), value: b"v2".to_vec() }, at(TestkitStoreOp::Put));
        assert!(matches!(&r, TestkitStoreReply::StoreFailed { reason, .. } if reason.contains("json.tmp")), "{r:?}");
        assert_eq!(get(&s, b"k"), TestkitStoreReply::Value { at: at(TestkitStoreOp::Get), value: b"v1".to_vec() }, "the held value is unchanged");
    }

    #[test]
    fn the_protocol_is_testkit_forwardable_and_every_variant_round_trips() {
        assert_eq!(TestkitStore::OP, 0x70);
        assert!(TestkitStore::FORWARDABLE);
        let a = at(TestkitStoreOp::Get);
        let replies = vec![
            TestkitStoreReply::Value { at: a.clone(), value: vec![1] },
            TestkitStoreReply::Absent { at: a.clone() },
            TestkitStoreReply::Stored { at: a.clone() },
            TestkitStoreReply::Deleted { at: a.clone() },
            TestkitStoreReply::Swapped { at: a.clone() },
            TestkitStoreReply::Mismatch { at: a.clone(), current: None },
            TestkitStoreReply::TooLarge { at: a.clone(), field: "key".into(), limit: 1, got: 2 },
            TestkitStoreReply::StoreFailed { at: a.clone(), reason: "r".into() },
            TestkitStoreReply::PeerUnresolved { reason: "p".into() },
            TestkitStoreReply::NotReady { reason: "n".into() },
            TestkitStoreReply::Busy { reason: "b".into() },
            TestkitStoreReply::ConnectionsDeleting { reason: "d".into() },
            TestkitStoreReply::Malformed { kind: MalformedKind::Corrupt },
            TestkitStoreReply::Unauthorized { reason: "u".into() },
        ];
        assert_eq!(replies.len() as u32, TestkitStore::REPLY_VARIANTS);
        for r in replies {
            assert_eq!(TestkitStore::decode_reply(&TestkitStore::encode_reply(&r).unwrap()).unwrap(), r);
        }
        let requests = vec![
            TestkitStoreRequest::Get { key: vec![1] },
            TestkitStoreRequest::Put { key: vec![1], value: vec![2] },
            TestkitStoreRequest::Delete { key: vec![1] },
            TestkitStoreRequest::CompareAndSwap { key: vec![1], expected: None, new: Some(vec![3]) },
        ];
        assert_eq!(requests.len() as u32, TestkitStore::REQUEST_VARIANTS);
        for r in requests {
            assert_eq!(TestkitStore::decode_request(&TestkitStore::encode_request(&r).unwrap()).unwrap(), r);
        }
    }
}
