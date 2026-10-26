//! The copy of a storage change (storage.md §2, "Changing it", step 3): "The fabric-primary, the one
//! reader of every store, appends every line of every store on the old provider to the new one in
//! source order, dead births' and deleted nodes' records included, and every name claim and in-flight
//! row with its exact claimant. A copy resumed after a lost answer recognises the lines already there
//! and appends none twice; a new provider holding lines the old one does not is refused by name, and
//! nothing switches."
//!
//! Functional providers, no estate: the old provider is RDM's file provider on one store dir
//! (registered `file`), the new one the same backend on another (registered `target`). The old
//! provider's history is written through the store log and the store locks as its writers write it;
//! the new provider's is read back the same way.
//!
//! The copy's span is `rdm.node_admin.build.update.via-storage-copy` (i149 plan, "OTLP span-assertion
//! bar"), with `build_id`, `change_id`, `source`, `destination`, `source_position`, `copied` and
//! `outcome`:
//! - `source_position`: how many lines of the old provider, over its five stores, the new provider
//!   holds verified in source order when the copy ends (on a refusal, the lines verified before the
//!   first mismatch);
//! - `copied`: how many lines this copy appended to the new provider;
//! - `outcome`: `copied`, `refused` (the new provider's lines are not a prefix of the old one's), or
//!   `indeterminate` (a provider did not answer).
//!
//! - `a_copy_preserves_current_values_across_writer_changes`
//! - `a_copy_refuses_a_destination_hole_without_replaying_stale_state`
//! - `a_copy_resumes_after_a_lost_answer_without_duplicate_records`
//! - `a_provider_change_preserves_held_names_and_attempts`

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use rdm_core::{BuildId, ChangeId, FabricId, IncarnationId, NodeId};
use rdm_node::storage_error::StorageError;
use rdm_node::store_log::{self, Formats, Line, LockKind, LockTaken, LogWriter, Op, StoreLocks, StoreLog};
use rdm_node::store_root::{StoreRoot, BUILDS_FILE, CONNECTIONS_FILE, FABRIC_FILE, MESHES_FILE, NODES_FILE};
use serde_json::json;

use rdm_node::storage_copy::{copy, Claim, ClaimTaken, CopyError, CopyRun, FileProviderLines, ProviderLines, Store};

const COPY_SPAN: &str = "rdm.node_admin.build.update.via-storage-copy";
const OPEN_SPAN: &str = "rdm.store.open.via-fold";
const SOURCE: &str = "file";
const DESTINATION: &str = "target";
/// An application setting a node record carries: never in a refusal or a span.
const APP_SETTING: &str = "app-setting-0f3a9c";

const FILES: [(Store, &str); 5] = [(Store::Fabric, FABRIC_FILE), (Store::Meshes, MESHES_FILE), (Store::Nodes, NODES_FILE), (Store::Connections, CONNECTIONS_FILE), (Store::Builds, BUILDS_FILE)];

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("rdm-storage-copy-{tag}-{}", NodeId::mint()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn capture(tag: &str) -> rdm_transport::EvidenceCapture {
    rdm_transport::EvidenceCapture::start(&dir(&format!("{tag}-evidence")), "rdm-storage-copy").expect("the cell's evidence file can be created")
}

fn run() -> CopyRun {
    CopyRun { build_id: BuildId::mint(), change_id: ChangeId::mint(), source: SOURCE.into(), destination: DESTINATION.into() }
}

/// A line as a store holds it, its place in the file aside: its op, format, key and record.
fn line_of(l: &Line) -> (Option<Op>, String, String, serde_json::Value) {
    (l.op.clone(), l.format.clone(), l.key.clone(), l.record.clone())
}

/// Every line of `file` under the root `root`, as [`line_of`].
fn file_lines(root: &Path, file: &str) -> Vec<(Option<Op>, String, String, serde_json::Value)> {
    store_log::read_file(&root.join(file)).unwrap_or_else(|e| panic!("{file} reads: {e}")).iter().map(line_of).collect()
}

/// The bytes of every file under `store_dir`: the fabric's root, its store files and locks, and the
/// fabrics' name index.
fn snapshot(store_dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(at: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        let Ok(entries) = std::fs::read_dir(at) else { return };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.insert(p.clone(), std::fs::read(&p).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(store_dir, &mut out);
    out
}

/// The attribute `key` of span `s`, as text.
fn attr(s: &serde_json::Value, key: &str) -> Option<String> {
    match &s["attributes"][key] {
        serde_json::Value::Null => None,
        serde_json::Value::String(v) => Some(v.clone()),
        v => Some(v.to_string()),
    }
}

fn spans_named<'a>(spans: &'a [serde_json::Value], name: &str) -> Vec<&'a serde_json::Value> {
    spans.iter().filter(|s| s["name"] == name).collect()
}

/// `op` as a refusal names it: `<node id>/<incarnation>#<seq>`.
fn named(op: &Op) -> String {
    format!("{}/{}#{}", op.node_id, op.incarnation_id, op.seq)
}

fn writer() -> LogWriter {
    LogWriter::new(&NodeId::mint(), &IncarnationId::mint())
}

async fn log(root: &Path, file: &str) -> StoreLog {
    StoreLog::open(&root.join(file), Formats::any()).await.unwrap_or_else(|e| panic!("{file} opens: {e}"))
}

/// The old provider's history: every store written by writers that change over the fabric's life.
/// The fabric-primary's first birth (`fp1`) dies and its successor (`fp2`) writes the same keys; a
/// mesh-primary's first birth (`mp1`) dies and `mp2` re-creates a node at the same path.name with a new
/// node id, then deletes the first; a node `gone` whose connection facts stay after it is deleted;
/// Build facts of both fabric-primaries, one of them repeated by the successor at the same key.
struct History {
    store_dir: PathBuf,
    root: PathBuf,
    fabric: FabricId,
    fp1: LogWriter,
    fp2: LogWriter,
    gone: LogWriter,
    live: LogWriter,
    /// The node records' first and second node ids at `mesh1.rpc.2`.
    first: NodeId,
    second: NodeId,
}

impl History {
    async fn write() -> Self {
        let store_dir = dir("source");
        let fabric = FabricId::mint();
        let root = StoreRoot::open(&store_dir, &fabric).unwrap().dir().to_path_buf();
        let (fp1, fp2, mp1, mp2, gone, live) = (writer(), writer(), writer(), writer(), writer(), writer());
        let (mesh1, mesh2) = (rdm_core::MeshId::mint(), rdm_core::MeshId::mint());
        let (first, second) = (NodeId::mint(), NodeId::mint());
        let build = BuildId::mint();

        let f = log(&root, FABRIC_FILE).await;
        f.append(&fp1, "fabric-record/2", fabric.as_str(), &json!({"fabric_id": fabric, "name": "fabric1", "events": []})).await.unwrap();
        f.append(&fp2, "fabric-record/2", fabric.as_str(), &json!({"fabric_id": fabric, "name": "fabric1", "events": ["ready-for-traffic"]})).await.unwrap();

        let m = log(&root, MESHES_FILE).await;
        m.append(&fp1, "mesh-record/3", mesh1.as_str(), &json!({"mesh_id": mesh1, "name": "mesh1", "state": "Ready"})).await.unwrap();
        m.append(&fp1, "mesh-record/3", mesh2.as_str(), &json!({"mesh_id": mesh2, "name": "mesh2", "state": "Ready"})).await.unwrap();
        m.append(&fp2, "mesh-record/3", mesh2.as_str(), &json!({"mesh_id": mesh2, "name": "mesh2", "state": "Dead"})).await.unwrap();
        m.append(&fp2, "mesh-record/3", mesh1.as_str(), &json!({"mesh_id": mesh1, "name": "mesh1", "state": "Ready", "config": {"setting": APP_SETTING}})).await.unwrap();

        let n = log(&root, NODES_FILE).await;
        n.append(&mp1, "node-record/1", first.as_str(), &json!({"node_id": first, "name": "mesh1.rpc.2", "state": "Running", "config": {"setting": APP_SETTING}})).await.unwrap();
        n.append(&mp1, "node-runtime/1", &format!("{first}--{}", IncarnationId::mint()), &json!({"node_id": first, "name": "mesh1.rpc.2"})).await.unwrap();
        n.append(&mp1, "node-tag/1", &format!("{first}/blue"), &json!({"node_id": first, "label": "blue"})).await.unwrap();
        n.append(&mp2, "node-record/1", second.as_str(), &json!({"node_id": second, "name": "mesh1.rpc.2", "state": "Running", "config": {"setting": APP_SETTING}})).await.unwrap();
        n.append(&mp2, "node-record/1", first.as_str(), &json!({"node_id": first, "name": "mesh1.rpc.2", "state": "Deleted"})).await.unwrap();

        let c = log(&root, CONNECTIONS_FILE).await;
        c.append(&gone, "node-connection/2", "gone--live--00--Direct", &json!({"source": "mesh1.rpc.1", "state": "Connected"})).await.unwrap();
        c.append(&live, "node-connection/2", "live--gone--00--Direct", &json!({"source": "mesh1.rpc.3", "state": "Connected"})).await.unwrap();
        c.append(&gone, "node-connection/2", "gone--live--00--Direct", &json!({"source": "mesh1.rpc.1", "state": "Disconnected"})).await.unwrap();

        let b = log(&root, BUILDS_FILE).await;
        b.append(&fp1, "build-record/1", &format!("{build}/accepted"), &json!({"seq": 1, "fact": "accepted"})).await.unwrap();
        b.append(&fp1, "build-record/1", &format!("{build}/opened/1"), &json!({"seq": 2, "fact": "opened", "attempt": 1})).await.unwrap();
        b.append(&fp2, "build-record/1", &format!("{build}/opened/1"), &json!({"seq": 3, "fact": "opened", "attempt": 1})).await.unwrap();
        b.append(&fp2, "build-record/1", &format!("{build}/opened/2"), &json!({"seq": 4, "fact": "opened", "attempt": 2})).await.unwrap();

        Self { store_dir, root, fabric, fp1, fp2, gone, live, first, second }
    }

    fn total(&self) -> u64 {
        FILES.iter().map(|(_, f)| file_lines(&self.root, f).len() as u64).sum()
    }
}

/// CONTRACT (storage.md §2 Copy: "appends every line of every store on the old provider to the new
/// one in source order, dead births' and deleted nodes' records included"; i149 R5, R6): after the
/// copy each of the new provider's five stores holds the old one's lines, in the old one's order,
/// each with the op its writer wrote it with. So every current value is the old one's, though the
/// fabric's, a mesh's and a node's writer changed; a dead birth's connection facts are still that
/// node's (a connections reader reads ownership from the op); the node at a path.name is the one the
/// latest node record across node ids names; a Build fact's first line per key is the first writer's;
/// and a writer that resumes on the new provider writes past its copied ops. The copy's span names
/// the change, both providers, the source position verified and the lines copied; the new provider's
/// stores open on spans naming it. Must NOT happen: a line re-stamped with the copier's op, lines
/// reordered across keys, a dead writer's line left behind, or a copied op used again.
#[test]
fn a_copy_preserves_current_values_across_writer_changes() {
    let capture = capture("current");
    let (h, change) = capture.run(2, async {
        let h = History::write().await;
        let dest_dir = dir("destination");
        let source = FileProviderLines::open(SOURCE, &h.store_dir, &h.fabric).await.unwrap();
        let destination = FileProviderLines::open(DESTINATION, &dest_dir, &h.fabric).await.unwrap();
        let change = run();
        copy(&change, &source, &destination).await.unwrap_or_else(|e| panic!("the copy finishes: {e}"));
        let dest_root = dest_dir.join(h.fabric.as_str());

        for (_, file) in FILES {
            assert_eq!(file_lines(&dest_root, file), file_lines(&h.root, file), "{file}: the new provider holds the old one's lines in source order, each with its writer's op");
        }
        let (src, dst) = (store_log::latest_in(&h.root.join(MESHES_FILE)).unwrap(), store_log::latest_in(&dest_root.join(MESHES_FILE)).unwrap());
        assert_eq!(dst.values().map(line_of).collect::<Vec<_>>(), src.values().map(line_of).collect::<Vec<_>>(), "every mesh's current value is the old provider's");
        let fabric_now = store_log::latest_in(&dest_root.join(FABRIC_FILE)).unwrap();
        let fabric_now = fabric_now.values().next().expect("the Fabric record is copied");
        assert_eq!((fabric_now.op.as_ref().map(|o| (o.node_id.clone(), o.seq)), fabric_now.record["events"].clone()), (Some((h.fp2.node_id.to_string(), 1)), json!(["ready-for-traffic"])), "the Fabric record's current value is the successor's");
        let at_name = store_log::read_file(&dest_root.join(NODES_FILE)).unwrap().into_iter().filter(|l| l.kind() == "node-record" && l.record["name"] == "mesh1.rpc.2").last().unwrap();
        assert_eq!((at_name.key.clone(), at_name.record["state"].clone()), (h.first.to_string(), json!("Deleted")), "the latest node record at the path.name, across node ids, is the old provider's");
        assert!(file_lines(&dest_root, NODES_FILE).iter().any(|l| l.2 == h.second.to_string() && l.3["state"] == "Running"), "the re-created node's record is copied");
        let gone_lines: Vec<_> = file_lines(&dest_root, CONNECTIONS_FILE).into_iter().filter(|l| l.0.as_ref().is_some_and(|o| o.node_id == h.gone.node_id.to_string())).collect();
        assert_eq!(gone_lines.len(), 2, "the dead birth's connection facts are still its own, by their op: {gone_lines:?}");
        let mut first_per_key: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        for l in store_log::read_file(&dest_root.join(BUILDS_FILE)).unwrap() {
            first_per_key.entry(l.key.clone()).or_insert(l.record.clone());
        }
        assert!(first_per_key.iter().filter(|(k, _)| k.ends_with("/opened/1")).all(|(_, r)| r["seq"] == 2), "the first Build fact line per key is the first writer's: {first_per_key:?}");

        let resumed = log(&dest_root, CONNECTIONS_FILE).await;
        resumed.append(&h.live, "node-connection/2", "live--gone--00--Direct", &json!({"source": "mesh1.rpc.3", "state": "Disconnected"})).await.unwrap();
        let last = store_log::read_file(&dest_root.join(CONNECTIONS_FILE)).unwrap().pop().unwrap();
        assert_eq!(last.op.map(|o| (o.node_id, o.seq)), Some((h.live.node_id.to_string(), 2)), "a writer resuming on the new provider writes past its copied op");
        (h, change)
    });
    let spans = capture.spans().expect("the cell's spans");
    let copies = spans_named(&spans, COPY_SPAN);
    assert_eq!(copies.len(), 1, "one copy span: {copies:?}");
    let total = h.total();
    let s = copies[0];
    assert_eq!(
        ["build_id", "change_id", "source", "destination", "source_position", "copied", "outcome"].map(|k| attr(s, k)),
        [Some(change.build_id.log().to_string()), Some(change.change_id.to_string()), Some(SOURCE.into()), Some(DESTINATION.into()), Some(total.to_string()), Some(total.to_string()), Some("copied".into())],
        "the copy span names the change, both providers, the verified source position and the lines copied: {s}"
    );
    let opens: Vec<_> = spans_named(&spans, OPEN_SPAN).into_iter().filter(|s| attr(s, "provider").as_deref() == Some(DESTINATION)).collect();
    assert!(!opens.is_empty() && opens.iter().all(|s| attr(s, "bytes").is_some() && attr(s, "lines").is_some()), "the new provider's stores open on spans naming it, with their bytes and lines: {opens:?}");
}

/// CONTRACT (storage.md §2 Copy: "a new provider holding lines the old one does not is refused by
/// name, and nothing switches"; i149 R6 "reject a destination hole instead of replaying stale
/// state"; proposal: "A destination with operations 1 and 3 but missing 2 is refused without making
/// 2 current"): a new provider holding a key's first and third lines but not its second is refused,
/// the refusal naming the store, the key and the two ops that differ, and nothing is appended to any
/// of its stores, so its current value at that key stays as it was and the second line is never
/// replayed after the third. A new provider holding a line after the old one's last is refused the
/// same way. Neither the refusal nor the span carries a record's application setting. Must NOT
/// happen: the missing line appended after the later one (a stale current value), a partial copy
/// into the other stores, or a refusal that dumps the record.
#[test]
fn a_copy_refuses_a_destination_hole_without_replaying_stale_state() {
    let capture = capture("hole");
    let (change, extra_change) = capture.run(2, async {
        let store_dir = dir("hole-source");
        let fabric = FabricId::mint();
        let root = StoreRoot::open(&store_dir, &fabric).unwrap().dir().to_path_buf();
        let w = writer();
        let f = log(&root, FABRIC_FILE).await;
        f.append(&w, "fabric-record/2", fabric.as_str(), &json!({"fabric_id": fabric, "name": "fabric1"})).await.unwrap();
        let n = log(&root, NODES_FILE).await;
        for v in 1..=3 {
            n.append(&w, "node-record/1", "node-k", &json!({"name": "mesh1.rpc.2", "v": v, "config": {"setting": APP_SETTING}})).await.unwrap();
        }
        let src = store_log::read_file(&root.join(NODES_FILE)).unwrap();
        let source = FileProviderLines::open(SOURCE, &store_dir, &fabric).await.unwrap();

        // A new provider holding the key's lines 1 and 3, not 2.
        let dest_dir = dir("hole-destination");
        let dest_root = StoreRoot::open(&dest_dir, &fabric).unwrap().dir().to_path_buf();
        let raw = |l: &Line| store_log::encode_line(l.op.as_ref().unwrap(), &l.format, &l.key, &serde_json::to_vec(&l.record).unwrap());
        std::fs::write(dest_root.join(NODES_FILE), [raw(&src[0]), raw(&src[2])].concat()).unwrap();
        let destination = FileProviderLines::open(DESTINATION, &dest_dir, &fabric).await.unwrap();
        let before = snapshot(&dest_dir);
        let change = run();
        let refused = match copy(&change, &source, &destination).await {
            Err(CopyError::Refused(why)) => why,
            other => panic!("a new provider with a hole is refused by name, nothing appended: {:?}", other.map_err(|e| e.to_string())),
        };
        let (want, found) = (named(src[1].op.as_ref().unwrap()), named(src[2].op.as_ref().unwrap()));
        assert!(refused.contains("nodes") && refused.contains("node-k") && refused.contains(&want) && refused.contains(&found), "the refusal names the store, the key and the two ops that differ ({want}, {found}): {refused}");
        assert!(!refused.contains(APP_SETTING), "the refusal carries no record's application setting: {refused}");
        assert_eq!(snapshot(&dest_dir), before, "nothing is appended to any store of the new provider");
        let now = store_log::latest_in(&dest_root.join(NODES_FILE)).unwrap();
        assert_eq!(now.values().next().map(|l| l.record["v"].clone()), Some(json!(3)), "the key's current value on the new provider is not replayed to line 2");

        // A new provider holding a line the old one does not.
        let extra_dir = dir("extra-destination");
        let extra_root = StoreRoot::open(&extra_dir, &fabric).unwrap().dir().to_path_buf();
        let stray = Line { offset: None, op: Some(Op { node_id: w.node_id.to_string(), incarnation_id: w.incarnation_id.to_string(), seq: 4 }), format: "node-record/1".into(), key: "node-k".into(), record: json!({"v": 4}) };
        std::fs::write(extra_root.join(NODES_FILE), [raw(&src[0]), raw(&src[1]), raw(&src[2]), raw(&stray)].concat()).unwrap();
        let destination = FileProviderLines::open(DESTINATION, &extra_dir, &fabric).await.unwrap();
        let before = snapshot(&extra_dir);
        let extra_change = run();
        let refused = match copy(&extra_change, &source, &destination).await {
            Err(CopyError::Refused(why)) => why,
            other => panic!("a new provider holding a line the old one does not is refused by name: {:?}", other.map_err(|e| e.to_string())),
        };
        assert!(refused.contains("nodes") && refused.contains("node-k") && refused.contains(&named(stray.op.as_ref().unwrap())), "the refusal names the store, the key and the stray op: {refused}");
        assert_eq!(snapshot(&extra_dir), before, "nothing is appended to the new provider");
        (change, extra_change)
    });
    let spans = capture.spans().expect("the cell's spans");
    let copies = spans_named(&spans, COPY_SPAN);
    let of = |c: &CopyRun| copies.iter().find(|s| attr(s, "change_id").as_deref() == Some(c.change_id.as_str())).copied().unwrap_or_else(|| panic!("a copy span for change {}: {copies:?}", c.change_id));
    let s = of(&change);
    assert_eq!(
        ["build_id", "source", "destination", "source_position", "copied", "outcome"].map(|k| attr(s, k)),
        [Some(change.build_id.log().to_string()), Some(SOURCE.into()), Some(DESTINATION.into()), Some("1".into()), Some("0".into()), Some("refused".into())],
        "the refused copy's span: one source line verified, none copied: {s}"
    );
    assert_eq!(attr(of(&extra_change), "outcome").as_deref(), Some("refused"));
    assert!(copies.iter().all(|s| !s.to_string().contains(APP_SETTING)), "no copy span carries a record's application setting");
}

/// A new provider whose answers are lost: the `lose_append`th line appended and the `lose_claim`th
/// claim inserted land, and the copy is answered an error.
struct LosesAnswers {
    inner: FileProviderLines,
    appended: AtomicUsize,
    inserted: AtomicUsize,
    lose_append: usize,
    lose_claim: usize,
}

#[async_trait::async_trait]
impl ProviderLines for LosesAnswers {
    async fn lines(&self, store: Store) -> Result<Vec<Line>, StorageError> {
        self.inner.lines(store).await
    }
    async fn append_line(&self, store: Store, line: &Line) -> Result<(), StorageError> {
        self.inner.append_line(store, line).await?;
        if self.appended.fetch_add(1, Ordering::SeqCst) + 1 == self.lose_append {
            return Err(StorageError::Io { file: format!("{store:?}"), reason: "the append landed and its answer was lost".into() });
        }
        Ok(())
    }
    async fn claims(&self) -> Result<Vec<Claim>, StorageError> {
        self.inner.claims().await
    }
    async fn insert_claim(&self, claim: &Claim) -> Result<ClaimTaken, StorageError> {
        let taken = self.inner.insert_claim(claim).await?;
        if self.inserted.fetch_add(1, Ordering::SeqCst) + 1 == self.lose_claim {
            return Err(StorageError::Io { file: claim.name.clone(), reason: "the claim landed and its answer was lost".into() });
        }
        Ok(taken)
    }
}

/// CONTRACT (storage.md §2 Copy: "A copy resumed after a lost answer recognises the lines already
/// there and appends none twice"; i149 e3 "persist verified progress, and reconcile uncertain copy
/// answers without duplicate appends"): a copy whose third append lands with its answer lost stops,
/// answered the provider's error, not a refusal; the copy resumed from the new provider's verified
/// lines appends only the rest, and one whose claim insert lands with its answer lost is resumed the
/// same way. After the last copy the new provider holds the old one's lines and claims once each.
/// Must NOT happen: a line or claim appended twice, a resumed copy starting over, or a lost answer
/// read as a refusal.
#[test]
fn a_copy_resumes_after_a_lost_answer_without_duplicate_records() {
    let capture = capture("resume");
    let (changes, total) = capture.run(2, async {
        let h = History::write().await;
        let locks = StoreLocks::at(&h.root, &h.fp2);
        assert_eq!(locks.take(LockKind::Name, "mesh1", br#"{"format":"mesh-name/1","record":"m1","by":["a","b"]}"#).await.unwrap(), LockTaken::Won);
        assert_eq!(locks.take(LockKind::Name, "mesh1.rpc.2", br#"{"format":"node-name/1","record":"n2","by":["a","b"]}"#).await.unwrap(), LockTaken::Won);
        let dest_dir = dir("resume-destination");
        let source = FileProviderLines::open(SOURCE, &h.store_dir, &h.fabric).await.unwrap();
        let destination = LosesAnswers { inner: FileProviderLines::open(DESTINATION, &dest_dir, &h.fabric).await.unwrap(), appended: AtomicUsize::new(0), inserted: AtomicUsize::new(0), lose_append: 3, lose_claim: 1 };
        let changes = [run(), run(), run()];
        let dest_root = dest_dir.join(h.fabric.as_str());

        match copy(&changes[0], &source, &destination).await {
            Err(CopyError::Storage(_)) => {}
            other => panic!("a lost append answer stops the copy with the provider's error: {:?}", other.map(|c| c.copied).map_err(|e| e.to_string())),
        }
        let landed: usize = FILES.iter().map(|(_, f)| file_lines(&dest_root, f).len()).sum();
        assert_eq!(landed, 3, "the lines up to the one whose answer was lost landed");
        match copy(&changes[1], &source, &destination).await {
            Err(CopyError::Storage(_)) => {}
            other => panic!("a lost claim answer stops the copy with the provider's error: {:?}", other.map(|c| c.copied).map_err(|e| e.to_string())),
        }
        copy(&changes[2], &source, &destination).await.unwrap_or_else(|e| panic!("the resumed copy finishes: {e}"));

        let mut ops = BTreeSet::new();
        for (_, file) in FILES {
            let lines = file_lines(&dest_root, file);
            assert_eq!(lines, file_lines(&h.root, file), "{file}: the new provider holds the old one's lines once each, in source order");
            for l in lines {
                assert!(ops.insert((file, l.0.clone())), "{file}: op {:?} appended twice", l.0);
            }
        }
        let dest_locks = StoreLocks::at(&dest_root, &writer());
        for name in ["mesh1", "mesh1.rpc.2"] {
            assert_eq!(dest_locks.read(LockKind::Name, name).await.unwrap(), locks.read(LockKind::Name, name).await.unwrap(), "the claim of {name} is the old provider's");
        }
        (changes, h.total())
    });
    let spans = capture.spans().expect("the cell's spans");
    let copies = spans_named(&spans, COPY_SPAN);
    let of = |c: &CopyRun| copies.iter().find(|s| attr(s, "change_id").as_deref() == Some(c.change_id.as_str())).copied().unwrap_or_else(|| panic!("a copy span for change {}: {copies:?}", c.change_id));
    assert_eq!(changes.iter().map(|c| attr(of(c), "outcome")).collect::<Vec<_>>(), [Some("indeterminate".into()), Some("indeterminate".into()), Some("copied".into())], "two copies stopped on a lost answer, the third finished");
    assert_eq!(attr(of(&changes[1]), "copied"), Some((total - 3).to_string()), "the resumed copy appended only the lines the new provider did not hold");
    assert_eq!(["source_position", "copied"].map(|k| attr(of(&changes[2]), k)), [Some(total.to_string()), Some("0".into())], "the last copy verified every source line and appended none");
}

/// CONTRACT (storage.md §2 Copy: "every name claim and in-flight row with its exact claimant"; i149
/// R7 "transferring every held name and in-flight claim before" the switch; storage.md §2 Switch: "The
/// insert is the switch, of records and claims alike"): after the copy the new provider holds the
/// old one's mesh name claim, node path.name claim, a mesh's in-flight row and the fabric's claim of
/// its name, each byte for byte, so a take there by another claimant is held, naming the original
/// claimant; the attempt's Build facts are copied with its in-flight row; a claim the old provider
/// released is not on the new one. Must NOT happen: a claim re-taken by the copier, a released claim
/// revived, or an attempt whose row moved without its facts.
#[test]
fn a_provider_change_preserves_held_names_and_attempts() {
    let capture = capture("claims");
    let change = capture.run(2, async {
        let h = History::write().await;
        let locks = StoreLocks::at(&h.root, &h.fp2);
        let by = format!(r#""by":["{}","{}"]"#, h.fp2.node_id, h.fp2.incarnation_id);
        let mesh_name = format!(r#"{{"format":"mesh-name/1","record":"mesh1-id",{by}}}"#).into_bytes();
        let node_name = format!(r#"{{"format":"node-name/1","record":"node-2-id",{by}}}"#).into_bytes();
        let build = BuildId::mint();
        let in_flight = format!(r#"{{"format":"mesh-in-flight/2","record":{{"build_id":"{build}","attempt":2,"claimant":"mesh1.admin.1","claimant_node":"{}"}},{by}}}"#, h.fp2.node_id).into_bytes();
        assert_eq!(locks.take(LockKind::Name, "mesh1", &mesh_name).await.unwrap(), LockTaken::Won);
        assert_eq!(locks.take(LockKind::Name, "mesh1.rpc.2", &node_name).await.unwrap(), LockTaken::Won);
        assert_eq!(locks.take(LockKind::InFlight, "mesh1", &in_flight).await.unwrap(), LockTaken::Won);
        assert_eq!(locks.take(LockKind::Name, "mesh2", b"released").await.unwrap(), LockTaken::Won);
        assert!(locks.release(LockKind::Name, "mesh2", |_| true).await.unwrap(), "the old provider releases mesh2's name");
        let b = log(&h.root, BUILDS_FILE).await;
        b.append(&h.fp2, "build-record/1", &format!("{build}/claim/2"), &json!({"seq": 9, "fact": "claim", "attempt": 2, "claimant": "mesh1.admin.1"})).await.unwrap();
        let names = StoreRoot::open(&h.store_dir, &h.fabric).unwrap().fabric_names(&h.fp1.incarnation_id).await.unwrap();
        names.write("fabric1", "fabric-name/1", &json!({"fabric_id": h.fabric, "name": "fabric1"})).await.unwrap();
        let fabric_claim = std::fs::read(h.store_dir.join("fabrics").join("fabric1.json")).unwrap();

        let dest_dir = dir("claims-destination");
        let source = FileProviderLines::open(SOURCE, &h.store_dir, &h.fabric).await.unwrap();
        let destination = FileProviderLines::open(DESTINATION, &dest_dir, &h.fabric).await.unwrap();
        let change = run();
        copy(&change, &source, &destination).await.unwrap_or_else(|e| panic!("the copy finishes: {e}"));
        let dest_root = dest_dir.join(h.fabric.as_str());

        let dest_locks = StoreLocks::at(&dest_root, &writer());
        assert_eq!(dest_locks.read(LockKind::Name, "mesh1").await.unwrap(), Some(mesh_name.clone()), "the mesh name's claim is copied with its exact claimant");
        assert_eq!(dest_locks.read(LockKind::Name, "mesh1.rpc.2").await.unwrap(), Some(node_name.clone()), "the node path.name's claim is copied with its exact claimant");
        assert_eq!(dest_locks.read(LockKind::InFlight, "mesh1").await.unwrap(), Some(in_flight.clone()), "the mesh's in-flight row is copied with its exact claimant");
        assert_eq!(std::fs::read(dest_dir.join("fabrics").join("fabric1.json")).ok(), Some(fabric_claim), "the fabric's claim of its name is copied");
        assert_eq!(dest_locks.read(LockKind::Name, "mesh2").await.unwrap(), None, "a claim the old provider released is not on the new one");
        assert_eq!(dest_locks.take(LockKind::Name, "mesh1", b"another claimant").await.unwrap(), LockTaken::Held(mesh_name), "a take of the name on the new provider is held by the original claimant");
        assert_eq!(dest_locks.take(LockKind::InFlight, "mesh1", b"another attempt").await.unwrap(), LockTaken::Held(in_flight), "a take of the in-flight row on the new provider is held by the original attempt");
        assert_eq!(file_lines(&dest_root, BUILDS_FILE), file_lines(&h.root, BUILDS_FILE), "the attempt's Build facts are copied with its row");
        change
    });
    let spans = capture.spans().expect("the cell's spans");
    let copies = spans_named(&spans, COPY_SPAN);
    assert_eq!(copies.iter().map(|s| (attr(s, "change_id"), attr(s, "outcome"))).collect::<Vec<_>>(), [(Some(change.change_id.to_string()), Some("copied".into()))], "one copy span, copied: {copies:?}");
}
