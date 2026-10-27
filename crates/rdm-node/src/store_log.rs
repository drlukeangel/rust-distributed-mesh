//! One append-only JSONL file of a store root (R-SR1 to R-SR4, proposal
//! `store-root-one-directory-per-entity.md`): every write of a record is one line carrying the whole
//! record, the latest complete line for a key is the record's current state, and nothing is ever
//! removed.
//!
//! **A line** (R-SR3) is `\n` + one JSON object + `\n`, written as
//!
//! ```text
//! {"len":N,"op":{"node_id":"…","incarnation_id":"…","seq":S},"format":"<kind>/<n>","key":"…","record":{…},"crc32":"xxxxxxxx"}
//! ```
//!
//! - `len` is the object's byte count between its newlines, itself included: the smallest decimal
//!   `N` for which `{"len":` + `N` + the rest is `N` bytes long.
//! - `op` is the write: the writer's node id and incarnation, and `seq`, a sequence this writer
//!   increments for each new write to this file. A retry of one write keeps its `op`; a separate
//!   write gets a new `seq` even when its record is identical. A writer resumes its sequence from the
//!   file's own lines (the greatest `seq` of its node id and incarnation), so an `op` is never used
//!   twice.
//! - `format` is the record kind and its version; `key` the record's key inside its kind.
//! - `record` is the whole record, nested so no field of it can be read as a field of the line.
//! - `crc32` is the CRC-32 (IEEE) of the object's bytes from its opening brace up to, not including,
//!   the final `,"crc32":`, as eight lowercase hex digits: the last 20 bytes of every object are
//!   `,"crc32":"xxxxxxxx"}`.
//!
//! **The append.** A writer appends one line in one `write` on a descriptor opened `O_APPEND`, then
//! `fsync`s, and reports success only after the fsync. A short write and a failed fsync are
//! indeterminate, never success: the writer holds the write as its pending append and reconciles it
//! before its next write, by its `op`, from the offset its fold had consumed before the write,
//! validating every line between. A complete, terminated object is fsynced and accepted; a complete
//! object missing only its newline is finished with one newline alone and fsynced; a write that
//! reported zero bytes, with every line between valid, is retried under the same `op`. A partial
//! object, or an outcome the writer cannot identify, is refused by name: nothing is appended in its
//! place and no byte is discarded.
//!
//! **The fold** (R-SR3, R-SR4) skips blank lines only. Every other line must hold exactly `len`
//! bytes, match its `crc32` and parse as a format the store reads: a line that does not, the last
//! one included, refuses the open or the tail read by name, naming the file and the byte offset, and
//! nothing older is answered as current. A complete valid object with no final newline is kept: the
//! reader appends one newline alone and fsyncs before it answers (another writer's opening newline
//! may already have ended it; the added newline is then a blank line). The consumed offset is the
//! fold's own: it moves only over lines the fold has read whole, never to where a write landed, so a
//! writer's own append does not move it and its next tail read reads every line after it, its own
//! included.
//!
//! The file backend requires a local filesystem whose `O_APPEND` writes are serialized by the shared
//! host kernel (R14 item 5, as amended by R-SR3).

use crate::blocking::blocking_io;
use crate::storage_error::StorageError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The span every open of a store file records: the file, its provider when the opener names it, its
/// bytes and lines, the latest `op` of each writer and the fold's `duration` in milliseconds; a refused
/// fold its `invalid_offset` and cause (i149 plan, "OTLP span-assertion bar").
pub const OPEN_SPAN: &str = "rdm.node.store.resolve.via-fold";

/// The bytes every object ends with after its checksummed bytes: `,"crc32":"xxxxxxxx"}`.
const CRC_SUFFIX: usize = 20;

/// One write of a line: its writer and the writer's sequence in this file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Op {
    /// The writer's node id.
    pub node_id: String,
    /// The writer's incarnation.
    pub incarnation_id: String,
    /// The write's place in this writer's sequence of writes to this file, from 1.
    pub seq: u64,
}

/// The process writing through a store: its node id and incarnation, the first two fields of every
/// `op` it writes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogWriter {
    /// The writer's node id.
    pub node_id: crate::model::NodeId,
    /// The writer's incarnation.
    pub incarnation_id: crate::model::IncarnationId,
}

impl LogWriter {
    /// The writer born as `incarnation` of `node`.
    pub fn new(node: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Self {
        Self { node_id: node.clone(), incarnation_id: incarnation.clone() }
    }

    /// A writer of a newly minted node id and incarnation: a store a cell opens.
    pub fn mint() -> Self {
        Self { node_id: crate::model::NodeId::mint(), incarnation_id: crate::model::IncarnationId::mint() }
    }

    /// `(node id, incarnation)`, as an `op` names them.
    fn ids(&self) -> (String, String) {
        (self.node_id.to_string(), self.incarnation_id.to_string())
    }
}

impl std::fmt::Display for LogWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.node_id, self.incarnation_id)
    }
}

/// One record as a store reads it: its format, key and whole record, and, once on the disk, its
/// `op` and the byte offset of its object in the file.
#[derive(Debug, Clone, PartialEq)]
pub struct Line {
    /// The object's byte offset in the file; `None` for a line its writer still holds.
    pub offset: Option<u64>,
    /// The write that appended it; `None` for a line its writer still holds.
    pub op: Option<Op>,
    /// `<kind>/<n>`.
    pub format: String,
    /// The record's key inside its kind.
    pub key: String,
    /// The whole record.
    pub record: serde_json::Value,
    /// The record's bytes as its writer wrote them, for a line read from a store: a copy appends these,
    /// so the new provider holds the line byte for byte (storage.md §2 Copy). `None` for a line made in
    /// memory: its record is encoded.
    pub written: Option<Vec<u8>>,
}

impl Line {
    /// The record kind: the format without its version.
    pub fn kind(&self) -> &str {
        kind_of(&self.format)
    }

    /// The record as `T`, refused by name (the file and the line's offset) when it does not decode.
    pub fn decode<T: DeserializeOwned>(&self, file: &Path) -> Result<T, StorageError> {
        T::deserialize(&self.record).map_err(|e| StorageError::Unrecognised { file: file.display().to_string(), reason: format!("the {} line{} does not decode: {e}", self.format, self.offset.map(|o| format!(" at byte offset {o}")).unwrap_or_default()) })
    }
}

fn kind_of(format: &str) -> &str {
    format.split('/').next().unwrap_or(format)
}

/// A check that a record decodes as its format's type.
type Check = Arc<dyn Fn(&serde_json::Value) -> Result<(), String> + Send + Sync>;

/// The formats a store reads in one file, each with the check its records must pass: a line in any
/// other format, or whose record fails its check, refuses the fold by name.
#[derive(Clone, Default)]
pub struct Formats {
    checks: BTreeMap<String, Check>,
    /// Every format is read, unchecked: a look at a file, never a store's fold.
    any: bool,
}

impl Formats {
    /// No format.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every format, its records unchecked: a look at a whole file ([`read_file`]).
    pub fn any() -> Self {
        Self { checks: BTreeMap::new(), any: true }
    }

    /// `format`, whose records decode as `T`.
    pub fn with<T: DeserializeOwned + 'static>(mut self, format: &str) -> Self {
        self.checks.insert(format.to_string(), Arc::new(|v: &serde_json::Value| T::deserialize(v).map(|_| ()).map_err(|e| e.to_string())));
        self
    }

    fn check(&self, format: &str, record: &serde_json::Value) -> Result<(), String> {
        match self.checks.get(format) {
            Some(check) => check(record).map_err(|e| format!("its {format} record does not decode: {e}")),
            None if self.any => Ok(()),
            None => Err(format!("format {format:?}, this build reads {:?} here", self.checks.keys().collect::<Vec<_>>())),
        }
    }
}

impl std::fmt::Debug for Formats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.checks.keys()).finish()
    }
}

/// Every line of the store file at `path`, in file order, its records unchecked, read on the calling
/// thread and changing nothing: a missing file holds none; a malformed line is refused by name with
/// its offset, and a valid object with no final newline is read as a line.
pub fn read_file(path: &Path) -> Result<Vec<Line>, StorageError> {
    let named = |at: u64, why: String| StorageError::Unrecognised { file: path.display().to_string(), reason: format!("the line at byte offset {at} is malformed: {why}") };
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
    };
    let formats = Formats::any();
    let s = scan(&bytes, 0, &formats).map_err(|(at, why)| named(at, why))?;
    let mut lines = s.lines;
    if let Some((at, tail)) = s.tail {
        lines.push(verify(&tail, at, &formats).map_err(|why| named(at, why))?);
    }
    Ok(lines)
}

/// The latest record of `kind` (at `key`, when given) in the store file at `path`, as
/// `{"format": …, "record": …}`; `None` when there is none, or when the file cannot be read whole: a
/// look a cell takes at a root while its nodes write it.
pub fn read_record(path: &Path, kind: &str, key: Option<&str>) -> Option<serde_json::Value> {
    let lines = read_file(path).ok()?;
    let l = lines.into_iter().rev().find(|l| l.kind() == kind && key.is_none_or(|k| l.key == k))?;
    Some(serde_json::json!({ "format": l.format, "key": l.key, "record": l.record }))
}

/// One object's fields as its bytes hold them: its `op`, `format` and `key`, and its record exactly as
/// written (a cell rewriting an older writer's record keeps the bytes it does not change).
#[derive(Debug, Clone)]
pub struct RawObject {
    /// The write.
    pub op: Op,
    /// `<kind>/<n>`.
    pub format: String,
    /// The record's key.
    pub key: String,
    /// The record's JSON, byte for byte.
    pub record_json: String,
}

/// The fields of the object `obj` (no newlines) when it verifies, its record as written.
pub fn read_record_bytes(obj: &[u8]) -> Result<RawObject, String> {
    let line = verify(obj, 0, &Formats::any())?;
    let text = std::str::from_utf8(obj).map_err(|e| e.to_string())?;
    let start = text.find(",\"record\":").ok_or("no record field")? + ",\"record\":".len();
    let record_json = text[start..text.len() - CRC_SUFFIX].to_string();
    Ok(RawObject { op: line.op.expect("a line on the disk has its op"), format: line.format, key: line.key, record_json })
}

/// The keys of `kind` the store file at `path` holds a line of, sorted ([`read_file`]).
pub fn keys_in(path: &Path, kind: &str) -> Result<Vec<String>, StorageError> {
    Ok(latest_in(path)?.into_keys().filter(|(k, _)| k == kind).map(|(_, key)| key).collect())
}

/// The latest line of each (kind, key) of the store file at `path` ([`read_file`]).
pub fn latest_in(path: &Path) -> Result<BTreeMap<(String, String), Line>, StorageError> {
    Ok(read_file(path)?.into_iter().map(|l| ((l.kind().to_string(), l.key.clone()), l)).collect())
}

// ---------------------------------------------------------------- the line's bytes

/// CRC-32 (IEEE 802.3, reflected, polynomial 0xEDB88320) of `bytes`.
pub fn crc32(bytes: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            }
            *slot = c;
        }
        t
    });
    let mut c = 0xFFFF_FFFFu32;
    for b in bytes {
        c = table[((c ^ *b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

fn digits(mut n: usize) -> usize {
    let mut d = 1;
    while n >= 10 {
        n /= 10;
        d += 1;
    }
    d
}

/// The object (no newlines) of one write: `op`, `format`, `key` and `record`, its `len` and its
/// `crc32`.
pub fn encode_object(op: &Op, format: &str, key: &str, record: &[u8]) -> Vec<u8> {
    let json = |s: &str| serde_json::to_string(s).expect("a string encodes");
    let op_json = serde_json::to_string(op).expect("an op encodes");
    let rest = format!(",\"op\":{op_json},\"format\":{},\"key\":{},\"record\":", json(format), json(key));
    let fixed = "{\"len\":".len() + rest.len() + record.len() + CRC_SUFFIX;
    let len = (1..=20).map(|d| fixed + d).find(|n| digits(*n) == n - fixed).expect("a length has a width");
    let mut obj = format!("{{\"len\":{len}{rest}").into_bytes();
    obj.extend_from_slice(record);
    let crc = crc32(&obj);
    obj.extend_from_slice(format!(",\"crc32\":\"{crc:08x}\"}}").as_bytes());
    debug_assert_eq!(obj.len(), len);
    obj
}

/// The whole line of one write: `\n` + [`encode_object`] + `\n`.
pub fn encode_line(op: &Op, format: &str, key: &str, record: &[u8]) -> Vec<u8> {
    line_of(&encode_object(op, format, key, record))
}

/// The line of `obj`: `\n` + the object + `\n`.
fn line_of(obj: &[u8]) -> Vec<u8> {
    let mut line = Vec::with_capacity(obj.len() + 2);
    line.push(b'\n');
    line.extend_from_slice(obj);
    line.push(b'\n');
    line
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    len: usize,
    op: Op,
    format: String,
    key: String,
    record: serde_json::Value,
    #[allow(dead_code)]
    crc32: String,
}

/// The line of the object `obj` at byte offset `offset`, or why it is not one: its `len` must be its
/// byte count, its `crc32` must match, and it must parse as a format `formats` reads.
/// The object's record field name, as an encoded object holds it.
const RECORD_FIELD: &[u8] = b",\"record\":";

fn verify(obj: &[u8], offset: u64, formats: &Formats) -> Result<Line, String> {
    const HEAD: &[u8] = b"{\"len\":";
    if !obj.starts_with(HEAD) {
        return Err("it does not begin with its len".into());
    }
    let digits_end = obj[HEAD.len()..].iter().position(|b| !b.is_ascii_digit()).map(|p| p + HEAD.len()).unwrap_or(obj.len());
    let len: usize = std::str::from_utf8(&obj[HEAD.len()..digits_end]).ok().and_then(|s| s.parse().ok()).ok_or("its len is not a number")?;
    if len != obj.len() {
        return Err(format!("its len is {len} and the object holds {} bytes", obj.len()));
    }
    if obj.len() < HEAD.len() + CRC_SUFFIX || !obj[obj.len() - CRC_SUFFIX..].starts_with(b",\"crc32\":\"") || !obj.ends_with(b"\"}") {
        return Err("it does not end with its crc32".into());
    }
    let hex = std::str::from_utf8(&obj[obj.len() - 10..obj.len() - 2]).map_err(|_| "its crc32 is not hex")?;
    let stated = u32::from_str_radix(hex, 16).map_err(|_| format!("its crc32 {hex:?} is not hex"))?;
    let actual = crc32(&obj[..obj.len() - CRC_SUFFIX]);
    if stated != actual {
        return Err(format!("its crc32 is {stated:08x} and its bytes sum to {actual:08x}"));
    }
    let env: Envelope = serde_json::from_slice(obj).map_err(|e| format!("it does not parse: {e}"))?;
    if env.len != len {
        return Err("its len field is not its first".into());
    }
    formats.check(&env.format, &env.record)?;
    // The record's bytes: from after the object's `"record":` field name to its `crc32`. A key holding
    // a quote holds it escaped, so the field name's first unescaped occurrence is the field.
    let written = obj.windows(RECORD_FIELD.len()).position(|w| w == RECORD_FIELD).map(|at| obj[at + RECORD_FIELD.len()..obj.len() - CRC_SUFFIX].to_vec()).filter(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).is_ok_and(|v| v == env.record));
    Ok(Line { offset: Some(offset), op: Some(env.op), format: env.format, key: env.key, record: env.record, written })
}

/// What one scan of the bytes from an offset found.
struct Scan {
    /// The lines read whole, in file order.
    lines: Vec<Line>,
    /// The offset after the last newline: how far the lines were read whole.
    consumed: u64,
    /// The bytes after the last newline, when there are any: an object with no final newline.
    tail: Option<(u64, Vec<u8>)>,
}

/// Scan `bytes`, read from byte `from` of the file: every terminated nonblank line verified; a line
/// that does not verify refuses the scan, naming its offset.
fn scan(bytes: &[u8], from: u64, formats: &Formats) -> Result<Scan, (u64, String)> {
    let mut lines = Vec::new();
    let mut start = 0usize;
    while let Some(nl) = bytes[start..].iter().position(|b| *b == b'\n') {
        let seg = &bytes[start..start + nl];
        if !seg.is_empty() {
            let at = from + start as u64;
            lines.push(verify(seg, at, formats).map_err(|why| (at, why))?);
        }
        start += nl + 1;
    }
    let tail = (start < bytes.len()).then(|| (from + start as u64, bytes[start..].to_vec()));
    Ok(Scan { lines, consumed: from + start as u64, tail })
}

// ---------------------------------------------------------------- the disk calls

/// The disk calls of an append: one `write` and one `fsync`. Production makes them on the file; a
/// cell swaps in a faulted file to give each append the outcome it tests.
pub(crate) trait AppendIo: Send + Sync {
    /// One `write(2)` of `bytes` on `file` (opened `O_APPEND`): the count it reported.
    fn write(&self, file: &std::fs::File, bytes: &[u8]) -> std::io::Result<usize>;
    /// `fsync(2)` of `file`.
    fn sync(&self, file: &std::fs::File) -> std::io::Result<()>;
}

struct DiskIo;

impl AppendIo for DiskIo {
    fn write(&self, mut file: &std::fs::File, bytes: &[u8]) -> std::io::Result<usize> {
        use std::io::Write as _;
        file.write(bytes)
    }
    fn sync(&self, file: &std::fs::File) -> std::io::Result<()> {
        file.sync_all()
    }
}

/// Every byte of `file` from `from` to its end now.
fn read_from(file: &std::fs::File, from: u64) -> std::io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt as _;
    let len = file.metadata()?.len();
    let mut out = vec![0u8; len.saturating_sub(from) as usize];
    let mut got = 0;
    while got < out.len() {
        match file.read_at(&mut out[got..], from + got as u64)? {
            0 => break,
            n => got += n,
        }
    }
    out.truncate(got);
    Ok(out)
}

// ---------------------------------------------------------------- the fold

/// What the fold holds of one file: how far it read it, the latest line per (kind, key), and the
/// greatest `seq` of each writer.
#[derive(Default)]
struct Fold {
    consumed: u64,
    latest: BTreeMap<(String, String), Line>,
    lines: u64,
    ops: BTreeMap<(String, String), u64>,
}

impl Fold {
    fn take(&mut self, lines: Vec<Line>) {
        for l in lines {
            if let Some(op) = &l.op {
                let seq = self.ops.entry((op.node_id.clone(), op.incarnation_id.clone())).or_default();
                *seq = (*seq).max(op.seq);
            }
            self.lines += 1;
            self.latest.insert((l.kind().to_string(), l.key.clone()), l);
        }
    }
}

/// Each writer's last `seq` handed out in this process, per file: two stores of one process writing
/// one file as one writer never share an `op`.
fn issued() -> &'static Mutex<BTreeMap<(PathBuf, (String, String)), u64>> {
    static ISSUED: std::sync::OnceLock<Mutex<BTreeMap<(PathBuf, (String, String)), u64>>> = std::sync::OnceLock::new();
    ISSUED.get_or_init(Default::default)
}

/// The append a writer has not proven: its bytes, and where its fold stood before it.
#[derive(Debug, Clone)]
struct Pending {
    op: Op,
    obj: Vec<u8>,
    /// The fold's consumed offset before the write: reconciliation reads from here.
    from: u64,
    /// The bytes the write reported.
    reported: usize,
}

/// What a reconciliation found of a pending append.
enum Reconciled {
    /// Its object stands whole and terminated, fsynced: the append is done.
    Done,
    /// Nothing of it is in the file and every line after `from` is valid: it may be written again.
    Absent,
}

/// One store file: `<store root>/<store>.jsonl`.
#[derive(Clone)]
pub struct StoreLog(Arc<Inner>);

struct Inner {
    path: PathBuf,
    formats: Formats,
    file: Arc<std::fs::File>,
    io: Arc<dyn AppendIo>,
    fold: Mutex<Fold>,
    /// One tail read at a time.
    reading: tokio::sync::Mutex<()>,
    /// The tail read in progress: the span it was called under, and since when. What a read that
    /// waits for it names ([`Inner::read_waits`]).
    reading_by: Mutex<Option<(&'static str, std::time::Instant)>>,
    /// One append at a time, holding this store's pending append.
    append: Arc<tokio::sync::Mutex<Option<Pending>>>,
    /// Shown every line the fold reads, once each, in file order: an index the store folds beside
    /// the latest line per key.
    observer: Option<Observer>,
}

/// What a store folds from each line it reads beside the latest line per key ([`StoreLog::open_observed`]).
pub type Observer = Arc<dyn Fn(&Line) + Send + Sync>;

impl std::fmt::Debug for StoreLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreLog").field("path", &self.0.path).field("formats", &self.0.formats).finish()
    }
}

impl StoreLog {
    /// Open (creating) the store file at `path`, reading `formats`, and fold it from byte 0 on the
    /// blocking pool, recorded on the [`OPEN_SPAN`] span. A file this build cannot fold whole is
    /// refused by name.
    pub async fn open(path: &Path, formats: Formats) -> Result<Self, StorageError> {
        Self::opened(path, formats, Arc::new(DiskIo), None, None).await
    }

    /// [`Self::open`] of a store of the topology storage provider registered as `provider`, its open's
    /// span naming it.
    pub async fn open_for(provider: &str, path: &Path, formats: Formats) -> Result<Self, StorageError> {
        Self::opened(path, formats, Arc::new(DiskIo), None, Some(provider)).await
    }

    /// [`Self::open`], showing `observer` every line the fold reads, once each, in file order.
    pub async fn open_observed(path: &Path, formats: Formats, observer: Observer) -> Result<Self, StorageError> {
        Self::opened(path, formats, Arc::new(DiskIo), Some(observer), None).await
    }

    async fn opened(path: &Path, formats: Formats, io: Arc<dyn AppendIo>, observer: Option<Observer>, provider: Option<&str>) -> Result<Self, StorageError> {
        let span = tracing::info_span!("rdm.node.store.resolve.via-fold", file = %path.display(), provider = tracing::field::Empty, bytes = tracing::field::Empty, lines = tracing::field::Empty, writers = tracing::field::Empty, duration = tracing::field::Empty, outcome = tracing::field::Empty, invalid_offset = tracing::field::Empty, cause = tracing::field::Empty);
        if let Some(provider) = provider {
            span.record("provider", provider);
        }
        let began = std::time::Instant::now();
        let p = path.to_path_buf();
        let file = blocking_io(move || -> Result<std::fs::File, StorageError> {
            let io = |e: std::io::Error| StorageError::Io { file: p.display().to_string(), reason: e.to_string() };
            let existed = p.try_exists().map_err(io)?;
            let file = std::fs::OpenOptions::new().read(true).append(true).create(true).open(&p).map_err(io)?;
            if !existed {
                let dir = p.parent().unwrap_or(Path::new("."));
                crate::durable_file::sync_dir(dir, &p)?;
            }
            Ok(file)
        })
        .await;
        let file = match file {
            Ok(f) => Arc::new(f),
            Err(e) => {
                span.record("outcome", "refused").record("cause", e.to_string());
                return Err(e);
            }
        };
        let log = Self(Arc::new(Inner { path: path.to_path_buf(), formats, file, io, fold: Mutex::new(Fold::default()), reading: tokio::sync::Mutex::new(()), reading_by: Mutex::new(None), append: Arc::new(tokio::sync::Mutex::new(None)), observer }));
        match log.advance().await {
            Ok(()) => {
                let f = log.0.fold.lock().unwrap_or_else(|e| e.into_inner());
                let writers: Vec<String> = f.ops.iter().map(|((n, i), seq)| format!("{n}/{i}:{seq}")).collect();
                span.record("bytes", f.consumed).record("lines", f.lines).record("writers", writers.join(",")).record("duration", began.elapsed().as_millis() as u64).record("outcome", "folded");
                Ok(log.clone())
            }
            Err(e) => {
                let offset = match &e {
                    StorageError::Unrecognised { reason, .. } => reason.split("byte offset ").nth(1).and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next()).map(str::to_string),
                    _ => None,
                };
                span.record("outcome", "refused").record("cause", e.to_string()).record("duration", began.elapsed().as_millis() as u64);
                if let Some(o) = offset {
                    span.record("invalid_offset", o);
                }
                Err(e)
            }
        }
    }

    /// The file.
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    /// The store's file.
    pub fn store_name(&self) -> String {
        self.0.path.display().to_string()
    }

    /// The fold's consumed offset.
    pub fn consumed(&self) -> u64 {
        self.0.fold.lock().unwrap_or_else(|e| e.into_inner()).consumed
    }

    /// Read the file's lines from the fold's consumed offset to its end into the fold (R-SR4): only
    /// lines read whole move the offset. A complete valid object with no final newline is ended with
    /// one newline alone and fsynced, then read; a line that does not verify refuses the read by name,
    /// its offset named, and the fold is left as it was.
    pub async fn advance(&self) -> Result<(), StorageError> {
        let caller = caller_span();
        let _one = match self.0.reading.try_lock() {
            Ok(one) => one,
            Err(_) => {
                self.0.read_waits(caller);
                self.0.reading.lock().await
            }
        };
        let _by = ReadingBy::begin(&self.0, caller);
        #[cfg(feature = "testkit-store")]
        let door = read_door::take(&self.0.path, caller);
        let from = self.consumed();
        let inner = self.0.clone();
        let lines = blocking_io(move || {
            #[cfg(feature = "testkit-store")]
            if let Some(door) = door {
                door.hold();
            }
            inner.read_whole(from)
        })
        .await?;
        if let Some(observe) = &self.0.observer {
            for l in &lines.0 {
                observe(l);
            }
        }
        self.0.fold.lock().unwrap_or_else(|e| e.into_inner()).take(lines.0);
        self.0.fold.lock().unwrap_or_else(|e| e.into_inner()).consumed = lines.1;
        Ok(())
    }

    /// The latest line of (`kind`, `key`) after reading the tail.
    pub async fn latest(&self, kind: &str, key: &str) -> Result<Option<Line>, StorageError> {
        self.advance().await?;
        Ok(self.0.fold.lock().unwrap_or_else(|e| e.into_inner()).latest.get(&(kind.to_string(), key.to_string())).cloned())
    }

    /// The latest line of every key of `kind`, in key order, after reading the tail.
    pub async fn latest_of(&self, kind: &str) -> Result<Vec<Line>, StorageError> {
        self.advance().await?;
        let out: BTreeMap<String, Line> = self.0.fold.lock().unwrap_or_else(|e| e.into_inner()).latest.iter().filter(|((k, _), _)| k == kind).map(|((_, key), l)| (key.clone(), l.clone())).collect();
        Ok(out.into_values().collect())
    }

    /// Every line of the file, in file order, read whole from byte 0 (the raw log).
    pub async fn lines(&self) -> Result<Vec<Line>, StorageError> {
        let inner = self.0.clone();
        let (lines, _) = blocking_io(move || inner.read_whole(0)).await?;
        Ok(lines)
    }

    /// The greatest `seq` of each writer the fold has read, by (node id, incarnation).
    pub fn writers(&self) -> BTreeMap<(String, String), u64> {
        self.0.fold.lock().unwrap_or_else(|e| e.into_inner()).ops.clone()
    }

    /// Append `record` as one line in `format` at `key`, written by `writer` (R-SR3): this store's
    /// pending append is reconciled first, then the line is written in one `write` and fsynced, and success is
    /// answered only after the fsync.
    pub async fn append<T: Serialize + ?Sized>(&self, writer: &LogWriter, format: &str, key: &str, record: &T) -> Result<(), StorageError> {
        let bytes = serde_json::to_vec(record).map_err(|e| StorageError::Io { file: self.0.path.display().to_string(), reason: format!("the {format} record at {key:?} does not encode: {e}") })?;
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("encoded JSON parses");
        self.0.formats.check(format, &value).map_err(|why| StorageError::Unrecognised { file: self.0.path.display().to_string(), reason: format!("a write refused: {why}") })?;
        let guard = self.0.append.clone().lock_owned().await;
        let inner = self.0.clone();
        let (writer, format, key) = (writer.clone(), format.to_string(), key.to_string());
        blocking_io(move || {
            let mut guard = guard;
            inner.append_line(&mut *guard, &writer, &format, &key, &bytes)
        })
        .await
    }

    /// Append `line` as its writer wrote it, its `op` kept (storage.md §2 Copy: the old provider's lines
    /// appended to the new one, every writer's included): this store's pending append is reconciled
    /// first, then the line is written and fsynced as [`Self::append`] writes one, and a sequence this
    /// process hands its writer later in this file is past it.
    pub async fn append_line(&self, line: &Line) -> Result<(), StorageError> {
        let file = self.0.path.display().to_string();
        let op = line.op.clone().ok_or_else(|| StorageError::Io { file: file.clone(), reason: format!("the {} line at {:?} has no op: only a line on a store is appended as written", line.format, line.key) })?;
        let bytes = match &line.written {
            Some(written) => written.clone(),
            None => serde_json::to_vec(&line.record).map_err(|e| StorageError::Io { file: file.clone(), reason: format!("the {} record at {:?} does not encode: {e}", line.format, line.key) })?,
        };
        self.0.formats.check(&line.format, &line.record).map_err(|why| StorageError::Unrecognised { file, reason: format!("a write refused: {why}") })?;
        let guard = self.0.append.clone().lock_owned().await;
        let inner = self.0.clone();
        let (format, key) = (line.format.clone(), line.key.clone());
        blocking_io(move || {
            let mut guard = guard;
            inner.append_written(&mut *guard, op, &format, &key, &bytes)
        })
        .await
    }

    /// Reconcile this store's pending append, if it holds one: done, or refused by name.
    pub async fn reconcile(&self) -> Result<(), StorageError> {
        let guard = self.0.append.clone().lock_owned().await;
        let inner = self.0.clone();
        blocking_io(move || {
            let mut guard = guard;
            inner.settle(&mut *guard)
        })
        .await
    }

    /// Wait for every append this store issued.
    pub async fn finish_issued_writes(&self) {
        drop(self.0.append.lock().await);
    }

    #[cfg(test)]
    pub(crate) async fn open_with(path: &Path, formats: Formats, io: Arc<dyn AppendIo>) -> Result<Self, StorageError> {
        Self::opened(path, formats, io, None, None).await
    }

    /// Forget the `seq`s this process handed out for `path`: what a restart of the process is.
    #[cfg(test)]
    pub(crate) fn forget_issued(path: &Path) {
        issued().lock().unwrap().retain(|(p, _), _| p != path);
    }
}

/// The name of the span the caller runs under, `""` under none: what a tail read is named by.
fn caller_span() -> &'static str {
    tracing::Span::current().metadata().map(|m| m.name()).unwrap_or("")
}

/// The tail read in progress on a file, named while it runs.
struct ReadingBy<'a>(&'a Inner);

impl<'a> ReadingBy<'a> {
    fn begin(inner: &'a Inner, caller: &'static str) -> Self {
        *inner.reading_by.lock().unwrap_or_else(|e| e.into_inner()) = Some((caller, std::time::Instant::now()));
        Self(inner)
    }
}

impl Drop for ReadingBy<'_> {
    fn drop(&mut self) {
        *self.0.reading_by.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

impl Inner {
    /// A tail read called under `caller` waits for the one in progress on this file: named on its own
    /// span when the wait begins, so a wait that never ends still names the read it waits for.
    fn read_waits(&self, caller: &'static str) {
        let (held_by, held_ms) = match *self.reading_by.lock().unwrap_or_else(|e| e.into_inner()) {
            Some((by, since)) => (by, since.elapsed().as_millis() as u64),
            None => ("", 0),
        };
        tracing::info_span!("rdm.node.store.update.via-tail-read-waiting", file = %self.path.display(), caller, held_by, held_ms)
            .in_scope(|| tracing::info!("this tail read waits for the one in progress on the same file"));
    }

    fn named(&self, offset: u64, why: impl std::fmt::Display) -> StorageError {
        StorageError::Unrecognised { file: self.path.display().to_string(), reason: format!("the line at byte offset {offset} is malformed: {why}; nothing older is answered as current and the file is left as it is") }
    }

    fn io(&self, e: std::io::Error) -> StorageError {
        StorageError::Io { file: self.path.display().to_string(), reason: e.to_string() }
    }

    /// Read the lines from `from` to the end whole: a complete valid object with no final newline is
    /// ended with one newline alone and fsynced, then read again; an unended object that does not
    /// verify is read again while the file still grows (another writer's write in flight), then
    /// refused by name. Answers the lines and the offset they were read whole to.
    fn read_whole(&self, from: u64) -> Result<(Vec<Line>, u64), StorageError> {
        let mut quiet = 0;
        let mut ended = false;
        loop {
            let bytes = read_from(&self.file, from).map_err(|e| self.io(e))?;
            let s = scan(&bytes, from, &self.formats).map_err(|(at, why)| self.named(at, why))?;
            let Some((at, tail)) = s.tail else { return Ok((s.lines, s.consumed)) };
            match verify(&tail, at, &self.formats) {
                Ok(_) if !ended => {
                    // A complete object missing only its newline: end it, then read it whole.
                    let n = self.io.write(&self.file, b"\n").map_err(|e| self.io(e))?;
                    if n != 1 {
                        return Err(StorageError::Io { file: self.path.display().to_string(), reason: format!("the newline ending the object at byte offset {at} was not written ({n} bytes)") });
                    }
                    self.io.sync(&self.file).map_err(|e| self.io(e))?;
                    ended = true;
                }
                Ok(_) => return Err(self.named(at, "the object stays unended after a newline was appended to end it")),
                Err(why) => {
                    let len = self.file.metadata().map_err(|e| self.io(e))?.len();
                    if len > from + bytes.len() as u64 {
                        quiet = 0;
                        continue;
                    }
                    if quiet < 20 {
                        quiet += 1;
                        std::thread::sleep(std::time::Duration::from_millis(2));
                        continue;
                    }
                    return Err(self.named(at, format!("an unended object: {why}")));
                }
            }
        }
    }

    /// The next `op` of `writer` in this file: past every `seq` of it the fold has read and this
    /// process has handed out.
    fn next_op(&self, writer: &LogWriter) -> Op {
        let ids = writer.ids();
        let durable = self.fold.lock().unwrap_or_else(|e| e.into_inner()).ops.get(&ids).copied().unwrap_or(0);
        let mut issued = issued().lock().unwrap_or_else(|e| e.into_inner());
        let last = issued.entry((self.path.clone(), ids.clone())).or_insert(0);
        *last = (*last).max(durable) + 1;
        Op { node_id: ids.0, incarnation_id: ids.1, seq: *last }
    }

    fn append_line(&self, pending: &mut Option<Pending>, writer: &LogWriter, format: &str, key: &str, record: &[u8]) -> Result<(), StorageError> {
        self.settle(pending)?;
        let op = self.next_op(writer);
        self.write_new(pending, op, format, key, record)
    }

    /// Append a line under the `op` its writer wrote it with: this process's sequence of that writer in
    /// this file moves past it, so no later write reuses it.
    fn append_written(&self, pending: &mut Option<Pending>, op: Op, format: &str, key: &str, record: &[u8]) -> Result<(), StorageError> {
        self.settle(pending)?;
        let mut issued = issued().lock().unwrap_or_else(|e| e.into_inner());
        let last = issued.entry((self.path.clone(), (op.node_id.clone(), op.incarnation_id.clone()))).or_insert(0);
        *last = (*last).max(op.seq);
        drop(issued);
        self.write_new(pending, op, format, key, record)
    }

    fn write_new(&self, pending: &mut Option<Pending>, op: Op, format: &str, key: &str, record: &[u8]) -> Result<(), StorageError> {
        let obj = encode_object(&op, format, key, record);
        let from = self.fold.lock().unwrap_or_else(|e| e.into_inner()).consumed;
        *pending = Some(Pending { op, obj, from, reported: 0 });
        self.write_pending(pending)
    }

    /// Write the pending append's line in one `write`, then fsync: done only when both answered in
    /// full. A write that reported zero bytes is retried under the same `op` once the lines after
    /// the fold's offset validate; a short write is reconciled at once; a failed fsync stays pending.
    fn write_pending(&self, pending: &mut Option<Pending>) -> Result<(), StorageError> {
        let mut zero = 0;
        loop {
            let (line, op) = {
                let p = pending.as_ref().expect("a pending append");
                (line_of(&p.obj), describe(&p.op))
            };
            let wrote = self.io.write(&self.file, &line);
            pending.as_mut().expect("a pending append").reported = *wrote.as_ref().unwrap_or(&0);
            match wrote {
                Ok(n) if n == line.len() => {
                    return match self.io.sync(&self.file) {
                        Ok(()) => {
                            *pending = None;
                            Ok(())
                        }
                        Err(e) => Err(StorageError::Io { file: self.path.display().to_string(), reason: format!("indeterminate: the line of op {op} was written and its fsync failed ({e}); it is reconciled by its op before this store's next write") }),
                    };
                }
                Ok(n) if n > 0 => return self.settle(pending),
                written => {
                    if let Reconciled::Done = self.reconcile(pending.as_ref().expect("a pending append"))? {
                        *pending = None;
                        return Ok(());
                    }
                    zero += 1;
                    if zero >= 3 {
                        let why = written.err().map(|e| e.to_string()).unwrap_or_else(|| "it reported zero bytes".into());
                        *pending = None;
                        return Err(StorageError::Io { file: self.path.display().to_string(), reason: format!("the line of op {op} was not written: {why}, {zero} times; no byte of it is in the file") });
                    }
                }
            }
        }
    }

    /// Settle the pending append, if any, before another write: done (its object stands whole and is
    /// fsynced), written again (nothing of it is in the file and its write reported zero bytes), or
    /// refused by name, the append still pending.
    fn settle(&self, pending: &mut Option<Pending>) -> Result<(), StorageError> {
        let Some(p) = pending.as_ref() else { return Ok(()) };
        match self.reconcile(p)? {
            Reconciled::Done => {
                *pending = None;
                Ok(())
            }
            Reconciled::Absent if p.reported == 0 => self.write_pending(pending),
            Reconciled::Absent => Err(StorageError::Io { file: self.path.display().to_string(), reason: format!("indeterminate: the write of op {} reported {} bytes and no byte of its object is in the file after byte offset {}; nothing is written in its place", describe(&p.op), p.reported, p.from) }),
        }
    }

    /// Find the pending append by its object (its `op` makes it unique) from the fold's offset before
    /// its write, every other line between validated.
    fn reconcile(&self, p: &Pending) -> Result<Reconciled, StorageError> {
        let bytes = read_from(&self.file, p.from).map_err(|e| self.io(e))?;
        let refused = |at: u64, why: String| StorageError::Io { file: self.path.display().to_string(), reason: format!("indeterminate: the write of op {} cannot be reconciled: {why} at byte offset {at}; nothing is appended in its place and no byte is discarded", describe(&p.op)) };
        let mut start = 0usize;
        loop {
            let at = p.from + start as u64;
            let rest = &bytes[start..];
            let (seg, ended) = match rest.iter().position(|b| *b == b'\n') {
                Some(nl) => (&rest[..nl], true),
                None => (rest, false),
            };
            if seg == p.obj.as_slice() {
                if !ended {
                    // Complete but for its newline: finish it with one newline alone.
                    let n = self.io.write(&self.file, b"\n").map_err(|e| self.io(e))?;
                    if n != 1 {
                        return Err(refused(at, format!("its ending newline wrote {n} bytes")));
                    }
                }
                self.io.sync(&self.file).map_err(|e| StorageError::Io { file: self.path.display().to_string(), reason: format!("indeterminate: the line of op {} stands whole and its fsync failed ({e})", describe(&p.op)) })?;
                return Ok(Reconciled::Done);
            }
            if !seg.is_empty() {
                if p.obj.starts_with(seg) {
                    return Err(refused(at, "a partial object of it stands".into()));
                }
                if let Err(why) = verify(seg, at, &self.formats) {
                    return Err(refused(at, format!("the line there is malformed: {why}")));
                }
            }
            if !ended {
                return Ok(Reconciled::Absent);
            }
            start += seg.len() + 1;
        }
    }
}

fn describe(op: &Op) -> String {
    format!("{}/{}#{}", op.node_id, op.incarnation_id, op.seq)
}

// ---------------------------------------------------------------- the locks (R-SR5)

/// Which locks of a store root: the attempt in flight on a mesh name, or the claim of a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    /// `locks/in-flight/<mesh name>`: the one attempt in flight on that mesh name.
    InFlight,
    /// `locks/name/<name>`: the claim of a mesh name or a node path.name, the lock being the claim.
    Name,
    /// `locks/commit/<build_id>.<change_id>`: a storage change's `commit` record (storage.md §2 step 4),
    /// inserted on the new provider; the insert is the switch.
    Commit,
}

impl LockKind {
    fn dir(self) -> &'static str {
        match self {
            Self::InFlight => "in-flight",
            Self::Name => "name",
            Self::Commit => "commit",
        }
    }
}

/// The locks of a store root (R-SR5), `<root>/locks/`: each a create-exclusive file whose content
/// names its claimant, because an append cannot refuse. A take is the filesystem's own atomic link of
/// a finished file to the lock's path, which fails when the lock stands. Every removal of a name lock
/// runs under the name's reclamation mutex, an `flock(LOCK_EX)` on `locks/name/<name>.reclaim` (a file
/// that is only ever an anchor and is never removed, dropped by the kernel if its holder dies), and
/// re-reads the lock inside it, removing it only while its content still names the claimant the remover
/// expects. The persistent claim is the file because it outlives processes; the transient exclusion is
/// the flock because it must not.
#[derive(Debug, Clone)]
pub struct StoreLocks {
    dir: PathBuf,
    writer: String,
}

/// What a take of a lock came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockTaken {
    /// This take inserted the lock.
    Won,
    /// The lock stands: its content.
    Held(Vec<u8>),
}

impl StoreLocks {
    /// The locks of the store root `root_dir`, taken by the process born as `writer` (its temp files
    /// are named by it).
    pub fn at(root_dir: &Path, writer: &LogWriter) -> Self {
        Self { dir: root_dir.join(crate::store_root::LOCKS_DIR), writer: format!("{}-{}", writer.node_id, writer.incarnation_id) }
    }

    /// The lock `kind` of `name`.
    pub fn path(&self, kind: LockKind, name: &str) -> PathBuf {
        self.dir.join(kind.dir()).join(name)
    }

    fn checked(kind: LockKind, name: &str, dir: &Path) -> Result<(), StorageError> {
        if name.is_empty() || name.starts_with('.') || name.contains(['/', '\\']) || name.ends_with(".reclaim") {
            return Err(StorageError::Io { file: dir.join(kind.dir()).display().to_string(), reason: format!("{name:?} is not one path segment, so it names no lock") });
        }
        Ok(())
    }

    /// Take the lock `kind` of `name` with `content`: `Won` when this take inserted it, else `Held`
    /// with the standing lock's content. A lock the insert met and the read no longer finds was
    /// released between the two: refused by name, never answered as either outcome.
    pub async fn take(&self, kind: LockKind, name: &str, content: &[u8]) -> Result<LockTaken, StorageError> {
        Self::checked(kind, name, &self.dir)?;
        let (path, content) = (self.path(kind, name), content.to_vec());
        let tmp = self.dir.join(kind.dir()).join(format!(".{name}.{}.tmp", self.writer));
        blocking_io(move || {
            let dir = path.parent().expect("a lock's directory").to_path_buf();
            std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
            if crate::record_store::insert_file(&dir, &tmp, &path, &content)? {
                return Ok(LockTaken::Won);
            }
            match read_lock(&path)? {
                Some(held) => Ok(LockTaken::Held(held)),
                None => Err(StorageError::Io { file: path.display().to_string(), reason: "the lock stood at the take and was gone at the read after it: its claimant released it between the two; nothing is taken, and the next take decides on the free lock".into() }),
            }
        })
        .await
    }

    /// Every lock `kind` that stands, by name, with its content, in name order: a temp file of a take
    /// and a name's reclamation anchor are not locks.
    pub async fn held(&self, kind: LockKind) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let dir = self.dir.join(kind.dir());
        blocking_io(move || {
            let entries = match std::fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(e) => return Err(StorageError::Io { file: dir.display().to_string(), reason: e.to_string() }),
            };
            let mut held = BTreeMap::new();
            for entry in entries {
                let entry = entry.map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') || name.ends_with(".reclaim") {
                    continue;
                }
                if let Some(content) = read_lock(&entry.path())? {
                    held.insert(name, content);
                }
            }
            Ok(held.into_iter().collect())
        })
        .await
    }

    /// The content of the lock `kind` of `name`, `None` when it does not stand.
    pub async fn read(&self, kind: LockKind, name: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Self::checked(kind, name, &self.dir)?;
        let path = self.path(kind, name);
        blocking_io(move || read_lock(&path)).await
    }

    /// Remove the lock `kind` of `name` while `names` says its content still names the claimant the
    /// remover expects, re-read under the name's reclamation mutex for a name lock (R-SR5). Answers
    /// whether it was removed; a lock naming another claimant is left as it is.
    pub async fn release(&self, kind: LockKind, name: &str, names: impl Fn(&[u8]) -> bool + Send + 'static) -> Result<bool, StorageError> {
        Self::checked(kind, name, &self.dir)?;
        let path = self.path(kind, name);
        let anchor = (kind == LockKind::Name).then(|| self.dir.join(kind.dir()).join(format!("{name}.reclaim")));
        blocking_io(move || {
            let dir = path.parent().expect("a lock's directory").to_path_buf();
            let _mutex = match &anchor {
                Some(a) => {
                    std::fs::create_dir_all(&dir).map_err(|e| StorageError::Io { file: dir.display().to_string(), reason: e.to_string() })?;
                    Some(NameMutex::lock(a)?)
                }
                None => None,
            };
            match read_lock(&path)? {
                Some(held) if names(&held) => crate::record_store::remove_file(&dir, &path),
                _ => Ok(false),
            }
        })
        .await
    }

    /// The reclamation mutex of `name`, held by this process until the guard drops (a cell's hold of
    /// it, as another process's reclaimer inside it).
    #[cfg(test)]
    pub(crate) fn hold_mutex(&self, name: &str) -> NameMutex {
        std::fs::create_dir_all(self.dir.join(LockKind::Name.dir())).unwrap();
        NameMutex::lock(&self.dir.join(LockKind::Name.dir()).join(format!("{name}.reclaim"))).unwrap()
    }
}

/// The bytes of the lock at `path`, `None` when it does not stand.
fn read_lock(path: &Path) -> Result<Option<Vec<u8>>, StorageError> {
    match std::fs::read(path) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
    }
}

/// A name's reclamation mutex: `flock(LOCK_EX)` on its anchor file, held until dropped (or until its
/// holder dies: the kernel drops it).
pub(crate) struct NameMutex(#[allow(dead_code)] std::fs::File);

impl NameMutex {
    fn lock(anchor: &Path) -> Result<Self, StorageError> {
        use std::os::fd::AsRawFd as _;
        let io = |e: std::io::Error| StorageError::Io { file: anchor.display().to_string(), reason: format!("the name's reclamation mutex: {e}") };
        let f = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(false).open(anchor).map_err(io)?;
        loop {
            if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) } == 0 {
                return Ok(Self(f));
            }
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::Interrupted {
                return Err(io(e));
            }
        }
    }
}

/// The testkit's door on a store file's tail read (feature `testkit-store`, for rdm-test; absent from
/// production builds): armed for a file and the span a read is called under, it holds the first such
/// read inside the read itself, on its blocking call, until the cell releases it. Every other read of
/// the file passes.
#[cfg(feature = "testkit-store")]
pub mod read_door {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Condvar, Mutex, OnceLock};

    /// One armed door.
    pub struct Door {
        file: PathBuf,
        under: &'static str,
        released: Mutex<bool>,
        opened: Condvar,
        entered: tokio::sync::watch::Sender<bool>,
    }

    fn armed() -> &'static Mutex<Vec<Arc<Door>>> {
        static ARMED: OnceLock<Mutex<Vec<Arc<Door>>>> = OnceLock::new();
        ARMED.get_or_init(Default::default)
    }

    /// Hold the first tail read of a file whose path ends with `file` that is called under a span
    /// named `under`, inside the read, until [`Door::release`].
    pub fn arm(file: &Path, under: &'static str) -> Arc<Door> {
        let door = Arc::new(Door { file: file.to_path_buf(), under, released: Mutex::new(false), opened: Condvar::new(), entered: tokio::sync::watch::Sender::new(false) });
        armed().lock().unwrap().push(door.clone());
        door
    }

    /// The door armed for a read of `path` called under `caller`, taken: it holds this read only.
    pub(crate) fn take(path: &Path, caller: &'static str) -> Option<Arc<Door>> {
        let mut armed = armed().lock().unwrap();
        let at = armed.iter().position(|d| path.ends_with(&d.file) && d.under == caller)?;
        Some(armed.remove(at))
    }

    impl Door {
        /// On the read's blocking call: say it entered, then wait for the release.
        pub(crate) fn hold(&self) {
            tracing::info_span!("rdm.testkit.fault.update.via-hold", cut = "store-tail-read", detail = %format!("{} under {}", self.file.display(), self.under))
                .in_scope(|| tracing::info!("the testkit holds this tail read inside the read until the cell releases it"));
            self.entered.send_replace(true);
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.opened.wait(released).unwrap();
            }
        }

        /// Resolves once the read it holds is inside it.
        pub async fn entered(&self) {
            let _ = self.entered.subscribe().wait_for(|e| *e).await;
        }

        /// Let the held read go on.
        pub fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.opened.notify_all();
        }
    }
}

#[cfg(test)]
mod tests;
