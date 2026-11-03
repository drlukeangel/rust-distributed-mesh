//! One append-only JSONL file of a store root (R-SR1 to R-SR4, proposal
//! `store-root-one-directory-per-entity.md`): every write of a record is one line carrying the whole
//! record, the latest complete line for a subject is the record's current state, and nothing is ever
//! removed.
//!
//! **A line** (R-SR3, Q365) is `\n` + one JSON object + `\n`, the object a CloudEvents 1.0 event in its
//! JSON form, written as
//!
//! ```text
//! {"len":N,"specversion":"1.0","id":"<uuidv7>","source":"rdm://<fabric_id>/<node_id>/<incarnation>","type":"<type>","subject":"…","time":"…","datacontenttype":"application/json","dataschema":"<type>/<n>","data":{…},"crc32":"xxxxxxxx"}
//! ```
//!
//! - `len` is the object's byte count between its newlines, itself included: the smallest decimal
//!   `N` for which `{"len":` + `N` + the rest is `N` bytes long.
//! - `id` is the write: a time-ordered UUIDv7 ([`EventId`]) minted once for it. A retry of one write
//!   keeps its `id`; a separate write gets a new one even when its record is identical, so no two
//!   writes share an object.
//! - `source` is the writer's exact birth; `time` the writer's rafka-time at the write, absent before
//!   its process holds one (never another clock).
//! - `type` is the record kind and `dataschema` its kind and version; `subject` the record's key
//!   inside its kind, absent on an event that is about no node.
//! - `traceparent` and `tracestate`, when present, are the writer's W3C trace context (an event's).
//! - `data` is the whole record, nested so no field of it can be read as a field of the line, and
//!   last before `crc32`.
//! - `crc32` is the CRC-32 (IEEE) of the object's bytes from its opening brace up to, not including,
//!   the final `,"crc32":`, as eight lowercase hex digits: the last 20 bytes of every object are
//!   `,"crc32":"xxxxxxxx"}`. `len` and `crc32` are the line's two extension attributes.
//!
//! **The append.** A writer appends one line in one `write` on a descriptor opened `O_APPEND`, then
//! `fsync`s, and reports success only after the fsync. A short write and a failed fsync are
//! indeterminate, never success: the writer holds the write as its pending append and reconciles it
//! before its next write, by its object (its `id` makes it unique), from the offset its fold had consumed before the write,
//! validating every line between. A complete, terminated object is fsynced and accepted; a complete
//! object missing only its newline is finished with one newline alone and fsynced; a write that
//! reported zero bytes, with every line between valid, is retried under the same `id`. A partial
//! object, or an outcome the writer cannot identify, is refused by name: nothing is appended in its
//! place and no byte is discarded.
//!
//! **The fold** (R-SR3, R-SR4) skips blank lines only. Every other line must hold exactly `len`
//! bytes, match its `crc32` and parse as an event in a `dataschema` the store reads: a line that does not, the last
//! one included, refuses the open or the tail read by name, naming the file and the byte offset, and
//! nothing older is answered as current. A complete valid object with no final newline is kept: the
//! reader appends one newline alone and fsyncs before it answers (another writer's opening newline
//! may already have ended it; the added newline is then a blank line). An unended object that does
//! not verify is another writer's line still being written, however long it stands: a tail read
//! answers the lines before it and leaves it unconsumed for its next read, recorded on
//! [`UNENDED_SPAN`] with its offset, and an open refuses it by
//! name, naming the file and its offset (R-SR3, "at open ... an incomplete or invalid object is left
//! untouched and refused"). Once a line stands after it, its write has ended: the object is then a
//! line that does not verify and refuses every read by name. The consumed offset is the fold's own:
//! it moves only over lines the fold has read whole, never to where a write landed, so a writer's own
//! append does not move it and its next tail read reads every line after it, its own included.
//!
//! The file backend requires a local filesystem whose `O_APPEND` writes are serialized by the shared
//! host kernel (R14 item 5, as amended by R-SR3).

use crate::blocking::blocking_io;
use crate::storage_error::StorageError;
pub use rdm_core::EventId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// The span the first fold of a store file from byte 0 records: at its open for a store opened to
/// fold ([`StoreLog::open`]), at its first read or first append for one opened on its first read
/// ([`StoreLog::open_on_first_read`], rdm#89, Q227), under the span that read or append runs in. It
/// names the file, its provider when the opener names it, the caller's span and the reader, the
/// file's bytes and lines, each `source` with its count of lines and the fold's `duration` in milliseconds; a
/// refused fold its `invalid_offset` and cause (i149 plan, "OTLP span-assertion bar").
pub const OPEN_SPAN: &str = "rdm.node.store.resolve.via-fold";

/// The span a tail read records, under its caller's span, when it meets an unended object that does
/// not verify at the end of the file and leaves it unconsumed (R-SR4): the file, the reader, the
/// object's offset (where the consumed offset stands) and the bytes of it the file held then.
pub const UNENDED_SPAN: &str = "rdm.node.store.update.via-tail-read-unended";

/// The bytes every object ends with after its checksummed bytes: `,"crc32":"xxxxxxxx"}`.
const CRC_SUFFIX: usize = 20;

/// The CloudEvents version every line carries.
pub const SPEC_VERSION: &str = "1.0";
/// The content type of every line's `data`.
pub const DATA_CONTENT_TYPE: &str = "application/json";

/// `ms` of rafka-time as RFC 3339 in UTC milliseconds: a line's `time`.
pub fn rfc3339(ms: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms as i64).unwrap_or_default().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// The process writing through a store: its exact birth, every line's `source`, and its rafka-time,
/// every line's `time`.
#[derive(Debug, Clone)]
pub struct LogWriter {
    /// The writer's fabric.
    pub fabric_id: crate::model::FabricId,
    /// The writer's node id.
    pub node_id: crate::model::NodeId,
    /// The writer's incarnation.
    pub incarnation_id: crate::model::IncarnationId,
    /// The writer's rafka-time: a line written before its process holds one carries no `time`.
    pub time: rdm_transport::clock::RafkaTime,
}

impl LogWriter {
    /// The writer born as `incarnation` of `node` in `fabric`, stamping `time`.
    pub fn new(fabric: &crate::model::FabricId, node: &crate::model::NodeId, incarnation: &crate::model::IncarnationId, time: &rdm_transport::clock::RafkaTime) -> Self {
        Self { fabric_id: fabric.clone(), node_id: node.clone(), incarnation_id: incarnation.clone(), time: time.clone() }
    }

    /// A writer of a newly minted fabric, node id and incarnation, holding no rafka-time: a store a
    /// cell opens.
    pub fn mint() -> Self {
        Self::new(&crate::model::FabricId::mint(), &crate::model::NodeId::mint(), &crate::model::IncarnationId::mint(), &rdm_transport::clock::RafkaTime::unadopted())
    }

    /// `rdm://<fabric_id>/<node_id>/<incarnation>`: every line's `source`.
    pub fn source(&self) -> String {
        format!("rdm://{}/{}/{}", self.fabric_id, self.node_id, self.incarnation_id)
    }

    /// The writer's rafka-time now as a line's `time`, `None` while its process holds none.
    pub fn time(&self) -> Option<String> {
        self.time.try_now_ms().map(rfc3339)
    }
}

impl std::fmt::Display for LogWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.source())
    }
}

/// A line's CloudEvents attributes, every one but `data` (Q365): `specversion` and
/// `datacontenttype` are the same on every line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attributes {
    /// The write: a time-ordered UUIDv7.
    pub id: EventId,
    /// The writer's exact birth, `rdm://<fabric_id>/<node_id>/<incarnation>`.
    pub source: String,
    /// The record kind.
    pub r#type: String,
    /// The record's key inside its kind; absent on an event about no node.
    pub subject: Option<String>,
    /// The writer's rafka-time at the write, RFC 3339 in UTC milliseconds; absent before its process
    /// held one.
    pub time: Option<String>,
    /// `<type>/<n>`: the record's kind and schema version.
    pub dataschema: String,
    /// The writer's W3C trace parent.
    pub traceparent: Option<String>,
    /// The writer's W3C trace state.
    pub tracestate: Option<String>,
}

impl Attributes {
    /// The attributes of a new write by `writer` of a `type` record in `dataschema` at `subject`: its
    /// `id` minted, its `time` the writer's rafka-time.
    pub fn of(writer: &LogWriter, r#type: &str, dataschema: &str, subject: &str) -> Self {
        Self { id: EventId::mint(), source: writer.source(), r#type: r#type.into(), subject: Some(subject.into()), time: writer.time(), dataschema: dataschema.into(), traceparent: None, tracestate: None }
    }

    /// The node id of the writer in `source`.
    pub fn source_node_id(&self) -> Option<&str> {
        self.source.strip_prefix("rdm://")?.split('/').nth(1)
    }
}

/// One record as a store reads it: its attributes, its whole record, and the object's bytes as
/// its writer wrote them, with the byte offset of the object in the file.
#[derive(Debug, Clone, PartialEq)]
pub struct Line {
    /// The object's byte offset in the file; `None` for a line made in memory.
    pub offset: Option<u64>,
    /// Its CloudEvents attributes.
    pub attributes: Attributes,
    /// The whole record.
    pub data: serde_json::Value,
    /// The object's bytes, no newlines: a copy appends these, so the new provider holds the line
    /// byte for byte (storage.md §2 Copy).
    pub object: Vec<u8>,
}

impl Line {
    /// A line made in memory of `attributes` and the record `data`.
    pub fn new(attributes: Attributes, data: serde_json::Value) -> Self {
        let object = encode_object(&attributes, &serde_json::to_vec(&data).expect("a record encodes"));
        Self { offset: None, attributes, data, object }
    }

    /// The record kind.
    pub fn kind(&self) -> &str {
        &self.attributes.r#type
    }

    /// The record's key inside its kind, `""` for a line with none.
    pub fn subject(&self) -> &str {
        self.attributes.subject.as_deref().unwrap_or("")
    }

    /// The record as `T`, refused by name (the file and the line's offset) when it does not decode.
    pub fn decode<T: DeserializeOwned>(&self, file: &Path) -> Result<T, StorageError> {
        T::deserialize(&self.data).map_err(|e| StorageError::Unrecognised { file: file.display().to_string(), reason: format!("the {} line{} does not decode: {e}", self.attributes.dataschema, self.offset.map(|o| format!(" at byte offset {o}")).unwrap_or_default()) })
    }
}

/// A check that a record decodes as its `dataschema`'s type.
type Check = Arc<dyn Fn(&serde_json::Value) -> Result<(), String> + Send + Sync>;

/// The `dataschema`s a store reads in one file, each with the check its records must pass: a line in
/// any other, or whose record fails its check, refuses the fold by name.
#[derive(Clone, Default)]
pub struct Formats {
    checks: BTreeMap<String, Check>,
    /// Every `dataschema` is read, unchecked: a look at a file, never a store's fold.
    any: bool,
}

impl Formats {
    /// No `dataschema`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every `dataschema`, its records unchecked: a look at a whole file ([`read_file`]).
    pub fn any() -> Self {
        Self { checks: BTreeMap::new(), any: true }
    }

    /// `dataschema`, whose records decode as `T`.
    pub fn with<T: DeserializeOwned + 'static>(mut self, dataschema: &str) -> Self {
        self.checks.insert(dataschema.to_string(), Arc::new(|v: &serde_json::Value| T::deserialize(v).map(|_| ()).map_err(|e| e.to_string())));
        self
    }

    fn check(&self, dataschema: &str, record: &serde_json::Value) -> Result<(), String> {
        match self.checks.get(dataschema) {
            Some(check) => check(record).map_err(|e| format!("its {dataschema} record does not decode: {e}")),
            None if self.any => Ok(()),
            None => Err(format!("dataschema {dataschema:?}, this build reads {:?} here", self.checks.keys().collect::<Vec<_>>())),
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
/// its offset; a valid object with no final newline is read as a line, and an unended object that
/// does not verify (another writer's line still being written) is left out.
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
    if let Some(line) = s.tail.and_then(|(at, tail)| verify(&tail, at, &formats).ok()) {
        lines.push(line);
    }
    Ok(lines)
}

/// The latest record of `type` (at `subject`, when given) in the store file at `path`, as
/// `{"type": …, "subject": …, "dataschema": …, "data": …}`; `None` when there is none, or when the
/// file cannot be read whole: a look a cell takes at a root while its nodes write it.
pub fn read_record(path: &Path, r#type: &str, subject: Option<&str>) -> Option<serde_json::Value> {
    let lines = read_file(path).ok()?;
    let l = lines.into_iter().rev().find(|l| l.kind() == r#type && subject.is_none_or(|k| l.subject() == k))?;
    Some(serde_json::json!({ "type": l.attributes.r#type, "subject": l.attributes.subject, "dataschema": l.attributes.dataschema, "data": l.data }))
}

/// The subjects of `type` the store file at `path` holds a line of, sorted ([`read_file`]).
pub fn subjects_in(path: &Path, r#type: &str) -> Result<Vec<String>, StorageError> {
    Ok(latest_in(path)?.into_keys().filter(|(k, _)| k == r#type).map(|(_, subject)| subject).collect())
}

/// The latest line of each (type, subject) of the store file at `path` ([`read_file`]): a line with
/// no subject is in none.
pub fn latest_in(path: &Path) -> Result<BTreeMap<(String, String), Line>, StorageError> {
    Ok(read_file(path)?.into_iter().filter(|l| l.attributes.subject.is_some()).map(|l| ((l.kind().to_string(), l.subject().to_string()), l)).collect())
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

/// The object (no newlines) of one write: its `len`, its attributes in their order, `data` (the
/// record's bytes) and its `crc32`.
pub fn encode_object(attributes: &Attributes, data: &[u8]) -> Vec<u8> {
    let json = |s: &str| serde_json::to_string(s).expect("a string encodes");
    let mut rest = format!(",\"specversion\":{},\"id\":{},\"source\":{},\"type\":{}", json(SPEC_VERSION), json(attributes.id.as_str()), json(&attributes.source), json(&attributes.r#type));
    if let Some(subject) = &attributes.subject {
        rest.push_str(&format!(",\"subject\":{}", json(subject)));
    }
    if let Some(time) = &attributes.time {
        rest.push_str(&format!(",\"time\":{}", json(time)));
    }
    rest.push_str(&format!(",\"datacontenttype\":{},\"dataschema\":{}", json(DATA_CONTENT_TYPE), json(&attributes.dataschema)));
    if let Some(traceparent) = &attributes.traceparent {
        rest.push_str(&format!(",\"traceparent\":{}", json(traceparent)));
    }
    if let Some(tracestate) = &attributes.tracestate {
        rest.push_str(&format!(",\"tracestate\":{}", json(tracestate)));
    }
    rest.push_str(",\"data\":");
    let fixed = "{\"len\":".len() + rest.len() + data.len() + CRC_SUFFIX;
    let len = (1..=20).map(|d| fixed + d).find(|n| digits(*n) == n - fixed).expect("a length has a width");
    let mut obj = format!("{{\"len\":{len}{rest}").into_bytes();
    obj.extend_from_slice(data);
    let crc = crc32(&obj);
    obj.extend_from_slice(format!(",\"crc32\":\"{crc:08x}\"}}").as_bytes());
    debug_assert_eq!(obj.len(), len);
    obj
}

/// The whole line of one write: `\n` + [`encode_object`] + `\n`.
pub fn encode_line(attributes: &Attributes, data: &[u8]) -> Vec<u8> {
    line_of(&encode_object(attributes, data))
}

/// The line of `obj`: `\n` + the object + `\n`.
fn line_of(obj: &[u8]) -> Vec<u8> {
    let mut line = Vec::with_capacity(obj.len() + 2);
    line.push(b'\n');
    line.extend_from_slice(obj);
    line.push(b'\n');
    line
}

/// An object's fields as a CloudEvents 1.0 event in its JSON form, `len` and `crc32` its two
/// extension attributes.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    len: usize,
    specversion: String,
    id: EventId,
    source: String,
    r#type: String,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    time: Option<String>,
    datacontenttype: String,
    dataschema: String,
    #[serde(default)]
    traceparent: Option<String>,
    #[serde(default)]
    tracestate: Option<String>,
    data: serde_json::Value,
    #[allow(dead_code)]
    crc32: String,
}

/// The line of the object `obj` at byte offset `offset`, or why it is not one: its `len` must be its
/// byte count, its `crc32` must match, and it must parse as a CloudEvents 1.0 event in a
/// `dataschema` `formats` reads.
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
    if env.specversion != SPEC_VERSION || env.datacontenttype != DATA_CONTENT_TYPE {
        return Err(format!("its specversion is {:?} and its datacontenttype {:?}; this build reads {SPEC_VERSION:?} and {DATA_CONTENT_TYPE:?}", env.specversion, env.datacontenttype));
    }
    formats.check(&env.dataschema, &env.data)?;
    let attributes = Attributes { id: env.id, source: env.source, r#type: env.r#type, subject: env.subject, time: env.time, dataschema: env.dataschema, traceparent: env.traceparent, tracestate: env.tracestate };
    Ok(Line { offset: Some(offset), attributes, data: env.data, object: obj.to_vec() })
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

/// What the fold holds of one file: how far it read it, the latest line per (type, subject), and
/// how many lines each `source` wrote.
#[derive(Default)]
struct Fold {
    consumed: u64,
    latest: BTreeMap<(String, String), Line>,
    lines: u64,
    sources: BTreeMap<String, u64>,
}

impl Fold {
    fn take(&mut self, lines: Vec<Line>) {
        for l in lines {
            *self.sources.entry(l.attributes.source.clone()).or_default() += 1;
            self.lines += 1;
            if l.attributes.subject.is_some() {
                self.latest.insert((l.kind().to_string(), l.subject().to_string()), l);
            }
        }
    }
}

/// The append a writer has not proven: its bytes, and where its fold stood before it.
#[derive(Debug, Clone)]
struct Pending {
    /// The write's `id`: what an indeterminate outcome names.
    id: EventId,
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

/// Which read meets the file's unended tail: the open's fold (R-SR3) or a tail read (R-SR4).
#[derive(Clone, Copy)]
enum ReadKind {
    /// The open's fold from byte 0: an unended object that does not verify is refused by name.
    Open,
    /// A tail read, or the raw log: an unended object that does not verify stays unconsumed.
    Tail,
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
    /// One tail read at a time, read and folded as one. A std lock taken only by the blocking call
    /// that reads ([`StoreLog::advance`]), never by an awaiting caller: a caller held where it awaits
    /// that call (a task its node-admin's stop gate holds, stop_gate.rs) keeps no lock another
    /// task's read needs, and the read it began still ends.
    reading: Mutex<()>,
    /// The tail read in progress: the span it was called under, the reader it was called for
    /// ([`StoreLog::advance_for`]), and since when. What a read that waits for it names
    /// ([`Inner::read_waits`]).
    reading_by: Mutex<Option<(&'static str, &'static str, std::time::Instant)>>,
    /// One append at a time, holding this store's pending append.
    append: Arc<tokio::sync::Mutex<Option<Pending>>>,
    /// Shown every line the fold reads, once each, in file order: an index the store folds beside
    /// the latest line per (type, subject).
    observer: Option<Observer>,
    /// Opened to append ([`StoreLog::open_append`]): its open read nothing, and an append's
    /// reconciliation reads from the file's end as it stood before the append, never from a fold.
    appends: bool,
    /// The fold has read the file from byte 0 whole once: at the open of a store opened to fold, at the
    /// first read or append of one opened on its first read. The fold that sets it records
    /// [`OPEN_SPAN`].
    folded: std::sync::atomic::AtomicBool,
    /// The topology storage provider the opener names, recorded on [`OPEN_SPAN`].
    provider: Option<String>,
}

/// What a store folds from each line it reads beside the latest line per (type, subject) ([`StoreLog::open_observed_on_first_read`]).
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

    /// Open (creating) the store file at `path` to append to it, reading `formats` when it is read
    /// later: the open reads no line of it, so a line another writer is still writing, or one this
    /// build cannot read, never refuses it (Q227: a node reads nothing from the store on create, start
    /// or rejoin). The first read ([`Self::latest_of`] and the reads above it) folds the file then.
    pub async fn open_append(path: &Path, formats: Formats) -> Result<Self, StorageError> {
        let file = Arc::new(Self::file(path).await?);
        Ok(Self(Inner::unread(path, formats, file, None, true)))
    }

    /// Open (creating) the store file at `path`, reading `formats`, folded on its first read and never
    /// at its open (rdm#89; Q227: a node-admin reads nothing from the store on create, start or
    /// rejoin): the open reads no line of it, so a line another writer is still writing, or one this
    /// build cannot read, never refuses it. The first read, or the first append, folds the file from
    /// byte 0 as a tail read, on the [`OPEN_SPAN`] span under the span it runs in: a writer's first
    /// append resumes its sequence from the file's own lines (R-SR3) and reconciles from the offset
    /// that fold consumed.
    pub async fn open_on_first_read(path: &Path, formats: Formats) -> Result<Self, StorageError> {
        let file = Arc::new(Self::file(path).await?);
        Ok(Self(Inner::unread(path, formats, file, None, false)))
    }

    /// [`Self::open_on_first_read`], showing `observer` every line the fold reads, once each, in file
    /// order, from the first fold on.
    pub async fn open_observed_on_first_read(path: &Path, formats: Formats, observer: Observer) -> Result<Self, StorageError> {
        let file = Arc::new(Self::file(path).await?);
        Ok(Self(Inner::unread(path, formats, file, Some(observer), false)))
    }

    /// The store file at `path`, opened to read and append, created (and its directory entry
    /// fsynced) when absent.
    async fn file(path: &Path) -> Result<std::fs::File, StorageError> {
        let p = path.to_path_buf();
        blocking_io(move || -> Result<std::fs::File, StorageError> {
            let io = |e: std::io::Error| StorageError::Io { file: p.display().to_string(), reason: e.to_string() };
            let existed = p.try_exists().map_err(io)?;
            let file = std::fs::OpenOptions::new().read(true).append(true).create(true).open(&p).map_err(io)?;
            if !existed {
                let dir = p.parent().unwrap_or(Path::new("."));
                crate::durable_file::sync_dir(dir, &p)?;
            }
            Ok(file)
        })
        .await
    }

    async fn opened(path: &Path, formats: Formats, io: Arc<dyn AppendIo>, observer: Option<Observer>, provider: Option<&str>) -> Result<Self, StorageError> {
        let file = match Self::file(path).await {
            Ok(f) => Arc::new(f),
            Err(e) => {
                let span = tracing::info_span!("rdm.node.store.resolve.via-fold", file = %path.display(), provider = tracing::field::Empty, caller = caller_span(), reader = "open", outcome = "refused", cause = %e);
                if let Some(provider) = provider {
                    span.record("provider", provider);
                }
                return Err(e);
            }
        };
        let mut inner = Inner::unread(path, formats, file, observer, false);
        Arc::get_mut(&mut inner).expect("a store just made is held once").io = io;
        Arc::get_mut(&mut inner).expect("a store just made is held once").provider = provider.map(str::to_string);
        let log = Self(inner);
        log.read(ReadKind::Open, "open").await?;
        Ok(log)
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
    /// one newline alone and fsynced, then read; an unended object that does not verify is another
    /// writer's line still being written and stays unconsumed for the next read; a line that does not
    /// verify refuses the read by name, its offset named, and the fold is left as it was.
    ///
    /// The read and its fold are one blocking call, under the store's one-read-at-a-time lock taken
    /// on that call's thread: the caller holds no lock while it awaits it, so a caller held there (a
    /// task its node-admin's stop gate holds) keeps nothing another read needs, and the read ends and
    /// is folded whether the caller is polled again or not.
    pub async fn advance(&self) -> Result<(), StorageError> {
        self.advance_for("").await
    }

    /// [`StoreLog::advance`] for `reader`, the operation that reads (a Build journal's turn or read,
    /// `current_build`): a read that waits for another names both its own reader and the one it
    /// waits for, beside their callers' spans, so a wait that never ends names what is still open
    /// even where the caller runs under no span.
    pub async fn advance_for(&self, reader: &'static str) -> Result<(), StorageError> {
        self.read(ReadKind::Tail, reader).await
    }

    /// One read of `kind` for `reader` from the fold's consumed offset, one blocking call under the
    /// store's one-read-at-a-time lock ([`StoreLog::advance_for`]).
    async fn read(&self, kind: ReadKind, reader: &'static str) -> Result<(), StorageError> {
        let caller = caller_span();
        let parent = tracing::Span::current();
        #[cfg(feature = "testkit-store")]
        let door = read_door::take(&self.0.path, caller);
        let inner = self.0.clone();
        // The caller's dispatcher, so the spans this read records on the blocking pool are its caller's.
        let dispatch = tracing::dispatcher::get_default(|d| d.clone());
        let unended = blocking_io(move || tracing::dispatcher::with_default(&dispatch, || {
            #[cfg(feature = "testkit-store")]
            if let Some(door) = &door {
                door.admit_wait();
            }
            let _one = match inner.reading.try_lock() {
                Ok(one) => one,
                Err(std::sync::TryLockError::Poisoned(e)) => e.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    inner.read_waits(caller, reader, &parent);
                    inner.reading.lock().unwrap_or_else(|e| e.into_inner())
                }
            };
            let _by = ReadingBy::begin(&inner, caller, reader);
            #[cfg(feature = "testkit-store")]
            if let Some(door) = door {
                door.hold();
            }
            inner.read_held(kind, caller, reader, &parent)
        }))
        .await?;
        if let Some((offset, held)) = unended {
            tracing::info_span!("rdm.node.store.update.via-tail-read-unended", file = %self.0.path.display(), caller, reader, offset, held)
                .in_scope(|| tracing::info!("a line another writer is still writing stays unconsumed for the next read"));
        }
        Ok(())
    }

    /// The latest line of (`type`, `subject`) after reading the tail.
    pub async fn latest(&self, kind: &str, subject: &str) -> Result<Option<Line>, StorageError> {
        self.advance().await?;
        Ok(self.0.fold.lock().unwrap_or_else(|e| e.into_inner()).latest.get(&(kind.to_string(), subject.to_string())).cloned())
    }

    /// The latest line of every subject of the `type` `kind`, in subject order, after reading the tail.
    pub async fn latest_of(&self, kind: &str) -> Result<Vec<Line>, StorageError> {
        self.latest_under(kind, "").await
    }

    /// The latest lines whose subjects begin with `prefix`, in subject order, after reading the tail.
    /// Uses the existing ordered fold; no additional index or persisted copy is maintained.
    pub async fn latest_under(&self, kind: &str, prefix: &str) -> Result<Vec<Line>, StorageError> {
        self.advance().await?;
        Ok(self.0.fold.lock().unwrap_or_else(|e| e.into_inner()).latest
            .range((kind.to_string(), prefix.to_string())..)
            .take_while(|((held_kind, subject), _)| held_kind == kind && subject.starts_with(prefix))
            .map(|(_, line)| line.clone()).collect())
    }

    /// Every line of the file, in file order, read whole from byte 0 (the raw log).
    pub async fn lines(&self) -> Result<Vec<Line>, StorageError> {
        let inner = self.0.clone();
        let (lines, _, _) = blocking_io(move || inner.read_whole(0, ReadKind::Tail)).await?;
        Ok(lines)
    }

    /// How many lines each `source` wrote, of the lines the fold has read.
    pub fn sources(&self) -> BTreeMap<String, u64> {
        self.0.fold.lock().unwrap_or_else(|e| e.into_inner()).sources.clone()
    }

    /// Append `record` as one line of `type` in `dataschema` at `subject`, written by `writer` (R-SR3,
    /// Q365): its `id` minted and its `time` the writer's rafka-time; this store's pending append is
    /// reconciled first, then the line is written in one `write` and fsynced, and success is answered
    /// only after the fsync.
    pub async fn append<T: Serialize + ?Sized>(&self, writer: &LogWriter, r#type: &str, dataschema: &str, subject: &str, record: &T) -> Result<(), StorageError> {
        self.append_with(Attributes::of(writer, r#type, dataschema, subject), record).await
    }

    /// Append `record` as one line under `attributes`, as [`Self::append`] writes one: an event's
    /// line, its attributes the event's own.
    pub async fn append_with<T: Serialize + ?Sized>(&self, attributes: Attributes, record: &T) -> Result<(), StorageError> {
        let file = self.0.path.display().to_string();
        let bytes = serde_json::to_vec(record).map_err(|e| StorageError::Io { file: file.clone(), reason: format!("the {} record at {:?} does not encode: {e}", attributes.dataschema, attributes.subject) })?;
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("encoded JSON parses");
        self.0.formats.check(&attributes.dataschema, &value).map_err(|why| StorageError::Unrecognised { file, reason: format!("a write refused: {why}") })?;
        let subject = attributes.subject.clone().unwrap_or_default();
        self.append_object(attributes.id.clone(), subject, encode_object(&attributes, &bytes)).await
    }

    /// Append `line` as its writer wrote it, byte for byte, its `id` kept (storage.md §2 Copy: the old
    /// provider's lines appended to the new one, every writer's included): this store's pending append
    /// is reconciled first, then the line is written and fsynced as [`Self::append`] writes one.
    pub async fn append_line(&self, line: &Line) -> Result<(), StorageError> {
        let file = self.0.path.display().to_string();
        self.0.formats.check(&line.attributes.dataschema, &line.data).map_err(|why| StorageError::Unrecognised { file, reason: format!("a write refused: {why}") })?;
        self.append_object(line.attributes.id.clone(), line.subject().to_string(), line.object.clone()).await
    }

    /// Append the object `obj` of the write `id` at `subject`, under this store's one-append-at-a-time
    /// lock, its pending append reconciled first.
    async fn append_object(&self, id: EventId, subject: String, obj: Vec<u8>) -> Result<(), StorageError> {
        let (caller, parent) = (caller_span(), tracing::Span::current());
        let guard = self.0.append.clone().lock_owned().await;
        let inner = self.0.clone();
        let dispatch = tracing::dispatcher::get_default(|d| d.clone());
        blocking_io(move || {
            let mut guard = guard;
            #[cfg(feature = "testkit-store")]
            let door = write_door::take(&inner.path, &subject);
            #[cfg(not(feature = "testkit-store"))]
            let _ = &subject;
            #[cfg(feature = "testkit-store")]
            if let Some(door) = door.as_ref().filter(|d| !d.after()) {
                tracing::dispatcher::with_default(&dispatch, || door.hold());
            }
            tracing::dispatcher::with_default(&dispatch, || inner.fold_before_append(caller, &parent))?;
            let appended = inner.append_new(&mut *guard, id, obj);
            #[cfg(feature = "testkit-store")]
            if let Some(door) = door.as_ref().filter(|d| d.after() && appended.is_ok()) {
                tracing::dispatcher::with_default(&dispatch, || door.hold());
            }
            appended
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
}

/// The name of the span the caller runs under, `""` under none: what a tail read is named by.
fn caller_span() -> &'static str {
    tracing::Span::current().metadata().map(|m| m.name()).unwrap_or("")
}

/// The tail read in progress on a file, named while it runs.
struct ReadingBy<'a>(&'a Inner);

impl<'a> ReadingBy<'a> {
    fn begin(inner: &'a Inner, caller: &'static str, reader: &'static str) -> Self {
        *inner.reading_by.lock().unwrap_or_else(|e| e.into_inner()) = Some((caller, reader, std::time::Instant::now()));
        Self(inner)
    }
}

impl Drop for ReadingBy<'_> {
    fn drop(&mut self) {
        *self.0.reading_by.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

impl Inner {
    /// A tail read called under `caller` for `reader` waits for the one in progress on this file:
    /// named on its own span when the wait begins, so a wait that never ends still names the read it
    /// waits for (`held_by`, its caller's span; `held_reader`, its reader).
    fn read_waits(&self, caller: &'static str, reader: &'static str, parent: &tracing::Span) {
        let (held_by, held_reader, held_ms) = match *self.reading_by.lock().unwrap_or_else(|e| e.into_inner()) {
            Some((by, held_reader, since)) => (by, held_reader, since.elapsed().as_millis() as u64),
            None => ("", "", 0),
        };
        tracing::info_span!(parent: parent, "rdm.node.store.update.via-tail-read-waiting", file = %self.path.display(), caller, reader, held_by, held_reader, held_ms)
            .in_scope(|| tracing::info!("this tail read waits for the one in progress on the same file"));
    }

    /// A store whose fold has read nothing yet, on the open `file`.
    fn unread(path: &Path, formats: Formats, file: Arc<std::fs::File>, observer: Option<Observer>, appends: bool) -> Arc<Self> {
        Arc::new(Self {
            path: path.to_path_buf(),
            formats,
            file,
            io: Arc::new(DiskIo),
            fold: Mutex::new(Fold::default()),
            reading: Mutex::new(()),
            reading_by: Mutex::new(None),
            append: Arc::new(tokio::sync::Mutex::new(None)),
            observer,
            appends,
            folded: std::sync::atomic::AtomicBool::new(false),
            provider: None,
        })
    }

    /// One read of `kind` for `reader`, called under `caller`, holding the read lock ([`Self::read_tail`]).
    /// The first, from byte 0, is the store's fold of the whole file, recorded on [`OPEN_SPAN`] under
    /// `parent`: a refused one leaves the fold as it was, and the next read folds from byte 0 again.
    fn read_held(&self, kind: ReadKind, caller: &'static str, reader: &'static str, parent: &tracing::Span) -> Result<Option<(u64, u64)>, StorageError> {
        if self.folded.load(std::sync::atomic::Ordering::Acquire) {
            return self.read_tail(kind);
        }
        let span = tracing::info_span!(parent: parent, "rdm.node.store.resolve.via-fold", file = %self.path.display(), provider = tracing::field::Empty, caller, reader, bytes = tracing::field::Empty, lines = tracing::field::Empty, sources = tracing::field::Empty, duration = tracing::field::Empty, outcome = tracing::field::Empty, invalid_offset = tracing::field::Empty, cause = tracing::field::Empty);
        if let Some(provider) = &self.provider {
            span.record("provider", provider.as_str());
        }
        let began = std::time::Instant::now();
        let read = self.read_tail(kind);
        span.record("duration", began.elapsed().as_millis() as u64);
        match &read {
            Ok(_) => {
                self.folded.store(true, std::sync::atomic::Ordering::Release);
                let f = self.fold.lock().unwrap_or_else(|e| e.into_inner());
                let sources: Vec<String> = f.sources.iter().map(|(source, lines)| format!("{source}:{lines}")).collect();
                span.record("bytes", f.consumed).record("lines", f.lines).record("sources", sources.join(",")).record("outcome", "folded");
            }
            Err(e) => {
                span.record("outcome", "refused").record("cause", e.to_string());
                if let StorageError::Unrecognised { reason, .. } = e {
                    if let Some(o) = reason.split("byte offset ").nth(1).and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next()) {
                        span.record("invalid_offset", o);
                    }
                }
            }
        }
        read
    }

    /// Before the first append to a store opened on its first read whose fold has read nothing, fold
    /// the file from byte 0 as a tail read, holding the read lock: its pending append reconciles from
    /// the offset this fold consumed. A store opened to append ([`StoreLog::open_append`]) reads nothing.
    fn fold_before_append(&self, caller: &'static str, parent: &tracing::Span) -> Result<(), StorageError> {
        if self.appends || self.folded.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        let _one = self.reading.lock().unwrap_or_else(|e| e.into_inner());
        let _by = ReadingBy::begin(self, caller, "append");
        if let Some((offset, held)) = self.read_held(ReadKind::Tail, caller, "append", parent)? {
            tracing::info_span!(parent: parent, "rdm.node.store.update.via-tail-read-unended", file = %self.path.display(), caller, reader = "append", offset, held)
                .in_scope(|| tracing::info!("a line another writer is still writing stays unconsumed for the next read"));
        }
        Ok(())
    }

    /// One read of `kind` (the open's fold or a tail read), holding the read lock: the lines from the
    /// fold's consumed offset to the end, shown to the observer in file order and folded, the offset
    /// moved past them. Answers the offset and held bytes of an unended object a tail read left
    /// unconsumed.
    fn read_tail(&self, kind: ReadKind) -> Result<Option<(u64, u64)>, StorageError> {
        let from = self.fold.lock().unwrap_or_else(|e| e.into_inner()).consumed;
        let (lines, consumed, unended) = self.read_whole(from, kind)?;
        if let Some(observe) = &self.observer {
            for l in &lines {
                observe(l);
            }
        }
        let mut fold = self.fold.lock().unwrap_or_else(|e| e.into_inner());
        fold.take(lines);
        fold.consumed = consumed;
        Ok(unended)
    }

    fn named(&self, offset: u64, why: impl std::fmt::Display) -> StorageError {
        StorageError::Unrecognised { file: self.path.display().to_string(), reason: format!("the line at byte offset {offset} is malformed: {why}; nothing older is answered as current and the file is left as it is") }
    }

    fn io(&self, e: std::io::Error) -> StorageError {
        StorageError::Io { file: self.path.display().to_string(), reason: e.to_string() }
    }

    /// Read the lines from `from` to the end whole: a complete valid object with no final newline is
    /// ended with one newline alone and fsynced, then read again; an unended object that does not
    /// verify is another writer's line still being written: a tail read leaves it unconsumed and an
    /// open refuses it by name (R-SR3, R-SR4). Answers the lines, the offset they were read whole to
    /// and, when a tail read left an unended object unconsumed, its offset and the bytes held of it.
    fn read_whole(&self, from: u64, kind: ReadKind) -> Result<(Vec<Line>, u64, Option<(u64, u64)>), StorageError> {
        let mut ended = false;
        loop {
            let bytes = read_from(&self.file, from).map_err(|e| self.io(e))?;
            let s = scan(&bytes, from, &self.formats).map_err(|(at, why)| self.named(at, why))?;
            let Some((at, tail)) = s.tail else { return Ok((s.lines, s.consumed, None)) };
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
                    return match kind {
                        ReadKind::Tail => Ok((s.lines, s.consumed, Some((at, tail.len() as u64)))),
                        ReadKind::Open => Err(self.named(at, format!("an unended object: {why}"))),
                    };
                }
            }
        }
    }

    /// Append the object `obj` of the write `id`, this store's pending append settled first.
    fn append_new(&self, pending: &mut Option<Pending>, id: EventId, obj: Vec<u8>) -> Result<(), StorageError> {
        self.settle(pending)?;
        // A store opened to append reconciles from the file's end before this write (O_APPEND puts the
        // line at or past it), reading no line another writer wrote before it.
        let from = if self.appends { self.file.metadata().map_err(|e| self.io(e))?.len() } else { self.fold.lock().unwrap_or_else(|e| e.into_inner()).consumed };
        *pending = Some(Pending { id, obj, from, reported: 0 });
        self.write_pending(pending)
    }

    /// Write the pending append's line in one `write`, then fsync: done only when both answered in
    /// full. A write that reported zero bytes is retried under the same `id` once the lines after
    /// the fold's offset validate; a short write is reconciled at once; a failed fsync stays pending.
    fn write_pending(&self, pending: &mut Option<Pending>) -> Result<(), StorageError> {
        let mut zero = 0;
        loop {
            let (line, id) = {
                let p = pending.as_ref().expect("a pending append");
                (line_of(&p.obj), p.id.clone())
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
                        Err(e) => Err(StorageError::Io { file: self.path.display().to_string(), reason: format!("indeterminate: the line of id {id} was written and its fsync failed ({e}); it is reconciled by its object before this store's next write") }),
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
                        return Err(StorageError::Io { file: self.path.display().to_string(), reason: format!("the line of id {id} was not written: {why}, {zero} times; no byte of it is in the file") });
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
            Reconciled::Absent => Err(StorageError::Io { file: self.path.display().to_string(), reason: format!("indeterminate: the write of id {} reported {} bytes and no byte of its object is in the file after byte offset {}; nothing is written in its place", p.id, p.reported, p.from) }),
        }
    }

    /// Find the pending append by its object (its `id` makes it unique) from the fold's offset before
    /// its write, every other line between validated.
    fn reconcile(&self, p: &Pending) -> Result<Reconciled, StorageError> {
        let bytes = read_from(&self.file, p.from).map_err(|e| self.io(e))?;
        let refused = |at: u64, why: String| StorageError::Io { file: self.path.display().to_string(), reason: format!("indeterminate: the write of id {} cannot be reconciled: {why} at byte offset {at}; nothing is appended in its place and no byte is discarded", p.id) };
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
                self.io.sync(&self.file).map_err(|e| StorageError::Io { file: self.path.display().to_string(), reason: format!("indeterminate: the line of id {} stands whole and its fsync failed ({e})", p.id) })?;
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



/// The testkit's door on a store file's tail read (feature `testkit-store`, for rdm-test; absent from
/// production builds): armed for a file and the span a read is called under, it holds the first such
/// read on its blocking call, before the file's read until the cell admits it, then inside the read
/// itself until the cell releases it. Every other read of the file passes.
#[cfg(feature = "testkit-store")]
pub mod read_door {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Condvar, Mutex, OnceLock};

    /// One armed door.
    pub struct Door {
        file: PathBuf,
        under: &'static str,
        at: Mutex<Opened>,
        changed: Condvar,
        entered: tokio::sync::watch::Sender<bool>,
    }

    /// How far the cell has opened a door.
    #[derive(Default)]
    struct Opened {
        /// The held read may take the file's read ([`Door::admit`]).
        admitted: bool,
        /// The held read may end ([`Door::release`]).
        released: bool,
    }

    fn armed() -> &'static Mutex<Vec<Arc<Door>>> {
        static ARMED: OnceLock<Mutex<Vec<Arc<Door>>>> = OnceLock::new();
        ARMED.get_or_init(Default::default)
    }

    /// Hold the first tail read of a file whose path ends with `file` that is called under a span
    /// named `under`: before it takes the file's read until [`Door::admit`], so no other read of the
    /// file waits behind it until then, then inside the read until [`Door::release`].
    pub fn arm(file: &Path, under: &'static str) -> Arc<Door> {
        let door = Arc::new(Door { file: file.to_path_buf(), under, at: Mutex::new(Opened::default()), changed: Condvar::new(), entered: tokio::sync::watch::Sender::new(false) });
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
        /// On the read's blocking call, before it takes the file's read: wait for the admission.
        pub(crate) fn admit_wait(&self) {
            let mut at = self.at.lock().unwrap();
            if !at.admitted {
                tracing::info_span!("rdm.testkit.fault.update.via-hold", cut = "store-tail-read-entry", detail = %format!("{} under {}", self.file.display(), self.under))
                    .in_scope(|| tracing::info!("the testkit holds this tail read before it takes the file's read until the cell admits it"));
            }
            while !at.admitted {
                at = self.changed.wait(at).unwrap();
            }
        }

        /// On the read's blocking call, inside the read: say it entered, then wait for the release.
        pub(crate) fn hold(&self) {
            tracing::info_span!("rdm.testkit.fault.update.via-hold", cut = "store-tail-read", detail = %format!("{} under {}", self.file.display(), self.under))
                .in_scope(|| tracing::info!("the testkit holds this tail read inside the read until the cell releases it"));
            self.entered.send_replace(true);
            let mut at = self.at.lock().unwrap();
            while !at.released {
                at = self.changed.wait(at).unwrap();
            }
        }

        /// Resolves once the read it holds is inside it.
        pub async fn entered(&self) {
            let _ = self.entered.subscribe().wait_for(|e| *e).await;
        }

        /// Let the held read take the file's read, where it is held until [`Door::release`].
        pub fn admit(&self) {
            self.at.lock().unwrap().admitted = true;
            self.changed.notify_all();
        }

        /// Let the held read go on, admitted if it was not.
        pub fn release(&self) {
            let mut at = self.at.lock().unwrap();
            (at.admitted, at.released) = (true, true);
            self.changed.notify_all();
        }
    }
}

/// A test-only hold of one store append, while its ordering lock is owned by the disk job: before
/// the line is written ([`write_door::arm`], a provider completion delayed), or after it is written
/// and fsynced and before its caller is answered ([`write_door::arm_after`], the persistence boundary
/// a process is terminated at). Dropping the caller does not release this hold or the append lock.
#[cfg(feature = "testkit-store")]
pub mod write_door {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Condvar, Mutex, OnceLock};

    struct Held {
        file: PathBuf,
        subject: String,
        /// Held after the durable append, not before it.
        after: bool,
        released: Mutex<bool>,
        changed: Condvar,
        entered: tokio::sync::watch::Sender<bool>,
    }

    /// A one-shot fault handle; dropping it releases the blocked append, even on test failure.
    pub struct Door(Arc<Held>);

    fn armed() -> &'static Mutex<Vec<Arc<Held>>> {
        static ARMED: OnceLock<Mutex<Vec<Arc<Held>>>> = OnceLock::new();
        ARMED.get_or_init(Default::default)
    }

    /// Hold the first append to this exact file and subject before it is written. Other files and subjects
    /// are unaffected.
    pub fn arm(file: &Path, subject: &str) -> Door {
        armed_at(file, subject, false)
    }

    /// Hold the first append to this exact file and subject once it is written and fsynced, before its
    /// caller is answered: a process terminated while it is held has persisted the line and answered
    /// nothing.
    pub fn arm_after(file: &Path, subject: &str) -> Door {
        armed_at(file, subject, true)
    }

    fn armed_at(file: &Path, subject: &str, after: bool) -> Door {
        let held = Arc::new(Held {
            file: file.to_path_buf(), subject: subject.into(), after, released: Mutex::new(false),
            changed: Condvar::new(), entered: tokio::sync::watch::Sender::new(false),
        });
        armed().lock().unwrap().push(held.clone());
        Door(held)
    }

    pub(super) fn take(file: &Path, subject: &str) -> Option<Door> {
        let mut all = armed().lock().unwrap();
        let at = all.iter().position(|d| d.file == file && d.subject == subject)?;
        Some(Door(all.remove(at)))
    }

    impl Door {
        pub(super) fn after(&self) -> bool {
            self.0.after
        }

        pub(super) fn hold(&self) {
            let cut = if self.0.after { "store-appended" } else { "store-append" };
            tracing::info_span!("rdm.testkit.fault.update.via-hold", cut, file = %self.0.file.display(), subject = %self.0.subject)
                .in_scope(|| tracing::info!("the append owns its ordering lock before the caller can be cancelled"));
            self.0.entered.send_replace(true);
            let mut released = self.0.released.lock().unwrap();
            while !*released {
                released = self.0.changed.wait(released).unwrap();
            }
        }

        /// Whether the disk job holds the append now: entered and not released.
        pub fn holds(&self) -> bool {
            *self.0.entered.borrow() && !*self.0.released.lock().unwrap()
        }

        /// Wait until the disk job holds the append. Tests must supply their own wait deadline.
        pub async fn entered(&self) {
            let _ = self.0.entered.subscribe().wait_for(|v| *v).await;
        }

        /// Release this append, or disarm the fault if no append entered it.
        pub fn release(&self) {
            *self.0.released.lock().unwrap() = true;
            self.0.changed.notify_all();
            armed().lock().unwrap().retain(|d| !Arc::ptr_eq(d, &self.0));
        }
    }

    impl Drop for Door {
        fn drop(&mut self) { self.release(); }
    }
}

#[cfg(test)]
mod tests;
