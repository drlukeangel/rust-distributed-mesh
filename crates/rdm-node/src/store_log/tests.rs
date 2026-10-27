//! The store file's cells (proposal `store-root-one-directory-per-entity.md` §5.4, R-SR3 and R-SR4).

use super::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Rec {
    name: String,
    n: u64,
}

fn rec(name: &str, n: u64) -> Rec {
    Rec { name: name.into(), n }
}

fn formats() -> Formats {
    Formats::new().with::<Rec>("rec/1")
}

/// The writer `tag` names, one per tag for the whole binary.
fn writer(tag: &str) -> LogWriter {
    static WRITERS: Mutex<BTreeMap<String, LogWriter>> = Mutex::new(BTreeMap::new());
    WRITERS.lock().unwrap().entry(tag.to_string()).or_insert_with(LogWriter::mint).clone()
}

fn op_of(w: &LogWriter, seq: u64) -> Op {
    Op { node_id: w.node_id.to_string(), incarnation_id: w.incarnation_id.to_string(), seq }
}

fn file(tag: &str) -> PathBuf {
    crate::record_store::tempdir(tag).join("nodes.jsonl")
}

/// Another process's append of `record` at `key` as `w` with `seq`: one write on its own `O_APPEND`
/// descriptor.
fn other_appends(path: &Path, w: &LogWriter, seq: u64, key: &str, record: &Rec) {
    use std::io::Write as _;
    let op = op_of(w, seq);
    let line = line_of(&encode_object(&op, "rec/1", key, &serde_json::to_vec(record).unwrap()));
    std::fs::OpenOptions::new().append(true).open(path).unwrap().write_all(&line).unwrap();
}

fn append_raw(path: &Path, bytes: &[u8]) {
    use std::io::Write as _;
    std::fs::OpenOptions::new().append(true).create(true).open(path).unwrap().write_all(bytes).unwrap();
}

/// What a faulted write does.
enum WriteFault {
    /// Write all but the last `drop` bytes and report them, then run `then` (another writer's turn).
    Short { drop: usize, then: Option<Box<dyn FnOnce() + Send>> },
    /// Write nothing and report zero bytes.
    Zero,
}

/// A file whose next writes and fsyncs fail as scripted, the rest made on the disk.
#[derive(Default)]
struct Faulted {
    writes: Mutex<VecDeque<WriteFault>>,
    syncs: Mutex<VecDeque<()>>,
    written: Mutex<Vec<Vec<u8>>>,
    synced: AtomicUsize,
}

impl AppendIo for Faulted {
    fn write(&self, file: &std::fs::File, bytes: &[u8]) -> std::io::Result<usize> {
        self.written.lock().unwrap().push(bytes.to_vec());
        let fault = self.writes.lock().unwrap().pop_front();
        match fault {
            None => DiskIo.write(file, bytes),
            Some(WriteFault::Zero) => Ok(0),
            Some(WriteFault::Short { drop, then }) => {
                let n = DiskIo.write(file, &bytes[..bytes.len() - drop])?;
                if let Some(then) = then {
                    then();
                }
                Ok(n)
            }
        }
    }
    fn sync(&self, file: &std::fs::File) -> std::io::Result<()> {
        if self.syncs.lock().unwrap().pop_front().is_some() {
            return Err(std::io::Error::other("injected fsync failure"));
        }
        self.synced.fetch_add(1, Ordering::SeqCst);
        file.sync_all()
    }
}

async fn reopen(path: &Path) -> Result<StoreLog, StorageError> {
    StoreLog::open(path, formats()).await
}

async fn current(log: &StoreLog, key: &str) -> Option<Rec> {
    log.latest("rec", key).await.unwrap().map(|l| l.decode(log.path()).unwrap())
}

/// CONTRACT (R-SR3, the line's bytes): an object's `len` is its own byte count between its newlines,
/// its first field, and its `crc32` the CRC-32 of its bytes before the final `,"crc32":`, its last;
/// the object is ordinary JSON. Must NOT happen: a `len` that misstates the object, or a checksum over
/// other bytes. The fixture pins the bytes for ever.
#[test]
fn a_line_states_its_own_length_first_and_its_checksum_last() {
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926, "CRC-32 (IEEE) check value");
    let op = Op { node_id: "n".into(), incarnation_id: "i".into(), seq: 1 };
    let obj = encode_object(&op, "rec/1", "k", br#"{"name":"a","n":1}"#);
    assert_eq!(String::from_utf8(obj.clone()).unwrap(), r#"{"len":135,"op":{"node_id":"n","incarnation_id":"i","seq":1},"format":"rec/1","key":"k","record":{"name":"a","n":1},"crc32":"ead78455"}"#);
    assert_eq!(obj.len(), 135);
    assert_eq!(crc32(&obj[..obj.len() - 20]), 0xead7_8455, "the checksum covers the bytes before the final ,\"crc32\":");
    let v: serde_json::Value = serde_json::from_slice(&obj).unwrap();
    assert_eq!(v["len"], 135);
    for n in 0..400 {
        let obj = encode_object(&op, "rec/1", &"k".repeat(n), b"{}");
        let line = verify(&obj, 0, &Formats::new().with::<serde_json::Value>("rec/1")).unwrap_or_else(|e| panic!("{n}: {e}"));
        assert_eq!(line.key.len(), n);
    }
}

/// CONTRACT (R-SR3: one `write` on a descriptor opened `O_APPEND`; R-SR1: one file per store, many
/// writers): two writers, each on its own descriptor, append to one file at once; every line is found
/// whole, and each writer's lines stand in the order it wrote them. Must NOT happen: two writes
/// interleaved inside one line, a lost line, or a writer's lines out of its order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_writers_appending_at_once_leave_every_line_whole_and_in_order() {
    let path = file("store-log-two-writers");
    let (a, b) = (reopen(&path).await.unwrap(), reopen(&path).await.unwrap());
    let run = |log: StoreLog, w: LogWriter| {
        tokio::spawn(async move {
            for n in 0..200u64 {
                log.append(&w, "rec/1", &format!("{}-{n}", w.node_id), &rec(&"x".repeat((n % 50) as usize), n)).await.unwrap();
            }
        })
    };
    let (ja, jb) = (run(a, writer("a")), run(b, writer("b")));
    ja.await.unwrap();
    jb.await.unwrap();
    let lines = reopen(&path).await.unwrap().lines().await.unwrap();
    assert_eq!(lines.len(), 400, "every line found whole");
    for w in [writer("a").node_id.to_string(), writer("b").node_id.to_string()] {
        let seqs: Vec<u64> = lines.iter().filter_map(|l| l.op.as_ref()).filter(|o| o.node_id == w).map(|o| o.seq).collect();
        assert_eq!(seqs, (1..=200).collect::<Vec<_>>(), "{w}'s lines stand in the order it wrote them");
    }
}

/// CONTRACT (R-SR3: "A malformed line, including the last line, refuses the open or tail read by name
/// with the file and offset; no partial answer or older state is returned as current"): a reader that
/// folded a good line; a partial object follows it, and another writer's valid line after that ends
/// it. The reader's tail read is refused naming the file and the partial's offset, and it answers
/// nothing, neither the older line nor the newer one. Must NOT happen: the read answering the state
/// before the partial as current.
#[tokio::test]
async fn a_partial_object_followed_by_another_writers_line_refuses_the_read() {
    let path = file("store-log-partial-then-valid");
    let log = reopen(&path).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("first", 1)).await.unwrap();
    assert_eq!(current(&log, "k").await, Some(rec("first", 1)));
    let at = std::fs::metadata(&path).unwrap().len() + 1;
    let partial = line_of(&encode_object(&op_of(&writer("a"), 2), "rec/1", "k", br#"{"name":"second","n":2}"#));
    append_raw(&path, &partial[..partial.len() - 30]);
    other_appends(&path, &writer("b"), 1, "k", &rec("third", 3));
    let e = log.latest("rec", "k").await.unwrap_err().to_string();
    assert!(e.contains(&path.display().to_string()) && e.contains(&format!("byte offset {at}")), "refused by name with the file and the offset: {e}");
    assert!(log.latest_of("rec").await.is_err(), "no older or partial view is answered");
    assert!(reopen(&path).await.is_err(), "an open is refused the same way");
}

/// CONTRACT (R-SR3, the short-write boundary): a write that reported all but its final newline is
/// indeterminate and reconciled at once by its object, under both schedules: (a) another writer's
/// line lands first, its opening newline ending the object, and the reconciliation finds it whole and
/// only fsyncs; (b) the reconciliation comes first and appends one newline alone. In each the record
/// is folded once and success is answered only after the fsync. Must NOT happen: the payload written
/// again, or success answered before an fsync.
#[tokio::test]
async fn a_write_short_of_its_newline_is_finished_and_folded_once_under_either_schedule() {
    for other_first in [true, false] {
        let path = file("store-log-short-newline");
        let faulted = Arc::new(Faulted::default());
        let then: Option<Box<dyn FnOnce() + Send>> = other_first.then(|| {
            let p = path.clone();
            Box::new(move || other_appends(&p, &writer("b"), 1, "other", &rec("b", 1))) as Box<dyn FnOnce() + Send>
        });
        faulted.writes.lock().unwrap().push_back(WriteFault::Short { drop: 1, then });
        let log = StoreLog::open_with(&path, formats(), faulted.clone()).await.unwrap();
        let synced = faulted.synced.load(Ordering::SeqCst);
        log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.expect("the short write is reconciled to success");
        assert!(faulted.synced.load(Ordering::SeqCst) > synced, "success follows an fsync");
        let written = faulted.written.lock().unwrap().clone();
        assert_eq!(written.len(), if other_first { 1 } else { 2 }, "the payload is never written again ({other_first})");
        if !other_first {
            assert_eq!(written[1], b"\n", "the reconciliation appends one newline alone");
        }
        let lines = reopen(&path).await.unwrap().lines().await.unwrap();
        assert_eq!(lines.iter().filter(|l| l.key == "k").count(), 1, "the record is folded once ({other_first})");
        assert_eq!(lines.len(), if other_first { 2 } else { 1 });
    }
}

/// CONTRACT (R-SR3: every nonblank line must match its `crc32`): a line that parses as JSON with a
/// wrong `crc32` refuses the open by name, naming the file and the line's offset. Must NOT happen: the
/// line read as a value, or the open answering the lines before it.
#[tokio::test]
async fn a_line_with_a_wrong_checksum_refuses_the_open_by_name() {
    let path = file("store-log-crc");
    let log = reopen(&path).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    let at = bytes.len() as u64 + 1;
    let mut obj = encode_object(&op_of(&writer("a"), 2), "rec/1", "k", br#"{"name":"b","n":2}"#);
    let n = obj.len();
    obj[n - 3] = if obj[n - 3] == b'0' { b'1' } else { b'0' };
    assert!(serde_json::from_slice::<serde_json::Value>(&obj).is_ok(), "the damaged line still parses");
    bytes.extend(line_of(&obj));
    std::fs::write(&path, &bytes).unwrap();
    let e = reopen(&path).await.unwrap_err().to_string();
    assert!(e.contains(&path.display().to_string()) && e.contains(&format!("byte offset {at}")) && e.contains("crc32"), "{e}");
}

/// CONTRACT (R-SR3: "A fold skips blank lines only"): blank lines between lines are skipped and every
/// line around them is folded. Must NOT happen: a blank line refusing the fold.
#[tokio::test]
async fn a_blank_line_is_skipped() {
    let path = file("store-log-blank");
    let log = reopen(&path).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap();
    append_raw(&path, b"\n\n\n");
    log.append(&writer("a"), "rec/1", "j", &rec("b", 2)).await.unwrap();
    let reopened = reopen(&path).await.unwrap();
    assert_eq!((current(&reopened, "k").await, current(&reopened, "j").await), (Some(rec("a", 1)), Some(rec("b", 2))));
}

/// CONTRACT (R-SR2: every write appends the whole record; nothing is ever removed; the latest complete
/// line for a key is the current state): several writes of one key all stand as lines after a reopen,
/// and the latest is read as current. Must NOT happen: an earlier write replaced or removed.
#[tokio::test]
async fn several_writes_of_one_key_all_stand_and_the_latest_is_current() {
    let path = file("store-log-one-key");
    let log = reopen(&path).await.unwrap();
    for n in 1..=5 {
        log.append(&writer("a"), "rec/1", "k", &rec("a", n)).await.unwrap();
    }
    let reopened = reopen(&path).await.unwrap();
    let all: Vec<u64> = reopened.lines().await.unwrap().iter().map(|l| l.decode::<Rec>(&path).unwrap().n).collect();
    assert_eq!(all, vec![1, 2, 3, 4, 5], "every write stands as a line");
    assert_eq!(current(&reopened, "k").await, Some(rec("a", 5)), "the latest line is current");
}

/// CONTRACT (R-SR4, Architect 1 2026-10-25: "after A consumes to 100, B appends and A appends, A's next
/// fold starts at 100 and reads B's line and then its own"): a writer's own append does not move its
/// consumed offset, so its next tail read reads the other writer's line and its own, in file order.
/// Must NOT happen: A's offset jumping past B's line to where its own write landed.
#[tokio::test]
async fn a_writers_own_append_does_not_move_its_consumed_offset() {
    let path = file("store-log-aba");
    let (a, b) = (reopen(&path).await.unwrap(), reopen(&path).await.unwrap());
    a.append(&writer("a"), "rec/1", "first", &rec("a", 1)).await.unwrap();
    a.advance().await.unwrap();
    let consumed = a.consumed();
    b.append(&writer("b"), "rec/1", "k", &rec("b", 1)).await.unwrap();
    a.append(&writer("a"), "rec/1", "k", &rec("a", 2)).await.unwrap();
    assert_eq!(a.consumed(), consumed, "A's own append leaves its offset where its fold stood");
    assert_eq!(current(&a, "k").await, Some(rec("a", 2)), "A's next fold reads B's line, then its own: its own is the latest");
    assert_eq!(current(&a, "first").await, Some(rec("a", 1)));
    assert_eq!(a.consumed(), std::fs::metadata(&path).unwrap().len(), "the fold read every line whole");
}

/// CONTRACT (R-SR3: a malformed line, including the last, refuses the open; "no partial answer or older
/// state is returned as current"; nothing is discarded): a previously acknowledged record damaged so
/// its JSON no longer parses, and another with only its `len` corrupted upward, each as the last record
/// and as an interior record: the reopen is refused by name, its predecessor is not answered, and the
/// file's bytes are unchanged. Must NOT happen: the damaged line skipped, truncated, or the line before
/// it answered as current.
#[tokio::test]
async fn a_damaged_acknowledged_record_refuses_the_reopen_and_leaves_the_bytes() {
    for (damage, last) in [("json", true), ("json", false), ("len", true), ("len", false)] {
        let path = file("store-log-damaged");
        let log = reopen(&path).await.unwrap();
        log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap();
        let at = std::fs::metadata(&path).unwrap().len() + 1;
        log.append(&writer("a"), "rec/1", "k", &rec("b", 2)).await.unwrap();
        if !last {
            log.append(&writer("a"), "rec/1", "j", &rec("c", 3)).await.unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        let i = at as usize;
        match damage {
            // `{"len":` → `{"len"x`: the JSON no longer parses.
            "json" => bytes[i + 6] = b'x',
            // the len's last digit one higher.
            _ => {
                let end = i + 7 + bytes[i + 7..].iter().position(|b| !b.is_ascii_digit()).unwrap() - 1;
                bytes[end] += 1;
            }
        }
        std::fs::write(&path, &bytes).unwrap();
        let e = reopen(&path).await.unwrap_err().to_string();
        assert!(e.contains(&path.display().to_string()) && e.contains(&format!("byte offset {at}")), "{damage} last={last}: {e}");
        assert_eq!(std::fs::read(&path).unwrap(), bytes, "{damage} last={last}: the file's bytes are unchanged");
        assert!(log.latest("rec", "k").await.is_err(), "{damage} last={last}: the predecessor is not answered");
    }
}

/// CONTRACT (R-SR3: "a complete object with valid length, format and checksum but no final newline is
/// retained: append one newline alone and fsync before completing the open"): such a last object is
/// kept, ended with one newline, fsynced, and folded once; a later write lands after it. Must NOT
/// happen: the object discarded, folded twice, or the open answering before the fsync.
#[tokio::test]
async fn a_valid_last_object_missing_only_its_newline_is_ended_at_open_and_folded_once() {
    let path = file("store-log-unended");
    let log = reopen(&path).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap();
    let line = line_of(&encode_object(&op_of(&writer("a"), 2), "rec/1", "k", br#"{"name":"b","n":2}"#));
    append_raw(&path, &line[..line.len() - 1]);
    let faulted = Arc::new(Faulted::default());
    let reopened = StoreLog::open_with(&path, formats(), faulted.clone()).await.unwrap();
    assert_eq!(faulted.written.lock().unwrap().clone(), vec![b"\n".to_vec()], "one newline alone ends it");
    assert_eq!(faulted.synced.load(Ordering::SeqCst), 1, "fsynced before the open completed");
    assert_eq!(current(&reopened, "k").await, Some(rec("b", 2)), "the object is kept and current");
    reopened.append(&writer("a"), "rec/1", "j", &rec("c", 3)).await.unwrap();
    let lines = reopen(&path).await.unwrap().lines().await.unwrap();
    assert_eq!(lines.iter().map(|l| l.op.as_ref().unwrap().seq).collect::<Vec<_>>(), vec![1, 2, 3], "folded once each, the later write after it");
}

/// CONTRACT (R-SR3: the writer reconciles its pending append by its `op`; "a complete valid object
/// missing only its terminator is finished with one newline and fsync"): the same unended tail, met by
/// its own writer's reconciliation instead of an open, has the same outcome: one newline alone, an
/// fsync, the record folded once. Must NOT happen: the payload written again.
#[tokio::test]
async fn the_same_unended_tail_reconciled_by_its_writer_has_the_same_outcome() {
    let path = file("store-log-unended-writer");
    let faulted = Arc::new(Faulted::default());
    faulted.writes.lock().unwrap().push_back(WriteFault::Short { drop: 1, then: None });
    let log = StoreLog::open_with(&path, formats(), faulted.clone()).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap();
    let written = faulted.written.lock().unwrap().clone();
    assert_eq!(written.len(), 2);
    assert_eq!(written[1], b"\n");
    let lines = reopen(&path).await.unwrap().lines().await.unwrap();
    assert_eq!(lines.len(), 1, "folded once");
}

/// CONTRACT (R-SR3: "A partial malformed object ... is refused without appending a replacement or
/// discarding any bytes"): a write that landed only part of its object is refused by name, with and
/// without another writer's line after it; its payload is never written again, the writer's next write
/// is refused while the pending append stands, and no byte is removed. Must NOT happen: a retry of the
/// payload, or the partial truncated away.
#[tokio::test]
async fn a_malformed_short_write_refuses_without_retrying_its_payload() {
    for other_after in [false, true] {
        let path = file("store-log-short-partial");
        let faulted = Arc::new(Faulted::default());
        let then: Option<Box<dyn FnOnce() + Send>> = other_after.then(|| {
            let p = path.clone();
            Box::new(move || other_appends(&p, &writer("b"), 1, "other", &rec("b", 1))) as Box<dyn FnOnce() + Send>
        });
        faulted.writes.lock().unwrap().push_back(WriteFault::Short { drop: 30, then });
        let log = StoreLog::open_with(&path, formats(), faulted.clone()).await.unwrap();
        let e = log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap_err().to_string();
        assert!(e.contains("indeterminate") && e.contains("partial object"), "{other_after}: {e}");
        let bytes = std::fs::read(&path).unwrap();
        assert!(log.append(&writer("a"), "rec/1", "j", &rec("c", 2)).await.is_err(), "{other_after}: the next write is refused while the append is pending");
        assert_eq!(faulted.written.lock().unwrap().len(), 1, "{other_after}: the payload is never written again");
        assert_eq!(std::fs::read(&path).unwrap(), bytes, "{other_after}: no byte is appended or removed");
        assert!(reopen(&path).await.is_err(), "{other_after}: the file refuses an open by name");
    }
}

/// CONTRACT (R-SR3: "a separate write gets a new sequence even when its payload is identical. Equal
/// payloads never identify the pending operation"): two writes of one identical record at one key are
/// two lines with two `op`s; a second write that reported zero bytes is found absent by its own `op`,
/// never mistaken for the first, and written again. Must NOT happen: the first write accepted as the
/// second's.
#[tokio::test]
async fn identical_payloads_are_distinct_writes_and_reconciliation_never_takes_the_earlier() {
    let path = file("store-log-identical");
    let faulted = Arc::new(Faulted::default());
    let log = StoreLog::open_with(&path, formats(), faulted.clone()).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("same", 1)).await.unwrap();
    faulted.writes.lock().unwrap().push_back(WriteFault::Zero);
    log.append(&writer("a"), "rec/1", "k", &rec("same", 1)).await.unwrap();
    let lines = reopen(&path).await.unwrap().lines().await.unwrap();
    assert_eq!(lines.len(), 2, "two writes, two lines");
    assert_eq!(lines.iter().map(|l| l.op.as_ref().unwrap().seq).collect::<Vec<_>>(), vec![1, 2], "two ops");
    assert_eq!(lines[0].record, lines[1].record, "one identical payload");
}

/// CONTRACT (R-SR3: "Short writes and fsync failures are indeterminate, never success"; reconciliation
/// by `op`): a write whose fsync failed answers indeterminate, never success; the store's next
/// reconciliation finds the line whole by its `op`, fsyncs it and accepts it without writing it again.
/// Must NOT happen: success answered after the failed fsync, or the line written twice.
#[tokio::test]
async fn a_failed_fsync_answers_indeterminate_and_reconciles_by_op() {
    let path = file("store-log-fsync");
    let faulted = Arc::new(Faulted::default());
    let log = StoreLog::open_with(&path, formats(), faulted.clone()).await.unwrap();
    faulted.syncs.lock().unwrap().push_back(());
    let e = log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap_err().to_string();
    assert!(e.contains("indeterminate") && e.contains("fsync"), "{e}");
    log.reconcile().await.expect("the next reconciliation accepts the line found whole");
    log.append(&writer("a"), "rec/1", "j", &rec("b", 2)).await.unwrap();
    assert_eq!(faulted.written.lock().unwrap().len(), 2, "the first line is never written again");
    let lines = reopen(&path).await.unwrap().lines().await.unwrap();
    assert_eq!(lines.iter().map(|l| (l.key.clone(), l.op.as_ref().unwrap().seq)).collect::<Vec<_>>(), vec![("k".to_string(), 1), ("j".to_string(), 2)]);
}

/// CONTRACT (R-SR3: "If the syscall reported zero bytes written and the intervening bytes validate, the
/// line may be retried. Retries retain the pending op"): a write that reported zero bytes is written
/// again with the same bytes, its `op` unchanged, and answers success once. Must NOT happen: the retry
/// under a new `op`, or the write answered failed.
#[tokio::test]
async fn a_zero_byte_write_is_retried_under_its_pending_op() {
    let path = file("store-log-zero");
    let faulted = Arc::new(Faulted::default());
    faulted.writes.lock().unwrap().push_back(WriteFault::Zero);
    let log = StoreLog::open_with(&path, formats(), faulted.clone()).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap();
    let written = faulted.written.lock().unwrap().clone();
    assert_eq!(written.len(), 2);
    assert_eq!(written[0], written[1], "the retry writes the same bytes: the same op");
    let lines = reopen(&path).await.unwrap().lines().await.unwrap();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].op.as_ref().unwrap().seq, 1);
}

/// CONTRACT (R-SR3: "A writer must not reuse an op after restart: it resumes its sequence from durable
/// evidence"): a writer's process restarts (nothing of its sequence held in memory) and its next write
/// continues from the greatest `seq` of its own lines in the file; two stores of one process writing as
/// one writer never share a `seq`. Must NOT happen: an `op` written twice.
#[tokio::test]
async fn a_restart_never_reuses_an_earlier_op() {
    let path = file("store-log-restart");
    let log = reopen(&path).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("a", 2)).await.unwrap();
    StoreLog::forget_issued(&path);
    let restarted = reopen(&path).await.unwrap();
    let a = writer("a");
    assert_eq!(restarted.writers().get(&(a.node_id.to_string(), a.incarnation_id.to_string())), Some(&2), "the fold holds the writer's greatest seq");
    restarted.append(&writer("a"), "rec/1", "k", &rec("a", 3)).await.unwrap();
    let twin = reopen(&path).await.unwrap();
    twin.append(&writer("a"), "rec/1", "k", &rec("a", 4)).await.unwrap();
    restarted.append(&writer("a"), "rec/1", "k", &rec("a", 5)).await.unwrap();
    let seqs: Vec<u64> = reopen(&path).await.unwrap().lines().await.unwrap().iter().map(|l| l.op.as_ref().unwrap().seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5], "every op is new");
}

/// CONTRACT (proposal §4 "What open costs": "The open records one span per file,
/// rdm.node.store.resolve.via-fold, with the file's bytes, lines, the latest op per writer and the fold's
/// duration; a refused fold names its invalid offset and cause"): an open of a populated file records
/// its bytes, lines, writers and fold duration; an open of a damaged file records `refused` with the
/// offset and the cause. Must NOT happen: an open with no span, or a refusal with no offset.
#[tokio::test]
async fn the_open_span_carries_the_files_bytes_lines_and_duration() {
    let path = file("store-log-span");
    let log = reopen(&path).await.unwrap();
    log.append(&writer("a"), "rec/1", "k", &rec("a", 1)).await.unwrap();
    log.append(&writer("b"), "rec/1", "j", &rec("b", 2)).await.unwrap();
    let spans = capture_spans(|| async { reopen(&path).await.unwrap() }).await;
    let open = spans.iter().find(|s| s.0 == OPEN_SPAN).expect("an open span");
    let len = std::fs::metadata(&path).unwrap().len().to_string();
    assert_eq!(open.1.get("bytes"), Some(&len), "{open:?}");
    assert_eq!(open.1.get("lines").map(String::as_str), Some("2"), "{open:?}");
    let mut expected = vec![format!("{}:1", writer("a")), format!("{}:1", writer("b"))];
    expected.sort();
    assert_eq!(open.1.get("writers"), Some(&expected.join(",")), "{open:?}");
    assert!(open.1.contains_key("duration") && open.1.get("outcome").map(String::as_str) == Some("folded"), "{open:?}");
    let at = std::fs::metadata(&path).unwrap().len() + 1;
    append_raw(&path, b"\n{\"len\":5}\n");
    let spans = capture_spans(|| async { assert!(reopen(&path).await.is_err()) }).await;
    let open = spans.iter().find(|s| s.0 == OPEN_SPAN).expect("an open span");
    assert_eq!((open.1.get("outcome").map(String::as_str), open.1.get("invalid_offset")), (Some("refused"), Some(&at.to_string())), "{open:?}");
    assert!(open.1.get("cause").is_some_and(|c| c.contains("len")), "{open:?}");
}

type Spans = Vec<(String, BTreeMap<String, String>)>;

/// The spans `f` records, each with its fields as recorded.
async fn capture_spans<F: std::future::Future<Output = T>, T>(f: impl FnOnce() -> F) -> Spans {
    struct Sub(Arc<Mutex<Vec<(String, BTreeMap<String, String>)>>>);
    struct Fields<'a>(&'a mut BTreeMap<String, String>);
    impl tracing::field::Visit for Fields<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_string(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
    }
    impl tracing::Subscriber for Sub {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let mut fields = BTreeMap::new();
            span.record(&mut Fields(&mut fields));
            let mut all = self.0.lock().unwrap();
            all.push((span.metadata().name().to_string(), fields));
            tracing::span::Id::from_u64(all.len() as u64)
        }
        fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
            let mut all = self.0.lock().unwrap();
            if let Some((_, fields)) = all.get_mut(span.into_u64() as usize - 1) {
                values.record(&mut Fields(fields));
            }
        }
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    let spans = Arc::new(Mutex::new(Vec::new()));
    let dispatch = tracing::Dispatch::new(Sub(spans.clone()));
    {
        let _g = tracing::dispatcher::set_default(&dispatch);
        f().await;
    }
    let out = spans.lock().unwrap().clone();
    out
}

// ---------------------------------------------------------------- the locks (R-SR5)

fn locks(tag: &str) -> StoreLocks {
    StoreLocks::at(&crate::record_store::tempdir(tag), &LogWriter::mint())
}

fn names(claimant: &'static str) -> impl Fn(&[u8]) -> bool + Send + 'static {
    move |b: &[u8]| b == claimant.as_bytes()
}

/// CONTRACT (R-SR5: "one winner stays one winner"; the in-flight lock "taken and released as
/// mesh_claims.rs does today"): a lock is taken once across two claimants, the second answered the
/// standing lock's content; a release by an attempt that does not hold it removes nothing, and the
/// holder's release frees it for the next take. Must NOT happen: two claimants both holding one lock,
/// or a release removing another claimant's lock.
#[tokio::test]
async fn a_lock_is_taken_once_and_released_only_by_its_claimant() {
    let l = locks("store-locks-once");
    for kind in [LockKind::InFlight, LockKind::Name] {
        assert_eq!(l.take(kind, "mesh2", b"a").await.unwrap(), LockTaken::Won, "{kind:?}: the first claimant takes it");
        assert_eq!(l.take(kind, "mesh2", b"b").await.unwrap(), LockTaken::Held(b"a".to_vec()), "{kind:?}: the second is answered the claimant holding it");
        assert!(!l.release(kind, "mesh2", names("b")).await.unwrap(), "{kind:?}: a release naming another claimant removes nothing");
        assert_eq!(l.read(kind, "mesh2").await.unwrap(), Some(b"a".to_vec()));
        assert!(l.release(kind, "mesh2", names("a")).await.unwrap(), "{kind:?}: the claimant's release removes it");
        assert_eq!(l.take(kind, "mesh2", b"b").await.unwrap(), LockTaken::Won, "{kind:?}: the next take decides on the free lock");
    }
    assert!(l.take(LockKind::Name, "../x", b"a").await.is_err(), "a name that is not one path segment names no lock");
}

/// CONTRACT (R-SR5: "a second reclaimer blocks on the mutex, then re-reads and finds a live claimant,
/// and leaves it"; "a holder whose release was delayed past a reclamation finds another claimant's lock
/// and leaves it"): X holds a name and its release was due. R1 reclaims it (the release of X's lock,
/// then its own take); R2, which read X's lock before R1 ran and was paused before its removal, then
/// removes under the mutex only what still names X, and leaves R1's lock; so does X's own release,
/// delayed past the reclamation. Must NOT happen: R1's lock removed by a remover that expected X.
#[tokio::test]
async fn a_late_remover_finds_another_claimants_lock_and_leaves_it() {
    let l = locks("store-locks-reclaim");
    assert_eq!(l.take(LockKind::Name, "mesh1.rpc.1", b"X").await.unwrap(), LockTaken::Won);
    assert_eq!(l.read(LockKind::Name, "mesh1.rpc.1").await.unwrap(), Some(b"X".to_vec()), "R2 reads X's lock, then pauses");
    assert!(l.release(LockKind::Name, "mesh1.rpc.1", names("X")).await.unwrap(), "R1 releases X's lock under the mutex");
    assert_eq!(l.take(LockKind::Name, "mesh1.rpc.1", b"R1").await.unwrap(), LockTaken::Won, "and takes its own");
    assert!(!l.release(LockKind::Name, "mesh1.rpc.1", names("X")).await.unwrap(), "R2 re-reads under the mutex, finds R1, and removes nothing");
    assert!(!l.release(LockKind::Name, "mesh1.rpc.1", names("X")).await.unwrap(), "X's delayed release finds R1 and removes nothing");
    assert_eq!(l.read(LockKind::Name, "mesh1.rpc.1").await.unwrap(), Some(b"R1".to_vec()), "R1's lock stands");
    assert!(l.path(LockKind::Name, "mesh1.rpc.1").with_file_name("mesh1.rpc.1.reclaim").exists(), "the mutex's anchor stays");
}

/// CONTRACT (R-SR5: the reclamation mutex is "an flock(LOCK_EX) on locks/name/<name>.reclaim ...
/// dropped by the kernel if its holder dies"): a remover waits while another holds the name's mutex,
/// and proceeds once the holder's descriptor goes, as when its process dies. Must NOT happen: a
/// removal inside another's mutex, or a removal that never proceeds after the holder is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remover_waits_on_the_names_mutex_and_proceeds_when_its_holder_dies() {
    let l = locks("store-locks-mutex");
    assert_eq!(l.take(LockKind::Name, "mesh1.rpc.1", b"X").await.unwrap(), LockTaken::Won);
    let held = l.hold_mutex("mesh1.rpc.1");
    let remover = tokio::spawn({
        let l = l.clone();
        async move { l.release(LockKind::Name, "mesh1.rpc.1", names("X")).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(!remover.is_finished(), "the removal waits on the mutex its holder keeps");
    assert_eq!(l.read(LockKind::Name, "mesh1.rpc.1").await.unwrap(), Some(b"X".to_vec()));
    drop(held);
    assert!(tokio::time::timeout(std::time::Duration::from_secs(5), remover).await.expect("the removal proceeds once the holder is gone").unwrap().unwrap());
    assert_eq!(l.read(LockKind::Name, "mesh1.rpc.1").await.unwrap(), None);
}
