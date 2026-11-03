//! Two native participants for the container provider's shared-store qualification (R14 item 6):
//! each proves, from its own container, the filesystem contract the file backend relies on (R14
//! item 5 as amended by R-SR3): serialized `O_APPEND` writes, checked write results and file fsync,
//! create-exclusive insertion under `locks/` with its directory fsync, and coherent reads, a listing
//! of `locks/` included, on one backing namespace.

use anyhow::{bail, ensure, Context, Result};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{Duration, Instant};

/// The lines each participant appends to the one store file, both at once.
const LINES: usize = 32;
/// Each line's padding: a line far larger than a page, so a write that was not serialized with the
/// other participant's would show as an interleaved or torn line.
const PAD: usize = 16_384;
/// The store file both append to, as every writer of a fabric appends to its store's one file (R-SR1).
const STORE_FILE: &str = "nodes.jsonl";

/// Insert `name` under `dir` create-exclusively, as the backend takes a lock (`insert_file`): a temp
/// file written and fsynced, hard-linked to the name, which fails when the name stands, then the
/// directory fsynced. `true`: this insert made it; `false`: the name stood and is unchanged.
fn insert(dir: &Path, tmp: &str, name: &str, value: &[u8]) -> Result<bool> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(dir.join(tmp))?;
    file.write_all(value)?;
    file.sync_all()?;
    let linked = fs::hard_link(dir.join(tmp), dir.join(name));
    if linked.is_ok() {
        ensure!(fs::metadata(dir.join(tmp))?.ino() == fs::metadata(dir.join(name))?.ino(), "the insertion is a hard link");
    }
    fs::remove_file(dir.join(tmp))?;
    File::open(dir)?.sync_all()?;
    match linked {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(e) => bail!("the create-exclusive insert of {name} failed: {e}"),
    }
}

fn listing(root: &Path) -> Result<BTreeSet<String>> {
    fs::read_dir(root)?.map(|entry| Ok(entry?.file_name().into_string().map_err(|_| anyhow::anyhow!("a store filename is not UTF-8"))?)).collect()
}

/// One line as R-SR3 frames it, `\n` + one JSON object + `\n`, the object's first field its own byte
/// count.
fn line(role: &str, seq: usize) -> Vec<u8> {
    let pad = if role == "writer" { "w" } else { "r" }.repeat(PAD);
    let rest = format!(",\"by\":\"{role}\",\"seq\":{seq},\"pad\":\"{pad}\"}}");
    let mut len = 0usize;
    loop {
        let object = format!("{{\"len\":{len}{rest}");
        if object.len() == len {
            return format!("\n{object}\n").into_bytes();
        }
        len = object.len();
    }
}

/// Append one line in one `write` on a descriptor opened `O_APPEND`, check the count it reported,
/// then fsync and check that too (R-SR3).
fn append(file: &mut File, bytes: &[u8]) -> Result<()> {
    let written = file.write(bytes)?;
    ensure!(written == bytes.len(), "a short append: {written} of {} bytes", bytes.len());
    file.sync_all().context("the append's fsync")?;
    Ok(())
}

/// Every complete line of the store file, each checked whole: its `len` its own byte count, its
/// padding all one participant's. An unended object at the end is another participant's line still
/// being written, left unread (R-SR4). Answers each line's `(by, seq)` in file order.
fn fold(path: &Path) -> Result<Vec<(String, usize)>> {
    let bytes = fs::read(path)?;
    let ended = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
    let mut lines = Vec::new();
    for object in bytes[..ended].split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let v: serde_json::Value = serde_json::from_slice(object).with_context(|| format!("a torn or interleaved line of {} bytes", object.len()))?;
        ensure!(v["len"].as_u64() == Some(object.len() as u64), "a line's len {} is not its {} bytes", v["len"], object.len());
        let by = v["by"].as_str().context("a line names its writer")?.to_string();
        let pad = v["pad"].as_str().context("a line carries its padding")?;
        let mine = if by == "writer" { 'w' } else { 'r' };
        ensure!(pad.len() == PAD && pad.chars().all(|c| c == mine), "the line of {by} holds another participant's bytes");
        lines.push((by, v["seq"].as_u64().context("a line carries its sequence")? as usize));
    }
    Ok(lines)
}

/// Each participant's lines in the order it appended them, and how many there are.
fn in_order(lines: &[(String, usize)], role: &str) -> Result<usize> {
    let seqs: Vec<usize> = lines.iter().filter(|(by, _)| by == role).map(|(_, s)| *s).collect();
    ensure!(seqs.iter().enumerate().all(|(i, s)| i == *s), "the {role}'s lines are not in its append order: {seqs:?}");
    Ok(seqs.len())
}

fn wait_until(path: &Path, end: Instant, message: &str) -> Result<()> {
    while !path.exists() {
        ensure!(Instant::now() < end, "{message}");
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

fn idle_until(path: &Path) {
    while !path.exists() { std::thread::sleep(Duration::from_millis(1)); }
}

fn main() -> Result<()> {
    let role = std::env::args().nth(1).context("writer or reader is required")?;
    println!("native store participant entered {role}");
    std::io::stdout().flush()?;
    ensure!(role == "writer" || role == "reader", "unknown store participant {role:?}");
    let other = if role == "writer" { "reader" } else { "writer" };
    let root = std::env::var_os("RDM_STORE_DIR").context("RDM_STORE_DIR is required")?;
    let root = Path::new(&root);
    ensure!(root.is_dir(), "RDM_STORE_DIR {} is not mounted", root.display());
    let control = root.join(".qualification");
    let names = root.join("locks").join("name");
    let store = root.join(STORE_FILE);
    fs::write(control.join(format!("{role}-ready")), b"ready")?;
    // Idle between commands, like the fixture's other held runtimes. The parent bounds
    // readiness and every commanded phase at 15 s and removes both runtimes on failure.
    idle_until(&control.join("start"));
    match role.as_str() {
        "writer" => {
            println!("before create-exclusive insertion");
            fs::create_dir_all(&names)?;
            File::open(root.join("locks"))?.sync_all()?;
            ensure!(insert(&names, ".mesh1.node.1.writer.tmp", "mesh1.node.1", b"writer")?, "the first insert of a free name makes it");
            fs::write(control.join("inserted"), b"inserted")?;
            println!("create-exclusive insertion under locks/: file and directory fsync complete");
        }
        "reader" => {
            idle_until(&control.join("inserted"));
            ensure!(fs::read(names.join("mesh1.node.1"))? == b"writer", "the inserted lock is coherent");
            let expected: BTreeSet<_> = ["mesh1.node.1"].into_iter().map(str::to_string).collect();
            ensure!(listing(&names)? == expected, "the locks listing is coherent and complete");
            ensure!(!insert(&names, ".mesh1.node.1.reader.tmp", "mesh1.node.1", b"reader")?, "a create-exclusive insert must refuse a standing lock");
            ensure!(fs::read(names.join("mesh1.node.1"))? == b"writer", "the refused insert changed the standing lock");
            ensure!(listing(&names)? == expected, "the refused insert left an entry in locks/");
            fs::write(control.join("collision-complete"), b"complete")?;
            println!("create-exclusive insertion refused across containers; the lock unchanged");
        }
        _ => unreachable!(),
    }
    idle_until(&control.join("append"));
    // Both participants append to the one store file at once, each line one `write` on its own
    // `O_APPEND` descriptor, and between its appends reads the file as a fold does: every complete
    // line whole, whatever the other participant is writing.
    let mut file = OpenOptions::new().read(true).append(true).create(true).open(&store)?;
    File::open(root)?.sync_all()?;
    let started = Instant::now();
    let mut reads = 0;
    for seq in 0..LINES {
        append(&mut file, &line(&role, seq))?;
        let lines = fold(&store)?;
        in_order(&lines, &role)?;
        in_order(&lines, other)?;
        ensure!(lines.iter().filter(|(by, _)| *by == role).count() == seq + 1, "the {role} does not read back every line it appended");
        reads += 1;
    }
    println!("{LINES} O_APPEND lines of {} bytes, each write's count checked and fsynced, {reads} folds, {} ms", line(&role, 0).len(), started.elapsed().as_millis());
    fs::write(control.join(format!("{role}-appended")), reads.to_string())?;
    wait_until(&control.join(format!("{other}-appended")), Instant::now() + Duration::from_secs(10), "the other participant did not finish its appends")?;
    let lines = fold(&store)?;
    ensure!(in_order(&lines, "writer")? == LINES && in_order(&lines, "reader")? == LINES, "the fold holds every line of both participants: {} lines", lines.len());
    let ended = fs::read(&store)?;
    ensure!(ended.last() == Some(&b'\n'), "the store file ends in a complete line");
    fs::write(control.join(format!("{role}-folded")), b"complete")?;
    println!("{} lines of two participants folded whole and in each one's order", lines.len());
    idle_until(&control.join("reverse"));
    match role.as_str() {
        "reader" => {
            ensure!(insert(&names, ".mesh1.node.2.reader.tmp", "mesh1.node.2", b"reader")?, "the reader's insert of a free name makes it");
            fs::write(control.join("reader-inserted"), b"inserted")?;
        }
        "writer" => {
            wait_until(&control.join("reader-inserted"), Instant::now() + Duration::from_secs(10), "the reader did not insert its lock")?;
            ensure!(fs::read(names.join("mesh1.node.2"))? == b"reader", "the reverse insert is coherent");
            let expected: BTreeSet<_> = ["mesh1.node.1", "mesh1.node.2"].into_iter().map(str::to_string).collect();
            ensure!(listing(&names)? == expected, "the reverse locks listing is coherent and complete");
            ensure!(fs::read(names.join("mesh1.node.1"))? == b"writer", "the first insertion remains unchanged");
            fs::write(control.join("writer-complete"), b"complete")?;
            println!("reverse insert and complete locks listing visible on the writer");
        }
        _ => unreachable!(),
    }
    Ok(())
}
