//! Two native participants for the container provider's shared-store qualification.

use anyhow::{bail, ensure, Context, Result};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::{Duration, Instant};

const BYTES: usize = 131_072;
const REPLACEMENTS: usize = 64;

fn create(root: &Path, name: &str, value: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(root.join(name))?;
    file.write_all(value)?;
    file.sync_all()?;
    Ok(())
}

fn listing(root: &Path) -> Result<BTreeSet<String>> {
    fs::read_dir(root)?.map(|entry| Ok(entry?.file_name().into_string().map_err(|_| anyhow::anyhow!("a store filename is not UTF-8"))?)).collect()
}

fn wait(path: &Path, within: Duration, message: &str) -> Result<()> {
    let end = Instant::now() + within;
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
    let root = std::env::var_os("RDM_STORE_DIR").context("RDM_STORE_DIR is required")?;
    let root = Path::new(&root);
    ensure!(root.is_dir(), "RDM_STORE_DIR {} is not mounted", root.display());
    let control = root.join(".qualification");
    let directory = File::open(root)?;
    fs::write(control.join(format!("{role}-ready")), b"ready")?;
    // Idle between commands, like the fixture's other held runtimes. The parent bounds
    // readiness and every commanded phase at 15 s and removes both runtimes on failure.
    idle_until(&control.join("start"));
    match role.as_str() {
        "writer" => {
            println!("before exclusive insertion");
            create(root, "insert.tmp", b"first")?;
            fs::hard_link(root.join("insert.tmp"), root.join("created"))?;
            ensure!(fs::metadata(root.join("insert.tmp"))?.ino() == fs::metadata(root.join("created"))?.ino(), "the insertion is a hard link");
            directory.sync_all()?;
            fs::remove_file(root.join("insert.tmp"))?;
            directory.sync_all()?;
            create(root, "atomic", &vec![b'A'; BYTES])?;
            directory.sync_all()?;
            fs::write(control.join("inserted"), b"inserted")?;
            println!("hard-link insertion; file and directory fsync complete");
            idle_until(&control.join("atomic"));
            wait(&root.join("reader-ready"), Duration::from_secs(10), "reader did not enter its observation")?;
            println!("before atomic replacements");
            for n in 0..REPLACEMENTS {
                create(root, "replace.tmp", &vec![if n % 2 == 0 { b'A' } else { b'B' }; BYTES])?;
                fs::rename(root.join("replace.tmp"), root.join("atomic"))?;
                directory.sync_all()?;
            }
            fs::write(root.join("done"), b"done")?;
            directory.sync_all()?;
            println!("64 atomic replacements, each file and directory synced");
            fs::write(control.join("writer-atomic"), b"complete")?;
            idle_until(&control.join("reverse"));
            ensure!(fs::read(root.join("reader-created"))? == b"reader", "the reverse write is coherent");
            let expected: BTreeSet<_> = [".qualification", "created", "atomic", "reader-created", "reader-ready", "done"].into_iter().map(str::to_string).collect();
            ensure!(listing(root)? == expected, "the reverse listing is coherent and complete");
            ensure!(fs::read(root.join("created"))? == b"first", "the insertion remains unchanged");
            fs::write(control.join("writer-complete"), b"complete")?;
            println!("reverse write and complete listing visible on writer");
        }
        "reader" => {
            idle_until(&control.join("inserted"));
            ensure!(fs::read(root.join("created"))? == b"first", "the inserted value is coherent");
            let expected: BTreeSet<_> = [".qualification", "created", "atomic"].into_iter().map(str::to_string).collect();
            ensure!(listing(root)? == expected, "the insertion listing is coherent and complete");
            create(root, "collision.tmp", b"must-not-replace")?;
            match fs::hard_link(root.join("collision.tmp"), root.join("created")) {
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                other => bail!("create_new must refuse an existing value: {other:?}"),
            }
            ensure!(fs::read(root.join("created"))? == b"first", "create_new changed the existing value");
            fs::remove_file(root.join("collision.tmp"))?;
            directory.sync_all()?;
            fs::write(control.join("collision-complete"), b"complete")?;
            idle_until(&control.join("atomic"));
            let a = vec![b'A'; BYTES];
            let b = vec![b'B'; BYTES];
            ensure!(fs::read(root.join("atomic"))? == a, "the initial atomic value is coherent");
            // Observe replacements from the filesystem, rather than flooding the
            // shared mount with reads while the writer is syncing its 64 files.
            let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
            ensure!(fd >= 0, "watch the shared directory: {}", std::io::Error::last_os_error());
            let mut changes = unsafe { File::from_raw_fd(fd) };
            let path = std::ffi::CString::new(root.as_os_str().as_bytes())?;
            let watched = unsafe { libc::inotify_add_watch(changes.as_raw_fd(), path.as_ptr(), libc::IN_MOVED_TO | libc::IN_CLOSE_WRITE) };
            ensure!(watched >= 0, "watch shared replacements: {}", std::io::Error::last_os_error());
            fs::write(root.join("reader-ready"), b"ready")?;
            let until = Instant::now() + Duration::from_secs(10);
            let mut reads = 0;
            while !root.join("done").exists() {
                ensure!(Instant::now() < until, "writer did not complete its replacements");
                let mut poll = libc::pollfd { fd: changes.as_raw_fd(), events: libc::POLLIN, revents: 0 };
                let remaining = until.saturating_duration_since(Instant::now());
                let ready = unsafe { libc::poll(&mut poll, 1, remaining.as_millis().max(1) as i32) };
                if ready < 0 { return Err(std::io::Error::last_os_error().into()); }
                ensure!(ready > 0, "writer did not complete its replacements");
                let mut notifications = [0u8; 8192];
                changes.read(&mut notifications)?;
                let value = fs::read(root.join("atomic"))?;
                ensure!(value == a || value == b, "torn replacement: {} bytes", value.len());
                reads += 1;
            }
            ensure!(reads > 0, "the reader observed no replacements");
            ensure!(fs::read(root.join("atomic"))? == b, "the final atomic value is coherent");
            ensure!(!listing(root)?.contains("replace.tmp"), "the replacement temporary remains listed");
            create(root, "reader-created", b"reader")?;
            directory.sync_all()?;
            fs::write(control.join("reads"), reads.to_string())?;
            fs::write(control.join("reader-complete"), b"complete")?;
            println!("whole-file reads across concurrent replacements: {reads}");
        }
        _ => unreachable!(),
    }
    Ok(())
}
