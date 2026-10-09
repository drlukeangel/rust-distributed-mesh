//! Disk pressure on ONE node's own data dir: a bounded filler file really allocated in that
//! directory, removed on heal. The host's filesystem is never filled; the fault is the node's own
//! share of it.
//!
//! Each application is one span, `rdm.testkit.fault.update.via-disk-fill`, with the typed outcome;
//! the heal is `rdm.testkit.fault.remove.via-heal`.

use serde::Serialize;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// The most one fill allocates.
pub const MAX_FILL_BYTES: u64 = 1 << 30;
/// The filler's file name inside the data dir.
pub const FILLER: &str = "CHAOS-DISK-FULL.bin";

/// Why a fill was not applied. Nothing is allocated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "disk_refusal", rename_all = "snake_case")]
pub enum DiskRefusal {
    /// The data dir is not a directory.
    NoDataDir {
        /// The path.
        path: String,
    },
    /// The fill is zero or beyond [`MAX_FILL_BYTES`].
    Unbounded {
        /// Bytes asked.
        asked: u64,
        /// The most allowed.
        max: u64,
    },
    /// The filesystem would be left with less than the fill's own size free.
    WouldExhaustHost {
        /// Bytes asked.
        asked: u64,
        /// Bytes free to this user.
        free: u64,
    },
    /// A filler is already there.
    AlreadyFilled {
        /// The path.
        path: String,
    },
    /// The OS refused the allocation.
    AllocationFailed {
        /// The path.
        path: String,
        /// The errno.
        errno: i32,
    },
}

/// A filler allocated in a node's data dir, removed when dropped.
#[derive(Debug)]
pub struct DiskFull {
    /// The filler's path.
    pub path: PathBuf,
    /// Bytes asked for.
    pub bytes: u64,
    /// Bytes the filesystem reports allocated to the file (blocks * 512).
    pub allocated: u64,
    since: Instant,
}

fn free_bytes(dir: &Path) -> Option<u64> {
    let c = std::ffi::CString::new(dir.to_str()?).ok()?;
    // SAFETY: statvfs(3) writes only into the zeroed struct we own; `c` is a valid C string.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    (unsafe { libc::statvfs(c.as_ptr(), &mut st) } == 0).then(|| st.f_bavail as u64 * st.f_frsize as u64)
}

impl DiskFull {
    /// Allocate `bytes` in `data_dir`.
    pub fn fill(data_dir: &Path, bytes: u64) -> Result<Self, DiskRefusal> {
        let span = tracing::info_span!("rdm.testkit.fault.update.via-disk-fill", data_dir = %data_dir.display(), bytes, outcome = tracing::field::Empty);
        let _g = span.enter();
        let out = Self::fill_inner(data_dir, bytes);
        span.record("outcome", tracing::field::display(match &out {
            Ok(d) => format!("allocated {} bytes at {}", d.allocated, d.path.display()),
            Err(r) => serde_json::to_string(r).unwrap_or_default(),
        }));
        out
    }

    fn fill_inner(data_dir: &Path, bytes: u64) -> Result<Self, DiskRefusal> {
        if !data_dir.is_dir() {
            return Err(DiskRefusal::NoDataDir { path: data_dir.display().to_string() });
        }
        if bytes == 0 || bytes > MAX_FILL_BYTES {
            return Err(DiskRefusal::Unbounded { asked: bytes, max: MAX_FILL_BYTES });
        }
        let free = free_bytes(data_dir).unwrap_or(0);
        if free < bytes.saturating_mul(2) {
            return Err(DiskRefusal::WouldExhaustHost { asked: bytes, free });
        }
        let path = data_dir.join(FILLER);
        if path.exists() {
            return Err(DiskRefusal::AlreadyFilled { path: path.display().to_string() });
        }
        let fail = |e: std::io::Error| DiskRefusal::AllocationFailed { path: path.display().to_string(), errno: e.raw_os_error().unwrap_or(0) };
        let f = std::fs::OpenOptions::new().create_new(true).write(true).open(&path).map_err(fail)?;
        use std::os::fd::AsRawFd;
        // SAFETY: posix_fallocate(3) on a file descriptor this function owns.
        let rc = unsafe { libc::posix_fallocate(f.as_raw_fd(), 0, bytes as libc::off_t) };
        if rc != 0 {
            let _ = std::fs::remove_file(&path);
            return Err(DiskRefusal::AllocationFailed { path: path.display().to_string(), errno: rc });
        }
        let allocated = f.metadata().map(|m| m.blocks() * 512).unwrap_or(0);
        Ok(Self { path, bytes, allocated, since: Instant::now() })
    }
}

impl Drop for DiskFull {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        tracing::info_span!("rdm.testkit.fault.remove.via-heal", fault = "disk-fill", held_ms = self.since.elapsed().as_millis() as u64).in_scope(|| tracing::info!("the filler is removed"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn private_dir() -> PathBuf {
        let d = std::env::temp_dir().join(format!("rafka-chaos-disk-{}-{:?}", std::process::id(), std::thread::current().id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn a_fill_allocates_in_the_data_dir_is_refused_when_unbounded_or_repeated_and_heals_by_removal() {
        let dir = private_dir();
        assert!(matches!(DiskFull::fill(&dir.join("absent"), 4096), Err(DiskRefusal::NoDataDir { .. })));
        assert!(matches!(DiskFull::fill(&dir, 0), Err(DiskRefusal::Unbounded { .. })));
        assert!(matches!(DiskFull::fill(&dir, MAX_FILL_BYTES + 1), Err(DiskRefusal::Unbounded { .. })));
        let fill = DiskFull::fill(&dir, 8 << 20).expect("8 MiB fits");
        assert_eq!(std::fs::metadata(&fill.path).unwrap().len(), 8 << 20);
        assert!(fill.allocated >= 8 << 20, "really allocated, not sparse: {}", fill.allocated);
        assert!(matches!(DiskFull::fill(&dir, 4096), Err(DiskRefusal::AlreadyFilled { .. })));
        let path = fill.path.clone();
        drop(fill);
        assert!(!path.exists(), "the heal removes the filler");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
