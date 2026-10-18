//! A whole file written durably: to a temporary file, fsynced, renamed over its path, and its
//! directory fsynced, so a file that exists is whole and survives a crash. The file store's rows are
//! written this way, and so is a deployment provider's launch record, which is the provider's file
//! and no storage boundary's.

use crate::storage_error::StorageError;
use std::path::Path;

/// Write `bytes` to `tmp`, fsync, rename to `path`, and fsync `dir`.
pub fn write_file(dir: &Path, tmp: &Path, path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    use std::io::Write as _;
    let io = |e: std::io::Error| StorageError::Io { file: path.display().to_string(), reason: e.to_string() };
    let mut f = std::fs::File::create(tmp).map_err(io)?;
    f.write_all(bytes).map_err(io)?;
    f.sync_all().map_err(io)?;
    std::fs::rename(tmp, path).map_err(io)?;
    sync_dir(dir, path)
}

/// Sync `dir` after `file` was renamed, linked or removed in it: a change whose directory sync
/// fails is a failed change, named.
pub(crate) fn sync_dir(dir: &Path, file: &Path) -> Result<(), StorageError> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| StorageError::Io { file: file.display().to_string(), reason: format!("the change landed but the directory {} could not be synced: {e}", dir.display()) })
}
