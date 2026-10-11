//! The book's SUMMARY.md.
use crate::Page;
use std::path::Path;

/// Write `docs/SUMMARY.md`.
pub fn write(docs: &Path, _out: &Path, _pages: &[Page]) -> anyhow::Result<()> {
    std::fs::write(docs.join("SUMMARY.md"), format!("{}\n", crate::MARKER))?;
    Ok(())
}
