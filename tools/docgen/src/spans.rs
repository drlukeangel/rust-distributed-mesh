//! The spans page.
use std::path::Path;

/// The page title.
pub const TITLE: &str = "Spans";

/// Render the page.
pub fn page(_root: &Path) -> anyhow::Result<String> {
    Ok(format!("{}\n# {TITLE}\n", crate::MARKER))
}
