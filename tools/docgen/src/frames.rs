//! The frames page.
use crate::source::Workspace;

/// The page title.
pub const TITLE: &str = "Gossip frames";

/// Render the page.
pub fn page(_ws: &Workspace) -> anyhow::Result<String> {
    Ok(format!("{}\n# {TITLE}\n", crate::MARKER))
}
