//! The RDM workspace as parsed source.
use std::path::Path;

/// The parsed workspace.
pub struct Workspace;

impl Workspace {
    /// Parse every workspace member's `src/` under `root`.
    pub fn load(_root: &Path) -> anyhow::Result<Self> {
        Ok(Workspace)
    }
}
