//! i143 opening-audit gates (PRD §19 e0).
//!
//! Each module is one mechanical gate that a test or CI step runs against the
//! workspace. A gate returns every violation it finds, never just the first,
//! so one run names the whole gap.

pub mod admin_ui;
pub mod deps;
pub mod legacy;
pub mod parity;

use std::path::{Path, PathBuf};

/// The RDM workspace root (two levels above this crate).
pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("tools/mesh-audit sits two levels below the workspace root")
        .to_path_buf()
}
