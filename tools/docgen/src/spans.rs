//! spans.md: every span name rafka-mesh-audit's scanner finds (`telemetry::emitters`, called, not
//! copied), one row per site, sorted by name.

use std::collections::BTreeSet;
use std::path::Path;

/// The page title.
pub const TITLE: &str = "Spans";

/// Render the page from the source under `root`.
pub fn page(root: &Path) -> anyhow::Result<String> {
    let mut emitters = rafka_mesh_audit::telemetry::emitters(root)?;
    emitters.sort_by(|a, b| (&a.name, &a.file, a.line).cmp(&(&b.name, &b.file, b.line)));
    let names: BTreeSet<&str> = emitters.iter().map(|e| e.name.as_str()).collect();
    let mut out = format!("{}\n# {TITLE}\n\n", crate::MARKER);
    out.push_str(&format!(
        "Every span rafka-mesh-audit's scanner (`telemetry::emitters`) finds in the non-test source under `crates/`, `tools/` and `demo/`: the span macros, and each `#[instrument]` (one without a `name` is named by its function). {} names at {} sites, sorted by name, one row per site.\n\n",
        names.len(),
        emitters.len()
    ));
    out.push_str("| span | file | line |\n|---|---|---|\n");
    for e in &emitters {
        out.push_str(&format!("| {} | `{}` | {} |\n", crate::render::code(&e.name), e.file, e.line));
    }
    Ok(out)
}
