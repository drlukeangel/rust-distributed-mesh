//! The generic RPC proof node and its launch contract. An `rpc_node` binds exactly the endpoint node-admin assigned,
//! serves Node RPC, joins the fabric's membership topic and
//! publishes its digest. It carries no product semantics.
#![deny(missing_docs)]


pub use rafka_mesh_entity::launch;
pub mod node;
mod taskdump;
/// The app's `hydrate_before_ready` hook and everything it is handed.
pub use rafka_node_admin_core::app_hydration;
pub mod node_rpc;
pub mod declare_probe;
pub mod hydrate_probe;
pub mod proof_store;
pub mod resolve_probe;
pub mod faults;
pub mod originate;
pub mod admin_faults;
pub mod test_certs;
pub mod test_app;

/// The launcher half of a functional fabric, shared by test crates.
#[cfg(feature = "rig")]
pub mod rig;

/// The directory under an estate's data root where a scenario records the OS-clock skew of one
/// node: the file `<root>/os-clock-skew/<path.name>` holds signed milliseconds.
pub fn os_clock_skew_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("os-clock-skew")
}

/// Skew this process's OS clock by the milliseconds a scenario recorded for `name` under `root`
/// (`os_clock_skew_dir`), if it recorded any. Called only by testkit executables: it proves no
/// stamp of a node reads the OS clock (rafka-time is adopted, and the OS clock takes part in no
/// fleet decision). A product executable never calls it. Returns the skew applied.
#[cfg(feature = "testkit-skew")]
pub fn skew_os_clock_from_root(root: &std::path::Path, name: &str) -> Option<i64> {
    let ms: i64 = std::fs::read_to_string(os_clock_skew_dir(root).join(name)).ok()?.trim().parse().ok()?;
    rafka_mesh_transport::clock::skew_os_clock_for_testkit(ms);
    Some(ms)
}

/// The directory under an estate's data root where a scenario records a `hydrate_before_ready` knob
/// for one node: while the file `<root>/hydrate-knob/<path.name>` exists when the node starts, the
/// node registers [`hydrate_probe::KnobHydrator`], whose hook is Blocked with the file's text as its
/// reason until the text is `pass`.
pub fn hydrate_knob_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("hydrate-knob")
}

/// Record `text` as node `name`'s `hydrate_before_ready` knob under `root` ([`hydrate_knob_dir`]).
/// The node's hook reads the knob on every attempt, while the scenario rewrites it, so the text is
/// written to `<path.name>.staged` beside the knob and renamed over it: a read opens the old file or
/// the new one, each whole, and never a knob truncated before its new text is written.
pub fn write_hydrate_knob(root: &std::path::Path, name: &str, text: &str) -> std::io::Result<()> {
    let dir = hydrate_knob_dir(root);
    std::fs::create_dir_all(&dir)?;
    let staged = dir.join(format!("{name}.staged"));
    std::fs::write(&staged, text)?;
    std::fs::rename(&staged, dir.join(name))
}

/// The directory under an estate's data root where a scenario records a rafka-time knob for one
/// node: the file `<root>/rafka-time-knob/<path.name>` holds `ahead_ms=<signed ms>` and/or
/// `drift_ppm=<signed ppm>` lines.
pub fn rafka_time_knob_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("rafka-time-knob")
}

/// Put this node's rafka-time off its authority's, as a scenario recorded for `name` under `root`
/// (`rafka_time_knob_dir`): `ahead_ms` re-adopts a reference that far ahead of the reading, and
/// `drift_ppm` runs the node's monotonic time that many parts per million fast from now on. Called
/// only by testkit executables, once the node has adopted its authority's time, to prove the
/// discipline brings a node back to it. Returns `(ahead_ms, drift_ppm)` applied.
#[cfg(feature = "testkit-skew")]
pub fn apply_rafka_time_knob(root: &std::path::Path, name: &str, time: &rafka_mesh_transport::clock::RafkaTime, parent: &tracing::Span) -> Option<(i64, i64)> {
    let text = std::fs::read_to_string(rafka_time_knob_dir(root).join(name)).ok()?;
    let get = |k: &str| text.lines().find_map(|l| l.strip_prefix(k)?.strip_prefix('=')?.trim().parse::<i64>().ok()).unwrap_or(0);
    let (ahead_ms, drift_ppm) = (get("ahead_ms"), get("drift_ppm"));
    if ahead_ms != 0 {
        time.adopt((time.now_ms() as i64 + ahead_ms).max(0) as u64);
    }
    if drift_ppm != 0 {
        time.drift_for_testkit(drift_ppm);
    }
    tracing::info_span!(parent: parent, "rdm.mesh.node.update.via-rafka-time-knob", node = name, ahead_ms, drift_ppm)
        .in_scope(|| tracing::info!("a scenario put this node's rafka-time off its authority's"));
    Some((ahead_ms, drift_ppm))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    /// CONTRACT: a rewrite of a node's `hydrate_before_ready` knob never touches the file a reader
    /// already opened: the reader holding the knob reads the text it opened, whole, and the next
    /// open reads the new text. The knob's hook (`hydrate_probe::KnobHydrator`) reads the file on
    /// every attempt while a scenario rewrites it. What must NOT happen: a rewrite in place, which
    /// truncates the very file a reader holds and writes after it, so a read between the two
    /// sees the knob empty and the hook answers `Blocked` with no reason.
    #[test]
    fn a_knob_rewrite_never_touches_the_file_a_reader_holds_open() {
        let root = std::env::temp_dir().join(format!("hydrate-knob-rewrite-{}", rafka_mesh_entity::NodeId::mint()));
        let (a, b) = ("the rows of orders are not yet published", "the authority of orders is draining");
        write_hydrate_knob(&root, "mesh1.rpc.2", a).unwrap();
        let mut held = std::fs::File::open(hydrate_knob_dir(&root).join("mesh1.rpc.2")).unwrap();
        write_hydrate_knob(&root, "mesh1.rpc.2", b).unwrap();
        let mut read = String::new();
        held.read_to_string(&mut read).unwrap();
        assert_eq!(read, a, "the reader that opened the knob before the rewrite reads the text it opened, whole");
        assert_eq!(std::fs::read_to_string(hydrate_knob_dir(&root).join("mesh1.rpc.2")).unwrap(), b, "the next open reads the new text");
        let _ = std::fs::remove_dir_all(&root);
    }
}
