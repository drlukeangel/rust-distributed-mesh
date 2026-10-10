//! The generic RPC proof node and its launch contract. An `rpc_node` binds exactly the endpoint node-admin assigned,
//! serves Node RPC, joins the fabric's membership topic and
//! publishes its digest. It carries no product semantics.
#![deny(missing_docs)]


pub use rafka_mesh_entity::launch;
pub mod node;
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
