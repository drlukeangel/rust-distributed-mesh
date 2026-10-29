//! The generic RPC proof node and its launch contract. An `rpc_node` binds exactly the endpoint node-admin assigned,
//! serves Node RPC, joins the fabric's topology topic and
//! publishes its digest. It carries no product semantics.
//!
//! # Where it sits
//!
//! On every other RDM crate: it is the test kit an application's cells use, and RDM's own. An
//! [`estate::Estate`] is one fabric of real node processes a cell starts, drives through the typed
//! objects and reads back; the probes, proof protocols and fault doors beside it are what the cells
//! assert with.
//!
//! # Example
//!
//! ```no_run
//! use rdm_test::estate::{Estate, Owner};
//! use std::time::Duration;
//!
//! #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//! async fn a_fabric_of_two_meshes_reaches_ready() {
//!     let owner = Owner {
//!         product: "mesh".into(),
//!         feature: "fabric".into(),
//!         subfeature: "create".into(),
//!         rung: "MN".into(),
//!         provider: "process".into(),
//!         test: "a_fabric_of_two_meshes_reaches_ready".into(),
//!     };
//!     let estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;
//!     // mesh1 with one node-admin, mesh2 with one node-admin and one rpc node.
//!     estate.reconcile_shape("fabric1", &[("mesh1", 1, 0), ("mesh2", 1, 1)]).await;
//!     estate.until_fabric_ready(Duration::from_secs(120)).await;
//! }
//! ```
//!
//! # Testkit ops
//!
//! The testkit's own Node RPC ops, by opcode (RDM's own are on `rdm_core::protocol`). Each body
//! variant's documentation names its postcard position.
//!
//! | op | name | protocol | request | reply |
//! |---|---|---|---|---|
//! | `0x40` | `broker-data` | [`BrokerData`](crate::families::BrokerData) | [`BrokerDataRequest`](crate::families::BrokerDataRequest) | [`BrokerDataReply`](crate::families::BrokerDataReply) |
//! | `0x70` | `proof-store` | [`ProofStore`](crate::proof_store::ProofStore) | [`ProofRequest`](crate::proof_store::ProofRequest) | [`ProofReply`](crate::proof_store::ProofReply) |
//! | `0x71` | `resolve-probe` | [`ResolveProbe`](crate::resolve_probe::ResolveProbe) | [`ResolveRequest`](crate::resolve_probe::ResolveRequest) | [`ResolveReply`](crate::resolve_probe::ResolveReply) |
//! | `0x72` | `declare-probe` | [`DeclareProbe`](crate::declare_probe::DeclareProbe) | [`DeclareRequest`](crate::declare_probe::DeclareRequest) | [`DeclareReply`](crate::declare_probe::DeclareReply) |
//! | `0x73` | `originate` | [`Originate`](crate::originate::Originate) | [`OriginateRequest`](crate::originate::OriginateRequest) | [`OriginateReply`](crate::originate::OriginateReply) |
//! | `0x74` | `pre-node-ready-pull` | [`PreNodeReadyPull`](crate::pre_node_ready_probe::PreNodeReadyPull) | [`PullRequest`](crate::pre_node_ready_probe::PullRequest) | [`PullReply`](crate::pre_node_ready_probe::PullReply) |
//! | `0x75` | `fault-door` | [`FaultDoor`](crate::fault_door::FaultDoor) | [`FaultRequest`](crate::fault_door::FaultRequest) | [`FaultReply`](crate::fault_door::FaultReply) |
//!
//! # Guide
//!
//! [Test harnesses](../../guide/eng/testing-harnesses.html),
//! [Testing with node-admin](../../guide/eng/testing-with-node-admin.html) and
//! [A test's fabric](../../guide/eng/testing-mesh-lifecycle.html), in the docs site
//! `scripts/docs-site.sh` builds.
#![deny(missing_docs)]


pub use rdm_core::launch;
pub mod node;
pub(crate) use rdm_node::taskdump;
/// The app's `pre_node_ready` hook and everything it is handed.
pub use rdm_node::node_hooks;
pub use rdm_node::{executable_name, launch_for, read_launch, Role};
pub use rdm_node::node_rpc;
pub mod declare_probe;
pub mod pre_node_ready_probe;
pub mod proof_store;
pub mod families;
pub mod conformance;
pub use rdm_node::held_name;
pub mod resolve_probe;
pub mod faults;
pub mod originate;
pub mod admin_faults;
pub mod fault_door;
pub mod test_certs;
pub mod test_app;

/// The launcher half of a functional fabric, shared by test crates.
pub mod rig;
pub mod channel_member;

// ── Scenario & Replay Harness (from former rdm-test-scenario) ──
#[allow(missing_docs)]
pub mod canary;
#[allow(missing_docs)]
pub mod elections;
#[allow(missing_docs)]
pub mod estate;
#[allow(missing_docs)]
pub mod ledger;
#[allow(missing_docs)]
pub mod model;
#[allow(missing_docs)]
pub mod replay;
#[allow(missing_docs)]
pub mod runner;
#[allow(missing_docs)]
pub mod scenario;
#[allow(missing_docs)]
pub mod sim;
#[allow(missing_docs)]
pub mod soak;
#[allow(missing_docs)]
pub mod typed;
#[allow(missing_docs)]
pub mod executables;
#[allow(missing_docs)]
pub mod wedge;

// ── Chaos Kit Fault Backends (from former rafka-chaos) ──
pub mod container_faults;
pub mod host_net;
pub mod disk_faults;
pub mod link_faults;
pub mod netfault;
pub mod process_faults;
/// Rafka's replace steps and its rollback as a test caller submits them (lifecycles/node-replace.md).
pub mod steps;

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
    rdm_transport::clock::skew_os_clock_for_testkit(ms);
    Some(ms)
}

/// The directory under an estate's data root where a scenario records a `pre_node_ready` knob
/// for one node: while the file `<root>/pre-node-ready-knob/<path.name>` exists when the node starts, the
/// node registers [`pre_node_ready_probe::KnobPreNodeReady`], whose hook is Blocked with the file's text as its
/// reason until the text is `pass`.
pub fn pre_node_ready_knob_dir(root: &std::path::Path) -> std::path::PathBuf {
    root.join("pre-node-ready-knob")
}

/// Record `text` as node `name`'s `pre_node_ready` knob under `root` ([`pre_node_ready_knob_dir`]).
/// The node's hook reads the knob on every attempt, while the scenario rewrites it, so the text is
/// written to `<path.name>.staged` beside the knob and renamed over it: a read opens the old file or
/// the new one, each whole, and never a knob truncated before its new text is written.
pub fn write_pre_node_ready_knob(root: &std::path::Path, name: &str, text: &str) -> std::io::Result<()> {
    let dir = pre_node_ready_knob_dir(root);
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
pub fn apply_rafka_time_knob(root: &std::path::Path, name: &str, time: &rdm_transport::clock::RafkaTime, parent: &tracing::Span) -> Option<(i64, i64)> {
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

    /// CONTRACT: a rewrite of a node's `pre_node_ready` knob never touches the file a reader
    /// already opened: the reader holding the knob reads the text it opened, whole, and the next
    /// open reads the new text. The knob's hook (`pre_node_ready_probe::KnobPreNodeReady`) reads the file on
    /// every attempt while a scenario rewrites it. What must NOT happen: a rewrite in place, which
    /// truncates the very file a reader holds and writes after it, so a read between the two
    /// sees the knob empty and the hook answers `Blocked` with no reason.
    #[test]
    fn a_knob_rewrite_never_touches_the_file_a_reader_holds_open() {
        let root = std::env::temp_dir().join(format!("pre-node-ready-knob-rewrite-{}", rdm_core::NodeId::mint()));
        let (a, b) = ("the rows of orders are not yet published", "the authority of orders is connections-deleting");
        write_pre_node_ready_knob(&root, "mesh1.rpc.2", a).unwrap();
        let mut held = std::fs::File::open(pre_node_ready_knob_dir(&root).join("mesh1.rpc.2")).unwrap();
        write_pre_node_ready_knob(&root, "mesh1.rpc.2", b).unwrap();
        let mut read = String::new();
        held.read_to_string(&mut read).unwrap();
        assert_eq!(read, a, "the reader that opened the knob before the rewrite reads the text it opened, whole");
        assert_eq!(std::fs::read_to_string(pre_node_ready_knob_dir(&root).join("mesh1.rpc.2")).unwrap(), b, "the next open reads the new text");
        let _ = std::fs::remove_dir_all(&root);
    }
}

// ── Proof-Node Families & Oracles (from former rafka-node-base) ──────────────

/// The product family every role's own tags are ledgered under.
pub(crate) const PRODUCT: &str = "rdm-roles";

/// One live legacy op a product serves through a transitional adapter until its named cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyAdapter {
    /// The legacy op's tag byte.
    pub op: u8,
    /// The op's catalog name.
    pub name: &'static str,
    /// The product the core ledger reserves the op for.
    pub owner: &'static str,
    /// The migration unit that deletes the adapter.
    pub migration_unit: &'static str,
}

/// The proof product's transitional adapters: the ledgered Rafka reservations it stands in for.
pub const LEGACY_ADAPTERS: &[LegacyAdapter] = &[LegacyAdapter {
    op: 0x12,
    name: "data-frame",
    owner: "rafka",
    migration_unit: "i142 U6",
}];

/// Compose this process's catalog beyond the core: the role's own families and the product's
/// transitional adapters, into the one builder that builds once (i141 PRD §16).
pub fn compose(
    role: Role,
    served_by: &str,
    b: rdm_transport::ServerBuilder,
    client: std::sync::Arc<rdm_transport::NodeRpcClient>,
) -> rdm_transport::ServerBuilder {
    let b = families::for_kind(role.kind, served_by, b, client);
    LEGACY_ADAPTERS.iter().fold(b, |b, a| {
        b.adapter(rdm_core::catalog::CatalogEntry::transitional(
            a.op,
            a.name,
            a.owner,
            a.migration_unit,
            64 * 1024,
        ))
    })
}

/// The testkit's oracles the proof estate's probe speaks, served by every role.
pub struct Oracles {
    /// The data-dir key-value store the proof store protocol serves.
    pub store: std::sync::Arc<crate::proof_store::FileProofStore>,
    /// The declare oracle calls out through the process's one client, set once the node runs.
    pub declare_client: std::sync::Arc<std::sync::OnceLock<crate::node_rpc::ProcessNodeRpc>>,
}

impl Oracles {
    /// Open the oracles under `launch`'s data directory.
    pub fn open(launch: &rdm_core::launch::Launch) -> anyhow::Result<Self> {
        let store = crate::proof_store::FileProofStore::open(&launch.data_dir)
            .map_err(|e| anyhow::anyhow!("the proof store refused to open: {e}"))?;
        Ok(Self {
            store: std::sync::Arc::new(store),
            declare_client: std::sync::Arc::new(std::sync::OnceLock::new()),
        })
    }

    /// Add the proof store, resolve probe and declare probe protocols to `b`.
    pub fn serve(
        &self,
        b: rdm_transport::ServerBuilder,
        launch: &rdm_core::launch::Launch,
        resolver: std::sync::Arc<rdm_transport::LiveNodeResolver>,
        name: &crate::held_name::HeldName,
    ) -> rdm_transport::ServerBuilder {
        crate::declare_probe::serve(
            crate::resolve_probe::serve(
                crate::proof_store::serve(b, self.store.clone(), launch, name),
                resolver,
                launch,
                name,
            ),
            self.declare_client.clone(),
            launch,
        )
    }
}

/// The one proof-node executable's run (`rdm-node`).
pub async fn run_proof_launched() -> anyhow::Result<()> {
    run_proof(Role::of(&read_launch()?)).await
}

/// Run a proof role process to completion with testkit seams and oracles.
pub async fn run_proof(role: Role) -> anyhow::Result<()> {
    run_proof_with(role, crate::node_hooks::NodeHooks::none(), |b| b).await
}

/// [`run_proof`] for a proof role with a `pre_node_ready` hook.
pub async fn run_proof_with(
    role: Role,
    node_hooks: crate::node_hooks::NodeHooks,
    serve_app: impl FnOnce(rdm_transport::ServerBuilder) -> rdm_transport::ServerBuilder,
) -> anyhow::Result<()> {
    crate::node::listen_for_stop();
    let executable = executable_name()?;
    let _telemetry = rdm_transport::init_evidence_telemetry(&executable);
    let launch = launch_for(role)?;
    let boot = tracing::info_span!(
        "rdm.mesh.node.create.via-deployment",
        node = %launch.name,
        node_id = %launch.node_id,
        incarnation_id = %launch.incarnation,
        kind = role.name(),
        executable = %executable,
    );
    if let Ok(tp) = std::env::var("TRACEPARENT") {
        rdm_transport::set_parent(&boot, &tp);
    }
    let oracles = Oracles::open(&launch)?;
    let running = {
        use tracing::Instrument;
        let started = crate::node::start_pre_node_ready(&launch, node_hooks, |b, seams| {
            serve_app(oracles.serve(
                compose(role, &launch.node_id.to_string(), b, seams.client),
                &launch,
                seams.resolver,
                &seams.name,
            ))
        })
        .instrument(tracing::Span::none());
        let started = tokio::select! {
            r = started => r,
            () = crate::node::stop_requested() => {
                boot.in_scope(|| tracing::info!("stopped by signal before the node booted"));
                drop(boot);
                tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation, booted = false).in_scope(|| tracing::info!("stopping"));
                return Ok(());
            }
        };
        started.map_err(|e| {
            boot.in_scope(|| tracing::error!(error = %e, "node failed to come up"));
            e
        })?
    };
    let _ = oracles.declare_client.set(running.node_rpc.clone());
    let served = running.server.catalog().entries().count();
    boot.in_scope(|| tracing::info!(served, kind = role.name(), "role process serving on the imported substrate"));
    drop(boot);
    println!("RDM_NODE_READY {}", launch.node_id);
    let ended = tokio::select! {
        () = crate::node::wait_for_signal(&executable) => None,
        why = running.pre_node_ready_failed() => Some(why),
    };
    if let Some(why) = ended {
        tracing::info_span!("rdm.mesh.node.delete.via-pre-node-ready-failed", node = %launch.name, reason = %why).in_scope(|| tracing::error!("pre_node_ready failed; this node ends"));
        running.stop().await;
        return Err(anyhow::anyhow!("{} failed pre_node_ready: {why}", launch.name));
    }
    tracing::info_span!("rdm.mesh.node.delete.via-signal", node = %launch.name, incarnation_id = %launch.incarnation, booted = true).in_scope(|| tracing::info!("stopping"));
    running.stop().await;
    Ok(())
}

/// Public qualification cuts for app-channel loss; absent without the testkit feature.
#[cfg(feature = "testkit-gossip")]
pub mod gossip_faults;

/// Public qualification cuts for data-plane connection faults (`Q62`); absent without the testkit feature.
#[cfg(feature = "testkit-gossip")]
pub mod connection_faults;

