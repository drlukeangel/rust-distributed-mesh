//! The one door a unit cell of this crate installs its own span capture through: every callsite
//! enabled first (`rafka_mesh_telemetry::enable_callsites`), then the cell's layer as its own
//! thread's subscriber, so a span the cell opens reaches it whichever thread hit its callsite first.

use tracing_subscriber::layer::SubscriberExt as _;

/// Install `layer` as this thread's subscriber for as long as the guard lives, every callsite
/// enabled first.
pub(crate) fn install<L>(layer: L) -> tracing::subscriber::DefaultGuard
where
    L: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    rafka_mesh_telemetry::enable_callsites();
    tracing::subscriber::set_default(tracing_subscriber::registry().with(layer))
}
