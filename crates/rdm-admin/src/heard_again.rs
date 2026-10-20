//! A birth heard again (Q87, Luke 2026-10-20): every node coming to ready-for-traffic from any other
//! state connects to its node-admin for its cert, its time and its cache, and the `before_ready`
//! hook is where it does that; node-admins included, the Day-0 fabric-primary excepted. When the
//! tickle reaches a birth its authority holds off its mesh (node-recovery.md step 2, node-tickle.md
//! "What follows"), the authority begins the birth's next cycle and commands `node.mesh.join` to it,
//! back over its own call; the birth runs node.start steps 2 to 5 again on it and is published
//! ready-for-traffic only once its hook has passed (node-mesh-join.md step 10). One function, run by
//! the mesh-primary's offline ladder for its members and by the fabric-primary's seat watch for a
//! held seat holder; nothing re-publishes a presence by hand. `AdminRunner` registers the birth's
//! join (as it does a parked birth's) so the join door admits the report.

use crate::model::{IncarnationId, Node};
use crate::status_rpc::AuthoritySlot;
use rdm_core::status::{Status, StatusRequest};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use std::sync::Arc;

/// Begin `node`'s next cycle (its declared state is measured from Pending again) and command
/// `node.mesh.join` to its exact birth, naming the authority's current Build; the reply's name.
/// One span, `rdm.node_admin.node.update.via-mesh-join-commanded`, with the outcome.
pub(crate) async fn command_mesh_join(
    client: &Arc<NodeRpcClient>,
    authority: &AuthoritySlot,
    builds: &Arc<dyn crate::build_state::BuildStateAdapter>,
    runner: &crate::admin::runner::AdminRunner,
    node: &Node,
    incarnation: &IncarnationId,
) -> String {
    let (build_id, attempt) = match builds.current().await {
        Ok(Some(b)) => (b.build_id.to_string(), b.attempt),
        _ => (String::new(), 0),
    };
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-mesh-join-commanded",
        node = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        build_id = %build_id,
        attempt,
        outcome = tracing::field::Empty,
        "otel.kind" = "internal"
    );
    let outcome = async {
        let Some(authority) = authority.get() else {
            return "no-authority".to_string();
        };
        // The join the command makes the birth report is verified against the deployment this admin
        // holds of it (node-mesh-join.md step 4): the live birth is registered as a start after a
        // stop registers a parked one, from its runtime handle and its admitted digest.
        match runner.stopped_birth_or_live("node.mesh.join", &node.name).await {
            Ok((_, _, deployed)) => {
                let _ = runner.joins.expect(deployed);
            }
            Err(why) => return format!("join-not-registered: {why}"),
        }
        authority.begin_cycle(node, incarnation).await;
        let req = StatusRequest::JoinMesh {
            node_id: node.node_id.clone(),
            incarnation: incarnation.clone(),
            build_id,
            attempt,
            operation: format!("node.mesh.join:{}", node.name),
        };
        let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(node.node_id.clone()), &req, &CallOptions::default()).await;
        match out.reply() {
            Some(r) => r.value().name().to_string(),
            None => out.name().to_string(),
        }
    };
    let outcome = tracing::Instrument::instrument(outcome, span.clone()).await;
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!(node = %node.name, outcome = %outcome, "a birth heard again is commanded back through node.mesh.join"));
    outcome
}
