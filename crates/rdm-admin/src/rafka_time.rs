//! Rafka-time: Day-0 root and the pull when the fabric-primary moved (`rdm-admin`).

use rdm_core::NodeId;
use rdm_transport::clock::{Adopted, RafkaTime};
use std::sync::Arc;
use tracing::Instrument as _;

/// Rafka-time puller, server, refusal, and adoption helpers (`docs/architecture/fabric-node-hooks.md` "rafka-time").
pub use rdm_node::rafka_time::*;

/// Why the Day-0 root adopts its own clock (`docs/architecture/fabric-node-hooks.md` "rafka-time"):
/// no server exists above it.
pub const OWN_CLOCK_DAY0_ROOT: &str = "day0-root";
/// The fabric's root adopts its own OS clock as rafka-time, once (`docs/architecture/fabric-node-hooks.md`
/// "rafka-time"): the Day-0 root at boot, and the node-admin a fabric recovery launches when every
/// node the store names is confirmed gone by the provider. No node serves rafka-time to either.
pub fn adopt_own_clock(parent: &tracing::Span, time: &RafkaTime, node: &str, reason: &'static str) -> Adopted {
    let now = rdm_transport::clock::Clock::now_rafka_ms(&rdm_transport::clock::OsClock);
    let adopted = time.adopt(now);
    tracing::info_span!(
        parent: parent,
        "rdm.mesh.entry.update.via-rafka-time-adopted",
        node,
        source = "own-clock",
        reason,
        reference_ms = adopted.reference_ms,
    )
    .in_scope(|| tracing::info!("the fabric's root adopted its own OS clock as rafka-time"));
    adopted
}

/// How many reads one `fabric-primary` move owes before the pull ends unanswered, and the pause
/// between them (grows by this much per attempt). Read-only spacing: nothing else retries.
const ROLE_MOVE_ATTEMPTS: u32 = 12;
const ROLE_MOVE_PAUSE: std::time::Duration = std::time::Duration::from_millis(250);

/// A `mesh-primary` pulls rafka-time once from the new `fabric-primary` each time the `fabric-primary`
/// moves to another node (a record of a later revision naming another node;
/// `docs/architecture/fabric-node-hooks.md` "rafka-time"). The `fabric-primary` itself keeps the
/// rafka-time it holds: a handover changes the server, not the clock, so the fabric stays on one
/// lineage. A replica is not a `mesh-primary` and adopted its time at its join.
///
/// The pull is `GetTopology` of the `fabric-primary`, adopted through [`adopt_pulled`] under the rule
/// of a `mesh-primary`. Each read targets the `fabric-primary` this admin's primary records name at
/// that moment: a primary superseded while the pull runs (a hand-over whose new primary was itself
/// replaced) answers `served-by-a-replica` and is refused by name, and the next read goes to the
/// primary that superseded it. A primary that never answers ends the pull with the time held.
pub async fn run_role_move_pull(
    time: RafkaTime,
    me: crate::model::PathName,
    me_id: NodeId,
    client: Arc<rdm_transport::NodeRpcClient>,
    topology_view: rdm_transport::topology::TopologyView,
) {
    let primaries = topology_view.primaries().clone();
    let mut changed = primaries.subscribe();
    let mut last = primaries.fabric().map(|h| (h.node_id, h.revision));
    loop {
        if changed.changed().await.is_err() {
            return;
        }
        let Some(primary) = primaries.fabric() else { continue };
        let now = Some((primary.node_id.clone(), primary.revision));
        if now == last {
            continue;
        }
        let from = last.replace((primary.node_id.clone(), primary.revision));
        // The first record heard is not a move; and the fabric-primary pulls from no one.
        let Some((from_node, _)) = from else { continue };
        if primary.node_id == me_id || from_node == primary.node_id {
            continue;
        }
        if Puller::of(&primaries, &me.mesh, &me_id) != Puller::MeshPrimary {
            continue;
        }
        let span = tracing::info_span!("rdm.mesh.entry.resolve.via-fabric-primary-changed", node = %me, from = %from_node, to = %primary.node_id, to_mesh = %primary.mesh, revision = primary.revision, outcome = tracing::field::Empty, attempts = tracing::field::Empty, adopted_from_revision = tracing::field::Empty);
        let (outcome, attempts, adopted_from) = pull_from_fabric_primary(&time, &me, &me_id, &client, &topology_view, primary).instrument(span.clone()).await;
        span.record("outcome", outcome.as_str());
        span.record("attempts", attempts);
        if let Some(h) = adopted_from {
            span.record("adopted_from_revision", h.revision);
            // The record adopted from is the last move answered: a newer record is a newer move.
            last = Some((h.node_id, h.revision));
        }
        span.in_scope(|| tracing::info!("the fabric-primary role moved: this mesh primary pulled rafka-time once from the new fabric-primary"));
    }
}

async fn pull_from_fabric_primary(
    time: &RafkaTime,
    me: &crate::model::PathName,
    me_id: &NodeId,
    client: &rdm_transport::NodeRpcClient,
    topology_view: &rdm_transport::topology::TopologyView,
    moved_to: rdm_core::Primary,
) -> (String, u32, Option<rdm_core::Primary>) {
    let mut last = String::from("no attempt");
    let mut primary = moved_to;
    for attempt in 1..=ROLE_MOVE_ATTEMPTS {
        // The primary this admin's records name now: a role that moved again is pulled from where it is.
        if let Some(current) = topology_view.primaries().fabric() {
            if current.node_id == *me_id {
                return ("kept: this admin took the fabric-primary role itself".into(), attempt - 1, None);
            }
            primary = current;
        }
        let read = crate::topology_read::read_topology(client, &rdm_transport::NodeTarget::ExactNode(primary.node_id.clone()), &me.to_string(), Some(&primary.mesh), None).await;
        match read {
            Ok(read) => {
                let served_by = topology_view.digest_view.all().into_iter().find(|d| d.node.node_id == primary.node_id).map(|d| d.node.name.to_string()).unwrap_or_else(|| primary.node_id.to_string());
                match read.adopt_rafka_time(time, topology_view, &me.to_string(), &me.mesh, me_id, &served_by) {
                    Ok(_) => return ("adopted".into(), attempt, Some(primary)),
                    Err(e) => last = e.to_string(),
                }
            }
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(ROLE_MOVE_PAUSE * attempt).await;
    }
    tracing::info_span!("rdm.mesh.entry.reject.via-rafka-time-pull-unanswered", node = %me, to = %primary.node_id, attempts = ROLE_MOVE_ATTEMPTS, reason = %last)
        .in_scope(|| tracing::warn!("the fabric-primary did not serve rafka-time: this mesh primary keeps the rafka-time it holds"));
    (format!("kept: {last}"), ROLE_MOVE_ATTEMPTS, None)
}

/// A node-admin with no launcher and no `JoinNode` to make whose store names other nodes takes
/// rafka-time from the first of them that answers holding one (fabric-recovery.md "Nothing alive": "If
/// any node answers, it is not this case"); when none does, the caller adopts its own clock
/// ([`OWN_CLOCK_NOTHING_ALIVE`]). Every node is asked at once, so the answer comes within one read's
/// budget however many the store names. It judges nothing and publishes nothing until it has:
/// `contacts` are the births its durable map names, and the view of the membership it joined adds
/// whoever it hears. A member adopts from whoever answers; the lineage descends from the
/// fabric-primary whichever node that is.
///
/// Returns the `path.name` of the node it adopted from, or a named refusal listing each node
/// tried with the outcome of its read.
pub(crate) async fn adopt_from_reachable(
    time: &RafkaTime,
    me: &crate::model::PathName,
    client: &rdm_transport::NodeRpcClient,
    resolver: &rdm_transport::LiveNodeResolver,
    topology_view: &rdm_transport::topology::TopologyView,
    contacts: Vec<crate::reenter::MapNode>,
    parent: &tracing::Span,
) -> Result<String, String> {
    use rdm_transport::NodeTarget;
    let span = tracing::info_span!(parent: parent, "rdm.mesh.entry.resolve.via-recovery-pull", node = %me, contacts = contacts.len(), served_by = tracing::field::Empty, attempts = tracing::field::Empty, outcome = tracing::field::Empty);
    let outcome = async {
        let mut tried: std::collections::BTreeMap<String, String> = Default::default();
        let mut attempts = 0u32;
        let mut candidates = contacts;
        for d in topology_view.digest_view.current(topology_view.digest_view.staleness_floor()) {
            if d.node.name != *me && !candidates.iter().any(|c| c.node_id == d.node.node_id) {
                candidates.push(crate::reenter::MapNode {
                    node_id: d.node.node_id.clone(),
                    name: d.node.name.clone(),
                    endpoint_id: d.node.endpoint_id.clone(),
                    transport_addr: d.node.transport_addr,
                    incarnation: d.node.incarnation.clone(),
                    transition: None,
                    ready: false,
                    data_dir: None,
                    admin_api_base: None,
                });
            }
        }
        candidates.retain(|c| c.name != *me);
        candidates.sort_by_key(|c| (c.name.mesh != me.mesh, c.name.to_string()));
        for c in &candidates {
            if let Some(r) = c.resolved() {
                resolver.apply(r, None);
            }
        }
        let me_s = me.to_string();
        let mut reads: futures_util::stream::FuturesUnordered<_> = candidates
            .iter()
            .map(|c| {
                let me_s = me_s.clone();
                async move { (c, crate::topology_read::read_topology(client, &NodeTarget::ExactNode(c.node_id.clone()), &me_s, Some(&c.name.mesh), None).await) }
            })
            .collect();
        while let Some((c, read)) = futures_util::StreamExt::next(&mut reads).await {
            attempts += 1;
            match read {
                Ok(read) => match read.rafka_time {
                    Some(t) => {
                        adopt_pulled(time, Puller::Member, &me_s, Served { via: "get-topology", served_by: &c.name.to_string(), ms: t.ms, kind: t.kind }).expect("a member adopts from whoever answers");
                        span.record("attempts", attempts);
                        return Ok(c.name.to_string());
                    }
                    None => {
                        tried.insert(c.name.to_string(), "its answer carries no rafka-time".into());
                    }
                },
                Err(e) => {
                    tried.insert(c.name.to_string(), e.to_string());
                }
            }
        }
        span.record("attempts", attempts);
        Err(format!("{me}: no node it could reach holds rafka-time (tried {} reads of {tried:?})", attempts))
    }
    .instrument(span.clone())
    .await;
    match &outcome {
        Ok(by) => {
            span.record("served_by", by.as_str());
            span.record("outcome", "adopted");
        }
        Err(_) => {
            span.record("outcome", "no-live-node");
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    // @feature: rafka-time
    #[test]
    fn the_day0_root_adopts_its_own_clock_once_and_cloned_handle_shares_one_clock() {
        // docs/architecture/fabric-node-hooks.md:150 — "RDM exposes it to the application through
        // one public, cloneable `RafkaTime` handle with `now_ms()`. The app gets the handle from
        // the running node returned by RDM's start, and from `NodeReadyCtx`. It is the same
        // instance every RDM stamp reads; there is no second clock."
        let capture = crate::span_capture::Capture::new("rafka-time-own-clock-unit");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        let time = RafkaTime::unadopted();
        let app_handle = time.clone();
        assert_eq!(app_handle.try_now_ms(), None);

        let adopted = adopt_own_clock(&tracing::Span::none(), &time, "mesh1.admin.1", OWN_CLOCK_DAY0_ROOT);
        assert_eq!(adopted.previous_ms, None);
        assert!(app_handle.now_ms() >= adopted.reference_ms);

        // Advancing `time` through `adopt` is immediately visible on the cloned `app_handle`
        // ("there is no second clock").
        let next_ref = adopted.reference_ms + 50_000;
        time.adopt(next_ref);
        assert!(app_handle.now_ms() >= next_ref);

        let spans = capture.named("rdm.mesh.entry.update.via-rafka-time-adopted");
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].attributes.get("node").map(String::as_str), Some("mesh1.admin.1"));
        assert_eq!(spans[0].attributes.get("source").map(String::as_str), Some("own-clock"));
        assert_eq!(spans[0].attributes.get("reason").map(String::as_str), Some(OWN_CLOCK_DAY0_ROOT));
    }
}
