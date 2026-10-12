//! A node-admin's own Build log as a caller reads it: the one shared read of every cell that asks
//! "the Build as the admin at X holds it".
//!
//! The Build family (`build.get`) is the fabric-primary's alone. Every node-admin keeps its own Build
//! journal and serves it over `FetchBuildFacts` (op `0x1F`; `BuildFactsDoor::serve`, build.md "Each
//! node-admin keeps its own Build journal"), so a cell that asks what one exact admin holds reads that
//! admin's log here: [`log_read`] is the one read, [`log_fact`] awaits a typed fact of it through the
//! client's `await_fact`, and [`build_fact`] awaits a fact of the Build the log folds to (the view
//! `GET /api/builds?id=` served: `build_state::fold` over the same log).
//!
//! It lives in the test binary every cell links, not in `src/typed.rs`: the facts' types and the read
//! are the node-admin core's, and the scenario crate's shipped code reaches RDM only through the public
//! interfaces (`tools/mesh-audit/src/deps.rs`, rule 4; the core depends on `rafka-node-admin-client`,
//! so the client cannot host it either).

use rafka_node_admin_client::{await_fact, BuildCarrier, CallEnd};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_facts_read::{fetch_build_facts, FetchFailure};
use rafka_node_admin_core::build_state::{fold, BuildFact, BuildProjection};
use rafka_node_rpc::NodeTarget;
use std::time::Duration;

/// How a read of a node-admin's Build log over `FetchBuildFacts` ended, as the call ends: a responder
/// that refused by another name or does not serve the op is `Rejected`; any other failure is a read that
/// proved nothing, `Indeterminate`, and carries the failure's own words.
fn log_read_ended(failure: FetchFailure) -> CallEnd {
    match failure {
        FetchFailure::Refused(_) => CallEnd::Rejected { reason: failure.reason().into(), detail: failure.to_string() },
        other => CallEnd::Indeterminate { reason: other.to_string() },
    }
}

/// The facts the node-admin `carrier` starts at holds of Build `id` now, in one `FetchBuildFacts` read.
/// An admin that holds no fact of the Build ends the call `Indeterminate` (`unknown build`), naming it.
pub async fn log_read(carrier: &BuildCarrier, id: &str) -> Result<Vec<BuildFact>, CallEnd> {
    let target = NodeTarget::CurrentPath(carrier.fabric_primary().clone());
    fetch_build_facts(carrier.client(), &target, &BuildId(id.into())).await.map(|fetched| fetched.facts).map_err(log_read_ended)
}

/// Await the typed fact `holds` of Build `id` in the Build log of the node-admin `carrier` starts at.
/// `fact` names it. The read is `await_fact`'s: an admin whose log does not hold it within `within` ends
/// the call `Indeterminate`, naming the fact, the admin and the last thing its log held or how the last
/// read ended.
pub async fn log_fact(carrier: &BuildCarrier, id: &str, fact: &str, within: Duration, holds: impl Fn(&[BuildFact]) -> bool) -> Result<Vec<BuildFact>, CallEnd> {
    let at = carrier.fabric_primary().to_string();
    let report = |facts: &Vec<BuildFact>| {
        let last = facts.iter().rev().find_map(|f| if let BuildFact::Step(r) = f { Some(format!("{} {} of attempt {}", r.operation, r.step, r.attempt)) } else { None }).unwrap_or_else(|| "none".into());
        format!("{} facts, the last step receipt {last}", facts.len())
    };
    await_fact("build log", &at, &format!("{fact} of Build {id}"), within, || log_read(carrier, id), |facts: &Vec<BuildFact>| holds(facts), report).await
}

/// Build `id` as `facts` fold it, `None` while they hold no acceptance of it.
pub fn projection(facts: &[BuildFact], id: &str) -> Option<BuildProjection> {
    fold(facts).remove(&BuildId(id.into()))
}

/// Await the fact `holds` of the Build `id` as the Build log of the node-admin `carrier` starts at folds
/// it: the Build as that admin holds it.
pub async fn build_fact(carrier: &BuildCarrier, id: &str, fact: &str, within: Duration, holds: impl Fn(&BuildProjection) -> bool) -> Result<BuildProjection, CallEnd> {
    let facts = log_fact(carrier, id, fact, within, |facts| projection(facts, id).is_some_and(|p| holds(&p))).await?;
    Ok(projection(&facts, id).expect("the facts that held the awaited fact fold to the Build"))
}

/// Build `id` as the node-admin `carrier` starts at holds it now, as the JSON the view serializes to; a
/// log that holds no acceptance of it is a defect of the cell, named.
pub async fn build_value(carrier: &BuildCarrier, id: &str) -> serde_json::Value {
    let at = carrier.fabric_primary();
    let facts = log_read(carrier, id).await.unwrap_or_else(|end| panic!("the Build log of {at} does not hold Build {id}: {end}"));
    let build = projection(&facts, id).unwrap_or_else(|| panic!("the Build log of {at} holds {} facts of Build {id} and no acceptance of it", facts.len()));
    serde_json::to_value(build).expect("a Build view serializes")
}
