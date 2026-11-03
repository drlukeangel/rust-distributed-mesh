//! The Build log as a caller reads it: the one shared read of every cell that asks "the Build as the
//! fabric holds it".
//!
//! The Build records are on the fabric's store root, its `builds.jsonl`, which the fabric-primary
//! writes and every admin that needs a Build reads (Q292; build.md §5.3). So the read is the store's:
//! [`log_read`] reads the named Build's committed records from the estate's fabric store root
//! (`build_state::read_committed`, the read the admins make), [`log_fact`] awaits a typed fact of it
//! through the client's `await_fact`, and [`build_fact`] awaits a fact of the Build the records and its
//! events fold to (`BuildProjection::of` over the record and the Build's events on the same store root,
//! `events.jsonl`, build.md §5).
//!
//! It lives in the test binary every cell links, not in `src/typed.rs`: the facts' types and the read
//! are the node-admin core's, and the scenario crate's shipped code reaches RDM only through the public
//! interfaces (`tools/mesh-audit/src/deps.rs`, rule 4; the core depends on `rafka-node-admin-client`,
//! so the client cannot host it either).

use rdm_client::{await_fact, CallEnd};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{fold, read_committed, BuildFact, BuildProjection, BuildState, BuildStateError, StepOutcome};
use rdm_admin::events_storage::Event;
use rdm_test::estate::Estate;
use std::time::Duration;

/// The fabric's Build records, `builds.jsonl` on the estate's fabric store root.
fn builds_file(estate: &Estate) -> std::path::PathBuf {
    estate.fabric_store_root().join(rdm_admin::store_root::BUILDS_FILE)
}

/// The node.id Build `id`'s record minted for the node it creates at `at` (D58: the fabric-primary
/// mints it at acceptance and writes it in the record). Panics naming the Build when it creates none
/// there.
pub async fn created_id(estate: &Estate, id: &str, at: &rdm_core::PathName) -> rdm_core::NodeId {
    let facts = log_read(estate, id).await.unwrap_or_else(|end| panic!("the record of Build {id}: {end}"));
    let accepted = facts.iter().find_map(|f| match f { BuildFact::Accepted(a) => Some(a), _ => None }).unwrap_or_else(|| panic!("Build {id} holds no accepted record"));
    accepted.topology.created_id(at).cloned().unwrap_or_else(|| panic!("Build {id} creates no node at {at:?}: {:?}", accepted.topology.created))
}

/// The committed records of Build `id` on the estate's fabric store root now, read whole. A store that
/// holds no record of the Build ends the call `Indeterminate` (`unknown build`), naming it; a store that
/// cannot be read ends it `Indeterminate` with the read's own words.
pub async fn log_read(estate: &Estate, id: &str) -> Result<Vec<BuildFact>, CallEnd> {
    let file = builds_file(estate);
    let build = BuildId::parse(id).unwrap();
    let read = tokio::task::spawn_blocking({
        let file = file.clone();
        move || read_committed(&file, Some(&build))
    })
    .await
    .expect("the Build records read finishes");
    read.map_err(|e| {
        let reason = match e {
            BuildStateError::UnknownBuild(b) => format!("{}: unknown build {b}", file.display()),
            other => format!("{}: {other}", file.display()),
        };
        CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason }
    })
}

/// Await the typed fact `holds` of Build `id` in the fabric's Build records. `fact` names it. The read
/// is `await_fact`'s: records that do not hold it within `within` end the call `Indeterminate`, naming
/// the fact, the store and the last thing its records held or how the last read ended.
pub async fn log_fact(estate: &Estate, id: &str, fact: &str, within: Duration, holds: impl Fn(&[BuildFact]) -> bool) -> Result<Vec<BuildFact>, CallEnd> {
    let at = builds_file(estate).display().to_string();
    let report = |facts: &Vec<BuildFact>| format!("{} facts", facts.len());
    await_fact("build log", &at, &format!("{fact} of Build {id}"), within, || log_read(estate, id), |facts: &Vec<BuildFact>| holds(facts), report).await
}

/// The events of Build `id` on the estate's fabric store root, `events.jsonl`, in event.id order
/// (build.md §5): its `rdm.build.started`, `rdm.build.dispatched`, `rdm.build.step` and
/// `rdm.build.ended`.
pub fn build_events(estate: &Estate, id: &str) -> Vec<Event> {
    let build = BuildId::parse(id).unwrap();
    let file = estate.fabric_store_root().join(rdm_admin::store_root::EVENTS_FILE);
    let mut events: Vec<Event> = rdm_admin::store_log::read_file(&file)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|l| serde_json::from_value::<Event>(l.record).ok())
        .filter(|e| e.build_id().as_ref() == Some(&build))
        .collect();
    events.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
    events
}

/// How many runs Build `id` has had: its `rdm.build.started` events on the estate's fabric store root
/// (a submission and each resubmission writes one, build.md §5).
pub fn runs(estate: &Estate, id: &str) -> u64 {
    build_events(estate, id).iter().filter(|e| e.r#type == rdm_admin::events_storage::BUILD_STARTED).count() as u64
}

/// Build `id` as `facts` and its events on the estate's fabric store root fold it, `None` while the
/// facts hold no acceptance of it.
pub fn projection(estate: &Estate, facts: &[BuildFact], id: &str) -> Option<BuildProjection> {
    let accepted = fold(facts).remove(&BuildId::parse(id).unwrap())?;
    Some(BuildProjection::of(&accepted, &build_events(estate, id)))
}

/// Await the fact `holds` of the Build `id` as the fabric's Build records fold it. `fact` names it;
/// records that do not fold to it within `within` end the call `Indeterminate`, naming the fact, the
/// store and the Build they last held (state, steps, every failed step with its reason) or
/// how the last read ended.
pub async fn build_fact(estate: &Estate, id: &str, fact: &str, within: Duration, holds: impl Fn(&BuildProjection) -> bool) -> Result<BuildProjection, CallEnd> {
    let at = builds_file(estate).display().to_string();
    let read = || async {
        let facts = log_read(estate, id).await?;
        projection(estate, &facts, id).ok_or_else(|| CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: format!("{} facts of the Build and no acceptance of it", facts.len()) })
    };
    let report = |p: &BuildProjection| {
        let last = p.steps.last().map(|r| format!("mesh {}, operation {:?}, step {}", r.mesh, r.operation, r.step)).unwrap_or_else(|| "none".into());
        let failed: Vec<String> = p.steps.iter().filter_map(|r| if let StepOutcome::Failed { reason, .. } = &r.outcome { Some(format!("mesh {}, operation {:?}, step {}: {reason}", r.mesh, r.operation, r.step)) } else { None }).collect();
        format!("state {:?}, {} step events, the last {last}; failed {failed:?}; unfinished {:?}", p.state, p.steps.len(), p.unfinished)
    };
    await_fact("build log", &at, &format!("{fact} of Build {id}"), within, read, |p: &BuildProjection| holds(p), report).await
}

/// Build `id` as the fabric's Build records hold it now, as the JSON the view serializes to; records
/// that hold no acceptance of it are a defect of the cell, named.
pub async fn build_value(estate: &Estate, id: &str) -> serde_json::Value {
    let at = builds_file(estate).display().to_string();
    let facts = log_read(estate, id).await.unwrap_or_else(|end| panic!("the Build log of {at} does not hold Build {id}: {end}"));
    let build = projection(estate, &facts, id).unwrap_or_else(|| panic!("the Build log of {at} holds {} facts of Build {id} and no acceptance of it", facts.len()));
    serde_json::to_value(build).expect("a Build view serializes")
}

/// Await Build `id` complete in the fabric's Build records and its events (its `rdm.build.ended`
/// `complete`). A Build that folds to `failed` or `incomplete` fails the cell with the Build; records
/// that do not fold to it within `within` end the call naming the fact, the store and the Build they
/// last held.
pub async fn complete(estate: &Estate, id: &str, within: Duration) -> BuildProjection {
    build_fact(estate, id, "complete", within, |b| match b.state {
        BuildState::Complete => true,
        BuildState::Failed | BuildState::Incomplete => panic!("Build {id} ended {:?}: {b:#?}", b.state),
        _ => false,
    })
    .await
    .unwrap_or_else(|end| panic!("{end}"))
}

/// [`build_value`] as the soak driver reads a Build (`rdm_test::soak::BuildRead`): the Build as the
/// fabric's Build records hold it, as the JSON the projection serializes to. A Build whose acceptance is
/// not yet on the store, or a store that cannot be read, is read again for up to 60 s.
pub fn build_read<'a>(estate: &'a Estate, id: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = serde_json::Value> + Send + 'a>> {
    Box::pin(async move {
        let until = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let read = log_read(estate, id).await.and_then(|facts| {
                let n = facts.len();
                projection(estate, &facts, id).ok_or_else(|| CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: format!("{n} facts of Build {id} and no acceptance of it") })
            });
            match read {
                Ok(build) => return serde_json::to_value(build).expect("a Build view serializes"),
                Err(end) if std::time::Instant::now() >= until => panic!("the fabric's Build records do not hold Build {id} within 60 s: {end}"),
                Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    })
}

/// The `rdm.node.investigated` events of `node_id` on the estate's fabric store root, `events.jsonl`,
/// in file order (build.md §5): each as JSON, the CloudEvents envelope with its `data`.
pub fn node_investigated(estate: &Estate, node_id: &str) -> Vec<serde_json::Value> {
    let file = estate.fabric_store_root().join(rdm_admin::store_root::EVENTS_FILE);
    rdm_admin::store_log::read_file(&file)
        .unwrap_or_default()
        .into_iter()
        .map(|l| l.record)
        .filter(|e| e["type"] == "rdm.node.investigated" && e["subject"] == node_id)
        .collect()
}
