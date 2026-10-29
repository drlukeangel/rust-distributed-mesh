//! The Build-facts read (`FetchBuildFacts` and `FetchCurrentBuild`, op `0x1F`; node-rpc-envelope.md
//! "Build-facts, op `0x1F`, rdm"; i143 R-G6).
//!
//! A node-admin that does not hold the facts of a Build asks another node-admin for them. The responder ([`BuildFactsDoor`]) reads its own local Build log
//! once and streams the facts of the one Build, packed into Build messages
//! by ([`crate::fabric_builds::encode_chunks`]): it provides what it holds and decides nothing.
//! `FetchBuildFacts` names the Build; `FetchCurrentBuild` names none, and the responder streams the
//! facts of its current accepted Build (its builds store read last in, first out, R-BI3), whose id
//! `End` carries, or answers `NoCurrentBuild`.
//! The caller ([`fetch_build_facts`], [`fetch_current_build`]) returns the facts only for a stream that ended with its `End`,
//! whose chunks all arrived and whose count agrees; the facts then go through the absorption
//! a build.attempt.run's intent goes through ([`crate::build_state::LocalBuildLog::absorb_facts`]).

use crate::build::BuildId;
use crate::build_state::{BuildFact, BuildStateAdapter};
use crate::fabric_builds::{encode_chunks, BuildMessage};
use rdm_transport::stream::{ReplySink, NotStarted, StreamFailure, StreamItem};
use rdm_transport::{NodeRpcClient, NodeTarget, ServerBuilder};
use rdm_core::build_facts::{BuildFacts, BuildFactsReply, BuildFactsRequest};
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::RpcOutcome;
use rdm_core::streaming::FrameKind;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::Instrument as _;

/// What a node-admin answers a Build-facts read from: its own Build state, and what it needs to say
/// whether its own holdings are whole.
pub struct BuildFactsDoor {
    /// This admin's `path.name`.
    pub me: crate::model::PathName,
    /// The Build state this admin holds (its local log, through whatever decorates it).
    pub builds: Arc<dyn BuildStateAdapter>,
    /// The attempt floor this admin's entry named, when it was served one.
    pub floor: Arc<std::sync::Mutex<Option<(BuildId, u32)>>>,
}

/// The door a running admin fills once it holds a view.
pub type BuildFactsSlot = Arc<OnceLock<Arc<BuildFactsDoor>>>;

impl BuildFactsDoor {
    /// Answer a Build-facts read: stream the facts held for the requested Build (the one named, or
    /// this admin's current accepted Build) in chunks and end with whether the holdings are whole.
    pub async fn serve(&self, req: BuildFactsRequest, sink: ReplySink<BuildFacts, NotStarted>) -> BuildFactsReply {
        let me = self.builds.held_name().unwrap_or_else(|| self.me.clone());
        let span = tracing::info_span!(
            "rdm.node_admin.build.serve.via-fetch-facts",
            node = %me,
            request = req.op(),
            build_id = tracing::field::Empty,
            mesh = tracing::field::Empty,
            facts = tracing::field::Empty,
            chunks = tracing::field::Empty,
            complete = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        async move {
            let span = tracing::Span::current();
            // The meshes whose sections the acceptance travels as (Q284): every mesh it names, or the
            // one a section read names.
            let mesh = match &req {
                BuildFactsRequest::FetchBuildSection { mesh, .. } => {
                    span.record("mesh", mesh.as_str());
                    Some(mesh.clone())
                }
                _ => None,
            };
            let (id, build_id) = match req {
                BuildFactsRequest::FetchBuildFacts { build_id } | BuildFactsRequest::FetchBuildSection { build_id: Some(build_id), .. } => {
                    span.record("build_id", build_id.as_str());
                    match BuildId::parse(&build_id) {
                        Ok(id) => (id, build_id),
                        Err(e) => {
                            span.record("outcome", "malformed-build-id");
                            tracing::info_span!("rdm.node_admin.build.reject.via-malformed-build-id", node = %me, build_id = %build_id, reason = %e, "otel.kind" = "internal")
                                .in_scope(|| tracing::info!("the facts read is refused: its Build id is not a Crockford id"));
                            return rdm_core::reply::build_facts::malformed(rdm_core::outcome::MalformedKind::Corrupt);
                        }
                    }
                }
                // The current accepted Build: this admin's builds store read last in, first out (R-BI3).
                BuildFactsRequest::FetchCurrentBuild | BuildFactsRequest::FetchBuildSection { build_id: None, .. } => match self.builds.current_build().await {
                    Ok(current) => match current.into_build_id() {
                        Some(id) => {
                            let build_id = id.to_string();
                            span.record("build_id", build_id.as_str());
                            (id, build_id)
                        }
                        None => {
                            span.record("outcome", "no-current-build");
                            return rdm_core::reply::build_facts::no_current_build(format!("{me}: holds no current Build (no Build in its builds store is in progress or completed)"));
                        }
                    },
                    Err(e) => {
                        span.record("outcome", "not-ready");
                        return rdm_core::reply::build_facts::not_ready(format!("{me}: its builds store could not be read for the current Build: {e}"));
                    }
                },
            };
            // One read of this admin's log: the stream is a snapshot of it.
            let held: Vec<BuildFact> = match self.builds.facts().await {
                Ok(all) => all.into_iter().filter(|f| *f.build_id() == id).collect(),
                Err(e) => {
                    span.record("outcome", "not-ready");
                    return rdm_core::reply::build_facts::not_ready(format!("{me}: its Build log could not be read: {e}"));
                }
            };
            if held.is_empty() {
                span.record("outcome", "unknown-build");
                return rdm_core::reply::build_facts::unknown_build(build_id);
            }
            let wanted = mesh.map(|m| std::collections::BTreeSet::from([m]));
            let held = crate::fabric_builds::sectioned(held, wanted.as_ref());
            // A section read is whole only with the acceptance it asked for: a section of its mesh, or,
            // for a Build that names no such mesh, the Build's section with none.
            let sectioned = wanted.as_ref().is_none_or(|w| held.iter().any(|f| matches!(f, BuildFact::Accepted(a) if a.topology.meshes.keys().any(|m| w.contains(m)) || (a.topology.meshes.is_empty() && !w.iter().any(|m| a.topology.names_mesh(m))))));
            let facts = held.len() as u32;
            let (messages, refused) = encode_chunks(held);
            let floor = self.floor.lock().unwrap().clone();
            let whole = sectioned && refused.is_empty() && crate::admin::node_admin_not_ready(&me, &*self.builds, floor).await.is_none();
            for e in &refused {
                tracing::info_span!("rdm.node_admin.build.reject.via-unservable-fact", node = %me, build_id = %build_id, detail = %e)
                    .in_scope(|| tracing::info!("a held Build fact fits no chunk: this answer is not complete"));
            }
            let chunks = messages.len() as u32;
            let mut sink = match sink.started(BuildFactsReply::Started).await {
                Ok(s) => s,
                Err(e) => {
                    span.record("outcome", format!("caller-gone: {e:?}").as_str());
                    return BuildFactsReply::End { build_id, facts: 0, chunks: 0, complete: false };
                }
            };
            for (i, m) in messages.into_iter().enumerate() {
                let frame = BuildFactsReply::Facts { build_id: build_id.clone(), chunk_index: i as u32, chunk_count: chunks, facts: m.to_vec() };
                if let Err(e) = sink.data(frame).await {
                    span.record("outcome", format!("caller-gone: {e:?}").as_str());
                    return BuildFactsReply::End { build_id, facts: 0, chunks: 0, complete: false };
                }
            }
            span.record("facts", facts);
            span.record("chunks", chunks);
            span.record("complete", whole);
            span.record("outcome", "served");
            BuildFactsReply::End { build_id, facts: facts - refused.len() as u32, chunks, complete: whole }
        }
        .instrument(span)
        .await
    }
}

/// Serve the Build-facts read (`FetchBuildFacts`, `FetchCurrentBuild`) on this admin. Until `slot` is
/// filled the admin holds no Build state and a read is `NotReady` by name.
pub fn serve(b: ServerBuilder, slot: BuildFactsSlot) -> ServerBuilder {
    b.serve_stream::<BuildFacts, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: BuildFactsRequest, sink| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(rdm_core::reply::build_facts::not_ready("this admin holds no Build state yet".into()));
            };
            Ok(door.serve(req, sink).await)
        }
    })
}

/// The facts of one Build, read whole from one responder.
#[derive(Debug)]
pub struct Fetched {
    /// The Build.
    pub build_id: BuildId,
    /// The facts read.
    pub facts: Vec<BuildFact>,
    /// The number of chunks the stream took.
    pub chunks: u32,
    /// The responder's statement that its holdings of the Build are whole (`End.complete`).
    pub complete: bool,
}

/// Why a Build-facts read produced no facts to absorb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchFailure {
    /// The responder cannot answer yet.
    NotReady(String),
    /// The responder holds no fact of the Build.
    UnknownBuild(String),
    /// The responder holds no current Build (`FetchCurrentBuild` answered `NoCurrentBuild`).
    NoCurrentBuild(String),
    /// The responder refused by another name, or does not serve the op.
    Refused(String),
    /// The responder could not be reached.
    Unreached(String),
    /// The stream ended before its `End`, or failed.
    Interrupted(String),
    /// The stream ended but its chunks, count or frames disagree with its `End`.
    Incomplete(String),
}

impl std::fmt::Display for FetchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotReady(r) => write!(f, "not ready: {r}"),
            Self::UnknownBuild(r) => write!(f, "unknown build: {r}"),
            Self::NoCurrentBuild(r) => write!(f, "no current build: {r}"),
            Self::Refused(r) => write!(f, "refused: {r}"),
            Self::Unreached(r) => write!(f, "unreached: {r}"),
            Self::Interrupted(r) => write!(f, "interrupted: {r}"),
            Self::Incomplete(r) => write!(f, "incomplete: {r}"),
        }
    }
}

impl FetchFailure {
    /// The reason arm, as the span names it.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::NotReady(_) => "not-ready",
            Self::UnknownBuild(_) => "unknown-build",
            Self::NoCurrentBuild(_) => "no-current-build",
            Self::Refused(_) => "refused",
            Self::Unreached(_) => "unreached",
            Self::Interrupted(_) => "interrupted",
            Self::Incomplete(_) => "incomplete",
        }
    }
}

/// `FetchBuildFacts` (`0x1F`, `docs/architecture/node-rpc-envelope.md` "Build-facts, op `0x1F`,
/// rdm") to `target` for `build_id`. Returns the facts only for a stream whose `End` arrived with
/// every chunk `0..chunks` present once and a fact count that agrees; anything less is a failure that
/// names what was missing, and the caller absorbs nothing from it.
pub async fn fetch_build_facts(client: &NodeRpcClient, target: &NodeTarget, build_id: &BuildId) -> Result<Fetched, FetchFailure> {
    read_facts(client, target, &BuildFactsRequest::FetchBuildFacts { build_id: build_id.to_string() }, Some(build_id)).await
}

/// `FetchBuildSection` (Q284) to `target`: the facts of `build_id` with its acceptance as `mesh`'s
/// section only, counted as [`fetch_build_facts`] counts them.
/// With no Build named, the target's current accepted Build, whose id the stream's `End` carries.
pub async fn fetch_build_section(client: &NodeRpcClient, target: &NodeTarget, build_id: Option<&BuildId>, mesh: &str) -> Result<Fetched, FetchFailure> {
    read_facts(client, target, &BuildFactsRequest::FetchBuildSection { build_id: build_id.map(ToString::to_string), mesh: mesh.to_string() }, build_id).await
}

/// `FetchCurrentBuild` to `target`: the facts of its current accepted Build, whose id the stream's
/// `End` carries, counted as [`fetch_build_facts`] counts them; `NoCurrentBuild` when it holds none.
pub async fn fetch_current_build(client: &NodeRpcClient, target: &NodeTarget) -> Result<Fetched, FetchFailure> {
    read_facts(client, target, &BuildFactsRequest::FetchCurrentBuild, None).await
}

/// One Build-facts read of `req` to `target`. `asked` is the Build the request names; with none, the
/// Build is the one the stream's frames name, and every frame names the same one.
async fn read_facts(client: &NodeRpcClient, target: &NodeTarget, req: &BuildFactsRequest, asked: Option<&BuildId>) -> Result<Fetched, FetchFailure> {
    let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Overall(Duration::from_secs(5)), ..Default::default() };
    let mut stream = match client.call_stream::<BuildFacts>(target, req, &opts).await {
        // The facts are the responder's durable Build log, absorbed by the caller only from a whole
        // read and recovered by reading them again: reading a frame is its recovery (node-rpc.md §43).
        Ok((s, _)) => s.read_only(),
        Err((RpcOutcome::Reply(r), _)) => return Err(refusal_of(r.value().clone())),
        Err((RpcOutcome::Unserved(u), _)) => return Err(FetchFailure::Refused(format!("the node does not serve Build facts: {u:?}"))),
        Err((other, _)) => return Err(FetchFailure::Unreached(format!("the call ended {}", other.name()))),
    };
    // The Build the read is of: the one asked, or the one the first frame names.
    let mut build_id: Option<String> = asked.map(ToString::to_string);
    let mut chunks: Vec<Option<Vec<BuildFact>>> = Vec::new();
    let mut end = None;
    while let Some(item) = stream.next().await {
        match item {
            StreamItem::Frame(FrameKind::Refusal(_), r) => return Err(refusal_of(r)),
            StreamItem::Frame(_, BuildFactsReply::Started) => {}
            StreamItem::Frame(_, BuildFactsReply::Facts { build_id: b, chunk_index, chunk_count, facts }) => {
                let read = build_id.get_or_insert_with(|| b.clone());
                if b != *read {
                    return Err(FetchFailure::Incomplete(format!("a chunk of Build {b} arrived in the read of {read}")));
                }
                if chunks.is_empty() {
                    chunks = vec![None; chunk_count as usize];
                }
                if chunks.len() != chunk_count as usize || chunk_index as usize >= chunks.len() {
                    return Err(FetchFailure::Incomplete(format!("chunk {chunk_index} of {chunk_count} disagrees with the {} chunks announced first", chunks.len())));
                }
                let message = BuildMessage::from_bytes(&facts).map_err(|e| FetchFailure::Incomplete(format!("chunk {chunk_index} does not decode: {e}")))?;
                if chunks[chunk_index as usize].replace(message.facts).is_some() {
                    return Err(FetchFailure::Incomplete(format!("chunk {chunk_index} arrived twice")));
                }
            }
            StreamItem::Frame(_, BuildFactsReply::End { build_id: b, facts, chunks: n, complete }) => end = Some((b, facts, n, complete)),
            StreamItem::Frame(_, other) => return Err(FetchFailure::Refused(format!("an unexpected frame in the stream: {}", other.name()))),
            StreamItem::Failed(StreamFailure::Unserved(u)) => return Err(FetchFailure::Refused(format!("the node does not serve Build facts: {u:?}"))),
            StreamItem::Failed(f) => return Err(FetchFailure::Interrupted(format!("the stream failed before its end: {f:?}"))),
        }
    }
    let Some((b, facts, n, complete)) = end else {
        return Err(FetchFailure::Interrupted(format!("the stream ended after {} of {} chunks without its End frame", chunks.iter().flatten().count(), chunks.len())));
    };
    let read = build_id.get_or_insert_with(|| b.clone());
    if b != *read {
        return Err(FetchFailure::Incomplete(format!("the End names Build {b}, the read was of {read}")));
    }
    let build_id = BuildId::parse(&b).map_err(|e| FetchFailure::Incomplete(format!("the End names Build {b}, which is not a Build id: {e}")))?;
    if n as usize != chunks.len() || chunks.iter().any(Option::is_none) {
        return Err(FetchFailure::Incomplete(format!("the End counts {n} chunks, {} arrived of {} announced", chunks.iter().flatten().count(), chunks.len())));
    }
    let all: Vec<BuildFact> = chunks.into_iter().flatten().flatten().collect();
    if all.len() as u32 != facts {
        return Err(FetchFailure::Incomplete(format!("the End counts {facts} facts, {} decoded", all.len())));
    }
    Ok(Fetched { build_id, facts: all, chunks: n, complete })
}

fn refusal_of(r: BuildFactsReply) -> FetchFailure {
    match r {
        BuildFactsReply::NotReady { reason } => FetchFailure::NotReady(reason),
        BuildFactsReply::UnknownBuild { build_id } => FetchFailure::UnknownBuild(format!("the responder holds no fact of {build_id}")),
        BuildFactsReply::NoCurrentBuild { reason } => FetchFailure::NoCurrentBuild(reason),
        BuildFactsReply::PeerUnresolved { reason } => FetchFailure::Unreached(format!("peer unresolved: {reason}")),
        other => FetchFailure::Refused(format!("{}: {other:?}", other.name())),
    }
}
