//! The Build-facts read (`FetchBuildFacts`, op `0x1F`; node-rpc-envelope.md "Build facts, op
//! `0x1F`, rdm"; i143 R-G6).
//!
//! A node-admin that holds the Fabric record but not the facts of the Build it points at asks
//! another node-admin for them. The responder ([`BuildFactsDoor`]) reads its own local Build log
//! once and streams the facts of the one Build, packed by the method the Build topic's catch-up
//! uses ([`crate::fabric_builds::encode_chunks`]): it provides what it holds and decides nothing.
//! The caller ([`fetch_build_facts`]) returns the facts only for a stream that ended with its `End`,
//! whose chunks all arrived and whose count agrees; the facts then go through the same absorption
//! the Build topic's facts go through ([`crate::build_state::LocalBuildLog::absorb_facts`]).

use crate::build::BuildId;
use crate::build_state::{BuildFact, BuildStateAdapter};
use crate::fabric_builds::{encode_chunks, BuildMessage};
use rafka_node_rpc::stream::{ReplySink, NotStarted, StreamItem};
use rafka_node_rpc::{NodeRpcClient, NodeTarget, ServerBuilder};
use rafka_node_rpc_contract::build_facts::{BuildFacts, BuildFactsReply, BuildFactsRequest};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::streaming::FrameKind;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::Instrument as _;

/// What a node-admin answers a Build-facts read from: its own Build state, and what it needs to say
/// whether its own holdings are whole.
pub struct BuildFactsDoor {
    pub me: crate::model::PathName,
    /// The Build state this admin holds (its local log, through whatever decorates it).
    pub builds: Arc<dyn BuildStateAdapter>,
    /// This admin's `Fabric.build_id` holder.
    pub accepted: Arc<crate::accepted::AcceptedStore>,
    /// The attempt floor this admin's entry named, when it was served one.
    pub floor: Arc<std::sync::Mutex<Option<(BuildId, u32)>>>,
}

pub type BuildFactsSlot = Arc<OnceLock<Arc<BuildFactsDoor>>>;

impl BuildFactsDoor {
    pub async fn serve(&self, req: BuildFactsRequest, sink: ReplySink<BuildFacts, NotStarted>) -> BuildFactsReply {
        let BuildFactsRequest::FetchBuildFacts { build_id } = req;
        let span = tracing::info_span!(
            "rdm.node_admin.build.serve.via-fetch-facts",
            node = %self.me,
            build_id = %build_id,
            facts = tracing::field::Empty,
            chunks = tracing::field::Empty,
            complete = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        async move {
            let span = tracing::Span::current();
            let id = BuildId(build_id.clone());
            // One read of this admin's log: the stream is a snapshot of it.
            let held: Vec<BuildFact> = match self.builds.facts().await {
                Ok(all) => all.into_iter().filter(|f| *f.build_id() == id).collect(),
                Err(e) => {
                    span.record("outcome", "not-ready");
                    return BuildFactsReply::NotReady { reason: format!("{}: its Build log could not be read: {e}", self.me) };
                }
            };
            if held.is_empty() {
                span.record("outcome", "unknown-build");
                return BuildFactsReply::UnknownBuild { build_id };
            }
            let facts = held.len() as u32;
            let (messages, refused) = encode_chunks(held);
            let floor = self.floor.lock().unwrap().clone();
            let whole = refused.is_empty() && crate::admin::hydration_blocker(&self.me, &self.accepted, &*self.builds, floor).await.is_none();
            for e in &refused {
                tracing::info_span!("rdm.node_admin.build.reject.via-unservable-fact", node = %self.me, build_id = %build_id, detail = %e)
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

/// Serve `FetchBuildFacts` on this admin. Until `slot` is filled the admin holds no Build state and
/// a read is `NotReady` by name.
pub fn serve(b: ServerBuilder, slot: BuildFactsSlot) -> ServerBuilder {
    b.serve_stream::<BuildFacts, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: BuildFactsRequest, sink| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(BuildFactsReply::NotReady { reason: "this admin holds no Build state yet".into() });
            };
            Ok(door.serve(req, sink).await)
        }
    })
}

/// The facts of one Build, read whole from one responder.
#[derive(Debug)]
pub struct Fetched {
    pub build_id: BuildId,
    pub facts: Vec<BuildFact>,
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
            Self::Refused(_) => "refused",
            Self::Unreached(_) => "unreached",
            Self::Interrupted(_) => "interrupted",
            Self::Incomplete(_) => "incomplete",
        }
    }
}

/// `FetchBuildFacts` to `target` for `build_id`. Returns the facts only for a stream whose `End`
/// arrived with every chunk `0..chunks` present once and a fact count that agrees; anything less is
/// a failure that names what was missing, and the caller absorbs nothing from it.
pub async fn fetch_build_facts(client: &NodeRpcClient, target: &NodeTarget, build_id: &BuildId) -> Result<Fetched, FetchFailure> {
    let req = BuildFactsRequest::FetchBuildFacts { build_id: build_id.0.clone() };
    let opts = rafka_node_rpc::CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_secs(5)), ..Default::default() };
    let mut stream = match client.call_stream::<BuildFacts>(target, &req, &opts).await {
        Ok((s, _)) => s,
        Err((RpcOutcome::Reply(r), _)) => return Err(refusal_of(r.value().clone())),
        Err((RpcOutcome::Unserved(u), _)) => return Err(FetchFailure::Refused(format!("the node does not serve Build facts: {u:?}"))),
        Err((other, _)) => return Err(FetchFailure::Unreached(format!("the call ended {}", other.name()))),
    };
    let mut chunks: Vec<Option<Vec<BuildFact>>> = Vec::new();
    let mut end = None;
    while let Some(item) = stream.next().await {
        match item {
            StreamItem::Frame(FrameKind::Refusal(_), r) => return Err(refusal_of(r)),
            StreamItem::Frame(_, BuildFactsReply::Started) => {}
            StreamItem::Frame(_, BuildFactsReply::Facts { build_id: b, chunk_index, chunk_count, facts }) => {
                if b != build_id.0 {
                    return Err(FetchFailure::Incomplete(format!("a chunk of Build {b} arrived in the read of {build_id}")));
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
            StreamItem::Failed(f) => return Err(FetchFailure::Interrupted(format!("the stream failed before its end: {f:?}"))),
        }
    }
    let Some((b, facts, n, complete)) = end else {
        return Err(FetchFailure::Interrupted(format!("the stream ended after {} of {} chunks without its End frame", chunks.iter().flatten().count(), chunks.len())));
    };
    if b != build_id.0 {
        return Err(FetchFailure::Incomplete(format!("the End names Build {b}, the read was of {build_id}")));
    }
    if n as usize != chunks.len() || chunks.iter().any(Option::is_none) {
        return Err(FetchFailure::Incomplete(format!("the End counts {n} chunks, {} arrived of {} announced", chunks.iter().flatten().count(), chunks.len())));
    }
    let all: Vec<BuildFact> = chunks.into_iter().flatten().flatten().collect();
    if all.len() as u32 != facts {
        return Err(FetchFailure::Incomplete(format!("the End counts {facts} facts, {} decoded", all.len())));
    }
    Ok(Fetched { build_id: build_id.clone(), facts: all, chunks: n, complete })
}

fn refusal_of(r: BuildFactsReply) -> FetchFailure {
    match r {
        BuildFactsReply::NotReady { reason } => FetchFailure::NotReady(reason),
        BuildFactsReply::UnknownBuild { build_id } => FetchFailure::UnknownBuild(format!("the responder holds no fact of {build_id}")),
        BuildFactsReply::PeerUnresolved { reason } => FetchFailure::Unreached(format!("peer unresolved: {reason}")),
        other => FetchFailure::Refused(format!("{}: {other:?}", other.name())),
    }
}
