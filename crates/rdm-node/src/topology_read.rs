//! The topology read (`GetTopology`, op `0x1E`; node-rpc-envelope.md "Topology, op `0x1E`, rdm").
//!
//! Every node serves it: it reads what the node holds (its topology view and the snapshot
//! receiver's held sources, each with the publisher and `topology_version` it holds them at) and
//! streams one snapshot per mesh, chunked by the one method gossip's `MeshTopology` snapshots use
//! ([`rdm_transport::snapshot::chunks_of`]). It mutates nothing at the target.
//!
//! The caller ([`get_topology`]) installs a mesh only once every chunk of one snapshot is held,
//! through the receiver the Mesh channel's snapshots install into
//! ([`TopologyView::take_read_chunk`]): there is no second install path.

use rdm_core::wire::ProjectionEntry;
use rdm_core::{MeshDigest, PublisherId};
use rdm_transport::topology::TopologyView;
use rdm_transport::snapshot::{chunks_measured, Chunk, SourceSnapshot, Taken};
use rdm_transport::stream::StreamItem;
use rdm_transport::{NodeRpcClient, NodeTarget, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::RpcOutcome;
use rdm_core::streaming::FrameKind;
use rdm_core::topology::{SourceVersion, StoredNode, Topology, TopologyReply, TopologyRequest};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::Instrument as _;

/// What a node serves a topology read from.
pub struct TopologyDoor {
    /// The topology view the door reads held topology from.
    pub topology_view: TopologyView,
    /// This node's own digest, as it publishes it now.
    pub own: Arc<dyn Fn() -> MeshDigest + Send + Sync>,
    /// The map this node stores (an admin's durable map of the births it knows), for a mesh it holds
    /// no gossiped snapshot of.
    stored: Option<StoredSource>,
    /// Whether this node holds a recorded primary's exact birth as proven gone.
    gone: Option<GoneSource>,
    /// The caller's own whole row, when this node is the caller's mesh-primary (Q234).
    own_row: Option<OwnRowSource>,
    /// The rafka-time this node adopted: the value its answer carries, never its own clock.
    time: rdm_transport::clock::RafkaTime,
    snapshots: AtomicU64,
}

/// The incarnations of recorded primaries the node holds as proven gone, read once per topology read
/// (Q266): each primary record the read hands out is marked gone when its incarnation is among them.
pub type GoneSource = Arc<dyn Fn() -> std::collections::HashSet<rdm_core::IncarnationId> + Send + Sync>;

/// The whole row of the caller whose endpoint key is given, with the row's revision (P1), answered by its
/// own mesh-primary from its atlas (Q234), never the store: `None` from any other node, or for a caller that is not a node of the
/// answering mesh-primary's mesh.
pub type OwnRowSource = Arc<dyn Fn(String) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<(rdm_core::views::NodeView, u64)>> + Send>> + Send + Sync>;

/// What a node stores of the meshes it does not hold a snapshot of: the births it knows, and the id
/// it stored for each mesh.
#[derive(Debug, Clone, Default)]
pub struct StoredMap {
    /// The births the target stored.
    pub nodes: Vec<StoredNode>,
    /// The mesh id the target stored for each mesh, by mesh name.
    pub mesh_ids: std::collections::BTreeMap<String, rdm_core::MeshId>,
}

/// The births a node stores, read when a mesh has no gossiped snapshot to answer from.
pub type StoredSource = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<StoredMap, String>> + Send>> + Send + Sync>;

/// Stored nodes per `Stored` frame: a frame stays under the reply ceiling.
const STORED_PER_FRAME: usize = 24;

impl TopologyDoor {
    /// A door over `topology_view`, the node's own digest and the rafka-time the node adopted.
    pub fn new(topology_view: TopologyView, own: Arc<dyn Fn() -> MeshDigest + Send + Sync>, time: rdm_transport::clock::RafkaTime) -> Self {
        Self { topology_view, own, stored: None, gone: None, own_row: None, time, snapshots: AtomicU64::new(0) }
    }

    /// This node also answers, for a mesh it holds no snapshot of, from the map it stores.
    pub fn with_stored(mut self, stored: StoredSource) -> Self {
        self.stored = Some(stored);
        self
    }

    /// This node also answers a caller of its mesh its own whole row (Q234): a mesh-primary's door.
    pub fn with_own_row(mut self, own_row: OwnRowSource) -> Self {
        self.own_row = Some(own_row);
        self
    }

    /// This node also tells a reader which recorded primaries it holds as proven gone.
    pub fn with_gone(mut self, gone: GoneSource) -> Self {
        self.gone = Some(gone);
        self
    }

    fn next_snapshot_id(&self) -> u64 {
        self.snapshots.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// The door a running node fills once it holds a view.
pub type TopologySlot = Arc<OnceLock<Arc<TopologyDoor>>>;

/// The chunks of one held source as the replies that carry them, in the one chunking gossip uses.
pub fn snapshot_replies(s: &SourceSnapshot, snapshot_id: u64) -> Vec<TopologyReply> {
    let full = s.full();
    chunks_measured(&full, |digests, in_flight, deleted, chunk_index, chunk_count| TopologyReply::Snapshot {
        mesh: s.mesh.clone(),
        publisher: s.publisher.clone(),
        topology_version: s.topology_version,
        snapshot_id, chunk_index, chunk_count,
        digests: digests.iter().map(ProjectionEntry::from).collect(),
        in_flight, deleted,
    }, |reply| rdm_transport::wire::encode(reply).expect("topology reply serializes"))
}

impl TopologyDoor {
    async fn serve(&self, caller: String, req: TopologyRequest, sink: rdm_transport::stream::ReplySink<Topology, rdm_transport::stream::NotStarted>) -> TopologyReply {
        let TopologyRequest::GetTopology { mesh, since } = req;
        let me = (self.own)();
        let span = tracing::info_span!(
            "rdm.mesh.topology.serve.via-read",
            node = %me.node.name,
            requested = mesh.as_deref().unwrap_or("*"),
            meshes = tracing::field::Empty,
            snapshots = tracing::field::Empty,
            unchanged = tracing::field::Empty,
            stored = tracing::field::Empty,
            stored_nodes = tracing::field::Empty,
            primaries = tracing::field::Empty,
            rafka_time_ms = tracing::field::Empty,
            kind = tracing::field::Empty,
            own_row = tracing::field::Empty,
            members = tracing::field::Empty,
            bytes = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        async move {
            let span = tracing::Span::current();
            // A node that holds no rafka-time has none to serve: it starts no stream and says why.
            let Some(time_ms) = self.time.try_now_ms() else {
                span.record("outcome", "not-ready");
                return rdm_core::reply::topology::not_ready(format!("{}: this node holds no rafka-time yet", me.node.name));
            };
            // Only the mesh asked for is copied out of the view (Q266).
            let held = self.topology_view.held_topology_of(&me, mesh.as_deref());
            // A mesh with no gossiped snapshot is answered from the stored map, marked as having no
            // version, when the node stores births of it. A read of one mesh this node holds a snapshot
            // of, or of its own mesh, has none to answer from it, and the map is not built (Q266).
            let mut stored: std::collections::BTreeMap<String, Vec<StoredNode>> = Default::default();
            let mut stored_ids = std::collections::BTreeMap::new();
            let unheld = mesh.as_ref().is_none_or(|m| held.is_empty() && *m != me.node.name.mesh);
            if let Some(source) = self.stored.as_ref().filter(|_| unheld) {
                let map = match source().await {
                    Ok(map) => map,
                    Err(reason) => {
                        span.record("outcome", "not-ready");
                        return rdm_core::reply::topology::not_ready(format!("{}: the stored map could not be read: {reason}", me.node.name));
                    }
                };
                stored_ids = map.mesh_ids;
                for n in map.nodes {
                    let m = n.name.split('.').next().unwrap_or_default().to_string();
                    if m != me.node.name.mesh && held.iter().all(|s| s.mesh != m) && mesh.as_ref().is_none_or(|want| *want == m) {
                        stored.entry(m).or_default().push(n);
                    }
                }
            }
            if let Some(m) = &mesh {
                if held.is_empty() && stored.is_empty() {
                    span.record("outcome", "unknown-mesh");
                    return rdm_core::reply::topology::unknown_mesh(m.clone());
                }
            }
            let mut sink = match sink.started(TopologyReply::Started).await {
                Ok(s) => s,
                Err(e) => {
                    span.record("outcome", format!("caller-gone: {e:?}").as_str());
                    return TopologyReply::End { meshes: 0 };
                }
            };
            // What the read answers, per held source: `<mesh>@<topology_version>:<path.name>,...`.
            span.record("members", held.iter().map(|s| format!("{}@{}:{}", s.mesh, s.topology_version, s.digests.iter().map(|d| d.node.name.to_string()).collect::<Vec<_>>().join(","))).collect::<Vec<_>>().join(";").as_str());
            let (mut answered, mut snapshots, mut unchanged, mut bytes) = (0u32, 0u64, 0u64, 0u64);
            for s in &held {
                let same = mesh.is_some() && since.as_ref().is_some_and(|v| v.publisher == s.publisher && v.topology_version == s.topology_version);
                let replies = if same {
                    unchanged += 1;
                    vec![TopologyReply::Unchanged { mesh: s.mesh.clone(), publisher: s.publisher.clone(), topology_version: s.topology_version }]
                } else {
                    snapshots += 1;
                    snapshot_replies(s, self.next_snapshot_id())
                };
                for r in replies {
                    bytes += rdm_core::protocol::encode(&r).map(|b| b.len() as u64).unwrap_or(0);
                    if let Err(e) = sink.data(r).await {
                        span.record("outcome", format!("caller-gone: {e:?}").as_str());
                        return TopologyReply::End { meshes: answered };
                    }
                }
                answered += 1;
            }
            let stored_meshes = stored.len() as u64;
            span.record("stored_nodes", stored.values().map(|v| v.len()).sum::<usize>() as u64);
            for (m, nodes) in stored {
                for chunk in nodes.chunks(STORED_PER_FRAME) {
                    let r = TopologyReply::Stored { mesh: m.clone(), mesh_id: stored_ids.get(&m).cloned(), nodes: chunk.to_vec() };
                    bytes += rdm_core::protocol::encode(&r).map(|b| b.len() as u64).unwrap_or(0);
                    if let Err(e) = sink.data(r).await {
                        span.record("outcome", format!("caller-gone: {e:?}").as_str());
                        return TopologyReply::End { meshes: answered };
                    }
                }
                answered += 1;
            }
            // The recorded primaries this node holds: the fabric-primary and each mesh's mesh-primary, an
            // election input the reader holds before it computes any (elections §3).
            let mut records: Vec<(rdm_core::topology::PrimaryKind, rdm_core::Primary)> = self.topology_view.primaries().meshes().into_values().map(|h| (rdm_core::topology::PrimaryKind::MeshPrimary, h)).collect();
            records.extend(self.topology_view.primaries().fabric().map(|h| (rdm_core::topology::PrimaryKind::FabricPrimary, h)));
            let primary_records = records.len() as u64;
            let lost = self.gone.as_ref().filter(|_| primary_records > 0).map(|g| g()).unwrap_or_default();
            for (kind, primary) in records {
                let gone = lost.contains(&primary.incarnation);
                let r = TopologyReply::Primaries { kind, primary, gone };
                bytes += rdm_core::protocol::encode(&r).map(|b| b.len() as u64).unwrap_or(0);
                if let Err(e) = sink.data(r).await {
                    span.record("outcome", format!("caller-gone: {e:?}").as_str());
                    return TopologyReply::End { meshes: answered };
                }
            }
            // The rafka-time this node adopted and which primary it is, read from the primaries it
            // holds: the puller applies its own rule to the pair (`rafka_time::adopt_pulled`).
            let time_ms = self.time.try_now_ms().unwrap_or(time_ms);
            let kind = crate::rafka_time::primary_held(self.topology_view.primaries(), &me.node.name.mesh, &me.node.node_id);
            let r = TopologyReply::RafkaTime { ms: time_ms, kind };
            bytes += rdm_core::protocol::encode(&r).map(|b| b.len() as u64).unwrap_or(0);
            if let Err(e) = sink.data(r).await {
                span.record("outcome", format!("caller-gone: {e:?}").as_str());
                return TopologyReply::End { meshes: answered };
            }
            // The caller's own whole row, when this node is its mesh-primary (Q234).
            if let Some(source) = &self.own_row {
                let row = source(caller).await;
                span.record("own_row", row.as_ref().map_or(String::new(), |(r, revision)| format!("{}@{revision}", r.name)).as_str());
                if let Some((node, revision)) = row {
                    let r = TopologyReply::OwnRow { node, revision };
                    let len = rdm_core::protocol::encode(&r).map(|b| b.len() as u64).unwrap_or(0);
                    match sink.data(r).await {
                        Ok(()) => bytes += len,
                        // A row over the reply-frame ceiling is named on the span and not sent: the read
                        // still ends whole, and the reader holds the config its join handed it.
                        Err(rdm_transport::stream::SinkError::TooLarge { size, max }) => {
                            span.record("own_row", format!("too-large: {size} bytes over the {max}-byte reply frame").as_str());
                        }
                        Err(e) => {
                            span.record("outcome", format!("caller-gone: {e:?}").as_str());
                            return TopologyReply::End { meshes: answered };
                        }
                    }
                }
            }
            span.record("rafka_time_ms", time_ms);
            span.record("kind", kind.map_or("none", rdm_core::topology::PrimaryKind::name));
            span.record("primaries", primary_records);
            span.record("stored", stored_meshes);
            span.record("meshes", answered);
            span.record("snapshots", snapshots);
            span.record("unchanged", unchanged);
            span.record("bytes", bytes);
            span.record("outcome", "served");
            TopologyReply::End { meshes: answered }
        }
        .instrument(span)
        .await
    }
}

/// Serve `GetTopology` on this node. Until `slot` is filled the node holds no topology and a read
/// is `NotReady` by name.
pub fn serve(b: ServerBuilder, slot: TopologySlot) -> ServerBuilder {
    b.serve_stream::<Topology, _, _>(OpOwner::Product("rdm".into()), move |peer, req: TopologyRequest, sink| {
        let slot = slot.clone();
        let caller = peer.endpoint_id.to_string();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(rdm_core::reply::topology::not_ready("this node holds no topology view yet".into()));
            };
            Ok(door.serve(caller, req, sink).await)
        }
    })
}

/// What one topology read left installed (`lifecycles/node-topology-get.md`).
#[derive(Debug, Clone, Default)]
pub struct TopologyRead {
    /// Each mesh installed from a complete snapshot, with its node digests.
    pub installed: Vec<ReadMesh>,
    /// Each mesh the target answered `Unchanged`: `(mesh, topology_version)`.
    pub unchanged: Vec<(String, u64)>,
    /// The meshes the target says it answered.
    pub meshes: u32,
    /// Each mesh the target answered from its stored map, with no version: births to reach, never
    /// installed and never the mesh's current topology.
    pub stored: Vec<StoredMesh>,
    /// The primary records the target holds, in the order it sent them (`lifecycles/node-topology-get.md`).
    pub seats: Vec<(rdm_core::Seat, rdm_core::Primary, bool)>,
    /// The rafka-time the target served and which primary it was when it served it (`lifecycles/node-topology-get.md`). Every accepted
    /// read carries it.
    pub rafka_time: Option<ServedTime>,
    /// The reader's own whole row and the row's revision, when the target is its mesh-primary (Q234).
    pub own_row: Option<(rdm_core::views::NodeView, u64)>,
}

/// The rafka-time a topology read was served with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServedTime {
    /// The target's rafka-time in milliseconds.
    pub ms: u64,
    /// Which primary the target was, `None` for none.
    pub kind: Option<rdm_core::topology::PrimaryKind>,
}

impl TopologyRead {
    /// Adopt the rafka-time this read was served with into `time`, by the rule of the puller
    /// `node_id` of `mesh` as its own primary records stand (the same [`crate::rafka_time::adopt_pulled`]
    /// every pull goes through). `served_by` is the target's `path.name`.
    pub fn adopt_rafka_time(&self, time: &rdm_transport::clock::RafkaTime, topology_view: &TopologyView, node: &str, mesh: &str, node_id: &rdm_core::NodeId, served_by: &str) -> Result<rdm_transport::clock::Adopted, TimeNotAdopted> {
        let served = self.rafka_time.ok_or_else(|| TimeNotAdopted::Absent(format!("{served_by}: the topology answer carries no rafka-time")))?;
        let puller = crate::rafka_time::Puller::of(topology_view.primaries(), mesh, node_id);
        crate::rafka_time::adopt_pulled(time, puller, node, crate::rafka_time::Served { via: "get-topology", served_by, ms: served.ms, kind: served.kind }).map_err(TimeNotAdopted::Refused)
    }
}

/// Why a read's rafka-time was not adopted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeNotAdopted {
    /// The answer carried no rafka-time.
    Absent(String),
    /// A mesh primary refused it by name.
    Refused(crate::rafka_time::Refusal),
}

impl std::fmt::Display for TimeNotAdopted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent(why) => f.write_str(why),
            Self::Refused(r) => write!(f, "rafka-time refused: {r}"),
        }
    }
}

/// A mesh answered from the target's stored map.
#[derive(Debug, Clone)]
pub struct StoredMesh {
    /// The mesh's name.
    pub mesh: String,
    /// The id the target stored for the mesh, when it stored one.
    pub mesh_id: Option<rdm_core::MeshId>,
    /// The births the target stored.
    pub nodes: Vec<StoredNode>,
}

/// A mesh answered from a gossiped snapshot.
#[derive(Debug, Clone)]
pub struct ReadMesh {
    /// The mesh's name.
    pub mesh: String,
    /// The publisher of the snapshot.
    pub publisher: PublisherId,
    /// The snapshot's `topology_version`.
    pub topology_version: u64,
    /// The node digests of the snapshot.
    pub members: Vec<MeshDigest>,
}

/// Why a topology read installed nothing more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyFailure {
    /// The target holds no topology yet.
    NotReady(String),
    /// The target does not hold the requested mesh, or refused by another name.
    Refused(String),
    /// The target could not be reached, or the stream ended before its terminal.
    Unreached(String),
    /// The stream ended complete but its count disagrees with what was installed.
    Incomplete(String),
}

impl std::fmt::Display for TopologyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotReady(r) => write!(f, "not ready: {r}"),
            Self::Refused(r) => write!(f, "refused: {r}"),
            Self::Unreached(r) => write!(f, "unreached: {r}"),
            Self::Incomplete(r) => write!(f, "incomplete: {r}"),
        }
    }
}

/// `GetTopology` to `target` (a node of `answerer_mesh`), installing into `topology_view` each mesh whose snapshot arrives
/// complete. `mesh = None` reads every mesh the target holds; `since` is the source version held
/// for the one `mesh` asked for.
pub async fn get_topology(client: &NodeRpcClient, target: &NodeTarget, answerer_mesh: &str, topology_view: &TopologyView, mesh: Option<&str>, since: Option<SourceVersion>) -> Result<TopologyRead, TopologyFailure> {
    let node_name = topology_view.node_name();
    // A mesh whose served snapshot is older than the complete one this node already installed of the
    // same source (its publication arrived before the read's answer) is refused `OlderThanHeld`: the
    // held snapshot stands, and it is the one this read leaves installed (node-start.md step 4).
    let mut older_than_held: Vec<String> = Vec::new();
    let mut read = read_with(client, target, &node_name, mesh, since, |c| {
        let mesh = c.mesh.clone();
        let taken = topology_view.take_read_chunk(c, answerer_mesh);
        if matches!(taken, Taken::Refused(rdm_transport::snapshot::Refusal::OlderThanHeld { .. })) && !older_than_held.contains(&mesh) {
            older_than_held.push(mesh);
        }
        taken
    })
    .await?;
    older_than_held.retain(|m| read.installed.iter().all(|i| &i.mesh != m));
    for mesh in older_than_held {
        if let Some((publisher, topology_version, members)) = topology_view.installed_snapshot(&mesh) {
            read.installed.push(ReadMesh { mesh, publisher, topology_version, members });
        }
    }
    // The seat records the target holds are this node's too, wherever they supersede its own: it
    // computes no seat before it has them.
    topology_view.learn_primaries(&read.seats, "entry");
    // The reader's own whole row its mesh-primary's atlas holds: held on its own entry (Q234).
    if let Some((row, revision)) = &read.own_row {
        topology_view.set_own_row(row.clone(), *revision);
    }
    Ok(read)
}

/// The top-up read every receiver of a mesh channel hands [`TopologyView::top_ups`] (gossip.md §3.3: a
/// node whose source desynchronized "pulls the current full fabric topology from its own current
/// mesh-primary with GetTopology, installs it atomically, records the publisher and version baseline,
/// and resumes"): one [`get_topology`] of each desynchronized mesh to the exact birth of the primary
/// the topology view names, installed through the topology view's own receiver. The one workflow a
/// node-admin and every other node run.
///
/// A top-up is a topology read repair (`lifecycles/node-topology-get.md`) and adopts no rafka-time:
/// between its pulls a node keeps its rafka-time disciplined from its own primary's heartbeats only
/// (`crate::rafka_time`), so a gap's repair neither stalls its reading nor clears its
/// discipline's window or slew.
pub fn top_up_fetch(client: Arc<NodeRpcClient>) -> rdm_transport::topology::TopUpFetch {
    Arc::new(move |topology_view: TopologyView, primary: MeshDigest, meshes: Vec<String>| {
        let client = client.clone();
        Box::pin(async move {
            let mut done = rdm_transport::topology::TopUpDone::default();
            for mesh in meshes {
                let read = get_topology(&client, &NodeTarget::ExactNode(primary.node.node_id.clone()), &primary.node.name.mesh, &topology_view, Some(&mesh), None).await.map_err(|e| format!("{mesh}: {e}"))?;
                done.installed.extend(read.installed.iter().map(|m| (m.mesh.clone(), m.topology_version)));
            }
            Ok(done)
        })
    })
}

/// [`get_topology`] that installs nothing: each mesh is assembled by a receiver of its own, whole
/// or not at all, and returned. A recovering admin reads a local node's topology only to know
/// whom to reach; what it holds as nodes stays what it hears itself.
/// Read the topology from `target`.
pub async fn read_topology(client: &NodeRpcClient, target: &NodeTarget, node: &str, mesh: Option<&str>, since: Option<SourceVersion>) -> Result<TopologyRead, TopologyFailure> {
    let mut receiver = rdm_transport::snapshot::SnapshotReceiver::default();
    read_with(client, target, node, mesh, since, |c| receiver.take_chunk(c)).await
}

async fn read_with(
    client: &NodeRpcClient,
    target: &NodeTarget,
    node: &str,
    mesh: Option<&str>,
    since: Option<SourceVersion>,
    mut take: impl FnMut(Chunk) -> Taken,
) -> Result<TopologyRead, TopologyFailure> {
    let req = TopologyRequest::GetTopology { mesh: mesh.map(String::from), since };
    let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Overall(Duration::from_secs(10)), ..Default::default() };
    let target_name = match target {
        NodeTarget::ExactNode(id) => id.to_string(),
        other => format!("{other:?}"),
    };
    let mut stream = match client.call_stream::<Topology>(target, &req, &opts).await {
        Ok((s, _)) => s,
        Err((RpcOutcome::Reply(r), _)) => return Err(refusal_of(r.value().clone())),
        Err((other, _)) => return Err(TopologyFailure::Unreached(format!("the call ended {}", other.name()))),
    };
    let mut read = TopologyRead::default();
    let mut served_meshes = None;
    while let Some(item) = stream.next().await {
        match item {
            StreamItem::Frame(FrameKind::Refusal(_), r) => return Err(refusal_of(r)),
            StreamItem::Frame(_, TopologyReply::Started) => {}
            StreamItem::Frame(_, TopologyReply::Snapshot { mesh, publisher, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, deleted }) => {
                let chunk = Chunk {
                    mesh,
                    publisher,
                    forwarded_by: Some("topology-read".into()),
                    topology_version,
                    snapshot_id,
                    chunk_index,
                    chunk_count,
                    digests: digests.into_iter().map(MeshDigest::from).collect(),
                    in_flight,
                    deleted: deleted,
                };
                match take(chunk) {
                    Taken::Waiting { .. } => {}
                    Taken::Installed(i) => {
                        let members = i.full.digests();
                        tracing::info_span!(
                            "rdm.mesh.topology.update.via-read-install",
                            node = node,
                            mesh = %i.mesh,
                            publisher = %i.publisher,
                            topology_version = i.topology_version,
                            snapshot_id = i.snapshot_id,
                            members = members.len(),
                            target = %target_name,
                        )
                        .in_scope(|| tracing::info!("a complete snapshot read from the target is installed"));
                        read.installed.push(ReadMesh { mesh: i.mesh.clone(), publisher: i.publisher.clone(), topology_version: i.topology_version, members });
                    }
                    Taken::Refused(r) => {
                        tracing::info_span!("rdm.mesh.topology.reject.via-read-install", target = %target_name, reason = ?r)
                            .in_scope(|| tracing::info!("a snapshot chunk read from the target is refused: the held projection stands"));
                    }
                }
            }
            StreamItem::Frame(_, TopologyReply::Unchanged { mesh, topology_version, .. }) => read.unchanged.push((mesh, topology_version)),
            StreamItem::Frame(_, TopologyReply::Stored { mesh, mesh_id, nodes }) => match read.stored.iter_mut().find(|m| m.mesh == mesh) {
                Some(m) => m.nodes.extend(nodes),
                None => read.stored.push(StoredMesh { mesh, mesh_id, nodes }),
            },
            StreamItem::Frame(_, TopologyReply::Primaries { kind, primary, gone }) => read.seats.push((match kind {
                rdm_core::topology::PrimaryKind::MeshPrimary => rdm_core::Seat::MeshPrimary,
                rdm_core::topology::PrimaryKind::FabricPrimary => rdm_core::Seat::FabricPrimary,
            }, primary, gone)),
            StreamItem::Frame(_, TopologyReply::RafkaTime { ms, kind }) => read.rafka_time = Some(ServedTime { ms, kind }),
            StreamItem::Frame(_, TopologyReply::OwnRow { node, revision }) => read.own_row = Some((node, revision)),
            StreamItem::Frame(_, TopologyReply::End { meshes }) => served_meshes = Some(meshes),
            StreamItem::Frame(_, other) => return Err(TopologyFailure::Refused(format!("an unexpected frame in the stream: {}", other.name()))),
            StreamItem::Failed(f) => return Err(TopologyFailure::Unreached(format!("the stream failed before its end: {f:?}"))),
        }
        // The frame is taken into this birth's view and read result: recovered, it is acked.
        stream.accepted().await;
    }
    let Some(meshes) = served_meshes else {
        return Err(TopologyFailure::Unreached("the stream ended without its End frame".into()));
    };
    read.meshes = meshes;
    let held = (read.installed.len() + read.unchanged.len() + read.stored.len()) as u32;
    if held > meshes {
        return Err(TopologyFailure::Incomplete(format!("{held} meshes arrived whole, the target counted {meshes}")));
    }
    Ok(read)
}

fn refusal_of(r: TopologyReply) -> TopologyFailure {
    match r {
        TopologyReply::NotReady { reason } => TopologyFailure::NotReady(reason),
        TopologyReply::UnknownMesh { mesh } => TopologyFailure::Refused(format!("the target holds no mesh {mesh}")),
        other => TopologyFailure::Refused(format!("{}: {other:?}", other.name())),
    }
}
