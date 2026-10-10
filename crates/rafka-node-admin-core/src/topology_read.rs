//! The topology read (`GetTopology`, op `0x1E`; node-rpc-envelope.md "Topology, op `0x1E`, rdm").
//!
//! Every node serves it: it reads what the node holds (its membership book and the snapshot
//! receiver's held sources, each with the publisher and `topology_version` it holds them at) and
//! streams one snapshot per mesh, chunked by the one method gossip's `Members` snapshots use
//! ([`rafka_mesh_transport::snapshot::chunks_of`]). It mutates nothing at the target.
//!
//! The caller ([`get_topology`]) installs a mesh only once every chunk of one snapshot is held,
//! through the receiver the Mesh channel's snapshots install into
//! ([`Membership::take_read_chunk`]): there is no second install path.

use rafka_mesh_entity::wire::WireDigest;
use rafka_mesh_entity::{MeshDigest, PublisherId};
use rafka_mesh_transport::membership::{Frame, Membership};
use rafka_mesh_transport::snapshot::{chunks_of, Chunk, SourceSnapshot, Taken};
use rafka_node_rpc::stream::StreamItem;
use rafka_node_rpc::{NodeRpcClient, NodeTarget, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::streaming::FrameKind;
use rafka_node_rpc_contract::topology::{SourceVersion, StoredNode, Topology, TopologyReply, TopologyRequest};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::Instrument as _;

/// What a node serves a topology read from.
pub struct TopologyDoor {
    /// The membership the door reads held topology from.
    pub membership: Membership,
    /// This node's own digest, as it publishes it now.
    pub own: Arc<dyn Fn() -> MeshDigest + Send + Sync>,
    /// The map this node stores (an admin's durable map of the births it knows), for a mesh it holds
    /// no gossiped snapshot of.
    stored: Option<StoredSource>,
    /// Whether this node holds a seat holder's exact birth as proven gone.
    gone: Option<GoneSource>,
    /// The rafka-time this node adopted: the value its answer carries, never its own clock.
    time: rafka_mesh_transport::clock::RafkaTime,
    snapshots: AtomicU64,
}

/// Whether the node holds a recorded seat holder's exact birth as proven gone.
pub type GoneSource = Arc<dyn Fn(&rafka_mesh_entity::SeatHolder) -> bool + Send + Sync>;

/// What a node stores of the meshes it does not hold a snapshot of: the births it knows, and the id
/// it stored for each mesh.
#[derive(Debug, Clone, Default)]
pub struct StoredMap {
    /// The births the target stored.
    pub nodes: Vec<StoredNode>,
    /// The mesh id the target stored for each mesh, by mesh name.
    pub mesh_ids: std::collections::BTreeMap<String, rafka_mesh_entity::MeshId>,
}

/// The births a node stores, read when a mesh has no gossiped snapshot to answer from.
pub type StoredSource = Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<StoredMap, String>> + Send>> + Send + Sync>;

/// Stored nodes per `Stored` frame: a frame stays under the reply ceiling.
const STORED_PER_FRAME: usize = 24;

impl TopologyDoor {
    /// A door over `membership`, the node's own digest and the rafka-time the node adopted.
    pub fn new(membership: Membership, own: Arc<dyn Fn() -> MeshDigest + Send + Sync>, time: rafka_mesh_transport::clock::RafkaTime) -> Self {
        Self { membership, own, stored: None, gone: None, time, snapshots: AtomicU64::new(0) }
    }

    /// This node also answers, for a mesh it holds no snapshot of, from the map it stores.
    pub fn with_stored(mut self, stored: StoredSource) -> Self {
        self.stored = Some(stored);
        self
    }

    /// This node also tells a reader which recorded seat holders it holds as proven gone.
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
    chunks_of(&full, |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members {
        mesh: s.mesh.clone(),
        publisher: s.publisher.clone(),
        forwarded_by: None,
        topology_version: s.topology_version,
        published_at_rafka_ms: 0,
        snapshot_id,
        chunk_index,
        chunk_count,
        digests,
        in_flight,
        departed,
    })
    .into_iter()
    .map(|f| match f {
        Frame::Members { mesh, publisher, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed, .. } => TopologyReply::Snapshot {
            mesh,
            publisher,
            topology_version,
            snapshot_id,
            chunk_index,
            chunk_count,
            digests: digests.iter().map(WireDigest::from).collect(),
            in_flight,
            departed,
        },
        _ => unreachable!("chunks_of builds Members frames"),
    })
    .collect()
}

impl TopologyDoor {
    async fn serve(&self, req: TopologyRequest, sink: rafka_node_rpc::stream::ReplySink<Topology, rafka_node_rpc::stream::NotStarted>) -> TopologyReply {
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
            seats = tracing::field::Empty,
            rafka_time_ms = tracing::field::Empty,
            seat = tracing::field::Empty,
            bytes = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        async move {
            let span = tracing::Span::current();
            // A node that holds no rafka-time has none to serve: it starts no stream and says why.
            let Some(time_ms) = self.time.try_now_ms() else {
                span.record("outcome", "not-ready");
                return TopologyReply::NotReady { reason: format!("{}: this node holds no rafka-time yet", me.node.name) };
            };
            let mut held = self.membership.held_topology(&me);
            if let Some(m) = &mesh {
                held.retain(|s| &s.mesh == m);
            }
            // A mesh with no gossiped snapshot is answered from the stored map, marked as having no
            // version, when the node stores births of it.
            let mut stored: std::collections::BTreeMap<String, Vec<StoredNode>> = Default::default();
            let mut stored_ids = std::collections::BTreeMap::new();
            if let Some(source) = &self.stored {
                let map = match source().await {
                    Ok(map) => map,
                    Err(reason) => {
                        span.record("outcome", "not-ready");
                        return TopologyReply::NotReady { reason: format!("{}: the stored map could not be read: {reason}", me.node.name) };
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
                    return TopologyReply::UnknownMesh { mesh: m.clone() };
                }
            }
            let mut sink = match sink.started(TopologyReply::Started).await {
                Ok(s) => s,
                Err(e) => {
                    span.record("outcome", format!("caller-gone: {e:?}").as_str());
                    return TopologyReply::End { meshes: 0 };
                }
            };
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
                    bytes += rafka_node_rpc_contract::protocol::encode(&r).map(|b| b.len() as u64).unwrap_or(0);
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
                    bytes += rafka_node_rpc_contract::protocol::encode(&r).map(|b| b.len() as u64).unwrap_or(0);
                    if let Err(e) = sink.data(r).await {
                        span.record("outcome", format!("caller-gone: {e:?}").as_str());
                        return TopologyReply::End { meshes: answered };
                    }
                }
                answered += 1;
            }
            // The seat records this node holds: who holds the fabric seat and each mesh's, so the
            // reader computes no seat before it knows them (ruling R-A2).
            let mut records: Vec<(rafka_mesh_entity::Seat, rafka_mesh_entity::SeatHolder)> = self.membership.seats().meshes().into_values().map(|h| (rafka_mesh_entity::Seat::MeshPrimary, h)).collect();
            records.extend(self.membership.seats().fabric().map(|h| (rafka_mesh_entity::Seat::FabricPrimary, h)));
            let seat_records = records.len() as u64;
            for (seat, holder) in records {
                let gone = self.gone.as_ref().is_some_and(|g| g(&holder));
                let r = TopologyReply::Seats { seat, holder, gone };
                bytes += rafka_node_rpc_contract::protocol::encode(&r).map(|b| b.len() as u64).unwrap_or(0);
                if let Err(e) = sink.data(r).await {
                    span.record("outcome", format!("caller-gone: {e:?}").as_str());
                    return TopologyReply::End { meshes: answered };
                }
            }
            // The rafka-time this node adopted and the seat it holds, read from its own seat
            // records: the puller applies its own rule to the pair (`rafka_time::adopt_pulled`).
            let time_ms = self.time.try_now_ms().unwrap_or(time_ms);
            let seat = crate::rafka_time::seat_held(self.membership.seats(), &me.node.name.mesh, &me.node.node_id);
            let r = TopologyReply::RafkaTime { ms: time_ms, seat };
            bytes += rafka_node_rpc_contract::protocol::encode(&r).map(|b| b.len() as u64).unwrap_or(0);
            if let Err(e) = sink.data(r).await {
                span.record("outcome", format!("caller-gone: {e:?}").as_str());
                return TopologyReply::End { meshes: answered };
            }
            span.record("rafka_time_ms", time_ms);
            span.record("seat", seat.map_or("none", rafka_mesh_entity::Seat::name));
            span.record("seats", seat_records);
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
    b.serve_stream::<Topology, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: TopologyRequest, sink| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(TopologyReply::NotReady { reason: "this node holds no membership yet".into() });
            };
            Ok(door.serve(req, sink).await)
        }
    })
}

/// What one topology read left installed.
#[derive(Debug, Clone, Default)]
pub struct TopologyRead {
    /// Each mesh installed from a complete snapshot, with its members.
    pub installed: Vec<ReadMesh>,
    /// Each mesh the target answered `Unchanged`: `(mesh, topology_version)`.
    pub unchanged: Vec<(String, u64)>,
    /// The meshes the target says it answered.
    pub meshes: u32,
    /// Each mesh the target answered from its stored map, with no version: births to reach, never
    /// installed and never the mesh's current topology.
    pub stored: Vec<StoredMesh>,
    /// The seat records the target holds, in the order it sent them.
    pub seats: Vec<(rafka_mesh_entity::Seat, rafka_mesh_entity::SeatHolder, bool)>,
    /// The rafka-time the target served and the seat it held when it served it. Every accepted
    /// read carries it.
    pub rafka_time: Option<ServedTime>,
}

/// The rafka-time a topology read was served with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServedTime {
    /// The target's rafka-time in milliseconds.
    pub ms: u64,
    /// The seat the target held, `None` for a replica.
    pub seat: Option<rafka_mesh_entity::Seat>,
}

impl TopologyRead {
    /// Adopt the rafka-time this read was served with into `time`, by the rule of the puller
    /// `node_id` of `mesh` as its own seat records stand (the same [`crate::rafka_time::adopt_pulled`]
    /// every pull goes through). `served_by` is the target's `path.name`.
    pub fn adopt_rafka_time(&self, time: &rafka_mesh_transport::clock::RafkaTime, membership: &Membership, node: &str, mesh: &str, node_id: &rafka_mesh_entity::NodeId, served_by: &str) -> Result<rafka_mesh_transport::clock::Adopted, TimeNotAdopted> {
        let served = self.rafka_time.ok_or_else(|| TimeNotAdopted::Absent(format!("{served_by}: the topology answer carries no rafka-time")))?;
        let puller = crate::rafka_time::Puller::of(membership.seats(), mesh, node_id);
        crate::rafka_time::adopt_pulled(time, puller, node, crate::rafka_time::Served { via: "get-topology", served_by, ms: served.ms, seat: served.seat }).map_err(TimeNotAdopted::Refused)
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
    pub mesh_id: Option<rafka_mesh_entity::MeshId>,
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
    /// The members of the snapshot.
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

/// `GetTopology` to `target` (a node of `answerer_mesh`), installing into `membership` each mesh whose snapshot arrives
/// complete. `mesh = None` reads every mesh the target holds; `since` is the source version held
/// for the one `mesh` asked for.
pub async fn get_topology(client: &NodeRpcClient, target: &NodeTarget, answerer_mesh: &str, membership: &Membership, mesh: Option<&str>, since: Option<SourceVersion>) -> Result<TopologyRead, TopologyFailure> {
    let read = read_with(client, target, membership.node(), mesh, since, |c| membership.take_read_chunk(c, answerer_mesh)).await?;
    // The seat records the target holds are this node's too, wherever they supersede its own: it
    // computes no seat before it has them.
    membership.learn_seats(&read.seats, "entry");
    Ok(read)
}

/// [`get_topology`] that installs nothing: each mesh is assembled by a receiver of its own, whole
/// or not at all, and returned. A recovering admin reads a local node's topology only to know
/// whom to reach; what it holds as members stays what it hears itself.
pub(crate) async fn read_topology(client: &NodeRpcClient, target: &NodeTarget, node: &str, mesh: Option<&str>, since: Option<SourceVersion>) -> Result<TopologyRead, TopologyFailure> {
    let mut receiver = rafka_mesh_transport::snapshot::SnapshotReceiver::default();
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
    let opts = rafka_node_rpc::CallOptions { budget: rafka_node_rpc::Budget::Overall(Duration::from_secs(10)), ..Default::default() };
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
            StreamItem::Frame(_, TopologyReply::Snapshot { mesh, publisher, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed }) => {
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
                    departed,
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
            StreamItem::Frame(_, TopologyReply::Seats { seat, holder, gone }) => read.seats.push((seat, holder, gone)),
            StreamItem::Frame(_, TopologyReply::RafkaTime { ms, seat }) => read.rafka_time = Some(ServedTime { ms, seat }),
            StreamItem::Frame(_, TopologyReply::End { meshes }) => served_meshes = Some(meshes),
            StreamItem::Frame(_, other) => return Err(TopologyFailure::Refused(format!("an unexpected frame in the stream: {}", other.name()))),
            StreamItem::Failed(f) => return Err(TopologyFailure::Unreached(format!("the stream failed before its end: {f:?}"))),
        }
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
