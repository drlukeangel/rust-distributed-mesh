//! The topology a mesh-primary serves a read of its own mesh (node-rpc-envelope.md, Topology op
//! `0x1E`: "Snapshot uses gossip Members' shape"; "A matching held source version answers
//! Unchanged"; "Read-only: the target changes no state because of the read").
//!
//! gossip.md §3.1, `Members`: "the version bumps on a material change of the normalized projection
//! below and not on an unchanged republish"; the receiver rule: "A frame equal in revision to the
//! held one but different in content is an inconsistency, refused by name". A (publisher,
//! `topology_version`) names one content: the one its publication put into the mesh.

use iroh::protocol::Router;
use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_transport::topology::{Backbone, Frame, Membership};
use rdm_transport::snapshot::{chunks_of, Chunk, PublisherId, SourceSnapshot, Taken};
use serde_json::Value;

fn fabric() -> FabricId {
    FabricId::parse("fab000000001").unwrap()
}

fn digest(name: &str, node_id: &NodeId, incarnation: &IncarnationId, status: MemberStatus, seq: u64, port: u16) -> MeshDigest {
    MeshDigest { held_hash: Default::default(),
        fabric_id: fabric(),
        node: MeshNode {
            node_id: node_id.clone(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(format!("key-{name}")),
            transport_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            incarnation: incarnation.clone(),
            supersedes: None,
            runtime: None,
        },
        status,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: seq,
        emitted_at_rafka_ms: seq,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        config: Default::default(), is_primary: None, is_fabric_primary: None
    }
}

struct Node {
    ep: iroh::Endpoint,
    _router: Router,
    gossip: iroh_gossip::net::Gossip,
}

async fn node() -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    Node { ep, _router: router, gossip }
}

/// `mesh`'s snapshot in `served`, as the topology door holds it.
fn of(served: Vec<SourceSnapshot>, mesh: &str) -> SourceSnapshot {
    served.into_iter().find(|s| s.mesh == mesh).expect("the primary serves its own mesh")
}

/// The chunks of `s` as one topology read delivers them, under the door's `snapshot_id`.
fn read_chunks(s: &SourceSnapshot, snapshot_id: u64) -> Vec<Chunk> {
    chunks_of(&s.full(), |digests, in_flight, departed, chunk_index, chunk_count| Frame::Members {
        mesh: s.mesh.clone(),
        publisher: s.publisher.clone(),
        forwarded_by: None,
        topology_version: s.topology_version,
        published_at_rafka_ms: 1,
        snapshot_id,
        chunk_index,
        chunk_count,
        digests,
        in_flight,
        departed,
    }.authored(&crate::gossip_wire::author(), 0, 1))
    .into_iter()
    .map(|f| match f.into_data() {
        Frame::Members { mesh, publisher, forwarded_by, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed, .. } => {
            Chunk { mesh, publisher, forwarded_by, topology_version, snapshot_id, chunk_index, chunk_count, digests, in_flight, departed }
        }
        other => panic!("not a Members chunk: {other:?}"),
    })
    .collect()
}

/// Every chunk of one read, taken by `reader` as the joiner takes a read from its own mesh: the last
/// answer.
fn take_read(reader: &Membership, s: &SourceSnapshot, snapshot_id: u64) -> Taken {
    read_chunks(s, snapshot_id).into_iter().map(|c| reader.take_read_chunk(c, "mesh1")).last().expect("a snapshot has at least one chunk")
}

/// CONTRACT (gossip.md §3.1 `Members` and its receiver rule; node-rpc-envelope.md Topology `0x1E`):
/// the mesh-primary's publication publishes its own mesh at version N. A birth then joins its book: a
/// material change of the projection that no publication has published yet. A topology read in that
/// window serves the own mesh as published, the version-N publication itself at version N, and a
/// joiner that read version N before takes it again as the same content. The publication that follows
/// bumps the version and the read serves the new member there. Must NOT happen: the read serving
/// the book's live members under version N, which a joiner holding version N refuses as
/// `rdm.mesh.membership.reject.via-inconsistent`.
#[test]
fn a_read_between_a_material_change_and_its_publication_serves_the_published_members_at_their_version() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-mesh-transport-served-topology").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (p_node, r_node) = (node().await, node().await);
        let mesh_id = MeshId::mint();
        let clock = rdm_transport::clock::os_clock;
        let p = Membership::join(&p_node.gossip, &p_node.ep, &fabric(), "mesh1", &mesh_id, "mesh1.admin.1", clock(), vec![], &[]).await.unwrap();
        let reader = Membership::join(&r_node.gossip, &r_node.ep, &fabric(), "mesh1", &mesh_id, "mesh1.rpc.3", clock(), vec![], &[]).await.unwrap();
        let (p_birth, p_node_id) = (IncarnationId::mint(), NodeId::mint());
        let bb = Backbone::join(&p_node.gossip, &p_node.ep, &p, "mesh1", rdm_core::PublisherId { node: "mesh1.admin.1".to_string(), incarnation: p_birth.clone(), node_id: p_node_id.clone() }, vec![]).await.unwrap();
        bb.set_mesh_primary(true);
        let publisher = PublisherId { node: "mesh1.admin.1".into(), incarnation: p_birth.clone(), node_id: p_node_id.clone() };
        let publication = || async { assert!(bb.publish(&p, bb.own_members(&p)).await, "the primary publishes its publication") };

        let p_digest = digest("mesh1.admin.1", &p_node_id, &p_birth, MemberStatus::ReadyForTraffic, 0, 41_101);
        p.publish(&p_digest).await.unwrap();
        let (one, one_birth) = (NodeId::mint(), IncarnationId::mint());
        assert!(p.book.record(digest("mesh1.rpc.1", &one, &one_birth, MemberStatus::ReadyForTraffic, 1, 41_111)));
        assert!(p.book.started(&one, &one_birth, &"mesh1.rpc.1".parse().unwrap()));
        let own = || p.book.get(p_node_id.as_str()).expect("the primary holds itself").0;

        // Version N: the publication puts the projection into the mesh; a joiner reads it and installs it.
        publication().await;
        let published = of(p.held_topology(&own()), "mesh1");
        assert_eq!(published.publisher, publisher);
        let n = published.topology_version;
        assert!(n >= 1, "the publication published a version: {n}");
        assert!(matches!(take_read(&reader, &published, 1), Taken::Installed(_)), "the joiner installs version {n}");

        // A birth joins the book: a material change no publication has published yet.
        let (two, two_birth) = (NodeId::mint(), IncarnationId::mint());
        assert!(p.book.record(digest("mesh1.rpc.2", &two, &two_birth, MemberStatus::Pending, 1, 41_112)));
        assert!(p.book.get(two.as_str()).is_some(), "the primary's book holds the new birth");

        let served = of(p.held_topology(&own()), "mesh1");
        let names = |s: &SourceSnapshot| s.digests.iter().map(|d| d.node.name.to_string()).collect::<Vec<_>>();
        assert_eq!((served.publisher.clone(), served.topology_version), (publisher.clone(), n), "the read serves the version its content was published at");
        let again = take_read(&reader, &served, 2);
        assert!(!matches!(again, Taken::Refused(rdm_transport::snapshot::Refusal::Inconsistent)), "a joiner holding version {n} takes the read as the same content: {again:?}, serving {:?}", names(&served));
        assert!(served.digests.iter().all(|d| d.node.node_id != two), "the read serves no member under version {n} that version {n} does not carry: {:?}", names(&served));
        assert_eq!(served.full(), published.full(), "the read serves the version-{n} publication as published: {:?} against {:?}", names(&served), names(&published));

        // The publication that follows publishes the change at a later version, and the read serves it there.
        publication().await;
        let next = of(p.held_topology(&own()), "mesh1");
        assert!(next.topology_version > n, "the change was material: the publication bumped the version ({} after {n})", next.topology_version);
        assert!(next.digests.iter().any(|d| d.node.node_id == two), "the read serves the new member at the version that carries it: {:?}", names(&next));
        assert!(matches!(take_read(&reader, &next, 3), Taken::Installed(_)), "the joiner installs version {}", next.topology_version);

        let spans: Vec<Value> = capture.spans().expect("the cell's spans reached its evidence file");
        let refused: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.reject.via-inconsistent").collect();
        assert!(refused.is_empty(), "no read of the own mesh is refused as inconsistent: {refused:#?}");
        for n in [&p_node, &r_node] {
            n.ep.close().await;
        }
    });
}
