//! App channels use the envelope without applying RDM's replacement-state ordering.
//! Real Iroh endpoints use OS-assigned ports; hand-built chunks exercise receive order.

use bytes::Bytes;
use futures_util::StreamExt;
use iroh::protocol::Router;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId, PublisherId};
use rdm_transport::gossip::{channel_id, Channel, ChannelError, Event};
use rdm_transport::membership::Membership;
use std::time::Duration;

const TYPE: &str = "rafka.entity.channel-proof";

struct Node {
    ep: iroh::Endpoint,
    gossip: iroh_gossip::net::Gossip,
    membership: Membership,
    author: PublisherId,
    _router: Router,
}

async fn node(fabric: &FabricId, mesh: &MeshId, name: &str, seeds: Vec<iroh::EndpointAddr>) -> Node {
    let transport = iroh::endpoint::QuicTransportConfig::builder().build();
    let ep = rdm_transport::endpoint::bind_exact(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), vec![iroh_gossip::ALPN.to_vec()], transport).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let author = PublisherId { node: name.into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let membership = Membership::join(&gossip, &ep, fabric, "mesh1", mesh, name, rdm_transport::clock::os_clock(), seeds, &[TYPE]).await.unwrap();
    membership.set_author(&author);
    Node { ep, gossip, membership, author, _router: router }
}

fn address(n: &Node) -> iroh::EndpointAddr {
    let port = n.ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap().port();
    iroh::EndpointAddr::new(n.ep.id()).with_ip_addr(([127, 0, 0, 1], port).into())
}

async fn pair() -> (Node, Node, Channel, Channel, iroh_gossip::api::GossipSender, iroh_gossip::api::GossipReceiver) {
    let fabric = FabricId::mint();
    let mesh = MeshId::mint();
    let a = node(&fabric, &mesh, "mesh1.rpc.1", vec![]).await;
    let b = node(&fabric, &mesh, "mesh1.rpc.2", vec![address(&a)]).await;
    let ca = a.membership.join_channel(TYPE).await.unwrap();
    let cb = b.membership.join_channel(TYPE).await.unwrap();
    // Neighbour readiness is a protocol event, not an elapsed-time assumption.
    let mut raw = a.gossip.subscribe(channel_id(&fabric, TYPE), vec![b.ep.id()]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), raw.joined()).await.expect("the app-channel neighbour joins").unwrap();
    let (sender, receiver) = raw.split();
    (a, b, ca, cb, sender, receiver)
}

fn event(sequence: u64, data: Vec<u8>) -> Event {
    Event { subject: "org/proof/row".into(), sequence: Some(sequence), dataschema: Some("proof.v1".into()), data: Bytes::from(data) }
}

async fn receive(channel: &mut Channel) -> rdm_transport::gossip::Received {
    tokio::time::timeout(Duration::from_secs(10), channel.recv()).await.expect("the event arrives within the cell's bound").expect("the channel remains open")
}

fn encoded(n: &Node, kind: &str, id: &str, subject: &str, data: Vec<u8>, reverse: bool) -> Vec<u8> {
    let birth = n.membership.author_birth().unwrap();
    let mut attributes = vec![
        ("specversion".to_string(), "1.0".to_string()), ("id".into(), id.into()),
        ("source".into(), format!("rdm://{}/{}/{}", birth.fabric, birth.node, birth.incarnation)),
        ("type".into(), kind.into()), ("subject".into(), subject.into()),
        ("time".into(), "1970-01-01T00:00:00.001Z".into()),
        ("datacontenttype".into(), "application/octet-stream".into()),
    ];
    if kind == TYPE { attributes.push(("sequence".into(), "00000000000000000001".into())); }
    if reverse { attributes.reverse(); }
    postcard::to_allocvec(&(attributes, data)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undeclared_type_is_refused_by_name_on_join() {
    let fabric = FabricId::mint();
    let n = node(&fabric, &MeshId::mint(), "mesh1.rpc.1", vec![]).await;
    assert!(matches!(n.membership.join_channel("rafka.entity.unknown").await, Err(ChannelError::UndeclaredType { r#type }) if r#type == "rafka.entity.unknown"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lower_sequence_from_the_same_source_is_handed_to_the_app_not_dropped() {
    let (a, b, mut receiver, sender, _raw, _events) = pair().await;
    sender.publish(event(10, vec![10])).await.unwrap();
    let newer = receive(&mut receiver).await;
    assert_eq!(newer.sequence, Some(10));
    sender.publish(event(1, vec![1])).await.unwrap();
    let older = receive(&mut receiver).await;
    assert_eq!(older.sequence, Some(1));
    assert_eq!(older.data, Bytes::from_static(&[1]));
    assert_eq!(older.source, rdm_transport::envelope::Birth::publisher(a.membership.fabric_id(), &b.author));
    assert_eq!(older.dataschema.as_deref(), Some("proof.v1"));
}

#[test]
fn the_channel_id_differs_across_fabrics_for_one_type() {
    let a = FabricId::mint();
    let b = FabricId::mint();
    assert_ne!(channel_id(&a, TYPE), channel_id(&b, TYPE));
    assert_eq!(channel_id(&a, TYPE), channel_id(&a, TYPE));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_event_whose_data_fits_but_whose_complete_frame_does_not_is_chunked() {
    let (_a, _b, mut receiver, sender, _raw, mut events) = pair().await;
    let data = vec![0x5a; 3900];
    assert!(data.len() < rdm_transport::membership::MAX_FRAME);
    sender.publish(event(1, data.clone())).await.unwrap();
    assert_eq!(receive(&mut receiver).await.data.as_ref(), data.as_slice());
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(10), events.next()).await.unwrap().unwrap().unwrap();
        if let iroh_gossip::api::Event::Received(message) = ev {
            assert!(message.content.len() <= rdm_transport::membership::MAX_FRAME, "every complete chunk includes metadata and both vector prefixes");
            let (attributes, _): (Vec<(String, String)>, Vec<u8>) = postcard::from_bytes(&message.content).unwrap();
            assert_eq!(attributes.iter().find(|(k, _)| k == "type").unwrap().1, "rdm.gossip.chunk");
            break;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chunked_event_arrives_whole_when_its_chunks_are_reordered_and_one_is_duplicated() {
    let (a, _b, _unused, mut receiver, raw, _events) = pair().await;
    let data = vec![0x6b; 7000];
    let whole = encoded(&a, TYPE, "reordered", "org/proof/row", data.clone(), false);
    let chunks: Vec<_> = whole.chunks(1800).map(Vec::from).collect();
    for i in (0..chunks.len()).rev() {
        let body = postcard::to_allocvec(&(i as u16, chunks.len() as u16, &chunks[i])).unwrap();
        raw.broadcast(encoded(&a, "rdm.gossip.chunk", &format!("reordered/{i}"), "reordered", body.clone(), false).into()).await.unwrap();
        if i == 1 {
            // Reordered attributes change the gossip message bytes, while the declared chunk id
            // and content stay identical: transport byte dedupe cannot stand in for assembly dedupe.
            raw.broadcast(encoded(&a, "rdm.gossip.chunk", &format!("reordered/{i}"), "reordered", body, true).into()).await.unwrap();
        }
    }
    assert_eq!(receive(&mut receiver).await.data.as_ref(), data.as_slice());
    raw.broadcast(encoded(&a, TYPE, "control", "org/proof/control", b"control".to_vec(), false).into()).await.unwrap();
    assert_eq!(receive(&mut receiver).await.data.as_ref(), b"control", "no second event was delivered for the duplicate chunk");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chunked_event_with_a_missing_chunk_advances_lost_after_the_staleness_floor() {
    let (a, _b, _unused, mut receiver, raw, _events) = pair().await;
    let mut lost = receiver.lost();
    let first = *lost.borrow();
    let whole = encoded(&a, TYPE, "incomplete", "org/proof/row", vec![0x7c; 7000], false);
    let chunks: Vec<_> = whole.chunks(1800).map(Vec::from).collect();
    for (i, chunk) in chunks.iter().enumerate().take(chunks.len() - 1) {
        let body = postcard::to_allocvec(&(i as u16, chunks.len() as u16, chunk)).unwrap();
        raw.broadcast(encoded(&a, "rdm.gossip.chunk", &format!("incomplete/{i}"), "incomplete", body, false).into()).await.unwrap();
    }
    let bound = rdm_core::cadence::staleness_floor() + Duration::from_secs(2);
    tokio::time::timeout(bound, lost.changed()).await.expect("unfinished assembly is evicted after the declared floor").unwrap();
    assert!(*lost.borrow() > first);
    raw.broadcast(encoded(&a, TYPE, "after-loss", "org/proof/control", b"after-loss".to_vec(), false).into()).await.unwrap();
    assert_eq!(receive(&mut receiver).await.data.as_ref(), b"after-loss", "the carrier continues after the eviction");
}

#[allow(dead_code)]
async fn the_channel_surface_has_one_owned_receiver(mut channel: Channel, event: Event) {
    let _: u64 = channel.dropped();
    let _: tokio::sync::watch::Receiver<u64> = channel.lost();
    channel.publish(event).await.unwrap();
    let _: Option<rdm_transport::gossip::Received> = channel.recv().await;
    let (publisher, mut subscriber) = channel.split();
    let _: rdm_transport::gossip::Publisher = publisher;
    let _: Option<rdm_transport::gossip::Received> = subscriber.recv().await;
}
