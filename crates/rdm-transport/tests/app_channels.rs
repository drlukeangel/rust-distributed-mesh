//! App channels use the envelope without applying RDM's replacement-state ordering.
//! Real Iroh endpoints use OS-assigned ports; hand-built chunks exercise receive order.

use bytes::Bytes;
use futures_util::StreamExt;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId};
use rdm_test::channel_member::{Member, MemberAddr};
use rdm_transport::gossip::{channel_id, Channel, ChannelError, Event};
use std::time::Duration;

const TYPE: &str = "rafka.entity.channel-proof";

/// A member of the mesh declaring [`TYPE`]: the testkit's channel member, the rig an embedding's
/// app-channel cells use too.
async fn node(fabric: &FabricId, mesh: &MeshId, name: &str, seeds: Vec<MemberAddr>) -> Member {
    Member::join(fabric, mesh, name, &seeds, &[TYPE]).await
}

async fn pair() -> (
    Member,
    Member,
    Channel,
    Channel,
    iroh_gossip::api::GossipSender,
    iroh_gossip::api::GossipReceiver,
) {
    let fabric = FabricId::mint();
    let mesh = MeshId::mint();
    let a = node(&fabric, &mesh, "mesh1.rpc.1", vec![]).await;
    let b = node(&fabric, &mesh, "mesh1.rpc.2", vec![a.addr()]).await;
    let ca = a.topology_view.join_channel(TYPE).await.unwrap();
    let cb = b.topology_view.join_channel(TYPE).await.unwrap();
    // Neighbour readiness is a protocol event, not an elapsed-time assumption.
    let mut raw = a
        .gossip()
        .subscribe(channel_id(&fabric, TYPE), vec![b.endpoint_id()])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), raw.joined())
        .await
        .expect("the app-channel neighbour joins")
        .unwrap();
    let mut other = b
        .gossip()
        .subscribe(channel_id(&fabric, TYPE), vec![a.endpoint_id()])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), other.joined())
        .await
        .expect("the publishing app-channel neighbour joins")
        .unwrap();
    let (sender, receiver) = raw.split();
    (a, b, ca, cb, sender, receiver)
}

fn event(sequence: u64, data: Vec<u8>) -> Event {
    Event {
        subject: "org/proof/row".into(),
        sequence: Some(sequence),
        dataschema: Some("proof.v1".into()),
        data: Bytes::from(data),
    }
}

async fn receive(channel: &mut Channel) -> rdm_transport::gossip::Received {
    tokio::time::timeout(Duration::from_secs(10), channel.recv())
        .await
        .expect("the event arrives within the cell's bound")
        .expect("the channel remains open")
}

fn encoded(n: &Member, kind: &str, id: &str, subject: &str, data: Vec<u8>, reverse: bool) -> Vec<u8> {
    let birth = n.topology_view.author_birth().unwrap();
    let mut attributes = vec![
        ("specversion".to_string(), "1.0".to_string()),
        ("id".into(), id.into()),
        (
            "source".into(),
            format!(
                "rdm://{}/{}/{}",
                birth.fabric, birth.node, birth.incarnation
            ),
        ),
        ("type".into(), kind.into()),
        ("subject".into(), subject.into()),
        ("time".into(), "1970-01-01T00:00:00.001Z".into()),
        ("datacontenttype".into(), "application/octet-stream".into()),
    ];
    if kind == TYPE {
        attributes.push(("sequence".into(), "00000000000000000001".into()));
    }
    if reverse {
        attributes.reverse();
    }
    postcard::to_allocvec(&(attributes, data)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undeclared_type_is_refused_by_name_on_join() {
    let fabric = FabricId::mint();
    let n = node(&fabric, &MeshId::mint(), "mesh1.rpc.1", vec![]).await;
    assert!(
        matches!(n.topology_view.join_channel("rafka.entity.unknown").await, Err(ChannelError::UndeclaredType { r#type }) if r#type == "rafka.entity.unknown")
    );
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
    assert_eq!(
        older.source,
        rdm_transport::envelope::Birth::publisher(a.topology_view.fabric_id(), &b.author)
    );
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
    assert!(data.len() < rdm_transport::topology::MAX_FRAME);
    sender.publish(event(1, data.clone())).await.unwrap();
    assert_eq!(receive(&mut receiver).await.data.as_ref(), data.as_slice());
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(10), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let iroh_gossip::api::Event::Received(message) = ev {
            assert!(
                message.content.len() <= rdm_transport::topology::MAX_FRAME,
                "every complete chunk includes metadata and both vector prefixes"
            );
            let (attributes, _): (Vec<(String, String)>, Vec<u8>) =
                postcard::from_bytes(&message.content).unwrap();
            assert_eq!(
                attributes.iter().find(|(k, _)| k == "type").unwrap().1,
                "rdm.gossip.chunk"
            );
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
        raw.broadcast(
            encoded(
                &a,
                "rdm.gossip.chunk",
                &format!("reordered/{i}"),
                "reordered",
                body.clone(),
                false,
            )
            .into(),
        )
        .await
        .unwrap();
        if i == 1 {
            // Reordered attributes change the gossip message bytes, while the declared chunk id
            // and content stay identical: transport byte dedupe cannot stand in for assembly dedupe.
            raw.broadcast(
                encoded(
                    &a,
                    "rdm.gossip.chunk",
                    &format!("reordered/{i}"),
                    "reordered",
                    body,
                    true,
                )
                .into(),
            )
            .await
            .unwrap();
        }
    }
    assert_eq!(receive(&mut receiver).await.data.as_ref(), data.as_slice());
    raw.broadcast(
        encoded(
            &a,
            TYPE,
            "control",
            "org/proof/control",
            b"control".to_vec(),
            false,
        )
        .into(),
    )
    .await
    .unwrap();
    assert_eq!(
        receive(&mut receiver).await.data.as_ref(),
        b"control",
        "no second event was delivered for the duplicate chunk"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chunked_event_with_a_missing_chunk_advances_lost_after_the_staleness_floor() {
    let (a, _b, _unused, mut receiver, raw, _events) = pair().await;
    let mut lost = receiver.lost();
    let first = *lost.borrow();
    let whole = encoded(
        &a,
        TYPE,
        "incomplete",
        "org/proof/row",
        vec![0x7c; 7000],
        false,
    );
    let chunks: Vec<_> = whole.chunks(1800).map(Vec::from).collect();
    for (i, chunk) in chunks.iter().enumerate().take(chunks.len() - 1) {
        let body = postcard::to_allocvec(&(i as u16, chunks.len() as u16, chunk)).unwrap();
        raw.broadcast(
            encoded(
                &a,
                "rdm.gossip.chunk",
                &format!("incomplete/{i}"),
                "incomplete",
                body,
                false,
            )
            .into(),
        )
        .await
        .unwrap();
    }
    let bound = rdm_core::cadence::staleness_floor() + Duration::from_secs(2);
    tokio::time::timeout(bound, lost.changed())
        .await
        .expect("unfinished assembly is evicted after the declared floor")
        .unwrap();
    assert!(*lost.borrow() > first);
    raw.broadcast(
        encoded(
            &a,
            TYPE,
            "after-loss",
            "org/proof/control",
            b"after-loss".to_vec(),
            false,
        )
        .into(),
    )
    .await
    .unwrap();
    assert_eq!(
        receive(&mut receiver).await.data.as_ref(),
        b"after-loss",
        "the carrier continues after the eviction"
    );
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

#[test]
fn the_receive_span_parents_to_the_publishers_span_and_carries_no_data() {
    if crate::own_process::delegated(
        module_path!(),
        "the_receive_span_parents_to_the_publishers_span_and_carries_no_data",
    ) {
        return;
    }
    use tracing::Instrument;
    let capture =
        rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "app-channel-trace").unwrap();
    capture.run(4, async {
        let (_a, _b, mut receiver, mut sender, raw, _events) = pair().await;
        let author = tracing::info_span!("rdm.mesh.channel.update.via-author");
        let context = author
            .in_scope(rdm_transport::current_traceparent)
            .expect("author context");
        sender
            .publish(event(7, b"secret-app-body-123".to_vec()))
            .instrument(author.clone())
            .await
            .unwrap();
        let got = receive(&mut receiver).await;
        got.span.in_scope(|| tracing::info!("application applies"));
        drop(got);
        drop(author);
        // A malformed context is observed by name but does not discard the data.
        let mut attrs: (Vec<(String, String)>, Vec<u8>) = postcard::from_bytes(&encoded(
            &_a,
            TYPE,
            "bad-context",
            "bad-context",
            vec![42],
            false,
        ))
        .unwrap();
        attrs.0.push(("traceparent".into(), "broken".into()));
        raw.broadcast(postcard::to_allocvec(&attrs).unwrap().into())
            .await
            .unwrap();
        assert_eq!(receive(&mut sender).await.data.as_ref(), &[42]);
        let spans = capture.spans().unwrap();
        let received = spans
            .iter()
            .find(|s| {
                s["name"] == "rdm.mesh.topology.update.via-frame"
                    && s["attributes"]["subject"] == "org/proof/row"
            })
            .expect("received event span");
        assert_eq!(received["trace_id"].as_str(), Some(&context[3..35]));
        assert_eq!(received["parent_span_id"].as_str(), Some(&context[36..52]));
        assert!(spans
            .iter()
            .any(|s| s["name"] == "rdm.mesh.topology.update.via-context-dropped"));
        assert!(!serde_json::to_string(&spans)
            .unwrap()
            .contains("secret-app-body-123"));
    });
}

#[test]
fn an_undeclared_wire_type_is_dropped_by_name_and_the_next_event_applies() {
    if crate::own_process::delegated(
        module_path!(),
        "an_undeclared_wire_type_is_dropped_by_name_and_the_next_event_applies",
    ) {
        return;
    }
    let capture =
        rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "app-channel-type").unwrap();
    capture.run(4, async {
        let (a, _b, _unused, mut receiver, raw, _events) = pair().await;
        raw.broadcast(encoded(&a, "app.undeclared", "wrong", "wrong", vec![1], false).into())
            .await
            .unwrap();
        raw.broadcast(encoded(&a, TYPE, "next", "next", vec![2], false).into())
            .await
            .unwrap();
        assert_eq!(receive(&mut receiver).await.subject, "next");
        assert!(capture
            .spans()
            .unwrap()
            .iter()
            .any(|s| s["name"] == "rdm.mesh.channel.reject.via-undeclared-type"));
    });
}

/// CONTRACT (one error arm per fact; the type's one receiver): a second join of a type whose
/// receiver an earlier join holds is refused `ReceiverHeld` naming the type, never `Left`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_join_of_a_held_type_is_refused_receiver_held_by_name() {
    let n = node(&FabricId::mint(), &MeshId::mint(), "mesh1.rpc.1", vec![]).await;
    let _first = n.topology_view.join_channel(TYPE).await.unwrap();
    assert!(matches!(n.topology_view.join_channel(TYPE).await, Err(ChannelError::ReceiverHeld { r#type }) if r#type == TYPE));
}

/// CONTRACT (an event's `sequence` is 20 decimal digits, gossip.md's app event attributes): a frame
/// whose sequence is not is dropped by name (`rdm.mesh.channel.reject.via-malformed-sequence`,
/// naming the type and the event id), and the next well-formed event applies. Must NOT happen: a
/// silent drop.
#[test]
fn a_malformed_sequence_is_dropped_by_name_and_the_next_event_applies() {
    if crate::own_process::delegated(
        module_path!(),
        "a_malformed_sequence_is_dropped_by_name_and_the_next_event_applies",
    ) {
        return;
    }
    let capture =
        rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "app-channel-sequence").unwrap();
    capture.run(4, async {
        let (a, _b, _unused, mut receiver, raw, _events) = pair().await;
        let mut bad: (Vec<(String, String)>, Vec<u8>) = postcard::from_bytes(&encoded(&a, TYPE, "bad", "bad", vec![1], false)).unwrap();
        for (k, v) in bad.0.iter_mut() {
            if k == "sequence" {
                *v = "1".into();
            }
        }
        raw.broadcast(postcard::to_allocvec(&bad).unwrap().into()).await.unwrap();
        raw.broadcast(encoded(&a, TYPE, "next", "next", vec![2], false).into())
            .await
            .unwrap();
        assert_eq!(receive(&mut receiver).await.subject, "next");
        let spans = capture.spans().unwrap();
        let dropped: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.mesh.channel.reject.via-malformed-sequence").collect();
        assert_eq!(dropped.len(), 1, "the malformed frame is dropped by name, once");
    });
}

/// Compile a positive control first, then reject each forbidden receiver ownership shape.
#[test]
fn a_second_recv_owner_is_refused_by_the_type_system_and_split_is_once() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let artifacts = std::env::var_os("I143_PREBUILT_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target/cells/artifacts.json"));
    let records = std::fs::read_to_string(artifacts).unwrap();
    let rlib = records
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|m| m["reason"] == "compiler-artifact" && m["target"]["name"] == "rdm_transport")
        .flat_map(|m| m["filenames"].as_array().cloned().unwrap_or_default())
        .filter_map(|v| v.as_str().map(str::to_string))
        .find(|f| f.ends_with(".rlib"))
        .expect("the current build's transport library");
    let dir = std::env::temp_dir().join(format!("channel-ownership-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    let compile = |body: &str| {
        let source = dir.join("ownership.rs");
        std::fs::write(
            &source,
            format!("use rdm_transport::gossip::Channel; {body}"),
        )
        .unwrap();
        std::process::Command::new("rustc")
            .args([
                "--edition=2021",
                "--crate-type=lib",
                "--emit=metadata",
                "--crate-name=ownership",
            ])
            .arg(&source)
            .arg("--out-dir")
            .arg(&dir)
            .arg("--extern")
            .arg(format!("rdm_transport={rlib}"))
            .arg("-L")
            .arg(format!(
                "dependency={}",
                std::path::Path::new(&rlib).parent().unwrap().display()
            ))
            .output()
            .unwrap()
    };
    let good=compile("pub async fn valid(mut channel: Channel) { let _ = channel.recv().await; let (_, mut subscriber) = channel.split(); let _ = subscriber.recv().await; }");
    assert!(
        good.status.success(),
        "{}",
        String::from_utf8_lossy(&good.stderr)
    );
    for (body,code) in [
        ("pub fn invalid(channel: Channel) { let _other = channel.clone(); }","E0599"),
        ("pub async fn invalid(channel: &Channel) { let _ = channel.recv().await; }","E0596"),
        ("pub fn invalid(channel: Channel) { let _ = channel.split(); let _ = channel.split(); }","E0382"),
    ] {let result=compile(body);let error=String::from_utf8_lossy(&result.stderr);assert!(!result.status.success()&&error.contains(code),"{code}: {error}");}
    std::fs::remove_dir_all(dir).unwrap();
}

/// Holding a member's channel permits receipt, but does not declare its type for this author.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learned_type_is_receivable_but_its_undeclared_author_cannot_publish_it() {
    let n = node(&FabricId::mint(), &MeshId::mint(), "mesh1.admin.1", vec![]).await;
    let kind = "app.member-owned";
    let member = NodeId::mint();
    let birth = IncarnationId::mint();
    n.topology_view
        .learn_app_types(
            member,
            birth,
            [kind.to_string()].into(),
            [kind.to_string()].into(),
        )
        .await
        .unwrap();
    assert!(n.topology_view.held_types().contains(kind));
    assert!(!n.topology_view.app_types().contains(kind));
    let channel = n
        .topology_view
        .join_channel(kind)
        .await
        .expect("a learned type has a receiver");
    assert!(
        matches!(channel.publish(event(1,vec![1])).await,Err(ChannelError::UndeclaredType{r#type})if r#type==kind),
        "holding a member's channel does not let this birth author its events"
    );
}

fn member_digest(n: &Member) -> rdm_core::MeshDigest {
    rdm_core::MeshDigest {
        fabric_id: n.topology_view.fabric_id().clone(),
        node: rdm_core::MeshNode {
            node_id: n.author.node_id.clone(),
            name: n.author.node.parse().unwrap(),
            endpoint_id: rdm_core::EndpointId(n.endpoint_id().to_string()),
            transport_addr: n.bound_addr(),
            incarnation: n.author.incarnation.clone(),
            supersedes: None,
            runtime: None,
        },
        status: rdm_core::MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: 1,
        emitted_at_rafka_ms: 1,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None,
        held_hash: n.topology_view.held_hash(),
        is_primary: None,
        is_fabric_primary: None,
    }
}

/// A holder learned after boot is the physical join peer, including on the seedless first admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_held_type_joins_through_the_peer_that_reported_it() {
    const LATER: &str = "app.later-held";
    let fabric = FabricId::mint();
    let mesh = MeshId::mint();
    let a = node(&fabric, &mesh, "mesh1.admin.1", vec![]).await;
    let b = node(&fabric, &mesh, "mesh1.admin.2", vec![a.addr()]).await;
    while a.topology_view.neighbours() < 1 || b.topology_view.neighbours() < 1 {
        tokio::task::yield_now().await;
    }
    let c = Member::join(&fabric, &mesh, "mesh1.rpc.1", &[b.addr()], &[LATER]).await;
    while b.topology_view.neighbours() < 2 || c.topology_view.neighbours() < 1 {
        tokio::task::yield_now().await;
    }
    b.topology_view.learn(member_digest(&c), "join-types-probe");
    b.topology_view
        .learn_app_types(
            c.author.node_id.clone(),
            c.author.incarnation.clone(),
            c.topology_view.app_types(),
            c.topology_view.held_types(),
        )
        .await
        .unwrap();
    // Establish the member-to-own-admin connection on both sides without giving A any new
    // bootstrap peer, before A joins LATER through B so B's HyParView active_view
    // already holds C when A's Join arrives at B.
    let mut member = c
        .gossip()
        .subscribe(channel_id(&fabric, LATER), vec![b.endpoint_id()])
        .await
        .unwrap();
    let mut middle = b
        .gossip()
        .subscribe(channel_id(&fabric, LATER), vec![])
        .await
        .unwrap();
    member.joined().await.unwrap();
    while !member.neighbors().any(|p| p == b.endpoint_id()) {
        member
            .next()
            .await
            .expect("the publisher stays subscribed")
            .unwrap();
    }
    while !middle.neighbors().any(|p| p == c.endpoint_id()) {
        middle
            .next()
            .await
            .expect("the middle holder stays subscribed")
            .unwrap();
    }
    a.topology_view.learn(member_digest(&b), "peer-types-probe");
    a.topology_view
        .learn_app_types(
            b.author.node_id.clone(),
            b.author.incarnation.clone(),
            b.topology_view.app_types(),
            b.topology_view.held_types(),
        )
        .await
        .unwrap();
    let mut observed = a
        .gossip()
        .subscribe(channel_id(&fabric, LATER), vec![])
        .await
        .unwrap();
    observed
        .joined()
        .await
        .expect("the new type joins through its reporting holder, even with no boot seeds");
    while !middle.neighbors().any(|p| p == a.endpoint_id())
        || !middle.neighbors().any(|p| p == c.endpoint_id())
    {
        middle
            .next()
            .await
            .expect("the middle holder stays subscribed")
            .unwrap();
    }
    while !member.neighbors().any(|p| p == a.endpoint_id()) {
        member
            .next()
            .await
            .expect("the publisher stays subscribed")
            .unwrap();
    }
    let mut receiver = a.topology_view.join_channel(LATER).await.unwrap();
    let publisher = c.topology_view.join_channel(LATER).await.unwrap();
    publisher.publish(event(1, vec![7])).await.unwrap();
    assert_eq!(receive(&mut receiver).await.data.as_ref(), &[7]);

    // Rejoining uses the reporting holders retained for this type, without acquiring boot seeds.
    drop(receiver);
    drop(observed);
    a.topology_view.stop().await;
    while middle.neighbors().any(|p| p == a.endpoint_id()) {
        middle
            .next()
            .await
            .expect("the middle holder stays subscribed")
            .unwrap();
    }
    while member.neighbors().any(|p| p == a.endpoint_id()) {
        member
            .next()
            .await
            .expect("the publisher stays subscribed")
            .unwrap();
    }
    while b.topology_view.neighbours() > 1 || c.topology_view.neighbours() > 1 {
        tokio::task::yield_now().await;
    }
    a.topology_view.start().await;
    let mut rejoined = a
        .gossip()
        .subscribe(channel_id(&fabric, LATER), vec![])
        .await
        .unwrap();
    rejoined
        .joined()
        .await
        .expect("the held type rejoins its reporting holder");
    while !middle.neighbors().any(|p| p == a.endpoint_id()) {
        middle
            .next()
            .await
            .expect("the middle holder stays subscribed")
            .unwrap();
    }
    while !member.neighbors().any(|p| p == a.endpoint_id()) {
        member
            .next()
            .await
            .expect("the publisher stays subscribed")
            .unwrap();
    }
    // Q72 = b: after a rejoin, gossip is best-effort and an app event is the app's to order by
    // its own `sequence`. B's forwarded copy of event 1, sent before A left, may arrive on A's
    // new subscription; the app skips any event at or below the sequence it already applied.
    let mut receiver = a.topology_view.join_channel(LATER).await.unwrap();
    publisher.publish(event(2, vec![8])).await.unwrap();
    let next = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let got = receiver.recv().await.expect("the channel remains open");
            if got.sequence.is_some_and(|sequence| sequence > 1) {
                return got;
            }
        }
    })
    .await
    .expect("event 2 arrives within the cell's bound");
    assert_eq!(next.sequence, Some(2));
    assert_eq!(next.data.as_ref(), &[8]);
}

