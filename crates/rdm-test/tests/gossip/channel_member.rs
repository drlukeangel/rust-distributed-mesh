//! The testkit's channel member (`rdm_test::channel_member`): two members of one mesh, joined to one
//! declared app type, publish and receive an app event through the channel `join_channel` opens, as
//! an embedding's cell does with it.

use bytes::Bytes;
use rdm_core::{FabricId, MeshId};
use rdm_test::channel_member::Member;
use rdm_transport::gossip::{channel_id, Event};
use std::time::Duration;

const TYPE: &str = "testkit.channel-member";
const BOUND: Duration = Duration::from_secs(10);

fn event(sequence: u64, data: &'static [u8]) -> Event {
    Event { subject: "testkit/member".into(), sequence: Some(sequence), dataschema: Some("testkit.v1".into()), data: Bytes::from_static(data) }
}

// @feature: gossip
/// CONTRACT (gossip.md app channels; the testkit rig for app-channel cells): two members of a mesh,
/// the second joined through the first, both declaring one app type, open its channel with
/// `topology_view.join_channel` and `split` it. Once the channel's neighbours are up (a protocol event,
/// never elapsed time), each publishes one event and receives the other's: its data, sequence and
/// schema as published, sourced from the publishing member's author.
#[test]
fn two_members_publish_and_receive_one_event() {
    let cap = crate::common::capture("two_members_publish_and_receive_one_event");
    cap.run(crate::common::Threads::Workers(4), async {
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let a = Member::join(&fabric, &mesh, "mesh1.rpc.1", &[], &[TYPE]).await;
        let b = Member::join(&fabric, &mesh, "mesh1.rpc.2", &[a.addr()], &[TYPE]).await;
        let (to_b, mut from_b) = a.topology_view.join_channel(TYPE).await.expect("a joins its declared channel").split();
        let (to_a, mut from_a) = b.topology_view.join_channel(TYPE).await.expect("b joins its declared channel").split();
        for (on, other) in [(&a, &b), (&b, &a)] {
            let mut raw = on.gossip().subscribe(channel_id(&fabric, TYPE), vec![other.endpoint_id()]).await.unwrap();
            tokio::time::timeout(BOUND, raw.joined()).await.expect("the channel's neighbour is up").unwrap();
        }

        to_b.publish(event(1, b"from a")).await.expect("a publishes");
        let got = tokio::time::timeout(BOUND, from_a.recv()).await.expect("b receives a's event").expect("b's channel is open");
        assert_eq!((got.data.as_ref(), got.sequence, got.dataschema.as_deref()), (&b"from a"[..], Some(1), Some("testkit.v1")));
        assert_eq!(got.source, rdm_transport::envelope::Birth::publisher(&fabric, &a.author), "sourced from a's author");

        to_a.publish(event(2, b"from b")).await.expect("b publishes");
        let got = tokio::time::timeout(BOUND, from_b.recv()).await.expect("a receives b's event").expect("a's channel is open");
        assert_eq!((got.data.as_ref(), got.sequence), (&b"from b"[..], Some(2)));
        assert_eq!(got.source, rdm_transport::envelope::Birth::publisher(&fabric, &b.author), "sourced from b's author");
    })
}
