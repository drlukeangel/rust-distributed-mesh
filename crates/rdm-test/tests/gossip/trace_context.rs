//! CloudEvents tracing is authored context, never a receive decision.
use rdm_core::{FabricId, MeshId};
use rdm_transport::membership::{Frame, Membership, mesh_topic, learn_addresses};
use std::time::Duration;

async fn held(receiver: &Membership, mesh: &str, stamp: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while receiver.mesh_status(mesh).is_none_or(|s| s.changed_at_rafka_ms < stamp) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the following status proves receipt finished");
}

fn run(malformed: bool) {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");
    let service = if malformed { "rdm-gossip-malformed-trace" } else { "rdm-gossip-author-trace" };
    let capture = rdm_transport::EvidenceCapture::start(&dir, service).unwrap();
    let author_trace = capture.run(2, async {
        let (send_ep, gossip, _send_router) = crate::receive_rules::endpoint().await;
        let (recv_ep, recv_gossip, _recv_router) = crate::receive_rules::endpoint().await;
        let addr = iroh::EndpointAddr::new(send_ep.id()).with_ip_addr(send_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap());
        learn_addresses(&recv_ep, &[addr.clone()]).unwrap();
        let (fabric, mesh) = (FabricId::mint(), MeshId::mint());
        let mut topic = gossip.subscribe(mesh_topic(&fabric, &mesh), vec![]).await.unwrap();
        let receiver = Membership::join(&recv_gossip, &recv_ep, &fabric, "mesh1", &mesh, "mesh1.rpc.1", rdm_transport::clock::os_clock(), vec![addr]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), topic.joined()).await.unwrap().unwrap();
        let (sender, _rx) = topic.split();
        let author = crate::gossip_author(&fabric);
        let status = |scope: &str, stamp| Frame::MeshStatus { mesh: scope.into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: stamp };
        let (event, parent) = {
            let span = tracing::info_span!("rdm.test.gossip.update.via-author");
            span.in_scope(|| (status("traced", 1).authored(&author, 2, 1), rdm_transport::current_traceparent().unwrap()))
        };
        let author_trace = parent.split('-').nth(1).unwrap().trim_start_matches('0').to_string();
        let mut bytes = event.encode();
        if malformed {
            let (mut attrs, data): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&bytes).unwrap();
            attrs.retain(|(n, _)| n != "traceparent" && n != "tracestate");
            attrs.push(("traceparent".into(), "not-a-traceparent\n".into()));
            attrs.push(("tracestate".into(), "rojo=123".into()));
            bytes = rdm_transport::wire::encode(&(attrs, data)).unwrap();
        } else {
            let (attrs, _): (Vec<(String, String)>, Vec<u8>) = rdm_transport::wire::decode(&bytes).unwrap();
            assert_eq!(attrs.iter().find(|(n, _)| n == "traceparent").map(|(_, v)| v.as_str()), Some(parent.as_str()), "the authored trace crosses the wire");
        }
        sender.broadcast_neighbors(bytes.clone().into()).await.unwrap();
        sender.broadcast_neighbors(status("barrier", 1).authored(&author, 2, 1).encode().into()).await.unwrap();
        held(&receiver, "barrier", 1).await;
        assert_eq!(receiver.mesh_status("traced").map(|s| s.status), Some("ready-for-traffic".into()), "tracing never refuses the event");
        if !malformed {
            // The repeated fact's classifier is on the author's trace too; no second apply.
            sender.broadcast_neighbors(bytes.into()).await.unwrap();
            sender.broadcast_neighbors(status("barrier", 2).authored(&author, 2, 2).encode().into()).await.unwrap();
            held(&receiver, "barrier", 2).await;
        }
        send_ep.close().await; recv_ep.close().await;
        author_trace
    });
    let spans = capture.spans().unwrap();
    let applied: Vec<_> = spans.iter().filter(|s| s["name"] == "rdm.mesh.membership.update.via-mesh-status" && s["attributes"]["mesh"] == "traced").collect();
    assert_eq!(applied.len(), 1, "exactly one ordinary apply span");
    if malformed {
        assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.membership.update.via-context-dropped" && s["attributes"]["context_dropped"] == "traceparent"), "malformed traceparent dropped by name");
        assert_ne!(applied[0]["trace_id"].as_str().unwrap().trim_start_matches('0'), author_trace);
    } else {
        for name in ["rdm.mesh.membership.update.via-frame", "rdm.mesh.membership.update.via-mesh-status", "rdm.mesh.membership.update.via-duplicate"] {
            assert!(spans.iter().any(|s| s["name"] == name && s["trace_id"].as_str().unwrap_or("").trim_start_matches('0') == author_trace), "{name} shares the author's trace");
        }
    }
    println!("author trace {author_trace}; service {service}");
}

#[test]
fn a_received_frames_span_shares_its_authors_trace_id() { run(false); }

#[test]
fn a_malformed_traceparent_is_dropped_by_name_and_the_frame_applies() { run(true); }
