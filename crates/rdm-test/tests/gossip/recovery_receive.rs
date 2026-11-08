//! A real node.topology.get installs the recovery floor; subsequent gossip cannot move it back.
use tracing::Instrument;

use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, model::NodeStatus, MeshDigest, MeshId, MeshNode, NodeId, PublisherId, PrimaryRole, Primary};
use rdm_gossip::location::learn_addresses;
use rdm_gossip::frame::Frame;
use rdm_gossip::topology::TopologyView;
use rdm_gossip::status::StatusEvent;
use rdm_gossip::aggregate::{Chunk, Taken};
use rdm_transport::{NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use std::{sync::{Arc, OnceLock}, time::Duration};

async fn endpoint() -> (iroh::Endpoint, iroh_gossip::net::Gossip) {
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    (ep, gossip)
}
async fn send(sender: &iroh_gossip::api::GossipSender, receiver: &TopologyView, frame: Frame, serial: u64, author: &rdm_gossip::envelope::Birth) {
    // A Digest is authored by its own birth (`Frame::digest`); any other frame by `author`.
    sender.broadcast_neighbors(frame.authored(author, 10, 1).encode().into()).await.unwrap();
    sender.broadcast_neighbors(Frame::MeshStatus { mesh: "barrier".into(), status: "ready".into(), publisher: author.node.to_string(), forwarded_by: None, changed_at_rafka_ms: serial }.authored(author, 10, 1).encode().into()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while receiver.mesh_status("barrier").is_none_or(|s| s.changed_at_rafka_ms < serial) { tokio::time::sleep(Duration::from_millis(10)).await; }
    }).await.expect("the following frame proves this receipt finished");
}
fn birth(fabric: &FabricId, mesh: &MeshId, path: rdm_core::PathName, mesh_name: &str, r#type: &str, ordinal: u16) -> MeshDigest {
    MeshDigest { fabric_id: fabric.clone(), node: MeshNode { mesh: mesh_name.to_owned(), r#type: r#type.to_string(), node_id: NodeId::mint(), path: path.path.clone(), name: path.name.clone(), endpoint_id: EndpointId(iroh::SecretKey::generate().public().to_string()), transport_addr: format!("127.0.0.1:{}",12000+ordinal).parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, }, status: NodeStatus::Ready, digest_seq: 100, emitted_at_rafka_ms: 1, primary: Vec::new(), }
}
fn operation(d: &MeshDigest, command: &str) -> LifecycleOp {
    LifecycleOp { r#type: d.node.r#type.clone(), mesh: d.node.mesh.clone(), build_id: "recovery-receive".into(), operation: rdm_core::OperationId::node(format!("node.{command}"), &d.node.node_id), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), path: d.node.path.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }
}

/// CONTRACT: topology RPC installs the held version, births, terminal/lifecycle evidence and roles.
/// Stale replacement state, publications/deltas, delayed facts and reserved positions then change
/// none of that state. Statuses retain their prior authored context; R10 has its own revision cell.
#[test]
fn a_real_topology_read_then_gossip_of_every_category_changes_nothing_it_installed() {
    let dir=std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/evidence/gossip-conformance");std::fs::create_dir_all(&dir).unwrap();
    let capture=rdm_transport::EvidenceCapture::start(&dir,"rdm-recovery-receive").unwrap();
    let receiver_id = NodeId::mint();
    capture.run(4,async {
        let root = tracing::info_span!("rdm.test.gossip.update.via-read-recovery");
        async {
        let (fabric,mesh)=(FabricId::mint(),MeshId::mint());
        let (server_ep,server_gossip)=endpoint().await;
        let server_addr=server_ep.bound_sockets().into_iter().find(|a|a.is_ipv4()).unwrap();
        let mut own=birth(&fabric, &mesh, rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", 10);
        own.node.endpoint_id=EndpointId(server_ep.id().to_string());own.node.transport_addr=server_addr;
        let publisher=PublisherId::from(&own.node);
        let author=rdm_gossip::envelope::Birth::publisher(&fabric,&publisher);
        let time=rdm_transport::clock::RafkaTime::unadopted();time.adopt(5_000_000);
        let server_topology_view = rdm_gossip::topology::TopologyView::new(&fabric, "mesh1", &mesh, own.node.node_id.as_str(), Arc::new(time.clone()), Default::default());
        let server_topology_view_channels = rdm_gossip::channel::Channels::new(&server_gossip, &server_ep, &server_topology_view).unwrap();
        server_topology_view_channels.join(rdm_gossip::channel::ChannelId::mesh(server_topology_view.fabric_id(), server_topology_view.mesh_id()), vec![]).await.unwrap();
        let slot:rdm_node::topology_read::TopologySlot=Arc::new(OnceLock::new());
        let rpc=rdm_node::topology_read::serve(ServerBuilder::new(),slot.clone()).build(ServedBirth{node_id:own.node.node_id.to_string(),incarnation:own.node.incarnation.to_string()}).unwrap();
        let _server_router=iroh::protocol::Router::builder(server_ep.clone()).accept(iroh_gossip::ALPN,server_gossip.clone()).accept(rdm_transport::ALPN,rpc).spawn();
        let (receiver_ep,receiver_gossip)=endpoint().await;
        let _receiver_router=iroh::protocol::Router::builder(receiver_ep.clone()).accept(iroh_gossip::ALPN,receiver_gossip.clone()).spawn();
        let addr=iroh::EndpointAddr::new(server_ep.id()).with_ip_addr(server_addr);
        learn_addresses(&receiver_ep,&[addr.clone()]).unwrap();
        learn_addresses(&server_ep,&[iroh::EndpointAddr::new(receiver_ep.id()).with_ip_addr(receiver_ep.bound_sockets().into_iter().find(|a|a.is_ipv4()).unwrap())]).unwrap();
        let mut subscription = server_gossip.subscribe(rdm_gossip::channel::ChannelId::mesh(&fabric, &mesh).topic(),vec![]).await.unwrap();
        let receiver = rdm_gossip::topology::TopologyView::new(&fabric, "mesh1", &mesh, receiver_id.as_str(), rdm_transport::clock::os_clock(), Default::default());
        let receiver_channels = rdm_gossip::channel::Channels::new(&receiver_gossip, &receiver_ep, &receiver).unwrap();
        receiver_channels.join(rdm_gossip::channel::ChannelId::mesh(receiver.fabric_id(), receiver.mesh_id()), vec![addr]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5),subscription.joined()).await.unwrap().unwrap();
        let (sender,_rx)= subscription.split();
        let (restarted,started,deleted,rpc_primary)=(birth(&fabric, &mesh, rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 1),birth(&fabric, &mesh, rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", 2),birth(&fabric, &mesh, rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", 3),birth(&fabric, &mesh, rdm_core::PathName::new("mesh1.node", "4").unwrap(), "mesh1", "node", 4));
        let (stop,delete)=(operation(&started,"stop"),operation(&deleted,"delete"));
        // The version the read serves carries the `node` type primary record its publisher put in it:
        // a read serves a version's content as published, never records taken since (gossip.md §3.1).
        let full=vec![own.clone(),restarted.clone(),started.clone(),MeshDigest{ primary: rdm_core::RoleRevision::of_type(&rpc_primary.node.r#type, Some(10), None),..rpc_primary.clone()}];
        // The deletion the read carries is the node listed deleted (Q390).
        let listed=|d:&MeshDigest,status:NodeStatus|MeshDigest{status,..d.clone()};
        let served:Vec<MeshDigest>=full.iter().cloned().chain([listed(&deleted,NodeStatus::Deleted)]).collect();
        assert!(matches!(server_topology_view_channels.take_read_chunk(Chunk{mesh:"mesh1".into(), mesh_id: None,author:None,topology_version:100,time_ms:1,chunk_index:0,chunk_count:1,digests:served, runtimes: Vec::new()},"mesh1"),Taken::Installed(_)));
        // An aggregate of mesh1: its author the publisher, its stop and removal each the node's status.
        let aggregate=|version:u64,at:u64,nodes:Vec<MeshDigest>|Frame::aggregate("mesh1",None,&rdm_gossip::envelope::Birth::publisher(&fabric,&publisher),version,at,0,1,nodes);
        let primary_of=|d:&MeshDigest,revision|Primary{mesh:"mesh1".into(),node_id:d.node.node_id.clone(),incarnation:d.node.incarnation.clone(),revision};
        for (role,d) in [(PrimaryRole::MeshPrimary,&own),(PrimaryRole::FabricPrimary,&own),(PrimaryRole::NodePrimary("node".into()),&rpc_primary)] {server_topology_view.primaries().take(role,&primary_of(d,10));}
        let answer_own=own.clone();assert!(slot.set(Arc::new(rdm_node::topology_read::TopologyService::new(server_topology_view.clone(),Arc::new(move||answer_own.clone()),time))).is_ok());
        let mut serial=0;
        for d in [&own,&restarted,&started,&deleted,&rpc_primary] {serial+=1;let mut old=d.clone();old.digest_seq=1;send(&sender,&receiver,Frame::digest(&old),serial,&author).await;}
        let old_full=vec![own.clone(),restarted.clone(),listed(&started,NodeStatus::Stopped),listed(&deleted,NodeStatus::Deleting),rpc_primary.clone()].into_iter().map(|mut d|{d.digest_seq=1;d}).collect();
        serial+=1;send(&sender,&receiver,aggregate(10,10,old_full),serial,&author).await;
        serial+=1;send(&sender,&receiver,Frame::MeshStatus{mesh:"mesh1".into(),status:"ready".into(),publisher:own.node.node_id.to_string(),forwarded_by:None,changed_at_rafka_ms:100},serial,&author).await;
        serial+=1;send(&sender,&receiver,Frame::FabricStatus{fabric:fabric.clone(),status:"ready".into(),publisher:own.node.node_id.to_string(),forwarded_by:None,changed_at_rafka_ms:100},serial,&author).await;
        let resolver=Arc::new(StaticResolver::new());resolver.insert(ResolvedNode{ mesh: own.node.mesh.clone(), r#type: own.node.r#type.clone(),node_id:own.node.node_id.clone(),path:own.node.path.clone(),name:own.node.name.clone(),endpoint_id:server_ep.id(),transport_addr:server_addr,incarnation:own.node.incarnation.clone()});
        let client=NodeRpcClient::new(receiver_ep.clone(),resolver);
        let read=rdm_node::topology_read::get_topology(&client,&NodeTarget::ExactNode(own.node.node_id.clone()),"mesh1",&receiver_channels,None,None).await.unwrap();
        assert!(read.installed.iter().any(|s|s.mesh=="mesh1"&&s.topology_version==100));
        assert_eq!(receiver.held_source_version("mesh1"),Some((None,100)),"the read installs its version; its reply names no author");
        assert!(receiver.digest_view.in_flight().is_empty(),"the read installs the operations' completion/later lifecycle evidence");
        assert!(receiver.digest_view.is_deleted(deleted.node.node_id.as_str()));
        assert!(receiver.digest_view.get(deleted.node.node_id.as_str()).is_none());
        assert_eq!(receiver.primaries().node_type_primary("mesh1","node"),Some(primary_of(&rpc_primary,10)));
        for d in [&restarted,&started,&rpc_primary] {assert_eq!(receiver.digest_view.get(d.node.node_id.as_str()).unwrap().0.digest_seq,100);}
        let before:Vec<_>=full.iter().map(|d|receiver.digest_view.get(d.node.node_id.as_str()).unwrap().0).collect();
        let statuses=(receiver.mesh_status("mesh1"),receiver.fabric_status());
        let (births,changes,overlays,roles)=(receiver.digest_view.birth_changes(),receiver.digest_view.digest_changes(),receiver.digest_view.overlay_changes(),receiver.primaries().subscribe());
        let counts=(*births.borrow(),*changes.borrow(),*overlays.borrow(),*roles.borrow());
        let bad=iroh::SecretKey::generate().public();
        let mut stale=restarted.clone();stale.digest_seq=99;stale.node.endpoint_id=EndpointId(bad.to_string());
        let mut frames=vec![
            Frame::digest(&stale.clone()),
            Frame::MeshStatus{mesh:"mesh1".into(),status:"deleting".into(),publisher:own.node.node_id.to_string(),forwarded_by:None,changed_at_rafka_ms:99},
            Frame::FabricStatus{fabric:fabric.clone(),status:"deleting".into(),publisher:own.node.node_id.to_string(),forwarded_by:None,changed_at_rafka_ms:99},
            aggregate(99,200,vec![stale.clone(),listed(&started,NodeStatus::Stopped),listed(&deleted,NodeStatus::Deleting)]),
            Frame::TopologyDelta{mesh:"mesh1".into(), mesh_id: None,base_version:98,chunk_index:0,chunk_count:1,added:vec![rdm_core::wire::NodeEntry::of(&stale)],changed:vec![],removed:vec![rdm_core::framing::Birth{node_id:rpc_primary.node.node_id.clone(),incarnation:rpc_primary.node.incarnation.clone()}]}.authored_delta(&author,99,200),
            StatusEvent::Stopped.frame(stop,10),StatusEvent::Deleting.frame(delete.clone(),10),
            Frame::NodeDeleted{op:delete.clone(),forwarded_by:None},
            Frame::Concern{role:PrimaryRole::FabricPrimary,node_id:own.node.node_id.clone(),incarnation:own.node.incarnation.clone(),observer:NodeId::mint().to_string()},
            Frame::NodeConnectionsDeleting{op:delete.clone(),forwarded_by:None},Frame::NodeConnectionsDeleted{op:delete.clone(),forwarded_by:None},Frame::NodeLeaving{op:delete.clone(),forwarded_by:None},Frame::NodeLeft{op:delete,forwarded_by:None},
            Frame::NewFabricPrimary{primary:primary_of(&restarted,99),endpoint_id:bad.to_string(),transport_addr:"127.0.0.1:12099".parse().unwrap()},
        ];
        for frame in frames.drain(..) {
            serial+=1;send(&sender,&receiver,frame,serial,&author).await;
            assert_eq!(receiver.held_source_version("mesh1"),Some((None,100)));
            for held in &before {assert_eq!(receiver.digest_view.get(held.node.node_id.as_str()).unwrap().0,*held,"no category moves back the read-installed birth");}
            assert_eq!((receiver.mesh_status("mesh1"),receiver.fabric_status()),statuses);
            assert_eq!(receiver.primaries().node_type_primary("mesh1","node"),Some(primary_of(&rpc_primary,10)));
            assert_eq!(receiver.primaries().mesh("mesh1"),Some(primary_of(&own,10)));assert_eq!(receiver.primaries().fabric(),Some(primary_of(&own,10)));
            assert_eq!((*births.borrow(),*changes.borrow(),*overlays.borrow(),*roles.borrow()),counts);
            assert!(receiver.digest_view.in_flight().is_empty());assert!(receiver.digest_view.is_deleted(deleted.node.node_id.as_str()));assert!(receiver.digest_view.get(deleted.node.node_id.as_str()).is_none());
            assert!(receiver.desynced_sources().is_empty());assert!(receiver_channels.location_view().addrs(bad).is_empty(),"refused gossip supplies no endpoint");
        }
        server_ep.close().await;receiver_ep.close().await;
        }.instrument(root).await;
    });
    let spans=capture.spans().unwrap();
    assert!(!spans.iter().any(|s| s["name"]=="rdm.mesh.topology.reject.via-undecodable-frame"), "every category reaches its receive rule on a valid envelope");
    for s in &spans {if s["name"].as_str().is_some_and(|n|n.starts_with("rdm.mesh.topology.")||n.contains("topology")){println!("{}",serde_json::to_string(s).unwrap());}}
    assert!(spans.iter().any(|s|s["name"]=="rdm.mesh.topology.update.via-aggregate-installed"&&s["attributes"]["node"]==receiver_id.as_str()&&s["attributes"]["channel"]=="topology-read"&&s["attributes"]["topology_version"]=="100"));
    for frame in ["digest","mesh-status","fabric-status","mesh-topology","mesh-topology-delta"] {assert!(spans.iter().any(|s|s["name"]=="rdm.mesh.topology.reject.via-stale"&&s["attributes"]["frame"]==frame),"{frame} names its stale refusal after the read");}
    // The read holds the deletion by the node's status (Q390), with no operation identity: the later
    // NodeDeleted is refused by name against the departure it holds, which stands.
    assert!(spans.iter().any(|s|s["name"]=="rdm.mesh.topology.reject.via-deleted-birth"&&s["attributes"]["frame"]=="node-deleted"&&s["attributes"]["observer"]==receiver_id.as_str()),"the NodeDeleted after the read is refused by name");
    for position in 10..=15 {assert!(spans.iter().any(|s|s["name"]=="rdm.mesh.topology.reject.via-reserved"&&s["attributes"]["position"]==position.to_string()));}
}
