//! The app's way to read rafka-time: the handle a running node hands back.

use crate::common::{admin_side_on_rafka_time, TEST_MESH_ID};
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId};
use rdm_test::node;
use std::time::Duration;

/// A reference no OS clock reads: 1970-03-23.
const PRIMARY_REFERENCE_MS: u64 = 7_000_000_000;

// @feature: node-lifecycle
/// CONTRACT: the handle `RunningNode::rafka_time` is the one reader the node's gossip stamps read.
/// A node admitted by an authority whose rafka-time is 7 000 000 000 ms reads that lineage through
/// the handle, never the OS clock; the `emitted_at_rafka_ms` of a heartbeat the authority hears lies
/// between two readings of the handle taken before and after it; and the topology view's clock is the
/// same instance, so a clone of the handle and the stamps never disagree.
#[test]
fn the_handle_a_running_node_returns_reads_the_clock_its_gossip_stamps_read() {
    let cap = crate::common::capture("the_handle_a_running_node_returns_reads_the_clock_its_gossip_stamps_read");
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("rafka-time-handle-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let fabric = FabricId::mint();
        let admin = admin_side_on_rafka_time("127.0.0.1".parse().unwrap(), &fabric, PRIMARY_REFERENCE_MS).await;
        let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: fabric.clone(),
            name: "mesh1.rpc.1".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            seeds: vec![admin.seed.clone()],
            launcher: Some(admin.launcher.clone()),
            data_dir: dir.clone(),
            store_dir: dir.clone(),
            mesh_id: Some(MeshId::parse(TEST_MESH_ID).unwrap()),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        admin.deployed(&launch, &node::load_or_mint_key(&dir).unwrap());
        let running = node::start(&launch, |b, _| b).await.expect("the admitted node starts");

        let handle = running.rafka_time.clone();
        let before = handle.now_ms();
        assert!((PRIMARY_REFERENCE_MS..PRIMARY_REFERENCE_MS + 60_000).contains(&before), "the node reads its authority's lineage, not an OS clock: {before}");

        // A heartbeat the authority hears after the first reading carries a stamp from the same clock.
        let node_id = launch.node_id.to_string();
        let heard = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some((d, _)) = admin.observer.topology_view.digest_view.get(node_id.as_str()) {
                    if d.emitted_at_rafka_ms >= before {
                        return d.emitted_at_rafka_ms;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the authority hears a heartbeat stamped after the first reading");
        let after = handle.now_ms();
        assert!((before..=after).contains(&heard), "the gossip stamp {heard} lies between two readings of the handle: {before}..{after}");

        // The topology view composes the handle's instance: its clock and the handle never disagree.
        let (a, via_topology_view, b) = (handle.now_ms(), running.topology_view.clock().now_rafka_ms(), handle.now_ms());
        assert!((a..=b).contains(&via_topology_view), "{a} <= {via_topology_view} <= {b}");

        running.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    })
}
