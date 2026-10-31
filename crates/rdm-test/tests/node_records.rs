//! product=mesh, feature=node-records, subfeature=own-properties, rung=MN, provider=process.

use rdm_test::estate::{wait_for, Estate, Owner};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;

// @feature: node-records
/// CONTRACT (i150 R3, restart canary): a real node acknowledges its own property write only
/// after that node has persisted it. Killing that process preserves the acknowledged map for
/// recovery under a new ID; the retained log names the original node as the writer. Admin
/// forwarding of the map cannot stand in for that node-owned durable write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn own_properties_survive_node_restart() {
    let test = "own_properties_survive_node_restart";
    let owner = Owner {
        product: "mesh".into(),
        feature: "node-records".into(),
        subfeature: "own-properties".into(),
        rung: "MN".into(),
        provider: "process".into(),
        test: test.into(),
    };
    let mut estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;
    let shape = [("mesh1", 1, 2)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settled_views(&shape).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let before = estate
        .node_views()
        .await
        .into_iter()
        .find(|n| n.name.to_string() == "mesh1.rpc.2")
        .expect("the non-primary subject is ready");
    let expected: BTreeMap<String, String> =
        [("listener.data".into(), "127.0.0.1:45005".into())].into();
    let answer = estate.probe(&[
        "own-config",
        "--target",
        "path:mesh1.rpc.2",
        "--value",
        "listener.data=127.0.0.1:45005",
    ]);
    assert_eq!(
        answer["own_config"], "config-updated",
        "the own update was acknowledged: {answer}"
    );
    estate.artifact(
        "acknowledged-properties",
        &json!({"node_id": before.node_id, "config": expected}),
    );
    estate.kill_node("mesh1.rpc.2").await;
    let recovered = wait_for(
        "a recovered node exposes the acknowledged config",
        crate::node_recovery_due(),
        || async {
            estate.node_views().await.into_iter().find(|n| {
                n.name.to_string() == "mesh1.rpc.2"
                    && n.node_id != before.node_id
                    && n.status == rdm_client::NodeStatus::Ready
            })
        },
    )
    .await;
    let records = estate
        .fabric_store_root()
        .join(rdm_node::node_records::NODE_RECORDS_FILE);
    estate.stop().await;
    assert_eq!(
        recovered.config, expected,
        "the complete acknowledged map survives forced process loss"
    );
    let log = rdm_node::store_log::read_file(&records).unwrap_or_else(|e| panic!("the node must persist its own properties before acknowledging them; retained record log: {e}"));
    let own = log
        .iter()
        .find(|line| line.kind() == "node-properties" && line.key == before.node_id.as_str())
        .expect("the original node's acknowledged record survives its process exit");
    let record: rdm_node::node_records::NodeProperties = own.decode(&records).unwrap();
    assert_eq!(
        record.config, expected,
        "the log contains the entire acknowledged map"
    );
    assert_eq!(
        own.op.as_ref().unwrap().node_id,
        before.node_id.to_string(),
        "the node itself wrote the acknowledged record"
    );
    let spans = estate.spans();
    let writes = rdm_test::estate::named(&spans, "rdm.node.record.update.via-properties");
    let written = writes
        .into_iter()
        .find(|s| s["attributes"]["node_id"] == before.node_id.as_str())
        .expect("the production own-write span proves this path executed");
    estate.record_trace_url(written["trace_id"].as_str().expect("the write has a trace"));
}
