//! product=mesh, feature=node-records, subfeature=own-properties, rung=MN, provider=process.

use rdm_test::estate::{wait_for, Estate, Owner};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;

// @feature: node-records
/// CONTRACT (i150 R3/R7/R8): Builds apply entry properties to an existing node; a subsequent
/// own update stands through reattaching the same Build; a later Build overrides it. Publishing
/// the own map is one node-owned durable write, with no upward config delivery or copied atlas.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_applies_properties_and_a_nodes_later_update_stands() {
    let test = "a_build_applies_properties_and_a_nodes_later_update_stands";
    let owner = Owner {
        product: "mesh".into(),
        feature: "node-records".into(),
        subfeature: "build-properties".into(),
        rung: "MN".into(),
        provider: "process".into(),
        test: test.into(),
    };
    let mut estate = Estate::bootstrap(owner, "fabric1", "mesh1").await;
    let shape = [("mesh1", 1, 2)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settled_views(&shape).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let subject: rdm_core::PathName = rdm_core::PathName::new("mesh1.rpc", "2").unwrap();
    let change = |value: &'static str| {
        let subject = subject.clone();
        move |current: &rdm_test::executables::Current, _: &rdm_test::executables::Executables| {
            current
                .topology
                .values()
                .flat_map(|mesh| mesh.values())
                .cloned()
                .map(|mut spec| {
                    if spec.path == subject {
                        spec.config = [("entry".into(), value.into())].into();
                        spec.tags = [value.into()].into();
                    }
                    spec
                })
                .collect()
        }
    };
    let stream = estate
        .submit_named(
            "Build A applies an existing node's properties",
            change("build-a"),
        )
        .await;
    let applied = estate.follow(stream).await;
    let before = estate.node_view(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await;
    assert_eq!(before.config, [("entry".into(), "build-a".into())].into());
    assert_eq!(before.tags, ["build-a".into()].into());
    let answer = estate.probe(&[
        "own-config",
        "--target",
        &format!("exact:{}", before.node_id),
        "--value",
        "listener.data=127.0.0.1:45005",
    ]);
    assert_eq!(answer["own_config"], "config-updated", "{answer}");
    let own: BTreeMap<String, String> = [("listener.data".into(), "127.0.0.1:45005".into())].into();
    let again = estate
        .carrier()
        .await
        .reattach(&applied.build_id)
        .await
        .expect("the same Build reattaches");
    estate.follow(again.stream).await;
    let after = estate.node_view(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await;
    assert_eq!(
        after.node_id, before.node_id,
        "in-place property operations preserve identity"
    );
    assert_eq!(
        after.config, own,
        "reattaching the same Build preserves the later own map"
    );
    assert_eq!(after.tags, before.tags);
    let stream = estate
        .submit_named(
            "Build B overrides the existing node's own properties",
            change("build-b"),
        )
        .await;
    estate.follow(stream).await;
    let later = estate.node_view(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await;
    assert_eq!(later.node_id, before.node_id);
    assert_eq!(later.config, [("entry".into(), "build-b".into())].into());
    assert_eq!(later.tags, ["build-b".into()].into());
    estate.artifact(
        "build-property-readbacks",
        &json!({"build_id": applied.build_id, "before": before, "own": after, "later": later}),
    );
    estate.stop().await;
    let spans = estate.spans();
    let own_write = rdm_test::estate::named(&spans, "rdm.node.record.update.via-properties")
        .into_iter()
        .find(|s| s["attributes"]["node_id"] == before.node_id.as_str())
        .expect("the production node-owned write occurred");
    estate.record_trace_url(own_write["trace_id"].as_str().unwrap());
    assert!(
        rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-config")
            .into_iter()
            .all(|s| s["attributes"]["subject"] != "mesh1.rpc.2"
                || s["attributes"]["apply"] != "own"),
        "an own update must not deliver the map to a mesh-primary"
    );
    assert!(
        rdm_test::estate::named(&spans, "rdm.node_admin.node.update.via-config-push").is_empty(),
        "node properties must not be copied upward to the fabric-primary"
    );
}

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
        .find(|n| n.name == rdm_core::PathName::new("mesh1.rpc", "2").unwrap())
        .expect("the non-primary subject is ready");
    let expected: BTreeMap<String, String> =
        [("listener.data".into(), "127.0.0.1:45005".into())].into();
    let answer = estate.probe(&[
        "own-config",
        "--target",
        &format!("exact:{}", before.node_id),
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
    // Evidence export is batched. Observe the completed write span before SIGKILL so this
    // real-process canary retains its flame; storage durability is independently checked by
    // the forced-exit storage canary, which performs no telemetry flush or wait.
    let write_trace = wait_for(
        "the acknowledged own-write span is exported",
        Duration::from_secs(10),
        || async {
            estate
                .spans_named(&["rdm.node.record.update.via-properties"])
                .into_iter()
                .find(|s| s["attributes"]["node_id"] == before.node_id.as_str())
                .and_then(|s| s["trace_id"].as_str().map(str::to_string))
        },
    )
    .await;
    estate.kill_node(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await;
    let recovered = wait_for(
        "a recovered node exposes the acknowledged config",
        crate::node_recovery_due(),
        || async {
            estate.node_views().await.into_iter().find(|n| {
                n.name == rdm_core::PathName::new("mesh1.rpc", "2").unwrap()
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
        .rev()
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
    assert_eq!(
        written["trace_id"].as_str(),
        Some(write_trace.as_str()),
        "the retained flame is the observed own write"
    );
    estate.record_trace_url(&write_trace);
}
