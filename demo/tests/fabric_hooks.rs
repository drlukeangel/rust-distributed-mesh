//! The Fabric hook a consumer registers, on the demo's own node processes (fabric-node-hooks.md
//! `on_fabric_ready`; Q175, Q295). `rshape-node-admin`'s `on_fabric_ready` notice, after the fabric is
//! ready, does the replica's day-0 work on another node, as Rafka's does its system creations: it
//! produces one record to the `system` topic through a gateway, under the ready Build's operation id.
//!
//! CONTRACT (fabric-node-hooks.md `on_fabric_ready`: "After the ready state is set, the fabric-primary
//! sends the application one `on_fabric_ready` notice"): once the fabric is ready after the Build that
//! brought the gateway and the brokers up, the `system` topic, read through the gateway, holds that
//! Build's day-0 record, once. Must NOT happen: no record for the ready Build, or a record twice.

mod common;

use rdm_core::NodeKind;

const GATEWAY: &str = "mesh1.gateway.1";

/// A Build id's characters, whatever separators its rendering uses.
fn bare(id: &str) -> String {
    id.trim_start_matches("bid_").chars().filter(|c| c.is_ascii_alphanumeric()).collect()
}

#[test]
fn on_fabric_ready_produces_the_day_zero_record_through_the_gateway_once() {
    common::run(async {
        let estate = common::estate("demo-fabric-hooks", "on_fabric_ready_produces_the_day_zero_record_through_the_gateway_once", &[(NodeKind::NodeAdmin, 1), (NodeKind::Gateway, 1), (NodeKind::Broker, 2)]).await;
        estate.until_fabric_ready(common::SETTLE).await;
        let ready_build = bare(&estate.accepted_build_id().await);
        let until = std::time::Instant::now() + common::SETTLE;
        let records = loop {
            let records = common::fetch(&estate, GATEWAY, "system").await;
            if records.iter().any(|(_, r)| bare(r).contains(&ready_build)) {
                break records;
            }
            assert!(std::time::Instant::now() < until, "the system topic holds no day-0 record of Build {ready_build}: {records:?}");
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        };
        let mut seen = std::collections::BTreeSet::new();
        for (_, record) in &records {
            assert!(seen.insert(record.clone()), "a day-0 record is produced once per ready Build: {records:?}");
        }
        estate.shutdown().await;
    });
}
