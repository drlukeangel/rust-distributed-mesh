//! Node config update workflow (`docs/architecture/lifecycles/node-config-update.md`).
//!
//! Updates configuration for a node without restarting it, writing updated rows and
//! notifying the target node.

pub use crate::node_config::{NodeConfigs, NodeConfigsSlot};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atlas::{Atlas, ConfigInstall};
    use crate::fabric_storage::MemoryFabricStorage;
    use crate::model::{
        EndpointId, Fabric, FabricId, IncarnationId, Mesh, MeshId, Node, NodeId, NodeKind,
        NodeStatus, PathName, ProviderKind, ScopeStatus,
    };
    use crate::node_config::SentBy;
    use crate::status_rpc::{ConfigHeld, Declared, StatusAuthority};
    use crate::storage::{MemoryMeshStorage, MemoryNodesStorage, MeshStorage, NodeRow, NodesStorage};
    use crate::topology::Topology;
    use rdm_core::meta::{PlacementMeta, StorageMeta};
    use rdm_core::status::{NotPrimary, StatusReply, StatusRequest};
    use rdm_transport::{NodeRpcClient, StaticResolver};
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex};

    struct RefusingNodesStorage(MemoryNodesStorage);

    #[async_trait::async_trait]
    impl NodesStorage for RefusingNodesStorage {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.0.runtimes().await
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.0.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, crate::storage_error::StorageError> {
            self.0.node(node_id).await
        }
        async fn node_at(&self, name: &PathName) -> Result<Option<NodeRow>, crate::storage_error::StorageError> {
            self.0.node_at(name).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, crate::storage_error::StorageError> {
            self.0.nodes(mesh).await
        }
        async fn put_node(&self, row: &NodeRow) -> Result<(), crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io {
                file: format!("nodes/records/{}", row.node_id),
                reason: "simulated disk refusal".into(),
            })
        }
        async fn index_path(&self, name: &PathName, node_id: &NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.0.index_path(name, node_id).await
        }
        async fn index_tags(&self, mesh: &str, node_id: &NodeId, leaving: &BTreeSet<String>, arriving: &BTreeSet<String>) -> Result<(), crate::storage_error::StorageError> {
            self.0.index_tags(mesh, node_id, leaving, arriving).await
        }
        async fn tagged(&self, label: &str) -> Result<BTreeSet<NodeId>, crate::storage_error::StorageError> {
            self.0.tagged(label).await
        }
        async fn name_claim(&self, name: &PathName) -> Result<Option<NodeId>, crate::storage_error::StorageError> {
            self.0.name_claim(name).await
        }
        async fn claim_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<Result<(), crate::storage::NameHeld>, crate::storage_error::StorageError> {
            self.0.claim_node_name(name, node_id).await
        }
        async fn release_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.0.release_node_name(name, node_id).await
        }
    }

    /// CONTRACT:
    /// - `docs/architecture/lifecycles/node-config-update.md:5` — "`node.config.update` replaces a node's application config, the whole map: a PUT, never a patch. It is a single call, not a Build. Config is a property bag the application stores on the node; RDM never reads it ([node properties](../node-rpc-envelope.md#node-properties))."
    /// - `docs/architecture/lifecycles/node-config-update.md:7` — "A node's first config is its create spec's `config` in the Build ([build.md §1](../build.md#1-build-is-the-only-topology-mutation-engine)), written to its row at the claim and carried in its hydration sync ([node.topology.get](node-topology-get.md)); a later Build's different `config` is applied as a caller's update is, and supersedes the node's current map, whoever set it."
    /// - `docs/architecture/lifecycles/node-config-update.md:12` — "The update runs one of two ways. A caller's goes down from the fabric-primary. A node's own goes up from the node, so its mesh-primary and the fabric-primary hold the map the node will be hydrated with and a Build is diffed against; every install, at the node and in each atlas, is ordered by the row's revision."
    /// - `docs/architecture/lifecycles/node-config-update.md:21` — "1. The caller sends `node.config.update` to the fabric-primary, which sends the command to the node's mesh-primary (Forward `0x1A` when not directly reachable)."
    /// - `docs/architecture/lifecycles/node-config-update.md:23` — "2. The mesh-primary writes the record, stepping its revision, and its atlas."
    /// - `docs/architecture/lifecycles/node-config-update.md:24` — "3. A live node is sent `ApplyNodeConfig` with the map and its revision; it installs the map over an older revision. The command completes after `Applied` or `AlreadyApplied`. A stopped node is sent nothing: the command completes after the durable row write, and its next start receives the config in its hydration sync."
    /// - `docs/architecture/lifecycles/node-config-update.md:28` — "4. The completion carries the row as written; the fabric-primary writes it into its atlas over an older revision. Emit `node.config.updated`."
    /// - `docs/architecture/lifecycles/node-config-update.md:33` — "1. The node holds the map and sends it to the mesh-primary its topology view names, owed until answered and retargeted on a primary change; in process when that mesh-primary is this process."
    /// - `docs/architecture/lifecycles/node-config-update.md:35` — "2. The mesh-primary writes the record, stepping its revision, and its atlas, and answers the node `Applied` or refuses it by name; either answer carries the row as written, which the node installs over an older revision."
    /// - `docs/architecture/lifecycles/node-config-update.md:38` — "3. The mesh-primary owes the row to the fabric-primary, keyed by the node and its revision, until acknowledged; the fabric-primary writes it into its atlas over an older revision. A successor mesh-primary owes it again from the row. A mesh-primary that is the fabric-primary writes both at step 2."
    /// - `docs/architecture/lifecycles/node-config-update.md:42` — "4. Emit `node.config.updated`."
    /// - `docs/architecture/lifecycles/node-config-update.md:44` — "Row first, node second. It is admitted at any time (Q166), and never refused because a Build is running: every change to a node record goes through its mesh-primary's one held record, one at a time, so any interleaving with a Build is a serial order of the two. Two topology changes never run together; a second is refused `BuildInProgress` ([Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm))."
    /// - `docs/architecture/lifecycles/node-config-update.md:51` — "Command and completion messages (`node.config.update` -> write record -> `ApplyNodeConfig` -> `node.config.updated`; own `node.config.update` -> write record -> `node.config.updated` -> `NodeConfigUpdated`)."
    /// - `docs/architecture/lifecycles/node-config-update.md:63` — "Failures and stream certainty follow the [caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty)."
    /// - `docs/architecture/lifecycles/node-config-update.md:68` — "Trace propagation, step spans and correlation follow the [runtime spans](../node-rpc-envelope.md#runtime-spans)."
    #[tokio::test]
    async fn node_config_update_caller_and_own_sequences_revision_ordering_build_diff_and_refusals() {
        let admin_path = PathName::new("mesh1", NodeKind::NodeAdmin, 1);
        let admin_id = NodeId::mint();
        let admin_inc = IncarnationId::mint();
        let mut admin1 = Node::allocated(admin_path.clone());
        admin1.kind = NodeKind::NodeAdmin;
        admin1.mesh = "mesh1".to_string();
        admin1.node_id = admin_id.clone();
        admin1.incarnation_id = Some(admin_inc.clone());
        admin1.endpoint_id = Some(EndpointId("ep-admin-1".to_string()));
        admin1.status = NodeStatus::Ready;
        admin1.is_primary = true;
        admin1.is_fabric_primary = true;

        let rpc_path = PathName::new("mesh1", NodeKind::RpcNode, 1);
        let rpc_id = NodeId::mint();
        let rpc_inc = IncarnationId::mint();
        let mut rpc1 = Node::allocated(rpc_path.clone());
        rpc1.kind = NodeKind::RpcNode;
        rpc1.mesh = "mesh1".to_string();
        rpc1.node_id = rpc_id.clone();
        rpc1.incarnation_id = Some(rpc_inc.clone());
        rpc1.status = NodeStatus::Leaving;
        rpc1.stopped = true;

        let view = Topology {
            fabric: Fabric {
                id: FabricId::mint(),
                name: "fabric1".to_string(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: vec![Mesh {
                id: Some(MeshId::mint()),
                name: "mesh1".to_string(),
                status: ScopeStatus::Ready,
            }],
            nodes: vec![admin1.clone(), rpc1.clone()],
        };
        let topology = Arc::new(tokio::sync::RwLock::new(view.clone()));
        let nodes_storage = Arc::new(MemoryNodesStorage::default());
        let atlas = Arc::new(Atlas::default());

        let held_events: Arc<Mutex<Vec<ConfigHeld>>> = Arc::default();
        let configure: crate::status_rpc::Configure = Arc::new(std::sync::OnceLock::new());
        {
            let held_events = held_events.clone();
            let _ = configure.set(Arc::new(move |ev| {
                let held_events = held_events.clone();
                Box::pin(async move {
                    held_events.lock().unwrap().push(ev);
                })
            }));
        }

        let authority = Arc::new(StatusAuthority {
            certs: None,
            app_channels: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: admin_path.clone(),
            node_id: admin_id.clone(),
            fabric_id: view.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            meshes_storage: Arc::new(MemoryMeshStorage::default()),
            fabric_storage: Arc::new(MemoryFabricStorage::new()),
            nodes_storage: nodes_storage.clone(),
            caches: Default::default(),
            mesh_ids: Arc::new(BTreeMap::new),
            durable: Default::default(),
            commands: Arc::new(crate::node_commands::CommandView::default()),
            own: Arc::new(std::sync::OnceLock::new()),
            mesh_deleter: Arc::new(std::sync::OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            fabric_states: Default::default(),
            atlas: Default::default(),
            fabric_recovery: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            configure: configure.clone(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: Some(Arc::new(BTreeMap::new)),
            digest_view: None,
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        });
        authority.caches.nodes.feeds(atlas.clone());
        let _ = authority.atlas.set(atlas.clone());

        let ep = rdm_transport::endpoint::bind(
            iroh::SecretKey::from_bytes(&[19u8; 32]),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let client = Arc::new(NodeRpcClient::new(ep.clone(), Arc::new(StaticResolver::new())).with_caller_system("rdm"));
        let door = Arc::new(NodeConfigs::new(client, configure.clone(), authority.caches.nodes.clone()));
        let _ = authority.configs.set(door.clone());

        // node-config-update.md:7-9 — First config written to the node's row at claim.
        let initial_config = BTreeMap::from([("initial".to_string(), "v0".to_string())]);
        let mut initial_row = NodeRow::new(rpc_path.clone(), rpc_id.clone(), NodeStatus::Leaving);
        initial_row.config = initial_config.clone();
        authority.caches.nodes.put(&*nodes_storage, &initial_row).await.unwrap();
        assert_eq!(atlas.record(&rpc_id).unwrap().config, initial_config);
        assert_eq!(atlas.record(&rpc_id).unwrap().revision, 1);

        // node-config-update.md:21-29 — Caller's update on stopped node completes on the durable row write
        // without sending ApplyNodeConfig, steps revision 1 -> 2, updates atlas, and emits ConfigUpdated.
        let caller_map = BTreeMap::from([("zone".to_string(), "eu-west".to_string())]);
        let caller_req = StatusRequest::UpdateNodeConfig {
            node_id: rpc_id.clone(),
            incarnation: rpc_inc.clone(),
            config: caller_map.clone(),
        };
        let caller_reply = authority.apply(Some(rpc1.clone()), &caller_req).await;
        assert_eq!(caller_reply.name(), "config-updated");
        assert_eq!(
            caller_reply,
            StatusReply::ConfigUpdated {
                node_id: rpc_id.clone(),
                incarnation: rpc_inc.clone(),
                config: caller_map.clone(),
                revision: 2,
            }
        );
        assert_eq!(atlas.record(&rpc_id).unwrap().config, caller_map);
        assert_eq!(atlas.record(&rpc_id).unwrap().revision, 2);

        // node-config-update.md:9-11 — Later Build's different config is applied as a caller's update is
        // (`update_config_from_spec`) and supersedes the current map, stepping revision 2 -> 3.
        let build_map = BTreeMap::from([("zone".to_string(), "us-east".to_string()), ("tier".to_string(), "1".to_string())]);
        let build_req = StatusRequest::UpdateNodeConfig {
            node_id: rpc_id.clone(),
            incarnation: rpc_inc.clone(),
            config: build_map.clone(),
        };
        let build_reply = authority
            .update_config_from_spec(&build_req, StorageMeta::Ephemeral, PlacementMeta::default())
            .await;
        assert_eq!(
            build_reply,
            StatusReply::ConfigUpdated {
                node_id: rpc_id.clone(),
                incarnation: rpc_inc.clone(),
                config: build_map.clone(),
                revision: 3,
            }
        );
        assert_eq!(nodes_storage.node(&rpc_id).await.unwrap().unwrap().config, build_map);
        assert_eq!(atlas.record(&rpc_id).unwrap().revision, 3);

        // node-config-update.md:33-42 — A node's own update in-process at the mesh-primary (which is also
        // the fabric-primary) holds Sent at once, writes both row and atlas at step 2, and installs Answered.
        let own_map = BTreeMap::from([("admin_api_base".to_string(), "rdm://mesh1.admin.1".to_string())]);
        let own_reply = authority.own_config_update(admin_inc.clone(), own_map.clone()).await;
        assert_eq!(
            own_reply,
            StatusReply::ConfigUpdated {
                node_id: admin_id.clone(),
                incarnation: admin_inc.clone(),
                config: own_map.clone(),
                revision: 1,
            }
        );
        assert_eq!(
            held_events.lock().unwrap().clone(),
            vec![
                ConfigHeld::Sent(own_map.clone()),
                ConfigHeld::Answered(Some((own_map.clone(), 1))),
            ]
        );
        assert_eq!(atlas.record(&admin_id).unwrap().config, own_map);
        assert_eq!(atlas.record(&admin_id).unwrap().revision, 1);

        // node-config-update.md:14-15, 28, 38-39 — Atlas and node config install only over an older revision.
        let stale_push = StatusRequest::NodeConfigUpdated {
            node_id: rpc_id.clone(),
            config: BTreeMap::from([("stale".to_string(), "1".to_string())]),
            revision: 2,
        };
        assert_eq!(authority.apply(Some(admin1.clone()), &stale_push).await, StatusReply::AlreadyApplied);
        assert_eq!(atlas.record(&rpc_id).unwrap().config, build_map, "older revision must not overwrite newer revision 3");
        assert_eq!(
            atlas.install_config(&rpc_id, &BTreeMap::from([("newer".to_string(), "1".to_string())]), 4),
            ConfigInstall::Installed
        );
        assert_eq!(atlas.record(&rpc_id).unwrap().revision, 4);

        // node-config-update.md:35-37, 63 — Store refusal on own update returns ConfigRowRefused carrying
        // the standing row and revision so the node installs the standing row over an older revision.
        let refusing = RefusingNodesStorage(MemoryNodesStorage::default());
        let refused_own_req = StatusRequest::UpdateNodeConfig {
            node_id: rpc_id.clone(),
            incarnation: rpc_inc.clone(),
            config: BTreeMap::from([("unwritten".to_string(), "true".to_string())]),
        };
        let refused_reply = door.serve(&admin_path, &admin_id, &view, &topology, &refusing, Some(&atlas), SentBy::Own, &refused_own_req).await;
        assert!(
            matches!(
                &refused_reply,
                StatusReply::ConfigRowRefused { node, config, revision, .. }
                if node == "mesh1.rpc.1" && *config == build_map && *revision == 3
            ),
            "expected ConfigRowRefused carrying standing row (revision 3), got {refused_reply:?}"
        );

        // node-config-update.md:44-47 — Admitted at any time while a Build is in progress in builds.storage,
        // serialized through one_at_a_time.
        let guard = door.one_at_a_time().await;
        drop(guard);
        let unknown_push = StatusRequest::UpdateNodeConfig {
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            config: BTreeMap::new(),
        };
        assert_eq!(
            door.serve(&admin_path, &admin_id, &view, &topology, &*nodes_storage, Some(&atlas), SentBy::Own, &unknown_push).await,
            StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }
        );

        ep.close().await;
    }
}
