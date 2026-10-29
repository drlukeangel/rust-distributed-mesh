//! Mesh create workflow (`docs/architecture/lifecycles/mesh-create.md`).
//!
//! The mesh-absent arm of `node.create`, run as a step of the Build whose accepted
//! topology names the new mesh. A mesh comes into being with its first node-admin.

pub use crate::deployment::pipeline::CreateMeshStep;
pub use crate::mesh_claims::{InFlight, Taken};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accepted::{compile, plan, AcceptedStore, FabricTopology, TopologyChange};
    use crate::admin::Records;
    use crate::build::{BuildId, BuildOperation, FabricDesired, MeshDesired};
    use crate::build_state::{
        BuildAccepted, BuildState, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome,
    };
    use crate::certs::CertIssuer;
    use crate::control_plane::{ControlPlane, Refusal};
    use crate::model::{
        EndpointId, Fabric, FabricId, IncarnationId, Mesh, MeshId, Node, NodeId, NodeKind,
        NodeStatus, PathName, ProviderKind, ScopeStatus,
    };
    use crate::node_rows::{ClaimFor, NodeRows};
    use crate::status_rpc::{Declared, StatusDoor};
    use crate::storage::{
        Allocation, MemoryMeshStorage, MemoryNodesStorage, MeshRecord, MeshStorage, NodesStorage,
    };
    use crate::topology::Topology;
    use iroh::protocol::Router;
    use iroh::SecretKey;
    use rdm_core::catalog::OpOwner;
    use rdm_core::certs::CaKey;
    use rdm_core::launch::{Launch, ENV_ISSUING_CERT, ENV_ISSUING_KEY};
    use rdm_core::outcome::RpcOutcome;
    use rdm_core::ping::{Ping, PingReply, PingRequest};
    use rdm_core::status::{MeshState, Status, StatusReply, StatusRequest};
    use rdm_transport::clock::RafkaTime;
    use rdm_transport::{
        CallOptions, LiveNodeResolver, NodeRpcClient, NodeTarget, ServedBirth, ServerBuilder,
    };
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;

    fn ready_node(name: &str, is_primary: bool, is_fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::Ready;
        n.is_primary = is_primary;
        n.is_fabric_primary = is_fabric_primary;
        n
    }

    fn base_view(fabric_id: &FabricId, mesh1_id: &MeshId) -> Topology {
        Topology {
            fabric: Fabric {
                id: fabric_id.clone(),
                name: "fabric1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: vec![Mesh {
                id: Some(mesh1_id.clone()),
                name: "mesh1".into(),
                status: ScopeStatus::Ready,
            }],
            nodes: vec![ready_node("mesh1.admin.1", true, true)],
        }
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-create.md:16 "Caller / driver | The topology Build that adds the mesh (build.md §12.1); the fabric-primary accepts it and drives it through the Build carrier"
    /// docs/architecture/lifecycles/mesh-create.md:17 "Precondition | The Day-0 Build is Complete; before its ready it is the Build in progress, and the Build is refused build-in-progress (Q170)"
    /// docs/architecture/lifecycles/mesh-create.md:18 "MeshId | Minted once, when the mesh enters the accepted topology, and carried by every Build after it; never reissued"
    /// docs/architecture/lifecycles/mesh-create.md:19 "Name | mesh<#> by default, the Build's mesh_name; the first node-admin, as the mesh's primary, claims it at (path, name), unique within its path; held by a different MeshId answers NameHeld"
    /// docs/architecture/lifecycles/mesh-create.md:20 "Executor / maker | The fabric-primary: it is the maker of the mesh's first node-admin and writes the mesh's rows until the mesh has a primary (R-OW1)"
    /// docs/architecture/lifecycles/mesh-create.md:21 "Channel | f(FabricId, MeshId), created by its first listener, the node-admin"
    /// docs/architecture/lifecycles/mesh-create.md:22 "Members | The first node-admin only. The mesh's other nodes are created afterwards by that node-admin as mesh-primary, under the same accepted Build's attempts"
    /// docs/architecture/lifecycles/mesh-create.md:24 "Rows | The mesh row, written by the mesh-primary at creation; the node row, written by the fabric-primary until the mesh has a primary, then by the mesh-primary"
    /// docs/architecture/lifecycles/mesh-create.md:25 "Completion | The node-admin's node.created; the mesh is in the topology from acceptance and in traffic from ready"
    /// docs/architecture/lifecycles/mesh-create.md:26 "Not this | A node-admin created into a mesh that already exists and lost its node-admins is mesh investigation: same MeshId, same Build, no mesh create"
    /// docs/architecture/lifecycles/mesh-create.md:30-32 "1. A topology Build is accepted whose complete topology names a mesh the current topology does not have ({mesh1} -> {mesh1, mesh2}). Acceptance mints mesh2's MeshId; every later Build carries it. The Build is in progress until its attempts realize the topology."
    /// docs/architecture/lifecycles/mesh-create.md:65-66 "The decision in step 1 is taken from the accepted topology, never from the presence of a channel. A mesh that exists in the topology and has no reachable node-admin is not this arm."
    /// docs/architecture/lifecycles/mesh-create.md:175-179 "A refused name claim (NameHeld by another MeshId) fails the create step by name and nothing is launched ... A mesh whose first node-admin exits after creation is not re-created: it is a live mesh that lost its node-admins and mesh investigation gives it one under the same MeshId."
    /// docs/architecture/lifecycles/mesh-create.md:186 "| Acceptance | fabric-primary | rdm.node_admin.build.create.via-accepted; the MeshId in the accepted topology | Build id |"
    #[tokio::test]
    async fn mesh_create_contract_precondition_mesh_id_executor_rows_and_not_this() {
        let capture = crate::span_capture::Capture::new(
            "mesh_create_contract_precondition_mesh_id_executor_rows_and_not_this",
        );
        let _g = tracing::dispatcher::set_default(&capture.dispatch);

        let fabric_id = FabricId::mint();
        let mesh1_id = MeshId::mint();
        let mut day0_view = base_view(&fabric_id, &mesh1_id);
        day0_view.fabric.status = ScopeStatus::Pending;

        // Precondition (line 17): while the Day-0 Build is still in progress (before its Ready / Complete),
        // submitting a Build that adds mesh2 is refused `BuildInProgress`.
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let day0_id = BuildId::mint();
        let root_top = FabricTopology::root("fabric1", "mesh1", mesh1_id.clone());
        builds
            .publish_accepted(&BuildAccepted {
                build_id: day0_id.clone(),
                topology: root_top.clone(),
                submitted_change: None,
                submitted_at_ms: 1,
            })
            .await
            .unwrap();
        let storage = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        crate::fabric_storage::claim_identity(
            &*storage,
            &crate::fabric_storage::FabricIdentity {
                fabric_id: fabric_id.clone(),
                name: "fabric1".into(),
            },
        )
        .await
        .unwrap()
        .unwrap();
        let accepted_store = Arc::new(AcceptedStore::new(storage, "mesh1.admin.1"));
        let time = RafkaTime::unadopted();
        time.adopt(1_700_000_000_000);
        let mut cp_pending = ControlPlane::new(
            builds.clone(),
            accepted_store,
            "mesh1.admin.1".parse().unwrap(),
            day0_view.clone(),
            time.clone(),
        );
        cp_pending.topology = Arc::new(tokio::sync::RwLock::new(day0_view));
        let add_mesh2 = TopologyChange::CreateMesh {
            desired: MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]),
        };
        match cp_pending.submit("create-mesh2-early", add_mesh2.clone()).await {
            Err(Refusal::BuildInProgress(running)) => {
                assert_eq!(running.current_build_id, day0_id);
            }
            other => panic!("expected BuildInProgress before Day-0 Build is Complete, got {other:?}"),
        }

        // Once the Day-0 Build is Complete and the fabric is Ready, the topology Build that adds mesh2
        // is accepted by the fabric-primary (`rdm.node_admin.build.create.via-accepted`), minting
        // mesh2's MeshId once (`{mesh1} -> {mesh1, mesh2}`).
        let settled_builds = Arc::new(MemoryBuildStateAdapter::new());
        let settled_view = base_view(&fabric_id, &mesh1_id);
        let seeded = AcceptedStore::seeded(
            &*settled_builds,
            fabric_id.clone(),
            root_top,
            "mesh1.admin.1",
        )
        .await
        .unwrap();
        let mut cp = ControlPlane::new(
            settled_builds.clone(),
            seeded,
            "mesh1.admin.1".parse().unwrap(),
            settled_view.clone(),
            time,
        );
        cp.topology = Arc::new(tokio::sync::RwLock::new(settled_view.clone()));

        let current_build_id = settled_builds
            .current_build()
            .await
            .unwrap()
            .into_build_id()
            .unwrap();
        let _ = cp.node_rows.set(Arc::new(MemoryNodesStorage::default()));
        let exe_dir = crate::test_support::tempdir("mesh-create-exe");
        let spec_with_exe = |path: &str| {
            use std::os::unix::fs::PermissionsExt;
            let p: PathName = path.parse().unwrap();
            let bin = exe_dir.join(p.kind.name());
            std::fs::write(&bin, format!("#!/bin/sh\n# {}\n", p.kind.name())).unwrap();
            std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
            rdm_core::meta::NodeSpec {
                path: p.clone(),
                storage_persistence: rdm_core::meta::StorageMeta::Ephemeral,
                placement: rdm_core::meta::PlacementMeta::default(),
                executable: Some(rdm_core::binding::Binding {
                    launch_id: p.kind.name().into(),
                    sha256: rdm_core::binding::sha256_file(&bin).unwrap(),
                    executable: bin,
                    image: None,
                }),
                tags: std::collections::BTreeSet::new(),
                config: std::collections::BTreeMap::new(),
            }
        };
        let add_mesh2_reconcile = TopologyChange::Reconcile {
            based_on: current_build_id,
            nodes: vec![
                spec_with_exe("mesh1.admin.1"),
                spec_with_exe("mesh2.admin.1"),
                spec_with_exe("mesh2.rpc.1"),
            ],
            topology_storage_provider: None,
        };

        let opened = cp
            .submit("create-mesh2", add_mesh2_reconcile)
            .await
            .expect("CreateMesh accepted once Day-0 Build is Complete");
        let build_proj = settled_builds.read_build(&opened.build_id).await.unwrap();
        assert_eq!(
            build_proj.state,
            BuildState::Pending,
            "the Build is in progress until its attempts realize the topology"
        );
        let mesh2_top = build_proj
            .topology
            .mesh("mesh2")
            .expect("mesh2 is in the accepted topology from acceptance");
        let mesh2_id = mesh2_top.mesh_id.clone();
        assert_ne!(mesh2_id, mesh1_id);

        let accepted_spans = capture.named("rdm.node_admin.build.create.via-accepted");
        let build_id_str = opened.build_id.log().to_string();
        assert!(
            accepted_spans.iter().any(|s| {
                s.attributes.get("build_id").map(String::as_str) == Some(build_id_str.as_str())
            }),
            "Acceptance emits rdm.node_admin.build.create.via-accepted correlated by Build id: {accepted_spans:?}"
        );

        // MeshId (line 18, 30-32): carried unchanged by every Build after it; never reissued.
        let grown = compile(
            &build_proj.topology,
            &TopologyChange::ReconcileMesh {
                desired: MeshDesired::of(
                    "mesh2",
                    [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 2)],
                ),
            },
            &settled_view,
        )
        .unwrap();
        assert_eq!(grown.mesh("mesh2").unwrap().mesh_id, mesh2_id);
        let whole = compile(
            &grown,
            &TopologyChange::ReconcileFabric {
                desired: FabricDesired {
                    fabric: "fabric1".into(),
                    meshes: vec![
                        MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1)]),
                        MeshDesired::of(
                            "mesh2",
                            [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 2)],
                        ),
                    ],
                },
            },
            &settled_view,
        )
        .unwrap();
        assert_eq!(whole.mesh("mesh2").unwrap().mesh_id, mesh2_id);

        // Executor / maker & Members (lines 20, 22): while mesh2 has no primary, the fabric-primary
        // (`mesh1.admin.1`) executes `CreateMesh` and `CreateNode` of `mesh2.admin.1` on its own
        // (`first_admin_of` is true only for `mesh2.admin.1`); once `mesh2.admin.1` is Ready,
        // `mesh2.rpc.1` is never created by the fabric-primary (`executor_for` is `None` until the
        // election seats `mesh2.admin.1`, and `Some("mesh2.admin.1")` once it is mesh-primary).
        let initial_plan = plan(&build_proj.topology, &settled_view, None);
        assert!(initial_plan.operations.contains(&BuildOperation::CreateMesh {
            mesh: "mesh2".into(),
            mesh_id: mesh2_id.clone(),
        }));
        let m2_admin_path: crate::model::PathName = "mesh2.admin.1".parse().unwrap();
        let m2_rpc_path: crate::model::PathName = "mesh2.rpc.1".parse().unwrap();
        assert!(
            settled_view.first_admin_of(&m2_admin_path),
            "mesh2.admin.1 is the first node-admin of mesh2 and runs on its own"
        );
        assert!(
            !settled_view.first_admin_of(&m2_rpc_path),
            "mesh2.rpc.1 is not the first node-admin of mesh2"
        );
        let create_m2_admin = BuildOperation::CreateNode {
            node: m2_admin_path,
            over: None,
            executable: None,
            entry: None,
        };
        let create_m2_rpc = BuildOperation::CreateNode {
            node: m2_rpc_path,
            over: None,
            executable: None,
            entry: None,
        };
        assert_eq!(
            crate::executor::executor_for(&create_m2_admin, &settled_view)
                .map(|n| n.to_string()),
            Some("mesh1.admin.1".into()),
            "fabric-primary is maker and executor of mesh2's first node-admin"
        );
        let mut view_with_m2_admin_unseated = settled_view.clone();
        view_with_m2_admin_unseated.meshes.push(Mesh {
            id: Some(mesh2_id.clone()),
            name: "mesh2".into(),
            status: ScopeStatus::Pending,
        });
        view_with_m2_admin_unseated
            .nodes
            .push(ready_node("mesh2.admin.1", false, false));
        assert_eq!(
            crate::executor::executor_for(&create_m2_rpc, &view_with_m2_admin_unseated),
            None,
            "once mesh2.admin.1 is born, the fabric-primary does not create mesh2's other nodes while waiting for mesh2's own primary seat"
        );

        let mut view_with_m2_primary = settled_view.clone();
        view_with_m2_primary.meshes.push(Mesh {
            id: Some(mesh2_id.clone()),
            name: "mesh2".into(),
            status: ScopeStatus::Pending,
        });
        view_with_m2_primary
            .nodes
            .push(ready_node("mesh2.admin.1", true, false));
        assert_eq!(
            crate::executor::executor_for(&create_m2_rpc, &view_with_m2_primary)
                .map(|n| n.to_string()),
            Some("mesh2.admin.1".into()),
            "once mesh2.admin.1 is mesh-primary, it creates mesh2's other nodes under the same Build"
        );

        // Not this (lines 26, 65-66, 177-179): when mesh2 already exists in the topology and lost its
        // node-admins (mesh2 is in observed.meshes, mesh2.admin.1 is PendingReconnect/unheard), the plan
        // keeps the same MeshId and plans NO CreateMesh.
        let mut view_lost_m2_admin = view_with_m2_primary.clone();
        view_lost_m2_admin.nodes.retain(|n| n.mesh != "mesh2");
        let lost_plan = plan(&build_proj.topology, &view_lost_m2_admin, None);
        assert!(
            !lost_plan
                .operations
                .iter()
                .any(|op| matches!(op, BuildOperation::CreateMesh { .. })),
            "a live mesh that lost its node-admins is not re-created: {lost_plan:?}"
        );
        assert_eq!(
            build_proj.topology.mesh("mesh2").unwrap().mesh_id,
            mesh2_id,
            "same MeshId is preserved"
        );

        // Channel (line 21): f(FabricId, MeshId), distinct from mesh1's channel.
        assert_ne!(
            rdm_transport::topology::mesh_channel(&fabric_id, &mesh1_id),
            rdm_transport::topology::mesh_channel(&fabric_id, &mesh2_id),
            "mesh2's channel f(FabricId, MeshId) is distinct from mesh1's"
        );

        // Name & Failure and receipts (lines 19, 24, 175-177): a name held by a different MeshId answers
        // NameHeld, receipts ClaimMeshName as Failed, and writes nothing of the mesh; when free, the
        // mesh row is written and the fabric-primary writes mesh2.admin.1's node row until mesh2 has a
        // primary, after which mesh2.admin.1 writes mesh2's node rows.
        let meshes_held = MemoryMeshStorage::default();
        let other_id = MeshId::mint();
        crate::storage::claim_mesh(
            &meshes_held,
            None,
            &MeshRecord::new(other_id.clone(), "mesh2"),
        )
        .await
        .unwrap()
        .expect("other_id claims mesh2 first");
        let create_mesh_op = BuildOperation::CreateMesh {
            mesh: "mesh2".into(),
            mesh_id: mesh2_id.clone(),
        };
        let err = crate::admin::create_mesh_step(
            &Records::default(),
            &meshes_held,
            &*settled_builds,
            "key1@127.0.0.1:1",
            "mesh1.admin.1",
            &opened.build_id,
            1,
            &create_mesh_op,
        )
        .await
        .expect_err("NameHeld by another MeshId fails the create step by name");
        assert!(err.contains(&other_id.to_string()), "{err}");
        let failed_steps = settled_builds
            .read_build(&opened.build_id)
            .await
            .unwrap()
            .steps;
        assert!(
            failed_steps.iter().any(|r| {
                r.operation == "create-mesh:mesh2"
                    && r.step == CreateMeshStep::ClaimMeshName.name()
                    && matches!(&r.outcome, StepOutcome::Failed { reason, .. } if reason.contains(&other_id.to_string()))
            }),
            "refused name claim receipts ClaimMeshName as Failed naming the holder: {failed_steps:?}"
        );

        let meshes_free = MemoryMeshStorage::default();
        let records = Records::default();
        crate::admin::create_mesh(
            &records,
            &meshes_free,
            "mesh1.admin.1",
            "mesh2",
            &mesh2_id,
        )
        .await
        .expect("free mesh name is claimed");
        // Mesh-primary (`mesh2.admin.1`) re-claiming the same MeshId at seat take is idempotent (`Held`).
        let primary_claim = crate::admin::claim_mesh_name(
            &meshes_free,
            &records.caches,
            "mesh2.admin.1",
            "mesh2",
            &mesh2_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(primary_claim, crate::storage::MeshClaim::Held);

        // Node rows (line 24): written by the fabric-primary for mesh2.admin.1 while mesh2 has no
        // primary, then by mesh2.admin.1 for mesh2.rpc.1.
        let nodes_store = Arc::new(MemoryNodesStorage::default());
        let fp_rows = NodeRows::new(
            "mesh1.admin.1".parse().unwrap(),
            settled_view.nodes[0].node_id.clone(),
            Arc::new(tokio::sync::RwLock::new(settled_view.clone())),
            nodes_store.clone(),
        );
        let m2_admin_id = view_with_m2_primary.nodes.last().unwrap().node_id.clone();
        let m2_admin_alloc = Allocation {
            build_id: opened.build_id.clone(),
            operation: "node.create:mesh2.admin.1".into(),
            incarnation: IncarnationId::mint(),
            deployment_id: crate::model::DeploymentId::mint(),
        };
        fp_rows
            .claim(
                &"mesh2.admin.1".parse().unwrap(),
                &m2_admin_id,
                "node.create:mesh2.admin.1",
                Some(&m2_admin_alloc),
                None,
                ClaimFor::Create,
            )
            .await
            .expect("fabric-primary writes mesh2.admin.1's node row");
        let m2_primary_rows = NodeRows::new(
            "mesh2.admin.1".parse().unwrap(),
            m2_admin_id,
            Arc::new(tokio::sync::RwLock::new(view_with_m2_primary)),
            nodes_store.clone(),
        );
        let m2_rpc_id = NodeId::mint();
        let m2_rpc_alloc = Allocation {
            build_id: opened.build_id.clone(),
            operation: "node.create:mesh2.rpc.1".into(),
            incarnation: IncarnationId::mint(),
            deployment_id: crate::model::DeploymentId::mint(),
        };
        m2_primary_rows
            .claim(
                &"mesh2.rpc.1".parse().unwrap(),
                &m2_rpc_id,
                "node.create:mesh2.rpc.1",
                Some(&m2_rpc_alloc),
                None,
                ClaimFor::Create,
            )
            .await
            .expect("mesh2.admin.1 as mesh-primary writes mesh2.rpc.1's node row");
        assert_eq!(nodes_store.nodes("mesh2").await.unwrap().len(), 2);
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-create.md:23 "Mesh states | pending, set by the node-admin that launched the mesh's first admin, one ApplyMeshState(Pending) after the launched birth answers Ping; ready, set by the fabric-primary on its mesh-primary's report that every planned birth is ready (States)"
    /// docs/architecture/lifecycles/mesh-create.md:33-39 "2. The fabric-primary claims the attempt and executes node.create of mesh2's first node-admin itself: the mesh has no primary, so the fabric-primary is maker and executor. It records the deployment, prepares the data dir and seed, and launches the executable in the Create start mode (node-admin lifecycle §3). mesh.issuer.create: at the mesh's acceptance the fabric-primary mints mesh2's issuing cert, signed by the root, and writes it into the first admin's launch environment (RDM_ISSUING_KEY, RDM_ISSUING_CERT; fabric certs)."
    /// docs/architecture/lifecycles/mesh-create.md:40-41 "3. The node-admin binds port 0 and runs node.mesh.join: its report goes to its maker, the fabric-primary. The answer hands it the fabric identity, rafka-time, its cert, its name and config."
    /// docs/architecture/lifecycles/mesh-create.md:42-43 "4. The node-admin creates mesh2's channel, f(FabricId, MeshId), as its first listener, and joins the node-admin backbone."
    /// docs/architecture/lifecycles/mesh-create.md:44-46 "5. The fabric-primary sends one bounded Ping to the launched birth and, on the answer, applies the mesh's first state, ApplyMeshState(Pending), within 20 s of the launch (States, the mesh Pending row)."
    /// docs/architecture/lifecycles/mesh-create.md:47-51 "6. The rest of the node-admin's start runs: the pre_node_ready hook, the topology read on its maker, the Build floor, rafka-time last, node.started, then its on_node_ready notice. The fabric-primary, as writer of a mesh with no primary, sets the node-admin's Ready."
    /// docs/architecture/lifecycles/mesh-create.md:52-56 "7. The election names the node-admin mesh2's mesh-primary: the only candidate. As mesh-primary it claims the mesh's name, writes the mesh row and records is_primary on its own row, which its Digest carries, publishes its mesh's status pending as MeshStatus, and becomes the writer of mesh2's rows. node.created ends the step."
    /// docs/architecture/lifecycles/mesh-create.md:57-58 "8. The mesh-primary creates mesh2's other nodes as attempts of the same Build, at once: a create that fails stops none of the others (Q245)."
    /// docs/architecture/lifecycles/mesh-create.md:61-63 "Once that state is newly on the mesh's record the mesh-primary calls on_mesh_ready once, a notice that gates nothing (rdm.node_admin.mesh.update.via-on-mesh-ready)."
    /// docs/architecture/lifecycles/mesh-create.md:100-102 "A repeat of the same apply answers AlreadyApplied; an apply that names a mesh the receiver does not hold answers RejectedStaleMesh { held }; a state the mesh cannot move to answers RejectedInvalidMeshTransition { current }."
    /// docs/architecture/lifecycles/mesh-create.md:187-190 "Observability contract: Create (node.create's spans as maker and executor), Join (node.mesh.join), Mesh Pending (the Ping, then ApplyMeshState(Pending)'s call span), Seat and name (rdm.mesh.election.resolve.via-recompute; the name claim; the mesh row write)"
    #[test]
    fn mesh_create_steps_2_to_7_issuer_join_channel_ping_pending_ready_election_and_spans() {
        let capture = crate::span_capture::Capture::new(
            "mesh_create_steps_2_to_7_issuer_join_channel_ping_pending_ready_election_and_spans",
        );
        let dispatch = capture.dispatch.clone();
        let d = dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _guard = tracing::dispatcher::set_default(&dispatch);

        rt.block_on(async {
            let fabric_id = FabricId::mint();
            let mesh1_id = MeshId::mint();
            let mesh2_id = MeshId::mint();
            let now_ms = 1_718_290_010_000u64;
            let time = RafkaTime::unadopted();
            time.adopt(now_ms);

            // Step 2 (lines 33-39): `mesh.issuer.create` mints mesh2's issuing cert under the fabric root
            // and writes `RDM_ISSUING_KEY` and `RDM_ISSUING_CERT` into the first admin's Create launch env.
            let root_key = rdm_core::certs::mint_key();
            let ca_key = CaKey::mint(&root_key, rdm_core::certs::CA_CERT_TTL_SECS, now_ms);
            let issuer = CertIssuer::with_root(root_key.clone(), ca_key, None, time.clone());
            let (issuing_opt, minted_opt) = issuer
                .issuing_material_minting("mesh2")
                .expect("fabric-primary mints mesh2's issuing material");
            let issuing = issuing_opt.expect("issuing material present");
            let minted = minted_opt.expect("minted on first request for mesh2");
            assert_eq!(minted.mesh, "mesh2");

            let launch = Launch {
                fabric: "fabric1".into(),
                fabric_id: fabric_id.clone(),
                name: "mesh2.admin.1".parse().unwrap(),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                listeners: Vec::new(),
                launcher: None,
                seeds: vec![("fp-key".into(), "127.0.0.1:9000".parse().unwrap())],
                data_dir: "/tmp/m2-a1".into(),
                store_dir: "/tmp/store".into(),
                topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
                mesh_id: Some(mesh2_id.clone()),
                issuing: Some(issuing),
                roots: Vec::new(),
                connections_delete_deadline: Duration::from_secs(5),
                declared: Vec::new(),
            };
            let env = launch.to_env();
            assert!(env.contains_key(ENV_ISSUING_KEY), "RDM_ISSUING_KEY in launch env");
            assert!(env.contains_key(ENV_ISSUING_CERT), "RDM_ISSUING_CERT in launch env");

            // Step 5 (lines 44-46, 100-102, 189): fabric-primary sends bounded Ping to launched birth
            // `mesh2.admin.1` and applies `ApplyMeshState(Pending)`.
            let fp_key = SecretKey::generate();
            let m2_key = SecretKey::generate();
            let fp_ep = rdm_transport::endpoint::bind(fp_key.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let m2_ep = rdm_transport::endpoint::bind(m2_key.clone(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let m2_addr = m2_ep
                .bound_sockets()
                .into_iter()
                .find(|a| a.is_ipv4())
                .unwrap();

            let mut fp_node = ready_node("mesh1.admin.1", true, true);
            fp_node.endpoint_id = Some(EndpointId(fp_key.public().to_string()));
            let mut m2_node = ready_node("mesh2.admin.1", true, false);
            m2_node.mesh = "mesh2".into();
            m2_node.endpoint_id = Some(EndpointId(m2_key.public().to_string()));
            m2_node.transport_addr = Some(m2_addr);

            let top = Arc::new(tokio::sync::RwLock::new(Topology {
                fabric: Fabric {
                    id: fabric_id.clone(),
                    name: "fabric1".into(),
                    status: ScopeStatus::Pending,
                    provider: ProviderKind::Process,
                },
                meshes: vec![
                    Mesh {
                        id: Some(mesh1_id.clone()),
                        name: "mesh1".into(),
                        status: ScopeStatus::Ready,
                    },
                    Mesh {
                        id: Some(mesh2_id.clone()),
                        name: "mesh2".into(),
                        status: ScopeStatus::Pending,
                    },
                ],
                nodes: vec![fp_node.clone(), m2_node.clone()],
            }));

            let m2_mesh_ids: BTreeMap<String, MeshId> =
                [("mesh2".to_string(), mesh2_id.clone())].into_iter().collect();
            let m2_status = Arc::new(StatusDoor {
                certs: None,
                app_channels: None,
                declaration_seam: None,
                command_frame_seam: None,
                operation_effects: Default::default(),
                stopped: Default::default(),
                me: m2_node.name.clone(),
                node_id: m2_node.node_id.clone(),
                fabric_id: fabric_id.clone(),
                topology: top.clone(),
                declared: Arc::new(Mutex::new(Declared::default())),
                meshes_storage: Arc::new(MemoryMeshStorage::default()),
                fabric_storage: Arc::new(crate::fabric_storage::MemoryFabricStorage::default()),
                nodes_storage: Arc::new(MemoryNodesStorage::default()),
                caches: Default::default(),
                mesh_ids: Arc::new(move || m2_mesh_ids.clone()),
                durable: Default::default(),
                commands: Arc::new(crate::node_commands::CommandView::default()),
                own: Arc::new(OnceLock::new()),
                mesh_deleter: Arc::new(OnceLock::new()),
                hold_next_reply: Arc::new( std::sync::atomic::AtomicBool::new(false)),
                wake: Default::default(),
                fabric_states: Default::default(),
                atlas: Default::default(),
                fabric_recovery: Default::default(),
                configs: Default::default(),
                republish: Default::default(),
                configure: Default::default(),
                pre_node_ready: Default::default(),
                rename: Default::default(),
                row_moved: Default::default(),
                loads: None,
                digest_view: None,
                builds: Arc::new(MemoryBuildStateAdapter::new()),
            });
            let slot: Arc<OnceLock<Arc<StatusDoor>>> = Arc::new(OnceLock::new());
            let _ = slot.set(m2_status.clone());
            let core = ServerBuilder::new().serve::<Ping, _, _>(
                OpOwner::Core,
                |_peer, req: PingRequest| async move {
                    let PingRequest::Ping { payload } = req;
                    Ok(PingReply::Pong { payload })
                },
            );
            let server = crate::status_rpc::serve(core, slot)
                .build(ServedBirth {
                    node_id: m2_node.node_id.to_string(),
                    incarnation: m2_node.incarnation_id.clone().unwrap().to_string(),
                })
                .unwrap();
            let router = Router::builder(m2_ep.clone())
                .accept(rdm_transport::ALPN, server)
                .spawn();

            let resolver = Arc::new(LiveNodeResolver::new(Duration::from_secs(60)));
            let client =
                NodeRpcClient::new(fp_ep.clone(), resolver.clone()).with_caller_system("rdm");

            crate::admin::runner::apply_mesh_state(
                &client,
                &resolver,
                &fp_node.name,
                &fp_node.node_id,
                &top,
                "mesh2",
                mesh2_id.as_str(),
                MeshState::Pending,
                now_ms,
                &m2_node,
            )
            .await
            .expect("Ping + ApplyMeshState(Pending) succeeds");

            assert_eq!(
                m2_status.declared.lock().unwrap().meshes.get(&mesh2_id),
                Some(&MeshState::Pending),
                "mesh2.admin.1 holds MeshState::Pending after ApplyMeshState(Pending)"
            );

            // Repeat, StaleMesh, and InvalidMeshTransition replies (lines 100-102).
            let call_status = |req: StatusRequest| {
                let client = client.clone();
                let target = NodeTarget::ExactNode(m2_node.node_id.clone());
                async move {
                    match client.call::<Status>(&target, &req, &CallOptions::default()).await.0 {
                        RpcOutcome::Reply(r) => r.value().clone(),
                        other => panic!("expected StatusReply, got {other:?}"),
                    }
                }
            };
            assert_eq!(
                call_status(StatusRequest::ApplyMeshState {
                    mesh_id: mesh2_id.clone(),
                    mesh_name: "mesh2".into(),
                    state: MeshState::Pending,
                    changed_at_rafka_ms: now_ms,
                })
                .await,
                StatusReply::AlreadyApplied
            );
            assert_eq!(
                call_status(StatusRequest::ApplyMeshState {
                    mesh_id: MeshId::mint(),
                    mesh_name: "mesh2".into(),
                    state: MeshState::Pending,
                    changed_at_rafka_ms: now_ms,
                })
                .await,
                StatusReply::RejectedStaleMesh {
                    held: mesh2_id.clone()
                }
            );
            assert_eq!(
                call_status(StatusRequest::ApplyMeshState {
                    mesh_id: mesh2_id.clone(),
                    mesh_name: "mesh2".into(),
                    state: MeshState::Ready,
                    changed_at_rafka_ms: now_ms,
                })
                .await,
                StatusReply::Applied
            );
            assert_eq!(
                call_status(StatusRequest::ApplyMeshState {
                    mesh_id: mesh2_id.clone(),
                    mesh_name: "mesh2".into(),
                    state: MeshState::Pending,
                    changed_at_rafka_ms: now_ms,
                })
                .await,
                StatusReply::RejectedInvalidMeshTransition {
                    current: MeshState::Ready
                }
            );

            // Step 7 (lines 52-56, 190): election names mesh2.admin.1 mesh2's mesh-primary
            // (`rdm.mesh.election.resolve.via-recompute`) and it claims mesh2's name / writes the mesh row
            // (`rdm.node_admin.mesh.create.via-mesh-row`).
            let mut m2_candidates = vec![{
                let mut n = m2_node.clone();
                n.is_primary = false;
                n
            }];
            crate::election::resolve(&mut m2_candidates);
            assert!(
                m2_candidates[0].is_primary,
                "election names mesh2.admin.1 as mesh2's mesh-primary"
            );
            crate::election::ElectionLog::new(m2_node.name.clone()).observe(&*top.read().await);
            let meshes_store = MemoryMeshStorage::default();
            let records = Records::default();
            let claim_res = crate::admin::claim_mesh_name(
                &meshes_store,
                &records.caches,
                "mesh2.admin.1",
                "mesh2",
                &mesh2_id,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(claim_res, crate::storage::MeshClaim::Written { over: None });

            let _ = router.shutdown().await;
            fp_ep.close().await;
            m2_ep.close().await;
        });
        drop(rt);

        let cert_spans = capture.named("rdm.node_admin.cert.create.via-mesh-birth");
        assert!(
            cert_spans
                .iter()
                .any(|s| s.attributes.get("mesh").map(String::as_str) == Some("mesh2")),
            "mesh.issuer.create emits rdm.node_admin.cert.create.via-mesh-birth for mesh2: {cert_spans:?}"
        );

        let reach_spans = capture.named("rdm.node_admin.mesh.update.via-pending-reachability");
        assert!(
            reach_spans.iter().any(|s| {
                s.attributes.get("mesh").map(String::as_str) == Some("mesh2")
                    && s.attributes.get("target").map(String::as_str) == Some("mesh2.admin.1")
                    && s.attributes.get("reachable").map(String::as_str) == Some("true")
            }),
            "Ping reachability emits rdm.node_admin.mesh.update.via-pending-reachability: {reach_spans:?}"
        );

        let handoff_spans = capture.named("rdm.node_admin.mesh.update.via-pending-handoff");
        assert!(
            handoff_spans.iter().any(|s| {
                s.attributes.get("mesh").map(String::as_str) == Some("mesh2")
                    && s.attributes.get("state").map(String::as_str) == Some("Pending")
                    && s.attributes.get("outcome").map(String::as_str) == Some("applied")
            }),
            "ApplyMeshState(Pending) emits rdm.node_admin.mesh.update.via-pending-handoff: {handoff_spans:?}"
        );

        let election_spans = capture.named("rdm.mesh.election.resolve.via-recompute");
        assert!(
            election_spans.iter().any(|s| {
                s.attributes.get("mesh").map(String::as_str) == Some("mesh2")
                    && s.attributes.get("primary").map(String::as_str) == Some("mesh2.admin.1")
            }),
            "election emits rdm.mesh.election.resolve.via-recompute for mesh2: {election_spans:?}"
        );

        let row_spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        assert!(
            row_spans.iter().any(|s| {
                s.attributes.get("mesh").map(String::as_str) == Some("mesh2")
                    && s.attributes.get("node").map(String::as_str) == Some("mesh2.admin.1")
                    && s.attributes.get("outcome").map(String::as_str) == Some("row-written")
            }),
            "seat and name claim emits rdm.node_admin.mesh.create.via-mesh-row: {row_spans:?}"
        );
    }

}
