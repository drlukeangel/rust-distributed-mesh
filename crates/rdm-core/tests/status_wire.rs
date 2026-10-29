//! Fixed bytes for the coordinated 2026-10-07 status wire change (#2923); the four connections delete/stop command
//! variants (6..=9) are appended after it, their bytes derived from the postcard rules by hand, as are
//! `node.connection.delete`'s request (21) and its two replies (31, 32), the reads' four
//! requests (22..=25) and six replies (33..=38) with the views they carry, the mesh.delete workflow
//! and manifest refusals (39, 40, 41), the tag filter `node.get` (22) selects by, the tags and the
//! config its answer (33) carries, the tag refusal (43), the tags commands `node.tag.create` (26)
//! and `node.tag.delete` (27) with their replies (45, 46), and `node.name.update` (20) with its
//! completion (26), its refusals (27, 47) and the apply's answer naming held connections (48).
//! These literals lock discriminants and positional fields independently of codec round trips.
//! Never regenerate expectations from the Rust serializer to make a schema change pass.

use rdm_core::{ConnectionId, FabricId, IncarnationId, MeshId, NodeId, RuntimeFact, RuntimeLocator, RuntimeProvider};
use rdm_core::outcome::MalformedKind;
use rdm_core::protocol::NodeProtocol;
use rdm_core::status::{ConnectionScope, ConnectionsReceipt, DeclaredOperation, MAX_RECEIPT_MANIFEST_BYTES, FabricEvent, MeshDeleteIdentity, MeshState, NodeSelect, NodeState, NotPrimary, RetryOn, Status, StatusReply, StatusRequest, TagFilter, TagScope, WhichMesh};
use rdm_core::views::{ConnectionView, ConnectionsView, FabricView, MeshView, NodeStatus, NodeView, ProviderKind, ScopeStatus};

fn node() -> NodeId { NodeId::parse("0123456789ab").unwrap() }
fn mesh() -> MeshId { MeshId::parse("1123456789ab").unwrap() }
fn fabric() -> FabricId { FabricId::parse("2123456789ab").unwrap() }
fn birth() -> IncarnationId { IncarnationId::parse(&rdm_core::fixture_crockford60("birth")).unwrap() }
fn connection() -> ConnectionId { ConnectionId::parse("3123456789ab").unwrap() }

fn identity(build_id: &str, attempt: u32) -> MeshDeleteIdentity { MeshDeleteIdentity { build_id: build_id.into(), attempt, operation: "o".into() } }

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| {
        u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()
    }).collect()
}

#[test]
fn requests_match_the_frozen_thirty_seven_variant_wire_schema() {
    use StatusRequest::*;
    let fixtures = [
        (DeclareNodeState { node_id: node(), incarnation: birth(), state: NodeState::Stopped, operation: None }, "000c3031323334353637383961620c3977637072666777767468790300"),
        (DeclareMeshState { mesh_id: mesh(), state: MeshState::Leaving, changed_at_rafka_ms: 300 }, "010c31313233343536373839616202ac02"),
        (ApplyNodeState { node_id: node(), incarnation: birth(), state: NodeState::ConnectionsDeleting }, "020c3031323334353637383961620c39776370726667777674687902"),
        (ProbeNodeState { node_id: node(), incarnation: birth() }, "030c3031323334353637383961620c397763707266677776746879"),
        (ApplyMeshState { mesh_id: mesh(), mesh_name: "mesh1".into(), state: MeshState::Pending, changed_at_rafka_ms: 300 }, "040c313132333435363738396162056d6573683100ac02"),
        (ApplyFabricEvent { fabric_id: fabric(), event: FabricEvent::ShutdownInitiated { initiated_by: "mesh1.admin.1".into() } }, "050c323132333435363738396162010d6d657368312e61646d696e2e31"),
        (Reserved6 { node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "drain-node:mesh1.rpc.1".into() }, "060c3031323334353637383961620c3977637072666777767468790c796e6835673262717a6634720116647261696e2d6e6f64653a6d657368312e7270632e31"),
        (Reserved7 { node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "drain-node:mesh1.rpc.1".into() }, "070c3031323334353637383961620c3977637072666777767468790c796e6835673262717a6634720116647261696e2d6e6f64653a6d657368312e7270632e31"),
        (StopNode { node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() }, "080c3031323334353637383961620c3977637072666777767468790c796e6835673262717a663472011573746f702d6e6f64653a6d657368312e7270632e31"),
        (NodeLeft { node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() }, "090c3031323334353637383961620c3977637072666777767468790c796e6835673262717a663472011573746f702d6e6f64653a6d657368312e7270632e31"),
        (DeleteMesh { mesh_id: mesh(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "shutdown-mesh:1123456789ab".into() }, "0a0c3131323334353637383961620c796e6835673262717a663472011a73687574646f776e2d6d6573683a313132333435363738396162"),
        (
            MeshDeleted {
                mesh_id: mesh(),
                build_id: "ynh5g2bqzf4r".into(),
                attempt: 1,
                operation: "shutdown-mesh:1123456789ab".into(),
                final_node_id: node(),
                final_incarnation: birth(),
                final_runtime: RuntimeFact { deployment_id: rdm_core::fixture_crockford60("dep_mesh1_admin1"), provider: RuntimeProvider::Process, control_domain: "process:boot-a:pidns-a".into(), locator: RuntimeLocator::Process { pid: 4321, start: 123456 } },
                receipt_manifest: "ynh5g2bqzf4r/1/shutdown-mesh:1123456789ab/other-members-exited".into(),
            },
            "0b0c3131323334353637383961620c796e6835673262717a663472011a73687574646f776e2d6d6573683a3131323334353637383961620c3031323334353637383961620c3977637072666777767468790c39726d797a34306d67776563001670726f636573733a626f6f742d613a7069646e732d6100e121c0c4073e796e6835673262717a6634722f312f73687574646f776e2d6d6573683a3131323334353637383961622f6f746865722d6d656d626572732d657869746564",
        ),
        (Reserved12 { fabric_id: fabric(), node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "commit-state:fabric1".into() }, "0c0c3231323334353637383961620c3031323334353637383961620c3977637072666777767468790c796e6835673262717a6634720114636f6d6d69742d73746174653a66616272696331"),
        (Reserved13 { fabric_id: fabric(), node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "commit-state:fabric1".into() }, "0d0c3231323334353637383961620c3031323334353637383961620c3977637072666777767468790c796e6835673262717a6634720114636f6d6d69742d73746174653a66616272696331"),
        (Reserved14 { fabric_id: fabric(), node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "open-traffic:fabric1".into() }, "0e0c3231323334353637383961620c3031323334353637383961620c3977637072666777767468790c796e6835673262717a66347201146f70656e2d747261666669633a66616272696331"),
        (Reserved15 { fabric_id: fabric(), node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "open-traffic:fabric1".into() }, "0f0c3231323334353637383961620c3031323334353637383961620c3977637072666777767468790c796e6835673262717a66347201146f70656e2d747261666669633a66616272696331"),
        (StartNode { node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "start-node:mesh1.rpc.1".into() }, "100c3031323334353637383961620c3977637072666777767468790c796e6835673262717a663472011673746172742d6e6f64653a6d657368312e7270632e31"),
        (UpdateNodeConfig { node_id: node(), incarnation: birth(), config: [("k".to_string(), "v".to_string())].into() }, "110c3031323334353637383961620c39776370726667777674687901016b0176"),
        // node_id | incarnation | config (len 1: "k" "v") | revision 3 (P1)
        (ApplyNodeConfig { node_id: node(), incarnation: birth(), config: [("k".to_string(), "v".to_string())].into(), revision: 3 }, "120c3031323334353637383961620c39776370726667777674687901016b017603"),
        (ApplyNodeName { node_id: node(), incarnation: birth(), name: "mesh1.rpc.1".into() }, "130c3031323334353637383961620c3977637072666777767468790b6d657368312e7270632e31"),
        // node_id | incarnation | name "1.old" (the name only, never its path)
        (UpdateName { node_id: node(), incarnation: birth(), name: "1.old".into() }, "140c3031323334353637383961620c39776370726667777674687905312e6f6c64"),
        (DeleteConnections { node_id: node(), incarnation: birth(), scope: ConnectionScope::Connections(vec![connection()]) }, "150c3031323334353637383961620c39776370726667777674687901010c333132333435363738396162"),
        (GetNodes { select: NodeSelect::Mesh("mesh1".into()) }, "1601056d65736831"),
        (GetMeshes { mesh: Some("mesh1".into()) }, "1701056d65736831"),
        (GetFabric, "18"),
        (GetConnections { node_id: node(), incarnation: birth() }, "190c3031323334353637383961620c397763707266677776746879"),
        // node_id | incarnation | labels (len 1: "red")
        (CreateTags { node_id: node(), incarnation: birth(), labels: ["red".to_string()].into() }, "1a0c3031323334353637383961620c3977637072666777767468790103726564"),
        // node_id | incarnation | scope Labels (1) (len 1: "red")
        (DeleteTags { node_id: node(), incarnation: birth(), scope: TagScope::Labels(["red".to_string()].into()) }, "1b0c3031323334353637383961620c397763707266677776746879010103726564"),
        // node_id | incarnation | build_id | attempt 1 | operation "join-mesh:mesh1.rpc.1"
        (JoinMesh { node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "join-mesh:mesh1.rpc.1".into() }, "1c0c3031323334353637383961620c3977637072666777767468790c796e6835673262717a66347201156a6f696e2d6d6573683a6d657368312e7270632e31"),
        // node_id | incarnation | build_id | attempt 1 | operation "leave-mesh:mesh1.rpc.1"
        (LeaveMesh { node_id: node(), incarnation: birth(), build_id: "ynh5g2bqzf4r".into(), attempt: 1, operation: "leave-mesh:mesh1.rpc.1".into() }, "1d0c3031323334353637383961620c3977637072666777767468790c796e6835673262717a66347201166c656176652d6d6573683a6d657368312e7270632e31"),
        // node_id | incarnation | cert (len 3) (i145 e3, node.cert.renew)
        (RenewCert { node_id: node(), incarnation: birth(), cert: vec![1, 2, 3] }, "1e0c3031323334353637383961620c39776370726667777674687903010203"),
        // node_id | incarnation | key "ab" | cert "cd" (node.issuer.update)
        (UpdateIssuer { node_id: node(), incarnation: birth(), key: rdm_core::status::IssuingKeyHex("ab".into()), cert: "cd".into(), ca_key: rdm_core::status::CaKeyHex("ef".into()) }, "1f0c3031323334353637383961620c397763707266677776746879026162026364026566"),
        // fabrics.atlas.get (fabric-atlas.md R-FA4), appended last
        (GetAtlas, "20"),
        // The reads (Q167), appended last: entity Nodes (2) | node None | pairs (len 1: "mesh", values (len 1: "mesh1"))
        (Get { entity: rdm_core::filter::Entity::Nodes, node: None, filter: rdm_core::filter::Filter::new().eq("mesh", ["mesh1"]) }, "21020001046d65736801056d65736831"),
        // A node's own config pushed up (Q235), appended last: node_id | config (len 1: "k" "v") | revision 3
        (NodeConfigUpdated { node_id: node(), config: [("k".to_string(), "v".to_string())].into(), revision: 3 }, "220c30313233343536373839616201016b017603"),
        // node.roots.update, appended last: node_id | incarnation | roots (len 1: "ab") | phase Distributed (1) | secret Some("cd")
        (UpdateRoots { node_id: node(), incarnation: birth(), roots: vec!["ab".into()], phase: rdm_core::status::RootsPhase::Distributed, secret: Some(rdm_core::status::RootSecretHex("cd".into())) }, "230c3031323334353637383961620c397763707266677776746879010261620101026364"),
        // fabric.root.update, appended last: secret "ab"
        (UpdateRoot { secret: rdm_core::status::RootSecretHex("ab".into()) }, "24026162"),
    ];
    assert_eq!(fixtures.len() as u32, Status::REQUEST_VARIANTS);
    for (request, hex) in fixtures {
        let expected = bytes(hex);
        assert_eq!(Status::encode_request(&request).unwrap(), expected, "{request:?}");
        assert_eq!(Status::decode_request(&expected).unwrap(), request);
    }
}

#[test]
fn replies_match_the_frozen_sixty_three_variant_wire_schema() {
    let mut variants = std::collections::BTreeSet::new();
    use StatusReply::*;
    let fixtures = [
        (Applied, "00"),
        (AlreadyApplied, "01"),
        (Reserved2 { in_flight: 300 }, "02ac02"),
        (Current { node_id: node(), incarnation: birth(), state: NodeState::Ready }, "030c3031323334353637383961620c39776370726667777674687901"),
        (RejectedStaleIncarnation { held: birth() }, "040c397763707266677776746879"),
        (RejectedStaleMesh { held: mesh() }, "050c313132333435363738396162"),
        (RejectedStaleFabric { held: fabric() }, "060c323132333435363738396162"),
        (RejectedNotPrimary { why: NotPrimary::SenderNotSubject { sender: "peer-path".into() } }, "070109706565722d70617468"),
        (RejectedInvalidNodeTransition { current: NodeState::Stopped }, "0803"),
        (RejectedInvalidMeshTransition { current: MeshState::Dead }, "0903"),
        (PeerUnresolved { reason: "p".into() }, "0a0170"),
        (NotReady { reason: "n".into() }, "0b016e"),
        (Busy { reason: "b".into() }, "0c0162"),
        (ConnectionsDeleting { reason: "d".into() }, "0d0164"),
        (Malformed { kind: MalformedKind::Corrupt }, "0e02"),
        (Unauthorized { reason: "u".into() }, "0f0175"),
        (RejectedUnmatchedCompletion { field: "attempt".into(), expected: "1".into(), reported: "2".into() }, "1007617474656d707401310132"),
        (Stopped { receipt: ConnectionsReceipt::Established { in_flight: 0, closed: vec![] } }, "11000000"),
        (Started, "12"),
        (StartFailed { step: "node.join".into(), reason: "refused".into() }, "13096e6f64652e6a6f696e0772656675736564"),
        (CurrentStopped { node_id: node(), incarnation: birth() }, "140c3031323334353637383961620c397763707266677776746879"),
        // node_id | incarnation | the row as written: config (len 1: "k" "v") | revision 3 (P1)
        (ConfigUpdated { node_id: node(), incarnation: birth(), config: [("k".to_string(), "v".to_string())].into(), revision: 3 }, "150c3031323334353637383961620c39776370726667777674687901016b017603"),
        (Reserved22 { keys: 33, bytes: 10, max_keys: 32, max_bytes: 2048 }, "16210a208010"),
        // node | reason | the row as it stands: config (len 0) | revision 0 (P2)
        (ConfigRowRefused { node: "n".into(), reason: "r".into(), config: Default::default(), revision: 0 }, "17016e01720000"),
        (ConfigNotApplied { node: "n".into(), answered: "a".into(), reason: "r".into() }, "18016e01610172"),
        (Blocked { node_id: node(), incarnation: birth(), reason: "r".into(), retry_on: RetryOn::TargetReachable { node_id: node() } }, "190c3031323334353637383961620c3977637072666777767468790172010c303132333435363738396162"),
        (NameUpdated { node_id: node(), incarnation: birth() }, "1a0c3031323334353637383961620c397763707266677776746879"),
        // status Ready (1) | held_connections (len 1)
        (RejectedAdmittingConnections { status: NodeStatus::Ready, held_connections: vec![connection()] }, "1b01010c333132333435363738396162"),
        (NameHeld { name: "n".into(), by: node() }, "1c016e0c303132333435363738396162"),
        (NodeRowRefused { node: "n".into(), reason: "r".into() }, "1d016e0172"),
        (NameNotApplied { node: "n".into(), answered: "a".into(), reason: "r".into() }, "1e016e01610172"),
        (ConnectionsDeleted { closed: vec![connection()], not_open: vec![], held: vec![], receipt: None }, "1f010c333132333435363738396162000000"),
        (RejectedConnectionScope { reason: "r".into() }, "200172"),
        (
            Nodes { nodes: vec![node_view()] },
            concat!(
                "2101",
                "0b6d657368312e7270632e31", "01", "056d65736831", "0c303132333435363738396162", "010165", "010c397763707266677776746879",
                "00", "0100", "00", "01", "00", "00", "00", "01007f000001a01f", "00", "00", "00", "0101020304", "0102010304", "0103726564", "01047469657204676f6c64", "00", "00",
                // types and held lengths, and the fixed 32-byte held hash.
                "00", "00", "0000000000000000000000000000000000000000000000000000000000000000",
                // cert None (i145 e3)
                "00",
                // executable None (the row's, proposal executable-bindings-at-create.md rule 7)
                "00",
                // storage_persistence None and topology_storage_provider None (storage.md §1, §2), appended last
                "00", "00",
            ),
        ),
        (Meshes { meshes: vec![mesh_view()] }, concat!("2201", "010c313132333435363738396162", "056d65736831", "01", "010d6d657368312e61646d696e2e31", "00", "010d6d657368312e61646d696e2e31", "00")),
        (
            // id | name | status | provider | fabric_primary | admin_api_base | meshes | build_id Some("b") | next_expiry None
            Fabric { fabric: FabricView { next_expiry: None, topology_storage_provider: None, roots: None, ca: None, previous_topology_storage_providers: Vec::new(), id: fabric(), name: "fabric1".into(), status: ScopeStatus::Pending, provider: ProviderKind::Process, fabric_primary: None, admin_api_base: None, meshes: vec![], build_id: Some("b".into()) } },
            // ... | topology_storage_provider None | previous_topology_storage_providers [] (storage.md §2)
            // | roots None | ca None (fabric-ca-renew.md)
            concat!("23", "0c323132333435363738396162", "0766616272696331", "00", "00", "00", "00", "00", "010162", "00", "00", "00", "00", "00"),
        ),
        (
            Connections {
                connections: ConnectionsView {
                    node: "n".into(),
                    complete: true,
                    connections: vec![ConnectionView { id: "i".into(), source: "s".into(), destination: "d".into(), kind: "direct".into(), state: "connected".into(), carrier: None, reason: Some("r".into()), logged_at_ms: 300 }],
                },
            },
            concat!("24", "016e", "01", "01", "0169", "0173", "0164", "06646972656374", "09636f6e6e6563746564", "00", "010172", "ac02"),
        ),
        (UnknownMesh { mesh: "m".into() }, "25016d"),
        (ReadOverBound { read: "r".into(), bytes: 300, max: 65536 }, "260172ac02808004"),
        // held { build_id "b" | attempt 2 | operation "o" } | offered { "b" | 1 | "o" }
        (RejectedStaleWorkflow { held: identity("b", 2), offered: identity("b", 1) }, "27016202016f016201016f"),
        // held { "b" | 1 | "o" } | offered { "c" | 1 | "o" }
        (RejectedConflictingWorkflow { held: identity("b", 1), offered: identity("c", 1) }, "28016201016f016301016f"),
        // missing 1 | without_exit (len 1: "n i") | reason "r"
        (RejectedIncompleteManifest { missing: 1, without_exit: vec!["n i".into()], reason: "r".into() }, "290101036e20690172"),
        // source "s" | op "o" | cause "c"
        (RejectedSourceFailed { source: "s".into(), op: "o".into(), cause: "c".into() }, "2a0173016f0163"),
        // reason "r"
        (RejectedTags { reason: "r".into() }, "2b0172"),
        // workflow "w" | status "s"
        (Reserved44 { workflow: "w".into(), status: "s".into() }, "2c01770173"),
        // node_id | incarnation | already_carried (len 1: "red")
        (TagsCreated { node_id: node(), incarnation: birth(), already_carried: ["red".to_string()].into() }, "2d0c3031323334353637383961620c3977637072666777767468790103726564"),
        // node_id | incarnation | not_carried (len 1: "blue")
        (TagsDeleted { node_id: node(), incarnation: birth(), not_carried: ["blue".to_string()].into() }, "2e0c3031323334353637383961620c3977637072666777767468790104626c7565"),
        // reason "r"
        (RejectedName { reason: "r".into() }, "2f0172"),
        // connections (len 1)
        (BusyHoldingConnections { connections: vec![connection()] }, "30010c333132333435363738396162"),
        // node_id | incarnation | joined (len 2: Own (0), Backbone (1))
        (MeshJoined { node_id: node(), incarnation: birth(), joined: vec![WhichMesh::Own, WhichMesh::Backbone] }, "310c3031323334353637383961620c397763707266677776746879020001"),
        // node_id | incarnation | left (len 1: Own (0))
        (MeshLeft { node_id: node(), incarnation: birth(), left: vec![WhichMesh::Own] }, "320c3031323334353637383961620c3977637072666777767468790100"),
        (RequestNotHandled { request: "declare-node-state".into() }, "33126465636c6172652d6e6f64652d7374617465"),
        // node_id | incarnation | expires_at 7 (i145 e3, node.cert.renewed)
        (CertRenewed { node_id: node(), incarnation: birth(), expires_at: 7 }, "340c3031323334353637383961620c39776370726667777674687907"),
        // node_id | incarnation | issuer_expires_at 7 (node.issuer.updated)
        (IssuerUpdated { node_id: node(), incarnation: birth(), issuer_expires_at: 7 }, "350c3031323334353637383961620c39776370726667777674687907"),
        // reason "x"
        (RejectedCert { reason: "x".into() }, "360178"),
        // nodes (len 0) (fabric.atlas.get, fabric-atlas.md R-FA4), appended last
        (Atlas { atlas: rdm_core::views::AtlasView { nodes: vec![] } }, "3700"),
        // fabrics (len 0) (fabric.get, Q167), appended last
        (Fabrics { fabrics: vec![] }, "3800"),
        // reason "x" (a filter refused naming the pair, Q167), appended last
        (RejectedFilter { reason: "x".into() }, "390178"),
        // node.roots.updated, appended last: node_id | incarnation | roots (len 1: "ab")
        (RootsUpdated { node_id: node(), incarnation: birth(), roots: vec!["ab".into()] }, "3a0c3031323334353637383961620c39776370726667777674687901026162"),
        // root-still-chained, appended last: root "ab"
        (RejectedRootStillChained { root: "ab".into() }, "3b026162"),
        // fabric.root.updated, appended last: phase Minted (0)
        (RootUpdated { phase: rdm_core::status::RootsPhase::Minted }, "3c00"),
        // rotation-in-progress, appended last: phase Distributed (1)
        (RejectedRotationInProgress { phase: rdm_core::status::RootsPhase::Distributed }, "3d01"),
        // root-already-trusted, appended last: root "ab"
        (RejectedRootAlreadyTrusted { root: "ab".into() }, "3e026162"),
    ];
    assert_eq!(fixtures.len() as u32, Status::REPLY_VARIANTS);
    for (reply, hex) in fixtures {
        let expected = bytes(hex);
        variants.insert(Status::encode_reply(&reply).unwrap()[0]);
        crate::outcome_codes::check::<Status>(&reply);
        assert_eq!(Status::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Status::decode_reply(&expected).unwrap(), reply);
    }
    assert_eq!(variants.len(), Status::REPLY_VARIANTS as usize);
    crate::outcome_codes::check_malformed::<Status>();
}

/// CONTRACT: a `ConnectionsDeleted` carries its `ConnectionsReceipt` in frozen bytes after the ids it
/// closed, did not find open and still holds: absent for a list of ids, present for `All`.
#[test]
fn a_connections_deleted_reply_carries_its_receipt_in_frozen_bytes() {
    use StatusReply::ConnectionsDeleted;
    for (reply, hex) in [
        (ConnectionsDeleted { closed: vec![connection()], not_open: vec![], held: vec![], receipt: None }, "1f010c333132333435363738396162000000"),
        (ConnectionsDeleted { closed: vec![connection()], not_open: vec![], held: vec![], receipt: Some(ConnectionsReceipt::Established { in_flight: 0, closed: vec![connection()] }) }, "1f010c3331323334353637383961620000010000010c333132333435363738396162"),
    ] {
        let expected = bytes(hex);
        assert_eq!(Status::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Status::decode_reply(&expected).unwrap(), reply);
    }
}

/// CONTRACT: the `ConnectionsReceipt` a `Stopped` carries is frozen: `Established` is variant 0 with
/// its count as a varint and the ids closed, `Deadline` variant 1 with the last count, the ids closed
/// and the ids still held.
#[test]
fn a_stopped_reply_carries_its_connections_receipt_in_frozen_bytes() {
    use StatusReply::Stopped;
    for (reply, hex) in [
        (Stopped { receipt: ConnectionsReceipt::Established { in_flight: 0, closed: vec![] } }, "11000000"),
        (Stopped { receipt: ConnectionsReceipt::Established { in_flight: 3, closed: vec![connection()] } }, "110003010c333132333435363738396162"),
        (Stopped { receipt: ConnectionsReceipt::Deadline { last_in_flight: 300, closed: vec![], held: vec![connection()] } }, "1101ac0200010c333132333435363738396162"),
    ] {
        let expected = bytes(hex);
        assert_eq!(Status::encode_reply(&reply).unwrap(), expected, "{reply:?}");
        assert_eq!(Status::decode_reply(&expected).unwrap(), reply);
    }
}

/// CONTRACT: the largest `MeshDeleted` (a container's 64-character id and a manifest reference at its
/// 128-byte bound) fits the Status request ceiling with room for every other field; a manifest
/// reference over its bound is the caller's to refuse before it sends.
#[test]
fn the_largest_mesh_deleted_fits_the_request_ceiling() {
    let request = StatusRequest::MeshDeleted {
        mesh_id: mesh(),
        build_id: "ynh5g2bqzf4r".into(),
        attempt: u32::MAX,
        operation: "shutdown-mesh:1123456789ab".into(),
        final_node_id: node(),
        final_incarnation: IncarnationId::parse(&"z".repeat(12)).unwrap(),
        final_runtime: RuntimeFact { deployment_id: "d".repeat(12), provider: RuntimeProvider::Container, control_domain: "c".repeat(128), locator: RuntimeLocator::Container { id: "a".repeat(64) } },
        receipt_manifest: "m".repeat(MAX_RECEIPT_MANIFEST_BYTES),
    };
    let bytes = Status::encode_request(&request).unwrap();
    assert!(bytes.len() + 512 <= Status::MAX_REQUEST_FRAME_BYTES, "{} bytes of {}", bytes.len(), Status::MAX_REQUEST_FRAME_BYTES);
    assert_eq!(Status::decode_request(&bytes).unwrap(), request);
}

/// One node as a `node.get` answer carries it: an rpc node with its birth, provider, address, load
/// and gossip stats, a tag and a config entry, the optional fields it lacks absent.
fn node_view() -> NodeView {
    NodeView { cert: None, executable: None, storage_persistence: None, topology_storage_provider: None, types: Default::default(), held: Default::default(), held_hash: Default::default(),
        name: "mesh1.rpc.1".parse().unwrap(),
        kind: rdm_core::NodeKind::RpcNode,
        mesh: "mesh1".into(),
        node_id: node(),
        endpoint_id: Some("e".into()),
        incarnation_id: Some(birth()),
        deployment_id: None,
        provider: Some(ProviderKind::Process),
        data_dir: None,
        status: NodeStatus::Ready,
        is_primary: false,
        is_fabric_primary: false,
        admin_api_base: None,
        transport_addr: Some("127.0.0.1:4000".parse().unwrap()),
        listeners: vec![],
        declared: None,
        stopped: false,
        load: Some(rdm_core::NodeLoad { cpu_used_millicores: 1, cpu_budget_millicores: 2, ram_used_bytes: 3, ram_budget_bytes: 4 }),
        gossip: Some(rdm_core::GossipStats { heard: 2, neighbours: 1, frames_sent: 3, frames_received: 4 }),
        tags: ["red".to_string()].into(),
        config: [("tier".to_string(), "gold".to_string())].into(),
        data_dir_lost: None,
        data_dir_unreadable: None,
    }
}

/// One mesh as a `mesh.get` answer carries it.
fn mesh_view() -> MeshView {
    MeshView { renewals: None, id: Some(mesh()), name: "mesh1".into(), status: ScopeStatus::Ready, primary_admin: Some("mesh1.admin.1".parse().unwrap()), admin_api_base: None, nodes: vec!["mesh1.admin.1".parse().unwrap()] }
}

fn golden<T: serde::Serialize>(value: T, expected: &str) {
    assert_eq!(postcard::to_allocvec(&value).unwrap(), bytes(expected));
}

#[test]
fn nested_enum_discriminants_and_fields_are_frozen_too() {
    for (state, expected) in [
        (NodeState::Pending, "00"), (NodeState::Ready, "01"),
        (NodeState::ConnectionsDeleting, "02"), (NodeState::Stopped, "03"),
    ] { golden(state, expected); }
    for (state, expected) in [
        (MeshState::Pending, "00"), (MeshState::Ready, "01"),
        (MeshState::Leaving, "02"), (MeshState::Dead, "03"),
    ] { golden(state, expected); }
    // node.roots.update's phase (fabric-ca-renew.md "fabric.root.update").
    for (phase, expected) in [
        (rdm_core::status::RootsPhase::Minted, "00"), (rdm_core::status::RootsPhase::Distributed, "01"),
        (rdm_core::status::RootsPhase::CaRotated, "02"), (rdm_core::status::RootsPhase::Retired, "03"),
    ] { golden(phase, expected); }
    // DeclareNodeState's operation (i146 Q36): build_id | attempt | operation, and its Option.
    let op = DeclaredOperation { build_id: "b".into(), attempt: 2, operation: "node.stop:m".into() };
    golden(op.clone(), "0162020b6e6f64652e73746f703a6d");
    golden(Some(op), "010162020b6e6f64652e73746f703a6d");
    golden(FabricEvent::Ready, "00");
    golden(FabricEvent::ShutdownInitiated { initiated_by: "mesh1.admin.1".into() }, "010d6d657368312e61646d696e2e31");
    golden(NotPrimary::ReceiverNotPrimary { needed: "mesh-primary".into() }, "000c6d6573682d7072696d617279");
    golden(NotPrimary::SenderNotSubject { sender: "peer-path".into() }, "0109706565722d70617468");
    golden(NotPrimary::SubjectUnknown, "02");
    golden(MalformedKind::TooLarge, "00");
    golden(MalformedKind::UnknownVariant, "01");
    golden(MalformedKind::Corrupt, "02");
    golden(MalformedKind::TypesOverCap, "03");
    golden(RetryOn::PrimaryChanged, "00");
    golden(RetryOn::TargetReachable { node_id: node() }, "010c303132333435363738396162");
    golden(RetryOn::PrimaryReady, "02");
    golden(RetryOn::PrimaryAddressed, "03");
    golden(ConnectionScope::All, "00");
    golden(ConnectionScope::Connections(vec![connection()]), "01010c333132333435363738396162");
    golden(NodeSelect::Id(node()), "000c303132333435363738396162");
    golden(NodeSelect::Mesh("m".into()), "01016d");
    // fabric.nodes.get: the selector over every mesh, appended last
    golden(NodeSelect::Fabric, "03");
    // Tags | Any | len 1 | "red"
    golden(NodeSelect::Tags(TagFilter::Any(["red".to_string()].into())), "02010103726564");
    // mesh.nodes.get { tags } (Q165), appended last: MeshTags | "m" | Any | len 1 | "red"
    golden(NodeSelect::MeshTags("m".into(), TagFilter::Any(["red".to_string()].into())), "04016d010103726564");
    golden(TagScope::All, "00");
    // Labels | len 1 | "red"
    golden(TagScope::Labels(["red".to_string()].into()), "010103726564");
    // All | len 2 | "a" | "b"
    golden(TagFilter::All(["a".to_string(), "b".to_string()].into()), "000201610162");
    // The read's entity (Q167): Fabrics | Meshes | Nodes | Connections
    golden(rdm_core::filter::Entity::Fabrics, "00");
    golden(rdm_core::filter::Entity::Meshes, "01");
    golden(rdm_core::filter::Entity::Nodes, "02");
    golden(rdm_core::filter::Entity::Connections, "03");
    // node.connection.get { node } with no pair: Connections | Some(node, birth) | pairs (len 0)
    golden(StatusRequest::Get { entity: rdm_core::filter::Entity::Connections, node: Some((node(), birth())), filter: rdm_core::filter::Filter::new() }, "2103010c3031323334353637383961620c39776370726667777674687900");
    for (status, expected) in [
        (NodeStatus::Pending, "00"), (NodeStatus::Ready, "01"), (NodeStatus::ConnectionsDeleting, "02"), (NodeStatus::Leaving, "03"),
        (NodeStatus::PendingReconnect, "04"), (NodeStatus::Reserved5, "05"), (NodeStatus::Dead, "06"), (NodeStatus::Deleted, "07"),
        // The row statuses states.md names beside them, appended after Deleted: no position before them moves.
        (NodeStatus::Starting, "08"), (NodeStatus::ConnectionsDeleted, "09"), (NodeStatus::Stopping, "0a"), (NodeStatus::Stopped, "0b"),
        (NodeStatus::Deleting, "0c"),
    ] { golden(status, expected); }
    for (status, expected) in [
        (ScopeStatus::Pending, "00"), (ScopeStatus::Ready, "01"), (ScopeStatus::Leaving, "02"), (ScopeStatus::Dead, "03"),
        (ScopeStatus::Degraded, "04"),
    ] { golden(status, expected); }
    golden(ProviderKind::Process, "00");
    golden(ProviderKind::Container, "01");
    // build_id "b" | attempt 300 (ac 02) | operation "o"
    golden(MeshDeleteIdentity { build_id: "b".into(), attempt: 300, operation: "o".into() }, "0162ac02016f");
}

#[test]
fn every_reply_variant_of_status_classifies_to_one_code() {
    replies_match_the_frozen_sixty_three_variant_wire_schema();
}
