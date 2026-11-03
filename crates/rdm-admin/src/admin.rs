//! The node-admin runtime (`rdm-node-admin`; `docs/architecture/node-admin-lifecycle.md`).
//!
//! A node-admin is a fabric node that serves the control API. The first
//! one bootstraps the fabric (its `MESH_SPAWN_TYPE` becomes fabric policy);
//! every other one is launched by an admin's deployment pipeline and takes
//! the policy, with its first view, from the entry pull it makes to that
//! admin over QUIC (`rdm_transport::entry`). No node calls an admin's
//! HTTP API: that is for tests and people.
//!
//! Every admin holds the same two projections over iroh-gossip: the
//! topology view (each node's digest) and the fabric's Build facts. From
//! the topology view it derives the observed topology it publishes on its views,
//! with every primary role resolved by `election` (the lowest ready `NodeId` of each
//! `(mesh, kind)` group; the lowest-`NodeId` `mesh-primary` is the `fabric-primary`,
//! `docs/architecture/fabric-node-lifecycle-elections.md`). Admins execute Builds (`executor`): the
//! `fabric-primary` and each mesh's `mesh-primary` claim the attempts whose next operation is
//! theirs and run what is left through the deployment pipeline (create,
//! delete) and the lifecycle pipeline (a node's `Pending -> Ready`).

use crate::build::BuildOperation;
use crate::build_state::BuildStateAdapter;
use crate::accepted::AcceptedStore;
use crate::deployment::pipeline::{adoption_missing, CurrentRuntimeAdoption, Publication, 
    CreateRequest, DeploymentPipeline, LaunchTemplate, NodeObserver, DeleteRequest, Timeouts, TopologySink,
};
use crate::deployment::provider::{DeploymentHandle, DeploymentProvider, FabricPolicy};
use crate::executor::{BuildExecutor, OperationRunner};
use crate::election::ElectionLog;
use crate::fabric_builds::FabricBuildStateAdapter;
use crate::control_plane::ControlPlane;
use crate::lifecycle::{
    HookRegistry, LifecycleScope, LifecycleState, LifecycleTransitionPipeline, MemoryReceiptLog, ShapeFacts, Transition,
    TransitionKey, TransitionResult,
};
use crate::model::*;
use crate::topology::Topology;
use iroh::protocol::Router as IrohRouter;
use iroh::{Endpoint, EndpointAddr, SecretKey};
use rdm_core::launch::Launch;
use rdm_core::{MemberStatus, MeshDigest, MeshNode};
use rdm_transport::topology::{gossip_interval, Backbone, DigestView, TopologyView};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use tracing::Instrument;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

pub(crate) mod adoption;
mod boot;
mod mesh_delete;
mod projection;
mod reconcile;
mod records;
mod recovery;
mod rows_writer;
pub(crate) mod runner;
mod session;

pub use boot::*;
pub use mesh_delete::MeshDeleteDoor;
pub use projection::*;
pub use reconcile::*;
pub use records::*;
pub use recovery::*;
pub use rows_writer::*;
pub use runner::*;
pub use session::*;

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (node-recovery.md "Failure and receipts"): a recovery Build that ends `Failed { step,
    /// reason }` is recorded as that failure on the submit span's outcome, and the submit ends in an
    /// error so the mesh-primary's next tick finds the member still proven exited and submits again.
    /// A Complete terminal is recorded as complete, and a stream that ends with no terminal is named.
    /// CONTRACT: the mesh primary's per-birth bookkeeping (the TRUE OFFLINE set, the silence-status
    /// memo) holds only births its view still watches at that exact incarnation. A birth deleted by
    /// its recovery (a new node.id at its path and name) and a birth superseded by a new incarnation of its
    /// node are deleted; a watched TRUE OFFLINE birth keeps both. Must NOT happen: entries of gone births accumulating, one per recovered birth.
    #[test]
    fn the_mesh_primarys_offline_bookkeeping_drops_deleted_and_superseded_births() {
        let watched_node = |path: PathName, mesh: &str, r#type: &str| {
            let mut n = Node::allocated(path.clone(), mesh, r#type);
            n.incarnation_id = Some(IncarnationId::mint());
            n.status = NodeStatus::Dead;
            n
        };
        let kept = watched_node(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node");
        let restarted = watched_node(PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node");
        let superseded = (restarted.node_id.clone(), IncarnationId::mint());
        let deleted = (NodeId::mint(), IncarnationId::mint());
        let kept_birth = (kept.node_id.clone(), kept.incarnation_id.clone().unwrap());
        let watched: BTreeMap<String, Node> = [&kept, &restarted].into_iter().map(|n| (n.node_id.to_string(), n.clone())).collect();
        let mut offline: HashSet<_> = [kept_birth.clone(), superseded.clone(), deleted.clone()].into();
        let mut written: WrittenSilence = [(kept.node_id.clone(), (kept.incarnation_id.clone(), Some(NodeStatus::Dead))), (deleted.0.clone(), (Some(deleted.1.clone()), Some(NodeStatus::Dead)))].into();
        clear_unwatched(&watched, &mut offline, &mut written);
        assert_eq!(offline, [kept_birth.clone()].into(), "only the watched TRUE OFFLINE birth stays offline");
        assert_eq!(written.keys().cloned().collect::<HashSet<_>>(), [kept.node_id.clone()].into(), "the silence memo of a deleted node is deleted");
    }

    /// CONTRACT (node-recovery.md step 1: "Each node's pruner marks a member `PendingReconnect` once it is
    /// unheard past the staleness floor", step 2: "Half a floor after the mark it runs node
    /// investigation"; Q332: a spent call is the one clock that marks a birth; Q181): a node-admin
    /// member of the mesh-primary's own mesh that is heard within the floor and no call to which was
    /// spent is not silent: the mesh-primary's tick neither watches it, marks it nor lists it
    /// silent, and the primary watch does not investigate it. Must NOT happen: a live, heard node-admin
    /// tickled, investigated or replaced.
    #[test]
    fn a_heard_node_admin_member_that_answers_its_commands_is_not_investigated() {
        let digest_view = DigestView::default();
        let (primary, member) = (digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", MemberStatus::Ready));
        for d in [primary.clone(), member.clone()] {
            digest_view.record(d);
        }
        let mut view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        view.nodes.iter_mut().filter(|n| n.node_id == primary.node.node_id).for_each(|n| {
            n.is_primary = true;
            n.is_fabric_primary = true;
        });
        let me_id = primary.node.node_id.clone();
        let (now, now_ms) = (std::time::Instant::now(), local_now_ms());
        let floor = rdm_transport::topology::staleness_floor();
        let mut watched = watched_births(&view, &me_id, "mesh1", &[]);
        let marks = crate::offline::held_marks(&crate::offline::UnreachableMarks::default(), &view, &digest_view, now);
        crate::offline::watch_marked(&mut watched, &marks, &view, "mesh1", &me_id);
        assert!(watched.is_empty() && marks.is_empty(), "a heard node-admin that answers its commands is not watched: {watched:?} {marks:?}");
        let silent = crate::offline::silent_set(view.members().filter(|n| n.node_id == member.node.node_id), &digest_view, now, now_ms, floor);
        assert!(silent.iter().all(|s| s.marked_at_ms.is_none()), "heard within the floor: no mark: {silent:?}");
        let plan = crate::primary_watch::plan(&view, "mesh1", &(me_id, primary.node.incarnation.clone()), &mut crate::primary_watch::Said::default(), &|_| false, &|_| false);
        assert_eq!(plan, crate::primary_watch::Plan::default(), "the primary watch investigates no live node-admin");
    }

    #[tokio::test]
    async fn a_recovery_that_ends_failed_is_recorded_as_its_failed_step_never_as_submitted() {
        let operation = rdm_core::OperationId::location("node.delete", &rdm_core::PathName::new("mesh2.node", "1").unwrap());
        let failed = RecoveryEnd::Failed { attempt: 3, operation: operation.clone(), step: "NodeDeleted".into(), reason: "the exited-verdict row is missing".into() };
        let (outcome, r) = settle(Ok(failed));
        assert_eq!(outcome, format!("failed: attempt 3 {operation:?} step NodeDeleted: the exited-verdict row is missing"));
        assert_eq!(r, Err(outcome.clone()), "a failed recovery is not a submitted one: the next tick submits again");
        let (outcome, r) = settle(Ok(RecoveryEnd::Complete { attempt: 3 }));
        assert_eq!((outcome.as_str(), r), ("complete: attempt 3", Ok(())));
        let (outcome, r) = settle(Err("the run of attempt 3 ended without its terminal".into()));
        assert_eq!(outcome, "the run of attempt 3 ended without its terminal");
        assert!(r.is_err());
    }

    /// CONTRACT: loading the node key from the data dir never waits on the disk on a runtime
    /// worker: it runs on the blocking pool (`blocking::blocking_io`), as every store read
    /// does. The admin loads it inside its boot, on the runtime. The cell loads a key whose file
    /// holds the read: the runtime's only thread keeps running, and the load answers that key.
    #[test]
    fn loading_the_node_key_waits_on_the_disk_off_the_runtime_thread() {
        let d = crate::test_support::tempdir("node-key-offload");
        let key = SecretKey::generate();
        let fifo = d.join("node-key");
        let dir = d.clone();
        let (ran, loaded) = crate::test_support::runtime_runs_while_a_fifo_is_read(fifo, hex::encode(key.to_bytes()).into_bytes(), Box::pin(async move { load_or_mint_key(&dir).await.map(|k| k.to_bytes()) }));
        assert!(ran, "the runtime thread ran while the key's read waited on the disk");
        assert_eq!(loaded, Ok(key.to_bytes()));
    }

    fn fixture_actor(second: bool) -> rdm_core::framing::Birth {
        rdm_core::framing::Birth { node_id: NodeId::parse(if second { "200000000000" } else { "100000000000" }).unwrap(), incarnation: IncarnationId::parse("n4w3mjy921q8").unwrap() }
    }

    fn fabric1() -> FabricId {
        FabricId::parse("fab000000001").unwrap()
    }

    /// A fixed canonical id per test mesh (`mesh2` -> `meshid000002`).
    fn mesh_id(mesh: &str) -> MeshId {
        MeshId::parse(&format!("meshd{:0>7}", mesh.trim_start_matches("mesh"))).unwrap()
    }

    pub(super) fn digest(name: PathName, mesh: &str, r#type: &str, status: MemberStatus) -> MeshDigest {
        let node_id = NodeId::mint();
        MeshDigest { held_hash: Default::default(),
            fabric_id: fabric1(),
            node: MeshNode { mesh: mesh.to_owned(), r#type: r#type.to_string(),
                node_id: node_id.clone(),
                name: name.clone(),
                endpoint_id: EndpointId(format!("key-{node_id}")),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                runtime: None,
            },
            status,
            node_state: Default::default(),
            admin_api_base: (rdm_core::is_node_admin(&r#type)).then(|| format!("http://admin-{node_id}")),
            emitted_at_rafka_ms: 0,
            digest_seq: 1,
            mesh_id: Some(mesh_id(mesh)),
            in_flight: None,
            load: None,
            gossip: None,
            data_dir: None, is_primary: None, is_fabric_primary: None
        }
    }

    /// CONTRACT (lifecycles/states.md: "Mesh: `pending` → `ready` → `deleting` →
    /// `deleted` ... deleted is final"; mesh-delete.md step 7: the fabric-primary stops the deleted
    /// mesh's last admin, "from that admin's stop on it publishes the deleted mesh's topology"): the
    /// fabric-primary accepted mesh.deleted for mesh1 and holds mesh1's last admin, mesh1.node-admin.1, from
    /// that admin's own digest. The admin is stopped: the digest view holds it stopped. The view still states
    /// mesh1 deleted, under its id, with the stopped admin its one node. Must NOT happen: the stopped
    /// birth carrying no mesh id into the view, so mesh1 loses its id and is projected
    /// ready again (handover stem: the fabric-primary re-sent mesh1 the down op for the id
    /// its view lost, 410 ms after the stop, and its view named mesh1 live).
    #[test]
    fn a_deleted_mesh_whose_last_admin_is_stopped_stays_deleted_under_its_id() {
        let digest_view = DigestView::default();
        let records = Records::default();
        let last = digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready);
        assert!(digest_view.record(last.clone()));
        let id = last.mesh_id.clone().expect("a node-admin's digest states its mesh id");
        records.hold_mesh_deleted(&id);
        let mesh1 = |t: &Topology| t.meshes.iter().find(|m| m.name == "mesh1").map(|m| (m.id.clone(), m.status));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(mesh1(&t), Some((Some(id.clone()), ScopeStatus::Dead)), "mesh.deleted accepted: mesh1 is deleted");
        assert!(digest_view.stopped(rdm_core::LifecycleOp { r#type: last.node.r#type.clone(), mesh: last.node.mesh.clone(),
            build_id: "b7".into(),
            attempt: 1,
            operation: rdm_core::OperationId::location("node.stop", &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()),
            node_id: last.node.node_id.clone(),
            incarnation: last.node.incarnation.clone(),
            name: last.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        }));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        let n = t.node(&last.node.name).expect("the stopped admin is listed");
        assert!(n.stopped, "listed stopped: {n:?}");
        assert_eq!(mesh1(&t), Some((Some(id), ScopeStatus::Dead)), "a deleted mesh whose last admin is stopped stays deleted, under its id");
    }

    /// CONTRACT (node-identity-and-state.md §6 "Re-born": "the predecessor is deleted by the recovery
    /// Build, so the projection holds one entry per `(path, name)`"; §2.3: "the deleted row releases the
    /// `(path, name)`"): the birth at a path is the one the records say is current, stopped or not. A
    /// recovery re-created mesh1.node.2 under a new node id; the predecessor's row is `Deleted`, and its
    /// digest is still held, heard or silent past the staleness floor (its `NodeDeleted` never reached
    /// this view). The successor was then stopped. The projection lists the successor, stopped, at
    /// mesh1.node.2. Must NOT happen: the predecessor's held digest taking the path from the stopped
    /// successor (release red at 31666a3c3: `node.start:mesh2.node-admin.1 is targeted to birth ptkpqdd24s4x,
    /// the view holds z8pb1w7gckwy at that path`).
    #[tokio::test]
    async fn a_stopped_birth_keeps_its_path_from_an_older_birth_whose_row_is_deleted() {
        for (backend, nodes, _meshes) in record_stores("current-birth-at-path").await {
            let digest_view = DigestView::default();
            let records = Records::default();
            let _ = records.contacts.set(nodes.clone());
            let predecessor = digest(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", MemberStatus::Ready);
            let mut successor = digest(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", MemberStatus::Ready);
            successor.node.endpoint_id = EndpointId("key-mesh1.node.2-successor".into());
            // The records: the predecessor's node deleted by the recovery, the successor's row at the name.
            created_record(&*nodes, &predecessor).await;
            crate::node_rows::write_deleted(&*nodes, &crate::node_rows::WrittenRows::default(), &predecessor.node.name, &predecessor.node.node_id).await.ok().expect("the predecessor's Deleted record");
            created_record(&*nodes, &successor).await;
            // The view: the predecessor's digest held, the successor heard and then stopped.
            assert!(digest_view.record(predecessor.clone()));
            assert!(digest_view.record(successor.clone()));
            assert!(digest_view.stopped(rdm_core::LifecycleOp { r#type: successor.node.r#type.clone(), mesh: successor.node.mesh.clone(),
                build_id: "b9".into(),
                attempt: 9,
                operation: rdm_core::OperationId::location("node.stop", &rdm_core::PathName::new("mesh1.node", "2").unwrap()),
                node_id: successor.node.node_id.clone(),
                incarnation: successor.node.incarnation.clone(),
                name: successor.node.name.clone(),
                event_at_rafka_ms: 1,
                traceparent: None,
                tracestate: None,
            }));
            // Within the staleness floor the predecessor still publishes its own name (heard); past it,
            // it publishes nothing (silent). Either way the successor's row holds the name.
            for (when, at) in [("heard", std::time::Instant::now()), ("silent", std::time::Instant::now() + digest_view.staleness_floor() + std::time::Duration::from_secs(1))] {
                let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
                let reads = ViewReads { at, rows: super::projection::RowNames::read(&me, &digest_view, &records, &fabric1(), at).await };
                let t = project_with("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, &reads);
                let n = t.node(&successor.node.name).expect("a birth is listed at mesh1.node.2");
                assert_eq!(
                    n.incarnation_id.as_ref().map(|i| i.as_str()),
                    Some(successor.node.incarnation.as_str()),
                    "{backend}, predecessor {when}: mesh1.node.2 lists the successor {} (the birth its row names), not the deleted predecessor {}: {n:?}",
                    successor.node.incarnation.as_str(),
                    predecessor.node.incarnation.as_str()
                );
                assert!(n.stopped, "{backend}, predecessor {when}: listed stopped: {n:?}");
            }
        }
    }

    /// A memory and a file nodes store, each beside its own mesh store, on a root of its own: the two
    /// backends a cell over the records runs on.
    async fn record_stores(name: &str) -> Vec<(&'static str, Arc<dyn crate::storage::NodesStorage>, Arc<dyn crate::storage::MeshStorage>)> {
        #[allow(unused_mut)]
        let mut stores: Vec<(&'static str, Arc<dyn crate::storage::NodesStorage>, Arc<dyn crate::storage::MeshStorage>)> = vec![("memory", Arc::new(crate::storage::MemoryNodesStorage::default()), Arc::new(crate::storage::MemoryMeshStorage::default()))];
        #[cfg(feature = "file-backend")]
        {
            let d = crate::test_support::tempdir(name);
            let root = crate::store_root::StoreRoot::at(&d.join("root"));
            stores.push(("file", Arc::new(crate::storage::FileNodesStorage::open(&d.join("100000000000"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap()), Arc::new(crate::storage::FileMeshStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap())));
        }
        let _ = name;
        stores
    }

    /// `d` with the primaries its view holds for it: `is_primary`, `is_fabric_primary`.
    fn primaries(mut d: MeshDigest, is_primary: Option<u64>, is_fabric_primary: Option<u64>) -> MeshDigest {
        d.is_primary = is_primary;
        d.is_fabric_primary = is_fabric_primary;
        d
    }

    /// The primaries the records on `nodes` hold, taken into a view that holds none.
    async fn primaries_recorded(meshes: &dyn crate::storage::MeshStorage, nodes: &dyn crate::storage::NodesStorage) -> usize {
        rows_writer::take_recorded_primaries(meshes, nodes, "mesh1", &DigestView::default().primaries).await.unwrap()
    }

    /// The node ids of the births the records on `nodes` name to reach.
    async fn births_named(meshes: &dyn crate::storage::MeshStorage, nodes: &dyn crate::storage::NodesStorage) -> Vec<NodeId> {
        rows_writer::recorded_births(meshes, nodes, "mesh1").await.unwrap().into_iter().map(|b| b.node_id).collect()
    }

    /// CONTRACT (the store root's births are read off the Node records: each record's newest birth gives
    /// the node id, name, incarnation and primaries its mesh's writer recorded on it, and the endpoint and
    /// address the node wrote in its own part of its record at that birth (Q333);
    /// node-identity-and-state.md §2.7, the record; elections §3: the recorded primary is an election
    /// input), on either backend. Every read a restart makes of the births the store holds answers from
    /// the records: the clean-dir refusal (a birth the records name to reach is a durable topology; an
    /// empty root names none), recovery's gossip seeds and rafka-time's peers and the `PendingReconnect`
    /// publication and the stored topology (`recorded_births`: every live record whose newest birth
    /// carries an address, every mesh's, the mesh-primary's own included), the durable map on an
    /// inherited root (`TopologySource::DurableMap` over the same births), and the recorded primaries an
    /// election starts from, the own one included (`take_recorded_primaries`), a `Deleted` record's
    /// included: a deleted birth's recorded primary stays an input until a newer revision supersedes it
    /// (elections §3; the role moves before a primary is deleted, node-primary-update.md). Must NOT
    /// happen: a `Deleted` record's birth read as a node to reach, or a record whose birth carries no
    /// address yet read as a seed.
    #[tokio::test]
    async fn every_read_of_the_births_a_store_holds_answers_from_the_node_records_on_either_backend() {
        for (backend, nodes, meshes) in record_stores("births-from-records").await {
            assert!(rows_writer::recorded_births(&*meshes, &*nodes, "mesh1").await.unwrap().is_empty(), "{backend}: an empty root holds no durable topology");
            let admin = primaries(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), Some(3), Some(1));
            let rpc = primaries(digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready), Some(2), None);
            let unaddressed = digest(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", MemberStatus::Pending);
            // The rpc type's primary before its role moved to mesh1.node.1 at the next revision.
            let deleted = primaries(digest(PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", MemberStatus::Ready), Some(1), None);
            let other = digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready);
            for d in [&admin, &rpc, &unaddressed, &deleted, &other] {
                created_record(&*nodes, d).await;
            }
            let mut mesh1 = MeshRowsWriter::new("100000000000", "mesh1", mesh_id("mesh1"), nodes.clone(), meshes.clone(), Default::default());
            let mut mesh2 = MeshRowsWriter::new("mesh2.node-admin.1", "mesh2", mesh_id("mesh2"), nodes.clone(), meshes.clone(), Default::default());
            mesh1.sweep(&[admin.clone(), rpc.clone(), deleted.clone()], &writing(&["mesh1"]), |_| false).await;
            mesh2.sweep(std::slice::from_ref(&other), &writing(&["mesh2"]), |_| false).await;
            // Each node that started wrote its own part, its address in it; mesh1.node.2 has not yet.
            for d in [&admin, &rpc, &deleted, &other] {
                own_part(&*nodes, d).await;
            }
            crate::node_rows::write_deleted(&*nodes, &crate::node_rows::WrittenRows::default(), &deleted.node.name, &deleted.node.node_id).await.ok().expect("the deleted node's record");
            // The live records of every mesh: no Deleted one.
            let live: std::collections::BTreeSet<PathName> = crate::storage::live_records(&*meshes, &*nodes, "mesh1").await.unwrap().into_iter().map(|r| r.name.clone()).collect();
            assert_eq!(live, [PathName::new("mesh1.node-admin", "1").unwrap(), PathName::new("mesh1.node", "1").unwrap(), PathName::new("mesh1.node", "2").unwrap(), PathName::new("mesh2.node-admin", "1").unwrap()].into_iter().collect(), "{backend}: the live records of every mesh");
            // The clean-dir refusal, seeds, rafka-time's peers, the PendingReconnect publication and the stored topology.
            let mut births = rows_writer::recorded_births(&*meshes, &*nodes, "mesh1").await.unwrap();
            births.sort_by_key(|b| b.name.clone());
            let want: Vec<crate::reenter::MapNode> = [&admin, &rpc, &other]
                .into_iter()
                .map(|d| crate::reenter::MapNode { mesh: d.node.mesh.clone(), r#type: d.node.r#type.clone(), node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.clone(), transport_addr: d.node.transport_addr, incarnation: d.node.incarnation.clone(), transition: None, ready: false, data_dir: None, admin_api_base: None })
                .collect();
            assert_eq!(births, want, "{backend}: each live record's newest birth at the address its node wrote, every mesh's; no Deleted record's, none with no address");
            // The durable map on an inherited root.
            assert_eq!(crate::reenter::get_topology(crate::reenter::TopologySource::DurableMap(&births)).await.unwrap(), want, "{backend}: the durable map is the births the records hold");
            // The recorded primaries, the own one included.
            let election = DigestView::default();
            let taken = rows_writer::take_recorded_primaries(&*meshes, &*nodes, "mesh1", &election.primaries).await.unwrap();
            assert!((3..=4).contains(&taken), "{backend}: the mesh-primary's, the fabric-primary's and the rpc type's primary, the Deleted record's superseded: {taken}");
            let held = |role, revision, d: &MeshDigest| matches!(election.primaries.take(role, &rdm_core::Primary { mesh: "mesh1".into(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision }), rdm_transport::topology::RoleTaken::Same);
            assert!(held(rdm_core::PrimaryRole::of_type("node-admin"), 3, &admin) && held(rdm_core::PrimaryRole::FabricPrimary, 1, &admin) && held(rdm_core::PrimaryRole::of_type("node"), 2, &rpc), "{backend}: each recorded primary is held at its revision");
        }
    }

    /// CONTRACT (mesh-delete.md step 7: "Deleted births resolve Gone"; node-delete.md step 3: the
    /// deleted node's record is `Deleted`; a record keeps the facts of its current birth and of the one a
    /// restart replaced, the newest last): a birth that left is no seed and no primary once its
    /// record's next write lands, on either backend. A restarted node's record takes its new birth's
    /// facts as its newest: the replaced birth is no seed and its primaries are no election input, and
    /// once the node writes the new birth's address in its own part that birth is the seed. A deleted node's record
    /// is `Deleted`: its address is not read, and its recorded primary stays an election input until a
    /// newer revision supersedes it (elections §3), as a lost primary's does through fabric recovery.
    /// Must NOT happen: a replaced or a deleted birth handed to a restart as a node to reach, or a
    /// replaced birth taken as a recorded primary.
    #[tokio::test]
    async fn a_birth_that_left_is_no_seed_and_no_primary_after_its_records_next_write() {
        for (backend, nodes, meshes) in record_stores("birth-that-left").await {
            let admin = primaries(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), Some(3), None);
            let rpc = primaries(digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready), Some(2), None);
            for d in [&admin, &rpc] {
                created_record(&*nodes, d).await;
            }
            let written = crate::record_cache::Caches::default();
            let mut writer = MeshRowsWriter::new("100000000000", "mesh1", mesh_id("mesh1"), nodes.clone(), meshes.clone(), written.clone());
            writer.sweep(&[admin.clone(), rpc.clone()], &writing(&["mesh1"]), |_| false).await;
            for d in [&admin, &rpc] {
                own_part(&*nodes, d).await;
            }
            assert_eq!(births_named(&*meshes, &*nodes).await.len(), 2, "{backend}: both births are seeds");
            assert_eq!(primaries_recorded(&*meshes, &*nodes).await, 2, "{backend}: both primaries are recorded");
            // mesh1.node.1 restarts: its record's next write is its new birth's declared state.
            let restarted = MeshDigest { node: MeshNode { incarnation: IncarnationId::mint(), supersedes: Some(rpc.node.incarnation.clone()), ..rpc.node.clone() }, is_primary: None, ..rpc.clone() };
            written.nodes.update(&*nodes, &rpc.node.node_id, None, |r| r.birth_mut(&restarted.node.incarnation, &restarted.node.name).declared.insert(rdm_core::status::NodeState::Pending)).await.unwrap();
            assert_eq!(births_named(&*meshes, &*nodes).await, vec![admin.node.node_id.clone()], "{backend}: the replaced birth is no seed");
            assert_eq!(primaries_recorded(&*meshes, &*nodes).await, 1, "{backend}: the replaced birth's primary is no election input");
            writer.sweep(&[admin.clone(), restarted.clone()], &writing(&["mesh1"]), |_| false).await;
            own_part(&*nodes, &restarted).await;
            let seeds = rows_writer::recorded_births(&*meshes, &*nodes, "mesh1").await.unwrap();
            assert!(seeds.iter().any(|b| b.node_id == rpc.node.node_id && b.incarnation == restarted.node.incarnation), "{backend}: the new birth is the seed once the node writes its address: {seeds:?}");
            // mesh1.node-admin.1 is deleted: its record is Deleted.
            crate::node_rows::write_deleted(&*nodes, &written.nodes, &admin.node.name, &admin.node.node_id).await.ok().expect("the deleted node's record");
            assert_eq!(births_named(&*meshes, &*nodes).await, vec![rpc.node.node_id.clone()], "{backend}: the deleted node's birth is no seed");
            assert_eq!(primaries_recorded(&*meshes, &*nodes).await, 1, "{backend}: its recorded primary stays an input until a newer revision supersedes it");
        }
    }

    /// CONTRACT (R13: the fabric-primary writes the rows of a mesh with no primary, as its acting
    /// primary, `crate::node_rows::writes_for`; node-identity-and-state.md §2.7): the fabric-primary's
    /// rows writer writes the address and the primaries its view holds onto the records of a mesh with no
    /// primary, like any other mesh it writes, each record naming that mesh's id, and writes no Mesh row of
    /// that mesh: the Mesh row is its own primary's (or the fabric-primary's `create-mesh`). Once that
    /// mesh has a primary again, the fabric-primary writes none of its records. On either backend.
    #[tokio::test]
    async fn the_fabric_primary_writes_the_primaries_of_a_mesh_with_no_primary_onto_its_records() {
        for (backend, nodes, meshes) in record_stores("acting-addresses").await {
            let fp = primaries(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), Some(1), Some(1));
            let orphan = primaries(digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready), Some(4), None);
            for d in [&fp, &orphan] {
                created_record(&*nodes, d).await;
            }
            let counting = Arc::new(CountingNodes(nodes.clone(), Default::default()));
            let mut writer = MeshRowsWriter::new("100000000000", "mesh1", mesh_id("mesh1"), counting.clone(), meshes.clone(), Default::default());
            writer.sweep(&[fp.clone(), orphan.clone()], &writing(&["mesh1", "mesh2"]), |_| false).await;
            let b = newest_of(&*nodes, &orphan).await.unwrap_or_else(|| panic!("{backend}: mesh2's member's record carries its birth"));
            assert_eq!(b.is_primary, Some(4), "{backend}: the acting primary writes the primaries its view holds");
            assert_eq!(nodes.node(&orphan.node.node_id).await.unwrap().and_then(|r| r.mesh_id), Some(mesh_id("mesh2")), "{backend}: the record names its own mesh's id");
            let named: Vec<String> = meshes.meshes().await.unwrap().into_iter().map(|m| m.name).collect();
            assert_eq!(named, vec!["mesh1".to_string()], "{backend}: the acting primary writes no Mesh row of the mesh it acts for");
            // mesh2 has a primary again: the fabric-primary writes none of its records.
            let before = counting.1.load(std::sync::atomic::Ordering::SeqCst);
            let moved = MeshDigest { node: MeshNode { transport_addr: "127.0.0.1:41999".parse().unwrap(), ..orphan.node.clone() }, ..orphan.clone() };
            writer.sweep(&[fp.clone(), moved], &writing(&["mesh1"]), |_| false).await;
            assert_eq!(counting.1.load(std::sync::atomic::Ordering::SeqCst), before, "{backend}: a mesh it no longer writes is not written");
        }
    }

    /// CONTRACT (storage-whole-records.md: one writer per record, each record written whole from the
    /// writer's copy; R13, `crate::node_rows::writes_for`): a mesh whose mesh-primary this admin's view
    /// holds by another birth is that primary's to write, even in a sweep whose writing set, read from a
    /// view that has not caught up, still names the mesh. Must NOT happen: the fabric-primary writes a
    /// record of a mesh whose primary is already writing it, its older copy replacing what that primary
    /// wrote (fabric_states: mesh2.node-admin.1's Ready lost to the fabric-primary's `is_primary` write).
    #[tokio::test]
    async fn the_fabric_primary_writes_no_record_of_a_mesh_its_view_holds_a_mesh_primary_for() {
        for (backend, nodes, meshes) in record_stores("acting-until-held").await {
            let fp = primaries(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), Some(1), Some(1));
            let held = primaries(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), Some(1), None);
            let member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
            for d in [&fp, &held, &member] {
                created_record(&*nodes, d).await;
            }
            let counting = Arc::new(CountingNodes(nodes.clone(), Default::default()));
            let mut writer = MeshRowsWriter::new("100000000000", "mesh1", mesh_id("mesh1"), counting.clone(), meshes.clone(), Default::default());
            writer.sweep(std::slice::from_ref(&fp), &writing(&["mesh1"]), |_| false).await;
            let before = counting.1.load(std::sync::atomic::Ordering::SeqCst);
            writer.sweep(&[fp.clone(), held.clone(), member.clone()], &writing(&["mesh1", "mesh2"]), |_| false).await;
            assert_eq!(counting.1.load(std::sync::atomic::Ordering::SeqCst), before, "{backend}: no record of mesh2, whose mesh-primary the view holds, is written");
            assert!(newest_of(&*nodes, &held).await.and_then(|b| b.is_primary).is_none(), "{backend}: mesh2's primary's record is its own to write");
        }
    }

    /// The election's node of the birth `d` names, at `status`.
    fn elected_node(d: &MeshDigest, status: NodeStatus) -> crate::model::Node {
        let mut n = crate::model::Node::allocated(d.node.name.clone(), d.node.mesh.clone(), &d.node.r#type);
        n.node_id = d.node.node_id.clone();
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.status = status;
        n
    }

    /// The node-admin `nodes` resolve as `mesh`'s primary.
    fn mesh_primary_of(nodes: &[crate::model::Node], mesh: &str) -> Option<NodeId> {
        nodes.iter().find(|n| n.is_primary && n.mesh == mesh && rdm_core::is_node_admin(&n.r#type)).map(|n| n.node_id.clone())
    }

    /// CONTRACT (elections §3: the recorded primary is an election input, "the recorded primary, not yet
    /// heard -> it waits"; the Q122 line: the node-admin type of a mesh whose unheard mesh-primary was
    /// also the fabric-primary elects no node-admin until fabric recovery's restore; Q227: a launched
    /// node-admin reads no store at its start and pulls what it needs from its mesh-primary or the
    /// fabric-primary): the node-admin re-created at the lost fabric-primary's path and name is launched by
    /// the standby that runs fabric recovery, which holds the lost birth's records (mesh-primary and
    /// fabric-primary, revision 1) and holds that birth gone. The re-created birth takes them from the
    /// `Primaries` frames its pull is served (node.topology.get: each record the server holds, `gone`
    /// as the server's election holds the birth lost), as `TopologyView::learn_primaries` takes them, so
    /// a standby's Ready digest heard before its entry elects nobody; the restore at revision 2 names
    /// the re-created birth. Must NOT happen: a transient standby winner, or a record read from the store.
    #[tokio::test]
    async fn a_re_created_admin_holds_the_lost_primarys_records_from_its_pull_and_elects_no_node_admin_until_the_restore() {
        let lost = digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready);
        let standby = digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", MemberStatus::Ready);
        let lost_record = rdm_core::Primary { mesh: "mesh1".into(), node_id: lost.node.node_id.clone(), incarnation: lost.node.incarnation.clone(), revision: 1 };
        // The standby that launches the re-created birth: it holds the lost birth's records, and the birth gone.
        let server = DigestView::default();
        server.primaries.take(rdm_core::PrimaryRole::MeshPrimary, &lost_record);
        server.primaries.take(rdm_core::PrimaryRole::FabricPrimary, &lost_record);
        server.primaries.mark_gone(lost.node.incarnation.clone());
        // The Primaries frames its topology door serves (boot.rs: the records it holds, `gone` from its election's lost set).
        let lost_set = super::projection::incumbency_of(&server, &Records::default()).lost;
        let mut served: Vec<(rdm_core::PrimaryRole, rdm_core::Primary, bool)> = server.primaries.meshes().into_values().map(|p| (rdm_core::PrimaryRole::MeshPrimary, p)).chain(server.primaries.fabric().map(|p| (rdm_core::PrimaryRole::FabricPrimary, p))).map(|(s, p)| { let gone = lost_set.contains(&p.incarnation); (s, p, gone) }).collect();
        served.sort_by_key(|(s, _, _)| s.name());
        assert_eq!(served.iter().map(|(s, p, g)| (s.name(), p.node_id.clone(), *g)).collect::<Vec<_>>(), vec![("fabric-primary".to_string(), lost.node.node_id.clone(), true), ("mesh-primary".to_string(), lost.node.node_id.clone(), true)], "the pull serves both of the lost birth's records, each gone");
        // The re-created birth takes them as `TopologyView::learn_primaries` does, then hears the standby's Ready.
        let digest_view = DigestView::default();
        for (role, primary, gone) in &served {
            digest_view.primaries.take(role.clone(), primary);
            if *gone {
                digest_view.primaries.mark_gone(primary.incarnation.clone());
            }
        }
        let recreated = digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Pending);
        let mut view = vec![elected_node(&recreated, NodeStatus::Pending), elected_node(&standby, NodeStatus::Ready)];
        crate::election::resolve_with(&mut view, &super::projection::incumbency_of(&digest_view, &Records::default()));
        assert_eq!(mesh_primary_of(&view, "mesh1"), None, "the lost primary's record holds the election: no node-admin is elected");
        assert!(view.iter().all(|n| !n.is_fabric_primary), "nor fabric-primary");
        // Fabric recovery's restore at revision 2.
        let restored = rdm_core::Primary { mesh: "mesh1".into(), node_id: recreated.node.node_id.clone(), incarnation: recreated.node.incarnation.clone(), revision: 2 };
        digest_view.primaries.take(rdm_core::PrimaryRole::MeshPrimary, &restored);
        digest_view.primaries.take(rdm_core::PrimaryRole::FabricPrimary, &restored);
        let mut view = vec![elected_node(&recreated, NodeStatus::Ready), elected_node(&standby, NodeStatus::Ready)];
        crate::election::resolve_with(&mut view, &super::projection::incumbency_of(&digest_view, &Records::default()));
        assert_eq!(mesh_primary_of(&view, "mesh1"), Some(recreated.node.node_id.clone()), "the restore names the re-created birth");
    }

    /// CONTRACT (elections §3: a recorded primary is held only when it supersedes the one held;
    /// node-primary-update.md: the role moves to the successor at the next revision before the
    /// predecessor is deleted; Q149 arm A): in a mesh whose deleted former primary's record still carries
    /// its older `is_primary`, the boot read holds the successor's later revision whatever order the
    /// records are read in, and the election names the successor. Must NOT happen: the Deleted record
    /// pins the election to its obsolete primary.
    #[tokio::test]
    async fn a_deleted_former_primarys_older_record_never_overrides_its_successors() {
        for (backend, nodes, meshes) in record_stores("deleted-older-primary").await {
            let former = primaries(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), Some(1), None);
            let successor = primaries(digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), Some(2), None);
            let other = digest(PathName::new("mesh1.node-admin", "3").unwrap(), "mesh1", "node-admin", MemberStatus::Ready);
            for d in [&former, &successor, &other] {
                created_record(&*nodes, d).await;
            }
            let mut writer = MeshRowsWriter::new("200000000000", "mesh1", mesh_id("mesh1"), nodes.clone(), meshes.clone(), Default::default());
            writer.sweep(&[former.clone(), successor.clone(), other.clone()], &writing(&["mesh1"]), |_| false).await;
            crate::node_rows::write_deleted(&*nodes, &crate::node_rows::WrittenRows::default(), &former.node.name, &former.node.node_id).await.ok().expect("the former primary's Deleted record");
            let digest_view = DigestView::default();
            rows_writer::take_recorded_primaries(&*meshes, &*nodes, "mesh1", &digest_view.primaries).await.unwrap();
            assert_eq!(digest_view.primaries.meshes().get("mesh1").map(|p| (p.node_id.clone(), p.revision)), Some((successor.node.node_id.clone(), 2)), "{backend}: the successor's later revision is held");
            let mut view = vec![elected_node(&successor, NodeStatus::Ready), elected_node(&other, NodeStatus::Ready)];
            crate::election::resolve_with(&mut view, &super::projection::incumbency_of(&digest_view, &Records::default()));
            assert_eq!(mesh_primary_of(&view, "mesh1"), Some(successor.node.node_id.clone()), "{backend}: the election names the successor");
        }
    }

    /// CONTRACT (R-OW1: "`admin_api_base` is config, never checked"; lifecycles/node-name-update.md: "Config on the
    /// row, `admin_api_base` included, is stored and gossiped as given; nothing checks it"): a
    /// node-admin birth this admin's view holds ready is Ready, its digest carrying no
    /// `admin_api_base`. What must NOT happen: a node-admin refused Ready for a config value RDM
    /// never reads.
    #[tokio::test]
    async fn a_node_admin_with_no_admin_api_base_is_ready_once_its_view_holds_it_ready() {
        let digest_view = DigestView::default();
        let mut d = digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", MemberStatus::Ready);
        d.admin_api_base = None;
        assert!(digest_view.record(d.clone()), "the view takes the node-admin's digest");
        assert_eq!(digest_view.get(d.node.node_id.as_str()).map(|(held, _)| held.status), Some(MemberStatus::Ready), "the view holds the birth ready");
        let mut node = Node::allocated(d.node.name.clone(), d.node.mesh.clone(), &d.node.r#type);
        node.node_id = d.node.node_id.clone();
        node.incarnation_id = Some(d.node.incarnation.clone());
        let observer = TopologyObserver { digest_view, client: None, commands: Arc::default(), status_door: Arc::default(), handover: Arc::default(), unreachable: Arc::default() };
        assert_eq!(crate::deployment::pipeline::NodeObserver::ready(&observer, &node).await, Ok(()));
    }

    /// A nodes store that counts the record writes made through it.
    #[cfg(feature = "file-backend")]
    struct CountingNodes(Arc<dyn crate::storage::NodesStorage>, std::sync::atomic::AtomicUsize);

    #[cfg(feature = "file-backend")]
    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for CountingNodes {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.0.runtimes().await
        }
        async fn runtime(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.0.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.nodes(mesh).await
        }
        async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_properties(record).await
        }
        async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, crate::storage_error::StorageError> {
            self.0.properties(node_id).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.put_node(row).await
        }
        async fn records_at(&self, name: &crate::model::PathName) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.records_at(name).await
        }
    }

    /// A nodes store whose record writes the store refuses while `refusing` is set, counting each
    /// refusal and each write it acknowledged.
    struct RefusingNodes {
        inner: Arc<dyn crate::storage::NodesStorage>,
        refusing: std::sync::atomic::AtomicBool,
        refused: std::sync::atomic::AtomicUsize,
        acknowledged: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for RefusingNodes {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.nodes(mesh).await
        }
        async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_properties(record).await
        }
        async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, crate::storage_error::StorageError> {
            self.inner.properties(node_id).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            if self.refusing.load(std::sync::atomic::Ordering::SeqCst) {
                self.refused.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(crate::storage_error::StorageError::Io { file: format!("nodes/{}/records/{}", row.mesh, row.node_id), reason: "No space left on device (os error 28)".into() });
            }
            self.inner.put_node(row).await?;
            self.acknowledged.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn records_at(&self, name: &crate::model::PathName) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.records_at(name).await
        }
    }

    /// The record of `d`'s node as its create writes it, before any address is on it.
    async fn created_record(nodes: &dyn crate::storage::NodesStorage, d: &MeshDigest) {
        crate::node_rows::write_record(nodes, &crate::storage::NodeRow::new(d.node.name.clone(), d.node.mesh.clone(), &d.node.r#type, d.node.node_id.clone(), NodeStatus::Pending)).await.unwrap().unwrap();
    }

    /// `d`'s own part of its record (Q333): its address, as its own writer puts it at its start.
    async fn own_part(nodes: &dyn crate::storage::NodesStorage, d: &MeshDigest) {
        nodes.put_properties(&rdm_node::node_records::NodeProperties {
            birth: rdm_node::node_records::RecordBirth { node_id: d.node.node_id.clone(), incarnation_id: d.node.incarnation.clone() },
            config: Default::default(), tags: Default::default(),
            endpoint_id: Some(d.node.endpoint_id.clone()), transport_addr: Some(d.node.transport_addr), listeners: Vec::new(), app_types: None, held: Default::default(),
        }).await.unwrap();
    }

    /// The newest birth `nodes` holds on `d`'s node's record, when that birth is `d`'s.
    async fn newest_of(nodes: &dyn crate::storage::NodesStorage, d: &MeshDigest) -> Option<crate::storage::BirthFacts> {
        nodes.node(&d.node.node_id).await.unwrap().and_then(|r| r.newest_birth().cloned()).filter(|b| b.incarnation_id == d.node.incarnation)
    }

    /// The meshes a writer writes: `meshes`.
    fn writing(meshes: &[&str]) -> std::collections::BTreeSet<String> {
        meshes.iter().map(|m| m.to_string()).collect()
    }

    /// The mesh-primary's rows writer over `nodes`, for `digest_view`, run as boot runs it: it is mesh-primary.
    async fn rows_written_over(nodes: Arc<dyn crate::storage::NodesStorage>) {
        let refusing = Arc::new(RefusingNodes { inner: nodes, refusing: true.into(), refused: Default::default(), acknowledged: Default::default() });
        let digest_view = DigestView::default();
        let (admin, rpc) = (digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready));
        assert!(digest_view.record(admin.clone()) && digest_view.record(rpc.clone()));
        for d in [&admin, &rpc] {
            created_record(&*refusing.inner, d).await;
        }
        // The election's result this admin holds: itself mesh-primary at 3, mesh1.node.1 its type's primary at 2.
        let primary_of = |d: &MeshDigest, revision| rdm_core::Primary { mesh: "mesh1".into(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision };
        digest_view.primaries.take(rdm_core::PrimaryRole::MeshPrimary, &primary_of(&admin, 3));
        digest_view.primaries.take(rdm_core::PrimaryRole::NodePrimary("node".into()), &primary_of(&rpc, 2));
        let writer = MeshRowsWriter::new("100000000000", "mesh1", mesh_id("mesh1"), refusing.clone(), Arc::new(crate::storage::MemoryMeshStorage::default()), Default::default());
        let (_writes_tx, writes) = tokio::sync::watch::channel(writing(&["mesh1"]));
        let task = tokio::spawn(rows_writer::keep_mesh_rows(writer, digest_view.clone(), fabric1(), writes, crate::stop_gate::StopGate::new(), Arc::default()));
        let at = |n: std::sync::atomic::Ordering| (refusing.refused.load(n), refusing.acknowledged.load(n));
        tokio::time::timeout(Duration::from_secs(10), async {
            while at(std::sync::atomic::Ordering::SeqCst).0 < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the store refused the first sweep's writes");
        assert_eq!(at(std::sync::atomic::Ordering::SeqCst).1, 0, "nothing was acknowledged while the store refused");
        // The store recovers. No birth, digest or primary record changes from here.
        let (births, primaries) = (*digest_view.birth_changes().borrow(), *digest_view.primaries.subscribe().borrow());
        refusing.refusing.store(false, std::sync::atomic::Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(30), async {
            let addressed = async |d: &MeshDigest| newest_of(&*refusing.inner, d).await.is_some_and(|b| b.is_primary.is_some());
            while !(addressed(&admin).await && addressed(&rpc).await) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the mesh-primary writes its owed rows on its own clock once the store recovers, with no topology change");
        assert_eq!((*digest_view.birth_changes().borrow(), *digest_view.primaries.subscribe().borrow()), (births, primaries), "no birth or primary changed while the rows were owed");
        let row = async |d: &MeshDigest| newest_of(&*refusing.inner, d).await.expect("the member's record carries its birth");
        let (a, r) = (row(&admin).await, row(&rpc).await);
        assert_eq!((a.is_primary, a.is_fabric_primary), (Some(3), None), "the mesh-primary's own row records is_primary at its revision");
        assert_eq!((r.is_primary, r.is_fabric_primary), (Some(2), None), "the rpc node's row records is_primary at its revision");
        assert_eq!(refusing.inner.node(&rpc.node.node_id).await.unwrap().and_then(|r| r.mesh_id), Some(mesh_id("mesh1")), "the record names its mesh by id");
        task.abort();
    }

    /// CONTRACT (node-identity-and-state.md §2.7: "A write the store refuses or fails is owed: the writer
    /// retries the row's current state under the one backoff rule until the store acknowledges it, on its
    /// own clock, never waiting for a birth, a digest or a primary change, and marks the row written only
    /// on that acknowledgement"; node-primary-update.md step 5, the record): the mesh-primary's rows
    /// writer meets a store that refuses its first writes; no birth, digest or primary record changes
    /// after that; the store recovers, and the writer persists every member's row with the primaries its
    /// view holds (`is_primary` at its revision). What must NOT happen: an owed row that waits for a
    /// topology change to be written again. Memory backend.
    #[tokio::test]
    async fn a_row_the_store_refused_is_written_on_the_writers_own_clock_once_the_store_recovers() {
        rows_written_over(Arc::new(crate::storage::MemoryNodesStorage::default())).await;
    }

    /// CONTRACT (storage.md §2 "Changing it", step 2: the copy holds every line of the old provider; i149
    /// R8: "Block new writes and claims, finish issued operations before copying"; lead decision D13):
    /// once its mesh-primary has replied `blocked`, nothing of its mesh writes the old provider. The
    /// mesh-primary's rows writer has written its members' rows; it replies `blocked`; then the rpc
    /// node's type primary changes (`is_primary` 2 to 4), the row change a storage change's copy would
    /// miss. While blocked the writer writes nothing; the row stays owed. At the unblock it writes the
    /// row whole with `is_primary` at 4 on the provider current then. Must NOT happen: a write while
    /// blocked, or the change dropped at the unblock. Memory backend.
    #[tokio::test]
    async fn a_row_change_after_blocked_is_held_and_written_at_the_unblock() {
        let nodes = Arc::new(RefusingNodes { inner: Arc::new(crate::storage::MemoryNodesStorage::default()), refusing: false.into(), refused: Default::default(), acknowledged: Default::default() });
        let digest_view = DigestView::default();
        let (admin, rpc) = (digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready));
        assert!(digest_view.record(admin.clone()) && digest_view.record(rpc.clone()));
        for d in [&admin, &rpc] {
            created_record(&*nodes.inner, d).await;
        }
        let primary_of = |d: &MeshDigest, revision| rdm_core::Primary { mesh: "mesh1".into(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision };
        digest_view.primaries.take(rdm_core::PrimaryRole::MeshPrimary, &primary_of(&admin, 3));
        digest_view.primaries.take(rdm_core::PrimaryRole::NodePrimary("node".into()), &primary_of(&rpc, 2));
        let commands: Arc<crate::node_commands::CommandView> = Arc::default();
        let writer = MeshRowsWriter::new("100000000000", "mesh1", mesh_id("mesh1"), nodes.clone(), Arc::new(crate::storage::MemoryMeshStorage::default()), Default::default());
        let (_writes_tx, writes) = tokio::sync::watch::channel(writing(&["mesh1"]));
        let task = tokio::spawn(rows_writer::keep_mesh_rows(writer, digest_view.clone(), fabric1(), writes, crate::stop_gate::StopGate::new(), commands.clone()));
        let is_primary = async |d: &MeshDigest| newest_of(&*nodes.inner, d).await.and_then(|b| b.is_primary);
        tokio::time::timeout(Duration::from_secs(10), async {
            while is_primary(&rpc).await != Some(2) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the rows writer writes the members' rows before the block");
        assert_eq!(commands.block().await, 0, "no command in flight at the block");
        let before = nodes.acknowledged.load(std::sync::atomic::Ordering::SeqCst);
        digest_view.primaries.take(rdm_core::PrimaryRole::NodePrimary("node".into()), &primary_of(&rpc, 4));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(nodes.acknowledged.load(std::sync::atomic::Ordering::SeqCst), before, "nothing is written while the mesh-primary is blocked");
        assert_eq!(is_primary(&rpc).await, Some(2), "the row still holds what was copied");
        commands.unblock();
        tokio::time::timeout(Duration::from_secs(10), async {
            while is_primary(&rpc).await != Some(4) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the held row change is written at the unblock");
        task.abort();
    }

    /// The same contract over the file backend on the shared store root.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_row_the_store_refused_is_written_on_the_writers_own_clock_once_the_store_recovers_on_the_file_backend() {
        let d = crate::test_support::tempdir("rows-owed");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        rows_written_over(Arc::new(crate::storage::FileNodesStorage::open(&d.join("100000000000"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap())).await;
    }

    /// CONTRACT (R14 item 2, the writer ladder node-identity-and-state.md §4.3; #3070 "the contacts
    /// family must follow the ladder"): on one shared root, a mesh's members' addresses on their records
    /// and its Mesh row are written by that mesh's primary alone, for its own members alone. mesh1's
    /// primary and another admin of mesh1 run over the same held births at once: the other writes
    /// nothing, the primary writes the address of each of mesh1's members (itself included) onto its
    /// record, and mesh1's Mesh row, never mesh2's. The primary role moves to the other admin while
    /// mesh1.node.1 is replaced by a new node at its path and name (its record `Deleted`, the new node's
    /// written): the new mesh-primary writes the new member's address, the store's births name it and
    /// not the deleted one, and the old mesh-primary writes nothing more. A launched birth's publication
    /// writes no record.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_meshs_member_records_and_mesh_row_are_written_by_its_primary_alone_on_the_shared_root() {
        let d = crate::test_support::tempdir("addresses-ladder");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let store = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(CountingNodes(Arc::new(crate::storage::FileNodesStorage::open(&data, &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap()), Default::default())) }
        };
        let (p_nodes, q_nodes, created_rows) = (store("100000000000").await, store("200000000000").await, store("created").await);
        let (p_meshes, q_meshes): (Arc<dyn crate::storage::MeshStorage>, Arc<dyn crate::storage::MeshStorage>) =
            (Arc::new(crate::storage::FileMeshStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap()), Arc::new(crate::storage::FileMeshStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap()));
        let (p, q, rpc, other) = (digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready), digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready));
        for m in [&p, &q, &rpc, &other] {
            created_record(&*created_rows, m).await;
        }
        let held = vec![p.clone(), q.clone(), rpc.clone(), other.clone()];
        let mut pw = MeshRowsWriter::new("100000000000", "mesh1", mesh_id("mesh1"), p_nodes.clone(), p_meshes, Default::default());
        let mut qw = MeshRowsWriter::new("200000000000", "mesh1", mesh_id("mesh1"), q_nodes.clone(), q_meshes.clone(), Default::default());
        let (primary, not_primary) = (writing(&["mesh1"]), writing(&[]));
        tokio::join!(pw.sweep(&held, &primary, |_| false), qw.sweep(&held, &not_primary, |_| false));
        // Each node writes its own part, its address in it (Q333).
        for m in [&p, &q, &rpc, &other] {
            own_part(&*created_rows, m).await;
        }
        for m in [&p, &q, &rpc] {
            assert!(newest_of(&*created_rows, m).await.is_some(), "mesh1's primary writes {}'s birth on its record", m.node.node_id);
        }
        assert_eq!(p_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 3, "mesh1's primary writes its three members' records");
        assert_eq!(newest_of(&*created_rows, &other).await, None, "mesh1's primary writes nothing of mesh2's");
        assert_eq!(q_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 0, "an admin that is not its mesh's primary writes no record");
        let mesh_rows = crate::store_log::keys_in(&root.dir().join(crate::store_root::MESHES_FILE), "mesh-record").unwrap();
        assert_eq!(mesh_rows, [mesh_id("mesh1").to_string()], "mesh1's primary writes mesh1's record alone");
        // Q becomes mesh1's primary; mesh1.node.1 is replaced by a new node at its path and name.
        crate::node_rows::write_deleted(&*created_rows, &crate::node_rows::WrittenRows::default(), &rpc.node.name, &rpc.node.node_id).await.ok().expect("the replaced node's record is Deleted");
        let recovered = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        created_record(&*created_rows, &recovered).await;
        own_part(&*created_rows, &recovered).await;
        let held = vec![p.clone(), q.clone(), recovered.clone(), other.clone()];
        let before = p_nodes.1.load(std::sync::atomic::Ordering::SeqCst);
        tokio::join!(pw.sweep(&held, &not_primary, |_| false), qw.sweep(&held, &primary, |_| false));
        assert_eq!(p_nodes.1.load(std::sync::atomic::Ordering::SeqCst), before, "the old primary writes nothing once the role moved");
        assert_eq!(q_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 1, "the new mesh-primary writes the one record that differs: the new member's");
        let now: Vec<NodeId> = rows_writer::recorded_births(&*q_meshes, &*q_nodes, "mesh1").await.unwrap().into_iter().map(|b| b.node_id).collect();
        assert_eq!(now.len(), 3, "{now:?}");
        assert!(now.contains(&recovered.node.node_id) && !now.contains(&rpc.node.node_id), "the store's births name the new member at the path, never the deleted one");
        // A launched birth's publication writes no record.
        let records = Records::default();
        let _ = records.contacts.set(q_nodes.clone());
        let writes = q_nodes.1.load(std::sync::atomic::Ordering::SeqCst);
        records.publish(crate::reenter::MapNode { mesh: "mesh1".into(), r#type: "node".into(), node_id: NodeId::mint(), name: rdm_core::PathName::new("mesh1.node", "2").unwrap(), endpoint_id: EndpointId("key-x".into()), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: IncarnationId::mint(), transition: None, ready: false, data_dir: None, admin_api_base: None }.as_node(NodeStatus::Pending, ProviderKind::Process));
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(q_nodes.1.load(std::sync::atomic::Ordering::SeqCst), writes, "a publication wrote a record");
    }

    /// CONTRACT (node-identity-and-state.md §2.7: the mesh-primary "checks the mesh's name against the mesh records at the key
    /// `(path, name)` ... Held means held by a different MeshId:
    /// `NameHeld` names that holder"; "A mesh whose name another mesh's row already holds stays held,
    /// naming the holder; it is never renamed, and the id never moves"; the mesh's row is its create's
    /// first durable write, and nothing else of the mesh is written beside a refused one): on one shared root, mesh A's primary writes mesh2's row and its member's
    /// address on its record. A second mesh under the same name (another MeshId) has its primary's
    /// sweeps refused by name: the row still names A, the second primary writes no record, and it is refused
    /// once per role tenure: one `rdm.node_admin.mesh.create.via-mesh-row` span `name-held` naming A,
    /// however many sweeps run. Must NOT happen: the second mesh's put overwriting A's row, its records
    /// written beside a refused write, or a refused write tried again on every birth change.
    // @feature: mesh-lifecycle
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_second_mesh_of_a_live_name_is_refused_by_name_and_writes_nothing() {
        let d = crate::test_support::tempdir("mesh-row-live");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let store = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(CountingNodes(Arc::new(crate::storage::FileNodesStorage::open(&data, &root, "mesh2", &crate::store_log::LogWriter::mint()).await.unwrap()), Default::default())) }
        };
        let (a_nodes, b_nodes, created_rows) = (store("a.node-admin").await, store("b.node-admin").await, store("created").await);
        let (a_meshes, b_meshes): (Arc<dyn crate::storage::MeshStorage>, Arc<dyn crate::storage::MeshStorage>) =
            (Arc::new(crate::storage::FileMeshStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap()), Arc::new(crate::storage::FileMeshStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap()));
        let (first, second) = (mesh_id("mesh2"), MeshId::mint());
        let (a_admin, b_admin) = (digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready));
        created_record(&*created_rows, &a_admin).await;
        let capture = crate::span_capture::Capture::new("i143-3089-mesh-name-held");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let mut aw = MeshRowsWriter::new("mesh2.node-admin.1", "mesh2", first.clone(), a_nodes.clone(), a_meshes.clone(), Default::default());
        aw.sweep(std::slice::from_ref(&a_admin), &writing(&["mesh2"]), |_| false).await;
        assert_eq!(crate::storage::MeshStorage::mesh_named(&*a_meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(first.clone()), "mesh A's primary writes mesh2's row");
        assert_eq!(a_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 1, "mesh A's primary writes its member's record once mesh2's row is written");
        let mut bw = MeshRowsWriter::new("mesh2.node-admin.1", "mesh2", second.clone(), b_nodes.clone(), b_meshes.clone(), Default::default());
        for _sweep in 0..2 {
            bw.sweep(std::slice::from_ref(&b_admin), &writing(&["mesh2"]), |_| false).await;
        }
        assert_eq!(crate::storage::MeshStorage::mesh_named(&*b_meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(first.clone()), "the second mesh's write at a live name writes no row: mesh2's row still names mesh A");
        assert_eq!(b_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 0, "the second mesh's primary writes no record beside its refused write");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("mesh_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("holder").cloned().unwrap_or_default())).collect();
        assert_eq!(
            said,
            vec![(first.to_string(), "row-written".to_string(), String::new()), (second.to_string(), "name-held".to_string(), first.to_string())],
            "one write per role tenure, the refusal naming the holder: {spans:?}"
        );
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[1].trace_id, spans[1].attributes);
    }

    /// CONTRACT (R13: the fabric-primary writes as the acting primary of a mesh with no primary;
    /// lifecycles/node-create.md "Mesh absent ... Until the mesh has a primary, the fabric-primary writes
    /// its rows"; the mesh's row is its create's first durable write, nothing else of the
    /// mesh is written beside a refused one; node-identity-and-state.md §2.7: `NameHeld` names the holder, the same MeshId
    /// writing its own row again is idempotent): `create-mesh:mesh2` at the fabric-primary writes mesh2's row first.
    /// On a free key it writes the Mesh row under the id the operation carries and holds that id for the
    /// mesh's launches; a re-run of the operation writes the same id and finds its own row. Where another mesh's
    /// live row holds mesh3, the operation fails naming the holder: mesh3's row stands and no id is held
    /// for mesh3, so no launch can carry one and the Build runs no create of mesh3's nodes. One
    /// `rdm.node_admin.mesh.create.via-mesh-row` span per write, naming the fabric-primary. Must NOT
    /// happen: a mesh id held, or anything of the mesh written, beside a refused write.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn create_mesh_writes_the_mesh_record_first_and_a_refused_name_holds_nothing_of_the_mesh() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let records = Records::default();
        let holder = MeshId::mint();
        let theirs = crate::storage::MeshRecord { mesh_id: holder.clone(), name: "mesh3".into(), state: rdm_core::status::MeshState::Pending };
        assert_eq!(crate::storage::write_mesh(&meshes, None, &theirs).await, Ok(Ok(crate::storage::MeshWritten::Written)));
        // The ids the two operations carry, minted when each mesh entered the accepted topology.
        let (carried, mesh3_carried) = (MeshId::mint(), MeshId::mint());
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-row");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let minted = create_mesh(&records, &meshes, "100000000000", "mesh2", &carried).await.expect("a free name is written");
        assert_eq!(minted, carried, "the row is written for the id the operation carries");
        assert_eq!(crate::storage::MeshStorage::mesh_named(&meshes, "mesh2").await.unwrap(), Some(crate::storage::MeshRecord { mesh_id: minted.clone(), name: "mesh2".into(), state: rdm_core::status::MeshState::Pending }), "mesh2's row is written under the carried id");
        assert_eq!(records.meshes.lock().unwrap().get("mesh2"), Some(&minted), "the written row's id is what the mesh's launches carry");
        assert_eq!(create_mesh(&records, &meshes, "100000000000", "mesh2", &carried).await, Ok(minted.clone()), "a re-run writes the same id");
        let refused = create_mesh(&records, &meshes, "100000000000", "mesh3", &mesh3_carried).await.expect_err("mesh3's name is held by another mesh's live row");
        let refused = refused.to_string();
        assert!(refused.contains("create-mesh:mesh3") && refused.contains(&holder.to_string()), "the refusal names the operation and the holder: {refused}");
        assert_eq!(crate::storage::MeshStorage::mesh_named(&meshes, "mesh3").await.unwrap(), Some(theirs), "the refused write wrote nothing: mesh3's row is the holder's");
        assert_eq!(records.meshes.lock().unwrap().get("mesh3"), None, "no id is held for a mesh whose row was refused: no launch of it can carry one");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String, String)> = spans
            .iter()
            .map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("mesh").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("holder").cloned().unwrap_or_default()))
            .collect();
        let row = |mesh: &str, outcome: &str, holder: &str| ("100000000000".to_string(), mesh.to_string(), outcome.to_string(), holder.to_string());
        assert_eq!(said, vec![row("mesh2", "row-written", ""), row("mesh2", "row-held-by-mesh", ""), row("mesh3", "name-held", &holder.to_string())], "{spans:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[2].trace_id, spans[2].attributes);
    }

    /// CONTRACT (node-identity-and-state.md §2.7: "Who mints it. The Build that creates a mesh's first
    /// node-admin mints the mesh id, never reissued"; "The same MeshId writing its own row again, on a
    /// retry or from a replaced primary, is idempotent and accepted"): the accepted
    /// Build that creates mesh2 is planned by its fabric-primary and, after that admin is lost, by its
    /// successor, which holds nothing of its predecessor's memory. Each runs the plan's
    /// `create-mesh:mesh2` over the one mesh store: the first run writes mesh2's row, and the
    /// successor's re-run writes with the same MeshId and finds its own row (`row-held-by-mesh`), so
    /// the id its launches carry is the row's. Must NOT happen: the successor minting another MeshId
    /// and being refused `name-held` by its own mesh's row.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_successor_re_running_create_mesh_of_the_same_build_writes_with_its_mesh_id_and_finds_its_own_row() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let observed = view(&[digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready)]);
        let current = crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap());
        let change = crate::accepted::TopologyChange::CreateMesh { desired: crate::build::MeshDesired::of("mesh2", [("node-admin", 1), ("node", 1)]) };
        let accepted = crate::accepted::compile(&current, &change, &observed).expect("mesh2 is accepted");
        let create_mesh_of = |plan: crate::build::BuildPlan| plan.operations.into_iter().find(|op| matches!(op, BuildOperation::CreateMesh { .. })).expect("the plan creates mesh2");
        let (first_op, successor_op) = (create_mesh_of(crate::accepted::plan(&accepted, &observed, None)), create_mesh_of(crate::accepted::plan(&accepted, &observed, None)));
        let (BuildOperation::CreateMesh { mesh: first_mesh, mesh_id: first_id }, BuildOperation::CreateMesh { mesh: successor_mesh, mesh_id: successor_id }) = (&first_op, &successor_op) else { unreachable!() };
        let minted = &accepted.mesh("mesh2").expect("the accepted topology holds mesh2").mesh_id;
        assert_eq!((first_id, successor_id), (minted, minted), "every plan of the Build carries the MeshId minted when mesh2 was accepted");
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-rerun");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (first, successor) = (Records::default(), Records::default());
        let written = create_mesh(&first, &meshes, "100000000000", first_mesh, first_id).await.expect("the first run writes mesh2's row at its free name");
        assert_eq!(&written, minted, "the first run writes with the Build's MeshId");
        let rerun = create_mesh(&successor, &meshes, "200000000000", successor_mesh, successor_id).await;
        assert_eq!(rerun, Ok(written.clone()), "the successor's re-run of the same Build's create-mesh writes with the same MeshId and finds its own row");
        assert_eq!(successor.meshes.lock().unwrap().get("mesh2"), Some(&written), "the successor's launches of mesh2 carry the id mesh2's row holds");
        assert_eq!(crate::storage::MeshStorage::mesh_named(&meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(written.clone()), "mesh2's row is the first run's");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("mesh_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(said, vec![("100000000000".to_string(), written.to_string(), "row-written".to_string()), ("200000000000".to_string(), written.to_string(), "row-held-by-mesh".to_string())], "{spans:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[1].trace_id, spans[1].attributes);
    }

    /// The accepted Build that creates mesh2 beside mesh1, held by a Build log, and its `create-mesh:mesh2`
    /// operation as the fabric-primary plans it.
    async fn build_creating_mesh2(builds: &crate::build_state::MemoryBuildStateAdapter) -> (crate::build::BuildId, BuildOperation) {
        let observed = view(&[digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready)]);
        let current = crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap());
        let change = crate::accepted::TopologyChange::CreateMesh { desired: crate::build::MeshDesired::of("mesh2", [("node-admin", 1), ("node", 1)]) };
        let accepted = crate::accepted::compile(&current, &change, &observed).expect("mesh2 is accepted");
        let op = crate::accepted::plan(&accepted, &observed, None).operations.into_iter().find(|op| matches!(op, BuildOperation::CreateMesh { .. })).expect("the plan creates mesh2");
        let build_id = crate::build::BuildId::parse("jrr142w4840j").unwrap();
        builds.publish_accepted(&crate::build_state::BuildAccepted { build_id: build_id.clone(), topology: accepted, submitted_change: Some(change), submitted_at_ms: 0 }).await.expect("the Build is accepted");
        (build_id, op)
    }

    /// The receipts of `operation` the Build holds: `(attempt, step, outcome, executor)`.
    async fn receipts_of(builds: &crate::build_state::MemoryBuildStateAdapter, build_id: &crate::build::BuildId, operation: &rdm_core::OperationId) -> Vec<(u32, String, crate::build_state::StepOutcome, Option<rdm_core::framing::Birth>)> {
        builds.read_build(build_id).await.expect("the Build is held").steps.into_iter().filter(|r| r.operation == *operation).map(|r| (r.attempt, r.step, r.outcome, r.executor)).collect()
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: "completed step receipts are read and replayed, never
    /// executed again"; Build carrier: "Completed steps are not rerun"): `create-mesh:mesh2` runs at the
    /// fabric-primary, writes mesh2's row and commits its `Complete` receipt. A successor fabric-primary
    /// that runs the same Build's `create-mesh:mesh2` reads that receipt: it makes no second write, writes
    /// no second receipt, and holds the MeshId the Build carries for its launches of mesh2. One
    /// `rdm.node_admin.mesh.create.via-mesh-row` span (the first run's `row-written`), and a
    /// `rdm.node_admin.mesh.create.via-receipt` span per run: `complete`, then `replayed`. Must NOT happen:
    /// the successor writing again (`row-held-by-mesh`), or a second `Complete` receipt of the step.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_completed_create_mesh_is_receipted_and_a_successor_replays_the_receipt_instead_of_writing_again() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let BuildOperation::CreateMesh { mesh_id, .. } = &op else { unreachable!() };
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-receipt");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (first, successor) = (Records::default(), Records::default());
        create_mesh_step(&first, &meshes, &builds, &fixture_actor(false), "100000000000", &build_id, 1, &op).await.expect("the first run writes mesh2's row at its free name");
        create_mesh_step(&successor, &meshes, &builds, &fixture_actor(true), "200000000000", &build_id, 2, &op).await.expect("the successor's run of the same step");
        assert_eq!(
            receipts_of(&builds, &build_id, &op.key()).await,
            vec![(1, "Complete".to_string(), crate::build_state::StepOutcome::Complete, Some(fixture_actor(false)))],
            "the step is receipted once, by the run that executed it"
        );
        assert_eq!(successor.meshes.lock().unwrap().get("mesh2"), Some(mesh_id), "the successor's launches of mesh2 carry the id the Build carries");
        let writes = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let written: Vec<(String, String)> = writes.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(written, vec![("100000000000".to_string(), "row-written".to_string())], "the successor makes no second write: {writes:?}");
        let steps = capture.named("rdm.node_admin.mesh.create.via-receipt");
        let said: Vec<(String, String)> = steps.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(said, vec![("100000000000".to_string(), "complete".to_string()), ("200000000000".to_string(), "replayed".to_string())], "{steps:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-receipt trace_id={} {:?}", steps[1].trace_id, steps[1].attributes);
    }

    /// CONTRACT (node-rpc-envelope.md, Build carrier: "Settle an idempotent uncertain step by its natural
    /// key"; node-identity-and-state.md §2.7: the same MeshId writing its own row again is idempotent): the
    /// fabric-primary wrote mesh2's row and was lost before it receipted the step, so the Build holds no
    /// receipt of it. The successor's run is uncertain: it writes by the step's natural key, the mesh's
    /// own row answers `row-held-by-mesh`, and the step is receipted `Complete` at the successor's
    /// attempt. Must NOT happen: a refused write, or a step that ends with no receipt.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_create_mesh_whose_receipt_was_never_written_settles_by_its_natural_key_and_is_receipted() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let BuildOperation::CreateMesh { mesh, mesh_id } = &op else { unreachable!() };
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-uncertain");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        // The lost fabric-primary: its row landed, and its receipt did not.
        create_mesh(&Records::default(), &meshes, "100000000000", mesh, mesh_id).await.expect("the lost fabric-primary's row");
        assert_eq!(receipts_of(&builds, &build_id, &op.key()).await, vec![], "the lost run left no receipt");
        let successor = Records::default();
        create_mesh_step(&successor, &meshes, &builds, &fixture_actor(true), "200000000000", &build_id, 2, &op).await.expect("the uncertain step settles by its natural key");
        assert_eq!(receipts_of(&builds, &build_id, &op.key()).await, vec![(2, "Complete".to_string(), crate::build_state::StepOutcome::Complete, Some(fixture_actor(true)))]);
        assert_eq!(successor.meshes.lock().unwrap().get("mesh2"), Some(mesh_id));
        let writes = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let written: Vec<(String, String)> = writes.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(written, vec![("100000000000".to_string(), "row-written".to_string()), ("200000000000".to_string(), "row-held-by-mesh".to_string())], "{writes:?}");
        let steps = capture.named("rdm.node_admin.mesh.create.via-receipt");
        let said: Vec<(String, String)> = steps.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(said, vec![("200000000000".to_string(), "complete".to_string())], "{steps:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-receipt trace_id={} {:?}", steps[0].trace_id, steps[0].attributes);
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: a receipt is read and replayed by the run it belongs
    /// to; `BuildProjection::opened`: a step receipt from the attempt a reopen opened on is the current
    /// run's): `create-mesh:mesh2` completed in the Build's first run, which ended `Converged`; the Build
    /// is reopened (attempt 2, a drift repair) and its plan names `create-mesh:mesh2` again because the
    /// mesh is not yet in the executor's view. The earlier run's receipt is not this run's: the step runs
    /// by its natural key (the mesh's own row answers `row-held-by-mesh`) and is receipted at attempt 2.
    /// Must NOT happen: a receipt of a finished run replayed into the run a reopen began.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_reopened_builds_create_mesh_runs_by_its_natural_key_and_is_receipted_in_the_new_run() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-reopened");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let claim = |attempt: u32, executor: &rdm_core::framing::Birth| crate::build_state::BuildAttemptClaim { executor_mesh: "mesh1".into(), claimed_mesh: None, claimant: None, mesh: None, build_id: build_id.clone(), attempt, executor: executor.clone() };
        assert_eq!(builds.claim_attempt(&claim(1, &fixture_actor(false))).await.unwrap(), crate::build_state::ClaimOutcome::Won);
        create_mesh_step(&Records::default(), &meshes, &builds, &fixture_actor(false), "100000000000", &build_id, 1, &op).await.expect("the first run");
        builds.append_attempt_receipt(&crate::build_state::BuildAttemptReceipt { mesh: None, build_id: build_id.clone(), attempt: 1, outcome: crate::build_state::AttemptOutcome::Converged }).await.unwrap();
        builds
            .open_attempt(&crate::build_state::AttemptOpened { mesh: None, build_id: build_id.clone(), attempt: 2, reason: crate::build_state::AttemptReason::ProvenDrift, action: None, opened_by: rdm_core::NodeId::parse("100000000000").unwrap(), opened_at_ms: 1, context: None })
            .await
            .expect("the Build is reopened");
        assert_eq!(builds.read_build(&build_id).await.unwrap().opened, 2, "attempt 2 begins the Build's current run");
        assert_eq!(builds.claim_attempt(&claim(2, &fixture_actor(true))).await.unwrap(), crate::build_state::ClaimOutcome::Won);
        create_mesh_step(&Records::default(), &meshes, &builds, &fixture_actor(true), "200000000000", &build_id, 2, &op).await.expect("the reopened run");
        let receipts = receipts_of(&builds, &build_id, &op.key()).await;
        assert_eq!(receipts.iter().map(|r| (r.0, r.1.as_str())).collect::<Vec<_>>(), vec![(1, "Complete"), (2, "Complete")], "each run receipts its own step: {receipts:?}");
        let writes: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-mesh-row").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(writes, ["row-written", "row-held-by-mesh"], "the reopened run writes by the natural key");
        let steps: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-receipt").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(steps, ["complete", "complete"]);
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: "Durable step outcomes remain Complete and Failed"):
    /// mesh2's name is held by another mesh's live row, so `create-mesh:mesh2` is refused by name. The
    /// step `ClaimMeshName` is receipted `Failed` with the refusal's reason (the holder named), holds no id
    /// for mesh2, and the operation is never receipted `Complete`. Must NOT happen: a `Complete` receipt beside a refused write.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_create_mesh_refused_by_name_is_receipted_failed_naming_the_holder() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let holder = MeshId::mint();
        crate::storage::write_mesh(&meshes, None, &crate::storage::MeshRecord { mesh_id: holder.clone(), name: "mesh2".into(), state: rdm_core::status::MeshState::Pending }).await.unwrap().unwrap();
        let records = Records::default();
        let refused = create_mesh_step(&records, &meshes, &builds, &fixture_actor(false), "100000000000", &build_id, 1, &op).await.expect_err("mesh2's name is held by another mesh");
        assert!(refused.contains("create-mesh:mesh2") && refused.contains(&holder.to_string()), "{refused}");
        let receipts = receipts_of(&builds, &build_id, &op.key()).await;
        assert_eq!(receipts.len(), 1, "{receipts:?}");
        let (attempt, step, outcome, executor) = &receipts[0];
        assert_eq!((*attempt, step.as_str(), executor.as_ref()), (1, "ClaimMeshName", Some(&fixture_actor(false))));
        assert!(matches!(outcome, crate::build_state::StepOutcome::Failed { reason, .. } if reason.contains(&holder.to_string())), "the Failed receipt names the holder: {outcome:?}");
        assert_eq!(records.meshes.lock().unwrap().get("mesh2"), None, "no id is held for a mesh whose row was refused");
    }

    /// A Build state over `0` whose step receipts are refused: nothing else differs.
    struct RefusesReceipts(Arc<crate::build_state::MemoryBuildStateAdapter>);

    #[async_trait::async_trait]
    impl BuildStateAdapter for RefusesReceipts {
        async fn publish_accepted(&self, a: &crate::build_state::BuildAccepted) -> Result<(), crate::build_state::BuildStateError> {
            self.0.publish_accepted(a).await
        }
        async fn open_attempt(&self, o: &crate::build_state::AttemptOpened) -> Result<(), crate::build_state::BuildStateError> {
            self.0.open_attempt(o).await
        }
        async fn read_build(&self, id: &crate::build::BuildId) -> Result<crate::build_state::BuildProjection, crate::build_state::BuildStateError> {
            self.0.read_build(id).await
        }
        async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
            self.0.current_build().await
        }
        async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, crate::build_state::BuildStateError> {
            self.0.newest_accepted_at_ms().await
        }
        async fn list_active(&self) -> Result<Vec<crate::build_state::BuildProjection>, crate::build_state::BuildStateError> {
            self.0.list_active().await
        }
        async fn claim_attempt(&self, c: &crate::build_state::BuildAttemptClaim) -> Result<crate::build_state::ClaimOutcome, crate::build_state::BuildStateError> {
            self.0.claim_attempt(c).await
        }
        async fn adopt_claim(&self, c: &crate::build_state::BuildAttemptClaim) -> Result<(), crate::build_state::BuildStateError> {
            self.0.adopt_claim(c).await
        }
        async fn append_step_receipt(&self, r: &crate::build_state::BuildStepReceipt) -> Result<(), crate::build_state::BuildStateError> {
            Err(crate::build_state::BuildStateError::Io(format!("the journal refused the {} receipt of {:?}", r.step, r.operation)))
        }
        async fn append_attempt_receipt(&self, r: &crate::build_state::BuildAttemptReceipt) -> Result<(), crate::build_state::BuildStateError> {
            self.0.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<crate::build_state::BuildFact>, crate::build_state::BuildStateError> {
            self.0.facts().await
        }
        async fn committed_facts(&self, build: Option<&crate::build::BuildId>) -> Result<Vec<crate::build_state::BuildFact>, crate::build_state::BuildStateError> {
            self.0.committed_facts(build).await
        }
        async fn forget(&self, id: &crate::build::BuildId) -> Result<(), crate::build_state::BuildStateError> {
            self.0.forget(id).await
        }
        async fn recovery_of(&self, p: &crate::model::PathName, n: &NodeId, i: &IncarnationId) -> Result<Option<(crate::build::BuildId, Option<String>, u32, rdm_core::OperationId)>, crate::build_state::BuildStateError> {
            self.0.recovery_of(p, n, i).await
        }
        async fn mesh_deleted_of(&self, n: &NodeId, i: &IncarnationId) -> Result<Option<(crate::build::BuildId, u32, rdm_core::OperationId)>, crate::build_state::BuildStateError> {
            self.0.mesh_deleted_of(n, i).await
        }
        async fn exits_owed(&self, id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, crate::build_state::BuildStateError> {
            self.0.exits_owed(id, mesh).await
        }
    }

    /// A mesh store that does not answer.
    struct UnansweredMeshes;

    #[async_trait::async_trait]
    impl crate::storage::MeshStorage for UnansweredMeshes {
        async fn mesh(&self, _: &crate::model::MeshId) -> Result<Option<crate::storage::MeshRecord>, crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: "mesh-records".into(), reason: "the disk did not answer".into() })
        }
        async fn mesh_named(&self, _: &str) -> Result<Option<crate::storage::MeshRecord>, crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: "mesh-records".into(), reason: "the disk did not answer".into() })
        }
        async fn put_mesh(&self, _: &crate::storage::MeshRecord) -> Result<(), crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: "mesh-records/mesh2.json".into(), reason: "the disk did not answer".into() })
        }
        async fn meshes(&self) -> Result<Vec<crate::storage::MeshRecord>, crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: "mesh-records".into(), reason: "the disk did not answer".into() })
        }
        async fn take_in_flight(&self, mesh: &str, _: &crate::mesh_claims::InFlight) -> Result<crate::mesh_claims::Taken, crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: format!("meshes/{mesh}/claims/in-flight.json"), reason: "the disk did not answer".into() })
        }
        async fn release_in_flight(&self, mesh: &str, _: &crate::mesh_claims::InFlight) -> Result<(), crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: format!("meshes/{mesh}/claims/in-flight.json"), reason: "the disk did not answer".into() })
        }
        async fn in_flight(&self, mesh: &str) -> Result<Option<crate::mesh_claims::InFlight>, crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: format!("meshes/{mesh}/claims/in-flight.json"), reason: "the disk did not answer".into() })
        }
    }


    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: a step is committed by its receipt; CLAUDE.md: an
    /// error state names what failed and where, distinct facts distinct arms): `create-mesh:mesh2` fails
    /// by its own name when (1) the Build cannot be read for its receipts, so it writes nothing, (2) the
    /// mesh store does not answer the write, so it receipts nothing, and (3) the Build state refuses the
    /// `Complete` receipt after the row is written: the step is uncommitted, and the next run settles it by its
    /// natural key (`row-held-by-mesh`) and receipts it. One
    /// `rdm.node_admin.mesh.create.via-receipt` span per run, `outcome` `build-unreadable`,
    /// `store-unanswered`, `receipt-refused`, then `complete`. Must NOT happen: a `Complete` receipt of a
    /// step that was not decided, or a step reported done whose receipt the Build state refused.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_create_mesh_that_cannot_read_decide_or_commit_fails_by_name_and_the_next_run_settles_it() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let memory = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        let (build_id, op) = build_creating_mesh2(&memory).await;
        let BuildOperation::CreateMesh { mesh_id, .. } = &op else { unreachable!() };
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-cannot");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        // (1) A Build the log never accepted: nothing is written.
        let unknown = crate::build::BuildId::parse("kpnnk1nrvac1").unwrap();
        let unread = create_mesh_step(&Records::default(), &meshes, &*memory, &fixture_actor(false), "100000000000", &unknown, 1, &op).await.expect_err("the Build is not held");
        assert!(unread.contains("could not be read for its receipts") && unread.contains("kpnnk1nrvac1"), "{unread}");
        assert_eq!(crate::storage::MeshStorage::mesh_named(&meshes, "mesh2").await.unwrap(), None, "nothing was written beside a Build that could not be read");
        // (2) The mesh store does not answer: nothing is receipted.
        let unanswered = create_mesh_step(&Records::default(), &UnansweredMeshes, &*memory, &fixture_actor(false), "100000000000", &build_id, 1, &op).await.expect_err("the mesh store does not answer");
        assert!(unanswered.contains("did not answer") && unanswered.contains("mesh2"), "{unanswered}");
        assert_eq!(receipts_of(&memory, &build_id, &op.key()).await, vec![], "an undecided step is not receipted");
        // (3) The row is written and the receipt is refused: the step is uncommitted.
        let refused = create_mesh_step(&Records::default(), &meshes, &RefusesReceipts(memory.clone()), &fixture_actor(false), "100000000000", &build_id, 1, &op).await.expect_err("the Build state refuses the receipt");
        assert!(refused.contains(&format!("Complete receipt of {:?}", op.key())) && refused.contains("was not written") && refused.contains("journal refused"), "{refused}");
        assert_eq!(crate::storage::MeshStorage::mesh_named(&meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(mesh_id.clone()), "the row stands");
        assert_eq!(receipts_of(&memory, &build_id, &op.key()).await, vec![], "the refused receipt is not in the log");
        // The next run settles the uncertain step by its natural key.
        create_mesh_step(&Records::default(), &meshes, &*memory, &fixture_actor(true), "200000000000", &build_id, 2, &op).await.expect("the next run settles the step");
        assert_eq!(receipts_of(&memory, &build_id, &op.key()).await.len(), 1);
        let outcomes: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-receipt").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(outcomes, ["build-unreadable", "store-unanswered", "receipt-refused", "complete"]);
        let writes: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-mesh-row").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(writes, ["row-refused", "row-written", "row-held-by-mesh"], "the write made beside the unanswered store is refused by name; the settled run finds the mesh's own row");
    }

    /// CONTRACT (node-identity-and-state.md §2.7: "The fabric-primary writes the fabric's row at Day 0,
    /// idempotent for the same FabricId"; a different FabricId's identity at the key is held, and nothing
    /// else of the fabric is written beside it): `fabric/` holds another Fabric's identity (a root an embedding app
    /// handed in). This fabric-primary's sweeps write the fabric's row, are refused naming the holder,
    /// and write nothing of the fabric: the held identity stands, and the write is made once, one
    /// `rdm.node_admin.fabric.create.via-fabric-row` span `name-held` naming the holder. Must NOT happen:
    /// a row written into another Fabric's `fabric/` beside a refused write.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_fabric_primary_refused_by_another_fabrics_identity_writes_nothing_of_the_fabric() {
        let storage: Arc<dyn crate::fabric_storage::FabricStorage> = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        let (holder, ours) = (crate::fabric_storage::FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }, crate::fabric_storage::FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() });
        let birth = |revision: u64| rdm_core::Primary { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision };
        let (h_birth, o_birth) = (birth(1), birth(2));
        let capture = crate::span_capture::Capture::new("i143-3089-fabric-name-held");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let h_primaries = rdm_transport::topology::PrimaryView::default();
        h_primaries.take(rdm_core::PrimaryRole::FabricPrimary, &h_birth);
        let mut hw = FabricRowsWriter::new((h_birth.node_id.clone(), h_birth.incarnation.clone()), holder.clone(), storage.clone());
        hw.sweep(&h_primaries).await;
        assert_eq!(storage.fabric().await.unwrap().map(|r| r.identity()), Some(holder.clone()), "the holder's fabric-primary writes its identity at the free key");
        let o_primaries = rdm_transport::topology::PrimaryView::default();
        o_primaries.take(rdm_core::PrimaryRole::FabricPrimary, &o_birth);
        let mut ow = FabricRowsWriter::new((o_birth.node_id.clone(), o_birth.incarnation.clone()), ours.clone(), storage.clone());
        for _sweep in 0..2 {
            ow.sweep(&o_primaries).await;
        }
        assert_eq!(storage.fabric().await.unwrap().map(|f| f.fabric_id), Some(holder.fabric_id.clone()), "the held identity stands");
        let spans = capture.named("rdm.node_admin.fabric.create.via-fabric-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("fabric_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("holder").cloned().unwrap_or_default())).collect();
        assert_eq!(
            said,
            vec![(holder.fabric_id.to_string(), "row-written".to_string(), String::new()), (ours.fabric_id.to_string(), "name-held".to_string(), holder.fabric_id.to_string())],
            "one write per writer, the refusal naming the holder: {spans:?}"
        );
        eprintln!("span rdm.node_admin.fabric.create.via-fabric-row trace_id={} {:?}", spans[1].trace_id, spans[1].attributes);
    }

    /// CONTRACT (R-MN1: a mesh's name is released only by its removal, the row deleted, and "a mesh created
    /// again under the same name is a new mesh with a new id"; a mesh's write treats a
    /// Dead mesh row as released; states.md: deleted is final, a mesh id is never reissued): mesh A's
    /// primary writes mesh2's row; the removal writes A's row deleted (`deleted-written`, a repeat
    /// `already-deleted`); mesh2 created again under a new MeshId has its primary write its row over the
    /// Dead row, the span naming the deleted mesh it took it over, and its member's address is written on its
    /// record (the removed mesh's admin's record `Deleted` by its node.delete). A's own
    /// later tenure writes nothing live again (`mesh-deleted`). Must NOT happen: the recreated mesh
    /// refused by the removed mesh's row, or the removed mesh's id written live again.
    // @feature: mesh-lifecycle
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_mesh_removed_and_created_again_writes_its_record_over_the_dead_row() {
        let d = crate::test_support::tempdir("mesh-row-dead");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let store = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(CountingNodes(Arc::new(crate::storage::FileNodesStorage::open(&data, &root, "mesh2", &crate::store_log::LogWriter::mint()).await.unwrap()), Default::default())) }
        };
        let (a_nodes, b_nodes, created_rows) = (store("a.node-admin").await, store("b.node-admin").await, store("created").await);
        let meshes: Arc<dyn crate::storage::MeshStorage> = Arc::new(crate::storage::FileMeshStorage::open(&root, &crate::store_log::LogWriter::mint()).await.unwrap());
        let (removed, recreated) = (mesh_id("mesh2"), MeshId::mint());
        let (a_admin, b_admin) = (digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready));
        created_record(&*created_rows, &a_admin).await;
        let capture = crate::span_capture::Capture::new("i143-3089-mesh-name-over-dead");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        // Mesh A's primary holds one copy of its mesh's record: its writes and its removal start from it.
        let caches = crate::record_cache::Caches::default();
        let mut aw = MeshRowsWriter::new("mesh2.node-admin.1", "mesh2", removed.clone(), a_nodes.clone(), meshes.clone(), caches.clone());
        aw.sweep(std::slice::from_ref(&a_admin), &writing(&["mesh2"]), |_| false).await;
        assert_eq!(crate::mesh_delete::mesh_row_deleted(&*meshes, &caches, "mesh2", &removed).await, Ok("deleted-written"), "the removal writes mesh A's row deleted");
        assert_eq!(crate::mesh_delete::mesh_row_deleted(&*meshes, &caches, "mesh2", &removed).await, Ok("already-deleted"), "a repeat of the removal writes nothing");
        // Mesh A's primary takes a new tenure: its id is deleted and is never written live again.
        let records_before = a_nodes.1.load(std::sync::atomic::Ordering::SeqCst);
        aw.sweep(std::slice::from_ref(&a_admin), &writing(&[]), |_| false).await;
        aw.sweep(std::slice::from_ref(&a_admin), &writing(&["mesh2"]), |_| false).await;
        assert_eq!(crate::storage::MeshStorage::mesh(&*meshes, &removed).await.unwrap(), Some(crate::storage::MeshRecord { mesh_id: removed.clone(), name: "mesh2".into(), state: rdm_core::status::MeshState::Deleted }), "the removed mesh's later tenure writes nothing live");
        assert_eq!(crate::storage::MeshStorage::mesh_named(&*meshes, "mesh2").await.unwrap(), None, "its deleted record released the name");
        assert_eq!(a_nodes.1.load(std::sync::atomic::Ordering::SeqCst), records_before, "nor any record");
        // The removed mesh's admin's node.delete wrote its record Deleted; the recreated mesh's admin wrote its row at the name.
        crate::node_rows::write_deleted(&*created_rows, &crate::node_rows::WrittenRows::default(), &a_admin.node.name, &a_admin.node.node_id).await.ok().expect("the removed mesh's admin's record is Deleted");
        created_record(&*created_rows, &b_admin).await;
        // Each node writes its own part, its address in it (Q333).
        own_part(&*created_rows, &a_admin).await;
        own_part(&*created_rows, &b_admin).await;
        let mut bw = MeshRowsWriter::new("mesh2.node-admin.1", "mesh2", recreated.clone(), b_nodes.clone(), meshes.clone(), Default::default());
        bw.sweep(std::slice::from_ref(&b_admin), &writing(&["mesh2"]), |_| false).await;
        let row = crate::storage::MeshStorage::mesh_named(&*meshes, "mesh2").await.unwrap();
        assert_eq!(row, Some(crate::storage::MeshRecord { mesh_id: recreated.clone(), name: "mesh2".into(), state: rdm_core::status::MeshState::Pending }), "the recreated mesh's row holds mesh2 under its new MeshId");
        let births: Vec<NodeId> = rows_writer::recorded_births(&*meshes, &*b_nodes, "mesh2").await.unwrap().into_iter().map(|c| c.node_id).collect();
        assert_eq!(births, vec![b_admin.node.node_id.clone()], "the recreated mesh's primary writes its member's address once its mesh's row is written, and the removed mesh's Deleted record at the (path, name) names no node");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("mesh_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("over").cloned().unwrap_or_default())).collect();
        assert_eq!(
            said,
            vec![
                (removed.to_string(), "row-written".to_string(), String::new()),
                (removed.to_string(), "mesh-deleted".to_string(), String::new()),
                (recreated.to_string(), "row-written".to_string(), String::new()),
            ],
            "the deleted mesh writes nothing again, and the recreated mesh writes its row at the name its deleted record released: {spans:?}"
        );
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[2].trace_id, spans[2].attributes);
    }

    fn view(digests: &[MeshDigest]) -> Topology {
        let digest_view = DigestView::default();
        for d in digests {
            digest_view.record(d.clone());
        }
        project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default())
    }

    fn with_id(mut d: MeshDigest, id: &str) -> MeshDigest {
        d.node.node_id = NodeId::parse(id).unwrap();
        d
    }

    /// A provider whose every inspection answers `status`, in the control domain of the runtime it
    /// is asked about; it counts the inspections.
    struct Answers {
        domain: String,
        status: crate::deployment::provider::DeploymentStatus,
        inspected: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for Answers {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            self.domain.clone()
        }
        async fn spawn(&self, spec: &crate::deployment::provider::ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<crate::deployment::provider::DeploymentHandle, crate::deployment::provider::DeployError> {
            Err(crate::deployment::provider::DeployError::Spawn { node: spec.node.clone(), reason: "this provider only answers inspections".into() })
        }
        async fn terminate(&self, _: &crate::deployment::provider::DeploymentHandle, _: crate::deployment::provider::TerminationMode, _deadline: crate::deployment::deadline::Deadline) -> Result<(), crate::deployment::provider::DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &crate::deployment::provider::DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> crate::deployment::provider::DeploymentStatus {
            self.inspected.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.status.clone()
        }
        async fn signal_stop(&self, _: &crate::deployment::provider::DeploymentHandle, _deadline: crate::deployment::deadline::Deadline) -> Result<(), crate::deployment::provider::DeployError> {
            Ok(())
        }
        async fn find(&self, _: &crate::deployment::provider::ResolvedNodeLaunch, _deadline: crate::deployment::deadline::Deadline) -> Result<crate::deployment::provider::Found, crate::deployment::provider::DeployError> {
            Ok(crate::deployment::provider::Found::NeverLaunched)
        }
    }

    /// A nodes store whose reads of runtime rows (the point read and the whole set) are refused; every
    /// other row is the memory store's.
    #[derive(Default)]
    struct RuntimeRowUnreadable(crate::storage::MemoryNodesStorage);

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for RuntimeRowUnreadable {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: "runtimes".into(), reason: "Input/output error (os error 5)".into() })
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Unrecognised { file: format!("runtimes/{node_id}--{}.json", incarnation.as_str()), reason: "format runtime/9 is not one this build reads".into() })
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.nodes(mesh).await
        }
        async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_properties(record).await
        }
        async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, crate::storage_error::StorageError> {
            self.0.properties(node_id).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_node(row).await
        }
        async fn records_at(&self, name: &crate::model::PathName) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.0.records_at(name).await
        }
    }

    /// A nodes.storage over memory that counts every list of the runtime rows and answers the point
    /// read of one birth's row as `point` says: the row held (`None`), or the store's refusal.
    #[derive(Default)]
    struct CountsRuntimeLists {
        inner: crate::storage::MemoryNodesStorage,
        lists: std::sync::atomic::AtomicUsize,
        point_refused: bool,
    }

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for CountsRuntimeLists {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            if self.point_refused {
                return Err(crate::storage_error::StorageError::Io { file: format!("runtimes/{node_id}--{}.json", incarnation.as_str()), reason: "Input/output error (os error 5)".into() });
            }
            self.inner.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.nodes(mesh).await
        }
        async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_properties(record).await
        }
        async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, crate::storage_error::StorageError> {
            self.inner.properties(node_id).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node(row).await
        }
        async fn records_at(&self, name: &crate::model::PathName) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.records_at(name).await
        }
    }

    /// CONTRACT (CLAUDE.md read rule: "Every lookup is a point read through a declared index"; error
    /// rule: "never written" and "could not be read" are different errors, distinct arms and spans):
    /// the fabric-primary proves a lost executor `mesh2.node-admin.1` deleted whose digest carries no
    /// runtime, so the proof reads that exact birth's durable runtime row by its key. Held and
    /// exited: the departure is proven. Never written: refused on
    /// `rdm.node_admin.node.reject.via-no-runtime-row`. Unreadable: refused on
    /// `rdm.node_admin.node.reject.via-runtime-row-unreadable` naming the store's cause. Must NOT
    /// happen: a list of every runtime row searched for the one birth, or the two refusals as one.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_executor_inspection_reads_its_births_runtime_row_and_names_unwritten_apart_from_unreadable() {
        use crate::build_drive::ExecutorInspection as _;
        use crate::storage::NodesStorage as _;
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-executor")).unwrap();
        let executor = digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready);
        let digest_view = DigestView::default();
        for d in [executor.clone(), digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready)] {
            digest_view.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        let row = crate::storage::RuntimeRow { r#type: executor.node.r#type.clone(), mesh: executor.node.mesh.clone(), node_id: executor.node.node_id.clone(), name: executor.node.name.clone(), incarnation_id: executor.node.incarnation.clone(), runtime: fact.clone(), data_dir: None };
        let departure = |nodes: Arc<CountsRuntimeLists>| ProviderDeparture {
            me: rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(),
            refresh: Arc::new(|| Box::pin(async {})),
            digest_view: digest_view.clone(),
            provider: Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() }),
            nodes,
        };
        let executor_name = rdm_core::framing::Birth { node_id: executor.node.node_id.clone(), incarnation: executor.node.incarnation.clone() };
        let birth = (executor.node.node_id.to_string(), executor.node.incarnation.to_string());
        let capture = crate::span_capture::Capture::new("i143-3051-executor-exited-verdict-point-read");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);

        let held = Arc::new(CountsRuntimeLists::default());
        held.put_runtime(&row).await.unwrap();
        departure(held.clone()).exited_or_os_stopped(&executor_name).await.expect("the held row's runtime is proven exited");
        let unwritten = Arc::new(CountsRuntimeLists::default());
        let never = departure(unwritten.clone()).exited_or_os_stopped(&executor_name).await.unwrap_err();
        let refused = Arc::new(CountsRuntimeLists { point_refused: true, ..Default::default() });
        let unread = departure(refused.clone()).exited_or_os_stopped(&executor_name).await.unwrap_err();

        let lists = |s: &CountsRuntimeLists| s.lists.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!((lists(&held), lists(&unwritten), lists(&refused)), (0, 0, 0), "the proof reads one row by its key, never the list of every row");
        assert!(never.contains("no durable runtime row") && !never.contains("could not be read"), "never written is its own refusal: {never}");
        assert!(unread.contains("could not be read") && unread.contains("Input/output error (os error 5)"), "unreadable names the store's cause: {unread}");
        let attrs = |name: &str| {
            let spans = capture.named(name);
            assert_eq!(spans.len(), 1, "{name} once: {spans:?}");
            eprintln!("span {name} trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
            spans[0].attributes.clone()
        };
        let attr = |a: &std::collections::BTreeMap<String, String>, k: &str| a.get(k).cloned().unwrap_or_default();
        let no_row = attrs("rdm.node_admin.node.reject.via-no-runtime-row");
        assert_eq!((attr(&no_row, "reader_path"), attr(&no_row, "reader_name"), attr(&no_row, "node_id"), attr(&no_row, "incarnation_id"), attr(&no_row, "store")), ("mesh1.node-admin".to_string(), "1".to_string(), birth.0.clone(), birth.1.clone(), "nodes.storage".to_string()), "{no_row:?}");
        let unreadable = attrs("rdm.node_admin.node.reject.via-runtime-row-unreadable");
        assert_eq!((attr(&unreadable, "reader_path"), attr(&unreadable, "reader_name"), attr(&unreadable, "node_id"), attr(&unreadable, "incarnation_id"), attr(&unreadable, "store")), ("mesh1.node-admin".to_string(), "1".to_string(), birth.0, birth.1, "nodes.storage".to_string()), "{unreadable:?}");
        assert!(attr(&unreadable, "error").contains("Input/output error (os error 5)"), "{unreadable:?}");
    }

    /// CONTRACT (build.md: "when the executor is lost with the attempt unfinished, after the provider's
    /// inspection of the executor's exact runtime answers `exited` or `os-stopped`: `running`,
    /// `unproven` and silence never let another claimant take it"; Q239 A narrowed): the fabric-primary
    /// proves a lost executor `mesh2.node-admin.1` exited or os-stopped by its exact runtime as the provider reads it.
    /// Exited and os-stopped: proven, and the view is projected again for the next attempt. Running
    /// and unproven (the provider's `Unknown`): refused naming the verdict, nothing projected. Must NOT
    /// happen: an os-stopped executor's attempt held, or a running or unproven one's taken.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_executor_inspection_answers_exited_or_os_stopped_and_never_running_or_unproven() {
        use crate::build_drive::ExecutorInspection as _;
        use crate::deployment::provider::DeploymentStatus;
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-verdicts")).unwrap();
        let mut executor = digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready);
        executor.node.runtime = Some(fact.clone());
        let digest_view = DigestView::default();
        for d in [executor.clone(), digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready)] {
            digest_view.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        let executor_name = rdm_core::framing::Birth { node_id: executor.node.node_id.clone(), incarnation: executor.node.incarnation.clone() };
        for (status, verdict, exited) in [
            (DeploymentStatus::Exited { code: Some(137) }, "exited", true),
            (DeploymentStatus::OsStopped, "os-stopped", true),
            (DeploymentStatus::Running, "running", false),
            (DeploymentStatus::Unknown, "unavailable", false),
        ] {
            let refreshed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status, inspected: Default::default() });
            let departure = ProviderDeparture {
                me: rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(),
                refresh: {
                    let refreshed = refreshed.clone();
                    Arc::new(move || {
                        refreshed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Box::pin(async {})
                    })
                },
                digest_view: digest_view.clone(),
                provider: provider.clone(),
                nodes: Arc::new(crate::storage::MemoryNodesStorage::default()),
            };
            let answer = departure.exited_or_os_stopped(&executor_name).await;
            assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 1, "{verdict}: the exact runtime is inspected once");
            match (exited, &answer) {
                (true, Ok(())) => assert_eq!(refreshed.load(std::sync::atomic::Ordering::SeqCst), 1, "{verdict}: the view is projected again for the next attempt"),
                (false, Err(why)) => {
                    assert!(why.contains(verdict), "{verdict}: the refusal names the verdict: {why}");
                    assert_eq!(refreshed.load(std::sync::atomic::Ordering::SeqCst), 0, "{verdict}: nothing is projected for an attempt that is held");
                }
                _ => panic!("{verdict}: the departure is {} but the proof answered {answer:?}", if exited { "proven" } else { "not proven" }),
            }
        }
    }

    /// The mesh primary `mesh2.node-admin.1`'s recovery of its own mesh's births, over `digest_view`, `provider`
    /// and `nodes_storage`, and the TRUE OFFLINE member `mesh2.node.1` of `view`.
    fn recovery_of(view: &Topology, digest_view: DigestView, provider: Arc<dyn DeploymentProvider>, nodes_storage: Arc<dyn crate::storage::NodesStorage>) -> (RecoverySubmitter, Arc<dyn crate::storage::NodesStorage>, Node) {
        let member = view.node(&rdm_core::PathName::new("mesh2.node", "1").unwrap()).cloned().expect("the member is in the view");
        let recovery = RecoverySubmitter {
            me: rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(),
            me_id: view.node(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).unwrap().node_id.clone(),
            topology: Arc::new(RwLock::new(view.clone())),
            digest_view,
            provider,
            nodes_storage: nodes_storage.clone(),
            written: Default::default(),
            door: Default::default(),
            client: None,
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
            data_root: PathBuf::new(),
            gone_dirs: Arc::default(),
            resubmits: Arc::default(),
            investigated: crate::node_investigation::NodeInvestigatedWriter::in_memory(&view.fabric.id, &view.node(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).unwrap().node_id),
        };
        (recovery, nodes_storage, member)
    }

    /// What the mesh primary's two ticks over one TRUE OFFLINE birth left: the verdict, the
    /// inspection spans and the submit spans.
    async fn two_ticks(recovery: &RecoverySubmitter, member: &Node, cell: &str) -> (Inspected, Vec<crate::span_capture::Captured>, Vec<crate::span_capture::Captured>) {
        let capture = crate::span_capture::Capture::new(cell);
        let outcome = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            let first = recovery.recover(member).await;
            let second = recovery.recover(member).await;
            assert_eq!(first, second, "the same birth, the same verdict on the next tick");
            first
        };
        (outcome, capture.named("rdm.node_admin.node.resolve.via-provider-inspection"), capture.named("rdm.node_admin.build.update.via-node-recovery"))
    }

    /// CONTRACT (node-recovery.md step 3: "Any other verdict (`running`, `os-stopped`, `exited`,
    /// `unproven`): the recovery below, at once. Nothing holds, nothing is inspected again, and nothing
    /// waits for a proof of termination"; node-runtime-inspect.md "Spans"): the mesh primary asks the
    /// provider about a TRUE OFFLINE member's exact runtime, and whatever the provider reads, the
    /// verdict is spanned with its page name and the recovery is submitted on that same tick, keyed on
    /// the exact birth, and again on the next tick while it is still owed. Must NOT happen: a member
    /// held because its runtime runs, is suspended or cannot be told; a submit only on an exit.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_true_offline_members_recovery_opens_at_once_on_whatever_the_provider_reads() {
        use crate::deployment::provider::DeploymentStatus;
        for (status, verdict) in [(DeploymentStatus::Running, "running"), (DeploymentStatus::OsStopped, "os-stopped"), (DeploymentStatus::Exited { code: Some(137) }, "exited"), (DeploymentStatus::Unknown, "unavailable")] {
            let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60(&format!("dep-{verdict}"))).unwrap();
            let mut member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
            member.node.runtime = Some(fact.clone());
            let digest_view = DigestView::default();
            for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), member.clone()] {
                digest_view.record(d);
            }
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: status.clone(), inspected: Default::default() });
            let (recovery, _, node) = recovery_of(&view, digest_view, provider.clone(), Arc::new(crate::storage::MemoryNodesStorage::default()));
            let (_, inspections, submits) = two_ticks(&recovery, &node, &format!("i143-l5-recovery-at-once-{verdict}")).await;
            assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 2, "{verdict}: the provider is asked on each tick");
            let verdicts: Vec<String> = inspections.iter().map(|c| c.attributes.get("verdict").cloned().unwrap_or_default()).collect();
            assert_eq!(verdicts, [verdict, verdict], "{verdict}: each tick's verdict is spanned by its page name: {inspections:?}");
            assert_eq!(submits.len(), 2, "{verdict}: the recovery is submitted on each tick it is owed, whatever the verdict: {submits:?}");
            let a = &submits[0].attributes;
            let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
            assert_eq!((attr("node_path"), attr("node_name"), attr("node_id"), attr("incarnation_id"), attr("verdict")), ("mesh2.node".to_string(), "1".to_string(), node.node_id.to_string(), member.node.incarnation.to_string(), verdict.to_string()), "{verdict}: keyed on the exact birth: {a:?}");
            eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {a:?}", submits[0].trace_id);
        }
    }

    /// CONTRACT (node-recovery.md "Execution": "While the mesh is cut from the fabric-primary, its
    /// recoveries fail by name and run after the heal"; "Failure and receipts": the mesh-primary submits
    /// again; node-rpc.md §43, the one backoff rule): a birth whose submit ended without the recovery is
    /// submitted again after the gossip interval, each later time after twice the delay before, to the
    /// staleness floor; a recovery that ran, or a birth the tick no longer owes, is due at once. Over ten
    /// seconds of submits that fail every time, ticked every gossip interval, the birth is inspected and
    /// submitted six times, not twenty. Must NOT happen: a failing submit made again on every tick, with
    /// a provider inspection each.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_recovery_submit_that_ends_without_the_recovery_is_made_again_under_the_one_backoff_rule() {
        use crate::deployment::provider::DeploymentStatus;
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-backoff")).unwrap();
        let mut member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
        member.node.runtime = Some(fact.clone());
        let digest_view = DigestView::default();
        for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), member.clone()] {
            digest_view.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: DeploymentStatus::Running, inspected: Default::default() });
        let (recovery, _, node) = recovery_of(&view, digest_view, provider.clone(), Arc::new(crate::storage::MemoryNodesStorage::default()));
        let birth = (node.node_id.clone(), node.incarnation_id.clone().expect("the member's birth"));

        // The submitter's real tick: this rig builds no Build door, so the submit ends without the
        // recovery, and the birth is not due again at once. The submit's span carries its backoff.
        let capture = crate::span_capture::Capture::new("i143-3214-recovery-backoff");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        assert!(recovery.submit_due(&birth, std::time::Instant::now()), "a birth with no failed submit is due at once");
        recovery.recover(&node).await;
        assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 1, "the tick inspected the birth once");
        let held = recovery.resubmits.lock().unwrap().get(&birth).copied().expect("a submit that ended without the recovery is owed again later");
        assert_eq!(held.failures, 1, "{held:?}");
        assert!(!recovery.submit_due(&birth, std::time::Instant::now()), "not due again on the next instant: {held:?}");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
        assert_eq!(submits.len(), 1, "one submit span: {submits:?}");
        let backoff: u64 = submits[0].attributes.get("backoff_ms").and_then(|v| v.parse().ok()).unwrap_or_else(|| panic!("the failed submit's span records its backoff: {:?}", submits[0].attributes));
        let gossip = rdm_core::cadence::gossip_interval().as_millis() as u64;
        assert!(backoff >= gossip * 3 / 4 && backoff <= (gossip * 5 / 4).min(rdm_core::cadence::staleness_floor().as_millis() as u64), "the first backoff is the gossip interval ({gossip} ms, ±25 %): {backoff} ms");
        eprintln!("span {} trace_id={} {:?}", submits[0].name, submits[0].trace_id, submits[0].attributes);

        // The schedule, on the gate's cadences without jitter: 500 ms, doubling, to the 3 s floor.
        let pacing = rdm_transport::delivery::Pacing { cadence: std::time::Duration::from_millis(500), floor: std::time::Duration::from_secs(3), jitter: false };
        recovery.forget_resubmits(|_| false);
        assert!(recovery.submit_due(&birth, std::time::Instant::now()), "a birth the tick no longer owes keeps no backoff");
        let t0 = std::time::Instant::now();
        let (mut submits, mut delays) = (0u32, Vec::new());
        for tick in 0..20u32 {
            let now = t0 + pacing.cadence * tick;
            if recovery.submit_due(&birth, now) {
                submits += 1;
                delays.push(recovery.resubmit_after(&birth, false, &pacing, now).expect("a failed submit is owed again").as_millis() as u64);
            }
        }
        eprintln!("ten seconds of failing submits, one tick per gossip interval: {submits} submits, delays {delays:?} ms (every tick: 20)");
        assert_eq!(delays, [500, 1_000, 2_000, 3_000, 3_000, 3_000], "the gossip interval, doubling, capped at the staleness floor");
        assert_eq!(submits, 6, "six submits in ten seconds, not one per tick");
        assert_eq!(recovery.resubmit_after(&birth, true, &pacing, t0), None, "a recovery that ran owes nothing more");
        assert!(recovery.submit_due(&birth, t0), "and its birth, unheard again, is due at once");
    }

    /// CONTRACT (node-recovery.md "Detection and the verdict", Q209: "After a retryable recovery-submit
    /// failure, that exact birth becomes eligible again after a backoff doubling from the gossip interval
    /// to the staleness floor"; the ruling's bounds: an accepted, unfinished recovery is reattached or
    /// continued, never submitted as competing work; successful recovery, the birth returning, or its
    /// replacement clears the backoff): the tick's one gate, `take_eligible`. A birth whose submit is in
    /// flight is never taken again, eligible or not, until its follower ends; a failed submit makes only
    /// that exact birth wait, never another birth or the next birth at its path and name; whatever the run of
    /// failures, the birth is eligible again within one staleness floor of its last failure, so a mesh
    /// whose connectivity returns submits within the floor; a recovery that ran, or a birth the tick
    /// forgets (returned, replaced), is eligible at once. Must NOT happen: a second submit of a birth
    /// whose submit is in flight, a backoff past the floor, or a backoff kept by a returned or replaced birth.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_ticks_gate_takes_a_birth_once_while_in_flight_and_again_within_the_floor_after_a_failure() {
        let digest_view = DigestView::default();
        for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready)] {
            digest_view.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        let provider = Arc::new(Answers { domain: String::new(), status: crate::deployment::provider::DeploymentStatus::Running, inspected: Default::default() });
        let (recovery, _, node) = recovery_of(&view, digest_view, provider, Arc::new(crate::storage::MemoryNodesStorage::default()));
        let birth = (node.node_id.clone(), node.incarnation_id.clone().expect("the node's birth"));
        let replacement = (NodeId::mint(), IncarnationId::mint());
        let in_flight: Mutex<HashSet<(NodeId, IncarnationId)>> = Mutex::default();
        let pacing = rdm_transport::delivery::Pacing { cadence: std::time::Duration::from_millis(500), floor: std::time::Duration::from_secs(3), jitter: true };
        let t0 = std::time::Instant::now();

        // In flight: taken once, never again while its follower runs, on any tick.
        assert!(recovery.take_eligible(&birth, t0, &in_flight), "a birth with nothing in flight and no backoff is taken");
        for tick in 1..40u32 {
            assert!(!recovery.take_eligible(&birth, t0 + pacing.cadence * tick, &in_flight), "tick {tick}: an accepted, unfinished submit is followed, never submitted again beside it");
        }
        // Its follower ends without the recovery: in flight no more, eligible after its backoff only.
        in_flight.lock().unwrap().remove(&birth);
        let first = recovery.resubmit_after(&birth, false, &pacing, t0).expect("a failed submit owes a backoff");
        assert!(!recovery.take_eligible(&birth, t0, &in_flight), "not eligible inside its backoff ({first:?})");
        assert!(recovery.take_eligible(&replacement, t0, &in_flight), "another birth, the next at its (path, name), does not wait on it");
        assert!(recovery.take_eligible(&birth, t0 + first, &in_flight), "eligible once its backoff has passed");
        in_flight.lock().unwrap().remove(&birth);

        // However long the failures run, the next submit is within one floor of the last: connectivity
        // returning is answered within the floor plus the run itself.
        let mut last = t0;
        for failure in 2..30u32 {
            last += std::time::Duration::from_millis(137 * u64::from(failure));
            let after = recovery.resubmit_after(&birth, false, &pacing, last).expect("a failed submit owes a backoff");
            assert!(after <= pacing.floor, "failure {failure}: the backoff {after:?} is capped at the staleness floor");
            assert!(recovery.take_eligible(&birth, last + pacing.floor, &in_flight), "failure {failure}: eligible again within one floor of the failure");
            in_flight.lock().unwrap().remove(&birth);
        }

        // The recovery ran: no backoff. A returned or replaced birth the tick forgets: none either.
        recovery.resubmit_after(&birth, false, &pacing, last);
        assert_eq!(recovery.resubmit_after(&birth, true, &pacing, last), None, "a recovery that ran owes no backoff");
        assert!(recovery.take_eligible(&birth, last, &in_flight), "eligible at once after the recovery ran");
        in_flight.lock().unwrap().remove(&birth);
        recovery.resubmit_after(&birth, false, &pacing, last);
        recovery.resubmit_after(&replacement, false, &pacing, last);
        recovery.forget_resubmits(|b| *b == replacement);
        assert!(recovery.take_eligible(&birth, last, &in_flight), "a birth returned or replaced, forgotten by the tick, keeps no backoff");
        assert!(!recovery.take_eligible(&replacement, last, &in_flight), "a birth the tick still owes keeps its backoff");
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": "the mesh-primary is the submitter that
    /// resubmits by `build_id`"; CLAUDE.md read rule: node-local state that should be the durable
    /// answer is replaced by the read): a submitter that holds nothing in memory (an admin that just
    /// took the role, or restarted) is owed the birth mesh2.node.1 by the durable facts alone: the
    /// current Build holds that birth's run, whose attempt ended Failed after its delete, and the birth
    /// is in no view. Nothing is owed while this admin does not hold the role, nor to another mesh. The
    /// submit span names the Failed attempt it follows, and nothing is inspected again. Must NOT
    /// happen: an owed recovery found only in a previous submitter's memory.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_submitter_that_holds_nothing_in_memory_reads_the_failed_recovery_from_the_durable_facts() {
        let digest_view = DigestView::default();
        digest_view.record(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready));
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        let (path, node_id, incarnation): (PathName, _, _) = (rdm_core::PathName::new("mesh2.node", "1").unwrap(), NodeId::mint(), IncarnationId::mint());
        assert!(view.node(&path).is_none(), "the exited birth deleted: no view holds it");
        let b1 = crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        let deleted = |step: &str| crate::build_state::BuildStepReceipt {
            mesh: None,
            build_id: b1.clone(),
            attempt: 2,
            operation: rdm_core::OperationId::node("node.delete", &node_id),
            step: step.into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output: (step == "NodeDeleted").then(|| {
                serde_json::to_value(rdm_core::LifecycleOp { r#type: "node".into(), mesh: "mesh2".into(), build_id: "b1".into(), attempt: 2, operation: rdm_core::OperationId::node("node.delete", &node_id), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }).unwrap()
            }),
            executor: None,
        };
        builds.absorb(&[
            crate::build_state::BuildFact::Accepted(crate::build_state::BuildAccepted { build_id: b1.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 1 }),
            crate::build_state::BuildFact::Opened(crate::build_state::AttemptOpened {
                mesh: None,
                build_id: b1.clone(),
                attempt: 2,
                reason: crate::build_state::AttemptReason::Recovery,
                action: Some(crate::accepted::AttemptAction::Recover { r#type: "node".into(), mesh: "mesh2".into(), path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone(), backbone: false }),
                opened_by: rdm_core::NodeId::parse("100000000000").unwrap(),
                opened_at_ms: 1,
                context: None,
            }),
            crate::build_state::BuildFact::Claim(crate::build_state::BuildAttemptClaim { claimed_mesh: None, executor_mesh: "mesh2".into(), claimant: None, mesh: None, build_id: b1.clone(), attempt: 2, executor: fixture_actor(true) }),
            crate::build_state::BuildFact::Step(deleted("NodeDeleted")),
            crate::build_state::BuildFact::Step(deleted("Complete")),
            crate::build_state::BuildFact::Attempt(crate::build_state::BuildAttemptReceipt { mesh: None, build_id: b1.clone(), attempt: 2, outcome: crate::build_state::AttemptOutcome::Failed { reason: "node.create: PrepareStorage".into() , code: rdm_core::outcome::Code::FailedPrecondition } }),
        ]).await;
        let fabric = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        crate::fabric_storage::write_identity(&*fabric, &crate::fabric_storage::FabricIdentity { fabric_id: fabric1(), name: "fabric1".into() }).await.unwrap().expect("the fabric's name is written");
        // The exited birth's node record stays in the store, Deleted (a delete appends its status, never removes it).
        let nodes_storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let row = crate::storage::NodeRow { replacement_from: None, r#type: "node".into(), mesh: "mesh2".into(), initial_properties: None, topology_storage_provider: None, name: path.clone(), node_id: node_id.clone(), status: NodeStatus::Deleted, allocated: None, renamed_from: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, writer: None };
        crate::node_rows::write_record(&*nodes_storage, &row).await.unwrap().unwrap();
        let recovery = RecoverySubmitter {
            me: rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(),
            me_id: view.node(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).unwrap().node_id.clone(),
            topology: Arc::new(RwLock::new(view)),
            digest_view,
            provider: Arc::new(Answers { domain: String::new(), status: crate::deployment::provider::DeploymentStatus::Unknown, inspected: Default::default() }),
            nodes_storage,
            written: Default::default(),
            door: Default::default(),
            client: None,
            builds,
            data_root: PathBuf::new(),
            gone_dirs: Arc::default(),
            resubmits: Arc::default(),
            investigated: crate::node_investigation::NodeInvestigatedWriter::in_memory(&rdm_core::FabricId::mint(), &rdm_core::NodeId::mint()),
        };
        assert_eq!(recovery.owed_submits(false, "mesh2").await, vec![], "nothing is owed by an admin that does not hold the role");
        assert_eq!(recovery.owed_submits(true, "mesh1").await, vec![], "another mesh's primary is owed none of mesh2's exits");
        let owed = recovery.owed_submits(true, "mesh2").await;
        assert_eq!(owed, vec![crate::build_state::OwedExit { path: path.clone(), node_id: node_id.clone(), incarnation: incarnation.clone(), attempt: 2, reason: "node.create: PrepareStorage".into() }], "read from the durable facts alone");

        let capture = crate::span_capture::Capture::new("i143-3088-owed-exit-from-durable-facts");
        let submitted = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            recovery.submit_owed(&owed[0]).await
        };
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
        assert_eq!(submits.len(), 1, "the owed recovery reached the submit span: {submits:?}");
        let s = &submits[0].attributes;
        let sattr = |k: &str| s.get(k).cloned().unwrap_or_default();
        assert_eq!((sattr("node_path"), sattr("node_name"), sattr("node_id"), sattr("incarnation_id"), sattr("submitter")), (path.path.clone(), path.name.clone(), node_id.to_string(), incarnation.to_string(), recovery.me_id.to_string()), "keyed on the exact birth: {s:?}");
        assert_eq!(sattr("after_failed_attempt"), "2", "the Failed attempt it follows: {s:?}");
        assert!(submitted.is_err() && !sattr("outcome").is_empty(), "this cell has no fabric-primary door: the submit ends named: {s:?}");
        eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {s:?}", submits[0].trace_id);
    }

    /// CONTRACT (node-recovery.md: "Node recovery is RDM's drift reconciliation for a member that
    /// exits outside any workflow"; "Failure and receipts": "An exit during a workflow belongs to
    /// that workflow"): a birth whose removal the mesh primary's view holds announced (a delete's
    /// `NodeDeleting` overlay naming the exact birth, or its `NodeDeleted` departure) is not a
    /// member to recover, whatever its verdict: no recovery submit, one
    /// `rdm.node_admin.build.reject.via-removal-announced` span naming the workflow. A recovery's own node.delete of the exited birth (`node.delete:<path>`) is such a
    /// workflow while its overlay stands. Must NOT happen: a recovery submitted for a birth whose
    /// own workflow's delete announced its removal (trace 3b983b72926d8d5e3629d7e3d1f264cf: the
    /// tick's second ladder read the view before the workflow's NodeDeleting, inspected the exit
    /// that workflow's own TerminateRuntime made, and submitted).
    // @feature: node-lifecycle
    #[tokio::test]
    async fn an_unheard_birth_whose_removal_was_announced_submits_no_recovery() {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-removal-announced")).unwrap();
        for (case, announced, workflow) in [("delete-deleting", "node-deleting", "node.delete"), ("delete-deleted", "node-deleted", "node.delete")] {
            let mut member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
            member.node.runtime = Some(fact.clone());
            let digest_view = DigestView::default();
            for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), member.clone()] {
                digest_view.record(d);
            }
            // The tick's view, read before the announcement arrived.
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
            let op = rdm_core::LifecycleOp { r#type: member.node.r#type.clone(), mesh: member.node.mesh.clone(),
                build_id: "dbs8z9896j0f".into(),
                attempt: 2,
                operation: rdm_core::OperationId::node(workflow, &member.node.node_id),
                node_id: member.node.node_id.clone(),
                incarnation: member.node.incarnation.clone(),
                name: member.node.name.clone(),
                event_at_rafka_ms: 1,
                traceparent: None,
                tracestate: None,
            };
            assert!(digest_view.deleting(op.clone()), "{case}: the overlay is taken");
            if announced == "node-deleted" {
                assert!(digest_view.depart(op.clone()), "{case}: the departure is taken");
            }
            // The birth's durable runtime row (every admin's blind put on first holding it): the
            // inspection reads it once the departure dropped the digest.
            let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
            crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { r#type: member.node.r#type.clone(), mesh: member.node.mesh.clone(), node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
            let (recovery, _, node) = recovery_of(&view, digest_view, provider, nodes);
            let capture = crate::span_capture::Capture::new(&format!("i143-3068-removal-announced-{case}"));
            let outcome = {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                recovery.recover(&node).await
            };
            assert_eq!(outcome, Inspected::Exited(Some(137)), "{case}: the provider read the exact runtime exited");
            let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
            let rejects = capture.named("rdm.node_admin.build.reject.via-removal-announced");
            assert_eq!(submits.len(), 0, "{case}: no recovery submit for a birth whose removal was announced: {submits:?}");
            assert_eq!(rejects.len(), 1, "{case}: the refusal is spanned once: {rejects:?}");
            let a = &rejects[0].attributes;
            let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
            assert_eq!((attr("node_path"), attr("node_name"), attr("node_id"), attr("incarnation_id")), ("mesh2.node".to_string(), "1".to_string(), node.node_id.to_string(), member.node.incarnation.to_string()), "{case}: the exact birth: {a:?}");
            assert_eq!((attr("announced"), attr("operation"), attr("build_id"), attr("attempt")), (announced.to_string(), workflow.to_string(), "dbs8z9896j0f".to_string(), "2".to_string()), "{case}: the workflow the exit belongs to: {a:?}");
            assert_eq!((attr("submitter"), attr("verdict")), (recovery.me_id.to_string(), "exited".to_string()), "{case}: {a:?}");
            eprintln!("span rdm.node_admin.build.reject.via-removal-announced trace_id={} {a:?}", rejects[0].trace_id);
        }
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that
    /// workflow"; mesh-delete.md: from mesh.deleting, the mesh's status deleting, to mesh.deleted, every
    /// member's removal is the mesh.delete's): an exited member of a mesh whose status the mesh primary
    /// holds deleting or deleted is not a member to recover, a primary lost mid-delete included: no
    /// recovery is submitted, and one `rdm.node_admin.build.reject.via-removal-announced` span names the
    /// mesh.delete. Must NOT
    /// happen: mesh_lifecycle__mesh_delete_resume on f8001275, where mesh2's surviving admin, in its role
    /// after its primary was killed mid-delete, submitted the recovery of that primary 220 times
    /// (trace 8b3d152a6a51c90fc06a4031cd055d37, refused while the Build reconciled).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn an_unheard_member_of_a_deleting_or_deleted_mesh_submits_no_recovery() {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-mesh-deleting")).unwrap();
        for (status, announced) in [("leaving", "mesh-deleting"), ("dead", "mesh-deleted")] {
            let mut member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
            member.node.runtime = Some(fact.clone());
            let digest_view = DigestView::default();
            for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), member.clone()] {
                digest_view.record(d);
            }
            digest_view.statuses.take(&rdm_transport::topology::Frame::MeshStatus { mesh: "mesh2".into(), status: status.into(), publisher: "mesh2.node-admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 }, &fabric1());
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
            let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
            crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { r#type: member.node.r#type.clone(), mesh: member.node.mesh.clone(), node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
            let (recovery, _, node) = recovery_of(&view, digest_view, provider, nodes);
            let capture = crate::span_capture::Capture::new(&format!("i143-3078-mesh-{status}"));
            let outcome = {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                recovery.recover(&node).await
            };
            assert_eq!(outcome, Inspected::Exited(Some(137)), "{status}: the provider read the exact runtime exited");
            let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
            let rejects = capture.named("rdm.node_admin.build.reject.via-removal-announced");
            assert_eq!(submits.len(), 0, "{status}: no recovery of a member of a mesh being deleted: {submits:?}");
            assert_eq!(rejects.len(), 1, "{status}: the refusal is spanned once: {rejects:?}");
            let a = &rejects[0].attributes;
            assert_eq!(a.get("announced").map(String::as_str), Some(announced), "{status}: {a:?}");
            assert!(a.get("operation").is_some_and(|o| o == "mesh.delete") && a.get("operation_subject").is_some_and(|s| s.contains("Mesh(")), "{status}: the mesh.delete is named: {a:?}");
            eprintln!("span rdm.node_admin.build.reject.via-removal-announced trace_id={} {a:?}", rejects[0].trace_id);
        }
    }

    /// CONTRACT (node-investigation.md: "The primary that investigated writes it as one
    /// `rdm.node.investigated` event, `subject` the node.id, with the incarnation, path, name, verdict
    /// and what follows"; fabric-node-hooks.md "on_node_investigated", Q312: "Once it writes a node
    /// investigation's verdict on a node of its mesh as its `rdm.node.investigated` event, the
    /// mesh-primary calls `on_node_investigated` once, with `NodeInvestigatedCtx { fabric_id, mesh_id,
    /// node_id, incarnation, path, name, verdict, next, context }`, on a task of its own"): the
    /// mesh-primary's investigation of an unreached member ends in the provider's verdict (`running`,
    /// `exited`), which opens the re-create. The verdict is read back from the events store as one
    /// `rdm.node.investigated` event of the member's node.id, by this mesh-primary's exact birth, in
    /// the submit's trace, carrying the incarnation, path, name, verdict and `next` = `recreate`; and
    /// the application is told once from that write, naming the exact birth, its mesh and fabric, the
    /// verdict and `next`, under one `rdm.node_admin.node.update.via-on-node-investigated` span, a child
    /// of the submit's span, which names the event. Must NOT happen: no event, a second event or notice
    /// for the one verdict, an event or notice naming another birth or `rejoin`.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn an_investigations_verdict_is_one_node_investigated_event_and_on_node_investigated_is_told_once_from_it() {
        use crate::wiring::{NodeInvestigatedCtx, NodeInvestigatedNext, NodeInvestigatedVerdict};
        use crate::deployment::provider::DeploymentStatus;
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-on-investigated")).unwrap();
        for (status, verdict) in [(DeploymentStatus::Running, NodeInvestigatedVerdict::Running), (DeploymentStatus::Exited { code: Some(137) }, NodeInvestigatedVerdict::Exited)] {
            let mut member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
            member.node.runtime = Some(fact.clone());
            let digest_view = DigestView::default();
            for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), member.clone()] {
                digest_view.record(d);
            }
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
            let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
            crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { r#type: member.node.r#type.clone(), mesh: member.node.mesh.clone(), node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: status.clone(), inspected: Default::default() });
            let (mut recovery, _, node) = recovery_of(&view, digest_view, provider, nodes);
            let told: Arc<Mutex<Vec<NodeInvestigatedCtx>>> = Arc::default();
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            recovery.investigated.on_node_investigated = Some({
                let told = told.clone();
                Arc::new(move |ctx: NodeInvestigatedCtx| {
                    told.lock().unwrap().push(ctx);
                    let _ = tx.send(());
                    Box::pin(async {})
                })
            });
            let capture = crate::span_capture::Capture::new(&format!("i151-on-node-investigated-{}", verdict.as_str()));
            {
                // Held until the notice's task has run on this thread: its span, a child of the submit's,
                // releases the submit's span through this thread's dispatcher when it closes.
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                recovery.recover(&node).await;
                tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await.expect("on_node_investigated is called on its own task").expect("told");
                // Anything a second notice would have sent is in by now: the verdict's one recover has returned.
                tokio::task::yield_now().await;
            }
            let told = told.lock().unwrap().clone();
            assert_eq!(told.len(), 1, "{}: one notice for the one verdict: {told:?}", verdict.as_str());
            let ctx = &told[0];
            assert_eq!((ctx.verdict, ctx.next), (verdict, NodeInvestigatedNext::Recreate), "the verdict recorded and the re-create it opens");
            assert_eq!((&ctx.node_id, &ctx.incarnation, ctx.path.as_str(), ctx.name.as_str()), (&member.node.node_id, &member.node.incarnation, "mesh2.node", "1"), "the exact birth investigated");
            assert_eq!((&ctx.fabric_id, &ctx.mesh_id), (&fabric1(), &mesh_id("mesh2")), "its fabric and mesh");
            let notices = capture.named("rdm.node_admin.node.update.via-on-node-investigated");
            assert_eq!(notices.len(), 1, "one notice span: {notices:?}");
            let a = &notices[0].attributes;
            let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
            assert_eq!((attr("verdict"), attr("next"), attr("outcome"), attr("node_id")), (verdict.as_str().to_string(), "recreate".to_string(), "sent".to_string(), member.node.node_id.to_string()), "{a:?}");
            let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
            assert_eq!(submits.len(), 1, "the verdict's one submit: {submits:?}");
            assert_eq!(notices[0].trace_id, submits[0].trace_id, "the notice is in the submit's trace");
            assert!(ctx.context.traceparent.as_deref().is_some_and(|tp| tp.contains(&submits[0].trace_id)), "the notice carries the investigation's trace context: {:?}", ctx.context);
            // The verdict read back: one event of the member's node.id, written before the notice was sent.
            let events = recovery.investigated.events.events.of_node(&member.node.node_id).await.unwrap();
            assert_eq!(events.len(), 1, "{}: one event for the one verdict: {events:#?}", verdict.as_str());
            let e = &events[0];
            assert_eq!((e.specversion.as_str(), e.r#type.as_str(), e.datacontenttype.as_str(), e.subject.as_ref()), ("1.0", crate::events_storage::NODE_INVESTIGATED, "application/json", Some(&member.node.node_id)), "a CloudEvents 1.0 rdm.node.investigated event about the member: {e:?}");
            assert_eq!(e.source, recovery.investigated.events.source.uri(), "written by the mesh-primary that investigated, its exact birth");
            let data = serde_json::Value::Object(e.data.clone());
            assert_eq!(data, serde_json::json!({"incarnation": member.node.incarnation, "path": "mesh2.node", "name": "1", "verdict": verdict.as_str(), "next": "recreate"}), "the event's data");
            assert!(e.traceparent.as_deref().is_some_and(|tp| tp.contains(&submits[0].trace_id)), "the event carries the investigation's trace context: {e:?}");
            assert_eq!(attr("event_id"), e.id.to_string(), "the notice is sent from the event's write, which its span names");
            assert_eq!(recovery.investigated.events.events.event(&e.id).await.unwrap().as_ref(), Some(e), "the event read by its id");
            eprintln!("span rdm.node_admin.node.update.via-on-node-investigated trace_id={} {a:?}", notices[0].trace_id);
        }
    }

    /// CONTRACT (gossip.md `NodeDeleting`: keyed `(kind, build_id, attempt, operation)`, "the fact
    /// rides `MeshTopology.in_flight` until the operation completes", and `MeshTopology.in_flight` is "while
    /// the operation is claimed and has no completed receipt (derived from Build facts at each publication)";
    /// node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that workflow:
    /// its Build ends Failed, and the mesh-primary's next tick finds the member still proven exited
    /// and submits the recovery"): a removal's `NodeDeleting` overlay, opened from its attempt's
    /// receipt, closes when that attempt's verdict is `Failed` (one
    /// `rdm.mesh.topology.update.via-deleting-closed-by-facts` span naming the attempt), and the
    /// mesh primary's next tick then submits the recovery of the member the failed workflow left:
    /// one `via-node-recovery` submit is made. Must NOT happen: the overlay held after its attempt
    /// failed, so the tick refuses the member `via-removal-announced` for a workflow that is over and
    /// the member is never put back.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_failed_removal_attempts_target_is_recovered_by_the_next_tick() {
        use crate::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-failed-removal")).unwrap();
        let mut member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
        member.node.runtime = Some(fact.clone());
        let digest_view = DigestView::default();
        for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), member.clone()] {
            digest_view.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        let op = rdm_core::LifecycleOp { r#type: member.node.r#type.clone(), mesh: member.node.mesh.clone(),
            build_id: "9r7cpewghdkk".into(),
            attempt: 2,
            operation: rdm_core::OperationId::location("node.delete", &rdm_core::PathName::new("mesh2.node", "1").unwrap()),
            node_id: member.node.node_id.clone(),
            incarnation: member.node.incarnation.clone(),
            name: member.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        };
        let build = crate::build::BuildId::parse(&op.build_id).unwrap();
        let mut facts = vec![
            BuildFact::Accepted(BuildAccepted { build_id: build.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh2", crate::model::MeshId::parse("meshd0000002").unwrap()), submitted_change: None, submitted_at_ms: 1 }),
            BuildFact::Step(BuildStepReceipt { mesh: None, build_id: build.clone(), attempt: 2, operation: op.operation.clone(), step: "NodeDeleting".into(), outcome: StepOutcome::Complete, output: Some(serde_json::to_value(&op).unwrap()), executor: None }),
        ];
        let capture = crate::span_capture::Capture::new("i143-3068-failed-removal-recovered");
        let mut named = NamedSteps::new();
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            overlays_from_facts(&facts, &digest_view, &mut named);
        }
        assert_eq!(digest_view.in_flight(), vec![op.clone()], "the removal's NodeDeleting receipt opens its overlay");
        // The attempt's verdict: the workflow failed (the member's runtime exited under it).
        facts.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: build.clone(), attempt: 2, outcome: AttemptOutcome::Failed { reason: "TerminateRuntime: the member exited under the replace".into() , code: rdm_core::outcome::Code::FailedPrecondition } }));
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            overlays_from_facts(&facts, &digest_view, &mut named);
        }
        assert_eq!(digest_view.in_flight(), Vec::<rdm_core::LifecycleOp>::new(), "the failed attempt's overlay leaves MeshTopology.in_flight: its operation is no longer claimed by that attempt");
        let closed = capture.named("rdm.mesh.topology.update.via-deleting-closed-by-facts");
        assert_eq!(closed.len(), 1, "the close is spanned once: {closed:?}");
        let a = &closed[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("operation"), attr("build_id"), attr("attempt"), attr("by")), ("node.delete".to_string(), "9r7cpewghdkk".to_string(), "2".to_string(), "attempt-failed".to_string()), "the attempt the facts ended: {a:?}");
        eprintln!("span rdm.mesh.topology.update.via-deleting-closed-by-facts trace_id={} {a:?}", closed[0].trace_id);
        // A late copy of the closed attempt's overlay (a frame, a mesh topology) is applied once, not again.
        assert!(!digest_view.deleting(op.clone()), "the closed overlay is not opened again by a copy of it");
        // The mesh primary's next tick: the member's exact runtime is proven exited.
        let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
        crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { r#type: member.node.r#type.clone(), mesh: member.node.mesh.clone(), node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
        let (recovery, _, node) = recovery_of(&view, digest_view, provider, nodes);
        let outcome = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            recovery.recover(&node).await
        };
        assert_eq!(outcome, Inspected::Exited(Some(137)), "the provider read the exact runtime exited");
        let rejects = capture.named("rdm.node_admin.build.reject.via-removal-announced");
        assert!(rejects.is_empty(), "no workflow holds the member: its failed attempt is over: {rejects:?}");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
        assert_eq!(submits.len(), 1, "the recovery is submitted: {submits:?}");
        eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {:?}", submits[0].trace_id, submits[0].attributes);
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that
    /// workflow: its Build ends Failed, and the mesh-primary's next tick finds the member still proven
    /// exited and submits the recovery"; R-BI7 caveat 2: a mesh's drift attempts are numbered in its
    /// own lane, so an attempt number names one attempt per lane): a recovery's `NodeDeleting` hold,
    /// opened from attempt 1 of mesh2's lane, closes when that attempt's verdict in mesh2's lane is
    /// `Failed` (one `rdm.mesh.topology.update.via-deleting-closed-by-facts` span, by
    /// `attempt-failed`). In another Build, the fabric lane's attempt 1 failing closes nothing of
    /// mesh2's attempt 1, which still runs: its hold stays open. Must NOT happen: a drift attempt's
    /// hold held after its attempt failed (the tick then refuses the member
    /// `via-exit-inside-workflow` for good), or one lane's verdict closing another lane's hold.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_drift_attempts_removal_hold_closes_on_its_own_verdict_and_never_on_another_attempts() {
        use crate::build_state::{AttemptOutcome, AttemptReason, AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
        let member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
        let digest_view = DigestView::default();
        for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), member.clone()] {
            digest_view.record(d);
        }
        let mesh = || Some("mesh2".to_string());
        let recovery = |build: &str| rdm_core::LifecycleOp { r#type: member.node.r#type.clone(), mesh: member.node.mesh.clone(),
            build_id: build.into(),
            attempt: 1,
            operation: rdm_core::OperationId::location("node.delete", &rdm_core::PathName::new("mesh2.node", "1").unwrap()),
            node_id: member.node.node_id.clone(),
            incarnation: member.node.incarnation.clone(),
            name: member.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        };
        // mesh2's lane runs attempt 1, the member's recovery; its node.delete published NodeDeleting.
        let lane_attempt = |build: &crate::build::BuildId, op: &rdm_core::LifecycleOp| {
            vec![
                BuildFact::Accepted(BuildAccepted { build_id: build.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh2", crate::model::MeshId::parse("meshd0000002").unwrap()), submitted_change: None, submitted_at_ms: 1 }),
                BuildFact::Opened(AttemptOpened { mesh: mesh(), build_id: build.clone(), attempt: 1, reason: AttemptReason::Recovery, action: None, opened_by: rdm_core::NodeId::parse("300000000000").unwrap(), opened_at_ms: 1, context: None }),
                BuildFact::Claim(BuildAttemptClaim { claimed_mesh: None, executor_mesh: "mesh2".into(), mesh: mesh(), build_id: build.clone(), attempt: 1, executor: fixture_actor(false), claimant: Some(fixture_actor(false).node_id) }),
                BuildFact::Step(BuildStepReceipt { mesh: mesh(), build_id: build.clone(), attempt: 1, operation: op.operation.clone(), step: "NodeDeleting".into(), outcome: StepOutcome::Complete, output: Some(serde_json::to_value(op).unwrap()), executor: None }),
            ]
        };
        let capture = crate::span_capture::Capture::new("i143-s78-drift-lane-hold");
        let mut named = NamedSteps::new();
        let derive = |facts: &[BuildFact], named: &mut NamedSteps| {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            overlays_from_facts(facts, &digest_view, named);
        };

        let op = recovery("9r7cpewghdkk");
        let build = crate::build::BuildId::parse(&op.build_id).unwrap();
        let mut facts = lane_attempt(&build, &op);
        derive(&facts, &mut named);
        assert_eq!(digest_view.in_flight(), vec![op.clone()], "mesh2's attempt 1 holds the member");
        // The attempt's verdict in mesh2's lane: Failed (its claimant exited with it unfinished).
        facts.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: mesh(), build_id: build.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: "mesh2.node-admin.1 exited with attempt 1 of mesh2's lane unfinished".into() , code: rdm_core::outcome::Code::FailedPrecondition } }));
        derive(&facts, &mut named);
        assert_eq!(digest_view.in_flight(), Vec::<rdm_core::LifecycleOp>::new(), "the failed drift attempt's hold closes: the member's exit belongs to no workflow now");
        let closed = capture.named("rdm.mesh.topology.update.via-deleting-closed-by-facts");
        assert_eq!(closed.len(), 1, "the close is spanned once: {closed:?}");
        let a = &closed[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("operation"), attr("attempt"), attr("by")), ("node.delete".to_string(), "1".to_string(), "attempt-failed".to_string()), "{a:?}");
        eprintln!("span rdm.mesh.topology.update.via-deleting-closed-by-facts trace_id={} {a:?}", closed[0].trace_id);

        // Another Build: the fabric lane's attempt 1 failed; mesh2's attempt 1 still runs.
        let op = recovery("7k2qm4xw9bdc");
        let build = crate::build::BuildId::parse(&op.build_id).unwrap();
        let mut facts = lane_attempt(&build, &op);
        facts.insert(1, BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: build.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: "a topology operation failed".into() , code: rdm_core::outcome::Code::FailedPrecondition } }));
        derive(&facts, &mut named);
        assert_eq!(digest_view.in_flight(), vec![op], "the fabric lane's verdict closes nothing of mesh2's running attempt");
        assert_eq!(capture.named("rdm.mesh.topology.update.via-deleting-closed-by-facts").len(), 1, "no second close");
    }

    /// CONTRACT (node-recovery.md "Detection and proof": the mesh primary watches its own mesh's
    /// births, those known only by a durable runtime row included; CLAUDE.md: an error state names
    /// what failed and where): when the tick's read of the durable runtime rows is refused, the
    /// refusal is one `rdm.node_admin.node.reject.via-runtime-rows-unreadable` span naming the
    /// reader, the store and its cause, and the tick watches the view's births only. Must NOT happen:
    /// the narrowed watch with no span (an info line only).
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_refused_read_of_the_durable_runtime_rows_is_spanned_and_the_tick_watches_the_views_births() {
        let digest_view = DigestView::default();
        for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready)] {
            digest_view.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        let provider = Arc::new(Answers { domain: String::new(), status: crate::deployment::provider::DeploymentStatus::Unknown, inspected: Default::default() });
        let (recovery, _, _) = recovery_of(&view, digest_view, provider, Arc::new(RuntimeRowUnreadable::default()));
        let capture = crate::span_capture::Capture::new("i143-3051-runtime-rows-unreadable");
        let rows = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            TenureRows::default().for_tick(true, &*recovery.nodes_storage, &recovery.held_name(), "mesh2").await.to_vec()
        };
        assert!(rows.is_empty(), "no row is read from a refused store: the view's births only");
        let spans = capture.named("rdm.node_admin.node.reject.via-runtime-rows-unreadable");
        assert_eq!(spans.len(), 1, "the refused read is spanned: {spans:?}");
        let a = &spans[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("reader_path"), attr("reader_name"), attr("store"), attr("error")), ("mesh2.node-admin".to_string(), "1".to_string(), "nodes.storage".to_string(), "runtimes: Input/output error (os error 5)".to_string()), "{a:?}");
        eprintln!("span rdm.node_admin.node.reject.via-runtime-rows-unreadable trace_id={} {a:?}", spans[0].trace_id);
    }

    /// CONTRACT (node-recovery.md step 3: the provider is asked "about the exact runtime recorded at
    /// launch", and any verdict but `reached` opens the recovery at once): a TRUE OFFLINE member whose
    /// digest carries no runtime is inspected through its durable runtime row, and that point read is
    /// refused. With no runtime fact the provider is not asked: the verdict is `unavailable`, naming the
    /// row and the store's own cause, and the recovery is submitted on it. Must NOT happen: the refusal
    /// read as "no row", an inspection of a guessed runtime, a member held for want of a verdict.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_true_offline_member_whose_runtime_row_cannot_be_read_is_unproven_and_recovered() {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-unreadable")).unwrap();
        let member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
        let digest_view = DigestView::default();
        for d in [digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), member.clone()] {
            digest_view.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        // The provider would read an exit: it must never be asked without the birth's runtime fact.
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() });
        let (recovery, _, node) = recovery_of(&view, digest_view, provider.clone(), Arc::new(RuntimeRowUnreadable::default()));
        let (outcome, inspections, submits) = two_ticks(&recovery, &node, "i143-3051-runtime-row-unreadable").await;
        let why = format!("the durable runtime row of {} at {} could not be read: runtimes/{}--{}.json is not a record this build recognises: format runtime/9 is not one this build reads", node.node_id, member.node.incarnation.as_str(), node.node_id, member.node.incarnation.as_str());
        assert_eq!(outcome, Inspected::Unavailable(why.clone()), "an unreadable row is named as unreadable");
        assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 0, "with no runtime fact the provider is not asked");
        let a = &inspections[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("verdict"), attr("reason")), ("unavailable".to_string(), why), "{a:?}");
        assert_eq!(submits.len(), 2, "the recovery is submitted on each tick: {submits:?}");
        eprintln!("span rdm.node_admin.node.resolve.via-provider-inspection trace_id={} {a:?}", inspections[0].trace_id);
    }

    /// CONTRACT (states.md): a mesh is ready when every node of the mesh is. A mesh with a
    /// primary and a member not yet ready projects `pending`, and the mesh whose nodes
    /// are all ready projects `ready`. Must NOT happen: a mesh ready because it has a
    /// primary.
    #[test]
    fn a_mesh_with_a_node_not_ready_projects_pending() {
        use MemberStatus::*;
        let t = view(&[
            with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000"),
            with_id(digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Pending), "300000000000"),
            with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "400000000000"),
            with_id(digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", Ready), "500000000000"),
        ]);
        assert!(t.node_type_primary("mesh1", "node-admin").is_some(), "mesh1 has a primary");
        let status = |m: &str| t.mesh_view(m).unwrap().status;
        assert_eq!(status("mesh1"), ScopeStatus::Pending, "a node of mesh1 is not ready: the mesh is not");
        assert_eq!(status("mesh2"), ScopeStatus::Ready, "every node of mesh2 is ready");
    }

    /// CONTRACT (states.md, "Resize and executable replacement"): the nodes a mesh is counted over are the accepted
    /// Build's planned births. A planned birth the view has not heard keeps its mesh `pending`, and a
    /// mesh planned and not yet created is not listed. Must NOT happen: a mesh ready because
    /// the births that happen to be heard are ready.
    #[test]
    fn a_mesh_with_a_planned_birth_not_yet_heard_projects_pending() {
        use MemberStatus::*;
        let digest_view = DigestView::default();
        for d in [with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000"), with_id(digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Ready), "300000000000")] {
            digest_view.record(d);
        }
        let desired = |mesh: &str, rpc: u32| crate::build::MeshDesired::of(mesh, [("node-admin", 1), ("node", rpc)]);
        let records = Records::default();
        let mut plan = crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), topology_storage_provider: None, fabric: "fabric1".into(), meshes: BTreeMap::new() };
        plan.meshes.insert("mesh1".into(), crate::accepted::MeshTopology::of(&desired("mesh1", 1)));
        records.set_planned(&plan);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::Ready, "every planned birth of mesh1 is heard and ready");
        plan.meshes.insert("mesh1".into(), crate::accepted::MeshTopology::of(&desired("mesh1", 2)));
        plan.meshes.insert("mesh2".into(), crate::accepted::MeshTopology::of(&desired("mesh2", 1)));
        records.set_planned(&plan);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::Pending, "mesh1.node.2 is planned and not heard");
        assert!(t.mesh_view("mesh2").is_none(), "mesh2 is planned and does not exist yet: the view lists what is observed, so the Build still plans its creation");
        let why = t.mesh_unready("mesh1", records.planned().get("mesh1")).iter().map(|m| m.to_string()).collect::<Vec<_>>();
        assert_eq!(why, [r#"path="mesh1.node", name="2"(no-birth-heard)"#], "the node behind a pending mesh is named by its planned (path, name): no birth stands there");
    }

    /// CONTRACT (R-S3, states.md): a mesh's lifecycle is pending, ready, leaving, dead. A mesh
    /// heard ready that loses a node projects ready while it has a primary, so the primary that
    /// takes over adopts the state and runs its checklist. A mesh never heard ready projects pending while a
    /// node is not ready. Must NOT happen: a ready mesh projected pending because one of its nodes is gone,
    /// which gives the takeover a new status and no checklist.
    #[test]
    fn a_mesh_heard_ready_stays_ready_when_a_node_is_lost() {
        use MemberStatus::*;
        let digest_view = DigestView::default();
        for d in [with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000"), with_id(digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Ready), "300000000000"), with_id(digest(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", Pending), "400000000000")] {
            digest_view.record(d);
        }
        let records = Records::default();
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::Pending, "never heard ready, and mesh1.node.2 is not ready");
        records.set_heard_ready(BTreeSet::from(["mesh1".to_string()]));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::Ready, "heard ready: a node not ready does not take the mesh back to pending");
    }

    /// CONTRACT (gossip.md §3.1 `NodeDeleted`: the birth is removed): a birth whose node.delete was
    /// heard leaves the view whatever its last digest said, so a mesh whose admins are all deleted has
    /// no primary admin. Must NOT happen: a deleted admin holds its mesh's role because its digest says
    /// ready.
    #[tokio::test]
    async fn a_deleted_birth_leaves_the_view_whatever_its_digest_says() {
        use MemberStatus::*;
        let digest_view = DigestView::default();
        let a = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000");
        let b = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "300000000000");
        digest_view.record(a.clone());
        digest_view.record(b.clone());
        let records = Records::default();
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert!(t.node_type_primary("mesh2", "node-admin").is_some());
        let t = depart(&digest_view, &records, &b).await;
        assert!(t.node(&rdm_core::PathName::new("mesh2.node-admin", "1").unwrap()).is_none(), "the deleted birth left the view");
        assert!(t.node_type_primary("mesh2", "node-admin").is_none(), "a deleted admin holds no primary role");
    }

    /// CONTRACT (node-recovery.md "The mesh-primary is the reconciler for its own members";
    /// node-recovery.md step 2: an unheard peer mesh-primary is the fabric-primary's to investigate): a mesh-primary's offline tickle
    /// watches the silent members of its own mesh and nothing of a peer mesh, its node-admins
    /// included. A peer mesh's node-admins are reached by the investigation's probes alone. Must NOT
    /// happen: a mesh-primary tickling, holding TRUE OFFLINE or writing a silence status for a peer
    /// mesh's node-admin on a per-member clock beside the investigation.
    #[test]
    fn a_mesh_primary_watches_only_its_own_meshs_silent_members() {
        use MemberStatus::*;
        let digest_view = DigestView::default();
        let me = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000");
        let own = with_id(digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Ready), "300000000000");
        let peer_admin = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "400000000000");
        let peer_rpc = with_id(digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", Ready), "500000000000");
        for d in [&me, &own, &peer_admin, &peer_rpc] {
            digest_view.record((*d).clone());
        }
        let mut t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        for n in t.nodes.iter_mut().filter(|n| n.node_id != me.node.node_id) {
            n.status = NodeStatus::PendingReconnect;
        }
        let watched: Vec<PathName> = watched_births(&t, &me.node.node_id, "mesh1", &[]).values().map(|n| n.name.clone()).collect();
        assert_eq!(watched, vec![PathName::new("mesh1.node", "1").unwrap()], "only its own mesh's silent member is watched");
    }

    /// CONTRACT (node-tickle.md step 1; R-PV1: a `NotSent` command "is a reason to ask now"; i144 R9): a
    /// member heard a moment ago (well within the floor) that a command never reached for a
    /// reachability reason is watched `PendingReconnect` by its mesh-primary's tick, counted silent,
    /// and its silence window starts at the mark, not at the floor. A digest of the birth heard after
    /// the mark ends it; so does the view no longer holding the birth. A mark of another mesh's birth
    /// stands in the one map and is not this tick's to watch (the primary watch reads it, Q338).
    /// Must NOT happen: a marked member left out of the silent set because its last digest is recent.
    // @feature: node-lifecycle
    #[test]
    fn an_unreachable_mark_watches_the_member_at_once_and_ends_when_it_is_heard_after_it() {
        use MemberStatus::*;
        let digest_view = DigestView::default();
        let me = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000");
        let own = with_id(digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Ready), "300000000000");
        let peer = with_id(digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", Ready), "500000000000");
        for d in [&me, &own, &peer] {
            digest_view.record((*d).clone());
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        let floor = std::time::Duration::from_secs(30);
        let marks: crate::offline::UnreachableMarks = Arc::default();
        let at = std::time::Instant::now();
        for d in [&own, &peer] {
            marks.lock().unwrap().insert(d.node.node_id.clone(), crate::offline::UnreachableMark { incarnation: d.node.incarnation.clone(), at, at_ms: 1_000, reason: "connect-timed-out" });
        }
        let now = at + std::time::Duration::from_millis(1);
        let held = crate::offline::held_marks(&marks, &view, &digest_view, now);
        assert_eq!(held.len(), 2, "both marks stand: the map is one, the tick watches its own mesh's");
        let mut watched = watched_births(&view, &me.node.node_id, "mesh1", &[]);
        assert!(watched.is_empty(), "nothing is silent past the floor");
        crate::offline::watch_marked(&mut watched, &held, &view, "mesh1", &me.node.node_id);
        let n = watched.get(own.node.node_id.as_str()).expect("the marked member is watched");
        assert_eq!(n.status, NodeStatus::PendingReconnect);
        assert!(!watched.contains_key(peer.node.node_id.as_str()), "another mesh's birth is not this tick's to watch");
        let silent: Vec<_> = crate::offline::silent_set(watched.values(), &digest_view, now, 5_000, floor).into_iter().map(|s| crate::offline::marked_from(s, &held)).collect();
        assert_eq!(silent.iter().map(|s| (s.node_id.clone(), s.marked_at_ms)).collect::<Vec<_>>(), vec![(own.node.node_id.to_string(), Some(1_000))], "silent from the mark, its window from the mark's instant");
        // A digest heard after the mark (the birth's next, a higher digest_seq) ends it.
        let mut next = own.clone();
        next.digest_seq += 1;
        digest_view.record(next);
        let later = std::time::Instant::now() + std::time::Duration::from_millis(5);
        let held = crate::offline::held_marks(&marks, &view, &digest_view, later);
        assert_eq!(held.keys().cloned().collect::<Vec<_>>(), vec![peer.node.node_id.clone()], "heard after the mark: the mark ends; the unheard one stands");
        assert!(!marks.lock().unwrap().contains_key(&own.node.node_id), "an ended mark is dropped");
    }

    /// CONTRACT (i151.s2; node-rpc.md §43, Q311: "a call whose attempts end unanswered starts node
    /// investigation on that birth (the mesh-primary itself, ...)"; node-recovery.md step 1: "The
    /// mesh-primary also marks a member `PendingReconnect` at once when a command to it was never
    /// sent"; step 2: "Half a floor after the mark it runs node investigation"): a mesh-primary's own
    /// call to a member of its mesh whose attempts are spent marks the birth at once, the tick watches it
    /// from the mark, and its ladder starts half a floor after the mark, never before. A second spent
    /// call keeps the first mark (one investigation per silence). Must NOT happen: a birth of another
    /// mesh marked here (it is reported to its own mesh-primary), or a ladder before half a floor.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_mesh_primarys_spent_call_to_a_member_marks_it_and_the_tick_investigates_it_half_a_floor_after_the_mark() {
        use MemberStatus::*;
        let digest_view = DigestView::default();
        let me = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000");
        let own = with_id(digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Ready), "300000000000");
        let peer = with_id(digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", Ready), "500000000000");
        for d in [&me, &own, &peer] {
            digest_view.record((*d).clone());
        }
        let mut view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        for n in view.nodes.iter_mut().filter(|n| n.node_id == me.node.node_id) {
            n.is_primary = true;
        }
        assert!(view.is_mesh_primary("mesh1", &me.node.node_id));
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let resolver = Arc::new(rdm_transport::LiveNodeResolver::default());
        let client = rdm_transport::NodeRpcClient::new(ep.clone(), resolver.clone());
        let marks: crate::offline::UnreachableMarks = Arc::default();
        let decider = crate::offline::MarkOrReport {
            report: rdm_node::spent_calls::ReportToPrimary::new(resolver, &client, me.node.node_id.clone(), me.node.incarnation.clone(), me.node.name.clone()),
            topology: Arc::new(tokio::sync::RwLock::new(view.clone())),
            marks: marks.clone(),
            digest_view: digest_view.clone(),
            me_id: me.node.node_id.clone(),
        };
        let spent = |d: &MeshDigest| rdm_node::spent_calls::SpentCall {
            receiver: rdm_transport::ResolvedNode { mesh: d.node.mesh.clone(), r#type: d.node.r#type.clone(), node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: iroh::SecretKey::generate().public(), transport_addr: d.node.transport_addr, incarnation: d.node.incarnation.clone() },
            reason: "NotSent(Connection(\"connect refused\"))".into(),
        };
        decider.decide(&spent(&own)).await;
        decider.decide(&spent(&peer)).await;
        let first = marks.lock().unwrap().get(&own.node.node_id).cloned().expect("the member is marked at once");
        assert_eq!((first.incarnation.clone(), first.reason), (own.node.incarnation.clone(), "attempts-spent"));
        assert!(!marks.lock().unwrap().contains_key(&peer.node.node_id), "another mesh's birth is reported to its own mesh-primary, never marked here");
        decider.decide(&spent(&own)).await;
        assert_eq!(marks.lock().unwrap().get(&own.node.node_id), Some(&first), "a later spent call keeps the first mark");
        // The tick: the marked member is watched PendingReconnect, silent from the mark's instant.
        let floor = std::time::Duration::from_secs(30);
        let now = std::time::Instant::now();
        let held = crate::offline::held_marks(&marks, &view, &digest_view, now);
        let mut watched = watched_births(&view, &me.node.node_id, "mesh1", &[]);
        crate::offline::watch_marked(&mut watched, &held, &view, "mesh1", &me.node.node_id);
        assert_eq!(watched.get(own.node.node_id.as_str()).map(|n| n.status), Some(NodeStatus::PendingReconnect));
        let silent: Vec<_> = crate::offline::silent_set(watched.values(), &digest_view, now, first.at_ms, floor).into_iter().map(|s| crate::offline::marked_from(s, &held)).collect();
        assert_eq!(silent.iter().map(|s| (s.node_id.clone(), s.marked_at_ms)).collect::<Vec<_>>(), vec![(own.node.node_id.to_string(), Some(first.at_ms))]);
        // Its ladder starts half a floor after the mark (node-recovery.md step 2), not a tick before.
        let half = floor.as_millis() as u64 / 2;
        let started = std::sync::atomic::AtomicUsize::new(0);
        let mut tickle = crate::offline::OfflineTickle::new();
        for (at, ladders) in [(first.at_ms + half - 1, 0), (first.at_ms + half, 1)] {
            tickle
                .tick(at, true, silent.clone(), |_| true, half, |_| {
                    started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    async { crate::offline::Ladder::Tickled(Err("the cell runs no probe".into())) }
                }, |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() })
                .await;
            assert_eq!(started.load(std::sync::atomic::Ordering::SeqCst), ladders, "at {} ms after the mark", at - first.at_ms);
        }
        ep.close().await;
    }

    /// CONTRACT (node-investigation.md: "a call whose attempts end unanswered marks its birth at the
    /// primary above it (the caller itself when it is that primary ...)"; Caller: "the fabric-primary for
    /// a mesh-primary"; Q338): the fabric-primary's own spent call to a peer mesh-primary marks that
    /// birth at once; its spent call to a member of that mesh marks nothing here (the member's
    /// mesh-primary is the primary above it); a mesh-primary's spent call to the fabric-primary marks
    /// nothing (nothing is above it). Must NOT happen: the fabric-primary marking a peer mesh's member, or
    /// anyone marking the fabric-primary.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_fabric_primarys_spent_call_to_a_mesh_primary_marks_it_and_nothing_marks_the_fabric_primary() {
        use MemberStatus::*;
        let digest_view = DigestView::default();
        let fp = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000");
        let m2p = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "300000000000");
        let m2rpc = with_id(digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", Ready), "500000000000");
        for d in [&fp, &m2p, &m2rpc] {
            digest_view.record((*d).clone());
        }
        let mut view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        for n in view.nodes.iter_mut() {
            n.is_primary = n.node_id == fp.node.node_id || n.node_id == m2p.node.node_id;
            n.is_fabric_primary = n.node_id == fp.node.node_id;
        }
        assert!(view.is_fabric_primary(&fp.node.node_id) && view.is_mesh_primary("mesh2", &m2p.node.node_id));
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let resolver = Arc::new(rdm_transport::LiveNodeResolver::default());
        let client = rdm_transport::NodeRpcClient::new(ep.clone(), resolver.clone());
        let decider = |me: &MeshDigest, marks: &crate::offline::UnreachableMarks| crate::offline::MarkOrReport {
            report: rdm_node::spent_calls::ReportToPrimary::new(resolver.clone(), &client, me.node.node_id.clone(), me.node.incarnation.clone(), me.node.name.clone()),
            topology: Arc::new(tokio::sync::RwLock::new(view.clone())),
            marks: marks.clone(),
            digest_view: digest_view.clone(),
            me_id: me.node.node_id.clone(),
        };
        let spent = |d: &MeshDigest| rdm_node::spent_calls::SpentCall {
            receiver: rdm_transport::ResolvedNode { mesh: d.node.mesh.clone(), r#type: d.node.r#type.clone(), node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: iroh::SecretKey::generate().public(), transport_addr: d.node.transport_addr, incarnation: d.node.incarnation.clone() },
            reason: "Indeterminate(Timeout)".into(),
        };
        let at_fp: crate::offline::UnreachableMarks = Arc::default();
        decider(&fp, &at_fp).decide(&spent(&m2rpc)).await;
        assert!(at_fp.lock().unwrap().is_empty(), "a peer mesh's member is its mesh-primary's, never marked at the fabric-primary");
        decider(&fp, &at_fp).decide(&spent(&m2p)).await;
        let mark = at_fp.lock().unwrap().get(&m2p.node.node_id).cloned().expect("the fabric-primary's own spent call marks the mesh-primary at once");
        assert_eq!((mark.incarnation, mark.reason), (m2p.node.incarnation.clone(), "attempts-spent"));
        let at_m2p: crate::offline::UnreachableMarks = Arc::default();
        decider(&m2p, &at_m2p).decide(&spent(&fp)).await;
        assert!(at_m2p.lock().unwrap().is_empty(), "nothing is above the fabric-primary: nothing marks it");
        ep.close().await;
    }

    /// CONTRACT (node-recovery.md step 1, Q340: "a silence mark ends when a digest of the exact birth
    /// arrives after it or when the birth answers its tickle; a spent-call mark ends only when the
    /// exact birth answers ..."; node-investigation.md: "A node that does not answer a call is silent,
    /// whatever it gossips"; Q338): a mesh-primary that gossips every interval and drops every call is
    /// marked at the fabric-primary for a spent call, keeps the mark through the digests it sends after
    /// it, and the fabric-primary's primary watch investigates it from that mark. A command-never-sent
    /// mark of the same birth ends on its next digest, as before. Must NOT happen: a gossiping
    /// mesh-primary's spent-call mark ended by its digest, so nothing investigates it.
    // @feature: node-lifecycle
    #[test]
    fn a_gossiping_mesh_primarys_spent_call_mark_stands_through_its_digests_and_the_fabric_primary_investigates_it() {
        use MemberStatus::*;
        let digest_view = DigestView::default();
        let fp = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000");
        let m2p = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "300000000000");
        for d in [&fp, &m2p] {
            digest_view.record((*d).clone());
        }
        let mut view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        for n in view.nodes.iter_mut() {
            n.is_primary = true;
            n.is_fabric_primary = n.node_id == fp.node.node_id;
        }
        let node = view.own(&m2p.node.node_id).cloned().expect("the view holds mesh2's primary");
        assert_eq!(node.status, NodeStatus::Ready, "it gossips: the view holds it ready");
        let spent: crate::offline::UnreachableMarks = Arc::default();
        assert_eq!(crate::offline::mark_spent(&spent, Some(&digest_view), &node, "mesh2.node.1", "Indeterminate(Timeout)"), "marked");
        let never_sent: crate::offline::UnreachableMarks = Arc::default();
        never_sent.lock().unwrap().insert(node.node_id.clone(), crate::offline::UnreachableMark { incarnation: m2p.node.incarnation.clone(), at: std::time::Instant::now(), at_ms: local_now_ms(), reason: "connect-refused" });
        // Its digests keep arriving after the mark.
        for _ in 0..3 {
            std::thread::sleep(std::time::Duration::from_millis(2));
            let mut next = digest_view.get(m2p.node.node_id.as_str()).map(|(d, _)| d).expect("held");
            next.digest_seq += 1;
            digest_view.record(next);
        }
        let now = std::time::Instant::now() + std::time::Duration::from_millis(1);
        let held = crate::offline::held_marks(&spent, &view, &digest_view, now);
        assert!(held.contains_key(&node.node_id), "a spent-call mark is never ended by a digest");
        assert!(crate::offline::held_marks(&never_sent, &view, &digest_view, now).is_empty(), "a silence mark ends on a digest after it, as before");
        let marked = |n: &crate::model::Node| held.get(&n.node_id).is_some_and(|m| n.incarnation_id.as_ref() == Some(&m.incarnation));
        let me = (fp.node.node_id.clone(), fp.node.incarnation.clone());
        let plan = crate::primary_watch::plan(&view, "mesh1", &me, &mut crate::primary_watch::Said::default(), &|_| false, &marked);
        assert_eq!(plan.investigate.iter().map(|n| n.node_id.clone()).collect::<Vec<_>>(), vec![node.node_id.clone()], "the fabric-primary investigates the gossiping mesh-primary from its mark");
        // The view no longer holds the birth: the mark ends.
        let gone = Topology { nodes: view.nodes.iter().filter(|n| n.node_id != node.node_id).cloned().collect(), ..view.clone() };
        assert!(crate::offline::held_marks(&spent, &gone, &digest_view, now).is_empty(), "a birth the view no longer holds ends its mark");
    }

    /// CONTRACT (node-recovery.md step 1, Q340: "a spent-call mark ends only when the exact birth
    /// answers, a call of this primary's or the investigation's tickle"; node-rpc.md §43: the client
    /// tells its observer how each call ends): the fabric-primary holds a spent-call mark of mesh2's
    /// primary; the birth's digests after it leave it standing; a later call of the fabric-primary's own
    /// client that the exact birth answers ends it at once. A silence mark (a command never sent) is
    /// not ended by an answered call. Must NOT happen: the mark ended by a digest, or left standing after
    /// the marked birth answered this primary's call.
    // @feature: node-lifecycle
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_spent_call_mark_ends_when_the_marked_birth_answers_a_later_call_of_this_primary_and_not_on_its_digests() {
        use MemberStatus::*;
        use rdm_core::ping::{Ping, PingReply, PingRequest};
        let digest_view = DigestView::default();
        let fp = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "200000000000");
        let m2p = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "300000000000");
        for d in [&fp, &m2p] {
            digest_view.record((*d).clone());
        }
        let mut view = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        for n in view.nodes.iter_mut() {
            n.is_primary = true;
            n.is_fabric_primary = n.node_id == fp.node.node_id;
        }
        let node = view.own(&m2p.node.node_id).cloned().expect("the view holds mesh2's primary");
        // mesh2's primary: a birth that answers Ping.
        let key = iroh::SecretKey::generate();
        let m2p_ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let served = rdm_transport::ServerBuilder::new()
            .serve::<Ping, _, _>(rdm_core::catalog::OpOwner::Core, |_peer, req: PingRequest| async move {
                let PingRequest::Ping { payload } = req;
                Ok(PingReply::Pong { payload })
            })
            .build(rdm_transport::ServedBirth { node_id: m2p.node.node_id.to_string(), incarnation: m2p.node.incarnation.to_string() })
            .unwrap();
        let router = iroh::protocol::Router::builder(m2p_ep.clone()).accept(rdm_transport::ALPN, served).spawn();
        let resolver = Arc::new(rdm_transport::StaticResolver::new());
        resolver.insert(rdm_transport::ResolvedNode { mesh: "mesh2".into(), r#type: "node-admin".into(), node_id: m2p.node.node_id.clone(), name: m2p.node.name.clone(), endpoint_id: key.public(), transport_addr: m2p_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap(), incarnation: m2p.node.incarnation.clone() });
        // The fabric-primary's client, its calls' ends told to its MarkOrReport.
        let fp_ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let ends = rdm_transport::CallEndsSlot::default();
        let client = rdm_transport::NodeRpcClient::new(fp_ep.clone(), resolver.clone()).with_call_ends(ends.clone());
        let marks: crate::offline::UnreachableMarks = Arc::default();
        let _ = ends.set(Arc::new(crate::offline::MarkOrReport {
            report: rdm_node::spent_calls::ReportToPrimary::new(Arc::new(rdm_transport::LiveNodeResolver::default()), &client, fp.node.node_id.clone(), fp.node.incarnation.clone(), fp.node.name.clone()),
            topology: Arc::new(tokio::sync::RwLock::new(view.clone())),
            marks: marks.clone(),
            digest_view: digest_view.clone(),
            me_id: fp.node.node_id.clone(),
        }));
        // The mark, then the birth's digests after it: it stands.
        assert_eq!(crate::offline::mark_spent(&marks, Some(&digest_view), &node, "mesh2.node.1", "Indeterminate(ReplyDeadline)"), "marked");
        std::thread::sleep(std::time::Duration::from_millis(2));
        let mut next = m2p.clone();
        next.digest_seq += 1;
        digest_view.record(next);
        let now = std::time::Instant::now() + std::time::Duration::from_millis(1);
        assert!(crate::offline::held_marks(&marks, &view, &digest_view, now).contains_key(&node.node_id), "a digest after the mark leaves it standing");
        // A later call of this primary's, answered by the exact birth: the mark ends.
        let (out, _) = client.call::<Ping>(&rdm_transport::NodeTarget::ExactNode(m2p.node.node_id.clone()), &PingRequest::Ping { payload: b"p".to_vec() }, &rdm_transport::CallOptions::default()).await;
        assert_eq!(out.name(), "Reply", "the marked birth answered");
        assert!(!marks.lock().unwrap().contains_key(&node.node_id), "the birth answered a call of this primary's: its spent-call mark ended");
        // A silence mark of the same birth is not ended by an answered call.
        marks.lock().unwrap().insert(node.node_id.clone(), crate::offline::UnreachableMark { incarnation: m2p.node.incarnation.clone(), at: std::time::Instant::now(), at_ms: local_now_ms(), reason: "connect-refused" });
        let (out, _) = client.call::<Ping>(&rdm_transport::NodeTarget::ExactNode(m2p.node.node_id.clone()), &PingRequest::Ping { payload: b"q".to_vec() }, &rdm_transport::CallOptions::default()).await;
        assert_eq!(out.name(), "Reply");
        assert!(marks.lock().unwrap().contains_key(&node.node_id), "a silence mark is not a spent-call mark: an answered call does not end it");
        router.shutdown().await.ok();
        fp_ep.close().await;
    }

    /// CONTRACT (node-recovery.md, Detection: "The mesh-primary's tick (every gossip interval) reads
    /// the members its view marks `PendingReconnect` (unheard past the staleness floor)"; "Detection
    /// and proof" step 1: "Every node-admin keeps the silence bookkeeping (`first_seen`), so a
    /// node-admin that takes the role mid-window continues the window already running"): an admin of
    /// mesh2 whose view holds mesh2.node.1 only from its mesh-primary's projection (the member's own
    /// digest never reached it) lists the birth in its view, never routable: with the projected
    /// status within one staleness floor of first holding it, and `PendingReconnect` past it. Its
    /// tick watches the birth, and the silence mark is the instant it first held it plus the floor,
    /// so the window it keeps began before any role change. Must NOT happen: the view holding no birth
    /// of the member, so an heir's tick watches nothing and the member is never recovered.
    #[test]
    fn an_admin_lists_and_watches_an_own_mesh_birth_held_only_from_its_primarys_projection() {
        use MemberStatus::*;
        let digest_view = DigestView::default();
        let me = with_id(digest(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", Ready), "200000000000");
        assert!(digest_view.record(me.clone()));
        let member = with_id(digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", Ready), "300000000000");
        let t0 = std::time::Instant::now();
        digest_view.hold_projected_at(&member, t0);
        let floor = digest_view.staleness_floor();
        let records = Records::default();
        let path: PathName = rdm_core::PathName::new("mesh2.node", "1").unwrap();
        let early = project_at("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, t0 + floor / 2);
        let n = early.node(&path).unwrap_or_else(|| panic!("the view holds the birth its primary's projection lists: {:?}", early.nodes));
        assert_eq!((n.node_id.clone(), n.status, n.routable), (member.node.node_id.clone(), NodeStatus::Ready, false), "within the floor: the projected status, never routable (no liveness heard)");
        let late_at = t0 + floor + Duration::from_millis(500);
        let late = project_at("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, late_at);
        assert_eq!(late.node(&path).map(|n| n.status), Some(NodeStatus::PendingReconnect), "unheard past the floor since it was first held");
        let watched = watched_births(&late, &me.node.node_id, "mesh2", &[]);
        assert_eq!(watched.keys().cloned().collect::<Vec<_>>(), vec![member.node.node_id.to_string()], "the tick watches it");
        let now_ms = 1_000_000u64;
        let silent = crate::offline::silent_set(watched.values(), &digest_view, late_at, now_ms, floor);
        assert_eq!(silent.iter().map(|s| s.marked_at_ms).collect::<Vec<_>>(), vec![Some(now_ms - 500)], "the mark: first held plus the floor");
    }

    /// CONTRACT (R-FM1: "An answer from the exact birth or its returning digest ends the hold and clears
    /// the fact (`reachable-silent` / `offline-returned`, no action)"; Q7 = a): the mesh primary hears a
    /// member again against what its tick watches. A member its view does not hold, named only by a
    /// durable runtime row at a planned path (born while this admin was silent), is in that set, so the
    /// ladder reports it returned once it leaves the silent set; a node id neither the view nor the watched
    /// births hold is not, and stays gone. Must NOT happen: the re-born admin's tick deciding from its view
    /// snapshot alone, reporting such a member `via-offline-gone` and keeping its mark and its hold
    /// (kept/e6s7-i144-e4-re-born-admin-third-defect).
    #[tokio::test(start_paused = true)]
    async fn a_member_watched_only_through_durable_rows_is_heard_again_not_gone() {
        let me = digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready);
        let member = digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready);
        let snapshot = view(&[me.clone()]);
        assert!(snapshot.births().all(|n| n.node_id != member.node.node_id), "the view does not hold the member");
        let row = crate::storage::RuntimeRow { r#type: member.node.r#type.clone(), mesh: member.node.mesh.clone(), node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("q7-heard-against")).unwrap(), data_dir: None };
        let durable = durable_births(&snapshot, &[row], &|_| true, &|_| false);
        let watched = watched_births(&snapshot, &me.node.node_id, "mesh2", &durable);
        let heard = heard_against(&snapshot, &watched);
        let id = member.node.node_id.to_string();
        assert!(heard.contains(&id), "a birth named only by durable rows is heard against: {heard:?}");
        let unknown = NodeId::mint().to_string();
        assert!(!heard.contains(&unknown), "a node id neither viewed nor watched is not");
        // The ladder: the member held TRUE OFFLINE, then no longer silent while the view still lacks it.
        let mut t = crate::offline::OfflineTickle::new();
        let silent = |ids: &[&str]| ids.iter().map(|i| crate::offline::SilentNode { node_id: (*i).to_string(), path: rdm_core::PathName::new("mesh2.node", *i).unwrap(), incarnation: None, marked_at_ms: None }).collect::<Vec<_>>();
        let floor = 30_000u64;
        let mut report = crate::offline::OfflineTickReport::default();
        for (now, set) in [(0, silent(&[&id, &unknown])), (floor / 2, silent(&[&id, &unknown])), (floor / 2, silent(&[&id, &unknown])), (floor * 3 / 2, silent(&[&id, &unknown])), (floor * 3 / 2, silent(&[&id, &unknown])), (floor * 3 / 2 + 1, vec![])] {
            if set.is_empty() {
                assert!(t.is_offline(&id) && t.is_offline(&unknown), "both held TRUE OFFLINE first");
            }
            report = t
                .tick(
                    now,
                    true,
                    set,
                    |i: &str| heard.contains(i),
                    floor / 2,
                    |_| async { crate::offline::Ladder::Tickled(Ok(crate::offline::Tickle::Unreached(crate::offline::Unreached::of(crate::offline::ProbeEnd::Other { end: "NotSent", detail: "no answer".into() }, vec![crate::offline::PeerProbe { carrier: NodeId::mint(), end: crate::offline::ProbeEnd::Other { end: "NotSent", detail: "deadline".into() } }])))) },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await;
            tokio::task::yield_now().await;
        }
        assert_eq!(report.returned, vec![id.clone()], "heard again, it is returned: {report:?}");
        assert_eq!(report.gone, vec![unknown], "a node id the tick no longer watches is gone: {report:?}");
    }

    /// CONTRACT (node-rpc-envelope.md, "Node RPC readiness gate": "A node-admin waits until its Status
    /// authority is filled and its view holds it"): the admin's Node RPC door opens with the first view
    /// installed that holds its exact birth once the authority is filled, whichever task installs that
    /// view, so nothing that reads the installed view (the declarer that tells its primary it is ready
    /// among them) sees its own birth while its door still refuses. A view without the birth, one
    /// holding another birth of its name, and any view while the authority is not filled leave the
    /// door closed. Must NOT happen: the door opened only by the view-rebuild task, which starts after
    /// the declarer and a Build-log catch-up, so the admin declares itself ready to its primary while
    /// its router answers every state command NotReady (gate red at b24a4303,
    /// fabric_states__rounds trace 95adb727: mesh1.node-admin.2 declared Ready at 788604, its door
    /// opened at 788637, and mesh1.node-admin.1's three commit-state sends had all come back NotReady).
    #[tokio::test]
    async fn an_admins_door_opens_with_the_first_installed_view_that_holds_its_exact_birth() {
        let me = digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", MemberStatus::Ready);
        let primary = digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready);
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let filled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let records = Records::default();
        let status_door = filled.clone();
        let topology = RwLock::new(view(&[primary.clone()]));
        records.set_door(ServingDoor::new(ready.clone(), (me.node.node_id.clone(), me.node.incarnation.clone()), Box::new(move || status_door.load(std::sync::atomic::Ordering::SeqCst)), me.node.name.clone()), &*topology.read().await);
        let mut other = me.clone();
        other.node.incarnation = IncarnationId::mint();
        let opened = || ready.load(std::sync::atomic::Ordering::SeqCst);
        records.install_projected(&topology, || view(&[primary.clone(), me.clone()])).await;
        assert!(!opened(), "the authority is not filled: the door stays closed");
        filled.store(true, std::sync::atomic::Ordering::SeqCst);
        records.install_projected(&topology, || view(&[primary.clone()])).await;
        assert!(!opened(), "a view without this admin's birth leaves the door closed");
        records.install_projected(&topology, || view(&[primary.clone(), other.clone()])).await;
        assert!(!opened(), "another birth of its name is not this admin's birth");
        records.install_projected(&topology, || view(&[primary.clone(), me.clone()])).await;
        assert!(opened(), "the first installed view holding its exact birth, the authority filled, opens the door");
    }

    fn primary_of(d: &MeshDigest, revision: u64) -> rdm_core::Primary {
        rdm_core::Primary { mesh: d.node.mesh.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision }
    }

    /// CONTRACT (R-A2, the dw33-1 shape): the primary records an admin holds decide its projection. A
    /// ready admin with a lower NodeId that appears, in any mesh, changes no role; a view that has
    /// not been told who holds the roles (an entering admin) elects nobody; the primary proven
    /// exited, or replaced by a later birth that says so, hands the role to its own mesh.
    #[tokio::test]
    async fn the_projection_keeps_a_recorded_primary_and_elects_nobody_before_it_is_told() {
        use MemberStatus::*;
        use rdm_core::PrimaryRole::{FabricPrimary, MeshPrimary};
        let digest_view = DigestView::default();
        let primary = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "500000000000");
        let sibling = with_id(digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", Ready), "800000000000");
        for d in [&primary, &sibling] {
            digest_view.record((*d).clone());
        }
        digest_view.primaries.take(MeshPrimary, &primary_of(&primary, 1));
        digest_view.primaries.take(FabricPrimary, &primary_of(&primary, 1));
        let records = Records::default();
        let role_names = |t: &Topology| (t.fabric_primary().map(|n| n.name.clone()), t.node_type_primary("mesh1", "node-admin").map(|n| n.name.clone()));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(role_names(&t), (Some(PathName::new("mesh1.node-admin", "1").unwrap()), Some(PathName::new("mesh1.node-admin", "1").unwrap())));

        // A lower NodeId appears in another mesh and one in the incumbent mesh, both ready: nothing moves.
        digest_view.record(with_id(digest(PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin", Ready), "100000000000"));
        digest_view.record(with_id(digest(PathName::new("mesh1.node-admin", "3").unwrap(), "mesh1", "node-admin", Ready), "200000000000"));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(role_names(&t), (Some(PathName::new("mesh1.node-admin", "1").unwrap()), Some(PathName::new("mesh1.node-admin", "1").unwrap())), "a lower NodeId never displaces a living primary");
        assert_eq!(t.node_type_primary("mesh3", "node-admin").map(|n| n.name.clone()), Some(PathName::new("mesh3.node-admin", "1").unwrap()));

        // An entering admin has not been told who holds the roles: it computes none.
        records.set_entering(true);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert!(t.nodes.iter().all(|n| !n.is_primary && !n.is_fabric_primary), "no role before the entry read");
        records.set_entering(false);

        // Deleted: fabric recovery restores the fabric-primary (Q122): nobody, not mesh1's lowest Ready
        // admin and not mesh3's lower id, is fabric-primary or mesh1's primary meanwhile.
        let t = depart(&digest_view, &records, &primary).await;
        assert_eq!(role_names(&t), (None, None));
    }

    /// CONTRACT (R-A2; elections §3, Q122): a later birth that names the recorded primary's incarnation as
    /// the one it replaced proves that birth gone: the fabric-primary is fabric recovery's to restore, and
    /// nobody, not the lowest Ready admin of its mesh, holds either role meanwhile.
    #[test]
    fn a_later_birth_that_supersedes_the_primary_proves_it_gone() {
        use MemberStatus::*;
        use rdm_core::PrimaryRole::{FabricPrimary, MeshPrimary};
        let digest_view = DigestView::default();
        let primary = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "500000000000");
        digest_view.record(primary.clone());
        digest_view.primaries.take(MeshPrimary, &primary_of(&primary, 1));
        digest_view.primaries.take(FabricPrimary, &primary_of(&primary, 1));
        let mut recovered = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Pending), "900000000000");
        recovered.node.supersedes = Some(primary.node.incarnation.clone());
        digest_view.record(recovered.clone());
        let ready = with_id(digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", Ready), "800000000000");
        digest_view.record(ready);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        assert_eq!(t.fabric_primary().map(|n| n.name.clone()), None);
        assert_eq!(t.node_type_primary("mesh1", "node-admin").map(|n| n.name.clone()), None);
    }

    /// The fabric-primary `mesh1.node-admin.1` (both roles of mesh1) and `mesh2`'s births `mesh2`, the
    /// first in its role as mesh2's primary at revision 1: the fabric-primary's view, its records holding
    /// its node-rows writer over `storage`, and its view projected from both.
    async fn fabric_primary_over(mesh2: &[MeshDigest], storage: Arc<crate::storage::MemoryNodesStorage>) -> (MeshDigest, DigestView, Arc<Records>, Arc<RwLock<Topology>>, Arc<crate::node_rows::NodeRows>) {
        use rdm_core::PrimaryRole::{FabricPrimary, MeshPrimary};
        let fp = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), "100000000000");
        let digest_view = DigestView::default();
        digest_view.record(fp.clone());
        for d in mesh2 {
            digest_view.record(d.clone());
        }
        digest_view.primaries.take(FabricPrimary, &primary_of(&fp, 1));
        digest_view.primaries.take(MeshPrimary, &primary_of(&fp, 1));
        digest_view.primaries.take(MeshPrimary, &primary_of(&mesh2[0], 1));
        let records = Arc::new(Records::default());
        records.set_digest_view(digest_view.clone());
        let view = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records)));
        let rows = Arc::new(crate::node_rows::NodeRows::new(fp.node.name.clone(), fp.node.node_id.clone(), view.clone(), storage));
        records.set_rows(rows.clone());
        (fp, digest_view, records, view, rows)
    }

    /// `d`'s birth deleted (its node.delete's `NodeDeleted` heard: gossip.md §3.1, the birth is
    /// removed), the departure every projection and election reads as the birth gone; the installed
    /// view.
    async fn depart(digest_view: &DigestView, records: &Records, d: &MeshDigest) -> Topology {
        records.set_digest_view(digest_view.clone());
        digest_view.depart(rdm_core::LifecycleOp { r#type: d.node.r#type.clone(), mesh: d.node.mesh.clone(), build_id: "dbs8z9896j0f".into(), attempt: 1, operation: rdm_core::OperationId::node("node.delete", &d.node.node_id), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None });
        let view = RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, digest_view, records));
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, digest_view, records)).await
    }

    /// The exact Exited receipt of `d`'s birth, as a leave records it.
    fn exited_receipt(d: &MeshDigest) -> crate::deployment::pipeline::TerminalReceipt {
        crate::deployment::pipeline::TerminalReceipt { node: d.node.name.clone(), node_id: d.node.node_id.to_string(), incarnation_id: d.node.incarnation.to_string(), runtime: None, exit_code: Some(0) , verdict: None, data_dir: None }
    }

    /// CONTRACT (R-MD1 = A, i143-rulings-2026-10-11-dev.md: "The mesh-primary commits the mesh row
    /// Dead at mesh.deleted ... At mesh-left the fabric-primary writes the final primary's row
    /// Deleted, unchanged. Its ensure-deleted operation returns AlreadyApplied if the mesh-primary wrote
    /// Dead; only when no primary survives to make that write does the fabric-primary write Dead as
    /// acting primary ... Retries cause no second transition"; a mesh's status facts live on the mesh,
    /// `mesh.storage`): with mesh2's state deleted already on mesh2
    /// (its primary's write), mesh-left's Dead write answers AlreadyApplied, writes nothing, and its
    /// `rdm.node_admin.mesh.update.via-deleted-row` span says `already-applied` by `mesh-primary`; with
    /// none (no primary survived), the fabric-primary puts the row deleted among mesh2's status rows as
    /// acting primary (`applied`, `acting-primary`), and its replay answers AlreadyApplied. The Mesh
    /// row on mesh.storage follows the same rule (R-MN1 item 2, the name released): `already-deleted`
    /// over the mesh-primary's, else `deleted-written` once and `already-deleted` on the replay. No deleted
    /// row of mesh2 is ever put among the fabric-primary's own mesh's rows. Must NOT happen: a second
    /// transition over the mesh-primary's Dead (the fabric-primary deciding from what it applied
    /// itself, which holds nothing of mesh2), or the row written under the fabric-primary's mesh.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn mesh_lefts_dead_write_is_already_applied_over_the_mesh_primarys_dead_and_the_acting_primarys_when_none_survived() {
        use crate::storage::MeshStorage;
        let dead_on = async |meshes: &crate::storage::MemoryMeshStorage, m: &str| meshes.mesh(&mesh_id(m)).await.unwrap().is_some_and(|r| r.is_deleted());
        let key = crate::mesh_delete::MeshDeleteKey { mesh_id: mesh_id("mesh2"), build_id: crate::build::BuildId::parse("6n8kaqdncqwv").unwrap(), attempt: 1, operation: crate::mesh_delete::operation(&mesh_id("mesh2")) };
        for case in ["surviving-primary-wrote-deleted", "no-survivor"] {
            let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
            let last = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Leaving), "200000000000");
            let (_fp, digest_view, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage.clone()).await;
            crate::node_rows::write_record(&*storage, &crate::storage::NodeRow::new(last.node.name.clone(), last.node.mesh.clone(), &last.node.r#type, last.node.node_id.clone(), NodeStatus::Ready)).await.unwrap().unwrap();
            depart(&digest_view, &records, &last).await;
            records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records)).await;
            let meshes = crate::storage::MemoryMeshStorage::default();
            if case == "surviving-primary-wrote-deleted" {
                // mesh2's primary committed Dead at mesh.deleted on mesh2's record, the record deleted with
                // it (R-MD1 item 1; admin/leave.rs, mesh.deleted).
                crate::storage::write_mesh(&meshes, None, &crate::storage::MeshRecord { mesh_id: mesh_id("mesh2"), name: "mesh2".into(), state: rdm_core::status::MeshState::Deleted }).await.unwrap().unwrap();
            }
            let capture = crate::span_capture::Capture::new(&format!("i143-3068-r-md1-{case}"));
            {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                for _replay in 0..2 {
                    crate::mesh_delete::mesh_left_rows(records.node_rows().as_deref(), &meshes, &records.caches, &records.declared, "100000000000", &key, "mesh2", &[exited_receipt(&last)]).await.unwrap_or_else(|e| panic!("{case}: mesh-left's rows: {e}"));
                }
            }
            assert!(dead_on(&meshes, "mesh2").await, "{case}: mesh2's record stands Dead, its state Dead among its states");
            assert!(!dead_on(&meshes, "mesh1").await, "{case}: no Dead on the fabric-primary's own mesh's record");
            let spans = capture.named("rdm.node_admin.mesh.update.via-deleted-row");
            let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("writer").cloned().unwrap_or_default(), s.attributes.get("mesh_row").cloned().unwrap_or_default())).collect();
            let expected: Vec<(String, String, String)> = match case {
                "surviving-primary-wrote-deleted" => vec![("already-applied".into(), "mesh-primary".into(), "already-deleted".into()); 2],
                _ => vec![("applied".into(), "acting-primary".into(), "deleted-written".into()), ("already-applied".into(), "acting-primary".into(), "already-deleted".into())],
            };
            assert_eq!(said, expected, "{case}: one transition at most, and the span names who wrote the row: {spans:?}");
            eprintln!("{case}: span rdm.node_admin.mesh.update.via-deleted-row trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
        }
    }

    /// CONTRACT (R-MN1: a mesh removal deletes every roster birth, and a mesh created again under
    /// the name is a new mesh; mesh-delete.md step 7: at mesh-left the fabric-primary is the acting
    /// primary of a mesh that has none left, and deleted births resolve Gone; R14 item 2: a mesh's
    /// records are its primary's rows; mesh-delete.md "Rows": each member's row deleted by its
    /// node.delete): mesh-left takes the primaries of every birth its proof set names off its node's
    /// record (the address is the node's own part of the record, Q333), so neither the stored map the
    /// fabric-primary answers a recreated mesh's first admin with nor an election names any of them;
    /// another mesh's record is untouched. Must NOT happen:
    /// mesh_identity::canonical_ids at :208 (trace a6f24e5914ac3658d302079ddb0c28f0): the recreated
    /// mesh2.node-admin.1's entry sweep pings the removed mesh's births (via-entry-sweep-no-reply), handed by
    /// the maker's stored map from addresses nobody took off (rdm.mesh.topology.serve.via-read stored 1,
    /// stored_nodes 5).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn mesh_left_takes_the_primaries_of_every_birth_its_manifest_names_off_its_record() {
        use crate::storage::NodesStorage;
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let last = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Leaving), "200000000000");
        let member = with_id(digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Leaving), "210000000000");
        let (fp, digest_view, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage.clone()).await;
        // mesh2's primary wrote its members' primaries on their records while it was mesh2's primary, and
        // each node its own address; mesh1's own stays.
        for d in [&last, &member, &fp] {
            let mut row = crate::storage::NodeRow::new(d.node.name.clone(), d.node.mesh.clone(), &d.node.r#type, d.node.node_id.clone(), NodeStatus::Ready);
            row.birth_mut(&d.node.incarnation, &d.node.name).is_primary = Some(1);
            crate::node_rows::write_record(&*storage, &row).await.unwrap().unwrap();
            own_part(&*storage, d).await;
        }
        // Each member of mesh2 deleted by its node.delete (mesh-delete.md "Rows").
        for d in [&last, &member] {
            crate::node_rows::write_deleted(&*storage, &crate::node_rows::WrittenRows::default(), &d.node.name, &d.node.node_id).await.ok().expect("the member's Deleted record");
        }
        depart(&digest_view, &records, &last).await;
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records)).await;
        let key = crate::mesh_delete::MeshDeleteKey { mesh_id: mesh_id("mesh2"), build_id: crate::build::BuildId::parse("6n8kaqdncqwv").unwrap(), attempt: 1, operation: crate::mesh_delete::operation(&mesh_id("mesh2")) };
        let capture = crate::span_capture::Capture::new("i143-3068-mesh-left-contacts");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            crate::mesh_delete::mesh_left_rows(records.node_rows().as_deref(), &crate::storage::MemoryMeshStorage::default(), &records.caches, &records.declared, "100000000000", &key, "mesh2", &[exited_receipt(&last), exited_receipt(&member)]).await.unwrap_or_else(|e| panic!("mesh-left's rows: {e}"));
        }
        let meshes = crate::storage::MemoryMeshStorage::default();
        assert!(rows_writer::recorded_births(&meshes, &*storage, "mesh2").await.unwrap().is_empty(), "no address of the removed mesh's births stands");
        assert_eq!(rows_writer::take_recorded_primaries(&meshes, &*storage, "mesh2", &DigestView::default().primaries).await.unwrap(), 0, "nor any primary of them");
        let mesh1: Vec<PathName> = rows_writer::recorded_births(&meshes, &*storage, "mesh1").await.unwrap().into_iter().map(|b| b.name.clone()).collect();
        assert_eq!(mesh1, vec![PathName::new("mesh1.node-admin", "1").unwrap()], "another mesh's record is untouched");
        assert!(storage.node(&last.node.node_id).await.unwrap().is_some_and(|r| r.birth(&last.node.incarnation).is_some()), "the record keeps the birth's other facts");
        let spans = capture.named("rdm.node_admin.node.remove.via-mesh-left-contact");
        assert_eq!(spans.len(), 1, "{spans:?}");
        let dropped = spans[0].attributes.get("dropped").cloned().unwrap_or_default();
        assert!(dropped.contains(last.node.node_id.as_str()) && dropped.contains(member.node.node_id.as_str()), "the span names each birth whose primaries are taken off, by node id: {dropped}");
        eprintln!("span rdm.node_admin.node.remove.via-mesh-left-contact trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
    }

    /// CONTRACT (mesh-delete.md, "Resume after the primary's loss", steps 1-2: the lost final
    /// primary's exact Exited is proven and recorded first, then the fabric-primary creates the one
    /// node-admin of a mesh with no live admin; R11: record predecessor Exited first): the proven
    /// exit, recorded as its exited-verdict row and read by the install `admin_for_deleting_mesh` runs
    /// before the create, vacates the lost primary's role, so the fabric-primary is the writer of the mesh's
    /// rows and its row of the created admin is written. Must NOT happen: the refusal of
    /// mesh_lifecycle::mesh_delete_no_survivor on 8803f84e (AllocateIdentity refused the birth:
    /// "mesh1.node-admin.1 is not the writer of mesh2's node rows").
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn the_lost_only_admins_exit_held_admits_the_fabric_primarys_row_for_the_admin_it_creates() {
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let lost = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), "200000000000");
        let (_fp, digest_view, records, view, rows) = fabric_primary_over(std::slice::from_ref(&lost), storage.clone()).await;
        let capture = crate::span_capture::Capture::new("i143-3068-leaving-mesh-admin-row");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let created: PathName = rdm_core::PathName::new("mesh2.node-admin", "2").unwrap();
        let refused = rows.create_row(&created, "mesh2", "node-admin", &NodeId::mint(), &rdm_core::OperationId::location("node.create", &created), None, None, crate::node_rows::RowFor::Create).await;
        assert!(matches!(&refused, Err(crate::node_rows::RowRefused::NotMeshPrimary(_))), "unheld, the lost primary still writes mesh2's rows: {refused:?}");
        depart(&digest_view, &records, &lost).await;
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records)).await;
        let born = NodeId::mint();
        match rows.create_row(&created, "mesh2", "node-admin", &born, &rdm_core::OperationId::location("node.create", &created), None, None, crate::node_rows::RowFor::Create).await {
            Ok(crate::node_rows::RowCreated::Written { row, .. }) => assert_eq!((row.node_id, row.status), (born, NodeStatus::Starting), "a create writes the record starting"),
            other => panic!("the fabric-primary writes the created admin's row once the lost primary's exit is held: {other:?}"),
        }
        let mut outcomes: Vec<String> = capture.named("rdm.node_admin.node.create.via-node-row").iter().filter(|s| s.attributes.get("moment").map(String::as_str) == Some("create")).map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        outcomes.dedup();
        assert_eq!(outcomes, ["not-mesh-primary", "row-written"], "{outcomes:?}");
        let written = capture.named("rdm.node_admin.node.create.via-node-row").pop().unwrap();
        eprintln!("span rdm.node_admin.node.create.via-node-row trace_id={} {:?}", written.trace_id, written.attributes);
    }

    /// CONTRACT (mesh-delete.md, "Resume after the primary's loss": the workflow goes to the admin
    /// in its role; gossip.md §3.1 `NodeDeleted`): mesh2's primary `mesh2.node-admin.1` is lost and
    /// `mesh2.node-admin.2` is live. Once the lost primary's node.delete is heard, the role is
    /// `mesh2.node-admin.2`'s in the fabric-primary's view (the leave a later attempt resumes hands it the
    /// workflow) and in its own, which hears the same departure and announces the role at the next
    /// revision. Must NOT happen: the no_survivor attempts refused "mesh2.node-admin.2 is live and holds no
    /// role yet" after mesh2.node-admin.1 was deleted (trace 5bc960a3e759a77cfb42ae6e9bc687d0).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn the_lost_primarys_departure_makes_the_live_admin_primary_in_the_fabric_primarys_view_and_its_own() {
        use rdm_core::PrimaryRole::MeshPrimary;
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let lost = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), "200000000000");
        let live = with_id(digest(PathName::new("mesh2.node-admin", "2").unwrap(), "mesh2", "node-admin", MemberStatus::Ready), "300000000000");
        let (_fp, digest_view, records, view, _rows) = fabric_primary_over(&[lost.clone(), live.clone()], storage).await;
        let primary = |t: &Topology| t.node_type_primary("mesh2", "node-admin").map(|n| n.name.clone());
        assert_eq!(primary(&*view.read().await), Some(PathName::new("mesh2.node-admin", "1").unwrap()), "unheld, the lost primary keeps its role (R-EL1)");
        depart(&digest_view, &records, &lost).await;
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records)).await;
        assert_eq!(primary(&*view.read().await), Some(PathName::new("mesh2.node-admin", "2").unwrap()), "held, the fabric-primary reads the live admin primary");
        // mesh2.node-admin.2's own view: the same departure, heard by its own digest view.
        let own = Records::default();
        own.set_digest_view(digest_view.clone());
        let view2 = RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &own));
        own.install_projected(&view2, || project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &own)).await;
        let decided = incumbency_of(&digest_view, &own);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &own, &decided, &ViewReads::none_at(std::time::Instant::now()));
        assert_eq!(primary(&t), Some(PathName::new("mesh2.node-admin", "2").unwrap()), "mesh2.node-admin.2 reads itself primary");
        let said = role_announcements(&t, "mesh2", (&live.node.node_id, &live.node.incarnation), &decided);
        assert!(said.iter().any(|(role, h)| *role == MeshPrimary && h.mesh == "mesh2" && h.is_birth(&live.node.node_id, &live.node.incarnation) && h.revision == 2), "mesh2.node-admin.2 announces mesh2's role at the next revision: {said:?}");
    }

    /// CONTRACT (R-EL1, fabric-node-lifecycle-elections.md §5 "a living primary keeps its role"): the
    /// view's readers name the primary the records this admin holds give, whatever its status. A primary that is Leaving (connections-deleted and stopped by its
    /// Build) whose yield this observer has not heard yet keeps both roles; once the yield is held
    /// (node.primary.updated: both records at the next revision naming the successor), the readers name
    /// the successor. Must NOT happen: an observer reading a Leaving primary as no primary (an
    /// election with no winner while the record names P).
    #[test]
    fn a_leaving_primary_stays_both_primaries_in_the_readers_until_its_hand_over_is_held() {
        use MemberStatus::*;
        use rdm_core::PrimaryRole::{FabricPrimary, MeshPrimary};
        let digest_view = DigestView::default();
        let records = Records::default();
        let primary = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Leaving), "500000000000");
        let successor = with_id(digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", Ready), "800000000000");
        let other = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "300000000000");
        for d in [&primary, &successor, &other] {
            digest_view.record((*d).clone());
        }
        digest_view.primaries.take(MeshPrimary, &primary_of(&primary, 1));
        digest_view.primaries.take(FabricPrimary, &primary_of(&primary, 1));
        digest_view.primaries.take(MeshPrimary, &primary_of(&other, 1));
        let role_names = |t: &Topology| (t.fabric_primary().map(|n| n.name.clone()), t.node_type_primary("mesh1", "node-admin").map(|n| n.name.clone()));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        let flags = t.nodes.iter().find(|n| n.name == PathName::new("mesh1.node-admin", "1").unwrap()).map(|n| (n.status, n.is_primary, n.is_fabric_primary));
        assert_eq!(flags, Some((NodeStatus::Leaving, true, true)), "the election keeps the Leaving primary in its role (R-EL1)");
        assert_eq!(role_names(&t), (Some(PathName::new("mesh1.node-admin", "1").unwrap()), Some(PathName::new("mesh1.node-admin", "1").unwrap())), "the readers name the Leaving primary until its yield is held");
        assert!(t.is_fabric_primary(&primary.node.node_id) && t.is_mesh_primary("mesh1", &primary.node.node_id));
        assert_eq!(t.mesh_view("mesh1").and_then(|m| m.primary_admin), Some(primary.node.node_id.clone()));

        // The yield is held: PrimaryUpdated's records name the successor at the next revisions.
        digest_view.primaries.take(MeshPrimary, &primary_of(&successor, 2));
        digest_view.primaries.take(FabricPrimary, &primary_of(&successor, 2));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(role_names(&t), (Some(PathName::new("mesh1.node-admin", "2").unwrap()), Some(PathName::new("mesh1.node-admin", "2").unwrap())), "the readers name the successor once the yield is held");
        assert!(!t.is_fabric_primary(&primary.node.node_id) && !t.is_mesh_primary("mesh1", &primary.node.node_id));
    }

    /// CONTRACT (fabric-node-lifecycle-elections.md §4.4, lifecycles/node-primary-update.md step 3): the
    /// committed next-revision primary record is the resolver's input and the incumbent yields fabric
    /// authority. A hierarchy-loop decision is announced against the primary record it was decided
    /// from: a committed handover that lands while the loop awaits (its change) is not
    /// answered by the incumbent taking the role back at the next revision. Must NOT happen: the
    /// yielded incumbent announcing itself over the committed successor (the e3..e12 role flap of
    /// the fabric-primary handover stem on f06c8197).
    #[tokio::test]
    async fn a_handover_committed_while_the_loop_awaits_is_not_taken_back_by_the_incumbent() {
        use MemberStatus::*;
        use rdm_core::PrimaryRole::{FabricPrimary, MeshPrimary};
        let digest_view = DigestView::default();
        let records = Records::default();
        let me = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "500000000000");
        let sibling = with_id(digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", Ready), "800000000000");
        let successor = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "300000000000");
        for d in [&me, &sibling, &successor] {
            digest_view.record((*d).clone());
        }
        digest_view.primaries.take(MeshPrimary, &primary_of(&me, 1));
        digest_view.primaries.take(MeshPrimary, &primary_of(&successor, 1));
        digest_view.primaries.take(FabricPrimary, &primary_of(&me, 1));
        let me_birth = (&me.node.node_id, &me.node.incarnation);
        // The loop decides: this admin holds both of its roles, and their records already name it.
        let decided = incumbency_of(&digest_view, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        assert_eq!(t.fabric_primary().map(|n| n.name.clone()), Some(PathName::new("mesh1.node-admin", "1").unwrap()));
        // While the loop awaits its change, the committed transfer lands: mesh2.node-admin.1 at revision 2.
        digest_view.primaries.take(FabricPrimary, &primary_of(&successor, 2));
        let said = role_announcements(&t, "mesh1", me_birth, &decided);
        assert!(said.is_empty(), "the yielded incumbent announces nothing over the committed successor: {said:?} (decided against {:?})", decided.fabric);

        // A legitimate succession is still announced: the recorded mesh-primary's birth deleted (the
        // fabric-primary, heard in another mesh, is not it), the election names its own mesh's next admin,
        // at the next revision over that record.
        let digest_view = DigestView::default();
        let records = Records::default();
        let gone = with_id(digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", Ready), "100000000000");
        let fabric = with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "300000000000");
        for d in [&me, &gone, &fabric] {
            digest_view.record((*d).clone());
        }
        digest_view.primaries.take(MeshPrimary, &primary_of(&gone, 5));
        digest_view.primaries.take(MeshPrimary, &primary_of(&fabric, 1));
        digest_view.primaries.take(FabricPrimary, &primary_of(&fabric, 1));
        depart(&digest_view, &records, &gone).await;
        let decided = incumbency_of(&digest_view, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        let said = role_announcements(&t, "mesh1", me_birth, &decided);
        let revisions: Vec<_> = said.iter().map(|(s, h)| (s.clone(), h.node_id.clone(), h.revision)).collect();
        assert_eq!(revisions, vec![(MeshPrimary, me.node.node_id.clone(), 6)], "the successor of a deleted mesh-primary is its mesh's primary at the next revision (decided against {:?})", decided.meshes);
    }

    /// CONTRACT (lifecycles/node-primary-update.md "Writer": "for every other type the mesh-primary
    /// records the node's row"; the mesh-primary is the sole author of member status): a mesh-primary announces the role of each other node type of its mesh the election fills
    /// with a birth its record does not name (a first election, a primary proven gone), at the next
    /// revision over that record, and nothing while the record names the living primary. No other admin
    /// announces a node-type primary role. Must NOT happen: an rpc node-type primary role that no record holds, so a
    /// lower NodeId that later becomes Ready displaces the living primary.
    #[tokio::test]
    async fn a_mesh_primary_announces_each_node_type_primary_the_election_fills_in_its_mesh() {
        use MemberStatus::*;
        use rdm_core::PrimaryRole::{FabricPrimary, MeshPrimary, NodePrimary};
        let digest_view = DigestView::default();
        let records = Records::default();
        let me = with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "500000000000");
        let sibling = with_id(digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", Ready), "800000000000");
        let rpc1 = with_id(digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Ready), "100000000000");
        let rpc2 = with_id(digest(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", Ready), "300000000000");
        for d in [&me, &sibling, &rpc1, &rpc2] {
            digest_view.record((*d).clone());
        }
        digest_view.primaries.take(MeshPrimary, &primary_of(&me, 1));
        digest_view.primaries.take(FabricPrimary, &primary_of(&me, 1));
        let me_birth = (&me.node.node_id, &me.node.incarnation);
        let node_type_said = |said: &[(rdm_core::PrimaryRole, rdm_core::Primary)]| said.iter().filter(|(s, _)| matches!(s, NodePrimary(_))).map(|(s, h)| (s.clone(), h.node_id.to_string(), h.revision)).collect::<Vec<_>>();
        // A first election: no record of the rpc node type, the lowest Ready fills it at revision 1.
        let decided = incumbency_of(&digest_view, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        let said = role_announcements(&t, "mesh1", me_birth, &decided);
        assert_eq!(node_type_said(&said), vec![(NodePrimary("node".into()), rpc1.node.node_id.to_string(), 1)], "the mesh-primary roles mesh1.node.1: {said:?}");
        let sib = role_announcements(&t, "mesh1", (&sibling.node.node_id, &sibling.node.incarnation), &decided);
        assert!(node_type_said(&sib).is_empty(), "an admin that is not the mesh-primary announces no node-type primary role: {sib:?}");
        // The record names the living mesh1.node.2: it keeps the role over the lower Ready NodeId and nothing is said.
        digest_view.primaries.take(NodePrimary("node".into()), &primary_of(&rpc2, 3));
        let decided = incumbency_of(&digest_view, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        assert_eq!(t.node_type_primary("mesh1", "node").map(|n| n.name.clone()), Some(PathName::new("mesh1.node", "2").unwrap()));
        assert!(node_type_said(&role_announcements(&t, "mesh1", me_birth, &decided)).is_empty(), "a living recorded primary is not announced again");
        // mesh1.node.2 proven exited: the role goes to the lowest Ready, announced at the next revision.
        depart(&digest_view, &records, &rpc2).await;
        let decided = incumbency_of(&digest_view, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        let said = role_announcements(&t, "mesh1", me_birth, &decided);
        assert_eq!(node_type_said(&said), vec![(NodePrimary("node".into()), rpc1.node.node_id.to_string(), 4)], "{said:?}");
    }

    #[test]
    fn the_projection_elects_the_lowest_ready_node_id_per_node_type_and_across_mesh_primaries() {
        use MemberStatus::*;
        let t = view(&[
            with_id(digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready), "200000000000"),
            with_id(digest(PathName::new("mesh2.node", "2").unwrap(), "mesh2", "node", Ready), "700000000000"),
            with_id(digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", Pending), "000000000001"),
            with_id(digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", Ready), "300000000000"),
            with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Ready), "400000000000"),
            with_id(digest(PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", Ready), "500000000000"),
        ]);
        let mut primaries: Vec<PathName> = t.nodes.iter().filter(|n| n.is_primary).map(|n| n.name.clone()).collect();
        primaries.sort();
        assert_eq!(primaries, [PathName::new("mesh1.node", "3").unwrap(), PathName::new("mesh1.node-admin", "2").unwrap(), PathName::new("mesh2.node", "2").unwrap(), PathName::new("mesh2.node-admin", "1").unwrap()], "a pending member is never primary");
        assert_eq!(t.fabric_primary().map(|n| n.name.clone()), Some(PathName::new("mesh2.node-admin", "1").unwrap()), "not the lowest-named mesh");
        assert!(t.violations().is_empty(), "{:?}", t.violations());
        assert_eq!(t.mesh_view("mesh2").unwrap().id, Some(mesh_id("mesh2")));
        assert_eq!(t.fabric.status, ScopeStatus::Pending, "a fabric-primary existing does not make the fabric ready: the view carries the held fabric state");
    }

    #[test]
    fn ready_since_and_ordinal_never_change_a_winner() {
        // The ready time no digest carries any more (Q203: a digest carries no config map).
        let at = |name: PathName, mesh: &str, r#type: &str, id: &str, _since: u64| with_id(digest(name, mesh, r#type, MemberStatus::Ready), id);
        // rpc.3 has the highest ordinal and the latest ready time, and the lowest id.
        let a = view(&[at(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "100000000000", 10), at(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", "900000000000", 1), at(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", "800000000000", 2), at(PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", "200000000000", 900)]);
        let b = view(&[at(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", "100000000000", 999), at(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", "900000000000", 900), at(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", "800000000000", 800), at(PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", "200000000000", 1)]);
        for t in [a, b] {
            let mut primaries: Vec<PathName> = t.nodes.iter().filter(|n| n.is_primary).map(|n| n.name.clone()).collect();
            primaries.sort();
            assert_eq!(primaries, [PathName::new("mesh1.node", "3").unwrap(), PathName::new("mesh1.node-admin", "1").unwrap()]);
        }
    }

    #[test]
    fn an_admin_can_take_over_only_when_every_held_birth_is_adoptable_here() {
        use crate::deployment::process::ProcessDeploymentProvider;
        use crate::deployment::provider::DeploymentProvider;
        use rdm_core::{RuntimeLocator, RuntimeProvider};
        let p = ProcessDeploymentProvider::new();
        let with = |name: PathName, mesh: &str, r#type: &str, domain: Option<String>| {
            let mut d = digest(name, mesh, r#type, MemberStatus::Ready);
            d.node.runtime = domain.map(|control_domain| rdm_core::RuntimeFact {
                deployment_id: rdm_core::fixture_crockford60("dep"),
                provider: RuntimeProvider::Process,
                control_domain,
                locator: RuntimeLocator::Process { pid: 4242, start: 17 },
            });
            d
        };
        let here = p.control_domain();
        assert!(take_over_blockers(&[with(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Some(here.clone())), with(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", Some(here.clone()))], &p).is_empty());
        let (unpublished, elsewhere) = (with(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", None), with(PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", Some("process:another-host:1".into())));
        let blocked = take_over_blockers(&[with(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Some(here.clone())), unpublished.clone(), elsewhere.clone()], &p);
        assert_eq!(blocked.len(), 2, "{blocked:?}");
        assert!(blocked[0].starts_with(&format!("{}: publishes no runtime fact", unpublished.node.node_id)), "{blocked:?}");
        assert!(blocked[1].starts_with(&format!("{}:", elsewhere.node.node_id)) && blocked[1].contains("control domain"), "{blocked:?}");
    }

    /// CONTRACT (i143 export gate: normal joining or restarted admins cannot self-Ready; only the
    /// Day-0 root may apply its own mesh's Pending): a mesh's first admin that is not Day 0 is held
    /// by name until the fabric primary applies Pending; an admin joining a mesh whose node-admins are live owes
    /// nothing; Day 0 alone self-applies; an applied Pending holds no one.
    #[test]
    fn only_the_day0_root_applies_its_own_meshs_pending() {
        let me: PathName = rdm_core::PathName::new("mesh2.node-admin", "1").unwrap();
        assert_eq!(pending_gate(&me, "mesh2", true, false, false), PendingGate::SelfApply, "Day 0 has no upstream authority");
        match pending_gate(&me, "mesh2", false, false, false) {
            PendingGate::Blocked(why) => assert!(why.contains("not been applied here by the fabric primary"), "{why}"),
            other => panic!("a first admin that is not Day 0 never self-applies: {other:?}"),
        }
        assert_eq!(pending_gate(&me, "mesh2", false, false, true), PendingGate::Clear, "an admin entering an existing mesh owes no Pending, its node type live or not");
        assert_eq!(pending_gate(&me, "mesh2", false, true, false), PendingGate::Clear);
        assert_eq!(pending_gate(&me, "mesh2", true, true, false), PendingGate::Clear, "Day 0 applies it once");
    }

    /// CONTRACT (R-BI3: the current Build is read from the Build entities; node-start.md step 4, the
    /// Build floor): an admin that holds no current Build, or not the facts of the Build its entry
    /// named at the attempt the answering admin held, is blocked from Ready, each reason named with
    /// the request that recovers it. Day 0 (no entry) owes no floor.
    #[tokio::test]
    async fn an_admin_without_a_current_build_can_never_take_over() {
        use crate::build_state::{AttemptOpened, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildStateAdapter as _, MemoryBuildStateAdapter};
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        let builds = MemoryBuildStateAdapter::new();
        let blocked = node_admin_not_ready(&me, &builds, None).await.expect("blocked while it holds no Build");
        assert_eq!(blocked.kind(), "no-current-build");
        assert!(blocked.to_string().starts_with(&format!("{me:?}: holds no current Build")), "{blocked}");
        let id = crate::build::BuildId::parse("7wy02yhavgde").unwrap();
        let blocked = node_admin_not_ready(&me, &builds, Some((id.clone(), 2))).await.expect("blocked while the entry's Build is not held");
        assert_eq!(blocked.need(), crate::fetch_build::Need::BuildFacts(id.clone()), "{blocked}");
        assert!(blocked.to_string().contains(&format!("holds no facts of Build {id}")), "{blocked}");
        builds.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        assert_eq!(node_admin_not_ready(&me, &builds, None).await, None, "an admin served no entry owes no attempt floor");
        let blocked = node_admin_not_ready(&me, &builds, Some((id.clone(), 2))).await.expect("blocked below the entry's attempt").to_string();
        assert!(blocked.contains(&format!("holds attempt 0 of Build {id}")) && blocked.contains("held attempt 2"), "{blocked}");
        for attempt in 1..=2 {
            if attempt > 1 {
                builds.open_attempt(&AttemptOpened { mesh: None, build_id: id.clone(), attempt, reason: AttemptReason::Stop, action: None, opened_by: rdm_core::NodeId::parse("400000000000").unwrap(), opened_at_ms: 0, context: None }).await.unwrap();
            }
            builds.claim_attempt(&BuildAttemptClaim { claimed_mesh: None, executor_mesh: "mesh1".into(), claimant: None, mesh: None, build_id: id.clone(), attempt, executor: fixture_actor(false) }).await.unwrap();
        }
        assert_eq!(node_admin_not_ready(&me, &builds, Some((id.clone(), 2))).await, None, "its attempt facts reached the entry's");
        let other = crate::build::BuildId::parse("qsmy968wj1zt").unwrap();
        let blocked = node_admin_not_ready(&me, &builds, Some((other.clone(), 9))).await.expect("a floor naming a Build not held blocks until its facts are held");
        assert_eq!(blocked.need(), crate::fetch_build::Need::BuildFacts(other), "{blocked}");
    }

    /// CONTRACT (R-S2): the declarer is woken by a view that changed and by the declare gate
    /// opening or closing; writing the same view, or setting the gate to the value it holds,
    /// wakes nothing (the 200 ms projection write is not a timer in disguise).
    #[tokio::test]
    async fn a_changed_view_or_gate_wakes_the_declarer_and_a_repeat_does_not() {
        let records = Records::default();
        let quiet = |records: &Records| {
            let wake = records.wake.clone();
            async move { tokio::time::timeout(Duration::from_millis(150), wake.woken()).await.is_err() }
        };
        let topology = RwLock::new(view(&[digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready)]));
        let same = topology.read().await.clone();
        records.install_projected(&topology, || same.clone()).await;
        assert!(quiet(&records).await, "the same view wakes nothing");
        let changed = view(&[digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready)]);
        records.install_projected(&topology, || changed.clone()).await;
        assert!(!quiet(&records).await, "a changed view wakes the declarer");
        records.set_declare_gate(true);
        assert!(!quiet(&records).await, "the gate opening wakes it");
        records.set_declare_gate(true);
        assert!(quiet(&records).await, "the gate set to the value it holds wakes nothing");
        records.set_declare_gate(false);
        assert!(!quiet(&records).await, "the gate closing wakes it");
    }

    /// CONTRACT (R-J1 step 5): a node-admin the fabric-primary admitted over `JoinNode` is held by
    /// the fabric-primary's status authority when the join is answered, before any gossip carries its
    /// digest. Before the join its declaration names a subject the authority does not hold
    /// (`SubjectUnknown`); after it, its first declaration (`Ready`) is `Applied`. Nothing
    /// checks who is calling (node-rpc.md §14): the same declaration from an endpoint that never
    /// joined is decided for the subject it names.
    #[tokio::test]
    async fn an_admitted_node_admin_is_a_held_subject_before_its_digest_is_gossiped() {
        use crate::join::{Deployed, JoinDoor, Joins};
        use crate::status_rpc::{Declared, StatusDoor};
        use rdm_core::status::{NodeState, NotPrimary, StatusReply, StatusRequest};
        use rdm_core::join::{JoinReply, JoinRequest};
        let digest_view = DigestView::default();
        let records = Arc::new(Records::default());
        digest_view.record(with_id(digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", MemberStatus::Ready), "100000000000"));
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records)));
        let me = topology.read().await.fabric_primary().expect("admin.1 holds the fabric-primary role").clone();
        let status_door = StatusDoor {
            certs: None,
            app_channels: None,
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: me.name.clone(),
            node_id: me.node_id.clone(),
            fabric_id: fabric1(),
            topology: topology.clone(),
            declared: records.declared.clone(),
            meshes_storage: Arc::new(crate::storage::MemoryMeshStorage::default()),
            fabric_storage: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            caches: Default::default(),
            mesh_ids: Arc::new(BTreeMap::new),
            durable: Default::default(),
            commands: Arc::new(crate::node_commands::CommandView::default()),
            own: Arc::new(std::sync::OnceLock::new()),
            mesh_deleter: Arc::new(std::sync::OnceLock::new()),
            storage_changes: Default::default(), hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: records.wake.clone(),
            fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
            configs: Default::default(),
            republish: Default::default(),
            pre_node_ready: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            digest_view: None,
            unreachable: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        let _ = Declared::default();

        // The birth this admin deployed, and the digest it reports on its first call.
        let runtime = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-admitted")).unwrap();
        let mut reported = with_id(digest(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", MemberStatus::Pending), "200000000000");
        reported.node.runtime = Some(runtime.clone());
        reported.data_dir = Some("/data/mesh1.node-admin.2".into());
        let deployed = Deployed { mesh: reported.node.mesh.clone(), r#type: reported.node.r#type.clone(),
            name: reported.node.name.clone(),
            node_id: reported.node.node_id.clone(),
            incarnation: reported.node.incarnation.clone(),
            supersedes: None,
            endpoint_id: reported.node.endpoint_id.clone(),
            runtime,
            data_dir: "/data/mesh1.node-admin.2".into(),
            last_accepted_name: None,
            held: false,
            listeners: Vec::new(),
        };
        let joins = Arc::new(Joins::default());
        let _wait = joins.expect(deployed);
        let installed = digest_view.clone();
        let door = JoinDoor {
            nodes: None,
            me: me.name.clone(),
            joins,
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(crate::wire::JoinAnswer {
                        served_by: NodeId::parse("100000000000").unwrap(),
                        control: crate::wire::JoinControl { cert_policy: None, provider: ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None },
                        statuses: vec![],
                    })
                })
            }),
            types: Arc::new(|_, _| Box::pin(async { Ok(()) })),
            install: Arc::new(move |d: &MeshDigest| {
                installed.record(d.clone());
            }),
            known: view_projector(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, digest_view.clone(), records.clone()),
            pending: Arc::new(|_, _| Box::pin(async {})),
            primary: Arc::new(|| None),
            atlas: None,
            issuer: Arc::new(crate::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let endpoint = reported.node.endpoint_id.clone();
        assert!(status_door.sender_of(&endpoint).await.is_none(), "before the join the authority's view does not hold the birth");
        let declare = StatusRequest::DeclareNodeState { node_id: reported.node.node_id.clone(), incarnation: reported.node.incarnation.clone(), state: NodeState::Ready, operation: None };
        assert_eq!(status_door.apply(None, &declare).await, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "before the join the subject is not held");
        let joined = door.serve(endpoint.clone(), JoinRequest::JoinNode { types: Vec::new(), digest: (&reported).into(), listeners: Vec::new() }).await;
        assert!(matches!(joined, JoinReply::Joined { .. }), "{joined:?}");

        // Acceptance made the birth known; no projection tick ran since.
        let sender = status_door.sender_of(&endpoint).await.expect("the admitted birth is known when its join is answered");
        assert_eq!(status_door.apply(Some(sender), &declare).await, StatusReply::Applied);

        // An endpoint that never joined sends the same declaration: decided for the subject it names.
        assert!(status_door.sender_of(&EndpointId("key-never-joined".into())).await.is_none());
        assert_eq!(status_door.apply(None, &declare).await, StatusReply::AlreadyApplied);
    }

    #[test]
    fn a_mesh_without_a_ready_admin_cedes_the_fabric_and_stays_pending() {
        use MemberStatus::*;
        let t = view(&[digest(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", ConnectionsDeleting), digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", Ready), digest(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", Ready)]);
        assert_eq!(t.fabric_primary().map(|n| n.name.clone()), Some(PathName::new("mesh2.node-admin", "1").unwrap()));
        assert_eq!(t.meshes.iter().find(|m| m.name == "mesh1").unwrap().status, ScopeStatus::Pending);
        let empty = view(&[]);
        assert!(empty.fabric_primary().is_none());
        assert_eq!(empty.fabric.status, ScopeStatus::Pending);
    }

    /// CONTRACT (CLAUDE.md read rule: a kind with no projection is a point read through its store
    /// index; R12/R13: the mesh-primary writes the row, a stopped node is told nothing): `mesh1.node.1`
    /// is stopped and its mesh-primary moves its row to `mesh1.node.1-old` on the fabric's shared store
    /// root. Another admin, which performed no rename and holds nothing of it, lists the stopped birth
    /// at the row's name: its view reads the name through the row's `(path, name)` key. What must NOT
    /// happen: that admin listing the birth at `mesh1.node.1`, the name its digest last published, so a
    /// start or a `node.get` there names a row that no longer holds it.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_stopped_birth_whose_row_another_admin_moved_is_listed_at_the_rows_name() {
        let d = crate::test_support::tempdir("row-name-view");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let primary = crate::storage::FileNodesStorage::open(&d.join("100000000000"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        let mine: Arc<dyn crate::storage::NodesStorage> = Arc::new(crate::storage::FileNodesStorage::open(&d.join("200000000000"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap());
        let digest_view = DigestView::default();
        let held = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        digest_view.record(held.clone());
        assert!(digest_view.stopped(rdm_core::LifecycleOp { r#type: held.node.r#type.clone(), mesh: held.node.mesh.clone(),
            build_id: "b1".into(),
            attempt: 1,
            operation: rdm_core::OperationId::location("node.stop", &rdm_core::PathName::new("mesh1.node", "1").unwrap()),
            node_id: held.node.node_id.clone(),
            incarnation: held.node.incarnation.clone(),
            name: held.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        }));
        // The mesh-primary's rename on the shared root, through the one writer of a row's move.
        let to: PathName = held.node.name.renamed().unwrap();
        let mut record = Node::allocated(held.node.name.clone(), held.node.mesh.clone(), &held.node.r#type);
        record.node_id = held.node.node_id.clone();
        record.incarnation_id = Some(held.node.incarnation.clone());
        record.status = NodeStatus::Leaving;
        let row = crate::storage::NodeRow { replacement_from: None, r#type: held.node.r#type.clone(), mesh: held.node.mesh.clone(), initial_properties: None, topology_storage_provider: None, name: held.node.name.clone(), node_id: held.node.node_id.clone(), status: NodeStatus::Leaving, allocated: None, renamed_from: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, writer: None };
        crate::node_rows::write_record(&primary, &row).await.unwrap().unwrap();
        crate::node_rows::move_row(&primary, &Default::default(), &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), &record, &held.node.name, &to).await.unwrap();
        // This admin: the same view, its own store on the same root, no rename of its own.
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(mine);
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestView::default(), &Records::default())));
        view_projector(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, digest_view.clone(), records.clone())().await;
        let t = topology.read().await.clone();
        let names: Vec<PathName> = t.nodes.iter().map(|n| n.name.clone()).collect();
        let n = t.node(&to).unwrap_or_else(|| panic!("the stopped birth is listed at its row's name: {names:?}"));
        assert_eq!((n.node_id.clone(), n.incarnation_id.clone(), n.status, n.stopped), (held.node.node_id.clone(), Some(held.node.incarnation.clone()), NodeStatus::Leaving, true));
        assert!(t.node(&held.node.name).is_none(), "nothing is listed at the name the row moved from: {names:?}");
        // The view's held stopped digest keeps the name the node last published (R-JN1's last accepted
        // name): only the view names the birth by its row.
        assert_eq!(digest_view.stopped_digests().iter().map(|d| d.node.name.clone()).collect::<Vec<_>>(), vec![held.node.name.clone()]);
        // The row moved back (node-replace.md Rollback): the next projection reads it there.
        crate::node_rows::move_row(&primary, &Default::default(), &rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), &record, &to, &held.node.name).await.unwrap();
        view_projector(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, digest_view.clone(), records.clone())().await;
        let t = topology.read().await.clone();
        assert!(t.node(&held.node.name).is_some_and(|n| n.node_id == held.node.node_id && n.stopped) && t.node(&to).is_none(), "{:?}", t.nodes.iter().map(|n| n.name.clone()).collect::<Vec<_>>());
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (CLAUDE.md error rule: "never written" and "could not be read" are different facts):
    /// a stopped birth whose row cannot be read (its entry in the row's id index is not a record this
    /// build reads) is not listed by the view, its name being unknown, and the refused read is one
    /// span naming the reading admin, the birth, the index and the store's own cause. What must NOT
    /// happen: the birth listed at the name its digest last published as if no row had moved it, or
    /// the refusal silent.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_stopped_birth_whose_row_cannot_be_read_is_not_listed_and_the_refusal_is_spanned() {
        let d = crate::test_support::tempdir("row-name-unreadable");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let mine: Arc<dyn crate::storage::NodesStorage> = Arc::new(crate::storage::FileNodesStorage::open(&d.join("200000000000"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap());
        let digest_view = DigestView::default();
        let held = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        digest_view.record(held.clone());
        assert!(digest_view.stopped(rdm_core::LifecycleOp { r#type: held.node.r#type.clone(), mesh: held.node.mesh.clone(),
            build_id: "b1".into(),
            attempt: 1,
            operation: rdm_core::OperationId::location("node.stop", &rdm_core::PathName::new("mesh1.node", "1").unwrap()),
            node_id: held.node.node_id.clone(),
            incarnation: held.node.incarnation.clone(),
            name: held.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        }));
        let file = root.dir().join(crate::store_root::NODES_FILE);
        std::io::Write::write_all(&mut std::fs::OpenOptions::new().append(true).open(&file).unwrap(), b"\nnot a record line\n").unwrap();
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(mine);
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestView::default(), &Records::default())));
        let capture = crate::span_capture::Capture::new("row-name-unreadable");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            view_projector(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, digest_view.clone(), records.clone())().await;
        }
        let t = topology.read().await.clone();
        assert!(t.births().all(|n| n.node_id != held.node.node_id), "a birth whose row cannot be read is not listed: {:?}", t.nodes.iter().map(|n| n.name.clone()).collect::<Vec<_>>());
        let spans = capture.named("rdm.node_admin.topology.reject.via-row-unreadable");
        assert_eq!(spans.len(), 1, "{spans:?}");
        let a = &spans[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("node_path"), attr("node_name"), attr("subject_path"), attr("subject_name"), attr("index"), attr("node_id")), ("mesh1.node-admin".to_string(), "2".to_string(), "mesh1.node".to_string(), "1".to_string(), "id".to_string(), held.node.node_id.to_string()), "{a:?}");
        assert!(attr("detail").contains(&file.display().to_string()), "the store's cause names the file: {a:?}");
        eprintln!("span rdm.node_admin.topology.reject.via-row-unreadable trace_id={} {a:?}", spans[0].trace_id);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (states.md "Routing"): the view's `routable` is application routing's answer: a
    /// birth is routable only while it is ready and its mesh is ready, as this
    /// admin holds them. A `Pending` birth of a ready mesh, and a ready birth of a mesh this admin
    /// holds `pending` or `leaving` (or holds no status of), are listed and not routable; a ready
    /// birth of a ready mesh is routable; a birth listed only from this admin's launch record is
    /// Pending and not routable. Must NOT happen: a listed status other than
    /// ready, or a mesh not held ready, answering routable.
    #[test]
    fn the_view_routes_a_birth_only_while_it_and_its_mesh_are_ready() {
        use rdm_transport::topology::Frame;
        let digest_view = DigestView::default();
        let held = |mesh: &str, status: &str| {
            let f = Frame::MeshStatus { mesh: mesh.into(), status: status.into(), publisher: format!("{mesh}.node-admin.1"), forwarded_by: None, changed_at_rafka_ms: 10 };
            assert!(digest_view.statuses.take(&f, &fabric1()), "{f:?}");
        };
        held("mesh1", "ready");
        held("mesh2", "pending");
        held("mesh3", "leaving");
        let (ready, pending) = (digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready), digest(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", MemberStatus::Pending));
        let (of_pending_mesh, of_leaving_mesh, of_unheard_mesh) = (digest(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", MemberStatus::Ready), digest(PathName::new("mesh3.node", "1").unwrap(), "mesh3", "node", MemberStatus::Ready), digest(PathName::new("mesh4.node", "1").unwrap(), "mesh4", "node", MemberStatus::Ready));
        for d in [&ready, &pending, &of_pending_mesh, &of_leaving_mesh, &of_unheard_mesh] {
            assert!(digest_view.record(d.clone()));
        }
        // A birth this admin launched that topology view has not spoken for yet: listed from its
        // launch record, Pending, nothing realised.
        let records = Records::default();
        let launched: PathName = rdm_core::PathName::new("mesh1.node", "3").unwrap();
        records.nodes.lock().unwrap().insert(launched.clone(), crate::model::Node::allocated(launched.clone(), "mesh1", "node"));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        let routable = |d: &MeshDigest| t.node(&d.node.name).unwrap_or_else(|| panic!("{} is listed", d.node.node_id)).routable;
        let unheard = t.node(&launched).expect("the launch record lists the unheard birth");
        assert_eq!(unheard.status, NodeStatus::Pending);
        assert!(!unheard.routable, "a launched birth the topology view has not spoken for is Pending: a routing stop");
        assert!(routable(&ready), "a ready birth of a ready mesh is routable");
        assert!(!routable(&pending), "a Pending birth is a routing stop");
        assert!(!routable(&of_pending_mesh), "a birth of a pending mesh is a routing stop");
        assert!(!routable(&of_leaving_mesh), "a birth of a leaving mesh is a routing stop");
        assert!(!routable(&of_unheard_mesh), "a birth of a mesh this admin holds no status of is not held ready");
    }

    /// CONTRACT (gossip.md §6.1: a stopped status by itself and silence are not departure proof;
    /// node-identity-and-state.md §4.1: `Leaving` is a status the routing rule reads; §6: the
    /// observer's inference evicts at the staleness floor): a member the view holds `Leaving` is
    /// listed `Leaving` with the state it declared, whatever the age of its last heartbeat within the
    /// staleness floor: a node declared Leaving that still heartbeats is a member of its mesh until it
    /// is stopped or deleted. Past the floor it is the silent member any silent member is. Must NOT
    /// happen: a `Leaving` member one gossip interval and a millisecond after its heartbeat absent from
    /// the view, which is what makes `declared` unreadable at the authority that applied it.
    #[test]
    fn a_member_held_leaving_is_listed_leaving_with_its_declared_state_until_it_is_silent_past_the_staleness_floor() {
        use rdm_core::status::NodeState;
        let digest_view = DigestView::default();
        let records = Records::default();
        let d = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Leaving);
        let t0 = std::time::Instant::now();
        assert!(digest_view.record_at(d.clone(), t0));
        records.declared.lock().unwrap().nodes.insert((d.node.node_id.clone(), d.node.incarnation.clone()), NodeState::Stopped);
        let listed = |at: std::time::Instant| project_at("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, at).node(&d.node.name).cloned();

        let within = listed(t0 + gossip_interval() + Duration::from_millis(1)).expect("a Leaving member one interval after its heartbeat is a member of the view");
        assert_eq!((within.status, within.declared.as_deref(), within.routable, within.stopped), (NodeStatus::Leaving, Some("Stopped"), false, false));
        let silent = listed(t0 + digest_view.staleness_floor() + Duration::from_millis(1)).expect("silent past the floor it is listed, not deleted");
        assert_eq!((silent.status, silent.declared.as_deref()), (NodeStatus::PendingReconnect, Some("Stopped")), "silent past the staleness floor it is the silent member any silent member is");
    }

    /// CONTRACT (states.md: a mesh is pending -> ready -> leaving -> dead; "Routing": a
    /// mesh Leaving or Dead is a routing stop): the mesh status this admin projects is the state its
    /// mesh row holds once that is past ready. The projection is what the mesh primary
    /// announces on the existing MeshStatus send (`Backbone::announce_statuses`, fed from
    /// `t.meshes`), so a leaving mesh is announced leaving by its own primary. Must NOT happen: a
    /// mesh whose row holds Leaving (or Dead) projected, and so announced, ready.
    #[test]
    fn a_mesh_whose_row_holds_leaving_is_projected_leaving_so_its_primary_announces_leaving() {
        use rdm_core::status::MeshState;
        let digest_view = DigestView::default();
        let records = Records::default();
        for (mesh, state) in [("mesh1", MeshState::Deleting), ("mesh2", MeshState::Deleted), ("mesh3", MeshState::Ready)] {
            assert!(digest_view.record(digest(PathName::new(format!("{mesh}.node-admin"), "1").unwrap(), mesh, "node-admin", MemberStatus::Ready)));
            records.meshes.lock().unwrap().insert(mesh.into(), mesh_id(mesh));
            records.declared.lock().unwrap().meshes.insert(mesh_id(mesh), state);
        }
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        let status = |m: &str| t.meshes.iter().find(|x| x.name == m).unwrap_or_else(|| panic!("{m} is in the view: {:?}", t.meshes)).status;
        assert_eq!(status("mesh1"), ScopeStatus::Leaving, "a mesh whose row holds Leaving is projected leaving, so its primary announces leaving");
        assert_eq!(status("mesh2"), ScopeStatus::Dead, "a mesh whose row holds deleted is projected dead");
        assert_eq!(status("mesh3"), ScopeStatus::Ready, "a mesh whose row holds ready is projected from its members, as before");
    }

    /// CONTRACT (#2805, rdm#175; node-create.md "A node-admin created into a live mesh"; fabric
    /// primary §7; states.md: a mesh's state only moves forward): the state the fabric-primary hands a
    /// mesh's first admin is the state it holds for that mesh, and the mesh primary's declaration this
    /// admin applied (by the mesh's MeshId) is held, whatever its gossip view has heard yet. Pending
    /// only for a mesh it holds no state for (a new mesh). Must NOT happen: a mesh whose Ready this
    /// fabric-primary applied, its node-admin lost before its Ready MeshStatus frame arrived here,
    /// handed Pending at its recovery (rejected by the recovered birth as a backward move).
    #[test]
    fn the_hand_off_gives_the_declared_mesh_state_this_admin_applied_ahead_of_the_gossip_view() {
        use rdm_core::status::MeshState;
        use rdm_transport::topology::Frame;
        let digest_view = DigestView::default();
        let records = Records::default();
        assert_eq!(runner::held_mesh_state(&digest_view, &records, "mesh2"), MeshState::Pending, "a mesh this admin holds no state for is new: Pending");
        records.meshes.lock().unwrap().insert("mesh2".into(), mesh_id("mesh2"));
        let pending = Frame::MeshStatus { mesh: "mesh2".into(), status: "pending".into(), publisher: "mesh2.node-admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 };
        assert!(digest_view.statuses.take(&pending, &fabric1()));
        records.declared.lock().unwrap().meshes.insert(mesh_id("mesh2"), MeshState::Ready);
        assert_eq!(runner::held_mesh_state(&digest_view, &records, "mesh2"), MeshState::Ready, "the declaration applied here holds Ready while the gossip view still holds pending");
        let leaving = Frame::MeshStatus { mesh: "mesh2".into(), status: "leaving".into(), publisher: "mesh2.node-admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 20 };
        assert!(digest_view.statuses.take(&leaving, &fabric1()));
        assert_eq!(runner::held_mesh_state(&digest_view, &records, "mesh2"), MeshState::Deleting, "the furthest state held, from either: a state only moves forward");
        records.declared.lock().unwrap().meshes.insert(mesh_id("mesh3"), MeshState::Deleted);
        assert_eq!(runner::held_mesh_state(&digest_view, &records, "mesh3"), MeshState::Pending, "a declaration is read by this mesh's own MeshId only");
    }

    /// CONTRACT (states.md "Routing": a stop is applied by a peer on receipt of the fact): the view
    /// application routing reads is rebuilt at once when a routing fact is taken. A mesh status taken
    /// makes the rebuild due; so does a birth saying another status, and a lifecycle overlay opening.
    /// Must NOT happen: the view waiting for anything else to apply a fact the view already holds.
    #[tokio::test(start_paused = true)]
    async fn the_view_rebuild_is_due_on_receipt_of_a_routing_fact() {
        use futures_util::FutureExt;
        use rdm_transport::topology::Frame;
        let digest_view = DigestView::default();
        let records = Records::default();
        let rpc = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        assert!(digest_view.record(rpc.clone()));
        let mut wake = ViewWake::new(&digest_view, &records);
        let gate = crate::stop_gate::StopGate::new();

        wake.seen();
        let f = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready".into(), publisher: "100000000000".into(), forwarded_by: None, changed_at_rafka_ms: 10 };
        assert!(digest_view.statuses.take(&f, &fabric1()));
        assert!(view_rebuild_due(&mut wake, &gate).now_or_never().is_some_and(|(_, woken)| woken == "meshes"), "a mesh status taken makes the rebuild due at once");

        wake.seen();
        assert!(view_rebuild_due(&mut wake, &gate).now_or_never().is_none(), "nothing moved: no rebuild is due");
        let mut connections_deleting = rpc.clone();
        connections_deleting.status = MemberStatus::ConnectionsDeleting;
        connections_deleting.digest_seq += 1;
        assert!(digest_view.record(connections_deleting));
        assert!(view_rebuild_due(&mut wake, &gate).now_or_never().is_some(), "a birth saying ConnectionsDeleting makes the rebuild due at once");

        wake.seen();
        let stop = rdm_core::LifecycleOp { r#type: rpc.node.r#type.clone(), mesh: rpc.node.mesh.clone(), build_id: "b1".into(), attempt: 1, operation: rdm_core::OperationId::location("node.stop", &rdm_core::PathName::new("mesh1.node", "1").unwrap()), node_id: rpc.node.node_id.clone(), incarnation: rpc.node.incarnation.clone(), name: rpc.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        assert!(digest_view.stopped(stop));
        assert!(view_rebuild_due(&mut wake, &gate).now_or_never().is_some(), "a NodeStopped overlay makes the rebuild due at once");
    }

    /// CONTRACT (Q254, Luke 2026-10-28: "no deadline sleep replaces a tick (a sleep is a timer)"): the
    /// view rebuild has no clock of its own. With nothing moved, an hour passes and no rebuild is due;
    /// a record the projection reads changed at its setter (a birth held true offline is not, a launch
    /// record published, the fabric state) makes it due, named `records`; a primary record taken makes it
    /// due, named `primaries`; the pruner's mark of a birth silent past the floor makes it due, named
    /// `silence`. A setter that changes nothing moves nothing. Must NOT happen: a rebuild due on the
    /// passing of time, or a projection input changed with no rebuild due.
    #[tokio::test(start_paused = true)]
    async fn the_view_rebuild_is_due_on_each_input_of_the_projection_and_never_on_time() {
        use futures_util::FutureExt;
        let digest_view = DigestView::default();
        let records = Records::default();
        let rpc = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        assert!(digest_view.record(rpc.clone()));
        let mut wake = ViewWake::new(&digest_view, &records);
        let gate = crate::stop_gate::StopGate::new();
        let due = |wake: &mut ViewWake| view_rebuild_due(wake, &gate).now_or_never().map(|(_, woken)| woken);

        wake.seen();
        tokio::time::advance(Duration::from_secs(3600)).await;
        assert_eq!(due(&mut wake), None, "an hour with nothing moved: no rebuild is due");

        records.set_fabric_state(crate::fabric_state::FabricState::Ready);
        assert_eq!(due(&mut wake), Some("records"), "the fabric state changed at its setter");
        wake.seen();
        records.set_fabric_state(crate::fabric_state::FabricState::Ready);
        assert_eq!(due(&mut wake), None, "the same fabric state set again moves nothing");

        crate::deployment::pipeline::TopologySink::publish(&records, Node::allocated(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"));
        assert_eq!(due(&mut wake), Some("records"), "a launch record published");
        wake.seen();

        let primary = rdm_core::Primary { mesh: "mesh1".into(), node_id: rpc.node.node_id.clone(), incarnation: rpc.node.incarnation.clone(), revision: 1 };
        digest_view.primaries.take(rdm_core::PrimaryRole::NodePrimary("node".into()), &primary);
        assert_eq!(due(&mut wake), Some("primaries"), "the rpc nodes' node-type primary record taken");
        wake.seen();

        // The pruner's sweep, past the floor: the birth unheard since it was recorded.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(digest_view.mark_silence_at(std::time::Instant::now() + digest_view.staleness_floor() * 2), "the sweep marks the birth silent");
        assert_eq!(due(&mut wake), Some("silence"), "the pruner's mark makes the rebuild due");

        // The states this admin applied (status_rpc), moved at their setters.
        use rdm_core::status::{MeshState, NodeState};
        wake.seen();
        let birth = (rpc.node.node_id.clone(), rpc.node.incarnation.clone());
        records.declared.lock().unwrap().hold_node(birth.clone(), NodeState::Ready);
        assert_eq!(due(&mut wake), Some("declared"), "a birth's declared state held");
        wake.seen();
        records.declared.lock().unwrap().hold_node(birth.clone(), NodeState::Ready);
        assert_eq!(due(&mut wake), None, "the same declared state again moves nothing");
        records.declared.lock().unwrap().hold_mesh(mesh_id("mesh1"), MeshState::Deleted);
        assert_eq!(due(&mut wake), Some("declared"), "a Mesh's state held");
        wake.seen();
        records.declared.lock().unwrap().forget_node(&birth);
        assert_eq!(due(&mut wake), Some("declared"), "a birth's declared state forgotten, its cycle begun");
        wake.seen();

        // The node rows this admin writes: a row's name moved, or released, is a change of the name the
        // view lists a birth that does not publish at.
        let storage = crate::storage::MemoryNodesStorage::default();
        let row = crate::storage::NodeRow::new(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", rpc.node.node_id.clone(), crate::model::NodeStatus::Ready);
        records.caches.nodes.put(&storage, &row).await.unwrap();
        assert_eq!(due(&mut wake), Some("rows"), "a row first written");
        wake.seen();
        let held = records.caches.nodes.row_of(&row.node_id).unwrap();
        records.caches.nodes.put(&storage, &held).await.unwrap();
        assert_eq!(due(&mut wake), None, "the row written again at its name, not released, moves nothing");
        records.caches.nodes.put(&storage, &crate::storage::NodeRow { name: rdm_core::PathName::new("mesh1.node", "9").unwrap(), ..held.clone() }).await.unwrap();
        assert_eq!(due(&mut wake), Some("rows"), "the row's name moved");
        wake.seen();
        let held = records.caches.nodes.row_of(&row.node_id).unwrap();
        records.caches.nodes.put(&storage, &crate::storage::NodeRow { status: crate::model::NodeStatus::Deleted, ..held }).await.unwrap();
        assert_eq!(due(&mut wake), Some("rows"), "the row released its name");
        wake.seen();

        // A mesh's id the records hold, held at its setter.
        crate::admin::rows_writer::hold_mesh_id(&records, "mesh3", &mesh_id("mesh3"));
        assert_eq!(due(&mut wake), Some("records"), "a mesh id held");
    }

    /// CONTRACT (node.stop: a stopped admin's tasks are held; states.md "Routing"): the view rebuild
    /// is held at its own wait, never inside the view's lock. While the admin's gate is stopped a
    /// routing fact makes no rebuild due; the gate opening does, at once. Must NOT happen: a stopped
    /// admin rebuilding its view, or a rebuild held where it waits on the view's lock (a gated task
    /// stops where it stands, and one stopped queued on the lock holds it from every reader: the
    /// stopped admin then never serves the start that would open it).
    #[tokio::test(start_paused = true)]
    async fn a_stopped_admins_view_rebuild_is_held_at_its_own_wait_and_resumes_when_the_gate_opens() {
        use futures_util::FutureExt;
        use rdm_transport::topology::Frame;
        let digest_view = DigestView::default();
        let records = Records::default();
        let mut wake = ViewWake::new(&digest_view, &records);
        let gate = crate::stop_gate::StopGate::new();
        wake.seen();
        gate.stop();
        let f = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready".into(), publisher: "100000000000".into(), forwarded_by: None, changed_at_rafka_ms: 10 };
        assert!(digest_view.statuses.take(&f, &fabric1()));
        let due = view_rebuild_due(&mut wake, &gate);
        tokio::pin!(due);
        assert!((&mut due).now_or_never().is_none(), "stopped: a routing fact makes no rebuild due");
        gate.resume();
        assert!((&mut due).now_or_never().is_some(), "the gate opening makes the held rebuild due at once");
    }

    /// A nodes store over `inner` that counts the node-row reads made through it, by `(path, name)` key
    /// and by node id, and how many of them ran while `view`'s write lock was held.
    #[cfg(feature = "file-backend")]
    struct ProbedRows {
        inner: Arc<dyn crate::storage::NodesStorage>,
        view: Arc<RwLock<Topology>>,
        key_reads: std::sync::atomic::AtomicUsize,
        id_reads: std::sync::atomic::AtomicUsize,
        under_write_lock: std::sync::atomic::AtomicUsize,
    }

    #[cfg(feature = "file-backend")]
    impl ProbedRows {
        fn read(&self, by: &std::sync::atomic::AtomicUsize) {
            by.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.view.try_read().is_err() {
                self.under_write_lock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        fn count(by: &std::sync::atomic::AtomicUsize) -> usize {
            by.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[cfg(feature = "file-backend")]
    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for ProbedRows {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.read(&self.id_reads);
            self.inner.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.nodes(mesh).await
        }
        async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_properties(record).await
        }
        async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, crate::storage_error::StorageError> {
            self.inner.properties(node_id).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node(row).await
        }
        async fn records_at(&self, name: &crate::model::PathName) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.records_at(name).await
        }
    }

    /// A birth born `mesh1.node.7` and stopped there, its row moved twice by its mesh-primary on the
    /// fabric's shared store root (to `mesh1.node.1`, then to `mesh1.node.1-old`), and another admin's
    /// view over the same root, whose row reads are probed.
    #[cfg(feature = "file-backend")]
    async fn renamed_twice(cell: &str) -> (std::path::PathBuf, MeshDigest, Arc<ProbedRows>, Arc<RwLock<Topology>>, Arc<Records>, DigestView) {
        let d = crate::test_support::tempdir(cell);
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let primary = crate::storage::FileNodesStorage::open(&d.join("100000000000"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap();
        let digest_view = DigestView::default();
        let held = digest(PathName::new("mesh1.node", "7").unwrap(), "mesh1", "node", MemberStatus::Ready);
        digest_view.record(held.clone());
        assert!(digest_view.stopped(rdm_core::LifecycleOp { r#type: held.node.r#type.clone(), mesh: held.node.mesh.clone(),
            build_id: "b1".into(),
            attempt: 1,
            operation: rdm_core::OperationId::location("node.stop", &rdm_core::PathName::new("mesh1.node", "7").unwrap()),
            node_id: held.node.node_id.clone(),
            incarnation: held.node.incarnation.clone(),
            name: held.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        }));
        let mut record = Node::allocated(held.node.name.clone(), held.node.mesh.clone(), &held.node.r#type);
        record.node_id = held.node.node_id.clone();
        record.incarnation_id = Some(held.node.incarnation.clone());
        record.status = NodeStatus::Leaving;
        let row = crate::storage::NodeRow { replacement_from: None, r#type: held.node.r#type.clone(), mesh: held.node.mesh.clone(), initial_properties: None, topology_storage_provider: None, name: held.node.name.clone(), node_id: held.node.node_id.clone(), status: NodeStatus::Leaving, allocated: None, renamed_from: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, writer: None };
        crate::node_rows::write_record(&primary, &row).await.unwrap().unwrap();
        let (me, base) = (rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), rdm_core::PathName::new("mesh1.node", "1").unwrap());
        crate::node_rows::move_row(&primary, &Default::default(), &me, &record, &held.node.name, &base).await.unwrap();
        crate::node_rows::move_row(&primary, &Default::default(), &me, &record, &base, &base.renamed().unwrap()).await.unwrap();
        let view = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestView::default(), &Records::default())));
        let probed = Arc::new(ProbedRows {
            inner: Arc::new(crate::storage::FileNodesStorage::open(&d.join("200000000000"), &root, "mesh1", &crate::store_log::LogWriter::mint()).await.unwrap()),
            view: view.clone(),
            key_reads: Default::default(),
            id_reads: Default::default(),
            under_write_lock: Default::default(),
        });
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(probed.clone());
        (d, held, probed, view, records, digest_view)
    }

    /// CONTRACT (CLAUDE.md read rule: "id: only a lookup by id that cannot read the store's own key";
    /// "add the index, never the scan"): a stopped birth whose row was moved twice (`mesh1.node.7` to
    /// `mesh1.node.1`, then to `mesh1.node.1-old`) is listed by another admin's
    /// view at the row's current name, `mesh1.node.1-old`, read by the birth's node id through the
    /// node row's id index: one read, no `(path, name)` key guessed. What must NOT happen: the birth
    /// listed at a name it no longer holds, or its name found by trying the keys a move could reach.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_stopped_birth_renamed_twice_by_its_row_is_listed_at_the_rows_current_name_read_by_its_node_id() {
        let (d, held, probed, view, records, digest_view) = renamed_twice("row-name-twice").await;
        view_projector(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), view.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, digest_view.clone(), records.clone())().await;
        let t = view.read().await.clone();
        let names: Vec<PathName> = t.nodes.iter().map(|n| n.name.clone()).collect();
        let moved: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let now = moved.renamed().unwrap();
        assert!(t.node(&now).is_some_and(|n| n.node_id == held.node.node_id && n.stopped), "the stopped birth is listed at its row's current name: {names:?}");
        assert!(t.node(&held.node.name).is_none() && t.node(&moved).is_none(), "nothing is listed at a name the row moved from: {names:?}");
        assert_eq!((ProbedRows::count(&probed.key_reads), ProbedRows::count(&probed.id_reads)), (0, 1), "the birth's row is read once, by its node id, and no (path, name) key is tried");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (no disk read under the view's write lock): the projection reads the rows of the
    /// births that do not publish first, then takes the view's write lock and installs. Every row
    /// read finds the view's lock free. What must NOT happen: a row read made while the view's write
    /// lock is held, which holds every reader of the view (a stopped admin's handlers, the router)
    /// behind a disk read.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn the_projection_reads_its_rows_before_it_takes_the_views_write_lock() {
        let (d, _held, probed, view, records, digest_view) = renamed_twice("row-name-lock").await;
        view_projector(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), view.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, digest_view.clone(), records.clone())().await;
        let reads = ProbedRows::count(&probed.key_reads) + ProbedRows::count(&probed.id_reads);
        assert!(reads >= 1, "the projection read the stopped birth's row");
        assert_eq!(ProbedRows::count(&probed.under_write_lock), 0, "none of the {reads} row reads ran under the view's write lock");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (node.stop, a successor admin): an admin that joined after the stop never held the
    /// node's digest, only the stop overlay the mesh topology carries (`MeshTopology.in_flight`, read
    /// by `GetTopology`). Its view lists the node `Leaving` and stopped from that overlay.
    #[test]
    fn a_successor_admin_lists_a_stopped_node_from_the_stop_overlay_it_read() {
        let digest_view = DigestView::default();
        let stopped = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        let stop = rdm_core::LifecycleOp { r#type: stopped.node.r#type.clone(), mesh: stopped.node.mesh.clone(),
            build_id: "b1".into(),
            attempt: 1,
            operation: rdm_core::OperationId::location("node.stop", &rdm_core::PathName::new("mesh1.node", "1").unwrap()),
            node_id: stopped.node.node_id.clone(),
            incarnation: stopped.node.incarnation.clone(),
            name: stopped.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        };
        assert!(digest_view.stopped(stop));
        assert!(digest_view.get(stopped.node.node_id.as_str()).is_none(), "this admin never held the node's digest");
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &Records::default());
        let n = t.node(&stopped.node.name).expect("the stop overlay lists the stopped node");
        assert_eq!((n.status, n.stopped, n.routable), (NodeStatus::Leaving, true, false));
        assert_eq!(n.node_id, stopped.node.node_id);
        assert_eq!(n.incarnation_id.as_ref(), Some(&stopped.node.incarnation));
    }

    #[test]
    fn a_launch_record_never_revives_a_birth_that_another_admin_deleted() {
        // This admin launched mesh1.node.1; another admin (the primary) deleted
        // it: its last word is Leaving, then silence. The topology view speaks for the
        // birth (silent past the floor), so this admin's own launch record must
        // not bring it back as ready.
        let digest_view = DigestView::default();
        let left = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Leaving);
        digest_view.record(left.clone());
        let records = Records::default();
        let mut n = Node::allocated(left.node.name.clone(), left.node.mesh.clone(), &left.node.r#type);
        n.incarnation_id = Some(left.node.incarnation.clone());
        n.status = NodeStatus::Ready;
        records.publish(n.clone());
        let past_floor = std::time::Instant::now() + digest_view.staleness_floor() + Duration::from_millis(1);
        let t = project_at("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records, past_floor);
        assert_eq!(t.node(&left.node.name).map(|n| n.status), Some(NodeStatus::PendingReconnect), "the view speaks for the birth, silent past the floor; the launch record's ready never stands in: {:?}", t.node(&left.node.name));
        // A birth it launched that topology view has not spoken for yet is shown.
        let fresh = Records::default();
        let mut pending = Node::allocated(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node");
        pending.incarnation_id = Some(IncarnationId::mint());
        fresh.publish(pending.clone());
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &fresh);
        assert!(t.node(&pending.name).is_some());
        // A birth it launched whose departure another admin proved and published: the view no
        // longer holds the digest, and the launch record never stands in for it.
        let gone = digest(PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node", MemberStatus::Ready);
        digest_view.record(gone.clone());
        let launched = Records::default();
        let mut n = Node::allocated(gone.node.name.clone(), gone.node.mesh.clone(), &gone.node.r#type);
        n.node_id = gone.node.node_id.clone();
        n.incarnation_id = Some(gone.node.incarnation.clone());
        n.status = NodeStatus::Ready;
        launched.publish(n);
        assert!(project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &launched).node(&gone.node.name).is_some());
        digest_view.depart(rdm_core::LifecycleOp { r#type: gone.node.r#type.clone(), mesh: gone.node.mesh.clone(),
            build_id: "b1".into(),
            attempt: 1,
            operation: rdm_core::OperationId::location("node.delete", &rdm_core::PathName::new("mesh1.node", "3").unwrap()),
            node_id: gone.node.node_id.clone(),
            incarnation: gone.node.incarnation.clone(),
            name: gone.node.name.clone(),
            event_at_rafka_ms: 1,
            traceparent: None,
            tracestate: None,
        });
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &launched);
        assert!(t.node(&gone.node.name).is_none(), "a deleted birth's launch record is not a node: {:?}", t.node(&gone.node.name));
    }

    #[test]
    fn removed_births_leave_the_view_and_a_newer_birth_at_the_path_does_not() {
        let digest_view = DigestView::default();
        let old = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        digest_view.record(old.clone());
        let records = Records::default();
        let mut n = Node::allocated(old.node.name.clone(), old.node.mesh.clone(), &old.node.r#type);
        n.incarnation_id = Some(old.node.incarnation.clone());
        records.publish(n);
        records.remove(&old.node.name);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert!(t.node(&old.node.name).is_none(), "the removed birth is gone");
        let mut newer = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        newer.node.node_id = NodeId::mint();
        digest_view.record(newer.clone());
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(t.node(&old.node.name).unwrap().incarnation_id.as_ref(), Some(&newer.node.incarnation));
    }

    /// CONTRACT (node-recovery.md, sequence step 3): the recovery's delete removes exactly the exited
    /// birth. A rerun of the recovery after the new node already holds the path removes nothing of the
    /// new node, and the exited birth stays out of the view even when the records never held it.
    /// Must NOT happen: the new node's record removed, or its birth filtered from the view.
    #[test]
    fn a_recovery_removes_exactly_the_exited_birth_and_never_the_new_node_at_its_path() {
        let digest_view = DigestView::default();
        let old = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        digest_view.record(old.clone());
        let mut new = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        new.node.node_id = NodeId::mint();
        digest_view.record(new.clone());
        let records = Records::default();
        let mut n = Node::allocated(new.node.name.clone(), new.node.mesh.clone(), &new.node.r#type);
        n.node_id = new.node.node_id.clone();
        n.incarnation_id = Some(new.node.incarnation.clone());
        records.publish(n);
        // The rerun: the new node holds the path, the exited birth is removed again.
        records.remove_birth(&old.node.name, &old.node.node_id, &old.node.incarnation);
        assert_eq!(records.nodes.lock().unwrap().get(&new.node.name).map(|n| n.node_id.clone()), Some(new.node.node_id.clone()), "the new node's record stays");
        let removed = records.removed.lock().unwrap().clone();
        assert!(removed.contains(&(old.node.name.clone(), Some(old.node.incarnation.clone()))), "the exited birth is the one removed: {removed:?}");
        assert!(!removed.contains(&(new.node.name.clone(), Some(new.node.incarnation.clone()))), "the new birth is not removed: {removed:?}");
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records);
        assert_eq!(t.node(&new.node.name).map(|n| n.node_id.clone()), Some(new.node.node_id.clone()), "the view holds the new node at the path");
    }

    /// CONTRACT (gossip.md §3.1 `NodeDeleted`: "the birth is removed ... the current topology no
    /// longer lists it"; node-identity-and-state.md §6: all removal from the view is the topology
    /// view's): a birth this admin's records hold (its entry sweep reached it) leaves the view for good
    /// once it is deleted. Its path and name taken by a new birth that is deleted in turn, and the deleted
    /// retention passed (the digest view then holds nothing of either birth), the view lists nothing at that
    /// path and name. What must NOT happen: the deleted birth's record standing in for it again,
    /// ready, once the view has forgotten the departure (soak seed 1432787: z5dwdq96cky0
    /// came back as an arrival at mesh1.node.1 862 s after its deletion).
    #[tokio::test]
    async fn a_deleted_birth_never_comes_back_from_this_admins_records_once_its_path_name_is_free() {
        async fn view_over(me: &PathName, topology: &RwLock<Topology>, digest_view: &DigestView, records: &Records) -> Topology {
            records.install_read(me, topology, digest_view, &fabric1(), |reads| project_with("fabric1", &fabric1(), ProviderKind::Process, digest_view, records, reads)).await
        }
        let deleted = |d: &MeshDigest| rdm_core::LifecycleOp { r#type: d.node.r#type.clone(), mesh: d.node.mesh.clone(), build_id: "b1".into(), attempt: 1, operation: rdm_core::OperationId::node("node.delete", &d.node.node_id), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let digest_view = DigestView::default();
        let records = Records::default();
        let first = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        digest_view.record(first.clone());
        // This admin's entry sweep reached the birth (`spawn_entry_sweep`): its record stands in for it.
        let reached = crate::reenter::MapNode { mesh: first.node.mesh.clone(), r#type: first.node.r#type.clone(), node_id: first.node.node_id.clone(), name: first.node.name.clone(), endpoint_id: first.node.endpoint_id.clone(), transport_addr: first.node.transport_addr, incarnation: first.node.incarnation.clone(), transition: None, ready: true, data_dir: None, admin_api_base: None };
        records.publish(reached.as_node(NodeStatus::Ready, ProviderKind::Process));
        let topology = RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records));
        let listed = |t: &Topology| t.node(&first.node.name).map(|n| (n.node_id.clone(), n.status));
        assert_eq!(listed(&view_over(&me, &topology, &digest_view, &records).await), Some((first.node.node_id.clone(), NodeStatus::Ready)));

        assert!(digest_view.depart(deleted(&first)), "the birth is deleted");
        assert_eq!(listed(&view_over(&me, &topology, &digest_view, &records).await), None, "the deleted birth leaves the view");
        // A new birth takes the path and name and is deleted in turn.
        let second = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        digest_view.record(second.clone());
        assert_eq!(listed(&view_over(&me, &topology, &digest_view, &records).await), Some((second.node.node_id.clone(), NodeStatus::Ready)));
        assert!(digest_view.depart(deleted(&second)), "the new birth is deleted");
        assert_eq!(listed(&view_over(&me, &topology, &digest_view, &records).await), None);

        // The deleted retention passes: the view holds nothing of either birth.
        let after = DigestView::default();
        assert_eq!(listed(&view_over(&me, &topology, &after, &records).await), None, "a deleted birth came back into the view from this admin's records");
    }

    /// CONTRACT: a view installed by a writer that had to wait for the view lock reflects the
    /// records as they are when it holds the lock, so a removal that lands while it waits is not
    /// undone by the older projection.
    #[tokio::test]
    async fn a_projection_that_waits_for_the_view_lock_installs_the_state_at_the_lock() {
        let digest_view = Arc::new(DigestView::default());
        let old = digest(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", MemberStatus::Ready);
        digest_view.record(old.clone());
        let records = Arc::new(Records::default());
        let mut n = Node::allocated(old.node.name.clone(), old.node.mesh.clone(), &old.node.r#type);
        n.incarnation_id = Some(old.node.incarnation.clone());
        records.publish(n);
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records)));
        assert!(topology.read().await.node(&old.node.name).is_some(), "the node is in the view before its removal");
        // Another installer holds the view while this one is asked to install.
        let held = topology.write().await;
        let waiting = {
            let (records, topology, digest_view) = (records.clone(), topology.clone(), digest_view.clone());
            tokio::spawn(async move { records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &digest_view, &records)).await })
        };
        tokio::task::yield_now().await;
        records.remove(&old.node.name);
        drop(held);
        waiting.await.unwrap();
        assert!(topology.read().await.node(&old.node.name).is_none(), "the view holds the removed node: a projection taken before the lock was installed after the removal");
    }

    const STORE_DIR: &str = "/tmp/rdm-admin-cells/store";

    fn cfg(env: &[(&str, &str)]) -> AdminConfig {
        let mut env: std::collections::HashMap<String, String> = env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        env.entry("RDM_STORE_DIR".into()).or_insert_with(|| STORE_DIR.into());
        AdminConfig::from_env(|k| env.get(k).cloned()).expect("a readable environment")
    }

    /// CONTRACT (R14 items 1 and 8; acceptance "a missing store_dir is refused by name at boot"):
    /// a node-admin is given the fabric's store root (`RDM_STORE_DIR`, or its launch's); with none it
    /// is refused by name and nothing falls back to its data dir.
    #[test]
    fn a_start_without_a_store_dir_is_refused_by_name() {
        let env: std::collections::HashMap<String, String> = [("RDM_DATA_DIR", "/tmp/rdm-no-store/mesh1.node-admin.1")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let e = AdminConfig::from_env(|k| env.get(k).cloned()).err().expect("a start with no store_dir was accepted");
        assert!(e.contains("RDM_STORE_DIR") && e.contains("required"), "{e}");
        assert_eq!(cfg(&[]).store_dir, std::path::PathBuf::from(STORE_DIR));
    }

    /// CONTRACT (node-connections-delete.md step 5): a person-started admin reads its
    /// connections-delete bound once, from `RDM_CONNECTIONS_DELETE_DEADLINE_MS` in its config, 5000 ms
    /// when it names none; a launched admin takes its launch's. A value that is no whole number of
    /// milliseconds is refused by name at boot, never read as the default.
    #[test]
    fn an_admin_takes_its_connections_delete_bound_from_its_config_once() {
        assert_eq!(cfg(&[]).connections_delete_deadline, rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE);
        assert_eq!(cfg(&[("RDM_CONNECTIONS_DELETE_DEADLINE_MS", "750")]).connections_delete_deadline, Duration::from_millis(750));
        let env: std::collections::HashMap<String, String> = [("RDM_STORE_DIR", STORE_DIR), ("RDM_CONNECTIONS_DELETE_DEADLINE_MS", "soon")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let e = AdminConfig::from_env(|k| env.get(k).cloned()).err().expect("a bound that is no number was accepted");
        assert!(e.starts_with("RDM_CONNECTIONS_DELETE_DEADLINE_MS `soon`"), "{e}");
        let launch = rdm_core::launch::Launch { initial_properties: Default::default(), topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            mesh: "mesh1".into(),
            r#type: "node-admin".into(),
            name: rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            launcher: None,
            seeds: vec![],
            data_dir: "/tmp/rdm-launched/mesh1.node-admin.2".into(),
            store_dir: STORE_DIR.into(),
            mesh_id: None,
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: Duration::from_millis(1250),
            declared: Default::default(),
        };
        let launched = launch.to_env();
        assert_eq!(AdminConfig::from_env(|k| launched.get(k).cloned()).unwrap().connections_delete_deadline, Duration::from_millis(1250));
    }

    /// CONTRACT (Q208, Luke 2026-10-27: "have the first step of the bootstrap mint the id"): the
    /// configuration, the bootstrap's first step, holds this admin's node id before anything else of the
    /// boot runs, its telemetry included. With no launcher (Day 0, a start over an inherited data dir, an
    /// adoption launch) it is minted there, a new one at every start: a start over an inherited data dir is
    /// a new node with a new node.id (fabric-recovery.md, node-admin-lifecycle.md §3). A launched admin's
    /// is its launch's `RDM_NODE_ID`, unchanged. What must NOT happen: a no-launcher start holding no id
    /// until its boot mints one, or two starts sharing one.
    #[test]
    fn the_bootstraps_first_step_holds_the_node_id_a_launched_admin_its_launchs() {
        let (day0, again) = (cfg(&[]), cfg(&[]));
        assert!(NodeId::parse(&day0.node_id.to_string()).is_ok(), "a canonical minted node id: {}", day0.node_id);
        assert_ne!(day0.node_id, again.node_id, "every start with no launcher mints its own node id");
        let fabric = FabricId::mint().to_string();
        let adoption = cfg(&[("RDM_NODE_PATH", "mesh1.node-admin"), ("RDM_NODE_NAME", "1"), ("RDM_FABRIC_ID", fabric.as_str()), ("RDM_MESH", "mesh1")]);
        assert!(adoption.adopt.is_some() && adoption.launch.is_none(), "an adoption launch: {adoption:?}");
        assert_ne!(adoption.node_id, day0.node_id, "an adoption launch is a new birth with its own node.id");
        let given = NodeId::mint();
        let launch = rdm_core::launch::Launch {
            initial_properties: Default::default(),
            topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            mesh: "mesh1".into(),
            r#type: "node-admin".into(),
            name: rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(),
            node_id: given.clone(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            launcher: None,
            seeds: vec![],
            data_dir: "/tmp/rdm-launched/mesh1.node-admin.2".into(),
            store_dir: STORE_DIR.into(),
            mesh_id: None,
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: Duration::from_millis(1250),
            declared: Default::default(),
        };
        let launched = launch.to_env();
        assert_eq!(launched.get(rdm_core::launch::ENV_NODE_ID), Some(&given.to_string()));
        let mut wrong_kind = launched.clone();
        wrong_kind.insert(rdm_core::launch::ENV_NODE_TYPE.into(), "node".into());
        let error = AdminConfig::from_env(|k| wrong_kind.get(k).cloned()).unwrap_err();
        assert_eq!(error, "RDM_NODE_TYPE names node: this executable serves node-admin");
        let launched = AdminConfig::from_env(|k| launched.get(k).cloned()).unwrap();
        assert_eq!((&launched.node_id, launched.launch.as_ref().map(|l| &l.node_id)), (&given, Some(&given)), "a launched admin runs as its launch's RDM_NODE_ID");
    }

    const MESH_ID: &str = "4f14h1k7m3p9";
    const SEED: &str = "5d6b3f0e1a2c4b7d8e9f00112233445566778899aabbccddeeff001122334455@127.0.0.1:41000";

    /// A person-started admin that is not a restart is exactly one of: Day 0 (no mesh, no seed, no
    /// flag, an empty data dir) or a fabric recovery (Fabric, mesh, seed and both flags). Anything
    /// else is refused by name, and nothing is minted over durable records.
    #[test]
    fn a_start_that_is_neither_day_zero_nor_a_named_recovery_is_refused_by_name() {
        let ok = |env: &[(&str, &str)]| refuse_contradictory_start(&cfg(env), None);
        let fabric = FabricId::mint().to_string();
        assert_eq!(ok(&[]), Ok(()), "Day 0");
        let recovery = [("RDM_FABRIC_ID", fabric.as_str()), ("RDM_MESH_ID", MESH_ID), ("RDM_SEEDS", SEED), ("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")];
        assert_eq!(ok(&recovery), Ok(()), "a fabric recovery names everything it needs");
        // Durable records without this admin's own row: never Day 0, never a mint.
        for what in ["a Fabric record", "a mesh record", "a durable topology"] {
            let e = refuse_contradictory_start(&cfg(&[]), Some(what)).unwrap_err();
            assert!(e.contains(what) && e.contains("neither a restart nor a fresh start"), "{e}");
            assert!(refuse_contradictory_start(&cfg(&recovery), Some(what)).is_err(), "a recovery start does not override durable records either");
        }
        // A mesh or a seed without a flag is not Day 0.
        let e = ok(&[("RDM_MESH_ID", MESH_ID)]).unwrap_err();
        assert!(e.contains("RDM_MESH_ID") && e.contains("no recovery flag"), "{e}");
        assert!(ok(&[("RDM_SEEDS", SEED)]).unwrap_err().contains("RDM_SEEDS"));
        // A recovery names what it recovers: each missing input is named.
        let missing = ok(&[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]).unwrap_err();
        for k in ["RDM_MESH_ID", "RDM_FABRIC_ID", "RDM_SEEDS"] {
            assert!(missing.contains(k), "{missing}");
        }
        // A mesh recovery is the fabric primary's launch, not a person's.
        assert!(ok(&[("RDM_MESH_PRIMARY", "1"), ("RDM_MESH_ID", MESH_ID), ("RDM_SEEDS", SEED)]).unwrap_err().contains("started by the fabric primary"));
        // The fabric flag implies the mesh flag.
        assert!(AdminConfig::from_env(|k| match k {
            "RDM_FABRIC_PRIMARY" => Some("1".to_string()),
            "RDM_STORE_DIR" => Some(STORE_DIR.to_string()),
            _ => None,
        })
        .unwrap_err()
        .contains("RDM_FABRIC_PRIMARY is set without RDM_MESH_PRIMARY"));
    }

    /// An entry answer from `served_by` holding `frames`.
    fn entry_answer(frames: Vec<rdm_transport::topology::Frame>) -> crate::wire::JoinAnswer {
        crate::wire::JoinAnswer {
            served_by: NodeId::parse("100000000000").unwrap(),
            control: crate::wire::JoinControl { cert_policy: None, provider: ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None },
            statuses: frames,
        }
    }

    fn mesh_status(mesh: &str, status: &str) -> rdm_transport::topology::Frame {
        rdm_transport::topology::Frame::MeshStatus { mesh: mesh.into(), status: status.into(), publisher: "100000000000".into(), forwarded_by: None, changed_at_rafka_ms: 10 }
    }

    /// CONTRACT (i144 Q56; node-create.md "A node-admin created into a live mesh" step 2: the joining
    /// admin "obtains its control/status context" from its maker; fabric-authority.md §7: the successor
    /// adopts the mesh's existing state exactly): the admin created into a lost mesh takes its mesh's
    /// held state from its maker's entry answer into its applied state, so its Ready gate holds its
    /// mesh's state whatever the fabric-primary's own view decided about a hand-off. Must NOT happen:
    /// the joining admin applying nothing from the answer and waiting on a hand-off that may never come,
    /// or a state it holds replaced, or a word it does not read guessed into a state.
    #[test]
    fn a_lost_meshs_created_admin_adopts_its_meshs_held_state_from_its_makers_answer() {
        use rdm_core::status::MeshState;
        let (id, other) = (mesh_id("mesh2"), mesh_id("mesh3"));
        let records = Records::default();
        adopt_held_mesh_state(&entry_answer(vec![mesh_status("mesh3", "dead"), mesh_status("mesh2", "ready")]), &records, "mesh2", &id);
        assert_eq!(records.declared.lock().unwrap().meshes.get(&id), Some(&MeshState::Ready), "its own mesh's held state, exactly");
        assert_eq!(records.declared.lock().unwrap().meshes.get(&other), None, "another mesh's state is not this admin's to adopt");

        // A state this admin holds is kept: an answer is no transition.
        adopt_held_mesh_state(&entry_answer(vec![mesh_status("mesh2", "pending")]), &records, "mesh2", &id);
        assert_eq!(records.declared.lock().unwrap().meshes.get(&id), Some(&MeshState::Ready), "an applied state is kept");

        // A new mesh's first admin is answered no state for its mesh: it adopts nothing and waits for its Pending.
        let fresh = Records::default();
        adopt_held_mesh_state(&entry_answer(vec![mesh_status("mesh1", "ready")]), &fresh, "mesh2", &id);
        assert!(fresh.declared.lock().unwrap().meshes.is_empty(), "no held state for its mesh: nothing adopted");

        // A word that is no Mesh lifecycle state is refused by name and adopts nothing.
        let unread = Records::default();
        adopt_held_mesh_state(&entry_answer(vec![mesh_status("mesh2", "degraded")]), &unread, "mesh2", &id);
        assert!(unread.declared.lock().unwrap().meshes.is_empty(), "a word it does not read adopts nothing");
        assert!(mesh_state_of("degraded").unwrap_err().contains("\"degraded\""), "the refusal names the word");
    }

    /// A nodes store counting its reads of every runtime row, refusing them while `refusing` is set.
    #[derive(Default)]
    struct RuntimeReads {
        inner: crate::storage::MemoryNodesStorage,
        reads: std::sync::atomic::AtomicUsize,
        refusing: std::sync::atomic::AtomicBool,
    }

    impl RuntimeReads {
        fn reads(&self) -> usize {
            self.reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for RuntimeReads {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.refusing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(crate::storage_error::StorageError::Io { file: "nodes.jsonl".into(), reason: "Input/output error (os error 5)".into() });
            }
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.inner.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.nodes(mesh).await
        }
        async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_properties(record).await
        }
        async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, crate::storage_error::StorageError> {
            self.inner.properties(node_id).await
        }
        async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node(row).await
        }
        async fn records_at(&self, name: &crate::model::PathName) -> Result<Vec<crate::storage::NodeRow>, crate::storage_error::StorageError> {
            self.inner.records_at(name).await
        }
    }

    /// CONTRACT (node-recovery.md "Detection and the verdict" step 2: "Only the mesh-primary acts";
    /// fabric-node-lifecycle-elections.md §4.1: the mesh-primary runs node investigation; build.md §5.3:
    /// the launching admin writes a birth's runtime row, and every attempt in a mesh with a running
    /// mesh-primary is that mesh-primary's to run): the births only a durable runtime row names are
    /// watched by the mesh-primary alone, from the rows of its own mesh read once as it takes the role
    /// and held while it keeps it. An admin that is not its mesh's primary reads no runtime row on any
    /// tick; one that loses the role holds none, and takes them again with the role; a refused read
    /// holds none, so the next tick reads again. Must NOT happen: a read of every runtime row on every
    /// tick of every admin (rust-distributed-mesh#99: 9 reads in 3 s on an admin that is no primary, 12
    /// on the mesh-primary), or another mesh's rows held.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_offline_tick_reads_its_meshs_runtime_rows_once_per_tenure_as_its_mesh_primary_and_never_otherwise() {
        let capture = crate::span_capture::Capture::new("rdm99-tick-runtime-rows");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let nodes = RuntimeReads::default();
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("tick")).unwrap();
        for (path, mesh) in [(PathName::new("mesh1.node", "2").unwrap(), "mesh1"), (PathName::new("mesh2.node", "1").unwrap(), "mesh2")] {
            let row = crate::storage::RuntimeRow { mesh: mesh.into(), r#type: "node".into(), node_id: NodeId::mint(), name: path.clone(), incarnation_id: IncarnationId::mint(), runtime: fact.clone(), data_dir: None };
            crate::storage::NodesStorage::put_runtime(&nodes, &row).await.unwrap();
        }
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        let mut rows = TenureRows::default();
        let names = |rows: &[crate::storage::RuntimeRow]| rows.iter().map(|r| r.name.clone()).collect::<Vec<_>>();
        for _ in 0..3 {
            assert!(rows.for_tick(false, &nodes, &me, "mesh1").await.is_empty(), "an admin that is not its mesh's primary watches no birth through runtime rows");
        }
        assert_eq!(nodes.reads(), 0, "an admin that is not its mesh's primary reads no runtime row on any tick");
        for _ in 0..3 {
            assert_eq!(names(rows.for_tick(true, &nodes, &me, "mesh1").await), vec![PathName::new("mesh1.node", "2").unwrap()], "its own mesh's rows, and no other mesh's");
        }
        assert_eq!(nodes.reads(), 1, "the mesh-primary reads the rows once, as it takes the role, and holds them while it keeps it");
        assert!(rows.for_tick(false, &nodes, &me, "mesh1").await.is_empty(), "the role lost, no row is held");
        assert_eq!(names(rows.for_tick(true, &nodes, &me, "mesh1").await), vec![PathName::new("mesh1.node", "2").unwrap()]);
        assert_eq!(nodes.reads(), 2, "a later tenure reads the rows again, once");
        // A read refused as the role is taken holds nothing; the next tick reads again.
        assert!(rows.for_tick(false, &nodes, &me, "mesh1").await.is_empty());
        nodes.refusing.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(rows.for_tick(true, &nodes, &me, "mesh1").await.is_empty(), "a refused read holds no row");
        nodes.refusing.store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(names(rows.for_tick(true, &nodes, &me, "mesh1").await), vec![PathName::new("mesh1.node", "2").unwrap()], "the tick after a refused read reads again");
        assert_eq!(names(rows.for_tick(true, &nodes, &me, "mesh1").await), vec![PathName::new("mesh1.node", "2").unwrap()]);
        assert_eq!(nodes.reads(), 4, "one refused read and one read that holds: {} reads", nodes.reads());
        assert_eq!(capture.named("rdm.node_admin.node.reject.via-runtime-rows-unreadable").len(), 1, "the refused read is named");
        let read = capture.named("rdm.node_admin.node.resolve.via-runtime-rows-read");
        assert_eq!(read.len(), 3, "each read that holds is spanned, once per tenure: {read:?}");
        assert_eq!((read[0].attributes.get("reader_path").map(String::as_str), read[0].attributes.get("reader_name").map(String::as_str), read[0].attributes.get("mesh").map(String::as_str), read[0].attributes.get("rows").map(String::as_str)), (Some("mesh1.node-admin"), Some("2"), Some("mesh1"), Some("1")), "{:?}", read[0].attributes);
        eprintln!("span rdm.node_admin.node.resolve.via-runtime-rows-read trace_id={} {:?}", read[0].trace_id, read[0].attributes);
    }

    /// CONTRACT (node-identity-and-state.md §2.7, the Meshes schema's `state`): a mesh's record holds
    /// one `state`, and it moves pending (its create's write) -> ready -> deleting -> deleted (its
    /// removal), each move one event of the state it moved to (`rdm.mesh.pending`, `.ready`,
    /// `.deleting`, `.deleted`; build.md §5), a CloudEvents 1.0 envelope by this admin's birth naming
    /// the mesh. A repeat of a state moves nothing and writes
    /// none. Must NOT happen: a move with no event, two events for one move, or an event for a state
    /// the record did not move to.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_mesh_state_moves_pending_ready_deleting_deleted_with_one_event_each() {
        use rdm_core::status::MeshState;
        let meshes = crate::storage::MemoryMeshStorage::default();
        let records = Records::default();
        let appended = std::sync::Arc::new(crate::events_storage::AppendedEvents::default());
        let (fabric_id, me) = (rdm_core::FabricId::mint(), rdm_core::NodeId::mint());
        let writer = crate::events_storage::EventsWriter::on(appended.clone(), &fabric_id, &me);
        let source = writer.source.uri();
        records.caches.writes_events(writer);
        let mesh_id = MeshId::mint();
        create_mesh(&records, &meshes, &me.to_string(), "mesh2", &mesh_id).await.expect("a free name is written");
        create_mesh(&records, &meshes, &me.to_string(), "mesh2", &mesh_id).await.expect("a re-run writes the same id");
        for state in [MeshState::Ready, MeshState::Ready, MeshState::Deleting, MeshState::Deleting] {
            let reply = crate::status_rpc::commit_mesh_state(&meshes, &records.caches, &records.declared, "mesh2", &mesh_id, state, 0).await;
            assert!(matches!(reply, rdm_core::status::StatusReply::Applied | rdm_core::status::StatusReply::AlreadyApplied), "{state:?}: {reply:?}");
        }
        assert_eq!(crate::mesh_delete::mesh_row_deleted(&meshes, &records.caches, "mesh2", &mesh_id).await, Ok("deleted-written"));
        assert_eq!(crate::mesh_delete::mesh_row_deleted(&meshes, &records.caches, "mesh2", &mesh_id).await, Ok("already-deleted"));
        let record = crate::storage::MeshStorage::mesh(&meshes, &mesh_id).await.unwrap().expect("mesh2's record");
        assert_eq!(record.state, MeshState::Deleted, "the record holds one state, the last it moved to: {record:?}");
        let events = appended.0.lock().unwrap().clone();
        let moves: Vec<(&str, serde_json::Value)> = events.iter().map(|e| (e.r#type.as_str(), serde_json::Value::Object(e.data.clone()))).collect();
        let data = serde_json::json!({"mesh_id": mesh_id.to_string(), "mesh": "mesh2"});
        use crate::events_storage::{MESH_DELETED, MESH_DELETING, MESH_PENDING, MESH_READY};
        assert_eq!(moves, [MESH_PENDING, MESH_READY, MESH_DELETING, MESH_DELETED].map(|t| (t, data.clone())).to_vec(), "one event per move, its type the state moved to, in order: {events:#?}");
        for e in &events {
            assert_eq!((e.specversion.as_str(), e.datacontenttype.as_str(), e.subject.as_ref(), e.source.as_str()), ("1.0", "application/json", None, source.as_str()), "a CloudEvents 1.0 event about no node, by this admin's birth: {e:?}");
        }
    }

    /// CONTRACT (node-identity-and-state.md §2.8, the Fabrics schema's `state`): the Fabric record holds
    /// one `state`; its fabric-primary's commit of the fabric's ready moves it pending -> ready, one
    /// `rdm.fabric.ready` event naming the fabric; a repeat moves nothing and writes none.
    /// Must NOT happen: the move with no event, or a second event for it.
    // @feature: fabric-lifecycle
    #[tokio::test]
    async fn the_fabric_state_moves_to_ready_with_one_event() {
        let fabric = crate::fabric_storage::MemoryFabricStorage::new();
        let records = Records::default();
        let appended = std::sync::Arc::new(crate::events_storage::AppendedEvents::default());
        let identity = crate::fabric_storage::FabricIdentity { fabric_id: rdm_core::FabricId::mint(), name: "fabric1".into() };
        records.caches.writes_events(crate::events_storage::EventsWriter::on(appended.clone(), &identity.fabric_id, &rdm_core::NodeId::mint()));
        crate::fabric_storage::write_identity(&fabric, &identity).await.unwrap().expect("the fabric's name is written");
        for _ in 0..2 {
            let reply = crate::status_rpc::commit_fabric_event(&fabric, &records.caches, &records.declared, &identity.fabric_id, Some("fabric1"), "ready").await;
            assert!(matches!(reply, rdm_core::status::StatusReply::Applied | rdm_core::status::StatusReply::AlreadyApplied), "{reply:?}");
        }
        let record = crate::fabric_storage::FabricStorage::fabric(&fabric).await.unwrap().expect("the Fabric record");
        assert_eq!(record.state, crate::fabric_state::FabricState::Ready, "{record:?}");
        let events = appended.0.lock().unwrap().clone();
        let moves: Vec<(&str, serde_json::Value)> = events.iter().map(|e| (e.r#type.as_str(), serde_json::Value::Object(e.data.clone()))).collect();
        assert_eq!(moves, vec![(crate::events_storage::FABRIC_READY, serde_json::json!({"fabric_id": identity.fabric_id.to_string()}))], "one rdm.fabric.ready event for the move: {events:#?}");
    }
}
