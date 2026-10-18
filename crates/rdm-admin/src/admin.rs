//! The node-admin runtime (`rafka-node-admin`).
//!
//! A node-admin is a fabric member that serves the control API. The first
//! one bootstraps the fabric (its `MESH_SPAWN_TYPE` becomes fabric policy);
//! every other one is launched by an admin's deployment pipeline and takes
//! the policy, with its first view, from the entry pull it makes to that
//! admin over QUIC (`rdm_transport::entry`). No node calls an admin's
//! HTTP API: that is for tests and people.
//!
//! Every admin holds the same two projections over iroh-gossip: fabric
//! membership (each member's digest) and the fabric's Build facts. From
//! membership it derives the observed topology it publishes on its views,
//! with every seat resolved by `election` (the lowest ready NodeId of each
//! cohort; the lowest-NodeId mesh primary holds the fabric). Admins execute Builds (`executor`): the fabric primary
//! and each mesh's admin primary claim the attempts whose next operation is
//! theirs and run what is left through the deployment pipeline (create, restart,
//! retire) and the lifecycle pipeline (a node's `Pending -> ReadyForTraffic`).

use crate::build::BuildOperation;
use crate::build_state::{BuildStateAdapter, LocalBuildLog as _};
use crate::deployment::endpoint::spec_for;
use crate::accepted::AcceptedStore;
use crate::deployment::pipeline::{adoption_missing, CurrentRuntimeAdoption, Publication, 
    CreateRequest, DeploymentPipeline, LaunchTemplate, NodeObserver, RetireRequest, Timeouts, TopologySink,
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
use rdm_transport::membership::{gossip_interval, Backbone, DigestBook, Membership};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use tracing::Instrument;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;


mod boot;
mod mesh_delete;
mod projection;
mod reconcile;
mod records;
mod recovery;
mod rows_writer;
mod runner;
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
    /// CONTRACT: the mesh primary's per-birth bookkeeping (the TRUE OFFLINE set, the inspection
    /// verdicts, the silence-status memo) holds only births its view still watches at that exact
    /// incarnation. A birth deleted by its recovery (a new node.id at its path.name) and a birth
    /// superseded by a new incarnation of its node are retired; a watched TRUE OFFLINE birth keeps
    /// all three. Must NOT happen: entries of gone births accumulating, one per recovered birth.
    #[test]
    fn the_mesh_primarys_offline_bookkeeping_retires_deleted_and_superseded_births() {
        let watched_node = |path: &str| {
            let mut n = Node::allocated(path.parse().unwrap());
            n.incarnation_id = Some(IncarnationId::mint());
            n.status = NodeStatus::Dead;
            n
        };
        let kept = watched_node("mesh2.rpc.1");
        let restarted = watched_node("mesh2.rpc.2");
        let superseded = (restarted.node_id.clone(), IncarnationId::mint());
        let deleted = (NodeId::mint(), IncarnationId::mint());
        let kept_birth = (kept.node_id.clone(), kept.incarnation_id.clone().unwrap());
        let watched: BTreeMap<String, Node> = [&kept, &restarted].into_iter().map(|n| (n.node_id.to_string(), n.clone())).collect();
        let mut offline: HashSet<_> = [kept_birth.clone(), superseded.clone(), deleted.clone()].into();
        let exited = Inspected::Exited(Some(4), "provider");
        let mut verdicts: HashMap<_, _> = [(kept_birth.clone(), exited.clone()), (superseded.clone(), exited.clone()), (deleted.clone(), exited.clone())].into();
        let mut written: WrittenSilence = [(kept.node_id.clone(), (kept.incarnation_id.clone(), Some(NodeStatus::Dead))), (deleted.0.clone(), (Some(deleted.1.clone()), Some(NodeStatus::Dead)))].into();
        clear_unwatched(&watched, &mut offline, &mut verdicts, &mut written);
        assert_eq!(offline, [kept_birth.clone()].into(), "only the watched TRUE OFFLINE birth stays offline");
        assert_eq!(verdicts.keys().cloned().collect::<HashSet<_>>(), [kept_birth.clone()].into(), "a verdict lives as long as its birth is TRUE OFFLINE");
        assert_eq!(written.keys().cloned().collect::<HashSet<_>>(), [kept.node_id.clone()].into(), "the silence memo of a deleted node is retired");
    }

    #[tokio::test]
    async fn a_recovery_that_ends_failed_is_recorded_as_its_failed_step_never_as_submitted() {
        let failed = RecoveryEnd::Failed { attempt: 3, operation: "node.delete:mesh2.rpc.1".into(), step: "NodeDeleted".into(), reason: "the exit-proof row is missing".into() };
        let (outcome, r) = settle(Ok(failed));
        assert_eq!(outcome, "failed: attempt 3 node.delete:mesh2.rpc.1 step NodeDeleted: the exit-proof row is missing");
        assert_eq!(r, Err(outcome.clone()), "a failed recovery is not a submitted one: the next tick submits again");
        let (outcome, r) = settle(Ok(RecoveryEnd::Complete { attempt: 3 }));
        assert_eq!((outcome.as_str(), r), ("complete: attempt 3", Ok(())));
        let (outcome, r) = settle(Err("the run of attempt 3 ended without its terminal".into()));
        assert_eq!(outcome, "the run of attempt 3 ended without its terminal");
        assert!(r.is_err());
    }

    /// CONTRACT: loading the node key from the data dir never waits on the disk on a runtime
    /// worker: it runs on the blocking pool (`record_store::blocking_io`), as every store read
    /// does. The admin loads it inside its boot, on the runtime. The cell loads a key whose file
    /// holds the read: the runtime's only thread keeps running, and the load answers that key.
    #[test]
    fn loading_the_node_key_waits_on_the_disk_off_the_runtime_thread() {
        let d = crate::record_store::tempdir("node-key-offload");
        let key = SecretKey::generate();
        let fifo = d.join("node-key");
        let dir = d.clone();
        let (ran, loaded) = crate::record_store::runtime_runs_while_a_fifo_is_read(fifo, hex::encode(key.to_bytes()).into_bytes(), Box::pin(async move { load_or_mint_key(&dir).await.map(|k| k.to_bytes()) }));
        assert!(ran, "the runtime thread ran while the key's read waited on the disk");
        assert_eq!(loaded, Ok(key.to_bytes()));
    }

    fn fabric1() -> FabricId {
        FabricId::parse("fab000000001").unwrap()
    }

    /// A fixed canonical id per test mesh (`mesh2` -> `meshid000002`).
    fn mesh_id(mesh: &str) -> MeshId {
        MeshId::parse(&format!("meshd{:0>7}", mesh.trim_start_matches("mesh"))).unwrap()
    }

    fn digest(name: &str, status: MemberStatus) -> MeshDigest {
        let name: PathName = name.parse().unwrap();
        MeshDigest {
            fabric_id: fabric1(),
            node: MeshNode {
                node_id: NodeId::mint(),
                name: name.clone(),
                endpoint_id: EndpointId(format!("key-{name}")),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation: IncarnationId::mint(),
                supersedes: None,
                runtime: None,
            },
            status,
            node_state: Default::default(),
            admin_api_base: (name.kind == NodeKind::NodeAdmin).then(|| format!("http://admin-{name}")),
            emitted_at_rafka_ms: 0,
            digest_seq: 1,
            mesh_id: Some(mesh_id(&name.mesh)),
            in_flight: None,
            config: BTreeMap::new(),
            load: None,
            gossip: None,
            data_dir: None,
        }
    }

    /// CONTRACT (lifecycles/states.md: "Mesh: `pending` → `ready-for-traffic` → `deleting` →
    /// `deleted` ... deleted is final"; mesh-delete.md step 7: the fabric-primary stops the deleted
    /// mesh's last admin, "from that admin's stop on it publishes the deleted mesh's aggregate"): the
    /// fabric-primary accepted mesh.deleted for mesh1 and holds mesh1's last admin, mesh1.admin.1, from
    /// that admin's own digest. The admin is stopped: the book holds it parked. The view still states
    /// mesh1 deleted, under its id, with the parked admin its one node. Must NOT happen: the parked
    /// birth carrying no mesh id into the view, so mesh1 loses its id and is projected
    /// ready-for-traffic again (handover stem: the fabric-primary re-sent mesh1 the down op for the id
    /// its view lost, 410 ms after the stop, and its view named mesh1 live).
    #[test]
    fn a_deleted_mesh_whose_last_admin_is_stopped_stays_deleted_under_its_id() {
        let book = DigestBook::default();
        let records = Records::default();
        let last = digest("mesh1.admin.1", MemberStatus::ReadyForTraffic);
        assert!(book.record(last.clone()));
        let id = last.mesh_id.clone().expect("a node-admin's digest states its mesh id");
        records.hold_mesh_deleted(&id);
        let mesh1 = |t: &Topology| t.meshes.iter().find(|m| m.name == "mesh1").map(|m| (m.id.clone(), m.status));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(mesh1(&t), Some((Some(id.clone()), ScopeStatus::Dead)), "mesh.deleted accepted: mesh1 is deleted");
        assert!(book.stopped(rdm_core::LifecycleOp {
            build_id: "b7".into(),
            attempt: 1,
            operation: "node.stop:mesh1.admin.1".into(),
            node_id: last.node.node_id.clone(),
            incarnation: last.node.incarnation.clone(),
            name: last.node.name.clone(),
            event_at_rafka_ms: 1,
        }));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        let n = t.node(&last.node.name).expect("the stopped admin is listed");
        assert!(n.parked, "listed parked: {n:?}");
        assert_eq!(mesh1(&t), Some((Some(id), ScopeStatus::Dead)), "a deleted mesh whose last admin is stopped stays deleted, under its id");
    }

    /// CONTRACT (mesh-delete.md step 7: "Departed births resolve Gone"; R-MN1: "a mesh created again
    /// is a new mesh"): a contact is a bootstrap hint for a birth this admin holds, never a record of
    /// one that is gone. A birth the book holds departed (node.delete's NodeDeleted, or mesh-left,
    /// which departs every birth of the removed mesh) is dropped from the contacts, so a maker's
    /// stored map never hands a removed mesh's births to the first admin of a mesh created again
    /// under the same name. A contact replaced at its path by another logical node is dropped as
    /// before; a contact of a birth that is simply not held now (silent, not departed) is kept.
    #[test]
    fn a_departed_births_contact_is_dropped_and_a_silent_ones_is_kept() {
        let record = |d: &MeshDigest| crate::storage::NodeRecord { node_id: d.node.node_id.clone(), fabric_id: d.fabric_id.clone(), name: d.node.name.clone(), mesh_id: mesh_id(&d.node.name.mesh), incarnation_id: d.node.incarnation.clone(), endpoint_id: d.node.endpoint_id.clone(), transport_addr: d.node.transport_addr, listeners: Vec::new() };
        let (departed, silent, replaced, successor) = (digest("mesh2.rpc.1", MemberStatus::Leaving), digest("mesh2.rpc.2", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic));
        let written: HashMap<NodeId, crate::storage::NodeRecord> = [&departed, &silent, &replaced].into_iter().map(|d| (d.node.node_id.clone(), record(d))).collect();
        let mut dropped = dropped_contacts(&written, std::slice::from_ref(&successor), |id| id == &departed.node.node_id);
        dropped.sort();
        let mut want = vec![departed.node.node_id.clone(), replaced.node.node_id.clone()];
        want.sort();
        assert_eq!(dropped, want, "the departed birth's and the replaced birth's contacts go; the silent one's stays");
    }

    /// CONTRACT (R-OW1: "`admin_api_base` is config, never checked"; lifecycles/node-name-update.md: "Config on the
    /// row, `admin_api_base` included, is stored and gossiped as given; nothing checks it"): a
    /// node-admin birth this admin's book holds ready-for-traffic is Ready, its digest carrying no
    /// `admin_api_base`. What must NOT happen: a node-admin refused Ready for a config value RDM
    /// never reads.
    #[tokio::test]
    async fn a_node_admin_with_no_admin_api_base_is_ready_once_its_book_holds_it_ready_for_traffic() {
        let book = DigestBook::default();
        let mut d = digest("mesh1.admin.2", MemberStatus::ReadyForTraffic);
        d.admin_api_base = None;
        assert!(book.record(d.clone()), "the book takes the node-admin's digest");
        assert_eq!(book.get(d.node.node_id.as_str()).map(|(held, _)| held.status), Some(MemberStatus::ReadyForTraffic), "the book holds the birth ready-for-traffic");
        let mut node = Node::allocated(d.node.name.clone());
        node.node_id = d.node.node_id.clone();
        node.incarnation_id = Some(d.node.incarnation.clone());
        let observer = MembershipObserver { book, client: None, commands: Arc::default(), authority: Arc::default(), handover: Arc::default(), unreachable: Arc::default() };
        assert_eq!(crate::deployment::pipeline::NodeObserver::ready(&observer, &node).await, Ok(()));
    }

    /// A nodes store that counts the contact writes made through it.
    struct CountingNodes(Arc<dyn crate::storage::NodesStorage>, std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for CountingNodes {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::record_store::StorageError> {
            self.0.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.0.runtimes().await
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.0.runtime(node_id, incarnation).await
        }
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.0.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.0.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.0.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), crate::record_store::StorageError> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<Result<(), crate::storage::NameHeld>, crate::record_store::StorageError> {
            self.0.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &PathName) -> Result<Option<crate::storage::NodeRow>, crate::record_store::StorageError> {
            self.0.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, crate::record_store::StorageError> {
            self.0.node_row_name(mesh, node_id).await
        }
        async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.0.hold_node_row(name, holder).await
        }
        async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.0.release_node_row(name, holder).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, crate::record_store::StorageError> {
            self.0.tagged(label).await
        }
    }

    fn contact_names(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir).map(|d| d.filter_map(|e| e.ok()).filter(|e| e.file_name().to_string_lossy().ends_with(".json")).count()).unwrap_or(0)
    }

    /// CONTRACT (R14 item 2, the writer ladder node-identity-and-state.md §4.3; #3070 "the contacts
    /// family must follow the ladder"): on one shared root, a mesh's contacts and its Mesh row are
    /// written by that mesh's primary alone, for its own members alone. A seated and an unseated
    /// admin of mesh1 run over the same held births at once: the unseated one writes nothing, the
    /// seated one writes mesh1's members (itself included) and mesh1's Mesh row, never mesh2's. The
    /// seat moves: the new holder writes the replaced member's new contact and drops the old one,
    /// and the old holder writes nothing more. A launched birth's publication writes no contact.
    #[tokio::test]
    async fn a_meshs_contacts_and_mesh_row_are_written_by_its_primary_alone_on_the_shared_root() {
        let d = crate::record_store::tempdir("contacts-ladder");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let store = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(CountingNodes(Arc::new(crate::storage::FileNodesStorage::open(&data, &root, "mesh1", &IncarnationId::mint()).await.unwrap()), Default::default())) }
        };
        let (p_nodes, q_nodes) = (store("mesh1.admin.1").await, store("mesh1.admin.2").await);
        let (p_meshes, q_meshes): (Arc<dyn crate::storage::MeshStorage>, Arc<dyn crate::storage::MeshStorage>) =
            (Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap()), Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap()));
        let (p, q, rpc, other) = (digest("mesh1.admin.1", MemberStatus::ReadyForTraffic), digest("mesh1.admin.2", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh2.admin.1", MemberStatus::ReadyForTraffic));
        let held = vec![p.clone(), q.clone(), rpc.clone(), other.clone()];
        let mut pw = MeshRowsWriter::new("mesh1.admin.1", fabric1(), "mesh1", mesh_id("mesh1"), p_nodes.clone(), p_meshes);
        let mut qw = MeshRowsWriter::new("mesh1.admin.2", fabric1(), "mesh1", mesh_id("mesh1"), q_nodes.clone(), q_meshes);
        tokio::join!(pw.round(&held, true, |_| false), qw.round(&held, false, |_| false));
        let contacts = root.dir().join("nodes").join("mesh1").join("contacts");
        assert_eq!(contact_names(&contacts), 3, "mesh1's primary writes its three members");
        assert_eq!(contact_names(&root.dir().join("nodes").join("mesh2").join("contacts")), 0, "mesh1's primary writes no mesh2 contact");
        assert_eq!(q_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 0, "an unseated admin writes no contact");
        let mesh_rows: Vec<String> = std::fs::read_dir(root.dir().join("meshes")).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).filter(|n| n.ends_with(".json")).collect();
        assert_eq!(mesh_rows, ["mesh1.json"], "mesh1's primary writes mesh1's row alone");
        // The seat moves to Q; mesh1.rpc.1 is replaced by a new node at its path.name.
        let recovered = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        let held = vec![p.clone(), q.clone(), recovered.clone(), other.clone()];
        let before = p_nodes.1.load(std::sync::atomic::Ordering::SeqCst);
        tokio::join!(pw.round(&held, false, |_| false), qw.round(&held, true, |_| false));
        assert_eq!(p_nodes.1.load(std::sync::atomic::Ordering::SeqCst), before, "the old holder writes nothing once the seat moved");
        let now = crate::storage::NodesStorage::contacts(&*q_nodes).await.unwrap();
        assert_eq!(now.len(), 3, "{now:?}");
        assert!(now.iter().any(|c| c.node_id == recovered.node.node_id) && !now.iter().any(|c| c.node_id == rpc.node.node_id), "the new holder replaced the member's contact");
        // A launched birth's publication writes no contact.
        let records = Records::default();
        let _ = records.contacts.set(q_nodes.clone());
        let writes = q_nodes.1.load(std::sync::atomic::Ordering::SeqCst);
        records.publish(crate::reenter::MapNode { node_id: NodeId::mint(), name: "mesh1.rpc.2".parse().unwrap(), endpoint_id: EndpointId("key-x".into()), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: IncarnationId::mint(), transition: None, ready: false, data_dir: None, admin_api_base: None }.as_node(NodeStatus::Pending, ProviderKind::Process));
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(q_nodes.1.load(std::sync::atomic::Ordering::SeqCst), writes, "a publication wrote a contact");
    }

    /// CONTRACT (node-identity-and-state.md §2.7: the mesh-primary "claims the mesh's name by
    /// insert-and-fail on the key `(path, name)` ... Held means held by a different MeshId:
    /// `NameHeld` names that holder"; "A mesh whose name another mesh's row already holds stays held
    /// as `name-taken`, naming the holder; it is never renamed, and the id never moves"; CLAUDE.md:
    /// the claim is the first durable write of a create, nothing is written for the entity until it
    /// wins): on one shared root, mesh A's primary claims `mesh2` and writes its row and its member's
    /// contact. A second mesh under the same name (another MeshId) has its primary's rounds refused by
    /// name: the row still names A, the second primary writes no contact, and it claims once per seat
    /// tenure: one `rdm.node_admin.mesh.create.via-mesh-row` span `name-held` naming A, however many
    /// rounds run. Must NOT happen: the second mesh's put overwriting A's row, its contacts written
    /// beside a refused claim, or a refused claim tried again on every birth change.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_second_mesh_of_a_live_name_is_refused_by_name_and_writes_nothing() {
        let d = crate::record_store::tempdir("mesh-claim-live");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let store = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(CountingNodes(Arc::new(crate::storage::FileNodesStorage::open(&data, &root, "mesh2", &IncarnationId::mint()).await.unwrap()), Default::default())) }
        };
        let (a_nodes, b_nodes) = (store("a.admin").await, store("b.admin").await);
        let (a_meshes, b_meshes): (Arc<dyn crate::storage::MeshStorage>, Arc<dyn crate::storage::MeshStorage>) =
            (Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap()), Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap()));
        let (first, second) = (mesh_id("mesh2"), MeshId::mint());
        let (a_admin, b_admin) = (digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), digest("mesh2.admin.1", MemberStatus::ReadyForTraffic));
        let capture = crate::span_capture::Capture::new("i143-3089-mesh-name-held");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let mut aw = MeshRowsWriter::new("mesh2.admin.1", fabric1(), "mesh2", first.clone(), a_nodes.clone(), a_meshes.clone());
        aw.round(std::slice::from_ref(&a_admin), true, |_| false).await;
        assert_eq!(crate::storage::MeshStorage::mesh(&*a_meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(first.clone()), "mesh A's primary claims mesh2 and writes its row");
        assert_eq!(a_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 1, "mesh A's primary writes its member's contact once its claim won");
        let mut bw = MeshRowsWriter::new("mesh2.admin.1", fabric1(), "mesh2", second.clone(), b_nodes.clone(), b_meshes.clone());
        for _round in 0..2 {
            bw.round(std::slice::from_ref(&b_admin), true, |_| false).await;
        }
        assert_eq!(crate::storage::MeshStorage::mesh(&*b_meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(first.clone()), "the second mesh's claim of a live name writes no row: mesh2's row still names mesh A");
        assert_eq!(b_nodes.1.load(std::sync::atomic::Ordering::SeqCst), 0, "the second mesh's primary writes no contact beside its refused claim");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("mesh_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("holder").cloned().unwrap_or_default())).collect();
        assert_eq!(
            said,
            vec![(first.to_string(), "row-written".to_string(), String::new()), (second.to_string(), "name-held".to_string(), first.to_string())],
            "one claim per seat tenure, the refusal naming the holder: {spans:?}"
        );
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[1].trace_id, spans[1].attributes);
    }

    /// CONTRACT (R13: the fabric-primary writes as the acting primary of a mesh with no primary;
    /// lifecycles/node-create.md "Mesh absent ... Until the mesh has a primary, the fabric-primary writes
    /// its rows"; CLAUDE.md: the claim is the first durable write of a create, nothing is written for the
    /// entity until it wins; node-identity-and-state.md §2.7: `NameHeld` names the holder, the same MeshId
    /// re-claiming is idempotent): `create-mesh:mesh2` at the fabric-primary claims mesh2's name first.
    /// On a free key it writes the Mesh row under the id the operation carries and holds that id for the
    /// mesh's launches; a re-run of the operation claims the same id and finds its own row. Where another mesh's
    /// live row holds mesh3, the operation fails naming the holder: mesh3's row stands and no id is held
    /// for mesh3, so no launch can carry one and the Build runs no create of mesh3's nodes. One
    /// `rdm.node_admin.mesh.create.via-mesh-row` span per claim, naming the fabric-primary. Must NOT
    /// happen: a mesh id held, or anything of the mesh written, beside a refused claim.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn create_mesh_claims_the_mesh_name_first_and_a_refused_claim_holds_nothing_of_the_mesh() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let records = Records::default();
        let holder = MeshId::mint();
        let theirs = crate::storage::MeshRecord { mesh_id: holder.clone(), name: "mesh3".into(), dead: false };
        assert_eq!(crate::storage::MeshStorage::claim_mesh(&meshes, &theirs).await, Ok(Ok(crate::storage::MeshClaim::Written { over: None })));
        // The ids the two operations carry, minted when each mesh entered the accepted topology.
        let (carried, mesh3_carried) = (MeshId::mint(), MeshId::mint());
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-claim");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let minted = create_mesh(&records, &meshes, "mesh1.admin.1", "mesh2", &carried).await.expect("a free name is claimed");
        assert_eq!(minted, carried, "the claim is made for the id the operation carries");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh2").await.unwrap(), Some(crate::storage::MeshRecord { mesh_id: minted.clone(), name: "mesh2".into(), dead: false }), "the claim wrote mesh2's row under the carried id");
        assert_eq!(records.meshes.lock().unwrap().get("mesh2"), Some(&minted), "the won claim's id is what the mesh's launches carry");
        assert_eq!(create_mesh(&records, &meshes, "mesh1.admin.1", "mesh2", &carried).await, Ok(minted.clone()), "a re-run claims the same id");
        let refused = create_mesh(&records, &meshes, "mesh1.admin.1", "mesh3", &mesh3_carried).await.expect_err("mesh3's name is held by another mesh's live row");
        let refused = refused.to_string();
        assert!(refused.contains("create-mesh:mesh3") && refused.contains(&holder.to_string()), "the refusal names the operation and the holder: {refused}");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh3").await.unwrap(), Some(theirs), "the refused claim wrote nothing: mesh3's row is the holder's");
        assert_eq!(records.meshes.lock().unwrap().get("mesh3"), None, "no id is held for a mesh whose claim was refused: no launch of it can carry one");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String, String)> = spans
            .iter()
            .map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("mesh").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("holder").cloned().unwrap_or_default()))
            .collect();
        let row = |mesh: &str, outcome: &str, holder: &str| ("mesh1.admin.1".to_string(), mesh.to_string(), outcome.to_string(), holder.to_string());
        assert_eq!(said, vec![row("mesh2", "row-written", ""), row("mesh2", "row-held-by-mesh", ""), row("mesh3", "name-held", &holder.to_string())], "{spans:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[2].trace_id, spans[2].attributes);
    }

    /// CONTRACT (node-identity-and-state.md §2.7: "Who mints it. The Build that creates a mesh's first
    /// node-admin mints the mesh id, never reissued"; "The same MeshId re-claiming its own name, at
    /// state-commit, on a retry or from a replaced primary, is idempotent and accepted"): the accepted
    /// Build that creates mesh2 is planned by its fabric-primary and, after that admin is lost, by its
    /// successor, which holds nothing of its predecessor's memory. Each runs the plan's
    /// `create-mesh:mesh2` over the one mesh store: the first claim writes mesh2's row, and the
    /// successor's re-run claims with the same MeshId and finds its own row (`row-held-by-mesh`), so
    /// the id its launches carry is the row's. Must NOT happen: the successor minting another MeshId
    /// and being refused `name-held` by its own mesh's row.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_successor_re_running_create_mesh_of_the_same_build_claims_with_its_mesh_id_and_finds_its_own_row() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let observed = view(&[digest("mesh1.admin.1", MemberStatus::ReadyForTraffic)]);
        let current = crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap());
        let change = crate::accepted::TopologyChange::CreateMesh { desired: crate::build::MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]) };
        let accepted = crate::accepted::compile(&current, &change, &observed).expect("mesh2 is accepted");
        let create_mesh_of = |plan: crate::build::BuildPlan| plan.operations.into_iter().find(|op| matches!(op, BuildOperation::CreateMesh { .. })).expect("the plan creates mesh2");
        let (first_op, successor_op) = (create_mesh_of(crate::accepted::plan(&accepted, &observed, None)), create_mesh_of(crate::accepted::plan(&accepted, &observed, None)));
        let (BuildOperation::CreateMesh { mesh: first_mesh, mesh_id: first_id }, BuildOperation::CreateMesh { mesh: successor_mesh, mesh_id: successor_id }) = (&first_op, &successor_op) else { unreachable!() };
        let minted = &accepted.mesh("mesh2").expect("the accepted topology holds mesh2").mesh_id;
        assert_eq!((first_id, successor_id), (minted, minted), "every plan of the Build carries the MeshId minted when mesh2 was accepted");
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-rerun");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (first, successor) = (Records::default(), Records::default());
        let claimed = create_mesh(&first, &meshes, "mesh1.admin.1", first_mesh, first_id).await.expect("the first run claims mesh2's free name");
        assert_eq!(&claimed, minted, "the first run claims with the Build's MeshId");
        let rerun = create_mesh(&successor, &meshes, "mesh1.admin.2", successor_mesh, successor_id).await;
        assert_eq!(rerun, Ok(claimed.clone()), "the successor's re-run of the same Build's create-mesh claims with the same MeshId and finds its own row");
        assert_eq!(successor.meshes.lock().unwrap().get("mesh2"), Some(&claimed), "the successor's launches of mesh2 carry the id mesh2's row holds");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(claimed.clone()), "mesh2's row is the first claim's");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("mesh_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(said, vec![("mesh1.admin.1".to_string(), claimed.to_string(), "row-written".to_string()), ("mesh1.admin.2".to_string(), claimed.to_string(), "row-held-by-mesh".to_string())], "{spans:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[1].trace_id, spans[1].attributes);
    }

    /// The accepted Build that creates mesh2 beside mesh1, held by a Build log, and its `create-mesh:mesh2`
    /// operation as the fabric-primary plans it.
    async fn build_creating_mesh2(builds: &crate::build_state::MemoryBuildStateAdapter) -> (crate::build::BuildId, BuildOperation) {
        let observed = view(&[digest("mesh1.admin.1", MemberStatus::ReadyForTraffic)]);
        let current = crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap());
        let change = crate::accepted::TopologyChange::CreateMesh { desired: crate::build::MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)]) };
        let accepted = crate::accepted::compile(&current, &change, &observed).expect("mesh2 is accepted");
        let op = crate::accepted::plan(&accepted, &observed, None).operations.into_iter().find(|op| matches!(op, BuildOperation::CreateMesh { .. })).expect("the plan creates mesh2");
        let build_id = crate::build::BuildId::parse("jrr142w4840j").unwrap();
        builds.publish_accepted(&crate::build_state::BuildAccepted { build_id: build_id.clone(), topology: accepted, submitted_change: Some(change), submitted_at_ms: 0 }).await.expect("the Build is accepted");
        (build_id, op)
    }

    /// The receipts of `operation` the Build holds: `(attempt, step, outcome, executor)`.
    async fn receipts_of(builds: &crate::build_state::MemoryBuildStateAdapter, build_id: &crate::build::BuildId, operation: &str) -> Vec<(u32, String, crate::build_state::StepOutcome, Option<String>)> {
        builds.read_build(build_id).await.expect("the Build is held").steps.into_iter().filter(|r| r.operation == operation).map(|r| (r.attempt, r.step, r.outcome, r.executor)).collect()
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: "completed step receipts are read and replayed, never
    /// executed again"; Build carrier: "Completed steps are not rerun"): `create-mesh:mesh2` runs at the
    /// fabric-primary, claims mesh2's name and commits its `Complete` receipt. A successor fabric-primary
    /// that runs the same Build's `create-mesh:mesh2` reads that receipt: it makes no second claim, writes
    /// no second receipt, and holds the MeshId the Build carries for its launches of mesh2. One
    /// `rdm.node_admin.mesh.create.via-mesh-row` span (the first run's `row-written`), and a
    /// `rdm.node_admin.mesh.create.via-receipt` span per run: `complete`, then `replayed`. Must NOT happen:
    /// the successor claiming again (`row-held-by-mesh`), or a second `Complete` receipt of the step.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_completed_create_mesh_is_receipted_and_a_successor_replays_the_receipt_instead_of_claiming_again() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let BuildOperation::CreateMesh { mesh_id, .. } = &op else { unreachable!() };
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-receipt");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let (first, successor) = (Records::default(), Records::default());
        create_mesh_step(&first, &meshes, &builds, "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect("the first run claims mesh2's free name");
        create_mesh_step(&successor, &meshes, &builds, "key2@127.0.0.1:2", "mesh1.admin.2", &build_id, 2, &op).await.expect("the successor's run of the same step");
        assert_eq!(
            receipts_of(&builds, &build_id, "create-mesh:mesh2").await,
            vec![(1, "Complete".to_string(), crate::build_state::StepOutcome::Complete, Some("key1@127.0.0.1:1".to_string()))],
            "the step is receipted once, by the run that executed it"
        );
        assert_eq!(successor.meshes.lock().unwrap().get("mesh2"), Some(mesh_id), "the successor's launches of mesh2 carry the id the Build carries");
        let claims = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let claimed: Vec<(String, String)> = claims.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(claimed, vec![("mesh1.admin.1".to_string(), "row-written".to_string())], "the successor makes no second claim: {claims:?}");
        let steps = capture.named("rdm.node_admin.mesh.create.via-receipt");
        let said: Vec<(String, String)> = steps.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(said, vec![("mesh1.admin.1".to_string(), "complete".to_string()), ("mesh1.admin.2".to_string(), "replayed".to_string())], "{steps:?}");
        eprintln!("span rdm.node_admin.mesh.create.via-receipt trace_id={} {:?}", steps[1].trace_id, steps[1].attributes);
    }

    /// CONTRACT (node-rpc-envelope.md, Build carrier: "Settle an idempotent uncertain step by its natural
    /// key"; node-identity-and-state.md §2.7: the same MeshId re-claiming its name is idempotent): the
    /// fabric-primary claimed mesh2's name and was lost before it receipted the step, so the Build holds no
    /// receipt of it. The successor's run is uncertain: it claims by the step's natural key, the mesh's
    /// own row answers `row-held-by-mesh`, and the step is receipted `Complete` at the successor's
    /// attempt. Must NOT happen: a refused claim, or a step that ends with no receipt.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_create_mesh_whose_receipt_was_never_written_settles_by_its_natural_key_and_is_receipted() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let BuildOperation::CreateMesh { mesh, mesh_id } = &op else { unreachable!() };
        let capture = crate::span_capture::Capture::new("i143-3089-create-mesh-uncertain");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        // The lost fabric-primary: its claim landed, and its receipt did not.
        create_mesh(&Records::default(), &meshes, "mesh1.admin.1", mesh, mesh_id).await.expect("the lost fabric-primary's claim");
        assert_eq!(receipts_of(&builds, &build_id, "create-mesh:mesh2").await, vec![], "the lost run left no receipt");
        let successor = Records::default();
        create_mesh_step(&successor, &meshes, &builds, "key2@127.0.0.1:2", "mesh1.admin.2", &build_id, 2, &op).await.expect("the uncertain step settles by its natural key");
        assert_eq!(receipts_of(&builds, &build_id, "create-mesh:mesh2").await, vec![(2, "Complete".to_string(), crate::build_state::StepOutcome::Complete, Some("key2@127.0.0.1:2".to_string()))]);
        assert_eq!(successor.meshes.lock().unwrap().get("mesh2"), Some(mesh_id));
        let claims = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let claimed: Vec<(String, String)> = claims.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(claimed, vec![("mesh1.admin.1".to_string(), "row-written".to_string()), ("mesh1.admin.2".to_string(), "row-held-by-mesh".to_string())], "{claims:?}");
        let steps = capture.named("rdm.node_admin.mesh.create.via-receipt");
        let said: Vec<(String, String)> = steps.iter().map(|s| (s.attributes.get("node").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default())).collect();
        assert_eq!(said, vec![("mesh1.admin.2".to_string(), "complete".to_string())], "{steps:?}");
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
        let claim = |attempt: u32, executor: &str| crate::build_state::BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt, executor: executor.into() };
        assert_eq!(builds.claim_attempt(&claim(1, "key1@127.0.0.1:1")).await.unwrap(), crate::build_state::ClaimOutcome::Won);
        create_mesh_step(&Records::default(), &meshes, &builds, "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect("the first run");
        builds.append_attempt_receipt(&crate::build_state::BuildAttemptReceipt { mesh: None, build_id: build_id.clone(), attempt: 1, outcome: crate::build_state::AttemptOutcome::Converged }).await.unwrap();
        builds
            .open_attempt(&crate::build_state::AttemptOpened { mesh: None, build_id: build_id.clone(), attempt: 2, reason: crate::build_state::AttemptReason::ProvenDrift, action: None, opened_by: "mesh1.admin.1".into(), opened_at_ms: 1 })
            .await
            .expect("the Build is reopened");
        assert_eq!(builds.read_build(&build_id).await.unwrap().opened, 2, "attempt 2 begins the Build's current run");
        assert_eq!(builds.claim_attempt(&claim(2, "key2@127.0.0.1:2")).await.unwrap(), crate::build_state::ClaimOutcome::Won);
        create_mesh_step(&Records::default(), &meshes, &builds, "key2@127.0.0.1:2", "mesh1.admin.2", &build_id, 2, &op).await.expect("the reopened run");
        let receipts = receipts_of(&builds, &build_id, "create-mesh:mesh2").await;
        assert_eq!(receipts.iter().map(|r| (r.0, r.1.as_str())).collect::<Vec<_>>(), vec![(1, "Complete"), (2, "Complete")], "each run receipts its own step: {receipts:?}");
        let claims: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-mesh-row").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(claims, ["row-written", "row-held-by-mesh"], "the reopened run claims by the natural key");
        let steps: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-receipt").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(steps, ["complete", "complete"]);
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: "Durable step outcomes remain Complete and Failed"):
    /// mesh2's name is held by another mesh's live row, so `create-mesh:mesh2` is refused by name. The
    /// step `ClaimMeshName` is receipted `Failed` with the refusal's reason (the holder named), holds no id
    /// for mesh2, and the operation is never receipted `Complete`. Must NOT happen: a `Complete` receipt beside a refused claim.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_create_mesh_refused_by_name_is_receipted_failed_naming_the_holder() {
        let meshes = crate::storage::MemoryMeshStorage::default();
        let builds = crate::build_state::MemoryBuildStateAdapter::new();
        let (build_id, op) = build_creating_mesh2(&builds).await;
        let holder = MeshId::mint();
        crate::storage::MeshStorage::claim_mesh(&meshes, &crate::storage::MeshRecord { mesh_id: holder.clone(), name: "mesh2".into(), dead: false }).await.unwrap().unwrap();
        let records = Records::default();
        let refused = create_mesh_step(&records, &meshes, &builds, "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect_err("mesh2's name is held by another mesh");
        assert!(refused.contains("create-mesh:mesh2") && refused.contains(&holder.to_string()), "{refused}");
        let receipts = receipts_of(&builds, &build_id, "create-mesh:mesh2").await;
        assert_eq!(receipts.len(), 1, "{receipts:?}");
        let (attempt, step, outcome, executor) = &receipts[0];
        assert_eq!((*attempt, step.as_str(), executor.as_deref()), (1, "ClaimMeshName", Some("key1@127.0.0.1:1")));
        assert!(matches!(outcome, crate::build_state::StepOutcome::Failed { reason, .. } if reason.contains(&holder.to_string())), "the Failed receipt names the holder: {outcome:?}");
        assert_eq!(records.meshes.lock().unwrap().get("mesh2"), None, "no id is held for a mesh whose claim was refused");
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
            Err(crate::build_state::BuildStateError::Io(format!("the journal refused the {} receipt of {}", r.step, r.operation)))
        }
        async fn append_attempt_receipt(&self, r: &crate::build_state::BuildAttemptReceipt) -> Result<(), crate::build_state::BuildStateError> {
            self.0.append_attempt_receipt(r).await
        }
        async fn facts(&self) -> Result<Vec<crate::build_state::BuildFact>, crate::build_state::BuildStateError> {
            self.0.facts().await
        }
        async fn forget(&self, id: &crate::build::BuildId) -> Result<(), crate::build_state::BuildStateError> {
            self.0.forget(id).await
        }
        async fn recovery_of(&self, p: &crate::model::PathName, n: &NodeId, i: &IncarnationId) -> Result<Option<(crate::build::BuildId, Option<String>, u32, String)>, crate::build_state::BuildStateError> {
            self.0.recovery_of(p, n, i).await
        }
        async fn mesh_deleted_of(&self, n: &NodeId, i: &IncarnationId) -> Result<Option<(crate::build::BuildId, u32, String)>, crate::build_state::BuildStateError> {
            self.0.mesh_deleted_of(n, i).await
        }
        async fn terminate_failures_of(&self, n: &NodeId, i: &IncarnationId) -> Result<Vec<crate::build_state::FailedTerminate>, crate::build_state::BuildStateError> {
            self.0.terminate_failures_of(n, i).await
        }
        async fn exits_owed(&self, id: &crate::build::BuildId, mesh: &str) -> Result<Vec<crate::build_state::OwedExit>, crate::build_state::BuildStateError> {
            self.0.exits_owed(id, mesh).await
        }
    }

    /// A mesh store that does not answer.
    struct UnansweredMeshes;

    #[async_trait::async_trait]
    impl crate::storage::MeshStorage for UnansweredMeshes {
        async fn mesh(&self, _: &str) -> Result<Option<crate::storage::MeshRecord>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Io { file: "meshes".into(), reason: "the disk did not answer".into() })
        }
        async fn claim_mesh(&self, _: &crate::storage::MeshRecord) -> Result<Result<crate::storage::MeshClaim, crate::storage::MeshClaimRefused>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Io { file: "meshes/mesh2.json".into(), reason: "the disk did not answer".into() })
        }
        async fn meshes(&self) -> Result<Vec<crate::storage::MeshRecord>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Io { file: "meshes".into(), reason: "the disk did not answer".into() })
        }
    }

    /// CONTRACT (node-rpc-envelope.md, Build op 0x20: a step is committed by its receipt; CLAUDE.md: an
    /// error state names what failed and where, distinct facts distinct arms): `create-mesh:mesh2` fails
    /// by its own name when (1) the Build cannot be read for its receipts, so it claims nothing, (2) the
    /// mesh store does not answer the claim, so it receipts nothing, and (3) the Build state refuses the
    /// `Complete` receipt after the claim won: the step is uncommitted, and the next run settles it by its
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
        // (1) A Build the log never accepted: nothing is claimed.
        let unknown = crate::build::BuildId::parse("kpnnk1nrvac1").unwrap();
        let unread = create_mesh_step(&Records::default(), &meshes, &*memory, "key1@127.0.0.1:1", "mesh1.admin.1", &unknown, 1, &op).await.expect_err("the Build is not held");
        assert!(unread.contains("could not be read for its receipts") && unread.contains("kpnnk1nrvac1"), "{unread}");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh2").await.unwrap(), None, "nothing was claimed beside a Build that could not be read");
        // (2) The mesh store does not answer: nothing is receipted.
        let unanswered = create_mesh_step(&Records::default(), &UnansweredMeshes, &*memory, "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect_err("the mesh store does not answer");
        assert!(unanswered.contains("did not answer") && unanswered.contains("mesh2"), "{unanswered}");
        assert_eq!(receipts_of(&memory, &build_id, "create-mesh:mesh2").await, vec![], "an undecided step is not receipted");
        // (3) The claim won and the receipt is refused: the step is uncommitted.
        let refused = create_mesh_step(&Records::default(), &meshes, &RefusesReceipts(memory.clone()), "key1@127.0.0.1:1", "mesh1.admin.1", &build_id, 1, &op).await.expect_err("the Build state refuses the receipt");
        assert!(refused.contains("Complete receipt of create-mesh:mesh2") && refused.contains("was not written") && refused.contains("journal refused"), "{refused}");
        assert_eq!(crate::storage::MeshStorage::mesh(&meshes, "mesh2").await.unwrap().map(|m| m.mesh_id), Some(mesh_id.clone()), "the claim stands");
        assert_eq!(receipts_of(&memory, &build_id, "create-mesh:mesh2").await, vec![], "the refused receipt is not in the log");
        // The next run settles the uncertain step by its natural key.
        create_mesh_step(&Records::default(), &meshes, &*memory, "key2@127.0.0.1:2", "mesh1.admin.2", &build_id, 2, &op).await.expect("the next run settles the step");
        assert_eq!(receipts_of(&memory, &build_id, "create-mesh:mesh2").await.len(), 1);
        let outcomes: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-receipt").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(outcomes, ["build-unreadable", "store-unanswered", "receipt-refused", "complete"]);
        let claims: Vec<String> = capture.named("rdm.node_admin.mesh.create.via-mesh-row").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(claims, ["row-refused", "row-written", "row-held-by-mesh"], "the claim made beside the unanswered store is refused by name; the settled run finds the mesh's own row");
    }

    /// CONTRACT (node-identity-and-state.md §2.8: "The fabric-primary claims the fabric's name by
    /// insert-and-fail on its `(path, name)` ... idempotent for the same FabricId and `NameHeld` naming a
    /// different one"; CLAUDE.md: the claim is the first durable write of a create, nothing is written
    /// for the entity until it wins): `fabric/` holds another Fabric's identity (a root an embedding app
    /// handed in). This fabric-primary's rounds claim the fabric's name, are refused naming the holder,
    /// and write nothing of the fabric: the held identity stands, and the claim is made once, one
    /// `rdm.node_admin.fabric.create.via-fabric-row` span `name-held` naming the holder. Must NOT happen:
    /// a row written into another Fabric's `fabric/` beside a refused claim.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_fabric_primarys_refused_fabric_claim_writes_nothing_of_the_fabric() {
        let storage: Arc<dyn crate::fabric_storage::FabricStorage> = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        let (holder, ours) = (crate::fabric_storage::FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() }, crate::fabric_storage::FabricIdentity { fabric_id: FabricId::mint(), name: "fabric1".into() });
        let birth = |revision: u64| rdm_core::SeatHolder { mesh: "mesh1".into(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), revision };
        let (h_birth, o_birth) = (birth(1), birth(2));
        let capture = crate::span_capture::Capture::new("i143-3089-fabric-name-held");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let h_seats = rdm_transport::membership::SeatBook::default();
        h_seats.take(rdm_core::Seat::FabricPrimary, &h_birth);
        let mut hw = FabricRowsWriter::new((h_birth.node_id.clone(), h_birth.incarnation.clone()), holder.clone(), storage.clone());
        hw.round(&h_seats).await;
        assert_eq!(storage.fabric().await.unwrap(), Some(holder.clone()), "the holder's fabric-primary writes its identity once its claim won");
        let o_seats = rdm_transport::membership::SeatBook::default();
        o_seats.take(rdm_core::Seat::FabricPrimary, &o_birth);
        let mut ow = FabricRowsWriter::new((o_birth.node_id.clone(), o_birth.incarnation.clone()), ours.clone(), storage.clone());
        for _round in 0..2 {
            ow.round(&o_seats).await;
        }
        assert_eq!(storage.fabric().await.unwrap().map(|f| f.fabric_id), Some(holder.fabric_id.clone()), "the held identity stands");
        let spans = capture.named("rdm.node_admin.fabric.create.via-fabric-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("fabric_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("holder").cloned().unwrap_or_default())).collect();
        assert_eq!(
            said,
            vec![(holder.fabric_id.to_string(), "row-written".to_string(), String::new()), (ours.fabric_id.to_string(), "name-held".to_string(), holder.fabric_id.to_string())],
            "one claim per writer, the refusal naming the holder: {spans:?}"
        );
        eprintln!("span rdm.node_admin.fabric.create.via-fabric-row trace_id={} {:?}", spans[1].trace_id, spans[1].attributes);
    }

    /// CONTRACT (R-MN1: a mesh's name is released only by its removal, the row Dead, and "a mesh created
    /// again under the same name is a new mesh with a new id"; change 4: "the mesh-name claim treats a
    /// Dead mesh row as released"; states.md: dead is final, a mesh id is never reissued): mesh A's
    /// primary claims `mesh2`; the removal writes A's row Dead (`dead-written`, a repeat
    /// `already-dead`); mesh2 created again under a new MeshId has its primary claim the name over the
    /// Dead row, the span naming the Dead mesh it took it over, and its contact is written. A's own
    /// later tenure claims nothing live again (`mesh-dead`). Must NOT happen: the recreated mesh
    /// refused by the removed mesh's row, or the removed mesh's id written live again.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_mesh_removed_and_created_again_claims_its_name_over_the_dead_row() {
        let d = crate::record_store::tempdir("mesh-claim-dead");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let store = |data: &str| {
            let (data, root) = (d.join(data), root.clone());
            async move { Arc::new(CountingNodes(Arc::new(crate::storage::FileNodesStorage::open(&data, &root, "mesh2", &IncarnationId::mint()).await.unwrap()), Default::default())) }
        };
        let (a_nodes, b_nodes) = (store("a.admin").await, store("b.admin").await);
        let meshes: Arc<dyn crate::storage::MeshStorage> = Arc::new(crate::storage::FileMeshStorage::open(&root, &IncarnationId::mint()).await.unwrap());
        let (removed, recreated) = (mesh_id("mesh2"), MeshId::mint());
        let (a_admin, b_admin) = (digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), digest("mesh2.admin.1", MemberStatus::ReadyForTraffic));
        let capture = crate::span_capture::Capture::new("i143-3089-mesh-name-over-dead");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let mut aw = MeshRowsWriter::new("mesh2.admin.1", fabric1(), "mesh2", removed.clone(), a_nodes.clone(), meshes.clone());
        aw.round(std::slice::from_ref(&a_admin), true, |_| false).await;
        assert_eq!(crate::mesh_delete::mesh_row_dead(&*meshes, "mesh2", &removed).await, Ok("dead-written"), "the removal writes mesh A's row Dead");
        assert_eq!(crate::mesh_delete::mesh_row_dead(&*meshes, "mesh2", &removed).await, Ok("already-dead"), "a repeat of the removal writes nothing");
        // Mesh A's primary takes a new tenure: its id is Dead and is never written live again.
        let contacts_before = a_nodes.1.load(std::sync::atomic::Ordering::SeqCst);
        aw.round(std::slice::from_ref(&a_admin), false, |_| false).await;
        aw.round(std::slice::from_ref(&a_admin), true, |_| false).await;
        assert_eq!(crate::storage::MeshStorage::mesh(&*meshes, "mesh2").await.unwrap(), Some(crate::storage::MeshRecord { mesh_id: removed.clone(), name: "mesh2".into(), dead: true }), "the removed mesh's later tenure writes nothing live");
        assert_eq!(a_nodes.1.load(std::sync::atomic::Ordering::SeqCst), contacts_before, "nor any contact");
        let mut bw = MeshRowsWriter::new("mesh2.admin.1", fabric1(), "mesh2", recreated.clone(), b_nodes.clone(), meshes.clone());
        bw.round(std::slice::from_ref(&b_admin), true, |_| false).await;
        let row = crate::storage::MeshStorage::mesh(&*meshes, "mesh2").await.unwrap();
        assert_eq!(row, Some(crate::storage::MeshRecord { mesh_id: recreated.clone(), name: "mesh2".into(), dead: false }), "the recreated mesh's row holds mesh2 under its new MeshId");
        let contacts: Vec<NodeId> = crate::storage::NodesStorage::contacts(&*b_nodes).await.unwrap().into_iter().map(|c| c.node_id).collect();
        assert_eq!(contacts, vec![b_admin.node.node_id.clone()], "the recreated mesh's primary writes its member's contact once its claim won, and drops the removed mesh's at the path.name it now holds");
        let spans = capture.named("rdm.node_admin.mesh.create.via-mesh-row");
        let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("mesh_id").cloned().unwrap_or_default(), s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("over").cloned().unwrap_or_default())).collect();
        assert_eq!(
            said,
            vec![
                (removed.to_string(), "row-written".to_string(), String::new()),
                (removed.to_string(), "mesh-dead".to_string(), String::new()),
                (recreated.to_string(), "row-written".to_string(), removed.to_string()),
            ],
            "the dead mesh claims nothing again, and the recreated mesh claims over the Dead row: {spans:?}"
        );
        eprintln!("span rdm.node_admin.mesh.create.via-mesh-row trace_id={} {:?}", spans[2].trace_id, spans[2].attributes);
    }

    fn view(digests: &[MeshDigest]) -> Topology {
        let book = DigestBook::default();
        for d in digests {
            book.record(d.clone());
        }
        project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default())
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
        async fn spawn(&self, spec: &crate::deployment::provider::ResolvedNodeLaunch) -> Result<crate::deployment::provider::DeploymentHandle, crate::deployment::provider::DeployError> {
            Err(crate::deployment::provider::DeployError::Spawn { node: spec.node.to_string(), reason: "this provider only answers inspections".into() })
        }
        async fn terminate(&self, _: &crate::deployment::provider::DeploymentHandle, _: crate::deployment::provider::TerminationMode) -> Result<(), crate::deployment::provider::DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &crate::deployment::provider::DeploymentHandle) -> crate::deployment::provider::DeploymentStatus {
            self.inspected.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.status.clone()
        }
        async fn signal_stop(&self, _: &crate::deployment::provider::DeploymentHandle) -> Result<(), crate::deployment::provider::DeployError> {
            Ok(())
        }
        async fn find(&self, _: &crate::deployment::provider::ResolvedNodeLaunch) -> Result<crate::deployment::provider::Found, crate::deployment::provider::DeployError> {
            Ok(crate::deployment::provider::Found::NeverLaunched)
        }
    }

    /// A nodes store whose reads of runtime rows (the point read and the whole set) are refused; every
    /// other row is the memory store's.
    #[derive(Default)]
    struct RuntimeRowUnreadable(crate::storage::MemoryNodesStorage);

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for RuntimeRowUnreadable {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::record_store::StorageError> {
            self.0.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Io { file: "runtimes".into(), reason: "Input/output error (os error 5)".into() })
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            Err(crate::record_store::StorageError::Unrecognised { file: format!("runtimes/{node_id}--{}.json", incarnation.as_str()), reason: "format runtime/9 is not one this build reads".into() })
        }
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.0.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.0.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.0.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.0.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), crate::record_store::StorageError> {
            self.0.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<Result<(), crate::storage::NameHeld>, crate::record_store::StorageError> {
            self.0.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &PathName) -> Result<Option<crate::storage::NodeRow>, crate::record_store::StorageError> {
            self.0.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, crate::record_store::StorageError> {
            self.0.node_row_name(mesh, node_id).await
        }
        async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.0.hold_node_row(name, holder).await
        }
        async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.0.release_node_row(name, holder).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, crate::record_store::StorageError> {
            self.0.tagged(label).await
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
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            if self.point_refused {
                return Err(crate::record_store::StorageError::Io { file: format!("runtimes/{node_id}--{}.json", incarnation.as_str()), reason: "Input/output error (os error 5)".into() });
            }
            self.inner.runtime(node_id, incarnation).await
        }
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.inner.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.inner.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<Result<(), crate::storage::NameHeld>, crate::record_store::StorageError> {
            self.inner.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &PathName) -> Result<Option<crate::storage::NodeRow>, crate::record_store::StorageError> {
            self.inner.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, crate::record_store::StorageError> {
            self.inner.node_row_name(mesh, node_id).await
        }
        async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.inner.hold_node_row(name, holder).await
        }
        async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.inner.release_node_row(name, holder).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, crate::record_store::StorageError> {
            self.inner.tagged(label).await
        }
    }

    /// CONTRACT (CLAUDE.md read rule: "Every lookup is a point read through a declared index"; error
    /// rule: "never written" and "could not be read" are different errors, distinct arms and spans):
    /// the fabric-primary proves a lost executor `mesh2.admin.1` departed whose digest carries no
    /// runtime, so the proof reads that exact birth's durable runtime row by its key. Held and
    /// exited: the departure is proven. Never written: refused on
    /// `rdm.node_admin.node.reject.via-no-runtime-row`. Unreadable: refused on
    /// `rdm.node_admin.node.reject.via-runtime-row-unreadable` naming the store's cause. Must NOT
    /// happen: a list of every runtime row searched for the one birth, or the two refusals as one.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_executor_exit_proof_point_reads_its_births_runtime_row_and_names_unwritten_apart_from_unreadable() {
        use crate::build_drive::DepartureProof as _;
        use crate::storage::NodesStorage as _;
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-executor")).unwrap();
        let executor = digest("mesh2.admin.1", MemberStatus::ReadyForTraffic);
        let book = DigestBook::default();
        for d in [executor.clone(), digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic)] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let row = crate::storage::RuntimeRow { node_id: executor.node.node_id.clone(), name: executor.node.name.clone(), incarnation_id: executor.node.incarnation.clone(), runtime: fact.clone(), data_dir: None };
        let departure = |nodes: Arc<CountsRuntimeLists>| ProviderDeparture {
            me: "mesh1.admin.1".parse().unwrap(),
            topology: Arc::new(RwLock::new(view.clone())),
            refresh: Arc::new(|| Box::pin(async {})),
            book: book.clone(),
            provider: Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() }),
            nodes,
            records: {
                // The status store the proven exit is recorded in.
                let r = Records::default();
                r.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
                Arc::new(r)
            },
            fabric: Arc::new(crate::fabric_storage::MemoryFabricStorage::default()),
        };
        let executor_name: PathName = "mesh2.admin.1".parse().unwrap();
        let birth = (executor.node.node_id.to_string(), executor.node.incarnation.to_string());
        let capture = crate::span_capture::Capture::new("i143-3051-executor-exit-proof-point-read");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);

        let held = Arc::new(CountsRuntimeLists::default());
        held.put_runtime(&row).await.unwrap();
        departure(held.clone()).proven(&executor_name).await.expect("the held row's runtime is proven exited");
        let unwritten = Arc::new(CountsRuntimeLists::default());
        let never = departure(unwritten.clone()).proven(&executor_name).await.unwrap_err();
        let refused = Arc::new(CountsRuntimeLists { point_refused: true, ..Default::default() });
        let unread = departure(refused.clone()).proven(&executor_name).await.unwrap_err();

        let lists = |s: &CountsRuntimeLists| s.lists.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!((lists(&held), lists(&unwritten), lists(&refused)), (0, 0, 0), "the proof reads one row by its key, never the list of every row");
        assert_eq!(capture.named("rdm.node_admin.build.update.via-executor-exit-proof").len(), 1, "the held row proves the exit once");
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
        assert_eq!((attr(&no_row, "reader"), attr(&no_row, "node"), attr(&no_row, "node_id"), attr(&no_row, "incarnation_id"), attr(&no_row, "store")), ("mesh1.admin.1".to_string(), "mesh2.admin.1".to_string(), birth.0.clone(), birth.1.clone(), "nodes.storage".to_string()), "{no_row:?}");
        let unreadable = attrs("rdm.node_admin.node.reject.via-runtime-row-unreadable");
        assert_eq!((attr(&unreadable, "reader"), attr(&unreadable, "node"), attr(&unreadable, "node_id"), attr(&unreadable, "incarnation_id"), attr(&unreadable, "store")), ("mesh1.admin.1".to_string(), "mesh2.admin.1".to_string(), birth.0, birth.1, "nodes.storage".to_string()), "{unreadable:?}");
        assert!(attr(&unreadable, "error").contains("Input/output error (os error 5)"), "{unreadable:?}");
    }

    /// CONTRACT (fabric-node-lifecycle: a re-run never calls the lost executor or proves its exit
    /// again): the fabric-primary's departure proof answers `recorded` from the exit-proof row of
    /// the EXACT birth the view names for the executor. Must NOT happen: `recorded` false
    /// for a birth whose exit is recorded (the drive would call the lost executor again and a
    /// second via-executor-exit-proof would follow), or true for an earlier birth of the same node.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_departure_recorded_for_the_executors_exact_birth_is_answered_without_a_new_proof() {
        use crate::build_drive::DepartureProof as _;
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-recorded")).unwrap();
        let executor = digest("mesh2.admin.1", MemberStatus::ReadyForTraffic);
        let book = DigestBook::default();
        book.record(executor.clone());
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let records = Arc::new(Records::default());
        records.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
        let departure = ProviderDeparture {
            me: "mesh1.admin.1".parse().unwrap(),
            topology: Arc::new(RwLock::new(view)),
            refresh: Arc::new(|| Box::pin(async {})),
            book,
            provider: Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() }),
            nodes: Arc::new(CountsRuntimeLists::default()),
            records: records.clone(),
            fabric: Arc::new(crate::fabric_storage::MemoryFabricStorage::default()),
        };
        let name: PathName = "mesh2.admin.1".parse().unwrap();
        assert!(!departure.recorded(&name).await, "nothing is recorded yet");
        let mut birth = Node::allocated(name.clone());
        birth.node_id = executor.node.node_id.clone();
        birth.incarnation_id = Some(IncarnationId::parse(&rdm_core::fixture_crockford60("an-earlier-birth")).unwrap());
        records.put_exit(&birth, Some(1), "provider").await.unwrap();
        assert!(!departure.recorded(&name).await, "an earlier birth's exit is not this birth's");
        birth.incarnation_id = Some(executor.node.incarnation.clone());
        records.put_exit(&birth, Some(1), "provider").await.unwrap();
        assert!(departure.recorded(&name).await, "the exact birth's exit is recorded, so the drive takes over without a new proof");
        assert!(!departure.recorded(&"mesh9.admin.1".parse().unwrap()).await, "a node outside the view names no birth");
    }

    /// The mesh primary `mesh2.admin.1`'s recovery of its own mesh's births, over `book`, `provider`
    /// and `nodes_storage`, and the TRUE OFFLINE member `mesh2.rpc.1` of `view`.
    fn recovery_of(view: &Topology, book: DigestBook, provider: Arc<dyn DeploymentProvider>, nodes_storage: Arc<dyn crate::storage::NodesStorage>, status: Arc<dyn crate::status_storage::StatusStorage>) -> (RecoverySubmitter, Arc<dyn crate::status_storage::StatusStorage>, Node) {
        let member = view.node(&"mesh2.rpc.1".parse().unwrap()).cloned().expect("the member is in the view");
        let recovery = RecoverySubmitter {
            me: "mesh2.admin.1".parse().unwrap(),
            topology: Arc::new(RwLock::new(view.clone())),
            book,
            provider,
            nodes_storage,
            status_storage: status.clone(),
            door: Default::default(),
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
            data_root: PathBuf::new(),
            gone_dirs: Arc::default(),
            os_stopped_confirmed: Arc::default(),
            terminate_refusals_named: Arc::default(),
        };
        (recovery, status, member)
    }

    /// What the mesh primary's two ticks over one TRUE OFFLINE birth left: the verdict, the
    /// inspection spans, the submit spans, and the birth's exit-proof row.
    async fn two_ticks(recovery: &RecoverySubmitter, status: &dyn crate::status_storage::StatusStorage, member: &Node, cell: &str) -> (Inspected, Vec<crate::span_capture::Captured>, usize, Option<crate::status_storage::NodeExitRow>) {
        let capture = crate::span_capture::Capture::new(cell);
        let verdicts = Mutex::new(HashMap::new());
        let outcome = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            let first = recovery.recover_if_proven(member, &verdicts).await;
            let second = recovery.recover_if_proven(member, &verdicts).await;
            assert_eq!(first, second, "the same birth, the same verdict on the next tick");
            first
        };
        let inspections = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery").len();
        let proof = status.node_exit(&member.node_id, member.incarnation_id.as_ref().unwrap()).await.unwrap();
        (outcome, inspections, submits, proof)
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 3: "Exited is the proof, recorded
    /// durably. Still running is no proof"; operation summary "Trigger: Exact provider proof only"):
    /// the mesh primary asks the provider about a TRUE OFFLINE member's exact runtime and the
    /// provider cannot tell (`Unknown`). That is no proof: the verdict is unavailable with the
    /// provider's answer as its reason, named once over two ticks, and nothing is recorded or
    /// submitted. Must NOT happen: an exit-proof row, a recovery submit, a verdict spanned every tick.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_true_offline_member_the_provider_cannot_tell_about_is_not_proven_exited_and_never_recovered() {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-unknown")).unwrap();
        let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        member.node.runtime = Some(fact.clone());
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Unknown, inspected: Default::default() });
        let (recovery, status, node) = recovery_of(&view, book, provider.clone(), Arc::new(crate::storage::MemoryNodesStorage::default()), Arc::new(crate::status_storage::MemoryStatusStorage::default()));
        let (outcome, inspections, submits, proof) = two_ticks(&recovery, &*status, &node, "i143-3051-provider-unknown").await;
        assert_eq!(outcome, Inspected::Unavailable("its runtime is not proven exited: Unknown".into()), "Unknown is no proof");
        assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 2, "the provider was asked about the exact runtime on each tick");
        assert_eq!(inspections.len(), 1, "the verdict is named once: {inspections:?}");
        let a = &inspections[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("node"), attr("node_id"), attr("incarnation_id")), ("mesh2.rpc.1".to_string(), node.node_id.to_string(), member.node.incarnation.to_string()), "the exact birth: {a:?}");
        assert_eq!((attr("verdict"), attr("exit_proof"), attr("reason")), ("unavailable".to_string(), "none".to_string(), "its runtime is not proven exited: Unknown".to_string()), "{a:?}");
        assert_eq!(attr("inspector"), "mesh2.admin.1", "{a:?}");
        assert_eq!((submits, proof), (0, None), "no exit-proof row and no recovery submit without an exit proof");
        eprintln!("span rdm.node_admin.node.resolve.via-provider-inspection trace_id={} {a:?}", inspections[0].trace_id);
    }

    /// A TRUE OFFLINE member `mesh2.rpc.1` whose exact runtime the provider `answers`: its view, its
    /// book, the provider and a memory status store.
    fn os_stopped_member(answers: crate::deployment::provider::DeploymentStatus, cell: &str) -> (RecoverySubmitter, Arc<dyn crate::status_storage::StatusStorage>, Node, Arc<Answers>, DigestBook, Topology) {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60(cell)).unwrap();
        let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        member.node.runtime = Some(fact.clone());
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: answers, inspected: Default::default() });
        let (recovery, status, node) = recovery_of(&view, book.clone(), provider.clone(), Arc::new(crate::storage::MemoryNodesStorage::default()), Arc::new(crate::status_storage::MemoryStatusStorage::default()));
        (recovery, status, node, provider, book, view)
    }

    fn attrs(c: &crate::span_capture::Captured) -> impl Fn(&str) -> String + '_ {
        move |k: &str| c.attributes.get(k).cloned().unwrap_or_default()
    }

    /// CONTRACT (deployment-provider.md §2.5; node-recovery.md step 3; R-PV1): the provider reads a
    /// TRUE OFFLINE member's exact runtime `OsStopped`. The mesh primary names the verdict
    /// `os-stopped` once, records the birth's `os-stopped` row (`since` kept, its inspections
    /// counted on every tick) on `rdm.node_admin.node.update.via-os-stopped` (`recorded`, once), and
    /// that is all: no `held-running` row, no exit proof, no recovery submit. An `OsStopped` verdict
    /// never counts toward a hung runtime's hold. Must NOT happen: a hold, a terminate, an exit
    /// proof or a submit from an os-stopped observation.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn an_os_stopped_birth_is_recorded_and_opens_no_hold_and_never_counts_toward_held_running() {
        let (recovery, status, node, provider, _, _) = os_stopped_member(crate::deployment::provider::DeploymentStatus::OsStopped, "dep-os-stopped");
        let inc = node.incarnation_id.clone().unwrap();
        let capture = crate::span_capture::Capture::new("i144-e2-os-stopped-recorded");
        let verdicts = Mutex::new(HashMap::new());
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            for _ in 0..3 {
                assert_eq!(recovery.recover_if_proven(&node, &verdicts).await, Inspected::OsStopped);
            }
        }
        assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 3, "the exact runtime is inspected on every tick");
        let row = status.node_os_stopped_of("mesh2", &node.node_id, &inc).await.unwrap().expect("the os-stopped row stands");
        assert_eq!((row.inspections, row.name.to_string(), row.incarnation_id.clone()), (3, "mesh2.rpc.1".to_string(), inc.clone()), "{row:?}");
        assert_eq!(status.node_held_running_of("mesh2", &node.node_id, &inc).await.unwrap(), None, "no hold opens for an os-stopped runtime");
        assert_eq!(status.node_exit(&node.node_id, &inc).await.unwrap(), None, "os-stopped is never exit proof");
        let inspections = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
        assert_eq!(inspections.len(), 1, "the verdict is named once: {inspections:?}");
        assert_eq!(attrs(&inspections[0])("verdict"), "os-stopped");
        let facts = capture.named("rdm.node_admin.node.update.via-os-stopped");
        assert_eq!(facts.iter().map(|c| attrs(c)("outcome")).collect::<Vec<_>>(), vec!["recorded".to_string()], "recorded once: {facts:?}");
        assert_eq!(attrs(&facts[0])("since"), row.since.to_string());
        assert!(capture.named("rdm.node_admin.node.update.via-held-running").is_empty(), "no held-running verdict");
        assert!(capture.named("rdm.node_admin.build.update.via-node-recovery").is_empty(), "no recovery submit");
        eprintln!("span rdm.node_admin.node.update.via-os-stopped trace_id={}", facts[0].trace_id);
    }

    /// CONTRACT (deployment-provider.md §2.5: "Entering it from `Running` cancels an open R-FM1 hold:
    /// the `held-running` row is removed and its pending `TerminateRuntime` is void"): a member held
    /// (`held-running` with two verdicts) is then read `OsStopped`. The hold is cancelled on
    /// `via-held-running` with outcome `cancelled-os-stopped`, naming the voided deadline, its row is
    /// gone, the `os-stopped` row is recorded, and no recovery is submitted however long past the old
    /// deadline. Read `Running` again, the os-stopped row clears (`cleared`, `running`) and a fresh
    /// hold opens with its own `since`.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn an_os_stopped_verdict_cancels_an_open_hold_and_running_again_clears_it_and_opens_a_fresh_one() {
        let (recovery, status, node, _, book, view) = os_stopped_member(crate::deployment::provider::DeploymentStatus::OsStopped, "dep-hold-cancel");
        let inc = node.incarnation_id.clone().unwrap();
        let old = crate::status_storage::NodeHeldRunningRow { node_id: node.node_id.clone(), name: node.name.clone(), incarnation_id: inc.clone(), since: 1, verdicts: 2 };
        status.put_node_held_running_of("mesh2", &old).await.unwrap();
        let capture = crate::span_capture::Capture::new("i144-e2-hold-cancel");
        let verdicts = Mutex::new(HashMap::new());
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            assert_eq!(recovery.recover_if_proven(&node, &verdicts).await, Inspected::OsStopped);
            assert_eq!(recovery.recover_if_proven(&node, &verdicts).await, Inspected::OsStopped);
        }
        assert_eq!(status.node_held_running_of("mesh2", &node.node_id, &inc).await.unwrap(), None, "the hold's row is removed");
        assert!(status.node_os_stopped_of("mesh2", &node.node_id, &inc).await.unwrap().is_some(), "the os-stopped fact is recorded");
        let cancelled: Vec<_> = capture.named("rdm.node_admin.node.update.via-held-running").into_iter().filter(|c| attrs(c)("outcome") == "cancelled-os-stopped").collect();
        assert_eq!(cancelled.len(), 1, "the hold is cancelled once: {cancelled:?}");
        assert_eq!((attrs(&cancelled[0])("since"), attrs(&cancelled[0])("verdicts"), attrs(&cancelled[0])("voided_deadline")), ("1".to_string(), "2".to_string(), (1 + hung_hold_ms()).to_string()), "the voided deadline is named");
        assert!(capture.named("rdm.node_admin.build.update.via-node-recovery").is_empty(), "no terminate is submitted past the old deadline");
        // The runtime runs again and stays unreachable: the fact clears and a fresh hold opens.
        let running = Arc::new(Answers { domain: recovery.provider.control_domain(), status: crate::deployment::provider::DeploymentStatus::Running, inspected: Default::default() });
        let (resumed, _, _) = recovery_of(&view, book, running, recovery.nodes_storage.clone(), status.clone());
        let capture = crate::span_capture::Capture::new("i144-e2-hold-fresh");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            assert_eq!(resumed.recover_if_proven(&node, &verdicts).await, Inspected::Running);
        }
        assert_eq!(status.node_os_stopped_of("mesh2", &node.node_id, &inc).await.unwrap(), None, "Running again clears the os-stopped fact");
        let cleared = capture.named("rdm.node_admin.node.update.via-os-stopped");
        assert_eq!(cleared.iter().map(|c| (attrs(c)("outcome"), attrs(c)("detail"))).collect::<Vec<_>>(), vec![("cleared".to_string(), "running".to_string())]);
        let fresh = status.node_held_running_of("mesh2", &node.node_id, &inc).await.unwrap().expect("a fresh hold opens");
        assert!(fresh.since > 1 && fresh.verdicts == 1, "a fresh hold, its own since: {fresh:?}");
        eprintln!("span via-held-running cancelled-os-stopped trace_id={}", cancelled[0].trace_id);
    }

    /// CONTRACT (node-delete.md "OS-suspended target": the `os-stopped` row is removed once, `deleted`, or
    /// `exited` when the mesh-primary's tick reads the exit first): an os-stopped member whose delete has
    /// begun (its `NodeDeleting` overlay taken) is read `Running` by the tick, as a terminate's kill wakes
    /// a suspended runtime before it dies. The fact stands for the delete, and no hold opens. Must NOT
    /// happen: the fact cleared `running` under the delete that owns the birth (the release gate's
    /// `provider_state` explicit-delete cell saw it), or a fresh hold on a birth being deleted.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_running_read_under_an_announced_delete_leaves_the_os_stopped_fact_to_the_delete() {
        let (recovery, status, node, provider, book, view) = os_stopped_member(crate::deployment::provider::DeploymentStatus::OsStopped, "dep-os-stopped-deleting");
        let inc = node.incarnation_id.clone().unwrap();
        let verdicts = Mutex::new(HashMap::new());
        assert_eq!(recovery.recover_if_proven(&node, &verdicts).await, Inspected::OsStopped);
        assert!(status.node_os_stopped_of("mesh2", &node.node_id, &inc).await.unwrap().is_some(), "the os-stopped fact is recorded");
        let op = rdm_core::LifecycleOp {
            build_id: "dbs8z9896j0f".into(),
            attempt: 1,
            operation: "node.delete:mesh2.rpc.1".into(),
            node_id: node.node_id.clone(),
            incarnation: inc.clone(),
            name: node.name.clone(),
            event_at_rafka_ms: 1,
        };
        assert!(book.deleting(op), "the delete's overlay is taken");
        // The terminate's kill wakes the suspended runtime: the tick reads it Running.
        let running = Arc::new(Answers { domain: provider.domain.clone(), status: crate::deployment::provider::DeploymentStatus::Running, inspected: Default::default() });
        let (ticking, _, _) = recovery_of(&view, book, running, recovery.nodes_storage.clone(), status.clone());
        let capture = crate::span_capture::Capture::new("i144-e2-os-stopped-deleting");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            assert_eq!(ticking.recover_if_proven(&node, &verdicts).await, Inspected::Running);
        }
        assert!(status.node_os_stopped_of("mesh2", &node.node_id, &inc).await.unwrap().is_some(), "the fact stands for the delete that owns the birth");
        assert!(capture.named("rdm.node_admin.node.update.via-os-stopped").is_empty(), "no clear under the delete");
        assert_eq!(status.node_held_running_of("mesh2", &node.node_id, &inc).await.unwrap(), None, "no hold on a birth being deleted");
    }

    /// CONTRACT (deployment-provider.md §2.5: "A new authority revalidates the stored fact with its own
    /// inspection"; i144 R10): an `os-stopped` row another admin wrote stands when a successor
    /// mesh-primary first inspects the birth. Its own `OsStopped` verdict confirms it once
    /// (`revalidated`, the row's `since` kept, its inspections counted on); its own `Exited` verdict
    /// instead clears it (`cleared`, `exited`) and the exit proof stands. Must NOT happen: the
    /// predecessor's row taken as the successor's own without an inspection, or a new `since`.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_successor_mesh_primary_revalidates_a_held_os_stopped_fact_with_its_own_inspection() {
        let (recovery, status, node, _, book, view) = os_stopped_member(crate::deployment::provider::DeploymentStatus::OsStopped, "dep-revalidate");
        let inc = node.incarnation_id.clone().unwrap();
        let held = crate::status_storage::NodeOsStoppedRow { node_id: node.node_id.clone(), name: node.name.clone(), incarnation_id: inc.clone(), since: 7, inspections: 4 };
        status.put_node_os_stopped_of("mesh2", &held).await.unwrap();
        let capture = crate::span_capture::Capture::new("i144-e2-revalidate");
        let verdicts = Mutex::new(HashMap::new());
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            assert_eq!(recovery.recover_if_proven(&node, &verdicts).await, Inspected::OsStopped);
            assert_eq!(recovery.recover_if_proven(&node, &verdicts).await, Inspected::OsStopped);
        }
        let row = status.node_os_stopped_of("mesh2", &node.node_id, &inc).await.unwrap().unwrap();
        assert_eq!((row.since, row.inspections), (7, 6), "the predecessor's since is kept and the successor's inspections count on");
        let facts = capture.named("rdm.node_admin.node.update.via-os-stopped");
        assert_eq!(facts.iter().map(|c| attrs(c)("outcome")).collect::<Vec<_>>(), vec!["revalidated".to_string()], "confirmed once, by its own inspection: {facts:?}");
        // Another successor whose inspection reads the runtime exited clears the fact.
        let exited = Arc::new(Answers { domain: recovery.provider.control_domain(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(9) }, inspected: Default::default() });
        let (next, _, _) = recovery_of(&view, book, exited, recovery.nodes_storage.clone(), status.clone());
        let capture = crate::span_capture::Capture::new("i144-e2-revalidate-exited");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            assert_eq!(next.recover_if_proven(&node, &Mutex::new(HashMap::new())).await, Inspected::Exited(Some(9), "provider"));
        }
        assert_eq!(status.node_os_stopped_of("mesh2", &node.node_id, &inc).await.unwrap(), None, "an exit clears the os-stopped fact");
        assert_eq!(capture.named("rdm.node_admin.node.update.via-os-stopped").iter().map(|c| (attrs(c)("outcome"), attrs(c)("detail"))).collect::<Vec<_>>(), vec![("cleared".to_string(), "exited".to_string())]);
        assert!(status.node_exit(&node.node_id, &inc).await.unwrap().is_some(), "the exit proof stands");
        eprintln!("span via-os-stopped revalidated trace_id={}", facts[0].trace_id);
    }

    /// CONTRACT (deployment-provider.md §2.5 `Unknown`: "recorded as uncertainty; no hold, no
    /// terminate, no exit proof is inferred"): a provider that cannot tell leaves an `os-stopped` row
    /// that stands as it is (no new inspection counted, never cleared), opens no hold and proves no
    /// exit.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn an_unknown_inspection_records_uncertainty_and_infers_nothing() {
        let (recovery, status, node, _, _, _) = os_stopped_member(crate::deployment::provider::DeploymentStatus::Unknown, "dep-unknown-os");
        let inc = node.incarnation_id.clone().unwrap();
        let held = crate::status_storage::NodeOsStoppedRow { node_id: node.node_id.clone(), name: node.name.clone(), incarnation_id: inc.clone(), since: 3, inspections: 2 };
        status.put_node_os_stopped_of("mesh2", &held).await.unwrap();
        let capture = crate::span_capture::Capture::new("i144-e2-unknown");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            assert!(matches!(recovery.recover_if_proven(&node, &Mutex::new(HashMap::new())).await, Inspected::Unavailable(_)));
        }
        assert_eq!(status.node_os_stopped_of("mesh2", &node.node_id, &inc).await.unwrap(), Some(held), "the fact stands untouched");
        assert_eq!(status.node_held_running_of("mesh2", &node.node_id, &inc).await.unwrap(), None);
        assert_eq!(status.node_exit(&node.node_id, &inc).await.unwrap(), None);
        assert_eq!(capture.named("rdm.node_admin.node.resolve.via-provider-inspection").iter().map(|c| attrs(c)("verdict")).collect::<Vec<_>>(), vec!["unavailable".to_string()]);
        assert!(capture.named("rdm.node_admin.node.update.via-os-stopped").is_empty());
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 3: "Exited is the proof, recorded
    /// durably"; CLAUDE.md: an error state names what failed and where): the provider proves a TRUE
    /// OFFLINE member exited and status.storage refuses the birth's exit-proof row. The submit ends
    /// there, on its own `via-node-recovery` span, whose outcome names the row and the store's own
    /// cause; nothing reaches the fabric-primary (no Build named) and no row stands. Must NOT happen:
    /// a refused exit-proof write with no span (an info line only), or a submit without its row.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_refused_exit_proof_write_ends_the_recovery_submit_on_its_span_naming_the_stores_cause() {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-exit-refused")).unwrap();
        let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        member.node.runtime = Some(fact.clone());
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() });
        let refusing = crate::offline::tests::SilenceRefusing { exit: true, ..Default::default() };
        let (recovery, status, node) = recovery_of(&view, book, provider, Arc::new(crate::storage::MemoryNodesStorage::default()), Arc::new(refusing));
        let capture = crate::span_capture::Capture::new("i143-3051-exit-proof-refused");
        let outcome = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            recovery.recover_if_proven(&node, &Mutex::new(HashMap::new())).await
        };
        assert_eq!(outcome, Inspected::Exited(Some(1), "provider"), "the provider proved the exit");
        assert_eq!(status.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap(), None, "no exit-proof row stands");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
        assert_eq!(submits.len(), 1, "the submit that ended on the refused row is spanned: {submits:?}");
        let a = &submits[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("node"), attr("node_id"), attr("incarnation_id")), ("mesh2.rpc.1".to_string(), node.node_id.to_string(), member.node.incarnation.to_string()), "the exact birth: {a:?}");
        assert!(attr("outcome").contains("exit-proof row") && attr("outcome").contains("No space left on device (os error 28)"), "the outcome names the row and the store's cause: {a:?}");
        assert_eq!(attr("build_id"), "", "nothing reached the fabric-primary: {a:?}");
        eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {a:?}", submits[0].trace_id);
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": "the mesh-primary is the submitter that
    /// resubmits by `build_id` ... the mesh-primary's next tick finds the member still proven exited
    /// and submits"; CLAUDE.md read rule: node-local state that should be the durable answer is
    /// replaced by the read): a submitter that holds nothing in memory (an admin that just took the
    /// seat, or restarted) is owed the exited birth mesh2.rpc.1 by the durable facts alone: the
    /// current Build holds that birth's run, whose attempt ended Failed after its
    /// delete, and the birth is in no view. Nothing is owed while this admin does not hold the seat or
    /// a fabric shutdown froze recovery, nor to another mesh. With no exit-proof row the submit is
    /// refused on `rdm.node_admin.node.reject.via-exit-proof-unread` (reason `missing`) and nothing is
    /// submitted; once status.storage holds the row, the submit span carries the row's exit code and
    /// proof and the Failed attempt it follows. Must NOT happen: an owed exit found only in a previous
    /// submitter's memory, or a submit with no exit proof.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_submitter_that_holds_nothing_in_memory_reads_the_failed_exit_from_the_durable_facts() {
        let book = DigestBook::default();
        book.record(digest("mesh2.admin.1", MemberStatus::ReadyForTraffic));
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let (path, node_id, incarnation): (PathName, _, _) = ("mesh2.rpc.1".parse().unwrap(), NodeId::mint(), IncarnationId::mint());
        assert!(view.node(&path).is_none(), "the exited birth departed: no view holds it");
        let b1 = crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        let deleted = |step: &str| crate::build_state::BuildStepReceipt {
            mesh: None,
            build_id: b1.clone(),
            attempt: 2,
            operation: format!("node.delete:{path}"),
            step: step.into(),
            outcome: crate::build_state::StepOutcome::Complete,
            output: (step == "NodeDeleted").then(|| {
                serde_json::to_value(rdm_core::LifecycleOp { build_id: "b1".into(), attempt: 2, operation: format!("node.delete:{path}"), node_id: node_id.clone(), incarnation: incarnation.clone(), name: path.clone(), event_at_rafka_ms: 1 }).unwrap()
            }),
            executor: Some("mesh2.admin.2".into()),
        };
        builds.absorb(&[
            crate::build_state::BuildFact::Accepted(crate::build_state::BuildAccepted { build_id: b1.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 1 }),
            crate::build_state::BuildFact::Opened(crate::build_state::AttemptOpened {
                mesh: None,
                build_id: b1.clone(),
                attempt: 2,
                reason: crate::build_state::AttemptReason::Recovery,
                action: Some(crate::accepted::AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone() }),
                opened_by: "mesh1.admin.1".into(),
                opened_at_ms: 1,
            }),
            crate::build_state::BuildFact::Claim(crate::build_state::BuildAttemptClaim { claimant: None, mesh: None, build_id: b1.clone(), attempt: 2, executor: "mesh2.admin.2".into() }),
            crate::build_state::BuildFact::Step(deleted("NodeDeleted")),
            crate::build_state::BuildFact::Step(deleted("Complete")),
            crate::build_state::BuildFact::Attempt(crate::build_state::BuildAttemptReceipt { mesh: None, build_id: b1.clone(), attempt: 2, outcome: crate::build_state::AttemptOutcome::Failed { reason: "node.create: PrepareStorage".into() , code: rdm_core::outcome::Code::FailedPrecondition } }),
        ]).await;
        let fabric = Arc::new(crate::fabric_storage::MemoryFabricStorage::new());
        crate::fabric_storage::FabricStorage::claim_identity(&*fabric, &crate::fabric_storage::FabricIdentity { fabric_id: fabric1(), name: "fabric1".into() }).await.unwrap().expect("the fabric's name is claimed");
        let status = Arc::new(crate::status_storage::MemoryStatusStorage::default());
        let recovery = RecoverySubmitter {
            me: "mesh2.admin.1".parse().unwrap(),
            topology: Arc::new(RwLock::new(view)),
            book,
            provider: Arc::new(Answers { domain: String::new(), status: crate::deployment::provider::DeploymentStatus::Unknown, inspected: Default::default() }),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            status_storage: status.clone(),
            door: Default::default(),
            builds,
            data_root: PathBuf::new(),
            gone_dirs: Arc::default(),
            os_stopped_confirmed: Arc::default(),
            terminate_refusals_named: Arc::default(),
        };
        assert_eq!(recovery.owed_submits(false, "mesh2").await, vec![], "nothing is owed by an admin that does not hold the seat");
        assert_eq!(recovery.owed_submits(true, "mesh1").await, vec![], "another mesh's primary is owed none of mesh2's exits");
        let owed = recovery.owed_submits(true, "mesh2").await;
        assert_eq!(owed, vec![crate::build_state::OwedExit { path: path.clone(), node_id: node_id.clone(), incarnation: incarnation.clone(), attempt: 2, reason: "node.create: PrepareStorage".into() }], "read from the durable facts alone");

        let capture = crate::span_capture::Capture::new("i143-3088-owed-exit-from-durable-facts");
        let (unproven, submitted) = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            let unproven = recovery.submit_owed(&owed[0]).await;
            crate::offline::put_exit_proof(&*status, &{
                let mut n = Node::allocated(path.clone());
                n.node_id = node_id.clone();
                n.incarnation_id = Some(incarnation.clone());
                n
            }, Some(137), "provider", 1)
            .await
            .unwrap();
            (unproven, recovery.submit_owed(&owed[0]).await)
        };
        let refusals = capture.named("rdm.node_admin.node.reject.via-exit-proof-unread");
        assert_eq!(refusals.len(), 1, "the submit with no exit-proof row is refused on its span: {refusals:?}");
        let a = &refusals[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("node"), attr("node_id"), attr("incarnation_id"), attr("submitter"), attr("failed_attempt"), attr("reason")), (path.to_string(), node_id.to_string(), incarnation.to_string(), "mesh2.admin.1".to_string(), "2".to_string(), "missing".to_string()), "{a:?}");
        assert!(unproven.as_ref().is_err_and(|e| e.contains("no exit-proof row") && e.contains("missing")), "{unproven:?}");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
        assert_eq!(submits.len(), 1, "only the submit with its exit proof reached the submit span: {submits:?}");
        let s = &submits[0].attributes;
        let sattr = |k: &str| s.get(k).cloned().unwrap_or_default();
        assert_eq!((sattr("node"), sattr("node_id"), sattr("incarnation_id"), sattr("submitter")), (path.to_string(), node_id.to_string(), incarnation.to_string(), "mesh2.admin.1".to_string()), "keyed on the exact exited birth: {s:?}");
        assert_eq!((sattr("exit_code"), sattr("exit_proof"), sattr("after_failed_attempt")), ("137".to_string(), "provider".to_string(), "2".to_string()), "the row's proof and the Failed attempt it follows: {s:?}");
        assert!(submitted.is_err() && !sattr("outcome").is_empty(), "this cell has no fabric-primary door: the submit ends named: {s:?}");
        eprintln!("span rdm.node_admin.node.reject.via-exit-proof-unread trace_id={} {a:?}", refusals[0].trace_id);
        eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {s:?}", submits[0].trace_id);
    }

    /// CONTRACT (node-recovery.md: "Node recovery is RDM's drift reconciliation for a member that
    /// exits outside any workflow"; "Failure and receipts": "An exit during a workflow belongs to
    /// that workflow"): a birth whose removal the mesh primary's book holds announced (a delete's
    /// `NodeDeleting` overlay naming the exact birth, or its `NodeDeleted` departure) is not a
    /// member to recover, however exited its exact runtime is proven: its exit recorded as its
    /// exit-proof row, no recovery submit, one `rdm.node_admin.build.reject.via-exit-inside-workflow`
    /// span naming the workflow. A recovery's own node.delete of the exited birth (`node.delete:<path>`) is such a
    /// workflow while its overlay stands. Must NOT happen: a recovery submitted for a birth whose
    /// own workflow's delete announced its removal (trace 3b983b72926d8d5e3629d7e3d1f264cf: the
    /// tick's ladder round 2 read the view before the workflow's NodeDeleting, inspected the exit
    /// that workflow's own TerminateRuntime made, and submitted).
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_proven_exit_of_a_birth_whose_removal_was_announced_submits_no_recovery() {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-removal-announced")).unwrap();
        for (case, announced, workflow) in [("delete-deleting", "node-deleting", "node.delete"), ("delete-deleted", "node-deleted", "node.delete")] {
            let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
            member.node.runtime = Some(fact.clone());
            let book = DigestBook::default();
            for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
                book.record(d);
            }
            // The tick's view, read before the announcement arrived.
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
            let op = rdm_core::LifecycleOp {
                build_id: "dbs8z9896j0f".into(),
                attempt: 2,
                operation: format!("{workflow}:mesh2.rpc.1"),
                node_id: member.node.node_id.clone(),
                incarnation: member.node.incarnation.clone(),
                name: member.node.name.clone(),
                event_at_rafka_ms: 1,
            };
            assert!(book.deleting(op.clone()), "{case}: the overlay is taken");
            if announced == "node-deleted" {
                assert!(book.depart(op.clone()), "{case}: the departure is taken");
            }
            // The birth's durable runtime row (every admin's blind put on first holding it): the
            // inspection proves the exit from it once the departure dropped the digest.
            let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
            crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
            let (recovery, status, node) = recovery_of(&view, book, provider, nodes, Arc::new(crate::status_storage::MemoryStatusStorage::default()));
            let capture = crate::span_capture::Capture::new(&format!("i143-3068-removal-announced-{case}"));
            let outcome = {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                recovery.recover_if_proven(&node, &Mutex::new(HashMap::new())).await
            };
            assert_eq!(outcome, Inspected::Exited(Some(137), "provider"), "{case}: the provider proved the exact runtime exited");
            let proof = status.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap();
            let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
            let rejects = capture.named("rdm.node_admin.build.reject.via-exit-inside-workflow");
            assert_eq!((submits.len(), proof.is_some()), (0, true), "{case}: no recovery submit for a birth whose removal was announced, and its proven exit recorded as its exit-proof row (the proven loss every projection reads): {submits:?}");
            assert_eq!(rejects.len(), 1, "{case}: the refusal is spanned once: {rejects:?}");
            let a = &rejects[0].attributes;
            let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
            assert_eq!((attr("node"), attr("node_id"), attr("incarnation_id")), ("mesh2.rpc.1".to_string(), node.node_id.to_string(), member.node.incarnation.to_string()), "{case}: the exact birth: {a:?}");
            assert_eq!((attr("announced"), attr("operation"), attr("build_id"), attr("attempt")), (announced.to_string(), format!("{workflow}:mesh2.rpc.1"), "dbs8z9896j0f".to_string(), "2".to_string()), "{case}: the workflow the exit belongs to: {a:?}");
            assert_eq!(attr("submitter"), "mesh2.admin.1", "{case}: {a:?}");
            eprintln!("span rdm.node_admin.build.reject.via-exit-inside-workflow trace_id={} {a:?}", rejects[0].trace_id);
        }
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that
    /// workflow"; mesh-delete.md: from mesh.deleting, the mesh's status deleting, to mesh.deleted, every
    /// member's removal is the mesh.delete's): an exited member of a mesh whose status the mesh primary
    /// holds deleting or deleted is not a member to recover, a primary lost mid-delete included: its
    /// exit is recorded as its exit-proof row, no recovery is submitted, and one
    /// `rdm.node_admin.build.reject.via-exit-inside-workflow` span naming the mesh.delete. Must NOT
    /// happen: mesh_lifecycle__mesh_delete_resume on f8001275, where mesh2's surviving admin, seated
    /// after its primary was killed mid-delete, submitted the recovery of that primary 220 times
    /// (trace 8b3d152a6a51c90fc06a4031cd055d37, refused while the Build reconciled).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_proven_exit_of_a_member_of_a_deleting_or_deleted_mesh_submits_no_recovery() {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-mesh-deleting")).unwrap();
        for (status, announced) in [("leaving", "mesh-deleting"), ("dead", "mesh-deleted")] {
            let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
            member.node.runtime = Some(fact.clone());
            let book = DigestBook::default();
            for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
                book.record(d);
            }
            book.statuses.take(&rdm_transport::membership::Frame::MeshStatus { mesh: "mesh2".into(), status: status.into(), publisher: "mesh2.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 }, &fabric1());
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
            let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
            crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
            let (recovery, status_rows, node) = recovery_of(&view, book, provider, nodes, Arc::new(crate::status_storage::MemoryStatusStorage::default()));
            let capture = crate::span_capture::Capture::new(&format!("i143-3078-mesh-{status}"));
            let outcome = {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                recovery.recover_if_proven(&node, &Mutex::new(HashMap::new())).await
            };
            assert_eq!(outcome, Inspected::Exited(Some(137), "provider"), "{status}: the provider proved the exact runtime exited");
            let proof = status_rows.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap();
            let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
            let rejects = capture.named("rdm.node_admin.build.reject.via-exit-inside-workflow");
            assert_eq!((submits.len(), proof.is_some()), (0, true), "{status}: no recovery of a member of a mesh being deleted, its exit recorded: {submits:?}");
            assert_eq!(rejects.len(), 1, "{status}: the refusal is spanned once: {rejects:?}");
            let a = &rejects[0].attributes;
            assert_eq!(a.get("announced").map(String::as_str), Some(announced), "{status}: {a:?}");
            assert!(a.get("operation").is_some_and(|o| o.contains("mesh2") || o.starts_with("mesh.delete:")), "{status}: the mesh.delete is named: {a:?}");
            eprintln!("span rdm.node_admin.build.reject.via-exit-inside-workflow trace_id={} {a:?}", rejects[0].trace_id);
        }
    }

    /// A status store counting every exit-proof row it is offered (`put_node_exit`); every row is
    /// the memory store's.
    #[derive(Default)]
    struct CountsExitPuts {
        inner: crate::status_storage::MemoryStatusStorage,
        exit_puts: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::status_storage::StatusStorage for CountsExitPuts {
        async fn put_mesh_status(&self, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status(row).await
        }
        async fn mesh_statuses(&self) -> Result<HashMap<crate::model::MeshId, rdm_core::status::MeshState>, crate::record_store::StorageError> {
            self.inner.mesh_statuses().await
        }
        async fn put_fabric_event(&self, row: &crate::status_storage::FabricEventRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_fabric_event(row).await
        }
        async fn fabric_events(&self) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events().await
        }
        async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events_of(mesh).await
        }
        async fn put_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::record_store::StorageError> { self.inner.put_node_operation(row).await }
        async fn node_operations(&self) -> Result<Vec<crate::status_storage::NodeOperationRow>, crate::record_store::StorageError> { self.inner.node_operations().await }
        async fn remove_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::record_store::StorageError> { self.inner.remove_node_operation(row).await }

        async fn put_node_declared(&self, row: &crate::status_storage::NodeDeclaredRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_declared(row).await
        }
        async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_node_declared(node_id, incarnation_id).await
        }
        async fn node_declared(&self) -> Result<Vec<crate::status_storage::NodeDeclaredRow>, crate::record_store::StorageError> {
            self.inner.node_declared().await
        }
        async fn put_node_silence(&self, row: &crate::status_storage::NodeSilenceRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_silence(row).await
        }
        async fn node_silences(&self) -> Result<Vec<crate::status_storage::NodeSilenceRow>, crate::record_store::StorageError> {
            self.inner.node_silences().await
        }
        async fn put_node_exit(&self, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.exit_puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_node_exit(row).await
        }
        async fn node_exit(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.inner.node_exit(node_id, incarnation_id).await
        }
        async fn mesh_status_held_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<bool, crate::record_store::StorageError> {
            self.inner.mesh_status_held_of(mesh, row).await
        }
        async fn put_mesh_status_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status_of(mesh, row).await
        }
        async fn node_exit_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.inner.node_exit_of(mesh, node_id, incarnation_id).await
        }
        async fn put_node_exit_of(&self, mesh: &str, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.exit_puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_node_exit_of(mesh, row).await
        }
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 3: "Exited is the proof, recorded
    /// durably"; the exit-proof row is a blind put of the birth's own key, offline.rs
    /// `put_exit_proof`): the mesh primary's tick finds the same exited birth TRUE OFFLINE on three
    /// ticks, and the birth's exit-proof row is put once, on the tick that proved the exit. Both arms
    /// of the tick hold it: a birth whose removal was announced (its `retire-node` NodeDeleting
    /// overlay; `via-exit-inside-workflow` names the row `put` on the first tick and `recorded`
    /// after), and a member outside any workflow whose recovery submit ends without the recovery
    /// (here its Build door is not built), submitted again on each tick (node-recovery.md "Failure
    /// and receipts"). Must NOT happen: the row put again on every tick for an exit already recorded
    /// (admin.rs `recover_if_proven`, s60 part 15's finding: the refusal arm puts it per tick).
    // @feature: node-lifecycle
    #[tokio::test]
    async fn the_mesh_primary_records_a_proven_exit_once_over_every_tick_that_finds_it() {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-exit-once")).unwrap();
        for case in ["announced", "submitted"] {
            let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
            member.node.runtime = Some(fact.clone());
            let book = DigestBook::default();
            for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
                book.record(d);
            }
            let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
            if case == "announced" {
                let op = rdm_core::LifecycleOp {
                    build_id: "dbs8z9896j0f".into(),
                    attempt: 2,
                    operation: "node.delete:mesh2.rpc.1".into(),
                    node_id: member.node.node_id.clone(),
                    incarnation: member.node.incarnation.clone(),
                    name: member.node.name.clone(),
                    event_at_rafka_ms: 1,
                };
                assert!(book.deleting(op), "{case}: the overlay is taken");
            }
            let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
            let counting = Arc::new(CountsExitPuts::default());
            let (recovery, status, node) = recovery_of(&view, book, provider.clone(), Arc::new(crate::storage::MemoryNodesStorage::default()), counting.clone());
            let capture = crate::span_capture::Capture::new(&format!("i143-s78-exit-recorded-once-{case}"));
            let verdicts = Mutex::new(HashMap::new());
            {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                for tick in 1..=3 {
                    assert_eq!(recovery.recover_if_proven(&node, &verdicts).await, Inspected::Exited(Some(137), "provider"), "{case}: tick {tick} proves the exit");
                }
            }
            assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 3, "{case}: the provider is asked on every tick");
            assert!(status.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap().is_some_and(|r| r.exit_code == Some(137) && r.proof == "provider"), "{case}: the birth's exit-proof row stands");
            assert_eq!(counting.exit_puts.load(std::sync::atomic::Ordering::SeqCst), 1, "{case}: the exit-proof row is put once, on the tick that proved the exit, never per tick");
            let inspections = capture.named("rdm.node_admin.node.resolve.via-provider-inspection");
            assert_eq!(inspections.len(), 1, "{case}: the verdict is named once: {inspections:?}");
            if case == "announced" {
                let rejects = capture.named("rdm.node_admin.build.reject.via-exit-inside-workflow");
                let rows: Vec<String> = rejects.iter().map(|r| r.attributes.get("exit_proof_row").cloned().unwrap_or_default()).collect();
                assert_eq!(rows, ["put", "recorded", "recorded"], "{case}: each tick's refusal names the row: put when that tick put it, recorded after: {rejects:?}");
                eprintln!("span rdm.node_admin.build.reject.via-exit-inside-workflow trace_id={} {:?}", rejects[0].trace_id, rejects[0].attributes);
            } else {
                let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
                assert_eq!(submits.len(), 3, "{case}: the recovery is submitted on each tick it is still owed: {submits:?}");
                eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {:?}", submits[0].trace_id, submits[0].attributes);
            }
        }
    }

    /// CONTRACT (gossip.md `NodeDeleting`: keyed `(kind, build_id, attempt, operation)`, "the fact
    /// rides `Members.in_flight` until the operation completes", and `Members.in_flight` is "while
    /// the operation is claimed and has no completed receipt (derived from Build facts each round)";
    /// node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that workflow:
    /// its Build ends Failed, and the mesh-primary's next tick finds the member still proven exited
    /// and submits the recovery"): a removal's `NodeDeleting` overlay, opened from its attempt's
    /// receipt, closes when that attempt's verdict is `Failed` (one
    /// `rdm.mesh.membership.update.via-deleting-closed-by-facts` span naming the attempt), and the
    /// mesh primary's next tick then submits the recovery of the member whose exact runtime the
    /// failed workflow left exited: the exit-proof row is put and one `via-node-recovery` submit is
    /// made. Must NOT happen: the overlay held after its attempt failed, so the tick refuses the
    /// member `via-exit-inside-workflow` for a workflow that is over and the member is never put back.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_failed_removal_attempts_target_is_recovered_by_the_next_tick() {
        use crate::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-failed-removal")).unwrap();
        let mut member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        member.node.runtime = Some(fact.clone());
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let op = rdm_core::LifecycleOp {
            build_id: "9r7cpewghdkk".into(),
            attempt: 2,
            operation: "node.delete:mesh2.rpc.1".into(),
            node_id: member.node.node_id.clone(),
            incarnation: member.node.incarnation.clone(),
            name: member.node.name.clone(),
            event_at_rafka_ms: 1,
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
            overlays_from_facts(&facts, &book, &mut named);
        }
        assert_eq!(book.in_flight(), vec![op.clone()], "the removal's NodeDeleting receipt opens its overlay");
        // The attempt's verdict: the workflow failed (the member's runtime exited under it).
        facts.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: build.clone(), attempt: 2, outcome: AttemptOutcome::Failed { reason: "TerminateRuntime: the member exited under the replace".into() , code: rdm_core::outcome::Code::FailedPrecondition } }));
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            overlays_from_facts(&facts, &book, &mut named);
        }
        assert_eq!(book.in_flight(), Vec::<rdm_core::LifecycleOp>::new(), "the failed attempt's overlay leaves Members.in_flight: its operation is no longer claimed by that attempt");
        let closed = capture.named("rdm.mesh.membership.update.via-deleting-closed-by-facts");
        assert_eq!(closed.len(), 1, "the close is spanned once: {closed:?}");
        let a = &closed[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("operation"), attr("build_id"), attr("attempt"), attr("by")), ("node.delete:mesh2.rpc.1".to_string(), "9r7cpewghdkk".to_string(), "2".to_string(), "attempt-failed".to_string()), "the attempt the facts ended: {a:?}");
        eprintln!("span rdm.mesh.membership.update.via-deleting-closed-by-facts trace_id={} {a:?}", closed[0].trace_id);
        // A late copy of the closed attempt's overlay (a frame, an aggregate) is applied once, not again.
        assert!(!book.deleting(op.clone()), "the closed overlay is not opened again by a copy of it");
        // The mesh primary's next tick: the member's exact runtime is proven exited.
        let nodes = Arc::new(crate::storage::MemoryNodesStorage::default());
        crate::storage::NodesStorage::put_runtime(&*nodes, &crate::storage::RuntimeRow { node_id: member.node.node_id.clone(), name: member.node.name.clone(), incarnation_id: member.node.incarnation.clone(), runtime: fact.clone(), data_dir: None }).await.unwrap();
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(137) }, inspected: Default::default() });
        let (recovery, status, node) = recovery_of(&view, book, provider, nodes, Arc::new(crate::status_storage::MemoryStatusStorage::default()));
        let outcome = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            recovery.recover_if_proven(&node, &Mutex::new(HashMap::new())).await
        };
        assert_eq!(outcome, Inspected::Exited(Some(137), "provider"), "the provider proved the exact runtime exited");
        let rejects = capture.named("rdm.node_admin.build.reject.via-exit-inside-workflow");
        assert!(rejects.is_empty(), "no workflow holds the member: its failed attempt is over: {rejects:?}");
        assert!(status.node_exit(&node.node_id, node.incarnation_id.as_ref().unwrap()).await.unwrap().is_some(), "the exit-proof row is put");
        let submits = capture.named("rdm.node_admin.build.update.via-node-recovery");
        assert_eq!(submits.len(), 1, "the recovery is submitted: {submits:?}");
        eprintln!("span rdm.node_admin.build.update.via-node-recovery trace_id={} {:?}", submits[0].trace_id, submits[0].attributes);
    }

    /// CONTRACT (node-recovery.md "Failure and receipts": "An exit during a workflow belongs to that
    /// workflow: its Build ends Failed, and the mesh-primary's next tick finds the member still proven
    /// exited and submits the recovery"; R-BI7 caveat 2: a mesh's drift attempts are numbered in its
    /// own lane, so an attempt number names one attempt per lane): a recovery's `NodeDeleting` hold,
    /// opened from attempt 1 of mesh2's lane, closes when that attempt's verdict in mesh2's lane is
    /// `Failed` (one `rdm.mesh.membership.update.via-deleting-closed-by-facts` span, by
    /// `attempt-failed`). In another Build, the fabric lane's attempt 1 failing closes nothing of
    /// mesh2's attempt 1, which still runs: its hold stays open. Must NOT happen: a drift attempt's
    /// hold held after its attempt failed (the tick then refuses the member
    /// `via-exit-inside-workflow` for good), or one lane's verdict closing another lane's hold.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_drift_lanes_removal_hold_closes_on_its_own_attempts_verdict_and_on_no_other_lanes() {
        use crate::build_state::{AttemptOutcome, AttemptReason, AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
        let member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let mesh = || Some("mesh2".to_string());
        let recovery = |build: &str| rdm_core::LifecycleOp {
            build_id: build.into(),
            attempt: 1,
            operation: "node.delete:mesh2.rpc.1".into(),
            node_id: member.node.node_id.clone(),
            incarnation: member.node.incarnation.clone(),
            name: member.node.name.clone(),
            event_at_rafka_ms: 1,
        };
        // mesh2's lane runs attempt 1, the member's recovery; its node.delete published NodeDeleting.
        let lane_attempt = |build: &crate::build::BuildId, op: &rdm_core::LifecycleOp| {
            vec![
                BuildFact::Accepted(BuildAccepted { build_id: build.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh2", crate::model::MeshId::parse("meshd0000002").unwrap()), submitted_change: None, submitted_at_ms: 1 }),
                BuildFact::Opened(AttemptOpened { mesh: mesh(), build_id: build.clone(), attempt: 1, reason: AttemptReason::Recovery, action: None, opened_by: "mesh2.admin.1".into(), opened_at_ms: 1 }),
                BuildFact::Claim(BuildAttemptClaim { mesh: mesh(), build_id: build.clone(), attempt: 1, executor: "mesh2.admin.1".into(), claimant: Some("mesh2.admin.1".into()) }),
                BuildFact::Step(BuildStepReceipt { mesh: mesh(), build_id: build.clone(), attempt: 1, operation: op.operation.clone(), step: "NodeDeleting".into(), outcome: StepOutcome::Complete, output: Some(serde_json::to_value(op).unwrap()), executor: None }),
            ]
        };
        let capture = crate::span_capture::Capture::new("i143-s78-drift-lane-hold");
        let mut named = NamedSteps::new();
        let derive = |facts: &[BuildFact], named: &mut NamedSteps| {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            overlays_from_facts(facts, &book, named);
        };

        let op = recovery("9r7cpewghdkk");
        let build = crate::build::BuildId::parse(&op.build_id).unwrap();
        let mut facts = lane_attempt(&build, &op);
        derive(&facts, &mut named);
        assert_eq!(book.in_flight(), vec![op.clone()], "mesh2's attempt 1 holds the member");
        // The attempt's verdict in mesh2's lane: Failed (its claimant departed with it unfinished).
        facts.push(BuildFact::Attempt(BuildAttemptReceipt { mesh: mesh(), build_id: build.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: "mesh2.admin.1 departed with attempt 1 of mesh2's lane unfinished".into() , code: rdm_core::outcome::Code::FailedPrecondition } }));
        derive(&facts, &mut named);
        assert_eq!(book.in_flight(), Vec::<rdm_core::LifecycleOp>::new(), "the failed drift attempt's hold closes: the member's exit belongs to no workflow now");
        let closed = capture.named("rdm.mesh.membership.update.via-deleting-closed-by-facts");
        assert_eq!(closed.len(), 1, "the close is spanned once: {closed:?}");
        let a = &closed[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("operation"), attr("attempt"), attr("by")), ("node.delete:mesh2.rpc.1".to_string(), "1".to_string(), "attempt-failed".to_string()), "{a:?}");
        eprintln!("span rdm.mesh.membership.update.via-deleting-closed-by-facts trace_id={} {a:?}", closed[0].trace_id);

        // Another Build: the fabric lane's attempt 1 failed; mesh2's attempt 1 still runs.
        let op = recovery("7k2qm4xw9bdc");
        let build = crate::build::BuildId::parse(&op.build_id).unwrap();
        let mut facts = lane_attempt(&build, &op);
        facts.insert(1, BuildFact::Attempt(BuildAttemptReceipt { mesh: None, build_id: build.clone(), attempt: 1, outcome: AttemptOutcome::Failed { reason: "a topology operation failed".into() , code: rdm_core::outcome::Code::FailedPrecondition } }));
        derive(&facts, &mut named);
        assert_eq!(book.in_flight(), vec![op], "the fabric lane's verdict closes nothing of mesh2's running attempt");
        assert_eq!(capture.named("rdm.mesh.membership.update.via-deleting-closed-by-facts").len(), 1, "no second close");
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
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic)] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let provider = Arc::new(Answers { domain: String::new(), status: crate::deployment::provider::DeploymentStatus::Unknown, inspected: Default::default() });
        let (recovery, _, _) = recovery_of(&view, book, provider, Arc::new(RuntimeRowUnreadable::default()), Arc::new(crate::status_storage::MemoryStatusStorage::default()));
        let capture = crate::span_capture::Capture::new("i143-3051-runtime-rows-unreadable");
        let rows = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            recovery.durable_rows().await
        };
        assert!(rows.is_empty(), "no row is read from a refused store: the view's births only");
        let spans = capture.named("rdm.node_admin.node.reject.via-runtime-rows-unreadable");
        assert_eq!(spans.len(), 1, "the refused read is spanned: {spans:?}");
        let a = &spans[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("reader"), attr("store"), attr("error")), ("mesh2.admin.1".to_string(), "nodes.storage".to_string(), "runtimes: Input/output error (os error 5)".to_string()), "{a:?}");
        eprintln!("span rdm.node_admin.node.reject.via-runtime-rows-unreadable trace_id={} {a:?}", spans[0].trace_id);
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 3: the provider is asked "about the
    /// exact runtime recorded at launch"): a TRUE OFFLINE member whose digest carries no runtime is
    /// inspected through its durable runtime row, and that point read is refused. With no runtime
    /// fact nothing can be proven, whatever the provider would answer: the verdict is not-exited,
    /// naming the row and the store's own cause, the provider is not asked, and nothing is recorded
    /// or submitted. Must NOT happen: the refusal read as "no row", an inspection of a guessed
    /// runtime, an exit-proof row, a recovery submit.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_true_offline_member_whose_runtime_row_cannot_be_read_is_not_proven_exited_and_never_recovered() {
        let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-unreadable")).unwrap();
        let member = digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic);
        let book = DigestBook::default();
        for d in [digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), member.clone()] {
            book.record(d);
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        // The provider would prove an exit: it must never be asked without the birth's runtime fact.
        let provider = Arc::new(Answers { domain: fact.control_domain.clone(), status: crate::deployment::provider::DeploymentStatus::Exited { code: Some(1) }, inspected: Default::default() });
        let (recovery, status, node) = recovery_of(&view, book, provider.clone(), Arc::new(RuntimeRowUnreadable::default()), Arc::new(crate::status_storage::MemoryStatusStorage::default()));
        let (outcome, inspections, submits, proof) = two_ticks(&recovery, &*status, &node, "i143-3051-runtime-row-unreadable").await;
        let why = format!("the durable runtime row of {} at {} could not be read: runtimes/{}--{}.json is not a record this build recognises: format runtime/9 is not one this build reads", node.node_id, member.node.incarnation.as_str(), node.node_id, member.node.incarnation.as_str());
        assert_eq!(outcome, Inspected::Unavailable(why.clone()), "an unreadable row is no proof, and is named as unreadable");
        assert_eq!(provider.inspected.load(std::sync::atomic::Ordering::SeqCst), 0, "with no runtime fact the provider is not asked");
        assert_eq!(inspections.len(), 1, "the verdict is named once: {inspections:?}");
        let a = &inspections[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("verdict"), attr("exit_proof"), attr("reason")), ("unavailable".to_string(), "none".to_string(), why), "{a:?}");
        assert_eq!((submits, proof), (0, None), "no exit-proof row and no recovery submit without an exit proof");
        eprintln!("span rdm.node_admin.node.resolve.via-provider-inspection trace_id={} {a:?}", inspections[0].trace_id);
    }

    /// CONTRACT (states.md): a mesh is ready-for-traffic when every node of the mesh is. A mesh with a
    /// primary and a member not yet ready-for-traffic projects `pending`, and the mesh whose nodes
    /// are all ready projects `ready-for-traffic`. Must NOT happen: a mesh ready because it has a
    /// primary.
    #[test]
    fn a_mesh_with_a_node_not_ready_for_traffic_projects_pending() {
        use MemberStatus::*;
        let t = view(&[
            with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000"),
            with_id(digest("mesh1.rpc.1", Pending), "300000000000"),
            with_id(digest("mesh2.admin.1", ReadyForTraffic), "400000000000"),
            with_id(digest("mesh2.rpc.1", ReadyForTraffic), "500000000000"),
        ]);
        assert!(t.cohort_primary("mesh1", NodeKind::NodeAdmin).is_some(), "mesh1 has a primary");
        let status = |m: &str| t.mesh_view(m).unwrap().status;
        assert_eq!(status("mesh1"), ScopeStatus::Pending, "a node of mesh1 is not ready: the mesh is not");
        assert_eq!(status("mesh2"), ScopeStatus::ReadyForTraffic, "every node of mesh2 is ready");
    }

    /// CONTRACT (states.md, "Resize and roll"): the nodes a mesh is counted over are the accepted
    /// Build's planned births. A planned birth the view has not heard keeps its mesh `pending`, and a
    /// mesh planned and not yet created is not listed. Must NOT happen: a mesh ready because
    /// the births that happen to be heard are ready.
    #[test]
    fn a_mesh_with_a_planned_birth_not_yet_heard_projects_pending() {
        use MemberStatus::*;
        let book = DigestBook::default();
        for d in [with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000"), with_id(digest("mesh1.rpc.1", ReadyForTraffic), "300000000000")] {
            book.record(d);
        }
        let desired = |mesh: &str, rpc: u32| crate::build::MeshDesired::of(mesh, [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, rpc)]);
        let records = Records::default();
        let mut plan = crate::accepted::FabricTopology { fabric: "fabric1".into(), meshes: BTreeMap::new() };
        plan.meshes.insert("mesh1".into(), crate::accepted::MeshTopology::of(&desired("mesh1", 1)));
        records.set_planned(&plan);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::ReadyForTraffic, "every planned birth of mesh1 is heard and ready");
        plan.meshes.insert("mesh1".into(), crate::accepted::MeshTopology::of(&desired("mesh1", 2)));
        plan.meshes.insert("mesh2".into(), crate::accepted::MeshTopology::of(&desired("mesh2", 1)));
        records.set_planned(&plan);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::Pending, "mesh1.rpc.2 is planned and not heard");
        assert!(t.mesh_view("mesh2").is_none(), "mesh2 is planned and does not exist yet: the view lists what is observed, so the Build still plans its creation");
        let why = t.mesh_unready("mesh1", records.planned().get("mesh1")).iter().map(|m| m.to_string()).collect::<Vec<_>>();
        assert_eq!(why, ["mesh1.rpc.2(no-birth-heard)"], "the node behind a pending mesh is named");
    }

    /// CONTRACT (R-S3, states.md): a mesh's lifecycle is pending, ready-for-traffic, leaving, dead. A mesh
    /// heard ready-for-traffic that loses a node projects ready while it has a primary, so the primary that
    /// takes over adopts the state and runs its round. A mesh never heard ready projects pending while a
    /// node is not ready. Must NOT happen: a ready mesh projected pending because one of its nodes is gone,
    /// which gives the takeover a new status and no round.
    #[test]
    fn a_mesh_heard_ready_stays_ready_when_a_node_is_lost() {
        use MemberStatus::*;
        let book = DigestBook::default();
        for d in [with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000"), with_id(digest("mesh1.rpc.1", ReadyForTraffic), "300000000000"), with_id(digest("mesh1.rpc.2", Pending), "400000000000")] {
            book.record(d);
        }
        let records = Records::default();
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::Pending, "never heard ready, and mesh1.rpc.2 is not ready");
        records.set_heard_ready(BTreeSet::from(["mesh1".to_string()]));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.mesh_view("mesh1").unwrap().status, ScopeStatus::ReadyForTraffic, "heard ready: a node not ready does not take the mesh back to pending");
    }

    /// CONTRACT: an admin that does not hold the fabric-primary role (it lost the seat to a recovered
    /// mesh's admin while the fabric was degraded) answers `GET /api/fabric` with the status the holder
    /// published, `degraded`, until the holder publishes ready. Must NOT happen: `ready-for-traffic`
    /// because its own authority records (cleared with the seat) are empty.
    #[test]
    fn a_non_holder_projects_the_degraded_status_the_holder_published() {
        use MemberStatus::*;
        let book = DigestBook::default();
        for d in [with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000"), with_id(digest("mesh2.admin.1", ReadyForTraffic), "300000000000")] {
            book.record(d);
        }
        let records = Records::default();
        records.set_fabric_state(crate::fabric_state::FabricState::ReadyForTraffic);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.fabric.status, ScopeStatus::ReadyForTraffic, "nothing published degraded: ready");
        records.set_published_degraded(true);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.fabric.status, ScopeStatus::Degraded, "the holder published degraded and has not published ready");
    }

    /// CONTRACT: a birth the fabric authority proved exited is `Dead` in the view whatever its last
    /// digest said, so a mesh whose admins were learned from topology alone and are all proven gone
    /// has no primary admin. Must NOT happen: a dead admin holds its mesh's seat because its digest
    /// says ready.
    #[tokio::test]
    async fn a_proven_exited_birth_is_dead_whatever_its_digest_says() {
        use MemberStatus::*;
        let book = DigestBook::default();
        let a = with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000");
        let b = with_id(digest("mesh2.admin.1", ReadyForTraffic), "300000000000");
        book.record(a.clone());
        book.record(b.clone());
        let records = Records::default();
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert!(t.cohort_primary("mesh2", NodeKind::NodeAdmin).is_some());
        let t = prove_exit(&book, &records, &b).await;
        assert_eq!(t.node(&"mesh2.admin.1".parse().unwrap()).unwrap().status, NodeStatus::Dead);
        assert!(t.cohort_primary("mesh2", NodeKind::NodeAdmin).is_none(), "a proven-gone admin holds no seat");
    }

    /// CONTRACT (node-recovery.md "The mesh-primary is the reconciler for its own members";
    /// mesh-investigation.md: a peer mesh unheard on the backbone is the fabric-primary's
    /// investigation, counted from t since it last heard that mesh): a mesh-primary's offline tickle
    /// watches the silent members of its own mesh and nothing of a peer mesh, its node-admins
    /// included. A peer mesh's node-admins are reached by the investigation's probes alone. Must NOT
    /// happen: a mesh-primary tickling, holding TRUE OFFLINE or writing a silence status for a peer
    /// mesh's node-admin on a per-member clock beside the investigation.
    #[test]
    fn a_mesh_primary_watches_only_its_own_meshs_silent_members() {
        use MemberStatus::*;
        let book = DigestBook::default();
        let me = with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000");
        let own = with_id(digest("mesh1.rpc.1", ReadyForTraffic), "300000000000");
        let peer_admin = with_id(digest("mesh2.admin.1", ReadyForTraffic), "400000000000");
        let peer_rpc = with_id(digest("mesh2.rpc.1", ReadyForTraffic), "500000000000");
        for d in [&me, &own, &peer_admin, &peer_rpc] {
            book.record((*d).clone());
        }
        let mut t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        for n in t.nodes.iter_mut().filter(|n| n.node_id != me.node.node_id) {
            n.status = NodeStatus::PendingReconnect;
        }
        let watched: Vec<String> = watched_births(&t, &me.node.node_id, "mesh1", &[]).values().map(|n| n.name.to_string()).collect();
        assert_eq!(watched, vec!["mesh1.rpc.1".to_string()], "only its own mesh's silent member is watched");
    }

    /// CONTRACT (node-tickle.md step 1; R-PV1: a `NotSent` command "is a reason to ask now"; i144 R9): a
    /// member heard a moment ago (well within the floor) that a command never reached for a
    /// reachability reason is watched `PendingReconnect` by its mesh-primary's tick, counted silent,
    /// and its silence window starts at the mark, not at the floor. A digest of the birth heard after
    /// the mark ends it; so does a mark of another mesh's birth or of a birth the view no longer holds.
    /// Must NOT happen: a marked member left out of the silent set because its last digest is recent.
    // @feature: node-lifecycle
    #[test]
    fn an_unreachable_mark_watches_the_member_at_once_and_ends_when_it_is_heard_after_it() {
        use MemberStatus::*;
        let book = DigestBook::default();
        let me = with_id(digest("mesh1.admin.1", ReadyForTraffic), "200000000000");
        let own = with_id(digest("mesh1.rpc.1", ReadyForTraffic), "300000000000");
        let peer = with_id(digest("mesh2.rpc.1", ReadyForTraffic), "500000000000");
        for d in [&me, &own, &peer] {
            book.record((*d).clone());
        }
        let view = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let floor = std::time::Duration::from_secs(30);
        let marks: crate::offline::UnreachableMarks = Arc::default();
        let at = std::time::Instant::now();
        for d in [&own, &peer] {
            marks.lock().unwrap().insert(d.node.node_id.clone(), crate::offline::UnreachableMark { incarnation: d.node.incarnation.clone(), at, at_ms: 1_000, reason: "connect-timed-out" });
        }
        let now = at + std::time::Duration::from_millis(1);
        let held = crate::offline::held_marks(&marks, &view, "mesh1", &book, now);
        assert_eq!(held.keys().cloned().collect::<Vec<_>>(), vec![own.node.node_id.clone()], "another mesh's birth is not this mesh-primary's to mark");
        let mut watched = watched_births(&view, &me.node.node_id, "mesh1", &[]);
        assert!(watched.is_empty(), "nothing is silent past the floor");
        crate::offline::watch_marked(&mut watched, &held, &view, &me.node.node_id);
        let n = watched.get(own.node.node_id.as_str()).expect("the marked member is watched");
        assert_eq!(n.status, NodeStatus::PendingReconnect);
        let silent: Vec<_> = crate::offline::silent_set(watched.values(), &book, now, 5_000, floor).into_iter().map(|s| crate::offline::marked_from(s, &held)).collect();
        assert_eq!(silent.iter().map(|s| (s.node_id.clone(), s.marked_at_ms)).collect::<Vec<_>>(), vec![(own.node.node_id.to_string(), Some(1_000))], "silent from the mark, its window from the mark's instant");
        // A digest heard after the mark (the birth's next, a higher digest_seq) ends it.
        let mut next = own.clone();
        next.digest_seq += 1;
        book.record(next);
        let later = std::time::Instant::now() + std::time::Duration::from_millis(5);
        assert!(crate::offline::held_marks(&marks, &view, "mesh1", &book, later).is_empty(), "heard after the mark: the mark ends");
        assert!(marks.lock().unwrap().is_empty(), "an ended mark is dropped");
    }

    /// CONTRACT (node-recovery.md, Detection: "The mesh-primary's tick (every gossip interval) reads
    /// the members its view marks `PendingReconnect` (unheard past the staleness floor)"; "Detection
    /// and proof" step 1: "Every node-admin keeps the silence bookkeeping (`first_seen`), so a
    /// node-admin that takes the seat mid-window continues the window already running"): an admin of
    /// mesh2 whose book holds mesh2.rpc.1 only from its mesh-primary's projection (the member's own
    /// digest never reached it) lists the birth in its view, never routable: with the projected
    /// status within one staleness floor of first holding it, and `PendingReconnect` past it. Its
    /// tick watches the birth, and the silence mark is the instant it first held it plus the floor,
    /// so the window it keeps began before any seat change. Must NOT happen: the view holding no birth
    /// of the member, so an heir's tick watches nothing and the member is never recovered.
    #[test]
    fn an_admin_lists_and_watches_an_own_mesh_birth_held_only_from_its_primarys_projection() {
        use MemberStatus::*;
        let book = DigestBook::default();
        let me = with_id(digest("mesh2.admin.2", ReadyForTraffic), "200000000000");
        assert!(book.record(me.clone()));
        let member = with_id(digest("mesh2.rpc.1", ReadyForTraffic), "300000000000");
        let t0 = std::time::Instant::now();
        book.hold_projected_at(&member, t0);
        let floor = book.staleness_floor();
        let records = Records::default();
        let path: PathName = "mesh2.rpc.1".parse().unwrap();
        let early = project_at("fabric1", &fabric1(), ProviderKind::Process, &book, &records, t0 + floor / 2);
        let n = early.node(&path).unwrap_or_else(|| panic!("the view holds the birth its primary's projection lists: {:?}", early.nodes));
        assert_eq!((n.node_id.clone(), n.status, n.routable), (member.node.node_id.clone(), NodeStatus::ReadyForTraffic, false), "within the floor: the projected status, never routable (no liveness heard)");
        let late_at = t0 + floor + Duration::from_millis(500);
        let late = project_at("fabric1", &fabric1(), ProviderKind::Process, &book, &records, late_at);
        assert_eq!(late.node(&path).map(|n| n.status), Some(NodeStatus::PendingReconnect), "unheard past the floor since it was first held");
        let watched = watched_births(&late, &me.node.node_id, "mesh2", &[]);
        assert_eq!(watched.keys().cloned().collect::<Vec<_>>(), vec![member.node.node_id.to_string()], "the tick watches it");
        let now_ms = 1_000_000u64;
        let silent = crate::offline::silent_set(watched.values(), &book, late_at, now_ms, floor);
        assert_eq!(silent.iter().map(|s| s.marked_at_ms).collect::<Vec<_>>(), vec![Some(now_ms - 500)], "the mark: first held plus the floor");
    }

    /// CONTRACT (node-rpc-envelope.md, "Node RPC readiness gate": "A node-admin waits until its Status
    /// authority is filled and its view holds it"): the admin's Node RPC door opens with the first view
    /// installed that holds its exact birth once the authority is filled, whichever task installs that
    /// view, so nothing that reads the installed view (the declarer that tells its primary it is ready
    /// among them) sees its own birth while its door still refuses. A view without the birth, one
    /// holding another birth of its name, and any view while the authority is not filled leave the
    /// door closed. Must NOT happen: the door opened only by the view-rebuild task, which starts after
    /// the declarer and a Build-log catch-up, so the admin declares itself ready to its primary while
    /// its router answers every round command NotReady (gate red, tree b24a4303,
    /// fabric_states__rounds trace 95adb727: mesh1.admin.2 declared ReadyForTraffic at 788604, its door
    /// opened at 788637, and mesh1.admin.1's three commit-state sends had all come back NotReady).
    #[tokio::test]
    async fn an_admins_door_opens_with_the_first_installed_view_that_holds_its_exact_birth() {
        let me = digest("mesh1.admin.2", MemberStatus::ReadyForTraffic);
        let primary = digest("mesh1.admin.1", MemberStatus::ReadyForTraffic);
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let filled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let records = Records::default();
        let authority = filled.clone();
        let topology = RwLock::new(view(&[primary.clone()]));
        records.set_door(ServingDoor::new(ready.clone(), (me.node.node_id.clone(), me.node.incarnation.clone()), Box::new(move || authority.load(std::sync::atomic::Ordering::SeqCst)), me.node.name.clone()), &*topology.read().await);
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

    /// CONTRACT (R-EL1, "a seat moves only by explicit yield": the election ignores a holder's
    /// status; seat_watch, R-A2: only a runtime found exited is a verified loss; node-identity-and-state.md
    /// §7.3: every admin keeps the silence window, only the seat holder tickles): a mesh primary that
    /// goes silent keeps its seat in its successor's view until its exact runtime is proven exited
    /// (its exit-proof row, what seat_watch puts, read by the next install). Until then the successor keeps the window but
    /// tickles nothing, holds nothing TRUE OFFLINE and has no recovery due, however long the silence
    /// lasts. Once the exit is proven the seat moves to the successor, which tickles on the window
    /// already running (round 1 at once) and, one staleness floor later, holds the old primary TRUE
    /// OFFLINE and due for its recovery. Must NOT happen: a recovery due at the successor while the
    /// silent primary still holds the seat.
    #[tokio::test(start_paused = true)]
    async fn a_successor_has_no_recovery_due_for_a_silent_mesh_primary_until_the_seat_moves() {
        use crate::offline::{OfflineTickle, SilentNode, ViaPeerVerdict};
        use rdm_core::Seat::MeshPrimary;
        use MemberStatus::*;
        let book = DigestBook::default();
        let holder = with_id(digest("mesh2.admin.1", ReadyForTraffic), "200000000000");
        let successor = with_id(digest("mesh2.admin.2", ReadyForTraffic), "300000000000");
        let rpc = with_id(digest("mesh2.rpc.1", ReadyForTraffic), "400000000000");
        for d in [&holder, &successor, &rpc] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&holder, 1));
        let records = Records::default();
        let me: PathName = "mesh2.admin.2".parse().unwrap();
        // The successor's view: the holder silent past the floor (its pruner's PendingReconnect, or
        // Dead once proven exited), the seats by the one election over the holder record.
        let view_now = || {
            let mut t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
            for n in t.nodes.iter_mut().filter(|n| n.node_id == holder.node.node_id && n.status.is_live()) {
                n.status = NodeStatus::PendingReconnect;
            }
            crate::election::resolve_with(&mut t.nodes, &incumbency_of(&book, &records));
            t
        };
        let floor = book.staleness_floor().as_millis() as u64;
        let mut tickle = OfflineTickle::observed_by(me.to_string(), crate::stop_gate::StopGate::new());
        let mut offline: HashSet<(NodeId, IncarnationId)> = HashSet::new();
        let pass = async |now: u64, tickle: &mut OfflineTickle, offline: &mut HashSet<(NodeId, IncarnationId)>| {
            // The rounds the last pass started run beside the loop until this one (each probe here
            // answers at once).
            tokio::task::yield_now().await;
            let view = view_now();
            let holds_seat = view.holds_mesh_seat("mesh2", &successor.node.node_id);
            let watched = watched_births(&view, &successor.node.node_id, "mesh2", &[]);
            let silent = watched.values().map(|n| SilentNode { node_id: n.node_id.to_string(), path: n.name.to_string(), incarnation: n.incarnation_id.clone(), marked_at_ms: None }).collect();
            let in_view: HashSet<String> = view.members().map(|n| n.node_id.to_string()).collect();
            let report = tickle
                .tick(
                    now,
                    holds_seat,
                    silent,
                    |id: &str| in_view.contains(id),
                    floor / 2,
                    floor,
                    |id| async move { Err::<(), String>(format!("{id} did not answer the ping")) },
                    |_| async { ViaPeerVerdict::NoPath { asked: Vec::new() } },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await;
            for id in &report.offline {
                if let Some(n) = watched.get(id) {
                    offline.insert((n.node_id.clone(), n.incarnation_id.clone().unwrap()));
                }
            }
            let due: Vec<String> = recovery_due(holds_seat, "mesh2", &watched, offline).iter().map(|n| n.name.to_string()).collect();
            (holds_seat, report, due)
        };
        let t0 = 1_000_000u64;
        // The holder silent for six floors, its exit not proven: the seat stays, nothing is due.
        for k in 0..6 {
            let (holds_seat, report, due) = pass(t0 + k * floor, &mut tickle, &mut offline).await;
            assert!(!holds_seat, "floor {k}: the silent holder keeps the seat");
            assert!(report.tickled.is_empty() && report.offline.is_empty(), "floor {k}: no tickle, nothing TRUE OFFLINE: {report:?}");
            assert!(due.is_empty(), "floor {k}: no recovery due before the seat moves: {due:?}");
        }
        // seat_watch proves the exact runtime exited: the seat moves to the successor.
        prove_exit(&book, &records, &holder).await;
        let t1 = t0 + 6 * floor;
        let (holds_seat, report, due) = pass(t1, &mut tickle, &mut offline).await;
        assert!(holds_seat, "the proven exit moves the seat to the successor");
        assert_eq!(report.tickled, vec![holder.node.node_id.to_string()], "round 1 at once, on the window kept since the silence began");
        assert!(report.offline.is_empty() && due.is_empty(), "round 1 opens the hold-down only: {report:?} {due:?}");
        let (_, report, due) = pass(t1 + floor, &mut tickle, &mut offline).await;
        assert_eq!(report.tickled, vec![holder.node.node_id.to_string()], "round 2, one floor later");
        assert!(report.offline.is_empty() && due.is_empty(), "round 2 runs beside the loop: nothing is decided before a tick folds it: {report:?} {due:?}");
        let (_, report, due) = pass(t1 + floor + 500, &mut tickle, &mut offline).await;
        assert_eq!(report.offline, vec![holder.node.node_id.to_string()], "the next tick folds round 2: TRUE OFFLINE");
        assert_eq!(due, vec!["mesh2.admin.1".to_string()], "the old primary is due for recovery at the successor, after the seat moved");
    }

    fn holder_of(d: &MeshDigest, revision: u64) -> rdm_core::SeatHolder {
        rdm_core::SeatHolder { mesh: d.node.name.mesh.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), revision }
    }

    /// CONTRACT (R-A2, the dw33-1 shape): the seat records an admin holds decide its projection. A
    /// ready admin with a lower NodeId that appears, in any mesh, changes no seat; a view that has
    /// not been told who holds the seats (an entering admin) elects nobody; the holder proven
    /// exited, or replaced by a later birth that says so, hands the seat to its own mesh.
    #[tokio::test]
    async fn the_projection_keeps_a_recorded_holder_and_elects_nobody_before_it_is_told() {
        use MemberStatus::*;
        use rdm_core::Seat::{FabricPrimary, MeshPrimary};
        let book = DigestBook::default();
        let holder = with_id(digest("mesh1.admin.1", ReadyForTraffic), "500000000000");
        let sibling = with_id(digest("mesh1.admin.2", ReadyForTraffic), "800000000000");
        for d in [&holder, &sibling] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&holder, 1));
        book.seats.take(FabricPrimary, &holder_of(&holder, 1));
        let records = Records::default();
        let seat_names = |t: &Topology| (t.fabric_primary().map(|n| n.name.to_string()), t.cohort_primary("mesh1", NodeKind::NodeAdmin).map(|n| n.name.to_string()));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(seat_names(&t), (Some("mesh1.admin.1".into()), Some("mesh1.admin.1".into())));

        // A lower NodeId appears in another mesh and one in the incumbent mesh, both ready: nothing moves.
        book.record(with_id(digest("mesh3.admin.1", ReadyForTraffic), "100000000000"));
        book.record(with_id(digest("mesh1.admin.3", ReadyForTraffic), "200000000000"));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(seat_names(&t), (Some("mesh1.admin.1".into()), Some("mesh1.admin.1".into())), "a lower NodeId never displaces a living holder");
        assert_eq!(t.cohort_primary("mesh3", NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh3.admin.1"));

        // An entering admin has not been told who holds the seats: it computes none.
        records.set_entering(true);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert!(t.nodes.iter().all(|n| !n.is_primary && !n.is_fabric_primary), "no seat before the entry read");
        records.set_entering(false);

        // Proven exited: the lowest Ready NodeId of ITS mesh fills both seats, not mesh3's lower id.
        let t = prove_exit(&book, &records, &holder).await;
        assert_eq!(seat_names(&t), (Some("mesh1.admin.3".into()), Some("mesh1.admin.3".into())));
    }

    /// CONTRACT (R-A2): a later birth that names the recorded holder's incarnation as the one it
    /// replaced proves that birth gone: the seat goes to the lowest Ready admin of its mesh.
    #[test]
    fn a_later_birth_that_supersedes_the_holder_proves_it_gone() {
        use MemberStatus::*;
        use rdm_core::Seat::{FabricPrimary, MeshPrimary};
        let book = DigestBook::default();
        let holder = with_id(digest("mesh1.admin.1", ReadyForTraffic), "500000000000");
        book.record(holder.clone());
        book.seats.take(MeshPrimary, &holder_of(&holder, 1));
        book.seats.take(FabricPrimary, &holder_of(&holder, 1));
        let mut recovered = with_id(digest("mesh1.admin.1", Pending), "900000000000");
        recovered.node.supersedes = Some(holder.node.incarnation.clone());
        book.record(recovered.clone());
        let ready = with_id(digest("mesh1.admin.2", ReadyForTraffic), "800000000000");
        book.record(ready);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.2"));
    }

    /// The fabric-primary `mesh1.admin.1` (both seats of mesh1) and `mesh2`'s births `mesh2`, the
    /// first seated as mesh2's primary at epoch 1: the fabric-primary's book, its records holding
    /// its node-rows writer over `storage`, and its view projected from both.
    async fn fabric_primary_over(mesh2: &[MeshDigest], storage: Arc<crate::storage::MemoryNodesStorage>) -> (MeshDigest, DigestBook, Arc<Records>, Arc<RwLock<Topology>>, Arc<crate::node_rows::NodeRows>) {
        use rdm_core::Seat::{FabricPrimary, MeshPrimary};
        let fp = with_id(digest("mesh1.admin.1", MemberStatus::ReadyForTraffic), "100000000000");
        let book = DigestBook::default();
        book.record(fp.clone());
        for d in mesh2 {
            book.record(d.clone());
        }
        book.seats.take(FabricPrimary, &holder_of(&fp, 1));
        book.seats.take(MeshPrimary, &holder_of(&fp, 1));
        book.seats.take(MeshPrimary, &holder_of(&mesh2[0], 1));
        let records = Arc::new(Records::default());
        records.set_book(book.clone());
        let view = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)));
        let rows = Arc::new(crate::node_rows::NodeRows::new(fp.node.name.clone(), fp.node.node_id.clone(), view.clone(), storage));
        records.set_rows(rows.clone());
        (fp, book, records, view, rows)
    }

    /// The provider's exit proof of `d`'s birth recorded as the product records it
    /// (`Records::put_exit`), in the status store `records` holds (one in memory, set on first use
    /// under a mesh no view names), then read by an install of the view, as every projection reads
    /// it; the installed view.
    async fn prove_exit(book: &DigestBook, records: &Records, d: &MeshDigest) -> Topology {
        if records.status.get().is_none() {
            records.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "nomesh".into(), "nomesh.admin.1".parse().unwrap(), NodeId::mint());
        }
        records.set_book(book.clone());
        let mut n = Node::allocated(d.node.name.clone());
        n.node_id = d.node.node_id.clone();
        n.incarnation_id = Some(d.node.incarnation.clone());
        records.put_exit(&n, Some(0), "provider").await.unwrap();
        let view = RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, book, records));
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, book, records)).await
    }

    /// The exact Exited receipt of `d`'s birth, as a leave records it.
    fn exited_receipt(d: &MeshDigest) -> crate::deployment::pipeline::TerminalReceipt {
        crate::deployment::pipeline::TerminalReceipt { node: d.node.name.to_string(), node_id: d.node.node_id.to_string(), incarnation_id: d.node.incarnation.to_string(), runtime: None, exit_code: Some(0) }
    }

    /// CONTRACT (mesh-delete.md step 7: the final primary's own node.delete, the fabric-primary its
    /// executor, "as acting primary of a mesh that has no primary, writes its row deleted"; R-EL1: a
    /// seat moves only by explicit yield or proven loss): the final primary's exact Exited is that
    /// proven loss. Recorded as its exit-proof row and read by the install before the view the row is
    /// written from is projected, the exited holder is no longer seated, mesh2 has no primary in the
    /// fabric-primary's view, and the fabric-primary writes the final primary's row Deleted (the write
    /// node.delete's NodeDeleted step makes). Must NOT happen: the refusal of
    /// mesh_identity::canonical_ids on 8803f84e ("mesh1.admin.1 (40ynvqbyvdhh) is not the writer of
    /// mesh2's node rows ...: mesh2's primary node-admin mesh2.admin.1 (kz0m1x9y3wcn) writes them"):
    /// the exit proven, never read as a loss, so the election kept the exited holder.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn the_final_primarys_exit_held_as_a_proven_loss_leaves_the_fabric_primary_writing_its_row_deleted() {
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let last = with_id(digest("mesh2.admin.1", MemberStatus::Leaving), "200000000000");
        let (_fp, book, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage.clone()).await;
        crate::storage::NodesStorage::put_node_row(&*storage, &crate::storage::NodeRow { name: last.node.name.clone(), node_id: last.node.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None }).await.unwrap().unwrap();
        let capture = crate::span_capture::Capture::new("i143-3068-mesh-left-final-exit-held");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let final_primary = crate::mesh_delete::RosterBirth { node: last.node.name.to_string(), node_id: last.node.node_id.to_string(), incarnation_id: last.node.incarnation.to_string() };
        let write = || async {
            let nodes: Vec<Node> = view.read().await.nodes.iter().filter(|n| n.mesh == "mesh2").cloned().collect();
            crate::mesh_delete::birth_row_deleted(records.node_rows().as_deref(), "mesh1.admin.1", "node.delete:mesh2.admin.1", &nodes, &final_primary).await
        };
        // The exit proven and not held: the election keeps the exited holder seated (R-EL1).
        let refused = write().await;
        assert!(refused.as_ref().is_err_and(|e| e.contains("is not the writer of mesh2's node rows")), "unheld, the exited holder still writes mesh2's rows: {refused:?}");
        records.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
        records.put_exit_of_receipt(&exited_receipt(&last)).await.unwrap();
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(view.read().await.cohort_primary("mesh2", NodeKind::NodeAdmin).map(|n| n.name.to_string()), None, "held, the exited holder holds no seat: mesh2 has no primary");
        write().await.unwrap_or_else(|e| panic!("the fabric-primary writes the final primary's row Deleted: {e}"));
        let row = crate::storage::NodesStorage::node_row_at(&*storage, &last.node.name).await.unwrap().expect("the row stays at its path.name");
        assert_eq!((row.status, &row.node_id), (NodeStatus::Deleted, &last.node.node_id), "{row:?}");
        let outcomes: Vec<String> = capture.named("rdm.node_admin.node.delete.via-node-row").iter().map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        assert_eq!(outcomes, ["not-mesh-primary", "row-deleted"], "{outcomes:?}");
        let deleted = capture.named("rdm.node_admin.node.delete.via-node-row").pop().unwrap();
        eprintln!("span rdm.node_admin.node.delete.via-node-row trace_id={} {:?}", deleted.trace_id, deleted.attributes);
    }

    /// CONTRACT (fabric-node-lifecycle-elections.md §4.1: "Only the provider's exit proof vacates a
    /// seat"; node-recovery.md: "Exited is the proof, recorded durably"; CLAUDE.md read rule: a read
    /// is a point read through the store index, and node-local state that should be the durable
    /// answer is replaced by that read, never fed from it): the election's lost set is a read of the
    /// durable exit-proof rows. A fresh fabric-primary (its records hold nothing proven) whose store
    /// holds the exit-proof row of mesh2's seated final primary, among mesh2's status rows, resolves
    /// mesh2 with no primary at its first install: the exited birth is shown Dead, holds no seat, and
    /// the fabric-primary is the writer of mesh2's node rows (mesh-delete.md step 7: acting primary of
    /// a mesh with none left). Must NOT happen: the exited holder kept seated because no proof was
    /// fed into this admin's memory (the proof exists only as a durable row: written by another
    /// admin, or by this one before it restarted).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn a_fresh_admin_whose_store_holds_the_final_primarys_exit_proof_resolves_its_mesh_with_no_primary() {
        use crate::status_storage::StatusStorage;
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let last = with_id(digest("mesh2.admin.1", MemberStatus::Leaving), "200000000000");
        let (fp, book, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage).await;
        assert_eq!(view.read().await.cohort_primary("mesh2", NodeKind::NodeAdmin).map(|n| n.name.to_string()).as_deref(), Some("mesh2.admin.1"), "seated before any proof is read");
        // The proof as the store holds it: mesh2's status rows, written by whoever proved the exit.
        let status = Arc::new(crate::status_storage::MemoryStatusStorage::default());
        let proof = crate::status_storage::NodeExitRow { node_id: last.node.node_id.clone(), name: last.node.name.clone(), incarnation_id: last.node.incarnation.clone(), exit_code: Some(0), proof: "provider".into(), observed_at_ms: 1 };
        status.put_node_exit_of("mesh2", &proof).await.unwrap();
        records.set_status(status.clone(), "mesh1".into(), fp.node.name.clone(), fp.node.node_id.clone());
        let capture = crate::span_capture::Capture::new("i143-3068-fresh-admin-reads-exit-row");
        let installed = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await
        };
        let read = capture.named("rdm.node_admin.node.resolve.via-exit-proof-row");
        assert_eq!(read.len(), 1, "the install reads the final primary's exit-proof row once: {read:?}");
        let a = &read[0].attributes;
        assert_eq!((a.get("node").cloned(), a.get("node_id").cloned(), a.get("mesh").cloned()), (Some("mesh2.admin.1".to_string()), Some(last.node.node_id.to_string()), Some("mesh2".to_string())), "{a:?}");
        eprintln!("span rdm.node_admin.node.resolve.via-exit-proof-row trace_id={} {a:?}", read[0].trace_id);
        let held = installed.node(&last.node.name).map(|n| (n.status, n.is_primary));
        assert_eq!(held, Some((NodeStatus::Dead, false)), "the proven-exited holder is Dead and holds no seat");
        assert_eq!(installed.cohort_primary("mesh2", NodeKind::NodeAdmin).map(|n| n.name.to_string()), None, "mesh2 has no primary");
        assert!(crate::node_rows::writes_for(&installed, &fp.node.node_id, "mesh2"), "the fabric-primary writes mesh2's rows as acting primary");
    }

    /// CONTRACT (R-MD1 = A, i143-rulings-2026-10-11-dev.md: "The mesh-primary commits the mesh row
    /// Dead at mesh.deleted ... At mesh-left the fabric-primary writes the final primary's row
    /// Deleted, unchanged. Its ensure-Dead operation returns AlreadyApplied if the mesh-primary wrote
    /// Dead; only when no primary survives to make that write does the fabric-primary write Dead as
    /// acting primary ... Retries cause no second transition"; status_storage.rs: a mesh's status
    /// rows live under `status/<mesh>/`): with mesh2's row Dead already among mesh2's status rows
    /// (its primary's write), mesh-left's Dead write answers AlreadyApplied, writes nothing, and its
    /// `rdm.node_admin.mesh.update.via-dead-row` span says `already-applied` by `mesh-primary`; with
    /// none (no primary survived), the fabric-primary puts the row Dead among mesh2's status rows as
    /// acting primary (`applied`, `acting-primary`), and its replay answers AlreadyApplied. The Mesh
    /// row on mesh.storage follows the same rule (R-MN1 item 2, the name released): `already-dead`
    /// over the mesh-primary's, else `dead-written` once and `already-dead` on the replay. No Dead
    /// row of mesh2 is ever put among the fabric-primary's own mesh's rows. Must NOT happen: a second
    /// transition over the mesh-primary's Dead (the fabric-primary deciding from what it applied
    /// itself, which holds nothing of mesh2), or the row written under the fabric-primary's mesh.
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn mesh_lefts_dead_write_is_already_applied_over_the_mesh_primarys_dead_and_the_acting_primarys_when_none_survived() {
        use crate::status_storage::{MeshStatusRow, StatusStorage};
        let dead = |m: &str| MeshStatusRow { mesh_id: mesh_id(m), state: rdm_core::status::MeshState::Dead };
        let key = crate::mesh_delete::MeshDeleteKey { mesh_id: mesh_id("mesh2"), build_id: crate::build::BuildId::parse("6n8kaqdncqwv").unwrap(), attempt: 1, operation: format!("mesh.delete:{}", mesh_id("mesh2")) };
        for case in ["surviving-primary-wrote-dead", "no-survivor"] {
            let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
            let last = with_id(digest("mesh2.admin.1", MemberStatus::Leaving), "200000000000");
            let (_fp, book, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage.clone()).await;
            crate::storage::NodesStorage::put_node_row(&*storage, &crate::storage::NodeRow { name: last.node.name.clone(), node_id: last.node.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None }).await.unwrap().unwrap();
            prove_exit(&book, &records, &last).await;
            records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
            let status = crate::status_storage::MemoryStatusStorage::default();
            let meshes = crate::storage::MemoryMeshStorage::default();
            if case == "surviving-primary-wrote-dead" {
                // mesh2's primary committed Dead at mesh.deleted, among mesh2's status rows (R-MD1 item 1).
                status.put_mesh_status_of("mesh2", &dead("mesh2")).await.unwrap();
                // ... and its Mesh row Dead on mesh.storage with it (admin/leave.rs, mesh.deleted).
                crate::storage::MeshStorage::claim_mesh(&meshes, &crate::storage::MeshRecord { mesh_id: mesh_id("mesh2"), name: "mesh2".into(), dead: true }).await.unwrap().unwrap();
            }
            let capture = crate::span_capture::Capture::new(&format!("i143-3068-r-md1-{case}"));
            {
                let _g = tracing::dispatcher::set_default(&capture.dispatch);
                for _replay in 0..2 {
                    crate::mesh_delete::mesh_left_rows(records.node_rows().as_deref(), &status, &meshes, &records.declared, "mesh1.admin.1", &key, "mesh2", &[exited_receipt(&last)]).await.unwrap_or_else(|e| panic!("{case}: mesh-left's rows: {e}"));
                }
            }
            assert!(status.mesh_status_held_of("mesh2", &dead("mesh2")).await.unwrap(), "{case}: mesh2's row Dead stands among mesh2's status rows");
            assert_eq!(status.mesh_statuses().await.unwrap().get(&mesh_id("mesh2")), None, "{case}: no Dead row of mesh2 among the fabric-primary's own mesh's rows");
            let spans = capture.named("rdm.node_admin.mesh.update.via-dead-row");
            let said: Vec<(String, String, String)> = spans.iter().map(|s| (s.attributes.get("outcome").cloned().unwrap_or_default(), s.attributes.get("writer").cloned().unwrap_or_default(), s.attributes.get("mesh_row").cloned().unwrap_or_default())).collect();
            let expected: Vec<(String, String, String)> = match case {
                "surviving-primary-wrote-dead" => vec![("already-applied".into(), "mesh-primary".into(), "already-dead".into()); 2],
                _ => vec![("applied".into(), "acting-primary".into(), "dead-written".into()), ("already-applied".into(), "acting-primary".into(), "already-dead".into())],
            };
            assert_eq!(said, expected, "{case}: one transition at most, and the span names who wrote the row: {spans:?}");
            eprintln!("{case}: span rdm.node_admin.mesh.update.via-dead-row trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
        }
    }

    /// CONTRACT (R-MN1: a mesh removal deletes every roster birth, and a mesh created again under
    /// the name is a new mesh; mesh-delete.md step 7: at mesh-left the fabric-primary is the acting
    /// primary of a mesh that has none left, and departed births resolve Gone; R14 item 2: a mesh's
    /// contacts are its primary's rows; `dropped_contacts`: a departed birth's contact is dropped, so
    /// a maker's stored map never hands it to a mesh created again under the same name): mesh-left
    /// drops the contact of every birth its proof set names from the removed mesh's contacts, so
    /// the stored map the fabric-primary answers a recreated mesh's first admin with names none of
    /// them. Must NOT happen: mesh_identity::canonical_ids at :208 (trace
    /// a6f24e5914ac3658d302079ddb0c28f0): the recreated mesh2.admin.1's entry sweep pings the removed
    /// mesh's births (via-entry-sweep-no-reply), handed by the maker's stored map from the contacts
    /// nobody dropped (rdm.mesh.topology.serve.via-read stored 1, stored_nodes 5).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn mesh_left_drops_the_removed_meshs_contacts_of_every_birth_its_proof_set_names() {
        use crate::storage::NodesStorage;
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let last = with_id(digest("mesh2.admin.1", MemberStatus::Leaving), "200000000000");
        let member = with_id(digest("mesh2.rpc.1", MemberStatus::Leaving), "210000000000");
        let (fp, book, records, view, _rows) = fabric_primary_over(std::slice::from_ref(&last), storage.clone()).await;
        storage.put_node_row(&crate::storage::NodeRow { name: last.node.name.clone(), node_id: last.node.node_id.clone(), status: NodeStatus::ReadyForTraffic, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None }).await.unwrap().unwrap();
        let contact = |d: &MeshDigest| crate::storage::NodeRecord { node_id: d.node.node_id.clone(), fabric_id: fabric1(), name: d.node.name.clone(), mesh_id: mesh_id(&d.node.name.mesh), incarnation_id: d.node.incarnation.clone(), endpoint_id: d.node.endpoint_id.clone(), transport_addr: d.node.transport_addr, listeners: Vec::new() };
        // mesh2's primary wrote its members' contacts while it held the seat; mesh1's own stays.
        for d in [&last, &member, &fp] {
            storage.put_contact(&contact(d)).await.unwrap();
        }
        prove_exit(&book, &records, &last).await;
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        let key = crate::mesh_delete::MeshDeleteKey { mesh_id: mesh_id("mesh2"), build_id: crate::build::BuildId::parse("6n8kaqdncqwv").unwrap(), attempt: 1, operation: format!("mesh.delete:{}", mesh_id("mesh2")) };
        let status = crate::status_storage::MemoryStatusStorage::default();
        let capture = crate::span_capture::Capture::new("i143-3068-mesh-left-contacts");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            crate::mesh_delete::mesh_left_rows(records.node_rows().as_deref(), &status, &crate::storage::MemoryMeshStorage::default(), &records.declared, "mesh1.admin.1", &key, "mesh2", &[exited_receipt(&last), exited_receipt(&member)]).await.unwrap_or_else(|e| panic!("mesh-left's rows: {e}"));
        }
        let held: Vec<String> = storage.contacts().await.unwrap().into_iter().map(|c| c.name.to_string()).collect();
        assert_eq!(held, vec!["mesh1.admin.1".to_string()], "no contact of the removed mesh's births stands; another mesh's is untouched");
        let spans = capture.named("rdm.node_admin.node.remove.via-mesh-left-contact");
        assert_eq!(spans.len(), 1, "{spans:?}");
        let dropped = spans[0].attributes.get("dropped").cloned().unwrap_or_default();
        assert!(dropped.contains("mesh2.admin.1") && dropped.contains("mesh2.rpc.1"), "the span names each birth whose contact is dropped: {dropped}");
        eprintln!("span rdm.node_admin.node.remove.via-mesh-left-contact trace_id={} {:?}", spans[0].trace_id, spans[0].attributes);
    }

    /// CONTRACT (mesh-delete.md, "Resume after the primary's loss", steps 1-2: the lost final
    /// primary's exact Exited is proven and recorded first, then the fabric-primary creates the one
    /// node-admin of a mesh with no live admin; R11: record predecessor Exited first): the proven
    /// exit, recorded as its exit-proof row and read by the install `admin_for_deleting_mesh` runs
    /// before the create, vacates the lost holder's seat, so the fabric-primary is the writer of the mesh's
    /// rows and its claim of the created admin's name is written. Must NOT happen: the refusal of
    /// mesh_lifecycle::mesh_delete_no_survivor on 8803f84e ("AllocateIdentity failed: the name claim
    /// refused the birth ...: mesh1.admin.1 is not the writer of mesh2's node rows").
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn the_lost_only_admins_exit_held_admits_the_fabric_primarys_claim_for_the_admin_it_creates() {
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let lost = with_id(digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), "200000000000");
        let (_fp, book, records, view, rows) = fabric_primary_over(std::slice::from_ref(&lost), storage.clone()).await;
        let capture = crate::span_capture::Capture::new("i143-3068-leaving-mesh-admin-claim");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let created: PathName = "mesh2.admin.2".parse().unwrap();
        let refused = rows.claim(&created, &NodeId::mint(), "node.create:mesh2.admin.2", None, crate::node_rows::ClaimFor::Create).await;
        assert!(matches!(&refused, Err(crate::node_rows::RowRefused::NotMeshPrimary(_))), "unheld, the lost holder still writes mesh2's rows: {refused:?}");
        prove_exit(&book, &records, &lost).await;
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        let born = NodeId::mint();
        match rows.claim(&created, &born, "node.create:mesh2.admin.2", None, crate::node_rows::ClaimFor::Create).await {
            Ok(crate::node_rows::Claimed::Written { row, .. }) => assert_eq!((row.node_id, row.status), (born, NodeStatus::Pending)),
            other => panic!("the fabric-primary claims the created admin's name once the lost holder's exit is held: {other:?}"),
        }
        let mut outcomes: Vec<String> = capture.named("rdm.node_admin.node.create.via-node-row").iter().filter(|s| s.attributes.get("moment").map(String::as_str) == Some("claim")).map(|s| s.attributes.get("outcome").cloned().unwrap_or_default()).collect();
        outcomes.dedup();
        assert_eq!(outcomes, ["not-mesh-primary", "row-written"], "{outcomes:?}");
        let written = capture.named("rdm.node_admin.node.create.via-node-row").pop().unwrap();
        eprintln!("span rdm.node_admin.node.create.via-node-row trace_id={} {:?}", written.trace_id, written.attributes);
    }

    /// CONTRACT (R-EL1: a seat moves only by explicit yield or proven loss; seat_watch.rs: "Only a
    /// runtime found exited is a verified loss"; mesh-delete.md, "Resume after the primary's loss":
    /// the workflow goes to the admin seated): mesh2's primary `mesh2.admin.1` is lost and
    /// `mesh2.admin.2` is live. Once the lost holder's exact Exited is proven and its exit-proof row
    /// put, the seat is `mesh2.admin.2`'s in the fabric-primary's view (the leave a later attempt
    /// resumes hands it the workflow) and in its own, which reads the same row and announces the
    /// seat at the next epoch. Must NOT happen: the no_survivor attempts refused "mesh2.admin.2 is live and
    /// holds no seat yet" after mesh2.admin.1's exit was proven (trace 5bc960a3e759a77cfb42ae6e9bc687d0).
    // @feature: mesh-lifecycle
    #[tokio::test]
    async fn the_lost_primarys_exit_held_seats_the_live_admin_in_the_fabric_primarys_view_and_its_own() {
        use rdm_core::Seat::MeshPrimary;
        let storage = Arc::new(crate::storage::MemoryNodesStorage::default());
        let lost = with_id(digest("mesh2.admin.1", MemberStatus::ReadyForTraffic), "200000000000");
        let live = with_id(digest("mesh2.admin.2", MemberStatus::ReadyForTraffic), "300000000000");
        let (_fp, book, records, view, _rows) = fabric_primary_over(&[lost.clone(), live.clone()], storage).await;
        let primary = |t: &Topology| t.cohort_primary("mesh2", NodeKind::NodeAdmin).map(|n| n.name.to_string());
        assert_eq!(primary(&*view.read().await).as_deref(), Some("mesh2.admin.1"), "unheld, the lost holder keeps its seat (R-EL1)");
        prove_exit(&book, &records, &lost).await;
        records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(primary(&*view.read().await).as_deref(), Some("mesh2.admin.2"), "held, the fabric-primary reads the live admin seated");
        // mesh2.admin.2's own view: its investigation's exit proof, read from the same row.
        let own = Records::default();
        own.set_status(records.status.get().unwrap().storage().clone(), "mesh2".into(), live.node.name.clone(), live.node.node_id.clone());
        own.set_book(book.clone());
        let view2 = RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &own));
        own.install_projected(&view2, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &own)).await;
        let decided = incumbency_of(&book, &own);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &book, &own, &decided, &ViewReads::none_at(std::time::Instant::now()));
        assert_eq!(primary(&t).as_deref(), Some("mesh2.admin.2"), "mesh2.admin.2 reads itself seated");
        let said = seat_announcements(&t, true, "mesh2", (&live.node.node_id, &live.node.incarnation), &decided);
        assert!(said.iter().any(|(seat, h)| *seat == MeshPrimary && h.mesh == "mesh2" && h.is_birth(&live.node.node_id, &live.node.incarnation) && h.revision == 2), "mesh2.admin.2 announces mesh2's seat at the next revision: {said:?}");
    }

    /// CONTRACT (R-EL1, fabric-node-lifecycle-elections.md §5 "a living holder keeps its seat";
    /// proposal node-primary-update Part C3 edit 6: the committed seat row is the authority, "primary
    /// updated" only announces it): the view's seat readers name the holder the
    /// seat records give, whatever its status. A holder that is Leaving (drained and stopped by its
    /// Build) whose yield this observer has not heard yet keeps both seats; once the yield is held
    /// (PrimaryUpdated: both records at the next revision naming the successor), the readers name the
    /// successor. Must NOT happen: an observer reading a Leaving holder's seats as vacant (the
    /// sticky_seat hand-over cell's via-fabric-recompute with winner "" while the record names P).
    #[test]
    fn a_leaving_holder_keeps_both_seats_in_the_readers_until_its_yield_is_held() {
        use MemberStatus::*;
        use rdm_core::Seat::{FabricPrimary, MeshPrimary};
        let book = DigestBook::default();
        let records = Records::default();
        let holder = with_id(digest("mesh1.admin.1", Leaving), "500000000000");
        let successor = with_id(digest("mesh1.admin.2", ReadyForTraffic), "800000000000");
        let other = with_id(digest("mesh2.admin.1", ReadyForTraffic), "300000000000");
        for d in [&holder, &successor, &other] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&holder, 1));
        book.seats.take(FabricPrimary, &holder_of(&holder, 1));
        book.seats.take(MeshPrimary, &holder_of(&other, 1));
        let seat_names = |t: &Topology| (t.fabric_primary().map(|n| n.name.to_string()), t.cohort_primary("mesh1", NodeKind::NodeAdmin).map(|n| n.name.to_string()));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        let flags = t.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").map(|n| (n.status, n.is_primary, n.is_fabric_primary));
        assert_eq!(flags, Some((NodeStatus::Leaving, true, true)), "the election keeps the Leaving holder seated (R-EL1)");
        assert_eq!(seat_names(&t), (Some("mesh1.admin.1".into()), Some("mesh1.admin.1".into())), "the readers name the Leaving holder until its yield is held");
        assert!(t.holds_fabric_seat(&holder.node.node_id) && t.holds_mesh_seat("mesh1", &holder.node.node_id));
        assert_eq!(t.mesh_view("mesh1").and_then(|m| m.primary_admin).map(|p| p.to_string()).as_deref(), Some("mesh1.admin.1"));

        // The yield is held: PrimaryUpdated's records name the successor at the next revisions.
        book.seats.take(MeshPrimary, &holder_of(&successor, 2));
        book.seats.take(FabricPrimary, &holder_of(&successor, 2));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(seat_names(&t), (Some("mesh1.admin.2".into()), Some("mesh1.admin.2".into())), "the readers name the successor once the yield is held");
        assert!(!t.holds_fabric_seat(&holder.node.node_id) && !t.holds_mesh_seat("mesh1", &holder.node.node_id));
    }

    /// CONTRACT (fabric-node-lifecycle-elections.md §4.4, lifecycles/node-primary-update.md step 3): the
    /// committed next-revision seat record is the resolver's input and the incumbent yields fabric
    /// authority. A hierarchy-loop decision is announced against the seat record it was decided
    /// from: a committed handover that lands while the loop awaits (its round climb) is not
    /// answered by the incumbent taking the seat back at the next revision. Must NOT happen: the
    /// yielded incumbent announcing itself over the committed successor (the e3..e12 seat flap of
    /// the fabric-primary handover stem on f06c8197).
    #[tokio::test]
    async fn a_handover_committed_while_the_loop_awaits_is_not_taken_back_by_the_incumbent() {
        use MemberStatus::*;
        use rdm_core::Seat::{FabricPrimary, MeshPrimary};
        let book = DigestBook::default();
        let records = Records::default();
        let me = with_id(digest("mesh1.admin.1", ReadyForTraffic), "500000000000");
        let sibling = with_id(digest("mesh1.admin.2", ReadyForTraffic), "800000000000");
        let successor = with_id(digest("mesh2.admin.1", ReadyForTraffic), "300000000000");
        for d in [&me, &sibling, &successor] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&me, 1));
        book.seats.take(MeshPrimary, &holder_of(&successor, 1));
        book.seats.take(FabricPrimary, &holder_of(&me, 1));
        let me_birth = (&me.node.node_id, &me.node.incarnation);
        // The loop decides: this admin holds both of its seats, and their records already name it.
        let decided = incumbency_of(&book, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &book, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh1.admin.1"));
        // While the loop awaits its climb, the committed transfer lands: mesh2.admin.1 at revision 2.
        book.seats.take(FabricPrimary, &holder_of(&successor, 2));
        let said = seat_announcements(&t, true, "mesh1", me_birth, &decided);
        assert!(said.is_empty(), "the yielded incumbent announces nothing over the committed successor: {said:?} (decided against {:?})", decided.fabric);

        // A legitimate succession is still announced: the recorded holder's birth proven exited,
        // the election seats its own mesh's next admin, at the next revision over that record.
        let book = DigestBook::default();
        let records = Records::default();
        let gone = with_id(digest("mesh1.admin.2", ReadyForTraffic), "100000000000");
        for d in [&me, &gone] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&gone, 5));
        book.seats.take(FabricPrimary, &holder_of(&gone, 5));
        prove_exit(&book, &records, &gone).await;
        let decided = incumbency_of(&book, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &book, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        let said = seat_announcements(&t, true, "mesh1", me_birth, &decided);
        let revisions: Vec<_> = said.iter().map(|(s, h)| (*s, h.node_id.clone(), h.revision)).collect();
        assert_eq!(revisions, vec![(MeshPrimary, me.node.node_id.clone(), 6), (FabricPrimary, me.node.node_id.clone(), 6)], "the successor of a proven-gone holder takes both seats at the next revision (decided against {:?})", decided.fabric);
    }

    /// CONTRACT (R-MT1 Q3: the mesh-primary "writes the cohort's seat row as the mesh's one row writer"
    /// and is the sole author of member status; proposal Part B1, every cohort keeps its seat by the one
    /// rule): a mesh-primary announces the seat of each other cohort of its mesh the election fills
    /// with a birth its record does not name (a first election, a holder proven gone), at the next
    /// revision over that record, and nothing while the record names the living holder. No other admin
    /// announces a cohort seat. Must NOT happen: an rpc cohort's seat that no record holds, so a
    /// lower NodeId that later becomes Ready displaces the living holder.
    #[tokio::test]
    async fn a_mesh_primary_announces_its_meshs_cohort_seats_the_election_fills() {
        use MemberStatus::*;
        use rdm_core::Seat::{Cohort, FabricPrimary, MeshPrimary};
        let book = DigestBook::default();
        let records = Records::default();
        let me = with_id(digest("mesh1.admin.1", ReadyForTraffic), "500000000000");
        let sibling = with_id(digest("mesh1.admin.2", ReadyForTraffic), "800000000000");
        let rpc1 = with_id(digest("mesh1.rpc.1", ReadyForTraffic), "100000000000");
        let rpc2 = with_id(digest("mesh1.rpc.2", ReadyForTraffic), "300000000000");
        for d in [&me, &sibling, &rpc1, &rpc2] {
            book.record((*d).clone());
        }
        book.seats.take(MeshPrimary, &holder_of(&me, 1));
        book.seats.take(FabricPrimary, &holder_of(&me, 1));
        let me_birth = (&me.node.node_id, &me.node.incarnation);
        let cohort_said = |said: &[(rdm_core::Seat, rdm_core::SeatHolder)]| said.iter().filter(|(s, _)| matches!(s, Cohort(_))).map(|(s, h)| (*s, h.node_id.to_string(), h.revision)).collect::<Vec<_>>();
        // A first election: no record of the rpc cohort, the lowest Ready fills it at revision 1.
        let decided = incumbency_of(&book, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &book, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        let said = seat_announcements(&t, true, "mesh1", me_birth, &decided);
        assert_eq!(cohort_said(&said), vec![(Cohort(NodeKind::RpcNode), rpc1.node.node_id.to_string(), 1)], "the mesh-primary seats mesh1.rpc.1: {said:?}");
        let sib = seat_announcements(&t, true, "mesh1", (&sibling.node.node_id, &sibling.node.incarnation), &decided);
        assert!(cohort_said(&sib).is_empty(), "an admin that is not the mesh-primary announces no cohort seat: {sib:?}");
        // The record names the living mesh1.rpc.2: it keeps the seat over the lower Ready NodeId and nothing is said.
        book.seats.take(Cohort(NodeKind::RpcNode), &holder_of(&rpc2, 3));
        let decided = incumbency_of(&book, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &book, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        assert_eq!(t.cohort_primary("mesh1", NodeKind::RpcNode).map(|n| n.name.to_string()).as_deref(), Some("mesh1.rpc.2"));
        assert!(cohort_said(&seat_announcements(&t, true, "mesh1", me_birth, &decided)).is_empty(), "a living recorded holder is not announced again");
        // mesh1.rpc.2 proven exited: the seat goes to the lowest Ready, announced at the next revision.
        prove_exit(&book, &records, &rpc2).await;
        let decided = incumbency_of(&book, &records);
        let t = project_decided("fabric1", &fabric1(), ProviderKind::Process, &book, &records, &decided, &ViewReads::none_at(std::time::Instant::now()));
        let said = seat_announcements(&t, true, "mesh1", me_birth, &decided);
        assert_eq!(cohort_said(&said), vec![(Cohort(NodeKind::RpcNode), rpc1.node.node_id.to_string(), 4)], "{said:?}");
    }

    #[test]
    fn the_projection_elects_the_lowest_ready_node_id_per_cohort_and_across_mesh_primaries() {
        use MemberStatus::*;
        let t = view(&[
            with_id(digest("mesh2.admin.1", ReadyForTraffic), "200000000000"),
            with_id(digest("mesh2.rpc.2", ReadyForTraffic), "700000000000"),
            with_id(digest("mesh2.rpc.1", Pending), "000000000001"),
            with_id(digest("mesh1.admin.2", ReadyForTraffic), "300000000000"),
            with_id(digest("mesh1.admin.1", ReadyForTraffic), "400000000000"),
            with_id(digest("mesh1.rpc.3", ReadyForTraffic), "500000000000"),
        ]);
        let mut primaries: Vec<String> = t.nodes.iter().filter(|n| n.is_primary).map(|n| n.name.to_string()).collect();
        primaries.sort();
        assert_eq!(primaries, ["mesh1.admin.2", "mesh1.rpc.3", "mesh2.admin.1", "mesh2.rpc.2"], "a pending member is never primary");
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh2.admin.1"), "not the lowest-named mesh");
        assert!(t.violations().is_empty(), "{:?}", t.violations());
        assert_eq!(t.mesh_view("mesh2").unwrap().id, Some(mesh_id("mesh2")));
        assert_eq!(t.fabric.status, ScopeStatus::Pending, "a fabric-primary existing does not make the fabric ready: the view carries the held fabric state");
    }

    #[test]
    fn ready_since_and_ordinal_never_change_a_winner() {
        let at = |name: &str, id: &str, since: u64| {
            let mut d = with_id(digest(name, MemberStatus::ReadyForTraffic), id);
            // A digest still carrying the retired readiness claim: a fact nothing reads.
            d.config.insert("ready_since_ms".into(), since.to_string());
            d
        };
        // rpc.3 has the highest ordinal and the latest ready claim, and the lowest id.
        let a = view(&[at("mesh1.admin.1", "100000000000", 10), at("mesh1.rpc.1", "900000000000", 1), at("mesh1.rpc.2", "800000000000", 2), at("mesh1.rpc.3", "200000000000", 900)]);
        let b = view(&[at("mesh1.admin.1", "100000000000", 999), at("mesh1.rpc.1", "900000000000", 900), at("mesh1.rpc.2", "800000000000", 800), at("mesh1.rpc.3", "200000000000", 1)]);
        for t in [a, b] {
            let mut primaries: Vec<String> = t.nodes.iter().filter(|n| n.is_primary).map(|n| n.name.to_string()).collect();
            primaries.sort();
            assert_eq!(primaries, ["mesh1.admin.1", "mesh1.rpc.3"]);
        }
    }

    #[test]
    fn an_admin_is_authority_capable_only_when_every_held_birth_is_adoptable_here() {
        use crate::deployment::process::ProcessDeploymentProvider;
        use crate::deployment::provider::DeploymentProvider;
        use rdm_core::{RuntimeLocator, RuntimeProvider};
        let p = ProcessDeploymentProvider::new();
        let with = |name: &str, domain: Option<String>| {
            let mut d = digest(name, MemberStatus::ReadyForTraffic);
            d.node.runtime = domain.map(|control_domain| rdm_core::RuntimeFact {
                deployment_id: rdm_core::fixture_crockford60("dep"),
                provider: RuntimeProvider::Process,
                control_domain,
                locator: RuntimeLocator::Process { pid: 4242, start: 17 },
            });
            d
        };
        let here = p.control_domain();
        assert!(authority_blockers(&[with("mesh1.rpc.1", Some(here.clone())), with("mesh1.admin.1", Some(here.clone()))], &p).is_empty());
        let blocked = authority_blockers(
            &[with("mesh1.rpc.1", Some(here.clone())), with("mesh1.rpc.2", None), with("mesh1.rpc.3", Some("process:another-host:1".into()))],
            &p,
        );
        assert_eq!(blocked.len(), 2, "{blocked:?}");
        assert!(blocked[0].starts_with("mesh1.rpc.2: publishes no runtime fact"), "{blocked:?}");
        assert!(blocked[1].starts_with("mesh1.rpc.3:") && blocked[1].contains("control domain"), "{blocked:?}");
    }

    /// CONTRACT (i143 export gate: normal joining or restarted admins cannot self-Ready; only the
    /// Day-0 root may apply its own mesh's Pending): a mesh's first admin that is not Day 0 is held
    /// by name until the fabric primary applies Pending; an admin joining a live cohort owes
    /// nothing; Day 0 alone self-applies; an applied Pending holds no one.
    #[test]
    fn only_the_day0_root_applies_its_own_meshs_pending() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        assert_eq!(pending_gate(&me, "mesh2", true, false, false), PendingGate::SelfApply, "Day 0 has no upstream authority");
        match pending_gate(&me, "mesh2", false, false, false) {
            PendingGate::Blocked(why) => assert!(why.contains("not been applied here by the fabric primary"), "{why}"),
            other => panic!("a first admin that is not Day 0 never self-applies: {other:?}"),
        }
        assert_eq!(pending_gate(&me, "mesh2", false, false, true), PendingGate::Clear, "joining a live cohort owes no Pending");
        assert_eq!(pending_gate(&me, "mesh2", false, true, false), PendingGate::Clear);
        assert_eq!(pending_gate(&me, "mesh2", true, true, false), PendingGate::Clear, "Day 0 applies it once");
    }

    /// CONTRACT (R-BI3: the current Build is read from the Build entities; node-start.md step 4, the
    /// Build floor): an admin that holds no current Build, or not the facts of the Build its entry
    /// named at the attempt the answering admin held, is blocked from Ready, each reason named with
    /// the request that recovers it. Day 0 (no entry) owes no floor.
    #[tokio::test]
    async fn an_admin_without_a_current_build_is_never_authority_capable() {
        use crate::build_state::{AttemptOpened, AttemptReason, BuildAccepted, BuildAttemptClaim, BuildStateAdapter as _, MemoryBuildStateAdapter};
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let builds = MemoryBuildStateAdapter::new();
        let blocked = hydration_blocker(&me, &builds, None).await.expect("blocked while unhydrated");
        assert_eq!(blocked.kind(), "no-current-build");
        assert!(blocked.to_string().starts_with("mesh1.admin.2: holds no current Build"), "{blocked}");
        let id = crate::build::BuildId::parse("7wy02yhavgde").unwrap();
        let blocked = hydration_blocker(&me, &builds, Some((id.clone(), 2))).await.expect("blocked while the entry's Build is not held");
        assert_eq!(blocked.need(), crate::hydrate::Need::BuildFacts(id.clone()), "{blocked}");
        assert!(blocked.to_string().contains(&format!("holds no facts of Build {id}")), "{blocked}");
        builds.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 }).await.unwrap();
        assert_eq!(hydration_blocker(&me, &builds, None).await, None, "an admin served no entry owes no attempt floor");
        let blocked = hydration_blocker(&me, &builds, Some((id.clone(), 2))).await.expect("blocked below the entry's attempt").to_string();
        assert!(blocked.contains(&format!("holds attempt 0 of Build {id}")) && blocked.contains("held attempt 2"), "{blocked}");
        for attempt in 1..=2 {
            if attempt > 1 {
                builds.open_attempt(&AttemptOpened { mesh: None, build_id: id.clone(), attempt, reason: AttemptReason::Restart, action: None, opened_by: "x".into(), opened_at_ms: 0 }).await.unwrap();
            }
            builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
        }
        assert_eq!(hydration_blocker(&me, &builds, Some((id.clone(), 2))).await, None, "its attempt facts reached the entry's");
        let other = crate::build::BuildId::parse("qsmy968wj1zt").unwrap();
        let blocked = hydration_blocker(&me, &builds, Some((other.clone(), 9))).await.expect("a floor naming a Build not held blocks until its facts are held");
        assert_eq!(blocked.need(), crate::hydrate::Need::BuildFacts(other), "{blocked}");
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
        let topology = RwLock::new(view(&[digest("mesh1.admin.1", MemberStatus::ReadyForTraffic)]));
        let same = topology.read().await.clone();
        records.install_projected(&topology, || same.clone()).await;
        assert!(quiet(&records).await, "the same view wakes nothing");
        let changed = view(&[digest("mesh1.admin.1", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic)]);
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
    /// (`SubjectUnknown`); after it, its first declaration (`ReadyForTraffic`) is `Applied`. Nothing
    /// checks who is calling (node-rpc.md §14): the same declaration from an endpoint that never
    /// joined is decided for the subject it names.
    #[tokio::test]
    async fn an_admitted_node_admin_is_a_held_subject_before_its_digest_is_gossiped() {
        use crate::join::{Deployed, JoinDoor, Joins};
        use crate::status_rpc::{Declared, StatusAuthority};
        use rdm_core::status::{NodeState, NotAuthority, StatusReply, StatusRequest};
        use rdm_core::join::{JoinReply, JoinRequest};
        let book = DigestBook::default();
        let records = Arc::new(Records::default());
        book.record(with_id(digest("mesh1.admin.1", MemberStatus::ReadyForTraffic), "100000000000"));
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)));
        let me = topology.read().await.fabric_primary().expect("admin.1 holds the fabric seat").clone();
        let authority = StatusAuthority {
            held_takeover: Default::default(),
            declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
            stopped: Default::default(),
            me: me.name.clone(),
            node_id: me.node_id.clone(),
            fabric_id: fabric1(),
            topology: topology.clone(),
            declared: records.declared.clone(),
            status_storage: Arc::new(crate::status_storage::MemoryStatusStorage::default()),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(std::sync::OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(std::sync::OnceLock::new()),
            mesh_deleter: Arc::new(std::sync::OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: records.wake.clone(),
            rounds: Default::default(),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
            loads: None,
            book: None,
            first_ready: Default::default(),
            builds: std::sync::Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
        };
        let _ = Declared::default();

        // The birth this admin deployed, and the digest it reports on its first call.
        let runtime = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-admitted")).unwrap();
        let mut reported = with_id(digest("mesh1.admin.2", MemberStatus::Pending), "200000000000");
        reported.node.runtime = Some(runtime.clone());
        reported.data_dir = Some("/data/mesh1.admin.2".into());
        let deployed = Deployed {
            name: reported.node.name.clone(),
            node_id: reported.node.node_id.clone(),
            incarnation: reported.node.incarnation.clone(),
            supersedes: None,
            endpoint_id: reported.node.endpoint_id.clone(),
            runtime,
            data_dir: "/data/mesh1.admin.2".into(),
            last_accepted_name: None,
            held: false,
        };
        let joins = Arc::new(Joins::default());
        let _wait = joins.expect(deployed);
        let installed = book.clone();
        let door = JoinDoor {
            me: me.name.clone(),
            joins,
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(crate::wire::JoinAnswer {
                        served_by: "mesh1.admin.1".into(),
                        control: crate::wire::JoinControl { provider: ProviderKind::Process, fabric: None, build: None, rafka_time_ms: 1_000, member_cert: Vec::new(), name: None, config: None },
                        statuses: vec![],
                    })
                })
            }),
            install: Arc::new(move |d: &MeshDigest| {
                installed.record(d.clone());
            }),
            known: view_projector("mesh1.admin.1".parse().unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone()),
            primary: Arc::new(|| None),
            row: Arc::new(|_| Box::pin(async { Ok(None) })),
            issuer: Arc::new(crate::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let endpoint = reported.node.endpoint_id.clone();
        assert!(authority.sender_of(&endpoint).await.is_none(), "before the join the authority's view does not hold the birth");
        let declare = StatusRequest::DeclareNodeState { node_id: reported.node.node_id.clone(), incarnation: reported.node.incarnation.clone(), state: NodeState::ReadyForTraffic, operation: None };
        assert_eq!(authority.apply(None, &declare).await, StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, "before the join the subject is not held");
        let joined = door.serve(endpoint.clone(), JoinRequest::JoinNode { digest: (&reported).into() }).await;
        assert!(matches!(joined, JoinReply::Joined { .. }), "{joined:?}");

        // Acceptance made the birth known; no projection tick ran since.
        let sender = authority.sender_of(&endpoint).await.expect("the admitted birth is known when its join is answered");
        assert_eq!(authority.apply(Some(sender), &declare).await, StatusReply::Applied);

        // An endpoint that never joined sends the same declaration: decided for the subject it names.
        assert!(authority.sender_of(&EndpointId("key-never-joined".into())).await.is_none());
        assert_eq!(authority.apply(None, &declare).await, StatusReply::AlreadyApplied);
    }

    #[test]
    fn a_mesh_without_a_ready_admin_cedes_the_fabric_and_stays_pending() {
        use MemberStatus::*;
        let t = view(&[digest("mesh1.admin.1", ConnectionsDeleting), digest("mesh1.rpc.1", ReadyForTraffic), digest("mesh2.admin.1", ReadyForTraffic)]);
        assert_eq!(t.fabric_primary().map(|n| n.name.to_string()).as_deref(), Some("mesh2.admin.1"));
        assert_eq!(t.meshes.iter().find(|m| m.name == "mesh1").unwrap().status, ScopeStatus::Pending);
        let empty = view(&[]);
        assert!(empty.fabric_primary().is_none());
        assert_eq!(empty.fabric.status, ScopeStatus::Pending);
    }

    /// CONTRACT (CLAUDE.md read rule: a kind with no projection is a point read through its store
    /// index; R12/R13: the mesh-primary writes the row, a stopped node is told nothing): `mesh1.rpc.1`
    /// is stopped and its mesh-primary moves its row to `mesh1.rpc.1.old` on the fabric's shared store
    /// root. Another admin, which performed no rename and holds nothing of it, lists the parked birth
    /// at the row's name: its view reads the name through the row's `path.name` key. What must NOT
    /// happen: that admin listing the birth at `mesh1.rpc.1`, the name its digest last published, so a
    /// start or a `node.get` there names a row that no longer holds it.
    #[tokio::test]
    async fn a_parked_birth_whose_row_another_admin_moved_is_listed_at_the_rows_name() {
        let d = crate::record_store::tempdir("row-name-view");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let primary = crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let mine: Arc<dyn crate::storage::NodesStorage> = Arc::new(crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &IncarnationId::mint()).await.unwrap());
        let book = DigestBook::default();
        let held = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(held.clone());
        assert!(book.stopped(rdm_core::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "node.stop:mesh1.rpc.1".into(),
            node_id: held.node.node_id.clone(),
            incarnation: held.node.incarnation.clone(),
            name: held.node.name.clone(),
            event_at_rafka_ms: 1,
        }));
        // The mesh-primary's rename on the shared root, through the one writer of a row's move.
        let to: PathName = "mesh1.rpc.1.old".parse().unwrap();
        let mut record = Node::allocated(held.node.name.clone());
        record.node_id = held.node.node_id.clone();
        record.incarnation_id = Some(held.node.incarnation.clone());
        record.status = NodeStatus::Leaving;
        let row = crate::storage::NodeRow { name: held.node.name.clone(), node_id: held.node.node_id.clone(), status: NodeStatus::Leaving, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None };
        assert_eq!(crate::storage::NodesStorage::put_node_row(&primary, &row).await.unwrap(), Ok(()));
        crate::node_rows::move_row(&primary, &"mesh1.admin.1".parse().unwrap(), &record, BTreeMap::new(), Default::default(), &held.node.name, &to).await.unwrap();
        // This admin: the same book, its own store on the same root, no rename of its own.
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(mine);
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestBook::default(), &Records::default())));
        view_projector("mesh1.admin.1".parse().unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        let t = topology.read().await.clone();
        let names: Vec<String> = t.nodes.iter().map(|n| n.name.to_string()).collect();
        let n = t.node(&to).unwrap_or_else(|| panic!("the parked birth is listed at its row's name: {names:?}"));
        assert_eq!((n.node_id.clone(), n.incarnation_id.clone(), n.status, n.parked), (held.node.node_id.clone(), Some(held.node.incarnation.clone()), NodeStatus::Leaving, true));
        assert!(t.node(&held.node.name).is_none(), "nothing is listed at the name the row moved from: {names:?}");
        // The book's held stopped digest keeps the name the node last published (R-JN1's last accepted
        // name): only the view names the birth by its row.
        assert_eq!(book.parked().iter().map(|d| d.node.name.clone()).collect::<Vec<_>>(), vec![held.node.name.clone()]);
        // The row moved back (node-replace.md Rollback): the next projection reads it there.
        crate::node_rows::move_row(&primary, &"mesh1.admin.1".parse().unwrap(), &record, BTreeMap::new(), Default::default(), &to, &held.node.name).await.unwrap();
        view_projector("mesh1.admin.1".parse().unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        let t = topology.read().await.clone();
        assert!(t.node(&held.node.name).is_some_and(|n| n.node_id == held.node.node_id && n.parked) && t.node(&to).is_none(), "{:?}", t.nodes.iter().map(|n| n.name.to_string()).collect::<Vec<_>>());
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (CLAUDE.md error rule: "never written" and "could not be read" are different facts):
    /// a parked birth whose row cannot be read (its entry in the row's id index is not a record this
    /// build reads) is not listed by the view, its name being unknown, and the refused read is one
    /// span naming the reading admin, the birth, the index and the store's own cause. What must NOT
    /// happen: the birth listed at the name its digest last published as if no row had moved it, or
    /// the refusal silent.
    #[tokio::test]
    async fn a_parked_birth_whose_row_cannot_be_read_is_not_listed_and_the_refusal_is_spanned() {
        let d = crate::record_store::tempdir("row-name-unreadable");
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let mine: Arc<dyn crate::storage::NodesStorage> = Arc::new(crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &IncarnationId::mint()).await.unwrap());
        let book = DigestBook::default();
        let held = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(held.clone());
        assert!(book.stopped(rdm_core::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "node.stop:mesh1.rpc.1".into(),
            node_id: held.node.node_id.clone(),
            incarnation: held.node.incarnation.clone(),
            name: held.node.name.clone(),
            event_at_rafka_ms: 1,
        }));
        let file = root.nodes("mesh1").join("ids").join(format!("{}.json", held.node.node_id));
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"not an index entry").unwrap();
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(mine);
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestBook::default(), &Records::default())));
        let capture = crate::span_capture::Capture::new("row-name-unreadable");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            view_projector("mesh1.admin.2".parse().unwrap(), topology.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        }
        let t = topology.read().await.clone();
        assert!(t.births().all(|n| n.node_id != held.node.node_id), "a birth whose row cannot be read is not listed: {:?}", t.nodes.iter().map(|n| n.name.to_string()).collect::<Vec<_>>());
        let spans = capture.named("rdm.node_admin.topology.reject.via-row-unreadable");
        assert_eq!(spans.len(), 1, "{spans:?}");
        let a = &spans[0].attributes;
        let attr = |k: &str| a.get(k).cloned().unwrap_or_default();
        assert_eq!((attr("node"), attr("subject"), attr("index"), attr("node_id")), ("mesh1.admin.2".to_string(), "mesh1.rpc.1".to_string(), "id".to_string(), held.node.node_id.to_string()), "{a:?}");
        assert!(attr("detail").contains(&file.display().to_string()), "the store's cause names the file: {a:?}");
        eprintln!("span rdm.node_admin.topology.reject.via-row-unreadable trace_id={} {a:?}", spans[0].trace_id);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (states.md "Routing"): the view's `routable` is application routing's answer: a
    /// birth is routable only while it is ready-for-traffic and its mesh is ready-for-traffic, as this
    /// admin holds them. A `Pending` birth of a ready mesh, and a ready birth of a mesh this admin
    /// holds `pending` or `leaving` (or holds no status of), are listed and not routable; a ready
    /// birth of a ready mesh is routable; a birth listed only from this admin's launch record is
    /// Pending and not routable. Must NOT happen: a listed status other than
    /// ready-for-traffic, or a mesh not held ready-for-traffic, answering routable.
    #[test]
    fn the_view_routes_a_birth_only_while_it_and_its_mesh_are_ready_for_traffic() {
        use rdm_transport::membership::Frame;
        let book = DigestBook::default();
        let held = |mesh: &str, status: &str| {
            let f = Frame::MeshStatus { mesh: mesh.into(), status: status.into(), publisher: format!("{mesh}.admin.1"), forwarded_by: None, changed_at_rafka_ms: 10 };
            assert!(book.statuses.take(&f, &fabric1()), "{f:?}");
        };
        held("mesh1", "ready-for-traffic");
        held("mesh2", "pending");
        held("mesh3", "leaving");
        let (ready, pending) = (digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh1.rpc.2", MemberStatus::Pending));
        let (of_pending_mesh, of_leaving_mesh, of_unheard_mesh) = (digest("mesh2.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh3.rpc.1", MemberStatus::ReadyForTraffic), digest("mesh4.rpc.1", MemberStatus::ReadyForTraffic));
        for d in [&ready, &pending, &of_pending_mesh, &of_leaving_mesh, &of_unheard_mesh] {
            assert!(book.record(d.clone()));
        }
        // A birth this admin launched that membership has not spoken for yet: listed from its
        // launch record, Pending, nothing realised.
        let records = Records::default();
        let launched: PathName = "mesh1.rpc.3".parse().unwrap();
        records.nodes.lock().unwrap().insert(launched.clone(), crate::model::Node::allocated(launched.clone()));
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        let routable = |d: &MeshDigest| t.node(&d.node.name).unwrap_or_else(|| panic!("{} is listed", d.node.name)).routable;
        let unheard = t.node(&launched).expect("the launch record lists the unheard birth");
        assert_eq!(unheard.status, NodeStatus::Pending);
        assert!(!unheard.routable, "a launched birth membership has not spoken for is Pending: a routing stop");
        assert!(routable(&ready), "a ready-for-traffic birth of a ready-for-traffic mesh is routable");
        assert!(!routable(&pending), "a Pending birth is a routing stop");
        assert!(!routable(&of_pending_mesh), "a birth of a pending mesh is a routing stop");
        assert!(!routable(&of_leaving_mesh), "a birth of a leaving mesh is a routing stop");
        assert!(!routable(&of_unheard_mesh), "a birth of a mesh this admin holds no status of is not held ready-for-traffic");
    }

    /// CONTRACT (gossip.md §6.1: a stopped status by itself and silence are not departure proof;
    /// node-identity-and-state.md §4.1: `Leaving` is a status the routing rule reads; §6: the
    /// observer's inference evicts at the staleness floor): a member the book holds `Leaving` is
    /// listed `Leaving` with the state it declared, whatever the age of its last heartbeat within the
    /// staleness floor: a node declared Leaving that still heartbeats is a member of its mesh until it
    /// is stopped or deleted. Past the floor it is the silent member any silent member is. Must NOT
    /// happen: a `Leaving` member one gossip interval and a millisecond after its heartbeat absent from
    /// the view, which is what makes `declared` unreadable at the authority that applied it.
    #[test]
    fn a_member_held_leaving_is_listed_leaving_with_its_declared_state_until_it_is_silent_past_the_staleness_floor() {
        use rdm_core::status::NodeState;
        let book = DigestBook::default();
        let records = Records::default();
        let d = digest("mesh1.rpc.1", MemberStatus::Leaving);
        let t0 = std::time::Instant::now();
        assert!(book.record_at(d.clone(), t0));
        records.declared.lock().unwrap().nodes.insert((d.node.node_id.clone(), d.node.incarnation.clone()), NodeState::Stopped);
        let listed = |at: std::time::Instant| project_at("fabric1", &fabric1(), ProviderKind::Process, &book, &records, at).node(&d.node.name).cloned();

        let within = listed(t0 + gossip_interval() + Duration::from_millis(1)).expect("a Leaving member one interval after its heartbeat is a member of the view");
        assert_eq!((within.status, within.declared.as_deref(), within.routable, within.parked), (NodeStatus::Leaving, Some("Stopped"), false, false));
        let silent = listed(t0 + book.staleness_floor() + Duration::from_millis(1)).expect("silent past the floor it is listed, not departed");
        assert_eq!((silent.status, silent.declared.as_deref()), (NodeStatus::PendingReconnect, Some("Stopped")), "silent past the staleness floor it is the silent member any silent member is");
    }

    /// CONTRACT (states.md: a mesh is pending -> ready-for-traffic -> leaving -> dead; "Routing": a
    /// mesh Leaving or Dead is a routing stop): the mesh status this admin projects is the state its
    /// mesh row holds once that is past ready-for-traffic. The projection is what the mesh primary
    /// announces on the existing MeshStatus send (`Backbone::announce_statuses`, fed from
    /// `t.meshes`), so a leaving mesh is announced leaving by its own primary. Must NOT happen: a
    /// mesh whose row holds Leaving (or Dead) projected, and so announced, ready-for-traffic.
    #[test]
    fn a_mesh_whose_row_holds_leaving_is_projected_leaving_so_its_primary_announces_leaving() {
        use rdm_core::status::MeshState;
        let book = DigestBook::default();
        let records = Records::default();
        for (mesh, state) in [("mesh1", MeshState::Leaving), ("mesh2", MeshState::Dead), ("mesh3", MeshState::ReadyForTraffic)] {
            assert!(book.record(digest(&format!("{mesh}.admin.1"), MemberStatus::ReadyForTraffic)));
            records.meshes.lock().unwrap().insert(mesh.into(), mesh_id(mesh));
            records.declared.lock().unwrap().meshes.insert(mesh_id(mesh), state);
        }
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        let status = |m: &str| t.meshes.iter().find(|x| x.name == m).unwrap_or_else(|| panic!("{m} is in the view: {:?}", t.meshes)).status;
        assert_eq!(status("mesh1"), ScopeStatus::Leaving, "a mesh whose row holds Leaving is projected leaving, so its primary announces leaving");
        assert_eq!(status("mesh2"), ScopeStatus::Dead, "a mesh whose row holds Dead is projected dead");
        assert_eq!(status("mesh3"), ScopeStatus::ReadyForTraffic, "a mesh whose row holds ready-for-traffic is projected from its members, as before");
    }

    /// CONTRACT (states.md "Routing": a stop is applied by a peer on receipt of the fact): the view
    /// application routing reads is rebuilt at once when a routing fact is taken, never on the
    /// poll. A mesh status taken makes the rebuild due though the poll is an hour away; so does a
    /// birth saying another status, and a lifecycle overlay opening. Must NOT happen: the view
    /// waiting for its poll to apply a fact the book already holds.
    #[tokio::test(start_paused = true)]
    async fn the_view_rebuild_is_due_on_receipt_of_a_routing_fact_not_on_the_poll() {
        use futures_util::FutureExt;
        use rdm_transport::membership::Frame;
        let book = DigestBook::default();
        let rpc = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        assert!(book.record(rpc.clone()));
        let hour = Duration::from_secs(3600);
        let mut facts = book.routing_watch();
        let gate = crate::stop_gate::StopGate::new();

        facts.seen();
        let f = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 };
        assert!(book.statuses.take(&f, &fabric1()));
        assert!(view_rebuild_due(&mut facts, hour, &gate).now_or_never().is_some(), "a mesh status taken makes the rebuild due at once");

        facts.seen();
        assert!(view_rebuild_due(&mut facts, hour, &gate).now_or_never().is_none(), "nothing moved: the rebuild waits for its poll");
        let mut draining = rpc.clone();
        draining.status = MemberStatus::ConnectionsDeleting;
        draining.digest_seq += 1;
        assert!(book.record(draining));
        assert!(view_rebuild_due(&mut facts, hour, &gate).now_or_never().is_some(), "a birth saying Draining makes the rebuild due at once");

        facts.seen();
        let stop = rdm_core::LifecycleOp { build_id: "b1".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into(), node_id: rpc.node.node_id.clone(), incarnation: rpc.node.incarnation.clone(), name: rpc.node.name.clone(), event_at_rafka_ms: 1 };
        assert!(book.stopped(stop));
        assert!(view_rebuild_due(&mut facts, hour, &gate).now_or_never().is_some(), "a NodeStopped overlay makes the rebuild due at once");
    }

    /// CONTRACT (node.stop: a parked admin's tasks are held; states.md "Routing"): the view rebuild
    /// is held at its own wait, never inside the view's lock. While the admin's gate is parked a
    /// routing fact makes no rebuild due; the gate opening does, at once. Must NOT happen: a parked
    /// admin rebuilding its view, or a rebuild held where it waits on the view's lock (a gated task
    /// stops where it stands, and one stopped queued on the lock holds it from every reader: the
    /// parked admin then never serves the start that would open it).
    #[tokio::test(start_paused = true)]
    async fn a_parked_admins_view_rebuild_is_held_at_its_own_wait_and_resumes_when_the_gate_opens() {
        use futures_util::FutureExt;
        use rdm_transport::membership::Frame;
        let book = DigestBook::default();
        let mut facts = book.routing_watch();
        let gate = crate::stop_gate::StopGate::new();
        let hour = Duration::from_secs(3600);
        facts.seen();
        gate.park();
        let f = Frame::MeshStatus { mesh: "mesh1".into(), status: "ready-for-traffic".into(), publisher: "mesh1.admin.1".into(), forwarded_by: None, changed_at_rafka_ms: 10 };
        assert!(book.statuses.take(&f, &fabric1()));
        let due = view_rebuild_due(&mut facts, hour, &gate);
        tokio::pin!(due);
        assert!((&mut due).now_or_never().is_none(), "parked: a routing fact makes no rebuild due");
        gate.resume();
        assert!((&mut due).now_or_never().is_some(), "the gate opening makes the held rebuild due at once");
    }

    /// A nodes store over `inner` that counts the node-row reads made through it, by `path.name` key
    /// and by node id, and how many of them ran while `view`'s write lock was held.
    struct ProbedRows {
        inner: Arc<dyn crate::storage::NodesStorage>,
        view: Arc<RwLock<Topology>>,
        key_reads: std::sync::atomic::AtomicUsize,
        id_reads: std::sync::atomic::AtomicUsize,
        under_write_lock: std::sync::atomic::AtomicUsize,
    }

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

    #[async_trait::async_trait]
    impl crate::storage::NodesStorage for ProbedRows {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.inner.runtimes().await
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::record_store::StorageError> {
            self.inner.runtime(node_id, incarnation).await
        }
        async fn own(&self) -> Result<Option<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.inner.own().await
        }
        async fn put_own(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_own(record).await
        }
        async fn contacts(&self) -> Result<Vec<crate::storage::NodeRecord>, crate::record_store::StorageError> {
            self.inner.contacts().await
        }
        async fn put_contact(&self, record: &crate::storage::NodeRecord) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_contact(record).await
        }
        async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_contact(mesh, node_id).await
        }
        async fn put_node_row(&self, row: &crate::storage::NodeRow) -> Result<Result<(), crate::storage::NameHeld>, crate::record_store::StorageError> {
            self.inner.put_node_row(row).await
        }
        async fn node_row_at(&self, name: &PathName) -> Result<Option<crate::storage::NodeRow>, crate::record_store::StorageError> {
            self.read(&self.key_reads);
            self.inner.node_row_at(name).await
        }
        async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, crate::record_store::StorageError> {
            self.read(&self.id_reads);
            self.inner.node_row_name(mesh, node_id).await
        }
        async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.inner.hold_node_row(name, holder).await
        }
        async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, crate::record_store::StorageError> {
            self.inner.release_node_row(name, holder).await
        }
        async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, crate::record_store::StorageError> {
            self.inner.tagged(label).await
        }
    }

    /// `mesh1.rpc.1`, born `mesh1.rpc.1.new` and stopped there, its row moved twice by its mesh-primary
    /// on the fabric's shared store root (to `mesh1.rpc.1`, then to `mesh1.rpc.1.old`: each move one
    /// suffix toggled, the stopped node told nothing), and another admin's view over the same root,
    /// whose row reads are probed.
    async fn renamed_twice(cell: &str) -> (std::path::PathBuf, MeshDigest, Arc<ProbedRows>, Arc<RwLock<Topology>>, Arc<Records>, DigestBook) {
        let d = crate::record_store::tempdir(cell);
        let root = crate::store_root::StoreRoot::at(&d.join("root"));
        let primary = crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.1"), &root, "mesh1", &IncarnationId::mint()).await.unwrap();
        let book = DigestBook::default();
        let held = digest("mesh1.rpc.1.new", MemberStatus::ReadyForTraffic);
        book.record(held.clone());
        assert!(book.stopped(rdm_core::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "node.stop:mesh1.rpc.1.new".into(),
            node_id: held.node.node_id.clone(),
            incarnation: held.node.incarnation.clone(),
            name: held.node.name.clone(),
            event_at_rafka_ms: 1,
        }));
        let mut record = Node::allocated(held.node.name.clone());
        record.node_id = held.node.node_id.clone();
        record.incarnation_id = Some(held.node.incarnation.clone());
        record.status = NodeStatus::Leaving;
        let row = crate::storage::NodeRow { name: held.node.name.clone(), node_id: held.node.node_id.clone(), status: NodeStatus::Leaving, config: BTreeMap::new(), tags: Default::default(), allocated: None, renamed_from: None, inherited_by: None };
        assert_eq!(crate::storage::NodesStorage::put_node_row(&primary, &row).await.unwrap(), Ok(()));
        let (me, base) = ("mesh1.admin.1".parse::<PathName>().unwrap(), held.node.name.base());
        crate::node_rows::move_row(&primary, &me, &record, BTreeMap::new(), Default::default(), &held.node.name, &base).await.unwrap();
        crate::node_rows::move_row(&primary, &me, &record, BTreeMap::new(), Default::default(), &base, &base.renamed()).await.unwrap();
        let view = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestBook::default(), &Records::default())));
        let probed = Arc::new(ProbedRows {
            inner: Arc::new(crate::storage::FileNodesStorage::open(&d.join("mesh1.admin.2"), &root, "mesh1", &IncarnationId::mint()).await.unwrap()),
            view: view.clone(),
            key_reads: Default::default(),
            id_reads: Default::default(),
            under_write_lock: Default::default(),
        });
        let records = Arc::new(Records::default());
        let _ = records.contacts.set(probed.clone());
        (d, held, probed, view, records, book)
    }

    /// CONTRACT (CLAUDE.md read rule: "id: only a lookup by id that cannot read the store's own key";
    /// "add the index, never the scan"): a parked birth whose row was moved twice while it was stopped
    /// (`mesh1.rpc.1.new` to `mesh1.rpc.1`, then to `mesh1.rpc.1.old`) is listed by another admin's
    /// view at the row's current name, `mesh1.rpc.1.old`, read by the birth's node id through the
    /// node row's id index: one read, no `path.name` key guessed. What must NOT happen: the birth
    /// listed at a name it no longer holds, or its name found by trying the keys a move could reach.
    #[tokio::test]
    async fn a_parked_birth_renamed_twice_by_its_row_is_listed_at_the_rows_current_name_read_by_its_node_id() {
        let (d, held, probed, view, records, book) = renamed_twice("row-name-twice").await;
        view_projector("mesh1.admin.2".parse().unwrap(), view.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        let t = view.read().await.clone();
        let names: Vec<String> = t.nodes.iter().map(|n| n.name.to_string()).collect();
        let now = held.node.name.base().renamed();
        assert!(t.node(&now).is_some_and(|n| n.node_id == held.node.node_id && n.parked), "the parked birth is listed at its row's current name: {names:?}");
        assert!(t.node(&held.node.name).is_none() && t.node(&held.node.name.base()).is_none(), "nothing is listed at a name the row moved from: {names:?}");
        assert_eq!((ProbedRows::count(&probed.key_reads), ProbedRows::count(&probed.id_reads)), (0, 1), "the birth's row is read once, by its node id, and no path.name key is tried");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (no disk read under the view's write lock): the projection reads the rows of the
    /// births that do not publish first, then takes the view's write lock and installs. Every row
    /// read finds the view's lock free. What must NOT happen: a row read made while the view's write
    /// lock is held, which holds every reader of the view (a parked admin's handlers, the router)
    /// behind a disk read.
    #[tokio::test]
    async fn the_projection_reads_its_rows_before_it_takes_the_views_write_lock() {
        let (d, _held, probed, view, records, book) = renamed_twice("row-name-lock").await;
        view_projector("mesh1.admin.2".parse().unwrap(), view.clone(), "fabric1".into(), fabric1(), ProviderKind::Process, book.clone(), records.clone())().await;
        let reads = ProbedRows::count(&probed.key_reads) + ProbedRows::count(&probed.id_reads);
        assert!(reads >= 1, "the projection read the parked birth's row");
        assert_eq!(ProbedRows::count(&probed.under_write_lock), 0, "none of the {reads} row reads ran under the view's write lock");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// CONTRACT (node.stop, a successor admin): an admin that joined after the stop never held the
    /// node's digest, only the stop overlay the mesh topology carries (`Members.in_flight`, read
    /// by `GetTopology`). Its view lists the node `Leaving` and parked from that overlay.
    #[test]
    fn a_successor_admin_lists_a_parked_node_from_the_stop_overlay_it_read() {
        let book = DigestBook::default();
        let stopped = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        let stop = rdm_core::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "node.stop:mesh1.rpc.1".into(),
            node_id: stopped.node.node_id.clone(),
            incarnation: stopped.node.incarnation.clone(),
            name: stopped.node.name.clone(),
            event_at_rafka_ms: 1,
        };
        assert!(book.stopped(stop));
        assert!(book.get(stopped.node.node_id.as_str()).is_none(), "this admin never held the node's digest");
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &Records::default());
        let n = t.node(&stopped.node.name).expect("the stop overlay lists the parked node");
        assert_eq!((n.status, n.parked, n.routable), (NodeStatus::Leaving, true, false));
        assert_eq!(n.node_id, stopped.node.node_id);
        assert_eq!(n.incarnation_id.as_ref(), Some(&stopped.node.incarnation));
    }

    #[test]
    fn a_launch_record_never_revives_a_birth_that_another_admin_retired() {
        // This admin launched mesh1.rpc.1; another admin (the primary) retired
        // it: its last word is Leaving, then silence. Membership speaks for the
        // birth (silent past the floor), so this admin's own launch record must
        // not bring it back as ready.
        let book = DigestBook::default();
        let left = digest("mesh1.rpc.1", MemberStatus::Leaving);
        book.record(left.clone());
        let records = Records::default();
        let mut n = Node::allocated(left.node.name.clone());
        n.incarnation_id = Some(left.node.incarnation.clone());
        n.status = NodeStatus::ReadyForTraffic;
        records.publish(n.clone());
        let past_floor = std::time::Instant::now() + book.staleness_floor() + Duration::from_millis(1);
        let t = project_at("fabric1", &fabric1(), ProviderKind::Process, &book, &records, past_floor);
        assert_eq!(t.node(&left.node.name).map(|n| n.status), Some(NodeStatus::PendingReconnect), "the book speaks for the birth, silent past the floor; the launch record's ready never stands in: {:?}", t.node(&left.node.name));
        // A birth it launched that membership has not spoken for yet is shown.
        let fresh = Records::default();
        let mut pending = Node::allocated("mesh1.rpc.2".parse().unwrap());
        pending.incarnation_id = Some(IncarnationId::mint());
        fresh.publish(pending.clone());
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &fresh);
        assert!(t.node(&pending.name).is_some());
        // A birth it launched whose departure another admin proved and published: the book no
        // longer holds the digest, and the launch record never stands in for it.
        let gone = digest("mesh1.rpc.3", MemberStatus::ReadyForTraffic);
        book.record(gone.clone());
        let launched = Records::default();
        let mut n = Node::allocated(gone.node.name.clone());
        n.node_id = gone.node.node_id.clone();
        n.incarnation_id = Some(gone.node.incarnation.clone());
        n.status = NodeStatus::ReadyForTraffic;
        launched.publish(n);
        assert!(project("fabric1", &fabric1(), ProviderKind::Process, &book, &launched).node(&gone.node.name).is_some());
        book.depart(rdm_core::LifecycleOp {
            build_id: "b1".into(),
            attempt: 1,
            operation: "node.delete:mesh1.rpc.3".into(),
            node_id: gone.node.node_id.clone(),
            incarnation: gone.node.incarnation.clone(),
            name: gone.node.name.clone(),
            event_at_rafka_ms: 1,
        });
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &launched);
        assert!(t.node(&gone.node.name).is_none(), "a departed birth's launch record is not a node: {:?}", t.node(&gone.node.name));
    }

    #[test]
    fn removed_births_leave_the_view_and_a_newer_birth_at_the_path_does_not() {
        let book = DigestBook::default();
        let old = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(old.clone());
        let records = Records::default();
        let mut n = Node::allocated(old.node.name.clone());
        n.incarnation_id = Some(old.node.incarnation.clone());
        records.publish(n);
        records.remove(&old.node.name);
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert!(t.node(&old.node.name).is_none(), "the removed birth is gone");
        let mut newer = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        newer.node.node_id = NodeId::mint();
        book.record(newer.clone());
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.node(&old.node.name).unwrap().incarnation_id.as_ref(), Some(&newer.node.incarnation));
    }

    /// CONTRACT (node-recovery.md, sequence step 3): the recovery's delete removes exactly the exited
    /// birth. A rerun of the recovery after the new node already holds the path removes nothing of the
    /// new node, and the exited birth stays out of the view even when the records never held it.
    /// Must NOT happen: the new node's record removed, or its birth filtered from the view.
    #[test]
    fn a_recovery_removes_exactly_the_exited_birth_and_never_the_new_node_at_its_path() {
        let book = DigestBook::default();
        let old = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(old.clone());
        let mut new = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        new.node.node_id = NodeId::mint();
        book.record(new.clone());
        let records = Records::default();
        let mut n = Node::allocated(new.node.name.clone());
        n.node_id = new.node.node_id.clone();
        n.incarnation_id = Some(new.node.incarnation.clone());
        records.publish(n);
        // The rerun: the new node holds the path, the exited birth is removed again.
        records.remove_birth(&old.node.name, &old.node.node_id, &old.node.incarnation);
        assert_eq!(records.nodes.lock().unwrap().get(&new.node.name).map(|n| n.node_id.clone()), Some(new.node.node_id.clone()), "the new node's record stays");
        let removed = records.removed.lock().unwrap().clone();
        assert!(removed.contains(&(old.node.name.clone(), Some(old.node.incarnation.clone()))), "the exited birth is the one removed: {removed:?}");
        assert!(!removed.contains(&(new.node.name.clone(), Some(new.node.incarnation.clone()))), "the new birth is not removed: {removed:?}");
        let t = project("fabric1", &fabric1(), ProviderKind::Process, &book, &records);
        assert_eq!(t.node(&new.node.name).map(|n| n.node_id.clone()), Some(new.node.node_id.clone()), "the view holds the new node at the path");
    }

    /// CONTRACT: a view installed by a writer that had to wait for the view lock reflects the
    /// records as they are when it holds the lock, so a removal that lands while it waits is not
    /// undone by the older projection.
    #[tokio::test]
    async fn a_projection_that_waits_for_the_view_lock_installs_the_state_at_the_lock() {
        let book = Arc::new(DigestBook::default());
        let old = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(old.clone());
        let records = Arc::new(Records::default());
        let mut n = Node::allocated(old.node.name.clone());
        n.incarnation_id = Some(old.node.incarnation.clone());
        records.publish(n);
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)));
        assert!(topology.read().await.node(&old.node.name).is_some(), "the node is in the view before its removal");
        // Another installer holds the view while this one is asked to install.
        let held = topology.write().await;
        let waiting = {
            let (records, topology, book) = (records.clone(), topology.clone(), book.clone());
            tokio::spawn(async move { records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await })
        };
        tokio::task::yield_now().await;
        records.remove(&old.node.name);
        drop(held);
        waiting.await.unwrap();
        assert!(topology.read().await.node(&old.node.name).is_none(), "the view holds the removed node: a projection taken before the lock was installed after the removal");
    }

    /// A status store whose exit-proof row read, while `latch` is set, takes its answer and then
    /// waits on a latch: it says it is inside the read, then holds until released. While `refuse` is
    /// set the read fails. Every other row is the memory store's.
    struct LatchedExits {
        inner: crate::status_storage::MemoryStatusStorage,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        latch: std::sync::atomic::AtomicBool,
        refuse: std::sync::atomic::AtomicBool,
    }

    impl LatchedExits {
        fn new(latch: bool) -> Self {
            Self { inner: Default::default(), entered: tokio::sync::Notify::new(), release: tokio::sync::Notify::new(), latch: latch.into(), refuse: false.into() }
        }
    }

    #[async_trait::async_trait]
    impl crate::status_storage::StatusStorage for LatchedExits {
        async fn put_mesh_status(&self, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status(row).await
        }
        async fn mesh_statuses(&self) -> Result<HashMap<MeshId, rdm_core::status::MeshState>, crate::record_store::StorageError> {
            self.inner.mesh_statuses().await
        }
        async fn put_fabric_event(&self, row: &crate::status_storage::FabricEventRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_fabric_event(row).await
        }
        async fn fabric_events(&self) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events().await
        }
        async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events_of(mesh).await
        }
        async fn put_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::record_store::StorageError> { self.inner.put_node_operation(row).await }
        async fn node_operations(&self) -> Result<Vec<crate::status_storage::NodeOperationRow>, crate::record_store::StorageError> { self.inner.node_operations().await }
        async fn remove_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::record_store::StorageError> { self.inner.remove_node_operation(row).await }

        async fn put_node_declared(&self, row: &crate::status_storage::NodeDeclaredRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_declared(row).await
        }
        async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_node_declared(node_id, incarnation_id).await
        }
        async fn node_declared(&self) -> Result<Vec<crate::status_storage::NodeDeclaredRow>, crate::record_store::StorageError> {
            self.inner.node_declared().await
        }
        async fn put_node_silence(&self, row: &crate::status_storage::NodeSilenceRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_silence(row).await
        }
        async fn node_silences(&self) -> Result<Vec<crate::status_storage::NodeSilenceRow>, crate::record_store::StorageError> {
            self.inner.node_silences().await
        }
        async fn put_node_exit(&self, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_exit(row).await
        }
        async fn node_exit(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.inner.node_exit(node_id, incarnation_id).await
        }
        async fn mesh_status_held_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<bool, crate::record_store::StorageError> {
            self.inner.mesh_status_held_of(mesh, row).await
        }
        async fn put_mesh_status_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status_of(mesh, row).await
        }
        async fn node_exit_of(&self, mesh: &str, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            let answer = self.inner.node_exit_of(mesh, node_id, incarnation_id).await;
            if self.latch.load(std::sync::atomic::Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            if self.refuse.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(crate::record_store::StorageError::Io { file: format!("status/{mesh}/exits"), reason: "the cell refuses the read".into() });
            }
            answer
        }
        async fn put_node_exit_of(&self, mesh: &str, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_exit_of(mesh, row).await
        }
    }

    /// CONTRACT (s41 land 4: no disk read under the view's write lock): an install reads the
    /// exit-proof rows of the births it projects before it takes the view's write lock, so a reader of
    /// the view (a handler, the status authority) is served while an exit-row read is slow, and the
    /// projection installed under the lock holds what the read found. Must NOT happen: the view's
    /// write lock held across a point read of the store.
    #[tokio::test]
    async fn a_reader_of_the_view_is_served_while_an_installs_exit_row_read_is_slow() {
        use crate::status_storage::StatusStorage as _;
        let book = Arc::new(DigestBook::default());
        let lost = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(lost.clone());
        let store = Arc::new(LatchedExits::new(true));
        let row = crate::status_storage::NodeExitRow { node_id: lost.node.node_id.clone(), name: lost.node.name.clone(), incarnation_id: lost.node.incarnation.clone(), exit_code: Some(9), proof: "provider".into(), observed_at_ms: 1 };
        store.inner.put_node_exit_of("mesh1", &row).await.unwrap();
        let records = Arc::new(Records::default());
        records.set_status(store.clone(), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
        records.set_book((*book).clone());
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &DigestBook::default(), &Records::default())));
        let capture = crate::span_capture::Capture::new("i143-3068-exit-rows-read-before-the-view-lock");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let install = {
            let (records, topology, book) = (records.clone(), topology.clone(), book.clone());
            tokio::spawn(async move { records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await })
        };
        store.entered.notified().await;
        let served = topology.try_read().map(|t| t.nodes.len());
        store.release.notify_one();
        let installed = install.await.unwrap();
        assert!(served.is_ok(), "the view's write lock is held while the install waits on an exit-proof row read");
        let listed = installed.node(&lost.node.name).map(|n| n.status);
        assert_eq!(listed, Some(NodeStatus::Dead), "the projection installed under the lock holds the exit the read found");
        assert_eq!(topology.read().await.node(&lost.node.name).map(|n| n.status), Some(NodeStatus::Dead));
        let read = capture.named("rdm.node_admin.node.resolve.via-exit-proof-row");
        assert_eq!(read.len(), 1, "the read phase reads the birth's exit-proof row once: {read:?}");
        eprintln!("span rdm.node_admin.node.resolve.via-exit-proof-row trace_id={} {:?}", read[0].trace_id, read[0].attributes);
    }

    /// CONTRACT (s41 land 4; node-recovery.md: "Exited is the proof, recorded durably"; the status
    /// store puts and point-reads an exit-proof row and nothing removes it): with the reads outside
    /// the view's lock two installs interleave, and a proof once read stays held while its birth is
    /// named: an install whose read was taken before the row was put, and stored after a later
    /// install read the row, takes nothing away; nor does a read that fails. Must NOT happen: a
    /// proven-exited birth read live again (and so seated again) by a stale or failed read.
    #[tokio::test]
    async fn an_exit_proof_once_read_is_never_withdrawn_by_an_older_or_failed_read() {
        use crate::status_storage::StatusStorage as _;
        use std::sync::atomic::Ordering::SeqCst;
        let book = Arc::new(DigestBook::default());
        let lost = digest("mesh1.rpc.1", MemberStatus::ReadyForTraffic);
        book.record(lost.clone());
        let store = Arc::new(LatchedExits::new(true));
        let records = Arc::new(Records::default());
        records.set_status(store.clone(), "mesh1".into(), "mesh1.admin.1".parse().unwrap(), NodeId::mint());
        records.set_book((*book).clone());
        let topology = Arc::new(RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)));
        let status = |t: &Topology| t.node(&lost.node.name).map(|n| n.status);
        let capture = crate::span_capture::Capture::new("i143-3068-exit-proof-never-withdrawn");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        // The older install: its read finds no row, and it is held before it stores.
        let older = {
            let (records, topology, book) = (records.clone(), topology.clone(), book.clone());
            tokio::spawn(async move { records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await })
        };
        store.entered.notified().await;
        store.latch.store(false, SeqCst);
        // The exit is proven and its row put; a later install reads it.
        let row = crate::status_storage::NodeExitRow { node_id: lost.node.node_id.clone(), name: lost.node.name.clone(), incarnation_id: lost.node.incarnation.clone(), exit_code: Some(9), proof: "provider".into(), observed_at_ms: 1 };
        store.inner.put_node_exit_of("mesh1", &row).await.unwrap();
        let later = records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(status(&later), Some(NodeStatus::Dead), "the later install reads the proof");
        store.release.notify_one();
        let after_older = older.await.unwrap();
        assert_eq!(status(&after_older), Some(NodeStatus::Dead), "the older read, stored after the later one, withdrew the proof");
        // A read that fails proves nothing new and takes nothing away.
        store.refuse.store(true, SeqCst);
        let failed = records.install_projected(&topology, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(status(&failed), Some(NodeStatus::Dead), "a failed read withdrew the proof");
        assert_eq!(status(&*topology.read().await), Some(NodeStatus::Dead));
        let (read, unreadable) = (capture.named("rdm.node_admin.node.resolve.via-exit-proof-row"), capture.named("rdm.node_admin.node.reject.via-exit-row-unreadable"));
        assert_eq!((read.len(), unreadable.len()), (1, 1), "the proof enters once, and the failed read is named once: {read:?} {unreadable:?}");
        eprintln!("span rdm.node_admin.node.resolve.via-exit-proof-row trace_id={} {:?}", read[0].trace_id, read[0].attributes);
        eprintln!("span rdm.node_admin.node.reject.via-exit-row-unreadable trace_id={} {:?}", unreadable[0].trace_id, unreadable[0].attributes);
    }

    /// CONTRACT (node-recovery.md: "Exited is the proof, recorded durably"; the projection reads
    /// every birth's exit-proof row): a birth this admin's book never held, listed from the record
    /// this admin published of it (a mesh leave's member run from the accepted roster, a launch not
    /// yet heard), is listed `Dead` once its exit-proof row is read, as a birth listed from a digest
    /// or a stop is. Must NOT happen: a proven-exited birth listed in the state its record last held.
    #[tokio::test]
    async fn a_birth_listed_from_its_record_alone_is_dead_once_its_exit_proof_is_read() {
        let book = DigestBook::default();
        let records = Records::default();
        records.set_status(Arc::new(crate::status_storage::MemoryStatusStorage::default()), "mesh2".into(), "mesh2.admin.2".parse().unwrap(), NodeId::mint());
        records.set_book(book.clone());
        let mut n = Node::allocated("mesh2.rpc.1".parse().unwrap());
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::Leaving;
        records.publish(n.clone());
        let view = RwLock::new(project("fabric1", &fabric1(), ProviderKind::Process, &book, &records));
        assert_eq!(view.read().await.node(&n.name).map(|v| v.status), Some(NodeStatus::Leaving), "listed from its record");
        records.put_exit(&n, Some(0), "terminal-receipt").await.unwrap();
        let t = records.install_projected(&view, || project("fabric1", &fabric1(), ProviderKind::Process, &book, &records)).await;
        assert_eq!(t.node(&n.name).map(|v| v.status), Some(NodeStatus::Dead), "the proven exit of a birth listed from its record alone");
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
        let env: std::collections::HashMap<String, String> = [("RDM_DATA_DIR", "/tmp/rdm-no-store/mesh1.admin.1")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
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
        let launch = rdm_core::launch::Launch {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            name: "mesh1.admin.2".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            launcher: None,
            seeds: vec![],
            data_dir: "/tmp/rdm-launched/mesh1.admin.2".into(),
            store_dir: STORE_DIR.into(),
            mesh_id: None,
            mesh_issuer: None,
            connections_delete_deadline: Duration::from_millis(1250),
        };
        let launched = launch.to_env();
        assert_eq!(AdminConfig::from_env(|k| launched.get(k).cloned()).unwrap().connections_delete_deadline, Duration::from_millis(1250));
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
}
