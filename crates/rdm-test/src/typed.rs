//! The estate's typed handles on the fabric: `Nodes`, `Meshes`, `Fabrics` and `Builds` over Node RPC
//! (the typed-harness ruling: new tests drive the mesh through the typed objects, await reply
//! streams and typed facts, and never read a REST string or sleep-poll for a Build).
//!
//! The handles start at the node-admin the estate asks (`Estate::admin`, a `path.name`): reads answer
//! from that admin's view, and a Build call it does not own follows the redirect to the fabric-primary.
//! The handover reaches that admin once per entry, with no listing read: an admin the estate started
//! printed its own birth (`rdm_client::BIRTH_LINE`), and any other admin is dialled by the
//! birth a view the estate read over Node RPC lists for it.

use crate::estate::Estate;
use rdm_client::{LaunchKind as NodeKind, PathName};
use rdm_client::{
    birth_of_view, Accepted, BuildCarrier, BuildFrame, BuildId, BuildStream, Builds, CallEnd, Change, FabricDesired, FabricView, Fabrics, Frame, MeshDesired, MeshNode, MeshView, Meshes, NodeStatus, NodeView, Nodes, Peers, WorkflowStream,
};
use std::sync::Arc;

/// The handover of one entry admin: the client over Node RPC that reaches the fabric's admins, and the
/// admin it starts at. `stale` is set when a read ended: the next call reads the views first.
pub(crate) struct Wire {
    peers: Arc<Peers>,
    entry: PathName,
    stale: bool,
}

/// A fabric shape of counts a cell asks for: [`Estate::submit`] expands it against the current topology
/// into the complete-topology request (`Change::Reconcile`), every created path on the estate's
/// executable for its kind. The Build wire carries no count form (proposal
/// executable-bindings-at-create.md §3.10(a)): this is the harness's shorthand, never a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildSpec {
    /// The whole fabric reconciled to these meshes: a mesh left out is removed.
    Fabric(FabricDesired),
    /// A new mesh with these counts, beside the meshes the fabric holds.
    CreateMesh(MeshDesired),
}

/// A Build that ran to its `Complete` frame.
#[derive(Debug, Clone)]
pub struct Reconciled {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt the call opened.
    pub attempt: u32,
    /// Every frame the stream carried after `Started`, `Complete` last.
    pub frames: Vec<BuildFrame>,
}

impl Estate {
    /// The carrier of the typed objects: it starts at the node-admin `Estate::admin`. A change of
    /// `Estate::admin` is a new handover.
    pub async fn carrier(&self) -> BuildCarrier {
        let mut wire = self.wire.lock().await;
        if wire.as_ref().is_none_or(|w| w.stale || w.entry != self.admin) {
            // A handover after a read ended (`rehandover`) reads the views first: the entry admin may
            // hold a new birth.
            let (peers, after_an_ending) = match wire.take() {
                Some(w) => (w.peers, w.stale),
                None => {
                    let birth = {
                        let printed = self.printed.lock().unwrap();
                        printed.iter().find(|b| b.name == self.admin).or_else(|| printed.first()).cloned().expect("the handover starts from a printed exact birth")
                    };
                    let peers = Peers::at(&birth).await.unwrap_or_else(|e| panic!("the handover to {} ({}): {e}", birth.name, birth.node_id));
                    (Arc::new(peers), false)
                },
            };
            self.reach_entry(&peers, after_an_ending).await;
            *wire = Some(Wire { peers, entry: self.admin.clone(), stale: false });
        }
        let w = wire.as_ref().expect("the handover was made");
        w.peers.carrier_at(w.entry.clone())
    }

    /// Make the node-admin `Estate::admin` reachable on `peers`. A birth an admin this estate started
    /// printed is learned once, as printed: it is that process's own birth, which its door opens for
    /// (node-rpc-envelope.md, "Node RPC readiness gate": the router answers NotReady until then), so the
    /// handover waits until it answers `fabric.get`. `relearn` reads the views of every node-admin the
    /// estate knows first.
    async fn reach_entry(&self, peers: &Peers, relearn: bool) {
        let printed = self.learn_printed(peers);
        if relearn {
            self.relearn(peers).await;
        }
        if let Some(birth) = printed.iter().find(|birth| birth.name == self.admin) {
            self.await_door(peers, &self.admin, birth).await;
        }
    }

    /// Learn, once, the births the admins this estate started printed. Returns what was learned.
    fn learn_printed(&self, peers: &Peers) -> Vec<MeshNode> {
        let printed = std::mem::take(&mut *self.printed.lock().unwrap());
        for birth in &printed {
            peers.learn_birth(birth).unwrap_or_else(|e| panic!("the handover: {} (node {}) printed a birth that cannot be dialled: {e}", birth.name, birth.node_id));
        }
        printed
    }

    /// Wait until the admin `entry`, which printed `birth`, answers `fabric.get` over Node RPC: its door
    /// opens for its own birth (node-rpc-envelope.md, "Node RPC readiness gate": the router answers
    /// NotReady until then).
    async fn await_door(&self, peers: &Peers, entry: &PathName, birth: &MeshNode) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            let answer = tokio::time::timeout(std::time::Duration::from_secs(2), the_fabric(&Fabrics::new(peers.carrier_at(entry.clone())))).await;
            if matches!(answer, Ok(Ok(_))) {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "the handover: {entry} (node {}, incarnation {}, the birth it printed) does not answer fabric.get over Node RPC within 120 s; last: {answer:?}", birth.node_id, birth.incarnation.as_str());
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// Read the fabric's views ([`views_of`]: `node.get` at the fabric-primary) through the first
    /// node-admin the estate knows of that answers for this Fabric within its bound, and learn what they
    /// list.
    async fn relearn(&self, peers: &Peers) {
        let admins: Vec<PathName> = self.births.lock().unwrap().keys().cloned().collect();
        for admin in admins {
            if let Some(views) = node_views_of(peers, &admin, &self.fabric_id).await {
                self.note(peers, &views);
                return;
            }
        }
    }

    /// Make the nodes of `views` reachable on `peers` and hold the birth each live node-admin of them
    /// advertises.
    fn note(&self, peers: &Peers, views: &[NodeView]) {
        peers.learn_known(views);
        let mut births = self.births.lock().unwrap();
        for n in views.iter().filter(|n| n.kind == NodeKind::NodeAdmin && !matches!(n.status, NodeStatus::Dead | NodeStatus::Deleted)) {
            if let Some(birth) = birth_of_view(n) {
                births.insert(n.name.clone(), birth);
            }
        }
    }

    /// A carrier whose calls start at the node-admin `admin`: its reads answer from that admin's view.
    pub async fn carrier_at(&self, admin: &PathName) -> BuildCarrier {
        self.peers().await.carrier_at(admin.clone())
    }

    /// `fabric.get` at `admin`, only when it answers within 2 s and its view names this estate's
    /// Fabric: an admin that is gone, or does not hold this Fabric, answers `None`.
    pub async fn fabric_view_at(&self, admin: &PathName) -> Option<FabricView> {
        fabric_view_of(self.peers().await.as_ref(), admin, &self.fabric_id).await
    }

    /// `node.get` of every node of every mesh, at `admin` when it is a primary, else at the fabric-primary
    /// its `fabric.get` names ([`views_of`]; query.md §1), `None` when a read does not answer for this
    /// Fabric within 2 s. What one particular node holds is [`Self::held_topology_at`].
    pub async fn node_views_at(&self, admin: &PathName) -> Option<Vec<NodeView>> {
        self.views_at(admin).await.ok()
    }

    /// [`Self::held_topology_at`] as values a cell reads like a view: one `{name, node_id, status}` per birth
    /// the node holds, its status as the node holds it; a birth silent past the staleness floor has left the
    /// node's topology and is not listed (query.md §2: "a node silent past the staleness floor leaves the
    /// view"). Empty when the node does not answer within 2 s.
    pub async fn held_values_at(&self, node: &PathName) -> Vec<serde_json::Value> {
        let Some(held) = self.held_topology_at(node).await else { return Vec::new() };
        held.members.iter().map(|d| serde_json::json!({"name": d.node.name.to_string(), "node_id": d.node.node_id.to_string(), "status": serde_json::to_value(d.status).unwrap_or_default()})).collect()
    }

    /// What the node `node` holds itself, never a primary's atlas (query.md §2: a node queries its own
    /// topology): every birth it holds of every mesh, with the status it holds for each, and the
    /// primaries it records, read over `node.topology.get` (op `0x1E`) of every mesh. A cell asking what
    /// one particular node holds reads this, never `node.get` at it, which a node that is not a primary
    /// forwards to its own mesh-primary (query.md §1; Q243). `None` when the node does not answer within
    /// 2 s.
    pub async fn held_topology_at(&self, node: &PathName) -> Option<HeldTopology> {
        let carrier = self.peers().await.carrier_at(node.clone());
        let (rpc, target, opts) = (rdm_client::NodeRpc::new(carrier.client()), rdm_transport::NodeTarget::CurrentPath(node.clone()), rdm_transport::CallOptions::default());
        let frames = tokio::time::timeout(std::time::Duration::from_secs(2), rpc.topology_get(&target, None, None, &opts)).await.ok()?.ok()?;
        let mut held = HeldTopology::default();
        for frame in frames {
            match frame {
                rdm_core::topology::TopologyReply::Snapshot { digests, .. } => held.members.extend(digests.into_iter().map(rdm_core::MeshDigest::from)),
                rdm_core::topology::TopologyReply::Primaries { kind, primary, gone } => held.primaries.push((kind, primary, gone)),
                _ => {}
            }
        }
        Some(held)
    }

    /// [`Self::node_views_at`], naming the read that ended ([`views_of`]).
    pub async fn views_at(&self, admin: &PathName) -> Result<Vec<NodeView>, ViewsEnd> {
        let peers = self.peers().await;
        let views = match views_of(peers.as_ref(), admin, &self.fabric_id).await {
            Ok(views) => views,
            // A read not answered yet (carried up, query.md §1, or not answered within its 2 s bound) or one
            // that ended: the path may hold a new birth (a node-admin re-created at its path.name, by fabric
            // recovery or a replace), which every known admin's view names, so the read goes to the path's
            // birth once more; still unanswered, it is not answered yet, and a wait on it goes on to its bound.
            Err(ViewsEnd::NotYet(_) | ViewsEnd::Ended(_)) => {
                self.relearn(&peers).await;
                views_of(peers.as_ref(), admin, &self.fabric_id).await?
            }
        };
        // What the view lists is reachable from here on: a node created again at a path.name (a new node
        // id, as a deleted admin's successor is) is dialled as the view names it, never by the id the
        // path held before.
        self.note(&peers, &views);
        Ok(views)
    }

    /// Forget the handover of the entry admin: the next call makes it again, reading the views of every
    /// node-admin the estate knows first. A wait on the view does this when a read ends, because the admin
    /// the estate asks may hold a new birth at its name (a restarted node-admin takes its role back with a
    /// new birth), which a view of a live admin lists.
    pub async fn rehandover(&self) {
        if let Some(w) = self.wire.lock().await.as_mut() {
            w.stale = true;
        }
    }

    /// Point the typed objects at the node-admin `admin`, by name. Its birth is already on `Peers` (an admin
    /// this estate started printed it; any other is listed by a view the estate read), and the entry it
    /// leaves is forgotten: a node-admin that died is never asked again. An admin that printed its birth
    /// just now is waited for until its door opens.
    pub async fn enter(&mut self, admin: &PathName) {
        let peers = self.peers().await;
        let printed = self.learn_printed(&peers);
        match printed.iter().rev().find(|birth| birth.name == *admin) {
            Some(birth) => self.await_door(&peers, admin, birth).await,
            // Its views are read and noted. An admin whose birth the estate already holds needs no answer:
            // after a loss its reads of other meshes are carried up to a fabric-primary that may not answer
            // them yet (query.md §1), and the birth it holds is the one the calls go to.
            None => match views_of(&peers, admin, &self.fabric_id).await {
                Ok(views) => self.note(&peers, &views),
                Err(_) if self.births.lock().unwrap().contains_key(admin) => {}
                Err(end) => panic!("enter: {admin}, whose birth the estate does not hold, does not answer for {} over Node RPC: {end}; admins held: {:?}", self.fabric_id, self.births.lock().unwrap().keys().collect::<Vec<_>>()),
            },
        }
        if self.admin != *admin {
            self.births.lock().unwrap().remove(&self.admin);
        }
        self.admin = admin.clone();
        *self.wire.lock().await = Some(Wire { peers, entry: admin.clone(), stale: false });
    }

    /// Point the typed objects at the fabric-primary the entry admin's `fabric.get` names.
    pub async fn enter_fabric_primary(&mut self) {
        let primary = self.fabric_view().await.fabric_primary.expect("fabric.get names the fabric-primary");
        self.enter(&primary).await;
    }

    /// The admin the typed objects start at.
    pub async fn entry(&self) -> PathName {
        self.carrier().await;
        self.wire.lock().await.as_ref().expect("the handover was made").entry.clone()
    }

    /// Make the nodes of `views` reachable by the typed objects.
    async fn learn(&self, views: &[NodeView]) {
        if let Some(w) = self.wire.lock().await.as_ref() {
            self.note(&w.peers, views);
        }
    }

    /// The `node` objects.
    pub async fn nodes_obj(&self) -> Nodes {
        Nodes::new(self.carrier().await)
    }

    /// The `connections` objects (`node.connection.get { node }`, Q167).
    pub async fn connections_obj(&self) -> rdm_client::Connections {
        rdm_client::Connections::new(self.carrier().await)
    }

    /// The `mesh` objects.
    pub async fn meshes_obj(&self) -> Meshes {
        Meshes::new(self.carrier().await)
    }

    /// The `fabric` objects.
    pub async fn fabrics_obj(&self) -> Fabrics {
        Fabrics::new(self.carrier().await)
    }

    /// The `build` objects.
    pub async fn builds_obj(&self) -> Builds {
        Builds::new(self.carrier().await)
    }

    /// `Fabric.build_id` as the admin the estate asks holds it: the id of the Build whose topology the
    /// fabric accepted, as `fabric.get` (op `0x1B`) answers it; empty while it names none.
    pub async fn accepted_build_id(&self) -> String {
        self.fabric_view().await.build_id.unwrap_or_default()
    }

    /// `fabric.get` at the admin the estate asks, or how the call ended.
    pub async fn try_fabric_view(&self) -> Result<FabricView, CallEnd> {
        the_fabric(&self.fabrics_obj().await).await
    }

    /// `mesh.nodes.get` of every mesh the admin holds, or how a call ended: a wait on the view reads
    /// through this, because an admin that is recovering answers some calls `Indeterminate` and the wait
    /// goes on until its own bound, naming the last ending.
    pub async fn try_node_views(&self) -> Result<Vec<NodeView>, CallEnd> {
        let nodes = self.nodes_obj().await;
        let mut all = Vec::new();
        for mesh in self.try_fabric_view().await?.meshes {
            if mesh.nodes.is_empty() {
                continue;
            }
            let views = mesh_nodes(&nodes, &mesh.name).await?;
            self.learn(&views).await;
            all.extend(views);
        }
        Ok(all)
    }

    /// `fabric.get` at the admin the estate asks.
    pub async fn fabric_view(&self) -> FabricView {
        if let Ok(view) = the_fabric(&self.fabrics_obj().await).await {
            return view;
        }
        // The entry admin's path may hold a new birth: hand over again, reading every known admin's view
        // first, and ask that birth once more.
        self.rehandover().await;
        let entry = self.entry().await;
        the_fabric(&self.fabrics_obj().await).await.unwrap_or_else(|e| panic!("fabric.get at {entry}: {e}"))
    }

    /// `mesh.get [name: mesh]` at the admin the estate asks, `None` when it holds no such mesh.
    pub async fn mesh_view_opt(&self, mesh: &str) -> Option<MeshView> {
        let entry = self.entry().await;
        one_mesh(&self.meshes_obj().await, mesh).await.unwrap_or_else(|e| panic!("mesh.get [name: {mesh}] at {entry}: {e}"))
    }

    /// `mesh.get [name: mesh]` at the admin the estate asks.
    pub async fn mesh_view(&self, mesh: &str) -> MeshView {
        self.mesh_view_opt(mesh).await.unwrap_or_else(|| panic!("the admin holds no mesh {mesh}"))
    }

    /// `node.get [mesh: mesh]` at the admin the estate asks.
    pub async fn mesh_node_views(&self, mesh: &str) -> Vec<NodeView> {
        let entry = self.entry().await;
        let views = mesh_nodes(&self.nodes_obj().await, mesh).await.unwrap_or_else(|e| panic!("node.get [mesh: {mesh}] at {entry}: {e}"));
        self.learn(&views).await;
        views
    }

    /// `node.get [mesh: m]` of every mesh the admin holds. A read the admin carried up and has not
    /// answered yet ([`not_yet`]: no fabric-primary answers it, query.md §1) lists nothing yet, and a wait
    /// on the views goes on to its own bound; any other end of a read panics naming it.
    pub async fn node_views(&self) -> Vec<NodeView> {
        let fabric = match the_fabric(&self.fabrics_obj().await).await {
            Ok(view) => view,
            Err(e) if not_yet(&e) => return Vec::new(),
            // The entry admin's path may hold a new birth: hand over again and ask once more.
            Err(_) => {
                self.rehandover().await;
                let entry = self.entry().await;
                match the_fabric(&self.fabrics_obj().await).await {
                    Ok(view) => view,
                    Err(e) if not_yet(&e) => return Vec::new(),
                    Err(e) => panic!("fabric.get at {entry}: {e}"),
                }
            }
        };
        let mut all = Vec::new();
        for mesh in fabric.meshes {
            if mesh.nodes.is_empty() {
                continue;
            }
            let entry = self.entry().await;
            match mesh_nodes(&self.nodes_obj().await, &mesh.name).await {
                Ok(views) => {
                    self.learn(&views).await;
                    all.extend(views);
                }
                Err(e) if not_yet(&e) => return Vec::new(),
                Err(e) => panic!("node.get [mesh: {}] at {entry}: {e}", mesh.name),
            }
        }
        all
    }

    /// The node at `name` if the admin lists it now. A deleted birth leaves the view before its
    /// replacement's record appears, so a wait on a path uses this, never [`Self::node_view`].
    pub async fn node_view_opt(&self, name: &str) -> Option<NodeView> {
        let path: PathName = name.parse().unwrap_or_else(|e| panic!("{name} is not a path.name: {e:?}"));
        let entry = self.entry().await;
        // A mesh the admin does not hold is the empty list (Q167 R-RD3).
        let views = mesh_nodes(&self.nodes_obj().await, &path.mesh).await.unwrap_or_else(|e| panic!("node.get [mesh: {}] at {entry}: {e}", path.mesh));
        self.learn(&views).await;
        views.into_iter().find(|n| n.name == path)
    }

    /// The node at `name`.
    pub async fn node_view(&self, name: &str) -> NodeView {
        self.node_view_opt(name).await.unwrap_or_else(|| panic!("no node {name}"))
    }

    /// The view the Build left: every `(mesh, node_admin, rpc_node)` cohort holds exactly its count of
    /// nodes, every one ready, as `mesh.nodes.get` reads it at the admin the estate asks. A Build's
    /// shape is counts, not paths: a shrink deletes non-primaries, so which ordinals remain depends on
    /// where the roles are. The Build's terminal frame is the executor's: the answering admin learns the
    /// nodes of another mesh by gossip, so its view converges on the shape a moment after (a wait on the
    /// view, bounded at 60 s, named when it does not settle).
    pub async fn settled_views(&self, meshes: &[(&str, u32, u32)]) -> Vec<NodeView> {
        self.settles_to(meshes, std::time::Duration::from_secs(60)).await
    }

    /// Wait until the fabric the admin holds is ready: the fabric changes its states (sync, commit,
    /// open traffic) after the nodes of a Build are ready, and writes its record at the commit. No stream
    /// announces it; `fabric.get` carries the state. The wait is where the estate records that its fabric
    /// has started: from then on a kill or terminate of one of its nodes is a test
    /// ([`crate::estate::FabricReady`], Q74). A read that happens to see the state does not record it, so
    /// whether a cell may kill never depends on when its last read landed.
    pub async fn until_fabric_ready(&self, within: std::time::Duration) {
        crate::estate::wait_for("the fabric is ready", within, || async { self.try_fabric_view().await.is_ok_and(|f| f.status == rdm_client::ScopeStatus::Ready).then_some(()) }).await;
        let _ = self.ready.set(crate::estate::FabricReady(()));
    }

    /// [`Self::until_fabric_ready`] read where the fabric's state is decided: `fabric.get` answered by
    /// `fabric_primary`, the admin whose view names itself the fabric-primary, ready. Another
    /// admin's answer carries the FabricStatus it heard by gossip, which can lag the fabric-primary's own
    /// state. The wait records that the fabric has started, as [`Self::until_fabric_ready`] does.
    pub async fn until_fabric_ready_at(&self, fabric_primary: &PathName, within: std::time::Duration) {
        crate::estate::wait_for(&format!("{fabric_primary}, the fabric-primary, answers fabric.get ready"), within, || async {
            self.fabric_view_at(fabric_primary).await.filter(|f| f.status == rdm_client::ScopeStatus::Ready && f.fabric_primary.as_ref() == Some(fabric_primary)).map(|_| ())
        })
        .await;
        let _ = self.ready.set(crate::estate::FabricReady(()));
    }

    /// The view the Build left, by name: exactly `names`, every one ready, as one `mesh.nodes.get` per
    /// mesh reads it.
    pub async fn settled_named(&self, names: &std::collections::BTreeSet<String>) -> Vec<NodeView> {
        let meshes: std::collections::BTreeSet<String> = names.iter().map(|n| n.split('.').next().unwrap_or_default().to_string()).collect();
        let mut all = Vec::new();
        for mesh in meshes {
            all.extend(self.mesh_node_views(&mesh).await);
        }
        let have: std::collections::BTreeSet<String> = all.iter().map(|n| n.name.to_string()).collect();
        assert!(&have == names && all.iter().all(|n| n.status == NodeStatus::Ready), "the view does not hold the nodes the Build completed: wanted {names:?}, the admin lists {:?}", all.iter().map(|n| format!("{}={:?}", n.name, n.status)).collect::<Vec<_>>());
        all
    }

    /// The view once it holds exactly `names`, every one ready, as `mesh.nodes.get` reads it at the
    /// admin the estate asks. The answering admin learns a node another admin created by gossip, so its
    /// view converges a moment after the Build's terminal frame (a wait on the view, bounded by `within`,
    /// naming the last view when it does not settle).
    pub async fn settles_to_named(&self, names: &std::collections::BTreeSet<String>, within: std::time::Duration) -> Vec<NodeView> {
        self.until_view(&format!("the view holds exactly {names:?}, every node ready"), within, |views| {
            let have: std::collections::BTreeSet<String> = views.iter().map(|n| n.name.to_string()).collect();
            (&have == names && views.iter().all(|n| n.status == NodeStatus::Ready)).then(|| views.to_vec())
        })
        .await
    }

    /// `views` as the JSON each serializes to: the shape the cells record.
    pub fn values(views: &[NodeView]) -> Vec<serde_json::Value> {
        views.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect()
    }

    /// The view once the fabric has recovered to `meshes` after a fault: every `(mesh, node_admin, rpc_node)`
    /// cohort holds exactly its count of nodes, every one ready. No stream announces it: the
    /// recovery is the rectifier's (a later attempt of the accepted Build, or a person's admin on a former
    /// role's data dir), where no call is held open. `within` bounds the wait and a view that does not settle
    /// is named.
    pub async fn settles_to(&self, meshes: &[(&str, u32, u32)], within: std::time::Duration) -> Vec<NodeView> {
        let want: std::collections::BTreeMap<(String, NodeKind), usize> =
            meshes.iter().flat_map(|(m, a, r)| [((m.to_string(), NodeKind::NodeAdmin), *a as usize), ((m.to_string(), NodeKind::RpcNode), *r as usize)]).filter(|(_, n)| *n > 0).collect();
        let named: std::collections::BTreeSet<&str> = meshes.iter().map(|(m, _, _)| *m).collect();
        self.until_view(&format!("the view settles on {want:?}, every node ready"), within, |views| {
            let views: Vec<NodeView> = views.iter().filter(|n| named.contains(n.mesh.as_str())).cloned().collect();
            let mut have: std::collections::BTreeMap<(String, NodeKind), usize> = std::collections::BTreeMap::new();
            for n in &views {
                *have.entry((n.mesh.clone(), n.kind)).or_default() += 1;
            }
            (have == want && views.iter().all(|n| n.status == NodeStatus::Ready)).then_some(views)
        })
        .await
    }

    /// The view once every `(mesh, kind)` cohort of the fabric holds exactly its count of nodes in `want`, every
    /// one ready: the view of the whole fabric, every mesh and kind named, as `mesh.nodes.get` reads it at
    /// the admin the estate asks. `within` bounds the wait and a view that does not settle is named.
    pub async fn settles_to_counts(&self, want: std::collections::BTreeMap<(String, NodeKind), usize>, within: std::time::Duration) -> Vec<NodeView> {
        self.until_view(&format!("the view settles on {want:?}, every node ready"), within, |views| {
            let mut have: std::collections::BTreeMap<(String, NodeKind), usize> = std::collections::BTreeMap::new();
            for n in views {
                *have.entry((n.mesh.clone(), n.kind)).or_default() += 1;
            }
            (have == want && views.iter().all(|n| n.status == NodeStatus::Ready)).then(|| views.to_vec())
        })
        .await
    }

    /// Wait until the node `witness` holds the exact birth the admin's view lists for `node` (same node id,
    /// same incarnation): a node learns a peer's birth by gossip, not from the admin's settled view, so
    /// a call through a witness is made only once the witness's own held view of the mesh holds the birth
    /// it will call. The witness reports it (`node.topology.get`) and a witness that never does is named.
    pub async fn witness_holds_birth(&self, witness: &str, node: &str) {
        let (witness_view, birth) = (self.node_view(witness).await, self.node_view(node).await);
        let incarnation = birth.incarnation_id.clone().unwrap_or_else(|| panic!("{node} has no birth to hold: {birth:?}"));
        self.peers().await.fact(&witness_view, &birth.mesh, &format!("{witness} holds incarnation {incarnation:?} of {node}"), |v| v.birth(&birth.node_id).is_some_and(|d| d.node.incarnation == incarnation)).await;
    }

    /// Wait until the node `witness` retains the departure of `node_id` in its held view of `mesh`
    /// (`node.topology.get`): a node learns a departure by gossip or from the retained mesh topology, never
    /// from a reply stream it holds, so the witness reports it and one that never does is named.
    pub async fn holds_departure(&self, witness: &str, mesh: &str, node_id: &str) {
        let witness_view = self.node_view(witness).await;
        self.peers().await.fact(&witness_view, mesh, &format!("{witness} retains the departure of {node_id}"), |v| v.deleted.iter().any(|d| d.node_id.as_str() == node_id)).await;
    }

    /// Wait for the answering admin's node view to hold a state no stream announces: `check` reads the
    /// `mesh.nodes.get` of every mesh until it yields `Some`. A fact a reply stream carries is awaited on the
    /// stream; this is only for a state the admin learns by gossip or by its own detection (a node
    /// recovered after its runtime died, a peer unheard), where no call is held open to wait on. A wait that
    /// ends names `what` and the view it last read.
    pub async fn until_view<T>(&self, what: &str, within: std::time::Duration, mut check: impl FnMut(&[NodeView]) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let last = match self.try_node_views().await {
                Ok(views) => {
                    if let Some(v) = check(&views) {
                        return v;
                    }
                    format!("{:?}", views.iter().map(|n| format!("{}={:?}/{}", n.name, n.status, n.node_id)).collect::<Vec<_>>())
                }
                Err(e) => {
                    self.rehandover().await;
                    format!("the read ended: {e}")
                }
            };
            assert!(tokio::time::Instant::now() < deadline, "timed out after {within:?} waiting for: {what}; last view: {last}");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// [`Self::until_view`] at the node-admin `admin`: `check` reads what `mesh.nodes.get` of every mesh that admin
    /// holds lists, as that admin's own view holds it, until it yields `Some`. An admin that does not answer
    /// for this Fabric within 2 s is a read that ended, and the wait goes on to its own bound.
    pub async fn until_view_at<T>(&self, admin: &PathName, what: &str, within: std::time::Duration, mut check: impl FnMut(&[NodeView]) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let last = match self.node_views_at(admin).await {
                Some(views) => {
                    if let Some(v) = check(&views) {
                        return v;
                    }
                    format!("{:?}", views.iter().map(|n| format!("{}={:?}/{}", n.name, n.status, n.node_id)).collect::<Vec<_>>())
                }
                None => format!("{admin} did not answer mesh.nodes.get for {}", self.fabric_id),
            };
            assert!(tokio::time::Instant::now() < deadline, "timed out after {within:?} waiting for: {what} (as {admin} holds its view); last view: {last}");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// [`Self::until_view`] for the one node at `name`: `check` reads what `mesh.nodes.get` of its mesh lists
    /// at the path, `None` while it lists nothing there.
    pub async fn until_node<T>(&self, name: &str, what: &str, within: std::time::Duration, mut check: impl FnMut(Option<&NodeView>) -> Option<T>) -> T {
        let path: PathName = name.parse().unwrap_or_else(|e| panic!("{name} is not a path.name: {e:?}"));
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let last = match self.nodes_obj().await.get(&rdm_client::Filter::new().eq("mesh", [&path.mesh])).await {
                Ok(views) => {
                    self.learn(&views).await;
                    let view = views.into_iter().find(|n| n.name == path);
                    if let Some(v) = check(view.as_ref()) {
                        return v;
                    }
                    format!("{:?}", view.map(|n| format!("{:?}/{}", n.status, n.node_id)))
                }
                Err(e) => {
                    self.rehandover().await;
                    format!("the read ended: {e}")
                }
            };
            assert!(tokio::time::Instant::now() < deadline, "timed out after {within:?} waiting for: {what}; {name} last read: {last}");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// [`Self::node_views`] as the JSON each view serializes to: the shape the cells record.
    pub async fn node_values(&self) -> Vec<serde_json::Value> {
        self.node_views().await.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect()
    }

    /// [`Self::node_views_at`] of the admin named `admin`, as JSON; an admin that does not answer for this
    /// Fabric is named.
    /// A read the admin carried up and has not answered yet ([`not_yet`]) lists nothing yet: a wait on
    /// it goes on to its own bound.
    pub async fn node_values_at(&self, admin: &str) -> Vec<serde_json::Value> {
        let path: PathName = admin.parse().unwrap_or_else(|e| panic!("{admin} is not a path.name: {e:?}"));
        let views = match self.views_at(&path).await {
            Ok(views) => views,
            Err(ViewsEnd::NotYet(_)) => Vec::new(),
            Err(ViewsEnd::Ended(why)) => panic!("{admin} does not answer for {}: {why}", self.fabric_id),
        };
        views.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect()
    }

    /// [`Self::node_view_opt`] as the JSON the view serializes to.
    pub async fn node_value_opt(&self, name: &str) -> Option<serde_json::Value> {
        self.node_view_opt(name).await.map(|n| serde_json::to_value(n).expect("a view serializes"))
    }

    /// [`Self::node_view`] as the JSON the view serializes to.
    pub async fn node_value(&self, name: &str) -> serde_json::Value {
        serde_json::to_value(self.node_view(name).await).expect("a view serializes")
    }

    /// The data dir of `node`. Only the admin that launched a node advertises it, and that is whichever
    /// admin executed its creation, so the entry admin's view is read first and then every other
    /// ready node-admin's, each as that admin's own `mesh.nodes.get`.
    pub async fn data_dir(&self, node: &str) -> String {
        self.advertised_data_dir(node).await.unwrap_or_else(|| panic!("no live admin advertises {node}'s data dir"))
    }

    /// [`Self::data_dir`], or `None` when no live admin advertises it.
    pub async fn advertised_data_dir(&self, node: &str) -> Option<String> {
        let entry = self.entry().await;
        let mut admins = vec![entry.clone()];
        admins.extend(self.node_views().await.into_iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::Ready && n.name != entry).map(|n| n.name));
        for admin in admins {
            let Some(views) = self.node_views_at(&admin).await else { continue };
            if let Some(dir) = views.into_iter().find(|n| n.name.to_string() == node).and_then(|n| n.data_dir) {
                return Some(dir);
            }
        }
        None
    }

    /// The attempt the accepted Build `build_id` has reached, as `build.get` reads it.
    pub async fn attempt_of_build(&self, build_id: &str) -> u64 {
        u64::from(self.carrier().await.build(&BuildId::parse(build_id).unwrap_or_else(|e| panic!("build.get [id: {build_id}]: {e}"))).await.unwrap_or_else(|e| panic!("build.get [id: {build_id}]: {e}")).attempt)
    }

    /// `build.create` of `spec`, accepted: the stream of the Build that runs it. The harness names every
    /// path ([`crate::executables`]): a shape of counts (`Fabric`, `CreateMesh`) is expanded
    /// against the current topology into the complete topology, each created path on its executable,
    /// and submitted as `Change::Reconcile`. A cell that
    /// does not test a refusal never has a submit refused: the estate's birth followed the Day-0 Build to
    /// its terminal frame, and a cell follows each Build it started (`Estate::follow`) before it submits
    /// the next, so a refusal here, a Build in progress included, is a defect of the cell and panics
    /// naming the spec and the refusal.
    pub async fn submit(&self, spec: &BuildSpec) -> BuildStream {
        let (meshes, whole) = match spec {
            BuildSpec::Fabric(d) => (d.meshes.clone(), true),
            BuildSpec::CreateMesh(m) => (vec![m.clone()], false),
        };
        self.submit_named(&format!("{spec:?}"), |current, executables| current.resized(&meshes, whole, executables)).await
    }

    /// `build.create` of `change` (a change of the accepted Build the carrier carries without a
    /// [`BuildSpec`]: a replace, a mesh removal), accepted: the stream of the Build that runs it. A cell
    /// that does not test a refusal never has a submit refused: the Build it started itself is followed to
    /// its terminal frame (`Estate::follow`) before the next change is submitted, so a refusal here, a
    /// Build in progress included, is a defect of the cell and panics naming the change and the refusal.
    pub async fn submit_change(&self, change: Change) -> BuildStream {
        match self.carrier().await.create(change.clone()).await {
            Ok(stream) => stream,
            Err(e) => panic!("the fabric-primary refused {change:?}: {e}"),
        }
    }

    /// The current topology read (the current Build at the fabric-primary, its accepted topology and
    /// every node's row): what a complete-topology request is built from. A read that fails is a
    /// defect of the cell, named.
    pub async fn current(&self) -> crate::executables::Current {
        crate::executables::Current::read(&self.carrier().await).await.unwrap_or_else(|e| panic!("the current topology could not be read: {e}"))
    }

    /// `build.create { Change(Reconcile) }` of the complete topology `nodes` builds from the current
    /// read, accepted. A request built from a read another Build overtook is refused
    /// `based-on-not-current` and read again (the caller re-reads, proposal executable-bindings-at-create.md
    /// rule 1); any other refusal is a defect of the cell, named with `what`.
    pub async fn submit_named(&self, what: &str, nodes: impl Fn(&crate::executables::Current, &crate::executables::Executables) -> Vec<rdm_core::meta::NodeSpec>) -> BuildStream {
        let mut tries = 0;
        loop {
            let current = self.current().await;
            let change = Change::Reconcile { topology_storage_provider: None, based_on: current.based_on.clone(), nodes: nodes(&current, &self.executables) };
            match self.carrier().await.create(change.clone()).await {
                Ok(stream) => return stream,
                Err(CallEnd::Rejected { reason, .. }) if reason == "based-on-not-current" && tries < 3 => tries += 1,
                Err(e) => panic!("the fabric-primary refused {what} as {change:?}: {e}"),
            }
        }
    }

    /// The complete-topology request (`Change::Reconcile`) of the fabric shape `desired`, read now at the
    /// admin the estate asks, every created path named: for a cell that submits it itself, at an admin it
    /// names, or that asserts a refusal.
    pub async fn shape_change(&self, desired: &FabricDesired) -> Change {
        self.shape_change_at(&self.carrier().await, desired).await
    }

    /// [`Self::shape_change`], read through `carrier`.
    pub async fn shape_change_at(&self, carrier: &BuildCarrier, desired: &FabricDesired) -> Change {
        let current = crate::executables::Current::read(carrier).await.unwrap_or_else(|e| panic!("the current topology could not be read through {}: {e}", carrier.fabric_primary()));
        let nodes = current.resized(&desired.meshes, true, &self.executables);
        Change::Reconcile { topology_storage_provider: None, based_on: current.based_on, nodes }
    }

    /// `node.create` of one more `kind` node in `mesh`, at the lowest free ordinal of its kind, on the
    /// estate's executable for it: the complete topology with the path added
    /// ([`rdm_client::Nodes::create_named`]). Not accepted: how the call ended.
    pub async fn create_node(&self, mesh: &str, kind: NodeKind) -> Result<WorkflowStream, CallEnd> {
        let (based_on, nodes) = self.create_node_request(mesh, kind).await;
        self.nodes_obj().await.create_named(based_on, nodes).await
    }

    /// The `based_on` and complete topology of [`Self::create_node`], for a cell that submits it itself.
    pub async fn create_node_request(&self, mesh: &str, kind: NodeKind) -> (BuildId, Vec<rdm_core::meta::NodeSpec>) {
        let current = self.current().await;
        let nodes = current.with_one_more(mesh, kind, &self.executables);
        (current.based_on, nodes)
    }

    /// Reconcile the fabric to `desired`: `build.create`, awaited to the stream's `Complete`.
    pub async fn reconcile(&self, desired: &FabricDesired) -> Reconciled {
        let stream = self.submit(&BuildSpec::Fabric(desired.clone())).await;
        self.follow(stream).await
    }

    /// A Build stream to its `Complete`, followed by the client's watcher ([`rdm_client::follow_with`]):
    /// a stream that breaks (its connection closed) or ends `FabricPrimaryMoved` (a planned fabric-primary
    /// handover) is reattached by its build id from the attempt it carried, at the role the carrier
    /// reaches now. A failed Build or a reattachment refused is a defect of the cell, named.
    pub async fn follow(&self, stream: BuildStream) -> Reconciled {
        let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
        let followed = rdm_client::follow_with(stream, |id, _from| async move { self.carrier().await.reattach(&id).await })
            .await
            .unwrap_or_else(|e| panic!("Build {build_id} could not be followed to its terminal from attempt {attempt}: {e}"));
        match followed.frames.last() {
            Some(BuildFrame::Complete { .. }) => {}
            Some(BuildFrame::Failed { attempt, operation, step, reason, .. }) => panic!("Build {build_id} attempt {attempt} failed at {operation} {step}: {reason}"),
            other => panic!("Build {build_id} ended at {other:?}, not Complete: {:?}", followed.frames),
        }
        self.refresh().await;
        Reconciled { build_id, attempt, frames: followed.frames }
    }

    /// Reconcile the fabric to `meshes`, each `(name, node_admin, rpc_node)`.
    pub async fn reconcile_shape(&self, fabric: &str, meshes: &[(&str, u32, u32)]) -> Reconciled {
        let stream = self.submit_shape(fabric, meshes).await;
        self.follow(stream).await
    }

    /// `build.create` of the fabric reconciled to `meshes`, each `(name, node_admin, rpc_node)`, accepted:
    /// the stream of the Build, for a cell that acts while the Build runs (its fabric change held, Q154).
    pub async fn submit_shape(&self, fabric: &str, meshes: &[(&str, u32, u32)]) -> BuildStream {
        let desired = FabricDesired { fabric: fabric.into(), meshes: meshes.iter().map(|(m, a, r)| MeshDesired { name: (*m).into(), node_admin: *a, rpc_node: *r, broker: 0, gateway: 0, compute: 0 }).collect() };
        self.submit(&BuildSpec::Fabric(desired)).await
    }

    /// Every frame of a Build stream, to its end; a failed step or a broken stream is a defect of the
    /// cell, named. `Complete` last. A role that moves under the stream is `follow`'s to chase.
    pub async fn until_complete(stream: &mut BuildStream) -> Reconciled {
        match Self::read_to_end(stream).await {
            Ok(done) => done,
            Err(moved) => panic!("the fabric-primary role moved ({:?}) while Build {} ran: follow the stream with Estate::follow", moved.1, stream.build_id()),
        }
    }

    /// The stream's frames to `Complete`, or to the `FabricPrimaryMoved` that ends it (the frames read and the
    /// new role the old one named).
    async fn read_to_end(stream: &mut BuildStream) -> Result<Reconciled, (Vec<BuildFrame>, Option<String>)> {
        let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
        let mut frames = Vec::new();
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap_or_else(|e| panic!("the stream of Build {build_id} attempt {attempt} broke before its terminal frame: {e}"));
            frames.push(frame.clone());
            match frame {
                BuildFrame::Complete { .. } => return Ok(Reconciled { build_id, attempt, frames }),
                BuildFrame::Failed { attempt, operation, step, reason, .. } => panic!("Build {build_id} attempt {attempt} failed at {operation} {step}: {reason}"),
                BuildFrame::FabricPrimaryMoved { fabric_primary } => return Err((frames, fabric_primary)),
                _ => {}
            }
        }
        panic!("the stream of Build {build_id} attempt {attempt} ended without a terminal frame: {frames:?}")
    }

    /// Re-submit Build `build_id` from `from_attempt` and await the stream to the first `Complete`:
    /// the Build goes on at the current fabric-primary, and a later attempt is what converges it.
    pub async fn resume(&self, build_id: &BuildId, from_attempt: u32) -> Reconciled {
        let carrier = self.carrier().await;
        let stream = carrier.resubmit(build_id, from_attempt).await.unwrap_or_else(|e| panic!("the fabric-primary does not stream Build {build_id} from attempt {from_attempt}: {e}"));
        self.follow(stream).await
    }

    /// The node client the typed objects share: what a peer node holds of its mesh, by `Peers::fact`.
    pub async fn peers(&self) -> Arc<Peers> {
        // The client outlives the entry admin: a read at a named admin (`fabric_view_at`, `node_views_at`) is made
        // after the entry died, and only the first call, which has no client yet, needs the handover.
        if let Some(w) = self.wire.lock().await.as_ref() {
            return w.peers.clone();
        }
        self.carrier().await;
        self.wire.lock().await.as_ref().expect("the handover was made").peers.clone()
    }

    /// Every frame of a workflow stream, to its end.
    pub async fn frames_of(stream: &mut WorkflowStream) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Some(f) = stream.next().await {
            frames.push(f.unwrap_or_else(|e| panic!("the workflow stream broke: {e}")));
        }
        frames
    }

    /// A workflow stream to its `Complete`; a failed step is a defect of the cell, named. The attempt
    /// the call opened.
    pub async fn complete(stream: &mut WorkflowStream) -> Accepted {
        let accepted = stream.accepted().clone();
        let frames = Self::frames_of(stream).await;
        match frames.last() {
            Some(Frame::Complete) => accepted,
            other => panic!("Build {} attempt {}: the workflow did not complete: {other:?} of {frames:?}", accepted.build_id, accepted.attempt),
        }
    }

    /// `node.stop` of `node`, then `node.start` of it, each awaited to its `Complete` (Q183: callers run
    /// node.stop then node.start; the same birth, node.id and incarnation, rejoins). The start is asked
    /// once the fabric-primary's own view lists the node stopped: the fabric-primary admits a start of a
    /// stopped node only (node-start.md "Entry"), and a stop executed in another mesh reaches its view by
    /// that mesh's projection. The two attempts the calls opened, the stop's first; a refusal is a
    /// defect of the cell.
    pub async fn stop_then_start(&self, node: &PathName) -> (Accepted, Accepted) {
        let nodes = self.nodes_obj().await;
        let stopped = Self::workflow(&format!("node.stop of {node}"), nodes.stop(node).await).await;
        self.until_stopped_at_the_fabric_primary(node).await;
        let started = Self::workflow(&format!("node.start of {node}"), self.nodes_obj().await.start(node).await).await;
        (stopped, started)
    }

    /// Wait, bounded at 60 s, until the fabric-primary's own view lists `node` stopped: what its
    /// admission of a `node.start` reads (node-start.md "Entry").
    pub async fn until_stopped_at_the_fabric_primary(&self, node: &PathName) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let fp = self.fabric_view().await.fabric_primary;
            let held = match &fp {
                Some(fp) => self.node_views_at(fp).await.is_some_and(|views| views.iter().any(|n| &n.name == node && n.stopped)),
                None => false,
            };
            if held {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "{node}: the fabric-primary {fp:?} did not list the node stopped within 60 s of its node.stop's Complete");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// A workflow call the fabric-primary accepted, awaited to its `Complete`: a refusal is a defect of the
    /// cell and `what` names the call.
    pub async fn workflow(what: &str, call: Result<WorkflowStream, CallEnd>) -> Accepted {
        let mut stream = call.unwrap_or_else(|end| panic!("the fabric-primary refused {what}: {end}"));
        Self::complete(&mut stream).await
    }

    /// `name` as a path.name; a name that is not one is a defect of the cell.
    pub fn path(name: &str) -> PathName {
        name.parse().unwrap_or_else(|e| panic!("{name} is not a path.name: {e:?}"))
    }

    /// Re-learn every node the admin lists, so the typed objects reach births the last Build made.
    async fn refresh(&self) {
        let _ = self.node_views().await;
    }
}

/// Await a fact the caller's own observer records, woken by each arrival it announces on `arrivals`
/// and bounded by `within`: the observer is the typed source of the fact, so nothing sleeps and
/// polls. A fact that never arrives fails naming `what`.
pub async fn until_arrival<T>(what: &str, within: std::time::Duration, arrivals: &tokio::sync::Notify, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let woken = arrivals.notified();
        tokio::pin!(woken);
        woken.as_mut().enable();
        if let Some(v) = check() {
            return v;
        }
        if tokio::time::timeout_at(deadline, woken).await.is_err() {
            if let Some(v) = check() {
                return v;
            }
            panic!("timed out after {within:?} waiting for: {what}");
        }
    }
}

/// `fabric.get` with no pair at the admin `fabrics` asks (Q167): the one fabric it holds.
pub async fn the_fabric(fabrics: &Fabrics) -> Result<FabricView, CallEnd> {
    fabrics.get(&rdm_client::Filter::new()).await?.into_iter().next().ok_or_else(|| CallEnd::Indeterminate { code: rdm_core::outcome::Code::NotFound, reason: "fabric.get answered no fabric".into() })
}

/// `mesh.get [name: mesh]`, or `[id: mesh]` for a canonical id, at the admin `meshes` asks (Q167):
/// the one mesh, `None` when it holds none by that name or id.
pub async fn one_mesh(meshes: &Meshes, mesh: &str) -> Result<Option<MeshView>, CallEnd> {
    let property = if rdm_core::ids::decode_crockford60(mesh).is_some() { "id" } else { "name" };
    Ok(meshes.get(&rdm_client::Filter::new().eq(property, [mesh])).await?.into_iter().next())
}

/// `node.get [mesh: mesh]` at the admin `nodes` asks (Q167): the nodes of one mesh.
pub async fn mesh_nodes(nodes: &Nodes, mesh: &str) -> Result<Vec<NodeView>, CallEnd> {
    nodes.get(&rdm_client::Filter::new().eq("mesh", [mesh])).await
}

/// `fabric.get` at `admin` over `peers`, only when it answers within 2 s and its view names the Fabric
/// `fabric_id`: an admin that is gone, or does not hold that Fabric, answers `None`. A sampler that
/// outlives a borrow of the estate asks through this.
pub async fn fabric_view_of(peers: &Peers, admin: &PathName, fabric_id: &str) -> Option<FabricView> {
    let fabrics = Fabrics::new(peers.carrier_at(admin.clone()));
    let f = tokio::time::timeout(std::time::Duration::from_secs(2), the_fabric(&fabrics)).await.ok()?.ok()?;
    (f.id.to_string() == fabric_id).then_some(f)
}

/// [`views_of`] over `peers`: `node.get` of every mesh at the fabric-primary `admin`'s `fabric.get`
/// names, `None` when a read does not answer for the Fabric `fabric_id` within 2 s a call.
pub async fn node_views_of(peers: &Peers, admin: &PathName, fabric_id: &str) -> Option<Vec<NodeView>> {
    views_of(peers, admin, fabric_id).await.ok()
}

/// Whether a read ended `not-ready` (`UNAVAILABLE`): a read a mesh-primary carried up to a
/// fabric-primary that has not answered it, or that its view names none to carry it to (query.md §1:
/// "A mesh-primary answers a read of its own mesh from its atlas and carries any other read up to the
/// fabric-primary"). The read is not answered yet, and a wait goes on to its own bound.
pub fn not_yet(end: &CallEnd) -> bool {
    matches!(end, CallEnd::Rejected { reason, .. } if reason == "not-ready")
}

/// What one node holds itself ([`Estate::held_topology_at`]; query.md §2): the births it holds and the
/// primaries it records.
#[derive(Debug, Clone, Default)]
pub struct HeldTopology {
    /// Every birth the node holds, of every mesh, with the status it holds for each.
    pub members: Vec<rdm_core::MeshDigest>,
    /// Every primary the node records: which primary, the record, and whether it holds that exact birth
    /// as gone.
    pub primaries: Vec<(rdm_core::topology::PrimaryKind, rdm_core::Primary, bool)>,
}

impl HeldTopology {
    /// The node the node records as `mesh`'s mesh-primary.
    pub fn mesh_primary(&self, mesh: &str) -> Option<&rdm_core::NodeId> {
        self.primaries.iter().find(|(k, p, _)| *k == rdm_core::topology::PrimaryKind::MeshPrimary && p.mesh == mesh).map(|(_, p, _)| &p.node_id)
    }

    /// The node the node records as the fabric-primary.
    pub fn fabric_primary(&self) -> Option<&rdm_core::NodeId> {
        self.primaries.iter().find(|(k, _, _)| *k == rdm_core::topology::PrimaryKind::FabricPrimary).map(|(_, p, _)| &p.node_id)
    }
}

/// How a read of an admin's views ended without them, naming the read that ended.
#[derive(Debug, Clone)]
pub enum ViewsEnd {
    /// The read is not answered yet ([`not_yet`]).
    NotYet(String),
    /// The read ended otherwise: no answer within 2 s, another Fabric, or another refusal.
    Ended(String),
}

impl std::fmt::Display for ViewsEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewsEnd::NotYet(why) | ViewsEnd::Ended(why) => f.write_str(why),
        }
    }
}

/// [`node_views_of`], naming the read that ended: `fabric.get` at `admin` first, then `node.get [mesh: m]`
/// of each mesh the fabric names, at `admin` when that answer names it a primary (its atlas answers, query.md
/// §1), else sent to the fabric-primary the answer names, as a caller outside the node sends it, never to a
/// node-admin that is not a primary, which forwards it to its own mesh-primary (Q243); each bounded at 2 s. A fabric-primary whose node the caller holds no address
/// for (none at its name, or another node id than the answer's) is reached as
/// [`Peers::carrier_following`] reaches one, from `admin`'s node list.
pub async fn views_of(peers: &Peers, admin: &PathName, fabric_id: &str) -> Result<Vec<NodeView>, ViewsEnd> {
    let ended = |read: String, e: CallEnd| if not_yet(&e) { ViewsEnd::NotYet(format!("{read} at {admin} is not answered yet: {e}")) } else { ViewsEnd::Ended(format!("{read} at {admin} ended: {e}")) };
    let fabrics = Fabrics::new(peers.carrier_at(admin.clone()));
    let f = match tokio::time::timeout(std::time::Duration::from_secs(2), the_fabric(&fabrics)).await {
        Err(_) => return Err(ViewsEnd::NotYet(format!("fabric.get at {admin} is not answered within 2 s: a read it carried up (query.md §1) waits on its own bound"))),
        Ok(Err(e)) => return Err(ended("fabric.get".into(), e)),
        Ok(Ok(f)) if f.id.to_string() != fabric_id => return Err(ViewsEnd::Ended(format!("fabric.get at {admin} answered the Fabric {}, not {fabric_id}", f.id))),
        Ok(Ok(f)) => f,
    };
    // The fabric-primary's node id, from its control API base (`rdm://<node id>`): a birth the caller
    // holds at its name under another id is a node that left the path.
    let fp_id = f.admin_api_base.as_deref().and_then(|b| b.strip_prefix("rdm://")).map(str::to_string);
    // A primary answers from its own atlas: the fabric-primary every mesh, a mesh-primary its own mesh,
    // carrying any other read up (query.md §1).
    let primary = f.fabric_primary.as_ref() == Some(admin) || f.meshes.iter().any(|m| m.primary_admin.as_ref() == Some(admin));
    let (at, carrier) = match f.fabric_primary.clone() {
        _ if primary => (admin.clone(), peers.carrier_at(admin.clone())),
        Some(fp) if peers.node_at(&fp).map(|id| id.to_string()) != fp_id => match tokio::time::timeout(std::time::Duration::from_secs(2), peers.carrier_following(admin, &fp)).await {
            Err(_) => return Err(ViewsEnd::NotYet(format!("the birth of {fp}, the fabric-primary, read at {admin} is not answered within 2 s"))),
            Ok(Err(e)) => return Err(ended(format!("the birth of {fp}, the fabric-primary,"), e)),
            Ok(Ok(carrier)) => (fp, carrier),
        },
        Some(fp) => (fp.clone(), peers.carrier_at(fp)),
        // A view naming no fabric-primary (its last mesh deleted): the admin answers what it holds.
        None => (admin.clone(), peers.carrier_at(admin.clone())),
    };
    let nodes = Nodes::new(carrier);
    let mut all = Vec::new();
    for mesh in f.meshes.iter().filter(|m| !m.nodes.is_empty()) {
        let read = format!("node.get [mesh: {}]", mesh.name);
        match tokio::time::timeout(std::time::Duration::from_secs(2), mesh_nodes(&nodes, &mesh.name)).await {
            Err(_) => return Err(ViewsEnd::NotYet(format!("{read} at {at} is not answered within 2 s"))),
            Ok(Err(e)) if not_yet(&e) => return Err(ViewsEnd::NotYet(format!("{read} at {at}, the fabric-primary {admin} names, is not answered yet: {e}"))),
            Ok(Err(e)) => return Err(ViewsEnd::Ended(format!("{read} at {at}, the fabric-primary {admin} names, ended: {e}"))),
            Ok(Ok(views)) => all.extend(views),
        }
    }
    Ok(all)
}
