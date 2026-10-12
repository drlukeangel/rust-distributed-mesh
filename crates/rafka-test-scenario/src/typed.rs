//! The estate's typed handles on the fabric: `Nodes`, `Meshes`, `Fabrics` and `Builds` over Node RPC
//! (the typed-harness ruling: new tests drive the mesh through the typed objects, await reply
//! streams and typed facts, and never read a REST string or sleep-poll for a Build).
//!
//! The handles start at the node-admin the estate asks (`Estate::admin`): reads answer from that
//! admin's view, and a Build call it does not own follows the redirect to the fabric-primary. The
//! control API is the handover that names that admin and its birth, once per control base; every
//! call after it goes over Node RPC (ops-naming step 0; the handover itself is finding 15).

use crate::estate::Estate;
use rafka_node_admin_client::{LaunchKind as NodeKind, PathName};
use rafka_node_admin_client::{
    Accepted, BuildCarrier, BuildFrame, BuildId, BuildSpec, BuildStream, Builds, CallEnd, FabricDesired, FabricView, Fabrics, Frame, MeshDesired, MeshSelector, MeshView, Meshes, NodeAdminClient, NodeSelector, NodeStatus, NodeView, Nodes, Peers, WorkflowStream,
};
use std::sync::Arc;

/// The handover of one control base: the client over Node RPC that reaches the fabric's admins, and
/// the admin the base names.
pub(crate) struct Wire {
    base: String,
    peers: Arc<Peers>,
    entry: PathName,
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
    /// The carrier of the typed objects: it starts at the node-admin `Estate::admin` names. A
    /// change of `Estate::admin` is a new handover.
    pub async fn carrier(&self) -> BuildCarrier {
        let mut wire = self.wire.lock().await;
        if wire.as_ref().is_none_or(|w| w.base != self.admin) {
            let peers = match wire.take() {
                Some(w) => w.peers,
                None => Arc::new(Peers::of(&[]).await),
            };
            // The control base names the admin; the admin answers over Node RPC once its row carries the
            // birth that is running (an admin a test just started on a former seat's data dir lists its
            // old birth until it has rejoined), so the handover reads the listing until the admin it names
            // answers `fabric.get` under the birth the listing gives.
            let admin = NodeAdminClient::new(self.admin.clone());
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
            let entry = loop {
                let views = admin.nodes().await.unwrap_or_else(|e| panic!("the handover: the admin at {} lists no nodes: {e}", self.admin));
                peers.learn_known(&views);
                let entry = views.iter().find(|n| n.admin_api_base.as_deref() == Some(self.admin.as_str())).map(|n| n.name.clone()).unwrap_or_else(|| panic!("the handover: no node-admin of {} advertises the control base {}", self.fabric_id, self.admin));
                let answers = tokio::time::timeout(std::time::Duration::from_secs(2), Fabrics::new(peers.carrier_at(entry.clone())).get()).await;
                if matches!(answers, Ok(Ok(_))) {
                    break entry;
                }
                assert!(tokio::time::Instant::now() < deadline, "the handover: {entry} (the admin at {}) does not answer fabric.get over Node RPC within 120 s; last: {answers:?}", self.admin);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            };
            *wire = Some(Wire { base: self.admin.clone(), peers, entry });
        }
        let w = wire.as_ref().expect("the handover was made");
        w.peers.carrier_at(w.entry.clone())
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

    /// `node.get` of every node of every mesh `admin` holds, as that admin's view holds them, `None`
    /// when it does not answer for this Fabric within 2 s.
    pub async fn node_views_at(&self, admin: &PathName) -> Option<Vec<NodeView>> {
        let carrier = self.carrier_at(admin).await;
        let f = self.fabric_view_at(admin).await?;
        let nodes = Nodes::new(carrier);
        let mut all = Vec::new();
        for mesh in f.meshes.iter().filter(|m| !m.nodes.is_empty()) {
            all.extend(tokio::time::timeout(std::time::Duration::from_secs(2), nodes.get(&NodeSelector::Mesh(mesh.name.clone()))).await.ok()?.ok()?);
        }
        Some(all)
    }

    /// Forget the handover of the control base: the next call reads the admin's listing again. A wait on the
    /// view does this when a read ends, because the admin the estate asks may have been reborn under the
    /// base it advertises (a restarted node-admin takes its seat back with a new birth), and the listing
    /// the control API gives is the only place the estate learns the new one.
    async fn rehandover(&self) {
        if let Some(w) = self.wire.lock().await.as_mut() {
            w.base.clear();
        }
    }

    /// The admin the typed objects start at.
    pub async fn entry(&self) -> PathName {
        self.carrier().await;
        self.wire.lock().await.as_ref().expect("the handover was made").entry.clone()
    }

    /// Make the nodes of `views` reachable by the typed objects.
    async fn learn(&self, views: &[NodeView]) {
        if let Some(w) = self.wire.lock().await.as_ref() {
            w.peers.learn_known(views);
        }
    }

    /// The `node` objects.
    pub async fn nodes_obj(&self) -> Nodes {
        Nodes::new(self.carrier().await)
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
        self.fabrics_obj().await.get().await
    }

    /// `node.get` of every node of every mesh the admin holds, or how a call ended: a wait on the view reads
    /// through this, because an admin that is recovering answers some calls `Indeterminate` and the wait
    /// goes on until its own bound, naming the last ending.
    pub async fn try_node_views(&self) -> Result<Vec<NodeView>, CallEnd> {
        let nodes = self.nodes_obj().await;
        let mut all = Vec::new();
        for mesh in self.try_fabric_view().await?.meshes {
            if mesh.nodes.is_empty() {
                continue;
            }
            let views = nodes.get(&NodeSelector::Mesh(mesh.name.clone())).await?;
            self.learn(&views).await;
            all.extend(views);
        }
        Ok(all)
    }

    /// `fabric.get` at the admin the estate asks.
    pub async fn fabric_view(&self) -> FabricView {
        let entry = self.entry().await;
        self.fabrics_obj().await.get().await.unwrap_or_else(|e| panic!("fabric.get at {entry}: {e}"))
    }

    /// `mesh.get` of `mesh` at the admin the estate asks, `None` when it holds no such mesh.
    pub async fn mesh_view_opt(&self, mesh: &str) -> Option<MeshView> {
        let entry = self.entry().await;
        match self.meshes_obj().await.get(&MeshSelector::Mesh(mesh.into())).await {
            Ok(mut v) => v.pop(),
            Err(CallEnd::Rejected { reason, .. }) if reason == "unknown-mesh" => None,
            Err(e) => panic!("mesh.get {mesh} at {entry}: {e}"),
        }
    }

    /// `mesh.get` of `mesh` at the admin the estate asks.
    pub async fn mesh_view(&self, mesh: &str) -> MeshView {
        self.mesh_view_opt(mesh).await.unwrap_or_else(|| panic!("the admin holds no mesh {mesh}"))
    }

    /// `node.get` of every node of `mesh` at the admin the estate asks.
    pub async fn mesh_node_views(&self, mesh: &str) -> Vec<NodeView> {
        let entry = self.entry().await;
        let views = self.nodes_obj().await.get(&NodeSelector::Mesh(mesh.into())).await.unwrap_or_else(|e| panic!("node.get mesh {mesh} at {entry}: {e}"));
        self.learn(&views).await;
        views
    }

    /// `node.get` of every node of every mesh the admin holds.
    pub async fn node_views(&self) -> Vec<NodeView> {
        let mut all = Vec::new();
        for mesh in self.fabric_view().await.meshes {
            if mesh.nodes.is_empty() {
                continue;
            }
            all.extend(self.mesh_node_views(&mesh.name).await);
        }
        all
    }

    /// The node at `name` if the admin lists it now. A departed birth leaves the view before its
    /// replacement's record appears, so a wait on a path uses this, never [`Self::node_view`].
    pub async fn node_view_opt(&self, name: &str) -> Option<NodeView> {
        let path: PathName = name.parse().unwrap_or_else(|e| panic!("{name} is not a path.name: {e:?}"));
        let entry = self.entry().await;
        let views = match self.nodes_obj().await.get(&NodeSelector::Mesh(path.mesh.clone())).await {
            Ok(v) => v,
            Err(CallEnd::Rejected { reason, .. }) if reason == "unknown-mesh" => return None,
            Err(e) => panic!("node.get mesh {} at {entry}: {e}", path.mesh),
        };
        self.learn(&views).await;
        views.into_iter().find(|n| n.name == path)
    }

    /// The node at `name`.
    pub async fn node_view(&self, name: &str) -> NodeView {
        self.node_view_opt(name).await.unwrap_or_else(|| panic!("no node {name}"))
    }

    /// The view the Build left: every `(mesh, node_admin, rpc_node)` cohort holds exactly its count of
    /// nodes, every one ready-for-traffic, as `node.get` reads it at the admin the estate asks. A Build's
    /// shape is counts, not paths: a shrink retires non-primaries, so which ordinals remain depends on
    /// where the seats are. The Build's terminal frame is the executor's: the answering admin learns the
    /// nodes of another mesh by gossip, so its view converges on the shape a moment after (a wait on the
    /// view, bounded at 60 s, named when it does not settle).
    pub async fn settled_views(&self, meshes: &[(&str, u32, u32)]) -> Vec<NodeView> {
        self.settles_to(meshes, std::time::Duration::from_secs(60)).await
    }

    /// Wait until the fabric the admin holds is ready-for-traffic: the fabric climbs its states (sync, commit,
    /// open traffic) after the nodes of a Build are ready, and writes its record at the commit. No stream
    /// announces it; `fabric.get` carries the state.
    pub async fn until_fabric_ready(&self, within: std::time::Duration) {
        crate::estate::wait_for("the fabric is ready-for-traffic", within, || async { self.try_fabric_view().await.is_ok_and(|f| f.status == rafka_node_admin_client::ScopeStatus::ReadyForTraffic).then_some(()) }).await
    }

    /// The view the Build left, by name: exactly `names`, every one ready-for-traffic, as one `node.get` per
    /// mesh reads it.
    pub async fn settled_named(&self, names: &std::collections::BTreeSet<String>) -> Vec<NodeView> {
        let meshes: std::collections::BTreeSet<String> = names.iter().map(|n| n.split('.').next().unwrap_or_default().to_string()).collect();
        let mut all = Vec::new();
        for mesh in meshes {
            all.extend(self.mesh_node_views(&mesh).await);
        }
        let have: std::collections::BTreeSet<String> = all.iter().map(|n| n.name.to_string()).collect();
        assert!(&have == names && all.iter().all(|n| n.status == NodeStatus::ReadyForTraffic), "the view does not hold the nodes the Build completed: wanted {names:?}, the admin lists {:?}", all.iter().map(|n| format!("{}={:?}", n.name, n.status)).collect::<Vec<_>>());
        all
    }

    /// The view once it holds exactly `names`, every one ready-for-traffic, as `node.get` reads it at the
    /// admin the estate asks. The answering admin learns a node another admin created by gossip, so its
    /// view converges a moment after the Build's terminal frame (a wait on the view, bounded by `within`,
    /// naming the last view when it does not settle).
    pub async fn settles_to_named(&self, names: &std::collections::BTreeSet<String>, within: std::time::Duration) -> Vec<NodeView> {
        self.until_view(&format!("the view holds exactly {names:?}, every node ready-for-traffic"), within, |views| {
            let have: std::collections::BTreeSet<String> = views.iter().map(|n| n.name.to_string()).collect();
            (&have == names && views.iter().all(|n| n.status == NodeStatus::ReadyForTraffic)).then(|| views.to_vec())
        })
        .await
    }

    /// `views` as the JSON each serializes to: the shape the cells record.
    pub fn values(views: &[NodeView]) -> Vec<serde_json::Value> {
        views.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect()
    }

    /// The view once the fabric has recovered to `meshes` after a fault: every `(mesh, node_admin, rpc_node)`
    /// cohort holds exactly its count of nodes, every one ready-for-traffic. No stream announces it: the
    /// recovery is the rectifier's (a later attempt of the accepted Build, or a person's admin on a former
    /// seat's data dir), where no call is held open. `within` bounds the wait and a view that does not settle
    /// is named.
    pub async fn settles_to(&self, meshes: &[(&str, u32, u32)], within: std::time::Duration) -> Vec<NodeView> {
        let want: std::collections::BTreeMap<(String, NodeKind), usize> =
            meshes.iter().flat_map(|(m, a, r)| [((m.to_string(), NodeKind::NodeAdmin), *a as usize), ((m.to_string(), NodeKind::RpcNode), *r as usize)]).filter(|(_, n)| *n > 0).collect();
        let named: std::collections::BTreeSet<&str> = meshes.iter().map(|(m, _, _)| *m).collect();
        self.until_view(&format!("the view settles on {want:?}, every node ready-for-traffic"), within, |views| {
            let views: Vec<NodeView> = views.iter().filter(|n| named.contains(n.mesh.as_str())).cloned().collect();
            let mut have: std::collections::BTreeMap<(String, NodeKind), usize> = std::collections::BTreeMap::new();
            for n in &views {
                *have.entry((n.mesh.clone(), n.kind)).or_default() += 1;
            }
            (have == want && views.iter().all(|n| n.status == NodeStatus::ReadyForTraffic)).then_some(views)
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
    /// (`node.topology.get`): a node learns a departure by gossip or from the retained aggregate, never
    /// from a reply stream it holds, so the witness reports it and one that never does is named.
    pub async fn holds_departure(&self, witness: &str, mesh: &str, node_id: &str) {
        let witness_view = self.node_view(witness).await;
        self.peers().await.fact(&witness_view, mesh, &format!("{witness} retains the departure of {node_id}"), |v| v.departed.iter().any(|d| d.node_id.as_str() == node_id)).await;
    }

    /// Wait for the answering admin's node view to hold a state no stream announces: `check` reads the
    /// `node.get` of every mesh until it yields `Some`. A fact a reply stream carries is awaited on the
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

    /// [`Self::until_view`] at the node-admin `admin`: `check` reads what `node.get` of every mesh that admin
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
                None => format!("{admin} did not answer node.get for {}", self.fabric_id),
            };
            assert!(tokio::time::Instant::now() < deadline, "timed out after {within:?} waiting for: {what} (as {admin} holds its view); last view: {last}");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// [`Self::until_view`] for the one node at `name`: `check` reads what `node.get` of its mesh lists
    /// at the path, `None` while it lists nothing there.
    pub async fn until_node<T>(&self, name: &str, what: &str, within: std::time::Duration, mut check: impl FnMut(Option<&NodeView>) -> Option<T>) -> T {
        let path: PathName = name.parse().unwrap_or_else(|e| panic!("{name} is not a path.name: {e:?}"));
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let last = match self.nodes_obj().await.get(&NodeSelector::Mesh(path.mesh.clone())).await {
                Ok(views) => {
                    self.learn(&views).await;
                    let view = views.into_iter().find(|n| n.name == path);
                    if let Some(v) = check(view.as_ref()) {
                        return v;
                    }
                    format!("{:?}", view.map(|n| format!("{:?}/{}", n.status, n.node_id)))
                }
                Err(CallEnd::Rejected { reason, .. }) if reason == "unknown-mesh" => {
                    if let Some(v) = check(None) {
                        return v;
                    }
                    "the admin holds no such mesh".to_string()
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
    pub async fn node_values_at(&self, admin: &str) -> Vec<serde_json::Value> {
        let path: PathName = admin.parse().unwrap_or_else(|e| panic!("{admin} is not a path.name: {e:?}"));
        let views = self.node_views_at(&path).await.unwrap_or_else(|| panic!("{admin} does not answer node.get for {}", self.fabric_id));
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
    /// ready node-admin's, each as that admin's own `node.get`.
    pub async fn data_dir(&self, node: &str) -> String {
        let entry = self.entry().await;
        let mut admins = vec![entry.clone()];
        admins.extend(self.node_views().await.into_iter().filter(|n| n.kind == NodeKind::NodeAdmin && n.status == NodeStatus::ReadyForTraffic && n.name != entry).map(|n| n.name));
        for admin in admins {
            let Some(views) = self.node_views_at(&admin).await else { continue };
            if let Some(dir) = views.into_iter().find(|n| n.name.to_string() == node).and_then(|n| n.data_dir) {
                return dir;
            }
        }
        panic!("no live admin advertises {node}'s data dir")
    }

    /// The attempt the accepted Build `build_id` has reached, as `build.get` reads it.
    pub async fn attempt_of_build(&self, build_id: &str) -> u64 {
        u64::from(self.builds_obj().await.get(&BuildId(build_id.into())).await.unwrap_or_else(|e| panic!("build.get of {build_id}: {e}")).attempt)
    }

    /// `build.create` of `spec`, accepted: the stream of the Build that runs it. The Build the fabric
    /// holds may still be reconciling (the Day-0 Build, or an earlier call's): its own stream says when
    /// it is done, and `spec` is accepted after it.
    pub async fn submit(&self, spec: &BuildSpec) -> BuildStream {
        match self.try_submit(spec).await {
            Ok(stream) => stream,
            Err(e) => panic!("the fabric-primary refused build.create {spec:?}: {e}"),
        }
    }

    /// [`Self::submit`], the refusal returned by name.
    pub async fn try_submit(&self, spec: &BuildSpec) -> Result<BuildStream, CallEnd> {
        let carrier = self.carrier().await;
        let builds = Builds::new(carrier.clone());
        match builds.create(spec).await {
            Err(CallEnd::BuildInProgress { current }) => {
                let mut running = carrier.resubmit(&current, 1).await.unwrap_or_else(|e| panic!("the fabric-primary does not stream the Build {current} it is reconciling: {e}"));
                Self::until_complete(&mut running).await;
                builds.create(spec).await
            }
            other => other,
        }
    }

    /// Reconcile the fabric to `desired`: `build.create`, awaited to the stream's `Complete`.
    pub async fn reconcile(&self, desired: &FabricDesired) -> Reconciled {
        let stream = self.submit(&BuildSpec::Fabric(desired.clone())).await;
        self.follow(stream).await
    }

    /// A Build stream to its `Complete`. A stream that ends `SeatMoved` (a planned fabric-primary
    /// handover) is followed: the same Build is re-submitted at the seat the carrier reaches now,
    /// from the attempt the stream carried.
    pub async fn follow(&self, mut stream: BuildStream) -> Reconciled {
        let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
        let mut frames = Vec::new();
        loop {
            match Self::read_to_end(&mut stream).await {
                Ok(done) => {
                    frames.extend(done.frames);
                    self.refresh().await;
                    return Reconciled { build_id, attempt, frames };
                }
                Err(moved) => {
                    frames.extend(moved.0);
                    stream = self.carrier().await.resubmit(&build_id, attempt).await.unwrap_or_else(|e| panic!("the fabric-primary seat moved ({:?}) and the new seat does not stream Build {build_id} from attempt {attempt}: {e}", moved.1));
                }
            }
        }
    }

    /// Reconcile the fabric to `meshes`, each `(name, node_admin, rpc_node)`.
    pub async fn reconcile_shape(&self, fabric: &str, meshes: &[(&str, u32, u32)]) -> Reconciled {
        let desired = FabricDesired { fabric: fabric.into(), meshes: meshes.iter().map(|(m, a, r)| MeshDesired { name: (*m).into(), node_admin: *a, rpc_node: *r, broker: 0, gateway: 0, compute: 0 }).collect() };
        self.reconcile(&desired).await
    }

    /// Every frame of a Build stream, to its end; a failed step or a broken stream is a defect of the
    /// cell, named. `Complete` last. A seat that moves under the stream is `follow`'s to chase.
    pub async fn until_complete(stream: &mut BuildStream) -> Reconciled {
        match Self::read_to_end(stream).await {
            Ok(done) => done,
            Err(moved) => panic!("the fabric-primary seat moved ({:?}) while Build {} ran: follow the stream with Estate::follow", moved.1, stream.build_id()),
        }
    }

    /// The stream's frames to `Complete`, or to the `SeatMoved` that ends it (the frames read and the
    /// new seat the old one named).
    async fn read_to_end(stream: &mut BuildStream) -> Result<Reconciled, (Vec<BuildFrame>, Option<String>)> {
        let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
        let mut frames = Vec::new();
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap_or_else(|e| panic!("the stream of Build {build_id} attempt {attempt} broke before its terminal frame: {e}"));
            frames.push(frame.clone());
            match frame {
                BuildFrame::Complete { .. } => return Ok(Reconciled { build_id, attempt, frames }),
                BuildFrame::Failed { attempt, operation, step, reason } => panic!("Build {build_id} attempt {attempt} failed at {operation} {step}: {reason}"),
                BuildFrame::SeatMoved { fabric_primary } => return Err((frames, fabric_primary)),
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

/// `fabric.get` at `admin` over `peers`, only when it answers within 2 s and its view names the Fabric
/// `fabric_id`: an admin that is gone, or does not hold that Fabric, answers `None`. A sampler that
/// outlives a borrow of the estate asks through this.
pub async fn fabric_view_of(peers: &Peers, admin: &PathName, fabric_id: &str) -> Option<FabricView> {
    let fabrics = Fabrics::new(peers.carrier_at(admin.clone()));
    let f = tokio::time::timeout(std::time::Duration::from_secs(2), fabrics.get()).await.ok()?.ok()?;
    (f.id.to_string() == fabric_id).then_some(f)
}
