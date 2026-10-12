//! The estate's typed handles on the fabric: `Nodes`, `Meshes`, `Fabrics` and `Builds` over Node RPC
//! (the typed-harness ruling: new tests drive the mesh through the typed objects, await reply
//! streams and typed facts, and never read a REST string or sleep-poll for a Build).
//!
//! The handles start at the node-admin the estate asks (`Estate::admin`): reads answer from that
//! admin's view, and a Build call it does not own follows the redirect to the fabric-primary. The
//! control API is the handover that names that admin and its birth, once per control base; every
//! call after it goes over Node RPC (ops-naming step 0; the handover itself is finding 15).

use crate::estate::Estate;
use rafka_mesh_entity::{NodeKind, PathName};
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
            let views = NodeAdminClient::new(self.admin.clone()).nodes().await.unwrap_or_else(|e| panic!("the handover: the admin at {} lists no nodes: {e}", self.admin));
            peers.learn_known(&views);
            let entry = views
                .iter()
                .find(|n| n.admin_api_base.as_deref() == Some(self.admin.as_str()))
                .map(|n| n.name.clone())
                .unwrap_or_else(|| panic!("the handover: no node-admin of {} advertises the control base {}", self.fabric_id, self.admin));
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
        let fabrics = Fabrics::new(self.carrier_at(admin).await);
        let f = tokio::time::timeout(std::time::Duration::from_secs(2), fabrics.get()).await.ok()?.ok()?;
        (f.id.to_string() == self.fabric_id).then_some(f)
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
    /// nodes, every one ready-for-traffic, as one `node.get` per mesh reads it. A Build's shape is
    /// counts, not paths: a shrink retires non-primaries, so which ordinals remain depends on where
    /// the seats are.
    pub async fn settled_views(&self, meshes: &[(&str, u32, u32)]) -> Vec<NodeView> {
        let mut all = Vec::new();
        let mut have: std::collections::BTreeMap<(String, NodeKind), usize> = std::collections::BTreeMap::new();
        for (mesh, _, _) in meshes {
            for n in self.mesh_node_views(mesh).await {
                *have.entry((n.mesh.clone(), n.kind)).or_default() += 1;
                all.push(n);
            }
        }
        let want: std::collections::BTreeMap<(String, NodeKind), usize> =
            meshes.iter().flat_map(|(m, a, r)| [((m.to_string(), NodeKind::NodeAdmin), *a as usize), ((m.to_string(), NodeKind::RpcNode), *r as usize)]).filter(|(_, n)| *n > 0).collect();
        let ready = all.iter().all(|n| n.status == NodeStatus::ReadyForTraffic);
        assert!(have == want && ready, "the view does not hold the shape the Build completed: wanted {want:?}, the admin lists {have:?}: {:#?}", all.iter().map(|n| format!("{}={:?}", n.name, n.status)).collect::<Vec<_>>());
        all
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

    /// Wait for the answering admin's node view to hold a state no stream announces: `check` reads the
    /// `node.get` of every mesh until it yields `Some`. A fact a reply stream carries is awaited on the
    /// stream; this is only for a state the admin learns by gossip or by its own detection (a node
    /// recovered after its runtime died, a peer unheard), where no call is held open to wait on. A wait that
    /// ends names `what` and the view it last read.
    pub async fn until_view<T>(&self, what: &str, within: std::time::Duration, mut check: impl FnMut(&[NodeView]) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let views = self.node_views().await;
            if let Some(v) = check(&views) {
                return v;
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out after {within:?} waiting for: {what}; last view: {:?}", views.iter().map(|n| format!("{}={:?}/{}", n.name, n.status, n.node_id)).collect::<Vec<_>>());
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// [`Self::until_view`] for the one node at `name`: `check` reads what `node.get` of its mesh lists
    /// at the path, `None` while it lists nothing there.
    pub async fn until_node<T>(&self, name: &str, what: &str, within: std::time::Duration, mut check: impl FnMut(Option<&NodeView>) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let view = self.node_view_opt(name).await;
            if let Some(v) = check(view.as_ref()) {
                return v;
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out after {within:?} waiting for: {what}; {name} last read: {:?}", view.map(|n| format!("{:?}/{}", n.status, n.node_id)));
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
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
    pub async fn drain(stream: &mut WorkflowStream) -> Vec<Frame> {
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
        let frames = Self::drain(stream).await;
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
