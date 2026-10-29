//! Incumbency, seat sources, and topology view projection.

use super::*;

/// What an election takes beside the nodes: the seat records this admin holds, and the births gone. A
/// birth is gone when its departure was heard (its node.delete), when a later birth announced that it
/// replaced it (`supersedes`), or when the seats hold it gone. Silence, an unreachable path and the
/// observer-inferred `Dead` are not.
pub(crate) fn incumbency_of(digest_view: &DigestView, _records: &Records) -> crate::election::Incumbency {
    let mut lost: std::collections::HashSet<IncarnationId> = digest_view.deleted().into_iter().map(|op| op.incarnation).collect();
    lost.extend(digest_view.superseded());
    lost.extend(digest_view.seats.gone());
    crate::election::Incumbency { fabric: digest_view.seats.fabric(), meshes: digest_view.seats.meshes(), cohorts: digest_view.seats.node_type_primaries(), lost }
}

/// What this birth's Build log reads its target from: the fabric-primary record `digest_view` holds, the
/// election's result this admin installed, judged by [`crate::seat_watch::fabric_seat_record`] against the births proven gone (the
/// lost set every election takes, [`incumbency_of`]) and named from the digests `digest_view` holds.
///
/// While the fabric-primary is unavailable, the fabric recovery workflow's executor writes that attempt's
/// Build records (build.md §5.3, Q122): for it, the record naming the lost birth is open, naming no
/// holder. `view` is this admin's view once it is built.
pub(crate) fn fabric_seat_source(digest_view: &DigestView, records: &Arc<Records>, me: (NodeId, IncarnationId), me_name: &PathName, view: Arc<std::sync::OnceLock<Arc<tokio::sync::RwLock<crate::topology::Topology>>>>) -> crate::fabric_builds::SeatSource {
    let changed = Some(digest_view.seats.subscribe());
    let (digest_view, records, me_name) = (digest_view.clone(), records.clone(), me_name.to_string());
    let read = Arc::new(move || {
        let incumbency = incumbency_of(&digest_view, &records);
        let held_name = digest_view.held_path_name().map(|p| p.to_string()).unwrap_or_else(|| me_name.clone());
        let executes = view.get().and_then(|v| v.try_read().ok().and_then(|t| crate::fabric_recovery::of(&t, &incumbency, &digest_view))).is_some_and(|fr| fr.executor.as_ref().is_some_and(|e| e.to_string() == held_name) && incumbency.fabric.as_ref() == Some(&fr.target));
        if executes {
            return crate::fabric_builds::SeatRecord::Open { holder: None };
        }
        let name_of = |id: &NodeId| if *id == me.0 { Some(held_name.clone()) } else { digest_view.get(id.as_str()).map(|(d, _)| d.node.name.to_string()) };
        crate::seat_watch::fabric_seat_record(incumbency.fabric.as_ref(), &me, &incumbency.lost, name_of)
    });
    crate::fabric_builds::SeatSource { read, changed }
}

/// Whether this birth reads the Build records now ([`crate::build_state::LocalBuildLog::read_records_while`];
/// Q227: "only FP reads the builds"): its held path.name while the fabric-primary record `digest_view`
/// holds names this exact birth, or while fabric recovery names it the executor that restores the
/// fabric-primary in its place (build.md §5.3, Q122: it writes that attempt's Build records), or, for an
/// admin started with no launcher (`unlaunched`: Day 0, a start again from its own data dir, an adoption
/// launch; Q227's first or re-born fabric-primary), while that record names no birth or a birth proven
/// gone; `None` otherwise, and the admin answers from what reached it. `view` is this admin's view once
/// it is built.
pub(crate) fn build_records_reader(digest_view: &DigestView, records: &Arc<Records>, me: (NodeId, IncarnationId), me_name: &PathName, unlaunched: bool, view: Arc<std::sync::OnceLock<Arc<tokio::sync::RwLock<crate::topology::Topology>>>>) -> crate::build_state::RecordsReader {
    let (digest_view, records, me_name) = (digest_view.clone(), records.clone(), me_name.to_string());
    Arc::new(move || {
        let held_name = digest_view.held_path_name().map(|p| p.to_string()).unwrap_or_else(|| me_name.clone());
        let record = digest_view.seats.fabric();
        if record.as_ref().is_some_and(|h| h.is_birth(&me.0, &me.1)) {
            return Some(held_name);
        }
        let incumbency = incumbency_of(&digest_view, &records);
        if unlaunched && record.as_ref().is_none_or(|h| incumbency.lost.contains(&h.incarnation)) {
            return Some(held_name);
        }
        let executes = view.get().and_then(|v| v.try_read().ok().and_then(|t| crate::fabric_recovery::of(&t, &incumbency, &digest_view))).is_some_and(|fr| fr.executor.as_ref().is_some_and(|e| e.to_string() == held_name) && incumbency.fabric.as_ref() == Some(&fr.target));
        executes.then_some(held_name)
    })
}

/// The seats this admin announces from one decision of its hierarchy loop: each seat the projection
/// `t` gives it whose record names another birth, at the
/// next revision over that record; and, while it is its mesh's primary, each other cohort's seat of its
/// mesh that `t` fills with a birth the cohort's record does not name. The record is the one `t` was decided against (`decided`): a seat
/// record held after the decision (a committed handover landing while the loop awaits) is the next
/// decision's input, and is never superseded by this one (fabric-node-lifecycle-elections.md §4.4).
/// The record this admin already holds for itself needs no announcement.
pub(crate) fn seat_announcements(t: &Topology, mesh: &str, me: (&NodeId, &IncarnationId), decided: &crate::election::Incumbency) -> Vec<(rdm_core::Seat, rdm_core::Primary)> {
    let mesh_primary = t.is_mesh_primary(mesh, me.0);
    let fabric_primary = t.is_fabric_primary(me.0);
    let mut said = Vec::new();
    for (seat, holds) in [(rdm_core::Seat::MeshPrimary, mesh_primary), (rdm_core::Seat::FabricPrimary, fabric_primary)] {
        if !holds {
            continue;
        }
        let held = if seat == rdm_core::Seat::FabricPrimary { decided.fabric.clone() } else { decided.meshes.get(mesh).cloned() };
        if held.as_ref().is_some_and(|h| h.is_birth(me.0, me.1)) {
            continue;
        }
        said.push((seat, rdm_core::Primary { mesh: mesh.to_string(), node_id: me.0.clone(), incarnation: me.1.clone(), revision: held.map(|h| h.revision + 1).unwrap_or(1) }));
    }
    // The mesh-primary is its mesh's one row writer and sole author of member status (R-MT1 Q3): it
    // says each other cohort's seat the election filled with a birth the record does not name.
    if mesh_primary {
        for kind in NodeKind::ALL.into_iter().filter(|k| *k != NodeKind::NodeAdmin) {
            let Some((node_id, incarnation)) = t.node_type_primary(mesh, kind).and_then(|n| Some((n.node_id.clone(), n.incarnation_id.clone()?))) else { continue };
            let held = decided.cohorts.get(&(mesh.to_string(), kind));
            if held.is_some_and(|h| h.is_birth(&node_id, &incarnation)) {
                continue;
            }
            said.push((rdm_core::Seat::Cohort(kind), rdm_core::Primary { mesh: mesh.to_string(), node_id, incarnation, revision: held.map(|h| h.revision + 1).unwrap_or(1) }));
        }
    }
    said
}

/// The observed topology from topology digests and this admin's records,
/// with the settled primary rule (module docs). No node row is read: each birth is listed at the name
/// it published ([`project_with`] names the births that do not publish by their rows).
pub fn project(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, digest_view: &DigestView, records: &Records) -> Topology {
    project_at(fabric, fabric_id, provider, digest_view, records, std::time::Instant::now())
}

/// [`project`] as of the instant `reads` were taken at, from what they read: each birth that does not
/// publish its own name listed at the name its row holds.
pub fn project_with(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, digest_view: &DigestView, records: &Records, reads: &ViewReads) -> Topology {
    project_decided_at(fabric, fabric_id, provider, digest_view, records, &incumbency_of(digest_view, records), reads)
}

/// What a view rebuild reads from the stores, taken in one read phase before the view's write lock
/// ([`Records::install_read`], `view_now`) and handed to the projection under it. Every store read a
/// projection needs is read in [`ViewReads::read`]: the exited-verdict rows, folded into the records
/// (`Records::read_exits`), and the node rows, a field here; nothing is read under the lock.
pub struct ViewReads {
    /// The instant the births were judged at: the projection is taken as of it, so the births it lists
    /// from these reads are exactly the ones the reads covered.
    pub(crate) at: std::time::Instant,
    /// The names the node rows hold for the births that do not publish their own.
    pub(crate) rows: RowNames,
}

impl ViewReads {
    /// Nothing read: every birth is listed at the name it published, as of `at`.
    pub fn none_at(at: std::time::Instant) -> Self {
        Self { at, rows: RowNames::default() }
    }

    /// The read phase of the admin `me`'s rebuild over `digest_view`'s births of `fabric_id`, as of now: first
    /// each birth `digest_view` holds deleted leaves `records` ([`Records::remove_deleted`]), then the row of
    /// every birth that does not publish its own name, from `records`' nodes store (no store: nothing
    /// is read).
    pub async fn read(me: &PathName, digest_view: &DigestView, records: &Records, fabric_id: &FabricId) -> Self {
        let me = digest_view.held_path_name().unwrap_or_else(|| me.clone());
        records.remove_deleted(digest_view);
        let at = std::time::Instant::now();
        Self { at, rows: RowNames::read(&me, digest_view, records, fabric_id, at).await }
    }
}

/// The names the node rows hold for the births a projection lists that do not publish their own
/// (CLAUDE.md read rule: the node-row kind declares no projection, so each is a point read through a
/// store index, read for one projection and held by nothing after it; the caller holds the birth's
/// node id, so it is the row's id index, `NodesStorage::node_row_name`: one read per birth). A birth
/// that publishes names itself: its digest carries its row's name (R12: it publishes only the row's
/// name, a live rename reaching it as `ApplyNodeName`). One that does not (stopped, silent past the
/// staleness floor, held true offline) may have had its row moved while it said
/// nothing: a stopped node's `node.name.update` (R13: it is told nothing and reads the name at its next
/// join), the replace's renames included. Beside the names: which births' rows released their
/// name, read for every birth read here and for each birth that publishes a name another birth the
/// view holds is listed at too (rare: a deleted birth whose deletion this view has not applied), so
/// the projection lists the current birth at a contested path ([`RowNames::takes_path`]).
#[derive(Default)]
pub(crate) struct RowNames {
    /// Each birth read: the name it published, and where its row stands.
    pub(crate) names: HashMap<(NodeId, IncarnationId), (PathName, RowName)>,
    /// Each birth read whose row released its name (`Deleted` or `Withdrawn`,
    /// [`crate::node_rows::released_its_name`]).
    pub(crate) released: HashSet<(NodeId, IncarnationId)>,
}

/// Where the row of one birth stands.
pub(crate) enum RowName {
    /// The name of the row the birth holds (the name it published, when it holds no row).
    At(PathName),
    /// Its id index entry could not be read: the birth's name is not known, and the view does not
    /// list it.
    Unreadable,
}

impl RowNames {
    /// The row of every birth of `fabric_id` in `digest_view` that does not publish its own name as of `at`,
    /// and of every birth that publishes a name another birth is listed at too, read by the admin `me`
    /// from `records`' nodes store (no store: nothing is read).
    pub(crate) async fn read(me: &PathName, digest_view: &DigestView, records: &Records, fabric_id: &FabricId, at: std::time::Instant) -> Self {
        let mut rows = Self::default();
        let Some(store) = records.contacts.get().cloned() else { return rows };
        let mut quiet: Vec<(NodeId, IncarnationId, PathName)> = Vec::new();
        let mut publishing: Vec<(NodeId, IncarnationId, PathName)> = Vec::new();
        for d in digest_view.all() {
            let Some((_, age)) = digest_view.get_at(d.node.node_id.as_str(), at) else { continue };
            if &d.fabric_id != fabric_id {
                continue;
            }
            let birth = (d.node.node_id.clone(), d.node.incarnation.clone(), d.node.name.clone());
            if publishes(digest_view, records, &d.node.node_id, &d.node.incarnation, age) {
                publishing.push(birth);
            } else {
                quiet.push(birth);
            }
        }
        quiet.extend(digest_view.stopped_digests().into_iter().filter(|d| &d.fabric_id == fabric_id).map(|d| (d.node.node_id, d.node.incarnation, d.node.name)));
        quiet.extend(digest_view.stopped_unheard().into_iter().map(|op| (op.node_id, op.incarnation, op.name)));
        // Every name a birth the view holds is listed at, by how many births: a publishing birth's row
        // is read only where another birth is listed at its name too.
        let mut listed: HashMap<PathName, usize> = HashMap::new();
        for (_, _, published) in quiet.iter().chain(publishing.iter()) {
            *listed.entry(published.clone()).or_default() += 1;
        }
        for (d, _) in digest_view.projected_unheard_at(at) {
            if &d.fabric_id == fabric_id {
                *listed.entry(d.node.name.clone()).or_default() += 1;
            }
        }
        for (node_id, incarnation, published) in quiet {
            let (name, released) = row_name(me, &*store, &node_id, &incarnation, &published).await;
            if let RowName::At(moved) = &name {
                if moved != &published {
                    *listed.entry(moved.clone()).or_default() += 1;
                }
            }
            if released {
                rows.released.insert((node_id.clone(), incarnation.clone()));
            }
            rows.names.insert((node_id, incarnation), (published, name));
        }
        // A publishing birth is listed at the name it published whatever its row says; its row is read
        // only for whether it released the name (unreadable: not known to, and the birth keeps today's rule).
        for (node_id, incarnation, published) in publishing {
            if listed.get(&published).copied().unwrap_or(0) < 2 {
                continue;
            }
            if let Ok(Some(row)) = store.node(&node_id).await {
                if row.name.mesh == published.mesh && crate::node_rows::released_its_name(&row) {
                    rows.released.insert((node_id, incarnation));
                }
            }
        }
        rows
    }

    /// Each birth read whose row stands at another name than the one it published:
    /// `(node id, incarnation, published, row's name)`.
    pub(crate) fn moved(&self) -> impl Iterator<Item = (&NodeId, &IncarnationId, &PathName, &PathName)> {
        self.names.iter().filter_map(|((id, inc), (published, row))| match row {
            RowName::At(name) if name != published => Some((id, inc, published, name)),
            _ => None,
        })
    }

    /// The name the projection lists the birth `(node_id, incarnation)` at, which published
    /// `published`: its row's when this read covered it, else the published one. `None`: its row
    /// could not be read, and the birth is not listed.
    pub(crate) fn name_of(&self, node_id: &NodeId, incarnation: &IncarnationId, published: &PathName) -> Option<PathName> {
        match self.names.get(&(node_id.clone(), incarnation.clone())) {
            Some((_, RowName::At(n))) => Some(n.clone()),
            Some((_, RowName::Unreadable)) => None,
            None => Some(published.clone()),
        }
    }

    /// The row this read covered for `n`'s birth released its name (`Deleted` or `Withdrawn`).
    fn released(&self, n: &Node) -> bool {
        n.incarnation_id.as_ref().is_some_and(|inc| self.released.contains(&(n.node_id.clone(), inc.clone())))
    }

    /// Which of two births listed at one path the records say is current, when they say it: the
    /// birth at a path is the one whose row holds the name, stopped or not, and a birth whose row
    /// released the name never takes the path from it (node-identity-and-state.md §2.3: "the deleted
    /// row releases the `path.name`"; §6 "Re-born": "the predecessor is deleted by the recovery Build,
    /// so the projection holds one entry per `path.name`"; node-investigation.md: "A node that comes
    /// back after it was re-created is a deleted birth"). `Some(true)`: `candidate` takes the path
    /// from `listed`; `Some(false)`: `listed` keeps it; `None`: the rows read do not tell them apart.
    pub(crate) fn takes_path(&self, listed: &Node, candidate: &Node) -> Option<bool> {
        match (self.released(listed), self.released(candidate)) {
            (true, false) => Some(true),
            (false, true) => Some(false),
            _ => None,
        }
    }

    /// List `n` at `name` when no birth is listed there yet, or when the birth listed there is not the
    /// current one and `n` is ([`Self::takes_path`]).
    pub(crate) fn place(&self, nodes: &mut BTreeMap<PathName, Node>, name: PathName, n: Node) {
        match nodes.get(&name) {
            Some(listed) if self.takes_path(listed, &n) != Some(true) => {}
            _ => {
                nodes.insert(name, n);
            }
        }
    }
}

/// The birth speaks for its own name: it published within the staleness floor and is not held true
/// offline.
pub(crate) fn publishes(digest_view: &DigestView, records: &Records, node_id: &NodeId, incarnation: &IncarnationId, age: Duration) -> bool {
    age <= digest_view.staleness_floor() && !records.offline.lock().unwrap().contains(&(node_id.clone(), incarnation.clone()))
}

/// Where the row of the birth `(node_id, incarnation)`, which last published `published`, stands, as
/// the admin `me` reads it, and whether that row released its name ([`crate::node_rows::released_its_name`]):
/// one point read through the row's id index in the birth's mesh (a rename never changes the mesh). An
/// index entry that cannot be read is one span on every projection that reads it, until it can be read.
pub(crate) async fn row_name(me: &PathName, store: &dyn crate::storage::NodesStorage, node_id: &NodeId, incarnation: &IncarnationId, published: &PathName) -> (RowName, bool) {
    match store.node(node_id).await.map(|r| r.filter(|r| r.name.mesh == published.mesh)) {
        Ok(Some(row)) => {
            let released = crate::node_rows::released_its_name(&row);
            (RowName::At(row.name), released)
        }
        // The node holds no row: no rename moved it, and the name it published stands.
        Ok(None) => (RowName::At(published.clone()), false),
        Err(e) => {
            tracing::info_span!("rdm.node_admin.topology.reject.via-row-unreadable", node = %me, subject = %published, node_id = %node_id, incarnation_id = %incarnation.as_str(), index = "id", detail = %e)
                .in_scope(|| tracing::info!("the birth's row could not be read through its id index: its name is not known and the view does not list it"));
            (RowName::Unreadable, false)
        }
    }
}

/// Projects the digest view into the view now: what the join door runs once it installed an admitted
/// birth's address, so the view the status authority resolves senders from holds that birth.
pub fn view_projector(me: PathName, topology: Arc<RwLock<Topology>>, fabric: String, fabric_id: FabricId, provider: ProviderKind, digest_view: DigestView, records: Arc<Records>) -> Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync> {
    Arc::new(move || {
        let (me, topology, fabric, fabric_id, digest_view, records) = (me.clone(), topology.clone(), fabric.clone(), fabric_id.clone(), digest_view.clone(), records.clone());
        Box::pin(async move {
            records.install_read(&me, &topology, &digest_view, &fabric_id, |reads| project_with(&fabric, &fabric_id, provider, &digest_view, &records, reads)).await;
        })
    })
}

/// Why [`project`] lacks `name` right now, from the same inputs: every digest the view holds for
/// the name (incarnation, status, unheard age, predecessor, forwarded, deleted, routable), the
/// removed set's entries for it, and the records' entry. The text of an unknown-node refusal from
/// the live view, so the exclusion is named where it happens.
pub fn describe_absence(digest_view: &DigestView, records: &Records, name: &PathName) -> String {
    let now = std::time::Instant::now();
    let digests: Vec<String> = digest_view
        .all()
        .into_iter()
        .filter(|d| &d.node.name == name)
        .map(|d| {
            let age = digest_view.get_at(d.node.node_id.as_str(), now).map(|(_, a)| a.as_millis() as u64).unwrap_or(0);
            format!(
                "digest{{incarnation {}, status {:?}, unheard {} ms, supersedes {}, deleted {}, routable {}, floor {} ms}}",
                d.node.incarnation.as_str(),
                d.status,
                age,
                d.node.supersedes.as_ref().map(|s| s.to_string()).unwrap_or_else(|| "none".into()),
                digest_view.is_deleted(d.node.node_id.as_str()),
                digest_view.routable(d.node.node_id.as_str()),
                digest_view.staleness_floor().as_millis()
            )
        })
        .collect();
    let removed: Vec<String> = records
        .removed
        .lock()
        .unwrap()
        .iter()
        .filter(|(n, _)| n == name)
        .map(|(_, inc)| inc.as_ref().map(|i| i.to_string()).unwrap_or_else(|| "none".into()))
        .collect();
    let recorded = records.nodes.lock().unwrap().get(name).map(|n| format!("incarnation {}, status {:?}", n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_else(|| "none".into()), n.status));
    format!(
        "view: [{}]; removed: [{}]; records: {}",
        if digests.is_empty() { "no digest for the name".to_string() } else { digests.join(", ") },
        removed.join(", "),
        recorded.unwrap_or_else(|| "none".into())
    )
}

/// [`project`] as of `now`.
pub fn project_at(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, digest_view: &DigestView, records: &Records, now: std::time::Instant) -> Topology {
    project_decided_at(fabric, fabric_id, provider, digest_view, records, &incumbency_of(digest_view, records), &ViewReads::none_at(now))
}

/// [`project_with`] with its seats decided against `decided`, the seat records and proven-gone births
/// read once: a caller that acts on the projection's seats (the hierarchy loop announcing one)
/// acts against the very records the projection was decided from, never a later read of them.
pub(crate) fn project_decided(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, digest_view: &DigestView, records: &Records, decided: &crate::election::Incumbency, reads: &ViewReads) -> Topology {
    project_decided_at(fabric, fabric_id, provider, digest_view, records, decided, reads)
}

/// The launch record this admin holds of the exact birth `incarnation`, whatever name the view lists
/// the birth at.
pub(crate) fn launched(recorded: &BTreeMap<PathName, Node>, incarnation: &Option<IncarnationId>) -> Option<Node> {
    incarnation.as_ref().and_then(|inc| recorded.values().find(|r| r.incarnation_id.as_ref() == Some(inc)).cloned())
}

pub(crate) fn project_decided_at(fabric: &str, fabric_id: &FabricId, provider: ProviderKind, digest_view: &DigestView, records: &Records, decided: &crate::election::Incumbency, reads: &ViewReads) -> Topology {
    let (now, rows) = (reads.at, &reads.rows);
    let removed = records.removed.lock().unwrap().clone();
    let recorded = records.nodes.lock().unwrap().clone();
    let mut nodes: BTreeMap<PathName, Node> = BTreeMap::new();
    // Every birth the topology view has spoken for, whatever became of it.
    let mut heard: HashSet<IncarnationId> = HashSet::new();
    // Each birth's election claim, by incarnation.
    let mut mesh_ids: BTreeMap<String, MeshId> = records.meshes.lock().unwrap().clone();
    for d in digest_view.all() {
        if &d.fabric_id != fabric_id {
            continue;
        }
        let Some((_, age)) = digest_view.get_at(d.node.node_id.as_str(), now) else { continue };
        heard.insert(d.node.incarnation.clone());
        // A birth that publishes names itself (R12); one that does not is listed at the name its row
        // holds (a stopped predecessor a node.update renamed `<path>.old`), one whose row could not be read not at all.
        let name = if publishes(digest_view, records, &d.node.node_id, &d.node.incarnation, age) {
            d.node.name.clone()
        } else {
            match rows.name_of(&d.node.node_id, &d.node.incarnation, &d.node.name) {
                Some(n) => n,
                None => continue,
            }
        };
        if removed.contains(&(name.clone(), Some(d.node.incarnation.clone()))) {
            continue;
        }
        let silent = age > digest_view.staleness_floor();
        if let Some(id) = d.mesh_id.clone() {
            mesh_ids.entry(name.mesh.clone()).or_insert(id);
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = d.node.node_id.clone();
        n.endpoint_id = Some(d.node.endpoint_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(provider);
        // Silent past the staleness floor: this node's own pruner marks it PendingReconnect in place;
        // `Dead` only while the mesh primary's offline tickle holds this exact birth true offline.
        n.status = if records.offline.lock().unwrap().contains(&(d.node.node_id.clone(), d.node.incarnation.clone())) {
            NodeStatus::Dead
        } else if !silent {
            node_status(d.status)
        } else {
            NodeStatus::PendingReconnect
        };
        // Application routing (states.md "Routing"): the birth listed ready, and the view
        // holds it and its mesh ready. Anything else is a routing stop.
        n.routable = n.status == NodeStatus::Ready && digest_view.routable(d.node.node_id.as_str());
        n.declared = n.incarnation_id.as_ref().and_then(|inc| records.declared.lock().unwrap().node(&n.node_id, inc)).map(|s| format!("{s:?}"));
        n.admin_api_base = d.admin_api_base.clone();
        n.transport_addr = Some(d.node.transport_addr);
        n.data_dir = d.data_dir.clone();
        // The birth's own published runtime fact names its deployment, whichever admin launched it.
        n.deployment_id = d.node.runtime.as_ref().and_then(|f| f.deployment().ok());
        if let Some(r) = launched(&recorded, &n.incarnation_id) {
            n.deployment_id = r.deployment_id.clone().or(n.deployment_id.take());
            n.data_dir = r.data_dir.clone();
            n.listeners = r.listeners.clone();
        }
        // One birth per path: the current birth by its row (`RowNames::takes_path`); where the rows
        // read do not tell the two apart, the newest digest wins over a superseded one.
        match nodes.get(&name) {
            Some(prev) if rows.takes_path(prev, &n).unwrap_or(!(prev.status.is_live() && !n.status.is_live())) => {
                nodes.insert(name, n);
            }
            Some(_) => {}
            None => {
                nodes.insert(name, n);
            }
        }
    }
    // A stopped birth (its process alive): the digest view dropped it, and this view keeps it,
    // `Leaving` and `stopped`, until it rejoins as itself or departs.
    for d in digest_view.stopped_digests() {
        if &d.fabric_id != fabric_id || digest_view.is_deleted(d.node.node_id.as_str()) {
            continue;
        }
        heard.insert(d.node.incarnation.clone());
        // A stopped node is told nothing of a rename (R13): it is listed at the name its row holds.
        let Some(name) = rows.name_of(&d.node.node_id, &d.node.incarnation, &d.node.name) else { continue };
        if removed.contains(&(name.clone(), Some(d.node.incarnation.clone()))) {
            continue;
        }
        // Its held digest states its mesh's id as a heard one does: a mesh whose births are all
        // stopped keeps its id, and with it its state (states.md: a mesh's deleted is final).
        if let Some(id) = d.mesh_id.clone() {
            mesh_ids.entry(name.mesh.clone()).or_insert(id);
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = d.node.node_id.clone();
        n.endpoint_id = Some(d.node.endpoint_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(provider);
        n.status = NodeStatus::Leaving;
        n.stopped = true;
        n.routable = false;
        n.declared = n.incarnation_id.as_ref().and_then(|inc| records.declared.lock().unwrap().node(&n.node_id, inc)).map(|s| format!("{s:?}"));
        n.admin_api_base = d.admin_api_base.clone();
        n.transport_addr = Some(d.node.transport_addr);
        n.data_dir = d.data_dir.clone();
        n.deployment_id = d.node.runtime.as_ref().and_then(|f| f.deployment().ok());
        if let Some(r) = launched(&recorded, &n.incarnation_id) {
            n.deployment_id = r.deployment_id.clone().or(n.deployment_id.take());
            n.data_dir = r.data_dir.clone();
            n.listeners = r.listeners.clone();
        }
        rows.place(&mut nodes, name, n);
    }
    // A stopped birth this admin never held a digest of (it joined after the stop): the stop
    // overlay the mesh topology carried names it, `Leaving` and `stopped`, until it rejoins or departs.
    for op in digest_view.stopped_unheard() {
        if digest_view.is_deleted(op.node_id.as_str()) {
            continue;
        }
        heard.insert(op.incarnation.clone());
        let Some(name) = rows.name_of(&op.node_id, &op.incarnation, &op.name) else { continue };
        if removed.contains(&(name.clone(), Some(op.incarnation.clone()))) {
            continue;
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = op.node_id.clone();
        n.incarnation_id = Some(op.incarnation.clone());
        n.provider = Some(provider);
        n.status = NodeStatus::Leaving;
        n.stopped = true;
        n.routable = false;
        n.declared = n.incarnation_id.as_ref().and_then(|inc| records.declared.lock().unwrap().node(&n.node_id, inc)).map(|s| format!("{s:?}"));
        if let Some(r) = launched(&recorded, &n.incarnation_id) {
            n.deployment_id = r.deployment_id.clone();
            n.data_dir = r.data_dir.clone();
            n.listeners = r.listeners.clone();
        }
        rows.place(&mut nodes, name, n);
    }
    // An own-mesh birth this admin holds only from its mesh-primary's projection (gossip.md §3.1
    // `MeshTopology`: what every node-admin holds of every Mesh; the member's own Digest never reached this
    // admin): listed as the projection carried it, never routable, its status the projection's while
    // it is within the staleness floor of this admin first holding it, and past the floor marked
    // `PendingReconnect` by this admin's own pruner (node-recovery.md "Detection and proof" step 1),
    // so the window it keeps runs from that first hold, whichever admin takes the seat.
    for (d, age) in digest_view.projected_unheard_at(now) {
        if &d.fabric_id != fabric_id {
            continue;
        }
        heard.insert(d.node.incarnation.clone());
        let Some(name) = rows.name_of(&d.node.node_id, &d.node.incarnation, &d.node.name) else { continue };
        if removed.contains(&(name.clone(), Some(d.node.incarnation.clone()))) {
            continue;
        }
        if let Some(id) = d.mesh_id.clone() {
            mesh_ids.entry(name.mesh.clone()).or_insert(id);
        }
        let mut n = Node::allocated(name.clone());
        n.node_id = d.node.node_id.clone();
        n.endpoint_id = Some(d.node.endpoint_id.clone());
        n.incarnation_id = Some(d.node.incarnation.clone());
        n.provider = Some(provider);
        n.status = if records.offline.lock().unwrap().contains(&(d.node.node_id.clone(), d.node.incarnation.clone())) {
            NodeStatus::Dead
        } else if age <= digest_view.staleness_floor() {
            node_status(d.status)
        } else {
            NodeStatus::PendingReconnect
        };
        n.routable = false;
        n.declared = n.incarnation_id.as_ref().and_then(|inc| records.declared.lock().unwrap().node(&n.node_id, inc)).map(|s| format!("{s:?}"));
        n.admin_api_base = d.admin_api_base.clone();
        n.transport_addr = Some(d.node.transport_addr);
        n.data_dir = d.data_dir.clone();
        n.deployment_id = d.node.runtime.as_ref().and_then(|f| f.deployment().ok());
        if let Some(r) = launched(&recorded, &n.incarnation_id) {
            n.deployment_id = r.deployment_id.clone().or(n.deployment_id.take());
            n.data_dir = r.data_dir.clone();
            n.listeners = r.listeners.clone();
        }
        rows.place(&mut nodes, name, n);
    }
    // Births this admin started that have not reported yet. A launch record
    // stands in for a birth only until the topology view speaks for it: once heard,
    // it never revives the birth (another admin may have deleted it since). A
    // deleted birth was heard and has left: its record never stands in either.
    for op in digest_view.deleted() {
        heard.insert(op.incarnation.clone());
    }
    for (name, r) in recorded {
        if r.incarnation_id.as_ref().is_some_and(|i| heard.contains(i)) || digest_view.is_deleted(r.node_id.as_str()) {
            continue;
        }
        rows.place(&mut nodes, name, r);
    }
    let mut topology = Topology {
        fabric: Fabric { id: fabric_id.clone(), name: fabric.into(), status: ScopeStatus::Pending, provider },
        meshes: Vec::new(),
        nodes: nodes.into_values().collect(),
    };
    let planned = records.planned();
    // A mesh the accepted Build names, or one that still has members: a mesh that left (the Build
    // does not name it and it has no member) is not in the topology. A planned mesh that does not
    // exist yet is not in the view either: the view is what is observed, and the Build's plan is
    // `topology − observed`.
    let mesh_names: BTreeSet<String> = mesh_ids.keys().filter(|m| digest_view.accepted_has_mesh(m) != Some(false)).cloned().chain(topology.members().map(|n| n.mesh.clone())).collect();
    // Every seat, by the one election function (`election`); through a fabric shutdown, the
    // seats held when it was learned (`Seats`).
    if records.is_entering() {
        // Not told yet who holds the seats: this view elects nobody.
        for n in topology.nodes.iter_mut() {
            n.is_primary = false;
            n.is_fabric_primary = false;
        }
    } else {
        // The election takes members: a replaced predecessor holds no seat.
        let (mut members, predecessors): (Vec<Node>, Vec<Node>) = std::mem::take(&mut topology.nodes).into_iter().partition(|n| !n.name.old);
        crate::election::resolve_with(&mut members, decided);
        topology.nodes = members.into_iter().chain(predecessors).collect();
    }
    if topology.fabric_primary().is_some() {
        // The fabric primary authors `degraded` from the investigation's recovery decision until the
        // recovered mesh's primary reports for its checklist (`crate::investigate`), and a successor holder
        // keeps an adopted `degraded` until every mesh primary reports (`crate::checklist`). An admin
        // that holds no seat serves the `degraded` the holder published.
        let held = records.fabric_state();
        let degraded = records.peer_recovering() || records.adopted_degraded() || records.published_degraded();
        topology.fabric.status = if held == crate::fabric_state::FabricState::Ready && degraded { ScopeStatus::Degraded } else { held.scope() };
    }
    let declared_meshes = records.declared.lock().unwrap().meshes.clone();
    for m in mesh_names {
        // A mesh becomes ready when every node of the mesh that is not stopped is (states.md:16, Q240); once heard
        // ready it stays ready while it has a primary, because a lost node is a degraded fabric and
        // a checklist for the primary that takes over, never a mesh gone back to pending.
        let ready = topology.mesh_unready(&m, planned.get(&m)).is_empty() || (records.heard_ready(&m) && topology.node_type_primary(&m, NodeKind::NodeAdmin).is_some());
        let id = mesh_ids.get(&m).cloned();
        // A mesh past ready is the state its row holds (states.md: pending ->
        // ready -> leaving -> dead): the leaving mesh's primary commits Leaving at
        // delete-mesh, and this projection is what that primary announces on its MeshStatus send.
        // Dead is final: after mesh.deleted the mesh row is Dead and its last admin, still its
        // primary, publishes MeshStatus Dead (R-NS1 item 1); an admin that does not author the
        // mesh's status also states the Dead it heard, bound to this mesh id (never to a name).
        let row = id.as_ref().and_then(|id| declared_meshes.get(id).copied());
        let authored_here = records.me.get().is_some_and(|me| topology.node_type_primary(&m, NodeKind::NodeAdmin).is_some_and(|p| &p.node_id == me));
        let status = match row {
            Some(rdm_core::status::MeshState::Dead) => ScopeStatus::Dead,
            _ if id.as_ref().is_some_and(|id| records.mesh_deleted(id)) => ScopeStatus::Dead,
            _ if records.heard_dead(id.as_ref(), authored_here) => ScopeStatus::Dead,
            Some(rdm_core::status::MeshState::Leaving) => ScopeStatus::Leaving,
            _ if ready => ScopeStatus::Ready,
            _ => ScopeStatus::Pending,
        };
        topology.meshes.push(Mesh { id, name: m, status });
    }
    topology
}

/// What makes the view's rebuild due: every input the projection reads, each moved where it changes
/// (Q254: no timer, no poll; a sleep is a timer). The digest view's routing watch (a birth's status,
/// held or held only from its mesh-primary's projection; an overlay or a stop mark; a departure taken
/// or forgotten; a mesh status; the meshes the accepted Build names: states.md "Routing", applied on
/// receipt; and a birth silent past the staleness floor, marked by the pruner's sweep, the one clock the
/// projection reads), the records' inputs ([`Records::inputs`]), the primary records the election reads
/// ([`rdm_transport::topology::PrimaryView::subscribe`]), the states this admin applied
/// ([`crate::status_rpc::Declared::subscribe`]) and the node rows this admin writes
/// ([`crate::node_rows::WrittenRows::subscribe_names`]). A watch whose sender is gone leaves the wait;
/// the others are still waited on ([`rdm_transport::topology::RoutingWatch::moved`]).
pub(crate) struct ViewWake {
    inputs: rdm_transport::topology::RoutingWatch,
}

impl ViewWake {
    /// The inputs of the projection over `digest_view` and `records`, watched.
    pub(crate) fn new(digest_view: &DigestView, records: &Records) -> Self {
        let declared = records.declared.lock().unwrap().subscribe();
        let inputs = digest_view
            .routing_watch()
            .watching("records", records.subscribe_inputs())
            .watching("primaries", digest_view.seats.subscribe())
            .watching("declared", declared)
            .watching("rows", records.caches.nodes.subscribe_names());
        Self { inputs }
    }

    /// Every input moved so far is seen: called right before a rebuild reads them, so one moved
    /// while it reads makes the next one due.
    pub(crate) fn seen(&mut self) {
        self.inputs.seen();
    }

    /// Returns when an input moved since [`Self::seen`], naming it.
    async fn moved(&mut self) -> &'static str {
        self.inputs.moved().await
    }
}

/// The view's next rebuild is due: when an input of the projection moved since the last rebuild
/// read it ([`ViewWake`]), and only while the admin's gate is open; never on a clock of its own. A
/// stopped admin's rebuild is held here, holding nothing, and is due the moment the gate opens.
/// Answers the rebuild's turn, which the caller ends before it waits here again, and what made it
/// due.
pub(crate) async fn view_rebuild_due(wake: &mut ViewWake, gate: &crate::stop_gate::StopGate) -> (crate::stop_gate::Turn, &'static str) {
    let woken = wake.moved().await;
    (gate.turn("view-rebuild").await, woken)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fabric_builds::SeatRecord;
    use crate::storage::{MemoryNodesStorage, NodeRow, NodesStorage};
    use rdm_core::{Primary, Seat};

    /// CONTRACT: docs/architecture/build.md:453
    /// "One writer per key: the fabric-primary writes the Fabric record and every topology attempt's Build facts"
    /// CONTRACT: docs/architecture/node-identity-and-state.md:149
    /// "node.delete releases the node's identity and name; its row stays. The node's mesh-primary writes
    /// the row with status deleted, keeping the record and its config; the deleted row releases..."
    #[tokio::test]
    async fn fabric_primary_source_build_records_reader_takes_path_row_name_and_describe_absence_behave_as_specified() {
        let dv = DigestView::default();
        let records = Arc::new(Records::default());
        let me_id = NodeId::mint();
        let me_inc = IncarnationId::mint();
        let me_name: PathName = "mesh1.admin.1".parse().unwrap();
        let view = Arc::new(std::sync::OnceLock::new());

        // 1. `fabric_seat_source` and `build_records_reader`:
        //    When no fabric primary is recorded, `fabric_seat_source` returns `Open { holder: None }`
        //    and `build_records_reader` returns `Some` when `unlaunched = true`, `None` when `unlaunched = false`.
        let primary_src = fabric_seat_source(&dv, &records, (me_id.clone(), me_inc.clone()), &me_name, view.clone());
        assert_eq!((primary_src.read)(), SeatRecord::Open { holder: None });

        let reader_launched = build_records_reader(&dv, &records, (me_id.clone(), me_inc.clone()), &me_name, false, view.clone());
        let reader_unlaunched = build_records_reader(&dv, &records, (me_id.clone(), me_inc.clone()), &me_name, true, view.clone());
        assert_eq!(reader_launched(), None);
        assert_eq!(reader_unlaunched(), Some(me_name.to_string()));

        // When the fabric primary names this exact birth, both `primary_src` and `reader_launched` name `me_name`.
        dv.seats.take(
            Seat::FabricPrimary,
            &Primary {
                mesh: "mesh1".to_string(),
                node_id: me_id.clone(),
                incarnation: me_inc.clone(),
                revision: 1,
            },
        );
        assert_eq!((primary_src.read)(), SeatRecord::Open { holder: Some(me_name.to_string()) });
        assert_eq!(reader_launched(), Some(me_name.to_string()));

        // When a newer revision moves the fabric primary to another birth, `primary_src` reports `Moved` and `reader_launched` returns `None`.
        let other_id = NodeId::mint();
        let other_inc = IncarnationId::mint();
        dv.seats.take(
            Seat::FabricPrimary,
            &Primary {
                mesh: "mesh1".to_string(),
                node_id: other_id,
                incarnation: other_inc,
                revision: 2,
            },
        );
        assert!(matches!((primary_src.read)(), SeatRecord::Moved { .. }));
        assert_eq!(reader_launched(), None);

        // 2. `takes_path` and `row_name`:
        //    `row_name` reads a birth's row from `NodesStorage` and indicates whether the row released its name (`Deleted` / `Withdrawn`).
        //    `takes_path` lets a candidate whose row has not released its name take the path from a listed birth whose row released its name.
        let store = MemoryNodesStorage::default();
        let rpc_path: PathName = "mesh1.rpc.1".parse().unwrap();
        let old_id = NodeId::mint();
        let old_inc = IncarnationId::mint();
        let new_id = NodeId::mint();
        let new_inc = IncarnationId::mint();

        let (r_none, rel_none) = row_name(&me_name, &store, &new_id, &new_inc, &rpc_path).await;
        assert!(matches!(r_none, RowName::At(ref p) if p == &rpc_path));
        assert!(!rel_none);

        store.put_node(&NodeRow::new(rpc_path.clone(), old_id.clone(), NodeStatus::Deleted)).await.unwrap();
        let (r_del, rel_del) = row_name(&me_name, &store, &old_id, &old_inc, &rpc_path).await;
        assert!(matches!(r_del, RowName::At(ref p) if p == &rpc_path));
        assert!(rel_del);

        let mut row_names = RowNames::default();
        row_names.released.insert((old_id.clone(), old_inc.clone()));

        let mut old_node = Node::allocated(rpc_path.clone());
        old_node.node_id = old_id;
        old_node.incarnation_id = Some(old_inc);

        let mut new_node = Node::allocated(rpc_path.clone());
        new_node.node_id = new_id;
        new_node.incarnation_id = Some(new_inc);

        assert_eq!(row_names.takes_path(&old_node, &new_node), Some(true));
        assert_eq!(row_names.takes_path(&new_node, &old_node), Some(false));
        assert_eq!(row_names.takes_path(&new_node, &new_node), None);

        // 3. `describe_absence` formats the view digests, removed set, and records entry for an absent path.
        records.removed.lock().unwrap().insert((rpc_path.clone(), None));
        let desc = describe_absence(&dv, &records, &rpc_path);
        assert!(desc.contains("no digest for the name"), "{desc}");
        assert!(desc.contains("removed: [none]"), "{desc}");
        assert!(desc.contains("records: none"), "{desc}");
    }
}
