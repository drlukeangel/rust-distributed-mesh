//! The crate's one integration-test executable: every file in this directory is a module
//! here, so the crate links once. A stem runs alone as `<exe> <stem>::`.

/// Only this story's spans reached the exporter: every one is RDM's own (`rdm.*`). A transport
/// internal (iroh, noq) or any other foreign span in a cell's trace is a defect of the capture.
pub fn assert_own_story<'a>(names: impl IntoIterator<Item = &'a str>) {
    let foreign: std::collections::BTreeSet<&str> = names.into_iter().filter(|n| !n.starts_with("rdm.")).collect();
    assert!(foreign.is_empty(), "spans outside the cell's story were exported: {foreign:?}");
}

#[path = "../../../tools/test-support/own_process.rs"]
mod own_process;
#[allow(dead_code)]
#[path = "../../../tools/test-support/in_process_provider.rs"]
mod in_process_provider;

/// The binary's process environment, set once, here, before `main` and before any thread exists
/// (one binary is one process): a cell never sets or removes a variable. The gate's cadence
/// (`tools/test-runner/src/lib.rs` CADENCE), each value only where the environment the binary was
/// started with names none: the gate's own export, or the environment a delegating parent hands the
/// child it starts for one cell (`own_process::delegated_with_env`, a cell whose process needs other
/// values: a stretched checklist, its own evidence directory, a runtime socket), stays as given. Span
/// export carries DEBUG (`RDM_OTEL_DEBUG=1`), the per-call Node RPC spans the cells read among them (Q441).
#[ctor::ctor]
fn process_env() {
    for (k, v) in [("RDM_STALENESS_MS", "3000"), ("RDM_GOSSIP_INTERVAL_MS", "500"), ("RDM_BACKBONE_INTERVAL_MS", "500"), ("RDM_OTEL_DEBUG", "1")] {
        if std::env::var_os(k).is_none() {
            std::env::set_var(k, v);
        }
    }
}

/// The rafka-time a test admin holds, adopted from a reference nowhere near any OS clock.
pub fn adopted_time() -> rdm_transport::clock::RafkaTime {
    let t = rdm_transport::clock::RafkaTime::unadopted();
    t.adopt(1_000_000);
    t
}

/// A control plane as boot wires it: its own node id and incarnation are set (boot sets them from its
/// birth, `boot.rs`), here the birth the view holds at `me`.
pub fn control_plane(builds: std::sync::Arc<dyn rdm_admin::build_state::BuildStateAdapter>, accepted: std::sync::Arc<rdm_admin::accepted::AcceptedStore>, me: rdm_core::PathName, topology: rdm_admin::topology::Topology, time: rdm_transport::clock::RafkaTime) -> rdm_admin::control_plane::ControlPlane {
    let birth = topology.node(&me).map(|n| (n.node_id.clone(), n.incarnation_id.clone()));
    let cp = rdm_admin::control_plane::ControlPlane::new(builds, accepted, me, topology, time);
    if let Some((id, incarnation)) = birth {
        let _ = cp.node_id.set(id);
        if let Some(incarnation) = incarnation {
            let _ = cp.incarnation.set(incarnation);
        }
    }
    cp
}

/// A `NodeConfigs` bound to the nodes store as boot binds it (`NodeConfigs::uses_records`): a fresh
/// nodes store, its own channels joined on an endpoint of its own.
pub async fn bound_configs(configs: rdm_admin::node_config::NodeConfigs) -> rdm_admin::node_config::NodeConfigs {
    bound_configs_on(std::sync::Arc::new(rdm_admin::storage::MemoryNodesStorage::default()), configs).await
}

/// [`bound_configs`] over `records`, the nodes store a cell reads the written properties back from.
pub async fn bound_configs_on(records: std::sync::Arc<rdm_admin::storage::MemoryNodesStorage>, configs: rdm_admin::node_config::NodeConfigs) -> rdm_admin::node_config::NodeConfigs {
    bound_configs_over(records, configs).await
}

/// [`bound_configs`] over any nodes store: a cell's own (one that refuses a write, say).
pub async fn bound_configs_over(records: std::sync::Arc<dyn rdm_admin::storage::NodesStorage>, configs: rdm_admin::node_config::NodeConfigs) -> rdm_admin::node_config::NodeConfigs {
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let topology_view = rdm_gossip::topology::TopologyView::new(&rdm_core::FabricId::mint(), "mesh1", &rdm_core::MeshId::mint(), rdm_core::NodeId::mint().as_str(), rdm_transport::clock::os_clock(), Default::default());
    let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).unwrap();
    channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![]).await.unwrap();
    configs.uses_records(records, || "memory".into(), || None);
    configs
}

/// One application notice of a Build's opening or ending, as a cell's application recorded it.
#[derive(Debug, Clone)]
pub enum Notice {
    /// `on_build_started`.
    Started(rdm_admin::wiring::BuildStartedCtx),
    /// `on_build_ended`.
    Ended(rdm_admin::wiring::BuildEndedCtx),
}

impl Notice {
    /// The Build the notice names, and `started` or the ended state's name.
    pub fn key(&self) -> (String, &'static str) {
        match self {
            Notice::Started(c) => (c.build_id.clone(), "started"),
            Notice::Ended(c) => (c.build_id.clone(), c.state.as_str()),
        }
    }
}

/// The application of a cell: it records every Build notice the fabric-primary sends it, in the order
/// it was told (fabric-node-hooks.md `on_build_started`, `on_build_ended`; Q256).
#[derive(Clone, Default)]
pub struct Told(std::sync::Arc<std::sync::Mutex<Vec<Notice>>>);

impl Told {
    /// The notices a drive sends this application.
    pub fn notices(&self) -> rdm_admin::build_drive::BuildNotices {
        let (started, ended) = (self.0.clone(), self.0.clone());
        let on_build_started: rdm_admin::wiring::BuildStartedNotice = std::sync::Arc::new(move |c| {
            started.lock().unwrap().push(Notice::Started(c));
            Box::pin(async {})
        });
        let on_build_ended: rdm_admin::wiring::BuildEndedNotice = std::sync::Arc::new(move |c| {
            ended.lock().unwrap().push(Notice::Ended(c));
            Box::pin(async {})
        });
        let hooks = rdm_admin::wiring::FabricHooks { on_build_started: Some(on_build_started), on_build_ended: Some(on_build_ended), ..rdm_admin::wiring::FabricHooks::no_app_work() };
        rdm_admin::build_drive::BuildNotices::of(rdm_core::FabricId::mint(), &hooks)
    }

    /// Every notice told so far.
    pub fn all(&self) -> Vec<Notice> {
        self.0.lock().unwrap().clone()
    }

    /// The notices told once `want` of them are in (bounded at 10 s: a missing notice is named by the
    /// cell's assert), then after a further 500 ms in which no other may arrive.
    pub async fn settled(&self, want: usize) -> Vec<Notice> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while self.0.lock().unwrap().len() < want && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        self.all()
    }
}

/// The executable a test request names for a path of `type`: a file of its own per type, hashed once,
/// so the `BuildService`'s acceptance (it hashes every executable a request creates a node on) passes.
pub fn test_executable(r#type: &str) -> rdm_core::binding::Binding {
    use std::os::unix::fs::PermissionsExt;
    static DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    let dir = DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("admin-test-executables-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    });
    let p = dir.join(r#type);
    if !p.exists() {
        let tmp = dir.join(format!("{}.{}", r#type, rand::random::<u32>()));
        std::fs::write(&tmp, format!("#!/bin/sh\n# {}\nexit 0\n", r#type)).unwrap();
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).unwrap();
        let _ = std::fs::rename(&tmp, &p);
    }
    rdm_core::binding::Binding { launch_id: r#type.into(), sha256: rdm_core::binding::sha256_file(&p).unwrap(), executable: p, image: None }
}

/// The request `of` names against `cp`'s view: a cell states its change as counts or paths, and the
/// caller names every node before the typed call (build.md §1: counts are the product's API's vocabulary).
pub async fn named_request(cp: &rdm_admin::control_plane::ControlPlane, of: impl FnOnce(&rdm_admin::topology::Topology) -> rdm_core::build::BuildChange) -> rdm_core::build::BuildChange {
    let observed = cp.topology.read().await.clone();
    of(&observed)
}

/// The Create that grows each mesh of `desired` to its counts in `t`: each missing node at the lowest
/// free ordinal of its type the view holds no node at, on [`test_executable`] of its type, and each mesh
/// `t` does not hold with it.
/// A count below what the mesh holds removes nothing (a Delete names the nodes it removes).
pub fn grown(t: &rdm_admin::topology::Topology, desired: &[rdm_admin::build::MeshDesired]) -> rdm_core::build::BuildChange {
    let mut meshes = Vec::new();
    let mut nodes = Vec::new();
    for d in desired {
        if !t.meshes.iter().any(|m| m.name == d.name) {
            meshes.push(rdm_core::build::Meshes { name: d.name.clone() });
        }
        let mut types: Vec<(String, u32)> = d.counts().into_iter().collect();
        types.sort_by_key(|(r#type, _)| !rdm_core::is_node_admin(r#type));
        for (r#type, want) in types {
            let held = |p: &rdm_core::PathName| t.node(p).is_some();
            let mut have = t.nodes.iter().filter(|n| n.mesh == d.name && n.r#type == r#type).map(|n| n.location()).collect::<std::collections::BTreeSet<_>>().len() as u32;
            let mut ordinal = 1;
            while have < want {
                let path = rdm_core::PathName::numbered(&d.name, &r#type, ordinal);
                ordinal += 1;
                if held(&path) {
                    continue;
                }
                nodes.push(rdm_core::build::Nodes { path: path.path, name: path.name, mesh: d.name.clone(), r#type: r#type.clone(), storage_persistence: None, executable: Some(test_executable(&r#type)), tags: Default::default(), config: Default::default() });
                have += 1;
            }
        }
    }
    rdm_core::build::BuildChange::Create { meshes, nodes }
}

/// The Create of one more `type` node in `mesh` than `t` holds there.
pub fn grown_by_one(t: &rdm_admin::topology::Topology, mesh: &str, r#type: &str) -> rdm_core::build::BuildChange {
    let have = t.nodes.iter().filter(|n| n.mesh == mesh && n.r#type == r#type).map(|n| n.location()).collect::<std::collections::BTreeSet<_>>().len() as u32;
    grown(t, &[rdm_admin::build::MeshDesired::of(mesh, [(r#type, have + 1)])])
}

/// The Delete of the node `t` holds at each of `paths`, by its node.id.
pub fn removed(t: &rdm_admin::topology::Topology, paths: &[rdm_core::PathName]) -> rdm_core::build::BuildChange {
    let node_ids = paths.iter().map(|p| t.node(p).unwrap_or_else(|| panic!("the view holds {p:?}")).node_id.clone()).collect();
    rdm_core::build::BuildChange::Delete { meshes: Vec::new(), node_ids }
}

/// A nodes store holding the record of every birth `t` names, each written as its create wrote it
/// (`node_rows::write_record`: its name claimed, the record, its path index), its status
/// `Ready`: the fabric-primary's node records, from which its `BuildService` accepts an action on a
/// birth (the atlas, R-FA1), never from the gossip view (Q197).
pub async fn records_of(t: &rdm_admin::topology::Topology) -> std::sync::Arc<rdm_admin::storage::MemoryNodesStorage> {
    records_of_views(&[t]).await
}

/// [`records_of`] over several views of one estate: each birth any of them holds, once.
pub async fn records_of_views(views: &[&rdm_admin::topology::Topology]) -> std::sync::Arc<rdm_admin::storage::MemoryNodesStorage> {
        let rows = std::sync::Arc::new(rdm_admin::storage::MemoryNodesStorage::default());
    let mut seen = std::collections::BTreeSet::new();
    for n in views.iter().flat_map(|t| t.nodes.iter()).filter(|n| seen.insert(n.node_id.clone())) {
        let mut row = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), rdm_core::IncarnationId::mint(), rdm_admin::model::NodeStatus::Ready);
        if let Some(incarnation) = &n.incarnation_id {
            row.incarnation = incarnation.clone();
        }
        // Each birth's own record, as it writes it at its start (Q356).
        rdm_admin::storage::NodesStorage::put_node(&*rows, &row).await.unwrap_or_else(|e| panic!("{:?}'s record is put: {e}", n.name));
    }
    rows
}

/// [`records_of_views`] bound on `cp` as the records its acceptance and name lookups read ([`bind_store`]).
pub async fn bind_records(cp: &rdm_admin::control_plane::ControlPlane, views: &[&rdm_admin::topology::Topology]) -> std::sync::Arc<rdm_admin::storage::MemoryNodesStorage> {
    let rows = records_of_views(views).await;
    bind_store(cp, rows.clone());
    rows
}

/// Bind on `cp` the records its acceptance and name lookups read (query.md §1: the pair's name claim,
/// then the record at the node id): `rows`, the nodes store the claims and records are read from.
pub fn bind_store(cp: &rdm_admin::control_plane::ControlPlane, rows: std::sync::Arc<rdm_admin::storage::MemoryNodesStorage>) {
    let _ = cp.node_rows.set(rows.clone());
}

/// Every mesh `t` names given its Mesh record and name claim in `meshes` (the one `meshes` holds by name,
/// else `t`'s id for it, else a minted one): the Mesh records a reader (`current_record::CurrentRecords`)
/// reads beside the nodes' records.
pub async fn write_meshes(t: &rdm_admin::topology::Topology, meshes: &dyn rdm_admin::storage::MeshStorage) {
    let names: std::collections::BTreeSet<String> = t.nodes.iter().map(|n| n.mesh.clone()).chain(t.meshes.iter().map(|m| m.name.clone())).collect();
    for mesh in names {
        if meshes.mesh_named(&mesh).await.expect("the mesh store reads").is_none() {
            let id = t.meshes.iter().find(|m| m.name == mesh).and_then(|m| m.id.clone()).unwrap_or_else(rdm_admin::model::MeshId::mint);
            meshes.put_mesh(&rdm_admin::storage::MeshRecord::new(id.clone(), mesh.clone())).await.expect("the Mesh record is written");
        }
    }
}

#[cfg(feature = "file-backend")]
/// Now, in milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

/// The fabric of the Build events the cells write.
pub fn cell_fabric() -> rdm_admin::model::FabricId {
    rdm_admin::model::FabricId::parse("fab000000001").unwrap()
}

/// `build_id`'s `rdm.build.started`, written into `builds`' events store by `by`: a run of the Build
/// began (build.md §5).
pub async fn start_build(builds: &dyn rdm_admin::build_state::BuildStateAdapter, build_id: &rdm_admin::build::BuildId, by: &rdm_core::framing::Birth) {
    let source = rdm_admin::events_storage::EventSource { fabric_id: cell_fabric(), node_id: by.node_id.clone(), incarnation: by.incarnation.clone() };
    builds.events().append(&rdm_admin::events_storage::Event::build_started(&source, now_ms(), build_id)).await.unwrap();
}

/// A run of `build_id` by `by` that ended `ended`: its `rdm.build.started`, then its `rdm.build.ended`.
pub async fn end_build(builds: &dyn rdm_admin::build_state::BuildStateAdapter, build_id: &rdm_admin::build::BuildId, by: &rdm_core::framing::Birth, ended: rdm_admin::events_storage::EndedState) {
    start_build(builds, build_id, by).await;
    let source = rdm_admin::events_storage::EventSource { fabric_id: cell_fabric(), node_id: by.node_id.clone(), incarnation: by.incarnation.clone() };
    builds.events().append(&rdm_admin::events_storage::Event::build_ended(&source, now_ms(), build_id, &ended)).await.unwrap();
}

/// `notices`, the fabric-primary `by` of `fabric_id` writing the Build-level events into `builds`'
/// events store, as boot wires them (build.md §5).
pub fn written_by(notices: rdm_admin::build_drive::BuildNotices, builds: &dyn rdm_admin::build_state::BuildStateAdapter, fabric_id: &rdm_admin::model::FabricId, by: &rdm_core::framing::Birth) -> rdm_admin::build_drive::BuildNotices {
    let mut events = rdm_admin::events_storage::EventsWriter::on(builds.events(), fabric_id, &by.node_id);
    events.source.incarnation = by.incarnation.clone();
    notices.writing_to(events)
}

/// A Build store over memory whose steps are written as events by `by`, of `mesh`.
pub fn stepping(by: &rdm_core::NodeId, mesh: &str) -> rdm_admin::build_run::FramedBuilds {
    rdm_admin::build_run::FramedBuilds::stepping(std::sync::Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()), &cell_fabric(), by, mesh)
}

mod admin_app_alpns;
mod app_protocol_dials;
mod admin_serves_forward;
mod ids_crockford;
#[cfg(feature = "file-backend")]
mod backbone_anti_entropy;
mod boot_stop;
mod loopback;
mod mesh_delete_resume;
mod mesh_delete_workflow;
mod mesh_delete_source;
mod mesh_delete_view;
mod build_admission;
mod operation_gates;
mod pinned_birth;
mod build_drive;
mod build_run;
mod build_floor_read;
mod builds_read;
mod build_takeover;
mod command_admission_status;
#[cfg(feature = "file-backend")]
mod connections_writer;
#[cfg(feature = "file-backend")]
mod container_proof;
mod container_unsupported;
mod current_build;
mod declared_types;
#[cfg(feature = "file-backend")]
mod primary_update;
mod fabric_delete_admission;
mod status_rpc;
mod executor_redecide;
#[cfg(feature = "file-backend")]
mod connections_reconnect;
mod gossip_frames_wire;
mod abandoned_join;
mod join_admission;
mod join_wire;
mod node_objects;
#[cfg(feature = "file-backend")]
mod node_rows;
mod node_get_reads;
mod node_rpc_objects;
mod node_command_caller;
#[cfg(feature = "file-backend")]
mod stop_gate_quiesce;
#[cfg(feature = "file-backend")]
mod stop_gate_view_lock;
mod own_run_stop;
mod process_launch_environment;
mod provider_state;
mod provider_policy;
mod read_stream;
mod readiness_gates;
mod batch_change;
mod step_event_refusal;
mod start_join_order;
mod pipeline_moves_no_primary;
mod pipeline_step_io;
mod status_declarations;
#[cfg(feature = "file-backend")]
mod mesh_primary_status_fold;
mod stopped_delete;
mod stopped_gc;
#[cfg(feature = "file-backend")]
mod store_root_boot;
mod topology_read;
mod fabric_create;
mod fabric_states;
mod fabric_delete;
mod build_reattach;
mod node_create;
mod node_delete;
mod node_investigation;
mod node_tickle;
mod node_runtime_inspect;
mod node_connections_delete;
mod named_listeners;
mod fabric_atlas_read;

#[allow(dead_code)]
#[path = "../../../tools/test-support/gossip_wire.rs"]
mod gossip_wire;
