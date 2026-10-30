//! The reads over Node RPC (node-rpc-envelope.md "Typed objects mapped to wire and execution
//! owners", Q167: one read per entity, the filter a property match): `node.get`, `mesh.get` and
//! `fabric.get` answer the rows the answering node-admin holds that match every `(property, values)`
//! pair, and `node.connection.get { node }` a node's held connections (fabric-mesh-node-lifecycles.md:
//! "Reading or cutting a node's held connections"). The typed objects read them from the node
//! itself over Node RPC; they hold no control API client.

use rdm_core::connections::NodeConnection;
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId, NodeKind, PathName};
use rdm_client::{BuildCarrier, CallEnd, Connections, ConnectionsView, ExactBirth, Fabrics, Filter, Meshes, NodeRpc, NodeStatus, Nodes};
use rdm_core::status::TagScope;
use rdm_core::status::StatusReply;
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_transport::{NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rdm_test::node;
use rdm_test::originate::Seams;
use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

const WRONG: Duration = Duration::from_secs(60);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.clone()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// A caller outside the mesh: its own endpoint, and a resolver that holds exactly `nodes`.
async fn caller(nodes: &[ResolvedNode]) -> (Arc<NodeRpcClient>, iroh::Endpoint) {
    let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller's endpoint binds");
    let resolver = Arc::new(StaticResolver::new());
    for n in nodes {
        resolver.insert(n.clone());
    }
    (Arc::new(NodeRpcClient::new(endpoint.clone(), resolver).with_caller_system("rdm")), endpoint)
}

/// The ids of the connections a node holds: its latest Direct per destination that is `connected`,
/// and its active Proxies (the open entries `node.connection.delete` names).
fn own_ids(held: &rdm_core::connections::ConnectionsHeld) -> BTreeSet<String> {
    held.own_latest_directs()
        .into_iter()
        .filter(|e| e.state == rdm_core::connections::ConnectionState::Connected)
        .chain(held.own_active_proxies())
        .map(|e: &NodeConnection| e.id.to_string())
        .collect()
}

fn ids(view: &ConnectionsView) -> BTreeSet<String> {
    view.connections.iter().map(|c| c.id.clone()).collect()
}

/// The node rows' `path.name`s of a `node.get`.
fn names_of(nodes: &[rdm_client::NodeView]) -> Vec<String> {
    nodes.iter().map(|n| n.name.to_string()).collect()
}

/// CONTRACT (Q167 R-RD1 to R-RD3: one read per entity, plural, the filter a list of `(property,
/// values)` pairs, AND across pairs, any-of within, nothing matching the empty list): a Day-0
/// node-admin answers the reads over Node RPC from what it holds, and the caller reaches nothing but
/// the admin's Node RPC endpoint. `node.get []` is the fabric's topology, the admin alone, as its
/// view holds it, with its row's status as its atlas holds it (Q233, query.md §1: Ready
/// once its row took it; primary of its mesh and of the fabric, its birth, its load and gossip stats
/// when its view carries them); `[id: the admin]` is that one node and `[id: another]` the empty list;
/// `[mesh: mesh1] [kind: admin]` the admin and `[mesh: mesh1] [kind: gateway]` nothing (AND across
/// pairs); `[mesh: mesh9]`, a mesh the admin does not hold, the empty list. `mesh.get []` lists
/// every mesh the view holds, by name; `[name: mesh1]` and `[id: mesh1's id]` the one mesh with the
/// admin its primary and only node; `[name: mesh9]` the empty list (no `unknown-mesh`). `fabric.get
/// []` is `fabric1` with the admin as its fabric-primary, and `[name: fabric9]` the empty list. A
/// pair naming a property no node row has (`zone`, not `config.zone`) is refused INVALID_ARGUMENT
/// naming the pair, on the wire and through the typed object. `node.connection.get { node }` of the
/// admin's exact birth names the admin and exactly the connections it holds, and `node.connection.get`
/// with no node is refused naming its address. Each read is served by the admin: its serve span
/// names the read, the pairs' properties and how much it answered. What must NOT happen: a read that
/// needs a control API, a read answered from anything but the admin's own view, a mesh the admin
/// does not hold refused instead of answered empty, or an unknown property answered as matching
/// nothing.
#[test]
fn a_node_admin_answers_the_plural_reads_by_property_and_its_connections_over_node_rpc() {
    if crate::own_process::delegated(module_path!(), "a_node_admin_answers_the_plural_reads_by_property_and_its_connections_over_node_rpc") {
        return;
    }
    let cap = crate::common::capture("a_node_admin_answers_the_plural_reads_by_property_and_its_connections_over_node_rpc");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("node-reads-{}", NodeId::mint()));
        let running = start_with(day0_config(&dir), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        let me = running.digest.lock().unwrap().clone();
        // The reads answer the births the admin's view holds, each with its row's status as the atlas
        // holds it (query.md §1 "Status": "a node's `status` is its row's as written: the state the row
        // last took from the node's frame or the mesh-primary's receipt", Q233). The view holds the
        // admin ready and primary of its mesh and of the fabric at its own Ready; its row takes
        // Ready when its mesh-primary applies its node.started report (states.md "Who sets
        // each state"), after that. The reads start once both hold.
        tokio::time::timeout(WRONG, async {
            loop {
                let ready = running.control.topology.read().await.births().any(|n| n.node_id == me.node.node_id && n.status == rdm_admin::model::NodeStatus::Ready && n.is_primary && n.is_fabric_primary);
                let row_ready = running.control.atlas.record(&me.node.node_id).is_some_and(|r| r.status == rdm_admin::model::NodeStatus::Ready);
                if ready && row_ready {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the admin's view never held it ready and primary of its mesh and of the fabric, with its row ready in its atlas");
        let admin = ResolvedNode { node_id: me.node.node_id.clone(), name: me.node.name.clone(), endpoint_id: me.node.endpoint_id.0.parse().unwrap(), incarnation: me.node.incarnation.clone(), transport_addr: me.node.transport_addr };
        let (client, _endpoint) = caller(&[admin]).await;
        let carrier = BuildCarrier::new(client.clone(), me.node.name.clone());
        let nodes = Nodes::new(carrier.clone());
        let meshes = Meshes::new(carrier.clone());
        let fabrics = Fabrics::new(carrier.clone());
        let nodes_get = |f: Filter| {
            let nodes = nodes.clone();
            async move { nodes.get(&f).await.unwrap_or_else(|e| panic!("node.get {f:?}: {e}")) }
        };

        // node.get: no pair is the fabric's topology, the admin alone, as its view holds it.
        let every_node = nodes_get(Filter::new()).await;
        let view = running.control.topology.read().await.clone();
        let loads = (running.control.loads.get().expect("the admin reads its members' loads"))();
        let held = view.births().find(|n| n.node_id == me.node.node_id).cloned().expect("the admin's view holds the admin");
        assert_eq!(names_of(&every_node), vec![held.name.to_string()], "the fabric topology is mesh1's one node: {every_node:#?}");
        let got = &every_node[0];
        assert_eq!((got.name.clone(), got.kind, got.mesh.as_str(), got.node_id.clone()), (held.name.clone(), NodeKind::NodeAdmin, "mesh1", me.node.node_id.clone()));
        assert_eq!((got.incarnation_id.clone(), got.endpoint_id.clone(), got.transport_addr), (Some(me.node.incarnation.clone()), Some(me.node.endpoint_id.0.clone()), Some(me.node.transport_addr)));
        assert_eq!((got.status, got.is_primary, got.is_fabric_primary), (NodeStatus::Ready, true, true), "{got:#?}");
        let (load, gossip) = loads.get(me.node.node_id.as_str()).cloned().unwrap_or((None, None));
        assert_eq!((got.load.is_some(), got.gossip.is_some()), (load.is_some(), gossip.is_some()), "the load and gossip stats are what the admin's view carries: {got:#?}");
        // [id: x]: one node or none; never NOT_FOUND.
        let by_id = nodes_get(Filter::new().eq("id", [me.node.node_id.to_string()])).await;
        assert_eq!(by_id.iter().map(|n| (n.name.clone(), n.node_id.clone(), n.incarnation_id.clone())).collect::<Vec<_>>(), vec![(held.name.clone(), me.node.node_id.clone(), Some(me.node.incarnation.clone()))], "[id] is the one node: {by_id:#?}");
        assert!(nodes_get(Filter::new().eq("id", [NodeId::mint().to_string()])).await.is_empty(), "an id the admin does not hold is the empty list");
        // AND across pairs; a mesh the admin does not hold is the empty list.
        assert_eq!(names_of(&nodes_get(Filter::new().eq("mesh", ["mesh1"]).eq("kind", ["admin"])).await), vec!["mesh1.admin.1"], "[mesh: mesh1] [kind: admin]");
        assert!(nodes_get(Filter::new().eq("mesh", ["mesh1"]).eq("kind", ["gateway"])).await.is_empty(), "[mesh: mesh1] [kind: gateway]: mesh1 holds no gateway");
        assert!(nodes_get(Filter::new().eq("mesh", ["mesh9"])).await.is_empty(), "[mesh: mesh9]: a mesh the admin does not hold answers the empty list");
        assert_eq!(names_of(&nodes_get(Filter::new().eq("is_fabric_primary", ["true"]).eq("status", ["ready"])).await), vec!["mesh1.admin.1"], "a boolean and an enum by their page names");

        // An unknown property is INVALID_ARGUMENT naming the pair, on the wire and through the object.
        let bad = Filter::new().eq("mesh", ["mesh1"]).eq("zone", ["eu-1"]);
        match rdm_client::NodeRpc::new(&client).read(&NodeTarget::CurrentPath(me.node.name.clone()), rdm_client::Entity::Nodes, &bad, &rdm_transport::CallOptions::default()).await {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains("the pair (zone, [eu-1])"), "op 0x23 refuses naming the pair: {reason}: {detail}"),
            other => panic!("an unknown property is RejectedFilter on op 0x23, not {other:?}"),
        }
        // The same read on Status is refused by name, naming op 0x23 (Q285).
        let on_status = rdm_core::status::StatusRequest::Get { entity: rdm_client::Entity::Nodes, node: None, filter: Filter::new() };
        let (out, _) = client.call::<rdm_core::status::Status>(&NodeTarget::CurrentPath(me.node.name.clone()), &on_status, &rdm_transport::CallOptions::default()).await;
        match out.reply().map(|r| r.value().clone()) {
            Some(StatusReply::RejectedFilter { reason }) => assert!(reason.contains("op 0x23"), "Status names op 0x23: {reason}"),
            other => panic!("a plural read on Status is refused naming op 0x23, not {other:?}"),
        }
        match nodes.get(&Filter::new().eq("zone", ["eu-1"])).await {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains("(zone, [eu-1])"), "refused naming the pair: {reason}: {detail}"),
            other => panic!("an unknown property is refused by name, not {other:?}"),
        }

        // mesh.get: every mesh by name, one by name or by id, and the empty list for a mesh not held.
        let mesh_id = me.mesh_id.clone().expect("the Day-0 admin holds its mesh id");
        for f in [Filter::new().eq("name", ["mesh1"]), Filter::new().eq("id", [mesh_id.to_string()])] {
            let one = meshes.get(&f).await.unwrap_or_else(|e| panic!("mesh.get {f:?}: {e}"));
            assert_eq!(one.iter().map(|m| (m.name.as_str(), m.primary_admin.clone(), m.nodes.clone())).collect::<Vec<_>>(), vec![("mesh1", Some(me.node.name.clone()), vec![me.node.name.clone()])], "{f:?}");
        }
        let every = meshes.get(&Filter::new()).await.unwrap_or_else(|e| panic!("mesh.get: {e}"));
        let mut held_meshes: Vec<String> = view.fabric_view().meshes.iter().map(|m| m.name.clone()).collect();
        held_meshes.sort();
        assert_eq!(every.iter().map(|m| m.name.clone()).collect::<Vec<_>>(), held_meshes, "every mesh the view holds, by name");
        assert!(meshes.get(&Filter::new().eq("name", ["mesh9"])).await.unwrap_or_else(|e| panic!("mesh.get [name: mesh9]: {e}")).is_empty(), "a mesh the admin does not hold is the empty list");

        // fabric.get.
        let fabric = fabrics.get(&Filter::new()).await.unwrap_or_else(|e| panic!("fabric.get: {e}"));
        assert_eq!(fabric.iter().map(|f| (f.name.as_str(), f.fabric_primary.clone())).collect::<Vec<_>>(), vec![("fabric1", Some(me.node.name.clone()))]);
        assert_eq!(fabric[0].meshes.iter().map(|m| m.name.clone()).collect::<BTreeSet<_>>(), held_meshes.iter().cloned().collect::<BTreeSet<_>>());
        assert!(fabrics.get(&Filter::new().eq("name", ["fabric9"])).await.unwrap_or_else(|e| panic!("fabric.get [name: fabric9]: {e}")).is_empty(), "a fabric that does not match is the empty list");

        // fabric.get awaited: a fact the admin's view holds is the read that held it; a fact it never
        // reports ends the call Indeterminate, naming the fact, the admin and its last report.
        let awaited = fabrics.fact("the admin is the fabric-primary", WRONG, |f| f.fabric_primary.as_ref() == Some(&me.node.name)).await.unwrap_or_else(|e| panic!("fabric.get awaited: {e}"));
        assert_eq!(awaited.fabric_primary, Some(me.node.name.clone()));
        match fabrics.fact("a fabric-primary other than the admin", Duration::from_secs(1), |f| f.fabric_primary.as_ref().is_some_and(|p| *p != me.node.name)).await {
            Err(CallEnd::Indeterminate { reason, .. }) => assert!(
                reason.contains("a fabric-primary other than the admin") && reason.contains(&me.node.name.to_string()) && reason.contains(&format!("fabric-primary {}", me.node.name)),
                "the unreported fact is named with the admin and its last report: {reason}"
            ),
            other => panic!("a fact the admin never reports ends Indeterminate, not {other:?}"),
        }

        // node.connection.get { node } of the admin: exactly what it holds; Get { Connections } on 0x23
        // names GetConnections, and Get { Connections } on Status names op 0x23.
        let birth = ExactBirth { target: NodeTarget::ExactNode(me.node.node_id.clone()), node_id: me.node.node_id.clone(), incarnation: me.node.incarnation.clone() };
        let connections = Connections::new(carrier.clone()).get(&birth, &Filter::new()).await.unwrap_or_else(|e| panic!("node.connection.get of the admin: {e}"));
        let (complete, own) = {
            let held = running.control.connections.get().expect("the admin holds its connections").lock().unwrap();
            (held.is_complete(), own_ids(&held))
        };
        assert_eq!((connections.node.as_str(), connections.complete), ("mesh1.admin.1", complete));
        assert_eq!(ids(&connections), own, "the connections the admin holds: {connections:#?}");
        match rdm_client::NodeRpc::new(&client).read(&birth.target, rdm_client::Entity::Connections, &Filter::new(), &rdm_transport::CallOptions::default()).await {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains("GetConnections"), "Get {{ Connections }} names GetConnections: {reason}: {detail}"),
            other => panic!("Get {{ Connections }} on op 0x23 is RejectedFilter naming GetConnections, not {other:?}"),
        }
        let on_status_connections = rdm_core::status::StatusRequest::Get { entity: rdm_client::Entity::Connections, node: Some((me.node.node_id.clone(), me.node.incarnation.clone())), filter: Filter::new() };
        let (out, _) = client.call::<rdm_core::status::Status>(&birth.target, &on_status_connections, &rdm_transport::CallOptions::default()).await;
        match out.reply().map(|r| r.value().clone()) {
            Some(StatusReply::RejectedFilter { reason }) => assert!(reason.contains("op 0x23"), "Status names op 0x23: {reason}"),
            other => panic!("node.connection.get on Status is RejectedFilter naming op 0x23, not {other:?}"),
        }

        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    // The trace: every read served by the admin, naming what was read, its pairs and how much.
    let served = |name: &str, wanted: &dyn Fn(&std::collections::BTreeMap<String, String>) -> bool| spans.0.lock().unwrap().values().any(|(n, _, f)| n == name && wanted(f));
    let at = |f: &std::collections::BTreeMap<String, String>, k: &str, v: &str| f.get(k).map(String::as_str) == Some(v);
    assert!(served("rdm.node_admin.node.serve.via-get", &|f| at(f, "read", "node.get") && at(f, "properties", "") && at(f, "nodes", "1")), "node.get [] served");
    assert!(served("rdm.node_admin.node.serve.via-get", &|f| at(f, "properties", "id") && at(f, "nodes", "0")), "node.get [id: unheld] served, matching nothing");
    assert!(served("rdm.node_admin.node.serve.via-get", &|f| at(f, "properties", "mesh,kind") && at(f, "pairs", "(mesh, [mesh1]) (kind, [gateway])") && at(f, "nodes", "0")), "node.get [mesh: mesh1] [kind: gateway] served, matching nothing");
    assert!(served("rdm.node_admin.node.serve.via-get", &|f| at(f, "properties", "mesh") && at(f, "pairs", "(mesh, [mesh9])") && at(f, "nodes", "0")), "node.get [mesh: mesh9] served, matching nothing");
    assert!(served("rdm.node_admin.node.serve.via-get", &|f| at(f, "outcome", "rejected-filter") && f.get("detail").is_some_and(|d| d.contains("(zone, [eu-1])"))), "node.get [zone: eu-1] refused naming the pair");
    assert!(served("rdm.node_admin.mesh.serve.via-get", &|f| at(f, "properties", "name") && at(f, "meshes", "1")), "mesh.get [name: mesh1] served");
    assert!(served("rdm.node_admin.mesh.serve.via-get", &|f| at(f, "pairs", "(name, [mesh9])") && at(f, "meshes", "0")), "mesh.get [name: mesh9] served, empty");
    assert!(served("rdm.node_admin.fabric.serve.via-get", &|f| at(f, "read", "fabric.get") && at(f, "fabrics", "1")), "fabric.get served");
    assert!(served("rdm.node_admin.node.serve.via-connection-get", &|f| at(f, "node", "mesh1.admin.1")), "node.connection.get served by the admin");
}

fn labels(l: &[&str]) -> BTreeSet<String> {
    l.iter().map(|s| s.to_string()).collect()
}

/// CONTRACT (Q167 R-RD2: a set field matches when it carries the value, a repeated property is all
/// of, one pair's values any of, `config.<key>` one dot into the config map; R-TG1, node-rpc-envelope.md
/// node.tags: "`node.tag.create { label | [labels] }` adds one label or a list; `node.tag.delete {
/// All | [labels] }` removes every label or the named ones ... Create is a blind put of that key, so
/// creating a label the node already carries is ignored and writes no duplicate"): a caller's
/// `node.tag.create {gold, ssd}` of the Day-0 admin ends `TagsCreated` carrying nothing already, and
/// its `node.config.update {zone: eu-1, rack.row: 7}` ends `ConfigUpdated`; then `node.get
/// [config.zone: eu-1]` and `[config.rack.row: 7]` answer the admin and `[config.zone: us-2]` nothing;
/// `[tags: gold] [tags: ssd]` (all of) answers the admin and `[tags: gold] [tags: blue]` nothing;
/// `[tags: gold, silver]` (any of) answers the admin and `[tags: blue, silver]` nothing; each with
/// `[mesh: mesh1]` the same, and with `[mesh: mesh2]` nothing. `node.tag.create {ssd}` again ends
/// `TagsCreated` naming ssd as already carried. `node.tag.delete {gold, green}` ends `TagsDeleted`
/// naming green as not carried, and gold no longer matches. A create whose label is not a label
/// (`Gold`) reaching the mesh-primary is refused `RejectedTags` naming it; a read whose tag value is
/// not a label is refused `RejectedFilter` naming the pair. `node.tag.delete { All }` leaves the
/// admin no label and matched by none. Each read's `via-get` span names its pairs and how many nodes
/// it answered. What must NOT happen: a label lost, a removed label still matching, all-of read as
/// any-of, or a config key answered by a node whose config lacks it.
#[test]
fn nodes_get_matches_a_nodes_tags_all_of_and_any_of_and_its_config_by_key() {
    if crate::own_process::delegated(module_path!(), "nodes_get_matches_a_nodes_tags_all_of_and_any_of_and_its_config_by_key") {
        return;
    }
    let cap = crate::common::capture("nodes_get_matches_a_nodes_tags_all_of_and_any_of_and_its_config_by_key");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("node-tags-{}", NodeId::mint()));
        let running = start_with(day0_config(&dir), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        let me = running.digest.lock().unwrap().clone();
        // A tags command is refused until the initial fabric has first reached ready (states.md "The
        // initial fabric before ready"): the admin is ready in both seats, and the fabric is ready.
        tokio::time::timeout(WRONG, async {
            loop {
                let t = running.control.topology.read().await;
                let ready = t.fabric.status == rdm_admin::model::ScopeStatus::Ready
                    && t.births().any(|n| n.node_id == me.node.node_id && n.status == rdm_admin::model::NodeStatus::Ready && n.is_primary && n.is_fabric_primary);
                drop(t);
                if ready {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the admin's view never held it ready in both seats under a ready fabric");
        let admin = ResolvedNode { node_id: me.node.node_id.clone(), name: me.node.name.clone(), endpoint_id: me.node.endpoint_id.0.parse().unwrap(), incarnation: me.node.incarnation.clone(), transport_addr: me.node.transport_addr };
        let (client, _endpoint) = caller(&[admin]).await;
        let carrier = BuildCarrier::new(client.clone(), me.node.name.clone());
        let nodes = Nodes::new(carrier);
        let one = || async {
            let mut got = nodes.get(&Filter::new().eq("id", [me.node.node_id.to_string()])).await.unwrap_or_else(|e| panic!("node.get [id: the admin]: {e}"));
            assert_eq!(got.len(), 1, "[id: the admin] is the admin: {got:#?}");
            got.remove(0)
        };
        // The nodes a filter matches, alone, with [mesh: mesh1] (the same) and with [mesh: mesh2] (none).
        let matching = |f: Filter| {
            let nodes = nodes.clone();
            async move {
                let ids = |v: Vec<rdm_client::NodeView>| v.into_iter().map(|n| n.node_id).collect::<Vec<_>>();
                let mut in_mesh1 = Filter::new().eq("mesh", ["mesh1"]);
                in_mesh1.pairs.extend(f.pairs.clone());
                let mut in_mesh2 = Filter::new().eq("mesh", ["mesh2"]);
                in_mesh2.pairs.extend(f.pairs.clone());
                let fabric = ids(nodes.get(&f).await.unwrap_or_else(|e| panic!("node.get {f:?}: {e}")));
                let mesh1 = ids(nodes.get(&in_mesh1).await.unwrap_or_else(|e| panic!("node.get {in_mesh1:?}: {e}")));
                let mesh2 = ids(nodes.get(&in_mesh2).await.unwrap_or_else(|e| panic!("node.get {in_mesh2:?}: {e}")));
                assert_eq!(mesh1, fabric, "{f:?}: mesh1 holds every node the fabric answers");
                assert!(mesh2.is_empty(), "{f:?}: mesh2 holds none of them: {mesh2:?}");
                fabric
            }
        };
        let got = one().await;
        assert!(got.tags.is_empty(), "a node born with no tags carries none: {got:#?}");
        let route = rdm_transport::RouteChoice::Direct;
        let mine = vec![me.node.node_id.clone()];
        let birth = ExactBirth { target: NodeTarget::ExactNode(me.node.node_id.clone()), node_id: me.node.node_id.clone(), incarnation: me.node.incarnation.clone() };
        let created = |already: &[&str]| StatusReply::TagsCreated { node_id: me.node.node_id.clone(), incarnation: me.node.incarnation.clone(), already_carried: labels(already) };
        let deleted = |not: &[&str]| StatusReply::TagsDeleted { node_id: me.node.node_id.clone(), incarnation: me.node.incarnation.clone(), not_carried: labels(not) };
        let tags = |vals: &[&str]| Filter::new().eq("tags", vals.iter().map(|v| v.to_string()));

        // node.tag.create {gold, ssd} and node.config.update {zone: eu-1, rack.row: 7}.
        let reply = nodes.tags_create(&birth, &me.node.node_id, &route, &labels(&["gold", "ssd"])).await.expect("node.tag.create answered");
        assert_eq!(reply, created(&[]));
        let config: std::collections::BTreeMap<String, String> = [("zone", "eu-1"), ("rack.row", "7")].into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let reply = NodeRpc::new(&client).config_update(&me.node.node_id, &route, &birth, config.clone(), &rdm_transport::CallOptions::default()).await.expect("node.config.update answered");
        assert!(matches!(reply, StatusReply::ConfigUpdated { .. }), "node.config.updated: {reply:?}");
        let got = one().await;
        assert_eq!((got.tags.clone(), got.config.clone()), (labels(&["gold", "ssd"]), config), "the row answers the labels and the config");

        // config.<key>: the key everything after the first dot.
        assert_eq!(matching(Filter::new().eq("config.zone", ["eu-1"])).await, mine, "[config.zone: eu-1] answers the admin");
        assert_eq!(matching(Filter::new().eq("config.rack.row", ["7"])).await, mine, "[config.rack.row: 7]: the dotted key addressed whole");
        assert!(matching(Filter::new().eq("config.zone", ["us-2"])).await.is_empty(), "[config.zone: us-2] answers nothing");
        assert!(matching(Filter::new().eq("config.region", ["eu-1"])).await.is_empty(), "a key the admin's config lacks matches nothing");
        // tags: all of, repeated; any of, within one pair.
        assert_eq!(matching(tags(&["gold"]).eq("tags", ["ssd"])).await, mine, "[tags: gold] [tags: ssd] (all of) answers the admin");
        assert!(matching(tags(&["gold"]).eq("tags", ["blue"])).await.is_empty(), "[tags: gold] [tags: blue] answers nothing: the admin carries no blue");
        assert_eq!(matching(tags(&["gold", "silver"])).await, mine, "[tags: gold, silver] (any of) answers the admin");
        assert!(matching(tags(&["blue", "silver"])).await.is_empty(), "[tags: blue, silver] answers nothing");

        // A label the node already carries is ignored and named.
        let reply = nodes.tags_create(&birth, &me.node.node_id, &route, &labels(&["ssd"])).await.expect("node.tag.create answered");
        assert_eq!(reply, created(&["ssd"]), "ssd is already carried");
        assert_eq!(one().await.tags, labels(&["gold", "ssd"]), "nothing changes");

        // node.tag.delete {gold, green}: green is not carried.
        let reply = nodes.tags_delete(&birth, &me.node.node_id, &route, &TagScope::Labels(labels(&["gold", "green"]))).await.expect("node.tag.delete answered");
        assert_eq!(reply, deleted(&["green"]));
        assert!(matching(tags(&["gold"])).await.is_empty(), "[tags: gold] answers nothing once gold is deleted");
        assert_eq!(matching(tags(&["ssd", "gold"])).await, mine, "[tags: ssd, gold] answers the admin once");
        assert_eq!(one().await.tags, labels(&["ssd"]), "the row answers the label left");

        // A create whose label is not a label, past the typed object's own check, is refused by the
        // mesh-primary naming it; a read whose tag value is not a label is refused naming the pair.
        let refused = NodeRpc::new(&client).tags_create(&me.node.node_id, &route, &birth, &labels(&["Gold"]), &rdm_transport::CallOptions::default()).await.expect("a typed reply");
        assert!(matches!(&refused, StatusReply::RejectedTags { reason } if reason.contains("\"Gold\"")), "refused naming the label: {refused:?}");
        match nodes.get(&tags(&["Gold"])).await {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains("(tags, [Gold])"), "refused naming the pair: {reason}: {detail}"),
            other => panic!("a tag value that is not a label is refused by name, not {other:?}"),
        }
        assert_eq!(one().await.tags, labels(&["ssd"]), "a refused create changes nothing");

        // node.tag.delete { All }.
        let reply = nodes.tags_delete(&birth, &me.node.node_id, &route, &TagScope::All).await.expect("node.tag.delete answered");
        assert_eq!(reply, deleted(&[]));
        assert!(one().await.tags.is_empty(), "the admin carries no label");
        assert!(matching(tags(&["ssd"])).await.is_empty(), "[tags: ssd] answers nothing");

        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    let named = |name: &str| spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, parent, f)| (parent.clone(), f.clone())).collect::<Vec<_>>();
    let at = |f: &std::collections::BTreeMap<String, String>, k: &str| f.get(k).cloned().unwrap_or_default();
    let creates: Vec<(String, String, String)> = named("rdm.node_admin.tag.create.via-writer").iter().map(|(_, f)| (at(f, "outcome"), at(f, "arrived"), at(f, "already_carried"))).collect();
    assert!(creates.contains(&("tags-created".into(), "gold,ssd".into(), "".into())), "the first create's labels arrived: {creates:?}");
    assert!(creates.contains(&("tags-created".into(), "".into(), "ssd".into())), "the second create wrote nothing new: {creates:?}");
    assert!(creates.iter().any(|(o, _, _)| o == "rejected-tags"), "the non-label create was refused: {creates:?}");
    let deletes: Vec<(String, String, String)> = named("rdm.node_admin.tag.delete.via-writer").iter().map(|(_, f)| (at(f, "outcome"), at(f, "left"), at(f, "not_carried"))).collect();
    assert!(deletes.contains(&("tags-deleted".into(), "gold".into(), "green".into())), "the named delete removed gold: {deletes:?}");
    assert!(deletes.contains(&("tags-deleted".into(), "ssd".into(), "".into())), "the All delete removed ssd: {deletes:?}");
    let reads: Vec<(String, String)> = named("rdm.node_admin.node.serve.via-get").iter().map(|(_, f)| (at(f, "pairs"), at(f, "nodes"))).collect();
    for (pairs, count) in [
        ("(config.zone, [eu-1])", "1"),
        ("(config.rack.row, [7])", "1"),
        ("(mesh, [mesh1]) (config.zone, [eu-1])", "1"),
        ("(mesh, [mesh2]) (config.zone, [eu-1])", "0"),
        ("(tags, [gold]) (tags, [ssd])", "1"),
        ("(tags, [gold]) (tags, [blue])", "0"),
        ("(tags, [gold, silver])", "1"),
        ("(tags, [blue, silver])", "0"),
    ] {
        assert!(reads.contains(&(pairs.to_string(), count.to_string())), "node.get {pairs} answered {count}: {reads:?}");
    }
}

/// CONTRACT (Q167 R-RD7: the node is `node.connection.get`'s address, the filter matches the connection
/// rows): an rpc node answers `node.connection.get { node }` of its exact birth itself, with exactly the
/// connections it holds (its latest Direct per destination that is connected and its active
/// Proxies), named by the minted ids `node.connection.delete` names, its route to its admin among
/// them; the answer names the node by the name it holds. `[destination: mesh1.admin.1] [kind:
/// direct]` answers the route to its admin alone, `[kind: proxy]` nothing, and `[state: open]` (no
/// state) is refused naming the pair. What must NOT happen: a control API in the path, or a
/// connection the node does not hold.
#[test]
fn an_rpc_node_answers_its_held_connections_by_the_ids_its_delete_names() {
    let cap = crate::common::capture("an_rpc_node_answers_its_held_connections_by_the_ids_its_delete_names");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = crate::common::admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::Ready, crate::common::serve_ping).await;
        let root = std::env::temp_dir().join(format!("node-reads-rpc-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let name: PathName = "mesh1.rpc.1".parse().unwrap();
        let dir = root.join(name.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(&dir).unwrap();
        let key = node::load_or_mint_key(&dir).unwrap();
        let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
            fabric: "fabric1".into(),
            fabric_id: fabric.clone(),
            name: name.clone(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
            seeds: vec![admin.seed.clone()],
            launcher: Some(admin.launcher.clone()),
            data_dir: dir,
            store_dir: store.clone(),
            mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
            issuing: None,
            roots: Vec::new(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
        };
        admin.deployed(&launch, &key);
        let seams: Arc<OnceLock<Seams>> = Arc::default();
        let slot = seams.clone();
        let _running = node::start_with_seams(&launch, move |b, s| {
            let _ = slot.set(s);
            b
        })
        .await
        .unwrap_or_else(|e| panic!("{name} comes up: {e}"));
        let seams = seams.get().cloned().expect("the node handed its seams");
        let digest_view = &admin.observer.topology_view.digest_view;
        let mut heard = digest_view.heard_changes();
        while digest_view.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::Ready) {
            heard.changed().await.unwrap();
        }
        // The node originates a data-plane connection to its admin: a Direct entry of its log.
        match seams.client.call::<rdm_core::ping::Ping>(&NodeTarget::ExactNode(admin.launcher.node_id.clone()), &rdm_core::ping::PingRequest::Ping { payload: b"the node reaches its admin".to_vec() }, &rdm_transport::CallOptions::default()).await.0 {
            rdm_core::outcome::RpcOutcome::Reply(_) => {}
            other => panic!("the node's data-plane ping did not end in a reply: {other:?}"),
        }
        seams.connections.observations_landed().await;

        let (d, _) = digest_view.get(launch.node_id.as_str()).unwrap();
        let rpc = ResolvedNode { node_id: d.node.node_id.clone(), name: d.node.name.clone(), endpoint_id: d.node.endpoint_id.0.parse().unwrap(), incarnation: d.node.incarnation.clone(), transport_addr: d.node.transport_addr };
        let (client, _endpoint) = caller(&[rpc]).await;
        let reads = Connections::new(BuildCarrier::new(client.clone(), "mesh1.admin.1".parse().unwrap()));
        let birth = ExactBirth { target: NodeTarget::ExactNode(launch.node_id.clone()), node_id: launch.node_id.clone(), incarnation: launch.incarnation.clone() };
        let connections = reads.get(&birth, &Filter::new()).await.unwrap_or_else(|e| panic!("node.connection.get of {name}: {e}"));
        let (complete, own) = {
            let held = seams.connections.held();
            let held = held.lock().unwrap();
            (held.is_complete(), own_ids(&held))
        };
        assert_eq!((connections.node.as_str(), connections.complete), ("mesh1.rpc.1", complete));
        assert_eq!(ids(&connections), own, "exactly the connections the node holds: {connections:#?}");
        let to_admin = connections.connections.iter().find(|c| c.destination == "mesh1.admin.1").unwrap_or_else(|| panic!("the node's route to its admin is listed: {connections:#?}"));
        assert_eq!((to_admin.source.as_str(), to_admin.kind.as_str(), to_admin.state.as_str()), ("mesh1.rpc.1", "direct", "connected"));
        let direct = reads.get(&birth, &Filter::new().eq("destination", ["mesh1.admin.1"]).eq("kind", ["direct"])).await.unwrap_or_else(|e| panic!("node.connection.get [destination] [kind] of {name}: {e}"));
        assert_eq!(ids(&direct), [to_admin.id.clone()].into(), "[destination: mesh1.admin.1] [kind: direct] is the route to its admin: {direct:#?}");
        assert!(reads.get(&birth, &Filter::new().eq("kind", ["proxy"])).await.unwrap_or_else(|e| panic!("node.connection.get [kind: proxy]: {e}")).connections.is_empty(), "the node holds no proxy");
        match reads.get(&birth, &Filter::new().eq("state", ["open"])).await {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains("(state, [open])"), "refused naming the pair: {reason}: {detail}"),
            other => panic!("a state no connection takes is refused by name, not {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    });
    assert!(spans.0.lock().unwrap().values().any(|(n, _, f)| n == "rdm.node_admin.node.serve.via-connection-get" && f.get("node").map(String::as_str) == Some("mesh1.rpc.1")), "node.connection.get served by the rpc node");
}

/// The fields of every span named `name` that `node` opened, as the cell's capture holds them.
fn spans_of(spans: &crate::common::Spans, name: &str) -> Vec<std::collections::BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, _, f)| f.clone()).collect()
}

// @feature: fabric-build
/// CONTRACT (Q167 R-RD5; node-rpc-envelope.md "Build, op `0x20`": `build.get` caller → fabric-primary;
/// Luke: "only FP reads the builds"): a Day-0 node-admin, mesh1.admin.1 (the fabric-primary), and
/// mesh1.admin.2, launched by it, both ready. A caller that enters at mesh1.admin.2 reads `build.get`:
/// mesh1.admin.2 serves no Build read, refusing it NotFabricPrimary naming mesh1.admin.1, and the
/// carrier follows to it. The fabric-primary answers from its own Build log: every Build it holds,
/// newest first, each whole, its current Build marked, header, topology (mesh1.admin.1's create spec)
/// and receipts as its log folds them. `[current: true]` is that Build alone, `[phase: failed]` and
/// `[id: another]` the empty list, and a pair naming no property of the Build header is refused
/// INVALID_ARGUMENT naming the pair. Each read is served by mesh1.admin.1 alone: its serve span names
/// `build.get` and ends `end`, and mesh1.admin.2's only Build spans are its refusals naming
/// mesh1.admin.1. Must NOT happen: a build.get served by an admin that is not the fabric-primary, the
/// current Build unmarked, or a Build answered without its topology or receipts.
#[test]
fn builds_get_is_served_by_the_fabric_primary_alone_each_build_whole_the_current_one_marked() {
    use crate::admin_reads_from_atlas::{day0_config as launch_day0, deployed, launch_by, until_ready};
    use rdm_admin::build_state::BuildStateAdapter as _;
    if crate::own_process::delegated(module_path!(), "builds_get_is_served_by_the_fabric_primary_alone_each_build_whole_the_current_one_marked") {
        return;
    }
    let cap = crate::common::capture("builds_get_is_served_by_the_fabric_primary_alone_each_build_whole_the_current_one_marked");
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let base = std::env::temp_dir().join(format!("builds-get-{}", NodeId::mint()));
        let (root_dir, admin_dir) = (base.join("admin1"), base.join("admin2"));
        std::fs::create_dir_all(&root_dir).unwrap();
        let root = start_with(launch_day0(&root_dir), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.expect("the Day-0 admin comes up");
        root.stop_reconciling().await;
        until_ready("the Day-0 admin", || root.digest.lock().unwrap().clone()).await;
        let launch = launch_by(&root, "mesh1.admin.2", &admin_dir, &root_dir);
        let _ = root.runner.joins.expect(deployed(&launch));
        let env = launch.to_env();
        let admin = start_with(AdminConfig::from_env(|k| env.get(k).cloned()).expect("the launch is a readable environment"), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() })
            .await
            .unwrap_or_else(|e| panic!("mesh1.admin.2 starts: {e}"));
        until_ready("mesh1.admin.2", || admin.digest.lock().unwrap().clone()).await;
        let (fp, other) = (root.digest.lock().unwrap().node.clone(), admin.digest.lock().unwrap().node.clone());
        let resolved = |n: &rdm_core::MeshNode| ResolvedNode { node_id: n.node_id.clone(), name: n.name.clone(), endpoint_id: n.endpoint_id.0.parse().unwrap(), incarnation: n.incarnation.clone(), transport_addr: n.transport_addr };
        let (client, _endpoint) = caller(&[resolved(&fp), resolved(&other)]).await;
        // The caller enters at mesh1.admin.2, an admin that is not the fabric-primary.
        let builds = rdm_client::Builds::new(BuildCarrier::new(client.clone(), other.name.clone()));
        let get = |f: Filter| {
            let builds = builds.clone();
            async move { tokio::time::timeout(WRONG, builds.get(&f)).await.unwrap_or_else(|_| panic!("build.get {f:?} answered nothing")) }
        };

        let held = root.control.builds.current().await.expect("the fabric-primary's Build log reads").expect("a current Build");
        let every = get(Filter::new()).await.unwrap_or_else(|e| panic!("build.get []: {e}"));
        let newest_first = root.control.builds.accepted_newest_first().await.expect("the acceptance order reads");
        assert_eq!(every.iter().map(|b| b.build_id.clone()).collect::<Vec<_>>(), newest_first, "every Build the fabric-primary holds, newest first");
        let current: Vec<&rdm_client::Build> = every.iter().filter(|b| b.current).collect();
        assert_eq!(current.iter().map(|b| b.build_id.clone()).collect::<Vec<_>>(), vec![held.build_id.clone()], "the current Build alone is marked");
        let got = current[0];
        assert_eq!((got.attempt, got.executor.clone(), got.last_failure.clone(), got.steps.len()), (held.attempt, held.executor.clone(), held.last_failure.clone(), held.steps.len()), "the header and receipts are the log's: {got:#?}");
        let specs: Vec<rdm_core::meta::NodeSpec> = held.topology.meshes.values().flat_map(|m| m.spec_by_path.values().cloned()).collect();
        assert_eq!(got.nodes, specs, "the topology is every node's create spec");
        assert!(got.nodes.iter().any(|s| s.path == fp.name), "the Day-0 Build names mesh1.admin.1: {:?}", got.nodes);

        let one = get(Filter::new().eq("current", ["true"])).await.unwrap_or_else(|e| panic!("build.get [current: true]: {e}"));
        assert_eq!(one.iter().map(|b| (b.build_id.clone(), b.current)).collect::<Vec<_>>(), vec![(held.build_id.clone(), true)], "[current: true] is the current Build alone");
        assert!(get(Filter::new().eq("phase", ["failed"])).await.unwrap().is_empty(), "[phase: failed]: no Build failed");
        assert!(get(Filter::new().eq("id", [rdm_core::BuildId::mint().to_string()])).await.unwrap().is_empty(), "[id: another] is the empty list");
        match get(Filter::new().eq("zone", ["eu"])).await {
            Err(CallEnd::Rejected { reason, detail }) => assert!(reason == "rejected-filter" && detail.contains("the pair (zone, [eu])"), "refused naming the pair: {reason}: {detail}"),
            other => panic!("a pair naming no Build header property is refused by name, not {other:?}"),
        }

        // Served by the fabric-primary alone; mesh1.admin.2 only refused, naming it.
        let serves: Vec<_> = spans_of(&spans, "rdm.node_admin.build.serve.via-node-rpc").into_iter().filter(|f| f.get("call").map(String::as_str) == Some("build.get")).collect();
        assert!(!serves.is_empty(), "build.get was served");
        for s in &serves {
            let outcome = s.get("outcome").map(String::as_str).unwrap_or_default();
            match s.get("node").map(String::as_str) {
                Some("mesh1.admin.1") => assert!(outcome == "end" || outcome == "rejected-filter", "the fabric-primary's serve ends the read: {s:?}"),
                Some("mesh1.admin.2") => assert_eq!(outcome, "not-fabric-primary", "mesh1.admin.2 serves no Build read: {s:?}"),
                other => panic!("a build.get served by {other:?}: {s:?}"),
            }
        }
        assert_eq!(serves.iter().filter(|s| s.get("node").map(String::as_str) == Some("mesh1.admin.1")).count(), 5, "the fabric-primary served each of the five reads: {serves:#?}");
        let refused = spans_of(&spans, "rdm.node_admin.build.reject.via-not-fabric-primary");
        assert!(refused.iter().filter(|f| f.get("route").map(String::as_str) == Some("build.get")).all(|f| f.get("node").map(String::as_str) == Some("mesh1.admin.2") && f.get("fabric_primary").map(String::as_str) == Some("mesh1.admin.1")), "each refusal is mesh1.admin.2's, naming mesh1.admin.1: {refused:#?}");

        admin.leave().await;
        root.leave().await;
        let _ = std::fs::remove_dir_all(&base);
    })
}
