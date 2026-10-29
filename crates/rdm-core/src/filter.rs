//! The reads' one filter (Q167, R-RD1 to R-RD3; node-rpc-envelope.md "Typed objects mapped to wire
//! and execution owners"): one read per entity, plural (`fabric.get`, `mesh.get`, `node.get`,
//! `node.connection.get`), each taking one [`Filter`] and answering a list in a stable order.
//!
//! A filter is a list of `(property, values)` pairs. Pairs are AND-combined, a repeated property is
//! AND (all of these tags), and the values within one pair are any-of. The property is a public field
//! of the entity's row by its page name, with one dot into the one map a row has: `config.<key>`, the
//! key being everything after the first dot. A set field matches when it carries the value. Values are
//! strings: a boolean is `true` or `false`, an enum its page name, a number its decimal digits, an id
//! its canonical text; a string field matches exactly, and `""` matches an empty string, never an
//! absent one. An empty filter is no filter; an empty values list is refused. Validation is against
//! the entity's schema, never a row ([`Filter::refusal`]): an unknown property, a value the field
//! cannot take or an empty values list is refused naming the pair; a property the schema has and one
//! row lacks (a config entry) does not match that row. Nothing matching is the empty list.

use crate::digest::MeshDigest;
use crate::views::{ConnectionView, FabricView, MeshView, NodeView};
use serde::{Deserialize, Serialize};

/// The entity a read reads (R-RD1). Positional order is part of the wire schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Entity {
    /// `fabric.get`: the fabric the answering admin holds.
    ///
    /// Position 0 on the wire.
    Fabrics,
    /// `mesh.get`: the meshes the answering admin holds.
    ///
    /// Position 1 on the wire.
    Meshes,
    /// `node.get`: the node rows the answering admin holds.
    ///
    /// Position 2 on the wire.
    Nodes,
    /// `node.connection.get { node }`: the exact birth's own connections.
    ///
    /// Position 3 on the wire.
    Connections,
}

impl Entity {
    /// The read, by its name in node-rpc-envelope.md.
    pub fn read(self) -> &'static str {
        match self {
            Self::Fabrics => "fabric.get",
            Self::Meshes => "mesh.get",
            Self::Nodes => "node.get",
            Self::Connections => "node.connection.get",
        }
    }

    /// The row the read answers, by its name in refusals.
    fn row(self) -> &'static str {
        match self {
            Self::Fabrics => "fabric",
            Self::Meshes => "mesh",
            Self::Nodes => "node",
            Self::Connections => "connection",
        }
    }

    /// The properties a pair may name on this entity's row.
    pub fn schema(self) -> &'static [Property] {
        match self {
            Self::Fabrics => FABRIC,
            Self::Meshes => MESH,
            Self::Nodes => NODE,
            Self::Connections => CONNECTION,
        }
    }

    /// The one map a row of this entity has, addressed `<map>.<key>`.
    pub fn map(self) -> Option<&'static str> {
        match self {
            Self::Nodes => Some("config"),
            _ => None,
        }
    }
}

/// What a property's values may be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Takes {
    /// Any string.
    Text,
    /// `true` or `false`.
    Bool,
    /// A canonical id (12 lowercase Crockford characters).
    Id,
    /// Decimal digits.
    Number,
    /// A tag label ([`crate::status::tag_refusal`]).
    Label,
    /// One of these page names.
    OneOf(&'static [&'static str]),
}

/// One property of an entity's row, by its page name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Property {
    /// The property's page name.
    pub name: &'static str,
    /// What its values may be.
    pub takes: Takes,
}

const fn p(name: &'static str, takes: Takes) -> Property {
    Property { name, takes }
}

/// A node's lifecycle states, by their page names.
pub const NODE_STATUSES: &[&str] = &[
    "pending", "ready", "connections-deleting", "leaving", "pending-reconnect", "dead", "deleted", "starting",
    "connections-deleted", "stopping", "stopped", "deleting", "left",
];
/// A mesh's or the fabric's lifecycle states, by their page names.
pub const SCOPE_STATUSES: &[&str] = &["pending", "ready", "leaving", "dead", "degraded"];
/// The node kinds, by their `path.name` segment.
pub const KINDS: &[&str] = &["admin", "rpc", "broker", "gateway", "compute"];
/// The deployment providers.
pub const PROVIDERS: &[&str] = &["process", "container"];
/// A node's own storage (storage.md §1).
pub const PERSISTENCES: &[&str] = &["ephemeral", "persistent"];

const NODE: &[Property] = &[
    p("id", Takes::Id),
    p("incarnation", Takes::Id),
    p("endpoint", Takes::Text),
    p("transport_addr", Takes::Text),
    p("status", Takes::OneOf(NODE_STATUSES)),
    p("path", Takes::Text),
    p("mesh", Takes::Text),
    p("kind", Takes::OneOf(KINDS)),
    p("node_type", Takes::Text),
    p("name", Takes::Text),
    p("tags", Takes::Label),
    p("types", Takes::Text),
    p("held", Takes::Text),
    p("held_hash", Takes::Text),
    p("is_primary", Takes::Bool),
    p("is_fabric_primary", Takes::Bool),
    p("stopped", Takes::Bool),
    p("declared", Takes::Text),
    p("executable", Takes::Text),
    p("provider", Takes::OneOf(PROVIDERS)),
    p("deployment_id", Takes::Text),
    p("data_dir", Takes::Text),
    p("admin_api_base", Takes::Text),
    p("listeners", Takes::Text),
    p("storage_persistence", Takes::OneOf(PERSISTENCES)),
    p("topology_storage_provider", Takes::Text),
];

const MESH: &[Property] = &[
    p("id", Takes::Id),
    p("name", Takes::Text),
    p("status", Takes::OneOf(SCOPE_STATUSES)),
    p("primary_admin", Takes::Text),
    p("admin_api_base", Takes::Text),
    p("nodes", Takes::Text),
];

const FABRIC: &[Property] = &[
    p("id", Takes::Id),
    p("name", Takes::Text),
    p("status", Takes::OneOf(SCOPE_STATUSES)),
    p("provider", Takes::OneOf(PROVIDERS)),
    p("fabric_primary", Takes::Text),
    p("admin_api_base", Takes::Text),
    p("build_id", Takes::Id),
    p("meshes", Takes::Text),
    p("topology_storage_provider", Takes::Text),
];

const CONNECTION: &[Property] = &[
    p("id", Takes::Id),
    p("source", Takes::Text),
    p("destination", Takes::Text),
    p("kind", Takes::OneOf(&["direct", "proxy"])),
    p("state", Takes::OneOf(&["connected", "disconnected", "failed"])),
    p("carrier", Takes::Text),
    p("reason", Takes::Text),
    p("logged_at_ms", Takes::Number),
];

/// A Build's phases, by their page names (`BuildPhase`, lowercase).
pub const BUILD_PHASES: &[&str] = &["pending", "running", "complete", "failed"];

/// The Build header's properties (Q167 R-RD5): what `build.get` filters on. Its topology and receipts
/// are the Build's content, never a property.
const BUILD_HEADER: &[Property] = &[
    p("id", Takes::Id),
    p("current", Takes::Bool),
    p("phase", Takes::OneOf(BUILD_PHASES)),
    p("attempt", Takes::Number),
    p("executor", Takes::Text),
];

/// The node properties the topology a node holds carries for every node it hears, the gossiped digest
/// (query.md §2; Q203: gossip carries what routing needs and nothing else): what a node's local
/// `nodes_get` may name. Every other node property is the row's, read with `node.get`.
pub const GOSSIPED: &[&str] = &["id", "incarnation", "endpoint", "transport_addr", "status", "path", "mesh", "kind", "node_type", "name", "is_primary", "is_fabric_primary"];

/// A row's value of one property: one string, a set (matched when it carries a value), or absent
/// (matching nothing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// One value.
    One(String),
    /// A set or a list of values.
    Set(Vec<String>),
    /// The row lacks the property.
    Absent,
}

impl Value {
    fn of(v: Option<impl ToString>) -> Self {
        v.map_or(Self::Absent, |v| Self::One(v.to_string()))
    }

    fn set<T: ToString>(v: impl IntoIterator<Item = T>) -> Self {
        Self::Set(v.into_iter().map(|v| v.to_string()).collect())
    }

    fn bool(b: bool) -> Self {
        Self::One(b.to_string())
    }
}

/// A row a filter is matched against: its value of each property its entity's schema names.
pub trait Row {
    /// The row's value of `property`, a schema name or `<map>.<key>`.
    fn value(&self, property: &str) -> Value;
}

/// A serde-named enum's page name (`ready`).
fn page_name(v: &impl Serialize) -> Option<String> {
    serde_json::to_value(v).ok()?.as_str().map(str::to_string)
}

impl Row for NodeView {
    fn value(&self, property: &str) -> Value {
        if let Some(key) = property.strip_prefix("config.") {
            return Value::of(self.config.get(key));
        }
        match property {
            "id" => Value::One(self.node_id.to_string()),
            "incarnation" => Value::of(self.incarnation_id.as_ref()),
            "endpoint" => Value::of(self.endpoint_id.as_ref()),
            "transport_addr" => Value::of(self.transport_addr),
            "status" => Value::of(page_name(&self.status)),
            "path" => Value::One(self.path()),
            "mesh" => Value::One(self.mesh.clone()),
            "kind" => Value::One(self.kind.segment().to_string()),
            "node_type" => Value::One(self.node_type().to_string()),
            "name" => Value::One(self.node_name()),
            "tags" => Value::set(&self.tags),
            "types" => Value::set(&self.types),
            "held" => Value::set(&self.held),
            "held_hash" => Value::One(self.held_hash.iter().map(|b| format!("{b:02x}")).collect()),
            "is_primary" => Value::bool(self.is_primary),
            "is_fabric_primary" => Value::bool(self.is_fabric_primary),
            "stopped" => Value::bool(self.stopped),
            "declared" => Value::of(self.declared.as_ref()),
            "executable" => Value::of(self.executable.as_ref().map(|b| b.executable.display().to_string())),
            "provider" => Value::of(self.provider.as_ref().and_then(page_name)),
            "deployment_id" => Value::of(self.deployment_id.as_ref()),
            "data_dir" => Value::of(self.data_dir.as_ref()),
            "admin_api_base" => Value::of(self.admin_api_base.as_ref()),
            "listeners" => Value::set(self.listeners.iter().map(|(name, _)| name)),
            "storage_persistence" => Value::of(self.storage_persistence.as_ref().map(|s| match s {
                crate::meta::StorageMeta::Ephemeral => "ephemeral",
                crate::meta::StorageMeta::Persistent { .. } => "persistent",
            })),
            "topology_storage_provider" => Value::of(self.topology_storage_provider.as_ref()),
            _ => Value::Absent,
        }
    }
}

/// A gossiped digest, the row of a node's local query (query.md §2), by the node schema's names: the
/// [`GOSSIPED`] properties only; every other property is absent from it.
impl Row for MeshDigest {
    fn value(&self, property: &str) -> Value {
        let name = &self.node.name;
        match property {
            "id" => Value::One(self.node.node_id.to_string()),
            "incarnation" => Value::One(self.node.incarnation.as_str().to_string()),
            "endpoint" => Value::One(self.node.endpoint_id.0.clone()),
            "transport_addr" => Value::One(self.node.transport_addr.to_string()),
            "status" => Value::of(page_name(&self.status)),
            "path" => Value::One(name.path()),
            "mesh" => Value::One(name.mesh.clone()),
            "kind" => Value::One(name.kind.segment().to_string()),
            "node_type" => Value::One(name.node_type().to_string()),
            "name" => Value::One(name.name()),
            "is_primary" => Value::bool(self.is_primary.is_some()),
            "is_fabric_primary" => Value::bool(self.is_fabric_primary.is_some()),
            _ => Value::Absent,
        }
    }
}

impl Row for MeshView {
    fn value(&self, property: &str) -> Value {
        match property {
            "id" => Value::of(self.id.as_ref()),
            "name" => Value::One(self.name.clone()),
            "status" => Value::of(page_name(&self.status)),
            "primary_admin" => Value::of(self.primary_admin.as_ref()),
            "admin_api_base" => Value::of(self.admin_api_base.as_ref()),
            "nodes" => Value::set(&self.nodes),
            _ => Value::Absent,
        }
    }
}

impl Row for FabricView {
    fn value(&self, property: &str) -> Value {
        match property {
            "id" => Value::One(self.id.to_string()),
            "name" => Value::One(self.name.clone()),
            "status" => Value::of(page_name(&self.status)),
            "provider" => Value::of(page_name(&self.provider)),
            "fabric_primary" => Value::of(self.fabric_primary.as_ref()),
            "admin_api_base" => Value::of(self.admin_api_base.as_ref()),
            "build_id" => Value::of(self.build_id.as_ref()),
            "meshes" => Value::set(self.meshes.iter().map(|m| &m.name)),
            "topology_storage_provider" => Value::of(self.topology_storage_provider.as_ref()),
            _ => Value::Absent,
        }
    }
}

impl Row for crate::build::BuildHeader {
    fn value(&self, property: &str) -> Value {
        match property {
            "id" => Value::One(self.build_id.clone()),
            "current" => Value::bool(self.current),
            "phase" => Value::One(self.phase.page_name().to_string()),
            "attempt" => Value::One(self.attempt.to_string()),
            "executor" => Value::of(self.executor.as_ref()),
            _ => Value::Absent,
        }
    }
}

impl Row for ConnectionView {
    fn value(&self, property: &str) -> Value {
        match property {
            "id" => Value::One(self.id.clone()),
            "source" => Value::One(self.source.clone()),
            "destination" => Value::One(self.destination.clone()),
            "kind" => Value::One(self.kind.clone()),
            "state" => Value::One(self.state.clone()),
            "carrier" => Value::of(self.carrier.as_ref()),
            "reason" => Value::of(self.reason.as_ref()),
            "logged_at_ms" => Value::One(self.logged_at_ms.to_string()),
            _ => Value::Absent,
        }
    }
}

/// A read's filter (R-RD2): `(property, values)` pairs, AND across pairs, any-of within. Empty is no
/// filter. On the wire as the pair list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    /// The pairs, in the caller's order.
    pub pairs: Vec<(String, Vec<String>)>,
}

impl Filter {
    /// No filter: every row.
    pub fn new() -> Self {
        Self::default()
    }

    /// This filter and one more pair: rows whose `property` is any of `values`.
    pub fn eq<S: Into<String>>(mut self, property: &str, values: impl IntoIterator<Item = S>) -> Self {
        self.pairs.push((property.to_string(), values.into_iter().map(Into::into).collect()));
        self
    }

    /// The properties the pairs name, in order, comma-joined (a read's span field).
    pub fn properties(&self) -> String {
        self.pairs.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>().join(",")
    }

    /// Why this filter is refused for `entity`, naming the pair, or `None` when every pair names a
    /// property of the entity's row and values it can take. Read against the schema, never a row.
    pub fn refusal(&self, entity: Entity) -> Option<String> {
        self.refusal_in(&entity.of())
    }

    /// Why this filter is refused for `build.get` (Q167 R-RD5), naming the pair, or `None` when every
    /// pair names a property of the Build header (`BUILD_HEADER`: `id`, `current`, `phase`,
    /// `attempt`, `executor`) and values it can take. Read against the schema, never a Build.
    pub fn builds_refusal(&self) -> Option<String> {
        self.refusal_in(&BUILDS)
    }

    fn refusal_in(&self, schema: &Schema) -> Option<String> {
        self.pairs.iter().find_map(|(property, values)| pair_refusal(schema, property, values).map(|why| format!("{}: the pair {} {why}", schema.read, pair(property, values))))
    }

    /// Why this filter is refused for a node's local query of the topology it holds (query.md §2,
    /// Q232), naming the pair: a pair the node schema refuses ([`Self::refusal`]), or one naming a
    /// property of the node's row that gossip does not carry ([`GOSSIPED`]: `tags`, `config.<key>`,
    /// `executable`, ...), named with `node.get`, the read that answers it. Read against the schema,
    /// never a row: a row-only property is refused whatever rows would have matched.
    pub fn local_refusal(&self) -> Option<String> {
        self.refusal(Entity::Nodes).or_else(|| {
            self.pairs.iter().find(|(property, _)| !GOSSIPED.contains(&property.as_str())).map(|(property, values)| {
                format!(
                    "the topology a node holds: the pair {} names {property}, a property of the node's row that gossip does not carry (its properties: {}); read it with {}",
                    pair(property, values),
                    GOSSIPED.join(", "),
                    Entity::Nodes.read()
                )
            })
        })
    }

    /// Whether `row` matches every pair. A filter is read through [`Self::refusal`] first.
    pub fn matches(&self, row: &impl Row) -> bool {
        self.pairs.iter().all(|(property, values)| match row.value(property) {
            Value::One(v) => values.contains(&v),
            Value::Set(set) => values.iter().any(|v| set.contains(v)),
            Value::Absent => false,
        })
    }
}

/// A pair as a refusal names it: `(kind, [gateway, broker])`.
fn pair(property: &str, values: &[String]) -> String {
    format!("({property}, [{}])", values.join(", "))
}

/// What a filter is validated against: the read's name, its row's name in refusals, the row's
/// properties and its one map.
struct Schema {
    read: &'static str,
    row: &'static str,
    properties: &'static [Property],
    map: Option<&'static str>,
}

impl Entity {
    fn of(self) -> Schema {
        Schema { read: self.read(), row: self.row(), properties: self.schema(), map: self.map() }
    }
}

/// The Build header's schema (Q167 R-RD5): what a `build.get` pair (Build 0x20 `GetBuilds`) may name.
const BUILDS: Schema = Schema { read: "build.get", row: "Build header", properties: BUILD_HEADER, map: None };

fn pair_refusal(schema: &Schema, property: &str, values: &[String]) -> Option<String> {
    if values.is_empty() {
        return Some("names no value: a pair needs at least one".into());
    }
    if let Some(map) = schema.map {
        if property.strip_prefix(map).is_some_and(|rest| rest.starts_with('.')) {
            return None;
        }
    }
    let Some(prop) = schema.properties.iter().find(|p| p.name == property) else {
        let map = schema.map.map(|m| format!(", nor {m}.<key>")).unwrap_or_default();
        return Some(format!("names no property of a {} row (its properties: {}{map})", schema.row, schema.properties.iter().map(|p| p.name).collect::<Vec<_>>().join(", ")));
    };
    values.iter().find_map(|v| value_refusal(prop.takes, v).map(|why| format!("names {v:?}, which {property} cannot take: {why}")))
}

fn value_refusal(takes: Takes, v: &str) -> Option<String> {
    match takes {
        Takes::Text => None,
        Takes::Bool => (v != "true" && v != "false").then(|| "a boolean is true or false".to_string()),
        Takes::Id => crate::ids::decode_crockford60(v).is_none().then(|| format!("an id is {} lowercase Crockford characters", crate::CROCKFORD60_LEN)),
        Takes::Number => (v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit())).then(|| "a number is decimal digits".to_string()),
        Takes::Label => crate::status::tag_refusal(v),
        Takes::OneOf(names) => (!names.contains(&v)).then(|| format!("it is one of {}", names.join(", "))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::views::{NodeStatus, ScopeStatus};
    use crate::{NodeId, NodeKind};

    fn node(name: &str, kind: NodeKind, tags: &[&str], config: &[(&str, &str)]) -> NodeView {
        let mut v: NodeView = serde_json::from_value(serde_json::json!({
            "name": name, "kind": kind, "mesh": name.split('.').next().unwrap(), "node_id": NodeId::mint(), "endpoint_id": null,
            "incarnation_id": null, "deployment_id": null, "provider": "process", "data_dir": null, "status": "ready",
            "is_primary": false, "is_fabric_primary": false, "admin_api_base": null, "transport_addr": null, "listeners": [],
            "declared": null, "stopped": false, "load": null, "gossip": null, "tags": [], "config": {}, "data_dir_lost": null,
            "data_dir_unreadable": null, "types": [], "held": [], "held_hash": vec![0u8; 32], "cert": null, "executable": null,
            "storage_persistence": null, "topology_storage_provider": null,
        }))
        .expect("a node row");
        v.tags = tags.iter().map(|t| t.to_string()).collect();
        v.config = config.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        v
    }

    fn names(rows: &[NodeView], f: &Filter) -> Vec<String> {
        assert_eq!(f.refusal(Entity::Nodes), None, "{f:?}");
        rows.iter().filter(|r| f.matches(*r)).map(|r| r.name.to_string()).collect()
    }

    /// CONTRACT (Q167 R-RD2): a filter's pairs AND, a repeated property ANDs, the values of one pair
    /// are any-of, a set field (tags) matches when it carries a value, `config.<key>` reads the config
    /// map with the key everything after the first dot (a dotted key addressed whole), a config key one
    /// row lacks does not match that row and matches the others, `""` matches an empty config value
    /// and never an absent one, an enum (kind, status) matches its page name, a boolean `true` or
    /// `false`, an id its canonical text, and the empty filter matches every row. What must NOT happen:
    /// any-of read as all-of, a repeated property read as any-of, an absent key matching `""`.
    #[test]
    fn a_filter_ands_its_pairs_ors_a_pairs_values_and_reads_config_by_its_key() {
        let gold = node("mesh1.gateway.1", NodeKind::Gateway, &["gold", "ssd"], &[("zone", "eu-1"), ("rack.row", "7"), ("note", "")]);
        let silver = node("mesh1.broker.1", NodeKind::Broker, &["silver"], &[("zone", "us-2")]);
        let other = node("mesh2.gateway.1", NodeKind::Gateway, &["gold"], &[]);
        let rows = [gold.clone(), silver.clone(), other.clone()];
        assert_eq!(names(&rows, &Filter::new()), ["mesh1.gateway.1", "mesh1.broker.1", "mesh2.gateway.1"], "no pair is every row");
        assert_eq!(names(&rows, &Filter::new().eq("config.zone", ["eu-1"])), ["mesh1.gateway.1"]);
        assert_eq!(names(&rows, &Filter::new().eq("config.rack.row", ["7"])), ["mesh1.gateway.1"], "the key is everything after the first dot");
        assert_eq!(names(&rows, &Filter::new().eq("config.zone", ["eu-1", "us-2"])), ["mesh1.gateway.1", "mesh1.broker.1"], "a key mesh2.gateway.1 lacks matches the others");
        assert_eq!(names(&rows, &Filter::new().eq("config.note", [""])), ["mesh1.gateway.1"], "\"\" matches an empty value, never an absent one");
        assert_eq!(names(&rows, &Filter::new().eq("tags", ["gold"]).eq("tags", ["ssd"])), ["mesh1.gateway.1"], "a repeated property is all of");
        assert_eq!(names(&rows, &Filter::new().eq("tags", ["gold", "silver"])), ["mesh1.gateway.1", "mesh1.broker.1", "mesh2.gateway.1"], "one pair's values are any of");
        assert_eq!(names(&rows, &Filter::new().eq("mesh", ["mesh1"]).eq("kind", ["gateway"])), ["mesh1.gateway.1"], "pairs are AND");
        assert!(names(&rows, &Filter::new().eq("mesh", ["mesh9"])).is_empty(), "a mesh no row is in matches nothing");
        assert_eq!(names(&rows, &Filter::new().eq("status", ["ready"]).eq("is_primary", ["false"])).len(), 3);
        assert_eq!(names(&rows, &Filter::new().eq("id", [silver.node_id.to_string()])), ["mesh1.broker.1"]);
        assert_eq!(names(&rows, &Filter::new().eq("path", ["mesh1.gateway"]).eq("name", ["1"])), ["mesh1.gateway.1"], "path and name are the page's two fields");
        assert!(names(&rows, &Filter::new().eq("incarnation", [NodeId::mint().to_string()])).is_empty(), "a row lacking the property does not match");
        let _ = (NodeStatus::Pending, ScopeStatus::Pending);
    }

    /// CONTRACT (Q167 R-RD2): a filter is validated against the entity's schema, never a row, and a
    /// refusal names the pair: an unknown property (`zone` without `config.`), a value the field cannot
    /// take (a boolean spelt `yes`, a kind that is no kind, a non-canonical id, a label outside the
    /// label grammar, a number with a letter) and an empty values list are refused; `config.<key>` is
    /// a property of a node and of no other entity. What must NOT happen: an unknown property read as
    /// matching nothing, or a refusal that does not name the pair.
    #[test]
    fn a_filter_naming_an_unknown_property_an_impossible_value_or_no_value_is_refused_naming_the_pair() {
        let refused = |e: Entity, f: Filter, wanted: &str| {
            let why = f.refusal(e).unwrap_or_else(|| panic!("{f:?} on {e:?} is refused"));
            assert!(why.contains(wanted), "{why:?} names {wanted:?}");
        };
        refused(Entity::Nodes, Filter::new().eq("zone", ["eu-1"]), "the pair (zone, [eu-1]) names no property of a node row");
        refused(Entity::Nodes, Filter::new().eq("kind", ["gateway"]).eq("is_primary", ["yes"]), "the pair (is_primary, [yes]) names \"yes\", which is_primary cannot take: a boolean is true or false");
        refused(Entity::Nodes, Filter::new().eq("kind", ["gatway"]), "the pair (kind, [gatway])");
        refused(Entity::Nodes, Filter::new().eq("id", ["NOT-AN-ID"]), "the pair (id, [NOT-AN-ID])");
        refused(Entity::Nodes, Filter::new().eq("tags", ["Gold"]), "the pair (tags, [Gold])");
        refused(Entity::Nodes, Filter::new().eq("mesh", Vec::<String>::new()), "the pair (mesh, []) names no value");
        refused(Entity::Meshes, Filter::new().eq("config.zone", ["eu-1"]), "the pair (config.zone, [eu-1]) names no property of a mesh row");
        refused(Entity::Connections, Filter::new().eq("logged_at_ms", ["12a"]), "a number is decimal digits");
        refused(Entity::Fabrics, Filter::new().eq("status", ["bogus"]), "the pair (status, [bogus])");
        assert_eq!(Filter::new().eq("config.anything.at.all", ["x"]).refusal(Entity::Nodes), None);
        assert_eq!(Filter::new().eq("status", ["ready"]).eq("name", ["mesh1"]).refusal(Entity::Meshes), None);
        assert_eq!(Filter::new().eq("state", ["connected"]).eq("kind", ["direct", "proxy"]).refusal(Entity::Connections), None);
    }

    fn digest(name: &str, status: crate::MemberStatus, is_primary: bool) -> MeshDigest {
        MeshDigest {
            held_hash: Default::default(),
            fabric_id: crate::FabricId::mint(),
            node: crate::MeshNode {
                node_id: NodeId::mint(),
                name: name.parse().unwrap(),
                endpoint_id: crate::EndpointId(format!("key-{name}")),
                transport_addr: "127.0.0.1:20001".parse().unwrap(),
                incarnation: crate::IncarnationId::mint(),
                supersedes: None,
                runtime: None,
            },
            status,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 1,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: is_primary.then_some(1),
            is_fabric_primary: None,
        }
    }

    /// CONTRACT (query.md §2; Q232): a node's local query matches the digests it holds with the same
    /// matcher and the node schema's names: `[kind: broker] [status: ready]` the ready
    /// broker alone, `[mesh: mesh1] [is_primary: true]` the mesh's primary, `[id: x]` that node; the
    /// gossiped properties pass the local check. A pair naming a row-only property (`tags`,
    /// `config.<key>`, `executable`) is refused naming the pair and `node.get`, whatever digests would
    /// have matched; a pair the node schema refuses is refused as `node.get` refuses it. Must NOT
    /// happen: a row-only property answered as no match, or a refusal that does not say where to read it.
    #[test]
    fn a_local_query_matches_the_digests_held_and_refuses_a_row_only_property_naming_nodes_get() {
        use crate::MemberStatus as M;
        let ready = digest("mesh1.broker.1", M::Ready, false);
        let pending = digest("mesh1.broker.2", M::Pending, false);
        let admin = digest("mesh1.admin.1", M::Ready, true);
        let held = [ready.clone(), pending.clone(), admin.clone()];
        let local = |f: &Filter| -> Vec<String> {
            assert_eq!(f.local_refusal(), None, "{f:?}");
            held.iter().filter(|d| f.matches(*d)).map(|d| d.node.name.to_string()).collect()
        };
        assert_eq!(local(&Filter::new().eq("kind", ["broker"]).eq("status", ["ready"])), ["mesh1.broker.1"]);
        assert_eq!(local(&Filter::new().eq("mesh", ["mesh1"]).eq("is_primary", ["true"])), ["mesh1.admin.1"]);
        assert_eq!(local(&Filter::new().eq("id", [pending.node.node_id.to_string()])), ["mesh1.broker.2"]);
        assert_eq!(local(&Filter::new().eq("path", ["mesh1.broker"]).eq("name", ["2"])), ["mesh1.broker.2"]);
        assert_eq!(local(&Filter::new().eq("incarnation", [ready.node.incarnation.as_str().to_string()]).eq("endpoint", ["key-mesh1.broker.1"]).eq("transport_addr", ["127.0.0.1:20001"])), ["mesh1.broker.1"]);
        assert_eq!(local(&Filter::new()).len(), 3, "no pair: every node held");
        for (f, pair) in [
            (Filter::new().eq("kind", ["broker"]).eq("tags", ["gold"]), "the pair (tags, [gold]) names tags"),
            (Filter::new().eq("config.zone", ["eu-1"]), "the pair (config.zone, [eu-1]) names config.zone"),
            (Filter::new().eq("executable", ["/bin/rpc"]), "the pair (executable, [/bin/rpc]) names executable"),
        ] {
            let why = f.local_refusal().unwrap_or_else(|| panic!("{f:?} is refused locally"));
            assert!(why.contains(pair) && why.contains("read it with node.get"), "{why:?} names {pair:?} and node.get");
            assert_eq!(f.refusal(Entity::Nodes), None, "node.get takes {f:?}");
        }
        let why = Filter::new().eq("kind", ["gatway"]).local_refusal().expect("a value the field cannot take");
        assert!(why.starts_with("node.get: the pair (kind, [gatway])"), "{why:?}");
    }

    /// CONTRACT (Q167 R-RD5): `build.get` filters on the Build header (`id`, `current`, `phase`,
    /// `attempt`, `executor`): a phase by its lowercase page name, `current` a boolean, `attempt` its
    /// digits, an executor absent from a Build no executor holds; a pair naming anything else (a
    /// topology or receipt field, a node property) or a value the field cannot take is refused naming
    /// the pair and `build.get`. Must NOT happen: a phase spelt `Failed` accepted, or `tags` read as
    /// matching nothing.
    #[test]
    fn a_builds_get_filter_matches_the_build_header_and_refuses_anything_else_naming_the_pair() {
        use crate::build::{BuildHeader, BuildPhase};
        let header = |id: &str, phase, current, executor: Option<&str>| BuildHeader { build_id: id.into(), phase, attempt: 2, executor: executor.map(Into::into), reason: "requested".into(), last_failure: None, current };
        let (a, b) = (crate::BuildId::mint().to_string(), crate::BuildId::mint().to_string());
        let rows = [header(&a, BuildPhase::Failed, false, None), header(&b, BuildPhase::Complete, true, Some("mesh1.admin.1"))];
        let ids = |f: Filter| -> Vec<String> {
            assert_eq!(f.builds_refusal(), None, "{f:?}");
            rows.iter().filter(|r| f.matches(*r)).map(|r| r.build_id.clone()).collect()
        };
        assert_eq!(ids(Filter::new()), [a.clone(), b.clone()]);
        assert_eq!(ids(Filter::new().eq("current", ["true"])), [b.clone()]);
        assert_eq!(ids(Filter::new().eq("phase", ["failed", "pending"])), [a.clone()]);
        assert_eq!(ids(Filter::new().eq("id", [a.clone()]).eq("attempt", ["2"])), [a.clone()]);
        assert_eq!(ids(Filter::new().eq("executor", ["mesh1.admin.1"])), [b.clone()], "a Build no executor holds lacks the property");
        for (f, wanted) in [
            (Filter::new().eq("phase", ["Failed"]), "build.get: the pair (phase, [Failed])"),
            (Filter::new().eq("tags", ["gold"]), "build.get: the pair (tags, [gold]) names no property of a Build header row"),
            (Filter::new().eq("current", ["yes"]), "a boolean is true or false"),
            (Filter::new().eq("attempt", ["two"]), "a number is decimal digits"),
            (Filter::new().eq("id", Vec::<String>::new()), "names no value"),
        ] {
            let why = f.builds_refusal().unwrap_or_else(|| panic!("{f:?} is refused"));
            assert!(why.contains(wanted), "{why:?} names {wanted:?}");
        }
    }

    /// CONTRACT: every lifecycle state, kind and provider a row can carry is a value its schema takes,
    /// by the name the row's own serialization gives it, so no row is unmatchable by its own state.
    #[test]
    fn every_state_kind_and_provider_a_row_carries_is_a_value_its_schema_takes() {
        use NodeStatus as S;
        for s in [S::Pending, S::Ready, S::ConnectionsDeleting, S::Leaving, S::PendingReconnect, S::Dead, S::Deleted, S::Starting, S::ConnectionsDeleted, S::Stopping, S::Stopped, S::Deleting, S::Left] {
            assert!(NODE_STATUSES.contains(&page_name(&s).unwrap().as_str()), "{s:?}");
        }
        for s in [ScopeStatus::Pending, ScopeStatus::Ready, ScopeStatus::Leaving, ScopeStatus::Dead, ScopeStatus::Degraded] {
            assert!(SCOPE_STATUSES.contains(&page_name(&s).unwrap().as_str()), "{s:?}");
        }
        for k in NodeKind::ALL {
            assert!(KINDS.contains(&k.segment()), "{k:?}");
        }
        for p in [crate::views::ProviderKind::Process, crate::views::ProviderKind::Container] {
            assert!(PROVIDERS.contains(&page_name(&p).unwrap().as_str()), "{p:?}");
        }
    }
}
