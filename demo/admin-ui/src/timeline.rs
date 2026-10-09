//! The Timeline: the running log of the estate, read from the nodes' own span records.
//!
//! Every node process writes one `<bin>.<pid>-….spans.jsonl` into the estate's evidence folder
//! (`RDM_EVIDENCE_DIR`). [`Evidence::refresh`] reads only the bytes appended since the last read
//! of each file, keeps the meaningful events (status changes, node-rpc calls and serves with their
//! outcomes, seat moves, Build steps, connection observations, fabric status sends) and counts
//! the high-volume ones (membership snapshots and the like) behind a toggle.

use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// One line of the running log.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Event {
    /// When it happened (ms since the Unix epoch).
    pub ts_ms: u64,
    /// The node that recorded it (its `path.name`), or the UI.
    pub node: String,
    /// The span name, or `ui.…` for an action the UI took.
    pub name: String,
    /// The span's own attributes that tell the story, in a fixed order.
    pub summary: String,
    /// Whether it is behind the high-volume toggle.
    pub high_volume: bool,
    /// `span` for a node's record, `ui` for the UI's own action.
    pub source: &'static str,
}

/// Span-name prefixes that are the story by default; everything else is high-volume.
const MEANINGFUL: &[&str] = &[
    "rdm.node_rpc.request.",
    "rdm.node_rpc.status.",
    "rdm.node_admin.",
    "rdm.mesh.election.",
    "rdm.mesh.fabric.",
    "rdm.mesh.node.",
];
/// Inside the meaningful prefixes, still too frequent to read as a story.
const HIGH_VOLUME: &[&str] = &["rdm.node_admin.deployment.update.via-pipeline", HEARTBEAT_SPAN];

/// Every node emits one of these every 5 s: a pulse, not a story.
pub const HEARTBEAT_SPAN: &str = "rdm.mesh.node.update.via-heartbeat";

/// A held member left the observer's current view (`member`, `silent_ms`, `staleness_ms`).
pub const MEMBER_STALE_SPAN: &str = "rdm.mesh.membership.update.via-member-stale";

/// Attributes that carry a span's story, in the order they are shown.
const KEYS: &[&str] = &[
    "op", "protocol", "target", "peer", "outcome", "reason", "elapsed_ms", "status", "scope", "step", "build_id", "attempt", "from", "to", "state",
    "held_by", "member", "silent_ms", "staleness_ms", "sender", "winner_path", "observer", "source", "destination", "kind", "executor", "route", "key", "detail", "change",
];

/// Whether `name` is shown without the toggle.
pub fn is_meaningful(name: &str) -> bool {
    (MEANINGFUL.iter().any(|p| name.starts_with(p)) || name == MEMBER_STALE_SPAN) && !HIGH_VOLUME.contains(&name)
}

/// The event one span record makes, or `None` for a line that is not a span.
pub fn event_of(span: &Value, fallback_node: &str) -> Option<Event> {
    let name = span.get("name")?.as_str()?.to_string();
    let ts_ns = span.get("start_unix_nano")?.as_u64()?;
    let attrs = span.get("attributes").and_then(Value::as_object);
    let attr = |k: &str| attrs.and_then(|a| a.get(k)).and_then(Value::as_str).filter(|v| !v.is_empty());
    let node = attr("node").or_else(|| attr("observer")).unwrap_or(fallback_node).to_string();
    let mut parts: Vec<String> = KEYS.iter().filter_map(|k| attr(k).map(|v| format!("{k}={}", clip(v, 90)))).collect();
    if let Some(msg) = span.get("events").and_then(Value::as_array).and_then(|e| e.first()).and_then(|e| e.get("name")).and_then(Value::as_str) {
        parts.push(format!("“{}”", clip(msg, 100)));
    }
    Some(Event { ts_ms: ts_ns / 1_000_000, node, high_volume: !is_meaningful(&name), name, summary: parts.join(" "), source: "span" })
}

fn clip(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        format!("{}…", s.chars().take(n).collect::<String>())
    }
}

/// The span every node writes for each connection fact it observes of its own pooled connections
/// (`ConnectionsWriter`, crates/rafka-node-admin-core/src/connections_writer.rs). Every node of the
/// estate writes one, role nodes included, so these spans are the one place every node's facts are
/// readable together.
pub const CONNECTION_FACT_SPAN: &str = "rdm.node_admin.connection.update.via-observed";

/// One node's latest fact about one `(source, destination, kind)`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConnFact {
    /// The node that observed it.
    pub source: String,
    /// The node it names.
    pub destination: String,
    /// `direct` or `proxy`.
    pub kind: String,
    /// `connected`, `disconnected` or `failed`.
    pub state: String,
    /// Why it was dropped or failed.
    pub reason: String,
    /// A proxy's carrier.
    pub carrier: String,
    /// The process birth of the source it was written by (empty when the span does not name it).
    pub source_incarnation: String,
    /// The process birth of the destination it names (empty when the span does not name it).
    pub destination_incarnation: String,
    /// When the writer stamped it (ms).
    pub logged_at_ms: u64,
}

/// The fact one span records, or `None` for any other span.
pub fn conn_fact_of(span: &Value) -> Option<ConnFact> {
    if span.get("name")?.as_str()? != CONNECTION_FACT_SPAN {
        return None;
    }
    let attrs = span.get("attributes")?.as_object()?;
    let a = |k: &str| attrs.get(k).and_then(Value::as_str).unwrap_or_default().to_string();
    let logged = attrs.get("logged_at_ms").and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))).unwrap_or_else(|| span.get("start_unix_nano").and_then(Value::as_u64).unwrap_or(0) / 1_000_000);
    Some(ConnFact {
        source: a("source"),
        destination: a("destination"),
        kind: a("kind"),
        state: a("state"),
        reason: a("reason"),
        carrier: a("carrier"),
        source_incarnation: a("source_incarnation"),
        destination_incarnation: a("destination_incarnation"),
        logged_at_ms: logged,
    })
}

/// One `via-member-stale` report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleMember {
    /// When it was reported (ms).
    pub ts_ms: u64,
    /// The node that stopped hearing it.
    pub observer: String,
    /// The member it stopped hearing.
    pub member: String,
    /// How long the member had been silent (ms).
    pub silent_ms: String,
    /// The staleness floor it exceeded (ms).
    pub staleness_ms: String,
}

/// One message the nodes exchanged: a Node RPC call or serve, or a gossip-channel event.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Message {
    /// When (ms since the Unix epoch).
    pub ts_ms: u64,
    /// `rpc` or `gossip`.
    pub kind: &'static str,
    /// The span it was read from.
    pub span: String,
    /// The node that recorded it.
    pub node: String,
    /// The Node RPC op (decimal), when the span names one.
    pub op: String,
    /// The protocol family, when named.
    pub protocol: String,
    /// The raw target as the span names it (`ExactNode(NodeId("..."))`, a node id, a path).
    pub target: String,
    /// The raw peer or caller as the span names it (an endpoint id).
    pub peer: String,
    /// The outcome the span records.
    pub outcome: String,
    /// How long the call took (ms), when the span says.
    pub elapsed_ms: String,
    /// The span's remaining story attributes, in the Timeline's order.
    pub detail: String,
}

/// Which spans are messages: Node RPC requests, and the gossip channels' seat, concern, leave,
/// forwarded and backbone activity.
pub fn message_kind(name: &str) -> Option<&'static str> {
    if name.starts_with("rdm.node_rpc.request.") {
        return Some("rpc");
    }
    let gossip = ["rdm.mesh.seat.", "rdm.mesh.concern.", "rdm.mesh.backbone.", "rdm.mesh.membership.update.via-forwarded", "rdm.mesh.membership.update.via-leave", "rdm.mesh.membership.update.via-member-stale"];
    if gossip.iter().any(|p| name.starts_with(p)) || (name.starts_with("rdm.mesh.") && (name.contains("concern") || name.contains("leave"))) {
        return Some("gossip");
    }
    None
}

/// What the Timeline has read of one evidence folder.
#[derive(Default)]
pub struct Evidence {
    /// The messages nodes exchanged, oldest first.
    messages: std::collections::VecDeque<Message>,
    /// Nodes that joined the backbone (`via-backbone-peers-joined`, or publish or forward on it).
    backbone_listeners: std::collections::BTreeSet<String>,
    /// Nodes that publish the mesh's aggregate on the backbone (`via-aggregate-publisher`).
    backbone_publishers: std::collections::BTreeSet<String>,
    /// The newest heartbeat's `peer_count` per node: `(at_ms, peers)`.
    peers: HashMap<String, (u64, u32)>,
    /// Members a node reported leaving its view, not yet taken by the alerts.
    stale: Vec<StaleMember>,
    /// The latest fact per `(source, destination, kind)` any node of the estate wrote.
    facts: HashMap<(String, String, String), ConnFact>,
    offsets: HashMap<PathBuf, u64>,
    /// The node each process file belongs to, learned from the first span in it that names one.
    owners: HashMap<PathBuf, String>,
    meaningful: Vec<Event>,
    high_volume: Vec<Event>,
    high_volume_seen: usize,
}

/// Events kept per class; the oldest go first.
const KEEP_MEANINGFUL: usize = 40_000;
const KEEP_HIGH_VOLUME: usize = 4_000;

impl Evidence {
    /// Read what every `*.spans.jsonl` in `dir` gained since the last call.
    pub fn refresh(&mut self, dir: &Path) -> std::io::Result<usize> {
        let mut files = 0;
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            let Some(file) = path.file_name().and_then(|f| f.to_str()).map(str::to_string) else { continue };
            if !file.ends_with(".spans.jsonl") {
                continue;
            }
            files += 1;
            let offset = self.offsets.get(&path).copied().unwrap_or(0);
            let mut f = std::fs::File::open(&path)?;
            if f.metadata()?.len() <= offset {
                continue;
            }
            f.seek(SeekFrom::Start(offset))?;
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            let complete = buf.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
            let stem = file.splitn(3, '.').take(2).collect::<Vec<_>>().join(".");
            let parsed: Vec<Value> = buf[..complete].split(|b| *b == b'\n').filter(|l| !l.is_empty()).filter_map(|l| serde_json::from_slice(l).ok()).collect();
            if !self.owners.contains_key(&path) {
                if let Some(n) = parsed.iter().find_map(|v| v["attributes"]["node"].as_str().filter(|n| !n.is_empty())) {
                    self.owners.insert(path.clone(), n.to_string());
                }
            }
            let label = self.owners.get(&path).cloned().unwrap_or(stem);
            for v in parsed {
                if let Some(f) = conn_fact_of(&v) {
                    let key = (f.source.clone(), f.destination.clone(), f.kind.clone());
                    if self.facts.get(&key).is_none_or(|old| old.logged_at_ms <= f.logged_at_ms) {
                        self.facts.insert(key, f);
                    }
                }
                if v.get("name").and_then(Value::as_str) == Some(MEMBER_STALE_SPAN) {
                    let a = |k: &str| v["attributes"][k].as_str().unwrap_or_default().to_string();
                    let observer = Some(a("node")).filter(|n| !n.is_empty()).unwrap_or_else(|| label.clone());
                    self.stale.push(StaleMember { ts_ms: v["start_unix_nano"].as_u64().unwrap_or(0) / 1_000_000, observer, member: a("member"), silent_ms: a("silent_ms"), staleness_ms: a("staleness_ms") });
                }
                self.fold_signals(&v, &label);
                let Some(ev) = event_of(&v, &label) else { continue };
                if ev.high_volume {
                    self.high_volume_seen += 1;
                    self.high_volume.push(ev);
                } else {
                    self.meaningful.push(ev);
                }
            }
            self.offsets.insert(path, offset + complete as u64);
        }
        trim(&mut self.meaningful, KEEP_MEANINGFUL);
        trim(&mut self.high_volume, KEEP_HIGH_VOLUME);
        Ok(files)
    }

    /// Backbone membership, heartbeat peers and messages from one span.
    fn fold_signals(&mut self, v: &Value, label: &str) {
        let Some(name) = v.get("name").and_then(Value::as_str) else { return };
        let attr = |k: &str| v["attributes"][k].as_str().unwrap_or_default().to_string();
        let ts_ms = v["start_unix_nano"].as_u64().unwrap_or(0) / 1_000_000;
        let node = Some(attr("node")).filter(|n| !n.is_empty()).unwrap_or_else(|| label.to_string());
        match name {
            "rdm.mesh.connection.update.via-backbone-peers-joined" => {
                self.backbone_listeners.insert(node.clone());
            }
            "rdm.mesh.backbone.update.via-aggregate-publisher" => {
                self.backbone_listeners.insert(node.clone());
                self.backbone_publishers.insert(node.clone());
            }
            "rdm.mesh.backbone.update.via-forwarder" => {
                self.backbone_listeners.insert(node.clone());
            }
            HEARTBEAT_SPAN => {
                if let Ok(p) = attr("peer_count").parse::<u32>() {
                    if self.peers.get(&node).is_none_or(|(t, _)| *t <= ts_ms) {
                        self.peers.insert(node.clone(), (ts_ms, p));
                    }
                }
            }
            _ => {}
        }
        let Some(kind) = message_kind(name) else { return };
        let story: Vec<String> = ["holder", "seat", "mesh", "member", "source_publisher", "role", "topology_version", "inner_op", "reason"]
            .iter()
            .filter_map(|k| Some(attr(k)).filter(|x| !x.is_empty()).map(|x| format!("{k}={}", clip(&x, 80))))
            .collect();
        self.messages.push_back(Message {
            ts_ms, kind, span: name.to_string(), node, op: Some(attr("op")).filter(|o| !o.is_empty()).unwrap_or_else(|| attr("inner_op")),
            protocol: attr("protocol"), target: attr("target"), peer: Some(attr("peer")).filter(|p| !p.is_empty()).unwrap_or_else(|| attr("caller")),
            outcome: attr("outcome"), elapsed_ms: attr("elapsed_ms"), detail: story.join(" "),
        });
        if self.messages.len() > 6000 {
            self.messages.pop_front();
        }
    }

    /// The newest `limit` messages (newest first) of `kind` (`rpc`, `gossip`, or `all`).
    pub fn messages(&self, kind: &str, limit: usize) -> Vec<Message> {
        self.messages.iter().rev().filter(|m| kind == "all" || m.kind == kind).take(limit).cloned().collect()
    }

    /// The nodes on the backbone and the ones publishing on it.
    pub fn backbone(&self) -> (Vec<String>, Vec<String>) {
        (self.backbone_listeners.iter().cloned().collect(), self.backbone_publishers.iter().cloned().collect())
    }

    /// Mean of each node's newest heartbeat `peer_count`, over the nodes in `present`.
    pub fn mean_peers(&self, present: &[String]) -> Option<f64> {
        let v: Vec<f64> = present.iter().filter_map(|n| self.peers.get(n).map(|(_, p)| f64::from(*p))).collect();
        (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64)
    }

    /// The member-stale reports read since the last call.
    pub fn take_stale(&mut self) -> Vec<StaleMember> {
        std::mem::take(&mut self.stale)
    }

    /// The latest connection fact per `(source, destination, kind)`, from every node's records.
    pub fn connection_facts(&self) -> Vec<ConnFact> {
        self.facts.values().cloned().collect()
    }

    /// The newest `limit` events, newest first, across every node and the UI's own `ui` events.
    pub fn newest(&self, ui: &[Event], all: bool, limit: usize) -> (Vec<Event>, usize) {
        let mut out: Vec<Event> = self.meaningful.iter().chain(ui.iter()).cloned().collect();
        if all {
            out.extend(self.high_volume.iter().cloned());
        }
        out.sort_by(|a, b| b.ts_ms.cmp(&a.ts_ms));
        out.truncate(limit);
        (out, self.high_volume_seen)
    }
}

fn trim(v: &mut Vec<Event>, keep: usize) {
    if v.len() > keep {
        v.drain(..v.len() - keep);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn span(name: &str, ns: u64, attrs: Value) -> String {
        json!({"name": name, "start_unix_nano": ns, "attributes": attrs, "events": [{"name": "an event"}]}).to_string()
    }

    #[test]
    fn meaningful_spans_show_by_default_and_membership_snapshots_hide_behind_the_toggle() {
        let dir = std::env::temp_dir().join(format!("tl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lines = [
            span("rdm.node_rpc.request.update.via-call", 3_000_000_000, json!({"node": "mesh1.rpc.1", "op": "27", "outcome": "Reply"})),
            span("rdm.mesh.membership.update.via-snapshot-installed", 4_000_000_000, json!({"node": "mesh1.rpc.1"})),
            span("rdm.node_admin.status.update.via-declare", 1_000_000_000, json!({"node": "mesh1.admin.1", "outcome": "applied"})),
        ];
        std::fs::write(dir.join("rafka-rpc-node.1-1.spans.jsonl"), lines.join("\n") + "\n").unwrap();
        let mut e = Evidence::default();
        assert_eq!(e.refresh(&dir).unwrap(), 1);
        let (shown, hidden) = e.newest(&[], false, 10);
        assert_eq!(shown.iter().map(|x| x.name.as_str()).collect::<Vec<_>>(), ["rdm.node_rpc.request.update.via-call", "rdm.node_admin.status.update.via-declare"]);
        assert_eq!(hidden, 1);
        assert!(shown[0].summary.contains("outcome=Reply") && shown[0].summary.contains("op=27"), "{}", shown[0].summary);
        let (all, _) = e.newest(&[], true, 10);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].name, "rdm.mesh.membership.update.via-snapshot-installed", "newest first across the toggle");
        // A second refresh reads only what was appended, and never a half-written line.
        let mut f = std::fs::OpenOptions::new().append(true).open(dir.join("rafka-rpc-node.1-1.spans.jsonl")).unwrap();
        use std::io::Write;
        write!(f, "{}\n{{\"name\":\"half", span("rdm.mesh.election.resolve.via-mesh-primary", 5_000_000_000, json!({"observer": "mesh1.admin.2"}))).unwrap();
        e.refresh(&dir).unwrap();
        let (shown, _) = e.newest(&[], false, 10);
        assert_eq!(shown[0].name, "rdm.mesh.election.resolve.via-mesh-primary");
        assert_eq!(shown[0].node, "mesh1.admin.2");
        assert_eq!(shown.len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn heartbeats_hide_behind_the_toggle_and_a_stale_member_is_a_story_and_an_alert() {
        let dir = std::env::temp_dir().join(format!("tl-stale-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lines = [
            span(HEARTBEAT_SPAN, 1_000_000_000, json!({"node": "mesh1.broker.1", "peer_count": "9"})),
            span(MEMBER_STALE_SPAN, 2_000_000_000, json!({"node": "mesh1.admin.1", "member": "mesh2.gateway.1", "silent_ms": "7000", "staleness_ms": "6000"})),
        ];
        std::fs::write(dir.join("rshape-node-admin.1-1.spans.jsonl"), lines.join("\n") + "\n").unwrap();
        let mut e = Evidence::default();
        e.refresh(&dir).unwrap();
        let (shown, hidden) = e.newest(&[], false, 10);
        assert_eq!(shown.iter().map(|x| x.name.as_str()).collect::<Vec<_>>(), [MEMBER_STALE_SPAN]);
        assert_eq!(hidden, 1, "the heartbeat is counted behind the toggle");
        let stale = e.take_stale();
        assert_eq!((stale.len(), stale[0].member.as_str(), stale[0].observer.as_str()), (1, "mesh2.gateway.1", "mesh1.admin.1"));
        assert!(e.take_stale().is_empty(), "each report is taken once");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
