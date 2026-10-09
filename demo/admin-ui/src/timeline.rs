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
const HIGH_VOLUME: &[&str] = &["rdm.node_admin.deployment.update.via-pipeline"];

/// Attributes that carry a span's story, in the order they are shown.
const KEYS: &[&str] = &[
    "op", "protocol", "target", "peer", "outcome", "reason", "elapsed_ms", "status", "scope", "step", "build_id", "attempt", "from", "to", "state",
    "held_by", "sender", "winner_path", "observer", "source", "destination", "kind", "executor", "route", "key", "detail", "change",
];

/// Whether `name` is shown without the toggle.
pub fn is_meaningful(name: &str) -> bool {
    MEANINGFUL.iter().any(|p| name.starts_with(p)) && !HIGH_VOLUME.contains(&name)
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

/// What the Timeline has read of one evidence folder.
#[derive(Default)]
pub struct Evidence {
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
}
