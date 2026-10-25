//! The declared secrets (Q150): the per-process variables an application's wiring declares by name,
//! each with the node kinds it is set on ([`rdm_core::launch::DeclaredSecret`]).
//!
//! - The value is captured once, at the bootstrap: the bootstrap admin reads each declared value from
//!   its own environment at its start, as it reads the fabric's root. Every node-admin RDM launches is
//!   written them beside the root, in the custody document (`RDM_DECLARED_SECRETS`), so whichever admin
//!   runs a create can launch the named kinds.
//! - Every admin keeps the values beside its data dir ([`KEPT_FILE`]), as it keeps the root
//!   (`ca-secret`), and reads them back on a start from disk without a launcher. A replace and an executable
//!   replacement launch from the launching admin's copy, so the value never changes while the fabric runs:
//!   changing it is a new fabric.
//! - A value is set in the process environment of the named kinds only, and every declared name is
//!   withheld from what a process runtime inherits.
//! - A value is held as [`SecretHex`] from its read to the spawn, and written to no row, Build record,
//!   receipt, launch record or span (deployment-provider.md §2.3); RDM's own diagnostics are scrubbed
//!   of every declared value and RDM's four secrets before they reach a receipt or a span ([`scrub`]).
//!
//! A declared name that RDM sets or passes through, and a name declared twice, are refused at the
//! admin's start by name, naming the fabric's provider, before anything is bootstrapped.

use rdm_core::launch::{DeclaredSecret, Launch, SecretHex};
use rdm_core::NodeKind;
use std::collections::BTreeMap;
use std::path::Path;

/// The file in a node-admin's data dir that keeps the declared values it holds: the custody document
/// (`RDM_DECLARED_SECRETS`'s form).
pub const KEPT_FILE: &str = "declared-secrets";

/// The declarations an admin's wiring registered, and the values it holds for them.
#[derive(Debug, Clone, Default)]
pub struct DeclaredSecrets {
    /// Every declaration, as the wiring registered it.
    pub declared: Vec<DeclaredSecret>,
    /// The values this admin holds, by name, each value's bytes as hex. A declared name with no value
    /// at the read is set on no launch.
    pub values: BTreeMap<String, SecretHex>,
}

impl DeclaredSecrets {
    /// The values of `declared` this admin holds, read once at its start: those its data dir keeps
    /// (`data_dir`/[`KEPT_FILE`]) when it keeps them, else its launch's custody document when a
    /// pipeline launched it (`launch`), else its own environment (`get`). A declaration RDM's own names
    /// collide with, or a name declared twice, is refused by name, naming the fabric's `provider`
    /// ([`refuse_rdm_names`]); a kept file that cannot be read or does not parse is refused by name,
    /// echoing none of it.
    pub fn read(declared: Vec<DeclaredSecret>, launch: Option<&Launch>, get: impl Fn(&str) -> Option<String>, data_dir: &Path, provider: &str) -> Result<Self, String> {
        refuse_rdm_names(&declared, provider)?;
        if declared.is_empty() {
            return Ok(Self::default());
        }
        let file = data_dir.join(KEPT_FILE);
        let kept: Option<BTreeMap<String, SecretHex>> = match std::fs::read_to_string(&file) {
            Ok(text) => Some(rdm_core::launch::decode_custody(Some(&text)).map_err(|e| format!("refusing to start: {}: the declared values kept here: {e}", file.display()))?.into_iter().map(|(d, v)| (d.name, v)).collect()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(format!("refusing to start: {}: the declared values kept here cannot be read: {e}", file.display())),
        };
        let launched: Option<BTreeMap<String, SecretHex>> = launch.map(|l| l.declared.iter().map(|(d, v)| (d.name.clone(), v.clone())).collect());
        let values = declared
            .iter()
            .filter_map(|d| {
                let held = match (&kept, &launched) {
                    (Some(k), _) | (None, Some(k)) => k.get(&d.name).cloned(),
                    (None, None) => get(&d.name).map(|v| SecretHex::of_value(&v)),
                };
                held.map(|v| (d.name.clone(), v))
            })
            .collect();
        Ok(Self { declared, values })
    }

    /// Keep the values this admin holds beside `data_dir` ([`KEPT_FILE`]) when none are kept there
    /// yet, so the same admin started again holds the same values: as the root is kept (`ca-secret`).
    /// Nothing is written when the wiring declares nothing.
    pub fn keep(&self, data_dir: &Path) -> Result<(), String> {
        let file = data_dir.join(KEPT_FILE);
        if self.declared.is_empty() || file.exists() {
            return Ok(());
        }
        std::fs::create_dir_all(data_dir).map_err(|e| format!("{}: the data dir the declared values are kept in: {e}", data_dir.display()))?;
        std::fs::write(&file, rdm_core::launch::encode_custody(&self.custody())).map_err(|e| format!("{}: the declared values could not be kept: {e}", file.display()))
    }

    /// Every declaration whose value this admin holds, with that value: what a node-admin's launch
    /// carries in custody and what [`Self::keep`] writes.
    pub fn custody(&self) -> Vec<(DeclaredSecret, SecretHex)> {
        self.declared.iter().filter_map(|d| Some((d.clone(), self.values.get(&d.name)?.clone()))).collect()
    }

    /// The values set in the environment of a runtime of `kind`: those whose declaration names it.
    pub fn set_on(&self, kind: NodeKind) -> BTreeMap<String, SecretHex> {
        self.declared.iter().filter(|d| d.kinds.contains(&kind)).filter_map(|d| Some((d.name.clone(), self.values.get(&d.name)?.clone()))).collect()
    }

    /// Every declared name: withheld from what a process runtime inherits.
    pub fn names(&self) -> Vec<String> {
        self.declared.iter().map(|d| d.name.clone()).collect()
    }

    /// Every spelling of each value this admin holds a diagnostic could carry it in, by name: as
    /// given, each of its lines, JSON-escaped (as the custody document carries it) and its bytes as
    /// hex; and the custody document itself (`RDM_DECLARED_SECRETS`). An empty value spells nothing.
    pub fn spellings(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (d, v) in self.custody() {
            let value = v.value(&d.name).unwrap_or_default();
            let escaped = serde_json::to_string(&value).unwrap_or_default();
            let escaped = escaped.get(1..escaped.len().saturating_sub(1)).unwrap_or_default().to_string();
            // Each line apart as well: a runtime's last words reach a diagnostic with their lines joined.
            let lines: Vec<String> = if value.contains('\n') { value.lines().filter(|l| !l.trim().is_empty()).map(str::to_string).collect() } else { Vec::new() };
            for spelt in [value, escaped, v.0.clone()].into_iter().chain(lines) {
                out.push((d.name.clone(), spelt));
            }
        }
        if !self.values.is_empty() {
            out.push((rdm_core::launch::ENV_DECLARED_SECRETS.to_string(), rdm_core::launch::encode_custody(&self.custody())));
        }
        out
    }
}

/// `text` with every secret of `secrets` (by name, its spelling) replaced by `[<name> redacted]`:
/// RDM's diagnostics (a spawn error, a runtime's last words) before they reach a receipt or a span.
/// The longest spelling is replaced first, so a document holding a value is replaced whole; an empty
/// spelling replaces nothing.
pub fn scrub(text: &str, secrets: &[(String, String)]) -> String {
    let mut by_length: Vec<&(String, String)> = secrets.iter().filter(|(_, s)| !s.is_empty()).collect();
    by_length.sort_by_key(|(_, s)| std::cmp::Reverse(s.len()));
    let mut out = text.to_string();
    for (name, spelt) in by_length {
        if out.contains(spelt.as_str()) {
            out = out.replace(spelt.as_str(), &format!("[{name} redacted]"));
        }
    }
    out
}

/// Refuses, by name, a declaration whose name RDM sets or passes through: `RDM_*`, `TRACEPARENT`,
/// `TRACESTATE` and the pass-through list ([`crate::admin::PASSTHROUGH`]); a name that is no
/// environment variable name; and two declarations of one name, which are not merged (registration
/// order chooses nothing). The refusal names the variable, never a value, and the fabric's `provider`
/// (R-D3.3).
pub fn refuse_rdm_names(declared: &[DeclaredSecret], provider: &str) -> Result<(), String> {
    let mut seen = std::collections::BTreeSet::new();
    for d in declared {
        let n = d.name.as_str();
        let why = if n.is_empty() || n.starts_with(|c: char| c.is_ascii_digit()) || !n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            Some("it is no environment variable name (letters, digits and `_`, not starting with a digit)")
        } else if n.starts_with("RDM_") {
            Some("RDM sets every `RDM_*` variable of a launch")
        } else if n == "TRACEPARENT" || n == "TRACESTATE" {
            Some("RDM sets the trace context of a launch")
        } else if crate::admin::PASSTHROUGH.contains(&n) {
            Some("RDM passes it through to every launch")
        } else if !seen.insert(n) {
            Some("it is declared twice")
        } else {
            None
        };
        if let Some(why) = why {
            return Err(format!("refusing to start: the declared secret `{n}` is refused: {why} (the fabric's provider is {provider})"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declared(name: &str, kinds: &[NodeKind]) -> DeclaredSecret {
        DeclaredSecret { name: name.into(), kinds: kinds.to_vec() }
    }

    fn dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("declared-unit-{}", rdm_core::NodeId::mint()))
    }

    /// CONTRACT (Q150 amendment 3; a duplicate declaration; R-D3.3): a declared name RDM sets or passes
    /// through, and a name declared twice, are refused by name, naming the fabric's provider.
    #[test]
    fn a_declared_name_rdm_sets_or_passes_through_is_refused_by_name() {
        for (name, says) in [
            ("RDM_CA_SECRET", "`RDM_*`"),
            ("RDM_ANYTHING", "`RDM_*`"),
            ("TRACEPARENT", "trace context"),
            ("TRACESTATE", "trace context"),
            ("RUST_LOG", "passes it through"),
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "passes it through"),
            ("A=B", "no environment variable name"),
            ("", "no environment variable name"),
            ("1KEY", "no environment variable name"),
        ] {
            let e = refuse_rdm_names(&[declared(name, &[NodeKind::Broker])], "container").unwrap_err();
            assert!(e.starts_with("refusing to start") && e.contains(&format!("`{name}`")) && e.contains(says) && e.contains("the fabric's provider is container"), "{name}: {e}");
        }
        let e = refuse_rdm_names(&[declared("APP_KEY", &[NodeKind::Broker]), declared("APP_KEY", &[NodeKind::Gateway])], "process").unwrap_err();
        assert!(e.contains("`APP_KEY`") && e.contains("declared twice") && e.contains("the fabric's provider is process"), "{e}");
        assert!(refuse_rdm_names(&[declared("APP_KEY", &[NodeKind::Broker])], "process").is_ok());
    }

    /// CONTRACT (Q150): the bootstrap admin reads each declared value from its own environment and
    /// keeps it beside its data dir; a launched admin from its launch's custody document, never its
    /// environment; an admin whose data dir keeps the values reads them from there, never its launch or
    /// its environment, so the value never changes; a value is set on the named kinds only, every
    /// declared name is withheld, and the debug form names no value.
    #[test]
    fn declared_values_are_read_once_kept_beside_the_data_dir_and_set_on_the_named_kinds_only() {
        let decl = vec![declared("APP_KEY", &[NodeKind::Broker, NodeKind::Gateway]), declared("APP_ADMIN_KEY", &[NodeKind::NodeAdmin]), declared("APP_UNSET", &[NodeKind::Broker])];
        let env = |k: &str| match k {
            "APP_KEY" => Some("bootstrap,value=\nwith a newline".to_string()),
            "APP_ADMIN_KEY" => Some(String::new()),
            _ => None,
        };
        let bootstrap = dir();
        let held = DeclaredSecrets::read(decl.clone(), None, env, &bootstrap, "process").unwrap();
        assert_eq!(held.set_on(NodeKind::Broker).keys().collect::<Vec<_>>(), ["APP_KEY"], "an unset declared name is set on no launch");
        assert_eq!(held.set_on(NodeKind::Broker)["APP_KEY"].value("APP_KEY").unwrap(), "bootstrap,value=\nwith a newline");
        assert!(held.set_on(NodeKind::Compute).is_empty() && held.set_on(NodeKind::RpcNode).is_empty());
        assert_eq!(held.set_on(NodeKind::NodeAdmin)["APP_ADMIN_KEY"].value("APP_ADMIN_KEY").unwrap(), "", "an empty value is a value");
        assert_eq!(held.names(), ["APP_KEY", "APP_ADMIN_KEY", "APP_UNSET"]);
        assert!(!format!("{held:?}").contains("bootstrap,value") && !format!("{held:?}").contains(&rdm_core::launch::encode_hex(b"bootstrap,value=\nwith a newline")), "{held:?}");
        assert!(!bootstrap.join(KEPT_FILE).exists(), "the read writes nothing");
        held.keep(&bootstrap).unwrap();
        // Started again on its data dir, with another value in its environment: the kept one.
        let other = |k: &str| (k == "APP_KEY").then(|| "another value".to_string());
        let again = DeclaredSecrets::read(decl.clone(), None, other, &bootstrap, "process").unwrap();
        assert_eq!(again.set_on(NodeKind::Gateway)["APP_KEY"].value("APP_KEY").unwrap(), "bootstrap,value=\nwith a newline", "the value never changes while the fabric runs");
        assert_eq!(again.set_on(NodeKind::NodeAdmin)["APP_ADMIN_KEY"].value("APP_ADMIN_KEY").unwrap(), "");
        let launch = Launch::from_env(|k| match k {
            "RDM_FABRIC" => Some("fabric1".into()),
            "RDM_FABRIC_ID" => Some(rdm_core::FabricId::mint().to_string()),
            "RDM_NODE_NAME" => Some("mesh2.admin.1".into()),
            "RDM_NODE_ID" => Some(rdm_core::NodeId::mint().to_string()),
            "RDM_INCARNATION_ID" => Some(rdm_core::IncarnationId::mint().to_string()),
            "RDM_TRANSPORT_ADDR" => Some("127.0.0.1:0".into()),
            "RDM_DATA_DIR" | "RDM_STORE_DIR" => Some("/tmp/x".into()),
            "RDM_DECLARED_SECRETS" => Some(rdm_core::launch::encode_custody(&[(declared("APP_KEY", &[NodeKind::Broker]), SecretHex::of_value("custody-value"))])),
            _ => None,
        })
        .unwrap();
        let launched_dir = dir();
        let launched = DeclaredSecrets::read(decl, Some(&launch), env, &launched_dir, "process").unwrap();
        assert_eq!(launched.set_on(NodeKind::Gateway)["APP_KEY"].value("APP_KEY").unwrap(), "custody-value", "a launched admin's value is its launch's");
        assert!(launched.set_on(NodeKind::NodeAdmin).is_empty(), "never its own environment's");
        let _ = std::fs::remove_dir_all(&bootstrap);
    }

    /// CONTRACT (Q150 §0.1, redaction): a diagnostic is scrubbed of every spelling of a held value and of
    /// the custody document whole; an empty value scrubs nothing.
    #[test]
    fn a_diagnostic_is_scrubbed_of_every_held_value_and_the_custody_document() {
        let held = DeclaredSecrets {
            declared: vec![declared("APP_KEY", &[NodeKind::Broker]), declared("APP_EMPTY", &[NodeKind::Broker])],
            values: BTreeMap::from([("APP_KEY".to_string(), SecretHex::of_value("a,b=c\nsecond-line")), ("APP_EMPTY".to_string(), SecretHex::of_value(""))]),
        };
        let doc = rdm_core::launch::encode_custody(&held.custody());
        let hex = rdm_core::launch::encode_hex(b"a,b=c\nsecond-line");
        let text = format!("plain a,b=c\nsecond-line; escaped a,b=c\\nsecond-line; hex {hex}; doc {doc}; joined a,b=c | second-line; end");
        let out = scrub(&text, &held.spellings());
        assert_eq!(out, "plain [APP_KEY redacted]; escaped [APP_KEY redacted]; hex [APP_KEY redacted]; doc [RDM_DECLARED_SECRETS redacted]; joined [APP_KEY redacted] | [APP_KEY redacted]; end");
    }
}
