//! The declared secrets (Q150): the per-process variables an application's wiring declares by name,
//! each with the node kinds it is set on ([`rdm_core::launch::DeclaredSecret`]).
//!
//! - The bootstrap admin, and a person-started one, reads each declared value once, from its own
//!   environment at its start, as it reads the fabric's root. A launched admin takes them from its
//!   launch (`RDM_DECLARED_SECRETS`), the custody copy every node-admin RDM launches is written beside
//!   the root, so whichever admin runs a create can launch the named kinds.
//! - A value is set in the process environment of the named kinds only, and every declared name is
//!   withheld from what a process runtime inherits.
//! - A value is held as [`SecretHex`] from its read to the spawn, and written to no row, Build record,
//!   receipt, launch record or span (deployment-provider.md §2.3).
//!
//! A declared name that RDM sets or passes through is refused at the admin's start by name, before
//! anything is bootstrapped.

use rdm_core::launch::{DeclaredSecret, Launch, SecretHex};
use rdm_core::NodeKind;
use std::collections::BTreeMap;

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
    /// The values of `declared` this admin holds: from its launch's custody copy when a pipeline
    /// launched it (`launch`), else from its own environment (`get`), read once. A declaration RDM's own
    /// names collide with is refused by name ([`refuse_rdm_names`]).
    pub fn read(declared: Vec<DeclaredSecret>, launch: Option<&Launch>, get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        refuse_rdm_names(&declared)?;
        let values = declared
            .iter()
            .filter_map(|d| {
                let held = match launch {
                    Some(l) => l.declared.get(&d.name).cloned(),
                    None => get(&d.name).map(|v| SecretHex::of_value(&v)),
                };
                held.map(|v| (d.name.clone(), v))
            })
            .collect();
        Ok(Self { declared, values })
    }

    /// The values set in the environment of a runtime of `kind`: those whose declaration names it.
    pub fn set_on(&self, kind: NodeKind) -> BTreeMap<String, SecretHex> {
        self.declared.iter().filter(|d| d.kinds.contains(&kind)).filter_map(|d| Some((d.name.clone(), self.values.get(&d.name)?.clone()))).collect()
    }

    /// Every declared name: withheld from what a process runtime inherits.
    pub fn names(&self) -> Vec<String> {
        self.declared.iter().map(|d| d.name.clone()).collect()
    }
}

/// Refuses, by name, a declaration whose name RDM sets or passes through: `RDM_*`, `TRACEPARENT`,
/// `TRACESTATE` and the pass-through list ([`crate::admin::PASSTHROUGH`]); and a name that is no
/// environment variable name, or that is declared twice.
pub fn refuse_rdm_names(declared: &[DeclaredSecret]) -> Result<(), String> {
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
            return Err(format!("refusing to start: the declared secret `{n}` is refused: {why}"));
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

    /// CONTRACT (Q150 amendment 3): a declared name RDM sets or passes through is refused by name.
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
            let e = refuse_rdm_names(&[declared(name, &[NodeKind::Broker])]).unwrap_err();
            assert!(e.starts_with("refusing to start") && e.contains(&format!("`{name}`")) && e.contains(says), "{name}: {e}");
        }
        let e = refuse_rdm_names(&[declared("APP_KEY", &[NodeKind::Broker]), declared("APP_KEY", &[NodeKind::Gateway])]).unwrap_err();
        assert!(e.contains("declared twice"), "{e}");
        assert!(refuse_rdm_names(&[declared("APP_KEY", &[NodeKind::Broker])]).is_ok());
    }

    /// CONTRACT (Q150): the bootstrap admin reads each declared value from its own environment; a
    /// launched admin from its launch's custody copy, never its environment; a value is set on the named
    /// kinds only, every declared name is withheld, and the debug form names no value.
    #[test]
    fn declared_values_are_read_once_and_set_on_the_named_kinds_only() {
        let decl = vec![declared("APP_KEY", &[NodeKind::Broker, NodeKind::Gateway]), declared("APP_ADMIN_KEY", &[NodeKind::NodeAdmin]), declared("APP_UNSET", &[NodeKind::Broker])];
        let env = |k: &str| match k {
            "APP_KEY" => Some("bootstrap-value".to_string()),
            "APP_ADMIN_KEY" => Some("admin-value".to_string()),
            _ => None,
        };
        let held = DeclaredSecrets::read(decl.clone(), None, env).unwrap();
        assert_eq!(held.set_on(NodeKind::Broker).keys().collect::<Vec<_>>(), ["APP_KEY"], "an unset declared name is set on no launch");
        assert_eq!(held.set_on(NodeKind::Broker)["APP_KEY"].value("APP_KEY").unwrap(), "bootstrap-value");
        assert!(held.set_on(NodeKind::Compute).is_empty() && held.set_on(NodeKind::RpcNode).is_empty());
        assert_eq!(held.set_on(NodeKind::NodeAdmin).keys().collect::<Vec<_>>(), ["APP_ADMIN_KEY"]);
        assert_eq!(held.names(), ["APP_KEY", "APP_ADMIN_KEY", "APP_UNSET"]);
        assert!(!format!("{held:?}").contains("bootstrap-value") && !format!("{held:?}").contains(&rdm_core::launch::encode_hex(b"bootstrap-value")), "{held:?}");
        let launch = Launch::from_env(|k| match k {
            "RDM_FABRIC" => Some("fabric1".into()),
            "RDM_FABRIC_ID" => Some(rdm_core::FabricId::mint().to_string()),
            "RDM_NODE_NAME" => Some("mesh2.admin.1".into()),
            "RDM_NODE_ID" => Some(rdm_core::NodeId::mint().to_string()),
            "RDM_INCARNATION_ID" => Some(rdm_core::IncarnationId::mint().to_string()),
            "RDM_TRANSPORT_ADDR" => Some("127.0.0.1:0".into()),
            "RDM_DATA_DIR" | "RDM_STORE_DIR" => Some("/tmp/x".into()),
            "RDM_DECLARED_SECRETS" => Some(rdm_core::launch::encode_declared(&BTreeMap::from([("APP_KEY".to_string(), SecretHex::of_value("custody-value"))]))),
            _ => None,
        })
        .unwrap();
        let launched = DeclaredSecrets::read(decl, Some(&launch), env).unwrap();
        assert_eq!(launched.set_on(NodeKind::Gateway)["APP_KEY"].value("APP_KEY").unwrap(), "custody-value", "a launched admin's value is its launch's");
        assert!(launched.set_on(NodeKind::NodeAdmin).is_empty(), "never its own environment's");
    }
}
