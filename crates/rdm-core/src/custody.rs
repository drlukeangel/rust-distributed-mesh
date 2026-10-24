//! The custody document (`RDM_DECLARED_SECRETS`, and the file a node-admin keeps beside its data dir):
//! the declared secrets a node-admin holds to hand on (deployment-provider.md §2.3; proposal
//! per-process-launch.md §0.1). A launch-environment and data-dir format, as the binding set and the
//! runtime record are, never a Node RPC frame.

use crate::launch::{DeclaredSecret, SecretHex, CUSTODY_VERSION, ENV_DECLARED_SECRETS};
use crate::NodeKind;

type Result<T> = std::result::Result<T, String>;

/// `declared` as the custody document carries it (`RDM_DECLARED_SECRETS`, and the file an admin keeps
/// beside its data dir): `{"v":1,"declared":[{"name","kinds","value"}]}`, each kind by its name, each
/// value as given. A [`SecretHex`] is spelt from a value's bytes ([`SecretHex::of_value`]), so it
/// always decodes.
pub fn encode_custody(declared: &[(DeclaredSecret, SecretHex)]) -> String {
    let entries: Vec<serde_json::Value> = declared
        .iter()
        .map(|(d, v)| serde_json::json!({ "name": d.name, "kinds": d.kinds.iter().map(|k| k.name()).collect::<Vec<_>>(), "value": v.value(&d.name).unwrap_or_default() }))
        .collect();
    serde_json::json!({ "v": CUSTODY_VERSION, "declared": entries }).to_string()
}

/// The declarations and values the custody document `v` carries: absent or empty is none. A document
/// that does not parse (not JSON, or an entry without a name, its kinds by name, or a value), names an
/// unknown version or repeats a name is refused by name, echoing none of it.
pub fn decode_custody(v: Option<&str>) -> Result<Vec<(DeclaredSecret, SecretHex)>> {
    let Some(text) = v.filter(|t| !t.trim().is_empty()) else { return Ok(Vec::new()) };
    let unparsed = || format!("{ENV_DECLARED_SECRETS} does not parse as the custody document {{\"v\":{CUSTODY_VERSION},\"declared\":[{{\"name\",\"kinds\",\"value\"}}]}}");
    let doc: serde_json::Value = serde_json::from_str(text).map_err(|_| unparsed())?;
    match doc.get("v").and_then(serde_json::Value::as_u64) {
        Some(CUSTODY_VERSION) => {}
        Some(_) => return Err(format!("{ENV_DECLARED_SECRETS} names an unknown version of the custody document (this node reads version {CUSTODY_VERSION})")),
        None => return Err(unparsed()),
    }
    let mut out: Vec<(DeclaredSecret, SecretHex)> = Vec::new();
    for e in doc.get("declared").and_then(serde_json::Value::as_array).ok_or_else(unparsed)? {
        let name = e.get("name").and_then(serde_json::Value::as_str).ok_or_else(unparsed)?;
        let value = e.get("value").and_then(serde_json::Value::as_str).ok_or_else(unparsed)?;
        let kinds = e
            .get("kinds")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(unparsed)?
            .iter()
            .map(|k| k.as_str().and_then(|k| NodeKind::ALL.into_iter().find(|n| n.name() == k)).ok_or_else(unparsed))
            .collect::<Result<Vec<_>>>()?;
        if out.iter().any(|(d, _)| d.name == name) {
            return Err(format!("{ENV_DECLARED_SECRETS} repeats a name in the custody document"));
        }
        out.push((DeclaredSecret { name: name.to_string(), kinds }, SecretHex::of_value(value)));
    }
    Ok(out)
}

