//! The fault door of a faulted node-admin, as a scenario drives it (i143.e8.s2, #2780): found by
//! the path the admin recorded it at (`<root>/faults/<name>.door`), armed, observed through
//! `GET /faults` and released. Shared by every cell that holds a real admin at a named cut.

use crate::estate::wait_for;
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

/// One faulted admin's door (`rafka_node_rpc_testkit::admin_faults::router`), found by the path
/// the admin recorded it at.
#[derive(Clone)]
pub struct Door {
    pub name: String,
    /// The fault door.
    pub base: String,
    /// The admin's control API: the Build state it holds is the one it wrote.
    pub api: String,
    pub http: reqwest::Client,
}

impl Door {
    pub async fn open(root: &Path, name: &str, api: &str) -> Door {
        let file = root.join("faults").join(format!("{name}.door"));
        let base = wait_for(&format!("{name} records its fault door at {}", file.display()), Duration::from_secs(60), || async { std::fs::read_to_string(&file).ok().filter(|b| !b.is_empty()) }).await;
        Door { name: name.into(), base, api: api.into(), http: reqwest::Client::new() }
    }

    pub async fn arm(&self, id: &str, spec: Value) -> Value {
        let mut body = spec;
        body["id"] = json!(id);
        let r = self.http.post(format!("{}/faults/arm", self.base)).json(&body).send().await.unwrap_or_else(|e| panic!("{}: arm {id}: {e}", self.name));
        let status = r.status().as_u16();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        assert_eq!(status, 200, "{}: arming `{id}` is acknowledged: {v}", self.name);
        assert_eq!(v["armed"], id, "{v}");
        v
    }

    pub async fn state(&self) -> Value {
        let r = self.http.get(format!("{}/faults", self.base)).send().await.unwrap_or_else(|e| panic!("{}: fault state: {e}", self.name));
        r.json().await.unwrap()
    }

    pub async fn cut(&self, id: &str) -> Value {
        self.state().await["cuts"][id].clone()
    }

    /// The acknowledgement that the injection is active: the cut holds a call, and which.
    pub async fn wait_held(&self, id: &str) -> Value {
        wait_for(&format!("{}: cut `{id}` holds a call", self.name), Duration::from_secs(90), || async {
            let c = self.cut(id).await;
            (c["held"] == true && !c["hit"].is_null()).then_some(c)
        })
        .await
    }

    pub async fn release(&self, id: &str) -> Value {
        let r = self.http.post(format!("{}/faults/release", self.base)).json(&json!({"id": id})).send().await.unwrap_or_else(|e| panic!("{}: release {id}: {e}", self.name));
        let status = r.status().as_u16();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        assert_eq!(status, 200, "{}: releasing `{id}` is acknowledged: {v}", self.name);
        assert_eq!(v["released"], id, "{v}");
        v
    }
}
