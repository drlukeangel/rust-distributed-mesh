use anyhow::Result;
use rafka_node_base::{NodeRuntime, Role};

#[tokio::main]
async fn main() -> Result<()> {
    NodeRuntime::new("registry")
        .with_role(Role::Registry)
        .run()
        .await
}
