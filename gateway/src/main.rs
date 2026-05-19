use anyhow::Result;
use rafka_node_base::{NodeRuntime, Role};

#[tokio::main]
async fn main() -> Result<()> {
    NodeRuntime::new("gateway")
        .with_role(Role::Gateway)
        .run()
        .await
}
