use anyhow::Result;
use rafka_node_base::{NodeRuntime, Role};

#[tokio::main]
async fn main() -> Result<()> {
    NodeRuntime::new("bridge")
        .with_role(Role::Bridge)
        .run()
        .await
}
