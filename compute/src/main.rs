use anyhow::Result;
use rafka_node_base::{NodeRuntime, Role};

#[tokio::main]
async fn main() -> Result<()> {
    NodeRuntime::new("compute")
        .with_role(Role::Compute)
        .run()
        .await
}
