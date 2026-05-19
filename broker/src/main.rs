use anyhow::Result;
use rafka_node_base::{NodeRuntime, Role};

#[tokio::main]
async fn main() -> Result<()> {
    NodeRuntime::new("broker")
        .with_role(Role::Broker)
        .run()
        .await
}
