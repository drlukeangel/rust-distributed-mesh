//! `rshape-broker`: the R-shape consumer's broker entry point. It names its role and nothing else.

use rdm_node::{NodeKind, Role};

#[tokio::main]
async fn main() {
    rshape_consumer::exit_on_error("rshape-broker", rshape_consumer::run(Role { kind: NodeKind::Broker }, "rshape-broker").await);
}
