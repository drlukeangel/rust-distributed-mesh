//! `rshape-compute`: the R-shape consumer's compute entry point. It names its role and nothing else.

use rdm_node::{Role};

#[tokio::main]
async fn main() {
    rshape_consumer::exit_on_error("rshape-compute", rshape_consumer::run(Role { r#type: "compute".into() }, "rshape-compute").await);
}
