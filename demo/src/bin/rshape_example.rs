//! `rshape-example`: the compiled consumer example (`rshape_consumer::consumer_example`) as a node
//! executable. Its role is the kind its launch names; on the demo estate a Build puts it on one
//! broker and one compute (`scripts/demo-estate.sh example`).

#[tokio::main]
async fn main() {
    rshape_consumer::exit_on_error("rshape-example", rshape_consumer::consumer_example::run().await);
}
