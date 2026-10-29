//! The plural reads served on op `0x23` (node-rpc-envelope.md "Read, op `0x23`, rdm"; Q285).
//!
//! `fabric.get`, `mesh.get` and `node.get` answer every row they match, never bounded as one frame:
//! the rows are read once, in their stable order ([`crate::status_rpc::StatusDoor::read_rows`]: this
//! admin's atlas, or the primary's carried read), packed in order into data frames that each fit the
//! reply frame (`rdm_client::read_frames`, over `rdm_transport::chunking::pack_in_order`, the one
//! packer the Build facts and topology snapshots share), and streamed `Started`, the frames, `End { rows, frames }`. A row that fits no
//! frame alone is `ReadOverBound` naming it, and nothing is sent: no row is ever left out. One span,
//! `rdm.node_admin.<entity>.serve.via-read`, names the read, its pairs, the rows and frames sent and
//! the outcome.

use rdm_core::catalog::OpOwner;
use rdm_core::filter::Entity;
use rdm_core::read::{Read, ReadReply, ReadRequest};
use rdm_transport::stream::{NotStarted, ReplySink};
use rdm_transport::ServerBuilder;
use tracing::Instrument as _;

/// Serve the plural reads on op `0x23` on this admin. Until `door` is filled the admin decides
/// nothing and a read is `NotReady` by name.
pub fn serve(b: ServerBuilder, door: crate::status_rpc::StatusDoorSlot) -> ServerBuilder {
    b.serve_stream::<Read, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: ReadRequest, sink| {
        let door = door.clone();
        async move {
            let Some(door) = door.get().cloned() else {
                return Ok(ReadReply::NotReady { reason: "this admin holds no Status door yet: it reads nothing".into() });
            };
            Ok(answer(&door, req, sink).await)
        }
    })
}

/// Answer one read: its rows streamed in frames, or its refusal.
async fn answer(door: &crate::status_rpc::StatusDoor, req: ReadRequest, sink: ReplySink<Read, NotStarted>) -> ReadReply {
    let ReadRequest::Get { entity, filter } = req;
    let read = entity.read();
    let empty = tracing::field::Empty;
    let pairs = crate::views::pairs(&filter);
    let span = match entity {
        Entity::Nodes => tracing::info_span!("rdm.node_admin.node.serve.via-read", read, pairs = %pairs, rows = empty, frames = empty, outcome = empty, "otel.kind" = "internal"),
        Entity::Meshes => tracing::info_span!("rdm.node_admin.mesh.serve.via-read", read, pairs = %pairs, rows = empty, frames = empty, outcome = empty, "otel.kind" = "internal"),
        _ => tracing::info_span!("rdm.node_admin.fabric.serve.via-read", read, pairs = %pairs, rows = empty, frames = empty, outcome = empty, "otel.kind" = "internal"),
    };
    async move {
        let span = tracing::Span::current();
        let refused = |r: ReadReply| {
            span.record("outcome", r.name());
            r
        };
        if entity == Entity::Connections {
            return refused(ReadReply::RejectedFilter { reason: format!("{read} is Status's read (0x1B Get with its node), never op 0x23") });
        }
        let rows = match door.read_rows(&rdm_core::status::Read { entity, node: None, filter }).await {
            Ok(rows) => rows,
            Err(r) => return refused(r),
        };
        let count = rows.len() as u32;
        let frames = match rdm_client::read_frames(rows.into_rows(), read) {
            Ok(f) => f,
            Err(over) => return refused(over),
        };
        let n = frames.len() as u32;
        let mut sink = match sink.started(ReadReply::Started { read: read.to_string() }).await {
            Ok(s) => s,
            Err(e) => return refused(ReadReply::NotReady { reason: format!("the caller left before the read started: {e:?}") }),
        };
        for f in frames {
            if let Err(e) = sink.data(f).await {
                return refused(ReadReply::NotReady { reason: format!("the caller left during the read: {e:?}") });
            }
        }
        span.record("rows", count).record("frames", n).record("outcome", "served");
        span.in_scope(|| tracing::info!("the read's rows streamed in frames"));
        ReadReply::End { rows: count, frames: n }
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::protocol::NodeProtocol;
    use rdm_core::read::Rows;

    fn node(i: usize, note: usize) -> rdm_core::views::NodeView {
        let name = format!("mesh{}.rpc.{}", i / 1000 + 1, i % 1000 + 1);
        let mut n: rdm_core::views::NodeView = serde_json::from_value(serde_json::json!({
            "name": name, "kind": "rpc_node", "mesh": name.split('.').next().unwrap(), "node_id": rdm_core::NodeId::mint(), "endpoint_id": null,
            "incarnation_id": null, "deployment_id": null, "provider": "process", "data_dir": null, "status": "ready",
            "is_primary": false, "is_fabric_primary": false, "admin_api_base": null, "transport_addr": null, "listeners": [],
            "declared": null, "stopped": false, "load": null, "gossip": null, "tags": [], "config": {}, "data_dir_lost": null,
            "data_dir_unreadable": null, "types": [], "held": [], "held_hash": vec![0u8; 32], "cert": null, "executable": null,
            "storage_persistence": null, "topology_storage_provider": null,
        }))
        .expect("a node row from its fields");
        n.config.insert("note".into(), "n".repeat(note));
        n
    }

    /// CONTRACT (Q285: "each batch packed to the 64 KiB frame by `chunking::pack_in_order` ... a row
    /// that fits no frame `ReadOverBound` naming the row ... a row the packer rejects is never
    /// omitted"): rows several times one frame pack into frames each within the bound, in order, none
    /// lost; a row larger than a frame alone is named and nothing is packed.
    #[test]
    fn rows_pack_in_order_into_frames_within_the_bound_and_a_row_past_it_is_named() {
        let rows: Vec<rdm_core::views::NodeView> = (0..2000).map(|i| node(i, 200)).collect();
        let packed = rdm_client::read_frames(Rows::Nodes(rows.clone()).into_rows(), "node.get").expect("every row fits a frame");
        assert!(packed.len() > 3, "several frames: {}", packed.len());
        assert!(packed.iter().all(|f| Read::encode_reply(f).unwrap().len() <= Read::MAX_REPLY_FRAME_BYTES));
        let mut back = Rows::Nodes(Vec::new());
        for f in packed {
            assert!(back.take(f));
        }
        assert_eq!(back, Rows::Nodes(rows), "every row, in order");

        let mut over = vec![node(0, 10), node(1, 70 * 1024), node(2, 10)];
        let Err(ReadReply::ReadOverBound { read, row, bytes, max }) = rdm_client::read_frames(Rows::Nodes(std::mem::take(&mut over)).into_rows(), "node.get") else { panic!("a row past the frame is refused") };
        assert_eq!((read.as_str(), row.as_str(), max), ("node.get", "mesh1.rpc.2", Read::MAX_REPLY_FRAME_BYTES as u64));
        assert!(bytes > max);
    }
}
