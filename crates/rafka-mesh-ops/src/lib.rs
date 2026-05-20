use opentelemetry::{
    trace::{SpanContext, TraceContextExt, TraceFlags, TraceId, SpanId, TraceState},
    Context,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InternalMeshFrame {
    Ping { org_id: u64 },
    Pong { org_id: u64 },
    /// First frame sent after a peer connection opens. Carries the sender's mesh_id
    /// and node_type so the receiver can tag peer.connected with peer_mesh_id and
    /// emit a cross-mesh peer.connected span when meshes differ. Mesh-to-mesh phase
    /// 2 substrate per feature `mesh-to-mesh`.
    Hello { mesh_id: String, node_type: String },
}

/// Wire layout:
///   [0..16]  trace_id (big-endian bytes)
///   [16..24] span_id (big-endian bytes)
///   [24]     trace_flags (u8, 0x01 = sampled)
///   [25..32] padding (zeros, reserved for future use)
///   [32..]   postcard-encoded InternalMeshFrame
///
/// Encoding is `postcard` (golden principle #11: internal data read once →
/// postcard for smaller wire size + format stability). Previously bincode 2.
const HEADER_LEN: usize = 32;

impl InternalMeshFrame {
    /// Encode frame with OTel context from `ctx` embedded in the wire header.
    /// Caller must enter the sending span before calling this so ctx carries
    /// the correct span to propagate.
    pub fn encode_with_context(&self, ctx: &Context) -> Vec<u8> {
        let span = ctx.span();
        let sc = span.span_context();

        let mut buf = Vec::with_capacity(HEADER_LEN + 64);
        buf.extend_from_slice(&sc.trace_id().to_bytes());
        buf.extend_from_slice(&sc.span_id().to_bytes());
        buf.push(sc.trace_flags().to_u8());
        buf.extend_from_slice(&[0u8; 7]); // padding / reserved

        let frame_bytes =
            postcard::to_allocvec(self).expect("InternalMeshFrame encode is infallible");
        buf.extend_from_slice(&frame_bytes);
        buf
    }

    /// Decode frame + reconstruct OTel parent context from wire header.
    /// Returns `(parent_ctx, frame)`. Sub-32-byte input is treated as decode failure.
    pub fn decode_with_context(bytes: &[u8]) -> Result<(Context, Self), postcard::Error> {
        if bytes.len() < HEADER_LEN {
            return Err(postcard::Error::DeserializeUnexpectedEnd);
        }

        let trace_id = TraceId::from_bytes(bytes[0..16].try_into().unwrap());
        let span_id = SpanId::from_bytes(bytes[16..24].try_into().unwrap());
        let flags = TraceFlags::new(bytes[24]);

        let sc = SpanContext::new(trace_id, span_id, flags, true, TraceState::default());
        let parent_ctx = Context::new().with_remote_span_context(sc);

        let frame = postcard::from_bytes(&bytes[HEADER_LEN..])?;
        Ok((parent_ctx, frame))
    }
}
