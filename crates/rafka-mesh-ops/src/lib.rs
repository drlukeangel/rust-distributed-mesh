use opentelemetry::{
    trace::{SpanContext, TraceContextExt, TraceFlags, TraceId, SpanId, TraceState},
    Context,
};
use serde::{Deserialize, Serialize};

pub mod framer;
pub use framer::{decode as framer_decode, encode as framer_encode, FramerError, TAG_LEGACY_FRAME};

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

/// Sender's OTel context embedded with every traced frame so the receiver can
/// `set_parent()` on its consumer span — preserves per-record observability
/// across QUIC hops per golden principle #7.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub flags: u8,
}

/// The on-wire shape for tag `0x10` (TAG_LEGACY_FRAME): a trace context paired
/// with an `InternalMeshFrame`. Postcard-encodes cleanly because every field
/// is a fixed primitive or sum-type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracedFrame {
    pub ctx: TraceContext,
    pub inner: InternalMeshFrame,
}

impl InternalMeshFrame {
    /// Encode frame with OTel context using the new framed wire format:
    /// `tag(0x10) + varint(len) + postcard(TracedFrame { ctx, inner })`.
    /// Caller must enter the sending span before calling so ctx carries the
    /// correct span to propagate.
    pub fn encode_with_context(&self, ctx: &Context) -> Vec<u8> {
        let span = ctx.span();
        let sc = span.span_context();
        let traced = TracedFrame {
            ctx: TraceContext {
                trace_id: sc.trace_id().to_bytes(),
                span_id: sc.span_id().to_bytes(),
                flags: sc.trace_flags().to_u8(),
            },
            inner: self.clone(),
        };
        framer::encode(framer::TAG_LEGACY_FRAME, &traced)
    }

    /// Decode a framed `tag(0x10)` stream payload, reconstructing the OTel
    /// parent context. Unknown tags raise `FramerError::Postcard` because the
    /// payload won't deserialize as `TracedFrame`. The reader layer is expected
    /// to demux on tag BEFORE calling decode — this function assumes 0x10.
    pub fn decode_with_context(bytes: &[u8]) -> Result<(Context, Self), framer::FramerError> {
        let (tag, traced, _consumed) = framer::decode::<TracedFrame>(bytes)?;
        debug_assert_eq!(tag, framer::TAG_LEGACY_FRAME, "demuxer must route 0x10 here");
        let trace_id = TraceId::from_bytes(traced.ctx.trace_id);
        let span_id = SpanId::from_bytes(traced.ctx.span_id);
        let flags = TraceFlags::new(traced.ctx.flags);
        let sc = SpanContext::new(trace_id, span_id, flags, true, TraceState::default());
        let parent_ctx = Context::new().with_remote_span_context(sc);
        Ok((parent_ctx, traced.inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sender encodes via the public API; receiver decodes via the public API.
    /// Wire format = framer (tag 0x10 + varint + postcard TracedFrame).
    /// Trace context survives the round trip byte-identical.
    #[test]
    fn traced_frame_round_trip_ping() {
        let ctx = Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from_bytes([0xAB; 16]),
            SpanId::from_bytes([0xCD; 8]),
            TraceFlags::new(0x01),
            true,
            TraceState::default(),
        ));
        let frame = InternalMeshFrame::Ping { org_id: 42 };
        let encoded = frame.encode_with_context(&ctx);
        // First byte must be the legacy-frame tag.
        assert_eq!(encoded[0], framer::TAG_LEGACY_FRAME);
        let (ctx2, frame2) = InternalMeshFrame::decode_with_context(&encoded).unwrap();
        let sc = ctx2.span().span_context().clone();
        assert_eq!(sc.trace_id().to_bytes(), [0xAB; 16]);
        assert_eq!(sc.span_id().to_bytes(), [0xCD; 8]);
        assert_eq!(sc.trace_flags().to_u8(), 0x01);
        match frame2 {
            InternalMeshFrame::Ping { org_id } => assert_eq!(org_id, 42),
            other => panic!("expected Ping, got {other:?}"),
        }
    }

    #[test]
    fn traced_frame_round_trip_hello() {
        let ctx = Context::new();
        let frame = InternalMeshFrame::Hello {
            mesh_id: "mesh-A".into(),
            node_type: "broker".into(),
        };
        let encoded = frame.encode_with_context(&ctx);
        let (_, frame2) = InternalMeshFrame::decode_with_context(&encoded).unwrap();
        match frame2 {
            InternalMeshFrame::Hello { mesh_id, node_type } => {
                assert_eq!(mesh_id, "mesh-A");
                assert_eq!(node_type, "broker");
            }
            other => panic!("expected Hello, got {other:?}"),
        }
    }

    /// Demuxer hostility: a frame with a non-legacy tag must NOT decode as
    /// TracedFrame — caller is responsible for tag-based routing.
    #[test]
    fn unknown_tag_fails_decode() {
        let bogus = framer::encode(0x42, &"not a TracedFrame");
        let result = InternalMeshFrame::decode_with_context(&bogus);
        assert!(result.is_err(), "non-0x10 tag must not deserialize as TracedFrame");
    }
}
