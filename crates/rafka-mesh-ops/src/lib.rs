use opentelemetry::{global, Context};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    /// Application write-sim frame (mesh-v2 Phase 2). A gateway resolves a target
    /// node's location from the gossiped topology cache and sends this directly.
    /// `from` is the sender's node_name, `to` the target's node_name, `seq` a
    /// monotonic counter. Surfaces in the Messages tab; maps to op_kind="produce".
    Write { from: String, to: String, seq: u64 },
    /// Broker→gateway acknowledgement (sprint-13 B5). Written back on the response
    /// half of the produce bi-stream after the broker handles the Write. `from` is
    /// the broker's node_name, `seq` echoes the produce seq. Maps to op_kind="ack".
    /// The frame's W3C carrier holds the BROKER's produce.ack span context, so the
    /// gateway's ack-receive span becomes a child of it → broker→gateway edge.
    Ack { from: String, seq: u64 },
    /// Control-plane shutdown op. ANY mesh participant (e.g. an admin-ui console)
    /// dials a target node by node_id and sends this; the target shuts ITSELF down
    /// gracefully (emits node.stopping, broadcasts its own tombstone, exits). This
    /// is how a node is killed across meshes WITHOUT the caller owning the OS
    /// process — the self-aware-fleet replacement for TerminateProcess-on-own-child.
    /// `reason` identifies the requester (e.g. "operator:mesh1.admin-ui.b0b781").
    /// Maps to op_kind="control".
    Shutdown { reason: String },
}

/// W3C trace-context carrier embedded with every traced frame (sprint-13 B3).
///
/// Replaces the old hand-rolled `TraceContext { trace_id, span_id, flags }`
/// struct. The carrier is the standard W3C key→value map that the global
/// `TextMapPropagator` (`TraceContextPropagator`, installed in rafka-telemetry)
/// writes/reads — i.e. `traceparent` AND `tracestate`. This makes mesh hops use
/// the SAME propagation mechanism as HTTP hops, and (unlike the old struct) it
/// preserves `tracestate` end-to-end.
///
/// Implements `opentelemetry::propagation::{Injector, Extractor}` so it can be
/// handed straight to `inject_context` / `extract`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct W3CCarrier {
    pub fields: HashMap<String, String>,
}

impl opentelemetry::propagation::Injector for W3CCarrier {
    fn set(&mut self, key: &str, value: String) {
        self.fields.insert(key.to_string(), value);
    }
}

impl opentelemetry::propagation::Extractor for W3CCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(|s| s.as_str())
    }
    fn keys(&self) -> Vec<&str> {
        self.fields.keys().map(|s| s.as_str()).collect()
    }
}

/// The on-wire shape for tag `0x10` (TAG_LEGACY_FRAME): a W3C carrier paired
/// with an `InternalMeshFrame`. Postcard-encodes cleanly (the carrier is a
/// `HashMap<String,String>`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracedFrame {
    pub carrier: W3CCarrier,
    pub inner: InternalMeshFrame,
}

impl InternalMeshFrame {
    /// Encode frame with OTel context using the framed wire format:
    /// `tag(0x10) + varint(len) + postcard(TracedFrame { carrier, inner })`.
    ///
    /// Sprint-13 B3: injects `ctx` into a W3C carrier via the GLOBAL text-map
    /// propagator (`traceparent` + `tracestate`). Caller passes the context whose
    /// active span should become the remote parent on the receiving side.
    pub fn encode_with_context(&self, ctx: &Context) -> Vec<u8> {
        let mut carrier = W3CCarrier::default();
        global::get_text_map_propagator(|prop| prop.inject_context(ctx, &mut carrier));
        let traced = TracedFrame {
            carrier,
            inner: self.clone(),
        };
        framer::encode(framer::TAG_LEGACY_FRAME, &traced)
    }

    /// Decode a framed `tag(0x10)` stream payload, reconstructing the OTel
    /// parent context via the GLOBAL propagator's `extract` (W3C traceparent +
    /// tracestate). Unknown tags raise `FramerError::Postcard`. The reader layer
    /// demuxes on tag BEFORE calling decode — this function assumes 0x10.
    pub fn decode_with_context(bytes: &[u8]) -> Result<(Context, Self), framer::FramerError> {
        let (tag, traced, _consumed) = framer::decode::<TracedFrame>(bytes)?;
        debug_assert_eq!(tag, framer::TAG_LEGACY_FRAME, "demuxer must route 0x10 here");
        let parent_ctx =
            global::get_text_map_propagator(|prop| prop.extract(&traced.carrier));
        Ok((parent_ctx, traced.inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{
        SpanContext, TraceContextExt, TraceFlags, TraceId, SpanId, TraceState,
    };

    fn install_w3c_propagator() {
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
    }

    /// Sprint-13 B3: encode injects via the global W3C propagator, decode
    /// extracts via it. A real (sampled) remote span context round-trips through
    /// the `traceparent` field — trace_id/span_id/sampled survive.
    #[test]
    fn traced_frame_round_trip_ping_w3c() {
        install_w3c_propagator();
        let ctx = Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from_bytes([0xAB; 16]),
            SpanId::from_bytes([0xCD; 8]),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ));
        let frame = InternalMeshFrame::Ping { org_id: 42 };
        let encoded = frame.encode_with_context(&ctx);
        assert_eq!(encoded[0], framer::TAG_LEGACY_FRAME);
        let (ctx2, frame2) = InternalMeshFrame::decode_with_context(&encoded).unwrap();
        let sc = ctx2.span().span_context().clone();
        assert_eq!(sc.trace_id().to_bytes(), [0xAB; 16]);
        assert_eq!(sc.span_id().to_bytes(), [0xCD; 8]);
        assert!(sc.is_sampled());
        match frame2 {
            InternalMeshFrame::Ping { org_id } => assert_eq!(org_id, 42),
            other => panic!("expected Ping, got {other:?}"),
        }
    }

    /// `tracestate` must survive the round trip — the whole point of B3 over the
    /// old hand-rolled struct (which hardcoded `TraceState::default()`).
    #[test]
    fn traced_frame_preserves_tracestate() {
        install_w3c_propagator();
        let tracestate = TraceState::from_key_value(vec![("vendor", "v1")]).unwrap();
        let ctx = Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from_bytes([0x11; 16]),
            SpanId::from_bytes([0x22; 8]),
            TraceFlags::SAMPLED,
            true,
            tracestate,
        ));
        let encoded = InternalMeshFrame::Ping { org_id: 7 }.encode_with_context(&ctx);
        let (ctx2, _) = InternalMeshFrame::decode_with_context(&encoded).unwrap();
        let sc = ctx2.span().span_context().clone();
        assert_eq!(sc.trace_state().get("vendor"), Some("v1"),
            "tracestate must survive the W3C round trip");
    }

    #[test]
    fn traced_frame_round_trip_hello() {
        install_w3c_propagator();
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
