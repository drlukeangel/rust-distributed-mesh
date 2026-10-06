//! Server-streaming order (node-rpc.md §11.2, §31; i143.e6.s3).
//!
//! A streaming protocol's reply frames are its own `Reply` enum; it classifies each as a refusal,
//! `Started`, `Data` or `Terminal`. The legal orders are one `Refusal`, or `Started Data* Terminal`.
//! There is no generic START/DATA/ERROR envelope: the runtime enforces order with the classifier.

use crate::outcome::ReplyKind;
use crate::protocol::NodeProtocol;

/// How a streaming protocol classifies one reply frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// A refusal: legal only as the first and only frame.
    Refusal(ReplyKind),
    Started,
    Data,
    Terminal,
}

/// A server-streaming protocol: its reply frames are `Self::Reply`.
pub trait StreamingProtocol: NodeProtocol {
    fn frame_kind(frame: &Self::Reply) -> FrameKind;
}

/// Where a stream stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    NotStarted,
    Streaming,
    Done,
}

/// The order enforcer one side of a stream runs over the frames it sends or receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameOrder {
    state: OrderState,
}

impl Default for FrameOrder {
    fn default() -> Self {
        Self { state: OrderState::NotStarted }
    }
}

impl FrameOrder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> OrderState {
        self.state
    }

    /// Admit the next frame of kind `kind`, or name the violation.
    pub fn admit(&mut self, kind: FrameKind) -> Result<(), String> {
        let next = match (self.state, kind) {
            (OrderState::NotStarted, FrameKind::Refusal(_)) => OrderState::Done,
            (OrderState::NotStarted, FrameKind::Started) => OrderState::Streaming,
            (OrderState::Streaming, FrameKind::Data) => OrderState::Streaming,
            (OrderState::Streaming, FrameKind::Terminal) => OrderState::Done,
            (OrderState::NotStarted, FrameKind::Data) => return Err("Data before Started".into()),
            (OrderState::NotStarted, FrameKind::Terminal) => return Err("Terminal before Started".into()),
            (OrderState::Streaming, FrameKind::Started) => return Err("a second Started".into()),
            (OrderState::Streaming, FrameKind::Refusal(_)) => return Err("a refusal after Started".into()),
            (OrderState::Done, k) => return Err(format!("{k:?} after the stream ended")),
        };
        self.state = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_legal_orders_are_admitted() {
        let mut o = FrameOrder::new();
        assert!(o.admit(FrameKind::Refusal(ReplyKind::Busy)).is_ok());
        assert_eq!(o.state(), OrderState::Done);
        let mut o = FrameOrder::new();
        for k in [FrameKind::Started, FrameKind::Data, FrameKind::Data, FrameKind::Terminal] {
            o.admit(k).unwrap();
        }
        assert_eq!(o.state(), OrderState::Done);
    }

    #[test]
    fn every_illegal_order_is_named() {
        assert!(FrameOrder::new().admit(FrameKind::Data).unwrap_err().contains("before Started"));
        let mut o = FrameOrder::new();
        o.admit(FrameKind::Started).unwrap();
        assert!(o.admit(FrameKind::Started).unwrap_err().contains("second Started"));
        assert!(o.admit(FrameKind::Refusal(ReplyKind::Busy)).unwrap_err().contains("after Started"));
        o.admit(FrameKind::Terminal).unwrap();
        assert!(o.admit(FrameKind::Data).unwrap_err().contains("after the stream ended"));
    }
}
