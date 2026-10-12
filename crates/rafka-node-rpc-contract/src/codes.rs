//! The reserved Node RPC reset/stop code namespace (ownership amendment §12).

/// A code Node RPC itself emits on a stream reset or stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ResetCode {
    /// No catalog entry serves the op; proves no protocol dispatch.
    UnservedOp = 421,
    /// The server stops the request body after a typed early refusal;
    /// flow-control hygiene, never result certainty.
    RequestStop = 422,
    /// A supervised unexpected runtime/handler fault after dispatch.
    InternalRpcFailure = 423,
    /// A framing/order/runtime contract violation.
    ProtocolViolation = 424,
    /// The sender reset an unfinished request; proves pre-dispatch `NotSent`.
    FrameNotSent = 499,
}

/// Historical codes: reserved forever, never emitted, never reassigned. `425` was the receiver's
/// refusal of a fence naming another node; no receiver refuses a call for the node id it names
/// (R-UC1a), so a `425` a caller reads proves nothing.
#[cfg(test)]
pub(crate) const RETIRED_CODES: [u32; 4] = [413, 425, 501, 503];

impl ResetCode {
    /// Every code Node RPC emits.
    pub const ALL: [ResetCode; 5] = [Self::UnservedOp, Self::RequestStop, Self::InternalRpcFailure, Self::ProtocolViolation, Self::FrameNotSent];

    /// The numeric code.
    pub fn code(self) -> u32 {
        self as u32
    }

    /// The code's canonical name.
    pub fn name(self) -> &'static str {
        match self {
            Self::UnservedOp => "UNSERVED_OP",
            Self::RequestStop => "REQUEST_STOP",
            Self::InternalRpcFailure => "INTERNAL_RPC_FAILURE",
            Self::ProtocolViolation => "PROTOCOL_VIOLATION",
            Self::FrameNotSent => "FRAME_NOT_SENT",
        }
    }

    /// The code a reset/stop carried, if it is one of ours. A retired code is
    /// recognised by number but is not a current `ResetCode`.
    pub(crate) fn from_code(code: u64) -> Option<Self> {
        Self::ALL.into_iter().find(|c| u64::from(c.code()) == code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_is_421_to_424_plus_499() {
        let codes: Vec<u32> = ResetCode::ALL.iter().map(|c| c.code()).collect();
        assert_eq!(codes, vec![421, 422, 423, 424, 499]);
        for c in ResetCode::ALL {
            assert_eq!(ResetCode::from_code(c.code().into()), Some(c));
        }
    }

    #[test]
    fn retired_codes_are_never_current() {
        for r in RETIRED_CODES {
            assert_eq!(ResetCode::from_code(r.into()), None);
            assert!(!ResetCode::ALL.iter().any(|c| c.code() == r));
        }
    }
}
