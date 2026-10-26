//! The fabric's states (states.md; lifecycles/fabric-ready-for-traffic.md).
//!
//! A fabric is `pending`, then `ready-for-traffic` once every mesh the accepted Build names is
//! ready-for-traffic (Q174). Only the fabric-primary moves it.

/// The state of the fabric: the value of a FabricStatus frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum FabricState {
    /// Created; a mesh the accepted Build names is not yet ready-for-traffic.
    #[default]
    Pending,
    /// Every mesh the accepted Build names is ready-for-traffic.
    ReadyForTraffic,
}

impl FabricState {
    /// The name the FabricStatus frame, the control API and the spans carry.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::ReadyForTraffic => "ready-for-traffic",
        }
    }

    /// The state a FabricStatus frame's value names, when it names one.
    pub fn parse(s: &str) -> Option<Self> {
        [Self::Pending, Self::ReadyForTraffic].into_iter().find(|f| f.as_str() == s)
    }

    /// The view's scope status for this state.
    pub fn scope(self) -> crate::model::ScopeStatus {
        use crate::model::ScopeStatus as S;
        match self {
            Self::Pending => S::Pending,
            Self::ReadyForTraffic => S::ReadyForTraffic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_states_are_ordered_and_named_as_the_frame_carries_them() {
        assert!(FabricState::Pending < FabricState::ReadyForTraffic);
        for s in [FabricState::Pending, FabricState::ReadyForTraffic] {
            assert_eq!(FabricState::parse(s.as_str()), Some(s));
            assert_eq!(serde_json::to_value(s.scope()).unwrap(), serde_json::json!(s.as_str()), "the view's scope status names the state the same way");
        }
        assert_eq!(FabricState::parse("degraded"), None);
    }
}
