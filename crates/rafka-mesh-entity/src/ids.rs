//! Opaque identities, compared by equality only.

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! opaque_id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            /// Mint a fresh id: 128 random bits, lower-case hex.
            pub fn mint() -> Self {
                Self(hex::encode(rand::random::<[u8; 16]>()))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

opaque_id!(
    /// Logical node identity: minted once, kept across restarts, never reused.
    NodeId
);
opaque_id!(
    /// One process birth of a logical node (`RuntimeIncarnationId`).
    IncarnationId
);
opaque_id!(
    /// Authenticated transport (Iroh) identity of the node.
    FabricId
);
opaque_id!(
    /// Freshness of one endpoint-slot assignment.
    FreshnessToken
);
