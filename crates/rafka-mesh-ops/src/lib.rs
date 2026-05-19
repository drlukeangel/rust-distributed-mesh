use bincode::error::DecodeError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, bincode::Encode, bincode::Decode)]
pub enum InternalMeshFrame {
    Ping { org_id: u64 },
    Pong { org_id: u64 },
}

impl InternalMeshFrame {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .expect("InternalMeshFrame encode is infallible")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let (frame, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
        Ok(frame)
    }
}
