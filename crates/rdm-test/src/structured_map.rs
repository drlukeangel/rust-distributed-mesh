//! Ordered map entries with structured keys, without stringifying their fields.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

pub fn serialize<K: Serialize, V: Serialize, S: Serializer>(map: &BTreeMap<K, V>, serializer: S) -> Result<S::Ok, S::Error> {
    map.iter().collect::<Vec<_>>().serialize(serializer)
}

pub fn deserialize<'de, K: Deserialize<'de> + Ord, V: Deserialize<'de>, D: Deserializer<'de>>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error> {
    let entries = Vec::<(K, V)>::deserialize(deserializer)?;
    let mut map = BTreeMap::new();
    for (key, value) in entries {
        if map.insert(key, value).is_some() {
            return Err(serde::de::Error::custom("duplicate structured map key"));
        }
    }
    Ok(map)
}
