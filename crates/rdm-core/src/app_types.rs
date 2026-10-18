//! Immutable type declarations and the hash carried by a digest.
use std::collections::BTreeSet;
/// Validate a declaration before admitting any part of it.
pub fn check(types: &[String]) -> Result<(), crate::outcome::MalformedKind> {
    if types.len() > 256 || postcard::to_allocvec(types).map_or(true, |b| b.len() > 8192) {
        return Err(crate::outcome::MalformedKind::TypesOverCap);
    }
    if types
        .iter()
        .any(|s| s.is_empty() || s.starts_with("rdm.") || s.chars().any(char::is_control))
    {
        return Err(crate::outcome::MalformedKind::Corrupt);
    }
    Ok(())
}
/// Blake3 of the canonical sorted postcard list; no list rides the digest.
pub fn hash(types: &BTreeSet<String>) -> [u8; 32] {
    *blake3::hash(&postcard::to_allocvec(types).expect("strings encode")).as_bytes()
}
