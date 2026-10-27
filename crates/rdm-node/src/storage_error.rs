//! The error every storage boundary answers: one type for every backend (the file backend, the
//! memory backends, Rafka's topic backends), each failure named with what it touched.

/// Why a storage read or write failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// The file could not be read or written.
    Io {
        /// The file.
        file: String,
        /// Why it failed.
        reason: String,
    },
    /// A file this build does not recognise: refused, never read as a value.
    Unrecognised {
        /// The file.
        file: String,
        /// Why it is not recognised.
        reason: String,
    },
    /// The backend answered and refused the read or write by name (it cannot list the store, or it
    /// refuses the call): asked again, it answers the same.
    Refused {
        /// What the read or write touched.
        file: String,
        /// The refusal, as the backend names it.
        reason: String,
    },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { file, reason } => write!(f, "{file}: {reason}"),
            Self::Unrecognised { file, reason } => write!(f, "{file} is not a record this build recognises: {reason}"),
            Self::Refused { file, reason } => write!(f, "{file} is refused: {reason}"),
        }
    }
}
