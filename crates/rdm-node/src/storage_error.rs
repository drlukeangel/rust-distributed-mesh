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
    /// A write refused because this store's writer gave up its authority (a seat transfer's
    /// cut-off, R14 item 4): it never reached the disk.
    CutOff {
        /// The file.
        file: String,
        /// The cut-off's reason.
        reason: String,
    },
    /// A write refused because this writer does not hold the seat that writes the row (R14 item 2:
    /// the writers are exactly the writer ladder): it never reached the disk.
    NotSeatHolder {
        /// The file.
        file: String,
        /// The seat and what the committed seat record says of this writer.
        reason: String,
    },
    /// A file this build does not recognise: refused, never read as a value.
    Unrecognised {
        /// The file.
        file: String,
        /// Why it is not recognised.
        reason: String,
    },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { file, reason } => write!(f, "{file}: {reason}"),
            Self::CutOff { file, reason } => write!(f, "{file}: this store's writes are cut off: {reason}"),
            Self::NotSeatHolder { file, reason } => write!(f, "{file}: this writer does not hold the seat that writes it: {reason}"),
            Self::Unrecognised { file, reason } => write!(f, "{file} is not a record this build recognises: {reason}"),
        }
    }
}
