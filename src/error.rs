/// Errors returned by ParticleDB SDK operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    #[error("Not found")]
    NotFound,

    /// An error the server answered with. `sqlstate` is the five-character
    /// SQLSTATE (`23505`, `42P01`, `40001`, ...): decoded from the native error
    /// frame's code, or from a trailing ` [XXXXX]` marker in the message.
    /// `"HY000"` (general error) when the server named no class -- a
    /// `MSG_BATCH_RESULT` entry carries a message but no code. `code` is the raw
    /// native frame code when there was one.
    #[error("Server error [{sqlstate}]: {message}")]
    Server {
        sqlstate: String,
        code: Option<u16>,
        message: String,
    },

    /// A prepared bulk call ([`crate::SqlExecutor::execute_many`]) in which at
    /// least one parameter row failed. Outside a transaction every row is its
    /// own autocommit statement, so `applied` rows DID commit even though the
    /// call failed; see [`BatchFailure`].
    #[error("Batch error: {0}")]
    Batch(BatchFailure),
}

/// The outcome of a prepared bulk call in which a row failed.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchFailure {
    /// Rows sent in the call.
    pub rows: usize,
    /// Affected-row count summed over the rows that succeeded (and, for an
    /// autocommit call, committed).
    pub applied: u64,
    /// Every failed row as `(row index in the caller's slice, SQLSTATE,
    /// message)`, in row order. At most [`BatchFailure::MAX_RECORDED`] are
    /// kept; `failed` is the full count.
    pub errors: Vec<(usize, String, String)>,
    /// Number of rows that failed.
    pub failed: usize,
    /// Rows never attempted: a transactional call stops at the first failure,
    /// and a frame the server refused as a whole (a statement that does not
    /// prepare, a failed transaction) is recorded as ONE failure against its
    /// first row with every later row counted here.
    pub not_attempted: usize,
}

impl BatchFailure {
    /// Cap on the per-row errors recorded in [`BatchFailure::errors`].
    pub const MAX_RECORDED: usize = 100;
}

impl std::fmt::Display for BatchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} of {} rows failed, {} not attempted, {} affected by the rows that succeeded",
            self.failed, self.rows, self.not_attempted, self.applied
        )?;
        if let Some((row, state, msg)) = self.errors.first() {
            write!(f, "; first failure: row {row} [{state}] {msg}")?;
        }
        Ok(())
    }
}

impl Error {
    /// The SQLSTATE of a server error (`Server`), or of the first failed row
    /// of a `Batch`. `None` for client-side errors (connection, conversion).
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Error::Server { sqlstate, .. } => Some(sqlstate),
            Error::Batch(b) => b.errors.first().map(|(_, s, _)| s.as_str()),
            _ => None,
        }
    }
}

/// A convenience alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_error_display() {
        let err = Error::Connection("refused".into());
        assert_eq!(err.to_string(), "Connection error: refused");
    }

    #[test]
    fn test_query_error_display() {
        let err = Error::Query("syntax error".into());
        assert_eq!(err.to_string(), "Query error: syntax error");
    }

    #[test]
    fn test_type_conversion_error_display() {
        let err = Error::TypeConversion("expected i64".into());
        assert_eq!(err.to_string(), "Type conversion error: expected i64");
    }

    #[test]
    fn test_not_found_display() {
        let err = Error::NotFound;
        assert_eq!(err.to_string(), "Not found");
    }
}
