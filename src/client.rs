use crate::ai::AIOps;
use crate::error::{BatchFailure, Error, Result};
use crate::kv::KVOps;
use crate::rag::RAGOps;
use crate::types::{QueryResult, Row, Value};
use crate::vector::VectorOps;

/// Connection configuration for ParticleDB.
#[derive(Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: usize,
    /// Maximum wait for a connection or a pool slot.
    pub connect_timeout: std::time::Duration,
    /// Maximum wait for each socket read or write.
    pub io_timeout: std::time::Duration,
    /// Verified TLS. A private CA can be supplied in PEM format.
    pub tls: Option<TlsConfig>,
    /// For a local `host` (`localhost`, `127.x`, `::1`) connect over the
    /// server's Unix socket `<unix_socket_dir>/.s.PDB.<port>` when that file
    /// exists, else TCP. `false` always uses TCP. Default `true`.
    pub prefer_unix_socket: bool,
    /// The server's `--unix-socket-dir`. Default `/tmp`.
    pub unix_socket_dir: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            // The PDB native wire port (`PdbWireExecutor`); 5432 is the
            // PostgreSQL wire, which this SDK does not speak.
            port: crate::pdb_wire_executor::DEFAULT_PORT,
            user: "particledb".into(),
            password: String::new(),
            database: "particledb".into(),
            pool_size: 4,
            connect_timeout: std::time::Duration::from_secs(10),
            io_timeout: std::time::Duration::from_secs(30),
            tls: None,
            prefer_unix_socket: true,
            unix_socket_dir: crate::pdb_wire_executor::DEFAULT_UNIX_SOCKET_DIR.into(),
        }
    }
}

/// TLS verifies the certificate chain and the configured host. There is no plaintext fallback.
#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    /// Additional trusted root certificate, in PEM format.
    pub root_cert: Option<std::path::PathBuf>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"[redacted]")
            .field("database", &self.database)
            .field("pool_size", &self.pool_size)
            .field("connect_timeout", &self.connect_timeout)
            .field("io_timeout", &self.io_timeout)
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

impl Config {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.host.is_empty()
            || self.port == 0
            || self.pool_size == 0
            || self.connect_timeout.is_zero()
            || self.io_timeout.is_zero()
        {
            return Err(Error::Connection(
                "host, port, pool_size and timeouts must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

/// Trait abstracting SQL execution, enabling mock implementations for testing.
pub trait SqlExecutor {
    /// Execute a query that returns rows.
    fn execute_query(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Execute a statement that returns the number of rows affected.
    fn execute_statement(&self, sql: &str, params: &[Value]) -> Result<u64>;

    /// Execute `sql` once per parameter row (bulk insert / update) and return
    /// the summed affected-row count. Each row is its own statement: rows that
    /// succeed stay applied when another row fails, and any failure is
    /// returned as [`Error::Batch`] describing which rows failed and how many
    /// rows were applied.
    ///
    /// The default runs [`SqlExecutor::execute_statement`] per row;
    /// `PdbWireExecutor` sends the rows as `MSG_EXECUTE_BATCH` frames.
    fn execute_many(&self, sql: &str, rows: &[Vec<Value>]) -> Result<u64> {
        let mut acc = BatchFailure {
            rows: rows.len(),
            applied: 0,
            errors: Vec::new(),
            failed: 0,
            not_attempted: 0,
        };
        for (i, row) in rows.iter().enumerate() {
            match self.execute_statement(sql, row) {
                Ok(n) => acc.applied += n,
                Err(e @ Error::Connection(_)) => return Err(e),
                Err(e) => {
                    acc.failed += 1;
                    if acc.errors.len() < BatchFailure::MAX_RECORDED {
                        let state = e.sqlstate().unwrap_or("HY000").to_string();
                        acc.errors.push((i, state, e.to_string()));
                    }
                }
            }
        }
        if acc.failed > 0 {
            Err(Error::Batch(acc))
        } else {
            Ok(acc.applied)
        }
    }
}

/// The main ParticleDB client, parameterised over an [`SqlExecutor`] for testability.
pub struct ParticleDB<E: SqlExecutor = crate::pdb_wire_executor::PdbWireExecutor> {
    config: Config,
    executor: E,
}

impl ParticleDB<crate::pdb_wire_executor::PdbWireExecutor> {
    /// Connect using the native protocol and verify the server is reachable.
    pub fn connect(config: Config) -> Result<Self> {
        config.validate()?;
        let executor = crate::pdb_wire_executor::PdbWireExecutor::new(config.clone());
        executor.ping()?;
        Ok(Self { config, executor })
    }

    /// Run a callback on one connection. A failed callback rolls back.
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&crate::pdb_wire_executor::Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        self.executor.transaction(f)
    }
}

impl ParticleDB<MockExecutor> {
    /// Create a client backed by a [`MockExecutor`] (useful for unit tests).
    pub fn mock(config: Config) -> Self {
        Self {
            config,
            executor: MockExecutor::new(),
        }
    }
}

impl<E: SqlExecutor> ParticleDB<E> {
    /// Create a client with the given executor.
    pub fn with_executor(config: Config, executor: E) -> Self {
        Self { config, executor }
    }

    /// Execute a query that returns rows.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        self.executor.execute_query(sql, params)
    }

    /// Execute a query and return at most one row.
    pub fn query_one(&self, sql: &str, params: &[Value]) -> Result<Option<Row>> {
        let result = self.executor.execute_query(sql, params)?;
        Ok(result.rows.into_iter().next())
    }

    /// Execute a statement (INSERT, UPDATE, DELETE, DDL) and return the number of rows affected.
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        self.executor.execute_statement(sql, params)
    }

    /// Execute `sql` once per parameter row; see [`SqlExecutor::execute_many`].
    pub fn execute_many(&self, sql: &str, rows: &[Vec<Value>]) -> Result<u64> {
        self.executor.execute_many(sql, rows)
    }

    /// Access vector search operations.
    pub fn vector(&self) -> VectorOps<'_, E> {
        VectorOps::new(self)
    }

    /// Access key-value operations.
    pub fn kv(&self) -> KVOps<'_, E> {
        KVOps::new(self)
    }

    /// Access RAG operations.
    pub fn rag(&self) -> RAGOps<'_, E> {
        RAGOps::new(self)
    }

    /// Access AI function operations.
    pub fn ai(&self) -> AIOps<'_, E> {
        AIOps::new(self)
    }

    /// Build a PostgreSQL-compatible connection string from the current config.
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.config.host,
            self.config.port,
            self.config.user,
            self.config.password,
            self.config.database
        )
    }

    /// Return a reference to the underlying executor.
    pub fn executor(&self) -> &E {
        &self.executor
    }

    /// Return a reference to the current configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }
}

/// A mock SQL executor that returns pre-configured responses, for testing SDK
/// logic without a live database.
pub struct MockExecutor {
    responses: std::sync::Mutex<Vec<QueryResult>>,
}

impl MockExecutor {
    /// Create a new empty mock executor.
    pub fn new() -> Self {
        Self {
            responses: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Push a response that will be returned by the next query or statement call (FIFO order).
    pub fn push_response(&self, result: QueryResult) {
        self.responses.lock().unwrap().push(result);
    }

    fn pop_response(&self) -> Result<QueryResult> {
        let mut responses = self.responses.lock().unwrap();
        if responses.is_empty() {
            Ok(QueryResult::empty())
        } else {
            Ok(responses.remove(0))
        }
    }
}

impl Default for MockExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlExecutor for MockExecutor {
    fn execute_query(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult> {
        self.pop_response()
    }

    fn execute_statement(&self, _sql: &str, _params: &[Value]) -> Result<u64> {
        let result = self.pop_response()?;
        Ok(result.rows_affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 7173, "the default is the native wire port");
        assert_eq!(cfg.user, "particledb");
        assert_eq!(cfg.password, "");
        assert_eq!(cfg.database, "particledb");
        assert_eq!(cfg.pool_size, 4);
    }

    #[test]
    fn test_connection_string() {
        let cfg = Config {
            host: "myhost".into(),
            port: 5433,
            user: "admin".into(),
            password: "secret".into(),
            database: "mydb".into(),
            pool_size: 8,
            ..Config::default()
        };
        let db = ParticleDB::with_executor(cfg, MockExecutor::new());
        assert_eq!(
            db.connection_string(),
            "host=myhost port=5433 user=admin password=secret dbname=mydb"
        );
    }

    #[test]
    fn test_mock_executor_push_pop() {
        let mock = MockExecutor::new();
        let qr = QueryResult {
            rows: vec![Row::new(vec!["id".into()], vec![Value::Int64(1)])],
            columns: vec!["id".into()],
            rows_affected: 0,
        };
        mock.push_response(qr);

        let result = mock.execute_query("SELECT 1", &[]).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 1);
    }

    #[test]
    fn test_mock_executor_empty_returns_empty_result() {
        let mock = MockExecutor::new();
        let result = mock.execute_query("SELECT 1", &[]).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_query_one_returns_first_row() {
        let db = ParticleDB::mock(Config::default());
        let qr = QueryResult {
            rows: vec![
                Row::new(vec!["v".into()], vec![Value::Int64(10)]),
                Row::new(vec!["v".into()], vec![Value::Int64(20)]),
            ],
            columns: vec!["v".into()],
            rows_affected: 0,
        };
        db.executor().push_response(qr);

        let row = db.query_one("SELECT v FROM t", &[]).unwrap().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 10);
    }

    #[test]
    fn test_query_one_returns_none_for_empty() {
        let db = ParticleDB::mock(Config::default());
        let result = db.query_one("SELECT 1 WHERE false", &[]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_execute_returns_rows_affected() {
        let db = ParticleDB::mock(Config::default());
        let qr = QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
            rows_affected: 5,
        };
        db.executor().push_response(qr);

        let affected = db.execute("DELETE FROM t WHERE id < 10", &[]).unwrap();
        assert_eq!(affected, 5);
    }

    #[test]
    fn test_mock_executor_fifo_order() {
        let mock = MockExecutor::new();
        mock.push_response(QueryResult {
            rows: vec![Row::new(vec!["v".into()], vec![Value::Int64(1)])],
            columns: vec!["v".into()],
            rows_affected: 0,
        });
        mock.push_response(QueryResult {
            rows: vec![Row::new(vec!["v".into()], vec![Value::Int64(2)])],
            columns: vec!["v".into()],
            rows_affected: 0,
        });

        let r1 = mock.execute_query("q1", &[]).unwrap();
        let r2 = mock.execute_query("q2", &[]).unwrap();
        assert_eq!(r1.rows[0].get::<i64>(0).unwrap(), 1);
        assert_eq!(r2.rows[0].get::<i64>(0).unwrap(), 2);
    }
}
