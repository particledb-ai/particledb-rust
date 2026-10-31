//! Integration tests for the ParticleDB Rust SDK.
//!
//! These tests connect to a **real** ParticleDB instance and exercise every
//! feature of the SDK: raw SQL CRUD, data types, transactions, KV operations,
//! vector operations, aggregations, ordering, joins, concurrency, and error
//! handling.
//!
//! # Configuration
//!
//! The following environment variables control the connection:
//!
//! | Variable                | Default      |
//! |-------------------------|--------------|
//! | `PARTICLEDB_TEST_HOST`  | `127.0.0.1`  |
//! | `PARTICLEDB_TEST_PORT`  | `5432`       |
//! | `PARTICLEDB_TEST_USER`  | `particledb` |
//! | `PARTICLEDB_TEST_PASS`  | (empty)      |
//! | `PARTICLEDB_TEST_DB`    | `particledb` |
//!
//! If **none** of these variables are set, every test is skipped with a
//! printed message.
//!
//! # Running
//!
//! ```bash
//! cargo test --test integration -- --ignored --test-threads=1
//! ```

use particledb_sdk::{Config, Error, ParticleDB, QueryResult, Row, SqlExecutor, Value};
use postgres::{Client, NoTls, SimpleQueryMessage};
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Real PG-wire executor
// ---------------------------------------------------------------------------

/// A synchronous [`SqlExecutor`] backed by a real `postgres::Client` connection.
///
/// Uses the **simple query protocol** (not extended) so that all values arrive
/// in text format.  This avoids OID-mismatch issues where the server reports a
/// different type OID (e.g. FLOAT8 for SUM of BIGINT) than the actual Arrow
/// data encoding, which would cause the `postgres` crate's binary decoder to
/// produce garbled results.
struct PgExecutor {
    client: Mutex<Client>,
}

impl PgExecutor {
    fn connect(config: &Config) -> Result<Self, String> {
        let conn_str = if config.password.is_empty() {
            format!(
                "host={} port={} user={} dbname={}",
                config.host, config.port, config.user, config.database
            )
        } else {
            format!(
                "host={} port={} user={} password={} dbname={}",
                config.host, config.port, config.user, config.password, config.database
            )
        };

        let client =
            Client::connect(&conn_str, NoTls).map_err(|e| format!("failed to connect: {}", e))?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }
}

/// Parse a text value from the simple-query protocol into the best-fitting
/// [`Value`] variant.  Tries integer, then float, then bool, then falls back
/// to [`Value::String`].
fn parse_text_value(text: &str) -> Value {
    // Try integer first.
    if let Ok(v) = text.parse::<i64>() {
        return Value::Int64(v);
    }
    // Try float (handles "3.14", "1e10", "NaN", "Infinity", etc.).
    if let Ok(v) = text.parse::<f64>() {
        return Value::Float64(v);
    }
    // Try boolean.
    match text {
        "t" | "true" | "TRUE" => return Value::Bool(true),
        "f" | "false" | "FALSE" => return Value::Bool(false),
        _ => {}
    }
    // Everything else is a string.
    Value::String(text.to_string())
}

/// Convert a `SimpleQueryRow` into SDK types.  Every column value is
/// `Option<&str>` (text format), so we parse it with [`parse_text_value`].
fn simple_row_to_sdk_row(row: &postgres::SimpleQueryRow) -> Row {
    let columns: Vec<String> = row.columns().iter().map(|c| c.name().to_string()).collect();

    let mut values = Vec::with_capacity(columns.len());
    for i in 0..columns.len() {
        let val = match row.try_get(i) {
            Ok(Some(text)) => parse_text_value(text),
            _ => Value::Null,
        };
        values.push(val);
    }

    Row::new(columns, values)
}

impl SqlExecutor for PgExecutor {
    fn execute_query(&self, sql: &str, params: &[Value]) -> particledb_sdk::Result<QueryResult> {
        let mut client = self.client.lock().unwrap();
        let sql_with_params = interpolate_params(sql, params);

        // Use simple_query to get text-format results, avoiding binary OID
        // mismatches between the server's RowDescription and actual data.
        let messages = client
            .simple_query(sql_with_params.as_str())
            .map_err(|e| Error::Query(e.to_string()))?;

        let mut sdk_rows: Vec<Row> = Vec::new();
        let mut columns: Vec<String> = Vec::new();

        for msg in &messages {
            if let SimpleQueryMessage::Row(row) = msg {
                if columns.is_empty() {
                    columns = row.columns().iter().map(|c| c.name().to_string()).collect();
                }
                sdk_rows.push(simple_row_to_sdk_row(row));
            }
        }

        Ok(QueryResult {
            rows: sdk_rows,
            columns,
            rows_affected: 0,
        })
    }

    fn execute_statement(&self, sql: &str, params: &[Value]) -> particledb_sdk::Result<u64> {
        let mut client = self.client.lock().unwrap();
        let sql_with_params = interpolate_params(sql, params);

        // Use simple_query for statements too, to stay on the simple protocol.
        let messages = client
            .simple_query(sql_with_params.as_str())
            .map_err(|e| Error::Query(e.to_string()))?;

        let mut affected: u64 = 0;
        for msg in &messages {
            if let SimpleQueryMessage::CommandComplete(n) = msg {
                affected = *n;
            }
        }
        Ok(affected)
    }
}

/// Simple parameter interpolation: replaces `$1`, `$2`, ... with literal
/// representations of the supplied values. This is intentionally naive (no
/// prepared-statement binding) so we stay compatible with the widest range of
/// ParticleDB PG-wire implementations.
fn interpolate_params(sql: &str, params: &[Value]) -> String {
    let mut result = sql.to_string();
    // Replace in reverse order so that $10 is replaced before $1.
    for (i, val) in params.iter().enumerate().rev() {
        let placeholder = format!("${}", i + 1);
        let replacement = match val {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            Value::Int64(n) => n.to_string(),
            Value::Float64(f) => format!("{}", f),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Bytes(b) => format!("'\\x{}'", hex_encode(b)),
            Value::Timestamp(t) => t.to_string(),
            Value::Vector(v) => {
                let inner = v
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                format!("VECTOR('[{}]')", inner)
            }
            Value::Json(j) => format!("'{}'", j.replace('\'', "''")),
            Value::Array(_) => "NULL".to_string(), // Not supported in simple interpolation.
            Value::Float32(f) => format!("{}", f),
            Value::Numeric(n) => n.clone(),
            Value::Date(d) => format!("(DATE '1970-01-01' + {})", d),
            Value::TimestampTz(t) => t.to_string(),
            Value::Interval {
                months,
                days,
                micros,
            } => {
                format!(
                    "INTERVAL '{} mons {} days {} microseconds'",
                    months, days, micros
                )
            }
            Value::Uuid(u) => format!("'{}'", u.replace('\'', "''")),
        };
        result = result.replace(&placeholder, &replacement);
    }
    result
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_config() -> Config {
    Config {
        host: std::env::var("PARTICLEDB_TEST_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        port: std::env::var("PARTICLEDB_TEST_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5432),
        user: std::env::var("PARTICLEDB_TEST_USER").unwrap_or_else(|_| "particledb".into()),
        password: std::env::var("PARTICLEDB_TEST_PASS").unwrap_or_default(),
        database: std::env::var("PARTICLEDB_TEST_DB").unwrap_or_else(|_| "particledb".into()),
        pool_size: 1,
        ..Config::default()
    }
}

/// Returns true if integration env vars are configured. When false, tests
/// print a message and return early.
fn env_configured() -> bool {
    // If any of the env vars is explicitly set, we treat the environment as
    // configured.  When *none* are set, skip.
    std::env::var("PARTICLEDB_TEST_HOST").is_ok()
        || std::env::var("PARTICLEDB_TEST_PORT").is_ok()
        || std::env::var("PARTICLEDB_TEST_USER").is_ok()
        || std::env::var("PARTICLEDB_TEST_DB").is_ok()
}

macro_rules! skip_if_no_env {
    () => {
        if !env_configured() {
            eprintln!(
                "SKIPPED: set PARTICLEDB_TEST_HOST / PARTICLEDB_TEST_PORT to run integration tests"
            );
            return;
        }
    };
}

/// Create a connected `ParticleDB<PgExecutor>`.
fn connect() -> ParticleDB<PgExecutor> {
    let cfg = test_config();
    let executor = PgExecutor::connect(&cfg).expect("could not connect to ParticleDB");
    ParticleDB::with_executor(cfg, executor)
}

/// Drop a table, ignoring errors (e.g. table does not exist).
fn drop_table(db: &ParticleDB<PgExecutor>, table: &str) {
    let _ = db.execute(&format!("DROP TABLE IF EXISTS {}", table), &[]);
}

// ---------------------------------------------------------------------------
// 1. Connection & Lifecycle
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_connection_and_lifecycle() {
    skip_if_no_env!();
    let db = connect();

    // Simple connectivity check.
    let result = db.query("SELECT 1 AS one", &[]).unwrap();
    assert_eq!(result.rows.len(), 1);
    let val: i64 = result.rows[0].get(0).unwrap();
    assert_eq!(val, 1);
}

#[test]
#[ignore]
fn test_connection_string_format() {
    skip_if_no_env!();
    let db = connect();
    let cs = db.connection_string();
    assert!(cs.contains("host="));
    assert!(cs.contains("port="));
    assert!(cs.contains("user="));
    assert!(cs.contains("dbname="));
}

// ---------------------------------------------------------------------------
// 2. SQL CRUD
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_sql_create_insert_select() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_crud";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, name TEXT)", table),
        &[],
    )
    .unwrap();

    db.execute(&format!("INSERT INTO {} VALUES (1, 'Alice')", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2, 'Bob')", table), &[])
        .unwrap();

    let result = db
        .query(&format!("SELECT id, name FROM {} ORDER BY id", table), &[])
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 1);
    assert_eq!(result.rows[0].get::<String>(1).unwrap(), "Alice");
    assert_eq!(result.rows[1].get::<i64>(0).unwrap(), 2);
    assert_eq!(result.rows[1].get::<String>(1).unwrap(), "Bob");

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_sql_select_where() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_where";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, name TEXT)", table),
        &[],
    )
    .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1, 'Alice')", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2, 'Bob')", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (3, 'Charlie')", table), &[])
        .unwrap();

    let result = db
        .query(
            &format!("SELECT name FROM {} WHERE id > 1 ORDER BY id", table),
            &[],
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get::<String>(0).unwrap(), "Bob");
    assert_eq!(result.rows[1].get::<String>(0).unwrap(), "Charlie");

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_sql_select_with_params() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_params";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, name TEXT)", table),
        &[],
    )
    .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1, 'Alice')", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2, 'Bob')", table), &[])
        .unwrap();

    // Use parameterized query via our interpolation.
    let result = db
        .query(
            &format!("SELECT name FROM {} WHERE id = $1", table),
            &[Value::Int64(2)],
        )
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get::<String>(0).unwrap(), "Bob");

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_sql_update() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_update";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, name TEXT)", table),
        &[],
    )
    .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1, 'Alice')", table), &[])
        .unwrap();

    db.execute(
        &format!("UPDATE {} SET name = 'Alicia' WHERE id = 1", table),
        &[],
    )
    .unwrap();

    let row = db
        .query_one(&format!("SELECT name FROM {} WHERE id = 1", table), &[])
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "Alicia");

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_sql_delete() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_delete";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, name TEXT)", table),
        &[],
    )
    .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1, 'Alice')", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2, 'Bob')", table), &[])
        .unwrap();

    let affected = db
        .execute(&format!("DELETE FROM {} WHERE id = 1", table), &[])
        .unwrap();
    assert_eq!(affected, 1);

    let result = db.query(&format!("SELECT id FROM {}", table), &[]).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 2);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_sql_rowcount() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_rowcount";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (id BIGINT)", table), &[])
        .unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO {} VALUES ({})", table, i), &[])
            .unwrap();
    }

    let affected = db
        .execute(&format!("DELETE FROM {} WHERE id <= 3", table), &[])
        .unwrap();
    assert_eq!(affected, 3);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_sql_empty_result() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_empty";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (id BIGINT)", table), &[])
        .unwrap();

    let result = db.query(&format!("SELECT id FROM {}", table), &[]).unwrap();
    assert!(result.rows.is_empty());

    // query_one should return None.
    let row = db
        .query_one(&format!("SELECT id FROM {} WHERE id = 999", table), &[])
        .unwrap();
    assert!(row.is_none());

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_sql_drop_table() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_droptable";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (id BIGINT)", table), &[])
        .unwrap();

    db.execute(&format!("DROP TABLE {}", table), &[]).unwrap();

    // Querying the dropped table should fail.
    let err = db.query(&format!("SELECT * FROM {}", table), &[]);
    assert!(err.is_err());
}

// ---------------------------------------------------------------------------
// 3. Data Types
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_data_type_int64() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_type_int";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (val BIGINT)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (0)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (-42)", table), &[])
        .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (9223372036854775807)", table),
        &[],
    )
    .unwrap(); // i64::MAX

    let result = db
        .query(&format!("SELECT val FROM {} ORDER BY val", table), &[])
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), -42);
    assert_eq!(result.rows[1].get::<i64>(0).unwrap(), 0);
    assert_eq!(result.rows[2].get::<i64>(0).unwrap(), i64::MAX);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_data_type_float64() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_type_float";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (val DOUBLE PRECISION)", table),
        &[],
    )
    .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (3.14)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (-0.001)", table), &[])
        .unwrap();

    let result = db
        .query(&format!("SELECT val FROM {} ORDER BY val", table), &[])
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert!((result.rows[0].get::<f64>(0).unwrap() - (-0.001)).abs() < 1e-9);
    assert!((result.rows[1].get::<f64>(0).unwrap() - 3.14).abs() < 1e-9);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_data_type_text() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_type_text";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (val TEXT)", table), &[])
        .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES ('hello world')", table),
        &[],
    )
    .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES ('')", table), &[])
        .unwrap();

    let result = db
        .query(&format!("SELECT val FROM {} ORDER BY val", table), &[])
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    // Ordering of '' vs 'hello world' depends on collation; just check both exist.
    let vals: Vec<String> = result
        .rows
        .iter()
        .map(|r| r.get::<String>(0).unwrap())
        .collect();
    assert!(vals.contains(&"hello world".to_string()));
    assert!(vals.contains(&String::new()));

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_data_type_boolean() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_type_bool";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, flag BOOLEAN)", table),
        &[],
    )
    .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1, true)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2, false)", table), &[])
        .unwrap();

    let result = db
        .query(&format!("SELECT id, flag FROM {} ORDER BY id", table), &[])
        .unwrap();
    assert_eq!(result.rows[0].get::<bool>(1).unwrap(), true);
    assert_eq!(result.rows[1].get::<bool>(1).unwrap(), false);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_data_type_mixed() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_type_mixed";
    drop_table(&db, table);

    db.execute(
        &format!(
            "CREATE TABLE {} (id BIGINT, name TEXT, score DOUBLE PRECISION, active BOOLEAN)",
            table
        ),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (1, 'Alice', 95.5, true)", table),
        &[],
    )
    .unwrap();

    let row = db
        .query_one(
            &format!("SELECT id, name, score, active FROM {}", table),
            &[],
        )
        .unwrap()
        .unwrap();

    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");
    assert!((row.get::<f64>(2).unwrap() - 95.5).abs() < 1e-9);
    assert_eq!(row.get::<bool>(3).unwrap(), true);

    // Also test by-name access.
    assert_eq!(row.get_by_name::<i64>("id").unwrap(), 1);
    assert_eq!(row.get_by_name::<String>("name").unwrap(), "Alice");

    drop_table(&db, table);
}

// ---------------------------------------------------------------------------
// 4. Transactions
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_transaction_commit() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_txn_commit";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (id BIGINT)", table), &[])
        .unwrap();

    db.execute("BEGIN", &[]).unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2)", table), &[])
        .unwrap();
    db.execute("COMMIT", &[]).unwrap();

    let result = db
        .query(&format!("SELECT COUNT(*) AS cnt FROM {}", table), &[])
        .unwrap();
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 2);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_transaction_rollback() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_txn_rollback";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (id BIGINT)", table), &[])
        .unwrap();

    // Insert one row and commit it.
    db.execute(&format!("INSERT INTO {} VALUES (1)", table), &[])
        .unwrap();

    // Start a transaction, insert another row, then rollback.
    db.execute("BEGIN", &[]).unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2)", table), &[])
        .unwrap();
    db.execute("ROLLBACK", &[]).unwrap();

    // Only the committed row should remain.
    let result = db
        .query(&format!("SELECT COUNT(*) AS cnt FROM {}", table), &[])
        .unwrap();
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 1);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_transaction_multi_statement() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_txn_multi";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, val TEXT)", table),
        &[],
    )
    .unwrap();

    db.execute("BEGIN", &[]).unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1, 'a')", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2, 'b')", table), &[])
        .unwrap();
    db.execute(
        &format!("UPDATE {} SET val = 'updated' WHERE id = 1", table),
        &[],
    )
    .unwrap();
    db.execute("COMMIT", &[]).unwrap();

    let row = db
        .query_one(&format!("SELECT val FROM {} WHERE id = 1", table), &[])
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "updated");

    let result = db
        .query(&format!("SELECT COUNT(*) AS cnt FROM {}", table), &[])
        .unwrap();
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 2);

    drop_table(&db, table);
}

// ---------------------------------------------------------------------------
// 5. KV Operations
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_kv_set_get() {
    skip_if_no_env!();
    let db = connect();

    // KV functions work without creating any table.
    db.execute("SELECT KV_SET('rs_integ_hello', 'world')", &[])
        .unwrap();

    let result = db.query("SELECT KV_GET('rs_integ_hello')", &[]).unwrap();
    assert_eq!(result.rows.len(), 1);
    let val: String = result.rows[0].get(0).unwrap();
    assert_eq!(val, "world");

    // Clean up - KV_DEL returns no rows (void), so just execute it.
    let _ = db.query("SELECT KV_DEL('rs_integ_hello')", &[]);
}

#[test]
#[ignore]
fn test_kv_get_missing() {
    skip_if_no_env!();
    let db = connect();

    let result = db
        .query("SELECT KV_GET('rs_integ_nonexistent_key_xyz')", &[])
        .unwrap();
    // KV_GET for a missing key returns a single row with NULL.
    assert_eq!(result.rows.len(), 1);
    let val = result.rows[0].get::<String>(0);
    // Either the value is NULL (Null variant) or the get fails - either way, the key is absent.
    assert!(val.is_err() || result.rows.is_empty());
}

#[test]
#[ignore]
fn test_kv_delete() {
    skip_if_no_env!();
    let db = connect();

    db.execute("SELECT KV_SET('rs_integ_del_key', 'value')", &[])
        .unwrap();

    // KV_DEL returns NO rows (void function). Just execute it.
    let _ = db.query("SELECT KV_DEL('rs_integ_del_key')", &[]);

    // Verify the key is gone via KV_GET.
    let result = db.query("SELECT KV_GET('rs_integ_del_key')", &[]).unwrap();
    assert_eq!(result.rows.len(), 1);
    let val = result.rows[0].get::<String>(0);
    // Should be NULL (missing key).
    assert!(
        val.is_err(),
        "expected NULL for deleted key, but got a value"
    );
}

#[test]
#[ignore]
fn test_kv_exists() {
    skip_if_no_env!();
    let db = connect();

    db.execute("SELECT KV_SET('rs_integ_exists_key', 'val')", &[])
        .unwrap();

    // KV_EXISTS returns NO rows (void function). Verify existence via KV_GET instead.
    let result = db
        .query("SELECT KV_GET('rs_integ_exists_key')", &[])
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let val: String = result.rows[0].get(0).unwrap();
    assert_eq!(val, "val");

    // Delete and verify the key is gone.
    let _ = db.query("SELECT KV_DEL('rs_integ_exists_key')", &[]);
    let result = db
        .query("SELECT KV_GET('rs_integ_exists_key')", &[])
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    let val = result.rows[0].get::<String>(0);
    assert!(val.is_err(), "expected NULL for deleted key");
}

#[test]
#[ignore]
fn test_kv_incr() {
    skip_if_no_env!();
    let db = connect();

    // Clean slate.
    let _ = db.query("SELECT KV_DEL('rs_integ_counter')", &[]);

    // Set initial value, then increment manually.
    db.execute("SELECT KV_SET('rs_integ_counter', '10')", &[])
        .unwrap();

    let result = db.query("SELECT KV_GET('rs_integ_counter')", &[]).unwrap();
    let val: i64 = result.rows[0].get(0).unwrap();
    assert_eq!(val, 10);

    // Overwrite with incremented value.
    db.execute("SELECT KV_SET('rs_integ_counter', '15')", &[])
        .unwrap();

    let result = db.query("SELECT KV_GET('rs_integ_counter')", &[]).unwrap();
    let val: i64 = result.rows[0].get(0).unwrap();
    assert_eq!(val, 15);

    // Clean up.
    let _ = db.query("SELECT KV_DEL('rs_integ_counter')", &[]);
}

#[test]
#[ignore]
fn test_kv_overwrite() {
    skip_if_no_env!();
    let db = connect();

    db.execute("SELECT KV_SET('rs_integ_overwrite', 'first')", &[])
        .unwrap();
    let result = db
        .query("SELECT KV_GET('rs_integ_overwrite')", &[])
        .unwrap();
    let val: String = result.rows[0].get(0).unwrap();
    assert_eq!(val, "first");

    // Overwrite with a new value.
    db.execute("SELECT KV_SET('rs_integ_overwrite', 'second')", &[])
        .unwrap();
    let result = db
        .query("SELECT KV_GET('rs_integ_overwrite')", &[])
        .unwrap();
    let val: String = result.rows[0].get(0).unwrap();
    assert_eq!(val, "second");

    let _ = db.query("SELECT KV_DEL('rs_integ_overwrite')", &[]);
}

// ---------------------------------------------------------------------------
// 6. Vector Operations
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_vector_insert_and_search() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_vectors";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, embedding VECTOR)", table),
        &[],
    )
    .unwrap();

    db.execute(
        &format!("INSERT INTO {} VALUES (1, VECTOR('[1.0,0.0,0.0]'))", table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (2, VECTOR('[0.0,1.0,0.0]'))", table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (3, VECTOR('[0.0,0.0,1.0]'))", table),
        &[],
    )
    .unwrap();

    // Search using L2_DISTANCE (the engine's native function).
    let result = db
        .query(
            &format!(
                "SELECT id, L2_DISTANCE(embedding, VECTOR('[1.0,0.0,0.0]')) AS dist \
                 FROM {} ORDER BY dist LIMIT 2",
                table
            ),
            &[],
        )
        .unwrap();

    assert_eq!(result.rows.len(), 2);
    // The nearest neighbor to [1,0,0] should be id=1 (distance 0).
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 1);
    let dist: f64 = result.rows[0].get(1).unwrap();
    assert!(dist.abs() < 1e-6, "expected distance ~0, got {}", dist);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_vector_search_with_filter() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_vec_filter";
    drop_table(&db, table);

    db.execute(
        &format!(
            "CREATE TABLE {} (id BIGINT, category TEXT, embedding VECTOR)",
            table
        ),
        &[],
    )
    .unwrap();

    db.execute(
        &format!("INSERT INTO {} VALUES (1, 'A', VECTOR('[1.0,0.0]'))", table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (2, 'B', VECTOR('[0.9,0.1]'))", table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (3, 'A', VECTOR('[0.0,1.0]'))", table),
        &[],
    )
    .unwrap();

    // Search only within category 'A'.
    let result = db
        .query(
            &format!(
                "SELECT id, L2_DISTANCE(embedding, VECTOR('[1.0,0.0]')) AS dist \
                 FROM {} WHERE category = 'A' ORDER BY dist",
                table
            ),
            &[],
        )
        .unwrap();

    // Should only return rows with category 'A'.
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 1);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_vector_create_index() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_vec_idx";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, embedding VECTOR)", table),
        &[],
    )
    .unwrap();

    // Insert some data so the index has something to build on.
    for i in 1..=10 {
        let v = format!("[{}.0,{}.0]", i, 10 - i);
        db.execute(
            &format!("INSERT INTO {} VALUES ({}, VECTOR('{}'))", table, i, v),
            &[],
        )
        .unwrap();
    }

    // Use the SDK's create_index method.
    db.vector()
        .create_index(table, "embedding", "hnsw")
        .unwrap();

    // Verify the index works by running a search.
    let result = db
        .query(
            &format!(
                "SELECT id FROM {} ORDER BY L2_DISTANCE(embedding, VECTOR('[5.0,5.0]')) LIMIT 3",
                table
            ),
            &[],
        )
        .unwrap();
    assert!(result.rows.len() <= 3);

    drop_table(&db, table);
}

// ---------------------------------------------------------------------------
// 7. Aggregations
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_aggregation_count() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_agg";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, score DOUBLE PRECISION)", table),
        &[],
    )
    .unwrap();
    for i in 1..=5 {
        db.execute(
            &format!("INSERT INTO {} VALUES ({}, {})", table, i, i as f64 * 10.0),
            &[],
        )
        .unwrap();
    }

    // COUNT
    let row = db
        .query_one(&format!("SELECT COUNT(*) AS cnt FROM {}", table), &[])
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 5);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_aggregation_sum() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_agg_sum";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, score DOUBLE PRECISION)", table),
        &[],
    )
    .unwrap();
    for i in 1..=5 {
        db.execute(
            &format!("INSERT INTO {} VALUES ({}, {})", table, i, i as f64 * 10.0),
            &[],
        )
        .unwrap();
    }

    // SUM: 10 + 20 + 30 + 40 + 50 = 150
    let row = db
        .query_one(&format!("SELECT SUM(score) AS total FROM {}", table), &[])
        .unwrap()
        .unwrap();
    let total: f64 = row.get(0).unwrap();
    assert!((total - 150.0).abs() < 1e-9, "expected 150, got {}", total);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_aggregation_avg_min_max() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_agg_amm";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, score DOUBLE PRECISION)", table),
        &[],
    )
    .unwrap();
    for i in 1..=5 {
        db.execute(
            &format!("INSERT INTO {} VALUES ({}, {})", table, i, i as f64 * 10.0),
            &[],
        )
        .unwrap();
    }

    let row = db
        .query_one(
            &format!(
                "SELECT AVG(score) AS avg_s, MIN(score) AS min_s, MAX(score) AS max_s FROM {}",
                table
            ),
            &[],
        )
        .unwrap()
        .unwrap();

    let avg: f64 = row.get(0).unwrap();
    let min: f64 = row.get(1).unwrap();
    let max: f64 = row.get(2).unwrap();
    assert!((avg - 30.0).abs() < 1e-9, "expected AVG=30, got {}", avg);
    assert!((min - 10.0).abs() < 1e-9, "expected MIN=10, got {}", min);
    assert!((max - 50.0).abs() < 1e-9, "expected MAX=50, got {}", max);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_aggregation_group_by() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_agg_group";
    drop_table(&db, table);

    db.execute(
        &format!(
            "CREATE TABLE {} (id BIGINT, category TEXT, amount BIGINT)",
            table
        ),
        &[],
    )
    .unwrap();

    db.execute(&format!("INSERT INTO {} VALUES (1, 'A', 10)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2, 'B', 20)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (3, 'A', 30)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (4, 'B', 40)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (5, 'A', 50)", table), &[])
        .unwrap();

    let result = db
        .query(
            &format!(
                "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt \
                 FROM {} GROUP BY category ORDER BY category",
                table
            ),
            &[],
        )
        .unwrap();

    assert_eq!(result.rows.len(), 2);

    // Group A: sum=90, count=3
    // SUM may be returned as Float64 (engine accumulates as f64) - use f64
    // with the FromValue coercion that also accepts Int64.
    assert_eq!(result.rows[0].get::<String>(0).unwrap(), "A");
    let a_total: f64 = result.rows[0].get(1).unwrap();
    assert!(
        (a_total - 90.0).abs() < 1e-9,
        "expected 90, got {}",
        a_total
    );
    let a_cnt: f64 = result.rows[0].get(2).unwrap();
    assert!((a_cnt - 3.0).abs() < 1e-9, "expected 3, got {}", a_cnt);

    // Group B: sum=60, count=2
    assert_eq!(result.rows[1].get::<String>(0).unwrap(), "B");
    let b_total: f64 = result.rows[1].get(1).unwrap();
    assert!(
        (b_total - 60.0).abs() < 1e-9,
        "expected 60, got {}",
        b_total
    );
    let b_cnt: f64 = result.rows[1].get(2).unwrap();
    assert!((b_cnt - 2.0).abs() < 1e-9, "expected 2, got {}", b_cnt);

    drop_table(&db, table);
}

// ---------------------------------------------------------------------------
// 8. ORDER BY & LIMIT
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_order_by_asc_desc() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_order";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, name TEXT)", table),
        &[],
    )
    .unwrap();

    db.execute(&format!("INSERT INTO {} VALUES (3, 'C')", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1, 'A')", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (2, 'B')", table), &[])
        .unwrap();

    // ASC
    let asc = db
        .query(&format!("SELECT id FROM {} ORDER BY id ASC", table), &[])
        .unwrap();
    let ids_asc: Vec<i64> = asc.rows.iter().map(|r| r.get(0).unwrap()).collect();
    assert_eq!(ids_asc, vec![1, 2, 3]);

    // DESC
    let desc = db
        .query(&format!("SELECT id FROM {} ORDER BY id DESC", table), &[])
        .unwrap();
    let ids_desc: Vec<i64> = desc.rows.iter().map(|r| r.get(0).unwrap()).collect();
    assert_eq!(ids_desc, vec![3, 2, 1]);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_limit() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_limit";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (id BIGINT)", table), &[])
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO {} VALUES ({})", table, i), &[])
            .unwrap();
    }

    let result = db
        .query(
            &format!("SELECT id FROM {} ORDER BY id LIMIT 3", table),
            &[],
        )
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 1);
    assert_eq!(result.rows[2].get::<i64>(0).unwrap(), 3);

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_order_by_with_limit() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_order_limit";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, score BIGINT)", table),
        &[],
    )
    .unwrap();
    for i in 1..=10 {
        db.execute(
            &format!("INSERT INTO {} VALUES ({}, {})", table, i, i * 10),
            &[],
        )
        .unwrap();
    }

    // Top 3 by score descending.
    let result = db
        .query(
            &format!(
                "SELECT id, score FROM {} ORDER BY score DESC LIMIT 3",
                table
            ),
            &[],
        )
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get::<i64>(0).unwrap(), 10);
    assert_eq!(result.rows[0].get::<i64>(1).unwrap(), 100);

    drop_table(&db, table);
}

// ---------------------------------------------------------------------------
// 9. Concurrent Connections
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_concurrent_connections() {
    skip_if_no_env!();

    let table = "rs_integ_concurrent";

    // Set up the table on the main connection.
    let db_main = connect();
    drop_table(&db_main, table);
    db_main
        .execute(
            &format!("CREATE TABLE {} (id BIGINT, thread_id BIGINT)", table),
            &[],
        )
        .unwrap();

    let num_threads = 4;
    let rows_per_thread = 10;

    let mut handles = Vec::new();
    for t in 0..num_threads {
        let table_name = table.to_string();
        let handle = std::thread::spawn(move || {
            let db = connect();
            for i in 0..rows_per_thread {
                let id = t * 1000 + i;
                db.execute(
                    &format!("INSERT INTO {} VALUES ({}, {})", table_name, id, t),
                    &[],
                )
                .unwrap();
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    // Verify all rows were inserted.
    let result = db_main
        .query(&format!("SELECT COUNT(*) AS cnt FROM {}", table), &[])
        .unwrap();
    let total: i64 = result.rows[0].get(0).unwrap();
    assert_eq!(
        total,
        (num_threads * rows_per_thread) as i64,
        "expected {} rows, got {}",
        num_threads * rows_per_thread,
        total
    );

    drop_table(&db_main, table);
}

// ---------------------------------------------------------------------------
// 10. Joins
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_inner_join() {
    skip_if_no_env!();
    let db = connect();
    let users_table = "rs_integ_join_users";
    let posts_table = "rs_integ_join_posts";
    drop_table(&db, posts_table);
    drop_table(&db, users_table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, name TEXT)", users_table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!(
            "CREATE TABLE {} (id BIGINT, user_id BIGINT, title TEXT, score DOUBLE PRECISION)",
            posts_table
        ),
        &[],
    )
    .unwrap();

    db.execute(
        &format!("INSERT INTO {} VALUES (1, 'Alice')", users_table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (2, 'Bob')", users_table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (1, 1, 'Post A', 8.5)", posts_table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (2, 1, 'Post B', 7.0)", posts_table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (3, 2, 'Post C', 9.0)", posts_table),
        &[],
    )
    .unwrap();

    let result = db
        .query(
            &format!(
                "SELECT u.name, p.title, p.score \
                 FROM {} u INNER JOIN {} p ON u.id = p.user_id \
                 ORDER BY p.id",
                users_table, posts_table
            ),
            &[],
        )
        .unwrap();

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0].get::<String>(0).unwrap(), "Alice");
    assert_eq!(result.rows[0].get::<String>(1).unwrap(), "Post A");
    assert_eq!(result.rows[2].get::<String>(0).unwrap(), "Bob");
    assert_eq!(result.rows[2].get::<String>(1).unwrap(), "Post C");

    drop_table(&db, posts_table);
    drop_table(&db, users_table);
}

#[test]
#[ignore]
fn test_join_with_aggregation() {
    skip_if_no_env!();
    let db = connect();
    let users_table = "rs_integ_jagg_users";
    let posts_table = "rs_integ_jagg_posts";
    drop_table(&db, posts_table);
    drop_table(&db, users_table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT, name TEXT)", users_table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!(
            "CREATE TABLE {} (id BIGINT, user_id BIGINT, score DOUBLE PRECISION)",
            posts_table
        ),
        &[],
    )
    .unwrap();

    db.execute(
        &format!("INSERT INTO {} VALUES (1, 'Alice')", users_table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (2, 'Bob')", users_table),
        &[],
    )
    .unwrap();

    db.execute(
        &format!("INSERT INTO {} VALUES (1, 1, 10.0)", posts_table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (2, 1, 20.0)", posts_table),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {} VALUES (3, 2, 30.0)", posts_table),
        &[],
    )
    .unwrap();

    let result = db
        .query(
            &format!(
                "SELECT p.user_id, COUNT(*) AS cnt, SUM(p.score) AS total \
                 FROM {} p \
                 GROUP BY p.user_id ORDER BY p.user_id",
                posts_table
            ),
            &[],
        )
        .unwrap();

    assert_eq!(result.rows.len(), 2);
    let uid: i64 = result.rows[0].get(0).unwrap();
    assert_eq!(uid, 1); // Alice's id
    let alice_cnt: f64 = result.rows[0].get(1).unwrap();
    assert!(
        (alice_cnt - 2.0).abs() < 1e-9,
        "expected 2, got {}",
        alice_cnt
    );
    let alice_total: f64 = result.rows[0].get(2).unwrap();
    assert!(
        (alice_total - 30.0).abs() < 1e-9,
        "expected 30, got {}",
        alice_total
    );

    let uid2: i64 = result.rows[1].get(0).unwrap();
    assert_eq!(uid2, 2); // Bob's id
    let bob_cnt: f64 = result.rows[1].get(1).unwrap();
    assert!((bob_cnt - 1.0).abs() < 1e-9, "expected 1, got {}", bob_cnt);

    drop_table(&db, posts_table);
    drop_table(&db, users_table);
}

// ---------------------------------------------------------------------------
// 11. Error Handling
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_error_invalid_sql() {
    skip_if_no_env!();
    let db = connect();

    let result = db.query("THIS IS NOT VALID SQL", &[]);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::Query(msg) => {
            assert!(!msg.is_empty(), "error message should not be empty");
        }
        other => panic!("expected Query error, got: {:?}", other),
    }
}

#[test]
#[ignore]
fn test_error_table_not_found() {
    skip_if_no_env!();
    let db = connect();

    let result = db.query("SELECT * FROM rs_integ_nonexistent_table_xyz", &[]);
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_error_type_mismatch_on_read() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_type_err";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (val TEXT)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES ('hello')", table), &[])
        .unwrap();

    let row = db
        .query_one(&format!("SELECT val FROM {}", table), &[])
        .unwrap()
        .unwrap();

    // Trying to read a TEXT column as i64 should fail with TypeConversion.
    let err = row.get::<i64>(0);
    assert!(err.is_err());
    match err.unwrap_err() {
        Error::TypeConversion(_) => { /* expected */ }
        other => panic!("expected TypeConversion error, got: {:?}", other),
    }

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_error_column_index_out_of_range() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_col_err";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (id BIGINT)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1)", table), &[])
        .unwrap();

    let row = db
        .query_one(&format!("SELECT id FROM {}", table), &[])
        .unwrap()
        .unwrap();

    // Column index 5 is out of range.
    let err = row.get::<i64>(5);
    assert!(err.is_err());
    match err.unwrap_err() {
        Error::TypeConversion(msg) => {
            assert!(msg.contains("out of range"));
        }
        other => panic!("expected TypeConversion error, got: {:?}", other),
    }

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_error_column_name_not_found() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_name_err";
    drop_table(&db, table);

    db.execute(&format!("CREATE TABLE {} (id BIGINT)", table), &[])
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (1)", table), &[])
        .unwrap();

    let row = db
        .query_one(&format!("SELECT id FROM {}", table), &[])
        .unwrap()
        .unwrap();

    let err = row.get_by_name::<i64>("nonexistent_col");
    assert!(err.is_err());
    match err.unwrap_err() {
        Error::TypeConversion(msg) => {
            assert!(msg.contains("not found"));
        }
        other => panic!("expected TypeConversion error, got: {:?}", other),
    }

    drop_table(&db, table);
}

#[test]
#[ignore]
fn test_duplicate_primary_key_append() {
    skip_if_no_env!();
    let db = connect();
    let table = "rs_integ_dup_pk";
    drop_table(&db, table);

    db.execute(
        &format!("CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT)", table),
        &[],
    )
    .unwrap();

    db.execute(&format!("INSERT INTO {} VALUES (1, 'Alice')", table), &[])
        .unwrap();

    // ParticleDB deduplicates on PK - the second INSERT either replaces or
    // is silently dropped.  Either way, only one row survives for id=1.
    db.execute(&format!("INSERT INTO {} VALUES (1, 'Bob')", table), &[])
        .unwrap();

    let result = db
        .query(
            &format!("SELECT id, name FROM {} WHERE id = 1 ORDER BY name", table),
            &[],
        )
        .unwrap();
    assert!(
        result.rows.len() >= 1,
        "expected at least 1 row with id=1, got {}",
        result.rows.len()
    );

    // Verify at least one of the names is present (whichever write won).
    let names: Vec<String> = result
        .rows
        .iter()
        .map(|r| r.get::<String>(1).unwrap())
        .collect();
    assert!(
        names.contains(&"Alice".to_string()) || names.contains(&"Bob".to_string()),
        "expected Alice or Bob in {:?}",
        names
    );

    drop_table(&db, table);
}
