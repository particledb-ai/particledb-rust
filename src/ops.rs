//! Extended database operations.
//!
//! **This module is not compiled.** There is no `mod ops;` anywhere under
//! `src/` (checked 2026-09-02), so `ExtendedOps` is not part of the
//! published crate and the 22 unit tests at the bottom of this file never
//! run. Wiring it in is a deliberate API decision that has not been made;
//! until it is, treat everything here as unshipped. The tests below were
//! nonetheless executed once, by temporarily adding `pub mod ops;` to
//! `lib.rs` (57 -> 79 passing), so they are known to hold.

use crate::client::{ParticleDB, SqlExecutor};
use crate::error::{Error, Result};
use crate::types::QueryResult;

/// The error every namespace method fails with. `verb` is the SQL verb the
/// caller was reaching for.
///
/// See the `Multi-Tenant / Namespaces` section note on [`ExtendedOps`].
fn namespace_unsupported(verb: &str) -> Error {
    Error::Query(format!(
        "{verb} is not implemented by any ParticleDB server: there is no \
         namespace surface on any wire, so this call can never succeed. Use a \
         table-name prefix, a tenant column filtered on every query, or a \
         separate server per tenant. \
         See https://particledb.ai/getting-started/databases-and-schemas/"
    ))
}

/// Quote a SQL identifier by wrapping in double quotes with internal escaping.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Escape a SQL string literal by doubling single quotes.
fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Extended database operations for ParticleDB.
///
/// Provides methods for time travel, materialized views, changefeeds/CDC,
/// geospatial functions, time-series operations, multi-tenant namespace
/// management, and lakehouse file access.
pub struct ExtendedOps<'a, E: SqlExecutor> {
    client: &'a ParticleDB<E>,
}

impl<'a, E: SqlExecutor> ExtendedOps<'a, E> {
    pub(crate) fn new(client: &'a ParticleDB<E>) -> Self {
        Self { client }
    }

    // ── Time Travel ───────────────────────────────────────────────────

    /// Build the SQL for a time-travel query.
    pub fn query_as_of_sql(sql: &str, timestamp: &str) -> String {
        format!("SET TIMESTAMP AS OF '{}'; {}", escape_literal(timestamp), sql)
    }

    /// Execute a query as of a historical timestamp.
    pub fn query_as_of(&self, sql: &str, timestamp: &str) -> Result<QueryResult> {
        let as_of_sql = Self::query_as_of_sql(sql, timestamp);
        self.client.query(&as_of_sql, &[])
    }

    // ── Materialized Views ────────────────────────────────────────────

    /// Build the DDL to create a materialized view.
    pub fn create_materialized_view_sql(name: &str, query: &str, refresh: &str) -> String {
        if refresh.is_empty() || refresh == "manual" {
            format!("CREATE MATERIALIZED VIEW {} AS {}", quote_ident(name), query)
        } else {
            format!(
                "CREATE MATERIALIZED VIEW {} WITH (refresh_interval = '{}') AS {}",
                quote_ident(name), escape_literal(refresh), query
            )
        }
    }

    /// Create a materialized view.
    pub fn create_materialized_view(
        &self,
        name: &str,
        query: &str,
        refresh: &str,
    ) -> Result<u64> {
        let sql = Self::create_materialized_view_sql(name, query, refresh);
        self.client.execute(&sql, &[])
    }

    /// Build the SQL to refresh a materialized view.
    pub fn refresh_materialized_view_sql(name: &str) -> String {
        format!("REFRESH MATERIALIZED VIEW {}", quote_ident(name))
    }

    /// Trigger a refresh of a materialized view.
    pub fn refresh_materialized_view(&self, name: &str) -> Result<u64> {
        let sql = Self::refresh_materialized_view_sql(name);
        self.client.execute(&sql, &[])
    }

    /// Build the DDL to drop a materialized view.
    pub fn drop_materialized_view_sql(name: &str) -> String {
        format!("DROP MATERIALIZED VIEW IF EXISTS {}", quote_ident(name))
    }

    /// Drop a materialized view.
    pub fn drop_materialized_view(&self, name: &str) -> Result<u64> {
        let sql = Self::drop_materialized_view_sql(name);
        self.client.execute(&sql, &[])
    }

    // ── Changefeeds / CDC ─────────────────────────────────────────────

    /// Build the DDL to create a changefeed.
    pub fn create_changefeed_sql(
        name: &str,
        table: &str,
        sink_type: &str,
        sink_url: &str,
    ) -> String {
        format!(
            "CREATE CHANGEFEED {} ON {} INTO '{}' WITH (sink_url = '{}')",
            quote_ident(name), quote_ident(table), escape_literal(sink_type), escape_literal(sink_url)
        )
    }

    /// Create a changefeed for change data capture.
    pub fn create_changefeed(
        &self,
        name: &str,
        table: &str,
        sink_type: &str,
        sink_url: &str,
    ) -> Result<u64> {
        let sql = Self::create_changefeed_sql(name, table, sink_type, sink_url);
        self.client.execute(&sql, &[])
    }

    /// Build the SQL to pause a changefeed.
    pub fn pause_changefeed_sql(name: &str) -> String {
        format!("ALTER CHANGEFEED {} PAUSE", quote_ident(name))
    }

    /// Pause a running changefeed.
    pub fn pause_changefeed(&self, name: &str) -> Result<u64> {
        let sql = Self::pause_changefeed_sql(name);
        self.client.execute(&sql, &[])
    }

    /// Build the SQL to resume a changefeed.
    pub fn resume_changefeed_sql(name: &str) -> String {
        format!("ALTER CHANGEFEED {} RESUME", quote_ident(name))
    }

    /// Resume a paused changefeed.
    pub fn resume_changefeed(&self, name: &str) -> Result<u64> {
        let sql = Self::resume_changefeed_sql(name);
        self.client.execute(&sql, &[])
    }

    /// Build the DDL to drop a changefeed.
    pub fn drop_changefeed_sql(name: &str) -> String {
        format!("DROP CHANGEFEED IF EXISTS {}", quote_ident(name))
    }

    /// Drop a changefeed.
    pub fn drop_changefeed(&self, name: &str) -> Result<u64> {
        let sql = Self::drop_changefeed_sql(name);
        self.client.execute(&sql, &[])
    }

    /// Build the SQL to list all changefeeds.
    pub fn list_changefeeds_sql() -> String {
        "SHOW CHANGEFEEDS".to_string()
    }

    /// List all changefeeds.
    pub fn list_changefeeds(&self) -> Result<QueryResult> {
        self.client.query("SHOW CHANGEFEEDS", &[])
    }

    // ── Geospatial ────────────────────────────────────────────────────

    /// Build the SQL for computing the distance between two geographic points.
    pub fn st_distance_sql(p1_lon: f64, p1_lat: f64, p2_lon: f64, p2_lat: f64) -> String {
        format!(
            "SELECT ST_DISTANCE(ST_POINT({}, {}), ST_POINT({}, {})) AS distance",
            p1_lon, p1_lat, p2_lon, p2_lat
        )
    }

    /// Compute the Haversine distance between two geographic points (in meters).
    pub fn st_distance(
        &self,
        p1_lon: f64,
        p1_lat: f64,
        p2_lon: f64,
        p2_lat: f64,
    ) -> Result<f64> {
        let sql = Self::st_distance_sql(p1_lon, p1_lat, p2_lon, p2_lat);
        let result = self.client.query(&sql, &[])?;
        match result.rows.into_iter().next() {
            Some(row) => row.get::<f64>(0),
            None => Ok(0.0),
        }
    }

    /// Build the SQL for a point-in-polygon test.
    pub fn st_within_sql(point_lon: f64, point_lat: f64, polygon_wkt: &str) -> String {
        format!(
            "SELECT ST_WITHIN(ST_POINT({}, {}), ST_GEOMFROMTEXT('{}')) AS result",
            point_lon, point_lat, escape_literal(polygon_wkt)
        )
    }

    /// Test whether a point falls within a polygon.
    pub fn st_within(
        &self,
        point_lon: f64,
        point_lat: f64,
        polygon_wkt: &str,
    ) -> Result<bool> {
        let sql = Self::st_within_sql(point_lon, point_lat, polygon_wkt);
        let result = self.client.query(&sql, &[])?;
        match result.rows.into_iter().next() {
            Some(row) => {
                let val = row.get::<String>(0)?;
                Ok(val == "true" || val == "t")
            }
            None => Ok(false),
        }
    }

    // ── Time-Series ───────────────────────────────────────────────────

    /// Build the DDL for a retention policy.
    pub fn create_retention_policy_sql(
        name: &str,
        table: &str,
        column: &str,
        retention: &str,
    ) -> String {
        format!(
            "CREATE RETENTION POLICY {} ON {} USING ({}) RETAIN FOR '{}'",
            quote_ident(name), quote_ident(table), quote_ident(column), escape_literal(retention)
        )
    }

    /// Create a data retention policy on a time-series table.
    pub fn create_retention_policy(
        &self,
        name: &str,
        table: &str,
        column: &str,
        retention: &str,
    ) -> Result<u64> {
        let sql = Self::create_retention_policy_sql(name, table, column, retention);
        self.client.execute(&sql, &[])
    }

    /// Build the DDL for a continuous aggregate.
    pub fn create_continuous_aggregate_sql(
        name: &str,
        query: &str,
        interval: &str,
    ) -> String {
        format!(
            "CREATE CONTINUOUS AGGREGATE {} WITH (refresh_interval = '{}') AS {}",
            quote_ident(name), escape_literal(interval), query
        )
    }

    /// Create a continuous aggregate for time-series data.
    pub fn create_continuous_aggregate(
        &self,
        name: &str,
        query: &str,
        interval: &str,
    ) -> Result<u64> {
        let sql = Self::create_continuous_aggregate_sql(name, query, interval);
        self.client.execute(&sql, &[])
    }

    // ── Multi-Tenant / Namespaces - NOT IMPLEMENTED SERVER-SIDE ─────
    //
    // DEPRECATED 2026-09-03. ParticleDB has no server-side namespace surface.
    // CREATE NAMESPACE, SET NAMESPACE and DROP NAMESPACE are refused with a
    // parse error on every wire (PG, native and MySQL), and the tenancy model
    // that would give them a meaning is still an open decision - all three
    // candidate models leave these verbs refused. The methods below used to
    // emit that SQL and document a row count on success, an outcome that could
    // not occur. They now return `Error::Query` without contacting the server,
    // and are kept only so existing code keeps compiling; they will be removed
    // in the next major version.
    //
    // For tenant isolation today: a table-name prefix, a tenant column filtered
    // on every query, or a separate server per tenant (its own --data-dir and
    // ports). See https://particledb.ai/getting-started/databases-and-schemas/

    /// Build the DDL for creating a namespace.
    ///
    /// **Deprecated 2026-09-03.** The DDL this returns is refused by every
    /// ParticleDB server. Retained as a pure function so existing code keeps
    /// compiling.
    #[deprecated(note = "CREATE NAMESPACE is not implemented by any ParticleDB server; this can never succeed. See the section note on ExtendedOps.")]
    pub fn create_namespace_sql(name: &str, quotas: &[(&str, &str)]) -> String {
        if quotas.is_empty() {
            format!("CREATE NAMESPACE {}", quote_ident(name))
        } else {
            let parts: Vec<String> = quotas
                .iter()
                .map(|(k, v)| format!("{} = {}", k, v))
                .collect();
            format!("CREATE NAMESPACE {} WITH ({})", quote_ident(name), parts.join(", "))
        }
    }

    /// Always fails - `CREATE NAMESPACE` is not implemented server-side.
    ///
    /// **Deprecated 2026-09-03.** Returns [`Error::Query`] without contacting
    /// the server; see the section note above.
    #[deprecated(note = "CREATE NAMESPACE is not implemented by any ParticleDB server; this can never succeed. See the section note on ExtendedOps.")]
    pub fn create_namespace(&self, _name: &str, _quotas: &[(&str, &str)]) -> Result<u64> {
        Err(namespace_unsupported("CREATE NAMESPACE"))
    }

    /// Build the SQL to set the active namespace.
    ///
    /// **Deprecated 2026-09-03.** The SQL this returns is refused by every
    /// ParticleDB server. Retained as a pure function so existing code keeps
    /// compiling.
    #[deprecated(note = "SET NAMESPACE is not implemented by any ParticleDB server; this can never succeed. See the section note on ExtendedOps.")]
    pub fn set_namespace_sql(name: &str) -> String {
        format!("SET NAMESPACE {}", quote_ident(name))
    }

    /// Always fails - `SET NAMESPACE` is not implemented server-side.
    ///
    /// **Deprecated 2026-09-03.** Returns [`Error::Query`] without contacting
    /// the server; see the section note above.
    #[deprecated(note = "SET NAMESPACE is not implemented by any ParticleDB server; this can never succeed. See the section note on ExtendedOps.")]
    pub fn set_namespace(&self, _name: &str) -> Result<u64> {
        Err(namespace_unsupported("SET NAMESPACE"))
    }

    /// Build the DDL to drop a namespace.
    ///
    /// **Deprecated 2026-09-03.** The DDL this returns is refused by every
    /// ParticleDB server. Retained as a pure function so existing code keeps
    /// compiling.
    #[deprecated(note = "DROP NAMESPACE is not implemented by any ParticleDB server; this can never succeed. See the section note on ExtendedOps.")]
    pub fn drop_namespace_sql(name: &str) -> String {
        format!("DROP NAMESPACE IF EXISTS {}", quote_ident(name))
    }

    /// Always fails - `DROP NAMESPACE` is not implemented server-side.
    ///
    /// **Deprecated 2026-09-03.** Returns [`Error::Query`] without contacting
    /// the server; see the section note above.
    #[deprecated(note = "DROP NAMESPACE is not implemented by any ParticleDB server; this can never succeed. See the section note on ExtendedOps.")]
    pub fn drop_namespace(&self, _name: &str) -> Result<u64> {
        Err(namespace_unsupported("DROP NAMESPACE"))
    }

    // ── Lakehouse ─────────────────────────────────────────────────────

    /// Build the SQL to query a Parquet file.
    pub fn read_parquet_sql(path: &str) -> String {
        format!("SELECT * FROM read_parquet('{}')", escape_literal(path))
    }

    /// Query a Parquet file directly.
    pub fn read_parquet(&self, path: &str) -> Result<QueryResult> {
        let sql = Self::read_parquet_sql(path);
        self.client.query(&sql, &[])
    }

    /// Build the SQL to query a CSV file.
    pub fn read_csv_sql(path: &str) -> String {
        format!("SELECT * FROM read_csv('{}')", escape_literal(path))
    }

    /// Query a CSV file directly.
    pub fn read_csv(&self, path: &str) -> Result<QueryResult> {
        let sql = Self::read_csv_sql(path);
        self.client.query(&sql, &[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Config, MockExecutor};
    use crate::types::Value;

    #[test]
    fn test_query_as_of_sql() {
        let sql = ExtendedOps::<MockExecutor>::query_as_of_sql(
            "SELECT * FROM orders",
            "2024-01-01T00:00:00Z",
        );
        assert_eq!(
            sql,
            "SET TIMESTAMP AS OF '2024-01-01T00:00:00Z'; SELECT * FROM orders"
        );
    }

    #[test]
    fn test_query_as_of_sql_escapes_quotes() {
        let sql = ExtendedOps::<MockExecutor>::query_as_of_sql(
            "SELECT 1",
            "2024-01-01'; DROP TABLE users; --",
        );
        assert!(sql.contains("2024-01-01''; DROP TABLE users; --"));
    }

    #[test]
    fn test_create_materialized_view_sql_manual() {
        let sql = ExtendedOps::<MockExecutor>::create_materialized_view_sql(
            "mv_users",
            "SELECT * FROM users WHERE active",
            "manual",
        );
        assert_eq!(
            sql,
            "CREATE MATERIALIZED VIEW \"mv_users\" AS SELECT * FROM users WHERE active"
        );
    }

    #[test]
    fn test_create_materialized_view_sql_interval() {
        let sql = ExtendedOps::<MockExecutor>::create_materialized_view_sql(
            "mv_users",
            "SELECT * FROM users",
            "1h",
        );
        assert!(sql.contains("refresh_interval = '1h'"));
    }

    #[test]
    fn test_refresh_materialized_view_sql() {
        let sql = ExtendedOps::<MockExecutor>::refresh_materialized_view_sql("mv_users");
        assert_eq!(sql, "REFRESH MATERIALIZED VIEW \"mv_users\"");
    }

    #[test]
    fn test_drop_materialized_view_sql() {
        let sql = ExtendedOps::<MockExecutor>::drop_materialized_view_sql("mv_users");
        assert_eq!(sql, "DROP MATERIALIZED VIEW IF EXISTS \"mv_users\"");
    }

    #[test]
    fn test_create_changefeed_sql() {
        let sql = ExtendedOps::<MockExecutor>::create_changefeed_sql(
            "orders_feed",
            "orders",
            "kafka",
            "kafka://localhost:9092/orders",
        );
        assert!(sql.contains("CREATE CHANGEFEED \"orders_feed\" ON \"orders\""));
        assert!(sql.contains("kafka"));
    }

    #[test]
    fn test_pause_changefeed_sql() {
        let sql = ExtendedOps::<MockExecutor>::pause_changefeed_sql("orders_feed");
        assert_eq!(sql, "ALTER CHANGEFEED \"orders_feed\" PAUSE");
    }

    #[test]
    fn test_resume_changefeed_sql() {
        let sql = ExtendedOps::<MockExecutor>::resume_changefeed_sql("orders_feed");
        assert_eq!(sql, "ALTER CHANGEFEED \"orders_feed\" RESUME");
    }

    #[test]
    fn test_drop_changefeed_sql() {
        let sql = ExtendedOps::<MockExecutor>::drop_changefeed_sql("orders_feed");
        assert_eq!(sql, "DROP CHANGEFEED IF EXISTS \"orders_feed\"");
    }

    #[test]
    fn test_st_distance_sql() {
        let sql = ExtendedOps::<MockExecutor>::st_distance_sql(-73.9857, 40.7484, -0.1278, 51.5074);
        assert!(sql.contains("ST_DISTANCE"));
        assert!(sql.contains("ST_POINT"));
    }

    #[test]
    fn test_st_within_sql() {
        let sql = ExtendedOps::<MockExecutor>::st_within_sql(
            -73.9857,
            40.7484,
            "POLYGON((-74 40, -74 41, -73 41, -73 40, -74 40))",
        );
        assert!(sql.contains("ST_WITHIN"));
        assert!(sql.contains("ST_GEOMFROMTEXT"));
    }

    #[test]
    fn test_create_retention_policy_sql() {
        let sql = ExtendedOps::<MockExecutor>::create_retention_policy_sql(
            "events_90d",
            "events",
            "created_at",
            "90d",
        );
        assert_eq!(
            sql,
            "CREATE RETENTION POLICY \"events_90d\" ON \"events\" USING (\"created_at\") RETAIN FOR '90d'"
        );
    }

    #[test]
    fn test_create_continuous_aggregate_sql() {
        let sql = ExtendedOps::<MockExecutor>::create_continuous_aggregate_sql(
            "hourly_sales",
            "SELECT date_trunc('hour', ts) AS hour, SUM(amount) FROM sales GROUP BY 1",
            "1h",
        );
        assert!(sql.contains("CREATE CONTINUOUS AGGREGATE \"hourly_sales\""));
        assert!(sql.contains("refresh_interval = '1h'"));
    }

    #[test]
    #[allow(deprecated)] // the builder is deprecated on purpose; its output is still pinned
    fn test_create_namespace_sql_no_quotas() {
        let sql = ExtendedOps::<MockExecutor>::create_namespace_sql("tenant_acme", &[]);
        assert_eq!(sql, "CREATE NAMESPACE \"tenant_acme\"");
    }

    #[test]
    #[allow(deprecated)] // the builder is deprecated on purpose; its output is still pinned
    fn test_create_namespace_sql_with_quotas() {
        let sql = ExtendedOps::<MockExecutor>::create_namespace_sql(
            "tenant_acme",
            &[("max_tables", "100"), ("max_storage_gb", "10")],
        );
        assert!(sql.contains("CREATE NAMESPACE \"tenant_acme\" WITH"));
        assert!(sql.contains("max_tables = 100"));
    }

    #[test]
    #[allow(deprecated)] // the builder is deprecated on purpose; its output is still pinned
    fn test_set_namespace_sql() {
        let sql = ExtendedOps::<MockExecutor>::set_namespace_sql("tenant_acme");
        assert_eq!(sql, "SET NAMESPACE \"tenant_acme\"");
    }

    #[test]
    #[allow(deprecated)] // the builder is deprecated on purpose; its output is still pinned
    fn test_drop_namespace_sql() {
        let sql = ExtendedOps::<MockExecutor>::drop_namespace_sql("tenant_acme");
        assert_eq!(sql, "DROP NAMESPACE IF EXISTS \"tenant_acme\"");
    }

    #[test]
    fn test_read_parquet_sql() {
        let sql = ExtendedOps::<MockExecutor>::read_parquet_sql("/data/events.parquet");
        assert_eq!(sql, "SELECT * FROM read_parquet('/data/events.parquet')");
    }

    #[test]
    fn test_read_parquet_sql_escapes_quotes() {
        let sql = ExtendedOps::<MockExecutor>::read_parquet_sql("/data/it's.parquet");
        assert_eq!(sql, "SELECT * FROM read_parquet('/data/it''s.parquet')");
    }

    #[test]
    fn test_read_csv_sql() {
        let sql = ExtendedOps::<MockExecutor>::read_csv_sql("/data/events.csv");
        assert_eq!(sql, "SELECT * FROM read_csv('/data/events.csv')");
    }

    #[test]
    fn test_quote_ident_escapes_double_quotes() {
        let sql = ExtendedOps::<MockExecutor>::refresh_materialized_view_sql("my\"view");
        assert_eq!(sql, "REFRESH MATERIALIZED VIEW \"my\"\"view\"");
    }

    // ── The namespace methods cannot succeed, and now say so ──────────
    //
    // `create_namespace` / `set_namespace` / `drop_namespace` used to emit
    // CREATE/SET/DROP NAMESPACE. No parser in the engine accepts any of those
    // verbs on any wire, so every call was refused, while the rustdoc described
    // a row count on success - an outcome that could not occur.
    //
    // What is pinned below: the call returns `Err`, it returns WITHOUT
    // contacting the server (the executor here panics on any call, so a
    // returned `Err` proves nothing was sent - that is what separates "fails
    // fast with an explanation" from "sends dead SQL and relays a parse
    // error"), and the message names the verb and the cause.
    //
    // Reverting the fix turns these red: the executor panics instead of the
    // method returning `Err`.

    /// An executor that fails the test if any SQL reaches it.
    struct PanickingExecutor;

    impl SqlExecutor for PanickingExecutor {
        fn execute_query(&self, sql: &str, _params: &[Value]) -> Result<QueryResult> {
            panic!("namespace methods must not send SQL to the server; got: {sql}");
        }

        fn execute_statement(&self, sql: &str, _params: &[Value]) -> Result<u64> {
            panic!("namespace methods must not send SQL to the server; got: {sql}");
        }
    }

    fn panicking_db() -> ParticleDB<PanickingExecutor> {
        ParticleDB::with_executor(Config::default(), PanickingExecutor)
    }

    /// Positive control for the probe. A zero-send claim is worthless unless the
    /// instrument can register a send, so this proves the panicking executor
    /// really does fire for a method that contacts the server.
    #[test]
    #[should_panic(expected = "must not send SQL")]
    fn panicking_executor_detects_a_send() {
        let db = panicking_db();
        let _ = ExtendedOps::new(&db).refresh_materialized_view("mv_users");
    }

    fn assert_refusal(err: crate::error::Error, verb: &str) {
        let msg = err.to_string();
        for want in [
            verb,
            "not implemented by any ParticleDB server",
            "can never succeed",
            "separate server per tenant",
            "databases-and-schemas",
        ] {
            assert!(
                msg.contains(want),
                "error message missing {want:?}; got: {msg}"
            );
        }
    }

    #[test]
    #[allow(deprecated)]
    fn create_namespace_fails_without_contacting_the_server() {
        let db = panicking_db();
        let ops = ExtendedOps::new(&db);
        let err = ops
            .create_namespace("tenant_acme", &[])
            .expect_err("CREATE NAMESPACE is refused by the server on every wire");
        assert_refusal(err, "CREATE NAMESPACE");
    }

    #[test]
    #[allow(deprecated)]
    fn create_namespace_with_quotas_fails_without_contacting_the_server() {
        let db = panicking_db();
        let ops = ExtendedOps::new(&db);
        let err = ops
            .create_namespace("tenant_acme", &[("max_tables", "100")])
            .expect_err("CREATE NAMESPACE is refused by the server on every wire");
        assert_refusal(err, "CREATE NAMESPACE");
    }

    #[test]
    #[allow(deprecated)]
    fn set_namespace_fails_without_contacting_the_server() {
        let db = panicking_db();
        let ops = ExtendedOps::new(&db);
        let err = ops
            .set_namespace("tenant_acme")
            .expect_err("SET NAMESPACE is refused by the server on every wire");
        assert_refusal(err, "SET NAMESPACE");
    }

    #[test]
    #[allow(deprecated)]
    fn drop_namespace_fails_without_contacting_the_server() {
        let db = panicking_db();
        let ops = ExtendedOps::new(&db);
        let err = ops
            .drop_namespace("tenant_acme")
            .expect_err("DROP NAMESPACE is refused by the server on every wire");
        assert_refusal(err, "DROP NAMESPACE");
    }
}
