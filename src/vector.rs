use crate::client::{ParticleDB, SqlExecutor};
use crate::error::{Error, Result};
use crate::sql::identifier;
use crate::types::VectorSearchResult;

/// Vector search operations on ParticleDB tables.
pub struct VectorOps<'a, E: SqlExecutor> {
    client: &'a ParticleDB<E>,
}

impl<'a, E: SqlExecutor> VectorOps<'a, E> {
    pub(crate) fn new(client: &'a ParticleDB<E>) -> Self {
        Self { client }
    }

    /// Build the SQL for a vector similarity search.
    ///
    /// Returns a query like:
    /// ```sql
    /// SELECT *, L2_DISTANCE(<column>, VECTOR('[...]')) AS __distance
    /// FROM <table> [WHERE <filter>] ORDER BY __distance LIMIT <k>
    /// ```
    ///
    /// The distance is selected explicitly, as the Go SDK does: the server adds
    /// no hidden distance column, and this is a form the planner sends to the
    /// column's vector index.
    pub fn search_sql(
        table: &str,
        column: &str,
        query_vector: &[f32],
        k: usize,
        filter: Option<&str>,
    ) -> String {
        let vec_str = query_vector
            .iter()
            .map(|v| format!("{}", v))
            .collect::<Vec<_>>()
            .join(",");

        let where_clause = match filter {
            Some(f) => format!(" WHERE {}", f),
            None => String::new(),
        };

        let table = table
            .split('.')
            .map(identifier)
            .collect::<Vec<_>>()
            .join(".");
        let column = identifier(column);
        format!("SELECT *, L2_DISTANCE({column}, VECTOR('[{vec_str}]')) AS __distance FROM {table}{where_clause} ORDER BY __distance LIMIT {k}")
    }

    /// Execute a vector similarity search against the database.
    /// `filter` is a trusted SQL expression, not a value or user input.
    pub fn search(
        &self,
        table: &str,
        column: &str,
        query_vector: &[f32],
        k: usize,
        filter: Option<&str>,
    ) -> Result<Vec<VectorSearchResult>> {
        if query_vector.is_empty() || query_vector.iter().any(|v| !v.is_finite()) || k == 0 {
            return Err(Error::Query(
                "vector must be nonempty and finite; k must be positive".into(),
            ));
        }
        let sql = Self::search_sql(table, column, query_vector, k, filter);
        let result = self.client.query(&sql, &[])?;

        let mut results = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            let distance = row.get_by_name::<f64>("__distance")?;
            results.push(VectorSearchResult { row, distance });
        }
        Ok(results)
    }

    /// Build the SQL for creating a vector index.
    ///
    /// Returns a statement like:
    /// ```sql
    /// CREATE INDEX idx_<table>_<column> ON <table> USING hnsw (<column>)
    /// WITH (m = <m>, ef_construction = <ef>)
    /// ```
    pub fn create_index_sql(
        table: &str,
        column: &str,
        index_type: &str,
        m: Option<usize>,
        ef: Option<usize>,
    ) -> String {
        let index_name = identifier(&format!("idx_{}_{}", table, column));
        let table = table
            .split('.')
            .map(identifier)
            .collect::<Vec<_>>()
            .join(".");
        let column = identifier(column);
        let index_type = identifier(index_type);
        let mut sql = format!("CREATE INDEX {index_name} ON {table} USING {index_type} ({column})");

        let mut with_parts = Vec::new();
        if let Some(m_val) = m {
            with_parts.push(format!("m = {}", m_val));
        }
        if let Some(ef_val) = ef {
            with_parts.push(format!("ef_construction = {}", ef_val));
        }
        if !with_parts.is_empty() {
            sql.push_str(&format!(" WITH ({})", with_parts.join(", ")));
        }

        sql
    }

    /// Create a vector index on a table column.
    pub fn create_index(&self, table: &str, column: &str, index_type: &str) -> Result<()> {
        let sql = Self::create_index_sql(table, column, index_type, None, None);
        self.client.execute(&sql, &[])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Config, MockExecutor};
    use crate::types::{QueryResult, Row, Value};

    #[test]
    fn test_search_sql_no_filter() {
        let sql = VectorOps::<MockExecutor>::search_sql(
            "documents",
            "embedding",
            &[1.0, 2.0, 3.0],
            5,
            None,
        );
        assert_eq!(
            sql,
            r#"SELECT *, L2_DISTANCE("embedding", VECTOR('[1,2,3]')) AS __distance FROM "documents" ORDER BY __distance LIMIT 5"#
        );
        assert!(sql.contains("LIMIT 5"));
        assert!(!sql.contains("WHERE"));
    }

    #[test]
    fn test_search_sql_with_filter() {
        let sql = VectorOps::<MockExecutor>::search_sql(
            "docs",
            "vec",
            &[0.5],
            10,
            Some("category = 'science'"),
        );
        assert!(sql.contains("WHERE category = 'science'"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_create_index_sql_defaults() {
        let sql =
            VectorOps::<MockExecutor>::create_index_sql("my_table", "my_col", "hnsw", None, None);
        assert_eq!(
            sql,
            r#"CREATE INDEX "idx_my_table_my_col" ON "my_table" USING "hnsw" ("my_col")"#
        );
    }

    #[test]
    fn test_create_index_sql_with_params() {
        let sql = VectorOps::<MockExecutor>::create_index_sql(
            "docs",
            "embedding",
            "hnsw",
            Some(24),
            Some(200),
        );
        assert!(sql.contains("WITH (m = 24, ef_construction = 200)"));
    }

    #[test]
    fn test_search_returns_results() {
        let db = ParticleDB::mock(Config::default());
        let qr = QueryResult {
            rows: vec![Row::new(
                vec!["id".into(), "__distance".into()],
                vec![Value::Int64(1), Value::Float64(0.123)],
            )],
            columns: vec!["id".into(), "__distance".into()],
            rows_affected: 0,
        };
        db.executor().push_response(qr);

        let results = db
            .vector()
            .search("docs", "embedding", &[1.0, 2.0], 5, None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].distance, 0.123);
        assert_eq!(results[0].row.get::<i64>(0).unwrap(), 1);
    }

    #[test]
    fn test_create_index_executes() {
        let db = ParticleDB::mock(Config::default());
        db.executor().push_response(QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
            rows_affected: 0,
        });
        let result = db.vector().create_index("docs", "embedding", "hnsw");
        assert!(result.is_ok());
    }
}
