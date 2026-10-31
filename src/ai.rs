use crate::client::{ParticleDB, SqlExecutor};
use crate::error::Result;
use crate::sql::literal;
use crate::types::Value;

/// AI function operations exposed through ParticleDB SQL.
pub struct AIOps<'a, E: SqlExecutor> {
    client: &'a ParticleDB<E>,
}

impl<'a, E: SqlExecutor> AIOps<'a, E> {
    pub(crate) fn new(client: &'a ParticleDB<E>) -> Self {
        Self { client }
    }

    /// Build the SQL to compute an embedding for the given text.
    pub fn embed_sql(text: &str, model: Option<&str>) -> String {
        match model {
            Some(m) => format!("SELECT AI_EMBED({}, {})", literal(text), literal(m)),
            None => format!("SELECT AI_EMBED({})", literal(text)),
        }
    }

    /// Compute an embedding vector for the given text.
    pub fn embed(&self, text: &str, model: Option<&str>) -> Result<Vec<f32>> {
        let mut params = vec![Value::String(text.into())];
        let sql = if let Some(model) = model {
            params.push(Value::String(model.into()));
            "SELECT AI_EMBED($1, $2)"
        } else {
            "SELECT AI_EMBED($1)"
        };
        let result = self.client.query(sql, &params)?;
        match result.rows.into_iter().next() {
            Some(row) => row.get::<Vec<f32>>(0),
            None => Ok(Vec::new()),
        }
    }

    /// Build the SQL to generate text from a prompt using the AI model.
    pub fn generate_sql(prompt: &str) -> String {
        format!("SELECT AI_GENERATE({})", literal(prompt))
    }

    /// Generate text from a prompt using the AI model.
    pub fn generate(&self, prompt: &str) -> Result<String> {
        let result = self
            .client
            .query("SELECT AI_GENERATE($1)", &[Value::String(prompt.into())])?;
        match result.rows.into_iter().next() {
            Some(row) => row.get::<String>(0),
            None => Ok(String::new()),
        }
    }

    /// Build the SQL to classify text into one of the given labels.
    pub fn classify_sql(text: &str, labels: &[&str]) -> String {
        let labels_str = labels
            .iter()
            .map(|l| literal(l))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT AI_CLASSIFY({}, ARRAY[{}])",
            literal(text),
            labels_str
        )
    }

    /// Build the SQL to summarize the given text.
    pub fn summarize_sql(text: &str) -> String {
        format!("SELECT AI_SUMMARIZE({})", literal(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{Config, MockExecutor};
    use crate::types::{QueryResult, Row, Value};

    #[test]
    fn test_embed_sql_default_model() {
        let sql = AIOps::<MockExecutor>::embed_sql("hello world", None);
        assert_eq!(sql, "SELECT AI_EMBED('hello world')");
    }

    #[test]
    fn test_embed_sql_custom_model() {
        let sql = AIOps::<MockExecutor>::embed_sql("hello", Some("text-embedding-3-small"));
        assert_eq!(sql, "SELECT AI_EMBED('hello', 'text-embedding-3-small')");
    }

    #[test]
    fn test_generate_sql() {
        let sql = AIOps::<MockExecutor>::generate_sql("Explain quantum computing");
        assert_eq!(sql, "SELECT AI_GENERATE('Explain quantum computing')");
    }

    #[test]
    fn test_classify_sql() {
        let sql =
            AIOps::<MockExecutor>::classify_sql("I love this product", &["positive", "negative"]);
        assert_eq!(
            sql,
            "SELECT AI_CLASSIFY('I love this product', ARRAY['positive', 'negative'])"
        );
    }

    #[test]
    fn test_summarize_sql() {
        let sql = AIOps::<MockExecutor>::summarize_sql("Long text here...");
        assert_eq!(sql, "SELECT AI_SUMMARIZE('Long text here...')");
    }

    #[test]
    fn test_embed_returns_vector() {
        let db = ParticleDB::mock(Config::default());
        let vec = vec![0.1f32, 0.2, 0.3];
        db.executor().push_response(QueryResult {
            rows: vec![Row::new(
                vec!["embedding".into()],
                vec![Value::Vector(vec.clone())],
            )],
            columns: vec!["embedding".into()],
            rows_affected: 0,
        });

        let result = db.ai().embed("hello", None).unwrap();
        assert_eq!(result, vec);
    }

    #[test]
    fn test_generate_returns_text() {
        let db = ParticleDB::mock(Config::default());
        db.executor().push_response(QueryResult {
            rows: vec![Row::new(
                vec!["result".into()],
                vec![Value::String("Generated text".into())],
            )],
            columns: vec!["result".into()],
            rows_affected: 0,
        });

        let result = db.ai().generate("Tell me a joke").unwrap();
        assert_eq!(result, "Generated text");
    }

    #[test]
    fn test_classify_sql_single_label() {
        let sql = AIOps::<MockExecutor>::classify_sql("text", &["spam"]);
        assert_eq!(sql, "SELECT AI_CLASSIFY('text', ARRAY['spam'])");
    }

    #[test]
    fn test_classify_sql_multiple_labels() {
        let sql = AIOps::<MockExecutor>::classify_sql("text", &["a", "b", "c"]);
        assert!(sql.contains("ARRAY['a', 'b', 'c']"));
    }
}
