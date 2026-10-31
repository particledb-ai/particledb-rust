use crate::client::{ParticleDB, SqlExecutor};
use crate::error::{Error, Result};
use crate::sql::{literal, pipeline_name};
use crate::types::Value;

/// Retrieval queries and reusable pipeline definitions.
pub struct RAGOps<'a, E: SqlExecutor> {
    client: &'a ParticleDB<E>,
}

impl<'a, E: SqlExecutor> RAGOps<'a, E> {
    pub(crate) fn new(client: &'a ParticleDB<E>) -> Self {
        Self { client }
    }

    pub fn query_sql(
        question: &str,
        table: &str,
        vec_col: &str,
        text_col: &str,
        k: usize,
    ) -> String {
        format!(
            "SELECT RAG_QUERY({}, {}, {}, {}, {k}) AS answer",
            literal(question),
            literal(table),
            literal(vec_col),
            literal(text_col)
        )
    }

    /// Run a named pipeline. The server must have a model provider configured.
    pub fn query(&self, question: &str, pipeline: &str) -> Result<String> {
        let row = self
            .client
            .query_one(
                "SELECT RAG($1, $2) AS answer",
                &[
                    Value::String(question.into()),
                    Value::String(pipeline.into()),
                ],
            )?
            .ok_or_else(|| Error::Query("RAG returned no result".into()))?;
        row.get::<String>(0)
    }

    /// Build a vector-mode pipeline definition using simple SQL identifiers.
    pub fn create_pipeline_sql(
        name: &str,
        table: &str,
        vec_col: &str,
        text_col: &str,
        k: usize,
    ) -> Result<String> {
        pipeline_name(name, false)?;
        pipeline_name(table, true)?;
        pipeline_name(vec_col, false)?;
        pipeline_name(text_col, false)?;
        if k == 0 {
            return Err(Error::Query("k must be positive".into()));
        }
        Ok(format!("CREATE RAG PIPELINE {name} TABLE {table} VECTOR_COLUMN {vec_col} TEXT_COLUMN {text_col} TOP_K {k} MODE vector"))
    }

    pub fn create_pipeline(
        &self,
        name: &str,
        table: &str,
        vec_col: &str,
        text_col: &str,
        k: usize,
    ) -> Result<()> {
        self.client.execute(
            &Self::create_pipeline_sql(name, table, vec_col, text_col, k)?,
            &[],
        )?;
        Ok(())
    }
}
