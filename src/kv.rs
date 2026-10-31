use crate::client::{ParticleDB, SqlExecutor};
use crate::error::Result;
use crate::sql::literal;
use crate::types::Value;

/// String key-value operations in the database's native KV store.
pub struct KVOps<'a, E: SqlExecutor> {
    client: &'a ParticleDB<E>,
}

impl<'a, E: SqlExecutor> KVOps<'a, E> {
    pub(crate) fn new(client: &'a ParticleDB<E>) -> Self {
        Self { client }
    }

    pub fn set_sql(key: &str, value: &str) -> String {
        format!("SELECT KV_SET({}, {})", literal(key), literal(value))
    }
    pub fn get_sql(key: &str) -> String {
        format!("SELECT KV_GET({})", literal(key))
    }
    pub fn delete_sql(key: &str) -> String {
        format!("SELECT KV_DEL({})", literal(key))
    }
    pub fn mset_sql(pairs: &[(&str, &str)]) -> String {
        let args = pairs
            .iter()
            .flat_map(|(k, v)| [literal(k), literal(v)])
            .collect::<Vec<_>>()
            .join(", ");
        format!("SELECT KV_MSET({args})")
    }
    pub fn incr_sql(key: &str, amount: i64) -> String {
        format!("SELECT KV_INCR({}, {amount})", literal(key))
    }
    pub fn exists_sql(key: &str) -> String {
        format!("SELECT KV_EXISTS({})", literal(key))
    }
    pub fn ttl_sql(key: &str) -> String {
        format!("SELECT KV_TTL({})", literal(key))
    }
    /// Build a glob-pattern lookup, for example `session:*`.
    pub fn keys_sql(pattern: &str) -> String {
        format!("SELECT KV_KEYS({})", literal(pattern))
    }

    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        self.client.query(
            "SELECT KV_SET($1, $2)",
            &[Value::String(key.into()), Value::String(value.into())],
        )?;
        Ok(())
    }
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        match self
            .client
            .query_one("SELECT KV_GET($1)", &[Value::String(key.into())])?
        {
            Some(row) => row.get::<Option<String>>(0),
            None => Ok(None),
        }
    }
    pub fn delete(&self, key: &str) -> Result<bool> {
        match self
            .client
            .query_one("SELECT KV_DEL($1)", &[Value::String(key.into())])?
        {
            Some(row) => Ok(row.get::<i64>(0)? > 0),
            None => Ok(false),
        }
    }
}
