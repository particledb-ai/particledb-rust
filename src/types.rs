use crate::error::{Error, Result};

/// A dynamically-typed value that can represent any ParticleDB column value.
///
/// Decoded from the native wire's Arrow results (see `arrow_codec`) and
/// encoded as typed Arrow parameters. Temporal values are integers with fixed
/// units so they round-trip exactly: `Timestamp` / `TimestampTz` are
/// MICROSECONDS since 1970-01-01 00:00:00 (`TimestampTz` is an absolute UTC
/// instant), `Date` is DAYS since 1970-01-01. `Numeric` is the exact decimal text (`"123.4500"`), never a float.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// Every integer type (`smallint`, `integer`, `bigint`).
    Int64(i64),
    /// `double precision`.
    Float64(f64),
    /// `text`, `varchar`, `char`, and any type without a dedicated variant.
    String(String),
    /// `bytea`.
    Bytes(Vec<u8>),
    /// `timestamp` (without time zone): microseconds since the epoch.
    Timestamp(i64),
    /// `vector(n)`.
    Vector(Vec<f32>),
    /// `json` / `jsonb` text.
    Json(String),
    /// A SQL array (`T[]`), elements typed by the declared element type.
    Array(Vec<Value>),
    /// `real`.
    Float32(f32),
    /// `numeric` / `decimal`: exact decimal text.
    Numeric(String),
    /// `date`: days since 1970-01-01.
    Date(i32),
    /// `timestamptz`: microseconds since the epoch, UTC.
    TimestampTz(i64),
    /// `interval`.
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
    /// `uuid`, canonical text.
    Uuid(String),
}

/// A single row from a query result, providing typed access by column index or name.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl Row {
    /// Create a new row from column names and values.
    pub fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        Self { columns, values }
    }

    /// Get a typed value by column index.
    pub fn get<T: FromValue>(&self, index: usize) -> Result<T> {
        let val = self
            .values
            .get(index)
            .ok_or_else(|| Error::TypeConversion(format!("column index {} out of range", index)))?;
        T::from_value(val)
    }

    /// Get a typed value by column name.
    pub fn get_by_name<T: FromValue>(&self, name: &str) -> Result<T> {
        let index = self
            .columns
            .iter()
            .position(|c| c == name)
            .ok_or_else(|| Error::TypeConversion(format!("column '{}' not found", name)))?;
        self.get(index)
    }

    /// Return the column names for this row.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Return the raw values for this row.
    pub fn values(&self) -> &[Value] {
        &self.values
    }
}

/// Trait for converting a [`Value`] reference into a concrete Rust type.
pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Result<Self>;
}

impl FromValue for i64 {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Int64(n) => Ok(*n),
            other => Err(Error::TypeConversion(format!(
                "expected Int64, got {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }
}

impl FromValue for f64 {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Float64(n) => Ok(*n),
            Value::Float32(n) => Ok(*n as f64),
            Value::Int64(n) => Ok(*n as f64),
            other => Err(Error::TypeConversion(format!(
                "expected Float64, got {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }
}

impl FromValue for f32 {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Float32(n) => Ok(*n),
            other => Err(Error::TypeConversion(format!(
                "expected Float32, got {:?}",
                other
            ))),
        }
    }
}

impl FromValue for i32 {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Int64(n) => i32::try_from(*n)
                .map_err(|_| Error::TypeConversion(format!("{} does not fit i32", n))),
            other => Err(Error::TypeConversion(format!(
                "expected Int64, got {:?}",
                other
            ))),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Bytes(b) => Ok(b.clone()),
            other => Err(Error::TypeConversion(format!(
                "expected Bytes, got {:?}",
                other
            ))),
        }
    }
}

impl FromValue for Vec<Value> {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Array(a) => Ok(a.clone()),
            other => Err(Error::TypeConversion(format!(
                "expected Array, got {:?}",
                other
            ))),
        }
    }
}

impl FromValue for Value {
    fn from_value(v: &Value) -> Result<Self> {
        Ok(v.clone())
    }
}

impl FromValue for String {
    /// Text-carrying variants: `String`, `Json`, `Numeric`, `Uuid`.
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::String(s) | Value::Json(s) | Value::Numeric(s) | Value::Uuid(s) => Ok(s.clone()),
            other => Err(Error::TypeConversion(format!(
                "expected String, got {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }
}

impl FromValue for bool {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Bool(b) => Ok(*b),
            other => Err(Error::TypeConversion(format!(
                "expected Bool, got {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Null => Ok(None),
            other => Ok(Some(T::from_value(other)?)),
        }
    }
}

impl FromValue for Vec<f32> {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Vector(vec) => Ok(vec.clone()),
            other => Err(Error::TypeConversion(format!(
                "expected Vector, got {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }
}

/// The result of executing a query, containing rows and metadata.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub columns: Vec<String>,
    pub rows_affected: u64,
}

impl QueryResult {
    /// Create an empty query result with no rows.
    pub fn empty() -> Self {
        Self {
            rows: Vec::new(),
            columns: Vec::new(),
            rows_affected: 0,
        }
    }
}

/// A single result from a vector similarity search, pairing a row with its distance.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub row: Row,
    pub distance: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_get_by_index() {
        let row = Row::new(
            vec!["id".into(), "name".into()],
            vec![Value::Int64(42), Value::String("alice".into())],
        );
        assert_eq!(row.get::<i64>(0).unwrap(), 42);
        assert_eq!(row.get::<String>(1).unwrap(), "alice");
    }

    #[test]
    fn test_row_get_by_name() {
        let row = Row::new(
            vec!["id".into(), "name".into()],
            vec![Value::Int64(7), Value::String("bob".into())],
        );
        assert_eq!(row.get_by_name::<i64>("id").unwrap(), 7);
        assert_eq!(row.get_by_name::<String>("name").unwrap(), "bob");
    }

    #[test]
    fn test_row_get_by_name_not_found() {
        let row = Row::new(vec!["id".into()], vec![Value::Int64(1)]);
        let err = row.get_by_name::<i64>("missing").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_row_get_out_of_range() {
        let row = Row::new(vec!["id".into()], vec![Value::Int64(1)]);
        let err = row.get::<i64>(5).unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn test_from_value_i64() {
        assert_eq!(i64::from_value(&Value::Int64(100)).unwrap(), 100);
        assert!(i64::from_value(&Value::String("x".into())).is_err());
    }

    #[test]
    fn test_from_value_f64() {
        assert_eq!(f64::from_value(&Value::Float64(3.14)).unwrap(), 3.14);
        // Int64 -> f64 coercion
        assert_eq!(f64::from_value(&Value::Int64(10)).unwrap(), 10.0);
        assert!(f64::from_value(&Value::String("x".into())).is_err());
    }

    #[test]
    fn test_from_value_string() {
        assert_eq!(
            String::from_value(&Value::String("hello".into())).unwrap(),
            "hello"
        );
        assert!(String::from_value(&Value::Int64(1)).is_err());
    }

    #[test]
    fn test_from_value_bool() {
        assert_eq!(bool::from_value(&Value::Bool(true)).unwrap(), true);
        assert!(bool::from_value(&Value::Int64(1)).is_err());
    }

    #[test]
    fn test_from_value_option_some() {
        let result = Option::<i64>::from_value(&Value::Int64(42)).unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_from_value_option_none() {
        let result = Option::<i64>::from_value(&Value::Null).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_from_value_vec_f32() {
        let v = vec![1.0f32, 2.0, 3.0];
        let result = Vec::<f32>::from_value(&Value::Vector(v.clone())).unwrap();
        assert_eq!(result, v);
        assert!(Vec::<f32>::from_value(&Value::Int64(1)).is_err());
    }

    #[test]
    fn test_value_null_handling() {
        let row = Row::new(vec!["nullable_col".into()], vec![Value::Null]);
        // Null as Option succeeds
        assert_eq!(row.get::<Option<i64>>(0).unwrap(), None);
        // Null as concrete type fails
        assert!(row.get::<i64>(0).is_err());
    }

    #[test]
    fn test_query_result_empty() {
        let qr = QueryResult::empty();
        assert!(qr.rows.is_empty());
        assert!(qr.columns.is_empty());
        assert_eq!(qr.rows_affected, 0);
    }

    #[test]
    fn test_row_columns() {
        let row = Row::new(
            vec!["a".into(), "b".into()],
            vec![Value::Int64(1), Value::Int64(2)],
        );
        assert_eq!(row.columns(), &["a".to_string(), "b".to_string()]);
    }
}
