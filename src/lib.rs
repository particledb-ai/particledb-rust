//! # ParticleDB Rust SDK
//!
//! Rust SDK for [ParticleDB](https://particledb.ai): a typed interface over
//! the native PDB wire ([`pdb_wire_executor::PdbWireExecutor`], port 7173) --
//! Arrow results decoded into [`Row`]s, typed `$n` parameters, bulk
//! [`SqlExecutor::execute_many`]. [`MockExecutor`] stands in for tests.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use particledb_sdk::{ParticleDB, Config, Value};
//!
//! let db = ParticleDB::connect(Config::default()).unwrap();
//!
//! // Raw SQL
//! let result = db.query("SELECT * FROM users WHERE id = $1", &[Value::Int64(1)]).unwrap();
//!
//! // Vector search
//! let results = db.vector().search("documents", "embedding", &[0.1, 0.2, 0.3], 5, None).unwrap();
//!
//! // Key-value
//! db.kv().set("session:123", "active").unwrap();
//! let val = db.kv().get("session:123").unwrap();
//!
//! // AI
//! let embedding = db.ai().embed("hello world", None).unwrap();
//! ```

pub mod ai;
pub mod arrow_codec;
pub mod client;
pub mod error;
pub mod kv;
pub mod pdb_wire_executor;
pub mod rag;
mod sql;
pub mod types;
pub mod vector;

// Re-export the most commonly used items at the crate root.
pub use client::{Config, MockExecutor, ParticleDB, SqlExecutor, TlsConfig};
pub use error::{BatchFailure, Error, Result};
pub use types::{FromValue, QueryResult, Row, Value, VectorSearchResult};
