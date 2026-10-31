# particledb-sdk

Blocking Rust client for ParticleDB over the native wire, with a connection pool, prepared statements, batches, transactions and KV.

## Install

```sh
cargo add particledb-sdk
```

Or add `particledb-sdk = "0.2"` under `[dependencies]` in `Cargo.toml`.

On Linux, the TLS dependency needs a C toolchain, `pkg-config` and OpenSSL headers. On Debian or Ubuntu, install `pkg-config` and `libssl-dev`. macOS and Windows use the system TLS library.

## Quick start

Start a local development server with authentication off:

```sh
curl -fsSL https://get.particledb.ai | bash
particledb start --data-dir ./pdb-dev --no-auth \
  --concurrency-control optimistic --durability groupsync --kv-durability async
```

Connect. `Config::default()` is `localhost:7173`.

```rust,no_run
use particledb_sdk::{Config, ParticleDB};

fn main() -> particledb_sdk::Result<()> {
    let db = ParticleDB::connect(Config::default())?;
    let row = db.query_one("SELECT 42 AS answer", &[])?;
    assert_eq!(row.unwrap().get_by_name::<i64>("answer")?, 42);
    Ok(())
}
```

`connect` checks that the server is reachable before it returns. Calls block. A client can be shared between threads.

## Connect

`Config` fields:

| Name | Default | Meaning |
|---|---|---|
| `host` | `"localhost"` | Server host. |
| `port` | `7173` | Native wire port. |
| `user` | `"particledb"` | Login role. |
| `password` | `""` | Password. |
| `database` | `"particledb"` | Descriptive only. The server hosts one database per instance. |
| `pool_size` | `4` | Maximum live connections. |
| `connect_timeout` | 10 s | Maximum wait for a connection or a pool slot. |
| `io_timeout` | 30 s | Maximum wait for each socket read or write. |
| `tls` | `None` | `Some(TlsConfig::default())` enables verified TLS. |
| `prefer_unix_socket` | `true` | For `localhost`, `127.x` or `::1`, use `<unix_socket_dir>/.s.PDB.<port>` when it exists, else TCP. |
| `unix_socket_dir` | `"/tmp"` | The server's `--unix-socket-dir`. |

DNS resolution uses the operating system resolver.

### Authentication

Start the server with a login role. The password file holds only the password. Keep it readable by you alone.

```sh
particledb start --data-dir ./pdb-dev \
  --auth-method scram-sha-256 --pg-username app --pg-password-file ./pdb-password
```

```rust,no_run
use particledb_sdk::{Config, ParticleDB};

fn main() -> particledb_sdk::Result<()> {
    let db = ParticleDB::connect(Config {
        user: "app".into(),
        password: std::env::var("PARTICLEDB_PASSWORD").unwrap_or_default(),
        ..Config::default()
    })?;
    db.query_one("SELECT 1", &[])?;
    Ok(())
}
```

SCRAM authenticates without TLS. Use TLS for remote access.

### TLS

```rust,no_run
use particledb_sdk::{Config, ParticleDB, TlsConfig};

fn main() -> particledb_sdk::Result<()> {
    let db = ParticleDB::connect(Config {
        host: "db.example.com".into(),
        user: "app".into(),
        password: std::env::var("PARTICLEDB_PASSWORD").unwrap_or_default(),
        tls: Some(TlsConfig::default()),
        ..Config::default()
    })?;
    db.query_one("SELECT 1", &[])?;
    Ok(())
}
```

TLS verifies the certificate chain and hostname against system roots. `TlsConfig::root_cert` adds a private root certificate (PEM path). TLS runs over TCP and never falls back to plaintext. Client certificate authentication is not exposed.

## Usage

### Query with parameters

```rust,no_run
use particledb_sdk::{Config, ParticleDB, Value};

fn main() -> particledb_sdk::Result<()> {
    let db = ParticleDB::connect(Config::default())?;
    db.execute("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)", &[])?;
    db.execute("INSERT INTO items VALUES ($1, $2)",
        &[Value::Int64(1), Value::String("first".into())])?;
    let result = db.query("SELECT id, name FROM items WHERE id = $1", &[Value::Int64(1)])?;
    for row in &result.rows {
        let id: i64 = row.get(0)?;
        let name: String = row.get_by_name("name")?;
        println!("{id} {name}");
    }
    Ok(())
}
```

Parameters are bound separately from the SQL. `Row::get` and `Row::get_by_name` return conversion errors as `Result`. Results are held in memory.

| `Value` variant | SQL type |
|---|---|
| `Null` | NULL |
| `Bool` | `boolean` |
| `Int64` | `smallint`, `integer`, `bigint` |
| `Float32` | `real` |
| `Float64` | `double precision` |
| `Numeric` | `numeric`, `decimal` (exact decimal text) |
| `String` | `text`, `varchar`, `char`, and types with no dedicated variant |
| `Bytes` | `bytea` |
| `Json` | `json`, `jsonb` (text) |
| `Uuid` | `uuid` (canonical text) |
| `Date` | `date` (days since 1970-01-01) |
| `Timestamp` | `timestamp` (microseconds since the epoch) |
| `TimestampTz` | `timestamptz` (microseconds since the epoch, UTC) |
| `Interval { months, days, micros }` | `interval` |
| `Vector` | `vector(n)` |
| `Array` | `T[]` |

### Transactions

```rust,no_run
use particledb_sdk::{Config, ParticleDB, Value};

fn main() -> particledb_sdk::Result<()> {
    let db = ParticleDB::connect(Config::default())?;
    db.transaction(|tx| {
        tx.execute("UPDATE items SET name = $1 WHERE id = $2",
            &[Value::String("updated".into()), Value::Int64(1)])?;
        Ok(())
    })?;
    Ok(())
}
```

The callback runs on one connection. Return `Err` to roll back. A panic drops the connection, which rolls back. If COMMIT rolls back a failed block, `transaction` returns an error. A request that may have reached the server is never retried.

### Batches

`execute_many` is an autocommit batch: rows that succeed commit even if another row fails. `Error::Batch` carries a `BatchFailure` with the applied count and the failed row indexes. For an atomic batch, call `db.executor().execute_many_in_transaction(sql, &rows)`.

### Errors

```rust,no_run
use particledb_sdk::{Config, Error, ParticleDB};

fn main() -> particledb_sdk::Result<()> {
    let db = ParticleDB::connect(Config::default())?;
    match db.execute("INSERT INTO items VALUES (1, 'dup')", &[]) {
        Err(e) if e.sqlstate() == Some("23505") => println!("duplicate key"),
        Err(Error::Connection(msg)) => println!("connection: {msg}"),
        Err(e) => return Err(e),
        Ok(_) => {}
    }
    Ok(())
}
```

`Error::sqlstate()` returns the SQLSTATE when the server sent one.

### KV

```rust,no_run
use particledb_sdk::{Config, ParticleDB};

fn main() -> particledb_sdk::Result<()> {
    let db = ParticleDB::connect(Config::default())?;
    db.kv().set("session:123", "active")?;
    assert_eq!(db.kv().get("session:123")?, Some("active".into()));
    db.kv().delete("session:123")?;
    Ok(())
}
```

Byte-level native KV calls are on the executor: `kv_set`, `kv_get`, `kv_del`, `kv_incr`. See `examples/kv_native_wire.rs`.

### Vector search

```rust,no_run
use particledb_sdk::{Config, ParticleDB};

fn main() -> particledb_sdk::Result<()> {
    let db = ParticleDB::connect(Config::default())?;
    let hits = db.vector().search("docs", "embedding", &[0.1, 0.2, 0.3], 5, None)?;
    for hit in hits {
        println!("{} {:?}", hit.distance, hit.row.values());
    }
    Ok(())
}
```

`search` orders by L2 distance. The `filter` argument is a trusted SQL expression, not user input.

### Limits

Protocol 1, one request per pooled connection. No async I/O, server-side cancellation, native multiplexing or result streaming.

## Compatibility

| Component | Version |
|---|---|
| ParticleDB server | 1.1.19-rc.15 or later |
| Rust | 1.87 or later |

## Testing

Unit tests need no server:

```sh
cargo test --lib
```

Live suites against throwaway servers started from `$PARTICLEDB_BIN` (else `particledb` on PATH):

```sh
tests/run_live.sh
```

Live suites against servers you started:

```sh
PDB_NATIVE_PORT=PORT PDB_AUTH_PORT=AUTH_PORT PDB_AUTH_USER=USER \
  PDB_AUTH_PASS=PASSWORD PDB_TLS_PORT=TLS_PORT PDB_TLS_CA=ca.pem \
  cargo test -- --test-threads=1
```

A live test that skips because a variable is unset is not a pass. The older `integration` suite is ignored by default.

## Links

- https://particledb.ai/sdks/overview/
- https://particledb.ai/sdks/rust/

## License

MIT. See [LICENSE](LICENSE).

The ParticleDB server is under the ParticleDB License: https://particledb.ai/license/
