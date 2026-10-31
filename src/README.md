# src - the `particledb-sdk` crate sources

| File | What it is for |
|---|---|
| `lib.rs` | Crate root; declares the compiled modules and re-exports the public API. |
| `client.rs` | `Config` (default port 7173, the native wire; `prefer_unix_socket` default true, `unix_socket_dir` default `/tmp`), the `SqlExecutor` trait (`execute_query`, `execute_statement`, `execute_many` with a per-row default), the `ParticleDB<E>` facade and `MockExecutor`. |
| `error.rs` | `Error` (`Server { sqlstate, code, message }`, `Batch(BatchFailure)`, `Connection`, ...) / `Result`. |
| `types.rs` | `Value`, `Row`, `QueryResult` and `FromValue` conversions. Temporal values are integers with fixed units (microseconds / days). |
| `arrow_codec.rs` | Arrow IPC result -> rows (`decode_result`) and parameter rows -> Arrow IPC (`encode_params`). |
| `pdb_wire_executor.rs` | `PdbWireExecutor`: native wire (v1 framing) `SqlExecutor` with a connection pool: HELLO/AUTH handshake, `MSG_QUERY`, prepared `MSG_PREPARE`/`MSG_EXECUTE`/`MSG_EXECUTE_BATCH`, `MSG_BATCH`, native KV calls. Transport: `PdbConn::connect_unix` uses `unix_socket_path_in(Config::unix_socket_dir, port)` for a local host (`is_local_host`: `localhost`, `127.x`, `::1`, no DNS) when `Config::prefer_unix_socket` and the file exists; anything else, or a socket that does not answer, is TCP (`connect_tcp`, which strips `[` `]` so `::1` connects). |
| `kv.rs`, `vector.rs`, `rag.rs`, `ai.rs` | Convenience helpers for native KV functions, vector search, model functions and pipeline DDL. Runtime value arguments use bound parameters. Vector filters are explicitly trusted SQL expressions. |
| `ops.rs` | Not declared in `lib.rs`, so not compiled (a deliberate, still-open API decision; see its header). |

## Constraints in `arrow_codec.rs`

* The Arrow type picks the `Value`; for `Utf8` / `Binary` columns the field's
  `pdb:decl` metadata (the declared SQL type the server stamps) refines it:
  an array arrives as its `ARRAY[...]` constructor TEXT (`INTEGER[]`), a
  `vector(n)` as `Binary` of `n` little-endian `f32`s, an unconstrained
  `numeric` as text (`CAP_NUMERIC_TEXT`), `interval` / `uuid` / `json(b)` as
  text, `timestamptz` as an Arrow `Timestamp` with no zone declared
  `TIMESTAMPTZ`. These shapes were read off a live server; if the server
  changes one, `tests/native_wire_live.rs` `every_type_round_trips` fails.
* An Arrow type the decoder does not know is an error, never a guessed value.
* Parameters use only the Arrow types the server's strict parameter decoder
  accepts (`decode_arrow_param` in the server):
  bool, int64, float32/64, utf8, binary, decimal128, date32. Every other value
  is sent as its PostgreSQL text form in a `Utf8` column. An all-NULL column is
  `Utf8` (Arrow's `Null` type is not accepted by the server).

## Constraints in `pdb_wire_executor.rs`

* `SQLSTATE_BAND` must equal the server's `SQLSTATE_WIRE_BAND`
  (the server's wire protocol); the unit test `band_matches_the_server_table`
  reads the server source and fails on any difference.
* A top-level `MSG_ERROR` frame is always an `Err` (`Error::Server`), even one
  too short to hold its u16 code.
* `execute_batch` decodes every `MSG_BATCH_RESULT` entry: an ERROR entry is
  `Error::Server` (SQLSTATE from a trailing `[XXXXX]` marker, else `HY000` --
  an entry carries no code); an entry count different from the statements
  sent with no error entry is a protocol error. A failed batch's connection is
  dropped (closed), never pooled.
* `execute_many` (autocommit) sends every chunk even after a row fails, since
  the server runs every row of a frame; `execute_many_in_transaction` stops at
  the first failed row and rolls back.
* A streamed result (`MSG_ARROW_STREAM` with the more-chunks flag) is read to
  its last chunk before the connection is pooled.
* EVERY failed request drops its connection (`Pool::put_if_ok`, or `?` before
  `put`): after a transport error or timeout the request may have executed and
  its late reply may still arrive on that socket. The executor never retries a
  sent request. The single exception, `PdbConn::with_prepared`, re-prepares and
  re-sends an EXECUTE the server refused with code 26 (statement unknown):
  nothing executed.
