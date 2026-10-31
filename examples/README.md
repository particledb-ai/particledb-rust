# examples

Runnable examples (`cargo run --release --example <name> -- …`) against a live
server with the native wire enabled.

| file | what it is for |
|---|---|
| `kv_native_wire.rs` | The native-wire KV calls (`PdbWireExecutor::kv_{set,get,del,incr}`) end to end, then a sequential `kv_set` loop. |
| `default_path_bench.rs` | The SDK's DEFAULT path, measured: N threads sharing ONE `PdbWireExecutor` from `Config::default()` (host/port only), each a closed loop of `kv_set` (`kvset`) or `execute_query("SELECT KV_SET(..)")` (`sqlset`). Prints a kvbench-format line; with `KVBENCH_SERVER_PID` + `KVBENCH` set, the server's raw counters around the timed window. |

Constraint: `execute_query` decodes its Arrow result into rows (it returned
an empty result before 2026-10-12), and every new pooled connection now opens
with a HELLO round trip (plus AUTH when challenged), so `sqlset` numbers from
before that date measured less client work.

Constraint: `localhost` / `127.0.0.1` makes the SDK try the Unix socket
`/tmp/.s.PDB.<port>` first, so a localhost number is a Unix-socket number.
