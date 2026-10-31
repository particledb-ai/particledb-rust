//! Example: native-wire KV ops through the Rust SDK.
//!
//! Demonstrates `PdbWireExecutor::kv_{set,get,del,incr}` - the fast path that
//! bypasses SQL parsing/planning and writes directly into the engine's KV store.
//!
//! Run a local server:
//!     particledb start --no-auth --concurrency-control optimistic --pdb-addr 127.0.0.1:7173
//!
//! Then:
//!     cargo run --release --example kv_native_wire

use particledb_sdk::client::Config;
use particledb_sdk::pdb_wire_executor::PdbWireExecutor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: 7173,
        pool_size: 4,
        ..Config::default()
    };
    let exec = PdbWireExecutor::new(config);
    exec.ping()?;

    exec.kv_set(b"hello", b"world")?;
    let v = exec.kv_get(b"hello")?.expect("set then get returns Some");
    assert_eq!(v, b"world");
    println!("kv_get(hello) = {:?}", std::str::from_utf8(&v).unwrap());

    let n0 = exec.kv_incr(b"counter")?;
    let n1 = exec.kv_incr(b"counter")?;
    assert_eq!(n1, n0 + 1);
    println!("kv_incr(counter) {} -> {}", n0, n1);

    exec.kv_del(b"hello")?;
    assert!(exec.kv_get(b"hello")?.is_none());
    println!("kv_del(hello) ok");

    let start = std::time::Instant::now();
    let n = 100_000;
    for i in 0..n {
        let k = format!("k{}", i);
        exec.kv_set(k.as_bytes(), b"value")?;
    }
    let dur = start.elapsed();
    println!(
        "{} sequential kv_set in {:?} ({:.0} RPS, single-thread, single-conn-pooled)",
        n,
        dur,
        n as f64 / dur.as_secs_f64()
    );

    Ok(())
}
