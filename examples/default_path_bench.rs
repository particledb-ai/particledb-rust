//! The Rust SDK's DEFAULT native-wire path, measured: THREADS application
//! threads share ONE `PdbWireExecutor` built from `Config::default()` (only
//! host/port changed), each a closed loop of one call.
//!
//!     cargo run --release --example default_path_bench -- PORT THREADS SECONDS <kvset|sqlset>
//!
//! `kvset` = `PdbWireExecutor::kv_set` (the native KV opcode); `sqlset` =
//! `SqlExecutor::execute_query("SELECT KV_SET('s:K','v')")`, the same
//! statement kvbench's `sqlset` sends. Prints one kvbench-format line.

use particledb_sdk::client::{Config, SqlExecutor};
use particledb_sdk::pdb_wire_executor::PdbWireExecutor;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let port: u16 = a[1].parse().unwrap();
    let threads: usize = a[2].parse().unwrap();
    let secs: u64 = a[3].parse().unwrap();
    let wl = a.get(4).cloned().unwrap_or_else(|| "kvset".into());
    let config = Config {
        host: "127.0.0.1".into(),
        port,
        ..Config::default()
    };
    let pool = config.pool_size;
    let exec = Arc::new(PdbWireExecutor::new(config));
    let val = "x".repeat(64);
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut hs = Vec::new();
    for id in 0..threads {
        let exec = exec.clone();
        let barrier = barrier.clone();
        let val = val.clone();
        let wl = wl.clone();
        hs.push(std::thread::spawn(move || {
            let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ (id as u64 + 1);
            let mut lat = Vec::with_capacity(1 << 16);
            barrier.wait();
            let end = Instant::now() + Duration::from_secs(secs);
            let mut ops = 0u64;
            loop {
                let t0 = Instant::now();
                if t0 >= end {
                    break;
                }
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let k = x % 100_000;
                if wl == "kvset" {
                    exec.kv_set(format!("s:{k}").as_bytes(), val.as_bytes())
                        .expect("kv_set");
                } else {
                    exec.execute_query(&format!("SELECT KV_SET('s:{k}','{val}')"), &[])
                        .expect("query");
                }
                lat.push(t0.elapsed().as_micros() as u32);
                ops += 1;
            }
            (lat, ops)
        }));
    }
    let srvstat = || -> Option<String> {
        let pid = std::env::var("KVBENCH_SERVER_PID").ok()?;
        let kb = std::env::var("KVBENCH").ok()?;
        let o = std::process::Command::new(kb)
            .args(["srvstat", &pid])
            .output()
            .ok()?;
        Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
    };
    let s0 = srvstat();
    barrier.wait();
    let w0 = Instant::now();
    let mut all = Vec::new();
    let mut total = 0u64;
    for h in hs {
        let (l, o) = h.join().unwrap();
        all.extend(l);
        total += o;
    }
    all.sort_unstable();
    let p = |q: f64| all[((all.len() as f64 * q) as usize).min(all.len() - 1)] as f64 / 1000.0;
    println!(
        "proto=rustsdk clients={threads} wl={wl} pool={pool} ops={total} ops/s={:.0} p50_ms={:.3} p99_ms={:.3}",
        total as f64 / secs as f64,
        p(0.5),
        p(0.99)
    );
    let wall = w0.elapsed().as_secs_f64();
    if let (Some(a), Some(b)) = (s0, srvstat()) {
        println!("SRVRAW {a} | {b} | {total} {wall:.3}");
    }
}
