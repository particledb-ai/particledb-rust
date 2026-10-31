//! Live-server test: a failed statement inside a native-wire batch must be
//! returned as an `Err`, never as success, and a failed transaction batch must
//! not leave the pooled connection inside a failed transaction.
//!
//! Runs only when `PDB_NATIVE_PORT` is set (host 127.0.0.1, a `--no-auth`
//! server); otherwise it prints a skip line and returns. Prints
//! `NBE_PASS rust` when every case passed, so a runner can prove it ran.
//!
//! ```bash
//! PDB_NATIVE_PORT=48720 cargo test --test native_batch_errors -- --nocapture
//! ```
//!
//! The SDK has no transaction API on the native wire, so case (b) sends the
//! transaction as the batch `[BEGIN, INSERT k3, INSERT k3, COMMIT]` -- exactly
//! what a transaction over this executor is. Row presence is checked with
//! `DELETE ... WHERE id = k` and its affected-row count. (Prepared bulk
//! failures -- `execute_many` -- are covered by `native_wire_live.rs`.)

use particledb_sdk::pdb_wire_executor::PdbWireExecutor;
use particledb_sdk::{Config, Error, SqlExecutor};

const TABLE: &str = "nbe_rust";

fn expect_dup(case: &str, r: Result<u64, Error>, failures: &mut Vec<String>) {
    match r {
        Err(e) if e.to_string().contains("duplicate key") => {
            println!("NBE case {case}: error returned: {e}");
        }
        Err(e) => failures.push(format!("({case}) wrong error: {e}")),
        Ok(n) => failures.push(format!("({case}) failed batch reported SUCCESS: Ok({n})")),
    }
}

#[test]
fn native_batch_errors_are_returned() {
    let port: u16 = match std::env::var("PDB_NATIVE_PORT") {
        Ok(p) => p.parse().expect("PDB_NATIVE_PORT must be a port number"),
        Err(_) => {
            println!("SKIP native_batch_errors: PDB_NATIVE_PORT not set");
            return;
        }
    };
    // pool_size 1: case (c) runs on whatever connection the pool hands back
    // after the failed transaction.
    let exec = PdbWireExecutor::new(Config {
        host: "127.0.0.1".into(),
        port,
        pool_size: 1,
        ..Config::default()
    });
    let stmt = |sql: &str| exec.execute_statement(sql, &[]);
    stmt(&format!("DROP TABLE IF EXISTS {TABLE}")).expect("drop");
    stmt(&format!("CREATE TABLE {TABLE} (id INT PRIMARY KEY)")).expect("create");

    let mut failures: Vec<String> = Vec::new();
    let ins = |k: i32| format!("INSERT INTO {TABLE} VALUES ({k})");

    // (a) batch [INSERT k1, INSERT k1, INSERT k2] -> Err.
    let (i1, i2) = (ins(1), ins(2));
    expect_dup("a", exec.execute_batch(&[&i1, &i1, &i2]), &mut failures);

    // (b) transaction [INSERT k3, INSERT k3] -> Err.
    let i3 = ins(3);
    expect_dup(
        "b",
        exec.execute_batch(&["BEGIN", &i3, &i3, "COMMIT"]),
        &mut failures,
    );

    // (c) a plain statement on the same executor succeeds -- the connection
    // was not left inside the failed transaction -- and the failed
    // transaction committed nothing.
    match stmt(&ins(30)) {
        Ok(1) => println!("NBE case c: plain statement after failed transaction succeeded"),
        other => failures.push(format!("(c) plain statement after failed txn: {other:?}")),
    }
    match stmt(&format!("DELETE FROM {TABLE} WHERE id = 3")) {
        Ok(0) => println!("NBE case c: failed transaction committed nothing"),
        other => failures.push(format!("(c) k3 should be absent, DELETE gave {other:?}")),
    }

    // (d) no false positives: an all-success batch returns success with the
    // summed row count, and a successful transaction commits (row visible).
    let (i4, i5) = (ins(4), ins(5));
    match exec.execute_batch(&[&i4, &i5]) {
        Ok(2) => println!("NBE case d: successful batch returned Ok(2)"),
        other => failures.push(format!("(d) successful batch: {other:?}")),
    }
    let (i6, i7) = (ins(6), ins(7));
    match exec.execute_batch(&["BEGIN", &i6, &i7, "COMMIT"]) {
        Ok(_) => {}
        Err(e) => failures.push(format!("(d) successful transaction: Err({e})")),
    }
    match stmt(&format!("DELETE FROM {TABLE} WHERE id IN (6, 7)")) {
        Ok(2) => println!("NBE case d: successful transaction committed (2 rows visible)"),
        other => failures.push(format!("(d) committed rows, DELETE gave {other:?}")),
    }

    let _ = stmt(&format!("DROP TABLE IF EXISTS {TABLE}"));
    if !failures.is_empty() {
        panic!(
            "native batch error cases failed:\n  {}",
            failures.join("\n  ")
        );
    }
    println!("NBE_PASS rust");
}
