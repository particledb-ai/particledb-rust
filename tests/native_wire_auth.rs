//! LIVE: the native-wire handshake authenticates when the server challenges.
//!
//! Runs only when `PDB_AUTH_PORT`, `PDB_AUTH_USER` and `PDB_AUTH_PASS` name a
//! server that requires authentication (127.0.0.1); otherwise prints a `SKIP`
//! line. Prints `NWA_PASS` when it ran and passed. `tests/run_live.sh` starts
//! such a server alongside the `--no-auth` one.

use particledb_sdk::pdb_wire_executor::PdbWireExecutor;
use particledb_sdk::{Config, SqlExecutor, Value};

#[test]
fn handshake_authenticates_and_refuses_a_bad_password() {
    let (Ok(port), Ok(user), Ok(pass)) = (
        std::env::var("PDB_AUTH_PORT"),
        std::env::var("PDB_AUTH_USER"),
        std::env::var("PDB_AUTH_PASS"),
    ) else {
        println!("SKIP native_wire_auth: PDB_AUTH_PORT/USER/PASS not set");
        return;
    };
    let port: u16 = port.parse().expect("PDB_AUTH_PORT");
    let cfg = |password: &str| Config {
        host: "127.0.0.1".into(),
        port,
        user: user.clone(),
        password: password.into(),
        pool_size: 1,
        ..Config::default()
    };

    let good = PdbWireExecutor::new(cfg(&pass));
    let r = good
        .execute_query("SELECT $1 + 1", &[Value::Int64(41)])
        .expect("authenticated query");
    assert_eq!(r.rows[0].get::<i64>(0).unwrap(), 42);

    let bad = PdbWireExecutor::new(cfg("definitely-not-the-password"));
    match bad.execute_query("SELECT 1", &[]) {
        Err(e) => assert_eq!(e.sqlstate(), Some("28P01"), "{e:?}"),
        Ok(r) => panic!("a wrong password was accepted: {} rows", r.rows.len()),
    }
    println!("NWA_PASS");
}
