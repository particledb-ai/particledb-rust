//! Unix-domain-socket selection for `PdbWireExecutor`.
//!
//! Unit part (no server): which hosts try the socket and the path convention
//! (`<unix_socket_dir>/.s.PDB.<port>`, the server's `--unix-socket-dir`).
//!
//! Live part (needs `PDB_NATIVE_PORT`, a `--no-auth` server with its default
//! socket in `/tmp`; else a `SKIP` line): the route is proven by a port with
//! NO TCP listener whose `<dir>/.s.PDB.<port>` is a symlink to the live
//! server's socket, so only the socket can answer. `prefer_unix_socket =
//! false` and another `unix_socket_dir` then fail; a stale socket file in
//! front of a live TCP port falls back to TCP. Prints `NUS_PASS rust`.
//!
//! `PDB_NATIVE_PORT=<port> cargo test --test unix_socket -- --nocapture`

#![cfg(unix)]

use particledb_sdk::pdb_wire_executor::{
    is_local_host, unix_socket_path, unix_socket_path_in, PdbWireExecutor, DEFAULT_UNIX_SOCKET_DIR,
};
use particledb_sdk::Config;

#[test]
fn local_host_literals_and_path_convention() {
    for h in [
        "localhost",
        "LOCALHOST",
        "127.0.0.1",
        "127.9.9.9",
        "::1",
        "[::1]",
    ] {
        assert!(is_local_host(h), "{h}");
    }
    for h in ["10.0.0.7", "db.example.com", "192.0.2.1", "::2"] {
        assert!(!is_local_host(h), "{h}");
    }
    assert_eq!(DEFAULT_UNIX_SOCKET_DIR, "/tmp");
    assert_eq!(unix_socket_path(5440), "/tmp/.s.PDB.5440");
    assert_eq!(
        unix_socket_path_in("/run/pdb", 7001),
        "/run/pdb/.s.PDB.7001"
    );
    let d = Config::default();
    assert!(d.prefer_unix_socket);
    assert_eq!(d.unix_socket_dir, "/tmp");
}

fn exec(host: &str, port: u16, prefer: bool, dir: &str) -> PdbWireExecutor {
    PdbWireExecutor::new(Config {
        host: host.into(),
        port,
        pool_size: 1,
        prefer_unix_socket: prefer,
        unix_socket_dir: dir.into(),
        ..Config::default()
    })
}

#[test]
fn live_route_selection() {
    let Some(live) = std::env::var("PDB_NATIVE_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
    else {
        println!("SKIP unix_socket: PDB_NATIVE_PORT not set");
        return;
    };
    let real = unix_socket_path(live);
    if !std::path::Path::new(&real).exists() {
        println!("SKIP unix_socket: no server socket at {real}");
        return;
    }
    let dir = std::path::PathBuf::from(format!("/tmp/pdbrs-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let d = dir.to_str().unwrap();
    let dead = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    std::os::unix::fs::symlink(&real, unix_socket_path_in(d, dead)).unwrap();

    for h in ["127.0.0.1", "localhost", "::1"] {
        exec(h, dead, true, d)
            .ping()
            .unwrap_or_else(|e| panic!("{h}: socket route failed: {e}"));
    }
    assert!(
        exec("127.0.0.1", dead, false, d).ping().is_err(),
        "prefer_unix_socket=false used the socket"
    );
    assert!(
        exec("127.0.0.1", dead, true, "/nonexistent")
            .ping()
            .is_err(),
        "another dir used the socket"
    );

    // Stale socket file in front of a live TCP port: falls back to TCP.
    let stale = unix_socket_path_in(d, live);
    drop(std::os::unix::net::UnixListener::bind(&stale).unwrap());
    assert!(std::path::Path::new(&stale).exists());
    exec("127.0.0.1", live, true, d)
        .ping()
        .expect("stale socket must fall back to TCP");

    let _ = std::fs::remove_dir_all(&dir);
    println!("NUS_PASS rust");
}
