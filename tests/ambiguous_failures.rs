//! Ambiguous transport failures: surfaced, never retried, connection discarded.
//!
//! Once any byte of a request may have reached the server, a socket error or
//! EOF is AMBIGUOUS: the server may have executed and committed it. The
//! executor must return `Error::Connection`, must not resend, and must NOT
//! return that connection to the pool -- it may still deliver a late reply, or
//! be inside a transaction.
//!
//! A real TCP fake server (no ParticleDB needed) counts the requests it read
//! and the connections it accepted (the connect-time HELLO is answered and
//! not counted). Each request is answered per a plan:
//! `Close` reads it and closes the socket, `Ok` answers success.
//!
//! The executor has no client-side retry at all (so there is no pre-send
//! retry to preserve). A response deadline also leaves the outcome unknown;
//! the timeout case checks that a late reply cannot enter the next operation.

#[cfg(unix)]
use particledb_sdk::pdb_wire_executor::unix_socket_path;
use particledb_sdk::pdb_wire_executor::PdbWireExecutor;
use particledb_sdk::{Config, Error, SqlExecutor};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const MSG_HELLO: u8 = 0x00;
const MSG_HELLO_OK: u8 = 0x80;
const MSG_QUERY: u8 = 0x01;
const MSG_BATCH: u8 = 0x02;
const MSG_PING: u8 = 0x03;
const MSG_CLOSE: u8 = 0x04;
const MSG_KV_SET: u8 = 0x10;
const MSG_OK: u8 = 0x82;
const MSG_PONG: u8 = 0x84;
const MSG_BATCH_RESULT: u8 = 0x85;

#[derive(Clone, Copy)]
enum Act {
    Close,
    Delay,
    Ok,
}

#[derive(Default)]
struct Counts {
    accepts: usize,
    requests: usize,
}

struct FakeServer {
    port: u16,
    counts: Arc<Mutex<Counts>>,
}

impl FakeServer {
    fn start(plan: &[Act]) -> Self {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        // The executor tries this Unix socket first on 127.0.0.1. A stale
        // socket FILE refuses the connect and the executor falls back to TCP;
        // only a LIVE listener there would bypass the fake server.
        #[cfg(unix)]
        assert!(
            std::os::unix::net::UnixStream::connect(unix_socket_path(port)).is_err(),
            "a live Unix socket serves port {port}; the fake TCP server would not be used"
        );
        let counts = Arc::new(Mutex::new(Counts::default()));
        let plan = Arc::new(Mutex::new(plan.iter().copied().collect::<VecDeque<_>>()));
        let c2 = counts.clone();
        thread::spawn(move || {
            for s in l.incoming() {
                let Ok(s) = s else { return };
                c2.lock().unwrap().accepts += 1;
                let (c3, p3) = (c2.clone(), plan.clone());
                thread::spawn(move || serve(s, c3, p3));
            }
        });
        FakeServer { port, counts }
    }
    fn accepts(&self) -> usize {
        self.counts.lock().unwrap().accepts
    }
    fn requests(&self) -> usize {
        self.counts.lock().unwrap().requests
    }
    fn exec(&self) -> PdbWireExecutor {
        PdbWireExecutor::new(Config {
            host: "127.0.0.1".into(),
            port: self.port,
            pool_size: 2,
            ..Config::default()
        })
    }
}

fn frame(t: u8, body: &[u8]) -> Vec<u8> {
    let mut v = (body.len() as u32).to_le_bytes().to_vec();
    v.extend_from_slice(&[0, 0, t, 0]);
    v.extend_from_slice(body);
    v
}

fn serve(mut s: TcpStream, counts: Arc<Mutex<Counts>>, plan: Arc<Mutex<VecDeque<Act>>>) {
    loop {
        let mut hdr = [0u8; 8];
        if s.read_exact(&mut hdr).is_err() {
            return;
        }
        let plen = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let mtype = hdr[6];
        let mut payload = vec![0u8; plen];
        if s.read_exact(&mut payload).is_err() {
            return;
        }
        if mtype == MSG_CLOSE {
            return;
        }
        // The connect handshake is not a request: answer it with a v2-shaped
        // HELLO_OK (no AUTH challenge) and do not count it.
        if mtype == MSG_HELLO {
            let mut ok = vec![1u8, 0x10, 0x01];
            ok.extend_from_slice(&[0u8; 8]);
            if s.write_all(&frame(MSG_HELLO_OK, &ok)).is_err() {
                return;
            }
            continue;
        }
        counts.lock().unwrap().requests += 1;
        let act = plan.lock().unwrap().pop_front().unwrap_or(Act::Ok);
        if let Act::Close = act {
            return; // drops (closes) the socket after reading the request
        }
        if let Act::Delay = act {
            thread::sleep(Duration::from_millis(300));
        }
        let reply = match mtype {
            MSG_QUERY | MSG_KV_SET => frame(MSG_OK, &1u64.to_le_bytes()),
            MSG_PING => frame(MSG_PONG, &[]),
            MSG_BATCH => {
                let n = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let mut b = n.to_le_bytes().to_vec();
                for _ in 0..n {
                    b.push(MSG_OK);
                    b.extend_from_slice(&1u64.to_le_bytes());
                }
                frame(MSG_BATCH_RESULT, &b)
            }
            _ => return,
        };
        if s.write_all(&reply).is_err() {
            return;
        }
    }
}

fn assert_conn_err<T: std::fmt::Debug>(what: &str, r: Result<T, Error>) {
    match r {
        Err(Error::Connection(_)) => {}
        other => panic!("{what}: expected Error::Connection, got {other:?}"),
    }
}

#[test]
fn batch_dropped_after_send_is_err_sent_once_conn_discarded() {
    let srv = FakeServer::start(&[Act::Close]);
    let ex = srv.exec();
    let r = ex.execute_batch(&["INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"]);
    assert_eq!(srv.requests(), 1, "the batch was resent");
    assert_conn_err("batch", r);
    assert_eq!(ex.execute_batch(&["INSERT INTO t VALUES (3)"]).unwrap(), 1);
    assert_eq!(srv.accepts(), 2, "the dead connection was reused");
}

#[test]
fn statement_dropped_after_send_is_err_sent_once_conn_discarded() {
    let srv = FakeServer::start(&[Act::Close]);
    let ex = srv.exec();
    let r = ex.execute_statement("SELECT KV_SET('k', 'v')", &[]);
    assert_eq!(srv.requests(), 1, "the statement was resent");
    assert_conn_err("statement", r);
    assert_eq!(ex.execute_statement("SELECT 1", &[]).unwrap(), 1);
    assert_eq!(srv.accepts(), 2, "the dead connection was reused");
}

#[test]
fn kv_set_dropped_after_send_is_err_sent_once_conn_discarded() {
    let srv = FakeServer::start(&[Act::Close]);
    let ex = srv.exec();
    let r = ex.kv_set(b"k", b"v");
    assert_eq!(srv.requests(), 1, "KV_SET was resent");
    assert_conn_err("kv_set", r);
    // The next call must open a NEW socket rather than write on the dead one.
    ex.kv_set(b"k", b"v2")
        .expect("kv_set on a fresh connection");
    assert_eq!(
        srv.accepts(),
        2,
        "the dead connection was returned to the pool"
    );
    assert_eq!(srv.requests(), 2);
}

#[test]
fn ping_dropped_is_err_and_conn_discarded() {
    let srv = FakeServer::start(&[Act::Close]);
    let ex = srv.exec();
    assert_conn_err("ping", ex.ping());
    assert_eq!(srv.requests(), 1);
    ex.ping().expect("ping on a fresh connection");
    assert_eq!(
        srv.accepts(),
        2,
        "the dead connection was returned to the pool"
    );
}

#[test]
fn connect_refused_is_a_connection_error() {
    // Bind then drop: nothing listens on the port, so the connect is refused
    // and no request byte is ever written.
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let ex = PdbWireExecutor::new(Config {
        host: "127.0.0.1".into(),
        port,
        pool_size: 1,
        ..Config::default()
    });
    assert_conn_err("refused", ex.execute_statement("SELECT 1", &[]));
}

#[test]
fn response_timeout_is_not_retried_or_reused() {
    let srv = FakeServer::start(&[Act::Delay]);
    let ex = PdbWireExecutor::new(Config {
        host: "127.0.0.1".into(),
        port: srv.port,
        pool_size: 1,
        prefer_unix_socket: false,
        io_timeout: Duration::from_millis(75),
        ..Config::default()
    });
    let started = Instant::now();
    assert_conn_err("response timeout", ex.kv_set(b"k", b"first"));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "response deadline was not bounded"
    );
    assert_eq!(srv.requests(), 1, "the timed out write was resent");
    ex.kv_set(b"k", b"second")
        .expect("write on a fresh connection");
    assert_eq!(srv.accepts(), 2, "the timed out connection was reused");
    assert_eq!(srv.requests(), 2);
}
