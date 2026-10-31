//! PDB Native Wire Protocol executor - the fastest path to ParticleDB.
//!
//! Speaks the v1-framed native wire (the server's wire protocol): Arrow IPC
//! results, typed Arrow parameters, batched requests. Automatically uses the
//! Unix socket `/tmp/.s.PDB.<port>` on localhost, falling back to TCP.
//!
//! Per connection: a `MSG_HELLO` opting into `CAP_NUMERIC_TEXT` (an
//! unconstrained `numeric` arrives as exact text), then `MSG_AUTH` when the
//! server's `HELLO_OK` carries `CAP_AUTH_REQUIRED`.
//!
//! * no parameters -> `MSG_QUERY` (0x01);
//! * parameters -> `MSG_PREPARE` (0x05, cached per connection) then
//!   `MSG_EXECUTE` (0x06) with ONE Arrow parameter row. Parameters are bound by
//!   the server into the parsed statement by index, never spliced into SQL;
//! * [`SqlExecutor::execute_many`] -> `MSG_EXECUTE_BATCH` (0x07): M Arrow
//!   parameter rows per frame, chunked under the server's 16 MiB frame cap.

use crate::arrow_codec;
use crate::client::{Config, SqlExecutor};
use crate::error::{BatchFailure, Error, Result};
use crate::types::{QueryResult, Value};

use std::collections::{HashMap, VecDeque};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

// Protocol constants (must match the server's wire protocol)
const HEADER_SIZE: usize = 8;
const MSG_HELLO: u8 = 0x00;
const MSG_QUERY: u8 = 0x01;
const MSG_BATCH: u8 = 0x02;
const MSG_PING: u8 = 0x03;
const MSG_CLOSE: u8 = 0x04;
const MSG_PREPARE: u8 = 0x05;
const MSG_EXECUTE: u8 = 0x06;
const MSG_EXECUTE_BATCH: u8 = 0x07;
const MSG_CLOSE_STMT: u8 = 0x08;
const MSG_AUTH: u8 = 0x09;
const MSG_KV_SET: u8 = 0x10;
const MSG_KV_GET: u8 = 0x11;
const MSG_KV_DEL: u8 = 0x12;
const MSG_KV_INCR: u8 = 0x13;
const MSG_HELLO_OK: u8 = 0x80;
const MSG_ARROW_RESULT: u8 = 0x81;
const MSG_OK: u8 = 0x82;
const MSG_ERROR: u8 = 0x83;
const MSG_PONG: u8 = 0x84;
const MSG_BATCH_RESULT: u8 = 0x85;
const MSG_ARROW_STREAM: u8 = 0x86;
/// Frame flag: more `MSG_ARROW_STREAM` chunks follow this one.
const FLAG_MORE_CHUNKS: u8 = 0x02;
const MSG_KV_BULK: u8 = 0x87;
const MSG_KV_NIL: u8 = 0x88;
const MSG_PREPARE_OK: u8 = 0x89;

/// HELLO capability bits (the server's protocol-2 capability table).
const CAP_V1_LEGACY: u8 = 0x01;
const CAP_NUMERIC_TEXT: u8 = 0x10;
const CAP_AUTH_REQUIRED: u8 = 0x20;
const FMT_ARROW_IPC: u8 = 0x01;

/// Native error code: the prepared statement named by EXECUTE does not exist
/// on this connection (e.g. after `DISCARD ALL`).
const ERR_NO_SUCH_STMT: u16 = 26;

/// The server's frame-size cap (`MAX_PAYLOAD_SIZE`, 16 MiB). A bulk call's
/// Arrow parameter chunk is kept below it with headroom for the name prefix.
const MAX_FRAME_PAYLOAD: usize = 16 * 1024 * 1024;
const BULK_FRAME_BUDGET: usize = MAX_FRAME_PAYLOAD - 64 * 1024;
/// Rows per `MSG_EXECUTE_BATCH` frame before the size budget is checked.
const BULK_CHUNK_ROWS: usize = 10_000;
/// Prepared statements kept per connection (the server allows 4096).
const STMT_CACHE_CAP: usize = 256;

/// Default PDB wire port.
pub const DEFAULT_PORT: u16 = 7173;

/// Default pool size: min(available CPUs, 16).
pub fn default_pool_size() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().min(16))
        .unwrap_or(4)
}

/// The server's default `--unix-socket-dir`.
pub const DEFAULT_UNIX_SOCKET_DIR: &str = "/tmp";

/// The server's native-wire socket for `port` in the default directory:
/// `/tmp/.s.PDB.<port>`.
pub fn unix_socket_path(port: u16) -> String {
    unix_socket_path_in(DEFAULT_UNIX_SOCKET_DIR, port)
}

/// The server's native-wire socket for `port` under `dir` (its
/// `--unix-socket-dir`): `<dir>/.s.PDB.<port>`.
pub fn unix_socket_path_in(dir: &str, port: u16) -> String {
    std::path::Path::new(dir)
        .join(format!(".s.PDB.{port}"))
        .to_string_lossy()
        .into_owned()
}

/// `localhost` or a loopback IP literal (`127.0.0.0/8`, `::1`). No DNS
/// lookup: only these get the Unix socket first.
pub fn is_local_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

// ── SQLSTATE of a native error frame ───────────────────────────────────────

/// Letter-bearing SQLSTATEs carried in the reserved `60000+` band. Must equal
/// the server's `SQLSTATE_WIRE_BAND` (its wire protocol), the same
/// table the JDBC driver inverts (`PdbSqlState.BAND`). APPEND-ONLY.
const SQLSTATE_BAND: &[(u16, &str)] = &[
    (60000, "XX000"),
    (60001, "42P01"),
    (60002, "42P02"),
    (60003, "25P02"),
    (60004, "25P01"),
    (60005, "40P01"),
    (60006, "55P03"),
    (60007, "0A000"),
    (60008, "22P02"),
    (60009, "3B001"),
    (60010, "28P01"),
    (60011, "22P04"),
    (60012, "2201B"),
    (60013, "2201E"),
    (60014, "2201F"),
    (60015, "2201G"),
    (60016, "2202E"),
    (60017, "2200H"),
    (60018, "22P05"),
    (60019, "2BP01"),
    (60020, "3D000"),
    (60021, "3F000"),
    (60022, "42P05"),
    (60023, "42P06"),
    (60024, "42P07"),
    (60025, "42P10"),
    (60026, "42P16"),
    (60027, "42P17"),
    (60028, "42P18"),
    (60029, "42P19"),
    (60030, "42P20"),
    (60031, "42P21"),
    (60032, "428C9"),
    (60033, "55P02"),
    (60034, "57P01"),
    (60035, "57P03"),
    (60036, "08P01"),
    (60037, "P0001"),
];

/// The SQLSTATE a native error code stands for (the JDBC driver's
/// `PdbSqlState.forWireCode`): the band, the protocol-level codes, an
/// all-digit SQLSTATE sent as its number (`23505`, `8007` -> `08007`), else
/// `HY000`.
pub fn sqlstate_for_code(code: u16) -> String {
    if code >= 60000 {
        return SQLSTATE_BAND
            .iter()
            .find(|(c, _)| *c == code)
            .map(|(_, s)| s.to_string())
            .unwrap_or_else(|| "HY000".into());
    }
    let proto = match code {
        0x00 => Some("08006"),
        0x01..=0x06 => Some("08P01"),
        0x07 => Some("57014"),
        0x08 => Some("08006"),
        0x10 => Some("08004"),
        0x11 => Some("53300"),
        0x12 => Some("08004"),
        0x13 => Some("28000"),
        0x14 => Some("28P01"),
        26 => Some("26000"),
        1064 => Some("HY000"),
        _ => None,
    };
    if let Some(s) = proto {
        return s.into();
    }
    if code >= 10000 {
        code.to_string()
    } else if code >= 1000 {
        format!("0{code}")
    } else {
        "HY000".into()
    }
}

/// A trailing ` [XXXXX]` SQLSTATE marker in a server message.
fn trailing_marker(msg: &str) -> Option<&str> {
    let t = msg.trim_end();
    let b = t.as_bytes();
    if b.len() >= 8
        && b[b.len() - 1] == b']'
        && b[b.len() - 7] == b'['
        && b[b.len() - 8] == b' '
        && b[b.len() - 6..b.len() - 1]
            .iter()
            .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
    {
        Some(&t[t.len() - 6..t.len() - 1])
    } else {
        None
    }
}

/// A top-level `MSG_ERROR` frame as `Error::Server`. The frame code decides
/// the SQLSTATE; a trailing marker is used only when the code names no class
/// (`HY000`) -- the server already folds a marker into the code whenever the
/// code can carry it.
fn server_error(code: u16, message: String) -> Error {
    let mut sqlstate = sqlstate_for_code(code);
    if sqlstate == "HY000" {
        if let Some(m) = trailing_marker(&message) {
            sqlstate = m.to_string();
        }
    }
    Error::Server {
        sqlstate,
        code: Some(code),
        message,
    }
}

/// A `MSG_BATCH_RESULT` error entry (message only, no code): the SQLSTATE is
/// the message's trailing marker, else `HY000`.
fn entry_sqlstate(message: &str) -> String {
    trailing_marker(message).unwrap_or("HY000").to_string()
}

// ── Low-level connection ───────────────────────────────────────────────────

#[derive(Clone)]
struct TlsIo(Arc<Mutex<native_tls::TlsStream<TcpStream>>>);
impl Read for TlsIo {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("TLS lock poisoned"))?
            .read(b)
    }
}
impl Write for TlsIo {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("TLS lock poisoned"))?
            .write(b)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("TLS lock poisoned"))?
            .flush()
    }
}

struct Permit(Arc<(Mutex<usize>, Condvar)>);
impl Drop for Permit {
    fn drop(&mut self) {
        if let Ok(mut n) = self.0 .0.lock() {
            *n -= 1;
            self.0 .1.notify_all();
        }
    }
}

struct PdbConn {
    reader: BufReader<Box<dyn Read + Send>>,
    writer: BufWriter<Box<dyn Write + Send>>,
    /// SQL text -> (server-side statement name, parameter count).
    stmts: HashMap<String, (String, u32)>,
    /// Insertion order of `stmts`, for eviction.
    stmt_order: VecDeque<String>,
    next_stmt: u64,
    permit: Option<Permit>,
    last_flags: u8,
}

impl PdbConn {
    fn connect(config: &Config) -> Result<Self> {
        config.validate()?;
        let mut tcp = None;
        let (r, w): (Box<dyn Read + Send>, Box<dyn Write + Send>) = match Self::connect_unix(config)
        {
            Some(rw) => rw,
            None => {
                let stream = Self::connect_tcp(config)?;
                tcp = Some(stream.try_clone().map_err(io_err)?);
                (
                    Box::new(stream.try_clone().map_err(io_err)?),
                    Box::new(stream),
                )
            }
        };

        let mut conn = PdbConn {
            reader: BufReader::with_capacity(131072, r),
            writer: BufWriter::with_capacity(131072, w),
            stmts: HashMap::new(),
            stmt_order: VecDeque::new(),
            next_stmt: 0,
            permit: None,
            last_flags: 0,
        };
        conn.handshake(config, tcp)?;
        Ok(conn)
    }

    /// The server's socket for a local host, when preferred and the file
    /// exists. `None` = use TCP: no socket file, the opt-out, a remote host,
    /// or a socket that does not answer (a stale file).
    #[cfg(unix)]
    fn connect_unix(config: &Config) -> Option<(Box<dyn Read + Send>, Box<dyn Write + Send>)> {
        if config.tls.is_some() || !config.prefer_unix_socket || !is_local_host(&config.host) {
            return None;
        }
        let path = unix_socket_path_in(&config.unix_socket_dir, config.port);
        if !std::path::Path::new(&path).exists() {
            return None;
        }
        let s = UnixStream::connect(&path).ok()?;
        s.set_read_timeout(Some(config.io_timeout)).ok()?;
        s.set_write_timeout(Some(config.io_timeout)).ok()?;
        let r = s.try_clone().ok()?;
        Some((Box::new(r), Box::new(s)))
    }

    #[cfg(not(unix))]
    fn connect_unix(_config: &Config) -> Option<(Box<dyn Read + Send>, Box<dyn Write + Send>)> {
        None
    }

    fn connect_tcp(config: &Config) -> Result<TcpStream> {
        let host = config.host.trim_start_matches('[').trim_end_matches(']');
        let deadline = Instant::now() + config.connect_timeout;
        let addresses = (host, config.port).to_socket_addrs().map_err(io_err)?;
        let mut last = "no addresses for host".to_string();
        for address in addresses {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match TcpStream::connect_timeout(&address, remaining) {
                Ok(stream) => {
                    stream.set_nodelay(true).map_err(io_err)?;
                    stream
                        .set_read_timeout(Some(config.io_timeout))
                        .map_err(io_err)?;
                    stream
                        .set_write_timeout(Some(config.io_timeout))
                        .map_err(io_err)?;
                    return Ok(stream);
                }
                Err(error) => last = error.to_string(),
            }
        }
        Err(Error::Connection(last))
    }

    /// HELLO (v1 framing, `CAP_NUMERIC_TEXT`), then AUTH when challenged. No
    /// statement has been sent yet, so any failure here is unambiguous.
    fn handshake(&mut self, config: &Config, tcp: Option<TcpStream>) -> Result<()> {
        // [u8 max_ver=1][u8 ncaps=1][caps][u8 nfmts=1][FMT_ARROW_IPC]
        let hello = [
            1u8,
            1,
            CAP_V1_LEGACY | CAP_NUMERIC_TEXT | if config.tls.is_some() { 0x08 } else { 0 },
            1,
            FMT_ARROW_IPC,
        ];
        self.write_frame(MSG_HELLO, &hello)?;
        let (t, p) = self.read_frame()?;
        if t != MSG_HELLO_OK {
            return Err(Error::Connection(format!(
                "HELLO answered with msg 0x{t:02X}"
            )));
        }
        // v2-shaped HELLO_OK: [ver][chosen_caps][fmt][u32][u32]; the legacy
        // two-byte shape carries no caps.
        let caps = if p.len() >= 11 { p[1] } else { 0 };
        if let Some(options) = &config.tls {
            if caps & 0x08 == 0 {
                return Err(Error::Connection(
                    "server did not accept TLS; refusing plaintext".into(),
                ));
            }
            let mut builder = native_tls::TlsConnector::builder();
            if let Some(path) = &options.root_cert {
                let pem = std::fs::read(path).map_err(io_err)?;
                let cert = native_tls::Certificate::from_pem(&pem)
                    .map_err(|e| Error::Connection(e.to_string()))?;
                builder.add_root_certificate(cert);
            }
            let connector = builder
                .build()
                .map_err(|e| Error::Connection(e.to_string()))?;
            let host = config.host.trim_start_matches('[').trim_end_matches(']');
            let stream = tcp.ok_or_else(|| Error::Connection("TLS requires TCP".into()))?;
            let secure = connector
                .connect(host, stream)
                .map_err(|e| Error::Connection(e.to_string()))?;
            let io = TlsIo(Arc::new(Mutex::new(secure)));
            self.reader = BufReader::new(Box::new(io.clone()));
            self.writer = BufWriter::new(Box::new(io));
        }
        if caps & CAP_AUTH_REQUIRED != 0 {
            let (u, pw) = (config.user.as_bytes(), config.password.as_bytes());
            let mut payload = Vec::with_capacity(8 + u.len() + pw.len());
            payload.extend_from_slice(&(u.len() as u32).to_le_bytes());
            payload.extend_from_slice(u);
            payload.extend_from_slice(&(pw.len() as u32).to_le_bytes());
            payload.extend_from_slice(pw);
            self.write_frame(MSG_AUTH, &payload)?;
            let (t, _) = self.read_frame()?;
            if t != MSG_OK {
                return Err(Error::Connection(format!(
                    "AUTH answered with msg 0x{t:02X}"
                )));
            }
        }
        Ok(())
    }

    fn send_query(&mut self, sql: &str) -> Result<(u8, Vec<u8>)> {
        self.write_frame(MSG_QUERY, sql.as_bytes())?;
        self.read_response()
    }

    /// Read one response, reassembling a streamed Arrow result. A large result
    /// arrives as several `MSG_ARROW_STREAM` frames; reading only the first
    /// would leave the rest on the socket, and the next request on this pooled
    /// connection would read a stale chunk as its own answer.
    fn read_response(&mut self) -> Result<(u8, Vec<u8>)> {
        let (msg_type, flags, mut data) = self.read_frame_flags()?;
        if msg_type == MSG_ARROW_STREAM {
            let mut more = flags & FLAG_MORE_CHUNKS != 0;
            while more {
                let (t, f, chunk) = self.read_frame_flags()?;
                if t != MSG_ARROW_STREAM {
                    return Err(Error::Connection(format!(
                        "ARROW_STREAM interrupted by msg 0x{:02X}",
                        t
                    )));
                }
                data.extend_from_slice(&chunk);
                more = f & FLAG_MORE_CHUNKS != 0;
            }
        }
        Ok((msg_type, data))
    }

    fn send_batch(&mut self, sqls: &[&str]) -> Result<(u8, Vec<u8>)> {
        let mut payload = Vec::with_capacity(4 + sqls.iter().map(|s| 4 + s.len()).sum::<usize>());
        payload.extend_from_slice(&(sqls.len() as u32).to_le_bytes());
        for sql in sqls {
            payload.extend_from_slice(&(sql.len() as u32).to_le_bytes());
            payload.extend_from_slice(sql.as_bytes());
        }
        self.write_frame(MSG_BATCH, &payload)?;
        self.read_frame()
    }

    fn read_frame(&mut self) -> Result<(u8, Vec<u8>)> {
        let (msg_type, _flags, data) = self.read_frame_flags()?;
        Ok((msg_type, data))
    }

    /// Read one frame. A top-level `MSG_ERROR` is ALWAYS an `Err`, whatever its
    /// length: a payload too short to hold the u16 code is still a failure,
    /// and returning it as an ordinary frame let callers report it as success.
    fn read_frame_flags(&mut self) -> Result<(u8, u8, Vec<u8>)> {
        let mut hdr = [0u8; HEADER_SIZE];
        self.reader.read_exact(&mut hdr).map_err(io_err)?;
        let plen = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
        let msg_type = hdr[6];
        let flags = hdr[7];
        if plen > MAX_FRAME_PAYLOAD {
            return Err(Error::Connection(format!(
                "response frame exceeds {MAX_FRAME_PAYLOAD} bytes"
            )));
        }
        self.last_flags = flags;
        let mut data = vec![0u8; plen];
        if plen > 0 {
            self.reader.read_exact(&mut data).map_err(io_err)?;
        }

        if msg_type == MSG_ERROR {
            return Err(if data.len() >= 2 {
                let code = u16::from_le_bytes([data[0], data[1]]);
                server_error(code, String::from_utf8_lossy(&data[2..]).to_string())
            } else {
                Error::Server {
                    sqlstate: "HY000".into(),
                    code: None,
                    message: "server error (no message)".into(),
                }
            });
        }

        Ok((msg_type, flags, data))
    }

    fn ping(&mut self) -> Result<()> {
        self.write_frame(MSG_PING, &[])?;
        let (msg_type, _) = self.read_frame()?;
        if msg_type == MSG_PONG {
            Ok(())
        } else {
            Err(Error::Connection("bad pong".into()))
        }
    }

    fn close(&mut self) {
        let _ = self.writer.write_all(&[0, 0, 0, 0, 0, 0, MSG_CLOSE, 0]);
        let _ = self.writer.flush();
    }

    fn write_frame(&mut self, msg_type: u8, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_FRAME_PAYLOAD {
            // Refused locally: the server would drop the connection unanswered.
            return Err(Error::Query(format!(
                "request of {} bytes exceeds the native wire's {} byte frame limit",
                payload.len(),
                MAX_FRAME_PAYLOAD
            )));
        }
        let len = payload.len() as u32;
        self.writer.write_all(&len.to_le_bytes()).map_err(io_err)?;
        self.writer
            .write_all(&[0, 0, msg_type, 0])
            .map_err(io_err)?;
        if !payload.is_empty() {
            self.writer.write_all(payload).map_err(io_err)?;
        }
        self.writer.flush().map_err(io_err)
    }

    // ── Prepared statements ────────────────────────────────────────────

    /// `MSG_PREPARE` `sql` under a fresh name (or reuse this connection's
    /// cached one). Returns (name, n_params).
    fn prepared(&mut self, sql: &str) -> Result<(String, u32)> {
        if let Some(e) = self.stmts.get(sql) {
            return Ok(e.clone());
        }
        if self.stmts.len() >= STMT_CACHE_CAP {
            if let Some(old_sql) = self.stmt_order.pop_front() {
                if let Some((old, _)) = self.stmts.remove(&old_sql) {
                    self.close_stmt(&old)?;
                }
            }
        }
        self.next_stmt += 1;
        let name = format!("rs{}", self.next_stmt);
        let mut payload = Vec::with_capacity(8 + name.len() + sql.len());
        payload.extend_from_slice(&(name.len() as u32).to_le_bytes());
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(&(sql.len() as u32).to_le_bytes());
        payload.extend_from_slice(sql.as_bytes());
        self.write_frame(MSG_PREPARE, &payload)?;
        let (t, data) = self.read_frame()?;
        if t != MSG_PREPARE_OK || data.len() < 2 {
            return Err(Error::Connection(format!(
                "PREPARE answered with msg 0x{t:02X} ({} bytes)",
                data.len()
            )));
        }
        let n = u16::from_le_bytes([data[0], data[1]]) as u32;
        self.stmts.insert(sql.to_string(), (name.clone(), n));
        self.stmt_order.push_back(sql.to_string());
        Ok((name, n))
    }

    fn forget_stmt(&mut self, sql: &str) {
        self.stmts.remove(sql);
        self.stmt_order.retain(|s| s != sql);
    }

    fn close_stmt(&mut self, name: &str) -> Result<()> {
        let mut payload = Vec::with_capacity(4 + name.len());
        payload.extend_from_slice(&(name.len() as u32).to_le_bytes());
        payload.extend_from_slice(name.as_bytes());
        self.write_frame(MSG_CLOSE_STMT, &payload)?;
        let (t, _) = self.read_frame()?;
        if t == MSG_OK {
            Ok(())
        } else {
            Err(Error::Connection(format!(
                "CLOSE_STMT answered with msg 0x{t:02X}"
            )))
        }
    }

    fn name_and_body(name: &str, body: &[u8]) -> Vec<u8> {
        let mut p = Vec::with_capacity(4 + name.len() + body.len());
        p.extend_from_slice(&(name.len() as u32).to_le_bytes());
        p.extend_from_slice(name.as_bytes());
        p.extend_from_slice(body);
        p
    }

    /// Run `f(conn, name, n_params)` against the prepared form of `sql`. If the
    /// server no longer knows the statement (`26`, e.g. after `DISCARD ALL`),
    /// it is re-prepared and `f` runs once more: the refusal happens before
    /// anything executes, so this is not a resend of an executed request.
    fn with_prepared<T>(
        &mut self,
        sql: &str,
        mut f: impl FnMut(&mut Self, &str, u32) -> Result<T>,
    ) -> Result<T> {
        let (name, n) = self.prepared(sql)?;
        match f(self, &name, n) {
            Err(Error::Server {
                code: Some(ERR_NO_SUCH_STMT),
                ..
            }) => {
                self.forget_stmt(sql);
                let (name, n) = self.prepared(sql)?;
                f(self, &name, n)
            }
            r => r,
        }
    }

    /// `MSG_EXECUTE` with one parameter row. Returns the response frame.
    fn execute_params(&mut self, sql: &str, params: &[Value]) -> Result<(u8, Vec<u8>)> {
        self.with_prepared(sql, |c, name, n| {
            check_param_count(params.len(), n)?;
            let body = arrow_codec::encode_params(&[params], n as usize)?;
            c.write_frame(MSG_EXECUTE, &Self::name_and_body(name, &body))?;
            c.read_response()
        })
    }

    /// `MSG_EXECUTE_BATCH` with `rows`, encoded with the column `kinds` the
    /// caller decided over the whole call. Returns the `MSG_BATCH_RESULT`
    /// payload.
    fn execute_batch_params(
        &mut self,
        sql: &str,
        rows: &[&[Value]],
        kinds: &[arrow_codec::Kind],
    ) -> Result<Vec<u8>> {
        self.with_prepared(sql, |c, name, n| {
            check_param_count(kinds.len(), n)?;
            let body = arrow_codec::encode_params_with(rows, kinds)?;
            if body.len() + 4 + name.len() > MAX_FRAME_PAYLOAD {
                return Err(Error::Query(format!(
                    "{} parameter rows encode to {} bytes, over the {} byte frame limit",
                    rows.len(),
                    body.len(),
                    MAX_FRAME_PAYLOAD
                )));
            }
            c.write_frame(MSG_EXECUTE_BATCH, &Self::name_and_body(name, &body))?;
            let (t, data) = c.read_frame()?;
            if t != MSG_BATCH_RESULT {
                return Err(Error::Connection(format!(
                    "EXECUTE_BATCH answered with msg 0x{t:02X}, expected BATCH_RESULT"
                )));
            }
            Ok(data)
        })
    }

    // ── KV ──────────────────────────────────────────────────────────────

    fn kv_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut payload = Vec::with_capacity(8 + key.len() + value.len());
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(value);
        self.write_frame(MSG_KV_SET, &payload)?;
        let (msg_type, _) = self.read_frame()?;
        if msg_type == MSG_OK {
            Ok(())
        } else {
            Err(Error::Query(format!(
                "KV_SET: unexpected msg 0x{:02X}",
                msg_type
            )))
        }
    }

    fn kv_get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut payload = Vec::with_capacity(4 + key.len());
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        self.write_frame(MSG_KV_GET, &payload)?;
        let (msg_type, data) = self.read_frame()?;
        match msg_type {
            MSG_KV_BULK => {
                if data.len() < 4 {
                    return Err(Error::Query("KV_GET: short bulk".into()));
                }
                let vlen = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if data.len() < 4 + vlen {
                    return Err(Error::Query("KV_GET: bulk len mismatch".into()));
                }
                Ok(Some(data[4..4 + vlen].to_vec()))
            }
            MSG_KV_NIL => Ok(None),
            _ => Err(Error::Query(format!(
                "KV_GET: unexpected msg 0x{:02X}",
                msg_type
            ))),
        }
    }

    fn kv_del(&mut self, key: &[u8]) -> Result<()> {
        let mut payload = Vec::with_capacity(4 + key.len());
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        self.write_frame(MSG_KV_DEL, &payload)?;
        let (msg_type, _) = self.read_frame()?;
        if msg_type == MSG_OK {
            Ok(())
        } else {
            Err(Error::Query(format!(
                "KV_DEL: unexpected msg 0x{:02X}",
                msg_type
            )))
        }
    }

    fn kv_incr(&mut self, key: &[u8]) -> Result<i64> {
        let mut payload = Vec::with_capacity(4 + key.len());
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        self.write_frame(MSG_KV_INCR, &payload)?;
        let (msg_type, data) = self.read_frame()?;
        if msg_type != MSG_KV_BULK {
            return Err(Error::Query(format!(
                "KV_INCR: unexpected msg 0x{:02X}",
                msg_type
            )));
        }
        if data.len() < 4 {
            return Err(Error::Query("KV_INCR: short bulk".into()));
        }
        let vlen = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + vlen {
            return Err(Error::Query("KV_INCR: bulk len mismatch".into()));
        }
        std::str::from_utf8(&data[4..4 + vlen])
            .map_err(|e| Error::Query(format!("KV_INCR: bad utf8: {}", e)))?
            .parse::<i64>()
            .map_err(|e| Error::Query(format!("KV_INCR: bad int: {}", e)))
    }
}

impl Drop for PdbConn {
    fn drop(&mut self) {
        self.close();
    }
}

fn io_err(e: std::io::Error) -> Error {
    Error::Connection(e.to_string())
}

fn check_param_count(got: usize, want: u32) -> Result<()> {
    if got == want as usize {
        Ok(())
    } else {
        Err(Error::Query(format!(
            "statement takes {want} parameter(s) but {got} were supplied"
        )))
    }
}

/// One decoded `MSG_BATCH_RESULT` entry.
#[derive(Debug, PartialEq)]
enum Entry<'a> {
    /// Affected rows.
    Ok(u64),
    /// A row-returning result (Arrow IPC).
    Rows(&'a [u8]),
    /// A failed statement / row: the server's message (no code).
    Failed(&'a [u8]),
}

/// Decode a `MSG_BATCH_RESULT` payload into its entries.
///
/// Payload: `[u32 count]` then `count` entries, each `[u8 tag][body]`:
/// `MSG_OK` = u64 affected, `MSG_ARROW_RESULT` = u32 len + Arrow IPC,
/// `MSG_ERROR` = u32 len + UTF-8 message (an entry carries NO u16 code, unlike
/// a top-level `MSG_ERROR` frame). Truncation or an unknown tag is a protocol
/// error, never a success.
fn decode_entries(data: &[u8]) -> Result<Vec<Entry<'_>>> {
    let proto = |m: String| Error::Query(format!("BATCH_RESULT protocol error: {}", m));
    let u32_at = |pos: usize| -> Option<usize> {
        data.get(pos..pos + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
    };
    let count =
        u32_at(0).ok_or_else(|| proto(format!("payload too short ({} bytes)", data.len())))?;
    let mut pos = 4;
    let mut out = Vec::with_capacity(count.min(1 << 20));
    for idx in 0..count {
        let tag = *data
            .get(pos)
            .ok_or_else(|| proto(format!("entry {} of {} has no tag", idx, count)))?;
        pos += 1;
        match tag {
            MSG_OK => {
                let b = data
                    .get(pos..pos + 8)
                    .ok_or_else(|| proto(format!("truncated OK entry {}", idx)))?;
                out.push(Entry::Ok(u64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ])));
                pos += 8;
            }
            MSG_ARROW_RESULT | MSG_ERROR => {
                let len = u32_at(pos).ok_or_else(|| proto(format!("truncated entry {}", idx)))?;
                pos += 4;
                let body = data
                    .get(pos..pos + len)
                    .ok_or_else(|| proto(format!("truncated entry body {}", idx)))?;
                pos += len;
                out.push(if tag == MSG_ERROR {
                    Entry::Failed(body)
                } else {
                    Entry::Rows(body)
                });
            }
            other => return Err(proto(format!("entry {}: unknown tag 0x{:02X}", idx, other))),
        }
    }
    if pos != data.len() {
        return Err(proto(format!(
            "{} trailing bytes after {} entries",
            data.len() - pos,
            count
        )));
    }
    Ok(out)
}

/// Decode a `MSG_BATCH_RESULT` for a batch of `sent` SQL statements and
/// return the affected-row count summed over its `MSG_OK` entries.
///
/// The server stops at the first failing statement, so an error entry means
/// the rest -- including any trailing COMMIT -- never ran: it is returned as
/// `Error::Server` (SQLSTATE from the message's marker, else `HY000`). A count
/// that differs from `sent` with no error entry is a protocol error.
fn decode_batch_result(data: &[u8], sent: usize) -> Result<u64> {
    let entries = decode_entries(data)?;
    let mut total = 0u64;
    for (idx, e) in entries.iter().enumerate() {
        match e {
            Entry::Ok(n) => total += n,
            Entry::Rows(_) => {}
            Entry::Failed(msg) => {
                let msg = String::from_utf8_lossy(msg).to_string();
                return Err(Error::Server {
                    sqlstate: entry_sqlstate(&msg),
                    code: None,
                    message: format!("batch statement {} failed: {}", idx, msg),
                });
            }
        }
    }
    if entries.len() != sent {
        return Err(Error::Query(format!(
            "BATCH_RESULT protocol error: {} entries for {} statements and no error entry",
            entries.len(),
            sent
        )));
    }
    Ok(total)
}

/// Fold one `MSG_EXECUTE_BATCH` chunk's entries into `acc`. `base` is the
/// chunk's first row index in the caller's slice. `stops_at_failure` is the
/// in-transaction rule (the server stops and poisons the block): fewer entries
/// than rows is then legal after a failure; in autocommit every row answers.
fn fold_chunk(
    data: &[u8],
    base: usize,
    rows: usize,
    stops_at_failure: bool,
    acc: &mut BatchFailure,
) -> Result<bool> {
    let entries = decode_entries(data)?;
    let mut failed_here = false;
    for (k, e) in entries.iter().enumerate() {
        match e {
            Entry::Ok(n) => acc.applied += n,
            Entry::Rows(_) => {}
            Entry::Failed(msg) => {
                failed_here = true;
                acc.failed += 1;
                if acc.errors.len() < BatchFailure::MAX_RECORDED {
                    let m = String::from_utf8_lossy(msg).to_string();
                    acc.errors.push((base + k, entry_sqlstate(&m), m));
                }
            }
        }
    }
    let complete = entries.len() == rows;
    let short_after_failure = stops_at_failure && failed_here && entries.len() < rows;
    if !complete && !short_after_failure {
        return Err(Error::Query(format!(
            "BATCH_RESULT protocol error: {} entries for {} parameter rows",
            entries.len(),
            rows
        )));
    }
    if short_after_failure {
        acc.not_attempted += rows - entries.len();
    }
    Ok(failed_here)
}

// ── Connection Pool ────────────────────────────────────────────────────────

struct Pool {
    config: Config,
    conns: Mutex<Vec<PdbConn>>,
    max_size: usize,
    permits: Arc<(Mutex<usize>, Condvar)>,
}

impl Pool {
    fn new(config: Config) -> Self {
        let max_size = config.pool_size;
        Pool {
            config,
            conns: Mutex::new(Vec::with_capacity(max_size)),
            max_size,
            permits: Arc::new((Mutex::new(0), Condvar::new())),
        }
    }

    fn get(&self) -> Result<PdbConn> {
        self.config.validate()?;
        let deadline = Instant::now() + self.config.connect_timeout;
        let mut total = self
            .permits
            .0
            .lock()
            .map_err(|_| Error::Connection("pool lock poisoned".into()))?;
        loop {
            if let Some(conn) = self
                .conns
                .lock()
                .map_err(|_| Error::Connection("pool lock poisoned".into()))?
                .pop()
            {
                return Ok(conn);
            }
            if *total < self.max_size {
                *total += 1;
                let permit = Permit(self.permits.clone());
                drop(total);
                let mut conn = PdbConn::connect(&self.config)?;
                conn.permit = Some(permit);
                return Ok(conn);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Connection("pool wait timed out".into()));
            }
            total = self
                .permits
                .1
                .wait_timeout(total, remaining)
                .map_err(|_| Error::Connection("pool lock poisoned".into()))?
                .0;
        }
    }

    /// Return `conn` to the pool only when its request succeeded; otherwise
    /// drop (close) it. After a transport error or timeout the request may
    /// have reached the server and its reply may still arrive on this socket,
    /// and after any other failure the framing (or the connection's
    /// transaction state) is in doubt, so a failed connection is never reused.
    /// The error is returned to the caller as is -- never retried.
    fn put_if_ok<T>(&self, conn: PdbConn, r: Result<T>) -> Result<T> {
        if r.is_ok() {
            self.put(conn);
        }
        r
    }

    fn put(&self, conn: PdbConn) {
        if let Ok(_total) = self.permits.0.lock() {
            if let Ok(mut idle) = self.conns.lock() {
                idle.push(conn);
            }
            self.permits.1.notify_one();
        }
    }
}

// ── PdbWireExecutor ────────────────────────────────────────────────────────

/// SQL executor using PDB's native wire protocol.
///
/// Automatically pools connections and uses Unix sockets on localhost.
///
/// # Parameters
///
/// `$1, $2, ...` placeholders. With a non-empty parameter slice the statement
/// is prepared once per pooled connection (`MSG_PREPARE`) and executed with a
/// typed Arrow parameter row (`MSG_EXECUTE`); the server binds each value into
/// the parsed statement, so no value is ever spliced into SQL text.
///
/// # Failures are never retried
///
/// The executor never resends a request. A connect failure is returned as
/// `Error::Connection` before anything was sent. Once a request has started to
/// be written, a socket error, EOF or timeout is AMBIGUOUS -- the server may
/// have executed and committed it -- so it is returned as `Error::Connection`
/// and the connection is closed, never pooled (it may still deliver the late
/// reply, or be inside a transaction). Whether to repeat the operation is the
/// caller's decision. The one re-send is of an EXECUTE the server refused
/// because the prepared statement was gone (code 26): nothing ran.
///
/// # Example
/// ```no_run
/// use particledb_sdk::{ParticleDB, Config, Value, pdb_wire_executor::PdbWireExecutor};
///
/// let config = Config { pool_size: 8, ..Config::default() }; // port 7173
/// let executor = PdbWireExecutor::new(config.clone());
/// let db = ParticleDB::with_executor(config, executor);
/// let result = db.query("SELECT * FROM users WHERE id = $1", &[Value::Int64(7)]).unwrap();
/// ```
pub struct PdbWireExecutor {
    pool: Arc<Pool>,
}

impl PdbWireExecutor {
    pub fn new(config: Config) -> Self {
        Self {
            pool: Arc::new(Pool::new(config)),
        }
    }

    /// Execute a callback on one connection. A panic drops the connection and rolls back on disconnect.
    pub fn transaction<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut conn = self.pool.get()?;
        conn.send_query("BEGIN")?;
        let result = f(&Transaction {
            conn: std::cell::RefCell::new(&mut conn),
        });
        match result {
            Ok(value) => {
                conn.send_query("COMMIT")?;
                if conn.last_flags & 0x40 != 0 {
                    return Err(Error::Query(
                        "COMMIT rolled back a failed transaction".into(),
                    ));
                }
                self.pool.put(conn);
                Ok(value)
            }
            Err(error) => {
                if conn.send_query("ROLLBACK").is_ok() {
                    self.pool.put(conn);
                }
                Err(error)
            }
        }
    }

    /// Execute a batch of SQL statements in one round-trip.
    /// Returns the number of affected rows summed over the batch's statements.
    ///
    /// Every entry of the batch result is decoded. If any statement failed the
    /// server's message is returned as `Error::Server`; the server stops at the
    /// first failure, so later statements (including a trailing `COMMIT`) did
    /// not run. A failed batch's connection is closed rather than returned to
    /// the pool: it may still be inside the failed transaction, where every
    /// later statement on it would be refused.
    pub fn execute_batch(&self, sqls: &[&str]) -> Result<u64> {
        if sqls.is_empty() {
            return Ok(0);
        }
        let mut conn = self.pool.get()?;
        // On Err the connection is dropped (closed), never pooled.
        let (msg_type, data) = conn.send_batch(sqls)?;
        if msg_type != MSG_BATCH_RESULT {
            return Err(Error::Query(format!(
                "BATCH answered with msg 0x{:02X}, expected BATCH_RESULT 0x{:02X}",
                msg_type, MSG_BATCH_RESULT
            )));
        }
        let affected = decode_batch_result(&data, sqls.len())?;
        self.pool.put(conn);
        Ok(affected)
    }

    /// Run `sql` once per parameter row inside ONE transaction, all or
    /// nothing: `BEGIN`, `MSG_EXECUTE_BATCH` chunks, `COMMIT`. The first failed
    /// row stops the call; the transaction is rolled back and
    /// `Error::Batch` reports the failure (`applied` counts work that was
    /// rolled back, `not_attempted` the rows never run). Returns the summed
    /// affected-row count on success.
    ///
    /// Must not be called while this executor's connection is inside a
    /// transaction the caller opened with `BEGIN`.
    pub fn execute_many_in_transaction(&self, sql: &str, rows: &[Vec<Value>]) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        // Decided over every row before anything is sent.
        let kinds = arrow_codec::column_kinds(rows.iter().map(|r| r.as_slice()), rows[0].len())?;
        let mut conn = self.pool.get()?;
        let (t, _) = conn.send_query("BEGIN")?;
        if t != MSG_OK {
            return Err(Error::Query(format!("BEGIN answered with msg 0x{t:02X}")));
        }
        let mut acc = BatchFailure {
            rows: rows.len(),
            applied: 0,
            errors: Vec::new(),
            failed: 0,
            not_attempted: 0,
        };
        let mut start = 0;
        for chunk in chunk_rows(rows)? {
            let refs: Vec<&[Value]> = chunk.iter().map(|r| r.as_slice()).collect();
            let data = match conn.execute_batch_params(sql, &refs, &kinds) {
                Ok(d) => d,
                Err(e) => {
                    // A server refusal of the whole frame: roll back and report.
                    // A transport error drops the connection (the server aborts
                    // the open transaction when it closes).
                    if let Error::Server {
                        sqlstate, message, ..
                    } = &e
                    {
                        let _ = conn.send_query("ROLLBACK");
                        acc.failed += 1;
                        acc.errors.push((start, sqlstate.clone(), message.clone()));
                        acc.not_attempted = rows.len() - start - 1;
                        return Err(Error::Batch(acc));
                    }
                    return Err(e);
                }
            };
            let failed = fold_chunk(&data, start, chunk.len(), true, &mut acc)?;
            start += chunk.len();
            if failed {
                acc.not_attempted += rows.len() - start;
                let (t, _) = conn.send_query("ROLLBACK")?;
                if t != MSG_OK {
                    return Err(Error::Query(format!(
                        "ROLLBACK answered with msg 0x{t:02X}"
                    )));
                }
                self.pool.put(conn);
                return Err(Error::Batch(acc));
            }
        }
        let (t, data) = conn.send_query("COMMIT")?;
        if t != MSG_OK || data.len() < 8 {
            return Err(Error::Query(format!("COMMIT answered with msg 0x{t:02X}")));
        }
        if conn.last_flags & 0x40 != 0 {
            return Err(Error::Query(
                "COMMIT rolled back a failed transaction".into(),
            ));
        }
        self.pool.put(conn);
        Ok(acc.applied)
    }

    /// Ping the server to verify the connection is alive.
    pub fn ping(&self) -> Result<()> {
        let mut conn = self.pool.get()?;
        let r = conn.ping();
        self.pool.put_if_ok(conn, r)
    }

    /// Native-wire KV set. Bypasses SQL parser/planner and writes directly into
    /// the engine's KV store. ~10× faster than `INSERT ... ON CONFLICT`.
    pub fn kv_set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut conn = self.pool.get()?;
        let r = conn.kv_set(key, value);
        self.pool.put_if_ok(conn, r)
    }

    /// Native-wire KV get. Returns `None` if the key does not exist.
    pub fn kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut conn = self.pool.get()?;
        let r = conn.kv_get(key);
        self.pool.put_if_ok(conn, r)
    }

    /// Native-wire KV delete (idempotent).
    pub fn kv_del(&self, key: &[u8]) -> Result<()> {
        let mut conn = self.pool.get()?;
        let r = conn.kv_del(key);
        self.pool.put_if_ok(conn, r)
    }

    /// Native-wire KV atomic increment. Initializes missing keys to 0 before incrementing.
    pub fn kv_incr(&self, key: &[u8]) -> Result<i64> {
        let mut conn = self.pool.get()?;
        let r = conn.kv_incr(key);
        self.pool.put_if_ok(conn, r)
    }

    /// One statement: `MSG_QUERY` without parameters, `MSG_PREPARE` +
    /// `MSG_EXECUTE` with them. Returns the result frame.
    fn run(&self, sql: &str, params: &[Value]) -> Result<(PdbConn, u8, Vec<u8>)> {
        let mut conn = self.pool.get()?;
        let (t, data) = if params.is_empty() {
            conn.send_query(sql)?
        } else {
            conn.execute_params(sql, params)?
        };
        Ok((conn, t, data))
    }
}

/// Split `rows` into `MSG_EXECUTE_BATCH` chunks: at most `BULK_CHUNK_ROWS`
/// rows, and a chunk whose rows are large is split further so each frame
/// stays under the server's frame cap (estimated from the values' sizes; the
/// exact encoded size is checked again before sending).
fn chunk_rows(rows: &[Vec<Value>]) -> Result<Vec<&[Vec<Value>]>> {
    fn est(v: &Value) -> usize {
        match v {
            Value::String(s) | Value::Json(s) | Value::Numeric(s) | Value::Uuid(s) => s.len() + 4,
            Value::Bytes(b) => b.len() + 4,
            Value::Vector(v) => v.len() * 12 + 4,
            Value::Array(a) => a.iter().map(est).sum::<usize>() + 8,
            _ => 32,
        }
    }
    let mut out = Vec::new();
    let mut start = 0;
    let mut bytes = 0usize;
    for (i, r) in rows.iter().enumerate() {
        let sz: usize = r.iter().map(est).sum::<usize>() + 8;
        if sz > BULK_FRAME_BUDGET {
            return Err(Error::Query(format!(
                "parameter row {i} is about {sz} bytes, over the native wire's frame limit"
            )));
        }
        if i > start && (i - start >= BULK_CHUNK_ROWS || bytes + sz > BULK_FRAME_BUDGET / 2) {
            out.push(&rows[start..i]);
            start = i;
            bytes = 0;
        }
        bytes += sz;
    }
    if start < rows.len() {
        out.push(&rows[start..]);
    }
    Ok(out)
}

impl SqlExecutor for PdbWireExecutor {
    fn execute_query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let (conn, msg_type, data) = self.run(sql, params)?;
        match msg_type {
            MSG_ARROW_RESULT | MSG_ARROW_STREAM => {
                let r = arrow_codec::decode_result(&data);
                // The whole frame was read: the framing is intact either way,
                // so the connection is reusable even if decoding failed.
                self.pool.put(conn);
                r
            }
            MSG_OK if data.len() >= 8 => {
                self.pool.put(conn);
                let n = u64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                    rows_affected: n,
                })
            }
            // An unrecognised answer is not a result; the connection's framing
            // can no longer be trusted, so it is dropped rather than pooled.
            other => Err(Error::Query(format!(
                "query answered with unexpected msg 0x{:02X} ({} bytes)",
                other,
                data.len()
            ))),
        }
    }

    fn execute_statement(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let (conn, msg_type, data) = self.run(sql, params)?;
        match msg_type {
            MSG_OK if data.len() >= 8 => {
                self.pool.put(conn);
                Ok(u64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]))
            }
            // A row-returning statement run through execute: no rows affected.
            MSG_ARROW_RESULT | MSG_ARROW_STREAM => {
                self.pool.put(conn);
                Ok(0)
            }
            // A short OK or any other frame is not a success; drop the
            // connection rather than pool one whose framing is in doubt.
            other => Err(Error::Query(format!(
                "statement answered with unexpected msg 0x{:02X} ({} bytes)",
                other,
                data.len()
            ))),
        }
    }

    /// `MSG_EXECUTE_BATCH`: every row is its own autocommit statement (unless
    /// the connection is inside a caller-opened transaction), and the server
    /// runs every row of a frame even after one fails. All chunks are sent;
    /// any failed row makes the call `Err(Error::Batch)` whose `applied`
    /// counts the rows that DID commit. For all-or-nothing use
    /// [`PdbWireExecutor::execute_many_in_transaction`].
    fn execute_many(&self, sql: &str, rows: &[Vec<Value>]) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        // Decided over every row before the first frame is sent, so no late
        // row can fail locally after earlier frames were applied.
        let kinds = arrow_codec::column_kinds(rows.iter().map(|r| r.as_slice()), rows[0].len())?;
        let chunks = chunk_rows(rows)?;
        let mut conn = self.pool.get()?;
        let mut acc = BatchFailure {
            rows: rows.len(),
            applied: 0,
            errors: Vec::new(),
            failed: 0,
            not_attempted: 0,
        };
        let mut start = 0;
        let mut conn_ok = true;
        for chunk in chunks {
            let refs: Vec<&[Value]> = chunk.iter().map(|r| r.as_slice()).collect();
            match conn.execute_batch_params(sql, &refs, &kinds) {
                Ok(data) => {
                    // In a caller-opened transaction the server stops at the
                    // first failure; accept that shape and stop too.
                    if fold_chunk(&data, start, chunk.len(), true, &mut acc)?
                        && acc.not_attempted > 0
                    {
                        acc.not_attempted += rows.len() - start - chunk.len();
                        conn_ok = false;
                        break;
                    }
                }
                // A refusal of the whole frame (prepare error, failed
                // transaction, bad parameters): nothing in it ran.
                Err(Error::Server {
                    sqlstate, message, ..
                }) => {
                    acc.failed += 1;
                    acc.errors.push((start, sqlstate, message));
                    acc.not_attempted += rows.len() - start - 1;
                    conn_ok = false;
                    break;
                }
                // A transport failure (`Connection`, ambiguous for this
                // chunk) or a local refusal before this chunk was sent: the
                // earlier chunks' rows stay applied, so say how far it got.
                Err(e) if start > 0 => {
                    let ctx = format!(
                        "after {start} of {} rows were sent ({} affected)",
                        rows.len(),
                        acc.applied
                    );
                    return Err(match e {
                        Error::Connection(m) => Error::Connection(format!("{m} ({ctx})")),
                        other => Error::Query(format!("{other} ({ctx})")),
                    });
                }
                Err(e) => return Err(e),
            }
            start += chunk.len();
        }
        if conn_ok {
            self.pool.put(conn);
        }
        if acc.failed > 0 {
            return Err(Error::Batch(acc));
        }
        Ok(acc.applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(n: u64) -> Vec<u8> {
        let mut v = vec![MSG_OK];
        v.extend_from_slice(&n.to_le_bytes());
        v
    }
    fn err(m: &str) -> Vec<u8> {
        let mut v = vec![MSG_ERROR];
        v.extend_from_slice(&(m.len() as u32).to_le_bytes());
        v.extend_from_slice(m.as_bytes());
        v
    }
    fn payload(entries: &[Vec<u8>]) -> Vec<u8> {
        let mut v = (entries.len() as u32).to_le_bytes().to_vec();
        for e in entries {
            v.extend_from_slice(e);
        }
        v
    }
    fn acc(rows: usize) -> BatchFailure {
        BatchFailure {
            rows,
            applied: 0,
            errors: vec![],
            failed: 0,
            not_attempted: 0,
        }
    }

    #[test]
    fn all_ok_sums_affected() {
        assert_eq!(
            decode_batch_result(&payload(&[ok(1), ok(2)]), 2).unwrap(),
            3
        );
    }

    #[test]
    fn error_entry_is_err_with_server_message() {
        let p = payload(&[ok(1), err("duplicate key value [23505]")]);
        let e = decode_batch_result(&p, 3).unwrap_err();
        assert!(e.to_string().contains("duplicate key value"), "{e}");
        assert_eq!(e.sqlstate(), Some("23505"));
    }

    #[test]
    fn short_count_without_error_is_protocol_error() {
        let e = decode_batch_result(&payload(&[ok(1)]), 2)
            .unwrap_err()
            .to_string();
        assert!(e.contains("protocol error"), "{e}");
    }

    #[test]
    fn truncated_and_unknown_tag_are_errors() {
        assert!(decode_batch_result(&[1, 0], 1).is_err());
        assert!(decode_batch_result(&[1, 0, 0, 0, MSG_OK, 1], 1).is_err());
        assert!(decode_batch_result(&payload(&[vec![0x7F]]), 1).is_err());
    }

    #[test]
    fn fold_chunk_autocommit_counts_every_row() {
        let mut a = acc(3);
        let failed =
            fold_chunk(&payload(&[ok(1), err("boom"), ok(1)]), 10, 3, true, &mut a).unwrap();
        assert!(failed);
        assert_eq!((a.applied, a.failed, a.not_attempted), (2, 1, 0));
        assert_eq!(a.errors[0].0, 11);
        assert_eq!(a.errors[0].1, "HY000");
    }

    #[test]
    fn fold_chunk_short_without_failure_is_protocol_error() {
        let mut a = acc(3);
        assert!(fold_chunk(&payload(&[ok(1), ok(1)]), 0, 3, true, &mut a).is_err());
    }

    #[test]
    fn fold_chunk_txn_stop_counts_not_attempted() {
        let mut a = acc(4);
        assert!(fold_chunk(&payload(&[ok(1), err("x")]), 0, 4, true, &mut a).unwrap());
        assert_eq!((a.applied, a.failed, a.not_attempted), (1, 1, 2));
    }

    #[test]
    fn sqlstate_codes_mirror_the_jdbc_inverse() {
        assert_eq!(sqlstate_for_code(23505), "23505");
        assert_eq!(sqlstate_for_code(8007), "08007");
        assert_eq!(sqlstate_for_code(60001), "42P01");
        assert_eq!(sqlstate_for_code(60999), "HY000");
        assert_eq!(sqlstate_for_code(1064), "HY000");
        assert_eq!(sqlstate_for_code(0x14), "28P01");
        assert_eq!(sqlstate_for_code(26), "26000");
        let e = server_error(1064, "bad thing [2201X]".into());
        assert_eq!(e.sqlstate(), Some("2201X"));
        let e = server_error(23505, "dup [40001]".into());
        assert_eq!(
            e.sqlstate(),
            Some("23505"),
            "an authoritative code is not relabelled"
        );
    }

    /// The band must equal the server's `SQLSTATE_WIRE_BAND`. Off by default:
    /// set `PDB_SERVER_WIRE_SRC` to the server source file that defines the
    /// table to compare against it.
    #[test]
    fn band_matches_the_server_table() {
        let Some(src) = std::env::var_os("PDB_SERVER_WIRE_SRC") else {
            println!("SKIP band check: PDB_SERVER_WIRE_SRC not set");
            return;
        };
        let text = std::fs::read_to_string(&src)
            .unwrap_or_else(|e| panic!("PDB_SERVER_WIRE_SRC={src:?}: {e}"));
        let start = text
            .find("pub const SQLSTATE_WIRE_BAND")
            .expect("band table");
        let end = start + text[start..].find("];").expect("band end");
        let mut server: Vec<(u16, String)> = Vec::new();
        for line in text[start..end].lines() {
            let l = line.trim();
            if let Some(rest) = l.strip_prefix("(\"") {
                let (state, tail) = rest.split_once('"').unwrap();
                let code: u16 = tail
                    .trim_start_matches(',')
                    .trim()
                    .split(')')
                    .next()
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                server.push((code, state.to_string()));
            }
        }
        let ours: Vec<(u16, String)> = SQLSTATE_BAND
            .iter()
            .map(|(c, s)| (*c, s.to_string()))
            .collect();
        assert!(!server.is_empty());
        assert_eq!(ours, server);
    }

    #[test]
    fn chunking_respects_row_cap() {
        let rows: Vec<Vec<Value>> = (0..25_001).map(|i| vec![Value::Int64(i)]).collect();
        let chunks = chunk_rows(&rows).unwrap();
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), 25_001);
        assert!(chunks.iter().all(|c| c.len() <= BULK_CHUNK_ROWS));
        let big: Vec<Vec<Value>> = (0..10)
            .map(|_| vec![Value::Bytes(vec![0; 3 * 1024 * 1024])])
            .collect();
        let chunks = chunk_rows(&big).unwrap();
        assert!(chunks.len() >= 4, "{} chunks", chunks.len());
    }
}

/// A callback-scoped transaction. Calls use the same physical connection.
pub struct Transaction<'a> {
    conn: std::cell::RefCell<&'a mut PdbConn>,
}
impl Transaction<'_> {
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let (kind, bytes) = {
            let mut conn = self.conn.borrow_mut();
            if params.is_empty() {
                conn.send_query(sql)?
            } else {
                conn.execute_params(sql, params)?
            }
        };
        if kind != MSG_ARROW_RESULT && kind != MSG_ARROW_STREAM {
            return Err(Error::Query("statement did not return rows".into()));
        }
        arrow_codec::decode_result(&bytes)
    }
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let (kind, bytes) = {
            let mut conn = self.conn.borrow_mut();
            if params.is_empty() {
                conn.send_query(sql)?
            } else {
                conn.execute_params(sql, params)?
            }
        };
        if kind != MSG_OK || bytes.len() < 8 {
            return Err(Error::Query(
                "statement did not return an affected row count".into(),
            ));
        }
        Ok(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
    }
}
