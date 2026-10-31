//! LIVE native-wire tests of `PdbWireExecutor` against a real ParticleDB.
//!
//! Runs only when `PDB_NATIVE_PORT` is set (host 127.0.0.1, a `--no-auth`
//! server); otherwise each test prints a `SKIP` line and returns. Each test
//! prints `NWL_PASS <name>` when it ran and passed, so a runner can prove the
//! tests executed rather than skipped. `tests/run_live.sh` starts a debug
//! server on a unique port and runs them.
//!
//! * `every_type_round_trips` -- one column per PDB type, written once with
//!   literal SQL (decode only) and once with typed parameters (encode +
//!   decode), read back and compared value by value, NULLs included.
//! * `parameters_bind_and_never_splice` -- `$n` binding, NULL, an injection
//!   string stored verbatim, count mismatch refused, re-prepare after
//!   `DISCARD ALL`.
//! * `execute_many_inserts_100k_rows` -- `MSG_EXECUTE_BATCH` bulk insert of
//!   100 000 rows in one transaction, then 12 000 autocommit rows (two
//!   frames); count / sum / NULL pattern / per-row checks prove every row
//!   landed with its own values.
//! * `execute_many_reports_failed_rows` -- autocommit semantics (other rows
//!   commit) and the all-or-nothing transactional variant (nothing commits).
//! * `errors_carry_sqlstate` -- server errors on the simple and the prepared
//!   path carry the right SQLSTATE.

use particledb_sdk::pdb_wire_executor::PdbWireExecutor;
use particledb_sdk::{Config, Error, QueryResult, SqlExecutor, Value};

fn exec() -> Option<PdbWireExecutor> {
    let port: u16 = match std::env::var("PDB_NATIVE_PORT") {
        Ok(p) => p.parse().expect("PDB_NATIVE_PORT must be a port number"),
        Err(_) => return None,
    };
    Some(PdbWireExecutor::new(Config {
        host: "127.0.0.1".into(),
        port,
        pool_size: 2,
        ..Config::default()
    }))
}

macro_rules! live {
    ($name:literal) => {
        match exec() {
            Some(e) => e,
            None => {
                println!("SKIP {}: PDB_NATIVE_PORT not set", $name);
                return;
            }
        }
    };
}

fn q(e: &PdbWireExecutor, sql: &str, p: &[Value]) -> QueryResult {
    e.execute_query(sql, p)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

fn st(e: &PdbWireExecutor, sql: &str) {
    e.execute_statement(sql, &[])
        .unwrap_or_else(|err| panic!("{sql}: {err}"));
}

fn one_i64(e: &PdbWireExecutor, sql: &str) -> i64 {
    let r = q(e, sql, &[]);
    assert_eq!(r.rows.len(), 1, "{sql}: {} rows", r.rows.len());
    r.rows[0]
        .get::<i64>(0)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

/// Microseconds since the epoch of a UTC civil time.
fn micros(y: i64, mo: u32, d: u32, h: i64, mi: i64, s: i64, us: i64) -> i64 {
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let mp = (mo as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    ((days * 24 + h) * 60 + mi) * 60_000_000 + s * 1_000_000 + us
}

/// (column, declared type, SQL literal, expected value). Row 2 of the table
/// is all NULL.
fn type_cases() -> Vec<(&'static str, &'static str, &'static str, Value)> {
    vec![
        ("c_i2", "SMALLINT", "-32768", Value::Int64(-32768)),
        ("c_i4", "INTEGER", "2147483647", Value::Int64(2_147_483_647)),
        (
            "c_i8",
            "BIGINT",
            "-9223372036854775807",
            Value::Int64(-9_223_372_036_854_775_807),
        ),
        ("c_f4", "REAL", "1.5", Value::Float32(1.5)),
        (
            "c_f8",
            "DOUBLE PRECISION",
            "-2.25e10",
            Value::Float64(-2.25e10),
        ),
        (
            "c_num",
            "NUMERIC(20,6)",
            "-12345678901234.000001",
            Value::Numeric("-12345678901234.000001".into()),
        ),
        (
            "c_numu",
            "NUMERIC",
            "98765432109876543210.0123456789",
            Value::Numeric("98765432109876543210.0123456789".into()),
        ),
        (
            "c_text",
            "TEXT",
            "'it''s \"quoted\" ünïcödé'",
            Value::String("it's \"quoted\" ünïcödé".into()),
        ),
        (
            "c_vc",
            "VARCHAR(16)",
            "'short'",
            Value::String("short".into()),
        ),
        ("c_bool", "BOOLEAN", "true", Value::Bool(true)),
        (
            "c_bytea",
            "BYTEA",
            "'\\x00ff10'",
            Value::Bytes(vec![0x00, 0xff, 0x10]),
        ),
        ("c_date", "DATE", "'1969-07-20'", Value::Date(-165)),
        (
            "c_ts",
            "TIMESTAMP",
            "'2026-01-15 12:44:05.123456'",
            Value::Timestamp(micros(2026, 1, 15, 12, 44, 5, 123_456)),
        ),
        (
            "c_tstz",
            "TIMESTAMPTZ",
            "'2026-01-15 12:44:05.5+00'",
            Value::TimestampTz(micros(2026, 1, 15, 12, 44, 5, 500_000)),
        ),
        (
            "c_json",
            "JSON",
            "'{\"a\": [1, 2]}'",
            Value::Json("{\"a\": [1, 2]}".into()),
        ),
        (
            "c_jsonb",
            "JSONB",
            "'{\"k\": \"v\"}'",
            Value::Json("{\"k\": \"v\"}".into()),
        ),
        (
            "c_uuid",
            "UUID",
            "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
            Value::Uuid("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11".into()),
        ),
        (
            "c_iarr",
            "INTEGER[]",
            "ARRAY[1,NULL,3]",
            Value::Array(vec![Value::Int64(1), Value::Null, Value::Int64(3)]),
        ),
        (
            "c_tarr",
            "TEXT[]",
            "ARRAY['a,b','c\"d']",
            Value::Array(vec![
                Value::String("a,b".into()),
                Value::String("c\"d".into()),
            ]),
        ),
        (
            "c_vec",
            "VECTOR(3)",
            "'[1,2.5,-3]'",
            Value::Vector(vec![1.0, 2.5, -3.0]),
        ),
        (
            "c_barr",
            "BYTEA[]",
            "ARRAY['\\x0102'::bytea, '\\xff'::bytea]",
            Value::Array(vec![Value::Bytes(vec![1, 2]), Value::Bytes(vec![0xff])]),
        ),
        (
            "c_iv",
            "INTERVAL",
            "'1 day 02:00:00.25'",
            Value::Interval {
                months: 0,
                days: 1,
                micros: 7_200_250_000,
            },
        ),
        ("c_char", "CHAR(4)", "'ab'", Value::String("ab  ".into())),
    ]
}

fn check_row(
    label: &str,
    r: &QueryResult,
    row: usize,
    null_row: bool,
    cases: &[(&str, &str, &str, Value)],
    failures: &mut Vec<String>,
) {
    let got = &r.rows[row];
    for (i, (col, decl, _, want)) in cases.iter().enumerate() {
        assert_eq!(got.columns()[i + 1], *col);
        let v = &got.values()[i + 1];
        let want = if null_row { Value::Null } else { want.clone() };
        if v != &want {
            failures.push(format!("{label} {col} ({decl}): got {v:?}, want {want:?}"));
        }
    }
}

#[test]
fn every_type_round_trips() {
    let e = live!("every_type_round_trips");
    let cases = type_cases();
    let cols: Vec<String> = cases
        .iter()
        .map(|(c, t, _, _)| format!("{c} {t}"))
        .collect();
    let names: Vec<&str> = cases.iter().map(|(c, ..)| *c).collect();
    st(&e, "DROP TABLE IF EXISTS nwl_types");
    st(
        &e,
        &format!(
            "CREATE TABLE nwl_types (id INT PRIMARY KEY, {})",
            cols.join(", ")
        ),
    );

    // id 1: literals, id 2: all NULL (literal), id 3: parameters, id 4: NULL parameters.
    let lits: Vec<&str> = cases.iter().map(|(_, _, l, _)| *l).collect();
    st(
        &e,
        &format!(
            "INSERT INTO nwl_types (id, {}) VALUES (1, {})",
            names.join(", "),
            lits.join(", ")
        ),
    );
    st(&e, "INSERT INTO nwl_types (id) VALUES (2)");
    let ph: Vec<String> = (2..=cases.len() + 1).map(|i| format!("${i}")).collect();
    let ins = format!(
        "INSERT INTO nwl_types (id, {}) VALUES ($1, {})",
        names.join(", "),
        ph.join(", ")
    );
    let mut p = vec![Value::Int64(3)];
    p.extend(cases.iter().map(|(.., v)| v.clone()));
    assert_eq!(e.execute_statement(&ins, &p).expect("param insert"), 1);
    let mut p = vec![Value::Int64(4)];
    p.extend(cases.iter().map(|_| Value::Null));
    assert_eq!(e.execute_statement(&ins, &p).expect("NULL param insert"), 1);

    let sel = format!("SELECT id, {} FROM nwl_types ORDER BY id", names.join(", "));
    let simple = q(&e, &sel, &[]);
    assert_eq!(simple.rows.len(), 4);
    assert_eq!(simple.columns.len(), cases.len() + 1);
    let mut failures = Vec::new();
    check_row("literal", &simple, 0, false, &cases, &mut failures);
    check_row("literal-null", &simple, 1, true, &cases, &mut failures);
    check_row("param", &simple, 2, false, &cases, &mut failures);
    check_row("param-null", &simple, 3, true, &cases, &mut failures);

    // The same rows through the prepared path (MSG_EXECUTE result decode).
    let prepared = q(
        &e,
        &format!(
            "SELECT id, {} FROM nwl_types WHERE id >= $1 ORDER BY id",
            names.join(", ")
        ),
        &[Value::Int64(3)],
    );
    assert_eq!(prepared.rows.len(), 2);
    check_row(
        "prepared-select",
        &prepared,
        0,
        false,
        &cases,
        &mut failures,
    );
    check_row(
        "prepared-select-null",
        &prepared,
        1,
        true,
        &cases,
        &mut failures,
    );

    // A parameter compared against each typed column finds row 1 (binding is
    // typed, not textual): one probe per column.
    for (col, decl, _, v) in &cases {
        if decl.contains("JSON") || decl.contains("[]") || decl.starts_with("VECTOR") {
            continue; // no equality operator worth asserting on these here
        }
        let r = e.execute_query(
            &format!("SELECT id FROM nwl_types WHERE {col} = $1 ORDER BY id"),
            std::slice::from_ref(v),
        );
        match r {
            Ok(r) => {
                let ids: Vec<i64> = r.rows.iter().map(|x| x.get::<i64>(0).unwrap()).collect();
                if ids != vec![1, 3] {
                    failures.push(format!(
                        "WHERE {col} = $1 ({decl}) matched ids {ids:?}, want [1, 3]"
                    ));
                }
            }
            Err(err) => failures.push(format!("WHERE {col} = $1 ({decl}): {err}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
    println!("NWL_PASS every_type_round_trips");
}

#[test]
fn parameters_bind_and_never_splice() {
    let e = live!("parameters_bind_and_never_splice");
    st(&e, "DROP TABLE IF EXISTS nwl_params");
    st(
        &e,
        "CREATE TABLE nwl_params (id INT PRIMARY KEY, name TEXT, score DOUBLE PRECISION)",
    );
    let evil = "x'); DROP TABLE nwl_params; --";
    let ins = "INSERT INTO nwl_params VALUES ($1, $2, $3)";
    assert_eq!(
        e.execute_statement(
            ins,
            &[
                Value::Int64(1),
                Value::String(evil.into()),
                Value::Float64(0.5)
            ]
        )
        .unwrap(),
        1
    );
    assert_eq!(
        e.execute_statement(ins, &[Value::Int64(2), Value::Null, Value::Float64(1.5)])
            .unwrap(),
        1
    );

    let r = q(
        &e,
        "SELECT name, score FROM nwl_params WHERE id = $1",
        &[Value::Int64(1)],
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        r.rows[0].get::<String>(0).unwrap(),
        evil,
        "the string was stored verbatim"
    );
    assert_eq!(r.rows[0].get::<f64>(1).unwrap(), 0.5);
    let r = q(&e, "SELECT id FROM nwl_params WHERE name IS NULL", &[]);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].get::<i64>(0).unwrap(), 2);
    assert_eq!(
        one_i64(&e, "SELECT count(*) FROM nwl_params"),
        2,
        "table survived the injection string"
    );

    // An UPDATE's affected count, through a parameter.
    let n = e
        .execute_statement(
            "UPDATE nwl_params SET score = score + $1 WHERE id >= $2",
            &[Value::Float64(1.0), Value::Int64(1)],
        )
        .unwrap();
    assert_eq!(n, 2);
    // execute_query reports the affected count of a DML statement too.
    let r = q(
        &e,
        "DELETE FROM nwl_params WHERE id = $1",
        &[Value::Int64(2)],
    );
    assert_eq!(r.rows_affected, 1);

    // Wrong parameter count: refused before anything runs.
    let err = e.execute_statement(ins, &[Value::Int64(9)]).unwrap_err();
    assert!(err.to_string().contains("3 parameter"), "{err}");
    assert_eq!(
        one_i64(&e, "SELECT count(*) FROM nwl_params WHERE id = 9"),
        0
    );

    // DISCARD ALL drops the server-side statements; the next EXECUTE
    // re-prepares instead of failing (pool_size 2, so drain both conns).
    for _ in 0..2 {
        st(&e, "DISCARD ALL");
    }
    let r = q(
        &e,
        "SELECT name FROM nwl_params WHERE id = $1",
        &[Value::Int64(1)],
    );
    assert_eq!(r.rows[0].get::<String>(0).unwrap(), evil);
    println!("NWL_PASS parameters_bind_and_never_splice");
}

fn bulk_rows(from: i64, to: i64) -> Vec<Vec<Value>> {
    (from..to)
        .map(|i| {
            vec![
                Value::Int64(i),
                Value::String(format!("row-{i}")),
                Value::Numeric(format!("{}.{:02}", i, i % 100)),
                if i % 7 == 0 {
                    Value::Null
                } else {
                    Value::Bool(i % 2 == 0)
                },
            ]
        })
        .collect()
}

/// Every row in [from, to) landed with its own values: count, sum, NULL
/// pattern, a per-row label invariant, and spot checks across chunk edges.
fn check_bulk(e: &PdbWireExecutor, from: i64, to: i64) {
    let n = to - from;
    let w = format!("id >= {from} AND id < {to}");
    assert_eq!(
        one_i64(e, &format!("SELECT count(*) FROM nwl_bulk WHERE {w}")),
        n
    );
    // sum(bigint) is numeric (PostgreSQL semantics).
    let r = q(e, &format!("SELECT sum(id) FROM nwl_bulk WHERE {w}"), &[]);
    let want_sum = (from..to).sum::<i64>();
    assert_eq!(r.rows[0].values()[0], Value::Numeric(want_sum.to_string()));
    let nulls = (from..to).filter(|i| i % 7 == 0).count() as i64;
    assert_eq!(
        one_i64(
            e,
            &format!("SELECT count(*) FROM nwl_bulk WHERE {w} AND flag IS NULL")
        ),
        nulls
    );
    assert_eq!(
        one_i64(
            e,
            &format!(
                "SELECT count(*) FROM nwl_bulk WHERE {w} AND label = 'row-' || CAST(id AS TEXT)"
            )
        ),
        n
    );
    for i in [
        from,
        from + 1,
        from + 9_999,
        from + 10_000,
        (from + to) / 2,
        to - 1,
    ] {
        if i >= to {
            continue;
        }
        let r = q(
            e,
            "SELECT label, amount, flag FROM nwl_bulk WHERE id = $1",
            &[Value::Int64(i)],
        );
        assert_eq!(r.rows.len(), 1, "id {i}");
        let row = &r.rows[0];
        assert_eq!(row.get::<String>(0).unwrap(), format!("row-{i}"));
        assert_eq!(
            row.values()[1],
            Value::Numeric(format!("{}.{:02}", i, i % 100)),
            "id {i}"
        );
        let want_flag = if i % 7 == 0 {
            Value::Null
        } else {
            Value::Bool(i % 2 == 0)
        };
        assert_eq!(row.values()[2], want_flag, "id {i}");
    }
}

#[test]
fn execute_many_inserts_100k_rows() {
    let e = live!("execute_many_inserts_100k_rows");
    const N: i64 = 100_000;
    const AUTO: i64 = 12_000; // > one 10 000-row frame: two EXECUTE_BATCH chunks
    st(&e, "DROP TABLE IF EXISTS nwl_bulk");
    st(&e, "CREATE TABLE nwl_bulk (id BIGINT PRIMARY KEY, label TEXT, amount NUMERIC(12,2), flag BOOLEAN)");
    let ins = "INSERT INTO nwl_bulk VALUES ($1, $2, $3, $4)";

    // 100 000 rows in one transaction (10 EXECUTE_BATCH frames, one COMMIT).
    let t = std::time::Instant::now();
    let n = e
        .execute_many_in_transaction(ins, &bulk_rows(0, N))
        .expect("bulk insert in txn");
    println!("execute_many_in_transaction: {N} rows in {:?}", t.elapsed());
    assert_eq!(n, N as u64, "affected-row count");
    check_bulk(&e, 0, N);

    // Autocommit bulk path: every row its own statement.
    let t = std::time::Instant::now();
    let n = e
        .execute_many(ins, &bulk_rows(N, N + AUTO))
        .expect("autocommit bulk insert");
    println!(
        "execute_many (autocommit): {AUTO} rows in {:?}",
        t.elapsed()
    );
    assert_eq!(n, AUTO as u64);
    check_bulk(&e, N, N + AUTO);
    assert_eq!(one_i64(&e, "SELECT count(*) FROM nwl_bulk"), N + AUTO);
    println!("NWL_PASS execute_many_inserts_100k_rows");
}

#[test]
fn execute_many_reports_failed_rows() {
    let e = live!("execute_many_reports_failed_rows");
    st(&e, "DROP TABLE IF EXISTS nwl_bulk_err");
    st(&e, "CREATE TABLE nwl_bulk_err (id INT PRIMARY KEY, v TEXT)");
    let ins = "INSERT INTO nwl_bulk_err VALUES ($1, $2)";
    let rows = |ids: &[i64]| -> Vec<Vec<Value>> {
        ids.iter()
            .map(|i| vec![Value::Int64(*i), Value::String(format!("v{i}"))])
            .collect()
    };

    // Autocommit: row 2 duplicates row 0; rows 0, 1, 3 commit.
    match e.execute_many(ins, &rows(&[1, 2, 1, 3])) {
        Err(Error::Batch(b)) => {
            assert_eq!(
                (b.rows, b.failed, b.applied, b.not_attempted),
                (4, 1, 3, 0),
                "{b}"
            );
            assert_eq!(b.errors[0].0, 2, "{b}");
            // A MSG_BATCH_RESULT entry carries a message but no error code
            // (server framing), so the SQLSTATE is HY000 unless the message
            // ends in a [XXXXX] marker.
            assert!(b.errors[0].2.contains("duplicate key"), "{b}");
        }
        other => panic!("expected Error::Batch, got {other:?}"),
    }
    assert_eq!(one_i64(&e, "SELECT count(*) FROM nwl_bulk_err"), 3);

    // Transactional: the duplicate rolls back the whole call.
    match e.execute_many_in_transaction(ins, &rows(&[10, 11, 10, 12])) {
        Err(Error::Batch(b)) => {
            assert_eq!((b.failed, b.not_attempted), (1, 1), "{b}");
            assert_eq!(b.errors[0].0, 2, "{b}");
        }
        other => panic!("expected Error::Batch, got {other:?}"),
    }
    assert_eq!(
        one_i64(&e, "SELECT count(*) FROM nwl_bulk_err"),
        3,
        "the failed transaction committed rows"
    );
    // ... and the executor is usable afterwards (no connection left in a failed block).
    assert_eq!(
        e.execute_many_in_transaction(ins, &rows(&[20, 21]))
            .unwrap(),
        2
    );
    assert_eq!(one_i64(&e, "SELECT count(*) FROM nwl_bulk_err"), 5);

    // A statement on a missing table: an error, nothing applied.
    match e.execute_many(
        "INSERT INTO nwl_no_such_table VALUES ($1)",
        &[vec![Value::Int64(1)]],
    ) {
        Err(Error::Batch(b)) => {
            assert_eq!((b.failed, b.applied), (1, 0), "{b}");
            assert!(b.errors[0].2.contains("nwl_no_such_table"), "{b}");
        }
        other => panic!("insert into a missing table: {other:?}"),
    }
    println!("NWL_PASS execute_many_reports_failed_rows");
}

#[test]
fn errors_carry_sqlstate() {
    let e = live!("errors_carry_sqlstate");
    st(&e, "DROP TABLE IF EXISTS nwl_err");
    st(&e, "CREATE TABLE nwl_err (id INT PRIMARY KEY)");
    st(&e, "INSERT INTO nwl_err VALUES (1)");
    let cases: Vec<(&str, Vec<Value>, &str)> = vec![
        ("SELEC 1", vec![], "42601"),
        ("SELECT * FROM nwl_missing_table", vec![], "42P01"),
        (
            "SELECT * FROM nwl_missing_table WHERE id = $1",
            vec![Value::Int64(1)],
            "42P01",
        ),
        ("INSERT INTO nwl_err VALUES (1)", vec![], "23505"),
        (
            "INSERT INTO nwl_err VALUES ($1)",
            vec![Value::Int64(1)],
            "23505",
        ),
        ("SELECT 1 / 0", vec![], "22012"),
        (
            "SELECT id / $1 FROM nwl_err",
            vec![Value::Int64(0)],
            "22012",
        ),
    ];
    let mut failures = Vec::new();
    for (sql, params, want) in cases {
        match e.execute_query(sql, &params) {
            Ok(r) => failures.push(format!("{sql}: succeeded with {} rows", r.rows.len())),
            Err(err) => match &err {
                Error::Server { sqlstate, .. } if sqlstate == want => {}
                other => failures.push(format!("{sql}: want SQLSTATE {want}, got {other:?}")),
            },
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    // The executor still works after errors.
    assert_eq!(one_i64(&e, "SELECT count(*) FROM nwl_err"), 1);
    println!("NWL_PASS errors_carry_sqlstate");
}
