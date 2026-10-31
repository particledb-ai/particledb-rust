//! Arrow IPC <-> SDK values for the native wire.
//!
//! * [`decode_result`]: a result's Arrow IPC stream -> [`QueryResult`]. The
//!   Arrow type decides the [`Value`] variant; for a `Utf8` column the
//!   engine's declared SQL type (`pdb:decl` field metadata, stamped by the
//!   server -- the only channel that survives for a type it materializes as
//!   text) refines it: numeric -> `Numeric`, json/jsonb -> `Json`, `T[]` ->
//!   `Array` (the server ships an array as its `ARRAY[...]` constructor
//!   text), interval -> `Interval`, uuid -> `Uuid`. A `vector(n)` arrives as
//!   `Binary` holding `n` little-endian `f32`s; `timestamptz` as an Arrow
//!   `Timestamp` with no zone, declared `TIMESTAMPTZ` (UTC).
//! * [`encode_params`]: parameter rows -> one Arrow IPC stream (one column per
//!   parameter, one row per parameter set), the `MSG_EXECUTE` /
//!   `MSG_EXECUTE_BATCH` body. Only Arrow types the server's strict decoder
//!   accepts are produced (`decode_arrow_param` in
//!   the server's parameter decoder: bool, ints, floats, utf8, binary,
//!   decimal128, date32); every other value travels as its PostgreSQL text
//!   form in a `Utf8` column and the engine converts it at the target.
//!
//! An Arrow type this module cannot represent is an error, never a guessed or
//! empty value.

use std::sync::Arc;

use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
        Float64Builder, Int64Builder, StringBuilder,
    },
    cast::AsArray,
    types::*,
    Array, ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::error::{Error, Result};
use crate::types::{QueryResult, Row, Value};

fn conv(msg: impl Into<String>) -> Error {
    Error::TypeConversion(msg.into())
}

// ── Calendar helpers (proleptic Gregorian, no external date crate) ─────────

/// Days since 1970-01-01 of a civil date (H. Hinnant's `days_from_civil`).
pub(crate) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Civil date of a day count since 1970-01-01.
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// `YYYY-MM-DD` (years outside 1..=9999 keep PostgreSQL's ` BC` suffix rule out
/// of scope: they are rendered with a sign).
pub(crate) fn format_date(days: i64) -> String {
    let (ymd, bc) = date_parts(days);
    if bc {
        format!("{ymd} BC")
    } else {
        ymd
    }
}

/// `YYYY-MM-DD` with PostgreSQL's era rule (astronomical year 0 is 1 BC), and
/// whether it is BC.
fn date_parts(days: i64) -> (String, bool) {
    let (y, m, d) = civil_from_days(days);
    if y <= 0 {
        (format!("{:04}-{m:02}-{d:02}", 1 - y), true)
    } else {
        (format!("{y:04}-{m:02}-{d:02}"), false)
    }
}

/// `HH:MM:SS[.ffffff]` of microseconds since midnight.
pub(crate) fn format_time(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    let (h, mi, s) = (secs / 3600, (secs / 60) % 60, secs % 60);
    if frac == 0 {
        format!("{h:02}:{mi:02}:{s:02}")
    } else {
        format!("{h:02}:{mi:02}:{s:02}.{frac:06}")
    }
}

/// `YYYY-MM-DD HH:MM:SS[.ffffff]` of microseconds since the Unix epoch.
pub(crate) fn format_timestamp(micros: i64) -> String {
    timestamp_text(micros, "")
}

/// Timestamp text with an optional zone suffix (`+00`), the era last as
/// PostgreSQL writes it: `0044-03-15 12:00:00+00 BC`.
pub(crate) fn timestamp_text(micros: i64, zone: &str) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let tod = micros.rem_euclid(MICROS_PER_DAY);
    let (ymd, bc) = date_parts(days);
    format!(
        "{ymd} {}{zone}{}",
        format_time(tod),
        if bc { " BC" } else { "" }
    )
}

/// Strip a trailing PostgreSQL era marker: (text, is_bc).
fn strip_era(s: &str) -> (&str, bool) {
    let t = s.trim_end();
    if let Some(head) = t.strip_suffix(" BC").or_else(|| t.strip_suffix(" bc")) {
        (head.trim_end(), true)
    } else if let Some(head) = t.strip_suffix(" AD").or_else(|| t.strip_suffix(" ad")) {
        (head.trim_end(), false)
    } else {
        (t, false)
    }
}

/// Parse `YYYY-MM-DD` -> days since epoch.
fn parse_date(s: &str) -> Option<i64> {
    let (s, bc) = strip_era(s.trim());
    if bc {
        // `YYYY-MM-DD BC`: astronomical year 1 - YYYY.
        let mut it = s.splitn(3, '-');
        let y: i64 = it.next()?.parse().ok()?;
        let m: u32 = it.next()?.parse().ok()?;
        let d: u32 = it.next()?.parse().ok()?;
        if y < 1 || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
            return None;
        }
        return Some(days_from_civil(1 - y, m, d));
    }
    let (neg, body) = match s.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, s),
    };
    let mut it = body.splitn(3, '-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(if neg { -y } else { y }, m, d))
}

/// Parse `HH:MM[:SS[.ffffff]]` -> microseconds since midnight.
fn parse_time(s: &str) -> Option<i64> {
    let mut it = s.trim().splitn(3, ':');
    let h: i64 = it.next()?.parse().ok()?;
    let mi: i64 = it.next()?.parse().ok()?;
    let (sec, frac) = match it.next() {
        None => (0, 0),
        Some(rest) => match rest.split_once('.') {
            None => (rest.parse().ok()?, 0),
            Some((a, f)) => {
                if f.is_empty() || f.len() > 9 || !f.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                let mut digits = f.to_string();
                digits.truncate(6);
                while digits.len() < 6 {
                    digits.push('0');
                }
                (a.parse().ok()?, digits.parse::<i64>().ok()?)
            }
        },
    };
    if h > 24 || mi > 59 || sec > 60 {
        return None;
    }
    Some(((h * 60 + mi) * 60 + sec) * 1_000_000 + frac)
}

/// Parse a UTC offset `+HH`, `+HH:MM`, `+HHMM`, `Z` -> seconds east of UTC.
fn parse_offset(s: &str) -> Option<i64> {
    if s == "Z" || s == "z" {
        return Some(0);
    }
    let sign = match s.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let body = &s[1..];
    let (h, m, sec) = if let Some((h, rest)) = body.split_once(':') {
        let mut parts = rest.splitn(2, ':');
        let m = parts.next()?;
        let sec = parts.next().unwrap_or("0");
        (h, m, sec)
    } else if body.len() == 4 {
        (&body[..2], &body[2..], "0")
    } else {
        (body, "0", "0")
    };
    let (h, m, sec): (i64, i64, i64) = (h.parse().ok()?, m.parse().ok()?, sec.parse().ok()?);
    Some(sign * (h * 3600 + m * 60 + sec))
}

/// Parse a PostgreSQL timestamp text (`YYYY-MM-DD[ T]HH:MM:SS[.f][offset]`)
/// -> (microseconds since epoch, as UTC when an offset was present; offset seen).
pub(crate) fn parse_timestamp(s: &str) -> Option<(i64, bool)> {
    let (s, bc) = strip_era(s.trim());
    let sep = s.find([' ', 'T'])?;
    let days = if bc {
        parse_date(&format!("{} BC", &s[..sep]))?
    } else {
        parse_date(&s[..sep])?
    };
    let rest = &s[sep + 1..];
    // The offset starts at the first '+', 'Z' or a '-' after the time.
    let off_at = rest.find(['+', 'Z', 'z']).or_else(|| rest.find('-'));
    let (time_s, off) = match off_at {
        Some(i) => (&rest[..i], Some(parse_offset(&rest[i..])?)),
        None => (rest, None),
    };
    let tod = parse_time(time_s)?;
    let local = days * MICROS_PER_DAY + tod;
    match off {
        Some(o) => Some((local - o * 1_000_000, true)),
        None => Some((local, false)),
    }
}

// ── PostgreSQL array / vector text ─────────────────────────────────────────

/// A parsed PostgreSQL array literal: nested lists of optional element text.
#[derive(Debug, PartialEq)]
enum PgArr {
    Elem(Option<String>),
    List(Vec<PgArr>),
}

/// Parse an array value as the server ships it: the SQL constructor form
/// `ARRAY[1,NULL,'a''b',ARRAY[2]]` (what the engine stores and returns), or
/// PostgreSQL's output syntax `{a,"b c",NULL,{1,2}}`.
fn parse_pg_array(s: &str) -> Option<PgArr> {
    let t = s.trim();
    if t.len() >= 6 && t[..6].eq_ignore_ascii_case("ARRAY[") {
        let (v, end) = parse_sql_array(t.as_bytes(), 0)?;
        return if t.as_bytes()[end..].iter().all(|c| c.is_ascii_whitespace()) {
            Some(v)
        } else {
            None
        };
    }
    let b = t.as_bytes();
    // A dimension decoration (`[1:2]={...}`) is skipped.
    let start = if b.first() == Some(&b'[') {
        s.trim().find('=')? + 1
    } else {
        0
    };
    let (v, end) = parse_list(&b[start..], 0)?;
    if b[start + end..].iter().all(|c| c.is_ascii_whitespace()) {
        Some(v)
    } else {
        None
    }
}

/// `ARRAY[` elem (`,` elem)* `]`; an element is `NULL`, a `'...'` string
/// (`''` escapes a quote), a nested `ARRAY[...]` / `[...]`, or a bare token
/// (number, `true`, ...).
fn parse_sql_array(b: &[u8], mut i: usize) -> Option<(PgArr, usize)> {
    let ws = |b: &[u8], mut i: usize| {
        while b.get(i).is_some_and(|c| c.is_ascii_whitespace()) {
            i += 1;
        }
        i
    };
    i = ws(b, i);
    if b.len() >= i + 6 && b[i..i + 6].eq_ignore_ascii_case(b"ARRAY[") {
        i += 6;
    } else if b.get(i) == Some(&b'[') {
        i += 1;
    } else {
        return None;
    }
    let mut out = Vec::new();
    i = ws(b, i);
    if b.get(i) == Some(&b']') {
        return Some((PgArr::List(out), i + 1));
    }
    loop {
        i = ws(b, i);
        let c = *b.get(i)?;
        if c == b'[' || (b.len() >= i + 6 && b[i..i + 6].eq_ignore_ascii_case(b"ARRAY[")) {
            let (v, j) = parse_sql_array(b, i)?;
            out.push(v);
            i = j;
        } else if c == b'\'' {
            i += 1;
            let mut buf = Vec::new();
            loop {
                match *b.get(i)? {
                    b'\'' if b.get(i + 1) == Some(&b'\'') => {
                        buf.push(b'\'');
                        i += 2;
                    }
                    b'\'' => {
                        i += 1;
                        break;
                    }
                    x => {
                        buf.push(x);
                        i += 1;
                    }
                }
            }
            // An optional cast suffix (`'..'::type`) is skipped.
            if b.get(i) == Some(&b':') && b.get(i + 1) == Some(&b':') {
                while !matches!(b.get(i)?, b',' | b']') {
                    i += 1;
                }
            }
            out.push(PgArr::Elem(Some(String::from_utf8(buf).ok()?)));
        } else {
            let st = i;
            while !matches!(b.get(i)?, b',' | b']') {
                i += 1;
            }
            let raw = std::str::from_utf8(&b[st..i]).ok()?.trim();
            if raw.eq_ignore_ascii_case("NULL") {
                out.push(PgArr::Elem(None));
            } else {
                out.push(PgArr::Elem(Some(raw.to_string())));
            }
        }
        i = ws(b, i);
        match b.get(i)? {
            b',' => i += 1,
            b']' => return Some((PgArr::List(out), i + 1)),
            _ => return None,
        }
    }
}

fn parse_list(b: &[u8], mut i: usize) -> Option<(PgArr, usize)> {
    if b.get(i) != Some(&b'{') {
        return None;
    }
    i += 1;
    let mut out = Vec::new();
    if b.get(i) == Some(&b'}') {
        return Some((PgArr::List(out), i + 1));
    }
    loop {
        while b.get(i).is_some_and(|c| c.is_ascii_whitespace()) {
            i += 1;
        }
        match b.get(i)? {
            b'{' => {
                let (v, j) = parse_list(b, i)?;
                out.push(v);
                i = j;
            }
            b'"' => {
                i += 1;
                let mut buf = Vec::new();
                loop {
                    match *b.get(i)? {
                        b'\\' => {
                            buf.push(*b.get(i + 1)?);
                            i += 2;
                        }
                        b'"' => {
                            i += 1;
                            break;
                        }
                        c => {
                            buf.push(c);
                            i += 1;
                        }
                    }
                }
                out.push(PgArr::Elem(Some(String::from_utf8(buf).ok()?)));
            }
            _ => {
                let st = i;
                while !matches!(b.get(i)?, b',' | b'}') {
                    i += 1;
                }
                let raw = std::str::from_utf8(&b[st..i]).ok()?.trim();
                if raw.eq_ignore_ascii_case("NULL") {
                    out.push(PgArr::Elem(None));
                } else {
                    out.push(PgArr::Elem(Some(raw.to_string())));
                }
            }
        }
        while b.get(i).is_some_and(|c| c.is_ascii_whitespace()) {
            i += 1;
        }
        match b.get(i)? {
            b',' => i += 1,
            b'}' => return Some((PgArr::List(out), i + 1)),
            _ => return None,
        }
    }
}

/// The element-level declared type of an array decl (`integer[]` -> `integer`,
/// `_int4` -> `int4`, `text[][]` -> `text`).
fn array_elem_decl(decl: &str) -> Option<String> {
    let d = decl.trim().to_ascii_lowercase();
    if let Some(i) = d.find('[') {
        return Some(d[..i].trim().to_string());
    }
    if let Some(rest) = d.strip_prefix('_') {
        return Some(rest.to_string());
    }
    if let Some(rest) = d.strip_suffix(" array") {
        return Some(rest.trim().to_string());
    }
    None
}

/// A declared type without its modifiers, lowercased, whitespace collapsed:
/// `TIMESTAMP(6) WITH TIME ZONE` -> `timestamp with time zone`,
/// `NUMERIC(10,2)` -> `numeric`. Every `(...)` is removed, not just a
/// trailing one, so a qualifier after the modifier survives.
fn base_type(decl: &str) -> String {
    let mut out = String::with_capacity(decl.len());
    let mut depth = 0u32;
    for c in decl.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 => out.push(c.to_ascii_lowercase()),
            _ => {}
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// A scalar of declared type `decl` from its PostgreSQL text form.
fn scalar_from_text(decl: &str, text: &str) -> Result<Value> {
    let base = base_type(decl);
    let bad = || conv(format!("cannot decode {text:?} as {decl}"));
    Ok(match base.as_str() {
        "smallint" | "integer" | "bigint" | "int" | "int2" | "int4" | "int8" | "smallserial"
        | "serial" | "bigserial" => Value::Int64(text.trim().parse().map_err(|_| bad())?),
        "real" | "float4" => Value::Float32(parse_pg_float(text).ok_or_else(bad)? as f32),
        "double precision" | "float8" | "float" => {
            Value::Float64(parse_pg_float(text).ok_or_else(bad)?)
        }
        "numeric" | "decimal" | "dec" => Value::Numeric(text.trim().to_string()),
        "boolean" | "bool" => match text.trim() {
            "t" | "true" | "TRUE" => Value::Bool(true),
            "f" | "false" | "FALSE" => Value::Bool(false),
            _ => return Err(bad()),
        },
        "json" | "jsonb" => Value::Json(text.to_string()),
        "date" => Value::Date(parse_date(text).ok_or_else(bad)? as i32),
        "timestamptz" | "timestamp with time zone" => {
            Value::TimestampTz(parse_timestamp(text).ok_or_else(bad)?.0)
        }
        "timestamp" | "timestamp without time zone" => {
            Value::Timestamp(parse_timestamp(text).ok_or_else(bad)?.0)
        }
        "vector" => Value::Vector(parse_vector(text).ok_or_else(bad)?),
        "bytea" => Value::Bytes(parse_bytea(text).ok_or_else(bad)?),
        "interval" => {
            let (months, days, micros) = parse_interval(text).ok_or_else(bad)?;
            Value::Interval {
                months,
                days,
                micros,
            }
        }
        "uuid" => Value::Uuid(text.to_string()),
        _ => Value::String(text.to_string()),
    })
}

/// Parse PostgreSQL's default interval output (`1 year 2 mons -3 days
/// 04:05:06.5`, `00:00:00`, `@ 1 day` verbose units) -> (months, days, micros).
pub(crate) fn parse_interval(s: &str) -> Option<(i32, i32, i64)> {
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i128 = 0;
    let toks: Vec<&str> = s.split_whitespace().filter(|t| *t != "@").collect();
    if toks.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        if t.contains(':') {
            let (neg, body) = match t.strip_prefix('-') {
                Some(b) => (true, b),
                None => (false, t.strip_prefix('+').unwrap_or(t)),
            };
            let mut parts = body.splitn(3, ':');
            let h: i128 = parts.next()?.parse().ok()?;
            let m: i128 = parts.next()?.parse().ok()?;
            let sec_us = match parts.next() {
                Some(sec) => {
                    let (whole, frac) = sec.split_once('.').unwrap_or((sec, ""));
                    let mut f = frac.to_string();
                    if f.len() > 6 || !f.bytes().all(|c| c.is_ascii_digit()) {
                        return None;
                    }
                    while f.len() < 6 {
                        f.push('0');
                    }
                    whole.parse::<i128>().ok()? * 1_000_000 + f.parse::<i128>().ok()?
                }
                None => 0,
            };
            let v = (h * 3600 + m * 60) * 1_000_000 + sec_us;
            micros += if neg { -v } else { v };
            i += 1;
            continue;
        }
        let n: i64 = t.parse().ok()?;
        let unit = toks.get(i + 1)?.trim_end_matches(',').to_ascii_lowercase();
        let unit = unit.trim_end_matches('s');
        match unit {
            "year" => months += n * 12,
            "mon" | "month" => months += n,
            "week" => days += n * 7,
            "day" => days += n,
            "hour" => micros += n as i128 * 3_600_000_000,
            "min" | "minute" => micros += n as i128 * 60_000_000,
            "sec" | "second" => micros += n as i128 * 1_000_000,
            _ => return None,
        }
        i += 2;
    }
    Some((
        i32::try_from(months).ok()?,
        i32::try_from(days).ok()?,
        i64::try_from(micros).ok()?,
    ))
}

/// PostgreSQL interval input text of (months, days, micros).
pub(crate) fn interval_text(months: i32, days: i32, micros: i64) -> String {
    let neg = micros < 0;
    // unsigned_abs: |i64::MIN| does not fit i64.
    let m = micros.unsigned_abs();
    let (secs, frac) = (m / 1_000_000, m % 1_000_000);
    let (h, mi, sec) = (secs / 3600, (secs / 60) % 60, secs % 60);
    format!(
        "{months} mons {days} days {}{h:02}:{mi:02}:{sec:02}.{frac:06}",
        if neg { "-" } else { "" }
    )
}

/// A `bytea` element as the server renders it inside an array (`X'0102'`),
/// or PostgreSQL's hex output (`\x0102`).
fn parse_bytea(s: &str) -> Option<Vec<u8>> {
    let t = s.trim();
    let hex = if let Some(h) = t.strip_prefix("X'").or_else(|| t.strip_prefix("x'")) {
        h.strip_suffix('\'')?
    } else {
        t.strip_prefix("\\x")?
    };
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

fn parse_pg_float(s: &str) -> Option<f64> {
    match s.trim() {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        t => t.parse().ok(),
    }
}

/// Parse a vector literal `[1,2.5,3]`.
pub(crate) fn parse_vector(s: &str) -> Option<Vec<f32>> {
    let t = s.trim().strip_prefix('[')?.strip_suffix(']')?;
    if t.trim().is_empty() {
        return Some(Vec::new());
    }
    t.split(',').map(|x| x.trim().parse::<f32>().ok()).collect()
}

fn pg_array_to_value(a: PgArr, elem_decl: &str) -> Result<Value> {
    match a {
        PgArr::Elem(None) => Ok(Value::Null),
        PgArr::Elem(Some(t)) => scalar_from_text(elem_decl, &t),
        PgArr::List(items) => Ok(Value::Array(
            items
                .into_iter()
                .map(|i| pg_array_to_value(i, elem_decl))
                .collect::<Result<_>>()?,
        )),
    }
}

// ── Decode ─────────────────────────────────────────────────────────────────

fn decl_of(field: &Field) -> Option<String> {
    field
        .metadata()
        .get("pdb:decl")
        .map(|d| d.trim().to_ascii_lowercase())
}

/// Decode a result's Arrow IPC stream into rows. Column names (and their
/// order and duplicates) come from the schema, so a 0-row result still
/// reports its columns.
pub fn decode_result(bytes: &[u8]) -> Result<QueryResult> {
    let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| Error::Query(format!("bad Arrow IPC result: {e}")))?;
    let schema = reader.schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| Error::Query(format!("bad Arrow IPC batch: {e}")))?;
        let bschema = batch.schema();
        let mut cols: Vec<Vec<Value>> = Vec::with_capacity(batch.num_columns());
        for (ci, array) in batch.columns().iter().enumerate() {
            // Metadata from the stream schema (the batch schema carries the same).
            let field = schema
                .fields()
                .get(ci)
                .cloned()
                .unwrap_or_else(|| bschema.fields()[ci].clone());
            let decl = decl_of(&field);
            let mut col = Vec::with_capacity(array.len());
            let at =
                |r: usize, e: Error| conv(format!("column {:?} row {}: {}", field.name(), r, e));
            if let DataType::Dictionary(_, _) = array.data_type() {
                // Resolve the keys once for the column (per cell would be
                // quadratic); a NULL key is a NULL cell.
                let d = array.as_any_dictionary();
                let keys = d.normalized_keys();
                for (r, key) in keys.iter().enumerate() {
                    col.push(if array.is_null(r) {
                        Value::Null
                    } else {
                        cell(d.values().as_ref(), *key, decl.as_deref()).map_err(|e| at(r, e))?
                    });
                }
            } else {
                for r in 0..array.len() {
                    col.push(cell(array.as_ref(), r, decl.as_deref()).map_err(|e| at(r, e))?);
                }
            }
            cols.push(col);
        }
        let mut col_iters: Vec<_> = cols.into_iter().map(|c| c.into_iter()).collect();
        for _ in 0..batch.num_rows() {
            let vals: Vec<Value> = col_iters
                .iter_mut()
                .map(|it| it.next().unwrap_or(Value::Null))
                .collect();
            rows.push(Row::new(columns.clone(), vals));
        }
    }
    Ok(QueryResult {
        rows,
        columns,
        rows_affected: 0,
    })
}

fn decimal_text(v: i128, scale: i8) -> String {
    decimal_text_of(&v.to_string(), scale)
}

/// Place the decimal point in a signed coefficient's base-10 text (any width:
/// a Decimal256 coefficient is formatted from its full 76 digits).
fn decimal_text_of(coef: &str, scale: i8) -> String {
    let (neg, digits) = match coef.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, coef),
    };
    let sign = if neg { "-" } else { "" };
    if scale <= 0 {
        let zeros = "0".repeat((-(scale as i32)) as usize);
        return if digits == "0" {
            "0".into()
        } else {
            format!("{sign}{digits}{zeros}")
        };
    }
    let scale = scale as usize;
    let (int, frac) = if digits.len() > scale {
        (
            digits[..digits.len() - scale].to_string(),
            digits[digits.len() - scale..].to_string(),
        )
    } else {
        (
            "0".to_string(),
            format!("{}{}", "0".repeat(scale - digits.len()), digits),
        )
    };
    format!("{sign}{int}.{frac}")
}

fn ts_to_micros(v: i64, unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => v.saturating_mul(1_000_000),
        TimeUnit::Millisecond => v.saturating_mul(1_000),
        TimeUnit::Microsecond => v,
        TimeUnit::Nanosecond => v.div_euclid(1_000),
    }
}

/// One cell. `decl` is the column's lowercased `pdb:decl`, if any.
fn cell(array: &dyn Array, i: usize, decl: Option<&str>) -> Result<Value> {
    // `NullArray` has no validity buffer: `is_null` is false for it.
    if array.data_type() == &DataType::Null || array.is_null(i) {
        return Ok(Value::Null);
    }
    let unsupported = || {
        conv(format!(
            "unsupported Arrow type {:?} (decl {:?})",
            array.data_type(),
            decl
        ))
    };
    Ok(match array.data_type() {
        DataType::Boolean => Value::Bool(array.as_boolean().value(i)),
        DataType::Int8 => Value::Int64(array.as_primitive::<Int8Type>().value(i) as i64),
        DataType::Int16 => Value::Int64(array.as_primitive::<Int16Type>().value(i) as i64),
        DataType::Int32 => Value::Int64(array.as_primitive::<Int32Type>().value(i) as i64),
        DataType::Int64 => Value::Int64(array.as_primitive::<Int64Type>().value(i)),
        DataType::UInt8 => Value::Int64(array.as_primitive::<UInt8Type>().value(i) as i64),
        DataType::UInt16 => Value::Int64(array.as_primitive::<UInt16Type>().value(i) as i64),
        DataType::UInt32 => Value::Int64(array.as_primitive::<UInt32Type>().value(i) as i64),
        DataType::UInt64 => {
            let v = array.as_primitive::<UInt64Type>().value(i);
            match i64::try_from(v) {
                Ok(x) => Value::Int64(x),
                Err(_) => Value::Numeric(v.to_string()),
            }
        }
        DataType::Float16 => Value::Float32(array.as_primitive::<Float16Type>().value(i).to_f32()),
        DataType::Float32 => Value::Float32(array.as_primitive::<Float32Type>().value(i)),
        DataType::Float64 => Value::Float64(array.as_primitive::<Float64Type>().value(i)),
        DataType::Decimal128(_, s) => Value::Numeric(decimal_text(
            array.as_primitive::<Decimal128Type>().value(i),
            *s,
        )),
        DataType::Decimal256(_, s) => {
            let v = array.as_primitive::<Decimal256Type>().value(i);
            Value::Numeric(decimal_text_of(&v.to_string(), *s))
        }
        DataType::Utf8 => text_cell(array.as_string::<i32>().value(i), decl)?,
        DataType::LargeUtf8 => text_cell(array.as_string::<i64>().value(i), decl)?,
        DataType::Utf8View => text_cell(array.as_string_view().value(i), decl)?,
        DataType::Binary if is_vector_decl(decl) => {
            Value::Vector(vector_from_le_bytes(array.as_binary::<i32>().value(i))?)
        }
        DataType::Binary => Value::Bytes(array.as_binary::<i32>().value(i).to_vec()),
        DataType::LargeBinary => Value::Bytes(array.as_binary::<i64>().value(i).to_vec()),
        DataType::BinaryView => Value::Bytes(array.as_binary_view().value(i).to_vec()),
        DataType::FixedSizeBinary(16) if decl == Some("uuid") => {
            Value::Uuid(uuid_text(array.as_fixed_size_binary().value(i)))
        }
        DataType::FixedSizeBinary(_) => {
            Value::Bytes(array.as_fixed_size_binary().value(i).to_vec())
        }
        DataType::Date32 => Value::Date(array.as_primitive::<Date32Type>().value(i)),
        DataType::Date64 => Value::Date(
            array
                .as_primitive::<Date64Type>()
                .value(i)
                .div_euclid(86_400_000) as i32,
        ),
        DataType::Timestamp(unit, tz) => {
            let raw = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(i),
                TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(i),
                TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(i),
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(i),
            };
            let us = ts_to_micros(raw, unit);
            // A zone on the Arrow type, or a `timestamptz` declaration, means
            // the value is an absolute instant (stored as UTC).
            let tz_decl = matches!(decl, Some(d) if d.starts_with("timestamptz") || d.contains("with time zone"));
            if tz.is_some() || tz_decl {
                Value::TimestampTz(us)
            } else {
                Value::Timestamp(us)
            }
        }
        DataType::Interval(_) => {
            use arrow_schema::IntervalUnit;
            let (months, days, micros) = match array.data_type() {
                DataType::Interval(IntervalUnit::YearMonth) => {
                    (array.as_primitive::<IntervalYearMonthType>().value(i), 0, 0)
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    let v = array.as_primitive::<IntervalDayTimeType>().value(i);
                    (0, v.days, v.milliseconds as i64 * 1_000)
                }
                _ => {
                    let v = array.as_primitive::<IntervalMonthDayNanoType>().value(i);
                    (v.months, v.days, v.nanoseconds / 1_000)
                }
            };
            Value::Interval {
                months,
                days,
                micros,
            }
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let child: ArrayRef = match array.data_type() {
                DataType::List(_) => array.as_list::<i32>().value(i),
                DataType::LargeList(_) => array.as_list::<i64>().value(i),
                _ => array.as_fixed_size_list().value(i),
            };
            let is_vector = matches!(decl, Some(d) if d.starts_with("vector"));
            if is_vector
                || (matches!(array.data_type(), DataType::FixedSizeList(_, _))
                    && child.data_type() == &DataType::Float32)
            {
                let mut v = Vec::with_capacity(child.len());
                for j in 0..child.len() {
                    match cell(child.as_ref(), j, None)? {
                        Value::Float32(f) => v.push(f),
                        Value::Float64(f) => v.push(f as f32),
                        Value::Int64(n) => v.push(n as f32),
                        other => return Err(conv(format!("vector element {other:?}"))),
                    }
                }
                Value::Vector(v)
            } else {
                let elem_decl = decl.and_then(array_elem_decl);
                let mut v = Vec::with_capacity(child.len());
                for j in 0..child.len() {
                    v.push(cell(child.as_ref(), j, elem_decl.as_deref())?);
                }
                Value::Array(v)
            }
        }
        // A dictionary nested in a list (top-level columns are resolved once
        // per column in `decode_result`).
        DataType::Dictionary(_, _) => {
            let d = array.as_any_dictionary();
            let key = d.normalized_keys()[i];
            cell(d.values().as_ref(), key, decl)?
        }
        _ => return Err(unsupported()),
    })
}

fn is_vector_decl(decl: Option<&str>) -> bool {
    matches!(decl, Some(d) if d.starts_with("vector"))
}

/// A `vector(n)` cell as the server ships it: `n` little-endian `f32`s.
fn vector_from_le_bytes(b: &[u8]) -> Result<Vec<f32>> {
    if b.len() % 4 != 0 {
        return Err(conv(format!(
            "vector payload of {} bytes is not a whole number of f32s",
            b.len()
        )));
    }
    Ok(b.chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

fn uuid_text(b: &[u8]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// A `Utf8` cell, typed by its declared SQL type.
fn text_cell(s: &str, decl: Option<&str>) -> Result<Value> {
    let Some(d) = decl else {
        return Ok(Value::String(s.to_string()));
    };
    if let Some(elem) = array_elem_decl(d) {
        let arr = parse_pg_array(s)
            .ok_or_else(|| conv(format!("cannot parse array literal {s:?} ({d})")))?;
        return pg_array_to_value(arr, &elem);
    }
    if d == "uuid" {
        return Ok(Value::Uuid(s.to_string()));
    }
    scalar_from_text(d, s)
}

// ── Encode (parameters) ────────────────────────────────────────────────────

/// The Arrow column kind a parameter column is sent as.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum Kind {
    Bool,
    Int,
    F32,
    F64,
    Bytes,
    Date,
    Decimal(i8),
    /// A `Numeric` that does not fit `Decimal128` (NaN, > 38 digits, exponent):
    /// the whole column then goes as text.
    NumText,
    Text,
}

/// Exact `Decimal128(38, scale)` digits of a numeric text, or `None` when it
/// does not fit (more than 38 digits, NaN, exponent) and must go as text.
fn numeric_to_i128(s: &str) -> Option<(i128, i8)> {
    let t = s.trim();
    let (neg, body) = match t.as_bytes().first()? {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits = format!("{int}{frac}");
    let sig = digits.trim_start_matches('0');
    if sig.len() > 38 || frac.len() > 38 {
        return None;
    }
    let v: i128 = if sig.is_empty() { 0 } else { sig.parse().ok()? };
    Some((if neg { -v } else { v }, frac.len() as i8))
}

fn kind_of(v: &Value) -> Option<Kind> {
    Some(match v {
        Value::Null => return None,
        Value::Bool(_) => Kind::Bool,
        Value::Int64(_) => Kind::Int,
        Value::Float32(_) => Kind::F32,
        Value::Float64(_) => Kind::F64,
        Value::Bytes(_) => Kind::Bytes,
        Value::Date(_) => Kind::Date,
        Value::Numeric(s) => match numeric_to_i128(s) {
            Some((_, scale)) => Kind::Decimal(scale),
            None => Kind::NumText,
        },
        _ => Kind::Text,
    })
}

/// The PostgreSQL text form of a value sent in a `Utf8` parameter column.
pub(crate) fn param_text(v: &Value) -> Result<String> {
    Ok(match v {
        Value::String(s) | Value::Json(s) | Value::Numeric(s) | Value::Uuid(s) => s.clone(),
        Value::Timestamp(us) => format_timestamp(*us),
        Value::TimestampTz(us) => timestamp_text(*us, "+00"),
        Value::Date(d) => format_date(*d as i64),
        Value::Interval {
            months,
            days,
            micros,
        } => interval_text(*months, *days, *micros),
        Value::Vector(v) => vector_text(v),
        Value::Array(items) => array_text(items)?,
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float32(f) => float_text(*f as f64),
        Value::Float64(f) => float_text(*f),
        Value::Bytes(b) => {
            let mut s = String::from("\\x");
            for x in b {
                s.push_str(&format!("{x:02x}"));
            }
            s
        }
        Value::Null => return Err(conv("NULL has no text form")),
    })
}

fn float_text(f: f64) -> String {
    if f.is_nan() {
        "NaN".into()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity" } else { "-Infinity" }.into()
    } else {
        format!("{f:?}")
    }
}

fn vector_text(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|f| format!("{f:?}")).collect();
    format!("[{}]", parts.join(","))
}

/// A PostgreSQL array literal (`{1,"a b",NULL,{2,3}}`).
fn array_text(items: &[Value]) -> Result<String> {
    let mut out = String::from("{");
    for (k, it) in items.iter().enumerate() {
        if k > 0 {
            out.push(',');
        }
        match it {
            Value::Null => out.push_str("NULL"),
            Value::Array(inner) => out.push_str(&array_text(inner)?),
            other => {
                let t = param_text(other)?;
                out.push('"');
                for c in t.chars() {
                    if c == '"' || c == '\\' {
                        out.push('\\');
                    }
                    out.push(c);
                }
                out.push('"');
            }
        }
    }
    out.push('}');
    Ok(out)
}

/// Encode parameter rows as one Arrow IPC stream (one column per parameter,
/// one row per parameter set). Every row must have `n_params` values. A
/// column's Arrow type is chosen from its non-NULL values; values of
/// different kinds in one column (other than NULL) are an error rather than a
/// silent conversion. An all-NULL column is sent as `Utf8` NULLs (the server
/// binds a NULL whatever the column type).
pub fn encode_params(rows: &[&[Value]], n_params: usize) -> Result<Vec<u8>> {
    let kinds = column_kinds(rows.iter().copied(), n_params)?;
    encode_params_with(rows, &kinds)
}

/// The Arrow kind of each of the `n_params` parameter columns over `rows`
/// (every row must have `n_params` values). Computed over ALL rows of a bulk
/// call before its first frame is sent, so a kind conflict in a late row can
/// never surface after earlier frames were applied.
pub(crate) fn column_kinds<'a>(
    rows: impl Iterator<Item = &'a [Value]>,
    n_params: usize,
) -> Result<Vec<Kind>> {
    let all: Vec<&[Value]> = rows.collect();
    let mut kinds: Vec<Option<Kind>> = vec![None; n_params];
    for (r, row) in all.iter().enumerate() {
        if row.len() != n_params {
            return Err(conv(format!(
                "parameter row {r} has {} value(s); the statement takes {n_params}",
                row.len()
            )));
        }
        for (c, v) in row.iter().enumerate() {
            let Some(k) = kind_of(v) else { continue };
            kinds[c] = Some(match (kinds[c], k) {
                (None, k) => k,
                (Some(a), b) if a == b => a,
                // Mixed numeric scales: widen to the larger scale.
                (Some(Kind::Decimal(a)), Kind::Decimal(b)) => Kind::Decimal(a.max(b)),
                // Any numeric that cannot be a Decimal128 sends the column as text.
                (Some(Kind::Decimal(_)), Kind::NumText)
                | (Some(Kind::NumText), Kind::Decimal(_)) => Kind::NumText,
                (Some(a), b) => {
                    return Err(conv(format!(
                        "parameter ${} mixes value kinds {a:?} and {b:?} (row {r})",
                        c + 1
                    )))
                }
            });
        }
    }
    let mut kinds: Vec<Kind> = kinds.into_iter().map(|k| k.unwrap_or(Kind::Text)).collect();
    // A Decimal column's widened scale must fit EVERY value (rescaling adds
    // digits); if one would overflow Decimal128 the whole column goes as exact
    // text. Decided here, before any frame is sent, never during a later chunk.
    for (c, kind) in kinds.iter_mut().enumerate() {
        if let Kind::Decimal(scale) = *kind {
            let fits = all.iter().all(|row| match &row[c] {
                Value::Numeric(s) => rescale(s, scale).is_some(),
                _ => true,
            });
            if !fits {
                *kind = Kind::NumText;
            }
        }
    }
    Ok(kinds)
}

/// `s` as a Decimal128 coefficient at `scale` (exact: only adds zeros), or
/// `None` when it does not fit 38 digits.
fn rescale(s: &str, scale: i8) -> Option<i128> {
    let (raw, sc) = numeric_to_i128(s)?;
    if sc > scale {
        return None;
    }
    let v = 10i128
        .checked_pow((scale - sc) as u32)
        .and_then(|m| raw.checked_mul(m))?;
    (v.unsigned_abs() < 10u128.pow(38)).then_some(v)
}

/// Encode `rows` with precomputed column kinds (see [`column_kinds`]).
pub(crate) fn encode_params_with(rows: &[&[Value]], kinds: &[Kind]) -> Result<Vec<u8>> {
    let n_params = kinds.len();
    for (r, row) in rows.iter().enumerate() {
        if row.len() != n_params {
            return Err(conv(format!(
                "parameter row {r} has {} value(s); the statement takes {n_params}",
                row.len()
            )));
        }
    }
    let mut fields = Vec::with_capacity(n_params);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(n_params);
    for (c, kind) in kinds.iter().copied().enumerate() {
        // A value whose kind differs from the column's (only possible when
        // the kinds were computed over other rows) is refused, never coerced.
        for (r, row) in rows.iter().enumerate() {
            if let Some(k) = kind_of(&row[c]) {
                let fits = k == kind
                    || matches!(
                        (kind, k),
                        (Kind::Decimal(_), Kind::Decimal(_)) | (Kind::NumText, Kind::Decimal(_))
                    );
                if !fits {
                    return Err(conv(format!(
                        "parameter ${} value kind {k:?} does not match its column kind {kind:?} (row {r})",
                        c + 1
                    )));
                }
            }
        }
        let name = format!("p{}", c + 1);
        let col = rows.iter().map(|row| &row[c]);
        let (dt, arr): (DataType, ArrayRef) = match kind {
            Kind::Bool => {
                let mut b = BooleanBuilder::with_capacity(rows.len());
                for v in col {
                    b.append_option(match v {
                        Value::Bool(x) => Some(*x),
                        _ => None,
                    });
                }
                (DataType::Boolean, Arc::new(b.finish()))
            }
            Kind::Int => {
                let mut b = Int64Builder::with_capacity(rows.len());
                for v in col {
                    b.append_option(match v {
                        Value::Int64(x) => Some(*x),
                        _ => None,
                    });
                }
                (DataType::Int64, Arc::new(b.finish()))
            }
            Kind::F32 => {
                let mut b = Float32Builder::with_capacity(rows.len());
                for v in col {
                    b.append_option(match v {
                        Value::Float32(x) => Some(*x),
                        _ => None,
                    });
                }
                (DataType::Float32, Arc::new(b.finish()))
            }
            Kind::F64 => {
                let mut b = Float64Builder::with_capacity(rows.len());
                for v in col {
                    b.append_option(match v {
                        Value::Float64(x) => Some(*x),
                        _ => None,
                    });
                }
                (DataType::Float64, Arc::new(b.finish()))
            }
            Kind::Bytes => {
                let mut b = BinaryBuilder::new();
                for v in col {
                    match v {
                        Value::Bytes(x) => b.append_value(x),
                        _ => b.append_null(),
                    }
                }
                (DataType::Binary, Arc::new(b.finish()))
            }
            Kind::Date => {
                let mut b = Date32Builder::with_capacity(rows.len());
                for v in col {
                    b.append_option(match v {
                        Value::Date(x) => Some(*x),
                        _ => None,
                    });
                }
                (DataType::Date32, Arc::new(b.finish()))
            }
            Kind::Decimal(scale) => {
                let mut b = Decimal128Builder::with_capacity(rows.len());
                for v in col {
                    match v {
                        Value::Numeric(s) => {
                            // `column_kinds` proved every value fits; a miss
                            // here is still refused, never truncated.
                            let v = rescale(s, scale).ok_or_else(|| {
                                conv(format!(
                                    "numeric {s:?} does not fit Decimal128 at scale {scale}"
                                ))
                            })?;
                            b.append_value(v);
                        }
                        _ => b.append_null(),
                    }
                }
                let dt = DataType::Decimal128(38, scale);
                let a = b
                    .finish()
                    .with_precision_and_scale(38, scale)
                    .map_err(|e| conv(e.to_string()))?;
                (dt, Arc::new(a))
            }
            Kind::Text | Kind::NumText => {
                let mut b = StringBuilder::new();
                for v in col {
                    match v {
                        Value::Null => b.append_null(),
                        other => b.append_value(param_text(other)?),
                    }
                }
                (DataType::Utf8, Arc::new(b.finish()))
            }
        };
        fields.push(Field::new(name, dt, true));
        arrays.push(arr);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = if n_params == 0 {
        RecordBatch::try_new_with_options(
            schema.clone(),
            arrays,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
    } else {
        RecordBatch::try_new(schema.clone(), arrays)
    }
    .map_err(|e| conv(format!("param batch: {e}")))?;
    let mut buf = Vec::new();
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| conv(format!("param IPC: {e}")))?;
        w.write(&batch)
            .map_err(|e| conv(format!("param IPC: {e}")))?;
        w.finish().map_err(|e| conv(format!("param IPC: {e}")))?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_round_trips() {
        for d in [
            -800_000i64,
            -1,
            0,
            1,
            59,
            60,
            365,
            10_957,
            20_468,
            2_932_896,
        ] {
            let (y, m, dd) = civil_from_days(d);
            assert_eq!(days_from_civil(y, m, dd), d, "{d}");
        }
        assert_eq!(format_date(0), "1970-01-01");
        assert_eq!(format_date(20_468), "2026-01-15");
        assert_eq!(parse_date("2026-01-15"), Some(20_468));
    }

    #[test]
    fn timestamp_text_round_trips() {
        let us = 1_768_481_045_123_456i64;
        let t = format_timestamp(us);
        assert_eq!(t, "2026-01-15 12:44:05.123456");
        assert_eq!(parse_timestamp(&t), Some((us, false)));
        assert_eq!(parse_timestamp(&format!("{t}+00")), Some((us, true)));
        assert_eq!(
            parse_timestamp("2026-01-15 14:44:05.123456+02:00"),
            Some((us, true))
        );
        assert_eq!(
            parse_timestamp("2026-01-15T12:44:05.123456Z"),
            Some((us, true))
        );
        assert_eq!(
            parse_timestamp("1969-12-31 23:59:59"),
            Some((-1_000_000, false))
        );
    }

    #[test]
    fn modifiers_do_not_drop_the_zone_qualifier() {
        assert_eq!(
            base_type("TIMESTAMP(6) WITH TIME ZONE"),
            "timestamp with time zone"
        );
        assert_eq!(base_type("NUMERIC(10,2)"), "numeric");
        let v = text_cell(
            "ARRAY['2026-01-15 12:00:00+00']",
            Some("timestamp(6) with time zone[]"),
        )
        .unwrap();
        let us = days_from_civil(2026, 1, 15) * MICROS_PER_DAY + 12 * 3_600_000_000;
        assert_eq!(v, Value::Array(vec![Value::TimestampTz(us)]));
        let v = text_cell(
            "ARRAY['2026-01-15 12:00:00']",
            Some("timestamp(3) without time zone[]"),
        )
        .unwrap();
        assert_eq!(v, Value::Array(vec![Value::Timestamp(us)]));
    }

    #[test]
    fn bc_dates_and_timestamps_round_trip() {
        // 0001-01-01 BC is astronomical year 0.
        let d = days_from_civil(0, 1, 1);
        assert_eq!(format_date(d), "0001-01-01 BC");
        assert_eq!(parse_date("0001-01-01 BC"), Some(d));
        let us = days_from_civil(-43, 3, 15) * MICROS_PER_DAY + 12 * 3_600_000_000;
        assert_eq!(timestamp_text(us, "+00"), "0044-03-15 12:00:00+00 BC");
        assert_eq!(
            parse_timestamp("0044-03-15 12:00:00+00 BC"),
            Some((us, true))
        );
        assert_eq!(parse_timestamp(&format_timestamp(us)), Some((us, false)));
        let v = text_cell("ARRAY['0001-01-01 BC','2026-01-15']", Some("date[]")).unwrap();
        assert_eq!(
            v,
            Value::Array(vec![Value::Date(d as i32), Value::Date(20_468)])
        );
    }

    #[test]
    fn pg_array_parses_quotes_nulls_and_nesting() {
        let v = pg_array_to_value(parse_pg_array(r#"{1,NULL,3}"#).unwrap(), "integer").unwrap();
        assert_eq!(
            v,
            Value::Array(vec![Value::Int64(1), Value::Null, Value::Int64(3)])
        );
        let v = pg_array_to_value(parse_pg_array(r#"{"a,b","c\"d",e}"#).unwrap(), "text").unwrap();
        assert_eq!(
            v,
            Value::Array(vec![
                Value::String("a,b".into()),
                Value::String("c\"d".into()),
                Value::String("e".into())
            ])
        );
        let v = pg_array_to_value(parse_pg_array("{{1,2},{3,4}}").unwrap(), "int8").unwrap();
        assert_eq!(
            v,
            Value::Array(vec![
                Value::Array(vec![Value::Int64(1), Value::Int64(2)]),
                Value::Array(vec![Value::Int64(3), Value::Int64(4)])
            ])
        );
        assert_eq!(parse_pg_array("{}"), Some(PgArr::List(vec![])));
        assert!(parse_pg_array("{1,2").is_none());
    }

    #[test]
    fn sql_array_form_parses() {
        let v = pg_array_to_value(parse_pg_array("ARRAY[1,NULL,3]").unwrap(), "integer").unwrap();
        assert_eq!(
            v,
            Value::Array(vec![Value::Int64(1), Value::Null, Value::Int64(3)])
        );
        let v = pg_array_to_value(
            parse_pg_array("ARRAY['a,b','c\"d','e''f',NULL]").unwrap(),
            "text",
        )
        .unwrap();
        assert_eq!(
            v,
            Value::Array(vec![
                Value::String("a,b".into()),
                Value::String("c\"d".into()),
                Value::String("e'f".into()),
                Value::Null
            ])
        );
        let v = pg_array_to_value(parse_pg_array("ARRAY[ARRAY[1,2],ARRAY[3]]").unwrap(), "int")
            .unwrap();
        assert_eq!(
            v,
            Value::Array(vec![
                Value::Array(vec![Value::Int64(1), Value::Int64(2)]),
                Value::Array(vec![Value::Int64(3)])
            ])
        );
        assert_eq!(parse_pg_array("ARRAY[]"), Some(PgArr::List(vec![])));
        assert!(parse_pg_array("ARRAY[1,2").is_none());
    }

    #[test]
    fn interval_text_round_trips() {
        assert_eq!(
            parse_interval("1 day 02:00:00"),
            Some((0, 1, 7_200_000_000))
        );
        assert_eq!(
            parse_interval("1 year 2 mons -3 days -04:05:06.5"),
            Some((14, -3, -14_706_500_000))
        );
        assert_eq!(parse_interval("00:00:00"), Some((0, 0, 0)));
        let t = interval_text(14, -3, -14_706_500_000);
        assert_eq!(parse_interval(&t), Some((14, -3, -14_706_500_000)));
        for m in [i64::MIN, i64::MAX, 0, 1, -1] {
            assert_eq!(
                parse_interval(&interval_text(0, 0, m)),
                Some((0, 0, m)),
                "{m}"
            );
        }
        assert!(parse_interval("bogus").is_none());
    }

    #[test]
    fn vector_bytes_decode() {
        let mut b = Vec::new();
        for f in [1.0f32, 2.5, -3.0] {
            b.extend_from_slice(&f.to_le_bytes());
        }
        assert_eq!(vector_from_le_bytes(&b).unwrap(), vec![1.0, 2.5, -3.0]);
        assert!(vector_from_le_bytes(&b[..5]).is_err());
    }

    #[test]
    fn array_text_round_trips_through_the_parser() {
        let v = Value::Array(vec![
            Value::String("a,b".into()),
            Value::Null,
            Value::String("q\"\\".into()),
        ]);
        let t = param_text(&v).unwrap();
        assert_eq!(
            pg_array_to_value(parse_pg_array(&t).unwrap(), "text").unwrap(),
            v
        );
    }

    #[test]
    fn numeric_digits() {
        assert_eq!(numeric_to_i128("123.4500"), Some((1_234_500, 4)));
        assert_eq!(numeric_to_i128("-0.01"), Some((-1, 2)));
        assert_eq!(numeric_to_i128("NaN"), None);
        assert_eq!(numeric_to_i128(&"9".repeat(39)), None);
        assert_eq!(decimal_text(1_234_500, 4), "123.4500");
        assert_eq!(decimal_text(-1, 2), "-0.01");
        assert_eq!(decimal_text(5, 0), "5");
        assert_eq!(decimal_text(5, -2), "500");
        // A Decimal256 coefficient beyond i128 keeps every digit.
        let big = format!("-{}", "9".repeat(70));
        assert_eq!(decimal_text_of(&big, 3), format!("-{}.999", "9".repeat(67)));
        let v = arrow_buffer::i256::from_string(&big).unwrap();
        assert_eq!(v.to_string(), big);
    }

    #[test]
    fn params_round_trip_through_arrow_ipc() {
        let rows: Vec<Vec<Value>> = vec![
            vec![
                Value::Int64(1),
                Value::String("x".into()),
                Value::Numeric("1.5".into()),
                Value::Null,
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Numeric("-20.25".into()),
                Value::Null,
            ],
        ];
        let refs: Vec<&[Value]> = rows.iter().map(|r| r.as_slice()).collect();
        let ipc = encode_params(&refs, 4).unwrap();
        let reader =
            arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None).unwrap();
        let schema = reader.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        let b = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(b.num_rows(), 2);
        assert_eq!(b.column(2).as_primitive::<Decimal128Type>().value(0), 150);
        assert!(b.column(0).is_null(1));
    }

    #[test]
    fn mixed_kinds_in_a_column_are_refused() {
        let rows = [vec![Value::Int64(1)], vec![Value::String("1".into())]];
        let refs: Vec<&[Value]> = rows.iter().map(|r| r.as_slice()).collect();
        assert!(encode_params(&refs, 1).is_err());
    }

    #[test]
    fn unrepresentable_numeric_sends_the_column_as_text() {
        let rows = [
            vec![Value::Numeric("1.5".into())],
            vec![Value::Numeric("NaN".into())],
        ];
        let refs: Vec<&[Value]> = rows.iter().map(|r| r.as_slice()).collect();
        let ipc = encode_params(&refs, 1).unwrap();
        let reader =
            arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None).unwrap();
        assert_eq!(reader.schema().field(0).data_type(), &DataType::Utf8);
        let b = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(b.column(0).as_string::<i32>().value(0), "1.5");
        assert_eq!(b.column(0).as_string::<i32>().value(1), "NaN");
    }

    #[test]
    fn widened_scale_that_overflows_sends_the_column_as_text() {
        let big = "9".repeat(38);
        let rows = [
            vec![Value::Numeric("0.1".into())],
            vec![Value::Numeric(big.clone())],
        ];
        let kinds = column_kinds(rows.iter().map(|r| r.as_slice()), 1).unwrap();
        assert_eq!(kinds, vec![Kind::NumText]);
        // Encoding the LAST row alone (a later chunk) with those kinds works.
        let tail: Vec<&[Value]> = vec![rows[1].as_slice()];
        assert!(encode_params_with(&tail, &kinds).is_ok());
    }

    #[test]
    fn bytea_array_elements_decode_as_bytes() {
        let v = text_cell("ARRAY[X'0102',NULL,X'']", Some("bytea[]")).unwrap();
        assert_eq!(
            v,
            Value::Array(vec![
                Value::Bytes(vec![1, 2]),
                Value::Null,
                Value::Bytes(vec![])
            ])
        );
        assert!(text_cell("ARRAY[X'010']", Some("bytea[]")).is_err());
    }

    #[test]
    fn kinds_are_decided_over_all_rows_before_any_chunk() {
        let rows = [
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
            vec![Value::Bool(true)],
        ];
        assert!(column_kinds(rows.iter().map(|r| r.as_slice()), 1).is_err());
        let kinds = column_kinds(rows[..2].iter().map(|r| r.as_slice()), 1).unwrap();
        let tail: Vec<&[Value]> = vec![rows[2].as_slice()];
        assert!(
            encode_params_with(&tail, &kinds).is_err(),
            "a row of another kind is refused"
        );
    }

    #[test]
    fn decl_typed_text_cells() {
        assert_eq!(
            text_cell("12.30", Some("numeric")).unwrap(),
            Value::Numeric("12.30".into())
        );
        assert_eq!(
            text_cell("{\"a\":1}", Some("jsonb")).unwrap(),
            Value::Json("{\"a\":1}".into())
        );
        assert_eq!(
            text_cell("[1,2.5]", Some("vector(2)")).unwrap(),
            Value::Vector(vec![1.0, 2.5])
        );
        assert_eq!(
            text_cell("{t,f}", Some("boolean[]")).unwrap(),
            Value::Array(vec![Value::Bool(true), Value::Bool(false)])
        );
        assert_eq!(
            text_cell("plain", None).unwrap(),
            Value::String("plain".into())
        );
    }
}
