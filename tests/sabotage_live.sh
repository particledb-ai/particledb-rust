#!/usr/bin/env bash
# Sabotage check for tests/native_wire_live.rs: prove each live test can FAIL.
#
# Starts one throwaway server (as run_live.sh does), then for each
# mutation copies the crate to a temp dir, applies ONE source edit there (the
# edit must match exactly once -- a mutation that no longer applies is a red
# result, not a silent pass), runs the named live test, and requires it to
# FAIL. An unmutated copy must PASS the same tests first (positive control).
# The working tree is never edited.
#
#   PARTICLEDB_BIN=/path/to/particledb tests/sabotage_live.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# The server binary: $PARTICLEDB_BIN, else `particledb` on PATH.
BIN="${PARTICLEDB_BIN:-$(command -v particledb || true)}"
[ -n "$BIN" ] && [ -x "$BIN" ] || { echo "sabotage: no server binary (set PARTICLEDB_BIN or put particledb on PATH)" >&2; exit 2; }
BASE="${PDB_LIVE_BASE_PORT:-$(( 42000 + (RANDOM % 1000) * 10 ))}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pdb-sdk-sabotage.XXXXXX")"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$WORK/target}"

"$BIN" start --no-auth --data-dir "$WORK/data" \
  --pdb-addr "127.0.0.1:$BASE" --pg-addr "127.0.0.1:$((BASE + 1))" \
  --grpc-addr "127.0.0.1:$((BASE + 2))" --http-addr "127.0.0.1:$((BASE + 3))" \
  --metrics-addr "127.0.0.1:$((BASE + 4))" --health-port "$((BASE + 5))" \
  --redis-addr "127.0.0.1:$((BASE + 6))" >"$WORK/server.log" 2>&1 &
SRV=$!
trap 'kill "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true' EXIT INT TERM
for _ in $(seq 1 240); do
  kill -0 "$SRV" 2>/dev/null || { echo "sabotage: server died; $WORK/server.log" >&2; exit 2; }
  (exec 3<>"/dev/tcp/127.0.0.1/$BASE") 2>/dev/null && break
  sleep 0.5
done

# Mutation name -> the live test that must fail under it. The edits
# themselves are in mutate() below.
MUTATIONS=(
  "rows-dropped every_type_round_trips"
  "params-ignored parameters_bind_and_never_splice"
  "decimal-scale every_type_round_trips"
  "vector-bytes every_type_round_trips"
  "batch-first-chunk-only execute_many_inserts_100k_rows"
  "sqlstate-lost errors_carry_sqlstate"
  "failed-rows-hidden execute_many_reports_failed_rows"
)

mutate() { # crate-dir mutation-name
  python3 -I - "$1" "$2" <<'PY'
import sys
root, name = sys.argv[1], sys.argv[2]
EX, CODEC = "src/pdb_wire_executor.rs", "src/arrow_codec.rs"
M = {
    "rows-dropped": (EX,
        "let r = arrow_codec::decode_result(&data);",
        "let r = arrow_codec::decode_result(&data).map(|mut q| { q.rows.clear(); q });"),
    "params-ignored": (EX,
        "let body = arrow_codec::encode_params(&[params], n as usize)?;",
        "let nulls: Vec<Value> = params.iter().map(|_| Value::Null).collect();\n"
        "            let body = arrow_codec::encode_params(&[&nulls[..]], n as usize)?;"),
    "decimal-scale": (CODEC,
        "Value::Numeric(decimal_text(array.as_primitive::<Decimal128Type>().value(i), *s))",
        "Value::Numeric(decimal_text(array.as_primitive::<Decimal128Type>().value(i), *s - 1))"),
    "vector-bytes": (CODEC,
        "f32::from_le_bytes([c[0], c[1], c[2], c[3]])",
        "f32::from_be_bytes([c[0], c[1], c[2], c[3]])"),
    "batch-first-chunk-only": (EX,
        "        for chunk in chunk_rows(rows)? {\n"
        "            let refs: Vec<&[Value]> = chunk.iter().map(|r| r.as_slice()).collect();\n"
        "            let data = match",
        "        for chunk in chunk_rows(rows)?.into_iter().take(1) {\n"
        "            let refs: Vec<&[Value]> = chunk.iter().map(|r| r.as_slice()).collect();\n"
        "            let data = match"),
    "sqlstate-lost": (EX,
        "    if code >= 10000 {\n        code.to_string()",
        "    if code >= 10000 {\n        \"HY000\".to_string()"),
    "failed-rows-hidden": (EX,
        "        if acc.failed > 0 {\n            return Err(Error::Batch(acc));\n        }\n        Ok(acc.applied)\n    }\n}",
        "        Ok(acc.applied)\n    }\n}"),
}
path, old, new = M[name]
path = f"{root}/{path}"
s = open(path).read()
n = s.count(old)
if n != 1:
    sys.exit(f"mutation {name}: text found {n} times in {path} (must be exactly 1)")
open(path, "w").write(s.replace(old, new))
PY
}

copy_crate() {
  local dst="$1"
  mkdir -p "$dst"
  cp -R "$HERE/src" "$HERE/tests" "$HERE/Cargo.toml" "$HERE/Cargo.lock" "$dst/"
}

run_test() { # dir filter -> rc
  ( cd "$1" && PDB_NATIVE_PORT="$BASE" cargo test -q --test native_wire_live "$2" -- --nocapture --test-threads=1 ) >"$1.out" 2>&1
}

# Positive control: the unmutated crate passes every targeted test.
copy_crate "$WORK/base"
if ! run_test "$WORK/base" ""; then
  echo "sabotage: BASELINE FAILED (see $WORK/base.out)" >&2; exit 1
fi
for m in "${MUTATIONS[@]}"; do
  t="${m##* }"
  grep -q "NWL_PASS $t" "$WORK/base.out" || { echo "sabotage: baseline $t did not run" >&2; exit 1; }
done
echo "sabotage: baseline passes all targeted tests"

bad=0
i=0
for m in "${MUTATIONS[@]}"; do
  i=$((i + 1))
  name="${m%% *}"; test="${m##* }"
  dir="$WORK/m$i"
  copy_crate "$dir"
  if ! mutate "$dir" "$name"; then
    echo "sabotage: $name no longer applies (red: update the mutation)"; bad=1; continue
  fi
  if run_test "$dir" "$test"; then
    echo "sabotage: $name -> $test PASSED under the mutation (test is blind)"; bad=1
  elif grep -q "error\[E" "$dir.out"; then
    echo "sabotage: $name did not compile (proves nothing; see $dir.out)"; bad=1
  elif ! grep -qx "    $test" "$dir.out"; then
    echo "sabotage: $name failed, but not as a failure of $test (see $dir.out)"; bad=1
  else
    echo "sabotage: $name -> $test FAILED as required"
  fi
done
[ "$bad" -eq 0 ] && { echo "sabotage: PASS (${#MUTATIONS[@]} mutations, each caught)"; rm -rf "$WORK"; exit 0; }
echo "sabotage: FAIL (work dir kept: $WORK)"; exit 1
