#!/usr/bin/env bash
# Start a throwaway ParticleDB on unique ports (fresh data dir,
# --no-auth, default durability mode) and run the SDK's live native-wire tests
# against it. Exit status is the tests' status; the server is stopped by PID.
#
#   PARTICLEDB_BIN=/path/to/particledb tests/run_live.sh [extra cargo test args]
#
# PARTICLEDB_BIN defaults to `particledb` on PATH. PDB_LIVE_BASE_PORT
# picks the port block (default: random in 42000-51999); 7 consecutive ports
# are used by the --no-auth server, BASE+10.. by a second server that requires
# authentication (scram-sha-256, a random password). Runs
# tests/native_wire_live.rs, tests/native_batch_errors.rs and
# tests/native_wire_auth.rs. Each prints its marker (`NWL_PASS <name>`,
# `NBE_PASS rust`, `NWA_PASS`); the script
# fails unless every one printed its marker (a skipped test is not a pass).
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
# The server binary: $PARTICLEDB_BIN, else `particledb` on PATH.
BIN="${PARTICLEDB_BIN:-$(command -v particledb || true)}"
[ -n "$BIN" ] && [ -x "$BIN" ] || { echo "run_live: no server binary (set PARTICLEDB_BIN or put particledb on PATH: curl -fsSL https://get.particledb.ai | bash)" >&2; exit 2; }

BASE="${PDB_LIVE_BASE_PORT:-$(( 42000 + (RANDOM % 1000) * 10 ))}"
PDB_PORT=$BASE
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pdb-sdk-live.XXXXXX")"
LOG="$WORK/server.log"

"$BIN" start --no-auth \
  --data-dir "$WORK/data" \
  --pdb-addr "127.0.0.1:$PDB_PORT" \
  --pg-addr "127.0.0.1:$((BASE + 1))" \
  --grpc-addr "127.0.0.1:$((BASE + 2))" \
  --http-addr "127.0.0.1:$((BASE + 3))" \
  --metrics-addr "127.0.0.1:$((BASE + 4))" \
  --health-port "$((BASE + 5))" \
  --redis-addr "127.0.0.1:$((BASE + 6))" \
  >"$LOG" 2>&1 &
SRV=$!
# A second server that REQUIRES authentication (native-wire MSG_AUTH).
AUTH_PORT=$((BASE + 10))
AUTH_USER=sdk_live
AUTH_PASS="pw-$RANDOM-$RANDOM"
"$BIN" start --auth-method scram-sha-256 --pg-username "$AUTH_USER" --pg-password "$AUTH_PASS" \
  --data-dir "$WORK/data-auth" \
  --pdb-addr "127.0.0.1:$AUTH_PORT" \
  --pg-addr "127.0.0.1:$((AUTH_PORT + 1))" \
  --grpc-addr "127.0.0.1:$((AUTH_PORT + 2))" \
  --http-addr "127.0.0.1:$((AUTH_PORT + 3))" \
  --metrics-addr "127.0.0.1:$((AUTH_PORT + 4))" \
  --health-port "$((AUTH_PORT + 5))" \
  --redis-addr "127.0.0.1:$((AUTH_PORT + 6))" \
  >"$WORK/server-auth.log" 2>&1 &
SRV_AUTH=$!
cleanup() {
  kill "$SRV" "$SRV_AUTH" 2>/dev/null || true
  wait "$SRV" "$SRV_AUTH" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Ready when each native port accepts a connection (bounded: 120 s each).
wait_port() { # pid port log
  for _ in $(seq 1 240); do
    if ! kill -0 "$1" 2>/dev/null; then
      echo "run_live: server exited during startup; log: $3" >&2; tail -20 "$3" >&2; exit 2
    fi
    if (exec 3<>"/dev/tcp/127.0.0.1/$2") 2>/dev/null; then return 0; fi
    sleep 0.5
  done
  echo "run_live: port $2 never opened; log: $3" >&2; exit 2
}
wait_port "$SRV" "$PDB_PORT" "$LOG"
wait_port "$SRV_AUTH" "$AUTH_PORT" "$WORK/server-auth.log"

OUT="$WORK/test.out"
set +e
( cd "$HERE" && PDB_NATIVE_PORT="$PDB_PORT" PDB_AUTH_PORT="$AUTH_PORT" PDB_AUTH_USER="$AUTH_USER" PDB_AUTH_PASS="$AUTH_PASS" \
    cargo test --test native_wire_live --test native_batch_errors --test native_wire_auth "$@" -- --nocapture --test-threads=1 ) 2>&1 | tee "$OUT"
RC=${PIPESTATUS[0]}
set -e

want=$(grep -B1 '^fn ' "$HERE/tests/native_wire_live.rs" | grep -c '#\[test\]' | tr -d ' ')
# --nocapture interleaves `test x ... ` and the marker on one line.
got=$(grep -o 'NWL_PASS [a-z_0-9]*' "$OUT" | sort -u | wc -l | tr -d ' ')
nbe=$(grep -c 'NBE_PASS rust' "$OUT" | tr -d ' ')
nwa=$(grep -c 'NWA_PASS' "$OUT" | tr -d ' ')
echo "run_live: rc=$RC NWL_PASS=$got/$want NBE_PASS=$nbe NWA_PASS=$nwa port=$PDB_PORT log=$LOG"
if [ "$RC" -ne 0 ] || [ "$got" -ne "$want" ] || [ "$nbe" -lt 1 ] || [ "$nwa" -lt 1 ]; then
  echo "run_live: FAIL (server log kept: $LOG)" >&2
  exit 1
fi
rm -rf "$WORK"
echo "run_live: PASS"
