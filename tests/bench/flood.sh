#!/usr/bin/env bash
# Reconnect-flood test: N devices (one TLS connection each) dial the collector
# at once. Runs without admission control, then with the given limits.
#
# Usage: tests/bench/flood.sh [devices] [admission yaml (flow style)]
#   IFACES=200 SIM_EXTRA="--burst 5" for a heavier initial dump per device.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
N=${1:-3000}
LIMITS=${2:-"{registration-rate: 300, registration-burst: 300, max-concurrent-setups: 100, max-concurrent-handshakes: 64}"}
cargo build --quiet --release --manifest-path "$ROOT/Cargo.toml" --bins
BIN="$ROOT/target/release"
ulimit -n 65536 2>/dev/null || true

run() { # $1=label $2=admission yaml
  local dir; dir=$(mktemp -d "${TMPDIR:-/tmp}/gnmic-flood.XXXX")
  cat > "$dir/cfg.yaml" <<YAML
insecure: true
retry: 2s
tunnel-server:
  address: 127.0.0.1:57499
  tls: {}
  admission: $2
subscriptions:
  counters: {paths: [/interfaces], stream-mode: sample, sample-interval: 10s}
outputs:
  f: {type: file, filename: /dev/null, format: event}
YAML
  "$BIN/gnmic-rs" --config "$dir/cfg.yaml" >"$dir/collector.log" 2>&1 &
  local cp=$!
  sleep 1
  "$BIN/dialout-sim" --server 127.0.0.1:57499 --tls --connection-per-target --targets "$N" \
    --interfaces ${IFACES:-20} --initial-delay-ms 300 --interval-ms 60000 ${SIM_EXTRA:-} >"$dir/sim.log" 2>&1 &
  local sp=$! t0; t0=$(date +%s) peak=0 cpu_peak=0
  while :; do
    local synced; synced=$(grep -c 'initial sync complete' "$dir/collector.log" || true)
    read -r rss cpu < <(ps -o rss=,%cpu= -p "$cp")
    (( rss > peak )) && peak=$rss
    cpu_peak=$(echo "if ($cpu > $cpu_peak) $cpu else $cpu_peak" | bc)
    local elapsed=$(( $(date +%s) - t0 ))
    if (( synced >= N )) || (( elapsed > 180 )); then break; fi
    sleep 0.5
  done
  printf "%-10s devices synced=%-5s time=%3ss  peak RSS=%5d MiB  peak CPU=%5.0f%%  errors=%s\n" \
    "$1" "$synced" "$elapsed" $((peak / 1024)) "$cpu_peak" \
    "$(grep -E 'ERROR|WARN' "$dir/collector.log" | grep -vc 'self-signed' || true)"
  kill "$sp" 2>/dev/null || true
  kill -TERM "$cp"; wait "$cp" 2>/dev/null || true
}

run "unlimited" "{max-concurrent-handshakes: 0, max-concurrent-setups: 0}"
run "admission" "$LIMITS"
