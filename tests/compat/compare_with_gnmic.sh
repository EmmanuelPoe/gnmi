#!/usr/bin/env bash
# Runs the reference Go gnmic (Docker) and gnmic-rs against the same
# deterministic dial-out simulator and diffs their file outputs.
#
# Usage: tests/compat/compare_with_gnmic.sh   (needs docker; builds with cargo)
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
IMAGE=${GNMIC_IMAGE:-ghcr.io/openconfig/gnmic:latest}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/gnmic-compat.XXXX")
GO_PORT=57491
RS_PORT=57492

cargo build --quiet --manifest-path "$ROOT/Cargo.toml" --bins
BIN="$ROOT/target/debug"

write_config() { # $1=dir $2=listen address
  cat > "$1/cfg.yaml" <<YAML
insecure: true
username: admin
password: secret
encoding: json_ietf
tunnel-server:
  address: "$2"
subscriptions:
  counters:
    paths: [/interfaces]
    stream-mode: sample
    sample-interval: 1s
  typed:
    paths: ["openconfig:/interfaces"]
    stream-mode: sample
    sample-interval: 1s
    encoding: proto
  legacy-json:
    prefix: /system
    paths: [/cpu]
    stream-mode: sample
    sample-interval: 1s
    encoding: json
    target: device-x
outputs:
  json:
    type: file
    filename: $3/json.log
  json-multiline:
    type: file
    filename: $3/json-multiline.log
    multiline: true
  event:
    type: file
    filename: $3/event.log
    format: event
  event-split:
    type: file
    filename: $3/event-split.log
    format: event
    split-events: true
  event-add-target:
    type: file
    filename: $3/event-add-target.log
    format: event
    add-target: if-not-present
YAML
}

mkdir -p "$WORK/go" "$WORK/rs"
chmod 777 "$WORK/go"
write_config "$WORK/go" ":57400" "/work"
write_config "$WORK/rs" "127.0.0.1:$RS_PORT" "$WORK/rs"

SIM_ARGS=(--targets 2 --interfaces 3 --count 3 --deterministic --username admin --password secret --interval-ms 200)

echo "== Go gnmic ($IMAGE)"
CID=$(docker run -d --rm -e TZ=UTC -p "$GO_PORT:57400" -v "$WORK/go:/work" "$IMAGE" \
  --config /work/cfg.yaml --use-tunnel-server subscribe)
sleep 2
"$BIN/dialout-sim" --server "127.0.0.1:$GO_PORT" "${SIM_ARGS[@]}" >"$WORK/go/sim.log" 2>&1 &
SIM_GO=$!

echo "== gnmic-rs"
TZ=UTC "$BIN/gnmic-rs" --config "$WORK/rs/cfg.yaml" >"$WORK/rs/collector.log" 2>&1 &
RS=$!
sleep 1
"$BIN/dialout-sim" --server "127.0.0.1:$RS_PORT" "${SIM_ARGS[@]}" >"$WORK/rs/sim.log" 2>&1 &
SIM_RS=$!

sleep 6
kill "$SIM_GO" "$SIM_RS" 2>/dev/null || true
docker stop "$CID" >/dev/null
kill -TERM "$RS"; wait "$RS" || true

status=0
for f in json json-multiline event event-split event-add-target; do
  # Records arrive in a different order across targets/subscriptions: compare
  # sorted records (multiline records are joined first).
  norm() { if [ "$f" = json-multiline ]; then tr -d '\n' <"$1" | sed 's/}{/}\n{/g'; else cat "$1"; fi | sort; }
  if diff <(norm "$WORK/go/$f.log") <(norm "$WORK/rs/$f.log") >"$WORK/$f.diff"; then
    echo "IDENTICAL  $f ($(wc -l <"$WORK/rs/$f.log" | tr -d ' ') lines)"
  else
    echo "DIFFERENT  $f (see $WORK/$f.diff)"
    status=1
  fi
done
echo "work dir: $WORK"
exit $status
