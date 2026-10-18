#!/usr/bin/env bash
# Throughput / resource comparison: Go gnmic vs gnmic-rs, both in Docker with
# the same CPU limit, fed by the dialout-sim container on a private network.
#
# Usage: tests/bench/bench.sh [format] [cpus] [duration_s] [sim args...]
#   format    json | event        (default event)
#   cpus      collector CPU limit (default 1)
#   duration  measured seconds    (default 30)
#   sim args  load shape (default: 40 targets x 20 interfaces, unthrottled)
set -euo pipefail

FORMAT=${1:-event}
CPUS=${2:-1}
DURATION=${3:-30}
shift $(( $# > 3 ? 3 : $# )) || true
SIM_ARGS=("$@")
if [ ${#SIM_ARGS[@]} -eq 0 ]; then
  SIM_ARGS=(--targets 40 --interfaces 20 --burst 50 --interval-ms 10)
fi
RS_IMAGE=${RS_IMAGE:-gnmic-rs:dev}
GO_IMAGE=${GO_IMAGE:-ghcr.io/openconfig/gnmic:latest}
NET=gnmic-bench
CFG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/gnmic-bench.XXXX")

cat > "$CFG_DIR/cfg.yaml" <<YAML
insecure: true
encoding: json_ietf
tunnel-server:
  address: ":57400"
subscriptions:
  counters:
    paths: [/interfaces]
    stream-mode: sample
    sample-interval: 1s
outputs:
  file:
    type: file
    filename: /data/out.log
    format: $FORMAT
YAML
chmod -R a+rX "$CFG_DIR"

docker network create "$NET" >/dev/null 2>&1 || true

run() { # $1=label $2=image $3...=collector args
  local label=$1 image=$2; shift 2
  docker rm -f bench-collector bench-sim >/dev/null 2>&1 || true
  docker volume rm -f bench-data >/dev/null 2>&1 || true
  docker volume create bench-data >/dev/null
  docker run --rm -v bench-data:/data alpine chmod 777 /data >/dev/null
  docker run -d --name bench-collector --network "$NET" --cpus "$CPUS" \
    -v "$CFG_DIR:/cfg:ro" -v bench-data:/data "$image" "$@" >/dev/null
  sleep 2
  docker run -d --name bench-sim --network "$NET" --cpus 6 --entrypoint /usr/local/bin/dialout-sim \
    "$RS_IMAGE" --server bench-collector:57400 "${SIM_ARGS[@]}" >/dev/null
  sleep 5 # warm-up: registrations, subscriptions
  # File size, not line counts: a fast writer can outrun `wc` forever.
  size() { docker run --rm -v bench-data:/data alpine stat -c %s /data/out.log; }
  local start_bytes; start_bytes=$(size)
  local cpu_sum=0 mem_max=0 n=0 t0; t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt "$DURATION" ]; do
    read -r cpu mem < <(docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' bench-collector \
      | awk '{gsub("%","",$1); m=$2; u=m; gsub(/[0-9.]/,"",u); gsub(/[A-Za-z]/,"",m);
              if (u=="GiB") m*=1024; if (u=="KiB") m/=1024; print $1, m}')
    cpu_sum=$(echo "$cpu_sum + $cpu" | bc); n=$((n+1))
    mem_max=$(echo "if ($mem > $mem_max) $mem else $mem_max" | bc)
  done
  local end_bytes; end_bytes=$(size)
  local elapsed=$(( $(date +%s) - t0 ))
  docker rm -f bench-sim >/dev/null
  local avg; avg=$(docker run --rm -v bench-data:/data alpine sh -c 'head -n 2000 /data/out.log | wc -c')
  local rate=$(( (end_bytes - start_bytes) * 2000 / avg / elapsed ))
  local mbps=$(( (end_bytes - start_bytes) / elapsed / 1000000 ))
  printf "%-10s format=%-5s cpus=%s  records/s=%-8s (%4s MB/s)  avg CPU=%6.1f%%  max RSS=%7.1f MiB\n" \
    "$label" "$FORMAT" "$CPUS" "$rate" "$mbps" "$(echo "$cpu_sum / $n" | bc -l)" "$mem_max"
  docker rm -f bench-collector >/dev/null
  docker volume rm -f bench-data >/dev/null
}

run "gnmic(go)" "$GO_IMAGE" --config /cfg/cfg.yaml --use-tunnel-server subscribe
run "gnmic-rs" "$RS_IMAGE" --config /cfg/cfg.yaml
docker volume rm -f bench-data >/dev/null
