# gnmic-rs

A Rust re-implementation of [gnmic](https://github.com/openconfig/gnmic)'s
**gNMI dial-out collector**: an embedded
[openconfig/grpctunnel](https://github.com/openconfig/grpctunnel) server that
devices (e.g. Cisco IOS XE) dial into, a gNMI client that subscribes to each
registered device through its tunnel, and outputs to **files** (plain or
rotated + gzip) and **PostgreSQL**.

It reads gnmic's configuration format and writes **byte-identical** `json` and
`event` output (verified against gnmic 0.49.0, see [Testing](#testing)), using
about half the CPU and a fraction of the memory.

## Build

Only a Rust toolchain (1.88+) is needed — protobuf definitions are compiled
by a pure-Rust compiler, no `protoc` or Go:

```sh
cargo build --release          # target/release/gnmic-rs (and dialout-sim, the test device)
docker build -t gnmic-rs .     # 63 MB distroless image, runs as non-root
```

## Run

```sh
gnmic-rs --config gnmic.yaml           # also accepts gnmic's: --use-tunnel-server subscribe
gnmic-rs --config gnmic.yaml --check   # validate configuration and certificates, then exit
gnmic-rs --config gnmic.yaml --skip-verify   # accept self-signed device certificates (like gnmic)
```

`SIGTERM`/`SIGINT` stop accepting devices, end subscriptions, and drain and flush
every output before exiting. Logs go to stderr (`--log-file`, `--log-format json`,
`--debug`; `RUST_LOG` overrides the filter).

Deployment: [`deploy/docker-compose.yml`](deploy/docker-compose.yml) (gnmic-rs +
PostgreSQL; `--profile test` adds three simulated devices) or the systemd unit
[`deploy/gnmic-rs.service`](deploy/gnmic-rs.service).

## Configuration

[`examples/gnmic.yaml`](examples/gnmic.yaml) documents every option. A gnmic
tunnel-server config works as is; a minimal one:

```yaml
username: admin
password: ${GNMI_PASSWORD}
insecure: true                  # gNMI inside the tunnel is plaintext

tunnel-server:
  address: ":57400"
  tls:                          # device -> collector TLS
    cert-file: /etc/gnmic-rs/server.pem
    key-file: /etc/gnmic-rs/server.key
    ca-file: /etc/gnmic-rs/devices-ca.pem   # devices must present a cert from this CA

subscriptions:
  interfaces:
    paths: [/interfaces/interface/state/counters]
    stream-mode: sample
    sample-interval: 30s

outputs:
  archive:
    type: file
    filename: /var/lib/gnmic-rs/telemetry.json
    format: event
    rotation: {max-size: 100, max-backups: 10, compress: true}
```

Supported, with gnmic semantics:

| Section | Keys |
|---|---|
| global (target defaults) | `username`, `password`, `token`, `insecure`, `skip-verify`, `tls-ca`, `tls-cert`, `tls-key`, `tls-server-name`, `encoding`, `timeout`, `retry`, `gzip`, `metadata`, `max-msg-size`, `log`, `log-file`, `log-format`, `debug` |
| `tunnel-server` | `address`, `tls` (`ca-file`, `cert-file`, `key-file`, `client-auth`), `targets` (`id`/`type` regex rules with per-target `config`: the target keys above plus `subscriptions`, `outputs`, `event-tags`, `retry-timer`) |
| `subscriptions` | `paths`, `prefix`, `target`, `set-target`, `mode` (stream, once), `stream-mode`, `sample-interval`, `heartbeat-interval`, `suppress-redundant`, `updates-only`, `encoding`, `qos`, `models`, `depth`, `stream-subscriptions`, `outputs` |
| `outputs` type `file` | `filename`, `file-type` (stdout, stderr), `format` (json, event, protojson, prototext), `multiline`, `indent`, `separator`, `split-events`, `add-target`, `override-timestamps`, `calculate-latency`, `rotation` (`max-size`, `max-backups`, `max-age`, `compress`) |
| `outputs` type `postgres` | see below |

Environment variables (`$VAR`, `${VAR}`) are expanded in string values;
passwords only when they start with `$`, as in gnmic. Unknown top-level keys
are logged and ignored; unknown keys inside an output are rejected as typos.

### Self-signed certificates

As in gnmic, `skip-verify: true` (config, per target rule, or the
`--skip-verify` flag) accepts device certificates that cannot be verified,
such as self-signed ones, for gNMI inside the tunnel; `insecure: true` (or
`--insecure`) uses plaintext. A `tunnel-server.tls` section without
`cert-file`/`key-file` makes the collector present a generated self-signed
certificate to devices. For Postgres, `tls.mode: require` encrypts without
verifying the server certificate.

### Reconnect floods (admission control)

gnmic has no protection when thousands of devices reconnect at once (after a
collector restart or a network event). `tunnel-server.admission` paces the
flood at four stages:

| Stage | Setting | When over the limit |
|---|---|---|
| Connections | `max-connections` | Shed: HTTP/2 GOAWAY on cleartext listeners (gRPC clients reconnect with backoff); TCP close on TLS listeners, before the costly handshake |
| TLS handshakes | `max-concurrent-handshakes` (default 128) | Queued |
| Registrations | `registration-rate`, `registration-burst` | Queued in arrival order up to `registration-queue-timeout`, then refused with `RESOURCE_EXHAUSTED` (and a `grpc-retry-pushback-ms` hint) |
| Target setups | `max-concurrent-setups` (default 256), `setup-timeout` | Queued: only N targets dial and receive their initial sync at once; a slot frees at the first sync-response |

While limits are active, a summary is logged every 10 s
(`admission control active shed_connections=… shed_registrations=…
waiting_setups=…`), and each subscription logs `initial sync complete`.

`tests/bench/flood.sh` reconnects 3,000 devices at once (one TLS connection
each). With a heavy initial dump (200 interfaces × 5 samples per device),
unlimited admission saturates the 8-core host (peaks of 766–881% CPU over
two runs); with `registration-rate: 300, max-concurrent-setups: 100,
max-concurrent-handshakes: 64` the peak stays around 200–219% CPU and all
3,000 devices are synced within 10 s. Memory is set by the number of open connections
(about 200 KB each), not by the flood.

### PostgreSQL output

Events are converted to rows and bulk-loaded with binary `COPY`. The table
layout is defined by `columns`; each column has a PostgreSQL `type` and a
`source`:

| source | value |
|---|---|
| `timestamp` | notification timestamp (`timestamptz`/`timestamp`, or nanoseconds in `bigint`) |
| `now` | insertion time |
| `name` | subscription name |
| `path`, `value` | leaf path and value — `row-mode: value` (default): one row per leaf |
| `value:<leaf>` | a leaf by exact path or path suffix (`value:in-octets`) — `row-mode: event`: one row per event |
| `values` | all leaf values of the row as an object |
| `tags`, `tag:<name>` | all tags as an object, or one tag (`tag:source` is the target, `tag:interface_name` a path key) |

Types: `text`, `varchar`, `bigint`, `integer`, `smallint`, `double precision`,
`real`, `boolean`, `jsonb`, `json`, `timestamptz`, `timestamp`. Values are
coerced to the column type (e.g. RFC 7951 string counters into `bigint`), and
become `NULL` when they cannot be. Without `columns`, the table is
`(time timestamptz, target text, subscription text, path text, value jsonb, tags jsonb)`.

```yaml
outputs:
  counters:
    type: postgres
    url: postgres://gnmic:${PG_PASSWORD}@db:5432/telemetry   # or host/port/database/username/password
    tls: {mode: verify-full, ca-file: /etc/gnmic-rs/pg-ca.pem}  # disable | require | verify-full
    table: interface_counters
    create-table: true             # CREATE TABLE IF NOT EXISTS on every (re)connect
    row-mode: event
    columns:
      - {name: time,      type: timestamptz, source: timestamp}
      - {name: device,    type: text,        source: "tag:source"}
      - {name: interface, type: text,        source: "tag:interface_name"}
      - {name: in_octets, type: bigint,      source: "value:in-octets"}
    batch-size: 1000               # rows per COPY
    flush-interval: 1s
    max-buffered-rows: 100000      # memory bound while the database is down
```

**Slow or unavailable database.** Each message is converted to rows by the
subscription that received it, then passes through two buffers:

1. the output queue (`buffer-size` messages, default 1024), drained
   continuously into
2. an in-memory row buffer (`max-buffered-rows`, default 100,000), from which
   a separate writer task sends batches of `batch-size` rows with binary COPY,
   at least every `flush-interval`.

When Postgres is slower than the incoming data, the row buffer grows. When
it is unreachable, rows accumulate while writes are retried with exponential
backoff (1 s up to 30 s, 60 s timeout per COPY); a failed batch is put back
in order. When the buffer is full, `buffer-full` decides:

- `drop-oldest` (default): the oldest rows are dropped and counted in an
  error log. Collection and the other outputs are never slowed down.
- `block`: the output stops taking messages, so backpressure slows the
  subscriptions feeding it (and their other outputs) down to the database's
  pace; nothing is lost while running. This is gnmic's behavior.

On shutdown, buffered rows get a bounded final flush (up to 10 s). Rows the
server rejects (constraint violations) are dropped with an error log.

### Cisco devices

The device opens the tunnel and registers a target ID of type `GNMI_GNOI`;
gnmic-rs then subscribes with the matching `tunnel-server.targets` rule (all
`GNMI_GNOI` targets when there are no rules). Device-side configuration is
the IOS XE "gNMI dial-out using gRPC tunnel" feature (17.11 and later) — see
Cisco's *Programmability Configuration Guide* for the exact commands of your
release. Whether gNMI inside the tunnel uses TLS depends on the device
configuration: start with `insecure: true`, and use `skip-verify: true` or
`tls-ca` if the logs show TLS errors when dialing the target.

## Differences from gnmic

Scope is the dial-out collector: no CLI commands besides `subscribe`, no
event processors, templates, other inputs/outputs, clustering, API or
Prometheus metrics; `poll` subscriptions are rejected (they are interactive).
Behavior changes, chosen for unattended operation:

- **Newest registration wins**: a device re-registering a target ID from a new
  connection (e.g. after a reboot, while its old TCP session is still
  half-open) replaces the old registration instead of being refused.
- **Dead devices are detected**: HTTP/2 keepalive (30 s) on device connections.
- **Fail fast**: invalid subscriptions, output settings and unreadable
  certificate files stop startup (gnmic exits on some of these only when a
  device registers).
- **Flood protection** (admission control, above).
- **Bounded memory**: per-output queues apply backpressure instead of
  spawning unbounded writers; HTTP/2 flow-control windows are bounded.
- **Rotation never splits a record** across files, and compressed backups
  are written atomically (no truncated `.gz` after a crash).
- Integers inside JSON values keep full 64-bit precision (gnmic rounds
  numbers above 2^53 through float64).

## Testing

```sh
cargo test                                   # unit + end-to-end tests (spawn collector and simulated devices)

# also run the PostgreSQL end-to-end tests:
docker run -d --name gnmic-rs-pg -e POSTGRES_USER=gnmic -e POSTGRES_PASSWORD=gnmic \
  -e POSTGRES_DB=telemetry -p 55432:5432 postgres:17-alpine
GNMIC_RS_TEST_PG_URL=postgres://gnmic:gnmic@127.0.0.1:55432/telemetry cargo test

tests/compat/compare_with_gnmic.sh           # diff output against the official gnmic image
tests/bench/bench.sh event 1 30              # throughput/CPU/memory vs gnmic, both in Docker
tests/bench/flood.sh 3000                    # 3,000 devices reconnecting at once, with/without admission
```

CI (`.github/workflows/ci.yml`) runs `cargo fmt --check`, `cargo clippy -- -D warnings` and the full test suite, including the PostgreSQL tests against a service container.

The end-to-end suite covers file formats, credentials and retries, tunnel
mTLS and self-signed TLS, TLS inside the tunnel, target rules, graceful and
abrupt device loss, re-registration, broken tunnel sessions, `once`
subscriptions, rotation with compression, connection shedding with GOAWAY,
registration rate limiting, queued target setups, `--skip-verify`, and
PostgreSQL long/wide tables, outages and `buffer-full: block`.

`dialout-sim` emulates a device: it registers targets over grpctunnel and
streams interface counters (JSON_IETF, JSON or PROTO with every gNMI value
type). It is also useful against a real deployment:
`dialout-sim --server collector:57400 --targets 10 --interval-ms 1000`.

### Results

`compare_with_gnmic.sh`: `json`, multiline `json`, `event`, split `event` and
`add-target` outputs are byte-identical to gnmic 0.49.0 (sorted records,
`TZ=UTC`), across JSON_IETF, JSON and PROTO encodings and all value types.

`bench.sh`, both collectors in Docker with the same CPU limit, simulated
devices with 20 interfaces each (40 devices unthrottled for the saturated
rows, 50 devices at 100 notifications/s for the fixed load; Apple
silicon host):

| Scenario | gnmic 0.49.0 | gnmic-rs |
|---|---|---|
| Saturated, 1 CPU, `event` | 7.0k records/s, 203 MiB | **15.2k records/s, 94 MiB** |
| Saturated, 1 CPU, `json` | 10.8k records/s, 360 MiB | **23.0k records/s, 81 MiB** |
| Saturated, 4 CPUs, `event` | 25.7k records/s, 539 MiB | **47.2k records/s, 142 MiB** |
| Fixed 5k notifications/s, 2 CPUs | 75% CPU, 120 MiB | **40% CPU, 67 MiB** |

PostgreSQL (default long table, local container): 240k rows/s sustained.

## Layout

```
src/tunnel.rs      grpctunnel server: registrations, sessions, tunnel byte streams
src/admission.rs   flood protection: connection, handshake, registration, setup limits
src/target.rs      per-device lifecycle: dial through the tunnel, subscribe, retry
src/subscribe.rs   SubscribeRequest construction (gnmic semantics)
src/path.rs        xpath <-> gNMI path
src/event.rs       gnmic event model (tags, flattened values)
src/format.rs      json / event / protojson / prototext formatting
src/gojson.rs      Go encoding/json-compatible serializer
src/output/        file (rotation, gzip) and postgres outputs
src/tls.rs         rustls configuration
src/bin/dialout_sim.rs   simulated device for tests
proto/             vendored gnmi, gnmi_ext and grpctunnel definitions
```

## License and attribution

Licensed under the [Apache License 2.0](LICENSE). gnmic-rs is an independent Rust re-implementation of gnmic's dial-out collector; parts of its configuration handling, path conversion, subscription building and output formatting were translated from [gnmic](https://github.com/openconfig/gnmic) (Apache-2.0, © Nokia and the gNMIc contributors), and `proto/` holds the gNMI and grpctunnel definitions from [openconfig/gnmi](https://github.com/openconfig/gnmi) and [openconfig/grpctunnel](https://github.com/openconfig/grpctunnel) (Apache-2.0, © Google). Details are in [NOTICE](NOTICE).

This project is not affiliated with or endorsed by Nokia, Google or the OpenConfig project; "gnmic" is used only to describe compatibility.

