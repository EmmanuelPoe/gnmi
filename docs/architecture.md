# Architecture

The README covers configuration and behavior in depth; this page explains how the pieces fit for someone changing the code.

## Runtime flow

```
device ──TCP/TLS──> tunnel server (tunnel.rs, hyper HTTP/2 + tonic grpctunnel service)
                       │  admission.rs: connection cap, handshake cap, registration token bucket
                       │  Register(target id, type GNMI_GNOI) → matched against tunnel-server.targets rules
                       ▼
                    target.rs: per-target task
                       │  admission.rs: setup slot (max-concurrent-setups), released at first sync-response
                       │  dial through the tunnel (new tunnel session = byte stream), gNMI client (tonic)
                       │  subscribe.rs builds SubscribeRequest (gnmic semantics); retry with retry-timer
                       ▼
                    responses → event.rs (gnmic event model) / format.rs (json, event, protojson, prototext)
                       ▼
                    output/mod.rs: per-output bounded queue (buffer-size)
                       ├─ output/file.rs (+ rotate.rs: size/age rotation, atomic gzip backups)
                       └─ output/postgres.rs (row conversion → row buffer → writer task → binary COPY, backoff, buffer-full policy)
```

`lib.rs::run(cfg, listener, shutdown)` wires these together; `main.rs` parses the CLI (clap, gnmic-compatible flags such as `--use-tunnel-server subscribe`, `--skip-verify`, `--check`), loads config, sets up tracing, binds the listener and handles signals.

## Modules

| Module | Responsibility |
|---|---|
| `config.rs` | serde model of gnmic's YAML; env-var expansion (`$VAR`, `${VAR}`; passwords only when they start with `$`); unknown top-level keys warned, unknown output keys rejected |
| `tunnel.rs` | grpctunnel server: registrations ("newest registration wins"), sessions, HTTP/2 keepalive 30 s |
| `admission.rs` | the four flood-control stages (see README "Reconnect floods") and the 10 s summary log |
| `target.rs` | target lifecycle: dial, subscribe, detect loss, retry, `initial sync complete` logging |
| `subscribe.rs`, `path.rs` | SubscribeRequest construction; xpath ↔ gNMI path |
| `event.rs`, `format.rs`, `gojson.rs` | output encodings; `gojson.rs` reproduces Go `encoding/json` formatting for byte compatibility, with full 64-bit integers |
| `output/*` | file and Postgres sinks, rotation |
| `tls.rs` | rustls configs: server (optional client auth / mTLS), client (CA, skip-verify, native roots), self-signed generation with rcgen |
| `bin/dialout_sim.rs` | simulated device: registers targets, streams interface counters in JSON_IETF/JSON/PROTO |

## Protobufs

`proto/gnmi.proto`, `gnmi_ext.proto`, `tunnel.proto` are vendored. `build.rs` compiles them with `protox` (pure Rust) and `tonic-prost-build`, and emits a descriptor set used by `prost-reflect` for protojson/prototext output. No `protoc` or Go toolchain is required.

## Deployment artifacts

- `Dockerfile`: multi-stage, `cargo build --release --locked`, runtime `gcr.io/distroless/cc-debian12:nonroot` (uid 65532), exposes 57400, writable `/var/lib/gnmic-rs`.
- `deploy/docker-compose.yml`: Postgres 17 + collector (port `${TUNNEL_PORT:-57400}`); `--profile test` adds three `dialout-sim` devices.
- `deploy/gnmic-rs.service`: systemd unit running as user `gnmic` with `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome` and related hardening; secrets via `EnvironmentFile=-/etc/gnmic-rs/env`.

Release profile: fat LTO, one codegen unit, `panic = "abort"`, stripped.
