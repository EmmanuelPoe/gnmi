# CLAUDE.md

Guidance for AI agents working in this repository. Read this first, then the matching file in `docs/`. The README is accurate and detailed — use it as the reference for configuration and behavior.

## What this is

**gnmic-rs**: a Rust re-implementation of gnmic's **gNMI dial-out collector**. Devices (e.g. Cisco IOS XE 17.11+) dial into an embedded openconfig/grpctunnel server; the collector subscribes to each registered target through its tunnel and writes telemetry to files (plain or rotated + gzip) and/or PostgreSQL (binary `COPY`). It reads gnmic's YAML config format and produces **byte-identical** `json`/`event` output to gnmic 0.49.0.

Two binaries: `gnmic-rs` (the collector) and `dialout-sim` (a simulated device used by tests and benchmarks).

## Commands

```bash
cargo build --release                      # target/release/gnmic-rs, dialout-sim (Rust 1.88+, edition 2024; no protoc needed)
cargo test                                 # unit + end-to-end tests (spawn collector + simulated devices); ~20 s
GNMIC_RS_TEST_PG_URL=postgres://gnmic:gnmic@127.0.0.1:55432/telemetry cargo test   # also run Postgres e2e tests
cargo fmt --check                          # rustfmt.toml: max_width = 110
cargo clippy --all-targets
gnmic-rs --config examples/gnmic.yaml --check   # validate config + certificates, then exit
docker build -t gnmic-rs .                 # distroless, non-root
```

CI (`.github/workflows/ci.yml`) runs `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test --locked` with a Postgres 17 service (so the Postgres e2e tests run too). Locally: 39 unit and 21 e2e tests pass, fmt and clippy are clean.

## Hard constraints

1. **gnmic compatibility**: config keys keep gnmic's names and semantics, and `json`/`event` output must stay byte-identical to gnmic. Any change to `src/format.rs`, `src/gojson.rs`, `src/event.rs` or `src/path.rs` must be re-checked with `tests/compat/compare_with_gnmic.sh` (needs Docker).
2. **Bounded memory and backpressure**: every queue/buffer has a limit (`buffer-size`, `max-buffered-rows`, HTTP/2 windows). Never introduce unbounded channels or per-message task spawning.
3. **Fail fast at startup**: invalid config, unknown output keys and unreadable certificates must stop startup, not fail later when a device registers.
4. **Graceful shutdown**: SIGTERM/SIGINT drain and flush all outputs (Postgres final flush bounded to 10 s).
5. Behavior that intentionally differs from gnmic is listed in the README under "Differences from gnmic"; keep that list current.

## Layout

See the README "Layout" section. Quick map: `tunnel.rs` (grpctunnel server), `admission.rs` (flood control), `target.rs` (per-device lifecycle), `subscribe.rs`, `path.rs`, `event.rs`, `format.rs`, `gojson.rs`, `output/{file,rotate,postgres,mod}.rs`, `tls.rs`, `config.rs`, `main.rs` (CLI), `lib.rs` (`run`), `proto/` (vendored .proto files compiled by `build.rs` with protox).

## Rules for agents

- Never commit real device credentials, certificates or private keys; tests generate their own with `rcgen`.
- Add an e2e test in `tests/e2e.rs` for any behavior change visible on the wire or in outputs.
- Do not edit vendored files in `proto/` except to update them wholesale from upstream; keep their license headers and the modification note in `gnmi.proto`.
- Code translated from gnmic keeps its attribution header, and `NOTICE` lists every derived file; update both when porting more.
- New config keys: add to `config.rs` with serde, document in `examples/gnmic.yaml` and the README tables.

## Docs

- [docs/architecture.md](docs/architecture.md)
- [docs/security.md](docs/security.md)
- [docs/testing.md](docs/testing.md)
- [docs/agent-guide.md](docs/agent-guide.md)

There is no UI; the interfaces are the CLI, the YAML config, logs, and output files/tables.
