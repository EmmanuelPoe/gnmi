# Testing

## Suites

| Command | What it runs | Needs |
|---|---|---|
| `cargo test` | unit tests (`#[cfg(test)]` in config, format, admission, gojson, subscribe, event, path, rotate, tunnel, output/file, output/postgres) and `tests/e2e.rs` (spawns the collector and `dialout-sim` devices) | nothing external; ~20 s after build |
| `GNMIC_RS_TEST_PG_URL=... cargo test` | adds Postgres e2e tests (long/wide tables, outages, `buffer-full: block`) | a Postgres 17 instance, e.g. `docker run -d --name gnmic-rs-pg -e POSTGRES_USER=gnmic -e POSTGRES_PASSWORD=gnmic -e POSTGRES_DB=telemetry -p 55432:5432 postgres:17-alpine` |
| `tests/compat/compare_with_gnmic.sh` | byte-for-byte output comparison with the official gnmic image | Docker |
| `tests/bench/bench.sh <format> <cpus> <seconds>` | throughput/CPU/memory vs gnmic | Docker |
| `tests/bench/flood.sh <devices>` | reconnect flood with and without admission control | Docker, a multi-core host |

Last local run: 39 unit and 21 e2e tests passed without Postgres. CI (`.github/workflows/ci.yml`) runs everything including the Postgres suite (service container on port 55432, `GNMIC_RS_TEST_PG_URL` set).

## Before every PR

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings   # CI treats warnings as errors
cargo test
```

Also run `compare_with_gnmic.sh` when touching `format.rs`, `gojson.rs`, `event.rs`, `path.rs` or `subscribe.rs`, and the Postgres suite when touching `output/postgres.rs`.

## Writing tests

- e2e tests use helpers in `tests/e2e.rs` (an environment that starts the collector with a YAML string, launches simulated devices and offers `wait_for(description, timeout, predicate)`). Prefer polling with `wait_for` over fixed sleeps.
- Certificates are generated per test with `rcgen`; never add real keys to the repo.
- For admission-control behavior, assert on log lines or counters rather than timing where possible; timing-based assertions are flaky on loaded CI hosts.
- `dialout-sim` can stream every gNMI value type in JSON_IETF, JSON and PROTO; use it to cover encoding edge cases.

