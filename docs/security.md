# Security

The collector accepts inbound connections from network devices and holds device credentials, so most of its security is transport and credential handling.

## Transport

- **Device → collector (tunnel)**: configure `tunnel-server.tls` with `cert-file`/`key-file` and a `ca-file` so devices must present a certificate from your device CA (mTLS, `client-auth`). Without `cert-file`/`key-file` the collector presents a generated self-signed certificate; without `tls` the listener is cleartext HTTP/2 — use that only in labs.
- **gNMI inside the tunnel**: `insecure: true` is plaintext inside the (possibly TLS) tunnel; `skip-verify: true` accepts unverifiable device certificates. Both exist for gnmic compatibility; prefer `tls-ca` with real verification.
- **Postgres**: `tls.mode` `disable | require | verify-full`. `require` encrypts without verifying the server; use `verify-full` with `ca-file` in production.

## Credentials

- Put passwords in the environment and reference them as `${GNMI_PASSWORD}` / `${PG_PASSWORD}`; never commit them in YAML. Passwords are only expanded when the value starts with `$` (gnmic behavior), so a literal password containing `$` elsewhere is used as is.
- `deploy/docker-compose.yml` has no default credentials: `PG_PASSWORD`, `GNMI_USERNAME` and `GNMI_PASSWORD` must be set or `docker compose` refuses to start.
- Dependencies are checked with `cargo audit` (RustSec). The minimum Rust version is 1.88 because the patched `time` and `ctutils` releases require it. Two advisories remain as warnings only: `serde_yaml`'s `unsafe-libyaml` and `chrono` are marked unmaintained, with no known vulnerability.
- systemd: secrets go in `/etc/gnmic-rs/env` (mode 600, owned by root or `gnmic`), loaded by `EnvironmentFile`.
- Do not log credentials or tokens; keep `tracing` fields for config free of secret values.

## Exposure and resource limits

- The tunnel port (default 57400) is the only listener. Restrict it with firewalls to device management ranges.
- Admission control (`max-connections`, `max-concurrent-handshakes`, `registration-rate`, `max-concurrent-setups`) protects against reconnect floods and connection exhaustion; set limits for production.
- Memory is bounded by per-output queues and `max-buffered-rows`; `buffer-full: drop-oldest` (default) sheds data under DB outages instead of growing memory.
- Target-ID rules (`tunnel-server.targets` with `id`/`type` regexes) decide which registrations get subscribed; anchor regexes (`^...$`) so unexpected IDs are not matched.

## Runtime hardening

- Container: distroless, non-root (uid 65532), no shell.
- systemd unit: dedicated user, `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`, kernel/cgroup protections.
- Release builds use `--locked`; run `cargo audit` (or `cargo deny check advisories`) before releases.

## Output data

Telemetry files and tables reveal interface names, addresses and traffic levels. Rotated files are written atomically; set file permissions on the output directory accordingly.
