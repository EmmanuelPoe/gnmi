# Agent guide

## Recipes

**Add a config key**: add the field to the right struct in `config.rs` (serde rename to the gnmic kebab-case name), apply its default and validation at load time (fail fast), use it in the relevant module, document it in `examples/gnmic.yaml` and the README table, and add a config unit test plus an e2e test if behavior changes.

**Add an output type**: implement it under `src/output/`, register it in `output/mod.rs` with a bounded queue like the existing outputs, support graceful drain on shutdown, reject unknown keys, and add e2e coverage. Update the README's "Differences from gnmic" if gnmic does not have it.

**Add a Postgres column source**: extend the `source` parsing and row conversion in `output/postgres.rs`, define coercion for every supported column type (values that cannot be coerced become `NULL`), and test with `GNMIC_RS_TEST_PG_URL` set.

**Simulate devices against a running collector**: `dialout-sim --server collector:57400 --targets 10 --interval-ms 1000`.

## Pitfalls

- Output compatibility is checked byte for byte: key order, number formatting, escaping and separators all matter. `gojson.rs` exists to mimic Go's `encoding/json`; route new JSON output through it.
- Integers above 2^53 must keep full precision (a deliberate improvement over gnmic); do not convert through `f64`.
- Re-registration of an existing target ID replaces the old session ("newest wins"); code that caches per-target state must handle replacement, not just removal.
- Setup slots in admission control are released at the first sync-response; a code path that never sends one (errors, `once` subscriptions) must still release the slot.
- Rotation must never split a record across files and must write `.gz` backups atomically.
- `poll` subscriptions are rejected by design.

## Done means

`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test` pass (CI enforces all three); compat and Postgres suites run when relevant; README and `examples/gnmic.yaml` updated for user-visible changes.
