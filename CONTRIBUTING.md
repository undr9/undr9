# Contributing To UNDR9

Start with `DEVELOPERS.md` for local setup, repository orientation, operational
commands, and common troubleshooting before making changes.

## Development Expectations

- keep module boundaries aligned with `docs/repository-structure.md`
- verify new work against `docs/subsystem-specifications.md`
- document scope or requirement ambiguities through ADRs under `docs/adr/`
- keep transport logic out of storage, query, and ranking modules
- add unit tests for core logic and integration tests for cross-crate behavior

## Local Validation

Run the same checks locally that CI runs:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Pull Request Checklist

- update docs when behavior or architecture changes
- ensure public APIs are documented and tested
- avoid introducing circular dependencies between crates
- keep new dependencies justified and minimal
