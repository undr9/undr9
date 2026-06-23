# Development Onboarding

## Prerequisites

- stable Rust toolchain
- `clippy` and `rustfmt` components

## First-Time Setup

```bash
cargo check --workspace
cargo test --workspace
```

## Repository Workflow

1. Read the architecture documents in `docs/`.
2. Confirm the change fits the milestone roadmap.
3. Update or add ADRs if requirements are ambiguous.
4. Implement within the owning crate.
5. Run formatting, tests, and clippy before submitting changes.
