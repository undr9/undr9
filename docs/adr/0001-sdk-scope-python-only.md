# ADR 0001: SDK Scope Is Python Only

## Status

Accepted

## Context

The UNDR9 requirements document describes a broad SDK strategy that includes Python, TypeScript, Rust, Go, Java, and C#. It also states in the V1 scope that official Python and TypeScript SDKs are expected.

The repository-level implementation directive for this project is more restrictive:

- do not create SDKs for multiple languages
- do not create JavaScript, TypeScript, Go, Java, C#, PHP, Ruby, or other SDKs
- create only a Python SDK

These instructions conflict unless an explicit repository-scoped decision is made before implementation.

## Decision

For this repository baseline, UNDR9 will implement only the official Python SDK under `sdk/python/`.

The server architecture, API design, and documentation should remain compatible with future SDK expansion, but no additional SDK code will be introduced unless a later directive supersedes this ADR.

## Consequences

### Positive

- keeps the repository focused on core database correctness
- reduces maintenance surface during early architecture and storage development
- avoids dispersing effort across multiple client ecosystems before the API stabilizes

### Negative

- initial language reach is narrower than the requirements document's broader SDK vision
- examples and adoption paths for non-Python users will be deferred

## Follow-Up Rules

- public API contracts must remain language-agnostic and versioned
- response envelopes and error models must be stable enough to support future SDK generation or hand-written clients
- if future multi-language SDK work is approved, add a new ADR describing expansion scope and compatibility guarantees
