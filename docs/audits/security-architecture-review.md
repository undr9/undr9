# Security Architecture Review

## Scope

This review evaluates the current UNDR9 security posture across:

- authentication
- authorization
- secrets
- API abuse protection
- denial-of-service resistance
- dependency and supply-chain posture
- SBOM readiness

## Executive Summary

UNDR9 has the beginnings of a security model, but it is not yet enterprise-hard.

Current strengths:

- API key authentication exists
- role-based authorization exists
- write and admin actions are broadly audited
- a rate limiter exists

Current blockers:

- insecure default development keys ship in config
- auth can be completely bypassed by disabling it
- transport security is external-only
- no evidence of secret rotation or stronger identity models
- no visible SBOM or supply-chain security workflow

## Authentication

## Current state

Authentication is API-key based through the `x-api-key` header.

This is acceptable as an early internal security baseline, but it is weak for enterprise production use.

## Strengths

- simple to operate
- easy to reason about
- aligns with coarse Reader / Writer / Admin roles

## Weaknesses

- plaintext static keys
- no expiry
- no rotation model
- no hashing-at-rest model for credentials
- no federation
- no mTLS
- no JWT or session model

## Critical issue

Default config includes hard-coded development keys.

This is a serious production anti-pattern.

## Authorization

## Current state

The system implements role-based authorization:

- Reader
- Writer
- Admin

This is a valid starting point, but it is still coarse.

## Missing

- finer-grained resource authorization
- namespace-scoped security boundaries
- field-level or action-level enterprise policy controls
- delegated administration model

## Secrets

## Current state

I did not find a mature secrets management model.

Gaps include:

- no integrated secret manager story
- no clear rotation workflow
- no ephemeral credentials
- no bootstrap hardening evidence

## API Abuse and DoS Protection

## Rate limiting

Rate limiting exists, which is a positive sign.

## Remaining gaps

- request body size limits are configured but not clearly enforced end-to-end
- request timeout settings are configured but not clearly enforced end-to-end
- expensive query families remain susceptible to abuse because:
  - vector search is linear scan
  - ranked retrieval is wide candidate scoring
  - deep traversal can still fan out hard

## DoS assessment

DoS resistance is not yet strong enough for hostile production exposure.

## Transport Security

Transport security is delegated to an external reverse proxy.

This can be acceptable operationally, but the current review still flags:

- no in-process TLS option
- no mTLS story
- production security depends heavily on external deployment correctness

## Audit Logging

Audit logging is a strong positive.

The product records many write and maintenance actions into an append-only audit log.

Remaining concerns:

- no visible retention and rotation strategy
- no integrity protection beyond append behavior
- no central export story

## Dependency Vulnerabilities

## Current state

The repository has CI for:

- build
- tests
- clippy

I did not find visible automated workflows for:

- dependency vulnerability scanning
- container image scanning
- license scanning
- secret scanning

## Supply Chain Security

## Current state

I did not find visible evidence of:

- signed release artifacts
- provenance attestations
- dependency pinning policy beyond Cargo norms
- hardened container supply-chain workflow

## SBOM

## Current state

I did not find SBOM generation or publication workflows.

For enterprise customers, this is a meaningful gap.

## Security Risk Summary

| Area | Current State | Risk |
| --- | --- | --- |
| Authentication | Basic API keys | High |
| Authorization | Coarse RBAC | Medium |
| Secret management | Weak | High |
| API abuse protection | Partial | High |
| Transport security | Proxy-dependent | Medium |
| Audit trail | Present | Medium |
| Dependency scanning | Missing evidence | Medium |
| Supply-chain security | Missing evidence | High |
| SBOM | Missing evidence | Medium |

## Required Improvements

## Immediate

- remove hard-coded development keys from shipped defaults
- make insecure auth-disabled mode harder to misuse
- enforce request body limits
- enforce request timeouts

## Near term

- add key rotation and secret management guidance
- add dependency and container vulnerability scanning
- add secret scanning in CI
- add SBOM generation

## Longer term

- support stronger identity models
- support mTLS or equivalent high-assurance transport options
- add more granular authorization boundaries

## Production Readiness Verdict

UNDR9 is not security-ready for enterprise production exposure.

It has a usable early security baseline, but the gaps are large enough that security should remain a dedicated engineering track before production planning proceeds.
