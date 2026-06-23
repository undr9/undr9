# ADR 0002: V1 Transaction Boundary Uses Atomic Write Batches

## Status

Accepted

## Context

The requirements document mandates:

- crash recovery through WAL
- atomic record-level writes or protection through transaction semantics
- stronger isolation and transactions in V2

This creates an implementation boundary question for V1:

- should V1 include full multi-statement ACID transactions
- or should V1 provide durable atomic write batches with a simpler concurrency model, leaving stronger isolation for V2

## Decision

V1 will implement the following guarantees:

- WAL-backed durability for committed writes
- atomic visibility of a logical write batch that may include node, edge, and index mutations
- consistent committed-state reads
- a single-writer commit pipeline as the default correctness model

V1 will not promise full general-purpose multi-statement ACID transactions with advanced isolation levels.

V2 may extend this model with explicit transaction sessions, stronger isolation guarantees, and richer concurrency control without invalidating the V1 storage and WAL foundations.

## Rationale

- It satisfies the reliability requirements for atomicity and crash recovery.
- It keeps the storage engine simpler and safer during the earliest implementation phases.
- It aligns with the product emphasis on read-heavy workloads and operational simplicity.
- It avoids premature complexity before the single-node storage and retrieval path is proven.

## Consequences

### Positive

- clearer implementation scope for the storage and WAL layers
- lower early-stage concurrency complexity
- easier recovery and correctness testing

### Negative

- write concurrency is more limited in V1
- some users may expect broader transaction semantics than the first release provides

## Follow-Up Rules

- public API documentation must state V1 guarantees precisely
- WAL record formats must preserve enough structure to support future transaction expansion
- any move to MVCC, lock managers, or explicit multi-statement transactions requires a subsequent ADR
