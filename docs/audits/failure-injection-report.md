# Failure Injection Report

## Scope

This report reviews the current failure behavior of UNDR9 and identifies where explicit failure injection is still required.

Focus areas:

- process crash
- machine crash
- disk full
- WAL corruption
- partial writes
- network partition
- replica disconnect

## Executive Summary

UNDR9 has a better failure foundation than many early systems because it already includes:

- WAL checksums
- replay limits
- typed errors
- corruption detection
- repair flows
- recovery tests

However, the failure model is still only partially proven.

The repository demonstrates:

- crash recovery logic
- WAL corruption detection
- repair mechanisms
- injected WAL and storage I/O failure coverage for disk-full-like write failures
- some replication validation

It does not yet demonstrate a comprehensive failure-injection program for:

- power-loss windows
- broad disk-full behavior across every artifact and maintenance path
- network partition handling
- replica divergence
- long-lived operator recovery workflows

## Process Crash

## Current evidence

- WAL replay is implemented
- recovery tests exist
- restart after committed writes is validated

## Assessment

Process crash recovery looks functionally credible for a milestone-stage single-node system.

## Remaining gap

Missing explicit repeated crash-loop testing around:

- write in progress
- compaction in progress
- backup/restore in progress
- index rebuild in progress

## Machine Crash / Power Loss

## Current evidence

The system uses temp-file write and rename for snapshots and manifest publishing.

## Assessment

This is a reasonable baseline, but production-grade power-loss safety depends on more than rename alone. The current code does not clearly establish full fsync discipline for:

- newly written snapshot files
- manifest durability
- parent directory durability
- safe WAL truncation window during compaction

## Risk

Compaction is the riskiest area because it republishes state and truncates WAL afterward.

## Disk Full

## Current evidence

I did not find a dedicated disk-full failure test.

## Likely behavior

Writes should eventually fail through I/O errors, but the operator experience and recovery semantics are not yet clearly documented or tested.

## Gap

Need explicit testing for:

- WAL append failure due to disk exhaustion
- snapshot publish failure due to disk exhaustion
- partial maintenance failure due to low disk space

## WAL Corruption

## Current evidence

This is one of the better-covered areas.

The WAL layer includes:

- checksums
- replay validation
- trailing partial frame tolerance
- corruption detection

## Assessment

WAL corruption handling is stronger than many other failure areas.

## Remaining gap

Need explicit large-scale failure drills for:

- corruption in older segments
- corruption during compaction cycles
- corruption combined with long WAL tails

## Partial Writes

## Current evidence

The WAL replay path ignores incomplete trailing frames safely.

This is good for interrupted writes at the WAL tail.

## Gap

Partial write testing is still incomplete for:

- node snapshot files
- edge snapshot files
- vector snapshot files
- index snapshot files
- manifest publication interruption

## Network Partition

## Current evidence

The current replication model is operator-driven and not consensus-based.

## Assessment

Network partition is a major unresolved risk.

Without quorum commit and safe election semantics, partitions can lead to:

- ambiguous leadership
- stale replicas
- unsafe promotions
- divergence risk

## Verdict

Partition handling is not production-ready.

## Replica Disconnect

## Current evidence

The system has:

- replica health tracking
- follower state tracking
- shipping and apply flows
- manual promotion workflows

## Assessment

Replica disconnect can be observed and managed manually, but the system does not yet provide strong automatic safety guarantees.

## Gap

Need explicit testing for:

- long replica lag
- reconnect after lag
- missing history segments
- promotion of stale follower

## Failure Matrix

| Failure Mode | Current State | Confidence |
| --- | --- | --- |
| Process crash | Implemented and partially tested | Medium |
| Machine crash / power loss | Partially addressed | Low |
| Disk full | Not adequately tested | Low |
| WAL corruption | Strongest failure area | Medium-High |
| Partial writes | Partially addressed | Medium |
| Network partition | Not production-safe | Low |
| Replica disconnect | Manual handling only | Low |

## Required Failure Injection Program

## Storage failures

- kill process during WAL append
- kill process during snapshot publish
- kill process during compaction
- force disk full during write
- corrupt WAL tail
- corrupt older WAL segment
- corrupt manifest

## Replication failures

- disconnect follower during shipping
- reconnect follower after lag
- promote stale follower
- partition leader from replicas
- replay with missing history

## Recovery assertions

Each test should verify:

- committed data survives or fails safely
- no silent divergence
- no partial graph visibility
- repair flow is documented and reproducible

## Production Readiness Verdict

UNDR9 is not yet failure-tested enough for production planning.

It has a good early durability and corruption foundation, but it still needs a dedicated fault-injection program before it can be trusted as a production graph database.
