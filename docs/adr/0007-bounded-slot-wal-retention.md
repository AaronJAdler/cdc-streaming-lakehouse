# 7. A bound on the WAL a replication slot may retain

- Status: Accepted
- Date: 2026-11-08

## Context

A logical replication slot pins every WAL segment from its `restart_lsn` forward until the
consumer confirms it. That is what makes capture lossless across a consumer restart. It is also
what makes an *unread* slot dangerous: if Debezium or `cdclake capture` stops and nobody notices,
the primary keeps writing WAL it may never delete, the volume fills, and Postgres stops accepting
writes. The shop database, not the lake, goes down. By default Postgres has no limit
(`max_slot_wal_keep_size = -1`).

## Decision

Set `max_slot_wal_keep_size` wherever this project runs Postgres: 1 GB in `docker-compose.yml` and
in the embedded demo server, 64 MB in the test server so a test can cross it in seconds. Past the
bound Postgres marks the slot `lost` and releases its WAL.

`capture.slot_health()` reads `pg_replication_slots.wal_status`, `safe_wal_size` and the bytes
retained; `capture.drain()` checks it first and raises `SlotLost` with the recovery steps instead
of calling `pg_logical_slot_peek_binary_changes` on a slot that can never catch up.

## Consequences

- A stalled consumer costs at most 1 GB of disk on the source. The failure moves from "the
  source database is down" to "the lake is stale and must be re-snapshotted", which is the
  cheaper one to recover from.
- Recovery after `lost` is a full re-sync: drop the slot, snapshot the tables into the mirror,
  create a new slot (`cdclake setup`). Changes between the loss and the new slot are not
  captured individually; the snapshot carries their net effect. The LSN guard in the mirror
  ([ADR 0002](0002-soft-delete-mirror-with-lsn-guard.md)) makes the snapshot safe to apply over
  rows that were already current.
- The bound is a sizing decision: it must exceed the WAL written during the longest outage the
  team wants to ride out without a re-sync. 1 GB suits the demo workload; a production
  deployment would set it from measured WAL volume and alert on `safe_wal_size` shrinking,
  which `slot_health()` exposes for that purpose.
- Debezium's own connector sees the same `lost` state and fails its task; the Compose stack
  therefore fails the same way as the local path.

## Alternatives considered

- **No bound, with monitoring only**: the default. An alert nobody acts on still ends in a full
  disk; the bound makes the worst case survivable even when the alert is missed.
- **A bound on the consumer side (drop the slot when it is idle)**: needs a second process that
  is itself unmonitored, and a race between it and a consumer that is only slow.
- **`wal_keep_size`**: bounds WAL kept for *streaming replicas*, not for slots; it does not apply.
