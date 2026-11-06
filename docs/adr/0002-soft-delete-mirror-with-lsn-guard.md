# 2. Mirror tables merged by key, guarded by LSN, with deletes kept as tombstones

- Status: Accepted
- Date: 2026-11-06

## Context

Consumers want each source table as a Delta table they can query like the original. The change
feed can deliver the same change more than once (Debezium restarts, Spark replays). It can also
deliver changes to one row out of order across micro-batches. Auditors want to know which rows
were deleted, and when.

## Decision

For each table, keep one Delta row per primary key. Each micro-batch:

1. Keeps only the latest change per key in the batch (highest LSN).
2. `MERGE`s on the primary key, updating a matched row **only if** `source._lsn > target._lsn`,
   and inserting unmatched rows.
3. Writes a delete as a row with its last known values and `_is_deleted = true`.

Metadata columns: `_lsn`, `_op`, `_is_deleted`, `_source_ts`.

## Consequences

- Replays and out-of-order deliveries cannot move a row backwards in time. Applying the same
  batch twice gives the same table.
- Comparing LSNs per row is sound because Postgres row locks serialise conflicting writers: a
  later change to a row always has a higher LSN.
- Current state is `WHERE NOT _is_deleted`. Deleted rows stay queryable for audit.
- The table holds current state, not history. For history, append the raw change stream to a
  bronze table as well. That is a small addition, and Project 2 shows the SCD2 approach.
- Tombstones accumulate. A retention job can purge old tombstones if audit does not need them.

## Alternatives considered

- **Append-only change log, with current state as a view**: simplest to write, but every reader
  pays for "latest per key" logic, and it gets slower as the table grows.
- **Hard deletes in the mirror**: smaller tables, but a delete that arrives before its insert
  (possible after a replay) leaves the row behind. It also loses audit information.
- **Ordering by `ts_ms` instead of LSN**: timestamps come from clocks that can go backwards and
  have millisecond resolution; LSNs are strictly ordered.
