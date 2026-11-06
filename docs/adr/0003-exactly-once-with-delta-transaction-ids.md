# 3. Exactly-once writes with Delta transaction ids keyed by batch id

- Status: Accepted
- Date: 2026-11-06

## Context

`foreachBatch` gives at-least-once delivery: after a crash, Spark replays the last planned batch
with the same id and the same input. One batch writes three tables in three separate commits,
so a crash can land between them. Without care, a replay could re-apply changes to tables that
already committed. With the LSN guard that would be harmless for the mirror, but it would
double-count additive metrics.

## Decision

Every commit records a Delta application transaction `(app_id, version = batch_id)`:
`cdclake-mirror` for the mirror tables and `cdclake-metrics` for the metrics table. Before
writing a table, the sink reads `DeltaTable.transaction_version(app_id)` and skips the table if
the recorded version is at least the current batch id.

The check is explicit about `None`. An earlier version used `version or -1`, which treated a
recorded batch 0 as "never applied". A regression test now covers it.

## Consequences

- Each table applies each batch exactly once, including a batch interrupted between tables.
  This is proven by a test that kills the process with `os._exit(137)` after one table's commit
  (`CDCLAKE_CRASH_AFTER=<batch>:<table>`), and by the same test in CI against Kafka.
- It works with plain delta-rs merges, with no Spark Delta extension. The same transaction ids
  are what Spark's own `txnAppId`/`txnVersion` writer options set, so the approach carries over
  to Databricks unchanged.
- The app id must change if the checkpoint is deleted and batch ids restart from 0. Otherwise
  the new batches look like old ones and are skipped.

## Alternatives considered

- **Rely on the LSN guard alone**: correct for the mirror, but not for additive aggregates,
  and every replay rewrites files.
- **One commit for all tables**: Delta has no multi-table transactions.
- **Write a "done" marker per batch to a side table**: the marker and the data can still
  disagree after a crash. A transaction id stored in the same commit as the data cannot.
