# 6. A small pgoutput decoder for local runs, with real Debezium in CI

- Status: Accepted
- Date: 2026-11-06

## Context

The full stack is Postgres, Kafka, Kafka Connect with Debezium, and Spark. Starting it takes
minutes and needs Docker, which many laptops, and some sandboxes, do not have. Tests that need
it get run rarely, and then they rot.

## Decision

Ship two paths that meet at the same message format:

- **Local and unit tests**: `pgserver` (Postgres 16 from a pip package) with
  `wal_level=logical`. `capture.py` reads the same kind of replication slot through
  `pg_logical_slot_peek_binary_changes`, decodes pgoutput protocol v1 (relation, begin, insert,
  update, delete, including null and unchanged-TOAST markers), and writes Debezium-shaped JSON
  files. Spark reads them with the file source. The slot advances only after the files exist,
  and each file is written under a temporary name and renamed.
- **CI**: Docker Compose with Postgres 16, Kafka 3.9 (KRaft) and Debezium 3.0. The script
  covers an initial snapshot, live traffic, a schema change, a forced crash, recovery, and a
  full reconciliation.

## Consequences

- `pytest` covers capture, merge semantics, schema evolution, late data and crash recovery in
  about a minute, with no Docker.
- The local decoder is about 100 lines. It handles the column types used here (integers,
  booleans, timestamps, text) and not every Postgres type. Production uses Debezium.
- A test feeds a message shaped exactly like Debezium's into the sink, with the extra source
  fields and timestamps without fractional seconds. That catches format drift between the two
  paths without Docker.
- The two paths could still differ in ways no test covers, which is why the Debezium job runs
  on every push.

## Alternatives considered

- **Testcontainers everywhere**: realistic, but slow and unavailable without Docker.
- **Mock change events only**: fast, but never touches a real replication slot, so it would not
  find the problems this project hit (a missing `test_decoding` plugin, wal_level, slot advance
  semantics).
- **`test_decoding` plugin**: not in every Postgres build. `pgserver` lacks it, which is how
  this choice came about, and it is not what Debezium uses.
