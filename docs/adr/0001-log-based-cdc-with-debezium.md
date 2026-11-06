# 1. Capture changes from the write-ahead log with Debezium

- Status: Accepted
- Date: 2026-11-06

## Context

Analytics needs a copy of the shop database that is minutes old at most, and that includes
deletes. The application team will not change their code to publish events. The database
cannot take heavy extra read load during the day.

## Decision

Read changes from Postgres's write-ahead log through a logical replication slot, using the
built-in `pgoutput` plugin. Debezium (Kafka Connect) publishes each change to a Kafka topic per
table, as JSON with schemas disabled. Every table uses `REPLICA IDENTITY FULL`, so updates and
deletes carry the complete old row.

## Consequences

- Every committed change is captured, including hard deletes, in commit order, with no change
  to application code and almost no query load.
- Each change carries its log position (LSN), which gives the sink a per-row ordering key
  (ADR 0002).
- A replication slot keeps WAL on the database disk until it is read. A stopped consumer can
  fill that disk, so slot lag must be monitored and `max_slot_wal_keep_size` set.
- `REPLICA IDENTITY FULL` makes the WAL larger for updates. For these narrow tables that is a
  good trade for having full before images.
- Debezium, Kafka Connect and Kafka have to be run, or bought as a managed service.

## Alternatives considered

- **Polling with `updated_at > last_seen`**: simple, but misses hard deletes and rows updated
  twice between polls, and loads the database with range scans.
- **Triggers into an outbox table**: captures everything, but adds write cost to every
  transaction and means changes to the application's schema.
- **Dual writes from the application**: whenever one write succeeds and the other fails, the
  copies drift silently.
- **`test_decoding` or `wal2json` plugins**: `test_decoding` is a demo format;
  `wal2json` is an extension managed services do not always offer. `pgoutput` ships with
  Postgres 10 and later, and is what Debezium recommends.
