# 5. Add new source columns automatically, as nullable strings

- Status: Accepted
- Date: 2026-11-06

## Context

The application team adds columns without warning the data team. A pipeline that fails on an
unknown column stops the copy for everyone until someone deploys a fix. A pipeline that silently
drops the column loses data.

## Decision

When a batch contains a key in `before`/`after` that the table does not have, the sink adds a
nullable **string** column to the Delta table in a separate commit, then merges. Columns listed
in `schema.py` are cast to their declared types; new columns stay strings until someone adds them
there. Renames, drops and type changes are not automated.

## Consequences

- An `ADD COLUMN` at the source shows up in the mirror within one batch, with nothing to deploy.
  Rows from before the change have `null`. The workload exercises this on every test run and
  in CI.
- Strings never fail a cast, so a new column cannot break the pipeline. Typing is a deliberate,
  reviewed change in `schema.py`.
- The column is added in its own commit. If the merge after it fails, the retry finds the
  column already there and goes straight to the merge.
- A renamed column looks like a new column, and the old one stops updating. That is visible
  in data-quality checks (Project 7) and needs a planned migration.

## Alternatives considered

- **Avro or Protobuf with a schema registry**: the right choice at scale and for strict
  contracts, with typed new columns. It adds another service; the JSON converter keeps this
  project runnable anywhere. The sink logic would not change.
- **Fail on unknown columns**: safest for strict contracts, but turns every application
  migration into an outage for analytics.
- **Infer types from values**: `"00123"` becomes an integer and loses its zeros, and the first
  null-only batch has no type to infer.
