# 4. Watermarked live metrics, plus a batch restatement for exact numbers

- Status: Accepted
- Date: 2026-11-06

## Context

Operations wants orders and revenue per 5 minutes, live. Most orders arrive within seconds, but
offline stores sync orders hours late. A streaming aggregation that waits forever for late data
keeps every window in memory forever.

## Decision

The live query uses a 10-minute watermark on `created_at` and update output mode. Windows close
and release their state once the watermark passes them, and later events for those windows are
dropped from live metrics. The mirror still receives those late orders. `cdclake restate`
rebuilds exact windows from the mirror into `gold/orders_per_window_exact` and reports how far
the live table is off.

## Consequences

- Streaming state stays bounded however long the pipeline runs.
- Live numbers are slightly low after a late sync. In the demo, 298 of 300 orders were counted
  live; the restatement had all 300. Dashboards that need to be exact (finance) read the
  restated table; operational screens read the live one.
- Testing showed two non-obvious Spark 4 behaviours, now written into the tests and docs:
  - Lateness is judged against the **previous** batch's watermark and the window's **end**.
    A small delay (45 minutes, with 35 minutes of event time per batch) was never dropped.
  - `numRowsDroppedByWatermark` counts rows after partial aggregation, so it can undercount
    late events. The exact gap comes from the restatement, not that metric.

## Alternatives considered

- **No watermark**: every window stays in state forever, and memory grows without limit.
- **A watermark of several hours**: catches the late stores, but delays every final number by
  hours and multiplies state size.
- **Compute metrics only in batch**: always exact, but never live.
