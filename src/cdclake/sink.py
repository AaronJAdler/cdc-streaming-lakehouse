"""Applies change events to Delta Lake mirror tables. Safe to apply the same events any number of
times, in any number of retries.

Two independent guards make replays harmless:
1. Each row keeps the log sequence number (LSN) of its latest change, and a merge only replaces
   a row with a strictly newer change. A replayed or older event changes nothing.
2. Each commit records the streaming batch id as a Delta transaction id, so a batch retried
   after a crash skips tables it already committed instead of rewriting them.
"""

from __future__ import annotations

import os
from pathlib import Path

from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
from deltalake.schema import Field, PrimitiveType
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from cdclake.schema import TABLES, SourceTable

APP_ID = "cdclake-mirror"
METRICS_APP_ID = "cdclake-metrics"


def _table_path(lake: str, table: str) -> str:
    return str(Path(lake) / "mirror" / table)


def existing_columns(lake: str, table: str) -> list[str]:
    path = _table_path(lake, table)
    if not DeltaTable.is_deltatable(path):
        return []
    return [f.name for f in DeltaTable(path).schema().fields]


def typed_changes(events: DataFrame, table: SourceTable, extra_columns: list[str]) -> DataFrame:
    """The latest change per key in this batch, as typed columns plus change metadata.

    A delete has no "after" image, so its columns come from "before", keeping the last known
    values. Columns added to the source after the pipeline started arrive as strings.
    """

    def column(name: str) -> Column:
        return F.when(F.col("op") == "d", F.col("before")[name]).otherwise(F.col("after")[name])

    typed = [column(c).cast(t).alias(c) for c, t in table.columns.items()]
    extras = [column(c).alias(c) for c in extra_columns]
    rows = events.where(F.col("table") == table.name).select(
        *typed,
        *extras,
        F.col("lsn").alias("_lsn"),
        F.col("op").alias("_op"),
        (F.col("op") == "d").alias("_is_deleted"),
        F.timestamp_millis("ts_ms").alias("_source_ts"),
    )
    latest = Window.partitionBy(table.key).orderBy(F.col("_lsn").desc())
    return rows.withColumn("_rank", F.row_number().over(latest)).where("_rank = 1").drop("_rank")


def new_columns(events: DataFrame, table: SourceTable, known: list[str]) -> list[str]:
    images = events.where(F.col("table") == table.name).select(
        F.explode(F.map_keys(F.coalesce("after", "before"))).alias("name")
    )
    seen = {r.name for r in images.distinct().collect()}
    return sorted(seen - set(table.columns) - set(known))


def already_applied(path: str, app_id: str, batch_id: int) -> bool:
    """True when a commit for this batch id (or a later one) is already in the table's log."""
    if not DeltaTable.is_deltatable(path):
        return False
    version = DeltaTable(path).transaction_version(app_id)
    return version is not None and version >= batch_id


def apply_batch(events: DataFrame, batch_id: int, lake: str) -> dict[str, int]:
    """Merges one micro-batch into every mirror table. Returns rows written per table."""
    written: dict[str, int] = {}
    events = events.cache()
    for table in TABLES.values():
        path = _table_path(lake, table.name)
        exists = DeltaTable.is_deltatable(path)
        if already_applied(path, APP_ID, batch_id):
            continue  # this batch already reached this table before a crash
        known = [
            c
            for c in existing_columns(lake, table.name)
            if c not in table.columns and not c.startswith("_")
        ]
        added = new_columns(events, table, known)
        changes = typed_changes(events, table, known + added).toArrow()
        if changes.num_rows == 0:
            continue
        commit = CommitProperties(app_transactions=[Transaction(app_id=APP_ID, version=batch_id)])
        if not exists:
            write_deltalake(path, changes, commit_properties=commit)
        else:
            target = DeltaTable(path)
            if added:
                # A column added at the source arrives as a nullable string. Adding it is its own
                # commit; if the merge then fails, the retry sees the column and skips this step.
                target.alter.add_columns(
                    [Field(c, PrimitiveType("string"), nullable=True) for c in added]
                )
                target = DeltaTable(path)
            (
                target.merge(
                    changes,
                    predicate=f"t.{table.key} = s.{table.key}",
                    source_alias="s",
                    target_alias="t",
                    commit_properties=commit,
                )
                .when_matched_update_all(predicate="s._lsn > t._lsn")
                .when_not_matched_insert_all()
                .execute()
            )
        written[table.name] = changes.num_rows
        # Fault injection for the crash test: stop after one table, before the others commit.
        if os.environ.get("CDCLAKE_CRASH_AFTER") == f"{batch_id}:{table.name}":
            os._exit(137)
    events.unpersist()
    return written


def apply_metrics(windows: DataFrame, batch_id: int, lake: str) -> None:
    """Upserts window totals. Update mode sends a window's full current total each time."""
    path = str(Path(lake) / "gold" / "orders_per_window")
    data = windows.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "orders",
        "gross_cents",
    ).toArrow()
    if data.num_rows == 0:
        return
    commit = CommitProperties(
        app_transactions=[Transaction(app_id=METRICS_APP_ID, version=batch_id)]
    )
    if not DeltaTable.is_deltatable(path):
        write_deltalake(path, data, commit_properties=commit)
        return
    if already_applied(path, METRICS_APP_ID, batch_id):
        return
    (
        DeltaTable(path)
        .merge(
            data,
            "t.window_start = s.window_start",
            source_alias="s",
            target_alias="t",
            commit_properties=commit,
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
