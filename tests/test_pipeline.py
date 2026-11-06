"""The whole pipeline, source database to reconciled mirror, including a crash mid-batch."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from deltalake import DeltaTable
from pyspark.sql import SparkSession

from cdclake import capture, metrics, reconcile, workload
from cdclake.stream import StreamConfig, run


def _config(paths: dict[str, str]) -> StreamConfig:
    return StreamConfig(
        lake=paths["lake"], checkpoints=paths["checkpoints"], input_path=paths["landing"]
    )


def test_incremental_runs_keep_the_mirror_identical_to_the_source(
    spark: SparkSession, database: str, paths: dict[str, str]
) -> None:
    workload.setup(database)
    capture.create_slot(database)
    stats = None
    for round_ in range(3):  # traffic, capture and streaming interleave like a real day
        stats = workload.run(database, orders=60, seed=round_, stats=stats)
        capture.drain(database, paths["landing"], max_changes=150)
        run(spark, _config(paths))
        report = reconcile.reconcile(database, paths["lake"])
        assert report.matches, report.tables
    assert stats and stats.schema_changed
    assert report.tables["orders"]["deleted_in_mirror"] == stats.cancellations


def test_late_orders_miss_live_metrics_and_restatement_recovers_them(
    spark: SparkSession, database: str, paths: dict[str, str]
) -> None:
    workload.setup(database)
    capture.create_slot(database)
    stats = workload.run(database, orders=300, late_share=0.05)
    capture.drain(database, paths["landing"], max_changes=200)
    summary = run(spark, _config(paths))

    assert stats.late_orders > 0
    # Spark counts dropped rows after partial aggregation, so two late orders landing in the same
    # window of the same batch count once. The restated totals below are the exact check.
    assert 1 <= summary["late_rows_dropped"] <= stats.late_orders
    gap = metrics.restate(paths["lake"])
    assert gap["orders_exact"] == stats.orders_created
    assert gap["orders_live"] == stats.orders_created - stats.late_orders
    exact = DeltaTable(f"{paths['lake']}/gold/orders_per_window_exact").to_pyarrow_table()
    assert sum(exact["orders"].to_pylist()) == stats.orders_created


def _stream_subprocess(paths: dict[str, str], crash_after: str | None) -> int:
    env = {**os.environ}
    env.pop("CDCLAKE_CRASH_AFTER", None)
    if crash_after:
        env["CDCLAKE_CRASH_AFTER"] = crash_after
    command = [
        sys.executable,
        "-m",
        "cdclake.cli",
        "stream",
        "--lake",
        paths["lake"],
        "--checkpoints",
        paths["checkpoints"],
        "--input",
        paths["landing"],
    ]
    return subprocess.run(command, env=env, capture_output=True, timeout=600).returncode


@pytest.mark.slow
def test_a_crash_between_tables_recovers_without_duplicates(
    database: str, paths: dict[str, str]
) -> None:
    """Kill the process after batch 2 commits to `orders` but before `order_items`.

    On restart Spark replays batch 2. The orders table recognises the batch id and skips it; the
    other tables apply it for the first time. The result matches the source exactly.
    """
    workload.setup(database)
    capture.create_slot(database)
    workload.run(database, orders=120)
    capture.drain(database, paths["landing"], max_changes=100)

    assert _stream_subprocess(paths, crash_after="2:orders") == 137
    orders = DeltaTable(f"{paths['lake']}/mirror/orders")
    assert orders.transaction_version("cdclake-mirror") == 2
    items = DeltaTable(f"{paths['lake']}/mirror/order_items")
    assert items.transaction_version("cdclake-mirror") == 1  # batch 2 never reached it

    assert _stream_subprocess(paths, crash_after=None) == 0
    report = reconcile.reconcile(database, paths["lake"])
    assert report.matches, report.tables
    keys = DeltaTable(f"{paths['lake']}/mirror/orders").to_pyarrow_table()["order_id"].to_pylist()
    assert len(keys) == len(set(keys))
