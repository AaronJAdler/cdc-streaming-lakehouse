"""Merge semantics of the mirror: ordering, replays, deletes and schema changes."""

from __future__ import annotations

import json
from typing import Any

from deltalake import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from cdclake.sink import APP_ID, already_applied, apply_batch
from cdclake.stream import parse


def _order(order_id: int, status: str, lsn: int, op: str = "u", **extra: Any) -> dict:
    row = {
        "order_id": order_id,
        "customer_id": 1,
        "status": status,
        "total_cents": 500,
        "created_at": "2026-01-01T08:00:00.000000Z",
        "updated_at": "2026-01-01T08:00:00.000000Z",
        **extra,
    }
    return {
        "before": row if op == "d" else None,
        "after": None if op == "d" else row,
        "op": op,
        "ts_ms": 1_767_254_400_000 + lsn,
        "source": {"lsn": lsn, "txId": lsn, "table": "orders"},
    }


def _batch(spark: SparkSession, *events: dict) -> DataFrame:
    return parse(spark.createDataFrame([(json.dumps(e),) for e in events], "value string"))


def _orders(lake: str) -> dict[int, dict]:
    rows = DeltaTable(f"{lake}/mirror/orders").to_pyarrow_table().to_pylist()
    return {r["order_id"]: r for r in rows}


def test_latest_change_wins_within_a_batch(spark: SparkSession, paths: dict[str, str]) -> None:
    lake = paths["lake"]
    apply_batch(
        _batch(
            spark, _order(1, "placed", 10, "c"), _order(1, "paid", 20), _order(1, "shipped", 30)
        ),
        0,
        lake,
    )
    assert _orders(lake)[1]["status"] == "shipped"
    assert _orders(lake)[1]["_lsn"] == 30


def test_an_older_change_never_overwrites_a_newer_one(
    spark: SparkSession, paths: dict[str, str]
) -> None:
    lake = paths["lake"]
    apply_batch(_batch(spark, _order(1, "shipped", 30, "c")), 0, lake)
    apply_batch(_batch(spark, _order(1, "paid", 20)), 1, lake)  # arrives late, e.g. a replay
    assert _orders(lake)[1]["status"] == "shipped"


def test_deletes_are_kept_as_tombstone_rows(spark: SparkSession, paths: dict[str, str]) -> None:
    lake = paths["lake"]
    apply_batch(_batch(spark, _order(1, "placed", 10, "c"), _order(2, "placed", 11, "c")), 0, lake)
    apply_batch(_batch(spark, _order(1, "placed", 12, "d")), 1, lake)
    rows = _orders(lake)
    assert rows[1]["_is_deleted"] and rows[1]["_op"] == "d"
    assert rows[1]["status"] == "placed"  # last known values stay for auditing
    assert not rows[2]["_is_deleted"]


def test_a_retried_batch_is_skipped_by_its_transaction_id(
    spark: SparkSession, paths: dict[str, str]
) -> None:
    lake = paths["lake"]
    apply_batch(_batch(spark, _order(1, "placed", 10, "c")), 0, lake)
    versions = DeltaTable(f"{lake}/mirror/orders").version()
    assert apply_batch(_batch(spark, _order(1, "placed", 10, "c")), 0, lake) == {}
    assert DeltaTable(f"{lake}/mirror/orders").version() == versions  # no new commit


def test_batch_zero_counts_as_applied(spark: SparkSession, paths: dict[str, str]) -> None:
    """Regression: `version or -1` once treated a recorded batch 0 as never applied."""
    lake = paths["lake"]
    apply_batch(_batch(spark, _order(1, "placed", 10, "c")), 0, lake)
    assert DeltaTable(f"{lake}/mirror/orders").transaction_version(APP_ID) == 0
    assert already_applied(f"{lake}/mirror/orders", APP_ID, 0)
    assert not already_applied(f"{lake}/mirror/orders", APP_ID, 1)


def test_a_column_added_at_the_source_is_added_to_the_mirror(
    spark: SparkSession, paths: dict[str, str]
) -> None:
    lake = paths["lake"]
    apply_batch(_batch(spark, _order(1, "placed", 10, "c")), 0, lake)
    apply_batch(_batch(spark, _order(2, "placed", 11, "c", channel="app")), 1, lake)
    apply_batch(_batch(spark, _order(1, "paid", 12, channel="web")), 2, lake)
    rows = _orders(lake)
    assert rows[2]["channel"] == "app"
    assert rows[1]["channel"] == "web"
    assert "channel" in DeltaTable(f"{lake}/mirror/orders").schema().to_arrow().names


def test_a_real_debezium_message_parses_and_applies(
    spark: SparkSession, paths: dict[str, str]
) -> None:
    """Debezium adds fields the local capture does not, and formats timestamps without fractions."""
    message = {
        "before": None,
        "after": {
            "order_id": 7,
            "customer_id": 3,
            "status": "placed",
            "total_cents": 4599,
            "created_at": "2026-01-01T08:00:32Z",
            "updated_at": "2026-01-01T08:00:32.250000Z",
        },
        "source": {
            "version": "3.0.8.Final",
            "connector": "postgresql",
            "name": "shop",
            "ts_ms": 1767254432000,
            "snapshot": "true",
            "db": "shop",
            "sequence": '[null,"23456"]',
            "schema": "public",
            "table": "orders",
            "txId": 750,
            "lsn": 23456,
            "xmin": None,
        },
        "transaction": None,
        "op": "r",
        "ts_ms": 1767254433000,
        "ts_us": 1767254433000000,
    }
    lake = paths["lake"]
    apply_batch(_batch(spark, message), 0, lake)
    row = _orders(lake)[7]
    assert row["_op"] == "r" and not row["_is_deleted"]
    assert row["total_cents"] == 4599
    assert row["created_at"].isoformat() == "2026-01-01T08:00:32+00:00"
    assert row["updated_at"].microsecond == 250000
