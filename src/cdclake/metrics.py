"""Exact order metrics rebuilt from the mirror, to set beside the live streaming metrics.

The live query finalizes each window once the watermark passes it, so an order that syncs hours
late never reaches its window. The mirror still has that order, so a batch job can rebuild every
window exactly. That is the usual pairing: fast numbers that are almost always right, plus a
scheduled restatement that is always right.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake


def exact_windows(lake: str, minutes: int = 5) -> pa.Table:
    """Orders and gross per window from every order ever created, cancelled ones included.

    Cancelled orders stay in the mirror as soft deletes, so they count here as they do in the live
    metrics, which see the create event before any cancellation.
    """
    orders = DeltaTable(str(Path(lake) / "mirror" / "orders")).to_pyarrow_table(
        columns=["created_at", "total_cents"]
    )
    start = pc.floor_temporal(orders["created_at"], multiple=minutes, unit="minute")
    grouped = (
        pa.table({"window_start": start, "total_cents": orders["total_cents"]})
        .group_by("window_start")
        .aggregate([("total_cents", "count"), ("total_cents", "sum")])
        .rename_columns(["window_start", "orders", "gross_cents"])
        .sort_by("window_start")
    )
    end = pc.add(grouped["window_start"], pa.scalar(minutes * 60_000_000, pa.duration("us")))
    return grouped.append_column("window_end", end).select(
        ["window_start", "window_end", "orders", "gross_cents"]
    )


def restate(lake: str) -> dict[str, int]:
    """Writes exact windows beside the live table and reports how far the live table is off."""
    exact = exact_windows(lake)
    write_deltalake(str(Path(lake) / "gold" / "orders_per_window_exact"), exact, mode="overwrite")
    live_path = str(Path(lake) / "gold" / "orders_per_window")
    live = (
        DeltaTable(live_path).to_pyarrow_table()
        if DeltaTable.is_deltatable(live_path)
        else exact.slice(0, 0)
    )
    live_counts = dict(
        zip(live["window_start"].to_pylist(), live["orders"].to_pylist(), strict=True)
    )
    exact_counts = dict(
        zip(exact["window_start"].to_pylist(), exact["orders"].to_pylist(), strict=True)
    )
    return {
        "windows_exact": len(exact_counts),
        "windows_live": len(live_counts),
        "windows_missing_live": len(exact_counts.keys() - live_counts.keys()),
        "windows_undercounted": sum(
            1 for k, n in exact_counts.items() if k in live_counts and live_counts[k] < n
        ),
        "orders_exact": sum(exact_counts.values()),
        "orders_live": sum(live_counts.values()),
    }
