"""Proves the mirror matches the source database, row by row and column by column."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
from deltalake import DeltaTable

from cdclake.schema import TABLES


@dataclass
class Report:
    tables: dict[str, dict[str, int]] = field(default_factory=dict)

    @property
    def matches(self) -> bool:
        return all(t["missing"] == t["extra"] == t["different"] == 0 for t in self.tables.values())


def _normal(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return str(value.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    return str(value)


def reconcile(dsn: str, lake: str) -> Report:
    report = Report()
    with psycopg.connect(dsn) as conn:
        for table in TABLES.values():
            path = str(Path(lake) / "mirror" / table.name)
            mirror = (
                DeltaTable(path).to_pyarrow_table().to_pylist()
                if DeltaTable.is_deltatable(path)
                else []
            )
            cursor = conn.execute(f"SELECT * FROM {table.name}")  # names come from the registry
            columns = [d.name for d in cursor.description or []]
            source = {
                str(row[table.key]): {c: _normal(row[c]) for c in columns}
                for row in (dict(zip(columns, values, strict=True)) for values in cursor.fetchall())
            }
            live = {
                str(r[table.key]): {c: _normal(r.get(c)) for c in columns}
                for r in mirror
                if not r["_is_deleted"]
            }
            different = sum(1 for k in source.keys() & live.keys() if source[k] != live[k])
            report.tables[table.name] = {
                "source_rows": len(source),
                "mirror_rows": len(live),
                "deleted_in_mirror": sum(1 for r in mirror if r["_is_deleted"]),
                "missing": len(source.keys() - live.keys()),
                "extra": len(live.keys() - source.keys()),
                "different": different,
            }
    return report
