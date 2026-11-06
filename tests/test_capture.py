"""Change capture against a real Postgres: what pgoutput sends, and how it becomes events."""

from __future__ import annotations

import json
import os
import struct
from pathlib import Path

import psycopg
import pytest

from cdclake import capture, workload
from cdclake.capture import Decoder, Relation


def _events(out: Path) -> list[dict]:
    return [json.loads(line) for f in sorted(out.glob("changes-*.json")) for line in f.open()]


def test_insert_update_delete_carry_before_and_after_images(database: str, tmp_path: Path) -> None:
    workload.setup(database)
    capture.create_slot(database)
    with psycopg.connect(database, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO customers (email, state, created_at) "
            "VALUES ('a@example.com', 'CA', '2026-01-01 08:00:00+00')"
        )
        conn.execute(
            "INSERT INTO orders (customer_id, status, total_cents, created_at, updated_at) "
            "VALUES (1, 'placed', 1299, '2026-01-01 08:00:01-08', '2026-01-01 08:00:01-08')"
        )
        conn.execute("UPDATE orders SET status = 'paid' WHERE order_id = 1")
        conn.execute("DELETE FROM orders WHERE order_id = 1")

    assert capture.drain(database, str(tmp_path)) == 4
    customer, created, updated, deleted = _events(tmp_path)

    assert customer["op"] == "c" and customer["before"] is None
    assert customer["after"]["customer_id"] == 1  # integers stay integers
    assert created["after"]["created_at"] == "2026-01-01T16:00:01.000000Z"  # UTC, like Debezium
    assert updated["op"] == "u"
    assert updated["before"]["status"] == "placed"  # REPLICA IDENTITY FULL sends the old row
    assert updated["after"]["status"] == "paid"
    assert deleted["op"] == "d" and deleted["after"] is None
    assert deleted["before"]["total_cents"] == 1299
    assert {e["source"]["table"] for e in (created, updated, deleted)} == {"orders"}
    lsns = [e["source"]["lsn"] for e in (customer, created, updated, deleted)]
    assert lsns == sorted(lsns) and len(set(lsns)) == 4


def test_drain_advances_the_slot_and_splits_files(database: str, tmp_path: Path) -> None:
    workload.setup(database)
    capture.create_slot(database)
    capture.create_slot(database)  # safe to call twice
    stats = workload.run(database, orders=20)
    first = capture.drain(database, str(tmp_path), max_changes=25)

    assert first > 20 + stats.items
    assert capture.drain(database, str(tmp_path)) == 0  # nothing new
    files = sorted(tmp_path.glob("changes-*.json"))
    assert len(files) == -(-first // 25)
    assert not list(tmp_path.glob(".*.tmp"))  # no half-written files left behind


def test_a_crash_mid_drain_replays_instead_of_losing_changes(
    database: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Changes are acknowledged only after every file exists, so a failure re-reads them all."""
    workload.setup(database)
    capture.create_slot(database)
    workload.run(database, orders=10)
    real_replace = os.replace
    calls: list[object] = []

    def disk_full_on_second_file(src: object, dst: object) -> None:
        calls.append(dst)
        if len(calls) == 2:
            raise OSError("disk full")
        real_replace(src, dst)  # type: ignore[arg-type]

    monkeypatch.setattr(capture.os, "replace", disk_full_on_second_file)
    with pytest.raises(OSError):
        capture.drain(database, str(tmp_path), max_changes=20)
    monkeypatch.setattr(capture.os, "replace", real_replace)

    total = capture.drain(database, str(tmp_path), max_changes=20)
    assert total > 40
    # the first file was rewritten under the same name, so nothing is duplicated or missing
    assert len(_events(tmp_path)) == total
    assert capture.drain(database, str(tmp_path)) == 0


def test_new_columns_appear_in_events_after_alter_table(database: str, tmp_path: Path) -> None:
    workload.setup(database)
    capture.create_slot(database)
    workload.run(database, orders=10, schema_change_at=0.5)
    capture.drain(database, str(tmp_path))
    orders = [e for e in _events(tmp_path) if e["source"]["table"] == "orders" and e["op"] == "c"]
    assert "channel" not in orders[0]["after"]
    assert orders[-1]["after"]["channel"] in {"web", "app", "store"}


def _tuple(*values: bytes | None) -> bytes:
    body = struct.pack(">h", len(values))
    for value in values:
        if value is None:
            body += b"n"
        elif value == b"<unchanged>":
            body += b"u"
        else:
            body += b"t" + struct.pack(">i", len(value)) + value
    return body


def test_decoder_handles_nulls_and_unchanged_large_values() -> None:
    decoder = Decoder()
    decoder.relations[7] = Relation(
        "public", "orders", [("order_id", 20), ("status", 25), ("note", 25)]
    )
    message = b"U" + struct.pack(">i", 7) + b"N" + _tuple(b"42", None, b"<unchanged>")
    event = decoder.decode(message, lsn=100)
    assert event is not None
    assert event["op"] == "u" and event["before"] is None
    # an unchanged TOAST value is left out rather than overwritten with null
    assert event["after"] == {"order_id": 42, "status": None}
