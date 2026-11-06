"""Local change capture: reads Postgres's write-ahead log and writes Debezium-shaped events.

This exists so the whole pipeline runs on a laptop and in fast tests. In production, Debezium
reads the same log through the same pgoutput plugin and publishes the same envelope to Kafka;
CI runs that path for real.

It decodes pgoutput's binary protocol (version 1) from a logical replication slot. Events keep
Debezium's shape: before, after, op, ts_ms, and source.lsn/txId/table.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

SLOT = "cdclake"
PUBLICATION = "cdclake"
_INT_TYPES = {20, 21, 23}  # int8, int2, int4
_TIMESTAMP_TYPES = {1114, 1184}
_BOOL = 16


@dataclass
class Relation:
    schema: str
    name: str
    columns: list[tuple[str, int]]  # name, type OID


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data, self.pos = data, 0

    def take(self, n: int) -> bytes:
        chunk = self.data[self.pos : self.pos + n]
        self.pos += n
        return chunk

    def byte(self) -> str:
        return self.take(1).decode()

    def int16(self) -> int:
        return int(struct.unpack(">h", self.take(2))[0])

    def int32(self) -> int:
        return int(struct.unpack(">i", self.take(4))[0])

    def int64(self) -> int:
        return int(struct.unpack(">q", self.take(8))[0])

    def string(self) -> str:
        end = self.data.index(b"\0", self.pos)
        text = self.data[self.pos : end].decode()
        self.pos = end + 1
        return text


def lsn_to_int(lsn: str) -> int:
    high, low = lsn.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def _value(type_oid: int, text: str | None) -> Any:
    if text is None:
        return None
    if type_oid in _INT_TYPES:
        return int(text)
    if type_oid == _BOOL:
        return text == "t"
    if type_oid in _TIMESTAMP_TYPES:
        moment = dt.datetime.fromisoformat(text)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=dt.UTC)
        # Debezium sends timestamptz as an ISO-8601 string in UTC.
        return moment.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return text


def _tuple(reader: _Reader, relation: Relation) -> dict[str, Any]:
    count = reader.int16()
    values: dict[str, Any] = {}
    for (name, type_oid), _ in zip(relation.columns, range(count), strict=False):
        kind = reader.byte()
        if kind == "n":
            values[name] = None
        elif kind == "t":
            values[name] = _value(type_oid, reader.take(reader.int32()).decode())
        # "u" means an unchanged large value that was not sent; leave it out
    return values


class Decoder:
    """Turns pgoutput messages into Debezium envelopes, remembering table layouts as it goes."""

    def __init__(self) -> None:
        self.relations: dict[int, Relation] = {}
        self.xid = 0

    def decode(self, message: bytes, lsn: int) -> dict[str, Any] | None:
        reader = _Reader(message)
        kind = reader.byte()
        if kind == "B":
            reader.int64()  # final LSN of the transaction
            reader.int64()  # commit timestamp
            self.xid = reader.int32()
            return None
        if kind == "R":
            relation_id = reader.int32()
            schema, name = reader.string(), reader.string()
            reader.byte()  # replica identity setting
            columns = []
            for _ in range(reader.int16()):
                reader.byte()  # flags
                column = reader.string()
                columns.append((column, reader.int32()))
                reader.int32()  # type modifier
            self.relations[relation_id] = Relation(schema or "public", name, columns)
            return None
        if kind not in "IUD":
            return None  # commit, type, origin and truncate messages are not row changes
        relation = self.relations[reader.int32()]
        before: dict[str, Any] | None = None
        after: dict[str, Any] | None = None
        marker = reader.byte()
        if kind == "I":
            after = _tuple(reader, relation)
        elif kind == "U":
            if marker in "KO":
                before = _tuple(reader, relation)
                reader.byte()  # "N"
            after = _tuple(reader, relation)
        else:
            before = _tuple(reader, relation)
        return {
            "before": before,
            "after": after,
            "op": {"I": "c", "U": "u", "D": "d"}[kind],
            "ts_ms": int(dt.datetime.now(dt.UTC).timestamp() * 1000),
            "source": {
                "lsn": lsn,
                "txId": self.xid,
                "table": relation.name,
                "schema": relation.schema,
            },
        }


def create_slot(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        if not conn.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s", (PUBLICATION,)
        ).fetchone():
            conn.execute(f"CREATE PUBLICATION {PUBLICATION} FOR ALL TABLES")
        if not conn.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)
        ).fetchone():
            conn.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (SLOT,))


def drain(dsn: str, out_dir: str, max_changes: int = 500) -> int:
    """Writes pending changes to files of up to `max_changes` events each. Returns the count.

    Each file is written under a temporary name and renamed, so a streaming reader never sees a
    half-written file. The slot advances only after the files exist, so a crash re-reads changes
    rather than losing them; downstream merges ignore anything already applied.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes("
            "%s, NULL, NULL, 'proto_version', '1', 'publication_names', %s)",
            (SLOT, PUBLICATION),
        ).fetchall()
        decoder = Decoder()
        events = [e for lsn, data in rows if (e := decoder.decode(bytes(data), lsn_to_int(lsn)))]
        for start in range(0, len(events), max_changes):
            chunk = events[start : start + max_changes]
            name = f"changes-{chunk[0]['source']['lsn']:020d}.json"
            tmp = out / f".{name}.tmp"
            tmp.write_text("".join(json.dumps(e, separators=(",", ":")) + "\n" for e in chunk))
            os.replace(tmp, out / name)
        if rows:
            conn.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (SLOT, rows[-1][0]))
    return len(events)
