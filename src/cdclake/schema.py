"""The source tables, their primary keys, and the Postgres schema the workload creates."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceTable:
    name: str
    key: str
    columns: dict[str, str]  # column name to Spark SQL type


TABLES = {
    t.name: t
    for t in (
        SourceTable(
            "customers",
            "customer_id",
            {
                "customer_id": "bigint",
                "email": "string",
                "state": "string",
                "created_at": "timestamp",
            },
        ),
        SourceTable(
            "orders",
            "order_id",
            {
                "order_id": "bigint",
                "customer_id": "bigint",
                "status": "string",
                "total_cents": "bigint",
                "created_at": "timestamp",
                "updated_at": "timestamp",
            },
        ),
        SourceTable(
            "order_items",
            "item_id",
            {
                "item_id": "bigint",
                "order_id": "bigint",
                "sku": "string",
                "quantity": "int",
                "price_cents": "bigint",
            },
        ),
    )
}

# REPLICA IDENTITY FULL puts every column in a delete's "before" image, so the mirror keeps the
# last known values of a deleted row instead of nulls.
DDL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id bigserial PRIMARY KEY,
    email text NOT NULL,
    state text NOT NULL,
    created_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id bigserial PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES customers,
    status text NOT NULL,
    total_cents bigint NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS order_items (
    item_id bigserial PRIMARY KEY,
    order_id bigint NOT NULL REFERENCES orders ON DELETE CASCADE,
    sku text NOT NULL,
    quantity int NOT NULL,
    price_cents bigint NOT NULL
);
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
"""

# Columns every mirror table carries in addition to the source columns.
META_COLUMNS = ("_lsn", "_op", "_is_deleted", "_source_ts")
