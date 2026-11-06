"""An online shop's database traffic: inserts, status changes, cancellations, a schema change,
and orders from offline stores that sync late.

A simulated clock sets created_at, so a few seconds of real traffic span hours of event time and
fill many metric windows. Every choice comes from a seeded random generator, so runs repeat.
"""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field

import psycopg

from cdclake.schema import DDL

STATES = ["CA", "OR", "WA", "NV", "AZ"]
SKUS = [f"SKU-{n:04d}" for n in range(1, 201)]
START = dt.datetime(2026, 1, 1, 8, 0, tzinfo=dt.UTC)


@dataclass
class WorkloadStats:
    customers: int = 0
    orders_created: int = 0
    late_orders: int = 0
    status_updates: int = 0
    cancellations: int = 0
    items: int = 0
    schema_changed: bool = False
    clock: dt.datetime = field(default=START)

    def as_dict(self) -> dict[str, object]:
        values = dict(vars(self))
        values["clock"] = self.clock.isoformat()
        return values


def setup(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(DDL)


def run(
    dsn: str,
    orders: int,
    seed: int = 11,
    late_share: float = 0.03,
    late_after: float = 0.4,
    late_by: dt.timedelta = dt.timedelta(hours=2),
    schema_change_at: float | None = 0.6,
    stats: WorkloadStats | None = None,
) -> WorkloadStats:
    """Runs `orders` new orders plus the updates and deletes that follow them."""
    rng = random.Random(seed)  # noqa: S311 - synthetic data, repeatable on purpose
    stats = stats or WorkloadStats()
    with psycopg.connect(dsn) as conn:
        if stats.customers == 0:
            with conn.transaction():
                for i in range(max(10, orders // 5)):
                    conn.execute(
                        "INSERT INTO customers (email, state, created_at) VALUES (%s, %s, %s)",
                        (f"shopper{i}@example.com", rng.choice(STATES), START),
                    )
                    stats.customers += 1
        open_orders: list[int] = []
        for n in range(orders):
            if (
                schema_change_at is not None
                and not stats.schema_changed
                and n >= orders * schema_change_at
            ):
                conn.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS channel text")
                conn.commit()
                stats.schema_changed = True

            stats.clock += dt.timedelta(seconds=rng.randint(20, 90))
            # An offline store syncing hours later. Spark drops an event only when its window has
            # closed under the previous batch's watermark, so the delay has to be well past the
            # 10-minute threshold for the drop to be visible.
            late = n > orders * late_after and rng.random() < late_share
            created = stats.clock - late_by if late else stats.clock
            stats.late_orders += late
            # Checkout prices the cart first, so the order row is born with its final total.
            cart = [
                (rng.choice(SKUS), rng.randint(1, 3), rng.randint(199, 9999))
                for _ in range(rng.randint(1, 4))
            ]
            total = sum(qty * price for _, qty, price in cart)
            with conn.transaction():
                columns = "customer_id, status, total_cents, created_at, updated_at"
                values: list[object] = [
                    rng.randint(1, stats.customers),
                    "placed",
                    total,
                    created,
                    created,
                ]
                if stats.schema_changed:
                    columns += ", channel"
                    values.append(rng.choice(["web", "app", "store"]))
                placeholders = ", ".join(["%s"] * len(values))
                row = conn.execute(
                    f"INSERT INTO orders ({columns}) VALUES ({placeholders}) RETURNING order_id",
                    values,
                ).fetchone()
                assert row is not None  # noqa: S101
                order_id = int(row[0])
                for sku, qty, price in cart:
                    conn.execute(
                        "INSERT INTO order_items (order_id, sku, quantity, price_cents) "
                        "VALUES (%s, %s, %s, %s)",
                        (order_id, sku, qty, price),
                    )
                    stats.items += 1
            stats.orders_created += 1
            open_orders.append(order_id)

            # Earlier orders move along, and a few are cancelled and deleted outright.
            if len(open_orders) > 3 and rng.random() < 0.5:
                target = open_orders.pop(rng.randrange(len(open_orders) - 2))
                with conn.transaction():
                    if rng.random() < 0.15:
                        conn.execute("DELETE FROM orders WHERE order_id = %s", (target,))
                        stats.cancellations += 1
                    else:
                        conn.execute(
                            "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                            (rng.choice(["paid", "shipped", "delivered"]), stats.clock, target),
                        )
                        stats.status_updates += 1
    return stats
