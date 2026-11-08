from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from pyspark.sql import SparkSession

from cdclake.stream import spark_session


@pytest.fixture(scope="session")
def postgres(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """A throwaway Postgres server with logical decoding turned on."""
    import pgserver

    data = tmp_path_factory.mktemp("pg")
    server = pgserver.get_server(data, cleanup_mode="stop")
    with psycopg.connect(server.get_uri(), autocommit=True) as conn:
        conn.execute("ALTER SYSTEM SET wal_level = logical")
        # Small on purpose, so a test can push a slot past the bound in seconds.
        conn.execute("ALTER SYSTEM SET max_slot_wal_keep_size = '64MB'")
    server.cleanup()
    server = pgserver.get_server(data, cleanup_mode="stop")
    yield server.get_uri()
    server.cleanup()


@pytest.fixture
def database(postgres: str) -> Iterator[str]:
    """A fresh database per test, so tests never share rows or replication slots."""
    name = f"t{abs(hash(object())) % 10**9}"
    with psycopg.connect(postgres, autocommit=True) as conn:
        conn.execute(f"CREATE DATABASE {name}")
    dsn = postgres.replace("/postgres?", f"/{name}?")
    yield dsn
    with psycopg.connect(postgres, autocommit=True) as conn:
        conn.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE database = %s",
            (name,),
        )
        conn.execute(f"DROP DATABASE {name} WITH (FORCE)")


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = spark_session()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def paths(tmp_path: Path) -> dict[str, str]:
    return {k: str(tmp_path / k) for k in ("landing", "lake", "checkpoints")}
