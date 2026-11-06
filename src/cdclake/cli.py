"""Command line for the CDC pipeline.

cdclake setup      --dsn ... [--local-slot]        create the source tables
cdclake workload   --dsn ... --orders 500          run the shop workload
cdclake capture    --dsn ... --out landing/        write pending changes to files
cdclake stream     --lake lake/ --checkpoints ck/ --input landing/
cdclake stream     --lake lake/ --checkpoints ck/ --kafka localhost:9092
cdclake reconcile  --dsn ... --lake lake/          exit 1 unless the mirror matches
cdclake restate    --lake lake/                    rebuild exact metrics from the mirror
cdclake demo       --workdir .cdclake               all of the above on an embedded Postgres
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from pathlib import Path
from typing import Any

from cdclake import capture, metrics, reconcile, workload


def _stream(args: argparse.Namespace) -> dict[str, int]:
    from cdclake.stream import StreamConfig, run, spark_session

    kafka = args.kafka is not None
    config = StreamConfig(
        lake=args.lake,
        checkpoints=args.checkpoints,
        source="kafka" if kafka else "files",
        input_path=args.input,
        bootstrap=args.kafka,
        max_per_batch=args.max_per_batch,
    )
    spark = spark_session(kafka=kafka)
    spark.sparkContext.setLogLevel("ERROR")
    return run(spark, config, available_now=not args.continuous)


def _embedded_postgres(workdir: Path) -> Any:
    """Starts a local Postgres with logical decoding turned on. Needs the dev dependencies."""
    import pgserver
    import psycopg

    logging.getLogger("pgserver").setLevel(logging.WARNING)
    data = workdir / "pg"
    server = pgserver.get_server(data, cleanup_mode="stop")  # type: ignore[attr-defined]
    with psycopg.connect(server.get_uri(), autocommit=True) as conn:
        level = conn.execute("SHOW wal_level").fetchone()
        if level and level[0] != "logical":
            conn.execute("ALTER SYSTEM SET wal_level = logical")
            server.cleanup()  # the setting needs a restart
            server = pgserver.get_server(data, cleanup_mode="stop")  # type: ignore[attr-defined]
    return server


def _demo(args: argparse.Namespace) -> int:
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    server = _embedded_postgres(workdir)
    try:
        return _demo_run(args, workdir, str(server.get_uri()))
    finally:
        server.cleanup()


def _demo_run(args: argparse.Namespace, workdir: Path, dsn: str) -> int:
    workload.setup(dsn)
    capture.create_slot(dsn)
    stats = workload.run(dsn, orders=args.orders)
    landing, lake = workdir / "landing", workdir / "lake"
    events = capture.drain(dsn, str(landing), max_changes=args.file_size)
    args.lake, args.checkpoints, args.input = str(lake), str(workdir / "checkpoints"), str(landing)
    args.kafka, args.continuous = None, False
    summary = _stream(args)
    report = reconcile.reconcile(dsn, str(lake))
    print(
        json.dumps(
            {
                "workload": stats.as_dict(),
                "events": events,
                "stream": summary,
                "reconcile": report.tables,
                "matches": report.matches,
                "metrics": metrics.restate(str(lake)),
            },
            indent=2,
            default=str,
        )
    )
    return 0 if report.matches else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cdclake", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)

    for name in ("setup", "workload", "capture", "reconcile"):
        sub = commands.add_parser(name)
        sub.add_argument("--dsn", required=True)
        if name == "setup":
            sub.add_argument(
                "--local-slot",
                action="store_true",
                help="also create the slot `capture` reads; Debezium creates its own",
            )
        if name == "workload":
            sub.add_argument("--orders", type=int, default=500)
            sub.add_argument("--seed", type=int, default=11)
            sub.add_argument(
                "--start",
                type=dt.datetime.fromisoformat,
                default=workload.START,
                help="simulated clock start, e.g. 2026-01-02T08:00:00+00:00",
            )
            sub.add_argument("--late-share", type=float, default=0.03)
            sub.add_argument("--no-schema-change", action="store_true")
        if name == "capture":
            sub.add_argument("--out", required=True)
            sub.add_argument("--file-size", type=int, default=500)
        if name == "reconcile":
            sub.add_argument("--lake", required=True)

    commands.add_parser("restate").add_argument("--lake", required=True)

    stream = commands.add_parser("stream")
    stream.add_argument("--lake", required=True)
    stream.add_argument("--checkpoints", required=True)
    source = stream.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", help="folder of change files written by `capture`")
    source.add_argument("--kafka", metavar="BOOTSTRAP", help="Kafka bootstrap servers (Debezium)")
    stream.add_argument("--max-per-batch", type=int, default=1)
    stream.add_argument("--continuous", action="store_true", help="keep running every 2 seconds")

    demo = commands.add_parser("demo")
    demo.add_argument("--workdir", default=".cdclake")
    demo.add_argument("--orders", type=int, default=300)
    demo.add_argument("--file-size", type=int, default=200)
    demo.add_argument("--max-per-batch", type=int, default=1)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    if args.command == "setup":
        workload.setup(args.dsn)
        if args.local_slot:
            capture.create_slot(args.dsn)
    elif args.command == "workload":
        stats = workload.run(
            args.dsn,
            orders=args.orders,
            seed=args.seed,
            late_share=args.late_share,
            schema_change_at=None if args.no_schema_change else 0.6,
            stats=workload.WorkloadStats(clock=args.start),
        )
        print(json.dumps(stats.as_dict()))
    elif args.command == "capture":
        print(json.dumps({"events": capture.drain(args.dsn, args.out, max_changes=args.file_size)}))
    elif args.command == "stream":
        print(json.dumps(_stream(args)))
    elif args.command == "reconcile":
        report = reconcile.reconcile(args.dsn, args.lake)
        print(json.dumps({"tables": report.tables, "matches": report.matches}, indent=2))
        return 0 if report.matches else 1
    elif args.command == "restate":
        print(json.dumps(metrics.restate(args.lake), indent=2))
    elif args.command == "demo":
        return _demo(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
