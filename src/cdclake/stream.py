"""Two streaming queries over the same change feed.

1. The mirror query applies every change to Delta tables that match the source database.
2. The metrics query counts orders in 5-minute event-time windows. Its watermark lets Spark
   finalize a window once events are 10 minutes past it. Orders that arrive later than that are
   dropped from the live metrics but still land in the mirror, so exact totals can always be
   recomputed from it.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from cdclake.sink import apply_batch, apply_metrics

log = logging.getLogger(__name__)

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
ENVELOPE = T.StructType(
    [
        T.StructField("before", T.MapType(T.StringType(), T.StringType())),
        T.StructField("after", T.MapType(T.StringType(), T.StringType())),
        T.StructField("op", T.StringType()),
        T.StructField("ts_ms", T.LongType()),
        T.StructField(
            "source",
            T.StructType(
                [
                    T.StructField("lsn", T.LongType()),
                    T.StructField("txId", T.LongType()),
                    T.StructField("table", T.StringType()),
                ]
            ),
        ),
    ]
)


@dataclass(frozen=True)
class StreamConfig:
    lake: str
    checkpoints: str
    source: str = "files"  # "files" for local runs, "kafka" for Debezium
    input_path: str | None = None
    bootstrap: str | None = None
    topic_pattern: str = r"shop\.public\..*"
    max_per_batch: int = 1
    watermark: str = "10 minutes"
    window: str = "5 minutes"


def spark_session(kafka: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("cdclake")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    return builder.getOrCreate()


def read_changes(spark: SparkSession, config: StreamConfig) -> DataFrame:
    if config.source == "kafka":
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.bootstrap)
            .option("subscribePattern", config.topic_pattern)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", config.max_per_batch * 500)
            .load()
            .selectExpr("CAST(value AS STRING) AS value")
            .where("value IS NOT NULL")  # Kafka tombstones carry no value
        )
    else:
        raw = (
            spark.readStream.format("text")
            .option("maxFilesPerTrigger", config.max_per_batch)
            .load(config.input_path)
        )
    return parse(raw)


def parse(raw: DataFrame) -> DataFrame:
    """Debezium JSON in a `value` column to one row per change. Works on batch or stream input."""
    parsed = raw.select(F.from_json("value", ENVELOPE).alias("e"))
    return parsed.select(
        "e.before",
        "e.after",
        "e.op",
        "e.ts_ms",
        F.col("e.source.lsn").alias("lsn"),
        F.col("e.source.txId").alias("tx_id"),
        F.col("e.source.table").alias("table"),
    ).where(F.col("op").isin("c", "u", "d", "r"))


def order_windows(changes: DataFrame, config: StreamConfig) -> DataFrame:
    created = changes.where((F.col("table") == "orders") & F.col("op").isin("c", "r")).select(
        F.col("after")["created_at"].cast("timestamp").alias("event_time"),
        F.col("after")["total_cents"].cast("long").alias("total_cents"),
    )
    return (
        created.withWatermark("event_time", config.watermark)
        .groupBy(F.window("event_time", config.window))
        .agg(F.count("*").alias("orders"), F.sum("total_cents").alias("gross_cents"))
    )


def run(spark: SparkSession, config: StreamConfig, available_now: bool = True) -> dict[str, int]:
    """Runs both queries. With available_now, processes everything waiting and returns."""
    changes = read_changes(spark, config)
    checkpoints = Path(config.checkpoints)

    def mirror_batch(df: DataFrame, batch_id: int) -> None:
        apply_batch(df, batch_id, config.lake)

    def metrics_batch(df: DataFrame, batch_id: int) -> None:
        apply_metrics(df, batch_id, config.lake)

    mirror = (
        changes.writeStream.foreachBatch(mirror_batch)
        .option("checkpointLocation", str(checkpoints / "mirror"))
        .queryName("mirror")
    )
    metrics = (
        order_windows(changes, config)
        .writeStream.outputMode("update")
        .foreachBatch(metrics_batch)
        .option("checkpointLocation", str(checkpoints / "metrics"))
        .queryName("metrics")
    )
    if available_now:
        mirror, metrics = mirror.trigger(availableNow=True), metrics.trigger(availableNow=True)
    else:
        mirror = mirror.trigger(processingTime="2 seconds")
        metrics = metrics.trigger(processingTime="2 seconds")
    queries = [mirror.start(), metrics.start()]
    for query in queries:
        query.awaitTermination()
    dropped = sum(
        op.get("numRowsDroppedByWatermark", 0)
        for progress in queries[1].recentProgress
        for op in progress.get("stateOperators", [])
    )
    summary = {
        "mirror_batches": len(queries[0].recentProgress),
        "metrics_batches": len(queries[1].recentProgress),
        "late_rows_dropped": int(dropped),
    }
    ops = Path(config.lake) / "_ops"
    ops.mkdir(parents=True, exist_ok=True)
    with (ops / "runs.jsonl").open("a") as handle:
        handle.write(json.dumps(summary) + "\n")
    log.info("stream run: %s", summary)
    return summary
