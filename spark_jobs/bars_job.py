"""Streaming job: trades.v1 (Kafka) -> 1-minute bars -> Postgres live.bars_1m."""

import logging
import os
import time

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQueryListener

from spark_jobs.transforms import aggregate_bars, deduplicate, parse_trades

logger = logging.getLogger("bars_job")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
VALID_TOPIC = os.getenv("VALID_TOPIC", "trades.v1")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "checkpoints/bars_1m")
WATERMARK = os.getenv("WATERMARK_DELAY", "10 seconds")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "10 seconds")

UPSERT_SQL = """
INSERT INTO live.bars_1m (
    symbol, window_start, window_end, trade_count, base_volume, buy_base_volume,
    quote_volume, vwap, open, high, low, close
) VALUES (%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, window_start) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    trade_count = EXCLUDED.trade_count,
    base_volume = EXCLUDED.base_volume,
    buy_base_volume = EXCLUDED.buy_base_volume,
    quote_volume = EXCLUDED.quote_volume,
    vwap = EXCLUDED.vwap,
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close
"""


def batch_summary(rows: list[tuple]) -> tuple[int, int | None, int | None]:
    """Return the emitted row count and the inclusive window-start bounds."""
    if not rows:
        return 0, None, None
    starts = [row[1] for row in rows]
    return len(rows), min(starts), max(starts)


def _sleep_for_test(env_name: str) -> None:
    value = os.getenv(env_name)
    if value is None:
        return
    seconds = float(value)
    if seconds < 0:
        raise ValueError(f"{env_name} must not be negative")
    logger.warning("test hook: sleeping %.3f seconds for %s", seconds, env_name)
    time.sleep(seconds)


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "crypto"),
        user=os.getenv("POSTGRES_USER", "crypto"),
        password=os.environ["POSTGRES_PASSWORD"],
    )


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Upsert the finished bars of this micro-batch. Safe to run twice for the same batch."""
    rows = [tuple(row) for row in batch_df.collect()]
    count, min_start, max_start = batch_summary(rows)
    logger.info(
        "batch %d: emitted_rows=%d window_start_min=%s window_start_max=%s",
        batch_id,
        count,
        min_start,
        max_start,
    )
    if not rows:
        return
    _sleep_for_test("BARS_TEST_SLEEP_BEFORE_WRITE_SECONDS")
    with connect() as conn, conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, rows)
    logger.info("batch %d: postgres commit complete", batch_id)
    _sleep_for_test("BARS_WRITE_SLEEP_SECONDS")
    logger.info("batch %d: upserted %d bars", batch_id, len(rows))


class ProgressLogger(StreamingQueryListener):
    """Log the watermark associated with each completed streaming batch."""

    def onQueryStarted(self, event) -> None:
        pass

    def onQueryProgress(self, event) -> None:
        progress = event.progress
        logger.info(
            "progress batch %d: watermark=%s",
            progress.batchId,
            progress.eventTime.get("watermark"),
        )

    def onQueryTerminated(self, event) -> None:
        pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    spark = (
        SparkSession.builder.appName("bars_1m")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", VALID_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    trades = parse_trades(kafka_df).withWatermark("trade_time", WATERMARK)
    bars = aggregate_bars(deduplicate(trades))

    query = (
        bars.writeStream.outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER)
        .start()
    )
    logger.info("streaming query started, reading %s", VALID_TOPIC)
    query.awaitTermination()


if __name__ == "__main__":
    main()
