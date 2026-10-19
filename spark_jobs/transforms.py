"""Pure DataFrame transformations for the streaming job (no Kafka, no Postgres)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# The format the validator writes to trades.v1.
TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("trade_id", LongType()),
        StructField("price", DoubleType()),
        StructField("quantity", DoubleType()),
        StructField("quote_quantity", DoubleType()),
        StructField("trade_time_ms", LongType()),
        StructField("is_buyer_maker", BooleanType()),
        StructField("received_at_ms", LongType()),
    ]
)


def add_event_time(trades: DataFrame) -> DataFrame:
    """Add the event-time column: a real timestamp built from trade_time_ms."""
    return trades.withColumn("trade_time", F.timestamp_millis("trade_time_ms"))


def parse_trades(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows (binary value) -> one row per trade, with an event-time column."""
    parsed = (
        kafka_df.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"))
        .select("t.*")
        .where(F.col("trade_id").isNotNull())
    )
    return add_event_time(parsed)


def deduplicate(trades: DataFrame) -> DataFrame:
    """Drop repeated (symbol, trade_id) pairs. Streaming only: call withWatermark first.

    Spark keeps the ids it has seen only until the watermark passes them, so state stays small.
    """
    return trades.dropDuplicatesWithinWatermark(["symbol", "trade_id"])


def aggregate_bars(trades: DataFrame, window: str = "1 minute") -> DataFrame:
    """One row per symbol per window, with the columns of live.bars_1m.

    Window boundaries are returned as epoch seconds so time zone conversions
    cannot shift them when the rows are handed to Postgres.
    """
    order_key = F.struct("trade_time", "trade_id")
    taker_buy_qty = F.when(~F.col("is_buyer_maker"), F.col("quantity")).otherwise(F.lit(0.0))
    bars = trades.groupBy("symbol", F.window("trade_time", window)).agg(
        F.count("*").alias("trade_count"),
        F.sum("quantity").alias("base_volume"),
        F.sum(taker_buy_qty).alias("buy_base_volume"),
        F.sum("quote_quantity").alias("quote_volume"),
        F.min_by("price", order_key).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", order_key).alias("close"),
    )
    return bars.select(
        "symbol",
        F.col("window.start").cast("long").alias("window_start_s"),
        F.col("window.end").cast("long").alias("window_end_s"),
        "trade_count",
        "base_volume",
        "buy_base_volume",
        "quote_volume",
        (F.col("quote_volume") / F.col("base_volume")).alias("vwap"),
        "open",
        "high",
        "low",
        "close",
    )
