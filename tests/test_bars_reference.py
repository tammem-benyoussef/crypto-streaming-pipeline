import json
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.bars_job import batch_summary
from spark_jobs.transforms import TRADE_SCHEMA, add_event_time, aggregate_bars

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent / "fixtures" / "trades_v1_sample.jsonl"
KEYS = ["symbol", "window_start_s"]
COLUMNS = [
    *KEYS,
    "trade_count",
    "base_volume",
    "buy_base_volume",
    "quote_volume",
    "vwap",
    "open",
    "high",
    "low",
    "close",
]


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def reference_bars() -> pd.DataFrame:
    """Independent calculation in plain pandas. It must not use any Spark code."""
    trades = pd.DataFrame([json.loads(line) for line in FIXTURE.read_text().splitlines()])
    trades = trades.sort_values(["trade_time_ms", "trade_id"])
    trades["window_start_s"] = (trades["trade_time_ms"] // 60_000) * 60
    grouped = trades.groupby(KEYS)
    bars = grouped.agg(
        trade_count=("trade_id", "count"),
        base_volume=("quantity", "sum"),
        quote_volume=("quote_quantity", "sum"),
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
    ).reset_index()
    taker_buys = (
        trades[~trades["is_buyer_maker"]]
        .groupby(KEYS)["quantity"]
        .sum()
        .rename("buy_base_volume")
        .reset_index()
    )
    bars = bars.merge(taker_buys, on=KEYS, how="left").fillna({"buy_base_volume": 0.0})
    bars["vwap"] = bars["quote_volume"] / bars["base_volume"]
    return bars


def spark_bars(spark, shuffle: bool = False) -> pd.DataFrame:
    trades = add_event_time(spark.read.schema(TRADE_SCHEMA).json(str(FIXTURE)))
    if shuffle:
        trades = trades.orderBy(F.rand(seed=7))
    rows = aggregate_bars(trades).collect()
    return pd.DataFrame([row.asDict() for row in rows])


def assert_same(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    a = actual[COLUMNS].sort_values(KEYS).reset_index(drop=True)
    e = expected[COLUMNS].sort_values(KEYS).reset_index(drop=True)
    pd.testing.assert_frame_equal(a, e, check_dtype=False, rtol=1e-9)


def test_bars_match_pandas_reference(spark):
    assert_same(spark_bars(spark), reference_bars())


def test_bars_do_not_depend_on_input_order(spark):
    assert_same(spark_bars(spark, shuffle=True), reference_bars())


def test_window_end_is_one_minute_after_start(spark):
    bars = spark_bars(spark)
    assert ((bars["window_end_s"] - bars["window_start_s"]) == 60).all()


def test_batch_summary_reports_emitted_window_bounds():
    rows = [
        ("BTCUSDT", 120, 180, 1, 2.0),
        ("ETHUSDT", 60, 120, 1, 3.0),
    ]
    assert batch_summary(rows) == (2, 60, 120)


def test_batch_summary_handles_empty_batch():
    assert batch_summary([]) == (0, None, None)
