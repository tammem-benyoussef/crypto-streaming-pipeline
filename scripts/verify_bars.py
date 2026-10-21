"""Independent check: recount trades.v1 straight from Kafka, compare with live.bars_1m.

Usage: python scripts/verify_bars.py [SINCE_MINUTES]
"""

import json
import os
import sys
import time
import uuid

import numpy as np
import pandas as pd
import psycopg
from confluent_kafka import Consumer, TopicPartition

TOPIC = os.getenv("VALID_TOPIC", "trades.v1")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
IGNORE_SYMBOLS = ["TESTUSDT"]
SETTLE_SECONDS = 180  # newer minutes may not be finalized yet, so they are not compared
READ_TIMEOUT_SECONDS = 180
KEYS = ["symbol", "window_start_s"]


def read_all_trades() -> pd.DataFrame:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"verify-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    partitions = list(consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions)
    lows, ends = {}, {}
    for p in partitions:
        lows[p], ends[p] = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
    consumer.assign([TopicPartition(TOPIC, p, lows[p]) for p in partitions])

    pending = {p for p in partitions if lows[p] < ends[p]}
    rows = []
    deadline = time.time() + READ_TIMEOUT_SECONDS
    while pending:
        if time.time() > deadline:
            raise TimeoutError("could not read all of trades.v1 in time")
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(msg.error())
        rows.append(json.loads(msg.value()))
        if msg.offset() + 1 >= ends[msg.partition()]:
            pending.discard(msg.partition())
    consumer.close()
    if not rows:
        raise RuntimeError("trades.v1 is empty")
    return pd.DataFrame(rows)


def expected_bars(trades: pd.DataFrame) -> pd.DataFrame:
    trades = trades[~trades["symbol"].isin(IGNORE_SYMBOLS)]
    trades = trades.drop_duplicates(["symbol", "trade_id"])
    trades = trades.assign(window_start_s=trades["trade_time_ms"] // 60_000 * 60)
    return (
        trades.groupby(KEYS)
        .agg(trade_count=("trade_id", "count"), base_volume=("quantity", "sum"))
        .reset_index()
    )


def actual_bars() -> pd.DataFrame:
    query = """
        SELECT symbol, extract(epoch FROM window_start)::bigint AS window_start_s,
               trade_count, base_volume
        FROM live.bars_1m WHERE symbol <> ALL(%s)
    """
    with (
        psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "crypto"),
            user=os.getenv("POSTGRES_USER", "crypto"),
            password=os.environ["POSTGRES_PASSWORD"],
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(query, (IGNORE_SYMBOLS,))
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=[*KEYS, "trade_count", "base_volume"])


def main() -> int:
    expected = expected_bars(read_all_trades())
    actual = actual_bars()
    if actual.empty:
        print("live.bars_1m is empty: nothing to compare")
        return 1

    first = actual["window_start_s"].min()
    if len(sys.argv) > 1:
        first = max(first, time.time() - int(sys.argv[1]) * 60)
    last = time.time() - SETTLE_SECONDS
    expected = expected[
        (expected["window_start_s"] >= first) & (expected["window_start_s"] + 60 <= last)
    ]
    actual = actual[(actual["window_start_s"] >= first) & (actual["window_start_s"] + 60 <= last)]

    merged = expected.merge(
        actual, on=KEYS, how="outer", suffixes=("_kafka", "_db"), indicator=True
    )
    missing = merged[merged["_merge"] == "left_only"]
    extra = merged[merged["_merge"] == "right_only"]
    both = merged[merged["_merge"] == "both"]
    volume_ok = np.isclose(both["base_volume_kafka"], both["base_volume_db"], rtol=1e-9, atol=1e-9)
    wrong = both[(both["trade_count_kafka"] != both["trade_count_db"]) | ~volume_ok]

    start = pd.to_datetime(first, unit="s", utc=True)
    end = pd.to_datetime(last, unit="s", utc=True)
    print(f"compared {len(both)} bars between {start} and {end}")
    print(f"  matching:                  {len(both) - len(wrong)}")
    print(f"  wrong count or volume:     {len(wrong)}")
    print(f"  missing from live.bars_1m: {len(missing)}")
    print(f"  extra in live.bars_1m:     {len(extra)}")
    for name, frame in (("WRONG", wrong), ("MISSING", missing), ("EXTRA", extra)):
        if not frame.empty:
            print(f"\n{name} (first 10):")
            print(frame.head(10).to_string(index=False))
    ok = wrong.empty and missing.empty and extra.empty
    print("\nRESULT:", "OK" if ok else "MISMATCH")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
