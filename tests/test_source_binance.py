import json

import pytest

from producer.source_binance import (
    build_envelope,
    build_stream_url,
    extract_symbol,
    unwrap_message,
)

TRADE = {"e": "trade", "s": "BTCUSDT", "t": 1, "p": "65000.1", "q": "0.01", "T": 1700000000000}


def test_build_stream_url():
    url = build_stream_url(["BTCUSDT", " ethusdt "], base="wss://example.com/")
    assert url == "wss://example.com/stream?streams=btcusdt@trade/ethusdt@trade"


def test_build_stream_url_requires_a_symbol():
    with pytest.raises(ValueError):
        build_stream_url(["", " "])


def test_unwrap_combined_message():
    assert unwrap_message({"stream": "btcusdt@trade", "data": TRADE}) == TRADE


def test_unwrap_plain_message_is_unchanged():
    assert unwrap_message(TRADE) == TRADE


def test_extract_symbol():
    assert extract_symbol(TRADE) == "BTCUSDT"
    assert extract_symbol({"no": "symbol"}) is None
    assert extract_symbol("not a dict") is None


def test_envelope_keeps_payload_and_adds_receive_time():
    raw = json.dumps({"stream": "btcusdt@trade", "data": TRADE})
    key, value = build_envelope(raw, 1700000000500)
    assert key == "BTCUSDT"
    assert json.loads(value) == {"received_at_ms": 1700000000500, "payload": TRADE}


def test_envelope_accepts_bytes():
    key, _ = build_envelope(json.dumps(TRADE).encode(), 1)
    assert key == "BTCUSDT"


def test_envelope_keeps_non_json_message_as_text():
    key, value = build_envelope("garbage{", 1)
    assert key is None
    assert json.loads(value)["payload"] == "garbage{"
