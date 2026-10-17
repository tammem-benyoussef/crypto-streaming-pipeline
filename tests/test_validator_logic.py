import json

import pytest

from validator.logic import decide

NOW = 1700000001000
GOOD = {
    "e": "trade",
    "E": 1700000000130,
    "s": "BTCUSDT",
    "t": 12345,
    "p": "65000.10",
    "q": "0.002",
    "T": 1700000000120,
    "m": False,
    "M": True,
}


def make_envelope(payload) -> str:
    return json.dumps({"received_at_ms": 1700000000200, "payload": payload})


def test_valid_message_goes_to_v1():
    decision = decide(make_envelope(GOOD), NOW)
    assert decision.is_valid
    assert decision.key == "BTCUSDT"
    trade = json.loads(decision.value)
    assert trade["symbol"] == "BTCUSDT"
    assert trade["trade_id"] == 12345
    assert trade["price"] == 65000.10
    assert trade["received_at_ms"] == 1700000000200


def test_bytes_input_is_accepted():
    decision = decide(make_envelope(GOOD).encode(), NOW)
    assert decision.is_valid


@pytest.mark.parametrize(
    "raw,expected_reason",
    [
        (None, "empty message"),
        (b"", "empty message"),
        ("not json at all", "not valid JSON"),
        ("[1, 2, 3]", "not a valid envelope"),
        ('{"received_at_ms": 1}', "not a valid envelope"),
        (json.dumps({"payload": GOOD}), "received_at_ms"),
        ('{"received_at_ms": Infinity, "payload": {}}', "received_at_ms"),
        (json.dumps({"received_at_ms": 1, "payload": "garbage{"}), "not a JSON object"),
        (make_envelope({k: v for k, v in GOOD.items() if k != "p"}), "missing fields"),
        (make_envelope({**GOOD, "p": "-1"}), "positive"),
        (make_envelope({**GOOD, "t": float("inf")}), "invalid field type"),
    ],
)
def test_invalid_messages_go_to_dlq_with_reason(raw, expected_reason):
    decision = decide(raw, NOW)
    assert not decision.is_valid
    assert expected_reason in json.loads(decision.value)["reason"]


def test_dlq_record_keeps_original_text_and_timestamp():
    raw = "not json at all"
    decision = decide(raw, NOW)
    record = json.loads(decision.value)
    assert decision.key is None
    assert record["raw"] == raw
    assert record["failed_at_ms"] == NOW
