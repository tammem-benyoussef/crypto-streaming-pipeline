import pytest

from common.parsing import parse_trade

VALID = {
    "e": "trade",
    "E": 1700000000123,
    "s": "BTCUSDT",
    "t": 12345,
    "p": "65000.10",
    "q": "0.002",
    "T": 1700000000120,
    "m": True,
}


def test_parse_valid_trade():
    trade = parse_trade(VALID)
    assert trade["symbol"] == "BTCUSDT"
    assert trade["trade_id"] == 12345
    assert trade["price"] == 65000.10
    assert trade["quantity"] == 0.002
    assert trade["trade_time_ms"] == 1700000000120
    assert trade["is_buyer_maker"] is True


def test_quote_quantity_is_price_times_quantity():
    trade = parse_trade(VALID)
    assert trade["quote_quantity"] == pytest.approx(65000.10 * 0.002)


def test_missing_field_raises():
    bad = {k: v for k, v in VALID.items() if k != "p"}
    with pytest.raises(ValueError, match="missing fields"):
        parse_trade(bad)


@pytest.mark.parametrize(
    "field,value",
    [
        ("p", "0"),
        ("p", "-1"),
        ("q", "0"),
        ("p", "abc"),
        ("p", "nan"),
        ("t", "x"),
        ("t", float("inf")),
        ("T", float("inf")),
    ],
)
def test_invalid_values_raise(field, value):
    bad = {**VALID, field: value}
    with pytest.raises(ValueError):
        parse_trade(bad)


def test_non_dict_raises():
    with pytest.raises(ValueError):
        parse_trade("not a dict")
