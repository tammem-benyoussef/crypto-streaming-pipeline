"""Turn a raw Binance trade message into our standard trade format."""

import math

REQUIRED_FIELDS = ("s", "t", "p", "q", "T")


class InvalidTradeError(ValueError):
    """Raised when a raw trade message is malformed or has invalid values."""


def parse_trade(raw: dict) -> dict:
    """Convert one raw Binance `trade` payload into our standard dict.

    Raises InvalidTradeError if the message is malformed, so the validator
    can send it to the dead-letter topic together with the reason.
    """
    if not isinstance(raw, dict):
        raise InvalidTradeError("payload is not a JSON object")

    missing = [f for f in REQUIRED_FIELDS if f not in raw]
    if missing:
        raise InvalidTradeError(f"missing fields: {missing}")

    try:
        price = float(raw["p"])
        quantity = float(raw["q"])
        trade_id = int(raw["t"])
        trade_time_ms = int(raw["T"])
    except (TypeError, ValueError) as exc:
        raise InvalidTradeError(f"invalid field type: {exc}") from exc

    if not (math.isfinite(price) and math.isfinite(quantity) and price > 0 and quantity > 0):
        raise InvalidTradeError("price and quantity must be positive numbers")

    return {
        "symbol": str(raw["s"]).upper(),
        "trade_id": trade_id,
        "price": price,
        "quantity": quantity,
        "quote_quantity": price * quantity,
        "trade_time_ms": trade_time_ms,
        "is_buyer_maker": bool(raw.get("m", False)),
    }
