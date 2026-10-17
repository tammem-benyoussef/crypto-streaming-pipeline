"""Pure validation logic: decide where one message from trades.raw should go."""

import json
from dataclasses import dataclass

from common.parsing import InvalidTradeError, parse_trade


@dataclass(frozen=True)
class Decision:
    is_valid: bool
    key: str | None
    value: str


def _to_text(raw_value: str | bytes | None) -> str:
    if raw_value is None:
        return ""
    if isinstance(raw_value, bytes):
        return raw_value.decode("utf-8", errors="replace")
    return raw_value


def _reject(raw_text: str, reason: str, now_ms: int) -> Decision:
    value = json.dumps({"reason": reason, "raw": raw_text, "failed_at_ms": now_ms})
    return Decision(is_valid=False, key=None, value=value)


def decide(raw_value: str | bytes | None, now_ms: int) -> Decision:
    """Turn one message from trades.raw into a Decision.

    Valid  -> value is the clean trade (plus received_at_ms), key is the symbol.
    Invalid -> value is {reason, raw, failed_at_ms}, key is None.
    Only bad *messages* are rejected. Real bugs are not caught and will crash loudly.
    """
    raw_text = _to_text(raw_value)
    if not raw_text:
        return _reject(raw_text, "empty message", now_ms)

    try:
        envelope = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return _reject(raw_text, f"not valid JSON: {exc}", now_ms)

    if not isinstance(envelope, dict) or "payload" not in envelope:
        return _reject(raw_text, "not a valid envelope (needs an object with 'payload')", now_ms)

    try:
        received_at_ms = int(envelope["received_at_ms"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return _reject(raw_text, "missing or invalid received_at_ms", now_ms)

    try:
        trade = parse_trade(envelope["payload"])
    except InvalidTradeError as exc:
        return _reject(raw_text, str(exc), now_ms)

    trade["received_at_ms"] = received_at_ms
    return Decision(is_valid=True, key=trade["symbol"], value=json.dumps(trade))
