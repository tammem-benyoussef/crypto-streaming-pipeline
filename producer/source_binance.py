"""Binance-specific helpers: stream URL, message unwrapping, Kafka envelope."""

import json

DEFAULT_WS_BASE = "wss://data-stream.binance.vision"


def build_stream_url(symbols: list[str], base: str = DEFAULT_WS_BASE) -> str:
    """Build a combined-stream URL, e.g. .../stream?streams=btcusdt@trade/ethusdt@trade"""
    streams = "/".join(f"{s.strip().lower()}@trade" for s in symbols if s.strip())
    if not streams:
        raise ValueError("at least one symbol is required")
    return f"{base.rstrip('/')}/stream?streams={streams}"


def unwrap_message(message):
    """Combined streams wrap each trade as {"stream": ..., "data": {...}}."""
    if isinstance(message, dict) and "stream" in message and "data" in message:
        return message["data"]
    return message


def extract_symbol(payload) -> str | None:
    """Read the symbol only to use it as the Kafka key. No validation here."""
    if isinstance(payload, dict) and "s" in payload:
        return str(payload["s"]).upper()
    return None


def build_envelope(raw_message: str | bytes, received_at_ms: int) -> tuple[str | None, str]:
    """Return (kafka_key, kafka_value) for one received WebSocket message.

    The payload is kept exactly as received. We only add when we received it.
    """
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8", errors="replace")
    try:
        payload = unwrap_message(json.loads(raw_message))
    except json.JSONDecodeError:
        payload = raw_message  # not JSON: keep as-is, the validator will reject it
    value = json.dumps({"received_at_ms": received_at_ms, "payload": payload})
    return extract_symbol(payload), value
