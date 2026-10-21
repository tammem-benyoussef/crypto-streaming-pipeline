"""Live producer: Binance trade stream -> Kafka topic `trades.raw`.

The producer does NOT validate. It stores every message it receives, unchanged,
inside an envelope that adds the receive time. Validation happens in the validator.
"""

import asyncio
import logging
import os
import signal
import time

import websockets
from confluent_kafka import Producer

from producer.source_binance import DEFAULT_WS_BASE, build_envelope, build_stream_url

logger = logging.getLogger("producer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "trades.raw")
SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
WS_BASE = os.getenv("BINANCE_WS_BASE", DEFAULT_WS_BASE)
LOG_EVERY = 500
RECONNECT_INITIAL_DELAY = 1
RECONNECT_MAX_DELAY = 60


def on_delivery(err, msg) -> None:
    """Called by the Kafka client when a message is acknowledged or fails."""
    if err is not None:
        logger.error("delivery failed for key=%s: %s", msg.key(), err)


def publish(producer: Producer, key: str | None, value: str) -> None:
    try:
        producer.produce(RAW_TOPIC, key=key, value=value, on_delivery=on_delivery)
    except BufferError:
        # local send queue is full: let it drain, then retry once
        producer.poll(1)
        producer.produce(RAW_TOPIC, key=key, value=value, on_delivery=on_delivery)
    producer.poll(0)  # serve delivery callbacks without blocking


async def run(producer: Producer) -> None:
    url = build_stream_url(SYMBOLS, WS_BASE)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        logger.info("shutdown requested")
        stop_event.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request_stop)
        except (NotImplementedError, RuntimeError):
            logger.warning("signal handlers unavailable for signal %s", signum.name)

    count = 0
    reconnect_delay = RECONNECT_INITIAL_DELAY
    try:
        while not stop_event.is_set():
            try:
                logger.info("connecting to %s", url)
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info("connected, publishing to %s", RAW_TOPIC)
                    reconnect_delay = RECONNECT_INITIAL_DELAY
                    while not stop_event.is_set():
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=1)
                        except TimeoutError:
                            continue
                        key, value = build_envelope(message, int(time.time() * 1000))
                        publish(producer, key, value)
                        count += 1
                        if count % LOG_EVERY == 0:
                            logger.info("published %d messages", count)
            except (
                OSError,
                TimeoutError,
                websockets.exceptions.WebSocketException,
            ) as exc:
                if stop_event.is_set():
                    break
                logger.warning("WebSocket connection lost: %s", exc)

            if not stop_event.is_set():
                logger.info("reconnecting in %d seconds", reconnect_delay)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=reconnect_delay)
                except TimeoutError:
                    reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.remove_signal_handler(signum)
            except (NotImplementedError, RuntimeError):
                pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
        }
    )
    try:
        asyncio.run(run(producer))
    except KeyboardInterrupt:
        logger.info("stopping")
    finally:
        not_delivered = producer.flush(10)
        if not_delivered:
            logger.warning("%d messages were not delivered", not_delivered)


if __name__ == "__main__":
    main()
