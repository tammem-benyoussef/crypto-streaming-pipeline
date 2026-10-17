"""Validator: reads trades.raw, writes valid trades to trades.v1 and the rest to trades.dlq."""

import logging
import os
import time

from confluent_kafka import Consumer, KafkaException, Producer

from validator.logic import decide

logger = logging.getLogger("validator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "trades.raw")
VALID_TOPIC = os.getenv("VALID_TOPIC", "trades.v1")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "trades.dlq")
GROUP_ID = os.getenv("VALIDATOR_GROUP_ID", "validator")
BATCH_SIZE = 500
LOG_EVERY = 1000

delivery_errors: list[str] = []


def on_delivery(err, msg) -> None:
    if err is not None:
        delivery_errors.append(str(err))
        logger.error("delivery failed for key=%s: %s", msg.key(), err)


def process_batch(consumer: Consumer, producer: Producer, counts: dict[str, int]) -> None:
    messages = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
    if not messages:
        return

    now_ms = int(time.time() * 1000)
    for msg in messages:
        if msg.error():
            raise KafkaException(msg.error())
        decision = decide(msg.value(), now_ms)
        topic = VALID_TOPIC if decision.is_valid else DLQ_TOPIC
        producer.produce(topic, key=decision.key, value=decision.value, on_delivery=on_delivery)
        counts["valid" if decision.is_valid else "invalid"] += 1
        producer.poll(0)

    # Output first, offsets second: a crash in between causes duplicates, never losses.
    not_delivered = producer.flush(30)
    if not_delivered or delivery_errors:
        raise RuntimeError(
            f"{not_delivered} messages undelivered, {len(delivery_errors)} delivery errors"
        )
    consumer.commit(asynchronous=False)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
        }
    )
    consumer.subscribe([RAW_TOPIC])
    logger.info("reading %s, writing %s and %s", RAW_TOPIC, VALID_TOPIC, DLQ_TOPIC)

    counts = {"valid": 0, "invalid": 0}
    last_logged = 0
    try:
        while True:
            process_batch(consumer, producer, counts)
            total = counts["valid"] + counts["invalid"]
            if total - last_logged >= LOG_EVERY:
                logger.info(
                    "processed %d (valid=%d, invalid=%d)",
                    total,
                    counts["valid"],
                    counts["invalid"],
                )
                last_logged = total
    except KeyboardInterrupt:
        logger.info("stopping")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
