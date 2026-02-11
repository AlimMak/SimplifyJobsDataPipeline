"""
Stream Processor: Raw → Cleaned transformation.

Simulates a Kafka Streams-style processor that:
  1. Consumes raw HTML rows from 'raw-job-listings'
  2. Cleans and parses each row
  3. Produces cleaned records to 'cleaned-jobs'

This demonstrates the processor pattern where a service acts as
BOTH a consumer and a producer, transforming data between topics.
"""

import json
import logging

from kafka import KafkaConsumer, KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPICS,
)
from models import RawJobRow
from scraper import clean_row

logger = logging.getLogger(__name__)

PROCESSOR_GROUP = "cg-stream-processor"


def create_consumer() -> KafkaConsumer:
    """Consumer that reads raw HTML rows."""
    return KafkaConsumer(
        TOPICS["raw"],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=PROCESSOR_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=15000,
        client_id="stream-processor-consumer",
    )


def create_producer() -> KafkaProducer:
    """Producer that writes cleaned job records."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        client_id="stream-processor-producer",
    )


def run_stream_processor():
    """
    Kafka Streams-style processing loop:

        raw-job-listings → [parse & clean] → cleaned-jobs

    This pattern (consume → transform → produce) is the core of
    stream processing. In production you'd use Kafka Streams (Java)
    or Faust (Python), but this demonstrates the concept.
    """
    logger.info("=" * 60)
    logger.info("  STREAM PROCESSOR: raw → cleaned")
    logger.info("  Input:  %s", TOPICS["raw"])
    logger.info("  Output: %s", TOPICS["cleaned"])
    logger.info("=" * 60)

    consumer = create_consumer()
    producer = create_producer()

    processed = 0
    errors = 0

    try:
        for message in consumer:
            raw_data = message.value
            raw_row = RawJobRow(
                html=raw_data["html"],
                category=raw_data["category"],
                row_index=raw_data["row_index"],
            )

            # Transform: parse raw HTML → structured job listing
            job = clean_row(raw_row)

            if job is None:
                errors += 1
                logger.debug("Could not parse row %d", raw_row.row_index)
                continue

            # Produce cleaned record
            producer.send(
                TOPICS["cleaned"],
                key=job.job_id,
                value=json.loads(job.to_json()),
            )

            processed += 1

            if processed % 50 == 0:
                producer.flush()
                consumer.commit()
                logger.info("Processed %d records (errors: %d)", processed, errors)

        # Final flush
        producer.flush()
        consumer.commit()

    except KeyboardInterrupt:
        logger.info("Stream processor interrupted")
    finally:
        consumer.close()
        producer.close()

    logger.info("=" * 60)
    logger.info("  STREAM PROCESSOR COMPLETE")
    logger.info("  Processed: %d", processed)
    logger.info("  Errors:    %d", errors)
    logger.info("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_stream_processor()
