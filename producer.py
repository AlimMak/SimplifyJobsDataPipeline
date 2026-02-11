"""
Kafka Producer — scrapes job listings and publishes them to topics.

Demonstrates:
  • Producer configuration (acks, retries, serialization)
  • Publishing to multiple topics
  • Message keys for partitioning
  • Delivery callbacks
"""

import json
import logging
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPICS,
    CATEGORY_PARTITION_MAP,
)
from scraper import scrape_and_clean, fetch_readme, extract_raw_rows
from models import JobListing, RawJobRow

logger = logging.getLogger(__name__)


def create_producer() -> KafkaProducer:
    """
    Create a configured Kafka producer.

    Key settings:
      - acks='all': Wait for all replicas to acknowledge (strongest guarantee)
      - retries=3: Auto-retry on transient failures
      - value_serializer: JSON encoding for message values
      - key_serializer: UTF-8 encoding for message keys
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        retries=3,
        retry_backoff_ms=500,
        max_in_flight_requests_per_connection=1,  # strict ordering
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        compression_type="gzip",
        client_id="job-scraper-producer",
    )


def on_send_success(record_metadata):
    """Callback for successful message delivery."""
    logger.debug(
        "Delivered → topic=%s partition=%d offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(excp):
    """Callback for failed message delivery."""
    logger.error("Delivery failed: %s", excp)


def publish_raw_rows(producer: KafkaProducer, raw_rows: list[RawJobRow]):
    """
    Stage 1: Publish raw HTML rows to the 'raw-job-listings' topic.
    This preserves the original data before any transformation.
    """
    topic = TOPICS["raw"]
    logger.info("Publishing %d raw rows to topic '%s'", len(raw_rows), topic)

    for row in raw_rows:
        message = {
            "html": row.html,
            "category": row.category,
            "row_index": row.row_index,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }

        future = producer.send(
            topic,
            key=f"row-{row.row_index}",
            value=message,
        )
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

    producer.flush()
    logger.info("✓ Published all raw rows")


def publish_cleaned_jobs(producer: KafkaProducer, jobs: list[JobListing]):
    """
    Stage 2: Publish cleaned job listings to the 'cleaned-jobs' topic.

    Message key = job_id (ensures same job always goes to same partition).
    """
    topic = TOPICS["cleaned"]
    logger.info("Publishing %d cleaned jobs to topic '%s'", len(jobs), topic)

    for job in jobs:
        future = producer.send(
            topic,
            key=job.job_id,
            value=json.loads(job.to_json()),
        )
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

    producer.flush()
    logger.info("✓ Published all cleaned jobs")


def publish_by_category(producer: KafkaProducer, jobs: list[JobListing]):
    """
    Stage 3: Publish to 'jobs-by-category' with explicit partition assignment.

    Each category maps to a specific partition, allowing consumers to
    subscribe to only the categories they care about.
    """
    topic = TOPICS["by_category"]
    logger.info("Publishing %d jobs by category to topic '%s'", len(jobs), topic)

    category_counts: dict[str, int] = {}

    for job in jobs:
        partition = CATEGORY_PARTITION_MAP.get(job.category, 0)
        category_counts[job.category] = category_counts.get(job.category, 0) + 1

        future = producer.send(
            topic,
            key=job.category,
            value=json.loads(job.to_json()),
            partition=partition,
        )
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

    producer.flush()

    logger.info("✓ Published by category:")
    for cat, count in sorted(category_counts.items()):
        logger.info("    %-45s → partition %d  (%d jobs)",
                     cat, CATEGORY_PARTITION_MAP.get(cat, 0), count)


def publish_alerts(producer: KafkaProducer, jobs: list[JobListing]):
    """
    Stage 4: Publish high-interest jobs to the 'job-alerts' topic.

    Filtering criteria (from config):
      - Location matches preferred list
      - Company matches FAANG+ or keyword list
      - Posted within ALERT_MAX_AGE_DAYS
      - Not closed
    """
    from config import (
        ALERT_LOCATIONS,
        ALERT_COMPANIES_KEYWORDS,
        ALERT_MAX_AGE_DAYS,
        ALERT_EXCLUDE_CLOSED,
    )

    topic = TOPICS["alerts"]
    alert_jobs = []

    for job in jobs:
        if ALERT_EXCLUDE_CLOSED and job.is_closed:
            continue
        if job.age_days > ALERT_MAX_AGE_DAYS:
            continue

        # Check location match
        location_match = any(
            any(loc_kw.lower() in loc.lower() for loc_kw in ALERT_LOCATIONS)
            for loc in job.location
        )

        # Check company match
        company_match = job.is_faang or any(
            kw.lower() in job.company.lower()
            for kw in ALERT_COMPANIES_KEYWORDS
        )

        if location_match or company_match:
            alert_jobs.append(job)

    logger.info("Publishing %d alert-worthy jobs to topic '%s'", len(alert_jobs), topic)

    for job in alert_jobs:
        message = json.loads(job.to_json())
        message["alert_reason"] = []
        if job.is_faang:
            message["alert_reason"].append("FAANG+")
        if any(kw.lower() in job.company.lower() for kw in ALERT_COMPANIES_KEYWORDS):
            message["alert_reason"].append("Top Company")
        for loc in job.location:
            for loc_kw in ALERT_LOCATIONS:
                if loc_kw.lower() in loc.lower():
                    message["alert_reason"].append(f"Location: {loc}")
                    break

        future = producer.send(
            topic,
            key=job.company,
            value=message,
        )
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

    producer.flush()
    logger.info("✓ Published all alerts")


def run_producer():
    """
    Main producer workflow:
      1. Scrape the GitHub README
      2. Extract raw rows → publish to 'raw-job-listings'
      3. Clean & parse rows → publish to 'cleaned-jobs'
      4. Partition by category → publish to 'jobs-by-category'
      5. Filter high-interest → publish to 'job-alerts'
    """
    logger.info("=" * 60)
    logger.info("  JOB LISTINGS KAFKA PRODUCER")
    logger.info("=" * 60)

    producer = create_producer()

    try:
        # Scrape
        start = time.time()
        markdown = fetch_readme()
        raw_rows = list(extract_raw_rows(markdown))
        logger.info("Scraping took %.2fs — %d raw rows found", time.time() - start, len(raw_rows))

        # Clean
        from scraper import scrape_and_clean
        jobs = scrape_and_clean()

        # Publish to all topics
        publish_raw_rows(producer, raw_rows)
        publish_cleaned_jobs(producer, jobs)
        publish_by_category(producer, jobs)
        publish_alerts(producer, jobs)

        logger.info("=" * 60)
        logger.info("  PRODUCER COMPLETE — %d jobs published across %d topics",
                     len(jobs), len(TOPICS))
        logger.info("=" * 60)

    except Exception as e:
        logger.exception("Producer failed: %s", e)
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_producer()
