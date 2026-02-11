"""
Consumer Group 3: Job Alerts
Consumes from 'job-alerts' topic and logs high-interest postings.

Demonstrates:
  • Consuming from a filtered/derived topic
  • Consumer group for a different topic than groups 1 & 2
  • Alert formatting and notification patterns
"""

import json
import logging
import os
from datetime import datetime

from kafka import KafkaConsumer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPICS,
    CONSUMER_GROUPS,
)

logger = logging.getLogger(__name__)

ALERTS_LOG_PATH = "data/alerts.log"


def create_consumer() -> KafkaConsumer:
    """
    Create a Kafka consumer for the alerts consumer group.

    This group consumes from 'job-alerts' (not 'cleaned-jobs'),
    demonstrating that different consumer groups can subscribe
    to different topics.
    """
    return KafkaConsumer(
        TOPICS["alerts"],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUPS["alerts"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=10000,
        client_id="job-alerts-consumer",
    )


def format_alert(job: dict) -> str:
    """Format a job alert for display and logging."""
    reasons = job.get("alert_reason", [])
    locations = ", ".join(job.get("location", []))
    flags = []
    if job.get("is_faang"):
        flags.append("🔥 FAANG+")
    if job.get("no_sponsorship"):
        flags.append("🛂 No Sponsor")
    if job.get("us_citizenship"):
        flags.append("🇺🇸 US Only")
    if job.get("advanced_degree"):
        flags.append("🎓 Adv. Degree")

    flag_str = " | ".join(flags) if flags else "None"

    return (
        f"\n{'─' * 60}\n"
        f"  🚨  JOB ALERT\n"
        f"{'─' * 60}\n"
        f"  Company:   {job.get('company', 'N/A')}\n"
        f"  Role:      {job.get('role', 'N/A')}\n"
        f"  Location:  {locations}\n"
        f"  Posted:    {job.get('age_text', 'N/A')} ago\n"
        f"  Category:  {job.get('category', 'N/A')}\n"
        f"  Flags:     {flag_str}\n"
        f"  Reasons:   {', '.join(reasons)}\n"
        f"  Apply:     {job.get('apply_url', 'N/A')}\n"
        f"{'─' * 60}"
    )


def log_alert(job: dict, log_path: str = ALERTS_LOG_PATH):
    """Append alert to a persistent log file."""
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        timestamp = datetime.now().isoformat()
        f.write(f"[{timestamp}] {job.get('company')} | {job.get('role')} | "
                f"{', '.join(job.get('location', []))} | "
                f"Reasons: {', '.join(job.get('alert_reason', []))}\n")


def run_alerts_consumer():
    """
    Consume from the alerts topic and display/log notifications.

    This consumer demonstrates a common pattern: a derived topic
    (job-alerts) that has already been filtered by the producer,
    so the consumer only sees high-interest messages.
    """
    logger.info("=" * 60)
    logger.info("  CONSUMER GROUP 3: JOB ALERTS")
    logger.info("  Group: %s", CONSUMER_GROUPS["alerts"])
    logger.info("  Topic: %s", TOPICS["alerts"])
    logger.info("=" * 60)

    consumer = create_consumer()
    alert_count = 0

    try:
        for message in consumer:
            job = message.value
            alert_count += 1

            # Print formatted alert
            print(format_alert(job))

            # Persist to log
            log_alert(job)

            logger.debug(
                "Alert %d: offset=%d, partition=%d, key=%s",
                alert_count, message.offset, message.partition, message.key,
            )

    except KeyboardInterrupt:
        logger.info("Alerts consumer interrupted")
    finally:
        consumer.close()

    logger.info("=" * 60)
    logger.info("  ALERTS CONSUMER COMPLETE")
    logger.info("  Total alerts: %d", alert_count)
    logger.info("  Log file: %s", ALERTS_LOG_PATH)
    logger.info("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_alerts_consumer()
