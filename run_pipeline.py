"""
Pipeline Orchestrator — runs all components in sequence for demo purposes.

In production, each component would run as an independent service.
This script runs them sequentially to demonstrate the full flow.
"""

import logging
import sys
import os

# Ensure src is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS

logger = logging.getLogger(__name__)


def check_kafka_connection() -> bool:
    """Verify Kafka broker is reachable."""
    try:
        from kafka import KafkaClient
        client = KafkaClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        client.close()
        return True
    except Exception:
        return False


def run_demo_without_kafka():
    """
    Run the scraping and cleaning pipeline without Kafka.
    Useful for testing the data extraction logic independently.
    """
    from scraper import scrape_and_clean
    from consumer_analytics import JobAnalytics
    import json

    logger.info("=" * 70)
    logger.info("  RUNNING IN DEMO MODE (no Kafka broker required)")
    logger.info("=" * 70)

    # Step 1: Scrape and clean
    logger.info("\n📥 Step 1: Scraping & cleaning job listings...")
    jobs = scrape_and_clean()
    logger.info("   Found %d unique job listings\n", len(jobs))

    # Step 2: Simulate analytics consumer
    logger.info("📊 Step 2: Running analytics...")
    analytics = JobAnalytics()
    for job in jobs:
        analytics.process(json.loads(job.to_json()))
    analytics.print_dashboard()

    # Step 3: Simulate alerts
    from config import (
        ALERT_LOCATIONS,
        ALERT_COMPANIES_KEYWORDS,
        ALERT_MAX_AGE_DAYS,
    )
    from consumer_alerts import format_alert

    logger.info("\n🚨 Step 3: Generating alerts...")
    alert_count = 0
    for job in jobs:
        if job.is_closed or job.age_days > ALERT_MAX_AGE_DAYS:
            continue

        location_match = any(
            any(kw.lower() in loc.lower() for kw in ALERT_LOCATIONS)
            for loc in job.location
        )
        company_match = job.is_faang or any(
            kw.lower() in job.company.lower()
            for kw in ALERT_COMPANIES_KEYWORDS
        )

        if location_match or company_match:
            job_dict = json.loads(job.to_json())
            job_dict["alert_reason"] = []
            if job.is_faang:
                job_dict["alert_reason"].append("FAANG+")
            if company_match:
                job_dict["alert_reason"].append("Top Company")
            for loc in job.location:
                for kw in ALERT_LOCATIONS:
                    if kw.lower() in loc.lower():
                        job_dict["alert_reason"].append(f"Location: {loc}")
                        break

            print(format_alert(job_dict))
            alert_count += 1

    # Step 4: SQLite storage
    from consumer_store import init_db, upsert_job

    logger.info("\n💾 Step 4: Storing to SQLite...")
    conn = init_db()
    for job in jobs:
        upsert_job(conn, json.loads(job.to_json()))
    conn.commit()
    conn.close()
    logger.info("   Stored %d jobs to database\n", len(jobs))

    # Summary
    logger.info("=" * 70)
    logger.info("  PIPELINE COMPLETE")
    logger.info("  Jobs scraped:   %d", len(jobs))
    logger.info("  Alerts fired:   %d", alert_count)
    logger.info("  Database:       data/jobs.db")
    logger.info("=" * 70)


def run_full_pipeline():
    """
    Run the full Kafka-based pipeline.

    Order:
      1. Create topics
      2. Run producer (scrape → publish to all topics)
      3. Run stream processor (raw → cleaned)
      4. Run all three consumer groups
    """
    from admin_topics import create_topics
    from producer import run_producer
    from stream_processor import run_stream_processor
    from consumer_store import run_store_consumer
    from consumer_analytics import run_analytics_consumer
    from consumer_alerts import run_alerts_consumer

    logger.info("=" * 70)
    logger.info("  FULL KAFKA PIPELINE")
    logger.info("  Broker: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("=" * 70)

    # Step 1: Create topics
    logger.info("\n📋 Step 1: Creating topics...")
    create_topics()

    # Step 2: Producer
    logger.info("\n📤 Step 2: Running producer...")
    run_producer()

    # Step 3: Stream processor
    logger.info("\n⚙️ Step 3: Running stream processor...")
    run_stream_processor()

    # Step 4: Consumer Group 1 - Store
    logger.info("\n💾 Step 4: Running store consumer...")
    run_store_consumer()

    # Step 5: Consumer Group 2 - Analytics
    logger.info("\n📊 Step 5: Running analytics consumer...")
    run_analytics_consumer()

    # Step 6: Consumer Group 3 - Alerts
    logger.info("\n🚨 Step 6: Running alerts consumer...")
    run_alerts_consumer()

    logger.info("\n" + "=" * 70)
    logger.info("  ALL PIPELINE STAGES COMPLETE")
    logger.info("=" * 70)


def main():
    if "--demo" in sys.argv or not check_kafka_connection():
        if "--demo" not in sys.argv:
            logger.warning(
                "Cannot connect to Kafka at %s.\n"
                "  Running in demo mode (no broker required).\n"
                "  To use Kafka: docker-compose up -d\n",
                KAFKA_BOOTSTRAP_SERVERS,
            )
        run_demo_without_kafka()
    else:
        run_full_pipeline()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main()
