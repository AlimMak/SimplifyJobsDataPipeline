"""
Consumer Group 1: Job Store
Consumes from 'cleaned-jobs' and persists to SQLite.

Demonstrates:
  • Consumer group membership (cg-job-store)
  • Auto-offset management
  • At-least-once delivery with manual commit
  • Idempotent writes (upsert by job_id)
"""

import json
import logging
import os
import sqlite3
from typing import Optional

from kafka import KafkaConsumer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPICS,
    CONSUMER_GROUPS,
    SQLITE_DB_PATH,
)
from models import JobListing

logger = logging.getLogger(__name__)


def init_db(db_path: str = SQLITE_DB_PATH) -> sqlite3.Connection:
    """Create the SQLite database and jobs table if they don't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id          TEXT PRIMARY KEY,
            company         TEXT NOT NULL,
            role            TEXT NOT NULL,
            location        TEXT NOT NULL,       -- JSON array
            apply_url       TEXT,
            simplify_url    TEXT,
            age_text        TEXT,
            age_days        INTEGER,
            category        TEXT,
            is_faang        BOOLEAN DEFAULT 0,
            is_closed       BOOLEAN DEFAULT 0,
            no_sponsorship  BOOLEAN DEFAULT 0,
            us_citizenship  BOOLEAN DEFAULT 0,
            advanced_degree BOOLEAN DEFAULT 0,
            scraped_at      TEXT,
            inserted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_category ON jobs(category)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_jobs_age ON jobs(age_days)
    """)
    conn.commit()
    logger.info("Database initialised at %s", db_path)
    return conn


def upsert_job(conn: sqlite3.Connection, job: dict):
    """Insert or update a job listing (idempotent by job_id)."""
    conn.execute("""
        INSERT INTO jobs (
            job_id, company, role, location, apply_url, simplify_url,
            age_text, age_days, category, is_faang, is_closed,
            no_sponsorship, us_citizenship, advanced_degree, scraped_at
        ) VALUES (
            :job_id, :company, :role, :location, :apply_url, :simplify_url,
            :age_text, :age_days, :category, :is_faang, :is_closed,
            :no_sponsorship, :us_citizenship, :advanced_degree, :scraped_at
        )
        ON CONFLICT(job_id) DO UPDATE SET
            age_text = excluded.age_text,
            age_days = excluded.age_days,
            is_closed = excluded.is_closed,
            scraped_at = excluded.scraped_at,
            updated_at = CURRENT_TIMESTAMP
    """, {
        **job,
        "location": json.dumps(job.get("location", [])),
        "is_faang": int(job.get("is_faang", False)),
        "is_closed": int(job.get("is_closed", False)),
        "no_sponsorship": int(job.get("no_sponsorship", False)),
        "us_citizenship": int(job.get("us_citizenship", False)),
        "advanced_degree": int(job.get("advanced_degree", False)),
    })


def create_consumer() -> KafkaConsumer:
    """
    Create a Kafka consumer for the store consumer group.

    Key settings:
      - group_id: All instances of this consumer share the 'cg-job-store' group
      - auto_offset_reset='earliest': Start from beginning if no committed offset
      - enable_auto_commit=False: We commit manually after DB write (at-least-once)
      - value_deserializer: JSON decoding
    """
    return KafkaConsumer(
        TOPICS["cleaned"],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUPS["store"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,       # manual commit for at-least-once
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=10000,      # stop after 10s of no messages
        client_id="job-store-consumer",
    )


def run_store_consumer():
    """
    Consume cleaned job listings and persist them to SQLite.

    Processing loop:
      1. Poll for messages
      2. Upsert each job into SQLite
      3. Commit offsets after successful DB write
    """
    logger.info("=" * 60)
    logger.info("  CONSUMER GROUP 1: JOB STORE (SQLite)")
    logger.info("  Group: %s", CONSUMER_GROUPS["store"])
    logger.info("  Topic: %s", TOPICS["cleaned"])
    logger.info("=" * 60)

    conn = init_db()
    consumer = create_consumer()

    total_processed = 0
    total_new = 0
    batch_size = 0

    try:
        for message in consumer:
            job_data = message.value

            # Check if this is a new insert or update
            cursor = conn.execute(
                "SELECT 1 FROM jobs WHERE job_id = ?",
                (job_data.get("job_id", ""),)
            )
            is_new = cursor.fetchone() is None

            upsert_job(conn, job_data)
            batch_size += 1

            if is_new:
                total_new += 1

            total_processed += 1

            # Commit DB + Kafka offset every 50 messages
            if batch_size >= 50:
                conn.commit()
                consumer.commit()
                logger.info(
                    "Committed batch: %d messages (offset=%d, partition=%d)",
                    batch_size, message.offset, message.partition,
                )
                batch_size = 0

        # Final commit
        if batch_size > 0:
            conn.commit()
            consumer.commit()

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    finally:
        consumer.close()
        conn.close()

    logger.info("=" * 60)
    logger.info("  STORE CONSUMER COMPLETE")
    logger.info("  Processed: %d messages", total_processed)
    logger.info("  New jobs:  %d", total_new)
    logger.info("  Database:  %s", SQLITE_DB_PATH)
    logger.info("=" * 60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_store_consumer()
