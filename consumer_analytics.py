"""
Consumer Group 2: Real-Time Analytics
Consumes from 'cleaned-jobs' and computes live statistics.

Demonstrates:
  • Independent consumer group (cg-job-analytics)
  • Same topic, different processing from Group 1
  • Stateful aggregation from a stream
  • Consumer group parallelism concept
"""

import json
import logging
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPICS,
    CONSUMER_GROUPS,
)

logger = logging.getLogger(__name__)


class JobAnalytics:
    """Aggregates statistics from the job listing stream."""

    def __init__(self):
        self.total_jobs = 0
        self.by_category: dict[str, int] = defaultdict(int)
        self.by_location: dict[str, int] = defaultdict(int)
        self.by_company: dict[str, int] = defaultdict(int)
        self.faang_count = 0
        self.closed_count = 0
        self.no_sponsor_count = 0
        self.us_only_count = 0
        self.remote_count = 0
        self.age_distribution: dict[str, int] = defaultdict(int)  # buckets
        self.recent_jobs: list[dict] = []  # last 10

    def process(self, job: dict):
        """Process a single job message and update stats."""
        self.total_jobs += 1
        self.by_category[job.get("category", "Unknown")] += 1
        self.by_company[job.get("company", "Unknown")] += 1

        # Location stats
        for loc in job.get("location", []):
            self.by_location[loc] += 1
            if "remote" in loc.lower():
                self.remote_count += 1

        # Flag stats
        if job.get("is_faang"):
            self.faang_count += 1
        if job.get("is_closed"):
            self.closed_count += 1
        if job.get("no_sponsorship"):
            self.no_sponsor_count += 1
        if job.get("us_citizenship"):
            self.us_only_count += 1

        # Age buckets
        age = job.get("age_days", 0)
        if age <= 1:
            self.age_distribution["Today"] += 1
        elif age <= 7:
            self.age_distribution["This week"] += 1
        elif age <= 14:
            self.age_distribution["Last 2 weeks"] += 1
        elif age <= 30:
            self.age_distribution["This month"] += 1
        else:
            self.age_distribution["Older"] += 1

        # Keep last 10 jobs
        self.recent_jobs.append({
            "company": job.get("company"),
            "role": job.get("role"),
            "age": job.get("age_text"),
        })
        if len(self.recent_jobs) > 10:
            self.recent_jobs.pop(0)

    def print_dashboard(self):
        """Print a terminal-based analytics dashboard."""
        print("\n" + "=" * 70)
        print("  📊  JOB LISTINGS ANALYTICS DASHBOARD")
        print(f"  📅  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        print(f"\n  Total Jobs: {self.total_jobs}")
        print(f"  🔥 FAANG+:  {self.faang_count}")
        print(f"  🏠 Remote:  {self.remote_count}")
        print(f"  🔒 Closed:  {self.closed_count}")
        print(f"  🛂 No Sponsor: {self.no_sponsor_count}")
        print(f"  🇺🇸 US Only: {self.us_only_count}")

        print("\n  ── By Category ──")
        for cat, count in sorted(self.by_category.items(), key=lambda x: -x[1]):
            bar = "█" * min(count, 40)
            print(f"  {cat:45s} {count:4d}  {bar}")

        print("\n  ── Top 15 Locations ──")
        top_locs = sorted(self.by_location.items(), key=lambda x: -x[1])[:15]
        for loc, count in top_locs:
            bar = "█" * min(count, 30)
            print(f"  {loc:35s} {count:4d}  {bar}")

        print("\n  ── Top 15 Companies (by # listings) ──")
        top_cos = sorted(self.by_company.items(), key=lambda x: -x[1])[:15]
        for co, count in top_cos:
            bar = "█" * min(count * 3, 30)
            print(f"  {co:35s} {count:4d}  {bar}")

        print("\n  ── Age Distribution ──")
        for bucket in ["Today", "This week", "Last 2 weeks", "This month", "Older"]:
            count = self.age_distribution.get(bucket, 0)
            bar = "█" * min(count, 40)
            print(f"  {bucket:20s} {count:4d}  {bar}")

        print("\n  ── Most Recent Jobs ──")
        for j in reversed(self.recent_jobs):
            print(f"  [{j['age']:>4s}]  {j['company']:25s}  {j['role']}")

        print("\n" + "=" * 70)


def create_consumer() -> KafkaConsumer:
    """
    Create a Kafka consumer for the analytics consumer group.

    This is an INDEPENDENT consumer group from the store consumer.
    Both groups receive ALL messages from the same topic — this is
    the Kafka pub/sub pattern via consumer groups.
    """
    return KafkaConsumer(
        TOPICS["cleaned"],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUPS["analytics"],    # Different group!
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=10000,
        client_id="job-analytics-consumer",
    )


def run_analytics_consumer():
    """
    Consume cleaned job listings and produce real-time analytics.

    This consumer group processes the SAME messages as the store consumer,
    but does completely different work (aggregation vs. persistence).
    """
    logger.info("=" * 60)
    logger.info("  CONSUMER GROUP 2: ANALYTICS")
    logger.info("  Group: %s", CONSUMER_GROUPS["analytics"])
    logger.info("  Topic: %s", TOPICS["cleaned"])
    logger.info("=" * 60)

    analytics = JobAnalytics()
    consumer = create_consumer()

    try:
        for message in consumer:
            analytics.process(message.value)

            # Update dashboard every 50 messages
            if analytics.total_jobs % 50 == 0:
                analytics.print_dashboard()

        # Final dashboard
        analytics.print_dashboard()

    except KeyboardInterrupt:
        logger.info("Analytics consumer interrupted")
        analytics.print_dashboard()
    finally:
        consumer.close()

    return analytics


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_analytics_consumer()
