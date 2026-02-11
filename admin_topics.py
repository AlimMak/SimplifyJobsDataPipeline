"""
Kafka topic administration — create, list, and describe topics.

Run this before starting the pipeline to ensure all topics exist.
"""

import logging
import sys

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_PARTITIONS,
    TOPIC_REPLICATION_FACTOR,
    TOPICS,
)

logger = logging.getLogger(__name__)


def create_topics():
    """Create all pipeline topics if they don't already exist."""
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id="job-pipeline-admin",
        )
    except NoBrokersAvailable:
        logger.error(
            "Cannot connect to Kafka at %s. Is the broker running?\n"
            "  → docker-compose up -d",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        sys.exit(1)

    new_topics = []
    for _key, topic_name in TOPICS.items():
        partitions = TOPIC_PARTITIONS.get(topic_name, 1)
        new_topics.append(
            NewTopic(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=TOPIC_REPLICATION_FACTOR,
            )
        )

    for topic in new_topics:
        try:
            admin.create_topics([topic], validate_only=False)
            logger.info("Created topic: %-25s (partitions=%d)", topic.name, topic.num_partitions)
        except TopicAlreadyExistsError:
            logger.info("Topic already exists: %s", topic.name)
        except Exception as e:
            logger.error("Failed to create topic %s: %s", topic.name, e)

    # List all topics
    existing = admin.list_topics()
    logger.info("\nAll topics on broker:")
    for t in sorted(existing):
        if not t.startswith("_"):
            logger.info("  • %s", t)

    admin.close()


def delete_topics():
    """Delete all pipeline topics (for resetting)."""
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="job-pipeline-admin",
    )
    topic_names = list(TOPICS.values())
    try:
        admin.delete_topics(topic_names)
        logger.info("Deleted topics: %s", topic_names)
    except Exception as e:
        logger.warning("Could not delete topics: %s", e)
    admin.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if "--delete" in sys.argv:
        delete_topics()
    else:
        create_topics()
