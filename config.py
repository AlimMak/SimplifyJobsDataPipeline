"""
Kafka configuration and topic definitions for the job listings pipeline.
"""

# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------
TOPICS = {
    "raw": "raw-job-listings",       # Raw scraped HTML rows
    "cleaned": "cleaned-jobs",       # Parsed & normalised job records
    "by_category": "jobs-by-category",  # Partitioned by job category
    "alerts": "job-alerts",          # Filtered high-interest postings
}

# Number of partitions per topic (one per job category keeps ordering simple)
TOPIC_PARTITIONS = {
    "raw-job-listings": 1,
    "cleaned-jobs": 3,
    "jobs-by-category": 6,   # one per category
    "job-alerts": 1,
}

TOPIC_REPLICATION_FACTOR = 1  # single-broker dev setup

# ---------------------------------------------------------------------------
# Consumer groups
# ---------------------------------------------------------------------------
CONSUMER_GROUPS = {
    "store": "cg-job-store",         # Group 1 – persists to SQLite
    "analytics": "cg-job-analytics", # Group 2 – real-time stats
    "alerts": "cg-job-alerts",       # Group 3 – filtered notifications
}

# ---------------------------------------------------------------------------
# Job categories (mapped from README section headers)
# ---------------------------------------------------------------------------
CATEGORIES = [
    "Software Engineering",
    "Product Management",
    "Data Science, AI & Machine Learning",
    "Quantitative Finance",
    "Hardware Engineering",
    "Other",
]

CATEGORY_PARTITION_MAP = {cat: idx for idx, cat in enumerate(CATEGORIES)}

# ---------------------------------------------------------------------------
# Alert filters — customise to your preferences
# ---------------------------------------------------------------------------
ALERT_LOCATIONS = [
    "Remote",
    "SF",
    "NYC",
    "Austin",
    "Seattle",
    "San Francisco",
    "Mountain View",
    "Houston",
    "TX",
    "California",
    "CA",
]

ALERT_COMPANIES_KEYWORDS = [
    "Google", "Microsoft", "Amazon", "Meta", "Apple", "Netflix",
    "NVIDIA", "Stripe", "Coinbase", "Roblox", "Pinterest",
    "Airtable", "Anduril", "Toast", "Affirm", "Verkada",
]

ALERT_MAX_AGE_DAYS = 7  # only alert on jobs posted within this window
ALERT_EXCLUDE_CLOSED = True  # skip 🔒 listings

# ---------------------------------------------------------------------------
# Freshness filter — used by producer to only publish recent jobs
# ---------------------------------------------------------------------------
FRESH_JOBS_MAX_AGE_DAYS = 3  # only publish jobs posted in last 3 days to cleaned/alerts
PUBLISH_ALL_TO_RAW = True     # still keep ALL jobs in raw topic for historical record

# ---------------------------------------------------------------------------
# Data source
# ---------------------------------------------------------------------------
GITHUB_RAW_URL = (
    "https://raw.githubusercontent.com/"
    "SimplifyJobs/New-Grad-Positions/refs/heads/dev/README.md"
)

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
SQLITE_DB_PATH = "data/jobs.db"
