"""
Scraper & cleaner for the SimplifyJobs New-Grad-Positions GitHub README.

Parses the HTML tables embedded in the Markdown, extracts structured job
records, and applies cleaning / normalisation.
"""

from __future__ import annotations

import re
import logging
from datetime import datetime, timezone
from typing import Generator

import requests
from bs4 import BeautifulSoup, Tag

from models import JobListing, RawJobRow
from config import GITHUB_RAW_URL, CATEGORIES

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Emoji flag constants
# ---------------------------------------------------------------------------
FLAG_FAANG = "\U0001f525"          # 🔥
FLAG_NO_SPONSOR = "\U0001f6c2"     # 🛂
FLAG_US_CITIZEN = "\U0001f1fa\U0001f1f8"  # 🇺🇸
FLAG_CLOSED = "\U0001f512"         # 🔒
FLAG_ADV_DEGREE = "\U0001f393"     # 🎓

# Section header → category mapping
_SECTION_PATTERNS = {
    "software engineering": "Software Engineering",
    "product management": "Product Management",
    "data science": "Data Science, AI & Machine Learning",
    "quantitative finance": "Quantitative Finance",
    "hardware engineering": "Hardware Engineering",
    "other": "Other",
}


def fetch_readme(url: str = GITHUB_RAW_URL) -> str:
    """Download the raw README markdown from GitHub."""
    logger.info("Fetching README from %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _detect_category(heading_text: str) -> str:
    """Map a section heading to a canonical category name."""
    lower = heading_text.lower()
    for pattern, category in _SECTION_PATTERNS.items():
        if pattern in lower:
            return category
    return "Other"


def extract_raw_rows(markdown: str) -> Generator[RawJobRow, None, None]:
    """
    Yield RawJobRow objects — one per <tr> in each category table.
    Keeps track of which category section each row belongs to.
    """
    soup = BeautifulSoup(markdown, "html.parser")

    current_category = "Other"
    row_idx = 0

    for element in soup.children:
        # Detect category headings (## with emoji + text)
        if isinstance(element, Tag) and element.name in ("h2", "h3"):
            current_category = _detect_category(element.get_text())

        # Find all tables in the document
        if isinstance(element, Tag):
            for table in element.find_all("table") if element.name == "table" else [element] if element.name == "table" else element.find_all("table"):
                tbody = table.find("tbody")
                if not tbody:
                    continue
                for tr in tbody.find_all("tr"):
                    yield RawJobRow(
                        html=str(tr),
                        category=current_category,
                        row_index=row_idx,
                    )
                    row_idx += 1

    # Fallback: if BeautifulSoup doesn't find tables cleanly due to mixed
    # markdown/html, do a regex-based extraction
    if row_idx == 0:
        logger.warning("No tables found via BS4, falling back to regex parsing")
        yield from _regex_extract_rows(markdown)


def _regex_extract_rows(markdown: str) -> Generator[RawJobRow, None, None]:
    """Fallback regex-based row extraction."""
    current_category = "Other"
    row_idx = 0

    # Detect section headers
    section_re = re.compile(r"^##\s+.+?(?:New Grad Roles)", re.MULTILINE)
    tr_re = re.compile(r"<tr>\s*.*?</tr>", re.DOTALL)

    lines = markdown.split("\n")
    for line in lines:
        # Check for section headers
        if line.startswith("## "):
            current_category = _detect_category(line)

        # Find table rows
        for match in tr_re.finditer(line):
            yield RawJobRow(
                html=match.group(0),
                category=current_category,
                row_index=row_idx,
            )
            row_idx += 1

    # Process full text for multi-line rows
    if row_idx == 0:
        sections = re.split(r"(?=^## )", markdown, flags=re.MULTILINE)
        for section in sections:
            header_match = re.match(r"^## .+", section)
            if header_match:
                current_category = _detect_category(header_match.group(0))

            for match in tr_re.finditer(section):
                yield RawJobRow(
                    html=match.group(0),
                    category=current_category,
                    row_index=row_idx,
                )
                row_idx += 1


# ---------------------------------------------------------------------------
# Cleaning / parsing a single <tr> into a JobListing
# ---------------------------------------------------------------------------

def _extract_text(tag: Tag | None) -> str:
    """Get cleaned text from a BS4 tag."""
    if tag is None:
        return ""
    return tag.get_text(separator=" ", strip=True)


def _extract_links(td: Tag) -> tuple[str | None, str | None]:
    """
    Extract (apply_url, simplify_url) from the Application column.
    The column typically has two links: a direct apply and a Simplify link.
    """
    apply_url = None
    simplify_url = None

    for a_tag in td.find_all("a", href=True):
        href = a_tag["href"]
        # Strip tracking params
        clean = re.sub(r"[?&]utm_source=Simplify.*", "", href)
        clean = re.sub(r"[?&]ref=Simplify.*", "", clean)

        if "simplify.jobs" in href:
            simplify_url = clean
        elif apply_url is None:
            apply_url = clean

    return apply_url, simplify_url


def _parse_locations(td: Tag) -> list[str]:
    """
    Parse the Location column. Handles:
    - Simple text: "NYC"
    - Multi-line with <br/>: "Lincoln, NE</br>Remote in USA"
    - Expandable <details>: "<details><summary>6 locations</summary>..."
    """
    locations = []

    # Check for <details> tag (expandable list)
    details = td.find("details")
    if details:
        for loc in details.stripped_strings:
            loc = loc.strip()
            if loc and "location" not in loc.lower():
                locations.append(loc)
        return locations if locations else [_extract_text(td)]

    # Use BS4's internal representation to split on <br> elements
    # This handles all variants: <br>, <br/>, </br>
    parts = []
    current = []
    for child in td.children:
        if isinstance(child, Tag) and child.name == "br":
            text = "".join(current).strip()
            if text:
                parts.append(text)
            current = []
        else:
            if isinstance(child, Tag):
                current.append(child.get_text(strip=True))
            else:
                current.append(str(child).strip())
    # Don't forget the last segment
    text = "".join(current).strip()
    if text:
        parts.append(text)

    if parts:
        return parts

    # Fallback: just get all text
    full_text = _extract_text(td)
    return [full_text] if full_text else ["Unknown"]


def _parse_age(age_text: str) -> int:
    """Convert age text like '2d', '1mo', '1w' to days integer."""
    age_text = age_text.strip().lower()

    match = re.match(r"(\d+)\s*(d|day|days|w|week|weeks|mo|month|months|y|year|years)", age_text)
    if not match:
        return 0

    num = int(match.group(1))
    unit = match.group(2)

    if unit.startswith("d"):
        return num
    elif unit.startswith("w"):
        return num * 7
    elif unit.startswith("mo"):
        return num * 30
    elif unit.startswith("y"):
        return num * 365

    return 0


def _check_flags(text: str) -> dict:
    """Detect emoji flags in text."""
    return {
        "is_faang": FLAG_FAANG in text,
        "no_sponsorship": FLAG_NO_SPONSOR in text,
        "us_citizenship": FLAG_US_CITIZEN in text,
        "is_closed": FLAG_CLOSED in text,
        "advanced_degree": FLAG_ADV_DEGREE in text,
    }


def clean_row(raw: RawJobRow) -> JobListing | None:
    """
    Parse a single raw HTML <tr> row into a structured JobListing.
    Returns None if the row can't be parsed (e.g. header, separator).
    """
    soup = BeautifulSoup(
        # Normalize all BR variants to a consistent <br/> BEFORE BS4 parses
        re.sub(r"</?\s*br\s*/?\s*>", "<br/>", raw.html, flags=re.IGNORECASE),
        "html.parser",
    )
    cells = soup.find_all("td")

    if len(cells) < 4:
        return None

    # --- Company (column 0) ---
    company_cell = cells[0]
    company_text = _extract_text(company_cell)

    # Handle "↳" continuation rows (sub-listings under same company)
    if company_text.strip() == "↳":
        company_text = "↳ (see above)"

    # Clean company name: remove 🔥 and link artifacts
    company_clean = company_text.replace(FLAG_FAANG, "").strip()
    company_clean = re.sub(r"\s+", " ", company_clean)

    # Detect flags from company + role columns combined
    combined_text = _extract_text(company_cell) + " " + _extract_text(cells[1])
    flags = _check_flags(combined_text)

    # --- Role (column 1) ---
    role = _extract_text(cells[1])
    # Clean emoji flags from role text
    for flag in [FLAG_FAANG, FLAG_NO_SPONSOR, FLAG_US_CITIZEN, FLAG_CLOSED, FLAG_ADV_DEGREE]:
        role = role.replace(flag, "").strip()
    role = re.sub(r"\s+", " ", role)

    # --- Location (column 2) ---
    locations = _parse_locations(cells[2])

    # --- Application links (column 3) ---
    apply_url, simplify_url = _extract_links(cells[3])

    # --- Age (column 4, if present) ---
    age_text = _extract_text(cells[4]) if len(cells) > 4 else "0d"
    age_days = _parse_age(age_text)

    return JobListing(
        company=company_clean,
        role=role,
        location=locations,
        apply_url=apply_url,
        simplify_url=simplify_url,
        age_text=age_text,
        age_days=age_days,
        category=raw.category,
        is_faang=flags["is_faang"],
        is_closed=flags["is_closed"],
        no_sponsorship=flags["no_sponsorship"],
        us_citizenship=flags["us_citizenship"],
        advanced_degree=flags["advanced_degree"],
        scraped_at=datetime.now(timezone.utc).isoformat(),
    )


def scrape_and_clean(url: str = GITHUB_RAW_URL) -> list[JobListing]:
    """
    Full pipeline: fetch → extract raw rows → clean → deduplicate.
    Returns a list of cleaned JobListing objects.
    """
    markdown = fetch_readme(url)
    raw_rows = list(extract_raw_rows(markdown))
    logger.info("Extracted %d raw rows", len(raw_rows))

    jobs: list[JobListing] = []
    seen_ids: set[str] = set()

    for raw in raw_rows:
        job = clean_row(raw)
        if job is None:
            continue
        if job.job_id in seen_ids:
            logger.debug("Skipping duplicate: %s", job.job_id)
            continue
        seen_ids.add(job.job_id)
        jobs.append(job)

    logger.info("Cleaned %d unique job listings", len(jobs))
    return jobs


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    jobs = scrape_and_clean()
    for j in jobs[:5]:
        print(f"  {j.company:30s} | {j.role:50s} | {', '.join(j.location)}")
    print(f"\n  Total: {len(jobs)} jobs")
