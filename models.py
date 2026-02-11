"""
Data models for job listings.
"""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class JobListing:
    """A single cleaned job listing."""

    company: str
    role: str
    location: list[str]
    apply_url: Optional[str]
    simplify_url: Optional[str]
    age_text: str                      # original e.g. "2d", "1mo"
    age_days: int                      # normalised to integer days
    category: str                      # section the job appeared under
    is_faang: bool = False             # 🔥 marker
    is_closed: bool = False            # 🔒 marker
    no_sponsorship: bool = False       # 🛂 marker
    us_citizenship: bool = False       # 🇺🇸 marker
    advanced_degree: bool = False      # 🎓 marker
    scraped_at: str = ""               # ISO timestamp of scrape
    job_id: str = ""                   # deterministic hash

    def __post_init__(self):
        if not self.job_id:
            self.job_id = self._generate_id()

    def _generate_id(self) -> str:
        """Deterministic ID based on company + role + locations."""
        raw = f"{self.company}|{self.role}|{'|'.join(sorted(self.location))}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    # -- Serialisation helpers for Kafka --

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    def to_bytes(self) -> bytes:
        return self.to_json().encode("utf-8")

    @classmethod
    def from_json(cls, raw: str | bytes) -> "JobListing":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        return cls(**data)


@dataclass
class RawJobRow:
    """A raw, unparsed table row from the README."""

    html: str
    category: str
    row_index: int

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> "RawJobRow":
        return cls(**json.loads(raw.decode("utf-8")))
