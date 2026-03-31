"""
Base extractor class for job site specific extractors.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional
from urllib.parse import urljoin

from ..models import JobPosting


class BaseExtractor(ABC):
    """Base class for site-specific job extractors."""

    def __init__(self, domain: str):
        self.domain = domain

    @abstractmethod
    async def extract_jobs(self, content: str, url: str) -> List[JobPosting]:
        """Extract job postings from page content."""
        raise NotImplementedError

    @abstractmethod
    async def extract_job_urls(self, content: str, url: str) -> List[str]:
        """Extract additional URLs to crawl from this page."""
        raise NotImplementedError

    async def discover_job_urls(self, base_url: str, content: str) -> List[str]:
        """Backwards-compatible alias."""
        return await self.extract_job_urls(content=content, url=base_url)

    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object."""
        if not date_str:
            return None

        formats = [
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue

        if "ago" in date_str.lower():
            return self._parse_relative_date(date_str)

        return None

    def _parse_relative_date(self, date_str: str) -> Optional[datetime]:
        import re
        from datetime import timedelta

        now = datetime.now()
        pattern = r"(\d+)\s+(day|week|month|hour)s?\s+ago"
        match = re.search(pattern, date_str.lower().strip())
        if not match:
            return None

        amount = int(match.group(1))
        unit = match.group(2)
        if unit == "day":
            return now - timedelta(days=amount)
        if unit == "week":
            return now - timedelta(weeks=amount)
        if unit == "month":
            return now - timedelta(days=amount * 30)
        if unit == "hour":
            return now - timedelta(hours=amount)
        return None

    def clean_text(self, text: str) -> str:
        if not text:
            return ""
        return " ".join(text.split()).strip()

    def make_absolute_url(self, base_url: str, relative_url: str) -> str:
        return urljoin(base_url, relative_url)


__all__ = ["BaseExtractor"]

