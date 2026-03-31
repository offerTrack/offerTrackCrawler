import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from .base import BaseExtractor
from ..models import JobPosting

logger = logging.getLogger(__name__)


class GenericSchemaOrgExtractor(BaseExtractor):
    """Generic extractor that understands Schema.org `JobPosting` JSON-LD."""

    async def extract_jobs(self, content: str, url: str) -> List[JobPosting]:
        soup = BeautifulSoup(content, "html.parser")

        jobs: List[JobPosting] = []
        for obj in self._iter_jsonld_objects(soup):
            for jp in self._iter_jobpostings(obj):
                job = self._jobposting_from_schema(jp, url=url)
                if job:
                    jobs.append(job)

        if not jobs:
            title = self._first_text(soup, ["h1", "title"])
            if title:
                jobs.append(
                    JobPosting(
                        title=title,
                        company=self._best_effort_company(soup) or urlparse(url).netloc,
                        url=url,
                        location=self._best_effort_location(soup),
                        description=self._best_effort_description(soup),
                        posted_date=self._best_effort_posted_date(soup),
                        source=self.domain,
                        raw={"_fallback": True},
                    )
                )
        return jobs

    async def extract_job_urls(self, content: str, url: str) -> List[str]:
        soup = BeautifulSoup(content, "html.parser")
        base_host = urlparse(url).netloc
        urls: List[str] = []
        for a in soup.select("a[href]"):
            href = a.get("href")
            if not href:
                continue
            abs_url = self.make_absolute_url(url, href)
            p = urlparse(abs_url)
            if p.scheme not in ("http", "https") or p.netloc != base_host:
                continue
            path = (p.path or "").lower()
            if any(k in path for k in ("/job", "/jobs", "/career", "/careers", "/position", "/positions")):
                urls.append(abs_url)

        seen = set()
        out: List[str] = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    def _iter_jsonld_objects(self, soup: BeautifulSoup) -> List[Any]:
        out: List[Any] = []
        for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
            text = tag.get_text(strip=True)
            if not text:
                continue
            try:
                out.append(json.loads(text))
            except Exception:
                continue
        return out

    def _iter_jobpostings(self, obj: Any) -> List[Dict[str, Any]]:
        postings: List[Dict[str, Any]] = []

        def visit(x: Any):
            if isinstance(x, dict):
                t = x.get("@type") or x.get("type")
                if isinstance(t, list):
                    is_job = any(str(v).lower() == "jobposting" for v in t)
                else:
                    is_job = str(t).lower() == "jobposting"
                if is_job:
                    postings.append(x)
                graph = x.get("@graph")
                if isinstance(graph, list):
                    for g in graph:
                        visit(g)
                for v in x.values():
                    visit(v)
            elif isinstance(x, list):
                for i in x:
                    visit(i)

        visit(obj)
        return postings

    def _jobposting_from_schema(self, jp: Dict[str, Any], url: str) -> Optional[JobPosting]:
        title = jp.get("title") or jp.get("name")
        if not title:
            return None
        hiring_org = jp.get("hiringOrganization") or {}
        company = hiring_org.get("name") if isinstance(hiring_org, dict) else None
        loc = self._schema_location(jp.get("jobLocation"))
        desc = jp.get("description")
        date_posted = self._parse_iso_date(jp.get("datePosted"))
        canonical_url = str(jp.get("url") or jp.get("sameAs") or url)
        return JobPosting(
            title=self.clean_text(str(title)),
            company=self.clean_text(company or self.domain),
            url=canonical_url,
            location=self.clean_text(loc) if loc else None,
            description=self.clean_text(desc) if isinstance(desc, str) else None,
            posted_date=date_posted,
            source=self.domain,
            raw={"schema_org": jp},
        )

    def _schema_location(self, job_location: Any) -> Optional[str]:
        if isinstance(job_location, str):
            return job_location
        if isinstance(job_location, list) and job_location:
            return self._schema_location(job_location[0])
        if isinstance(job_location, dict):
            addr = job_location.get("address")
            if isinstance(addr, str):
                return addr
            if isinstance(addr, dict):
                parts = [addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry")]
                parts = [p for p in parts if p]
                if parts:
                    return ", ".join(map(str, parts))
        return None

    def _parse_iso_date(self, value: Any) -> Optional[datetime]:
        if not value or not isinstance(value, str):
            return None
        v = value.strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                continue
        return self.parse_date(v)

    def _first_text(self, soup: BeautifulSoup, selectors: List[str]) -> Optional[str]:
        for sel in selectors:
            el = soup.select_one(sel)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt:
                    return self.clean_text(txt)
        return None

    def _best_effort_company(self, soup: BeautifulSoup) -> Optional[str]:
        meta = soup.find("meta", attrs={"property": "og:site_name"})
        if meta and meta.get("content"):
            return self.clean_text(meta["content"])
        return None

    def _best_effort_location(self, soup: BeautifulSoup) -> Optional[str]:
        for key in ("jobLocation", "location", "address"):
            meta = soup.find("meta", attrs={"name": key})
            if meta and meta.get("content"):
                return self.clean_text(meta["content"])
        return None

    def _best_effort_description(self, soup: BeautifulSoup) -> Optional[str]:
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return self.clean_text(meta["content"])
        return None

    def _best_effort_posted_date(self, soup: BeautifulSoup) -> Optional[datetime]:
        for key in ("datePosted", "article:published_time", "og:updated_time"):
            meta = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
            if meta and meta.get("content"):
                d = self.parse_date(meta["content"])
                if d:
                    return d
        return None


__all__ = ["GenericSchemaOrgExtractor"]

