import asyncio
import aiohttp
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from dataclasses import dataclass
import hashlib

from .models import JobPosting
from .extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


@dataclass
class CrawlConfig:
    max_concurrent_requests: int = 5
    delay_between_requests: float = 1.0
    request_timeout: int = 30
    max_retries: int = 3
    user_agent: str = "RAG-Platform-Crawler/1.0"
    respect_robots_txt: bool = True
    freshness_days: int = 3
    max_pages_per_site: int = 100


class RateLimiter:
    def __init__(self, delay: float):
        self.delay = delay
        self.last_request_time = {}

    async def wait_if_needed(self, domain: str):
        current_time = time.time()
        last_time = self.last_request_time.get(domain, 0)
        if current_time - last_time < self.delay:
            await asyncio.sleep(self.delay - (current_time - last_time))
        self.last_request_time[domain] = time.time()


class RobotsChecker:
    def __init__(self):
        self.robots_cache = {}

    async def can_fetch(self, url: str, user_agent: str) -> bool:
        domain = urlparse(url).netloc
        if domain not in self.robots_cache:
            await self._load_robots(domain)
        rp = self.robots_cache.get(domain)
        if rp is None:
            return True
        return rp.can_fetch(user_agent, url)

    async def _load_robots(self, domain: str):
        robots_url = f"https://{domain}/robots.txt"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(robots_url, timeout=10) as response:
                    if response.status != 200:
                        self.robots_cache[domain] = None
                        return
                    robots_content = await response.text()
                    rp = RobotFileParser()
                    rp.set_url(robots_url)
                    rp.parse(robots_content.splitlines())
                    self.robots_cache[domain] = rp
        except Exception as e:
            logger.warning(f"Failed to load robots.txt for {domain}: {e}")
            self.robots_cache[domain] = None


class WebCrawler:
    def __init__(self, config: CrawlConfig = None):
        self.config = config or CrawlConfig()
        self.rate_limiter = RateLimiter(self.config.delay_between_requests)
        self.robots_checker = RobotsChecker()
        self.session = None
        self.crawled_urls = set()
        self.extractors = {}
        self.crawl_stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "jobs_extracted": 0,
            "fresh_jobs": 0,
        }

    def register_extractor(self, domain: str, extractor: BaseExtractor):
        self.extractors[domain] = extractor

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=self.config.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"User-Agent": self.config.user_agent},
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def crawl_sites(self, sites: List[Dict[str, str]]) -> List[JobPosting]:
        semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        tasks = [asyncio.create_task(self._crawl_site_with_semaphore(semaphore, site)) for site in sites]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_jobs = []
        for result in results:
            if isinstance(result, list):
                all_jobs.extend(result)
        fresh_jobs = self._filter_fresh_jobs(all_jobs)
        self.crawl_stats["fresh_jobs"] = len(fresh_jobs)
        return fresh_jobs

    async def _crawl_site_with_semaphore(self, semaphore: asyncio.Semaphore, site: Dict[str, str]) -> List[JobPosting]:
        async with semaphore:
            return await self._crawl_site(site)

    async def _crawl_site(self, site: Dict[str, str]) -> List[JobPosting]:
        domain = site["domain"]
        start_urls = site.get("start_urls", [])
        if domain not in self.extractors:
            return []

        extractor = self.extractors[domain]
        jobs = []
        urls_to_crawl = set(start_urls)
        crawled_count = 0
        while urls_to_crawl and crawled_count < self.config.max_pages_per_site:
            url = urls_to_crawl.pop()
            if url in self.crawled_urls:
                continue
            try:
                if self.config.respect_robots_txt:
                    if not await self.robots_checker.can_fetch(url, self.config.user_agent):
                        continue
                await self.rate_limiter.wait_if_needed(domain)
                page_content = await self._fetch_page(url)
                if page_content is None:
                    continue
                self.crawled_urls.add(url)
                crawled_count += 1
                page_jobs = await extractor.extract_jobs(page_content, url)
                jobs.extend(page_jobs)
                self.crawl_stats["jobs_extracted"] += len(page_jobs)
                additional_urls = await extractor.extract_job_urls(page_content, url)
                for additional_url in additional_urls:
                    if additional_url not in self.crawled_urls:
                        urls_to_crawl.add(additional_url)
            except Exception as e:
                logger.error(f"Error crawling {url}: {e}")
                self.crawl_stats["failed_requests"] += 1
        return jobs

    async def _fetch_page(self, url: str) -> Optional[str]:
        for attempt in range(self.config.max_retries):
            try:
                self.crawl_stats["total_requests"] += 1
                async with self.session.get(url) as response:
                    if response.status == 200:
                        content = await response.text()
                        self.crawl_stats["successful_requests"] += 1
                        return content
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {url} (attempt {attempt + 1})")
            except Exception as e:
                logger.error(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < self.config.max_retries - 1:
                await asyncio.sleep(2**attempt)
        self.crawl_stats["failed_requests"] += 1
        return None

    def _filter_fresh_jobs(self, jobs: List[JobPosting]) -> List[JobPosting]:
        cutoff_date = datetime.now() - timedelta(days=self.config.freshness_days)
        fresh_jobs = []
        for job in jobs:
            if job.posted_date and job.posted_date >= cutoff_date:
                fresh_jobs.append(job)
            elif job.posted_date is None:
                fresh_jobs.append(job)
        return fresh_jobs

    def get_crawl_stats(self) -> Dict:
        return self.crawl_stats.copy()

    async def crawl_single_url(self, url: str, extractor: BaseExtractor) -> List[JobPosting]:
        if self.config.respect_robots_txt:
            if not await self.robots_checker.can_fetch(url, self.config.user_agent):
                return []
        domain = urlparse(url).netloc
        await self.rate_limiter.wait_if_needed(domain)
        page_content = await self._fetch_page(url)
        if page_content is None:
            return []
        jobs = await extractor.extract_jobs(page_content, url)
        return self._filter_fresh_jobs(jobs)


class CrawlSession:
    def __init__(self, session_id: str = None):
        self.session_id = session_id or self._generate_session_id()
        self.start_time = datetime.now()
        self.end_time = None
        self.total_jobs = 0
        self.fresh_jobs = 0
        self.sites_crawled = []
        self.errors = []

    def _generate_session_id(self) -> str:
        return hashlib.md5(datetime.now().isoformat().encode()).hexdigest()[:8]

    def add_site_result(self, domain: str, job_count: int, error: str = None):
        result = {"domain": domain, "job_count": job_count, "timestamp": datetime.now().isoformat()}
        if error:
            result["error"] = error
            self.errors.append(error)
        self.sites_crawled.append(result)
        self.total_jobs += job_count

    def finish(self, fresh_job_count: int):
        self.end_time = datetime.now()
        self.fresh_jobs = fresh_job_count

    def to_dict(self) -> Dict:
        return {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "total_jobs": self.total_jobs,
            "fresh_jobs": self.fresh_jobs,
            "sites_crawled": self.sites_crawled,
            "errors": self.errors,
            "duration_seconds": (self.end_time - self.start_time).total_seconds() if self.end_time else None,
        }


__all__ = ["CrawlConfig", "CrawlSession", "WebCrawler"]

