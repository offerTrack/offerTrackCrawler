"""
Crawling scheduler for managing web crawler execution and job freshness.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import json
import os

from .web_crawler import WebCrawler, CrawlConfig
from .models import JobPosting
from .extractors import GenericSchemaOrgExtractor

logger = logging.getLogger(__name__)


@dataclass
class CrawlJob:
    site_name: str
    url: str
    extractor_class: str
    priority: int = 1
    last_crawled: Optional[datetime] = None
    next_crawl: Optional[datetime] = None
    enabled: bool = True
    crawl_interval_hours: int = 6
    max_retries: int = 3
    retry_count: int = 0

    def should_crawl(self) -> bool:
        if not self.enabled:
            return False
        if self.next_crawl is None:
            return True
        return datetime.now() >= self.next_crawl

    def schedule_next_crawl(self):
        self.last_crawled = datetime.now()
        self.next_crawl = self.last_crawled + timedelta(hours=self.crawl_interval_hours)
        self.retry_count = 0

    def schedule_retry(self):
        self.retry_count += 1
        if self.retry_count <= self.max_retries:
            retry_delay = min(2 ** (self.retry_count - 1), 4)
            self.next_crawl = datetime.now() + timedelta(hours=retry_delay)
        else:
            self.schedule_next_crawl()


class CrawlScheduler:
    def __init__(self, config_path: str = "config/crawl_jobs.json"):
        self.config_path = config_path
        self.crawl_jobs: Dict[str, CrawlJob] = {}
        self.crawl_config = CrawlConfig()
        self.running = False
        self.freshness_days = 3
        self._load_config()

    def _load_config(self):
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                for job_config in config.get("crawl_jobs", []):
                    job = CrawlJob(**job_config)
                    self.crawl_jobs[job.site_name] = job
            except Exception as e:
                logger.error(f"Failed to load crawl config: {e}")
        else:
            self._create_default_config()

    def _create_default_config(self):
        default_jobs = [
            {
                "site_name": "example_careers",
                "url": "https://example.com/careers",
                "extractor_class": "GenericSchemaOrgExtractor",
                "priority": 1,
                "crawl_interval_hours": 6,
            }
        ]
        for job_config in default_jobs:
            self.crawl_jobs[job_config["site_name"]] = CrawlJob(**job_config)
        self._save_config()

    def _save_config(self):
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            config = {
                "crawl_jobs": [
                    {
                        "site_name": job.site_name,
                        "url": job.url,
                        "extractor_class": job.extractor_class,
                        "priority": job.priority,
                        "crawl_interval_hours": job.crawl_interval_hours,
                        "enabled": job.enabled,
                        "max_retries": job.max_retries,
                    }
                    for job in self.crawl_jobs.values()
                ]
            }
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save crawl config: {e}")

    def get_pending_jobs(self) -> List[CrawlJob]:
        pending = [job for job in self.crawl_jobs.values() if job.should_crawl()]
        return sorted(pending, key=lambda x: x.priority)

    def _create_extractor(self, extractor_class: str, domain: str):
        if extractor_class == "GenericSchemaOrgExtractor":
            return GenericSchemaOrgExtractor(domain=domain)
        raise ValueError(f"Unknown extractor_class: {extractor_class}")

    async def crawl_job(self, job: CrawlJob) -> List[JobPosting]:
        try:
            domain = job.url.split("/")[2] if "://" in job.url else job.url
            extractor = self._create_extractor(job.extractor_class, domain)
            sites = [{"domain": domain, "start_urls": [job.url]}]
            async with WebCrawler(config=self.crawl_config) as crawler:
                crawler.register_extractor(domain, extractor)
                job_postings = await crawler.crawl_sites(sites)
            cutoff_date = datetime.now() - timedelta(days=self.freshness_days)
            fresh_jobs = [p for p in job_postings if p.posted_date and p.posted_date >= cutoff_date]
            job.schedule_next_crawl()
            self._save_config()
            return fresh_jobs
        except Exception as e:
            logger.error(f"Failed to crawl {job.site_name}: {e}")
            job.schedule_retry()
            self._save_config()
            return []

    async def run_scheduled_crawls(self):
        pending_jobs = self.get_pending_jobs()
        all_fresh_jobs = []
        for job in pending_jobs:
            all_fresh_jobs.extend(await self.crawl_job(job))
            await asyncio.sleep(2)
        return all_fresh_jobs

    async def start_scheduler(self, check_interval_minutes: int = 30):
        self.running = True
        while self.running:
            try:
                await self.run_scheduled_crawls()
                await asyncio.sleep(check_interval_minutes * 60)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60)

    def stop_scheduler(self):
        self.running = False

    def get_status(self) -> Dict:
        return {
            "running": self.running,
            "total_jobs": len(self.crawl_jobs),
            "enabled_jobs": sum(1 for job in self.crawl_jobs.values() if job.enabled),
            "pending_jobs": len(self.get_pending_jobs()),
            "jobs": [
                {
                    "site_name": job.site_name,
                    "enabled": job.enabled,
                    "last_crawled": job.last_crawled.isoformat() if job.last_crawled else None,
                    "next_crawl": job.next_crawl.isoformat() if job.next_crawl else None,
                    "retry_count": job.retry_count,
                }
                for job in self.crawl_jobs.values()
            ],
        }


__all__ = ["CrawlJob", "CrawlScheduler"]

