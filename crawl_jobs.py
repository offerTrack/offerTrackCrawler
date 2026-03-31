import argparse
import asyncio
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

import aiohttp

from src.crawler.web_crawler import WebCrawler, CrawlConfig
from src.crawler.extractors.generic_schemaorg import GenericSchemaOrgExtractor
from src.crawler.storage import JobStorage, assign_stable_listing_ids
from src.crawler.models import JobPosting


def _build_extractor(name: str, domain: str):
    if name == "GenericSchemaOrgExtractor":
        return GenericSchemaOrgExtractor(domain=domain)
    raise SystemExit(f"Unknown extractor: {name}")


def _parse_date(value: Any):
    if not value:
        return None
    s = str(value).strip()
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%a, %d %b %Y %H:%M:%S %z",
    ):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=None)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _is_fresh(job: JobPosting, freshness_days: int) -> bool:
    if not job.posted_date:
        return True
    return job.posted_date >= datetime.utcnow() - timedelta(days=freshness_days)


async def _fetch_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json()


async def _fetch_text(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.text()


def _next_data_from_html(html: str) -> Optional[dict]:
    marker = '<script id="__NEXT_DATA__" type="application/json">'
    start = html.find(marker)
    if start == -1:
        return None
    start += len(marker)
    end = html.find("</script>", start)
    if end == -1:
        return None
    return json.loads(html[start:end])


def _company_guess_from_summary(summary: Optional[str]) -> Optional[str]:
    if not summary:
        return None
    m = re.match(r"^([A-Z0-9][A-Za-z0-9.&'\-\s]{1,72}?)\s+is\s+", summary.strip())
    if m:
        return m.group(1).strip()
    return None


def _company_from_jobright_detail(payload: dict) -> Optional[str]:
    pp = payload.get("props", {}).get("pageProps", {})
    ds = pp.get("dataSource") or {}
    cr = ds.get("companyResult") or {}
    return cr.get("companyName") or cr.get("name")


async def _jobright_fetch_company(
    session: aiohttp.ClientSession,
    job_page_url: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    async with sem:
        try:
            html = await _fetch_text(session, job_page_url.split("?", 1)[0])
            payload = _next_data_from_html(html)
            if not payload:
                return None
            return _company_from_jobright_detail(payload)
        except Exception:
            return None


async def _crawl_jobright(session: aiohttp.ClientSession, source: Dict[str, Any]) -> List[JobPosting]:
    """
    Jobright.ai: parse public /job-list/* HTML (Next.js __NEXT_DATA__).
    Does not call /api/* (disallowed in robots.txt). Optional per-job detail
    fetch from /jobs/info/{id} to read companyName.
    """
    raw_urls = list(source.get("job_list_urls") or [])
    if source.get("url"):
        raw_urls.append(source["url"])
    seen = set()
    list_urls = []
    for u in raw_urls:
        u = (u or "").strip()
        if not u or "jobright.ai" not in u or u in seen:
            continue
        seen.add(u)
        list_urls.append(u)
    if not list_urls:
        return []

    fetch_detail = bool(source.get("fetch_company_detail", True))
    max_details = int(source.get("max_detail_requests", 20))
    concurrency = int(source.get("detail_concurrency", 4))
    sem = asyncio.Semaphore(max(1, concurrency))

    jobs: List[JobPosting] = []
    for list_url in list_urls:
        try:
            html = await _fetch_text(session, list_url)
            payload = _next_data_from_html(html)
            if not payload:
                continue
            job_list = (
                payload.get("props", {})
                .get("pageProps", {})
                .get("jobList", [])
            )
            for item in job_list:
                jr = (item.get("jobResult") or {})
                title = (jr.get("jobTitle") or "").strip()
                detail_url = (jr.get("url") or jr.get("applyLink") or "").strip()
                if not title or not detail_url:
                    continue
                summary = jr.get("jobSummary")
                company = _company_guess_from_summary(summary) or "Jobright (company via detail or summary)"
                loc = jr.get("jobLocation")
                posted = _parse_date(jr.get("publishTime"))
                jobs.append(
                    JobPosting(
                        title=title,
                        company=company,
                        location=loc,
                        description=summary,
                        url=detail_url,
                        posted_date=posted,
                        source="jobright.ai",
                        raw={"list_url": list_url, "jobResult": jr},
                    )
                )
        except Exception as e:
            print(f"[WARN] jobright list failed ({list_url}): {e}")

    if fetch_detail and jobs:
        to_enrich = jobs[: max_details]
        tasks = [_jobright_fetch_company(session, j.url, sem) for j in to_enrich]
        companies = await asyncio.gather(*tasks)
        for j, co in zip(to_enrich, companies):
            if co:
                j.company = co
                j.raw = dict(j.raw or {})
                j.raw["companyResolvedFrom"] = "detail"

    return jobs


async def _crawl_greenhouse(session: aiohttp.ClientSession, source: Dict[str, Any]) -> List[JobPosting]:
    board = source["board"]
    api_url = source.get("api_url") or f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
    data = await _fetch_json(session, api_url)
    jobs: List[JobPosting] = []
    for item in data.get("jobs", []):
        jobs.append(
            JobPosting(
                title=item.get("title", ""),
                company=source.get("company", board),
                location=((item.get("location") or {}).get("name") if isinstance(item.get("location"), dict) else None),
                description=(item.get("content") or ""),
                url=item.get("absolute_url") or "",
                posted_date=_parse_date(item.get("updated_at") or item.get("created_at")),
                source=f"greenhouse:{board}",
                raw=item,
            )
        )
    return [j for j in jobs if j.title and j.url]


async def _crawl_lever(session: aiohttp.ClientSession, source: Dict[str, Any]) -> List[JobPosting]:
    company = source["company"]
    api_url = source.get("api_url") or f"https://api.lever.co/v0/postings/{company}?mode=json"
    data = await _fetch_json(session, api_url)
    jobs: List[JobPosting] = []
    for item in data:
        jobs.append(
            JobPosting(
                title=item.get("text", ""),
                company=source.get("display_name", company),
                location=((item.get("categories") or {}).get("location") if isinstance(item.get("categories"), dict) else None),
                description=(item.get("descriptionPlain") or item.get("description") or ""),
                url=item.get("hostedUrl") or "",
                posted_date=_parse_date(item.get("createdAt")),
                source=f"lever:{company}",
                raw=item,
            )
        )
    return [j for j in jobs if j.title and j.url]


async def _crawl_rss(session: aiohttp.ClientSession, source: Dict[str, Any]) -> List[JobPosting]:
    feed_url = source["url"]
    xml_text = await _fetch_text(session, feed_url)
    root = ET.fromstring(xml_text)
    jobs: List[JobPosting] = []

    # RSS items
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = item.findtext("pubDate") or item.findtext("published")
        desc = item.findtext("description")
        jobs.append(
            JobPosting(
                title=title or "Untitled job",
                company=source.get("company", urlparse(feed_url).netloc),
                location=source.get("default_location"),
                description=desc,
                url=link or feed_url,
                posted_date=_parse_date(pub),
                source=f"rss:{feed_url}",
                raw={"pubDate": pub},
            )
        )

    # Atom entries
    for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        title = (entry.findtext("{http://www.w3.org/2005/Atom}title") or "").strip()
        published = entry.findtext("{http://www.w3.org/2005/Atom}published")
        updated = entry.findtext("{http://www.w3.org/2005/Atom}updated")
        link_el = entry.find("{http://www.w3.org/2005/Atom}link")
        link = link_el.get("href") if link_el is not None else feed_url
        jobs.append(
            JobPosting(
                title=title or "Untitled job",
                company=source.get("company", urlparse(feed_url).netloc),
                location=source.get("default_location"),
                description=None,
                url=link or feed_url,
                posted_date=_parse_date(published or updated),
                source=f"atom:{feed_url}",
                raw={"published": published, "updated": updated},
            )
        )

    return [j for j in jobs if j.title and j.url]


async def _crawl_html_sites(cfg_json: Dict[str, Any], crawl_cfg: CrawlConfig) -> List[JobPosting]:
    sites = []
    extractors = {}
    for site in cfg_json.get("sites", []):
        if not site.get("enabled", True):
            continue
        domain = site["domain"]
        start_urls = site.get("start_urls", [])
        extractor_name = site.get("extractor", "GenericSchemaOrgExtractor")
        extractors[domain] = _build_extractor(extractor_name, domain=domain)
        sites.append({"domain": domain, "start_urls": start_urls})

    if not sites:
        return []

    async with WebCrawler(crawl_cfg) as crawler:
        for domain, extractor in extractors.items():
            crawler.register_extractor(domain, extractor)
        return await crawler.crawl_sites(sites)


async def _crawl_api_sources(cfg_json: Dict[str, Any], user_agent: str) -> List[JobPosting]:
    api_sources = cfg_json.get("api_sources", [])
    if not api_sources:
        return []

    timeout = aiohttp.ClientTimeout(total=120)
    headers = {"User-Agent": user_agent}
    out: List[JobPosting] = []
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        for source in api_sources:
            if not source.get("enabled", True):
                continue
            source_type = source.get("type")
            try:
                if source_type == "greenhouse":
                    out.extend(await _crawl_greenhouse(session, source))
                elif source_type == "lever":
                    out.extend(await _crawl_lever(session, source))
                elif source_type in ("rss", "atom"):
                    out.extend(await _crawl_rss(session, source))
                elif source_type == "jobright":
                    out.extend(await _crawl_jobright(session, source))
            except Exception as exc:
                print(f"[WARN] source failed ({source_type}): {exc}")
    return out


async def _run(
    config_path: Path,
    output_path: Path,
    db_path: Path,
    *,
    minimal_export: bool = False,
):
    cfg_json = json.loads(config_path.read_text(encoding="utf-8"))
    crawl_cfg = CrawlConfig(
        delay_between_requests=float(cfg_json.get("delay_between_requests", 1.0)),
        respect_robots_txt=bool(cfg_json.get("respect_robots_txt", True)),
        freshness_days=int(cfg_json.get("freshness_days", 3)),
        max_pages_per_site=int(cfg_json.get("max_pages_per_site", 100)),
    )
    freshness_days = crawl_cfg.freshness_days

    html_jobs, api_jobs = await asyncio.gather(
        _crawl_html_sites(cfg_json, crawl_cfg),
        _crawl_api_sources(cfg_json, crawl_cfg.user_agent),
    )
    all_jobs = [j for j in (html_jobs + api_jobs) if _is_fresh(j, freshness_days)]
    assign_stable_listing_ids(all_jobs)

    storage = JobStorage(str(db_path))
    try:
        stats = storage.upsert_jobs(all_jobs)
        recent = storage.recent_jobs(days=freshness_days)

        if minimal_export:
            rows_by_id: Dict[str, Any] = {}
            if all_jobs:
                ids = [j.job_id for j in all_jobs]
                placeholders = ",".join("?" * len(ids))
                cur = storage.conn.execute(
                    f"SELECT job_id, first_seen_at, description FROM jobs WHERE job_id IN ({placeholders})",
                    ids,
                )
                rows_by_id = {str(r["job_id"]): r for r in cur.fetchall()}
            output = []
            for j in all_jobs:
                row = rows_by_id.get(j.job_id)
                if not row:
                    continue
                output.append(
                    {
                        "job_id": j.job_id,
                        "jd": (row["description"] or j.description or ""),
                        "first_seen_at": row["first_seen_at"],
                    }
                )
        else:
            output = [
                {
                    "job_id": j.job_id,
                    "title": j.title,
                    "company": j.company,
                    "location": j.location,
                    "url": j.url,
                    "posted_date": j.posted_date.isoformat() if j.posted_date else None,
                    "source": j.source,
                    "jd": j.description or "",
                }
                for j in all_jobs
            ]
    finally:
        storage.close()

    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "total_crawled_after_fresh_filter": len(all_jobs),
                "db_inserted_new": stats["inserted"],
                "db_updated_existing": stats["updated"],
                "db_recent_jobs": len(recent),
                "output_path": str(output_path),
                "db_path": str(db_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def main():
    p = argparse.ArgumentParser(description="Crawl job sites for postings within N days.")
    p.add_argument("--config", default="config/crawl_sites.json")
    p.add_argument("--out", default="out/jobs.json")
    p.add_argument("--db", default="state/jobs.db")
    p.add_argument(
        "--minimal-export",
        action="store_true",
        help="Write job_id, jd, first_seen_at (from DB after upsert; listing dedupe by source+URL).",
    )
    args = p.parse_args()

    config_path = Path(args.config)
    out_path = Path(args.out)
    db_path = Path(args.db)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    asyncio.run(
        _run(config_path, out_path, db_path, minimal_export=args.minimal_export)
    )


if __name__ == "__main__":
    main()

