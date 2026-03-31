import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
from urllib.parse import parse_qs, urlencode, urlparse

from .models import JobPosting

# Stable namespace for deterministic job_id per (source, listing URL).
_JOB_LISTING_NAMESPACE = uuid.UUID("018d6b2e-8000-7000-8000-000000000001")


def _to_iso(v):
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def _canonical_url(url: str) -> str:
    """Normalize listing URL: lowercase host/path, drop fragments, strip utm_* only.

    Keeps id query params (e.g. gh_jid on Greenhouse boards that share one path).
    """
    u = (url or "").strip()
    if not u:
        return ""
    p = urlparse(u)
    netloc = (p.netloc or "").lower()
    path = (p.path or "").rstrip("/") or "/"
    base = f"{(p.scheme or 'https').lower()}://{netloc}{path}"
    if not p.query:
        return base
    qs = parse_qs(p.query, keep_blank_values=False)
    filtered = []
    for key in sorted(qs.keys()):
        lk = key.lower()
        if lk == "utm_source" or lk.startswith("utm_"):
            continue
        val = qs[key][0] if qs[key] else ""
        filtered.append((key, val))
    if not filtered:
        return base
    return base + "?" + urlencode(filtered)


def listing_signature(job: JobPosting) -> str:
    """One row per listing on a given source (not cross-platform entity match)."""
    key = f"{(job.source or '').strip()}\0{_canonical_url(job.url)}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def stable_job_id(job: JobPosting) -> str:
    """Deterministic id: same source + canonical URL => same job_id across runs."""
    return str(uuid.uuid5(_JOB_LISTING_NAMESPACE, listing_signature(job)))


def assign_stable_listing_ids(jobs: List[JobPosting]) -> None:
    for j in jobs:
        j.job_id = stable_job_id(j)


class JobStorage:
    """SQLite storage with dedupe for crawled jobs."""

    def __init__(self, db_path: str = "state/jobs.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signature TEXT NOT NULL UNIQUE,
                job_id TEXT,
                canonical_job_id TEXT,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                location TEXT,
                description TEXT,
                url TEXT NOT NULL,
                posted_date TEXT,
                source TEXT,
                raw_json TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );
            """
        )
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(jobs)").fetchall()}
        if "job_id" not in cols:
            self.conn.execute("ALTER TABLE jobs ADD COLUMN job_id TEXT")
        if "canonical_job_id" not in cols:
            self.conn.execute("ALTER TABLE jobs ADD COLUMN canonical_job_id TEXT")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_posted_date ON jobs(posted_date);"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_last_seen_at ON jobs(last_seen_at);"
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_job_id ON jobs(job_id);")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_canonical ON jobs(canonical_job_id);"
        )
        self.conn.commit()

    def upsert_jobs(self, jobs: List[JobPosting]) -> Dict[str, int]:
        now = datetime.utcnow().isoformat()
        inserted = 0
        updated = 0
        with self.conn:
            for job in jobs:
                sig = listing_signature(job)
                jid = stable_job_id(job)
                existing = self.conn.execute(
                    "SELECT id FROM jobs WHERE signature = ?",
                    (sig,),
                ).fetchone()
                payload = (
                    jid,
                    job.title,
                    job.company,
                    job.location,
                    job.description,
                    job.url,
                    _to_iso(job.posted_date),
                    job.source,
                    json.dumps(job.raw or {}, ensure_ascii=False),
                    now,
                )
                if existing:
                    self.conn.execute(
                        """
                        UPDATE jobs
                        SET job_id=?, title=?, company=?, location=?, description=?,
                            url=?, posted_date=?, source=?, raw_json=?, last_seen_at=?
                        WHERE signature=?
                        """,
                        payload + (sig,),
                    )
                    updated += 1
                else:
                    self.conn.execute(
                        """
                        INSERT INTO jobs (
                            signature, job_id, title, company, location, description, url,
                            posted_date, source, raw_json, first_seen_at, last_seen_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (sig,) + payload + (now,),
                    )
                    inserted += 1
        return {"inserted": inserted, "updated": updated}

    def recent_jobs(self, days: int = 3) -> List[Dict]:
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        rows = self.conn.execute(
            """
            SELECT job_id, title, company, location, description, url, posted_date, source, first_seen_at, last_seen_at
            FROM jobs
            WHERE posted_date IS NULL OR posted_date >= ?
            ORDER BY COALESCE(posted_date, first_seen_at) DESC
            """,
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.conn.close()


__all__ = [
    "JobStorage",
    "assign_stable_listing_ids",
    "listing_signature",
    "stable_job_id",
]

