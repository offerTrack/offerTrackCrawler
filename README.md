# offerTrackML

**Open-source** job crawling and local persistence: exports normalized JSON and SQLite for downstream use. Products such as offerTrack can consume these artifacts in a **private repository** for embeddings, RAG, batch jobs, and matching logic.

**Closed-source** pieces (document parsing/chunking, embeddings, vector stores, map/batch jobs, full RAG, and model training) live in a **separate private repo**; they are not part of this codebase.

**The crawler still runs as before.** Removing embedding/RAG code from `src/` does not affect `crawl_jobs.py` or `src/crawler/`. Install `requirements.txt`, edit `config/crawl_sites.json`, then run the commands below from the repository root. No ML stack is required to crawl.

## Features

- **Crawler** (`src/crawler/`, `crawl_jobs.py`): Greenhouse, Lever, RSS/Atom, Jobright list pages, optional HTML + Schema.org extraction.
- **Storage**: SQLite deduplicated upsert by **source + canonical URL**; JSON export.

## Requirements

- Python **3.10+** (3.11 recommended)
- Network access for crawling public sources

## Install

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Development and tests:

```bash
pip install -r requirements-dev.txt
```

See `requirements.txt` and `requirements-dev.txt` for dependencies.

## Layout

| Path | Purpose |
|------|---------|
| `crawl_jobs.py` | CLI: load config, crawl, write `out/` and SQLite |
| `config/crawl_sites.json` | Source definitions and crawl settings |
| `src/crawler/` | Fetch, extract, schedule, persist |
| `docs/` | Draft specs (e.g. crawler, interfaces) |

Generated files are **not** committed by default (see `.gitignore`): `out/`, `state/`, etc. The repo keeps `out/.gitkeep` and `state/.gitkeep` so directories exist after clone.

## Usage: crawl jobs

Run these steps **from the repository root** (where `crawl_jobs.py` lives).

1. Edit `config/crawl_sites.json`: set `type` under `api_sources` (`greenhouse` / `lever` / `rss` / `atom` / `jobright`), toggle `enabled`; optional HTML crawling under `sites`.
2. Run:

```bash
python crawl_jobs.py --config config/crawl_sites.json --out out/jobs.json --db state/jobs.db
```

3. **Minimal export** for downstream models (writes DB first, then reads `first_seen_at` from SQLite):

```bash
python crawl_jobs.py --minimal-export --out out/jobs_min.json --db state/jobs.db
```

4. The CLI prints JSON stats: `total_crawled_after_fresh_filter`, `db_inserted_new`, `db_updated_existing`, `db_recent_jobs`, etc.

### Jobright

Reads Next.js data from `https://jobright.ai/job-list/...` pages. Does **not** call `/api/*` paths disallowed by their `robots.txt`. Optional fetches to `/jobs/info/{id}` can enrich company name (see `fetch_company_detail` and related config).

### Export fields (default `out/jobs.json`)

Each row typically includes: `job_id` (stable deterministic id), `title`, `company`, `location`, `url`, `posted_date` (from the source), `source`, `jd` (HTML body; section headings vary by employer template).

### Deduplication and `job_id`

- **Unique key in DB**: `source` + **canonical URL** (no fragment; strip `utm_*`; keep id params such as Greenhouse `gh_jid`).
- **`job_id`**: deterministic UUID derived from that listing key; stable across runs for the same posting.
- **`canonical_job_id`** column (nullable) is reserved for future cross-platform “same job” clustering; not populated by the crawler.

## Tests

```bash
pytest tests/ -q
```

## What this open repo includes

- `crawl_jobs.py`, `src/crawler/`, sample `config/crawl_sites.json`, and the crawl/export behavior described here.
- `tests/` for the above, `docs/`, `requirements*.txt`, `.env.example`, `LICENSE`, `NOTICE`, and this README.

**Typically gitignored**: local `out/`, `state/`, `.env`, keys, and generated databases.

## What stays private (other repo)

| Area | Examples |
|------|----------|
| Embeddings & RAG | Parsing/chunking, embedding services, vector storage, retrieval + generation orchestration |
| Batch | Map/batch jobs, joins with user or internal data, production scheduler config |
| Training & eval | Training scripts, datasets/labels, benchmarks tied to the product |
| Product logic | Ranking/matching formulas, strategy parameters, user-bound pipelines |
| Secrets & prod config | API keys, cloud credentials, internal endpoints |
| Sensitive data | Raw user trails, non-public resumes |

Document integration only (e.g. consume `out/jobs.json` or `state/jobs.db`); no need to expose the private repo.

### Do not commit

| Avoid in this public repo | Examples |
|---------------------------|----------|
| Secrets | API keys, cloud creds, `*.pem`, `credentials.json` (use local `.env`, never commit it) |

## Documentation

- See `docs/` (e.g. `web_crawler.md`, `interface.md`).

## Contributing & security

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [SECURITY.md](SECURITY.md)

## License

**Apache License 2.0** — see [LICENSE](LICENSE). Attribution: [NOTICE](NOTICE). When you redistribute, keep the `NOTICE` contents; merge or retain third-party `NOTICE` entries as required by those dependencies.
