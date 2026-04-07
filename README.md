# offerTrackML

**Open-source** job crawling and local persistence: exports normalized JSON and SQLite for downstream use. Products such as offerTrack can consume these artifacts in a **private repository** for embeddings, RAG, batch jobs, and matching logic.

## Same change → three places (do not edit in isolation)

Integration with **offerTrackPlatform** + **offerTrackModelTraining** is documented as one pipeline. If you change export shape, ingest API, or `Job.id` rules, update **crawler docs + Rust CLIs**, **platform admin routes + OpenAPI**, and **ModelTraining docs/scripts** together. **Hub:** [../README.md](../README.md) (monorepo checklist).

**Stack:** **Rust** (`rust/`): **`offertrack-crawl`** (Greenhouse, Lever, **Ashby**, RSS/Atom, Jobright, **amazon_jobs**, **workday** + HTML `sites`), **`offertrack-merge-jobs`** (merge two `jobs.json`-shaped feeds with canonical URL dedupe), **`offertrack-career-discover`**, **`offertrack-discovery-merge`**, **`offertrack-push`**, **`offertrack-registry-import`**. **Node + Playwright** (`scripts/spa-careers/`) for **SPA / XHR** employers that need a headless layer; output merges with the Rust crawl (see [docs/global-job-coverage.md](docs/global-job-coverage.md)). Employer scale-out: **`config/registry/employers.json`** (see [config/registry/README.md](config/registry/README.md)). **Change-one, check-three:** [../README.md](../README.md).

**Closed-source** pieces (document parsing/chunking, embeddings, vector stores, map/batch jobs, full RAG, and model training) live in a **separate private repo**; they are not part of this codebase.

## Features

- **Rust crawler** (`rust/offertrack-crawler/`): `api_sources` (Greenhouse, Lever, Ashby, RSS, Jobright, **`amazon_jobs`**) + optional HTML `sites` (BFS, rate limit, optional `robots.txt`); **`out/jobs.json`** + **`state/jobs.db`**. Optional **`--csv`** writes **`out/jobs.csv`**; **`--csv-include-jd`** adds a `jd` column. **`offertrack-merge-jobs`** can **`--in-place`** merge an extra feed into **`jobs.json`** (pipe **`scripts/spa-careers/crawl.mjs`** with **`--extra -`**). See [docs/global-job-coverage.md](docs/global-job-coverage.md).
- **Rust push** (`offertrack-push`): upload full `jobs.json` to the platform admin ingest API.
- **Default sources** aim for **multiple industries** (not dev-only): WWR RSS for support / sales / design / DevOps; Jobright lists for nursing, trades, warehouse, hospitality, retail, etc.; registry employers span **healthcare, education, fintech, logistics, mobility, media** (see [docs/global-job-coverage.md](docs/global-job-coverage.md)).
- **Merge + dedupe**: All feeds are **unioned**, then collapsed by **canonical apply URL**; `source` becomes a merged label (e.g. `greenhouse:stripe | jobright.ai`). **Storage**: SQLite row key + **`job_id`** use **canonical URL only** (UUID v5 over SHA-256 of canonical URL).

## Requirements

- **Rust:** current **stable** toolchain (`rust/rust-toolchain.toml` pins `stable`; run `rustup update stable` if Cargo errors on newer crate manifests).
- Network access for crawling public sources.

## Career discovery (before expanding the registry)

```bash
cd rust
cargo run --release -p offertrack-crawler --bin offertrack-career-discover -- \
  ../config/discovery/example-seeds.csv -o ../out/discovered-careers.csv
```

Edit `config/discovery/example-seeds.csv` (or use a domain-per-line file). For rows with `detected_ats` in {greenhouse, lever, ashby, workday, amazon_jobs}, run **`offertrack-discovery-merge`** to append into a copy of `employers.json` (see [docs/global-job-coverage.md](docs/global-job-coverage.md)). For **`unknown_html`** / SPA career sites, use **`scripts/spa-careers/`** (Playwright) and **`offertrack-merge-jobs`** to combine with `out/jobs.json` (documented in the same guide).

## Build — Rust

```bash
cd rust
cargo build --release
# crawl (paths relative to offerTrackCrawler repo root):
cargo run --release -p offertrack-crawler --bin offertrack-crawl -- \
  --config ../config/crawl_sites.json --out ../out/jobs.json --db ../state/jobs.db
```

**Minimal export**:

```bash
cargo run --release -p offertrack-crawler --bin offertrack-crawl -- \
  --config ../config/crawl_sites.json --out ../out/jobs_min.json --db ../state/jobs.db --minimal-export
```

## Layout

| Path | Purpose |
|------|---------|
| `rust/` | **Rust** workspace: `offertrack-crawl`, `offertrack-merge-jobs`, `offertrack-push`, `offertrack-registry-import` |
| `scripts/spa-careers/` | SPA / http_json / Playwright → **stdout** JSON; pipe to **`offertrack-merge-jobs --extra - --in-place`** |
| `scripts/crawl-with-spa-merge.sh` | Crawl then merge SPA feed into **`out/jobs.json`** (single artifact) |
| `rust/offertrack-crawler/src/html/` | HTML `sites`: Schema.org JSON-LD + link discovery |
| `config/crawl_sites.json` | Global options + `api_sources` / `sites` + optional `registry` path |
| `config/registry/employers.json` | Employer list (FAANG → Series A → SMB templates); merged into crawl |
| `docs/` | [offertrack-platform.md](docs/offertrack-platform.md), [global-job-coverage.md](docs/global-job-coverage.md) |

Generated files are **not** committed by default (see `.gitignore`): `out/`, `state/`, `rust/target/`, etc.

## Usage: crawl jobs

1. Edit `config/crawl_sites.json`: `api_sources` and optional HTML under `sites` (`extractor`: `GenericSchemaOrgExtractor`).
2. Run (from `rust/` as above, or with paths relative to repo root).

Run summary JSON includes `runner`, `total_after_freshness_before_dedup`, `dedup_removed_duplicate_urls`, `total_merged_unique_listings`, `total_crawled_after_fresh_filter` (same as merged count), `csv_path`, `api_sources_count`, `sites_count`, `registry_loaded`, `registry_path`, `html_sites_jobs_before_fresh_filter`, `db_inserted_new`, `db_updated_existing`, etc.

### Employer registry (many companies)

- Edit **`config/registry/employers.json`** or generate from CSV:

```bash
cd rust
cargo run -p offertrack-crawler --bin offertrack-registry-import -- \
  ../config/registry/employers_template.csv \
  -o ../config/registry/employers.generated.json
```

- `crawl_sites.json` references the registry with `"registry": "registry/employers.json"` (relative to the **`config/`** folder that contains `crawl_sites.json`).
- Override path: `offertrack-crawl --registry /path/to/employers.json`.

### Push jobs to offerTrackPlatform

With the **offerTrack** backend running (local or staging), POST the full `out/jobs.json` to `POST /api/v1/admin/ingest/crawler-jobs`:

```bash
export OFFERTRACK_API_URL=http://localhost:3000
# export OFFERTRACK_ADMIN_KEY=...   # when platform ADMIN_API_KEY is set
cd rust
cargo run --release -p offertrack-crawler --bin offertrack-push -- ../out/jobs.json
# dry-run: add --dry-run
```

Details: [docs/offertrack-platform.md](docs/offertrack-platform.md).

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
cd rust/offertrack-crawler
cargo test
```

## What this open repo includes

- Rust crate (`rust/offertrack-crawler/`): `offertrack-crawl`, `offertrack-push`, sample `config/crawl_sites.json`, and the crawl/export behavior described here.
- `docs/`, `.env.example`, `LICENSE`, `NOTICE`, and this README.

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
