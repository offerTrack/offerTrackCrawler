# Global job coverage — product & engineering roadmap

This document ties together **phases 1–4**, **legal/commercial (Phase 4)**, and **finding company career sites** (FAANG → Series A → SMB). It matches what the Rust crawler implements today and what you still own as data + ops.

---

## What “global” means here

**Not** “every HTML page on the Internet.”  
**Yes** “as many **consenting, technically reachable** public job sources as we can maintain,” with metrics: active employers, jobs/day, parse success rate, legal flags.

---

## Phase 0 — Career URL discovery (implemented, CLI)

**Goal:** From a **seed list** (`company` + `domain`), probe common career paths, follow redirects, and **regex-detect** embedded ATS (Greenhouse, Lever, Ashby, Workday host, iCIMS host, Amazon.jobs mention).

**Shipped:** `offertrack-career-discover` (Rust binary).

```bash
cd rust/offertrack-crawler
cargo run --release --bin offertrack-career-discover -- \
  ../../config/discovery/example-seeds.csv -o ../../out/discovered-careers.csv
```

- **Input:** CSV with header `company,domain`, or a plain file with **one domain per line**.
- **Output:** CSV columns `company`, `domain`, `career_url`, `status`, `detected_ats`, `slug_or_board`, `workday_page_url`, `next_step` — use **`offertrack-discovery-merge`** to append rows into `employers.json` (skips duplicate `id` / ATS keys), or edit by hand.
- **Merge into registry:**

```bash
cargo run --release -p offertrack-crawler --bin offertrack-discovery-merge -- \
  --discovered ../out/discovered-careers.csv \
  --employers ../config/registry/employers.json \
  --output ../config/registry/employers.merged.json
```

Inspect `employers.merged.json`, then replace `employers.json` if satisfied. `--dry-run` prints `+ {...}` lines only.
- **Limits:** Does not guarantee every company; **SPA sites** (many big-tech career portals) often return HTML **without** ATS strings → `detected_ats=unknown_html` but `career_url` may still be the right landing page for humans. **Respect robots / rate** (`--delay-ms`, default 800 ms between domains).

This is the **“find career first”** step upstream of `offertrack-crawl`; it does **not** replace ATS-specific crawlers for Google-class SPAs.

---

## Phase 1 — Employer registry (implemented)

**Goal:** Maintain **thousands** of employers without editing one giant `crawl_sites.json` by hand.

**Shipped:**

- `config/registry/employers.json` — list of employers with `ats`, slugs, optional `tier`, `career_url`.
- `crawl_sites.json` → `"registry": "registry/employers.json"` (path **relative to the directory containing** `crawl_sites.json`).
- Merge + **dedupe** of `api_sources` / `sites` at startup.
- CLI: `--registry /path/to/employers.json` overrides the config value.
- **`offertrack-registry-import`** — CSV → `employers.json` (see `config/registry/README.md`).

**Company website (`career_url`):**  
Stored as `_career_url` on the generated source row for **your internal ops** (dashboards, audits, future discovery). The crawler does not auto-visit it unless you also add a **`site`** row with `start_urls`.

---

## Phase 2 — More ATS connectors (partially implemented)

**Goal:** Same registry format, more `ats` values.

**Shipped:**

- **`ashby`** — `GET https://api.ashbyhq.com/posting-api/job-board/{slug}` (public boards; filter `isListed`).

**Still typical Phase 2 work (not in repo yet):**

- **Google / Microsoft / Meta‑class career portals** — SPA + internal RPC; **not** covered by `offertrack-crawl` alone. Use the **Playwright layer** under `scripts/spa-careers/` (or a **vendor‑documented API** if they publish one): each employer typically needs its own `intercept` URL patterns + `fieldMap`, or a small custom script that still writes the **same JSON rows** as `jobs.json`.
- **Indeed / LinkedIn‑class aggregators** — usually **ToS + commercial APIs** (Phase 4), not raw HTML scraping.
- Workday, SmartRecruiters, iCIMS, BambooHR, etc. — each needs its own fetch + normalisation (often tenant-specific URLs).

**Shipped (non‑ATS giant board):**

- **`amazon_jobs`** — `www.amazon.jobs/.../search.json` (same JSON the site uses; undocumented). Registry fields: `locale_prefix` (default `/en`), `base_query`, `loc_query`, `result_limit`, `max_jobs`, `sort`, `page_delay_ms`. **Union + dedupe** with other sources still runs on the full crawl output (canonical URL).

- **`workday`** — CXS `POST …/wday/cxs/{org}/{site}/jobs` with JSON body (`limit`/`offset`). Registry: `workday_host`, `cxs_org`, `cxs_site`, `locale` (default `en-US`), `page_limit`, `max_jobs`, `page_delay_ms`, optional `cxs_jobs_url` if the API path differs. Stops pagination when `externalPath` repeats (Workday quirk). **JD text** is not in the list response (optional future detail fetch).

---

## SPA / headless supplement (Playwright) + merge (implemented)

**Goal:** For sites where the Rust crawler sees `unknown_html` or empty Schema.org, still emit **`ExportRow[]`** (same fields as `out/jobs.json`: `job_id`, `title`, `company`, `location`, `url`, `posted_date`, `source`, `jd`) and **merge** with the main crawl using the **same** canonical‑URL dedupe and `job_id` assignment.

**Shipped:**

- **`scripts/spa-careers/`** — Node (`crawl.mjs`). Modes:
  - **`http_json`** — **no browser**: `GET` each URL in `fetchUrls`, read JSON, take a root array or `arrayPath` (e.g. Greenhouse `jobs`); map with `fieldMap`. Fast default when the site exposes the same public API its SPA uses (no Playwright install required).
  - **`json_ld`** — **Playwright**: open each `startUrl`, wait for the SPA to settle, collect **`JobPosting`** from embedded `application/ld+json` (including `@graph`). Set root **`chromiumChannel`** to `"chrome"` (or env `PLAYWRIGHT_CHANNEL=chrome`) to use the system Chrome if you did not run `npx playwright install chromium`.
  - **`intercept`** — **Playwright**: while loading `startUrl`, capture **JSON** responses whose URL contains any of `intercept.urlIncludes`, parse either a root array or `arrayPath` (dot path) into objects; map fields with `fieldMap` (dot paths) into ExportRow columns.
- **`offertrack-merge-jobs`** — **`--base`** (e.g. `out/jobs.json`); **`--out PATH`** *or* **`--in-place`** (overwrite `--base`). **`--extra`** optional file; use **`--extra -`** to read **`ExportRow[]`** from **stdin** (pipe from `crawl.mjs`). Missing file path → 0 rows. Optional **`--csv`** / **`--csv-include-jd`**. Summary JSON on stdout.

**Setup (once per machine):**

```bash
cd scripts/spa-careers
npm install
# http_json-only configs need no browser. For json_ld / intercept:
#   npx playwright install chromium   OR   set "chromiumChannel": "chrome" in spa_config.json (system Chrome)
cp spa_config.example.json spa_config.json   # repo includes a ready spa_config.json with sample http_json sources
node crawl.mjs   # JSON to stdout; stderr = summary (no file under out/ unless you set outFile)
```

**End‑to‑end (single `out/jobs.json`):**

```bash
# Option A — from offerTrackCrawler/
./scripts/crawl-with-spa-merge.sh --config config/crawl_sites.json --out out/jobs.json --db state/jobs.db

# Option B — manual (from rust/ after crawl wrote ../out/jobs.json)
node ../scripts/spa-careers/crawl.mjs | cargo run --release -p offertrack-crawler --bin offertrack-merge-jobs -- \
  --base ../out/jobs.json --extra - --in-place
```

Use **`out/jobs.json`** for **`offertrack-push`** and downstream pipelines. No `spa-extra-jobs.json` / `jobs.merged.json` unless you opt in via **`outFile`** or **`--out`**.

**Optional file** `outFile` in `spa_config.json` only if you want a debug copy on disk; default is **stdout only** (nothing extra under `out/`).

**Long‑tail SMB:** keep using **`offertrack-career-discover`** → **`offertrack-discovery-merge`** → registry rows with **`ats: site`** (Schema.org HTML crawl) or future ATS connectors; the Playwright config is for **exceptions** (SPA, non‑standard JSON) where `site` is not enough.

**Aggregators (Indeed, LinkedIn, …):** do **not** treat this Playwright template as permission to scrape those sites; use **compliant APIs and contracts** (Phase 4). The intercept mode is for **employer‑owned** career endpoints you are allowed to call.

**Finding slugs:**

- **Greenhouse:** `boards-api.greenhouse.io/v1/boards/{board}/jobs` — `board` from jobs URL / embed.
- **Lever:** `api.lever.co/v0/postings/{company}` — slug from careers URL.
- **Ashby:** `jobs.ashbyhq.com/{slug}` → same slug for API path above.

---

## Phase 3 — Career-site HTML / Schema.org (implemented baseline)

**Goal:** Employers without a simple public API.

**Shipped:**

- Registry `ats: "site"` → HTML crawl + `GenericSchemaOrgExtractor` (JSON-LD `JobPosting`, BFS, robots, rate limit).

**Scale-out (your process):**

1. Resolve **canonical career URL** (homepage → “Careers” link, or known pattern).
2. Confirm **robots.txt** and acceptable crawl rate.
3. Add `site` row: `domain`, `start_urls`, `enabled: true`.
4. If Schema.org is missing, you need **custom extractors** or a separate parsing service (future code).

---

## Phase 4 — Do you need it? What is “法务”?

**Phase 4** usually means **aggregators and restricted sources** (e.g. Indeed, LinkedIn, niche boards) where:

- **Terms of Service** forbid or limit automated collection / redistribution.
- **Commercial APIs** are the compliant path (paid, contracts, DPA).
- **Regional law** (GDPR, US state privacy, etc.) affects **what you store** (PII in job text, IP logs) and **where** you process data.

**“法务”** here = **legal + compliance**: review ToS, contracts, privacy policy, opt-out / takedown, and **document** which sources are allowed for which use cases (index only vs. display vs. ML).

**Recommendation:** Treat Phase 4 as **explicit**: no silent scraping of aggregators; use **APIs + paperwork** when you need “everything” beyond ATS + career sites.

---

## FAANG → Series A → SMB — how to plug them in

| Segment | Typical stack | Registry `ats` | Notes |
|--------|----------------|----------------|--------|
| Large tech | Greenhouse, Ashby, Lever, Workday | `greenhouse`, `ashby`, `lever` | Add boards/slugs to `employers.json`. |
| Growth / Series A | Often Greenhouse, Ashby, Lever | same | Same mechanics; more manual onboarding. |
| SMB | Website only, mixed ATS | `site` or future connectors | Start with `career_url` + manual `start_urls`; verify robots. |

**“Find the company website”** is **not** solved inside the crawler binary today; it is a **data pipeline**:

- Manual curation, CRM, Clearbit-like enrichment, search APIs, or ML-assisted “careers page” detection — all **upstream** of the registry.

---

## Suggested metrics (per sprint)

- `api_sources_count` / `sites_count` (printed in crawl summary).
- New jobs / run, `skipped` from platform ingest, parse error rate per `ats`.
- % employers with fresh jobs in last 7 days.

---

## Industry / role diversity (default config)

The crawler **does not classify** “nurse vs engineer” in code — it ingests **whatever titles** each source returns. To go beyond software-only listings:

- **`crawl_sites.json`** includes **We Work Remotely** RSS for support, sales/marketing, design, and DevOps (not only programming), plus **Jobright** list pages for US-wide **nursing, healthcare, teaching, construction, electrician, mechanic, truck driver, warehouse, hospitality, retail, manufacturing, accounting**, and a tech example (Python).
- **`employers.json`** adds ATS boards tagged with optional **`vertical`** (`healthcare`, `education`, `fintech`, `logistics_retail`, `mobility`, `realestate`, `media`, …) for your own routing or analytics.

You still expand coverage by **adding more employers and feeds** (hospitals, unions, local boards, government RSS where allowed). There is no single switch for “all professions worldwide.”

## Related files

- `config/registry/README.md` — schema + CSV import.
- `config/registry/employers.json` — sample employers (edit `enabled`, add rows).
- Rust: `src/registry.rs`, `src/sources/ashby.rs`, `offertrack-registry-import` binary.
