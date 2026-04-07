# Employer registry (Phase 1)

- **File:** `employers.json` — array under `"employers"` (or a bare JSON array).
- **Merge:** `crawl_sites.json` sets `"registry": "registry/employers.json"` (path is **relative to the directory that contains** `crawl_sites.json`, i.e. `config/`). Override with `--registry /abs/or/cwd-relative/path.json`.
- **Dedupe:** Same `api_sources` key (e.g. same Greenhouse `board`) appears only once; first wins (base `api_sources`, then registry order).

## Row fields

| Field | Required | Notes |
|-------|----------|--------|
| `company` | yes | Display / `JobPosting.company` |
| `ats` | yes | `greenhouse`, `lever`, `ashby`, `rss`, `jobright`, `amazon_jobs`, `workday`, `site` |
| `enabled` | no | default `true` |
| `tier` | no | Metadata only (`faang`, `bigtech`, `growth`, `series_a`, `smb`) → `_tier` on source |
| `vertical` / `industry` | no | Optional tag (e.g. `healthcare`, `education`, `trades`) → `_vertical` / `_industry` on source for your filters |
| `id` | no | Optional stable id → `_registry_id` |
| `career_url` | no | Stored as `_career_url` (for your ops / future discovery); `site` can use as single `start_urls` |

**greenhouse:** `board`  
**lever:** `lever_company` (API slug) or `board` as slug  
**ashby:** `board` (Ashby job board slug, see `https://jobs.ashbyhq.com/{slug}`)  
**rss:** `feed_url` or `url`  
**jobright:** `job_list_urls` (array)  
**amazon_jobs:** `locale_prefix` (e.g. `/en`), optional `base_query`, `loc_query`, `result_limit`, `max_jobs`, `sort`, `page_delay_ms`  
**workday:** `workday_host` (e.g. `blackstone.wd1.myworkdayjobs.com`), `cxs_org`, `cxs_site` (from `/wday/cxs/ORG/SITE/jobs` or discover CSV), optional `locale`, `page_limit`, `max_jobs`, `page_delay_ms`, optional `cxs_jobs_url`  
**site:** `domain` + `start_urls` or `career_url` only  

## Bulk CSV import

From `rust/offertrack-crawler`:

```bash
cargo run -p offertrack-crawler --bin offertrack-registry-import -- --help
```

See [docs/global-job-coverage.md](../docs/global-job-coverage.md) for phases 2–4 and legal notes.
