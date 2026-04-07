# SPA / headless career capture

Produces **`ExportRow[]`** JSON (same shape as `out/jobs.json`) for employers where the Rust crawler cannot rely on ATS APIs or server-rendered Schema.org alone.

- **Default:** JSON array on **stdout** only (no extra files under `out/`). Set **`outFile`** in `spa_config.json` only for debugging.
- **`http_json`** — `GET` public JSON (e.g. Greenhouse `boards-api…/v1/boards/{slug}/jobs`) with **no Playwright browser**.

### Greenhouse `http_json` (boards API)

The **list** endpoint returns **no full job description** (`content` is only on the **per-job** URL). Some boards (e.g. Stripe) set `absolute_url` to an **SPA search** link (`…/jobs/search?gh_jid=…`) that is a poor bookmark and may not open a single posting in the browser.

Optional fields on the source object:

| Field | Purpose |
|--------|--------|
| `greenhouseBoard` | Board slug (e.g. `stripe`, `airbnb`) — required for the options below. |
| `preferGreenhouseHostedUrl` | If `true`, set `url` to `https://job-boards.greenhouse.io/{board}/jobs/{id}` (stable; often redirects to the employer careers page). |
| List URL `?content=true` | Greenhouse list JSON omits `content` by default. Use **`…/jobs?content=true`** and map **`"jd": "content"`** in `fieldMap` so descriptions load in one batch (HTML is stripped to plain text). |
| `fetchGreenhouseJobDetails` | If `true`, only rows with an **empty `jd`** after the list step get a per-job `GET` (fallback when `content` is missing). Default **`false`** in the sample config when using `?content=true`. |
| `greenhouseDetailConcurrency` | Parallel detail fetches (default `5`). |
| `inferLocationFromTitleWhenPlaceholder` | Default **true**: when `location.name` is a placeholder (e.g. `"LOCATION"`), try the segment after the last **`,`** in the title (e.g. `…, India` → `India`). |

`spa_config.json` uses hosted URLs, **`?content=true`**, **`jd` ← `content`**, and turns **off** bulk detail fetches by default.
- **`json_ld` / `intercept`** — need Playwright: `npm install`, then `npx playwright install chromium` or **`"chromiumChannel": "chrome"`** in config.
- Repo **`spa_config.json`** — sample `http_json` sources; edit `sources` as needed.
- Run: `npm run crawl` or `node crawl.mjs` (optional `--stdout` forces stdout even if `outFile` is set).

Merge into the main crawl **in place** (overwrite **`out/jobs.json`**):

```bash
# from rust/ (after offertrack-crawl)
node ../scripts/spa-careers/crawl.mjs | ./target/release/offertrack-merge-jobs \
  --base ../out/jobs.json --extra - --in-place
```

Or use **`../scripts/crawl-with-spa-merge.sh`** from **`offerTrackCrawler/`** (crawl + pipe merge in one step).

Full notes: [../../docs/global-job-coverage.md](../../docs/global-job-coverage.md).
