#!/usr/bin/env node
/**
 * Extra job feeds: http_json (no browser) + Playwright json_ld / intercept.
 * Writes the same JSON array shape as offertrack-crawl jobs.json (ExportRow).
 * Merge: node crawl.mjs | offertrack-merge-jobs --base out/jobs.json --extra - --in-place
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function usage() {
  console.error(
    "Usage: node crawl.mjs [--config path/to/spa_config.json] [--stdout]\n" +
      "Default: JSON array to stdout only (no file in out/). Set config \"outFile\" to write a path.\n" +
      "Default config: ./spa_config.json (copy from spa_config.example.json)"
  );
}

function getDot(obj, dotPath) {
  if (obj == null || !dotPath) return undefined;
  let cur = obj;
  for (const part of dotPath.split(".")) {
    if (cur == null || typeof cur !== "object") return undefined;
    cur = cur[part];
  }
  return cur;
}

function toRowsFromFieldMap(row, fieldMap, defaults) {
  const out = {};
  const keys = ["title", "company", "location", "url", "posted_date", "jd", "source"];
  for (const k of keys) {
    const src = fieldMap[k];
    if (src === undefined || src === null) continue;
    const v = getDot(row, src);
    if (v !== undefined && v !== null && String(v).length > 0) {
      out[k] = typeof v === "string" ? v : String(v);
    }
  }
  return { ...defaults, ...out };
}

/** Greenhouse sometimes returns placeholder text instead of a real office name. */
function isPlaceholderLocation(loc) {
  const t = String(loc ?? "").trim();
  if (!t) return true;
  const u = t.toUpperCase();
  return (
    u === "LOCATION" ||
    u === "SEE OPENING ID" ||
    u === "N/A" ||
    u === "TBD"
  );
}

function decodeHtmlEntitiesOnce(text) {
  if (text == null || text === "") return "";
  let s = String(text);
  const named = {
    amp: "&",
    lt: "<",
    gt: ">",
    quot: '"',
    apos: "'",
    nbsp: " ",
  };
  s = s.replace(/&(#x?[0-9a-fA-F]+|\w+);/g, (m, code) => {
    const lc = String(code).toLowerCase();
    if (named[lc] !== undefined) return named[lc];
    if (code[0] === "#") {
      const n =
        code[1] === "x" || code[1] === "X"
          ? parseInt(code.slice(2), 16)
          : parseInt(code.slice(1), 10);
      if (Number.isFinite(n) && n >= 0 && n <= 0x10ffff)
        return String.fromCodePoint(n);
    }
    return m;
  });
  return s;
}

function decodeHtmlEntitiesDeep(text) {
  let s = String(text ?? "");
  for (let i = 0; i < 6; i++) {
    const next = decodeHtmlEntitiesOnce(s);
    if (next === s) break;
    s = next;
  }
  return s;
}

/** Strip tags for RAG-friendly `jd` (Greenhouse `content` is HTML, often entity-encoded). */
function greenhouseContentToPlainText(html) {
  const decoded = decodeHtmlEntitiesDeep(html);
  return decoded
    .replace(/\s*<br\s*\/?>\s*/gi, "\n")
    .replace(/<\/(p|div|li|h[1-6]|tr)\s*>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function greenhouseHostedJobUrl(board, jobId) {
  const b = String(board).trim();
  const id = String(jobId).trim();
  if (!b || !id) return "";
  return `https://job-boards.greenhouse.io/${b}/jobs/${id}`;
}

/** When Greenhouse sends location.name === "LOCATION", title often ends with ", India" / ", UK". */
function inferLocationTailFromTitle(title) {
  const t = String(title ?? "").trim();
  const idx = t.lastIndexOf(",");
  if (idx <= 0 || idx >= t.length - 1) return undefined;
  const tail = t.slice(idx + 1).trim();
  if (tail.length < 2 || tail.length > 120) return undefined;
  if (/^see\s+opening/i.test(tail)) return undefined;
  return tail;
}

async function fetchGreenhouseJobJson(board, jobId, headers) {
  const url = `https://boards-api.greenhouse.io/v1/boards/${encodeURIComponent(
    board
  )}/jobs/${encodeURIComponent(jobId)}`;
  const res = await fetch(url, { headers });
  if (!res.ok) return null;
  try {
    return await res.json();
  } catch {
    return null;
  }
}

async function enrichGreenhouseRowsFromDetails(rowsMeta, board, headers, concurrency) {
  const n = Math.max(1, Number(concurrency ?? 4));
  for (let i = 0; i < rowsMeta.length; i += n) {
    const chunk = rowsMeta.slice(i, i + n);
    await Promise.all(
      chunk.map(async ({ row, jobId }) => {
        const detail = await fetchGreenhouseJobJson(board, jobId, headers);
        if (!detail) return;
        if (detail.content) {
          const plain = greenhouseContentToPlainText(detail.content);
          if (plain.length > 0) row.jd = plain;
        }
        const locName = detail.location && detail.location.name;
        if (locName && !isPlaceholderLocation(locName)) {
          if (!row.location || isPlaceholderLocation(row.location)) {
            row.location = String(locName).trim();
          }
        }
      })
    );
  }
}

function jobLocationToString(loc) {
  if (loc == null) return undefined;
  if (typeof loc === "string") return loc.trim() || undefined;
  if (typeof loc !== "object") return undefined;
  const t = loc["@type"];
  const isPlace =
    t === "Place" || (Array.isArray(t) && t.includes("Place"));
  if (!isPlace && !loc.address) return undefined;
  const a = loc.address;
  if (typeof a === "string") return a.trim() || undefined;
  if (a && typeof a === "object") {
    const parts = [
      a.addressLocality,
      a.addressRegion,
      a.postalCode,
      a.addressCountry,
    ].filter(Boolean);
    if (parts.length) return parts.join(", ");
  }
  return loc.name && String(loc.name);
}

function hiringOrgName(org) {
  if (!org) return undefined;
  if (typeof org === "string") return org;
  if (typeof org === "object" && org.name) return String(org.name);
  return undefined;
}

function absoluteUrl(maybe, base) {
  if (!maybe) return "";
  const s = String(maybe).trim();
  if (!s) return "";
  try {
    return new URL(s, base).href;
  } catch {
    return s;
  }
}

function normalizeExportRow(r, pageUrl, defaultCompany, sourceTag) {
  const title = (r.title && String(r.title).trim()) || "";
  const url = absoluteUrl(r.url, pageUrl);
  const company =
    (r.company && String(r.company).trim()) || defaultCompany || "";
  const jd = r.jd != null ? String(r.jd) : "";
  const location =
    r.location != null && String(r.location).trim()
      ? String(r.location).trim()
      : undefined;
  const posted_date =
    r.posted_date != null && String(r.posted_date).trim()
      ? String(r.posted_date).trim()
      : undefined;
  const source = r.source != null && String(r.source).trim()
    ? String(r.source).trim()
    : sourceTag;
  return {
    job_id: "",
    title,
    company,
    location,
    url,
    posted_date,
    source,
    jd,
  };
}

function jobPostingToRow(j, pageUrl, defaultCompany, sourceTag) {
  const title = j.title ? String(j.title) : "";
  const url = absoluteUrl(j.url || j.sameAs, pageUrl);
  const company =
    hiringOrgName(j.hiringOrganization) || defaultCompany || "";
  const jd = j.description != null ? String(j.description) : "";
  let location;
  if (j.jobLocation) {
    if (Array.isArray(j.jobLocation)) {
      location = j.jobLocation.map(jobLocationToString).filter(Boolean).join(" | ");
    } else {
      location = jobLocationToString(j.jobLocation);
    }
  }
  const posted_date = j.datePosted ? String(j.datePosted) : undefined;
  return normalizeExportRow(
    { title, company, location, url, posted_date, jd, source: sourceTag },
    pageUrl,
    defaultCompany,
    sourceTag
  );
}

async function collectJsonLd(page) {
  return page.evaluate(() => {
    const jobs = [];
    const isJobPosting = (j) => {
      const t = j && j["@type"];
      if (t === "JobPosting") return true;
      return Array.isArray(t) && t.includes("JobPosting");
    };
    const visit = (node) => {
      if (node == null) return;
      if (Array.isArray(node)) {
        for (const x of node) visit(x);
        return;
      }
      if (typeof node !== "object") return;
      if (node["@graph"]) visit(node["@graph"]);
      if (isJobPosting(node)) jobs.push(node);
      for (const v of Object.values(node)) {
        if (v && typeof v === "object") visit(v);
      }
    };
    for (const s of document.querySelectorAll(
      'script[type="application/ld+json"]'
    )) {
      const text = s.textContent?.trim();
      if (!text) continue;
      try {
        visit(JSON.parse(text));
      } catch {
        /* ignore */
      }
    }
    return jobs;
  });
}

/** Public JSON GET (no browser). Same ExportRow output as Playwright modes; use for documented APIs the careers SPA calls. */
async function runHttpJsonSource(source) {
  const id = source.id || "source";
  const company = source.company || "";
  const sourceTag = source.sourceTag || `spa:http:${id}`;
  const fetchUrls = source.fetchUrls || [];
  if (fetchUrls.length === 0) {
    throw new Error(`http_json source "${id}": fetchUrls must be a non-empty array`);
  }
  const arrayPath = source.arrayPath ? String(source.arrayPath) : "";
  const fieldMap = source.fieldMap || null;
  const maxListings = Number(source.maxListings ?? 10000);
  const defaultUA =
    source.userAgent ||
    "OfferTrackSpaCrawler/1.0 (+https://github/) http_json";

  const rows = [];
  const seenUrl = new Set();
  const greenhouseBoard =
    source.greenhouseBoard != null
      ? String(source.greenhouseBoard).trim()
      : "";
  const preferHosted = source.preferGreenhouseHostedUrl === true;
  const fetchGhDetails = source.fetchGreenhouseJobDetails === true;
  const detailConcurrency = Number(source.greenhouseDetailConcurrency ?? 5);
  const greenhouseDetailQueue = [];

  const pushRow = (row) => {
    if (rows.length >= maxListings) return false;
    if (!row.url || !row.title) return true;
    const key = row.url.trim().toLowerCase();
    if (seenUrl.has(key)) return true;
    seenUrl.add(key);
    rows.push(row);
    return rows.length < maxListings;
  };

  const headers = {
    Accept: "application/json",
    "User-Agent": defaultUA,
    ...(source.headers && typeof source.headers === "object"
      ? source.headers
      : {}),
  };

  const defaults = {
    company,
    source: sourceTag,
    job_id: "",
    jd: "",
  };

  for (const url of fetchUrls) {
    if (rows.length >= maxListings) break;
    const res = await fetch(url, { headers });
    if (!res.ok) {
      throw new Error(`http_json source "${id}": HTTP ${res.status} ${url}`);
    }
    const body = await res.json();
    let arr;
    if (Array.isArray(body)) arr = body;
    else if (arrayPath) {
      const at = getDot(body, arrayPath);
      arr = Array.isArray(at) ? at : null;
    } else {
      arr = null;
    }
    if (!Array.isArray(arr)) {
      throw new Error(
        `http_json source "${id}": expected array or arrayPath → array from ${url}`
      );
    }

    for (const item of arr) {
      if (rows.length >= maxListings) break;
      let mapped;
      if (fieldMap && typeof fieldMap === "object") {
        mapped = toRowsFromFieldMap(item, fieldMap, defaults);
      } else {
        mapped = { ...defaults, ...item };
      }
      if (
        mapped.jd &&
        String(mapped.jd).length > 0 &&
        /[<&]/.test(String(mapped.jd))
      ) {
        mapped.jd = greenhouseContentToPlainText(mapped.jd);
      }
      const row = normalizeExportRow(mapped, url, company, sourceTag);
      const jobId =
        item && item.id !== undefined && item.id !== null ? item.id : null;

      if (preferHosted && greenhouseBoard && jobId != null) {
        const hosted = greenhouseHostedJobUrl(greenhouseBoard, jobId);
        if (hosted) row.url = hosted;
      }

      if (row.location != null && isPlaceholderLocation(row.location)) {
        row.location = undefined;
      }

      if (
        !row.location &&
        source.inferLocationFromTitleWhenPlaceholder !== false
      ) {
        const guess = inferLocationTailFromTitle(row.title);
        if (guess) row.location = guess;
      }

      if (!pushRow(row)) break;

      const needDetail =
        fetchGhDetails &&
        greenhouseBoard &&
        jobId != null &&
        (!row.jd || String(row.jd).trim() === "");
      if (needDetail) {
        greenhouseDetailQueue.push({ row, jobId });
      }
    }
  }

  if (greenhouseDetailQueue.length > 0 && greenhouseBoard) {
    process.stderr.write(
      `[spa-careers] http_json "${id}": fetching ${greenhouseDetailQueue.length} Greenhouse job details (jd + location) …\n`
    );
    await enrichGreenhouseRowsFromDetails(
      greenhouseDetailQueue,
      greenhouseBoard,
      headers,
      detailConcurrency
    );
  }

  return rows;
}

function sourceNeedsPlaywright(s) {
  return s.mode === "json_ld" || s.mode === "intercept";
}

async function runSource(browser, source, globalTimeout) {
  const mode = source.mode;
  const id = source.id || "source";
  const company = source.company || "";
  const sourceTag = source.sourceTag || `spa:${id}`;
  const startUrls = source.startUrls || [];
  const settleMs = Number(source.settleMs ?? 2000);
  const maxListings = Number(source.maxListings ?? 10000);
  const navWait = source.navigationWaitUntil || "networkidle";

  const rows = [];
  const seenUrl = new Set();

  const pushRow = (row) => {
    if (rows.length >= maxListings) return false;
    if (!row.url || !row.title) return true;
    const key = row.url.trim().toLowerCase();
    if (seenUrl.has(key)) return true;
    seenUrl.add(key);
    rows.push(row);
    return rows.length < maxListings;
  };

  const context = await browser.newContext({
    userAgent:
      "OfferTrackSpaCrawler/1.0 (+https://github.com/) Chromium Playwright",
  });
  context.setDefaultTimeout(globalTimeout);

  try {
    for (const startUrl of startUrls) {
      if (rows.length >= maxListings) break;

      if (mode === "json_ld") {
        const page = await context.newPage();
        try {
          await page.goto(startUrl, {
            waitUntil: navWait,
            timeout: globalTimeout,
          });
          if (settleMs > 0)
            await new Promise((r) => setTimeout(r, settleMs));
          const jp = await collectJsonLd(page);
          for (const j of jp) {
            const row = jobPostingToRow(j, page.url(), company, sourceTag);
            if (!pushRow(row)) break;
          }
        } finally {
          await page.close();
        }
      } else if (mode === "intercept") {
        const intercept = source.intercept || {};
        const patterns = (intercept.urlIncludes || []).map(String);
        const arrayPath = intercept.arrayPath
          ? String(intercept.arrayPath)
          : "";
        const fieldMap = source.fieldMap || null;
        const maxResponses = Number(intercept.maxResponses ?? 100);
        const defaults = {
          company,
          source: sourceTag,
          job_id: "",
          jd: "",
        };

        let responseCount = 0;
        const page = await context.newPage();
        const onResponse = async (response) => {
          if (rows.length >= maxListings) return;
          if (responseCount >= maxResponses) return;
          if (response.request().resourceType() === "document") return;
          const u = response.url();
          if (!patterns.some((p) => u.includes(p))) return;
          const status = response.status();
          if (status < 200 || status >= 300) return;
          const ct = (response.headers()["content-type"] || "").toLowerCase();
          if (!ct.includes("json")) return;
          let body;
          try {
            body = await response.json();
          } catch {
            return;
          }
          responseCount += 1;
          let arr;
          if (Array.isArray(body)) arr = body;
          else if (arrayPath) {
            const at = getDot(body, arrayPath);
            arr = Array.isArray(at) ? at : null;
          } else {
            arr = null;
          }
          if (!arr) return;

          for (const item of arr) {
            if (rows.length >= maxListings) break;
            let mapped;
            if (fieldMap && typeof fieldMap === "object") {
              mapped = toRowsFromFieldMap(item, fieldMap, defaults);
            } else {
              mapped = { ...defaults, ...item };
            }
            const row = normalizeExportRow(
              mapped,
              page.url() || startUrl,
              company,
              sourceTag
            );
            if (!pushRow(row)) break;
          }
        };
        page.on("response", onResponse);
        try {
          await page.goto(startUrl, {
            waitUntil: navWait,
            timeout: globalTimeout,
          });
          if (settleMs > 0)
            await new Promise((r) => setTimeout(r, settleMs));
        } finally {
          page.off("response", onResponse);
          await page.close();
        }
      } else {
        throw new Error(`Unknown mode for source "${id}": ${mode}`);
      }
    }
  } finally {
    await context.close();
  }

  return rows;
}

async function main() {
  const argv = process.argv.slice(2);
  let configPath = path.join(__dirname, "spa_config.json");
  let stdoutOnly = false;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--config" && argv[i + 1]) {
      configPath = path.resolve(process.cwd(), argv[i + 1]);
      i++;
    } else if (argv[i] === "--stdout") {
      stdoutOnly = true;
    } else if (argv[i] === "-h" || argv[i] === "--help") {
      usage();
      process.exit(0);
    }
  }

  if (!fs.existsSync(configPath)) {
    console.error(`Config not found: ${configPath}`);
    console.error("Copy spa_config.example.json to spa_config.json and edit.");
    usage();
    process.exit(1);
  }

  const raw = fs.readFileSync(configPath, "utf8");
  const config = JSON.parse(raw);
  const sources = config.sources;
  if (!Array.isArray(sources)) {
    console.error("config.sources must be an array");
    process.exit(1);
  }

  const outFileRaw = config.outFile;
  const writeToFile =
    !stdoutOnly &&
    outFileRaw != null &&
    String(outFileRaw).trim() !== "";
  const outFile = writeToFile
    ? path.resolve(path.dirname(configPath), String(outFileRaw).trim())
    : null;
  const defaultTimeout = Number(config.defaultTimeoutMs ?? 120000);
  const headless = config.headless !== false;

  const playwrightSources = sources.filter(sourceNeedsPlaywright);
  let browser = null;
  if (playwrightSources.length > 0) {
    const launchOpts = { headless };
    const ch = config.chromiumChannel || process.env.PLAYWRIGHT_CHANNEL;
    if (ch) launchOpts.channel = ch;
    browser = await chromium.launch(launchOpts);
  }

  const all = [];
  try {
    for (const src of sources) {
      const id = src.id || "?";
      const mode = src.mode || "?";
      if (mode === "http_json") {
        process.stderr.write(`[spa-careers] source ${id} (http_json) …\n`);
        const part = await runHttpJsonSource(src);
        process.stderr.write(`[spa-careers] source ${id}: ${part.length} rows\n`);
        all.push(...part);
        continue;
      }
      if (!browser) {
        throw new Error(
          `source "${id}": mode "${mode}" needs Playwright but no browser was started (add json_ld or intercept source, or install browsers / set chromiumChannel)`
        );
      }
      process.stderr.write(`[spa-careers] source ${id} (${mode}) …\n`);
      const part = await runSource(browser, src, defaultTimeout);
      process.stderr.write(`[spa-careers] source ${id}: ${part.length} rows\n`);
      all.push(...part);
    }
  } finally {
    if (browser) await browser.close();
  }

  const jsonOut = JSON.stringify(all, null, 2) + "\n";
  if (writeToFile && outFile) {
    fs.mkdirSync(path.dirname(outFile), { recursive: true });
    fs.writeFileSync(outFile, jsonOut, "utf8");
  } else {
    process.stdout.write(jsonOut);
  }
  const summary = {
    outFile: writeToFile ? outFile : null,
    stdout_only: !writeToFile,
    total_rows: all.length,
    sources_run: sources.length,
  };
  console.error(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
